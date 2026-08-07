"""Focused contracts for the dedicated durable worker and typed handlers."""

from __future__ import annotations

import os
import sys
import threading
import time
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from src.config import Config
from src.services.durable_job_handlers import (
    BotAskPayload,
    DurableHandlerBusyError,
    MarketReviewPayload,
    ScheduledAnalysisPayload,
    StockAnalysisPayload,
    _run_market_review_locked,
    build_default_durable_job_registry,
    get_durable_execution_context,
)
from src.services.durable_jobs import (
    JOB_STATUS_CANCELLED,
    JOB_STATUS_FAILED,
    JOB_STATUS_SUCCEEDED,
    DurableJobHandlerRegistry,
    DurableJobStore,
    DurableNoiseClaimBusyError,
    InvalidJobPayloadError,
    JobEnqueueRequest,
    NotificationOutboxStore,
)
from src.services.durable_worker import (
    DurableWorker,
    DurableWorkerPreflightError,
    check_worker_heartbeat,
    classify_job_exception,
    preflight_worker,
    sanitize_error_message,
)
from src.services.run_diagnostics import get_current_diagnostic_context
from src.storage import DatabaseManager
from src.storage import ProviderHealthRecord


class ProbePayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    value: str


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    old_migration_mode = os.environ.get("DATABASE_MIGRATION_MODE")
    os.environ["DATABASE_PATH"] = str(tmp_path / "durable-worker.db")
    os.environ["DATABASE_MIGRATION_MODE"] = "auto"
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if old_database_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old_database_path
        if old_migration_mode is None:
            os.environ.pop("DATABASE_MIGRATION_MODE", None)
        else:
            os.environ["DATABASE_MIGRATION_MODE"] = old_migration_mode


def _store(isolated_db, handler, *, lease_seconds: float = 5.0) -> DurableJobStore:
    registry = DurableJobHandlerRegistry()
    registry.register("probe", 1, ProbePayload, handler)
    return DurableJobStore(
        registry,
        isolated_db,
        lease_seconds=lease_seconds,
        heartbeat_seconds=min(1.0, lease_seconds / 2),
    )


def _enqueue(store: DurableJobStore, task_id: str, value: str = "ok", **kwargs: Any) -> None:
    store.enqueue(
        JobEnqueueRequest(
            task_id=task_id,
            job_type="probe",
            payload={"value": value},
            trace_id=kwargs.pop("trace_id", f"trace-{task_id}"),
            **kwargs,
        )
    )


def test_default_registry_uses_strict_versioned_json_and_secret_safe_bot_payloads() -> None:
    registry = build_default_durable_job_registry()
    encoded = registry.encode_payload(
        "stock_analysis",
        1,
        {
            "stock_code": "600519",
            "stock_name": "Kweichow Moutai",
            "original_query": "analyze 600519",
            "selection_source": "manual",
            "notify": False,
        },
    )
    decoded = registry.decode_payload("stock_analysis", encoded)

    assert isinstance(decoded, StockAnalysisPayload)
    assert decoded.stock_name == "Kweichow Moutai"
    assert decoded.original_query == "analyze 600519"
    assert decoded.selection_source == "manual"
    assert decoded.notify is False
    with pytest.raises(InvalidJobPayloadError):
        registry.encode_payload("stock_analysis", 1, {"stock_code": "600519", "notify": "false"})
    with pytest.raises(InvalidJobPayloadError):
        registry.encode_payload("stock_analysis", 1, {"stock_code": "600519", "callable": "no"})

    bot_payload = registry.encode_payload(
        "bot_ask",
        1,
        {
            "target": {
                "platform": "feishu",
                "chat_id": "conversation-1",
                "message_id": "message-1",
            },
            "stock_codes": ["600519"],
        },
    )
    decoded_bot = registry.decode_payload("bot_ask", bot_payload)
    assert isinstance(decoded_bot, BotAskPayload)
    assert decoded_bot.target.model_dump() == {
        "platform": "feishu",
        "chat_id": "conversation-1",
        "message_id": "message-1",
    }
    with pytest.raises(InvalidJobPayloadError):
        registry.encode_payload(
            "bot_ask",
            1,
            {
                "target": {
                    "platform": "dingtalk",
                    "chat_id": "conversation-1",
                    "message_id": "message-1",
                    "session_webhook": "https://secret.example",
                },
                "stock_codes": ["600519"],
            },
        )

    assert MarketReviewPayload(region="both").region == "cn,hk,us,jp,kr"
    assert MarketReviewPayload(region="us,cn,hk").region == "cn,hk,us"
    with pytest.raises(ValueError, match="both"):
        MarketReviewPayload(region="both,cn")
    with pytest.raises(ValueError, match="duplicates"):
        MarketReviewPayload(region="cn,cn")
    assert ScheduledAnalysisPayload(workers=4).workers == 4
    with pytest.raises(ValueError):
        ScheduledAnalysisPayload(workers=0)


def test_worker_executes_up_to_configured_concurrency_with_context_and_flow_events(isolated_db) -> None:
    lock = threading.Lock()
    all_started = threading.Event()
    active = 0
    max_active = 0
    diagnostic_seen: dict[str, dict[str, Any]] = {}

    def handler(payload: ProbePayload) -> dict[str, Any]:
        nonlocal active, max_active
        context = get_durable_execution_context()
        diagnostic = get_current_diagnostic_context()
        assert diagnostic is not None
        assert diagnostic.stage == "starting"
        assert diagnostic.task_id == context.job_id
        assert diagnostic.query_id == context.query_id
        assert diagnostic.trace_id == context.trace_id
        assert diagnostic.attempt_no == 1
        assert diagnostic.event_sink is not None
        diagnostic.record_llm_run_started(
            call_type="probe",
            provider="fixture-provider",
            model="fixture-model",
        )
        diagnostic.event_sink(
            {
                "type": "provider_run",
                "severity": "success",
                "message": "fixture provider succeeded",
                "metadata": {
                    "provider": "fixture-provider",
                    "data_type": "quote",
                    "duration_ms": 12.5,
                },
            }
        )
        with lock:
            diagnostic_seen[context.job_id] = diagnostic.snapshot()
            active += 1
            max_active = max(max_active, active)
            if active == 2:
                all_started.set()
        assert all_started.wait(3)
        context.stage("running_probe", progress=30, message=f"running {payload.value}")
        assert get_current_diagnostic_context().stage == "running_probe"
        context.flow({"kind": "probe", "value": payload.value})
        with lock:
            active -= 1
        return {
            "value": payload.value,
            "job_id": context.job_id,
            "query_id": context.query_id,
            "trace_id": context.trace_id,
        }

    store = _store(isolated_db, handler)
    _enqueue(store, "probe-a", "a", trace_id="trace-a")
    _enqueue(store, "probe-b", "b", trace_id="trace-b")
    worker = DurableWorker(
        store,
        worker_id="worker-concurrency",
        max_workers=2,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )

    worker.run_until_idle()

    assert max_active == 2
    assert set(diagnostic_seen) == {"probe-a", "probe-b"}
    assert get_current_diagnostic_context() is None
    first = store.get_job("probe-a")
    second = store.get_job("probe-b")
    assert first.status == second.status == JOB_STATUS_SUCCEEDED
    assert first.result == {
        "job_id": "probe-a",
        "query_id": "probe-a",
        "trace_id": "trace-a",
        "value": "a",
    }
    assert second.result["trace_id"] == "trace-b"
    assert sum(
        event.event_type == "task_flow"
        for event in store.read_events(job_id="probe-a")
    ) >= 2
    with isolated_db.get_session() as session:
        health = session.query(ProviderHealthRecord).filter_by(
            kind="component",
            provider_key="durable_worker",
            scope="worker-concurrency",
        ).one()
        assert health.status == "stopped"
        provider_health = session.query(ProviderHealthRecord).filter_by(
            kind="provider",
            provider_key="fixture-provider",
            scope="quote",
        ).one()
        assert provider_health.status == "healthy"
        assert provider_health.success_count == 2
        assert provider_health.last_latency_ms == 12.5


def test_worker_rejects_concurrency_outside_supported_bounds(isolated_db) -> None:
    store = _store(isolated_db, lambda payload: {"value": payload.value})

    with pytest.raises(ValueError, match="between 1 and 64"):
        DurableWorker(store, max_workers=0)
    with pytest.raises(ValueError, match="between 1 and 64"):
        DurableWorker(store, max_workers=65)


def test_worker_healthcheck_requires_fresh_accepting_component_heartbeat(isolated_db) -> None:
    store = _store(isolated_db, lambda payload: {"value": payload.value})
    checked_at = datetime(2026, 8, 8, 8, 0, tzinfo=timezone.utc)
    store.heartbeat_component(
        "durable_worker",
        "health-worker",
        status="idle",
        now=checked_at,
    )

    assert check_worker_heartbeat(
        isolated_db._db_url,
        worker_id="health-worker",
        max_age_seconds=45,
        now=checked_at + timedelta(seconds=44),
    )
    assert not check_worker_heartbeat(
        isolated_db._db_url,
        worker_id="health-worker",
        max_age_seconds=45,
        now=checked_at + timedelta(seconds=46),
    )
    assert not check_worker_heartbeat(
        isolated_db._db_url,
        worker_id="different-worker",
        max_age_seconds=45,
        now=checked_at,
    )

    store.heartbeat_component(
        "durable_worker",
        "health-worker",
        status="stopping",
        now=checked_at + timedelta(seconds=1),
    )
    assert not check_worker_heartbeat(
        isolated_db._db_url,
        worker_id="health-worker",
        max_age_seconds=45,
        now=checked_at + timedelta(seconds=1),
    )


def test_stock_handler_injects_job_query_and_trace_ids_into_existing_service(
    isolated_db,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeAnalysisService:
        last_error = None

        def analyze_stock(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            kwargs["progress_callback"](45, "halfway")
            return {"stock_code": kwargs["stock_code"], "query_id": kwargs["query_id"]}

    fake_module = types.ModuleType("src.services.analysis_service")
    fake_module.AnalysisService = FakeAnalysisService
    monkeypatch.setitem(sys.modules, "src.services.analysis_service", fake_module)

    registry = build_default_durable_job_registry()
    store = DurableJobStore(registry, isolated_db, lease_seconds=5, heartbeat_seconds=1)
    store.enqueue(
        JobEnqueueRequest(
            task_id="stock-job",
            job_type="stock_analysis",
            payload={
                "stock_code": "600519",
                "stock_name": "Moutai",
                "original_query": "600519",
                "selection_source": "manual",
                "notify": False,
                "query_source": "api",
                "bot_target": {
                    "platform": "feishu",
                    "chat_id": "chat-stock",
                    "message_id": "message-stock",
                },
            },
            trace_id="trace-stock",
        )
    )
    worker = DurableWorker(
        store,
        worker_id="worker-stock",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )

    worker.run_until_idle()

    assert store.get_job("stock-job").status == JOB_STATUS_SUCCEEDED
    assert captured["query_id"] == "stock-job"
    assert captured["trace_id"] == "trace-stock"
    assert captured["send_notification"] is False
    assert captured["query_source"] == "api"
    assert captured["source_message"].platform == "feishu"
    assert captured["source_message"].chat_id == "chat-stock"
    assert captured["source_message"].user_id == ""
    assert captured["source_message"].content == ""
    assert captured["source_message"].raw_data == {}


def test_bot_ask_worker_queues_safe_final_context_reply(
    isolated_db,
    monkeypatch,
) -> None:
    from bot.commands.ask import AskCommand
    from bot.models import BotResponse

    config = Config.get_instance()
    monkeypatch.setattr(config, "agent_mode", True)
    monkeypatch.setattr(config, "durable_jobs_enabled", True)
    captured: dict[str, Any] = {}

    def fake_execute(self, runtime_config, message, codes, skill_id, skill_text):
        captured.update(
            message=message,
            codes=codes,
            skill_id=skill_id,
            skill_text=skill_text,
        )
        return BotResponse.markdown_response("final durable bot answer")

    monkeypatch.setattr(AskCommand, "_execute_parsed", fake_execute)
    registry = build_default_durable_job_registry()
    store = DurableJobStore(registry, isolated_db, lease_seconds=5, heartbeat_seconds=1)
    store.enqueue(
        JobEnqueueRequest(
            task_id="bot-ask-job",
            job_type="bot_ask",
            payload={
                "target": {
                    "platform": "feishu",
                    "chat_id": "chat-safe",
                    "message_id": "message-safe",
                },
                "stock_codes": ["600519"],
                "skill_id": "bull_trend",
                "skill_text": "focus on trend",
            },
            trace_id="trace-bot-ask",
            notify=True,
        )
    )
    worker = DurableWorker(
        store,
        worker_id="worker-bot-ask",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )

    worker.run_until_idle()

    snapshot = store.get_job("bot-ask-job")
    assert snapshot.status == JOB_STATUS_SUCCEEDED
    assert snapshot.result["notification"]["status"] in {"queued", "already_sent"}
    source_message = captured["message"]
    assert source_message.platform == "feishu"
    assert source_message.chat_id == "chat-safe"
    assert source_message.message_id == "message-safe"
    assert source_message.user_id == ""
    assert source_message.content == ""
    assert source_message.raw_data == {}

    claimed = NotificationOutboxStore(isolated_db).claim_next("outbox-inspector")
    assert claimed is not None
    assert claimed.channel == "context_feishu"
    assert claimed.payload["target"] == {
        "platform": "feishu",
        "chat_id": "chat-safe",
        "message_id": "message-safe",
    }
    assert claimed.payload["content"] == "final durable bot answer"


def test_dingtalk_durable_bot_reply_is_explicitly_unsupported(
    isolated_db,
    monkeypatch,
) -> None:
    from bot.commands.research import ResearchCommand
    from bot.models import BotResponse

    config = Config.get_instance()
    monkeypatch.setattr(config, "agent_mode", True)
    monkeypatch.setattr(config, "durable_jobs_enabled", True)
    monkeypatch.setattr(
        ResearchCommand,
        "_run_research",
        lambda self, runtime_config, stock_code, question: BotResponse.text_response(
            "dingtalk result"
        ),
    )
    registry = build_default_durable_job_registry()
    store = DurableJobStore(registry, isolated_db, lease_seconds=5, heartbeat_seconds=1)
    store.enqueue(
        JobEnqueueRequest(
            task_id="bot-dingtalk-job",
            job_type="bot_research",
            payload={
                "target": {
                    "platform": "dingtalk",
                    "chat_id": "chat-dingtalk",
                    "message_id": "message-dingtalk",
                },
                "stock_code": "600519",
                "question": "required research query",
            },
            notify=True,
        )
    )
    worker = DurableWorker(
        store,
        worker_id="worker-dingtalk",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )

    worker.run_until_idle()

    snapshot = store.get_job("bot-dingtalk-job")
    assert snapshot.status == JOB_STATUS_SUCCEEDED
    assert snapshot.result["notification"] == {
        "status": "unsupported_context",
        "accepted": False,
    }
    assert NotificationOutboxStore(isolated_db).claim_next("outbox-inspector") is None


def test_bot_scheduled_analysis_reconstructs_only_safe_target_in_worker(
    isolated_db,
    monkeypatch,
) -> None:
    import src.core.pipeline as pipeline_module

    captured: dict[str, Any] = {}

    class FakePipeline:
        def __init__(self, **kwargs: Any) -> None:
            captured["init"] = kwargs

        def run(self, **kwargs: Any) -> list[Any]:
            captured["run"] = kwargs
            return []

    monkeypatch.setattr(pipeline_module, "StockAnalysisPipeline", FakePipeline)
    registry = build_default_durable_job_registry()
    store = DurableJobStore(registry, isolated_db, lease_seconds=5, heartbeat_seconds=1)
    store.enqueue(
        JobEnqueueRequest(
            task_id="bot-batch-job",
            job_type="scheduled_analysis",
            payload={
                "stock_codes": ["600519", "000858"],
                "no_market_review": True,
                "single_notify": True,
                "bot_target": {
                    "platform": "telegram",
                    "chat_id": "chat-batch",
                    "message_id": "message-batch",
                },
            },
            notify=True,
        )
    )
    worker = DurableWorker(
        store,
        worker_id="worker-batch",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )

    worker.run_until_idle()

    snapshot = store.get_job("bot-batch-job")
    assert snapshot.status == JOB_STATUS_SUCCEEDED
    assert snapshot.result["analyzed_count"] == 0
    source_message = captured["init"]["source_message"]
    assert source_message.platform == "telegram"
    assert source_message.chat_id == "chat-batch"
    assert source_message.message_id == "message-batch"
    assert source_message.user_id == ""
    assert source_message.content == ""
    assert source_message.raw_data == {}
    assert captured["run"]["stock_codes"] == ["600519", "000858"]


def test_scheduled_handler_preserves_runtime_worker_override(
    isolated_db,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(config: Any, args: Any, stock_codes: Any) -> bool:
        captured.update(config=config, args=args, stock_codes=stock_codes)
        diagnostic = get_current_diagnostic_context()
        assert diagnostic is not None
        assert diagnostic.task_id == "scheduled-job"
        return True

    fake_main = types.ModuleType("main")
    fake_main.run_scheduled_analysis = fake_run
    monkeypatch.setitem(sys.modules, "main", fake_main)

    registry = build_default_durable_job_registry()
    store = DurableJobStore(registry, isolated_db, lease_seconds=5, heartbeat_seconds=1)
    store.enqueue(
        JobEnqueueRequest(
            task_id="scheduled-job",
            job_type="scheduled_analysis",
            payload={
                "stock_codes": ["600519"],
                "workers": 4,
                "no_notify": True,
            },
            trace_id="trace-scheduled",
        )
    )
    worker = DurableWorker(
        store,
        worker_id="worker-scheduled",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )

    worker.run_until_idle()

    assert store.get_job("scheduled-job").status == JOB_STATUS_SUCCEEDED
    assert captured["args"].workers == 4
    assert captured["stock_codes"] == ["600519"]


def test_event_monitor_notification_failure_fails_the_durable_job(
    isolated_db,
    monkeypatch,
) -> None:
    import src.agent.events as events_module
    from src.notification import NotificationService

    def fake_run_event_monitor_once(monitor: Any) -> list[Any]:
        triggered = SimpleNamespace(
            rule=monitor.rules[0],
            message="600519 crossed the configured threshold",
        )
        for callback in monitor._callbacks:
            try:
                callback(triggered)
            except RuntimeError:
                # Match EventMonitor's legacy best-effort callback boundary.
                pass
        return [triggered]

    def fail_notification(*args: Any, **kwargs: Any) -> bool:
        raise RuntimeError("outbox event write failed")

    monkeypatch.setattr(events_module, "run_event_monitor_once", fake_run_event_monitor_once)
    monkeypatch.setattr(NotificationService, "send", fail_notification)

    registry = build_default_durable_job_registry()
    store = DurableJobStore(registry, isolated_db, lease_seconds=5, heartbeat_seconds=1)
    store.enqueue(
        JobEnqueueRequest(
            task_id="event-monitor-notification-failure",
            job_type="event_monitor",
            payload={
                "rules": [
                    {
                        "stock_code": "600519",
                        "alert_type": "price_cross",
                        "direction": "above",
                        "price": 1.0,
                    }
                ],
                "send_notification": True,
            },
            max_attempts=1,
        )
    )
    worker = DurableWorker(
        store,
        worker_id="worker-event-notification-failure",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )

    worker.run_until_idle()

    snapshot = store.get_job("event-monitor-notification-failure")
    assert snapshot.status == JOB_STATUS_FAILED
    assert snapshot.error_code == "handler_error"
    assert "outbox event write failed" in (snapshot.error_message_sanitized or "")


def test_processing_cancellation_stops_at_checkpoint_and_discards_result(isolated_db) -> None:
    started = threading.Event()

    def handler(_: ProbePayload) -> dict[str, bool]:
        context = get_durable_execution_context()
        started.set()
        while True:
            time.sleep(0.01)
            context.checkpoint()

    store = _store(isolated_db, handler)
    _enqueue(store, "cancel-job")
    worker = DurableWorker(
        store,
        worker_id="worker-cancel",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )

    assert worker.run_claim_cycle() == 1
    assert started.wait(2)
    store.cancel("cancel-job")
    assert worker.wait_for_idle(3)
    worker.shutdown(wait=True)

    snapshot = store.get_job("cancel-job")
    assert snapshot.status == JOB_STATUS_CANCELLED
    assert snapshot.result is None


def test_graceful_shutdown_keeps_heartbeating_until_handler_finishes(isolated_db) -> None:
    started = threading.Event()
    release = threading.Event()

    def handler(_: ProbePayload) -> dict[str, bool]:
        started.set()
        assert release.wait(5)
        return {"finished": True}

    store = _store(isolated_db, handler, lease_seconds=1.0)
    _enqueue(store, "drain-job")
    worker = DurableWorker(
        store,
        worker_id="worker-drain",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )
    assert worker.run_claim_cycle() == 1
    assert started.wait(2)
    initial_expiry = store.get_job("drain-job").lease_expires_at

    stopped = threading.Event()

    def shutdown() -> None:
        worker.shutdown(wait=True)
        stopped.set()

    shutdown_thread = threading.Thread(target=shutdown)
    shutdown_thread.start()
    deadline = time.monotonic() + 2
    renewed_expiry = initial_expiry
    while time.monotonic() < deadline and renewed_expiry <= initial_expiry:
        time.sleep(0.03)
        renewed_expiry = store.get_job("drain-job").lease_expires_at

    assert worker.is_stopping is True
    assert renewed_expiry > initial_expiry
    assert stopped.is_set() is False
    release.set()
    shutdown_thread.join(timeout=3)
    assert stopped.is_set() is True
    assert store.get_job("drain-job").status == JOB_STATUS_SUCCEEDED


def test_new_worker_recovers_a_killed_workers_expired_lease(isolated_db) -> None:
    def handler(payload: ProbePayload) -> dict[str, str]:
        return {"value": payload.value}

    store = _store(isolated_db, handler, lease_seconds=1.0)
    past = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=2)
    store.enqueue(
        JobEnqueueRequest(
            task_id="recovered-job",
            job_type="probe",
            payload={"value": "recovered"},
        ),
        now=past,
    )
    killed_claim = store.claim_next("worker-killed", now=past)
    assert killed_claim.attempt == 1

    replacement = DurableWorker(
        store,
        worker_id="worker-replacement",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )
    replacement.run_until_idle()

    snapshot = store.get_job("recovered-job")
    assert snapshot.status == JOB_STATUS_SUCCEEDED
    assert snapshot.attempt == 2
    assert snapshot.result == {"value": "recovered"}


class HttpError(RuntimeError):
    def __init__(self, status_code: int, message: str = "provider failed", retry_after: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = SimpleNamespace(
            status_code=status_code,
            headers={} if retry_after is None else {"Retry-After": retry_after},
        )


@pytest.mark.parametrize(
    ("error", "code", "retryable"),
    [
        (HttpError(429, retry_after="17"), "rate_limited", True),
        (HttpError(503), "provider_server_error", True),
        (HttpError(404), "provider_http_404", False),
        (HttpError(401), "authentication_failed", False),
        (HttpError(403), "permission_denied", False),
        (TimeoutError("timed out"), "timeout", True),
        (ConnectionError("connection closed"), "connection_failed", True),
        (ValueError("bad input"), "validation_error", False),
        (RuntimeError("unknown"), "handler_error", False),
    ],
)
def test_retry_classification_is_allowlisted(error, code: str, retryable: bool) -> None:
    disposition = classify_job_exception(error)
    assert disposition.error_code == code
    assert disposition.retryable is retryable
    if isinstance(error, HttpError) and error.status_code == 429:
        assert disposition.retry_after == "17"


def test_nested_transport_failure_remains_retryable_but_auth_and_permission_do_not() -> None:
    try:
        raise TimeoutError("provider timeout")
    except TimeoutError as cause:
        wrapped = HttpError(424)
        wrapped.__cause__ = cause
    assert classify_job_exception(wrapped).retryable is True

    try:
        raise PermissionError("denied")
    except PermissionError as cause:
        wrapped_permission = HttpError(424)
        wrapped_permission.__cause__ = cause
    disposition = classify_job_exception(wrapped_permission)
    assert disposition.error_code == "permission_denied"
    assert disposition.retryable is False

    malformed_retry_after = classify_job_exception(HttpError(429, retry_after="not-a-date"))
    assert malformed_retry_after.retryable is True
    assert malformed_retry_after.retry_after is None


def test_error_sanitization_redacts_headers_urls_query_tokens_and_credentials() -> None:
    safe = sanitize_error_message(
        "Authorization: Bearer header-secret "
        "api_key=key-secret token='token-secret' password=pw-secret "
        "payload={'refresh_token': 'json-secret'} "
        "https://user:pass-secret@example.test/path?access_token=query-secret&x=1"
    )
    assert "header-secret" not in safe
    assert "key-secret" not in safe
    assert "token-secret" not in safe
    assert "pw-secret" not in safe
    assert "json-secret" not in safe
    assert "pass-secret" not in safe
    assert "query-secret" not in safe
    assert safe.lower().count("<redacted>") >= 5


def test_worker_preflight_is_flag_and_migration_fail_closed() -> None:
    disabled = SimpleNamespace(durable_jobs_enabled=False)
    with pytest.raises(DurableWorkerPreflightError, match="DURABLE_JOBS_ENABLED"):
        preflight_worker(
            disabled,
            "sqlite:///ignored.db",
            checker=lambda _: pytest.fail("migration checker must not run when the flag is off"),
        )

    enabled = SimpleNamespace(durable_jobs_enabled=True)
    stale = SimpleNamespace(
        is_compatible=True,
        is_current=False,
        error=None,
        pending_versions=("pr1",),
    )
    with pytest.raises(DurableWorkerPreflightError, match="migration is not current"):
        preflight_worker(enabled, "sqlite:///ignored.db", checker=lambda _: stale)

    current = SimpleNamespace(
        is_compatible=True,
        is_current=True,
        error=None,
        pending_versions=(),
    )
    preflight_worker(enabled, "sqlite:///ignored.db", checker=lambda _: current)


def test_market_review_worker_lock_rejects_duplicate_owner_and_never_leaks(tmp_path) -> None:
    from src.core.market_review_lock import (
        release_market_review_lock,
        try_acquire_market_review_lock,
    )

    config = SimpleNamespace(database_path=str(tmp_path / "market-review.db"))
    existing = try_acquire_market_review_lock(config)
    assert existing is not None
    called = False

    def must_not_run(**_: Any) -> None:
        nonlocal called
        called = True

    try:
        with pytest.raises(DurableHandlerBusyError, match="still running"):
            _run_market_review_locked(config, must_not_run)
        assert called is False
    finally:
        release_market_review_lock(existing)

    def failing_runner(**kwargs: Any) -> None:
        assert kwargs["config"] is config
        raise RuntimeError("fixture failure")

    with pytest.raises(RuntimeError, match="fixture failure"):
        _run_market_review_locked(config, failing_runner)

    reacquired = try_acquire_market_review_lock(config)
    assert reacquired is not None
    release_market_review_lock(reacquired)

    disposition = classify_job_exception(DurableHandlerBusyError("busy", retry_after=23))
    assert disposition.error_code == "resource_busy"
    assert disposition.retryable is True
    assert disposition.retry_after == 23

    noise_busy = classify_job_exception(
        DurableNoiseClaimBusyError("noise owner still active", retry_after=31)
    )
    assert noise_busy.error_code == "resource_busy"
    assert noise_busy.retryable is True
    assert noise_busy.retry_after == 31
