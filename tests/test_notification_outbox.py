"""End-to-end contracts for durable notification planning and delivery."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timedelta
from unittest import mock

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from bot.models import BotMessage, ChatType
from src.config import Config
from src.notification import NotificationChannel, NotificationService
from src.notification_noise import reset_notification_noise_state
from src.services.durable_job_handlers import (
    DurableExecutionContext,
    bind_durable_execution_context,
)
from src.services.durable_jobs import (
    OUTBOX_STATUS_CANCELLED,
    OUTBOX_STATUS_DELIVERY_UNKNOWN,
    OUTBOX_STATUS_FAILED,
    OUTBOX_STATUS_PENDING,
    OUTBOX_STATUS_PROCESSING,
    OUTBOX_STATUS_SENT,
    JOB_STATUS_CANCEL_REQUESTED,
    JOB_STATUS_SUCCEEDED,
    DurableJobHandlerRegistry,
    DurableNoiseClaimBusyError,
    DurableJobStore,
    JobEnqueueRequest,
    NotificationOutboxStore,
    OutboxEnqueueRequest,
    StaleLeaseError,
)
from src.services.notification_outbox_dispatcher import (
    NotificationDeliveryError,
    NotificationOutboxDispatcher,
)
from src.services.durable_worker import DurableWorker
from src.storage import (
    AnalysisJobRecord,
    DatabaseManager,
    NotificationOutboxRecord,
    ProviderHealthRecord,
)


class _ProbePayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    value: str


class _SimulatedProcessCrash(BaseException):
    pass


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    old_migration_mode = os.environ.get("DATABASE_MIGRATION_MODE")
    os.environ["DATABASE_PATH"] = str(tmp_path / "notification-outbox.db")
    os.environ["DATABASE_MIGRATION_MODE"] = "auto"
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        reset_notification_noise_state()
        if old_database_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old_database_path
        if old_migration_mode is None:
            os.environ.pop("DATABASE_MIGRATION_MODE", None)
        else:
            os.environ["DATABASE_MIGRATION_MODE"] = old_migration_mode


def _config(**overrides) -> Config:
    return Config(stock_list=[], **overrides)


def _job_context(
    db: DatabaseManager,
    *,
    task_id: str,
    notify: bool,
    now: datetime,
) -> DurableExecutionContext:
    registry = DurableJobHandlerRegistry()
    registry.register("probe", 1, _ProbePayload, lambda payload: payload.value)
    store = DurableJobStore(registry, db, lease_seconds=90, heartbeat_seconds=15)
    store.enqueue(
        JobEnqueueRequest(
            task_id=task_id,
            job_type="probe",
            payload={"value": "ok"},
            trace_id=f"trace-{task_id}",
            notify=notify,
        ),
        now=now,
    )
    claimed = store.claim_next(f"worker-{task_id}", now=now)
    assert claimed is not None
    return DurableExecutionContext(
        store=store,
        claimed_job=claimed,
        query_id=f"query-{task_id}",
        trace_id=claimed.trace_id,
        cancel_requested=threading.Event(),
        lease_lost=threading.Event(),
    )


def _rows(db: DatabaseManager) -> list[NotificationOutboxRecord]:
    with db.get_session() as session:
        return list(
            session.execute(
                select(NotificationOutboxRecord).order_by(NotificationOutboxRecord.id)
            ).scalars()
        )


def _static_request(
    service: NotificationService,
    channel: NotificationChannel,
    *,
    logical_id: str,
    content: str = "report",
    max_attempts: int = 4,
) -> OutboxEnqueueRequest:
    target = service._static_delivery_target(
        channel,
        email_stock_codes=None,
        email_send_to_all=False,
    )
    return OutboxEnqueueRequest(
        logical_notification_id=logical_id,
        notification_type="report",
        channel=channel.value,
        route="report",
        recipient=service._static_recipient_identity(channel, target),
        content_sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
        payload={
            "version": 1,
            "kind": "static",
            "content": content,
            "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
            "delivery_mode": "text",
            "delivery_target": target,
            "image_max_chars": 15000,
            "structured_payload": None,
        },
        max_attempts=max_attempts,
    )


def test_flag_off_preserves_direct_send_and_channel_contract(monkeypatch) -> None:
    config = _config(wechat_webhook_url="https://example.invalid/wechat")
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    service = NotificationService()
    low_level = mock.Mock(return_value=True)
    monkeypatch.setattr(service, "_send_to_static_channel", low_level)

    result = service.send_with_results("legacy report", route_type="report")

    assert result.status == "sent"
    assert result.dispatched is True
    assert result.success is True
    assert service.send("legacy report 2", route_type="report") is True
    assert service.get_available_channels() == [NotificationChannel.WECHAT]
    assert low_level.call_count == 2


def test_durable_planning_is_per_channel_idempotent_and_secret_free(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 9, 0, 0)
    monkeypatch.setattr("src.services.durable_jobs.utc_naive_now", lambda: now)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=secret-key",
        telegram_bot_token="bot-token-must-not-persist",
        telegram_chat_id="100200300",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    outbox = NotificationOutboxStore(isolated_db)
    service = NotificationService(outbox_store=outbox)
    monkeypatch.setattr(
        service,
        "_send_to_static_channel",
        mock.Mock(side_effect=AssertionError("durable planning must not send")),
    )
    context = _job_context(isolated_db, task_id="notify-job", notify=True, now=now)
    structured = {
        "score": 88,
        "raw_data": {
            "webhook_url": "https://hooks.example.invalid/secret",
            "token": "nested-secret",
        },
    }

    with bind_durable_execution_context(context):
        first = service.send_with_results(
            "report\ntoken=top-secret",
            route_type="report",
            dedup_key="daily:600519:20260808",
            structured_payload=structured,
        )
        duplicate = service.send_with_results(
            "report\ntoken=top-secret",
            route_type="report",
            dedup_key="daily:600519:20260808",
            structured_payload=structured,
        )

    rows = _rows(isolated_db)
    assert first.status == "queued"
    assert first.dispatched is False
    assert first.success is True
    assert all(item.queued for item in first.channel_results)
    assert duplicate.status == "queued"
    assert len(rows) == 2
    assert {row.channel for row in rows} == {"wechat", "telegram"}
    assert {row.job_id for row in rows} == {"notify-job"}
    assert len({row.logical_notification_id for row in rows}) == 1

    persisted = "\n".join(row.payload_json + row.recipient for row in rows)
    assert "top-secret" not in persisted
    assert "nested-secret" not in persisted
    assert "bot-token-must-not-persist" not in persisted
    assert "secret-key" not in persisted
    assert "webhook_url" not in persisted
    assert "raw_data" not in persisted
    for row in rows:
        payload = json.loads(row.payload_json)
        assert payload["content"] == "report\ntoken=[REDACTED]"
        assert payload["content_sha256"] == hashlib.sha256(
            payload["content"].encode("utf-8")
        ).hexdigest()
        assert row.content_sha256 == payload["content_sha256"]


def test_durable_changed_replay_notification_keeps_first_persisted_content(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 9, 30, 0)
    monkeypatch.setattr("src.services.durable_jobs.utc_naive_now", lambda: now)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    service = NotificationService(outbox_store=NotificationOutboxStore(isolated_db))
    context = _job_context(
        isolated_db,
        task_id="notify-conflicting-replay",
        notify=True,
        now=now,
    )

    with bind_durable_execution_context(context):
        first = service.send_with_results(
            "first committed notification",
            route_type="report",
            dedup_key="stable-logical-delivery",
        )
        replay = service.send_with_results(
            "changed retry notification",
            route_type="report",
            dedup_key="stable-logical-delivery",
        )

    assert first.status == "queued"
    assert replay.status == "queued"
    rows = _rows(isolated_db)
    assert len(rows) == 1
    assert json.loads(rows[0].payload_json)["content"] == "first committed notification"


def test_durable_multichannel_plan_rolls_back_as_one_transaction(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 9, 35, 0)
    monkeypatch.setattr("src.services.durable_jobs.utc_naive_now", lambda: now)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
        telegram_bot_token="telegram-token",
        telegram_chat_id="100200300",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    service = NotificationService(outbox_store=NotificationOutboxStore(isolated_db))
    context = _job_context(isolated_db, task_id="atomic-plan", notify=True, now=now)
    session_class = isolated_db._SessionLocal.class_
    original_flush = session_class.flush

    def fail_multichannel_flush(session, *args, **kwargs):
        new_outbox_rows = [
            row for row in session.new if isinstance(row, NotificationOutboxRecord)
        ]
        if len(new_outbox_rows) == 2:
            raise RuntimeError("second channel insert failed")
        return original_flush(session, *args, **kwargs)

    monkeypatch.setattr(session_class, "flush", fail_multichannel_flush)
    with bind_durable_execution_context(context), pytest.raises(
        RuntimeError,
        match="second channel insert failed",
    ):
        service.send_with_results(
            "atomic report",
            route_type="report",
            dedup_key="atomic-multichannel",
        )

    assert _rows(isolated_db) == []


def test_durable_replay_freezes_first_channel_set_and_payload(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 9, 40, 0)
    monkeypatch.setattr("src.services.durable_jobs.utc_naive_now", lambda: now)
    first_config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
        telegram_bot_token="telegram-token",
        telegram_chat_id="100200300",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: first_config)
    store = NotificationOutboxStore(isolated_db)
    first_service = NotificationService(outbox_store=store)
    context = _job_context(isolated_db, task_id="frozen-plan", notify=True, now=now)
    with bind_durable_execution_context(context):
        first = first_service.send_with_results(
            "first frozen content",
            route_type="report",
            dedup_key="frozen-multichannel",
        )
    assert {row.channel for row in _rows(isolated_db)} == {"wechat", "telegram"}

    changed_config = _config(
        durable_jobs_enabled=True,
        email_sender="owner@example.invalid",
        email_password="smtp-secret",
        email_receivers=["owner@example.invalid"],
    )
    monkeypatch.setattr("src.notification.get_config", lambda: changed_config)
    changed_service = NotificationService(outbox_store=store)
    with bind_durable_execution_context(context):
        replay = changed_service.send_with_results(
            "changed retry content",
            route_type="report",
            dedup_key="frozen-multichannel",
        )

    assert first.status == replay.status == "queued"
    assert {item.channel for item in replay.channel_results} == {"wechat", "telegram"}
    rows = _rows(isolated_db)
    assert len(rows) == 2
    assert {row.channel for row in rows} == {"wechat", "telegram"}
    assert {
        json.loads(row.payload_json)["content"] for row in rows
    } == {"first frozen content"}


def test_durable_notification_requires_stable_key_and_batch_api(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 9, 42, 0)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    store = NotificationOutboxStore(isolated_db)
    service = NotificationService(outbox_store=store)
    context = _job_context(isolated_db, task_id="stable-key", notify=True, now=now)
    with bind_durable_execution_context(context), pytest.raises(
        ValueError,
        match="stable dedup_key",
    ):
        service.send_with_results("unstable report", route_type="report")

    request = _static_request(
        service,
        NotificationChannel.WECHAT,
        logical_id="job-bound-single",
    )
    request = OutboxEnqueueRequest(
        **{
            **request.__dict__,
            "job_id": context.job_id,
            "worker_id": context.worker_id,
            "lease_token": context.lease_token,
        }
    )
    with pytest.raises(ValueError, match="enqueue_batch"):
        store.enqueue(request, now=now)
    assert _rows(isolated_db) == []


def test_durable_outbox_persistence_failure_propagates_to_job_attempt(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 9, 45, 0)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    outbox = mock.Mock(spec=NotificationOutboxStore)
    outbox.get_frozen_plan.return_value = []
    outbox.enqueue_batch.side_effect = RuntimeError("sqlite outbox write failed")
    service = NotificationService(outbox_store=outbox)
    context = _job_context(
        isolated_db,
        task_id="notify-persistence-failure",
        notify=True,
        now=now,
    )

    with bind_durable_execution_context(context), pytest.raises(
        RuntimeError,
        match="sqlite outbox write failed",
    ):
        service.send_with_results(
            "must be durable before completion",
            route_type="report",
            dedup_key="outbox-write-failure",
        )


def test_durable_notify_false_and_noise_suppression_never_enqueue(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 10, 0, 0)
    monkeypatch.setattr("src.services.durable_jobs.utc_naive_now", lambda: now)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
        notification_min_severity="warning",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    service = NotificationService(outbox_store=NotificationOutboxStore(isolated_db))

    disabled_context = _job_context(
        isolated_db,
        task_id="notification-disabled",
        notify=False,
        now=now,
    )
    with bind_durable_execution_context(disabled_context):
        disabled = service.send_with_results("report", route_type="report")
    assert disabled.status == "notification_disabled"
    assert _rows(isolated_db) == []

    enabled_context = _job_context(
        isolated_db,
        task_id="notification-noise",
        notify=True,
        now=now,
    )
    with bind_durable_execution_context(enabled_context):
        suppressed = service.send_with_results(
            "report",
            route_type="report",
            dedup_key="noise-suppressed",
        )
    assert suppressed.status == "noise_suppressed"
    assert _rows(isolated_db) == []


@pytest.mark.parametrize("lease_failure", ["replaced", "cancelled", "expired"])
def test_durable_notification_enqueue_rejects_invalid_parent_lease(
    isolated_db,
    monkeypatch,
    lease_failure,
) -> None:
    now = datetime(2026, 8, 8, 10, 30, 0)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    service = NotificationService(outbox_store=NotificationOutboxStore(isolated_db))
    context = _job_context(isolated_db, task_id="replaced-lease", notify=True, now=now)
    with isolated_db.session_scope() as session:
        parent = session.get(AnalysisJobRecord, context.job_id)
        if lease_failure == "replaced":
            parent.lease_owner = "replacement-worker"
            parent.lease_token = "replacement-token"
            parent.lease_expires_at = now + timedelta(minutes=5)
        elif lease_failure == "cancelled":
            parent.status = JOB_STATUS_CANCEL_REQUESTED
            parent.cancel_requested_at = now
        else:
            parent.lease_expires_at = datetime(2000, 1, 1)

    with bind_durable_execution_context(context):
        with pytest.raises(StaleLeaseError, match="lease is stale"):
            service.send_with_results(
                "must not queue",
                route_type="report",
                dedup_key=f"invalid-parent-{lease_failure}",
            )

    assert _rows(isolated_db) == []


def test_durable_notification_enqueue_uses_fresh_transaction_clock(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 10, 45, 0)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    service = NotificationService(outbox_store=NotificationOutboxStore(isolated_db))
    context = _job_context(isolated_db, task_id="lease-expires-before-write", notify=True, now=now)

    # The implicit clock must be sampled after the write transaction starts.
    # This models a writer that waited on SQLite until its parent lease expired.
    monkeypatch.setattr(
        "src.services.durable_jobs.utc_naive_now",
        lambda: now + timedelta(seconds=91),
    )
    with bind_durable_execution_context(context), pytest.raises(
        StaleLeaseError,
        match="lease is stale",
    ):
        service.send_with_results(
            "must not queue after lease expiry",
            route_type="report",
            dedup_key="expired-before-write",
        )

    assert _rows(isolated_db) == []


def test_context_target_is_minimal_and_dingtalk_session_is_explicitly_unsupported(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 11, 0, 0)
    monkeypatch.setattr("src.services.durable_jobs.utc_naive_now", lambda: now)
    monkeypatch.setattr(
        "src.notification.get_config",
        lambda: _config(durable_jobs_enabled=True),
    )
    outbox = NotificationOutboxStore(isolated_db)

    feishu_message = BotMessage(
        platform="feishu",
        message_id="msg-feishu",
        user_id="user-secret",
        user_name="tester",
        chat_id="chat-safe",
        chat_type=ChatType.GROUP,
        content="/a 600519",
        raw_data={"token": "raw-secret", "webhook": "https://secret.invalid/hook"},
    )
    feishu = NotificationService(source_message=feishu_message, outbox_store=outbox)
    context = _job_context(isolated_db, task_id="feishu-context", notify=True, now=now)
    with bind_durable_execution_context(context):
        result = feishu.send_with_results(
            "context report",
            dedup_key="feishu-context-reply",
        )
    assert result.status == "queued"
    row = _rows(isolated_db)[0]
    payload = json.loads(row.payload_json)
    assert row.channel == "context_feishu"
    assert payload["target"] == {
        "chat_id": "chat-safe",
        "message_id": "msg-feishu",
        "platform": "feishu",
    }
    assert "raw-secret" not in row.payload_json
    assert "user-secret" not in row.payload_json
    send_feishu = mock.Mock(return_value=True)
    monkeypatch.setattr(feishu, "_send_feishu_stream_reply", send_feishu)
    context.store.complete(
        context.job_id,
        context.worker_id,
        context.lease_token,
        {"ok": True},
        now=now,
    )
    delivered = NotificationOutboxDispatcher(
        outbox,
        service_factory=lambda: feishu,
    ).dispatch_once(now=now)
    assert delivered.status == OUTBOX_STATUS_SENT
    send_feishu.assert_called_once_with("chat-safe", "context report")

    dingtalk_message = BotMessage(
        platform="dingtalk",
        message_id="msg-dingtalk",
        user_id="user",
        user_name="tester",
        chat_id="chat",
        chat_type=ChatType.GROUP,
        content="/a 600519",
        raw_data={},
    )
    dingtalk = NotificationService(source_message=dingtalk_message, outbox_store=outbox)
    dingtalk_context = _job_context(
        isolated_db,
        task_id="dingtalk-context",
        notify=True,
        now=now,
    )
    with bind_durable_execution_context(dingtalk_context):
        unsupported = dingtalk.send_with_results("context report")
    assert unsupported.status == "unsupported_context"
    assert unsupported.channel_results[0].retryable is False
    assert len(_rows(isolated_db)) == 1


def test_dispatcher_partial_success_retries_only_failed_channel_and_honors_retry_after(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 12, 0, 0)
    config = _config(
        wechat_webhook_url="https://example.invalid/wechat",
        email_sender="owner@example.invalid",
        email_password="smtp-secret",
        email_receivers=["owner@example.invalid"],
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    service = NotificationService()
    store = NotificationOutboxStore(isolated_db, lease_seconds=30)
    store.enqueue(_static_request(service, NotificationChannel.WECHAT, logical_id="partial"), now=now)
    store.enqueue(_static_request(service, NotificationChannel.EMAIL, logical_id="partial"), now=now)

    attempts: list[str] = []

    def send(channel, *_args, **_kwargs):
        attempts.append(channel.value)
        if channel == NotificationChannel.EMAIL and attempts.count("email") == 1:
            raise NotificationDeliveryError(
                "provider 429 token=do-not-store",
                error_code="provider_429",
                retryable=True,
                retry_after=10,
            )
        return True

    monkeypatch.setattr(service, "_send_to_static_channel", send)
    health_registry = DurableJobHandlerRegistry()
    health_registry.register("probe", 1, _ProbePayload, lambda payload: payload.value)
    health_recorder = DurableJobStore(health_registry, isolated_db)
    dispatcher = NotificationOutboxDispatcher(
        store,
        worker_id="outbox-worker",
        service_factory=lambda: service,
        health_recorder=health_recorder,
    )

    assert dispatcher.dispatch_once(now=now).status == OUTBOX_STATUS_SENT
    retry = dispatcher.dispatch_once(now=now)
    assert retry.status == OUTBOX_STATUS_PENDING
    assert dispatcher.dispatch_once(now=now + timedelta(seconds=9)).status == "idle"
    assert dispatcher.dispatch_once(now=now + timedelta(seconds=10)).status == OUTBOX_STATUS_SENT
    assert attempts == ["wechat", "email", "email"]

    rows = _rows(isolated_db)
    assert [row.status for row in rows] == [OUTBOX_STATUS_SENT, OUTBOX_STATUS_SENT]
    assert rows[0].attempt == 1
    assert rows[1].attempt == 2
    assert "do-not-store" not in (rows[1].error_message_sanitized or "")
    with isolated_db.get_session() as session:
        health = {
            row.provider_key: row
            for row in session.execute(
                select(ProviderHealthRecord).where(
                    ProviderHealthRecord.kind == "notification"
                )
            ).scalars()
        }
    assert health["wechat"].success_count == 1
    assert health["wechat"].failure_count == 0
    assert health["email"].success_count == 1
    assert health["email"].failure_count == 1


def test_dispatcher_nonretryable_failure_is_terminal_and_not_reclaimed(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 13, 0, 0)
    config = _config(wechat_webhook_url="https://example.invalid/wechat")
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    service = NotificationService()
    store = NotificationOutboxStore(isolated_db, lease_seconds=30)
    store.enqueue(_static_request(service, NotificationChannel.WECHAT, logical_id="bad"), now=now)
    send = mock.Mock(
        side_effect=NotificationDeliveryError(
            "provider rejected recipient",
            error_code="provider_400",
            retryable=False,
        )
    )
    monkeypatch.setattr(service, "_send_to_static_channel", send)
    dispatcher = NotificationOutboxDispatcher(store, service_factory=lambda: service)

    assert dispatcher.dispatch_once(now=now).status == OUTBOX_STATUS_FAILED
    assert dispatcher.dispatch_once(now=now + timedelta(hours=1)).status == "idle"
    assert send.call_count == 1
    assert _rows(isolated_db)[0].status == OUTBOX_STATUS_FAILED


@pytest.mark.parametrize("provider_result", ["http_500", "false"])
def test_dispatcher_ambiguous_provider_result_is_never_resent(
    isolated_db,
    monkeypatch,
    provider_result,
) -> None:
    now = datetime(2026, 8, 8, 13, 30, 0)
    config = _config(wechat_webhook_url="https://example.invalid/wechat")
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    service = NotificationService()
    store = NotificationOutboxStore(isolated_db, lease_seconds=30)
    store.enqueue(
        _static_request(service, NotificationChannel.WECHAT, logical_id=provider_result),
        now=now,
    )
    external_accepts: list[str] = []

    class AcceptedThenHttp500(RuntimeError):
        status_code = 500

    def send(*_args, **_kwargs):
        external_accepts.append(provider_result)
        if provider_result == "http_500":
            raise AcceptedThenHttp500("provider failed after accepting the body")
        return False

    monkeypatch.setattr(service, "_send_to_static_channel", send)
    dispatcher = NotificationOutboxDispatcher(store, service_factory=lambda: service)

    assert dispatcher.dispatch_once(now=now).status == OUTBOX_STATUS_DELIVERY_UNKNOWN
    assert dispatcher.dispatch_once(now=now + timedelta(hours=1)).status == "idle"
    assert external_accepts == [provider_result]
    row = _rows(isolated_db)[0]
    assert row.status == OUTBOX_STATUS_DELIVERY_UNKNOWN
    assert row.attempt == 1


def test_job_bound_outbox_waits_for_success_and_cancels_after_failure(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 13, 45, 0)
    monkeypatch.setattr("src.services.durable_jobs.utc_naive_now", lambda: now)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    store = NotificationOutboxStore(isolated_db, lease_seconds=30)
    service = NotificationService(outbox_store=store)
    low_level = mock.Mock(return_value=True)
    monkeypatch.setattr(service, "_send_to_static_channel", low_level)
    dispatcher = NotificationOutboxDispatcher(store, service_factory=lambda: service)

    succeeds = _job_context(isolated_db, task_id="parent-succeeds", notify=True, now=now)
    with bind_durable_execution_context(succeeds):
        assert service.send_with_results(
            "success report",
            route_type="report",
            dedup_key="parent-succeeds",
        ).status == "queued"
    assert dispatcher.dispatch_once(now=now).status == "idle"
    low_level.assert_not_called()

    succeeds.store.complete(
        succeeds.job_id,
        succeeds.worker_id,
        succeeds.lease_token,
        {"ok": True},
        now=now,
    )
    assert dispatcher.dispatch_once(now=now).status == OUTBOX_STATUS_SENT
    low_level.assert_called_once()

    fails = _job_context(isolated_db, task_id="parent-fails", notify=True, now=now)
    with bind_durable_execution_context(fails):
        assert service.send_with_results(
            "failed report",
            route_type="report",
            dedup_key="parent-fails",
        ).status == "queued"
    fails.store.fail(
        fails.job_id,
        fails.worker_id,
        fails.lease_token,
        "terminal_failure",
        "Parent failed.",
        retryable=False,
        now=now,
    )
    assert dispatcher.dispatch_once(now=now).status == "idle"
    assert low_level.call_count == 1
    failed_row = _rows(isolated_db)[-1]
    assert failed_row.job_id == fails.job_id
    assert failed_row.status == OUTBOX_STATUS_CANCELLED
    assert failed_row.attempt == 0


def test_durable_noise_claim_retries_concurrent_job_and_releases_failed_parent(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 13, 50, 0)
    monkeypatch.setattr("src.services.durable_jobs.utc_naive_now", lambda: now)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
        notification_dedup_ttl_seconds=3600,
        notification_cooldown_seconds=3600,
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    store = NotificationOutboxStore(isolated_db, lease_seconds=30)
    service = NotificationService(outbox_store=store)

    first = _job_context(isolated_db, task_id="noise-first", notify=True, now=now)
    concurrent = _job_context(
        isolated_db,
        task_id="noise-concurrent",
        notify=True,
        now=now,
    )
    with bind_durable_execution_context(first):
        assert service.send_with_results(
            "first noise report",
            route_type="report",
            dedup_key="shared-durable-noise",
            cooldown_key="shared-durable-noise",
        ).status == "queued"
    with bind_durable_execution_context(concurrent):
        with pytest.raises(DurableNoiseClaimBusyError):
            service.send_with_results(
                "concurrent noise report",
                route_type="report",
                dedup_key="shared-durable-noise",
                cooldown_key="shared-durable-noise",
            )
    assert len(_rows(isolated_db)) == 1

    concurrent.store.fail(
        concurrent.job_id,
        concurrent.worker_id,
        concurrent.lease_token,
        "resource_busy",
        "Another durable notification plan is still active.",
        retryable=True,
        retry_after=200,
        now=now,
    )

    first.store.fail(
        first.job_id,
        first.worker_id,
        first.lease_token,
        "terminal_failure",
        "Parent failed before delivery.",
        retryable=False,
        now=now,
    )
    retry_claim = concurrent.store.claim_next(
        "noise-retry-worker",
        now=now + timedelta(seconds=200),
    )
    assert retry_claim is not None
    retry_context = DurableExecutionContext(
        store=concurrent.store,
        claimed_job=retry_claim,
        query_id="query-noise-concurrent-retry",
        trace_id=retry_claim.trace_id,
        cancel_requested=threading.Event(),
        lease_lost=threading.Event(),
    )
    with bind_durable_execution_context(retry_context):
        accepted = service.send_with_results(
            "replacement noise report",
            route_type="report",
            dedup_key="shared-durable-noise",
            cooldown_key="shared-durable-noise",
        )
    assert accepted.status == "queued"
    assert {row.job_id for row in _rows(isolated_db)} == {
        first.job_id,
        concurrent.job_id,
    }


def test_durable_plan_does_not_treat_uncommitted_process_reservation_as_suppression(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 13, 55, 0)
    monkeypatch.setattr("src.services.durable_jobs.utc_naive_now", lambda: now)
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
        notification_dedup_ttl_seconds=3600,
        notification_cooldown_seconds=3600,
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    store = NotificationOutboxStore(isolated_db)
    service = NotificationService(outbox_store=store)
    reservation = service.evaluate_noise_control(
        "reserved report",
        route_type="report",
        dedup_key="precommit-race",
        cooldown_key="precommit-race",
    )
    assert reservation.should_send is True
    assert reservation.dedup_reserved is True

    context = _job_context(
        isolated_db,
        task_id="reservation-is-not-authority",
        notify=True,
        now=now,
    )
    try:
        with bind_durable_execution_context(context):
            planned = service.send_with_results(
                "reserved report",
                route_type="report",
                dedup_key="precommit-race",
                cooldown_key="precommit-race",
            )
    finally:
        service.release_noise_control(reservation)

    assert planned.status == "queued"
    assert len(_rows(isolated_db)) == 1


def test_kill_after_send_expires_to_delivery_unknown_without_resend(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 14, 0, 0)
    config = _config(wechat_webhook_url="https://example.invalid/wechat")
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    service = NotificationService()
    store = NotificationOutboxStore(isolated_db, lease_seconds=5)
    store.enqueue(_static_request(service, NotificationChannel.WECHAT, logical_id="crash"), now=now)
    send = mock.Mock(return_value=True)
    monkeypatch.setattr(service, "_send_to_static_channel", send)

    def crash_after_send(_claimed) -> None:
        raise _SimulatedProcessCrash()

    crashing = NotificationOutboxDispatcher(
        store,
        worker_id="crashing-worker",
        service_factory=lambda: service,
        after_external_send=crash_after_send,
    )
    with pytest.raises(_SimulatedProcessCrash):
        crashing.dispatch_once(now=now)
    assert _rows(isolated_db)[0].status == OUTBOX_STATUS_PROCESSING

    restarted = NotificationOutboxDispatcher(
        store,
        worker_id="replacement-worker",
        service_factory=lambda: service,
    )
    assert restarted.dispatch_once(now=now + timedelta(seconds=6)).status == "idle"
    row = _rows(isolated_db)[0]
    assert row.status == OUTBOX_STATUS_DELIVERY_UNKNOWN
    assert row.attempt == 1
    assert send.call_count == 1


def test_dispatcher_refuses_changed_recipient_without_external_send(
    isolated_db,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 8, 15, 0, 0)
    initial = _config(wechat_webhook_url="https://example.invalid/old-target")
    monkeypatch.setattr("src.notification.get_config", lambda: initial)
    planner_service = NotificationService()
    store = NotificationOutboxStore(isolated_db)
    store.enqueue(
        _static_request(planner_service, NotificationChannel.WECHAT, logical_id="target-change"),
        now=now,
    )

    changed = _config(wechat_webhook_url="https://example.invalid/new-target")
    monkeypatch.setattr("src.notification.get_config", lambda: changed)
    delivery_service = NotificationService()
    send = mock.Mock(return_value=True)
    monkeypatch.setattr(delivery_service, "_send_to_static_channel", send)
    result = NotificationOutboxDispatcher(
        store,
        service_factory=lambda: delivery_service,
    ).dispatch_once(now=now)

    assert result.status == OUTBOX_STATUS_FAILED
    assert _rows(isolated_db)[0].error_code == "recipient_configuration_changed"
    send.assert_not_called()


def test_durable_worker_drains_queued_notification_in_the_same_process(
    isolated_db,
    monkeypatch,
) -> None:
    config = _config(
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    outbox = NotificationOutboxStore(isolated_db, lease_seconds=30)
    service = NotificationService(outbox_store=outbox)
    low_level = mock.Mock(return_value=True)
    monkeypatch.setattr(service, "_send_to_static_channel", low_level)

    def handler(_payload: _ProbePayload) -> dict[str, bool]:
        planned = service.send_with_results(
            "worker report",
            route_type="report",
            dedup_key="worker-report",
        )
        assert planned.status == "queued"
        return {"notification_queued": True}

    registry = DurableJobHandlerRegistry()
    registry.register("probe", 1, _ProbePayload, handler)
    job_store = DurableJobStore(
        registry,
        isolated_db,
        lease_seconds=30,
        heartbeat_seconds=5,
    )
    job_store.enqueue(
        JobEnqueueRequest(
            task_id="worker-notification",
            job_type="probe",
            payload={"value": "ok"},
            notify=True,
        )
    )
    dispatcher = NotificationOutboxDispatcher(
        outbox,
        worker_id="same-process-worker:outbox",
        service_factory=lambda: service,
        health_recorder=job_store,
    )
    worker = DurableWorker(
        job_store,
        worker_id="same-process-worker",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=5,
        outbox_dispatcher=dispatcher,
    )

    worker.run_until_idle()

    rows = _rows(isolated_db)
    assert len(rows) == 1
    assert rows[0].status == OUTBOX_STATUS_SENT
    low_level.assert_called_once()
    with isolated_db.get_session() as session:
        health = session.execute(
            select(ProviderHealthRecord).where(
                ProviderHealthRecord.kind == "notification",
                ProviderHealthRecord.provider_key == "wechat",
                ProviderHealthRecord.scope == "report",
            )
        ).scalar_one()
    assert health.success_count == 1
    assert health.failure_count == 0


def test_dispatcher_exception_is_fail_open_for_worker_job_execution(isolated_db) -> None:
    handled: list[str] = []
    registry = DurableJobHandlerRegistry()
    registry.register(
        "probe",
        1,
        _ProbePayload,
        lambda payload: handled.append(payload.value) or {"done": True},
    )
    job_store = DurableJobStore(
        registry,
        isolated_db,
        lease_seconds=30,
        heartbeat_seconds=5,
    )
    job_store.enqueue(
        JobEnqueueRequest(
            task_id="worker-survives-outbox",
            job_type="probe",
            payload={"value": "completed"},
            notify=False,
        )
    )

    class FailingDispatcher:
        attempts = 0

        def dispatch_once(self):
            self.attempts += 1
            raise RuntimeError("webhook=https://secret.invalid/hook")

        @staticmethod
        def dispatch_available(*, max_messages):
            assert max_messages == 16
            return []

    failing_dispatcher = FailingDispatcher()
    worker = DurableWorker(
        job_store,
        worker_id="fail-open-worker",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=5,
        outbox_dispatcher=failing_dispatcher,
    )

    worker.run_until_idle()

    assert handled == ["completed"]
    assert failing_dispatcher.attempts >= 1
    assert job_store.get_job("worker-survives-outbox").status == JOB_STATUS_SUCCEEDED
