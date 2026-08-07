"""Facade contracts for switching AnalysisTaskQueue to durable SQLite jobs."""

from __future__ import annotations

from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.config import Config
from src.services.durable_jobs import InvalidJobPayloadError
from src.services.task_queue import AnalysisTaskQueue, TaskInfo, TaskStatus
from src.storage import DatabaseManager


@pytest.fixture()
def durable_queue(tmp_path, monkeypatch):
    previous_queue = AnalysisTaskQueue._instance
    AnalysisTaskQueue._instance = None
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "durable-facade.db"))
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    monkeypatch.setenv("DURABLE_JOBS_ENABLED", "true")
    Config.reset_instance()
    DatabaseManager.reset_instance()

    queue = AnalysisTaskQueue(max_workers=3)
    try:
        yield queue
    finally:
        queue.shutdown()
        AnalysisTaskQueue._instance = previous_queue
        DatabaseManager.reset_instance()
        Config.reset_instance()


def test_flag_off_keeps_executor_submission_contract() -> None:
    previous_queue = AnalysisTaskQueue._instance
    AnalysisTaskQueue._instance = None

    class CapturingExecutor:
        def __init__(self) -> None:
            self.calls = []

        def submit(self, fn, *args, **kwargs):
            self.calls.append((fn, args, kwargs))
            return Future()

    try:
        with patch(
            "src.config.get_config",
            return_value=SimpleNamespace(durable_jobs_enabled=False),
        ):
            queue = AnalysisTaskQueue(max_workers=1)
        executor = CapturingExecutor()
        queue._executor = executor

        accepted, duplicates = queue.submit_tasks_batch(["600519"])

        assert queue.durable_enabled is False
        assert duplicates == []
        assert accepted[0].status == TaskStatus.PENDING
        assert len(executor.calls) == 1
        assert queue._durable_store is None
    finally:
        AnalysisTaskQueue._instance = previous_queue


def test_flag_off_cancel_only_succeeds_before_legacy_callable_starts() -> None:
    previous_queue = AnalysisTaskQueue._instance
    AnalysisTaskQueue._instance = None
    try:
        with patch(
            "src.config.get_config",
            return_value=SimpleNamespace(durable_jobs_enabled=False),
        ):
            queue = AnalysisTaskQueue(max_workers=1)

        pending = TaskInfo(task_id="pending", stock_code="600519")
        pending_future = Future()
        queue._tasks[pending.task_id] = pending
        queue._futures[pending.task_id] = pending_future
        queue._analyzing_stocks["600519"] = pending.task_id

        cancelled = queue.cancel_task(pending.task_id)

        assert cancelled is not None
        assert cancelled.status == TaskStatus.CANCELLED
        assert pending_future.cancelled()
        assert "600519" not in queue._analyzing_stocks

        processing = TaskInfo(
            task_id="processing",
            stock_code="000001",
            status=TaskStatus.PROCESSING,
        )
        running_future = Future()
        assert running_future.set_running_or_notify_cancel() is True
        queue._tasks[processing.task_id] = processing
        queue._futures[processing.task_id] = running_future
        queue._analyzing_stocks["000001"] = processing.task_id

        with pytest.raises(RuntimeError, match="不支持中途取消"):
            queue.cancel_task(processing.task_id)

        assert queue.get_task(processing.task_id).status == TaskStatus.PROCESSING
        assert queue._analyzing_stocks["000001"] == processing.task_id
    finally:
        AnalysisTaskQueue._instance = previous_queue


def test_durable_flag_is_restart_latched_and_never_creates_executor(
    durable_queue,
    monkeypatch,
) -> None:
    original_store = durable_queue._durable_store
    monkeypatch.setenv("DURABLE_JOBS_ENABLED", "false")
    Config.reset_instance()

    same_queue = AnalysisTaskQueue(max_workers=99)
    sync_result = same_queue.sync_max_workers(2)

    assert same_queue is durable_queue
    assert same_queue.durable_enabled is True
    assert same_queue._durable_store is original_store
    assert same_queue._executor is None
    assert sync_result == "applied"
    assert same_queue._executor is None
    with pytest.raises(RuntimeError, match="do not execute callables"):
        _ = same_queue.executor


def test_durable_stock_batch_only_inserts_strict_complete_payload(durable_queue) -> None:
    portfolio = {"account_id": 7, "quantity": 100}
    skills = ["growth_quality"]

    with patch("src.services.analysis_service.AnalysisService") as analysis_service:
        accepted, duplicates = durable_queue.submit_tasks_batch(
            ["600519"],
            stock_name="贵州茅台",
            original_query="看看茅台",
            selection_source="autocomplete",
            query_source="portfolio",
            portfolio_context=portfolio,
            report_type="detailed",
            analysis_phase="intraday",
            force_refresh=True,
            notify=False,
            skills=skills,
            report_language="zh",
        )
    analysis_service.assert_not_called()
    portfolio["quantity"] = 999
    skills.append("mutated")

    assert duplicates == []
    assert len(accepted) == 1
    assert durable_queue._executor is None
    snapshot = durable_queue._durable_store.get_job(accepted[0].task_id)
    assert snapshot.status == "pending"
    assert snapshot.started_at is None
    assert snapshot.result is None
    assert snapshot.notify is False
    assert snapshot.payload == {
        "analysis_phase": "intraday",
        "force_refresh": True,
        "notify": False,
        "original_query": "看看茅台",
        "portfolio_context": {"account_id": 7, "quantity": 100},
        "query_source": "portfolio",
        "report_language": "zh",
        "report_type": "detailed",
        "selection_source": "autocomplete",
        "skills": ["growth_quality"],
        "stock_code": "600519",
        "stock_name": "贵州茅台",
    }
    assert snapshot.trace_id == accepted[0].task_id


def test_durable_batch_validation_rolls_back_all_rows(durable_queue, monkeypatch) -> None:
    registry = durable_queue._durable_store.registry
    original_encode = registry.encode_payload
    call_count = 0

    def fail_second(job_type, payload_version, payload):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise InvalidJobPayloadError("second payload rejected")
        return original_encode(job_type, payload_version, payload)

    monkeypatch.setattr(registry, "encode_payload", fail_second)

    with pytest.raises(InvalidJobPayloadError, match="second payload rejected"):
        durable_queue.submit_tasks_batch(["600519", "000858"])

    assert durable_queue.list_all_tasks() == []
    assert durable_queue.durable_event_high_water() == 0


def test_durable_active_stock_deduplicates_aliases_and_request_metadata(
    durable_queue,
) -> None:
    accepted, duplicates = durable_queue.submit_tasks_batch(
        ["600519"],
        original_query="first",
    )
    accepted_again, duplicates_again = durable_queue.submit_tasks_batch(
        ["600519.SH", "600519"],
        original_query="different metadata must still dedupe",
        analysis_phase="intraday",
    )

    assert duplicates == []
    assert accepted_again == []
    assert len(duplicates_again) == 2
    assert {item.existing_task_id for item in duplicates_again} == {
        accepted[0].task_id
    }
    assert durable_queue.is_analyzing("600519.SH") is True
    assert durable_queue.get_analyzing_task_id("600519") == accepted[0].task_id


def test_durable_jobs_remain_readable_after_queue_singleton_restart(
    durable_queue,
) -> None:
    task = durable_queue.submit_task(
        "600519",
        original_query="restart me",
        selection_source="manual",
        skills=["bull_trend"],
    )
    original_store = durable_queue._durable_store
    AnalysisTaskQueue._instance = None

    restarted = AnalysisTaskQueue(max_workers=1)
    restored = restarted.get_task(task.task_id)

    assert restarted is not durable_queue
    assert restarted._durable_store is not original_store
    assert restored is not None
    assert restored.task_id == task.task_id
    assert restored.status == TaskStatus.PENDING
    assert restored.original_query == "restart me"
    assert restored.selection_source == "manual"
    assert restored.skills == ["bull_trend"]
    assert restarted._executor is None


def test_durable_status_result_error_region_and_stats_mapping(durable_queue) -> None:
    completed = durable_queue.submit_task("600519")
    claimed = durable_queue._durable_store.claim_next("worker-complete")
    durable_queue._durable_store.complete(
        claimed.task_id,
        claimed.worker_id,
        claimed.lease_token,
        {"stock_name": "贵州茅台", "score": 88},
    )

    completed_state = durable_queue.get_task(completed.task_id)
    assert completed_state.status == TaskStatus.COMPLETED
    assert completed_state.result == {"stock_name": "贵州茅台", "score": 88}
    assert completed_state.stock_name == "贵州茅台"
    assert "lease_token" not in repr(completed_state.to_dict())

    failed = durable_queue.submit_task("000858")
    failed_claim = durable_queue._durable_store.claim_next("worker-fail")
    durable_queue._durable_store.fail(
        failed_claim.task_id,
        failed_claim.worker_id,
        failed_claim.lease_token,
        "provider_error",
        "token=super-secret provider failed",
        retryable=False,
    )
    failed_state = durable_queue.get_task(failed.task_id)
    assert failed_state.status == TaskStatus.FAILED
    assert "super-secret" not in failed_state.error
    assert "[REDACTED]" in failed_state.error

    review = durable_queue.submit_typed_job(
        "market_review",
        {
            "region": "cn,hk",
            "send_notification": False,
            "merge_notification": False,
            "save_report_file": True,
            "persist_history": True,
            "trigger_source": "api",
        },
        stock_code="market_review",
        stock_name="大盘复盘",
        region="cn,hk",
        notify=False,
    )
    assert review.region == "cn,hk"
    assert review.query_source == "api"

    stats = durable_queue.get_task_stats()
    assert stats["total"] == 3
    assert stats["completed"] == 1
    assert stats["failed"] == 1
    assert stats["pending"] == 1


def test_durable_rejects_callable_submission_and_cancels_pending(durable_queue) -> None:
    with pytest.raises(RuntimeError, match="submit_typed_job"):
        durable_queue.submit_background_task(
            lambda: None,
            stock_code="background",
        )

    task = durable_queue.submit_task("600519")
    cancelled = durable_queue.cancel_task(task.task_id)

    assert cancelled.status == TaskStatus.CANCELLED
    assert cancelled.completed_at is not None
    assert durable_queue.list_pending_tasks() == []
    assert durable_queue.cancel_task("missing") is None


def test_durable_flow_and_cursor_facade_are_detached_and_ordered(durable_queue) -> None:
    task = durable_queue.submit_task("600519")
    claimed = durable_queue._durable_store.claim_next("worker-flow")
    durable_queue._durable_store.append_flow_event(
        claimed.task_id,
        claimed.worker_id,
        claimed.lease_token,
        {"id": "provider-1", "type": "provider_run"},
    )

    events = durable_queue.read_durable_events(after_id=0, limit=100)
    flow = durable_queue.get_task_flow_events(task.task_id)

    assert [event.id for event in events] == sorted(event.id for event in events)
    assert durable_queue.durable_event_min_id() == events[0].id
    assert durable_queue.durable_event_high_water() == events[-1].id
    assert flow == [{"id": "provider-1", "type": "provider_run"}]
    flow[0]["id"] = "mutated"
    assert durable_queue.get_task_flow_events(task.task_id)[0]["id"] == "provider-1"


def test_durable_event_facade_rejects_memory_backend() -> None:
    previous_queue = AnalysisTaskQueue._instance
    AnalysisTaskQueue._instance = None
    try:
        with patch(
            "src.config.get_config",
            return_value=SimpleNamespace(durable_jobs_enabled=False),
        ):
            queue = AnalysisTaskQueue(max_workers=1)
        with pytest.raises(RuntimeError, match="DURABLE_JOBS_ENABLED"):
            queue.read_durable_events()
    finally:
        AnalysisTaskQueue._instance = previous_queue
