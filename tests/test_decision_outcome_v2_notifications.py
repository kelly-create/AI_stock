"""Durable summary-notification contracts for Decision Outcome v2."""

from __future__ import annotations

import json
import os
import threading

import pytest
from sqlalchemy import select

from src.config import Config
from src.services.durable_job_handlers import (
    DecisionOutcomesV2Payload,
    DurableExecutionContext,
    _decision_outcomes_v2_handler,
    bind_durable_execution_context,
    build_default_durable_job_registry,
)
from src.services.durable_jobs import DurableJobStore, JobEnqueueRequest
from src.services.durable_worker import DurableWorker
from src.storage import DatabaseManager, NotificationOutboxRecord, utc_naive_now


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    old_migration_mode = os.environ.get("DATABASE_MIGRATION_MODE")
    os.environ["DATABASE_PATH"] = str(tmp_path / "outcome-v2-notifications.db")
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


class _OutcomeService:
    calls = 0

    def run_outcomes(self, **_kwargs):
        type(self).calls += 1
        return {
            "contract": "decision-outcome-v2-run",
            "version": "v1",
            "engine_version": "personal-research-outcome-v2",
            "horizons": ["5d"],
            "selected": 2,
            "created": 1,
            "updated": 0,
            "transitioned": 1,
            "unchanged": 0,
            "items": [
                {"signal_id": 41, "eval_status": "pending"},
                {"signal_id": 42, "eval_status": "evaluated"},
            ],
        }


def _claimed_context(
    db: DatabaseManager,
    *,
    task_id: str,
) -> tuple[DurableJobStore, DurableExecutionContext]:
    store = DurableJobStore(build_default_durable_job_registry(), db)
    now = utc_naive_now()
    store.enqueue(
        JobEnqueueRequest(
            task_id=task_id,
            job_type="decision_outcomes_v2",
            payload={"horizons": ["5d"], "notify": True},
            trace_id=f"trace-{task_id}",
            notify=True,
        ),
        now=now,
    )
    claimed = store.claim_next(
        f"worker-{task_id}",
        now=now,
    )
    assert claimed is not None
    return store, DurableExecutionContext(
        store=store,
        claimed_job=claimed,
        query_id=task_id,
        trace_id=claimed.trace_id,
        cancel_requested=threading.Event(),
        lease_lost=threading.Event(),
    )


def test_summary_notification_is_single_and_outbox_deduplicated(
    isolated_db,
    monkeypatch,
) -> None:
    config = Config(
        stock_list=[],
        durable_jobs_enabled=True,
        wechat_webhook_url="https://example.invalid/wechat",
    )
    monkeypatch.setattr("src.notification.get_config", lambda: config)
    monkeypatch.setattr(
        "src.services.decision_outcome_v2_service.DecisionOutcomeV2Service",
        _OutcomeService,
    )
    _OutcomeService.calls = 0
    _, context = _claimed_context(isolated_db, task_id="outcome-v2-notify")
    payload = DecisionOutcomesV2Payload(horizons=["5d"], notify=True)

    with bind_durable_execution_context(context):
        first = _decision_outcomes_v2_handler(payload)
        replay = _decision_outcomes_v2_handler(payload)

    with isolated_db.get_session() as session:
        rows = list(session.execute(select(NotificationOutboxRecord)).scalars())
    assert first["notification_status"] == "queued"
    assert replay["notification_status"] == "queued"
    assert len(rows) == 1
    assert rows[0].route == "report"
    content = json.loads(rows[0].payload_json)["content"]
    assert content.count("Decision Outcome v2 job summary") == 1
    assert "Status counts: pending=1, evaluated=1" in content
    assert "signal_id" not in content


def test_notification_exception_does_not_fail_completed_outcome_job(
    isolated_db,
    monkeypatch,
) -> None:
    class FailingNotificationService:
        def send_with_results(self, *_args, **_kwargs):
            raise RuntimeError("notification transport exploded")

    monkeypatch.setattr(
        "src.services.decision_outcome_v2_service.DecisionOutcomeV2Service",
        _OutcomeService,
    )
    monkeypatch.setattr(
        "src.notification.NotificationService",
        FailingNotificationService,
    )
    _OutcomeService.calls = 0
    store = DurableJobStore(build_default_durable_job_registry(), isolated_db)
    store.enqueue(
        JobEnqueueRequest(
            task_id="outcome-v2-notify-failure",
            job_type="decision_outcomes_v2",
            payload={"horizons": ["5d"], "notify": True},
            trace_id="trace-outcome-v2-notify-failure",
            notify=True,
        )
    )
    worker = DurableWorker(
        store,
        worker_id="worker-outcome-v2-notify-failure",
        max_workers=1,
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.05,
    )

    worker.run_until_idle()

    completed = store.get_job("outcome-v2-notify-failure")
    assert completed is not None
    assert completed.status == "succeeded"
    assert completed.result["notification_status"] == "failed"
    assert _OutcomeService.calls == 1
