"""Lease-fenced, retry-safe persistence for durable analysis side effects."""

from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.config import Config
from src.services.durable_job_handlers import (
    DurableExecutionContext,
    bind_durable_execution_context,
    build_default_durable_job_registry,
)
from src.services.durable_jobs import DurableJobStore, JobEnqueueRequest, StaleLeaseError
from src.services.decision_signal_service import DecisionSignalService
from src.repositories.decision_signal_repo import DecisionSignalRepository
from src.storage import (
    AnalysisHistory,
    AnalysisJobRecord,
    DatabaseManager,
    DecisionSignalRecord,
)


@pytest.fixture()
def durable_db(tmp_path):
    previous_path = os.environ.get("DATABASE_PATH")
    previous_mode = os.environ.get("DATABASE_MIGRATION_MODE")
    os.environ["DATABASE_PATH"] = str(tmp_path / "durable-side-effects.db")
    os.environ["DATABASE_MIGRATION_MODE"] = "auto"
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if previous_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = previous_path
        if previous_mode is None:
            os.environ.pop("DATABASE_MIGRATION_MODE", None)
        else:
            os.environ["DATABASE_MIGRATION_MODE"] = previous_mode


class _AnalysisResultFixture(SimpleNamespace):
    def to_dict(self):
        return {
            key: value
            for key, value in vars(self).items()
            if not key.startswith("_")
        }


def _analysis_result(*, summary="fixture", action="hold"):
    return _AnalysisResultFixture(
        code="600519",
        name="Kweichow Moutai",
        sentiment_score=80,
        operation_advice=action,
        action=action,
        trend_prediction="up",
        analysis_summary=summary,
        data_sources="fixture",
        raw_response=None,
    )


def test_history_write_reuses_job_row_and_rejects_stale_lease(durable_db) -> None:
    registry = build_default_durable_job_registry()
    store = DurableJobStore(registry, durable_db, lease_seconds=90, heartbeat_seconds=15)
    store.enqueue(
        JobEnqueueRequest(
            task_id="job-history",
            job_type="stock_analysis",
            payload={"stock_code": "600519", "notify": False},
            trace_id="trace-history",
        )
    )
    claimed = store.claim_next("worker-history")
    assert claimed is not None
    context = DurableExecutionContext(
        store=store,
        claimed_job=claimed,
        query_id=claimed.task_id,
        trace_id=claimed.trace_id,
        cancel_requested=threading.Event(),
        lease_lost=threading.Event(),
    )

    first_result = _analysis_result(summary="first-attempt", action="hold")
    retry_result = _analysis_result(summary="second-attempt", action="sell")
    with bind_durable_execution_context(context):
        first_id = durable_db.save_analysis_history(
            result=first_result,
            query_id="query-first-attempt",
            report_type="detailed",
            news_content=None,
            context_snapshot={"attempt": "first"},
        )
        retry_id = durable_db.save_analysis_history(
            result=retry_result,
            query_id="query-retry",
            report_type="detailed",
            news_content=None,
            context_snapshot={"attempt": "second"},
        )

    assert first_id > 0
    assert retry_id == first_id
    assert retry_result.analysis_summary == "first-attempt"
    assert retry_result.operation_advice == "hold"
    assert retry_result.action == "hold"
    assert retry_result.diagnostic_context_snapshot == {"attempt": "first"}
    assert retry_result._durable_history_reused is True
    signal_service = DecisionSignalService(db_manager=durable_db)
    signal_payload = {
        "stock_code": "600519",
        "stock_name": "Kweichow Moutai",
        "market": "cn",
        "source_type": "analysis",
        "source_report_id": first_id,
        "trace_id": "trace-history",
        "market_phase": "postmarket",
        "trigger_source": "durable_worker",
        "action": "hold",
        "confidence": 0.7,
        "score": 70,
        "horizon": "10d",
        "reason": "fixture",
    }
    with bind_durable_execution_context(context):
        first_signal = signal_service.create_signal_with_outcome(signal_payload)
        retry_signal = signal_service.create_signal_with_outcome(
            {
                **signal_payload,
                "action": "sell",
                "horizon": "1d",
                "market_phase": "intraday",
                "reason": "changed retry output",
            }
        )

    assert first_signal.created is True
    assert retry_signal.duplicate is True
    assert retry_signal.item["id"] == first_signal.item["id"]
    with durable_db.get_session() as session:
        rows = session.query(AnalysisHistory).all()
        assert len(rows) == 1
        assert rows[0].job_id == "job-history"
        signals = session.query(DecisionSignalRecord).all()
        assert len(signals) == 1
        assert signals[0].idempotency_key is not None
        assert signals[0].idempotency_key.startswith("job:job-history:")
        job = session.get(AnalysisJobRecord, "job-history")
        assert job is not None
        job.lease_token = "replacement-lease"
        session.commit()

    with bind_durable_execution_context(context), pytest.raises(
        StaleLeaseError,
        match="rejected after lease loss",
    ):
        durable_db.save_analysis_history(
            result=_analysis_result(),
            query_id="query-stale-worker",
            report_type="brief",
            news_content=None,
        )
    with bind_durable_execution_context(context):
        with pytest.raises(StaleLeaseError):
            signal_service.create_signal(
                {**signal_payload, "action": "sell", "reason": "stale fixture"}
            )

    with durable_db.get_session() as session:
        assert session.query(AnalysisHistory).count() == 1
        assert session.query(DecisionSignalRecord).count() == 1


def test_durable_history_write_failure_is_not_reported_as_success(durable_db) -> None:
    store = DurableJobStore(
        build_default_durable_job_registry(),
        durable_db,
        lease_seconds=90,
        heartbeat_seconds=15,
    )
    store.enqueue(
        JobEnqueueRequest(
            task_id="job-history-failure",
            job_type="stock_analysis",
            payload={"stock_code": "600519", "notify": False},
            trace_id="trace-history-failure",
        )
    )
    claimed = store.claim_next("worker-history-failure")
    assert claimed is not None
    context = DurableExecutionContext(
        store=store,
        claimed_job=claimed,
        query_id=claimed.task_id,
        trace_id=claimed.trace_id,
        cancel_requested=threading.Event(),
        lease_lost=threading.Event(),
    )

    with bind_durable_execution_context(context), patch.object(
        durable_db,
        "_run_write_transaction",
        side_effect=RuntimeError("history write failed"),
    ), pytest.raises(RuntimeError, match="history write failed"):
        durable_db.save_analysis_history(
            result=_analysis_result(),
            query_id=claimed.task_id,
            report_type="detailed",
            news_content=None,
        )


def test_lease_recovery_freezes_job_history_and_signal_to_first_result(durable_db) -> None:
    store = DurableJobStore(
        build_default_durable_job_registry(),
        durable_db,
        lease_seconds=90,
        heartbeat_seconds=15,
    )
    store.enqueue(
        JobEnqueueRequest(
            task_id="job-retry-convergence",
            job_type="stock_analysis",
            payload={"stock_code": "600519", "notify": False},
            trace_id="trace-retry-convergence",
        )
    )
    first_claim = store.claim_next("worker-first")
    assert first_claim is not None
    first_context = DurableExecutionContext(
        store=store,
        claimed_job=first_claim,
        query_id=first_claim.task_id,
        trace_id=first_claim.trace_id,
        cancel_requested=threading.Event(),
        lease_lost=threading.Event(),
    )
    signal_service = DecisionSignalService(db_manager=durable_db)
    first_result = _analysis_result(summary="first-attempt", action="hold")

    with bind_durable_execution_context(first_context):
        history_id = durable_db.save_analysis_history(
            result=first_result,
            query_id=first_claim.task_id,
            report_type="detailed",
            news_content=None,
            context_snapshot={"attempt": "first"},
        )
        first_signal = signal_service.create_signal_with_outcome(
            {
                "stock_code": "600519",
                "market": "cn",
                "source_type": "analysis",
                "source_report_id": history_id,
                "market_phase": "postmarket",
                "trigger_source": "durable_worker",
                "action": first_result.action,
                "confidence": 0.7,
                "score": 70,
                "horizon": "10d",
                "reason": first_result.analysis_summary,
            }
        )
    assert first_signal.created is True

    with durable_db.get_session() as session:
        job = session.get(AnalysisJobRecord, first_claim.task_id)
        assert job is not None
        job.lease_expires_at = (
            datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=1)
        )
        session.commit()

    retry_claim = store.claim_next("worker-retry")
    assert retry_claim is not None
    assert retry_claim.attempt == 2
    retry_context = DurableExecutionContext(
        store=store,
        claimed_job=retry_claim,
        query_id=retry_claim.task_id,
        trace_id=retry_claim.trace_id,
        cancel_requested=threading.Event(),
        lease_lost=threading.Event(),
    )
    retry_result = _analysis_result(summary="second-attempt", action="sell")

    with bind_durable_execution_context(retry_context):
        retry_history_id = durable_db.save_analysis_history(
            result=retry_result,
            query_id=retry_claim.task_id,
            report_type="detailed",
            news_content=None,
            context_snapshot={"attempt": "second"},
        )
        retry_signal = signal_service.create_signal_with_outcome(
            {
                "stock_code": "600519",
                "market": "cn",
                "source_type": "analysis",
                "source_report_id": retry_history_id,
                "market_phase": "postmarket",
                "trigger_source": "durable_worker",
                "action": retry_result.action,
                "confidence": 0.7,
                "score": 70,
                "horizon": "10d",
                "reason": retry_result.analysis_summary,
            }
        )
        store.complete(
            retry_claim.task_id,
            retry_claim.worker_id,
            retry_claim.lease_token,
            retry_result.to_dict(),
        )

    assert retry_history_id == history_id
    assert retry_signal.duplicate is True
    job_snapshot = store.get_job(retry_claim.task_id)
    assert job_snapshot is not None
    assert job_snapshot.result["analysis_summary"] == "first-attempt"
    assert job_snapshot.result["action"] == "hold"
    with durable_db.get_session() as session:
        histories = session.query(AnalysisHistory).all()
        signals = session.query(DecisionSignalRecord).all()
        assert len(histories) == 1
        assert histories[0].analysis_summary == "first-attempt"
        assert len(signals) == 1
        assert signals[0].action == "hold"


def test_signal_repository_fences_stale_lease_inside_write_transaction(durable_db) -> None:
    registry = build_default_durable_job_registry()
    store = DurableJobStore(registry, durable_db, lease_seconds=90, heartbeat_seconds=15)
    store.enqueue(
        JobEnqueueRequest(
            task_id="job-signal-fence",
            job_type="stock_analysis",
            payload={"stock_code": "600519", "notify": False},
            trace_id="trace-signal-fence",
        )
    )
    claimed = store.claim_next("worker-signal-fence")
    assert claimed is not None
    context = DurableExecutionContext(
        store=store,
        claimed_job=claimed,
        query_id=claimed.task_id,
        trace_id=claimed.trace_id,
        cancel_requested=threading.Event(),
        lease_lost=threading.Event(),
    )
    fields = {
        "stock_code": "600519",
        "stock_name": "Kweichow Moutai",
        "market": "cn",
        "source_type": "analysis",
        "source_report_id": 1,
        "trace_id": "trace-signal-fence",
        "decision_profile": "default",
        "market_phase": "postmarket",
        "trigger_source": "durable_worker",
        "action": "hold",
        "confidence": 0.7,
        "score": 70.0,
        "horizon": "10d",
        "reason": "fixture",
        "status": "active",
        "idempotency_key": "job:job-signal-fence:fixture",
    }

    # Simulate the narrow race after a service checkpoint but before the
    # repository transaction begins: a new owner has replaced the token.
    with durable_db.get_session() as session:
        job = session.get(AnalysisJobRecord, claimed.task_id)
        assert job is not None
        job.lease_owner = "replacement-worker"
        job.lease_token = "replacement-token"
        session.commit()

    repo = DecisionSignalRepository(durable_db)
    with bind_durable_execution_context(context):
        with pytest.raises(StaleLeaseError, match="rejected after lease loss"):
            repo.create_if_absent(fields)

    with durable_db.get_session() as session:
        assert session.query(DecisionSignalRecord).count() == 0
