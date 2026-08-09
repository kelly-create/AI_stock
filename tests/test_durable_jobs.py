"""Contract and concurrency tests for the PR1 durable job state machine."""

from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select

from src.config import Config
from src.services.durable_jobs import (
    JOB_STATUS_CANCELLED,
    JOB_STATUS_CANCEL_REQUESTED,
    JOB_STATUS_FAILED,
    JOB_STATUS_PENDING,
    JOB_STATUS_SUCCEEDED,
    OUTBOX_STATUS_DELIVERY_UNKNOWN,
    OUTBOX_STATUS_PENDING,
    OUTBOX_STATUS_SENT,
    DurableJobConflictError,
    DurableJobHandlerRegistry,
    DurableJobStore,
    InvalidJobPayloadError,
    JobEnqueueRequest,
    NotificationOutboxStore,
    OutboxEnqueueRequest,
    StaleLeaseError,
    UnknownJobHandlerError,
    parse_retry_after_seconds,
)
from src.services.research.repositories import LeaseFence, ResearchSnapshotRepository
from src.storage import (
    AnalysisJobRecord,
    DatabaseManager,
    JobEventRecord,
    NotificationOutboxRecord,
    ProviderHealthRecord,
)


class DemoPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stock_code: str
    sequence: int = 0


class UnsafePayload(BaseModel):
    value: object


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    old_migration_mode = os.environ.get("DATABASE_MIGRATION_MODE")
    os.environ["DATABASE_PATH"] = str(tmp_path / "durable-jobs.db")
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


@pytest.fixture()
def registry() -> DurableJobHandlerRegistry:
    value = DurableJobHandlerRegistry()
    value.register("analysis", 1, DemoPayload, lambda payload: payload.stock_code)
    return value


@pytest.fixture()
def store(isolated_db, registry) -> DurableJobStore:
    return DurableJobStore(registry, isolated_db, lease_seconds=90, heartbeat_seconds=15)


def _request(
    stock_code: str,
    *,
    job_type: str = "analysis",
    sequence: int = 0,
    task_id: str | None = None,
    priority: int = 0,
    dedupe_key: str | None = None,
    idempotency_key: str | None = None,
    available_at: datetime | None = None,
    max_attempts: int = 4,
    notify: bool = False,
) -> JobEnqueueRequest:
    return JobEnqueueRequest(
        job_type=job_type,
        payload={"stock_code": stock_code, "sequence": sequence},
        task_id=task_id,
        stock_code=stock_code,
        priority=priority,
        dedupe_key=dedupe_key,
        idempotency_key=idempotency_key,
        available_at=available_at,
        max_attempts=max_attempts,
        notify=notify,
    )


def test_registry_persists_only_strict_versioned_json() -> None:
    registry = DurableJobHandlerRegistry()
    registry.register("unsafe", 1, UnsafePayload, lambda payload: payload.value)

    with pytest.raises(InvalidJobPayloadError, match="callable values are forbidden"):
        registry.encode_payload("unsafe", 1, {"value": lambda: None})
    with pytest.raises(InvalidJobPayloadError, match="non-finite"):
        registry.encode_payload("unsafe", 1, {"value": float("nan")})
    with pytest.raises(UnknownJobHandlerError):
        registry.encode_payload("missing", 1, {})
    with pytest.raises(ValueError, match="already registered"):
        registry.register("unsafe", 1, UnsafePayload, lambda payload: payload.value)

    encoded = registry.encode_payload("unsafe", 1, {"value": {"b": 2, "a": 1}})
    assert encoded == '{"data":{"value":{"a":1,"b":2}},"version":1}'
    assert registry.decode_payload("unsafe", encoded).value == {"a": 1, "b": 2}


def test_batch_enqueue_is_atomic_and_idempotent(store, isolated_db) -> None:
    first = store.enqueue(
        _request("600519", task_id="job-a", dedupe_key="stock:600519", idempotency_key="idem-a")
    )
    duplicate = store.enqueue(
        _request("600519", task_id="ignored", dedupe_key="stock:600519", idempotency_key="idem-a")
    )
    deduplicated = store.enqueue(
        _request("600519", task_id="ignored-too", dedupe_key="stock:600519")
    )

    assert (first.task_id, first.created) == ("job-a", True)
    assert (duplicate.task_id, duplicate.created) == ("job-a", False)
    assert (deduplicated.task_id, deduplicated.created) == ("job-a", False)
    with pytest.raises(DurableJobConflictError):
        store.enqueue(
            _request(
                "600519",
                dedupe_key="stock:600519",
                idempotency_key="idem-a",
                notify=True,
            )
        )

    with pytest.raises(DurableJobConflictError):
        store.enqueue_many(
            [
                _request("000001", task_id="rolled-back", idempotency_key="same-batch"),
                _request("000002", task_id="conflict", idempotency_key="same-batch"),
            ]
        )

    with isolated_db.get_session() as session:
        assert session.get(AnalysisJobRecord, "rolled-back") is None
        assert session.get(AnalysisJobRecord, "conflict") is None
        assert session.scalar(select(func.count()).select_from(AnalysisJobRecord)) == 1
        assert session.scalar(select(func.count()).select_from(JobEventRecord)) == 1


def test_claim_is_priority_fifo_and_concurrent_claims_are_unique(store) -> None:
    created = datetime(2026, 8, 8, 1, 0, 0)
    store.enqueue_many(
        [
            _request("low", task_id="low", priority=0),
            _request("first", task_id="first", priority=5),
            _request("second", task_id="second", priority=5),
        ],
        now=created,
    )
    assert store.claim_next("worker-0", now=created).task_id == "first"
    assert store.claim_next("worker-0", now=created).task_id == "second"
    assert store.claim_next("worker-0", now=created).task_id == "low"

    concurrent_store = store
    requests = [
        _request(str(index), task_id=f"concurrent-{index}", priority=1)
        for index in range(12)
    ]
    concurrent_store.enqueue_many(requests, now=created)

    def claim(index: int) -> str | None:
        result = concurrent_store.claim_next(f"worker-{index}", now=created)
        return result.task_id if result else None

    with ThreadPoolExecutor(max_workers=8) as executor:
        claimed = list(executor.map(claim, range(16)))

    non_null = [task_id for task_id in claimed if task_id is not None]
    assert len(non_null) == 12
    assert len(set(non_null)) == 12


def test_detached_read_models_counts_and_global_event_watermarks(store, isolated_db) -> None:
    first_time = datetime(2026, 8, 8, 1, 30, 0)
    second_time = first_time + timedelta(seconds=1)
    store.enqueue(_request("first", task_id="read-first", notify=True), now=first_time)
    store.enqueue(_request("second", task_id="read-second"), now=second_time)
    claimed = store.claim_next("reader-worker", now=second_time)
    store.complete("read-first", "reader-worker", claimed.lease_token, {"ok": True}, now=second_time)

    snapshot = store.get_job("read-first")
    assert snapshot.task_id == "read-first"
    assert snapshot.status == JOB_STATUS_SUCCEEDED
    assert snapshot.payload == {"stock_code": "first", "sequence": 0}
    assert snapshot.result == {"ok": True}
    assert snapshot.notify is True
    assert not hasattr(snapshot, "lease_token")
    assert store.get_job("missing") is None
    assert [job.task_id for job in store.list_jobs()] == ["read-second", "read-first"]
    assert [job.task_id for job in store.list_active_jobs()] == ["read-second"]
    assert store.count_jobs() == 2
    assert store.count_jobs(statuses=[JOB_STATUS_PENDING]) == 1
    assert store.count_jobs(statuses=[]) == 0
    assert store.get_status_counts() == {JOB_STATUS_PENDING: 1, JOB_STATUS_SUCCEEDED: 1}

    events = store.read_events()
    assert store.get_min_event_id() == events[0].id
    assert store.get_high_water_event_id() == events[-1].id
    assert [event.id for event in store.read_events(after_id=events[0].id)] == [
        event.id for event in events[1:]
    ]

    # Snapshots are detached values, not live ORM instances.
    with isolated_db.session_scope() as session:
        session.get(AnalysisJobRecord, "read-first").message = "changed later"
    assert snapshot.message is None


def test_lease_recovery_fences_old_worker_and_terminal_state_is_immutable(store, isolated_db) -> None:
    started = datetime(2026, 8, 8, 2, 0, 0)
    store.enqueue(_request("600519", task_id="lease-job"), now=started)
    first = store.claim_next("worker-a", now=started)
    assert first.attempt == 1

    with pytest.raises(StaleLeaseError):
        store.heartbeat("lease-job", "worker-b", first.lease_token, now=started + timedelta(seconds=10))
    with pytest.raises(StaleLeaseError):
        store.heartbeat("lease-job", "worker-a", "wrong-token", now=started + timedelta(seconds=10))

    heartbeat = store.heartbeat(
        "lease-job", "worker-a", first.lease_token, now=started + timedelta(seconds=10)
    )
    assert heartbeat.lease_expires_at == started + timedelta(seconds=100)
    assert heartbeat.cancel_requested is False

    recovered_at = started + timedelta(seconds=101)
    assert store.recover_expired_leases(now=recovered_at) == {
        "requeued": 1,
        "failed": 0,
        "cancelled": 0,
    }
    with pytest.raises(StaleLeaseError):
        store.complete("lease-job", "worker-a", first.lease_token, {"stale": True}, now=recovered_at)

    second = store.claim_next("worker-b", now=recovered_at)
    assert second.attempt == 2
    assert second.lease_token != first.lease_token
    assert store.complete(
        "lease-job", "worker-b", second.lease_token, {"ok": True}, now=recovered_at
    ) == JOB_STATUS_SUCCEEDED
    with pytest.raises(StaleLeaseError):
        store.complete("lease-job", "worker-b", second.lease_token, now=recovered_at)
    assert store.cancel("lease-job", now=recovered_at) == JOB_STATUS_SUCCEEDED

    with isolated_db.get_session() as session:
        row = session.get(AnalysisJobRecord, "lease-job")
        assert row.status == JOB_STATUS_SUCCEEDED
        assert row.result_json == '{"ok":true}'
        assert row.attempt == 2


def test_retry_after_seconds_http_date_and_attempt_exhaustion(store, isolated_db) -> None:
    now = datetime(2026, 8, 8, 3, 0, 0)
    http_date = format_datetime((now + timedelta(seconds=45)).replace(tzinfo=timezone.utc), usegmt=True)
    assert parse_retry_after_seconds("7", now=now, attempt=2) == 7
    assert parse_retry_after_seconds(http_date, now=now, attempt=2) == 45
    assert parse_retry_after_seconds(None, now=now, attempt=3) == 8

    store.enqueue(_request("retry", task_id="retry-job", max_attempts=2), now=now)
    first = store.claim_next("worker", now=now)
    assert store.fail(
        "retry-job",
        "worker",
        first.lease_token,
        "rate_limited",
        "Provider asked us to retry.",
        retry_after=http_date,
        now=now,
    ) == JOB_STATUS_PENDING
    assert store.claim_next("worker", now=now + timedelta(seconds=44)) is None

    second = store.claim_next("worker", now=now + timedelta(seconds=45))
    assert second.attempt == 2
    assert store.fail(
        "retry-job",
        "worker",
        second.lease_token,
        "timeout",
        "API key=super-secret provider timeout.",
        retryable=True,
        now=now + timedelta(seconds=45),
    ) == JOB_STATUS_FAILED

    with isolated_db.get_session() as session:
        row = session.get(AnalysisJobRecord, "retry-job")
        assert row.status == JOB_STATUS_FAILED
        assert row.error_code == "timeout"
        assert "super-secret" not in row.error_message_sanitized


def test_pending_and_processing_cancellation_contract(store, isolated_db) -> None:
    now = datetime(2026, 8, 8, 4, 0, 0)
    store.enqueue_many(
        [_request("pending", task_id="pending-cancel"), _request("active", task_id="active-cancel")],
        now=now,
    )
    assert store.cancel("pending-cancel", now=now) == JOB_STATUS_CANCELLED

    claimed = store.claim_next("worker", now=now)
    assert claimed.task_id == "active-cancel"
    assert store.cancel("active-cancel", now=now) == JOB_STATUS_CANCEL_REQUESTED
    heartbeat = store.heartbeat("active-cancel", "worker", claimed.lease_token, now=now)
    assert heartbeat.cancel_requested is True
    assert store.complete(
        "active-cancel", "worker", claimed.lease_token, {"ignored": True}, now=now
    ) == JOB_STATUS_CANCELLED

    with isolated_db.get_session() as session:
        rows = session.execute(
            select(AnalysisJobRecord).order_by(AnalysisJobRecord.task_id)
        ).scalars().all()
        assert [row.status for row in rows] == [JOB_STATUS_CANCELLED, JOB_STATUS_CANCELLED]
        assert all(row.cancel_requested_at == now for row in rows)


def test_flow_event_append_is_double_fenced_and_does_not_mutate_progress(store) -> None:
    now = datetime(2026, 8, 8, 4, 30, 0)
    store.enqueue(
        JobEnqueueRequest(
            job_type="analysis",
            payload={"stock_code": "flow", "sequence": 1},
            task_id="flow-job",
            stage="provider",
            progress=37,
        ),
        now=now,
    )
    claimed = store.claim_next("worker-a", now=now)
    with pytest.raises(StaleLeaseError):
        store.append_flow_event(
            "flow-job", "worker-b", claimed.lease_token, {"type": "provider_run"}, now=now
        )
    with pytest.raises(StaleLeaseError):
        store.append_flow_event(
            "flow-job", "worker-a", "wrong-token", {"type": "provider_run"}, now=now
        )

    appended = store.append_flow_event(
        "flow-job",
        "worker-a",
        claimed.lease_token,
        {"id": "flow-1", "type": "provider_run", "status": "success"},
        now=now,
    )
    assert appended.event_type == "task_flow"
    assert appended.payload["id"] == "flow-1"
    assert store.get_job("flow-job").progress == 37
    assert store.read_events(after_id=appended.id - 1)[0] == appended


def test_event_global_cursor_and_per_job_retention(isolated_db, registry) -> None:
    now = datetime(2026, 8, 8, 5, 0, 0)
    store = DurableJobStore(
        registry,
        isolated_db,
        lease_seconds=90,
        heartbeat_seconds=15,
        event_limit_per_job=3,
    )
    store.enqueue(_request("events", task_id="event-job"), now=now)
    claimed = store.claim_next("worker", now=now)
    for progress in (10, 20, 30, 40):
        store.update_progress(
            "event-job",
            "worker",
            claimed.lease_token,
            progress=progress,
            now=now + timedelta(seconds=progress),
        )

    events = store.read_events(job_id="event-job")
    assert len(events) == 3
    assert [event.payload["progress"] for event in events] == [20, 30, 40]
    assert store.read_events(after_id=events[1].id, job_id="event-job") == [events[2]]

    with isolated_db.get_session() as session:
        old = JobEventRecord(
            job_id="event-job",
            event_type="old",
            payload_json="{}",
            created_at=now - timedelta(days=31),
        )
        session.add(old)
        session.commit()
    assert store.prune_events(now=now) == 1


def test_research_state_events_follow_parent_lifecycle_retention(
    isolated_db,
    registry,
) -> None:
    now = datetime(2026, 8, 8, 5, 0, 0)
    old = now - timedelta(days=30)
    reference_time = datetime(2026, 7, 1, 1, 2, 3, tzinfo=timezone.utc)
    store = DurableJobStore(
        registry,
        isolated_db,
        event_retention_days=1,
        event_limit_per_job=2,
    )
    registry.register(
        "stock_analysis",
        1,
        DemoPayload,
        lambda payload: payload.stock_code,
    )
    store.enqueue(
        _request(
            "600519",
            task_id="active-research-job",
            job_type="stock_analysis",
        ),
        now=old,
    )
    first_lease = store.claim_next("worker-a", now=old)
    assert first_lease is not None

    store.enqueue(
        _request(
            "600519",
            task_id="terminal-research-job",
            job_type="stock_analysis",
        ),
        now=old,
    )
    terminal_lease = store.claim_next("terminal-worker", now=old + timedelta(seconds=1))
    assert terminal_lease is not None
    assert terminal_lease.task_id == "terminal-research-job"
    store.complete(
        terminal_lease.task_id,
        "terminal-worker",
        terminal_lease.lease_token,
        now=old + timedelta(seconds=2),
    )

    with isolated_db.get_session() as session:
        session.add_all(
            [
                JobEventRecord(
                    job_id="active-research-job",
                    event_type="research_reference_time",
                    payload_json=(
                        '{"scope_type": "stock", "scope_value": "600519", '
                        '"as_of": "2026-07-01T01:02:03.000000Z", '
                        '"marker": "active-old-state"}'
                    ),
                    created_at=old,
                ),
                JobEventRecord(
                    job_id="active-research-job",
                    event_type="research_evidence_snapshot",
                    payload_json='{"marker": "active-old-evidence"}',
                    created_at=old,
                ),
                JobEventRecord(
                    job_id="active-research-job",
                    event_type="research_debate_request",
                    payload_json='{"marker": "active-old-debate-request"}',
                    created_at=old,
                ),
                JobEventRecord(
                    job_id="terminal-research-job",
                    event_type="research_reference_time",
                    payload_json='{"marker": "terminal-old-state"}',
                    created_at=old,
                ),
                JobEventRecord(
                    job_id="terminal-research-job",
                    event_type="research_evidence_snapshot",
                    payload_json='{"marker": "terminal-old-evidence"}',
                    created_at=old,
                ),
                JobEventRecord(
                    job_id="terminal-research-job",
                    event_type="research_debate_request",
                    payload_json='{"marker": "terminal-old-debate-request"}',
                    created_at=old,
                ),
                JobEventRecord(
                    job_id="active-research-job",
                    event_type="ordinary-old-event",
                    payload_json='{"marker": "ordinary-old"}',
                    created_at=old,
                ),
            ]
        )
        for event_type in (
            "research_reference_time",
            "research_dataset_snapshot",
            "research_factor_snapshot",
            "research_evidence_snapshot",
            "research_debate_request",
            "research_debate_turn",
            "research_debate_failure",
            "research_debate_snapshot",
            "research_snapshot",
        ):
            payload_json = '{"marker": "fresh-state"}'
            if event_type == "research_reference_time":
                payload_json = (
                    '{"scope_type": "stock", "scope_value": "600519", '
                    '"as_of": "2026-07-01T01:02:03.000000Z", '
                    '"marker": "fresh-state"}'
                )
            session.add(
                JobEventRecord(
                    job_id="active-research-job",
                    event_type=event_type,
                    payload_json=payload_json,
                    created_at=now,
                )
            )
        for index in range(4):
            session.add(
                JobEventRecord(
                    job_id="active-research-job",
                    event_type="task_progress",
                    payload_json=f'{{"progress": {index}}}',
                    created_at=now,
                )
            )
        session.commit()

    store.prune_events(now=now)

    events = store.read_events(job_id="active-research-job")
    assert any(event.payload.get("marker") == "active-old-state" for event in events)
    assert any(
        event.payload.get("marker") == "active-old-evidence"
        for event in events
    )
    assert any(
        event.payload.get("marker") == "active-old-debate-request"
        for event in events
    )
    assert all(event.payload.get("marker") != "ordinary-old" for event in events)
    assert {
        event.event_type
        for event in events
        if event.event_type.startswith("research_")
    } == {
        "research_reference_time",
        "research_dataset_snapshot",
        "research_factor_snapshot",
        "research_evidence_snapshot",
        "research_debate_request",
        "research_debate_turn",
        "research_debate_failure",
        "research_debate_snapshot",
        "research_snapshot",
    }
    assert len([event for event in events if event.event_type == "task_progress"]) == 2

    terminal_events = store.read_events(job_id="terminal-research-job")
    assert all(
        event.payload.get("marker")
        not in {
            "terminal-old-state",
            "terminal-old-evidence",
            "terminal-old-debate-request",
        }
        for event in terminal_events
    )

    second_lease = store.claim_next("worker-b", now=now)
    assert second_lease is not None
    assert second_lease.task_id == "active-research-job"
    assert second_lease.lease_token != first_lease.lease_token
    resumed_events = store.read_events(job_id=second_lease.task_id)
    assert any(
        event.payload.get("marker") == "active-old-state"
        for event in resumed_events
    )
    repository = ResearchSnapshotRepository(isolated_db)
    assert repository.get_research_reference_time(
        scope_value="600519",
        lease=LeaseFence(
            job_id=second_lease.task_id,
            worker_id=second_lease.worker_id,
            lease_token=second_lease.lease_token,
        ),
        now=now + timedelta(seconds=1),
    ) == reference_time


def test_payload_version_mismatch_fails_closed(store, isolated_db) -> None:
    now = datetime(2026, 8, 8, 6, 0, 0)
    store.enqueue(_request("version", task_id="version-job"), now=now)
    with isolated_db.session_scope() as session:
        session.get(AnalysisJobRecord, "version-job").payload_version = "2"

    assert store.claim_next("worker", now=now) is None
    with isolated_db.get_session() as session:
        row = session.get(AnalysisJobRecord, "version-job")
        assert row.status == JOB_STATUS_FAILED
        assert row.error_code == "invalid_durable_payload"
        assert "version column/envelope mismatch" in row.error_message_sanitized


def test_outbox_per_channel_idempotency_double_fencing_and_ambiguity(isolated_db) -> None:
    now = datetime(2026, 8, 8, 7, 0, 0)
    outbox = NotificationOutboxStore(isolated_db, lease_seconds=30)
    request = OutboxEnqueueRequest(
        job_id=None,
        trace_id="trace-1",
        logical_notification_id="risk:600519:20260808",
        notification_type="major_risk",
        channel="telegram",
        route="primary",
        severity="critical",
        recipient="user-1",
        payload={"text": "risk"},
        content_sha256=hashlib.sha256(b"risk").hexdigest(),
    )
    created = outbox.enqueue(request, now=now)
    duplicate = outbox.enqueue(request, now=now)
    assert created.created is True
    assert (duplicate.id, duplicate.created) == (created.id, False)
    with isolated_db.get_session() as session:
        assert session.get(NotificationOutboxRecord, created.id).content_sha256 == (
            hashlib.sha256(b"risk").hexdigest()
        )

    invalid_hash = OutboxEnqueueRequest(
        **{**request.__dict__, "logical_notification_id": "invalid-hash", "content_sha256": "bad"}
    )
    with pytest.raises(ValueError, match="content_sha256"):
        outbox.enqueue(invalid_hash, now=now)

    conflicting = OutboxEnqueueRequest(**{**request.__dict__, "payload": {"text": "different"}})
    with pytest.raises(DurableJobConflictError):
        outbox.enqueue(conflicting, now=now)

    claimed = outbox.claim_next("sender-a", now=now)
    assert claimed.worker_id == "sender-a"
    with pytest.raises(StaleLeaseError):
        outbox.mark_sent(claimed.id, "sender-b", claimed.lease_token, now=now)
    with pytest.raises(StaleLeaseError):
        outbox.mark_sent(claimed.id, "sender-a", "wrong-token", now=now)

    assert outbox.expire_ambiguous_leases(now=now + timedelta(seconds=31)) == 1
    assert outbox.claim_next("sender-b", now=now + timedelta(seconds=31)) is None
    with isolated_db.get_session() as session:
        row = session.get(NotificationOutboxRecord, created.id)
        assert row.status == OUTBOX_STATUS_DELIVERY_UNKNOWN
        assert row.attempt == 1

    second = outbox.enqueue(
        OutboxEnqueueRequest(**{**request.__dict__, "logical_notification_id": "action:600519:20260808"}),
        now=now,
    )
    second_claim = outbox.claim_next("sender-a", now=now)
    assert second_claim.id == second.id
    outbox.mark_sent(second.id, "sender-a", second_claim.lease_token, provider_message_id="remote-1", now=now)
    with isolated_db.get_session() as session:
        row = session.get(NotificationOutboxRecord, second.id)
        assert row.status == OUTBOX_STATUS_SENT
        assert row.provider_message_id == "remote-1"


def test_outbox_known_failure_can_retry_but_expired_delivery_cannot(isolated_db) -> None:
    now = datetime(2026, 8, 8, 8, 0, 0)
    outbox = NotificationOutboxStore(isolated_db, lease_seconds=30)
    row = outbox.enqueue(
        OutboxEnqueueRequest(
            logical_notification_id="retry-notification",
            notification_type="account_action",
            channel="email",
            route="primary",
            recipient="owner@example.invalid",
            payload={"action": "review"},
            content_sha256=hashlib.sha256(b"review").hexdigest(),
            max_attempts=2,
        ),
        now=now,
    )
    first = outbox.claim_next("sender", now=now)
    assert outbox.mark_failed(
        row.id,
        "sender",
        first.lease_token,
        "provider_429",
        "Provider rate limit.",
        retry_after=10,
        now=now,
    ) == OUTBOX_STATUS_PENDING
    assert outbox.claim_next("sender", now=now + timedelta(seconds=9)) is None
    second = outbox.claim_next("sender", now=now + timedelta(seconds=10))
    assert second.attempt == 2
    outbox.mark_sent(row.id, "sender", second.lease_token, now=now + timedelta(seconds=10))


def test_provider_and_component_health_upsert(store, isolated_db) -> None:
    now = datetime(2026, 8, 8, 9, 0, 0)
    store.record_health(
        "provider",
        "tushare",
        "cyq_perf",
        status="healthy",
        success=True,
        latency_ms=100,
        metadata={"endpoint": "cyq_perf"},
        now=now,
    )
    store.record_health(
        "provider",
        "tushare",
        "cyq_perf",
        status="degraded",
        success=False,
        latency_ms=200,
        error_code="rate_limited",
        error_message_sanitized="token=private provider rate limit",
        now=now + timedelta(seconds=1),
    )
    store.heartbeat_component("worker", "worker-1", now=now)

    with isolated_db.get_session() as session:
        provider = session.execute(
            select(ProviderHealthRecord).where(ProviderHealthRecord.kind == "provider")
        ).scalar_one()
        component = session.execute(
            select(ProviderHealthRecord).where(ProviderHealthRecord.kind == "component")
        ).scalar_one()
        assert provider.success_count == 1
        assert provider.failure_count == 1
        assert provider.consecutive_successes == 0
        assert provider.consecutive_failures == 1
        assert provider.latency_ewma_ms == pytest.approx(120)
        assert provider.last_latency_ms == 200
        assert provider.last_error_code == "rate_limited"
        assert "private" not in provider.last_error_message_sanitized
        assert component.provider_key == "worker"
        assert component.scope == "worker-1"
        assert component.status == "healthy"
