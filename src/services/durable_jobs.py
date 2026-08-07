"""SQLite-backed durable analysis jobs and notification outbox primitives.

The durable backend deliberately stores only versioned JSON payloads.  Runtime
callables live in :class:`DurableJobHandlerRegistry`; they are never serialized
or persisted, which keeps queued jobs portable across worker processes and code
restarts.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence, Type

from pydantic import BaseModel, ValidationError
from pydantic_core import PydanticSerializationError
from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.exc import IntegrityError

from src.storage import (
    AnalysisJobRecord,
    DatabaseManager,
    JobEventRecord,
    NotificationOutboxRecord,
    ProviderHealthRecord,
    to_utc_naive_datetime,
    utc_naive_now,
)


JOB_STATUS_PENDING = "pending"
JOB_STATUS_PROCESSING = "processing"
JOB_STATUS_CANCEL_REQUESTED = "cancel_requested"
JOB_STATUS_SUCCEEDED = "succeeded"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_CANCELLED = "cancelled"

ACTIVE_JOB_STATUSES = frozenset(
    {
        JOB_STATUS_PENDING,
        JOB_STATUS_PROCESSING,
        JOB_STATUS_CANCEL_REQUESTED,
    }
)
TERMINAL_JOB_STATUSES = frozenset(
    {
        JOB_STATUS_SUCCEEDED,
        JOB_STATUS_FAILED,
        JOB_STATUS_CANCELLED,
    }
)

OUTBOX_STATUS_PENDING = "pending"
OUTBOX_STATUS_PROCESSING = "processing"
OUTBOX_STATUS_SENT = "sent"
OUTBOX_STATUS_FAILED = "failed"
OUTBOX_STATUS_DELIVERY_UNKNOWN = "delivery_unknown"
OUTBOX_STATUS_CANCELLED = "cancelled"
OUTBOX_STATUS_SUPPRESSED = "suppressed"
TERMINAL_OUTBOX_STATUSES = frozenset(
    {
        OUTBOX_STATUS_SENT,
        OUTBOX_STATUS_FAILED,
        OUTBOX_STATUS_DELIVERY_UNKNOWN,
        OUTBOX_STATUS_CANCELLED,
    }
)

DEFAULT_HEARTBEAT_SECONDS = 15
DEFAULT_LEASE_SECONDS = 90
DEFAULT_MAX_ATTEMPTS = 4  # initial attempt plus three retries
DEFAULT_EVENT_RETENTION_DAYS = 30
DEFAULT_EVENT_LIMIT_PER_JOB = 1000
DEFAULT_RETRY_BASE_SECONDS = 2.0


class DurableJobError(RuntimeError):
    """Base error for durable job operations."""


class UnknownJobHandlerError(DurableJobError):
    """Raised when a job type/payload version has no registered handler."""


class InvalidJobPayloadError(DurableJobError):
    """Raised when a durable payload is invalid or cannot be JSON encoded."""


class DurableJobConflictError(DurableJobError):
    """Raised when an idempotency key is reused for a different request."""


class DurableNoiseClaimBusyError(DurableJobError):
    """Raised when another live job owns a durable notification noise claim."""

    def __init__(self, message: str, *, retry_after: float = 200.0) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class StaleLeaseError(DurableJobError):
    """Raised when a worker no longer owns the current fencing token."""


class JobNotFoundError(DurableJobError):
    """Raised when a requested durable job does not exist."""


@dataclass(frozen=True)
class RegisteredJobHandler:
    job_type: str
    payload_version: int
    payload_model: Type[BaseModel]
    handler: Callable[[BaseModel], Any]


class DurableJobHandlerRegistry:
    """In-process registry for typed, versioned durable job handlers."""

    def __init__(self) -> None:
        self._handlers: dict[tuple[str, int], RegisteredJobHandler] = {}

    def register(
        self,
        job_type: str,
        payload_version: int,
        payload_model: Type[BaseModel],
        handler: Callable[[BaseModel], Any],
    ) -> None:
        normalized_type = _normalize_required_text(job_type, "job_type")
        if not isinstance(payload_version, int) or isinstance(payload_version, bool) or payload_version < 1:
            raise ValueError("payload_version must be a positive integer")
        if not isinstance(payload_model, type) or not issubclass(payload_model, BaseModel):
            raise TypeError("payload_model must be a pydantic BaseModel subclass")
        if not callable(handler):
            raise TypeError("handler must be callable")

        key = (normalized_type, payload_version)
        if key in self._handlers:
            raise ValueError(f"handler already registered for {normalized_type!r} v{payload_version}")
        self._handlers[key] = RegisteredJobHandler(
            job_type=normalized_type,
            payload_version=payload_version,
            payload_model=payload_model,
            handler=handler,
        )

    def resolve(self, job_type: str, payload_version: int) -> RegisteredJobHandler:
        key = (job_type, payload_version)
        try:
            return self._handlers[key]
        except KeyError as exc:
            raise UnknownJobHandlerError(
                f"no durable handler registered for {job_type!r} v{payload_version}"
            ) from exc

    def encode_payload(self, job_type: str, payload_version: int, payload: Any) -> str:
        if not isinstance(payload_version, int) or isinstance(payload_version, bool) or payload_version < 1:
            raise InvalidJobPayloadError("payload_version must be a positive integer")
        registered = self.resolve(job_type, payload_version)
        try:
            model = payload if isinstance(payload, registered.payload_model) else registered.payload_model.model_validate(payload)
        except (ValidationError, TypeError) as exc:
            raise InvalidJobPayloadError(str(exc)) from exc

        try:
            python_data = model.model_dump(mode="python")
            _assert_no_callable(python_data)
            data = model.model_dump(mode="json")
        except (PydanticSerializationError, TypeError) as exc:
            raise InvalidJobPayloadError(f"payload cannot be serialized as JSON: {exc}") from exc
        _assert_json_value(data)
        return _canonical_json({"version": payload_version, "data": data})

    def decode_payload(self, job_type: str, payload_json: str) -> BaseModel:
        envelope = _decode_payload_envelope(payload_json)
        registered = self.resolve(job_type, envelope["version"])
        try:
            return registered.payload_model.model_validate(envelope["data"])
        except ValidationError as exc:
            raise InvalidJobPayloadError(str(exc)) from exc

    def execute(self, job_type: str, payload_json: str) -> Any:
        envelope = _decode_payload_envelope(payload_json)
        registered = self.resolve(job_type, envelope["version"])
        try:
            payload = registered.payload_model.model_validate(envelope["data"])
        except ValidationError as exc:
            raise InvalidJobPayloadError(str(exc)) from exc
        return registered.handler(payload)


@dataclass(frozen=True)
class JobEnqueueRequest:
    job_type: str
    payload: Any
    payload_version: int = 1
    task_id: Optional[str] = None
    stock_code: Optional[str] = None
    stock_name: Optional[str] = None
    dedupe_key: Optional[str] = None
    idempotency_key: Optional[str] = None
    priority: int = 0
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    available_at: Optional[datetime] = None
    stage: Optional[str] = None
    progress: int = 0
    message: Optional[str] = None
    report_type: Optional[str] = None
    analysis_phase: Optional[str] = None
    query_source: Optional[str] = None
    trace_id: Optional[str] = None
    notify: bool = False


@dataclass(frozen=True)
class JobEnqueueResult:
    task_id: str
    status: str
    created: bool


@dataclass(frozen=True)
class ClaimedJob:
    task_id: str
    job_type: str
    payload: BaseModel
    payload_version: int
    trace_id: str
    notify: bool
    worker_id: str
    lease_token: str
    lease_expires_at: datetime
    attempt: int
    max_attempts: int
    priority: int
    cancel_requested: bool = False


@dataclass(frozen=True)
class JobHeartbeatResult:
    lease_expires_at: datetime
    cancel_requested: bool


@dataclass(frozen=True)
class JobEvent:
    id: int
    job_id: str
    event_type: str
    stage: Optional[str]
    payload: Any
    created_at: datetime


@dataclass(frozen=True)
class JobSnapshot:
    """Detached, lease-token-free view safe for facade/API consumers."""

    task_id: str
    job_type: str
    stock_code: Optional[str]
    stock_name: Optional[str]
    status: str
    stage: Optional[str]
    progress: int
    message: Optional[str]
    payload: Any
    result: Any
    error_code: Optional[str]
    error_message_sanitized: Optional[str]
    report_type: Optional[str]
    analysis_phase: Optional[str]
    query_source: Optional[str]
    trace_id: str
    priority: int
    attempt: int
    max_attempts: int
    available_at: datetime
    lease_owner: Optional[str]
    lease_expires_at: Optional[datetime]
    cancel_requested_at: Optional[datetime]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    notify: bool


@dataclass(frozen=True)
class OutboxEnqueueRequest:
    notification_type: str
    channel: str
    recipient: str
    payload: Any
    content_sha256: str
    logical_notification_id: str
    route: str = "default"
    trace_id: Optional[str] = None
    severity: str = "info"
    job_id: Optional[str] = None
    worker_id: Optional[str] = None
    lease_token: Optional[str] = None
    priority: int = 0
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    available_at: Optional[datetime] = None


@dataclass(frozen=True)
class OutboxEnqueueResult:
    id: int
    status: str
    created: bool
    channel: Optional[str] = None


@dataclass(frozen=True)
class _PreparedOutboxEnqueue:
    notification_type: str
    channel: str
    recipient: str
    payload_json: str
    content_sha256: str
    logical_notification_id: str
    route: str
    trace_id: str
    severity: str
    job_id: Optional[str]
    worker_id: Optional[str]
    lease_token: Optional[str]
    priority: int
    max_attempts: int
    available_at: Optional[datetime]
    idempotency_key: str


@dataclass(frozen=True)
class ClaimedOutboxMessage:
    id: int
    job_id: Optional[str]
    notification_type: str
    channel: str
    route: str
    recipient: str
    payload: Any
    worker_id: str
    lease_token: str
    lease_expires_at: datetime
    attempt: int
    max_attempts: int


def parse_retry_after_seconds(
    value: Any,
    *,
    now: Optional[datetime] = None,
    attempt: int = 1,
    base_seconds: float = DEFAULT_RETRY_BASE_SECONDS,
) -> float:
    """Parse Retry-After seconds/HTTP-date or return exponential backoff."""

    current = _coerce_now(now)
    if value is None or (isinstance(value, str) and not value.strip()):
        return max(0.0, float(base_seconds)) * (2 ** max(0, int(attempt) - 1))

    if isinstance(value, bool):
        raise ValueError("Retry-After cannot be boolean")
    if isinstance(value, (int, float)):
        seconds = float(value)
    else:
        raw = str(value).strip()
        try:
            seconds = float(raw)
        except ValueError:
            try:
                parsed = parsedate_to_datetime(raw)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"invalid Retry-After value: {value!r}") from exc
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            parsed_utc_naive = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            seconds = (parsed_utc_naive - current).total_seconds()

    if not math.isfinite(seconds):
        raise ValueError("Retry-After must be finite")
    return max(0.0, seconds)


class DurableJobStore:
    """Atomic durable job state machine backed by ``DatabaseManager``."""

    def __init__(
        self,
        registry: DurableJobHandlerRegistry,
        db_manager: Optional[DatabaseManager] = None,
        *,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        heartbeat_seconds: int = DEFAULT_HEARTBEAT_SECONDS,
        event_retention_days: int = DEFAULT_EVENT_RETENTION_DAYS,
        event_limit_per_job: int = DEFAULT_EVENT_LIMIT_PER_JOB,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if heartbeat_seconds <= 0 or heartbeat_seconds >= lease_seconds:
            raise ValueError("heartbeat_seconds must be positive and shorter than the lease")
        if event_retention_days <= 0 or event_limit_per_job <= 0:
            raise ValueError("event retention limits must be positive")
        self.registry = registry
        self.db = db_manager or DatabaseManager.get_instance()
        self.lease_seconds = int(lease_seconds)
        self.heartbeat_seconds = int(heartbeat_seconds)
        self.event_retention_days = int(event_retention_days)
        self.event_limit_per_job = int(event_limit_per_job)

    def enqueue(self, request: JobEnqueueRequest, *, now: Optional[datetime] = None) -> JobEnqueueResult:
        return self.enqueue_many([request], now=now)[0]

    def enqueue_many(
        self,
        requests: Iterable[JobEnqueueRequest],
        *,
        now: Optional[datetime] = None,
    ) -> list[JobEnqueueResult]:
        request_list = list(requests)
        if not request_list:
            return []
        current = _coerce_now(now)
        prepared = [self._prepare_job_request(request, current) for request in request_list]

        def _enqueue(session):
            results: list[JobEnqueueResult] = []
            idempotency_rows: dict[str, AnalysisJobRecord] = {}
            dedupe_rows: dict[str, AnalysisJobRecord] = {}

            idempotency_keys = {item["idempotency_key"] for item in prepared if item["idempotency_key"]}
            if idempotency_keys:
                existing = session.execute(
                    select(AnalysisJobRecord).where(AnalysisJobRecord.idempotency_key.in_(idempotency_keys))
                ).scalars()
                idempotency_rows.update({row.idempotency_key: row for row in existing})

            dedupe_keys = {item["dedupe_key"] for item in prepared if item["dedupe_key"]}
            if dedupe_keys:
                existing = session.execute(
                    select(AnalysisJobRecord).where(
                        AnalysisJobRecord.dedupe_key.in_(dedupe_keys),
                        AnalysisJobRecord.status.in_(ACTIVE_JOB_STATUSES),
                    )
                ).scalars()
                dedupe_rows.update({row.dedupe_key: row for row in existing})

            for values in prepared:
                idem_key = values["idempotency_key"]
                if idem_key and idem_key in idempotency_rows:
                    existing = idempotency_rows[idem_key]
                    self._assert_same_job_request(existing, values, "idempotency_key")
                    results.append(JobEnqueueResult(existing.task_id, existing.status, False))
                    continue

                dedupe_key = values["dedupe_key"]
                if dedupe_key and dedupe_key in dedupe_rows:
                    existing = dedupe_rows[dedupe_key]
                    self._assert_same_job_request(existing, values, "dedupe_key")
                    results.append(JobEnqueueResult(existing.task_id, existing.status, False))
                    if idem_key:
                        idempotency_rows[idem_key] = existing
                    continue

                row = AnalysisJobRecord(**values)
                session.add(row)
                session.flush()
                self._append_event(
                    session,
                    row.task_id,
                    "queued",
                    stage=row.stage,
                    payload={"priority": row.priority, "available_at": _isoformat(row.available_at)},
                    now=current,
                )
                results.append(JobEnqueueResult(row.task_id, row.status, True))
                if idem_key:
                    idempotency_rows[idem_key] = row
                if dedupe_key:
                    dedupe_rows[dedupe_key] = row

            return results

        try:
            return self.db._run_write_transaction("enqueue durable analysis jobs", _enqueue)
        except IntegrityError as exc:
            raise DurableJobConflictError("durable job idempotency or active dedupe conflict") from exc

    def claim_next(self, worker_id: str, *, now: Optional[datetime] = None) -> Optional[ClaimedJob]:
        normalized_worker = _normalize_required_text(worker_id, "worker_id")
        current = _coerce_now(now)

        def _claim(session):
            self._recover_expired_in_session(session, current)
            session.flush()
            while True:
                row = session.execute(
                    select(AnalysisJobRecord)
                    .where(
                        AnalysisJobRecord.status == JOB_STATUS_PENDING,
                        AnalysisJobRecord.available_at <= current,
                    )
                    .order_by(
                        AnalysisJobRecord.priority.desc(),
                        AnalysisJobRecord.created_at.asc(),
                        AnalysisJobRecord.task_id.asc(),
                    )
                    .limit(1)
                ).scalar_one_or_none()
                if row is None:
                    return None
                if row.attempt >= row.max_attempts:
                    row.status = JOB_STATUS_FAILED
                    row.error_code = "attempts_exhausted"
                    row.error_message_sanitized = "Maximum attempts exhausted before claim."
                    row.completed_at = current
                    row.updated_at = current
                    self._append_event(
                        session,
                        row.task_id,
                        "failed",
                        stage=row.stage,
                        payload={"reason": "attempts_exhausted"},
                        now=current,
                    )
                    session.flush()
                    continue

                try:
                    envelope = _decode_payload_envelope(row.payload_json)
                    if str(row.payload_version) != str(envelope["version"]):
                        raise InvalidJobPayloadError(
                            f"job {row.task_id!r} payload version column/envelope mismatch"
                        )
                    payload = self.registry.decode_payload(row.job_type, row.payload_json)
                except DurableJobError as exc:
                    row.status = JOB_STATUS_FAILED
                    row.error_code = "invalid_durable_payload"
                    row.error_message_sanitized = _sanitize_error_message(str(exc))
                    row.completed_at = current
                    row.updated_at = current
                    self._append_event(
                        session,
                        row.task_id,
                        "failed",
                        stage=row.stage,
                        payload={"error_code": row.error_code},
                        now=current,
                    )
                    session.flush()
                    continue

                lease_token = uuid.uuid4().hex
                lease_expires_at = current + timedelta(seconds=self.lease_seconds)
                row.status = JOB_STATUS_PROCESSING
                row.attempt += 1
                row.lease_owner = normalized_worker
                row.lease_token = lease_token
                row.lease_expires_at = lease_expires_at
                row.heartbeat_at = current
                row.started_at = row.started_at or current
                row.updated_at = current
                self._append_event(
                    session,
                    row.task_id,
                    "claimed",
                    stage=row.stage,
                    payload={
                        "worker_id": normalized_worker,
                        "attempt": row.attempt,
                        "max_attempts": row.max_attempts,
                        "lease_expires_at": _isoformat(lease_expires_at),
                    },
                    now=current,
                )
                return ClaimedJob(
                    task_id=row.task_id,
                    job_type=row.job_type,
                    payload=payload,
                    payload_version=envelope["version"],
                    trace_id=row.trace_id,
                    notify=bool(row.notify),
                    worker_id=normalized_worker,
                    lease_token=lease_token,
                    lease_expires_at=lease_expires_at,
                    attempt=row.attempt,
                    max_attempts=row.max_attempts,
                    priority=row.priority,
                )

        return self.db._run_write_transaction("claim durable analysis job", _claim)

    def heartbeat(
        self,
        task_id: str,
        worker_id: str,
        lease_token: str,
        *,
        now: Optional[datetime] = None,
    ) -> JobHeartbeatResult:
        current = _coerce_now(now)
        normalized_worker = _normalize_required_text(worker_id, "worker_id")
        expires = current + timedelta(seconds=self.lease_seconds)

        def _heartbeat(session):
            result = session.execute(
                update(AnalysisJobRecord)
                .where(
                    AnalysisJobRecord.task_id == task_id,
                    AnalysisJobRecord.status.in_({JOB_STATUS_PROCESSING, JOB_STATUS_CANCEL_REQUESTED}),
                    AnalysisJobRecord.lease_owner == normalized_worker,
                    AnalysisJobRecord.lease_token == lease_token,
                    AnalysisJobRecord.lease_expires_at > current,
                )
                .values(heartbeat_at=current, lease_expires_at=expires, updated_at=current)
            )
            if result.rowcount != 1:
                raise StaleLeaseError(f"job {task_id!r} lease is stale")
            status = session.execute(
                select(AnalysisJobRecord.status).where(AnalysisJobRecord.task_id == task_id)
            ).scalar_one()
            return JobHeartbeatResult(
                lease_expires_at=expires,
                cancel_requested=status == JOB_STATUS_CANCEL_REQUESTED,
            )

        return self.db._run_write_transaction("heartbeat durable analysis job", _heartbeat)

    def update_progress(
        self,
        task_id: str,
        worker_id: str,
        lease_token: str,
        *,
        stage: Optional[str] = None,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        event_payload: Any = None,
        now: Optional[datetime] = None,
    ) -> str:
        if progress is not None and (isinstance(progress, bool) or progress < 0 or progress > 100):
            raise ValueError("progress must be between 0 and 100")
        current = _coerce_now(now)
        normalized_worker = _normalize_required_text(worker_id, "worker_id")
        values: dict[str, Any] = {"updated_at": current}
        if stage is not None:
            values["stage"] = stage
        if progress is not None:
            values["progress"] = progress
        if message is not None:
            values["message"] = message

        def _update(session):
            result = session.execute(
                update(AnalysisJobRecord)
                .where(
                    AnalysisJobRecord.task_id == task_id,
                    AnalysisJobRecord.status.in_({JOB_STATUS_PROCESSING, JOB_STATUS_CANCEL_REQUESTED}),
                    AnalysisJobRecord.lease_owner == normalized_worker,
                    AnalysisJobRecord.lease_token == lease_token,
                    AnalysisJobRecord.lease_expires_at > current,
                )
                .values(**values)
            )
            if result.rowcount != 1:
                raise StaleLeaseError(f"job {task_id!r} lease is stale")
            row = session.get(AnalysisJobRecord, task_id)
            self._append_event(
                session,
                task_id,
                "progress",
                stage=row.stage,
                payload={
                    "progress": row.progress,
                    "message": row.message,
                    "detail": event_payload,
                },
                now=current,
            )
            self._prune_events_in_session(session, now=current, job_id=task_id)
            return row.status

        return self.db._run_write_transaction("update durable analysis job progress", _update)

    def append_flow_event(
        self,
        task_id: str,
        worker_id: str,
        lease_token: str,
        flow_event: Mapping[str, Any],
        *,
        now: Optional[datetime] = None,
    ) -> JobEvent:
        """Persist one flow event only while the caller owns the live lease."""

        if not isinstance(flow_event, Mapping):
            raise InvalidJobPayloadError("flow_event must be a JSON object")
        event_payload = dict(flow_event)
        _assert_json_value(event_payload)
        current = _coerce_now(now)
        normalized_worker = _normalize_required_text(worker_id, "worker_id")

        def _append(session):
            # The no-op update is intentional: it is a compare-and-swap fence
            # that cannot race a cancellation/reclaim on other SQL backends.
            cas = session.execute(
                update(AnalysisJobRecord)
                .where(
                    AnalysisJobRecord.task_id == task_id,
                    AnalysisJobRecord.status.in_({JOB_STATUS_PROCESSING, JOB_STATUS_CANCEL_REQUESTED}),
                    AnalysisJobRecord.lease_owner == normalized_worker,
                    AnalysisJobRecord.lease_token == lease_token,
                    AnalysisJobRecord.lease_expires_at > current,
                )
                .values(heartbeat_at=AnalysisJobRecord.heartbeat_at)
            )
            if cas.rowcount != 1:
                raise StaleLeaseError(f"job {task_id!r} lease is stale")
            stage = session.scalar(
                select(AnalysisJobRecord.stage).where(AnalysisJobRecord.task_id == task_id)
            )
            row = self._append_event(
                session,
                task_id,
                "task_flow",
                stage=stage,
                payload=event_payload,
                now=current,
            )
            self._prune_events_in_session(session, now=current, job_id=task_id)
            session.flush()
            return JobEvent(
                id=row.id,
                job_id=row.job_id,
                event_type=row.event_type,
                stage=row.stage,
                payload=event_payload,
                created_at=row.created_at,
            )

        return self.db._run_write_transaction("append durable analysis task flow event", _append)

    def update_stage(
        self,
        task_id: str,
        worker_id: str,
        lease_token: str,
        stage: str,
        *,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> str:
        return self.update_progress(
            task_id,
            worker_id,
            lease_token,
            stage=_normalize_required_text(stage, "stage"),
            progress=progress,
            message=message,
            now=now,
        )

    def complete(
        self,
        task_id: str,
        worker_id: str,
        lease_token: str,
        result: Any = None,
        *,
        now: Optional[datetime] = None,
    ) -> str:
        current = _coerce_now(now)
        normalized_worker = _normalize_required_text(worker_id, "worker_id")
        result_json = _canonical_json(result) if result is not None else None

        def _complete(session):
            succeeded = session.execute(
                update(AnalysisJobRecord)
                .where(
                    AnalysisJobRecord.task_id == task_id,
                    AnalysisJobRecord.status == JOB_STATUS_PROCESSING,
                    AnalysisJobRecord.lease_owner == normalized_worker,
                    AnalysisJobRecord.lease_token == lease_token,
                    AnalysisJobRecord.lease_expires_at > current,
                )
                .values(
                    status=JOB_STATUS_SUCCEEDED,
                    progress=100,
                    result_json=result_json,
                    error_code=None,
                    error_message_sanitized=None,
                    completed_at=current,
                    lease_owner=None,
                    lease_token=None,
                    lease_expires_at=None,
                    heartbeat_at=None,
                    updated_at=current,
                )
            )
            final_status = JOB_STATUS_SUCCEEDED
            event_type = "completed"
            if succeeded.rowcount != 1:
                cancelled = session.execute(
                    update(AnalysisJobRecord)
                    .where(
                        AnalysisJobRecord.task_id == task_id,
                        AnalysisJobRecord.status == JOB_STATUS_CANCEL_REQUESTED,
                        AnalysisJobRecord.lease_owner == normalized_worker,
                        AnalysisJobRecord.lease_token == lease_token,
                        AnalysisJobRecord.lease_expires_at > current,
                    )
                    .values(
                        status=JOB_STATUS_CANCELLED,
                        completed_at=current,
                        lease_owner=None,
                        lease_token=None,
                        lease_expires_at=None,
                        heartbeat_at=None,
                        updated_at=current,
                    )
                )
                if cancelled.rowcount != 1:
                    raise StaleLeaseError(f"job {task_id!r} lease is stale or terminal")
                final_status = JOB_STATUS_CANCELLED
                event_type = "cancelled"

            row = session.get(AnalysisJobRecord, task_id)
            self._append_event(session, task_id, event_type, stage=row.stage, now=current)
            self._prune_events_in_session(session, now=current, job_id=task_id)
            return final_status

        return self.db._run_write_transaction("complete durable analysis job", _complete)

    def fail(
        self,
        task_id: str,
        worker_id: str,
        lease_token: str,
        error_code: str,
        error_message_sanitized: str,
        *,
        retryable: bool = True,
        retry_after: Any = None,
        now: Optional[datetime] = None,
    ) -> str:
        current = _coerce_now(now)
        normalized_worker = _normalize_required_text(worker_id, "worker_id")
        normalized_error_code = _normalize_required_text(error_code, "error_code")
        normalized_error_message = _sanitize_error_message(error_message_sanitized)

        def _fail(session):
            row = session.execute(
                select(AnalysisJobRecord).where(AnalysisJobRecord.task_id == task_id)
            ).scalar_one_or_none()
            if row is None:
                raise JobNotFoundError(task_id)
            if (
                row.status not in {JOB_STATUS_PROCESSING, JOB_STATUS_CANCEL_REQUESTED}
                or row.lease_owner != normalized_worker
                or row.lease_token != lease_token
                or row.lease_expires_at is None
                or row.lease_expires_at <= current
            ):
                raise StaleLeaseError(f"job {task_id!r} lease is stale or terminal")

            if row.status == JOB_STATUS_CANCEL_REQUESTED:
                row.status = JOB_STATUS_CANCELLED
                row.completed_at = current
                event_type = "cancelled"
            elif retryable and row.attempt < row.max_attempts:
                delay = parse_retry_after_seconds(retry_after, now=current, attempt=row.attempt)
                row.status = JOB_STATUS_PENDING
                row.available_at = current + timedelta(seconds=delay)
                event_type = "retry_scheduled"
            else:
                row.status = JOB_STATUS_FAILED
                row.completed_at = current
                event_type = "failed"

            row.error_code = normalized_error_code
            row.error_message_sanitized = normalized_error_message
            row.lease_owner = None
            row.lease_token = None
            row.lease_expires_at = None
            row.heartbeat_at = None
            row.updated_at = current
            payload: dict[str, Any] = {
                "error_code": normalized_error_code,
                "error_message": normalized_error_message,
                "attempt": row.attempt,
                "max_attempts": row.max_attempts,
            }
            if row.status == JOB_STATUS_PENDING:
                payload["available_at"] = _isoformat(row.available_at)
            self._append_event(session, task_id, event_type, stage=row.stage, payload=payload, now=current)
            self._prune_events_in_session(session, now=current, job_id=task_id)
            return row.status

        return self.db._run_write_transaction("fail durable analysis job", _fail)

    def cancel(self, task_id: str, *, now: Optional[datetime] = None) -> str:
        current = _coerce_now(now)

        def _cancel(session):
            row = session.get(AnalysisJobRecord, task_id)
            if row is None:
                raise JobNotFoundError(task_id)
            if row.status in TERMINAL_JOB_STATUSES or row.status == JOB_STATUS_CANCEL_REQUESTED:
                return row.status

            row.cancel_requested_at = row.cancel_requested_at or current
            row.updated_at = current
            if row.status == JOB_STATUS_PENDING:
                row.status = JOB_STATUS_CANCELLED
                row.completed_at = current
                event_type = "cancelled"
            elif row.status == JOB_STATUS_PROCESSING:
                row.status = JOB_STATUS_CANCEL_REQUESTED
                event_type = "cancel_requested"
            else:
                raise DurableJobError(f"unsupported job state {row.status!r}")
            self._append_event(session, task_id, event_type, stage=row.stage, now=current)
            self._prune_events_in_session(session, now=current, job_id=task_id)
            return row.status

        return self.db._run_write_transaction("cancel durable analysis job", _cancel)

    def recover_expired_leases(self, *, now: Optional[datetime] = None) -> dict[str, int]:
        current = _coerce_now(now)

        def _recover(session):
            counts = self._recover_expired_in_session(session, current)
            self._prune_events_in_session(session, now=current)
            return counts

        return self.db._run_write_transaction("recover durable analysis job leases", _recover)

    def read_events(
        self,
        *,
        after_id: int = 0,
        job_id: Optional[str] = None,
        limit: int = 500,
    ) -> list[JobEvent]:
        if after_id < 0:
            raise ValueError("after_id cannot be negative")
        if limit < 1 or limit > 5000:
            raise ValueError("limit must be between 1 and 5000")
        statement = select(JobEventRecord).where(JobEventRecord.id > after_id)
        if job_id is not None:
            statement = statement.where(JobEventRecord.job_id == job_id)
        statement = statement.order_by(JobEventRecord.id.asc()).limit(limit)
        with self.db.get_session() as session:
            rows = session.execute(statement).scalars().all()
            return [
                JobEvent(
                    id=row.id,
                    job_id=row.job_id,
                    event_type=row.event_type,
                    stage=row.stage,
                    payload=_load_optional_json(row.payload_json),
                    created_at=row.created_at,
                )
                for row in rows
            ]

    def get_job(self, task_id: str) -> Optional[JobSnapshot]:
        with self.db.get_session() as session:
            row = session.get(AnalysisJobRecord, task_id)
            return _job_snapshot(row) if row is not None else None

    def list_jobs(
        self,
        *,
        limit: int = 100,
        statuses: Optional[Iterable[str]] = None,
    ) -> list[JobSnapshot]:
        if limit < 1 or limit > 5000:
            raise ValueError("limit must be between 1 and 5000")
        statement = select(AnalysisJobRecord)
        if statuses is not None:
            normalized_statuses = tuple(
                dict.fromkeys(_normalize_required_text(status, "status") for status in statuses)
            )
            if not normalized_statuses:
                return []
            statement = statement.where(AnalysisJobRecord.status.in_(normalized_statuses))
        statement = statement.order_by(
            AnalysisJobRecord.created_at.desc(),
            AnalysisJobRecord.task_id.desc(),
        ).limit(limit)
        with self.db.get_session() as session:
            return [_job_snapshot(row) for row in session.execute(statement).scalars()]

    def list_active_jobs(self, *, limit: int = 100) -> list[JobSnapshot]:
        return self.list_jobs(limit=limit, statuses=ACTIVE_JOB_STATUSES)

    def count_jobs(self, *, statuses: Optional[Iterable[str]] = None) -> int:
        statement = select(func.count()).select_from(AnalysisJobRecord)
        if statuses is not None:
            normalized_statuses = tuple(
                dict.fromkeys(_normalize_required_text(status, "status") for status in statuses)
            )
            if not normalized_statuses:
                return 0
            statement = statement.where(AnalysisJobRecord.status.in_(normalized_statuses))
        with self.db.get_session() as session:
            return int(session.scalar(statement) or 0)

    def get_status_counts(self) -> dict[str, int]:
        statement = (
            select(AnalysisJobRecord.status, func.count())
            .group_by(AnalysisJobRecord.status)
            .order_by(AnalysisJobRecord.status)
        )
        with self.db.get_session() as session:
            return {status: int(count) for status, count in session.execute(statement)}

    def get_high_water_event_id(self) -> int:
        with self.db.get_session() as session:
            return int(session.scalar(select(func.max(JobEventRecord.id))) or 0)

    def get_min_event_id(self) -> Optional[int]:
        with self.db.get_session() as session:
            value = session.scalar(select(func.min(JobEventRecord.id)))
            return int(value) if value is not None else None

    def prune_events(self, *, now: Optional[datetime] = None) -> int:
        current = _coerce_now(now)

        def _prune(session):
            return self._prune_events_in_session(session, now=current)

        return self.db._run_write_transaction("prune durable analysis job events", _prune)

    def record_health(
        self,
        kind: str,
        provider_key: str,
        scope: str = "default",
        *,
        status: str,
        success: Optional[bool] = None,
        latency_ms: Optional[float] = None,
        error_code: Optional[str] = None,
        error_message_sanitized: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        retry_after_at: Optional[datetime] = None,
        circuit_open_until: Optional[datetime] = None,
        now: Optional[datetime] = None,
    ) -> None:
        normalized_kind = _normalize_required_text(kind, "kind")
        normalized_provider_key = _normalize_required_text(provider_key, "provider_key")
        normalized_scope = _normalize_required_text(scope, "scope")
        normalized_status = _normalize_required_text(status, "status")
        current = _coerce_now(now)
        incoming_metadata = dict(metadata or {})
        _assert_json_value(incoming_metadata)

        def _upsert(session):
            row = session.execute(
                select(ProviderHealthRecord).where(
                    ProviderHealthRecord.kind == normalized_kind,
                    ProviderHealthRecord.provider_key == normalized_provider_key,
                    ProviderHealthRecord.scope == normalized_scope,
                )
            ).scalar_one_or_none()
            if row is None:
                row = ProviderHealthRecord(
                    kind=normalized_kind,
                    provider_key=normalized_provider_key,
                    scope=normalized_scope,
                    success_count=0,
                    failure_count=0,
                    consecutive_failures=0,
                    consecutive_successes=0,
                )
                session.add(row)
            row.status = normalized_status
            row.last_checked_at = current
            row.updated_at = current
            if latency_ms is not None:
                measured_latency = max(0.0, float(latency_ms))
                row.last_latency_ms = measured_latency
                row.latency_ewma_ms = (
                    measured_latency
                    if row.latency_ewma_ms is None
                    else (0.2 * measured_latency) + (0.8 * row.latency_ewma_ms)
                )
            if retry_after_at is not None:
                row.retry_after_at = _coerce_now(retry_after_at)
            if circuit_open_until is not None:
                row.circuit_open_until = _coerce_now(circuit_open_until)
            if success is True:
                row.success_count = (row.success_count or 0) + 1
                row.consecutive_successes = (row.consecutive_successes or 0) + 1
                row.consecutive_failures = 0
                row.last_success_at = current
                row.last_error_code = None
                row.last_error_message_sanitized = None
            elif success is False:
                row.failure_count = (row.failure_count or 0) + 1
                row.consecutive_failures = (row.consecutive_failures or 0) + 1
                row.consecutive_successes = 0
                row.last_failure_at = current
                row.last_error_code = _normalize_optional_text(error_code)
                row.last_error_message_sanitized = (
                    _sanitize_error_message(error_message_sanitized)
                    if error_message_sanitized
                    else None
                )
            existing_metadata = _load_optional_json(row.metadata_json)
            merged_metadata = dict(existing_metadata) if isinstance(existing_metadata, dict) else {}
            merged_metadata.update(incoming_metadata)
            row.metadata_json = _canonical_json(merged_metadata)

        self.db._run_write_transaction("upsert provider or component health", _upsert)

    def heartbeat_component(
        self,
        component: str,
        instance_id: str,
        *,
        status: str = "healthy",
        now: Optional[datetime] = None,
    ) -> None:
        """Upsert a process/component heartbeat using the generic health table."""

        self.record_health(
            kind="component",
            provider_key=_normalize_required_text(component, "component"),
            scope=_normalize_required_text(instance_id, "instance_id"),
            status=status,
            success=True,
            now=now,
        )

    def _prepare_job_request(self, request: JobEnqueueRequest, current: datetime) -> dict[str, Any]:
        if not isinstance(request, JobEnqueueRequest):
            raise TypeError("request must be JobEnqueueRequest")
        job_type = _normalize_required_text(request.job_type, "job_type")
        payload_json = self.registry.encode_payload(job_type, request.payload_version, request.payload)
        if (
            isinstance(request.max_attempts, bool)
            or not isinstance(request.max_attempts, int)
            or request.max_attempts < 1
        ):
            raise ValueError("max_attempts must be a positive integer")
        if isinstance(request.priority, bool) or not isinstance(request.priority, int):
            raise ValueError("priority must be an integer")
        if isinstance(request.progress, bool) or request.progress < 0 or request.progress > 100:
            raise ValueError("progress must be between 0 and 100")
        return {
            "task_id": _normalize_optional_text(request.task_id) or uuid.uuid4().hex,
            "job_type": job_type,
            "stock_code": request.stock_code,
            "stock_name": request.stock_name,
            "dedupe_key": _normalize_optional_text(request.dedupe_key),
            "status": JOB_STATUS_PENDING,
            "stage": request.stage,
            "progress": request.progress,
            "message": request.message,
            "payload_json": payload_json,
            "payload_version": str(request.payload_version),
            "result_json": None,
            "error_code": None,
            "error_message_sanitized": None,
            "report_type": request.report_type,
            "analysis_phase": request.analysis_phase,
            "query_source": request.query_source,
            "trace_id": _normalize_optional_text(request.trace_id) or uuid.uuid4().hex,
            "notify": bool(request.notify),
            "idempotency_key": _normalize_optional_text(request.idempotency_key),
            "priority": request.priority,
            "attempt": 0,
            "max_attempts": request.max_attempts,
            "available_at": _coerce_now(request.available_at) if request.available_at else current,
            "lease_owner": None,
            "lease_token": None,
            "lease_expires_at": None,
            "heartbeat_at": None,
            "cancel_requested_at": None,
            "started_at": None,
            "completed_at": None,
            "created_at": current,
            "updated_at": current,
        }

    @staticmethod
    def _assert_same_job_request(row: AnalysisJobRecord, values: Mapping[str, Any], key_name: str) -> None:
        if (
            row.job_type != values["job_type"]
            or str(row.payload_version) != str(values["payload_version"])
            or row.payload_json != values["payload_json"]
            or bool(row.notify) != bool(values["notify"])
        ):
            raise DurableJobConflictError(
                f"{key_name} {values[key_name]!r} already belongs to a different durable job request"
            )

    def _recover_expired_in_session(self, session, current: datetime) -> dict[str, int]:
        rows = session.execute(
            select(AnalysisJobRecord).where(
                AnalysisJobRecord.status.in_({JOB_STATUS_PROCESSING, JOB_STATUS_CANCEL_REQUESTED}),
                AnalysisJobRecord.lease_expires_at.is_not(None),
                AnalysisJobRecord.lease_expires_at <= current,
            )
        ).scalars().all()
        counts = {"requeued": 0, "failed": 0, "cancelled": 0}
        for row in rows:
            expired_worker = row.lease_owner
            if row.status == JOB_STATUS_CANCEL_REQUESTED:
                row.status = JOB_STATUS_CANCELLED
                row.completed_at = current
                event_type = "cancelled"
                counts["cancelled"] += 1
            elif row.attempt >= row.max_attempts:
                row.status = JOB_STATUS_FAILED
                row.error_code = "lease_expired_attempts_exhausted"
                row.error_message_sanitized = "Worker lease expired after maximum attempts."
                row.completed_at = current
                event_type = "failed"
                counts["failed"] += 1
            else:
                row.status = JOB_STATUS_PENDING
                row.error_code = "lease_expired"
                row.error_message_sanitized = "Worker lease expired; job recovered."
                row.available_at = current
                event_type = "lease_recovered"
                counts["requeued"] += 1
            row.lease_owner = None
            row.lease_token = None
            row.lease_expires_at = None
            row.heartbeat_at = None
            row.updated_at = current
            self._append_event(
                session,
                row.task_id,
                event_type,
                stage=row.stage,
                payload={"expired_worker_id": expired_worker, "attempt": row.attempt},
                now=current,
            )
        return counts

    @staticmethod
    def _append_event(
        session,
        job_id: str,
        event_type: str,
        *,
        stage: Optional[str] = None,
        payload: Any = None,
        now: Optional[datetime] = None,
    ) -> JobEventRecord:
        row = JobEventRecord(
            job_id=job_id,
            event_type=event_type,
            stage=stage,
            payload_json=_canonical_json(payload if payload is not None else {}),
            created_at=_coerce_now(now),
        )
        session.add(row)
        return row

    def _prune_events_in_session(
        self,
        session,
        *,
        now: datetime,
        job_id: Optional[str] = None,
    ) -> int:
        session.flush()
        cutoff = now - timedelta(days=self.event_retention_days)
        conditions = [JobEventRecord.created_at < cutoff]
        if job_id is not None:
            conditions.append(JobEventRecord.job_id == job_id)
        expired = session.execute(
            delete(JobEventRecord)
            .where(*conditions)
            .execution_options(synchronize_session=False)
        )
        deleted_count = expired.rowcount or 0

        job_ids: Sequence[str]
        if job_id is not None:
            job_ids = [job_id]
        else:
            job_ids = list(session.execute(select(JobEventRecord.job_id).distinct()).scalars())
        for current_job_id in job_ids:
            keep_ids = (
                select(JobEventRecord.id)
                .where(JobEventRecord.job_id == current_job_id)
                .order_by(JobEventRecord.id.desc())
                .limit(self.event_limit_per_job)
            )
            result = session.execute(
                delete(JobEventRecord)
                .where(
                    JobEventRecord.job_id == current_job_id,
                    JobEventRecord.id.not_in(keep_ids),
                )
                .execution_options(synchronize_session=False)
            )
            deleted_count += result.rowcount or 0
        return deleted_count


class NotificationOutboxStore:
    """Per-channel durable outbox with at-most-once ambiguity handling."""

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        *,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.db = db_manager or DatabaseManager.get_instance()
        self.lease_seconds = int(lease_seconds)

    def enqueue(
        self,
        request: OutboxEnqueueRequest,
        *,
        now: Optional[datetime] = None,
    ) -> OutboxEnqueueResult:
        requested_now = _coerce_now(now) if now is not None else None
        prepared = _prepare_outbox_enqueue(request)
        if prepared.job_id is not None:
            raise ValueError(
                "job-bound notification plans must use enqueue_batch"
            )

        def _enqueue(session):
            # When a writer waits on SQLite's BEGIN IMMEDIATE lock, the Lease
            # can expire before this transaction actually begins.  Use a fresh
            # clock at the fenced write boundary; explicit ``now`` remains
            # available for deterministic tests.
            current = requested_now or utc_naive_now()
            if prepared.job_id is not None:
                parent = session.get(AnalysisJobRecord, prepared.job_id)
                if (
                    parent is None
                    or parent.status != JOB_STATUS_PROCESSING
                    or parent.cancel_requested_at is not None
                    or parent.lease_owner != prepared.worker_id
                    or parent.lease_token != prepared.lease_token
                    or parent.lease_expires_at is None
                    or parent.lease_expires_at <= current
                ):
                    raise StaleLeaseError(
                        f"job {prepared.job_id!r} lease is stale or cancellation was requested"
                    )
            existing = session.execute(
                select(NotificationOutboxRecord).where(
                    NotificationOutboxRecord.logical_notification_id
                    == prepared.logical_notification_id,
                    NotificationOutboxRecord.channel == prepared.channel,
                    NotificationOutboxRecord.route == prepared.route,
                )
            ).scalar_one_or_none()
            if existing is not None:
                same_payload = (
                    existing.notification_type == prepared.notification_type
                    and existing.recipient == prepared.recipient
                    and existing.severity == prepared.severity
                    and existing.content_sha256 == prepared.content_sha256
                    and existing.payload_json == prepared.payload_json
                )
                same_parent = existing.job_id == prepared.job_id
                if not same_parent or (prepared.job_id is None and not same_payload):
                    raise DurableJobConflictError(
                        "logical notification/channel/route already belongs to different content"
                    )
                # A recovered job may regenerate time-bearing or LLM-authored
                # notification text.  Its first persisted row is the immutable
                # delivery authority, just like the first committed analysis
                # history.  Return that row without rewriting or re-routing it.
                return OutboxEnqueueResult(
                    existing.id,
                    existing.status,
                    False,
                    existing.channel,
                )

            row = NotificationOutboxRecord(
                job_id=prepared.job_id,
                trace_id=prepared.trace_id,
                logical_notification_id=prepared.logical_notification_id,
                notification_type=prepared.notification_type,
                channel=prepared.channel,
                route=prepared.route,
                severity=prepared.severity,
                recipient=prepared.recipient,
                payload_json=prepared.payload_json,
                content_sha256=prepared.content_sha256,
                idempotency_key=prepared.idempotency_key,
                status=OUTBOX_STATUS_PENDING,
                priority=prepared.priority,
                attempt=0,
                max_attempts=prepared.max_attempts,
                available_at=prepared.available_at or current,
                created_at=current,
                updated_at=current,
            )
            session.add(row)
            session.flush()
            return OutboxEnqueueResult(row.id, row.status, True, row.channel)

        try:
            return self.db._run_write_transaction("enqueue notification outbox row", _enqueue)
        except IntegrityError as exc:
            raise DurableJobConflictError("notification outbox logical delivery conflict") from exc

    def enqueue_batch(
        self,
        requests: Sequence[OutboxEnqueueRequest],
        *,
        now: Optional[datetime] = None,
    ) -> list[OutboxEnqueueResult]:
        """Atomically freeze every channel in one logical delivery plan.

        A recovered job may regenerate different text or run under changed
        notification configuration.  Once any job-bound plan is committed,
        its complete first channel set and payloads remain authoritative.
        """
        prepared = [_prepare_outbox_enqueue(request) for request in requests]
        if not prepared:
            raise ValueError("notification outbox batch must not be empty")
        first = prepared[0]
        plan_identity = (
            first.logical_notification_id,
            first.route,
            first.job_id,
            first.worker_id,
            first.lease_token,
        )
        if any(
            (
                item.logical_notification_id,
                item.route,
                item.job_id,
                item.worker_id,
                item.lease_token,
            )
            != plan_identity
            for item in prepared[1:]
        ):
            raise ValueError(
                "notification outbox batch must share one logical delivery and lease"
            )
        channels = [item.channel for item in prepared]
        if len(set(channels)) != len(channels):
            raise ValueError("notification outbox batch contains duplicate channels")
        requested_now = _coerce_now(now) if now is not None else None

        def _enqueue_batch(session):
            # Take the implicit clock only after BEGIN IMMEDIATE succeeds.
            current = requested_now or utc_naive_now()
            if first.job_id is not None:
                parent = session.get(AnalysisJobRecord, first.job_id)
                if (
                    parent is None
                    or parent.status != JOB_STATUS_PROCESSING
                    or parent.cancel_requested_at is not None
                    or parent.lease_owner != first.worker_id
                    or parent.lease_token != first.lease_token
                    or parent.lease_expires_at is None
                    or parent.lease_expires_at <= current
                ):
                    raise StaleLeaseError(
                        f"job {first.job_id!r} lease is stale or cancellation was requested"
                    )

            existing_rows = list(
                session.execute(
                    select(NotificationOutboxRecord)
                    .where(
                        NotificationOutboxRecord.logical_notification_id
                        == first.logical_notification_id,
                        NotificationOutboxRecord.route == first.route,
                    )
                    .order_by(NotificationOutboxRecord.id)
                ).scalars()
            )
            if existing_rows:
                if any(row.job_id != first.job_id for row in existing_rows):
                    raise DurableJobConflictError(
                        "logical notification plan already belongs to a different job"
                    )
                if first.job_id is None:
                    by_channel = {item.channel: item for item in prepared}
                    if set(by_channel) != {row.channel for row in existing_rows}:
                        raise DurableJobConflictError(
                            "logical notification plan already belongs to a different channel set"
                        )
                    for row in existing_rows:
                        item = by_channel[row.channel]
                        if not _outbox_row_matches_prepared(row, item):
                            raise DurableJobConflictError(
                                "logical notification plan already belongs to different content"
                            )
                # For a job retry, the first committed plan is immutable.  The
                # entire initial batch was one transaction, so returning this
                # set cannot expose a partially persisted plan.
                return [
                    OutboxEnqueueResult(row.id, row.status, False, row.channel)
                    for row in existing_rows
                ]

            noise_conflict = _find_durable_noise_conflict(
                session,
                prepared=prepared,
                current=current,
            )
            if noise_conflict is not None:
                conflict_row, conflict_state = noise_conflict
                if conflict_state == "busy":
                    raise DurableNoiseClaimBusyError(
                        "Another durable job owns this notification noise claim."
                    )
                return [
                    OutboxEnqueueResult(
                        conflict_row.id,
                        OUTBOX_STATUS_SUPPRESSED,
                        False,
                        conflict_row.channel,
                    )
                ]

            rows = [
                NotificationOutboxRecord(
                    job_id=item.job_id,
                    trace_id=item.trace_id,
                    logical_notification_id=item.logical_notification_id,
                    notification_type=item.notification_type,
                    channel=item.channel,
                    route=item.route,
                    severity=item.severity,
                    recipient=item.recipient,
                    payload_json=item.payload_json,
                    content_sha256=item.content_sha256,
                    idempotency_key=item.idempotency_key,
                    status=OUTBOX_STATUS_PENDING,
                    priority=item.priority,
                    attempt=0,
                    max_attempts=item.max_attempts,
                    available_at=item.available_at or current,
                    created_at=current,
                    updated_at=current,
                )
                for item in prepared
            ]
            session.add_all(rows)
            session.flush()
            return [
                OutboxEnqueueResult(row.id, row.status, True, row.channel)
                for row in rows
            ]

        try:
            return self.db._run_write_transaction(
                "enqueue notification outbox plan",
                _enqueue_batch,
            )
        except IntegrityError as exc:
            raise DurableJobConflictError(
                "notification outbox logical delivery conflict"
            ) from exc

    def get_frozen_plan(
        self,
        *,
        job_id: str,
        logical_notification_id: str,
        route: str,
        worker_id: str,
        lease_token: str,
        now: Optional[datetime] = None,
    ) -> list[OutboxEnqueueResult]:
        """Return a prior job-bound plan under the caller's live lease."""
        normalized_job_id = _normalize_required_text(job_id, "job_id")
        normalized_logical_id = _normalize_required_text(
            logical_notification_id,
            "logical_notification_id",
        )
        normalized_route = _normalize_required_text(route, "route")
        normalized_worker_id = _normalize_required_text(worker_id, "worker_id")
        normalized_lease_token = _normalize_required_text(lease_token, "lease_token")
        requested_now = _coerce_now(now) if now is not None else None

        def _read(session):
            current = requested_now or utc_naive_now()
            parent = session.get(AnalysisJobRecord, normalized_job_id)
            if (
                parent is None
                or parent.status != JOB_STATUS_PROCESSING
                or parent.cancel_requested_at is not None
                or parent.lease_owner != normalized_worker_id
                or parent.lease_token != normalized_lease_token
                or parent.lease_expires_at is None
                or parent.lease_expires_at <= current
            ):
                raise StaleLeaseError(
                    f"job {normalized_job_id!r} lease is stale or cancellation was requested"
                )
            rows = list(
                session.execute(
                    select(NotificationOutboxRecord)
                    .where(
                        NotificationOutboxRecord.logical_notification_id
                        == normalized_logical_id,
                        NotificationOutboxRecord.route == normalized_route,
                    )
                    .order_by(NotificationOutboxRecord.id)
                ).scalars()
            )
            if any(row.job_id != normalized_job_id for row in rows):
                raise DurableJobConflictError(
                    "logical notification plan already belongs to a different job"
                )
            return [
                OutboxEnqueueResult(row.id, row.status, False, row.channel)
                for row in rows
            ]

        return self.db._run_write_transaction(
            "read frozen notification outbox plan",
            _read,
        )

    def claim_next(
        self,
        worker_id: str,
        *,
        now: Optional[datetime] = None,
    ) -> Optional[ClaimedOutboxMessage]:
        normalized_worker = _normalize_required_text(worker_id, "worker_id")
        current = _coerce_now(now)

        def _claim(session):
            self._expire_ambiguous_in_session(session, current)
            failed_parent_ids = select(AnalysisJobRecord.task_id).where(
                AnalysisJobRecord.status.in_((JOB_STATUS_FAILED, JOB_STATUS_CANCELLED))
            )
            session.execute(
                update(NotificationOutboxRecord)
                .where(
                    NotificationOutboxRecord.status == OUTBOX_STATUS_PENDING,
                    NotificationOutboxRecord.job_id.in_(failed_parent_ids),
                )
                .values(
                    status=OUTBOX_STATUS_CANCELLED,
                    error_code="parent_job_not_succeeded",
                    error_message_sanitized=(
                        "Parent analysis job ended without succeeding; delivery was cancelled."
                    ),
                    updated_at=current,
                )
            )
            succeeded_parent_ids = select(AnalysisJobRecord.task_id).where(
                AnalysisJobRecord.status == JOB_STATUS_SUCCEEDED
            )
            while True:
                row = session.execute(
                    select(NotificationOutboxRecord)
                    .where(
                        NotificationOutboxRecord.status == OUTBOX_STATUS_PENDING,
                        NotificationOutboxRecord.available_at <= current,
                        or_(
                            NotificationOutboxRecord.job_id.is_(None),
                            NotificationOutboxRecord.job_id.in_(succeeded_parent_ids),
                        ),
                    )
                    .order_by(
                        NotificationOutboxRecord.priority.desc(),
                        NotificationOutboxRecord.created_at.asc(),
                        NotificationOutboxRecord.id.asc(),
                    )
                    .limit(1)
                ).scalar_one_or_none()
                if row is None:
                    return None
                if row.attempt >= row.max_attempts:
                    row.status = OUTBOX_STATUS_FAILED
                    row.error_code = "attempts_exhausted"
                    row.error_message_sanitized = "Maximum attempts exhausted before claim."
                    row.updated_at = current
                    session.flush()
                    continue

                token = uuid.uuid4().hex
                expires = current + timedelta(seconds=self.lease_seconds)
                row.status = OUTBOX_STATUS_PROCESSING
                row.attempt += 1
                row.lease_owner = normalized_worker
                row.lease_token = token
                row.lease_expires_at = expires
                row.updated_at = current
                return ClaimedOutboxMessage(
                    id=row.id,
                    job_id=row.job_id,
                    notification_type=row.notification_type,
                    channel=row.channel,
                    route=row.route,
                    recipient=row.recipient,
                    payload=json.loads(row.payload_json),
                    worker_id=normalized_worker,
                    lease_token=token,
                    lease_expires_at=expires,
                    attempt=row.attempt,
                    max_attempts=row.max_attempts,
                )

        return self.db._run_write_transaction("claim notification outbox row", _claim)

    def mark_sent(
        self,
        outbox_id: int,
        worker_id: str,
        lease_token: str,
        *,
        provider_message_id: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> None:
        current = _coerce_now(now)
        normalized_worker = _normalize_required_text(worker_id, "worker_id")

        def _sent(session):
            result = session.execute(
                update(NotificationOutboxRecord)
                .where(
                    NotificationOutboxRecord.id == outbox_id,
                    NotificationOutboxRecord.status == OUTBOX_STATUS_PROCESSING,
                    NotificationOutboxRecord.lease_owner == normalized_worker,
                    NotificationOutboxRecord.lease_token == lease_token,
                    NotificationOutboxRecord.lease_expires_at > current,
                )
                .values(
                    status=OUTBOX_STATUS_SENT,
                    provider_message_id=provider_message_id,
                    sent_at=current,
                    lease_owner=None,
                    lease_token=None,
                    lease_expires_at=None,
                    error_code=None,
                    error_message_sanitized=None,
                    updated_at=current,
                )
            )
            if result.rowcount != 1:
                raise StaleLeaseError(f"outbox row {outbox_id} lease is stale or terminal")

        self.db._run_write_transaction("mark notification outbox row sent", _sent)

    def mark_failed(
        self,
        outbox_id: int,
        worker_id: str,
        lease_token: str,
        error_code: str,
        error_message_sanitized: str,
        *,
        retryable: bool = True,
        retry_after: Any = None,
        now: Optional[datetime] = None,
    ) -> str:
        current = _coerce_now(now)
        normalized_worker = _normalize_required_text(worker_id, "worker_id")
        normalized_error_code = _normalize_required_text(error_code, "error_code")
        normalized_error_message = _sanitize_error_message(error_message_sanitized)

        def _failed(session):
            row = session.get(NotificationOutboxRecord, outbox_id)
            if (
                row is None
                or row.status != OUTBOX_STATUS_PROCESSING
                or row.lease_owner != normalized_worker
                or row.lease_token != lease_token
                or row.lease_expires_at is None
                or row.lease_expires_at <= current
            ):
                raise StaleLeaseError(f"outbox row {outbox_id} lease is stale or terminal")
            row.error_code = normalized_error_code
            row.error_message_sanitized = normalized_error_message
            if retryable and row.attempt < row.max_attempts:
                delay = parse_retry_after_seconds(retry_after, now=current, attempt=row.attempt)
                row.status = OUTBOX_STATUS_PENDING
                row.available_at = current + timedelta(seconds=delay)
            else:
                row.status = OUTBOX_STATUS_FAILED
            row.lease_owner = None
            row.lease_token = None
            row.lease_expires_at = None
            row.updated_at = current
            return row.status

        return self.db._run_write_transaction("mark notification outbox row failed", _failed)

    def mark_delivery_unknown(
        self,
        outbox_id: int,
        worker_id: str,
        lease_token: str,
        error_code: str,
        error_message_sanitized: str,
        *,
        now: Optional[datetime] = None,
    ) -> None:
        current = _coerce_now(now)
        normalized_worker = _normalize_required_text(worker_id, "worker_id")
        normalized_error_code = _normalize_required_text(error_code, "error_code")
        normalized_error_message = _sanitize_error_message(error_message_sanitized)

        def _unknown(session):
            result = session.execute(
                update(NotificationOutboxRecord)
                .where(
                    NotificationOutboxRecord.id == outbox_id,
                    NotificationOutboxRecord.status == OUTBOX_STATUS_PROCESSING,
                    NotificationOutboxRecord.lease_owner == normalized_worker,
                    NotificationOutboxRecord.lease_token == lease_token,
                )
                .values(
                    status=OUTBOX_STATUS_DELIVERY_UNKNOWN,
                    error_code=normalized_error_code,
                    error_message_sanitized=normalized_error_message,
                    lease_owner=None,
                    lease_token=None,
                    lease_expires_at=None,
                    updated_at=current,
                )
            )
            if result.rowcount != 1:
                raise StaleLeaseError(f"outbox row {outbox_id} lease is stale or terminal")

        self.db._run_write_transaction("mark notification outbox delivery unknown", _unknown)

    def expire_ambiguous_leases(self, *, now: Optional[datetime] = None) -> int:
        current = _coerce_now(now)

        def _expire(session):
            return self._expire_ambiguous_in_session(session, current)

        return self.db._run_write_transaction("expire ambiguous notification outbox leases", _expire)

    @staticmethod
    def _expire_ambiguous_in_session(session, current: datetime) -> int:
        result = session.execute(
            update(NotificationOutboxRecord)
            .where(
                NotificationOutboxRecord.status == OUTBOX_STATUS_PROCESSING,
                NotificationOutboxRecord.lease_expires_at.is_not(None),
                NotificationOutboxRecord.lease_expires_at <= current,
            )
            .values(
                status=OUTBOX_STATUS_DELIVERY_UNKNOWN,
                error_code="delivery_outcome_unknown",
                error_message_sanitized="Notification lease expired; delivery outcome is unknown.",
                lease_owner=None,
                lease_token=None,
                lease_expires_at=None,
                updated_at=current,
            )
        )
        return result.rowcount or 0


def _decode_payload_envelope(payload_json: str) -> dict[str, Any]:
    try:
        value = json.loads(payload_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise InvalidJobPayloadError("durable payload is not valid JSON") from exc
    if not isinstance(value, dict) or set(value) != {"version", "data"}:
        raise InvalidJobPayloadError("durable payload must contain exactly version and data")
    version = value["version"]
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise InvalidJobPayloadError("durable payload version must be a positive integer")
    if not isinstance(value["data"], dict):
        raise InvalidJobPayloadError("durable payload data must be an object")
    _assert_json_value(value["data"])
    return value


def _canonical_json(value: Any) -> str:
    _assert_json_value(value)
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise InvalidJobPayloadError(f"value is not strict JSON: {exc}") from exc


def _prepare_outbox_enqueue(request: OutboxEnqueueRequest) -> _PreparedOutboxEnqueue:
    notification_type = _normalize_required_text(
        request.notification_type,
        "notification_type",
    )
    channel = _normalize_required_text(request.channel, "channel")
    recipient = _normalize_required_text(request.recipient, "recipient")
    logical_notification_id = _normalize_required_text(
        request.logical_notification_id,
        "logical_notification_id",
    )
    route = _normalize_required_text(request.route, "route")
    severity = _normalize_required_text(request.severity, "severity")
    job_id = _normalize_optional_text(request.job_id)
    worker_id = _normalize_optional_text(request.worker_id)
    lease_token = _normalize_optional_text(request.lease_token)
    if job_id is not None and (worker_id is None or lease_token is None):
        raise StaleLeaseError(
            "job-bound notification enqueue requires the current worker lease"
        )
    if job_id is None and (worker_id is not None or lease_token is not None):
        raise ValueError("worker_id and lease_token require job_id")
    if (
        isinstance(request.max_attempts, bool)
        or not isinstance(request.max_attempts, int)
        or request.max_attempts < 1
    ):
        raise ValueError("max_attempts must be a positive integer")
    if isinstance(request.priority, bool) or not isinstance(request.priority, int):
        raise ValueError("priority must be an integer")
    payload_json = _canonical_json(request.payload)
    content_sha256 = str(request.content_sha256 or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", content_sha256) is None:
        raise ValueError("content_sha256 must be a lowercase SHA256 hex digest")
    trace_id = _normalize_optional_text(request.trace_id) or uuid.uuid4().hex
    idempotency_key = hashlib.sha256(
        f"{logical_notification_id}\0{channel}\0{route}".encode("utf-8")
    ).hexdigest()
    return _PreparedOutboxEnqueue(
        notification_type=notification_type,
        channel=channel,
        recipient=recipient,
        payload_json=payload_json,
        content_sha256=content_sha256,
        logical_notification_id=logical_notification_id,
        route=route,
        trace_id=trace_id,
        severity=severity,
        job_id=job_id,
        worker_id=worker_id,
        lease_token=lease_token,
        priority=int(request.priority),
        max_attempts=request.max_attempts,
        available_at=(
            _coerce_now(request.available_at)
            if request.available_at is not None
            else None
        ),
        idempotency_key=idempotency_key,
    )


def _outbox_row_matches_prepared(
    row: NotificationOutboxRecord,
    prepared: _PreparedOutboxEnqueue,
) -> bool:
    return (
        row.notification_type == prepared.notification_type
        and row.recipient == prepared.recipient
        and row.severity == prepared.severity
        and row.content_sha256 == prepared.content_sha256
        and row.payload_json == prepared.payload_json
    )


def _outbox_noise_metadata(payload_json: str) -> Optional[dict[str, Any]]:
    try:
        payload = json.loads(payload_json)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    metadata = payload.get("noise_control")
    if not isinstance(metadata, Mapping):
        return None
    try:
        return {
            "dedup_key": _normalize_optional_text(metadata.get("dedup_key")),
            "cooldown_key": _normalize_optional_text(metadata.get("cooldown_key")),
            "dedup_ttl_seconds": max(
                0,
                int(metadata.get("dedup_ttl_seconds") or 0),
            ),
            "cooldown_seconds": max(
                0,
                int(metadata.get("cooldown_seconds") or 0),
            ),
        }
    except (TypeError, ValueError):
        return None


def _noise_keys_overlap(
    planned: Mapping[str, Any],
    existing: Mapping[str, Any],
) -> tuple[bool, bool]:
    dedup_matches = bool(
        planned.get("dedup_ttl_seconds", 0) > 0
        and existing.get("dedup_ttl_seconds", 0) > 0
        and planned.get("dedup_key")
        and planned.get("dedup_key") == existing.get("dedup_key")
    )
    cooldown_matches = bool(
        planned.get("cooldown_seconds", 0) > 0
        and existing.get("cooldown_seconds", 0) > 0
        and planned.get("cooldown_key")
        and planned.get("cooldown_key") == existing.get("cooldown_key")
    )
    return dedup_matches, cooldown_matches


def _find_durable_noise_conflict(
    session: Any,
    *,
    prepared: Sequence[_PreparedOutboxEnqueue],
    current: datetime,
) -> Optional[tuple[NotificationOutboxRecord, str]]:
    """Serialize durable cross-job dedup/cooldown using persisted plans."""
    planned_noise = _outbox_noise_metadata(prepared[0].payload_json)
    if planned_noise is None or not (
        planned_noise["dedup_ttl_seconds"] > 0
        or planned_noise["cooldown_seconds"] > 0
    ):
        return None

    candidates = session.execute(
        select(NotificationOutboxRecord)
        .where(
            NotificationOutboxRecord.job_id.is_not(None),
            NotificationOutboxRecord.job_id != prepared[0].job_id,
            NotificationOutboxRecord.status.in_(
                (
                    OUTBOX_STATUS_PENDING,
                    OUTBOX_STATUS_PROCESSING,
                    OUTBOX_STATUS_SENT,
                    OUTBOX_STATUS_DELIVERY_UNKNOWN,
                )
            ),
        )
        .order_by(NotificationOutboxRecord.id.desc())
    ).scalars()
    for row in candidates:
        existing_noise = _outbox_noise_metadata(row.payload_json)
        if existing_noise is None:
            continue
        dedup_matches, cooldown_matches = _noise_keys_overlap(
            planned_noise,
            existing_noise,
        )
        if not dedup_matches and not cooldown_matches:
            continue
        parent = session.get(AnalysisJobRecord, row.job_id)
        if parent is None or parent.status in {JOB_STATUS_FAILED, JOB_STATUS_CANCELLED}:
            continue
        if row.status in {OUTBOX_STATUS_PENDING, OUTBOX_STATUS_PROCESSING}:
            return row, "busy"
        delivered_at = row.sent_at or row.updated_at
        if dedup_matches and (
            delivered_at + timedelta(seconds=existing_noise["dedup_ttl_seconds"])
            > current
        ):
            return row, "suppressed"
        if cooldown_matches and (
            delivered_at + timedelta(seconds=existing_noise["cooldown_seconds"])
            > current
        ):
            return row, "suppressed"
    return None


def _assert_json_value(value: Any, path: str = "$") -> None:
    if callable(value):
        raise InvalidJobPayloadError(f"callable values are forbidden in durable JSON at {path}")
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise InvalidJobPayloadError(f"non-finite number is forbidden at {path}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise InvalidJobPayloadError(f"JSON object key at {path} must be a string")
            _assert_json_value(item, f"{path}.{key}")
        return
    raise InvalidJobPayloadError(f"unsupported durable JSON value {type(value).__name__} at {path}")


def _assert_no_callable(value: Any, path: str = "$") -> None:
    if callable(value):
        raise InvalidJobPayloadError(f"callable values are forbidden in durable JSON at {path}")
    if isinstance(value, float) and not math.isfinite(value):
        raise InvalidJobPayloadError(f"non-finite number is forbidden at {path}")
    if isinstance(value, Mapping):
        for key, item in value.items():
            _assert_no_callable(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _assert_no_callable(item, f"{path}[{index}]")


def _load_optional_json(value: Optional[str]) -> Any:
    return None if value is None else json.loads(value)


def _job_snapshot(row: AnalysisJobRecord) -> JobSnapshot:
    envelope = _decode_payload_envelope(row.payload_json)
    if str(row.payload_version) != str(envelope["version"]):
        raise InvalidJobPayloadError(
            f"job {row.task_id!r} payload version column/envelope mismatch"
        )
    return JobSnapshot(
        task_id=row.task_id,
        job_type=row.job_type,
        stock_code=row.stock_code,
        stock_name=row.stock_name,
        status=row.status,
        stage=row.stage,
        progress=row.progress,
        message=row.message,
        payload=envelope["data"],
        result=_load_optional_json(row.result_json),
        error_code=row.error_code,
        error_message_sanitized=row.error_message_sanitized,
        report_type=row.report_type,
        analysis_phase=row.analysis_phase,
        query_source=row.query_source,
        trace_id=row.trace_id,
        priority=row.priority,
        attempt=row.attempt,
        max_attempts=row.max_attempts,
        available_at=row.available_at,
        lease_owner=row.lease_owner,
        lease_expires_at=row.lease_expires_at,
        cancel_requested_at=row.cancel_requested_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
        notify=bool(row.notify),
    )


def _normalize_required_text(value: Any, field_name: str) -> str:
    normalized = str(value).strip() if value is not None else ""
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _normalize_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _sanitize_error_message(value: Any, *, limit: int = 2000) -> str:
    """Keep persisted/operator-visible errors bounded and redact common secrets."""

    message = " ".join(str(value).split()) or "Operation failed."
    message = re.sub(
        r"(?i)\b(api[\s_-]?key|access[\s_-]?token|token|password|secret)\b\s*[:=]\s*[^\s,;]+",
        r"\1=[REDACTED]",
        message,
    )
    return message[:limit]


def _coerce_now(value: Optional[datetime]) -> datetime:
    if value is None:
        return utc_naive_now()
    if not isinstance(value, datetime):
        raise TypeError("expected datetime")
    return to_utc_naive_datetime(value)


def _isoformat(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat(timespec="microseconds") + "Z" if value is not None else None


__all__ = [
    "ACTIVE_JOB_STATUSES",
    "ClaimedJob",
    "ClaimedOutboxMessage",
    "DEFAULT_HEARTBEAT_SECONDS",
    "DEFAULT_LEASE_SECONDS",
    "DEFAULT_MAX_ATTEMPTS",
    "DurableJobConflictError",
    "DurableNoiseClaimBusyError",
    "DurableJobHandlerRegistry",
    "DurableJobStore",
    "InvalidJobPayloadError",
    "JOB_STATUS_CANCELLED",
    "JOB_STATUS_CANCEL_REQUESTED",
    "JOB_STATUS_FAILED",
    "JOB_STATUS_PENDING",
    "JOB_STATUS_PROCESSING",
    "JOB_STATUS_SUCCEEDED",
    "JobEnqueueRequest",
    "JobEnqueueResult",
    "JobEvent",
    "JobHeartbeatResult",
    "JobSnapshot",
    "JobNotFoundError",
    "NotificationOutboxStore",
    "OUTBOX_STATUS_DELIVERY_UNKNOWN",
    "OUTBOX_STATUS_CANCELLED",
    "OUTBOX_STATUS_FAILED",
    "OUTBOX_STATUS_PENDING",
    "OUTBOX_STATUS_PROCESSING",
    "OUTBOX_STATUS_SENT",
    "OUTBOX_STATUS_SUPPRESSED",
    "OutboxEnqueueRequest",
    "OutboxEnqueueResult",
    "StaleLeaseError",
    "TERMINAL_JOB_STATUSES",
    "UnknownJobHandlerError",
    "parse_retry_after_seconds",
]
