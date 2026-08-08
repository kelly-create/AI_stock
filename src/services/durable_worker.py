"""Dedicated lease-fenced worker runtime for durable analysis jobs.

Run it with ``python -m src.services.durable_worker``.  The process refuses to
start unless durable jobs are enabled and the database is already at the
current migration version; schema changes remain the migrator's responsibility.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import signal
import sqlite3
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from pydantic import ValidationError

from src.services.durable_job_handlers import (
    DurableExecutionContext,
    DurableHandlerBusyError,
    DurableHandlerExecutionError,
    DurableHandlerUnavailableError,
    DurableJobCancelled,
    bind_durable_execution_context,
    build_default_durable_job_registry,
)
from src.services.durable_jobs import (
    ClaimedJob,
    DurableJobStore,
    DurableNoiseClaimBusyError,
    InvalidJobPayloadError,
    StaleLeaseError,
    parse_retry_after_seconds,
)
from src.services.run_diagnostics import (
    activate_run_diagnostic_context,
    reset_run_diagnostic_context,
    sanitize_diagnostic_text,
)

logger = logging.getLogger(__name__)


class DurableWorkerPreflightError(RuntimeError):
    """Raised when the dedicated worker cannot safely start."""


@dataclass(frozen=True)
class FailureDisposition:
    """Safe persisted failure metadata and retry decision."""

    error_code: str
    message: str
    retryable: bool
    retry_after: Any = None


@dataclass
class _ActiveExecution:
    claimed_job: ClaimedJob
    context: DurableExecutionContext
    future: Optional[Future[Any]] = None


class DurableWorker:
    """Claim and execute typed jobs with one process-level heartbeat loop."""

    def __init__(
        self,
        store: DurableJobStore,
        *,
        worker_id: Optional[str] = None,
        max_workers: int = 3,
        poll_interval_seconds: float = 0.25,
        heartbeat_interval_seconds: Optional[float] = None,
        outbox_dispatcher: Optional[Any] = None,
        owner_lock: Optional[Any] = None,
    ) -> None:
        if (
            isinstance(max_workers, bool)
            or not isinstance(max_workers, int)
            or max_workers < 1
            or max_workers > 64
        ):
            raise ValueError("max_workers must be an integer between 1 and 64")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        heartbeat_interval = (
            float(heartbeat_interval_seconds)
            if heartbeat_interval_seconds is not None
            else float(store.heartbeat_seconds)
        )
        if heartbeat_interval <= 0 or heartbeat_interval >= float(store.lease_seconds):
            raise ValueError("heartbeat interval must be positive and shorter than the lease")

        self.store = store
        self.worker_id = worker_id or _default_worker_id()
        self.max_workers = max_workers
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.heartbeat_interval_seconds = heartbeat_interval
        self.outbox_dispatcher = outbox_dispatcher
        self._owner_lock = owner_lock

        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="durable-job",
        )
        self._active: dict[str, _ActiveExecution] = {}
        self._active_lock = threading.RLock()
        self._stop_accepting = threading.Event()
        self._stop_heartbeat = threading.Event()
        self._started = False
        self._closed = False
        self._heartbeat_thread: Optional[threading.Thread] = None

    @property
    def active_count(self) -> int:
        with self._active_lock:
            return len(self._active)

    @property
    def is_stopping(self) -> bool:
        return self._stop_accepting.is_set()

    def start(self) -> None:
        """Recover expired work and start the single heartbeat loop."""

        if self._closed:
            raise RuntimeError("durable worker is closed")
        if self._started:
            return
        self.store.recover_expired_leases()
        self._started = True
        thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="durable-worker-heartbeat",
        )
        self._heartbeat_thread = thread
        thread.start()

    def request_stop(self) -> None:
        """Stop claiming new jobs; active jobs keep their heartbeat while draining."""

        self._stop_accepting.set()

    def run_claim_cycle(self) -> int:
        """Reap completed work and fill every currently available worker slot."""

        self.start()
        self._reap_finished()
        if self._stop_accepting.is_set():
            return 0

        claimed_count = 0
        while not self._stop_accepting.is_set():
            with self._active_lock:
                if len(self._active) >= self.max_workers:
                    break
            claimed = self.store.claim_next(self.worker_id)
            if claimed is None:
                break
            self._submit(claimed)
            claimed_count += 1
        return claimed_count

    def run_forever(self) -> None:
        """Poll until a stop signal, then drain without abandoning live leases."""

        try:
            self.start()
            while not self._stop_accepting.is_set():
                claimed = self.run_claim_cycle()
                outbox_dispatched = self._dispatch_outbox_once()
                if claimed == 0 and not outbox_dispatched:
                    self._stop_accepting.wait(self.poll_interval_seconds)
        finally:
            self.shutdown(wait=True)

    def run_until_idle(self) -> None:
        """Drain currently claimable work and exit before delayed retries become due."""

        try:
            self.start()
            while True:
                claimed = self.run_claim_cycle()
                outbox_dispatched = self._dispatch_outbox_once()
                if self.active_count == 0 and claimed == 0 and not outbox_dispatched:
                    break
                time.sleep(min(self.poll_interval_seconds, 0.05))
        finally:
            self.request_stop()
            self.shutdown(wait=True)

    def wait_for_idle(self, timeout_seconds: float = 10.0) -> bool:
        """Wait for submitted jobs to finish; primarily useful to embedding callers."""

        deadline = time.monotonic() + max(0.0, timeout_seconds)
        while time.monotonic() <= deadline:
            self._reap_finished()
            if self.active_count == 0:
                return True
            time.sleep(0.01)
        self._reap_finished()
        return self.active_count == 0

    def shutdown(self, *, wait: bool = True) -> None:
        """Stop claiming; a graceful shutdown keeps heartbeats until work drains."""

        if self._closed:
            self._release_owner_lock()
            return
        self.request_stop()
        if wait:
            while True:
                self._reap_finished()
                if self.active_count == 0:
                    break
                time.sleep(0.02)
        elif self.active_count:
            # Stopping heartbeats with live executions would make a nominally
            # graceful API abandon leases.  Callers must explicitly wait.
            raise RuntimeError("cannot close a durable worker while jobs are active")
        try:
            if wait and self.outbox_dispatcher is not None:
                # Finish a bounded number of already-accepted deliveries after the
                # last handler exits. Remaining rows stay durable for the restart.
                try:
                    self.outbox_dispatcher.dispatch_available(max_messages=16)
                except Exception as exc:  # noqa: BLE001 - local resources must still close.
                    logger.warning(
                        "Durable notification shutdown drain failed: message=%s",
                        sanitize_error_message(exc),
                    )

            self._stop_heartbeat.set()
            heartbeat_thread = self._heartbeat_thread
            if heartbeat_thread is not None:
                heartbeat_thread.join(timeout=max(1.0, self.heartbeat_interval_seconds * 2))
            try:
                self.store.heartbeat_component(
                    "durable_worker",
                    self.worker_id,
                    status="stopped",
                )
            except Exception as exc:  # noqa: BLE001 - shutdown must still close local resources.
                logger.warning(
                    "Durable worker final component heartbeat failed: message=%s",
                    sanitize_error_message(exc),
                )
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._closed = True
        finally:
            self._release_owner_lock()

    def _release_owner_lock(self) -> None:
        owner_lock = self._owner_lock
        if owner_lock is None:
            return
        self._owner_lock = None
        try:
            owner_lock.release()
        except Exception as exc:  # noqa: BLE001 - process shutdown must continue.
            logger.warning(
                "Durable worker owner lock release failed: error_type=%s",
                type(exc).__name__,
            )

    def _dispatch_outbox_once(self) -> bool:
        """Interleave at most one serial notification without using job slots."""
        if self.outbox_dispatcher is None or self._stop_accepting.is_set():
            return False
        try:
            result = self.outbox_dispatcher.dispatch_once()
            return result.status != "idle"
        except Exception as exc:  # noqa: BLE001 - notifications cannot stop job execution.
            logger.warning(
                "Durable notification dispatch failed and will retry: message=%s",
                sanitize_error_message(exc),
            )
            return False

    def _submit(self, claimed: ClaimedJob) -> None:
        trace_id = getattr(claimed, "trace_id", None) or claimed.task_id
        context = DurableExecutionContext(
            store=self.store,
            claimed_job=claimed,
            query_id=claimed.task_id,
            trace_id=trace_id,
            cancel_requested=threading.Event(),
            lease_lost=threading.Event(),
        )
        active = _ActiveExecution(claimed_job=claimed, context=context)
        with self._active_lock:
            if claimed.task_id in self._active:
                raise RuntimeError(f"job {claimed.task_id!r} is already active in this worker")
            self._active[claimed.task_id] = active
        try:
            active.future = self._executor.submit(self._execute, active)
        except Exception:
            with self._active_lock:
                self._active.pop(claimed.task_id, None)
            raise

    def _execute(self, active: _ActiveExecution) -> None:
        claimed = active.claimed_job
        context = active.context
        try:
            with bind_durable_execution_context(context):
                payload = claimed.payload
                diagnostic_token = activate_run_diagnostic_context(
                    trace_id=context.trace_id,
                    task_id=context.job_id,
                    query_id=context.query_id,
                    stock_code=getattr(payload, "stock_code", None),
                    trigger_source=getattr(payload, "query_source", None) or claimed.job_type,
                    scope=claimed.job_type,
                    stage="starting",
                    attempt_no=claimed.attempt,
                    event_sink=lambda event: self._emit_diagnostic_event(context, event),
                )
                try:
                    context.checkpoint()
                    registered = self.store.registry.resolve(claimed.job_type, claimed.payload_version)
                    result = registered.handler(payload)
                    context.checkpoint()
                    self.store.complete(
                        claimed.task_id,
                        claimed.worker_id,
                        claimed.lease_token,
                        result,
                    )
                finally:
                    reset_run_diagnostic_context(diagnostic_token)
        except DurableJobCancelled:
            self._mark_cancelled(active)
        except StaleLeaseError:
            active.context.lease_lost.set()
            logger.warning(
                "Durable job lease lost; fenced worker will not persist a result: job_id=%s",
                claimed.task_id,
            )
        except Exception as exc:  # noqa: BLE001 - classification is the worker boundary.
            disposition = classify_job_exception(exc)
            logger.warning(
                "Durable job failed: job_id=%s error_code=%s retryable=%s message=%s",
                claimed.task_id,
                disposition.error_code,
                disposition.retryable,
                disposition.message,
            )
            try:
                self.store.fail(
                    claimed.task_id,
                    claimed.worker_id,
                    claimed.lease_token,
                    disposition.error_code,
                    disposition.message,
                    retryable=disposition.retryable,
                    retry_after=disposition.retry_after,
                )
            except StaleLeaseError:
                active.context.lease_lost.set()
                logger.warning(
                    "Durable job failure was fenced by a newer lease: job_id=%s",
                    claimed.task_id,
                )
        finally:
            # ContextVar copies held by timed-out daemon helpers must not remain
            # a valid worker boundary after this execution has terminated.
            context.lease_lost.set()

    def _emit_diagnostic_event(
        self,
        context: DurableExecutionContext,
        event: Mapping[str, Any],
    ) -> None:
        """Persist task flow and update the non-routing provider-health projection."""

        context.flow(event)
        event_type = str(event.get("type") or "")
        if event_type not in {"provider_run", "llm_run"}:
            return
        metadata = event.get("metadata")
        if not isinstance(metadata, Mapping):
            return
        provider = str(
            metadata.get("provider")
            or metadata.get("model")
            or ""
        ).strip()
        if not provider:
            return
        success = str(event.get("severity") or "").lower() == "success"
        scope_key = "data_type" if event_type == "provider_run" else "call_type"
        scope = str(metadata.get(scope_key) or "default").strip() or "default"
        try:
            self.store.record_health(
                kind="provider" if event_type == "provider_run" else "llm",
                provider_key=provider,
                scope=scope,
                status="healthy" if success else "failing",
                success=success,
                latency_ms=metadata.get("duration_ms"),
                error_code=(
                    str(metadata.get("error_type") or "").strip() or None
                ),
                error_message_sanitized=(
                    None if success else str(event.get("message") or "")
                ),
                metadata={"last_job_type": context.claimed_job.job_type},
            )
        except Exception as exc:  # noqa: BLE001 - health is a fail-open projection.
            logger.warning(
                "Durable provider health projection failed: job_id=%s message=%s",
                context.job_id,
                sanitize_error_message(exc),
            )

    def _mark_cancelled(self, active: _ActiveExecution) -> None:
        claimed = active.claimed_job
        try:
            self.store.fail(
                claimed.task_id,
                claimed.worker_id,
                claimed.lease_token,
                "cancelled",
                "Cancellation requested.",
                retryable=False,
            )
        except StaleLeaseError:
            active.context.lease_lost.set()

    def _reap_finished(self) -> None:
        with self._active_lock:
            completed = [
                task_id
                for task_id, active in self._active.items()
                if active.future is not None and active.future.done()
            ]
            for task_id in completed:
                active = self._active.pop(task_id)
                try:
                    active.future.result()
                except Exception as exc:  # pragma: no cover - _execute contains its boundary.
                    logger.error(
                        "Unexpected durable worker future failure: job_id=%s message=%s",
                        task_id,
                        sanitize_error_message(exc),
                    )

    def _heartbeat_loop(self) -> None:
        while not self._stop_heartbeat.is_set():
            self._heartbeat_once()
            self._stop_heartbeat.wait(self.heartbeat_interval_seconds)

    def _heartbeat_once(self) -> None:
        with self._active_lock:
            active_jobs = list(self._active.values())
        component_status = "stopping" if self._stop_accepting.is_set() else ("busy" if active_jobs else "idle")
        try:
            self.store.heartbeat_component(
                "durable_worker",
                self.worker_id,
                status=component_status,
            )
        except Exception as exc:  # noqa: BLE001 - next heartbeat retries health telemetry.
            logger.warning(
                "Durable worker component heartbeat failed: message=%s",
                sanitize_error_message(exc),
            )

        for active in active_jobs:
            if active.future is not None and active.future.done():
                continue
            claimed = active.claimed_job
            try:
                heartbeat = self.store.heartbeat(
                    claimed.task_id,
                    claimed.worker_id,
                    claimed.lease_token,
                )
                if heartbeat.cancel_requested:
                    active.context.cancel_requested.set()
            except StaleLeaseError:
                active.context.lease_lost.set()
            except Exception as exc:  # noqa: BLE001 - retain lease until a definitive stale response.
                logger.warning(
                    "Durable job heartbeat failed and will retry: job_id=%s message=%s",
                    claimed.task_id,
                    sanitize_error_message(exc),
                )


def classify_job_exception(exc: Exception) -> FailureDisposition:
    """Classify provider/runtime failures without trusting exception messages."""

    chain = list(_exception_chain(exc))
    retry_after = _extract_retry_after(chain)
    if retry_after is not None:
        try:
            parse_retry_after_seconds(retry_after)
        except (TypeError, ValueError, OverflowError):
            retry_after = None
    safe_message = sanitize_error_message(exc)

    if any(isinstance(item, DurableHandlerUnavailableError) for item in chain):
        return FailureDisposition("handler_unavailable", safe_message, False)
    busy_error = next(
        (
            item
            for item in chain
            if isinstance(item, (DurableHandlerBusyError, DurableNoiseClaimBusyError))
        ),
        None,
    )
    if busy_error is not None:
        return FailureDisposition(
            "resource_busy",
            safe_message,
            True,
            getattr(busy_error, "retry_after", None),
        )
    # Tushare exposes a typed 429-equivalent without fabricating an HTTP
    # response object. Import lazily so worker preflight stays independent from
    # optional data-provider initialization.
    from data_provider.tushare_provider import TushareRateLimitError

    if any(isinstance(item, TushareRateLimitError) for item in chain):
        return FailureDisposition("rate_limited", safe_message, True, retry_after)
    if any(isinstance(item, DurableHandlerExecutionError) for item in chain):
        return FailureDisposition("handler_failed", safe_message, False)
    if any(isinstance(item, (PermissionError,)) for item in chain) or _chain_name_contains(
        chain,
        ("permission", "forbidden"),
    ):
        return FailureDisposition("permission_denied", safe_message, False)
    if _chain_name_contains(chain, ("authentication", "unauthorized", "invalidtoken")):
        return FailureDisposition("authentication_failed", safe_message, False)

    statuses = [status for item in chain if (status := _status_code(item)) is not None]
    if 401 in statuses:
        return FailureDisposition("authentication_failed", safe_message, False)
    if 403 in statuses:
        return FailureDisposition("permission_denied", safe_message, False)
    if 429 in statuses:
        return FailureDisposition("rate_limited", safe_message, True, retry_after)
    if 408 in statuses:
        return FailureDisposition("timeout", safe_message, True, retry_after)

    if _chain_name_contains(chain, ("timeout", "timedout")) or any(
        isinstance(item, TimeoutError) for item in chain
    ):
        return FailureDisposition("timeout", safe_message, True, retry_after)
    if _chain_name_contains(
        chain,
        (
            "connectionerror",
            "connecterror",
            "connectorerror",
            "connecttimeout",
            "connectionreset",
            "connectionaborted",
            "brokenpipe",
            "gaierror",
            "networkerror",
        ),
    ) or any(isinstance(item, ConnectionError) for item in chain):
        return FailureDisposition("connection_failed", safe_message, True, retry_after)
    if any(500 <= status <= 599 for status in statuses):
        return FailureDisposition("provider_server_error", safe_message, True, retry_after)

    if any(
        isinstance(item, (InvalidJobPayloadError, ValidationError, ValueError, TypeError))
        for item in chain
    ):
        return FailureDisposition("validation_error", safe_message, False)
    client_status = next((status for status in statuses if 400 <= status <= 499), None)
    if client_status is not None:
        return FailureDisposition(f"provider_http_{client_status}", safe_message, False)
    return FailureDisposition("handler_error", safe_message, False)


def sanitize_error_message(value: Any, *, limit: int = 1000) -> str:
    """Bound and redact exception text before logs or durable persistence."""

    message = " ".join(str(value).split()) or "Operation failed."
    message = re.sub(
        r"(?i)(authorization\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+",
        r"\1[REDACTED]",
        message,
    )
    message = re.sub(
        r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]+",
        r"\1[REDACTED]",
        message,
    )
    message = re.sub(
        r"(?i)\b(api[\s_-]?key|access[\s_-]?token|refresh[\s_-]?token|token|password|secret)"
        r"\b\s*[:=]\s*['\"]?[^\s,'\";}&]+",
        r"\1=[REDACTED]",
        message,
    )
    message = re.sub(
        r"(?i)(https?://)([^/@\s:]+):([^/@\s]+)@",
        r"\1[REDACTED]@",
        message,
    )
    message = re.sub(
        r"(?i)([?&](?:api[_-]?key|access[_-]?token|token|password|secret)=)[^&#\s]+",
        r"\1[REDACTED]",
        message,
    )
    return sanitize_diagnostic_text(message, max_length=max(1, int(limit))) or "Operation failed."


def preflight_worker(config: Any, database_url: str, *, checker: Any = None) -> None:
    """Fail closed before constructing a write-capable database manager."""

    if not bool(getattr(config, "durable_jobs_enabled", False)):
        raise DurableWorkerPreflightError(
            "DURABLE_JOBS_ENABLED is false; dedicated worker startup refused."
        )
    if checker is None:
        from src.migrations import check_migration_state

        checker = check_migration_state
    state = checker(database_url)
    if not state.is_compatible or not state.is_current or state.error:
        detail = state.error or "pending=" + ",".join(state.pending_versions)
        raise DurableWorkerPreflightError(
            "Database migration is not current; run `python -m src.migrations --apply` "
            f"before the worker ({sanitize_error_message(detail)})."
        )


def build_worker_from_runtime(
    *,
    database_url: Optional[str] = None,
    worker_id: Optional[str] = None,
    max_workers: Optional[int] = None,
    poll_interval_seconds: float = 0.25,
) -> DurableWorker:
    """Create a preflighted worker without permitting implicit migrations."""

    from src.config import get_config
    from src.storage import DatabaseManager

    config = get_config()
    resolved_url = database_url or config.get_db_url()
    preflight_worker(config, resolved_url)

    owner_lock = None
    if bool(getattr(config, "tushare_research_enabled", False)):
        from src.services.research.worker_owner import acquire_tushare_worker_owner

        owner_lock = acquire_tushare_worker_owner(resolved_url, enabled=True)

    # A dedicated worker must never become a competing first-DDL writer.  The
    # read-only preflight above proves the schema is current; explicit mode then
    # makes DatabaseManager independently enforce that invariant.
    try:
        config.database_migration_mode = "explicit"
        db_manager = DatabaseManager(resolved_url)
        registry = build_default_durable_job_registry()
        store = DurableJobStore(registry, db_manager)
        resolved_worker_id = worker_id or _default_worker_id()
        # Import notification delivery only after feature and migration preflight,
        # keeping a failed startup read-only and free of provider initialization.
        from src.services.durable_jobs import NotificationOutboxStore
        from src.services.notification_outbox_dispatcher import NotificationOutboxDispatcher

        outbox_dispatcher = NotificationOutboxDispatcher(
            NotificationOutboxStore(db_manager),
            worker_id=f"{resolved_worker_id}:outbox",
            health_recorder=store,
        )
        return DurableWorker(
            store,
            worker_id=resolved_worker_id,
            max_workers=(int(config.max_workers) if max_workers is None else max_workers),
            poll_interval_seconds=poll_interval_seconds,
            outbox_dispatcher=outbox_dispatcher,
            owner_lock=owner_lock,
        )
    except Exception:
        if owner_lock is not None:
            owner_lock.release()
        raise


def check_worker_heartbeat(
    database_url: str,
    *,
    worker_id: Optional[str] = None,
    max_age_seconds: float = 45.0,
    now: Optional[datetime] = None,
) -> bool:
    """Read-only liveness check for a fresh, accepting durable worker heartbeat."""

    if max_age_seconds <= 0:
        raise ValueError("max_age_seconds must be positive")

    from sqlalchemy.engine import make_url

    from src.storage import to_utc_naive_datetime, utc_naive_now

    url = make_url(database_url)
    if url.get_backend_name() != "sqlite" or not url.database or url.database == ":memory:":
        return False
    database_path = Path(url.database).expanduser()
    if not database_path.is_absolute():
        database_path = database_path.resolve()
    if not database_path.is_file():
        return False

    sql = (
        "SELECT status, last_checked_at FROM provider_health "
        "WHERE kind = ? AND provider_key = ?"
    )
    params: list[Any] = ["component", "durable_worker"]
    if worker_id:
        sql += " AND scope = ?"
        params.append(worker_id)
    sql += " ORDER BY last_checked_at DESC LIMIT 1"

    connection: Optional[sqlite3.Connection] = None
    try:
        connection = sqlite3.connect(
            database_path.resolve().as_uri() + "?mode=ro",
            uri=True,
            timeout=2.0,
        )
        row = connection.execute(sql, params).fetchone()
    except sqlite3.Error:
        return False
    finally:
        if connection is not None:
            connection.close()
    if row is None or row[0] not in {"idle", "busy"} or row[1] is None:
        return False

    try:
        checked_at = (
            row[1]
            if isinstance(row[1], datetime)
            else datetime.fromisoformat(str(row[1]).removesuffix("Z"))
        )
        checked_at = to_utc_naive_datetime(checked_at)
    except (TypeError, ValueError):
        return False
    current = to_utc_naive_datetime(now) if now is not None else utc_naive_now()
    age = current - checked_at
    return -timedelta(seconds=5) <= age <= timedelta(seconds=max_age_seconds)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run the DSA durable analysis worker")
    parser.add_argument("--database-url", help="Explicit SQLAlchemy SQLite URL")
    parser.add_argument(
        "--worker-id",
        default=os.getenv("DURABLE_WORKER_ID"),
        help="Stable worker instance identifier (defaults to DURABLE_WORKER_ID)",
    )
    parser.add_argument("--workers", type=int, help="Concurrent handler slots; defaults to MAX_WORKERS")
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--once", action="store_true", help="Drain currently claimable jobs and exit")
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="Exit after checking a fresh worker heartbeat in the migrated database",
    )
    parser.add_argument(
        "--max-heartbeat-age",
        type=float,
        default=os.getenv("DURABLE_WORKER_HEALTH_MAX_AGE_SECONDS", "45"),
        help="Maximum heartbeat age accepted by --healthcheck (default: 45 seconds)",
    )
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    if args.healthcheck:
        try:
            from src.config import get_config

            health_config = get_config()
            health_database_url = args.database_url or health_config.get_db_url()
            preflight_worker(health_config, health_database_url)
            return 0 if check_worker_heartbeat(
                health_database_url,
                worker_id=args.worker_id,
                max_age_seconds=args.max_heartbeat_age,
            ) else 1
        except (DurableWorkerPreflightError, TypeError, ValueError) as exc:
            logger.error("Durable worker healthcheck failed: %s", sanitize_error_message(exc))
            return 1
        except Exception as exc:  # noqa: BLE001 - healthcheck must fail closed.
            logger.error("Durable worker healthcheck failed: %s", sanitize_error_message(exc))
            return 1

    try:
        worker = build_worker_from_runtime(
            database_url=args.database_url,
            worker_id=args.worker_id,
            max_workers=args.workers,
            poll_interval_seconds=args.poll_interval,
        )
    except (DurableWorkerPreflightError, ValueError) as exc:
        logger.error("Durable worker preflight failed: %s", sanitize_error_message(exc))
        return 2
    except Exception as exc:  # noqa: BLE001 - startup errors must be redacted.
        logger.error("Durable worker startup failed: %s", sanitize_error_message(exc))
        return 1

    previous_handlers: dict[int, Any] = {}

    def _request_stop(signum: int, _frame: Any) -> None:
        logger.info("Durable worker stop requested: signal=%s", signum)
        worker.request_stop()

    for signal_name in ("SIGINT", "SIGTERM"):
        signal_value = getattr(signal, signal_name, None)
        if signal_value is not None:
            previous_handlers[signal_value] = signal.getsignal(signal_value)
            signal.signal(signal_value, _request_stop)

    try:
        if args.once:
            worker.run_until_idle()
        else:
            worker.run_forever()
        return 0
    except KeyboardInterrupt:
        worker.request_stop()
        worker.shutdown(wait=True)
        return 130
    except Exception as exc:  # noqa: BLE001 - process boundary with redacted output.
        logger.error("Durable worker stopped after an error: %s", sanitize_error_message(exc))
        return 1
    finally:
        for signal_value, previous in previous_handlers.items():
            signal.signal(signal_value, previous)


def _default_worker_id() -> str:
    return f"durable-worker-{os.getpid()}-{uuid.uuid4().hex[:12]}"


def _exception_chain(exc: Exception) -> Iterable[Exception]:
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while isinstance(current, Exception) and id(current) not in seen and len(seen) < 8:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _chain_name_contains(chain: Iterable[Exception], fragments: Iterable[str]) -> bool:
    lowered = tuple(fragment.lower() for fragment in fragments)
    return any(
        any(fragment in item.__class__.__name__.lower() for fragment in lowered)
        for item in chain
    )


def _status_code(exc: Exception) -> Optional[int]:
    raw = getattr(exc, "status_code", None)
    if raw is None:
        raw = getattr(getattr(exc, "response", None), "status_code", None)
    if raw is None:
        raw = getattr(exc, "code", None)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if 100 <= value <= 599 else None


def _extract_retry_after(chain: Iterable[Exception]) -> Any:
    for exc in chain:
        direct_value = getattr(exc, "retry_after", None)
        if direct_value is not None:
            return direct_value
        response = getattr(exc, "response", None)
        for headers in (getattr(response, "headers", None), getattr(exc, "headers", None)):
            if headers is None:
                continue
            getter = getattr(headers, "get", None)
            if callable(getter):
                value = getter("Retry-After")
                if value is None:
                    value = getter("retry-after")
                if value is not None:
                    return value
    return None


if __name__ == "__main__":  # pragma: no cover - exercised through CLI invocation.
    raise SystemExit(main())


__all__ = [
    "DurableWorker",
    "DurableWorkerPreflightError",
    "FailureDisposition",
    "build_worker_from_runtime",
    "check_worker_heartbeat",
    "classify_job_exception",
    "main",
    "preflight_worker",
    "sanitize_error_message",
]
