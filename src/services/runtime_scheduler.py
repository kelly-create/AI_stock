# -*- coding: utf-8 -*-
"""Runtime scheduler service for long-lived API/Web/Desktop processes."""

from __future__ import annotations

import logging
import os
import threading
import _thread
import time
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Set

from src.config import Config, get_config
from src.scheduler import Scheduler, normalize_schedule_times

logger = logging.getLogger(__name__)
CLI_SCHEDULER_OWNER_ENV = "DSA_CLI_SCHEDULER_OWNS_SCHEDULE"
RUNTIME_SCHEDULER_FORCE_ENABLED_ENV = "DSA_RUNTIME_SCHEDULER_FORCE_ENABLED"
RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV = "DSA_RUNTIME_SCHEDULER_RUN_IMMEDIATELY"
RUNTIME_SCHEDULER_SUPPRESS_START_ENV = "DSA_RUNTIME_SCHEDULER_SUPPRESS_START"
RUNTIME_SCHEDULER_ARGS_ENV = "DSA_RUNTIME_SCHEDULER_ARGS"
_RUNTIME_ANALYSIS_LOCK = threading.Lock()
SCHEDULE_ARGS_OVERRIDE_KEYS = {
    "no_notify",
    "no_market_review",
    "dry_run",
    "force_run",
    "single_notify",
    "no_context_snapshot",
    "workers",
    "portfolio",
}


def _durable_jobs_enabled(config: Any) -> bool:
    """Return the restart-latched durable scheduler mode for this process."""

    return getattr(config, "durable_jobs_enabled", False) is True


def durable_worker_heartbeat_is_fresh(config: Config) -> bool:
    """Check the configured durable Worker heartbeat without changing readiness."""

    from src.services.durable_worker import check_worker_heartbeat

    return check_worker_heartbeat(
        config.get_db_url(),
        worker_id=getattr(config, "durable_worker_id", None),
        max_age_seconds=float(
            getattr(config, "durable_worker_health_max_age_seconds", 45)
        ),
    )


def wait_for_durable_worker(
    config: Config,
    *,
    heartbeat_checker: Optional[Callable[[Config], bool]] = None,
    timeout_seconds: Optional[float] = None,
    poll_interval_seconds: float = 1.0,
    monotonic: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> bool:
    """Bound scheduler startup until a fresh Worker heartbeat is observable."""

    timeout = (
        float(timeout_seconds)
        if timeout_seconds is not None
        else float(getattr(config, "durable_worker_startup_timeout_seconds", 120))
    )
    if timeout <= 0:
        raise ValueError("durable worker startup timeout must be positive")
    if poll_interval_seconds <= 0:
        raise ValueError("durable worker poll interval must be positive")
    checker = heartbeat_checker or durable_worker_heartbeat_is_fresh
    deadline = monotonic() + timeout
    while True:
        try:
            if checker(config):
                return True
        except Exception as exc:  # noqa: BLE001 - startup probe remains fail closed.
            logger.warning("Durable Worker heartbeat probe failed: %s", exc)
        remaining = deadline - monotonic()
        if remaining <= 0:
            return False
        sleeper(min(poll_interval_seconds, remaining))


def _submit_durable_scheduler_job(
    job_type: str,
    payload: Dict[str, Any],
    *,
    stock_code: str,
    dedupe_key: str,
    notify: bool,
    message: str,
) -> Any:
    """Submit one typed scheduler job while coalescing an active older tick."""

    from src.services.durable_jobs import DurableJobConflictError
    from src.services.task_queue import get_task_queue

    queue = get_task_queue()
    if queue.durable_enabled is not True:
        raise RuntimeError(
            "Scheduler requested a durable job but the process task queue is not durable; "
            "restart every DSA component with the same DURABLE_JOBS_ENABLED value."
        )
    try:
        task = queue.submit_typed_job(
            job_type,
            payload,
            stock_code=stock_code,
            query_source="scheduler",
            notify=notify,
            message=message,
            stage="queued",
            dedupe_key=dedupe_key,
        )
    except DurableJobConflictError:
        # A live job with this singleton scheduler key may carry the previous
        # runtime payload after a settings change. Let it finish and allow the
        # next tick to enqueue the new payload rather than creating two owners.
        logger.info(
            "Durable scheduler tick coalesced behind an active job: job_type=%s dedupe_key=%s",
            job_type,
            dedupe_key,
        )
        return None
    logger.info(
        "Durable scheduler job available: job_type=%s task_id=%s status=%s",
        job_type,
        task.task_id,
        task.status.value,
    )
    return task


def enqueue_durable_scheduled_analysis(
    config: Config,
    args: Any,
    stock_codes: Optional[List[str]] = None,
) -> Any:
    """Persist one scheduler orchestration job without running providers locally."""

    payload = {
        "stock_codes": list(stock_codes) if stock_codes is not None else None,
        "workers": getattr(args, "workers", None),
        "no_notify": bool(getattr(args, "no_notify", False)),
        "no_market_review": bool(getattr(args, "no_market_review", False)),
        "force_run": bool(getattr(args, "force_run", False)),
        "dry_run": bool(getattr(args, "dry_run", False)),
        "single_notify": bool(getattr(args, "single_notify", False)),
        "no_context_snapshot": bool(getattr(args, "no_context_snapshot", False)),
        "portfolio": getattr(args, "portfolio", None),
    }
    return _submit_durable_scheduler_job(
        "scheduled_analysis",
        payload,
        stock_code="scheduled_analysis",
        dedupe_key="scheduler:scheduled_analysis",
        notify=not payload["no_notify"],
        message="Scheduled analysis queued",
    )


def run_with_global_analysis_lock(
    task_runner: Callable[[Config, Any, Optional[List[str]]], Any],
    config: Config,
    args: Any,
    stock_codes: Optional[List[str]] = None,
    *,
    blocking: bool = True,
) -> bool:
    """Execute a task while holding the shared runtime analysis lock."""
    if not _RUNTIME_ANALYSIS_LOCK.acquire(blocking=blocking):
        return False
    try:
        task_runner(config, args, stock_codes)
    finally:
        _RUNTIME_ANALYSIS_LOCK.release()
    return True


def _agent_event_monitor_interval_seconds(config: Config) -> int:
    """Return the validated Event Monitor polling interval in seconds."""
    interval_minutes = getattr(config, "agent_event_monitor_interval_minutes", 5)
    try:
        interval_minutes = max(1, int(interval_minutes))
    except (TypeError, ValueError):  # pragma: no cover - defensive branch
        logger.warning(
            "Invalid AGENT_EVENT_MONITOR_INTERVAL_MINUTES=%r; use fallback 5",
            interval_minutes,
        )
        interval_minutes = 5
    return interval_minutes * 60


def build_agent_event_monitor_background_tasks(
    config: Config,
    *,
    config_provider: Callable[[], Config],
    durable_enabled: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """Build scheduler background tasks used by the runtime scheduler."""
    if not getattr(config, "agent_event_monitor_enabled", False):
        return []

    interval_seconds = _agent_event_monitor_interval_seconds(config)
    if durable_enabled is True or (
        durable_enabled is None and _durable_jobs_enabled(config)
    ):
        def enqueue_event_monitor() -> None:
            _submit_durable_scheduler_job(
                "event_monitor",
                {"rules": None, "send_notification": True},
                stock_code="event_monitor",
                dedupe_key="scheduler:event_monitor",
                notify=True,
                message="Event monitor cycle queued",
            )

        return [{
            "task": enqueue_event_monitor,
            "interval_seconds": interval_seconds,
            "run_immediately": True,
            "name": "agent_event_monitor",
        }]

    from src.services.alert_worker import AlertWorker

    try:
        alert_worker = AlertWorker(config_provider=config_provider)
    except Exception as exc:  # pragma: no cover - defensive branch
        logger.warning("Failed to initialize AlertWorker for event monitor: %s", exc)
        return []

    def event_monitor_task() -> None:
        stats = alert_worker.run_once()
        triggered_count = stats.get("triggered", 0)
        if triggered_count:
            logger.info("[EventMonitor] triggered %d alert(s)", triggered_count)

    return [{
        "task": event_monitor_task,
        "interval_seconds": interval_seconds,
        "run_immediately": True,
        "name": "agent_event_monitor",
    }]


def _decision_signal_outcome_interval_seconds(config: Config) -> int:
    interval_minutes = getattr(config, "decision_signal_outcome_interval_minutes", 30)
    try:
        interval_minutes = max(5, int(interval_minutes))
    except (TypeError, ValueError):  # pragma: no cover - defensive branch
        logger.warning(
            "Invalid DECISION_SIGNAL_OUTCOME_INTERVAL_MINUTES=%r; use fallback 30",
            interval_minutes,
        )
        interval_minutes = 30
    return interval_minutes * 60


def _decision_signal_outcome_batch_limit(config: Config) -> int:
    batch_limit = getattr(config, "decision_signal_outcome_batch_limit", 100)
    try:
        return max(1, min(int(batch_limit), 500))
    except (TypeError, ValueError):  # pragma: no cover - defensive branch
        logger.warning(
            "Invalid DECISION_SIGNAL_OUTCOME_BATCH_LIMIT=%r; use fallback 100",
            batch_limit,
        )
        return 100


def build_decision_signal_outcome_background_tasks(
    config: Config,
    *,
    config_provider: Optional[Callable[[], Config]] = None,
    durable_enabled: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """Build the local-only maintenance task that advances signal outcomes."""
    if not getattr(config, "decision_signal_outcome_enabled", False):
        return []

    if durable_enabled is True or (
        durable_enabled is None and _durable_jobs_enabled(config)
    ):
        def enqueue_outcome_task() -> None:
            runtime_config = config_provider() if config_provider is not None else config
            batch_limit = _decision_signal_outcome_batch_limit(runtime_config)
            _submit_durable_scheduler_job(
                "decision_signal_outcomes",
                {"limit": batch_limit},
                stock_code="decision_signal_outcomes",
                dedupe_key="scheduler:decision_signal_outcomes",
                notify=False,
                message="Decision signal outcomes queued",
            )

        return [{
            "task": enqueue_outcome_task,
            "interval_seconds": _decision_signal_outcome_interval_seconds(config),
            "run_immediately": True,
            "name": "decision_signal_outcomes",
        }]

    from src.services.decision_signal_outcome_service import DecisionSignalOutcomeService

    service = DecisionSignalOutcomeService()

    def outcome_task() -> None:
        runtime_config = config_provider() if config_provider is not None else config
        batch_limit = _decision_signal_outcome_batch_limit(runtime_config)
        stats = service.run_outcomes(limit=batch_limit)
        logger.info(
            "[DecisionSignalOutcome] evaluated=%d created=%d updated=%d skipped=%d",
            int(stats.get("evaluated", 0)),
            int(stats.get("created", 0)),
            int(stats.get("updated", 0)),
            int(stats.get("skipped", 0)),
        )

    return [{
        "task": outcome_task,
        "interval_seconds": _decision_signal_outcome_interval_seconds(config),
        "run_immediately": True,
        "name": "decision_signal_outcomes",
    }]


def _decision_outcome_v2_interval_seconds(config: Config) -> int:
    interval_minutes = getattr(config, "decision_outcome_v2_interval_minutes", 60)
    try:
        return max(1, min(int(interval_minutes), 1440)) * 60
    except (TypeError, ValueError):  # pragma: no cover - defensive branch
        logger.warning(
            "Invalid DECISION_OUTCOME_V2_INTERVAL_MINUTES=%r; use fallback 60",
            interval_minutes,
        )
        return 60 * 60


def _decision_outcome_v2_batch_limit(config: Config) -> int:
    batch_limit = getattr(config, "decision_outcome_v2_batch_limit", 100)
    try:
        return max(1, min(int(batch_limit), 500))
    except (TypeError, ValueError):  # pragma: no cover - defensive branch
        logger.warning(
            "Invalid DECISION_OUTCOME_V2_BATCH_LIMIT=%r; use fallback 100",
            batch_limit,
        )
        return 100


def build_decision_outcome_v2_background_tasks(
    config: Config,
    *,
    config_provider: Optional[Callable[[], Config]] = None,
    durable_enabled: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """Submit Outcome v2 work without importing providers in the scheduler."""

    if not getattr(config, "decision_outcome_v2_enabled", False):
        return []
    is_durable = (
        durable_enabled is True
        or (durable_enabled is None and _durable_jobs_enabled(config))
    )
    if not is_durable:
        logger.error(
            "Decision Outcome v2 requires DURABLE_JOBS_ENABLED; "
            "the scheduler will not evaluate provider data locally"
        )
        return []

    def enqueue_outcome_v2_task() -> None:
        runtime_config = config_provider() if config_provider is not None else config
        if not getattr(runtime_config, "decision_outcome_v2_enabled", False):
            return
        _submit_durable_scheduler_job(
            "decision_outcomes_v2",
            {
                "signal_id": None,
                "horizons": ["5d", "10d", "20d"],
                "stock_code": None,
                "decision_profile": None,
                "limit": _decision_outcome_v2_batch_limit(runtime_config),
                "notify": False,
            },
            stock_code="decision-outcome-v2",
            dedupe_key="scheduler:decision_outcomes_v2",
            notify=False,
            message="Decision Outcome v2 maintenance queued",
        )

    return [{
        "task": enqueue_outcome_v2_task,
        "interval_seconds": _decision_outcome_v2_interval_seconds(config),
        "run_immediately": True,
        "name": "decision_outcomes_v2",
    }]


class RuntimeSchedulerService:
    """Manage scheduled analysis inside the current API/Web/Desktop process."""

    def __init__(
        self,
        *,
        config_provider: Callable[[], Config] = get_config,
        task_runner: Optional[Callable[[Config, Any, Optional[List[str]]], Any]] = None,
        owns_schedule: Optional[bool] = None,
        force_enabled: bool = False,
        run_immediately_in_background: bool = False,
        background_tasks_provider: Optional[Callable[[Config], List[Dict[str, Any]]]] = None,
        schedule_args_overrides: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._config_provider = config_provider
        self._task_runner = task_runner
        if owns_schedule is None:
            owns_schedule = os.getenv(CLI_SCHEDULER_OWNER_ENV, "").strip().lower() not in {
                "1",
                "true",
                "yes",
                "on",
            }
        self._owns_schedule = owns_schedule
        self._force_enabled = force_enabled
        self._run_immediately_in_background = run_immediately_in_background
        self._durable_enabled: Optional[bool] = None
        self._background_tasks_provider = background_tasks_provider
        self._schedule_args_overrides = {
            key: value
            for key, value in (schedule_args_overrides or {}).items()
            if key in SCHEDULE_ARGS_OVERRIDE_KEYS
        }
        self._background_task_cache: Dict[str, Dict[str, Any]] = {}
        self._background_task_registered_names: Set[str] = set()
        self._lock = threading.RLock()
        self._run_lock = _RUNTIME_ANALYSIS_LOCK
        self._scheduler: Optional[Scheduler] = None
        self._thread: Optional[threading.Thread] = None
        self._enabled = False
        self._last_run_at: Optional[str] = None
        self._last_success_at: Optional[str] = None
        self._last_error: Optional[str] = None
        self._last_skipped_at: Optional[str] = None
        self._last_skip_reason: Optional[str] = None

    def _is_durable_mode(self, config: Config) -> bool:
        if self._durable_enabled is None:
            self._durable_enabled = _durable_jobs_enabled(config)
        return self._durable_enabled

    def _make_schedule_args(self) -> SimpleNamespace:
        defaults = {
            "schedule": True,
            "no_run_immediately": True,
            "no_notify": False,
            "no_market_review": False,
            "dry_run": False,
            "force_run": False,
            "single_notify": False,
            "no_context_snapshot": False,
            "market_review": False,
            "serve": False,
            "serve_only": True,
            "stocks": None,
            "portfolio": None,
            "workers": None,
        }
        defaults.update(self._schedule_args_overrides)
        return SimpleNamespace(**defaults)

    def _reload_config(self) -> Config:
        from main import _reload_runtime_config

        return _reload_runtime_config()

    def _record_analysis_busy_skip(self) -> None:
        self._last_skipped_at = datetime.now().isoformat()
        self._last_skip_reason = "analysis_already_running"
        logger.warning("Runtime scheduler skipped run: analysis already running")

    def _run_analysis_locked(self, stock_codes: Optional[List[str]]) -> None:
        try:
            config = self._reload_config()
            self._last_run_at = datetime.now().isoformat()
            if self._is_durable_mode(config):
                enqueue_durable_scheduled_analysis(
                    config,
                    self._make_schedule_args(),
                    stock_codes,
                )
            else:
                runner = self._task_runner
                if runner is None:
                    from main import run_scheduled_analysis

                    runner = run_scheduled_analysis
                result = runner(config, self._make_schedule_args(), stock_codes)
                if result is False:
                    raise RuntimeError("runtime scheduled analysis reported failure")
            self._last_success_at = datetime.now().isoformat()
            self._last_error = None
        except Exception as exc:  # noqa: BLE001 - scheduled runs must not kill API process.
            self._last_error = str(exc)
            logger.exception("Runtime scheduled analysis failed: %s", exc)

    def _run_analysis_once(self, stock_codes: Optional[List[str]] = None) -> bool:
        if not self._run_lock.acquire(blocking=False):
            self._record_analysis_busy_skip()
            return False
        try:
            self._run_analysis_locked(stock_codes)
        finally:
            self._run_lock.release()
        return True

    def _current_times(self) -> List[str]:
        config = self._config_provider()
        return normalize_schedule_times(
            getattr(config, "schedule_times", None),
            fallback_time=getattr(config, "schedule_time", "18:00"),
        )

    def _is_schedule_enabled(self, config: Config) -> bool:
        return self._force_enabled or bool(getattr(config, "schedule_enabled", False))

    def _current_background_tasks(self, config: Config) -> List[Dict[str, Any]]:
        if self._background_tasks_provider is not None:
            return self._background_tasks_provider(config)
        return [
            *self._current_agent_event_monitor_background_tasks(config),
            *self._current_decision_signal_outcome_background_tasks(config),
            *self._current_decision_outcome_v2_background_tasks(config),
        ]

    def _current_agent_event_monitor_background_tasks(self, config: Config) -> List[Dict[str, Any]]:
        name = "agent_event_monitor"
        if not getattr(config, "agent_event_monitor_enabled", False):
            self._background_task_cache.pop(name, None)
            self._background_task_registered_names.discard(name)
            return []

        cached = self._background_task_cache.get(name)
        if cached is None:
            entries = build_agent_event_monitor_background_tasks(
                config,
                config_provider=self._reload_config,
                durable_enabled=self._is_durable_mode(config),
            )
            if not entries:
                self._background_task_cache.pop(name, None)
                self._background_task_registered_names.discard(name)
                return []
            cached = dict(entries[0])
            cached["name"] = name
            self._background_task_cache[name] = cached
            interval_seconds = int(cached["interval_seconds"])
        else:
            interval_seconds = _agent_event_monitor_interval_seconds(config)

        run_immediately = (
            bool(cached.get("run_immediately", False))
            and name not in self._background_task_registered_names
        )
        self._background_task_registered_names.add(name)
        return [{
            "task": cached["task"],
            "interval_seconds": interval_seconds,
            "run_immediately": run_immediately,
            "name": name,
        }]

    def _current_decision_signal_outcome_background_tasks(self, config: Config) -> List[Dict[str, Any]]:
        name = "decision_signal_outcomes"
        if not getattr(config, "decision_signal_outcome_enabled", False):
            self._background_task_cache.pop(name, None)
            self._background_task_registered_names.discard(name)
            return []

        cached = self._background_task_cache.get(name)
        if cached is None:
            entries = build_decision_signal_outcome_background_tasks(
                config,
                config_provider=self._reload_config,
                durable_enabled=self._is_durable_mode(config),
            )
            if not entries:
                self._background_task_cache.pop(name, None)
                self._background_task_registered_names.discard(name)
                return []
            cached = dict(entries[0])
            cached["name"] = name
            self._background_task_cache[name] = cached

        run_immediately = (
            bool(cached.get("run_immediately", False))
            and name not in self._background_task_registered_names
        )
        self._background_task_registered_names.add(name)
        return [{
            "task": cached["task"],
            "interval_seconds": _decision_signal_outcome_interval_seconds(config),
            "run_immediately": run_immediately,
            "name": name,
        }]

    def _current_decision_outcome_v2_background_tasks(
        self,
        config: Config,
    ) -> List[Dict[str, Any]]:
        name = "decision_outcomes_v2"
        if not getattr(config, "decision_outcome_v2_enabled", False):
            self._background_task_cache.pop(name, None)
            self._background_task_registered_names.discard(name)
            return []

        cached = self._background_task_cache.get(name)
        if cached is None:
            entries = build_decision_outcome_v2_background_tasks(
                config,
                config_provider=self._reload_config,
                durable_enabled=self._is_durable_mode(config),
            )
            if not entries:
                self._background_task_cache.pop(name, None)
                self._background_task_registered_names.discard(name)
                return []
            cached = dict(entries[0])
            cached["name"] = name
            self._background_task_cache[name] = cached

        run_immediately = (
            bool(cached.get("run_immediately", False))
            and name not in self._background_task_registered_names
        )
        self._background_task_registered_names.add(name)
        return [{
            "task": cached["task"],
            "interval_seconds": _decision_outcome_v2_interval_seconds(config),
            "run_immediately": run_immediately,
            "name": name,
        }]

    @staticmethod
    def _run_in_background_thread(target: Callable[[], None]) -> None:
        """Run a callback in a background thread without blocking startup."""
        try:
            _thread.start_new_thread(target, ())
            return
        except Exception:
            # Best-effort fallback for environments where the low-level thread API
            # is unavailable or restricted.
            thread = threading.Thread(target=target, daemon=True)
            thread.start()

    def start(self, *, run_immediately: bool = False) -> None:
        with self._lock:
            if not self._owns_schedule:
                self.stop()
                return
            config = self._config_provider()
            if not self._is_schedule_enabled(config):
                self.stop()
                return
            if (
                self._is_durable_mode(config)
                and not durable_worker_heartbeat_is_fresh(config)
            ):
                self.stop()
                self._last_error = "durable_worker_heartbeat_not_ready"
                logger.warning(
                    "Runtime scheduler remains stopped until a fresh Durable Worker heartbeat exists"
                )
                return
            background_tasks = self._current_background_tasks(config)
            self.stop()
            times = normalize_schedule_times(
                getattr(config, "schedule_times", None),
                fallback_time=getattr(config, "schedule_time", "18:00"),
            )
            scheduler = Scheduler(
                schedule_time=getattr(config, "schedule_time", "18:00"),
                schedule_times=times,
                schedule_times_provider=self._current_times,
                register_signals=False,
            )
            if run_immediately and self._run_immediately_in_background:
                scheduler.set_daily_task(self._run_analysis_once, run_immediately=False)
            else:
                scheduler.set_daily_task(self._run_analysis_once, run_immediately=run_immediately)
            for entry in background_tasks:
                scheduler.add_background_task(
                    entry["task"],
                    interval_seconds=entry["interval_seconds"],
                    run_immediately=entry.get("run_immediately", False),
                    name=entry.get("name"),
                )
            if run_immediately and self._run_immediately_in_background:
                self._run_in_background_thread(self._run_analysis_once)
            thread = threading.Thread(
                target=scheduler.run,
                daemon=True,
                name="runtime-scheduler",
            )
            self._scheduler = scheduler
            self._thread = thread
            self._enabled = True
            thread.start()

    def stop(self) -> None:
        scheduler = self._scheduler
        if scheduler is not None:
            scheduler.stop()
        self._scheduler = None
        self._thread = None
        self._enabled = False

    def reconcile_from_config(
        self,
        *,
        run_immediately: bool = False,
        clear_enabled_override: bool = False,
    ) -> None:
        if clear_enabled_override:
            self._force_enabled = False
        if not self._owns_schedule:
            self.stop()
            return
        config = self._config_provider()
        if self._is_schedule_enabled(config):
            self.start(run_immediately=run_immediately)
        else:
            self.stop()

    def run_now(self) -> Dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            self._record_analysis_busy_skip()
            return {
                "accepted": False,
                "running": True,
                "reason": "analysis_already_running",
            }

        def run_and_release() -> None:
            try:
                self._run_analysis_locked(None)
            finally:
                self._run_lock.release()

        worker = threading.Thread(
            target=run_and_release,
            daemon=True,
            name="runtime-scheduler-run-now",
        )
        try:
            worker.start()
        except Exception:
            self._run_lock.release()
            raise
        return {"accepted": True, "running": True}

    def status(self) -> Dict[str, Any]:
        scheduler = self._scheduler
        jobs = scheduler.schedule.get_jobs() if scheduler is not None else []
        next_run = None
        if jobs:
            next_run = min(job.next_run for job in jobs).isoformat()
        if scheduler is not None:
            schedule_times = list(getattr(scheduler, "schedule_times", []))
        else:
            try:
                schedule_times = self._current_times()
            except Exception:  # pragma: no cover - defensive status fallback
                schedule_times = []
        running = self._run_lock.locked()
        return {
            "enabled": self._enabled,
            "running": running,
            "schedule_times": schedule_times,
            "next_run_at": next_run,
            "last_run_at": self._last_run_at,
            "last_success_at": self._last_success_at,
            "last_error": self._last_error,
            "last_skipped_at": self._last_skipped_at,
            "last_skip_reason": self._last_skip_reason,
        }
