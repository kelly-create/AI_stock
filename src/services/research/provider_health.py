"""Low-write projection of process-wide Tushare metrics into provider_health."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from data_provider.tushare_provider import TushareRequestCoordinator


logger = logging.getLogger(__name__)


class TushareProviderHealthReporter:
    """Coalesce provider metrics without making telemetry part of job correctness."""

    def __init__(
        self,
        store: Any,
        coordinator: TushareRequestCoordinator,
        *,
        min_interval_seconds: float = 5.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if min_interval_seconds <= 0:
            raise ValueError("min_interval_seconds must be positive")
        if not callable(getattr(store, "record_health", None)):
            raise TypeError("store must expose record_health")
        if not isinstance(coordinator, TushareRequestCoordinator):
            raise TypeError("coordinator must be a TushareRequestCoordinator")
        self.store = store
        self.coordinator = coordinator
        self.min_interval_seconds = float(min_interval_seconds)
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._last_flush_at: float | None = None
        self._last_status: str | None = None

    def maybe_flush(self, *, force: bool = False) -> bool:
        """Write one merged snapshot on status change, interval, or explicit flush.

        Health persistence is observability-only: SQLite contention or any other
        recorder failure returns ``False`` and remains eligible for a later retry.
        """

        with self._lock:
            now = float(self._monotonic())
            metrics = self.coordinator.snapshot()
            status = _provider_status(metrics.total_calls, metrics.last_result_success)
            interval_elapsed = (
                self._last_flush_at is None
                or now - self._last_flush_at >= self.min_interval_seconds
            )
            if not force and status == self._last_status and not interval_elapsed:
                return False
            try:
                self.store.record_health(
                    kind="provider",
                    provider_key="tushare",
                    scope="account",
                    status=status,
                    success=None,
                    latency_ms=metrics.last_latency_ms,
                    metadata={
                        "metrics_version": 1,
                        "metrics": metrics.to_dict(),
                    },
                )
            except Exception as exc:  # noqa: BLE001 - telemetry must not fail research.
                logger.warning(
                    "Tushare provider health projection failed: error_type=%s",
                    type(exc).__name__,
                )
                return False
            self._last_flush_at = now
            self._last_status = status
            return True

    def flush(self) -> bool:
        """Force one collection-boundary projection."""

        return self.maybe_flush(force=True)


def _provider_status(total_calls: int, last_result_success: bool | None) -> str:
    if total_calls <= 0 or last_result_success is None:
        return "unknown"
    return "healthy" if last_result_success else "degraded"


__all__ = ["TushareProviderHealthReporter"]
