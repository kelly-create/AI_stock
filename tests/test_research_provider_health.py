"""Low-write Tushare provider health projection contracts."""

from __future__ import annotations

from data_provider.tushare_provider import TushareRequestCoordinator
from src.services.research.provider_health import TushareProviderHealthReporter


class _Clock:
    def __init__(self) -> None:
        self.value = 100.0

    def monotonic(self) -> float:
        return self.value


class _Store:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.error: Exception | None = None

    def record_health(self, **kwargs) -> None:
        if self.error is not None:
            raise self.error
        self.calls.append(kwargs)


def test_reporter_coalesces_interval_but_flushes_status_changes_and_collection_end() -> None:
    clock = _Clock()
    store = _Store()
    coordinator = TushareRequestCoordinator()
    reporter = TushareProviderHealthReporter(
        store,
        coordinator,
        min_interval_seconds=5,
        monotonic=clock.monotonic,
    )

    assert reporter.maybe_flush() is True
    assert store.calls[-1]["status"] == "unknown"
    assert reporter.maybe_flush() is False

    coordinator.record_result(
        "daily",
        success=True,
        rows=3,
        latency_ms=12.5,
    )
    assert reporter.maybe_flush() is True
    assert store.calls[-1]["status"] == "healthy"
    assert store.calls[-1]["success"] is None
    metrics = store.calls[-1]["metadata"]["metrics"]
    assert metrics["total_calls"] == 1
    assert metrics["success_count"] == 1
    assert metrics["total_rows"] == 3
    assert metrics["endpoint_metrics"]["daily"]["rows"] == 3

    coordinator.record_result(
        "daily",
        success=True,
        rows=2,
        latency_ms=10,
    )
    assert reporter.maybe_flush() is False
    clock.value += 5
    assert reporter.maybe_flush() is True
    assert store.calls[-1]["metadata"]["metrics"]["total_calls"] == 2

    coordinator.record_result(
        "cyq_chips",
        success=False,
        rows=0,
        latency_ms=8,
        error_type="TushareRateLimitError",
    )
    assert reporter.maybe_flush() is True
    assert store.calls[-1]["status"] == "degraded"
    assert store.calls[-1]["metadata"]["metrics"]["error_types"] == {
        "TushareRateLimitError": 1
    }

    prior_writes = len(store.calls)
    assert reporter.flush() is True
    assert len(store.calls) == prior_writes + 1
    assert store.calls[-1]["kind"] == "provider"
    assert store.calls[-1]["provider_key"] == "tushare"
    assert store.calls[-1]["scope"] == "account"


def test_reporter_failure_is_fail_open_and_remains_retryable() -> None:
    store = _Store()
    coordinator = TushareRequestCoordinator()
    reporter = TushareProviderHealthReporter(store, coordinator)
    store.error = RuntimeError("database busy")

    assert reporter.flush() is False
    assert store.calls == []

    store.error = None
    assert reporter.maybe_flush() is True
    assert len(store.calls) == 1


def test_reporter_payload_contains_no_provider_url_or_token() -> None:
    store = _Store()
    coordinator = TushareRequestCoordinator()
    coordinator.record_result(
        "daily",
        success=False,
        rows=0,
        latency_ms=1,
        error_type="TushareTransportError",
    )
    reporter = TushareProviderHealthReporter(store, coordinator)

    assert reporter.flush() is True
    serialized = repr(store.calls[-1])
    assert "secret-token" not in serialized
    assert "api.tushare.pro" not in serialized
