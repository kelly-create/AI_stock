"""Hard coordination and transport contracts for the unified Tushare provider."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
import json
import sys
from threading import Event, Lock, Thread
import time
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

import data_provider.tushare_provider as tushare_provider_module
from data_provider.tushare_fundamental_adapter import TushareFundamentalAdapter
from data_provider.tushare_provider import (
    TushareAuthenticationError,
    TushareConnectionError,
    TusharePermissionError,
    TushareProvider,
    TushareRateLimitError,
    TushareRequestCancelled,
    TushareRequestCoordinator,
    TushareResponseSchemaError,
    TushareTimeoutError,
    TushareTransportError,
    TushareWorkerBoundaryError,
    _reset_process_tushare_coordinators_for_tests,
    get_process_tushare_coordinator,
)
from src.services.durable_job_handlers import bind_durable_execution_context
from src.services.screening import daily as screening_daily
from src.services.screening import snapshot as screening_snapshot
from src.services.screening.source_guard import SourceCallTimeout, call_with_timeout


class _Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def monotonic(self) -> float:
        return self.value


class _AdvancingCondition:
    """Condition-compatible fake that advances monotonic time on waits."""

    def __init__(self, clock: _Clock) -> None:
        self._lock = Lock()
        self._clock = clock

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, *_args):
        self._lock.release()

    def wait(self, timeout=None) -> None:
        self._clock.value += float(timeout or 0.0)

    def notify_all(self) -> None:
        return None


def _response(
    *,
    status: int = 200,
    body: object | None = None,
    headers: dict[str, str] | None = None,
) -> SimpleNamespace:
    if body is None:
        body = {"code": 0, "data": {"fields": ["value"], "items": [[1]]}}
    text = body if isinstance(body, str) else json.dumps(body)
    return SimpleNamespace(status_code=status, text=text, headers=headers or {})


@pytest.fixture(autouse=True)
def _clear_process_coordinators() -> None:
    _reset_process_tushare_coordinators_for_tests()
    yield
    _reset_process_tushare_coordinators_for_tests()


def test_process_registry_shares_same_token_and_url_and_only_tightens() -> None:
    first = get_process_tushare_coordinator(
        "token-a",
        "https://gateway.example/tushare",
        global_calls_per_minute=450,
        max_inflight=2,
        endpoint_limits={"daily": 100},
    )
    second = get_process_tushare_coordinator(
        "token-a",
        "https://gateway.example/tushare",
        global_calls_per_minute=999,
        max_inflight=9,
        endpoint_limits={"daily": 200},
    )
    third = get_process_tushare_coordinator(
        "token-a",
        "https://gateway.example/tushare",
        global_calls_per_minute=300,
        max_inflight=1,
        endpoint_limits={"daily": 50},
    )

    assert first is second is third
    assert third.snapshot().endpoint_limits["daily"] == 50
    # A tightened in-flight limit takes effect for all existing clients.
    third.acquire("daily")
    assert third.snapshot().inflight == 1
    third.release()

    assert get_process_tushare_coordinator(
        "token-b", "https://gateway.example/tushare"
    ) is not first


def test_same_account_shares_quota_across_different_transport_urls() -> None:
    first = get_process_tushare_coordinator(
        "same-account",
        "https://primary.example/tushare",
        global_calls_per_minute=450,
        max_inflight=2,
        endpoint_limits={"daily": 100},
    )
    second = get_process_tushare_coordinator(
        "same-account",
        "https://fallback.example/tushare",
        global_calls_per_minute=300,
        max_inflight=1,
        endpoint_limits={"daily": 50},
    )

    assert first is second
    first.acquire("daily")
    first.release()
    assert second.snapshot().requests_in_window == 1
    assert second.snapshot().endpoint_limits["daily"] == 50


def test_equivalent_urls_share_one_450_call_bucket() -> None:
    response = _response()
    providers = (
        TushareProvider(
            token="same-account",
            api_url="HTTPS://GATEWAY.EXAMPLE:443/tushare/",
            transport=lambda *_args, **_kwargs: response,
        ),
        TushareProvider(
            token="same-account",
            api_url="https://gateway.example/tushare",
            transport=lambda *_args, **_kwargs: response,
        ),
    )

    for provider in providers:
        for _ in range(225):
            provider.query("daily")

    assert providers[0].coordinator is providers[1].coordinator
    assert providers[0].coordinator.snapshot().requests_in_window == 450


def test_rolling_account_bucket_holds_the_451st_call_until_60_seconds() -> None:
    clock = _Clock()
    coordinator = TushareRequestCoordinator(
        global_limit=450,
        window_seconds=60,
        inflight_limit=2,
        condition=_AdvancingCondition(clock),  # type: ignore[arg-type]
        monotonic=clock.monotonic,
    )

    for _ in range(450):
        coordinator.acquire("daily")
        coordinator.release()
    assert coordinator.snapshot().requests_in_window == 450

    coordinator.acquire("daily_basic")
    coordinator.release()
    assert clock.value >= 60.0
    assert coordinator.snapshot().requests_in_window == 1


def test_endpoint_bucket_can_only_tighten_the_global_bucket() -> None:
    clock = _Clock()
    coordinator = TushareRequestCoordinator(
        global_limit=10,
        window_seconds=1,
        endpoint_limits={"cyq_chips": 2},
        condition=_AdvancingCondition(clock),  # type: ignore[arg-type]
        monotonic=clock.monotonic,
    )
    for _ in range(2):
        coordinator.acquire("cyq_chips")
        coordinator.release()
    coordinator.acquire("cyq_chips")
    coordinator.release()
    assert clock.value >= 1.0
    assert coordinator.snapshot().requests_in_window == 1


def test_three_stock_mixed_requests_share_hard_peak_two() -> None:
    state_lock = Lock()
    active = 0
    peak = 0
    gate = Event()

    def transport(_url, *, json, timeout):
        del timeout
        nonlocal active, peak
        with state_lock:
            active += 1
            peak = max(peak, active)
            if peak >= 2:
                gate.set()
        gate.wait(timeout=1.0)
        time.sleep(0.01)
        with state_lock:
            active -= 1
        return _response(
            body={
                "code": 0,
                "data": {
                    "fields": ["ts_code", "endpoint"],
                    "items": [[json["params"]["ts_code"], json["api_name"]]],
                },
            }
        )

    equivalent_urls = (
        "HTTPS://GATEWAY.EXAMPLE:443/tushare/",
        "https://gateway.example/tushare",
        "https://gateway.example:443/tushare//",
    )
    providers = [
        TushareProvider(
            token="shared-token",
            api_url=equivalent_urls[index],
            transport=transport,
        )
        for index in range(3)
    ]
    requests_to_make = [
        (providers[index], endpoint, code)
        for index, code in enumerate(("600519.SH", "601398.SH", "300750.SZ"))
        for endpoint in ("daily", "fina_indicator", "cyq_chips")
    ]

    with ThreadPoolExecutor(max_workers=9) as pool:
        frames = list(
            pool.map(
                lambda item: item[0].query(item[1], ts_code=item[2]),
                requests_to_make,
            )
        )

    assert len(frames) == 9
    assert peak == 2
    assert providers[0].coordinator is providers[1].coordinator is providers[2].coordinator
    assert providers[0].coordinator.snapshot().requests_in_window == 9


def test_three_stock_mixed_metrics_count_success_failure_rows_and_peak() -> None:
    state_lock = Lock()
    active = 0
    transport_peak = 0
    gate = Event()

    def transport(_url, *, json, timeout):
        del timeout
        nonlocal active, transport_peak
        with state_lock:
            active += 1
            transport_peak = max(transport_peak, active)
            if transport_peak >= 2:
                gate.set()
        gate.wait(timeout=1.0)
        time.sleep(0.005)
        with state_lock:
            active -= 1
        endpoint = json["api_name"]
        code = json["params"]["ts_code"]
        if endpoint == "cyq_chips" and code == "601398.SH":
            return _response(status=429, headers={"Retry-After": "1"})
        row_count = 2 if endpoint == "daily" else 1
        return _response(
            body={
                "code": 0,
                "data": {
                    "fields": ["ts_code", "endpoint"],
                    "items": [[code, endpoint] for _ in range(row_count)],
                },
            }
        )

    providers = [
        TushareProvider(
            token="shared-account",
            api_url="https://gateway.example/tushare",
            transport=transport,
        )
        for _ in range(3)
    ]
    requests_to_make = [
        (providers[index], endpoint, code)
        for index, code in enumerate(("600519.SH", "601398.SH", "300750.SZ"))
        for endpoint in ("daily", "fina_indicator", "cyq_chips")
    ]

    with ThreadPoolExecutor(max_workers=9) as pool:
        futures = [
            pool.submit(provider.query, endpoint, ts_code=code)
            for provider, endpoint, code in requests_to_make
        ]
        outcomes = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except TushareRateLimitError:
                outcomes.append(None)

    snapshot = providers[0].coordinator.snapshot()
    assert sum(item is not None for item in outcomes) == 8
    assert transport_peak == snapshot.peak_inflight == 2
    assert snapshot.inflight == 0
    assert snapshot.requests_in_window == snapshot.total_calls == 9
    assert snapshot.success_count == 8
    assert snapshot.failure_count == 1
    assert snapshot.total_rows == 11
    assert snapshot.error_types == {"TushareRateLimitError": 1}
    assert snapshot.endpoint_metrics["daily"] == {
        "calls": 3,
        "success_count": 3,
        "failure_count": 0,
        "rows": 6,
        "latency_ms": snapshot.endpoint_metrics["daily"]["latency_ms"],
        "average_latency_ms": snapshot.endpoint_metrics["daily"]["average_latency_ms"],
        "error_types": {},
    }
    assert snapshot.endpoint_metrics["cyq_chips"]["calls"] == 3
    assert snapshot.endpoint_metrics["cyq_chips"]["success_count"] == 2
    assert snapshot.endpoint_metrics["cyq_chips"]["failure_count"] == 1
    assert snapshot.endpoint_metrics["cyq_chips"]["rows"] == 2
    assert snapshot.endpoint_metrics["cyq_chips"]["error_types"] == {
        "TushareRateLimitError": 1
    }
    assert snapshot.total_latency_ms >= 0
    assert snapshot.average_latency_ms == pytest.approx(
        snapshot.total_latency_ms / snapshot.total_calls,
        abs=0.001,
    )
    serialized = json.dumps(snapshot.to_dict(), sort_keys=True)
    assert "shared-account" not in serialized
    assert "gateway.example" not in serialized


def test_waiting_request_observes_cancellation() -> None:
    coordinator = TushareRequestCoordinator(inflight_limit=1)
    coordinator.acquire("daily")
    cancelled = Event()
    outcome: list[BaseException] = []

    def wait_for_slot() -> None:
        try:
            coordinator.acquire("daily_basic", cancel_event=cancelled)
        except BaseException as exc:  # noqa: BLE001 - assert exact propagated type.
            outcome.append(exc)

    waiter = Thread(target=wait_for_slot)
    waiter.start()
    time.sleep(0.02)
    cancelled.set()
    waiter.join(timeout=1.0)
    coordinator.release()

    assert not waiter.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], TushareRequestCancelled)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (401, TushareAuthenticationError),
        (403, TusharePermissionError),
    ],
)
def test_http_auth_and_permission_failures_are_typed(status, expected) -> None:
    provider = TushareProvider(
        token="secret-token",
        transport=lambda *_args, **_kwargs: _response(status=status),
    )
    with pytest.raises(expected):
        provider.query("daily")


def test_http_429_preserves_retry_after() -> None:
    provider = TushareProvider(
        token="secret-token",
        transport=lambda *_args, **_kwargs: _response(
            status=429,
            headers={"Retry-After": "7.5"},
        ),
    )
    with pytest.raises(TushareRateLimitError) as caught:
        provider.query("daily")
    assert caught.value.retry_after == 7.5


def test_transport_error_constructor_remains_message_compatible() -> None:
    error = TushareTransportError("legacy transport failure")

    assert str(error) == "legacy transport failure"
    assert error.status_code is None
    assert error.retry_after is None


@pytest.mark.parametrize("status", [400, 408, 500, 503])
def test_unclassified_http_error_exposes_status_and_retry_after(status: int) -> None:
    provider = TushareProvider(
        token="secret-token",
        transport=lambda *_args, **_kwargs: _response(
            status=status,
            headers={"Retry-After": "11"},
        ),
    )

    with pytest.raises(TushareTransportError) as caught:
        provider.query("daily")

    assert caught.value.status_code == status
    assert caught.value.retry_after == 11.0


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (requests.Timeout("slow"), TushareTimeoutError),
        (requests.ConnectionError("offline"), TushareConnectionError),
    ],
)
def test_transport_timeout_and_connection_failures_are_typed(failure, expected) -> None:
    def transport(*_args, **_kwargs):
        raise failure

    provider = TushareProvider(token="secret-token", transport=transport)
    with pytest.raises(expected):
        provider.query("daily")


@pytest.mark.parametrize(
    "body",
    [
        "not-json",
        {"code": 0, "data": None},
        {"code": 0, "data": {"fields": "ts_code", "items": []}},
        {"code": 0, "data": {"fields": ["a", "b"], "items": [[1]]}},
    ],
)
def test_response_schema_drift_is_typed(body) -> None:
    provider = TushareProvider(
        token="secret-token",
        transport=lambda *_args, **_kwargs: _response(body=body),
    )
    with pytest.raises(TushareResponseSchemaError):
        provider.query("daily")


def test_remote_error_never_echoes_token() -> None:
    provider = TushareProvider(
        token="top-secret-token",
        transport=lambda *_args, **_kwargs: _response(
            body={
                "code": -1,
                "msg": "invalid token top-secret-token",
                "data": {"fields": [], "items": []},
            }
        ),
    )
    with pytest.raises(TushareAuthenticationError) as caught:
        provider.query("daily")
    assert "top-secret-token" not in str(caught.value)
    assert "[REDACTED]" in str(caught.value)


def test_worker_only_boundary_rejects_before_transport() -> None:
    called = False

    def transport(*_args, **_kwargs):
        nonlocal called
        called = True
        return _response()

    provider = TushareProvider(
        token="secret-token",
        worker_only=True,
        worker_context_checker=lambda: False,
        transport=transport,
    )
    with pytest.raises(TushareWorkerBoundaryError):
        provider.query("daily")
    assert called is False


def test_durable_cancellation_is_translated_to_worker_exception(monkeypatch) -> None:
    class DurableCancelled(RuntimeError):
        pass

    cancelled = Event()
    cancelled.set()

    def raise_if_stopped() -> None:
        raise DurableCancelled("cancelled")

    context = SimpleNamespace(
        cancel_requested=cancelled,
        lease_lost=Event(),
        _raise_if_stopped=raise_if_stopped,
    )
    monkeypatch.setattr(
        tushare_provider_module,
        "_optional_durable_context",
        lambda: context,
    )
    provider = TushareProvider(
        token="secret-token",
        worker_only=True,
        transport=lambda *_args, **_kwargs: pytest.fail("transport must not run"),
    )

    with pytest.raises(DurableCancelled, match="cancelled"):
        provider.query("daily")


def test_source_guard_and_nested_screening_threads_copy_context(monkeypatch) -> None:
    marker: ContextVar[str] = ContextVar("tushare-test-marker", default="missing")
    token = marker.set("durable-context")
    try:
        assert call_with_timeout(
            marker.get,
            timeout_sec=1,
            label="context-probe",
        ) == "durable-context"

        seen: list[str] = []

        def history_fetcher(_code, **_kwargs):
            seen.append(marker.get())
            return pd.DataFrame({"close": [1.0]})

        monkeypatch.setattr(screening_daily, "compute_daily_features", lambda _frame: {})
        screening_daily.enrich_daily_features(
            pd.DataFrame({"code": ["600519", "601398", "300750"]}),
            max_workers=3,
            history_fetcher=history_fetcher,
        )
        assert seen == ["durable-context"] * 3
    finally:
        marker.reset(token)


def test_source_timeout_cancels_late_worker_context_copy_before_transport() -> None:
    transport_called = Event()
    helper_finished = Event()
    context = SimpleNamespace(
        cancel_requested=Event(),
        lease_lost=Event(),
        _raise_if_stopped=lambda: None,
    )
    provider = TushareProvider(
        token="secret-token",
        worker_only=True,
        enforce_limits=False,
        transport=lambda *_args, **_kwargs: transport_called.set() or _response(),
    )

    def late_call() -> None:
        try:
            time.sleep(0.05)
            provider.query("daily")
        finally:
            helper_finished.set()

    with bind_durable_execution_context(context):
        with pytest.raises(SourceCallTimeout):
            call_with_timeout(late_call, timeout_sec=0.001, label="late-tushare")

    assert helper_finished.wait(1.0)
    assert not transport_called.is_set()


def test_fundamental_endpoint_threads_copy_context() -> None:
    marker: ContextVar[str] = ContextVar("fundamental-test-marker", default="missing")
    seen: list[str] = []

    class Client:
        def query(self, _api_name, **_kwargs):
            seen.append(marker.get())
            return pd.DataFrame()

    token = marker.set("durable-context")
    try:
        frames, errors = TushareFundamentalAdapter()._fetch_frames(
            Client(), "600519.SH"
        )
    finally:
        marker.reset(token)

    assert len(frames) == 8
    assert not errors
    assert seen == ["durable-context"] * 8


def test_screening_snapshot_uses_unified_provider(monkeypatch) -> None:
    calls: list[str] = []
    captured: dict[str, object] = {}

    class Provider:
        def daily(self, **_kwargs):
            calls.append("daily")
            return pd.DataFrame({"ts_code": ["600519.SH"]})

        def daily_basic(self, **_kwargs):
            calls.append("daily_basic")
            return pd.DataFrame({"ts_code": ["600519.SH"]})

        def stock_basic(self, **_kwargs):
            calls.append("stock_basic")
            return pd.DataFrame({"ts_code": ["600519.SH"]})

    def factory(**kwargs):
        captured.update(kwargs)
        return Provider()

    monkeypatch.setenv("TUSHARE_TOKEN", "screening-token")
    monkeypatch.setenv("TUSHARE_TRADE_DATE", "20260808")
    monkeypatch.delenv("TUSHARE_API_URL", raising=False)
    monkeypatch.delenv("TUSHARE_HTTP_URL", raising=False)
    monkeypatch.setattr(screening_snapshot, "_tushare_research_enabled", lambda: True)
    monkeypatch.setattr(screening_snapshot, "build_runtime_tushare_provider", factory)
    monkeypatch.setattr(
        screening_snapshot,
        "_prepare_tushare_snapshot",
        lambda *_frames: pd.DataFrame({"code": ["600519"]}),
    )

    result = screening_snapshot._fetch_tushare()

    assert result["code"].tolist() == ["600519"]
    assert calls == ["daily", "daily_basic", "stock_basic"]
    assert captured == {
        "token": "screening-token",
        "api_url": "http://api.tushare.pro",
    }


def test_screening_daily_uses_unified_provider(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class Provider:
        def daily(self, **_kwargs):
            return pd.DataFrame(
                {
                    "ts_code": ["600519.SH"],
                    "trade_date": ["20260808"],
                    "open": [1.0],
                    "high": [2.0],
                    "low": [0.5],
                    "close": [1.5],
                    "vol": [10.0],
                    "amount": [20.0],
                }
            )

    def factory(**kwargs):
        captured.update(kwargs)
        return Provider()

    monkeypatch.setenv("TUSHARE_TOKEN", "screening-token")
    monkeypatch.setenv("TUSHARE_DAILY_ADJ", "none")
    monkeypatch.delenv("TUSHARE_API_URL", raising=False)
    monkeypatch.delenv("TUSHARE_HTTP_URL", raising=False)
    monkeypatch.setattr(screening_daily, "_tushare_research_enabled", lambda: True)
    monkeypatch.setattr(screening_daily, "build_runtime_tushare_provider", factory)

    result = screening_daily._fetch_daily_tushare("600519", lookback_days=30)

    assert result["close"].tolist() == [1.5]
    assert captured == {
        "token": "screening-token",
        "api_url": "http://api.tushare.pro",
    }


def test_screening_snapshot_flag_off_preserves_legacy_sdk_and_waditu(monkeypatch) -> None:
    captured_tokens: list[str] = []

    class LegacyClient:
        def daily(self, **_kwargs):
            return pd.DataFrame({"ts_code": ["600519.SH"]})

        def daily_basic(self, **_kwargs):
            return pd.DataFrame({"ts_code": ["600519.SH"]})

        def stock_basic(self, **_kwargs):
            return pd.DataFrame({"ts_code": ["600519.SH"]})

    client = LegacyClient()
    sdk = SimpleNamespace(
        pro_api=lambda token: captured_tokens.append(token) or client,
    )
    monkeypatch.setitem(sys.modules, "tushare", sdk)
    monkeypatch.setenv("TUSHARE_TOKEN", "legacy-token")
    monkeypatch.setenv("TUSHARE_TRADE_DATE", "20260808")
    monkeypatch.delenv("TUSHARE_API_URL", raising=False)
    monkeypatch.delenv("TUSHARE_HTTP_URL", raising=False)
    monkeypatch.setattr(screening_snapshot, "_tushare_research_enabled", lambda: False)
    monkeypatch.setattr(
        screening_snapshot,
        "build_runtime_tushare_provider",
        lambda **_kwargs: pytest.fail("flag-off path must not build unified provider"),
    )
    monkeypatch.setattr(
        screening_snapshot,
        "_prepare_tushare_snapshot",
        lambda *_frames: pd.DataFrame({"code": ["600519"]}),
    )

    result = screening_snapshot._fetch_tushare()

    assert result["code"].tolist() == ["600519"]
    assert captured_tokens == ["legacy-token"]
    assert client._DataApi__token == "legacy-token"
    assert client._DataApi__http_url == "http://api.waditu.com"


def test_screening_daily_flag_off_preserves_legacy_sdk_and_exception(monkeypatch) -> None:
    class LegacyFailure(RuntimeError):
        pass

    failure = LegacyFailure("legacy-sdk-contract")

    class LegacyClient:
        def daily(self, **_kwargs):
            raise failure

    client = LegacyClient()
    sdk = SimpleNamespace(pro_api=lambda _token: client)
    monkeypatch.setitem(sys.modules, "tushare", sdk)
    monkeypatch.setenv("TUSHARE_TOKEN", "legacy-token")
    monkeypatch.setenv("TUSHARE_DAILY_ADJ", "none")
    monkeypatch.delenv("TUSHARE_API_URL", raising=False)
    monkeypatch.delenv("TUSHARE_HTTP_URL", raising=False)
    monkeypatch.setattr(screening_daily, "_tushare_research_enabled", lambda: False)
    monkeypatch.setattr(
        screening_daily,
        "build_runtime_tushare_provider",
        lambda **_kwargs: pytest.fail("flag-off path must not build unified provider"),
    )

    with pytest.raises(LegacyFailure) as caught:
        screening_daily._fetch_daily_tushare("600519", lookback_days=30)

    assert caught.value is failure
    assert client._DataApi__token == "legacy-token"
    assert client._DataApi__http_url == "http://api.waditu.com"
