# -*- coding: utf-8 -*-
"""Unified Tushare Pro transport and process-wide request coordination.

The production research path must share one account-level rolling request
bucket and one in-flight cap, even when several fetcher/adapter instances use
different compatible gateway URLs. Coordination is keyed by a digest of the
account token so credentials are neither logged nor retained as registry keys;
the URL remains transport identity only.

This module deliberately performs no business retry.  Callers may make retry
decisions from the typed failures and ``retry_after`` metadata, while every
physical request still consumes exactly one coordinator slot.
"""

from __future__ import annotations

from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
import threading
import time
from typing import Any, Callable, Iterator, Mapping, MutableMapping, Optional, Protocol
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import requests


DEFAULT_TUSHARE_HTTP_URL = "http://api.tushare.pro"
_GLOBAL_REQUEST_LIMIT = 450
_GLOBAL_WINDOW_SECONDS = 60.0
_GLOBAL_INFLIGHT_LIMIT = 2
_CANCEL_POLL_SECONDS = 0.1


class CancellationSignal(Protocol):
    """Small protocol supported by ``threading.Event`` and worker stop tokens."""

    def is_set(self) -> bool:
        ...


class TushareProviderError(RuntimeError):
    """Base class for failures produced by the unified provider."""


class TushareAuthenticationError(TushareProviderError):
    """The configured token was rejected."""


class TusharePermissionError(TushareProviderError):
    """The account cannot access the requested endpoint."""


class TushareRateLimitError(TushareProviderError):
    """The remote endpoint rejected a request because of its rate limit."""

    def __init__(self, message: str, *, retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class TushareTimeoutError(TushareProviderError):
    """The HTTP request exceeded its transport timeout."""


class TushareConnectionError(TushareProviderError):
    """The HTTP connection could not be established or completed."""


class TushareResponseSchemaError(TushareProviderError):
    """The response body did not satisfy the Tushare table contract."""


class TushareTransportError(TushareProviderError):
    """The remote server returned an unclassified HTTP failure."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        retry_after: Optional[float] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class TushareApiError(TushareProviderError):
    """Tushare returned a non-zero API result code."""

    def __init__(self, message: str, *, api_code: Any = None) -> None:
        super().__init__(message)
        self.api_code = api_code


class TushareRequestCancelled(TushareProviderError):
    """The caller cancelled while waiting for, or using, request capacity."""


class TushareWorkerBoundaryError(TushareProviderError):
    """A research-only provider call was attempted outside a durable worker."""


def canonicalize_tushare_api_url(value: Any) -> str:
    """Return one credential-preserving identity for equivalent HTTP endpoints."""

    text = str(value or "").strip()
    parsed = urlsplit(text)
    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"} or parsed.hostname is None:
        raise ValueError("Tushare API URL must be an absolute http(s) URL")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Tushare API URL contains an invalid port") from exc

    hostname = parsed.hostname.casefold()
    host = f"[{hostname}]" if ":" in hostname else hostname
    if port is not None and not (
        (scheme == "http" and port == 80)
        or (scheme == "https" and port == 443)
    ):
        host = f"{host}:{port}"
    userinfo = parsed.netloc.rsplit("@", 1)[0] if "@" in parsed.netloc else ""
    netloc = f"{userinfo}@{host}" if userinfo else host
    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, netloc, path, parsed.query, ""))


def resolve_tushare_api_url_from_env(
    *,
    default: Optional[str] = None,
    include_api_alias: bool = True,
) -> Optional[str]:
    """Resolve configured URLs with a deterministic, compatibility-safe priority.

    Research callers accept ``TUSHARE_API_URL`` as the preferred alias.  Legacy
    fetcher paths can set ``include_api_alias=False`` so enabling the new alias
    does not silently reroute them while the research feature remains disabled.
    """

    names = (
        ("TUSHARE_API_URL", "TUSHARE_HTTP_URL")
        if include_api_alias
        else ("TUSHARE_HTTP_URL",)
    )
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return canonicalize_tushare_api_url(value)
    if default is None or not str(default).strip():
        return None
    return canonicalize_tushare_api_url(default)


def _optional_durable_context() -> Any:
    try:
        from src.services.durable_job_handlers import (  # noqa: PLC0415
            get_optional_durable_execution_context,
        )

        return get_optional_durable_execution_context()
    except (ImportError, RuntimeError):
        return None


def _default_worker_context_checker() -> bool:
    """Avoid importing the durable worker stack unless the boundary is enabled."""

    return _optional_durable_context() is not None


@dataclass(frozen=True)
class _DurableStopSignal:
    context: Any

    def is_set(self) -> bool:
        return bool(
            self.context.cancel_requested.is_set()
            or self.context.lease_lost.is_set()
        )


def _current_durable_stop_signal() -> Optional[_DurableStopSignal]:
    context = _optional_durable_context()
    return _DurableStopSignal(context) if context is not None else None


def _current_source_call_stop_signal() -> Optional[CancellationSignal]:
    try:
        from src.services.screening.source_guard import (  # noqa: PLC0415
            get_current_source_call_cancel_event,
        )

        return get_current_source_call_cancel_event()
    except ImportError:
        return None


@dataclass(frozen=True)
class _CombinedStopSignal:
    signals: tuple[CancellationSignal, ...]

    def is_set(self) -> bool:
        return any(signal.is_set() for signal in self.signals)


def _combined_stop_signal(
    explicit: Optional[CancellationSignal],
) -> Optional[CancellationSignal]:
    signals = tuple(
        signal
        for signal in (
            explicit,
            _current_source_call_stop_signal(),
            _current_durable_stop_signal(),
        )
        if signal is not None
    )
    if not signals:
        return None
    if len(signals) == 1:
        return signals[0]
    return _CombinedStopSignal(signals)


def _raise_current_durable_stop() -> None:
    context = _optional_durable_context()
    if context is not None:
        context._raise_if_stopped()


def _cancelled(signal: Optional[CancellationSignal]) -> bool:
    return bool(signal is not None and signal.is_set())


def _safe_remote_message(value: Any, *, token: str) -> str:
    """Bound and redact remote text before putting it in an exception."""

    message = str(value or "Tushare request failed").replace("\r", " ").replace("\n", " ")
    if token:
        message = message.replace(token, "[REDACTED]")
    return message[:300]


def _parse_retry_after(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


@dataclass(frozen=True)
class TushareCoordinatorSnapshot:
    """Non-secret state exposed for health checks and deterministic tests."""

    requests_in_window: int
    inflight: int
    peak_inflight: int
    endpoint_requests_in_window: Mapping[str, int]
    endpoint_limits: Mapping[str, int]
    total_calls: int
    success_count: int
    failure_count: int
    total_rows: int
    total_latency_ms: float
    average_latency_ms: Optional[float]
    last_latency_ms: Optional[float]
    last_result_success: Optional[bool]
    last_error_type: Optional[str]
    endpoint_metrics: Mapping[str, Mapping[str, Any]]
    error_types: Mapping[str, int]

    def to_dict(self) -> dict[str, Any]:
        """Return a detached JSON-safe payload with no account credentials."""

        return {
            "requests_in_window": self.requests_in_window,
            "inflight": self.inflight,
            "peak_inflight": self.peak_inflight,
            "endpoint_requests_in_window": dict(self.endpoint_requests_in_window),
            "endpoint_limits": dict(self.endpoint_limits),
            "total_calls": self.total_calls,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "total_rows": self.total_rows,
            "total_latency_ms": self.total_latency_ms,
            "average_latency_ms": self.average_latency_ms,
            "last_latency_ms": self.last_latency_ms,
            "last_result_success": self.last_result_success,
            "last_error_type": self.last_error_type,
            "endpoint_metrics": {
                name: dict(metrics) for name, metrics in self.endpoint_metrics.items()
            },
            "error_types": dict(self.error_types),
        }


@dataclass
class _RequestMetrics:
    calls: int = 0
    success_count: int = 0
    failure_count: int = 0
    rows: int = 0
    latency_ms: float = 0.0
    error_types: MutableMapping[str, int] | None = None

    def __post_init__(self) -> None:
        if self.error_types is None:
            self.error_types = defaultdict(int)

    def snapshot(self) -> dict[str, Any]:
        average = self.latency_ms / self.calls if self.calls else None
        return {
            "calls": self.calls,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "rows": self.rows,
            "latency_ms": round(self.latency_ms, 3),
            "average_latency_ms": round(average, 3) if average is not None else None,
            "error_types": dict(sorted((self.error_types or {}).items())),
        }


class TushareRequestCoordinator:
    """Thread-safe rolling-window limiter with a hard in-flight ceiling.

    ``condition`` and ``monotonic`` are injectable so tests can advance a fake
    clock without sleeping.  Production registry instances always use the hard
    account limits declared at module level.
    """

    def __init__(
        self,
        *,
        global_limit: int = _GLOBAL_REQUEST_LIMIT,
        window_seconds: float = _GLOBAL_WINDOW_SECONDS,
        inflight_limit: int = _GLOBAL_INFLIGHT_LIMIT,
        endpoint_limits: Optional[Mapping[str, int]] = None,
        condition: Optional[threading.Condition] = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if int(global_limit) <= 0:
            raise ValueError("global_limit must be positive")
        if float(window_seconds) <= 0:
            raise ValueError("window_seconds must be positive")
        if int(inflight_limit) <= 0:
            raise ValueError("inflight_limit must be positive")
        self._global_limit = int(global_limit)
        self._window_seconds = float(window_seconds)
        self._inflight_limit = int(inflight_limit)
        self._condition = condition or threading.Condition()
        self._monotonic = monotonic
        self._requests: deque[float] = deque()
        self._endpoint_requests: MutableMapping[str, deque[float]] = defaultdict(deque)
        self._endpoint_limits: dict[str, int] = {}
        self._inflight = 0
        self._peak_inflight = 0
        self._metrics = _RequestMetrics()
        self._endpoint_metrics: MutableMapping[str, _RequestMetrics] = defaultdict(
            _RequestMetrics
        )
        self._last_latency_ms: Optional[float] = None
        self._last_result_success: Optional[bool] = None
        self._last_error_type: Optional[str] = None
        self.tighten_endpoint_limits(endpoint_limits or {})

    def tighten_endpoint_limits(self, limits: Mapping[str, int]) -> None:
        """Register endpoint limits; later registrations may only lower them."""

        with self._condition:
            for raw_name, raw_limit in limits.items():
                name = str(raw_name).strip()
                if not name:
                    raise ValueError("endpoint limit name must not be empty")
                limit = int(raw_limit)
                if limit <= 0:
                    raise ValueError(f"endpoint limit for {name} must be positive")
                limit = min(limit, self._global_limit)
                current = self._endpoint_limits.get(name)
                self._endpoint_limits[name] = limit if current is None else min(current, limit)
            self._condition.notify_all()

    def tighten_limits(
        self,
        *,
        global_calls_per_minute: Optional[int] = None,
        max_inflight: Optional[int] = None,
    ) -> None:
        """Tighten shared account limits without ever widening an existing bucket."""

        with self._condition:
            if global_calls_per_minute is not None:
                requested = int(global_calls_per_minute)
                if requested <= 0:
                    raise ValueError("global_calls_per_minute must be positive")
                self._global_limit = min(
                    self._global_limit,
                    requested,
                    _GLOBAL_REQUEST_LIMIT,
                )
                self._endpoint_limits = {
                    name: min(limit, self._global_limit)
                    for name, limit in self._endpoint_limits.items()
                }
            if max_inflight is not None:
                requested_inflight = int(max_inflight)
                if requested_inflight <= 0:
                    raise ValueError("max_inflight must be positive")
                self._inflight_limit = min(
                    self._inflight_limit,
                    requested_inflight,
                    _GLOBAL_INFLIGHT_LIMIT,
                )
            self._condition.notify_all()

    def _prune(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._requests and self._requests[0] <= cutoff:
            self._requests.popleft()
        for endpoint, timestamps in list(self._endpoint_requests.items()):
            while timestamps and timestamps[0] <= cutoff:
                timestamps.popleft()
            if not timestamps and endpoint not in self._endpoint_limits:
                self._endpoint_requests.pop(endpoint, None)

    def _capacity_wait(self, endpoint: str, now: float) -> float:
        waits: list[float] = []
        if len(self._requests) >= self._global_limit:
            waits.append(self._requests[0] + self._window_seconds - now)
        endpoint_limit = self._endpoint_limits.get(endpoint)
        endpoint_requests = self._endpoint_requests[endpoint]
        if endpoint_limit is not None and len(endpoint_requests) >= endpoint_limit:
            waits.append(endpoint_requests[0] + self._window_seconds - now)
        if self._inflight >= self._inflight_limit:
            # There is no predicted release time; notifications wake us early.
            waits.append(_CANCEL_POLL_SECONDS)
        if not waits:
            return 0.0
        return max(0.001, min(waits))

    def acquire(
        self,
        endpoint: str,
        *,
        cancel_event: Optional[CancellationSignal] = None,
    ) -> None:
        endpoint_name = str(endpoint).strip()
        if not endpoint_name:
            raise ValueError("endpoint must not be empty")
        with self._condition:
            while True:
                if _cancelled(cancel_event):
                    raise TushareRequestCancelled("Tushare request cancelled before transport")
                now = float(self._monotonic())
                self._prune(now)
                wait_seconds = self._capacity_wait(endpoint_name, now)
                if wait_seconds <= 0:
                    self._requests.append(now)
                    self._endpoint_requests[endpoint_name].append(now)
                    self._inflight += 1
                    self._peak_inflight = max(self._peak_inflight, self._inflight)
                    return
                self._condition.wait(timeout=min(wait_seconds, _CANCEL_POLL_SECONDS))

    def release(self) -> None:
        with self._condition:
            if self._inflight <= 0:
                raise RuntimeError("Tushare coordinator release without acquire")
            self._inflight -= 1
            self._condition.notify_all()

    def record_result(
        self,
        endpoint: str,
        *,
        success: bool,
        rows: int,
        latency_ms: float,
        error_type: Optional[str] = None,
    ) -> None:
        """Record one completed physical transport attempt under the coordinator lock."""

        endpoint_name = str(endpoint).strip()
        if not endpoint_name:
            raise ValueError("endpoint must not be empty")
        row_count = max(0, int(rows))
        measured_latency = max(0.0, float(latency_ms))
        normalized_error = str(error_type or "").strip()[:128] or None
        with self._condition:
            for metrics in (self._metrics, self._endpoint_metrics[endpoint_name]):
                metrics.calls += 1
                metrics.rows += row_count
                metrics.latency_ms += measured_latency
                if success:
                    metrics.success_count += 1
                else:
                    metrics.failure_count += 1
                    if normalized_error is not None:
                        assert metrics.error_types is not None
                        metrics.error_types[normalized_error] += 1
            self._last_latency_ms = measured_latency
            self._last_result_success = bool(success)
            self._last_error_type = None if success else normalized_error

    @contextmanager
    def slot(
        self,
        endpoint: str,
        *,
        cancel_event: Optional[CancellationSignal] = None,
    ) -> Iterator[None]:
        self.acquire(endpoint, cancel_event=cancel_event)
        try:
            yield
        finally:
            self.release()

    def snapshot(self) -> TushareCoordinatorSnapshot:
        with self._condition:
            self._prune(float(self._monotonic()))
            return TushareCoordinatorSnapshot(
                requests_in_window=len(self._requests),
                inflight=self._inflight,
                peak_inflight=self._peak_inflight,
                endpoint_requests_in_window={
                    name: len(values) for name, values in self._endpoint_requests.items()
                },
                endpoint_limits=dict(self._endpoint_limits),
                total_calls=self._metrics.calls,
                success_count=self._metrics.success_count,
                failure_count=self._metrics.failure_count,
                total_rows=self._metrics.rows,
                total_latency_ms=round(self._metrics.latency_ms, 3),
                average_latency_ms=(
                    round(self._metrics.latency_ms / self._metrics.calls, 3)
                    if self._metrics.calls
                    else None
                ),
                last_latency_ms=(
                    round(self._last_latency_ms, 3)
                    if self._last_latency_ms is not None
                    else None
                ),
                last_result_success=self._last_result_success,
                last_error_type=self._last_error_type,
                endpoint_metrics={
                    name: metrics.snapshot()
                    for name, metrics in sorted(self._endpoint_metrics.items())
                },
                error_types=dict(sorted((self._metrics.error_types or {}).items())),
            )


_COORDINATORS: dict[str, TushareRequestCoordinator] = {}
_COORDINATORS_LOCK = threading.Lock()


def _coordinator_key(token: str) -> str:
    """Return a non-secret account identity independent of transport URL."""

    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def get_process_tushare_coordinator(
    token: str,
    api_url: str,
    *,
    global_calls_per_minute: int = _GLOBAL_REQUEST_LIMIT,
    max_inflight: int = _GLOBAL_INFLIGHT_LIMIT,
    endpoint_limits: Optional[Mapping[str, int]] = None,
) -> TushareRequestCoordinator:
    """Return the one process coordinator for the same Tushare account."""

    # Validate the transport identity even though account quota ownership is
    # intentionally independent from it.
    canonicalize_tushare_api_url(api_url)
    key = _coordinator_key(token)
    with _COORDINATORS_LOCK:
        coordinator = _COORDINATORS.get(key)
        if coordinator is None:
            coordinator = TushareRequestCoordinator(
                global_limit=min(int(global_calls_per_minute), _GLOBAL_REQUEST_LIMIT),
                inflight_limit=min(int(max_inflight), _GLOBAL_INFLIGHT_LIMIT),
                endpoint_limits=endpoint_limits,
            )
            _COORDINATORS[key] = coordinator
        else:
            coordinator.tighten_limits(
                global_calls_per_minute=global_calls_per_minute,
                max_inflight=max_inflight,
            )
            if endpoint_limits:
                coordinator.tighten_endpoint_limits(endpoint_limits)
        return coordinator


def _reset_process_tushare_coordinators_for_tests() -> None:
    """Clear registry state.  Tests only; never call while requests are active."""

    with _COORDINATORS_LOCK:
        _COORDINATORS.clear()


def build_runtime_tushare_provider(
    *,
    token: str,
    api_url: str = DEFAULT_TUSHARE_HTTP_URL,
    timeout: float = 30,
) -> "TushareProvider":
    """Build a provider from the staged research settings, with safe defaults."""

    from src.config import get_config  # noqa: PLC0415 - avoid config import cycles.

    config = get_config()
    research_enabled = bool(getattr(config, "tushare_research_enabled", False))
    return TushareProvider(
        token=token,
        timeout=timeout,
        api_url=api_url,
        enforce_limits=research_enabled,
        worker_only=research_enabled,
        global_calls_per_minute=int(
            getattr(config, "tushare_global_calls_per_minute", 450)
        ),
        max_inflight=int(getattr(config, "tushare_max_inflight", 2)),
        endpoint_limits=dict(getattr(config, "tushare_endpoint_limits", {}) or {}),
    )


class TushareProvider:
    """Lightweight Tushare Pro client over the unified HTTP transport."""

    def __init__(
        self,
        token: str,
        timeout: float = 30,
        api_url: str = DEFAULT_TUSHARE_HTTP_URL,
        *,
        enforce_limits: bool = True,
        worker_only: bool = False,
        global_calls_per_minute: int = _GLOBAL_REQUEST_LIMIT,
        max_inflight: int = _GLOBAL_INFLIGHT_LIMIT,
        endpoint_limits: Optional[Mapping[str, int]] = None,
        coordinator: Optional[TushareRequestCoordinator] = None,
        worker_context_checker: Optional[Callable[[], bool]] = None,
        transport: Optional[Callable[..., Any]] = None,
        preserve_api_url: bool = False,
    ) -> None:
        cleaned_token = str(token or "").strip()
        raw_url = str(api_url or "").strip()
        canonical_url = canonicalize_tushare_api_url(raw_url)
        cleaned_url = raw_url if preserve_api_url else canonical_url
        if not cleaned_token:
            raise ValueError("Tushare token must not be empty")
        self._token = cleaned_token
        self._timeout = float(timeout)
        self._api_url = cleaned_url
        self._enforce_limits = bool(enforce_limits)
        self._worker_only = bool(worker_only)
        self._worker_context_checker = worker_context_checker or _default_worker_context_checker
        self._transport = transport
        self._coordinator = coordinator or get_process_tushare_coordinator(
            cleaned_token,
            cleaned_url,
            global_calls_per_minute=global_calls_per_minute,
            max_inflight=max_inflight,
            endpoint_limits=endpoint_limits,
        )
        if coordinator is not None:
            coordinator.tighten_limits(
                global_calls_per_minute=global_calls_per_minute,
                max_inflight=max_inflight,
            )
            if endpoint_limits:
                coordinator.tighten_endpoint_limits(endpoint_limits)

    @property
    def coordinator(self) -> TushareRequestCoordinator:
        return self._coordinator

    def _check_worker_boundary(self) -> None:
        if self._worker_only and not self._worker_context_checker():
            raise TushareWorkerBoundaryError(
                "Tushare research requests must execute inside the durable worker"
            )
        if self._worker_only:
            _raise_current_durable_stop()

    def query(
        self,
        api_name: str,
        fields: str = "",
        *,
        _cancel_event: Optional[CancellationSignal] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        endpoint = str(api_name or "").strip()
        if not endpoint:
            raise ValueError("Tushare api_name must not be empty")
        self._check_worker_boundary()
        cancel_event = _combined_stop_signal(_cancel_event)
        if _cancelled(cancel_event):
            _raise_current_durable_stop()
            raise TushareRequestCancelled("Tushare request cancelled before transport")

        req_params = {
            "api_name": endpoint,
            "token": self._token,
            "params": kwargs,
            "fields": fields,
        }
        slot = (
            self._coordinator.slot(endpoint, cancel_event=cancel_event)
            if self._enforce_limits
            else _noop_slot()
        )
        try:
            with slot:
                _raise_current_durable_stop()
                if _cancelled(cancel_event):
                    raise TushareRequestCancelled(
                        "Tushare request cancelled before transport"
                    )
                transport = self._transport or requests.post
                transport_started = time.monotonic()
                try:
                    try:
                        response = transport(
                            self._api_url,
                            json=req_params,
                            timeout=self._timeout,
                        )
                    except requests.Timeout as exc:
                        raise TushareTimeoutError(
                            f"Tushare {endpoint} request timed out"
                        ) from exc
                    except requests.ConnectionError as exc:
                        raise TushareConnectionError(
                            f"Tushare {endpoint} connection failed"
                        ) from exc

                    _raise_current_durable_stop()
                    if _cancelled(cancel_event):
                        raise TushareRequestCancelled(
                            "Tushare request cancelled during transport"
                        )
                    self._raise_for_http_status(response, endpoint=endpoint)
                    frame = self._decode_table(response, endpoint=endpoint)
                except Exception as exc:
                    self._coordinator.record_result(
                        endpoint,
                        success=False,
                        rows=0,
                        latency_ms=(time.monotonic() - transport_started) * 1000.0,
                        error_type=type(exc).__name__,
                    )
                    raise
                self._coordinator.record_result(
                    endpoint,
                    success=True,
                    rows=len(frame.index),
                    latency_ms=(time.monotonic() - transport_started) * 1000.0,
                )
                return frame
        except TushareRequestCancelled:
            # Preserve the durable worker's canonical cancellation/lease-loss
            # exception so its terminal-state classifier remains authoritative.
            _raise_current_durable_stop()
            raise

    def _raise_for_http_status(self, response: Any, *, endpoint: str) -> None:
        status = int(getattr(response, "status_code", 0) or 0)
        if status == 200:
            return
        if status == 401:
            raise TushareAuthenticationError(f"Tushare {endpoint} authentication failed")
        if status == 403:
            raise TusharePermissionError(f"Tushare {endpoint} permission denied")
        if status == 429:
            headers = getattr(response, "headers", {}) or {}
            raise TushareRateLimitError(
                f"Tushare {endpoint} rate limited",
                retry_after=_parse_retry_after(headers.get("Retry-After")),
            )
        headers = getattr(response, "headers", {}) or {}
        raise TushareTransportError(
            f"Tushare {endpoint} HTTP {status or 'unknown'}",
            status_code=status or None,
            retry_after=_parse_retry_after(headers.get("Retry-After")),
        )

    def _decode_table(self, response: Any, *, endpoint: str) -> pd.DataFrame:
        try:
            result = json.loads(str(getattr(response, "text", "")))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise TushareResponseSchemaError(
                f"Tushare {endpoint} returned invalid JSON"
            ) from exc
        if not isinstance(result, dict):
            raise TushareResponseSchemaError(
                f"Tushare {endpoint} response must be an object"
            )

        code = result.get("code")
        if code != 0:
            message = _safe_remote_message(result.get("msg"), token=self._token)
            lowered = message.lower()
            if any(marker in lowered for marker in ("token", "auth", "认证", "无效用户")):
                raise TushareAuthenticationError(message)
            if any(marker in lowered for marker in ("permission", "privilege", "权限", "积分")):
                raise TusharePermissionError(message)
            if any(marker in lowered for marker in ("rate", "limit", "quota", "频率", "每分钟")):
                raise TushareRateLimitError(message)
            raise TushareApiError(message, api_code=code)

        data = result.get("data")
        if not isinstance(data, dict):
            raise TushareResponseSchemaError(
                f"Tushare {endpoint} response data must be an object"
            )
        columns = data.get("fields")
        items = data.get("items")
        if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
            raise TushareResponseSchemaError(
                f"Tushare {endpoint} response fields must be a string list"
            )
        if not isinstance(items, list) or not all(isinstance(row, (list, tuple)) for row in items):
            raise TushareResponseSchemaError(
                f"Tushare {endpoint} response items must be a row list"
            )
        if any(len(row) != len(columns) for row in items):
            raise TushareResponseSchemaError(
                f"Tushare {endpoint} response row width does not match fields"
            )
        return pd.DataFrame(items, columns=columns)

    def __getattr__(self, api_name: str) -> Callable[..., pd.DataFrame]:
        if api_name.startswith("_"):
            raise AttributeError(api_name)

        def caller(**kwargs: Any) -> pd.DataFrame:
            return self.query(api_name, **kwargs)

        return caller


@contextmanager
def _noop_slot() -> Iterator[None]:
    yield


__all__ = [
    "DEFAULT_TUSHARE_HTTP_URL",
    "TushareApiError",
    "TushareAuthenticationError",
    "TushareConnectionError",
    "TusharePermissionError",
    "TushareProvider",
    "TushareProviderError",
    "TushareRateLimitError",
    "TushareRequestCancelled",
    "TushareRequestCoordinator",
    "TushareResponseSchemaError",
    "TushareTimeoutError",
    "TushareTransportError",
    "TushareWorkerBoundaryError",
    "build_runtime_tushare_provider",
    "canonicalize_tushare_api_url",
    "get_process_tushare_coordinator",
    "resolve_tushare_api_url_from_env",
]
