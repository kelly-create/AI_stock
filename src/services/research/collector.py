"""Durable, point-in-time Tushare dataset collection orchestration."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
import re
import threading
from types import MappingProxyType
import json
from typing import Any, Callable, Literal, Mapping, Optional, Protocol, Sequence

import pandas as pd
from sqlalchemy import func, or_, select

from data_provider.tushare_provider import (
    TushareApiError,
    TushareAuthenticationError,
    TushareConnectionError,
    TusharePermissionError,
    TushareProvider,
    TushareRateLimitError,
    TushareRequestCancelled,
    TushareTimeoutError,
    TushareTransportError,
)
from src.services.durable_jobs import StaleLeaseError
from src.storage import AnalysisJobRecord, JobEventRecord, ResearchDatasetSnapshotRecord

from .availability import (
    DATASET_DEFINITIONS,
    DEFAULT_RESEARCH_DATASETS,
    DatasetDefinition,
    as_of_trade_date,
    assess_dataset_rows,
    format_tushare_date,
    normalize_as_of,
    normalize_frame_rows,
    to_tushare_ts_code,
)
from .datasets import DatasetStatus
from .canonical import canonical_json
from .raw_store import RawArtifactStore
from .repositories import (
    DatasetSnapshotInput,
    LeaseFence,
    ResearchSnapshotRepository,
    ResearchReferenceTimeConflictError,
    SnapshotWriteResult,
)


ResearchReferenceMode = Literal["live", "historical"]


class CancellationSignal(Protocol):
    def is_set(self) -> bool:
        ...


@dataclass(frozen=True)
class DatasetCollectionResult:
    dataset: str
    status: str
    row_count: int
    snapshot: Optional[SnapshotWriteResult]
    query_params: Mapping[str, Any]
    normalized_rows: tuple[Mapping[str, Any], ...]
    available_at: datetime
    data_as_of: datetime
    trade_date: Optional[date] = None
    report_date: Optional[date] = None
    announcement_date: Optional[date] = None
    raw_ref: Optional[Mapping[str, Any]] = None
    dropped_future_rows: int = 0
    dropped_unknown_availability_rows: int = 0
    invalid_value_rows: int = 0
    error_code: Optional[str] = None
    error_message_sanitized: Optional[str] = None
    retry_after: Optional[float] = None
    retryable: bool = False
    skipped: bool = False
    reused: bool = False
    source_snapshot_hashes: tuple[str, ...] = ()

    def to_frame(self) -> pd.DataFrame:
        """Return a detached DataFrame for legacy consumers without refetching."""

        return pd.DataFrame([dict(row) for row in self.normalized_rows])


@dataclass(frozen=True)
class ResearchCollectionResult:
    stock_code: str
    ts_code: Optional[str]
    as_of: datetime
    datasets: tuple[DatasetCollectionResult, ...]

    @property
    def by_dataset(self) -> Mapping[str, DatasetCollectionResult]:
        return MappingProxyType({item.dataset: item for item in self.datasets})

    @property
    def normalized_by_dataset(self) -> Mapping[str, tuple[Mapping[str, Any], ...]]:
        return MappingProxyType(
            {item.dataset: item.normalized_rows for item in self.datasets}
        )

    @property
    def rows_by_dataset(self) -> Mapping[str, tuple[Mapping[str, Any], ...]]:
        """Stable alias used by deterministic factor composition."""

        return self.normalized_by_dataset

    def to_frames(self) -> Mapping[str, pd.DataFrame]:
        """Build detached legacy-compatible frames from the frozen datasets."""

        return {item.dataset: item.to_frame() for item in self.datasets}

    def frames_copy(self) -> Mapping[str, pd.DataFrame]:
        """Return detached frames for legacy fundamental/chip normalization."""

        return self.to_frames()

    @property
    def max_available_at(self) -> datetime:
        return max(
            (item.available_at for item in self.datasets),
            default=self.as_of,
        )


@dataclass
class _CollectionFlight:
    event: threading.Event
    frame: Optional[pd.DataFrame] = None
    error: Optional[BaseException] = None


_CYQ_STATE_LOCK = threading.Lock()
_CYQ_FLIGHTS: dict[tuple[str, str, str, str], _CollectionFlight] = {}
_JOB_DATASET_RESULT_CACHE_MAX_ENTRIES = 256
_CYQ_CACHE_MAX_KEYS = 256
_CYQ_RESULT_CHUNKS_MAX_PER_KEY = 128
_RESULT_CACHE_SINGLE_MAX_ROWS = 512
_RESULT_CACHE_SINGLE_MAX_ESTIMATED_BYTES = 512 * 1024
_JOB_DATASET_RESULT_CACHE_MAX_ROWS = 4096
_JOB_DATASET_RESULT_CACHE_MAX_ESTIMATED_BYTES = 8 * 1024 * 1024
_CYQ_CACHE_MAX_ROWS = 4096
_CYQ_CACHE_MAX_ESTIMATED_BYTES = 8 * 1024 * 1024
_CACHE_ESTIMATE_MAX_DEPTH = 8

_CYQ_KEY_LRU: OrderedDict[tuple[str, str], None] = OrderedDict()
_CYQ_HIGH_WATERMARKS: OrderedDict[tuple[str, str], date] = OrderedDict()
_CYQ_LATEST_RESULTS: OrderedDict[
    tuple[str, str], DatasetCollectionResult
] = OrderedDict()
_CYQ_RESULT_CHUNKS: OrderedDict[
    tuple[str, str], list[DatasetCollectionResult]
] = OrderedDict()
_JOB_DATASET_RESULTS: OrderedDict[
    tuple[str, str, str, str], DatasetCollectionResult
] = OrderedDict()
_CYQ_KEY_WEIGHTS: dict[tuple[str, str], tuple[int, int]] = {}
_JOB_DATASET_RESULT_WEIGHTS: OrderedDict[
    tuple[str, str, str, str], tuple[int, int]
] = OrderedDict()
_CYQ_CACHE_TOTAL_ROWS = 0
_CYQ_CACHE_TOTAL_ESTIMATED_BYTES = 0
_JOB_DATASET_RESULT_CACHE_TOTAL_ROWS = 0
_JOB_DATASET_RESULT_CACHE_TOTAL_ESTIMATED_BYTES = 0


def _bounded_cache_value_size(
    value: Any,
    *,
    remaining: int,
    depth: int = 0,
    seen: Optional[set[int]] = None,
) -> int:
    """Conservatively estimate cached payload size without unbounded work."""

    if remaining <= 0:
        return 1
    if value is None or isinstance(value, (bool, int, float)):
        return min(32, remaining + 1)
    if isinstance(value, str):
        return min(49 + len(value) * 4, remaining + 1)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return min(33 + len(value), remaining + 1)
    if depth >= _CACHE_ESTIMATE_MAX_DEPTH:
        return min(256, remaining + 1)

    seen = seen if seen is not None else set()
    identity = id(value)
    if identity in seen:
        return min(16, remaining + 1)
    seen.add(identity)
    try:
        total = 64
        if isinstance(value, Mapping):
            iterator = value.items()
        elif isinstance(value, (list, tuple, set, frozenset)):
            iterator = ((None, item) for item in value)
        else:
            return min(256, remaining + 1)
        for key, item in iterator:
            if key is not None:
                total += _bounded_cache_value_size(
                    key,
                    remaining=remaining - total,
                    depth=depth + 1,
                    seen=seen,
                )
            total += _bounded_cache_value_size(
                item,
                remaining=remaining - total,
                depth=depth + 1,
                seen=seen,
            )
            if total > remaining:
                return remaining + 1
        return total
    finally:
        seen.discard(identity)


def _result_cache_weight(
    result: DatasetCollectionResult,
) -> Optional[tuple[int, int]]:
    rows = len(result.normalized_rows)
    if rows > _RESULT_CACHE_SINGLE_MAX_ROWS:
        return None
    estimated_bytes = 512 + _bounded_cache_value_size(
        (
            result.normalized_rows,
            result.query_params,
            result.raw_ref,
            result.error_message_sanitized,
            result.source_snapshot_hashes,
        ),
        remaining=_RESULT_CACHE_SINGLE_MAX_ESTIMATED_BYTES - 512,
    )
    if estimated_bytes > _RESULT_CACHE_SINGLE_MAX_ESTIMATED_BYTES:
        return None
    return rows, estimated_bytes


def _evict_cyq_key_locked(key: tuple[str, str]) -> None:
    global _CYQ_CACHE_TOTAL_ROWS
    global _CYQ_CACHE_TOTAL_ESTIMATED_BYTES

    _CYQ_KEY_LRU.pop(key, None)
    _CYQ_HIGH_WATERMARKS.pop(key, None)
    _CYQ_LATEST_RESULTS.pop(key, None)
    _CYQ_RESULT_CHUNKS.pop(key, None)
    rows, estimated_bytes = _CYQ_KEY_WEIGHTS.pop(key, (0, 0))
    _CYQ_CACHE_TOTAL_ROWS -= rows
    _CYQ_CACHE_TOTAL_ESTIMATED_BYTES -= estimated_bytes


def _touch_cyq_key_locked(key: tuple[str, str]) -> None:
    """Touch one shared CYQ key and evict all of its cached projections."""

    _CYQ_KEY_LRU[key] = None
    _CYQ_KEY_LRU.move_to_end(key)
    while len(_CYQ_KEY_LRU) > _CYQ_CACHE_MAX_KEYS:
        evicted = next(iter(_CYQ_KEY_LRU))
        _evict_cyq_key_locked(evicted)


def _reweight_cyq_key_locked(key: tuple[str, str]) -> None:
    """Apply global row/byte budgets to all projections for one CYQ key."""

    global _CYQ_CACHE_TOTAL_ROWS
    global _CYQ_CACHE_TOTAL_ESTIMATED_BYTES

    old_rows, old_bytes = _CYQ_KEY_WEIGHTS.pop(key, (0, 0))
    _CYQ_CACHE_TOTAL_ROWS -= old_rows
    _CYQ_CACHE_TOTAL_ESTIMATED_BYTES -= old_bytes

    candidates = [*_CYQ_RESULT_CHUNKS.get(key, ())]
    latest = _CYQ_LATEST_RESULTS.get(key)
    if latest is not None:
        candidates.append(latest)
    unique: dict[tuple[str, Any], DatasetCollectionResult] = {}
    for result in candidates:
        content_hash = (
            result.snapshot.content_hash if result.snapshot is not None else None
        )
        identity = (
            ("hash", content_hash)
            if content_hash
            else ("object", id(result))
        )
        unique[identity] = result

    rows = 0
    estimated_bytes = 0
    for result in unique.values():
        weight = _result_cache_weight(result)
        if weight is None:
            _evict_cyq_key_locked(key)
            return
        rows += weight[0]
        estimated_bytes += weight[1]
    _CYQ_KEY_WEIGHTS[key] = (rows, estimated_bytes)
    _CYQ_CACHE_TOTAL_ROWS += rows
    _CYQ_CACHE_TOTAL_ESTIMATED_BYTES += estimated_bytes

    while _CYQ_KEY_LRU and (
        _CYQ_CACHE_TOTAL_ROWS > _CYQ_CACHE_MAX_ROWS
        or _CYQ_CACHE_TOTAL_ESTIMATED_BYTES
        > _CYQ_CACHE_MAX_ESTIMATED_BYTES
    ):
        _evict_cyq_key_locked(next(iter(_CYQ_KEY_LRU)))


def _remember_job_result_locked(
    key: tuple[str, str, str, str],
    result: DatasetCollectionResult,
) -> None:
    """Keep only a bounded LRU acceleration layer over durable job events."""

    global _JOB_DATASET_RESULT_CACHE_TOTAL_ROWS
    global _JOB_DATASET_RESULT_CACHE_TOTAL_ESTIMATED_BYTES

    previous_rows, previous_bytes = _JOB_DATASET_RESULT_WEIGHTS.pop(
        key,
        (0, 0),
    )
    _JOB_DATASET_RESULT_CACHE_TOTAL_ROWS -= previous_rows
    _JOB_DATASET_RESULT_CACHE_TOTAL_ESTIMATED_BYTES -= previous_bytes
    _JOB_DATASET_RESULTS.pop(key, None)

    weight = _result_cache_weight(result)
    if weight is None:
        return
    _JOB_DATASET_RESULTS[key] = result
    _JOB_DATASET_RESULT_WEIGHTS[key] = weight
    _JOB_DATASET_RESULTS.move_to_end(key)
    _JOB_DATASET_RESULT_WEIGHTS.move_to_end(key)
    _JOB_DATASET_RESULT_CACHE_TOTAL_ROWS += weight[0]
    _JOB_DATASET_RESULT_CACHE_TOTAL_ESTIMATED_BYTES += weight[1]
    while _JOB_DATASET_RESULTS and (
        len(_JOB_DATASET_RESULTS) > _JOB_DATASET_RESULT_CACHE_MAX_ENTRIES
        or _JOB_DATASET_RESULT_CACHE_TOTAL_ROWS
        > _JOB_DATASET_RESULT_CACHE_MAX_ROWS
        or _JOB_DATASET_RESULT_CACHE_TOTAL_ESTIMATED_BYTES
        > _JOB_DATASET_RESULT_CACHE_MAX_ESTIMATED_BYTES
    ):
        evicted, _unused = _JOB_DATASET_RESULTS.popitem(last=False)
        evicted_rows, evicted_bytes = _JOB_DATASET_RESULT_WEIGHTS.pop(
            evicted,
            (0, 0),
        )
        _JOB_DATASET_RESULT_CACHE_TOTAL_ROWS -= evicted_rows
        _JOB_DATASET_RESULT_CACHE_TOTAL_ESTIMATED_BYTES -= evicted_bytes


def _cyq_result_order_key(
    result: DatasetCollectionResult,
) -> tuple[date, datetime, datetime, int]:
    return (
        result.trade_date or date.min,
        result.available_at,
        result.data_as_of,
        result.snapshot.record_id if result.snapshot is not None else 0,
    )


def _remember_cyq_latest_locked(
    key: tuple[str, str],
    result: DatasetCollectionResult,
) -> None:
    if _result_cache_weight(result) is None:
        _evict_cyq_key_locked(key)
        return
    current = _CYQ_LATEST_RESULTS.get(key)
    if current is None or _cyq_result_order_key(result) >= _cyq_result_order_key(
        current
    ):
        _CYQ_LATEST_RESULTS[key] = result
    _touch_cyq_key_locked(key)
    _reweight_cyq_key_locked(key)


def _reset_collector_state_for_tests() -> None:
    global _CYQ_CACHE_TOTAL_ROWS
    global _CYQ_CACHE_TOTAL_ESTIMATED_BYTES
    global _JOB_DATASET_RESULT_CACHE_TOTAL_ROWS
    global _JOB_DATASET_RESULT_CACHE_TOTAL_ESTIMATED_BYTES

    with _CYQ_STATE_LOCK:
        _CYQ_FLIGHTS.clear()
        _CYQ_KEY_LRU.clear()
        _CYQ_HIGH_WATERMARKS.clear()
        _CYQ_LATEST_RESULTS.clear()
        _CYQ_RESULT_CHUNKS.clear()
        _JOB_DATASET_RESULTS.clear()
        _CYQ_KEY_WEIGHTS.clear()
        _JOB_DATASET_RESULT_WEIGHTS.clear()
        _CYQ_CACHE_TOTAL_ROWS = 0
        _CYQ_CACHE_TOTAL_ESTIMATED_BYTES = 0
        _JOB_DATASET_RESULT_CACHE_TOTAL_ROWS = 0
        _JOB_DATASET_RESULT_CACHE_TOTAL_ESTIMATED_BYTES = 0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_error_message(error: BaseException) -> str:
    from .repositories import sanitize_error_message

    return sanitize_error_message(error) or type(error).__name__


def _transport_status(error: TushareTransportError) -> Optional[int]:
    explicit = getattr(error, "status_code", None)
    try:
        if explicit is not None:
            return int(explicit)
    except (TypeError, ValueError):
        pass
    matched = re.search(r"\bHTTP\s+(\d{3})\b", str(error), re.IGNORECASE)
    return int(matched.group(1)) if matched is not None else None


def _unsupported_api_error(error: TushareApiError) -> bool:
    message = str(error).lower()
    return any(
        marker in message
        for marker in (
            "unsupported",
            "not support",
            "not found",
            "unknown api",
            "接口不存在",
            "不支持",
            "未开放",
        )
    )


class ResearchDatasetCollector:
    """Fetch, freeze, and persist Tushare datasets without internal retries."""

    def __init__(
        self,
        provider: TushareProvider,
        repository: ResearchSnapshotRepository,
        raw_store: RawArtifactStore,
        *,
        clock: Callable[[], datetime] = _utc_now,
        last_trade_date_resolver: Optional[
            Callable[[str, str, date], Optional[date]]
        ] = None,
    ) -> None:
        self.provider = provider
        self.repository = repository
        self.raw_store = raw_store
        self.clock = clock
        self._last_trade_date_resolver = last_trade_date_resolver

    def collect(
        self,
        stock_code: str,
        *,
        as_of: datetime,
        lease: LeaseFence,
        datasets: Sequence[str] = DEFAULT_RESEARCH_DATASETS,
        cancel_event: Optional[CancellationSignal] = None,
        reference_mode: ResearchReferenceMode = "historical",
    ) -> ResearchCollectionResult:
        requested_boundary = normalize_as_of(as_of)
        if reference_mode not in {"live", "historical"}:
            raise ValueError("reference_mode must be 'live' or 'historical'")
        try:
            ts_code = to_tushare_ts_code(stock_code)
        except ValueError:
            ts_code = None
        scope_value = (
            ts_code.split(".", 1)[0]
            if ts_code is not None
            else str(stock_code).strip()
        )
        boundary = self.repository.get_research_reference_time(
            scope_value=scope_value,
            lease=lease,
        )
        effective_mode: ResearchReferenceMode = "historical"
        requested_datasets = tuple(str(item).strip() for item in datasets)
        collected: dict[str, DatasetCollectionResult] = {}
        if boundary is None and reference_mode == "historical":
            boundary = self.repository.establish_research_reference_time(
                scope_value=scope_value,
                reference_time=requested_boundary,
                lease=lease,
            )
        elif boundary is None and "stock_basic" in requested_datasets:
            stock_basic = self.collect_dataset(
                stock_code,
                "stock_basic",
                as_of=requested_boundary,
                lease=lease,
                cancel_event=cancel_event,
                _ts_code=ts_code,
                reference_mode="live",
            )
            collected["stock_basic"] = stock_basic
            boundary = self.repository.get_research_reference_time(
                scope_value=scope_value,
                lease=lease,
            )
            if boundary is None:
                raise RuntimeError(
                    "live stock_basic collection did not establish a research boundary"
                )
        elif boundary is None:
            # A custom live collection without stock_basic still needs one
            # durable point-in-time boundary before any provider call.
            boundary = self.repository.establish_research_reference_time(
                scope_value=scope_value,
                reference_time=normalize_as_of(self.clock()),
                lease=lease,
            )

        assert boundary is not None
        for dataset in requested_datasets:
            if dataset in collected:
                continue
            collected[dataset] = self.collect_dataset(
                stock_code,
                dataset,
                as_of=boundary,
                lease=lease,
                cancel_event=cancel_event,
                _ts_code=ts_code,
                reference_mode=effective_mode,
            )
        results = tuple(collected[dataset] for dataset in requested_datasets)
        return ResearchCollectionResult(
            stock_code=str(stock_code).strip(),
            ts_code=ts_code,
            as_of=boundary,
            datasets=results,
        )

    def collect_dataset(
        self,
        stock_code: str,
        dataset: str,
        *,
        as_of: datetime,
        lease: LeaseFence,
        cancel_event: Optional[CancellationSignal] = None,
        _ts_code: Optional[str] = None,
        reference_mode: ResearchReferenceMode = "historical",
    ) -> DatasetCollectionResult:
        boundary = normalize_as_of(as_of)
        if reference_mode not in {"live", "historical"}:
            raise ValueError("reference_mode must be 'live' or 'historical'")
        dataset_name = str(dataset or "").strip()
        definition = DATASET_DEFINITIONS.get(dataset_name)
        try:
            ts_code = _ts_code or to_tushare_ts_code(stock_code)
        except ValueError as error:
            return self._persist_error_snapshot(
                stock_code=str(stock_code).strip(),
                dataset=dataset_name or "unknown",
                definition=definition,
                boundary=boundary,
                lease=lease,
                status=DatasetStatus.NOT_SUPPORTED.value,
                error_code="unsupported_stock_code",
                error=error,
            )
        if definition is None:
            return self._persist_error_snapshot(
                stock_code=ts_code.split(".", 1)[0],
                dataset=dataset_name or "unknown",
                definition=None,
                boundary=boundary,
                lease=lease,
                status=DatasetStatus.NOT_SUPPORTED.value,
                error_code="unsupported_dataset",
                error=ValueError(f"unsupported research dataset: {dataset_name!r}"),
            )
        scope_value = ts_code.split(".", 1)[0]
        recovered = self._load_job_terminal_result(
            lease,
            definition.name,
            scope_value,
            boundary,
        )
        if recovered is not None:
            if definition.incremental_by_trade_date:
                return self._merge_incremental_window(
                    definition,
                    scope_value,
                    boundary,
                    recovered,
                )
            return recovered
        if definition.current_state and reference_mode == "historical":
            existing = self._load_latest_current_state_result(
                definition.name,
                scope_value,
                boundary,
            )
            if existing is None:
                return DatasetCollectionResult(
                    dataset=definition.name,
                    status=DatasetStatus.EMPTY.value,
                    row_count=0,
                    snapshot=None,
                    query_params=MappingProxyType({}),
                    normalized_rows=(),
                    available_at=boundary,
                    data_as_of=boundary,
                    skipped=True,
                    reused=True,
                )
            return self._bind_existing_result(
                existing,
                definition=definition,
                scope_value=scope_value,
                boundary=boundary,
                lease=lease,
            )
        result = self._collect_supported_dataset(
            ts_code,
            definition,
            boundary=boundary,
            lease=lease,
            cancel_event=cancel_event,
            bootstrap_current_state=(
                definition.current_state and reference_mode == "live"
            ),
        )
        if definition.incremental_by_trade_date:
            result = self._merge_incremental_window(
                definition,
                scope_value,
                boundary,
                result,
            )
        self._remember_terminal_result(
            lease,
            definition.name,
            scope_value,
            (
                result.data_as_of
                if definition.current_state and reference_mode == "live"
                else boundary
            ),
            result,
        )
        return result

    def _collect_supported_dataset(
        self,
        ts_code: str,
        definition: DatasetDefinition,
        *,
        boundary: datetime,
        lease: LeaseFence,
        cancel_event: Optional[CancellationSignal],
        bootstrap_current_state: bool = False,
    ) -> DatasetCollectionResult:
        scope_value = ts_code.split(".", 1)[0]

        def _terminal_boundary() -> datetime:
            if bootstrap_current_state:
                return normalize_as_of(self.clock())
            return boundary

        def _terminal_error(
            error: BaseException,
            *,
            status: str,
            error_code: str,
            query_params: Mapping[str, Any],
            retryable: bool = False,
        ) -> DatasetCollectionResult:
            terminal_boundary = _terminal_boundary()
            return self._persist_error_snapshot(
                stock_code=scope_value,
                dataset=definition.name,
                definition=definition,
                boundary=terminal_boundary,
                lease=lease,
                status=status,
                error_code=error_code,
                error=error,
                query_params=query_params,
                establish_reference_for=(
                    scope_value if bootstrap_current_state else None
                ),
                retryable=retryable,
                observed_at=(
                    terminal_boundary if bootstrap_current_state else None
                ),
            )

        last_trade_date = (
            self._resolve_last_trade_date(definition.name, scope_value, boundary)
            if definition.incremental_by_trade_date
            else None
        )
        query_params = self._build_query_params(
            ts_code,
            definition,
            boundary=boundary,
            last_trade_date=last_trade_date,
        )
        if query_params is None:
            existing = self._load_latest_terminal_result(
                definition.name,
                scope_value,
                boundary,
            )
            if existing is not None:
                return self._bind_existing_incremental_result(
                    existing,
                    definition=definition,
                    scope_value=scope_value,
                    boundary=boundary,
                    lease=lease,
                )
            cutoff_text = format_tushare_date(as_of_trade_date(boundary))
            query_params = {
                "ts_code": ts_code,
                "start_date": cutoff_text,
                "end_date": cutoff_text,
            }
        try:
            if definition.incremental_by_trade_date:
                key = (
                    definition.name,
                    ts_code,
                    str(query_params.get("start_date") or ""),
                    str(query_params.get("end_date") or ""),
                )
                frame = self._query_cyq_single_flight(
                    key,
                    lambda: self.provider.query(
                        definition.name,
                        # The physical request is shared. One caller's stop
                        # token must not cancel another caller's transport;
                        # each caller is fenced immediately after the wait.
                        _cancel_event=None,
                        **query_params,
                    ),
                    cancel_event=cancel_event,
                )
                # A waiter may have been cancelled or lost its lease while the
                # shared transport was in flight. It must never inherit the
                # owner's authority to persist.
                if cancel_event is not None and cancel_event.is_set():
                    raise TushareRequestCancelled(
                        "Tushare collection cancelled after single-flight"
                    )
                self._assert_live_lease_after_wait(lease)
            else:
                frame = self.provider.query(
                    definition.name,
                    _cancel_event=cancel_event,
                    **query_params,
                )
        except TushareRequestCancelled:
            raise
        except (
            TushareRateLimitError,
            TushareTimeoutError,
            TushareConnectionError,
        ) as error:
            if bootstrap_current_state:
                # A transient failure is not a consumed current-state result.
                # Leave the job/stock boundary unset so a later attempt can
                # observe stock_basic and freeze it atomically.
                raise
            # Persist the point-in-time failure for audit, then preserve the
            # typed exception so DurableWorker owns retry/Retry-After policy.
            self._persist_error_snapshot(
                stock_code=scope_value,
                dataset=definition.name,
                definition=definition,
                boundary=boundary,
                lease=lease,
                status=DatasetStatus.FETCH_FAILED.value,
                error_code=type(error).__name__,
                error=error,
                query_params=query_params,
                retryable=True,
            )
            raise
        except (TusharePermissionError, TushareAuthenticationError) as error:
            return _terminal_error(
                error,
                status=DatasetStatus.PERMISSION_DENIED.value,
                error_code=(
                    "permission_denied"
                    if isinstance(error, TusharePermissionError)
                    else "authentication_failed"
                ),
                query_params=query_params,
            )
        except TushareTransportError as error:
            transport_status = _transport_status(error) or 0
            if bootstrap_current_state and (
                transport_status == 408 or transport_status >= 500
            ):
                raise
            result = _terminal_error(
                error,
                status=DatasetStatus.FETCH_FAILED.value,
                error_code=(
                    "tushare_transport_retryable"
                    if transport_status == 408 or transport_status >= 500
                    else "tushare_transport_error"
                ),
                query_params=query_params,
                retryable=(
                    transport_status == 408 or transport_status >= 500
                ),
            )
            if transport_status == 408 or transport_status >= 500:
                raise
            return result
        except TushareApiError as error:
            return _terminal_error(
                error,
                status=(
                    DatasetStatus.NOT_SUPPORTED.value
                    if _unsupported_api_error(error)
                    else DatasetStatus.FETCH_FAILED.value
                ),
                error_code=(
                    "unsupported_endpoint"
                    if _unsupported_api_error(error)
                    else "tushare_api_error"
                ),
                query_params=query_params,
            )
        except Exception as error:
            if isinstance(error, StaleLeaseError) or type(error).__name__ == "DurableJobCancelled":
                raise
            return _terminal_error(
                error,
                status=DatasetStatus.FETCH_FAILED.value,
                error_code=type(error).__name__,
                query_params=query_params,
            )

        try:
            rows, invalid_value_rows = normalize_frame_rows(frame)
        except (TypeError, ValueError) as error:
            return _terminal_error(
                error,
                status=DatasetStatus.FETCH_FAILED.value,
                error_code="response_schema_error",
                query_params=query_params,
            )

        raw_reference = self.raw_store.put_json(
            {
                "api_name": definition.name,
                "params": query_params,
                "columns": [str(column) for column in frame.columns],
                "rows": rows,
            }
        )
        observation_time = (
            normalize_as_of(self.clock())
            if bootstrap_current_state
            else None
        )
        effective_boundary = observation_time or boundary
        assessment = assess_dataset_rows(
            definition,
            rows,
            as_of=effective_boundary,
            invalid_value_rows=invalid_value_rows,
            observed_at=observation_time,
        )
        observed_at = (
            observation_time
            if observation_time is not None
            else max(normalize_as_of(self.clock()), assessment.available_at)
        )
        snapshot = DatasetSnapshotInput(
            dataset=definition.name,
            scope_type="stock",
            scope_value=scope_value,
            market="A",
            provider="tushare",
            schema_version=definition.schema_version,
            trade_date=assessment.trade_date,
            report_date=assessment.report_date,
            announcement_date=assessment.announcement_date,
            data_as_of=assessment.data_as_of,
            available_at=assessment.available_at,
            observed_at=observed_at,
            status=assessment.status,
            normalized=list(assessment.rows),
            raw_ref=raw_reference.to_dict(),
            knowledge_as_of=effective_boundary,
        )
        try:
            write_result = self.repository.write_dataset(
                snapshot,
                lease=lease,
                establish_reference_for=(
                    scope_value if bootstrap_current_state else None
                ),
            )
        except ResearchReferenceTimeConflictError as error:
            recovered = self._load_job_terminal_result(
                lease,
                definition.name,
                scope_value,
                error.reference_time,
            )
            if recovered is not None:
                return recovered
            raise
        if (
            definition.incremental_by_trade_date
            and assessment.trade_date is not None
            and assessment.status
            in {
                DatasetStatus.AVAILABLE.value,
                DatasetStatus.PARTIAL.value,
                DatasetStatus.STALE.value,
            }
        ):
            self._record_high_watermark(
                definition.name,
                scope_value,
                assessment.trade_date,
            )
        result = DatasetCollectionResult(
            dataset=definition.name,
            status=assessment.status,
            row_count=len(assessment.rows),
            snapshot=write_result,
            query_params=dict(query_params),
            normalized_rows=tuple(
                MappingProxyType(dict(row)) for row in assessment.rows
            ),
            available_at=assessment.available_at,
            data_as_of=assessment.data_as_of,
            trade_date=assessment.trade_date,
            report_date=assessment.report_date,
            announcement_date=assessment.announcement_date,
            raw_ref=raw_reference.to_dict(),
            dropped_future_rows=assessment.dropped_future_rows,
            dropped_unknown_availability_rows=(
                assessment.dropped_unknown_availability_rows
            ),
            invalid_value_rows=assessment.invalid_value_rows,
            source_snapshot_hashes=(write_result.content_hash,),
        )
        if definition.incremental_by_trade_date and assessment.trade_date is not None:
            self._remember_incremental_result(
                definition,
                scope_value,
                result,
            )
        return result

    def _persist_error_snapshot(
        self,
        *,
        stock_code: str,
        dataset: str,
        definition: Optional[DatasetDefinition],
        boundary: datetime,
        lease: LeaseFence,
        status: str,
        error_code: str,
        error: BaseException,
        query_params: Optional[Mapping[str, Any]] = None,
        establish_reference_for: Optional[str] = None,
        retryable: bool = False,
        observed_at: Optional[datetime] = None,
    ) -> DatasetCollectionResult:
        snapshot_observed_at = (
            normalize_as_of(observed_at)
            if observed_at is not None
            else max(normalize_as_of(self.clock()), boundary)
        )
        snapshot_input = DatasetSnapshotInput(
            dataset=dataset,
            scope_type="stock",
            scope_value=stock_code,
            market="A",
            provider="tushare",
            schema_version=(
                definition.schema_version
                if definition is not None
                else "tushare-unsupported-dataset-v1"
            ),
            data_as_of=boundary,
            available_at=boundary,
            observed_at=snapshot_observed_at,
            status=status,
            normalized=None,
            error_code=error_code,
            error_message_sanitized=_safe_error_message(error),
            knowledge_as_of=boundary,
            retryable=retryable,
        )
        try:
            write_result = self.repository.write_dataset(
                snapshot_input,
                lease=lease,
                establish_reference_for=establish_reference_for,
            )
        except ResearchReferenceTimeConflictError as conflict:
            recovered = self._load_job_terminal_result(
                lease,
                dataset,
                stock_code,
                conflict.reference_time,
            )
            if recovered is not None:
                return recovered
            raise
        return DatasetCollectionResult(
            dataset=dataset,
            status=status,
            row_count=0,
            snapshot=write_result,
            query_params=dict(query_params or {}),
            normalized_rows=(),
            available_at=boundary,
            data_as_of=boundary,
            error_code=error_code,
            error_message_sanitized=_safe_error_message(error),
            retry_after=getattr(error, "retry_after", None),
            retryable=retryable,
        )

    @staticmethod
    def _build_query_params(
        ts_code: str,
        definition: DatasetDefinition,
        *,
        boundary: datetime,
        last_trade_date: Optional[date],
    ) -> Optional[dict[str, Any]]:
        params: dict[str, Any] = {"ts_code": ts_code}
        if not definition.accepts_date_range:
            return params
        end_date = as_of_trade_date(boundary)
        history_days = definition.history_days or 300
        start_date = end_date - timedelta(days=history_days - 1)
        if last_trade_date is not None:
            start_date = max(start_date, last_trade_date + timedelta(days=1))
        if start_date > end_date:
            return None
        params["start_date"] = format_tushare_date(start_date)
        params["end_date"] = format_tushare_date(end_date)
        return params

    def _resolve_last_trade_date(
        self,
        dataset: str,
        scope_value: str,
        boundary: datetime,
    ) -> Optional[date]:
        cutoff = as_of_trade_date(boundary)
        with _CYQ_STATE_LOCK:
            cache_key = (dataset, scope_value)
            in_process = _CYQ_HIGH_WATERMARKS.get(cache_key)
            if in_process is not None:
                _touch_cyq_key_locked(cache_key)
            if in_process is not None and in_process > cutoff:
                in_process = None
        persisted: Optional[date] = None
        if self._last_trade_date_resolver is not None:
            persisted = self._last_trade_date_resolver(dataset, scope_value, cutoff)
            if persisted is not None and persisted > cutoff:
                persisted = None
        else:
            db = getattr(self.repository, "db", None)
            if db is not None:
                with db.get_session() as session:
                    persisted = session.scalar(
                        select(func.max(ResearchDatasetSnapshotRecord.trade_date)).where(
                            ResearchDatasetSnapshotRecord.dataset == dataset,
                            ResearchDatasetSnapshotRecord.scope_type == "stock",
                            ResearchDatasetSnapshotRecord.scope_value == scope_value,
                            ResearchDatasetSnapshotRecord.trade_date <= cutoff,
                            ResearchDatasetSnapshotRecord.available_at
                            <= boundary.replace(tzinfo=None),
                            ResearchDatasetSnapshotRecord.data_as_of
                            <= boundary.replace(tzinfo=None),
                            ResearchDatasetSnapshotRecord.status.in_(
                                {
                                    DatasetStatus.AVAILABLE.value,
                                    DatasetStatus.PARTIAL.value,
                                    DatasetStatus.STALE.value,
                                }
                            ),
                        )
                    )
        candidates = [value for value in (in_process, persisted) if value is not None]
        return max(candidates) if candidates else None

    @staticmethod
    def _record_high_watermark(dataset: str, scope_value: str, value: date) -> None:
        with _CYQ_STATE_LOCK:
            cache_key = (dataset, scope_value)
            current = _CYQ_HIGH_WATERMARKS.get(cache_key)
            if current is None or value > current:
                _CYQ_HIGH_WATERMARKS[cache_key] = value
            _touch_cyq_key_locked(cache_key)

    @staticmethod
    def _remember_incremental_result(
        definition: DatasetDefinition,
        scope_value: str,
        result: DatasetCollectionResult,
    ) -> None:
        """Bound and deduplicate the in-process CYQ acceleration window."""

        cache_key = (definition.name, scope_value)
        with _CYQ_STATE_LOCK:
            candidates = [*_CYQ_RESULT_CHUNKS.get(cache_key, ()), result]
            by_hash: dict[str, DatasetCollectionResult] = {}
            without_hash: list[DatasetCollectionResult] = []
            for item in candidates:
                content_hash = (
                    item.snapshot.content_hash if item.snapshot is not None else ""
                )
                if content_hash:
                    by_hash[content_hash] = item
                else:
                    without_hash.append(item)
            bounded = [*without_hash, *by_hash.values()]
            latest_trade_date = max(
                (
                    item.trade_date
                    for item in bounded
                    if item.trade_date is not None
                ),
                default=result.trade_date,
            )
            if latest_trade_date is not None:
                history_start = latest_trade_date - timedelta(
                    days=(definition.history_days or 300) - 1
                )
                bounded = [
                    item
                    for item in bounded
                    if item.trade_date is not None
                    and history_start <= item.trade_date <= latest_trade_date
                ]
            bounded.sort(key=_cyq_result_order_key)
            _CYQ_RESULT_CHUNKS[cache_key] = bounded[
                -_CYQ_RESULT_CHUNKS_MAX_PER_KEY:
            ]
            _remember_cyq_latest_locked(cache_key, result)

    def _assert_live_lease_after_wait(self, lease: LeaseFence) -> None:
        """Fence a caller again after waiting on another caller's transport."""

        db = getattr(self.repository, "db", None)
        if db is None:
            return
        with db.get_session() as session:
            now = _utc_now().replace(tzinfo=None)
            live_job = session.execute(
                select(AnalysisJobRecord.task_id).where(
                    AnalysisJobRecord.task_id == lease.job_id,
                    AnalysisJobRecord.status == "processing",
                    AnalysisJobRecord.cancel_requested_at.is_(None),
                    AnalysisJobRecord.lease_owner == lease.worker_id,
                    AnalysisJobRecord.lease_token == lease.lease_token,
                    AnalysisJobRecord.lease_expires_at.is_not(None),
                    AnalysisJobRecord.lease_expires_at > now,
                )
            ).scalar_one_or_none()
        if live_job is None:
            raise StaleLeaseError(
                f"research dataset persistence rejected after wait for stale job "
                f"{lease.job_id!r}"
            )

    def _bind_existing_incremental_result(
        self,
        existing: DatasetCollectionResult,
        *,
        definition: DatasetDefinition,
        scope_value: str,
        boundary: datetime,
        lease: LeaseFence,
    ) -> DatasetCollectionResult:
        """Audit that this job consumed an existing immutable CYQ chunk."""

        return self._bind_existing_result(
            existing,
            definition=definition,
            scope_value=scope_value,
            boundary=boundary,
            lease=lease,
        )

    def _bind_existing_result(
        self,
        existing: DatasetCollectionResult,
        *,
        definition: DatasetDefinition,
        scope_value: str,
        boundary: datetime,
        lease: LeaseFence,
    ) -> DatasetCollectionResult:
        """Lease-fence and audit reuse of one immutable terminal dataset."""

        observed_at = max(normalize_as_of(self.clock()), existing.available_at)
        normalized: Any
        if existing.status in {
            DatasetStatus.AVAILABLE.value,
            DatasetStatus.EMPTY.value,
            DatasetStatus.PARTIAL.value,
            DatasetStatus.STALE.value,
        }:
            normalized = [dict(row) for row in existing.normalized_rows]
        else:
            normalized = None
        snapshot = DatasetSnapshotInput(
            dataset=definition.name,
            scope_type="stock",
            scope_value=scope_value,
            market="A",
            provider="tushare",
            schema_version=definition.schema_version,
            trade_date=existing.trade_date,
            report_date=existing.report_date,
            announcement_date=existing.announcement_date,
            data_as_of=existing.data_as_of,
            available_at=existing.available_at,
            observed_at=observed_at,
            status=existing.status,
            normalized=normalized,
            raw_ref=existing.raw_ref,
            error_code=existing.error_code,
            error_message_sanitized=existing.error_message_sanitized,
            knowledge_as_of=boundary,
            retryable=existing.retryable,
        )
        self.repository.write_dataset(snapshot, lease=lease)
        # A content-addressed repository returns the same hash. Preserve the
        # hydrated immutable reference so light-weight injectable fakes do not
        # need to reproduce repository hashing merely to verify the bind call.
        return replace(existing, skipped=True, reused=True)

    def _incremental_chunks(
        self,
        definition: DatasetDefinition,
        scope_value: str,
        boundary: datetime,
        current: DatasetCollectionResult,
    ) -> list[DatasetCollectionResult]:
        cutoff = as_of_trade_date(boundary)
        start_date = cutoff - timedelta(days=(definition.history_days or 300) - 1)
        with _CYQ_STATE_LOCK:
            cache_key = (definition.name, scope_value)
            cached_chunks = _CYQ_RESULT_CHUNKS.get(cache_key)
            if cached_chunks is not None:
                _touch_cyq_key_locked(cache_key)
            chunks = list(cached_chunks or ())
        known_hashes = {
            item.snapshot.content_hash
            for item in chunks
            if item.snapshot is not None
        }
        db = getattr(self.repository, "db", None)
        if db is not None:
            boundary_naive = normalize_as_of(boundary).replace(tzinfo=None)
            with db.get_session() as session:
                rows = session.execute(
                    select(ResearchDatasetSnapshotRecord)
                    .where(
                        ResearchDatasetSnapshotRecord.dataset == definition.name,
                        ResearchDatasetSnapshotRecord.scope_type == "stock",
                        ResearchDatasetSnapshotRecord.scope_value == scope_value,
                        ResearchDatasetSnapshotRecord.trade_date.is_not(None),
                        ResearchDatasetSnapshotRecord.trade_date >= start_date,
                        ResearchDatasetSnapshotRecord.trade_date <= cutoff,
                        ResearchDatasetSnapshotRecord.available_at <= boundary_naive,
                        ResearchDatasetSnapshotRecord.data_as_of <= boundary_naive,
                        ResearchDatasetSnapshotRecord.status.in_(
                            {
                                DatasetStatus.AVAILABLE.value,
                                DatasetStatus.PARTIAL.value,
                                DatasetStatus.STALE.value,
                            }
                        ),
                    )
                    .order_by(
                        ResearchDatasetSnapshotRecord.trade_date.asc(),
                        ResearchDatasetSnapshotRecord.id.asc(),
                    )
                ).scalars().all()
                for row in rows:
                    if row.content_hash not in known_hashes:
                        chunks.append(self._hydrate_snapshot_row(row, reused=True))
                        known_hashes.add(row.content_hash)
        if current.snapshot is not None and current.snapshot.content_hash not in known_hashes:
            chunks.append(current)
        return [
            item
            for item in chunks
            if item.trade_date is not None
            and start_date <= item.trade_date <= cutoff
            and item.available_at <= boundary
            and item.data_as_of <= boundary
            and item.status
            in {
                DatasetStatus.AVAILABLE.value,
                DatasetStatus.PARTIAL.value,
                DatasetStatus.STALE.value,
            }
        ]

    def _merge_incremental_window(
        self,
        definition: DatasetDefinition,
        scope_value: str,
        boundary: datetime,
        current: DatasetCollectionResult,
    ) -> DatasetCollectionResult:
        """Return the complete persisted CYQ window, not only the new chunk."""

        chunks = self._incremental_chunks(
            definition,
            scope_value,
            boundary,
            current,
        )
        if not chunks:
            return current
        unique_rows: dict[str, Mapping[str, Any]] = {}
        for chunk in chunks:
            for row in chunk.normalized_rows:
                unique_rows.setdefault(
                    canonical_json(row, exclude_volatile=False),
                    row,
                )
        ordered_rows = tuple(
            MappingProxyType(dict(row))
            for key, row in sorted(
                unique_rows.items(),
                key=lambda item: (
                    str(item[1].get("trade_date") or ""),
                    item[0],
                ),
            )
        )
        statuses = {item.status for item in chunks}
        latest_chunk = max(
            chunks,
            key=lambda item: (
                item.trade_date or date.min,
                item.available_at,
                item.snapshot.record_id if item.snapshot is not None else 0,
            ),
        )
        status = (
            DatasetStatus.PARTIAL.value
            if DatasetStatus.PARTIAL.value in statuses
            else DatasetStatus.STALE.value
            if latest_chunk.status == DatasetStatus.STALE.value
            else DatasetStatus.AVAILABLE.value
        )
        hashes = tuple(
            sorted(
                {
                    item.snapshot.content_hash
                    for item in chunks
                    if item.snapshot is not None
                }
            )
        )
        return replace(
            current,
            status=status,
            row_count=len(ordered_rows),
            normalized_rows=ordered_rows,
            available_at=max(item.available_at for item in chunks),
            data_as_of=max(item.data_as_of for item in chunks),
            trade_date=max(item.trade_date for item in chunks if item.trade_date is not None),
            source_snapshot_hashes=hashes,
        )

    @staticmethod
    def _boundary_key(boundary: datetime) -> str:
        value = normalize_as_of(boundary).replace(tzinfo=None)
        return value.isoformat(timespec="microseconds") + "Z"

    @staticmethod
    def _aware_utc(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @classmethod
    def _hydrate_snapshot_row(
        cls,
        row: ResearchDatasetSnapshotRecord,
        *,
        reused: bool,
    ) -> DatasetCollectionResult:
        normalized_payload = (
            json.loads(row.normalized_json) if row.normalized_json is not None else None
        )
        if isinstance(normalized_payload, list):
            normalized_rows = tuple(
                MappingProxyType(dict(item))
                for item in normalized_payload
                if isinstance(item, Mapping)
            )
        elif isinstance(normalized_payload, Mapping):
            normalized_rows = (MappingProxyType(dict(normalized_payload)),)
        else:
            normalized_rows = ()
        raw_payload = json.loads(row.raw_ref_json) if row.raw_ref_json else None
        raw_ref = raw_payload if isinstance(raw_payload, Mapping) else None
        return DatasetCollectionResult(
            dataset=row.dataset,
            status=row.status,
            row_count=len(normalized_rows),
            snapshot=SnapshotWriteResult(
                int(row.id),
                str(row.content_hash),
                False,
            ),
            query_params=MappingProxyType({}),
            normalized_rows=normalized_rows,
            available_at=cls._aware_utc(row.available_at),
            data_as_of=cls._aware_utc(row.data_as_of),
            trade_date=row.trade_date,
            report_date=row.report_date,
            announcement_date=row.announcement_date,
            raw_ref=raw_ref,
            error_code=row.error_code,
            error_message_sanitized=row.error_message_sanitized,
            skipped=reused,
            reused=reused,
            source_snapshot_hashes=(str(row.content_hash),),
        )

    def _remember_terminal_result(
        self,
        lease: LeaseFence,
        dataset: str,
        scope_value: str,
        boundary: datetime,
        result: DatasetCollectionResult,
    ) -> None:
        if result.retryable:
            return
        key = (
            lease.job_id,
            dataset,
            scope_value,
            self._boundary_key(boundary),
        )
        with _CYQ_STATE_LOCK:
            _remember_job_result_locked(key, result)

    def _load_job_terminal_result(
        self,
        lease: LeaseFence,
        dataset: str,
        scope_value: str,
        boundary: datetime,
    ) -> Optional[DatasetCollectionResult]:
        boundary_key = self._boundary_key(boundary)
        cache_key = (lease.job_id, dataset, scope_value, boundary_key)
        with _CYQ_STATE_LOCK:
            cached = _JOB_DATASET_RESULTS.get(cache_key)
            if cached is not None:
                _JOB_DATASET_RESULTS.move_to_end(cache_key)
        if cached is not None:
            self._assert_live_lease_after_wait(lease)
            return replace(cached, skipped=True, reused=True)

        db = getattr(self.repository, "db", None)
        if db is None:
            return None
        selected_hash: Optional[str] = None
        with db.get_session() as session:
            current = _utc_now().replace(tzinfo=None)
            live_job = session.execute(
                select(AnalysisJobRecord.task_id).where(
                    AnalysisJobRecord.task_id == lease.job_id,
                    AnalysisJobRecord.status == "processing",
                    AnalysisJobRecord.cancel_requested_at.is_(None),
                    AnalysisJobRecord.lease_owner == lease.worker_id,
                    AnalysisJobRecord.lease_token == lease.lease_token,
                    AnalysisJobRecord.lease_expires_at > current,
                )
            ).scalar_one_or_none()
            if live_job is None:
                raise StaleLeaseError(
                    f"research dataset resume rejected for stale job {lease.job_id!r}"
                )
            payload_json = session.execute(
                select(JobEventRecord.payload_json)
                .where(
                    JobEventRecord.job_id == lease.job_id,
                    JobEventRecord.event_type == "research_dataset_snapshot",
                    func.json_extract(
                        JobEventRecord.payload_json,
                        "$.dataset",
                    )
                    == dataset,
                    func.json_extract(
                        JobEventRecord.payload_json,
                        "$.scope_type",
                    )
                    == "stock",
                    func.json_extract(
                        JobEventRecord.payload_json,
                        "$.scope_value",
                    )
                    == scope_value,
                    func.json_extract(
                        JobEventRecord.payload_json,
                        "$.knowledge_as_of",
                    )
                    == boundary_key,
                )
                .where(
                    or_(
                        func.json_extract(
                            JobEventRecord.payload_json,
                            "$.status",
                        )
                        != DatasetStatus.FETCH_FAILED.value,
                        func.coalesce(
                            func.json_extract(
                                JobEventRecord.payload_json,
                                "$.retryable",
                            ),
                            0,
                        )
                        == 0,
                    )
                )
                .order_by(JobEventRecord.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if payload_json is not None:
                try:
                    payload = json.loads(payload_json)
                except (TypeError, ValueError, json.JSONDecodeError):
                    payload = None
                if isinstance(payload, Mapping):
                    selected_hash = str(payload.get("content_hash") or "")
            if not selected_hash:
                return None
            row = session.execute(
                select(ResearchDatasetSnapshotRecord).where(
                    ResearchDatasetSnapshotRecord.content_hash == selected_hash,
                    ResearchDatasetSnapshotRecord.available_at
                    <= normalize_as_of(boundary).replace(tzinfo=None),
                    ResearchDatasetSnapshotRecord.data_as_of
                    <= normalize_as_of(boundary).replace(tzinfo=None),
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            result = self._hydrate_snapshot_row(row, reused=True)
        with _CYQ_STATE_LOCK:
            _remember_job_result_locked(cache_key, result)
        return result

    def _load_latest_terminal_result(
        self,
        dataset: str,
        scope_value: str,
        boundary: datetime,
    ) -> Optional[DatasetCollectionResult]:
        boundary_value = normalize_as_of(boundary)
        with _CYQ_STATE_LOCK:
            cache_key = (dataset, scope_value)
            cached = _CYQ_LATEST_RESULTS.get(cache_key)
            if cached is not None:
                _touch_cyq_key_locked(cache_key)
        if (
            cached is not None
            and cached.trade_date is not None
            and cached.trade_date <= as_of_trade_date(boundary_value)
            and cached.available_at <= boundary_value
            and cached.data_as_of <= boundary_value
        ):
            return cached

        db = getattr(self.repository, "db", None)
        if db is None:
            return None
        with db.get_session() as session:
            row = session.execute(
                select(ResearchDatasetSnapshotRecord)
                .where(
                    ResearchDatasetSnapshotRecord.dataset == dataset,
                    ResearchDatasetSnapshotRecord.scope_type == "stock",
                    ResearchDatasetSnapshotRecord.scope_value == scope_value,
                    ResearchDatasetSnapshotRecord.trade_date.is_not(None),
                    ResearchDatasetSnapshotRecord.trade_date
                    <= as_of_trade_date(boundary_value),
                    ResearchDatasetSnapshotRecord.available_at
                    <= boundary_value.replace(tzinfo=None),
                    ResearchDatasetSnapshotRecord.data_as_of
                    <= boundary_value.replace(tzinfo=None),
                    ResearchDatasetSnapshotRecord.status.in_(
                        {
                            DatasetStatus.AVAILABLE.value,
                            DatasetStatus.PARTIAL.value,
                            DatasetStatus.STALE.value,
                        }
                    ),
                )
                .order_by(
                    ResearchDatasetSnapshotRecord.trade_date.desc(),
                    ResearchDatasetSnapshotRecord.available_at.desc(),
                    ResearchDatasetSnapshotRecord.id.desc(),
                )
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            result = self._hydrate_snapshot_row(row, reused=True)
        with _CYQ_STATE_LOCK:
            _remember_cyq_latest_locked((dataset, scope_value), result)
        return result

    def _load_latest_current_state_result(
        self,
        dataset: str,
        scope_value: str,
        boundary: datetime,
    ) -> Optional[DatasetCollectionResult]:
        """Read an observation that was genuinely known by a historical cutoff."""

        db = getattr(self.repository, "db", None)
        if db is None:
            return None
        boundary_value = normalize_as_of(boundary).replace(tzinfo=None)
        with db.get_session() as session:
            row = session.execute(
                select(ResearchDatasetSnapshotRecord)
                .where(
                    ResearchDatasetSnapshotRecord.dataset == dataset,
                    ResearchDatasetSnapshotRecord.scope_type == "stock",
                    ResearchDatasetSnapshotRecord.scope_value == scope_value,
                    ResearchDatasetSnapshotRecord.available_at <= boundary_value,
                    ResearchDatasetSnapshotRecord.data_as_of <= boundary_value,
                    ResearchDatasetSnapshotRecord.observed_at <= boundary_value,
                    ResearchDatasetSnapshotRecord.status
                    != DatasetStatus.FETCH_FAILED.value,
                )
                .order_by(
                    ResearchDatasetSnapshotRecord.available_at.desc(),
                    ResearchDatasetSnapshotRecord.id.desc(),
                )
                .limit(1)
            ).scalar_one_or_none()
            return (
                self._hydrate_snapshot_row(row, reused=True)
                if row is not None
                else None
            )

    @staticmethod
    def _query_cyq_single_flight(
        key: tuple[str, str, str, str],
        operation: Callable[[], pd.DataFrame],
        *,
        cancel_event: Optional[CancellationSignal],
    ) -> pd.DataFrame:
        with _CYQ_STATE_LOCK:
            flight = _CYQ_FLIGHTS.get(key)
            owner = flight is None
            if flight is None:
                flight = _CollectionFlight(event=threading.Event())
                _CYQ_FLIGHTS[key] = flight
        if owner:
            try:
                flight.frame = operation().copy(deep=True)
            except BaseException as error:
                flight.error = error
            finally:
                flight.event.set()
                with _CYQ_STATE_LOCK:
                    if _CYQ_FLIGHTS.get(key) is flight:
                        _CYQ_FLIGHTS.pop(key, None)
        else:
            while not flight.event.wait(timeout=0.1):
                if cancel_event is not None and cancel_event.is_set():
                    raise TushareRequestCancelled(
                        "Tushare collection cancelled while waiting for single-flight"
                    )
        if cancel_event is not None and cancel_event.is_set():
            raise TushareRequestCancelled(
                "Tushare collection cancelled after single-flight"
            )
        if flight.error is not None:
            raise flight.error
        if flight.frame is None:
            raise RuntimeError("single-flight completed without a frame")
        return flight.frame.copy(deep=True)


__all__ = [
    "DatasetCollectionResult",
    "ResearchCollectionResult",
    "ResearchDatasetCollector",
]
