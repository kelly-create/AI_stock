"""Durable, point-in-time Tushare dataset collection orchestration."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
import json
import re
import threading
from types import MappingProxyType
from typing import Any, Callable, Literal, Mapping, Optional, Protocol, Sequence

import pandas as pd

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
from .decision_outcome_v2_datasets import (
    DecisionOutcomeV2DatasetQuery,
    validate_decision_outcome_v2_frames,
)
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
        query_plan: Optional[DecisionOutcomeV2DatasetQuery] = None,
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
        if query_plan is not None and query_plan.dataset != definition.name:
            raise ValueError("query_plan dataset conflicts with requested dataset")
        if definition.requires_query_plan and query_plan is None:
            raise ValueError(
                f"research dataset {definition.name!r} requires an exact query_plan"
            )
        if not definition.requires_query_plan and query_plan is not None:
            raise ValueError(
                f"research dataset {definition.name!r} does not accept a query_plan"
            )
        scope_value = ts_code.split(".", 1)[0]
        recovered = self._load_job_terminal_result(
            lease,
            definition.name,
            scope_value,
            boundary,
        )
        if (
            recovered is None
            and definition.current_state
            and reference_mode == "live"
        ):
            recovered = self._load_job_terminal_result_at_any_boundary(
                lease,
                definition.name,
                scope_value,
            )
        if recovered is not None:
            if query_plan is not None:
                self._assert_recovered_query_plan(recovered, query_plan)
            if definition.incremental_by_trade_date:
                return self._merge_incremental_window(
                    definition,
                    scope_value,
                    boundary,
                    recovered,
                    lease=lease,
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
            if query_plan is not None:
                self._assert_recovered_query_plan(existing, query_plan)
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
            query_plan=query_plan,
            bootstrap_current_state=(
                definition.name == "stock_basic" and reference_mode == "live"
            ),
            observe_current_state=(
                definition.current_state and reference_mode == "live"
            ),
        )
        if definition.incremental_by_trade_date:
            result = self._merge_incremental_window(
                definition,
                scope_value,
                boundary,
                result,
                lease=lease,
            )
        # A merged incremental window intentionally binds more than one
        # immutable chunk to the same job boundary.  The durable events remain
        # authoritative; caching one of those chunks as though it were the
        # sole terminal binding would make a later resume order-dependent.
        if not (
            definition.incremental_by_trade_date
            and len(result.source_snapshot_hashes) > 1
        ):
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

    def _assert_recovered_query_plan(
        self,
        result: DatasetCollectionResult,
        query_plan: DecisionOutcomeV2DatasetQuery,
    ) -> None:
        """Reject reuse when an immutable payload came from another window."""

        if result.raw_ref is None:
            # Permission and transport terminals intentionally carry no raw
            # payload. Dataset + stock scope + frozen boundary remain their
            # complete identity and their typed failure must stay reusable.
            return
        try:
            raw = json.loads(self.raw_store.read(result.raw_ref).decode("utf-8"))
        except (KeyError, TypeError, ValueError, OSError) as exc:
            raise ValueError("verified benchmark raw query plan is invalid") from exc
        if not isinstance(raw, Mapping):
            raise ValueError("verified benchmark raw query plan is invalid")
        expected = dict(query_plan.to_audit_params())
        if raw.get("params") != expected:
            raise ValueError(
                "verified benchmark snapshot conflicts with exact query plan"
            )

    def _collect_supported_dataset(
        self,
        ts_code: str,
        definition: DatasetDefinition,
        *,
        boundary: datetime,
        lease: LeaseFence,
        cancel_event: Optional[CancellationSignal],
        query_plan: Optional[DecisionOutcomeV2DatasetQuery] = None,
        bootstrap_current_state: bool = False,
        observe_current_state: bool = False,
    ) -> DatasetCollectionResult:
        scope_value = ts_code.split(".", 1)[0]

        def _terminal_boundary() -> datetime:
            if observe_current_state:
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
                    terminal_boundary if observe_current_state else None
                ),
            )

        last_trade_date = (
            self._resolve_last_trade_date(definition.name, scope_value, boundary)
            if definition.incremental_by_trade_date
            else None
        )
        query_params = (
            dict(query_plan.to_audit_params())
            if query_plan is not None
            else self._build_query_params(
                ts_code,
                definition,
                boundary=boundary,
                last_trade_date=last_trade_date,
            )
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
        frames: list[pd.DataFrame] = []
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
                frames.append(frame)
                # A waiter may have been cancelled or lost its lease while the
                # shared transport was in flight. It must never inherit the
                # owner's authority to persist.
                if cancel_event is not None and cancel_event.is_set():
                    raise TushareRequestCancelled(
                        "Tushare collection cancelled after single-flight"
                    )
                self._assert_live_lease_after_wait(lease)
            elif query_plan is not None:
                for request in query_plan.requests:
                    frames.append(
                        self.provider.query(
                            request.api_name,
                            fields=request.fields_arg,
                            _cancel_event=cancel_event,
                            **dict(request.provider_params),
                        )
                    )
            else:
                frames.append(
                    self.provider.query(
                        definition.provider_api_name or definition.name,
                        _cancel_event=cancel_event,
                        **query_params,
                    )
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
            frame = (
                validate_decision_outcome_v2_frames(query_plan, frames)
                if query_plan is not None
                else frames[0]
            )
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
                "api_name": (
                    query_plan.requests[0].api_name
                    if query_plan is not None and len(query_plan.requests) == 1
                    else definition.provider_api_name or definition.name
                ),
                "params": query_params,
                "fields": (
                    list(query_plan.requests[0].fields)
                    if query_plan is not None and len(query_plan.requests) == 1
                    else None
                ),
                "requests": (
                    [request.to_dict() for request in query_plan.requests]
                    if query_plan is not None
                    else None
                ),
                "columns": [str(column) for column in frame.columns],
                "rows": rows,
            }
        )
        observation_time = (
            normalize_as_of(self.clock())
            if observe_current_state
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
            cached_results = [*_CYQ_RESULT_CHUNKS.get(cache_key, ())]
            latest = _CYQ_LATEST_RESULTS.get(cache_key)
            if latest is not None:
                cached_results.append(latest)
        cached_hashes = {
            item.snapshot.content_hash
            for item in cached_results
            if item.snapshot is not None
            and item.trade_date is not None
            and item.trade_date <= cutoff
            and item.available_at <= boundary
            and item.data_as_of <= boundary
            and item.status
            in {
                DatasetStatus.AVAILABLE.value,
                DatasetStatus.PARTIAL.value,
                DatasetStatus.STALE.value,
            }
        }
        persisted: Optional[date] = None
        if self._last_trade_date_resolver is not None:
            persisted = self._last_trade_date_resolver(dataset, scope_value, cutoff)
            if persisted is not None and persisted > cutoff:
                persisted = None
        else:
            checkpoints = self._load_dataset_checkpoints(
                dataset=dataset,
                scope_value=scope_value,
                boundary=boundary,
                trade_date_to=cutoff,
                statuses=(
                    DatasetStatus.AVAILABLE.value,
                    DatasetStatus.PARTIAL.value,
                    DatasetStatus.STALE.value,
                ),
            )
            persisted_hashes = {
                item.snapshot.content_hash
                for item in checkpoints
                if item.snapshot is not None
            }
            missing_cached_hashes = cached_hashes.difference(persisted_hashes)
            if missing_cached_hashes:
                raise ValueError(
                    "cached incremental Dataset checkpoint is missing from "
                    "durable storage"
                )
            persisted = max(
                (
                    item.trade_date
                    for item in checkpoints
                    if item.trade_date is not None
                ),
                default=None,
            )
            if (
                in_process is not None
                and (persisted is None or persisted < in_process)
            ):
                raise ValueError(
                    "cached Dataset high-watermark is not backed by durable storage"
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

        self.repository.assert_live_lease(lease)

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
            cached = list(cached_chunks or ())
        cached_hashes = {
            item.snapshot.content_hash
            for item in cached
            if item.snapshot is not None
            and item.trade_date is not None
            and start_date <= item.trade_date <= cutoff
            and item.available_at <= boundary
            and item.data_as_of <= boundary
            and item.status
            in {
                DatasetStatus.AVAILABLE.value,
                DatasetStatus.PARTIAL.value,
                DatasetStatus.STALE.value,
            }
        }
        chunks = self._load_dataset_checkpoints(
            dataset=definition.name,
            scope_value=scope_value,
            boundary=boundary,
            trade_date_from=start_date,
            trade_date_to=cutoff,
            statuses=(
                DatasetStatus.AVAILABLE.value,
                DatasetStatus.PARTIAL.value,
                DatasetStatus.STALE.value,
            ),
        )
        durable_hashes = {
            item.snapshot.content_hash
            for item in chunks
            if item.snapshot is not None
        }
        if cached_hashes.difference(durable_hashes):
            raise ValueError(
                "cached incremental Dataset checkpoint is missing from "
                "durable storage"
            )
        if (
            current.snapshot is not None
            and current.trade_date is not None
            and start_date <= current.trade_date <= cutoff
            and current.status
            in {
                DatasetStatus.AVAILABLE.value,
                DatasetStatus.PARTIAL.value,
                DatasetStatus.STALE.value,
            }
            and current.snapshot.content_hash not in durable_hashes
        ):
            raise ValueError(
                "current incremental Dataset result is missing from durable storage"
            )
        return chunks

    def _merge_incremental_window(
        self,
        definition: DatasetDefinition,
        scope_value: str,
        boundary: datetime,
        current: DatasetCollectionResult,
        *,
        lease: LeaseFence,
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
        current_hash = (
            current.snapshot.content_hash if current.snapshot is not None else None
        )
        for chunk in chunks:
            chunk_hash = (
                chunk.snapshot.content_hash if chunk.snapshot is not None else None
            )
            if chunk_hash is None or chunk_hash == current_hash:
                continue
            self._bind_existing_incremental_result(
                chunk,
                definition=definition,
                scope_value=scope_value,
                boundary=boundary,
                lease=lease,
            )
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
        source_hashes = {
            item.snapshot.content_hash
            for item in chunks
            if item.snapshot is not None
        }
        # The primary incremental request is itself frozen provenance even
        # when it returns no new rows.  Keeping that terminal observation in
        # the merged lineage lets a cold durable resume distinguish "already
        # up to date" from a window assembled only from older chunks.
        if current.snapshot is not None:
            source_hashes.add(current.snapshot.content_hash)
        hashes = tuple(sorted(source_hashes))
        return replace(
            latest_chunk,
            status=status,
            row_count=len(ordered_rows),
            normalized_rows=ordered_rows,
            available_at=max(
                [current.available_at, *(item.available_at for item in chunks)]
            ),
            data_as_of=max(
                [current.data_as_of, *(item.data_as_of for item in chunks)]
            ),
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
    def _checkpoint_datetime(cls, value: Any, field_name: str) -> datetime:
        if isinstance(value, datetime):
            return cls._aware_utc(value)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"verified Dataset {field_name} is invalid")
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                f"verified Dataset {field_name} is invalid"
            ) from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"verified Dataset {field_name} must include an offset")
        return cls._aware_utc(parsed)

    @staticmethod
    def _checkpoint_date(value: Any, field_name: str) -> Optional[date]:
        if value is None:
            return None
        if isinstance(value, datetime):
            raise ValueError(f"verified Dataset {field_name} is invalid")
        if isinstance(value, date):
            return value
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"verified Dataset {field_name} is invalid")
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise ValueError(
                f"verified Dataset {field_name} is invalid"
            ) from exc

    @classmethod
    def _hydrate_snapshot_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        reused: bool,
    ) -> DatasetCollectionResult:
        dataset = payload.get("dataset")
        status = payload.get("status")
        record_id = payload.get("id")
        content_hash = payload.get("content_hash")
        if not isinstance(dataset, str) or not dataset.strip():
            raise ValueError("verified Dataset name is invalid")
        if status not in {item.value for item in DatasetStatus}:
            raise ValueError("verified Dataset status is invalid")
        if isinstance(record_id, bool) or not isinstance(record_id, int) or record_id <= 0:
            raise ValueError("verified Dataset id is invalid")
        if (
            not isinstance(content_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", content_hash) is None
        ):
            raise ValueError("verified Dataset content_hash is invalid")

        normalized_payload = payload.get("normalized")
        if isinstance(normalized_payload, list):
            if any(not isinstance(item, Mapping) for item in normalized_payload):
                raise ValueError("verified Dataset normalized rows are invalid")
            normalized_rows = tuple(
                MappingProxyType(dict(item)) for item in normalized_payload
            )
        elif isinstance(normalized_payload, Mapping):
            normalized_rows = (MappingProxyType(dict(normalized_payload)),)
        elif normalized_payload is None and status not in {
            DatasetStatus.AVAILABLE.value,
            DatasetStatus.EMPTY.value,
            DatasetStatus.PARTIAL.value,
            DatasetStatus.STALE.value,
        }:
            normalized_rows = ()
        else:
            raise ValueError("verified Dataset normalized payload is invalid")

        raw_payload = payload.get("raw_ref")
        if raw_payload is not None and not isinstance(raw_payload, Mapping):
            raise ValueError("verified Dataset raw_ref is invalid")
        raw_ref = (
            MappingProxyType(dict(raw_payload))
            if isinstance(raw_payload, Mapping)
            else None
        )
        retryable = payload.get(
            "binding_retryable",
            payload.get("retryable", False),
        )
        if not isinstance(retryable, bool):
            raise ValueError("verified Dataset retryable flag is invalid")
        error_code = payload.get("error_code")
        error_message = payload.get("error_message")
        if error_code is not None and not isinstance(error_code, str):
            raise ValueError("verified Dataset error_code is invalid")
        if error_message is not None and not isinstance(error_message, str):
            raise ValueError("verified Dataset error_message is invalid")
        return DatasetCollectionResult(
            dataset=dataset,
            status=status,
            row_count=len(normalized_rows),
            snapshot=SnapshotWriteResult(
                record_id,
                content_hash,
                False,
            ),
            query_params=MappingProxyType({}),
            normalized_rows=normalized_rows,
            available_at=cls._checkpoint_datetime(
                payload.get("available_at"), "available_at"
            ),
            data_as_of=cls._checkpoint_datetime(
                payload.get("data_as_of"), "data_as_of"
            ),
            trade_date=cls._checkpoint_date(
                payload.get("trade_date"), "trade_date"
            ),
            report_date=cls._checkpoint_date(
                payload.get("report_date"), "report_date"
            ),
            announcement_date=cls._checkpoint_date(
                payload.get("announcement_date"), "announcement_date"
            ),
            raw_ref=raw_ref,
            error_code=error_code,
            error_message_sanitized=error_message,
            retryable=retryable,
            skipped=reused,
            reused=reused,
            source_snapshot_hashes=(content_hash,),
        )

    def _load_dataset_checkpoints(
        self,
        *,
        dataset: str,
        scope_value: str,
        boundary: datetime,
        trade_date_from: Optional[date] = None,
        trade_date_to: Optional[date] = None,
        statuses: Optional[Sequence[str]] = None,
    ) -> list[DatasetCollectionResult]:
        """Hydrate only repository-verified rows; never trust ORM/cache state."""

        rows = self.repository.list_dataset_checkpoints(
            scope_value=scope_value,
            dataset=dataset,
            as_of=boundary,
            trade_date_from=trade_date_from,
            trade_date_to=trade_date_to,
            statuses=statuses,
        )
        results: list[DatasetCollectionResult] = []
        allowed_statuses = set(statuses) if statuses is not None else None
        boundary_value = normalize_as_of(boundary)
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError("verified Dataset checkpoint must be an object")
            if (
                row.get("dataset") != dataset
                or row.get("scope_type") != "stock"
                or row.get("scope_value") != scope_value
            ):
                raise ValueError(
                    "verified Dataset checkpoint conflicts with requested scope"
                )
            result = self._hydrate_snapshot_payload(row, reused=True)
            if result.available_at > boundary_value or result.data_as_of > boundary_value:
                raise ValueError("verified Dataset checkpoint is after cutoff")
            if dataset == "stock_basic":
                observed_at = self._checkpoint_datetime(
                    row.get("observed_at"), "observed_at"
                )
                if observed_at > boundary_value:
                    raise ValueError(
                        "verified stock_basic checkpoint was observed after cutoff"
                    )
            if allowed_statuses is not None and result.status not in allowed_statuses:
                raise ValueError("verified Dataset checkpoint has unexpected status")
            if trade_date_from is not None and (
                result.trade_date is None or result.trade_date < trade_date_from
            ):
                raise ValueError("verified Dataset checkpoint is before requested window")
            if trade_date_to is not None and (
                result.trade_date is None or result.trade_date > trade_date_to
            ):
                raise ValueError("verified Dataset checkpoint is after requested window")
            results.append(result)
        results.sort(
            key=lambda item: (
                item.trade_date or date.min,
                item.available_at,
                item.snapshot.record_id if item.snapshot is not None else 0,
            )
        )
        return results

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
        self._assert_live_lease_after_wait(lease)
        row = self.repository.get_job_dataset(
            job_id=lease.job_id,
            dataset=dataset,
            scope_value=scope_value,
            as_of=boundary,
            terminal_only=True,
        )
        if row is None:
            if cached is not None:
                raise ValueError(
                    "cached terminal Dataset checkpoint lost its durable binding"
                )
            return None
        if not isinstance(row, Mapping):
            raise ValueError("verified job Dataset checkpoint must be an object")
        if (
            row.get("dataset") != dataset
            or row.get("scope_type") != "stock"
            or row.get("scope_value") != scope_value
        ):
            raise ValueError("verified job Dataset checkpoint conflicts with scope")
        result = self._hydrate_snapshot_payload(row, reused=True)
        if result.retryable:
            raise ValueError("terminal Dataset checkpoint cannot be retryable")
        if result.available_at > boundary or result.data_as_of > boundary:
            raise ValueError("verified job Dataset checkpoint is after cutoff")
        if (
            cached is not None
            and cached.snapshot is not None
            and result.snapshot is not None
            and cached.snapshot.content_hash != result.snapshot.content_hash
        ):
            raise ValueError(
                "cached terminal Dataset checkpoint conflicts with durable binding"
            )
        with _CYQ_STATE_LOCK:
            _remember_job_result_locked(cache_key, result)
        return result

    def _load_job_terminal_result_at_any_boundary(
        self,
        lease: LeaseFence,
        dataset: str,
        scope_value: str,
    ) -> Optional[DatasetCollectionResult]:
        """Recover one live current-state result whose observation froze later."""

        self._assert_live_lease_after_wait(lease)
        row = self.repository.get_job_dataset(
            job_id=lease.job_id,
            dataset=dataset,
            scope_value=scope_value,
            as_of=None,
            terminal_only=True,
        )
        if row is None:
            return None
        if not isinstance(row, Mapping):
            raise ValueError("verified job Dataset checkpoint must be an object")
        if (
            row.get("dataset") != dataset
            or row.get("scope_type") != "stock"
            or row.get("scope_value") != scope_value
        ):
            raise ValueError("verified job Dataset checkpoint conflicts with scope")
        result = self._hydrate_snapshot_payload(row, reused=True)
        if result.retryable:
            raise ValueError("terminal Dataset checkpoint cannot be retryable")
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
        cached_is_visible = (
            cached is not None
            and cached.trade_date is not None
            and cached.trade_date <= as_of_trade_date(boundary_value)
            and cached.available_at <= boundary_value
            and cached.data_as_of <= boundary_value
        )
        checkpoints = self._load_dataset_checkpoints(
            dataset=dataset,
            scope_value=scope_value,
            boundary=boundary_value,
            trade_date_to=as_of_trade_date(boundary_value),
            statuses=(
                DatasetStatus.AVAILABLE.value,
                DatasetStatus.PARTIAL.value,
                DatasetStatus.STALE.value,
            ),
        )
        durable_hashes = {
            item.snapshot.content_hash
            for item in checkpoints
            if item.snapshot is not None
        }
        if (
            cached_is_visible
            and cached is not None
            and cached.snapshot is not None
            and cached.snapshot.content_hash not in durable_hashes
        ):
            raise ValueError(
                "cached latest Dataset checkpoint is missing from durable storage"
            )
        if not checkpoints:
            return None
        result = max(
            checkpoints,
            key=lambda item: (
                item.trade_date or date.min,
                item.available_at,
                item.snapshot.record_id if item.snapshot is not None else 0,
            ),
        )
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

        checkpoints = self._load_dataset_checkpoints(
            dataset=dataset,
            scope_value=scope_value,
            boundary=normalize_as_of(boundary),
            statuses=tuple(
                status.value
                for status in DatasetStatus
                if status is not DatasetStatus.FETCH_FAILED
            ),
        )
        if not checkpoints:
            return None
        return max(
            checkpoints,
            key=lambda item: (
                item.available_at,
                item.snapshot.record_id if item.snapshot is not None else 0,
            ),
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
