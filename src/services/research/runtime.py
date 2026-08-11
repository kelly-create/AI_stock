"""Feature-gated durable runtime for research collection, factors, and freeze.

The service is intentionally separate from the analysis pipeline.  Callers may
prepare research data before building an ``AnalysisContextPack`` and freeze the
final snapshot afterwards.  All expensive or stateful defaults are lazy so a
disabled rollout has no provider, filesystem, or database side effects.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import logging
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Callable, Literal, Mapping, Optional, Sequence

from data_provider.tushare_provider import (
    DEFAULT_TUSHARE_HTTP_URL,
    TushareProvider,
    TushareRequestCoordinator,
    resolve_tushare_api_url_from_env,
)

from .availability import normalize_as_of
from .canonical import canonical_json, canonicalize
from .collector import ResearchDatasetCollector
from .datasets import DatasetStatus
from .factor_input import build_factor_input
from .factor_service import evaluate_research_factors
from .provider_health import TushareProviderHealthReporter
from .raw_store import RawArtifactStore
from .repositories import (
    DebateFailureInput,
    FactorSnapshotInput,
    LeaseFence,
    ResearchSnapshotRepository,
    SnapshotWriteResult,
)
from .schemas import ComponentStatus, MetricStatus, ResearchFactorResult
from .snapshot_service import (
    DEBATE_FIELD_DICTIONARY_VERSION,
    DEBATE_SNAPSHOT_VERSION,
    EVIDENCE_FIELD_DICTIONARY_VERSION,
    EVIDENCE_SNAPSHOT_VERSION,
    FACTOR_ENGINE_VERSION,
    FrozenResearchSnapshot,
    build_research_snapshot,
    model_route_fingerprint,
)


class ResearchRuntimeContractError(RuntimeError):
    """Raised when a collector result does not expose its frozen contract."""


logger = logging.getLogger(__name__)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class _DurableStopSignal:
    signals: tuple[Any, ...]

    def is_set(self) -> bool:
        return any(bool(signal.is_set()) for signal in self.signals)


@dataclass(frozen=True)
class PreparedResearch:
    stock_code: str
    market: str
    as_of: datetime
    available_at: datetime
    lease: LeaseFence
    collection: Any
    rows_by_dataset: Mapping[str, tuple[Mapping[str, Any], ...]]
    datasets_payload: Mapping[str, Any]
    research_context: Mapping[str, Any]
    factor_input: Optional[Mapping[str, Any]]
    factors: Optional[ResearchFactorResult]
    factor_snapshot: Optional[SnapshotWriteResult]
    factors_enabled: bool
    evidence_snapshot: Optional[Any]
    evidence_context: Optional[Mapping[str, Any]]
    evidence_prompt_context: Optional[str]
    evidence_enabled: bool
    debate_snapshot: Optional[Any] = None
    debate_context: Optional[Mapping[str, Any]] = None
    debate_prompt_context: Optional[str] = None
    debate_enabled: bool = False
    task_decision: Optional[Any] = None
    budget_reservations: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class FrozenResearchWrite:
    snapshot: FrozenResearchSnapshot
    write_result: SnapshotWriteResult

    @property
    def snapshot_hash(self) -> str:
        return self.snapshot.snapshot_hash


def _config_value(config: Any, name: str, default: Any = None) -> Any:
    if isinstance(config, Mapping):
        return config.get(name, default)
    return getattr(config, name, default)


def _aware_utc(value: Any, *, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _current_durable_context() -> Any:
    from src.services.durable_job_handlers import get_optional_durable_execution_context

    return get_optional_durable_execution_context()


def _update_diagnostic_hash(snapshot_hash: str) -> None:
    from src.services.run_diagnostics import update_current_diagnostic_snapshot_hash

    update_current_diagnostic_snapshot_hash(snapshot_hash)


def _lease_from_context(context: Any) -> LeaseFence:
    if context is None:
        raise RuntimeError("Tushare research requires an active durable job execution context")
    try:
        lease = LeaseFence(
            job_id=str(context.job_id).strip(),
            worker_id=str(context.worker_id).strip(),
            lease_token=str(context.lease_token).strip(),
        )
    except AttributeError as exc:
        raise ResearchRuntimeContractError("durable context does not expose job_id/worker_id/lease_token") from exc
    if not lease.job_id or not lease.worker_id or not lease.lease_token:
        raise ResearchRuntimeContractError("durable context contains an empty lease fence")
    _raise_if_stopped(context)
    return lease


def _raise_if_stopped(context: Any) -> None:
    stop_check = getattr(context, "_raise_if_stopped", None)
    if not callable(stop_check):
        raise ResearchRuntimeContractError(
            "durable context must expose _raise_if_stopped"
        )
    stop_check()


def _durable_stop_signal(context: Any) -> _DurableStopSignal:
    signals = tuple(
        signal
        for signal in (
            getattr(context, "cancel_requested", None),
            getattr(context, "lease_lost", None),
        )
        if signal is not None and callable(getattr(signal, "is_set", None))
    )
    if len(signals) != 2:
        raise ResearchRuntimeContractError(
            "durable context must expose cancel_requested and lease_lost signals"
        )
    return _DurableStopSignal(signals)


def _adapt_rows_by_dataset(collection: Any) -> Mapping[str, tuple[Mapping[str, Any], ...]]:
    """Adapt the collector's documented frozen rows without querying storage."""

    source = getattr(collection, "rows_by_dataset", None)
    if source is None:
        # Temporary compatibility with collectors built immediately before the
        # stable alias was added.  This is still in-memory frozen data, not a DB
        # reconstruction.
        source = getattr(collection, "normalized_by_dataset", None)
    if not isinstance(source, Mapping):
        raise ResearchRuntimeContractError("collector result must expose rows_by_dataset")
    normalized: dict[str, tuple[Mapping[str, Any], ...]] = {}
    for raw_dataset, raw_rows in source.items():
        dataset = str(raw_dataset).strip()
        if not dataset:
            raise ResearchRuntimeContractError("collector returned an empty dataset name")
        if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes, bytearray)):
            raise ResearchRuntimeContractError(f"collector rows for {dataset!r} must be a sequence")
        rows = []
        for row in raw_rows:
            if not isinstance(row, Mapping):
                raise ResearchRuntimeContractError(f"collector row for {dataset!r} must be a mapping")
            rows.append(MappingProxyType(dict(row)))
        normalized[dataset] = tuple(rows)
    return MappingProxyType(normalized)


def _canonical_dataset_projection_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Match the repository's stable, duplicate-free Dataset row ordering."""

    unique_rows: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        unique_rows.setdefault(
            canonical_json(row, exclude_volatile=False),
            row,
        )
    return [
        dict(row)
        for _, row in sorted(
            unique_rows.items(),
            key=lambda item: (
                str(item[1].get("trade_date") or ""),
                item[0],
            ),
        )
    ]


def _adapt_max_available_at(collection: Any, *, as_of: datetime) -> datetime:
    value = getattr(collection, "max_available_at", None)
    if value is None:
        datasets = getattr(collection, "datasets", None)
        if not isinstance(datasets, Sequence):
            raise ResearchRuntimeContractError("collector result must expose max_available_at")
        candidates = [getattr(item, "available_at", None) for item in datasets]
        candidates = [candidate for candidate in candidates if candidate is not None]
        if not candidates:
            raise ResearchRuntimeContractError("collector result has no available_at boundary")
        value = max(candidates)
    available_at = _aware_utc(value, field="collection.max_available_at")
    if available_at > as_of:
        raise ResearchRuntimeContractError("collector max_available_at cannot be after as_of")
    return available_at


def _required_dataset_hash(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise ResearchRuntimeContractError(
            f"collector {field} must be a lowercase SHA-256 digest"
        )
    text = value.strip()
    if not _SHA256_RE.fullmatch(text):
        raise ResearchRuntimeContractError(
            f"collector {field} must be a lowercase SHA-256 digest"
        )
    return text


def _dataset_write_hash(item: Any) -> Optional[str]:
    snapshot = getattr(item, "snapshot", None)
    if snapshot is None:
        return None
    dataset = str(getattr(item, "dataset", "") or "<unknown>").strip()
    return _required_dataset_hash(
        getattr(snapshot, "content_hash", None),
        field=f"dataset {dataset!r} snapshot.content_hash",
    )


def _dataset_content_hashes(item: Any) -> tuple[str, ...]:
    """Return every immutable chunk hash consumed by a dataset result."""

    dataset = str(getattr(item, "dataset", "") or "<unknown>").strip()
    source_hashes = getattr(item, "source_snapshot_hashes", ())
    if not isinstance(source_hashes, Sequence) or isinstance(
        source_hashes, (str, bytes, bytearray)
    ):
        raise ResearchRuntimeContractError(
            f"collector dataset {dataset!r} source_snapshot_hashes must be a sequence"
        )
    hashes = {
        _required_dataset_hash(
            value,
            field=f"dataset {dataset!r} source_snapshot_hashes[{index}]",
        )
        for index, value in enumerate(source_hashes)
    }
    current_hash = _dataset_write_hash(item)
    if current_hash is not None:
        hashes.add(current_hash)
    return tuple(sorted(hashes))


def _normalized_status(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().casefold()


def _adapt_datasets_payload(
    collection: Any,
    rows_by_dataset: Mapping[str, tuple[Mapping[str, Any], ...]],
    *,
    as_of: datetime,
) -> Mapping[str, Any]:
    """Project collection metadata when present, otherwise only frozen rows."""

    items = getattr(collection, "datasets", None)
    by_name = {}
    if isinstance(items, Sequence):
        by_name = {str(getattr(item, "dataset", "")).strip(): item for item in items}
    payload: dict[str, Any] = {}
    for dataset in sorted(rows_by_dataset):
        rows = rows_by_dataset[dataset]
        projection_rows = _canonical_dataset_projection_rows(rows)
        item = by_name.get(dataset)
        if item is None:
            # Do not infer provider status or reconstruct a row from the DB.
            payload[dataset] = {"rows": projection_rows}
            continue
        available_at = _aware_utc(getattr(item, "available_at"), field=f"{dataset}.available_at")
        data_as_of = _aware_utc(getattr(item, "data_as_of"), field=f"{dataset}.data_as_of")
        if available_at > as_of or data_as_of > as_of:
            raise ResearchRuntimeContractError(f"collector dataset {dataset!r} crosses the as_of boundary")
        content_hash = _dataset_write_hash(item)
        content_hashes = _dataset_content_hashes(item)
        payload[dataset] = {
            "dataset": dataset,
            "status": _normalized_status(getattr(item, "status", "")),
            "row_count": len(projection_rows),
            "available_at": available_at,
            "data_as_of": data_as_of,
            # Keep the current write hash for existing consumers while the full
            # ordered set preserves provenance for merged incremental windows.
            "content_hash": content_hash,
            "content_hashes": content_hashes,
            "raw_ref": getattr(item, "raw_ref", None),
            "rows": projection_rows,
        }
    return MappingProxyType(canonicalize(payload))


def _factor_unknowns(result: ResearchFactorResult) -> tuple[Mapping[str, Any], ...]:
    unknowns = []
    for component_name in ("value", "quality", "trend_timing", "catalyst", "risk"):
        component = getattr(result, component_name)
        for metric in component.metrics:
            if metric.status is not MetricStatus.MISSING:
                continue
            unknowns.append(
                {
                    "component": component_name,
                    "metric": metric.name,
                    "reason": metric.reason or "missing",
                }
            )
    unknowns.sort(key=lambda item: (item["component"], item["metric"], item["reason"]))
    return tuple(MappingProxyType(item) for item in unknowns)


def _factor_coverage(result: ResearchFactorResult) -> float:
    components = (result.value, result.quality, result.trend_timing, result.catalyst, result.risk)
    return round(sum(component.coverage for component in components) / len(components), 6)


def _factor_status(result: ResearchFactorResult) -> str:
    components = (result.value, result.quality, result.trend_timing, result.catalyst, result.risk)
    if all(component.status is ComponentStatus.AVAILABLE and component.score is not None for component in components):
        return DatasetStatus.AVAILABLE.value
    return DatasetStatus.PARTIAL.value


def _input_dataset_hashes(collection: Any) -> tuple[str, ...]:
    items = getattr(collection, "datasets", ())
    if not isinstance(items, Sequence):
        return ()
    hashes = {
        value
        for item in items
        for value in _dataset_content_hashes(item)
    }
    return tuple(sorted(hashes))


def _collection_status_without_factors(datasets_payload: Mapping[str, Any]) -> str:
    """Return the worst collection state, with factor-disabled as partial."""

    priority = {
        DatasetStatus.AVAILABLE.value: 0,
        DatasetStatus.EMPTY.value: 0,
        DatasetStatus.PARTIAL.value: 1,
        DatasetStatus.NOT_SUPPORTED.value: 2,
        DatasetStatus.STALE.value: 3,
        DatasetStatus.PERMISSION_DENIED.value: 4,
        DatasetStatus.FETCH_FAILED.value: 5,
    }
    selected = DatasetStatus.PARTIAL.value
    selected_priority = priority[selected]
    for item in datasets_payload.values():
        if not isinstance(item, Mapping):
            continue
        status = _normalized_status(item.get("status"))
        candidate_priority = priority.get(status, priority[DatasetStatus.PARTIAL.value])
        if candidate_priority > selected_priority:
            selected = status if status in priority else DatasetStatus.PARTIAL.value
            selected_priority = candidate_priority
    return selected


def _research_warnings(
    datasets_payload: Mapping[str, Any],
    *,
    factors_enabled: bool,
    factor_status: str,
    unknowns: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    warnings = []
    if not factors_enabled:
        warnings.append("research_factors_disabled")
    elif factor_status != DatasetStatus.AVAILABLE.value:
        warnings.append("research_factor_coverage_partial")
    if unknowns:
        warnings.append(f"research_factor_unknowns:{len(unknowns)}")
    warning_statuses = {
        DatasetStatus.PARTIAL.value,
        DatasetStatus.STALE.value,
        DatasetStatus.PERMISSION_DENIED.value,
        DatasetStatus.NOT_SUPPORTED.value,
        DatasetStatus.FETCH_FAILED.value,
    }
    for dataset in sorted(datasets_payload):
        item = datasets_payload[dataset]
        if not isinstance(item, Mapping):
            continue
        status = _normalized_status(item.get("status"))
        if status in warning_statuses:
            warnings.append(f"research_dataset_{dataset}_{status}")
    return tuple(warnings)


def _combined_research_status(factor_status: str, evidence_status: Optional[str]) -> str:
    if not evidence_status or evidence_status == DatasetStatus.AVAILABLE.value:
        return factor_status
    if factor_status == DatasetStatus.AVAILABLE.value:
        return DatasetStatus.PARTIAL.value
    return factor_status


def _news_dataset_payload(collection: Any, *, market: str) -> Mapping[str, Any]:
    content_hash = str(getattr(collection, "content_hash", "") or "").strip()
    if not _SHA256_RE.fullmatch(content_hash):
        raise ResearchRuntimeContractError(
            "persisted news_search collection must expose a lowercase SHA-256 hash"
        )
    to_input = getattr(collection, "to_dataset_input", None)
    if not callable(to_input):
        raise ResearchRuntimeContractError(
            "evidence collection must expose to_dataset_input"
        )
    item = to_input(market)
    normalized = item.normalized
    rows = (
        list(normalized)
        if isinstance(normalized, list)
        else [dict(normalized)]
        if isinstance(normalized, Mapping)
        else []
    )
    return MappingProxyType(
        canonicalize(
            {
                "dataset": item.dataset,
                "status": item.status,
                "row_count": len(rows),
                "available_at": item.available_at,
                "data_as_of": item.data_as_of,
                "content_hash": content_hash,
                "content_hashes": [content_hash],
                "raw_ref": item.raw_ref,
                "rows": rows,
            }
        )
    )


def _news_dataset_rows(collection: Any) -> tuple[Mapping[str, Any], ...]:
    raw_items = getattr(collection, "items", ())
    if not isinstance(raw_items, Sequence) or isinstance(
        raw_items, (str, bytes, bytearray)
    ):
        raise ResearchRuntimeContractError("evidence collection items must be a sequence")
    return tuple(
        MappingProxyType(canonicalize(dict(item)))
        for item in raw_items
        if isinstance(item, Mapping)
    )


class ResearchRuntimeService:
    """Two-stage research runtime with dependency-injected stateful edges."""

    def __init__(
        self,
        config: Any,
        *,
        collector: Any = None,
        repository: Any = None,
        provider_factory: Optional[Callable[[Any], Any]] = None,
        collector_factory: Optional[Callable[..., Any]] = None,
        raw_store_factory: Optional[Callable[[Path], Any]] = None,
        evidence_collector: Any = None,
        evidence_collector_factory: Optional[Callable[..., Any]] = None,
        durable_context_getter: Callable[[], Any] = _current_durable_context,
        diagnostic_updater: Callable[[str], None] = _update_diagnostic_hash,
    ) -> None:
        self.config = config
        self._collector = collector
        self._repository = repository or getattr(collector, "repository", None)
        self._provider_factory = provider_factory
        self._collector_factory = collector_factory or ResearchDatasetCollector
        self._raw_store_factory = raw_store_factory or RawArtifactStore
        self._evidence_collector = evidence_collector
        self._evidence_collector_factory = evidence_collector_factory
        self._durable_context_getter = durable_context_getter
        self._diagnostic_updater = diagnostic_updater
        self._provider_health_reporter: Optional[TushareProviderHealthReporter] = None
        self._provider_health_reporter_key: Optional[tuple[int, int]] = None

    def _personal_enabled(self) -> bool:
        return bool(_config_value(self.config, "personal_research_enabled", False))

    def _tushare_enabled(self) -> bool:
        return bool(_config_value(self.config, "tushare_research_enabled", False))

    def _factors_enabled(self) -> bool:
        return bool(_config_value(self.config, "research_factors_enabled", False))

    def _evidence_enabled(self) -> bool:
        return bool(_config_value(self.config, "research_evidence_enabled", False))

    def _debate_enabled(self) -> bool:
        return bool(_config_value(self.config, "research_debate_enabled", False))

    @staticmethod
    def _load_bound_debate(
        repository: Any,
        *,
        lease: LeaseFence,
        prepared: PreparedResearch,
    ) -> Any:
        """Load one fully bound Debate artifact without consulting origin_job_id."""

        from .debate_service import (
            hydrate_debate_request,
            hydrate_debate_snapshot,
        )

        evidence = prepared.evidence_snapshot
        if evidence is None:
            raise ResearchRuntimeContractError(
                "debate requires a frozen evidence snapshot"
            )
        result = repository.list_debate_snapshots(
            job_id=lease.job_id,
            stock_code=prepared.stock_code,
            evidence_snapshot_hash=evidence.evidence_hash,
            limit=100,
        )
        if not isinstance(result, Mapping) or not isinstance(
            result.get("items"), list
        ):
            raise ResearchRuntimeContractError(
                "repository.list_debate_snapshots returned an invalid contract"
            )
        records = result["items"]
        if not records:
            return None
        distinct_hashes = {
            str(item.get("debate_hash") or "")
            for item in records
            if isinstance(item, Mapping)
        }
        if len(records) != 1 or len(distinct_hashes) != 1:
            raise ResearchRuntimeContractError(
                "durable job contains ambiguous debate snapshots for one stock"
            )
        summary = records[0]
        if not isinstance(summary, Mapping):
            raise ResearchRuntimeContractError(
                "bound debate snapshot must be a mapping"
            )
        debate_hash = str(summary.get("debate_hash") or "")
        record = repository.get_debate_snapshot(debate_hash)
        if not isinstance(record, Mapping):
            raise ResearchRuntimeContractError(
                "bound debate snapshot references a missing immutable row"
            )
        request_hash = str(record.get("request_hash") or "")
        request_record = repository.get_debate_request(request_hash)
        if not isinstance(request_record, Mapping):
            raise ResearchRuntimeContractError(
                "bound debate snapshot references a missing frozen request"
            )
        request = hydrate_debate_request(
            request_record,
            evidence_snapshot=evidence,
        )
        snapshot = hydrate_debate_snapshot(record, request=request)
        if (
            snapshot.stock_code != prepared.stock_code
            or snapshot.evidence_snapshot_hash != evidence.evidence_hash
            or snapshot.as_of != evidence.as_of
            or snapshot.available_at != evidence.available_at
        ):
            raise ResearchRuntimeContractError(
                "bound debate lineage differs from prepared research"
            )
        return snapshot

    def prepare_debate(
        self,
        prepared: PreparedResearch,
        *,
        completion: Callable[[Any], Any],
        model_route: Mapping[str, Any],
    ) -> PreparedResearch:
        """Freeze, run, and persist the bounded Bull/Bear Debate stage."""

        if not isinstance(prepared, PreparedResearch):
            raise TypeError("prepared must be a PreparedResearch")
        if not prepared.debate_enabled:
            return prepared
        if not callable(completion):
            raise TypeError("completion must be callable")
        if not isinstance(model_route, Mapping):
            raise TypeError("model_route must be a mapping")
        if prepared.evidence_snapshot is None:
            raise ResearchRuntimeContractError(
                "debate-enabled research requires a frozen evidence snapshot"
            )

        from .debate_runner import run_research_debate
        from .debate_service import (
            DEBATE_PROMPT_VERSION,
            DebateFailure,
            build_debate_request,
            debate_context_from_snapshot,
            format_research_debate_context,
            hydrate_debate_request,
            hydrate_debate_turn,
        )

        self.checkpoint(prepared)
        repository = self._get_repository()
        snapshot = self._load_bound_debate(
            repository,
            lease=prepared.lease,
            prepared=prepared,
        )
        if snapshot is None:
            route_fingerprint = model_route_fingerprint(model_route)
            expected_request = build_debate_request(
                prepared.evidence_snapshot,
                model_route_fingerprint=route_fingerprint,
            )
            request_record = repository.get_job_debate_request(
                job_id=prepared.lease.job_id,
                stock_code=prepared.stock_code,
                evidence_snapshot_hash=prepared.evidence_snapshot.evidence_hash,
                prompt_version=DEBATE_PROMPT_VERSION,
                model_route_fingerprint=route_fingerprint,
            )
            if request_record is None:
                self.checkpoint(prepared)
                request_write = repository.write_debate_request(
                    expected_request.to_repository_input(),
                    lease=prepared.lease,
                )
                self.checkpoint(prepared)
                if request_write.content_hash != expected_request.request_hash:
                    raise ResearchRuntimeContractError(
                        "debate request repository returned a conflicting hash"
                    )
                request = expected_request
            else:
                request = hydrate_debate_request(
                    request_record,
                    evidence_snapshot=prepared.evidence_snapshot,
                )
                if request.request_hash != expected_request.request_hash:
                    raise ResearchRuntimeContractError(
                        "bound debate request differs from the current frozen contract"
                    )

            prompt_fingerprints = {
                item.stance: item.prompt_fingerprint
                for item in request.turn_requests
            }
            turn_records = repository.get_job_debate_turns(
                job_id=prepared.lease.job_id,
                stock_code=prepared.stock_code,
                evidence_snapshot_hash=request.evidence_snapshot_hash,
                request_hash=request.request_hash,
                prompt_version=request.prompt_version,
                prompt_fingerprints=prompt_fingerprints,
                model_route_fingerprint=request.model_route_fingerprint,
            )
            if not isinstance(turn_records, Mapping):
                raise ResearchRuntimeContractError(
                    "repository.get_job_debate_turns returned an invalid contract"
                )
            existing_turns = tuple(
                hydrate_debate_turn(turn_records[stance], request=request)
                for stance in ("bull", "bear")
                if stance in turn_records
            )
            failure_records = repository.get_job_debate_failures(
                job_id=prepared.lease.job_id,
                stock_code=prepared.stock_code,
                evidence_snapshot_hash=request.evidence_snapshot_hash,
                request_hash=request.request_hash,
                prompt_version=request.prompt_version,
                prompt_fingerprints=prompt_fingerprints,
                model_route_fingerprint=request.model_route_fingerprint,
            )
            if not isinstance(failure_records, Mapping):
                raise ResearchRuntimeContractError(
                    "repository.get_job_debate_failures returned an invalid contract"
                )
            existing_failures = tuple(
                DebateFailure(
                    stance=stance,
                    error_code=str(failure_records[stance].get("error_code") or ""),
                )
                for stance in ("bull", "bear")
                if stance in failure_records
                and isinstance(failure_records[stance], Mapping)
            )

            def _fenced_completion(call: Any) -> Any:
                self.checkpoint(prepared)
                result = completion(call)
                self.checkpoint(prepared)
                return result

            def _persist_turn(turn: Any) -> None:
                self.checkpoint(prepared)
                write_result = repository.write_debate_turn(
                    turn.to_repository_input(),
                    lease=prepared.lease,
                )
                self.checkpoint(prepared)
                if write_result.content_hash != turn.turn_hash:
                    raise ResearchRuntimeContractError(
                        "debate turn repository returned a conflicting hash"
                    )

            def _persist_failure(failure: Any) -> None:
                self.checkpoint(prepared)
                turn_request = request.request_for(failure.stance)
                repository.write_debate_failure(
                    DebateFailureInput(
                        stock_code=request.stock_code,
                        market=request.market,
                        stance=failure.stance,
                        debate_engine_version=request.debate_engine_version,
                        output_schema_version=request.output_schema_version,
                        prompt_version=request.prompt_version,
                        as_of=request.as_of,
                        available_at=request.available_at,
                        evidence_snapshot_hash=request.evidence_snapshot_hash,
                        request_hash=request.request_hash,
                        prompt_fingerprint=turn_request.prompt_fingerprint,
                        model_route_fingerprint=request.model_route_fingerprint,
                        error_code=failure.error_code,
                    ),
                    lease=prepared.lease,
                )
                self.checkpoint(prepared)

            run_result = run_research_debate(
                request,
                _fenced_completion,
                existing_turns=existing_turns,
                existing_failures=existing_failures,
                on_turn=_persist_turn,
                on_failure=_persist_failure,
            )
            snapshot = run_result.snapshot
            self.checkpoint(prepared)
            snapshot_write = repository.write_debate_snapshot(
                snapshot.to_repository_input(),
                lease=prepared.lease,
            )
            self.checkpoint(prepared)
            if snapshot_write.content_hash != snapshot.debate_hash:
                raise ResearchRuntimeContractError(
                    "debate snapshot repository returned a conflicting hash"
                )

        debate_context_values = dict(debate_context_from_snapshot(snapshot))
        debate_context_values.update(
            {
                "debate_hash": snapshot.debate_hash,
                "bull_argument_count": snapshot.bull_argument_count,
                "bear_argument_count": snapshot.bear_argument_count,
                "open_question_count": snapshot.open_question_count,
            }
        )
        debate_context = MappingProxyType(canonicalize(debate_context_values))
        warnings = tuple(prepared.research_context.get("warnings") or ())
        status = str(prepared.research_context.get("status") or "partial")
        if snapshot.status != DatasetStatus.AVAILABLE.value:
            warning = f"research_debate_{snapshot.status}"
            if warning not in warnings:
                warnings = (*warnings, warning)
            if status == DatasetStatus.AVAILABLE.value:
                status = DatasetStatus.PARTIAL.value
        research_context_values = dict(prepared.research_context)
        research_context_values.update(
            {
                "status": status,
                "warnings": warnings,
                "debate_snapshot_hash": snapshot.debate_hash,
                "debate_status": snapshot.status,
                "debate_bull_argument_count": snapshot.bull_argument_count,
                "debate_bear_argument_count": snapshot.bear_argument_count,
                "debate_open_question_count": snapshot.open_question_count,
            }
        )
        return replace(
            prepared,
            research_context=MappingProxyType(
                canonicalize(research_context_values)
            ),
            debate_snapshot=snapshot,
            debate_context=debate_context,
            debate_prompt_context=format_research_debate_context(snapshot),
        )

    def _new_evidence_collector(
        self,
        *,
        search_callable: Optional[Callable[..., Any]],
        repository: Any,
    ) -> Any:
        if self._evidence_collector is not None:
            collector = self._evidence_collector
            bound_repository = getattr(collector, "repository", None)
            if bound_repository is not None and bound_repository is not repository:
                raise ResearchRuntimeContractError(
                    "injected evidence collector belongs to a different repository"
                )
            if hasattr(collector, "repository"):
                collector.repository = repository
            if search_callable is not None and hasattr(collector, "search_callable"):
                collector.search_callable = search_callable
            return collector
        if self._evidence_collector_factory is not None:
            return self._evidence_collector_factory(
                search_callable=search_callable,
                repository=repository,
            )
        from .evidence_collector import EvidenceCollector

        return EvidenceCollector(
            search_callable=search_callable,
            repository=repository,
        )

    @staticmethod
    def _load_bound_evidence(
        repository: Any,
        *,
        lease: LeaseFence,
        stock_code: str,
        market: str,
        base_as_of: datetime,
        factor_snapshot_hash: str,
        base_dataset_hashes: Sequence[str],
    ) -> Any:
        from .evidence_service import hydrate_evidence_snapshot

        result = repository.list_evidence(
            job_id=lease.job_id,
            stock_code=stock_code,
            limit=100,
        )
        if not isinstance(result, Mapping) or not isinstance(result.get("items"), list):
            raise ResearchRuntimeContractError(
                "repository.list_evidence returned an invalid contract"
            )
        records = result["items"]
        if not records:
            return None
        distinct_hashes = {
            str(item.get("evidence_hash") or "")
            for item in records
            if isinstance(item, Mapping)
        }
        if len(records) != 1 or len(distinct_hashes) != 1:
            raise ResearchRuntimeContractError(
                "durable job contains ambiguous evidence snapshots for one stock"
            )
        snapshot = hydrate_evidence_snapshot(records[0])
        if snapshot.stock_code != stock_code or snapshot.market != market:
            raise ResearchRuntimeContractError(
                "bound evidence belongs to a different stock or market"
            )
        if snapshot.as_of < base_as_of:
            raise ResearchRuntimeContractError(
                "bound evidence predates the prepared research boundary"
            )
        if snapshot.factor_snapshot_hash != factor_snapshot_hash:
            raise ResearchRuntimeContractError(
                "bound evidence factor lineage differs from prepared research"
            )
        expected_base = set(base_dataset_hashes)
        if not expected_base.issubset(set(snapshot.input_dataset_hashes)):
            raise ResearchRuntimeContractError(
                "bound evidence dataset lineage differs from prepared research"
            )
        return snapshot

    def _prepare_evidence(
        self,
        *,
        context: Any,
        lease: LeaseFence,
        stock_code: str,
        stock_name: str,
        market: str,
        base_as_of: datetime,
        reference_mode: str,
        datasets_payload: Mapping[str, Any],
        factors: ResearchFactorResult,
        factor_snapshot: SnapshotWriteResult,
        collection: Any,
        search_callable: Optional[Callable[..., Any]],
    ) -> tuple[Any, Mapping[str, Any], str, Any, Mapping[str, Any]]:
        from .evidence_collector import hydrate_evidence_collection
        from .evidence_service import (
            build_evidence_snapshot,
            build_research_evidence_input,
            evidence_context_from_snapshot,
            format_research_evidence_context,
        )

        repository = self._get_repository()
        base_hashes = _input_dataset_hashes(collection)
        factor_hash = factor_snapshot.content_hash
        evidence_snapshot = self._load_bound_evidence(
            repository,
            lease=lease,
            stock_code=stock_code,
            market=market,
            base_as_of=base_as_of,
            factor_snapshot_hash=factor_hash,
            base_dataset_hashes=base_hashes,
        )
        news_record = repository.get_job_dataset(
            job_id=lease.job_id,
            dataset="news_search",
            scope_value=stock_code,
        )
        news_collection = (
            hydrate_evidence_collection(news_record)
            if news_record is not None
            else None
        )
        if evidence_snapshot is None:
            evidence_collector = self._new_evidence_collector(
                search_callable=search_callable,
                repository=repository,
            )
            _raise_if_stopped(context)
            try:
                news_collection = evidence_collector.collect(
                    stock_code,
                    stock_name=stock_name,
                    as_of=base_as_of,
                    reference_mode=reference_mode,
                    existing=news_collection,
                    cancel_event=_durable_stop_signal(context),
                )
            except Exception:
                # The generic collector reports a stopped signal with its own
                # boundary error. Translate it back through the durable
                # context so cancellation and stale leases keep their typed
                # worker semantics; otherwise preserve the original failure.
                _raise_if_stopped(context)
                raise
            _raise_if_stopped(context)
            news_collection = evidence_collector.persist(
                news_collection,
                market=market,
                lease=lease,
            )
            _raise_if_stopped(context)
            build_input = build_research_evidence_input(
                stock_code=stock_code,
                market=market,
                as_of=news_collection.as_of,
                datasets=datasets_payload,
                factors=factors,
                factor_snapshot_hash=factor_hash,
                collection=news_collection,
            )
            evidence_snapshot = build_evidence_snapshot(build_input)
            _raise_if_stopped(context)
            write_result = evidence_snapshot.persist(repository, lease=lease)
            _raise_if_stopped(context)
            if write_result.content_hash != evidence_snapshot.evidence_hash:
                raise ResearchRuntimeContractError(
                    "evidence repository returned a conflicting content hash"
                )
        elif news_collection is None:
            raise ResearchRuntimeContractError(
                "bound evidence is missing its durable news_search dataset binding"
            )
        if news_collection is None:
            raise ResearchRuntimeContractError(
                "evidence preparation did not produce a news_search collection"
            )
        if news_collection.content_hash not in evidence_snapshot.input_dataset_hashes:
            raise ResearchRuntimeContractError(
                "news_search dataset is absent from evidence lineage"
            )
        expected_hashes = set(base_hashes)
        expected_hashes.add(news_collection.content_hash)
        if set(evidence_snapshot.input_dataset_hashes) != expected_hashes:
            raise ResearchRuntimeContractError(
                "evidence input dataset hashes do not match consumed artifacts"
            )
        evidence_context = dict(evidence_context_from_snapshot(evidence_snapshot))
        evidence_context["evidence_hash"] = evidence_snapshot.evidence_hash
        return (
            evidence_snapshot,
            MappingProxyType(canonicalize(evidence_context)),
            format_research_evidence_context(evidence_snapshot),
            news_collection,
            _news_dataset_payload(news_collection, market=market),
        )

    def _default_raw_root(self) -> Path:
        database_path = Path(str(_config_value(self.config, "database_path", "./data/stock_analysis.db")))
        return database_path.expanduser().resolve(strict=False).parent / "research" / "raw"

    def _get_repository(self) -> Any:
        if self._repository is None:
            self._repository = ResearchSnapshotRepository()
        return self._repository

    def _build_default_provider(self) -> TushareProvider:
        token = str(_config_value(self.config, "tushare_token", "") or "").strip()
        if not token:
            raise ValueError("TUSHARE_RESEARCH_ENABLED requires TUSHARE_TOKEN")
        api_url = resolve_tushare_api_url_from_env(default=DEFAULT_TUSHARE_HTTP_URL) or DEFAULT_TUSHARE_HTTP_URL
        return TushareProvider(
            token=token,
            api_url=api_url,
            enforce_limits=True,
            worker_only=True,
            global_calls_per_minute=int(_config_value(self.config, "tushare_global_calls_per_minute", 450)),
            max_inflight=int(_config_value(self.config, "tushare_max_inflight", 2)),
            endpoint_limits=dict(_config_value(self.config, "tushare_endpoint_limits", {}) or {}),
            worker_context_checker=lambda: self._durable_context_getter() is not None,
        )

    def _get_collector(self) -> Any:
        if self._collector is not None:
            return self._collector
        provider = self._provider_factory(self.config) if self._provider_factory is not None else self._build_default_provider()
        repository = self._get_repository()
        raw_store = self._raw_store_factory(self._default_raw_root())
        self._collector = self._collector_factory(provider, repository, raw_store)
        return self._collector

    def _active_lease(self) -> tuple[Any, LeaseFence]:
        context = self._durable_context_getter()
        return context, _lease_from_context(context)

    def checkpoint(self, prepared: PreparedResearch) -> None:
        """Validate both the in-process stop signal and persisted lease fence."""

        if not isinstance(prepared, PreparedResearch):
            raise TypeError("prepared must be a PreparedResearch")
        context, active_lease = self._active_lease()
        if active_lease != prepared.lease:
            raise RuntimeError("research checkpoint lease no longer matches prepare")
        _raise_if_stopped(context)
        self._get_repository().assert_live_lease(prepared.lease)
        _raise_if_stopped(context)

    def get_bound_research_snapshot(
        self,
        prepared: PreparedResearch,
    ) -> Optional[Mapping[str, Any]]:
        """Return the task-bound final snapshot after exact lineage checks."""

        if not isinstance(prepared, PreparedResearch):
            raise TypeError("prepared must be a PreparedResearch")
        self.checkpoint(prepared)
        record = self._get_repository().get_job_research_snapshot(
            job_id=prepared.lease.job_id,
            stock_code=prepared.stock_code,
        )
        if record is None:
            return None
        expected = {
            "stock_code": prepared.stock_code,
            "market": prepared.market,
            "as_of": prepared.as_of,
            "factor_snapshot_hash": (
                prepared.factor_snapshot.content_hash
                if prepared.factor_snapshot is not None
                else None
            ),
            "evidence_snapshot_hash": (
                prepared.evidence_snapshot.evidence_hash
                if prepared.evidence_snapshot is not None
                else None
            ),
            "debate_snapshot_hash": (
                prepared.debate_snapshot.debate_hash
                if prepared.debate_snapshot is not None
                else None
            ),
        }
        raw_as_of = record.get("as_of")
        if isinstance(raw_as_of, str):
            try:
                raw_as_of = datetime.fromisoformat(
                    raw_as_of.strip().replace("Z", "+00:00")
                )
            except ValueError as exc:
                raise ResearchRuntimeContractError(
                    "bound final Research snapshot has an invalid as_of"
                ) from exc
        actual = {
            "stock_code": str(record.get("stock_code") or ""),
            "market": str(record.get("market") or ""),
            "as_of": _aware_utc(raw_as_of, field="snapshot.as_of"),
            "factor_snapshot_hash": record.get("factor_snapshot_hash"),
            "evidence_snapshot_hash": record.get("evidence_snapshot_hash"),
            "debate_snapshot_hash": record.get("debate_snapshot_hash"),
        }
        if actual != expected:
            mismatched = sorted(
                key for key in expected if expected[key] != actual[key]
            )
            raise ResearchRuntimeContractError(
                "bound final Research snapshot differs from prepared lineage: "
                + ",".join(mismatched)
            )
        return MappingProxyType(dict(record))

    def _flush_provider_health(self, context: Any, collector: Any) -> bool:
        """Best-effort collection-boundary telemetry with no job semantics."""

        store = getattr(context, "store", None)
        provider = getattr(collector, "provider", None)
        coordinator = getattr(provider, "coordinator", None)
        if store is None or not isinstance(coordinator, TushareRequestCoordinator):
            return False
        key = (id(store), id(coordinator))
        try:
            if (
                self._provider_health_reporter is None
                or self._provider_health_reporter_key != key
            ):
                self._provider_health_reporter = TushareProviderHealthReporter(
                    store,
                    coordinator,
                )
                self._provider_health_reporter_key = key
            return self._provider_health_reporter.flush()
        except Exception as exc:  # noqa: BLE001 - telemetry is fail-open.
            logger.warning(
                "Tushare provider health flush failed: error_type=%s",
                type(exc).__name__,
            )
            return False

    def prepare(
        self,
        stock_code: str,
        market: str,
        as_of: datetime,
        scenario: Optional[Mapping[str, Any]] = None,
        *,
        reference_mode: Literal["live", "historical"] = "historical",
        evidence_search: Optional[Callable[..., Any]] = None,
        requested_mode: str = "auto",
        priority: int = 50,
        budget_reservations: Sequence[Mapping[str, Any]] = (),
    ) -> Optional[PreparedResearch]:
        personal_enabled = self._personal_enabled()
        tushare_enabled = self._tushare_enabled()
        factors_enabled = self._factors_enabled()
        evidence_enabled = self._evidence_enabled()
        debate_capability_enabled = self._debate_enabled()
        if (
            not personal_enabled
            and not tushare_enabled
            and not factors_enabled
            and not evidence_enabled
            and not debate_capability_enabled
        ):
            return None
        if factors_enabled and (not personal_enabled or not tushare_enabled):
            raise RuntimeError("RESEARCH_FACTORS_ENABLED requires PERSONAL_RESEARCH_ENABLED and TUSHARE_RESEARCH_ENABLED")
        if not tushare_enabled:
            return None
        if not personal_enabled:
            raise RuntimeError("TUSHARE_RESEARCH_ENABLED requires PERSONAL_RESEARCH_ENABLED")
        if evidence_enabled and not factors_enabled:
            raise RuntimeError(
                "RESEARCH_EVIDENCE_ENABLED requires RESEARCH_FACTORS_ENABLED"
            )
        if debate_capability_enabled and not evidence_enabled:
            raise RuntimeError(
                "RESEARCH_DEBATE_ENABLED requires RESEARCH_EVIDENCE_ENABLED"
            )
        if reference_mode not in {"live", "historical"}:
            raise ValueError("reference_mode must be 'live' or 'historical'")

        boundary = normalize_as_of(as_of)
        context, lease = self._active_lease()
        collector = self._get_collector()
        collect_kwargs = {
            "as_of": boundary,
            "lease": lease,
            "cancel_event": _durable_stop_signal(context),
            "reference_mode": reference_mode,
        }
        try:
            collection = collector.collect(str(stock_code).strip(), **collect_kwargs)
        except Exception:
            # Transport failures still update the process coordinator. Project
            # that state before propagating the original collection error;
            # telemetry itself is fail-open and can never replace the cause.
            self._flush_provider_health(context, collector)
            raise
        _raise_if_stopped(context)
        self._flush_provider_health(context, collector)
        _raise_if_stopped(context)
        rows_by_dataset = _adapt_rows_by_dataset(collection)
        try:
            collection_boundary = _aware_utc(
                getattr(collection, "as_of", None),
                field="collection.as_of",
            )
        except TypeError as exc:
            raise ResearchRuntimeContractError(
                "collector result must expose an aware as_of datetime"
            ) from exc
        available_at = _adapt_max_available_at(
            collection,
            as_of=collection_boundary,
        )
        datasets_payload = _adapt_datasets_payload(
            collection,
            rows_by_dataset,
            as_of=collection_boundary,
        )

        factor_input = None
        factors = None
        factor_snapshot = None
        repository = None
        if factors_enabled:
            _raise_if_stopped(context)
            factor_input = build_factor_input(
                rows_by_dataset,
                stock_code=str(stock_code).strip(),
                market=str(market).strip() or "A",
                as_of=collection_boundary,
                scenario=scenario,
            )
            factors = evaluate_research_factors(factor_input)
            _raise_if_stopped(context)
            repository = self._get_repository()
            factor_snapshot_input = FactorSnapshotInput(
                stock_code=factors.stock_code,
                market=str(market).strip() or "A",
                company_profile=factors.profile.profile,
                engine_bundle_version=FACTOR_ENGINE_VERSION,
                factor_payload=factors.to_dict(),
                input_dataset_hashes=_input_dataset_hashes(collection),
                status=_factor_status(factors),
                coverage=_factor_coverage(factors),
                unknowns=_factor_unknowns(factors),
                as_of=collection_boundary,
                available_at=available_at,
                primary_horizon=factors.trend_timing.primary_horizon or 10,
                value_score=factors.value.score,
                quality_score=factors.quality.score,
                trend_score=factors.trend_timing.score,
                catalyst_score=factors.catalyst.score,
                risk_penalty=factors.risk.score,
            )
            _raise_if_stopped(context)
            factor_snapshot = repository.write_factors(factor_snapshot_input, lease=lease)
            _raise_if_stopped(context)

        evidence_snapshot = None
        evidence_context = None
        evidence_prompt_context = None
        evidence_status = None
        if evidence_enabled:
            if factors is None or factor_snapshot is None:
                raise ResearchRuntimeContractError(
                    "evidence requires a persisted deterministic factor snapshot"
                )
            stock_name = str(stock_code).strip()
            for item in rows_by_dataset.get("stock_basic", ()):
                if not isinstance(item, Mapping):
                    continue
                candidate = str(item.get("name") or item.get("fullname") or "").strip()
                if candidate:
                    stock_name = candidate
                    break
            (
                evidence_snapshot,
                evidence_context,
                evidence_prompt_context,
                news_collection,
                news_payload,
            ) = self._prepare_evidence(
                context=context,
                lease=lease,
                stock_code=str(stock_code).strip(),
                stock_name=stock_name,
                market=str(market).strip() or "A",
                base_as_of=collection_boundary,
                reference_mode=reference_mode,
                datasets_payload=datasets_payload,
                factors=factors,
                factor_snapshot=factor_snapshot,
                collection=collection,
                search_callable=evidence_search,
            )
            if evidence_snapshot.as_of < collection_boundary:
                raise ResearchRuntimeContractError(
                    "evidence boundary cannot precede dataset collection boundary"
                )
            collection_boundary = evidence_snapshot.as_of
            available_at = max(available_at, evidence_snapshot.available_at)
            combined_datasets = dict(datasets_payload)
            combined_datasets["news_search"] = news_payload
            datasets_payload = MappingProxyType(combined_datasets)
            combined_rows = dict(rows_by_dataset)
            combined_rows["news_search"] = _news_dataset_rows(news_collection)
            rows_by_dataset = MappingProxyType(combined_rows)
            evidence_status = evidence_snapshot.status

        factor_unknowns = _factor_unknowns(factors) if factors is not None else ()
        factor_or_dataset_status = (
            _factor_status(factors)
            if factors is not None
            else _collection_status_without_factors(datasets_payload)
        )
        research_status = _combined_research_status(
            factor_or_dataset_status,
            evidence_status,
        )
        research_warnings = _research_warnings(
            datasets_payload,
            factors_enabled=factors_enabled,
            factor_status=factor_or_dataset_status,
            unknowns=factor_unknowns,
        )
        if evidence_enabled and evidence_status != DatasetStatus.AVAILABLE.value:
            research_warnings = (
                *research_warnings,
                f"research_evidence_{evidence_status or 'partial'}",
            )
        dataset_summary = {
            dataset: {
                "row_count": len(rows),
                **(
                    {"status": datasets_payload[dataset].get("status")}
                    if isinstance(datasets_payload.get(dataset), Mapping)
                    and datasets_payload[dataset].get("status")
                    else {}
                ),
            }
            for dataset, rows in rows_by_dataset.items()
        }
        research_context_values = {
            "stock_code": str(stock_code).strip(),
            "market": str(market).strip() or "A",
            "as_of": collection_boundary,
            "available_at": available_at,
            "status": research_status,
            "datasets": dataset_summary,
            "factors": factors.to_dict() if factors is not None else None,
            "unknowns": factor_unknowns,
            "warnings": research_warnings,
            "factor_snapshot_hash": (
                factor_snapshot.content_hash if factor_snapshot is not None else None
            ),
        }
        if evidence_snapshot is not None:
            research_context_values.update(
                {
                    "evidence_snapshot_hash": evidence_snapshot.evidence_hash,
                    "evidence_status": evidence_snapshot.status,
                    "evidence_coverage": evidence_snapshot.coverage,
                }
            )
        task_decision = None
        debate_enabled = False
        if factors is not None and evidence_snapshot is not None:
            from .personal_task_decision import (
                derive_personal_research_task_decision,
            )

            task_decision = derive_personal_research_task_decision(
                requested_mode=requested_mode,
                priority=priority,
                debate_enabled=debate_capability_enabled,
                factors=factors,
                evidence_snapshot=evidence_snapshot,
            )
            debate_enabled = task_decision.debate.triggered
            research_context_values.update(
                {
                    "research_task_mode": task_decision.mode.resolved_mode,
                    "research_task_decision_hash": task_decision.decision_hash,
                    "debate_triggered": debate_enabled,
                    "debate_trigger_reason_codes": list(
                        task_decision.debate.reason_codes
                    ),
                }
            )
        normalized_reservations = tuple(
            MappingProxyType(canonicalize(dict(item), exclude_volatile=False))
            for item in budget_reservations
        )
        research_context = MappingProxyType(canonicalize(research_context_values))
        return PreparedResearch(
            stock_code=str(stock_code).strip(),
            market=str(market).strip() or "A",
            as_of=collection_boundary,
            available_at=available_at,
            lease=lease,
            collection=collection,
            rows_by_dataset=rows_by_dataset,
            datasets_payload=datasets_payload,
            research_context=research_context,
            factor_input=MappingProxyType(canonicalize(factor_input)) if factor_input is not None else None,
            factors=factors,
            factor_snapshot=factor_snapshot,
            factors_enabled=factors_enabled,
            evidence_snapshot=evidence_snapshot,
            evidence_context=evidence_context,
            evidence_prompt_context=evidence_prompt_context,
            evidence_enabled=evidence_enabled,
            debate_enabled=debate_enabled,
            task_decision=task_decision,
            budget_reservations=normalized_reservations,
        )

    def freeze(
        self,
        prepared: PreparedResearch,
        context_pack: Any,
        prompt_version: str,
        prompt: Any,
        model_route: Mapping[str, Any],
        policy_version: str,
        policy: Any,
        *,
        expected_snapshot_hash: Optional[str] = None,
    ) -> FrozenResearchWrite:
        if not isinstance(prepared, PreparedResearch):
            raise TypeError("prepared must be a PreparedResearch")
        context, active_lease = self._active_lease()
        if active_lease != prepared.lease:
            raise RuntimeError("research freeze lease fence no longer matches prepare")
        if prepared.available_at > prepared.as_of:
            raise ResearchRuntimeContractError("prepared available_at cannot be after as_of")
        if isinstance(context_pack, Mapping):
            pack_version = context_pack.get("pack_version")
        else:
            pack_version = getattr(context_pack, "pack_version", None)
        pack_version_text = str(pack_version or "").strip()
        if not pack_version_text:
            raise ResearchRuntimeContractError("context_pack must expose pack_version")
        snapshot_kwargs: dict[str, Any] = {}
        if prepared.evidence_enabled:
            if prepared.evidence_snapshot is None or prepared.evidence_context is None:
                raise ResearchRuntimeContractError(
                    "evidence-enabled research must expose a frozen evidence snapshot"
                )
            snapshot_kwargs.update(
                {
                    "snapshot_version": EVIDENCE_SNAPSHOT_VERSION,
                    "field_dictionary_version": EVIDENCE_FIELD_DICTIONARY_VERSION,
                    "evidence": prepared.evidence_snapshot.canonical_payload,
                    "evidence_snapshot_hash": prepared.evidence_snapshot.evidence_hash,
                }
            )
        if prepared.debate_enabled:
            if prepared.debate_snapshot is None or prepared.debate_context is None:
                raise ResearchRuntimeContractError(
                    "debate-enabled research must expose a frozen debate snapshot"
                )
            snapshot_kwargs.update(
                {
                    "snapshot_version": DEBATE_SNAPSHOT_VERSION,
                    "field_dictionary_version": DEBATE_FIELD_DICTIONARY_VERSION,
                    "debate": prepared.debate_snapshot.canonical_payload,
                    "debate_snapshot_hash": prepared.debate_snapshot.debate_hash,
                }
            )
        snapshot = build_research_snapshot(
            stock_code=prepared.stock_code,
            market=prepared.market,
            as_of=prepared.as_of,
            available_at=prepared.available_at,
            context_pack=context_pack,
            datasets=prepared.datasets_payload,
            factors=prepared.factors,
            prompt_version=prompt_version,
            prompt=prompt,
            model_route=model_route,
            policy_version=policy_version,
            policy=policy,
            pack_version=pack_version_text,
            status=(
                str(prepared.research_context.get("status") or "partial")
            ),
            factor_engine_version=FACTOR_ENGINE_VERSION,
            factor_snapshot_hash=(
                prepared.factor_snapshot.content_hash
                if prepared.factor_snapshot is not None
                else None
            ),
            **snapshot_kwargs,
        )
        if expected_snapshot_hash is not None:
            expected_hash = str(expected_snapshot_hash or "").strip()
            if not _SHA256_RE.fullmatch(expected_hash):
                raise ValueError(
                    "expected_snapshot_hash must be a lowercase SHA-256 digest"
                )
            if snapshot.snapshot_hash != expected_hash:
                raise ResearchRuntimeContractError(
                    "retry candidate differs from the task-bound final Research snapshot"
                )
        _raise_if_stopped(context)
        write_result = snapshot.persist(self._get_repository(), lease=prepared.lease)
        _raise_if_stopped(context)
        self._diagnostic_updater(snapshot.snapshot_hash)
        return FrozenResearchWrite(snapshot=snapshot, write_result=write_result)


__all__ = [
    "FrozenResearchWrite",
    "PreparedResearch",
    "ResearchRuntimeContractError",
    "ResearchRuntimeService",
]
