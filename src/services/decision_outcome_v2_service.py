"""Durable-worker orchestration for immutable Decision Outcome v2."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from data_provider.base import normalize_stock_code
from data_provider.tushare_provider import (
    DEFAULT_TUSHARE_HTTP_URL,
    TushareProvider,
    resolve_tushare_api_url_from_env,
)
from src.config import Config, get_config
from src.core.decision_outcome_v2_evaluator import (
    BenchmarkOutcomeV2,
    DECISION_OUTCOME_V2_ENGINE_VERSION,
    DecisionOutcomeV2Evaluation,
    DecisionOutcomeV2Evaluator,
    SUPPORTED_DECISION_OUTCOME_V2_HORIZONS,
    normalize_final_action_family,
)
from src.repositories.decision_outcome_v2_repo import (
    DECISION_OUTCOME_V2_HORIZONS,
    DecisionOutcomeV2Candidate,
    DecisionOutcomeV2Repository,
)
from src.services.research import (
    CSI300_INDEX_CODE,
    CSI300_INDEX_NAME,
    DatasetCollectionResult,
    LeaseFence,
    RawArtifactStore,
    ResearchDatasetCollector,
    ResearchSnapshotRepository,
    benchmark_dataset_unavailable_reason,
    build_csi300_index_daily_query,
    build_sw1_index_classify_query,
    build_sw1_index_daily_query,
    build_sw1_index_member_all_query,
    resolve_sw1_membership,
)
from src.services.research.availability import parse_tushare_date
from src.storage import DatabaseManager


_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CRITICAL_DATASETS = ("daily", "adj_factor", "stk_limit", "suspend_d")


@dataclass(frozen=True)
class DecisionOutcomeV2DatasetBundle:
    """Frozen rows and lineage consumed by one signal/horizon evaluation."""

    xshg_sessions: tuple[date, ...]
    stock_bars: tuple[Mapping[str, Any], ...]
    suspend_rows: tuple[Mapping[str, Any], ...]
    stk_limit_rows: tuple[Mapping[str, Any], ...]
    csi300_bars: tuple[Mapping[str, Any], ...]
    sw1_membership: Optional[Mapping[str, Any]]
    sw1_bars: tuple[Mapping[str, Any], ...]
    dataset_hashes: tuple[str, ...]
    evaluation_as_of: datetime
    critical_reason: Optional[str] = None
    csi300_reason: Optional[str] = None
    sw1_reason: Optional[str] = None


@dataclass(frozen=True)
class Sw1PointInTimeSnapshotCapture:
    """Audit summary for the SW1 snapshots frozen by personal research."""

    status: str
    reason: Optional[str]
    available_at: datetime
    classify_status: str
    member_status: str
    dataset_hashes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "available_at": self.available_at.isoformat(),
            "classify_status": self.classify_status,
            "member_status": self.member_status,
            "dataset_hashes": list(self.dataset_hashes),
        }


@dataclass(frozen=True)
class _StopSignal:
    signals: tuple[Any, ...]

    def is_set(self) -> bool:
        return any(bool(signal.is_set()) for signal in self.signals)


def _current_context() -> Any:
    from src.services.durable_job_handlers import (
        get_optional_durable_execution_context,
    )

    return get_optional_durable_execution_context()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("datetime value is required")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _xshg_sessions(signal_session: date, count: int) -> tuple[date, ...]:
    if not isinstance(signal_session, date) or isinstance(signal_session, datetime):
        raise TypeError("signal_session must be a date")
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ValueError("session count must be positive")
    try:
        import exchange_calendars as xcals

        calendar = xcals.get_calendar("XSHG")
        start = signal_session + timedelta(days=1)
        end = signal_session + timedelta(days=max(120, count * 5))
        sessions = tuple(
            value.date() for value in calendar.sessions_in_range(start, end)
        )
    except Exception as exc:
        raise RuntimeError("xshg_calendar_unavailable") from exc
    if len(sessions) < count:
        raise RuntimeError("insufficient_xshg_calendar_range")
    return sessions[:count]


class DecisionOutcomeV2Service:
    """Evaluate only from a live durable Worker and immutable Dataset writes."""

    def __init__(
        self,
        *,
        repository: Optional[DecisionOutcomeV2Repository] = None,
        db_manager: Optional[DatabaseManager] = None,
        config: Optional[Config] = None,
        collector: Optional[ResearchDatasetCollector] = None,
        context_getter: Callable[[], Any] = _current_context,
        clock: Callable[[], datetime] = _utc_now,
        session_resolver: Callable[[date, int], tuple[date, ...]] = _xshg_sessions,
        bundle_loader: Optional[
            Callable[
                [DecisionOutcomeV2Candidate, str, datetime, Any],
                DecisionOutcomeV2DatasetBundle,
            ]
        ] = None,
    ) -> None:
        resolved_db = db_manager or getattr(repository, "db", None)
        if repository is None and resolved_db is None:
            resolved_db = DatabaseManager.get_instance()
        self.db = resolved_db
        self.repository = repository or DecisionOutcomeV2Repository(resolved_db)
        self.config = config or get_config()
        self._collector = collector
        self._context_getter = context_getter
        self._clock = clock
        self._session_resolver = session_resolver
        self._bundle_loader = bundle_loader
        self._base_results: dict[int, dict[str, DatasetCollectionResult]] = {}
        self._membership_results: dict[
            int,
            tuple[DatasetCollectionResult, DatasetCollectionResult],
        ] = {}

    def capture_sw1_membership_snapshots(
        self,
        stock_code: str,
    ) -> Sw1PointInTimeSnapshotCapture:
        """Freeze SW1 classify/member state before a formal signal is created.

        This method is intentionally Worker-only and uses ``reference_mode=live``.
        Outcome replay uses the separate historical path below, so it can only
        consume one of these observations when it was available by the signal's
        persisted ``created_at`` decision boundary.
        """

        context = self._require_durable_context()
        job_type = getattr(
            getattr(context, "claimed_job", None),
            "job_type",
            getattr(context, "job_type", None),
        )
        if job_type != "personal_research":
            raise RuntimeError(
                "SW1 point-in-time capture requires a personal_research Worker"
            )
        self._assert_enabled()
        normalized_stock = normalize_stock_code(stock_code)
        boundary = _aware_utc(self._clock())
        collector = self._get_collector()
        lease = self._lease(context)
        stop_signal = self._stop_signal(context)
        classify_plan = build_sw1_index_classify_query()
        member_plan = build_sw1_index_member_all_query(normalized_stock)

        context.checkpoint()
        classify = self._collect_dataset(
            collector,
            normalized_stock,
            classify_plan.dataset,
            as_of=boundary,
            lease=lease,
            cancel_event=stop_signal,
            reference_mode="live",
            query_plan=classify_plan,
        )
        context.checkpoint()
        member = self._collect_dataset(
            collector,
            normalized_stock,
            member_plan.dataset,
            as_of=boundary,
            lease=lease,
            cancel_event=stop_signal,
            reference_mode="live",
            query_plan=member_plan,
        )
        context.checkpoint()

        reason = self._membership_capture_reason(classify, member)
        hashes = tuple(sorted({
            digest
            for result in (classify, member)
            for digest in self._result_hashes(result)
        }))
        return Sw1PointInTimeSnapshotCapture(
            status="available" if reason is None else "unavailable",
            reason=reason,
            available_at=max(classify.available_at, member.available_at),
            classify_status=classify.status,
            member_status=member.status,
            dataset_hashes=hashes,
        )

    def run_outcomes(
        self,
        *,
        signal_id: Optional[int] = None,
        horizons: Optional[Sequence[str]] = None,
        stock_code: Optional[str] = None,
        decision_profile: Optional[str] = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        context = self._require_durable_context()
        self._assert_enabled()
        horizon_values = self._normalize_horizons(horizons)
        stock_norm = (
            normalize_stock_code(stock_code) if stock_code not in (None, "") else None
        )
        profile_norm = (
            str(decision_profile).strip().lower()
            if decision_profile not in (None, "")
            else None
        )
        candidates = self.repository.list_candidate_keys(
            horizons=horizon_values,
            engine_version=DECISION_OUTCOME_V2_ENGINE_VERSION,
            limit=limit,
            signal_id=signal_id,
            stock_code=stock_norm,
            decision_profile=profile_norm,
        )
        self._base_results.clear()
        self._membership_results.clear()
        run_as_of = _aware_utc(self._clock())
        items: list[dict[str, Any]] = []
        counts = {
            "created": 0,
            "updated": 0,
            "transitioned": 0,
            "unchanged": 0,
        }
        total = len(candidates)
        for index, candidate in enumerate(candidates):
            context.checkpoint()
            # Exact benchmark query plans are part of the immutable Dataset
            # identity. A unique boundary prevents another signal/horizon in
            # the same maintenance job from reusing a different exact window.
            candidate_as_of = run_as_of - timedelta(microseconds=total - index)
            bundle = self._load_bundle(
                candidate,
                candidate.horizon,
                candidate_as_of,
                context,
            )
            evaluation = self._evaluate(candidate, bundle)
            result = self.repository.persist_evaluation(
                signal_id=int(candidate.signal.id),
                evaluation=evaluation,
            )
            if result.created:
                counts["created"] += 1
            elif result.transitioned:
                counts["transitioned"] += 1
            elif result.disposition == "updated":
                counts["updated"] += 1
            else:
                counts["unchanged"] += 1
            items.append({
                "id": result.row.id,
                "signal_id": result.row.signal_id,
                "horizon": result.row.horizon,
                "engine_version": result.row.engine_version,
                "eval_status": result.row.eval_status,
                "reason_code": result.row.reason_code,
                "observation_hash": result.row.observation_hash,
                "disposition": result.disposition,
            })
            progress = 95 if total == 0 else min(95, 10 + int((index + 1) / total * 85))
            context.progress(
                progress,
                f"Decision Outcome v2 evaluated {index + 1}/{total}",
                stage="decision_outcomes_v2",
            )
        context.checkpoint()
        return {
            "contract": "decision-outcome-v2-run",
            "version": "v1",
            "engine_version": DECISION_OUTCOME_V2_ENGINE_VERSION,
            "horizons": list(horizon_values),
            "selected": total,
            **counts,
            "items": items,
        }

    def _load_bundle(
        self,
        candidate: DecisionOutcomeV2Candidate,
        horizon: str,
        as_of: datetime,
        context: Any,
    ) -> DecisionOutcomeV2DatasetBundle:
        if self._bundle_loader is not None:
            bundle = self._bundle_loader(candidate, horizon, as_of, context)
            if not isinstance(bundle, DecisionOutcomeV2DatasetBundle):
                raise TypeError("bundle_loader must return DecisionOutcomeV2DatasetBundle")
            return bundle
        return self._collect_bundle(candidate, horizon, as_of, context)

    def _collect_bundle(
        self,
        candidate: DecisionOutcomeV2Candidate,
        horizon: str,
        as_of: datetime,
        context: Any,
    ) -> DecisionOutcomeV2DatasetBundle:
        signal = candidate.signal
        signal_created_at = _aware_utc(signal.created_at)
        signal_session = signal_created_at.astimezone(_SHANGHAI).date()
        horizon_days = SUPPORTED_DECISION_OUTCOME_V2_HORIZONS[horizon]
        try:
            sessions = self._session_resolver(signal_session, 20)
        except (TypeError, ValueError, RuntimeError) as exc:
            return DecisionOutcomeV2DatasetBundle(
                xshg_sessions=(),
                stock_bars=(),
                suspend_rows=(),
                stk_limit_rows=(),
                csi300_bars=(),
                sw1_membership=None,
                sw1_bars=(),
                dataset_hashes=(),
                evaluation_as_of=as_of,
                critical_reason=str(exc) or "xshg_calendar_unavailable",
                csi300_reason="xshg_calendar_unavailable",
                sw1_reason="xshg_calendar_unavailable",
            )
        entry_date = sessions[0]
        end_date = sessions[horizon_days - 1]
        horizon_close = datetime.combine(
            end_date,
            time(15, 0),
            tzinfo=_SHANGHAI,
        ).astimezone(timezone.utc)
        if _aware_utc(as_of) < horizon_close:
            return DecisionOutcomeV2DatasetBundle(
                xshg_sessions=tuple(sessions),
                stock_bars=(),
                suspend_rows=(),
                stk_limit_rows=(),
                csi300_bars=(),
                sw1_membership=None,
                sw1_bars=(),
                dataset_hashes=(),
                evaluation_as_of=as_of,
            )
        stock_code = signal.stock_code
        lease = self._lease(context)
        stop_signal = self._stop_signal(context)
        collector = self._get_collector()

        base = self._base_results.get(int(signal.id))
        if base is None:
            base = {
                dataset: self._collect_dataset(
                    collector,
                    stock_code,
                    dataset,
                    as_of=as_of,
                    lease=lease,
                    cancel_event=stop_signal,
                )
                for dataset in _CRITICAL_DATASETS
            }
            self._base_results[int(signal.id)] = base

        membership_results = self._membership_results.get(int(signal.id))
        if membership_results is None:
            classify_plan = build_sw1_index_classify_query()
            member_plan = build_sw1_index_member_all_query(stock_code)
            classify = self._collect_dataset(
                collector,
                stock_code,
                classify_plan.dataset,
                as_of=signal_created_at,
                lease=lease,
                cancel_event=stop_signal,
                reference_mode="historical",
                query_plan=classify_plan,
            )
            member = self._collect_dataset(
                collector,
                stock_code,
                member_plan.dataset,
                as_of=signal_created_at,
                lease=lease,
                cancel_event=stop_signal,
                reference_mode="historical",
                query_plan=member_plan,
            )
            membership_results = (classify, member)
            self._membership_results[int(signal.id)] = membership_results
        classify, member = membership_results

        csi_plan = build_csi300_index_daily_query(entry_date, end_date)
        csi = self._collect_dataset(
            collector,
            stock_code,
            csi_plan.dataset,
            as_of=as_of,
            lease=lease,
            cancel_event=stop_signal,
            query_plan=csi_plan,
        )

        resolution, sw1_reason = self._membership_resolution(
            stock_code=stock_code,
            signal_session=signal_session,
            decision_known_at=signal_created_at,
            classify=classify,
            member=member,
        )
        sw_daily: Optional[DatasetCollectionResult] = None
        if resolution is not None and resolution.frozen:
            sw_plan = build_sw1_index_daily_query(
                resolution,
                entry_date,
                end_date,
            )
            sw_daily = self._collect_dataset(
                collector,
                stock_code,
                sw_plan.dataset,
                as_of=as_of,
                lease=lease,
                cancel_event=stop_signal,
                query_plan=sw_plan,
            )
            if not sw_daily.normalized_rows:
                sw1_reason = benchmark_dataset_unavailable_reason(
                    sw_daily.dataset,
                    sw_daily.status,
                    sw_daily.error_code,
                )

        all_results = [*base.values(), classify, member, csi]
        if sw_daily is not None:
            all_results.append(sw_daily)
        hashes = tuple(sorted({
            digest
            for result in all_results
            for digest in self._result_hashes(result)
        }))
        critical_reason = next(
            (
                reason
                for name, result in base.items()
                if (reason := self._critical_failure_reason(name, result))
            ),
            None,
        )
        csi_reason = (
            benchmark_dataset_unavailable_reason(
                csi.dataset,
                csi.status,
                csi.error_code,
            )
            if not csi.normalized_rows
            else None
        )
        return DecisionOutcomeV2DatasetBundle(
            xshg_sessions=tuple(sessions),
            stock_bars=self._stock_bars(
                base["daily"].normalized_rows,
                base["adj_factor"].normalized_rows,
            ),
            suspend_rows=tuple(base["suspend_d"].normalized_rows),
            stk_limit_rows=tuple(base["stk_limit"].normalized_rows),
            csi300_bars=tuple(csi.normalized_rows),
            sw1_membership=(
                resolution.to_evaluator_mapping()
                if resolution is not None
                else None
            ),
            sw1_bars=(
                tuple(sw_daily.normalized_rows) if sw_daily is not None else ()
            ),
            dataset_hashes=hashes,
            evaluation_as_of=as_of,
            critical_reason=critical_reason,
            csi300_reason=csi_reason,
            sw1_reason=sw1_reason,
        )

    def _evaluate(
        self,
        candidate: DecisionOutcomeV2Candidate,
        bundle: DecisionOutcomeV2DatasetBundle,
    ) -> DecisionOutcomeV2Evaluation:
        signal = candidate.signal
        signal_session = _aware_utc(signal.created_at).astimezone(_SHANGHAI).date()
        family = normalize_final_action_family(
            candidate.policy_evaluation.final_account_action
        )
        if bundle.critical_reason is not None:
            return self._unable_evaluation(
                horizon=candidate.horizon,
                family=family,
                signal_session=signal_session,
                dataset_hashes=bundle.dataset_hashes,
                reason=bundle.critical_reason,
                csi300_reason=bundle.csi300_reason or "not_evaluated",
                sw1_reason=bundle.sw1_reason or "not_evaluated",
            )
        evaluation = DecisionOutcomeV2Evaluator.evaluate(
            final_action_family=family,
            horizon=candidate.horizon,
            signal_session=signal_session,
            xshg_sessions=bundle.xshg_sessions,
            stock_bars=bundle.stock_bars,
            suspend_rows=bundle.suspend_rows,
            stk_limit_rows=bundle.stk_limit_rows,
            csi300_bars=bundle.csi300_bars,
            csi300_unavailable_reason=bundle.csi300_reason,
            sw1_membership=bundle.sw1_membership,
            sw1_bars=bundle.sw1_bars,
            sw1_unavailable_reason=bundle.sw1_reason,
            stock_code=signal.stock_code,
            dataset_hashes=bundle.dataset_hashes,
            engine_version=DECISION_OUTCOME_V2_ENGINE_VERSION,
            evaluation_as_of=bundle.evaluation_as_of,
        )
        if evaluation.csi300.status == "unavailable" and bundle.csi300_reason:
            evaluation = replace(
                evaluation,
                csi300=BenchmarkOutcomeV2(
                    code=CSI300_INDEX_CODE,
                    name=CSI300_INDEX_NAME,
                    status="unavailable",
                    reason=bundle.csi300_reason,
                ),
            )
        if evaluation.sw1.status == "unavailable" and bundle.sw1_reason:
            evaluation = replace(
                evaluation,
                sw1=BenchmarkOutcomeV2(
                    code=evaluation.sw1.code,
                    name=evaluation.sw1.name,
                    status="unavailable",
                    reason=bundle.sw1_reason,
                ),
            )
        return evaluation

    @staticmethod
    def _unable_evaluation(
        *,
        horizon: str,
        family: str,
        signal_session: date,
        dataset_hashes: Sequence[str],
        reason: str,
        csi300_reason: str,
        sw1_reason: str,
    ) -> DecisionOutcomeV2Evaluation:
        return DecisionOutcomeV2Evaluation(
            horizon=horizon,
            engine_version=DECISION_OUTCOME_V2_ENGINE_VERSION,
            final_action_family=family,
            eval_status="unable",
            reason=reason[:128],
            signal_session=signal_session,
            dataset_hashes=tuple(sorted(set(dataset_hashes))),
            csi300=BenchmarkOutcomeV2(
                code=CSI300_INDEX_CODE,
                name=CSI300_INDEX_NAME,
                status="unavailable",
                reason=csi300_reason[:128],
            ),
            sw1=BenchmarkOutcomeV2(
                code=None,
                name=None,
                status="unavailable",
                reason=sw1_reason[:128],
            ),
        )

    def _membership_resolution(
        self,
        *,
        stock_code: str,
        signal_session: date,
        decision_known_at: datetime,
        classify: DatasetCollectionResult,
        member: DatasetCollectionResult,
    ) -> tuple[Any, Optional[str]]:
        classify_hash = self._primary_hash(classify)
        member_hash = self._primary_hash(member)
        for result in (classify, member):
            if result.status not in {"available", "partial", "stale"}:
                return None, benchmark_dataset_unavailable_reason(
                    result.dataset,
                    result.status,
                    result.error_code,
                )
        if classify_hash is None or member_hash is None:
            return None, "missing_sw1_membership_snapshot"
        resolution = resolve_sw1_membership(
            stock_code=stock_code,
            decision_date=signal_session,
            decision_known_at=decision_known_at,
            member_rows=member.normalized_rows,
            classify_rows=classify.normalized_rows,
            member_known_at=member.available_at,
            classify_known_at=classify.available_at,
            member_snapshot_hash=member_hash,
            classify_snapshot_hash=classify_hash,
        )
        return resolution, resolution.reason

    @classmethod
    def _membership_capture_reason(
        cls,
        classify: DatasetCollectionResult,
        member: DatasetCollectionResult,
    ) -> Optional[str]:
        for result in (classify, member):
            if result.status not in {"available", "partial", "stale"}:
                return benchmark_dataset_unavailable_reason(
                    result.dataset,
                    result.status,
                    result.error_code,
                )
            if not result.normalized_rows:
                return f"{result.dataset}_empty"
            if cls._primary_hash(result) is None:
                return f"{result.dataset}_missing_snapshot"
        return None

    def _get_collector(self) -> ResearchDatasetCollector:
        if self._collector is not None:
            return self._collector
        if self.db is None:
            raise RuntimeError("Decision Outcome v2 collector requires a database")
        token = str(getattr(self.config, "tushare_token", "") or "").strip()
        if not token:
            raise ValueError("TUSHARE_RESEARCH_ENABLED requires TUSHARE_TOKEN")
        provider = TushareProvider(
            token=token,
            api_url=(
                resolve_tushare_api_url_from_env(
                    default=DEFAULT_TUSHARE_HTTP_URL
                )
                or DEFAULT_TUSHARE_HTTP_URL
            ),
            enforce_limits=True,
            worker_only=True,
            global_calls_per_minute=int(
                getattr(self.config, "tushare_global_calls_per_minute", 450)
            ),
            max_inflight=int(getattr(self.config, "tushare_max_inflight", 2)),
            endpoint_limits=dict(
                getattr(self.config, "tushare_endpoint_limits", {}) or {}
            ),
            worker_context_checker=lambda: self._context_getter() is not None,
        )
        database_path = Path(
            str(getattr(self.config, "database_path", "./data/stock_analysis.db"))
        )
        raw_store = RawArtifactStore(
            database_path.expanduser().resolve(strict=False).parent
            / "research"
            / "raw"
        )
        self._collector = ResearchDatasetCollector(
            provider,
            ResearchSnapshotRepository(self.db),
            raw_store,
        )
        return self._collector

    @staticmethod
    def _collect_dataset(
        collector: ResearchDatasetCollector,
        stock_code: str,
        dataset: str,
        **kwargs: Any,
    ) -> DatasetCollectionResult:
        result = collector.collect_dataset(stock_code, dataset, **kwargs)
        if not isinstance(result, DatasetCollectionResult):
            raise TypeError("collector returned an invalid Dataset result")
        return result

    @staticmethod
    def _stock_bars(
        daily_rows: Sequence[Mapping[str, Any]],
        factor_rows: Sequence[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], ...]:
        factors: dict[date, Any] = {}
        for row in factor_rows:
            trade_date = parse_tushare_date(row.get("trade_date"))
            if trade_date is not None:
                factors[trade_date] = row.get("adj_factor")
        bars: list[Mapping[str, Any]] = []
        for raw in daily_rows:
            trade_date = parse_tushare_date(raw.get("trade_date"))
            row = dict(raw)
            if trade_date is not None and trade_date in factors:
                row["adj_factor"] = factors[trade_date]
            bars.append(row)
        return tuple(bars)

    @staticmethod
    def _result_hashes(result: DatasetCollectionResult) -> tuple[str, ...]:
        hashes = set(result.source_snapshot_hashes)
        if result.snapshot is not None:
            hashes.add(result.snapshot.content_hash)
        return tuple(sorted(hashes))

    @classmethod
    def _primary_hash(cls, result: DatasetCollectionResult) -> Optional[str]:
        hashes = cls._result_hashes(result)
        return hashes[-1] if hashes else None

    @staticmethod
    def _critical_failure_reason(
        dataset: str,
        result: DatasetCollectionResult,
    ) -> Optional[str]:
        if result.error_code == "response_schema_error":
            return f"{dataset}_schema_drift"
        if result.status in {"permission_denied", "not_supported"}:
            return f"{dataset}_{result.status}"
        if result.status == "fetch_failed" and not result.retryable:
            return f"{dataset}_fetch_failed"
        return None

    @staticmethod
    def _lease(context: Any) -> LeaseFence:
        return LeaseFence(
            job_id=str(context.job_id).strip(),
            worker_id=str(context.worker_id).strip(),
            lease_token=str(context.lease_token).strip(),
        )

    @staticmethod
    def _stop_signal(context: Any) -> _StopSignal:
        signals = tuple(
            signal
            for signal in (
                getattr(context, "cancel_requested", None),
                getattr(context, "lease_lost", None),
            )
            if signal is not None and callable(getattr(signal, "is_set", None))
        )
        return _StopSignal(signals)

    def _require_durable_context(self) -> Any:
        context = self._context_getter()
        if context is None:
            raise RuntimeError(
                "Decision Outcome v2 evaluation requires a durable Worker context"
            )
        for field in ("job_id", "worker_id", "lease_token"):
            if not str(getattr(context, field, "") or "").strip():
                raise RuntimeError("durable Worker context is incomplete")
        context.checkpoint()
        return context

    def _assert_enabled(self) -> None:
        required = {
            "DURABLE_JOBS_ENABLED": getattr(
                self.config,
                "durable_jobs_enabled",
                False,
            ),
            "PERSONAL_RESEARCH_ENABLED": getattr(
                self.config,
                "personal_research_enabled",
                False,
            ),
            "TUSHARE_RESEARCH_ENABLED": getattr(
                self.config,
                "tushare_research_enabled",
                False,
            ),
            "RESEARCH_FACTORS_ENABLED": getattr(
                self.config,
                "research_factors_enabled",
                False,
            ),
            "DECISION_OUTCOME_V2_ENABLED": getattr(
                self.config,
                "decision_outcome_v2_enabled",
                False,
            ),
        }
        missing = sorted(name for name, value in required.items() if value is not True)
        if missing:
            raise RuntimeError(
                "Decision Outcome v2 runtime is disabled: " + ",".join(missing)
            )

    @staticmethod
    def _normalize_horizons(values: Optional[Sequence[str]]) -> tuple[str, ...]:
        if values is None:
            return tuple(DECISION_OUTCOME_V2_HORIZONS)
        if isinstance(values, (str, bytes, bytearray)):
            raise TypeError("horizons must be an array")
        normalized = tuple(str(item).strip().lower() for item in values)
        if not normalized or len(normalized) != len(set(normalized)):
            raise ValueError("horizons must be non-empty and unique")
        if any(item not in DECISION_OUTCOME_V2_HORIZONS for item in normalized):
            raise ValueError("horizons must contain only 5d, 10d, and 20d")
        return normalized


__all__ = [
    "DecisionOutcomeV2DatasetBundle",
    "DecisionOutcomeV2Service",
    "Sw1PointInTimeSnapshotCapture",
]
