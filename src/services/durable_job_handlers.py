"""Typed durable-job payloads, execution context, and built-in handlers.

Only versioned Pydantic payloads are registered.  Heavy analysis modules are
imported inside handlers so the dedicated worker can perform its migration and
feature-flag preflight without importing provider, LLM, or notification code.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from enum import Enum
from types import SimpleNamespace
from typing import Annotated, Any, Dict, Iterator, List, Literal, Mapping, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_serializer,
    model_validator,
)

from src.services.durable_jobs import (
    JOB_STATUS_CANCEL_REQUESTED,
    ClaimedJob,
    DurableJobHandlerRegistry,
    DurableJobStore,
    InvalidJobPayloadError,
    StaleLeaseError,
)
from src.services.run_diagnostics import update_current_diagnostic_stage
from src.utils.market_review_region import normalize_market_review_region_strict


logger = logging.getLogger(__name__)


class DurableJobCancelled(RuntimeError):
    """Raised at a safe boundary after durable cancellation was requested."""


class DurableHandlerUnavailableError(RuntimeError):
    """Raised by an explicitly registered handler that is not implemented yet."""


class DurableHandlerExecutionError(RuntimeError):
    """Raised when a legacy service reports failure without raising an error."""


class DurableHandlerBusyError(RuntimeError):
    """Raised when another owner holds a shared execution resource."""

    def __init__(self, message: str, *, retry_after: float = 200.0) -> None:
        super().__init__(message)
        self.retry_after = retry_after


@dataclass(frozen=True)
class DurableExecutionContext:
    """Lease-fenced runtime context exposed to the currently executing handler."""

    store: DurableJobStore
    claimed_job: ClaimedJob
    query_id: str
    trace_id: str
    cancel_requested: Any
    lease_lost: Any

    @property
    def job_id(self) -> str:
        return self.claimed_job.task_id

    @property
    def worker_id(self) -> str:
        return self.claimed_job.worker_id

    @property
    def lease_token(self) -> str:
        return self.claimed_job.lease_token

    def _raise_if_stopped(self) -> None:
        if self.lease_lost.is_set():
            raise StaleLeaseError(f"job {self.job_id!r} lease is stale")
        if self.cancel_requested.is_set():
            raise DurableJobCancelled(f"job {self.job_id!r} cancellation requested")

    def checkpoint(self) -> None:
        """Heartbeat and stop at a cancellation-safe boundary."""

        self._raise_if_stopped()
        result = self.store.heartbeat(self.job_id, self.worker_id, self.lease_token)
        if result.cancel_requested:
            self.cancel_requested.set()
        self._raise_if_stopped()

    def progress(
        self,
        progress: int,
        message: Optional[str] = None,
        *,
        stage: Optional[str] = None,
        detail: Any = None,
    ) -> None:
        """Persist progress under the live lease and then honor cancellation."""

        self._raise_if_stopped()
        status = self.store.update_progress(
            self.job_id,
            self.worker_id,
            self.lease_token,
            stage=stage,
            progress=progress,
            message=message,
            event_payload=_json_ready(detail) if detail is not None else None,
        )
        if stage is not None:
            update_current_diagnostic_stage(stage)
        if status == JOB_STATUS_CANCEL_REQUESTED:
            self.cancel_requested.set()
        self._raise_if_stopped()

    def stage(
        self,
        stage: str,
        *,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        detail: Any = None,
    ) -> None:
        """Persist a named execution stage under the live lease."""

        self._raise_if_stopped()
        status = self.store.update_progress(
            self.job_id,
            self.worker_id,
            self.lease_token,
            stage=stage,
            progress=progress,
            message=message,
            event_payload=_json_ready(detail) if detail is not None else None,
        )
        update_current_diagnostic_stage(stage)
        if status == JOB_STATUS_CANCEL_REQUESTED:
            self.cancel_requested.set()
        self._raise_if_stopped()

    def flow(self, event: Mapping[str, Any]) -> None:
        """Append one lease-fenced task-flow event."""

        self._raise_if_stopped()
        try:
            self.store.append_flow_event(
                self.job_id,
                self.worker_id,
                self.lease_token,
                _json_ready(dict(event)),
            )
        except StaleLeaseError:
            self.lease_lost.set()
            raise
        self._raise_if_stopped()


_CURRENT_DURABLE_EXECUTION: ContextVar[Optional[DurableExecutionContext]] = ContextVar(
    "dsa_current_durable_execution",
    default=None,
)


def get_durable_execution_context() -> DurableExecutionContext:
    """Return the current worker context or fail outside a durable handler."""

    context = _CURRENT_DURABLE_EXECUTION.get()
    if context is None:
        raise RuntimeError("no durable job execution context is active")
    return context


def get_optional_durable_execution_context() -> Optional[DurableExecutionContext]:
    """Return the current worker context, or ``None`` outside durable execution."""

    return _CURRENT_DURABLE_EXECUTION.get()


@contextmanager
def bind_durable_execution_context(context: DurableExecutionContext) -> Iterator[None]:
    """Bind one execution context to the current handler thread."""

    token = _CURRENT_DURABLE_EXECUTION.set(context)
    try:
        yield
    finally:
        _CURRENT_DURABLE_EXECUTION.reset(token)


class _StrictPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, strict=True)


class BotTargetPayload(_StrictPayload):
    """Only the non-secret routing fields allowed to survive bot process exit."""

    platform: str = Field(min_length=1, max_length=32)
    chat_id: str = Field(min_length=1, max_length=256)
    message_id: str = Field(min_length=1, max_length=256)

    @field_validator("platform")
    @classmethod
    def _normalize_platform(cls, value: str) -> str:
        return value.lower()


class StockAnalysisPayload(_StrictPayload):
    stock_code: str = Field(min_length=1, max_length=32)
    stock_name: Optional[str] = Field(None, max_length=128)
    original_query: Optional[str] = Field(None, max_length=2048)
    selection_source: Optional[str] = Field(None, max_length=64)
    report_type: Literal["simple", "detailed", "full", "brief"] = "detailed"
    force_refresh: bool = False
    notify: bool = True
    skills: List[str] = Field(default_factory=list, max_length=32)
    analysis_phase: Literal["auto", "premarket", "intraday", "postmarket"] = "auto"
    query_source: str = Field("durable_worker", min_length=1, max_length=64)
    portfolio_context: Optional[Dict[str, Any]] = None
    report_language: Optional[str] = Field(None, max_length=16)
    bot_target: Optional[BotTargetPayload] = None

    @model_serializer(mode="wrap")
    def _serialize_without_empty_bot_target(self, handler: Any) -> Dict[str, Any]:
        data = handler(self)
        if self.bot_target is None:
            data.pop("bot_target", None)
        return data

    @field_validator("skills")
    @classmethod
    def _validate_skills(cls, value: List[str]) -> List[str]:
        cleaned = [item.strip() for item in value]
        if any(not item or len(item) > 64 for item in cleaned):
            raise ValueError("skills must contain non-empty names of at most 64 characters")
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("skills must not contain duplicates")
        return cleaned


class PersonalResearchPayload(_StrictPayload):
    """One stock-scoped personal research run with an explicit budget mode."""

    stock_code: str = Field(min_length=1, max_length=32)
    requested_mode: Literal["auto", "quick", "standard", "deep", "debate"] = "auto"
    priority: int = Field(50, ge=0, le=100)
    manual_daily_override: bool = False
    report_type: Optional[Literal["brief", "detailed", "full"]] = None
    notify: bool = True
    query_source: str = Field(
        "personal_research_api", min_length=1, max_length=64
    )
    policy_account_id: Optional[int] = Field(None, gt=0)
    target_weight_pct: Optional[float] = Field(
        None,
        ge=0.0,
        le=100.0,
        allow_inf_nan=False,
    )
    report_language: Optional[str] = Field(None, max_length=16)

    @model_validator(mode="after")
    def validate_policy_request(self) -> "PersonalResearchPayload":
        if (self.policy_account_id is None) != (self.target_weight_pct is None):
            raise ValueError(
                "policy_account_id and target_weight_pct must be supplied together"
            )
        return self


class ScreeningScreenPayload(_StrictPayload):
    strategy: str = Field("dual_low", min_length=1, max_length=64)
    market: str = Field("cn", min_length=1, max_length=16)
    max_results: int = Field(20, ge=1, le=100)
    selection_seed: str = Field("", max_length=128)


class MarketReviewPayload(_StrictPayload):
    region: Optional[str] = Field(None, min_length=2, max_length=32)
    send_notification: bool = True
    merge_notification: bool = False
    save_report_file: bool = True
    persist_history: bool = True
    trigger_source: str = Field("durable_worker", min_length=1, max_length=64)
    apply_trading_day_filter: bool = False
    bot_target: Optional[BotTargetPayload] = None

    @model_serializer(mode="wrap")
    def _serialize_optional_bot_fields(self, handler: Any) -> Dict[str, Any]:
        data = handler(self)
        if self.bot_target is None:
            data.pop("bot_target", None)
        if not self.apply_trading_day_filter:
            data.pop("apply_trading_day_filter", None)
        return data

    @field_validator("region")
    @classmethod
    def _validate_region(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        tokens = [item.strip().lower() for item in value.split(",")]
        if len(tokens) != len(set(tokens)):
            raise ValueError("region must not contain duplicates")
        return normalize_market_review_region_strict(value)


class ScheduledAnalysisPayload(_StrictPayload):
    stock_codes: Optional[List[str]] = Field(None, max_length=500)
    workers: Optional[int] = Field(None, ge=1, le=64)
    no_notify: bool = False
    no_market_review: bool = False
    force_run: bool = False
    dry_run: bool = False
    single_notify: bool = False
    no_context_snapshot: bool = False
    portfolio: Optional[str] = Field(None, max_length=4096)
    bot_target: Optional[BotTargetPayload] = None

    @model_serializer(mode="wrap")
    def _serialize_without_empty_bot_target(self, handler: Any) -> Dict[str, Any]:
        data = handler(self)
        if self.bot_target is None:
            data.pop("bot_target", None)
        return data

    @field_validator("stock_codes")
    @classmethod
    def _validate_stock_codes(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return None
        cleaned = [item.strip().upper() for item in value]
        if any(not item or len(item) > 32 for item in cleaned):
            raise ValueError("stock_codes must contain non-empty values of at most 32 characters")
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("stock_codes must not contain duplicates")
        return cleaned


class _EventRuleBase(_StrictPayload):
    stock_code: str = Field(min_length=1, max_length=32)
    description: str = Field("", max_length=500)
    status: Literal["active", "triggered", "expired", "dismissed"] = "active"
    ttl_hours: float = Field(24.0, gt=0, le=8760)
    created_at: Optional[float] = Field(None, ge=0)


class PriceCrossRulePayload(_EventRuleBase):
    alert_type: Literal["price_cross"]
    direction: Literal["above", "below"] = "above"
    price: float = Field(gt=0)


class PriceChangeRulePayload(_EventRuleBase):
    alert_type: Literal["price_change_percent"]
    direction: Literal["up", "down"] = "up"
    change_pct: float = Field(gt=0)


class VolumeSpikeRulePayload(_EventRuleBase):
    alert_type: Literal["volume_spike"]
    multiplier: float = Field(2.0, gt=0)


EventRulePayload = Annotated[
    Union[PriceCrossRulePayload, PriceChangeRulePayload, VolumeSpikeRulePayload],
    Field(discriminator="alert_type"),
]


class EventMonitorPayload(_StrictPayload):
    rules: Optional[List[EventRulePayload]] = Field(None, max_length=1000)
    send_notification: bool = True


class DecisionSignalOutcomesPayload(_StrictPayload):
    signal_id: Optional[int] = Field(None, gt=0)
    horizons: Optional[List[str]] = Field(None, max_length=16)
    force: bool = False
    market: Optional[str] = Field(None, max_length=16)
    stock_code: Optional[str] = Field(None, max_length=32)
    action: Optional[str] = Field(None, max_length=32)
    source_type: Optional[str] = Field(None, max_length=64)
    status: Optional[str] = Field(None, max_length=32)
    limit: int = Field(100, ge=1, le=500)

    @field_validator("horizons")
    @classmethod
    def _validate_horizons(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return None
        cleaned = [item.strip() for item in value]
        if any(not item or len(item) > 16 for item in cleaned):
            raise ValueError("horizons must contain non-empty values of at most 16 characters")
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("horizons must not contain duplicates")
        return cleaned


class DecisionOutcomesV2Payload(_StrictPayload):
    """Immutable personal-research Outcome v2 candidate selection."""

    signal_id: Optional[int] = Field(None, gt=0)
    horizons: List[Literal["5d", "10d", "20d"]] = Field(
        default_factory=lambda: ["5d", "10d", "20d"],
        min_length=1,
        max_length=3,
    )
    stock_code: Optional[str] = Field(None, min_length=1, max_length=16)
    decision_profile: Optional[str] = Field(None, min_length=1, max_length=16)
    limit: int = Field(100, ge=1, le=500)
    notify: bool = False

    @field_validator("horizons")
    @classmethod
    def _validate_v2_horizons(
        cls,
        value: List[Literal["5d", "10d", "20d"]],
    ) -> List[Literal["5d", "10d", "20d"]]:
        if len(value) != len(set(value)):
            raise ValueError("horizons must not contain duplicates")
        return value


class BotAskPayload(_StrictPayload):
    target: BotTargetPayload
    stock_codes: List[str] = Field(min_length=1, max_length=5)
    skill_id: Optional[str] = Field(None, max_length=64)
    skill_text: str = Field("", max_length=2048)

    @field_validator("stock_codes")
    @classmethod
    def _validate_stock_codes(cls, value: List[str]) -> List[str]:
        cleaned = [item.strip().upper() for item in value]
        if any(not item or len(item) > 32 for item in cleaned):
            raise ValueError("stock_codes must contain non-empty values of at most 32 characters")
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("stock_codes must not contain duplicates")
        return cleaned


class BotResearchPayload(_StrictPayload):
    target: BotTargetPayload
    stock_code: Optional[str] = Field(None, max_length=32)
    question: str = Field(min_length=1, max_length=4096)


def _stock_analysis_handler(payload: StockAnalysisPayload) -> Dict[str, Any]:
    context = get_durable_execution_context()
    context.stage("stock_analysis", progress=5, message="Stock analysis started")

    from src.services.analysis_service import AnalysisService

    service = AnalysisService()

    def report_progress(progress: int, message: str) -> None:
        context.progress(progress, message, stage="stock_analysis")

    analyze_kwargs: Dict[str, Any] = dict(
        stock_code=payload.stock_code,
        report_type=payload.report_type,
        force_refresh=payload.force_refresh,
        query_id=context.query_id,
        trace_id=context.trace_id,
        send_notification=payload.notify,
        progress_callback=report_progress,
        skills=payload.skills or None,
        analysis_phase=payload.analysis_phase,
        query_source=payload.query_source,
        portfolio_context=payload.portfolio_context,
        report_language=payload.report_language,
    )
    if payload.bot_target is not None:
        from bot.durable import bot_message_from_target

        analyze_kwargs["source_message"] = bot_message_from_target(payload.bot_target)
    result = service.analyze_stock(**analyze_kwargs)
    context.checkpoint()
    if result is None:
        raise DurableHandlerExecutionError(service.last_error or "Stock analysis returned no result")
    return _json_ready(result)


def _capture_personal_research_sw1_snapshots(
    payload: PersonalResearchPayload,
    context: DurableExecutionContext,
) -> Optional[Dict[str, Any]]:
    """Best-effort optional SW1 freeze before the formal signal boundary."""

    from src.config import get_config

    config = get_config()
    if not bool(getattr(config, "decision_outcome_v2_enabled", False)):
        return None

    from src.services.decision_outcome_v2_service import DecisionOutcomeV2Service

    try:
        capture = DecisionOutcomeV2Service(config=config).capture_sw1_membership_snapshots(
            payload.stock_code
        )
    except (StaleLeaseError, DurableJobCancelled):
        raise
    except Exception as exc:
        # SW1 is an optional benchmark. Preserve the base personal-research
        # result while recording a typed, non-secret reason. Cancellation and
        # lease loss are re-checked and can never be swallowed by this path.
        context.checkpoint()
        reason = f"sw1_snapshot_capture_failed_{type(exc).__name__}"
        logger.warning(
            "Optional personal-research SW1 snapshot capture failed (%s)",
            type(exc).__name__,
        )
        context.progress(
            8,
            f"Optional SW1 snapshot unavailable: {reason}",
            stage="personal_research",
            detail={"status": "unavailable", "reason": reason},
        )
        return {"status": "unavailable", "reason": reason}

    summary = capture.to_dict()
    if capture.status == "available":
        message = "Point-in-time SW1 snapshots frozen"
    else:
        message = f"Optional SW1 snapshot unavailable: {capture.reason}"
        logger.warning(
            "Optional personal-research SW1 snapshot unavailable (%s)",
            capture.reason,
        )
    context.progress(
        8,
        message,
        stage="personal_research",
        detail=summary,
    )
    return summary


def _personal_research_handler(payload: PersonalResearchPayload) -> Dict[str, Any]:
    context = get_durable_execution_context()
    context.stage(
        "personal_research",
        progress=5,
        message="Personal research started",
    )

    from src.services.analysis_service import AnalysisService
    from src.services.research_budget_service import ResearchBudgetService

    _capture_personal_research_sw1_snapshots(payload, context)
    resolved_mode = ResearchBudgetService.resolve_mode(
        payload.requested_mode,
        priority=payload.priority,
    )
    report_type = payload.report_type or {
        "quick": "brief",
        "standard": "detailed",
        "deep": "full",
        "debate": "full",
    }[resolved_mode]
    service = AnalysisService()

    def report_progress(progress: int, message: str) -> None:
        context.progress(progress, message, stage="personal_research")

    result = service.analyze_stock(
        stock_code=payload.stock_code,
        report_type=report_type,
        query_id=context.query_id,
        trace_id=context.trace_id,
        send_notification=payload.notify,
        progress_callback=report_progress,
        query_source=payload.query_source,
        policy_account_id=payload.policy_account_id,
        policy_target_weight_pct=payload.target_weight_pct,
        report_language=payload.report_language,
        research_mode=payload.requested_mode,
        research_priority=payload.priority,
        research_manual_daily_override=payload.manual_daily_override,
    )
    context.checkpoint()
    if result is None:
        raise DurableHandlerExecutionError(
            service.last_error or "Personal research returned no result"
        )
    return _json_ready(
        {
            **result,
            "research_mode": resolved_mode,
            "research_priority": payload.priority,
        }
    )


def _screening_screen_handler(payload: ScreeningScreenPayload) -> Dict[str, Any]:
    context = get_durable_execution_context()
    context.stage("screening", progress=10, message="Screening started")

    from src.config import get_config
    from src.services.screening_service import ScreeningService
    from src.storage import DatabaseManager

    service = ScreeningService(config=get_config(), db_manager=DatabaseManager.get_instance())

    def report_progress(progress: int, message: str) -> None:
        context.progress(progress, message, stage="screening")

    result = service.screen(
        strategy=payload.strategy,
        market=payload.market,
        max_results=payload.max_results,
        selection_seed=payload.selection_seed,
        run_id=context.job_id,
        progress_callback=report_progress,
    )
    context.checkpoint()
    return _json_ready(result)


def _market_review_handler(payload: MarketReviewPayload) -> Dict[str, Any]:
    context = get_durable_execution_context()
    context.stage("market_review", progress=10, message="Market review started")

    from src.config import get_config
    from src.core.market_review import run_market_review
    from src.core.market_review_runtime import build_market_review_runtime

    config = get_config()
    source_message = None
    if payload.bot_target is not None:
        from bot.durable import bot_message_from_target

        source_message = bot_message_from_target(payload.bot_target)

    effective_region = payload.region
    if payload.apply_trading_day_filter:
        from src.core.trading_calendar import (
            compute_effective_region,
            get_open_markets_today,
        )

        effective_region = compute_effective_region(
            payload.region or getattr(config, "market_review_region", "cn") or "cn",
            get_open_markets_today(),
        )
        if effective_region == "":
            dispatch_status = "notification_disabled"
            if payload.send_notification:
                from src.notification import NotificationService

                dispatch = NotificationService(source_message=source_message).send_with_results(
                    "🎯 大盘复盘\n\n今日相关市场休市，已跳过大盘复盘。",
                    email_send_to_all=True,
                    route_type="report",
                    dedup_key=f"bot-market-closed:{context.job_id}",
                )
                dispatch_status = dispatch.status
            context.checkpoint()
            return {
                "success": True,
                "skipped": True,
                "reason": "markets_closed",
                "notification_status": dispatch_status,
            }

    def run_with_runtime(**kwargs: Any) -> Any:
        if source_message is None:
            notifier, analyzer, search_service = build_market_review_runtime(config)
        else:
            notifier, analyzer, search_service = build_market_review_runtime(
                config,
                source_message=source_message,
            )
        return run_market_review(
            notifier=notifier,
            analyzer=analyzer,
            search_service=search_service,
            **kwargs,
        )

    result = _run_market_review_locked(
        config,
        run_with_runtime,
        send_notification=payload.send_notification,
        merge_notification=payload.merge_notification,
        override_region=effective_region,
        query_id=context.query_id,
        return_structured=True,
        save_report_file=payload.save_report_file,
        persist_history=payload.persist_history,
        trigger_source=payload.trigger_source,
        notification_dedup_key=f"market-review:{context.job_id}",
    )
    context.checkpoint()
    if result is None:
        raise DurableHandlerExecutionError("Market review returned no result")
    return _json_ready(result)


def _run_market_review_locked(config: Any, runner: Any, **kwargs: Any) -> Any:
    """Run market review under the shared API/CLI/worker owner lock."""

    from src.core.market_review_lock import (
        release_market_review_lock,
        try_acquire_market_review_lock,
    )

    lock_token = try_acquire_market_review_lock(config)
    if lock_token is None:
        raise DurableHandlerBusyError("Another market review owner is still running.")
    try:
        params = dict(kwargs)
        params.setdefault("config", config)
        return runner(**params)
    finally:
        release_market_review_lock(lock_token)


def _scheduled_analysis_handler(payload: ScheduledAnalysisPayload) -> Dict[str, Any]:
    context = get_durable_execution_context()
    context.stage("scheduled_analysis", progress=5, message="Scheduled analysis started")

    if payload.bot_target is not None:
        from bot.durable import bot_message_from_target
        from src.config import get_config
        from src.core.pipeline import StockAnalysisPipeline

        source_message = bot_message_from_target(payload.bot_target)
        pipeline = StockAnalysisPipeline(
            config=get_config(),
            max_workers=payload.workers,
            source_message=source_message,
            query_id=context.query_id,
            trace_id=context.trace_id,
            query_source="bot",
            save_context_snapshot=not payload.no_context_snapshot,
        )
        results = pipeline.run(
            stock_codes=payload.stock_codes,
            dry_run=payload.dry_run,
            send_notification=not payload.no_notify,
            merge_notification=False,
        )
        context.checkpoint()
        return {
            "success": True,
            "query_id": context.query_id,
            "job_id": context.job_id,
            "analyzed_count": len(results),
        }

    from main import run_scheduled_analysis
    from src.config import get_config

    args = SimpleNamespace(
        schedule=True,
        no_run_immediately=True,
        no_notify=payload.no_notify,
        no_market_review=payload.no_market_review,
        dry_run=payload.dry_run,
        force_run=payload.force_run,
        single_notify=payload.single_notify,
        no_context_snapshot=payload.no_context_snapshot,
        market_review=False,
        serve=False,
        serve_only=True,
        stocks=None,
        portfolio=payload.portfolio,
        workers=payload.workers,
    )
    result = run_scheduled_analysis(get_config(), args, payload.stock_codes)
    context.checkpoint()
    if result is not True:
        raise DurableHandlerExecutionError("Scheduled analysis did not complete successfully")
    return {"success": True, "query_id": context.query_id, "job_id": context.job_id}


def _event_monitor_handler(payload: EventMonitorPayload) -> Dict[str, Any]:
    context = get_durable_execution_context()
    context.stage("event_monitor", progress=10, message="Event monitor cycle started")

    from src.agent.events import (
        EventMonitor,
        parse_event_alert_rules,
        run_event_monitor_once,
    )

    if payload.rules is not None:
        rule_payloads = [rule.model_dump(mode="json") for rule in payload.rules]
    else:
        from src.config import get_config

        config = get_config()
        rule_payloads = parse_event_alert_rules(
            getattr(config, "agent_event_alert_rules_json", "")
        )
    monitor = EventMonitor.from_dict_list(rule_payloads)
    notification_errors: List[Exception] = []

    if payload.send_notification and monitor is not None:
        from src.notification import NotificationBuilder, NotificationService

        notification_service = NotificationService()

        def notify(triggered: Any) -> None:
            title = f"Event Alert | {triggered.rule.stock_code}"
            content = triggered.message or triggered.rule.description or "Alert triggered"
            alert_text = NotificationBuilder.build_simple_alert(
                title=title,
                content=content,
                alert_type="warning",
            )
            rule_identity = {
                key: _json_ready(value)
                for key, value in vars(triggered.rule).items()
                if key not in {"created_at", "triggered_at", "status"}
            }
            digest = hashlib.sha256(
                json.dumps(
                    rule_identity,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()[:24]
            dedup_key = f"event-monitor:{digest}"
            try:
                notification_service.send(
                    alert_text,
                    route_type="alert",
                    dedup_key=dedup_key,
                    cooldown_key=dedup_key,
                )
            except Exception as exc:
                # EventMonitor preserves legacy best-effort callbacks.  The
                # durable handler must surface a missing Outbox write instead.
                notification_errors.append(exc)
                raise

        monitor.on_trigger(notify)

    context.checkpoint()
    if monitor is None:
        return {"triggered": [], "triggered_count": 0, "monitor_configured": False}
    triggered = run_event_monitor_once(monitor)
    if notification_errors:
        raise notification_errors[0]
    context.checkpoint()
    rows = _json_ready(triggered)
    return {"triggered": rows, "triggered_count": len(rows), "monitor_configured": True}


def _decision_signal_outcomes_handler(payload: DecisionSignalOutcomesPayload) -> Dict[str, Any]:
    context = get_durable_execution_context()
    context.stage("decision_signal_outcomes", progress=10, message="Outcome evaluation started")

    from src.services.decision_signal_outcome_service import DecisionSignalOutcomeService

    result = DecisionSignalOutcomeService().run_outcomes(
        signal_id=payload.signal_id,
        horizons=payload.horizons,
        force=payload.force,
        market=payload.market,
        stock_code=payload.stock_code,
        action=payload.action,
        source_type=payload.source_type,
        status=payload.status,
        limit=payload.limit,
    )
    context.checkpoint()
    return _json_ready(result)


def _decision_outcomes_v2_handler(payload: DecisionOutcomesV2Payload) -> Dict[str, Any]:
    context = get_durable_execution_context()
    context.stage(
        "decision_outcomes_v2",
        progress=5,
        message="Decision Outcome v2 evaluation started",
    )

    from src.services.decision_outcome_v2_service import DecisionOutcomeV2Service

    result = DecisionOutcomeV2Service().run_outcomes(
        signal_id=payload.signal_id,
        horizons=payload.horizons,
        stock_code=payload.stock_code,
        decision_profile=payload.decision_profile,
        limit=payload.limit,
    )
    context.checkpoint()
    notification_status = "notification_disabled"
    if payload.notify:
        try:
            from src.notification import NotificationService

            dispatch = NotificationService().send_with_results(
                _decision_outcomes_v2_summary(result, job_id=context.job_id),
                route_type="report",
                dedup_key=f"decision-outcome-v2:{context.job_id}",
            )
            notification_status = dispatch.status
        except Exception as exc:
            notification_status = "failed"
            logger.warning(
                "Decision Outcome v2 summary notification failed (%s)",
                type(exc).__name__,
            )
    context.checkpoint()
    return _json_ready({**result, "notification_status": notification_status})


def _decision_outcomes_v2_summary(
    result: Mapping[str, Any],
    *,
    job_id: str,
) -> str:
    status_order = (
        "pending",
        "evaluated",
        "observational",
        "unexecutable",
        "unable",
    )
    status_counts = {status: 0 for status in status_order}
    for item in result.get("items", ()):
        if not isinstance(item, Mapping):
            continue
        status = str(item.get("eval_status") or "").strip().lower()
        if status in status_counts:
            status_counts[status] += 1
    counts = ", ".join(
        f"{status}={status_counts[status]}" for status in status_order
    )
    return "\n".join(
        (
            "# Decision Outcome v2 job summary",
            "",
            f"- Job: `{job_id}`",
            f"- Selected: {int(result.get('selected', 0) or 0)}",
            f"- Created: {int(result.get('created', 0) or 0)}",
            f"- Transitioned: {int(result.get('transitioned', 0) or 0)}",
            f"- Updated: {int(result.get('updated', 0) or 0)}",
            f"- Unchanged: {int(result.get('unchanged', 0) or 0)}",
            f"- Status counts: {counts}",
        )
    )


def _deliver_bot_response(
    target: BotTargetPayload,
    content: str,
    *,
    dedup_key: str,
) -> Dict[str, Any]:
    """Plan a final context reply through the durable notification outbox."""

    from bot.durable import bot_message_from_target
    from src.notification import NotificationService

    source_message = bot_message_from_target(target)
    dispatch = NotificationService(source_message=source_message).send_with_results(
        content,
        route_type="report",
        dedup_key=dedup_key,
    )
    return {
        "status": dispatch.status,
        "accepted": bool(dispatch.success),
    }


def _bot_ask_handler(payload: BotAskPayload) -> Dict[str, Any]:
    context = get_durable_execution_context()
    context.stage("bot_ask", progress=10, message="Bot ask started")

    from bot.commands.ask import AskCommand
    from bot.durable import bot_message_from_target
    from src.config import get_config

    config = get_config()
    command = AskCommand()
    source_message = bot_message_from_target(payload.target)
    if not getattr(config, "agent_mode", False):
        from bot.models import BotResponse

        response = BotResponse.text_response(
            "⚠️ Agent 模式未开启，无法使用问股功能。请在配置中设置 `AGENT_MODE=true`。"
        )
    else:
        response = command._execute_parsed(
            config,
            source_message,
            list(payload.stock_codes),
            payload.skill_id or "",
            payload.skill_text,
        )
    context.checkpoint()
    notification = _deliver_bot_response(
        payload.target,
        response.text,
        dedup_key=f"bot-ask-result:{context.job_id}",
    )
    return {
        "success": True,
        "response_markdown": bool(response.markdown),
        "notification": notification,
    }


def _bot_research_handler(payload: BotResearchPayload) -> Dict[str, Any]:
    context = get_durable_execution_context()
    context.stage("bot_research", progress=10, message="Bot research started")

    from bot.commands.research import ResearchCommand
    from src.config import get_config

    config = get_config()
    command = ResearchCommand()
    if not getattr(config, "agent_mode", False):
        from bot.models import BotResponse

        response = BotResponse.text_response(
            "⚠️ Agent 模式未开启，无法使用深度研究功能。请在配置中设置 `AGENT_MODE=true`。"
        )
    else:
        response = command._run_research(config, payload.stock_code, payload.question)
    context.checkpoint()
    notification = _deliver_bot_response(
        payload.target,
        response.text,
        dedup_key=f"bot-research-result:{context.job_id}",
    )
    return {
        "success": True,
        "response_markdown": bool(response.markdown),
        "notification": notification,
    }


def build_default_durable_job_registry() -> DurableJobHandlerRegistry:
    """Build the immutable v1 handler surface for a dedicated worker process."""

    registry = DurableJobHandlerRegistry()
    registry.register("stock_analysis", 1, StockAnalysisPayload, _stock_analysis_handler)
    registry.register(
        "personal_research",
        1,
        PersonalResearchPayload,
        _personal_research_handler,
    )
    registry.register("screening_screen", 1, ScreeningScreenPayload, _screening_screen_handler)
    registry.register("market_review", 1, MarketReviewPayload, _market_review_handler)
    registry.register("scheduled_analysis", 1, ScheduledAnalysisPayload, _scheduled_analysis_handler)
    registry.register("event_monitor", 1, EventMonitorPayload, _event_monitor_handler)
    registry.register(
        "decision_signal_outcomes",
        1,
        DecisionSignalOutcomesPayload,
        _decision_signal_outcomes_handler,
    )
    registry.register(
        "decision_outcomes_v2",
        1,
        DecisionOutcomesV2Payload,
        _decision_outcomes_v2_handler,
    )
    registry.register("bot_ask", 1, BotAskPayload, _bot_ask_handler)
    registry.register("bot_research", 1, BotResearchPayload, _bot_research_handler)
    return registry


def _json_ready(value: Any) -> Any:
    """Convert known result types to finite, strict JSON without repr fallbacks."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise InvalidJobPayloadError("handler result contains a non-finite number")
        return value
    if isinstance(value, Enum):
        return _json_ready(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, BaseModel):
        return _json_ready(value.model_dump(mode="json"))
    if is_dataclass(value) and not isinstance(value, type):
        return _json_ready(asdict(value))
    if isinstance(value, Mapping):
        result: Dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise InvalidJobPayloadError("handler result object keys must be strings")
            result[key] = _json_ready(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _json_ready(to_dict())
    raise InvalidJobPayloadError(
        f"handler result contains unsupported value {type(value).__name__}"
    )


__all__ = [
    "BotAskPayload",
    "BotResearchPayload",
    "BotTargetPayload",
    "DecisionSignalOutcomesPayload",
    "DecisionOutcomesV2Payload",
    "DurableExecutionContext",
    "DurableHandlerBusyError",
    "DurableHandlerExecutionError",
    "DurableHandlerUnavailableError",
    "DurableJobCancelled",
    "EventMonitorPayload",
    "MarketReviewPayload",
    "PersonalResearchPayload",
    "ScheduledAnalysisPayload",
    "ScreeningScreenPayload",
    "StockAnalysisPayload",
    "bind_durable_execution_context",
    "build_default_durable_job_registry",
    "get_durable_execution_context",
    "get_optional_durable_execution_context",
]
