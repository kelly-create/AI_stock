"""Submit one stock-scoped personal-research run to the durable Worker."""

from __future__ import annotations

import re
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Security
from fastapi.security import APIKeyCookie

from api.deps import get_config as get_config_dep, get_database_manager
from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.personal_research_runs import (
    PersonalResearchRunAccepted,
    PersonalResearchRunRequest,
)
from data_provider.base import normalize_stock_code
from src.auth import COOKIE_NAME
from src.config import Config
from src.core.trading_calendar import get_market_for_stock
from src.services.durable_job_handlers import build_default_durable_job_registry
from src.services.durable_jobs import (
    DurableJobConflictError,
    DurableJobStore,
    JobEnqueueRequest,
)
from src.services.research_budget_service import ResearchBudgetService
from src.storage import DatabaseManager


admin_session_cookie = APIKeyCookie(
    name=COOKIE_NAME,
    scheme_name="AdminSessionCookie",
    auto_error=False,
)
router = APIRouter(dependencies=[Security(admin_session_cookie)])

_IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")

AUTH_RESPONSE = {
    401: {
        "model": ErrorResponse,
        "description": "Authentication is required when admin auth is enabled.",
    }
}


def _required_flags(config: Config, *, requested_mode: str) -> None:
    required = {
        "DURABLE_JOBS_ENABLED": config.durable_jobs_enabled,
        "PERSONAL_RESEARCH_ENABLED": config.personal_research_enabled,
        "TUSHARE_RESEARCH_ENABLED": config.tushare_research_enabled,
        "RESEARCH_FACTORS_ENABLED": config.research_factors_enabled,
        "RESEARCH_EVIDENCE_ENABLED": config.research_evidence_enabled,
    }
    if requested_mode == "debate":
        required["RESEARCH_DEBATE_ENABLED"] = config.research_debate_enabled
    missing = sorted(name for name, enabled in required.items() if enabled is not True)
    if missing:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "personal_research_not_enabled",
                "message": "Required personal-research capabilities are disabled",
                "missing": missing,
            },
        )


@router.post(
    "/personal/runs",
    status_code=202,
    response_model=PersonalResearchRunAccepted,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Submit a durable personal-research run",
)
def create_personal_research_run(
    request: PersonalResearchRunRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    config: Config = Depends(get_config_dep),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> PersonalResearchRunAccepted:
    if _IDEMPOTENCY_RE.fullmatch(idempotency_key) is None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_idempotency_key",
                "message": (
                    "Idempotency-Key must be 8-128 public identifier characters"
                ),
            },
        )
    normalized_code = normalize_stock_code(request.stock_code)
    if (
        not normalized_code.isdigit()
        or len(normalized_code) != 6
        or get_market_for_stock(normalized_code) != "cn"
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_personal_research_stock",
                "message": "Personal research currently accepts one A-share stock",
            },
        )
    _required_flags(config, requested_mode=request.requested_mode)
    resolved_mode = ResearchBudgetService.resolve_mode(
        request.requested_mode,
        priority=request.priority,
    )
    if resolved_mode == "debate" and config.research_debate_enabled is not True:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "personal_research_debate_not_enabled",
                "message": "The resolved task mode requires RESEARCH_DEBATE_ENABLED",
            },
        )

    task_id = uuid.uuid4().hex
    payload = {
        "stock_code": normalized_code,
        "requested_mode": request.requested_mode,
        "priority": request.priority,
        "manual_daily_override": request.manual_daily_override,
        "report_type": request.report_type,
        "notify": request.notify,
        "query_source": "personal_research_api",
        "policy_account_id": request.policy_account_id,
        "target_weight_pct": request.target_weight_pct,
        "report_language": request.report_language,
    }
    store = DurableJobStore(
        build_default_durable_job_registry(),
        db_manager=db_manager,
    )
    try:
        outcome = store.enqueue(
            JobEnqueueRequest(
                job_type="personal_research",
                payload=payload,
                payload_version=1,
                task_id=task_id,
                stock_code=normalized_code,
                stock_name=normalized_code,
                dedupe_key=f"personal-research:cn:{normalized_code}:{resolved_mode}",
                idempotency_key=f"personal-research:{idempotency_key}",
                priority=request.priority,
                max_attempts=4,
                stage="personal_research",
                progress=0,
                message="Personal research queued",
                report_type=request.report_type or resolved_mode,
                query_source="personal_research_api",
                trace_id=task_id,
                notify=request.notify,
            )
        )
    except DurableJobConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"error": "durable_job_conflict", "message": str(exc)},
        ) from exc

    snapshot = store.get_job(outcome.task_id)
    if snapshot is None:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "durable_job_lost",
                "message": "Personal research job disappeared after enqueue",
            },
        )
    return PersonalResearchRunAccepted(
        task_id=outcome.task_id,
        trace_id=snapshot.trace_id or outcome.task_id,
        status=outcome.status,
        created=outcome.created,
        deduplicated=not outcome.created,
        stock_code=normalized_code,
        requested_mode=request.requested_mode,
        resolved_mode=resolved_mode,
        priority=request.priority,
    )


__all__ = ["router"]
