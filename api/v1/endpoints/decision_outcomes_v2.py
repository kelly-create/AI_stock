"""Independent HTTP surface for personal-research Decision Outcome v2."""

from __future__ import annotations

import re
import uuid
from typing import List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Security
from fastapi.security import APIKeyCookie

from api.deps import get_config as get_config_dep, get_database_manager
from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.decision_outcomes_v2 import (
    DecisionOutcomeV2ListResponse,
    DecisionOutcomeV2RunAccepted,
    DecisionOutcomeV2RunRequest,
    DecisionOutcomeV2StatsResponse,
)
from data_provider.base import normalize_stock_code
from src.auth import COOKIE_NAME
from src.config import Config
from src.core.decision_outcome_v2_evaluator import (
    DECISION_OUTCOME_V2_ENGINE_VERSION,
)
from src.services.decision_outcome_v2_query_service import (
    DecisionOutcomeV2NotFoundError,
    DecisionOutcomeV2QueryService,
)
from src.services.durable_job_handlers import build_default_durable_job_registry
from src.services.durable_jobs import (
    DurableJobConflictError,
    DurableJobStore,
    JobEnqueueRequest,
)
from src.services.research.canonical import canonical_hash
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


def _query_service(db_manager: DatabaseManager) -> DecisionOutcomeV2QueryService:
    return DecisionOutcomeV2QueryService(db_manager)


def _bad_request(exc: Exception) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={"error": "validation_error", "message": str(exc)},
    )


def _internal_error(message: str) -> HTTPException:
    return HTTPException(
        status_code=500,
        detail={"error": "internal_error", "message": message},
    )


def _required_runtime_flags(config: Config) -> None:
    required = {
        "DURABLE_JOBS_ENABLED": config.durable_jobs_enabled,
        "PERSONAL_RESEARCH_ENABLED": config.personal_research_enabled,
        "TUSHARE_RESEARCH_ENABLED": config.tushare_research_enabled,
        "RESEARCH_FACTORS_ENABLED": config.research_factors_enabled,
        "DECISION_OUTCOME_V2_ENABLED": config.decision_outcome_v2_enabled,
    }
    missing = sorted(name for name, enabled in required.items() if enabled is not True)
    if missing:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "decision_outcome_v2_not_enabled",
                "message": "Required Decision Outcome v2 capabilities are disabled",
                "missing": missing,
            },
        )


@router.post(
    "/outcomes-v2/run",
    status_code=202,
    response_model=DecisionOutcomeV2RunAccepted,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Submit durable Decision Outcome v2 evaluation",
    operation_id="runDecisionOutcomesV2",
)
def run_decision_outcomes_v2(
    request: DecisionOutcomeV2RunRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    config: Config = Depends(get_config_dep),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> DecisionOutcomeV2RunAccepted:
    """Queue work only; Tushare access remains inside the durable Worker."""

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
    _required_runtime_flags(config)
    stock_code = (
        normalize_stock_code(request.stock_code)
        if request.stock_code is not None
        else None
    )
    if stock_code is not None and (
        not stock_code.isdigit() or len(stock_code) != 6
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_decision_outcome_v2_stock",
                "message": "Decision Outcome v2 currently accepts A-share stocks",
            },
        )
    profile = (
        request.decision_profile.strip().lower()
        if request.decision_profile is not None
        else None
    )
    payload = {
        "signal_id": request.signal_id,
        "horizons": list(request.horizons),
        "stock_code": stock_code,
        "decision_profile": profile,
        "limit": request.limit,
        "notify": request.notify,
    }
    selection_hash = canonical_hash(payload, exclude_volatile=False)
    task_id = uuid.uuid4().hex
    store = DurableJobStore(
        build_default_durable_job_registry(),
        db_manager=db_manager,
    )
    try:
        result = store.enqueue(
            JobEnqueueRequest(
                job_type="decision_outcomes_v2",
                payload=payload,
                payload_version=1,
                task_id=task_id,
                stock_code=stock_code or "decision-outcome-v2",
                stock_name=stock_code or "Decision Outcome v2",
                dedupe_key=f"decision-outcome-v2:{selection_hash}",
                idempotency_key=f"decision-outcome-v2:{idempotency_key}",
                priority=20,
                max_attempts=4,
                stage="decision_outcomes_v2",
                progress=0,
                message="Decision Outcome v2 evaluation queued",
                report_type="outcome-v2",
                query_source="decision_outcome_v2_api",
                trace_id=task_id,
                notify=request.notify,
            )
        )
    except DurableJobConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"error": "durable_job_conflict", "message": str(exc)},
        ) from exc
    snapshot = store.get_job(result.task_id)
    if snapshot is None:
        raise _internal_error("Decision Outcome v2 job disappeared after enqueue")
    return DecisionOutcomeV2RunAccepted(
        accepted=True,
        task_id=result.task_id,
        status=result.status,
        deduplicated=not result.created,
        engine_version=DECISION_OUTCOME_V2_ENGINE_VERSION,
        horizons=list(request.horizons),
    )


@router.get(
    "/outcomes-v2",
    response_model=DecisionOutcomeV2ListResponse,
    responses={**AUTH_RESPONSE, 400: {"model": ErrorResponse}},
    summary="List immutable Decision Outcome v2 results",
    operation_id="listDecisionOutcomesV2",
)
def list_decision_outcomes_v2(
    signal_id: Optional[int] = Query(None, gt=0),
    horizon: Optional[str] = Query(None),
    engine_version: Optional[str] = Query(None),
    eval_status: Optional[str] = Query(None),
    final_action_family: Optional[str] = Query(None),
    decision_profile: Optional[str] = Query(None),
    stock_code: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> DecisionOutcomeV2ListResponse:
    try:
        result = _query_service(db_manager).list_outcomes(
            signal_id=signal_id,
            horizon=horizon,
            engine_version=engine_version,
            eval_status=eval_status,
            final_action_family=final_action_family,
            decision_profile=decision_profile,
            stock_code=stock_code,
            page=page,
            page_size=page_size,
        )
        return DecisionOutcomeV2ListResponse(**result)
    except ValueError as exc:
        raise _bad_request(exc)
    except Exception as exc:
        raise _internal_error("List Decision Outcome v2 results failed") from exc


@router.get(
    "/outcomes-v2/stats",
    response_model=DecisionOutcomeV2StatsResponse,
    responses={**AUTH_RESPONSE, 400: {"model": ErrorResponse}},
    summary="Get Decision Outcome v2 calibration statistics",
    operation_id="getDecisionOutcomesV2Stats",
)
def get_decision_outcomes_v2_stats(
    horizons: Optional[List[str]] = Query(None),
    engine_version: Optional[str] = Query(None),
    decision_profile: Optional[str] = Query(None),
    final_action_family: Optional[str] = Query(None),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> DecisionOutcomeV2StatsResponse:
    try:
        result = _query_service(db_manager).get_stats(
            horizons=horizons,
            engine_version=engine_version,
            decision_profile=decision_profile,
            final_action_family=final_action_family,
        )
        return DecisionOutcomeV2StatsResponse(**result)
    except ValueError as exc:
        raise _bad_request(exc)
    except Exception as exc:
        raise _internal_error("Get Decision Outcome v2 stats failed") from exc


@router.get(
    "/{signal_id}/outcomes-v2",
    response_model=DecisionOutcomeV2ListResponse,
    responses={
        **AUTH_RESPONSE,
        404: {"model": ErrorResponse},
    },
    summary="List Decision Outcome v2 results for one signal",
    operation_id="listDecisionOutcomesV2BySignal",
)
def list_signal_decision_outcomes_v2(
    signal_id: int,
    engine_version: Optional[str] = Query(None),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> DecisionOutcomeV2ListResponse:
    try:
        result = _query_service(db_manager).list_for_signal(
            signal_id,
            engine_version=engine_version,
        )
        return DecisionOutcomeV2ListResponse(**result)
    except DecisionOutcomeV2NotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail={"error": "not_found", "message": str(exc)},
        ) from exc
    except ValueError as exc:
        raise _bad_request(exc)
    except Exception as exc:
        raise _internal_error("List signal Decision Outcome v2 results failed") from exc


__all__ = ["router"]
