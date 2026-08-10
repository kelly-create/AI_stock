"""Authenticated read-only endpoints for immutable personal-research artifacts."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Path, Query, Security
from fastapi.security import APIKeyCookie
from pydantic import ValidationError

from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.personal_research_artifacts import (
    PersonalResearchDebateReviewResponse,
    PersonalResearchSkillExecutionListResponse,
    PersonalResearchSkillExecutionResponse,
    PersonalResearchThesisResponse,
)
from src.auth import COOKIE_NAME
from src.services.personal_research_artifact_query_service import (
    PersonalResearchArtifactContractError,
    PersonalResearchArtifactNotFoundError,
    PersonalResearchArtifactQueryService,
)


logger = logging.getLogger(__name__)
admin_session_cookie = APIKeyCookie(
    name=COOKIE_NAME,
    scheme_name="AdminSessionCookie",
    auto_error=False,
)
router = APIRouter(dependencies=[Security(admin_session_cookie)])

AUTH_RESPONSE = {
    401: {
        "model": ErrorResponse,
        "description": "Authentication is required when admin auth is enabled.",
    }
}
NOT_FOUND_RESPONSE = {
    "model": ErrorResponse,
    "description": "The requested immutable artifact does not exist.",
}


def _service() -> PersonalResearchArtifactQueryService:
    return PersonalResearchArtifactQueryService()


def _not_found(exc: PersonalResearchArtifactNotFoundError) -> HTTPException:
    return HTTPException(
        status_code=404,
        detail={"error": "not_found", "message": str(exc)},
    )


def _bad_request(exc: ValueError) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={"error": "validation_error", "message": str(exc)},
    )


def _internal_error(message: str, exc: Exception) -> HTTPException:
    logger.error("%s (%s)", message, type(exc).__name__, exc_info=True)
    return HTTPException(
        status_code=500,
        detail={"error": "internal_error", "message": message},
    )


@router.get(
    "/personal/artifacts/skills/tasks/{task_id}/stocks/{market}/{stock_code}",
    response_model=PersonalResearchSkillExecutionListResponse,
    responses={**AUTH_RESPONSE, 404: NOT_FOUND_RESPONSE},
    operation_id="listPersonalResearchSkillExecutions",
)
def list_skill_executions(
    task_id: str = Path(..., min_length=1, max_length=64),
    market: str = Path(..., min_length=1, max_length=16),
    stock_code: str = Path(..., min_length=1, max_length=16),
) -> PersonalResearchSkillExecutionListResponse:
    try:
        return PersonalResearchSkillExecutionListResponse(
            **_service().list_skill_executions(
                task_id=task_id,
                market=market,
                stock_code=stock_code,
            )
        )
    except PersonalResearchArtifactNotFoundError as exc:
        raise _not_found(exc) from exc
    except (PersonalResearchArtifactContractError, ValidationError) as exc:
        raise _internal_error("Read personal research Skill executions failed", exc)
    except ValueError as exc:
        raise _bad_request(exc) from exc
    except Exception as exc:
        raise _internal_error("Read personal research Skill executions failed", exc)


@router.get(
    "/personal/artifacts/skills/{execution_hash}",
    response_model=PersonalResearchSkillExecutionResponse,
    responses={**AUTH_RESPONSE, 404: NOT_FOUND_RESPONSE},
    operation_id="getPersonalResearchSkillExecution",
)
def get_skill_execution(
    execution_hash: str = Path(..., pattern=r"^[0-9a-f]{64}$"),
) -> PersonalResearchSkillExecutionResponse:
    try:
        return PersonalResearchSkillExecutionResponse(
            **_service().get_skill_execution(execution_hash)
        )
    except PersonalResearchArtifactNotFoundError as exc:
        raise _not_found(exc) from exc
    except (PersonalResearchArtifactContractError, ValidationError) as exc:
        raise _internal_error("Read personal research Skill execution failed", exc)
    except ValueError as exc:
        raise _bad_request(exc) from exc
    except Exception as exc:
        raise _internal_error("Read personal research Skill execution failed", exc)


@router.get(
    "/personal/artifacts/debate-reviews/{review_hash}",
    response_model=PersonalResearchDebateReviewResponse,
    responses={**AUTH_RESPONSE, 404: NOT_FOUND_RESPONSE},
    operation_id="getPersonalResearchDebateReview",
)
def get_debate_review(
    review_hash: str = Path(..., pattern=r"^[0-9a-f]{64}$"),
) -> PersonalResearchDebateReviewResponse:
    try:
        return PersonalResearchDebateReviewResponse(
            **_service().get_debate_review(review_hash)
        )
    except PersonalResearchArtifactNotFoundError as exc:
        raise _not_found(exc) from exc
    except (PersonalResearchArtifactContractError, ValidationError) as exc:
        raise _internal_error("Read personal research Debate review failed", exc)
    except ValueError as exc:
        raise _bad_request(exc) from exc
    except Exception as exc:
        raise _internal_error("Read personal research Debate review failed", exc)


@router.get(
    "/personal/artifacts/theses/by-signal/{decision_signal_id}",
    response_model=PersonalResearchThesisResponse,
    responses={**AUTH_RESPONSE, 404: NOT_FOUND_RESPONSE},
    operation_id="getLatestPersonalResearchThesisBySignal",
)
def get_latest_thesis_by_signal(
    decision_signal_id: int = Path(..., gt=0),
) -> PersonalResearchThesisResponse:
    try:
        return PersonalResearchThesisResponse(
            **_service().get_latest_thesis_by_signal_id(decision_signal_id)
        )
    except PersonalResearchArtifactNotFoundError as exc:
        raise _not_found(exc) from exc
    except (PersonalResearchArtifactContractError, ValidationError) as exc:
        raise _internal_error("Read personal research Thesis failed", exc)
    except ValueError as exc:
        raise _bad_request(exc) from exc
    except Exception as exc:
        raise _internal_error("Read personal research Thesis failed", exc)


@router.get(
    "/personal/artifacts/theses/latest",
    response_model=PersonalResearchThesisResponse,
    responses={**AUTH_RESPONSE, 404: NOT_FOUND_RESPONSE},
    operation_id="getLatestPersonalResearchThesisForTaskStock",
)
def get_latest_thesis_for_task_stock(
    task_id: str = Query(..., min_length=1, max_length=64),
    market: str = Query(..., min_length=1, max_length=16),
    stock_code: str = Query(..., min_length=1, max_length=16),
) -> PersonalResearchThesisResponse:
    try:
        return PersonalResearchThesisResponse(
            **_service().get_latest_thesis_for_task_stock(
                task_id=task_id,
                market=market,
                stock_code=stock_code,
            )
        )
    except PersonalResearchArtifactNotFoundError as exc:
        raise _not_found(exc) from exc
    except (PersonalResearchArtifactContractError, ValidationError) as exc:
        raise _internal_error("Read personal research Thesis failed", exc)
    except ValueError as exc:
        raise _bad_request(exc) from exc
    except Exception as exc:
        raise _internal_error("Read personal research Thesis failed", exc)


@router.get(
    "/personal/artifacts/theses/{thesis_hash}",
    response_model=PersonalResearchThesisResponse,
    responses={**AUTH_RESPONSE, 404: NOT_FOUND_RESPONSE},
    operation_id="getPersonalResearchThesis",
)
def get_thesis(
    thesis_hash: str = Path(..., pattern=r"^[0-9a-f]{64}$"),
) -> PersonalResearchThesisResponse:
    try:
        return PersonalResearchThesisResponse(
            **_service().get_thesis(thesis_hash)
        )
    except PersonalResearchArtifactNotFoundError as exc:
        raise _not_found(exc) from exc
    except (PersonalResearchArtifactContractError, ValidationError) as exc:
        raise _internal_error("Read personal research Thesis failed", exc)
    except ValueError as exc:
        raise _bad_request(exc) from exc
    except Exception as exc:
        raise _internal_error("Read personal research Thesis failed", exc)


__all__ = ["router"]
