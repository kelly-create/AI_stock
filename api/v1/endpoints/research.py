"""Read-only endpoints for immutable research factors and snapshots."""

from __future__ import annotations

from datetime import datetime
import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, Security
from fastapi.security import APIKeyCookie

from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.research import (
    ResearchDatasetListResponse,
    ResearchFactorResponse,
    ResearchSnapshotResponse,
)
from src.auth import COOKIE_NAME
from src.services.research.repositories import ResearchSnapshotRepository


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


def _repo() -> ResearchSnapshotRepository:
    return ResearchSnapshotRepository()


def _bad_request(exc: Exception) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={"error": "validation_error", "message": str(exc)},
    )


def _internal_error(message: str, exc: Exception) -> HTTPException:
    logger.error("%s: %s", message, exc, exc_info=True)
    return HTTPException(
        status_code=500,
        detail={"error": "internal_error", "message": message},
    )


def _summarize_payload(value: Any) -> Any:
    if isinstance(value, list):
        keys = sorted(
            {str(key) for row in value[:20] if isinstance(row, dict) for key in row}
        )
        return {"row_count": len(value), "fields": keys[:100]}
    if isinstance(value, dict):
        return {"fields": sorted(str(key) for key in value)[:100]}
    return value


def _shape_dataset_payload(item: dict[str, Any], *, detail: bool, row_limit: int) -> None:
    normalized = item.get("normalized")
    if not isinstance(normalized, list):
        item["normalized_row_count"] = None
        item["normalized_truncated"] = False
        if not detail:
            item["normalized"] = _summarize_payload(normalized)
        return
    item["normalized_row_count"] = len(normalized)
    if detail:
        item["normalized"] = normalized[:row_limit]
        item["normalized_truncated"] = len(normalized) > row_limit
    else:
        item["normalized"] = _summarize_payload(normalized)
        item["normalized_truncated"] = False


@router.get(
    "/factors/{stock_code}",
    response_model=ResearchFactorResponse,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse, "description": "Invalid query"},
        404: {"model": ErrorResponse, "description": "No matching factor snapshot"},
        500: {"model": ErrorResponse, "description": "Research query failed"},
    },
    operation_id="getResearchFactors",
)
def get_research_factors(
    stock_code: str,
    as_of: Optional[datetime] = Query(None),
    horizon_days: int = Query(10),
) -> ResearchFactorResponse:
    if horizon_days not in {5, 10, 20}:
        raise _bad_request(ValueError("horizon_days must be one of 5, 10, or 20"))
    try:
        item = _repo().get_latest_factors(
            stock_code=stock_code,
            as_of=as_of,
            horizon_days=horizon_days,
        )
        if item is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "message": "Research factors not found."},
            )
        return ResearchFactorResponse(**item)
    except HTTPException:
        raise
    except ValueError as exc:
        raise _bad_request(exc)
    except Exception as exc:
        raise _internal_error("Research factor query failed", exc)


@router.get(
    "/snapshots/{snapshot_hash}",
    response_model=ResearchSnapshotResponse,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse, "description": "Invalid snapshot hash"},
        404: {"model": ErrorResponse, "description": "Snapshot not found"},
        500: {"model": ErrorResponse, "description": "Research query failed"},
    },
    operation_id="getResearchSnapshot",
)
def get_research_snapshot(snapshot_hash: str) -> ResearchSnapshotResponse:
    try:
        item = _repo().get_research_snapshot(snapshot_hash)
        if item is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "message": "Research snapshot not found."},
            )
        return ResearchSnapshotResponse(**item)
    except HTTPException:
        raise
    except ValueError as exc:
        raise _bad_request(exc)
    except Exception as exc:
        raise _internal_error("Research snapshot query failed", exc)


@router.get(
    "/datasets/{stock_code}",
    response_model=ResearchDatasetListResponse,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Research query failed"},
    },
    operation_id="listResearchDatasets",
)
def list_research_datasets(
    stock_code: str,
    dataset: Optional[str] = Query(None),
    as_of: Optional[datetime] = Query(None),
    detail: bool = Query(False),
    limit: int = Query(100, ge=1, le=200),
    row_limit: int = Query(1000, ge=1, le=6000),
) -> ResearchDatasetListResponse:
    try:
        items = _repo().list_datasets(
            scope_value=stock_code,
            dataset=dataset,
            as_of=as_of,
            limit=limit,
        )
        for item in items:
            _shape_dataset_payload(item, detail=detail, row_limit=row_limit)
        return ResearchDatasetListResponse(
            items=items,
            count=len(items),
            detail=detail,
            row_limit=row_limit,
        )
    except ValueError as exc:
        raise _bad_request(exc)
    except Exception as exc:
        raise _internal_error("Research dataset query failed", exc)
