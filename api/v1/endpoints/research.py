"""Read-only endpoints for immutable research factors and snapshots."""

from __future__ import annotations

from datetime import datetime
import logging
import re
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Path, Query, Security
from fastapi.security import APIKeyCookie
from pydantic import ValidationError

from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.research import (
    ResearchDatasetListResponse,
    ResearchDebateDetailResponse,
    ResearchDebateListResponse,
    ResearchDebateSummary,
    ResearchEvidenceDetailResponse,
    ResearchEvidenceListResponse,
    ResearchEvidenceSummary,
    ResearchFactorResponse,
    ResearchSnapshotResponse,
)
from src.auth import COOKIE_NAME
from src.services.research.debate_security import strict_public_identifier
from src.services.research.repositories import (
    ResearchSnapshotRepository,
    _decode_evidence_cursor,
)


logger = logging.getLogger(__name__)
admin_session_cookie = APIKeyCookie(
    name=COOKIE_NAME,
    scheme_name="AdminSessionCookie",
    auto_error=False,
)
router = APIRouter(dependencies=[Security(admin_session_cookie)])

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

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


def _query_text(value: Any, *, field_name: str, max_length: int) -> str:
    if not isinstance(value, str):
        raise _bad_request(ValueError(f"{field_name} must be a string"))
    normalized = value.strip()
    if not normalized or len(normalized) > max_length:
        raise _bad_request(
            ValueError(
                f"{field_name} must contain between 1 and {max_length} characters"
            )
        )
    try:
        return strict_public_identifier(normalized, field=field_name)
    except (TypeError, ValueError):
        raise _bad_request(
            ValueError(f"{field_name} must be a safe public identifier")
        ) from None


def _query_sha256(value: Any, *, field_name: str) -> str:
    normalized = _query_text(value, field_name=field_name, max_length=64)
    if not _SHA256_PATTERN.fullmatch(normalized):
        raise _bad_request(
            ValueError(f"{field_name} must be a lowercase SHA-256 digest")
        )
    return normalized


def _evidence_bad_request(message: str) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={"error": "validation_error", "message": message},
    )


def _invalid_evidence_hash() -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={
            "error": "validation_error",
            "message": "Evidence hash must be a lowercase SHA-256 digest.",
        },
    )


def _evidence_internal_error(exc: Exception) -> HTTPException:
    logger.error(
        "Research evidence query failed (%s); payload omitted",
        type(exc).__name__,
    )
    return HTTPException(
        status_code=500,
        detail={
            "error": "internal_error",
            "message": "Research evidence query failed",
        },
    )


def _evidence_validation_error(message: str) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={"error": "validation_error", "message": message},
    )


def _optional_evidence_text(
    value: Any,
    *,
    field_name: str,
    max_length: int,
    pattern: Optional[re.Pattern[str]] = None,
    public_identifier: bool = False,
) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not 1 <= len(value) <= max_length:
        raise _evidence_validation_error(
            f"{field_name} must be a non-empty string of at most {max_length} characters."
        )
    normalized = value.strip()
    if normalized and pattern is not None and not pattern.fullmatch(normalized):
        raise _evidence_validation_error(
            f"{field_name} must be a lowercase SHA-256 digest."
        )
    if normalized and public_identifier:
        try:
            normalized = strict_public_identifier(normalized, field=field_name)
        except (TypeError, ValueError):
            raise _evidence_validation_error(
                f"{field_name} must be a safe public identifier."
            ) from None
    return normalized or None


def _parse_evidence_as_of(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise _evidence_validation_error("as_of must be an ISO 8601 date-time.")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise _evidence_validation_error(
            "as_of must be an ISO 8601 date-time."
        ) from None
    value = parsed
    if value.tzinfo is None or value.utcoffset() is None:
        raise _evidence_bad_request("as_of must include a UTC offset.")
    return value


def _parse_evidence_limit(value: Any) -> int:
    if isinstance(value, bool):
        raise _evidence_validation_error("limit must be an integer from 1 to 100.")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and 1 <= len(value) <= 16:
        try:
            parsed = int(value, 10)
        except ValueError:
            raise _evidence_validation_error(
                "limit must be an integer from 1 to 100."
            ) from None
    else:
        raise _evidence_validation_error("limit must be an integer from 1 to 100.")
    if not 1 <= parsed <= 100:
        raise _evidence_validation_error("limit must be an integer from 1 to 100.")
    return parsed


def _evidence_summary(item: Any) -> ResearchEvidenceSummary:
    if not isinstance(item, dict):
        raise TypeError("research evidence record must be an object")
    summary = dict(item)
    summary.pop("evidence", None)
    return ResearchEvidenceSummary(**summary)


def _debate_bad_request(message: str) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={"error": "validation_error", "message": message},
    )


def _debate_validation_error(message: str) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={"error": "validation_error", "message": message},
    )


def _invalid_debate_hash() -> HTTPException:
    return _debate_validation_error(
        "Debate hash must be a lowercase SHA-256 digest."
    )


def _debate_internal_error(exc: Exception) -> HTTPException:
    logger.error(
        "Research debate query failed (%s); payload omitted",
        type(exc).__name__,
    )
    return HTTPException(
        status_code=500,
        detail={
            "error": "internal_error",
            "message": "Research debate query failed",
        },
    )


def _optional_debate_text(
    value: Any,
    *,
    field_name: str,
    max_length: int,
    pattern: Optional[re.Pattern[str]] = None,
    public_identifier: bool = False,
) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not 1 <= len(value) <= max_length:
        raise _debate_validation_error(
            f"{field_name} must be a non-empty string of at most {max_length} characters."
        )
    normalized = value.strip()
    if normalized and pattern is not None and not pattern.fullmatch(normalized):
        raise _debate_validation_error(
            f"{field_name} must be a lowercase SHA-256 digest."
        )
    if normalized and public_identifier:
        try:
            normalized = strict_public_identifier(normalized, field=field_name)
        except (TypeError, ValueError):
            raise _debate_validation_error(
                f"{field_name} must be a safe public identifier."
            ) from None
    return normalized or None


def _validate_cursor_query(
    value: Optional[str],
    *,
    invalid: Any,
) -> Optional[str]:
    if value is None:
        return None
    try:
        _decode_evidence_cursor(value)
    except ValueError:
        raise invalid("cursor is invalid.") from None
    return value


def _parse_debate_as_of(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise _debate_validation_error("as_of must be an ISO 8601 date-time.")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise _debate_validation_error(
            "as_of must be an ISO 8601 date-time."
        ) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _debate_bad_request("as_of must include a UTC offset.")
    return parsed


def _parse_debate_limit(value: Any) -> int:
    message = "limit must be an integer from 1 to 100."
    if isinstance(value, bool):
        raise _debate_validation_error(message)
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and 1 <= len(value) <= 16:
        try:
            parsed = int(value, 10)
        except ValueError:
            raise _debate_validation_error(message) from None
    else:
        raise _debate_validation_error(message)
    if not 1 <= parsed <= 100:
        raise _debate_validation_error(message)
    return parsed


def _debate_summary(item: Any) -> ResearchDebateSummary:
    if not isinstance(item, dict):
        raise TypeError("research debate record must be an object")
    summary = dict(item)
    summary.pop("debate", None)
    return ResearchDebateSummary(**summary)


def _internal_error(message: str, exc: Exception) -> HTTPException:
    logger.error(
        "%s (%s); payload omitted",
        message,
        type(exc).__name__,
    )
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
    normalized_stock_code = _query_text(
        stock_code,
        field_name="stock_code",
        max_length=16,
    )
    try:
        item = _repo().get_latest_factors(
            stock_code=normalized_stock_code,
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
    except ValidationError as exc:
        raise _internal_error("Research factor query failed", exc)
    except ValueError as exc:
        raise _internal_error("Research factor query failed", exc)
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
    normalized_snapshot_hash = _query_sha256(
        snapshot_hash,
        field_name="snapshot_hash",
    )
    try:
        item = _repo().get_research_snapshot(normalized_snapshot_hash)
        if item is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "message": "Research snapshot not found."},
            )
        return ResearchSnapshotResponse(**item)
    except HTTPException:
        raise
    except ValidationError as exc:
        raise _internal_error("Research snapshot query failed", exc)
    except ValueError as exc:
        raise _internal_error("Research snapshot query failed", exc)
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
    normalized_stock_code = _query_text(
        stock_code,
        field_name="stock_code",
        max_length=128,
    )
    normalized_dataset = (
        _query_text(dataset, field_name="dataset", max_length=64)
        if dataset is not None
        else None
    )
    try:
        items = _repo().list_datasets(
            scope_value=normalized_stock_code,
            dataset=normalized_dataset,
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
    except ValidationError as exc:
        raise _internal_error("Research dataset query failed", exc)
    except ValueError as exc:
        raise _internal_error("Research dataset query failed", exc)
    except Exception as exc:
        raise _internal_error("Research dataset query failed", exc)


@router.get(
    "/evidence",
    response_model=ResearchEvidenceListResponse,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse, "description": "Invalid evidence query"},
        422: {"model": ErrorResponse, "description": "Invalid query parameter"},
        500: {"model": ErrorResponse, "description": "Evidence query failed"},
    },
    operation_id="listResearchEvidence",
)
def list_research_evidence(
    job_id: Any = Query(
        None,
        json_schema_extra={"type": "string", "minLength": 1, "maxLength": 64},
    ),
    research_snapshot_hash: Any = Query(
        None,
        json_schema_extra={
            "type": "string",
            "minLength": 64,
            "maxLength": 64,
            "pattern": r"^[0-9a-f]{64}$",
        },
    ),
    stock_code: Any = Query(
        None,
        json_schema_extra={"type": "string", "minLength": 1, "maxLength": 16},
    ),
    as_of: Any = Query(
        None,
        json_schema_extra={"type": "string", "format": "date-time"},
    ),
    cursor: Any = Query(
        None,
        json_schema_extra={"type": "string", "minLength": 1, "maxLength": 256},
    ),
    limit: Any = Query(
        20,
        json_schema_extra={"type": "integer", "minimum": 1, "maximum": 100},
    ),
) -> ResearchEvidenceListResponse:
    """List immutable evidence summaries with stable keyset pagination."""

    normalized_job_id = _optional_evidence_text(
        job_id,
        field_name="job_id",
        max_length=64,
        public_identifier=True,
    )
    normalized_snapshot_hash = _optional_evidence_text(
        research_snapshot_hash,
        field_name="research_snapshot_hash",
        max_length=64,
        pattern=_SHA256_PATTERN,
    )
    normalized_stock_code = _optional_evidence_text(
        stock_code,
        field_name="stock_code",
        max_length=16,
        public_identifier=True,
    )
    if not any(
        (normalized_job_id, normalized_snapshot_hash, normalized_stock_code)
    ):
        raise _evidence_bad_request(
            "At least one of job_id, research_snapshot_hash, or stock_code is required."
        )
    aware_as_of = _parse_evidence_as_of(as_of)
    normalized_cursor = _validate_cursor_query(
        _optional_evidence_text(
            cursor,
            field_name="cursor",
            max_length=256,
        ),
        invalid=_evidence_validation_error,
    )
    normalized_limit = _parse_evidence_limit(limit)
    try:
        page = _repo().list_evidence(
            job_id=normalized_job_id,
            research_snapshot_hash=normalized_snapshot_hash,
            stock_code=normalized_stock_code,
            as_of=aware_as_of,
            cursor=normalized_cursor,
            limit=normalized_limit,
        )
        if not isinstance(page, dict) or not isinstance(page.get("items"), list):
            raise TypeError("research evidence page is malformed")
        items = [_evidence_summary(item) for item in page["items"]]
        return ResearchEvidenceListResponse(
            items=items,
            count=len(items),
            next_cursor=page.get("next_cursor"),
        )
    except HTTPException:
        raise
    except ValidationError as exc:
        raise _evidence_internal_error(exc)
    except ValueError as exc:
        raise _evidence_internal_error(exc)
    except Exception as exc:
        raise _evidence_internal_error(exc)


@router.get(
    "/evidence/{evidence_hash}",
    response_model=ResearchEvidenceDetailResponse,
    responses={
        **AUTH_RESPONSE,
        404: {"model": ErrorResponse, "description": "Evidence not found"},
        422: {"model": ErrorResponse, "description": "Invalid evidence hash"},
        500: {"model": ErrorResponse, "description": "Evidence query failed"},
    },
    operation_id="getResearchEvidence",
)
def get_research_evidence(
    evidence_hash: str = Path(
        ...,
        json_schema_extra={
            "minLength": 64,
            "maxLength": 64,
            "pattern": r"^[0-9a-f]{64}$",
        },
    ),
) -> ResearchEvidenceDetailResponse:
    """Read one immutable evidence artifact by its content hash."""

    if not _SHA256_PATTERN.fullmatch(evidence_hash):
        raise _invalid_evidence_hash()
    try:
        item = _repo().get_evidence(evidence_hash)
        if item is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "not_found",
                    "message": "Research evidence not found.",
                },
            )
        if not isinstance(item, dict):
            raise TypeError("research evidence record must be an object")
        return ResearchEvidenceDetailResponse(**item)
    except HTTPException:
        raise
    except Exception as exc:
        raise _evidence_internal_error(exc)


@router.get(
    "/debates",
    response_model=ResearchDebateListResponse,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse, "description": "Invalid debate query"},
        422: {"model": ErrorResponse, "description": "Invalid query parameter"},
        500: {"model": ErrorResponse, "description": "Debate query failed"},
    },
    operation_id="listResearchDebates",
)
def list_research_debates(
    job_id: Any = Query(
        None,
        json_schema_extra={"type": "string", "minLength": 1, "maxLength": 64},
    ),
    research_snapshot_hash: Any = Query(
        None,
        json_schema_extra={
            "type": "string",
            "minLength": 64,
            "maxLength": 64,
            "pattern": r"^[0-9a-f]{64}$",
        },
    ),
    stock_code: Any = Query(
        None,
        json_schema_extra={"type": "string", "minLength": 1, "maxLength": 16},
    ),
    evidence_snapshot_hash: Any = Query(
        None,
        json_schema_extra={
            "type": "string",
            "minLength": 64,
            "maxLength": 64,
            "pattern": r"^[0-9a-f]{64}$",
        },
    ),
    as_of: Any = Query(
        None,
        json_schema_extra={"type": "string", "format": "date-time"},
    ),
    cursor: Any = Query(
        None,
        json_schema_extra={"type": "string", "minLength": 1, "maxLength": 256},
    ),
    limit: Any = Query(
        20,
        json_schema_extra={"type": "integer", "minimum": 1, "maximum": 100},
    ),
) -> ResearchDebateListResponse:
    """List immutable debate summaries with stable keyset pagination."""

    normalized_job_id = _optional_debate_text(
        job_id,
        field_name="job_id",
        max_length=64,
        public_identifier=True,
    )
    normalized_snapshot_hash = _optional_debate_text(
        research_snapshot_hash,
        field_name="research_snapshot_hash",
        max_length=64,
        pattern=_SHA256_PATTERN,
    )
    normalized_stock_code = _optional_debate_text(
        stock_code,
        field_name="stock_code",
        max_length=16,
        public_identifier=True,
    )
    if not any(
        (normalized_job_id, normalized_snapshot_hash, normalized_stock_code)
    ):
        raise _debate_bad_request(
            "At least one of job_id, research_snapshot_hash, or stock_code is required."
        )
    normalized_evidence_hash = _optional_debate_text(
        evidence_snapshot_hash,
        field_name="evidence_snapshot_hash",
        max_length=64,
        pattern=_SHA256_PATTERN,
    )
    aware_as_of = _parse_debate_as_of(as_of)
    normalized_cursor = _validate_cursor_query(
        _optional_debate_text(
            cursor,
            field_name="cursor",
            max_length=256,
        ),
        invalid=_debate_validation_error,
    )
    normalized_limit = _parse_debate_limit(limit)
    try:
        page = _repo().list_debate_snapshots(
            job_id=normalized_job_id,
            research_snapshot_hash=normalized_snapshot_hash,
            stock_code=normalized_stock_code,
            evidence_snapshot_hash=normalized_evidence_hash,
            as_of=aware_as_of,
            cursor=normalized_cursor,
            limit=normalized_limit,
        )
        if not isinstance(page, dict) or not isinstance(page.get("items"), list):
            raise TypeError("research debate page is malformed")
        items = [_debate_summary(item) for item in page["items"]]
        return ResearchDebateListResponse(
            items=items,
            count=len(items),
            next_cursor=page.get("next_cursor"),
        )
    except HTTPException:
        raise
    except ValidationError as exc:
        raise _debate_internal_error(exc)
    except ValueError as exc:
        raise _debate_internal_error(exc)
    except Exception as exc:
        raise _debate_internal_error(exc)


@router.get(
    "/debates/{debate_hash}",
    response_model=ResearchDebateDetailResponse,
    responses={
        **AUTH_RESPONSE,
        404: {"model": ErrorResponse, "description": "Debate not found"},
        422: {"model": ErrorResponse, "description": "Invalid debate hash"},
        500: {"model": ErrorResponse, "description": "Debate query failed"},
    },
    operation_id="getResearchDebate",
)
def get_research_debate(
    debate_hash: str = Path(
        ...,
        json_schema_extra={
            "minLength": 64,
            "maxLength": 64,
            "pattern": r"^[0-9a-f]{64}$",
        },
    ),
) -> ResearchDebateDetailResponse:
    """Read one immutable bounded debate by its content hash."""

    if not _SHA256_PATTERN.fullmatch(debate_hash):
        raise _invalid_debate_hash()
    try:
        item = _repo().get_debate_snapshot(debate_hash)
        if item is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "not_found",
                    "message": "Research debate not found.",
                },
            )
        if not isinstance(item, dict):
            raise TypeError("research debate record must be an object")
        return ResearchDebateDetailResponse(**item)
    except HTTPException:
        raise
    except Exception as exc:
        raise _debate_internal_error(exc)
