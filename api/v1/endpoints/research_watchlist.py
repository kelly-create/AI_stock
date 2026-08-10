"""Enhanced watchlist and effective personal-research universe endpoints."""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from api.deps import get_system_config_service
from api.v1.errors import api_error
from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.research_watchlist import (
    ResearchMarket,
    ResearchWatchlistDeleteResponse,
    ResearchWatchlistItem,
    ResearchWatchlistListResponse,
    ResearchWatchlistUpsertRequest,
)
from src.services.research_watchlist_service import (
    ResearchWatchlistService,
    normalize_research_identity,
)
from src.services.stock_list_parser import split_stock_list
from src.services.system_config_service import ConfigConflictError, SystemConfigService


logger = logging.getLogger(__name__)
router = APIRouter()


def _read_legacy_snapshot(service: SystemConfigService) -> tuple[list[str], str]:
    payload = service.get_config(include_schema=False)
    value = ""
    for item in payload.get("items", []):
        if item.get("key") == "STOCK_LIST":
            value = str(item.get("value", ""))
            break
    return split_stock_list(value), str(payload.get("config_version") or "")


def _read_legacy_codes(service: SystemConfigService) -> list[str]:
    codes, _ = _read_legacy_snapshot(service)
    return codes


def _write_legacy_codes(
    service: SystemConfigService,
    codes: list[str],
    *,
    config_version: str,
) -> None:
    service.update(
        config_version=config_version,
        items=[{"key": "STOCK_LIST", "value": ",".join(codes)}],
        mask_token="******",
        reload_now=True,
    )


def _identity_or_400(stock_code: str, market: Optional[str]) -> tuple[str, str]:
    try:
        return normalize_research_identity(stock_code, market)
    except ValueError as exc:
        raise api_error(400, "invalid_stock_code", str(exc)) from exc


def _same_identity(raw_code: str, identity: tuple[str, str]) -> bool:
    try:
        return normalize_research_identity(raw_code) == identity
    except ValueError:
        return False


@router.get(
    "/watchlist",
    response_model=ResearchWatchlistListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="List enhanced research-watchlist metadata",
)
def list_research_watchlist(
    include_inactive: bool = Query(False),
) -> ResearchWatchlistListResponse:
    try:
        return ResearchWatchlistListResponse(
            **ResearchWatchlistService().list_watchlist(include_inactive=include_inactive)
        )
    except Exception as exc:
        logger.error("List enhanced research watchlist failed: %s", exc, exc_info=True)
        raise api_error(500, "internal_error", "List enhanced research watchlist failed") from exc


@router.get(
    "/universe",
    response_model=ResearchWatchlistListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="Resolve effective research universe from watchlist and holdings",
)
def get_research_universe(
    as_of: Optional[date] = Query(None),
    config_service: SystemConfigService = Depends(get_system_config_service),
) -> ResearchWatchlistListResponse:
    try:
        data = ResearchWatchlistService().build_effective_universe(
            legacy_codes=_read_legacy_codes(config_service),
            as_of=as_of,
        )
        return ResearchWatchlistListResponse(**data)
    except Exception as exc:
        logger.error("Resolve effective research universe failed: %s", exc, exc_info=True)
        raise api_error(500, "internal_error", "Resolve effective research universe failed") from exc


@router.put(
    "/watchlist/{market}/{stock_code:path}",
    response_model=ResearchWatchlistItem,
    responses={
        400: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Create or replace enhanced research-watchlist metadata",
)
def upsert_research_watchlist_item(
    request: ResearchWatchlistUpsertRequest,
    market: ResearchMarket = Path(...),
    stock_code: str = Path(..., min_length=1, max_length=32),
    config_service: SystemConfigService = Depends(get_system_config_service),
) -> ResearchWatchlistItem:
    identity = _identity_or_400(stock_code, market)
    try:
        item = ResearchWatchlistService().upsert_item(
            stock_code=identity[1],
            market=identity[0],
            reason=request.reason,
            priority=request.priority,
            analysis_tier=request.analysis_tier,
            next_review_at=request.next_review_at,
            source="manual",
        )
        legacy_codes, config_version = _read_legacy_snapshot(config_service)
        if not any(_same_identity(code, identity) for code in legacy_codes):
            _write_legacy_codes(
                config_service,
                [*legacy_codes, identity[1]],
                config_version=config_version,
            )
        item["sources"] = ["enhanced", "legacy"]
        return ResearchWatchlistItem(**item)
    except ConfigConflictError as exc:
        raise api_error(409, "config_conflict", "STOCK_LIST changed concurrently; refresh and retry") from exc
    except ValueError as exc:
        raise api_error(400, "validation_error", str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Upsert enhanced research watchlist failed: %s", exc, exc_info=True)
        raise api_error(500, "internal_error", "Upsert enhanced research watchlist failed") from exc


@router.delete(
    "/watchlist/{market}/{stock_code:path}",
    response_model=ResearchWatchlistDeleteResponse,
    responses={
        400: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Deactivate enhanced metadata and tombstone legacy membership",
)
def deactivate_research_watchlist_item(
    market: ResearchMarket = Path(...),
    stock_code: str = Path(..., min_length=1, max_length=32),
    config_service: SystemConfigService = Depends(get_system_config_service),
) -> ResearchWatchlistDeleteResponse:
    identity = _identity_or_400(stock_code, market)
    service = ResearchWatchlistService()
    try:
        # The tombstone is written first so an active holding remains visible but
        # legacy membership cannot immediately resurrect a removed watchlist row.
        service.set_active(
            stock_code=identity[1],
            market=identity[0],
            active=False,
            source="manual",
        )
        legacy_codes, config_version = _read_legacy_snapshot(config_service)
        kept = [code for code in legacy_codes if not _same_identity(code, identity)]
        if len(kept) != len(legacy_codes):
            _write_legacy_codes(
                config_service,
                kept,
                config_version=config_version,
            )
        return ResearchWatchlistDeleteResponse(deleted=1)
    except ConfigConflictError as exc:
        raise api_error(409, "config_conflict", "STOCK_LIST changed concurrently; refresh and retry") from exc
    except ValueError as exc:
        raise api_error(400, "validation_error", str(exc)) from exc
    except Exception as exc:
        logger.error("Deactivate enhanced research watchlist failed: %s", exc, exc_info=True)
        raise api_error(500, "internal_error", "Deactivate enhanced research watchlist failed") from exc
