"""Read-only API schemas for immutable personal research artifacts."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


ResearchDataStatus = Literal[
    "available",
    "empty",
    "partial",
    "stale",
    "permission_denied",
    "not_supported",
    "fetch_failed",
]
ResearchHorizonDays = Literal[5, 10, 20]


class ResearchDatasetItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    dataset: str
    scope_type: str
    scope_value: str
    market: str
    provider: str
    schema_version: str
    trade_date: Optional[str] = None
    report_date: Optional[str] = None
    announcement_date: Optional[str] = None
    data_as_of: datetime
    available_at: datetime
    observed_at: datetime
    status: ResearchDataStatus
    normalized: Optional[Any] = None
    normalized_row_count: Optional[int] = Field(None, ge=0)
    normalized_truncated: bool = False
    content_hash: str = Field(..., min_length=64, max_length=64)
    raw_ref: Optional[Dict[str, Any]] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    supersedes_hash: Optional[str] = None
    origin_job_id: Optional[str] = None
    created_at: datetime


class ResearchDatasetListResponse(BaseModel):
    items: List[ResearchDatasetItem] = Field(default_factory=list)
    count: int = Field(..., ge=0)
    detail: bool = False
    row_limit: int = Field(..., ge=1, le=6000)


class ResearchFactorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    stock_code: str
    market: str
    company_profile: str
    primary_horizon: int
    requested_horizon: ResearchHorizonDays
    requested_trend: Optional[Dict[str, Any]] = None
    engine_bundle_version: str
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    trend_score: Optional[float] = None
    catalyst_score: Optional[float] = None
    risk_penalty: Optional[float] = None
    factors: Dict[str, Any]
    input_dataset_hashes: List[str]
    status: ResearchDataStatus
    coverage: float = Field(..., ge=0.0, le=1.0)
    unknowns: Any
    as_of: datetime
    available_at: datetime
    content_hash: str = Field(..., min_length=64, max_length=64)
    origin_job_id: Optional[str] = None
    created_at: datetime


class ResearchSnapshotResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    stock_code: str
    market: str
    snapshot_version: str
    field_dictionary_version: str
    factor_engine_version: str
    pack_version: str
    prompt_version: str
    policy_version: str
    model_route_fingerprint: str
    as_of: datetime
    available_at: datetime
    status: ResearchDataStatus
    snapshot: Dict[str, Any]
    snapshot_hash: str = Field(..., min_length=64, max_length=64)
    factor_snapshot_hash: Optional[str] = None
    origin_job_id: Optional[str] = None
    created_at: datetime
