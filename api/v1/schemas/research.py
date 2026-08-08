"""Read-only API schemas for immutable personal research artifacts."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Dict, List, Literal, Optional

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
ResearchEvidenceStatus = Literal["available", "partial", "empty", "fetch_failed"]
ResearchEvidenceClaimStatus = Literal[
    "supported",
    "partial",
    "contradicted",
    "insufficient",
]
ResearchEvidenceClaimKind = Literal["factor_metric", "reported_event"]
ResearchEvidenceRelation = Literal["supports", "contradicts", "context"]
ResearchEvidenceArtifactType = Literal["dataset", "factor"]
ResearchSha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
ResearchEvidenceIdentifier = Annotated[str, Field(min_length=1, max_length=128)]
ResearchEvidenceLimitation = Annotated[str, Field(max_length=500)]


class ResearchEvidenceCitation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: ResearchEvidenceIdentifier
    relation: ResearchEvidenceRelation
    artifact_type: ResearchEvidenceArtifactType
    artifact_hash: ResearchSha256
    json_pointer: str = Field(..., max_length=1024)
    value_hash: ResearchSha256
    available_at: datetime
    source_name: str = Field(..., min_length=1, max_length=160)
    title: str = Field(..., min_length=1, max_length=300)
    excerpt: str = Field(..., min_length=1, max_length=800)
    canonical_url: Optional[str] = Field(None, max_length=2048)


class ResearchEvidenceClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: ResearchEvidenceIdentifier
    kind: ResearchEvidenceClaimKind
    statement: str = Field(..., min_length=1, max_length=2000)
    status: ResearchEvidenceClaimStatus
    citation_ids: List[ResearchEvidenceIdentifier] = Field(
        default_factory=list,
        max_length=16,
    )
    limitations: List[ResearchEvidenceLimitation] = Field(
        default_factory=list,
        max_length=16,
    )
    available_at: datetime


class ResearchEvidencePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_engine_version: str = Field(..., min_length=1, max_length=64)
    claim_policy_version: str = Field(..., min_length=1, max_length=64)
    stock_code: str = Field(..., min_length=1, max_length=128)
    market: str = Field(..., min_length=1, max_length=16)
    as_of: datetime
    available_at: datetime
    status: ResearchEvidenceStatus
    coverage: float = Field(..., ge=0.0, le=1.0)
    input_dataset_hashes: List[ResearchSha256] = Field(default_factory=list)
    factor_snapshot_hash: ResearchSha256
    limitations: List[ResearchEvidenceLimitation] = Field(
        default_factory=list,
        max_length=32,
    )
    claims: List[ResearchEvidenceClaim] = Field(default_factory=list, max_length=32)
    citations: List[ResearchEvidenceCitation] = Field(
        default_factory=list,
        max_length=16,
    )


class ResearchEvidenceSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    stock_code: str = Field(..., min_length=1, max_length=128)
    market: str = Field(..., min_length=1, max_length=16)
    evidence_engine_version: str = Field(..., min_length=1, max_length=64)
    claim_policy_version: str = Field(..., min_length=1, max_length=64)
    as_of: datetime
    available_at: datetime
    status: ResearchEvidenceStatus
    coverage: float = Field(..., ge=0.0, le=1.0)
    claim_count: int = Field(..., ge=0, le=32)
    citation_count: int = Field(..., ge=0, le=16)
    input_dataset_hashes: List[ResearchSha256] = Field(default_factory=list)
    factor_snapshot_hash: ResearchSha256
    evidence_hash: ResearchSha256
    origin_job_id: Optional[str] = Field(None, max_length=64)
    created_at: datetime


class ResearchEvidenceDetailResponse(ResearchEvidenceSummary):
    evidence: ResearchEvidencePayload


class ResearchEvidenceListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: List[ResearchEvidenceSummary] = Field(default_factory=list)
    count: int = Field(..., ge=0)
    next_cursor: Optional[str] = Field(None, max_length=2048)


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
    evidence_snapshot_hash: Optional[str] = Field(
        None,
        pattern=r"^[0-9a-f]{64}$",
    )
    origin_job_id: Optional[str] = None
    created_at: datetime
