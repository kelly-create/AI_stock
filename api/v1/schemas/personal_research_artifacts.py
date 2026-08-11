"""Public API schemas for immutable personal-research artifacts."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
BoundedId = Annotated[str, Field(min_length=1, max_length=128)]
ArtifactVersion = Annotated[str, Field(min_length=1, max_length=64)]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PersonalResearchStockLineage(_StrictModel):
    task_id: BoundedId
    market: Annotated[str, Field(min_length=1, max_length=16)]
    stock_code: Annotated[str, Field(min_length=1, max_length=16)]


class PersonalResearchSkillContract(_StrictModel):
    skill_id: BoundedId
    version: ArtifactVersion
    contract_hash: Sha256
    score_field: BoundedId


class PersonalResearchSkillLineage(PersonalResearchStockLineage):
    research_snapshot_hash: Sha256
    factor_snapshot_hash: Sha256
    evidence_snapshot_hash: Sha256
    dataset_snapshot_hashes: List[Sha256]
    dataset_lineage_hash: Sha256
    input_hash: Sha256
    output_hash: Sha256


class PersonalResearchSkillResult(_StrictModel):
    status: Literal["succeeded", "failed"]
    score: Optional[Annotated[float, Field(ge=0, le=100)]] = None
    output: Dict[str, Any]


class PersonalResearchSkillExecutionResponse(_StrictModel):
    contract: Literal["personal-research-skill-execution"]
    version: Literal["v1"]
    execution_hash: Sha256
    skill_contract: PersonalResearchSkillContract
    lineage: PersonalResearchSkillLineage
    input: Dict[str, Any]
    result: PersonalResearchSkillResult
    created_at: datetime


class PersonalResearchSkillExecutionListResponse(_StrictModel):
    contract: Literal["personal-research-skill-execution-collection"]
    version: Literal["v1"]
    lineage: PersonalResearchStockLineage
    expected_skill_ids: List[BoundedId]
    missing_skill_ids: List[BoundedId]
    complete: bool
    executions: List[PersonalResearchSkillExecutionResponse]


class PersonalResearchDebateLineage(PersonalResearchStockLineage):
    debate_snapshot_hash: Sha256
    evidence_snapshot_hash: Sha256


class PersonalResearchDebateVerifier(_StrictModel):
    version: ArtifactVersion
    input_hash: Sha256
    output_hash: Sha256
    valid: bool
    fail_closed: bool
    reason_codes: List[BoundedId]
    input: Dict[str, Any]
    output: Dict[str, Any]


class PersonalResearchDebateJudge(_StrictModel):
    version: ArtifactVersion
    policy_hash: Sha256
    input_hash: Sha256
    output_hash: Sha256
    fail_closed: bool
    reason_codes: List[BoundedId]
    verdict: Literal["bull", "bear", "balanced", "fail_closed"]
    winner: Optional[Literal["bull", "bear"]] = None
    input: Dict[str, Any]
    output: Dict[str, Any]


class PersonalResearchDebateReviewResponse(_StrictModel):
    contract: Literal["personal-research-debate-review"]
    version: Literal["v1"]
    review_hash: Sha256
    lineage: PersonalResearchDebateLineage
    verifier: PersonalResearchDebateVerifier
    judge: PersonalResearchDebateJudge
    created_at: datetime


class PersonalResearchThesisSkillLineage(_StrictModel):
    personal_value_quality: Sha256 = Field(alias="personal-value-quality")
    personal_trend_timing: Sha256 = Field(alias="personal-trend-timing")
    personal_catalyst: Sha256 = Field(alias="personal-catalyst")
    personal_risk: Sha256 = Field(alias="personal-risk")
    personal_evidence_quality: Sha256 = Field(alias="personal-evidence-quality")


class PersonalResearchThesisLineage(PersonalResearchStockLineage):
    research_snapshot_hash: Sha256
    skill_execution_hashes: PersonalResearchThesisSkillLineage
    debate_snapshot_hash: Optional[Sha256] = None
    debate_review_hash: Optional[Sha256] = None
    decision_signal_id: Optional[Annotated[int, Field(gt=0)]] = None
    policy_evaluation_hash: Optional[Sha256] = None
    policy_version: Optional[ArtifactVersion] = None
    policy_hash: Optional[Sha256] = None
    portfolio_snapshot_ref: Optional[BoundedId] = None
    supersedes_thesis_hash: Optional[Sha256] = None


class PersonalResearchThesisScores(_StrictModel):
    value_quality_score: Annotated[float, Field(ge=0, le=100)]
    trend_timing_score: Annotated[float, Field(ge=0, le=100)]
    catalyst_score: Annotated[float, Field(ge=0, le=100)]
    risk_score: Annotated[float, Field(ge=0, le=100)]
    evidence_quality_score: Annotated[float, Field(ge=0, le=100)]


class PersonalResearchThesisResponse(_StrictModel):
    contract: Literal["personal-research-thesis"]
    version: ArtifactVersion
    thesis_hash: Sha256
    content_hash: Sha256
    lineage: PersonalResearchThesisLineage
    stance: Literal[
        "strong_bullish", "bullish", "watch", "neutral", "bearish", "avoid"
    ]
    account_action: Literal[
        "observe",
        "open_candidate",
        "add_candidate",
        "hold",
        "reduce_candidate",
        "exit_candidate",
    ]
    scores: PersonalResearchThesisScores
    catalysts: List[str]
    invalidators: List[str]
    unknowns: List[str]
    evidence_refs: List[BoundedId]
    content: Dict[str, Any]
    created_at: datetime


__all__ = [
    "PersonalResearchDebateReviewResponse",
    "PersonalResearchSkillExecutionListResponse",
    "PersonalResearchSkillExecutionResponse",
    "PersonalResearchThesisResponse",
]
