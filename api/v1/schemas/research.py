"""Read-only API schemas for immutable personal research artifacts."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.services.research.debate_security import (
    strict_error_code,
    strict_model_identifier,
    strict_public_identifier,
    strict_version_identifier,
    validate_debate_prose,
)
from src.services.research.factor_contract import (
    MAX_FACTOR_DATASET_HASHES,
    MAX_FACTOR_UNKNOWNS,
    validate_factor_contract,
)
from src.services.research.repositories import (
    _decode_evidence_cursor,
    sanitize_error_message,
)


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
ResearchVersionIdentifier = Annotated[str, Field(min_length=1, max_length=64)]
ResearchPublicIdentifier = Annotated[str, Field(min_length=1, max_length=128)]
ResearchOriginJobId = Annotated[str, Field(min_length=1, max_length=64)]
ResearchEvidenceLimitation = Annotated[str, Field(min_length=1, max_length=500)]
ResearchDebateStatus = Literal[
    "available",
    "partial",
    "empty",
    "generation_failed",
]
ResearchDebateStance = Literal["bull", "bear"]
ResearchDebateLimitation = Annotated[str, Field(min_length=1, max_length=500)]
ResearchDebateArgumentLimitation = Annotated[
    str,
    Field(min_length=1, max_length=300),
]
ResearchDebateOpenQuestion = Annotated[
    str,
    Field(min_length=1, max_length=500),
]
ResearchDebateErrorCode = Annotated[str, Field(min_length=1, max_length=64)]


def _require_unique(values: List[Any], *, field: str) -> List[Any]:
    if len(values) != len(set(values)):
        raise ValueError(f"{field} must contain unique values")
    return values


def _require_sorted_unique_hashes(values: List[str], *, field: str) -> List[str]:
    if values != sorted(set(values)):
        raise ValueError(f"{field} must be sorted and unique")
    return values


def _validate_public_cursor(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    try:
        _decode_evidence_cursor(value)
    except ValueError as exc:
        raise ValueError("next_cursor is invalid") from exc
    return value


def _evidence_coverage(claims: List["ResearchEvidenceClaim"]) -> float:
    weights = {
        "supported": 1.0,
        "contradicted": 1.0,
        "partial": 0.5,
        "insufficient": 0.0,
    }
    if not claims:
        return 0.0
    return round(sum(weights[claim.status] for claim in claims) / len(claims), 6)


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

    @field_validator("id")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        return strict_public_identifier(value, field="citation.id")


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

    @field_validator("id")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        return strict_public_identifier(value, field="claim.id")

    @field_validator("citation_ids")
    @classmethod
    def validate_citation_identifiers(cls, values: List[str]) -> List[str]:
        checked = [
            strict_public_identifier(value, field="claim.citation_ids")
            for value in values
        ]
        return _require_unique(checked, field="claim.citation_ids")

    @field_validator("limitations")
    @classmethod
    def validate_limitations(cls, values: List[str]) -> List[str]:
        return _require_unique(values, field="claim.limitations")


class ResearchEvidencePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_engine_version: ResearchVersionIdentifier
    claim_policy_version: ResearchVersionIdentifier
    stock_code: str = Field(..., min_length=1, max_length=16)
    market: str = Field(..., min_length=1, max_length=16)
    as_of: datetime
    available_at: datetime
    status: ResearchEvidenceStatus
    coverage: float = Field(..., ge=0.0, le=1.0)
    input_dataset_hashes: List[ResearchSha256] = Field(
        ...,
        min_length=1,
        max_length=MAX_FACTOR_DATASET_HASHES,
    )
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

    @field_validator("evidence_engine_version", "claim_policy_version")
    @classmethod
    def validate_versions(cls, value: str) -> str:
        return strict_version_identifier(value, field="evidence version")

    @field_validator("input_dataset_hashes")
    @classmethod
    def validate_dataset_hashes(cls, values: List[str]) -> List[str]:
        return _require_sorted_unique_hashes(
            values,
            field="input_dataset_hashes",
        )

    @field_validator("limitations")
    @classmethod
    def validate_limitations(cls, values: List[str]) -> List[str]:
        return _require_unique(values, field="evidence.limitations")

    @model_validator(mode="after")
    def validate_graph(self) -> "ResearchEvidencePayload":
        if self.available_at > self.as_of:
            raise ValueError("evidence available_at cannot be after as_of")

        citation_by_id: Dict[str, ResearchEvidenceCitation] = {}
        for citation in self.citations:
            if citation.id in citation_by_id:
                raise ValueError("evidence citation IDs must be unique")
            if citation.available_at > self.as_of:
                raise ValueError("citation available_at cannot be after as_of")
            if citation.artifact_type == "dataset":
                if citation.artifact_hash not in self.input_dataset_hashes:
                    raise ValueError("citation references an unknown Dataset hash")
            elif citation.artifact_hash != self.factor_snapshot_hash:
                raise ValueError("citation references an unknown Factor hash")
            citation_by_id[citation.id] = citation

        claim_ids: set[str] = set()
        referenced_citation_ids: set[str] = set()
        for claim in self.claims:
            if claim.id in claim_ids:
                raise ValueError("evidence claim IDs must be unique")
            claim_ids.add(claim.id)
            if claim.available_at > self.as_of:
                raise ValueError("claim available_at cannot be after as_of")
            unknown_citations = set(claim.citation_ids) - set(citation_by_id)
            if unknown_citations:
                raise ValueError("claim references an unknown citation")
            claim_citations = [
                citation_by_id[citation_id]
                for citation_id in claim.citation_ids
            ]
            referenced_citation_ids.update(claim.citation_ids)
            if claim_citations and claim.available_at < max(
                citation.available_at for citation in claim_citations
            ):
                raise ValueError("claim available_at cannot precede its citations")

            relations = {citation.relation for citation in claim_citations}
            if claim.status == "supported" and (
                "supports" not in relations or "contradicts" in relations
            ):
                raise ValueError("supported claim citation relations are invalid")
            if claim.status == "contradicted" and (
                "contradicts" not in relations or "supports" in relations
            ):
                raise ValueError("contradicted claim citation relations are invalid")
            if claim.status == "partial" and (
                not claim_citations or not claim.limitations
            ):
                raise ValueError("partial claims require citations and limitations")
            if claim.status == "insufficient" and (
                claim_citations or not claim.limitations
            ):
                raise ValueError(
                    "insufficient claims require limitations and no citations"
                )
            if claim.kind == "factor_metric" and any(
                citation.artifact_type != "factor"
                for citation in claim_citations
            ):
                raise ValueError("factor_metric claims may cite only Factor artifacts")
            if claim.kind == "reported_event" and (
                not claim_citations
                or any(
                    citation.artifact_type != "dataset"
                    or citation.relation != "context"
                    for citation in claim_citations
                )
                or claim.status != "partial"
            ):
                raise ValueError("reported_event claim citations are invalid")

        if set(citation_by_id) != referenced_citation_ids:
            raise ValueError("orphan evidence citations are forbidden")

        expected_coverage = _evidence_coverage(self.claims)
        if self.coverage != expected_coverage:
            raise ValueError("evidence coverage does not match claim statuses")
        if not self.claims:
            if self.status not in {"empty", "fetch_failed"}:
                raise ValueError("evidence status does not match its claims")
        elif self.status == "available" and expected_coverage != 1.0:
            raise ValueError("available evidence requires complete claim coverage")
        elif self.status in {"empty", "fetch_failed"}:
            raise ValueError("non-empty evidence cannot use an empty status")
        return self


class ResearchEvidenceSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    stock_code: str = Field(..., min_length=1, max_length=16)
    market: str = Field(..., min_length=1, max_length=16)
    evidence_engine_version: ResearchVersionIdentifier
    claim_policy_version: ResearchVersionIdentifier
    as_of: datetime
    available_at: datetime
    status: ResearchEvidenceStatus
    coverage: float = Field(..., ge=0.0, le=1.0)
    claim_count: int = Field(..., ge=0, le=32)
    citation_count: int = Field(..., ge=0, le=16)
    input_dataset_hashes: List[ResearchSha256] = Field(
        ...,
        min_length=1,
        max_length=MAX_FACTOR_DATASET_HASHES,
    )
    factor_snapshot_hash: ResearchSha256
    evidence_hash: ResearchSha256
    origin_job_id: Optional[ResearchOriginJobId] = None
    created_at: datetime

    @field_validator("evidence_engine_version", "claim_policy_version")
    @classmethod
    def validate_versions(cls, value: str) -> str:
        return strict_version_identifier(value, field="evidence version")

    @field_validator("origin_job_id")
    @classmethod
    def validate_origin_job_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return strict_public_identifier(value, field="origin_job_id")

    @field_validator("input_dataset_hashes")
    @classmethod
    def validate_dataset_hashes(cls, values: List[str]) -> List[str]:
        return _require_sorted_unique_hashes(
            values,
            field="input_dataset_hashes",
        )

    @model_validator(mode="after")
    def validate_times(self) -> "ResearchEvidenceSummary":
        if self.available_at > self.as_of:
            raise ValueError("evidence available_at cannot be after as_of")
        return self


class ResearchEvidenceDetailResponse(ResearchEvidenceSummary):
    evidence: ResearchEvidencePayload

    @model_validator(mode="after")
    def validate_projection(self) -> "ResearchEvidenceDetailResponse":
        evidence = self.evidence
        projections = {
            "stock_code": evidence.stock_code,
            "market": evidence.market,
            "evidence_engine_version": evidence.evidence_engine_version,
            "claim_policy_version": evidence.claim_policy_version,
            "as_of": evidence.as_of,
            "available_at": evidence.available_at,
            "status": evidence.status,
            "coverage": evidence.coverage,
            "input_dataset_hashes": evidence.input_dataset_hashes,
            "factor_snapshot_hash": evidence.factor_snapshot_hash,
            "claim_count": len(evidence.claims),
            "citation_count": len(evidence.citations),
        }
        mismatches = [
            field
            for field, expected in projections.items()
            if getattr(self, field) != expected
        ]
        if mismatches:
            raise ValueError("evidence summary does not match its payload")
        return self


class ResearchEvidenceListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: List[ResearchEvidenceSummary] = Field(default_factory=list)
    count: int = Field(..., ge=0)
    next_cursor: Optional[str] = Field(None, max_length=256)

    @field_validator("next_cursor")
    @classmethod
    def validate_next_cursor(cls, value: Optional[str]) -> Optional[str]:
        return _validate_public_cursor(value)

    @model_validator(mode="after")
    def validate_page(self) -> "ResearchEvidenceListResponse":
        if self.count != len(self.items):
            raise ValueError("count does not match items")
        hashes = [item.evidence_hash for item in self.items]
        _require_unique(hashes, field="evidence_hashes")
        return self


class ResearchDebateArgument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: ResearchEvidenceIdentifier
    statement: str = Field(..., min_length=1, max_length=1000)
    claim_ids: List[ResearchEvidenceIdentifier] = Field(
        ...,
        min_length=1,
        max_length=8,
    )
    citation_ids: List[ResearchEvidenceIdentifier] = Field(
        ...,
        min_length=1,
        max_length=8,
    )
    confidence: float = Field(..., ge=0.0, le=1.0)
    limitations: List[ResearchDebateArgumentLimitation] = Field(
        default_factory=list,
        max_length=4,
    )

    @field_validator("id")
    @classmethod
    def validate_public_id(cls, value: str) -> str:
        return strict_public_identifier(value, field="argument.id")

    @field_validator("claim_ids", "citation_ids")
    @classmethod
    def validate_reference_ids(cls, values: List[str]) -> List[str]:
        checked = [
            strict_public_identifier(value, field="debate reference id")
            for value in values
        ]
        if checked != sorted(set(checked)):
            raise ValueError("debate reference IDs must be sorted and unique")
        return checked

    @field_validator("statement")
    @classmethod
    def validate_statement(cls, value: str) -> str:
        validate_debate_prose(value, field="argument.statement")
        return value

    @field_validator("limitations")
    @classmethod
    def validate_argument_limitations(cls, values: List[str]) -> List[str]:
        for value in values:
            validate_debate_prose(value, field="argument.limitations")
        return _require_unique(values, field="argument.limitations")


class ResearchDebateTurn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    turn_hash: ResearchSha256
    prompt_fingerprint: ResearchSha256
    model_used: str = Field(..., min_length=1, max_length=128)
    stance: ResearchDebateStance
    summary: str = Field(..., min_length=1, max_length=1000)
    arguments: List[ResearchDebateArgument] = Field(
        ...,
        min_length=1,
        max_length=6,
    )
    open_questions: List[ResearchDebateOpenQuestion] = Field(
        default_factory=list,
        max_length=6,
    )

    @field_validator("model_used")
    @classmethod
    def validate_model_used(cls, value: str) -> str:
        return strict_model_identifier(value, field="model_used")

    @field_validator("summary")
    @classmethod
    def validate_summary(cls, value: str) -> str:
        validate_debate_prose(value, field="turn.summary")
        return value

    @field_validator("open_questions")
    @classmethod
    def validate_open_questions(cls, values: List[str]) -> List[str]:
        for value in values:
            validate_debate_prose(value, field="turn.open_questions")
        return _require_unique(values, field="turn.open_questions")

    @model_validator(mode="after")
    def validate_argument_ids(self) -> "ResearchDebateTurn":
        argument_ids = [argument.id for argument in self.arguments]
        _require_unique(argument_ids, field="turn argument IDs")
        return self


class ResearchDebateFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stance: ResearchDebateStance
    error_code: ResearchDebateErrorCode

    @field_validator("error_code")
    @classmethod
    def validate_error_code(cls, value: str) -> str:
        return strict_error_code(value, field="error_code")


class ResearchDebatePayload(BaseModel):
    """Strict, display-safe projection of one bounded debate artifact."""

    model_config = ConfigDict(extra="forbid")

    debate_engine_version: str = Field(..., min_length=1, max_length=64)
    output_schema_version: str = Field(..., min_length=1, max_length=64)
    prompt_version: str = Field(..., min_length=1, max_length=64)
    stock_code: str = Field(..., min_length=1, max_length=16)
    market: str = Field(..., min_length=1, max_length=16)
    as_of: datetime
    available_at: datetime
    status: ResearchDebateStatus
    evidence_snapshot_hash: ResearchSha256
    request_hash: ResearchSha256
    model_route_fingerprint: ResearchSha256
    bull_turn_hash: Optional[ResearchSha256] = None
    bear_turn_hash: Optional[ResearchSha256] = None
    failed_stances: List[ResearchDebateFailure] = Field(
        default_factory=list,
        max_length=2,
    )
    limitations: List[ResearchDebateLimitation] = Field(
        default_factory=list,
        max_length=8,
    )
    turns: List[ResearchDebateTurn] = Field(default_factory=list, max_length=2)

    @field_validator(
        "debate_engine_version",
        "output_schema_version",
        "prompt_version",
    )
    @classmethod
    def validate_versions(cls, value: str) -> str:
        return strict_version_identifier(value, field="debate version")

    @field_validator("limitations")
    @classmethod
    def validate_limitations(cls, values: List[str]) -> List[str]:
        for value in values:
            validate_debate_prose(value, field="debate.limitations")
        return _require_unique(values, field="debate.limitations")

    @model_validator(mode="after")
    def validate_graph(self) -> "ResearchDebatePayload":
        if self.available_at > self.as_of:
            raise ValueError("Debate available_at cannot be after as_of")

        turn_by_stance: Dict[str, ResearchDebateTurn] = {}
        for turn in self.turns:
            if turn.stance in turn_by_stance:
                raise ValueError("Debate turn stances must be unique")
            turn_by_stance[turn.stance] = turn
        failure_stances = [failure.stance for failure in self.failed_stances]
        _require_unique(failure_stances, field="failed_stances")
        canonical_order = ["bull", "bear"]
        if [turn.stance for turn in self.turns] != [
            stance for stance in canonical_order if stance in turn_by_stance
        ]:
            raise ValueError("Debate turns must use canonical stance order")
        if failure_stances != [
            stance for stance in canonical_order if stance in failure_stances
        ]:
            raise ValueError("Debate failures must use canonical stance order")
        if set(turn_by_stance).intersection(failure_stances):
            raise ValueError("one Debate stance cannot both succeed and fail")

        expected_counts = {
            "available": (2, 0),
            "partial": (1, 1),
            "empty": (0, 0),
            "generation_failed": (0, 2),
        }[self.status]
        if (len(self.turns), len(self.failed_stances)) != expected_counts:
            raise ValueError("Debate status does not match turns and failures")
        resolved = set(turn_by_stance).union(failure_stances)
        if self.status != "empty" and resolved != {"bull", "bear"}:
            raise ValueError("Debate must resolve both stances")

        expected_bull_hash = (
            turn_by_stance["bull"].turn_hash if "bull" in turn_by_stance else None
        )
        expected_bear_hash = (
            turn_by_stance["bear"].turn_hash if "bear" in turn_by_stance else None
        )
        if self.bull_turn_hash != expected_bull_hash:
            raise ValueError("bull_turn_hash does not match Debate turns")
        if self.bear_turn_hash != expected_bear_hash:
            raise ValueError("bear_turn_hash does not match Debate turns")
        return self


class ResearchDebateSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    stock_code: str = Field(..., min_length=1, max_length=16)
    market: str = Field(..., min_length=1, max_length=16)
    debate_engine_version: str = Field(..., min_length=1, max_length=64)
    output_schema_version: str = Field(..., min_length=1, max_length=64)
    prompt_version: str = Field(..., min_length=1, max_length=64)
    evidence_snapshot_hash: ResearchSha256
    request_hash: ResearchSha256
    model_route_fingerprint: ResearchSha256
    as_of: datetime
    available_at: datetime
    status: ResearchDebateStatus
    bull_turn_hash: Optional[ResearchSha256] = None
    bear_turn_hash: Optional[ResearchSha256] = None
    bull_argument_count: int = Field(..., ge=0, le=6)
    bear_argument_count: int = Field(..., ge=0, le=6)
    open_question_count: int = Field(..., ge=0, le=12)
    debate_hash: ResearchSha256
    origin_job_id: Optional[ResearchOriginJobId] = None
    created_at: datetime

    @field_validator(
        "debate_engine_version",
        "output_schema_version",
        "prompt_version",
    )
    @classmethod
    def validate_versions(cls, value: str) -> str:
        return strict_version_identifier(value, field="debate version")

    @field_validator("origin_job_id")
    @classmethod
    def validate_origin_job_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return strict_public_identifier(value, field="origin_job_id")

    @model_validator(mode="after")
    def validate_times(self) -> "ResearchDebateSummary":
        if self.available_at > self.as_of:
            raise ValueError("Debate available_at cannot be after as_of")
        return self


class ResearchDebateDetailResponse(ResearchDebateSummary):
    debate: ResearchDebatePayload

    @model_validator(mode="after")
    def validate_projection(self) -> "ResearchDebateDetailResponse":
        debate = self.debate
        bull_arguments = sum(
            len(turn.arguments) for turn in debate.turns if turn.stance == "bull"
        )
        bear_arguments = sum(
            len(turn.arguments) for turn in debate.turns if turn.stance == "bear"
        )
        open_questions = sum(len(turn.open_questions) for turn in debate.turns)
        projections = {
            "stock_code": debate.stock_code,
            "market": debate.market,
            "debate_engine_version": debate.debate_engine_version,
            "output_schema_version": debate.output_schema_version,
            "prompt_version": debate.prompt_version,
            "evidence_snapshot_hash": debate.evidence_snapshot_hash,
            "request_hash": debate.request_hash,
            "model_route_fingerprint": debate.model_route_fingerprint,
            "as_of": debate.as_of,
            "available_at": debate.available_at,
            "status": debate.status,
            "bull_turn_hash": debate.bull_turn_hash,
            "bear_turn_hash": debate.bear_turn_hash,
            "bull_argument_count": bull_arguments,
            "bear_argument_count": bear_arguments,
            "open_question_count": open_questions,
        }
        mismatches = [
            field
            for field, expected in projections.items()
            if getattr(self, field) != expected
        ]
        if mismatches:
            raise ValueError("Debate summary does not match its payload")
        return self


class ResearchDebateListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: List[ResearchDebateSummary] = Field(default_factory=list)
    count: int = Field(..., ge=0)
    next_cursor: Optional[str] = Field(None, max_length=256)

    @field_validator("next_cursor")
    @classmethod
    def validate_next_cursor(cls, value: Optional[str]) -> Optional[str]:
        return _validate_public_cursor(value)

    @model_validator(mode="after")
    def validate_page(self) -> "ResearchDebateListResponse":
        if self.count != len(self.items):
            raise ValueError("count does not match items")
        hashes = [item.debate_hash for item in self.items]
        _require_unique(hashes, field="debate_hashes")
        return self


class ResearchDatasetItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    dataset: str
    scope_type: str
    scope_value: str
    market: str
    provider: str
    schema_version: ResearchVersionIdentifier
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
    content_hash: ResearchSha256
    raw_ref: Optional[Dict[str, Any]] = None
    error_code: Optional[ResearchDebateErrorCode] = None
    error_message: Optional[str] = Field(None, max_length=500)
    supersedes_hash: Optional[ResearchSha256] = None
    origin_job_id: Optional[ResearchOriginJobId] = None
    created_at: datetime

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        return strict_version_identifier(value, field="schema_version")

    @field_validator("error_code")
    @classmethod
    def validate_error_code(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return strict_error_code(value, field="error_code")

    @field_validator("error_message")
    @classmethod
    def validate_error_message(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        if sanitize_error_message(value) != value:
            raise ValueError("error_message is not sanitized")
        return value

    @field_validator("origin_job_id")
    @classmethod
    def validate_origin_job_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return strict_public_identifier(value, field="origin_job_id")


class ResearchDatasetListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: List[ResearchDatasetItem] = Field(default_factory=list)
    count: int = Field(..., ge=0)
    detail: bool = False
    row_limit: int = Field(..., ge=1, le=6000)

    @model_validator(mode="after")
    def validate_page(self) -> "ResearchDatasetListResponse":
        if self.count != len(self.items):
            raise ValueError("count does not match items")
        hashes = [item.content_hash for item in self.items]
        _require_unique(hashes, field="dataset content hashes")
        return self


class ResearchFactorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    stock_code: str = Field(..., min_length=1, max_length=16)
    market: str = Field(..., min_length=1, max_length=16)
    company_profile: str = Field(..., min_length=1, max_length=32)
    primary_horizon: int = Field(..., gt=0)
    requested_horizon: ResearchHorizonDays
    requested_trend: Optional[Dict[str, Any]] = None
    engine_bundle_version: ResearchVersionIdentifier
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    trend_score: Optional[float] = None
    catalyst_score: Optional[float] = None
    risk_penalty: Optional[float] = None
    factors: Dict[str, Any]
    input_dataset_hashes: List[ResearchSha256] = Field(
        ...,
        max_length=MAX_FACTOR_DATASET_HASHES,
    )
    status: ResearchDataStatus
    coverage: float = Field(..., ge=0.0, le=1.0)
    unknowns: List[Dict[str, str]] = Field(
        default_factory=list,
        max_length=MAX_FACTOR_UNKNOWNS,
    )
    as_of: datetime
    available_at: datetime
    content_hash: ResearchSha256
    origin_job_id: Optional[ResearchOriginJobId] = None
    created_at: datetime

    @field_validator("engine_bundle_version")
    @classmethod
    def validate_engine_version(cls, value: str) -> str:
        return strict_version_identifier(value, field="engine_bundle_version")

    @field_validator("origin_job_id")
    @classmethod
    def validate_origin_job_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return strict_public_identifier(value, field="origin_job_id")

    @model_validator(mode="after")
    def validate_public_factor_contract(self) -> "ResearchFactorResponse":
        validate_factor_contract(
            factor_payload=self.factors,
            unknowns=self.unknowns,
            input_dataset_hashes=self.input_dataset_hashes,
            as_of=self.as_of,
            available_at=self.available_at,
            value_score=self.value_score,
            quality_score=self.quality_score,
            trend_score=self.trend_score,
            catalyst_score=self.catalyst_score,
            risk_penalty=self.risk_penalty,
            require_canonical_references=True,
        )
        return self


class ResearchSnapshotResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    stock_code: str = Field(..., min_length=1, max_length=16)
    market: str = Field(..., min_length=1, max_length=16)
    snapshot_version: ResearchVersionIdentifier
    field_dictionary_version: ResearchVersionIdentifier
    factor_engine_version: ResearchVersionIdentifier
    pack_version: ResearchVersionIdentifier
    prompt_version: ResearchVersionIdentifier
    policy_version: ResearchVersionIdentifier
    model_route_fingerprint: ResearchPublicIdentifier
    as_of: datetime
    available_at: datetime
    status: ResearchDataStatus
    snapshot: Dict[str, Any]
    snapshot_hash: ResearchSha256
    factor_snapshot_hash: Optional[ResearchSha256] = None
    evidence_snapshot_hash: Optional[str] = Field(
        None,
        pattern=r"^[0-9a-f]{64}$",
    )
    debate_snapshot_hash: Optional[str] = Field(
        None,
        pattern=r"^[0-9a-f]{64}$",
    )
    origin_job_id: Optional[ResearchOriginJobId] = None
    created_at: datetime

    @field_validator(
        "snapshot_version",
        "field_dictionary_version",
        "factor_engine_version",
        "pack_version",
        "prompt_version",
        "policy_version",
    )
    @classmethod
    def validate_versions(cls, value: str) -> str:
        return strict_version_identifier(value, field="research snapshot version")

    @field_validator("model_route_fingerprint")
    @classmethod
    def validate_route_fingerprint(cls, value: str) -> str:
        return strict_public_identifier(value, field="model_route_fingerprint")

    @field_validator("origin_job_id")
    @classmethod
    def validate_origin_job_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return strict_public_identifier(value, field="origin_job_id")

    @field_validator("snapshot")
    @classmethod
    def validate_nested_research_artifacts(
        cls,
        value: Dict[str, Any],
    ) -> Dict[str, Any]:
        evidence = value.get("evidence")
        if evidence is not None:
            ResearchEvidencePayload.model_validate(evidence)
        debate = value.get("debate")
        if debate is not None:
            ResearchDebatePayload.model_validate(debate)
        return value

    @model_validator(mode="after")
    def validate_artifact_graph(self) -> "ResearchSnapshotResponse":
        if self.available_at > self.as_of:
            raise ValueError("Research available_at cannot be after as_of")
        factor_payload = self.snapshot.get("factors")
        evidence_payload = self.snapshot.get("evidence")
        debate_payload = self.snapshot.get("debate")
        if (factor_payload is None) != (self.factor_snapshot_hash is None):
            raise ValueError(
                "factors and factor_snapshot_hash must both be present or absent"
            )
        if (evidence_payload is None) != (self.evidence_snapshot_hash is None):
            raise ValueError(
                "evidence and evidence_snapshot_hash must both be present or absent"
            )
        if (debate_payload is None) != (self.debate_snapshot_hash is None):
            raise ValueError(
                "debate and debate_snapshot_hash must both be present or absent"
            )
        if debate_payload is not None and evidence_payload is None:
            raise ValueError("Debate requires a frozen Evidence payload")

        evidence: Optional[ResearchEvidencePayload] = None
        if evidence_payload is not None:
            evidence = ResearchEvidencePayload.model_validate(evidence_payload)
            if self.factor_snapshot_hash is None:
                raise ValueError("Evidence requires a frozen Factor payload")
            if evidence.factor_snapshot_hash != self.factor_snapshot_hash:
                raise ValueError("Evidence references a different Factor snapshot")
            if (
                evidence.stock_code != self.stock_code
                or evidence.market != self.market
                or evidence.as_of != self.as_of
                or evidence.available_at > self.available_at
            ):
                raise ValueError("Evidence identity conflicts with Research snapshot")

        if debate_payload is not None:
            debate = ResearchDebatePayload.model_validate(debate_payload)
            if evidence is None or self.evidence_snapshot_hash is None:
                raise ValueError("Debate requires a frozen Evidence payload")
            if debate.evidence_snapshot_hash != self.evidence_snapshot_hash:
                raise ValueError("Debate references a different Evidence snapshot")
            if (
                debate.stock_code != self.stock_code
                or debate.market != self.market
                or debate.as_of != self.as_of
                or debate.available_at > self.available_at
            ):
                raise ValueError("Debate identity conflicts with Research snapshot")

            claim_by_id = {claim.id: claim for claim in evidence.claims}
            citation_ids = {citation.id for citation in evidence.citations}
            for turn in debate.turns:
                for argument in turn.arguments:
                    if set(argument.claim_ids) - set(claim_by_id):
                        raise ValueError(
                            "Debate argument references unknown Evidence claims"
                        )
                    if set(argument.citation_ids) - citation_ids:
                        raise ValueError(
                            "Debate argument references unknown Evidence citations"
                        )
                    cited = set(argument.citation_ids)
                    reachable: set[str] = set()
                    for claim_id in argument.claim_ids:
                        claim_citations = set(
                            claim_by_id[claim_id].citation_ids
                        )
                        reachable.update(claim_citations)
                        if not cited.intersection(claim_citations):
                            raise ValueError(
                                "Debate claim has no reachable citation"
                            )
                    if not cited.issubset(reachable):
                        raise ValueError(
                            "Debate citations are not reachable from its claims"
                        )

            has_eligible_claims = any(
                claim.citation_ids for claim in evidence.claims
            )
            if has_eligible_claims == (debate.status == "empty"):
                raise ValueError("Debate status conflicts with citable Evidence")
        return self
