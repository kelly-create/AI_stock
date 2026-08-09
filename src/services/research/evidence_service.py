"""Deterministic evidence graph construction for personal research.

Evidence is a small, immutable DAG.  Claims point to citations and citations
point only to already-frozen dataset/factor hashes.  The builder never performs
network or storage I/O and validates every time and JSON-pointer boundary before
the graph can be persisted or formatted for an LLM.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import math
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Optional, Sequence, TYPE_CHECKING

from src.services.untrusted_external_content import (
    UNTRUSTED_EXTERNAL_CONTENT_BEGIN,
    UNTRUSTED_EXTERNAL_CONTENT_END,
    escape_untrusted_external_content,
)

from .canonical import canonical_hash, canonical_json as encode_canonical_json, canonicalize
from .debate_security import strict_public_identifier, strict_version_identifier
from .evidence_security import (
    MAX_EVIDENCE_CLAIMS,
    MAX_EVIDENCE_EXCERPT_CHARS,
    MAX_EVIDENCE_ITEMS,
    MAX_EVIDENCE_PROMPT_CHARS,
    MAX_EVIDENCE_TITLE_CHARS,
    bounded_text,
    json_value_hash,
    require_aware_utc,
    require_identifier,
    require_sha256,
    safe_canonical_url,
    validate_json_pointer,
)

if TYPE_CHECKING:
    from .repositories import EvidenceSnapshotInput, LeaseFence, ResearchSnapshotRepository, SnapshotWriteResult


EVIDENCE_ENGINE_VERSION = "research-evidence-v1"
CLAIM_POLICY_VERSION = "research-claim-policy-v1"

EVIDENCE_RELATIONS = frozenset({"supports", "contradicts", "context"})
EVIDENCE_ARTIFACT_TYPES = frozenset({"dataset", "factor"})
RESEARCH_CLAIM_KINDS = frozenset({"factor_metric", "reported_event"})
RESEARCH_CLAIM_STATUSES = frozenset({"supported", "partial", "contradicted", "insufficient"})
EVIDENCE_SNAPSHOT_STATUSES = frozenset({"available", "partial", "empty", "fetch_failed"})

_MAX_STATEMENT_CHARS = 2_000
_MAX_LIMITATION_CHARS = 500
_MAX_LIMITATIONS = 32


def _required_text(value: Any, *, field_name: str, max_chars: int = 128) -> str:
    return bounded_text(value, field=field_name, max_chars=max_chars, required=True)


def _rfc3339(value: datetime) -> str:
    return require_aware_utc(value, field="datetime").isoformat().replace("+00:00", "Z")


def _repository_record_time(value: Any, *, field_name: str) -> datetime:
    """Read repository UTC-naive columns without weakening public time types."""

    if isinstance(value, datetime) and (value.tzinfo is None or value.utcoffset() is None):
        value = value.replace(tzinfo=timezone.utc)
    return require_aware_utc(value, field=field_name)


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _bounded_limitations(values: Iterable[Any], *, field_name: str = "limitations") -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for index, value in enumerate(values):
        text = bounded_text(
            value,
            field=f"{field_name}[{index}]",
            max_chars=_MAX_LIMITATION_CHARS,
            required=False,
        )
        if text and text not in seen:
            normalized.append(text)
            seen.add(text)
        if len(normalized) >= _MAX_LIMITATIONS:
            break
    return tuple(normalized)


@dataclass(frozen=True)
class EvidenceArtifact:
    """A trusted in-memory projection of one immutable research artifact."""

    artifact_type: str
    artifact_hash: str
    stock_code: str
    available_at: datetime
    payload: Any
    lineage_hashes: tuple[str, ...] = ()
    source_name: str = ""

    def __post_init__(self) -> None:
        artifact_type = str(self.artifact_type or "").strip().casefold()
        if artifact_type not in EVIDENCE_ARTIFACT_TYPES:
            raise ValueError(f"unsupported evidence artifact_type: {self.artifact_type!r}")
        object.__setattr__(self, "artifact_type", artifact_type)
        object.__setattr__(
            self,
            "artifact_hash",
            require_sha256(self.artifact_hash, field="artifact_hash"),
        )
        object.__setattr__(
            self,
            "stock_code",
            _required_text(self.stock_code, field_name="stock_code", max_chars=64),
        )
        object.__setattr__(
            self,
            "available_at",
            require_aware_utc(self.available_at, field="artifact.available_at"),
        )
        payload = canonicalize(self.payload, exclude_volatile=False)
        object.__setattr__(self, "payload", _deep_freeze(payload))
        lineage = tuple(
            sorted(
                {
                    require_sha256(item, field=f"lineage_hashes[{index}]")
                    for index, item in enumerate(self.lineage_hashes)
                }
            )
        )
        object.__setattr__(self, "lineage_hashes", lineage)
        object.__setattr__(
            self,
            "source_name",
            bounded_text(
                self.source_name,
                field="artifact.source_name",
                max_chars=160,
                required=False,
            ),
        )


@dataclass(frozen=True)
class EvidenceCitation:
    id: str
    relation: str
    artifact_type: str
    artifact_hash: str
    json_pointer: str
    value_hash: str
    available_at: datetime
    source_name: str
    title: str
    excerpt: str
    canonical_url: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "id",
            strict_public_identifier(self.id, field="citation.id"),
        )
        relation = str(self.relation or "").strip().casefold()
        if relation not in EVIDENCE_RELATIONS:
            raise ValueError(f"unsupported evidence relation: {self.relation!r}")
        object.__setattr__(self, "relation", relation)
        artifact_type = str(self.artifact_type or "").strip().casefold()
        if artifact_type not in EVIDENCE_ARTIFACT_TYPES:
            raise ValueError(f"unsupported evidence artifact_type: {self.artifact_type!r}")
        object.__setattr__(self, "artifact_type", artifact_type)
        object.__setattr__(
            self,
            "artifact_hash",
            require_sha256(self.artifact_hash, field="citation.artifact_hash"),
        )
        object.__setattr__(
            self,
            "json_pointer",
            validate_json_pointer(self.json_pointer, field="citation.json_pointer"),
        )
        object.__setattr__(
            self,
            "value_hash",
            require_sha256(self.value_hash, field="citation.value_hash"),
        )
        object.__setattr__(
            self,
            "available_at",
            require_aware_utc(self.available_at, field="citation.available_at"),
        )
        object.__setattr__(
            self,
            "source_name",
            _required_text(self.source_name, field_name="citation.source_name", max_chars=160),
        )
        object.__setattr__(
            self,
            "title",
            bounded_text(
                self.title,
                field="citation.title",
                max_chars=MAX_EVIDENCE_TITLE_CHARS,
                required=True,
            ),
        )
        object.__setattr__(
            self,
            "excerpt",
            bounded_text(
                self.excerpt,
                field="citation.excerpt",
                max_chars=MAX_EVIDENCE_EXCERPT_CHARS,
                required=True,
            ),
        )
        if self.canonical_url is not None:
            object.__setattr__(
                self,
                "canonical_url",
                safe_canonical_url(self.canonical_url, field="citation.canonical_url"),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "relation": self.relation,
            "artifact_type": self.artifact_type,
            "artifact_hash": self.artifact_hash,
            "json_pointer": self.json_pointer,
            "value_hash": self.value_hash,
            "available_at": _rfc3339(self.available_at),
            "source_name": self.source_name,
            "title": self.title,
            "excerpt": self.excerpt,
            "canonical_url": self.canonical_url,
        }


@dataclass(frozen=True)
class ResearchClaim:
    id: str
    kind: str
    statement: str
    status: str
    citation_ids: tuple[str, ...]
    limitations: tuple[str, ...]
    available_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "id",
            strict_public_identifier(self.id, field="claim.id"),
        )
        kind = str(self.kind or "").strip().casefold()
        if kind not in RESEARCH_CLAIM_KINDS:
            raise ValueError(f"unsupported research claim kind: {self.kind!r}")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(
            self,
            "statement",
            bounded_text(
                self.statement,
                field="claim.statement",
                max_chars=_MAX_STATEMENT_CHARS,
                required=True,
            ),
        )
        status = str(self.status or "").strip().casefold()
        if status not in RESEARCH_CLAIM_STATUSES:
            raise ValueError(f"unsupported research claim status: {self.status!r}")
        object.__setattr__(self, "status", status)
        citation_ids = tuple(
            strict_public_identifier(
                item,
                field=f"claim.citation_ids[{index}]",
            )
            for index, item in enumerate(self.citation_ids)
        )
        if len(citation_ids) != len(set(citation_ids)):
            raise ValueError("claim citation_ids must be unique")
        if len(citation_ids) > MAX_EVIDENCE_ITEMS:
            raise ValueError(f"claim citation_ids exceed {MAX_EVIDENCE_ITEMS}")
        object.__setattr__(self, "citation_ids", citation_ids)
        object.__setattr__(self, "limitations", _bounded_limitations(self.limitations, field_name="claim.limitations"))
        object.__setattr__(
            self,
            "available_at",
            require_aware_utc(self.available_at, field="claim.available_at"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "statement": self.statement,
            "status": self.status,
            "citation_ids": list(self.citation_ids),
            "limitations": list(self.limitations),
            "available_at": _rfc3339(self.available_at),
        }


@dataclass(frozen=True)
class EvidenceBuildInput:
    stock_code: str
    market: str
    as_of: datetime
    artifacts: tuple[EvidenceArtifact, ...]
    citations: tuple[EvidenceCitation, ...]
    claims: tuple[ResearchClaim, ...]
    status: str = "available"
    limitations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "stock_code",
            _required_text(self.stock_code, field_name="stock_code", max_chars=64),
        )
        object.__setattr__(
            self,
            "market",
            _required_text(self.market, field_name="market", max_chars=32),
        )
        object.__setattr__(self, "as_of", require_aware_utc(self.as_of, field="as_of"))
        object.__setattr__(self, "artifacts", tuple(self.artifacts))
        object.__setattr__(self, "citations", tuple(self.citations))
        object.__setattr__(self, "claims", tuple(self.claims))
        status = str(self.status or "").strip().casefold()
        if status not in EVIDENCE_SNAPSHOT_STATUSES:
            raise ValueError(f"unsupported evidence snapshot status: {self.status!r}")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "limitations", _bounded_limitations(self.limitations))


@dataclass(frozen=True)
class FrozenEvidenceSnapshot:
    stock_code: str
    market: str
    evidence_engine_version: str
    claim_policy_version: str
    as_of: datetime
    available_at: datetime
    status: str
    coverage: float
    citations: tuple[EvidenceCitation, ...]
    claims: tuple[ResearchClaim, ...]
    limitations: tuple[str, ...]
    canonical_payload: Mapping[str, Any]
    canonical_json: str
    evidence_hash: str
    input_dataset_hashes: tuple[str, ...]
    factor_snapshot_hash: str
    _artifacts: tuple[EvidenceArtifact, ...] = field(default=(), repr=False, compare=False)

    def to_repository_input(self) -> "EvidenceSnapshotInput":
        from .repositories import EvidenceSnapshotInput

        return EvidenceSnapshotInput(
            stock_code=self.stock_code,
            market=self.market,
            evidence_engine_version=self.evidence_engine_version,
            claim_policy_version=self.claim_policy_version,
            as_of=self.as_of,
            available_at=self.available_at,
            status=self.status,
            coverage=self.coverage,
            claim_count=len(self.claims),
            citation_count=len(self.citations),
            canonical_payload=self.canonical_payload,
            input_dataset_hashes=self.input_dataset_hashes,
            factor_snapshot_hash=self.factor_snapshot_hash,
        )

    def persist(
        self,
        repository: "ResearchSnapshotRepository",
        *,
        lease: "LeaseFence",
        now: Optional[datetime] = None,
    ) -> "SnapshotWriteResult":
        return repository.write_evidence(self.to_repository_input(), lease=lease, now=now)


def _artifact_registry(
    value: EvidenceBuildInput,
) -> tuple[dict[str, EvidenceArtifact], tuple[str, ...], str]:
    registry: dict[str, EvidenceArtifact] = {}
    factor_hashes: list[str] = []
    for index, artifact in enumerate(value.artifacts):
        if not isinstance(artifact, EvidenceArtifact):
            raise TypeError(f"artifacts[{index}] must be an EvidenceArtifact")
        if artifact.stock_code != value.stock_code:
            raise ValueError("evidence artifact stock_code does not match build input")
        if artifact.available_at > value.as_of:
            raise ValueError("evidence artifact available_at cannot be after as_of")
        existing = registry.get(artifact.artifact_hash)
        if existing is not None and existing != artifact:
            raise ValueError("one artifact hash resolves to multiple payloads")
        registry[artifact.artifact_hash] = artifact
        if artifact.artifact_type == "factor":
            factor_hashes.append(artifact.artifact_hash)
    unique_factor_hashes = tuple(sorted(set(factor_hashes)))
    if len(unique_factor_hashes) != 1:
        raise ValueError("an evidence snapshot must reference exactly one factor artifact")
    dataset_hashes = tuple(
        sorted(
            artifact_hash
            for artifact_hash, artifact in registry.items()
            if artifact.artifact_type == "dataset"
        )
    )
    dataset_set = set(dataset_hashes)
    for factor_hash in unique_factor_hashes:
        lineage = registry[factor_hash].lineage_hashes
        if not lineage:
            raise ValueError("factor evidence requires immutable dataset lineage")
        unknown_lineage = sorted(set(lineage) - dataset_set)
        if unknown_lineage:
            raise ValueError("factor evidence references unknown dataset lineage")
    return registry, dataset_hashes, unique_factor_hashes[0]


def _validate_graph(
    *,
    stock_code: str,
    as_of: datetime,
    citations: Sequence[EvidenceCitation],
    claims: Sequence[ResearchClaim],
    known_dataset_hashes: Sequence[str],
    factor_snapshot_hash: str,
    artifacts: Optional[Mapping[str, EvidenceArtifact]] = None,
) -> None:
    if len(citations) > MAX_EVIDENCE_ITEMS:
        raise ValueError(f"evidence citations exceed {MAX_EVIDENCE_ITEMS}")
    if len(claims) > MAX_EVIDENCE_CLAIMS:
        raise ValueError(f"research claims exceed {MAX_EVIDENCE_CLAIMS}")
    citation_by_id: dict[str, EvidenceCitation] = {}
    known_dataset_set = set(known_dataset_hashes)
    for citation in citations:
        if not isinstance(citation, EvidenceCitation):
            raise TypeError("every citation must be an EvidenceCitation")
        if citation.id in citation_by_id:
            raise ValueError(f"duplicate evidence citation id: {citation.id}")
        if citation.available_at > as_of:
            raise ValueError("citation available_at cannot be after as_of")
        if citation.artifact_type == "dataset":
            if citation.artifact_hash not in known_dataset_set:
                raise ValueError("citation references an unknown dataset hash")
        elif citation.artifact_hash != factor_snapshot_hash:
            raise ValueError("citation references an unknown factor hash")
        if artifacts is not None:
            artifact = artifacts.get(citation.artifact_hash)
            if artifact is None or artifact.artifact_type != citation.artifact_type:
                raise ValueError("citation artifact reference is missing or type-mismatched")
            if artifact.stock_code != stock_code:
                raise ValueError("citation artifact stock_code does not match snapshot")
            if citation.available_at != artifact.available_at:
                raise ValueError("citation available_at must equal its artifact boundary")
            expected_value_hash = json_value_hash(artifact.payload, citation.json_pointer)
            if expected_value_hash != citation.value_hash:
                raise ValueError("citation value_hash does not match its JSON pointer")
            if artifact.source_name and citation.source_name != artifact.source_name:
                raise ValueError("citation source_name does not match its artifact")
        citation_by_id[citation.id] = citation

    claim_ids: set[str] = set()
    referenced_citation_ids: set[str] = set()
    for claim in claims:
        if not isinstance(claim, ResearchClaim):
            raise TypeError("every claim must be a ResearchClaim")
        if claim.id in claim_ids:
            raise ValueError(f"duplicate research claim id: {claim.id}")
        claim_ids.add(claim.id)
        if claim.available_at > as_of:
            raise ValueError("claim available_at cannot be after as_of")
        try:
            claim_citations = [citation_by_id[citation_id] for citation_id in claim.citation_ids]
        except KeyError as exc:
            raise ValueError(f"claim references an unknown citation: {exc.args[0]}") from exc
        referenced_citation_ids.update(claim.citation_ids)
        if claim_citations and claim.available_at < max(item.available_at for item in claim_citations):
            raise ValueError("claim available_at cannot precede its citations")
        relations = {item.relation for item in claim_citations}
        if claim.status == "supported" and "supports" not in relations:
            raise ValueError("supported claims require a supporting citation")
        if claim.status == "supported" and "contradicts" in relations:
            raise ValueError("supported claims cannot ignore contradictory citations")
        if claim.status == "contradicted" and "contradicts" not in relations:
            raise ValueError("contradicted claims require a contradicting citation")
        if claim.status == "contradicted" and "supports" in relations:
            raise ValueError("contradicted claims cannot ignore supporting citations")
        if claim.status == "partial" and (not claim_citations or not claim.limitations):
            raise ValueError("partial claims require citations and limitations")
        if claim.status == "insufficient" and (claim_citations or not claim.limitations):
            raise ValueError("insufficient claims require limitations and no citations")
        if claim.kind == "factor_metric" and any(
            item.artifact_type != "factor" for item in claim_citations
        ):
            raise ValueError("factor_metric claims may cite only factor artifacts")
        if claim.kind == "reported_event":
            if not claim_citations or any(
                item.artifact_type != "dataset" or item.relation != "context"
                for item in claim_citations
            ):
                raise ValueError("reported_event claims require dataset context citations")
            attributed_sources = {
                item.source_name.casefold() for item in claim_citations if item.source_name
            }
            statement = claim.statement.casefold()
            if not any(source in statement for source in attributed_sources):
                raise ValueError("reported_event claim statement must attribute its source")
            if claim.status != "partial":
                raise ValueError("snippet-only reported_event claims must remain partial")

    orphaned = sorted(set(citation_by_id) - referenced_citation_ids)
    if orphaned:
        raise ValueError(f"orphan evidence citations are forbidden: {', '.join(orphaned)}")


def _coverage(claims: Sequence[ResearchClaim]) -> float:
    if not claims:
        return 0.0
    weights = {
        "supported": 1.0,
        "contradicted": 1.0,
        "partial": 0.5,
        "insufficient": 0.0,
    }
    return round(sum(weights[item.status] for item in claims) / len(claims), 6)


def _snapshot_status(requested: str, claims: Sequence[ResearchClaim], coverage: float) -> str:
    if not claims:
        return "fetch_failed" if requested == "fetch_failed" else "empty"
    if requested != "available" or coverage < 1.0:
        return "partial"
    return "available"


def _snapshot_payload(
    *,
    stock_code: str,
    market: str,
    evidence_engine_version: str,
    claim_policy_version: str,
    as_of: datetime,
    available_at: datetime,
    status: str,
    coverage: float,
    citations: Sequence[EvidenceCitation],
    claims: Sequence[ResearchClaim],
    limitations: Sequence[str],
    input_dataset_hashes: Sequence[str],
    factor_snapshot_hash: str,
) -> dict[str, Any]:
    return canonicalize(
        {
            "evidence_engine_version": evidence_engine_version,
            "claim_policy_version": claim_policy_version,
            "stock_code": stock_code,
            "market": market,
            "as_of": as_of,
            "available_at": available_at,
            "status": status,
            "coverage": coverage,
            "input_dataset_hashes": list(input_dataset_hashes),
            "factor_snapshot_hash": factor_snapshot_hash,
            "limitations": list(limitations),
            "claims": [item.to_dict() for item in claims],
            "citations": [item.to_dict() for item in citations],
        },
        exclude_volatile=False,
    )


def build_evidence_snapshot(
    build_input: EvidenceBuildInput,
    *,
    evidence_engine_version: str = EVIDENCE_ENGINE_VERSION,
    claim_policy_version: str = CLAIM_POLICY_VERSION,
) -> FrozenEvidenceSnapshot:
    """Validate and freeze one bounded evidence DAG without I/O."""

    if not isinstance(build_input, EvidenceBuildInput):
        raise TypeError("build_input must be an EvidenceBuildInput")
    engine_version = strict_version_identifier(
        evidence_engine_version,
        field="evidence_engine_version",
    )
    policy_version = strict_version_identifier(
        claim_policy_version,
        field="claim_policy_version",
    )
    registry, dataset_hashes, factor_hash = _artifact_registry(build_input)
    _validate_graph(
        stock_code=build_input.stock_code,
        as_of=build_input.as_of,
        citations=build_input.citations,
        claims=build_input.claims,
        known_dataset_hashes=dataset_hashes,
        factor_snapshot_hash=factor_hash,
        artifacts=registry,
    )
    times = [artifact.available_at for artifact in build_input.artifacts]
    times.extend(item.available_at for item in build_input.citations)
    times.extend(item.available_at for item in build_input.claims)
    available_at = max(times, default=build_input.as_of)
    if available_at > build_input.as_of:
        raise ValueError("evidence available_at cannot be after as_of")
    coverage = _coverage(build_input.claims)
    status = _snapshot_status(build_input.status, build_input.claims, coverage)
    payload = _snapshot_payload(
        stock_code=build_input.stock_code,
        market=build_input.market,
        evidence_engine_version=engine_version,
        claim_policy_version=policy_version,
        as_of=build_input.as_of,
        available_at=available_at,
        status=status,
        coverage=coverage,
        citations=build_input.citations,
        claims=build_input.claims,
        limitations=build_input.limitations,
        input_dataset_hashes=dataset_hashes,
        factor_snapshot_hash=factor_hash,
    )
    frozen = FrozenEvidenceSnapshot(
        stock_code=build_input.stock_code,
        market=build_input.market,
        evidence_engine_version=engine_version,
        claim_policy_version=policy_version,
        as_of=build_input.as_of,
        available_at=available_at,
        status=status,
        coverage=coverage,
        citations=build_input.citations,
        claims=build_input.claims,
        limitations=build_input.limitations,
        canonical_payload=_deep_freeze(payload),
        canonical_json=encode_canonical_json(payload, exclude_volatile=False),
        evidence_hash=canonical_hash(payload, exclude_volatile=False),
        input_dataset_hashes=dataset_hashes,
        factor_snapshot_hash=factor_hash,
        _artifacts=build_input.artifacts,
    )
    validate_evidence_snapshot(frozen)
    return frozen


def _validate_snapshot_payload(snapshot: FrozenEvidenceSnapshot) -> None:
    expected_payload = _snapshot_payload(
        stock_code=snapshot.stock_code,
        market=snapshot.market,
        evidence_engine_version=snapshot.evidence_engine_version,
        claim_policy_version=snapshot.claim_policy_version,
        as_of=snapshot.as_of,
        available_at=snapshot.available_at,
        status=snapshot.status,
        coverage=snapshot.coverage,
        citations=snapshot.citations,
        claims=snapshot.claims,
        limitations=snapshot.limitations,
        input_dataset_hashes=snapshot.input_dataset_hashes,
        factor_snapshot_hash=snapshot.factor_snapshot_hash,
    )
    if _plain(snapshot.canonical_payload) != expected_payload:
        raise ValueError("evidence canonical_payload does not match snapshot fields")
    expected_json = encode_canonical_json(expected_payload, exclude_volatile=False)
    if snapshot.canonical_json != expected_json:
        raise ValueError("evidence canonical_json does not match canonical_payload")
    expected_hash = canonical_hash(expected_payload, exclude_volatile=False)
    if snapshot.evidence_hash != expected_hash:
        raise ValueError("evidence_hash does not match canonical_payload")


def validate_evidence_snapshot(snapshot: FrozenEvidenceSnapshot) -> None:
    """Validate the frozen graph; dereference pointers when artifacts are present."""

    if not isinstance(snapshot, FrozenEvidenceSnapshot):
        raise TypeError("snapshot must be a FrozenEvidenceSnapshot")
    strict_version_identifier(
        snapshot.evidence_engine_version,
        field="evidence_engine_version",
    )
    strict_version_identifier(
        snapshot.claim_policy_version,
        field="claim_policy_version",
    )
    snapshot_as_of = require_aware_utc(snapshot.as_of, field="snapshot.as_of")
    available_at = require_aware_utc(snapshot.available_at, field="snapshot.available_at")
    if available_at > snapshot_as_of:
        raise ValueError("snapshot.available_at cannot be after as_of")
    if snapshot.status not in EVIDENCE_SNAPSHOT_STATUSES:
        raise ValueError("snapshot.status is invalid")
    if not math.isfinite(float(snapshot.coverage)) or not 0.0 <= float(snapshot.coverage) <= 1.0:
        raise ValueError("snapshot.coverage must be finite and between zero and one")
    expected_coverage = _coverage(snapshot.claims)
    if float(snapshot.coverage) != expected_coverage:
        raise ValueError("snapshot.coverage does not match claim statuses")
    expected_status = _snapshot_status(snapshot.status, snapshot.claims, expected_coverage)
    if expected_status != snapshot.status:
        raise ValueError("snapshot.status does not match its evidence coverage")
    dataset_hashes = tuple(
        require_sha256(item, field=f"input_dataset_hashes[{index}]")
        for index, item in enumerate(snapshot.input_dataset_hashes)
    )
    if dataset_hashes != tuple(sorted(set(dataset_hashes))):
        raise ValueError("input_dataset_hashes must be sorted and unique")
    factor_hash = require_sha256(snapshot.factor_snapshot_hash, field="factor_snapshot_hash")
    registry: Optional[dict[str, EvidenceArtifact]] = None
    if snapshot._artifacts:
        build_input = EvidenceBuildInput(
            stock_code=snapshot.stock_code,
            market=snapshot.market,
            as_of=snapshot.as_of,
            artifacts=snapshot._artifacts,
            citations=snapshot.citations,
            claims=snapshot.claims,
            status=snapshot.status,
            limitations=snapshot.limitations,
        )
        registry, derived_dataset_hashes, derived_factor_hash = _artifact_registry(build_input)
        if derived_dataset_hashes != dataset_hashes or derived_factor_hash != factor_hash:
            raise ValueError("snapshot artifact lineage does not match frozen hash references")
    _validate_graph(
        stock_code=snapshot.stock_code,
        as_of=snapshot_as_of,
        citations=snapshot.citations,
        claims=snapshot.claims,
        known_dataset_hashes=dataset_hashes,
        factor_snapshot_hash=factor_hash,
        artifacts=registry,
    )
    _validate_snapshot_payload(snapshot)


def _mapping_value(value: Any, *, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    converter = getattr(value, "to_dict", None)
    if callable(converter):
        converted = converter()
        if isinstance(converted, Mapping):
            return converted
    raise TypeError(f"{field_name} must be a mapping or expose to_dict()")


def _artifact_id(prefix: str, payload: Any) -> str:
    return f"{prefix}_{canonical_hash(payload, exclude_volatile=False)[:24]}"


def _factor_claims(
    *,
    artifact: EvidenceArtifact,
) -> tuple[tuple[EvidenceCitation, ...], tuple[ResearchClaim, ...]]:
    payload = _mapping_value(artifact.payload, field_name="factor payload")
    citations: list[EvidenceCitation] = []
    claims: list[ResearchClaim] = []
    for component_name in ("value", "quality", "trend_timing", "catalyst", "risk"):
        component = payload.get(component_name)
        if not isinstance(component, Mapping):
            continue
        component_status = str(component.get("status") or "missing").strip().casefold()
        score = component.get("score")
        claim_available_at = artifact.available_at
        if score is None:
            claims.append(
                ResearchClaim(
                    id=_artifact_id(
                        "claim",
                        {"artifact_hash": artifact.artifact_hash, "component": component_name},
                    ),
                    kind="factor_metric",
                    statement=f"{component_name} factor score is unavailable.",
                    status="insufficient",
                    citation_ids=(),
                    limitations=(f"factor_{component_name}_{component_status or 'missing'}",),
                    available_at=claim_available_at,
                )
            )
            continue
        pointer = f"/{component_name}/score"
        citation_id = _artifact_id(
            "citation",
            {"artifact_hash": artifact.artifact_hash, "json_pointer": pointer},
        )
        coverage = component.get("coverage")
        excerpt = f"{component_name}.score={score}; status={component_status}"
        if coverage is not None:
            excerpt += f"; coverage={coverage}"
        citations.append(
            EvidenceCitation(
                id=citation_id,
                relation="supports",
                artifact_type="factor",
                artifact_hash=artifact.artifact_hash,
                json_pointer=pointer,
                value_hash=json_value_hash(artifact.payload, pointer),
                available_at=artifact.available_at,
                source_name=artifact.source_name or "deterministic_factor_engine",
                title=f"{component_name} factor score",
                excerpt=excerpt,
            )
        )
        claim_status = "supported" if component_status == "available" else "partial"
        limitations = () if claim_status == "supported" else (f"factor_{component_name}_{component_status}",)
        claims.append(
            ResearchClaim(
                id=_artifact_id(
                    "claim",
                    {"artifact_hash": artifact.artifact_hash, "json_pointer": pointer},
                ),
                kind="factor_metric",
                statement=f"{component_name} factor score is {score}.",
                status=claim_status,
                citation_ids=(citation_id,),
                limitations=limitations,
                available_at=claim_available_at,
            )
        )
    return tuple(citations), tuple(claims)


def build_research_evidence_input(
    *,
    stock_code: str,
    market: str,
    as_of: datetime,
    datasets: Mapping[str, Any],
    factors: Any,
    factor_snapshot_hash: str,
    collection: Any = None,
) -> EvidenceBuildInput:
    """Adapt PR2 frozen datasets/factors plus an optional news collection."""

    boundary = require_aware_utc(as_of, field="as_of")
    normalized_stock = _required_text(stock_code, field_name="stock_code", max_chars=64)
    if not isinstance(datasets, Mapping):
        raise TypeError("datasets must be a mapping")
    factor_payload = _mapping_value(factors, field_name="factors")
    dataset_artifacts: list[EvidenceArtifact] = []
    dataset_hashes: set[str] = set()
    dataset_available_times: list[datetime] = []
    for dataset_name in sorted(datasets):
        item = datasets[dataset_name]
        if not isinstance(item, Mapping):
            raise TypeError(f"datasets[{dataset_name!r}] must be a mapping")
        available_at = require_aware_utc(
            item.get("available_at"),
            field=f"datasets.{dataset_name}.available_at",
        )
        if available_at > boundary:
            raise ValueError(f"datasets.{dataset_name}.available_at cannot be after as_of")
        raw_hashes = item.get("content_hashes") or ()
        if isinstance(raw_hashes, (str, bytes, bytearray)) or not isinstance(raw_hashes, Sequence):
            raise TypeError(f"datasets.{dataset_name}.content_hashes must be a sequence")
        hashes = set(raw_hashes)
        if item.get("content_hash") is not None:
            hashes.add(item["content_hash"])
        for raw_hash in sorted(hashes):
            content_hash = require_sha256(
                raw_hash,
                field=f"datasets.{dataset_name}.content_hash",
            )
            dataset_hashes.add(content_hash)
            dataset_artifacts.append(
                EvidenceArtifact(
                    artifact_type="dataset",
                    artifact_hash=content_hash,
                    stock_code=normalized_stock,
                    available_at=available_at,
                    # The immutable dataset hash is the provenance node.  The
                    # merged rows may cover hundreds of content hashes (for
                    # example CYQ history), so copying the same payload into
                    # every node would make memory grow quadratically.  Base
                    # datasets are not directly cited by this adapter; cited
                    # search datasets keep their payload in the collection
                    # artifact below.
                    payload={},
                    source_name=str(dataset_name),
                )
            )
        dataset_available_times.append(available_at)
    factor_hash = require_sha256(factor_snapshot_hash, field="factor_snapshot_hash")
    factor_available_at = max(dataset_available_times, default=boundary)
    factor_artifact = EvidenceArtifact(
        artifact_type="factor",
        artifact_hash=factor_hash,
        stock_code=normalized_stock,
        available_at=factor_available_at,
        payload=factor_payload,
        lineage_hashes=tuple(sorted(dataset_hashes)),
        source_name="deterministic_factor_engine",
    )
    artifacts: list[EvidenceArtifact] = [*dataset_artifacts, factor_artifact]
    citations, claims = _factor_claims(artifact=factor_artifact)
    all_citations = list(citations)
    all_claims = list(claims)
    limitations: list[str] = []
    requested_status = "available"
    if collection is not None:
        if str(getattr(collection, "stock_code", "")).strip() != normalized_stock:
            raise ValueError("evidence collection stock_code does not match research input")
        collection_as_of = require_aware_utc(getattr(collection, "as_of", None), field="collection.as_of")
        if collection_as_of != boundary:
            raise ValueError("evidence collection as_of does not match research input")
        artifact_builder = getattr(collection, "to_evidence_artifact", None)
        if not callable(artifact_builder):
            raise TypeError("collection must expose to_evidence_artifact()")
        collection_artifact = artifact_builder()
        if collection_artifact is not None:
            artifacts.append(collection_artifact)
        all_citations.extend(tuple(getattr(collection, "citations", ())))
        all_claims.extend(tuple(getattr(collection, "claims", ())))
        limitations.extend(tuple(getattr(collection, "limitations", ())))
        collection_status = str(getattr(collection, "status", "partial") or "partial").casefold()
        if collection_status != "available":
            requested_status = "partial" if all_claims else collection_status
    return EvidenceBuildInput(
        stock_code=normalized_stock,
        market=market,
        as_of=boundary,
        artifacts=tuple(artifacts),
        citations=tuple(all_citations),
        claims=tuple(all_claims),
        status=requested_status,
        limitations=tuple(limitations),
    )


def _citation_from_mapping(value: Mapping[str, Any]) -> EvidenceCitation:
    return EvidenceCitation(
        id=value.get("id"),
        relation=value.get("relation"),
        artifact_type=value.get("artifact_type"),
        artifact_hash=value.get("artifact_hash"),
        json_pointer=value.get("json_pointer"),
        value_hash=value.get("value_hash"),
        available_at=value.get("available_at"),
        source_name=value.get("source_name"),
        title=value.get("title"),
        excerpt=value.get("excerpt"),
        canonical_url=value.get("canonical_url"),
    )


def _claim_from_mapping(value: Mapping[str, Any]) -> ResearchClaim:
    return ResearchClaim(
        id=value.get("id"),
        kind=value.get("kind"),
        statement=value.get("statement"),
        status=value.get("status"),
        citation_ids=tuple(value.get("citation_ids") or ()),
        limitations=tuple(value.get("limitations") or ()),
        available_at=value.get("available_at"),
    )


def hydrate_evidence_snapshot(
    record_or_canonical_payload: Any,
    *,
    artifacts: Sequence[EvidenceArtifact] = (),
) -> FrozenEvidenceSnapshot:
    """Hydrate a persisted evidence record without search or provider calls."""

    if isinstance(record_or_canonical_payload, FrozenEvidenceSnapshot):
        validate_evidence_snapshot(record_or_canonical_payload)
        return record_or_canonical_payload
    if not isinstance(record_or_canonical_payload, Mapping):
        raise TypeError("evidence record must be a mapping")
    record = record_or_canonical_payload
    raw_payload: Any = record.get("evidence", record.get("canonical_payload", record))
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            raise ValueError("persisted evidence is invalid JSON") from exc
    if not isinstance(raw_payload, Mapping):
        raise TypeError("persisted evidence payload must be a mapping")
    payload = canonicalize(raw_payload, exclude_volatile=False)
    raw_citations = payload.get("citations")
    raw_claims = payload.get("claims")
    if not isinstance(raw_citations, Sequence) or isinstance(raw_citations, (str, bytes, bytearray)):
        raise TypeError("persisted evidence citations must be a sequence")
    if not isinstance(raw_claims, Sequence) or isinstance(raw_claims, (str, bytes, bytearray)):
        raise TypeError("persisted evidence claims must be a sequence")
    citations = tuple(_citation_from_mapping(item) for item in raw_citations if isinstance(item, Mapping))
    claims = tuple(_claim_from_mapping(item) for item in raw_claims if isinstance(item, Mapping))
    if len(citations) != len(raw_citations) or len(claims) != len(raw_claims):
        raise TypeError("persisted evidence claim/citation entries must be mappings")
    dataset_hashes = tuple(payload.get("input_dataset_hashes") or ())
    factor_hash = payload.get("factor_snapshot_hash")
    canonical_text = encode_canonical_json(payload, exclude_volatile=False)
    evidence_hash = canonical_hash(payload, exclude_volatile=False)
    frozen = FrozenEvidenceSnapshot(
        stock_code=_required_text(payload.get("stock_code"), field_name="stock_code", max_chars=64),
        market=_required_text(payload.get("market"), field_name="market", max_chars=32),
        evidence_engine_version=strict_version_identifier(
            payload.get("evidence_engine_version"),
            field="evidence_engine_version",
        ),
        claim_policy_version=strict_version_identifier(
            payload.get("claim_policy_version"),
            field="claim_policy_version",
        ),
        as_of=require_aware_utc(payload.get("as_of"), field="as_of"),
        available_at=require_aware_utc(payload.get("available_at"), field="available_at"),
        status=str(payload.get("status") or "").casefold(),
        coverage=float(payload.get("coverage")),
        citations=citations,
        claims=claims,
        limitations=_bounded_limitations(tuple(payload.get("limitations") or ())),
        canonical_payload=_deep_freeze(payload),
        canonical_json=canonical_text,
        evidence_hash=evidence_hash,
        input_dataset_hashes=dataset_hashes,
        factor_snapshot_hash=factor_hash,
        _artifacts=tuple(artifacts),
    )
    if record is not raw_payload:
        if record.get("evidence_hash") is not None and record.get("evidence_hash") != evidence_hash:
            raise ValueError("persisted evidence_hash does not match evidence payload")
        if record.get("claim_count") is not None and int(record["claim_count"]) != len(claims):
            raise ValueError("persisted claim_count does not match evidence payload")
        if record.get("citation_count") is not None and int(record["citation_count"]) != len(citations):
            raise ValueError("persisted citation_count does not match evidence payload")
        for key in (
            "stock_code",
            "market",
            "evidence_engine_version",
            "claim_policy_version",
            "status",
        ):
            if record.get(key) is not None and str(record[key]) != str(payload.get(key)):
                raise ValueError(f"persisted {key} does not match evidence payload")
        for key in ("as_of", "available_at"):
            if record.get(key) is not None and _repository_record_time(
                record[key],
                field_name=f"record.{key}",
            ) != require_aware_utc(payload.get(key), field=f"payload.{key}"):
                raise ValueError(f"persisted {key} does not match evidence payload")
        if record.get("coverage") is not None and float(record["coverage"]) != float(
            payload.get("coverage")
        ):
            raise ValueError("persisted coverage does not match evidence payload")
        if record.get("input_dataset_hashes") is not None and tuple(
            record["input_dataset_hashes"]
        ) != tuple(payload.get("input_dataset_hashes") or ()):
            raise ValueError("persisted input_dataset_hashes do not match evidence payload")
        if record.get("factor_snapshot_hash") is not None and str(
            record["factor_snapshot_hash"]
        ) != str(payload.get("factor_snapshot_hash")):
            raise ValueError("persisted factor_snapshot_hash does not match evidence payload")
    validate_evidence_snapshot(frozen)
    return frozen


def evidence_context_from_snapshot(snapshot: FrozenEvidenceSnapshot) -> Mapping[str, Any]:
    """Return the canonical evidence projection shared by every prompt path."""

    validate_evidence_snapshot(snapshot)
    return _deep_freeze(_plain(snapshot.canonical_payload))


def format_research_evidence_context(snapshot_or_context: Any) -> str:
    """Format evidence inside exactly one bounded untrusted-data sentinel."""

    if isinstance(snapshot_or_context, FrozenEvidenceSnapshot):
        context = evidence_context_from_snapshot(snapshot_or_context)
    elif isinstance(snapshot_or_context, Mapping):
        context = canonicalize(snapshot_or_context, exclude_volatile=False)
    else:
        raise TypeError("evidence context must be a FrozenEvidenceSnapshot or mapping")
    raw = encode_canonical_json(context, exclude_volatile=False)
    escaped = escape_untrusted_external_content(raw).strip()
    prefix = "\n".join(
        (
            "[Untrusted external data: research_evidence]",
            UNTRUSTED_EXTERNAL_CONTENT_BEGIN,
        )
    ) + "\n"
    suffix = f"\n{UNTRUSTED_EXTERNAL_CONTENT_END}"
    truncation = "\n[DSA_UNTRUSTED_EXTERNAL_DATA_TRUNCATED]"
    budget = MAX_EVIDENCE_PROMPT_CHARS - len(prefix) - len(suffix)
    if budget <= len(truncation):
        raise RuntimeError("research evidence prompt budget is invalid")
    if len(escaped) > budget:
        escaped = escaped[: budget - len(truncation)].rstrip() + truncation
    rendered = f"{prefix}{escaped}{suffix}"
    if len(rendered) > MAX_EVIDENCE_PROMPT_CHARS:
        raise RuntimeError("research evidence context exceeded its hard prompt cap")
    return rendered


__all__ = [
    "CLAIM_POLICY_VERSION",
    "EVIDENCE_ARTIFACT_TYPES",
    "EVIDENCE_ENGINE_VERSION",
    "EVIDENCE_RELATIONS",
    "EVIDENCE_SNAPSHOT_STATUSES",
    "EvidenceArtifact",
    "EvidenceBuildInput",
    "EvidenceCitation",
    "FrozenEvidenceSnapshot",
    "RESEARCH_CLAIM_KINDS",
    "RESEARCH_CLAIM_STATUSES",
    "ResearchClaim",
    "build_evidence_snapshot",
    "build_research_evidence_input",
    "evidence_context_from_snapshot",
    "format_research_evidence_context",
    "hydrate_evidence_snapshot",
    "validate_evidence_snapshot",
]
