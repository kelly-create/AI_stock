"""Deterministic evaluator for the five personal-research Skill contracts.

The evaluator consumes only already-frozen Factor and Evidence objects.  It
never calls a provider or model, never fills a missing score with zero, and
binds every successful output to concrete claim/citation identifiers from the
same immutable Evidence snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from .canonical import canonical_hash, canonicalize
from .evidence_service import FrozenEvidenceSnapshot
from .personal_skill_contract import (
    PERSONAL_RESEARCH_SKILL_IDS,
    PersonalResearchSkillOutput,
    build_personal_research_skill_input,
    build_personal_research_skill_output,
    get_personal_research_skill_contract,
)
from .schemas import ComponentResult, ResearchFactorResult


PERSONAL_SKILL_EVALUATOR_VERSION = "personal-research-skill-evaluator-v1"
_FACTOR_POINTERS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "personal-value-quality": ("/value/score", "/quality/score"),
        "personal-trend-timing": ("/trend_timing/score",),
        "personal-catalyst": ("/catalyst/score",),
        "personal-risk": ("/risk/score",),
    }
)


class PersonalResearchSkillEvaluationError(RuntimeError):
    """Raised when frozen inputs cannot produce a contract-valid scorecard."""


@dataclass(frozen=True)
class PersonalResearchSkillScorecard:
    """Exactly five successful, lineage-bound deterministic Skill outputs."""

    evaluator_version: str
    stock_code: str
    market: str
    research_snapshot_hash: str
    factor_snapshot_hash: str
    evidence_snapshot_hash: str
    outputs: Mapping[str, PersonalResearchSkillOutput]
    scorecard_hash: str

    def decision_signal_fields(self) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        evidence_refs: set[str] = set()
        for skill_id in PERSONAL_RESEARCH_SKILL_IDS:
            output = self.outputs[skill_id]
            fields[output.skill_input.contract.score_field] = output.score
            evidence_refs.update(output.evidence_refs)
        fields["evidence_refs"] = sorted(evidence_refs)
        return fields


def _score(component: ComponentResult, *, field: str) -> float:
    if not isinstance(component, ComponentResult):
        raise TypeError(f"{field} must be a ComponentResult")
    value = component.score
    if value is None:
        raise PersonalResearchSkillEvaluationError(
            f"{field} score is unavailable in the frozen Factor snapshot"
        )
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PersonalResearchSkillEvaluationError(f"{field} score has an invalid type")
    normalized = float(value)
    if not math.isfinite(normalized) or not 0.0 <= normalized <= 100.0:
        raise PersonalResearchSkillEvaluationError(f"{field} score is out of range")
    return round(normalized, 4)


def _factor_evidence_refs(
    evidence: FrozenEvidenceSnapshot,
    *,
    factor_snapshot_hash: str,
    pointers: Sequence[str],
) -> tuple[str, ...]:
    pointer_set = set(pointers)
    citation_ids = {
        item.id
        for item in evidence.citations
        if item.artifact_type == "factor"
        and item.artifact_hash == factor_snapshot_hash
        and item.json_pointer in pointer_set
    }
    if len(citation_ids) != len(pointer_set):
        raise PersonalResearchSkillEvaluationError(
            "frozen Evidence does not contain every required factor citation"
        )
    claim_ids = {
        claim.id
        for claim in evidence.claims
        if claim.kind == "factor_metric"
        and claim.status in {"supported", "partial"}
        and set(claim.citation_ids).intersection(citation_ids)
    }
    referenced_citations = {
        citation_id
        for claim in evidence.claims
        if claim.id in claim_ids
        for citation_id in claim.citation_ids
        if citation_id in citation_ids
    }
    if referenced_citations != citation_ids or len(claim_ids) != len(pointer_set):
        raise PersonalResearchSkillEvaluationError(
            "required factor citations are not bound by exact frozen claims"
        )
    return tuple(sorted(claim_ids | citation_ids))


def _evidence_quality_refs(evidence: FrozenEvidenceSnapshot) -> tuple[str, ...]:
    citable_claims = tuple(
        sorted(
            claim.id
            for claim in evidence.claims
            if claim.status in {"supported", "partial", "contradicted"}
            and claim.citation_ids
        )
    )
    if not citable_claims:
        raise PersonalResearchSkillEvaluationError(
            "frozen Evidence contains no citable claim"
        )
    selected_claims = citable_claims[:16]
    selected_claim_set = set(selected_claims)
    selected_citations = tuple(
        sorted(
            {
                citation_id
                for claim in evidence.claims
                if claim.id in selected_claim_set
                for citation_id in claim.citation_ids
            }
        )[:16]
    )
    if not selected_citations:
        raise PersonalResearchSkillEvaluationError(
            "selected Evidence claims contain no citation"
        )
    return tuple(sorted((*selected_claims, *selected_citations)))


def _skill_input(
    *,
    skill_id: str,
    stock_code: str,
    market: str,
    research_snapshot_hash: str,
    factor_snapshot_hash: str,
    evidence_snapshot_hash: str,
):
    contract = get_personal_research_skill_contract(skill_id)
    payload: dict[str, Any] = {
        "skill_id": contract.skill_id,
        "skill_version": contract.version,
        "skill_content_hash": contract.content_hash,
        "stock_code": stock_code,
        "market": market,
        "research_snapshot_hash": research_snapshot_hash,
    }
    if "factor_snapshot_hash" in contract.required_snapshot_fields:
        payload["factor_snapshot_hash"] = factor_snapshot_hash
    if "evidence_snapshot_hash" in contract.required_snapshot_fields:
        payload["evidence_snapshot_hash"] = evidence_snapshot_hash
    return build_personal_research_skill_input(payload)


def build_personal_research_skill_scorecard(
    *,
    stock_code: str,
    market: str,
    research_snapshot_hash: str,
    factor_snapshot_hash: str,
    evidence_snapshot: FrozenEvidenceSnapshot,
    factors: ResearchFactorResult,
) -> PersonalResearchSkillScorecard:
    """Build the exact five-Skill scorecard from one frozen lineage graph."""

    if not isinstance(factors, ResearchFactorResult):
        raise TypeError("factors must be a ResearchFactorResult")
    if not isinstance(evidence_snapshot, FrozenEvidenceSnapshot):
        raise TypeError("evidence_snapshot must be a FrozenEvidenceSnapshot")
    normalized_stock = str(stock_code or "").strip()
    normalized_market = str(market or "").strip().casefold()
    if not normalized_stock or not normalized_market:
        raise ValueError("stock_code and market are required")
    if factors.stock_code != normalized_stock:
        raise ValueError("Factor stock_code differs from the scorecard stock_code")
    if (
        evidence_snapshot.stock_code != normalized_stock
        or evidence_snapshot.market.casefold() != normalized_market
    ):
        raise ValueError("Evidence stock/market differs from the scorecard identity")
    if evidence_snapshot.factor_snapshot_hash != factor_snapshot_hash:
        raise ValueError("Evidence and scorecard reference different Factor snapshots")

    value_score = _score(factors.value, field="value")
    quality_score = _score(factors.quality, field="quality")
    score_values = {
        "personal-value-quality": round((value_score + quality_score) / 2.0, 4),
        "personal-trend-timing": _score(factors.trend_timing, field="trend_timing"),
        "personal-catalyst": _score(factors.catalyst, field="catalyst"),
        "personal-risk": _score(factors.risk, field="risk"),
        "personal-evidence-quality": round(float(evidence_snapshot.coverage) * 100.0, 4),
    }
    if not math.isfinite(score_values["personal-evidence-quality"]) or not (
        0.0 <= score_values["personal-evidence-quality"] <= 100.0
    ):
        raise PersonalResearchSkillEvaluationError(
            "frozen Evidence coverage is invalid"
        )

    evidence_quality_refs = _evidence_quality_refs(evidence_snapshot)
    outputs: dict[str, PersonalResearchSkillOutput] = {}
    for skill_id in PERSONAL_RESEARCH_SKILL_IDS:
        skill_input = _skill_input(
            skill_id=skill_id,
            stock_code=normalized_stock,
            market=normalized_market,
            research_snapshot_hash=research_snapshot_hash,
            factor_snapshot_hash=factor_snapshot_hash,
            evidence_snapshot_hash=evidence_snapshot.evidence_hash,
        )
        if skill_id == "personal-evidence-quality":
            refs = evidence_quality_refs
            reasons = ("frozen-evidence-coverage",)
        else:
            refs = _factor_evidence_refs(
                evidence_snapshot,
                factor_snapshot_hash=factor_snapshot_hash,
                pointers=_FACTOR_POINTERS[skill_id],
            )
            reasons = ("frozen-factor-score",)
        outputs[skill_id] = build_personal_research_skill_output(
            skill_input,
            {
                "score": score_values[skill_id],
                "evidence_refs": refs,
                "reason_codes": reasons,
            },
        )

    identity = canonicalize(
        {
            "schema_version": "personal-research-skill-scorecard-v1",
            "evaluator_version": PERSONAL_SKILL_EVALUATOR_VERSION,
            "stock_code": normalized_stock,
            "market": normalized_market,
            "research_snapshot_hash": research_snapshot_hash,
            "factor_snapshot_hash": factor_snapshot_hash,
            "evidence_snapshot_hash": evidence_snapshot.evidence_hash,
            "outputs": {
                skill_id: outputs[skill_id].output_hash
                for skill_id in PERSONAL_RESEARCH_SKILL_IDS
            },
        },
        exclude_volatile=False,
    )
    return PersonalResearchSkillScorecard(
        evaluator_version=PERSONAL_SKILL_EVALUATOR_VERSION,
        stock_code=normalized_stock,
        market=normalized_market,
        research_snapshot_hash=research_snapshot_hash,
        factor_snapshot_hash=factor_snapshot_hash,
        evidence_snapshot_hash=evidence_snapshot.evidence_hash,
        outputs=MappingProxyType(outputs),
        scorecard_hash=canonical_hash(identity, exclude_volatile=False),
    )


def hydrate_personal_research_skill_scorecard(
    rows: Sequence[Any],
) -> PersonalResearchSkillScorecard:
    """Rebuild and validate a complete scorecard from immutable execution rows."""

    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise TypeError("rows must be a sequence of Skill execution records")
    if len(rows) != len(PERSONAL_RESEARCH_SKILL_IDS):
        raise PersonalResearchSkillEvaluationError(
            "resume requires exactly five personal Skill executions"
        )
    by_skill = {str(getattr(row, "skill_id", "") or ""): row for row in rows}
    if set(by_skill) != set(PERSONAL_RESEARCH_SKILL_IDS):
        raise PersonalResearchSkillEvaluationError(
            "resume Skill rows do not cover exactly the registered contracts"
        )

    outputs: dict[str, PersonalResearchSkillOutput] = {}
    common: tuple[str, str, str, str, str] | None = None
    for skill_id in PERSONAL_RESEARCH_SKILL_IDS:
        row = by_skill[skill_id]
        if str(getattr(row, "result_status", "") or "") != "succeeded":
            raise PersonalResearchSkillEvaluationError(
                f"resume Skill execution is not successful: {skill_id}"
            )
        try:
            raw_input = json.loads(str(row.canonical_input_json))
            raw_output = json.loads(str(row.canonical_output_json))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PersonalResearchSkillEvaluationError(
                f"resume Skill execution contains invalid JSON: {skill_id}"
            ) from exc
        if not isinstance(raw_input, Mapping) or not isinstance(raw_output, Mapping):
            raise PersonalResearchSkillEvaluationError(
                f"resume Skill execution payload is not an object: {skill_id}"
            )
        input_payload = {
            key: value
            for key, value in raw_input.items()
            if key != "schema_version"
        }
        skill_input = build_personal_research_skill_input(input_payload)
        if str(row.canonical_input_json) != skill_input.canonical_json:
            raise PersonalResearchSkillEvaluationError(
                f"resume Skill input is not canonical: {skill_id}"
            )
        output = build_personal_research_skill_output(
            skill_input,
            {
                "score": raw_output.get("score"),
                "evidence_refs": raw_output.get("evidence_refs"),
                "reason_codes": raw_output.get("reason_codes"),
            },
        )
        if str(row.canonical_output_json) != output.canonical_json:
            raise PersonalResearchSkillEvaluationError(
                f"resume Skill output is not canonical: {skill_id}"
            )
        expected_row = {
            "skill_version": skill_input.contract.version,
            "contract_hash": skill_input.contract.content_hash,
            "score_field": skill_input.contract.score_field,
            "research_snapshot_hash": skill_input.snapshot_refs[
                "research_snapshot_hash"
            ],
            "input_hash": skill_input.input_hash,
            "output_hash": output.output_hash,
            "score": output.score,
        }
        mismatched = sorted(
            field
            for field, expected in expected_row.items()
            if getattr(row, field) != expected
        )
        if mismatched:
            raise PersonalResearchSkillEvaluationError(
                f"resume Skill execution row conflicts with payload ({skill_id}): "
                + ",".join(mismatched)
            )
        try:
            dataset_hashes = json.loads(str(row.dataset_snapshot_hashes_json))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PersonalResearchSkillEvaluationError(
                f"resume Skill dataset lineage is invalid JSON: {skill_id}"
            ) from exc
        if (
            not isinstance(dataset_hashes, list)
            or not dataset_hashes
            or dataset_hashes != sorted(set(dataset_hashes))
            or any(
                not isinstance(item, str)
                or len(item) != 64
                or any(char not in "0123456789abcdef" for char in item)
                for item in dataset_hashes
            )
        ):
            raise PersonalResearchSkillEvaluationError(
                f"resume Skill dataset lineage is not canonical: {skill_id}"
            )
        dataset_lineage_hash = canonical_hash(
            {"dataset_snapshot_hashes": dataset_hashes},
            exclude_volatile=False,
        )
        if str(getattr(row, "dataset_lineage_hash", "") or "") != dataset_lineage_hash:
            raise PersonalResearchSkillEvaluationError(
                f"resume Skill dataset lineage hash differs: {skill_id}"
            )
        execution_identity = canonicalize(
            {
                "schema_version": "personal-research-skill-execution-v1",
                "task_id": str(getattr(row, "task_id", "") or ""),
                "stock_code": str(getattr(row, "stock_code", "") or ""),
                "market": str(getattr(row, "market", "") or ""),
                "skill_id": skill_id,
                "skill_version": skill_input.contract.version,
                "contract_hash": skill_input.contract.content_hash,
                "score_field": skill_input.contract.score_field,
                "research_snapshot_hash": str(row.research_snapshot_hash),
                "factor_snapshot_hash": str(row.factor_snapshot_hash),
                "evidence_snapshot_hash": str(row.evidence_snapshot_hash),
                "dataset_snapshot_hashes": dataset_hashes,
                "dataset_lineage_hash": dataset_lineage_hash,
                "input_hash": skill_input.input_hash,
                "result_status": "succeeded",
                "output_hash": output.output_hash,
                "score": output.score,
            },
            exclude_volatile=False,
        )
        if str(getattr(row, "execution_hash", "") or "") != canonical_hash(
            execution_identity,
            exclude_volatile=False,
        ):
            raise PersonalResearchSkillEvaluationError(
                f"resume Skill execution hash differs: {skill_id}"
            )
        current_common = (
            str(getattr(row, "stock_code", "") or ""),
            str(getattr(row, "market", "") or ""),
            str(getattr(row, "research_snapshot_hash", "") or ""),
            str(getattr(row, "factor_snapshot_hash", "") or ""),
            str(getattr(row, "evidence_snapshot_hash", "") or ""),
        )
        if common is None:
            common = current_common
        elif current_common != common:
            raise PersonalResearchSkillEvaluationError(
                "resume Skill executions do not share one immutable lineage"
            )
        outputs[skill_id] = output

    assert common is not None
    stock_code, market, research_hash, factor_hash, evidence_hash = common
    identity = canonicalize(
        {
            "schema_version": "personal-research-skill-scorecard-v1",
            "evaluator_version": PERSONAL_SKILL_EVALUATOR_VERSION,
            "stock_code": stock_code,
            "market": market,
            "research_snapshot_hash": research_hash,
            "factor_snapshot_hash": factor_hash,
            "evidence_snapshot_hash": evidence_hash,
            "outputs": {
                skill_id: outputs[skill_id].output_hash
                for skill_id in PERSONAL_RESEARCH_SKILL_IDS
            },
        },
        exclude_volatile=False,
    )
    return PersonalResearchSkillScorecard(
        evaluator_version=PERSONAL_SKILL_EVALUATOR_VERSION,
        stock_code=stock_code,
        market=market,
        research_snapshot_hash=research_hash,
        factor_snapshot_hash=factor_hash,
        evidence_snapshot_hash=evidence_hash,
        outputs=MappingProxyType(outputs),
        scorecard_hash=canonical_hash(identity, exclude_volatile=False),
    )


__all__ = [
    "PERSONAL_SKILL_EVALUATOR_VERSION",
    "PersonalResearchSkillEvaluationError",
    "PersonalResearchSkillScorecard",
    "build_personal_research_skill_scorecard",
    "hydrate_personal_research_skill_scorecard",
]
