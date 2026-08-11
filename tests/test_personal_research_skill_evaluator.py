from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from src.services.research.evidence_service import (
    build_evidence_snapshot,
    build_research_evidence_input,
)
from src.services.research.personal_skill_evaluator import (
    PersonalResearchSkillEvaluationError,
    build_personal_research_skill_scorecard,
)
from src.services.research.schemas import (
    ComponentResult,
    ComponentStatus,
    ProfileResolution,
    ResearchFactorResult,
)


AS_OF = datetime(2026, 8, 10, 3, tzinfo=timezone.utc)
FACTOR_HASH = "b" * 64
RESEARCH_HASH = "c" * 64
DATASET_HASH = "d" * 64


def _component(name: str, score: float | None) -> ComponentResult:
    return ComponentResult(
        name=name,
        status=(ComponentStatus.AVAILABLE if score is not None else ComponentStatus.PARTIAL),
        score=score,
        coverage=1.0 if score is not None else 0.0,
        metrics=(),
        policy_version=f"{name}-v1",
    )


def _factors(*, trend: float | None = 66.0) -> ResearchFactorResult:
    return ResearchFactorResult(
        stock_code="600519",
        as_of=AS_OF.isoformat(),
        profile=ProfileResolution(
            profile="default",
            source="test",
            resolver_version="test-v1",
        ),
        value=_component("value", 72.0),
        quality=_component("quality", 82.0),
        trend_timing=_component("trend_timing", trend),
        catalyst=_component("catalyst", 55.0),
        risk=_component("risk", 20.0),
        policy_version="factor-policy-v1",
    )


def _evidence(*, factors: ResearchFactorResult | None = None):
    factor_result = factors or _factors()
    factor_payload = factor_result.to_dict()
    build_input = build_research_evidence_input(
        stock_code="600519",
        market="A",
        as_of=AS_OF,
        datasets={
            "daily": {
                "dataset": "daily",
                "status": "available",
                "available_at": AS_OF - timedelta(hours=1),
                "content_hash": DATASET_HASH,
                "content_hashes": [DATASET_HASH],
                "rows": [{"close": 1500.0}],
            }
        },
        factors=factor_payload,
        factor_snapshot_hash=FACTOR_HASH,
    )
    return build_evidence_snapshot(build_input)


def _scorecard(*, factors: ResearchFactorResult | None = None, evidence=None):
    factor_result = factors or _factors()
    return build_personal_research_skill_scorecard(
        stock_code="600519",
        market="A",
        research_snapshot_hash=RESEARCH_HASH,
        factor_snapshot_hash=FACTOR_HASH,
        evidence_snapshot=evidence or _evidence(factors=factor_result),
        factors=factor_result,
    )


def test_scorecard_uses_exact_frozen_formulas_and_is_deterministic() -> None:
    first = _scorecard()
    second = _scorecard()

    assert first.scorecard_hash == second.scorecard_hash
    assert first.decision_signal_fields() == {
        "value_quality_score": 77.0,
        "trend_timing_score": 66.0,
        "catalyst_score": 55.0,
        "risk_score": 20.0,
        "evidence_quality_score": 100.0,
        "evidence_refs": first.decision_signal_fields()["evidence_refs"],
    }
    assert len(first.outputs) == 5
    assert all(output.evidence_refs for output in first.outputs.values())
    assert all(
        ref.startswith(("claim_", "citation_"))
        for ref in first.decision_signal_fields()["evidence_refs"]
    )


def test_scorecard_does_not_turn_a_missing_factor_into_zero() -> None:
    factors = _factors(trend=None)
    evidence = _evidence(factors=factors)

    with pytest.raises(PersonalResearchSkillEvaluationError, match="trend_timing score"):
        _scorecard(factors=factors, evidence=evidence)


def test_scorecard_rejects_missing_factor_claim_or_citation() -> None:
    evidence = _evidence()
    trend_citation = next(
        item for item in evidence.citations if item.json_pointer == "/trend_timing/score"
    )
    citations = tuple(item for item in evidence.citations if item != trend_citation)
    claims = tuple(
        replace(claim, status="insufficient", citation_ids=(), limitations=("missing",))
        if trend_citation.id in claim.citation_ids
        else claim
        for claim in evidence.claims
    )
    malformed = replace(evidence, citations=citations, claims=claims)

    with pytest.raises(PersonalResearchSkillEvaluationError, match="factor citation"):
        _scorecard(evidence=malformed)


def test_scorecard_rejects_cross_snapshot_factor_lineage() -> None:
    evidence = replace(_evidence(), factor_snapshot_hash="e" * 64)
    with pytest.raises(ValueError, match="different Factor snapshots"):
        _scorecard(evidence=evidence)


def test_evidence_quality_skill_requires_a_citable_claim() -> None:
    evidence = _evidence()
    claims = tuple(
        replace(claim, status="insufficient", citation_ids=(), limitations=("missing",))
        for claim in evidence.claims
    )
    malformed = replace(evidence, claims=claims)

    with pytest.raises(PersonalResearchSkillEvaluationError, match="no citable claim"):
        _scorecard(evidence=malformed)
