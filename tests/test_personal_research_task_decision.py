from dataclasses import replace

from src.services.research.personal_task_decision import (
    derive_personal_research_task_decision,
)
from tests.test_personal_research_skill_evaluator import _evidence, _factors


def test_low_conflict_standard_task_does_not_debate_when_capability_is_on() -> None:
    factors = _factors()
    result = derive_personal_research_task_decision(
        requested_mode="standard",
        priority=50,
        debate_enabled=True,
        factors=factors,
        evidence_snapshot=_evidence(factors=factors),
    )

    assert result.mode.resolved_mode == "standard"
    assert result.debate.triggered is False
    assert result.debate.reason_codes == ("trigger_conditions_not_met",)
    assert result.decision_hash == derive_personal_research_task_decision(
        requested_mode="standard",
        priority=50,
        debate_enabled=True,
        factors=factors,
        evidence_snapshot=_evidence(factors=factors),
    ).decision_hash


def test_explicit_debate_and_high_significance_use_same_frozen_prerequisites() -> None:
    factors = _factors()
    evidence = _evidence(factors=factors)
    explicit = derive_personal_research_task_decision(
        requested_mode="debate",
        priority=1,
        debate_enabled=True,
        factors=factors,
        evidence_snapshot=evidence,
    )
    significant = derive_personal_research_task_decision(
        requested_mode="standard",
        priority=80,
        debate_enabled=True,
        factors=factors,
        evidence_snapshot=evidence,
    )

    assert explicit.debate.triggered is True
    assert "explicit_debate_mode" in explicit.debate.reason_codes
    assert significant.debate.triggered is True
    assert "material_significance" in significant.debate.reason_codes


def test_missing_factor_is_recorded_and_never_inserted_as_zero() -> None:
    factors = _factors(trend=None)
    result = derive_personal_research_task_decision(
        requested_mode="standard",
        priority=50,
        debate_enabled=True,
        factors=factors,
        evidence_snapshot=_evidence(factors=factors),
    )

    assert result.missing_score_fields == ("trend_timing",)
    assert all(name != "trend_timing" for name, _score in result.conflict_components)
    assert all(score != 0.0 for _name, score in result.conflict_components)


def test_uncitable_evidence_fails_closed_even_for_explicit_debate() -> None:
    factors = _factors()
    evidence = _evidence(factors=factors)
    claims = tuple(
        replace(claim, status="insufficient", citation_ids=(), limitations=("missing",))
        for claim in evidence.claims
    )
    result = derive_personal_research_task_decision(
        requested_mode="debate",
        priority=100,
        debate_enabled=True,
        factors=factors,
        evidence_snapshot=replace(evidence, claims=claims),
    )

    assert result.has_citable_evidence is False
    assert result.debate.triggered is False
    assert "citable_evidence_missing" in result.debate.reason_codes


def test_explicit_debate_accepts_citable_partial_evidence_but_auto_does_not() -> None:
    factors = _factors()
    partial = replace(_evidence(factors=factors), coverage=0.333333)

    explicit = derive_personal_research_task_decision(
        requested_mode="debate",
        priority=1,
        debate_enabled=True,
        factors=factors,
        evidence_snapshot=partial,
    )
    automatic = derive_personal_research_task_decision(
        requested_mode="standard",
        priority=100,
        debate_enabled=True,
        factors=factors,
        evidence_snapshot=partial,
    )

    assert explicit.debate.triggered is True
    assert "explicit_debate_mode" in explicit.debate.reason_codes
    assert automatic.debate.triggered is False
    assert "evidence_quality_below_minimum" in automatic.debate.reason_codes
