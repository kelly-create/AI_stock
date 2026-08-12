from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import math

import pytest

from src.services.research.canonical import canonical_hash
from src.services.research.debate_review import (
    DebateJudgePolicy,
    judge_bounded_debate,
    verify_bounded_debate,
)
from src.services.research.debate_service import (
    DebateBuildInput,
    DebateTurnBuildInput,
    build_debate_request,
    build_debate_snapshot,
    build_debate_turn,
)
from src.services.research.evidence_service import (
    build_evidence_snapshot,
    build_research_evidence_input,
)
from src.services.research.personal_skill_contract import (
    PERSONAL_RESEARCH_SCORE_FIELDS,
    PERSONAL_RESEARCH_SKILL_CONTRACTS,
    PERSONAL_RESEARCH_SKILL_IDS,
    build_personal_research_skill_input,
    build_personal_research_skill_output,
    get_personal_research_skill_contract,
)
from src.services.research.research_task_policy import (
    DebateTriggerPolicy,
    evaluate_debate_trigger,
    resolve_research_task_mode,
)


AS_OF = datetime(2025, 7, 1, 8, tzinfo=timezone.utc)
AVAILABLE_AT = AS_OF - timedelta(hours=1)
RESEARCH_HASH = "1" * 64
FACTOR_HASH = "2" * 64
EVIDENCE_HASH = "3" * 64
DATASET_HASH = "a" * 64
ROUTE_HASH = "c" * 64


def _skill_input_payload(skill_id: str) -> dict:
    contract = get_personal_research_skill_contract(skill_id)
    payload = {
        "skill_id": contract.skill_id,
        "skill_version": contract.version,
        "skill_content_hash": contract.content_hash,
        "stock_code": "600519",
        "market": "CN",
        "research_snapshot_hash": RESEARCH_HASH,
    }
    if "factor_snapshot_hash" in contract.required_snapshot_fields:
        payload["factor_snapshot_hash"] = FACTOR_HASH
    if "evidence_snapshot_hash" in contract.required_snapshot_fields:
        payload["evidence_snapshot_hash"] = EVIDENCE_HASH
    return payload


def _factor_payload() -> dict:
    return {
        "stock_code": "600519",
        "as_of": AS_OF,
        "value": {"status": "available", "score": 72.0, "coverage": 1.0},
        "quality": {"status": "available", "score": 81.0, "coverage": 1.0},
        "trend_timing": {
            "status": "available",
            "score": 66.0,
            "coverage": 1.0,
        },
        "catalyst": {"status": "available", "score": 55.0, "coverage": 1.0},
        "risk": {"status": "available", "score": 20.0, "coverage": 1.0},
    }


def _evidence():
    evidence_input = build_research_evidence_input(
        stock_code="600519",
        market="A",
        as_of=AS_OF,
        datasets={
            "daily": {
                "dataset": "daily",
                "status": "available",
                "available_at": AVAILABLE_AT,
                "content_hash": DATASET_HASH,
                "content_hashes": [DATASET_HASH],
                "rows": [{"close": 1500.0}],
            }
        },
        factors=_factor_payload(),
        factor_snapshot_hash=FACTOR_HASH,
    )
    return build_evidence_snapshot(evidence_input)


def _debate_snapshot(*, bull_confidence: float = 0.8, bear_confidence: float = 0.6):
    request = build_debate_request(
        _evidence(),
        model_route_fingerprint=ROUTE_HASH,
    )
    claim = request._evidence_snapshot.claims[0]

    def turn(stance: str, confidence: float):
        output = {
            "stance": stance,
            "summary": f"The cited Evidence supports the bounded {stance} case.",
            "arguments": [
                {
                    "id": f"{stance}_argument_1",
                    "statement": "The bounded metric is material to this case.",
                    "claim_ids": [claim.id],
                    "citation_ids": [claim.citation_ids[0]],
                    "confidence": confidence,
                    "limitations": ["Only the cited observation is considered."],
                }
            ],
            "open_questions": ["Will the cited metric remain stable?"],
        }
        return build_debate_turn(
            DebateTurnBuildInput(
                request=request,
                stance=stance,
                output=output,
                model_used="test-model",
            )
        )

    return build_debate_snapshot(
        DebateBuildInput(
            request=request,
            turns=(turn("bull", bull_confidence), turn("bear", bear_confidence)),
        )
    )


def test_five_skill_ids_and_score_fields_are_the_complete_approved_taxonomy() -> None:
    assert PERSONAL_RESEARCH_SKILL_IDS == (
        "personal-value-quality",
        "personal-trend-timing",
        "personal-catalyst",
        "personal-risk",
        "personal-evidence-quality",
    )
    assert tuple(PERSONAL_RESEARCH_SKILL_CONTRACTS) == PERSONAL_RESEARCH_SKILL_IDS
    assert tuple(
        contract.score_field for contract in PERSONAL_RESEARCH_SKILL_CONTRACTS.values()
    ) == PERSONAL_RESEARCH_SCORE_FIELDS
    assert all(contract.version == "1.0.0" for contract in PERSONAL_RESEARCH_SKILL_CONTRACTS.values())
    hashes = tuple(contract.content_hash for contract in PERSONAL_RESEARCH_SKILL_CONTRACTS.values())
    assert len(set(hashes)) == 5
    assert all(len(value) == 64 for value in hashes)


def test_skill_contract_content_hashes_are_stable_golden_values() -> None:
    assert {
        skill_id: contract.content_hash
        for skill_id, contract in PERSONAL_RESEARCH_SKILL_CONTRACTS.items()
    } == {
        # Golden values deliberately force an explicit version/content update
        # whenever a production Skill contract changes.
        "personal-value-quality": "ce0d52740afdf9f8ceb4eef5a498afefe4902917367a034e16192261aa57ea00",
        "personal-trend-timing": "9f3da98244212633dfb1d2e11c7c2c523e537c25042e064aaef9984622d2c1f1",
        "personal-catalyst": "1285ca2c03ff9f0da1a65fae50e23b2e68bd6c9e1112550bb14dc1e93c207d52",
        "personal-risk": "a36b4e4b400e23bd5d7eda918ae9ec620bb842c3b9376023388a96625a0683c5",
        "personal-evidence-quality": "a82c88e47abeafbdb007d3af0c0ae57c089456fdedb716ff4119fb73078c4d04",
    }


@pytest.mark.parametrize("skill_id", PERSONAL_RESEARCH_SKILL_IDS)
def test_skill_input_and_output_are_canonical_lineage_bound_and_projectable(skill_id: str) -> None:
    payload = _skill_input_payload(skill_id)
    skill_input = build_personal_research_skill_input(payload)
    reordered = build_personal_research_skill_input(dict(reversed(tuple(payload.items()))))

    assert skill_input.input_hash == reordered.input_hash
    assert skill_input.canonical_json == reordered.canonical_json
    assert skill_input.input_hash == canonical_hash(
        skill_input.canonical_payload,
        exclude_volatile=False,
    )

    result = build_personal_research_skill_output(
        skill_input,
        {
            "score": 72,
            "evidence_refs": ["claim_1", "claim_2"],
            "reason_codes": ["coverage_complete", "snapshot_bound"],
        },
    )
    assert result.score == 72.0
    assert result.output_hash == canonical_hash(
        result.canonical_payload,
        exclude_volatile=False,
    )
    assert result.to_decision_signal_fields() == {
        skill_input.contract.score_field: 72.0,
        "evidence_refs": ["claim_1", "claim_2"],
    }


def test_skill_input_rejects_missing_unknown_and_stale_contract_lineage() -> None:
    payload = _skill_input_payload("personal-value-quality")
    for missing in ("skill_id", "skill_version", "factor_snapshot_hash"):
        invalid = dict(payload)
        invalid.pop(missing)
        with pytest.raises(ValueError, match="missing required fields"):
            build_personal_research_skill_input(invalid)

    with pytest.raises(ValueError, match="unknown fields"):
        build_personal_research_skill_input({**payload, "trace_id": "volatile"})
    with pytest.raises(ValueError, match="skill_version"):
        build_personal_research_skill_input({**payload, "skill_version": "1.0.1"})
    with pytest.raises(ValueError, match="skill_content_hash"):
        build_personal_research_skill_input({**payload, "skill_content_hash": "0" * 64})
    with pytest.raises(ValueError, match="SHA-256"):
        build_personal_research_skill_input({**payload, "factor_snapshot_hash": "A" * 64})


@pytest.mark.parametrize("score", [True, "72", None])
def test_skill_output_rejects_non_numeric_or_bool_scores(score) -> None:
    skill_input = build_personal_research_skill_input(
        _skill_input_payload("personal-value-quality")
    )
    with pytest.raises(TypeError, match="finite number"):
        build_personal_research_skill_output(
            skill_input,
            {
                "score": score,
                "evidence_refs": ["claim_1"],
                "reason_codes": [],
            },
        )


@pytest.mark.parametrize("score", [-0.01, 100.01, math.nan, math.inf])
def test_skill_output_rejects_non_finite_or_out_of_range_scores(score: float) -> None:
    skill_input = build_personal_research_skill_input(
        _skill_input_payload("personal-value-quality")
    )
    with pytest.raises(ValueError):
        build_personal_research_skill_output(
            skill_input,
            {
                "score": score,
                "evidence_refs": ["claim_1"],
                "reason_codes": [],
            },
        )


def test_skill_output_rejects_missing_unknown_unsorted_and_overflow_fields() -> None:
    skill_input = build_personal_research_skill_input(
        _skill_input_payload("personal-value-quality")
    )
    with pytest.raises(ValueError, match="missing required fields"):
        build_personal_research_skill_output(
            skill_input,
            {"score": 72, "evidence_refs": ["claim_1"]},
        )
    with pytest.raises(ValueError, match="unknown fields"):
        build_personal_research_skill_output(
            skill_input,
            {
                "score": 72,
                "evidence_refs": ["claim_1"],
                "reason_codes": [],
                "action": "buy",
            },
        )
    with pytest.raises(ValueError, match="sorted and unique"):
        build_personal_research_skill_output(
            skill_input,
            {
                "score": 72,
                "evidence_refs": ["claim_2", "claim_1"],
                "reason_codes": [],
            },
        )
    with pytest.raises(ValueError, match="at most 16"):
        build_personal_research_skill_output(
            skill_input,
            {
                "score": 72,
                "evidence_refs": ["claim_1"],
                "reason_codes": [f"reason_{index:02d}" for index in range(17)],
            },
        )


@pytest.mark.parametrize(
    ("priority", "expected_mode", "expected_reason"),
    [
        (0, "quick", "auto_low_priority"),
        (39, "quick", "auto_low_priority"),
        (40, "standard", "auto_medium_priority"),
        (79, "standard", "auto_medium_priority"),
        (80, "deep", "auto_high_priority"),
        (100, "deep", "auto_high_priority"),
    ],
)
def test_auto_mode_resolution_has_exact_deterministic_boundaries(
    priority: int,
    expected_mode: str,
    expected_reason: str,
) -> None:
    first = resolve_research_task_mode(" AUTO ", priority=priority)
    second = resolve_research_task_mode("auto", priority=priority)
    assert first.resolved_mode == expected_mode
    assert first.reason_code == expected_reason
    assert first.resolution_hash == second.resolution_hash


@pytest.mark.parametrize("mode", ["quick", "standard", "deep", "debate"])
def test_explicit_mode_is_preserved(mode: str) -> None:
    result = resolve_research_task_mode(mode, priority=0)
    assert result.resolved_mode == mode
    assert result.reason_code == "explicit_mode"


@pytest.mark.parametrize("priority", [True, 40.0, "40", None])
def test_mode_resolution_requires_an_exact_integer(priority) -> None:
    with pytest.raises(TypeError, match="integer"):
        resolve_research_task_mode("auto", priority=priority)


@pytest.mark.parametrize("priority", [-1, 101])
def test_mode_resolution_rejects_out_of_range_priority(priority: int) -> None:
    with pytest.raises(ValueError, match="between 0 and 100"):
        resolve_research_task_mode("auto", priority=priority)


def test_global_debate_flag_does_not_debate_every_task() -> None:
    decision = evaluate_debate_trigger(
        resolved_mode="standard",
        debate_enabled=True,
        has_citable_evidence=True,
        conflict_score=59,
        significance_score=79,
        evidence_quality_score=90.0,
    )
    assert decision.triggered is False
    assert decision.reason_codes == ("trigger_conditions_not_met",)


def test_debate_trigger_accepts_explicit_conflict_or_significance_conditions() -> None:
    common = {
        "debate_enabled": True,
        "has_citable_evidence": True,
        "evidence_quality_score": 70.0,
    }
    explicit = evaluate_debate_trigger(
        resolved_mode="debate",
        conflict_score=0,
        significance_score=0,
        **common,
    )
    conflict = evaluate_debate_trigger(
        resolved_mode="standard",
        conflict_score=60,
        significance_score=0,
        **common,
    )
    significance = evaluate_debate_trigger(
        resolved_mode="quick",
        conflict_score=0,
        significance_score=80,
        **common,
    )

    assert explicit.triggered and explicit.reason_codes == ("explicit_debate_mode",)
    assert conflict.triggered and conflict.reason_codes == ("material_conflict",)
    assert significance.triggered and significance.reason_codes == ("material_significance",)


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"debate_enabled": False}, "debate_disabled"),
        ({"has_citable_evidence": False}, "citable_evidence_missing"),
        ({"evidence_quality_score": None}, "evidence_quality_missing"),
    ],
)
def test_debate_trigger_fails_closed_when_a_prerequisite_is_missing(overrides, reason) -> None:
    values = {
        "resolved_mode": "debate",
        "debate_enabled": True,
        "has_citable_evidence": True,
        "conflict_score": 100,
        "significance_score": 100,
        "evidence_quality_score": 90.0,
    }
    values.update(overrides)
    decision = evaluate_debate_trigger(**values)
    assert decision.triggered is False
    assert reason in decision.reason_codes


def test_evidence_quality_floor_applies_only_to_automatic_debate_promotion() -> None:
    common = {
        "debate_enabled": True,
        "has_citable_evidence": True,
        "conflict_score": 0,
        "significance_score": 100,
        "evidence_quality_score": 69.999,
    }

    explicit = evaluate_debate_trigger(resolved_mode="debate", **common)
    automatic = evaluate_debate_trigger(resolved_mode="standard", **common)

    assert explicit.triggered is True
    assert explicit.reason_codes == ("explicit_debate_mode", "material_significance")
    assert automatic.triggered is False
    assert automatic.reason_codes == ("evidence_quality_below_minimum",)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("debate_enabled", 1, TypeError),
        ("has_citable_evidence", "true", TypeError),
        ("conflict_score", True, TypeError),
        ("significance_score", 80.0, TypeError),
        ("conflict_score", 101, ValueError),
        ("evidence_quality_score", True, TypeError),
        ("evidence_quality_score", math.nan, ValueError),
    ],
)
def test_debate_trigger_rejects_type_smuggling_and_invalid_ranges(field, value, error) -> None:
    values = {
        "resolved_mode": "standard",
        "debate_enabled": True,
        "has_citable_evidence": True,
        "conflict_score": 60,
        "significance_score": 80,
        "evidence_quality_score": 80.0,
    }
    values[field] = value
    with pytest.raises(error):
        evaluate_debate_trigger(**values)


def test_debate_trigger_policy_rejects_bool_thresholds() -> None:
    with pytest.raises(TypeError, match="integer"):
        DebateTriggerPolicy(material_conflict_threshold=True)


def test_verifier_accepts_only_complete_cited_and_lineage_matching_debate() -> None:
    snapshot = _debate_snapshot()
    first = verify_bounded_debate(
        snapshot,
        expected_evidence_snapshot_hash=snapshot.evidence_snapshot_hash,
    )
    second = verify_bounded_debate(
        snapshot,
        expected_evidence_snapshot_hash=snapshot.evidence_snapshot_hash,
    )
    assert first.valid is True
    assert first.fail_closed is False
    assert first.reason_codes == ()
    assert first.verification_hash == second.verification_hash


def test_verifier_returns_stable_fail_closed_reasons_for_invalid_inputs() -> None:
    invalid_type = verify_bounded_debate({"status": "available"})
    assert invalid_type.fail_closed is True
    assert invalid_type.reason_codes == ("invalid_debate_type",)

    snapshot = _debate_snapshot()
    wrong_lineage = verify_bounded_debate(
        snapshot,
        expected_evidence_snapshot_hash="f" * 64,
    )
    assert wrong_lineage.reason_codes == ("evidence_lineage_mismatch",)

    corrupt = verify_bounded_debate(replace(snapshot, debate_hash="f" * 64))
    assert "debate_contract_invalid" in corrupt.reason_codes
    assert corrupt.fail_closed is True


def test_judge_uses_count_neutral_confidence_margin_and_balanced_result() -> None:
    bull = judge_bounded_debate(_debate_snapshot(bull_confidence=0.9, bear_confidence=0.6))
    bear = judge_bounded_debate(_debate_snapshot(bull_confidence=0.55, bear_confidence=0.8))
    balanced = judge_bounded_debate(_debate_snapshot(bull_confidence=0.7, bear_confidence=0.65))

    assert (bull.verdict, bull.margin, bull.reason_codes) == (
        "bull",
        0.3,
        ("bull_confidence_margin",),
    )
    assert (bear.verdict, bear.margin, bear.reason_codes) == (
        "bear",
        -0.25,
        ("bear_confidence_margin",),
    )
    assert (balanced.verdict, balanced.fail_closed, balanced.reason_codes) == (
        "balanced",
        False,
        ("confidence_margin_not_decisive",),
    )


def test_default_judge_threshold_uses_shared_prompt_contract() -> None:
    from src.services.research.debate_security import (
        DEBATE_JUDGE_MINIMUM_MEAN_CONFIDENCE,
    )

    assert (
        DebateJudgePolicy().minimum_mean_confidence
        == DEBATE_JUDGE_MINIMUM_MEAN_CONFIDENCE
    )


def test_judge_fails_closed_on_verification_or_low_confidence() -> None:
    invalid = judge_bounded_debate(None)
    assert invalid.verdict == "fail_closed"
    assert invalid.reason_codes == ("verification_failed", "invalid_debate_type")

    low = judge_bounded_debate(_debate_snapshot(bull_confidence=0.4, bear_confidence=0.3))
    assert low.verdict == "fail_closed"
    assert low.reason_codes == ("stance_confidence_below_minimum",)

    one_weak_side = judge_bounded_debate(
        _debate_snapshot(bull_confidence=0.9, bear_confidence=0.4)
    )
    assert one_weak_side.verdict == "fail_closed"
    assert one_weak_side.reason_codes == ("stance_confidence_below_minimum",)


@pytest.mark.parametrize(
    ("field", "value"),
    [("minimum_mean_confidence", True), ("decisive_margin", 1.01)],
)
def test_judge_policy_rejects_bool_and_out_of_range_values(field: str, value) -> None:
    with pytest.raises((TypeError, ValueError)):
        DebateJudgePolicy(**{field: value})
