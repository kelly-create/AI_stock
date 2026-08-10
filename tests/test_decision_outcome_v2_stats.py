# -*- coding: utf-8 -*-
"""Focused calibration tests for Decision Outcome v2."""

from __future__ import annotations

import pytest

from src.services.decision_outcome_v2_stats import (
    DECISION_OUTCOME_V2_CALIBRATION_BIN_COUNT,
    DecisionOutcomeV2CalibrationSample,
    DecisionOutcomeV2Stats,
    MIN_DECISION_OUTCOME_V2_CALIBRATION_SAMPLES,
)


def _sample(
    index: int,
    *,
    confidence: float | None = 0.8,
    correct: bool | None = True,
    status: str = "evaluated",
    csi300: float | None = 1.0,
    sw1: float | None = 2.0,
    horizon: str = "5d",
    family: str = "long",
) -> DecisionOutcomeV2CalibrationSample:
    return DecisionOutcomeV2CalibrationSample(
        engine_version="personal-research-outcome-v2",
        horizon=horizon,
        profile="balanced",
        final_action_family=family,
        eval_status=status,
        confidence=confidence,
        direction_correct=correct,
        csi300_directional_excess_return_pct=csi300,
        sw1_directional_excess_return_pct=sw1,
    )


def test_exact_four_dimension_bucket_has_five_bin_ece_and_brier_at_n_30() -> None:
    samples = [
        _sample(index, correct=index < 15, sw1=2.0 if index < 29 else None)
        for index in range(30)
    ]
    result = DecisionOutcomeV2Stats.aggregate(samples)

    assert result["bucket_dimensions"] == ["engine", "horizon", "profile", "final_action_family"]
    assert result["minimum_completed_sample_size"] == MIN_DECISION_OUTCOME_V2_CALIBRATION_SAMPLES == 30
    assert result["calibration_bin_count"] == DECISION_OUTCOME_V2_CALIBRATION_BIN_COUNT == 5
    assert len(result["buckets"]) == 1
    bucket = result["buckets"][0]
    assert bucket["dimensions"] == {
        "engine": "personal-research-outcome-v2",
        "horizon": "5d",
        "profile": "balanced",
        "final_action_family": "long",
    }
    assert bucket["calibration_samples"] == 30
    assert bucket["sample_sufficient"] is True
    assert bucket["accuracy"] == pytest.approx(0.5)
    assert bucket["ece"] == pytest.approx(0.3)
    assert bucket["brier_score"] == pytest.approx(0.34)
    assert len(bucket["bins"]) == 5
    assert [item["count"] for item in bucket["bins"]] == [0, 0, 0, 0, 30]
    assert bucket["bins"][4]["lower_bound"] == 0.8
    assert bucket["bins"][4]["upper_bound"] == 1.0
    assert bucket["bins"][4]["upper_inclusive"] is True

    csi300 = bucket["benchmarks"]["csi300"]
    sw1 = bucket["benchmarks"]["sw1"]
    assert csi300["samples"] == 30
    assert csi300["sample_sufficient"] is True
    assert csi300["calibration_samples"] == 30
    assert csi300["calibration_sample_sufficient"] is True
    assert csi300["accuracy"] == 1.0
    assert csi300["ece"] == pytest.approx(0.2)
    assert csi300["brier_score"] == pytest.approx(0.04)
    assert csi300["mean_directional_excess_return_pct"] == 1.0
    assert csi300["outperformance_rate"] == 1.0
    assert csi300["non_underperformance_rate"] == 1.0
    assert sw1["samples"] == 29
    assert sw1["sample_sufficient"] is False
    assert sw1["calibration_samples"] == 29
    assert sw1["calibration_sample_sufficient"] is False
    assert sw1["ece"] is None
    assert sw1["brier_score"] is None
    assert sw1["mean_directional_excess_return_pct"] is None
    assert sw1["outperformance_rate"] is None


def test_n_29_keeps_all_calibration_metrics_gated() -> None:
    bucket = DecisionOutcomeV2Stats.aggregate([_sample(index) for index in range(29)])["buckets"][0]

    assert bucket["calibration_samples"] == 29
    assert bucket["sample_sufficient"] is False
    assert bucket["accuracy"] is None
    assert bucket["ece"] is None
    assert bucket["brier_score"] is None


def test_observational_rows_are_excluded_from_calibration_and_directional_benchmarks() -> None:
    directional = [_sample(index) for index in range(30)]
    observational = [
        _sample(
            index,
            status="observational",
            confidence=0.99,
            correct=None,
            csi300=None,
            sw1=None,
        )
        for index in range(40)
    ]
    bucket = DecisionOutcomeV2Stats.aggregate(directional + observational)["buckets"][0]

    assert bucket["total"] == 70
    assert bucket["calibration_samples"] == 30
    assert bucket["benchmarks"]["csi300"]["samples"] == 30
    assert bucket["benchmarks"]["sw1"]["samples"] == 30


def test_observational_directional_fields_are_rejected_not_silently_ignored() -> None:
    with pytest.raises(ValueError, match="observational direction_correct"):
        DecisionOutcomeV2Stats.aggregate([
            _sample(0, status="observational", confidence=0.8, correct=True, csi300=None, sw1=None)
        ])

    with pytest.raises(ValueError, match="observational csi300"):
        DecisionOutcomeV2Stats.aggregate([
            _sample(0, status="observational", confidence=0.8, correct=None, csi300=1.0, sw1=None)
        ])


def test_benchmark_thresholds_are_independent_from_calibration_confidence() -> None:
    samples = [
        _sample(
            index,
            confidence=None,
            csi300=1.0 if index < 30 else None,
            sw1=2.0 if index < 29 else None,
        )
        for index in range(30)
    ]
    bucket = DecisionOutcomeV2Stats.aggregate(samples)["buckets"][0]

    assert bucket["calibration_samples"] == 0
    assert bucket["sample_sufficient"] is False
    assert bucket["benchmarks"]["csi300"]["samples"] == 30
    assert bucket["benchmarks"]["csi300"]["sample_sufficient"] is True
    assert bucket["benchmarks"]["csi300"]["calibration_samples"] == 0
    assert bucket["benchmarks"]["csi300"]["calibration_sample_sufficient"] is False
    assert bucket["benchmarks"]["csi300"]["ece"] is None
    assert bucket["benchmarks"]["sw1"]["samples"] == 29
    assert bucket["benchmarks"]["sw1"]["sample_sufficient"] is False


def test_bucket_key_never_collapses_horizon_or_action_family() -> None:
    samples = [
        _sample(0, horizon="5d", family="long"),
        _sample(1, horizon="10d", family="long"),
        _sample(2, horizon="5d", family="defensive"),
    ]
    result = DecisionOutcomeV2Stats.aggregate(samples)

    dimensions = {tuple(bucket["dimensions"].values()) for bucket in result["buckets"]}
    assert dimensions == {
        ("personal-research-outcome-v2", "5d", "balanced", "long"),
        ("personal-research-outcome-v2", "10d", "balanced", "long"),
        ("personal-research-outcome-v2", "5d", "balanced", "defensive"),
    }


def test_probability_boundaries_land_in_exact_five_bins() -> None:
    probabilities = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
    samples = [_sample(index, confidence=value) for index, value in enumerate(probabilities)]
    bucket = DecisionOutcomeV2Stats.aggregate(samples, minimum_sample_size=1)["buckets"][0]

    assert [item["count"] for item in bucket["bins"]] == [1, 1, 1, 1, 2]


def test_nested_evaluator_mapping_is_supported_without_schema_coupling() -> None:
    mapping = {
        "engine_version": "personal-research-outcome-v2",
        "horizon": "20d",
        "decision_profile": "aggressive",
        "final_action_family": "exit",
        "eval_status": "evaluated",
        "confidence": 0.7,
        "direction_correct": True,
        "csi300": {"directional_excess_return_pct": 3.5},
        "sw1": {"directional_excess_return_pct": -1.5},
    }
    bucket = DecisionOutcomeV2Stats.aggregate([mapping], minimum_sample_size=1)["buckets"][0]

    assert bucket["dimensions"]["profile"] == "aggressive"
    assert bucket["dimensions"]["final_action_family"] == "defensive"
    assert bucket["benchmarks"]["csi300"]["mean_directional_excess_return_pct"] == 3.5
    assert bucket["benchmarks"]["sw1"]["mean_directional_excess_return_pct"] == -1.5


@pytest.mark.parametrize("confidence", [-0.01, 1.01, float("nan"), float("inf"), True])
def test_invalid_confidence_fails_closed(confidence) -> None:
    with pytest.raises(ValueError, match="confidence"):
        DecisionOutcomeV2Stats.aggregate([_sample(0, confidence=confidence)])


def test_empty_input_has_stable_contract() -> None:
    assert DecisionOutcomeV2Stats.aggregate([]) == {
        "bucket_dimensions": ["engine", "horizon", "profile", "final_action_family"],
        "minimum_completed_sample_size": 30,
        "calibration_bin_count": 5,
        "buckets": [],
    }


@pytest.mark.parametrize("minimum", [0, -1, 1.5, "30", True])
def test_minimum_sample_size_is_a_strict_positive_integer(minimum) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        DecisionOutcomeV2Stats.aggregate([], minimum_sample_size=minimum)
