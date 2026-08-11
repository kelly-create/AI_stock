# -*- coding: utf-8 -*-
"""Pure calibration and benchmark statistics for Decision Outcome v2."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Iterable, Mapping, Optional

from src.core.decision_outcome_v2_evaluator import (
    SUPPORTED_DECISION_OUTCOME_V2_HORIZONS,
    normalize_final_action_family,
)


MIN_DECISION_OUTCOME_V2_CALIBRATION_SAMPLES = 30
DECISION_OUTCOME_V2_CALIBRATION_BIN_COUNT = 5


@dataclass(frozen=True)
class DecisionOutcomeV2CalibrationSample:
    """One immutable outcome projected into the calibration contract."""

    engine_version: str
    horizon: str
    profile: str
    final_action_family: str
    eval_status: str
    confidence: Optional[float] = None
    direction_correct: Optional[bool] = None
    csi300_directional_excess_return_pct: Optional[float] = None
    sw1_directional_excess_return_pct: Optional[float] = None


class DecisionOutcomeV2Stats:
    """Aggregate exact four-dimensional buckets with five-bin calibration."""

    @classmethod
    def aggregate(
        cls,
        samples: Iterable[DecisionOutcomeV2CalibrationSample | Mapping[str, Any]],
        *,
        minimum_sample_size: int = MIN_DECISION_OUTCOME_V2_CALIBRATION_SAMPLES,
    ) -> dict[str, Any]:
        if (
            isinstance(minimum_sample_size, bool)
            or not isinstance(minimum_sample_size, int)
            or minimum_sample_size < 1
        ):
            raise ValueError("minimum_sample_size must be a positive integer")
        minimum = minimum_sample_size
        normalized = [cls._normalize_sample(item) for item in samples]
        grouped: dict[tuple[str, str, str, str], list[DecisionOutcomeV2CalibrationSample]] = {}
        for sample in normalized:
            key = (
                sample.engine_version,
                sample.horizon,
                sample.profile,
                sample.final_action_family,
            )
            grouped.setdefault(key, []).append(sample)

        buckets = [
            cls._aggregate_bucket(key=key, samples=rows, minimum=minimum)
            for key, rows in grouped.items()
        ]
        buckets.sort(
            key=lambda item: (
                -int(item["total"]),
                item["dimensions"]["engine"],
                item["dimensions"]["horizon"],
                item["dimensions"]["profile"],
                item["dimensions"]["final_action_family"],
            )
        )
        return {
            "bucket_dimensions": ["engine", "horizon", "profile", "final_action_family"],
            "minimum_completed_sample_size": minimum,
            "calibration_bin_count": DECISION_OUTCOME_V2_CALIBRATION_BIN_COUNT,
            "buckets": buckets,
        }

    @classmethod
    def _aggregate_bucket(
        cls,
        *,
        key: tuple[str, str, str, str],
        samples: list[DecisionOutcomeV2CalibrationSample],
        minimum: int,
    ) -> dict[str, Any]:
        calibration_rows: list[tuple[float, float]] = []
        completed_outcomes = 0
        for sample in samples:
            if sample.eval_status != "evaluated":
                continue
            completed_outcomes += 1
            if sample.confidence is None:
                continue
            probability = cls._probability(sample.confidence)
            calibration_rows.append((probability, 1.0 if sample.direction_correct else 0.0))

        calibration = cls._calibration_summary(calibration_rows, minimum=minimum)

        csi300 = cls._benchmark_aggregate(
            samples,
            field_name="csi300_directional_excess_return_pct",
            minimum=minimum,
        )
        sw1 = cls._benchmark_aggregate(
            samples,
            field_name="sw1_directional_excess_return_pct",
            minimum=minimum,
        )
        return {
            "dimensions": {
                "engine": key[0],
                "horizon": key[1],
                "profile": key[2],
                "final_action_family": key[3],
            },
            "total": len(samples),
            "evaluated_directional_outcomes": completed_outcomes,
            "calibration_samples": len(calibration_rows),
            "sample_sufficient": calibration["sample_sufficient"],
            "accuracy": calibration["accuracy"],
            "ece": calibration["ece"],
            "brier_score": calibration["brier_score"],
            "bins": calibration["bins"],
            "benchmarks": {
                "csi300": csi300,
                "sw1": sw1,
            },
        }

    @classmethod
    def _calibration_bins(cls, rows: list[tuple[float, float]]) -> list[dict[str, Any]]:
        grouped: list[list[tuple[float, float]]] = [
            [] for _ in range(DECISION_OUTCOME_V2_CALIBRATION_BIN_COUNT)
        ]
        for probability, actual in rows:
            index = min(
                int(probability * DECISION_OUTCOME_V2_CALIBRATION_BIN_COUNT),
                DECISION_OUTCOME_V2_CALIBRATION_BIN_COUNT - 1,
            )
            grouped[index].append((probability, actual))

        bins: list[dict[str, Any]] = []
        width = 1.0 / DECISION_OUTCOME_V2_CALIBRATION_BIN_COUNT
        for index, values in enumerate(grouped):
            average_confidence = (
                sum(probability for probability, _ in values) / len(values)
                if values
                else None
            )
            empirical_accuracy = (
                sum(actual for _, actual in values) / len(values)
                if values
                else None
            )
            absolute_gap = (
                abs(float(average_confidence) - float(empirical_accuracy))
                if average_confidence is not None and empirical_accuracy is not None
                else None
            )
            bins.append({
                "index": index,
                "lower_bound": cls._rounded(index * width),
                "upper_bound": cls._rounded((index + 1) * width),
                "upper_inclusive": index == DECISION_OUTCOME_V2_CALIBRATION_BIN_COUNT - 1,
                "count": len(values),
                "average_confidence": cls._rounded(average_confidence),
                "empirical_accuracy": cls._rounded(empirical_accuracy),
                "absolute_gap": cls._rounded(absolute_gap),
            })
        return bins

    @classmethod
    def _benchmark_aggregate(
        cls,
        samples: list[DecisionOutcomeV2CalibrationSample],
        *,
        field_name: str,
        minimum: int,
    ) -> dict[str, Any]:
        values: list[float] = []
        calibration_rows: list[tuple[float, float]] = []
        for sample in samples:
            if sample.eval_status != "evaluated":
                continue
            raw = getattr(sample, field_name)
            if raw is None:
                continue
            excess = cls._finite_number(raw, field_name)
            values.append(excess)
            if sample.confidence is not None:
                calibration_rows.append((
                    cls._probability(sample.confidence),
                    1.0 if excess > 0.0 else 0.0,
                ))
        sufficient = len(values) >= minimum
        calibration = cls._calibration_summary(calibration_rows, minimum=minimum)
        if sufficient:
            mean = sum(values) / len(values)
            win_rate = sum(1 for value in values if value > 0.0) / len(values)
            non_underperformance_rate = sum(1 for value in values if value >= 0.0) / len(values)
        else:
            mean = None
            win_rate = None
            non_underperformance_rate = None
        return {
            "samples": len(values),
            "sample_sufficient": sufficient,
            "calibration_samples": len(calibration_rows),
            "calibration_sample_sufficient": calibration["sample_sufficient"],
            "accuracy": calibration["accuracy"],
            "ece": calibration["ece"],
            "brier_score": calibration["brier_score"],
            "bins": calibration["bins"],
            "mean_directional_excess_return_pct": cls._rounded(mean),
            "outperformance_rate": cls._rounded(win_rate),
            "non_underperformance_rate": cls._rounded(non_underperformance_rate),
        }

    @classmethod
    def _calibration_summary(
        cls,
        rows: list[tuple[float, float]],
        *,
        minimum: int,
    ) -> dict[str, Any]:
        bins = cls._calibration_bins(rows)
        sufficient = len(rows) >= minimum
        if sufficient:
            ece = sum(
                (item["count"] / len(rows)) * float(item["absolute_gap"])
                for item in bins
                if item["count"] and item["absolute_gap"] is not None
            )
            brier = sum((probability - actual) ** 2 for probability, actual in rows) / len(rows)
            accuracy = sum(actual for _, actual in rows) / len(rows)
        else:
            ece = None
            brier = None
            accuracy = None
        return {
            "sample_sufficient": sufficient,
            "accuracy": cls._rounded(accuracy),
            "ece": cls._rounded(ece),
            "brier_score": cls._rounded(brier),
            "bins": bins,
        }

    @classmethod
    def _normalize_sample(
        cls,
        value: DecisionOutcomeV2CalibrationSample | Mapping[str, Any],
    ) -> DecisionOutcomeV2CalibrationSample:
        if isinstance(value, DecisionOutcomeV2CalibrationSample):
            sample = value
        elif isinstance(value, Mapping):
            csi300 = value.get("csi300") if isinstance(value.get("csi300"), Mapping) else {}
            sw1 = value.get("sw1") if isinstance(value.get("sw1"), Mapping) else {}
            sample = DecisionOutcomeV2CalibrationSample(
                engine_version=str(value.get("engine_version") or value.get("engine") or "").strip(),
                horizon=str(value.get("horizon") or "").strip().lower(),
                profile=str(value.get("profile") or value.get("decision_profile") or "").strip().lower(),
                final_action_family=str(value.get("final_action_family") or "").strip().lower(),
                eval_status=str(value.get("eval_status") or "").strip().lower(),
                confidence=value.get("confidence"),
                direction_correct=value.get("direction_correct"),
                csi300_directional_excess_return_pct=value.get(
                    "csi300_directional_excess_return_pct",
                    csi300.get("directional_excess_return_pct"),
                ),
                sw1_directional_excess_return_pct=value.get(
                    "sw1_directional_excess_return_pct",
                    sw1.get("directional_excess_return_pct"),
                ),
            )
        else:
            raise TypeError("samples must be DecisionOutcomeV2CalibrationSample or mapping values")

        engine = cls._label(sample.engine_version, "engine_version")
        horizon = cls._label(sample.horizon, "horizon").lower()
        if horizon not in SUPPORTED_DECISION_OUTCOME_V2_HORIZONS:
            raise ValueError("horizon must be one of 5d/10d/20d")
        profile = cls._label(sample.profile, "profile").lower()
        family = normalize_final_action_family(sample.final_action_family)
        eval_status = cls._label(sample.eval_status, "eval_status").lower()
        if eval_status not in {"pending", "evaluated", "observational", "unable", "unexecutable"}:
            raise ValueError("invalid eval_status")
        if sample.direction_correct is not None and not isinstance(sample.direction_correct, bool):
            raise ValueError("direction_correct must be bool or None")
        if eval_status == "evaluated" and sample.direction_correct is None:
            raise ValueError("evaluated direction_correct must be bool")
        if eval_status == "observational" and sample.direction_correct is not None:
            raise ValueError("observational direction_correct must be None")
        if eval_status in {"pending", "unable", "unexecutable"} and sample.direction_correct is not None:
            raise ValueError(f"{eval_status} direction_correct must be None")
        if sample.confidence is not None:
            cls._probability(sample.confidence)
        for field_name in (
            "csi300_directional_excess_return_pct",
            "sw1_directional_excess_return_pct",
        ):
            raw = getattr(sample, field_name)
            if raw is not None:
                if eval_status != "evaluated":
                    raise ValueError(f"{eval_status} {field_name} must be None")
                cls._finite_number(raw, field_name)
        return DecisionOutcomeV2CalibrationSample(
            engine_version=engine,
            horizon=horizon,
            profile=profile,
            final_action_family=family,
            eval_status=eval_status,
            confidence=sample.confidence,
            direction_correct=sample.direction_correct,
            csi300_directional_excess_return_pct=sample.csi300_directional_excess_return_pct,
            sw1_directional_excess_return_pct=sample.sw1_directional_excess_return_pct,
        )

    @staticmethod
    def _label(value: Any, field_name: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"{field_name} is required")
        return text

    @staticmethod
    def _probability(value: Any) -> float:
        number = DecisionOutcomeV2Stats._finite_number(value, "confidence")
        if not 0.0 <= number <= 1.0:
            raise ValueError("confidence must be within [0, 1]")
        return number

    @staticmethod
    def _finite_number(value: Any, field_name: str) -> float:
        if value is None or isinstance(value, bool):
            raise ValueError(f"{field_name} must be finite")
        try:
            number = float(value)
        except (OverflowError, TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be finite") from exc
        if not math.isfinite(number):
            raise ValueError(f"{field_name} must be finite")
        return number

    @staticmethod
    def _rounded(value: Optional[float]) -> Optional[float]:
        return round(float(value), 6) if value is not None else None


__all__ = [
    "DECISION_OUTCOME_V2_CALIBRATION_BIN_COUNT",
    "DecisionOutcomeV2CalibrationSample",
    "DecisionOutcomeV2Stats",
    "MIN_DECISION_OUTCOME_V2_CALIBRATION_SAMPLES",
]
