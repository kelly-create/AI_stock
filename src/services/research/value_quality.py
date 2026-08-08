"""Deterministic, profile-aware value and quality factors."""

from __future__ import annotations

from typing import Any, Mapping

from .factor_policy_v1 import (
    MINIMUM_COVERAGE,
    POLICY_VERSION,
    QUALITY_METRIC_UNIVERSE,
    QUALITY_RULES,
    VALUE_METRIC_UNIVERSE,
    VALUE_RULES,
    BandRule,
    score_by_rule,
)
from .schemas import MetricResult, MetricStatus, ValueQualityResult, aggregate_component, metric_input


def _metric_result(name: str, rule: BandRule, fundamentals: Mapping[str, Any]) -> MetricResult:
    status, value, reason = metric_input(fundamentals, rule.aliases)
    score = score_by_rule(value, rule) if status is MetricStatus.AVAILABLE and value is not None else None
    return MetricResult(
        name=name,
        status=status,
        value=value,
        score=score,
        weight=rule.weight,
        reason=reason,
    )


def _not_applicable(name: str) -> MetricResult:
    return MetricResult(
        name=name,
        status=MetricStatus.NOT_APPLICABLE,
        value=None,
        score=None,
        weight=0.0,
        reason="profile_not_applicable",
    )


def _evaluate(
    *,
    component: str,
    profile: str,
    fundamentals: Mapping[str, Any],
    universe: tuple[str, ...],
    rule_sets: Mapping[str, Mapping[str, BandRule]],
):
    if profile not in rule_sets:
        raise ValueError(f"unsupported company profile: {profile}")
    rules = rule_sets[profile]
    metrics = tuple(
        _metric_result(name, rules[name], fundamentals) if name in rules else _not_applicable(name)
        for name in universe
    )
    return aggregate_component(
        name=component,
        metrics=metrics,
        minimum_coverage=MINIMUM_COVERAGE[component],
        policy_version=POLICY_VERSION,
    )


def evaluate_value(profile: str, fundamentals: Mapping[str, Any]):
    return _evaluate(
        component="value",
        profile=profile,
        fundamentals=fundamentals,
        universe=VALUE_METRIC_UNIVERSE,
        rule_sets=VALUE_RULES,
    )


def evaluate_quality(profile: str, fundamentals: Mapping[str, Any]):
    return _evaluate(
        component="quality",
        profile=profile,
        fundamentals=fundamentals,
        universe=QUALITY_METRIC_UNIVERSE,
        rule_sets=QUALITY_RULES,
    )


def evaluate_value_quality(profile: str, fundamentals: Mapping[str, Any]) -> ValueQualityResult:
    """Return value and quality independently; neither score feeds the other."""

    return ValueQualityResult(
        value=evaluate_value(profile, fundamentals),
        quality=evaluate_quality(profile, fundamentals),
    )


__all__ = ["evaluate_quality", "evaluate_value", "evaluate_value_quality"]
