"""Deterministic downside-risk factor (zero is good, one hundred bad)."""

from __future__ import annotations

from dataclasses import replace
from statistics import fmean
from typing import Any, Mapping, Sequence

from .catalyst import source_signal_values
from .factor_policy_v1 import (
    MINIMUM_COVERAGE,
    POLICY_VERSION,
    RISK_EMPTY_FORECAST_SCORE,
    RISK_LEVERAGE_RULES,
    RISK_RULES,
    RISK_SCORE_FLOORS,
    score_by_rule,
)
from .schemas import ComponentResult, MetricResult, MetricStatus, aggregate_component, metric_input
from .trend_timing import annualized_volatility_pct, completed_adjusted_bars, max_drawdown_pct


def _risk_flags(market_state: Mapping[str, Any]) -> set[str] | None:
    if "risk_flags" not in market_state:
        return None
    raw = market_state.get("risk_flags")
    if raw is None:
        return set()
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise TypeError("risk_flags must be a sequence")
    return {str(item).strip().casefold() for item in raw}


def _binary_metric(name: str, value: bool | None, *, reason: str) -> MetricResult:
    rule = RISK_RULES[name]
    numeric = float(value) if value is not None else None
    return MetricResult(
        name=name,
        status=MetricStatus.AVAILABLE if value is not None else MetricStatus.MISSING,
        value=numeric,
        score=score_by_rule(numeric, rule) if numeric is not None else None,
        weight=rule.weight,
        reason=reason if value is not None else "market_state_missing",
    )


def _is_st(company: Mapping[str, Any], market_state: Mapping[str, Any], flags: set[str] | None) -> tuple[bool | None, str]:
    if "is_st" in market_state:
        raw = market_state["is_st"]
        if isinstance(raw, bool):
            return raw, "explicit_is_st"
        if raw in (0, 1):
            return bool(raw), "explicit_is_st"
        raise TypeError("is_st must be a boolean")
    if flags is not None:
        return "st" in flags, "risk_flags"
    name = str(company.get("name") or company.get("stock_name") or "").strip().upper()
    if name:
        return name.startswith(("ST", "*ST", "SST", "S*ST")), "company_name"
    return None, "market_state_missing"


def _is_suspended(market_state: Mapping[str, Any], flags: set[str] | None) -> tuple[bool | None, str]:
    if "trading_status" in market_state:
        status = str(market_state.get("trading_status") or "").strip().casefold()
        if status in {"suspended", "halted", "停牌"}:
            return True, "trading_status"
        if status in {"normal", "trading", "active", "正常"}:
            return False, "trading_status"
        return None, "unknown_trading_status"
    if flags is not None:
        return bool({"suspended", "halted", "停牌"} & flags), "risk_flags"
    return None, "market_state_missing"


def _is_one_price_limit(market_state: Mapping[str, Any], flags: set[str] | None) -> tuple[bool | None, str]:
    if "limit_state" in market_state:
        state = str(market_state.get("limit_state") or "").strip().casefold()
        if state in {"one_price_limit_up", "one_price_limit_down", "一字涨停", "一字跌停"}:
            return True, "limit_state"
        if state in {"none", "normal", ""}:
            return False, "limit_state"
        return None, "unknown_limit_state"
    if flags is not None:
        values = {"limit_up", "limit_down", "one_price_limit_up", "one_price_limit_down"}
        return bool(values & flags), "risk_flags"
    return None, "market_state_missing"


def _continuous_metric(name: str, value: float | None, *, reason: str) -> MetricResult:
    rule = RISK_RULES[name]
    return MetricResult(
        name=name,
        status=MetricStatus.AVAILABLE if value is not None else MetricStatus.MISSING,
        value=value,
        score=score_by_rule(value, rule) if value is not None else None,
        weight=rule.weight,
        reason=reason if value is not None else "insufficient_completed_history",
    )


def _forecast_metric(catalysts: Mapping[str, Any], *, as_of: Any) -> MetricResult:
    rule = RISK_RULES["negative_forecast"]
    status, values, reason = source_signal_values("forecast", catalysts.get("forecast"), as_of=as_of)
    if status is MetricStatus.AVAILABLE and values:
        value = fmean(values)
        score = score_by_rule(value, rule)
    elif status is MetricStatus.AVAILABLE:
        value = 0.0
        score = RISK_EMPTY_FORECAST_SCORE
    else:
        value = None
        score = None
    return MetricResult(
        name="negative_forecast",
        status=status,
        value=value,
        score=score,
        weight=rule.weight,
        reason=reason,
    )


def _leverage_metric(profile: str, fundamentals: Mapping[str, Any]) -> MetricResult:
    if profile not in RISK_LEVERAGE_RULES:
        raise ValueError(f"unsupported company profile: {profile}")
    rule = RISK_LEVERAGE_RULES[profile]
    status, value, reason = metric_input(fundamentals, rule.aliases)
    score = score_by_rule(value, rule) if status is MetricStatus.AVAILABLE and value is not None else None
    return MetricResult(
        name="profile_leverage",
        status=status,
        value=value,
        score=score,
        weight=rule.weight,
        reason=f"{profile}:{reason}",
    )


def evaluate_risk(
    *,
    profile: str,
    company: Mapping[str, Any],
    fundamentals: Mapping[str, Any],
    bars: Sequence[Mapping[str, Any]],
    catalysts: Mapping[str, Any],
    market_state: Mapping[str, Any],
    as_of: Any,
) -> ComponentResult:
    flags = _risk_flags(market_state)
    st, st_reason = _is_st(company, market_state, flags)
    suspended, suspended_reason = _is_suspended(market_state, flags)
    one_price, one_price_reason = _is_one_price_limit(market_state, flags)
    adjusted = completed_adjusted_bars(bars, as_of=as_of)
    metrics = [
        _binary_metric("st", st, reason=st_reason),
        _binary_metric("suspended", suspended, reason=suspended_reason),
        _binary_metric("one_price_limit", one_price, reason=one_price_reason),
        _continuous_metric(
            "volatility_60d",
            annualized_volatility_pct(adjusted),
            reason="completed_adjusted_bars",
        ),
        _continuous_metric("drawdown_300d", max_drawdown_pct(adjusted), reason="completed_adjusted_bars"),
        _forecast_metric(catalysts, as_of=as_of),
        _leverage_metric(profile, fundamentals),
    ]
    result = aggregate_component(
        name="risk",
        metrics=metrics,
        minimum_coverage=MINIMUM_COVERAGE["risk"],
        policy_version=POLICY_VERSION,
        sample_size=len(adjusted),
    )
    if result.score is None:
        return result
    floor = 0.0
    if st:
        floor = max(floor, RISK_SCORE_FLOORS["st"])
    if suspended:
        floor = max(floor, RISK_SCORE_FLOORS["suspended"])
    if one_price:
        floor = max(floor, RISK_SCORE_FLOORS["one_price_limit"])
    return replace(result, score=max(result.score, floor))


__all__ = ["evaluate_risk"]
