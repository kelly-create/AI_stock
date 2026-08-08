"""Pure technical factor calculation over frozen, completed daily bars."""

from __future__ import annotations

from dataclasses import dataclass
import math
from statistics import fmean, pstdev
from typing import Any, Mapping, Optional, Sequence

from .factor_policy_v1 import (
    MAX_TREND_BARS,
    MINIMUM_COVERAGE,
    POLICY_VERSION,
    PRIMARY_TREND_HORIZON,
    TREND_RULES,
    score_by_rule,
)
from .schemas import MetricResult, MetricStatus, aggregate_component, finite_float, parse_datetime


@dataclass(frozen=True)
class AdjustedBar:
    timestamp: object
    close: float
    high: Optional[float]
    low: Optional[float]
    volume: Optional[float]


def _optional_number(value: Any, *, field: str) -> Optional[float]:
    if value is None:
        return None
    return finite_float(value, field=field)


def _bar_timestamp(bar: Mapping[str, Any]):
    for key in ("trade_date", "date", "datetime", "as_of"):
        if bar.get(key) is not None:
            return parse_datetime(bar[key], field=f"bar.{key}")
    raise ValueError("each bar requires trade_date, date, datetime, or as_of")


def completed_adjusted_bars(
    bars: Sequence[Mapping[str, Any]],
    *,
    as_of: Any,
    limit: int = MAX_TREND_BARS,
) -> tuple[AdjustedBar, ...]:
    """Return at most ``limit`` completed, split-adjusted bars before ``as_of``."""

    cutoff = parse_datetime(as_of, field="as_of")
    candidates: list[tuple[object, int, Mapping[str, Any]]] = []
    for index, bar in enumerate(bars):
        completed = bar.get("completed", bar.get("is_completed", True))
        if completed is False or completed == 0:
            continue
        timestamp = _bar_timestamp(bar)
        if timestamp > cutoff:
            continue
        candidates.append((timestamp, index, bar))
    candidates.sort(key=lambda item: (item[0], item[1]))
    if limit <= 0:
        raise ValueError("bar limit must be positive")
    candidates = candidates[-limit:]
    if not candidates:
        return ()

    factors: list[Optional[float]] = []
    for _, _, bar in candidates:
        factor = _optional_number(bar.get("adj_factor"), field="bar.adj_factor")
        if factor is not None and factor <= 0:
            raise ValueError("bar.adj_factor must be positive")
        factors.append(factor)
    latest_factor = next((factor for factor in reversed(factors) if factor is not None), None)

    adjusted: list[AdjustedBar] = []
    for (timestamp, _, bar), factor in zip(candidates, factors):
        close = _optional_number(bar.get("adj_close"), field="bar.adj_close")
        explicit_adjusted = close is not None
        if close is None:
            close = finite_float(bar.get("close"), field="bar.close")
        if close <= 0:
            raise ValueError("bar.close must be positive")
        high = _optional_number(bar.get("adj_high"), field="bar.adj_high")
        low = _optional_number(bar.get("adj_low"), field="bar.adj_low")
        if high is None:
            high = _optional_number(bar.get("high"), field="bar.high")
        if low is None:
            low = _optional_number(bar.get("low"), field="bar.low")
        if not explicit_adjusted and factor is not None and latest_factor is not None:
            multiplier = factor / latest_factor
            close *= multiplier
            if high is not None:
                high *= multiplier
            if low is not None:
                low *= multiplier
        volume = _optional_number(bar.get("volume", bar.get("vol")), field="bar.volume")
        if volume is not None and volume < 0:
            raise ValueError("bar.volume must be non-negative")
        adjusted.append(AdjustedBar(timestamp=timestamp, close=close, high=high, low=low, volume=volume))
    return tuple(adjusted)


def close_returns(bars: Sequence[AdjustedBar]) -> tuple[float, ...]:
    values: list[float] = []
    for previous, current in zip(bars, bars[1:]):
        if previous.close <= 0:
            raise ValueError("adjusted close must be positive")
        values.append(current.close / previous.close - 1.0)
    return tuple(values)


def annualized_volatility_pct(bars: Sequence[AdjustedBar], *, window: int = 60) -> Optional[float]:
    returns = close_returns(bars)
    if len(returns) < 2:
        return None
    sample = returns[-min(window, len(returns)) :]
    return pstdev(sample) * math.sqrt(252.0) * 100.0


def max_drawdown_pct(bars: Sequence[AdjustedBar]) -> Optional[float]:
    if len(bars) < 2:
        return None
    peak = bars[0].close
    maximum = 0.0
    for bar in bars[1:]:
        peak = max(peak, bar.close)
        maximum = max(maximum, (peak - bar.close) / peak * 100.0)
    return maximum


def _horizon_return(closes: Sequence[float], horizon: int) -> Optional[float]:
    if len(closes) <= horizon:
        return None
    reference = closes[-(horizon + 1)]
    if reference <= 0:
        return None
    return (closes[-1] / reference - 1.0) * 100.0


def _moving_average_slope(closes: Sequence[float], window: int, *, slope_periods: int = 5) -> Optional[float]:
    required = window + slope_periods
    if len(closes) < required:
        return None
    averages = [fmean(closes[end - window : end]) for end in range(len(closes) - slope_periods, len(closes) + 1)]
    count = len(averages)
    x_mean = (count - 1) / 2.0
    y_mean = fmean(averages)
    if y_mean <= 0:
        return None
    denominator = sum((index - x_mean) ** 2 for index in range(count))
    slope = sum((index - x_mean) * (value - y_mean) for index, value in enumerate(averages)) / denominator
    return slope / y_mean * 100.0


def _rsi14(closes: Sequence[float]) -> Optional[float]:
    if len(closes) < 15:
        return None
    changes = [current - previous for previous, current in zip(closes[-15:-1], closes[-14:])]
    gains = fmean(max(change, 0.0) for change in changes)
    losses = fmean(max(-change, 0.0) for change in changes)
    if losses == 0:
        return 100.0 if gains > 0 else 50.0
    relative_strength = gains / losses
    return 100.0 - 100.0 / (1.0 + relative_strength)


def _atr20_pct(bars: Sequence[AdjustedBar]) -> Optional[float]:
    if len(bars) < 21 or any(bar.high is None or bar.low is None for bar in bars[-20:]):
        return None
    true_ranges: list[float] = []
    for previous, current in zip(bars[-21:-1], bars[-20:]):
        assert current.high is not None and current.low is not None
        true_ranges.append(max(current.high - current.low, abs(current.high - previous.close), abs(current.low - previous.close)))
    if bars[-1].close <= 0:
        return None
    return fmean(true_ranges) / bars[-1].close * 100.0


def _volume_ratio20(bars: Sequence[AdjustedBar]) -> Optional[float]:
    if len(bars) < 21:
        return None
    volumes = [bar.volume for bar in bars[-21:]]
    if any(value is None for value in volumes):
        return None
    reference = fmean(value for value in volumes[:-1] if value is not None)
    if reference <= 0 or volumes[-1] is None:
        return None
    return volumes[-1] / reference


def _current_drawdown_pct(closes: Sequence[float]) -> Optional[float]:
    if len(closes) < 2:
        return None
    peak = max(closes)
    return (peak - closes[-1]) / peak * 100.0


def evaluate_trend_timing(bars: Sequence[Mapping[str, Any]], *, as_of: Any):
    adjusted = completed_adjusted_bars(bars, as_of=as_of)
    closes = [bar.close for bar in adjusted]
    values: dict[str, Optional[float]] = {
        "return_5d": _horizon_return(closes, 5),
        "return_10d": _horizon_return(closes, 10),
        "return_20d": _horizon_return(closes, 20),
        "ma5_slope": _moving_average_slope(closes, 5),
        "ma10_slope": _moving_average_slope(closes, 10),
        "ma20_slope": _moving_average_slope(closes, 20),
        "ma60_slope": _moving_average_slope(closes, 60),
        "ma120_slope": _moving_average_slope(closes, 120),
        "ma250_slope": _moving_average_slope(closes, 250),
        "rsi14": _rsi14(closes),
        "atr20_pct": _atr20_pct(adjusted),
        "volume_ratio20": _volume_ratio20(adjusted),
        "drawdown_pct": _current_drawdown_pct(closes),
    }
    metrics = []
    for name, rule in TREND_RULES.items():
        value = values[name]
        metrics.append(
            MetricResult(
                name=name,
                status=MetricStatus.AVAILABLE if value is not None else MetricStatus.MISSING,
                value=value,
                score=score_by_rule(value, rule) if value is not None else None,
                weight=rule.weight,
                reason="available" if value is not None else "insufficient_completed_history",
            )
        )
    return aggregate_component(
        name="trend_timing",
        metrics=metrics,
        minimum_coverage=MINIMUM_COVERAGE["trend_timing"],
        policy_version=POLICY_VERSION,
        sample_size=len(adjusted),
        primary_horizon=PRIMARY_TREND_HORIZON,
    )


__all__ = [
    "AdjustedBar",
    "annualized_volatility_pct",
    "close_returns",
    "completed_adjusted_bars",
    "evaluate_trend_timing",
    "max_drawdown_pct",
]
