"""As-of-safe deterministic catalyst scoring."""

from __future__ import annotations

from statistics import fmean
from typing import Any, Mapping, Sequence

from .factor_policy_v1 import (
    CATALYST_DIRECTION_VALUES,
    CATALYST_EMPTY_SCORE,
    CATALYST_RULES,
    MINIMUM_COVERAGE,
    POLICY_VERSION,
    score_by_rule,
)
from .schemas import MetricResult, MetricStatus, aggregate_component, finite_float, parse_datetime


FAILED_SOURCE_STATUSES = {
    "fetch_failed",
    "permission_denied",
    "not_supported",
    "stale",
    "missing",
}


def _source_rows(source: Any) -> tuple[str, list[Mapping[str, Any]], Any]:
    if source is None:
        return "missing", [], None
    if isinstance(source, Mapping):
        status = str(source.get("status") or "available").strip().lower()
        raw_rows = source.get("items", source.get("rows", source.get("data", [])))
        source_time = source.get("available_at", source.get("as_of"))
    elif isinstance(source, Sequence) and not isinstance(source, (str, bytes, bytearray)):
        status = "available"
        raw_rows = source
        source_time = None
    else:
        raise TypeError("catalyst source must be a mapping or sequence")
    if raw_rows is None:
        raw_rows = []
    if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes, bytearray)):
        raise TypeError("catalyst source rows must be a sequence")
    rows = []
    for row in raw_rows:
        if not isinstance(row, Mapping):
            raise TypeError("each catalyst row must be a mapping")
        rows.append(row)
    return status, rows, source_time


def _direction_value(value: Any) -> float | None:
    text = str(value or "").strip().casefold()
    if not text:
        return None
    if text in CATALYST_DIRECTION_VALUES:
        return CATALYST_DIRECTION_VALUES[text]
    # Tushare forecast types may include explanatory suffixes.
    for keyword, score in CATALYST_DIRECTION_VALUES.items():
        if keyword and keyword in text:
            return score
    return None


def _first_numeric(row: Mapping[str, Any], aliases: tuple[str, ...]) -> float | None:
    for key in aliases:
        if key in row and row[key] is not None:
            return finite_float(row[key], field=f"catalyst.{key}")
    return None


def _row_signal(kind: str, row: Mapping[str, Any]) -> float | None:
    if kind == "forecast":
        value = _first_numeric(row, ("change_pct", "profit_change_pct", "p_change_min", "forecast_change_pct"))
        return value if value is not None else _direction_value(row.get("direction") or row.get("type"))
    if kind == "dividend":
        return _first_numeric(row, ("dividend_yield", "cash_dividend_yield", "yield_pct"))
    if kind == "holder":
        value = _first_numeric(row, ("change_pct", "holder_change_pct", "holding_change_pct"))
        return value if value is not None else _direction_value(row.get("direction") or row.get("type"))
    if kind == "event":
        value = _first_numeric(row, ("impact_score", "event_impact", "sentiment_score"))
        return value if value is not None else _direction_value(row.get("sentiment") or row.get("direction"))
    raise ValueError(f"unsupported catalyst kind: {kind}")


def source_signal_values(kind: str, source: Any, *, as_of: Any) -> tuple[MetricStatus, tuple[float, ...], str]:
    """Return only signals observable at the exclusive research cutoff."""

    cutoff = parse_datetime(as_of, field="as_of")
    status, rows, source_time = _source_rows(source)
    if source_time is not None and parse_datetime(source_time, field=f"{kind}.available_at") > cutoff:
        return MetricStatus.MISSING, (), "source_not_available_as_of"
    if status == "not_applicable":
        return MetricStatus.NOT_APPLICABLE, (), "not_applicable"
    if status in FAILED_SOURCE_STATUSES:
        return MetricStatus.MISSING, (), f"source_{status}"
    if status == "empty":
        return MetricStatus.AVAILABLE, (), "source_empty_neutral"
    if status not in {"available", "partial"}:
        return MetricStatus.MISSING, (), f"source_{status}"

    values: list[float] = []
    future_filtered = False
    for row in rows:
        item_time = None
        for field in ("available_at", "published_at", "announced_at", "created_at"):
            if row.get(field) is not None:
                item_time = parse_datetime(row[field], field=f"{kind}.{field}")
                break
        if item_time is not None and item_time > cutoff:
            future_filtered = True
            continue
        value = _row_signal(kind, row)
        if value is not None:
            values.append(value)
    if values:
        reason = "source_partial" if status == "partial" else "available"
        return MetricStatus.AVAILABLE, tuple(values), reason
    if future_filtered:
        return MetricStatus.MISSING, (), "no_observable_items_as_of"
    if status == "available" and not rows:
        return MetricStatus.AVAILABLE, (), "source_empty_neutral"
    return MetricStatus.MISSING, (), "no_interpretable_items"


def evaluate_catalyst(sources: Mapping[str, Any], *, as_of: Any):
    metrics = []
    for kind, rule in CATALYST_RULES.items():
        status, values, reason = source_signal_values(kind, sources.get(kind), as_of=as_of)
        if status is MetricStatus.AVAILABLE and not values:
            raw_value = 0.0
            score = CATALYST_EMPTY_SCORE
        elif status is MetricStatus.AVAILABLE:
            raw_value = fmean(values)
            score = score_by_rule(raw_value, rule)
        else:
            raw_value = None
            score = None
        metrics.append(
            MetricResult(
                name=kind,
                status=status,
                value=raw_value,
                score=score,
                weight=rule.weight,
                reason=reason,
            )
        )
    return aggregate_component(
        name="catalyst",
        metrics=metrics,
        minimum_coverage=MINIMUM_COVERAGE["catalyst"],
        policy_version=POLICY_VERSION,
    )


__all__ = ["evaluate_catalyst", "source_signal_values"]
