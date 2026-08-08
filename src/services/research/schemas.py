"""Pure result schemas and numeric guards for deterministic research factors.

The factor engines deliberately consume ``Mapping`` objects rather than storage
or provider models.  This keeps the calculation layer usable with frozen
snapshots, fixtures, and future schema revisions without importing either the
database or an analyzer.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from enum import Enum
import math
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple


class MetricStatus(str, Enum):
    AVAILABLE = "available"
    MISSING = "missing"
    NOT_APPLICABLE = "not_applicable"


class ComponentStatus(str, Enum):
    AVAILABLE = "available"
    PARTIAL = "partial"
    NOT_APPLICABLE = "not_applicable"


def finite_float(value: Any, *, field: str = "value") -> float:
    """Return a finite float, rejecting booleans, NaN and infinities."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field} must not be NaN or infinity")
    return number


def optional_finite_float(value: Any, *, field: str = "value") -> Optional[float]:
    if value is None:
        return None
    return finite_float(value, field=field)


def clamp_score(value: float) -> float:
    return round(min(100.0, max(0.0, finite_float(value, field="score"))), 4)


def parse_datetime(value: Any, *, field: str = "datetime") -> datetime:
    """Parse an instant and normalize it to UTC.

    Naive values are interpreted as UTC.  Research snapshots should normally
    provide timezone-aware RFC3339 values, but accepting naive provider dates is
    useful for daily bars and remains deterministic.
    """

    if isinstance(value, datetime):
        result = value
    elif isinstance(value, date):
        result = datetime(value.year, value.month, value.day)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field} must not be empty")
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            result = datetime.fromisoformat(text)
        except ValueError:
            # Tushare commonly uses YYYYMMDD trade dates.
            try:
                result = datetime.strptime(text, "%Y%m%d")
            except ValueError as exc:
                raise ValueError(f"{field} is not an ISO/RFC3339 datetime") from exc
    else:
        raise TypeError(f"{field} must be a date, datetime, or string")
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


@dataclass(frozen=True)
class ProfileResolution:
    profile: str
    source: str
    resolver_version: str


@dataclass(frozen=True)
class MetricResult:
    name: str
    status: MetricStatus
    value: Optional[float]
    score: Optional[float]
    weight: float
    effective_weight: float = 0.0
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", MetricStatus(self.status))
        object.__setattr__(self, "value", optional_finite_float(self.value, field=f"{self.name}.value"))
        score = optional_finite_float(self.score, field=f"{self.name}.score")
        if score is not None:
            score = clamp_score(score)
        object.__setattr__(self, "score", score)
        weight = finite_float(self.weight, field=f"{self.name}.weight")
        effective_weight = finite_float(self.effective_weight, field=f"{self.name}.effective_weight")
        if weight < 0 or effective_weight < 0:
            raise ValueError("metric weights must be non-negative")
        # Normalized weights are part of the immutable factor payload.  Python
        # versions use different floating-point summation algorithms (notably
        # 3.11 versus 3.12), so the final division can differ by one ULP even
        # when the policy and inputs are identical.  Fix only that derived
        # value's public precision before hashing or persistence; the policy's
        # raw weight remains untouched.
        object.__setattr__(self, "weight", weight)
        object.__setattr__(self, "effective_weight", round(effective_weight, 12))
        if self.status is MetricStatus.AVAILABLE and (self.value is None or self.score is None):
            raise ValueError("available metrics require both a value and a score")
        if self.status is not MetricStatus.AVAILABLE and self.score is not None:
            raise ValueError("unavailable metrics cannot carry a score")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "value": self.value,
            "score": self.score,
            "weight": self.weight,
            "effective_weight": self.effective_weight,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ComponentResult:
    name: str
    status: ComponentStatus
    score: Optional[float]
    coverage: float
    metrics: Tuple[MetricResult, ...]
    policy_version: str
    sample_size: Optional[int] = None
    primary_horizon: Optional[int] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", ComponentStatus(self.status))
        score = optional_finite_float(self.score, field=f"{self.name}.score")
        if score is not None:
            score = clamp_score(score)
        object.__setattr__(self, "score", score)
        coverage = finite_float(self.coverage, field=f"{self.name}.coverage")
        if not 0.0 <= coverage <= 1.0:
            raise ValueError("coverage must be between zero and one")
        object.__setattr__(self, "coverage", round(coverage, 6))
        object.__setattr__(self, "metrics", tuple(self.metrics))

    def metric(self, name: str) -> MetricResult:
        for metric in self.metrics:
            if metric.name == name:
                return metric
        raise KeyError(name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "score": self.score,
            "coverage": self.coverage,
            "metrics": [item.to_dict() for item in self.metrics],
            "policy_version": self.policy_version,
            "sample_size": self.sample_size,
            "primary_horizon": self.primary_horizon,
        }


@dataclass(frozen=True)
class ValueQualityResult:
    value: ComponentResult
    quality: ComponentResult

    def to_dict(self) -> dict[str, Any]:
        return {"value": self.value.to_dict(), "quality": self.quality.to_dict()}


@dataclass(frozen=True)
class ResearchFactorResult:
    stock_code: str
    as_of: str
    profile: ProfileResolution
    value: ComponentResult
    quality: ComponentResult
    trend_timing: ComponentResult
    catalyst: ComponentResult
    risk: ComponentResult
    policy_version: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "stock_code": self.stock_code,
            "as_of": self.as_of,
            "profile": {
                "profile": self.profile.profile,
                "source": self.profile.source,
                "resolver_version": self.profile.resolver_version,
            },
            "value": self.value.to_dict(),
            "quality": self.quality.to_dict(),
            "trend_timing": self.trend_timing.to_dict(),
            "catalyst": self.catalyst.to_dict(),
            "risk": self.risk.to_dict(),
            "policy_version": self.policy_version,
        }


def metric_input(
    source: Mapping[str, Any],
    names: Sequence[str],
) -> tuple[MetricStatus, Optional[float], str]:
    """Read a numeric metric without turning missing data into zero.

    A provider may expose either a scalar or ``{"status": ..., "value": ...}``.
    Dataset failure states become ``missing`` while an explicit
    ``not_applicable`` remains distinct.
    """

    selected: Optional[str] = None
    raw: Any = None
    for name in names:
        if name in source:
            selected = name
            raw = source[name]
            break
    if selected is None:
        return MetricStatus.MISSING, None, "field_missing"
    status_text = "available"
    if isinstance(raw, Mapping):
        status_text = str(raw.get("status") or "available").strip().lower()
        raw = raw.get("value")
    if status_text == MetricStatus.NOT_APPLICABLE.value:
        return MetricStatus.NOT_APPLICABLE, None, "not_applicable"
    if status_text not in {"available", "partial"}:
        return MetricStatus.MISSING, None, f"source_{status_text}"
    if raw is None:
        return MetricStatus.MISSING, None, "value_missing"
    try:
        value = finite_float(raw, field=selected)
    except TypeError:
        return MetricStatus.MISSING, None, "invalid_numeric_type"
    return MetricStatus.AVAILABLE, value, "available"


def aggregate_component(
    *,
    name: str,
    metrics: Iterable[MetricResult],
    minimum_coverage: float,
    policy_version: str,
    sample_size: Optional[int] = None,
    primary_horizon: Optional[int] = None,
) -> ComponentResult:
    """Aggregate metrics with applicable and available weights re-normalized.

    ``not_applicable`` weights are removed from the denominator.  Missing
    weights reduce coverage but never contribute a zero score.  A component
    below its minimum coverage remains partial with ``score=None``.
    """

    metric_tuple = tuple(metrics)
    minimum = finite_float(minimum_coverage, field="minimum_coverage")
    if not 0.0 <= minimum <= 1.0:
        raise ValueError("minimum_coverage must be between zero and one")
    applicable = [item for item in metric_tuple if item.status is not MetricStatus.NOT_APPLICABLE]
    applicable_weight = sum(item.weight for item in applicable)
    if applicable_weight <= 0:
        return ComponentResult(
            name=name,
            status=ComponentStatus.NOT_APPLICABLE,
            score=None,
            coverage=0.0,
            metrics=metric_tuple,
            policy_version=policy_version,
            sample_size=sample_size,
            primary_horizon=primary_horizon,
        )
    available = [item for item in applicable if item.status is MetricStatus.AVAILABLE]
    available_weight = sum(item.weight for item in available)
    coverage = available_weight / applicable_weight
    adjusted = tuple(
        replace(item, effective_weight=(item.weight / available_weight if item in available and available_weight else 0.0))
        for item in metric_tuple
    )
    component_score: Optional[float] = None
    if coverage + 1e-12 >= minimum and available_weight > 0:
        component_score = clamp_score(
            sum((item.score or 0.0) * item.weight for item in available) / available_weight
        )
    status = ComponentStatus.AVAILABLE if abs(coverage - 1.0) <= 1e-12 else ComponentStatus.PARTIAL
    return ComponentResult(
        name=name,
        status=status,
        score=component_score,
        coverage=coverage,
        metrics=adjusted,
        policy_version=policy_version,
        sample_size=sample_size,
        primary_horizon=primary_horizon,
    )


__all__ = [
    "ComponentResult",
    "ComponentStatus",
    "MetricResult",
    "MetricStatus",
    "ProfileResolution",
    "ResearchFactorResult",
    "ValueQualityResult",
    "aggregate_component",
    "clamp_score",
    "finite_float",
    "metric_input",
    "parse_datetime",
]
