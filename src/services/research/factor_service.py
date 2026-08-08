"""Mapping-based facade for all deterministic research factors."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .catalyst import evaluate_catalyst
from .factor_policy_v1 import POLICY_VERSION
from .profiles import resolve_company_profile
from .risk import evaluate_risk
from .schemas import ResearchFactorResult, parse_datetime
from .trend_timing import evaluate_trend_timing
from .value_quality import evaluate_value_quality


def _mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be a mapping")
    return value


def _bars(value: Any) -> Sequence[Mapping[str, Any]]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("bars must be a sequence")
    if any(not isinstance(item, Mapping) for item in value):
        raise TypeError("every bar must be a mapping")
    return value


class ResearchFactorService:
    """Stateless facade accepting a normalized immutable snapshot mapping."""

    def evaluate(self, snapshot: Mapping[str, Any]) -> ResearchFactorResult:
        if not isinstance(snapshot, Mapping):
            raise TypeError("snapshot must be a mapping")
        stock_code = str(snapshot.get("stock_code") or snapshot.get("code") or "").strip()
        if not stock_code:
            raise ValueError("snapshot.stock_code is required")
        if snapshot.get("as_of") is None:
            raise ValueError("snapshot.as_of is required")
        as_of = parse_datetime(snapshot["as_of"], field="snapshot.as_of")
        company = dict(_mapping(snapshot.get("company"), field="company"))
        for key in ("comp_type", "industry", "name", "stock_name"):
            if key not in company and key in snapshot:
                company[key] = snapshot[key]
        fundamentals = _mapping(snapshot.get("fundamentals"), field="fundamentals")
        bars = _bars(snapshot.get("bars", snapshot.get("daily_bars")))
        catalysts = _mapping(snapshot.get("catalysts"), field="catalysts")
        market_state = _mapping(snapshot.get("market_state", snapshot.get("scenario")), field="market_state")

        profile = resolve_company_profile(company)
        value_quality = evaluate_value_quality(profile.profile, fundamentals)
        return ResearchFactorResult(
            stock_code=stock_code,
            as_of=as_of.isoformat().replace("+00:00", "Z"),
            profile=profile,
            value=value_quality.value,
            quality=value_quality.quality,
            trend_timing=evaluate_trend_timing(bars, as_of=as_of),
            catalyst=evaluate_catalyst(catalysts, as_of=as_of),
            risk=evaluate_risk(
                profile=profile.profile,
                company=company,
                fundamentals=fundamentals,
                bars=bars,
                catalysts=catalysts,
                market_state=market_state,
                as_of=as_of,
            ),
            policy_version=POLICY_VERSION,
        )


def evaluate_research_factors(snapshot: Mapping[str, Any]) -> ResearchFactorResult:
    return ResearchFactorService().evaluate(snapshot)


__all__ = ["ResearchFactorService", "evaluate_research_factors"]
