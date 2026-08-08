"""Versioned bands and weights for the deterministic factor engines."""

from __future__ import annotations

from dataclasses import dataclass
from bisect import bisect_right
from typing import Any, Mapping, Tuple

from .schemas import clamp_score, finite_float


POLICY_VERSION = "factor-policy-v1"
PROFILE_RESOLVER_VERSION = "company-profile-v1"
PRIMARY_TREND_HORIZON = 10
MAX_TREND_BARS = 300

MINIMUM_COVERAGE: Mapping[str, float] = {
    "value": 0.55,
    "quality": 0.55,
    "trend_timing": 0.50,
    "catalyst": 0.50,
    "risk": 0.45,
}


@dataclass(frozen=True)
class BandRule:
    name: str
    aliases: Tuple[str, ...]
    weight: float
    thresholds: Tuple[float, ...]
    scores: Tuple[float, ...]

    def __post_init__(self) -> None:
        if len(self.scores) != len(self.thresholds) + 1:
            raise ValueError(f"{self.name}: scores must contain one more item than thresholds")
        if tuple(sorted(self.thresholds)) != self.thresholds:
            raise ValueError(f"{self.name}: thresholds must be sorted")
        finite_float(self.weight, field=f"{self.name}.weight")
        for threshold in self.thresholds:
            finite_float(threshold, field=f"{self.name}.threshold")
        for score in self.scores:
            clamp_score(score)


def score_by_rule(value: float, rule: BandRule) -> float:
    number = finite_float(value, field=rule.name)
    return clamp_score(rule.scores[bisect_right(rule.thresholds, number)])


VALUE_RULES: Mapping[str, Mapping[str, BandRule]] = {
    "industrial": {
        "pe_ttm": BandRule("pe_ttm", ("pe_ttm", "pe"), 0.30, (10, 18, 30, 50), (95, 82, 62, 35, 10)),
        "pb": BandRule("pb", ("pb", "pb_mrq"), 0.25, (1, 2, 4, 7), (95, 82, 58, 30, 10)),
        "ps_ttm": BandRule("ps_ttm", ("ps_ttm", "ps"), 0.20, (1, 2, 4, 8), (95, 80, 58, 30, 10)),
        "dividend_yield": BandRule(
            "dividend_yield", ("dividend_yield", "dv_ttm"), 0.25, (1, 2, 4, 6), (15, 35, 65, 85, 100)
        ),
    },
    "bank": {
        "pe_ttm": BandRule("pe_ttm", ("pe_ttm", "pe"), 0.35, (4, 7, 10, 15), (98, 88, 68, 38, 12)),
        "pb": BandRule("pb", ("pb", "pb_mrq"), 0.40, (0.5, 0.8, 1.2, 2), (98, 86, 62, 32, 10)),
        "dividend_yield": BandRule(
            "dividend_yield", ("dividend_yield", "dv_ttm"), 0.25, (2, 4, 6, 8), (15, 40, 70, 90, 100)
        ),
    },
    "insurance": {
        "pe_ttm": BandRule("pe_ttm", ("pe_ttm", "pe"), 0.35, (7, 12, 18, 28), (95, 82, 62, 35, 12)),
        "pb": BandRule("pb", ("pb", "pb_mrq"), 0.40, (0.8, 1.3, 2, 3.5), (95, 82, 60, 32, 10)),
        "dividend_yield": BandRule(
            "dividend_yield", ("dividend_yield", "dv_ttm"), 0.25, (1, 2.5, 4.5, 7), (15, 40, 70, 90, 100)
        ),
    },
    "securities": {
        "pe_ttm": BandRule("pe_ttm", ("pe_ttm", "pe"), 0.35, (10, 18, 30, 50), (95, 82, 60, 32, 10)),
        "pb": BandRule("pb", ("pb", "pb_mrq"), 0.40, (1, 1.8, 3, 5), (95, 82, 60, 32, 10)),
        "dividend_yield": BandRule(
            "dividend_yield", ("dividend_yield", "dv_ttm"), 0.25, (1, 2, 4, 6), (15, 38, 68, 88, 100)
        ),
    },
}

VALUE_METRIC_UNIVERSE = ("pe_ttm", "pb", "ps_ttm", "dividend_yield")

QUALITY_RULES: Mapping[str, Mapping[str, BandRule]] = {
    "industrial": {
        "roe": BandRule("roe", ("roe", "roe_waa"), 0.22, (5, 10, 15, 22), (10, 35, 62, 82, 100)),
        "gross_margin": BandRule("gross_margin", ("gross_margin", "grossprofit_margin"), 0.14, (10, 20, 35, 50), (10, 35, 62, 82, 100)),
        "net_margin": BandRule("net_margin", ("net_margin", "netprofit_margin"), 0.14, (3, 8, 15, 25), (10, 35, 62, 82, 100)),
        "revenue_growth": BandRule("revenue_growth", ("revenue_growth", "tr_yoy"), 0.16, (-10, 0, 10, 25), (5, 25, 55, 78, 100)),
        "profit_growth": BandRule("profit_growth", ("profit_growth", "netprofit_yoy"), 0.18, (-20, 0, 15, 35), (5, 25, 55, 78, 100)),
        "debt_to_assets": BandRule("debt_to_assets", ("debt_to_assets", "debt_to_asset"), 0.16, (30, 50, 65, 80), (100, 82, 58, 30, 8)),
    },
    "bank": {
        "roe": BandRule("roe", ("roe", "roe_waa"), 0.20, (6, 9, 12, 16), (10, 35, 62, 82, 100)),
        "net_interest_margin": BandRule("net_interest_margin", ("net_interest_margin", "netint_margin"), 0.18, (1.2, 1.8, 2.3, 3), (10, 35, 62, 82, 100)),
        "provision_coverage_ratio": BandRule("provision_coverage_ratio", ("provision_coverage_ratio", "provision_cover"), 0.20, (120, 180, 250, 350), (8, 32, 60, 82, 100)),
        "nonperforming_loan_ratio": BandRule("nonperforming_loan_ratio", ("nonperforming_loan_ratio", "npl_ratio"), 0.22, (0.8, 1.2, 1.8, 3), (100, 82, 58, 30, 5)),
        "capital_adequacy_ratio": BandRule("capital_adequacy_ratio", ("capital_adequacy_ratio", "capital_adequacy"), 0.20, (10.5, 13, 16, 20), (8, 35, 62, 82, 100)),
    },
    "insurance": {
        "roe": BandRule("roe", ("roe", "roe_waa"), 0.24, (5, 9, 13, 18), (10, 35, 62, 82, 100)),
        "solvency_adequacy_ratio": BandRule("solvency_adequacy_ratio", ("solvency_adequacy_ratio", "solvency_ratio"), 0.24, (100, 150, 200, 260), (5, 30, 60, 82, 100)),
        "premium_growth": BandRule("premium_growth", ("premium_growth", "premium_yoy"), 0.18, (-10, 0, 10, 20), (5, 28, 58, 82, 100)),
        "combined_ratio": BandRule("combined_ratio", ("combined_ratio",), 0.18, (85, 95, 100, 110), (100, 82, 58, 30, 5)),
        "investment_yield": BandRule("investment_yield", ("investment_yield",), 0.16, (2, 3.5, 5, 7), (10, 35, 65, 85, 100)),
    },
    "securities": {
        "roe": BandRule("roe", ("roe", "roe_waa"), 0.28, (4, 8, 12, 18), (8, 32, 60, 82, 100)),
        "net_capital_ratio": BandRule("net_capital_ratio", ("net_capital_ratio",), 0.24, (100, 140, 180, 240), (5, 30, 60, 82, 100)),
        "revenue_growth": BandRule("revenue_growth", ("revenue_growth", "tr_yoy"), 0.22, (-15, 0, 15, 35), (5, 28, 58, 82, 100)),
        "profit_growth": BandRule("profit_growth", ("profit_growth", "netprofit_yoy"), 0.26, (-25, 0, 20, 45), (5, 28, 58, 82, 100)),
    },
}

QUALITY_METRIC_UNIVERSE = (
    "roe",
    "gross_margin",
    "net_margin",
    "revenue_growth",
    "profit_growth",
    "debt_to_assets",
    "net_interest_margin",
    "provision_coverage_ratio",
    "nonperforming_loan_ratio",
    "capital_adequacy_ratio",
    "solvency_adequacy_ratio",
    "premium_growth",
    "combined_ratio",
    "investment_yield",
    "net_capital_ratio",
)

TREND_RULES: Mapping[str, BandRule] = {
    "return_5d": BandRule("return_5d", ("return_5d",), 0.08, (-8, -2, 2, 8), (5, 25, 55, 82, 100)),
    "return_10d": BandRule("return_10d", ("return_10d",), 0.20, (-12, -3, 3, 12), (5, 25, 55, 82, 100)),
    "return_20d": BandRule("return_20d", ("return_20d",), 0.12, (-18, -5, 5, 18), (5, 25, 55, 82, 100)),
    "ma5_slope": BandRule("ma5_slope", ("ma5_slope",), 0.05, (-1, -0.2, 0.2, 1), (5, 25, 55, 82, 100)),
    "ma10_slope": BandRule("ma10_slope", ("ma10_slope",), 0.06, (-0.8, -0.15, 0.15, 0.8), (5, 25, 55, 82, 100)),
    "ma20_slope": BandRule("ma20_slope", ("ma20_slope",), 0.07, (-0.6, -0.1, 0.1, 0.6), (5, 25, 55, 82, 100)),
    "ma60_slope": BandRule("ma60_slope", ("ma60_slope",), 0.06, (-0.4, -0.05, 0.05, 0.4), (5, 25, 55, 82, 100)),
    "ma120_slope": BandRule("ma120_slope", ("ma120_slope",), 0.04, (-0.25, -0.03, 0.03, 0.25), (5, 25, 55, 82, 100)),
    "ma250_slope": BandRule("ma250_slope", ("ma250_slope",), 0.03, (-0.15, -0.02, 0.02, 0.15), (5, 25, 55, 82, 100)),
    "rsi14": BandRule("rsi14", ("rsi14",), 0.09, (25, 45, 65, 75), (35, 58, 82, 60, 25)),
    "atr20_pct": BandRule("atr20_pct", ("atr20_pct",), 0.08, (1.5, 3, 5, 8), (90, 78, 58, 30, 8)),
    "volume_ratio20": BandRule("volume_ratio20", ("volume_ratio20",), 0.06, (0.6, 0.9, 1.5, 3), (25, 50, 78, 65, 35)),
    "drawdown_pct": BandRule("drawdown_pct", ("drawdown_pct",), 0.06, (3, 8, 15, 30), (95, 80, 58, 30, 8)),
}

CATALYST_RULES: Mapping[str, BandRule] = {
    "forecast": BandRule("forecast", ("forecast",), 0.35, (-30, -5, 5, 30), (5, 25, 50, 78, 100)),
    "dividend": BandRule("dividend", ("dividend",), 0.20, (0.5, 1.5, 3, 5), (35, 50, 68, 85, 100)),
    "holder": BandRule("holder", ("holder",), 0.20, (-3, -0.2, 0.2, 3), (5, 25, 50, 78, 100)),
    "event": BandRule("event", ("event",), 0.25, (-60, -10, 10, 60), (5, 25, 50, 78, 100)),
}
CATALYST_EMPTY_SCORE = 50.0
CATALYST_DIRECTION_VALUES: Mapping[str, float] = {
    "positive": 20.0,
    "increase": 20.0,
    "buy": 20.0,
    "增持": 20.0,
    "预增": 30.0,
    "略增": 15.0,
    "扭亏": 25.0,
    "negative": -20.0,
    "decrease": -20.0,
    "sell": -20.0,
    "减持": -20.0,
    "预减": -30.0,
    "略减": -15.0,
    "首亏": -35.0,
    "续亏": -30.0,
    "neutral": 0.0,
    "unchanged": 0.0,
}

RISK_RULES: Mapping[str, BandRule] = {
    "st": BandRule("st", ("st",), 0.15, (0.5,), (0, 100)),
    "suspended": BandRule("suspended", ("suspended",), 0.15, (0.5,), (0, 100)),
    "one_price_limit": BandRule("one_price_limit", ("one_price_limit",), 0.10, (0.5,), (0, 100)),
    "volatility_60d": BandRule("volatility_60d", ("volatility_60d",), 0.15, (15, 30, 50, 80), (5, 25, 55, 82, 100)),
    "drawdown_300d": BandRule("drawdown_300d", ("drawdown_300d",), 0.15, (5, 12, 25, 45), (5, 25, 55, 82, 100)),
    "negative_forecast": BandRule("negative_forecast", ("negative_forecast",), 0.10, (-40, -10, 0, 20), (100, 75, 40, 15, 5)),
}

RISK_LEVERAGE_RULES: Mapping[str, BandRule] = {
    "industrial": BandRule("profile_leverage", ("debt_to_assets", "debt_to_asset"), 0.20, (30, 50, 65, 80), (5, 25, 55, 82, 100)),
    # Financial-company leverage is not compared with industrial debt ratios.
    "bank": BandRule("profile_leverage", ("capital_adequacy_ratio", "capital_adequacy"), 0.20, (10.5, 13, 16, 20), (100, 75, 45, 18, 5)),
    "insurance": BandRule("profile_leverage", ("solvency_adequacy_ratio", "solvency_ratio"), 0.20, (100, 150, 200, 260), (100, 75, 45, 18, 5)),
    "securities": BandRule("profile_leverage", ("net_capital_ratio",), 0.20, (100, 140, 180, 240), (100, 75, 45, 18, 5)),
}

RISK_SCORE_FLOORS: Mapping[str, float] = {
    "st": 85.0,
    "suspended": 100.0,
    "one_price_limit": 70.0,
}
RISK_EMPTY_FORECAST_SCORE = 0.0


def _band_rule_payload(rule: BandRule) -> dict[str, Any]:
    return {
        "name": rule.name,
        "aliases": list(rule.aliases),
        "weight": rule.weight,
        "thresholds": list(rule.thresholds),
        "scores": list(rule.scores),
    }


def _profile_rule_payload(
    rules: Mapping[str, Mapping[str, BandRule]],
) -> dict[str, Any]:
    return {
        profile: {
            metric: _band_rule_payload(profile_rules[metric])
            for metric in sorted(profile_rules)
        }
        for profile, profile_rules in sorted(rules.items())
    }


def _flat_rule_payload(rules: Mapping[str, BandRule]) -> dict[str, Any]:
    return {metric: _band_rule_payload(rules[metric]) for metric in sorted(rules)}


def factor_policy_payload() -> dict[str, Any]:
    """Return the complete JSON-safe policy fingerprint input.

    The payload intentionally contains calculation windows and classification
    semantics as well as score bands.  A caller can therefore fingerprint the
    actual deterministic policy instead of relying on a version label alone.
    A new detached tree is returned on every call so consumers cannot mutate
    the process-wide policy constants.
    """

    # Imported lazily to avoid a module cycle: profiles consumes the resolver
    # version declared above, while its ordered fallbacks are part of the
    # policy identity exposed here.
    from .profiles import COMP_TYPE_PROFILES, PROFILE_KEYWORDS

    return {
        "policy_version": POLICY_VERSION,
        "profile_resolver": {
            "version": PROFILE_RESOLVER_VERSION,
            "comp_type_profiles": {
                key: COMP_TYPE_PROFILES[key] for key in sorted(COMP_TYPE_PROFILES)
            },
            "fallback_order": ["bank", "insurance", "securities"],
            "keywords": {
                profile: list(PROFILE_KEYWORDS[profile])
                for profile in ("bank", "insurance", "securities")
            },
            "default_profile": "industrial",
        },
        "aggregation": {
            "minimum_coverage": {
                component: MINIMUM_COVERAGE[component]
                for component in sorted(MINIMUM_COVERAGE)
            },
            "missing_contributes_zero": False,
            "not_applicable_removed_from_denominator": True,
            "available_weights_renormalized": True,
            "score_min": 0.0,
            "score_max": 100.0,
            "score_round_digits": 4,
            "coverage_round_digits": 6,
            "comparison_epsilon": 1e-12,
        },
        "value": {
            "metric_universe": list(VALUE_METRIC_UNIVERSE),
            "rules_by_profile": _profile_rule_payload(VALUE_RULES),
        },
        "quality": {
            "metric_universe": list(QUALITY_METRIC_UNIVERSE),
            "rules_by_profile": _profile_rule_payload(QUALITY_RULES),
        },
        "trend_timing": {
            "rules": _flat_rule_payload(TREND_RULES),
            "max_completed_adjusted_bars": MAX_TREND_BARS,
            "primary_horizon": PRIMARY_TREND_HORIZON,
            "return_horizons": [5, 10, 20],
            "moving_average_windows": [5, 10, 20, 60, 120, 250],
            "moving_average_slope_periods": 5,
            "rsi_period": 14,
            "atr_window": 20,
            "volume_ratio_window": 20,
            "risk_volatility_window": 60,
            "annual_trading_days": 252,
            "drawdown_window": MAX_TREND_BARS,
        },
        "catalyst": {
            "rules": _flat_rule_payload(CATALYST_RULES),
            "empty_score": CATALYST_EMPTY_SCORE,
            "direction_values": {
                key: CATALYST_DIRECTION_VALUES[key]
                for key in sorted(CATALYST_DIRECTION_VALUES)
            },
            "failed_source_statuses": [
                "fetch_failed",
                "missing",
                "not_supported",
                "permission_denied",
                "stale",
            ],
            "accepted_source_statuses": ["available", "empty", "partial"],
            "observable_time_fields": [
                "available_at",
                "published_at",
                "announced_at",
                "created_at",
            ],
            "signal_fields": {
                "forecast": [
                    "change_pct",
                    "profit_change_pct",
                    "p_change_min",
                    "forecast_change_pct",
                    "direction",
                    "type",
                ],
                "dividend": [
                    "dividend_yield",
                    "cash_dividend_yield",
                    "yield_pct",
                ],
                "holder": [
                    "change_pct",
                    "holder_change_pct",
                    "holding_change_pct",
                    "direction",
                    "type",
                ],
                "event": [
                    "impact_score",
                    "event_impact",
                    "sentiment_score",
                    "sentiment",
                    "direction",
                ],
            },
        },
        "risk": {
            "rules": _flat_rule_payload(RISK_RULES),
            "leverage_rules_by_profile": {
                profile: _band_rule_payload(RISK_LEVERAGE_RULES[profile])
                for profile in sorted(RISK_LEVERAGE_RULES)
            },
            "score_floors": {
                key: RISK_SCORE_FLOORS[key] for key in sorted(RISK_SCORE_FLOORS)
            },
            "empty_forecast_score": RISK_EMPTY_FORECAST_SCORE,
            "st_name_prefixes": ["ST", "*ST", "SST", "S*ST"],
            "suspended_states": ["suspended", "halted", "停牌"],
            "normal_states": ["normal", "trading", "active", "正常"],
            "one_price_limit_states": [
                "one_price_limit_up",
                "one_price_limit_down",
                "一字涨停",
                "一字跌停",
            ],
            "limit_risk_flags": [
                "limit_up",
                "limit_down",
                "one_price_limit_up",
                "one_price_limit_down",
            ],
        },
    }


__all__ = [
    "BandRule",
    "CATALYST_EMPTY_SCORE",
    "CATALYST_DIRECTION_VALUES",
    "CATALYST_RULES",
    "MAX_TREND_BARS",
    "MINIMUM_COVERAGE",
    "POLICY_VERSION",
    "PRIMARY_TREND_HORIZON",
    "PROFILE_RESOLVER_VERSION",
    "QUALITY_METRIC_UNIVERSE",
    "QUALITY_RULES",
    "RISK_LEVERAGE_RULES",
    "RISK_EMPTY_FORECAST_SCORE",
    "RISK_RULES",
    "RISK_SCORE_FLOORS",
    "TREND_RULES",
    "VALUE_METRIC_UNIVERSE",
    "VALUE_RULES",
    "factor_policy_payload",
    "score_by_rule",
]
