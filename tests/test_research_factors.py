from __future__ import annotations

import ast
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path

import pytest

from src.services.research.catalyst import evaluate_catalyst
from src.services.research.canonical import canonical_hash
from src.services.research.factor_policy_v1 import (
    POLICY_VERSION,
    PROFILE_RESOLVER_VERSION,
    factor_policy_payload,
)
from src.services.research.factor_service import evaluate_research_factors
from src.services.research.profiles import resolve_company_profile
from src.services.research.risk import evaluate_risk
from src.services.research.schemas import ComponentStatus, MetricResult, MetricStatus
from src.services.research.trend_timing import completed_adjusted_bars, evaluate_trend_timing
from src.services.research.value_quality import evaluate_value_quality


AS_OF = datetime(2025, 6, 30, 18, 0, tzinfo=timezone(timedelta(hours=8)))
PROFILE_COMP_TYPES = {"industrial": 1, "bank": 2, "insurance": 3, "securities": 4}

FUNDAMENTALS = {
    "industrial": {
        "pe_ttm": 18.0,
        "pb": 3.2,
        "ps_ttm": 2.5,
        "dividend_yield": 3.2,
        "roe": 20.0,
        "gross_margin": 42.0,
        "net_margin": 18.0,
        "revenue_growth": 14.0,
        "profit_growth": 22.0,
        "debt_to_assets": 38.0,
    },
    "bank": {
        "pe_ttm": 6.0,
        "pb": 0.65,
        "dividend_yield": 6.0,
        "roe": 12.0,
        "net_interest_margin": 2.1,
        "provision_coverage_ratio": 220.0,
        "nonperforming_loan_ratio": 1.1,
        "capital_adequacy_ratio": 17.0,
    },
    "insurance": {
        "pe_ttm": 11.0,
        "pb": 1.1,
        "dividend_yield": 3.0,
        "roe": 13.0,
        "solvency_adequacy_ratio": 220.0,
        "premium_growth": 11.0,
        "combined_ratio": 94.0,
        "investment_yield": 4.5,
    },
    "securities": {
        "pe_ttm": 17.0,
        "pb": 1.7,
        "dividend_yield": 2.5,
        "roe": 11.0,
        "net_capital_ratio": 190.0,
        "revenue_growth": 18.0,
        "profit_growth": 24.0,
    },
}


def _bars(*, count: int = 300, volatility: float = 0.012, drift: float = 0.0008):
    start = AS_OF - timedelta(days=count - 1)
    result = []
    price = 100.0
    for index in range(count):
        wave = math.sin(index * 0.41) * volatility
        price *= 1.0 + drift + wave
        result.append(
            {
                "trade_date": (start + timedelta(days=index)).isoformat(),
                "close": price,
                "high": price * (1.0 + volatility),
                "low": price * (1.0 - volatility),
                "volume": 1_000_000.0 + index * 1_000.0,
                "completed": True,
            }
        )
    return result


def _empty_catalysts():
    return {kind: {"status": "empty"} for kind in ("forecast", "dividend", "holder", "event")}


def _snapshot(code: str, profile: str, *, market_state=None, fundamentals=None, bars=None, catalysts=None):
    return {
        "stock_code": code,
        "as_of": AS_OF.isoformat(),
        "company": {"comp_type": PROFILE_COMP_TYPES[profile], "name": code},
        "fundamentals": deepcopy(fundamentals if fundamentals is not None else FUNDAMENTALS[profile]),
        "bars": deepcopy(bars if bars is not None else _bars()),
        "catalysts": deepcopy(catalysts if catalysts is not None else _empty_catalysts()),
        "market_state": deepcopy(
            market_state
            if market_state is not None
            else {"trading_status": "normal", "limit_state": "none", "risk_flags": []}
        ),
    }


def test_profile_resolution_prefers_comp_type_then_versioned_industry_and_name_fallbacks():
    assert resolve_company_profile({"comp_type": 1, "industry": "银行"}).profile == "industrial"
    assert resolve_company_profile({"comp_type": "2"}).profile == "bank"
    assert resolve_company_profile({"comp_type": 3}).profile == "insurance"
    assert resolve_company_profile({"comp_type": 4}).profile == "securities"

    industry = resolve_company_profile({"industry": "保险服务"})
    assert (industry.profile, industry.source) == ("insurance", "industry_fallback")
    by_name = resolve_company_profile({"name": "中信证券股份有限公司"})
    assert (by_name.profile, by_name.source) == ("securities", "name_fallback")
    assert industry.resolver_version == by_name.resolver_version == PROFILE_RESOLVER_VERSION
    assert resolve_company_profile({"industry": "食品饮料"}).profile == "industrial"


def test_factor_policy_payload_is_complete_json_stable_and_detached():
    first = factor_policy_payload()
    second = factor_policy_payload()

    assert first == second
    assert json.dumps(first, ensure_ascii=False, sort_keys=True, allow_nan=False)
    assert first["policy_version"] == POLICY_VERSION
    assert set(first["value"]["rules_by_profile"]) == {
        "industrial",
        "bank",
        "insurance",
        "securities",
    }
    assert first["trend_timing"]["primary_horizon"] == 10
    assert first["trend_timing"]["max_completed_adjusted_bars"] == 300
    assert first["risk"]["score_floors"]["suspended"] == 100.0
    assert first["aggregation"]["missing_contributes_zero"] is False

    first["risk"]["score_floors"]["suspended"] = 1.0
    assert second["risk"]["score_floors"]["suspended"] == 100.0


@pytest.mark.parametrize(
    ("stock_code", "profile"),
    [
        ("600519", "industrial"),
        ("601398", "bank"),
        ("601318", "insurance"),
        ("600030", "securities"),
        ("300750", "industrial"),
    ],
)
def test_five_golden_company_profiles_have_stable_independent_factor_outputs(stock_code, profile):
    snapshot = _snapshot(stock_code, profile)
    original = deepcopy(snapshot)
    first = evaluate_research_factors(snapshot)
    second = evaluate_research_factors(snapshot)

    assert snapshot == original
    assert first.to_dict() == second.to_dict()
    assert first.profile.profile == profile
    assert first.policy_version == POLICY_VERSION
    assert first.value.score is not None
    assert first.quality.score is not None
    assert first.trend_timing.score is not None
    assert first.catalyst.score == 50.0
    assert first.risk.score is not None
    assert 0.0 <= first.risk.score <= 100.0
    # Value and quality are separate results and retain their own metric sets.
    assert first.value.name == "value"
    assert first.quality.name == "quality"
    assert first.value.metrics != first.quality.metrics


def test_golden_manifest_scenarios_cover_profiles_market_constraints_and_missing_data():
    path = Path(__file__).parent / "fixtures" / "research_golden" / "cases.json"
    cases = json.loads(path.read_text(encoding="utf-8"))["cases"]
    assert {case["case_id"] for case in cases} >= {
        "consumer_value_quality_600519",
        "bank_601398",
        "growth_volatility_300750",
        "insurance_601318",
        "securities_600030",
        "st_stock",
        "suspended_stock",
        "limit_up_not_buyable",
        "limit_down_not_sellable",
        "missing_data",
    }

    results = {}
    for case in cases:
        profile = case["company_profile"]
        fundamentals = {} if case["case_id"] == "missing_data" else FUNDAMENTALS[profile]
        bars = [] if case["case_id"] == "suspended_stock" else _bars()
        snapshot = _snapshot(
            case["stock_code"],
            profile,
            market_state=case["scenario"],
            fundamentals=fundamentals,
            bars=bars,
        )
        results[case["case_id"]] = evaluate_research_factors(snapshot)

    assert results["st_stock"].risk.score >= 85.0
    assert results["suspended_stock"].risk.score == 100.0
    assert results["limit_up_not_buyable"].risk.score >= 70.0
    assert results["limit_down_not_sellable"].risk.score >= 70.0
    assert results["missing_data"].value.score is None
    assert results["missing_data"].quality.score is None
    assert results["missing_data"].value.status is ComponentStatus.PARTIAL


def test_numeric_factor_golden_v2_locks_inputs_scores_and_full_output_hashes():
    path = Path(__file__).parent / "fixtures" / "research_golden" / "factor_outputs_v2.json"
    fixture = json.loads(path.read_text(encoding="utf-8"))

    assert fixture["fixture_version"] == "research-factor-golden-v2"
    assert fixture["policy_version"] == POLICY_VERSION
    assert canonical_hash(fixture, exclude_volatile=False) == (
        "e5f41a3f8359717897091fe2f847dbc8351d60c1786b19ea4c7eb2c09c9f71e1"
    )

    as_of = datetime.fromisoformat(fixture["as_of"])
    recipe = fixture["bars_recipe"]
    bars = []
    price = float(recipe["initial_price"])
    start = as_of - timedelta(days=int(recipe["count"]) - 1)
    for index in range(int(recipe["count"])):
        volatility = float(recipe["volatility"])
        wave = math.sin(index * float(recipe["wave_radians"])) * volatility
        price *= 1.0 + float(recipe["daily_drift"]) + wave
        bars.append(
            {
                "trade_date": (start + timedelta(days=index)).isoformat(),
                "close": price,
                "high": price * (1.0 + volatility),
                "low": price * (1.0 - volatility),
                "volume": float(recipe["initial_volume"])
                + index * float(recipe["daily_volume_increment"]),
                "completed": bool(recipe["completed"]),
            }
        )

    for case in fixture["cases"]:
        profile = case["profile"]
        snapshot = {
            "stock_code": case["stock_code"],
            "as_of": fixture["as_of"],
            "company": {
                "comp_type": fixture["profile_comp_types"][profile],
                "name": case["stock_code"],
            },
            "fundamentals": (
                {}
                if case.get("omit_fundamentals")
                else deepcopy(fixture["fundamentals"][profile])
            ),
            "bars": [] if case.get("omit_bars") else deepcopy(bars),
            "catalysts": deepcopy(fixture["catalysts"]),
            "market_state": deepcopy(case["scenario"]),
        }
        result = evaluate_research_factors(snapshot).to_dict()
        expected = case["expected"]

        assert result["value"]["score"] == expected["value"], case["id"]
        assert result["quality"]["score"] == expected["quality"], case["id"]
        assert result["trend_timing"]["score"] == expected["trend"], case["id"]
        assert result["catalyst"]["score"] == expected["catalyst"], case["id"]
        assert result["risk"]["score"] == expected["risk"], case["id"]
        assert canonical_hash(result, exclude_volatile=False) == expected[
            "output_sha256"
        ], case["id"]


def test_metric_schema_quantizes_normalized_weights_for_cross_version_hashes():
    metric = MetricResult(
        name="return_5d",
        status=MetricStatus.AVAILABLE,
        value=3.5,
        score=82.0,
        weight=0.08,
        effective_weight=0.08 / (1.0 + 2e-16),
        reason="available",
    )

    assert metric.effective_weight == 0.08


def test_missing_lowers_coverage_without_becoming_zero_and_not_applicable_is_separate():
    fundamentals = deepcopy(FUNDAMENTALS["bank"])
    del fundamentals["net_interest_margin"]
    result = evaluate_value_quality("bank", fundamentals)

    assert result.quality.metric("net_interest_margin").status is MetricStatus.MISSING
    assert result.quality.metric("net_interest_margin").value is None
    assert result.quality.metric("debt_to_assets").status is MetricStatus.NOT_APPLICABLE
    assert result.quality.coverage < 1.0
    assert result.quality.score is not None
    assert sum(metric.effective_weight for metric in result.quality.metrics) == pytest.approx(1.0)

    insufficient = evaluate_value_quality("industrial", {"pe_ttm": 12.0})
    assert insufficient.value.status is ComponentStatus.PARTIAL
    assert insufficient.value.score is None
    assert insufficient.value.metric("pb").value is None
    assert insufficient.value.metric("pb").score is None


def test_explicit_not_applicable_is_removed_from_applicable_weight():
    fundamentals = deepcopy(FUNDAMENTALS["industrial"])
    fundamentals["ps_ttm"] = {"status": "not_applicable", "value": None}
    result = evaluate_value_quality("industrial", fundamentals).value
    assert result.metric("ps_ttm").status is MetricStatus.NOT_APPLICABLE
    assert result.coverage == 1.0
    assert result.score is not None


def test_trend_uses_latest_300_completed_adjusted_bars_and_primary_ten_day_horizon():
    bars = _bars(count=305)
    bars.append(
        {
            "trade_date": (AS_OF + timedelta(days=1)).isoformat(),
            "close": 99999.0,
            "high": 99999.0,
            "low": 99999.0,
            "volume": 1.0,
            "completed": True,
        }
    )
    bars.append(
        {
            "trade_date": AS_OF.isoformat(),
            "close": 88888.0,
            "high": 88888.0,
            "low": 88888.0,
            "volume": 1.0,
            "completed": False,
        }
    )
    result = evaluate_trend_timing(bars, as_of=AS_OF)
    assert result.sample_size == 300
    assert result.primary_horizon == 10
    for metric in ("return_5d", "return_10d", "return_20d", "rsi14", "atr20_pct", "volume_ratio20"):
        assert result.metric(metric).status is MetricStatus.AVAILABLE
    for window in (5, 10, 20, 60, 120, 250):
        assert result.metric(f"ma{window}_slope").status is MetricStatus.AVAILABLE


def test_split_adjustment_removes_raw_close_discontinuity():
    bars = []
    for index in range(30):
        before_split = index < 15
        raw_close = 100.0 + index if before_split else (100.0 + index) / 2.0
        factor = 1.0 if before_split else 2.0
        bars.append(
            {
                "trade_date": (AS_OF - timedelta(days=29 - index)).isoformat(),
                "close": raw_close,
                "high": raw_close * 1.01,
                "low": raw_close * 0.99,
                "volume": 1000.0,
                "adj_factor": factor,
            }
        )
    adjusted = completed_adjusted_bars(bars, as_of=AS_OF)
    split_return = adjusted[15].close / adjusted[14].close - 1.0
    assert abs(split_return) < 0.02


def test_short_trend_history_is_partial_and_never_imputes_long_metrics():
    result = evaluate_trend_timing(_bars(count=18), as_of=AS_OF)
    assert result.status is ComponentStatus.PARTIAL
    assert result.sample_size == 18
    assert result.metric("return_5d").status is MetricStatus.AVAILABLE
    assert result.metric("ma250_slope").status is MetricStatus.MISSING
    assert result.metric("ma250_slope").value is None


def test_catalyst_enforces_as_of_and_distinguishes_empty_from_failed():
    sources = {
        "forecast": {
            "status": "available",
            "items": [
                {"available_at": "2025-06-29T12:00:00+08:00", "change_pct": -40.0},
                {"available_at": "2025-07-01T12:00:00+08:00", "change_pct": 200.0},
            ],
        },
        "dividend": {"status": "empty"},
        "holder": {"status": "fetch_failed"},
        "event": {
            "status": "available",
            "items": [{"available_at": "2025-07-01T12:00:00+08:00", "impact_score": 100.0}],
        },
    }
    result = evaluate_catalyst(sources, as_of=AS_OF)
    assert result.metric("forecast").value == -40.0
    assert result.metric("forecast").score == 5.0
    assert result.metric("dividend").status is MetricStatus.AVAILABLE
    assert result.metric("dividend").score == 50.0
    assert result.metric("holder").status is MetricStatus.MISSING
    assert result.metric("holder").score is None
    assert result.metric("event").status is MetricStatus.MISSING
    assert result.metric("event").reason == "no_observable_items_as_of"


@pytest.mark.parametrize(
    ("market_state", "minimum"),
    [
        ({"trading_status": "normal", "limit_state": "none", "risk_flags": ["st"]}, 85.0),
        ({"trading_status": "suspended", "limit_state": "none", "risk_flags": []}, 100.0),
        ({"trading_status": "normal", "limit_state": "one_price_limit_up", "risk_flags": []}, 70.0),
        ({"trading_status": "normal", "limit_state": "one_price_limit_down", "risk_flags": []}, 70.0),
    ],
)
def test_critical_market_risks_apply_auditable_floors(market_state, minimum):
    result = evaluate_research_factors(_snapshot("SYNTH", "industrial", market_state=market_state))
    assert result.risk.score is not None
    assert result.risk.score >= minimum


def test_risk_uses_profile_aware_leverage_not_industrial_debt_for_financials():
    bank = _snapshot("601398", "bank")
    bank["fundamentals"]["capital_adequacy_ratio"] = 9.0
    bank["fundamentals"]["debt_to_assets"] = 20.0
    bank_result = evaluate_research_factors(bank).risk
    assert bank_result.metric("profile_leverage").value == 9.0
    assert bank_result.metric("profile_leverage").score == 100.0

    industrial = _snapshot("600519", "industrial")
    industrial["fundamentals"]["debt_to_assets"] = 85.0
    industrial_result = evaluate_research_factors(industrial).risk
    assert industrial_result.metric("profile_leverage").score == 100.0


def test_high_volatility_drawdown_and_negative_forecast_raise_risk():
    calm = evaluate_research_factors(_snapshot("CALM", "industrial")).risk
    risky_bars = _bars(volatility=0.09, drift=-0.002)
    catalysts = _empty_catalysts()
    catalysts["forecast"] = {
        "status": "available",
        "items": [{"available_at": "2025-06-29T12:00:00+08:00", "change_pct": -60.0}],
    }
    risky = evaluate_research_factors(
        _snapshot("RISKY", "industrial", bars=risky_bars, catalysts=catalysts)
    ).risk
    assert risky.metric("volatility_60d").score > calm.metric("volatility_60d").score
    assert risky.metric("negative_forecast").score == 100.0
    assert risky.score > calm.score


@pytest.mark.parametrize(
    "mutation",
    [
        lambda snapshot: snapshot["fundamentals"].update({"pe_ttm": float("nan")}),
        lambda snapshot: snapshot["bars"][-1].update({"close": float("inf")}),
        lambda snapshot: snapshot["catalysts"].update(
            {
                "forecast": {
                    "status": "available",
                    "items": [{"available_at": "2025-06-29T12:00:00+08:00", "change_pct": float("nan")}],
                }
            }
        ),
    ],
)
def test_non_finite_inputs_are_rejected(mutation):
    snapshot = _snapshot("600519", "industrial")
    mutation(snapshot)
    with pytest.raises(ValueError, match="NaN|infinity"):
        evaluate_research_factors(snapshot)


def test_factor_modules_have_no_llm_or_analyzer_dependency():
    package = Path(__file__).parents[1] / "src" / "services" / "research"
    for path in package.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.append(node.module or "")
        assert not any("llm" in name.casefold() or "analyzer" in name.casefold() for name in imports), path.name


def test_direct_risk_engine_keeps_failed_forecast_missing_not_safe_zero():
    result = evaluate_risk(
        profile="industrial",
        company={"name": "普通公司"},
        fundamentals=FUNDAMENTALS["industrial"],
        bars=_bars(),
        catalysts={"forecast": {"status": "permission_denied"}},
        market_state={"trading_status": "normal", "limit_state": "none", "risk_flags": []},
        as_of=AS_OF,
    )
    metric = result.metric("negative_forecast")
    assert metric.status is MetricStatus.MISSING
    assert metric.value is None
    assert metric.score is None
