from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import pytest

from src.services.research.factor_input import build_factor_input
from src.services.research.factor_service import evaluate_research_factors


AS_OF = datetime(2025, 6, 30, 18, 0, tzinfo=timezone(timedelta(hours=8)))


def _market_rows(*, count=305, split_index=150, one_price_last=False):
    start = AS_OF.date() - timedelta(days=count - 1)
    daily = []
    factors = []
    for index in range(count):
        trade_date = start + timedelta(days=index)
        economic_price = 100.0 + index
        after_split = index >= split_index
        raw_close = economic_price / 2.0 if after_split else economic_price
        if one_price_last and index == count - 1:
            raw_close = 11.0
            high = low = 11.0
        else:
            high = raw_close * 1.01
            low = raw_close * 0.99
        daily.append(
            {
                "ts_code": "600519.SH",
                "trade_date": trade_date.strftime("%Y%m%d"),
                "open": raw_close * 0.995,
                "high": high,
                "low": low,
                "close": raw_close,
                "vol": 1000.0 + index,
            }
        )
        factors.append(
            {
                "ts_code": "600519.SH",
                "trade_date": trade_date.strftime("%Y%m%d"),
                "adj_factor": 2.0 if after_split else 1.0,
            }
        )
    return daily, factors


def _financial_rows(*, name="贵州茅台", comp_type="1"):
    return {
        "stock_basic": [
            {"ts_code": "600519.SH", "name": name, "industry": "白酒", "list_status": "L"},
            {"ts_code": "601398.SH", "name": "工商银行", "industry": "银行"},
        ],
        "daily_basic": [
            {
                "ts_code": "600519.SH",
                "trade_date": "20250627",
                "pe_ttm": 20.0,
                "pb": 5.0,
                "ps_ttm": 8.0,
                "dv_ttm": 2.5,
            },
            {
                "ts_code": "600519.SH",
                "trade_date": "20250630",
                "pe_ttm": 18.0,
                "pb": 4.5,
                "ps_ttm": 7.0,
                "dv_ttm": 3.0,
            },
            {"ts_code": "601398.SH", "trade_date": "20250630", "pe_ttm": 5.0},
        ],
        "fina_indicator": [
            {
                "ts_code": "600519.SH",
                "end_date": "20250331",
                "ann_date": "20250430",
                "roe_waa": 20.0,
                "grossprofit_margin": 91.0,
                "netprofit_margin": 52.0,
                "tr_yoy": 12.0,
                "netprofit_yoy": 14.0,
                "debt_to_assets": 18.0,
            },
            {
                "ts_code": "600519.SH",
                "end_date": "20250630",
                "ann_date": "20250731",
                "roe_waa": 99.0,
                "grossprofit_margin": 99.0,
                "netprofit_margin": 99.0,
                "tr_yoy": 99.0,
                "netprofit_yoy": 99.0,
                "debt_to_assets": 99.0,
            },
        ],
        "income": [
            {
                "ts_code": "600519.SH",
                "end_date": "20250331",
                "ann_date": "20250430",
                "comp_type": comp_type,
                "total_revenue": 120.0,
                "oper_cost": 12.0,
                "n_income_attr_p": 62.0,
            },
            {
                "ts_code": "600519.SH",
                "end_date": "20240331",
                "ann_date": "20240430",
                "comp_type": comp_type,
                "total_revenue": 100.0,
                "oper_cost": 11.0,
                "n_income_attr_p": 50.0,
            },
            {
                "ts_code": "600519.SH",
                "end_date": "20250630",
                "ann_date": "20250731",
                "comp_type": "2",
                "total_revenue": 999.0,
            },
        ],
    }


def test_daily_and_adj_factor_are_merged_filtered_deduplicated_and_capped_at_300():
    daily, factors = _market_rows()
    first_retained_date = AS_OF.date() - timedelta(days=299)
    daily.append(
        {
            "ts_code": "600519.SH",
            "trade_date": first_retained_date.strftime("%Y%m%d"),
            "open": 102.0,
            "high": 104.0,
            "low": 101.0,
            "close": 103.0,
            "vol": 9999.0,
        }
    )
    daily.extend(
        [
            {
                "ts_code": "600519.SH",
                "trade_date": "20250701",
                "high": 999.0,
                "low": 999.0,
                "close": 999.0,
                "completed": True,
            },
            {
                "ts_code": "600519.SH",
                "trade_date": "20250629",
                "high": 888.0,
                "low": 888.0,
                "close": 888.0,
                "completed": False,
            },
            {"ts_code": "601398.SH", "trade_date": "20250630", "close": 7.0},
        ]
    )
    result = build_factor_input(
        {"daily": daily, "adj_factor": factors},
        stock_code="600519",
        as_of=AS_OF,
    )
    bars = result["bars"]

    assert len(bars) == 300
    assert bars[0]["trade_date"] == first_retained_date.isoformat()
    assert bars[0]["close"] == 103.0  # stable last-row de-duplication
    assert bars[-1]["trade_date"] == "2025-06-30"
    assert [bar["trade_date"] for bar in bars] == sorted({bar["trade_date"] for bar in bars})
    assert all(bar["completed"] is True for bar in bars)
    assert all("adj_close" in bar for bar in bars)


def test_forward_adjusted_close_removes_split_discontinuity():
    daily, factors = _market_rows(count=40, split_index=20)
    bars = build_factor_input(
        {"daily": daily, "adj_factor": factors},
        stock_code="600519",
        as_of=AS_OF,
    )["bars"]
    split_return = bars[20]["adj_close"] / bars[19]["adj_close"] - 1.0
    assert abs(split_return) < 0.02
    assert bars[0]["adj_factor"] == 1.0
    assert bars[-1]["adj_factor"] == 2.0
    assert bars[0]["adjustment_status"] == "available"


def test_missing_adjustment_factor_keeps_raw_price_with_explicit_missing_status():
    daily, _ = _market_rows(count=3, split_index=99)
    bars = build_factor_input({"daily": daily}, stock_code="600519", as_of=AS_OF)["bars"]
    assert [bar["adj_close"] for bar in bars] == [bar["close"] for bar in bars]
    assert {bar["adjustment_status"] for bar in bars} == {"missing"}
    assert all("adj_factor" not in bar for bar in bars)


def test_same_day_unmarked_bar_is_only_completed_after_market_close():
    daily, factors = _market_rows(count=2, split_index=99)
    before_close = AS_OF.replace(hour=14, minute=59)
    result = build_factor_input(
        {"daily": daily, "adj_factor": factors},
        stock_code="600519",
        as_of=before_close,
    )
    assert result["bars"][-1]["trade_date"] == "2025-06-29"


def test_company_profile_and_fundamentals_use_latest_observable_rows_not_future_announcements():
    rows = _financial_rows()
    result = build_factor_input(rows, stock_code="600519", as_of=AS_OF)
    fundamentals = result["fundamentals"]

    assert result["company"] == {"name": "贵州茅台", "industry": "白酒", "comp_type": "1"}
    assert fundamentals["pe_ttm"] == {"status": "available", "value": 18.0}
    assert fundamentals["roe"] == {"status": "available", "value": 20.0}
    assert fundamentals["gross_margin"] == {"status": "available", "value": 91.0}
    assert fundamentals["debt_to_assets"] == {"status": "available", "value": 18.0}
    assert fundamentals["net_interest_margin"] == {"status": "not_applicable", "value": None}
    assert all(item["value"] != 99.0 for item in fundamentals.values())


def test_derived_growth_and_margin_are_used_when_indicator_fields_are_missing():
    rows = _financial_rows()
    rows["fina_indicator"] = []
    result = build_factor_input(rows, stock_code="600519", as_of=AS_OF)
    fundamentals = result["fundamentals"]
    assert fundamentals["gross_margin"]["value"] == pytest.approx(90.0)
    assert fundamentals["net_margin"]["value"] == pytest.approx(62 / 120 * 100)
    assert fundamentals["revenue_growth"]["value"] == pytest.approx(20.0)
    assert fundamentals["profit_growth"]["value"] == pytest.approx(24.0)


def test_missing_applicable_financial_value_stays_missing_and_never_becomes_zero():
    rows = _financial_rows()
    rows["fina_indicator"] = []
    rows["income"] = [
        {"ts_code": "600519.SH", "end_date": "20250331", "ann_date": "20250430", "comp_type": "1"}
    ]
    result = build_factor_input(rows, stock_code="600519", as_of=AS_OF)
    metric = result["fundamentals"]["roe"]
    assert metric == {"status": "missing", "value": None}
    assert metric["value"] != 0


def test_bank_profile_uses_financial_fields_and_marks_industrial_metrics_not_applicable():
    rows = _financial_rows(name="工商银行", comp_type="2")
    rows["stock_basic"][0]["industry"] = "银行"
    rows["bank_indicator"] = [
        {
            "ts_code": "600519.SH",
            "end_date": "20250331",
            "ann_date": "20250430",
            "netint_margin": 2.1,
            "provision_cover": 220.0,
            "npl_ratio": 1.1,
            "capital_adequacy": 17.0,
        }
    ]
    result = build_factor_input(rows, stock_code="600519", as_of=AS_OF)
    fundamentals = result["fundamentals"]
    assert result["company"]["comp_type"] == "2"
    assert fundamentals["net_interest_margin"]["value"] == 2.1
    assert fundamentals["capital_adequacy_ratio"]["value"] == 17.0
    assert fundamentals["gross_margin"] == {"status": "not_applicable", "value": None}
    assert fundamentals["ps_ttm"] == {"status": "not_applicable", "value": None}


def test_forecast_dividend_and_holder_catalysts_are_as_of_safe():
    rows = _financial_rows()
    rows.update(
        {
            "forecast": [
                {
                    "ts_code": "600519.SH",
                    "ann_date": "20250620",
                    "type": "预增",
                    "p_change_min": 10.0,
                    "p_change_max": 20.0,
                },
                {
                    "ts_code": "600519.SH",
                    "ann_date": "20250701",
                    "type": "预减",
                    "p_change_min": -90.0,
                    "p_change_max": -80.0,
                },
            ],
            "dividend": [
                {"ts_code": "600519.SH", "ann_date": "20250615", "cash_div_tax": 2.0}
            ],
            "stk_holdernumber": [
                {"ts_code": "600519.SH", "ann_date": "20250620", "end_date": "20250615", "holder_num": 800},
                {"ts_code": "600519.SH", "ann_date": "20250320", "end_date": "20250315", "holder_num": 1000},
            ],
        }
    )
    catalysts = build_factor_input(rows, stock_code="600519", as_of=AS_OF)["catalysts"]

    assert catalysts["forecast"]["status"] == "available"
    assert catalysts["forecast"]["items"] == [
        {"available_at": "2025-06-19T16:00:00Z", "type": "预增", "change_pct": 15.0}
    ]
    assert catalysts["dividend"]["items"][0]["dividend_yield"] == 3.0
    assert catalysts["holder"]["items"][0]["change_pct"] == pytest.approx(20.0)
    assert catalysts["event"] == {"status": "missing", "items": []}


def test_explicit_empty_catalyst_dataset_differs_from_missing_and_future_only():
    empty = build_factor_input({"forecast": []}, stock_code="600519", as_of=AS_OF)["catalysts"]
    assert empty["forecast"]["status"] == "empty"
    assert empty["dividend"]["status"] == "missing"

    future = build_factor_input(
        {"forecast": [{"ts_code": "600519.SH", "ann_date": "20250701", "p_change_min": 99.0}]},
        stock_code="600519",
        as_of=AS_OF,
    )["catalysts"]
    assert future["forecast"] == {"status": "partial", "items": []}


def test_st_suspend_and_one_price_limit_form_market_state_with_suspension_precedence():
    daily, factors = _market_rows(count=3, split_index=99, one_price_last=True)
    rows = {
        "daily": daily,
        "adj_factor": factors,
        "stock_basic": [{"ts_code": "600519.SH", "name": "*ST测试", "industry": "制造"}],
        "stk_limit": [
            {"ts_code": "600519.SH", "trade_date": "20250630", "up_limit": 11.0, "down_limit": 9.0}
        ],
        "suspend_d": [{"ts_code": "600519.SH", "trade_date": "20250630", "suspend_type": "S"}],
    }
    state = build_factor_input(rows, stock_code="600519", as_of=AS_OF)["market_state"]
    assert state["is_st"] is True
    assert state["trading_status"] == "suspended"
    assert state["limit_state"] == "one_price_limit_up"
    assert state["buy_executable"] is False
    assert state["sell_executable"] is False
    assert state["risk_flags"] == ["limit_up", "st", "suspended"]


def test_resume_event_and_scenario_override_are_deterministic():
    rows = {
        "stock_basic": [{"ts_code": "600519.SH", "name": "普通公司"}],
        "suspend_d": [
            {"ts_code": "600519.SH", "trade_date": "20250620", "suspend_type": "S"},
            {"ts_code": "600519.SH", "trade_date": "20250625", "suspend_type": "R"},
        ],
    }
    state = build_factor_input(
        rows,
        stock_code="600519",
        as_of=AS_OF,
        scenario={"limit_state": "one_price_limit_down", "risk_flags": ["manual"]},
    )["market_state"]
    assert state["trading_status"] == "normal"
    assert state["limit_state"] == "one_price_limit_down"
    assert state["buy_executable"] is True
    assert state["sell_executable"] is False
    assert state["risk_flags"] == ["limit_down", "manual"]


def test_output_is_stable_pure_and_consumable_by_factor_engine():
    daily, factors = _market_rows(count=300)
    rows = _financial_rows()
    rows.update({"daily": daily, "adj_factor": factors, "forecast": [], "dividend": [], "holder": [], "event": []})
    original = deepcopy(rows)
    first = build_factor_input(rows, stock_code="600519.SH", as_of=AS_OF, market="A")
    second = build_factor_input(rows, stock_code="600519.SH", as_of=AS_OF, market="A")

    assert rows == original
    assert first == second
    evaluated = evaluate_research_factors(first)
    assert evaluated.stock_code == "600519"
    assert evaluated.profile.profile == "industrial"
    assert evaluated.value.score is not None
    assert evaluated.trend_timing.sample_size == 300


@pytest.mark.parametrize(
    "rows",
    [
        {"daily": [{"trade_date": "20250630", "close": float("nan")}]},
        {
            "daily": [{"trade_date": "20250630", "close": 10.0}],
            "adj_factor": [{"trade_date": "20250630", "adj_factor": float("inf")}],
        },
        {
            "income": [
                {
                    "end_date": "20250331",
                    "ann_date": "20250430",
                    "comp_type": "1",
                    "total_revenue": float("nan"),
                }
            ]
        },
    ],
)
def test_non_finite_provider_values_are_rejected(rows):
    with pytest.raises(ValueError, match="NaN|infinity"):
        build_factor_input(rows, stock_code="600519", as_of=AS_OF)


def test_invalid_rows_contract_and_stock_code_are_rejected():
    with pytest.raises(TypeError, match="must be a mapping"):
        build_factor_input([], stock_code="600519", as_of=AS_OF)
    with pytest.raises(TypeError, match="must be a sequence"):
        build_factor_input({"daily": {}}, stock_code="600519", as_of=AS_OF)
    with pytest.raises(ValueError, match="unsupported A-share"):
        build_factor_input({}, stock_code="AAPL", as_of=AS_OF)


def test_date_only_as_of_means_end_of_a_share_day():
    daily = [{"ts_code": "600519.SH", "trade_date": "20250630", "close": 10.0}]
    result = build_factor_input({"daily": daily}, stock_code="600519", as_of=date(2025, 6, 30))
    assert result["bars"][0]["trade_date"] == "2025-06-30"
