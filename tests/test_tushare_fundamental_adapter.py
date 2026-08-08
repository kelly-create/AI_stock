# -*- coding: utf-8 -*-
"""Regression tests for the production Tushare fundamental adapter."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor as RealThreadPoolExecutor
from datetime import date, timedelta
from threading import Lock
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data_provider.tushare_fundamental_adapter import (
    TushareFundamentalAdapter,
    _latest_row,
    _to_ts_code,
)


def _compact(day: date) -> str:
    return day.strftime("%Y%m%d")


def test_to_ts_code_handles_shenzhen_shanghai_and_beijing() -> None:
    assert _to_ts_code("600519") == "600519.SH"
    assert _to_ts_code("sz000001") == "000001.SZ"
    assert _to_ts_code("920001") == "920001.BJ"
    assert _to_ts_code("688001.SS") == "688001.SH"


def test_latest_row_rejects_batches_with_only_future_or_invalid_periods() -> None:
    tomorrow = _compact(date.today() + timedelta(days=1))
    frame = pd.DataFrame(
        [
            {"end_date": tomorrow, "value": "future"},
            {"end_date": "not-a-date", "value": "invalid"},
        ]
    )

    assert _latest_row(frame) is None


def test_latest_row_fails_closed_when_the_date_column_drifts() -> None:
    frame = pd.DataFrame([{"unexpected_date": "20260320", "value": "unproven"}])

    assert _latest_row(frame, date_column="end_date") is None


def test_missing_token_is_not_supported_without_building_client() -> None:
    adapter = TushareFundamentalAdapter()
    with patch("src.config.get_config", return_value=SimpleNamespace(tushare_token="")), \
            patch.object(adapter, "_build_client") as build_client:
        result = adapter.get_fundamental_bundle("600519")

    assert result["status"] == "not_supported"
    assert result["valuation"] == {}
    build_client.assert_not_called()


def test_preloaded_frames_build_bundle_without_token_or_second_provider_call() -> None:
    adapter = TushareFundamentalAdapter()
    trade_date = _compact(date.today() - timedelta(days=1))
    frames = {
        "daily_basic": pd.DataFrame(
            [{"trade_date": trade_date, "pe_ttm": 18.5, "pb": 4.2, "total_mv": 10.0}]
        )
    }

    with patch("src.config.get_config") as get_config, patch.object(
        adapter, "_build_client"
    ) as build_client, patch.object(adapter, "_fetch_frames") as fetch_frames:
        result = adapter.get_fundamental_bundle(
            "600519",
            preloaded_frames=frames,
            preloaded_errors=("forecast:permission_denied",),
        )

    assert result["valuation"]["pe_ratio"] == 18.5
    assert result["valuation"]["total_mv"] == 100_000.0
    assert result["errors"] == ["forecast:permission_denied"]
    get_config.assert_not_called()
    build_client.assert_not_called()
    fetch_frames.assert_not_called()


@pytest.mark.parametrize(
    ("research_enabled", "expected_url"),
    [
        (False, "https://legacy.example/tushare"),
        (True, "https://research.example/tushare"),
    ],
)
def test_fundamental_client_preserves_flag_off_url_compatibility(
    research_enabled: bool,
    expected_url: str,
) -> None:
    config = SimpleNamespace(tushare_research_enabled=research_enabled)
    adapter = TushareFundamentalAdapter()

    with patch.dict(
        "os.environ",
        {
            "TUSHARE_API_URL": "https://research.example/tushare",
            "TUSHARE_HTTP_URL": "https://legacy.example/tushare",
        },
        clear=True,
    ):
        client = adapter._build_client("demo-token", config=config)

    assert client._api_url == expected_url


def test_adapter_normalizes_paid_fundamentals_and_preserves_source_lineage() -> None:
    adapter = TushareFundamentalAdapter()
    recent_day = date.today() - timedelta(days=2)
    dividend_day = date.today() - timedelta(days=30)
    report_period = "20251231"
    previous_period = "20241231"
    frames = {
        "daily_basic": pd.DataFrame([
            {
                "ts_code": "600519.SH",
                "trade_date": _compact(recent_day),
                "close": 1500,
                "pe": 25,
                "pe_ttm": 22,
                "pb": 8,
                "ps_ttm": 10,
                "dv_ttm": 3.2,
                "turnover_rate": 0.4,
                "volume_ratio": 1.1,
                "total_mv": 123.0,
                "circ_mv": 100.0,
            }
        ]),
        "fina_indicator": pd.DataFrame([
            {
                "end_date": report_period,
                "ann_date": "20260320",
                "eps": 5.2,
                "bps": 20,
                "ocfps": 6,
                "or_yoy": 12.5,
                "netprofit_yoy": 15.5,
                "roe_waa": 28,
                "grossprofit_margin": 91,
                "netprofit_margin": 52,
                "debt_to_assets": 32,
            }
        ]),
        "income": pd.DataFrame([
            {
                "end_date": report_period,
                "ann_date": "20260310",
                "f_ann_date": "20260318",
                "report_type": "1",
                "update_flag": "1",
                "total_revenue": 1000,
                "oper_cost": 100,
                "operate_profit": 600,
                "n_income_attr_p": 500,
                "basic_eps": 5,
            },
            {
                "end_date": previous_period,
                "ann_date": "20250310",
                "report_type": "1",
                "update_flag": "1",
                "total_revenue": 900,
                "n_income_attr_p": 430,
            },
        ]),
        "balancesheet": pd.DataFrame([
            {
                "end_date": report_period,
                "ann_date": "20260310",
                "report_type": "1",
                "update_flag": "1",
                "total_assets": 1000,
                "total_liab": 600,
                "total_hldr_eqy_inc_min_int": 400,
                "total_hldr_eqy_exc_min_int": 390,
            }
        ]),
        "cashflow": pd.DataFrame([
            {
                "end_date": report_period,
                "ann_date": "20260310",
                "report_type": "1",
                "update_flag": "1",
                "n_cashflow_act": 520,
                "n_cashflow_inv_act": -100,
                "n_cash_flows_fnc_act": -200,
                "free_cashflow": 420,
            }
        ]),
        "dividend": pd.DataFrame([
            {
                "ann_date": _compact(dividend_day - timedelta(days=20)),
                "ex_date": _compact(dividend_day),
                "cash_div": 24.0,
                "div_proc": "implemented",
            }
        ]),
        "stk_holdernumber": pd.DataFrame([
            {"end_date": report_period, "ann_date": "20260320", "holder_num": 1000},
            {"end_date": previous_period, "ann_date": "20250320", "holder_num": 1250},
        ]),
        "forecast": pd.DataFrame([
            {
                "ann_date": "20260301",
                "end_date": report_period,
                "type": "increase",
                "p_change_min": 10,
                "p_change_max": 20,
                "summary": "profit expected to rise",
                "change_reason": "demand growth",
            }
        ]),
    }

    with patch("src.config.get_config", return_value=SimpleNamespace(tushare_token="token")), \
            patch.object(adapter, "_build_client", return_value=MagicMock()), \
            patch.object(adapter, "_fetch_frames", return_value=(frames, [])):
        result = adapter.get_fundamental_bundle("600519")

    assert result["status"] == "partial"
    assert result["valuation"]["pe_ratio"] == 22.0
    assert result["valuation"]["total_mv"] == 1_230_000.0
    assert result["growth"]["revenue_yoy"] == 12.5
    report = result["earnings"]["financial_report"]
    assert report["revenue"] == 1000.0
    assert report["net_profit_parent"] == 500.0
    assert report["operating_cash_flow"] == 520.0
    assert report["balance_equation_gap_pct"] == 0.0
    assert result["earnings"]["dividend"]["ttm_cash_dividend_per_share"] == 24.0
    assert "10.0%~20.0%" in result["earnings"]["forecast_summary"]
    assert result["institution"]["holder_count_change_pct"] == -20.0
    assert "valuation:tushare.daily_basic" in result["source_chain"]
    assert not result["errors"]


@pytest.mark.parametrize(
    ("research_enabled", "expected_income_calls", "income_error"),
    [
        (False, 2, False),
        (True, 1, True),
    ],
)
def test_endpoint_retry_contract_follows_research_flag(
    research_enabled: bool,
    expected_income_calls: int,
    income_error: bool,
) -> None:
    adapter = TushareFundamentalAdapter()
    calls = Counter()
    lock = Lock()

    class Client:
        def query(self, api_name, **_kwargs):
            with lock:
                calls[api_name] += 1
                attempt = calls[api_name]
            if api_name == "income" and attempt == 1:
                raise TimeoutError("queued")
            if api_name == "forecast":
                raise PermissionError("permission denied")
            return pd.DataFrame()

    frames, errors = adapter._fetch_frames(
        Client(),
        "600519.SH",
        research_enabled=research_enabled,
    )

    assert set(frames) == set(adapter._query_specs("600519.SH"))
    assert calls["income"] == expected_income_calls
    assert calls["forecast"] == 1
    assert (
        any(error.startswith("income:TimeoutError:") for error in errors)
        is income_error
    )
    assert any(error.startswith("forecast:PermissionError:") for error in errors)


@pytest.mark.parametrize(
    ("research_enabled", "expected_workers"),
    [(False, 8), (True, 2)],
)
def test_fundamental_concurrency_contract_follows_research_flag(
    research_enabled: bool,
    expected_workers: int,
) -> None:
    adapter = TushareFundamentalAdapter()
    captured_workers: list[int] = []

    class Client:
        def query(self, _api_name, **_kwargs):
            return pd.DataFrame()

    def build_pool(*, max_workers: int, thread_name_prefix: str):
        captured_workers.append(max_workers)
        return RealThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
        )

    with patch(
        "data_provider.tushare_fundamental_adapter.ThreadPoolExecutor",
        side_effect=build_pool,
    ):
        adapter._fetch_frames(
            Client(),
            "600519.SH",
            research_enabled=research_enabled,
        )

    assert captured_workers == [expected_workers]
