# -*- coding: utf-8 -*-
"""Tushare Pro adapter for A-share fundamentals.

The adapter uses the project's lightweight HTTP client so a
``TUSHARE_HTTP_URL`` compatible gateway receives the same request contract as
the daily-price fetcher. Endpoints fail open independently: one unavailable
statement does not discard the remaining fundamental context.
"""

from __future__ import annotations

import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


_QUERY_FIELDS: Dict[str, str] = {
    "daily_basic": (
        "ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,"
        "pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,"
        "free_share,total_mv,circ_mv"
    ),
    "fina_indicator": (
        "ts_code,ann_date,end_date,eps,dt_eps,bps,ocfps,current_ratio,quick_ratio,"
        "assets_turn,netprofit_margin,grossprofit_margin,roe,roe_waa,roe_dt,"
        "debt_to_assets,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,"
        "tr_yoy,or_yoy"
    ),
    "income": (
        "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
        "basic_eps,diluted_eps,total_revenue,revenue,oper_cost,operate_profit,total_profit,"
        "n_income,n_income_attr_p,ebit,ebitda,update_flag"
    ),
    "balancesheet": (
        "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
        "total_share,money_cap,accounts_receiv,inventories,total_assets,total_liab,"
        "total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,update_flag"
    ),
    "cashflow": (
        "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
        "n_cashflow_act,n_cashflow_inv_act,n_cash_flows_fnc_act,free_cashflow,"
        "c_cash_equ_end_period,update_flag"
    ),
    "dividend": (
        "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
        "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,imp_ann_date"
    ),
    "stk_holdernumber": "ts_code,ann_date,end_date,holder_num",
    "forecast": (
        "ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,"
        "net_profit_max,last_parent_net,first_ann_date,summary,change_reason,update_flag"
    ),
}


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    number = _safe_float(value)
    return int(number) if number is not None else None


def _first_number(row: Optional[pd.Series], keys: Iterable[str]) -> Optional[float]:
    if row is None:
        return None
    for key in keys:
        value = _safe_float(row.get(key))
        if value is not None:
            return value
    return None


def _pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0):
        return None
    return round((current - previous) / abs(previous) * 100.0, 4)


def _clean_text(value: Any, limit: int = 240) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())[:limit]


def _compact_date(value: Any) -> str:
    if value is None:
        return ""
    digits = re.sub(r"\D", "", str(value))
    return digits[:8] if len(digits) >= 8 else ""


def _iso_date(value: Any) -> Optional[str]:
    compact = _compact_date(value)
    if len(compact) != 8:
        return None
    return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"


def _normalize_a_share_code(stock_code: str) -> str:
    raw = str(stock_code or "").strip().upper()
    raw = re.sub(r"^(SH|SZ|BJ|SS)[.]?", "", raw)
    raw = re.sub(r"[.](SH|SZ|BJ|SS)$", "", raw)
    digits = re.sub(r"\D", "", raw)
    return digits[-6:] if len(digits) >= 6 else digits


def _is_bse_code(code: str) -> bool:
    return len(code) == 6 and code.startswith(("4", "8", "92"))


def _to_ts_code(stock_code: str) -> str:
    raw = str(stock_code or "").strip().upper()
    code = _normalize_a_share_code(raw)
    if not (len(code) == 6 and code.isdigit()):
        raise ValueError(f"unsupported A-share code: {stock_code!r}")

    if raw.endswith((".SH", ".SS")) or raw.startswith(("SH", "SS")):
        return f"{code}.SH"
    if raw.endswith(".SZ") or raw.startswith("SZ"):
        return f"{code}.SZ"
    if raw.endswith(".BJ") or raw.startswith("BJ") or _is_bse_code(code):
        return f"{code}.BJ"
    if code.startswith(("600", "601", "603", "605", "688", "51", "52", "56", "58")):
        return f"{code}.SH"
    return f"{code}.SZ"


def _latest_row(
    frame: Optional[pd.DataFrame],
    *,
    date_column: str = "end_date",
    target_period: Optional[str] = None,
    prefer_consolidated: bool = False,
) -> Optional[pd.Series]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return None

    work = frame.copy()
    if date_column not in work.columns:
        return None

    work["__period"] = work[date_column].map(_compact_date)
    today_compact = date.today().strftime("%Y%m%d")
    valid = work["__period"].str.len().eq(8) & work["__period"].le(today_compact)
    work = work[valid]
    if target_period:
        matched = work[work["__period"].eq(_compact_date(target_period))]
        if not matched.empty:
            work = matched
    if work.empty:
        return None

    work["__update_priority"] = work.get(
        "update_flag", pd.Series(index=work.index, dtype=object)
    ).astype(str).ne("1").astype(int)
    if prefer_consolidated and "report_type" in work.columns:
        report_order = {"1": 0, "4": 1, "5": 2, "11": 3}
        work["__report_priority"] = (
            work["report_type"].astype(str).map(report_order).fillna(9).astype(int)
        )
    else:
        work["__report_priority"] = 0
    announcement = pd.Series("", index=work.index, dtype=object)
    for key in ("f_ann_date", "ann_date"):
        if key in work.columns:
            candidate = work[key].map(_compact_date)
            announcement = announcement.where(announcement.str.len().ge(8), candidate)
    work["__announcement"] = announcement
    work = work.sort_values(
        ["__period", "__report_priority", "__update_priority", "__announcement"],
        ascending=[False, True, True, False],
        kind="stable",
    )
    return work.iloc[0]


def _has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        return any(_has_content(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_has_content(item) for item in value)
    return True


def _format_forecast(row: Optional[pd.Series]) -> Tuple[Dict[str, Any], str]:
    if row is None:
        return {}, ""
    payload = {
        "report_date": _iso_date(row.get("end_date")),
        "announcement_date": _iso_date(row.get("ann_date")),
        "type": _clean_text(row.get("type"), 40) or None,
        "profit_change_min_pct": _safe_float(row.get("p_change_min")),
        "profit_change_max_pct": _safe_float(row.get("p_change_max")),
        "net_profit_min": _safe_float(row.get("net_profit_min")),
        "net_profit_max": _safe_float(row.get("net_profit_max")),
        "summary": _clean_text(row.get("summary")),
        "change_reason": _clean_text(row.get("change_reason")),
    }
    payload = {key: value for key, value in payload.items() if value not in (None, "")}
    parts = [str(payload.get("type") or "").strip(), str(payload.get("summary") or "").strip()]
    low = payload.get("profit_change_min_pct")
    high = payload.get("profit_change_max_pct")
    if low is not None or high is not None:
        parts.append(
            f"归母净利润预计变动 {low if low is not None else '?'}%~"
            f"{high if high is not None else '?'}%"
        )
    reason = str(payload.get("change_reason") or "").strip()
    if reason:
        parts.append(f"原因：{reason}")
    return payload, "；".join(part for part in parts if part)[:500]


def _build_dividend_payload(frame: Optional[pd.DataFrame]) -> Dict[str, Any]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return {}

    today = date.today()
    cutoff = today - timedelta(days=365)
    events: List[Dict[str, Any]] = []
    seen = set()
    for _, row in frame.iterrows():
        cash = _safe_float(row.get("cash_div"))
        if cash is None or cash <= 0:
            continue
        ex_date = _iso_date(row.get("ex_date"))
        record_date = _iso_date(row.get("record_date"))
        pay_date = _iso_date(row.get("pay_date"))
        event_date = ex_date or record_date or pay_date
        if not event_date:
            continue
        try:
            event_day = date.fromisoformat(event_date)
        except ValueError:
            continue
        if event_day > today:
            continue
        key = (event_date, round(cash, 8))
        if key in seen:
            continue
        seen.add(key)
        events.append({
            "event_date": event_date,
            "ex_dividend_date": ex_date,
            "record_date": record_date,
            "pay_date": pay_date,
            "announcement_date": _iso_date(row.get("ann_date") or row.get("imp_ann_date")),
            "cash_dividend_per_share": round(cash, 8),
            "is_pre_tax": True,
            "process": _clean_text(row.get("div_proc"), 40) or None,
        })

    if not events:
        return {}
    events.sort(key=lambda item: item["event_date"], reverse=True)
    ttm_events = [
        item for item in events
        if cutoff <= date.fromisoformat(item["event_date"]) <= today
    ]
    return {
        "events": events[:5],
        "ttm_event_count": len(ttm_events),
        "ttm_cash_dividend_per_share": (
            round(sum(float(item["cash_dividend_per_share"]) for item in ttm_events), 8)
            if ttm_events else None
        ),
        "coverage": "cash_dividend_pre_tax",
        "currency": "CNY",
        "as_of": today.isoformat(),
    }


def _build_holder_payload(frame: Optional[pd.DataFrame]) -> Dict[str, Any]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return {}
    work = frame.copy()
    end_dates = work["end_date"] if "end_date" in work.columns else pd.Series("", index=work.index)
    announcements = work["ann_date"] if "ann_date" in work.columns else pd.Series("", index=work.index)
    work["__period"] = end_dates.map(_compact_date)
    work["__announcement"] = announcements.map(_compact_date)
    work = work[work["__period"].str.len().eq(8)]
    if work.empty:
        return {}
    work = work.sort_values(["__period", "__announcement"], ascending=[False, False], kind="stable")
    work = work.drop_duplicates(subset=["__period"], keep="first")
    rows = list(work.head(2).iterrows())
    latest = rows[0][1]
    latest_count = _safe_int(latest.get("holder_num"))
    previous_count = _safe_int(rows[1][1].get("holder_num")) if len(rows) > 1 else None
    change_pct = None
    if latest_count is not None and previous_count not in (None, 0):
        change_pct = round((latest_count - previous_count) / previous_count * 100.0, 4)
    return {
        "holder_count": latest_count,
        "holder_count_previous": previous_count,
        "holder_count_change_pct": change_pct,
        "holder_count_report_date": _iso_date(latest.get("end_date")),
        "holder_count_announcement_date": _iso_date(latest.get("ann_date")),
    }


class TushareFundamentalAdapter:
    """Fetch normalized A-share fundamentals from Tushare-compatible HTTP APIs."""

    def __init__(self, request_timeout_seconds: float = 9.0) -> None:
        self.request_timeout_seconds = max(1.0, float(request_timeout_seconds))

    def _build_client(self, token: str):
        # Delayed import avoids a cycle: ``tushare_fetcher`` imports ``base``.
        from .tushare_fetcher import _TushareHttpClient, _resolve_tushare_http_url

        api_url = _resolve_tushare_http_url() or "http://api.tushare.pro"
        return _TushareHttpClient(
            token=token,
            timeout=self.request_timeout_seconds,
            api_url=api_url,
        )

    @staticmethod
    def _query_specs(ts_code: str) -> Dict[str, Dict[str, Any]]:
        today = date.today()
        recent_start = (today - timedelta(days=45)).strftime("%Y%m%d")
        statement_start = (today - timedelta(days=365 * 3 + 1)).strftime("%Y%m%d")
        forecast_start = (today - timedelta(days=730)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")
        return {
            "daily_basic": {"ts_code": ts_code, "start_date": recent_start, "end_date": end},
            "fina_indicator": {"ts_code": ts_code, "start_date": statement_start, "end_date": end},
            "income": {"ts_code": ts_code, "start_date": statement_start, "end_date": end},
            "balancesheet": {"ts_code": ts_code, "start_date": statement_start, "end_date": end},
            "cashflow": {"ts_code": ts_code, "start_date": statement_start, "end_date": end},
            "dividend": {"ts_code": ts_code},
            "stk_holdernumber": {"ts_code": ts_code, "start_date": statement_start, "end_date": end},
            "forecast": {"ts_code": ts_code, "start_date": forecast_start, "end_date": end},
        }

    def _fetch_frames(self, client: Any, ts_code: str) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
        specs = self._query_specs(ts_code)
        frames: Dict[str, pd.DataFrame] = {}
        failures: Dict[str, str] = {}

        def fetch(name: str, params: Dict[str, Any]) -> pd.DataFrame:
            return client.query(name, fields=_QUERY_FIELDS[name], **params)

        with ThreadPoolExecutor(
            max_workers=len(specs),
            thread_name_prefix="tushare-fundamental",
        ) as pool:
            futures = {pool.submit(fetch, name, params): name for name, params in specs.items()}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    frame = future.result()
                    frames[name] = frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()
                except Exception as exc:
                    message = _clean_text(exc, 160)
                    failures[name] = f"{name}:{type(exc).__name__}:{message}"
                    frames[name] = pd.DataFrame()

        # Compatible gateways can queue one request from the initial burst.
        # Retry at most two critical tables after that burst has drained.
        critical_order = (
            "income",
            "cashflow",
            "fina_indicator",
            "balancesheet",
            "daily_basic",
        )
        retry_names = [name for name in critical_order if name in failures][:2]
        for name in retry_names:
            try:
                frame = fetch(name, specs[name])
                frames[name] = frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()
                failures.pop(name, None)
            except Exception as exc:
                message = _clean_text(exc, 160)
                failures[name] = f"{name}:{type(exc).__name__}:{message}"
        return frames, [failures[name] for name in specs if name in failures]

    def get_fundamental_bundle(self, stock_code: str) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "status": "not_supported",
            "valuation": {},
            "growth": {},
            "earnings": {},
            "institution": {},
            "source_chain": [],
            "errors": [],
        }

        from src.config import get_config

        token = str(getattr(get_config(), "tushare_token", "") or "").strip()
        if not token:
            return result
        try:
            ts_code = _to_ts_code(stock_code)
            client = self._build_client(token)
            frames, errors = self._fetch_frames(client, ts_code)
        except Exception as exc:
            result["errors"].append(f"init:{type(exc).__name__}:{_clean_text(exc, 160)}")
            return result
        result["errors"].extend(errors)

        daily_row = _latest_row(frames.get("daily_basic"), date_column="trade_date")
        if daily_row is not None:
            total_mv = _safe_float(daily_row.get("total_mv"))
            circ_mv = _safe_float(daily_row.get("circ_mv"))
            result["valuation"] = {
                "as_of": _iso_date(daily_row.get("trade_date")),
                "close": _safe_float(daily_row.get("close")),
                "pe_ratio": _first_number(daily_row, ("pe_ttm", "pe")),
                "pe_ttm": _safe_float(daily_row.get("pe_ttm")),
                "pe_static": _safe_float(daily_row.get("pe")),
                "pb_ratio": _safe_float(daily_row.get("pb")),
                "ps_ttm": _safe_float(daily_row.get("ps_ttm")),
                "dividend_yield_ttm_pct": _safe_float(daily_row.get("dv_ttm")),
                "turnover_rate": _safe_float(daily_row.get("turnover_rate")),
                "volume_ratio": _safe_float(daily_row.get("volume_ratio")),
                # Tushare market-cap fields are in 10k CNY; normalize to CNY.
                "total_mv": total_mv * 10000.0 if total_mv is not None else None,
                "circ_mv": circ_mv * 10000.0 if circ_mv is not None else None,
                "currency": "CNY",
            }
            result["source_chain"].append("valuation:tushare.daily_basic")

        indicator_row = _latest_row(frames.get("fina_indicator"))
        target_period = _compact_date(indicator_row.get("end_date")) if indicator_row is not None else None
        income_row = _latest_row(
            frames.get("income"), target_period=target_period, prefer_consolidated=True
        )
        if not target_period and income_row is not None:
            target_period = _compact_date(income_row.get("end_date"))
        balance_row = _latest_row(
            frames.get("balancesheet"), target_period=target_period, prefer_consolidated=True
        )
        cashflow_row = _latest_row(
            frames.get("cashflow"), target_period=target_period, prefer_consolidated=True
        )

        previous_income_row = None
        if income_row is not None and target_period and len(target_period) == 8:
            previous_period = f"{int(target_period[:4]) - 1:04d}{target_period[4:]}"
            previous_income_row = _latest_row(
                frames.get("income"),
                target_period=previous_period,
                prefer_consolidated=True,
            )
        revenue = _first_number(income_row, ("total_revenue", "revenue"))
        previous_revenue = _first_number(previous_income_row, ("total_revenue", "revenue"))
        net_profit = _first_number(income_row, ("n_income_attr_p", "n_income"))
        previous_net_profit = _first_number(previous_income_row, ("n_income_attr_p", "n_income"))
        operating_cost = _safe_float(income_row.get("oper_cost")) if income_row is not None else None
        derived_gross_margin = None
        if revenue not in (None, 0) and operating_cost is not None:
            derived_gross_margin = round((revenue - operating_cost) / revenue * 100.0, 4)
        derived_net_margin = None
        if revenue not in (None, 0) and net_profit is not None:
            derived_net_margin = round(net_profit / revenue * 100.0, 4)

        growth_payload = {
            "report_date": _iso_date(
                (indicator_row.get("end_date") if indicator_row is not None else None)
                or (income_row.get("end_date") if income_row is not None else None)
            ),
            "announcement_date": _iso_date(
                (indicator_row.get("ann_date") if indicator_row is not None else None)
                or (income_row.get("f_ann_date") if income_row is not None else None)
                or (income_row.get("ann_date") if income_row is not None else None)
            ),
            "revenue_yoy": (
                _first_number(indicator_row, ("or_yoy", "tr_yoy"))
                if indicator_row is not None
                else _pct_change(revenue, previous_revenue)
            ),
            "net_profit_yoy": (
                _first_number(indicator_row, ("netprofit_yoy", "dt_netprofit_yoy"))
                if indicator_row is not None
                else _pct_change(net_profit, previous_net_profit)
            ),
            "operating_profit_yoy": (
                _safe_float(indicator_row.get("op_yoy")) if indicator_row is not None else None
            ),
            "operating_cash_flow_yoy": (
                _safe_float(indicator_row.get("ocf_yoy")) if indicator_row is not None else None
            ),
            "roe": _first_number(indicator_row, ("roe_waa", "roe", "roe_dt")),
            "gross_margin": (
                _safe_float(indicator_row.get("grossprofit_margin"))
                if indicator_row is not None else derived_gross_margin
            ),
            "net_profit_margin": (
                _safe_float(indicator_row.get("netprofit_margin"))
                if indicator_row is not None else derived_net_margin
            ),
            "debt_to_assets": (
                _safe_float(indicator_row.get("debt_to_assets"))
                if indicator_row is not None else None
            ),
            "current_ratio": (
                _safe_float(indicator_row.get("current_ratio"))
                if indicator_row is not None else None
            ),
            "quick_ratio": (
                _safe_float(indicator_row.get("quick_ratio"))
                if indicator_row is not None else None
            ),
            "asset_turnover": (
                _safe_float(indicator_row.get("assets_turn"))
                if indicator_row is not None else None
            ),
            "eps": (
                _safe_float(indicator_row.get("eps"))
                if indicator_row is not None else _first_number(income_row, ("basic_eps", "diluted_eps"))
            ),
            "book_value_per_share": (
                _safe_float(indicator_row.get("bps")) if indicator_row is not None else None
            ),
            "operating_cash_flow_per_share": (
                _safe_float(indicator_row.get("ocfps")) if indicator_row is not None else None
            ),
        }
        growth_payload = {key: value for key, value in growth_payload.items() if value not in (None, "")}
        if _has_content(growth_payload):
            result["growth"] = growth_payload
            result["source_chain"].append(
                "growth:tushare.fina_indicator"
                if indicator_row is not None else "growth:tushare.income_yoy"
            )

        total_equity = _first_number(
            balance_row, ("total_hldr_eqy_inc_min_int", "total_hldr_eqy_exc_min_int")
        )
        parent_equity = (
            _safe_float(balance_row.get("total_hldr_eqy_exc_min_int"))
            if balance_row is not None else None
        )
        financial_report = {
            "report_date": _iso_date(
                (indicator_row.get("end_date") if indicator_row is not None else None)
                or (income_row.get("end_date") if income_row is not None else None)
            ),
            "announcement_date": _iso_date(
                (income_row.get("f_ann_date") if income_row is not None else None)
                or (income_row.get("ann_date") if income_row is not None else None)
                or (indicator_row.get("ann_date") if indicator_row is not None else None)
            ),
            "revenue": revenue,
            "net_profit_parent": net_profit,
            "operating_profit": (
                _safe_float(income_row.get("operate_profit")) if income_row is not None else None
            ),
            "operating_cash_flow": (
                _safe_float(cashflow_row.get("n_cashflow_act")) if cashflow_row is not None else None
            ),
            "investing_cash_flow": (
                _safe_float(cashflow_row.get("n_cashflow_inv_act")) if cashflow_row is not None else None
            ),
            "financing_cash_flow": (
                _safe_float(cashflow_row.get("n_cash_flows_fnc_act")) if cashflow_row is not None else None
            ),
            "free_cash_flow": (
                _safe_float(cashflow_row.get("free_cashflow")) if cashflow_row is not None else None
            ),
            "total_assets": (
                _safe_float(balance_row.get("total_assets")) if balance_row is not None else None
            ),
            "total_liabilities": (
                _safe_float(balance_row.get("total_liab")) if balance_row is not None else None
            ),
            "shareholders_equity": total_equity,
            "shareholders_equity_parent": parent_equity,
            "roe": _first_number(indicator_row, ("roe_waa", "roe", "roe_dt")),
            "gross_margin": (
                _safe_float(indicator_row.get("grossprofit_margin"))
                if indicator_row is not None else None
            ),
            "basic_eps": _first_number(income_row, ("basic_eps", "diluted_eps")),
            "currency": "CNY",
        }
        financial_report = {
            key: value for key, value in financial_report.items() if value not in (None, "")
        }
        total_assets = _safe_float(balance_row.get("total_assets")) if balance_row is not None else None
        total_liabilities = _safe_float(balance_row.get("total_liab")) if balance_row is not None else None
        if total_assets not in (None, 0) and total_liabilities is not None and total_equity is not None:
            identity_gap_pct = round(
                abs(total_assets - total_liabilities - total_equity) / abs(total_assets) * 100.0,
                4,
            )
            financial_report["balance_equation_gap_pct"] = identity_gap_pct
            if identity_gap_pct > 5.0:
                result["errors"].append(
                    f"balancesheet:accounting_identity_gap:{identity_gap_pct}%"
                )
        if _has_content(financial_report):
            result["earnings"]["financial_report"] = financial_report
            result["source_chain"].append(
                "earnings.financial_report:tushare.income+balancesheet+cashflow"
            )

        dividend = _build_dividend_payload(frames.get("dividend"))
        if dividend:
            result["earnings"]["dividend"] = dividend
            result["source_chain"].append("earnings.dividend:tushare.dividend")

        forecast_row = _latest_row(frames.get("forecast"), date_column="ann_date")
        forecast, forecast_summary = _format_forecast(forecast_row)
        if forecast:
            result["earnings"]["forecast"] = forecast
            result["earnings"]["forecast_summary"] = forecast_summary
            result["source_chain"].append("earnings.forecast:tushare.forecast")

        institution = _build_holder_payload(frames.get("stk_holdernumber"))
        if institution:
            result["institution"] = institution
            result["source_chain"].append("institution:tushare.stk_holdernumber")

        has_content = any(
            _has_content(result.get(key))
            for key in ("valuation", "growth", "earnings", "institution")
        )
        result["status"] = "partial" if has_content else "not_supported"
        return result
