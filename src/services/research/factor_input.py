"""Pure Tushare-row projection into the deterministic factor input contract."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
import math
import re
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from .factor_policy_v1 import (
    QUALITY_METRIC_UNIVERSE,
    QUALITY_RULES,
    VALUE_METRIC_UNIVERSE,
    VALUE_RULES,
)
from .profiles import resolve_company_profile
from .schemas import parse_datetime


_A_SHARE_TIMEZONE = ZoneInfo("Asia/Shanghai")
_MARKET_CLOSE = time(15, 0)
_FINANCIAL_DATASETS = (
    "fina_indicator",
    "income",
    "balancesheet",
    "cashflow",
    "bank_indicator",
    "insurance_indicator",
    "securities_indicator",
)


def _stock_digits(value: Any) -> str:
    raw = str(value or "").strip().upper()
    raw = re.sub(r"^(SH|SZ|BJ|SS)[.]?", "", raw)
    raw = re.sub(r"[.](SH|SZ|BJ|SS)$", "", raw)
    digits = re.sub(r"\D", "", raw)
    return digits[-6:] if len(digits) >= 6 else digits


def _cutoff(value: Any) -> tuple[datetime, datetime]:
    if isinstance(value, date) and not isinstance(value, datetime):
        local = datetime.combine(value, time(23, 59, 59), tzinfo=_A_SHARE_TIMEZONE)
        return local.astimezone(timezone.utc), local
    if isinstance(value, str) and re.fullmatch(r"\d{4}-?\d{2}-?\d{2}", value.strip()):
        digits = re.sub(r"\D", "", value)
        local_date = datetime.strptime(digits, "%Y%m%d").date()
        local = datetime.combine(local_date, time(23, 59, 59), tzinfo=_A_SHARE_TIMEZONE)
        return local.astimezone(timezone.utc), local
    utc = parse_datetime(value, field="as_of")
    return utc, utc.astimezone(_A_SHARE_TIMEZONE)


def _date_value(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    if len(digits) < 8:
        return None
    try:
        return datetime.strptime(digits[:8], "%Y%m%d").date()
    except ValueError:
        return None


def _date_text(value: date) -> str:
    return value.isoformat()


def _available_at(value: date) -> str:
    return datetime.combine(value, time.min, tzinfo=_A_SHARE_TIMEZONE).astimezone(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _number(value: Any, *, field: str) -> Optional[float]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        raise ValueError(f"{field} must not be NaN or infinity")
    return result


def _rows(rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]], dataset: str) -> tuple[Mapping[str, Any], ...]:
    raw_rows = rows_by_dataset.get(dataset, ())
    if raw_rows is None:
        return ()
    if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes, bytearray)):
        raise TypeError(f"rows_by_dataset[{dataset!r}] must be a sequence")
    result = []
    for row in raw_rows:
        if not isinstance(row, Mapping):
            raise TypeError(f"rows_by_dataset[{dataset!r}] rows must be mappings")
        result.append(row)
    return tuple(result)


def _matching_rows(rows: Sequence[Mapping[str, Any]], stock_code: str) -> tuple[Mapping[str, Any], ...]:
    result = []
    for row in rows:
        raw_code = row.get("ts_code", row.get("stock_code", row.get("code")))
        if raw_code is None or _stock_digits(raw_code) == stock_code:
            result.append(row)
    return tuple(result)


def _explicit_completed(row: Mapping[str, Any]) -> Optional[bool]:
    for key in ("completed", "is_completed"):
        if key not in row:
            continue
        value = row[key]
        if isinstance(value, bool):
            return value
        text = str(value).strip().casefold()
        if text in {"1", "true", "yes", "completed"}:
            return True
        if text in {"0", "false", "no", "incomplete"}:
            return False
        raise ValueError(f"daily.{key} must be boolean")
    return None


def _is_completed_trade_day(row: Mapping[str, Any], trade_date: date, local_cutoff: datetime) -> bool:
    explicit = _explicit_completed(row)
    if explicit is False:
        return False
    if trade_date > local_cutoff.date():
        return False
    if explicit is True or trade_date < local_cutoff.date():
        return True
    return local_cutoff.timetz().replace(tzinfo=None) >= _MARKET_CLOSE


def _deduplicated_daily(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    stock_code: str,
    local_cutoff: datetime,
) -> list[tuple[date, Mapping[str, Any]]]:
    selected: dict[date, Mapping[str, Any]] = {}
    for row in _matching_rows(_rows(rows_by_dataset, "daily"), stock_code):
        trade_date = _date_value(row.get("trade_date", row.get("date")))
        if trade_date is None or not _is_completed_trade_day(row, trade_date, local_cutoff):
            continue
        if _number(row.get("close"), field="daily.close") is None:
            continue
        selected[trade_date] = row
    return sorted(selected.items(), key=lambda item: item[0])[-300:]


def _adjustment_factors(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    stock_code: str,
    cutoff_date: date,
) -> Mapping[date, float]:
    selected: dict[date, float] = {}
    for row in _matching_rows(_rows(rows_by_dataset, "adj_factor"), stock_code):
        trade_date = _date_value(row.get("trade_date", row.get("date")))
        if trade_date is None or trade_date > cutoff_date:
            continue
        factor = _number(row.get("adj_factor"), field="adj_factor.adj_factor")
        if factor is None:
            continue
        if factor <= 0:
            raise ValueError("adj_factor.adj_factor must be positive")
        selected[trade_date] = factor
    return selected


def _build_bars(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    stock_code: str,
    local_cutoff: datetime,
) -> list[dict[str, Any]]:
    daily = _deduplicated_daily(rows_by_dataset, stock_code=stock_code, local_cutoff=local_cutoff)
    if not daily:
        return []
    exact_factors = _adjustment_factors(
        rows_by_dataset,
        stock_code=stock_code,
        cutoff_date=local_cutoff.date(),
    )
    aligned: list[Optional[float]] = []
    first_trade_date = daily[0][0]
    prior_factor_dates = [trade_date for trade_date in exact_factors if trade_date <= first_trade_date]
    last_factor: Optional[float] = exact_factors[max(prior_factor_dates)] if prior_factor_dates else None
    for trade_date, row in daily:
        inline_factor = _number(row.get("adj_factor"), field="daily.adj_factor")
        factor = exact_factors.get(trade_date, inline_factor)
        if factor is not None:
            if factor <= 0:
                raise ValueError("daily.adj_factor must be positive")
            last_factor = factor
        aligned.append(last_factor)
    first_known = next((factor for factor in aligned if factor is not None), None)
    if first_known is not None:
        aligned = [first_known if factor is None else factor for factor in aligned]
    latest_factor = next((factor for factor in reversed(aligned) if factor is not None), None)

    bars = []
    for (trade_date, row), factor in zip(daily, aligned):
        close = _number(row.get("close"), field="daily.close")
        if close is None or close <= 0:
            continue
        multiplier = factor / latest_factor if factor is not None and latest_factor is not None else 1.0
        bar: dict[str, Any] = {
            "trade_date": _date_text(trade_date),
            "close": close,
            "adj_close": close * multiplier,
            "completed": True,
            "adjustment_status": "available" if factor is not None else "missing",
        }
        for source_key, target_key in (("open", "open"), ("high", "high"), ("low", "low")):
            value = _number(row.get(source_key), field=f"daily.{source_key}")
            if value is not None:
                bar[target_key] = value
                bar[f"adj_{target_key}"] = value * multiplier
        volume = _number(row.get("volume", row.get("vol")), field="daily.volume")
        if volume is not None:
            bar["volume"] = volume
        if factor is not None:
            bar["adj_factor"] = factor
        bars.append(bar)
    return bars[-300:]


def _latest_daily_basic(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    stock_code: str,
    local_cutoff: datetime,
) -> Optional[Mapping[str, Any]]:
    selected: dict[date, Mapping[str, Any]] = {}
    for row in _matching_rows(_rows(rows_by_dataset, "daily_basic"), stock_code):
        trade_date = _date_value(row.get("trade_date"))
        if trade_date is None or not _is_completed_trade_day(row, trade_date, local_cutoff):
            continue
        selected[trade_date] = row
    return selected[max(selected)] if selected else None


def _announcement_date(row: Mapping[str, Any]) -> Optional[date]:
    for key in ("f_ann_date", "ann_date", "imp_ann_date", "available_at", "published_at"):
        value = _date_value(row.get(key))
        if value is not None:
            return value
    return None


def _available_financial_rows(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    dataset: str,
    *,
    stock_code: str,
    cutoff_date: date,
) -> list[Mapping[str, Any]]:
    available = []
    for index, row in enumerate(_matching_rows(_rows(rows_by_dataset, dataset), stock_code)):
        announced = _announcement_date(row)
        period = _date_value(row.get("end_date", row.get("report_date")))
        if announced is None or announced > cutoff_date or (period is not None and period > cutoff_date):
            continue
        available.append((period or date.min, announced, 1 if str(row.get("update_flag")) == "1" else 0, index, row))
    available.sort(key=lambda item: (item[0], item[1], item[2], item[3]), reverse=True)
    return [item[-1] for item in available]


def _company(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    stock_code: str,
    name: Optional[str],
    cutoff_date: date,
) -> dict[str, Any]:
    stock_rows = _matching_rows(_rows(rows_by_dataset, "stock_basic"), stock_code)
    stock = stock_rows[-1] if stock_rows else {}
    result: dict[str, Any] = {}
    resolved_name = str(name or stock.get("name") or stock.get("stock_name") or "").strip()
    if resolved_name:
        result["name"] = resolved_name
    industry = str(stock.get("industry") or "").strip()
    if industry:
        result["industry"] = industry
    comp_type = stock.get("comp_type")
    if comp_type in (None, ""):
        for dataset in ("income", "balancesheet", "cashflow", "fina_indicator"):
            rows = _available_financial_rows(
                rows_by_dataset,
                dataset,
                stock_code=stock_code,
                cutoff_date=cutoff_date,
            )
            if rows and rows[0].get("comp_type") not in (None, ""):
                comp_type = rows[0]["comp_type"]
                break
    if comp_type not in (None, ""):
        result["comp_type"] = comp_type
    return result


def _first_number(sources: Sequence[Mapping[str, Any]], aliases: Sequence[str]) -> Optional[float]:
    for source in sources:
        for alias in aliases:
            if alias in source:
                value = _number(source[alias], field=f"fundamentals.{alias}")
                if value is not None:
                    return value
    return None


def _previous_income(
    income_rows: Sequence[Mapping[str, Any]],
    latest: Optional[Mapping[str, Any]],
) -> Optional[Mapping[str, Any]]:
    if latest is None:
        return None
    latest_period = _date_value(latest.get("end_date"))
    if latest_period is None:
        return income_rows[1] if len(income_rows) > 1 else None
    target = latest_period.replace(year=latest_period.year - 1)
    for row in income_rows[1:]:
        if _date_value(row.get("end_date")) == target:
            return row
    return income_rows[1] if len(income_rows) > 1 else None


def _growth(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / abs(previous) * 100.0


def _fundamentals(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    stock_code: str,
    profile: str,
    local_cutoff: datetime,
) -> dict[str, Mapping[str, Any]]:
    daily_basic = _latest_daily_basic(
        rows_by_dataset,
        stock_code=stock_code,
        local_cutoff=local_cutoff,
    )
    latest_by_dataset: dict[str, Optional[Mapping[str, Any]]] = {}
    rows_by_financial_dataset: dict[str, list[Mapping[str, Any]]] = {}
    for dataset in _FINANCIAL_DATASETS:
        rows = _available_financial_rows(
            rows_by_dataset,
            dataset,
            stock_code=stock_code,
            cutoff_date=local_cutoff.date(),
        )
        rows_by_financial_dataset[dataset] = rows
        latest_by_dataset[dataset] = rows[0] if rows else None
    sources = [source for source in (daily_basic, *latest_by_dataset.values()) if source is not None]

    income = latest_by_dataset["income"]
    previous_income = _previous_income(rows_by_financial_dataset["income"], income)
    derived: dict[str, float] = {}
    revenue = _first_number([income] if income else [], ("total_revenue", "revenue"))
    previous_revenue = _first_number([previous_income] if previous_income else [], ("total_revenue", "revenue"))
    profit = _first_number([income] if income else [], ("n_income_attr_p", "n_income"))
    previous_profit = _first_number([previous_income] if previous_income else [], ("n_income_attr_p", "n_income"))
    cost = _first_number([income] if income else [], ("oper_cost",))
    if revenue not in (None, 0) and cost is not None:
        derived["gross_margin"] = (revenue - cost) / revenue * 100.0
    if revenue not in (None, 0) and profit is not None:
        derived["net_margin"] = profit / revenue * 100.0
    revenue_growth = _growth(revenue, previous_revenue)
    profit_growth = _growth(profit, previous_profit)
    if revenue_growth is not None:
        derived["revenue_growth"] = revenue_growth
    if profit_growth is not None:
        derived["profit_growth"] = profit_growth
    sources.append(derived)

    applicable_rules = {**VALUE_RULES[profile], **QUALITY_RULES[profile]}
    result: dict[str, Mapping[str, Any]] = {}
    for metric in (*VALUE_METRIC_UNIVERSE, *QUALITY_METRIC_UNIVERSE):
        if metric in result:
            continue
        rule = applicable_rules.get(metric)
        if rule is None:
            result[metric] = {"status": "not_applicable", "value": None}
            continue
        aliases = (metric, *rule.aliases)
        value = _first_number(sources, aliases)
        result[metric] = {
            "status": "available" if value is not None else "missing",
            "value": value,
        }
    return result


def _catalyst_rows(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    aliases: Sequence[str],
    *,
    stock_code: str,
) -> tuple[bool, tuple[Mapping[str, Any], ...]]:
    present = any(alias in rows_by_dataset for alias in aliases)
    rows = []
    for alias in aliases:
        rows.extend(_matching_rows(_rows(rows_by_dataset, alias), stock_code))
    return present, tuple(rows)


def _visible_event_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    cutoff_date: date,
) -> tuple[list[tuple[date, Mapping[str, Any]]], bool]:
    visible = []
    future = False
    for row in rows:
        announced = _announcement_date(row)
        if announced is None:
            continue
        if announced > cutoff_date:
            future = True
            continue
        visible.append((announced, row))
    visible.sort(key=lambda item: item[0], reverse=True)
    return visible, future


def _source_payload(*, present: bool, original_count: int, items: list[dict[str, Any]], future: bool) -> dict[str, Any]:
    if items:
        return {"status": "available", "items": items}
    if not present:
        return {"status": "missing", "items": []}
    if original_count == 0:
        return {"status": "empty", "items": []}
    return {"status": "partial" if future else "partial", "items": []}


def _catalysts(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    stock_code: str,
    cutoff_date: date,
    daily_basic: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    forecast_present, forecast_rows = _catalyst_rows(rows_by_dataset, ("forecast",), stock_code=stock_code)
    visible_forecast, future_forecast = _visible_event_rows(forecast_rows, cutoff_date=cutoff_date)
    forecast_items = []
    for announced, row in visible_forecast:
        low = _number(row.get("p_change_min"), field="forecast.p_change_min")
        high = _number(row.get("p_change_max"), field="forecast.p_change_max")
        values = [value for value in (low, high) if value is not None]
        item: dict[str, Any] = {
            "available_at": _available_at(announced),
            "type": str(row.get("type") or "").strip(),
        }
        if values:
            item["change_pct"] = sum(values) / len(values)
        forecast_items.append(item)

    dividend_present, dividend_rows = _catalyst_rows(rows_by_dataset, ("dividend",), stock_code=stock_code)
    visible_dividend, future_dividend = _visible_event_rows(dividend_rows, cutoff_date=cutoff_date)
    dividend_yield = _first_number([daily_basic] if daily_basic else [], ("dv_ttm", "dividend_yield"))
    dividend_items = []
    for announced, row in visible_dividend:
        item = {"available_at": _available_at(announced)}
        if dividend_yield is not None:
            item["dividend_yield"] = dividend_yield
        cash = _number(row.get("cash_div_tax", row.get("cash_div")), field="dividend.cash_div")
        if cash is not None:
            item["cash_dividend"] = cash
        dividend_items.append(item)

    holder_present, holder_rows = _catalyst_rows(
        rows_by_dataset,
        ("holder", "stk_holdernumber", "stk_holdertrade"),
        stock_code=stock_code,
    )
    visible_holder, future_holder = _visible_event_rows(holder_rows, cutoff_date=cutoff_date)
    holder_items = []
    direct_item = None
    for announced, row in visible_holder:
        direct = _first_number([row], ("change_pct", "holder_change_pct", "holding_change_pct"))
        if direct is not None:
            direct_item = {"available_at": _available_at(announced), "change_pct": direct}
            break
    if direct_item is not None:
        holder_items.append(direct_item)
    else:
        counts_by_period: dict[date, tuple[date, float]] = {}
        for announced, row in visible_holder:
            count = _number(row.get("holder_num"), field="holder.holder_num")
            period = _date_value(row.get("end_date"))
            if count is not None and period is not None:
                current = counts_by_period.get(period)
                if current is None or announced > current[0]:
                    counts_by_period[period] = (announced, count)
        counts = [(period, announced, count) for period, (announced, count) in counts_by_period.items()]
        counts.sort(reverse=True)
        if len(counts) >= 2 and counts[1][2] != 0:
            # Fewer holders means greater concentration and therefore a
            # positive holder catalyst.
            concentration_change = (counts[1][2] - counts[0][2]) / abs(counts[1][2]) * 100.0
            holder_items.append(
                {"available_at": _available_at(counts[0][1]), "change_pct": concentration_change}
            )

    event_present, event_rows = _catalyst_rows(rows_by_dataset, ("event", "events"), stock_code=stock_code)
    visible_events, future_events = _visible_event_rows(event_rows, cutoff_date=cutoff_date)
    event_items = []
    for announced, row in visible_events:
        item = {"available_at": _available_at(announced)}
        for key in ("impact_score", "event_impact", "sentiment_score", "sentiment", "direction"):
            if row.get(key) is not None:
                item[key] = row[key]
        event_items.append(item)

    return {
        "forecast": _source_payload(
            present=forecast_present,
            original_count=len(forecast_rows),
            items=forecast_items,
            future=future_forecast,
        ),
        "dividend": _source_payload(
            present=dividend_present,
            original_count=len(dividend_rows),
            items=dividend_items,
            future=future_dividend,
        ),
        "holder": _source_payload(
            present=holder_present,
            original_count=len(holder_rows),
            items=holder_items,
            future=future_holder,
        ),
        "event": _source_payload(
            present=event_present,
            original_count=len(event_rows),
            items=event_items,
            future=future_events,
        ),
    }


def _latest_dated_row(
    rows: Sequence[Mapping[str, Any]],
    *,
    cutoff_date: date,
) -> Optional[Mapping[str, Any]]:
    dated = []
    for index, row in enumerate(rows):
        row_date = _date_value(row.get("trade_date", row.get("date")))
        if row_date is not None and row_date <= cutoff_date:
            dated.append((row_date, index, row))
    return max(dated, default=(None, None, None))[-1]


def _market_state(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    stock_code: str,
    company: Mapping[str, Any],
    bars: Sequence[Mapping[str, Any]],
    cutoff_date: date,
    scenario: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    state = dict(scenario or {})
    raw_flags = state.get("risk_flags", ())
    if raw_flags is None:
        raw_flags = ()
    if not isinstance(raw_flags, Sequence) or isinstance(raw_flags, (str, bytes, bytearray)):
        raise TypeError("scenario.risk_flags must be a sequence")
    flags = {str(item).strip().casefold() for item in raw_flags}
    name = str(company.get("name") or "").strip().upper()
    is_st = name.startswith(("ST", "*ST", "SST", "S*ST")) if name else None
    if state.get("is_st") is not None:
        is_st = bool(state["is_st"])
    if is_st:
        flags.add("st")
    if is_st is not None:
        state["is_st"] = is_st

    suspend_rows = _matching_rows(_rows(rows_by_dataset, "suspend_d"), stock_code)
    latest_suspend = _latest_dated_row(suspend_rows, cutoff_date=cutoff_date)
    suspended = False
    if latest_suspend is not None:
        suspend_type = str(latest_suspend.get("suspend_type") or latest_suspend.get("status") or "S").strip().upper()
        suspended = suspend_type not in {"R", "RESUME", "复牌"}
    if "trading_status" not in state:
        state["trading_status"] = "suspended" if suspended else "normal"
    if str(state.get("trading_status")).casefold() in {"suspended", "halted", "停牌"}:
        flags.add("suspended")

    limit_state = None
    price_limit_state = "none"
    latest_bar = bars[-1] if bars else None
    limit_rows = _matching_rows(_rows(rows_by_dataset, "stk_limit"), stock_code)
    latest_limit = _latest_dated_row(limit_rows, cutoff_date=cutoff_date)
    if latest_bar is not None and latest_limit is not None:
        bar_date = _date_value(latest_bar.get("trade_date"))
        limit_date = _date_value(latest_limit.get("trade_date"))
        if bar_date == limit_date:
            close = _number(latest_bar.get("close"), field="market.close")
            high = _number(latest_bar.get("high"), field="market.high")
            low = _number(latest_bar.get("low"), field="market.low")
            up = _number(latest_limit.get("up_limit"), field="stk_limit.up_limit")
            down = _number(latest_limit.get("down_limit"), field="stk_limit.down_limit")
            one_price = close is not None and high is not None and low is not None and math.isclose(high, low)
            if up is not None and math.isclose(close, up, rel_tol=1e-6, abs_tol=1e-6):
                price_limit_state = "limit_up"
                flags.add("limit_up")
                if one_price:
                    limit_state = "one_price_limit_up"
            elif down is not None and math.isclose(close, down, rel_tol=1e-6, abs_tol=1e-6):
                price_limit_state = "limit_down"
                flags.add("limit_down")
                if one_price:
                    limit_state = "one_price_limit_down"
    if "limit_state" not in state:
        state["limit_state"] = limit_state or "none"
    if "price_limit_state" not in state:
        if state["limit_state"] == "one_price_limit_up":
            state["price_limit_state"] = "limit_up"
        elif state["limit_state"] == "one_price_limit_down":
            state["price_limit_state"] = "limit_down"
        else:
            state["price_limit_state"] = price_limit_state
    if state["trading_status"] == "suspended":
        state.setdefault("buy_executable", False)
        state.setdefault("sell_executable", False)
    elif state["limit_state"] == "one_price_limit_up":
        state.setdefault("buy_executable", False)
        state.setdefault("sell_executable", True)
        flags.add("limit_up")
    elif state["limit_state"] == "one_price_limit_down":
        state.setdefault("buy_executable", True)
        state.setdefault("sell_executable", False)
        flags.add("limit_down")
    else:
        state.setdefault("buy_executable", True)
        state.setdefault("sell_executable", True)
    state["risk_flags"] = sorted(flags)
    return state


def build_factor_input(
    rows_by_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    stock_code: str,
    as_of: Any,
    market: Optional[str] = None,
    name: Optional[str] = None,
    scenario: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Build the pure Mapping consumed by ``evaluate_research_factors``."""

    if not isinstance(rows_by_dataset, Mapping):
        raise TypeError("rows_by_dataset must be a mapping")
    if scenario is not None and not isinstance(scenario, Mapping):
        raise TypeError("scenario must be a mapping")
    normalized_code = _stock_digits(stock_code)
    if len(normalized_code) != 6 or not normalized_code.isdigit():
        raise ValueError(f"unsupported A-share stock code: {stock_code!r}")
    utc_cutoff, local_cutoff = _cutoff(as_of)
    company = _company(
        rows_by_dataset,
        stock_code=normalized_code,
        name=name,
        cutoff_date=local_cutoff.date(),
    )
    profile = resolve_company_profile(company).profile
    bars = _build_bars(
        rows_by_dataset,
        stock_code=normalized_code,
        local_cutoff=local_cutoff,
    )
    daily_basic = _latest_daily_basic(
        rows_by_dataset,
        stock_code=normalized_code,
        local_cutoff=local_cutoff,
    )
    fundamentals = _fundamentals(
        rows_by_dataset,
        stock_code=normalized_code,
        profile=profile,
        local_cutoff=local_cutoff,
    )
    catalysts = _catalysts(
        rows_by_dataset,
        stock_code=normalized_code,
        cutoff_date=local_cutoff.date(),
        daily_basic=daily_basic,
    )
    market_state = _market_state(
        rows_by_dataset,
        stock_code=normalized_code,
        company=company,
        bars=bars,
        cutoff_date=local_cutoff.date(),
        scenario=scenario,
    )
    return {
        "stock_code": normalized_code,
        "market": str(market or "A").strip() or "A",
        "as_of": utc_cutoff.isoformat().replace("+00:00", "Z"),
        "company": company,
        "fundamentals": fundamentals,
        "bars": bars,
        "catalysts": catalysts,
        "market_state": market_state,
    }


__all__ = ["build_factor_input"]
