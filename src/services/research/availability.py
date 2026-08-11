"""Point-in-time availability rules for Tushare research datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import math
from typing import Any, Mapping, Optional, Sequence

import pandas as pd

from .datasets import DatasetStatus


CHINA_TIMEZONE = timezone(timedelta(hours=8))
MARKET_CLOSE_TIME = time(15, 0)
CONSERVATIVE_DATE_AVAILABLE_TIME = time(23, 59, 59, 999999)


@dataclass(frozen=True)
class DatasetDefinition:
    name: str
    availability_fields: tuple[str, ...]
    data_as_of_fields: tuple[str, ...]
    schema_version: str
    history_days: Optional[int] = None
    accepts_date_range: bool = True
    financial: bool = False
    market_daily: bool = False
    incremental_by_trade_date: bool = False
    current_state: bool = False
    stale_after_days: Optional[int] = None
    provider_api_name: Optional[str] = None
    required_fields: tuple[str, ...] = ()
    requires_query_plan: bool = False


@dataclass(frozen=True)
class AvailabilityAssessment:
    rows: tuple[Mapping[str, Any], ...]
    status: str
    available_at: datetime
    data_as_of: datetime
    trade_date: Optional[date]
    report_date: Optional[date]
    announcement_date: Optional[date]
    dropped_future_rows: int = 0
    dropped_unknown_availability_rows: int = 0
    invalid_value_rows: int = 0


def _definition(
    name: str,
    availability_fields: Sequence[str],
    data_as_of_fields: Sequence[str],
    **kwargs: Any,
) -> DatasetDefinition:
    return DatasetDefinition(
        name=name,
        availability_fields=tuple(availability_fields),
        data_as_of_fields=tuple(data_as_of_fields),
        schema_version=f"tushare-{name}-v1",
        **kwargs,
    )


DATASET_DEFINITIONS: Mapping[str, DatasetDefinition] = {
    "stock_basic": _definition(
        "stock_basic",
        (),
        (),
        accepts_date_range=False,
        current_state=True,
    ),
    "daily": _definition(
        "daily",
        ("trade_date",),
        ("trade_date",),
        # 450 calendar days safely covers at least 300 A-share sessions after
        # weekends and common holiday closures. Factor composition tails 300.
        history_days=450,
        market_daily=True,
        stale_after_days=10,
    ),
    "adj_factor": _definition(
        "adj_factor",
        ("trade_date",),
        ("trade_date",),
        history_days=450,
        market_daily=True,
        stale_after_days=10,
    ),
    "daily_basic": _definition(
        "daily_basic",
        ("trade_date",),
        ("trade_date",),
        history_days=300,
        market_daily=True,
        stale_after_days=10,
    ),
    "fina_indicator": _definition(
        "fina_indicator",
        ("f_ann_date", "ann_date"),
        ("end_date", "report_date"),
        history_days=365 * 5,
        financial=True,
    ),
    "income": _definition(
        "income",
        ("f_ann_date", "ann_date"),
        ("end_date", "report_date"),
        history_days=365 * 5,
        financial=True,
    ),
    "balancesheet": _definition(
        "balancesheet",
        ("f_ann_date", "ann_date"),
        ("end_date", "report_date"),
        history_days=365 * 5,
        financial=True,
    ),
    "cashflow": _definition(
        "cashflow",
        ("f_ann_date", "ann_date"),
        ("end_date", "report_date"),
        history_days=365 * 5,
        financial=True,
    ),
    "dividend": _definition(
        "dividend",
        ("ann_date",),
        ("end_date", "record_date", "ex_date"),
        accepts_date_range=False,
    ),
    "stk_holdernumber": _definition(
        "stk_holdernumber",
        ("ann_date",),
        ("end_date", "ann_date"),
        history_days=365 * 5,
    ),
    "forecast": _definition(
        "forecast",
        ("ann_date",),
        ("end_date", "ann_date"),
        history_days=365 * 5,
    ),
    "cyq_perf": _definition(
        "cyq_perf",
        ("trade_date",),
        ("trade_date",),
        history_days=300,
        market_daily=True,
        incremental_by_trade_date=True,
        stale_after_days=10,
    ),
    "cyq_chips": _definition(
        "cyq_chips",
        ("trade_date",),
        ("trade_date",),
        history_days=300,
        market_daily=True,
        incremental_by_trade_date=True,
        stale_after_days=10,
    ),
    "stk_limit": _definition(
        "stk_limit",
        ("trade_date",),
        ("trade_date",),
        history_days=300,
        market_daily=True,
        stale_after_days=10,
    ),
    "suspend_d": _definition(
        "suspend_d",
        ("trade_date",),
        ("trade_date",),
        history_days=300,
        market_daily=True,
        stale_after_days=10,
    ),
    # Decision Outcome v2 datasets are deliberately not part of the default
    # research bundle.  Their exact requests depend on T+1/horizon dates and,
    # for SW daily bars, a separately frozen point-in-time membership.
    "csi300_index_daily": _definition(
        "csi300_index_daily",
        ("trade_date",),
        ("trade_date",),
        market_daily=True,
        stale_after_days=10,
        provider_api_name="index_daily",
        required_fields=(
            "ts_code",
            "trade_date",
            "close",
            "open",
            "high",
            "low",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
        ),
        requires_query_plan=True,
    ),
    "sw1_index_classify": _definition(
        "sw1_index_classify",
        (),
        (),
        accepts_date_range=False,
        current_state=True,
        provider_api_name="index_classify",
        required_fields=(
            "index_code",
            "industry_name",
            "parent_code",
            "level",
            "industry_code",
            "is_pub",
            "src",
        ),
        requires_query_plan=True,
    ),
    "sw1_index_member_all": _definition(
        "sw1_index_member_all",
        (),
        (),
        accepts_date_range=False,
        current_state=True,
        provider_api_name="index_member_all",
        required_fields=(
            "l1_code",
            "l1_name",
            "l2_code",
            "l2_name",
            "l3_code",
            "l3_name",
            "ts_code",
            "name",
            "in_date",
            "out_date",
            "is_new",
        ),
        requires_query_plan=True,
    ),
    "sw1_index_daily": _definition(
        "sw1_index_daily",
        ("trade_date",),
        ("trade_date",),
        market_daily=True,
        stale_after_days=10,
        provider_api_name="sw_daily",
        required_fields=(
            "ts_code",
            "trade_date",
            "name",
            "open",
            "low",
            "high",
            "close",
            "change",
            "pct_change",
            "vol",
            "amount",
            "pe",
            "pb",
            "float_mv",
            "total_mv",
        ),
        requires_query_plan=True,
    ),
}
DEFAULT_RESEARCH_DATASETS = (
    "stock_basic",
    "daily",
    "adj_factor",
    "daily_basic",
    "fina_indicator",
    "income",
    "balancesheet",
    "cashflow",
    "dividend",
    "stk_holdernumber",
    "forecast",
    "cyq_perf",
    "cyq_chips",
    "stk_limit",
    "suspend_d",
)


def normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("as_of must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def as_of_trade_date(value: datetime) -> date:
    return normalize_as_of(value).astimezone(CHINA_TIMEZONE).date()


def parse_tushare_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    text_value = str(value).strip()
    if not text_value:
        return None
    if text_value.endswith(".0") and text_value[:-2].isdigit():
        text_value = text_value[:-2]
    for date_format in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text_value, date_format).date()
        except ValueError:
            continue
    return None


def format_tushare_date(value: date) -> str:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError("value must be a date")
    return value.strftime("%Y%m%d")


def to_tushare_ts_code(stock_code: str) -> str:
    raw = str(stock_code or "").strip().upper()
    if raw.endswith(".SS"):
        raw = f"{raw[:-3]}.SH"
    if raw.endswith((".SH", ".SZ", ".BJ")):
        code, suffix = raw.rsplit(".", 1)
        if code.isdigit() and len(code) == 6:
            return f"{code}.{suffix}"
        raise ValueError(f"unsupported A-share stock code: {stock_code!r}")
    for prefix, suffix in (("SH.", "SH"), ("SZ.", "SZ"), ("BJ.", "BJ")):
        if raw.startswith(prefix):
            raw = f"{raw[len(prefix):]}.{suffix}"
            return to_tushare_ts_code(raw)
    for prefix, suffix in (("SH", "SH"), ("SS", "SH"), ("SZ", "SZ"), ("BJ", "BJ")):
        if raw.startswith(prefix) and raw[len(prefix):].isdigit():
            return to_tushare_ts_code(f"{raw[len(prefix):]}.{suffix}")
    if not raw.isdigit() or len(raw) != 6:
        raise ValueError(f"unsupported A-share stock code: {stock_code!r}")
    if raw.startswith(("4", "8", "92")):
        suffix = "BJ"
    elif raw.startswith(("5", "6", "9")):
        suffix = "SH"
    else:
        suffix = "SZ"
    return f"{raw}.{suffix}"


def _normalize_scalar(value: Any) -> tuple[Any, bool]:
    if value is None:
        return None, False
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None, False
        return value.isoformat(), False
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            value = value.astimezone(timezone.utc)
        return value.isoformat(), False
    if isinstance(value, date):
        return value.isoformat(), False
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except (TypeError, ValueError):
            return None, True
    if isinstance(value, float):
        if math.isnan(value):
            return None, False
        if not math.isfinite(value):
            return None, True
        return value, False
    if isinstance(value, (str, bool, int)):
        return value, False
    try:
        if bool(pd.isna(value)):
            return None, False
    except (TypeError, ValueError):
        pass
    return None, True


def normalize_frame_rows(frame: pd.DataFrame) -> tuple[list[dict[str, Any]], int]:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("Tushare provider result must be a pandas DataFrame")
    columns = [str(column) for column in frame.columns]
    if len(columns) != len(set(columns)):
        raise ValueError("Tushare provider result contains duplicate columns")
    rows: list[dict[str, Any]] = []
    invalid_rows = 0
    for values in frame.itertuples(index=False, name=None):
        row: dict[str, Any] = {}
        row_invalid = False
        for column, value in zip(columns, values):
            normalized, invalid = _normalize_scalar(value)
            row[column] = normalized
            row_invalid = row_invalid or invalid
        invalid_rows += int(row_invalid)
        rows.append(row)
    return rows, invalid_rows


def _missing_date_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _preferred_date(
    row: Mapping[str, Any],
    fields: Sequence[str],
) -> tuple[Optional[date], bool]:
    for field in fields:
        if field not in row:
            continue
        raw_value = row.get(field)
        if _missing_date_value(raw_value):
            continue
        parsed = parse_tushare_date(raw_value)
        if parsed is not None:
            return parsed, False
        # A populated higher-priority field that cannot be parsed is not a
        # license to fall back to a lower-priority date and weaken the cutoff.
        return None, True
    return None, False


def _available_datetime(value: date, definition: DatasetDefinition) -> datetime:
    available_time = (
        MARKET_CLOSE_TIME if definition.market_daily else CONSERVATIVE_DATE_AVAILABLE_TIME
    )
    return datetime.combine(value, available_time, CHINA_TIMEZONE).astimezone(timezone.utc)


def _data_datetime(value: date, definition: DatasetDefinition) -> datetime:
    data_time = MARKET_CLOSE_TIME if definition.market_daily else CONSERVATIVE_DATE_AVAILABLE_TIME
    return datetime.combine(value, data_time, CHINA_TIMEZONE).astimezone(timezone.utc)


def assess_dataset_rows(
    definition: DatasetDefinition,
    rows: Sequence[Mapping[str, Any]],
    *,
    as_of: datetime,
    invalid_value_rows: int = 0,
    observed_at: Optional[datetime] = None,
) -> AvailabilityAssessment:
    boundary = normalize_as_of(as_of)
    if definition.current_state:
        if observed_at is None:
            raise ValueError(
                f"current-state dataset {definition.name!r} requires observed_at"
            )
        observation = normalize_as_of(observed_at)
        if observation > boundary:
            return AvailabilityAssessment(
                rows=(),
                status=DatasetStatus.EMPTY.value,
                available_at=boundary,
                data_as_of=boundary,
                trade_date=None,
                report_date=None,
                announcement_date=None,
                dropped_future_rows=len(rows),
                invalid_value_rows=int(invalid_value_rows),
            )
        normalized_rows = tuple(
            dict(row)
            for row in sorted(
                rows,
                key=lambda item: tuple(
                    str(item.get(key) or "") for key in sorted(item)
                ),
            )
        )
        status = (
            DatasetStatus.PARTIAL.value
            if invalid_value_rows
            else DatasetStatus.AVAILABLE.value
            if normalized_rows
            else DatasetStatus.EMPTY.value
        )
        return AvailabilityAssessment(
            rows=normalized_rows,
            status=status,
            available_at=observation,
            data_as_of=observation,
            trade_date=None,
            report_date=None,
            announcement_date=None,
            invalid_value_rows=int(invalid_value_rows),
        )
    included: list[tuple[Mapping[str, Any], date, date, datetime]] = []
    future_rows = 0
    unknown_rows = 0
    invalid_rows = int(invalid_value_rows)
    for row in rows:
        availability_date, _availability_invalid = _preferred_date(
            row, definition.availability_fields
        )
        if availability_date is None:
            unknown_rows += 1
            continue
        available_at = _available_datetime(availability_date, definition)
        if available_at > boundary:
            future_rows += 1
            continue
        data_date, data_date_invalid = _preferred_date(
            row, definition.data_as_of_fields
        )
        if data_date_invalid:
            invalid_rows += 1
        data_date = data_date or availability_date
        included.append((dict(row), availability_date, data_date, available_at))

    included.sort(key=lambda item: tuple(str(item[0].get(key) or "") for key in sorted(item[0])))
    normalized_rows = tuple(item[0] for item in included)
    if unknown_rows or invalid_rows:
        status = DatasetStatus.PARTIAL.value
    elif not normalized_rows:
        status = DatasetStatus.EMPTY.value
    else:
        status = DatasetStatus.AVAILABLE.value

    latest_availability = max((item[3] for item in included), default=boundary)
    latest_data_date = max((item[2] for item in included), default=as_of_trade_date(boundary))
    if (
        status == DatasetStatus.AVAILABLE.value
        and definition.stale_after_days is not None
        and (as_of_trade_date(boundary) - latest_data_date).days
        > definition.stale_after_days
    ):
        status = DatasetStatus.STALE.value

    trade_date = (
        max((item[2] for item in included), default=None)
        if definition.market_daily
        else None
    )
    report_date = (
        max((item[2] for item in included), default=None)
        if definition.financial
        else None
    )
    announcement_date = (
        max((item[1] for item in included), default=None)
        if definition.financial or "ann_date" in definition.availability_fields
        else None
    )
    return AvailabilityAssessment(
        rows=normalized_rows,
        status=status,
        available_at=latest_availability,
        data_as_of=(
            min(_data_datetime(latest_data_date, definition), boundary)
            if included
            else boundary
        ),
        trade_date=trade_date,
        report_date=report_date,
        announcement_date=announcement_date,
        dropped_future_rows=future_rows,
        dropped_unknown_availability_rows=unknown_rows,
        invalid_value_rows=invalid_rows,
    )


__all__ = [
    "AvailabilityAssessment",
    "CHINA_TIMEZONE",
    "DATASET_DEFINITIONS",
    "DEFAULT_RESEARCH_DATASETS",
    "DatasetDefinition",
    "as_of_trade_date",
    "assess_dataset_rows",
    "format_tushare_date",
    "normalize_as_of",
    "normalize_frame_rows",
    "parse_tushare_date",
    "to_tushare_ts_code",
]
