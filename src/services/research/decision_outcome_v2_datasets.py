"""Strict Tushare query and point-in-time schemas for Decision Outcome v2.

The module deliberately stops at immutable Dataset inputs.  It neither calls a
provider nor reads storage, so a durable worker can execute the returned query
plans and persist the verified rows through the existing research collector.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence

import pandas as pd

from .availability import (
    CHINA_TIMEZONE,
    DATASET_DEFINITIONS,
    parse_tushare_date,
    to_tushare_ts_code,
)


CSI300_INDEX_CODE = "000300.SH"
CSI300_INDEX_NAME = "CSI 300"
SW1_CLASSIFICATION_LEVEL = "L1"
SW1_CLASSIFICATION_SOURCE = "SW2021"

CSI300_INDEX_DAILY_DATASET = "csi300_index_daily"
SW1_INDEX_CLASSIFY_DATASET = "sw1_index_classify"
SW1_INDEX_MEMBER_ALL_DATASET = "sw1_index_member_all"
SW1_INDEX_DAILY_DATASET = "sw1_index_daily"

DECISION_OUTCOME_V2_BENCHMARK_DATASETS = (
    CSI300_INDEX_DAILY_DATASET,
    SW1_INDEX_CLASSIFY_DATASET,
    SW1_INDEX_MEMBER_ALL_DATASET,
    SW1_INDEX_DAILY_DATASET,
)

INDEX_DAILY_FIELDS = DATASET_DEFINITIONS[
    CSI300_INDEX_DAILY_DATASET
].required_fields
SW1_INDEX_CLASSIFY_FIELDS = DATASET_DEFINITIONS[
    SW1_INDEX_CLASSIFY_DATASET
].required_fields
SW1_INDEX_MEMBER_ALL_FIELDS = DATASET_DEFINITIONS[
    SW1_INDEX_MEMBER_ALL_DATASET
].required_fields
SW1_INDEX_DAILY_FIELDS = DATASET_DEFINITIONS[
    SW1_INDEX_DAILY_DATASET
].required_fields

DECISION_OUTCOME_V2_REQUIRED_FIELDS: Mapping[str, tuple[str, ...]] = (
    MappingProxyType(
        {
            CSI300_INDEX_DAILY_DATASET: INDEX_DAILY_FIELDS,
            SW1_INDEX_CLASSIFY_DATASET: SW1_INDEX_CLASSIFY_FIELDS,
            SW1_INDEX_MEMBER_ALL_DATASET: SW1_INDEX_MEMBER_ALL_FIELDS,
            SW1_INDEX_DAILY_DATASET: SW1_INDEX_DAILY_FIELDS,
        }
    )
)

_API_BY_DATASET = MappingProxyType(
    {
        CSI300_INDEX_DAILY_DATASET: "index_daily",
        SW1_INDEX_CLASSIFY_DATASET: "index_classify",
        SW1_INDEX_MEMBER_ALL_DATASET: "index_member_all",
        SW1_INDEX_DAILY_DATASET: "sw_daily",
    }
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SW_INDEX_CODE_RE = re.compile(r"^\d{6}\.SI$")


@dataclass(frozen=True)
class TushareDatasetRequestV2:
    """One exact physical Tushare request inside an immutable query plan."""

    api_name: str
    fields: tuple[str, ...]
    params: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        api_name = str(self.api_name or "").strip()
        if not api_name or re.fullmatch(r"[a-z][a-z0-9_]*", api_name) is None:
            raise ValueError("api_name must be a Tushare endpoint identifier")
        fields = tuple(str(value or "").strip() for value in self.fields)
        if not fields or any(not value for value in fields):
            raise ValueError("fields must be non-empty identifiers")
        if len(fields) != len(set(fields)):
            raise ValueError("fields must not contain duplicates")
        params: list[tuple[str, str]] = []
        seen: set[str] = set()
        for raw_key, raw_value in self.params:
            key = str(raw_key or "").strip()
            value = str(raw_value or "").strip()
            if not key or not value or key in seen:
                raise ValueError("request params must have unique non-empty values")
            seen.add(key)
            params.append((key, value))
        object.__setattr__(self, "api_name", api_name)
        object.__setattr__(self, "fields", fields)
        object.__setattr__(self, "params", tuple(sorted(params)))

    @property
    def provider_params(self) -> Mapping[str, str]:
        return MappingProxyType(dict(self.params))

    @property
    def fields_arg(self) -> str:
        return ",".join(self.fields)

    def to_dict(self) -> dict[str, Any]:
        return {
            "api_name": self.api_name,
            "fields": list(self.fields),
            "params": dict(self.params),
        }


@dataclass(frozen=True)
class DecisionOutcomeV2DatasetQuery:
    """Provider-independent, auditable query plan for one logical Dataset."""

    dataset: str
    requests: tuple[TushareDatasetRequestV2, ...]
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    source_dataset_hashes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        dataset = str(self.dataset or "").strip()
        if dataset not in DECISION_OUTCOME_V2_BENCHMARK_DATASETS:
            raise ValueError("unsupported Decision Outcome v2 benchmark dataset")
        requests = tuple(self.requests)
        if not requests:
            raise ValueError("query plan requires at least one physical request")
        expected_api = _API_BY_DATASET[dataset]
        expected_fields = DECISION_OUTCOME_V2_REQUIRED_FIELDS[dataset]
        for request in requests:
            if request.api_name != expected_api:
                raise ValueError("query plan endpoint conflicts with dataset")
            if request.fields != expected_fields:
                raise ValueError("query plan fields conflict with dataset schema")
        if (self.start_date is None) != (self.end_date is None):
            raise ValueError("query plan date window must contain both endpoints")
        if self.start_date is not None:
            _validate_window(self.start_date, self.end_date)
        hashes = _dataset_hashes(self.source_dataset_hashes)
        _validate_query_contract(
            dataset=dataset,
            requests=requests,
            start_date=self.start_date,
            end_date=self.end_date,
            source_dataset_hashes=hashes,
        )
        object.__setattr__(self, "dataset", dataset)
        object.__setattr__(self, "requests", requests)
        object.__setattr__(self, "source_dataset_hashes", hashes)

    def to_audit_params(self) -> Mapping[str, Any]:
        return MappingProxyType(
            {
                "requests": [request.to_dict() for request in self.requests],
                "source_dataset_hashes": list(self.source_dataset_hashes),
            }
        )


@dataclass(frozen=True)
class Sw1MembershipResolutionV2:
    """Frozen point-in-time SW1 membership consumed by the outcome evaluator."""

    status: str
    reason: Optional[str] = None
    industry_code: Optional[str] = None
    industry_name: Optional[str] = None
    effective_from: Optional[date] = None
    effective_to: Optional[date] = None
    known_at: Optional[datetime] = None
    stock_code: Optional[str] = None
    dataset_hashes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status not in {"available", "unavailable"}:
            raise ValueError("SW1 membership status must be available/unavailable")
        hashes = _dataset_hashes(self.dataset_hashes)
        object.__setattr__(self, "dataset_hashes", hashes)
        if self.status == "available":
            if self.reason is not None:
                raise ValueError("available SW1 membership cannot carry a reason")
            if (
                not self.industry_code
                or not self.industry_name
                or self.effective_from is None
                or self.known_at is None
                or not self.stock_code
                or len(hashes) < 2
            ):
                raise ValueError("available SW1 membership is not fully frozen")
            if _SW_INDEX_CODE_RE.fullmatch(self.industry_code) is None:
                raise ValueError("available SW1 membership code is invalid")
        elif not self.reason:
            raise ValueError("unavailable SW1 membership requires a reason")

    @property
    def frozen(self) -> bool:
        return self.status == "available" and len(self.dataset_hashes) >= 2

    def to_evaluator_mapping(self) -> Optional[Mapping[str, Any]]:
        if not self.frozen:
            return None
        assert self.industry_code is not None
        assert self.industry_name is not None
        assert self.effective_from is not None
        assert self.known_at is not None
        assert self.stock_code is not None
        return MappingProxyType(
            {
                "industry_code": self.industry_code,
                "industry_name": self.industry_name,
                "effective_from": self.effective_from.isoformat(),
                "effective_to": (
                    self.effective_to.isoformat()
                    if self.effective_to is not None
                    else None
                ),
                "known_at": self.known_at.isoformat(),
                "stock_code": self.stock_code,
                "snapshot_hash": self.dataset_hashes[-1],
            }
        )


def build_csi300_index_daily_query(
    entry_date: date,
    horizon_end_date: date,
) -> DecisionOutcomeV2DatasetQuery:
    """Query CSI300 only for the exact T+1-through-horizon window."""

    start, end = _validate_window(entry_date, horizon_end_date)
    request = _request(
        "index_daily",
        INDEX_DAILY_FIELDS,
        ts_code=CSI300_INDEX_CODE,
        start_date=_date_text(start),
        end_date=_date_text(end),
    )
    return DecisionOutcomeV2DatasetQuery(
        dataset=CSI300_INDEX_DAILY_DATASET,
        requests=(request,),
        start_date=start,
        end_date=end,
    )


def build_sw1_index_classify_query() -> DecisionOutcomeV2DatasetQuery:
    return DecisionOutcomeV2DatasetQuery(
        dataset=SW1_INDEX_CLASSIFY_DATASET,
        requests=(
            _request(
                "index_classify",
                SW1_INDEX_CLASSIFY_FIELDS,
                level=SW1_CLASSIFICATION_LEVEL,
                src=SW1_CLASSIFICATION_SOURCE,
            ),
        ),
    )


def build_sw1_index_member_all_query(
    stock_code: str,
) -> DecisionOutcomeV2DatasetQuery:
    """Fetch current and exited rows; the endpoint otherwise defaults to Y."""

    ts_code = to_tushare_ts_code(stock_code)
    return DecisionOutcomeV2DatasetQuery(
        dataset=SW1_INDEX_MEMBER_ALL_DATASET,
        requests=(
            _request(
                "index_member_all",
                SW1_INDEX_MEMBER_ALL_FIELDS,
                ts_code=ts_code,
                is_new="Y",
            ),
            _request(
                "index_member_all",
                SW1_INDEX_MEMBER_ALL_FIELDS,
                ts_code=ts_code,
                is_new="N",
            ),
        ),
    )


def build_sw1_index_daily_query(
    membership: Sw1MembershipResolutionV2,
    entry_date: date,
    horizon_end_date: date,
) -> DecisionOutcomeV2DatasetQuery:
    """Build ``sw_daily`` only from a previously frozen SW1 resolution."""

    if not isinstance(membership, Sw1MembershipResolutionV2) or not membership.frozen:
        raise ValueError("sw_daily requires a frozen SW1 membership resolution")
    assert membership.industry_code is not None
    start, end = _validate_window(entry_date, horizon_end_date)
    request = _request(
        "sw_daily",
        SW1_INDEX_DAILY_FIELDS,
        ts_code=membership.industry_code,
        start_date=_date_text(start),
        end_date=_date_text(end),
    )
    return DecisionOutcomeV2DatasetQuery(
        dataset=SW1_INDEX_DAILY_DATASET,
        requests=(request,),
        start_date=start,
        end_date=end,
        source_dataset_hashes=membership.dataset_hashes,
    )


def resolve_sw1_membership(
    *,
    stock_code: str,
    decision_date: date,
    decision_known_at: datetime,
    member_rows: Sequence[Mapping[str, Any]],
    classify_rows: Sequence[Mapping[str, Any]],
    member_known_at: datetime,
    classify_known_at: datetime,
    member_snapshot_hash: str,
    classify_snapshot_hash: str,
) -> Sw1MembershipResolutionV2:
    """Resolve one SW1 interval known no later than the decision date.

    Any duplicate or overlapping interval is treated as ambiguous.  The
    resolver never substitutes today's classification for a missing historical
    snapshot and never guesses an industry from a name.
    """

    try:
        ts_code = to_tushare_ts_code(stock_code)
        decision = _strict_date(decision_date, "decision_date")
        decision_cutoff = _aware_datetime(
            decision_known_at,
            "decision_known_at",
        )
        member_observed = _aware_datetime(member_known_at, "member_known_at")
        classify_observed = _aware_datetime(classify_known_at, "classify_known_at")
        hashes = _dataset_hashes(
            (member_snapshot_hash, classify_snapshot_hash)
        )
    except (TypeError, ValueError):
        return _unavailable_membership("invalid_sw1_snapshot_metadata")

    known_at = max(member_observed, classify_observed)
    if decision_cutoff.astimezone(CHINA_TIMEZONE).date() != decision:
        return _unavailable_membership("invalid_sw1_decision_cutoff")
    if known_at > decision_cutoff:
        return _unavailable_membership("sw1_membership_not_known_at_decision")

    intervals: list[tuple[date, Optional[date], Mapping[str, Any]]] = []
    for raw_row in member_rows:
        if not isinstance(raw_row, Mapping):
            return _unavailable_membership("invalid_sw1_member_schema")
        if any(field not in raw_row for field in SW1_INDEX_MEMBER_ALL_FIELDS):
            return _unavailable_membership("invalid_sw1_member_schema")
        if _text(raw_row.get("ts_code")) != ts_code:
            return _unavailable_membership("sw1_member_stock_mismatch")
        is_new = _text(raw_row.get("is_new")).upper()
        if is_new not in {"Y", "N"}:
            return _unavailable_membership("invalid_sw1_member_is_new")
        start = parse_tushare_date(raw_row.get("in_date"))
        raw_end = raw_row.get("out_date")
        end = parse_tushare_date(raw_end)
        if start is None or (
            raw_end not in (None, "") and end is None
        ) or (end is not None and end <= start):
            return _unavailable_membership("invalid_sw1_member_interval")
        if (is_new == "Y" and end is not None) or (is_new == "N" and end is None):
            return _unavailable_membership("inconsistent_sw1_member_interval")
        code = _text(raw_row.get("l1_code"))
        name = _text(raw_row.get("l1_name"))
        if _SW_INDEX_CODE_RE.fullmatch(code) is None or not name:
            return _unavailable_membership("invalid_sw1_member_l1")
        intervals.append((start, end, raw_row))

    ordered = sorted(intervals, key=lambda item: (item[0], item[1] or date.max))
    for previous, current in zip(ordered, ordered[1:]):
        previous_end = previous[1] or date.max
        if current[0] < previous_end:
            return _unavailable_membership("overlapping_sw1_membership_intervals")

    effective = [
        item
        for item in ordered
        if item[0] <= decision and (item[1] is None or decision < item[1])
    ]
    if not effective:
        return _unavailable_membership("missing_sw1_membership_at_decision")
    if len(effective) != 1:
        return _unavailable_membership("ambiguous_sw1_membership_at_decision")

    start, end, member = effective[0]
    industry_code = _text(member.get("l1_code"))
    member_name = _text(member.get("l1_name"))
    classifications: list[Mapping[str, Any]] = []
    for raw_row in classify_rows:
        if not isinstance(raw_row, Mapping):
            return _unavailable_membership("invalid_sw1_classify_schema")
        if any(field not in raw_row for field in SW1_INDEX_CLASSIFY_FIELDS):
            return _unavailable_membership("invalid_sw1_classify_schema")
        if (
            _text(raw_row.get("level")) != SW1_CLASSIFICATION_LEVEL
            or _text(raw_row.get("src")) != SW1_CLASSIFICATION_SOURCE
        ):
            return _unavailable_membership("invalid_sw1_classify_scope")
        if _text(raw_row.get("index_code")) == industry_code:
            classifications.append(raw_row)
    if not classifications:
        return _unavailable_membership("missing_sw1_classification")
    if len(classifications) != 1:
        return _unavailable_membership("ambiguous_sw1_classification")
    classification = classifications[0]
    industry_name = _text(classification.get("industry_name"))
    if not industry_name or industry_name != member_name:
        return _unavailable_membership("sw1_industry_name_mismatch")
    published = _text(classification.get("is_pub")).upper()
    if published in {"0", "N"}:
        return _unavailable_membership("sw1_index_not_published")
    if published not in {"1", "Y"}:
        return _unavailable_membership("invalid_sw1_index_publish_status")

    return Sw1MembershipResolutionV2(
        status="available",
        industry_code=industry_code,
        industry_name=industry_name,
        effective_from=start,
        effective_to=end,
        known_at=known_at,
        stock_code=ts_code,
        dataset_hashes=hashes,
    )


def validate_decision_outcome_v2_frames(
    query: DecisionOutcomeV2DatasetQuery,
    frames: Sequence[pd.DataFrame],
) -> pd.DataFrame:
    """Validate physical responses and return one detached combined frame."""

    if not isinstance(query, DecisionOutcomeV2DatasetQuery):
        raise TypeError("query must be a DecisionOutcomeV2DatasetQuery")
    materialized = tuple(frames)
    if len(materialized) != len(query.requests):
        raise ValueError("provider response count conflicts with query plan")
    combined: list[pd.DataFrame] = []
    for request, frame in zip(query.requests, materialized):
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("Tushare provider result must be a pandas DataFrame")
        columns = tuple(str(column) for column in frame.columns)
        if len(columns) != len(set(columns)):
            raise ValueError("Tushare provider result contains duplicate columns")
        missing = tuple(field for field in request.fields if field not in columns)
        if missing:
            raise ValueError(
                "Tushare provider result is missing required fields: "
                + ",".join(missing)
            )
        _validate_frame_rows(query, request, frame)
        combined.append(frame.loc[:, list(request.fields)].copy(deep=True))
    if len(combined) == 1:
        return combined[0]
    return pd.concat(combined, ignore_index=True)


def benchmark_dataset_unavailable_reason(
    dataset: str,
    status: str,
    error_code: Optional[str] = None,
) -> Optional[str]:
    """Translate a Dataset terminal state without collapsing failure classes."""

    name = str(dataset or "").strip()
    if name not in DECISION_OUTCOME_V2_BENCHMARK_DATASETS:
        raise ValueError("unsupported Decision Outcome v2 benchmark dataset")
    state = str(status or "").strip()
    if state == "available":
        return None
    if state == "permission_denied":
        if error_code == "authentication_failed":
            return f"{name}_authentication_failed"
        return f"{name}_permission_denied"
    if state == "empty":
        return f"{name}_empty"
    if error_code == "response_schema_error":
        return f"{name}_schema_drift"
    if state in {"partial", "stale", "not_supported", "fetch_failed"}:
        return f"{name}_{state}"
    return f"{name}_unavailable"


def _validate_frame_rows(
    query: DecisionOutcomeV2DatasetQuery,
    request: TushareDatasetRequestV2,
    frame: pd.DataFrame,
) -> None:
    params = request.provider_params
    records = frame.to_dict("records")
    expected_ts_code = params.get("ts_code")
    if expected_ts_code is not None and any(
        _text(row.get("ts_code")) != expected_ts_code for row in records
    ):
        raise ValueError("Tushare provider result conflicts with requested ts_code")
    if query.dataset == SW1_INDEX_CLASSIFY_DATASET:
        for row in records:
            if (
                _text(row.get("level")) != SW1_CLASSIFICATION_LEVEL
                or _text(row.get("src")) != SW1_CLASSIFICATION_SOURCE
                or not _text(row.get("index_code"))
                or not _text(row.get("industry_name"))
            ):
                raise ValueError("index_classify response conflicts with L1/SW2021")
        codes = [_text(row.get("index_code")) for row in records]
        if len(codes) != len(set(codes)):
            raise ValueError("index_classify response contains duplicate codes")
        return
    if query.dataset == SW1_INDEX_MEMBER_ALL_DATASET:
        expected_is_new = params.get("is_new")
        for row in records:
            if _text(row.get("is_new")).upper() != expected_is_new:
                raise ValueError("index_member_all response conflicts with is_new")
            if parse_tushare_date(row.get("in_date")) is None:
                raise ValueError("index_member_all response has invalid in_date")
            raw_end = row.get("out_date")
            if raw_end not in (None, "") and parse_tushare_date(raw_end) is None:
                raise ValueError("index_member_all response has invalid out_date")
        return

    dates: list[date] = []
    for row in records:
        trade_date = parse_tushare_date(row.get("trade_date"))
        if trade_date is None:
            raise ValueError("benchmark daily response has invalid trade_date")
        if query.start_date is None or query.end_date is None:
            raise ValueError("benchmark daily query is missing its exact window")
        if not query.start_date <= trade_date <= query.end_date:
            raise ValueError("benchmark daily response is outside the requested window")
        prices = {
            field: _positive_decimal(row.get(field), field)
            for field in ("open", "high", "low", "close")
        }
        if prices["high"] < max(prices["open"], prices["close"]):
            raise ValueError("benchmark daily response has invalid OHLC")
        if prices["low"] > min(prices["open"], prices["close"]):
            raise ValueError("benchmark daily response has invalid OHLC")
        if query.dataset == SW1_INDEX_DAILY_DATASET and not _text(row.get("name")):
            raise ValueError("sw_daily response is missing its index name")
        dates.append(trade_date)
    if len(dates) != len(set(dates)):
        raise ValueError("benchmark daily response contains duplicate sessions")


def _request(
    api_name: str,
    fields: tuple[str, ...],
    **params: str,
) -> TushareDatasetRequestV2:
    return TushareDatasetRequestV2(
        api_name=api_name,
        fields=fields,
        params=tuple(params.items()),
    )


def _validate_query_contract(
    *,
    dataset: str,
    requests: tuple[TushareDatasetRequestV2, ...],
    start_date: Optional[date],
    end_date: Optional[date],
    source_dataset_hashes: tuple[str, ...],
) -> None:
    params = [dict(request.params) for request in requests]
    if dataset == CSI300_INDEX_DAILY_DATASET:
        if len(requests) != 1 or start_date is None or end_date is None:
            raise ValueError("CSI300 query requires one exact date-window request")
        if params[0] != {
            "ts_code": CSI300_INDEX_CODE,
            "start_date": _date_text(start_date),
            "end_date": _date_text(end_date),
        }:
            raise ValueError("CSI300 query parameters conflict with exact window")
        if source_dataset_hashes:
            raise ValueError("CSI300 query cannot carry SW1 source hashes")
        return
    if dataset == SW1_INDEX_CLASSIFY_DATASET:
        if (
            len(requests) != 1
            or start_date is not None
            or end_date is not None
            or params[0]
            != {
                "level": SW1_CLASSIFICATION_LEVEL,
                "src": SW1_CLASSIFICATION_SOURCE,
            }
            or source_dataset_hashes
        ):
            raise ValueError("SW1 classification query contract is invalid")
        return
    if dataset == SW1_INDEX_MEMBER_ALL_DATASET:
        if (
            len(requests) != 2
            or start_date is not None
            or end_date is not None
            or source_dataset_hashes
        ):
            raise ValueError("SW1 membership query contract is invalid")
        ts_codes = [item.get("ts_code") for item in params]
        try:
            valid_stock = (
                ts_codes[0] is not None
                and ts_codes[0] == ts_codes[1]
                and to_tushare_ts_code(ts_codes[0]) == ts_codes[0]
            )
        except ValueError:
            valid_stock = False
        if not valid_stock or [item.get("is_new") for item in params] != ["Y", "N"]:
            raise ValueError("SW1 membership query must cover Y and N for one stock")
        if any(set(item) != {"ts_code", "is_new"} for item in params):
            raise ValueError("SW1 membership query parameters are invalid")
        return
    if dataset == SW1_INDEX_DAILY_DATASET:
        if (
            len(requests) != 1
            or start_date is None
            or end_date is None
            or len(source_dataset_hashes) < 2
        ):
            raise ValueError("SW1 daily query requires frozen membership and one window")
        code = str(params[0].get("ts_code") or "")
        if _SW_INDEX_CODE_RE.fullmatch(code) is None or params[0] != {
            "ts_code": code,
            "start_date": _date_text(start_date),
            "end_date": _date_text(end_date),
        }:
            raise ValueError("SW1 daily query parameters conflict with exact window")
        return
    raise ValueError("unsupported Decision Outcome v2 benchmark dataset")


def _validate_window(start: date, end: Optional[date]) -> tuple[date, date]:
    normalized_start = _strict_date(start, "entry_date")
    normalized_end = _strict_date(end, "horizon_end_date")
    if normalized_start > normalized_end:
        raise ValueError("entry_date cannot be after horizon_end_date")
    return normalized_start, normalized_end


def _strict_date(value: Any, field: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError(f"{field} must be a date")
    return value


def _aware_datetime(value: Any, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone offset")
    return value.astimezone(timezone.utc)


def _date_text(value: date) -> str:
    return value.strftime("%Y%m%d")


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _positive_decimal(value: Any, field: str) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError(f"benchmark daily {field} must be positive")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"benchmark daily {field} must be positive") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError(f"benchmark daily {field} must be positive")
    return number


def _dataset_hashes(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError("dataset hashes must be a sequence")
    hashes = tuple(sorted({str(value or "").strip().lower() for value in values}))
    if any(_SHA256_RE.fullmatch(value) is None for value in hashes):
        raise ValueError("dataset hashes must contain SHA-256 values")
    return hashes


def _unavailable_membership(reason: str) -> Sw1MembershipResolutionV2:
    return Sw1MembershipResolutionV2(status="unavailable", reason=reason)


__all__ = [
    "CSI300_INDEX_CODE",
    "CSI300_INDEX_DAILY_DATASET",
    "CSI300_INDEX_NAME",
    "DECISION_OUTCOME_V2_BENCHMARK_DATASETS",
    "DECISION_OUTCOME_V2_REQUIRED_FIELDS",
    "DecisionOutcomeV2DatasetQuery",
    "INDEX_DAILY_FIELDS",
    "SW1_CLASSIFICATION_LEVEL",
    "SW1_CLASSIFICATION_SOURCE",
    "SW1_INDEX_CLASSIFY_DATASET",
    "SW1_INDEX_CLASSIFY_FIELDS",
    "SW1_INDEX_DAILY_DATASET",
    "SW1_INDEX_DAILY_FIELDS",
    "SW1_INDEX_MEMBER_ALL_DATASET",
    "SW1_INDEX_MEMBER_ALL_FIELDS",
    "Sw1MembershipResolutionV2",
    "TushareDatasetRequestV2",
    "benchmark_dataset_unavailable_reason",
    "build_csi300_index_daily_query",
    "build_sw1_index_classify_query",
    "build_sw1_index_daily_query",
    "build_sw1_index_member_all_query",
    "resolve_sw1_membership",
    "validate_decision_outcome_v2_frames",
]
