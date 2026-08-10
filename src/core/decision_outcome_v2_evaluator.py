# -*- coding: utf-8 -*-
"""Pure Decision Outcome v2 evaluation for personal-research signals.

The evaluator is deliberately provider- and persistence-agnostic.  Callers
must supply the frozen XSHG calendar, stock bars, execution-state datasets,
and benchmark series that belong to the same research lineage.  Missing
benchmarks are recorded explicitly and are never substituted.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Mapping, Optional, Sequence


DECISION_OUTCOME_V2_ENGINE_VERSION = "personal-research-outcome-v2"
SUPPORTED_DECISION_OUTCOME_V2_HORIZONS = {
    "5d": 5,
    "10d": 10,
    "20d": 20,
}
TERMINAL_DECISION_OUTCOME_V2_STATUSES = frozenset({
    "evaluated",
    "observational",
    "unable",
    "unexecutable",
})
LONG_ACTIONS = frozenset({"open", "add", "open_candidate", "add_candidate"})
DEFENSIVE_ACTIONS = frozenset({
    "reduce",
    "exit",
    "reduce_candidate",
    "exit_candidate",
})
OBSERVATIONAL_ACTIONS = frozenset({"observe", "hold"})

_RESUME_TYPES = frozenset({"R", "RESUME", "RESUMED", "\u590d\u724c"})
_DATE_KEYS = ("trade_date", "date", "session_date")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SW1_CODE_RE = re.compile(r"^\d{6}\.SI$")
_XSHG_TIMEZONE = timezone(timedelta(hours=8))
_XSHG_SESSION_CLOSE = time(15, 0)


@dataclass(frozen=True)
class OutcomeBarV2:
    """One raw daily bar plus its point-in-time adjustment factor."""

    trade_date: date
    open: Any
    high: Any
    low: Any
    close: Any
    adj_factor: Any = None


@dataclass(frozen=True)
class PointInTimeSw1Membership:
    """SW1 membership on ``[effective_from, effective_to)`` known at signal time."""

    industry_code: str
    effective_from: date
    industry_name: Optional[str] = None
    effective_to: Optional[date] = None
    known_at: Optional[date | datetime] = None
    stock_code: Optional[str] = None
    snapshot_hash: Optional[str] = None


@dataclass(frozen=True)
class BenchmarkOutcomeV2:
    """Independent benchmark evaluation; unavailable never implies fallback."""

    code: Optional[str]
    name: Optional[str]
    status: str
    reason: Optional[str] = None
    return_pct: Optional[float] = None
    stock_excess_return_pct: Optional[float] = None
    directional_excess_return_pct: Optional[float] = None


@dataclass(frozen=True)
class DecisionOutcomeV2Evaluation:
    """Deterministic result for one signal/horizon pair."""

    horizon: str
    engine_version: str
    final_action_family: str
    eval_status: str
    outcome: Optional[str] = None
    direction_correct: Optional[bool] = None
    reason: Optional[str] = None
    signal_session: Optional[date] = None
    entry_trade_date: Optional[date] = None
    end_trade_date: Optional[date] = None
    trading_day_count: Optional[int] = None
    entry_raw_open: Optional[float] = None
    entry_adj_factor: Optional[float] = None
    end_adjusted_close: Optional[float] = None
    stock_return_pct: Optional[float] = None
    directional_return_pct: Optional[float] = None
    mfe_pct: Optional[float] = None
    mae_pct: Optional[float] = None
    dataset_hashes: tuple[str, ...] = ()
    csi300: BenchmarkOutcomeV2 = field(
        default_factory=lambda: BenchmarkOutcomeV2(
            code="000300.SH",
            name="CSI 300",
            status="unavailable",
            reason="not_evaluated",
        )
    )
    sw1: BenchmarkOutcomeV2 = field(
        default_factory=lambda: BenchmarkOutcomeV2(
            code=None,
            name=None,
            status="unavailable",
            reason="not_evaluated",
        )
    )

    @property
    def terminal(self) -> bool:
        return self.eval_status in TERMINAL_DECISION_OUTCOME_V2_STATUSES

    def to_fields(self) -> dict[str, Any]:
        """Return JSON/record-friendly fields without exposing implementation objects."""

        payload = asdict(self)
        payload["dataset_hashes"] = list(self.dataset_hashes)
        payload["terminal"] = self.terminal
        return payload


class DecisionOutcomeV2Evaluator:
    """Evaluate one immutable personal-research decision without I/O."""

    @classmethod
    def evaluate(
        cls,
        *,
        final_action_family: str,
        horizon: str,
        signal_session: date,
        xshg_sessions: Sequence[Any],
        stock_bars: Sequence[Any],
        suspend_rows: Sequence[Any] = (),
        stk_limit_rows: Sequence[Any] = (),
        csi300_bars: Sequence[Any] = (),
        csi300_unavailable_reason: Optional[str] = None,
        sw1_membership: Optional[PointInTimeSw1Membership | Mapping[str, Any]] = None,
        sw1_bars: Sequence[Any] = (),
        sw1_unavailable_reason: Optional[str] = None,
        stock_code: Optional[str] = None,
        dataset_hashes: Sequence[str] = (),
        evaluation_as_of: Optional[date | datetime] = None,
        engine_version: str = DECISION_OUTCOME_V2_ENGINE_VERSION,
    ) -> DecisionOutcomeV2Evaluation:
        family = normalize_final_action_family(final_action_family)
        invalid_lineage = False
        try:
            lineage_hashes = cls._dataset_hashes(dataset_hashes)
        except (TypeError, ValueError):
            lineage_hashes = ()
            invalid_lineage = True
        horizon_norm = str(horizon or "").strip().lower()
        eval_days = SUPPORTED_DECISION_OUTCOME_V2_HORIZONS.get(horizon_norm)
        base = {
            "horizon": horizon_norm,
            "engine_version": cls._nonempty(engine_version, "engine_version"),
            "final_action_family": family,
            "signal_session": signal_session if isinstance(signal_session, date) else None,
            "dataset_hashes": lineage_hashes,
        }
        try:
            evaluation_cutoff = cls._evaluation_cutoff(evaluation_as_of)
        except (TypeError, ValueError):
            return cls._unable(base, "invalid_evaluation_as_of")
        if eval_days is None:
            return cls._unable(base, "unsupported_horizon")
        signal_date = cls._optional_date(signal_session)
        if signal_date is None:
            return cls._unable(base, "invalid_signal_session")
        base["signal_session"] = signal_date

        try:
            sessions = cls._normalized_sessions(xshg_sessions)
        except (TypeError, ValueError):
            return cls._unable(base, "invalid_xshg_calendar")
        forward_sessions = [item for item in sessions if item > signal_date]
        if len(forward_sessions) < eval_days:
            return cls._pending(base, "insufficient_xshg_sessions")

        window_sessions = forward_sessions[:eval_days]
        entry_date = window_sessions[0]
        end_date = window_sessions[-1]
        window_base = {
            **base,
            "entry_trade_date": entry_date,
            "end_trade_date": end_date,
            "trading_day_count": eval_days,
        }
        if evaluation_cutoff is not None and not cls._session_completed(
            entry_date,
            evaluation_cutoff,
        ):
            return cls._pending(window_base, "entry_session_not_reached")
        if evaluation_cutoff is not None and not cls._session_completed(
            end_date,
            evaluation_cutoff,
        ):
            return cls._pending(window_base, "horizon_not_reached")
        if invalid_lineage:
            return cls._unable(window_base, "invalid_dataset_hashes")
        if not lineage_hashes:
            return cls._unable(window_base, "missing_dataset_hashes")
        try:
            suspended_dates = cls._suspended_dates(suspend_rows)
        except (TypeError, ValueError):
            return cls._unable(window_base, "invalid_suspend_series")
        try:
            dated_stock = cls._bars_by_date(stock_bars)
        except (TypeError, ValueError):
            return cls._unable(window_base, "invalid_stock_bar_series")
        entry_bar = dated_stock.get(entry_date)

        if family != "observational" and entry_date in suspended_dates:
            return cls._unexecutable(window_base, "entry_suspended")
        if entry_bar is None:
            return cls._unable(window_base, "missing_entry_stock_bar")

        try:
            entry_prices = cls._validated_prices(entry_bar)
            entry_factor = cls._positive_decimal(
                cls._field(entry_bar, "adj_factor"),
                "invalid_entry_adj_factor",
            )
        except ValueError as exc:
            return cls._unable(window_base, str(exc))
        entry_open = entry_prices["open"]

        if family != "observational":
            try:
                limit_row = cls._row_on_date(stk_limit_rows, entry_date)
            except (TypeError, ValueError):
                return cls._unable(window_base, "invalid_entry_stk_limit_series")
            if limit_row is None:
                return cls._unable(
                    {
                        **window_base,
                        "entry_raw_open": float(entry_open),
                        "entry_adj_factor": float(entry_factor),
                    },
                    "missing_entry_stk_limit",
                )
            unexecutable_reason = cls._limit_unexecutable_reason(
                family=family,
                prices=entry_prices,
                limit_row=limit_row,
            )
            if unexecutable_reason == "invalid_entry_stk_limit":
                return cls._unable(
                    {
                        **window_base,
                        "entry_raw_open": float(entry_open),
                        "entry_adj_factor": float(entry_factor),
                    },
                    unexecutable_reason,
                )
            if unexecutable_reason is not None:
                return cls._unexecutable(
                    {
                        **window_base,
                        "entry_raw_open": float(entry_open),
                        "entry_adj_factor": float(entry_factor),
                    },
                    unexecutable_reason,
                )

        end_bar = dated_stock.get(end_date)
        if end_bar is None:
            if end_date in suspended_dates:
                return cls._unable(
                    {
                        **window_base,
                        "entry_raw_open": float(entry_open),
                        "entry_adj_factor": float(entry_factor),
                    },
                    "end_suspended_without_close",
                )
            return cls._pending(
                {
                    **window_base,
                    "entry_raw_open": float(entry_open),
                    "entry_adj_factor": float(entry_factor),
                },
                "missing_end_stock_bar",
            )

        adjusted_highs: list[Decimal] = []
        adjusted_lows: list[Decimal] = []
        end_adjusted_close: Optional[Decimal] = None
        for session in window_sessions:
            bar = dated_stock.get(session)
            # A suspended intermediate session normally has no daily bar.  It
            # still counts in the XSHG horizon, while extrema use traded bars.
            if bar is None:
                if session in suspended_dates:
                    continue
                return cls._unable(
                    {
                        **window_base,
                        "entry_raw_open": float(entry_open),
                        "entry_adj_factor": float(entry_factor),
                    },
                    "missing_window_stock_bar",
                )
            try:
                prices = cls._validated_prices(bar)
                factor = cls._positive_decimal(
                    cls._field(bar, "adj_factor"),
                    "invalid_window_adj_factor",
                )
            except ValueError as exc:
                return cls._unable(
                    {
                        **window_base,
                        "entry_raw_open": float(entry_open),
                        "entry_adj_factor": float(entry_factor),
                    },
                    str(exc),
                )
            adjusted_highs.append(prices["high"] * factor / entry_factor)
            adjusted_lows.append(prices["low"] * factor / entry_factor)
            if session == end_date:
                end_adjusted_close = prices["close"] * factor / entry_factor

        if end_adjusted_close is None:
            return cls._pending(
                {
                    **window_base,
                    "entry_raw_open": float(entry_open),
                    "entry_adj_factor": float(entry_factor),
                },
                "missing_end_stock_bar",
            )
        if not adjusted_highs or not adjusted_lows:
            return cls._unable(window_base, "missing_window_extrema")

        stock_return = (end_adjusted_close / entry_open - Decimal("1")) * Decimal("100")
        directional_multiplier = (
            Decimal("1")
            if family == "long"
            else Decimal("-1") if family == "defensive" else None
        )
        directional_return = stock_return * directional_multiplier if directional_multiplier is not None else None
        max_high = max(adjusted_highs)
        min_low = min(adjusted_lows)
        if family == "long":
            mfe = max(Decimal("0"), (max_high / entry_open - Decimal("1")) * Decimal("100"))
            mae = max(Decimal("0"), (Decimal("1") - min_low / entry_open) * Decimal("100"))
        elif family == "defensive":
            mfe = max(Decimal("0"), (Decimal("1") - min_low / entry_open) * Decimal("100"))
            mae = max(Decimal("0"), (max_high / entry_open - Decimal("1")) * Decimal("100"))
        else:
            mfe = None
            mae = None

        if directional_return is None:
            outcome = None
            direction_correct = None
        elif directional_return > 0:
            outcome = "hit"
            direction_correct: Optional[bool] = True
        else:
            outcome = "miss"
            direction_correct = False

        csi300 = cls._benchmark_outcome(
            code="000300.SH",
            name="CSI 300",
            bars=csi300_bars,
            entry_date=entry_date,
            end_date=end_date,
            stock_return=stock_return,
            family=family,
            missing_reason=cls._benchmark_reason(
                csi300_unavailable_reason,
                default="missing_csi300_benchmark",
                invalid="invalid_csi300_unavailable_reason",
            ),
            prefix="csi300",
        )
        membership, membership_reason = cls._normalize_sw1_membership(
            sw1_membership,
            signal_session=signal_date,
            stock_code=stock_code,
        )
        if membership is None:
            sw1 = BenchmarkOutcomeV2(
                code=None,
                name=None,
                status="unavailable",
                reason=cls._benchmark_reason(
                    sw1_unavailable_reason,
                    default=membership_reason,
                    invalid="invalid_sw1_unavailable_reason",
                ),
            )
        else:
            sw1 = cls._benchmark_outcome(
                code=membership.industry_code,
                name=membership.industry_name,
                bars=sw1_bars,
                entry_date=entry_date,
                end_date=end_date,
                stock_return=stock_return,
                family=family,
                missing_reason=cls._benchmark_reason(
                    sw1_unavailable_reason,
                    default="missing_sw1_benchmark",
                    invalid="invalid_sw1_unavailable_reason",
                ),
                prefix="sw1",
            )

        return DecisionOutcomeV2Evaluation(
            **window_base,
            eval_status="observational" if family == "observational" else "evaluated",
            outcome=outcome,
            direction_correct=direction_correct,
            entry_raw_open=float(entry_open),
            entry_adj_factor=float(entry_factor),
            end_adjusted_close=float(end_adjusted_close),
            stock_return_pct=float(stock_return),
            directional_return_pct=float(directional_return) if directional_return is not None else None,
            mfe_pct=float(mfe) if mfe is not None else None,
            mae_pct=float(mae) if mae is not None else None,
            csi300=csi300,
            sw1=sw1,
        )

    @classmethod
    def _benchmark_outcome(
        cls,
        *,
        code: str,
        name: Optional[str],
        bars: Sequence[Any],
        entry_date: date,
        end_date: date,
        stock_return: Decimal,
        family: str,
        missing_reason: str,
        prefix: str,
    ) -> BenchmarkOutcomeV2:
        if not bars:
            return BenchmarkOutcomeV2(code=code, name=name, status="unavailable", reason=missing_reason)
        try:
            dated = cls._bars_by_date(bars)
        except (TypeError, ValueError):
            return BenchmarkOutcomeV2(
                code=code,
                name=name,
                status="unavailable",
                reason=f"invalid_{prefix}_bar_series",
            )
        entry_bar = dated.get(entry_date)
        if entry_bar is None:
            return BenchmarkOutcomeV2(
                code=code,
                name=name,
                status="unavailable",
                reason=f"missing_{prefix}_entry_bar",
            )
        end_bar = dated.get(end_date)
        if end_bar is None:
            return BenchmarkOutcomeV2(
                code=code,
                name=name,
                status="unavailable",
                reason=f"missing_{prefix}_end_bar",
            )
        try:
            entry_open = cls._positive_decimal(cls._field(entry_bar, "open"), f"invalid_{prefix}_entry_open")
            end_close = cls._positive_decimal(cls._field(end_bar, "close"), f"invalid_{prefix}_end_close")
        except ValueError as exc:
            return BenchmarkOutcomeV2(code=code, name=name, status="unavailable", reason=str(exc))
        benchmark_return = (end_close / entry_open - Decimal("1")) * Decimal("100")
        stock_excess = stock_return - benchmark_return
        multiplier = Decimal("1") if family == "long" else Decimal("-1") if family == "defensive" else None
        return BenchmarkOutcomeV2(
            code=code,
            name=name,
            status="available",
            return_pct=float(benchmark_return),
            stock_excess_return_pct=float(stock_excess),
            directional_excess_return_pct=(
                float(stock_excess * multiplier) if multiplier is not None else None
            ),
        )

    @staticmethod
    def _benchmark_reason(
        value: Optional[str],
        *,
        default: str,
        invalid: str,
    ) -> str:
        if value is None:
            return default
        normalized = str(value).strip()
        if re.fullmatch(r"[a-z][a-z0-9_]{0,127}", normalized) is None:
            return invalid
        return normalized

    @classmethod
    def _normalize_sw1_membership(
        cls,
        value: Optional[PointInTimeSw1Membership | Mapping[str, Any]],
        *,
        signal_session: date,
        stock_code: Optional[str],
    ) -> tuple[Optional[PointInTimeSw1Membership], str]:
        if value is None:
            return None, "missing_point_in_time_sw1_membership"
        try:
            if isinstance(value, PointInTimeSw1Membership):
                raw = value
            elif isinstance(value, Mapping):
                raw = PointInTimeSw1Membership(
                    industry_code=str(value.get("industry_code") or value.get("index_code") or "").strip(),
                    industry_name=str(value.get("industry_name") or value.get("index_name") or "").strip() or None,
                    effective_from=cls._as_date(value.get("effective_from") or value.get("in_date")),
                    effective_to=cls._optional_date_strict(value.get("effective_to") or value.get("out_date")),
                    known_at=cls._optional_datetime_or_date(value.get("known_at") or value.get("available_at")),
                    stock_code=str(value.get("stock_code") or value.get("ts_code") or "").strip() or None,
                    snapshot_hash=str(value.get("snapshot_hash") or "").strip() or None,
                )
            else:
                return None, "invalid_point_in_time_sw1_membership"
            membership = PointInTimeSw1Membership(
                industry_code=str(raw.industry_code or "").strip(),
                effective_from=cls._as_date(raw.effective_from),
                industry_name=str(raw.industry_name or "").strip() or None,
                effective_to=cls._optional_date_strict(raw.effective_to),
                known_at=cls._optional_datetime_or_date(raw.known_at),
                stock_code=str(raw.stock_code or "").strip() or None,
                snapshot_hash=str(raw.snapshot_hash or "").strip() or None,
            )
        except (TypeError, ValueError):
            return None, "invalid_point_in_time_sw1_membership"
        if (
            _SW1_CODE_RE.fullmatch(membership.industry_code) is None
            or not membership.industry_name
            or membership.known_at is None
        ):
            return None, "invalid_point_in_time_sw1_membership"
        if (
            membership.effective_to is not None
            and membership.effective_to <= membership.effective_from
        ):
            return None, "invalid_point_in_time_sw1_membership"
        if membership.effective_from > signal_session:
            return None, "sw1_membership_not_effective_at_signal"
        if membership.effective_to is not None and signal_session >= membership.effective_to:
            return None, "sw1_membership_not_effective_at_signal"
        known_date = cls._date_part(membership.known_at)
        if known_date is not None and known_date > signal_session:
            return None, "sw1_membership_not_known_at_signal"
        if stock_code and membership.stock_code and cls._normalize_stock_code(stock_code) != cls._normalize_stock_code(
            membership.stock_code
        ):
            return None, "sw1_membership_stock_mismatch"
        return membership, ""

    @classmethod
    def _limit_unexecutable_reason(
        cls,
        *,
        family: str,
        prices: Mapping[str, Decimal],
        limit_row: Any,
    ) -> Optional[str]:
        up = cls._optional_decimal(cls._field(limit_row, "up_limit"))
        down = cls._optional_decimal(cls._field(limit_row, "down_limit"))
        if up is None or down is None or up <= 0 or down <= 0:
            return "invalid_entry_stk_limit"
        one_price_up = all(prices[key] == up for key in ("open", "high", "low", "close"))
        one_price_down = all(prices[key] == down for key in ("open", "high", "low", "close"))
        if family == "long" and one_price_up:
            return "entry_one_price_limit_up"
        if family == "defensive" and one_price_down:
            return "entry_one_price_limit_down"
        return None

    @classmethod
    def _suspended_dates(cls, rows: Sequence[Any]) -> frozenset[date]:
        suspended: set[date] = set()
        for row in rows:
            trade_date = cls._row_date(row)
            if trade_date is None:
                raise ValueError("suspend row trade_date is required")
            raw = cls._field(row, "suspend_type", cls._field(row, "status", "S"))
            if str(raw or "S").strip().upper() not in _RESUME_TYPES:
                suspended.add(trade_date)
        return frozenset(suspended)

    @classmethod
    def _validated_prices(cls, bar: Any) -> dict[str, Decimal]:
        prices = {
            key: cls._positive_decimal(cls._field(bar, key), f"invalid_window_{key}")
            for key in ("open", "high", "low", "close")
        }
        if prices["high"] < max(prices["open"], prices["close"]):
            raise ValueError("invalid_window_ohlc")
        if prices["low"] > min(prices["open"], prices["close"]):
            raise ValueError("invalid_window_ohlc")
        return prices

    @classmethod
    def _bars_by_date(cls, bars: Sequence[Any]) -> dict[date, Any]:
        result: dict[date, Any] = {}
        for bar in bars:
            trade_date = cls._row_date(bar)
            if trade_date is None:
                raise ValueError("bar trade_date is required")
            if trade_date in result:
                raise ValueError(f"duplicate bar for {trade_date.isoformat()}")
            result[trade_date] = bar
        return result

    @classmethod
    def _row_on_date(cls, rows: Sequence[Any], target: date) -> Optional[Any]:
        matches = []
        for row in rows:
            trade_date = cls._row_date(row)
            if trade_date is None:
                raise ValueError("row trade_date is required")
            if trade_date == target:
                matches.append(row)
        if len(matches) > 1:
            raise ValueError(f"duplicate row for {target.isoformat()}")
        return matches[0] if matches else None

    @classmethod
    def _row_date(cls, value: Any) -> Optional[date]:
        for key in _DATE_KEYS:
            raw = cls._field(value, key)
            parsed = cls._optional_date(raw)
            if parsed is not None:
                return parsed
        return None

    @classmethod
    def _normalized_sessions(cls, values: Sequence[Any]) -> list[date]:
        sessions = [cls._as_date(value) for value in values]
        if sessions != sorted(sessions) or len(sessions) != len(set(sessions)):
            raise ValueError("XSHG sessions must be strictly increasing")
        return sessions

    @staticmethod
    def _field(value: Any, key: str, default: Any = None) -> Any:
        if isinstance(value, Mapping):
            return value.get(key, default)
        return getattr(value, key, default)

    @staticmethod
    def _positive_decimal(value: Any, reason: str) -> Decimal:
        number = DecisionOutcomeV2Evaluator._optional_decimal(value)
        if number is None or number <= 0:
            raise ValueError(reason)
        return number

    @staticmethod
    def _optional_decimal(value: Any) -> Optional[Decimal]:
        if value is None or isinstance(value, bool):
            return None
        try:
            number = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None
        return number if number.is_finite() else None

    @staticmethod
    def _as_date(value: Any) -> date:
        parsed = DecisionOutcomeV2Evaluator._optional_date(value)
        if parsed is None:
            raise ValueError("invalid date")
        return parsed

    @staticmethod
    def _optional_date(value: Any) -> Optional[date]:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            compact = text.replace("-", "")
            try:
                if len(compact) == 8 and compact.isdigit():
                    return date(int(compact[:4]), int(compact[4:6]), int(compact[6:]))
                return date.fromisoformat(text[:10])
            except ValueError:
                return None
        return None

    @staticmethod
    def _optional_date_strict(value: Any) -> Optional[date]:
        if value in (None, ""):
            return None
        return DecisionOutcomeV2Evaluator._as_date(value)

    @staticmethod
    def _optional_datetime_or_date(value: Any) -> Optional[date | datetime]:
        if value in (None, ""):
            return None
        if isinstance(value, (date, datetime)):
            return value
        if isinstance(value, str):
            text = value.strip()
            try:
                return datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return DecisionOutcomeV2Evaluator._optional_date(text)
        raise ValueError("invalid datetime")

    @staticmethod
    def _date_part(value: Optional[date | datetime]) -> Optional[date]:
        if isinstance(value, datetime):
            if value.tzinfo is not None and value.utcoffset() is not None:
                return value.astimezone(_XSHG_TIMEZONE).date()
            return value.date()
        return value

    @staticmethod
    def _evaluation_cutoff(
        value: Optional[date | datetime],
    ) -> Optional[tuple[date, time]]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("evaluation_as_of datetime must be timezone-aware")
            local_value = value.astimezone(_XSHG_TIMEZONE)
            return local_value.date(), local_value.time().replace(tzinfo=None)
        if isinstance(value, date):
            return value, time.max
        raise TypeError("evaluation_as_of must be a date or datetime")

    @staticmethod
    def _session_completed(
        session: date,
        cutoff: tuple[date, time],
    ) -> bool:
        cutoff_date, cutoff_time = cutoff
        return cutoff_date > session or (
            cutoff_date == session and cutoff_time >= _XSHG_SESSION_CLOSE
        )

    @staticmethod
    def _normalize_stock_code(value: str) -> str:
        return str(value or "").strip().upper().split(".", 1)[0]

    @staticmethod
    def _dataset_hashes(values: Sequence[str]) -> tuple[str, ...]:
        if isinstance(values, (str, bytes, bytearray)):
            raise TypeError("dataset_hashes must be a sequence")
        normalized: set[str] = set()
        for value in values:
            digest = str(value or "").strip().lower()
            if not _SHA256_RE.fullmatch(digest):
                raise ValueError("dataset_hashes must contain SHA-256 values")
            normalized.add(digest)
        return tuple(sorted(normalized))

    @staticmethod
    def _nonempty(value: Any, field_name: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"{field_name} is required")
        return text

    @staticmethod
    def _pending(base: Mapping[str, Any], reason: str) -> DecisionOutcomeV2Evaluation:
        return DecisionOutcomeV2Evaluation(**base, eval_status="pending", reason=reason)

    @staticmethod
    def _unable(base: Mapping[str, Any], reason: str) -> DecisionOutcomeV2Evaluation:
        return DecisionOutcomeV2Evaluation(**base, eval_status="unable", reason=reason)

    @staticmethod
    def _unexecutable(base: Mapping[str, Any], reason: str) -> DecisionOutcomeV2Evaluation:
        return DecisionOutcomeV2Evaluation(**base, eval_status="unexecutable", reason=reason)


def normalize_final_action_family(value: Any) -> str:
    """Normalize an action or already-canonical family into v2's families."""

    text = str(value or "").strip().lower()
    if text in {"long", *LONG_ACTIONS}:
        return "long"
    if text in {"defensive", *DEFENSIVE_ACTIONS}:
        return "defensive"
    if text in {"observational", *OBSERVATIONAL_ACTIONS}:
        return "observational"
    raise ValueError("final_action_family must be long/defensive/observational or a supported action")


__all__ = [
    "BenchmarkOutcomeV2",
    "DECISION_OUTCOME_V2_ENGINE_VERSION",
    "DecisionOutcomeV2Evaluation",
    "DecisionOutcomeV2Evaluator",
    "OutcomeBarV2",
    "PointInTimeSw1Membership",
    "SUPPORTED_DECISION_OUTCOME_V2_HORIZONS",
    "TERMINAL_DECISION_OUTCOME_V2_STATUSES",
    "normalize_final_action_family",
]
