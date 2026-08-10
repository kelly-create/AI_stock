"""Strict persistence for immutable personal-research Decision Outcome v2 rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import math
import re
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import and_, func, select

from src.services.research.canonical import canonical_hash, canonical_json
from src.storage import (
    DatabaseManager,
    DecisionOutcomeV2Record,
    DecisionSignalRecord,
    PortfolioPolicyEvaluationRecord,
    utc_naive_now,
)


DECISION_OUTCOME_V2_CONTRACT = "decision-outcome-v2"
DECISION_OUTCOME_V2_HORIZONS = ("5d", "10d", "20d")
DECISION_OUTCOME_V2_TERMINAL_STATUSES = frozenset(
    {"evaluated", "observational", "unexecutable", "unable"}
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_EVAL_STATUSES = frozenset({"pending", *DECISION_OUTCOME_V2_TERMINAL_STATUSES})
_ACTION_FAMILY_BY_ACCOUNT_ACTION = {
    "open_candidate": "long",
    "add_candidate": "long",
    "reduce_candidate": "defensive",
    "exit_candidate": "defensive",
    "observe": "observational",
    "hold": "observational",
}
_UNEXECUTABLE_REASONS = frozenset(
    {
        "entry_suspended",
        "entry_one_price_limit_up",
        "entry_one_price_limit_down",
    }
)
_EVALUATION_FIELDS = frozenset(
    {
        "horizon",
        "engine_version",
        "final_action_family",
        "eval_status",
        "outcome",
        "direction_correct",
        "reason",
        "reason_code",
        "signal_session",
        "entry_trade_date",
        "end_trade_date",
        "trading_day_count",
        "entry_raw_open",
        "entry_adj_factor",
        "end_adjusted_close",
        "stock_return_pct",
        "directional_return_pct",
        "mfe_pct",
        "mae_pct",
        "dataset_hashes",
        "csi300",
        "sw1",
        "terminal",
        "observation_hash",
    }
)
_BENCHMARK_FIELDS = frozenset(
    {
        "code",
        "name",
        "status",
        "reason",
        "reason_code",
        "return_pct",
        "stock_excess_return_pct",
        "directional_excess_return_pct",
    }
)
_FROZEN_COLUMNS = (
    "signal_id",
    "outcome_contract",
    "horizon",
    "engine_version",
    "final_action_family",
    "signal_created_at",
    "signal_session",
    "stock_code",
    "market",
    "source_type",
    "signal_action",
    "signal_horizon",
    "signal_status",
    "decision_profile",
    "research_snapshot_hash",
    "policy_version",
    "policy_hash",
    "policy_evaluation_hash",
    "portfolio_snapshot_ref",
    "prompt_version",
    "research_stance",
    "proposed_account_action",
    "final_account_action",
    "policy_mode",
    "policy_verdict",
    "policy_allowed",
    "would_block",
    "confidence",
    "signal_score",
    "value_quality_score",
    "trend_timing_score",
    "catalyst_score",
    "risk_score",
    "evidence_quality_score",
)
_MUTABLE_OBSERVATION_COLUMNS = (
    "eval_status",
    "outcome",
    "direction_correct",
    "reason_code",
    "execution_status",
    "entry_trade_date",
    "entry_raw_open",
    "entry_adj_factor",
    "end_trade_date",
    "trading_day_count",
    "end_adjusted_close",
    "stock_return_pct",
    "directional_return_pct",
    "mfe_pct",
    "mae_pct",
    "csi300_code",
    "csi300_name",
    "csi300_status",
    "csi300_reason_code",
    "csi300_return_pct",
    "csi300_stock_excess_return_pct",
    "csi300_directional_excess_return_pct",
    "sw1_code",
    "sw1_name",
    "sw1_status",
    "sw1_reason_code",
    "sw1_return_pct",
    "sw1_stock_excess_return_pct",
    "sw1_directional_excess_return_pct",
    "dataset_hashes_json",
    "observation_json",
    "observation_hash",
    "evaluated_at",
)
_BUSINESS_COLUMNS = _FROZEN_COLUMNS + _MUTABLE_OBSERVATION_COLUMNS


class DecisionOutcomeV2ConflictError(RuntimeError):
    """Raised when an immutable outcome identity is reused with different facts."""


@dataclass(frozen=True)
class DecisionOutcomeV2Candidate:
    signal: DecisionSignalRecord
    policy_evaluation: PortfolioPolicyEvaluationRecord
    horizon: str
    existing_outcome: Optional[DecisionOutcomeV2Record]


@dataclass(frozen=True)
class DecisionOutcomeV2WriteResult:
    row: DecisionOutcomeV2Record
    created: bool
    transitioned: bool
    disposition: str


def _mapping(value: Any, *, field: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_fields = getattr(value, "to_fields", None)
    if callable(to_fields):
        projected = to_fields()
        if isinstance(projected, Mapping):
            return dict(projected)
    raise TypeError(f"{field} must be a mapping or expose to_fields()")


def _bounded_text(
    value: Any,
    *,
    field: str,
    maximum: int,
    allow_none: bool = False,
) -> Optional[str]:
    if value is None and allow_none:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        if allow_none:
            return None
        raise ValueError(f"{field} must not be empty")
    if len(normalized) > maximum:
        raise ValueError(f"{field} must contain at most {maximum} characters")
    return normalized


def _sha256(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _optional_number(value: Any, *, field: str) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError(f"{field} must be a finite number or null")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError(f"{field} must be a finite number or null") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    return number


def _optional_date(value: Any, *, field: str) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO date or null") from exc
    raise TypeError(f"{field} must be a date or null")


def _optional_bool(value: Any, *, field: str) -> Optional[bool]:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise TypeError(f"{field} must be a boolean or null")
    return value


def _dataset_hashes(value: Any) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError("dataset_hashes must be an array")
    normalized = tuple(
        _sha256(item, field=f"dataset_hashes[{index}]")
        for index, item in enumerate(value)
    )
    if len(normalized) > 128:
        raise ValueError("dataset_hashes must contain at most 128 items")
    if normalized != tuple(sorted(set(normalized))):
        raise ValueError("dataset_hashes must be sorted and unique")
    return normalized


def _required_signal_number(value: Any, *, field: str) -> float:
    number = _optional_number(value, field=field)
    if number is None or not 0 <= number <= 100:
        raise ValueError(f"{field} must be within [0, 100]")
    return number


def _benchmark_fields(value: Any, *, prefix: str) -> dict[str, Any]:
    payload = _mapping(value, field=prefix)
    unexpected = sorted(set(payload).difference(_BENCHMARK_FIELDS))
    if unexpected:
        raise ValueError(f"{prefix} contains unsupported fields: {','.join(unexpected)}")
    reason = payload.get("reason_code", payload.get("reason"))
    if "reason" in payload and "reason_code" in payload:
        if payload["reason"] != payload["reason_code"]:
            raise ValueError(f"{prefix}.reason conflicts with reason_code")
    return {
        f"{prefix}_code": _bounded_text(
            payload.get("code"),
            field=f"{prefix}.code",
            maximum=32,
            allow_none=True,
        ),
        f"{prefix}_name": _bounded_text(
            payload.get("name"),
            field=f"{prefix}.name",
            maximum=128,
            allow_none=True,
        ),
        f"{prefix}_status": _bounded_text(
            payload.get("status"),
            field=f"{prefix}.status",
            maximum=16,
        ).lower(),
        f"{prefix}_reason_code": _bounded_text(
            reason,
            field=f"{prefix}.reason",
            maximum=128,
            allow_none=True,
        ),
        f"{prefix}_return_pct": _optional_number(
            payload.get("return_pct"), field=f"{prefix}.return_pct"
        ),
        f"{prefix}_stock_excess_return_pct": _optional_number(
            payload.get("stock_excess_return_pct"),
            field=f"{prefix}.stock_excess_return_pct",
        ),
        f"{prefix}_directional_excess_return_pct": _optional_number(
            payload.get("directional_excess_return_pct"),
            field=f"{prefix}.directional_excess_return_pct",
        ),
    }


def _execution_status(fields: Mapping[str, Any]) -> str:
    status = fields["eval_status"]
    if status == "pending":
        return "pending"
    if status in {"evaluated", "observational"}:
        return "executable"
    if status == "unexecutable":
        return "unexecutable"
    if fields.get("entry_raw_open") is not None and fields.get(
        "entry_adj_factor"
    ) is not None:
        return "executable"
    return "unavailable"


def _validate_benchmark_state(
    fields: Mapping[str, Any],
    *,
    prefix: str,
) -> None:
    status = fields[f"{prefix}_status"]
    reason = fields[f"{prefix}_reason_code"]
    result_values = (
        fields[f"{prefix}_return_pct"],
        fields[f"{prefix}_stock_excess_return_pct"],
        fields[f"{prefix}_directional_excess_return_pct"],
    )
    if status == "unavailable":
        if reason is None or any(value is not None for value in result_values):
            raise ValueError(
                f"{prefix} unavailable requires reason and null result metrics"
            )
        return
    if status != "available":
        raise ValueError(f"{prefix}.status must be available or unavailable")
    if fields[f"{prefix}_code"] is None or reason is not None:
        raise ValueError(f"{prefix} available requires code and no reason")
    if result_values[0] is None or result_values[1] is None:
        raise ValueError(f"{prefix} available requires return and stock excess")
    expected_stock_excess = fields["stock_return_pct"] - result_values[0]
    if not math.isclose(
        result_values[1],
        expected_stock_excess,
        rel_tol=0.0,
        abs_tol=1e-8,
    ):
        raise ValueError(f"{prefix} stock excess is inconsistent")
    directional = result_values[2]
    if fields["eval_status"] == "evaluated":
        if directional is None:
            raise ValueError(
                f"{prefix} evaluated benchmark requires directional excess"
            )
        multiplier = 1.0 if fields["final_action_family"] == "long" else -1.0
        expected_directional_excess = (
            fields["directional_return_pct"] - (result_values[0] * multiplier)
        )
        if not math.isclose(
            directional,
            expected_directional_excess,
            rel_tol=0.0,
            abs_tol=1e-8,
        ):
            raise ValueError(f"{prefix} directional excess is inconsistent")
    elif directional is not None:
        raise ValueError(
            f"{prefix} non-evaluated benchmark forbids directional excess"
        )


def _validate_evaluation_state(fields: Mapping[str, Any]) -> None:
    expected_days = int(str(fields["horizon"])[:-1])
    trading_days = fields["trading_day_count"]
    if trading_days is not None and trading_days != expected_days:
        raise ValueError("trading_day_count must match horizon")
    if (
        fields["signal_session"] is not None
        and fields["entry_trade_date"] is not None
        and fields["entry_trade_date"] <= fields["signal_session"]
    ):
        raise ValueError("entry_trade_date must be after signal_session")
    if (
        fields["entry_trade_date"] is not None
        and fields["end_trade_date"] is not None
        and fields["end_trade_date"] < fields["entry_trade_date"]
    ):
        raise ValueError("end_trade_date must not precede entry_trade_date")
    for field in ("entry_raw_open", "entry_adj_factor", "end_adjusted_close"):
        if fields[field] is not None and fields[field] <= 0:
            raise ValueError(f"{field} must be positive")
    for field in ("mfe_pct", "mae_pct"):
        if fields[field] is not None and fields[field] < 0:
            raise ValueError(f"{field} must be non-negative")

    status = fields["eval_status"]
    result_fields = (
        "end_adjusted_close",
        "stock_return_pct",
        "directional_return_pct",
        "mfe_pct",
        "mae_pct",
    )
    if status == "pending":
        if fields["reason_code"] is None:
            raise ValueError("pending requires reason_code")
        if fields["outcome"] is not None or fields["direction_correct"] is not None:
            raise ValueError("pending forbids outcome and direction_correct")
        if any(fields[field] is not None for field in result_fields):
            raise ValueError("pending forbids result metrics")
    elif status == "evaluated":
        if fields["final_action_family"] not in {"long", "defensive"}:
            raise ValueError("evaluated requires a directional action family")
        expected_direction = {"hit": True, "miss": False}.get(fields["outcome"])
        if expected_direction is None or fields["direction_correct"] is not expected_direction:
            raise ValueError("evaluated requires hit/true or miss/false")
        required = (
            "signal_session",
            "entry_trade_date",
            "entry_raw_open",
            "entry_adj_factor",
            "end_trade_date",
            "trading_day_count",
            *result_fields,
        )
        if fields["reason_code"] is not None or any(
            fields[field] is None for field in required
        ):
            raise ValueError("evaluated has incomplete or inconsistent metrics")
        expected_stock_return = (
            fields["end_adjusted_close"] / fields["entry_raw_open"] - 1.0
        ) * 100.0
        if not math.isclose(
            fields["stock_return_pct"],
            expected_stock_return,
            rel_tol=0.0,
            abs_tol=1e-8,
        ):
            raise ValueError("stock_return_pct is inconsistent with prices")
        expected_directional_return = (
            fields["stock_return_pct"]
            if fields["final_action_family"] == "long"
            else -fields["stock_return_pct"]
        )
        if not math.isclose(
            fields["directional_return_pct"],
            expected_directional_return,
            rel_tol=0.0,
            abs_tol=1e-8,
        ):
            raise ValueError("directional_return_pct is inconsistent")
        expected_outcome = (
            "hit" if fields["directional_return_pct"] > 0 else "miss"
        )
        if fields["outcome"] != expected_outcome:
            raise ValueError("outcome is inconsistent with directional return")
        if not fields["dataset_hashes"]:
            raise ValueError("evaluated requires dataset_hashes")
    elif status == "observational":
        required = (
            "signal_session",
            "entry_trade_date",
            "entry_raw_open",
            "entry_adj_factor",
            "end_trade_date",
            "trading_day_count",
            "end_adjusted_close",
            "stock_return_pct",
        )
        if fields["final_action_family"] != "observational":
            raise ValueError("observational requires observational action family")
        if fields["reason_code"] is not None or any(
            fields[field] is None for field in required
        ):
            raise ValueError("observational has incomplete metrics")
        forbidden = (
            "outcome",
            "direction_correct",
            "directional_return_pct",
            "mfe_pct",
            "mae_pct",
        )
        if any(fields[field] is not None for field in forbidden):
            raise ValueError("observational forbids directional result metrics")
        expected_stock_return = (
            fields["end_adjusted_close"] / fields["entry_raw_open"] - 1.0
        ) * 100.0
        if not math.isclose(
            fields["stock_return_pct"],
            expected_stock_return,
            rel_tol=0.0,
            abs_tol=1e-8,
        ):
            raise ValueError("stock_return_pct is inconsistent with prices")
        if not fields["dataset_hashes"]:
            raise ValueError("observational requires dataset_hashes")
    elif status == "unexecutable":
        if fields["final_action_family"] not in {"long", "defensive"}:
            raise ValueError("unexecutable requires a directional action family")
        if fields["reason_code"] is None or not fields["dataset_hashes"]:
            raise ValueError("unexecutable requires reason and dataset_hashes")
        if fields["reason_code"] not in _UNEXECUTABLE_REASONS:
            raise ValueError("unexecutable reason_code is invalid")
        if any(
            fields[field] is None
            for field in (
                "signal_session",
                "entry_trade_date",
                "end_trade_date",
                "trading_day_count",
            )
        ):
            raise ValueError("unexecutable requires the T+1 window dates")
        if fields["outcome"] is not None or fields["direction_correct"] is not None:
            raise ValueError("unexecutable forbids outcome facts")
        if any(fields[field] is not None for field in result_fields):
            raise ValueError("unexecutable forbids result metrics")
    else:
        if fields["reason_code"] is None:
            raise ValueError("unable requires reason_code")
        if fields["outcome"] is not None or fields["direction_correct"] is not None:
            raise ValueError("unable forbids outcome facts")
        if any(fields[field] is not None for field in result_fields):
            raise ValueError("unable forbids result metrics")

    _validate_benchmark_state(fields, prefix="csi300")
    _validate_benchmark_state(fields, prefix="sw1")


def _normalize_evaluation(value: Any) -> dict[str, Any]:
    payload = _mapping(value, field="evaluation")
    unexpected = sorted(set(payload).difference(_EVALUATION_FIELDS))
    if unexpected:
        raise ValueError(
            "evaluation contains unsupported fields: " + ",".join(unexpected)
        )
    if "reason" in payload and "reason_code" in payload:
        if payload["reason"] != payload["reason_code"]:
            raise ValueError("evaluation reason conflicts with reason_code")
    horizon = _bounded_text(
        payload.get("horizon"), field="horizon", maximum=16
    ).lower()
    if horizon not in DECISION_OUTCOME_V2_HORIZONS:
        raise ValueError("horizon must be 5d, 10d, or 20d")
    engine_version = _bounded_text(
        payload.get("engine_version"), field="engine_version", maximum=64
    )
    if _VERSION_RE.fullmatch(engine_version) is None:
        raise ValueError("engine_version is not a stable version identifier")
    family = _bounded_text(
        payload.get("final_action_family"),
        field="final_action_family",
        maximum=16,
    ).lower()
    if family not in {"long", "defensive", "observational"}:
        raise ValueError("final_action_family is invalid")
    status = _bounded_text(
        payload.get("eval_status"), field="eval_status", maximum=24
    ).lower()
    if status not in _EVAL_STATUSES:
        raise ValueError("eval_status is invalid")
    terminal = payload.get("terminal")
    if terminal is not None:
        if not isinstance(terminal, bool):
            raise TypeError("terminal must be a boolean")
        if terminal != (status in DECISION_OUTCOME_V2_TERMINAL_STATUSES):
            raise ValueError("terminal conflicts with eval_status")

    trading_day_count = payload.get("trading_day_count")
    if trading_day_count is not None:
        if isinstance(trading_day_count, bool) or not isinstance(
            trading_day_count, int
        ):
            raise TypeError("trading_day_count must be an integer or null")

    normalized = {
        "horizon": horizon,
        "engine_version": engine_version,
        "final_action_family": family,
        "eval_status": status,
        "outcome": _bounded_text(
            payload.get("outcome"),
            field="outcome",
            maximum=16,
            allow_none=True,
        ),
        "direction_correct": _optional_bool(
            payload.get("direction_correct"), field="direction_correct"
        ),
        "reason_code": _bounded_text(
            payload.get("reason_code", payload.get("reason")),
            field="reason_code",
            maximum=128,
            allow_none=True,
        ),
        "signal_session": _optional_date(
            payload.get("signal_session"), field="signal_session"
        ),
        "entry_trade_date": _optional_date(
            payload.get("entry_trade_date"), field="entry_trade_date"
        ),
        "end_trade_date": _optional_date(
            payload.get("end_trade_date"), field="end_trade_date"
        ),
        "trading_day_count": trading_day_count,
        "entry_raw_open": _optional_number(
            payload.get("entry_raw_open"), field="entry_raw_open"
        ),
        "entry_adj_factor": _optional_number(
            payload.get("entry_adj_factor"), field="entry_adj_factor"
        ),
        "end_adjusted_close": _optional_number(
            payload.get("end_adjusted_close"), field="end_adjusted_close"
        ),
        "stock_return_pct": _optional_number(
            payload.get("stock_return_pct"), field="stock_return_pct"
        ),
        "directional_return_pct": _optional_number(
            payload.get("directional_return_pct"),
            field="directional_return_pct",
        ),
        "mfe_pct": _optional_number(payload.get("mfe_pct"), field="mfe_pct"),
        "mae_pct": _optional_number(payload.get("mae_pct"), field="mae_pct"),
        "dataset_hashes": _dataset_hashes(payload.get("dataset_hashes", ())),
    }
    normalized.update(_benchmark_fields(payload.get("csi300"), prefix="csi300"))
    normalized.update(_benchmark_fields(payload.get("sw1"), prefix="sw1"))
    if normalized["outcome"] is not None:
        normalized["outcome"] = normalized["outcome"].lower()
    if normalized["csi300_code"] != "000300.SH":
        raise ValueError("csi300.code must be 000300.SH")
    _validate_evaluation_state(normalized)

    observation_payload = {
        "contract": DECISION_OUTCOME_V2_CONTRACT,
        **normalized,
    }
    observation_hash = canonical_hash(
        observation_payload,
        exclude_volatile=False,
    )
    supplied_hash = payload.get("observation_hash")
    if supplied_hash is not None and _sha256(
        supplied_hash, field="observation_hash"
    ) != observation_hash:
        raise ValueError("observation_hash does not match canonical evaluation")
    normalized["dataset_hashes_json"] = canonical_json(
        normalized.pop("dataset_hashes"),
        exclude_volatile=False,
    )
    normalized["observation_json"] = canonical_json(
        observation_payload,
        exclude_volatile=False,
    )
    normalized["observation_hash"] = observation_hash
    normalized["execution_status"] = _execution_status(normalized)
    return normalized


def _assert_equal(left: Any, right: Any, *, field: str) -> None:
    if left != right:
        raise ValueError(f"DecisionSignal and policy lineage disagree on {field}")


def _frozen_fields(
    signal: DecisionSignalRecord,
    policy: PortfolioPolicyEvaluationRecord,
    evaluation: Mapping[str, Any],
) -> dict[str, Any]:
    required_signal_text = {
        "source_type": signal.source_type,
        "signal_action": signal.action,
        "signal_status": signal.status,
        "decision_profile": signal.decision_profile,
        "research_snapshot_hash": signal.research_snapshot_hash,
        "policy_version": signal.policy_version,
        "policy_hash": signal.policy_hash,
        "policy_evaluation_hash": signal.policy_evaluation_hash,
        "portfolio_snapshot_ref": signal.portfolio_snapshot_ref,
        "research_stance": signal.research_stance,
        "final_account_action": signal.account_action,
        "policy_mode": signal.policy_mode,
        "policy_verdict": signal.policy_decision,
    }
    normalized_text = {
        name: _bounded_text(value, field=name, maximum=128)
        for name, value in required_signal_text.items()
    }
    if signal.created_at is None:
        raise ValueError("signal_created_at is required")
    if not isinstance(signal.would_block, bool):
        raise ValueError("signal would_block must be a boolean")
    confidence = _optional_number(signal.confidence, field="confidence")
    if confidence is not None and not 0 <= confidence <= 1:
        raise ValueError("confidence must be within [0, 1]")
    signal_score = _optional_number(signal.score, field="signal_score")
    if signal_score is not None and not 0 <= signal_score <= 100:
        raise ValueError("signal_score must be within [0, 100]")
    scores = {
        "value_quality_score": _required_signal_number(
            signal.value_quality_score, field="value_quality_score"
        ),
        "trend_timing_score": _required_signal_number(
            signal.trend_timing_score, field="trend_timing_score"
        ),
        "catalyst_score": _required_signal_number(
            signal.catalyst_score, field="catalyst_score"
        ),
        "risk_score": _required_signal_number(
            signal.risk_score, field="risk_score"
        ),
        "evidence_quality_score": _required_signal_number(
            signal.evidence_quality_score, field="evidence_quality_score"
        ),
    }
    if policy.signal_id != signal.id:
        raise ValueError("policy evaluation must bind the DecisionSignal")
    _assert_equal(policy.evaluation_hash, signal.policy_evaluation_hash, field="evaluation_hash")
    for field in (
        "stock_code",
        "policy_version",
        "policy_hash",
        "research_snapshot_hash",
        "portfolio_snapshot_ref",
        "research_stance",
        "final_account_action",
        "would_block",
    ):
        policy_field = "final_account_action" if field == "final_account_action" else field
        signal_field = "account_action" if field == "final_account_action" else field
        _assert_equal(
            getattr(policy, policy_field),
            getattr(signal, signal_field),
            field=field,
        )
    _assert_equal(str(policy.market).lower(), str(signal.market).lower(), field="market")
    _assert_equal(policy.mode, signal.policy_mode, field="policy_mode")
    _assert_equal(policy.verdict, signal.policy_decision, field="policy_verdict")
    if bool(policy.allowed) == bool(policy.would_block):
        raise ValueError("policy allowed and would_block must be opposites")
    expected_family = _ACTION_FAMILY_BY_ACCOUNT_ACTION.get(
        normalized_text["final_account_action"]
    )
    if expected_family is None:
        raise ValueError("final_account_action is unsupported by Outcome v2")
    if evaluation["final_action_family"] != expected_family:
        raise ValueError("final_action_family conflicts with final_account_action")

    return {
        "signal_id": int(signal.id),
        "outcome_contract": DECISION_OUTCOME_V2_CONTRACT,
        "horizon": evaluation["horizon"],
        "engine_version": evaluation["engine_version"],
        "final_action_family": expected_family,
        "signal_created_at": signal.created_at,
        "signal_session": evaluation["signal_session"],
        "stock_code": _bounded_text(
            signal.stock_code, field="stock_code", maximum=16
        ),
        "market": _bounded_text(signal.market, field="market", maximum=8),
        "source_type": normalized_text["source_type"],
        "signal_action": normalized_text["signal_action"],
        "signal_horizon": _bounded_text(
            signal.horizon,
            field="signal_horizon",
            maximum=16,
            allow_none=True,
        ),
        "signal_status": normalized_text["signal_status"],
        "decision_profile": normalized_text["decision_profile"],
        "research_snapshot_hash": _sha256(
            normalized_text["research_snapshot_hash"],
            field="research_snapshot_hash",
        ),
        "policy_version": normalized_text["policy_version"],
        "policy_hash": _sha256(
            normalized_text["policy_hash"], field="policy_hash"
        ),
        "policy_evaluation_hash": _sha256(
            normalized_text["policy_evaluation_hash"],
            field="policy_evaluation_hash",
        ),
        "portfolio_snapshot_ref": normalized_text["portfolio_snapshot_ref"],
        "prompt_version": _bounded_text(
            signal.prompt_version,
            field="prompt_version",
            maximum=64,
            allow_none=True,
        ),
        "research_stance": normalized_text["research_stance"],
        "proposed_account_action": _bounded_text(
            policy.proposed_account_action,
            field="proposed_account_action",
            maximum=24,
        ),
        "final_account_action": normalized_text["final_account_action"],
        "policy_mode": normalized_text["policy_mode"],
        "policy_verdict": normalized_text["policy_verdict"],
        "policy_allowed": bool(policy.allowed),
        "would_block": bool(signal.would_block),
        "confidence": confidence,
        "signal_score": signal_score,
        **scores,
    }


def _detach(session: Any, row: DecisionOutcomeV2Record) -> DecisionOutcomeV2Record:
    session.flush()
    session.expunge(row)
    return row


class DecisionOutcomeV2Repository:
    """Fairly schedule candidates and persist one immutable v2 identity."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def list_candidate_keys(
        self,
        *,
        horizons: Sequence[str],
        engine_version: str,
        limit: int,
        signal_id: Optional[int] = None,
        stock_code: Optional[str] = None,
        decision_profile: Optional[str] = None,
    ) -> list[DecisionOutcomeV2Candidate]:
        """Return a globally fair missing-or-pending key set."""

        normalized_horizons = tuple(str(item).strip().lower() for item in horizons)
        if not normalized_horizons or len(normalized_horizons) != len(
            set(normalized_horizons)
        ):
            raise ValueError("horizons must be a non-empty unique sequence")
        if any(item not in DECISION_OUTCOME_V2_HORIZONS for item in normalized_horizons):
            raise ValueError("horizons must contain only 5d, 10d, and 20d")
        engine = _bounded_text(
            engine_version, field="engine_version", maximum=64
        )
        if _VERSION_RE.fullmatch(engine) is None:
            raise ValueError("engine_version is not a stable version identifier")
        if isinstance(limit, bool) or int(limit) < 1:
            raise ValueError("limit must be a positive integer")
        row_limit = int(limit)

        candidates: list[DecisionOutcomeV2Candidate] = []
        with self.db.get_session() as session:
            for horizon in normalized_horizons:
                outcome_join = and_(
                    DecisionOutcomeV2Record.signal_id == DecisionSignalRecord.id,
                    DecisionOutcomeV2Record.horizon == horizon,
                    DecisionOutcomeV2Record.engine_version == engine,
                )
                policy_join = and_(
                    PortfolioPolicyEvaluationRecord.signal_id
                    == DecisionSignalRecord.id,
                    PortfolioPolicyEvaluationRecord.evaluation_hash
                    == DecisionSignalRecord.policy_evaluation_hash,
                    PortfolioPolicyEvaluationRecord.stock_code
                    == DecisionSignalRecord.stock_code,
                    func.lower(PortfolioPolicyEvaluationRecord.market)
                    == func.lower(DecisionSignalRecord.market),
                    PortfolioPolicyEvaluationRecord.policy_version
                    == DecisionSignalRecord.policy_version,
                    PortfolioPolicyEvaluationRecord.policy_hash
                    == DecisionSignalRecord.policy_hash,
                    PortfolioPolicyEvaluationRecord.research_snapshot_hash
                    == DecisionSignalRecord.research_snapshot_hash,
                    PortfolioPolicyEvaluationRecord.portfolio_snapshot_ref
                    == DecisionSignalRecord.portfolio_snapshot_ref,
                    PortfolioPolicyEvaluationRecord.research_stance
                    == DecisionSignalRecord.research_stance,
                    PortfolioPolicyEvaluationRecord.final_account_action
                    == DecisionSignalRecord.account_action,
                    PortfolioPolicyEvaluationRecord.mode
                    == DecisionSignalRecord.policy_mode,
                    PortfolioPolicyEvaluationRecord.verdict
                    == DecisionSignalRecord.policy_decision,
                    PortfolioPolicyEvaluationRecord.would_block
                    == DecisionSignalRecord.would_block,
                )
                conditions = [
                    DecisionOutcomeV2Record.id.is_(None)
                    | (DecisionOutcomeV2Record.eval_status == "pending"),
                    func.lower(DecisionSignalRecord.market).in_(("cn", "a")),
                    DecisionSignalRecord.created_at.is_not(None),
                    DecisionSignalRecord.decision_profile.is_not(None),
                    DecisionSignalRecord.research_snapshot_hash.is_not(None),
                    DecisionSignalRecord.policy_version.is_not(None),
                    DecisionSignalRecord.policy_hash.is_not(None),
                    DecisionSignalRecord.policy_evaluation_hash.is_not(None),
                    DecisionSignalRecord.portfolio_snapshot_ref.is_not(None),
                    DecisionSignalRecord.research_stance.is_not(None),
                    DecisionSignalRecord.account_action.is_not(None),
                    DecisionSignalRecord.policy_mode.is_not(None),
                    DecisionSignalRecord.policy_decision.is_not(None),
                    DecisionSignalRecord.value_quality_score.is_not(None),
                    DecisionSignalRecord.trend_timing_score.is_not(None),
                    DecisionSignalRecord.catalyst_score.is_not(None),
                    DecisionSignalRecord.risk_score.is_not(None),
                    DecisionSignalRecord.evidence_quality_score.is_not(None),
                ]
                if signal_id is not None:
                    conditions.append(DecisionSignalRecord.id == int(signal_id))
                if stock_code is not None:
                    conditions.append(DecisionSignalRecord.stock_code == stock_code)
                if decision_profile is not None:
                    conditions.append(
                        DecisionSignalRecord.decision_profile == decision_profile
                    )
                rows = session.execute(
                    select(
                        DecisionSignalRecord,
                        PortfolioPolicyEvaluationRecord,
                        DecisionOutcomeV2Record,
                    )
                    .join(PortfolioPolicyEvaluationRecord, policy_join)
                    .outerjoin(DecisionOutcomeV2Record, outcome_join)
                    .where(and_(*conditions))
                    .order_by(
                        func.coalesce(
                            DecisionOutcomeV2Record.updated_at,
                            DecisionSignalRecord.created_at,
                        ),
                        DecisionSignalRecord.id,
                    )
                    .limit(row_limit)
                ).all()
                candidates.extend(
                    DecisionOutcomeV2Candidate(
                        signal=signal,
                        policy_evaluation=policy,
                        horizon=horizon,
                        existing_outcome=outcome,
                    )
                    for signal, policy, outcome in rows
                )

        horizon_rank = {
            horizon: index for index, horizon in enumerate(normalized_horizons)
        }
        candidates.sort(
            key=lambda item: (
                (
                    item.existing_outcome.updated_at
                    if item.existing_outcome is not None
                    else item.signal.created_at
                )
                or datetime.min,
                int(item.signal.id),
                horizon_rank[item.horizon],
                item.existing_outcome is not None,
            )
        )
        return candidates[:row_limit]

    def persist_evaluation(
        self,
        *,
        signal_id: int,
        evaluation: Any,
    ) -> DecisionOutcomeV2WriteResult:
        """Create, refresh pending, or idempotently replay; never rewrite terminal."""

        normalized_evaluation = _normalize_evaluation(evaluation)
        terminal = (
            normalized_evaluation["eval_status"]
            in DECISION_OUTCOME_V2_TERMINAL_STATUSES
        )

        def _write(session: Any) -> DecisionOutcomeV2WriteResult:
            signal = session.execute(
                select(DecisionSignalRecord)
                .where(DecisionSignalRecord.id == int(signal_id))
                .limit(1)
            ).scalar_one_or_none()
            if signal is None:
                raise ValueError("DecisionSignal does not exist")
            policy = session.execute(
                select(PortfolioPolicyEvaluationRecord)
                .where(
                    PortfolioPolicyEvaluationRecord.signal_id == int(signal_id),
                    PortfolioPolicyEvaluationRecord.evaluation_hash
                    == signal.policy_evaluation_hash,
                )
                .limit(1)
            ).scalar_one_or_none()
            if policy is None:
                raise ValueError("DecisionSignal policy evaluation does not exist")

            fields = {
                **_frozen_fields(signal, policy, normalized_evaluation),
                **{
                    name: normalized_evaluation[name]
                    for name in _MUTABLE_OBSERVATION_COLUMNS
                    if name != "evaluated_at"
                },
                "evaluated_at": utc_naive_now() if terminal else None,
            }
            existing = session.execute(
                select(DecisionOutcomeV2Record)
                .where(
                    DecisionOutcomeV2Record.signal_id == int(signal_id),
                    DecisionOutcomeV2Record.horizon == fields["horizon"],
                    DecisionOutcomeV2Record.engine_version
                    == fields["engine_version"],
                )
                .limit(1)
            ).scalar_one_or_none()
            if existing is None:
                row = DecisionOutcomeV2Record(**fields)
                session.add(row)
                _detach(session, row)
                return DecisionOutcomeV2WriteResult(
                    row=row,
                    created=True,
                    transitioned=False,
                    disposition="created",
                )

            mismatched_frozen = [
                name
                for name in _FROZEN_COLUMNS
                if getattr(existing, name) != fields[name]
            ]
            if mismatched_frozen:
                raise DecisionOutcomeV2ConflictError(
                    "outcome identity conflicts with frozen fields: "
                    + ",".join(sorted(mismatched_frozen))
                )
            mismatched_business = [
                name
                for name in _BUSINESS_COLUMNS
                if name != "evaluated_at"
                and getattr(existing, name) != fields[name]
            ]
            if existing.eval_status in DECISION_OUTCOME_V2_TERMINAL_STATUSES:
                if mismatched_business:
                    raise DecisionOutcomeV2ConflictError(
                        "terminal outcome conflicts with immutable fields: "
                        + ",".join(sorted(mismatched_business))
                    )
                _detach(session, existing)
                return DecisionOutcomeV2WriteResult(
                    row=existing,
                    created=False,
                    transitioned=False,
                    disposition="unchanged",
                )
            if not mismatched_business:
                _detach(session, existing)
                return DecisionOutcomeV2WriteResult(
                    row=existing,
                    created=False,
                    transitioned=False,
                    disposition="unchanged",
                )

            was_pending = existing.eval_status == "pending"
            for name in _MUTABLE_OBSERVATION_COLUMNS:
                setattr(existing, name, fields[name])
            existing.updated_at = utc_naive_now()
            _detach(session, existing)
            return DecisionOutcomeV2WriteResult(
                row=existing,
                created=False,
                transitioned=was_pending and terminal,
                disposition="transitioned" if terminal else "updated",
            )

        return self.db._run_write_transaction(
            "persist Decision Outcome v2",
            _write,
        )

    def get_outcome(
        self,
        *,
        signal_id: int,
        horizon: str,
        engine_version: str,
    ) -> Optional[DecisionOutcomeV2Record]:
        with self.db.get_session() as session:
            row = session.execute(
                select(DecisionOutcomeV2Record)
                .where(
                    DecisionOutcomeV2Record.signal_id == int(signal_id),
                    DecisionOutcomeV2Record.horizon == horizon,
                    DecisionOutcomeV2Record.engine_version == engine_version,
                )
                .limit(1)
            ).scalar_one_or_none()
            if row is not None:
                session.expunge(row)
            return row

    def list_for_signal(self, signal_id: int) -> list[DecisionOutcomeV2Record]:
        with self.db.get_session() as session:
            rows = session.execute(
                select(DecisionOutcomeV2Record)
                .where(DecisionOutcomeV2Record.signal_id == int(signal_id))
                .order_by(
                    DecisionOutcomeV2Record.engine_version,
                    DecisionOutcomeV2Record.horizon,
                )
            ).scalars().all()
            for row in rows:
                session.expunge(row)
            return list(rows)


__all__ = [
    "DECISION_OUTCOME_V2_CONTRACT",
    "DECISION_OUTCOME_V2_HORIZONS",
    "DECISION_OUTCOME_V2_TERMINAL_STATUSES",
    "DecisionOutcomeV2Candidate",
    "DecisionOutcomeV2ConflictError",
    "DecisionOutcomeV2Repository",
    "DecisionOutcomeV2WriteResult",
]
