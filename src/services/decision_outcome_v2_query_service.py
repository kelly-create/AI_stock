"""Read-only query and calibration service for Decision Outcome v2."""

from __future__ import annotations

from datetime import date, datetime
import json
import re
from typing import Any, Optional, Sequence

from sqlalchemy import func, select

from data_provider.base import normalize_stock_code
from src.core.decision_outcome_v2_evaluator import (
    DECISION_OUTCOME_V2_ENGINE_VERSION,
    SUPPORTED_DECISION_OUTCOME_V2_HORIZONS,
)
from src.services.decision_outcome_v2_stats import (
    DecisionOutcomeV2CalibrationSample,
    DecisionOutcomeV2Stats,
)
from src.services.research.canonical import canonical_hash
from src.storage import DatabaseManager, DecisionOutcomeV2Record, DecisionSignalRecord


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STATUSES = frozenset(
    {"pending", "evaluated", "observational", "unexecutable", "unable"}
)
_ACTION_FAMILIES = frozenset({"long", "defensive", "observational"})


class DecisionOutcomeV2NotFoundError(ValueError):
    """Raised when a requested DecisionSignal does not exist."""


class DecisionOutcomeV2ContractError(RuntimeError):
    """Raised when immutable persisted content fails its read contract."""


def _optional_text(value: Any, *, field: str, maximum: int) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text or len(text) > maximum:
        raise ValueError(f"{field} must contain 1-{maximum} characters")
    return text


def _normalize_horizons(values: Optional[Sequence[str]]) -> tuple[str, ...]:
    if values is None:
        return tuple(SUPPORTED_DECISION_OUTCOME_V2_HORIZONS)
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError("horizons must be an array")
    normalized = tuple(str(item).strip().lower() for item in values)
    if not normalized or len(normalized) != len(set(normalized)):
        raise ValueError("horizons must be non-empty and unique")
    if any(item not in SUPPORTED_DECISION_OUTCOME_V2_HORIZONS for item in normalized):
        raise ValueError("horizons must contain only 5d, 10d, and 20d")
    return normalized


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise DecisionOutcomeV2ContractError(
        f"persisted temporal value has invalid type: {type(value).__name__}"
    )


def _json_object(value: Any, *, field: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DecisionOutcomeV2ContractError(
            f"persisted {field} is invalid JSON"
        ) from exc
    if not isinstance(parsed, dict):
        raise DecisionOutcomeV2ContractError(f"persisted {field} must be an object")
    return parsed


def _dataset_hashes(value: Any) -> list[str]:
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DecisionOutcomeV2ContractError(
            "persisted dataset_hashes_json is invalid JSON"
        ) from exc
    if not isinstance(parsed, list) or any(
        not isinstance(item, str) or _SHA256_RE.fullmatch(item) is None
        for item in parsed
    ):
        raise DecisionOutcomeV2ContractError(
            "persisted dataset_hashes_json must be a SHA-256 array"
        )
    if parsed != sorted(set(parsed)):
        raise DecisionOutcomeV2ContractError(
            "persisted dataset_hashes_json must be sorted and unique"
        )
    return parsed


class DecisionOutcomeV2QueryService:
    """Read v2 rows without consulting mutable signal or policy state."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def list_outcomes(
        self,
        *,
        signal_id: Optional[int] = None,
        horizon: Optional[str] = None,
        engine_version: Optional[str] = None,
        eval_status: Optional[str] = None,
        final_action_family: Optional[str] = None,
        decision_profile: Optional[str] = None,
        stock_code: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        signal_id_norm = self._optional_positive_int(signal_id, "signal_id")
        horizon_norm = self._optional_enum(
            horizon,
            frozenset(SUPPORTED_DECISION_OUTCOME_V2_HORIZONS),
            "horizon",
        )
        engine = _optional_text(
            engine_version or DECISION_OUTCOME_V2_ENGINE_VERSION,
            field="engine_version",
            maximum=64,
        )
        status_norm = self._optional_enum(eval_status, _STATUSES, "eval_status")
        family_norm = self._optional_enum(
            final_action_family,
            _ACTION_FAMILIES,
            "final_action_family",
        )
        profile_norm = _optional_text(
            decision_profile,
            field="decision_profile",
            maximum=16,
        )
        stock_norm = (
            normalize_stock_code(stock_code) if stock_code not in (None, "") else None
        )
        safe_page = max(1, int(page))
        safe_page_size = max(1, min(int(page_size), 100))

        conditions = [DecisionOutcomeV2Record.engine_version == engine]
        optional_conditions = (
            (DecisionOutcomeV2Record.signal_id, signal_id_norm),
            (DecisionOutcomeV2Record.horizon, horizon_norm),
            (DecisionOutcomeV2Record.eval_status, status_norm),
            (DecisionOutcomeV2Record.final_action_family, family_norm),
            (DecisionOutcomeV2Record.decision_profile, profile_norm),
            (DecisionOutcomeV2Record.stock_code, stock_norm),
        )
        conditions.extend(column == value for column, value in optional_conditions if value is not None)
        offset = (safe_page - 1) * safe_page_size
        with self.db.get_session() as session:
            total = int(
                session.execute(
                    select(func.count(DecisionOutcomeV2Record.id)).where(*conditions)
                ).scalar_one()
            )
            rows = session.execute(
                select(DecisionOutcomeV2Record)
                .where(*conditions)
                .order_by(
                    DecisionOutcomeV2Record.signal_created_at.desc(),
                    DecisionOutcomeV2Record.signal_id.desc(),
                    DecisionOutcomeV2Record.horizon,
                )
                .offset(offset)
                .limit(safe_page_size)
            ).scalars().all()
            for row in rows:
                session.expunge(row)
        return {
            "contract": "decision-outcome-v2-collection",
            "version": "v1",
            "items": [self.serialize(row) for row in rows],
            "total": total,
            "page": safe_page,
            "page_size": safe_page_size,
        }

    def list_for_signal(
        self,
        signal_id: int,
        *,
        engine_version: Optional[str] = None,
    ) -> dict[str, Any]:
        signal_id_norm = self._optional_positive_int(signal_id, "signal_id")
        with self.db.get_session() as session:
            exists = session.execute(
                select(DecisionSignalRecord.id)
                .where(DecisionSignalRecord.id == signal_id_norm)
                .limit(1)
            ).scalar_one_or_none()
        if exists is None:
            raise DecisionOutcomeV2NotFoundError(
                f"Decision signal not found: {signal_id_norm}"
            )
        return self.list_outcomes(
            signal_id=signal_id_norm,
            engine_version=engine_version,
            page=1,
            page_size=100,
        )

    def get_stats(
        self,
        *,
        horizons: Optional[Sequence[str]] = None,
        engine_version: Optional[str] = None,
        decision_profile: Optional[str] = None,
        final_action_family: Optional[str] = None,
    ) -> dict[str, Any]:
        horizons_norm = _normalize_horizons(horizons)
        engine = _optional_text(
            engine_version or DECISION_OUTCOME_V2_ENGINE_VERSION,
            field="engine_version",
            maximum=64,
        )
        profile_norm = _optional_text(
            decision_profile,
            field="decision_profile",
            maximum=16,
        )
        family_norm = self._optional_enum(
            final_action_family,
            _ACTION_FAMILIES,
            "final_action_family",
        )
        conditions = [
            DecisionOutcomeV2Record.engine_version == engine,
            DecisionOutcomeV2Record.horizon.in_(horizons_norm),
        ]
        if profile_norm is not None:
            conditions.append(DecisionOutcomeV2Record.decision_profile == profile_norm)
        if family_norm is not None:
            conditions.append(
                DecisionOutcomeV2Record.final_action_family == family_norm
            )
        with self.db.get_session() as session:
            rows = session.execute(
                select(DecisionOutcomeV2Record).where(*conditions)
            ).scalars().all()

        aggregate = DecisionOutcomeV2Stats.aggregate(
            DecisionOutcomeV2CalibrationSample(
                engine_version=row.engine_version,
                horizon=row.horizon,
                profile=row.decision_profile,
                final_action_family=row.final_action_family,
                eval_status=row.eval_status,
                confidence=row.confidence,
                direction_correct=row.direction_correct,
                csi300_directional_excess_return_pct=(
                    row.csi300_directional_excess_return_pct
                ),
                sw1_directional_excess_return_pct=(
                    row.sw1_directional_excess_return_pct
                ),
            )
            for row in rows
        )
        return {
            "contract": "decision-outcome-v2-stats",
            "version": "v1",
            "engine_version": engine,
            "horizons": list(horizons_norm),
            **aggregate,
        }

    @staticmethod
    def serialize(row: DecisionOutcomeV2Record) -> dict[str, Any]:
        dataset_hashes = _dataset_hashes(row.dataset_hashes_json)
        observation = _json_object(row.observation_json, field="observation_json")
        if canonical_hash(observation, exclude_volatile=False) != row.observation_hash:
            raise DecisionOutcomeV2ContractError(
                "persisted observation_json conflicts with observation_hash"
            )
        if observation.get("dataset_hashes") != dataset_hashes:
            raise DecisionOutcomeV2ContractError(
                "persisted observation dataset lineage is inconsistent"
            )
        return {
            "id": row.id,
            "signal_id": row.signal_id,
            "outcome_contract": row.outcome_contract,
            "horizon": row.horizon,
            "engine_version": row.engine_version,
            "eval_status": row.eval_status,
            "final_action_family": row.final_action_family,
            "outcome": row.outcome,
            "direction_correct": row.direction_correct,
            "reason_code": row.reason_code,
            "execution_status": row.execution_status,
            "signal_created_at": _iso(row.signal_created_at),
            "signal_session": _iso(row.signal_session),
            "stock_code": row.stock_code,
            "market": row.market,
            "source_type": row.source_type,
            "signal_action": row.signal_action,
            "signal_horizon": row.signal_horizon,
            "signal_status": row.signal_status,
            "decision_profile": row.decision_profile,
            "research_snapshot_hash": row.research_snapshot_hash,
            "policy_version": row.policy_version,
            "policy_hash": row.policy_hash,
            "policy_evaluation_hash": row.policy_evaluation_hash,
            "portfolio_snapshot_ref": row.portfolio_snapshot_ref,
            "prompt_version": row.prompt_version,
            "research_stance": row.research_stance,
            "proposed_account_action": row.proposed_account_action,
            "final_account_action": row.final_account_action,
            "policy_mode": row.policy_mode,
            "policy_verdict": row.policy_verdict,
            "policy_allowed": bool(row.policy_allowed),
            "would_block": bool(row.would_block),
            "confidence": row.confidence,
            "signal_score": row.signal_score,
            "value_quality_score": row.value_quality_score,
            "trend_timing_score": row.trend_timing_score,
            "catalyst_score": row.catalyst_score,
            "risk_score": row.risk_score,
            "evidence_quality_score": row.evidence_quality_score,
            "entry_trade_date": _iso(row.entry_trade_date),
            "entry_raw_open": row.entry_raw_open,
            "entry_adj_factor": row.entry_adj_factor,
            "end_trade_date": _iso(row.end_trade_date),
            "trading_day_count": row.trading_day_count,
            "end_adjusted_close": row.end_adjusted_close,
            "stock_return_pct": row.stock_return_pct,
            "directional_return_pct": row.directional_return_pct,
            "mfe_pct": row.mfe_pct,
            "mae_pct": row.mae_pct,
            "csi300": {
                "code": row.csi300_code,
                "name": row.csi300_name,
                "status": row.csi300_status,
                "reason_code": row.csi300_reason_code,
                "return_pct": row.csi300_return_pct,
                "stock_excess_return_pct": row.csi300_stock_excess_return_pct,
                "directional_excess_return_pct": (
                    row.csi300_directional_excess_return_pct
                ),
            },
            "sw1": {
                "code": row.sw1_code,
                "name": row.sw1_name,
                "status": row.sw1_status,
                "reason_code": row.sw1_reason_code,
                "return_pct": row.sw1_return_pct,
                "stock_excess_return_pct": row.sw1_stock_excess_return_pct,
                "directional_excess_return_pct": (
                    row.sw1_directional_excess_return_pct
                ),
            },
            "dataset_hashes": dataset_hashes,
            "observation_hash": row.observation_hash,
            "evaluated_at": _iso(row.evaluated_at),
            "created_at": _iso(row.created_at),
            "updated_at": _iso(row.updated_at),
        }

    @staticmethod
    def _optional_positive_int(value: Any, field: str) -> Optional[int]:
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            raise ValueError(f"{field} must be a positive integer")
        try:
            number = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be a positive integer") from exc
        if number <= 0:
            raise ValueError(f"{field} must be a positive integer")
        return number

    @staticmethod
    def _optional_enum(
        value: Any,
        allowed: frozenset[str],
        field: str,
    ) -> Optional[str]:
        if value in (None, ""):
            return None
        normalized = str(value).strip().lower()
        if normalized not in allowed:
            raise ValueError(
                f"{field} must be one of {', '.join(sorted(allowed))}"
            )
        return normalized


__all__ = [
    "DecisionOutcomeV2ContractError",
    "DecisionOutcomeV2NotFoundError",
    "DecisionOutcomeV2QueryService",
]
