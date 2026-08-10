# -*- coding: utf-8 -*-
"""Persistence helper for immutable Portfolio Policy Gate evaluations."""

from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy import select

from src.storage import PortfolioPolicyEvaluationRecord


class PortfolioPolicyEvaluationConflictError(RuntimeError):
    """Raised when one evaluation hash resolves to different immutable facts."""


class PortfolioPolicyEvaluationRepository:
    """Session-scoped helper used by the atomic DecisionSignal write path."""

    _IMMUTABLE_FIELDS = (
        "evaluation_hash",
        "job_id",
        "stock_code",
        "market",
        "mode",
        "policy_version",
        "policy_hash",
        "research_snapshot_hash",
        "portfolio_snapshot_ref",
        "portfolio_context_json",
        "input_hash",
        "output_hash",
        "research_stance",
        "proposed_account_action",
        "final_account_action",
        "verdict",
        "allowed",
        "would_block",
        "reasons_json",
        "component_scores_json",
        "limits_json",
    )

    @classmethod
    def ensure_in_session(
        cls,
        *,
        session: Any,
        signal_id: int,
        fields: Optional[Dict[str, Any]],
    ) -> tuple[Optional[PortfolioPolicyEvaluationRecord], bool]:
        if not fields:
            return None, False
        evaluation_hash = str(fields.get("evaluation_hash") or "")
        if not evaluation_hash:
            raise ValueError("policy evaluation_hash is required")
        existing = session.execute(
            select(PortfolioPolicyEvaluationRecord)
            .where(
                PortfolioPolicyEvaluationRecord.evaluation_hash
                == evaluation_hash
            )
            .limit(1)
        ).scalar_one_or_none()
        if existing is not None:
            cls._assert_same(existing, signal_id=signal_id, fields=fields)
            return existing, False

        row = PortfolioPolicyEvaluationRecord(signal_id=signal_id, **fields)
        session.add(row)
        session.flush()
        return row, True

    @classmethod
    def _assert_same(
        cls,
        row: PortfolioPolicyEvaluationRecord,
        *,
        signal_id: int,
        fields: Dict[str, Any],
    ) -> None:
        mismatched = [
            name
            for name in cls._IMMUTABLE_FIELDS
            if getattr(row, name) != fields.get(name)
        ]
        if row.signal_id != signal_id:
            mismatched.append("signal_id")
        if mismatched:
            raise PortfolioPolicyEvaluationConflictError(
                "policy evaluation hash conflicts with immutable fields: "
                + ",".join(sorted(mismatched))
            )
