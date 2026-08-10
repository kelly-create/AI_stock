# -*- coding: utf-8 -*-
"""Deterministic personal-research Portfolio Policy Gate.

The gate is deliberately pure: it never calls an LLM or a provider.  It
separates a research stance from an account action, records what shadow mode
would block, and makes enforce mode monotonically less risky by downgrading a
blocked proposal to ``observe``.
"""

from __future__ import annotations

import hashlib
import json
import math
import sys
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from src.config import Config, get_config
from src.services.research.canonical import canonical_hash, canonicalize


RESEARCH_STANCES = frozenset(
    {"strong_bullish", "bullish", "watch", "neutral", "bearish", "avoid"}
)
ACCOUNT_ACTIONS = frozenset(
    {
        "observe",
        "open_candidate",
        "add_candidate",
        "hold",
        "reduce_candidate",
        "exit_candidate",
    }
)
RISK_INCREASING_ACTIONS = frozenset({"open_candidate", "add_candidate"})
RISK_REDUCING_ACTIONS = frozenset({"reduce_candidate", "exit_candidate"})
POLICY_MODES = frozenset({"off", "shadow", "enforce"})
POLICY_REPLAY_CONTRACT_VERSION = "portfolio-policy-replay-v1"


@dataclass(frozen=True)
class InvestorPolicy:
    """Versioned deterministic limits from the approved personal plan."""

    version: str = "personal-cn-v1"
    minimum_value_quality_score: float = 65.0
    minimum_trend_timing_score: float = 65.0
    minimum_evidence_quality_score: float = 70.0
    maximum_risk_score: float = 45.0
    initial_position_limit_pct: float = 5.0
    normal_position_limit_pct: float = 10.0
    hard_position_limit_pct: float = 15.0
    sector_limit_pct: float = 30.0
    position_risk_limit_pct: float = 1.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "minimum_value_quality_score": self.minimum_value_quality_score,
            "minimum_trend_timing_score": self.minimum_trend_timing_score,
            "minimum_evidence_quality_score": self.minimum_evidence_quality_score,
            "maximum_risk_score": self.maximum_risk_score,
            "initial_position_limit_pct": self.initial_position_limit_pct,
            "normal_position_limit_pct": self.normal_position_limit_pct,
            "hard_position_limit_pct": self.hard_position_limit_pct,
            "sector_limit_pct": self.sector_limit_pct,
            "position_risk_limit_pct": self.position_risk_limit_pct,
        }

    @property
    def content_hash(self) -> str:
        return _sha256_json(self.as_dict())


@dataclass(frozen=True)
class PortfolioPolicyGateResult:
    """Normalized gate output plus immutable audit fields."""

    formal_personal_research: bool
    signal_fields: Dict[str, Any]
    evaluation_fields: Dict[str, Any]


class PortfolioPolicyGateService:
    """Evaluate one DecisionSignal proposal under a deterministic policy."""

    def __init__(
        self,
        *,
        config: Optional[Config] = None,
        policy: Optional[InvestorPolicy] = None,
    ) -> None:
        self.config = config or get_config()
        self.policy = policy or InvestorPolicy()

    def evaluate(
        self,
        *,
        payload: Mapping[str, Any],
        normalized_signal: Mapping[str, Any],
        job_id: Optional[str] = None,
        replay_contract: Optional[Mapping[str, Any]] = None,
    ) -> PortfolioPolicyGateResult:
        if replay_contract is not None:
            validated_replay = validate_portfolio_policy_replay_contract(
                replay_contract,
                policy=self.policy,
            )
            configured_mode = str(validated_replay["mode"])
        else:
            configured_mode = str(
                self.config.portfolio_policy_gate_mode or "off"
            ).strip().lower()
        if configured_mode not in POLICY_MODES:
            raise ValueError("PORTFOLIO_POLICY_GATE_MODE must be off, shadow, or enforce")

        # The gate belongs to the personal-research contract.  Enabling shadow
        # or enforce must not make legacy DecisionSignal writers suddenly
        # require research scores and portfolio snapshots.  A payload that
        # starts supplying formal fields is fail-closed and must provide the
        # complete lineage; a legacy payload remains an audited off-mode pass.
        formal_personal_research = self._has_formal_personal_research_input(
            payload=payload,
            normalized_signal=normalized_signal,
        )
        mode = configured_mode if formal_personal_research else "off"

        legacy_action = str(normalized_signal.get("action") or "").strip().lower()
        research_stance = self._research_stance(payload.get("research_stance"), legacy_action)
        proposed_action = self._account_action(payload.get("account_action"), legacy_action)
        # Opening or adding risk needs the complete research scorecard.  A
        # reduce/exit proposal must remain executable even when the upstream
        # research payload is incomplete; the policy gate must never make a
        # portfolio harder to de-risk.
        scores = self._scores(
            payload,
            required=mode != "off" and proposed_action in RISK_INCREASING_ACTIONS,
        )
        research_snapshot_hash = self._optional_sha256(
            normalized_signal.get("research_snapshot_hash"),
            "research_snapshot_hash",
            required=mode != "off",
        )
        context = self._policy_context(
            payload.get("policy_context"),
            required=mode != "off" and proposed_action in RISK_INCREASING_ACTIONS,
        )

        reasons: list[str] = []
        if mode != "off" and proposed_action in RISK_INCREASING_ACTIONS:
            reasons.extend(self._score_reasons(scores))
            reasons.extend(self._portfolio_reasons(proposed_action, context))

        # De-risking and passive actions are never blocked by ordinary opening
        # limits.  This also ensures the Gate cannot make a proposal riskier.
        allowed = not reasons
        would_block = not allowed
        final_action = (
            "observe"
            if mode == "enforce" and would_block
            else proposed_action
        )
        verdict = (
            "block"
            if would_block
            else "no_action"
            if proposed_action in {"observe", "hold"}
            else "allow"
        )

        policy_document = self.policy.as_dict()
        normalized_input = {
            "mode": mode,
            "configured_mode": configured_mode,
            "formal_personal_research": formal_personal_research,
            "job_id": job_id,
            "signal_identity": {
                "idempotency_key": normalized_signal.get("idempotency_key"),
                "source_report_id": normalized_signal.get("source_report_id"),
                "trace_id": normalized_signal.get("trace_id"),
                "market": normalized_signal.get("market"),
                "stock_code": normalized_signal.get("stock_code"),
                "decision_profile": normalized_signal.get("decision_profile"),
                "action": normalized_signal.get("action"),
                "horizon": normalized_signal.get("horizon"),
                "market_phase": normalized_signal.get("market_phase"),
            },
            "research_stance": research_stance,
            "proposed_account_action": proposed_action,
            "research_snapshot_hash": research_snapshot_hash,
            "scores": scores,
            "portfolio": context,
            "policy": policy_document,
        }
        normalized_output = {
            "allowed": allowed,
            "would_block": would_block,
            "final_account_action": final_action,
            "verdict": verdict,
            "reason_codes": reasons,
        }
        input_hash = _sha256_json(normalized_input)
        output_hash = _sha256_json(normalized_output)
        evaluation_hash = _sha256_json(
            {
                "schema": "portfolio-policy-evaluation-v1",
                "input_hash": input_hash,
                "output_hash": output_hash,
            }
        )

        signal_fields = {
            "research_stance": research_stance,
            "account_action": final_action,
            "value_quality_score": scores.get("value_quality"),
            "trend_timing_score": scores.get("trend_timing"),
            "catalyst_score": scores.get("catalyst"),
            "risk_score": scores.get("risk"),
            "evidence_quality_score": scores.get("evidence_quality"),
            "policy_version": self.policy.version,
            "policy_hash": self.policy.content_hash,
            "policy_evaluation_hash": evaluation_hash,
            "portfolio_snapshot_ref": context.get("portfolio_snapshot_ref"),
            "policy_mode": mode,
            "policy_decision": verdict,
            "would_block": would_block,
            "policy_reasons_json": _canonical_json(reasons),
        }
        evaluation_fields = {
            "evaluation_hash": evaluation_hash,
            "job_id": job_id,
            "stock_code": normalized_signal.get("stock_code"),
            "market": normalized_signal.get("market"),
            "mode": mode,
            "policy_version": self.policy.version,
            "policy_hash": self.policy.content_hash,
            "research_snapshot_hash": research_snapshot_hash,
            "portfolio_snapshot_ref": context.get("portfolio_snapshot_ref"),
            "portfolio_context_json": _canonical_json(context),
            "input_hash": input_hash,
            "output_hash": output_hash,
            "research_stance": research_stance,
            "proposed_account_action": proposed_action,
            "final_account_action": final_action,
            "verdict": verdict,
            "allowed": allowed,
            "would_block": would_block,
            "reasons_json": _canonical_json(reasons),
            "component_scores_json": _canonical_json(scores),
            "limits_json": _canonical_json(policy_document),
        }
        return PortfolioPolicyGateResult(
            formal_personal_research=formal_personal_research,
            signal_fields=signal_fields,
            evaluation_fields=evaluation_fields,
        )


    @staticmethod
    def _has_formal_personal_research_input(
        *,
        payload: Mapping[str, Any],
        normalized_signal: Mapping[str, Any],
    ) -> bool:
        if normalized_signal.get("research_snapshot_hash") not in (None, ""):
            return True
        formal_fields = (
            "research_stance",
            "account_action",
            "value_quality_score",
            "trend_timing_score",
            "catalyst_score",
            "risk_score",
            "evidence_quality_score",
            "policy_context",
        )
        return any(payload.get(field_name) not in (None, "") for field_name in formal_fields)

    @staticmethod
    def _research_stance(value: Any, legacy_action: str) -> str:
        if value not in (None, ""):
            normalized = str(value).strip().lower()
            if normalized not in RESEARCH_STANCES:
                raise ValueError(
                    "research_stance must be strong_bullish, bullish, watch, neutral, bearish, or avoid"
                )
            return normalized
        return {
            "buy": "bullish",
            "add": "bullish",
            "hold": "neutral",
            "reduce": "bearish",
            "sell": "bearish",
            "avoid": "avoid",
            "watch": "watch",
            "alert": "watch",
        }.get(legacy_action, "neutral")

    @staticmethod
    def _account_action(value: Any, legacy_action: str) -> str:
        if value not in (None, ""):
            normalized = str(value).strip().lower()
            if normalized not in ACCOUNT_ACTIONS:
                raise ValueError(
                    "account_action must be observe, open_candidate, add_candidate, hold, reduce_candidate, or exit_candidate"
                )
            return normalized
        return {
            "buy": "open_candidate",
            "add": "add_candidate",
            "hold": "hold",
            "reduce": "reduce_candidate",
            "sell": "exit_candidate",
        }.get(legacy_action, "observe")

    @classmethod
    def _scores(cls, payload: Mapping[str, Any], *, required: bool) -> Dict[str, Optional[float]]:
        field_map = {
            "value_quality": "value_quality_score",
            "trend_timing": "trend_timing_score",
            "catalyst": "catalyst_score",
            "risk": "risk_score",
            "evidence_quality": "evidence_quality_score",
        }
        scores: Dict[str, Optional[float]] = {}
        for key, field_name in field_map.items():
            value = payload.get(field_name)
            if value in (None, ""):
                if required:
                    raise ValueError(f"{field_name} is required when the policy gate is enabled")
                scores[key] = None
                continue
            scores[key] = cls._bounded_number(value, field_name, minimum=0.0, maximum=100.0)
        return scores

    @classmethod
    def _policy_context(cls, value: Any, *, required: bool) -> Dict[str, Any]:
        if value is None:
            if required:
                raise ValueError("policy_context is required for a risk-increasing action")
            return {}
        if not isinstance(value, Mapping):
            raise ValueError("policy_context must be an object")

        allowed_fields = {
            "portfolio_complete",
            "portfolio_snapshot_ref",
            "current_position_weight_pct",
            "projected_position_weight_pct",
            "projected_sector_weight_pct",
            "position_risk_pct",
            "incomplete_reason_codes",
            "audit_snapshot_hash",
            "audit_snapshot",
        }
        unknown_fields = sorted(set(value).difference(allowed_fields))
        if unknown_fields:
            raise ValueError(
                "policy_context contains unsupported fields: "
                + ",".join(str(item) for item in unknown_fields)
            )

        complete = value.get("portfolio_complete")
        if not isinstance(complete, bool):
            raise ValueError("policy_context.portfolio_complete must be a boolean")
        snapshot_ref = str(value.get("portfolio_snapshot_ref") or "").strip()
        if not snapshot_ref or len(snapshot_ref) > 128:
            raise ValueError("policy_context.portfolio_snapshot_ref is required and must be at most 128 characters")

        normalized: Dict[str, Any] = {
            "portfolio_complete": complete,
            "portfolio_snapshot_ref": snapshot_ref,
        }
        raw_reason_codes = value.get("incomplete_reason_codes", ())
        if not isinstance(raw_reason_codes, (list, tuple)) or any(
            not isinstance(item, str)
            or not item.strip()
            or len(item.strip()) > 64
            for item in raw_reason_codes
        ):
            raise ValueError(
                "policy_context.incomplete_reason_codes must be a list of non-empty strings"
            )
        normalized["incomplete_reason_codes"] = sorted(
            {item.strip() for item in raw_reason_codes}
        )
        audit_snapshot = value.get("audit_snapshot")
        audit_snapshot_hash = value.get("audit_snapshot_hash")
        if (audit_snapshot is None) != (audit_snapshot_hash is None):
            raise ValueError(
                "policy_context audit_snapshot and audit_snapshot_hash must be supplied together"
            )
        if audit_snapshot is not None:
            if not isinstance(audit_snapshot, Mapping):
                raise ValueError("policy_context.audit_snapshot must be an object")
            normalized_audit = canonicalize(
                audit_snapshot,
                exclude_volatile=False,
            )
            normalized_hash = cls._optional_sha256(
                audit_snapshot_hash,
                "policy_context.audit_snapshot_hash",
                required=True,
            )
            if canonical_hash(
                normalized_audit,
                exclude_volatile=False,
            ) != normalized_hash:
                raise ValueError("policy_context audit snapshot hash mismatch")
            expected_ref = f"portfolio-policy-context-v1:{normalized_hash}"
            if snapshot_ref != expected_ref:
                raise ValueError(
                    "policy_context.portfolio_snapshot_ref must bind the audit snapshot hash"
                )
            normalized["audit_snapshot_hash"] = normalized_hash
            normalized["audit_snapshot"] = normalized_audit
        for field_name in (
            "current_position_weight_pct",
            "projected_position_weight_pct",
            "projected_sector_weight_pct",
            "position_risk_pct",
        ):
            raw = value.get(field_name)
            if raw in (None, ""):
                normalized[field_name] = None
            else:
                normalized[field_name] = cls._bounded_number(
                    raw,
                    f"policy_context.{field_name}",
                    minimum=0.0,
                    maximum=100.0,
                )
        return normalized

    def _score_reasons(self, scores: Mapping[str, Optional[float]]) -> list[str]:
        reasons: list[str] = []
        if scores["value_quality"] is None or scores["value_quality"] < self.policy.minimum_value_quality_score:
            reasons.append("value_quality_below_minimum")
        if scores["trend_timing"] is None or scores["trend_timing"] < self.policy.minimum_trend_timing_score:
            reasons.append("trend_timing_below_minimum")
        if scores["evidence_quality"] is None or scores["evidence_quality"] < self.policy.minimum_evidence_quality_score:
            reasons.append("evidence_quality_below_minimum")
        if scores["risk"] is None or scores["risk"] > self.policy.maximum_risk_score:
            reasons.append("risk_score_above_maximum")
        return reasons

    def _portfolio_reasons(self, proposed_action: str, context: Mapping[str, Any]) -> list[str]:
        reasons: list[str] = []
        if context.get("portfolio_complete") is not True:
            reasons.append("portfolio_snapshot_incomplete")

        current = context.get("current_position_weight_pct")
        if current is None:
            reasons.append("current_position_weight_missing")
        elif proposed_action == "open_candidate" and current > 1e-8:
            reasons.append("open_candidate_existing_position")
        elif proposed_action == "add_candidate" and current <= 1e-8:
            reasons.append("add_candidate_without_position")

        projected = context.get("projected_position_weight_pct")
        if projected is None:
            reasons.append("projected_position_weight_missing")
        else:
            if (
                proposed_action == "add_candidate"
                and current is not None
                and projected <= current + 1e-8
            ):
                reasons.append("add_target_not_above_current")
            if projected > self.policy.hard_position_limit_pct:
                reasons.append("position_hard_limit_exceeded")
            limit = (
                self.policy.initial_position_limit_pct
                if proposed_action == "open_candidate"
                else self.policy.normal_position_limit_pct
            )
            if projected > limit:
                reasons.append(
                    "initial_position_limit_exceeded"
                    if proposed_action == "open_candidate"
                    else "normal_position_limit_exceeded"
                )

        sector = context.get("projected_sector_weight_pct")
        if sector is None:
            reasons.append("projected_sector_weight_missing")
        elif sector > self.policy.sector_limit_pct:
            reasons.append("sector_limit_exceeded")

        position_risk = context.get("position_risk_pct")
        if position_risk is None:
            reasons.append("position_risk_missing")
        elif position_risk > self.policy.position_risk_limit_pct:
            reasons.append("position_risk_limit_exceeded")
        return reasons

    @staticmethod
    def _optional_sha256(value: Any, field_name: str, *, required: bool) -> Optional[str]:
        if value in (None, ""):
            if required:
                raise ValueError(f"{field_name} is required when the policy gate is enabled")
            return None
        normalized = str(value).strip().lower()
        if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
            raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
        return normalized

    @staticmethod
    def _bounded_number(value: Any, field_name: str, *, minimum: float, maximum: float) -> float:
        if isinstance(value, bool):
            raise ValueError(f"{field_name} must be a number")
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be a number") from exc
        if not math.isfinite(number) or number < minimum or number > maximum:
            raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
        return number


def build_portfolio_policy_replay_contract(
    mode: str,
    *,
    policy: Optional[InvestorPolicy] = None,
) -> Dict[str, Any]:
    """Fingerprint the exact Policy implementation selected before history save."""

    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in POLICY_MODES:
        raise ValueError("policy replay mode must be off, shadow, or enforce")
    active_policy = policy or InvestorPolicy()
    from src.services import personal_research_policy_context_service
    from src.services.research.code_fingerprint import fingerprint_code

    payload = canonicalize(
        {
            "contract_version": POLICY_REPLAY_CONTRACT_VERSION,
            "mode": normalized_mode,
            "policy_version": active_policy.version,
            "policy_hash": active_policy.content_hash,
            "gate_code_hash": fingerprint_code(
                sys.modules[__name__],
                version="portfolio-policy-gate-module-v1",
            ),
            "context_code_hash": fingerprint_code(
                personal_research_policy_context_service,
                version="personal-research-policy-context-module-v1",
            ),
        },
        exclude_volatile=False,
    )
    return {
        **payload,
        "contract_hash": canonical_hash(payload, exclude_volatile=False),
    }


def validate_portfolio_policy_replay_contract(
    value: Mapping[str, Any],
    *,
    policy: Optional[InvestorPolicy] = None,
) -> Dict[str, Any]:
    """Fail closed when a history retry no longer runs the frozen contract."""

    if not isinstance(value, Mapping):
        raise ValueError("policy replay contract must be an object")
    mode = str(value.get("mode") or "").strip().lower()
    expected = build_portfolio_policy_replay_contract(mode, policy=policy)
    if canonicalize(value, exclude_volatile=False) != expected:
        raise ValueError("policy replay contract differs from the frozen implementation")
    return expected


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
