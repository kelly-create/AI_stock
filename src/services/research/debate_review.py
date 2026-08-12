"""Deterministic Verifier and Judge for existing bounded Debate artifacts.

The Verifier/Judge are pure post-processing roles.  They do not call a model,
provider, tool, database, or Portfolio Policy Gate.  Invalid or incomplete
artifacts return stable fail-closed reason codes instead of being interpreted
as a recommendation.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Any, Optional

from .canonical import canonical_hash, canonicalize
from .debate_service import (
    DEBATE_STANCES,
    FrozenDebateSnapshot,
    validate_debate_snapshot,
)
from .debate_security import DEBATE_JUDGE_MINIMUM_MEAN_CONFIDENCE


DEBATE_VERIFIER_VERSION = "personal-research-debate-verifier-v1"
DEBATE_JUDGE_VERSION = "personal-research-debate-judge-v1"
DEBATE_JUDGE_VERDICTS = ("bull", "bear", "balanced", "fail_closed")

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _optional_sha256(value: Any, *, field: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest or null")
    return value


def _unit_interval(value: Any, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be a finite number")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field} must be finite")
    if not 0.0 <= normalized <= 1.0:
        raise ValueError(f"{field} must be between 0 and 1")
    return round(normalized, 6)


@dataclass(frozen=True)
class DebateVerificationResult:
    valid: bool
    fail_closed: bool
    debate_hash: Optional[str]
    evidence_snapshot_hash: Optional[str]
    reason_codes: tuple[str, ...]
    verifier_version: str
    verification_hash: str


def verify_bounded_debate(
    snapshot: Any,
    *,
    expected_evidence_snapshot_hash: Any = None,
) -> DebateVerificationResult:
    """Verify the existing Debate domain object without raising on bad artifacts."""

    expected_hash = _optional_sha256(
        expected_evidence_snapshot_hash,
        field="expected_evidence_snapshot_hash",
    )
    reasons: list[str] = []
    debate_hash: Optional[str] = None
    evidence_hash: Optional[str] = None

    if not isinstance(snapshot, FrozenDebateSnapshot):
        reasons.append("invalid_debate_type")
    else:
        debate_hash = snapshot.debate_hash
        evidence_hash = snapshot.evidence_snapshot_hash
        try:
            validate_debate_snapshot(snapshot)
        except (TypeError, ValueError):
            reasons.append("debate_contract_invalid")
        if snapshot.status != "available":
            reasons.append("debate_not_complete")
        if snapshot.failures:
            reasons.append("stance_failure_present")
        stances = tuple(item.stance for item in snapshot.turns)
        if stances != DEBATE_STANCES:
            reasons.append("stance_set_incomplete")
        if any(
            not argument.claim_ids or not argument.citation_ids
            for turn in snapshot.turns
            for argument in turn.turn.arguments
        ):
            reasons.append("uncited_argument")
        if expected_hash is not None and evidence_hash != expected_hash:
            reasons.append("evidence_lineage_mismatch")

    reasons = list(dict.fromkeys(reasons))
    valid = not reasons
    payload = canonicalize(
        {
            "verifier_version": DEBATE_VERIFIER_VERSION,
            "debate_hash": debate_hash,
            "evidence_snapshot_hash": evidence_hash,
            "expected_evidence_snapshot_hash": expected_hash,
            "valid": valid,
            "fail_closed": not valid,
            "reason_codes": reasons,
        },
        exclude_volatile=False,
    )
    return DebateVerificationResult(
        valid=valid,
        fail_closed=not valid,
        debate_hash=debate_hash,
        evidence_snapshot_hash=evidence_hash,
        reason_codes=tuple(reasons),
        verifier_version=DEBATE_VERIFIER_VERSION,
        verification_hash=canonical_hash(payload, exclude_volatile=False),
    )


@dataclass(frozen=True)
class DebateJudgePolicy:
    """Versioned, count-neutral confidence-margin rule for two stances."""

    version: str = DEBATE_JUDGE_VERSION
    minimum_mean_confidence: float = DEBATE_JUDGE_MINIMUM_MEAN_CONFIDENCE
    decisive_margin: float = 0.15

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "minimum_mean_confidence",
            _unit_interval(
                self.minimum_mean_confidence,
                field="minimum_mean_confidence",
            ),
        )
        object.__setattr__(
            self,
            "decisive_margin",
            _unit_interval(self.decisive_margin, field="decisive_margin"),
        )
        if not isinstance(self.version, str) or not self.version.strip():
            raise TypeError("version must be a non-empty string")

    def content_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "minimum_mean_confidence": self.minimum_mean_confidence,
            "decisive_margin": self.decisive_margin,
        }

    @property
    def content_hash(self) -> str:
        return canonical_hash(self.content_payload(), exclude_volatile=False)


@dataclass(frozen=True)
class DebateJudgeResult:
    verdict: str
    fail_closed: bool
    bull_score: Optional[float]
    bear_score: Optional[float]
    margin: Optional[float]
    reason_codes: tuple[str, ...]
    verification_hash: str
    judge_version: str
    judge_policy_hash: str
    judgement_hash: str


def judge_bounded_debate(
    snapshot: Any,
    *,
    expected_evidence_snapshot_hash: Any = None,
    policy: Optional[DebateJudgePolicy] = None,
) -> DebateJudgeResult:
    """Judge a verified two-sided artifact; never infer through invalid input."""

    verification = verify_bounded_debate(
        snapshot,
        expected_evidence_snapshot_hash=expected_evidence_snapshot_hash,
    )
    active_policy = policy or DebateJudgePolicy()
    reasons: list[str] = []
    bull_score: Optional[float] = None
    bear_score: Optional[float] = None
    margin: Optional[float] = None

    if not verification.valid:
        verdict = "fail_closed"
        reasons.extend(("verification_failed", *verification.reason_codes))
    else:
        turn_by_stance = {item.stance: item for item in snapshot.turns}
        stance_scores: dict[str, float] = {}
        for stance in DEBATE_STANCES:
            confidences = [
                argument.confidence
                for argument in turn_by_stance[stance].turn.arguments
            ]
            stance_scores[stance] = round(sum(confidences) / len(confidences), 6)
        bull_score = stance_scores["bull"]
        bear_score = stance_scores["bear"]
        margin = round(bull_score - bear_score, 6)
        if min(bull_score, bear_score) < active_policy.minimum_mean_confidence:
            verdict = "fail_closed"
            reasons.append("stance_confidence_below_minimum")
        elif abs(margin) < active_policy.decisive_margin:
            verdict = "balanced"
            reasons.append("confidence_margin_not_decisive")
        elif margin > 0:
            verdict = "bull"
            reasons.append("bull_confidence_margin")
        else:
            verdict = "bear"
            reasons.append("bear_confidence_margin")

    fail_closed = verdict == "fail_closed"
    payload = canonicalize(
        {
            "judge_policy": active_policy.content_payload(),
            "judge_policy_hash": active_policy.content_hash,
            "verification_hash": verification.verification_hash,
            "verdict": verdict,
            "fail_closed": fail_closed,
            "bull_score": bull_score,
            "bear_score": bear_score,
            "margin": margin,
            "reason_codes": reasons,
        },
        exclude_volatile=False,
    )
    return DebateJudgeResult(
        verdict=verdict,
        fail_closed=fail_closed,
        bull_score=bull_score,
        bear_score=bear_score,
        margin=margin,
        reason_codes=tuple(reasons),
        verification_hash=verification.verification_hash,
        judge_version=active_policy.version,
        judge_policy_hash=active_policy.content_hash,
        judgement_hash=canonical_hash(payload, exclude_volatile=False),
    )


__all__ = [
    "DEBATE_JUDGE_VERDICTS",
    "DEBATE_JUDGE_VERSION",
    "DEBATE_VERIFIER_VERSION",
    "DebateJudgePolicy",
    "DebateJudgeResult",
    "DebateVerificationResult",
    "judge_bounded_debate",
    "verify_bounded_debate",
]
