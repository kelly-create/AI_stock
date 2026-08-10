"""Derive one task-level mode and bounded Debate decision from frozen facts."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Optional

from .canonical import canonical_hash, canonicalize
from .evidence_service import FrozenEvidenceSnapshot
from .research_task_policy import (
    DebateTriggerDecision,
    ResearchModeResolution,
    evaluate_debate_trigger,
    resolve_research_task_mode,
)
from .schemas import ComponentResult, ResearchFactorResult


PERSONAL_TASK_DECISION_VERSION = "personal-research-task-decision-v1"


@dataclass(frozen=True)
class PersonalResearchTaskDecision:
    version: str
    mode: ResearchModeResolution
    debate: DebateTriggerDecision
    value_quality_score: Optional[float]
    conflict_components: tuple[tuple[str, float], ...]
    missing_score_fields: tuple[str, ...]
    has_citable_evidence: bool
    evidence_quality_score: float
    decision_hash: str

    def policy_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "mode": {
                "requested_mode": self.mode.requested_mode,
                "resolved_mode": self.mode.resolved_mode,
                "priority": self.mode.priority,
                "resolver_version": self.mode.resolver_version,
                "reason_code": self.mode.reason_code,
                "resolution_hash": self.mode.resolution_hash,
            },
            "debate": {
                "triggered": self.debate.triggered,
                "conflict_score": self.debate.conflict_score,
                "significance_score": self.debate.significance_score,
                "evidence_quality_score": self.debate.evidence_quality_score,
                "reason_codes": list(self.debate.reason_codes),
                "policy_version": self.debate.policy_version,
                "policy_hash": self.debate.policy_hash,
                "decision_hash": self.debate.decision_hash,
            },
            "value_quality_score": self.value_quality_score,
            "conflict_components": [
                {"name": name, "score": score}
                for name, score in self.conflict_components
            ],
            "missing_score_fields": list(self.missing_score_fields),
            "has_citable_evidence": self.has_citable_evidence,
            "evidence_quality_score": self.evidence_quality_score,
            "decision_hash": self.decision_hash,
        }


def _optional_score(component: ComponentResult, *, field: str) -> Optional[float]:
    if not isinstance(component, ComponentResult):
        raise TypeError(f"{field} must be a ComponentResult")
    value = component.score
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field}.score must be a finite number or null")
    normalized = float(value)
    if not math.isfinite(normalized) or not 0.0 <= normalized <= 100.0:
        raise ValueError(f"{field}.score must be between 0 and 100")
    return round(normalized, 4)


def derive_personal_research_task_decision(
    *,
    requested_mode: Any,
    priority: Any,
    debate_enabled: Any,
    factors: ResearchFactorResult,
    evidence_snapshot: FrozenEvidenceSnapshot,
) -> PersonalResearchTaskDecision:
    """Resolve mode and trigger Debate without treating a missing score as zero."""

    if not isinstance(factors, ResearchFactorResult):
        raise TypeError("factors must be a ResearchFactorResult")
    if not isinstance(evidence_snapshot, FrozenEvidenceSnapshot):
        raise TypeError("evidence_snapshot must be a FrozenEvidenceSnapshot")
    mode = resolve_research_task_mode(requested_mode, priority=priority)
    value = _optional_score(factors.value, field="value")
    quality = _optional_score(factors.quality, field="quality")
    trend = _optional_score(factors.trend_timing, field="trend_timing")
    catalyst = _optional_score(factors.catalyst, field="catalyst")
    risk = _optional_score(factors.risk, field="risk")

    missing: list[str] = []
    if value is None:
        missing.append("value")
    if quality is None:
        missing.append("quality")
    if trend is None:
        missing.append("trend_timing")
    if catalyst is None:
        missing.append("catalyst")
    if risk is None:
        missing.append("risk")
    value_quality = (
        round((value + quality) / 2.0, 4)
        if value is not None and quality is not None
        else None
    )
    components = {
        "value_quality": value_quality,
        "trend_timing": trend,
        "catalyst": catalyst,
        "risk_inverse": round(100.0 - risk, 4) if risk is not None else None,
    }
    present = tuple(
        sorted(
            (name, score)
            for name, score in components.items()
            if score is not None
        )
    )
    conflict_score = (
        int(round(max(score for _name, score in present) - min(score for _name, score in present)))
        if len(present) >= 2
        else 0
    )
    coverage = evidence_snapshot.coverage
    if isinstance(coverage, bool) or not isinstance(coverage, (int, float)):
        raise TypeError("evidence coverage must be a finite number")
    coverage_float = float(coverage)
    if not math.isfinite(coverage_float) or not 0.0 <= coverage_float <= 1.0:
        raise ValueError("evidence coverage must be between 0 and 1")
    evidence_quality = round(coverage_float * 100.0, 4)
    has_citable = any(
        claim.citation_ids
        and claim.status in {"supported", "partial", "contradicted"}
        for claim in evidence_snapshot.claims
    )
    debate = evaluate_debate_trigger(
        resolved_mode=mode.resolved_mode,
        debate_enabled=debate_enabled,
        has_citable_evidence=has_citable,
        conflict_score=conflict_score,
        significance_score=mode.priority,
        evidence_quality_score=evidence_quality,
    )
    payload = canonicalize(
        {
            "version": PERSONAL_TASK_DECISION_VERSION,
            "mode_resolution_hash": mode.resolution_hash,
            "debate_decision_hash": debate.decision_hash,
            "value_quality_score": value_quality,
            "conflict_components": [
                {"name": name, "score": score} for name, score in present
            ],
            "missing_score_fields": sorted(missing),
            "has_citable_evidence": has_citable,
            "evidence_quality_score": evidence_quality,
        },
        exclude_volatile=False,
    )
    return PersonalResearchTaskDecision(
        version=PERSONAL_TASK_DECISION_VERSION,
        mode=mode,
        debate=debate,
        value_quality_score=value_quality,
        conflict_components=present,
        missing_score_fields=tuple(sorted(missing)),
        has_citable_evidence=has_citable,
        evidence_quality_score=evidence_quality,
        decision_hash=canonical_hash(payload, exclude_volatile=False),
    )


__all__ = [
    "PERSONAL_TASK_DECISION_VERSION",
    "PersonalResearchTaskDecision",
    "derive_personal_research_task_decision",
]
