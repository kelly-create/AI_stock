"""Pure PR4 task-mode resolution and on-demand Debate trigger policy.

Mode resolution and Debate triggering are intentionally separate decisions.
In particular, enabling the global Debate capability never promotes every
research task to Debate; a task must be explicit or meet a versioned material
conflict/significance threshold and all fail-closed prerequisites.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Optional

from .canonical import canonical_hash, canonicalize


RESEARCH_TASK_MODES = ("auto", "quick", "standard", "deep", "debate")
RESOLVED_RESEARCH_TASK_MODES = ("quick", "standard", "deep", "debate")
RESEARCH_TASK_MODE_RESOLVER_VERSION = "personal-research-mode-resolver-v1"
DEBATE_TRIGGER_POLICY_VERSION = "personal-research-debate-trigger-v1"


def _mode(value: Any, *, allow_auto: bool) -> str:
    if not isinstance(value, str):
        raise TypeError("research_mode must be a string")
    normalized = value.strip().casefold()
    allowed = RESEARCH_TASK_MODES if allow_auto else RESOLVED_RESEARCH_TASK_MODES
    if normalized not in allowed:
        raise ValueError(
            "research_mode must be auto, quick, standard, deep, or debate"
            if allow_auto
            else "resolved research_mode must be quick, standard, deep, or debate"
        )
    return normalized


def _exact_int(value: Any, *, field: str, minimum: int, maximum: int) -> int:
    if type(value) is not int:
        raise TypeError(f"{field} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return value


def _exact_bool(value: Any, *, field: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{field} must be a boolean")
    return value


def _optional_score(value: Any, *, field: str) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be a finite number or null")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field} must be finite")
    if not 0.0 <= normalized <= 100.0:
        raise ValueError(f"{field} must be between 0 and 100")
    return round(normalized, 4)


@dataclass(frozen=True)
class ResearchModeResolution:
    requested_mode: str
    resolved_mode: str
    priority: int
    resolver_version: str
    reason_code: str
    resolution_hash: str


def resolve_research_task_mode(
    requested_mode: Any,
    *,
    priority: Any,
) -> ResearchModeResolution:
    """Resolve ``auto`` with the approved Quick/Standard/Deep thresholds."""

    requested = _mode(requested_mode, allow_auto=True)
    normalized_priority = _exact_int(priority, field="priority", minimum=0, maximum=100)
    if requested != "auto":
        resolved = requested
        reason_code = "explicit_mode"
    elif normalized_priority >= 80:
        resolved = "deep"
        reason_code = "auto_high_priority"
    elif normalized_priority >= 40:
        resolved = "standard"
        reason_code = "auto_medium_priority"
    else:
        resolved = "quick"
        reason_code = "auto_low_priority"
    payload = canonicalize(
        {
            "resolver_version": RESEARCH_TASK_MODE_RESOLVER_VERSION,
            "requested_mode": requested,
            "resolved_mode": resolved,
            "priority": normalized_priority,
            "reason_code": reason_code,
        },
        exclude_volatile=False,
    )
    return ResearchModeResolution(
        requested_mode=requested,
        resolved_mode=resolved,
        priority=normalized_priority,
        resolver_version=RESEARCH_TASK_MODE_RESOLVER_VERSION,
        reason_code=reason_code,
        resolution_hash=canonical_hash(payload, exclude_volatile=False),
    )


@dataclass(frozen=True)
class DebateTriggerPolicy:
    """Versioned thresholds for automatic, bounded Debate promotion."""

    version: str = DEBATE_TRIGGER_POLICY_VERSION
    material_conflict_threshold: int = 60
    significance_threshold: int = 80
    minimum_evidence_quality_score: float = 70.0

    def __post_init__(self) -> None:
        _exact_int(
            self.material_conflict_threshold,
            field="material_conflict_threshold",
            minimum=0,
            maximum=100,
        )
        _exact_int(
            self.significance_threshold,
            field="significance_threshold",
            minimum=0,
            maximum=100,
        )
        minimum_evidence_quality = _optional_score(
            self.minimum_evidence_quality_score,
            field="minimum_evidence_quality_score",
        )
        if minimum_evidence_quality is None:
            raise TypeError("minimum_evidence_quality_score must be a finite number")
        if not isinstance(self.version, str):
            raise TypeError("version must be a non-empty string")
        if not self.version.strip():
            raise ValueError("version must be a non-empty string")
        object.__setattr__(self, "minimum_evidence_quality_score", minimum_evidence_quality)

    def content_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "material_conflict_threshold": self.material_conflict_threshold,
            "significance_threshold": self.significance_threshold,
            "minimum_evidence_quality_score": self.minimum_evidence_quality_score,
        }

    @property
    def content_hash(self) -> str:
        return canonical_hash(self.content_payload(), exclude_volatile=False)


@dataclass(frozen=True)
class DebateTriggerDecision:
    triggered: bool
    resolved_mode: str
    conflict_score: int
    significance_score: int
    evidence_quality_score: Optional[float]
    reason_codes: tuple[str, ...]
    policy_version: str
    policy_hash: str
    decision_hash: str


def evaluate_debate_trigger(
    *,
    resolved_mode: Any,
    debate_enabled: Any,
    has_citable_evidence: Any,
    conflict_score: Any,
    significance_score: Any,
    evidence_quality_score: Any,
    policy: Optional[DebateTriggerPolicy] = None,
) -> DebateTriggerDecision:
    """Decide whether one task may enter Debate, failing closed by reason code."""

    mode = _mode(resolved_mode, allow_auto=False)
    enabled = _exact_bool(debate_enabled, field="debate_enabled")
    citable = _exact_bool(has_citable_evidence, field="has_citable_evidence")
    conflict = _exact_int(conflict_score, field="conflict_score", minimum=0, maximum=100)
    significance = _exact_int(
        significance_score,
        field="significance_score",
        minimum=0,
        maximum=100,
    )
    evidence_quality = _optional_score(
        evidence_quality_score,
        field="evidence_quality_score",
    )
    active_policy = policy or DebateTriggerPolicy()

    explicit = mode == "debate"
    reasons: list[str] = []
    if not enabled:
        reasons.append("debate_disabled")
    if not citable:
        reasons.append("citable_evidence_missing")
    if evidence_quality is None:
        reasons.append("evidence_quality_missing")
    elif (
        not explicit
        and evidence_quality < active_policy.minimum_evidence_quality_score
    ):
        reasons.append("evidence_quality_below_minimum")

    material_conflict = conflict >= active_policy.material_conflict_threshold
    significant = significance >= active_policy.significance_threshold
    if not (explicit or material_conflict or significant):
        reasons.append("trigger_conditions_not_met")

    triggered = not reasons
    if triggered:
        if explicit:
            reasons.append("explicit_debate_mode")
        if material_conflict:
            reasons.append("material_conflict")
        if significant:
            reasons.append("material_significance")

    payload = canonicalize(
        {
            "policy": active_policy.content_payload(),
            "policy_hash": active_policy.content_hash,
            "resolved_mode": mode,
            "debate_enabled": enabled,
            "has_citable_evidence": citable,
            "conflict_score": conflict,
            "significance_score": significance,
            "evidence_quality_score": evidence_quality,
            "triggered": triggered,
            "reason_codes": reasons,
        },
        exclude_volatile=False,
    )
    return DebateTriggerDecision(
        triggered=triggered,
        resolved_mode=mode,
        conflict_score=conflict,
        significance_score=significance,
        evidence_quality_score=evidence_quality,
        reason_codes=tuple(reasons),
        policy_version=active_policy.version,
        policy_hash=active_policy.content_hash,
        decision_hash=canonical_hash(payload, exclude_volatile=False),
    )


__all__ = [
    "DEBATE_TRIGGER_POLICY_VERSION",
    "RESEARCH_TASK_MODES",
    "RESEARCH_TASK_MODE_RESOLVER_VERSION",
    "RESOLVED_RESEARCH_TASK_MODES",
    "DebateTriggerDecision",
    "DebateTriggerPolicy",
    "ResearchModeResolution",
    "evaluate_debate_trigger",
    "resolve_research_task_mode",
]
