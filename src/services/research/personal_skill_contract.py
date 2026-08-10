"""Immutable contracts for the five approved personal-research Skills.

This module is the candidate naming source of truth for the PR4 runtime.  A
Skill ID is stable and never embeds its version.  Version and content hash are
explicit lineage fields so a future implementation can change independently
without silently reinterpreting an old result.

The contracts deliberately map one-to-one to the five score fields already
accepted by ``DecisionSignal``.  They do not introduce another score taxonomy
or perform provider, database, or pipeline I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from types import MappingProxyType
from typing import Any, Mapping

from .canonical import canonical_hash, canonical_json, canonicalize


PERSONAL_SKILL_CONTRACT_SCHEMA_VERSION = "personal-research-skill-contract-v1"
PERSONAL_SKILL_INPUT_SCHEMA_VERSION = "personal-research-skill-input-v1"
PERSONAL_SKILL_OUTPUT_SCHEMA_VERSION = "personal-research-skill-output-v1"

PERSONAL_RESEARCH_SKILL_IDS = (
    "personal-value-quality",
    "personal-trend-timing",
    "personal-catalyst",
    "personal-risk",
    "personal-evidence-quality",
)

PERSONAL_RESEARCH_SCORE_FIELDS = (
    "value_quality_score",
    "trend_timing_score",
    "catalyst_score",
    "risk_score",
    "evidence_quality_score",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_VERSION_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_SNAPSHOT_FIELDS = frozenset(
    {"research_snapshot_hash", "factor_snapshot_hash", "evidence_snapshot_hash"}
)


def _bounded_text(value: Any, *, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    if len(normalized) > maximum:
        raise ValueError(f"{field} must contain at most {maximum} characters")
    return normalized


def _identifier(value: Any, *, field: str) -> str:
    normalized = _bounded_text(value, field=field, maximum=64).casefold()
    if _IDENTIFIER_RE.fullmatch(normalized) is None:
        raise ValueError(f"{field} must be a lowercase public identifier")
    return normalized


def _version(value: Any, *, field: str) -> str:
    normalized = _bounded_text(value, field=field, maximum=64).casefold()
    if _VERSION_RE.fullmatch(normalized) is None:
        raise ValueError(f"{field} must be a bounded lowercase version identifier")
    return normalized


def _sha256(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")
    return value


def _score(value: Any, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be a finite number")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field} must be finite")
    if not 0.0 <= normalized <= 100.0:
        raise ValueError(f"{field} must be between 0 and 100")
    return round(normalized, 4)


def _sorted_unique_identifiers(
    value: Any,
    *,
    field: str,
    maximum_items: int,
    allow_empty: bool,
) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, (list, tuple)):
        raise TypeError(f"{field} must be an array")
    if len(value) > maximum_items:
        raise ValueError(f"{field} must contain at most {maximum_items} items")
    normalized = tuple(_identifier(item, field=f"{field} item") for item in value)
    if not allow_empty and not normalized:
        raise ValueError(f"{field} must not be empty")
    if normalized != tuple(sorted(set(normalized))):
        raise ValueError(f"{field} must be sorted and unique")
    return normalized


def _sorted_unique_references(value: Any, *, field: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, (list, tuple)):
        raise TypeError(f"{field} must be an array")
    if not 1 <= len(value) <= 32:
        raise ValueError(f"{field} must contain between 1 and 32 items")
    normalized = tuple(
        _bounded_text(item, field=f"{field} item", maximum=128) for item in value
    )
    if normalized != tuple(sorted(set(normalized))):
        raise ValueError(f"{field} must be sorted and unique")
    return normalized


def _exact_mapping(
    value: Any,
    *,
    field: str,
    required: tuple[str, ...],
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be an object")
    keys = set(value)
    expected = set(required)
    missing = sorted(expected - keys)
    unknown = sorted(keys - expected)
    if missing:
        raise ValueError(f"{field} is missing required fields: {', '.join(missing)}")
    if unknown:
        raise ValueError(f"{field} contains unknown fields: {', '.join(unknown)}")
    return value


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _deep_freeze(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


@dataclass(frozen=True)
class PersonalResearchSkillContract:
    """Versioned definition of one score-producing personal Skill."""

    skill_id: str
    version: str
    score_field: str
    required_snapshot_fields: tuple[str, ...]
    purpose: str
    rules: tuple[str, ...]

    def __post_init__(self) -> None:
        skill_id = _identifier(self.skill_id, field="skill_id")
        version = _version(self.version, field="skill version")
        if self.score_field not in PERSONAL_RESEARCH_SCORE_FIELDS:
            raise ValueError("score_field must use the approved DecisionSignal score taxonomy")
        fields = tuple(self.required_snapshot_fields)
        if not fields or fields[0] != "research_snapshot_hash":
            raise ValueError("required_snapshot_fields must begin with research_snapshot_hash")
        if any(item not in _SNAPSHOT_FIELDS for item in fields):
            raise ValueError("required_snapshot_fields contains an unsupported lineage field")
        if len(fields) != len(set(fields)):
            raise ValueError("required_snapshot_fields must be unique")
        purpose = _bounded_text(self.purpose, field="purpose", maximum=500)
        rules = tuple(
            _bounded_text(item, field="rule", maximum=500) for item in self.rules
        )
        if not rules or len(rules) > 8:
            raise ValueError("rules must contain between 1 and 8 items")
        object.__setattr__(self, "skill_id", skill_id)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "required_snapshot_fields", fields)
        object.__setattr__(self, "purpose", purpose)
        object.__setattr__(self, "rules", rules)

    def content_payload(self) -> dict[str, Any]:
        return {
            "schema_version": PERSONAL_SKILL_CONTRACT_SCHEMA_VERSION,
            "skill_id": self.skill_id,
            "version": self.version,
            "score_field": self.score_field,
            "required_snapshot_fields": list(self.required_snapshot_fields),
            "purpose": self.purpose,
            "rules": list(self.rules),
        }

    @property
    def content_hash(self) -> str:
        return canonical_hash(self.content_payload(), exclude_volatile=False)


def _contract(
    skill_id: str,
    score_field: str,
    required_snapshot_fields: tuple[str, ...],
    purpose: str,
    rules: tuple[str, ...],
) -> PersonalResearchSkillContract:
    return PersonalResearchSkillContract(
        skill_id=skill_id,
        version="1.0.0",
        score_field=score_field,
        required_snapshot_fields=required_snapshot_fields,
        purpose=purpose,
        rules=rules,
    )


_CONTRACTS = (
    _contract(
        "personal-value-quality",
        "value_quality_score",
        ("research_snapshot_hash", "factor_snapshot_hash"),
        "Assess valuation and business quality only from the frozen deterministic factor snapshot.",
        (
            "The score is the arithmetic mean of the frozen value and quality component scores, rounded to four decimals.",
            "Both frozen component scores and their factor claim/citation lineage are required; do not infer a missing metric as zero.",
            "Bind every result to the supplied immutable research and factor snapshots.",
        ),
    ),
    _contract(
        "personal-trend-timing",
        "trend_timing_score",
        ("research_snapshot_hash", "factor_snapshot_hash"),
        "Assess trend and timing only from the frozen deterministic factor snapshot.",
        (
            "The score equals the frozen trend_timing component score without an additional model adjustment.",
            "Do not use live prices newer than the research knowledge boundary.",
            "The frozen trend factor claim and citation are required.",
            "Bind every result to the supplied immutable research and factor snapshots.",
        ),
    ),
    _contract(
        "personal-catalyst",
        "catalyst_score",
        ("research_snapshot_hash", "factor_snapshot_hash"),
        "Assess bounded catalysts only from the frozen deterministic factor snapshot.",
        (
            "The score equals the frozen catalyst component score without an additional model adjustment.",
            "Do not introduce an event that is absent from the frozen snapshot lineage.",
            "The frozen catalyst factor claim and citation are required.",
            "Bind every result to the supplied immutable research and factor snapshots.",
        ),
    ),
    _contract(
        "personal-risk",
        "risk_score",
        ("research_snapshot_hash", "factor_snapshot_hash"),
        "Assess downside and data risk only from the frozen deterministic factor snapshot.",
        (
            "The score equals the frozen risk component score without inversion or an additional model adjustment.",
            "A higher score means higher risk and must never be inverted.",
            "The frozen risk factor claim and citation are required.",
            "Bind every result to the supplied immutable research and factor snapshots.",
        ),
    ),
    _contract(
        "personal-evidence-quality",
        "evidence_quality_score",
        ("research_snapshot_hash", "evidence_snapshot_hash"),
        "Assess citation coverage and evidence quality from the frozen typed Evidence snapshot.",
        (
            "The score equals frozen Evidence coverage multiplied by 100 and rounded to four decimals.",
            "Treat missing, stale, or uncited evidence as a limitation, never as support.",
            "At least one frozen claim with a frozen citation is required; references use a deterministic bounded claim/citation selection.",
            "Bind every result to the supplied immutable research and Evidence snapshots.",
        ),
    ),
)

PERSONAL_RESEARCH_SKILL_CONTRACTS: Mapping[str, PersonalResearchSkillContract] = (
    MappingProxyType({item.skill_id: item for item in _CONTRACTS})
)


def get_personal_research_skill_contract(skill_id: Any) -> PersonalResearchSkillContract:
    normalized = _identifier(skill_id, field="skill_id")
    try:
        return PERSONAL_RESEARCH_SKILL_CONTRACTS[normalized]
    except KeyError as exc:
        raise ValueError(f"unknown personal research skill: {normalized}") from exc


@dataclass(frozen=True)
class PersonalResearchSkillInput:
    """Canonical, lineage-bound input for one Skill execution."""

    contract: PersonalResearchSkillContract
    stock_code: str
    market: str
    snapshot_refs: Mapping[str, str]
    canonical_payload: Mapping[str, Any]
    canonical_json: str
    input_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.contract, PersonalResearchSkillContract):
            raise TypeError("contract must be a PersonalResearchSkillContract")
        stock_code = _bounded_text(self.stock_code, field="stock_code", maximum=64)
        market = _bounded_text(self.market, field="market", maximum=32).casefold()
        refs = dict(self.snapshot_refs)
        if set(refs) != set(self.contract.required_snapshot_fields):
            raise ValueError("snapshot_refs must exactly match the registered contract")
        normalized_refs = {
            field: _sha256(refs[field], field=field)
            for field in self.contract.required_snapshot_fields
        }
        expected_payload = canonicalize(
            {
                "schema_version": PERSONAL_SKILL_INPUT_SCHEMA_VERSION,
                "skill_id": self.contract.skill_id,
                "skill_version": self.contract.version,
                "skill_content_hash": self.contract.content_hash,
                "stock_code": stock_code,
                "market": market,
                **normalized_refs,
            },
            exclude_volatile=False,
        )
        if canonicalize(self.canonical_payload, exclude_volatile=False) != expected_payload:
            raise ValueError("skill input canonical_payload does not match its fields")
        expected_json = canonical_json(expected_payload, exclude_volatile=False)
        if self.canonical_json != expected_json:
            raise ValueError("skill input canonical_json does not match its payload")
        expected_hash = canonical_hash(expected_payload, exclude_volatile=False)
        if self.input_hash != expected_hash:
            raise ValueError("input_hash does not match the skill input payload")
        object.__setattr__(self, "stock_code", stock_code)
        object.__setattr__(self, "market", market)
        object.__setattr__(self, "snapshot_refs", _deep_freeze(normalized_refs))
        object.__setattr__(self, "canonical_payload", _deep_freeze(expected_payload))


def build_personal_research_skill_input(payload: Any) -> PersonalResearchSkillInput:
    """Validate an exact mapping and freeze its canonical execution identity."""

    if not isinstance(payload, Mapping):
        raise TypeError("skill input must be an object")
    if "skill_id" not in payload:
        raise ValueError("skill input is missing required fields: skill_id")
    contract = get_personal_research_skill_contract(payload["skill_id"])
    required = (
        "skill_id",
        "skill_version",
        "skill_content_hash",
        "stock_code",
        "market",
        *contract.required_snapshot_fields,
    )
    item = _exact_mapping(payload, field="skill input", required=required)
    if item["skill_version"] != contract.version:
        raise ValueError("skill_version does not match the registered contract")
    if item["skill_content_hash"] != contract.content_hash:
        raise ValueError("skill_content_hash does not match the registered contract")
    stock_code = _bounded_text(item["stock_code"], field="stock_code", maximum=64)
    market = _bounded_text(item["market"], field="market", maximum=32).casefold()
    refs = {
        field: _sha256(item[field], field=field)
        for field in contract.required_snapshot_fields
    }
    canonical_payload = canonicalize(
        {
            "schema_version": PERSONAL_SKILL_INPUT_SCHEMA_VERSION,
            "skill_id": contract.skill_id,
            "skill_version": contract.version,
            "skill_content_hash": contract.content_hash,
            "stock_code": stock_code,
            "market": market,
            **refs,
        },
        exclude_volatile=False,
    )
    return PersonalResearchSkillInput(
        contract=contract,
        stock_code=stock_code,
        market=market,
        snapshot_refs=_deep_freeze(refs),
        canonical_payload=_deep_freeze(canonical_payload),
        canonical_json=canonical_json(canonical_payload, exclude_volatile=False),
        input_hash=canonical_hash(canonical_payload, exclude_volatile=False),
    )


@dataclass(frozen=True)
class PersonalResearchSkillOutput:
    """Canonical successful output that projects onto one DecisionSignal field."""

    skill_input: PersonalResearchSkillInput
    score: float
    evidence_refs: tuple[str, ...]
    reason_codes: tuple[str, ...]
    canonical_payload: Mapping[str, Any]
    canonical_json: str
    output_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.skill_input, PersonalResearchSkillInput):
            raise TypeError("skill_input must be a PersonalResearchSkillInput")
        score = _score(self.score, field=self.skill_input.contract.score_field)
        evidence_refs = _sorted_unique_references(
            self.evidence_refs,
            field="evidence_refs",
        )
        reason_codes = _sorted_unique_identifiers(
            self.reason_codes,
            field="reason_codes",
            maximum_items=16,
            allow_empty=True,
        )
        expected_payload = canonicalize(
            {
                "schema_version": PERSONAL_SKILL_OUTPUT_SCHEMA_VERSION,
                "skill_id": self.skill_input.contract.skill_id,
                "skill_version": self.skill_input.contract.version,
                "skill_content_hash": self.skill_input.contract.content_hash,
                "score_field": self.skill_input.contract.score_field,
                "input_hash": self.skill_input.input_hash,
                "score": score,
                "evidence_refs": list(evidence_refs),
                "reason_codes": list(reason_codes),
            },
            exclude_volatile=False,
        )
        if canonicalize(self.canonical_payload, exclude_volatile=False) != expected_payload:
            raise ValueError("skill output canonical_payload does not match its fields")
        expected_json = canonical_json(expected_payload, exclude_volatile=False)
        if self.canonical_json != expected_json:
            raise ValueError("skill output canonical_json does not match its payload")
        expected_hash = canonical_hash(expected_payload, exclude_volatile=False)
        if self.output_hash != expected_hash:
            raise ValueError("output_hash does not match the skill output payload")
        object.__setattr__(self, "score", score)
        object.__setattr__(self, "evidence_refs", evidence_refs)
        object.__setattr__(self, "reason_codes", reason_codes)
        object.__setattr__(self, "canonical_payload", _deep_freeze(expected_payload))

    def to_decision_signal_fields(self) -> dict[str, Any]:
        return {
            self.skill_input.contract.score_field: self.score,
            "evidence_refs": list(self.evidence_refs),
        }


def build_personal_research_skill_output(
    skill_input: PersonalResearchSkillInput,
    payload: Any,
) -> PersonalResearchSkillOutput:
    """Validate a successful Skill result and bind it to the exact input hash."""

    if not isinstance(skill_input, PersonalResearchSkillInput):
        raise TypeError("skill_input must be a PersonalResearchSkillInput")
    item = _exact_mapping(
        payload,
        field="skill output",
        required=("score", "evidence_refs", "reason_codes"),
    )
    score = _score(item["score"], field=skill_input.contract.score_field)
    evidence_refs = _sorted_unique_references(item["evidence_refs"], field="evidence_refs")
    reason_codes = _sorted_unique_identifiers(
        item["reason_codes"],
        field="reason_codes",
        maximum_items=16,
        allow_empty=True,
    )
    canonical_payload = canonicalize(
        {
            "schema_version": PERSONAL_SKILL_OUTPUT_SCHEMA_VERSION,
            "skill_id": skill_input.contract.skill_id,
            "skill_version": skill_input.contract.version,
            "skill_content_hash": skill_input.contract.content_hash,
            "score_field": skill_input.contract.score_field,
            "input_hash": skill_input.input_hash,
            "score": score,
            "evidence_refs": list(evidence_refs),
            "reason_codes": list(reason_codes),
        },
        exclude_volatile=False,
    )
    return PersonalResearchSkillOutput(
        skill_input=skill_input,
        score=score,
        evidence_refs=evidence_refs,
        reason_codes=reason_codes,
        canonical_payload=_deep_freeze(canonical_payload),
        canonical_json=canonical_json(canonical_payload, exclude_volatile=False),
        output_hash=canonical_hash(canonical_payload, exclude_volatile=False),
    )


__all__ = [
    "PERSONAL_RESEARCH_SCORE_FIELDS",
    "PERSONAL_RESEARCH_SKILL_CONTRACTS",
    "PERSONAL_RESEARCH_SKILL_IDS",
    "PERSONAL_SKILL_CONTRACT_SCHEMA_VERSION",
    "PERSONAL_SKILL_INPUT_SCHEMA_VERSION",
    "PERSONAL_SKILL_OUTPUT_SCHEMA_VERSION",
    "PersonalResearchSkillContract",
    "PersonalResearchSkillInput",
    "PersonalResearchSkillOutput",
    "build_personal_research_skill_input",
    "build_personal_research_skill_output",
    "get_personal_research_skill_contract",
]
