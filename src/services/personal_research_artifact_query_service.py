"""Read-only public contract for immutable personal-research artifacts."""

from __future__ import annotations

import json
from typing import Any, Optional

from src.repositories.personal_research_artifact_repo import (
    PersonalResearchDebateReviewRepository,
    PersonalResearchSkillExecutionRepository,
    PersonalResearchThesisRepository,
)
from src.services.research.personal_skill_contract import PERSONAL_RESEARCH_SKILL_IDS


class PersonalResearchArtifactNotFoundError(LookupError):
    """Raised when a requested immutable artifact identity does not exist."""


class PersonalResearchArtifactContractError(RuntimeError):
    """Raised when persisted data does not satisfy the public read contract."""


def _decode_json(value: Any, *, field: str, expected: type) -> Any:
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise PersonalResearchArtifactContractError(
            f"Persisted {field} is not valid JSON"
        ) from exc
    if not isinstance(decoded, expected):
        raise PersonalResearchArtifactContractError(
            f"Persisted {field} has the wrong JSON type"
        )
    return decoded


def _skill_execution(row: Any) -> dict[str, Any]:
    dataset_hashes = _decode_json(
        row.dataset_snapshot_hashes_json,
        field="dataset_snapshot_hashes_json",
        expected=list,
    )
    return {
        "contract": "personal-research-skill-execution",
        "version": "v1",
        "execution_hash": row.execution_hash,
        "skill_contract": {
            "skill_id": row.skill_id,
            "version": row.skill_version,
            "contract_hash": row.contract_hash,
            "score_field": row.score_field,
        },
        "lineage": {
            "task_id": row.task_id,
            "market": row.market,
            "stock_code": row.stock_code,
            "research_snapshot_hash": row.research_snapshot_hash,
            "factor_snapshot_hash": row.factor_snapshot_hash,
            "evidence_snapshot_hash": row.evidence_snapshot_hash,
            "dataset_snapshot_hashes": dataset_hashes,
            "dataset_lineage_hash": row.dataset_lineage_hash,
            "input_hash": row.input_hash,
            "output_hash": row.output_hash,
        },
        "input": _decode_json(
            row.canonical_input_json,
            field="canonical_input_json",
            expected=dict,
        ),
        "result": {
            "status": row.result_status,
            "score": row.score,
            "output": _decode_json(
                row.canonical_output_json,
                field="canonical_output_json",
                expected=dict,
            ),
        },
        "created_at": row.created_at,
    }


def _debate_review(row: Any) -> dict[str, Any]:
    return {
        "contract": "personal-research-debate-review",
        "version": "v1",
        "review_hash": row.review_hash,
        "lineage": {
            "task_id": row.task_id,
            "market": row.market,
            "stock_code": row.stock_code,
            "debate_snapshot_hash": row.debate_snapshot_hash,
            "evidence_snapshot_hash": row.evidence_snapshot_hash,
        },
        "verifier": {
            "version": row.verifier_version,
            "input_hash": row.verifier_input_hash,
            "output_hash": row.verifier_output_hash,
            "valid": row.verifier_valid,
            "fail_closed": row.verifier_fail_closed,
            "reason_codes": _decode_json(
                row.verifier_reason_codes_json,
                field="verifier_reason_codes_json",
                expected=list,
            ),
            "input": _decode_json(
                row.verifier_input_json,
                field="verifier_input_json",
                expected=dict,
            ),
            "output": _decode_json(
                row.verifier_output_json,
                field="verifier_output_json",
                expected=dict,
            ),
        },
        "judge": {
            "version": row.judge_version,
            "policy_hash": row.judge_policy_hash,
            "input_hash": row.judge_input_hash,
            "output_hash": row.judge_output_hash,
            "fail_closed": row.judge_fail_closed,
            "reason_codes": _decode_json(
                row.judge_reason_codes_json,
                field="judge_reason_codes_json",
                expected=list,
            ),
            "verdict": row.verdict,
            "winner": row.winner,
            "input": _decode_json(
                row.judge_input_json,
                field="judge_input_json",
                expected=dict,
            ),
            "output": _decode_json(
                row.judge_output_json,
                field="judge_output_json",
                expected=dict,
            ),
        },
        "created_at": row.created_at,
    }


def _thesis(row: Any) -> dict[str, Any]:
    return {
        "contract": "personal-research-thesis",
        "version": row.thesis_version,
        "thesis_hash": row.thesis_hash,
        "content_hash": row.content_hash,
        "lineage": {
            "task_id": row.task_id,
            "market": row.market,
            "stock_code": row.stock_code,
            "research_snapshot_hash": row.research_snapshot_hash,
            "skill_execution_hashes": {
                "personal-value-quality": row.value_quality_execution_hash,
                "personal-trend-timing": row.trend_timing_execution_hash,
                "personal-catalyst": row.catalyst_execution_hash,
                "personal-risk": row.risk_execution_hash,
                "personal-evidence-quality": row.evidence_quality_execution_hash,
            },
            "debate_snapshot_hash": row.debate_snapshot_hash,
            "debate_review_hash": row.debate_review_hash,
            "decision_signal_id": row.decision_signal_id,
            "policy_evaluation_hash": row.policy_evaluation_hash,
            "policy_version": row.policy_version,
            "policy_hash": row.policy_hash,
            "portfolio_snapshot_ref": row.portfolio_snapshot_ref,
            "supersedes_thesis_hash": row.supersedes_thesis_hash,
        },
        "stance": row.stance,
        "account_action": row.account_action,
        "scores": _decode_json(row.scores_json, field="scores_json", expected=dict),
        "catalysts": _decode_json(
            row.catalysts_json, field="catalysts_json", expected=list
        ),
        "invalidators": _decode_json(
            row.invalidators_json, field="invalidators_json", expected=list
        ),
        "unknowns": _decode_json(
            row.unknowns_json, field="unknowns_json", expected=list
        ),
        "evidence_refs": _decode_json(
            row.evidence_refs_json, field="evidence_refs_json", expected=list
        ),
        "content": _decode_json(
            row.canonical_content_json,
            field="canonical_content_json",
            expected=dict,
        ),
        "created_at": row.created_at,
    }


class PersonalResearchArtifactQueryService:
    """Query immutable PR4 artifacts without exposing ORM records."""

    def __init__(
        self,
        *,
        skill_repository: Optional[PersonalResearchSkillExecutionRepository] = None,
        review_repository: Optional[PersonalResearchDebateReviewRepository] = None,
        thesis_repository: Optional[PersonalResearchThesisRepository] = None,
    ) -> None:
        self.skills = skill_repository or PersonalResearchSkillExecutionRepository()
        self.reviews = review_repository or PersonalResearchDebateReviewRepository()
        self.theses = thesis_repository or PersonalResearchThesisRepository()

    def get_skill_execution(self, execution_hash: str) -> dict[str, Any]:
        row = self.skills.get_by_hash(execution_hash)
        if row is None:
            raise PersonalResearchArtifactNotFoundError(
                "Personal research Skill execution was not found"
            )
        return _skill_execution(row)

    def list_skill_executions(
        self,
        *,
        task_id: str,
        market: str,
        stock_code: str,
    ) -> dict[str, Any]:
        rows = self.skills.list_for_task_stock(
            task_id=task_id,
            market=market,
            stock_code=stock_code,
        )
        if not rows:
            raise PersonalResearchArtifactNotFoundError(
                "Personal research Skill executions were not found"
            )
        by_skill: dict[str, Any] = {}
        for row in rows:
            if row.skill_id not in PERSONAL_RESEARCH_SKILL_IDS:
                raise PersonalResearchArtifactContractError(
                    "Persisted Skill execution uses an unknown contract"
                )
            if row.skill_id in by_skill:
                raise PersonalResearchArtifactContractError(
                    "Persisted Skill executions contain a duplicate contract"
                )
            by_skill[row.skill_id] = row
        missing = [
            skill_id
            for skill_id in PERSONAL_RESEARCH_SKILL_IDS
            if skill_id not in by_skill
        ]
        ordered = [
            _skill_execution(by_skill[skill_id])
            for skill_id in PERSONAL_RESEARCH_SKILL_IDS
            if skill_id in by_skill
        ]
        first = rows[0]
        return {
            "contract": "personal-research-skill-execution-collection",
            "version": "v1",
            "lineage": {
                "task_id": first.task_id,
                "market": first.market,
                "stock_code": first.stock_code,
            },
            "expected_skill_ids": list(PERSONAL_RESEARCH_SKILL_IDS),
            "missing_skill_ids": missing,
            "complete": not missing,
            "executions": ordered,
        }

    def get_debate_review(self, review_hash: str) -> dict[str, Any]:
        row = self.reviews.get_by_hash(review_hash)
        if row is None:
            raise PersonalResearchArtifactNotFoundError(
                "Personal research Debate review was not found"
            )
        return _debate_review(row)

    def get_thesis(self, thesis_hash: str) -> dict[str, Any]:
        return self._require_thesis(self.theses.get_by_hash(thesis_hash))

    def get_latest_thesis_by_signal_id(
        self,
        decision_signal_id: int,
    ) -> dict[str, Any]:
        return self._require_thesis(
            self.theses.get_latest_by_signal_id(decision_signal_id)
        )

    def get_latest_thesis_for_task_stock(
        self,
        *,
        task_id: str,
        market: str,
        stock_code: str,
    ) -> dict[str, Any]:
        return self._require_thesis(
            self.theses.get_latest_for_task_stock(
                task_id=task_id,
                market=market,
                stock_code=stock_code,
            )
        )

    @staticmethod
    def _require_thesis(row: Any) -> dict[str, Any]:
        if row is None:
            raise PersonalResearchArtifactNotFoundError(
                "Personal research Thesis was not found"
            )
        return _thesis(row)


__all__ = [
    "PersonalResearchArtifactContractError",
    "PersonalResearchArtifactNotFoundError",
    "PersonalResearchArtifactQueryService",
]
