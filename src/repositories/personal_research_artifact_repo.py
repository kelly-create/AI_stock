"""Immutable persistence for personal Skill executions, Debate reviews, and Theses."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import and_, select

from src.services.research.canonical import canonical_hash, canonical_json, canonicalize
from src.services.research.debate_review import (
    DebateJudgePolicy,
    judge_bounded_debate,
    verify_bounded_debate,
)
from src.services.research.debate_service import FrozenDebateSnapshot
from src.services.research.personal_skill_contract import (
    PERSONAL_RESEARCH_SKILL_IDS,
    PersonalResearchSkillInput,
    PersonalResearchSkillOutput,
)
from src.storage import (
    AnalysisJobRecord,
    DatabaseManager,
    DecisionSignalRecord,
    PersonalResearchDebateReviewRecord,
    PersonalResearchSkillContractRecord,
    PersonalResearchSkillExecutionRecord,
    PersonalResearchThesisRecord,
    PortfolioPolicyEvaluationRecord,
    ResearchDebateSnapshotRecord,
    ResearchEvidenceSnapshotRecord,
    ResearchFactorSnapshotRecord,
    ResearchSnapshotRecord,
)


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_VERSION_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_THESIS_STANCES = frozenset(
    {"strong_bullish", "bullish", "watch", "neutral", "bearish", "avoid"}
)
_THESIS_ACTIONS = frozenset(
    {
        "observe",
        "open_candidate",
        "add_candidate",
        "hold",
        "reduce_candidate",
        "exit_candidate",
    }
)
_SKILL_EXECUTION_COLUMNS = {
    "personal-value-quality": "value_quality_execution_hash",
    "personal-trend-timing": "trend_timing_execution_hash",
    "personal-catalyst": "catalyst_execution_hash",
    "personal-risk": "risk_execution_hash",
    "personal-evidence-quality": "evidence_quality_execution_hash",
}


class PersonalResearchArtifactConflictError(RuntimeError):
    """Raised when a stable artifact identity is reused with different facts."""


@dataclass(frozen=True)
class PersonalResearchArtifactWriteResult:
    row: Any
    content_hash: str
    created: bool


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
        raise ValueError(f"{field} must be a bounded lowercase version")
    return normalized


def _sha256(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")
    return value


def _hashes(value: Any, *, field: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError(f"{field} must be an array")
    normalized = tuple(_sha256(item, field=f"{field} item") for item in value)
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    if len(normalized) > 128:
        raise ValueError(f"{field} must contain at most 128 items")
    if normalized != tuple(sorted(set(normalized))):
        raise ValueError(f"{field} must be sorted and unique")
    return normalized


def _identifier_list(value: Any, *, field: str, maximum: int = 32) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError(f"{field} must be an array")
    normalized = tuple(_identifier(item, field=f"{field} item") for item in value)
    if len(normalized) > maximum:
        raise ValueError(f"{field} must contain at most {maximum} items")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field} must not contain duplicates")
    return normalized


def _text_list(
    value: Any,
    *,
    field: str,
    maximum_items: int = 32,
    item_maximum: int = 500,
    sorted_unique: bool = False,
) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError(f"{field} must be an array")
    normalized = tuple(
        _bounded_text(item, field=f"{field} item", maximum=item_maximum)
        for item in value
    )
    if len(normalized) > maximum_items:
        raise ValueError(f"{field} must contain at most {maximum_items} items")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field} must not contain duplicates")
    if sorted_unique and normalized != tuple(sorted(normalized)):
        raise ValueError(f"{field} must be sorted")
    return normalized


def _json_array(values: Sequence[Any]) -> str:
    return canonical_json(list(values), exclude_volatile=False)


def _assert_same(row: Any, expected: Mapping[str, Any], *, artifact: str) -> None:
    mismatched = sorted(
        field
        for field, expected_value in expected.items()
        if getattr(row, field) != expected_value
    )
    if mismatched:
        raise PersonalResearchArtifactConflictError(
            f"{artifact} identity conflicts with immutable fields: "
            + ",".join(mismatched)
        )


def _detach(session: Any, row: Any) -> Any:
    session.flush()
    session.expunge(row)
    return row


class PersonalResearchSkillExecutionRepository:
    """Append terminal Skill executions after validating every lineage edge."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self.db = db_manager or DatabaseManager.get_instance()

    def persist_success(
        self,
        *,
        task_id: str,
        skill_input: PersonalResearchSkillInput,
        skill_output: PersonalResearchSkillOutput,
        dataset_snapshot_hashes: Sequence[str],
        factor_snapshot_hash: str,
        evidence_snapshot_hash: str,
    ) -> PersonalResearchArtifactWriteResult:
        if not isinstance(skill_input, PersonalResearchSkillInput):
            raise TypeError("skill_input must be a PersonalResearchSkillInput")
        if not isinstance(skill_output, PersonalResearchSkillOutput):
            raise TypeError("skill_output must be a PersonalResearchSkillOutput")
        if skill_output.skill_input != skill_input:
            raise ValueError("skill_output is not bound to skill_input")
        return self._persist(
            task_id=task_id,
            skill_input=skill_input,
            dataset_snapshot_hashes=dataset_snapshot_hashes,
            factor_snapshot_hash=factor_snapshot_hash,
            evidence_snapshot_hash=evidence_snapshot_hash,
            result_status="succeeded",
            canonical_output_json=skill_output.canonical_json,
            output_hash=skill_output.output_hash,
            score=skill_output.score,
        )

    def persist_failure(
        self,
        *,
        task_id: str,
        skill_input: PersonalResearchSkillInput,
        dataset_snapshot_hashes: Sequence[str],
        factor_snapshot_hash: str,
        evidence_snapshot_hash: str,
        error_code: str,
        reason_codes: Sequence[str],
    ) -> PersonalResearchArtifactWriteResult:
        if not isinstance(skill_input, PersonalResearchSkillInput):
            raise TypeError("skill_input must be a PersonalResearchSkillInput")
        normalized_error = _identifier(error_code, field="error_code")
        normalized_reasons = _identifier_list(reason_codes, field="reason_codes")
        output_payload = canonicalize(
            {
                "schema_version": "personal-research-skill-failure-v1",
                "skill_id": skill_input.contract.skill_id,
                "skill_version": skill_input.contract.version,
                "skill_content_hash": skill_input.contract.content_hash,
                "input_hash": skill_input.input_hash,
                "error_code": normalized_error,
                "reason_codes": list(normalized_reasons),
            },
            exclude_volatile=False,
        )
        return self._persist(
            task_id=task_id,
            skill_input=skill_input,
            dataset_snapshot_hashes=dataset_snapshot_hashes,
            factor_snapshot_hash=factor_snapshot_hash,
            evidence_snapshot_hash=evidence_snapshot_hash,
            result_status="failed",
            canonical_output_json=canonical_json(
                output_payload, exclude_volatile=False
            ),
            output_hash=canonical_hash(output_payload, exclude_volatile=False),
            score=None,
        )

    def _persist(
        self,
        *,
        task_id: str,
        skill_input: PersonalResearchSkillInput,
        dataset_snapshot_hashes: Sequence[str],
        factor_snapshot_hash: str,
        evidence_snapshot_hash: str,
        result_status: str,
        canonical_output_json: str,
        output_hash: str,
        score: Optional[float],
    ) -> PersonalResearchArtifactWriteResult:
        normalized_task = _bounded_text(task_id, field="task_id", maximum=64)
        normalized_stock = _bounded_text(
            skill_input.stock_code, field="stock_code", maximum=16
        )
        normalized_market = _bounded_text(
            skill_input.market, field="market", maximum=16
        ).casefold()
        dataset_hashes = _hashes(
            dataset_snapshot_hashes, field="dataset_snapshot_hashes"
        )
        factor_hash = _sha256(factor_snapshot_hash, field="factor_snapshot_hash")
        evidence_hash = _sha256(
            evidence_snapshot_hash, field="evidence_snapshot_hash"
        )
        research_hash = _sha256(
            skill_input.snapshot_refs["research_snapshot_hash"],
            field="research_snapshot_hash",
        )
        if "factor_snapshot_hash" in skill_input.snapshot_refs and (
            skill_input.snapshot_refs["factor_snapshot_hash"] != factor_hash
        ):
            raise ValueError("Skill input and execution bind different Factor snapshots")
        if "evidence_snapshot_hash" in skill_input.snapshot_refs and (
            skill_input.snapshot_refs["evidence_snapshot_hash"] != evidence_hash
        ):
            raise ValueError("Skill input and execution bind different Evidence snapshots")
        output_digest = _sha256(output_hash, field="output_hash")
        try:
            decoded_output = json.loads(canonical_output_json)
        except (TypeError, ValueError) as exc:
            raise ValueError("canonical_output_json must be valid JSON") from exc
        if not isinstance(decoded_output, dict) or canonical_json(
            decoded_output, exclude_volatile=False
        ) != canonical_output_json:
            raise ValueError("canonical_output_json must be a canonical object")
        if canonical_hash(decoded_output, exclude_volatile=False) != output_digest:
            raise ValueError("output_hash does not match canonical_output_json")

        dataset_json = _json_array(dataset_hashes)
        dataset_lineage_hash = canonical_hash(
            {"dataset_snapshot_hashes": list(dataset_hashes)},
            exclude_volatile=False,
        )
        identity_payload = canonicalize(
            {
                "schema_version": "personal-research-skill-execution-v1",
                "task_id": normalized_task,
                "stock_code": normalized_stock,
                "market": normalized_market,
                "skill_id": skill_input.contract.skill_id,
                "skill_version": skill_input.contract.version,
                "contract_hash": skill_input.contract.content_hash,
                "score_field": skill_input.contract.score_field,
                "research_snapshot_hash": research_hash,
                "factor_snapshot_hash": factor_hash,
                "evidence_snapshot_hash": evidence_hash,
                "dataset_snapshot_hashes": list(dataset_hashes),
                "dataset_lineage_hash": dataset_lineage_hash,
                "input_hash": skill_input.input_hash,
                "result_status": result_status,
                "output_hash": output_digest,
                "score": score,
            },
            exclude_volatile=False,
        )
        execution_hash = canonical_hash(identity_payload, exclude_volatile=False)
        values = {
            "execution_hash": execution_hash,
            "task_id": normalized_task,
            "stock_code": normalized_stock,
            "market": normalized_market,
            "skill_id": skill_input.contract.skill_id,
            "skill_version": skill_input.contract.version,
            "contract_hash": skill_input.contract.content_hash,
            "score_field": skill_input.contract.score_field,
            "research_snapshot_hash": research_hash,
            "factor_snapshot_hash": factor_hash,
            "evidence_snapshot_hash": evidence_hash,
            "dataset_snapshot_hashes_json": dataset_json,
            "dataset_lineage_hash": dataset_lineage_hash,
            "canonical_input_json": skill_input.canonical_json,
            "input_hash": skill_input.input_hash,
            "result_status": result_status,
            "canonical_output_json": canonical_output_json,
            "output_hash": output_digest,
            "score": score,
        }

        def _write(session: Any) -> PersonalResearchArtifactWriteResult:
            self._validate_lineage(session, values=values)
            existing = session.execute(
                select(PersonalResearchSkillExecutionRecord)
                .where(
                    and_(
                        PersonalResearchSkillExecutionRecord.task_id
                        == normalized_task,
                        PersonalResearchSkillExecutionRecord.market
                        == normalized_market,
                        PersonalResearchSkillExecutionRecord.stock_code
                        == normalized_stock,
                        PersonalResearchSkillExecutionRecord.skill_id
                        == skill_input.contract.skill_id,
                    )
                )
                .limit(1)
            ).scalar_one_or_none()
            if existing is not None:
                _assert_same(existing, values, artifact="Skill execution")
                return PersonalResearchArtifactWriteResult(
                    _detach(session, existing), execution_hash, False
                )
            hash_collision = session.execute(
                select(PersonalResearchSkillExecutionRecord)
                .where(
                    PersonalResearchSkillExecutionRecord.execution_hash
                    == execution_hash
                )
                .limit(1)
            ).scalar_one_or_none()
            if hash_collision is not None:
                _assert_same(hash_collision, values, artifact="Skill execution")
                return PersonalResearchArtifactWriteResult(
                    _detach(session, hash_collision), execution_hash, False
                )
            row = PersonalResearchSkillExecutionRecord(**values)
            session.add(row)
            return PersonalResearchArtifactWriteResult(
                _detach(session, row), execution_hash, True
            )

        return self.db._run_write_transaction(
            "persist immutable personal Skill execution", _write
        )

    def get_by_hash(
        self,
        execution_hash: str,
    ) -> Optional[PersonalResearchSkillExecutionRecord]:
        digest = _sha256(execution_hash, field="execution_hash")
        with self.db.get_session() as session:
            row = session.execute(
                select(PersonalResearchSkillExecutionRecord)
                .where(
                    PersonalResearchSkillExecutionRecord.execution_hash == digest
                )
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            session.expunge(row)
            return row

    def list_for_task_stock(
        self,
        *,
        task_id: str,
        market: str,
        stock_code: str,
    ) -> list[PersonalResearchSkillExecutionRecord]:
        normalized_task = _bounded_text(task_id, field="task_id", maximum=64)
        normalized_market = _bounded_text(
            market, field="market", maximum=16
        ).casefold()
        normalized_stock = _bounded_text(
            stock_code, field="stock_code", maximum=16
        )
        with self.db.get_session() as session:
            rows = list(
                session.execute(
                    select(PersonalResearchSkillExecutionRecord)
                    .where(
                        and_(
                            PersonalResearchSkillExecutionRecord.task_id
                            == normalized_task,
                            PersonalResearchSkillExecutionRecord.market
                            == normalized_market,
                            PersonalResearchSkillExecutionRecord.stock_code
                            == normalized_stock,
                        )
                    )
                    .order_by(PersonalResearchSkillExecutionRecord.skill_id)
                ).scalars()
            )
            for row in rows:
                session.expunge(row)
            return rows

    @staticmethod
    def _validate_lineage(session: Any, *, values: Mapping[str, Any]) -> None:
        if session.get(AnalysisJobRecord, values["task_id"]) is None:
            raise ValueError(f"Research task not found: {values['task_id']}")
        contract = session.execute(
            select(PersonalResearchSkillContractRecord)
            .where(
                and_(
                    PersonalResearchSkillContractRecord.skill_id
                    == values["skill_id"],
                    PersonalResearchSkillContractRecord.skill_version
                    == values["skill_version"],
                    PersonalResearchSkillContractRecord.contract_hash
                    == values["contract_hash"],
                )
            )
            .limit(1)
        ).scalar_one_or_none()
        if contract is None or contract.score_field != values["score_field"]:
            raise ValueError("Skill contract is not registered exactly")
        research = session.execute(
            select(ResearchSnapshotRecord)
            .where(
                ResearchSnapshotRecord.snapshot_hash
                == values["research_snapshot_hash"]
            )
            .limit(1)
        ).scalar_one_or_none()
        factor = session.execute(
            select(ResearchFactorSnapshotRecord)
            .where(
                ResearchFactorSnapshotRecord.content_hash
                == values["factor_snapshot_hash"]
            )
            .limit(1)
        ).scalar_one_or_none()
        evidence = session.execute(
            select(ResearchEvidenceSnapshotRecord)
            .where(
                ResearchEvidenceSnapshotRecord.evidence_hash
                == values["evidence_snapshot_hash"]
            )
            .limit(1)
        ).scalar_one_or_none()
        if research is None or factor is None or evidence is None:
            raise ValueError("Skill execution lineage references a missing snapshot")
        common = (values["stock_code"], values["market"])
        for name, row in (
            ("Research", research),
            ("Factor", factor),
            ("Evidence", evidence),
        ):
            if (row.stock_code, row.market.casefold()) != common:
                raise ValueError(f"{name} snapshot stock/market lineage differs")
        if (
            research.factor_snapshot_hash != factor.content_hash
            or research.evidence_snapshot_hash != evidence.evidence_hash
            or evidence.factor_snapshot_hash != factor.content_hash
        ):
            raise ValueError("Research, Factor, and Evidence lineage differs")
        try:
            factor_dataset_hashes = _hashes(
                json.loads(factor.input_dataset_hashes_json),
                field="Factor dataset lineage",
            )
            evidence_dataset_hashes = _hashes(
                json.loads(evidence.input_dataset_hashes_json),
                field="Evidence dataset lineage",
            )
            execution_dataset_hashes = _hashes(
                json.loads(values["dataset_snapshot_hashes_json"]),
                field="Skill execution dataset lineage",
            )
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ValueError("Dataset lineage is not canonical") from exc
        if evidence_dataset_hashes != execution_dataset_hashes:
            raise ValueError("Dataset lineage differs from Evidence")
        if not set(factor_dataset_hashes).issubset(execution_dataset_hashes):
            raise ValueError("Factor dataset lineage is not a Skill lineage subset")


class PersonalResearchDebateReviewRepository:
    """Persist the exact deterministic Verifier and Judge audit payloads."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self.db = db_manager or DatabaseManager.get_instance()

    def persist(
        self,
        *,
        task_id: str,
        snapshot: FrozenDebateSnapshot,
        expected_evidence_snapshot_hash: Optional[str] = None,
        policy: Optional[DebateJudgePolicy] = None,
    ) -> PersonalResearchArtifactWriteResult:
        if not isinstance(snapshot, FrozenDebateSnapshot):
            raise TypeError("snapshot must be a FrozenDebateSnapshot")
        normalized_task = _bounded_text(task_id, field="task_id", maximum=64)
        stock_code = _bounded_text(snapshot.stock_code, field="stock_code", maximum=16)
        market = _bounded_text(snapshot.market, field="market", maximum=16).casefold()
        expected_evidence = _sha256(
            expected_evidence_snapshot_hash or snapshot.evidence_snapshot_hash,
            field="expected_evidence_snapshot_hash",
        )
        active_policy = policy or DebateJudgePolicy()
        verification = verify_bounded_debate(
            snapshot,
            expected_evidence_snapshot_hash=expected_evidence,
        )
        judgement = judge_bounded_debate(
            snapshot,
            expected_evidence_snapshot_hash=expected_evidence,
            policy=active_policy,
        )
        verifier_input = canonicalize(
            {
                "schema_version": "personal-research-debate-verifier-input-v1",
                "verifier_version": verification.verifier_version,
                "debate_snapshot_hash": snapshot.debate_hash,
                "expected_evidence_snapshot_hash": expected_evidence,
            },
            exclude_volatile=False,
        )
        verifier_output = canonicalize(
            {
                "verifier_version": verification.verifier_version,
                "debate_hash": verification.debate_hash,
                "evidence_snapshot_hash": verification.evidence_snapshot_hash,
                "expected_evidence_snapshot_hash": expected_evidence,
                "valid": verification.valid,
                "fail_closed": verification.fail_closed,
                "reason_codes": list(verification.reason_codes),
            },
            exclude_volatile=False,
        )
        verifier_output_hash = canonical_hash(
            verifier_output, exclude_volatile=False
        )
        if verifier_output_hash != verification.verification_hash:
            raise ValueError("Verifier result hash does not match its canonical output")
        judge_input = canonicalize(
            {
                "schema_version": "personal-research-debate-judge-input-v1",
                "debate_snapshot_hash": snapshot.debate_hash,
                "verification_hash": verification.verification_hash,
                "judge_policy": active_policy.content_payload(),
                "judge_policy_hash": active_policy.content_hash,
            },
            exclude_volatile=False,
        )
        judge_output = canonicalize(
            {
                "judge_policy": active_policy.content_payload(),
                "judge_policy_hash": active_policy.content_hash,
                "verification_hash": verification.verification_hash,
                "verdict": judgement.verdict,
                "fail_closed": judgement.fail_closed,
                "bull_score": judgement.bull_score,
                "bear_score": judgement.bear_score,
                "margin": judgement.margin,
                "reason_codes": list(judgement.reason_codes),
            },
            exclude_volatile=False,
        )
        judge_output_hash = canonical_hash(judge_output, exclude_volatile=False)
        if judge_output_hash != judgement.judgement_hash:
            raise ValueError("Judge result hash does not match its canonical output")
        verifier_input_hash = canonical_hash(verifier_input, exclude_volatile=False)
        judge_input_hash = canonical_hash(judge_input, exclude_volatile=False)
        review_identity = canonicalize(
            {
                "schema_version": "personal-research-debate-review-v1",
                "task_id": normalized_task,
                "stock_code": stock_code,
                "market": market,
                "debate_snapshot_hash": snapshot.debate_hash,
                "evidence_snapshot_hash": snapshot.evidence_snapshot_hash,
                "verifier_version": verification.verifier_version,
                "verifier_input_hash": verifier_input_hash,
                "verifier_output_hash": verifier_output_hash,
                "judge_version": judgement.judge_version,
                "judge_policy_hash": judgement.judge_policy_hash,
                "judge_input_hash": judge_input_hash,
                "judge_output_hash": judge_output_hash,
                "verdict": judgement.verdict,
            },
            exclude_volatile=False,
        )
        review_hash = canonical_hash(review_identity, exclude_volatile=False)
        values = {
            "review_hash": review_hash,
            "task_id": normalized_task,
            "stock_code": stock_code,
            "market": market,
            "debate_snapshot_hash": snapshot.debate_hash,
            "evidence_snapshot_hash": snapshot.evidence_snapshot_hash,
            "verifier_version": verification.verifier_version,
            "verifier_input_json": canonical_json(
                verifier_input, exclude_volatile=False
            ),
            "verifier_input_hash": verifier_input_hash,
            "verifier_output_json": canonical_json(
                verifier_output, exclude_volatile=False
            ),
            "verifier_output_hash": verifier_output_hash,
            "verifier_valid": verification.valid,
            "verifier_fail_closed": verification.fail_closed,
            "verifier_reason_codes_json": _json_array(verification.reason_codes),
            "judge_version": judgement.judge_version,
            "judge_policy_hash": judgement.judge_policy_hash,
            "judge_input_json": canonical_json(judge_input, exclude_volatile=False),
            "judge_input_hash": judge_input_hash,
            "judge_output_json": canonical_json(judge_output, exclude_volatile=False),
            "judge_output_hash": judge_output_hash,
            "judge_fail_closed": judgement.fail_closed,
            "judge_reason_codes_json": _json_array(judgement.reason_codes),
            "verdict": judgement.verdict,
            "winner": judgement.verdict
            if judgement.verdict in {"bull", "bear"}
            else None,
        }

        def _write(session: Any) -> PersonalResearchArtifactWriteResult:
            if session.get(AnalysisJobRecord, normalized_task) is None:
                raise ValueError(f"Research task not found: {normalized_task}")
            debate = session.execute(
                select(ResearchDebateSnapshotRecord)
                .where(
                    ResearchDebateSnapshotRecord.debate_hash
                    == snapshot.debate_hash
                )
                .limit(1)
            ).scalar_one_or_none()
            if debate is None:
                raise ValueError("Debate snapshot is not persisted")
            if (
                debate.stock_code != stock_code
                or debate.market.casefold() != market
                or debate.evidence_snapshot_hash != snapshot.evidence_snapshot_hash
                or debate.canonical_json != snapshot.canonical_json
            ):
                raise ValueError("Persisted Debate differs from the review input")
            existing = session.execute(
                select(PersonalResearchDebateReviewRecord)
                .where(
                    and_(
                        PersonalResearchDebateReviewRecord.task_id
                        == normalized_task,
                        PersonalResearchDebateReviewRecord.market == market,
                        PersonalResearchDebateReviewRecord.stock_code == stock_code,
                        PersonalResearchDebateReviewRecord.debate_snapshot_hash
                        == snapshot.debate_hash,
                        PersonalResearchDebateReviewRecord.verifier_version
                        == verification.verifier_version,
                        PersonalResearchDebateReviewRecord.judge_version
                        == judgement.judge_version,
                        PersonalResearchDebateReviewRecord.judge_policy_hash
                        == judgement.judge_policy_hash,
                    )
                )
                .limit(1)
            ).scalar_one_or_none()
            if existing is not None:
                _assert_same(existing, values, artifact="Debate review")
                return PersonalResearchArtifactWriteResult(
                    _detach(session, existing), review_hash, False
                )
            row = PersonalResearchDebateReviewRecord(**values)
            session.add(row)
            return PersonalResearchArtifactWriteResult(
                _detach(session, row), review_hash, True
            )

        return self.db._run_write_transaction(
            "persist immutable personal Debate review", _write
        )

    def get_by_hash(
        self,
        review_hash: str,
    ) -> Optional[PersonalResearchDebateReviewRecord]:
        digest = _sha256(review_hash, field="review_hash")
        with self.db.get_session() as session:
            row = session.execute(
                select(PersonalResearchDebateReviewRecord)
                .where(PersonalResearchDebateReviewRecord.review_hash == digest)
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            session.expunge(row)
            return row

    def get_for_task_stock(
        self,
        *,
        task_id: str,
        market: str,
        stock_code: str,
        debate_snapshot_hash: str,
    ) -> Optional[PersonalResearchDebateReviewRecord]:
        """Read the unambiguous review for one task-bound Debate snapshot."""

        normalized_task = _bounded_text(task_id, field="task_id", maximum=64)
        normalized_market = _bounded_text(
            market,
            field="market",
            maximum=16,
        ).casefold()
        normalized_stock = _bounded_text(
            stock_code,
            field="stock_code",
            maximum=16,
        )
        debate_hash = _sha256(
            debate_snapshot_hash,
            field="debate_snapshot_hash",
        )
        with self.db.get_session() as session:
            rows = list(
                session.execute(
                    select(PersonalResearchDebateReviewRecord).where(
                        and_(
                            PersonalResearchDebateReviewRecord.task_id
                            == normalized_task,
                            PersonalResearchDebateReviewRecord.market
                            == normalized_market,
                            PersonalResearchDebateReviewRecord.stock_code
                            == normalized_stock,
                            PersonalResearchDebateReviewRecord.debate_snapshot_hash
                            == debate_hash,
                        )
                    )
                ).scalars()
            )
            if len(rows) > 1:
                raise PersonalResearchArtifactConflictError(
                    "durable task contains ambiguous Debate reviews"
                )
            if not rows:
                return None
            session.expunge(rows[0])
            return rows[0]


class PersonalResearchThesisRepository:
    """Create immutable Theses from exactly five successful Skill executions."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self.db = db_manager or DatabaseManager.get_instance()

    def persist(
        self,
        *,
        task_id: str,
        stock_code: str,
        market: str,
        research_snapshot_hash: str,
        skill_execution_hashes: Mapping[str, str],
        stance: str,
        account_action: str,
        catalysts: Sequence[str],
        invalidators: Sequence[str],
        unknowns: Sequence[str],
        evidence_refs: Sequence[str],
        thesis_version: str = "personal-research-thesis-v1",
        debate_snapshot_hash: Optional[str] = None,
        debate_review_hash: Optional[str] = None,
        decision_signal_id: Optional[int] = None,
        policy_evaluation_hash: Optional[str] = None,
        supersedes_thesis_hash: Optional[str] = None,
    ) -> PersonalResearchArtifactWriteResult:
        normalized_task = _bounded_text(task_id, field="task_id", maximum=64)
        normalized_stock = _bounded_text(stock_code, field="stock_code", maximum=16)
        normalized_market = _bounded_text(market, field="market", maximum=16).casefold()
        normalized_version = _version(thesis_version, field="thesis_version")
        normalized_research = _sha256(
            research_snapshot_hash, field="research_snapshot_hash"
        )
        normalized_stance = _identifier(stance, field="stance")
        if normalized_stance not in _THESIS_STANCES:
            raise ValueError("stance is not an approved Research Thesis stance")
        normalized_action = _identifier(account_action, field="account_action")
        if normalized_action not in _THESIS_ACTIONS:
            raise ValueError("account_action is not an approved account action")
        if not isinstance(skill_execution_hashes, Mapping) or set(
            skill_execution_hashes
        ) != set(PERSONAL_RESEARCH_SKILL_IDS):
            raise ValueError("skill_execution_hashes must contain exactly five Skill IDs")
        execution_hashes = {
            skill_id: _sha256(
                skill_execution_hashes[skill_id],
                field=f"skill_execution_hashes[{skill_id}]",
            )
            for skill_id in PERSONAL_RESEARCH_SKILL_IDS
        }
        normalized_catalysts = _text_list(catalysts, field="catalysts")
        normalized_invalidators = _text_list(invalidators, field="invalidators")
        normalized_unknowns = _text_list(unknowns, field="unknowns")
        normalized_evidence_refs = _text_list(
            evidence_refs,
            field="evidence_refs",
            item_maximum=128,
            sorted_unique=True,
        )
        if not normalized_evidence_refs:
            raise ValueError("evidence_refs must not be empty")
        has_debate = debate_snapshot_hash is not None or debate_review_hash is not None
        if has_debate and (
            debate_snapshot_hash is None or debate_review_hash is None
        ):
            raise ValueError("Debate snapshot and review hashes must be supplied together")
        normalized_debate = (
            _sha256(debate_snapshot_hash, field="debate_snapshot_hash")
            if debate_snapshot_hash is not None
            else None
        )
        normalized_review = (
            _sha256(debate_review_hash, field="debate_review_hash")
            if debate_review_hash is not None
            else None
        )
        has_policy = policy_evaluation_hash is not None
        if has_policy and decision_signal_id is None:
            raise ValueError("Policy evaluation requires a DecisionSignal")
        if decision_signal_id is not None and (
            isinstance(decision_signal_id, bool)
            or not isinstance(decision_signal_id, int)
            or decision_signal_id <= 0
        ):
            raise ValueError("decision_signal_id must be a positive integer")
        normalized_policy_evaluation = (
            _sha256(policy_evaluation_hash, field="policy_evaluation_hash")
            if policy_evaluation_hash is not None
            else None
        )
        normalized_supersedes = (
            _sha256(supersedes_thesis_hash, field="supersedes_thesis_hash")
            if supersedes_thesis_hash is not None
            else None
        )

        def _write(session: Any) -> PersonalResearchArtifactWriteResult:
            if session.get(AnalysisJobRecord, normalized_task) is None:
                raise ValueError(f"Research task not found: {normalized_task}")
            research = session.execute(
                select(ResearchSnapshotRecord)
                .where(ResearchSnapshotRecord.snapshot_hash == normalized_research)
                .limit(1)
            ).scalar_one_or_none()
            if research is None:
                raise ValueError("Research snapshot is not persisted")
            if (
                research.stock_code != normalized_stock
                or research.market.casefold() != normalized_market
            ):
                raise ValueError("Research snapshot stock/market lineage differs")
            execution_rows = list(
                session.execute(
                    select(PersonalResearchSkillExecutionRecord).where(
                        PersonalResearchSkillExecutionRecord.execution_hash.in_(
                            tuple(execution_hashes.values())
                        )
                    )
                ).scalars()
            )
            if len(execution_rows) != len(PERSONAL_RESEARCH_SKILL_IDS):
                raise ValueError("One or more Skill executions are missing")
            execution_by_skill = {row.skill_id: row for row in execution_rows}
            if set(execution_by_skill) != set(PERSONAL_RESEARCH_SKILL_IDS):
                raise ValueError("Skill execution hashes do not cover exactly five Skills")
            for skill_id, row in execution_by_skill.items():
                if row.execution_hash != execution_hashes[skill_id]:
                    raise ValueError(f"Execution hash is bound to the wrong Skill: {skill_id}")
                if (
                    row.task_id != normalized_task
                    or row.stock_code != normalized_stock
                    or row.market != normalized_market
                    or row.research_snapshot_hash != normalized_research
                    or row.result_status != "succeeded"
                    or row.score is None
                ):
                    raise ValueError(f"Skill execution lineage differs: {skill_id}")
            scores = {
                row.score_field: row.score
                for row in sorted(execution_rows, key=lambda item: item.skill_id)
            }
            available_evidence_refs: set[str] = set()
            for row in execution_rows:
                output = json.loads(row.canonical_output_json)
                refs = output.get("evidence_refs")
                if isinstance(refs, list):
                    available_evidence_refs.update(
                        item for item in refs if isinstance(item, str)
                    )
            if not set(normalized_evidence_refs).issubset(available_evidence_refs):
                raise ValueError("Thesis evidence_refs are not reachable from Skill outputs")

            if normalized_review is not None:
                review = session.execute(
                    select(PersonalResearchDebateReviewRecord)
                    .where(
                        PersonalResearchDebateReviewRecord.review_hash
                        == normalized_review
                    )
                    .limit(1)
                ).scalar_one_or_none()
                if review is None or (
                    review.debate_snapshot_hash != normalized_debate
                    or review.task_id != normalized_task
                    or review.stock_code != normalized_stock
                    or review.market != normalized_market
                ):
                    raise ValueError("Debate review lineage differs from the Thesis")
                if review.verifier_fail_closed or review.judge_fail_closed:
                    raise ValueError("A fail-closed Debate review cannot support a Thesis")

            policy_version: Optional[str] = None
            policy_hash: Optional[str] = None
            portfolio_snapshot_ref: Optional[str] = None
            signal: Optional[DecisionSignalRecord] = None
            if decision_signal_id is not None:
                signal = session.get(DecisionSignalRecord, decision_signal_id)
                if signal is None:
                    raise ValueError("DecisionSignal is missing")
                expected_signal = (
                    normalized_task,
                    normalized_stock,
                    normalized_market,
                    normalized_research,
                    normalized_stance,
                    normalized_action,
                )
                actual_signal = (
                    signal.trace_id,
                    signal.stock_code,
                    signal.market.casefold(),
                    signal.research_snapshot_hash,
                    signal.research_stance,
                    signal.account_action,
                )
                if actual_signal != expected_signal:
                    raise ValueError("DecisionSignal lineage differs from the Thesis")
                for score_field, score in scores.items():
                    if getattr(signal, score_field) != score:
                        raise ValueError(
                            f"DecisionSignal score differs from Skill output: {score_field}"
                        )
            if normalized_policy_evaluation is not None:
                policy = session.execute(
                    select(PortfolioPolicyEvaluationRecord)
                    .where(
                        PortfolioPolicyEvaluationRecord.evaluation_hash
                        == normalized_policy_evaluation
                    )
                    .limit(1)
                ).scalar_one_or_none()
                if policy is None or signal is None:
                    raise ValueError("DecisionSignal or policy evaluation is missing")
                expected_common = (
                    normalized_stock,
                    normalized_market,
                    normalized_research,
                    normalized_stance,
                    normalized_action,
                )
                policy_common = (
                    policy.stock_code,
                    policy.market.casefold(),
                    policy.research_snapshot_hash,
                    policy.research_stance,
                    policy.final_account_action,
                )
                signal_common = (
                    signal.stock_code,
                    signal.market.casefold(),
                    signal.research_snapshot_hash,
                    signal.research_stance,
                    signal.account_action,
                )
                if policy_common != expected_common or signal_common != expected_common:
                    raise ValueError("DecisionSignal or policy lineage differs from the Thesis")
                if (
                    policy.signal_id != decision_signal_id
                    or policy.job_id != normalized_task
                    or signal.policy_evaluation_hash != normalized_policy_evaluation
                    or signal.policy_version != policy.policy_version
                    or signal.policy_hash != policy.policy_hash
                    or signal.portfolio_snapshot_ref != policy.portfolio_snapshot_ref
                ):
                    raise ValueError("DecisionSignal and policy evaluation lineage differs")
                policy_version = policy.policy_version
                policy_hash = policy.policy_hash
                portfolio_snapshot_ref = policy.portfolio_snapshot_ref
                if not portfolio_snapshot_ref:
                    raise ValueError("Policy lineage requires portfolio_snapshot_ref")

            if normalized_supersedes is not None:
                prior = session.execute(
                    select(PersonalResearchThesisRecord)
                    .where(
                        PersonalResearchThesisRecord.thesis_hash
                        == normalized_supersedes
                    )
                    .limit(1)
                ).scalar_one_or_none()
                if prior is None or (
                    prior.stock_code != normalized_stock
                    or prior.market != normalized_market
                ):
                    raise ValueError("Superseded Thesis stock/market lineage differs")

            content_payload = canonicalize(
                {
                    "schema_version": "personal-research-thesis-content-v1",
                    "thesis_version": normalized_version,
                    "stance": normalized_stance,
                    "account_action": normalized_action,
                    "scores": scores,
                    "catalysts": list(normalized_catalysts),
                    "invalidators": list(normalized_invalidators),
                    "unknowns": list(normalized_unknowns),
                    "evidence_refs": list(normalized_evidence_refs),
                },
                exclude_volatile=False,
            )
            content_hash = canonical_hash(content_payload, exclude_volatile=False)
            identity_payload = canonicalize(
                {
                    "schema_version": "personal-research-thesis-v1",
                    "thesis_version": normalized_version,
                    "task_id": normalized_task,
                    "stock_code": normalized_stock,
                    "market": normalized_market,
                    "research_snapshot_hash": normalized_research,
                    "skill_execution_hashes": execution_hashes,
                    "debate_snapshot_hash": normalized_debate,
                    "debate_review_hash": normalized_review,
                    "decision_signal_id": decision_signal_id,
                    "policy_evaluation_hash": normalized_policy_evaluation,
                    "policy_version": policy_version,
                    "policy_hash": policy_hash,
                    "portfolio_snapshot_ref": portfolio_snapshot_ref,
                    "content_hash": content_hash,
                    "supersedes_thesis_hash": normalized_supersedes,
                },
                exclude_volatile=False,
            )
            thesis_hash = canonical_hash(identity_payload, exclude_volatile=False)
            values = {
                "thesis_hash": thesis_hash,
                "thesis_version": normalized_version,
                "task_id": normalized_task,
                "stock_code": normalized_stock,
                "market": normalized_market,
                "research_snapshot_hash": normalized_research,
                **{
                    _SKILL_EXECUTION_COLUMNS[skill_id]: execution_hashes[skill_id]
                    for skill_id in PERSONAL_RESEARCH_SKILL_IDS
                },
                "debate_snapshot_hash": normalized_debate,
                "debate_review_hash": normalized_review,
                "decision_signal_id": decision_signal_id,
                "policy_evaluation_hash": normalized_policy_evaluation,
                "policy_version": policy_version,
                "policy_hash": policy_hash,
                "portfolio_snapshot_ref": portfolio_snapshot_ref,
                "stance": normalized_stance,
                "account_action": normalized_action,
                "scores_json": canonical_json(scores, exclude_volatile=False),
                "catalysts_json": _json_array(normalized_catalysts),
                "invalidators_json": _json_array(normalized_invalidators),
                "unknowns_json": _json_array(normalized_unknowns),
                "evidence_refs_json": _json_array(normalized_evidence_refs),
                "canonical_content_json": canonical_json(
                    content_payload, exclude_volatile=False
                ),
                "content_hash": content_hash,
                "supersedes_thesis_hash": normalized_supersedes,
            }
            existing = session.execute(
                select(PersonalResearchThesisRecord)
                .where(PersonalResearchThesisRecord.thesis_hash == thesis_hash)
                .limit(1)
            ).scalar_one_or_none()
            if existing is not None:
                _assert_same(existing, values, artifact="Research Thesis")
                return PersonalResearchArtifactWriteResult(
                    _detach(session, existing), thesis_hash, False
                )
            row = PersonalResearchThesisRecord(**values)
            session.add(row)
            return PersonalResearchArtifactWriteResult(
                _detach(session, row), thesis_hash, True
            )

        return self.db._run_write_transaction(
            "persist immutable personal Research Thesis", _write
        )

    def get_by_hash(self, thesis_hash: str) -> Optional[PersonalResearchThesisRecord]:
        digest = _sha256(thesis_hash, field="thesis_hash")
        with self.db.get_session() as session:
            row = session.execute(
                select(PersonalResearchThesisRecord)
                .where(PersonalResearchThesisRecord.thesis_hash == digest)
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            session.expunge(row)
            return row

    def get_latest_by_signal_id(
        self,
        decision_signal_id: int,
    ) -> Optional[PersonalResearchThesisRecord]:
        if (
            isinstance(decision_signal_id, bool)
            or not isinstance(decision_signal_id, int)
            or decision_signal_id <= 0
        ):
            raise ValueError("decision_signal_id must be a positive integer")
        with self.db.get_session() as session:
            row = session.execute(
                select(PersonalResearchThesisRecord)
                .where(
                    PersonalResearchThesisRecord.decision_signal_id
                    == decision_signal_id
                )
                .order_by(
                    PersonalResearchThesisRecord.created_at.desc(),
                    PersonalResearchThesisRecord.id.desc(),
                )
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            session.expunge(row)
            return row

    def get_latest_for_task_stock(
        self,
        *,
        task_id: str,
        market: str,
        stock_code: str,
    ) -> Optional[PersonalResearchThesisRecord]:
        normalized_task = _bounded_text(task_id, field="task_id", maximum=64)
        normalized_market = _bounded_text(
            market, field="market", maximum=16
        ).casefold()
        normalized_stock = _bounded_text(
            stock_code, field="stock_code", maximum=16
        )
        with self.db.get_session() as session:
            row = session.execute(
                select(PersonalResearchThesisRecord)
                .where(
                    and_(
                        PersonalResearchThesisRecord.task_id == normalized_task,
                        PersonalResearchThesisRecord.market == normalized_market,
                        PersonalResearchThesisRecord.stock_code == normalized_stock,
                    )
                )
                .order_by(
                    PersonalResearchThesisRecord.created_at.desc(),
                    PersonalResearchThesisRecord.id.desc(),
                )
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            session.expunge(row)
            return row


__all__ = [
    "PersonalResearchArtifactConflictError",
    "PersonalResearchArtifactWriteResult",
    "PersonalResearchDebateReviewRepository",
    "PersonalResearchSkillExecutionRepository",
    "PersonalResearchThesisRepository",
]
