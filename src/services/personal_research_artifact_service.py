"""Runtime orchestration for immutable personal-research artifacts.

This module bridges the already frozen Research/Evidence/Debate graph to the
five deterministic personal Skills, the formal DecisionSignal fields, and the
terminal immutable Thesis.  It performs no provider or model calls.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from types import SimpleNamespace
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import and_, select

from src.repositories.personal_research_artifact_repo import (
    PersonalResearchDebateReviewRepository,
    PersonalResearchSkillExecutionRepository,
    PersonalResearchThesisRepository,
)
from src.services.research.canonical import canonical_hash, canonical_json, canonicalize
from src.services.research.debate_service import (
    hydrate_debate_request,
    hydrate_debate_snapshot,
)
from src.services.research.evidence_service import hydrate_evidence_snapshot
from src.services.research.personal_skill_contract import (
    PERSONAL_RESEARCH_SKILL_IDS,
)
from src.services.research.personal_skill_evaluator import (
    PersonalResearchSkillScorecard,
    build_personal_research_skill_scorecard,
    hydrate_personal_research_skill_scorecard,
)
from src.services.research.repositories import ResearchSnapshotRepository
from src.storage import (
    AnalysisHistory,
    DatabaseManager,
    DecisionSignalRecord,
    PersonalResearchThesisRecord,
)


class PersonalResearchArtifactRuntimeError(RuntimeError):
    """Raised when a formal personal-research lineage cannot be completed."""


@dataclass(frozen=True)
class FrozenPersonalResearchArtifacts:
    """All pre-LLM deterministic artifacts for one stock-scoped task."""

    task_id: str
    stock_code: str
    market: str
    research_snapshot_hash: str
    prompt_version: str
    scorecard: PersonalResearchSkillScorecard
    skill_execution_hashes: Mapping[str, str]
    debate_snapshot_hash: Optional[str]
    debate_review_hash: Optional[str]
    debate_verdict: Optional[str]
    catalysts: tuple[str, ...]
    invalidators: tuple[str, ...]
    unknowns: tuple[str, ...]
    evidence_refs: tuple[str, ...]

    def decision_signal_fields(self, legacy_action: Any) -> dict[str, Any]:
        """Return formal fields for the canonical DecisionSignal writer."""

        action = str(legacy_action or "").strip().casefold()
        account_action = {
            "buy": "open_candidate",
            "add": "add_candidate",
            "hold": "hold",
            "reduce": "reduce_candidate",
            "sell": "exit_candidate",
            "avoid": "observe",
            "watch": "observe",
            "alert": "observe",
        }.get(action, "observe")
        if self.debate_verdict == "bull":
            research_stance = "bullish"
        elif self.debate_verdict == "bear":
            research_stance = "bearish"
        elif self.debate_verdict == "balanced":
            research_stance = "watch"
        else:
            research_stance = {
                "buy": "bullish",
                "add": "bullish",
                "hold": "neutral",
                "reduce": "bearish",
                "sell": "bearish",
                "avoid": "avoid",
                "watch": "watch",
                "alert": "watch",
            }.get(action, "neutral")

        return {
            **self.scorecard.decision_signal_fields(),
            "research_snapshot_hash": self.research_snapshot_hash,
            "prompt_version": self.prompt_version,
            "research_stance": research_stance,
            "account_action": account_action,
            "catalysts": list(self.catalysts),
            "invalidators": list(self.invalidators),
            "unknowns": list(self.unknowns),
        }


@dataclass(frozen=True)
class PersonalResearchTerminalResume:
    """A fully validated durable report checkpoint loaded before any provider call."""

    result: Any
    history_id: int
    context_snapshot: Mapping[str, Any]
    artifacts: FrozenPersonalResearchArtifacts
    signal_item: Optional[Mapping[str, Any]]
    thesis_hash: Optional[str]


def _bounded_unique(values: Sequence[Any], *, maximum: int = 16) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            normalized.append(text[:500])
            seen.add(text)
        if len(normalized) >= maximum:
            break
    return tuple(normalized)


def _thesis_material(prepared: Any) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    evidence = prepared.evidence_snapshot
    debate = prepared.debate_snapshot
    if debate is not None:
        by_stance = {item.stance: item.turn for item in debate.turns}
        bull = by_stance.get("bull")
        bear = by_stance.get("bear")
        catalysts = _bounded_unique(
            [item.statement for item in (bull.arguments if bull is not None else ())]
        )
        invalidators = _bounded_unique(
            [item.statement for item in (bear.arguments if bear is not None else ())]
        )
        unknowns = _bounded_unique(
            [
                *(bull.open_questions if bull is not None else ()),
                *(bear.open_questions if bear is not None else ()),
                *debate.limitations,
            ]
        )
        return catalysts, invalidators, unknowns

    catalysts = _bounded_unique(
        claim.statement
        for claim in evidence.claims
        if claim.status in {"supported", "partial"}
    )
    invalidators = _bounded_unique(
        claim.statement for claim in evidence.claims if claim.status == "contradicted"
    )
    unknowns = _bounded_unique(
        [
            *(claim.statement for claim in evidence.claims if claim.status == "insufficient"),
            *(limitation for claim in evidence.claims for limitation in claim.limitations),
            *evidence.limitations,
        ]
    )
    return catalysts, invalidators, unknowns


_SKILL_EXECUTION_COLUMNS = {
    "personal-value-quality": "value_quality_execution_hash",
    "personal-trend-timing": "trend_timing_execution_hash",
    "personal-catalyst": "catalyst_execution_hash",
    "personal-risk": "risk_execution_hash",
    "personal-evidence-quality": "evidence_quality_execution_hash",
}


def _digest(value: Any, *, field: str) -> str:
    text = str(value or "")
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise PersonalResearchArtifactRuntimeError(
            f"persisted {field} is not a lowercase SHA-256 digest"
        )
    return text


def _canonical_object(value: Any, *, field: str) -> Mapping[str, Any]:
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise PersonalResearchArtifactRuntimeError(
            f"persisted {field} is invalid JSON"
        ) from exc
    if not isinstance(decoded, Mapping):
        raise PersonalResearchArtifactRuntimeError(
            f"persisted {field} must be an object"
        )
    if canonical_json(decoded, exclude_volatile=False) != str(value):
        raise PersonalResearchArtifactRuntimeError(
            f"persisted {field} is not canonical JSON"
        )
    return decoded


def _json_array(value: Any, *, field: str) -> tuple[Any, ...]:
    try:
        decoded = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise PersonalResearchArtifactRuntimeError(
            f"persisted {field} is invalid JSON"
        ) from exc
    if not isinstance(decoded, list):
        raise PersonalResearchArtifactRuntimeError(
            f"persisted {field} must be an array"
        )
    return tuple(decoded)


def _validate_skill_dataset_lineage(
    rows: Sequence[Any],
    *,
    evidence: Any,
) -> None:
    """Require every resumed Skill to consume the exact Evidence datasets."""

    expected = tuple(sorted(evidence.input_dataset_hashes))
    for row in rows:
        actual = _json_array(
            row.dataset_snapshot_hashes_json,
            field=f"{row.skill_id} dataset_snapshot_hashes",
        )
        if actual != expected:
            raise PersonalResearchArtifactRuntimeError(
                "persisted personal Skill dataset lineage differs from Evidence"
            )


def _validate_debate_review_row(
    row: Any,
    *,
    task_id: str,
    stock_code: str,
    market: str,
    debate_snapshot_hash: str,
    evidence_snapshot_hash: str,
) -> None:
    """Cryptographically validate a persisted deterministic review."""

    expected_lineage = (
        task_id,
        stock_code,
        market,
        debate_snapshot_hash,
        evidence_snapshot_hash,
    )
    actual_lineage = (
        str(getattr(row, "task_id", "") or ""),
        str(getattr(row, "stock_code", "") or ""),
        str(getattr(row, "market", "") or "").casefold(),
        str(getattr(row, "debate_snapshot_hash", "") or ""),
        str(getattr(row, "evidence_snapshot_hash", "") or ""),
    )
    if actual_lineage != expected_lineage:
        raise PersonalResearchArtifactRuntimeError(
            "persisted Debate review lineage differs from the frozen task"
        )
    verifier_input = _canonical_object(
        row.verifier_input_json,
        field="Debate verifier input",
    )
    verifier_output = _canonical_object(
        row.verifier_output_json,
        field="Debate verifier output",
    )
    judge_input = _canonical_object(
        row.judge_input_json,
        field="Debate judge input",
    )
    judge_output = _canonical_object(
        row.judge_output_json,
        field="Debate judge output",
    )
    verifier_input_hash = canonical_hash(verifier_input, exclude_volatile=False)
    verifier_output_hash = canonical_hash(verifier_output, exclude_volatile=False)
    judge_input_hash = canonical_hash(judge_input, exclude_volatile=False)
    judge_output_hash = canonical_hash(judge_output, exclude_volatile=False)
    expected_hashes = (
        verifier_input_hash,
        verifier_output_hash,
        judge_input_hash,
        judge_output_hash,
    )
    actual_hashes = (
        _digest(row.verifier_input_hash, field="verifier_input_hash"),
        _digest(row.verifier_output_hash, field="verifier_output_hash"),
        _digest(row.judge_input_hash, field="judge_input_hash"),
        _digest(row.judge_output_hash, field="judge_output_hash"),
    )
    if actual_hashes != expected_hashes:
        raise PersonalResearchArtifactRuntimeError(
            "persisted Debate review payload hashes differ"
        )
    if (
        verifier_input.get("debate_snapshot_hash") != debate_snapshot_hash
        or verifier_input.get("verifier_version") != row.verifier_version
        or verifier_input.get("expected_evidence_snapshot_hash")
        != evidence_snapshot_hash
        or verifier_output.get("debate_hash") != debate_snapshot_hash
        or verifier_output.get("evidence_snapshot_hash")
        != evidence_snapshot_hash
        or verifier_output.get("expected_evidence_snapshot_hash")
        != evidence_snapshot_hash
        or verifier_output.get("valid") is not True
        or verifier_output.get("fail_closed") is not False
        or bool(row.verifier_valid) is not True
        or bool(row.verifier_fail_closed) is not False
        or judge_input.get("debate_snapshot_hash") != debate_snapshot_hash
        or judge_input.get("verification_hash") != verifier_output_hash
        or judge_input.get("judge_policy_hash") != row.judge_policy_hash
        or canonical_hash(
            judge_input.get("judge_policy"),
            exclude_volatile=False,
        )
        != row.judge_policy_hash
        or judge_output.get("verification_hash") != verifier_output_hash
        or judge_output.get("judge_policy_hash") != row.judge_policy_hash
        or judge_output.get("judge_policy") != judge_input.get("judge_policy")
        or judge_output.get("verdict") != row.verdict
        or judge_output.get("fail_closed") is not False
        or bool(row.judge_fail_closed) is not False
        or row.verdict not in {"bull", "bear", "balanced"}
        or row.winner != (row.verdict if row.verdict in {"bull", "bear"} else None)
    ):
        raise PersonalResearchArtifactRuntimeError(
            "persisted Debate review failed closed or conflicts with its payload"
        )
    identity = canonicalize(
        {
            "schema_version": "personal-research-debate-review-v1",
            "task_id": task_id,
            "stock_code": stock_code,
            "market": market,
            "debate_snapshot_hash": debate_snapshot_hash,
            "evidence_snapshot_hash": evidence_snapshot_hash,
            "verifier_version": row.verifier_version,
            "verifier_input_hash": verifier_input_hash,
            "verifier_output_hash": verifier_output_hash,
            "judge_version": row.judge_version,
            "judge_policy_hash": row.judge_policy_hash,
            "judge_input_hash": judge_input_hash,
            "judge_output_hash": judge_output_hash,
            "verdict": row.verdict,
        },
        exclude_volatile=False,
    )
    if _digest(row.review_hash, field="review_hash") != canonical_hash(
        identity,
        exclude_volatile=False,
    ):
        raise PersonalResearchArtifactRuntimeError(
            "persisted Debate review identity hash differs"
        )


class PersonalResearchArtifactService:
    """Persist and cross-bind formal personal-research runtime artifacts."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self.db = db_manager or DatabaseManager.get_instance()
        self.skills = PersonalResearchSkillExecutionRepository(self.db)
        self.reviews = PersonalResearchDebateReviewRepository(self.db)
        self.theses = PersonalResearchThesisRepository(self.db)

    def load_completed_pre_llm(
        self,
        *,
        task_id: str,
        stock_code: str,
        market: str,
    ) -> Optional[FrozenPersonalResearchArtifacts]:
        """Load a complete frozen pre-LLM checkpoint without provider calls.

        A final Research row is located only through its immutable job-event
        binding.  A partially persisted five-Skill set is a valid pre-LLM crash
        point, so this reader returns ``None`` when none exist but fails closed
        when a supposedly complete terminal lineage is partial or inconsistent.
        """

        normalized_task = str(task_id or "").strip()
        normalized_stock = str(stock_code or "").strip()
        normalized_market = str(market or "").strip().casefold()
        if not normalized_task or not normalized_stock or not normalized_market:
            raise PersonalResearchArtifactRuntimeError(
                "resume requires task, stock, and market identities"
            )
        repository = ResearchSnapshotRepository(self.db)
        research = repository.get_job_research_snapshot(
            job_id=normalized_task,
            stock_code=normalized_stock,
        )
        if research is None:
            return None
        if (
            str(research.get("stock_code") or "") != normalized_stock
            or str(research.get("market") or "").casefold() != normalized_market
        ):
            raise PersonalResearchArtifactRuntimeError(
                "job-bound Research snapshot has different stock lineage"
            )
        research_hash = _digest(
            research.get("snapshot_hash"),
            field="research_snapshot_hash",
        )
        factor_hash = _digest(
            research.get("factor_snapshot_hash"),
            field="factor_snapshot_hash",
        )
        evidence_hash = _digest(
            research.get("evidence_snapshot_hash"),
            field="evidence_snapshot_hash",
        )
        prompt_version = str(research.get("prompt_version") or "").strip()
        if not prompt_version:
            raise PersonalResearchArtifactRuntimeError(
                "job-bound Research snapshot has no prompt version"
            )

        rows = self.skills.list_for_task_stock(
            task_id=normalized_task,
            market=normalized_market,
            stock_code=normalized_stock,
        )
        if not rows:
            return None
        if len(rows) != len(PERSONAL_RESEARCH_SKILL_IDS):
            raise PersonalResearchArtifactRuntimeError(
                "terminal resume found a partial personal Skill checkpoint"
            )
        scorecard = hydrate_personal_research_skill_scorecard(rows)
        if (
            scorecard.stock_code != normalized_stock
            or scorecard.market.casefold() != normalized_market
            or scorecard.research_snapshot_hash != research_hash
            or scorecard.factor_snapshot_hash != factor_hash
            or scorecard.evidence_snapshot_hash != evidence_hash
        ):
            raise PersonalResearchArtifactRuntimeError(
                "persisted personal Skill scorecard differs from Research lineage"
            )
        execution_hashes = {
            str(row.skill_id): _digest(
                row.execution_hash,
                field=f"{row.skill_id} execution_hash",
            )
            for row in rows
        }

        evidence_record = repository.get_evidence(evidence_hash)
        if evidence_record is None:
            raise PersonalResearchArtifactRuntimeError(
                "Research snapshot references missing Evidence"
            )
        evidence = hydrate_evidence_snapshot(evidence_record)
        if (
            evidence.stock_code != normalized_stock
            or evidence.market.casefold() != normalized_market
            or evidence.evidence_hash != evidence_hash
            or evidence.factor_snapshot_hash != factor_hash
        ):
            raise PersonalResearchArtifactRuntimeError(
                "persisted Evidence differs from Research lineage"
            )
        _validate_skill_dataset_lineage(rows, evidence=evidence)

        debate_hash_value = research.get("debate_snapshot_hash")
        debate = None
        review_hash: Optional[str] = None
        review_verdict: Optional[str] = None
        if debate_hash_value is not None:
            debate_hash = _digest(
                debate_hash_value,
                field="debate_snapshot_hash",
            )
            debate_record = repository.get_debate_snapshot(debate_hash)
            if debate_record is None:
                raise PersonalResearchArtifactRuntimeError(
                    "Research snapshot references missing Debate"
                )
            request_hash = _digest(
                debate_record.get("request_hash"),
                field="debate_request_hash",
            )
            request_record = repository.get_debate_request(request_hash)
            if request_record is None:
                raise PersonalResearchArtifactRuntimeError(
                    "Debate snapshot references missing request"
                )
            request = hydrate_debate_request(
                request_record,
                evidence_snapshot=evidence,
            )
            debate = hydrate_debate_snapshot(debate_record, request=request)
            if (
                debate.stock_code != normalized_stock
                or debate.market.casefold() != normalized_market
                or debate.debate_hash != debate_hash
                or debate.evidence_snapshot_hash != evidence_hash
            ):
                raise PersonalResearchArtifactRuntimeError(
                    "persisted Debate differs from Research lineage"
                )
            review = self.reviews.get_for_task_stock(
                task_id=normalized_task,
                market=normalized_market,
                stock_code=normalized_stock,
                debate_snapshot_hash=debate_hash,
            )
            if review is None:
                raise PersonalResearchArtifactRuntimeError(
                    "terminal resume is missing the formal Debate review"
                )
            _validate_debate_review_row(
                review,
                task_id=normalized_task,
                stock_code=normalized_stock,
                market=normalized_market,
                debate_snapshot_hash=debate_hash,
                evidence_snapshot_hash=evidence_hash,
            )
            review_hash = _digest(review.review_hash, field="review_hash")
            review_verdict = str(review.verdict)

        catalysts, invalidators, unknowns = _thesis_material(
            SimpleNamespace(
                evidence_snapshot=evidence,
                debate_snapshot=debate,
            )
        )
        signal_fields = scorecard.decision_signal_fields()
        return FrozenPersonalResearchArtifacts(
            task_id=normalized_task,
            stock_code=normalized_stock,
            market=normalized_market,
            research_snapshot_hash=research_hash,
            prompt_version=prompt_version,
            scorecard=scorecard,
            skill_execution_hashes=execution_hashes,
            debate_snapshot_hash=(debate.debate_hash if debate is not None else None),
            debate_review_hash=review_hash,
            debate_verdict=review_verdict,
            catalysts=catalysts,
            invalidators=invalidators,
            unknowns=unknowns,
            evidence_refs=tuple(signal_fields["evidence_refs"]),
        )

    def persist_pre_llm(
        self,
        *,
        prepared: Any,
        frozen_write: Any,
    ) -> FrozenPersonalResearchArtifacts:
        """Persist five Skills and, when present, a verified Debate review."""

        factors = getattr(prepared, "factors", None)
        factor_snapshot = getattr(prepared, "factor_snapshot", None)
        evidence = getattr(prepared, "evidence_snapshot", None)
        if factors is None or factor_snapshot is None or evidence is None:
            raise PersonalResearchArtifactRuntimeError(
                "personal research requires frozen Factor and Evidence snapshots"
            )
        task_id = str(getattr(getattr(prepared, "lease", None), "job_id", "") or "").strip()
        if not task_id:
            raise PersonalResearchArtifactRuntimeError(
                "personal research requires a durable task identity"
            )
        research_snapshot_hash = str(
            getattr(frozen_write, "snapshot_hash", "") or ""
        ).strip()
        prompt_version = str(
            getattr(getattr(frozen_write, "snapshot", None), "prompt_version", "")
            or ""
        ).strip()
        if len(research_snapshot_hash) != 64 or not prompt_version:
            raise PersonalResearchArtifactRuntimeError(
                "frozen Research snapshot does not expose its formal identity"
            )

        existing_rows = self.skills.list_for_task_stock(
            task_id=task_id,
            market=prepared.market,
            stock_code=prepared.stock_code,
        )
        existing_ids = {str(row.skill_id) for row in existing_rows}
        if len(existing_ids) != len(existing_rows) or not existing_ids.issubset(
            set(PERSONAL_RESEARCH_SKILL_IDS)
        ):
            raise PersonalResearchArtifactRuntimeError(
                "durable task contains ambiguous personal Skill executions"
            )
        execution_hashes: dict[str, str]
        if len(existing_rows) == len(PERSONAL_RESEARCH_SKILL_IDS):
            scorecard = hydrate_personal_research_skill_scorecard(
                existing_rows
            )
            if (
                scorecard.stock_code != prepared.stock_code
                or scorecard.market.casefold() != str(prepared.market).casefold()
                or scorecard.research_snapshot_hash != research_snapshot_hash
                or scorecard.factor_snapshot_hash != factor_snapshot.content_hash
                or scorecard.evidence_snapshot_hash != evidence.evidence_hash
            ):
                raise PersonalResearchArtifactRuntimeError(
                    "persisted personal Skill scorecard differs from the frozen inputs"
                )
            _validate_skill_dataset_lineage(existing_rows, evidence=evidence)
            execution_hashes = {
                str(row.skill_id): _digest(
                    row.execution_hash,
                    field=f"{row.skill_id} execution_hash",
                )
                for row in existing_rows
            }
        else:
            scorecard = build_personal_research_skill_scorecard(
                stock_code=prepared.stock_code,
                market=prepared.market,
                research_snapshot_hash=research_snapshot_hash,
                factor_snapshot_hash=factor_snapshot.content_hash,
                evidence_snapshot=evidence,
                factors=factors,
            )
            execution_hashes = {}
            # Replaying persist_success for an existing partial row invokes the
            # repository's immutable-field comparison; only missing Skills are
            # inserted, while a conflicting partial checkpoint fails closed.
            for skill_id in PERSONAL_RESEARCH_SKILL_IDS:
                output = scorecard.outputs[skill_id]
                write = self.skills.persist_success(
                    task_id=task_id,
                    skill_input=output.skill_input,
                    skill_output=output,
                    dataset_snapshot_hashes=tuple(
                        sorted(evidence.input_dataset_hashes)
                    ),
                    factor_snapshot_hash=factor_snapshot.content_hash,
                    evidence_snapshot_hash=evidence.evidence_hash,
                )
                execution_hashes[skill_id] = write.content_hash

        debate_snapshot = getattr(prepared, "debate_snapshot", None)
        debate_review_hash: Optional[str] = None
        debate_verdict: Optional[str] = None
        if debate_snapshot is not None:
            review_row = self.reviews.get_for_task_stock(
                task_id=task_id,
                market=prepared.market,
                stock_code=prepared.stock_code,
                debate_snapshot_hash=debate_snapshot.debate_hash,
            )
            if review_row is None:
                review_write = self.reviews.persist(
                    task_id=task_id,
                    snapshot=debate_snapshot,
                    expected_evidence_snapshot_hash=evidence.evidence_hash,
                )
                review_row = review_write.row
                if bool(review_row.verifier_fail_closed) or bool(
                    review_row.judge_fail_closed
                ):
                    raise PersonalResearchArtifactRuntimeError(
                        "formal Debate review failed closed"
                    )
                debate_review_hash = _digest(
                    review_write.content_hash,
                    field="review_hash",
                )
            else:
                _validate_debate_review_row(
                    review_row,
                    task_id=task_id,
                    stock_code=prepared.stock_code,
                    market=str(prepared.market).casefold(),
                    debate_snapshot_hash=debate_snapshot.debate_hash,
                    evidence_snapshot_hash=evidence.evidence_hash,
                )
                debate_review_hash = _digest(
                    review_row.review_hash,
                    field="review_hash",
                )
            debate_verdict = str(review_row.verdict)

        catalysts, invalidators, unknowns = _thesis_material(prepared)
        signal_fields = scorecard.decision_signal_fields()
        return FrozenPersonalResearchArtifacts(
            task_id=task_id,
            stock_code=prepared.stock_code,
            market=prepared.market,
            research_snapshot_hash=research_snapshot_hash,
            prompt_version=prompt_version,
            scorecard=scorecard,
            skill_execution_hashes=execution_hashes,
            debate_snapshot_hash=(
                debate_snapshot.debate_hash if debate_snapshot is not None else None
            ),
            debate_review_hash=debate_review_hash,
            debate_verdict=debate_verdict,
            catalysts=catalysts,
            invalidators=invalidators,
            unknowns=unknowns,
            evidence_refs=tuple(signal_fields["evidence_refs"]),
        )

    def load_terminal_resume(
        self,
        *,
        task_id: str,
        stock_code: str,
        market: str,
        report_type: str,
        query_id: str,
    ) -> Optional[PersonalResearchTerminalResume]:
        """Recover history, signal, and Thesis before invoking an LLM.

        The durable report is authoritative only when its complete immutable
        personal-research lineage can be proven.  A history without a signal is
        returned as a valid intermediate terminal checkpoint so the caller can
        run only the deterministic signal/Thesis tail.
        """

        normalized_task = str(task_id or "").strip()
        normalized_stock = str(stock_code or "").strip()
        normalized_market = str(market or "").strip().casefold()
        normalized_report = str(report_type or "").strip().casefold()
        normalized_query = str(query_id or "").strip()
        if not all(
            (
                normalized_task,
                normalized_stock,
                normalized_market,
                normalized_report,
                normalized_query,
            )
        ):
            raise PersonalResearchArtifactRuntimeError(
                "terminal resume requires complete durable identities"
            )
        with self.db.get_session() as session:
            history_rows = list(
                session.execute(
                    select(AnalysisHistory).where(
                        and_(
                            AnalysisHistory.job_id == normalized_task,
                            AnalysisHistory.code == normalized_stock,
                            AnalysisHistory.report_type == normalized_report,
                        )
                    )
                ).scalars()
            )
            if len(history_rows) > 1:
                raise PersonalResearchArtifactRuntimeError(
                    "durable task contains ambiguous analysis histories"
                )
            if not history_rows:
                return None
            history = history_rows[0]
            session.expunge(history)
        if str(history.query_id or "") != normalized_query:
            raise PersonalResearchArtifactRuntimeError(
                "durable analysis history query identity differs"
            )

        snapshot_builder = getattr(
            self.db,
            "_snapshot_frozen_analysis_history",
            None,
        )
        snapshot_restorer = getattr(
            self.db,
            "_restore_analysis_result_from_frozen_history",
            None,
        )
        if not callable(snapshot_builder) or not callable(snapshot_restorer):
            raise PersonalResearchArtifactRuntimeError(
                "database runtime cannot hydrate frozen analysis history"
            )
        frozen_history = snapshot_builder(history)
        raw_result = frozen_history.get("raw_result")
        context_snapshot = frozen_history.get("context_snapshot")
        columns = frozen_history.get("columns")
        if (
            not isinstance(raw_result, Mapping)
            or not isinstance(columns, Mapping)
            or not isinstance(context_snapshot, Mapping)
            or raw_result.get("code") != normalized_stock
            or raw_result.get("success") is False
        ):
            raise PersonalResearchArtifactRuntimeError(
                "durable analysis history is incomplete or conflicts with the task"
            )
        sentiment_score = columns.get("sentiment_score")
        trend_prediction = columns.get("trend_prediction")
        operation_advice = columns.get("operation_advice")
        if (
            isinstance(sentiment_score, bool)
            or not isinstance(sentiment_score, int)
            or not isinstance(trend_prediction, str)
            or not trend_prediction.strip()
            or not isinstance(operation_advice, str)
            or not operation_advice.strip()
        ):
            raise PersonalResearchArtifactRuntimeError(
                "durable analysis history has invalid conclusion columns"
            )
        from src.analyzer import AnalysisResult

        result = AnalysisResult(
            code=normalized_stock,
            name=str(columns.get("name") or raw_result.get("name") or normalized_stock),
            sentiment_score=sentiment_score,
            trend_prediction=trend_prediction,
            operation_advice=operation_advice,
        )
        snapshot_restorer(result, frozen_history)
        if not bool(getattr(result, "success", False)):
            raise PersonalResearchArtifactRuntimeError(
                "durable analysis history does not contain a successful report"
            )
        result.query_id = normalized_query

        artifacts = self.load_completed_pre_llm(
            task_id=normalized_task,
            stock_code=normalized_stock,
            market=normalized_market,
        )
        if artifacts is None:
            raise PersonalResearchArtifactRuntimeError(
                "durable analysis history is missing its pre-LLM artifacts"
            )
        setattr(result, "_personal_research_artifacts", artifacts)

        signal_row = self._load_terminal_signal_row(
            task_id=normalized_task,
            history_id=int(history.id),
            stock_code=normalized_stock,
            market=normalized_market,
        )
        signal_item: Optional[Mapping[str, Any]] = None
        if signal_row is not None:
            self._validate_terminal_signal_row(signal_row, artifacts=artifacts)
            from src.services.decision_signal_service import DecisionSignalService

            signal_item = DecisionSignalService(
                db_manager=self.db
            ).get_signal(int(signal_row.id))

        thesis_row = self._load_terminal_thesis_row(
            task_id=normalized_task,
            stock_code=normalized_stock,
            market=normalized_market,
        )
        thesis_hash: Optional[str] = None
        if thesis_row is not None:
            if signal_row is None or signal_item is None:
                raise PersonalResearchArtifactRuntimeError(
                    "terminal Thesis exists without its DecisionSignal"
                )
            self._validate_terminal_thesis_row(
                thesis_row,
                artifacts=artifacts,
                signal_row=signal_row,
            )
            thesis_hash = _digest(thesis_row.thesis_hash, field="thesis_hash")

        return PersonalResearchTerminalResume(
            result=result,
            history_id=int(history.id),
            context_snapshot=dict(context_snapshot),
            artifacts=artifacts,
            signal_item=signal_item,
            thesis_hash=thesis_hash,
        )

    def _load_terminal_signal_row(
        self,
        *,
        task_id: str,
        history_id: int,
        stock_code: str,
        market: str,
    ) -> Optional[DecisionSignalRecord]:
        with self.db.get_session() as session:
            rows = list(
                session.execute(
                    select(DecisionSignalRecord).where(
                        and_(
                            DecisionSignalRecord.source_report_id == history_id,
                            DecisionSignalRecord.stock_code == stock_code,
                            DecisionSignalRecord.market == market,
                        )
                    )
                ).scalars()
            )
            if len(rows) > 1:
                raise PersonalResearchArtifactRuntimeError(
                    "durable report contains ambiguous DecisionSignals"
                )
            if not rows:
                return None
            row = rows[0]
            idempotency_key = str(row.idempotency_key or "")
            if not idempotency_key.startswith(f"job:{task_id}:"):
                raise PersonalResearchArtifactRuntimeError(
                    "DecisionSignal is not bound to the durable task"
                )
            session.expunge(row)
            return row

    @staticmethod
    def _validate_terminal_signal_row(
        row: DecisionSignalRecord,
        *,
        artifacts: FrozenPersonalResearchArtifacts,
    ) -> None:
        fields = artifacts.scorecard.decision_signal_fields()
        expected_scores = {
            key: value
            for key, value in fields.items()
            if key != "evidence_refs"
        }
        actual_scores = {
            key: getattr(row, key)
            for key in expected_scores
        }
        if (
            str(row.trace_id or "") != artifacts.task_id
            or str(row.stock_code or "") != artifacts.stock_code
            or str(row.market or "").casefold() != artifacts.market
            or row.research_snapshot_hash != artifacts.research_snapshot_hash
            or row.prompt_version != artifacts.prompt_version
            or actual_scores != expected_scores
            or _json_array(row.catalysts_json, field="signal catalysts")
            != artifacts.catalysts
            or _json_array(row.invalidators_json, field="signal invalidators")
            != artifacts.invalidators
            or _json_array(row.unknowns_json, field="signal unknowns")
            != artifacts.unknowns
            or _json_array(row.evidence_refs_json, field="signal evidence_refs")
            != artifacts.evidence_refs
            or row.research_stance
            not in {"strong_bullish", "bullish", "watch", "neutral", "bearish", "avoid"}
            or row.account_action
            not in {
                "observe",
                "open_candidate",
                "add_candidate",
                "hold",
                "reduce_candidate",
                "exit_candidate",
            }
        ):
            raise PersonalResearchArtifactRuntimeError(
                "persisted DecisionSignal differs from frozen personal research"
            )
        policy_values = (
            row.policy_evaluation_hash,
            row.policy_version,
            row.policy_hash,
            row.portfolio_snapshot_ref,
        )
        if any(value is not None for value in policy_values) and not all(
            bool(value) for value in policy_values
        ):
            raise PersonalResearchArtifactRuntimeError(
                "persisted DecisionSignal has partial policy lineage"
            )
        if row.policy_evaluation_hash is not None:
            _digest(
                row.policy_evaluation_hash,
                field="signal policy_evaluation_hash",
            )
            _digest(row.policy_hash, field="signal policy_hash")

    def _load_terminal_thesis_row(
        self,
        *,
        task_id: str,
        stock_code: str,
        market: str,
    ) -> Optional[PersonalResearchThesisRecord]:
        with self.db.get_session() as session:
            rows = list(
                session.execute(
                    select(PersonalResearchThesisRecord).where(
                        and_(
                            PersonalResearchThesisRecord.task_id == task_id,
                            PersonalResearchThesisRecord.stock_code == stock_code,
                            PersonalResearchThesisRecord.market == market,
                        )
                    )
                ).scalars()
            )
            if len(rows) > 1:
                raise PersonalResearchArtifactRuntimeError(
                    "durable task contains ambiguous terminal Theses"
                )
            if not rows:
                return None
            session.expunge(rows[0])
            return rows[0]

    @staticmethod
    def _validate_terminal_thesis_row(
        row: PersonalResearchThesisRecord,
        *,
        artifacts: FrozenPersonalResearchArtifacts,
        signal_row: DecisionSignalRecord,
    ) -> None:
        execution_hashes = {
            skill_id: getattr(row, column)
            for skill_id, column in _SKILL_EXECUTION_COLUMNS.items()
        }
        scores_and_refs = artifacts.scorecard.decision_signal_fields()
        scores = {
            key: value
            for key, value in scores_and_refs.items()
            if key != "evidence_refs"
        }
        try:
            persisted_scores = json.loads(str(row.scores_json))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PersonalResearchArtifactRuntimeError(
                "terminal Thesis scores are invalid JSON"
            ) from exc
        if (
            row.research_snapshot_hash != artifacts.research_snapshot_hash
            or execution_hashes != dict(artifacts.skill_execution_hashes)
            or row.debate_snapshot_hash != artifacts.debate_snapshot_hash
            or row.debate_review_hash != artifacts.debate_review_hash
            or row.decision_signal_id != signal_row.id
            or row.policy_evaluation_hash != signal_row.policy_evaluation_hash
            or row.policy_version != signal_row.policy_version
            or row.policy_hash != signal_row.policy_hash
            or row.portfolio_snapshot_ref != signal_row.portfolio_snapshot_ref
            or row.stance != signal_row.research_stance
            or row.account_action != signal_row.account_action
            or persisted_scores != scores
            or _json_array(row.catalysts_json, field="Thesis catalysts")
            != artifacts.catalysts
            or _json_array(row.invalidators_json, field="Thesis invalidators")
            != artifacts.invalidators
            or _json_array(row.unknowns_json, field="Thesis unknowns")
            != artifacts.unknowns
            or _json_array(row.evidence_refs_json, field="Thesis evidence_refs")
            != artifacts.evidence_refs
        ):
            raise PersonalResearchArtifactRuntimeError(
                "terminal Thesis differs from its frozen lineage"
            )
        content = canonicalize(
            {
                "schema_version": "personal-research-thesis-content-v1",
                "thesis_version": row.thesis_version,
                "stance": row.stance,
                "account_action": row.account_action,
                "scores": scores,
                "catalysts": list(artifacts.catalysts),
                "invalidators": list(artifacts.invalidators),
                "unknowns": list(artifacts.unknowns),
                "evidence_refs": list(artifacts.evidence_refs),
            },
            exclude_volatile=False,
        )
        content_hash = canonical_hash(content, exclude_volatile=False)
        if (
            _canonical_object(
                row.canonical_content_json,
                field="Thesis canonical content",
            )
            != content
            or _digest(row.content_hash, field="Thesis content_hash")
            != content_hash
        ):
            raise PersonalResearchArtifactRuntimeError(
                "terminal Thesis canonical content differs"
            )
        identity = canonicalize(
            {
                "schema_version": "personal-research-thesis-v1",
                "thesis_version": row.thesis_version,
                "task_id": artifacts.task_id,
                "stock_code": artifacts.stock_code,
                "market": artifacts.market,
                "research_snapshot_hash": artifacts.research_snapshot_hash,
                "skill_execution_hashes": dict(artifacts.skill_execution_hashes),
                "debate_snapshot_hash": artifacts.debate_snapshot_hash,
                "debate_review_hash": artifacts.debate_review_hash,
                "decision_signal_id": signal_row.id,
                "policy_evaluation_hash": row.policy_evaluation_hash,
                "policy_version": row.policy_version,
                "policy_hash": row.policy_hash,
                "portfolio_snapshot_ref": row.portfolio_snapshot_ref,
                "content_hash": content_hash,
                "supersedes_thesis_hash": row.supersedes_thesis_hash,
            },
            exclude_volatile=False,
        )
        if _digest(row.thesis_hash, field="thesis_hash") != canonical_hash(
            identity,
            exclude_volatile=False,
        ):
            raise PersonalResearchArtifactRuntimeError(
                "terminal Thesis identity hash differs"
            )

    def persist_thesis(
        self,
        *,
        artifacts: FrozenPersonalResearchArtifacts,
        legacy_action: Any,
        signal_item: Optional[Mapping[str, Any]],
    ) -> Any:
        """Persist the terminal Thesis, optionally bound to a complete policy audit."""

        fields = artifacts.decision_signal_fields(legacy_action)
        decision_signal_id: Optional[int] = None
        policy_evaluation_hash: Optional[str] = None
        if signal_item is not None:
            signal_id = signal_item.get("id")
            policy_hash = signal_item.get("policy_evaluation_hash")
            portfolio_ref = signal_item.get("portfolio_snapshot_ref")
            if isinstance(signal_id, int) and not isinstance(signal_id, bool):
                decision_signal_id = signal_id
                if isinstance(policy_hash, str) and portfolio_ref:
                    policy_evaluation_hash = policy_hash
            fields["research_stance"] = signal_item.get(
                "research_stance", fields["research_stance"]
            )
            fields["account_action"] = signal_item.get(
                "account_action", fields["account_action"]
            )

        return self.theses.persist(
            task_id=artifacts.task_id,
            stock_code=artifacts.stock_code,
            market=artifacts.market,
            research_snapshot_hash=artifacts.research_snapshot_hash,
            skill_execution_hashes=artifacts.skill_execution_hashes,
            stance=fields["research_stance"],
            account_action=fields["account_action"],
            catalysts=artifacts.catalysts,
            invalidators=artifacts.invalidators,
            unknowns=artifacts.unknowns,
            evidence_refs=artifacts.evidence_refs,
            debate_snapshot_hash=artifacts.debate_snapshot_hash,
            debate_review_hash=artifacts.debate_review_hash,
            decision_signal_id=decision_signal_id,
            policy_evaluation_hash=policy_evaluation_hash,
        )


__all__ = [
    "FrozenPersonalResearchArtifacts",
    "PersonalResearchArtifactRuntimeError",
    "PersonalResearchArtifactService",
    "PersonalResearchTerminalResume",
]
