"""Focused immutable-storage tests for original-plan personal research PR4."""

from __future__ import annotations

from datetime import timedelta
import json

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError

from src.repositories.personal_research_artifact_repo import (
    PersonalResearchArtifactConflictError,
    PersonalResearchDebateReviewRepository,
    PersonalResearchSkillExecutionRepository,
    PersonalResearchThesisRepository,
)
from src.analyzer import AnalysisResult
from src.services.personal_research_artifact_service import (
    PersonalResearchArtifactService,
)
from src.services.research.debate_review import DebateJudgePolicy
from src.services.research.personal_skill_contract import (
    PERSONAL_RESEARCH_SKILL_IDS,
    build_personal_research_skill_input,
    build_personal_research_skill_output,
    get_personal_research_skill_contract,
)
from src.services.research.personal_skill_evaluator import (
    PersonalResearchSkillEvaluationError,
    hydrate_personal_research_skill_scorecard,
)
from src.services.research.repositories import ResearchSnapshotRepository
from src.services.research.snapshot_service import persist_research_snapshot
from src.storage import (
    AnalysisHistory,
    AnalysisJobRecord,
    DecisionSignalRecord,
    PersonalResearchDebateReviewRecord,
    PersonalResearchSkillExecutionRecord,
    PersonalResearchThesisRecord,
    PortfolioPolicyEvaluationRecord,
)
from tests.test_research_debate_storage import (
    _build_graph,
    _research_snapshot_with_debate,
    _write_graph,
)
from tests.test_research_evidence_storage import NOW, _claim


pytest_plugins = ("tests.test_research_evidence_storage",)


@pytest.fixture()
def personal_graph(evidence_db):
    db, store = evidence_db
    lease = _claim(store, "personal-research-job", worker_id="personal-worker")
    research_repo = ResearchSnapshotRepository(db)
    dataset, factor, evidence, request, turns, debate = _build_graph(
        research_repo, lease
    )
    _write_graph(research_repo, lease, request, turns, debate)
    research = _research_snapshot_with_debate(
        factor=factor,
        evidence=evidence,
        debate=debate,
    )
    persisted = persist_research_snapshot(
        research,
        research_repo,
        lease=lease,
        now=NOW + timedelta(seconds=8),
    )
    assert persisted.content_hash == research.snapshot_hash
    return {
        "db": db,
        "task_id": lease.job_id,
        "dataset": dataset,
        "factor": factor,
        "evidence": evidence,
        "debate": debate,
        "research": research,
    }


def _persist_five_skills(graph):
    repository = PersonalResearchSkillExecutionRepository(graph["db"])
    executions = {}
    for index, skill_id in enumerate(PERSONAL_RESEARCH_SKILL_IDS):
        contract = get_personal_research_skill_contract(skill_id)
        input_payload = {
            "skill_id": contract.skill_id,
            "skill_version": contract.version,
            "skill_content_hash": contract.content_hash,
            "stock_code": "600519",
            "market": "A",
            "research_snapshot_hash": graph["research"].snapshot_hash,
        }
        if "factor_snapshot_hash" in contract.required_snapshot_fields:
            input_payload["factor_snapshot_hash"] = graph["factor"].content_hash
        if "evidence_snapshot_hash" in contract.required_snapshot_fields:
            input_payload["evidence_snapshot_hash"] = graph["evidence"].evidence_hash
        skill_input = build_personal_research_skill_input(input_payload)
        skill_output = build_personal_research_skill_output(
            skill_input,
            {
                "score": 60 + index,
                "evidence_refs": ["citation-1"],
                "reason_codes": ["bounded_evidence"],
            },
        )
        result = repository.persist_success(
            task_id=graph["task_id"],
            skill_input=skill_input,
            skill_output=skill_output,
            dataset_snapshot_hashes=[graph["dataset"].content_hash],
            factor_snapshot_hash=graph["factor"].content_hash,
            evidence_snapshot_hash=graph["evidence"].evidence_hash,
        )
        assert result.created is True
        executions[skill_id] = result
    return executions


def test_five_skill_executions_are_exactly_lineage_bound_and_idempotent(
    personal_graph,
) -> None:
    graph = personal_graph
    executions = _persist_five_skills(graph)
    repository = PersonalResearchSkillExecutionRepository(graph["db"])

    first_id = PERSONAL_RESEARCH_SKILL_IDS[0]
    contract = get_personal_research_skill_contract(first_id)
    skill_input = build_personal_research_skill_input(
        {
            "skill_id": contract.skill_id,
            "skill_version": contract.version,
            "skill_content_hash": contract.content_hash,
            "stock_code": "600519",
            "market": "A",
            "research_snapshot_hash": graph["research"].snapshot_hash,
            "factor_snapshot_hash": graph["factor"].content_hash,
        }
    )
    identical_output = build_personal_research_skill_output(
        skill_input,
        {
            "score": 60,
            "evidence_refs": ["citation-1"],
            "reason_codes": ["bounded_evidence"],
        },
    )
    duplicate = repository.persist_success(
        task_id=graph["task_id"],
        skill_input=skill_input,
        skill_output=identical_output,
        dataset_snapshot_hashes=[graph["dataset"].content_hash],
        factor_snapshot_hash=graph["factor"].content_hash,
        evidence_snapshot_hash=graph["evidence"].evidence_hash,
    )
    assert duplicate.created is False
    assert duplicate.content_hash == executions[first_id].content_hash
    assert repository.get_by_hash(duplicate.content_hash).id == duplicate.row.id
    assert [
        row.skill_id
        for row in repository.list_for_task_stock(
            task_id=graph["task_id"], market="A", stock_code="600519"
        )
    ] == sorted(PERSONAL_RESEARCH_SKILL_IDS)

    changed_output = build_personal_research_skill_output(
        skill_input,
        {
            "score": 61,
            "evidence_refs": ["citation-1"],
            "reason_codes": ["bounded_evidence"],
        },
    )
    with pytest.raises(
        PersonalResearchArtifactConflictError,
        match="immutable fields",
    ):
        repository.persist_success(
            task_id=graph["task_id"],
            skill_input=skill_input,
            skill_output=changed_output,
            dataset_snapshot_hashes=[graph["dataset"].content_hash],
            factor_snapshot_hash=graph["factor"].content_hash,
            evidence_snapshot_hash=graph["evidence"].evidence_hash,
        )

    with graph["db"].get_session() as session:
        rows = session.execute(
            select(PersonalResearchSkillExecutionRecord).order_by(
                PersonalResearchSkillExecutionRecord.skill_id
            )
        ).scalars().all()
    assert len(rows) == 5
    assert {row.skill_id for row in rows} == set(PERSONAL_RESEARCH_SKILL_IDS)
    assert all(row.result_status == "succeeded" for row in rows)
    assert all(row.dataset_snapshot_hashes_json == f'["{graph["dataset"].content_hash}"]' for row in rows)

    with graph["db"].session_scope() as session:
        session.add(
            AnalysisJobRecord(
                task_id="raw-lineage-tamper-job",
                job_type="research",
                stock_code="600519",
                trace_id="raw-lineage-tamper-trace",
            )
        )
    session = graph["db"].get_session()
    try:
        with pytest.raises(IntegrityError, match="skill lineage mismatch"):
            session.connection().exec_driver_sql(
                "INSERT INTO personal_research_skill_executions ("
                "execution_hash, task_id, stock_code, market, skill_id, "
                "skill_version, contract_hash, score_field, research_snapshot_hash, "
                "factor_snapshot_hash, evidence_snapshot_hash, "
                "dataset_snapshot_hashes_json, dataset_lineage_hash, "
                "canonical_input_json, input_hash, result_status, "
                "canonical_output_json, output_hash, score"
                ") SELECT ?, ?, stock_code, market, skill_id, skill_version, "
                "contract_hash, score_field, research_snapshot_hash, ?, "
                "evidence_snapshot_hash, dataset_snapshot_hashes_json, "
                "dataset_lineage_hash, canonical_input_json, input_hash, "
                "result_status, canonical_output_json, output_hash, score "
                "FROM personal_research_skill_executions WHERE id = ?",
                (
                    "f" * 64,
                    "raw-lineage-tamper-job",
                    "e" * 64,
                    executions[first_id].row.id,
                ),
            )
    finally:
        session.rollback()
        session.close()


def test_skill_repository_rejects_dataset_or_snapshot_lineage_drift(personal_graph) -> None:
    graph = personal_graph
    contract = get_personal_research_skill_contract("personal-value-quality")
    skill_input = build_personal_research_skill_input(
        {
            "skill_id": contract.skill_id,
            "skill_version": contract.version,
            "skill_content_hash": contract.content_hash,
            "stock_code": "600519",
            "market": "A",
            "research_snapshot_hash": graph["research"].snapshot_hash,
            "factor_snapshot_hash": graph["factor"].content_hash,
        }
    )
    skill_output = build_personal_research_skill_output(
        skill_input,
        {
            "score": 70,
            "evidence_refs": ["citation-1"],
            "reason_codes": [],
        },
    )
    repository = PersonalResearchSkillExecutionRepository(graph["db"])
    with pytest.raises(ValueError, match="Dataset lineage"):
        repository.persist_success(
            task_id=graph["task_id"],
            skill_input=skill_input,
            skill_output=skill_output,
            dataset_snapshot_hashes=["f" * 64],
            factor_snapshot_hash=graph["factor"].content_hash,
            evidence_snapshot_hash=graph["evidence"].evidence_hash,
        )
    with pytest.raises(ValueError, match="different Factor"):
        repository.persist_success(
            task_id=graph["task_id"],
            skill_input=skill_input,
            skill_output=skill_output,
            dataset_snapshot_hashes=[graph["dataset"].content_hash],
            factor_snapshot_hash="e" * 64,
            evidence_snapshot_hash=graph["evidence"].evidence_hash,
        )


def test_terminal_skill_failure_is_canonical_and_cannot_be_rebound(personal_graph) -> None:
    graph = personal_graph
    with graph["db"].session_scope() as session:
        session.add(
            AnalysisJobRecord(
                task_id="personal-research-failed-job",
                job_type="research",
                stock_code="600519",
                trace_id="personal-research-failed-trace",
            )
        )
    contract = get_personal_research_skill_contract("personal-evidence-quality")
    skill_input = build_personal_research_skill_input(
        {
            "skill_id": contract.skill_id,
            "skill_version": contract.version,
            "skill_content_hash": contract.content_hash,
            "stock_code": "600519",
            "market": "A",
            "research_snapshot_hash": graph["research"].snapshot_hash,
            "evidence_snapshot_hash": graph["evidence"].evidence_hash,
        }
    )
    repository = PersonalResearchSkillExecutionRepository(graph["db"])
    failure = repository.persist_failure(
        task_id="personal-research-failed-job",
        skill_input=skill_input,
        dataset_snapshot_hashes=[graph["dataset"].content_hash],
        factor_snapshot_hash=graph["factor"].content_hash,
        evidence_snapshot_hash=graph["evidence"].evidence_hash,
        error_code="provider_unavailable",
        reason_codes=["retry_exhausted"],
    )
    assert failure.row.result_status == "failed"
    assert failure.row.score is None
    assert '"error_code":"provider_unavailable"' in failure.row.canonical_output_json

    success_output = build_personal_research_skill_output(
        skill_input,
        {
            "score": 70,
            "evidence_refs": ["citation-1"],
            "reason_codes": [],
        },
    )
    with pytest.raises(PersonalResearchArtifactConflictError, match="immutable fields"):
        repository.persist_success(
            task_id="personal-research-failed-job",
            skill_input=skill_input,
            skill_output=success_output,
            dataset_snapshot_hashes=[graph["dataset"].content_hash],
            factor_snapshot_hash=graph["factor"].content_hash,
            evidence_snapshot_hash=graph["evidence"].evidence_hash,
        )


def test_debate_review_and_thesis_are_immutable_idempotent_and_superseding(
    personal_graph,
) -> None:
    graph = personal_graph
    executions = _persist_five_skills(graph)
    review_repo = PersonalResearchDebateReviewRepository(graph["db"])
    review = review_repo.persist(
        task_id=graph["task_id"],
        snapshot=graph["debate"],
        expected_evidence_snapshot_hash=graph["evidence"].evidence_hash,
    )
    duplicate_review = review_repo.persist(
        task_id=graph["task_id"],
        snapshot=graph["debate"],
        expected_evidence_snapshot_hash=graph["evidence"].evidence_hash,
    )
    assert review.created is True
    assert duplicate_review.created is False
    assert duplicate_review.content_hash == review.content_hash
    assert review_repo.get_by_hash(review.content_hash).id == review.row.id
    assert review.row.verdict == "balanced"
    assert review.row.winner is None

    thesis_repo = PersonalResearchThesisRepository(graph["db"])
    common = {
        "task_id": graph["task_id"],
        "stock_code": "600519",
        "market": "A",
        "research_snapshot_hash": graph["research"].snapshot_hash,
        "skill_execution_hashes": {
            skill_id: item.content_hash for skill_id, item in executions.items()
        },
        "stance": "bullish",
        "account_action": "hold",
        "catalysts": ["Evidence-bound catalyst remains observable."],
        "invalidators": ["The cited metric reverses."],
        "unknowns": ["Future persistence is unknown."],
        "evidence_refs": ["citation-1"],
        "debate_snapshot_hash": graph["debate"].debate_hash,
        "debate_review_hash": review.content_hash,
    }
    first = thesis_repo.persist(**common)
    duplicate = thesis_repo.persist(**common)
    second = thesis_repo.persist(
        **{
            **common,
            "unknowns": ["A newer bounded unknown remains."],
            "supersedes_thesis_hash": first.content_hash,
        }
    )
    assert first.created is True
    assert duplicate.created is False
    assert second.created is True
    assert second.row.supersedes_thesis_hash == first.content_hash
    assert thesis_repo.get_by_hash(first.content_hash).id == first.row.id
    latest = thesis_repo.get_latest_for_task_stock(
        task_id=graph["task_id"],
        market="A",
        stock_code="600519",
    )
    assert latest is not None
    assert latest.thesis_hash == second.content_hash

    with graph["db"].get_session() as session:
        assert session.execute(
            select(func.count(PersonalResearchThesisRecord.id))
        ).scalar_one() == 2

    with graph["db"].get_session() as session:
        persisted_execution = session.get(
            PersonalResearchSkillExecutionRecord,
            executions["personal-value-quality"].row.id,
        )
        persisted_review = session.get(
            PersonalResearchDebateReviewRecord,
            review.row.id,
        )
        persisted_thesis = session.get(PersonalResearchThesisRecord, first.row.id)
        persisted_execution.score = 1
        with pytest.raises(IntegrityError, match="skill execution is immutable"):
            session.commit()
        session.rollback()
        persisted_review = session.get(PersonalResearchDebateReviewRecord, review.row.id)
        persisted_review.judge_reason_codes_json = "[]"
        with pytest.raises(IntegrityError, match="debate review is immutable"):
            session.commit()
        session.rollback()
        persisted_thesis = session.get(PersonalResearchThesisRecord, first.row.id)
        persisted_thesis.stance = "neutral"
        with pytest.raises(IntegrityError, match="thesis is immutable"):
            session.commit()
        session.rollback()
        with pytest.raises(IntegrityError, match="thesis is immutable"):
            session.execute(
                delete(PersonalResearchThesisRecord).where(
                    PersonalResearchThesisRecord.id == first.row.id
                )
            )


def test_complete_pre_llm_artifacts_resume_from_job_bound_snapshot(
    personal_graph,
) -> None:
    graph = personal_graph
    executions = _persist_five_skills(graph)
    review = PersonalResearchDebateReviewRepository(graph["db"]).persist(
        task_id=graph["task_id"],
        snapshot=graph["debate"],
        expected_evidence_snapshot_hash=graph["evidence"].evidence_hash,
    )

    artifacts = PersonalResearchArtifactService(
        graph["db"]
    ).load_completed_pre_llm(
        task_id=graph["task_id"],
        stock_code="600519",
        market="A",
    )

    assert artifacts is not None
    assert artifacts.research_snapshot_hash == graph["research"].snapshot_hash
    assert artifacts.debate_snapshot_hash == graph["debate"].debate_hash
    assert artifacts.debate_review_hash == review.content_hash
    assert artifacts.skill_execution_hashes == {
        skill_id: write.content_hash for skill_id, write in executions.items()
    }


def test_resume_fails_closed_on_noncanonical_skill_output(personal_graph) -> None:
    graph = personal_graph
    _persist_five_skills(graph)
    rows = PersonalResearchSkillExecutionRepository(
        graph["db"]
    ).list_for_task_stock(
        task_id=graph["task_id"],
        market="A",
        stock_code="600519",
    )
    rows[0].canonical_output_json += " "

    with pytest.raises(
        PersonalResearchSkillEvaluationError,
        match="output is not canonical",
    ):
        hydrate_personal_research_skill_scorecard(rows)


def test_resume_fails_closed_on_ambiguous_debate_reviews(personal_graph) -> None:
    graph = personal_graph
    repository = PersonalResearchDebateReviewRepository(graph["db"])
    repository.persist(
        task_id=graph["task_id"],
        snapshot=graph["debate"],
        expected_evidence_snapshot_hash=graph["evidence"].evidence_hash,
    )
    repository.persist(
        task_id=graph["task_id"],
        snapshot=graph["debate"],
        expected_evidence_snapshot_hash=graph["evidence"].evidence_hash,
        policy=DebateJudgePolicy(decisive_margin=0.2),
    )

    with pytest.raises(
        PersonalResearchArtifactConflictError,
        match="ambiguous Debate reviews",
    ):
        repository.get_for_task_stock(
            task_id=graph["task_id"],
            market="A",
            stock_code="600519",
            debate_snapshot_hash=graph["debate"].debate_hash,
        )


def test_terminal_resume_hydrates_history_signal_and_thesis_without_recompute(
    personal_graph,
) -> None:
    graph = personal_graph
    _persist_five_skills(graph)
    PersonalResearchDebateReviewRepository(graph["db"]).persist(
        task_id=graph["task_id"],
        snapshot=graph["debate"],
        expected_evidence_snapshot_hash=graph["evidence"].evidence_hash,
    )
    service = PersonalResearchArtifactService(graph["db"])
    artifacts = service.load_completed_pre_llm(
        task_id=graph["task_id"],
        stock_code="600519",
        market="A",
    )
    assert artifacts is not None
    result = AnalysisResult(
        code="600519",
        name="Kweichow Moutai",
        sentiment_score=68,
        trend_prediction="bullish",
        operation_advice="hold",
        action="hold",
        analysis_summary="first committed report",
    )
    with graph["db"].session_scope() as session:
        history = AnalysisHistory(
            query_id=graph["task_id"],
            job_id=graph["task_id"],
            code="600519",
            name=result.name,
            report_type="full",
            sentiment_score=result.sentiment_score,
            operation_advice=result.operation_advice,
            trend_prediction=result.trend_prediction,
            analysis_summary=result.analysis_summary,
            raw_result=json.dumps(result.to_dict(), ensure_ascii=False),
            context_snapshot=json.dumps(
                {"market_phase_summary": {"session_date": "2026-08-08"}}
            ),
        )
        session.add(history)
        session.flush()
        history_id = int(history.id)
        formal = artifacts.decision_signal_fields("hold")
        signal = DecisionSignalRecord(
            stock_code="600519",
            stock_name=result.name,
            market="a",
            source_type="analysis",
            source_report_id=history_id,
            trace_id=graph["task_id"],
            idempotency_key=f"job:{graph['task_id']}:terminal",
            trigger_source="personal_research_api",
            action="hold",
            action_label="Hold",
            score=result.sentiment_score,
            horizon="3d",
            plan_quality="complete",
            status="active",
            research_stance=formal["research_stance"],
            account_action=formal["account_action"],
            research_snapshot_hash=artifacts.research_snapshot_hash,
            prompt_version=artifacts.prompt_version,
            catalysts_json=json.dumps(list(artifacts.catalysts)),
            invalidators_json=json.dumps(list(artifacts.invalidators)),
            unknowns_json=json.dumps(list(artifacts.unknowns)),
            evidence_refs_json=json.dumps(list(artifacts.evidence_refs)),
            policy_mode="off",
            policy_decision="allow",
            would_block=False,
            **{
                key: value
                for key, value in formal.items()
                if key.endswith("_score")
            },
        )
        session.add(signal)
        session.flush()
        signal_id = int(signal.id)
        signal_stance = str(signal.research_stance)
        signal_action = str(signal.account_action)

    thesis = service.persist_thesis(
        artifacts=artifacts,
        legacy_action="hold",
        signal_item={
            "id": signal_id,
            "research_stance": signal_stance,
            "account_action": signal_action,
        },
    )

    resumed = service.load_terminal_resume(
        task_id=graph["task_id"],
        stock_code="600519",
        market="A",
        report_type="full",
        query_id=graph["task_id"],
    )

    assert resumed is not None
    assert resumed.history_id == history_id
    assert resumed.result.analysis_summary == "first committed report"
    assert resumed.signal_item is not None
    assert resumed.signal_item["id"] == signal_id
    assert resumed.thesis_hash == thesis.content_hash


def test_thesis_can_bind_exact_decision_signal_and_policy_audit(personal_graph) -> None:
    graph = personal_graph
    executions = _persist_five_skills(graph)
    scores = {item.row.score_field: item.row.score for item in executions.values()}
    evaluation_hash = "9" * 64
    policy_hash = "8" * 64
    with graph["db"].session_scope() as session:
        signal = DecisionSignalRecord(
            stock_code="600519",
            market="a",
            source_type="personal_research",
            trace_id=graph["task_id"],
            trigger_source="manual",
            action="observe",
            plan_quality="complete",
            status="active",
            research_stance="bullish",
            account_action="hold",
            research_snapshot_hash=graph["research"].snapshot_hash,
            policy_version="personal-cn-v1",
            policy_hash=policy_hash,
            policy_evaluation_hash=evaluation_hash,
            portfolio_snapshot_ref="portfolio-snapshot-1",
            policy_mode="shadow",
            policy_decision="allow",
            would_block=False,
            **scores,
        )
        session.add(signal)
        session.flush()
        session.add(
            PortfolioPolicyEvaluationRecord(
                evaluation_hash=evaluation_hash,
                job_id=graph["task_id"],
                signal_id=signal.id,
                stock_code="600519",
                market="a",
                mode="shadow",
                policy_version="personal-cn-v1",
                policy_hash=policy_hash,
                research_snapshot_hash=graph["research"].snapshot_hash,
                portfolio_snapshot_ref="portfolio-snapshot-1",
                input_hash="7" * 64,
                output_hash="6" * 64,
                research_stance="bullish",
                proposed_account_action="hold",
                final_account_action="hold",
                verdict="allow",
                allowed=True,
                would_block=False,
                reasons_json="[]",
                component_scores_json="{}",
                limits_json="{}",
            )
        )
        signal_id = signal.id

    thesis = PersonalResearchThesisRepository(graph["db"]).persist(
        task_id=graph["task_id"],
        stock_code="600519",
        market="a",
        research_snapshot_hash=graph["research"].snapshot_hash,
        skill_execution_hashes={
            skill_id: item.content_hash for skill_id, item in executions.items()
        },
        stance="bullish",
        account_action="hold",
        catalysts=[],
        invalidators=[],
        unknowns=[],
        evidence_refs=["citation-1"],
        decision_signal_id=signal_id,
        policy_evaluation_hash=evaluation_hash,
    )
    assert thesis.row.decision_signal_id == signal_id
    assert thesis.row.policy_evaluation_hash == evaluation_hash
    assert thesis.row.policy_version == "personal-cn-v1"
    assert thesis.row.policy_hash == policy_hash
    assert thesis.row.portfolio_snapshot_ref == "portfolio-snapshot-1"
    latest = PersonalResearchThesisRepository(graph["db"]).get_latest_by_signal_id(
        signal_id
    )
    assert latest is not None
    assert latest.thesis_hash == thesis.content_hash


def test_thesis_can_bind_formal_signal_without_policy_lineage(personal_graph) -> None:
    graph = personal_graph
    executions = _persist_five_skills(graph)
    scores = {item.row.score_field: item.row.score for item in executions.values()}
    with graph["db"].session_scope() as session:
        signal = DecisionSignalRecord(
            stock_code="600519",
            market="a",
            source_type="personal_research",
            trace_id=graph["task_id"],
            trigger_source="manual",
            action="observe",
            plan_quality="complete",
            status="active",
            research_stance="bullish",
            account_action="hold",
            research_snapshot_hash=graph["research"].snapshot_hash,
            policy_mode="off",
            policy_decision="allow",
            would_block=False,
            **scores,
        )
        session.add(signal)
        session.flush()
        signal_id = signal.id

    thesis = PersonalResearchThesisRepository(graph["db"]).persist(
        task_id=graph["task_id"],
        stock_code="600519",
        market="a",
        research_snapshot_hash=graph["research"].snapshot_hash,
        skill_execution_hashes={
            skill_id: item.content_hash for skill_id, item in executions.items()
        },
        stance="bullish",
        account_action="hold",
        catalysts=[],
        invalidators=[],
        unknowns=[],
        evidence_refs=["citation-1"],
        decision_signal_id=signal_id,
    )

    assert thesis.row.decision_signal_id == signal_id
    assert thesis.row.policy_evaluation_hash is None
    latest = PersonalResearchThesisRepository(graph["db"]).get_latest_by_signal_id(
        signal_id
    )
    assert latest is not None
    assert latest.thesis_hash == thesis.content_hash
