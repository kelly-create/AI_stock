import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.services.personal_research_artifact_service import (
    FrozenPersonalResearchArtifacts,
    PersonalResearchArtifactRuntimeError,
    PersonalResearchArtifactService,
)
from src.services.research.personal_skill_contract import (
    PERSONAL_RESEARCH_SKILL_IDS,
)


def _scorecard():
    outputs = {
        skill_id: SimpleNamespace(skill_input=object())
        for skill_id in PERSONAL_RESEARCH_SKILL_IDS
    }
    scorecard = MagicMock(outputs=outputs)
    scorecard.scorecard_hash = "scorecard-v1"
    scorecard.stock_code = "600519"
    scorecard.market = "cn"
    scorecard.research_snapshot_hash = "b" * 64
    scorecard.factor_snapshot_hash = "f" * 64
    scorecard.evidence_snapshot_hash = "e" * 64
    scorecard.decision_signal_fields.return_value = {
        "value_quality_score": 77.0,
        "trend_timing_score": 66.0,
        "catalyst_score": 55.0,
        "risk_score": 20.0,
        "evidence_quality_score": 100.0,
        "evidence_refs": ["citation_1", "claim_1"],
    }
    return scorecard


def _prepared(*, debate=True):
    bull = SimpleNamespace(
        arguments=(SimpleNamespace(statement="bull catalyst"),),
        open_questions=("bull unknown",),
    )
    bear = SimpleNamespace(
        arguments=(SimpleNamespace(statement="bear invalidator"),),
        open_questions=("bear unknown",),
    )
    debate_snapshot = (
        SimpleNamespace(
            debate_hash="d" * 64,
            turns=(
                SimpleNamespace(stance="bull", turn=bull),
                SimpleNamespace(stance="bear", turn=bear),
            ),
            limitations=("debate limitation",),
        )
        if debate
        else None
    )
    evidence = SimpleNamespace(
        evidence_hash="e" * 64,
        input_dataset_hashes=("a" * 64,),
        claims=(),
        limitations=(),
    )
    return SimpleNamespace(
        stock_code="600519",
        market="cn",
        lease=SimpleNamespace(job_id="task-1"),
        factors=object(),
        factor_snapshot=SimpleNamespace(content_hash="f" * 64),
        evidence_snapshot=evidence,
        debate_snapshot=debate_snapshot,
    )


def _service():
    service = PersonalResearchArtifactService.__new__(
        PersonalResearchArtifactService
    )
    service.db = MagicMock()
    service.skills = MagicMock()
    service.reviews = MagicMock()
    service.theses = MagicMock()
    service.reviews.get_for_task_stock.return_value = None
    service.skills.persist_success.side_effect = [
        SimpleNamespace(content_hash=(str(index) * 64))
        for index in range(1, 6)
    ]
    service.reviews.persist.return_value = SimpleNamespace(
        content_hash="a" * 64,
        row=SimpleNamespace(
            verifier_fail_closed=False,
            judge_fail_closed=False,
            verdict="bull",
        ),
    )
    return service


def test_pre_llm_persists_exactly_five_skills_and_verified_review() -> None:
    service = _service()
    scorecard = _scorecard()
    frozen = SimpleNamespace(
        snapshot_hash="b" * 64,
        snapshot=SimpleNamespace(prompt_version="traditional-analysis-v1"),
    )
    with patch(
        "src.services.personal_research_artifact_service."
        "build_personal_research_skill_scorecard",
        return_value=scorecard,
    ):
        result = service.persist_pre_llm(
            prepared=_prepared(),
            frozen_write=frozen,
        )

    assert tuple(result.skill_execution_hashes) == PERSONAL_RESEARCH_SKILL_IDS
    assert service.skills.persist_success.call_count == 5
    assert service.reviews.persist.call_count == 1
    assert result.debate_review_hash == "a" * 64
    assert result.debate_verdict == "bull"
    assert result.catalysts == ("bull catalyst",)
    assert result.invalidators == ("bear invalidator",)
    assert result.unknowns == (
        "bull unknown",
        "bear unknown",
        "debate limitation",
    )


def test_complete_skill_checkpoint_is_hydrated_without_reexecution() -> None:
    service = _service()
    scorecard = _scorecard()
    service.skills.list_for_task_stock.return_value = [
        SimpleNamespace(
            skill_id=skill_id,
            execution_hash=str(index) * 64,
            dataset_snapshot_hashes_json=json.dumps(["a" * 64]),
        )
        for index, skill_id in enumerate(PERSONAL_RESEARCH_SKILL_IDS, start=1)
    ]
    with patch(
        "src.services.personal_research_artifact_service."
        "build_personal_research_skill_scorecard",
        return_value=scorecard,
    ) as build_scorecard, patch(
        "src.services.personal_research_artifact_service."
        "hydrate_personal_research_skill_scorecard",
        return_value=scorecard,
    ):
        result = service.persist_pre_llm(
            prepared=_prepared(debate=False),
            frozen_write=SimpleNamespace(
                snapshot_hash="b" * 64,
                snapshot=SimpleNamespace(prompt_version="agent-analysis-v1"),
            ),
        )

    service.skills.persist_success.assert_not_called()
    build_scorecard.assert_not_called()
    assert set(result.skill_execution_hashes) == set(PERSONAL_RESEARCH_SKILL_IDS)


@pytest.mark.parametrize("field", ["verifier_fail_closed", "judge_fail_closed"])
def test_pre_llm_rejects_a_fail_closed_review(field: str) -> None:
    service = _service()
    setattr(service.reviews.persist.return_value.row, field, True)
    with patch(
        "src.services.personal_research_artifact_service."
        "build_personal_research_skill_scorecard",
        return_value=_scorecard(),
    ), pytest.raises(PersonalResearchArtifactRuntimeError, match="failed closed"):
        service.persist_pre_llm(
            prepared=_prepared(),
            frozen_write=SimpleNamespace(
                snapshot_hash="b" * 64,
                snapshot=SimpleNamespace(prompt_version="agent-analysis-v1"),
            ),
        )


def test_signal_fields_keep_research_stance_separate_from_account_action() -> None:
    scorecard = _scorecard()
    artifacts = FrozenPersonalResearchArtifacts(
        task_id="task-1",
        stock_code="600519",
        market="cn",
        research_snapshot_hash="b" * 64,
        prompt_version="agent-analysis-v1",
        scorecard=scorecard,
        skill_execution_hashes={skill_id: "a" * 64 for skill_id in PERSONAL_RESEARCH_SKILL_IDS},
        debate_snapshot_hash="d" * 64,
        debate_review_hash="r" * 64,
        debate_verdict="bull",
        catalysts=("catalyst",),
        invalidators=("invalidator",),
        unknowns=("unknown",),
        evidence_refs=("citation_1", "claim_1"),
    )

    fields = artifacts.decision_signal_fields("sell")

    assert fields["research_stance"] == "bullish"
    assert fields["account_action"] == "exit_candidate"
    assert fields["research_snapshot_hash"] == "b" * 64


def test_thesis_only_links_policy_when_portfolio_lineage_is_complete() -> None:
    service = _service()
    artifacts = FrozenPersonalResearchArtifacts(
        task_id="task-1",
        stock_code="600519",
        market="cn",
        research_snapshot_hash="b" * 64,
        prompt_version="agent-analysis-v1",
        scorecard=_scorecard(),
        skill_execution_hashes={skill_id: "a" * 64 for skill_id in PERSONAL_RESEARCH_SKILL_IDS},
        debate_snapshot_hash=None,
        debate_review_hash=None,
        debate_verdict=None,
        catalysts=(),
        invalidators=(),
        unknowns=(),
        evidence_refs=("citation_1", "claim_1"),
    )
    service.theses.persist.return_value = SimpleNamespace(content_hash="t" * 64)

    service.persist_thesis(
        artifacts=artifacts,
        legacy_action="buy",
        signal_item={
            "id": 7,
            "research_stance": "bullish",
            "account_action": "open_candidate",
            "policy_evaluation_hash": "p" * 64,
            "portfolio_snapshot_ref": "portfolio-ledger-v1:" + "1" * 64,
        },
    )

    assert service.theses.persist.call_args.kwargs["decision_signal_id"] == 7
    assert (
        service.theses.persist.call_args.kwargs["policy_evaluation_hash"]
        == "p" * 64
    )


def test_thesis_links_formal_signal_when_policy_context_is_off() -> None:
    service = _service()
    artifacts = FrozenPersonalResearchArtifacts(
        task_id="task-1",
        stock_code="600519",
        market="cn",
        research_snapshot_hash="b" * 64,
        prompt_version="agent-analysis-v1",
        scorecard=_scorecard(),
        skill_execution_hashes={
            skill_id: "a" * 64 for skill_id in PERSONAL_RESEARCH_SKILL_IDS
        },
        debate_snapshot_hash=None,
        debate_review_hash=None,
        debate_verdict=None,
        catalysts=(),
        invalidators=(),
        unknowns=(),
        evidence_refs=("citation_1", "claim_1"),
    )
    service.theses.persist.return_value = SimpleNamespace(content_hash="t" * 64)

    service.persist_thesis(
        artifacts=artifacts,
        legacy_action="buy",
        signal_item={
            "id": 8,
            "research_stance": "bullish",
            "account_action": "open_candidate",
            "policy_evaluation_hash": "p" * 64,
            "portfolio_snapshot_ref": None,
        },
    )

    assert service.theses.persist.call_args.kwargs["decision_signal_id"] == 8
    assert service.theses.persist.call_args.kwargs["policy_evaluation_hash"] is None
