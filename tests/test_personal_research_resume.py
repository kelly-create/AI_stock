from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.analyzer import AnalysisResult
from src.core.pipeline import StockAnalysisPipeline
from src.enums import ReportType
from src.core.pipeline import PERSONAL_RESEARCH_POLICY_REPLAY_KEY
from src.services.portfolio_policy_gate_service import (
    build_portfolio_policy_replay_contract,
)
from src.services.research.canonical import canonical_hash
from src.services.research.personal_skill_evaluator import (
    PersonalResearchSkillEvaluationError,
)


class _Pack:
    pack_version = "analysis-context-pack-v1"

    def __init__(self, created_at):
        self.created_at = created_at

    def model_copy(self, *, update, deep):
        assert deep is True
        copied = _Pack(self.created_at)
        copied.created_at = update["created_at"]
        return copied


class _Runtime:
    def __init__(self, bound_hash: str):
        self.bound_hash = bound_hash
        self.freeze_calls = []

    def get_bound_research_snapshot(self, prepared):
        return {"snapshot_hash": self.bound_hash}

    def freeze(self, *args, **kwargs):
        self.freeze_calls.append((args, kwargs))
        return SimpleNamespace(snapshot_hash=self.bound_hash)


def test_pipeline_pins_context_pack_time_and_bound_hash_on_retry() -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.config = SimpleNamespace(personal_research_enabled=False)
    runtime = _Runtime("a" * 64)
    pipeline._get_research_runtime = lambda: runtime
    pipeline._build_research_execution_policy = lambda **_kwargs: {"v": 1}
    pipeline._build_research_model_route = lambda **_kwargs: {"model": "frozen"}
    as_of = datetime(2026, 8, 10, 8, 0, tzinfo=timezone.utc)
    prepared = SimpleNamespace(
        as_of=as_of,
        task_decision=None,
        budget_reservations=(),
    )
    wall_clock_pack = _Pack(datetime(2026, 8, 11, 1, 2, tzinfo=timezone.utc))

    result = pipeline._freeze_research_before_llm(
        prepared,
        context_pack=wall_clock_pack,
        prompt={"system": "frozen"},
        use_agent=False,
    )

    assert result.snapshot_hash == "a" * 64
    args, kwargs = runtime.freeze_calls[0]
    assert args[1].created_at == as_of
    assert kwargs["expected_snapshot_hash"] == "a" * 64


@pytest.mark.parametrize(
    ("job_type", "should_degrade"),
    (
        ("stock_analysis", True),
        ("scheduled_analysis", True),
        ("personal_research", False),
    ),
)
def test_missing_factor_score_only_degrades_optional_artifacts_for_ordinary_jobs(
    monkeypatch: pytest.MonkeyPatch,
    job_type: str,
    should_degrade: bool,
) -> None:
    import src.core.pipeline as pipeline_module
    from src.services.personal_research_artifact_service import (
        PersonalResearchArtifactService,
    )

    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.config = SimpleNamespace(personal_research_enabled=True)
    pipeline.db = MagicMock()
    runtime = _Runtime("b" * 64)
    pipeline._get_research_runtime = lambda: runtime
    pipeline._build_research_execution_policy = lambda **_kwargs: {"v": 1}
    pipeline._build_research_model_route = lambda **_kwargs: {"model": "frozen"}
    prepared = SimpleNamespace(
        as_of=datetime(2026, 8, 11, 8, 0, tzinfo=timezone.utc),
        stock_code="601398",
        task_decision=None,
        budget_reservations=(),
    )

    def fail_scorecard(*_args, **_kwargs):
        raise PersonalResearchSkillEvaluationError(
            "quality score is unavailable in the frozen Factor snapshot"
        )

    monkeypatch.setattr(pipeline_module, "_durable_execution_active", lambda: True)
    monkeypatch.setattr(pipeline_module, "_durable_job_type", lambda: job_type)
    monkeypatch.setattr(
        PersonalResearchArtifactService,
        "persist_pre_llm",
        fail_scorecard,
    )

    if not should_degrade:
        with pytest.raises(
            PersonalResearchSkillEvaluationError,
            match="quality score is unavailable",
        ):
            pipeline._freeze_research_before_llm(
                prepared,
                context_pack=_Pack(prepared.as_of),
                prompt={"system": "frozen"},
                use_agent=False,
            )
        return

    result = pipeline._freeze_research_before_llm(
        prepared,
        context_pack=_Pack(prepared.as_of),
        prompt={"system": "frozen"},
        use_agent=False,
    )
    assert result.snapshot_hash == "b" * 64


def test_terminal_resume_skips_collection_and_llm() -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.query_id = "task-terminal"
    pipeline.trace_id = "task-terminal"
    pipeline.query_source = "personal_research_api"
    pipeline._resolve_resume_target_date = lambda *_args, **_kwargs: date(2026, 8, 8)
    pipeline._emit_progress = MagicMock()
    result = AnalysisResult(
        code="600519",
        name="Kweichow Moutai",
        sentiment_score=70,
        trend_prediction="bullish",
        operation_advice="hold",
    )
    resumed = SimpleNamespace(history_id=17, result=result)
    pipeline._load_personal_research_terminal_resume = MagicMock(
        return_value=resumed
    )
    pipeline._complete_personal_research_terminal_resume = MagicMock(
        return_value=result
    )
    pipeline._prepare_research_for_stock = MagicMock(
        side_effect=AssertionError("research collection must not run")
    )
    pipeline.fetch_and_save_stock_data = MagicMock(
        side_effect=AssertionError("legacy providers must not run")
    )
    pipeline.analyze_stock = MagicMock(
        side_effect=AssertionError("LLM analysis must not run")
    )

    actual = pipeline.process_single_stock(
        "600519",
        report_type=ReportType.FULL,
        single_stock_notify=False,
    )

    assert actual is result
    pipeline._complete_personal_research_terminal_resume.assert_called_once()
    pipeline._prepare_research_for_stock.assert_not_called()
    pipeline.fetch_and_save_stock_data.assert_not_called()
    pipeline.analyze_stock.assert_not_called()


def test_history_only_resume_runs_deterministic_signal_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.config = SimpleNamespace(research_thesis_enabled=True)
    pipeline.portfolio_context = None
    pipeline._extract_decision_signal_after_history_save = MagicMock()
    result = AnalysisResult(
        code="600519",
        name="Kweichow Moutai",
        sentiment_score=70,
        trend_prediction="bullish",
        operation_advice="hold",
    )
    resumed = SimpleNamespace(
        result=result,
        history_id=17,
        signal_item=None,
        thesis_hash=None,
        context_snapshot={"market_phase_summary": {"session_date": "2026-08-08"}},
        artifacts=object(),
    )
    history_run = MagicMock()
    monkeypatch.setattr("src.core.pipeline.record_history_run", history_run)

    actual = pipeline._complete_personal_research_terminal_resume(
        resumed,
        query_id="task-terminal",
        report_type=ReportType.FULL,
    )

    assert actual is result
    history_run.assert_called_once_with(
        report_saved=True,
        metadata_saved=True,
        analysis_history_id=17,
    )
    pipeline._extract_decision_signal_after_history_save.assert_called_once()


def test_signal_only_resume_persists_only_missing_thesis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.config = SimpleNamespace(research_thesis_enabled=True)
    pipeline.db = object()
    pipeline._extract_decision_signal_after_history_save = MagicMock()
    result = AnalysisResult(
        code="600519",
        name="Kweichow Moutai",
        sentiment_score=70,
        trend_prediction="bullish",
        operation_advice="hold",
        action="hold",
    )
    artifacts = object()
    signal_item = {"id": 23, "action": "hold"}
    resumed = SimpleNamespace(
        result=result,
        history_id=17,
        signal_item=signal_item,
        thesis_hash=None,
        context_snapshot={},
        artifacts=artifacts,
    )
    artifact_service = MagicMock()
    artifact_service.persist_thesis.return_value = SimpleNamespace(
        content_hash="f" * 64
    )
    service_type = MagicMock(return_value=artifact_service)
    monkeypatch.setattr(
        "src.services.personal_research_artifact_service."
        "PersonalResearchArtifactService",
        service_type,
    )
    monkeypatch.setattr("src.core.pipeline.record_history_run", MagicMock())
    monkeypatch.setattr(
        "src.core.pipeline.resolve_decision_signal_action_fields",
        MagicMock(return_value={"action": "hold"}),
    )
    monkeypatch.setattr(
        "src.core.pipeline.summarize_decision_signal",
        MagicMock(return_value="frozen signal"),
    )

    actual = pipeline._complete_personal_research_terminal_resume(
        resumed,
        query_id="task-terminal",
        report_type=ReportType.FULL,
    )

    assert actual is result
    assert result.personal_research_thesis_hash == "f" * 64
    assert result.decision_signal_summary == "frozen signal"
    pipeline._extract_decision_signal_after_history_save.assert_not_called()
    service_type.assert_called_once_with(pipeline.db)
    artifact_service.persist_thesis.assert_called_once_with(
        artifacts=artifacts,
        legacy_action="hold",
        signal_item=signal_item,
    )


def test_snapshot_disabled_still_persists_private_formal_resume_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.config = SimpleNamespace(portfolio_policy_gate_mode="shadow")
    pipeline.save_context_snapshot = False
    pipeline.db = object()
    pipeline.policy_account_id = 7
    pipeline.policy_target_weight_pct = 8.0
    result = AnalysisResult(
        code="600519",
        name="Kweichow Moutai",
        sentiment_score=70,
        trend_prediction="bullish",
        operation_advice="buy",
        action="buy",
    )
    artifacts = MagicMock()
    artifacts.decision_signal_fields.return_value = {
        "research_stance": "bullish",
        "account_action": "open_candidate",
    }
    setattr(result, "_personal_research_artifacts", artifacts)
    policy_context = {
        "portfolio_complete": True,
        "portfolio_snapshot_ref": "portfolio-policy-context-v1:" + "a" * 64,
    }
    builder = MagicMock(return_value=policy_context)
    monkeypatch.setattr(
        "src.services.personal_research_policy_context_service."
        "PersonalResearchPolicyContextService.build",
        builder,
    )
    monkeypatch.setattr(
        "src.utils.sniper_points.extract_sniper_points",
        lambda _result: {
            "ideal_buy": 100.0,
            "secondary_buy": 102.0,
            "stop_loss": 90.0,
        },
    )
    context = {
        "market_phase_summary": {
            "session_date": "2026-08-10",
            "effective_daily_bar_date": "2026-08-07",
        },
        "private_full_context": {"must_not_persist": True},
    }

    persisted, save_snapshot = pipeline._personal_research_history_context(
        result=result,
        report_type="full",
        context_snapshot=context,
    )

    assert save_snapshot is True
    assert set(persisted) == {PERSONAL_RESEARCH_POLICY_REPLAY_KEY}
    replay = persisted[PERSONAL_RESEARCH_POLICY_REPLAY_KEY]
    assert replay["contract"]["mode"] == "shadow"
    assert replay["policy_context"] == policy_context
    assert replay["market_phase_summary"] == context["market_phase_summary"]
    assert "private_full_context" not in persisted


def test_history_tail_replays_frozen_policy_without_current_portfolio_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.config = SimpleNamespace(
        research_thesis_enabled=False,
        portfolio_policy_gate_mode="enforce",
    )
    pipeline.db = object()
    pipeline.query_source = "personal_research_api"
    result = AnalysisResult(
        code="600519",
        name="Kweichow Moutai",
        sentiment_score=70,
        trend_prediction="bullish",
        operation_advice="buy",
        action="buy",
    )
    artifacts = MagicMock()
    artifacts.decision_signal_fields.return_value = {
        "research_stance": "bullish",
        "account_action": "open_candidate",
    }
    setattr(result, "_personal_research_artifacts", artifacts)
    frozen_context = {
        "portfolio_complete": True,
        "portfolio_snapshot_ref": "portfolio-policy-context-v1:" + "a" * 64,
    }
    replay_payload = {
        "schema_version": "personal-research-policy-replay-v1",
        "contract": build_portfolio_policy_replay_contract("shadow"),
        "policy_context": frozen_context,
        "market_phase_summary": {
            "phase": "postmarket",
            "session_date": "2026-08-10",
            "effective_daily_bar_date": "2026-08-10",
        },
    }
    replay = {
        **replay_payload,
        "replay_hash": canonical_hash(replay_payload, exclude_volatile=False),
    }
    extract = MagicMock(return_value={"item": None})
    monkeypatch.setattr(
        "src.core.pipeline.extract_and_persist_from_analysis_result",
        extract,
    )
    monkeypatch.setattr("src.core.pipeline._durable_execution_active", lambda: True)
    monkeypatch.setattr(
        "src.services.personal_research_policy_context_service."
        "PersonalResearchPolicyContextService.build",
        MagicMock(side_effect=AssertionError("current portfolio must not be read")),
    )

    pipeline._extract_decision_signal_after_history_save(
        result=result,
        query_id="task-terminal",
        source_report_id=17,
        report_type="full",
        context_snapshot={PERSONAL_RESEARCH_POLICY_REPLAY_KEY: replay},
    )

    kwargs = extract.call_args.kwargs
    assert kwargs["personal_research_fields"]["policy_context"] == frozen_context
    assert kwargs["personal_research_fields"]["policy_replay_contract"]["mode"] == "shadow"
    assert kwargs["context_snapshot"]["market_phase_summary"]["phase"] == "postmarket"

    tampered = {
        **replay,
        "market_phase_summary": {
            **replay["market_phase_summary"],
            "session_date": "2026-08-11",
        },
    }
    extract.reset_mock()
    with pytest.raises(RuntimeError, match="replay hash differs"):
        pipeline._extract_decision_signal_after_history_save(
            result=result,
            query_id="task-terminal",
            source_report_id=17,
            report_type="full",
            context_snapshot={PERSONAL_RESEARCH_POLICY_REPLAY_KEY: tampered},
        )
    extract.assert_not_called()
