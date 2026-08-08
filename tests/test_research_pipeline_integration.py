# -*- coding: utf-8 -*-
"""Focused PR2 integration contracts for the stock analysis pipeline.

These tests deliberately fake every network and LLM edge.  They protect the
handoff between the durable research runtime and the existing pipeline, where
an accidental fallback would otherwise make a second Tushare request or freeze
different prompt material from the content actually sent to the model.
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tests.litellm_stub import ensure_litellm_stub

ensure_litellm_stub()

from data_provider.realtime_types import ChipDistribution
from src.core.pipeline import StockAnalysisPipeline
from src.enums import ReportType


_SHANGHAI = timezone(timedelta(hours=8))
_AS_OF = datetime(2026, 8, 7, 18, 0, tzinfo=_SHANGHAI)


def _daily_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600519.SH",
                "trade_date": "20260807",
                "open": 1400.0,
                "high": 1420.0,
                "low": 1390.0,
                "close": 1410.0,
                "pre_close": 1398.0,
                "pct_chg": 0.86,
                "vol": 1234.0,
                "amount": 5678.0,
            }
        ]
    )


def _chips_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600519.SH",
                "trade_date": "20260807",
                "price": 1300.0,
                "percent": 40.0,
            },
            {
                "ts_code": "600519.SH",
                "trade_date": "20260807",
                "price": 1450.0,
                "percent": 60.0,
            },
        ]
    )


class _FrozenCollection:
    def __init__(self) -> None:
        self._frames = {
            "daily": _daily_frame(),
            "cyq_chips": _chips_frame(),
            "stock_basic": pd.DataFrame(
                [{"ts_code": "600519.SH", "name": "Kweichow Moutai"}]
            ),
            "income": pd.DataFrame([{"ts_code": "600519.SH", "revenue": 1.0}]),
        }
        self.datasets = (
            SimpleNamespace(
                dataset="balancesheet",
                status="permission_denied",
                error_code="permission_denied",
            ),
        )

    def frames_copy(self) -> dict[str, pd.DataFrame]:
        return {name: frame.copy(deep=True) for name, frame in self._frames.items()}


def _prepared_research(
    *,
    factors_enabled: bool = True,
    evidence_prompt_context: str | None = None,
) -> SimpleNamespace:
    collection = _FrozenCollection()
    evidence_context = None
    if evidence_prompt_context is not None:
        evidence_context = {
            "status": "available",
            "available_at": _AS_OF.isoformat(),
            "evidence_hash": "e" * 64,
            "claims": ({"id": "claim-1", "statement": "Frozen claim"},),
            "citations": ({"id": "citation-1", "artifact_hash": "d" * 64},),
            "limitations": (),
        }
    return SimpleNamespace(
        as_of=_AS_OF,
        lease=SimpleNamespace(
            job_id="research-job",
            worker_id="research-worker",
            lease_token="research-lease-token",
        ),
        collection=collection,
        rows_by_dataset={
            "stock_basic": ({"ts_code": "600519.SH", "name": "Kweichow Moutai"},),
        },
        research_context={"factor_snapshot_hash": "f" * 64},
        factors_enabled=factors_enabled,
        evidence_enabled=evidence_prompt_context is not None,
        evidence_context=evidence_context,
        evidence_prompt_context=evidence_prompt_context,
    )


def _base_config(**overrides: object) -> SimpleNamespace:
    values = {
        "tushare_research_enabled": False,
        "personal_research_enabled": False,
        "research_factors_enabled": False,
        "research_evidence_enabled": False,
        "enable_realtime_quote": False,
        "enable_chip_distribution": True,
        "realtime_source_priority": [],
        "agent_mode": False,
        "agent_skills": [],
        "report_language": "zh",
        "report_integrity_enabled": False,
        "fundamental_stage_timeout_seconds": 1,
        "litellm_model": "provider/traditional-model",
        "litellm_fallback_models": [],
        "agent_litellm_model": "provider/agent-model",
        "agent_fallback_models": [],
        "llm_model_list": [],
        "llm_temperature": 0.2,
        "generation_backend": "litellm",
        "agent_generation_backend": "litellm",
        "generation_fallback_backend": None,
        "openai_base_url": None,
        "generation_backend_timeout_seconds": 60,
        "generation_backend_max_output_bytes": 1_000_000,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _bare_pipeline(**config_overrides: object) -> StockAnalysisPipeline:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.config = _base_config(**config_overrides)
    pipeline.db = MagicMock()
    pipeline.fetcher_manager = MagicMock()
    pipeline.analyzer = MagicMock()
    pipeline.trend_analyzer = MagicMock()
    pipeline.search_service = SimpleNamespace(is_available=False)
    pipeline.social_sentiment_service = None
    pipeline.source_message = None
    pipeline.query_id = None
    pipeline.trace_id = None
    pipeline.query_source = "system"
    pipeline.save_context_snapshot = False
    pipeline.progress_callback = None
    pipeline.analysis_skills = None
    pipeline.analysis_phase = "auto"
    pipeline.portfolio_context = None
    pipeline._research_runtime = None
    pipeline._emit_progress = MagicMock()
    return pipeline


def _analysis_pipeline(**config_overrides: object) -> StockAnalysisPipeline:
    pipeline = _bare_pipeline(**config_overrides)
    pipeline.fetcher_manager.get_realtime_quote.return_value = None
    pipeline.fetcher_manager.get_fundamental_context.return_value = {
        "market": "cn",
        "status": "ok",
        "source_chain": ["preloaded_tushare"],
        "coverage": {},
    }
    pipeline.fetcher_manager.build_failed_fundamental_context.return_value = {
        "market": "cn",
        "status": "failed",
        "source_chain": [],
        "coverage": {},
    }
    pipeline.db.get_data_range.return_value = []
    pipeline.db.get_analysis_context.return_value = {
        "code": "600519",
        "date": "2026-08-07",
        "today": {"close": 1410.0},
        "yesterday": {"close": 1398.0},
    }
    pipeline._load_daily_market_context = MagicMock(return_value=None)
    pipeline._attach_daily_market_context = MagicMock()
    pipeline._attach_belong_boards_to_fundamental_context = MagicMock(
        side_effect=lambda _code, context, **_kwargs: context
    )
    pipeline._build_market_structure_context = MagicMock(return_value=None)
    pipeline._load_persisted_intelligence_context = MagicMock(return_value=None)
    pipeline._get_analysis_context_with_market_fallback = MagicMock(
        return_value={
            "code": "600519",
            "date": "2026-08-07",
            "today": {"close": 1410.0},
            "yesterday": {"close": 1398.0},
        }
    )
    return pipeline


def _phase_context() -> SimpleNamespace:
    payload = {
        "market": "cn",
        "phase": "post_close",
        "effective_daily_bar_date": "2026-08-07",
        "warnings": [],
    }
    return SimpleNamespace(
        effective_daily_bar_date="2026-08-07",
        to_dict=lambda: dict(payload),
    )


class TestResearchProcessSingleStockIntegration:
    def test_flag_off_preserves_legacy_fetch_and_has_no_runtime_side_effect(self) -> None:
        pipeline = _bare_pipeline(tushare_research_enabled=False)
        pipeline._resolve_resume_target_date = MagicMock(return_value=date(2026, 8, 7))
        pipeline._get_research_runtime = MagicMock(
            side_effect=AssertionError("flag-off path must not construct research runtime")
        )
        pipeline.fetch_and_save_stock_data = MagicMock(return_value=(True, None))
        pipeline.analyze_stock = MagicMock(return_value=None)

        result = pipeline.process_single_stock(
            "600519",
            report_type=ReportType.SIMPLE,
            analysis_query_id="legacy-q",
            current_time=_AS_OF,
        )

        assert result is None
        pipeline.fetch_and_save_stock_data.assert_called_once_with(
            "600519", current_time=_AS_OF
        )
        pipeline._get_research_runtime.assert_not_called()
        pipeline.db.save_daily_data.assert_not_called()
        pipeline.analyze_stock.assert_called_once_with(
            "600519",
            ReportType.SIMPLE,
            query_id="legacy-q",
            current_time=_AS_OF,
        )

    def test_a_share_research_prepares_first_without_mutating_legacy_daily(self) -> None:
        prepared = _prepared_research()
        ordering: list[str] = []
        runtime = MagicMock()
        runtime.prepare.side_effect = lambda *_args, **_kwargs: (
            ordering.append("prepare") or prepared
        )
        pipeline = _bare_pipeline(
            tushare_research_enabled=True,
            personal_research_enabled=True,
            research_factors_enabled=True,
        )
        pipeline._resolve_resume_target_date = MagicMock(return_value=date(2026, 8, 7))
        pipeline._get_research_runtime = MagicMock(return_value=runtime)
        pipeline.fetch_and_save_stock_data = MagicMock(
            side_effect=AssertionError("research daily must replace the legacy fetch")
        )
        runtime.checkpoint.side_effect = lambda *_args, **_kwargs: ordering.append(
            "checkpoint"
        )
        pipeline.analyze_stock = MagicMock(
            side_effect=lambda *_args, **_kwargs: ordering.append("analyze") or None
        )

        pipeline.process_single_stock(
            "600519",
            report_type=ReportType.SIMPLE,
            analysis_query_id="research-q",
            current_time=_AS_OF,
        )

        runtime.prepare.assert_called_once_with(
            "600519",
            "cn",
            _AS_OF,
            reference_mode="live",
        )
        assert ordering == ["prepare", "checkpoint", "analyze"]
        pipeline.fetch_and_save_stock_data.assert_not_called()
        pipeline.db.save_daily_data.assert_not_called()
        runtime.checkpoint.assert_called_once_with(prepared)
        assert pipeline.analyze_stock.call_args.kwargs["prepared_research"] is prepared

    def test_naive_current_time_is_interpreted_as_shanghai_before_runtime_prepare(self) -> None:
        prepared = _prepared_research()
        runtime = MagicMock()
        runtime.prepare.return_value = prepared
        pipeline = _bare_pipeline(
            tushare_research_enabled=True,
            personal_research_enabled=True,
        )
        pipeline._get_research_runtime = MagicMock(return_value=runtime)
        naive_local_time = datetime(2026, 8, 7, 18, 0)

        pipeline._prepare_research_for_stock(
            "600519",
            current_time=naive_local_time,
        )

        prepared_as_of = runtime.prepare.call_args.args[2]
        assert prepared_as_of.utcoffset() == timedelta(hours=8)
        assert prepared_as_of.replace(tzinfo=None) == naive_local_time

    def test_evidence_flag_injects_snippet_only_search_into_runtime(self) -> None:
        runtime = MagicMock()
        runtime.prepare.return_value = _prepared_research()
        pipeline = _bare_pipeline(
            tushare_research_enabled=True,
            personal_research_enabled=True,
            research_factors_enabled=True,
            research_evidence_enabled=True,
        )
        pipeline._get_research_runtime = MagicMock(return_value=runtime)
        response = object()
        pipeline.search_service = SimpleNamespace(
            is_available=True,
            search_stock_news=MagicMock(return_value=response),
        )

        pipeline._prepare_research_for_stock("600519", current_time=_AS_OF)

        prepare_kwargs = runtime.prepare.call_args.kwargs
        assert prepare_kwargs["reference_mode"] == "live"
        evidence_search = prepare_kwargs["evidence_search"]
        assert evidence_search(
            stock_code="600519",
            stock_name="Kweichow Moutai",
            max_results=5,
        ) is response
        pipeline.search_service.search_stock_news.assert_called_once_with(
            "600519",
            "Kweichow Moutai",
            max_results=5,
            snippet_only=True,
        )

    def test_non_a_share_and_flag_off_do_not_construct_or_call_runtime(self) -> None:
        for code, research_enabled in (
            ("00700.HK", True),
            ("AAPL", True),
            ("600519", False),
        ):
            pipeline = _bare_pipeline(
                tushare_research_enabled=research_enabled,
                personal_research_enabled=research_enabled,
            )
            pipeline._get_research_runtime = MagicMock(
                side_effect=AssertionError(f"unexpected runtime for {code}")
            )

            assert (
                pipeline._prepare_research_for_stock(code, current_time=_AS_OF) is None
            )
            pipeline._get_research_runtime.assert_not_called()


class TestResearchAnalyzeStockIntegration:
    def test_consumed_research_requires_context_pack_even_without_factors(self) -> None:
        pipeline = _analysis_pipeline(agent_mode=False)
        prepared = _prepared_research(factors_enabled=False)

        with pytest.raises(
            RuntimeError,
            match="research data is enabled but AnalysisContextPack could not be built",
        ):
            pipeline._freeze_research_before_llm(
                prepared,
                context_pack=None,
                prompt={"messages": []},
                use_agent=False,
            )

    def test_frozen_research_history_never_falls_back_to_a_second_provider_call(
        self,
    ) -> None:
        pipeline = _bare_pipeline(tushare_research_enabled=True)
        pipeline.db.get_data_range.return_value = []

        with patch(
            "src.services.history_loader.get_frozen_target_date",
            return_value=_AS_OF.date(),
        ):
            pipeline._ensure_agent_history("600519", allow_network=False)

        pipeline.fetcher_manager.get_daily_data.assert_not_called()

    @patch("src.core.pipeline.render_market_phase_summary", return_value={})
    @patch("src.core.pipeline.build_market_phase_context", return_value=_phase_context())
    def test_preloaded_name_chip_and_fundamentals_never_use_legacy_tushare_edges(
        self,
        _mock_phase: MagicMock,
        _mock_phase_summary: MagicMock,
    ) -> None:
        prepared = _prepared_research()
        pipeline = _analysis_pipeline(agent_mode=True)
        agent_marker = object()
        pipeline._analyze_with_agent = MagicMock(return_value=agent_marker)

        result = pipeline.analyze_stock(
            "600519",
            ReportType.SIMPLE,
            "preloaded-q",
            current_time=_AS_OF,
            prepared_research=prepared,
        )

        assert result is agent_marker
        pipeline.fetcher_manager.get_stock_name.assert_not_called()
        pipeline.fetcher_manager.get_chip_distribution.assert_not_called()
        fundamental_kwargs = pipeline.fetcher_manager.get_fundamental_context.call_args.kwargs
        assert fundamental_kwargs["preloaded_tushare_errors"] == (
            "balancesheet:permission_denied:permission_denied",
        )
        frames = fundamental_kwargs["preloaded_tushare_frames"]
        assert set(frames) >= {"daily", "cyq_chips", "stock_basic", "income"}
        pd.testing.assert_frame_equal(frames["daily"], _daily_frame())
        agent_call = pipeline._analyze_with_agent.call_args
        assert agent_call.args[3] == "Kweichow Moutai"
        assert isinstance(agent_call.args[5], ChipDistribution)
        assert agent_call.args[5].code == "600519"
        assert agent_call.kwargs["prepared_research"] is prepared

    @patch("src.core.pipeline.record_llm_run")
    @patch("src.core.pipeline.record_llm_run_started")
    @patch("src.core.pipeline.render_market_phase_summary", return_value={})
    @patch("src.core.pipeline.build_market_phase_context", return_value=_phase_context())
    def test_traditional_freeze_uses_actual_context_pack_and_exact_llm_messages(
        self,
        _mock_phase: MagicMock,
        _mock_phase_summary: MagicMock,
        _mock_started: MagicMock,
        _mock_record: MagicMock,
    ) -> None:
        evidence_prompt = "DSA-EXACT-FROZEN-EVIDENCE"
        prepared = _prepared_research(
            evidence_prompt_context=evidence_prompt,
        )
        pipeline = _analysis_pipeline(
            agent_mode=False,
            enable_realtime_quote=True,
        )
        pipeline.search_service = MagicMock()
        pipeline.search_service.is_available = True
        context_pack = SimpleNamespace(pack_version="analysis-context-pack-v1")
        pipeline._build_analysis_context_pack_bundle = MagicMock(
            return_value=(context_pack, "PACK-SUMMARY", {"data_quality": {}})
        )
        pipeline._enhance_context = MagicMock(return_value={"frozen": "enhanced"})
        pipeline.analyzer._format_prompt.return_value = "EXACT-USER-PROMPT"
        pipeline.analyzer._get_analysis_system_prompt.return_value = "EXACT-SYSTEM-PROMPT"
        ordering: list[str] = []

        def freeze(*_args: object, **_kwargs: object) -> None:
            ordering.append("freeze")

        def analyze(*_args: object, **_kwargs: object) -> None:
            ordering.append("analyze")
            assert ordering == ["freeze", "analyze"]
            return None

        real_freeze = pipeline._freeze_research_before_llm
        pipeline._research_runtime = SimpleNamespace(
            freeze=MagicMock(side_effect=freeze)
        )
        pipeline._freeze_research_before_llm = MagicMock(wraps=real_freeze)
        pipeline.analyzer.analyze.side_effect = analyze

        result = pipeline.analyze_stock(
            "600519",
            ReportType.SIMPLE,
            "traditional-q",
            current_time=_AS_OF,
            prepared_research=prepared,
        )

        assert result is None
        freeze_call = pipeline._freeze_research_before_llm.call_args
        assert freeze_call.args == (prepared,)
        assert freeze_call.kwargs["context_pack"] is context_pack
        assert freeze_call.kwargs["use_agent"] is False
        assert freeze_call.kwargs["prompt"] == {
            "messages": [
                {"role": "system", "content": "EXACT-SYSTEM-PROMPT"},
                {"role": "user", "content": "EXACT-USER-PROMPT"},
            ]
        }
        pipeline.analyzer._format_prompt.assert_called_once()
        assert (
            pipeline.analyzer._format_prompt.call_args.kwargs[
                "analysis_context_pack_summary"
            ]
            == f"PACK-SUMMARY\n\n{evidence_prompt}"
        )
        pipeline.analyzer.analyze.assert_called_once()
        runtime_freeze_args = pipeline._research_runtime.freeze.call_args.args
        assert runtime_freeze_args[5] == (
            "factor-policy-v1+decision-execution-v1"
        )
        frozen_policy = runtime_freeze_args[6]
        assert frozen_policy["factor_policy"]["policy_version"] == (
            "factor-policy-v1"
        )
        assert frozen_policy["decision_execution"]["mode"] == "traditional"
        assert frozen_policy["decision_execution"]["code_fingerprints"]
        pipeline.fetcher_manager.get_realtime_quote.assert_not_called()
        pipeline.search_service.search_comprehensive_intel.assert_not_called()
        pipeline._load_persisted_intelligence_context.assert_not_called()
        pipeline._get_analysis_context_with_market_fallback.assert_not_called()
        pipeline.db.get_data_range.assert_not_called()
        pipeline.db.save_fundamental_snapshot.assert_not_called()
        pipeline._load_daily_market_context.assert_not_called()
        trend_frame = pipeline.trend_analyzer.analyze.call_args.args[0]
        assert list(trend_frame["close"]) == [1410.0]
        pipeline._attach_belong_boards_to_fundamental_context.assert_called_once_with(
            "600519",
            pipeline.fetcher_manager.get_fundamental_context.return_value,
            allow_network=False,
        )
        pipeline._build_market_structure_context.assert_not_called()
        assert (
            pipeline.analyzer.analyze.call_args.kwargs[
                "analysis_context_pack_summary"
            ]
            == f"PACK-SUMMARY\n\n{evidence_prompt}"
        )

    def test_execution_policy_changes_with_agent_guardrail_configuration(self) -> None:
        pipeline = _analysis_pipeline(agent_mode=True, agent_arch="multi")
        pipeline.config.agent_risk_override = True

        enabled = pipeline._build_research_execution_policy(use_agent=True)
        pipeline.config.agent_risk_override = False
        disabled = pipeline._build_research_execution_policy(use_agent=True)
        traditional = pipeline._build_research_execution_policy(use_agent=False)

        assert enabled["agent"]["risk_override_enabled"] is True
        assert disabled["agent"]["risk_override_enabled"] is False
        assert enabled != disabled
        assert enabled["mode"] == "agent"
        assert traditional["mode"] == "traditional"
        assert all(
            len(value) == 64
            for value in enabled["code_fingerprints"].values()
        )
        assert "src.services.decision_signal_extractor" in enabled[
            "code_fingerprints"
        ]
        assert set(enabled["pipeline_postprocess_fingerprints"]) == {
            "_agent_result_to_analysis_result",
            "_refresh_decision_action_for_final_result",
            "_extract_decision_signal_after_history_save",
        }
        with patch(
            "src.services.research.code_fingerprint.inspect.getsource",
            side_effect=OSError("could not get source code"),
        ):
            frozen_build_policy = pipeline._build_research_execution_policy(
                use_agent=True
            )
        assert all(
            len(value) == 64
            for value in frozen_build_policy["code_fingerprints"].values()
        )

    @patch("src.core.pipeline.record_llm_run")
    @patch("src.core.pipeline.record_llm_run_started")
    @patch("src.core.pipeline.render_market_phase_summary", return_value={})
    @patch("src.core.pipeline.build_market_phase_context", return_value=_phase_context())
    def test_empty_frozen_daily_never_backfills_prompt_from_legacy_stock_daily(
        self,
        _mock_phase: MagicMock,
        _mock_phase_summary: MagicMock,
        _mock_started: MagicMock,
        _mock_record: MagicMock,
    ) -> None:
        prepared = _prepared_research(factors_enabled=False)
        prepared.collection._frames["daily"] = pd.DataFrame()
        pipeline = _analysis_pipeline(agent_mode=False)
        pipeline.db.get_analysis_context.return_value = {
            "code": "600519",
            "date": "2026-08-07",
            "today": {"close": 999.0},
            "yesterday": {"close": 998.0},
        }
        pipeline._enhance_context = MagicMock(
            side_effect=lambda context, *_args, **_kwargs: context
        )
        context_pack = SimpleNamespace(pack_version="analysis-context-pack-v1")
        pipeline._build_analysis_context_pack_bundle = MagicMock(
            return_value=(context_pack, "PACK-SUMMARY", {"data_quality": {}})
        )
        pipeline._research_runtime = SimpleNamespace(freeze=MagicMock())
        pipeline.analyzer._format_prompt.return_value = "USER"
        pipeline.analyzer._get_analysis_system_prompt.return_value = "SYSTEM"
        pipeline.analyzer.analyze.return_value = None

        result = pipeline.analyze_stock(
            "600519",
            ReportType.SIMPLE,
            "empty-daily-q",
            current_time=_AS_OF,
            prepared_research=prepared,
        )

        assert result is None
        frozen_context = pipeline._enhance_context.call_args.args[0]
        assert frozen_context["data_missing"] is True
        assert frozen_context["today"] == {}
        assert frozen_context["yesterday"] == {}
        assert "999" not in str(frozen_context)
        pipeline.db.get_analysis_context.assert_not_called()
        pipeline.db.get_data_range.assert_not_called()
        pipeline.trend_analyzer.analyze.assert_not_called()
        pipeline._get_analysis_context_with_market_fallback.assert_not_called()
        pipeline._load_daily_market_context.assert_not_called()

    @patch("src.core.pipeline.record_llm_run")
    @patch("src.core.pipeline.record_llm_run_started")
    def test_agent_freeze_uses_executor_exact_messages_before_same_executor_run(
        self,
        _mock_started: MagicMock,
        _mock_record: MagicMock,
    ) -> None:
        evidence_prompt = "DSA-EXACT-FROZEN-EVIDENCE"
        prepared = _prepared_research(
            evidence_prompt_context=evidence_prompt,
        )
        pipeline = _analysis_pipeline(agent_mode=True)
        pipeline._ensure_agent_history = MagicMock()
        pipeline._load_agent_analysis_context = MagicMock(
            return_value={"code": "600519", "today": {}, "yesterday": {}}
        )
        context_pack = SimpleNamespace(pack_version="analysis-context-pack-v1")
        pipeline._build_analysis_context_pack_bundle = MagicMock(
            return_value=(context_pack, "AGENT-PACK-SUMMARY", {"data_quality": {}})
        )
        pipeline._agent_result_to_analysis_result = MagicMock(return_value=None)
        pipeline.search_service = SimpleNamespace(
            is_available=True,
            search_stock_news=MagicMock(),
        )
        pipeline.db.save_news_intel = MagicMock()
        ordering: list[str] = []

        class FakeExecutor:
            max_steps = 7
            timeout_seconds = 45.0

            def __init__(self) -> None:
                self.tool_registry = None
                self.build_initial_messages = MagicMock(
                    side_effect=self._build_initial_messages
                )
                self.build_research_snapshot_prompt = MagicMock(
                    side_effect=self._build_research_snapshot_prompt
                )

            @staticmethod
            def _build_initial_messages(task: str, context: dict) -> list[dict[str, str]]:
                return [
                    {"role": "system", "content": "EXACT-AGENT-SYSTEM"},
                    {
                        "role": "user",
                        "content": f"EXACT-AGENT-USER::{task}::{context['analysis_context_pack_summary']}",
                    },
                ]

            def _build_research_snapshot_prompt(
                self,
                task: str,
                context: dict,
            ) -> dict:
                return {
                    "architecture": "single-agent",
                    "messages": self.build_initial_messages(task, context=context),
                    "tool_declarations": self.tool_registry.to_openai_tools(),
                    "max_steps": self.max_steps,
                    "timeout_seconds": self.timeout_seconds,
                }

            def run(self, task: str, context: dict) -> SimpleNamespace:
                ordering.append("run")
                # AgentExecutor.run uses this same builder.  Calling it here makes
                # the fake enforce that the frozen input and runtime input derive
                # from the exact same task/context pair.
                assert (
                    self.build_initial_messages(task, context=context)
                    == frozen_prompts[0]["messages"]
                )
                assert ordering == ["freeze", "run"]
                return SimpleNamespace(model="provider/agent-model")

        executor = FakeExecutor()
        frozen_prompts: list[dict] = []

        def freeze(*_args: object, **kwargs: object) -> None:
            ordering.append("freeze")
            frozen_prompts.append(kwargs["prompt"])

        def build_executor(*_args: object, **kwargs: object) -> FakeExecutor:
            assert kwargs["research_snapshot_locked"] is True
            executor.tool_registry = kwargs["tool_registry"]
            return executor

        pipeline._freeze_research_before_llm = MagicMock(side_effect=freeze)

        with patch(
            "src.agent.factory.build_agent_executor",
            side_effect=build_executor,
        ) as factory:
            result = pipeline._analyze_with_agent(
                "600519",
                ReportType.SIMPLE,
                "agent-q",
                "Kweichow Moutai",
                None,
                None,
                {"status": "ok"},
                None,
                market_phase_context={"phase": "post_close"},
                market_phase_summary={},
                prepared_research=prepared,
            )

        assert result is None
        assert ordering == ["freeze", "run"]
        pipeline.search_service.search_stock_news.assert_not_called()
        pipeline.db.save_news_intel.assert_not_called()
        pipeline.db.save_fundamental_snapshot.assert_not_called()
        pipeline._ensure_agent_history.assert_not_called()
        assert len(frozen_prompts) == 1
        assert frozen_prompts[0]["architecture"] == "single-agent"
        assert frozen_prompts[0]["messages"][0]["content"] == "EXACT-AGENT-SYSTEM"
        assert evidence_prompt in frozen_prompts[0]["messages"][1]["content"]
        assert frozen_prompts[0]["messages"][1]["content"].count(evidence_prompt) == 1
        assert executor.tool_registry.list_names() == []
        assert executor.tool_registry.resolve("get_stock_info") is None
        assert factory.call_args.kwargs["research_snapshot_locked"] is True
        pipeline._load_persisted_intelligence_context.assert_not_called()
        pipeline._load_agent_analysis_context.assert_called_once_with(
            "600519",
            "Kweichow Moutai",
            target_date=_AS_OF.date(),
            allow_network=False,
            prepared_research=prepared,
        )
        freeze_call = pipeline._freeze_research_before_llm.call_args
        assert freeze_call.args == (prepared,)
        assert freeze_call.kwargs["context_pack"] is context_pack
        assert freeze_call.kwargs["use_agent"] is True
        assert freeze_call.kwargs["tools"] == []
        assert freeze_call.kwargs["max_steps"] == 7
        assert freeze_call.kwargs["timeout_seconds"] == 45.0
        assert executor.build_initial_messages.call_count == 2
        freeze_builder_call, runtime_builder_call = (
            executor.build_initial_messages.call_args_list
        )
        assert freeze_builder_call.args[0] == runtime_builder_call.args[0]
        assert (
            freeze_builder_call.kwargs["context"]
            is runtime_builder_call.kwargs["context"]
        )

    @patch("src.core.pipeline.record_llm_run")
    @patch("src.core.pipeline.record_llm_run_started")
    def test_multi_agent_research_freezes_stage_contract_and_has_no_data_tools(
        self,
        _mock_started: MagicMock,
        _mock_record: MagicMock,
    ) -> None:
        from src.agent.orchestrator import AgentOrchestrator

        prepared = _prepared_research()
        pipeline = _analysis_pipeline(agent_mode=True, agent_arch="multi")
        pipeline._ensure_agent_history = MagicMock()
        pipeline._load_agent_analysis_context = MagicMock(
            return_value={"code": "600519", "today": {}, "yesterday": {}}
        )
        context_pack = SimpleNamespace(pack_version="analysis-context-pack-v1")
        pipeline._build_analysis_context_pack_bundle = MagicMock(
            return_value=(context_pack, "MULTI-PACK-SUMMARY", {"data_quality": {}})
        )
        pipeline._agent_result_to_analysis_result = MagicMock(return_value=None)
        pipeline.search_service = SimpleNamespace(
            is_available=True,
            search_stock_news=MagicMock(),
        )
        pipeline.db.save_news_intel = MagicMock()
        ordering: list[str] = []
        frozen_prompts: list[dict] = []
        captured: dict[str, AgentOrchestrator] = {}

        def freeze(*_args: object, **kwargs: object) -> None:
            ordering.append("freeze")
            frozen_prompts.append(kwargs["prompt"])

        def build_executor(*_args: object, **kwargs: object) -> AgentOrchestrator:
            executor = AgentOrchestrator(
                tool_registry=kwargs["tool_registry"],
                llm_adapter=MagicMock(),
                max_steps=5,
                mode="quick",
                config=SimpleNamespace(agent_orchestrator_timeout_s=30),
                research_snapshot_locked=kwargs["research_snapshot_locked"],
            )

            def run(*_run_args: object, **_run_kwargs: object) -> SimpleNamespace:
                ordering.append("run")
                assert ordering == ["freeze", "run"]
                return SimpleNamespace(model="provider/multi-agent-model")

            executor.run = MagicMock(side_effect=run)
            captured["executor"] = executor
            return executor

        pipeline._freeze_research_before_llm = MagicMock(side_effect=freeze)
        with patch(
            "src.agent.factory.build_agent_executor",
            side_effect=build_executor,
        ) as factory:
            result = pipeline._analyze_with_agent(
                "600519",
                ReportType.SIMPLE,
                "multi-agent-q",
                "Kweichow Moutai",
                None,
                None,
                {"status": "ok"},
                None,
                market_phase_context={"phase": "post_close"},
                market_phase_summary={},
                prepared_research=prepared,
            )

        assert result is None
        assert ordering == ["freeze", "run"]
        pipeline.search_service.search_stock_news.assert_not_called()
        pipeline.db.save_news_intel.assert_not_called()
        pipeline.db.save_fundamental_snapshot.assert_not_called()
        assert factory.call_args.kwargs["research_snapshot_locked"] is True
        executor = captured["executor"]
        assert executor.tool_registry.list_names() == []
        assert frozen_prompts[0]["architecture"] == "multi-agent"
        assert frozen_prompts[0]["mode"] == "quick"
        assert [item["stage"] for item in frozen_prompts[0]["stage_contracts"]] == [
            "technical",
            "decision",
        ]
        assert frozen_prompts[0]["tool_declarations"] == []
        assert executor.run.call_count == 1
