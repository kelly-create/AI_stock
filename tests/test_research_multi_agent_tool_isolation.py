# -*- coding: utf-8 -*-
"""End-to-end research isolation contract for the real multi-agent runner."""

from __future__ import annotations

import copy
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tests.litellm_stub import ensure_litellm_stub

ensure_litellm_stub()

from src.agent.llm_adapter import LLMResponse, ToolCall
from src.agent.orchestrator import AgentOrchestrator
from src.agent.skills.base import Skill, SkillManager
from src.agent.tools.registry import ToolDefinition, ToolParameter, ToolRegistry
from src.services.research.canonical import canonical_hash


class _RecordingLLMAdapter:
    """Deterministic fake that snapshots each request before the runner mutates it."""

    def __init__(self, responses: list[LLMResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def call_with_tools(
        self,
        messages: list[dict[str, object]],
        tools: list[dict[str, object]],
        timeout: float | None = None,
    ) -> LLMResponse:
        self.calls.append(
            {
                "messages": copy.deepcopy(messages),
                "tools": copy.deepcopy(tools),
                "timeout": timeout,
            }
        )
        if not self._responses:
            raise AssertionError("multi-agent run made an unexpected extra LLM call")
        return self._responses.pop(0)

    @property
    def remaining_responses(self) -> int:
        return len(self._responses)


def _technical_opinion() -> str:
    return json.dumps(
        {
            "signal": "hold",
            "confidence": 0.65,
            "reasoning": "Frozen evidence supports a deterministic hold opinion.",
            "key_levels": {
                "support": 1400.0,
                "resistance": 1450.0,
                "stop_loss": 1380.0,
            },
            "trend_score": 55,
            "ma_alignment": "neutral",
            "volume_status": "normal",
            "pattern": "none",
        }
    )


def _decision_dashboard() -> str:
    return json.dumps(
        {
            "stock_name": "Kweichow Moutai",
            "sentiment_score": 55,
            "trend_prediction": "range-bound",
            "operation_advice": "hold",
            "decision_type": "hold",
            "confidence_level": "Medium",
            "dashboard": {
                "phase_decision": {
                    "phase_context": "post_close",
                    "action_window": "next session",
                    "immediate_action": "hold",
                    "watch_conditions": ["support confirmation"],
                    "next_check_time": "next close",
                    "confidence_reason": "frozen research fixture",
                    "data_limitations": [],
                },
                "core_conclusion": {
                    "one_sentence": "Hold while price remains inside the frozen range.",
                    "signal_type": "hold",
                    "position_advice": {
                        "no_position": "watch",
                        "has_position": "hold",
                    },
                },
            },
            "analysis_summary": "The frozen evidence supports a hold decision.",
            "key_points": ["No mutable tool evidence was accepted."],
            "risk_warning": "Historical snapshot only.",
        }
    )


def test_locked_multi_agent_rejects_hallucinated_tool_without_mutating_snapshot() -> None:
    debate_sentinel = "DSA-MULTI-DEBATE-DECISION-ONLY"
    handler_calls: list[str] = []

    def _trap_get_stock_info(stock_code: str) -> dict[str, str]:
        handler_calls.append(stock_code)
        return {"stock_code": stock_code, "source": "mutable-ambient-registry"}

    # This ambient registry represents the normal mutable Agent tool surface.
    # Research execution must retain the separate, empty task-scoped registry.
    ambient_registry = ToolRegistry()
    ambient_registry.register(
        ToolDefinition(
            name="get_stock_info",
            description="Trap handler that must never run in snapshot mode.",
            parameters=[
                ToolParameter(
                    name="stock_code",
                    type="string",
                    description="Stock code",
                )
            ],
            handler=_trap_get_stock_info,
        )
    )
    task_registry = ToolRegistry()
    adapter = _RecordingLLMAdapter(
        [
            LLMResponse(
                content="",
                tool_calls=[
                    ToolCall(
                        id="hallucinated-get-stock-info",
                        name="get_stock_info",
                        arguments={"stock_code": "600519"},
                    )
                ],
                usage={"total_tokens": 3},
                provider="fake",
                model="fake/research",
            ),
            LLMResponse(
                content=_technical_opinion(),
                usage={"total_tokens": 5},
                provider="fake",
                model="fake/research",
            ),
            LLMResponse(
                content=_decision_dashboard(),
                usage={"total_tokens": 7},
                provider="fake",
                model="fake/research",
            ),
        ]
    )
    orchestrator = AgentOrchestrator(
        tool_registry=task_registry,
        llm_adapter=adapter,
        max_steps=5,
        mode="quick",
        config=SimpleNamespace(
            agent_orchestrator_timeout_s=0,
            agent_risk_override=True,
        ),
        research_snapshot_locked=True,
    )
    task = "Analyze 600519 from the frozen research snapshot"
    context = {
        "stock_code": "600519",
        "stock_name": "Kweichow Moutai",
        "report_language": "en",
        "analysis_context_pack_summary": "FROZEN-CONTEXT-PACK-v1",
        "research_debate_prompt_context": debate_sentinel,
        "daily_history": [{"date": "2026-08-07", "close": 1410.0}],
        "news_context": "FROZEN-NEWS-EVIDENCE",
    }
    context_before = copy.deepcopy(context)
    memories: list[SimpleNamespace] = []

    def _new_enabled_memory() -> SimpleNamespace:
        memory = SimpleNamespace(enabled=True)
        memories.append(memory)
        return memory

    with patch(
        "src.agent.agents.base_agent.AgentMemory.from_config",
        side_effect=_new_enabled_memory,
    ):
        frozen_contract = orchestrator.build_research_snapshot_prompt(task, context)
        frozen_contract_before_run = copy.deepcopy(frozen_contract)
        result = orchestrator.run(task, context)

    assert result.success is True
    assert result.error is None
    assert result.dashboard is not None
    assert result.dashboard["decision_type"] == "hold"
    assert result.total_steps == 2
    assert result.total_tokens == 15
    assert result.model == "fake/research"
    assert adapter.remaining_responses == 0
    assert len(adapter.calls) == 3

    assert orchestrator.tool_registry is task_registry
    assert task_registry.list_names() == []
    assert ambient_registry.list_names() == ["get_stock_info"]
    assert handler_calls == []
    assert all(call["tools"] == [] for call in adapter.calls)
    assert len(result.tool_calls_log) == 1
    rejected_call = result.tool_calls_log[0]
    assert rejected_call["step"] == 1
    assert rejected_call["tool"] == "get_stock_info"
    assert rejected_call["arguments"] == {"stock_code": "600519"}
    assert rejected_call["success"] is False
    assert rejected_call["cached"] is False
    assert rejected_call["result_length"] > 0

    technical_contract, decision_contract = frozen_contract["stage_contracts"]
    assert [technical_contract["stage"], decision_contract["stage"]] == [
        "technical",
        "decision",
    ]
    assert frozen_contract["tool_declarations"] == []
    assert frozen_contract == frozen_contract_before_run
    assert context == context_before

    technical_contract_text = "\n".join(
        str(message.get("content", ""))
        for message in technical_contract["messages"]
    )
    decision_contract_text = "\n".join(
        str(message.get("content", ""))
        for message in decision_contract["messages"]
    )
    assert technical_contract_text.count("FROZEN-CONTEXT-PACK-v1") == 1
    assert technical_contract_text.count(debate_sentinel) == 0
    assert decision_contract_text.count("FROZEN-CONTEXT-PACK-v1") == 1
    assert decision_contract_text.count(debate_sentinel) == 1

    first_technical_messages = adapter.calls[0]["messages"]
    second_technical_messages = adapter.calls[1]["messages"]
    decision_messages = adapter.calls[2]["messages"]
    assert first_technical_messages == technical_contract["messages"]
    assert second_technical_messages[: len(first_technical_messages)] == first_technical_messages
    assert second_technical_messages[-1]["role"] == "tool"
    assert second_technical_messages[-1]["name"] == "get_stock_info"
    assert "not found in registry" in second_technical_messages[-1]["content"]
    assert decision_messages[:-1] == decision_contract["messages"][:-1]
    assert decision_messages[-1]["content"].startswith(
        decision_contract["messages"][-1]["content"].split("\n", 1)[0]
    )
    assert "FROZEN-CONTEXT-PACK-v1" in "\n".join(
        str(message.get("content", "")) for message in decision_messages
    )
    assert all(
        debate_sentinel
        not in "\n".join(str(message.get("content", "")) for message in messages)
        for messages in (first_technical_messages, second_technical_messages)
    )
    assert "\n".join(
        str(message.get("content", "")) for message in decision_messages
    ).count(debate_sentinel) == 1
    assert all(memory.enabled is False for memory in memories)


def test_specialist_contract_freezes_router_skill_and_exact_static_prompt() -> None:
    manager = SkillManager()
    manager.register(
        Skill(
            name="quality_gate",
            display_name="Quality Gate",
            description="Evaluate balance-sheet quality.",
            instructions="Require positive cash conversion and stable leverage.",
            required_tools=["get_stock_info"],
            enabled=True,
            default_router=True,
            market_regimes=["sideways"],
        )
    )
    orchestrator = AgentOrchestrator(
        tool_registry=ToolRegistry(),
        llm_adapter=_RecordingLLMAdapter([]),
        max_steps=5,
        mode="specialist",
        skill_manager=manager,
        config=SimpleNamespace(
            agent_orchestrator_timeout_s=0,
            agent_skill_routing="manual",
            agent_skills=["quality_gate"],
            agent_skill_concurrency=2,
            agent_skill_agent_timeout_s=20,
        ),
        research_snapshot_locked=True,
    )
    task = "Analyze 600519 from frozen evidence"
    context = {
        "stock_code": "600519",
        "stock_name": "Kweichow Moutai",
        "analysis_context_pack_summary": "FROZEN-CONTEXT",
    }

    contract = orchestrator.build_research_snapshot_prompt(task, context)
    specialist = contract["specialist_contract"]
    skill_contract = specialist["skills"][0]

    assert specialist["version"] == "specialist-prompt-contract-v1"
    assert specialist["router"]["manual_skill_ids"] == ["quality_gate"]
    assert specialist["router"]["max_selected"] == 4
    assert specialist["scheduler"] == {
        "max_concurrency": 2,
        "timeout_seconds": 20.0,
    }
    assert skill_contract["skill_id"] == "quality_gate"
    assert "positive cash conversion" in skill_contract["system_prompt"]
    assert len(specialist["router"]["policy_fingerprint"]) == 64
    assert len(specialist["prompt_builder_fingerprint"]) == 64

    runtime_context = orchestrator._build_context(task, context)
    with patch(
        "src.agent.skills.router.SkillRouter.select_skills",
        return_value=["quality_gate"],
    ):
        runtime_agent = orchestrator._build_specialist_agents(runtime_context)[0]
    assert runtime_agent.system_prompt(runtime_context) == skill_contract["system_prompt"]
    assert runtime_agent.tool_names == ["get_stock_info"]
    assert runtime_agent.memory.enabled is False

    global_skill = Skill(
        name="global_drift",
        display_name="Global Drift",
        description="Must never replace the frozen task skill.",
        instructions="UNFROZEN GLOBAL PROMPT",
    )
    with patch(
        "src.agent.skills.router.SkillRouter._get_routing_mode",
        return_value="manual",
    ), patch(
        "src.agent.skills.router.SkillRouter._get_manual_skills",
        return_value=["global_drift"],
    ), patch(
        "src.agent.skills.router.SkillRouter._get_available_skills",
        return_value=[global_skill],
    ):
        drift_context = orchestrator._build_context(task, context)
        drift_agent = orchestrator._build_specialist_agents(drift_context)[0]
    assert drift_agent.skill_id == "quality_gate"
    assert drift_agent.system_prompt(drift_context) == skill_contract["system_prompt"]
    assert "UNFROZEN GLOBAL PROMPT" not in drift_agent.system_prompt(drift_context)

    with patch(
        "src.services.research.code_fingerprint.inspect.getsource",
        side_effect=OSError("could not get source code"),
    ):
        frozen_build_contract = orchestrator.build_research_snapshot_prompt(
            task,
            context,
        )
    assert len(
        frozen_build_contract["specialist_contract"]["router"][
            "policy_fingerprint"
        ]
    ) == 64

    original_hash = canonical_hash(contract)
    manager.get("quality_gate").instructions = "Changed prompt semantics."
    changed_hash = canonical_hash(
        orchestrator.build_research_snapshot_prompt(task, context)
    )
    assert changed_hash != original_hash
