"""Documentation closeout checks for the staged personal-research rollout."""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_personal_research_topics_are_indexed_and_locally_linkable() -> None:
    topic_paths = (
        DOCS / "personal-research-rollout.md",
        DOCS / "personal-research-production-acceptance.md",
        DOCS / "personal-research-watchlist-reconciliation.md",
        DOCS / "personal-research-execution-artifacts.md",
        DOCS / "decision-outcome-v2.md",
        DOCS / "decision-signals.md",
    )
    index = _read("docs/INDEX.md")
    index_en = _read("docs/INDEX_EN.md")

    for topic_path in topic_paths[:5]:
        assert topic_path.name in index
        assert topic_path.name in index_en

    link_pattern = re.compile(r"(?<!!)\[[^]]+\]\(([^)]+)\)")
    for topic_path in topic_paths:
        text = topic_path.read_text(encoding="utf-8")
        for raw_target in link_pattern.findall(text):
            target = raw_target.split("#", 1)[0]
            if not target or "://" in target or target.startswith("mailto:"):
                continue
            assert (topic_path.parent / target).resolve().exists(), (
                f"broken local documentation link in {topic_path.name}: {raw_target}"
            )


def test_personal_research_docs_capture_rollout_safety_boundaries() -> None:
    rollout = _read("docs/personal-research-rollout.md")
    artifacts = _read("docs/personal-research-execution-artifacts.md")
    decision_signals = _read("docs/decision-signals.md")

    for flag in (
        "PERSONAL_RESEARCH_ENABLED",
        "DURABLE_JOBS_ENABLED",
        "TUSHARE_RESEARCH_ENABLED",
        "RESEARCH_FACTORS_ENABLED",
        "RESEARCH_EVIDENCE_ENABLED",
        "RESEARCH_DEBATE_ENABLED",
        "RESEARCH_THESIS_ENABLED",
    ):
        assert f"`{flag}` | `false`" in artifacts

    assert "`PORTFOLIO_POLICY_GATE_MODE` | `off`" in artifacts
    assert "Debate 是按需触发，不是全量执行" in artifacts
    assert "Thesis 不要求 Debate" in artifacts
    assert "不伪造 0" in artifacts
    assert "7 个交易日" in artifacts
    assert "Decision Outcome v1 / v2 边界" in artifacts
    assert "v1 结果不等于个人投研 Outcome v2" in decision_signals
    assert "python -m src.migrations --check" in rollout
    assert "python -m src.migrations --apply" in rollout


def test_static_openapi_contains_only_accepted_personal_research_contracts() -> None:
    spec = json.loads(_read("docs/architecture/api_spec.json"))
    for path in (
        "/api/v1/research/watchlist",
        "/api/v1/research/universe",
        "/api/v1/research/personal/runs",
        "/api/v1/research/personal/artifacts/skills/{execution_hash}",
        "/api/v1/research/personal/artifacts/debate-reviews/{review_hash}",
        "/api/v1/research/personal/artifacts/theses/by-signal/{decision_signal_id}",
        "/api/v1/research/personal/artifacts/theses/latest",
        "/api/v1/research/personal/artifacts/theses/{thesis_hash}",
        "/api/v1/portfolio/accounts/{account_id}/reconciliations/preview",
        "/api/v1/portfolio/accounts/{account_id}/reconciliations/apply",
        "/api/v1/decision-signals/outcomes-v2/run",
        "/api/v1/decision-signals/outcomes-v2",
        "/api/v1/decision-signals/outcomes-v2/stats",
        "/api/v1/decision-signals/{signal_id}/outcomes-v2",
    ):
        assert path in spec["paths"]

    outcome_doc = _read("docs/decision-outcome-v2.md")
    assert "T+1" in outcome_doc
    assert "5/10/20" in outcome_doc
    assert "MFE/MAE" in outcome_doc
    assert "n/30" in outcome_doc
    assert "`null`" in outcome_doc


def test_production_acceptance_record_keeps_real_rollout_gates_pending() -> None:
    acceptance = _read("docs/personal-research-production-acceptance.md")

    for gate in (
        "G0 范围",
        "G1 契约",
        "G2 后端",
        "G3 Web",
        "G4 迁移",
        "G5 备份恢复",
        "G6 镜像与编排",
        "G7 flag-off 部署",
        "G8 分阶段 canary",
        "G9 Policy Shadow",
    ):
        assert gate in acceptance

    assert "7 个真实交易日" in acceptance
    assert "PORTFOLIO_POLICY_GATE_MODE" in acceptance
    assert "不可宣称整个项目已经正常上线" in acceptance
    assert "不得填零或跳过" in acceptance


def test_unreleased_changelog_keeps_flat_personal_research_entries() -> None:
    changelog = _read("docs/CHANGELOG.md")
    unreleased = changelog.split("## [Unreleased]", 1)[1].split("\n## ", 1)[0]

    assert not any(line.startswith("### ") for line in unreleased.splitlines())
    assert "所有 flags 默认关闭" in unreleased
    assert "Debate 按条件触发而非全量运行" in unreleased
    assert "Decision Outcome v1/v2 相互独立" in unreleased
