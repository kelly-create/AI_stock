from unittest.mock import patch

import pytest

from src.config import Config
from src.core.config_registry import build_schema_response, get_field_definition
from src.services.system_config_service import SystemConfigService


RESEARCH_ERROR_CODES = {
    "research_feature_dependency_missing",
    "portfolio_policy_gate_mode_invalid",
    "portfolio_policy_gate_dependency_missing",
}


def test_personal_research_flags_default_to_compatible_off_state() -> None:
    config = Config(stock_list=["600519"])

    assert config.database_migration_mode == "auto"
    assert config.personal_research_enabled is False
    assert config.durable_jobs_enabled is False
    assert config.durable_worker_id is None
    assert config.durable_worker_health_max_age_seconds == 45
    assert config.durable_worker_startup_timeout_seconds == 120
    assert config.tushare_research_enabled is False
    assert config.research_factors_enabled is False
    assert config.research_evidence_enabled is False
    assert config.research_debate_enabled is False
    assert config.research_thesis_enabled is False
    assert config.decision_outcome_v2_enabled is False
    assert config.portfolio_policy_gate_mode == "off"
    assert not (
        RESEARCH_ERROR_CODES
        & {issue.code for issue in config.validate_structured()}
    )


def test_database_migration_mode_is_normalized_and_validated() -> None:
    assert Config(
        stock_list=["600519"],
        database_migration_mode=" Explicit ",
    ).database_migration_mode == "explicit"
    with pytest.raises(ValueError, match="DATABASE_MIGRATION_MODE"):
        Config(stock_list=["600519"], database_migration_mode="write-on-start")


def test_complete_research_chain_and_shadow_gate_are_valid() -> None:
    config = Config(
        stock_list=["600519"],
        personal_research_enabled=True,
        durable_jobs_enabled=True,
        tushare_research_enabled=True,
        research_factors_enabled=True,
        research_evidence_enabled=True,
        research_debate_enabled=True,
        research_thesis_enabled=True,
        decision_outcome_v2_enabled=True,
        portfolio_policy_gate_mode="shadow",
    )

    assert config.research_feature_dependency_issues() == []
    config.assert_research_feature_dependencies()


def test_missing_research_dependencies_are_blocking() -> None:
    config = Config(
        stock_list=["600519"],
        research_factors_enabled=True,
        portfolio_policy_gate_mode="enforce",
    )

    issues = config.research_feature_dependency_issues()

    assert {issue.code for issue in issues} == {
        "research_feature_dependency_missing",
        "portfolio_policy_gate_dependency_missing",
    }
    assert all(issue.severity == "error" for issue in issues)
    with pytest.raises(ValueError, match="Invalid personal research feature configuration"):
        config.assert_research_feature_dependencies()


def test_runtime_env_loader_rejects_incomplete_research_chain() -> None:
    with patch.dict(
        "os.environ",
        {
            "STOCK_LIST": "600519",
            "RESEARCH_FACTORS_ENABLED": "true",
        },
        clear=True,
    ), patch("src.config.setup_env"), patch.object(
        Config,
        "_parse_litellm_yaml",
        return_value=[],
    ):
        with pytest.raises(ValueError, match="RESEARCH_FACTORS_ENABLED"):
            Config._load_from_env()


def test_runtime_env_loader_rejects_research_flag_typo() -> None:
    with patch.dict(
        "os.environ",
        {
            "STOCK_LIST": "600519",
            "PERSONAL_RESEARCH_ENABLED": "flase",
        },
        clear=True,
    ), patch("src.config.setup_env"), patch.object(
        Config,
        "_parse_litellm_yaml",
        return_value=[],
    ):
        with pytest.raises(ValueError, match="PERSONAL_RESEARCH_ENABLED"):
            Config._load_from_env()


def test_runtime_env_loader_reads_durable_worker_startup_contract() -> None:
    with patch.dict(
        "os.environ",
        {
            "STOCK_LIST": "600519",
            "DURABLE_WORKER_ID": "worker-a",
            "DURABLE_WORKER_HEALTH_MAX_AGE_SECONDS": "30",
            "DURABLE_WORKER_STARTUP_TIMEOUT_SECONDS": "75",
        },
        clear=True,
    ), patch("src.config.setup_env"), patch.object(
        Config,
        "_parse_litellm_yaml",
        return_value=[],
    ):
        config = Config._load_from_env()

    assert config.durable_worker_id == "worker-a"
    assert config.durable_worker_health_max_age_seconds == 30
    assert config.durable_worker_startup_timeout_seconds == 75


def test_system_config_cross_field_validation_rejects_incomplete_save() -> None:
    issues = SystemConfigService._validate_cross_field(
        effective_map={
            "PERSONAL_RESEARCH_ENABLED": "true",
            "RESEARCH_DEBATE_ENABLED": "true",
        },
        updated_keys={"RESEARCH_DEBATE_ENABLED"},
    )

    research_issues = [
        issue for issue in issues
        if issue.get("code") in RESEARCH_ERROR_CODES
    ]
    assert len(research_issues) == 1
    assert research_issues[0]["key"] == "RESEARCH_DEBATE_ENABLED"
    assert research_issues[0]["severity"] == "error"


def test_config_registry_exposes_all_nine_rollout_controls() -> None:
    schema = build_schema_response()
    research_category = next(
        category for category in schema["categories"]
        if category["category"] == "research"
    )
    fields = {field["key"]: field for field in research_category["fields"]}

    assert set(fields) == {
        "PERSONAL_RESEARCH_ENABLED",
        "DURABLE_JOBS_ENABLED",
        "TUSHARE_RESEARCH_ENABLED",
        "RESEARCH_FACTORS_ENABLED",
        "RESEARCH_EVIDENCE_ENABLED",
        "RESEARCH_DEBATE_ENABLED",
        "RESEARCH_THESIS_ENABLED",
        "DECISION_OUTCOME_V2_ENABLED",
        "PORTFOLIO_POLICY_GATE_MODE",
    }
    assert all(
        fields[key]["default_value"] == "false"
        for key in fields
        if key != "PORTFOLIO_POLICY_GATE_MODE"
    )
    gate = get_field_definition("PORTFOLIO_POLICY_GATE_MODE")
    assert gate["default_value"] == "off"
    assert gate["validation"]["enum"] == ["off", "shadow", "enforce"]
    expected_docs_url = "https://github.com/kelly-create/AI_stock/blob/main/docs/personal-research-rollout.md"
    assert all(field["docs"] == [{"label": "个人 A 股投研迁移与功能开关", "href": expected_docs_url}] for field in fields.values())
