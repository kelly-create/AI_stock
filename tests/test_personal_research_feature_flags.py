from unittest.mock import patch

import pytest

from src.config import Config
from src.core.config_registry import build_schema_response, get_field_definition
from src.services.system_config_service import SystemConfigService


RESEARCH_ERROR_CODES = {
    "research_feature_dependency_missing",
    "tushare_research_token_missing",
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
    assert config.tushare_global_calls_per_minute == 450
    assert config.tushare_max_inflight == 2
    assert config.tushare_endpoint_limits == {}
    assert config.research_factors_enabled is False
    assert config.research_evidence_enabled is False
    assert config.research_debate_enabled is False
    assert config.research_thesis_enabled is False
    assert config.decision_outcome_v2_enabled is False
    assert config.decision_outcome_v2_interval_minutes == 60
    assert config.decision_outcome_v2_batch_limit == 100
    assert config.portfolio_policy_gate_mode == "off"
    assert config.research_quick_daily_budget == 50
    assert config.research_standard_deep_daily_budget == 20
    assert config.research_debate_daily_budget == 8
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
        tushare_token="test-token",
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


def test_runtime_env_loader_reads_research_budget_contract() -> None:
    with patch.dict(
        "os.environ",
        {
            "STOCK_LIST": "600519",
            "RESEARCH_QUICK_DAILY_BUDGET": "60",
            "RESEARCH_STANDARD_DEEP_DAILY_BUDGET": "25",
            "RESEARCH_DEBATE_DAILY_BUDGET": "9",
        },
        clear=True,
    ), patch("src.config.setup_env"), patch.object(
        Config,
        "_parse_litellm_yaml",
        return_value=[],
    ):
        config = Config._load_from_env()

    assert config.research_quick_daily_budget == 60
    assert config.research_standard_deep_daily_budget == 25
    assert config.research_debate_daily_budget == 9


def test_runtime_env_loader_accepts_unlimited_personal_research_budgets() -> None:
    with patch.dict(
        "os.environ",
        {
            "STOCK_LIST": "600519",
            "RESEARCH_QUICK_DAILY_BUDGET": "0",
            "RESEARCH_STANDARD_DEEP_DAILY_BUDGET": "0",
            "RESEARCH_DEBATE_DAILY_BUDGET": "0",
        },
        clear=True,
    ), patch("src.config.setup_env"), patch.object(
        Config,
        "_parse_litellm_yaml",
        return_value=[],
    ):
        config = Config._load_from_env()

    assert config.research_quick_daily_budget == 0
    assert config.research_standard_deep_daily_budget == 0
    assert config.research_debate_daily_budget == 0


@pytest.mark.parametrize(
    ("field", "raw"),
    [
        ("RESEARCH_QUICK_DAILY_BUDGET", "-1"),
        ("RESEARCH_STANDARD_DEEP_DAILY_BUDGET", "2.5"),
        ("RESEARCH_DEBATE_DAILY_BUDGET", "true"),
    ],
)
def test_runtime_env_loader_rejects_invalid_research_budgets(
    field: str,
    raw: str,
) -> None:
    with patch.dict(
        "os.environ",
        {"STOCK_LIST": "600519", field: raw},
        clear=True,
    ), patch("src.config.setup_env"), patch.object(
        Config,
        "_parse_litellm_yaml",
        return_value=[],
    ):
        with pytest.raises(ValueError, match=field):
            Config._load_from_env()


def test_runtime_env_loader_reads_strict_tushare_quota_contract() -> None:
    with patch.dict(
        "os.environ",
        {
            "STOCK_LIST": "600519",
            "TUSHARE_GLOBAL_CALLS_PER_MINUTE": "400",
            "TUSHARE_MAX_INFLIGHT": "1",
            "TUSHARE_ENDPOINT_LIMITS_JSON": '{"cyq_chips":200,"daily":300}',
        },
        clear=True,
    ), patch("src.config.setup_env"), patch.object(
        Config,
        "_parse_litellm_yaml",
        return_value=[],
    ):
        config = Config._load_from_env()

    assert config.tushare_global_calls_per_minute == 400
    assert config.tushare_max_inflight == 1
    assert config.tushare_endpoint_limits == {"cyq_chips": 200, "daily": 300}


@pytest.mark.parametrize(
    "raw",
    [
        "[]",
        '{"cyq_chips":0}',
        '{"cyq_chips":451}',
        '{"cyq_chips":true}',
        '{"bad endpoint":10}',
    ],
)
def test_runtime_env_loader_rejects_invalid_tushare_endpoint_limits(raw: str) -> None:
    with patch.dict(
        "os.environ",
        {
            "STOCK_LIST": "600519",
            "TUSHARE_ENDPOINT_LIMITS_JSON": raw,
        },
        clear=True,
    ), patch("src.config.setup_env"), patch.object(
        Config,
        "_parse_litellm_yaml",
        return_value=[],
    ):
        with pytest.raises(ValueError, match="TUSHARE_ENDPOINT_LIMITS_JSON"):
            Config._load_from_env()


@pytest.mark.parametrize(
    ("field", "raw"),
    [
        ("TUSHARE_GLOBAL_CALLS_PER_MINUTE", "3OO"),
        ("TUSHARE_GLOBAL_CALLS_PER_MINUTE", "451"),
        ("TUSHARE_GLOBAL_CALLS_PER_MINUTE", "0"),
        ("TUSHARE_MAX_INFLIGHT", "2.0"),
        ("TUSHARE_MAX_INFLIGHT", "3"),
        ("TUSHARE_MAX_INFLIGHT", "0"),
    ],
)
def test_runtime_env_loader_fails_closed_for_invalid_tushare_safety_limits(
    field: str,
    raw: str,
) -> None:
    with patch.dict(
        "os.environ",
        {"STOCK_LIST": "600519", field: raw},
        clear=True,
    ), patch("src.config.setup_env"), patch.object(
        Config,
        "_parse_litellm_yaml",
        return_value=[],
    ):
        with pytest.raises(ValueError, match=field):
            Config._load_from_env()


def test_tushare_research_requires_an_explicit_token() -> None:
    config = Config(
        stock_list=["600519"],
        personal_research_enabled=True,
        durable_jobs_enabled=True,
        tushare_research_enabled=True,
    )

    issues = config.research_feature_dependency_issues()

    assert [(issue.code, issue.field) for issue in issues] == [
        ("tushare_research_token_missing", "TUSHARE_TOKEN")
    ]


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


def test_config_registry_exposes_rollout_and_tushare_quota_controls() -> None:
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
        "TUSHARE_GLOBAL_CALLS_PER_MINUTE",
        "TUSHARE_MAX_INFLIGHT",
        "TUSHARE_ENDPOINT_LIMITS_JSON",
        "RESEARCH_FACTORS_ENABLED",
        "RESEARCH_EVIDENCE_ENABLED",
        "RESEARCH_DEBATE_ENABLED",
        "RESEARCH_THESIS_ENABLED",
        "DECISION_OUTCOME_V2_ENABLED",
        "DECISION_OUTCOME_V2_INTERVAL_MINUTES",
        "DECISION_OUTCOME_V2_BATCH_LIMIT",
        "PORTFOLIO_POLICY_GATE_MODE",
        "RESEARCH_QUICK_DAILY_BUDGET",
        "RESEARCH_STANDARD_DEEP_DAILY_BUDGET",
        "RESEARCH_DEBATE_DAILY_BUDGET",
    }
    assert all(
        fields[key]["default_value"] == "false"
        for key in fields
        if key not in {
            "PORTFOLIO_POLICY_GATE_MODE",
            "TUSHARE_GLOBAL_CALLS_PER_MINUTE",
            "TUSHARE_MAX_INFLIGHT",
            "TUSHARE_ENDPOINT_LIMITS_JSON",
            "DECISION_OUTCOME_V2_INTERVAL_MINUTES",
            "DECISION_OUTCOME_V2_BATCH_LIMIT",
            "RESEARCH_QUICK_DAILY_BUDGET",
            "RESEARCH_STANDARD_DEEP_DAILY_BUDGET",
            "RESEARCH_DEBATE_DAILY_BUDGET",
        }
    )
    assert fields["TUSHARE_GLOBAL_CALLS_PER_MINUTE"]["default_value"] == "450"
    assert fields["TUSHARE_MAX_INFLIGHT"]["default_value"] == "2"
    assert fields["TUSHARE_ENDPOINT_LIMITS_JSON"]["default_value"] == "{}"
    assert fields["DECISION_OUTCOME_V2_INTERVAL_MINUTES"]["default_value"] == "60"
    assert fields["DECISION_OUTCOME_V2_BATCH_LIMIT"]["default_value"] == "100"
    assert fields["RESEARCH_QUICK_DAILY_BUDGET"]["default_value"] == "50"
    assert fields["RESEARCH_STANDARD_DEEP_DAILY_BUDGET"]["default_value"] == "20"
    assert fields["RESEARCH_DEBATE_DAILY_BUDGET"]["default_value"] == "8"
    assert fields["RESEARCH_QUICK_DAILY_BUDGET"]["validation"]["min"] == 0
    assert fields["RESEARCH_STANDARD_DEEP_DAILY_BUDGET"]["validation"]["min"] == 0
    assert fields["RESEARCH_DEBATE_DAILY_BUDGET"]["validation"]["min"] == 0
    gate = get_field_definition("PORTFOLIO_POLICY_GATE_MODE")
    assert gate["default_value"] == "off"
    assert gate["validation"]["enum"] == ["off", "shadow", "enforce"]
    expected_docs_url = "https://github.com/kelly-create/AI_stock/blob/main/docs/personal-research-rollout.md"
    assert all(field["docs"] == [{"label": "个人 A 股投研迁移与功能开关", "href": expected_docs_url}] for field in fields.values())
