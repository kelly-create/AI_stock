from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from api.v1.endpoints.personal_research_runs import (
    create_personal_research_run,
)
from api.v1.schemas.personal_research_runs import PersonalResearchRunRequest


def _config(**overrides):
    values = {
        "durable_jobs_enabled": True,
        "personal_research_enabled": True,
        "tushare_research_enabled": True,
        "research_factors_enabled": True,
        "research_evidence_enabled": True,
        "research_debate_enabled": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _Store:
    created = True
    last_request = None

    def __init__(self, registry, db_manager=None):
        self.registry = registry
        self.db_manager = db_manager

    def enqueue(self, request):
        type(self).last_request = request
        return SimpleNamespace(
            task_id=request.task_id if self.created else "existing-task",
            status="pending",
            created=self.created,
        )

    def get_job(self, task_id):
        return SimpleNamespace(task_id=task_id, trace_id=task_id)


def test_manual_budget_override_requires_explicit_ack() -> None:
    with pytest.raises(ValidationError, match="acknowledgement"):
        PersonalResearchRunRequest(
            stock_code="600519",
            manual_daily_override=True,
        )


def test_run_enqueue_is_durable_stock_scoped_and_idempotent() -> None:
    _Store.created = True
    with patch(
        "api.v1.endpoints.personal_research_runs.DurableJobStore",
        _Store,
    ), patch(
        "api.v1.endpoints.personal_research_runs.build_default_durable_job_registry",
        return_value=MagicMock(),
    ):
        response = create_personal_research_run(
            PersonalResearchRunRequest(
                stock_code="sh600519",
                requested_mode="standard",
                priority=70,
                notify=False,
            ),
            idempotency_key="research-run-0001",
            config=_config(),
            db_manager=MagicMock(),
        )

    assert response.created is True
    assert response.deduplicated is False
    assert response.stock_code == "600519"
    assert response.resolved_mode == "standard"
    queued = _Store.last_request
    assert queued.job_type == "personal_research"
    assert queued.dedupe_key == "personal-research:cn:600519:standard"
    assert queued.idempotency_key == "personal-research:research-run-0001"
    assert queued.payload == {
        "stock_code": "600519",
        "requested_mode": "standard",
        "priority": 70,
        "manual_daily_override": False,
        "report_type": None,
        "notify": False,
        "query_source": "personal_research_api",
        "policy_account_id": None,
        "target_weight_pct": None,
        "report_language": None,
    }


def test_policy_target_requires_server_account_and_is_enqueued_without_raw_facts() -> None:
    with pytest.raises(ValidationError, match="must be supplied together"):
        PersonalResearchRunRequest(
            stock_code="600519",
            target_weight_pct=8.0,
        )

    _Store.created = True
    with patch(
        "api.v1.endpoints.personal_research_runs.DurableJobStore",
        _Store,
    ), patch(
        "api.v1.endpoints.personal_research_runs.build_default_durable_job_registry",
        return_value=MagicMock(),
    ):
        create_personal_research_run(
            PersonalResearchRunRequest(
                stock_code="600519",
                policy_account_id=7,
                target_weight_pct=8.0,
            ),
            idempotency_key="research-run-policy-0001",
            config=_config(),
            db_manager=MagicMock(),
        )

    assert _Store.last_request.payload["policy_account_id"] == 7
    assert _Store.last_request.payload["target_weight_pct"] == 8.0
    assert "portfolio_context" not in _Store.last_request.payload


def test_existing_idempotent_run_is_reported_as_deduplicated() -> None:
    _Store.created = False
    with patch(
        "api.v1.endpoints.personal_research_runs.DurableJobStore",
        _Store,
    ), patch(
        "api.v1.endpoints.personal_research_runs.build_default_durable_job_registry",
        return_value=MagicMock(),
    ):
        response = create_personal_research_run(
            PersonalResearchRunRequest(stock_code="600519", requested_mode="quick"),
            idempotency_key="research-run-0002",
            config=_config(),
            db_manager=MagicMock(),
        )

    assert response.task_id == "existing-task"
    assert response.created is False
    assert response.deduplicated is True


@pytest.mark.parametrize("stock_code", ["AAPL", "hk00700", "", "123"])
def test_run_rejects_non_a_share_identity(stock_code: str) -> None:
    request_code = stock_code or " "
    with pytest.raises(HTTPException) as caught:
        create_personal_research_run(
            PersonalResearchRunRequest(stock_code=request_code),
            idempotency_key="research-run-0003",
            config=_config(),
            db_manager=MagicMock(),
        )
    assert caught.value.status_code == 400


def test_resolved_debate_requires_debate_capability() -> None:
    with pytest.raises(HTTPException) as caught:
        create_personal_research_run(
            PersonalResearchRunRequest(
                stock_code="600519",
                requested_mode="debate",
            ),
            idempotency_key="research-run-0004",
            config=_config(research_debate_enabled=False),
            db_manager=MagicMock(),
        )
    assert caught.value.status_code == 409
    assert caught.value.detail["error"] == "personal_research_not_enabled"
    assert "RESEARCH_DEBATE_ENABLED" in caught.value.detail["missing"]


def test_run_rejects_when_durable_or_research_dependencies_are_off() -> None:
    with pytest.raises(HTTPException) as caught:
        create_personal_research_run(
            PersonalResearchRunRequest(stock_code="600519"),
            idempotency_key="research-run-0005",
            config=_config(durable_jobs_enabled=False),
            db_manager=MagicMock(),
        )
    assert caught.value.status_code == 409
    assert "DURABLE_JOBS_ENABLED" in caught.value.detail["missing"]
