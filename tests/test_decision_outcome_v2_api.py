"""API and read-contract tests for independent Decision Outcome v2."""

from __future__ import annotations

from datetime import date, datetime
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from api.v1.endpoints import decision_outcomes_v2 as endpoint
from api.v1.schemas.decision_outcomes_v2 import (
    DecisionOutcomeV2RunRequest,
    DecisionOutcomeV2StatsResponse,
)
from src.services.decision_outcome_v2_query_service import (
    DecisionOutcomeV2ContractError,
    DecisionOutcomeV2QueryService,
)
from src.services.decision_outcome_v2_stats import (
    DecisionOutcomeV2CalibrationSample,
    DecisionOutcomeV2Stats,
)
from src.services.research.canonical import canonical_hash, canonical_json


def _config(**overrides):
    values = {
        "durable_jobs_enabled": True,
        "personal_research_enabled": True,
        "tushare_research_enabled": True,
        "research_factors_enabled": True,
        "decision_outcome_v2_enabled": True,
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
            task_id=request.task_id if self.created else "existing-v2-task",
            status="pending",
            created=self.created,
        )

    def get_job(self, task_id):
        return SimpleNamespace(task_id=task_id, trace_id=task_id)


def test_v2_post_only_enqueues_versioned_durable_work() -> None:
    _Store.created = True
    with patch.object(endpoint, "DurableJobStore", _Store), patch.object(
        endpoint,
        "build_default_durable_job_registry",
        return_value=MagicMock(),
    ):
        response = endpoint.run_decision_outcomes_v2(
            DecisionOutcomeV2RunRequest(
                signal_id=42,
                horizons=["5d", "20d"],
                stock_code="sh600519",
                decision_profile="Balanced",
                limit=12,
                notify=True,
            ),
            idempotency_key="outcome-v2-run-0001",
            config=_config(),
            db_manager=MagicMock(),
        )

    assert response.accepted is True
    assert response.deduplicated is False
    assert response.engine_version == "personal-research-outcome-v2"
    queued = _Store.last_request
    assert queued.job_type == "decision_outcomes_v2"
    assert queued.payload_version == 1
    assert queued.payload == {
        "signal_id": 42,
        "horizons": ["5d", "20d"],
        "stock_code": "600519",
        "decision_profile": "balanced",
        "limit": 12,
        "notify": True,
    }
    assert "force" not in queued.payload
    assert queued.notify is True
    assert queued.query_source == "decision_outcome_v2_api"


def test_v2_post_reports_idempotent_deduplication() -> None:
    _Store.created = False
    with patch.object(endpoint, "DurableJobStore", _Store), patch.object(
        endpoint,
        "build_default_durable_job_registry",
        return_value=MagicMock(),
    ):
        response = endpoint.run_decision_outcomes_v2(
            DecisionOutcomeV2RunRequest(signal_id=42),
            idempotency_key="outcome-v2-run-0002",
            config=_config(),
            db_manager=MagicMock(),
        )
    assert response.task_id == "existing-v2-task"
    assert response.deduplicated is True


def test_v2_post_fails_closed_when_feature_is_disabled() -> None:
    with pytest.raises(HTTPException) as caught:
        endpoint.run_decision_outcomes_v2(
            DecisionOutcomeV2RunRequest(signal_id=42),
            idempotency_key="outcome-v2-run-0003",
            config=_config(decision_outcome_v2_enabled=False),
            db_manager=MagicMock(),
        )
    assert caught.value.status_code == 409
    assert caught.value.detail["error"] == "decision_outcome_v2_not_enabled"
    assert "DECISION_OUTCOME_V2_ENABLED" in caught.value.detail["missing"]


def test_v2_run_contract_has_no_force_escape_hatch() -> None:
    with pytest.raises(ValidationError, match="force"):
        DecisionOutcomeV2RunRequest(signal_id=42, force=True)


def test_v2_run_notification_is_opt_in() -> None:
    assert DecisionOutcomeV2RunRequest(signal_id=42).notify is False


def test_v2_stats_response_rejects_unmodeled_bucket_fields() -> None:
    aggregate = DecisionOutcomeV2Stats.aggregate([
        DecisionOutcomeV2CalibrationSample(
            engine_version="personal-research-outcome-v2",
            horizon="5d",
            profile="balanced",
            final_action_family="long",
            eval_status="pending",
        )
    ])
    payload = {
        "contract": "decision-outcome-v2-stats",
        "version": "v1",
        "engine_version": "personal-research-outcome-v2",
        "horizons": ["5d"],
        **aggregate,
    }

    response = DecisionOutcomeV2StatsResponse.model_validate(payload)
    assert len(response.buckets[0].bins) == 5
    assert response.buckets[0].accuracy is None

    invalid = json.loads(json.dumps(payload))
    invalid["buckets"][0]["unexpected"] = True
    with pytest.raises(ValidationError):
        DecisionOutcomeV2StatsResponse.model_validate(invalid)


class _ReadService:
    def list_outcomes(self, **kwargs):
        self.list_query = kwargs
        return {
            "contract": "decision-outcome-v2-collection",
            "version": "v1",
            "items": [],
            "total": 0,
            "page": kwargs["page"],
            "page_size": kwargs["page_size"],
        }

    def get_stats(self, **kwargs):
        self.stats_query = kwargs
        return {
            "contract": "decision-outcome-v2-stats",
            "version": "v1",
            "engine_version": "personal-research-outcome-v2",
            "horizons": ["5d", "10d", "20d"],
            "bucket_dimensions": [
                "engine",
                "horizon",
                "profile",
                "final_action_family",
            ],
            "minimum_completed_sample_size": 30,
            "calibration_bin_count": 5,
            "buckets": [],
        }

    def list_for_signal(self, signal_id, *, engine_version=None):
        self.signal_query = (signal_id, engine_version)
        return {
            "contract": "decision-outcome-v2-collection",
            "version": "v1",
            "items": [],
            "total": 0,
            "page": 1,
            "page_size": 100,
        }


def test_v2_history_gets_are_readable_without_feature_flag(monkeypatch) -> None:
    service = _ReadService()
    monkeypatch.setattr(endpoint, "_query_service", lambda db_manager: service)
    # GET handlers deliberately have no Config/feature-flag dependency.  A
    # disabled deployment can still audit immutable historical outcomes.
    result = endpoint.list_decision_outcomes_v2(
        page=1,
        page_size=20,
        db_manager=MagicMock(),
    )
    assert result.total == 0
    assert result.contract == "decision-outcome-v2-collection"


def test_v2_routes_declare_admin_cookie_and_202_contract(monkeypatch) -> None:
    monkeypatch.setattr(endpoint, "_query_service", lambda db_manager: _ReadService())
    app = FastAPI()
    app.include_router(endpoint.router, prefix="/decision-signals")
    paths = TestClient(app).get("/openapi.json").json()["paths"]
    post = paths["/decision-signals/outcomes-v2/run"]["post"]
    assert post["security"] == [{"AdminSessionCookie": []}]
    assert "202" in post["responses"]
    assert paths["/decision-signals/outcomes-v2"]["get"]["security"] == [
        {"AdminSessionCookie": []}
    ]
    assert "/decision-signals/{signal_id}/outcomes-v2" in paths


def _row() -> SimpleNamespace:
    dataset_hashes = ["a" * 64]
    observation = {
        "contract": "decision-outcome-v2",
        "horizon": "5d",
        "engine_version": "personal-research-outcome-v2",
        "final_action_family": "long",
        "eval_status": "pending",
        "outcome": None,
        "direction_correct": None,
        "reason_code": "insufficient_xshg_sessions",
        "signal_session": "2026-08-10",
        "entry_trade_date": None,
        "end_trade_date": None,
        "trading_day_count": None,
        "entry_raw_open": None,
        "entry_adj_factor": None,
        "end_adjusted_close": None,
        "stock_return_pct": None,
        "directional_return_pct": None,
        "mfe_pct": None,
        "mae_pct": None,
        "dataset_hashes": dataset_hashes,
        "csi300_code": "000300.SH",
        "csi300_name": "CSI 300",
        "csi300_status": "unavailable",
        "csi300_reason_code": "not_evaluated",
        "csi300_return_pct": None,
        "csi300_stock_excess_return_pct": None,
        "csi300_directional_excess_return_pct": None,
        "sw1_code": None,
        "sw1_name": None,
        "sw1_status": "unavailable",
        "sw1_reason_code": "not_evaluated",
        "sw1_return_pct": None,
        "sw1_stock_excess_return_pct": None,
        "sw1_directional_excess_return_pct": None,
    }
    values = {
        "id": 1,
        "signal_id": 42,
        "outcome_contract": "decision-outcome-v2",
        "horizon": "5d",
        "engine_version": "personal-research-outcome-v2",
        "eval_status": "pending",
        "final_action_family": "long",
        "outcome": None,
        "direction_correct": None,
        "reason_code": "insufficient_xshg_sessions",
        "execution_status": "pending",
        "signal_created_at": datetime(2026, 8, 10, 8),
        "signal_session": date(2026, 8, 10),
        "stock_code": "600519",
        "market": "cn",
        "source_type": "analysis",
        "signal_action": "buy",
        "signal_horizon": "10d",
        "signal_status": "active",
        "decision_profile": "balanced",
        "research_snapshot_hash": "b" * 64,
        "policy_version": "v1",
        "policy_hash": "c" * 64,
        "policy_evaluation_hash": "d" * 64,
        "portfolio_snapshot_ref": "snapshot:1",
        "prompt_version": "v1",
        "research_stance": "bullish",
        "proposed_account_action": "open_candidate",
        "final_account_action": "open_candidate",
        "policy_mode": "shadow",
        "policy_verdict": "allow",
        "policy_allowed": True,
        "would_block": False,
        "confidence": 0.7,
        "signal_score": 70.0,
        "value_quality_score": 70.0,
        "trend_timing_score": 70.0,
        "catalyst_score": 70.0,
        "risk_score": 30.0,
        "evidence_quality_score": 80.0,
        "entry_trade_date": None,
        "entry_raw_open": None,
        "entry_adj_factor": None,
        "end_trade_date": None,
        "trading_day_count": None,
        "end_adjusted_close": None,
        "stock_return_pct": None,
        "directional_return_pct": None,
        "mfe_pct": None,
        "mae_pct": None,
        "csi300_code": "000300.SH",
        "csi300_name": "CSI 300",
        "csi300_status": "unavailable",
        "csi300_reason_code": "not_evaluated",
        "csi300_return_pct": None,
        "csi300_stock_excess_return_pct": None,
        "csi300_directional_excess_return_pct": None,
        "sw1_code": None,
        "sw1_name": None,
        "sw1_status": "unavailable",
        "sw1_reason_code": "not_evaluated",
        "sw1_return_pct": None,
        "sw1_stock_excess_return_pct": None,
        "sw1_directional_excess_return_pct": None,
        "dataset_hashes_json": json.dumps(dataset_hashes),
        "observation_json": canonical_json(observation, exclude_volatile=False),
        "observation_hash": canonical_hash(observation, exclude_volatile=False),
        "evaluated_at": None,
        "created_at": datetime(2026, 8, 10, 9),
        "updated_at": datetime(2026, 8, 10, 9),
    }
    return SimpleNamespace(**values)


def test_v2_query_serialization_verifies_and_exposes_frozen_lineage() -> None:
    result = DecisionOutcomeV2QueryService.serialize(_row())
    assert result["policy_evaluation_hash"] == "d" * 64
    assert result["dataset_hashes"] == ["a" * 64]
    assert result["csi300"]["reason_code"] == "not_evaluated"


def test_v2_query_serialization_fails_closed_on_tampered_observation() -> None:
    row = _row()
    row.observation_json = canonical_json(
        {**json.loads(row.observation_json), "reason_code": "tampered"},
        exclude_volatile=False,
    )
    with pytest.raises(DecisionOutcomeV2ContractError, match="observation_hash"):
        DecisionOutcomeV2QueryService.serialize(row)
