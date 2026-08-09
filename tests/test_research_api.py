"""PR2 read-only research API contract tests."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict, field_validator

from api.app import create_app
from api.middlewares import auth as auth_middleware
from api.middlewares.error_handler import add_error_handlers
from api.v1.endpoints import research
from src.auth import COOKIE_NAME
from src.config import Config
from src.services.research.repositories import ResearchSnapshotRepository
from src.storage import DatabaseManager, ResearchDatasetSnapshotRecord


HASH = "a" * 64
EVIDENCE_HASH = "b" * 64
SNAPSHOT_HASH = "c" * 64
VALUE_HASH = "d" * 64
DEBATE_HASH = "e" * 64
REQUEST_HASH = "f" * 64
BULL_TURN_HASH = "1" * 64
BEAR_TURN_HASH = "2" * 64
PROMPT_FINGERPRINT = "3" * 64
NOW = "2026-08-08T08:00:00Z"
PAGE_CURSOR = "WyIyMDI2LTA4LTA4VDA4OjAwOjAwWiIsMV0"
NEXT_PAGE_CURSOR = "WyIyMDI2LTA4LTA4VDA4OjAwOjAwWiIsMl0"


class _ValidationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    count: int


class _ValidationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    count: int


class _SensitiveValidationRequest(BaseModel):
    count: int

    @field_validator("count")
    @classmethod
    def reject_value(cls, value: int) -> int:
        raise ValueError("private-validation-marker")


def _evidence_item() -> dict:
    return {
        "id": 4,
        "stock_code": "600519",
        "market": "cn",
        "evidence_engine_version": "evidence-v1",
        "claim_policy_version": "claim-policy-v1",
        "as_of": NOW,
        "available_at": NOW,
        "status": "available",
        "coverage": 1.0,
        "claim_count": 1,
        "citation_count": 1,
        "evidence": {
            "evidence_engine_version": "evidence-v1",
            "claim_policy_version": "claim-policy-v1",
            "stock_code": "600519",
            "market": "cn",
            "as_of": NOW,
            "available_at": NOW,
            "status": "available",
            "coverage": 1.0,
            "input_dataset_hashes": [HASH],
            "factor_snapshot_hash": HASH,
            "limitations": ["financial history is incomplete"],
            "claims": [
                {
                    "id": "claim-value-score",
                    "kind": "factor_metric",
                    "statement": "The value factor is supported.",
                    "status": "supported",
                    "citation_ids": ["citation-value-score"],
                    "limitations": [],
                    "available_at": NOW,
                }
            ],
            "citations": [
                {
                    "id": "citation-value-score",
                    "relation": "supports",
                    "artifact_type": "factor",
                    "artifact_hash": HASH,
                    "json_pointer": "/factors/value/score",
                    "value_hash": VALUE_HASH,
                    "available_at": NOW,
                    "source_name": "factor_snapshot",
                    "title": "Value factor score",
                    "excerpt": "72.0",
                    "canonical_url": "https://example.com/research/value",
                }
            ],
        },
        "input_dataset_hashes": [HASH],
        "factor_snapshot_hash": HASH,
        "evidence_hash": EVIDENCE_HASH,
        "origin_job_id": "job-1",
        "created_at": NOW,
    }


def _debate_item() -> dict:
    debate = {
        "debate_engine_version": "research-debate-v1",
        "output_schema_version": "research-debate-output-v1",
        "prompt_version": "research-debate-prompt-v1",
        "stock_code": "600519",
        "market": "cn",
        "as_of": NOW,
        "available_at": NOW,
        "status": "available",
        "evidence_snapshot_hash": EVIDENCE_HASH,
        "request_hash": REQUEST_HASH,
        "model_route_fingerprint": HASH,
        "bull_turn_hash": BULL_TURN_HASH,
        "bear_turn_hash": BEAR_TURN_HASH,
        "failed_stances": [],
        "limitations": ["Debate is derived model output, not new evidence."],
        "turns": [
            {
                "turn_hash": BULL_TURN_HASH,
                "prompt_fingerprint": PROMPT_FINGERPRINT,
                "model_used": "provider/model",
                "stance": "bull",
                "summary": "The frozen evidence supports a bounded upside case.",
                "arguments": [
                    {
                        "id": "bull-1",
                        "statement": "Value evidence supports the upside case.",
                        "claim_ids": ["claim-value-score"],
                        "citation_ids": ["citation-value-score"],
                        "confidence": 0.7,
                        "limitations": ["Only one reporting period is available."],
                    }
                ],
                "open_questions": ["Will the valuation gap persist?"],
            },
            {
                "turn_hash": BEAR_TURN_HASH,
                "prompt_fingerprint": PROMPT_FINGERPRINT,
                "model_used": "provider/model",
                "stance": "bear",
                "summary": "The same evidence leaves material downside uncertainty.",
                "arguments": [
                    {
                        "id": "bear-1",
                        "statement": "Incomplete history limits confidence.",
                        "claim_ids": ["claim-value-score"],
                        "citation_ids": ["citation-value-score"],
                        "confidence": 0.6,
                        "limitations": [],
                    }
                ],
                "open_questions": [],
            },
        ],
    }
    return {
        "id": 5,
        "stock_code": "600519",
        "market": "cn",
        "debate_engine_version": "research-debate-v1",
        "output_schema_version": "research-debate-output-v1",
        "prompt_version": "research-debate-prompt-v1",
        "evidence_snapshot_hash": EVIDENCE_HASH,
        "request_hash": REQUEST_HASH,
        "model_route_fingerprint": HASH,
        "as_of": NOW,
        "available_at": NOW,
        "status": "available",
        "bull_turn_hash": BULL_TURN_HASH,
        "bear_turn_hash": BEAR_TURN_HASH,
        "bull_argument_count": 1,
        "bear_argument_count": 1,
        "open_question_count": 1,
        "debate_hash": DEBATE_HASH,
        "debate": debate,
        "origin_job_id": "job-1",
        "created_at": NOW,
    }


class _Repo:
    last_evidence_query = None
    last_debate_query = None

    def get_latest_factors(self, **kwargs):
        if kwargs["stock_code"] == "missing":
            return None
        return {
            "id": 2,
            "stock_code": kwargs["stock_code"],
            "market": "cn",
            "company_profile": "industrial",
            "primary_horizon": 10,
            "requested_horizon": kwargs["horizon_days"],
            "requested_trend": {"name": f"return_{kwargs['horizon_days']}d", "score": 65.0},
            "engine_bundle_version": "factor-v1",
            "value_score": 72.0,
            "quality_score": None,
            "trend_score": 65.0,
            "catalyst_score": 50.0,
            "risk_penalty": 20.0,
            "factors": {
                "value": {"score": 72.0},
                "trend_timing": {
                    "score": 65.0,
                    "metrics": [
                        {
                            "name": f"return_{kwargs['horizon_days']}d",
                            "score": 65.0,
                        }
                    ],
                },
                "catalyst": {"score": 50.0},
                "risk": {"score": 20.0},
            },
            "input_dataset_hashes": [HASH],
            "status": "partial",
            "coverage": 0.8,
            "unknowns": [
                {
                    "component": "quality",
                    "metric": "roe_5y",
                    "reason": "metric is unavailable",
                }
            ],
            "as_of": NOW,
            "available_at": NOW,
            "content_hash": HASH,
            "origin_job_id": "job-1",
            "created_at": NOW,
        }

    def get_research_snapshot(self, snapshot_hash):
        if snapshot_hash != HASH:
            return None
        return {
            "id": 3,
            "stock_code": "600519",
            "market": "cn",
            "snapshot_version": "snapshot-v1",
            "field_dictionary_version": "fields-v1",
            "factor_engine_version": "factor-v1",
            "pack_version": "1.0",
            "prompt_version": "prompt-v1",
            "policy_version": "policy-v1",
            "model_route_fingerprint": HASH,
            "as_of": NOW,
            "available_at": NOW,
            "status": "available",
            "snapshot": {
                "context_pack": {"subject": {"code": "600519"}},
                "datasets": [],
                "factors": {"value": {"score": 72.0}},
                "evidence": _evidence_item()["evidence"],
                "debate": _debate_item()["debate"],
            },
            "snapshot_hash": HASH,
            "factor_snapshot_hash": HASH,
            "evidence_snapshot_hash": EVIDENCE_HASH,
            "debate_snapshot_hash": DEBATE_HASH,
            "origin_job_id": "job-1",
            "created_at": NOW,
        }

    def list_datasets(self, **kwargs):
        return [
            {
                "id": 1,
                "dataset": kwargs.get("dataset") or "daily",
                "scope_type": "stock",
                "scope_value": kwargs["scope_value"],
                "market": "cn",
                "provider": "tushare",
                "schema_version": "daily-v1",
                "trade_date": "2026-08-07",
                "report_date": None,
                "announcement_date": None,
                "data_as_of": NOW,
                "available_at": NOW,
                "observed_at": NOW,
                "status": "available",
                "normalized": [
                    {"trade_date": "20260807", "close": 1.0},
                    {"trade_date": "20260806", "close": 0.9},
                ],
                "content_hash": HASH,
                "raw_ref": {"content_sha256": HASH},
                "error_code": None,
                "error_message": None,
                "supersedes_hash": None,
                "origin_job_id": "job-1",
                "created_at": NOW,
            }
        ]

    def get_evidence(self, evidence_hash):
        if evidence_hash != EVIDENCE_HASH:
            return None
        return _evidence_item()

    def list_evidence(self, **kwargs):
        type(self).last_evidence_query = kwargs
        return {"items": [_evidence_item()], "next_cursor": NEXT_PAGE_CURSOR}

    def get_debate_snapshot(self, debate_hash):
        if debate_hash != DEBATE_HASH:
            return None
        return _debate_item()

    def list_debate_snapshots(self, **kwargs):
        type(self).last_debate_query = kwargs
        return {"items": [_debate_item()], "next_cursor": NEXT_PAGE_CURSOR}


def _client(monkeypatch) -> TestClient:
    monkeypatch.setattr(research, "_repo", lambda: _Repo())
    app = FastAPI()
    app.include_router(research.router, prefix="/research")
    return TestClient(app)


def test_research_factor_and_snapshot_queries_are_available_without_write_flags(
    monkeypatch,
) -> None:
    client = _client(monkeypatch)

    for horizon_days in (5, 10, 20):
        factor = client.get(
            f"/research/factors/600519?horizon_days={horizon_days}"
        )
        assert factor.status_code == 200
        assert factor.json()["primary_horizon"] == 10
        assert factor.json()["requested_horizon"] == horizon_days
        assert factor.json()["requested_trend"] == {
            "name": f"return_{horizon_days}d",
            "score": 65.0,
        }
        assert factor.json()["factors"]["value"]["score"] == 72.0

    snapshot = client.get(f"/research/snapshots/{HASH}")
    assert snapshot.status_code == 200
    assert snapshot.json()["snapshot_hash"] == HASH

    assert client.get("/research/factors/missing").status_code == 404
    assert client.get("/research/factors/600519?horizon_days=7").status_code == 400


def test_dataset_query_defaults_to_summary_and_requires_explicit_detail(monkeypatch) -> None:
    client = _client(monkeypatch)

    summary = client.get("/research/datasets/600519?dataset=daily")
    assert summary.status_code == 200
    assert summary.json()["detail"] is False
    assert summary.json()["row_limit"] == 1000
    assert summary.json()["items"][0]["normalized"] == {
        "row_count": 2,
        "fields": ["close", "trade_date"],
    }
    assert summary.json()["items"][0]["normalized_row_count"] == 2
    assert summary.json()["items"][0]["normalized_truncated"] is False

    detail = client.get(
        "/research/datasets/600519?dataset=daily&detail=true&row_limit=1"
    )
    assert detail.status_code == 200
    assert detail.json()["items"][0]["normalized"][0]["close"] == 1.0
    assert len(detail.json()["items"][0]["normalized"]) == 1
    assert detail.json()["items"][0]["normalized_row_count"] == 2
    assert detail.json()["items"][0]["normalized_truncated"] is True
    assert detail.json()["row_limit"] == 1

    assert client.get(
        "/research/datasets/600519?detail=true&row_limit=6001"
    ).status_code == 422


def test_research_openapi_contains_only_read_paths(monkeypatch) -> None:
    client = _client(monkeypatch)
    paths = client.get("/openapi.json").json()["paths"]
    assert set(paths) == {
        "/research/factors/{stock_code}",
        "/research/snapshots/{snapshot_hash}",
        "/research/datasets/{stock_code}",
        "/research/evidence",
        "/research/evidence/{evidence_hash}",
        "/research/debates",
        "/research/debates/{debate_hash}",
    }
    assert all(set(operations) == {"get"} for operations in paths.values())


def test_dataset_api_hides_stock_basic_observed_after_historical_cutoff(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager(
        db_url=f"sqlite:///{(tmp_path / 'research-api.db').as_posix()}"
    )
    try:
        with db.get_session() as session:
            session.add(
                ResearchDatasetSnapshotRecord(
                    dataset="stock_basic",
                    scope_type="stock",
                    scope_value="600519",
                    market="A",
                    provider="tushare",
                    schema_version="legacy-stock-basic-v1",
                    data_as_of=datetime(2001, 8, 27, tzinfo=timezone.utc),
                    available_at=datetime(2001, 8, 27, tzinfo=timezone.utc),
                    observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    status="available",
                    normalized_json=json.dumps([{"name": "2026 renamed"}]),
                    content_hash="f" * 64,
                    created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                )
            )
            session.commit()

        repository = ResearchSnapshotRepository(db)
        monkeypatch.setattr(research, "_repo", lambda: repository)
        app = FastAPI()
        app.include_router(research.router, prefix="/research")
        client = TestClient(app)

        response = client.get(
            "/research/datasets/600519",
            params={
                "dataset": "stock_basic",
                "as_of": "2010-01-01T00:00:00Z",
                "detail": "true",
            },
        )

        assert response.status_code == 200
        assert response.json()["items"] == []
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def test_evidence_list_requires_selector_and_forwards_stable_page_inputs(
    monkeypatch,
) -> None:
    client = _client(monkeypatch)

    missing_selector = client.get("/research/evidence")
    assert missing_selector.status_code == 400
    assert "job_id" in missing_selector.json()["detail"]["message"]

    response = client.get(
        "/research/evidence",
        params={
            "job_id": "job-1",
            "research_snapshot_hash": SNAPSHOT_HASH,
            "stock_code": "600519",
            "as_of": "2026-08-08T16:00:00+08:00",
            "cursor": PAGE_CURSOR,
            "limit": 20,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["next_cursor"] == NEXT_PAGE_CURSOR
    assert payload["items"][0]["evidence_hash"] == EVIDENCE_HASH
    assert "evidence" not in payload["items"][0]
    assert _Repo.last_evidence_query == {
        "job_id": "job-1",
        "research_snapshot_hash": SNAPSHOT_HASH,
        "stock_code": "600519",
        "as_of": datetime(2026, 8, 8, 8, 0, tzinfo=timezone.utc),
        "cursor": PAGE_CURSOR,
        "limit": 20,
    }

    naive_as_of = client.get(
        "/research/evidence",
        params={"job_id": "job-1", "as_of": "2026-08-08T08:00:00"},
    )
    assert naive_as_of.status_code == 400
    assert naive_as_of.json()["detail"]["message"] == (
        "as_of must include a UTC offset."
    )

    assert client.get(
        "/research/evidence",
        params={"job_id": "job-1", "limit": 101},
    ).status_code == 422


def test_evidence_detail_is_hash_addressed_and_returns_typed_claims(
    monkeypatch,
) -> None:
    client = _client(monkeypatch)

    response = client.get(f"/research/evidence/{EVIDENCE_HASH}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["evidence_hash"] == EVIDENCE_HASH
    assert payload["evidence"]["claims"][0]["status"] == "supported"
    assert payload["evidence"]["citations"][0]["canonical_url"].startswith(
        "https://"
    )
    assert client.get(f"/research/evidence/{'F' * 64}").status_code == 422
    assert client.get(f"/research/evidence/{'e' * 64}").status_code == 404


def test_evidence_api_accepts_large_immutable_dataset_lineage(monkeypatch) -> None:
    input_dataset_hashes = [f"{index:064x}" for index in range(130)]

    class _LargeLineageRepo(_Repo):
        @staticmethod
        def _item() -> dict:
            item = _evidence_item()
            item["input_dataset_hashes"] = input_dataset_hashes
            item["evidence"]["input_dataset_hashes"] = input_dataset_hashes
            return item

        def get_evidence(self, evidence_hash):
            if evidence_hash != EVIDENCE_HASH:
                return None
            return self._item()

        def list_evidence(self, **kwargs):
            return {"items": [self._item()], "next_cursor": None}

    client = _client(monkeypatch)
    monkeypatch.setattr(research, "_repo", lambda: _LargeLineageRepo())

    list_response = client.get(
        "/research/evidence",
        params={"job_id": "job-cyq-lineage"},
    )
    detail_response = client.get(f"/research/evidence/{EVIDENCE_HASH}")

    assert list_response.status_code == 200
    assert detail_response.status_code == 200
    assert len(list_response.json()["items"][0]["input_dataset_hashes"]) == 130
    assert len(detail_response.json()["evidence"]["input_dataset_hashes"]) == 130


def test_evidence_history_read_does_not_depend_on_write_feature_flag(
    monkeypatch,
) -> None:
    monkeypatch.setenv("RESEARCH_EVIDENCE_ENABLED", "false")
    Config.reset_instance()
    try:
        client = _client(monkeypatch)
        response = client.get(
            "/research/evidence",
            params={"job_id": "job-1"},
        )

        assert response.status_code == 200
        assert response.json()["items"][0]["evidence_hash"] == EVIDENCE_HASH
    finally:
        Config.reset_instance()


def test_debate_list_requires_primary_selector_and_forwards_page_filters(
    monkeypatch,
) -> None:
    client = _client(monkeypatch)

    missing_selector = client.get(
        "/research/debates",
        params={"evidence_snapshot_hash": EVIDENCE_HASH},
    )
    assert missing_selector.status_code == 400
    assert "job_id" in missing_selector.json()["detail"]["message"]

    response = client.get(
        "/research/debates",
        params={
            "job_id": "job-1",
            "research_snapshot_hash": SNAPSHOT_HASH,
            "stock_code": "600519",
            "evidence_snapshot_hash": EVIDENCE_HASH,
            "as_of": "2026-08-08T16:00:00+08:00",
            "cursor": PAGE_CURSOR,
            "limit": 20,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["next_cursor"] == NEXT_PAGE_CURSOR
    assert payload["items"][0]["debate_hash"] == DEBATE_HASH
    assert payload["items"][0]["request_hash"] == REQUEST_HASH
    assert "debate" not in payload["items"][0]
    assert "messages" not in response.text
    assert _Repo.last_debate_query == {
        "job_id": "job-1",
        "research_snapshot_hash": SNAPSHOT_HASH,
        "stock_code": "600519",
        "evidence_snapshot_hash": EVIDENCE_HASH,
        "as_of": datetime(2026, 8, 8, 8, 0, tzinfo=timezone.utc),
        "cursor": PAGE_CURSOR,
        "limit": 20,
    }

    naive_as_of = client.get(
        "/research/debates",
        params={"job_id": "job-1", "as_of": "2026-08-08T08:00:00"},
    )
    assert naive_as_of.status_code == 400
    assert naive_as_of.json()["detail"]["message"] == (
        "as_of must include a UTC offset."
    )
    assert client.get(
        "/research/debates",
        params={"job_id": "job-1", "limit": 101},
    ).status_code == 422


def test_debate_detail_is_hash_addressed_and_strictly_typed(monkeypatch) -> None:
    client = _client(monkeypatch)

    response = client.get(f"/research/debates/{DEBATE_HASH}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["debate_hash"] == DEBATE_HASH
    assert payload["debate"]["turns"][0]["stance"] == "bull"
    assert payload["debate"]["turns"][0]["arguments"][0]["claim_ids"] == [
        "claim-value-score"
    ]
    assert "messages" not in response.text
    assert client.get(f"/research/debates/{'F' * 64}").status_code == 422
    assert client.get(f"/research/debates/{'4' * 64}").status_code == 404


def test_debate_history_read_does_not_depend_on_write_feature_flag(
    monkeypatch,
) -> None:
    monkeypatch.setenv("RESEARCH_DEBATE_ENABLED", "false")
    Config.reset_instance()
    try:
        client = _client(monkeypatch)
        response = client.get(
            "/research/debates",
            params={"job_id": "job-1"},
        )

        assert response.status_code == 200
        assert response.json()["items"][0]["debate_hash"] == DEBATE_HASH
    finally:
        Config.reset_instance()


def test_debate_api_never_exposes_exact_request_messages(monkeypatch, caplog) -> None:
    secret = "system prompt with token=private-debate-secret"

    class _LeakyRepo(_Repo):
        def list_debate_snapshots(self, **kwargs):
            item = _debate_item()
            item["request_messages"] = [{"role": "system", "content": secret}]
            return {"items": [item], "next_cursor": None}

        def get_debate_snapshot(self, debate_hash):
            item = _debate_item()
            item["debate"]["request_messages"] = [
                {"role": "system", "content": secret}
            ]
            return item

    monkeypatch.setattr(research, "_repo", lambda: _LeakyRepo())
    app = FastAPI()
    app.include_router(research.router, prefix="/research")
    client = TestClient(app, raise_server_exceptions=False)

    responses = (
        client.get("/research/debates", params={"job_id": "job-1"}),
        client.get(f"/research/debates/{DEBATE_HASH}"),
    )

    for response in responses:
        assert response.status_code == 500
        assert response.json()["detail"] == {
            "error": "internal_error",
            "message": "Research debate query failed",
        }
        assert secret not in response.text
    assert secret not in caplog.text


def test_debate_list_rejects_secret_in_public_version_fields(
    monkeypatch,
    caplog,
) -> None:
    secret = "password:supersecret"
    for field_name in (
        "debate_engine_version",
        "output_schema_version",
        "prompt_version",
        "origin_job_id",
    ):
        item = _debate_item()
        item[field_name] = secret

        class _LeakyVersionRepo(_Repo):
            def list_debate_snapshots(self, **kwargs):
                return {"items": [item], "next_cursor": None}

        monkeypatch.setattr(research, "_repo", lambda: _LeakyVersionRepo())
        app = FastAPI()
        app.include_router(research.router, prefix="/research")
        client = TestClient(app, raise_server_exceptions=False)
        caplog.clear()

        response = client.get("/research/debates", params={"job_id": "job-1"})

        assert response.status_code == 500
        assert response.json()["detail"] == {
            "error": "internal_error",
            "message": "Research debate query failed",
        }
        assert secret not in response.text
        assert secret not in caplog.text


def test_debate_detail_rejects_secret_or_directive_in_every_public_field(
    monkeypatch,
    caplog,
) -> None:
    cases = (
        (
            ("debate_engine_version",),
            "password:supersecret",
            "password:supersecret",
        ),
        (
            ("output_schema_version",),
            "password:supersecret",
            "password:supersecret",
        ),
        (
            ("prompt_version",),
            "password:supersecret",
            "password:supersecret",
        ),
        (
            ("turns", 0, "model_used"),
            "https://alice:p4ss@llm.example/v1",
            "https://alice:p4ss@llm.example/v1",
        ),
        (
            ("turns", 0, "summary"),
            "token=private-debate-secret",
            "token=private-debate-secret",
        ),
        (
            ("turns", 0, "arguments", 0, "id"),
            "sk-abcdefghijklmnopqrstuvwxyz123456",
            "sk-abcdefghijklmnopqrstuvwxyz123456",
        ),
        (
            ("turns", 0, "arguments", 0, "statement"),
            "password:supersecret",
            "password:supersecret",
        ),
        (
            ("turns", 0, "arguments", 0, "claim_ids", 0),
            "password:supersecret",
            "password:supersecret",
        ),
        (
            ("turns", 0, "arguments", 0, "citation_ids", 0),
            "ghp_abcdefghijklmnopqrstuvwxyz123456",
            "ghp_abcdefghijklmnopqrstuvwxyz123456",
        ),
        (
            ("turns", 0, "arguments", 0, "limitations", 0),
            "token=private-argument-limit",
            "token=private-argument-limit",
        ),
        (
            ("turns", 0, "open_questions", 0),
            "file://private/model",
            "file://private/model",
        ),
        (
            ("limitations", 0),
            "Final recommendation: buy",
            "Final recommendation: buy",
        ),
        (
            ("failed_stances",),
            [{"stance": "bull", "error_code": "password:supersecret"}],
            "password:supersecret",
        ),
    )

    for path, unsafe_value, secret_text in cases:
        item = _debate_item()
        target = item["debate"]
        for part in path[:-1]:
            target = target[part]
        target[path[-1]] = unsafe_value

        class _LeakyAllowedFieldRepo(_Repo):
            def get_debate_snapshot(self, debate_hash):
                return item if debate_hash == DEBATE_HASH else None

        monkeypatch.setattr(
            research,
            "_repo",
            lambda: _LeakyAllowedFieldRepo(),
        )
        app = FastAPI()
        app.include_router(research.router, prefix="/research")
        client = TestClient(app, raise_server_exceptions=False)
        caplog.clear()

        response = client.get(f"/research/debates/{DEBATE_HASH}")

        assert response.status_code == 500
        assert response.json()["detail"] == {
            "error": "internal_error",
            "message": "Research debate query failed",
        }
        assert secret_text not in response.text
        assert secret_text not in caplog.text


def test_evidence_create_app_auth_and_error_responses_do_not_expose_payload(
    monkeypatch,
    tmp_path,
    caplog,
) -> None:
    class _ExplodingRepo(_Repo):
        def list_evidence(self, **kwargs):
            if kwargs.get("cursor") == PAGE_CURSOR:
                raise ValueError(
                    "private evidence payload: guaranteed buy; token=secret"
                )
            raise RuntimeError("private evidence payload: guaranteed buy; token=secret")

    monkeypatch.setattr(research, "_repo", lambda: _ExplodingRepo())
    monkeypatch.setattr(auth_middleware, "is_auth_enabled", lambda: True)
    monkeypatch.setattr(auth_middleware, "verify_session", lambda value: value == "valid")
    app = create_app(static_dir=tmp_path / "missing-static")
    client = TestClient(app, raise_server_exceptions=False)

    unauthorized = client.get(
        "/api/v1/research/evidence",
        params={"job_id": "job-1"},
    )
    assert unauthorized.status_code == 401

    client.cookies.set(COOKIE_NAME, "valid")
    missing_selector = client.get("/api/v1/research/evidence")
    invalid_cursor = client.get(
        "/api/v1/research/evidence",
        params={"job_id": "job-1", "cursor": "invalid-cursor"},
    )
    corrupted_repository = client.get(
        "/api/v1/research/evidence",
        params={"job_id": "job-1", "cursor": PAGE_CURSOR},
    )
    not_found = client.get(f"/api/v1/research/evidence/{'e' * 64}")
    invalid = client.get(f"/api/v1/research/evidence/{'F' * 64}")
    invalid_query = client.get(
        "/api/v1/research/evidence",
        params={"research_snapshot_hash": "private-payload-token=secret"},
    )
    invalid_limit = client.get(
        "/api/v1/research/evidence",
        params={"job_id": "job-1", "limit": "private-payload-token=secret"},
    )
    internal_error = client.get(
        "/api/v1/research/evidence",
        params={"job_id": "job-1"},
    )

    assert missing_selector.status_code == 400
    assert invalid_cursor.status_code == 422
    assert corrupted_repository.status_code == 500
    assert not_found.status_code == 404
    assert invalid.status_code == 422
    assert invalid_query.status_code == 422
    assert invalid_limit.status_code == 422
    assert internal_error.status_code == 500
    for response in (
        missing_selector,
        invalid_cursor,
        corrupted_repository,
        not_found,
        invalid,
        invalid_query,
        invalid_limit,
        internal_error,
    ):
        assert "guaranteed buy" not in response.text
        assert "token=secret" not in response.text
    assert internal_error.json() == {
        "error": "internal_error",
        "message": "Research evidence query failed",
    }
    assert corrupted_repository.json() == internal_error.json()
    assert "guaranteed buy" not in caplog.text
    assert "token=secret" not in caplog.text


def test_evidence_api_rejects_unsafe_versions_and_graph_ids_without_leaking(
    monkeypatch,
    caplog,
) -> None:
    secret = "password:supersecret"
    cases = (
        ("list", ("evidence_engine_version",)),
        ("list", ("claim_policy_version",)),
        ("list", ("origin_job_id",)),
        ("detail", ("evidence", "evidence_engine_version")),
        ("detail", ("evidence", "claim_policy_version")),
        ("detail", ("evidence", "claims", 0, "id")),
        ("detail", ("evidence", "claims", 0, "citation_ids", 0)),
        ("detail", ("evidence", "citations", 0, "id")),
    )
    for endpoint, path in cases:
        item = _evidence_item()
        target = item
        for part in path[:-1]:
            target = target[part]
        target[path[-1]] = secret

        class _LeakyEvidenceRepo(_Repo):
            def list_evidence(self, **kwargs):
                return {"items": [item], "next_cursor": None}

            def get_evidence(self, evidence_hash):
                return item if evidence_hash == EVIDENCE_HASH else None

        monkeypatch.setattr(research, "_repo", lambda: _LeakyEvidenceRepo())
        app = FastAPI()
        app.include_router(research.router, prefix="/research")
        client = TestClient(app, raise_server_exceptions=False)
        caplog.clear()
        response = (
            client.get("/research/evidence", params={"job_id": "job-1"})
            if endpoint == "list"
            else client.get(f"/research/evidence/{EVIDENCE_HASH}")
        )

        assert response.status_code == 500
        assert response.json()["detail"] == {
            "error": "internal_error",
            "message": "Research evidence query failed",
        }
        assert secret not in response.text
        assert secret not in caplog.text


def test_other_research_api_models_fail_closed_on_unsafe_public_fields(
    monkeypatch,
    caplog,
) -> None:
    secret = "password:supersecret"
    cases = (
        ("dataset", "schema_version", secret),
        ("dataset", "error_code", secret),
        ("dataset", "origin_job_id", secret),
        (
            "dataset",
            "error_message",
            "sk-abcdefghijklmnopqrstuvwxyz123456",
        ),
        ("factor", "engine_bundle_version", secret),
        ("factor", "origin_job_id", secret),
        ("snapshot", "snapshot_version", secret),
        ("snapshot", "field_dictionary_version", secret),
        ("snapshot", "factor_engine_version", secret),
        ("snapshot", "pack_version", secret),
        ("snapshot", "prompt_version", secret),
        ("snapshot", "policy_version", secret),
        ("snapshot", "model_route_fingerprint", secret),
        ("snapshot", "origin_job_id", secret),
        ("snapshot_debate", "summary", secret),
    )
    for artifact, field_name, unsafe_value in cases:
        class _LeakyResearchRepo(_Repo):
            def list_datasets(self, **kwargs):
                item = super().list_datasets(**kwargs)[0]
                if artifact == "dataset":
                    item[field_name] = unsafe_value
                return [item]

            def get_latest_factors(self, **kwargs):
                item = super().get_latest_factors(**kwargs)
                if artifact == "factor":
                    item[field_name] = unsafe_value
                return item

            def get_research_snapshot(self, snapshot_hash):
                item = super().get_research_snapshot(snapshot_hash)
                if item is None:
                    return None
                if artifact == "snapshot":
                    item[field_name] = unsafe_value
                elif artifact == "snapshot_debate":
                    debate = _debate_item()["debate"]
                    debate["turns"][0][field_name] = unsafe_value
                    item["snapshot"]["debate"] = debate
                return item

        monkeypatch.setattr(research, "_repo", lambda: _LeakyResearchRepo())
        app = FastAPI()
        app.include_router(research.router, prefix="/research")
        client = TestClient(app, raise_server_exceptions=False)
        caplog.clear()
        if artifact == "dataset":
            response = client.get("/research/datasets/600519")
            expected_message = "Research dataset query failed"
        elif artifact == "factor":
            response = client.get("/research/factors/600519")
            expected_message = "Research factor query failed"
        else:
            response = client.get(f"/research/snapshots/{HASH}")
            expected_message = "Research snapshot query failed"

        assert response.status_code == 500
        assert response.json()["detail"] == {
            "error": "internal_error",
            "message": expected_message,
        }
        assert unsafe_value not in response.text
        assert unsafe_value not in caplog.text


def test_evidence_api_rejects_graph_projection_and_page_drift(monkeypatch) -> None:
    cases = []

    count_drift = _evidence_item()
    count_drift["claim_count"] = 2
    cases.append(count_drift)

    duplicate_lineage = _evidence_item()
    duplicate_lineage["evidence"]["input_dataset_hashes"] = [HASH, HASH]
    cases.append(duplicate_lineage)

    duplicate_claim = _evidence_item()
    duplicate_claim["evidence"]["claims"].append(
        json.loads(json.dumps(duplicate_claim["evidence"]["claims"][0]))
    )
    cases.append(duplicate_claim)

    dangling_reference = _evidence_item()
    dangling_reference["evidence"]["claims"][0]["citation_ids"] = [
        "citation-missing"
    ]
    cases.append(dangling_reference)

    orphan_citation = _evidence_item()
    extra_citation = json.loads(
        json.dumps(orphan_citation["evidence"]["citations"][0])
    )
    extra_citation["id"] = "citation-orphan"
    orphan_citation["evidence"]["citations"].append(extra_citation)
    cases.append(orphan_citation)

    for item in cases:
        class _DriftedEvidenceRepo(_Repo):
            def get_evidence(self, evidence_hash):
                return item if evidence_hash == EVIDENCE_HASH else None

        monkeypatch.setattr(research, "_repo", lambda: _DriftedEvidenceRepo())
        app = FastAPI()
        app.include_router(research.router, prefix="/research")
        response = TestClient(app, raise_server_exceptions=False).get(
            f"/research/evidence/{EVIDENCE_HASH}"
        )
        assert response.status_code == 500
        assert response.json()["detail"]["error"] == "internal_error"

    class _DuplicateEvidencePageRepo(_Repo):
        def list_evidence(self, **kwargs):
            return {
                "items": [_evidence_item(), _evidence_item()],
                "next_cursor": None,
            }

    monkeypatch.setattr(research, "_repo", lambda: _DuplicateEvidencePageRepo())
    app = FastAPI()
    app.include_router(research.router, prefix="/research")
    duplicate_page = TestClient(app, raise_server_exceptions=False).get(
        "/research/evidence",
        params={"job_id": "job-1"},
    )
    assert duplicate_page.status_code == 500

    class _InvalidEvidenceCursorRepo(_Repo):
        def list_evidence(self, **kwargs):
            return {"items": [_evidence_item()], "next_cursor": "not-a-cursor"}

    monkeypatch.setattr(research, "_repo", lambda: _InvalidEvidenceCursorRepo())
    app = FastAPI()
    app.include_router(research.router, prefix="/research")
    invalid_cursor = TestClient(app, raise_server_exceptions=False).get(
        "/research/evidence",
        params={"job_id": "job-1"},
    )
    assert invalid_cursor.status_code == 500


def test_debate_api_rejects_graph_projection_and_page_drift(monkeypatch) -> None:
    cases = []

    count_drift = _debate_item()
    count_drift["bull_argument_count"] = 2
    cases.append(count_drift)

    turn_hash_drift = _debate_item()
    turn_hash_drift["debate"]["bull_turn_hash"] = BEAR_TURN_HASH
    cases.append(turn_hash_drift)

    duplicate_stance = _debate_item()
    duplicate_stance["debate"]["turns"][1] = json.loads(
        json.dumps(duplicate_stance["debate"]["turns"][0])
    )
    cases.append(duplicate_stance)

    overlapping_failure = _debate_item()
    overlapping_failure["debate"]["failed_stances"] = [
        {"stance": "bull", "error_code": "generation_failed"}
    ]
    cases.append(overlapping_failure)

    duplicate_reference = _debate_item()
    duplicate_reference["debate"]["turns"][0]["arguments"][0][
        "claim_ids"
    ] = ["claim-value-score", "claim-value-score"]
    cases.append(duplicate_reference)

    for item in cases:
        class _DriftedDebateRepo(_Repo):
            def get_debate_snapshot(self, debate_hash):
                return item if debate_hash == DEBATE_HASH else None

        monkeypatch.setattr(research, "_repo", lambda: _DriftedDebateRepo())
        app = FastAPI()
        app.include_router(research.router, prefix="/research")
        response = TestClient(app, raise_server_exceptions=False).get(
            f"/research/debates/{DEBATE_HASH}"
        )
        assert response.status_code == 500
        assert response.json()["detail"]["error"] == "internal_error"

    class _DuplicateDebatePageRepo(_Repo):
        def list_debate_snapshots(self, **kwargs):
            return {
                "items": [_debate_item(), _debate_item()],
                "next_cursor": None,
            }

    monkeypatch.setattr(research, "_repo", lambda: _DuplicateDebatePageRepo())
    app = FastAPI()
    app.include_router(research.router, prefix="/research")
    duplicate_page = TestClient(app, raise_server_exceptions=False).get(
        "/research/debates",
        params={"job_id": "job-1"},
    )
    assert duplicate_page.status_code == 500


def test_factor_api_reuses_complete_factor_contract(monkeypatch) -> None:
    cases = []

    duplicate_lineage = _Repo().get_latest_factors(
        stock_code="600519",
        horizon_days=10,
    )
    duplicate_lineage["input_dataset_hashes"] = [HASH, HASH]
    cases.append(duplicate_lineage)

    score_drift = _Repo().get_latest_factors(
        stock_code="600519",
        horizon_days=10,
    )
    score_drift["factors"]["value"]["score"] = 71.0
    cases.append(score_drift)

    unknown_drift = _Repo().get_latest_factors(
        stock_code="600519",
        horizon_days=10,
    )
    unknown_drift["unknowns"][0]["extra"] = "unexpected"
    cases.append(unknown_drift)

    future_availability = _Repo().get_latest_factors(
        stock_code="600519",
        horizon_days=10,
    )
    future_availability["available_at"] = "2026-08-09T08:00:00Z"
    cases.append(future_availability)

    for item in cases:
        class _DriftedFactorRepo(_Repo):
            def get_latest_factors(self, **kwargs):
                return item

        monkeypatch.setattr(research, "_repo", lambda: _DriftedFactorRepo())
        app = FastAPI()
        app.include_router(research.router, prefix="/research")
        response = TestClient(app, raise_server_exceptions=False).get(
            "/research/factors/600519"
        )
        assert response.status_code == 500
        assert response.json()["detail"]["error"] == "internal_error"


def test_research_snapshot_api_closes_nested_artifact_graph(monkeypatch) -> None:
    cases = []

    missing_factor_payload = _Repo().get_research_snapshot(HASH)
    missing_factor_payload["snapshot"].pop("factors")
    cases.append(missing_factor_payload)

    evidence_hash_drift = _Repo().get_research_snapshot(HASH)
    evidence_hash_drift["evidence_snapshot_hash"] = "9" * 64
    cases.append(evidence_hash_drift)

    missing_evidence_payload = _Repo().get_research_snapshot(HASH)
    missing_evidence_payload["snapshot"].pop("evidence")
    cases.append(missing_evidence_payload)

    dangling_debate_claim = _Repo().get_research_snapshot(HASH)
    dangling_debate_claim["snapshot"]["debate"]["turns"][0]["arguments"][0][
        "claim_ids"
    ] = ["claim-missing"]
    cases.append(dangling_debate_claim)

    unreachable_debate_citation = _Repo().get_research_snapshot(HASH)
    extra_claim = json.loads(
        json.dumps(unreachable_debate_citation["snapshot"]["evidence"]["claims"][0])
    )
    extra_claim["id"] = "claim-second"
    extra_claim["citation_ids"] = ["citation-second"]
    extra_citation = json.loads(
        json.dumps(
            unreachable_debate_citation["snapshot"]["evidence"]["citations"][0]
        )
    )
    extra_citation["id"] = "citation-second"
    unreachable_debate_citation["snapshot"]["evidence"]["claims"].append(
        extra_claim
    )
    unreachable_debate_citation["snapshot"]["evidence"]["citations"].append(
        extra_citation
    )
    unreachable_debate_citation["snapshot"]["debate"]["turns"][0]["arguments"][0][
        "claim_ids"
    ] = ["claim-second"]
    cases.append(unreachable_debate_citation)

    for item in cases:
        class _DriftedSnapshotRepo(_Repo):
            def get_research_snapshot(self, snapshot_hash):
                return item if snapshot_hash == HASH else None

        monkeypatch.setattr(research, "_repo", lambda: _DriftedSnapshotRepo())
        app = FastAPI()
        app.include_router(research.router, prefix="/research")
        response = TestClient(app, raise_server_exceptions=False).get(
            f"/research/snapshots/{HASH}"
        )
        assert response.status_code == 500
        assert response.json()["detail"]["error"] == "internal_error"


def test_repository_value_errors_are_internal_but_query_errors_remain_4xx(
    monkeypatch,
    caplog,
) -> None:
    marker = "private-corruption-marker"

    class _CorruptRepo(_Repo):
        def get_latest_factors(self, **kwargs):
            raise ValueError(marker)

        def get_research_snapshot(self, snapshot_hash):
            raise ValueError(marker)

        def list_datasets(self, **kwargs):
            raise ValueError(marker)

        def list_evidence(self, **kwargs):
            raise ValueError(marker)

        def list_debate_snapshots(self, **kwargs):
            raise ValueError(marker)

    monkeypatch.setattr(research, "_repo", lambda: _CorruptRepo())
    app = FastAPI()
    app.include_router(research.router, prefix="/research")
    client = TestClient(app, raise_server_exceptions=False)

    repository_responses = (
        client.get("/research/factors/600519"),
        client.get(f"/research/snapshots/{HASH}"),
        client.get("/research/datasets/600519"),
        client.get("/research/evidence", params={"job_id": "job-1"}),
        client.get("/research/debates", params={"job_id": "job-1"}),
    )
    assert all(response.status_code == 500 for response in repository_responses)
    assert all(marker not in response.text for response in repository_responses)
    assert marker not in caplog.text

    query_responses = (
        client.get(f"/research/factors/{'A' * 17}"),
        client.get(f"/research/snapshots/{'F' * 64}"),
        client.get(
            "/research/datasets/600519",
            params={"dataset": "invalid/value"},
        ),
        client.get(
            "/research/evidence",
            params={"job_id": "job-1", "cursor": "invalid-cursor"},
        ),
        client.get(
            "/research/debates",
            params={"job_id": "job-1", "cursor": "invalid-cursor"},
        ),
    )
    assert all(400 <= response.status_code < 500 for response in query_responses)


def test_validation_error_handlers_expose_only_bounded_safe_metadata(
    caplog,
) -> None:
    marker = "private-validation-marker"
    app = FastAPI()

    @app.post("/request-validation")
    def request_validation(payload: _ValidationRequest) -> dict:
        return payload.model_dump()

    @app.get("/response-validation", response_model=_ValidationResponse)
    def response_validation() -> dict:
        return {"count": marker}

    @app.get("/query-validation")
    def query_validation(q: int) -> dict:
        return {"q": q}

    @app.post("/custom-validation")
    def custom_validation(payload: _SensitiveValidationRequest) -> dict:
        return payload.model_dump()

    add_error_handlers(app)
    caplog.set_level(logging.INFO, logger="api.middlewares.error_handler")
    client = TestClient(app, raise_server_exceptions=False)

    request_response = client.post(
        "/request-validation",
        json={"count": 1, marker: "rejected"},
    )
    query_response = client.get("/query-validation", params={"q": marker})
    custom_response = client.post("/custom-validation", json={"count": 1})
    response_response = client.get("/response-validation")

    assert request_response.status_code == 422
    assert request_response.json() == {
        "error": "validation_error",
        "message": "请求参数验证失败",
        "detail": [
            {
                "type": "extra_forbidden",
                "loc": ["body", "[redacted]"],
                "msg": "Extra inputs are not permitted",
            }
        ],
    }
    assert query_response.status_code == 422
    assert query_response.json()["detail"] == [
        {
            "type": "int_parsing",
            "loc": ["query", "q"],
            "msg": "Input should be a valid integer, unable to parse string as an integer",
        }
    ]
    assert custom_response.status_code == 422
    assert custom_response.json()["detail"] == [
        {
            "type": "value_error",
            "loc": ["body", "count"],
            "msg": "Invalid value",
        }
    ]
    assert response_response.status_code == 500
    assert response_response.json() == {
        "error": "internal_error",
        "message": "Response validation failed",
        "detail": {"error_count": 1, "sources": ["response"]},
    }
    assert marker not in request_response.text
    assert marker not in query_response.text
    assert marker not in custom_response.text
    assert marker not in response_response.text
    assert marker not in caplog.text
