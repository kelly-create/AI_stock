"""PR2 read-only research API contract tests."""

from __future__ import annotations

from datetime import datetime, timezone
import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.app import create_app
from api.middlewares import auth as auth_middleware
from api.v1.endpoints import research
from src.auth import COOKIE_NAME
from src.config import Config
from src.services.research.repositories import ResearchSnapshotRepository
from src.storage import DatabaseManager, ResearchDatasetSnapshotRecord


HASH = "a" * 64
EVIDENCE_HASH = "b" * 64
SNAPSHOT_HASH = "c" * 64
VALUE_HASH = "d" * 64
NOW = "2026-08-08T08:00:00Z"


def _evidence_item() -> dict:
    return {
        "id": 4,
        "stock_code": "600519",
        "market": "cn",
        "evidence_engine_version": "evidence-v1",
        "claim_policy_version": "claim-policy-v1",
        "as_of": NOW,
        "available_at": NOW,
        "status": "partial",
        "coverage": 0.5,
        "claim_count": 1,
        "citation_count": 1,
        "evidence": {
            "evidence_engine_version": "evidence-v1",
            "claim_policy_version": "claim-policy-v1",
            "stock_code": "600519",
            "market": "cn",
            "as_of": NOW,
            "available_at": NOW,
            "status": "partial",
            "coverage": 0.5,
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


class _Repo:
    last_evidence_query = None

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
            "factors": {"value": {"score": 72.0}},
            "input_dataset_hashes": [HASH],
            "status": "partial",
            "coverage": 0.8,
            "unknowns": ["roe_5y"],
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
            "snapshot": {"subject": {"code": "600519"}},
            "snapshot_hash": HASH,
            "factor_snapshot_hash": HASH,
            "evidence_snapshot_hash": EVIDENCE_HASH,
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
        return {"items": [_evidence_item()], "next_cursor": "opaque-page-2"}


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
            "cursor": "opaque-page-1",
            "limit": 20,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["next_cursor"] == "opaque-page-2"
    assert payload["items"][0]["evidence_hash"] == EVIDENCE_HASH
    assert "evidence" not in payload["items"][0]
    assert _Repo.last_evidence_query == {
        "job_id": "job-1",
        "research_snapshot_hash": SNAPSHOT_HASH,
        "stock_code": "600519",
        "as_of": datetime(2026, 8, 8, 8, 0, tzinfo=timezone.utc),
        "cursor": "opaque-page-1",
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


def test_evidence_create_app_auth_and_error_responses_do_not_expose_payload(
    monkeypatch,
    tmp_path,
    caplog,
) -> None:
    class _ExplodingRepo(_Repo):
        def list_evidence(self, **kwargs):
            if kwargs.get("cursor") == "invalid-cursor":
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
    bad_request = client.get(
        "/api/v1/research/evidence",
        params={"job_id": "job-1", "cursor": "invalid-cursor"},
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
    assert bad_request.status_code == 400
    assert not_found.status_code == 404
    assert invalid.status_code == 422
    assert invalid_query.status_code == 422
    assert invalid_limit.status_code == 422
    assert internal_error.status_code == 500
    for response in (
        missing_selector,
        bad_request,
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
    assert "guaranteed buy" not in caplog.text
    assert "token=secret" not in caplog.text
