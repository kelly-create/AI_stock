"""PR2 read-only research API contract tests."""

from __future__ import annotations

from datetime import datetime, timezone
import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1.endpoints import research
from src.config import Config
from src.services.research.repositories import ResearchSnapshotRepository
from src.storage import DatabaseManager, ResearchDatasetSnapshotRecord


HASH = "a" * 64
NOW = "2026-08-08T08:00:00Z"


class _Repo:
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
