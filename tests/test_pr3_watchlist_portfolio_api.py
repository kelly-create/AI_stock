"""HTTP contract tests for original-plan PR3 watchlist and reconciliation."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

import src.auth as auth
from api.app import create_app
from api.deps import get_system_config_service
from src.config import Config
from src.services.research_watchlist_service import ResearchWatchlistService
from src.services.system_config_service import ConfigConflictError
from src.storage import DatabaseManager


def _reset_auth_globals() -> None:
    auth._auth_enabled = None
    auth._session_secret = None
    auth._password_hash_salt = None
    auth._password_hash_stored = None
    auth._rate_limit = {}


class Pr3WatchlistPortfolioApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        _reset_auth_globals()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)
        self.env_path = self.data_dir / ".env"
        self.db_path = self.data_dir / "pr3_api.db"
        self.env_path.write_text(
            "\n".join(
                [
                    "STOCK_LIST=600519",
                    "GEMINI_API_KEY=test",
                    "ADMIN_AUTH_ENABLED=false",
                    f"DATABASE_PATH={self.db_path}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        os.environ["ENV_FILE"] = str(self.env_path)
        os.environ["DATABASE_PATH"] = str(self.db_path)
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.app = create_app(static_dir=self.data_dir / "static")
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        try:
            self.app.dependency_overrides.clear()
            DatabaseManager.reset_instance()
            Config.reset_instance()
        finally:
            os.environ.pop("ENV_FILE", None)
            os.environ.pop("DATABASE_PATH", None)
            self.temp_dir.cleanup()

    def test_enhanced_watchlist_put_union_and_tombstone(self) -> None:
        response = self.client.put(
            "/api/v1/research/watchlist/us/AAPL",
            json={
                "reason": "durable growth thesis",
                "priority": 80,
                "analysis_tier": "deep",
                "next_review_at": "2026-08-12T09:00:00",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["sources"], ["enhanced", "legacy"])

        universe = self.client.get("/api/v1/research/universe")
        self.assertEqual(universe.status_code, 200, universe.text)
        item = next(row for row in universe.json()["items"] if row["stock_code"] == "AAPL")
        self.assertEqual(item["analysis_tier"], "deep")
        self.assertEqual(item["sources"], ["enhanced", "legacy"])

        deleted = self.client.delete("/api/v1/research/watchlist/us/AAPL")
        self.assertEqual(deleted.status_code, 200, deleted.text)
        universe_after = self.client.get("/api/v1/research/universe")
        self.assertNotIn("AAPL", [row["stock_code"] for row in universe_after.json()["items"]])

    def test_watchlist_write_uses_original_config_version_and_is_retryable(self) -> None:
        class ConflictingConfigService:
            def __init__(self):
                self.submitted_version = None

            def get_config(self, *, include_schema=False):
                return {
                    "config_version": "read-version",
                    "items": [{"key": "STOCK_LIST", "value": "600519"}],
                }

            def update(self, *, config_version, **_kwargs):
                self.submitted_version = config_version
                raise ConfigConflictError("new-version")

        fake = ConflictingConfigService()
        self.app.dependency_overrides[get_system_config_service] = lambda: fake
        try:
            response = self.client.put(
                "/api/v1/research/watchlist/us/AAPL",
                json={
                    "reason": "explicit enhanced source",
                    "priority": 70,
                    "analysis_tier": "standard",
                    "next_review_at": None,
                },
            )
        finally:
            self.app.dependency_overrides.clear()

        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(fake.submitted_version, "read-version")
        # The enhanced source is committed first. A retry can safely finish the
        # independent legacy write without losing explicit metadata.
        items = ResearchWatchlistService().list_watchlist()["items"]
        self.assertEqual([item["stock_code"] for item in items], ["AAPL"])

    def test_opening_preview_apply_and_detail(self) -> None:
        account = self.client.post(
            "/api/v1/portfolio/accounts",
            json={
                "name": "opening-api",
                "broker": "unit-test",
                "market": "cn",
                "base_currency": "CNY",
            },
        )
        self.assertEqual(account.status_code, 200, account.text)
        account_id = account.json()["id"]
        preview = self.client.post(
            f"/api/v1/portfolio/accounts/{account_id}/reconciliations/preview",
            json={
                "event_type": "opening",
                "effective_date": "2026-08-01",
                "cash": [{"currency": "CNY", "balance": 1000}],
                "positions": [
                    {
                        "stock_code": "600519",
                        "market": "cn",
                        "currency": "CNY",
                        "quantity": 10,
                        "total_cost": 900,
                    }
                ],
                "source": "broker_statement",
                "note": "opening",
            },
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        applied = self.client.post(
            f"/api/v1/portfolio/accounts/{account_id}/reconciliations/apply",
            json={
                "preview_token": preview.json()["preview_token"],
                "idempotency_key": "api-opening-1",
            },
        )
        self.assertEqual(applied.status_code, 200, applied.text)
        self.assertEqual(applied.json()["status"], "applied")

        detail = self.client.get(
            f"/api/v1/portfolio/accounts/{account_id}/reconciliations/{applied.json()['id']}"
        )
        self.assertEqual(detail.status_code, 200, detail.text)
        self.assertEqual(len(detail.json()["adjustments"]), 2)
        self.assertEqual(detail.json()["source"], "broker_statement")
        self.assertEqual(detail.json()["target"]["positions"][0]["stock_code"], "600519")
        self.assertEqual(detail.json()["warnings"], ["fifo_lineage_reset:cn:600519:CNY"])
        self.assertNotIn("preview_token", detail.json())


if __name__ == "__main__":
    unittest.main()
