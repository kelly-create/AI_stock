# -*- coding: utf-8 -*-
"""Tests for liveness and readiness health endpoints."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.app import create_app
from src.services.readiness_service import ReadinessCheckResult, ReadinessReport


def _make_client():
    temp_dir = tempfile.TemporaryDirectory()
    return temp_dir, TestClient(create_app(static_dir=Path(temp_dir.name)))


class _StaticReadinessService:
    def __init__(self, *, ready: bool):
        self.ready = ready

    def check(self) -> ReadinessReport:
        return ReadinessReport(
            checks={
                "database_read": ReadinessCheckResult(
                    "ready" if self.ready else "not_ready",
                    "database_readable" if self.ready else "database_read_failed",
                )
            }
        )


class HealthEndpointTestCase(unittest.TestCase):
    """Health endpoints should return 200 with valid payload."""

    @classmethod
    def setUpClass(cls):
        cls._temp_dir, cls.client = _make_client()

    @classmethod
    def tearDownClass(cls):
        cls._temp_dir.cleanup()

    def test_api_health_returns_200(self):
        resp = self.client.get("/api/health")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["status"], "ok")
        self.assertIn("timestamp", body)

    def test_root_health_returns_200(self):
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["status"], "ok")
        self.assertIn("timestamp", body)

    def test_api_v1_health_returns_200(self):
        resp = self.client.get("/api/v1/health")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["status"], "ok")
        self.assertIn("timestamp", body)

    def test_api_v1_readiness_returns_200_or_503_from_real_checks(self):
        self.client.app.state.readiness_service = _StaticReadinessService(ready=True)
        try:
            ready_resp = self.client.get("/api/v1/health/ready")
            self.client.app.state.readiness_service = _StaticReadinessService(ready=False)
            unavailable_resp = self.client.get("/api/v1/health/ready")
        finally:
            delattr(self.client.app.state, "readiness_service")

        self.assertEqual(ready_resp.status_code, 200)
        self.assertEqual(ready_resp.json()["status"], "ready")
        self.assertEqual(unavailable_resp.status_code, 503)
        self.assertEqual(unavailable_resp.json()["status"], "not_ready")

    def test_api_v1_readiness_openapi_contract(self):
        runtime_spec = self.client.app.openapi()
        operation = runtime_spec["paths"]["/api/v1/health/ready"]["get"]

        self.assertEqual(operation["operationId"], "readinessCheck")
        self.assertEqual(operation["responses"]["200"]["description"], "服务可接收流量")
        self.assertIn("503", operation["responses"])

        static_spec_path = (
            Path(__file__).resolve().parents[1]
            / "docs"
            / "architecture"
            / "api_spec.json"
        )
        static_spec = json.loads(static_spec_path.read_text(encoding="utf-8"))
        self.assertEqual(
            static_spec["paths"]["/api/v1/health/ready"],
            runtime_spec["paths"]["/api/v1/health/ready"],
        )
        for schema_name in ("ReadinessCheckResponse", "ReadinessResponse"):
            self.assertEqual(
                static_spec["components"]["schemas"][schema_name],
                runtime_spec["components"]["schemas"][schema_name],
            )

    def test_root_health_is_not_handled_by_spa_fallback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            static_dir = Path(temp_dir)
            (static_dir / "assets").mkdir()
            (static_dir / "index.html").write_text("<!doctype html><div id=\"root\"></div>", encoding="utf-8")

            client = TestClient(create_app(static_dir=static_dir))
            resp = client.get("/health")

        self.assertEqual(resp.status_code, 200)
        self.assertIn("application/json", resp.headers["content-type"])
        self.assertEqual(resp.json()["status"], "ok")


class HealthEndpointAuthEnabledTestCase(unittest.TestCase):
    """Health endpoints must remain accessible when admin auth is enabled."""

    @classmethod
    def setUpClass(cls):
        cls._patcher = patch("api.middlewares.auth.is_auth_enabled", return_value=True)
        cls._patcher.start()
        cls._temp_dir, cls.client = _make_client()

    @classmethod
    def tearDownClass(cls):
        cls._temp_dir.cleanup()
        cls._patcher.stop()

    def test_api_health_returns_200_when_auth_enabled(self):
        resp = self.client.get("/api/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ok")

    def test_root_health_returns_200_when_auth_enabled(self):
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ok")

    def test_api_v1_health_returns_200_when_auth_enabled(self):
        resp = self.client.get("/api/v1/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ok")

    def test_api_v1_readiness_is_accessible_when_auth_enabled(self):
        self.client.app.state.readiness_service = _StaticReadinessService(ready=True)
        try:
            resp = self.client.get("/api/v1/health/ready")
        finally:
            delattr(self.client.app.state, "readiness_service")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ready")


if __name__ == "__main__":
    unittest.main()
