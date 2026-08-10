"""API contracts for durable cancellation and resumable task SSE."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient as FastAPITestClient

from api.deps import get_config_dep
from api.middlewares.error_handler import add_error_handlers
from api.v1.endpoints.analysis import (
    router as analysis_router,
    cancel_analysis_task,
    task_stream,
    trigger_analysis,
    trigger_market_review,
)
from api.v1.endpoints.screening import ScreeningScreenRequest, screening_start_screen_task
from api.v1.schemas.analysis import AnalyzeRequest, MarketReviewRequest
from src.services.durable_jobs import DurableJobConflictError
from src.services.task_queue import DuplicateTaskError, TaskStatus


class _Task:
    def __init__(self, task_id: str, status: str = "processing") -> None:
        self.task_id = task_id
        self.status = TaskStatus(status)

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "trace_id": self.task_id,
            "stock_code": "600519",
            "status": self.status.value,
            "progress": 40,
            "report_type": "detailed",
            "analysis_phase": "auto",
            "created_at": "2026-08-08T00:00:00",
        }


class _DurableQueue:
    durable_enabled = True

    def __init__(
        self,
        *,
        min_id=1,
        high_water=4,
        events=None,
        event_batches=None,
    ) -> None:
        self._min_id = min_id
        self._high_water = high_water
        self._event_batches = (
            [list(batch) for batch in event_batches]
            if event_batches is not None
            else [list(events or [])]
        )
        self.read_after_ids = []

    def durable_event_min_id(self):
        return self._min_id

    def durable_event_high_water(self):
        return self._high_water

    def list_pending_tasks(self):
        return [_Task("active-job")]

    def read_durable_events(self, *, after_id, limit):
        self.read_after_ids.append((after_id, limit))
        if not self._event_batches:
            return []
        return self._event_batches.pop(0)

    def get_task(self, task_id):
        return _Task(task_id)


def _analysis_client():
    app = FastAPI()
    add_error_handlers(app)
    app.include_router(analysis_router, prefix="/api/v1/analysis")
    app.dependency_overrides[get_config_dep] = lambda: SimpleNamespace()
    return FastAPITestClient(app)


def test_durable_sse_replays_strictly_after_cursor_with_event_id() -> None:
    event = SimpleNamespace(
        id=5,
        job_id="job-1",
        event_type="progress",
        stage="analyzing",
        payload={"progress": 40},
    )
    queue = _DurableQueue(events=[event])

    async def consume():
        with patch("api.v1.endpoints.analysis.get_task_queue", return_value=queue):
            response = await task_stream(last_event_id=4, last_event_id_header=None)
            iterator = response.body_iterator
            connected = await anext(iterator)
            progress = await anext(iterator)
            await iterator.aclose()
            return connected, progress

    connected, progress = asyncio.run(consume())
    assert "reset_required\": false" in connected.lower()
    assert progress.startswith("id: 5\nevent: task_progress\n")
    assert '"stage": "analyzing"' in progress
    assert queue.read_after_ids[0] == (4, 200)


def test_durable_sse_retention_gap_resets_without_advancing_heartbeat_cursor() -> None:
    queue = _DurableQueue(min_id=10, high_water=20)

    async def consume():
        with patch("api.v1.endpoints.analysis.get_task_queue", return_value=queue):
            response = await task_stream(last_event_id=2, last_event_id_header=None)
            iterator = response.body_iterator
            values = [await anext(iterator) for _ in range(3)]
            await iterator.aclose()
            return values

    connected, reset, snapshot = asyncio.run(consume())
    assert "reset_required\": true" in connected.lower()
    assert "event: stream_reset" in reset
    # Empty id clears EventSource's native Last-Event-ID without advancing to
    # an unseen durable row; following snapshots inherit the empty cursor.
    assert reset.startswith("id: \nevent: stream_reset\n")
    assert "event: task_created" in snapshot


def test_durable_sse_cursor_ahead_of_restored_database_resets() -> None:
    queue = _DurableQueue(min_id=1, high_water=20)

    async def consume():
        with patch("api.v1.endpoints.analysis.get_task_queue", return_value=queue):
            response = await task_stream(last_event_id=200, last_event_id_header=None)
            iterator = response.body_iterator
            values = [await anext(iterator) for _ in range(3)]
            await iterator.aclose()
            return values

    connected, reset, snapshot = asyncio.run(consume())
    assert '"cursor": 20' in connected
    assert '"reset_required": true' in connected.lower()
    assert "event: stream_reset" in reset
    assert "event: task_created" in snapshot


def test_durable_sse_middle_retention_gap_resets_even_when_min_id_is_old() -> None:
    retained = SimpleNamespace(
        id=5,
        job_id="job-1",
        event_type="progress",
        stage="analyzing",
        payload={"progress": 40},
    )
    # Event 1 from another job keeps min(id) below the cursor while events
    # 3-4 have been pruned from the middle of the global stream.
    queue = _DurableQueue(min_id=1, high_water=5, events=[retained])

    async def consume():
        with patch("api.v1.endpoints.analysis.get_task_queue", return_value=queue):
            response = await task_stream(last_event_id=2, last_event_id_header=None)
            iterator = response.body_iterator
            values = [await anext(iterator) for _ in range(3)]
            await iterator.aclose()
            return values

    connected, reset, snapshot = asyncio.run(consume())
    assert '"reset_required": true' in connected.lower()
    assert '"cursor": 5' in connected
    assert reset.startswith("id: \nevent: stream_reset\n")
    assert "event: stream_reset" in reset
    assert "event: task_created" in snapshot
    assert queue.read_after_ids[0] == (2, 200)


def test_durable_sse_gap_created_while_connected_emits_reset_and_snapshot() -> None:
    event_5 = SimpleNamespace(
        id=5,
        job_id="job-1",
        event_type="progress",
        stage="analyzing",
        payload={"progress": 40},
    )
    event_8 = SimpleNamespace(
        id=8,
        job_id="job-1",
        event_type="progress",
        stage="analyzing",
        payload={"progress": 70},
    )
    queue = _DurableQueue(
        min_id=1,
        high_water=8,
        event_batches=[[event_5], [event_8]],
    )

    async def consume():
        with patch("api.v1.endpoints.analysis.get_task_queue", return_value=queue):
            response = await task_stream(last_event_id=4, last_event_id_header=None)
            iterator = response.body_iterator
            values = [await anext(iterator) for _ in range(4)]
            await iterator.aclose()
            return values

    connected, progress, reset, snapshot = asyncio.run(consume())
    assert '"reset_required": false' in connected.lower()
    assert progress.startswith("id: 5\nevent: task_progress\n")
    assert reset.startswith("id: \nevent: stream_reset\n")
    assert "event: stream_reset" in reset
    assert '"cursor": 8' in reset
    assert "event: task_created" in snapshot


def test_invalid_last_event_id_header_fails_before_streaming() -> None:
    with patch("api.v1.endpoints.analysis.get_task_queue", return_value=_DurableQueue()):
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(task_stream(last_event_id=None, last_event_id_header="not-a-number"))
    assert exc_info.value.status_code == 400


def test_cancel_endpoint_maps_pending_and_missing_contracts() -> None:
    queue = SimpleNamespace(
        cancel_task=lambda _task_id: SimpleNamespace(status=TaskStatus.CANCEL_REQUESTED)
    )
    with patch("api.v1.endpoints.analysis.get_task_queue", return_value=queue):
        response = cancel_analysis_task("job-1")
    assert response.task_id == "job-1"
    assert response.status.value == "cancel_requested"

    def missing(_task_id):
        return None

    with patch(
        "api.v1.endpoints.analysis.get_task_queue",
        return_value=SimpleNamespace(cancel_task=missing),
    ):
        with pytest.raises(HTTPException) as exc_info:
            cancel_analysis_task("missing")
    assert exc_info.value.status_code == 404


def test_legacy_processing_cancel_returns_http_409_instead_of_false_promise() -> None:
    def unavailable(_task_id):
        raise RuntimeError("Legacy 任务已开始执行，当前后端不支持中途取消")

    with _analysis_client() as client, patch(
        "api.v1.endpoints.analysis.get_task_queue",
        return_value=SimpleNamespace(cancel_task=unavailable),
    ):
        response = client.post("/api/v1/analysis/tasks/legacy-running/cancel")

    assert response.status_code == 409
    assert response.json() == {
        "error": "task_cancel_unavailable",
        "message": "Legacy 任务已开始执行，当前后端不支持中途取消",
    }


def test_sync_request_enqueues_and_waits_without_running_analysis_in_api() -> None:
    completed = SimpleNamespace(
        task_id="job-sync",
        trace_id="job-sync",
        stock_code="600519",
        stock_name="贵州茅台",
        status=TaskStatus.COMPLETED,
        result={
            "query_id": "job-sync",
            "trace_id": "job-sync",
            "stock_code": "600519",
            "stock_name": "贵州茅台",
            "report": None,
            "created_at": "2026-08-08T00:00:00",
        },
        error=None,
        created_at="2026-08-08T00:00:00",
        completed_at="2026-08-08T00:00:01",
    )
    queue = SimpleNamespace(
        durable_enabled=True,
        submit_tasks_batch=lambda *args, **kwargs: ([completed], []),
        get_task=lambda _task_id: completed,
    )

    with patch("api.v1.endpoints.analysis.get_task_queue", return_value=queue), \
            patch("src.services.analysis_service.AnalysisService") as analysis_service:
        response = trigger_analysis(AnalyzeRequest(stock_code="600519"), config=SimpleNamespace())

    assert response.query_id == "job-sync"
    assert response.stock_code == "600519"
    analysis_service.assert_not_called()


def test_durable_sync_http_errors_match_declared_409_and_504_contracts() -> None:
    duplicate = DuplicateTaskError("600519", "job-existing")
    duplicate_queue = SimpleNamespace(
        durable_enabled=True,
        submit_tasks_batch=lambda *args, **kwargs: ([], [duplicate]),
    )
    with _analysis_client() as client, patch(
        "api.v1.endpoints.analysis.get_task_queue",
        return_value=duplicate_queue,
    ):
        duplicate_response = client.post(
            "/api/v1/analysis/analyze",
            json={"stock_code": "600519", "async_mode": False},
        )

    assert duplicate_response.status_code == 409
    assert duplicate_response.json() == {
        "error": "duplicate_task",
        "message": "股票 600519 正在分析中 (task_id: job-existing)",
        "stock_code": "600519",
        "existing_task_id": "job-existing",
    }

    pending = SimpleNamespace(task_id="job-timeout", status=TaskStatus.PENDING)
    timeout_queue = SimpleNamespace(
        durable_enabled=True,
        submit_tasks_batch=lambda *args, **kwargs: ([pending], []),
        get_task=lambda _task_id: pending,
    )
    fake_time = SimpleNamespace(
        monotonic=iter([0.0, 901.0]).__next__,
        sleep=lambda _seconds: None,
    )
    with _analysis_client() as client, patch(
        "api.v1.endpoints.analysis.get_task_queue",
        return_value=timeout_queue,
    ), patch("api.v1.endpoints.analysis.time", fake_time):
        timeout_response = client.post(
            "/api/v1/analysis/analyze",
            json={"stock_code": "600519", "async_mode": False},
        )

    assert timeout_response.status_code == 504
    assert timeout_response.json()["error"] == "durable_wait_timeout"

    with _analysis_client() as client:
        responses = client.app.openapi()["paths"]["/api/v1/analysis/analyze"]["post"][
            "responses"
        ]
    refs = {
        item["$ref"]
        for item in responses["409"]["content"]["application/json"]["schema"]["anyOf"]
    }
    assert refs == {
        "#/components/schemas/DuplicateTaskErrorResponse",
        "#/components/schemas/ErrorResponse",
    }
    assert responses["504"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/ErrorResponse"
    }


def test_market_review_durable_path_does_not_capture_api_lock_or_callable() -> None:
    submitted = {}

    def submit_typed_job(job_type, payload, **kwargs):
        submitted.update(job_type=job_type, payload=payload, kwargs=kwargs)
        return SimpleNamespace(task_id=kwargs["task_id"], trace_id=kwargs["trace_id"])

    queue = SimpleNamespace(durable_enabled=True, submit_typed_job=submit_typed_job)
    config = SimpleNamespace(market_review_region="cn,us")
    with patch("api.v1.endpoints.analysis.get_task_queue", return_value=queue), \
            patch("api.v1.endpoints.analysis._try_acquire_market_review_lock") as acquire_lock:
        response = trigger_market_review(
            MarketReviewRequest(send_notification=False),
            config=config,
        )

    assert response.status == "accepted"
    assert submitted["job_type"] == "market_review"
    assert submitted["payload"]["region"] == "cn,us"
    assert submitted["payload"]["send_notification"] is False
    assert submitted["kwargs"]["dedupe_key"] == "market_review:cn,us"
    acquire_lock.assert_not_called()


def test_market_review_durable_payload_conflict_is_still_http_409() -> None:
    def conflict(*_args, **_kwargs):
        raise DurableJobConflictError("active request has different notify payload")

    queue = SimpleNamespace(durable_enabled=True, submit_typed_job=conflict)
    with patch("api.v1.endpoints.analysis.get_task_queue", return_value=queue):
        with pytest.raises(HTTPException) as exc_info:
            trigger_market_review(
                MarketReviewRequest(send_notification=False),
                config=SimpleNamespace(market_review_region="cn"),
            )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["error"] == "duplicate_market_review"


def test_screening_durable_path_enqueues_typed_job_without_api_execution() -> None:
    submitted = {}

    def submit_typed_job(job_type, payload, **kwargs):
        submitted.update(job_type=job_type, payload=payload, kwargs=kwargs)
        return SimpleNamespace(
            task_id=kwargs["task_id"],
            trace_id=kwargs["trace_id"],
            status=TaskStatus.PENDING,
            message="queued",
        )

    queue = SimpleNamespace(durable_enabled=True, submit_typed_job=submit_typed_job)
    request = ScreeningScreenRequest(
        strategy="dual_low",
        market="cn",
        max_results=8,
        variant_seed="fixture-seed",
    )
    with patch("api.v1.endpoints.screening.get_task_queue", return_value=queue), \
            patch("api.v1.endpoints.screening._service") as service:
        response = screening_start_screen_task(
            request,
            http_request=SimpleNamespace(headers={}),
            config=SimpleNamespace(),
            db_manager=SimpleNamespace(),
        )

    assert response.status == "pending"
    assert submitted["job_type"] == "screening_screen"
    assert submitted["payload"] == {
        "strategy": "dual_low",
        "market": "cn",
        "max_results": 8,
        "selection_seed": "fixture-seed",
    }
    assert submitted["kwargs"]["idempotency_key"].startswith("screening_screen:")
    assert submitted["kwargs"]["notify"] is False
    service.assert_not_called()


def test_static_api_spec_tracks_cancel_stage_and_resumable_sse_contract() -> None:
    spec = json.loads(
        (Path(__file__).resolve().parents[1] / "docs" / "architecture" / "api_spec.json")
        .read_text(encoding="utf-8")
    )
    cancel = spec["paths"]["/api/v1/analysis/tasks/{task_id}/cancel"]["post"]
    assert cancel["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/TaskCancelResponse"
    }
    stream_params = {
        (item["in"], item["name"])
        for item in spec["paths"]["/api/v1/analysis/tasks/stream"]["get"]["parameters"]
    }
    assert stream_params == {("query", "last_event_id"), ("header", "Last-Event-ID")}
    stream = spec["paths"]["/api/v1/analysis/tasks/stream"]["get"]
    assert "400" in stream["responses"]
    assert "stream_reset" in stream["description"]
    analyze_responses = spec["paths"]["/api/v1/analysis/analyze"]["post"]["responses"]
    assert {
        item["$ref"]
        for item in analyze_responses["409"]["content"]["application/json"]["schema"][
            "anyOf"
        ]
    } == {
        "#/components/schemas/DuplicateTaskErrorResponse",
        "#/components/schemas/ErrorResponse",
    }
    assert analyze_responses["504"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/ErrorResponse"
    }
    assert "stage" in spec["components"]["schemas"]["TaskInfo"]["properties"]
    assert "stage" in spec["components"]["schemas"]["TaskStatus"]["properties"]
