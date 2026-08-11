"""End-to-end SW1 freeze and no-lookahead tests for Outcome v2."""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from pydantic import BaseModel, ConfigDict

from src.services.decision_outcome_v2_service import DecisionOutcomeV2Service
from src.services.decision_outcome_v2_service import Sw1PointInTimeSnapshotCapture
from src.services.durable_job_handlers import (
    PersonalResearchPayload,
    _personal_research_handler,
    bind_durable_execution_context,
)
from src.services.durable_jobs import (
    DurableJobHandlerRegistry,
    DurableJobStore,
    JobEnqueueRequest,
)
from src.services.research import (
    DatasetCollectionResult,
    LeaseFence,
    RawArtifactStore,
    ResearchDatasetCollector,
    ResearchSnapshotRepository,
    SnapshotWriteResult,
    build_sw1_index_classify_query,
    build_sw1_index_member_all_query,
)
from src.services.research.collector import _reset_collector_state_for_tests
from src.services.research.decision_outcome_v2_datasets import (
    SW1_INDEX_CLASSIFY_FIELDS,
    SW1_INDEX_MEMBER_ALL_FIELDS,
)
from src.storage import DatabaseManager


_SHANGHAI = ZoneInfo("Asia/Shanghai")


class _JobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stock_code: str


class _Context:
    cancel_requested = SimpleNamespace(is_set=lambda: False)
    lease_lost = SimpleNamespace(is_set=lambda: False)
    job_type = "personal_research"

    def __init__(self, lease: LeaseFence) -> None:
        self.job_id = lease.job_id
        self.worker_id = lease.worker_id
        self.lease_token = lease.lease_token
        self.checkpoints = 0

    def checkpoint(self) -> None:
        self.checkpoints += 1


class _HandlerContext:
    query_id = "query-sw1"
    trace_id = "trace-sw1"

    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def stage(self, stage: str, **kwargs) -> None:
        self.events.append(("stage", (stage, kwargs)))

    def progress(self, progress: int, message: str, **kwargs) -> None:
        self.events.append(("progress", (progress, message, kwargs)))

    def checkpoint(self) -> None:
        self.events.append(("checkpoint", None))


class _Sw1Provider:
    def __init__(self, *, current_code: str, current_name: str) -> None:
        self.current_code = current_code
        self.current_name = current_name
        self.calls: list[tuple[str, dict[str, str]]] = []

    def query(self, api_name: str, fields: str = "", **params):
        del fields
        params.pop("_cancel_event", None)
        self.calls.append((api_name, dict(params)))
        if api_name == "index_classify":
            return pd.DataFrame(
                [
                    _classify_row("801780.SI", "Bank"),
                    _classify_row(self.current_code, self.current_name),
                ],
                columns=SW1_INDEX_CLASSIFY_FIELDS,
            )
        if api_name == "index_member_all":
            if params["is_new"] == "Y":
                return pd.DataFrame(
                    [
                        _member_row(
                            code=self.current_code,
                            name=self.current_name,
                            in_date="20250101",
                            out_date=None,
                            is_new="Y",
                        )
                    ],
                    columns=SW1_INDEX_MEMBER_ALL_FIELDS,
                )
            return pd.DataFrame(
                [
                    _member_row(
                        code="801780.SI",
                        name="Bank",
                        in_date="20210101",
                        out_date="20250101",
                        is_new="N",
                    )
                ],
                columns=SW1_INDEX_MEMBER_ALL_FIELDS,
            )
        raise AssertionError(f"unexpected provider API: {api_name}")


class _NoLiveProvider:
    calls = 0

    def query(self, *_args, **_kwargs):
        self.calls += 1
        raise AssertionError("historical SW1 replay must not call Tushare")


def _classify_row(code: str, name: str) -> dict[str, str]:
    return {
        "index_code": code,
        "industry_name": name,
        "parent_code": "0",
        "level": "L1",
        "industry_code": code.split(".", 1)[0],
        "is_pub": "1",
        "src": "SW2021",
    }


def _member_row(
    *,
    code: str,
    name: str,
    in_date: str,
    out_date: str | None,
    is_new: str,
) -> dict[str, str | None]:
    return {
        "l1_code": code,
        "l1_name": name,
        "l2_code": "801000.SI",
        "l2_name": "Level 2",
        "l3_code": "851000.SI",
        "l3_name": "Level 3",
        "ts_code": "600519.SH",
        "name": "Kweichow Moutai",
        "in_date": in_date,
        "out_date": out_date,
        "is_new": is_new,
    }


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        durable_jobs_enabled=True,
        personal_research_enabled=True,
        tushare_research_enabled=True,
        research_factors_enabled=True,
        decision_outcome_v2_enabled=True,
    )


def _dataset_result(
    dataset: str,
    *,
    status: str,
    at: datetime,
    rows=(),
    error_code: str | None = None,
    digest: str | None = None,
) -> DatasetCollectionResult:
    return DatasetCollectionResult(
        dataset=dataset,
        status=status,
        row_count=len(rows),
        snapshot=(
            SnapshotWriteResult(1, digest, True) if digest is not None else None
        ),
        query_params={},
        normalized_rows=tuple(rows),
        available_at=at,
        data_as_of=at,
        error_code=error_code,
    )


@pytest.fixture()
def _sw1_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    DatabaseManager.reset_instance()
    _reset_collector_state_for_tests()
    db = DatabaseManager(db_url=f"sqlite:///{(tmp_path / 'sw1.db').as_posix()}")
    registry = DurableJobHandlerRegistry()
    for job_type in ("personal_research", "decision_outcomes_v2"):
        registry.register(job_type, 1, _JobPayload, lambda payload: payload.stock_code)
    store = DurableJobStore(registry, db, lease_seconds=600, heartbeat_seconds=30)
    repository = ResearchSnapshotRepository(db)
    try:
        yield db, store, repository, tmp_path / "raw"
    finally:
        _reset_collector_state_for_tests()
        DatabaseManager.reset_instance()


def _claim(
    store: DurableJobStore,
    *,
    job_type: str,
    task_id: str,
    now: datetime,
) -> tuple[object, LeaseFence]:
    store.enqueue(
        JobEnqueueRequest(
            job_type=job_type,
            payload={"stock_code": "600519"},
            task_id=task_id,
            stock_code="600519",
        ),
        now=now,
    )
    claimed = store.claim_next(f"{job_type}-worker", now=now)
    assert claimed is not None
    assert claimed.task_id == task_id
    return claimed, LeaseFence(
        job_id=claimed.task_id,
        worker_id=claimed.worker_id,
        lease_token=claimed.lease_token,
    )


def _complete(
    store: DurableJobStore,
    claimed: object,
    *,
    now: datetime,
) -> None:
    store.complete(
        claimed.task_id,
        claimed.worker_id,
        claimed.lease_token,
        result={"ok": True},
        now=now,
    )


def test_personal_research_freezes_sw1_and_outcome_replay_has_no_lookahead(
    _sw1_runtime,
) -> None:
    db, store, repository, raw_root = _sw1_runtime
    lease_now = datetime.now(timezone.utc)
    local_day = lease_now.astimezone(_SHANGHAI).date()
    captured_at = datetime.combine(local_day, time(10, 0), _SHANGHAI).astimezone(
        timezone.utc
    )
    decision_known_at = datetime.combine(
        local_day, time(11, 0), _SHANGHAI
    ).astimezone(timezone.utc)
    later_same_day = datetime.combine(
        local_day, time(12, 0), _SHANGHAI
    ).astimezone(timezone.utc)

    first_claim, first_lease = _claim(
        store,
        job_type="personal_research",
        task_id="personal-sw1-first",
        now=lease_now,
    )
    first_provider = _Sw1Provider(
        current_code="801120.SI",
        current_name="Food and Beverage",
    )
    first_collector = ResearchDatasetCollector(
        first_provider,
        repository,
        RawArtifactStore(raw_root),
        clock=lambda: captured_at,
    )
    first_capture = DecisionOutcomeV2Service(
        db_manager=db,
        config=_config(),
        collector=first_collector,
        context_getter=lambda: _Context(first_lease),
        clock=lambda: captured_at,
    ).capture_sw1_membership_snapshots("600519")

    assert first_capture.status == "available"
    assert first_capture.available_at <= decision_known_at
    assert len(first_capture.dataset_hashes) == 2
    assert [name for name, _params in first_provider.calls] == [
        "index_classify",
        "index_member_all",
        "index_member_all",
    ]
    assert [
        params.get("is_new")
        for name, params in first_provider.calls
        if name == "index_member_all"
    ] == ["Y", "N"]
    _complete(store, first_claim, now=lease_now + timedelta(seconds=1))

    later_claim, later_lease = _claim(
        store,
        job_type="personal_research",
        task_id="personal-sw1-later",
        now=lease_now + timedelta(seconds=2),
    )
    later_provider = _Sw1Provider(
        current_code="801200.SI",
        current_name="Retail",
    )
    later_collector = ResearchDatasetCollector(
        later_provider,
        repository,
        RawArtifactStore(raw_root),
        clock=lambda: later_same_day,
    )
    later_capture = DecisionOutcomeV2Service(
        db_manager=db,
        config=_config(),
        collector=later_collector,
        context_getter=lambda: _Context(later_lease),
        clock=lambda: later_same_day,
    ).capture_sw1_membership_snapshots("600519")
    assert later_capture.status == "available"
    assert later_capture.available_at > decision_known_at
    _complete(store, later_claim, now=lease_now + timedelta(seconds=3))

    outcome_claim, outcome_lease = _claim(
        store,
        job_type="decision_outcomes_v2",
        task_id="outcome-sw1-replay",
        now=lease_now + timedelta(seconds=4),
    )
    no_live_provider = _NoLiveProvider()
    historical_collector = ResearchDatasetCollector(
        no_live_provider,
        repository,
        RawArtifactStore(raw_root),
        clock=lambda: lease_now + timedelta(seconds=4),
    )
    classify = historical_collector.collect_dataset(
        "600519",
        "sw1_index_classify",
        as_of=decision_known_at,
        lease=outcome_lease,
        reference_mode="historical",
        query_plan=build_sw1_index_classify_query(),
    )
    member = historical_collector.collect_dataset(
        "600519",
        "sw1_index_member_all",
        as_of=decision_known_at,
        lease=outcome_lease,
        reference_mode="historical",
        query_plan=build_sw1_index_member_all_query("600519"),
    )
    resolution, reason = DecisionOutcomeV2Service(
        db_manager=db,
        config=_config(),
        collector=historical_collector,
        context_getter=lambda: _Context(outcome_lease),
    )._membership_resolution(
        stock_code="600519",
        signal_session=decision_known_at.astimezone(_SHANGHAI).date(),
        decision_known_at=decision_known_at,
        classify=classify,
        member=member,
    )

    assert no_live_provider.calls == 0
    assert classify.available_at == captured_at
    assert member.available_at == captured_at
    assert reason is None
    assert resolution is not None
    assert resolution.status == "available"
    assert resolution.industry_code == "801120.SI"
    assert resolution.industry_name == "Food and Beverage"
    assert "801200.SI" not in {
        str(row.get("index_code")) for row in classify.normalized_rows
    }
    _complete(store, outcome_claim, now=lease_now + timedelta(seconds=5))


@pytest.mark.parametrize(
    ("classify", "member", "expected_reason"),
    [
        (
            ("permission_denied", (), "permission_denied", None),
            ("available", (_member_row(
                code="801120.SI",
                name="Food and Beverage",
                in_date="20250101",
                out_date=None,
                is_new="Y",
            ),), None, "b" * 64),
            "sw1_index_classify_permission_denied",
        ),
        (
            ("available", (_classify_row("801120.SI", "Food and Beverage"),), None, "a" * 64),
            ("empty", (), None, "b" * 64),
            "sw1_index_member_all_empty",
        ),
    ],
)
def test_optional_capture_fails_closed_with_typed_permission_or_empty_reason(
    classify,
    member,
    expected_reason: str,
) -> None:
    at = datetime(2026, 8, 10, 7, 0, tzinfo=timezone.utc)

    class Collector:
        def collect_dataset(self, _stock_code, dataset, **_kwargs):
            values = classify if dataset == "sw1_index_classify" else member
            return _dataset_result(
                dataset,
                status=values[0],
                at=at,
                rows=values[1],
                error_code=values[2],
                digest=values[3],
            )

    capture = DecisionOutcomeV2Service(
        repository=SimpleNamespace(),
        config=_config(),
        collector=Collector(),
        context_getter=lambda: _Context(
            LeaseFence("personal-research", "worker", "lease")
        ),
        clock=lambda: at,
    ).capture_sw1_membership_snapshots("600519")

    assert capture.status == "unavailable"
    assert capture.reason == expected_reason


def test_live_capture_cannot_run_inside_outcome_worker() -> None:
    context = _Context(LeaseFence("outcome", "worker", "lease"))
    context.job_type = "decision_outcomes_v2"

    class Collector:
        def collect_dataset(self, *_args, **_kwargs):
            raise AssertionError("outcome worker must never live-capture SW1")

    with pytest.raises(RuntimeError, match="personal_research Worker"):
        DecisionOutcomeV2Service(
            repository=SimpleNamespace(),
            config=_config(),
            collector=Collector(),
            context_getter=lambda: context,
        ).capture_sw1_membership_snapshots("600519")


@pytest.mark.parametrize(
    "reason",
    [
        "sw1_index_classify_permission_denied",
        "sw1_index_member_all_empty",
    ],
)
def test_personal_research_handler_continues_when_optional_sw1_is_unavailable(
    reason: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    at = datetime(2026, 8, 10, 7, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "src.config.get_config",
        lambda: _config(),
    )

    class CaptureService:
        def __init__(self, **_kwargs) -> None:
            pass

        def capture_sw1_membership_snapshots(self, stock_code: str):
            assert stock_code == "600519"
            order.append("capture")
            return Sw1PointInTimeSnapshotCapture(
                status="unavailable",
                reason=reason,
                available_at=at,
                classify_status="permission_denied",
                member_status="empty",
                dataset_hashes=(),
            )

    class AnalysisService:
        last_error = None

        def analyze_stock(self, **kwargs):
            order.append("analysis")
            assert kwargs["stock_code"] == "600519"
            return {"success": True, "stock_code": "600519"}

    monkeypatch.setattr(
        "src.services.decision_outcome_v2_service.DecisionOutcomeV2Service",
        CaptureService,
    )
    monkeypatch.setattr(
        "src.services.analysis_service.AnalysisService",
        AnalysisService,
    )
    context = _HandlerContext()
    with bind_durable_execution_context(context):
        result = _personal_research_handler(
            PersonalResearchPayload(stock_code="600519", notify=False)
        )

    assert order == ["capture", "analysis"]
    assert result["success"] is True
    assert any(
        reason in event[1][1]
        for event in context.events
        if event[0] == "progress"
    )
