"""Deterministic, offline tests for the PR2 Tushare dataset collector."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import threading
import time

import pandas as pd
import pytest
from pydantic import BaseModel, ConfigDict

from data_provider.tushare_provider import (
    TushareConnectionError,
    TusharePermissionError,
    TushareRateLimitError,
    TushareRequestCancelled,
    TushareTimeoutError,
    TushareTransportError,
)
import src.services.research.collector as collector_module
from src.services.research.availability import (
    DEFAULT_RESEARCH_DATASETS,
    assess_dataset_rows,
    to_tushare_ts_code,
    DATASET_DEFINITIONS,
)
from src.services.research.collector import (
    DatasetCollectionResult,
    ResearchDatasetCollector,
    _CYQ_CACHE_MAX_KEYS,
    _CYQ_HIGH_WATERMARKS,
    _CYQ_KEY_LRU,
    _CYQ_LATEST_RESULTS,
    _CYQ_RESULT_CHUNKS,
    _CYQ_RESULT_CHUNKS_MAX_PER_KEY,
    _JOB_DATASET_RESULTS,
    _JOB_DATASET_RESULT_CACHE_MAX_ENTRIES,
    _reset_collector_state_for_tests,
)
from src.services.research.raw_store import RawArtifactStore
from src.services.research.repositories import (
    DatasetSnapshotInput,
    LeaseFence,
    SnapshotWriteResult,
)
from src.config import Config
from src.services.durable_jobs import (
    DurableJobHandlerRegistry,
    DurableJobStore,
    JobEnqueueRequest,
    StaleLeaseError,
)
from src.services.research.raw_retention import build_raw_retention_plan
from src.services.research.repositories import ResearchSnapshotRepository
from src.storage import (
    DatabaseManager,
    JobEventRecord,
    ResearchDatasetSnapshotRecord,
)


AS_OF = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
LEASE = LeaseFence(job_id="research-job", worker_id="worker-1", lease_token="token-1")


class CollectorJobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stock_code: str


class FakeProvider:
    def __init__(self, responses=None) -> None:
        self.responses = dict(responses or {})
        self.calls: list[tuple[str, dict]] = []
        self._lock = threading.Lock()

    def query(self, api_name: str, fields: str = "", **params):
        with self._lock:
            self.calls.append((api_name, dict(params)))
        response = self.responses.get(api_name, pd.DataFrame())
        if callable(response):
            response = response()
        if isinstance(response, BaseException):
            raise response
        return response.copy(deep=True)


class FakeRepository:
    def __init__(self) -> None:
        self.inputs = []
        self.leases = []
        self._lock = threading.Lock()
        self.references = {}
        self.records = {}
        self.bindings = {}

    @staticmethod
    def _as_utc(value):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def assert_live_lease(self, lease, *, now=None):
        del lease, now

    def get_research_reference_time(self, *, scope_value, lease):
        return self.references.get((lease.job_id, scope_value))

    def establish_research_reference_time(
        self,
        *,
        scope_value,
        reference_time,
        lease,
    ):
        key = (lease.job_id, scope_value)
        self.references.setdefault(key, reference_time)
        return self.references[key]

    def write_dataset(
        self,
        snapshot,
        *,
        lease,
        establish_reference_for=None,
    ):
        with self._lock:
            if establish_reference_for is not None:
                key = (lease.job_id, establish_reference_for)
                self.references.setdefault(key, snapshot.knowledge_as_of)
            self.inputs.append(snapshot)
            self.leases.append(lease)
            observed_at = snapshot.observed_at or snapshot.available_at
            candidate = {
                "id": None,
                "dataset": snapshot.dataset,
                "scope_type": snapshot.scope_type,
                "scope_value": snapshot.scope_value,
                "market": snapshot.market,
                "provider": snapshot.provider,
                "schema_version": snapshot.schema_version,
                "trade_date": snapshot.trade_date,
                "report_date": snapshot.report_date,
                "announcement_date": snapshot.announcement_date,
                "data_as_of": self._as_utc(snapshot.data_as_of),
                "available_at": self._as_utc(snapshot.available_at),
                "observed_at": self._as_utc(observed_at),
                "status": snapshot.status,
                "normalized": snapshot.normalized,
                "raw_ref": snapshot.raw_ref,
                "error_code": snapshot.error_code,
                "error_message": snapshot.error_message_sanitized,
                "retryable": snapshot.retryable,
                "supersedes_hash": snapshot.supersedes_hash,
            }
            record = next(
                (
                    item
                    for item in self.records.values()
                    if all(
                        item.get(field) == candidate.get(field)
                        for field in (
                            "dataset",
                            "scope_type",
                            "scope_value",
                            "market",
                            "provider",
                            "schema_version",
                            "trade_date",
                            "report_date",
                            "announcement_date",
                            "data_as_of",
                            "available_at",
                            "status",
                            "normalized",
                            "raw_ref",
                            "error_code",
                            "retryable",
                            "supersedes_hash",
                        )
                    )
                    and (
                        snapshot.dataset != "stock_basic"
                        or item.get("observed_at") == candidate.get("observed_at")
                    )
                ),
                None,
            )
            created = record is None
            if record is None:
                identifier = len(self.records) + 1
                content_hash = f"{identifier:064x}"
                candidate["id"] = identifier
                candidate["content_hash"] = content_hash
                record = candidate
                self.records[content_hash] = record
            else:
                identifier = int(record["id"])
                content_hash = next(
                    key for key, value in self.records.items() if value is record
                )
            boundary = self._as_utc(snapshot.knowledge_as_of)
            self.bindings[
                (
                    lease.job_id,
                    snapshot.dataset,
                    snapshot.scope_value,
                    boundary,
                )
            ] = record
        return SnapshotWriteResult(identifier, content_hash, created)

    def get_job_dataset(
        self,
        *,
        job_id,
        dataset,
        scope_value,
        as_of=None,
        terminal_only=False,
    ):
        boundary = self._as_utc(as_of) if as_of is not None else None
        if boundary is None:
            matches = [
                record
                for key, record in self.bindings.items()
                if key[:3] == (job_id, dataset, scope_value)
            ]
            if len(matches) > 1:
                raise ValueError("ambiguous fake Dataset binding")
            record = matches[0] if matches else None
        else:
            record = self.bindings.get(
                (job_id, dataset, scope_value, boundary)
            )
        if (
            record is not None
            and terminal_only
            and record["status"] == "fetch_failed"
            and record["retryable"] is True
        ):
            return None
        if record is None:
            return None
        result = dict(record)
        result["binding_retryable"] = result.pop("retryable")
        return result

    def list_dataset_checkpoints(
        self,
        *,
        scope_value,
        dataset,
        as_of,
        trade_date_from=None,
        trade_date_to=None,
        statuses=None,
    ):
        cutoff = self._as_utc(as_of)
        allowed = set(statuses) if statuses is not None else None
        rows = []
        for record in self.records.values():
            trade_day = record["trade_date"]
            if (
                record["scope_value"] != scope_value
                or record["dataset"] != dataset
                or record["available_at"] > cutoff
                or record["data_as_of"] > cutoff
                or (
                    dataset == "stock_basic"
                    and record["observed_at"] > cutoff
                )
                or (allowed is not None and record["status"] not in allowed)
                or (
                    trade_date_from is not None
                    and (trade_day is None or trade_day < trade_date_from)
                )
                or (
                    trade_date_to is not None
                    and (trade_day is None or trade_day > trade_date_to)
                )
            ):
                continue
            rows.append(dict(record))
        return sorted(
            rows,
            key=lambda item: (
                item["trade_date"] or date.min,
                item["available_at"],
                item["id"],
            ),
        )


@pytest.fixture(autouse=True)
def reset_single_flight_state():
    _reset_collector_state_for_tests()
    yield
    _reset_collector_state_for_tests()


def _build_collector(
    tmp_path: Path,
    responses=None,
    *,
    last_trade_date_resolver=None,
    provider=None,
    repository=None,
):
    fake_provider = provider or FakeProvider(responses)
    fake_repository = repository or FakeRepository()
    collector = ResearchDatasetCollector(
        fake_provider,
        fake_repository,
        RawArtifactStore(tmp_path / "raw"),
        clock=lambda: AS_OF + timedelta(minutes=1),
        last_trade_date_resolver=last_trade_date_resolver,
    )
    return collector, fake_provider, fake_repository


def _build_real_collector_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    database_name: str,
):
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager(
        db_url=f"sqlite:///{(tmp_path / database_name).as_posix()}"
    )
    registry = DurableJobHandlerRegistry()
    registry.register(
        "research-collector",
        1,
        CollectorJobPayload,
        lambda payload: payload.stock_code,
    )
    return (
        db,
        DurableJobStore(
            registry,
            db,
            lease_seconds=300,
            heartbeat_seconds=30,
        ),
        ResearchSnapshotRepository(db),
    )


def _claim_real_collector_job(
    store: DurableJobStore,
    *,
    task_id: str,
    worker_id: str,
    now: datetime,
    stock_code: str = "600519",
) -> LeaseFence:
    store.enqueue(
        JobEnqueueRequest(
            job_type="research-collector",
            payload={"stock_code": stock_code},
            task_id=task_id,
            stock_code=stock_code,
        ),
        now=now,
    )
    claim = store.claim_next(worker_id, now=now)
    assert claim is not None
    assert claim.task_id == task_id
    return LeaseFence(claim.task_id, claim.worker_id, claim.lease_token)


def test_first_release_dataset_registry_and_a_share_code_mapping_are_fixed() -> None:
    assert DEFAULT_RESEARCH_DATASETS == (
        "stock_basic",
        "daily",
        "adj_factor",
        "daily_basic",
        "fina_indicator",
        "income",
        "balancesheet",
        "cashflow",
        "dividend",
        "stk_holdernumber",
        "forecast",
        "cyq_perf",
        "cyq_chips",
        "stk_limit",
        "suspend_d",
    )
    assert to_tushare_ts_code("600519") == "600519.SH"
    assert to_tushare_ts_code("000001.SZ") == "000001.SZ"
    assert to_tushare_ts_code("920748") == "920748.BJ"
    assert DATASET_DEFINITIONS["daily"].history_days == 450
    assert DATASET_DEFINITIONS["adj_factor"].history_days == 450
    assert DATASET_DEFINITIONS["adj_factor"].market_daily is True
    with pytest.raises(ValueError, match="unsupported A-share"):
        to_tushare_ts_code("AAPL")


def test_daily_filters_future_rows_and_raw_ref_keeps_provider_response(tmp_path: Path) -> None:
    collector, provider, repository = _build_collector(
        tmp_path,
        {
            "daily": pd.DataFrame(
                [
                    {"ts_code": "600519.SH", "trade_date": "20260808", "close": 100.0},
                    {"ts_code": "600519.SH", "trade_date": "20260809", "close": 999.0},
                ]
            )
        },
    )

    result = collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)

    assert result.status == "available"
    assert result.row_count == 1
    assert result.dropped_future_rows == 1
    assert result.available_at <= AS_OF
    assert result.data_as_of <= AS_OF
    assert result.normalized_rows[0]["close"] == 100.0
    assert result.to_frame().to_dict("records") == [dict(result.normalized_rows[0])]
    assert provider.calls[0][1]["end_date"] == "20260808"
    assert provider.calls[0][1]["start_date"] == (
        date(2026, 8, 8) - timedelta(days=449)
    ).strftime("%Y%m%d")
    assert provider.calls[0][1]["ts_code"] == "600519.SH"
    assert result.raw_ref is not None
    raw = json.loads(collector.raw_store.read(result.raw_ref).decode("utf-8"))
    assert len(raw["rows"]) == 2
    assert raw["rows"][1]["close"] == 999.0
    assert repository.inputs[0].normalized == [dict(result.normalized_rows[0])]
    assert repository.inputs[0].raw_ref == result.raw_ref


def test_stale_lease_after_raw_publish_leaves_bounded_orphan_candidate(
    tmp_path: Path,
) -> None:
    class StaleRepository(FakeRepository):
        def write_dataset(self, snapshot, *, lease, establish_reference_for=None):
            raise StaleLeaseError("lease lost after raw publish")

    collector, _provider, _repository = _build_collector(
        tmp_path,
        {
            "daily": pd.DataFrame(
                [{"ts_code": "600519.SH", "trade_date": "20260808", "close": 1.0}]
            )
        },
        repository=StaleRepository(),
    )

    with pytest.raises(StaleLeaseError, match="after raw publish"):
        collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)

    orphan_files = tuple(collector.raw_store.root.glob("*/*.json.gz"))
    assert len(orphan_files) == 1
    old_timestamp = (AS_OF - timedelta(days=2)).timestamp()
    os.utime(orphan_files[0], (old_timestamp, old_timestamp))

    plan = build_raw_retention_plan((), collector.raw_store.root, as_of=AS_OF)

    assert plan.blocked is False
    assert plan.orphan_paths == (
        orphan_files[0].relative_to(collector.raw_store.root).as_posix(),
    )
    assert plan.candidate_paths == plan.orphan_paths


def test_financial_rows_prefer_f_ann_date_then_ann_date_and_mark_unknown_partial(
    tmp_path: Path,
) -> None:
    frame = pd.DataFrame(
        [
            {
                "ts_code": "600519.SH",
                "f_ann_date": "20260809",
                "ann_date": "20260701",
                "end_date": "20260630",
                "revenue": 1,
            },
            {
                "ts_code": "600519.SH",
                "f_ann_date": "20260801",
                "ann_date": "20260809",
                "end_date": "20260630",
                "revenue": 2,
            },
            {
                "ts_code": "600519.SH",
                "f_ann_date": None,
                "ann_date": "20260731",
                "end_date": "20260331",
                "revenue": 3,
            },
            {
                "ts_code": "600519.SH",
                "f_ann_date": None,
                "ann_date": None,
                "end_date": "20251231",
                "revenue": 4,
            },
            {
                "ts_code": "600519.SH",
                "f_ann_date": "malformed",
                "ann_date": "20260701",
                "end_date": "20251231",
                "revenue": 5,
            },
        ]
    )
    collector, _provider, repository = _build_collector(tmp_path, {"income": frame})

    result = collector.collect_dataset("600519", "income", as_of=AS_OF, lease=LEASE)

    assert result.status == "partial"
    assert result.row_count == 2
    assert result.dropped_future_rows == 1
    assert result.dropped_unknown_availability_rows == 2
    assert {row["revenue"] for row in result.normalized_rows} == {2, 3}
    assert repository.inputs[0].announcement_date == date(2026, 8, 1)
    assert repository.inputs[0].report_date == date(2026, 6, 30)


@pytest.mark.parametrize(
    ("dataset", "response", "expected_status"),
    [
        (
            "daily",
            pd.DataFrame([{"trade_date": "20260808", "close": 1.0}]),
            "available",
        ),
        ("daily", pd.DataFrame(columns=["trade_date", "close"]), "empty"),
        (
            "daily",
            pd.DataFrame(
                [
                    {"trade_date": "20260808", "close": 1.0},
                    {"trade_date": None, "close": 2.0},
                ]
            ),
            "partial",
        ),
        (
            "daily",
            pd.DataFrame([{"trade_date": "20260701", "close": 1.0}]),
            "stale",
        ),
        ("daily", TusharePermissionError("no privilege"), "permission_denied"),
        ("made_up", pd.DataFrame(), "not_supported"),
        ("daily", ValueError("invalid provider payload"), "fetch_failed"),
    ],
)
def test_all_seven_statuses_remain_distinct_and_failures_never_become_zero(
    tmp_path: Path,
    dataset: str,
    response,
    expected_status: str,
) -> None:
    collector, provider, repository = _build_collector(tmp_path, {dataset: response})

    result = collector.collect_dataset("600519", dataset, as_of=AS_OF, lease=LEASE)

    assert result.status == expected_status
    assert result.available_at <= AS_OF
    assert result.data_as_of <= AS_OF
    persisted = repository.inputs[0]
    assert persisted.status == expected_status
    if expected_status == "empty":
        assert persisted.normalized == []
    elif expected_status in {"permission_denied", "not_supported", "fetch_failed"}:
        assert persisted.normalized is None
        assert result.normalized_rows == ()
    else:
        assert isinstance(persisted.normalized, list)
    if expected_status == "not_supported":
        assert provider.calls == []


def test_no_internal_retry_and_retry_after_is_exposed(tmp_path: Path) -> None:
    error = TushareRateLimitError("limited", retry_after=17.0)
    collector, provider, repository = _build_collector(tmp_path, {"daily": error})

    with pytest.raises(TushareRateLimitError) as raised:
        collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)

    assert raised.value.retry_after == 17.0
    assert len(provider.calls) == 1
    assert len(repository.inputs) == 1
    assert repository.inputs[0].status == "fetch_failed"
    assert repository.inputs[0].normalized is None


@pytest.mark.parametrize(
    "error",
    [
        TushareTimeoutError("timed out"),
        TushareConnectionError("connection failed"),
    ],
)
def test_transient_timeout_and_connection_are_audited_then_rethrown(
    tmp_path: Path,
    error,
) -> None:
    collector, provider, repository = _build_collector(tmp_path, {"daily": error})

    with pytest.raises(type(error)):
        collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)

    assert len(provider.calls) == 1
    assert len(repository.inputs) == 1
    assert repository.inputs[0].status == "fetch_failed"


@pytest.mark.parametrize("status", [408, 503, 599])
def test_transport_5xx_is_audited_then_rethrown_for_durable_retry(
    tmp_path: Path,
    status: int,
) -> None:
    error = TushareTransportError(
        f"transport failure {status}", status_code=status
    )
    collector, provider, repository = _build_collector(tmp_path, {"daily": error})

    with pytest.raises(TushareTransportError):
        collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)

    assert len(provider.calls) == 1
    assert len(repository.inputs) == 1
    assert repository.inputs[0].status == "fetch_failed"


def test_transport_status_attribute_is_authoritative_over_legacy_message(
    tmp_path: Path,
) -> None:
    error = TushareTransportError(
        "legacy text mentions HTTP 400",
        status_code=503,
    )
    collector, _provider, repository = _build_collector(tmp_path, {"daily": error})

    with pytest.raises(TushareTransportError):
        collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)

    assert repository.inputs[0].status == "fetch_failed"


def test_transport_legacy_message_status_remains_compatible(tmp_path: Path) -> None:
    error = TushareTransportError("Tushare daily HTTP 500")
    collector, _provider, repository = _build_collector(tmp_path, {"daily": error})

    with pytest.raises(TushareTransportError):
        collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)

    assert repository.inputs[0].status == "fetch_failed"


def test_transport_400_is_terminal_and_error_diagnostics_are_sanitized(
    tmp_path: Path,
) -> None:
    error = TushareTransportError(
        "https://user:url-pass-secret@example.com/v1/PATH-CREDENTIAL-SECRET/resource"
        "?q=query-secret token=token-secret api_key=key-secret "
        "password=hunter2 cookie=session-secret client_secret=client-secret "
        "Authorization: Bearer top-secret",
        status_code=400,
    )
    collector, provider, repository = _build_collector(tmp_path, {"daily": error})

    result = collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)

    assert result.status == "fetch_failed"
    assert len(provider.calls) == 1
    assert len(repository.inputs) == 1
    message = result.error_message_sanitized or ""
    for secret in (
        "top-secret",
        "token-secret",
        "key-secret",
        "hunter2",
        "session-secret",
        "client-secret",
        "user",
        "url-pass-secret",
        "PATH-CREDENTIAL-SECRET",
        "query-secret",
    ):
        assert secret not in message
    assert "https://example.com/[REDACTED_PATH]" in message


def test_cancellation_propagates_without_writing_failure_snapshot(tmp_path: Path) -> None:
    collector, provider, repository = _build_collector(
        tmp_path,
        {"daily": TushareRequestCancelled("cancelled")},
    )

    with pytest.raises(TushareRequestCancelled):
        collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)

    assert len(provider.calls) == 1
    assert repository.inputs == []


def test_cyq_query_starts_after_persisted_trade_date_and_can_skip_current_range(
    tmp_path: Path,
) -> None:
    def resolver(dataset, scope, cutoff):
        return date(2026, 8, 6)
    collector, provider, _repository = _build_collector(
        tmp_path,
        {"cyq_perf": pd.DataFrame([{"trade_date": "20260807", "winner_rate": 0.8}])},
        last_trade_date_resolver=resolver,
    )

    result = collector.collect_dataset("600519", "cyq_perf", as_of=AS_OF, lease=LEASE)

    assert result.status == "available"
    assert provider.calls[0][1]["start_date"] == "20260807"
    assert provider.calls[0][1]["end_date"] == "20260808"

    up_to_date_collector, up_to_date_provider, up_to_date_repository = _build_collector(
        tmp_path / "up-to-date",
        {"cyq_chips": pd.DataFrame([{"trade_date": "20260808", "price": 99.0}])},
        last_trade_date_resolver=lambda dataset, scope, cutoff: None,
    )
    first_current = up_to_date_collector.collect_dataset(
        "600519", "cyq_chips", as_of=AS_OF, lease=LEASE
    )
    skipped = up_to_date_collector.collect_dataset(
        "600519",
        "cyq_chips",
        as_of=AS_OF,
        lease=LeaseFence("second-job", "worker-2", "token-2"),
    )
    assert skipped.skipped is True
    assert skipped.reused is True
    assert skipped.snapshot is not None
    assert skipped.snapshot.content_hash == first_current.snapshot.content_hash
    assert skipped.normalized_rows == first_current.normalized_rows
    assert len(up_to_date_provider.calls) == 1
    assert len(up_to_date_repository.inputs) == 2
    assert up_to_date_repository.leases[-1].job_id == "second-job"


def test_cyq_same_stock_and_range_shares_only_transport_not_persistence(
    tmp_path: Path,
) -> None:
    started = threading.Event()
    release = threading.Event()

    def blocking_response():
        started.set()
        assert release.wait(timeout=5)
        return pd.DataFrame([{"trade_date": "20260807", "price": 100.0}])

    provider = FakeProvider({"cyq_chips": blocking_response})
    repository = FakeRepository()
    collector, _provider, _repository = _build_collector(
        tmp_path,
        provider=provider,
        repository=repository,
        last_trade_date_resolver=lambda dataset, scope, cutoff: None,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            collector.collect_dataset,
            "600519",
            "cyq_chips",
            as_of=AS_OF,
            lease=LEASE,
        )
        assert started.wait(timeout=5)
        second = executor.submit(
            collector.collect_dataset,
            "600519",
            "cyq_chips",
            as_of=AS_OF,
            lease=LEASE,
        )
        time.sleep(0.05)
        release.set()
        first_result = first.result(timeout=5)
        second_result = second.result(timeout=5)

    assert len(provider.calls) == 1
    assert len(repository.inputs) == 2
    assert first_result.normalized_rows == second_result.normalized_rows
    assert repository.leases == [LEASE, LEASE]


def test_cyq_single_flight_filters_1459_and_1501_per_job_boundary(
    tmp_path: Path,
) -> None:
    started = threading.Event()
    release = threading.Event()

    def blocking_response():
        started.set()
        assert release.wait(timeout=5)
        return pd.DataFrame([{"trade_date": "20260808", "price": 100.0}])

    provider = FakeProvider({"cyq_chips": blocking_response})
    repository = FakeRepository()
    collector, _provider, _repository = _build_collector(
        tmp_path,
        provider=provider,
        repository=repository,
        last_trade_date_resolver=lambda dataset, scope, cutoff: None,
    )
    before_close = datetime(2026, 8, 8, 6, 59, tzinfo=timezone.utc)
    after_close = datetime(2026, 8, 8, 7, 1, tzinfo=timezone.utc)
    before_lease = LeaseFence("before-job", "worker-before", "before-token")
    after_lease = LeaseFence("after-job", "worker-after", "after-token")

    with ThreadPoolExecutor(max_workers=2) as executor:
        before_future = executor.submit(
            collector.collect_dataset,
            "600519",
            "cyq_chips",
            as_of=before_close,
            lease=before_lease,
        )
        assert started.wait(timeout=5)
        after_future = executor.submit(
            collector.collect_dataset,
            "600519",
            "cyq_chips",
            as_of=after_close,
            lease=after_lease,
        )
        time.sleep(0.05)
        release.set()
        before_result = before_future.result(timeout=5)
        after_result = after_future.result(timeout=5)

    assert len(provider.calls) == 1
    assert len(repository.inputs) == 2
    assert before_result.status == "empty"
    assert before_result.normalized_rows == ()
    assert after_result.status == "available"
    assert [dict(row) for row in after_result.normalized_rows] == [
        {"trade_date": "20260808", "price": 100.0}
    ]
    persisted_by_job = dict(zip(repository.leases, repository.inputs))
    assert persisted_by_job[before_lease].normalized == []
    assert persisted_by_job[after_lease].normalized == [
        {"trade_date": "20260808", "price": 100.0}
    ]


def test_cyq_d_plus_one_increment_merges_persisted_consumption_window(
    tmp_path: Path,
) -> None:
    responses = iter(
        [
            pd.DataFrame([{"trade_date": "20260807", "price": 99.0}]),
            pd.DataFrame([{"trade_date": "20260808", "price": 100.0}]),
        ]
    )
    collector, provider, repository = _build_collector(
        tmp_path,
        {"cyq_chips": lambda: next(responses)},
        last_trade_date_resolver=lambda dataset, scope, cutoff: None,
    )
    day_one = datetime(2026, 8, 7, 7, 1, tzinfo=timezone.utc)
    day_two = datetime(2026, 8, 8, 7, 1, tzinfo=timezone.utc)

    first = collector.collect_dataset(
        "600519", "cyq_chips", as_of=day_one, lease=LEASE
    )
    second = collector.collect_dataset(
        "600519",
        "cyq_chips",
        as_of=day_two,
        lease=LeaseFence("day-two-job", "worker-two", "token-two"),
    )

    assert len(provider.calls) == 2
    assert provider.calls[1][1]["start_date"] == "20260808"
    assert len(repository.inputs) == 3
    assert repository.leases[-1].job_id == "day-two-job"
    assert repository.inputs[-1].normalized == [
        {"trade_date": "20260807", "price": 99.0}
    ]
    assert first.row_count == 1
    assert [row["trade_date"] for row in second.normalized_rows] == [
        "20260807",
        "20260808",
    ]
    assert second.row_count == 2
    assert len(second.source_snapshot_hashes) == 2


def test_incremental_reuse_binds_every_source_hash_and_resumes_without_refetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db, store, repository = _build_real_collector_store(
        tmp_path,
        monkeypatch,
        database_name="incremental-lineage-resume.db",
    )
    _reset_collector_state_for_tests()
    boundary = datetime.now(timezone.utc)
    cutoff = (boundary + timedelta(hours=8)).date()
    first_day = cutoff - timedelta(days=2)
    second_day = cutoff - timedelta(days=1)
    first_lease = _claim_real_collector_job(
        store,
        task_id="incremental-lineage-first-job",
        worker_id="first-worker",
        now=boundary,
    )
    second_boundary = boundary + timedelta(microseconds=1)
    second_lease = _claim_real_collector_job(
        store,
        task_id="incremental-lineage-second-job",
        worker_id="second-worker",
        now=second_boundary,
    )
    try:
        first_provider = FakeProvider(
            {
                "cyq_chips": pd.DataFrame(
                    [
                        {
                            "trade_date": first_day.strftime("%Y%m%d"),
                            "price": 99.0,
                        }
                    ]
                )
            }
        )
        ResearchDatasetCollector(
            first_provider,
            repository,
            RawArtifactStore(tmp_path / "raw-incremental-lineage"),
            clock=lambda: boundary,
        ).collect_dataset(
            "600519",
            "cyq_chips",
            as_of=boundary,
            lease=first_lease,
        )

        second_provider = FakeProvider(
            {
                "cyq_chips": pd.DataFrame(
                    [
                        {
                            "trade_date": second_day.strftime("%Y%m%d"),
                            "price": 100.0,
                        }
                    ]
                )
            }
        )
        raw_store = RawArtifactStore(tmp_path / "raw-incremental-lineage")
        second = ResearchDatasetCollector(
            second_provider,
            repository,
            raw_store,
            clock=lambda: second_boundary,
        ).collect_dataset(
            "600519",
            "cyq_chips",
            as_of=second_boundary,
            lease=second_lease,
        )

        with db.get_session() as session:
            payloads = [
                json.loads(value)
                for (value,) in session.query(JobEventRecord.payload_json)
                .filter_by(
                    job_id=second_lease.job_id,
                    event_type="research_dataset_snapshot",
                )
                .all()
            ]
        assert {item["content_hash"] for item in payloads} == set(
            second.source_snapshot_hashes
        )
        assert len(payloads) == len(second.source_snapshot_hashes) == 2

        _reset_collector_state_for_tests()
        replay_provider = FakeProvider(
            {"cyq_chips": AssertionError("resume must not refetch")}
        )
        replayed = ResearchDatasetCollector(
            replay_provider,
            repository,
            raw_store,
            clock=lambda: second_boundary,
        ).collect_dataset(
            "600519",
            "cyq_chips",
            as_of=second_boundary,
            lease=second_lease,
        )

        assert replay_provider.calls == []
        assert replayed.snapshot.content_hash == second.snapshot.content_hash
        assert replayed.source_snapshot_hashes == second.source_snapshot_hashes
        assert [row["trade_date"] for row in replayed.normalized_rows] == [
            first_day.strftime("%Y%m%d"),
            second_day.strftime("%Y%m%d"),
        ]
        with db.get_session() as session:
            assert (
                session.query(JobEventRecord)
                .filter_by(
                    job_id=second_lease.job_id,
                    event_type="research_dataset_snapshot",
                )
                .count()
                == 2
            )
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def test_incremental_empty_primary_keeps_full_lineage_on_cold_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db, store, repository = _build_real_collector_store(
        tmp_path,
        monkeypatch,
        database_name="incremental-empty-primary-resume.db",
    )
    _reset_collector_state_for_tests()
    boundary = datetime.now(timezone.utc)
    cutoff = (boundary + timedelta(hours=8)).date()
    frozen_day = cutoff - timedelta(days=1)
    first_lease = _claim_real_collector_job(
        store,
        task_id="incremental-empty-primary-first-job",
        worker_id="first-worker",
        now=boundary,
    )
    second_boundary = boundary + timedelta(microseconds=1)
    second_lease = _claim_real_collector_job(
        store,
        task_id="incremental-empty-primary-second-job",
        worker_id="second-worker",
        now=second_boundary,
    )
    raw_store = RawArtifactStore(tmp_path / "raw-incremental-empty-primary")
    try:
        ResearchDatasetCollector(
            FakeProvider(
                {
                    "cyq_chips": pd.DataFrame(
                        [
                            {
                                "trade_date": frozen_day.strftime("%Y%m%d"),
                                "price": 99.0,
                            }
                        ]
                    )
                }
            ),
            repository,
            raw_store,
            clock=lambda: boundary,
        ).collect_dataset(
            "600519",
            "cyq_chips",
            as_of=boundary,
            lease=first_lease,
        )

        empty_provider = FakeProvider({"cyq_chips": pd.DataFrame()})
        second = ResearchDatasetCollector(
            empty_provider,
            repository,
            raw_store,
            clock=lambda: second_boundary,
        ).collect_dataset(
            "600519",
            "cyq_chips",
            as_of=second_boundary,
            lease=second_lease,
        )

        assert len(empty_provider.calls) == 1
        assert second.status == "available"
        assert second.available_at == second_boundary
        assert second.data_as_of == second_boundary
        assert [row["trade_date"] for row in second.normalized_rows] == [
            frozen_day.strftime("%Y%m%d")
        ]
        assert len(second.source_snapshot_hashes) == 2
        with db.get_session() as session:
            payloads = [
                json.loads(value)
                for (value,) in session.query(JobEventRecord.payload_json)
                .filter_by(
                    job_id=second_lease.job_id,
                    event_type="research_dataset_snapshot",
                )
                .order_by(JobEventRecord.id.asc())
                .all()
            ]
        assert [item["status"] for item in payloads] == ["empty", "available"]
        assert {item["content_hash"] for item in payloads} == set(
            second.source_snapshot_hashes
        )

        _reset_collector_state_for_tests()
        replay_provider = FakeProvider(
            {"cyq_chips": AssertionError("cold resume must not refetch")}
        )
        replayed = ResearchDatasetCollector(
            replay_provider,
            repository,
            raw_store,
            clock=lambda: second_boundary,
        ).collect_dataset(
            "600519",
            "cyq_chips",
            as_of=second_boundary,
            lease=second_lease,
        )

        assert replay_provider.calls == []
        assert replayed.source_snapshot_hashes == second.source_snapshot_hashes
        assert replayed.available_at == second_boundary
        assert replayed.data_as_of == second_boundary
        assert [row["trade_date"] for row in replayed.normalized_rows] == [
            frozen_day.strftime("%Y%m%d")
        ]
        with db.get_session() as session:
            assert (
                session.query(JobEventRecord)
                .filter_by(
                    job_id=second_lease.job_id,
                    event_type="research_dataset_snapshot",
                )
                .count()
                == 2
            )
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def test_bundle_exposes_normalized_rows_and_detached_frames_without_refetch(
    tmp_path: Path,
) -> None:
    collector, provider, _repository = _build_collector(
        tmp_path,
        {
            "daily": pd.DataFrame([{"trade_date": "20260808", "close": 100.0}]),
            "daily_basic": pd.DataFrame([{"trade_date": "20260808", "pe": 20.0}]),
        },
    )

    bundle = collector.collect(
        "600519",
        as_of=AS_OF,
        lease=LEASE,
        datasets=("daily", "daily_basic"),
    )
    frames = bundle.to_frames()

    assert set(bundle.normalized_by_dataset) == {"daily", "daily_basic"}
    assert bundle.rows_by_dataset == bundle.normalized_by_dataset
    assert frames["daily"].iloc[0]["close"] == 100.0
    assert frames["daily_basic"].iloc[0]["pe"] == 20.0
    frames["daily"].loc[0, "close"] = -1
    assert bundle.by_dataset["daily"].normalized_rows[0]["close"] == 100.0
    with pytest.raises(TypeError):
        bundle.by_dataset["daily"].normalized_rows[0]["close"] = -2
    assert bundle.frames_copy()["daily"].iloc[0]["close"] == 100.0
    assert [name for name, _params in provider.calls] == ["daily", "daily_basic"]
    assert bundle.max_available_at == max(
        item.available_at for item in bundle.datasets
    )
    assert bundle.max_available_at <= AS_OF


def test_same_job_reentry_reuses_terminal_dataset_without_second_provider_call(
    tmp_path: Path,
) -> None:
    collector, provider, repository = _build_collector(
        tmp_path,
        {"daily": pd.DataFrame([{"trade_date": "20260808", "close": 100.0}])},
    )

    first = collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)
    second = collector.collect_dataset("600519", "daily", as_of=AS_OF, lease=LEASE)

    assert len(provider.calls) == 1
    assert len(repository.inputs) == 1
    assert second.reused is True
    assert second.skipped is True
    assert second.snapshot.content_hash == first.snapshot.content_hash
    assert second.normalized_rows == first.normalized_rows


@pytest.mark.parametrize(
    "damage",
    ("row_payload", "missing_row", "cross_stock_event"),
)
def test_terminal_checkpoint_damage_fails_closed_before_recovery_provider_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    db, store, repository = _build_real_collector_store(
        tmp_path,
        monkeypatch,
        database_name=f"terminal-{damage}.db",
    )
    boundary = datetime.now(timezone.utc)
    trade_day = (boundary + timedelta(hours=8)).date() - timedelta(days=1)
    lease = _claim_real_collector_job(
        store,
        task_id=f"terminal-{damage}-job",
        worker_id="seed-worker",
        now=boundary,
    )
    seed_provider = FakeProvider(
        {
            "daily": pd.DataFrame(
                [
                    {
                        "trade_date": trade_day.strftime("%Y%m%d"),
                        "close": 100.0,
                    }
                ]
            )
        }
    )
    try:
        ResearchDatasetCollector(
            seed_provider,
            repository,
            RawArtifactStore(tmp_path / f"raw-terminal-seed-{damage}"),
            clock=lambda: boundary,
        ).collect_dataset("600519", "daily", as_of=boundary, lease=lease)
        assert len(seed_provider.calls) == 1

        with db.get_session() as session:
            row = session.query(ResearchDatasetSnapshotRecord).filter_by(
                dataset="daily",
                scope_value="600519",
            ).one()
            event = session.query(JobEventRecord).filter_by(
                job_id=lease.job_id,
                event_type="research_dataset_snapshot",
            ).one()
            if damage == "row_payload":
                row.normalized_json = json.dumps(
                    [{"trade_date": trade_day.isoformat(), "close": 999.0}]
                )
            elif damage == "missing_row":
                session.delete(row)
            else:
                payload = json.loads(event.payload_json)
                payload["scope_value"] = "000001"
                event.payload_json = json.dumps(payload, sort_keys=True)
            session.commit()

        recovery_provider = FakeProvider(
            {
                "daily": pd.DataFrame(
                    [
                        {
                            "trade_date": trade_day.strftime("%Y%m%d"),
                            "close": 101.0,
                        }
                    ]
                )
            }
        )
        recovery_collector = ResearchDatasetCollector(
            recovery_provider,
            repository,
            RawArtifactStore(tmp_path / f"raw-terminal-recovery-{damage}"),
            clock=lambda: boundary,
        )

        with pytest.raises(ValueError):
            recovery_collector.collect_dataset(
                "600519",
                "daily",
                as_of=boundary,
                lease=lease,
            )

        assert recovery_provider.calls == []
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def test_retryable_checkpoint_still_allows_one_real_recovery_provider_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _db, store, repository = _build_real_collector_store(
        tmp_path,
        monkeypatch,
        database_name="retryable-checkpoint.db",
    )
    boundary = datetime.now(timezone.utc)
    trade_day = (boundary + timedelta(hours=8)).date() - timedelta(days=1)
    lease = _claim_real_collector_job(
        store,
        task_id="retryable-checkpoint-job",
        worker_id="retry-worker",
        now=boundary,
    )
    try:
        first_provider = FakeProvider(
            {"daily": TushareTimeoutError("first attempt timed out")}
        )
        with pytest.raises(TushareTimeoutError):
            ResearchDatasetCollector(
                first_provider,
                repository,
                RawArtifactStore(tmp_path / "raw-retryable-first"),
                clock=lambda: boundary,
            ).collect_dataset("600519", "daily", as_of=boundary, lease=lease)
        assert len(first_provider.calls) == 1

        _reset_collector_state_for_tests()
        recovery_provider = FakeProvider(
            {
                "daily": pd.DataFrame(
                    [
                        {
                            "trade_date": trade_day.strftime("%Y%m%d"),
                            "close": 100.0,
                        }
                    ]
                )
            }
        )
        recovered = ResearchDatasetCollector(
            recovery_provider,
            repository,
            RawArtifactStore(tmp_path / "raw-retryable-recovery"),
            clock=lambda: boundary,
        ).collect_dataset("600519", "daily", as_of=boundary, lease=lease)

        assert recovered.status == "available"
        assert len(recovery_provider.calls) == 1
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def test_valid_cross_stock_checkpoint_event_fails_before_provider_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db, store, repository = _build_real_collector_store(
        tmp_path,
        monkeypatch,
        database_name="cross-stock-checkpoint.db",
    )
    boundary = datetime.now(timezone.utc)
    trade_day = (boundary + timedelta(hours=8)).date() - timedelta(days=1)
    requested_lease = _claim_real_collector_job(
        store,
        task_id="requested-stock-job",
        worker_id="requested-worker",
        now=boundary,
    )
    other_lease = _claim_real_collector_job(
        store,
        task_id="other-stock-job",
        worker_id="other-worker",
        stock_code="000001",
        now=boundary + timedelta(microseconds=1),
    )
    try:
        repository.write_dataset(
            DatasetSnapshotInput(
                dataset="daily",
                scope_type="stock",
                scope_value="000001",
                market="A",
                provider="tushare",
                schema_version=DATASET_DEFINITIONS["daily"].schema_version,
                trade_date=trade_day,
                data_as_of=boundary - timedelta(days=1),
                available_at=boundary - timedelta(days=1),
                observed_at=boundary - timedelta(days=1),
                status="available",
                normalized=[
                    {
                        "trade_date": trade_day.strftime("%Y%m%d"),
                        "close": 10.0,
                    }
                ],
                knowledge_as_of=boundary,
            ),
            lease=other_lease,
        )
        with db.get_session() as session:
            other_event = session.query(JobEventRecord).filter_by(
                job_id=other_lease.job_id,
                event_type="research_dataset_snapshot",
            ).one()
            session.add(
                JobEventRecord(
                    job_id=requested_lease.job_id,
                    event_type=other_event.event_type,
                    stage=other_event.stage,
                    payload_json=other_event.payload_json,
                    created_at=other_event.created_at,
                )
            )
            session.commit()

        provider = FakeProvider(
            {
                "daily": pd.DataFrame(
                    [
                        {
                            "trade_date": trade_day.strftime("%Y%m%d"),
                            "close": 100.0,
                        }
                    ]
                )
            }
        )
        with pytest.raises(ValueError):
            ResearchDatasetCollector(
                provider,
                repository,
                RawArtifactStore(tmp_path / "raw-cross-stock-checkpoint"),
                clock=lambda: boundary,
            ).collect_dataset(
                "600519",
                "daily",
                as_of=boundary,
                lease=requested_lease,
            )

        assert provider.calls == []
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


@pytest.mark.parametrize(
    "damage",
    ("missing_cached_row", "cold_status_tamper"),
)
def test_incremental_checkpoint_damage_fails_before_new_job_provider_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    db, store, repository = _build_real_collector_store(
        tmp_path,
        monkeypatch,
        database_name=f"incremental-{damage}.db",
    )
    boundary = datetime.now(timezone.utc)
    trade_day = (boundary + timedelta(hours=8)).date() - timedelta(days=1)
    seed_lease = _claim_real_collector_job(
        store,
        task_id="incremental-cache-seed-job",
        worker_id="seed-worker",
        now=boundary,
    )
    seed_provider = FakeProvider(
        {
            "cyq_chips": pd.DataFrame(
                [
                    {
                        "trade_date": trade_day.strftime("%Y%m%d"),
                        "price": 99.0,
                    }
                ]
            )
        }
    )
    try:
        ResearchDatasetCollector(
            seed_provider,
            repository,
            RawArtifactStore(tmp_path / "raw-incremental-cache-seed"),
            clock=lambda: boundary,
        ).collect_dataset(
            "600519",
            "cyq_chips",
            as_of=boundary,
            lease=seed_lease,
        )
        assert len(seed_provider.calls) == 1
        assert ("cyq_chips", "600519") in _CYQ_HIGH_WATERMARKS

        with db.get_session() as session:
            row = session.query(ResearchDatasetSnapshotRecord).filter_by(
                dataset="cyq_chips",
                scope_value="600519",
            ).one()
            if damage == "missing_cached_row":
                session.delete(row)
            else:
                row.status = "fetch_failed"
            session.commit()
        if damage == "cold_status_tamper":
            _reset_collector_state_for_tests()

        recovery_boundary = boundary + timedelta(microseconds=1)
        recovery_lease = _claim_real_collector_job(
            store,
            task_id="incremental-cache-recovery-job",
            worker_id="recovery-worker",
            now=recovery_boundary,
        )
        recovery_provider = FakeProvider(
            {
                "cyq_chips": pd.DataFrame(
                    [
                        {
                            "trade_date": trade_day.strftime("%Y%m%d"),
                            "price": 100.0,
                        }
                    ]
                )
            }
        )

        with pytest.raises(ValueError):
            ResearchDatasetCollector(
                recovery_provider,
                repository,
                RawArtifactStore(tmp_path / "raw-incremental-cache-recovery"),
                clock=lambda: recovery_boundary,
            ).collect_dataset(
                "600519",
                "cyq_chips",
                as_of=recovery_boundary,
                lease=recovery_lease,
            )

        assert recovery_provider.calls == []
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def test_corrupt_current_state_checkpoint_fails_closed_without_provider_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db, store, repository = _build_real_collector_store(
        tmp_path,
        monkeypatch,
        database_name="current-state-corrupt.db",
    )
    boundary = datetime.now(timezone.utc)
    seed_lease = _claim_real_collector_job(
        store,
        task_id="current-state-seed-job",
        worker_id="seed-worker",
        now=boundary,
    )
    try:
        repository.write_dataset(
            DatasetSnapshotInput(
                dataset="stock_basic",
                scope_type="stock",
                scope_value="600519",
                market="A",
                provider="tushare",
                schema_version=DATASET_DEFINITIONS["stock_basic"].schema_version,
                data_as_of=boundary,
                available_at=boundary,
                observed_at=boundary,
                status="available",
                normalized=[
                    {
                        "ts_code": "600519.SH",
                        "name": "seed company",
                    }
                ],
                knowledge_as_of=boundary,
            ),
            lease=seed_lease,
        )
        with db.get_session() as session:
            row = session.query(ResearchDatasetSnapshotRecord).filter_by(
                dataset="stock_basic",
                scope_value="600519",
            ).one()
            row.normalized_json = json.dumps(
                [{"ts_code": "600519.SH", "name": "tampered company"}]
            )
            session.commit()

        replay_lease = _claim_real_collector_job(
            store,
            task_id="current-state-replay-job",
            worker_id="replay-worker",
            now=boundary + timedelta(microseconds=1),
        )
        provider = FakeProvider(
            {
                "stock_basic": pd.DataFrame(
                    [{"ts_code": "600519.SH", "name": "provider company"}]
                )
            }
        )

        with pytest.raises(ValueError):
            ResearchDatasetCollector(
                provider,
                repository,
                RawArtifactStore(tmp_path / "raw-current-state-replay"),
                clock=lambda: boundary + timedelta(microseconds=1),
            ).collect_dataset(
                "600519",
                "stock_basic",
                as_of=boundary + timedelta(microseconds=1),
                lease=replay_lease,
                reference_mode="historical",
            )

        assert provider.calls == []
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def test_bundle_retry_only_refetches_endpoint_that_failed_transiently(
    tmp_path: Path,
) -> None:
    income_attempts = 0

    def flaky_income():
        nonlocal income_attempts
        income_attempts += 1
        if income_attempts == 1:
            return TushareTimeoutError("first attempt timed out")
        return pd.DataFrame(
            [{"f_ann_date": "20260801", "end_date": "20260630", "revenue": 10}]
        )

    collector, provider, _repository = _build_collector(
        tmp_path,
        {
            "daily": pd.DataFrame([{"trade_date": "20260808", "close": 100.0}]),
            "daily_basic": pd.DataFrame([{"trade_date": "20260808", "pe": 20.0}]),
            "income": flaky_income,
        },
    )
    datasets = ("daily", "daily_basic", "income")

    with pytest.raises(TushareTimeoutError):
        collector.collect("600519", as_of=AS_OF, lease=LEASE, datasets=datasets)
    retried = collector.collect(
        "600519", as_of=AS_OF, lease=LEASE, datasets=datasets
    )

    call_names = [name for name, _params in provider.calls]
    assert call_names.count("daily") == 1
    assert call_names.count("daily_basic") == 1
    assert call_names.count("income") == 2
    assert retried.by_dataset["daily"].reused is True
    assert retried.by_dataset["daily_basic"].reused is True
    assert retried.by_dataset["income"].status == "available"


def test_lease_reclaim_hydrates_job_binding_from_database_without_refetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager(db_url=f"sqlite:///{(tmp_path / 'collector.db').as_posix()}")
    registry = DurableJobHandlerRegistry()
    registry.register(
        "research-collector",
        1,
        CollectorJobPayload,
        lambda payload: payload.stock_code,
    )
    store = DurableJobStore(registry, db, lease_seconds=90, heartbeat_seconds=15)
    claimed_at = datetime.now(timezone.utc)
    as_of = claimed_at
    trade_date = (as_of + timedelta(hours=8)).date() - timedelta(days=1)
    provider = FakeProvider(
        {
            "daily": pd.DataFrame(
                [{"trade_date": trade_date.strftime("%Y%m%d"), "close": 100.0}]
            )
        }
    )
    repository = ResearchSnapshotRepository(db)
    try:
        store.enqueue(
            JobEnqueueRequest(
                job_type="research-collector",
                payload={"stock_code": "600519"},
                task_id="reclaimed-research-job",
            ),
            now=claimed_at,
        )
        first_claim = store.claim_next("worker-one", now=claimed_at)
        assert first_claim is not None
        first_lease = LeaseFence(
            first_claim.task_id,
            first_claim.worker_id,
            first_claim.lease_token,
        )
        first_collector = ResearchDatasetCollector(
            provider,
            repository,
            RawArtifactStore(tmp_path / "raw-real"),
            clock=lambda: as_of,
        )
        first = first_collector.collect_dataset(
            "600519", "daily", as_of=as_of, lease=first_lease
        )
        with db.get_session() as session:
            assert first.snapshot is not None
            for index in range(250):
                session.add(
                    JobEventRecord(
                        job_id=first_claim.task_id,
                        event_type="research_dataset_snapshot",
                        stage="research_data",
                        payload_json=json.dumps(
                            {
                                "dataset": "daily",
                                "scope_type": "stock",
                                "scope_value": "600519",
                                "knowledge_as_of": (
                                    as_of + timedelta(microseconds=index + 1)
                                ).replace(tzinfo=None).isoformat(
                                    timespec="microseconds"
                                )
                                + "Z",
                                "content_hash": first.snapshot.content_hash,
                                "status": first.status,
                                "retryable": False,
                            },
                            sort_keys=True,
                        ),
                        created_at=claimed_at.replace(tzinfo=None),
                    )
                )
            session.commit()

        reclaimed_at = claimed_at + timedelta(seconds=91)
        store.recover_expired_leases(now=reclaimed_at)
        second_claim = store.claim_next("worker-two", now=reclaimed_at)
        assert second_claim is not None
        assert second_claim.task_id == first_claim.task_id
        assert second_claim.lease_token != first_claim.lease_token
        second_lease = LeaseFence(
            second_claim.task_id,
            second_claim.worker_id,
            second_claim.lease_token,
        )
        _reset_collector_state_for_tests()
        second_collector = ResearchDatasetCollector(
            provider,
            repository,
            RawArtifactStore(tmp_path / "raw-real"),
            clock=lambda: as_of,
        )
        second = second_collector.collect_dataset(
            "600519", "daily", as_of=as_of, lease=second_lease
        )

        assert len(provider.calls) == 1
        assert second.reused is True
        assert second.snapshot.content_hash == first.snapshot.content_hash
        assert second.normalized_rows == first.normalized_rows
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def _cached_cyq_result(
    hash_index: int,
    trade_day: date,
) -> DatasetCollectionResult:
    content_hash = f"{hash_index:064x}"
    available_at = datetime(
        trade_day.year,
        trade_day.month,
        trade_day.day,
        7,
        tzinfo=timezone.utc,
    )
    return DatasetCollectionResult(
        dataset="cyq_chips",
        status="available",
        row_count=1,
        snapshot=SnapshotWriteResult(hash_index + 1, content_hash, True),
        query_params={},
        normalized_rows=(
            {"trade_date": trade_day.strftime("%Y%m%d"), "price": hash_index},
        ),
        available_at=available_at,
        data_as_of=available_at,
        trade_date=trade_day,
        source_snapshot_hashes=(content_hash,),
    )


def test_job_terminal_result_lru_eviction_rehydrates_without_provider_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager(db_url=f"sqlite:///{(tmp_path / 'lru.db').as_posix()}")
    registry = DurableJobHandlerRegistry()
    registry.register(
        "research-collector",
        1,
        CollectorJobPayload,
        lambda payload: payload.stock_code,
    )
    store = DurableJobStore(registry, db, lease_seconds=300, heartbeat_seconds=30)
    claimed_at = datetime.now(timezone.utc)
    trade_day = (claimed_at + timedelta(hours=8)).date() - timedelta(days=1)
    provider = FakeProvider(
        {
            "daily": pd.DataFrame(
                [{"trade_date": trade_day.strftime("%Y%m%d"), "close": 100.0}]
            )
        }
    )
    repository = ResearchSnapshotRepository(db)
    try:
        store.enqueue(
            JobEnqueueRequest(
                job_type="research-collector",
                payload={"stock_code": "600519"},
                task_id="bounded-cache-source-job",
            ),
            now=claimed_at,
        )
        claim = store.claim_next("bounded-cache-worker", now=claimed_at)
        assert claim is not None
        lease = LeaseFence(claim.task_id, claim.worker_id, claim.lease_token)
        collector = ResearchDatasetCollector(
            provider,
            repository,
            RawArtifactStore(tmp_path / "raw-lru"),
            clock=lambda: claimed_at,
        )
        first = collector.collect_dataset(
            "600519",
            "daily",
            as_of=claimed_at,
            lease=lease,
        )

        oversized = replace(
            first,
            row_count=6000,
            normalized_rows=tuple(
                {"trade_date": trade_day.strftime("%Y%m%d"), "close": index}
                for index in range(6000)
            ),
        )
        collector._remember_terminal_result(
            lease,
            "daily",
            "600519",
            claimed_at,
            oversized,
        )
        assert all(key[0] != claim.task_id for key in _JOB_DATASET_RESULTS)
        provider_calls_before_oversized_recovery = len(provider.calls)
        oversized_recovered = collector.collect_dataset(
            "600519",
            "daily",
            as_of=claimed_at,
            lease=lease,
        )
        assert len(provider.calls) == provider_calls_before_oversized_recovery
        assert oversized_recovered.snapshot is not None
        assert first.snapshot is not None
        assert (
            oversized_recovered.snapshot.content_hash
            == first.snapshot.content_hash
        )

        for index in range(_JOB_DATASET_RESULT_CACHE_MAX_ENTRIES):
            collector._remember_terminal_result(
                LeaseFence(
                    job_id=f"cache-pressure-{index}",
                    worker_id="cache-worker",
                    lease_token="cache-token",
                ),
                "daily",
                f"{index:06d}",
                claimed_at + timedelta(microseconds=index + 1),
                first,
            )

        assert len(_JOB_DATASET_RESULTS) == _JOB_DATASET_RESULT_CACHE_MAX_ENTRIES
        assert all(key[0] != claim.task_id for key in _JOB_DATASET_RESULTS)
        provider_calls = len(provider.calls)

        recovered = collector.collect_dataset(
            "600519",
            "daily",
            as_of=claimed_at,
            lease=lease,
        )

        assert len(_JOB_DATASET_RESULTS) <= _JOB_DATASET_RESULT_CACHE_MAX_ENTRIES
        assert len(provider.calls) == provider_calls
        assert recovered.reused is True
        assert recovered.snapshot is not None
        assert first.snapshot is not None
        assert recovered.snapshot.content_hash == first.snapshot.content_hash
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def test_cyq_chunk_cache_deduplicates_hashes_and_bounds_history_window(
    tmp_path: Path,
) -> None:
    collector, _provider, _repository = _build_collector(tmp_path)
    definition = DATASET_DEFINITIONS["cyq_chips"]
    unique_count = max(
        _CYQ_RESULT_CHUNKS_MAX_PER_KEY + 17,
        (definition.history_days or 300) + 17,
    )
    first_day = date(2020, 1, 1)

    for index in range(unique_count):
        result = _cached_cyq_result(index, first_day + timedelta(days=index))
        collector._remember_incremental_result(definition, "600519", result)
        collector._remember_incremental_result(definition, "600519", result)

    chunks = _CYQ_RESULT_CHUNKS[("cyq_chips", "600519")]
    hashes = [item.snapshot.content_hash for item in chunks if item.snapshot]
    latest_day = first_day + timedelta(days=unique_count - 1)
    history_start = latest_day - timedelta(days=(definition.history_days or 300) - 1)

    assert len(chunks) == _CYQ_RESULT_CHUNKS_MAX_PER_KEY
    assert len(hashes) == len(set(hashes))
    assert all(item.trade_date is not None for item in chunks)
    assert all(history_start <= item.trade_date <= latest_day for item in chunks)


def test_cyq_cache_uses_one_total_lru_bound_for_many_stocks(tmp_path: Path) -> None:
    collector, _provider, _repository = _build_collector(tmp_path)
    definition = DATASET_DEFINITIONS["cyq_chips"]
    trade_day = date(2026, 8, 8)

    for index in range(_CYQ_CACHE_MAX_KEYS + 17):
        scope_value = f"{index:06d}"
        result = _cached_cyq_result(index, trade_day)
        collector._remember_incremental_result(definition, scope_value, result)
        collector._record_high_watermark(definition.name, scope_value, trade_day)

    oldest_key = (definition.name, "000000")
    newest_key = (definition.name, f"{_CYQ_CACHE_MAX_KEYS + 16:06d}")
    cache_keys = set(_CYQ_KEY_LRU)

    assert len(cache_keys) == _CYQ_CACHE_MAX_KEYS
    assert len(_CYQ_RESULT_CHUNKS) <= _CYQ_CACHE_MAX_KEYS
    assert len(_CYQ_LATEST_RESULTS) <= _CYQ_CACHE_MAX_KEYS
    assert len(_CYQ_HIGH_WATERMARKS) <= _CYQ_CACHE_MAX_KEYS
    assert set(_CYQ_RESULT_CHUNKS) <= cache_keys
    assert set(_CYQ_LATEST_RESULTS) <= cache_keys
    assert set(_CYQ_HIGH_WATERMARKS) <= cache_keys
    assert oldest_key not in cache_keys
    assert newest_key in cache_keys


def test_result_caches_reject_single_oversized_payloads(tmp_path: Path) -> None:
    collector, _provider, _repository = _build_collector(tmp_path)
    definition = DATASET_DEFINITIONS["cyq_chips"]
    cheap = _cached_cyq_result(1, date(2026, 8, 8))
    collector._remember_terminal_result(
        LeaseFence("large-job", "worker", "token"),
        "cyq_chips",
        "600519",
        AS_OF,
        cheap,
    )
    assert len(_JOB_DATASET_RESULTS) == 1

    too_many_rows = tuple(
        {"trade_date": "20260808", "price": index}
        for index in range(6000)
    )
    oversized_rows = replace(
        cheap,
        row_count=len(too_many_rows),
        normalized_rows=too_many_rows,
    )
    collector._remember_terminal_result(
        LeaseFence("large-job", "worker", "token"),
        "cyq_chips",
        "600519",
        AS_OF,
        oversized_rows,
    )
    assert all(key[0] != "large-job" for key in _JOB_DATASET_RESULTS)

    huge_text = "x" * collector_module._RESULT_CACHE_SINGLE_MAX_ESTIMATED_BYTES
    oversized_bytes = replace(
        cheap,
        normalized_rows=({"payload": huge_text},),
    )
    collector._remember_incremental_result(
        definition,
        "600519",
        oversized_bytes,
    )
    assert (definition.name, "600519") not in _CYQ_RESULT_CHUNKS
    assert (definition.name, "600519") not in _CYQ_LATEST_RESULTS


def test_result_cache_total_row_and_byte_budgets_remain_bounded(
    tmp_path: Path,
) -> None:
    collector, _provider, _repository = _build_collector(tmp_path)
    definition = DATASET_DEFINITIONS["cyq_chips"]
    trade_day = date(2026, 8, 8)
    rows = tuple(
        {"trade_date": "20260808", "price": index, "label": "bounded"}
        for index in range(400)
    )

    for index in range(32):
        result = replace(
            _cached_cyq_result(index + 1, trade_day),
            row_count=len(rows),
            normalized_rows=rows,
        )
        collector._remember_terminal_result(
            LeaseFence(f"weighted-job-{index}", "worker", "token"),
            "daily",
            f"{index:06d}",
            AS_OF + timedelta(microseconds=index),
            result,
        )
        collector._remember_incremental_result(
            definition,
            f"{index:06d}",
            result,
        )

    assert (
        collector_module._JOB_DATASET_RESULT_CACHE_TOTAL_ROWS
        <= collector_module._JOB_DATASET_RESULT_CACHE_MAX_ROWS
    )
    assert (
        collector_module._JOB_DATASET_RESULT_CACHE_TOTAL_ESTIMATED_BYTES
        <= collector_module._JOB_DATASET_RESULT_CACHE_MAX_ESTIMATED_BYTES
    )
    assert (
        collector_module._CYQ_CACHE_TOTAL_ROWS
        <= collector_module._CYQ_CACHE_MAX_ROWS
    )
    assert (
        collector_module._CYQ_CACHE_TOTAL_ESTIMATED_BYTES
        <= collector_module._CYQ_CACHE_MAX_ESTIMATED_BYTES
    )
    assert len(_JOB_DATASET_RESULTS) < 32
    assert len(_CYQ_KEY_LRU) < 32


def test_market_row_is_unavailable_before_close_and_available_at_close() -> None:
    definition = DATASET_DEFINITIONS["daily"]
    rows = [{"trade_date": "20260808", "close": 100.0}]

    before = assess_dataset_rows(
        definition,
        rows,
        as_of=datetime(2026, 8, 8, 6, 59, 59, tzinfo=timezone.utc),
    )
    at_close = assess_dataset_rows(
        definition,
        rows,
        as_of=datetime(2026, 8, 8, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert before.status == "empty"
    assert before.dropped_future_rows == 1
    assert at_close.status == "available"
    assert len(at_close.rows) == 1


def test_current_state_rows_require_a_real_observation_time() -> None:
    definition = DATASET_DEFINITIONS["stock_basic"]
    rows = [
        {
            "ts_code": "600519.SH",
            "list_date": "20010827",
            "name": "2026 renamed company",
            "industry": "2026 industry",
        }
    ]

    with pytest.raises(ValueError, match="requires observed_at"):
        assess_dataset_rows(
            definition,
            rows,
            as_of=datetime(2010, 1, 1, tzinfo=timezone.utc),
        )


def test_live_stock_basic_uses_one_observation_for_reference_and_snapshot(
    tmp_path: Path,
) -> None:
    observation_start = AS_OF + timedelta(microseconds=1)
    clock_calls = 0

    def advancing_clock() -> datetime:
        nonlocal clock_calls
        value = observation_start + timedelta(microseconds=clock_calls)
        clock_calls += 1
        return value

    provider = FakeProvider(
        {
            "stock_basic": pd.DataFrame(
                [
                    {
                        "ts_code": "600519.SH",
                        "name": "observed name",
                        "industry": "observed industry",
                    }
                ]
            )
        }
    )
    repository = FakeRepository()
    collector = ResearchDatasetCollector(
        provider,
        repository,
        RawArtifactStore(tmp_path / "raw-reference-boundary"),
        clock=advancing_clock,
    )

    result = collector.collect(
        "600519",
        as_of=AS_OF,
        lease=LEASE,
        datasets=("stock_basic",),
        reference_mode="live",
    )

    snapshot = repository.inputs[0]
    assert clock_calls == 1
    assert result.as_of == observation_start
    assert snapshot.knowledge_as_of == observation_start
    assert snapshot.available_at == observation_start
    assert snapshot.data_as_of == observation_start
    assert snapshot.observed_at == observation_start
    assert repository.references[(LEASE.job_id, "600519")] == observation_start


@pytest.mark.parametrize(
    ("stock_basic_error", "expected_status"),
    (
        (
            TusharePermissionError("stock_basic permission denied"),
            "permission_denied",
        ),
        (
            TushareTransportError(
                "HTTP 400 invalid stock_basic request",
                status_code=400,
            ),
            "fetch_failed",
        ),
    ),
    ids=("permission", "http-400-terminal"),
)
def test_live_reference_survives_reclaim_after_terminal_stock_basic_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stock_basic_error: BaseException,
    expected_status: str,
) -> None:
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager(db_url=f"sqlite:///{(tmp_path / 'reference.db').as_posix()}")
    registry = DurableJobHandlerRegistry()
    registry.register(
        "research-collector",
        1,
        CollectorJobPayload,
        lambda payload: payload.stock_code,
    )
    store = DurableJobStore(registry, db, lease_seconds=90, heartbeat_seconds=15)
    claimed_at = datetime.now(timezone.utc)
    observation = claimed_at + timedelta(seconds=2)
    trade_date = (observation + timedelta(hours=8)).date() - timedelta(days=1)
    provider = FakeProvider(
        {
            "stock_basic": stock_basic_error,
            "daily": pd.DataFrame(
                [
                    {
                        "trade_date": trade_date.strftime("%Y%m%d"),
                        "close": 100.0,
                    }
                ]
            ),
        }
    )
    repository = ResearchSnapshotRepository(db)
    try:
        store.enqueue(
            JobEnqueueRequest(
                job_type="research-collector",
                payload={"stock_code": "600519"},
                task_id="stable-reference-job",
            ),
            now=claimed_at,
        )
        first_claim = store.claim_next("worker-one", now=claimed_at)
        assert first_claim is not None
        first_lease = LeaseFence(
            first_claim.task_id,
            first_claim.worker_id,
            first_claim.lease_token,
        )
        first = ResearchDatasetCollector(
            provider,
            repository,
            RawArtifactStore(tmp_path / "raw-reference"),
            clock=lambda: observation,
        ).collect(
            "600519",
            as_of=claimed_at,
            lease=first_lease,
            datasets=("stock_basic", "daily"),
            reference_mode="live",
        )
        assert first.as_of == observation
        assert first.by_dataset["stock_basic"].status == expected_status
        assert first.by_dataset["daily"].status == "available"
        assert [name for name, _params in provider.calls] == [
            "stock_basic",
            "daily",
        ]

        reclaimed_at = claimed_at + timedelta(seconds=91)
        assert store.recover_expired_leases(now=reclaimed_at)["requeued"] == 1
        second_claim = store.claim_next("worker-two", now=reclaimed_at)
        assert second_claim is not None
        second_lease = LeaseFence(
            second_claim.task_id,
            second_claim.worker_id,
            second_claim.lease_token,
        )
        _reset_collector_state_for_tests()
        second = ResearchDatasetCollector(
            provider,
            repository,
            RawArtifactStore(tmp_path / "raw-reference"),
            clock=lambda: observation + timedelta(minutes=5),
        ).collect(
            "600519",
            as_of=observation + timedelta(minutes=5),
            lease=second_lease,
            datasets=("stock_basic", "daily"),
            reference_mode="live",
        )

        assert second.as_of == observation
        assert second.by_dataset["stock_basic"].reused is True
        assert second.by_dataset["stock_basic"].status == expected_status
        assert (
            second.by_dataset["stock_basic"].snapshot.content_hash
            == first.by_dataset["stock_basic"].snapshot.content_hash
        )
        assert second.by_dataset["daily"].reused is True
        assert [name for name, _params in provider.calls] == [
            "stock_basic",
            "daily",
        ]
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def test_historical_stock_basic_does_not_reuse_later_observed_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager(db_url=f"sqlite:///{(tmp_path / 'historical.db').as_posix()}")
    registry = DurableJobHandlerRegistry()
    registry.register(
        "research-collector",
        1,
        CollectorJobPayload,
        lambda payload: payload.stock_code,
    )
    store = DurableJobStore(registry, db, lease_seconds=90, heartbeat_seconds=15)
    now = datetime.now(timezone.utc)
    historical_boundary = datetime(2010, 1, 1, tzinfo=timezone.utc)
    repository = ResearchSnapshotRepository(db)
    provider = FakeProvider()
    try:
        store.enqueue(
            JobEnqueueRequest(
                job_type="research-collector",
                payload={"stock_code": "600519"},
                task_id="legacy-observation-job",
            ),
            now=now,
        )
        legacy_claim = store.claim_next("legacy-worker", now=now)
        assert legacy_claim is not None
        legacy_lease = LeaseFence(
            legacy_claim.task_id,
            legacy_claim.worker_id,
            legacy_claim.lease_token,
        )
        repository.write_dataset(
            DatasetSnapshotInput(
                dataset="stock_basic",
                scope_type="stock",
                scope_value="600519",
                market="A",
                provider="tushare",
                schema_version="legacy-stock-basic-v0",
                data_as_of=datetime(2001, 8, 27, tzinfo=timezone.utc),
                available_at=datetime(2001, 8, 27, tzinfo=timezone.utc),
                observed_at=now,
                status="available",
                normalized=[
                    {
                        "list_date": "20010827",
                        "name": "2026 renamed company",
                        "industry": "2026 industry",
                    }
                ],
                knowledge_as_of=now,
            ),
            lease=legacy_lease,
        )

        store.enqueue(
            JobEnqueueRequest(
                job_type="research-collector",
                payload={"stock_code": "600519"},
                task_id="historical-replay-job",
            ),
            now=now,
        )
        replay_claim = store.claim_next("replay-worker", now=now)
        assert replay_claim is not None
        replay_lease = LeaseFence(
            replay_claim.task_id,
            replay_claim.worker_id,
            replay_claim.lease_token,
        )
        result = ResearchDatasetCollector(
            provider,
            repository,
            RawArtifactStore(tmp_path / "raw-historical"),
            clock=lambda: now,
        ).collect(
            "600519",
            as_of=historical_boundary,
            lease=replay_lease,
            datasets=("stock_basic",),
            reference_mode="historical",
        )

        assert result.as_of == historical_boundary
        assert result.by_dataset["stock_basic"].status == "empty"
        assert result.by_dataset["stock_basic"].normalized_rows == ()
        assert provider.calls == []
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
