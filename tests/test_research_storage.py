"""PR2 immutable research storage and lease-fencing contract tests."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import gzip
import json
import os
import threading
import time

import pandas as pd
import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select

from src.config import Config
from src.services.durable_jobs import (
    DurableJobHandlerRegistry,
    DurableJobStore,
    JobEnqueueRequest,
    StaleLeaseError,
)
from src.services.research import (
    CanonicalJSONError,
    DatasetSnapshotInput,
    DatasetStatus,
    FactorSnapshotInput,
    LeaseFence,
    RawArtifactStore,
    ResearchSnapshotInput,
    ResearchSnapshotRepository,
    canonical_hash,
    canonical_json,
)
from src.storage import (
    DatabaseManager,
    JobEventRecord,
    ResearchDatasetSnapshotRecord,
    ResearchFactorSnapshotRecord,
    ResearchSnapshotRecord,
    StockDaily,
)


NOW = datetime(2026, 8, 8, 8, 0, 0, tzinfo=timezone.utc)


class ResearchJobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stock_code: str


@pytest.fixture()
def research_db(tmp_path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager(db_url=f"sqlite:///{(tmp_path / 'research.db').as_posix()}")
    registry = DurableJobHandlerRegistry()
    registry.register("research", 1, ResearchJobPayload, lambda payload: payload.stock_code)
    store = DurableJobStore(
        registry,
        db,
        lease_seconds=90,
        heartbeat_seconds=15,
    )
    try:
        yield db, store
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def _claim(store: DurableJobStore, task_id: str = "research-job"):
    store.enqueue(
        JobEnqueueRequest(
            job_type="research",
            payload={"stock_code": "600519"},
            task_id=task_id,
            stock_code="600519",
        ),
        now=NOW,
    )
    claimed = store.claim_next("research-worker", now=NOW)
    assert claimed is not None
    assert claimed.task_id == task_id
    return claimed, LeaseFence(
        job_id=claimed.task_id,
        worker_id=claimed.worker_id,
        lease_token=claimed.lease_token,
    )


def test_research_daily_upsert_is_fenced_inside_the_write_transaction(
    research_db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db, store = research_db
    _first_claim, first_lease = _claim(store, task_id="daily-fence-job")
    monkeypatch.setattr(
        "src.storage.utc_naive_now",
        lambda: (NOW + timedelta(seconds=1)).replace(tzinfo=None),
    )

    def _frame(close: float) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "date": NOW.date(),
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume": 1.0,
                    "amount": close,
                }
            ]
        )

    assert db.save_daily_data(
        _frame(100.0),
        "600519",
        "first-attempt",
        lease_fence=first_lease,
    ) == 1

    reclaimed_at = NOW + timedelta(seconds=91)
    assert store.recover_expired_leases(now=reclaimed_at)["requeued"] == 1
    second_claim = store.claim_next("research-worker-two", now=reclaimed_at)
    assert second_claim is not None
    second_lease = LeaseFence(
        job_id=second_claim.task_id,
        worker_id=second_claim.worker_id,
        lease_token=second_claim.lease_token,
    )
    monkeypatch.setattr(
        "src.storage.utc_naive_now",
        lambda: (reclaimed_at + timedelta(seconds=1)).replace(tzinfo=None),
    )

    with pytest.raises(StaleLeaseError, match="daily data write rejected"):
        db.save_daily_data(
            _frame(90.0),
            "600519",
            "stale-attempt",
            lease_fence=first_lease,
        )

    assert db.save_daily_data(
        _frame(110.0),
        "600519",
        "second-attempt",
        lease_fence=second_lease,
    ) == 0
    with db.get_session() as session:
        row = session.execute(
            select(StockDaily).where(
                StockDaily.code == "600519",
                StockDaily.date == NOW.date(),
            )
        ).scalar_one()
        assert row.close == 110.0
        assert row.data_source == "second-attempt"


def _dataset_input(
    *,
    status: str = "available",
    normalized=None,
    dataset: str = "daily_basic",
) -> DatasetSnapshotInput:
    if normalized is None and status not in {
        "permission_denied",
        "not_supported",
        "fetch_failed",
    }:
        normalized = [{"trade_date": "2026-08-07", "pe_ttm": 20.5}]
    return DatasetSnapshotInput(
        dataset=dataset,
        scope_type="stock",
        scope_value="600519",
        market="A",
        provider="tushare",
        schema_version="tushare-daily-basic-v1",
        trade_date=NOW.date(),
        data_as_of=NOW - timedelta(days=1),
        available_at=NOW - timedelta(minutes=2),
        observed_at=NOW - timedelta(minutes=1),
        status=status,
        normalized=normalized,
    )


def _stock_basic_input(
    scope_value: str,
    reference_time: datetime,
) -> DatasetSnapshotInput:
    return DatasetSnapshotInput(
        dataset="stock_basic",
        scope_type="stock",
        scope_value=scope_value,
        market="A",
        provider="tushare",
        schema_version="tushare-stock-basic-v1",
        data_as_of=reference_time,
        available_at=reference_time,
        observed_at=reference_time,
        status="available",
        normalized=[
            {
                "ts_code": f"{scope_value}.SH",
                "name": f"name-{scope_value}",
                "industry": "observed-industry",
            }
        ],
        knowledge_as_of=reference_time,
    )


def test_stock_basic_reference_and_snapshot_binding_are_one_transaction(
    research_db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db, store = research_db
    _claimed, lease = _claim(store, task_id="atomic-reference-job")
    repository = ResearchSnapshotRepository(db)
    reference_time = NOW + timedelta(seconds=1)

    def fail_binding(*_args, **_kwargs):
        raise RuntimeError("binding failed after reference insert")

    monkeypatch.setattr(
        ResearchSnapshotRepository,
        "_bind_dataset_to_job",
        staticmethod(fail_binding),
    )
    with pytest.raises(RuntimeError, match="binding failed"):
        repository.write_dataset(
            _stock_basic_input("600519", reference_time),
            lease=lease,
            establish_reference_for="600519",
        )

    assert repository.get_research_reference_time(
        scope_value="600519",
        lease=lease,
    ) is None
    with db.get_session() as session:
        assert session.scalar(
            select(func.count(ResearchDatasetSnapshotRecord.id))
        ) == 0
        assert session.scalar(
            select(func.count(JobEventRecord.id)).where(
                JobEventRecord.event_type.in_(
                    {"research_reference_time", "research_dataset_snapshot"}
                )
            )
        ) == 0


def test_research_reference_time_is_unique_per_job_and_stock(research_db) -> None:
    db, store = research_db
    _claimed, lease = _claim(store, task_id="multi-stock-reference-job")
    repository = ResearchSnapshotRepository(db)
    first_time = NOW + timedelta(seconds=1)
    second_time = NOW + timedelta(seconds=3)

    repository.write_dataset(
        _stock_basic_input("600519", first_time),
        lease=lease,
        establish_reference_for="600519",
    )
    repository.write_dataset(
        _stock_basic_input("601398", second_time),
        lease=lease,
        establish_reference_for="601398",
    )

    assert repository.get_research_reference_time(
        scope_value="600519",
        lease=lease,
    ) == first_time
    assert repository.get_research_reference_time(
        scope_value="601398",
        lease=lease,
    ) == second_time
    with db.get_session() as session:
        assert session.scalar(
            select(func.count(JobEventRecord.id)).where(
                JobEventRecord.event_type == "research_reference_time"
            )
        ) == 2


def test_canonical_json_is_stable_excludes_execution_metadata_and_rejects_nonfinite() -> None:
    first = {
        "b": 2,
        "a": {"value": 1, "job_id": "job-a", "latency_ms": 12},
        "provider_latency_ms": 14,
        "originJobId": "job-camel",
        "trace_id": "trace-a",
        "created_at": "2026-08-08T00:00:00Z",
    }
    second = {
        "created_at": "later",
        "trace_id": "trace-b",
        "a": {"latency_ms": 999, "job_id": "job-b", "value": 1},
        "b": 2,
    }

    assert canonical_json(first) == '{"a":{"value":1},"b":2}'
    assert canonical_hash(first) == canonical_hash(second)
    with pytest.raises(CanonicalJSONError, match="NaN"):
        canonical_json({"value": float("nan")})
    with pytest.raises(CanonicalJSONError, match="infinite"):
        canonical_json({"value": float("inf")})


@pytest.mark.parametrize("field", ["job_growth", "trace_metal", "created_value"])
def test_canonical_hash_keeps_business_fields_that_contain_metadata_words(
    field: str,
) -> None:
    first = {field: 1, "value": 10}
    second = {field: 2, "value": 10}

    assert field in canonical_json(first)
    assert canonical_hash(first) != canonical_hash(second)


def test_raw_store_is_content_addressed_gzip_and_leaves_no_temp_files(tmp_path) -> None:
    store = RawArtifactStore(tmp_path / "raw")
    first = store.put_bytes(b'{"raw":true}', media_type="application/json", extension="json")
    second = store.put_bytes(b'{"raw":true}', media_type="application/json", extension="json")

    assert first == second
    assert first.compression == "gzip"
    assert store.read(first) == b'{"raw":true}'
    target = (tmp_path / "raw" / first.relative_path)
    assert target.exists()
    assert target.read_bytes()[:2] == b"\x1f\x8b"
    assert not list((tmp_path / "raw").rglob("*.tmp"))


def test_raw_store_concurrent_cross_instance_publish_is_idempotent_on_windows(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "raw"
    stores = [RawArtifactStore(root) for _ in range(12)]
    real_replace = os.replace
    replace_state = {"active": 0, "calls": 0, "collisions": 0}
    replace_state_lock = threading.Lock()

    def windows_sensitive_replace(source, destination) -> None:
        with replace_state_lock:
            replace_state["calls"] += 1
            if replace_state["active"]:
                replace_state["collisions"] += 1
                raise PermissionError("target is open by another publisher")
            replace_state["active"] += 1
        try:
            # Keep the publication window open long enough for every worker to
            # expose the former check-then-replace race deterministically.
            time.sleep(0.01)
            real_replace(source, destination)
        finally:
            with replace_state_lock:
                replace_state["active"] -= 1

    monkeypatch.setattr("src.services.research.raw_store.os.replace", windows_sensitive_replace)

    with ThreadPoolExecutor(max_workers=len(stores)) as executor:
        for round_number in range(25):
            payload = f'{{"round":{round_number},"raw":true}}'.encode()
            start = threading.Barrier(len(stores), timeout=5)

            def publish(store: RawArtifactStore):
                start.wait()
                return store.put_bytes(
                    payload,
                    media_type="application/json",
                    extension="json",
                )

            references = list(executor.map(publish, stores))
            assert all(reference == references[0] for reference in references)
            assert stores[0].read(references[0]) == payload

    assert replace_state == {"active": 0, "calls": 25, "collisions": 0}
    assert not list(root.rglob("*.tmp"))


def test_raw_store_does_not_accept_corrupt_existing_target(tmp_path) -> None:
    store = RawArtifactStore(tmp_path / "raw")
    reference = store.put_bytes(b"original", extension="bin")
    target = store.root / reference.relative_path
    target.write_bytes(gzip.compress(b"tampered", mtime=0))

    with pytest.raises(IOError, match="hash verification failed"):
        store.put_bytes(b"original", extension="bin")

    assert RawArtifactStore.read_path(target) == b"tampered"
    assert not list(store.root.rglob("*.tmp"))


@pytest.mark.parametrize("status", [status.value for status in DatasetStatus])
def test_all_seven_dataset_statuses_are_persisted_without_zero_fallback(
    research_db,
    status: str,
) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)
    if status == "empty":
        payload = []
    elif status in {"permission_denied", "not_supported", "fetch_failed"}:
        payload = None
    else:
        payload = [{"metric": None, "not_applicable": status == "partial"}]

    result = repository.write_dataset(
        _dataset_input(status=status, normalized=payload, dataset=f"dataset-{status}"),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )

    with db.get_session() as session:
        row = session.get(ResearchDatasetSnapshotRecord, result.record_id)
        assert row is not None
        assert row.status == status
        if status == "empty":
            assert row.normalized_json == "[]"
            assert row.normalized_json is not None
        elif payload is None:
            assert row.normalized_json is None
        else:
            assert "null" in row.normalized_json
            assert ":0" not in row.normalized_json


def test_empty_requires_explicit_payload_and_is_not_failure_null(research_db) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)

    with pytest.raises(ValueError, match="requires a normalized payload"):
        repository.write_dataset(
            replace(_dataset_input(), status="empty", normalized=None),
            lease=lease,
            now=NOW + timedelta(seconds=1),
        )


def test_dataset_cannot_claim_data_after_frozen_knowledge_boundary(research_db) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)

    with pytest.raises(ValueError, match="data_as_of cannot be after knowledge_as_of"):
        repository.write_dataset(
            replace(
                _dataset_input(),
                data_as_of=NOW + timedelta(minutes=1),
                knowledge_as_of=NOW,
            ),
            lease=lease,
            now=NOW + timedelta(seconds=1),
        )


def test_hash_idempotency_ignores_job_trace_created_and_latency_fields(research_db) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)
    first_input = _dataset_input(
        normalized={
            "rows": [{"close": 100}],
            "job_id": "first",
            "trace_id": "trace-first",
            "created_at": "first",
            "latency_ms": 1,
        }
    )
    second_input = replace(
        first_input,
        normalized={
            "latency_ms": 999,
            "created_at": "second",
            "trace_id": "trace-second",
            "job_id": "second",
            "rows": [{"close": 100}],
        },
    )

    first = repository.write_dataset(
        first_input, lease=lease, now=NOW + timedelta(seconds=1)
    )
    second = repository.write_dataset(
        second_input, lease=lease, now=NOW + timedelta(seconds=2)
    )

    assert first.created is True
    assert second.created is False
    assert second.record_id == first.record_id
    assert second.content_hash == first.content_hash
    with db.get_session() as session:
        assert session.scalar(select(func.count(ResearchDatasetSnapshotRecord.id))) == 1


def test_dataset_identity_ignores_observation_time(research_db) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)
    first_input = _dataset_input()
    second_input = replace(
        first_input,
        observed_at=first_input.observed_at + timedelta(minutes=5),
    )

    first = repository.write_dataset(
        first_input, lease=lease, now=NOW + timedelta(seconds=1)
    )
    second = repository.write_dataset(
        second_input, lease=lease, now=NOW + timedelta(seconds=2)
    )

    assert first.created is True
    assert second.created is False
    assert second.record_id == first.record_id
    assert second.content_hash == first.content_hash
    with db.get_session() as session:
        row = session.get(ResearchDatasetSnapshotRecord, first.record_id)
        assert row is not None
        assert row.observed_at == first_input.observed_at.replace(tzinfo=None)


def test_failure_identity_ignores_provider_error_wording(research_db) -> None:
    _db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(_db)
    first_input = replace(
        _dataset_input(status="fetch_failed", normalized=None),
        error_code="timeout",
        error_message_sanitized="first provider wording",
    )
    second_input = replace(
        first_input,
        error_message_sanitized="different provider wording",
    )

    first = repository.write_dataset(
        first_input, lease=lease, now=NOW + timedelta(seconds=1)
    )
    second = repository.write_dataset(
        second_input, lease=lease, now=NOW + timedelta(seconds=2)
    )

    assert first.content_hash == second.content_hash
    assert second.created is False


def test_persisted_error_message_redacts_credentials_and_url_secrets(research_db) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)
    snapshot = replace(
        _dataset_input(status="fetch_failed", normalized=None),
        error_code="transport",
        error_message_sanitized=(
            "https://user:url-pass-secret@example.com/v1/PATH-CREDENTIAL-SECRET/resource"
            "?q=query-secret token=token-secret api_key=key-secret "
            "password=hunter2 cookie=session-secret "
            "client_secret=client-secret secret_key=secret-key "
            "urn:PATH-CREDENTIAL-URN file:///C:/PATH-CREDENTIAL-FILE "
            "data:text/plain,PATH-CREDENTIAL-DATA "
            "Authorization: Basic dXNlcjpwYXNz"
        ),
    )

    result = repository.write_dataset(
        snapshot, lease=lease, now=NOW + timedelta(seconds=1)
    )

    with db.get_session() as session:
        row = session.get(ResearchDatasetSnapshotRecord, result.record_id)
        assert row is not None
        message = row.error_message_sanitized or ""
    for secret in (
        "token-secret",
        "key-secret",
        "hunter2",
        "session-secret",
        "client-secret",
        "secret-key",
        "PATH-CREDENTIAL-URN",
        "PATH-CREDENTIAL-FILE",
        "PATH-CREDENTIAL-DATA",
        "dXNlcjpwYXNz",
        "user",
        "url-pass-secret",
        "PATH-CREDENTIAL-SECRET",
        "query-secret",
    ):
        assert secret not in message
    assert "https://example.com/[REDACTED_PATH]" in message


def test_persisted_error_redacts_proxy_authorization_payload(research_db) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)

    result = repository.write_dataset(
        replace(
            _dataset_input(status="fetch_failed", normalized=None),
            error_code="transport",
            error_message_sanitized=str(
                {
                    "headers": {
                        "Authorization": "Basic dXNlcjpwYXNz",
                        "Proxy-Authorization": (
                            "Digest username=alice,response=deadbeef"
                        ),
                    },
                    "payload": {
                        "password": "hunter2",
                        "client_secret": "sekret",
                    },
                }
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )

    with db.get_session() as session:
        row = session.get(ResearchDatasetSnapshotRecord, result.record_id)
        assert row is not None
        message = row.error_message_sanitized or ""
    assert "alice" not in message
    assert "deadbeef" not in message
    assert "dXNlcjpwYXNz" not in message
    assert "hunter2" not in message
    assert "sekret" not in message
    assert "Authorization: [REDACTED]" in message


def test_persisted_error_redacts_plain_digest_authorization(research_db) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)

    result = repository.write_dataset(
        replace(
            _dataset_input(status="fetch_failed", normalized=None),
            error_code="transport",
            error_message_sanitized=(
                "request failed Proxy-Authorization: Digest "
                "username=alice,realm=secret,response=deadbeef"
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )

    with db.get_session() as session:
        row = session.get(ResearchDatasetSnapshotRecord, result.record_id)
        assert row is not None
        message = row.error_message_sanitized or ""
    for secret in ("alice", "realm=secret", "deadbeef"):
        assert secret not in message
    assert message.endswith("Authorization: [REDACTED]")


def test_persisted_error_redacts_plain_multi_cookie_header(research_db) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)

    result = repository.write_dataset(
        replace(
            _dataset_input(status="fetch_failed", normalized=None),
            error_code="transport",
            error_message_sanitized=(
                "request failed Set-Cookie: session=secret; HttpOnly; "
                "csrf=secret2"
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )

    with db.get_session() as session:
        row = session.get(ResearchDatasetSnapshotRecord, result.record_id)
        assert row is not None
        message = row.error_message_sanitized or ""
    for secret in ("session=secret", "csrf=secret2"):
        assert secret not in message
    assert message.endswith("Cookie: [REDACTED]")


def test_stale_expired_and_cancelled_workers_cannot_write_even_existing_hash(research_db) -> None:
    db, store = research_db
    claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)
    snapshot = _dataset_input()
    repository.write_dataset(snapshot, lease=lease, now=NOW + timedelta(seconds=1))

    with pytest.raises(StaleLeaseError, match="stale or cancelled"):
        repository.write_dataset(
            snapshot,
            lease=replace(lease, lease_token="0" * 32),
            now=NOW + timedelta(seconds=2),
        )
    with pytest.raises(StaleLeaseError, match="stale or cancelled"):
        repository.write_dataset(
            snapshot,
            lease=lease,
            now=claimed.lease_expires_at + timedelta(microseconds=1),
        )

    store.cancel(claimed.task_id, now=NOW + timedelta(seconds=3))
    with pytest.raises(StaleLeaseError, match="stale or cancelled"):
        repository.write_dataset(
            snapshot,
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )


def test_factor_and_research_snapshots_are_hash_idempotent_and_version_sensitive(
    research_db,
) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)
    dataset = repository.write_dataset(
        _dataset_input(), lease=lease, now=NOW + timedelta(seconds=1)
    )
    factor_input = FactorSnapshotInput(
        stock_code="600519",
        market="A",
        company_profile="industrial",
        engine_bundle_version="factor-engine-v1",
        factor_payload={"value": {"score": 72}, "unknown": None},
        input_dataset_hashes=[dataset.content_hash],
        status="available",
        coverage=0.8,
        unknowns=["roe_5y"],
        as_of=NOW,
        available_at=NOW - timedelta(minutes=1),
        value_score=72,
        quality_score=80,
        trend_score=68,
        catalyst_score=55,
        risk_penalty=20,
    )
    factor_first = repository.write_factors(
        factor_input, lease=lease, now=NOW + timedelta(seconds=2)
    )
    factor_second = repository.write_factors(
        factor_input, lease=lease, now=NOW + timedelta(seconds=3)
    )
    research_input = ResearchSnapshotInput(
        stock_code="600519",
        market="A",
        snapshot_version="research-snapshot-v1",
        field_dictionary_version="research-fields-v1",
        factor_engine_version="factor-engine-v1",
        pack_version="analysis-context-pack-v1",
        prompt_version="personal-research-v1",
        policy_version="personal-policy-v1",
        model_route_fingerprint="route-hash-v1",
        as_of=NOW,
        available_at=NOW - timedelta(seconds=1),
        status="available",
        canonical_payload={
            "datasets": [dataset.content_hash],
            "factor": factor_first.content_hash,
            "trace_id": "ignored",
        },
        factor_snapshot_hash=factor_first.content_hash,
    )
    research_first = repository.write_research_snapshot(
        research_input, lease=lease, now=NOW + timedelta(seconds=4)
    )
    research_second = repository.write_research_snapshot(
        replace(research_input, canonical_payload={
            "trace_id": "different",
            "factor": factor_first.content_hash,
            "datasets": [dataset.content_hash],
        }),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    changed_prompt = repository.write_research_snapshot(
        replace(research_input, prompt_version="personal-research-v2"),
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )
    changed_route = repository.write_research_snapshot(
        replace(research_input, model_route_fingerprint="route-hash-v2"),
        lease=lease,
        now=NOW + timedelta(seconds=7),
    )

    assert factor_first.created is True
    assert factor_second.created is False
    assert research_first.created is True
    assert research_second.created is False
    assert research_second.content_hash == research_first.content_hash
    assert changed_prompt.content_hash != research_first.content_hash
    assert changed_route.content_hash != research_first.content_hash
    with db.get_session() as session:
        assert session.scalar(select(func.count(ResearchFactorSnapshotRecord.id))) == 1
        assert session.scalar(select(func.count(ResearchSnapshotRecord.id))) == 3
        persisted = session.get(ResearchSnapshotRecord, research_first.record_id)
        assert persisted is not None
        assert "trace_id" not in json.loads(persisted.canonical_json)
        assert persisted.origin_job_id == lease.job_id


def test_factor_and_research_dedupe_bind_each_consuming_job_once(research_db) -> None:
    db, store = research_db
    _first_claim, first_lease = _claim(store, "binding-job-one")
    repository = ResearchSnapshotRepository(db)
    factor_input = FactorSnapshotInput(
        stock_code="600519",
        market="A",
        company_profile="industrial",
        engine_bundle_version="factor-engine-v1",
        factor_payload={"value": {"score": 72}},
        input_dataset_hashes=[],
        status="available",
        coverage=1.0,
        unknowns=[],
        as_of=NOW,
        available_at=NOW - timedelta(minutes=1),
        value_score=72,
    )
    first_factor = repository.write_factors(
        factor_input, lease=first_lease, now=NOW + timedelta(seconds=1)
    )
    research_input = ResearchSnapshotInput(
        stock_code="600519",
        market="A",
        snapshot_version="research-v1",
        field_dictionary_version="fields-v1",
        factor_engine_version="factor-engine-v1",
        pack_version="pack-v1",
        prompt_version="prompt-v1",
        policy_version="policy-v1",
        model_route_fingerprint="route-v1",
        as_of=NOW,
        available_at=NOW - timedelta(seconds=1),
        status="available",
        canonical_payload={"factor": first_factor.content_hash},
        factor_snapshot_hash=first_factor.content_hash,
    )
    first_snapshot = repository.write_research_snapshot(
        research_input, lease=first_lease, now=NOW + timedelta(seconds=2)
    )

    _second_claim, second_lease = _claim(store, "binding-job-two")
    second_factor = repository.write_factors(
        factor_input, lease=second_lease, now=NOW + timedelta(seconds=3)
    )
    second_snapshot = repository.write_research_snapshot(
        research_input, lease=second_lease, now=NOW + timedelta(seconds=4)
    )
    repository.write_factors(
        factor_input, lease=second_lease, now=NOW + timedelta(seconds=5)
    )
    repository.write_research_snapshot(
        research_input, lease=second_lease, now=NOW + timedelta(seconds=6)
    )

    assert second_factor.created is False
    assert second_factor.content_hash == first_factor.content_hash
    assert second_snapshot.created is False
    assert second_snapshot.content_hash == first_snapshot.content_hash
    with db.get_session() as session:
        events = session.execute(
            select(JobEventRecord).where(
                JobEventRecord.event_type.in_(
                    {"research_factor_snapshot", "research_snapshot"}
                )
            )
        ).scalars().all()
    assert len(events) == 4
    assert {(event.job_id, event.event_type) for event in events} == {
        ("binding-job-one", "research_factor_snapshot"),
        ("binding-job-one", "research_snapshot"),
        ("binding-job-two", "research_factor_snapshot"),
        ("binding-job-two", "research_snapshot"),
    }


def test_read_queries_return_detached_decoded_artifacts_when_flags_are_off(
    research_db,
) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)
    dataset = repository.write_dataset(
        _dataset_input(normalized=[{"trade_date": "20260807", "pe_ttm": 20.5}]),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    factors = repository.write_factors(
        FactorSnapshotInput(
            stock_code="600519",
            market="cn",
            company_profile="industrial",
            engine_bundle_version="research-factor-v1",
            factor_payload={
                "value": {"score": 72.0},
                "trend_timing": {
                    "metrics": [
                        {"name": "return_5d", "status": "available", "score": 61.0},
                        {"name": "return_10d", "status": "available", "score": 67.0},
                        {"name": "return_20d", "status": "available", "score": 74.0},
                    ]
                },
            },
            input_dataset_hashes=[dataset.content_hash],
            status="available",
            coverage=1.0,
            unknowns=[],
            as_of=NOW,
            available_at=NOW - timedelta(minutes=1),
            value_score=72.0,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    snapshot = repository.write_research_snapshot(
        ResearchSnapshotInput(
            stock_code="600519",
            market="cn",
            snapshot_version="research-v1",
            field_dictionary_version="fields-v1",
            factor_engine_version="research-factor-v1",
            pack_version="1.0",
            prompt_version="prompt-v1",
            policy_version="policy-v1",
            model_route_fingerprint="a" * 64,
            as_of=NOW,
            available_at=NOW - timedelta(seconds=1),
            status="available",
            canonical_payload={"datasets": [dataset.content_hash]},
            factor_snapshot_hash=factors.content_hash,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    dataset_rows = repository.list_datasets(
        scope_value="600519",
        dataset="daily_basic",
        as_of=NOW,
    )
    factor_row = repository.get_latest_factors(
        stock_code="600519",
        as_of=NOW,
        horizon_days=10,
    )
    factor_row_5d = repository.get_latest_factors(
        stock_code="600519",
        as_of=NOW,
        horizon_days=5,
    )
    factor_row_20d = repository.get_latest_factors(
        stock_code="600519",
        as_of=NOW,
        horizon_days=20,
    )
    snapshot_row = repository.get_research_snapshot(snapshot.content_hash)

    assert dataset_rows[0]["normalized"][0]["pe_ttm"] == 20.5
    assert factor_row is not None
    assert factor_row["factors"]["value"]["score"] == 72.0
    assert factor_row["input_dataset_hashes"] == [dataset.content_hash]
    assert factor_row["primary_horizon"] == 10
    assert factor_row["requested_horizon"] == 10
    assert factor_row["requested_trend"]["name"] == "return_10d"
    assert factor_row_5d["id"] == factor_row["id"]
    assert factor_row_5d["requested_horizon"] == 5
    assert factor_row_5d["requested_trend"]["score"] == 61.0
    assert factor_row_20d["id"] == factor_row["id"]
    assert factor_row_20d["requested_horizon"] == 20
    assert factor_row_20d["requested_trend"]["score"] == 74.0
    assert snapshot_row is not None
    assert snapshot_row["snapshot"]["datasets"] == [dataset.content_hash]
    assert snapshot_row["snapshot_hash"] == snapshot.content_hash


def test_read_cutoffs_apply_to_both_availability_and_data_time(research_db) -> None:
    db, store = research_db
    _claimed, lease = _claim(store)
    repository = ResearchSnapshotRepository(db)
    visible = repository.write_dataset(
        replace(
            _dataset_input(dataset="visible"),
            data_as_of=NOW - timedelta(hours=1),
            available_at=NOW - timedelta(minutes=2),
            knowledge_as_of=NOW,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    repository.write_dataset(
        replace(
            _dataset_input(dataset="future-data"),
            data_as_of=NOW + timedelta(hours=1),
            available_at=NOW - timedelta(minutes=1),
            knowledge_as_of=NOW + timedelta(hours=1),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    repository.write_factors(
        FactorSnapshotInput(
            stock_code="600519",
            market="A",
            company_profile="industrial",
            engine_bundle_version="factor-old",
            factor_payload={"value": {"score": 60}},
            input_dataset_hashes=[visible.content_hash],
            status="available",
            coverage=1.0,
            unknowns=[],
            as_of=NOW - timedelta(hours=1),
            available_at=NOW - timedelta(hours=2),
            value_score=60,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    repository.write_factors(
        FactorSnapshotInput(
            stock_code="600519",
            market="A",
            company_profile="industrial",
            engine_bundle_version="factor-future",
            factor_payload={"value": {"score": 99}},
            input_dataset_hashes=[visible.content_hash],
            status="available",
            coverage=1.0,
            unknowns=[],
            as_of=NOW + timedelta(hours=1),
            available_at=NOW - timedelta(minutes=1),
            value_score=99,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )

    datasets = repository.list_datasets(scope_value="600519", as_of=NOW)
    latest_factors = repository.get_latest_factors(
        stock_code="600519", as_of=NOW
    )

    assert [item["dataset"] for item in datasets] == ["visible"]
    assert latest_factors is not None
    assert latest_factors["value_score"] == 60


def test_historical_dataset_reads_reject_future_stock_basic_observations(
    research_db,
) -> None:
    db, _store = research_db
    cutoff = datetime(2010, 1, 1, tzinfo=timezone.utc)
    old_boundary = datetime(2001, 8, 27, tzinfo=timezone.utc)
    future_observation = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with db.get_session() as session:
        session.add_all(
            [
                ResearchDatasetSnapshotRecord(
                    dataset="stock_basic",
                    scope_type="stock",
                    scope_value="600519",
                    market="A",
                    provider="tushare",
                    schema_version="legacy-stock-basic-v1",
                    data_as_of=old_boundary,
                    available_at=old_boundary,
                    observed_at=future_observation,
                    status="available",
                    normalized_json=json.dumps(
                        [{"name": "2026 renamed", "industry": "2026 industry"}]
                    ),
                    content_hash="d" * 64,
                    created_at=future_observation,
                ),
                ResearchDatasetSnapshotRecord(
                    dataset="income",
                    scope_type="stock",
                    scope_value="600519",
                    market="A",
                    provider="tushare",
                    schema_version="legacy-income-v1",
                    data_as_of=old_boundary,
                    available_at=old_boundary,
                    observed_at=future_observation,
                    status="available",
                    normalized_json=json.dumps([{"revenue": 1.0}]),
                    content_hash="e" * 64,
                    created_at=future_observation,
                ),
            ]
        )
        session.commit()

    repository = ResearchSnapshotRepository(db)
    stock_basic = repository.list_datasets(
        scope_value="600519",
        dataset="stock_basic",
        as_of=cutoff,
    )
    financial = repository.list_datasets(
        scope_value="600519",
        dataset="income",
        as_of=cutoff,
    )

    assert stock_basic == []
    assert financial[0]["normalized"] == [{"revenue": 1.0}]
