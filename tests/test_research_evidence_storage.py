"""PR3 immutable evidence persistence, binding, and query contract tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

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
from src.services.research.canonical import canonical_hash
from src.services.research.repositories import (
    DatasetSnapshotInput,
    EvidenceSnapshotInput,
    FactorSnapshotInput,
    LeaseFence,
    ResearchSnapshotInput,
    ResearchSnapshotRepository,
)
from src.services.research.snapshot_service import (
    build_research_snapshot,
    persist_research_snapshot,
    project_evidence,
)
from src.storage import (
    DatabaseManager,
    JobEventRecord,
    ResearchEvidenceSnapshotRecord,
)


NOW = datetime(2026, 8, 8, 8, 0, 0, tzinfo=timezone.utc)


class ResearchJobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stock_code: str


@pytest.fixture()
def evidence_db(tmp_path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager(db_url=f"sqlite:///{(tmp_path / 'evidence.db').as_posix()}")
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


def _claim(
    store: DurableJobStore,
    task_id: str,
    *,
    worker_id: str = "evidence-worker",
    now: datetime = NOW,
    stock_code: str = "600519",
) -> LeaseFence:
    store.enqueue(
        JobEnqueueRequest(
            job_type="research",
            payload={"stock_code": stock_code},
            task_id=task_id,
            stock_code=stock_code,
        ),
        now=now,
    )
    claimed = store.claim_next(worker_id, now=now)
    assert claimed is not None
    assert claimed.task_id == task_id
    return LeaseFence(
        job_id=claimed.task_id,
        worker_id=claimed.worker_id,
        lease_token=claimed.lease_token,
    )


def _dataset_input(
    *,
    stock_code: str = "600519",
    market: str = "A",
    dataset: str = "daily_basic",
    data_as_of: datetime = NOW - timedelta(hours=2),
    available_at: datetime = NOW - timedelta(hours=1),
    observed_at: datetime = NOW - timedelta(minutes=30),
) -> DatasetSnapshotInput:
    return DatasetSnapshotInput(
        dataset=dataset,
        scope_type="stock",
        scope_value=stock_code,
        market=market,
        provider="tushare",
        schema_version=f"{dataset}-v1",
        data_as_of=data_as_of,
        available_at=available_at,
        observed_at=observed_at,
        knowledge_as_of=max(data_as_of, available_at, observed_at),
        status="available",
        normalized={"stock_code": stock_code, "value": dataset},
    )


def _factor_input(
    dataset_hashes: list[str],
    *,
    stock_code: str = "600519",
    market: str = "A",
    as_of: datetime = NOW - timedelta(minutes=10),
    available_at: datetime = NOW - timedelta(minutes=20),
    version: str = "factor-v1",
) -> FactorSnapshotInput:
    return FactorSnapshotInput(
        stock_code=stock_code,
        market=market,
        company_profile="industrial",
        engine_bundle_version=version,
        factor_payload={"quality": {"status": "available", "score": 80}},
        input_dataset_hashes=dataset_hashes,
        status="available",
        coverage=1.0,
        unknowns=[],
        as_of=as_of,
        available_at=available_at,
        quality_score=80,
    )


def _dataset_projection(dataset_hashes: list[str]) -> dict[str, object]:
    if not dataset_hashes:
        return {}
    hashes = sorted(set(dataset_hashes))
    return {
        "daily_basic": {
            "dataset": "daily_basic",
            "status": "available",
            "row_count": 1,
            "available_at": NOW - timedelta(hours=1),
            "data_as_of": NOW - timedelta(hours=2),
            "content_hash": hashes[-1],
            "content_hashes": hashes,
            "raw_ref": None,
            "rows": [{"stock_code": "600519", "value": "daily_basic"}],
        }
    }


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _evidence_input(
    dataset_hashes: list[str],
    factor_hash: str,
    *,
    market: str = "A",
    as_of: datetime = NOW,
    available_at: datetime = NOW - timedelta(minutes=20),
    engine_version: str = "evidence-v1",
) -> EvidenceSnapshotInput:
    claims = [
        {
            "id": "claim-1",
            "kind": "factor_metric",
            "statement": "quality factor score is 80.",
            "status": "supported",
            "citation_ids": ["citation-1"],
            "limitations": [],
            "available_at": _utc_text(available_at),
        }
    ]
    citations = [
        {
            "id": "citation-1",
            "relation": "supports",
            "artifact_type": "factor",
            "artifact_hash": factor_hash,
            "json_pointer": "/quality/score",
            "value_hash": canonical_hash(80, exclude_volatile=False),
            "available_at": _utc_text(available_at),
            "source_name": "deterministic_factor_engine",
            "title": "quality factor",
            "excerpt": "quality factor score is 80",
            "canonical_url": "https://news.example.com/_/sha256-" + "c" * 64,
        }
    ]
    payload = {
        "evidence_engine_version": engine_version,
        "claim_policy_version": "claim-policy-v1",
        "stock_code": "600519",
        "market": market,
        "as_of": _utc_text(as_of),
        "available_at": _utc_text(available_at),
        "status": "available",
        "coverage": 1.0,
        "input_dataset_hashes": sorted(set(dataset_hashes)),
        "factor_snapshot_hash": factor_hash,
        "limitations": [],
        "claims": claims,
        "citations": citations,
    }
    return EvidenceSnapshotInput(
        stock_code="600519",
        market=market,
        evidence_engine_version=engine_version,
        claim_policy_version="claim-policy-v1",
        as_of=as_of,
        available_at=available_at,
        status="available",
        coverage=1.0,
        claim_count=len(claims),
        citation_count=len(citations),
        canonical_payload=payload,
        input_dataset_hashes=dataset_hashes,
        factor_snapshot_hash=factor_hash,
    )


def _sources(repository: ResearchSnapshotRepository, lease: LeaseFence):
    dataset = repository.write_dataset(
        _dataset_input(),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    factor = repository.write_factors(
        _factor_input([dataset.content_hash]),
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    return dataset, factor


def _research_input(
    factor_hash: str,
    evidence_hash: str | None,
    *,
    stock_code: str = "600519",
    as_of: datetime = NOW,
    evidence_payload: dict | None = None,
    dataset_hashes: list[str] | None = None,
) -> ResearchSnapshotInput:
    canonical_payload = {
        "factor": factor_hash,
        "factors": {"quality": {"status": "available", "score": 80}},
        "datasets": _dataset_projection(dataset_hashes or []),
    }
    if evidence_hash is not None:
        canonical_payload["evidence"] = evidence_payload or {"status": "available"}
    return ResearchSnapshotInput(
        stock_code=stock_code,
        market="A",
        snapshot_version="research-v1",
        field_dictionary_version="fields-v1",
        factor_engine_version="factor-v1",
        pack_version="pack-v1",
        prompt_version="prompt-v1",
        policy_version="policy-v1",
        model_route_fingerprint="route-v1",
        as_of=as_of,
        available_at=as_of - timedelta(seconds=1),
        status="available",
        canonical_payload=canonical_payload,
        factor_snapshot_hash=factor_hash,
        evidence_snapshot_hash=evidence_hash,
    )


def test_evidence_is_hash_idempotent_and_dedupe_binds_each_job_event(
    evidence_db,
) -> None:
    db, store = evidence_db
    first_lease = _claim(store, "evidence-job-one", worker_id="worker-one")
    repository = ResearchSnapshotRepository(db)
    dataset, factor = _sources(repository, first_lease)
    snapshot = _evidence_input([dataset.content_hash], factor.content_hash)

    first = repository.write_evidence(
        snapshot,
        lease=first_lease,
        now=NOW + timedelta(seconds=3),
    )
    repeated = repository.write_evidence(
        snapshot,
        lease=first_lease,
        now=NOW + timedelta(seconds=4),
    )
    second_lease = _claim(store, "evidence-job-two", worker_id="worker-two")
    second_dataset, second_factor = _sources(repository, second_lease)
    assert second_dataset.content_hash == dataset.content_hash
    assert second_factor.content_hash == factor.content_hash
    consumed_again = repository.write_evidence(
        snapshot,
        lease=second_lease,
        now=NOW + timedelta(seconds=5),
    )

    assert first.created is True
    assert repeated.created is False
    assert consumed_again.created is False
    assert first.content_hash == canonical_hash(
        snapshot.canonical_payload,
        exclude_volatile=False,
    )
    assert consumed_again.content_hash == first.content_hash
    assert repository.get_evidence(first.content_hash)["evidence_hash"] == first.content_hash
    assert repository.list_evidence(job_id=second_lease.job_id)["items"][0][
        "evidence_hash"
    ] == first.content_hash
    with db.get_session() as session:
        row = session.get(ResearchEvidenceSnapshotRecord, first.record_id)
        assert row is not None
        assert row.origin_job_id == first_lease.job_id
        events = session.execute(
            select(JobEventRecord).where(
                JobEventRecord.event_type == "research_evidence_snapshot"
            )
        ).scalars().all()
    assert len(events) == 2
    assert {event.job_id for event in events} == {
        first_lease.job_id,
        second_lease.job_id,
    }


def test_factor_and_evidence_require_current_job_upstream_bindings(
    evidence_db,
) -> None:
    db, store = evidence_db
    repository = ResearchSnapshotRepository(db)
    first_lease = _claim(store, "lineage-source-job", worker_id="worker-one")
    dataset, factor = _sources(repository, first_lease)
    evidence_input = _evidence_input([dataset.content_hash], factor.content_hash)
    evidence = repository.write_evidence(
        evidence_input,
        lease=first_lease,
        now=NOW + timedelta(seconds=3),
    )

    second_lease = _claim(store, "lineage-consumer-job", worker_id="worker-two")
    with pytest.raises(ValueError, match="does not bind .*required Dataset"):
        repository.write_factors(
            _factor_input([dataset.content_hash]),
            lease=second_lease,
            now=NOW + timedelta(seconds=4),
        )
    with pytest.raises(ValueError, match="stored datasets"):
        repository.write_factors(
            _factor_input(["d" * 64], version="missing-dataset-factor"),
            lease=second_lease,
            now=NOW + timedelta(seconds=5),
        )

    rebound_dataset = repository.write_dataset(
        _dataset_input(),
        lease=second_lease,
        now=NOW + timedelta(seconds=6),
    )
    assert rebound_dataset.content_hash == dataset.content_hash
    with pytest.raises(ValueError, match="does not bind the Factor"):
        repository.write_evidence(
            evidence_input,
            lease=second_lease,
            now=NOW + timedelta(seconds=7),
        )
    rebound_factor = repository.write_factors(
        _factor_input([dataset.content_hash]),
        lease=second_lease,
        now=NOW + timedelta(seconds=8),
    )
    assert rebound_factor.content_hash == factor.content_hash
    rebound_evidence = repository.write_evidence(
        evidence_input,
        lease=second_lease,
        now=NOW + timedelta(seconds=9),
    )
    assert rebound_evidence.content_hash == evidence.content_hash


def test_research_snapshot_requires_current_job_factor_and_evidence_graph(
    evidence_db,
) -> None:
    db, store = evidence_db
    repository = ResearchSnapshotRepository(db)
    first_lease = _claim(store, "research-graph-source", worker_id="worker-one")
    dataset, factor = _sources(repository, first_lease)
    evidence = repository.write_evidence(
        _evidence_input([dataset.content_hash], factor.content_hash),
        lease=first_lease,
        now=NOW + timedelta(seconds=3),
    )
    evidence_record = repository.get_evidence(evidence.content_hash)
    assert evidence_record is not None
    evidence_projection = project_evidence(
        evidence_record["evidence"],
        as_of=NOW,
    )
    research_input = _research_input(
        factor.content_hash,
        evidence.content_hash,
        evidence_payload=evidence_projection,
        dataset_hashes=[dataset.content_hash],
    )

    second_lease = _claim(store, "research-graph-consumer", worker_id="worker-two")
    rebound_dataset = repository.write_dataset(
        _dataset_input(),
        lease=second_lease,
        now=NOW + timedelta(seconds=3, microseconds=1),
    )
    assert rebound_dataset.content_hash == dataset.content_hash
    with pytest.raises(ValueError, match="does not bind the Factor"):
        repository.write_research_snapshot(
            research_input,
            lease=second_lease,
            now=NOW + timedelta(seconds=4),
        )
    with pytest.raises(ValueError, match="stored factors"):
        repository.write_research_snapshot(
            _research_input("f" * 64, None),
            lease=second_lease,
            now=NOW + timedelta(seconds=5),
        )

    rebound_dataset, rebound_factor = _sources(repository, second_lease)
    assert rebound_dataset.content_hash == dataset.content_hash
    assert rebound_factor.content_hash == factor.content_hash
    with pytest.raises(ValueError, match="does not bind the Evidence"):
        repository.write_research_snapshot(
            research_input,
            lease=second_lease,
            now=NOW + timedelta(seconds=8),
        )
    rebound_evidence = repository.write_evidence(
        _evidence_input([dataset.content_hash], factor.content_hash),
        lease=second_lease,
        now=NOW + timedelta(seconds=9),
    )
    assert rebound_evidence.content_hash == evidence.content_hash
    written = repository.write_research_snapshot(
        research_input,
        lease=second_lease,
        now=NOW + timedelta(seconds=10),
    )
    assert written.content_hash


def test_factor_binding_recursively_requires_dataset_events(evidence_db) -> None:
    db, store = evidence_db
    lease = _claim(store, "factor-without-dataset-event")
    repository = ResearchSnapshotRepository(db)
    dataset, factor = _sources(repository, lease)
    with db.get_session() as session:
        rows = session.execute(
            select(JobEventRecord).where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type == "research_dataset_snapshot",
            )
        ).scalars().all()
        assert rows
        for row in rows:
            session.delete(row)
        session.commit()

    with pytest.raises(ValueError, match="does not bind .*required Dataset"):
        repository.write_evidence(
            _evidence_input([dataset.content_hash], factor.content_hash),
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )
    with pytest.raises(ValueError, match="does not bind .*required Dataset"):
        repository.write_research_snapshot(
            _research_input(
                factor.content_hash,
                None,
                dataset_hashes=[dataset.content_hash],
            ),
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )


def test_dataset_binding_cannot_claim_knowledge_before_snapshot_availability(
    evidence_db,
) -> None:
    db, store = evidence_db
    lease = _claim(store, "dataset-boundary-tamper")
    repository = ResearchSnapshotRepository(db)
    dataset, factor = _sources(repository, lease)
    with db.get_session() as session:
        event = session.execute(
            select(JobEventRecord).where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type == "research_dataset_snapshot",
            )
        ).scalar_one()
        payload = json.loads(event.payload_json)
        payload["knowledge_as_of"] = _utc_text(NOW - timedelta(minutes=90))
        event.payload_json = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        session.commit()

    with pytest.raises(ValueError, match="predates its stored snapshot"):
        repository.write_factors(
            _factor_input(
                [dataset.content_hash],
                version="tampered-boundary-factor",
            ),
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )
    with pytest.raises(ValueError, match="predates its stored snapshot"):
        repository.write_evidence(
            _evidence_input([dataset.content_hash], factor.content_hash),
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )
    with pytest.raises(ValueError, match="predates its stored snapshot"):
        repository.write_research_snapshot(
            _research_input(
                factor.content_hash,
                None,
                dataset_hashes=[dataset.content_hash],
            ),
            lease=lease,
            now=NOW + timedelta(seconds=6),
        )


def test_historical_evidence_accepts_late_observation_but_rejects_current_state(
    evidence_db,
) -> None:
    db, store = evidence_db
    lease = _claim(store, "historical-evidence-boundary")
    repository = ResearchSnapshotRepository(db)
    cutoff = NOW - timedelta(days=30)

    historical_dataset = repository.write_dataset(
        replace(
            _dataset_input(
                dataset="daily_basic",
                data_as_of=cutoff - timedelta(hours=2),
                available_at=cutoff - timedelta(hours=1),
                observed_at=NOW - timedelta(minutes=5),
            ),
            knowledge_as_of=cutoff,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    historical_factor = repository.write_factors(
        _factor_input(
            [historical_dataset.content_hash],
            as_of=cutoff,
            available_at=cutoff - timedelta(minutes=20),
            version="historical-factor",
        ),
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    accepted = repository.write_evidence(
        _evidence_input(
            [historical_dataset.content_hash],
            historical_factor.content_hash,
            as_of=cutoff,
            available_at=cutoff - timedelta(minutes=20),
            engine_version="historical-evidence",
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert accepted.content_hash

    current_state = repository.write_dataset(
        _dataset_input(
            dataset="stock_basic",
            data_as_of=cutoff - timedelta(hours=2),
            available_at=cutoff - timedelta(hours=1),
            observed_at=NOW - timedelta(minutes=4),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    with pytest.raises(
        ValueError,
        match="after consumer as_of|after the consumer boundary",
    ):
        repository.write_factors(
            _factor_input(
                [current_state.content_hash],
                as_of=cutoff,
                available_at=cutoff - timedelta(minutes=20),
                version="current-state-factor",
            ),
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )


def test_evidence_lineage_accepts_pipeline_cn_and_tushare_a_market_alias(
    evidence_db,
) -> None:
    db, store = evidence_db
    lease = _claim(store, "evidence-market-alias")
    repository = ResearchSnapshotRepository(db)
    dataset = repository.write_dataset(
        _dataset_input(market="A"),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    factor = repository.write_factors(
        _factor_input([dataset.content_hash], market="cn"),
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )

    accepted = repository.write_evidence(
        _evidence_input(
            [dataset.content_hash],
            factor.content_hash,
            market="cn",
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )

    assert accepted.content_hash

    with pytest.raises(ValueError, match="different stock or market"):
        repository.write_factors(
            _factor_input(
                [dataset.content_hash],
                market="hk",
                version="foreign-market-factor",
            ),
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )


def test_crash_resume_recovers_exact_job_dataset_binding_without_a_write(
    evidence_db,
) -> None:
    db, store = evidence_db
    first_lease = _claim(store, "news-crash-job", worker_id="worker-one")
    repository = ResearchSnapshotRepository(db)
    news = repository.write_dataset(
        replace(
            _dataset_input(dataset="news_search"),
            knowledge_as_of=NOW,
            normalized={"items": [{"title": "frozen news"}]},
        ),
        lease=first_lease,
        now=NOW + timedelta(seconds=1),
    )
    with db.get_session() as session:
        before_rows = session.scalar(
            select(func.count()).select_from(ResearchEvidenceSnapshotRecord)
        )

    recovered = repository.get_job_dataset(
        job_id=first_lease.job_id,
        dataset="news_search",
        scope_value="600519",
        as_of=NOW,
    )

    assert recovered is not None
    assert recovered["content_hash"] == news.content_hash
    assert recovered["normalized"]["items"][0]["title"] == "frozen news"
    recovered_without_known_completion_time = repository.get_job_dataset(
        job_id=first_lease.job_id,
        dataset="news_search",
        scope_value="600519",
    )
    assert recovered_without_known_completion_time is not None
    assert recovered_without_known_completion_time["content_hash"] == news.content_hash
    assert repository.get_job_dataset(
        job_id="another-job",
        dataset="news_search",
        scope_value="600519",
        as_of=NOW,
    ) is None
    assert repository.get_job_dataset(
        job_id=first_lease.job_id,
        dataset="news_search",
        scope_value="600519",
        as_of=NOW - timedelta(microseconds=1),
    ) is None
    with db.get_session() as session:
        after_rows = session.scalar(
            select(func.count()).select_from(ResearchEvidenceSnapshotRecord)
        )
    assert before_rows == after_rows == 0


def test_job_dataset_recovery_fails_closed_when_completion_binding_is_ambiguous(
    evidence_db,
) -> None:
    db, store = evidence_db
    lease = _claim(store, "ambiguous-news-job")
    repository = ResearchSnapshotRepository(db)
    first = repository.write_dataset(
        replace(
            _dataset_input(dataset="news_search"),
            knowledge_as_of=NOW,
            normalized={"items": [{"title": "first completion"}]},
        ),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    second_boundary = NOW + timedelta(minutes=1)
    second = repository.write_dataset(
        replace(
            _dataset_input(dataset="news_search"),
            knowledge_as_of=second_boundary,
            normalized={"items": [{"title": "second completion"}]},
        ),
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )

    with pytest.raises(ValueError, match="binding is ambiguous"):
        repository.get_job_dataset(
            job_id=lease.job_id,
            dataset="news_search",
            scope_value="600519",
        )
    exact_first = repository.get_job_dataset(
        job_id=lease.job_id,
        dataset="news_search",
        scope_value="600519",
        as_of=NOW,
    )
    exact_second = repository.get_job_dataset(
        job_id=lease.job_id,
        dataset="news_search",
        scope_value="600519",
        as_of=second_boundary,
    )
    assert exact_first is not None
    assert exact_second is not None
    assert exact_first["content_hash"] == first.content_hash
    assert exact_second["content_hash"] == second.content_hash


def test_evidence_source_references_must_exist_match_stock_and_precede_as_of(
    evidence_db,
) -> None:
    db, store = evidence_db
    lease = _claim(store, "evidence-source-validation")
    repository = ResearchSnapshotRepository(db)
    dataset, factor = _sources(repository, lease)
    valid = _evidence_input([dataset.content_hash], factor.content_hash)

    malformed_payload = {
        **valid.canonical_payload,
        "claims": [{"claim_id": "not-the-domain-contract"}],
        "citations": [{"citation_id": "not-the-domain-contract"}],
    }
    with pytest.raises((TypeError, ValueError)):
        repository.write_evidence(
            replace(valid, canonical_payload=malformed_payload),
            lease=lease,
            now=NOW + timedelta(seconds=3),
        )
    with pytest.raises(ValueError, match="stored factors"):
        repository.write_evidence(
            _evidence_input([dataset.content_hash], "f" * 64),
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )
    with pytest.raises(ValueError, match="stored datasets"):
        repository.write_evidence(
            replace(
                valid,
                input_dataset_hashes=["d" * 64],
                canonical_payload={
                    **valid.canonical_payload,
                    "input_dataset_hashes": ["d" * 64],
                },
            ),
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )

    other_lease = _claim(
        store,
        "evidence-other-stock-source",
        worker_id="other-stock-worker",
        stock_code="000001",
    )
    other_dataset = repository.write_dataset(
        _dataset_input(stock_code="000001", dataset="other-stock"),
        lease=other_lease,
        now=NOW + timedelta(seconds=6),
    )
    other_stock = _evidence_input([other_dataset.content_hash], factor.content_hash)
    with pytest.raises(ValueError, match="different stock"):
        repository.write_evidence(
            other_stock,
            lease=lease,
            now=NOW + timedelta(seconds=7),
        )

    future_dataset = repository.write_dataset(
        _dataset_input(
            dataset="future-dataset",
            data_as_of=NOW + timedelta(minutes=30),
            available_at=NOW - timedelta(minutes=1),
            observed_at=NOW + timedelta(minutes=31),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=8),
    )
    future_dataset_evidence = _evidence_input(
        [future_dataset.content_hash],
        factor.content_hash,
    )
    with pytest.raises(ValueError, match="input dataset is after"):
        repository.write_evidence(
            future_dataset_evidence,
            lease=lease,
            now=NOW + timedelta(seconds=9),
        )

    future_factor = repository.write_factors(
        _factor_input(
            [dataset.content_hash],
            as_of=NOW + timedelta(hours=1),
            available_at=NOW - timedelta(minutes=1),
            version="factor-future",
        ),
        lease=lease,
        now=NOW + timedelta(seconds=10),
    )
    future_evidence = _evidence_input([dataset.content_hash], future_factor.content_hash)
    with pytest.raises(
        ValueError,
        match="after consumer as_of|after the consumer boundary",
    ):
        repository.write_evidence(
            future_evidence,
            lease=lease,
            now=NOW + timedelta(seconds=11),
        )


def test_stale_cancelled_and_reclaimed_leases_fence_evidence_dedupe(
    evidence_db,
) -> None:
    db, store = evidence_db
    first_lease = _claim(store, "evidence-fence-job", worker_id="worker-one")
    repository = ResearchSnapshotRepository(db)
    dataset, factor = _sources(repository, first_lease)
    evidence = _evidence_input([dataset.content_hash], factor.content_hash)
    first = repository.write_evidence(
        evidence,
        lease=first_lease,
        now=NOW + timedelta(seconds=3),
    )

    with pytest.raises(StaleLeaseError, match="stale or cancelled"):
        repository.write_evidence(
            evidence,
            lease=replace(first_lease, lease_token="0" * 32),
            now=NOW + timedelta(seconds=4),
        )
    reclaimed_at = NOW + timedelta(seconds=91)
    assert store.recover_expired_leases(now=reclaimed_at)["requeued"] == 1
    claimed_again = store.claim_next("worker-two", now=reclaimed_at)
    assert claimed_again is not None
    second_lease = LeaseFence(
        job_id=claimed_again.task_id,
        worker_id=claimed_again.worker_id,
        lease_token=claimed_again.lease_token,
    )
    with pytest.raises(StaleLeaseError, match="stale or cancelled"):
        repository.write_evidence(
            evidence,
            lease=first_lease,
            now=reclaimed_at + timedelta(seconds=1),
        )
    rebound = repository.write_evidence(
        evidence,
        lease=second_lease,
        now=reclaimed_at + timedelta(seconds=2),
    )
    assert rebound.created is False
    assert rebound.record_id == first.record_id

    store.cancel(second_lease.job_id, now=reclaimed_at + timedelta(seconds=3))
    with pytest.raises(StaleLeaseError, match="stale or cancelled"):
        repository.write_evidence(
            evidence,
            lease=second_lease,
            now=reclaimed_at + timedelta(seconds=4),
        )


def test_research_snapshot_requires_real_compatible_evidence_reference(
    evidence_db,
) -> None:
    db, store = evidence_db
    lease = _claim(store, "research-evidence-reference")
    repository = ResearchSnapshotRepository(db)
    dataset, factor = _sources(repository, lease)
    evidence = repository.write_evidence(
        _evidence_input([dataset.content_hash], factor.content_hash),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    evidence_record = repository.get_evidence(evidence.content_hash)
    assert evidence_record is not None
    evidence_projection = project_evidence(evidence_record["evidence"], as_of=NOW)

    research = repository.write_research_snapshot(
        _research_input(
            factor.content_hash,
            evidence.content_hash,
            evidence_payload=evidence_projection,
            dataset_hashes=[dataset.content_hash],
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    persisted = repository.get_research_snapshot(research.content_hash)
    assert persisted is not None
    assert persisted["evidence_snapshot_hash"] == evidence.content_hash
    assert repository.list_evidence(
        research_snapshot_hash=research.content_hash
    )["items"][0]["evidence_hash"] == evidence.content_hash
    frozen = build_research_snapshot(
        stock_code="600519",
        market="A",
        as_of=NOW,
        available_at=NOW - timedelta(minutes=1),
        context_pack={
            "subject": {"code": "600519", "market": "A"},
            "pack_version": "1.0",
            "blocks": {},
        },
        datasets=_dataset_projection([dataset.content_hash]),
        factors={"quality": {"status": "available", "score": 80}},
        prompt_version="prompt-v1",
        prompt={"system": "frozen prompt"},
        model_route={
            "backend": "litellm",
            "model": "primary-model",
            "channel": "analysis",
        },
        policy_version="policy-v1",
        policy={"risk_max": 45},
        pack_version="1.0",
        factor_engine_version="factor-v1",
        factor_snapshot_hash=factor.content_hash,
        evidence=evidence_record["evidence"],
        evidence_snapshot_hash=evidence.content_hash,
    )
    frozen_result = persist_research_snapshot(
        frozen,
        repository,
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    assert frozen_result.content_hash == frozen.snapshot_hash

    missing_evidence_projection = _research_input(
        factor.content_hash,
        evidence.content_hash,
        dataset_hashes=[dataset.content_hash],
    )
    missing_evidence_payload = dict(missing_evidence_projection.canonical_payload)
    missing_evidence_payload.pop("evidence", None)
    with pytest.raises(ValueError, match="requires a frozen evidence projection"):
        repository.write_research_snapshot(
            replace(
                missing_evidence_projection,
                canonical_payload=missing_evidence_payload,
            ),
            lease=lease,
            now=NOW + timedelta(seconds=6),
        )
    unlinked_evidence = _research_input(
        factor.content_hash,
        None,
        dataset_hashes=[dataset.content_hash],
    )
    unlinked_evidence_payload = dict(unlinked_evidence.canonical_payload)
    unlinked_evidence_payload["evidence"] = evidence_record["evidence"]
    with pytest.raises(ValueError, match="cannot carry evidence"):
        repository.write_research_snapshot(
            replace(
                unlinked_evidence,
                canonical_payload=unlinked_evidence_payload,
            ),
            lease=lease,
            now=NOW + timedelta(seconds=7),
        )
    with pytest.raises(ValueError, match="projection conflicts"):
        repository.write_research_snapshot(
            _research_input(
                factor.content_hash,
                evidence.content_hash,
                evidence_payload={
                    **evidence_record["evidence"],
                    "evidence_engine_version": "different-engine",
                },
                dataset_hashes=[dataset.content_hash],
            ),
            lease=lease,
            now=NOW + timedelta(seconds=8),
        )
    tampered_statement = json.loads(json.dumps(evidence_projection))
    tampered_statement["claims"][0]["statement"] = "tampered claim"
    tampered_excerpt = json.loads(json.dumps(evidence_projection))
    tampered_excerpt["citations"][0]["excerpt"] = "tampered excerpt"
    tampered_value_hash = json.loads(json.dumps(evidence_projection))
    tampered_value_hash["citations"][0]["value_hash"] = "0" * 64
    for index, projection in enumerate(
        (tampered_statement, tampered_excerpt, tampered_value_hash),
        start=9,
    ):
        with pytest.raises(ValueError, match="projection conflicts"):
            repository.write_research_snapshot(
                _research_input(
                    factor.content_hash,
                    evidence.content_hash,
                    evidence_payload=projection,
                    dataset_hashes=[dataset.content_hash],
                ),
                lease=lease,
                now=NOW + timedelta(seconds=index),
            )

    with pytest.raises(ValueError, match="stored evidence"):
        repository.write_research_snapshot(
            _research_input(
                factor.content_hash,
                "e" * 64,
                dataset_hashes=[dataset.content_hash],
            ),
            lease=lease,
            now=NOW + timedelta(seconds=12),
        )
    other_stock_lease = _claim(
        store,
        "research-evidence-other-stock",
        worker_id="other-stock-research-worker",
        stock_code="000001",
    )
    with pytest.raises(ValueError, match="different stock"):
        repository.write_research_snapshot(
            _research_input(
                factor.content_hash,
                evidence.content_hash,
                stock_code="000001",
                evidence_payload=evidence_record["evidence"],
                dataset_hashes=[dataset.content_hash],
            ),
            lease=other_stock_lease,
            now=NOW + timedelta(seconds=13),
        )
    with pytest.raises(
        ValueError,
        match="after consumer as_of|after the consumer boundary",
    ):
        repository.write_research_snapshot(
            _research_input(
                factor.content_hash,
                evidence.content_hash,
                as_of=NOW - timedelta(hours=1),
                evidence_payload=evidence_record["evidence"],
                dataset_hashes=[dataset.content_hash],
            ),
            lease=lease,
            now=NOW + timedelta(seconds=14),
        )


def test_evidence_list_uses_dual_cutoff_and_stable_asof_id_cursor(
    evidence_db,
) -> None:
    db, store = evidence_db
    repository = ResearchSnapshotRepository(db)
    first_lease = _claim(store, "evidence-list-job-one")
    dataset, factor = _sources(repository, first_lease)
    first = repository.write_evidence(
        _evidence_input(
            [dataset.content_hash],
            factor.content_hash,
            as_of=NOW - timedelta(minutes=5),
            engine_version="evidence-page-one",
        ),
        lease=first_lease,
        now=NOW + timedelta(seconds=3),
    )
    second_lease = _claim(store, "evidence-list-job-two")
    dataset, factor = _sources(repository, second_lease)
    second = repository.write_evidence(
        _evidence_input(
            [dataset.content_hash],
            factor.content_hash,
            as_of=NOW - timedelta(minutes=5),
            engine_version="evidence-page-two",
        ),
        lease=second_lease,
        now=NOW + timedelta(seconds=4),
    )
    future_lease = _claim(store, "evidence-list-job-future")
    dataset, factor = _sources(repository, future_lease)
    repository.write_evidence(
        _evidence_input(
            [dataset.content_hash],
            factor.content_hash,
            as_of=NOW + timedelta(hours=1),
            engine_version="future-asof",
        ),
        lease=future_lease,
        now=NOW + timedelta(seconds=5),
    )
    with db.get_session() as session:
        session.add(
            ResearchEvidenceSnapshotRecord(
                stock_code="600519",
                market="A",
                evidence_engine_version="future-availability",
                claim_policy_version="claim-policy-v1",
                as_of=(NOW - timedelta(minutes=10)).replace(tzinfo=None),
                available_at=(NOW + timedelta(minutes=10)).replace(tzinfo=None),
                status="available",
                coverage=1.0,
                claim_count=0,
                citation_count=0,
                canonical_json="{}",
                input_dataset_hashes_json="[]",
                factor_snapshot_hash=factor.content_hash,
                evidence_hash="9" * 64,
                origin_job_id=first_lease.job_id,
            )
        )
        session.commit()

    page_one = repository.list_evidence(
        stock_code="600519",
        as_of=NOW,
        limit=1,
    )
    assert [item["evidence_hash"] for item in page_one["items"]] == [
        second.content_hash
    ]
    assert page_one["next_cursor"] is not None
    page_two = repository.list_evidence(
        stock_code="600519",
        as_of=NOW,
        cursor=page_one["next_cursor"],
        limit=1,
    )
    assert [item["evidence_hash"] for item in page_two["items"]] == [
        first.content_hash
    ]
    assert page_two["next_cursor"] is None
    visible_by_stock = repository.list_evidence(
        stock_code="600519",
        as_of=NOW,
    )
    visible_hashes = {
        item["evidence_hash"] for item in visible_by_stock["items"]
    }
    assert "9" * 64 not in visible_hashes
    assert {first.content_hash, second.content_hash}.issubset(visible_hashes)
    with pytest.raises(ValueError, match="cursor is invalid"):
        repository.list_evidence(cursor="not-a-valid-cursor")


def test_flag_off_frozen_v1_hash_matches_persisted_repository_identity(
    evidence_db,
) -> None:
    db, store = evidence_db
    lease = _claim(store, "evidence-flag-off")
    repository = ResearchSnapshotRepository(db)
    frozen = build_research_snapshot(
        stock_code="600519",
        market="A",
        as_of=NOW,
        available_at=NOW - timedelta(minutes=1),
        context_pack={
            "subject": {"code": "600519", "market": "A"},
            "pack_version": "1.0",
            "blocks": {},
        },
        datasets={},
        factors={},
        prompt_version="prompt-v1",
        prompt={"system": "frozen prompt"},
        model_route={
            "backend": "litellm",
            "model": "primary-model",
            "channel": "analysis",
        },
        policy_version="policy-v1",
        policy={"risk_max": 45},
        pack_version="1.0",
    )

    result = persist_research_snapshot(
        frozen,
        repository,
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )

    assert frozen.evidence_snapshot_hash is None
    assert result.content_hash == frozen.snapshot_hash
    with db.get_session() as session:
        event = session.execute(
            select(JobEventRecord).where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type == "research_snapshot",
            )
        ).scalar_one()
    assert "evidence_snapshot_hash" not in json.loads(event.payload_json)
    assert db._run_write_transaction(
        "count flag-off evidence rows",
        lambda session: session.scalar(
            select(func.count(ResearchEvidenceSnapshotRecord.id))
        ),
    ) == 0


def test_evidence_writer_and_readback_reject_unsafe_public_contracts(
    evidence_db,
) -> None:
    db, store = evidence_db
    lease = _claim(store, "evidence-public-contract")
    repository = ResearchSnapshotRepository(db)
    dataset, factor = _sources(repository, lease)
    evidence_input = _evidence_input(
        [dataset.content_hash],
        factor.content_hash,
    )
    for field_name in ("evidence_engine_version", "claim_policy_version"):
        payload = dict(evidence_input.canonical_payload)
        payload[field_name] = "password:supersecret"
        with pytest.raises(ValueError, match="secret-like|safe identifier"):
            repository.write_evidence(
                replace(
                    evidence_input,
                    canonical_payload=payload,
                    **{field_name: "password:supersecret"},
                ),
                lease=lease,
                now=NOW + timedelta(seconds=3),
            )

    payload = json.loads(json.dumps(evidence_input.canonical_payload))
    payload["claims"][0]["id"] = "sk-abcdefghijklmnopqrstuvwxyz123456"
    with pytest.raises(ValueError, match="secret-like"):
        repository.write_evidence(
            replace(evidence_input, canonical_payload=payload),
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )

    result = repository.write_evidence(
        evidence_input,
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    with db.get_session() as session:
        row = session.get(ResearchEvidenceSnapshotRecord, result.record_id)
        assert row is not None
        tampered = json.loads(row.canonical_json)
        tampered["claims"][0]["statement"] = "tampered claim"
        row.canonical_json = json.dumps(tampered)
        session.commit()

    with pytest.raises(ValueError, match="evidence_hash|canonical_payload"):
        repository.get_evidence(result.content_hash)
    with pytest.raises(ValueError, match="evidence_hash|canonical_payload"):
        repository.list_evidence(stock_code="600519")

    second_lease = _claim(store, "evidence-public-contract-second")
    second_dataset, second_factor = _sources(repository, second_lease)
    assert second_dataset.content_hash == dataset.content_hash
    assert second_factor.content_hash == factor.content_hash
    with pytest.raises(ValueError, match="evidence_hash|canonical_payload"):
        repository.write_evidence(
            evidence_input,
            lease=second_lease,
            now=NOW + timedelta(seconds=6),
        )
    with db.get_session() as session:
        assert session.scalar(
            select(func.count(JobEventRecord.id)).where(
                JobEventRecord.job_id == second_lease.job_id,
                JobEventRecord.event_type == "research_evidence_snapshot",
            )
        ) == 0


def test_job_evidence_list_requires_complete_graph_and_existing_rows(
    evidence_db,
) -> None:
    db, store = evidence_db
    first_lease = _claim(store, "evidence-graph-first")
    repository = ResearchSnapshotRepository(db)
    dataset, factor = _sources(repository, first_lease)
    evidence_input = _evidence_input(
        [dataset.content_hash],
        factor.content_hash,
    )
    evidence = repository.write_evidence(
        evidence_input,
        lease=first_lease,
        now=NOW + timedelta(seconds=3),
    )
    with db.get_session() as session:
        source_event = session.execute(
            select(JobEventRecord).where(
                JobEventRecord.job_id == first_lease.job_id,
                JobEventRecord.event_type == "research_evidence_snapshot",
            )
        ).scalar_one()
        source_payload = source_event.payload_json

    second_lease = _claim(store, "evidence-graph-second")
    with db.get_session() as session:
        session.add(
            JobEventRecord(
                job_id=second_lease.job_id,
                event_type="research_evidence_snapshot",
                stage="research_evidence",
                payload_json=source_payload,
                created_at=NOW.replace(tzinfo=None),
            )
        )
        session.commit()
    with pytest.raises(ValueError, match="Factor snapshot"):
        repository.list_evidence(job_id=second_lease.job_id)

    second_dataset, second_factor = _sources(repository, second_lease)
    assert second_dataset.content_hash == dataset.content_hash
    assert second_factor.content_hash == factor.content_hash
    reused = repository.write_evidence(
        evidence_input,
        lease=second_lease,
        now=NOW + timedelta(seconds=4),
    )
    assert reused.created is False
    assert reused.content_hash == evidence.content_hash
    assert [
        item["evidence_hash"]
        for item in repository.list_evidence(job_id=second_lease.job_id)["items"]
    ] == [evidence.content_hash]

    dangling_lease = _claim(store, "evidence-graph-dangling")
    with db.get_session() as session:
        session.add(
            JobEventRecord(
                job_id=dangling_lease.job_id,
                event_type="research_evidence_snapshot",
                stage="research_evidence",
                payload_json=json.dumps(
                    {
                        "stock_code": "600519",
                        "as_of": _utc_text(NOW),
                        "evidence_hash": "d" * 64,
                        "status": "available",
                    }
                ),
                created_at=NOW.replace(tzinfo=None),
            )
        )
        session.commit()
    with pytest.raises(ValueError, match="missing row"):
        repository.list_evidence(job_id=dangling_lease.job_id)
