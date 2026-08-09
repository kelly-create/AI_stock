"""PR4 durable debate request, turn, snapshot, and pack-link storage tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
import json

import pytest
from sqlalchemy import select

from src.services.durable_jobs import StaleLeaseError
from src.services.research.debate_service import (
    DebateBuildInput,
    DebateFailure,
    DebateTurnBuildInput,
    build_debate_request,
    build_debate_snapshot,
    build_debate_turn,
)
from src.services.research.evidence_service import hydrate_evidence_snapshot
from src.services.research.repositories import (
    DebateFailureInput,
    LeaseFence,
    ResearchSnapshotRepository,
)
from src.services.research.snapshot_service import (
    build_research_snapshot,
    persist_research_snapshot,
)
from src.storage import (
    JobEventRecord,
    ResearchDebateRequestRecord,
    ResearchDebateSnapshotRecord,
    ResearchDebateTurnRecord,
    ResearchEvidenceSnapshotRecord,
)
from tests.test_research_evidence_storage import (
    NOW,
    _claim,
    _dataset_projection,
    _evidence_input,
    _sources,
)


pytest_plugins = ("tests.test_research_evidence_storage",)
ROUTE_FINGERPRINT = "a" * 64


def _turn_output(stance: str, *, suffix: str = "") -> dict:
    return {
        "stance": stance,
        "summary": f"{stance} evidence-bound summary{suffix}",
        "arguments": [
            {
                "id": f"{stance}-argument-1{suffix}",
                "statement": f"{stance} argument grounded in claim one{suffix}",
                "claim_ids": ["claim-1"],
                "citation_ids": ["citation-1"],
                "confidence": 0.75,
                "limitations": [],
            }
        ],
        "open_questions": [f"{stance} open question{suffix}"],
    }


def _build_graph(repository: ResearchSnapshotRepository, lease: LeaseFence):
    dataset, factor = _sources(repository, lease)
    evidence_result = repository.write_evidence(
        _evidence_input([dataset.content_hash], factor.content_hash),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    evidence_record = repository.get_evidence(evidence_result.content_hash)
    assert evidence_record is not None
    evidence = hydrate_evidence_snapshot(evidence_record)
    request = build_debate_request(
        evidence,
        model_route_fingerprint=ROUTE_FINGERPRINT,
    )
    turns = tuple(
        build_debate_turn(
            DebateTurnBuildInput(
                request=request,
                stance=stance,
                output=_turn_output(stance),
                model_used="bounded-model-v1",
            )
        )
        for stance in ("bull", "bear")
    )
    debate = build_debate_snapshot(
        DebateBuildInput(request=request, turns=turns)
    )
    return dataset, factor, evidence, request, turns, debate


def _write_graph(
    repository: ResearchSnapshotRepository,
    lease: LeaseFence,
    request,
    turns,
    debate,
    *,
    offset: int = 4,
):
    request_result = repository.write_debate_request(
        request.to_repository_input(),
        lease=lease,
        now=NOW + timedelta(seconds=offset),
    )
    turn_results = tuple(
        repository.write_debate_turn(
            turn.to_repository_input(),
            lease=lease,
            now=NOW + timedelta(seconds=offset + index + 1),
        )
        for index, turn in enumerate(turns)
    )
    debate_result = repository.write_debate_snapshot(
        debate.to_repository_input(),
        lease=lease,
        now=NOW + timedelta(seconds=offset + 3),
    )
    return request_result, turn_results, debate_result


def _research_snapshot_with_debate(*, factor, evidence, debate):
    return build_research_snapshot(
        stock_code="600519",
        market="A",
        as_of=NOW,
        available_at=NOW - timedelta(minutes=1),
        context_pack={
            "subject": {"code": "600519", "market": "A"},
            "pack_version": "1.0",
            "blocks": {},
        },
        datasets=_dataset_projection(list(evidence.input_dataset_hashes)),
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
        evidence=evidence.canonical_payload,
        evidence_snapshot_hash=evidence.evidence_hash,
        debate=debate.canonical_payload,
        debate_snapshot_hash=debate.debate_hash,
    )


def test_debate_graph_is_idempotent_and_dedupe_binds_every_consuming_job(
    evidence_db,
) -> None:
    db, store = evidence_db
    first_lease = _claim(store, "debate-job-one", worker_id="worker-one")
    repository = ResearchSnapshotRepository(db)
    dataset, factor, evidence, request, turns, debate = _build_graph(
        repository,
        first_lease,
    )
    first_request, first_turns, first_debate = _write_graph(
        repository,
        first_lease,
        request,
        turns,
        debate,
    )

    second_lease = _claim(store, "debate-job-two", worker_id="worker-two")
    second_dataset, second_factor = _sources(repository, second_lease)
    assert second_dataset.content_hash == dataset.content_hash
    assert second_factor.content_hash == factor.content_hash
    second_evidence = repository.write_evidence(
        _evidence_input([dataset.content_hash], factor.content_hash),
        lease=second_lease,
        now=NOW + timedelta(seconds=19),
    )
    second_request, second_turns, second_debate = _write_graph(
        repository,
        second_lease,
        request,
        turns,
        debate,
        offset=20,
    )

    assert first_request.created is True
    assert all(item.created for item in first_turns)
    assert first_debate.created is True
    assert second_evidence.created is False
    assert second_request.created is False
    assert all(not item.created for item in second_turns)
    assert second_debate.created is False
    assert repository.get_debate_request(request.request_hash)[
        "request_hash"
    ] == request.request_hash
    assert repository.get_debate_turn(turns[0].turn_hash)[
        "turn_hash"
    ] == turns[0].turn_hash
    assert repository.get_debate_snapshot(debate.debate_hash)[
        "debate_hash"
    ] == debate.debate_hash
    resumed_request = repository.get_job_debate_request(
        job_id=second_lease.job_id,
        stock_code="600519",
        evidence_snapshot_hash=evidence.evidence_hash,
        prompt_version=request.prompt_version,
        model_route_fingerprint=ROUTE_FINGERPRINT,
    )
    assert resumed_request is not None
    assert resumed_request["request_hash"] == request.request_hash
    resumed_turns = repository.get_job_debate_turns(
        job_id=second_lease.job_id,
        stock_code="600519",
        evidence_snapshot_hash=evidence.evidence_hash,
        request_hash=request.request_hash,
        prompt_version=request.prompt_version,
        prompt_fingerprints={
            item.stance: item.prompt_fingerprint
            for item in request.turn_requests
        },
        model_route_fingerprint=ROUTE_FINGERPRINT,
    )
    assert set(resumed_turns) == {"bull", "bear"}
    assert repository.list_debate_snapshots(job_id=second_lease.job_id)[
        "items"
    ][0]["debate_hash"] == debate.debate_hash

    with db.get_session() as session:
        events = session.execute(
            select(JobEventRecord).where(
                JobEventRecord.event_type.in_(
                    {
                        "research_debate_request",
                        "research_debate_turn",
                        "research_debate_snapshot",
                    }
                )
            )
        ).scalars().all()
    assert len(events) == 8
    assert {event.job_id for event in events} == {
        first_lease.job_id,
        second_lease.job_id,
    }
    malformed_payload = json.loads(debate.canonical_json)
    malformed_payload["status"] = "partial"
    with pytest.raises(ValueError, match="exactly one turn"):
        repository.write_debate_snapshot(
            replace(
                debate.to_repository_input(),
                status="partial",
                canonical_payload=malformed_payload,
            ),
            lease=second_lease,
            now=NOW + timedelta(seconds=30),
        )


def test_cross_job_debate_lineage_requires_explicit_consumer_bindings(
    evidence_db,
) -> None:
    _, store = evidence_db
    repository = ResearchSnapshotRepository()
    first_lease = _claim(store, "debate-lineage-job-one", worker_id="worker-one")
    dataset, factor, evidence, request, turns, debate = _build_graph(
        repository,
        first_lease,
    )
    _write_graph(repository, first_lease, request, turns, debate)

    second_lease = _claim(
        store,
        "debate-lineage-job-two",
        worker_id="worker-two",
    )
    with pytest.raises(ValueError, match="does not bind the Evidence snapshot"):
        repository.write_debate_request(
            request.to_repository_input(),
            lease=second_lease,
            now=NOW + timedelta(seconds=20),
        )

    rebound_dataset, rebound_factor = _sources(repository, second_lease)
    assert rebound_dataset.content_hash == dataset.content_hash
    assert rebound_factor.content_hash == factor.content_hash
    evidence_rebind = repository.write_evidence(
        _evidence_input([dataset.content_hash], factor.content_hash),
        lease=second_lease,
        now=NOW + timedelta(seconds=21),
    )
    assert evidence_rebind.created is False
    assert evidence_rebind.content_hash == evidence.evidence_hash

    bull_request = request.request_for("bull")
    failure = DebateFailureInput(
        stock_code=request.stock_code,
        market=request.market,
        stance="bull",
        debate_engine_version=request.debate_engine_version,
        output_schema_version=request.output_schema_version,
        prompt_version=request.prompt_version,
        as_of=request.as_of,
        available_at=request.available_at,
        evidence_snapshot_hash=request.evidence_snapshot_hash,
        request_hash=request.request_hash,
        prompt_fingerprint=bull_request.prompt_fingerprint,
        model_route_fingerprint=request.model_route_fingerprint,
        error_code="content_rejected",
    )
    with pytest.raises(ValueError, match="does not bind the Debate request"):
        repository.write_debate_turn(
            turns[0].to_repository_input(),
            lease=second_lease,
            now=NOW + timedelta(seconds=22),
        )
    with pytest.raises(ValueError, match="does not bind the Debate request"):
        repository.write_debate_failure(
            failure,
            lease=second_lease,
            now=NOW + timedelta(seconds=23),
        )
    with pytest.raises(ValueError, match="does not bind the Debate request"):
        repository.write_debate_snapshot(
            debate.to_repository_input(),
            lease=second_lease,
            now=NOW + timedelta(seconds=24),
        )

    request_rebind = repository.write_debate_request(
        request.to_repository_input(),
        lease=second_lease,
        now=NOW + timedelta(seconds=25),
    )
    assert request_rebind.created is False
    for index, turn in enumerate(turns):
        result = repository.write_debate_turn(
            turn.to_repository_input(),
            lease=second_lease,
            now=NOW + timedelta(seconds=26 + index),
        )
        assert result.created is False

    frozen = _research_snapshot_with_debate(
        factor=factor,
        evidence=evidence,
        debate=debate,
    )
    with pytest.raises(ValueError, match="does not bind the Debate snapshot"):
        persist_research_snapshot(
            frozen,
            repository,
            lease=second_lease,
            now=NOW + timedelta(seconds=28),
        )

    debate_rebind = repository.write_debate_snapshot(
        debate.to_repository_input(),
        lease=second_lease,
        now=NOW + timedelta(seconds=29),
    )
    assert debate_rebind.created is False
    research_result = persist_research_snapshot(
        frozen,
        repository,
        lease=second_lease,
        now=NOW + timedelta(seconds=30),
    )
    assert research_result.content_hash == frozen.snapshot_hash
    assert repository.list_evidence(job_id=second_lease.job_id)["items"][0][
        "evidence_hash"
    ] == evidence.evidence_hash
    assert repository.list_debate_snapshots(job_id=second_lease.job_id)["items"][
        0
    ]["debate_hash"] == debate.debate_hash
    assert repository.list_debate_snapshots(
        research_snapshot_hash=research_result.content_hash,
    )["items"][0]["debate_hash"] == debate.debate_hash


def test_copied_debate_snapshot_event_cannot_bypass_current_job_graph(
    evidence_db,
) -> None:
    db, store = evidence_db
    repository = ResearchSnapshotRepository(db)
    source_lease = _claim(store, "debate-copy-source", worker_id="worker-source")
    dataset, factor, evidence, request, turns, debate = _build_graph(
        repository,
        source_lease,
    )
    _write_graph(repository, source_lease, request, turns, debate)

    def copy_snapshot_event(target_job_id: str, *, offset: int) -> None:
        with db.get_session() as session:
            source = session.execute(
                select(JobEventRecord).where(
                    JobEventRecord.job_id == source_lease.job_id,
                    JobEventRecord.event_type == "research_debate_snapshot",
                )
            ).scalar_one()
            session.add(
                JobEventRecord(
                    job_id=target_job_id,
                    event_type=source.event_type,
                    stage=source.stage,
                    payload_json=source.payload_json,
                    created_at=(NOW + timedelta(seconds=offset)).replace(tzinfo=None),
                )
            )
            session.commit()

    copied_only = _claim(store, "debate-copy-only", worker_id="worker-copy")
    copy_snapshot_event(copied_only.job_id, offset=20)
    with pytest.raises(ValueError, match="does not bind the Debate request"):
        repository.list_debate_snapshots(job_id=copied_only.job_id)

    missing_stances = _claim(
        store,
        "debate-copy-no-stances",
        worker_id="worker-no-stances",
    )
    rebound_dataset, rebound_factor = _sources(repository, missing_stances)
    assert rebound_dataset.content_hash == dataset.content_hash
    assert rebound_factor.content_hash == factor.content_hash
    rebound_evidence = repository.write_evidence(
        _evidence_input([dataset.content_hash], factor.content_hash),
        lease=missing_stances,
        now=NOW + timedelta(seconds=21),
    )
    assert rebound_evidence.content_hash == evidence.evidence_hash
    rebound_request = repository.write_debate_request(
        request.to_repository_input(),
        lease=missing_stances,
        now=NOW + timedelta(seconds=22),
    )
    assert rebound_request.content_hash == request.request_hash
    copy_snapshot_event(missing_stances.job_id, offset=23)
    with pytest.raises(ValueError, match="turn checkpoints"):
        repository.list_debate_snapshots(job_id=missing_stances.job_id)


def test_terminal_failure_is_fenced_resumable_and_required_by_snapshot(
    evidence_db,
) -> None:
    _db, store = evidence_db
    lease = _claim(store, "debate-terminal-checkpoint")
    repository = ResearchSnapshotRepository(_db)
    _, _, evidence, request, turns, _debate = _build_graph(repository, lease)
    repository.write_debate_request(
        request.to_repository_input(),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    bull_request = request.request_for("bull")
    failure_input = DebateFailureInput(
        stock_code=request.stock_code,
        market=request.market,
        stance="bull",
        debate_engine_version=request.debate_engine_version,
        output_schema_version=request.output_schema_version,
        prompt_version=request.prompt_version,
        as_of=request.as_of,
        available_at=request.available_at,
        evidence_snapshot_hash=evidence.evidence_hash,
        request_hash=request.request_hash,
        prompt_fingerprint=bull_request.prompt_fingerprint,
        model_route_fingerprint=ROUTE_FINGERPRINT,
        error_code="content_rejected",
    )
    repository.write_debate_failure(
        failure_input,
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    repository.write_debate_failure(
        failure_input,
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )

    failures = repository.get_job_debate_failures(
        job_id=lease.job_id,
        stock_code=request.stock_code,
        evidence_snapshot_hash=evidence.evidence_hash,
        request_hash=request.request_hash,
        prompt_version=request.prompt_version,
        prompt_fingerprints={
            item.stance: item.prompt_fingerprint
            for item in request.turn_requests
        },
        model_route_fingerprint=ROUTE_FINGERPRINT,
    )
    assert failures == {
        "bull": {"stance": "bull", "error_code": "content_rejected"}
    }

    with pytest.raises(ValueError, match="persisted terminal failure"):
        repository.write_debate_turn(
            turns[0].to_repository_input(),
            lease=lease,
            now=NOW + timedelta(seconds=7),
        )

    repository.write_debate_turn(
        turns[1].to_repository_input(),
        lease=lease,
        now=NOW + timedelta(seconds=8),
    )
    bear_request = request.request_for("bear")
    with pytest.raises(ValueError, match="persisted successful turn"):
        repository.write_debate_failure(
            replace(
                failure_input,
                stance="bear",
                prompt_fingerprint=bear_request.prompt_fingerprint,
            ),
            lease=lease,
            now=NOW + timedelta(seconds=9),
        )
    partial = build_debate_snapshot(
        DebateBuildInput(
            request=request,
            turns=(turns[1],),
            failures=(DebateFailure("bull", "content_rejected"),),
        )
    )
    result = repository.write_debate_snapshot(
        partial.to_repository_input(),
        lease=lease,
        now=NOW + timedelta(seconds=10),
    )
    assert result.content_hash == partial.debate_hash

    forged_payload = json.loads(partial.canonical_json)
    forged_payload["failed_stances"][0]["error_code"] = "different_failure"
    with pytest.raises(ValueError, match="failures conflict"):
        repository.write_debate_snapshot(
            replace(
                partial.to_repository_input(),
                canonical_payload=forged_payload,
            ),
            lease=lease,
            now=NOW + timedelta(seconds=11),
        )

    with pytest.raises(ValueError, match="ambiguous"):
        repository.write_debate_failure(
            replace(failure_input, error_code="different_failure"),
            lease=lease,
            now=NOW + timedelta(seconds=12),
        )


def test_debate_failure_error_code_is_safe_at_write_and_replay(evidence_db) -> None:
    db, store = evidence_db
    lease = _claim(store, "debate-safe-failure-code")
    repository = ResearchSnapshotRepository(db)
    _, _, evidence, request, _turns, _debate = _build_graph(repository, lease)
    repository.write_debate_request(
        request.to_repository_input(),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    bull_request = request.request_for("bull")
    failure_input = DebateFailureInput(
        stock_code=request.stock_code,
        market=request.market,
        stance="bull",
        debate_engine_version=request.debate_engine_version,
        output_schema_version=request.output_schema_version,
        prompt_version=request.prompt_version,
        as_of=request.as_of,
        available_at=request.available_at,
        evidence_snapshot_hash=evidence.evidence_hash,
        request_hash=request.request_hash,
        prompt_fingerprint=bull_request.prompt_fingerprint,
        model_route_fingerprint=ROUTE_FINGERPRINT,
        error_code="content_rejected",
    )

    for unsafe_code in (
        "password:supersecret",
        "sk-abcdefghijklmnopqrstuvwxyz123456",
        "ghp_abcdefghijklmnopqrstuvwxyz123456",
        "e" * 65,
    ):
        with pytest.raises(ValueError, match="safe identifier|exceeds"):
            repository.write_debate_failure(
                replace(failure_input, error_code=unsafe_code),
                lease=lease,
                now=NOW + timedelta(seconds=5),
            )
    with db.get_session() as session:
        assert session.execute(
            select(JobEventRecord).where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type == "research_debate_failure",
            )
        ).scalars().all() == []

    repository.write_debate_failure(
        failure_input,
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )
    with db.get_session() as session:
        event = session.execute(
            select(JobEventRecord).where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type == "research_debate_failure",
            )
        ).scalar_one()
        forged_payload = json.loads(event.payload_json)
        forged_payload["error_code"] = "password:supersecret"
        event.payload_json = json.dumps(
            forged_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        session.commit()

    with pytest.raises(ValueError, match="safe identifier"):
        repository.get_job_debate_failures(
            job_id=lease.job_id,
            stock_code=request.stock_code,
            evidence_snapshot_hash=evidence.evidence_hash,
            request_hash=request.request_hash,
            prompt_version=request.prompt_version,
            prompt_fingerprints={
                item.stance: item.prompt_fingerprint
                for item in request.turn_requests
            },
            model_route_fingerprint=ROUTE_FINGERPRINT,
        )


def test_debate_versions_are_safe_at_every_storage_write_boundary(evidence_db) -> None:
    db, store = evidence_db
    lease = _claim(store, "debate-safe-version-writes")
    repository = ResearchSnapshotRepository(db)
    _, _, evidence, request, turns, debate = _build_graph(repository, lease)
    bull_request = request.request_for("bull")
    failure = DebateFailureInput(
        stock_code=request.stock_code,
        market=request.market,
        stance="bull",
        debate_engine_version=request.debate_engine_version,
        output_schema_version=request.output_schema_version,
        prompt_version=request.prompt_version,
        as_of=request.as_of,
        available_at=request.available_at,
        evidence_snapshot_hash=evidence.evidence_hash,
        request_hash=request.request_hash,
        prompt_fingerprint=bull_request.prompt_fingerprint,
        model_route_fingerprint=ROUTE_FINGERPRINT,
        error_code="content_rejected",
    )
    writes = (
        (repository.write_debate_request, request.to_repository_input()),
        (repository.write_debate_turn, turns[0].to_repository_input()),
        (repository.write_debate_failure, failure),
        (repository.write_debate_snapshot, debate.to_repository_input()),
    )

    for writer, value in writes:
        for field_name in (
            "debate_engine_version",
            "output_schema_version",
            "prompt_version",
        ):
            with pytest.raises(ValueError, match="safe identifier"):
                writer(
                    replace(value, **{field_name: "password:supersecret"}),
                    lease=lease,
                    now=NOW + timedelta(seconds=4),
                )

    with db.get_session() as session:
        assert session.execute(
            select(JobEventRecord).where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type.like("research_debate_%"),
            )
        ).scalars().all() == []


def test_debate_version_tampering_fails_closed_on_repository_reads(evidence_db) -> None:
    db, store = evidence_db
    lease = _claim(store, "debate-safe-version-reads")
    repository = ResearchSnapshotRepository(db)
    _, _, _evidence, request, turns, debate = _build_graph(repository, lease)
    _write_graph(repository, lease, request, turns, debate)
    cases = (
        (
            ResearchDebateRequestRecord,
            "debate_engine_version",
            lambda: repository.get_debate_request(request.request_hash),
        ),
        (
            ResearchDebateTurnRecord,
            "output_schema_version",
            lambda: repository.get_debate_turn(turns[0].turn_hash),
        ),
        (
            ResearchDebateSnapshotRecord,
            "prompt_version",
            lambda: repository.list_debate_snapshots(stock_code="600519"),
        ),
    )

    for record_type, field_name, read in cases:
        with db.get_session() as session:
            row = session.execute(select(record_type)).scalars().first()
            assert row is not None
            original = getattr(row, field_name)
            setattr(row, field_name, "password:supersecret")
            session.commit()
        with pytest.raises(ValueError):
            read()
        with db.get_session() as session:
            row = session.execute(select(record_type)).scalars().first()
            assert row is not None
            setattr(row, field_name, original)
            session.commit()


@pytest.mark.parametrize(
    ("event_type", "field_name", "forged_value"),
    (
        ("research_debate_turn", "request_hash", "f" * 64),
        ("research_debate_turn", "prompt_fingerprint", "e" * 64),
        ("research_debate_failure", "request_hash", "f" * 64),
        (
            "research_debate_failure",
            "model_route_fingerprint",
            "forged-route",
        ),
    ),
)
def test_debate_checkpoint_contract_drift_fails_closed_before_resume(
    evidence_db,
    event_type: str,
    field_name: str,
    forged_value: str,
) -> None:
    db, store = evidence_db
    case_id = "turn" if event_type.endswith("turn") else "failure"
    field_id = "route" if field_name == "model_route_fingerprint" else field_name
    lease = _claim(store, f"debate-drift-{case_id}-{field_id}")
    repository = ResearchSnapshotRepository(db)
    _, _, evidence, request, turns, _debate = _build_graph(repository, lease)
    repository.write_debate_request(
        request.to_repository_input(),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    bull_request = request.request_for("bull")
    if event_type == "research_debate_turn":
        repository.write_debate_turn(
            turns[0].to_repository_input(),
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )
    else:
        repository.write_debate_failure(
            DebateFailureInput(
                stock_code=request.stock_code,
                market=request.market,
                stance="bull",
                debate_engine_version=request.debate_engine_version,
                output_schema_version=request.output_schema_version,
                prompt_version=request.prompt_version,
                as_of=request.as_of,
                available_at=request.available_at,
                evidence_snapshot_hash=request.evidence_snapshot_hash,
                request_hash=request.request_hash,
                prompt_fingerprint=bull_request.prompt_fingerprint,
                model_route_fingerprint=request.model_route_fingerprint,
                error_code="content_rejected",
            ),
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )

    with db.get_session() as session:
        event = session.execute(
            select(JobEventRecord).where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type == event_type,
            )
        ).scalar_one()
        payload = json.loads(event.payload_json)
        payload[field_name] = forged_value
        event.payload_json = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        session.commit()

    prompt_fingerprints = {
        item.stance: item.prompt_fingerprint for item in request.turn_requests
    }
    with pytest.raises(ValueError, match="binding conflicts"):
        if event_type == "research_debate_turn":
            repository.get_job_debate_turns(
                job_id=lease.job_id,
                stock_code=request.stock_code,
                evidence_snapshot_hash=evidence.evidence_hash,
                request_hash=request.request_hash,
                prompt_version=request.prompt_version,
                prompt_fingerprints=prompt_fingerprints,
                model_route_fingerprint=request.model_route_fingerprint,
            )
        else:
            repository.get_job_debate_failures(
                job_id=lease.job_id,
                stock_code=request.stock_code,
                evidence_snapshot_hash=evidence.evidence_hash,
                request_hash=request.request_hash,
                prompt_version=request.prompt_version,
                prompt_fingerprints=prompt_fingerprints,
                model_route_fingerprint=request.model_route_fingerprint,
            )


def test_debate_snapshot_requires_current_job_turn_checkpoints(evidence_db) -> None:
    db, store = evidence_db
    lease = _claim(store, "debate-turn-checkpoint-required")
    repository = ResearchSnapshotRepository(db)
    _, _, _evidence, request, _turns, debate = _build_graph(repository, lease)
    repository.write_debate_request(
        request.to_repository_input(),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )

    with pytest.raises(ValueError, match="turns conflict with durable"):
        repository.write_debate_snapshot(
            debate.to_repository_input(),
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )


def test_debate_resume_is_exact_unambiguous_and_stock_isolated(evidence_db) -> None:
    db, store = evidence_db
    lease = _claim(store, "debate-resume-job")
    repository = ResearchSnapshotRepository(db)
    _, _, evidence, request, turns, debate = _build_graph(repository, lease)
    _write_graph(repository, lease, request, turns, debate)
    bull_request = request.request_for("bull")

    assert repository.get_job_debate_turn(
        job_id=lease.job_id,
        stock_code="600519",
        evidence_snapshot_hash=evidence.evidence_hash,
        request_hash=request.request_hash,
        stance="bull",
        prompt_version=request.prompt_version,
        prompt_fingerprint=bull_request.prompt_fingerprint,
        model_route_fingerprint=ROUTE_FINGERPRINT,
    )["turn_hash"] == turns[0].turn_hash
    assert repository.get_job_debate_turn(
        job_id=lease.job_id,
        stock_code="000001",
        evidence_snapshot_hash=evidence.evidence_hash,
        request_hash=request.request_hash,
        stance="bull",
        prompt_version=request.prompt_version,
        prompt_fingerprint=bull_request.prompt_fingerprint,
        model_route_fingerprint=ROUTE_FINGERPRINT,
    ) is None
    with pytest.raises(ValueError, match="selector conflicts"):
        repository.get_job_debate_turn(
            job_id=lease.job_id,
            stock_code="600519",
            evidence_snapshot_hash=evidence.evidence_hash,
            request_hash=request.request_hash,
            stance="bull",
            prompt_version=request.prompt_version,
            prompt_fingerprint="b" * 64,
            model_route_fingerprint=ROUTE_FINGERPRINT,
        )
    conflicting_bull = build_debate_turn(
        DebateTurnBuildInput(
            request=request,
            stance="bull",
            output=_turn_output("bull", suffix="-changed"),
            model_used="bounded-model-v1",
        )
    )
    with pytest.raises(ValueError, match="conflicting Debate turn"):
        repository.write_debate_turn(
            conflicting_bull.to_repository_input(),
            lease=lease,
            now=NOW + timedelta(seconds=19),
        )

    with pytest.raises(ValueError, match="conflicts with the current prompt or route"):
        repository.get_job_debate_request(
            job_id=lease.job_id,
            stock_code="600519",
            evidence_snapshot_hash=evidence.evidence_hash,
            prompt_version=request.prompt_version,
            model_route_fingerprint="b" * 64,
        )
    conflicting_request = build_debate_request(
        evidence,
        model_route_fingerprint="b" * 64,
    )
    with pytest.raises(ValueError, match="conflicting Debate request contract"):
        repository.write_debate_request(
            conflicting_request.to_repository_input(),
            lease=lease,
            now=NOW + timedelta(seconds=20),
        )

    with db.get_session() as session:
        session.add(
            JobEventRecord(
                job_id=lease.job_id,
                event_type="research_debate_turn",
                stage="research_debate",
                payload_json=json.dumps(
                    {
                        "stock_code": "000001",
                        "as_of": NOW.isoformat(),
                        "stance": "bull",
                        "evidence_snapshot_hash": evidence.evidence_hash,
                        "request_hash": request.request_hash,
                        "prompt_version": request.prompt_version,
                        "prompt_fingerprint": bull_request.prompt_fingerprint,
                        "model_route_fingerprint": ROUTE_FINGERPRINT,
                        "turn_hash": "c" * 64,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                created_at=NOW.replace(tzinfo=None),
            )
        )
        session.commit()
    with pytest.raises(ValueError, match="outside the durable job stock scope"):
        repository.get_job_debate_turn(
            job_id=lease.job_id,
            stock_code="600519",
            evidence_snapshot_hash=evidence.evidence_hash,
            request_hash=request.request_hash,
            stance="bull",
            prompt_version=request.prompt_version,
            prompt_fingerprint=bull_request.prompt_fingerprint,
            model_route_fingerprint=ROUTE_FINGERPRINT,
        )

    with db.get_session() as session:
        session.add(
            JobEventRecord(
                job_id=lease.job_id,
                event_type="research_debate_request",
                stage="research_debate",
                payload_json=json.dumps(
                    {
                        "stock_code": "600519",
                        "as_of": NOW.isoformat(),
                        "evidence_snapshot_hash": evidence.evidence_hash,
                        "prompt_version": request.prompt_version,
                        "model_route_fingerprint": ROUTE_FINGERPRINT,
                        "request_hash": "9" * 64,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                created_at=NOW.replace(tzinfo=None),
            )
        )
        session.commit()
    with pytest.raises(ValueError, match="request binding is ambiguous"):
        repository.get_job_debate_request(
            job_id=lease.job_id,
            stock_code="600519",
            evidence_snapshot_hash=evidence.evidence_hash,
            prompt_version=request.prompt_version,
            model_route_fingerprint=ROUTE_FINGERPRINT,
        )

    with db.get_session() as session:
        payload = {
            "stock_code": "600519",
            "as_of": NOW.isoformat(),
            "stance": "bull",
            "evidence_snapshot_hash": evidence.evidence_hash,
            "request_hash": request.request_hash,
            "prompt_version": request.prompt_version,
            "prompt_fingerprint": bull_request.prompt_fingerprint,
            "model_route_fingerprint": ROUTE_FINGERPRINT,
            "turn_hash": "d" * 64,
        }
        session.add(
            JobEventRecord(
                job_id=lease.job_id,
                event_type="research_debate_turn",
                stage="research_debate",
                payload_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
                created_at=NOW.replace(tzinfo=None),
            )
        )
        session.commit()
    with pytest.raises(ValueError, match="conflicting|ambiguous"):
        repository.get_job_debate_turn(
            job_id=lease.job_id,
            stock_code="600519",
            evidence_snapshot_hash=evidence.evidence_hash,
            request_hash=request.request_hash,
            stance="bull",
            prompt_version=request.prompt_version,
            prompt_fingerprint=bull_request.prompt_fingerprint,
            model_route_fingerprint=ROUTE_FINGERPRINT,
        )


def test_debate_writes_revalidate_db_evidence_request_and_turn_payloads(
    evidence_db,
) -> None:
    db, store = evidence_db
    lease = _claim(store, "debate-db-revalidation")
    repository = ResearchSnapshotRepository(db)
    _, _, evidence, request, turns, debate = _build_graph(repository, lease)

    with db.get_session() as session:
        evidence_row = session.execute(
            select(ResearchEvidenceSnapshotRecord).where(
                ResearchEvidenceSnapshotRecord.evidence_hash == evidence.evidence_hash
            )
        ).scalar_one()
        original_evidence = evidence_row.canonical_json
        tampered = json.loads(original_evidence)
        tampered["claims"][0]["statement"] = "tampered Evidence statement"
        evidence_row.canonical_json = json.dumps(tampered, sort_keys=True)
        session.commit()
    with pytest.raises(ValueError, match="evidence|Evidence"):
        repository.write_debate_request(
            request.to_repository_input(),
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )
    with db.get_session() as session:
        evidence_row = session.execute(
            select(ResearchEvidenceSnapshotRecord).where(
                ResearchEvidenceSnapshotRecord.evidence_hash == evidence.evidence_hash
            )
        ).scalar_one()
        evidence_row.canonical_json = original_evidence
        session.commit()

    repository.write_debate_request(
        request.to_repository_input(),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    with db.get_session() as session:
        request_row = session.execute(
            select(ResearchDebateRequestRecord).where(
                ResearchDebateRequestRecord.request_hash == request.request_hash
            )
        ).scalar_one()
        original_request = request_row.canonical_json
        tampered = json.loads(original_request)
        tampered["turn_requests"][0]["messages"][1]["content"] += " tampered"
        request_row.canonical_json = json.dumps(tampered, sort_keys=True)
        session.commit()
    with pytest.raises(ValueError, match="request|prompt_fingerprint"):
        repository.write_debate_turn(
            turns[0].to_repository_input(),
            lease=lease,
            now=NOW + timedelta(seconds=6),
        )
    with db.get_session() as session:
        request_row = session.execute(
            select(ResearchDebateRequestRecord).where(
                ResearchDebateRequestRecord.request_hash == request.request_hash
            )
        ).scalar_one()
        request_row.canonical_json = original_request
        session.commit()

    for index, turn in enumerate(turns):
        repository.write_debate_turn(
            turn.to_repository_input(),
            lease=lease,
            now=NOW + timedelta(seconds=7 + index),
        )
    with db.get_session() as session:
        turn_row = session.execute(
            select(ResearchDebateTurnRecord).where(
                ResearchDebateTurnRecord.turn_hash == turns[0].turn_hash
            )
        ).scalar_one()
        original_turn = turn_row.canonical_json
        tampered = json.loads(original_turn)
        tampered["turn"]["arguments"][0]["statement"] = "tampered turn"
        turn_row.canonical_json = json.dumps(tampered, sort_keys=True)
        session.commit()
    with pytest.raises(ValueError, match="turn|turn_hash"):
        repository.write_debate_snapshot(
            debate.to_repository_input(),
            lease=lease,
            now=NOW + timedelta(seconds=10),
        )
    with db.get_session() as session:
        turn_row = session.execute(
            select(ResearchDebateTurnRecord).where(
                ResearchDebateTurnRecord.turn_hash == turns[0].turn_hash
            )
        ).scalar_one()
        turn_row.canonical_json = original_turn
        session.commit()


def test_debate_writes_are_fenced_through_reclaim_cancel_and_dedupe(
    evidence_db,
) -> None:
    _, store = evidence_db
    first_lease = _claim(store, "debate-fenced-job", worker_id="worker-one")
    repository = ResearchSnapshotRepository()
    _, _, _, request, turns, debate = _build_graph(repository, first_lease)
    _write_graph(repository, first_lease, request, turns, debate)

    with pytest.raises(StaleLeaseError, match="stale or cancelled"):
        repository.write_debate_snapshot(
            debate.to_repository_input(),
            lease=replace(first_lease, lease_token="0" * 32),
            now=NOW + timedelta(seconds=20),
        )
    reclaimed_at = NOW + timedelta(seconds=91)
    assert store.recover_expired_leases(now=reclaimed_at)["requeued"] == 1
    claimed = store.claim_next("worker-two", now=reclaimed_at)
    assert claimed is not None
    second_lease = LeaseFence(
        job_id=claimed.task_id,
        worker_id=claimed.worker_id,
        lease_token=claimed.lease_token,
    )
    with pytest.raises(StaleLeaseError, match="stale or cancelled"):
        repository.write_debate_request(
            request.to_repository_input(),
            lease=first_lease,
            now=reclaimed_at + timedelta(seconds=1),
        )
    rebound = repository.write_debate_snapshot(
        debate.to_repository_input(),
        lease=second_lease,
        now=reclaimed_at + timedelta(seconds=2),
    )
    assert rebound.created is False
    store.cancel(second_lease.job_id, now=reclaimed_at + timedelta(seconds=3))
    with pytest.raises(StaleLeaseError, match="stale or cancelled"):
        repository.write_debate_turn(
            turns[0].to_repository_input(),
            lease=second_lease,
            now=reclaimed_at + timedelta(seconds=4),
        )


def test_research_snapshot_requires_full_debate_projection_and_flag_off_v2_is_stable(
    evidence_db,
) -> None:
    db, store = evidence_db
    lease = _claim(store, "debate-research-link")
    repository = ResearchSnapshotRepository(db)
    dataset, factor, evidence, request, turns, debate = _build_graph(
        repository,
        lease,
    )
    _, _, debate_result = _write_graph(
        repository,
        lease,
        request,
        turns,
        debate,
    )
    base_values = {
        "stock_code": "600519",
        "market": "A",
        "as_of": NOW,
        "available_at": NOW - timedelta(minutes=1),
        "context_pack": {
            "subject": {"code": "600519", "market": "A"},
            "pack_version": "1.0",
            "blocks": {},
        },
        "datasets": _dataset_projection([dataset.content_hash]),
        "factors": {"quality": {"status": "available", "score": 80}},
        "prompt_version": "prompt-v1",
        "prompt": {"system": "frozen prompt"},
        "model_route": {
            "backend": "litellm",
            "model": "primary-model",
            "channel": "analysis",
        },
        "policy_version": "policy-v1",
        "policy": {"risk_max": 45},
        "pack_version": "1.0",
        "factor_engine_version": "factor-v1",
        "factor_snapshot_hash": factor.content_hash,
        "evidence": evidence.canonical_payload,
        "evidence_snapshot_hash": evidence.evidence_hash,
    }
    pr3_v2 = build_research_snapshot(**base_values)
    pr3_result = persist_research_snapshot(
        pr3_v2,
        repository,
        lease=lease,
        now=NOW + timedelta(seconds=12),
    )
    assert pr3_result.content_hash == pr3_v2.snapshot_hash
    with db.get_session() as session:
        pr3_event = session.execute(
            select(JobEventRecord)
            .where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type == "research_snapshot",
                JobEventRecord.payload_json.contains(pr3_v2.snapshot_hash),
            )
        ).scalar_one()
    assert "debate_snapshot_hash" not in json.loads(pr3_event.payload_json)

    frozen = build_research_snapshot(
        **base_values,
        debate=debate.canonical_payload,
        debate_snapshot_hash=debate_result.content_hash,
    )
    result = persist_research_snapshot(
        frozen,
        repository,
        lease=lease,
        now=NOW + timedelta(seconds=13),
    )
    assert result.content_hash == frozen.snapshot_hash
    persisted = repository.get_research_snapshot(result.content_hash)
    assert persisted is not None
    assert persisted["debate_snapshot_hash"] == debate.debate_hash
    assert repository.list_debate_snapshots(
        research_snapshot_hash=result.content_hash
    )["items"][0]["debate_hash"] == debate.debate_hash

    malformed = frozen.to_repository_input()
    tampered_payload = json.loads(frozen.canonical_json)
    tampered_payload["debate"]["turns"][0]["arguments"][0][
        "statement"
    ] = "tampered final projection"
    with pytest.raises(ValueError, match="projection conflicts"):
        repository.write_research_snapshot(
            replace(malformed, canonical_payload=tampered_payload),
            lease=lease,
            now=NOW + timedelta(seconds=14),
        )


def test_debate_list_uses_dual_cutoff_and_stable_asof_id_cursor(evidence_db) -> None:
    db, store = evidence_db
    lease = _claim(store, "debate-list-job")
    repository = ResearchSnapshotRepository(db)
    _, _, _, request, turns, debate = _build_graph(repository, lease)
    _write_graph(repository, lease, request, turns, debate)
    second = build_debate_snapshot(
        DebateBuildInput(
            request=request,
            turns=turns,
            limitations=("second immutable projection",),
        )
    )
    second_result = repository.write_debate_snapshot(
        second.to_repository_input(),
        lease=lease,
        now=NOW + timedelta(seconds=20),
    )
    with db.get_session() as session:
        template = session.execute(
            select(ResearchDebateSnapshotRecord).where(
                ResearchDebateSnapshotRecord.debate_hash == debate.debate_hash
            )
        ).scalar_one()
        for digest, future_as_of, future_available in (
            ("e" * 64, True, False),
            ("f" * 64, False, True),
        ):
            session.add(
                ResearchDebateSnapshotRecord(
                    stock_code=template.stock_code,
                    market=template.market,
                    debate_engine_version=template.debate_engine_version,
                    output_schema_version=template.output_schema_version,
                    prompt_version=template.prompt_version,
                    evidence_snapshot_hash=template.evidence_snapshot_hash,
                    request_hash=template.request_hash,
                    model_route_fingerprint=template.model_route_fingerprint,
                    as_of=(
                        NOW + timedelta(hours=1)
                        if future_as_of
                        else NOW - timedelta(minutes=1)
                    ).replace(tzinfo=None),
                    available_at=(
                        NOW + timedelta(hours=1)
                        if future_available
                        else NOW - timedelta(minutes=1)
                    ).replace(tzinfo=None),
                    status=template.status,
                    bull_turn_hash=template.bull_turn_hash,
                    bear_turn_hash=template.bear_turn_hash,
                    bull_argument_count=template.bull_argument_count,
                    bear_argument_count=template.bear_argument_count,
                    open_question_count=template.open_question_count,
                    canonical_json=template.canonical_json,
                    debate_hash=digest,
                    origin_job_id=lease.job_id,
                )
            )
        session.commit()

    page_one = repository.list_debate_snapshots(
        stock_code="600519",
        as_of=NOW,
        limit=1,
    )
    assert page_one["items"][0]["debate_hash"] == second_result.content_hash
    assert page_one["next_cursor"] is not None
    page_two = repository.list_debate_snapshots(
        stock_code="600519",
        as_of=NOW,
        cursor=page_one["next_cursor"],
        limit=1,
    )
    assert page_two["items"][0]["debate_hash"] == debate.debate_hash
    assert page_two["next_cursor"] is None
    visible_hashes = {
        item["debate_hash"]
        for item in repository.list_debate_snapshots(
            stock_code="600519",
            as_of=NOW,
        )["items"]
    }
    assert {"e" * 64, "f" * 64}.isdisjoint(visible_hashes)
    with pytest.raises(ValueError, match="cursor is invalid"):
        repository.list_debate_snapshots(cursor="not-a-valid-cursor")
