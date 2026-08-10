from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import threading

import pytest

from data_provider.tushare_provider import TushareRequestCoordinator
from src.services.research.canonical import canonical_hash
from src.services.research.collector import (
    DatasetCollectionResult,
    ResearchCollectionResult,
)
from src.services.research.factor_policy_v1 import factor_policy_payload
from src.services.research.evidence_collector import EvidenceCollector
from src.services.research.debate_runner import (
    DebateCompletionResult,
    DebateTerminalError,
    DebateTransientError,
)
from src.services.research.repositories import (
    LeaseFence,
    SnapshotWriteResult,
)
from src.services.research.runtime import (
    PreparedResearch,
    ResearchRuntimeContractError,
    ResearchRuntimeService,
)
from src.schemas.analysis_context_pack import ContextFieldStatus
from src.services.analysis_context_builder import (
    AnalysisContextBuilder,
    PipelineAnalysisArtifacts,
)


AS_OF = datetime(2025, 6, 30, 10, 0, tzinfo=timezone.utc)
AVAILABLE_AT = AS_OF - timedelta(hours=1)


class _LeaseLost(RuntimeError):
    pass


class _Cancelled(RuntimeError):
    pass


class _Context:
    def __init__(self, *, store=None) -> None:
        self.job_id = "job-1"
        self.worker_id = "worker-1"
        self.lease_token = "lease-1"
        self.cancel_requested = threading.Event()
        self.lease_lost = threading.Event()
        self.stop_checks = 0
        self.store = store

    def _raise_if_stopped(self) -> None:
        self.stop_checks += 1
        if self.lease_lost.is_set():
            raise _LeaseLost("lease lost")
        if self.cancel_requested.is_set():
            raise _Cancelled("cancelled")


class _Collector:
    def __init__(self, collection, *, hook=None, provider=None) -> None:
        self.collection = collection
        self.hook = hook
        self.provider = provider
        self.calls = []

    def collect(self, stock_code, **kwargs):
        self.calls.append((stock_code, kwargs))
        if self.hook is not None:
            self.hook(kwargs["cancel_event"])
        return self.collection


class _Repository:
    def __init__(self, *, factor_hook=None, research_hook=None) -> None:
        self.factor_hook = factor_hook
        self.research_hook = research_hook
        self.factor_calls = []
        self.research_calls = []
        self._research_hashes = set()
        self.job_research_snapshot = None
        self.lease_checks = []
        self.dataset_calls = []
        self.evidence_calls = []
        self.job_dataset = None
        self.evidence_records = []
        self.debate_request_records = []
        self.debate_turn_records = {}
        self.debate_failure_records = {}
        self.debate_records = []
        self.debate_events = []

    def assert_live_lease(self, lease, *, now=None):
        self.lease_checks.append((lease, now))

    def write_factors(self, snapshot, *, lease):
        self.factor_calls.append((snapshot, lease))
        result = SnapshotWriteResult(
            record_id=10,
            content_hash=_factor_hash(snapshot),
            created=len(self.factor_calls) == 1,
        )
        if self.factor_hook is not None:
            self.factor_hook()
        return result

    def write_research_snapshot(self, snapshot, *, lease, now=None):
        self.research_calls.append((snapshot, lease, now))
        payload = {
            "stock_code": snapshot.stock_code,
            "market": snapshot.market,
            "snapshot_version": snapshot.snapshot_version,
            "field_dictionary_version": snapshot.field_dictionary_version,
            "factor_engine_version": snapshot.factor_engine_version,
            "pack_version": snapshot.pack_version,
            "prompt_version": snapshot.prompt_version,
            "policy_version": snapshot.policy_version,
            "model_route_fingerprint": snapshot.model_route_fingerprint,
            "as_of": _utc_naive(snapshot.as_of),
            "available_at": _utc_naive(snapshot.available_at),
            "status": snapshot.status,
            "canonical_json": snapshot.canonical_payload,
            "factor_snapshot_hash": snapshot.factor_snapshot_hash,
        }
        if snapshot.evidence_snapshot_hash is not None:
            payload["evidence_snapshot_hash"] = snapshot.evidence_snapshot_hash
        if snapshot.debate_snapshot_hash is not None:
            payload["debate_snapshot_hash"] = snapshot.debate_snapshot_hash
        content_hash = canonical_hash(payload)
        self.job_research_snapshot = {
            "stock_code": snapshot.stock_code,
            "market": snapshot.market,
            "as_of": snapshot.as_of,
            "factor_snapshot_hash": snapshot.factor_snapshot_hash,
            "evidence_snapshot_hash": snapshot.evidence_snapshot_hash,
            "debate_snapshot_hash": snapshot.debate_snapshot_hash,
            "snapshot_hash": content_hash,
        }
        created = content_hash not in self._research_hashes
        self._research_hashes.add(content_hash)
        result = SnapshotWriteResult(
            record_id=len(self._research_hashes) + 20,
            content_hash=content_hash,
            created=created,
        )
        if self.research_hook is not None:
            self.research_hook()
        return result

    def get_job_research_snapshot(self, **_kwargs):
        return self.job_research_snapshot

    def write_dataset(self, snapshot, *, lease, now=None):
        self.dataset_calls.append((snapshot, lease, now))
        content_hash = canonical_hash(
            {
                "dataset": snapshot.dataset,
                "scope_value": snapshot.scope_value,
                "market": snapshot.market,
                "provider": snapshot.provider,
                "schema_version": snapshot.schema_version,
                "data_as_of": snapshot.data_as_of,
                "available_at": snapshot.available_at,
                "status": snapshot.status,
                "normalized": snapshot.normalized,
            }
        )
        self.job_dataset = {
            "dataset": snapshot.dataset,
            "scope_type": snapshot.scope_type,
            "scope_value": snapshot.scope_value,
            "market": snapshot.market,
            "provider": snapshot.provider,
            "schema_version": snapshot.schema_version,
            "data_as_of": snapshot.data_as_of,
            "available_at": snapshot.available_at,
            "observed_at": snapshot.observed_at,
            "knowledge_as_of": snapshot.knowledge_as_of,
            "status": snapshot.status,
            "normalized": snapshot.normalized,
            "content_hash": content_hash,
            "raw_ref": snapshot.raw_ref,
            "error_code": snapshot.error_code,
            "error_message_sanitized": snapshot.error_message_sanitized,
        }
        return SnapshotWriteResult(30, content_hash, len(self.dataset_calls) == 1)

    def get_job_dataset(self, **_kwargs):
        return self.job_dataset

    def write_evidence(self, snapshot, *, lease, now=None):
        self.evidence_calls.append((snapshot, lease, now))
        evidence_hash = canonical_hash(
            snapshot.canonical_payload,
            exclude_volatile=False,
        )
        record = {
            "stock_code": snapshot.stock_code,
            "market": snapshot.market,
            "evidence_engine_version": snapshot.evidence_engine_version,
            "claim_policy_version": snapshot.claim_policy_version,
            "as_of": snapshot.as_of,
            "available_at": snapshot.available_at,
            "status": snapshot.status,
            "coverage": snapshot.coverage,
            "claim_count": snapshot.claim_count,
            "citation_count": snapshot.citation_count,
            "evidence": snapshot.canonical_payload,
            "input_dataset_hashes": list(snapshot.input_dataset_hashes),
            "factor_snapshot_hash": snapshot.factor_snapshot_hash,
            "evidence_hash": evidence_hash,
        }
        if not self.evidence_records:
            self.evidence_records.append(record)
        return SnapshotWriteResult(40, evidence_hash, len(self.evidence_calls) == 1)

    def list_evidence(self, **_kwargs):
        return {"items": list(self.evidence_records), "next_cursor": None}

    def write_debate_request(self, snapshot, *, lease, now=None):
        request_hash = canonical_hash(
            snapshot.canonical_payload,
            exclude_volatile=False,
        )
        record = {
            **snapshot.__dict__,
            "debate_request": snapshot.canonical_payload,
            "request_hash": request_hash,
        }
        if not self.debate_request_records:
            self.debate_request_records.append(record)
        self.debate_events.append(("request", request_hash, lease, now))
        return SnapshotWriteResult(
            50,
            request_hash,
            len(self.debate_request_records) == 1,
        )

    def get_job_debate_request(self, **_kwargs):
        return (
            self.debate_request_records[0]
            if self.debate_request_records
            else None
        )

    def get_debate_request(self, request_hash):
        for record in self.debate_request_records:
            if record["request_hash"] == request_hash:
                return record
        return None

    def write_debate_turn(self, snapshot, *, lease, now=None):
        turn_hash = canonical_hash(
            snapshot.canonical_payload,
            exclude_volatile=False,
        )
        record = {
            **snapshot.__dict__,
            "debate_turn": snapshot.canonical_payload,
            "turn_hash": turn_hash,
        }
        self.debate_turn_records.setdefault(snapshot.stance, record)
        self.debate_events.append((snapshot.stance, turn_hash, lease, now))
        return SnapshotWriteResult(
            60 + len(self.debate_turn_records),
            turn_hash,
            True,
        )

    def get_job_debate_turns(self, **_kwargs):
        return dict(self.debate_turn_records)

    def write_debate_failure(self, snapshot, *, lease, now=None):
        record = {
            **snapshot.__dict__,
            "stance": snapshot.stance,
            "error_code": snapshot.error_code,
        }
        self.debate_failure_records.setdefault(snapshot.stance, record)
        self.debate_events.append(
            (f"{snapshot.stance}_failure", snapshot.error_code, lease, now)
        )

    def get_job_debate_failures(self, **_kwargs):
        return dict(self.debate_failure_records)

    def write_debate_snapshot(self, snapshot, *, lease, now=None):
        debate_hash = canonical_hash(
            snapshot.canonical_payload,
            exclude_volatile=False,
        )
        record = {
            **snapshot.__dict__,
            "debate": snapshot.canonical_payload,
            "debate_hash": debate_hash,
        }
        if not self.debate_records:
            self.debate_records.append(record)
        self.debate_events.append(("snapshot", debate_hash, lease, now))
        return SnapshotWriteResult(70, debate_hash, len(self.debate_records) == 1)

    def list_debate_snapshots(self, **_kwargs):
        return {"items": list(self.debate_records), "next_cursor": None}

    def get_debate_snapshot(self, debate_hash):
        for record in self.debate_records:
            if record["debate_hash"] == debate_hash:
                return record
        return None


class _HealthStore:
    def __init__(self, *, fail=False) -> None:
        self.fail = fail
        self.calls = []

    def record_health(self, **kwargs) -> None:
        if self.fail:
            raise RuntimeError("database busy")
        self.calls.append(kwargs)


def _utc_naive(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _factor_hash(snapshot) -> str:
    return canonical_hash(
        {
            "stock_code": snapshot.stock_code,
            "market": snapshot.market,
            "company_profile": snapshot.company_profile,
            "engine_bundle_version": snapshot.engine_bundle_version,
            "factor_payload": snapshot.factor_payload,
            "input_dataset_hashes": list(snapshot.input_dataset_hashes),
            "status": snapshot.status,
            "coverage": snapshot.coverage,
            "unknowns": [dict(item) for item in snapshot.unknowns],
            "as_of": snapshot.as_of,
            "available_at": snapshot.available_at,
            "primary_horizon": snapshot.primary_horizon,
            "value_score": snapshot.value_score,
            "quality_score": snapshot.quality_score,
            "trend_score": snapshot.trend_score,
            "catalyst_score": snapshot.catalyst_score,
            "risk_penalty": snapshot.risk_penalty,
        }
    )


def _config(
    tmp_path,
    *,
    personal=True,
    tushare=True,
    factors=True,
    evidence=False,
    debate=False,
):
    return SimpleNamespace(
        personal_research_enabled=personal,
        tushare_research_enabled=tushare,
        research_factors_enabled=factors,
        research_evidence_enabled=evidence,
        research_debate_enabled=debate,
        database_path=str(tmp_path / "data" / "stock_analysis.db"),
        tushare_token="test-token",
        tushare_global_calls_per_minute=450,
        tushare_max_inflight=2,
        tushare_endpoint_limits={},
    )


def _collection(*, available_at=AVAILABLE_AT, rows_by_dataset=None):
    rows = rows_by_dataset or {
        "stock_basic": [
            {
                "ts_code": "600519.SH",
                "name": "贵州茅台",
                "industry": "白酒",
            }
        ],
        "forecast": [],
        "dividend": [],
        "holder": [],
        "event": [],
    }
    datasets = []
    for index, dataset in enumerate(sorted(rows)):
        normalized = tuple(dict(row) for row in rows[dataset])
        datasets.append(
            DatasetCollectionResult(
                dataset=dataset,
                status="available" if normalized else "empty",
                row_count=len(normalized),
                snapshot=SnapshotWriteResult(
                    record_id=index + 1,
                    content_hash=canonical_hash(
                        {"dataset": dataset, "rows": normalized}
                    ),
                    created=True,
                ),
                query_params={},
                normalized_rows=normalized,
                available_at=available_at,
                data_as_of=available_at,
            )
        )
    return ResearchCollectionResult(
        stock_code="600519",
        ts_code="600519.SH",
        as_of=AS_OF,
        datasets=tuple(datasets),
    )


def _complete_rows():
    daily = []
    adj_factor = []
    start = AS_OF.date() - timedelta(days=299)
    for index in range(300):
        trade_date = (start + timedelta(days=index)).strftime("%Y%m%d")
        close = 100.0 + index * 0.2
        daily.append(
            {
                "ts_code": "600519.SH",
                "trade_date": trade_date,
                "open": close * 0.995,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "vol": 1_000_000.0 + index * 1_000.0,
            }
        )
        adj_factor.append(
            {
                "ts_code": "600519.SH",
                "trade_date": trade_date,
                "adj_factor": 1.0,
            }
        )
    return {
        "stock_basic": [
            {
                "ts_code": "600519.SH",
                "name": "贵州茅台",
                "industry": "白酒",
            }
        ],
        "daily": daily,
        "adj_factor": adj_factor,
        "daily_basic": [
            {
                "ts_code": "600519.SH",
                "trade_date": "20250630",
                "pe_ttm": 18.0,
                "pb": 4.5,
                "ps_ttm": 7.0,
                "dv_ttm": 3.0,
            }
        ],
        "fina_indicator": [
            {
                "ts_code": "600519.SH",
                "end_date": "20250331",
                "ann_date": "20250430",
                "roe_waa": 20.0,
                "grossprofit_margin": 91.0,
                "netprofit_margin": 52.0,
                "tr_yoy": 12.0,
                "netprofit_yoy": 14.0,
                "debt_to_assets": 18.0,
            }
        ],
        "income": [
            {
                "ts_code": "600519.SH",
                "end_date": "20250331",
                "ann_date": "20250430",
                "comp_type": "1",
                "total_revenue": 120.0,
                "oper_cost": 12.0,
                "n_income_attr_p": 62.0,
            }
        ],
        "forecast": [],
        "dividend": [],
        "holder": [],
        "event": [],
        "stk_limit": [],
        "suspend_d": [],
    }


def _analysis_pack(research_context):
    artifacts = PipelineAnalysisArtifacts(
        code="600519",
        stock_name="贵州茅台",
        market="cn",
        phase=None,
        base_context={},
        enhanced_context={},
        realtime_quote=None,
        trend_result=None,
        chip_data=None,
        fundamental_context=None,
        news_context=None,
        news_result_count=0,
        metadata={"query_id": "q-1"},
        research_context=dict(research_context),
    )
    return AnalysisContextBuilder.build(artifacts)


def _route(**overrides):
    route = {
        "backend": "litellm",
        "model": "primary-model",
        "channel": "analysis",
        "base_url": "https://user:secret@llm.example.com/v1?token=hidden",
        "temperature": 0.1,
        "reasoning_effort": "medium",
        "max_tokens": 4096,
        "fallbacks": ["fallback-model"],
    }
    route.update(overrides)
    return route


def _context_pack(**overrides):
    payload = {
        "pack_version": "analysis-context-pack-v1",
        "subject": {"code": "600519", "market": "A"},
        "blocks": {
            "quote": {
                "status": "available",
                "available_at": "2025-06-30T08:30:00Z",
                "close": 1500.0,
            }
        },
    }
    payload.update(overrides)
    return payload


def _service(tmp_path, *, context=None, collection=None, factors=True, repo=None):
    context = context or _Context()
    collection = collection or _collection()
    repository = repo or _Repository()
    collector = _Collector(collection)
    diagnostics = []
    service = ResearchRuntimeService(
        _config(tmp_path, factors=factors),
        collector=collector,
        repository=repository,
        durable_context_getter=lambda: context,
        diagnostic_updater=diagnostics.append,
    )
    return service, context, collector, repository, diagnostics


def _prepare(service):
    prepared = service.prepare("600519", "A", AS_OF)
    assert isinstance(prepared, PreparedResearch)
    return prepared


def _freeze(service, prepared, **overrides):
    values = {
        "context_pack": _context_pack(),
        "prompt_version": "personal-research-prompt-v1",
        "prompt": {"system": "research prompt"},
        "model_route": _route(),
        "policy_version": "factor-policy-v1",
        "policy": factor_policy_payload(),
    }
    values.update(overrides)
    return service.freeze(prepared, **values)


def test_all_flags_off_has_no_provider_repository_or_filesystem_side_effects(
    tmp_path,
):
    calls = []
    database_path = tmp_path / "never-created" / "stock.db"
    config = _config(tmp_path, personal=False, tushare=False, factors=False)
    config.database_path = str(database_path)
    service = ResearchRuntimeService(
        config,
        provider_factory=lambda _config: calls.append("provider"),
        durable_context_getter=lambda: calls.append("context"),
    )

    assert service.prepare("600519", "A", AS_OF) is None
    assert calls == []
    assert not database_path.parent.exists()


def test_tushare_enabled_requires_durable_context_before_constructing_provider(
    tmp_path,
):
    provider_calls = []
    service = ResearchRuntimeService(
        _config(tmp_path, factors=False),
        provider_factory=lambda config: provider_calls.append(config),
        durable_context_getter=lambda: None,
    )

    with pytest.raises(RuntimeError, match="active durable job"):
        service.prepare("600519", "A", AS_OF)
    assert provider_calls == []


def test_prepare_collects_builds_factors_and_persists_with_same_lease(tmp_path):
    service, context, collector, repository, _ = _service(tmp_path)
    prepared = _prepare(service)

    expected_lease = LeaseFence("job-1", "worker-1", "lease-1")
    assert collector.calls[0][0] == "600519"
    assert collector.calls[0][1]["as_of"] == AS_OF
    assert collector.calls[0][1]["lease"] == expected_lease
    stop_signal = collector.calls[0][1]["cancel_event"]
    assert stop_signal.is_set() is False
    context.cancel_requested.set()
    assert stop_signal.is_set() is True
    context.cancel_requested.clear()
    context.lease_lost.set()
    assert stop_signal.is_set() is True
    context.lease_lost.clear()

    assert prepared.lease == expected_lease
    assert prepared.collection is collector.collection
    assert prepared.available_at == AVAILABLE_AT
    assert prepared.as_of == AS_OF
    assert prepared.rows_by_dataset["stock_basic"][0]["name"]
    assert prepared.factor_input["stock_code"] == "600519"
    assert prepared.factors.profile.profile == "industrial"
    assert prepared.factor_snapshot is not None


def test_live_evidence_advances_boundary_persists_once_and_retry_reuses(
    tmp_path,
):
    context = _Context()
    repository = _Repository()
    research_collector = _Collector(_collection())
    observed_at = AS_OF + timedelta(minutes=5)
    search_calls = []

    def search(**kwargs):
        search_calls.append(kwargs)
        return {
            "success": True,
            "provider": "fixture-search",
            "results": [
                {
                    "title": "贵州茅台发布经营公告",
                    "snippet": "公司披露阶段性经营数据。",
                    "url": "https://example.com/news/600519?token=secret",
                    "source": "example.com",
                    "published_date": "2025-06-29",
                }
            ],
        }

    evidence_collector = EvidenceCollector(clock=lambda: observed_at)
    service = ResearchRuntimeService(
        _config(tmp_path, evidence=True),
        collector=research_collector,
        repository=repository,
        evidence_collector=evidence_collector,
        durable_context_getter=lambda: context,
    )

    first = service.prepare(
        "600519",
        "A",
        AS_OF,
        reference_mode="live",
        evidence_search=search,
    )
    second = service.prepare(
        "600519",
        "A",
        AS_OF,
        reference_mode="live",
        evidence_search=search,
    )

    assert first is not None and second is not None
    assert first.as_of == observed_at
    assert second.as_of == observed_at
    assert first.evidence_enabled is True
    assert first.evidence_snapshot is not None
    assert first.evidence_context["evidence_hash"] == first.evidence_snapshot.evidence_hash
    assert "DSA_UNTRUSTED_EXTERNAL_DATA_BEGIN" in first.evidence_prompt_context
    assert len(first.evidence_prompt_context) <= 12_000
    news_payload = first.datasets_payload["news_search"]
    assert news_payload["content_hash"]
    assert set(news_payload) == {
        "dataset",
        "status",
        "row_count",
        "available_at",
        "data_as_of",
        "content_hash",
        "content_hashes",
        "raw_ref",
        "rows",
    }
    assert news_payload["row_count"] == 1
    assert news_payload["rows"][0]["schema_version"] == "news-search-snippet-v1"
    assert news_payload["content_hashes"] == [news_payload["content_hash"]]
    assert len(first.rows_by_dataset["news_search"]) == 1
    assert len(search_calls) == 1
    assert search_calls[0] == {
        "stock_code": "600519",
        "stock_name": "贵州茅台",
        "max_results": 5,
    }
    assert len(repository.dataset_calls) == 1
    assert len(repository.evidence_calls) == 1
    assert second.evidence_snapshot.evidence_hash == first.evidence_snapshot.evidence_hash

    frozen = _freeze(service, second)
    assert frozen.snapshot.snapshot_version == "research-snapshot-v2"
    assert frozen.snapshot.field_dictionary_version == "research-fields-v2"
    assert frozen.snapshot.evidence_snapshot_hash == second.evidence_snapshot.evidence_hash
    assert repository.research_calls[-1][0].evidence_snapshot_hash == (
        second.evidence_snapshot.evidence_hash
    )


def _debate_output(prepared, stance):
    claim = next(
        item for item in prepared.evidence_snapshot.claims if item.citation_ids
    )
    return {
        "stance": stance,
        "summary": f"bounded {stance} summary",
        "arguments": [
            {
                "id": f"{stance}-1",
                "statement": f"bounded {stance} argument",
                "claim_ids": [claim.id],
                "citation_ids": [claim.citation_ids[0]],
                "confidence": 0.6,
                "limitations": ["Evidence remains bounded."],
            }
        ],
        "open_questions": ["What would invalidate this interpretation?"],
    }


def _prepare_debate_fixture(tmp_path):
    context = _Context()
    repository = _Repository()
    observed_at = AS_OF + timedelta(minutes=5)

    def search(**_kwargs):
        return {
            "success": True,
            "provider": "fixture-search",
            "results": [
                {
                    "title": "Company publishes an operating update",
                    "snippet": "The company reported bounded operating data.",
                    "url": "https://example.com/news/600519?token=secret",
                    "source": "example.com",
                    "published_date": "2025-06-29",
                }
            ],
        }

    service = ResearchRuntimeService(
        _config(tmp_path, evidence=True, debate=True),
        collector=_Collector(_collection(rows_by_dataset=_complete_rows())),
        repository=repository,
        evidence_collector=EvidenceCollector(clock=lambda: observed_at),
        durable_context_getter=lambda: context,
    )
    prepared = service.prepare(
        "600519",
        "A",
        AS_OF,
        reference_mode="live",
        evidence_search=search,
        requested_mode="debate",
    )
    assert prepared is not None
    return service, prepared, repository


def test_debate_freezes_request_before_two_calls_and_reuses_final_snapshot(
    tmp_path,
):
    service, prepared, repository = _prepare_debate_fixture(tmp_path)
    calls = []

    def completion(request):
        calls.append(
            (
                request.stance,
                tuple(item[0] for item in repository.debate_events),
                request.messages,
            )
        )
        return DebateCompletionResult(
            output=_debate_output(prepared, request.stance),
            model_used="fixture-model",
        )

    debated = service.prepare_debate(
        prepared,
        completion=completion,
        model_route=_route(channel="research-debate"),
    )

    assert [item[0] for item in calls] == ["bull", "bear"]
    assert calls[0][1] == ("request",)
    assert calls[1][1] == ("request", "bull")
    assert all(len(item[2]) == 2 for item in calls)
    assert [item[0] for item in repository.debate_events] == [
        "request",
        "bull",
        "bear",
        "snapshot",
    ]
    assert debated.debate_enabled is True
    assert debated.debate_snapshot.status == "available"
    assert debated.debate_context["debate_hash"] == (
        debated.debate_snapshot.debate_hash
    )
    assert debated.debate_prompt_context.count(
        "DSA_UNTRUSTED_EXTERNAL_DATA_BEGIN"
    ) == 1

    replay = service.prepare_debate(
        prepared,
        completion=lambda _request: pytest.fail("bound Debate must not rerun"),
        model_route=_route(channel="research-debate", model="changed-model"),
    )
    assert replay.debate_snapshot.debate_hash == debated.debate_snapshot.debate_hash
    assert len(repository.debate_events) == 4

    frozen = _freeze(service, debated)
    assert frozen.snapshot.snapshot_version == "research-snapshot-v3"
    assert frozen.snapshot.field_dictionary_version == "research-fields-v3"
    assert frozen.snapshot.debate_snapshot_hash == debated.debate_snapshot.debate_hash


def test_debate_retry_reuses_persisted_bull_and_calls_only_missing_bear(tmp_path):
    service, prepared, repository = _prepare_debate_fixture(tmp_path)
    calls = []

    def first_attempt(request):
        calls.append(request.stance)
        if request.stance == "bear":
            raise DebateTransientError("rate_limited")
        return DebateCompletionResult(
            output=_debate_output(prepared, request.stance),
            model_used="fixture-model",
        )

    with pytest.raises(DebateTransientError, match="rate_limited"):
        service.prepare_debate(
            prepared,
            completion=first_attempt,
            model_route=_route(channel="research-debate"),
        )

    assert calls == ["bull", "bear"]
    assert tuple(repository.debate_turn_records) == ("bull",)
    assert repository.debate_records == []

    with pytest.raises(
        ResearchRuntimeContractError,
        match="differs from the current frozen contract",
    ):
        service.prepare_debate(
            prepared,
            completion=lambda _request: pytest.fail(
                "route drift must fail before another Debate call"
            ),
            model_route=_route(
                channel="research-debate",
                model="changed-model",
            ),
        )
    assert calls == ["bull", "bear"]

    debated = service.prepare_debate(
        prepared,
        completion=lambda request: (
            calls.append(request.stance)
            or DebateCompletionResult(
                output=_debate_output(prepared, request.stance),
                model_used="fixture-model",
            )
        ),
        model_route=_route(channel="research-debate"),
    )

    assert calls == ["bull", "bear", "bear"]
    assert debated.debate_snapshot.status == "available"
    assert len(repository.debate_request_records) == 1
    assert len(repository.debate_turn_records) == 2
    assert len(repository.debate_records) == 1


def test_debate_retry_reuses_terminal_bull_and_calls_only_missing_bear(tmp_path):
    service, prepared, repository = _prepare_debate_fixture(tmp_path)
    calls = []

    def first_attempt(request):
        calls.append(request.stance)
        if request.stance == "bull":
            raise DebateTerminalError("content_rejected")
        raise DebateTransientError("rate_limited")

    with pytest.raises(DebateTransientError, match="rate_limited"):
        service.prepare_debate(
            prepared,
            completion=first_attempt,
            model_route=_route(channel="research-debate"),
        )

    assert calls == ["bull", "bear"]
    assert tuple(repository.debate_failure_records) == ("bull",)
    assert repository.debate_turn_records == {}
    assert repository.debate_records == []

    debated = service.prepare_debate(
        prepared,
        completion=lambda request: (
            calls.append(request.stance)
            or DebateCompletionResult(
                output=_debate_output(prepared, request.stance),
                model_used="fixture-model",
            )
        ),
        model_route=_route(channel="research-debate"),
    )

    assert calls == ["bull", "bear", "bear"]
    assert debated.debate_snapshot.status == "partial"
    assert debated.debate_snapshot.failures[0].error_code == "content_rejected"
    assert len(repository.debate_failure_records) == 1
    assert tuple(repository.debate_turn_records) == ("bear",)
    assert len(repository.debate_records) == 1


def test_debate_flag_requires_evidence_before_any_collection(tmp_path):
    context = _Context()
    collector = _Collector(_collection())
    service = ResearchRuntimeService(
        _config(tmp_path, evidence=False, debate=True),
        collector=collector,
        repository=_Repository(),
        durable_context_getter=lambda: context,
    )

    with pytest.raises(RuntimeError, match="RESEARCH_EVIDENCE_ENABLED"):
        service.prepare("600519", "A", AS_OF, reference_mode="live")
    assert collector.calls == []


def test_evidence_search_stop_signal_preserves_durable_cancellation(tmp_path):
    context = _Context()
    repository = _Repository()

    def cancel_during_search(**_kwargs):
        context.cancel_requested.set()
        return {"success": True, "provider": "fixture-search", "results": []}

    service = ResearchRuntimeService(
        _config(tmp_path, evidence=True),
        collector=_Collector(_collection()),
        repository=repository,
        evidence_collector=EvidenceCollector(clock=lambda: AS_OF + timedelta(minutes=1)),
        durable_context_getter=lambda: context,
    )

    with pytest.raises(_Cancelled, match="cancelled"):
        service.prepare(
            "600519",
            "A",
            AS_OF,
            reference_mode="live",
            evidence_search=cancel_during_search,
        )

    assert repository.dataset_calls == []
    assert repository.evidence_calls == []


def test_evidence_flag_requires_factors_before_any_collection(tmp_path):
    context = _Context()
    collector = _Collector(_collection())
    service = ResearchRuntimeService(
        _config(tmp_path, factors=False, evidence=True),
        collector=collector,
        repository=_Repository(),
        durable_context_getter=lambda: context,
    )

    with pytest.raises(RuntimeError, match="RESEARCH_FACTORS_ENABLED"):
        service.prepare("600519", "A", AS_OF, reference_mode="live")
    assert collector.calls == []


def test_checkpoint_validates_persisted_lease_before_analysis(tmp_path):
    service, context, _collector, repository, _ = _service(tmp_path)
    prepared = _prepare(service)

    service.checkpoint(prepared)

    assert repository.lease_checks == [(prepared.lease, None)]
    context.lease_lost.set()
    with pytest.raises(_LeaseLost, match="lease lost"):
        service.checkpoint(prepared)
    assert repository.lease_checks == [(prepared.lease, None)]


def test_factor_snapshot_contract_is_stable_and_lease_fenced(tmp_path):
    service, _, _, repository, _ = _service(tmp_path)
    prepared = _prepare(service)
    factor_input, lease = repository.factor_calls[0]

    assert lease == prepared.lease
    assert prepared.factor_snapshot.content_hash == _factor_hash(factor_input)
    assert factor_input.status == "partial"
    assert 0.0 <= factor_input.coverage <= 1.0
    unknowns = [dict(item) for item in factor_input.unknowns]
    assert unknowns
    assert unknowns == sorted(
        unknowns,
        key=lambda item: (item["component"], item["metric"], item["reason"]),
    )
    assert prepared.research_context["available_at"] == "2025-06-30T09:00:00Z"
    assert prepared.research_context["factor_snapshot_hash"] == prepared.factor_snapshot.content_hash
    assert prepared.research_context["status"] == "partial"
    assert prepared.research_context["unknowns"]
    assert "research_factor_coverage_partial" in prepared.research_context["warnings"]

    block = _analysis_pack(prepared.research_context).blocks["research_factors"]
    assert block.status is ContextFieldStatus.PARTIAL
    assert block.items["unknowns"].status is ContextFieldStatus.PARTIAL


def test_incremental_chunk_provenance_changes_factor_and_research_hashes(
    tmp_path,
):
    collection = _collection()
    item = collection.datasets[0]
    current_hash = item.snapshot.content_hash
    historical_a = canonical_hash({"chunk": "historical-a"})
    historical_b = canonical_hash({"chunk": "historical-b"})
    collection_a = replace(
        collection,
        datasets=(
            replace(
                item,
                source_snapshot_hashes=(
                    historical_a,
                    current_hash,
                    historical_a,
                ),
            ),
            *collection.datasets[1:],
        ),
    )
    collection_b = replace(
        collection,
        datasets=(
            replace(
                item,
                source_snapshot_hashes=(current_hash, historical_b),
            ),
            *collection.datasets[1:],
        ),
    )
    service_a, _, _, repository_a, _ = _service(
        tmp_path / "a",
        collection=collection_a,
    )
    service_b, _, _, repository_b, _ = _service(
        tmp_path / "b",
        collection=collection_b,
    )

    prepared_a = _prepare(service_a)
    prepared_b = _prepare(service_b)
    payload_a = prepared_a.datasets_payload[item.dataset]
    payload_b = prepared_b.datasets_payload[item.dataset]

    assert payload_a["content_hash"] == current_hash
    assert payload_b["content_hash"] == current_hash
    assert payload_a["content_hashes"] == sorted({current_hash, historical_a})
    assert payload_b["content_hashes"] == sorted({current_hash, historical_b})
    factor_a = repository_a.factor_calls[0][0]
    factor_b = repository_b.factor_calls[0][0]
    current_hashes = {
        dataset.snapshot.content_hash
        for dataset in collection.datasets
    }
    assert factor_a.factor_payload == factor_b.factor_payload
    assert factor_a.input_dataset_hashes == tuple(
        sorted(current_hashes | {historical_a})
    )
    assert factor_b.input_dataset_hashes == tuple(
        sorted(current_hashes | {historical_b})
    )
    assert prepared_a.factor_snapshot.content_hash != prepared_b.factor_snapshot.content_hash

    baseline = _freeze(service_a, prepared_a)
    provenance_only = replace(
        prepared_a,
        datasets_payload=prepared_b.datasets_payload,
    )
    changed = _freeze(service_a, provenance_only)
    assert baseline.snapshot.factor_snapshot_hash == changed.snapshot.factor_snapshot_hash
    assert baseline.snapshot_hash != changed.snapshot_hash


@pytest.mark.parametrize(
    "source_hashes",
    [
        ("not-a-sha256",),
        ("A" * 64,),
        (canonical_hash({"valid": True}), "invalid-second-hash"),
        "not-a-sequence-of-hashes",
    ],
)
def test_prepare_rejects_invalid_source_snapshot_hashes(
    tmp_path,
    source_hashes,
):
    collection = _collection()
    invalid = replace(
        collection.datasets[0],
        source_snapshot_hashes=source_hashes,
    )
    collection = replace(
        collection,
        datasets=(invalid, *collection.datasets[1:]),
    )
    service, _, _, repository, _ = _service(
        tmp_path,
        collection=collection,
    )

    with pytest.raises(
        ResearchRuntimeContractError,
        match="source_snapshot_hashes",
    ):
        _prepare(service)
    assert repository.factor_calls == []


def test_complete_factors_map_to_available_analysis_context_block(tmp_path):
    collection = _collection(rows_by_dataset=_complete_rows())
    service, _, _, _, _ = _service(tmp_path, collection=collection)
    prepared = _prepare(service)

    assert prepared.research_context["status"] == "available"
    assert prepared.research_context["unknowns"] == []
    assert prepared.research_context["warnings"] == []
    pack = _analysis_pack(prepared.research_context)
    block = pack.blocks["research_factors"]
    assert block.status is ContextFieldStatus.AVAILABLE
    assert block.items["factors"].status is ContextFieldStatus.AVAILABLE
    assert block.items["unknowns"].status is ContextFieldStatus.AVAILABLE
    frozen = _freeze(service, prepared, context_pack=pack)
    assert frozen.snapshot.canonical_payload["context_pack"]["pack_version"] == "1.0"


def test_tushare_without_factors_still_freezes_consumed_dataset_snapshot(tmp_path):
    service, _, collector, repository, diagnostics = _service(
        tmp_path,
        factors=False,
    )
    prepared = _prepare(service)

    assert len(collector.calls) == 1
    assert prepared.factors_enabled is False
    assert prepared.factor_input is None
    assert prepared.factors is None
    assert prepared.factor_snapshot is None
    assert prepared.research_context["status"] == "partial"
    assert prepared.research_context["unknowns"] == []
    assert prepared.research_context["warnings"] == [
        "research_factors_disabled"
    ]
    assert repository.factor_calls == []
    frozen = _freeze(service, prepared)
    assert frozen.snapshot.factor_snapshot_hash is None
    assert frozen.snapshot.canonical_payload["factors"] is None
    assert frozen.snapshot.status == "partial"
    assert len(repository.research_calls) == 1
    assert repository.research_calls[0][1] == prepared.lease
    assert diagnostics == [frozen.snapshot_hash]


def test_without_factors_research_status_uses_worst_collection_state(tmp_path):
    collection = _collection()
    failed = replace(
        collection.datasets[0],
        status="permission_denied",
        normalized_rows=(),
        row_count=0,
    )
    collection = replace(
        collection,
        datasets=(failed, *collection.datasets[1:]),
    )
    service, _, _, _, _ = _service(
        tmp_path,
        collection=collection,
        factors=False,
    )
    prepared = _prepare(service)

    assert prepared.research_context["status"] == "permission_denied"
    assert any(
        warning.endswith("_permission_denied")
        for warning in prepared.research_context["warnings"]
    )
    block = _analysis_pack(prepared.research_context).blocks["research_factors"]
    assert block.status is ContextFieldStatus.FETCH_FAILED


def test_prepare_and_freeze_are_hash_stable_and_persist_diagnostic_hash(tmp_path):
    service, _, _, repository, diagnostics = _service(tmp_path)
    prepared = _prepare(service)

    first = _freeze(service, prepared)
    second = _freeze(service, prepared)

    assert first.snapshot_hash == second.snapshot_hash
    assert first.write_result.content_hash == first.snapshot_hash
    assert second.write_result.content_hash == second.snapshot_hash
    assert first.write_result.created is True
    assert second.write_result.created is False
    assert [call[1] for call in repository.research_calls] == [
        prepared.lease,
        prepared.lease,
    ]
    assert diagnostics == [first.snapshot_hash, second.snapshot_hash]


def test_bound_research_snapshot_is_recovered_with_exact_prepared_lineage(tmp_path):
    service, _, _, _, _ = _service(tmp_path)
    prepared = _prepare(service)
    frozen = _freeze(service, prepared)

    recovered = service.get_bound_research_snapshot(prepared)

    assert recovered is not None
    assert recovered["snapshot_hash"] == frozen.snapshot_hash


def test_retry_freeze_fails_before_write_when_bound_snapshot_hash_differs(tmp_path):
    service, _, _, repository, diagnostics = _service(tmp_path)
    prepared = _prepare(service)
    first = _freeze(service, prepared)

    with pytest.raises(
        ResearchRuntimeContractError,
        match="task-bound final Research snapshot",
    ):
        _freeze(
            service,
            prepared,
            prompt={"system": "retry drift"},
            expected_snapshot_hash=first.snapshot_hash,
        )

    assert len(repository.research_calls) == 1
    assert diagnostics == [first.snapshot_hash]


def test_data_prompt_route_and_full_policy_payload_each_change_snapshot_hash(
    tmp_path,
):
    service, _, _, _, _ = _service(tmp_path)
    prepared = _prepare(service)
    baseline = _freeze(service, prepared).snapshot_hash

    changed_payload = {
        **dict(prepared.datasets_payload),
        "manual_fixture": {"rows": [{"revision": 2}]},
    }
    changed_data = replace(prepared, datasets_payload=changed_payload)
    assert _freeze(service, changed_data).snapshot_hash != baseline
    assert (
        _freeze(service, prepared, prompt={"system": "changed prompt"}).snapshot_hash
        != baseline
    )
    assert (
        _freeze(service, prepared, model_route=_route(temperature=0.2)).snapshot_hash
        != baseline
    )

    policy = deepcopy(factor_policy_payload())
    policy["risk"]["score_floors"]["st"] += 1.0
    assert _freeze(service, prepared, policy=policy).snapshot_hash != baseline


def test_freeze_rejects_changed_lease_token_before_write(tmp_path):
    service, context, _, repository, diagnostics = _service(tmp_path)
    prepared = _prepare(service)
    context.lease_token = "lease-2"

    with pytest.raises(RuntimeError, match="lease fence"):
        _freeze(service, prepared)
    assert repository.research_calls == []
    assert diagnostics == []


def test_lease_lost_while_collector_waits_is_visible_in_combined_signal(tmp_path):
    context = _Context()

    def lose_lease(stop_signal):
        assert stop_signal.is_set() is False
        context.lease_lost.set()
        assert stop_signal.is_set() is True
        raise _LeaseLost("waiter lost lease")

    collector = _Collector(_collection(), hook=lose_lease)
    repository = _Repository()
    service = ResearchRuntimeService(
        _config(tmp_path),
        collector=collector,
        repository=repository,
        durable_context_getter=lambda: context,
    )

    with pytest.raises(_LeaseLost, match="waiter"):
        service.prepare("600519", "A", AS_OF)
    assert repository.factor_calls == []


def test_lease_lost_after_collect_stops_before_factor_calculation_or_write(
    tmp_path,
):
    context = _Context()

    def lose_after_collect(_stop_signal):
        context.lease_lost.set()

    collector = _Collector(_collection(), hook=lose_after_collect)
    repository = _Repository()
    service = ResearchRuntimeService(
        _config(tmp_path),
        collector=collector,
        repository=repository,
        durable_context_getter=lambda: context,
    )

    with pytest.raises(_LeaseLost, match="lease lost"):
        service.prepare("600519", "A", AS_OF)
    assert repository.factor_calls == []


@pytest.mark.parametrize("store_fails", [False, True])
def test_collection_boundary_flushes_real_provider_health_fail_open(
    tmp_path,
    store_fails,
):
    store = _HealthStore(fail=store_fails)
    context = _Context(store=store)
    coordinator = TushareRequestCoordinator()
    coordinator.record_result(
        "daily",
        success=True,
        rows=3,
        latency_ms=12.5,
    )
    collector = _Collector(
        _collection(),
        provider=SimpleNamespace(coordinator=coordinator),
    )
    repository = _Repository()
    service = ResearchRuntimeService(
        _config(tmp_path, factors=False),
        collector=collector,
        repository=repository,
        durable_context_getter=lambda: context,
    )

    prepared = service.prepare("600519", "A", AS_OF)

    assert isinstance(prepared, PreparedResearch)
    if store_fails:
        assert store.calls == []
    else:
        assert len(store.calls) == 1
        assert store.calls[0]["status"] == "healthy"
        assert store.calls[0]["metadata"]["metrics"]["total_calls"] == 1


@pytest.mark.parametrize("store_fails", [False, True])
def test_failed_collection_flushes_health_without_masking_original_error(
    tmp_path,
    store_fails,
):
    store = _HealthStore(fail=store_fails)
    context = _Context(store=store)
    coordinator = TushareRequestCoordinator()

    def fail_collection(_stop_signal):
        coordinator.record_result(
            "daily",
            success=False,
            rows=0,
            latency_ms=8.0,
            error_type="TushareRateLimitError",
        )
        raise ValueError("original collector failure")

    collector = _Collector(
        _collection(),
        hook=fail_collection,
        provider=SimpleNamespace(coordinator=coordinator),
    )
    repository = _Repository()
    service = ResearchRuntimeService(
        _config(tmp_path, factors=False),
        collector=collector,
        repository=repository,
        durable_context_getter=lambda: context,
    )

    with pytest.raises(ValueError, match="original collector failure"):
        service.prepare("600519", "A", AS_OF)

    assert repository.factor_calls == []
    if store_fails:
        assert store.calls == []
    else:
        assert len(store.calls) == 1
        assert store.calls[0]["status"] == "degraded"
        metrics = store.calls[0]["metadata"]["metrics"]
        assert metrics["error_types"] == {"TushareRateLimitError": 1}


def test_lease_lost_after_freeze_write_stops_before_diagnostic_update(tmp_path):
    context = _Context()
    repository = _Repository(research_hook=context.lease_lost.set)
    service, _, _, _, diagnostics = _service(
        tmp_path,
        context=context,
        repo=repository,
    )
    prepared = _prepare(service)

    with pytest.raises(_LeaseLost, match="lease lost"):
        _freeze(service, prepared)
    assert len(repository.research_calls) == 1
    assert diagnostics == []


def test_future_collection_boundary_and_missing_frozen_rows_fail_closed(tmp_path):
    future = _collection(available_at=AS_OF + timedelta(seconds=1))
    service, _, _, repository, _ = _service(tmp_path, collection=future)
    with pytest.raises(ResearchRuntimeContractError, match="after as_of"):
        service.prepare("600519", "A", AS_OF)
    assert repository.factor_calls == []

    invalid_collection = SimpleNamespace(max_available_at=AVAILABLE_AT)
    service, _, _, repository, _ = _service(
        tmp_path,
        collection=invalid_collection,
    )
    with pytest.raises(ResearchRuntimeContractError, match="rows_by_dataset"):
        service.prepare("600519", "A", AS_OF)
    assert repository.factor_calls == []


def test_default_raw_root_is_beside_database_and_only_constructed_when_enabled(
    tmp_path,
):
    context = _Context()
    repository = _Repository()
    collection = _collection()
    captured = {}

    def raw_store_factory(path):
        captured["raw_root"] = path
        return object()

    def collector_factory(provider, repo, raw_store):
        captured["provider"] = provider
        captured["repository"] = repo
        captured["raw_store"] = raw_store
        return _Collector(collection)

    service = ResearchRuntimeService(
        _config(tmp_path, factors=False),
        repository=repository,
        provider_factory=lambda _config: "provider",
        collector_factory=collector_factory,
        raw_store_factory=raw_store_factory,
        durable_context_getter=lambda: context,
    )
    prepared = _prepare(service)

    assert prepared.collection is not None
    assert captured["raw_root"] == (
        Path(_config(tmp_path).database_path).resolve().parent
        / "research"
        / "raw"
    )
    assert captured["provider"] == "provider"
    assert captured["repository"] is repository
