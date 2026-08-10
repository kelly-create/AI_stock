"""Durable orchestration tests for Decision Outcome v2."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from src.repositories.decision_outcome_v2_repo import DecisionOutcomeV2Candidate
from src.services.decision_outcome_v2_service import (
    DecisionOutcomeV2DatasetBundle,
    DecisionOutcomeV2Service,
)
from src.services.research import DatasetCollectionResult
from src.services.durable_job_handlers import (
    DecisionOutcomesV2Payload,
    build_default_durable_job_registry,
)


def _config(**overrides):
    values = {
        "durable_jobs_enabled": True,
        "personal_research_enabled": True,
        "tushare_research_enabled": True,
        "research_factors_enabled": True,
        "decision_outcome_v2_enabled": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _Context:
    job_id = "outcome-v2-job"
    worker_id = "worker-1"
    lease_token = "lease-1"
    cancel_requested = SimpleNamespace(is_set=lambda: False)
    lease_lost = SimpleNamespace(is_set=lambda: False)

    def __init__(self):
        self.checkpoints = 0
        self.progress_events = []

    def checkpoint(self):
        self.checkpoints += 1

    def progress(self, progress, message, *, stage):
        self.progress_events.append((progress, message, stage))


def _candidate(horizon: str = "5d") -> DecisionOutcomeV2Candidate:
    signal = SimpleNamespace(
        id=42,
        stock_code="600519",
        created_at=datetime(2026, 8, 10, 8, tzinfo=timezone.utc),
    )
    policy = SimpleNamespace(final_account_action="open_candidate")
    return DecisionOutcomeV2Candidate(
        signal=signal,
        policy_evaluation=policy,
        horizon=horizon,
        existing_outcome=None,
    )


def _bundle(*, critical_reason=None) -> DecisionOutcomeV2DatasetBundle:
    sessions = tuple(date(2026, 8, 11 + index) for index in range(20))
    stock_bars = tuple(
        {
            "trade_date": session,
            "open": 100.0,
            "high": 103.0,
            "low": 99.0,
            "close": 102.0 if index == 4 else 101.0,
            "adj_factor": 1.0,
        }
        for index, session in enumerate(sessions)
    )
    csi_bars = (
        {
            "trade_date": sessions[0],
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
        },
        {
            "trade_date": sessions[4],
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
        },
    )
    return DecisionOutcomeV2DatasetBundle(
        xshg_sessions=sessions,
        stock_bars=stock_bars,
        suspend_rows=(),
        stk_limit_rows=(
            {
                "trade_date": sessions[0],
                "up_limit": 110.0,
                "down_limit": 90.0,
            },
        ),
        csi300_bars=csi_bars,
        sw1_membership=None,
        sw1_bars=(),
        dataset_hashes=("a" * 64,),
        evaluation_as_of=datetime(2026, 8, 20, tzinfo=timezone.utc),
        critical_reason=critical_reason,
        sw1_reason="sw1_index_member_all_empty",
    )


class _Repository:
    def __init__(self, candidates):
        self.candidates = candidates
        self.persisted = []
        self.query = None

    def list_candidate_keys(self, **kwargs):
        self.query = kwargs
        return self.candidates

    def persist_evaluation(self, *, signal_id, evaluation):
        self.persisted.append((signal_id, evaluation))
        row = SimpleNamespace(
            id=len(self.persisted),
            signal_id=signal_id,
            horizon=evaluation.horizon,
            engine_version=evaluation.engine_version,
            eval_status=evaluation.eval_status,
            reason_code=evaluation.reason,
            observation_hash="b" * 64,
        )
        return SimpleNamespace(
            row=row,
            created=True,
            transitioned=False,
            disposition="created",
        )


def test_service_rejects_execution_outside_durable_worker_before_loading_data() -> None:
    called = False

    def loader(*args):
        nonlocal called
        called = True
        return _bundle()

    service = DecisionOutcomeV2Service(
        repository=_Repository([_candidate()]),
        config=_config(),
        context_getter=lambda: None,
        bundle_loader=loader,
    )
    with pytest.raises(RuntimeError, match="durable Worker"):
        service.run_outcomes()
    assert called is False


def test_service_evaluates_selected_key_and_freezes_dataset_lineage() -> None:
    context = _Context()
    repository = _Repository([_candidate()])
    service = DecisionOutcomeV2Service(
        repository=repository,
        config=_config(),
        context_getter=lambda: context,
        clock=lambda: datetime(2026, 8, 20, tzinfo=timezone.utc),
        bundle_loader=lambda *_args: _bundle(),
    )

    result = service.run_outcomes(
        signal_id=42,
        horizons=["5d"],
        stock_code="sh600519",
        decision_profile="balanced",
        limit=1,
    )

    assert result["selected"] == 1
    assert result["created"] == 1
    assert repository.query["engine_version"] == "personal-research-outcome-v2"
    assert repository.query["stock_code"] == "600519"
    signal_id, evaluation = repository.persisted[0]
    assert signal_id == 42
    assert evaluation.eval_status == "evaluated"
    assert evaluation.entry_trade_date == date(2026, 8, 11)
    assert evaluation.end_trade_date == date(2026, 8, 15)
    assert evaluation.dataset_hashes == ("a" * 64,)
    assert evaluation.csi300.status == "available"
    assert evaluation.sw1.reason == "sw1_index_member_all_empty"
    assert context.checkpoints >= 3
    assert context.progress_events[-1][2] == "decision_outcomes_v2"


def test_service_maps_permanent_critical_dataset_failure_to_terminal_unable() -> None:
    repository = _Repository([_candidate()])
    service = DecisionOutcomeV2Service(
        repository=repository,
        config=_config(),
        context_getter=lambda: _Context(),
        bundle_loader=lambda *_args: _bundle(
            critical_reason="stk_limit_permission_denied"
        ),
    )

    service.run_outcomes(horizons=["5d"], limit=1)
    evaluation = repository.persisted[0][1]
    assert evaluation.eval_status == "unable"
    assert evaluation.reason == "stk_limit_permission_denied"
    assert evaluation.terminal is True


def test_worker_rechecks_feature_flag_before_any_dataset_load() -> None:
    called = False

    def loader(*args):
        nonlocal called
        called = True
        return _bundle()

    service = DecisionOutcomeV2Service(
        repository=_Repository([_candidate()]),
        config=_config(decision_outcome_v2_enabled=False),
        context_getter=lambda: _Context(),
        bundle_loader=loader,
    )
    with pytest.raises(RuntimeError, match="DECISION_OUTCOME_V2_ENABLED"):
        service.run_outcomes()
    assert called is False


def test_durable_registry_has_strict_v2_payload_without_force() -> None:
    registry = build_default_durable_job_registry()
    registered = registry.resolve("decision_outcomes_v2", 1)
    assert registered.payload_model is DecisionOutcomesV2Payload
    payload = registered.payload_model.model_validate(
        {"signal_id": 42, "horizons": ["5d", "20d"], "limit": 2}
    )
    assert payload.horizons == ["5d", "20d"]
    assert payload.notify is False
    with pytest.raises(Exception, match="force"):
        registered.payload_model.model_validate(
            {"signal_id": 42, "horizons": ["5d"], "force": True}
        )


def test_runtime_never_live_backfills_sw1_membership_after_decision() -> None:
    class Collector:
        def __init__(self):
            self.calls = []

        def collect_dataset(self, stock_code, dataset, **kwargs):
            self.calls.append((stock_code, dataset, kwargs.get("reference_mode")))
            return DatasetCollectionResult(
                dataset=dataset,
                status="empty",
                row_count=0,
                snapshot=None,
                query_params={},
                normalized_rows=(),
                available_at=kwargs["as_of"],
                data_as_of=kwargs["as_of"],
                reused=kwargs.get("reference_mode") == "historical",
            )

    collector = Collector()
    context = _Context()
    service = DecisionOutcomeV2Service(
        repository=_Repository([]),
        config=_config(),
        context_getter=lambda: context,
        collector=collector,
        session_resolver=lambda _date, count: tuple(
            date(2026, 8, 11 + index) for index in range(count)
        ),
    )
    service._collect_bundle(
        _candidate(),
        "5d",
        datetime(2026, 8, 20, tzinfo=timezone.utc),
        context,
    )

    sw_current_state_calls = [
        call
        for call in collector.calls
        if call[1] in {"sw1_index_classify", "sw1_index_member_all"}
    ]
    assert sw_current_state_calls == [
        ("600519", "sw1_index_classify", "historical"),
        ("600519", "sw1_index_member_all", "historical"),
    ]


def test_entry_session_before_close_remains_pending_not_terminal_unable() -> None:
    repository = _Repository([_candidate()])
    before_entry_close = replace(
        _bundle(),
        stock_bars=(),
        evaluation_as_of=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
    )
    service = DecisionOutcomeV2Service(
        repository=repository,
        config=_config(),
        context_getter=lambda: _Context(),
        bundle_loader=lambda *_args: before_entry_close,
    )

    service.run_outcomes(horizons=["5d"], limit=1)
    evaluation = repository.persisted[0][1]
    assert evaluation.eval_status == "pending"
    assert evaluation.reason == "entry_session_not_reached"
    assert evaluation.terminal is False


def test_future_horizon_skips_provider_and_can_later_progress() -> None:
    class Collector:
        calls = 0

        def collect_dataset(self, *_args, **_kwargs):
            self.calls += 1
            raise AssertionError("future horizon must not call the provider")

    repository = _Repository([_candidate()])
    collector = Collector()
    service = DecisionOutcomeV2Service(
        repository=repository,
        config=_config(),
        context_getter=lambda: _Context(),
        collector=collector,
        # 2026-08-15 14:59 Asia/Shanghai: the exact horizon-end close has not
        # occurred, so even a same-day query must not reach the provider.
        clock=lambda: datetime(2026, 8, 15, 6, 59, tzinfo=timezone.utc),
        session_resolver=lambda _date, count: tuple(
            date(2026, 8, 11 + index) for index in range(count)
        ),
    )

    service.run_outcomes(horizons=["5d"], limit=1)
    pending = repository.persisted[0][1]
    assert collector.calls == 0
    assert pending.eval_status == "pending"
    assert pending.reason == "horizon_not_reached"
    assert pending.terminal is False

    progressed = DecisionOutcomeV2Service(
        repository=repository,
        config=_config(),
        context_getter=lambda: _Context(),
        clock=lambda: datetime(2026, 8, 20, tzinfo=timezone.utc),
        bundle_loader=lambda *_args: _bundle(),
    )
    progressed.run_outcomes(horizons=["5d"], limit=1)
    assert repository.persisted[1][1].eval_status == "evaluated"


def test_unavailable_sw_daily_preserves_frozen_membership_identity() -> None:
    repository = _Repository([_candidate()])
    bundle = replace(
        _bundle(),
        sw1_membership={
            "industry_code": "801120.SI",
            "industry_name": "Food and Beverage",
            "effective_from": "2021-01-01",
            "effective_to": None,
            "known_at": "2026-08-10T07:00:00+00:00",
            "stock_code": "600519.SH",
            "snapshot_hash": "e" * 64,
        },
        sw1_reason="sw1_index_daily_empty",
    )
    service = DecisionOutcomeV2Service(
        repository=repository,
        config=_config(),
        context_getter=lambda: _Context(),
        bundle_loader=lambda *_args: bundle,
    )

    service.run_outcomes(horizons=["5d"], limit=1)
    sw1 = repository.persisted[0][1].sw1
    assert sw1.status == "unavailable"
    assert sw1.code == "801120.SI"
    assert sw1.name == "Food and Beverage"
    assert sw1.reason == "sw1_index_daily_empty"
