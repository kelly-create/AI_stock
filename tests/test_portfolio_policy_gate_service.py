from __future__ import annotations

import json
import os

import pytest
from sqlalchemy import select, text

from src.config import Config
from src.repositories.portfolio_policy_evaluation_repo import (
    PortfolioPolicyEvaluationRepository,
)
from src.services.decision_signal_service import DecisionSignalService
from src.services.portfolio_policy_gate_service import (
    InvestorPolicy,
    PortfolioPolicyGateService,
    build_portfolio_policy_replay_contract,
)
from src.storage import (
    DatabaseManager,
    DecisionSignalRecord,
    PortfolioPolicyEvaluationRecord,
)


def _config(mode: str) -> Config:
    return Config(
        stock_list=["600519"],
        personal_research_enabled=mode != "off",
        research_factors_enabled=mode != "off",
        research_evidence_enabled=mode != "off",
        portfolio_policy_gate_mode=mode,
    )


def _normalized_signal(**overrides):
    signal = {
        "idempotency_key": "signal-1",
        "source_report_id": 101,
        "trace_id": "trace-101",
        "market": "cn",
        "stock_code": "600519",
        "action": "buy",
        "research_snapshot_hash": "a" * 64,
    }
    signal.update(overrides)
    return signal


def _enabled_payload(**overrides):
    payload = {
        "research_stance": "bullish",
        "account_action": "open_candidate",
        "value_quality_score": 75,
        "trend_timing_score": 72,
        "catalyst_score": 68,
        "risk_score": 35,
        "evidence_quality_score": 80,
        "policy_context": {
            "portfolio_complete": True,
            "portfolio_snapshot_ref": "portfolio-snapshot-1",
            "current_position_weight_pct": 0,
            "projected_position_weight_pct": 5,
            "projected_sector_weight_pct": 20,
            "position_risk_pct": 1,
        },
    }
    payload.update(overrides)
    return payload


def test_frozen_replay_mode_overrides_changed_runtime_mode() -> None:
    service = PortfolioPolicyGateService(config=_config("enforce"))

    result = service.evaluate(
        payload=_enabled_payload(),
        normalized_signal=_normalized_signal(),
        job_id="task-policy-replay",
        replay_contract=build_portfolio_policy_replay_contract("shadow"),
    )

    assert result.signal_fields["policy_mode"] == "shadow"
    assert result.signal_fields["account_action"] == "open_candidate"


def test_frozen_replay_contract_drift_fails_closed() -> None:
    service = PortfolioPolicyGateService(config=_config("shadow"))
    contract = build_portfolio_policy_replay_contract("shadow")
    contract["gate_code_hash"] = "f" * 64

    with pytest.raises(ValueError, match="frozen implementation"):
        service.evaluate(
            payload=_enabled_payload(),
            normalized_signal=_normalized_signal(),
            replay_contract=contract,
        )


def test_off_mode_preserves_legacy_action_and_is_deterministic() -> None:
    service = PortfolioPolicyGateService(config=_config("off"))
    first = service.evaluate(payload={}, normalized_signal=_normalized_signal())
    second = service.evaluate(payload={}, normalized_signal=_normalized_signal())

    assert first == second
    assert first.signal_fields["research_stance"] == "bullish"
    assert first.signal_fields["account_action"] == "open_candidate"
    assert first.signal_fields["policy_mode"] == "off"
    assert first.signal_fields["would_block"] is False
    assert first.signal_fields["policy_decision"] == "allow"
    assert first.evaluation_fields["allowed"] is True
    assert first.evaluation_fields["evaluation_hash"] == second.evaluation_fields["evaluation_hash"]


def test_enabled_gate_preserves_legacy_signal_compatibility() -> None:
    service = PortfolioPolicyGateService(config=_config("shadow"))
    normalized = _normalized_signal(research_snapshot_hash=None)

    result = service.evaluate(payload={}, normalized_signal=normalized)

    assert result.formal_personal_research is False
    assert result.signal_fields["policy_mode"] == "off"
    assert result.signal_fields["account_action"] == "open_candidate"
    assert result.signal_fields["would_block"] is False
    assert result.evaluation_fields["mode"] == "off"


def test_partial_formal_signal_cannot_bypass_enabled_gate() -> None:
    service = PortfolioPolicyGateService(config=_config("shadow"))
    normalized = _normalized_signal(research_snapshot_hash=None)

    with pytest.raises(ValueError, match="research_snapshot_hash is required"):
        service.evaluate(
            payload=_enabled_payload(research_snapshot_hash=None),
            normalized_signal=normalized,
        )


def test_shadow_records_block_without_mutating_proposed_action() -> None:
    service = PortfolioPolicyGateService(config=_config("shadow"))
    payload = _enabled_payload(
        value_quality_score=64.99,
        risk_score=45.01,
        policy_context={
            **_enabled_payload()["policy_context"],
            "projected_position_weight_pct": 5.01,
            "projected_sector_weight_pct": 30.01,
            "position_risk_pct": 1.01,
        },
    )

    result = service.evaluate(payload=payload, normalized_signal=_normalized_signal())

    assert result.evaluation_fields["allowed"] is False
    assert result.evaluation_fields["would_block"] is True
    assert result.evaluation_fields["final_account_action"] == "open_candidate"
    assert result.signal_fields["account_action"] == "open_candidate"
    assert result.signal_fields["policy_decision"] == "block"
    assert set(json.loads(result.signal_fields["policy_reasons_json"])) == {
        "value_quality_below_minimum",
        "risk_score_above_maximum",
        "initial_position_limit_exceeded",
        "sector_limit_exceeded",
        "position_risk_limit_exceeded",
    }


def test_enforce_downgrades_blocked_new_risk_to_observe() -> None:
    service = PortfolioPolicyGateService(config=_config("enforce"))
    payload = _enabled_payload(
        policy_context={
            **_enabled_payload()["policy_context"],
            "portfolio_complete": False,
        }
    )

    result = service.evaluate(payload=payload, normalized_signal=_normalized_signal())

    assert result.evaluation_fields["allowed"] is False
    assert result.evaluation_fields["final_account_action"] == "observe"
    assert result.signal_fields["account_action"] == "observe"
    assert result.signal_fields["would_block"] is True


def test_exact_policy_boundaries_allow_open_candidate() -> None:
    policy = InvestorPolicy()
    service = PortfolioPolicyGateService(config=_config("enforce"), policy=policy)

    result = service.evaluate(
        payload=_enabled_payload(
            value_quality_score=65,
            trend_timing_score=65,
            risk_score=45,
            evidence_quality_score=70,
        ),
        normalized_signal=_normalized_signal(),
    )

    assert result.evaluation_fields["allowed"] is True
    assert result.evaluation_fields["final_account_action"] == "open_candidate"
    assert result.evaluation_fields["policy_hash"] == policy.content_hash


@pytest.mark.parametrize(
    ("action", "current", "projected", "reason"),
    [
        ("open_candidate", 1.0, 5.0, "open_candidate_existing_position"),
        ("add_candidate", 0.0, 5.0, "add_candidate_without_position"),
        ("add_candidate", 5.0, 5.0, "add_target_not_above_current"),
        ("add_candidate", 5.0, 4.0, "add_target_not_above_current"),
    ],
)
def test_risk_increasing_action_matches_current_position_direction(
    action: str,
    current: float,
    projected: float,
    reason: str,
) -> None:
    service = PortfolioPolicyGateService(config=_config("enforce"))
    context = {
        **_enabled_payload()["policy_context"],
        "current_position_weight_pct": current,
        "projected_position_weight_pct": projected,
    }

    result = service.evaluate(
        payload=_enabled_payload(account_action=action, policy_context=context),
        normalized_signal=_normalized_signal(),
    )

    assert result.evaluation_fields["allowed"] is False
    assert result.evaluation_fields["final_account_action"] == "observe"
    assert reason in json.loads(result.evaluation_fields["reasons_json"])


@pytest.mark.parametrize("action", ["reduce_candidate", "exit_candidate"])
def test_de_risking_action_is_not_blocked_by_missing_portfolio_facts(action: str) -> None:
    service = PortfolioPolicyGateService(config=_config("enforce"))
    payload = _enabled_payload(account_action=action)
    payload.pop("policy_context")
    for field_name in (
        "value_quality_score",
        "trend_timing_score",
        "catalyst_score",
        "risk_score",
        "evidence_quality_score",
    ):
        payload.pop(field_name)

    result = service.evaluate(payload=payload, normalized_signal=_normalized_signal(action="sell"))

    assert result.evaluation_fields["allowed"] is True
    assert result.evaluation_fields["final_account_action"] == action


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"evidence_quality_score": None}, "evidence_quality_score is required"),
        ({"value_quality_score": True}, "value_quality_score must be a number"),
        ({"policy_context": None}, "policy_context is required"),
    ],
)
def test_enabled_gate_rejects_missing_or_type_smuggled_inputs(mutation, message) -> None:
    service = PortfolioPolicyGateService(config=_config("shadow"))
    with pytest.raises(ValueError, match=message):
        service.evaluate(
            payload=_enabled_payload(**mutation),
            normalized_signal=_normalized_signal(),
        )


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(tmp_path / "policy-gate.db")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if old_database_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old_database_path


def _signal_payload(**overrides):
    payload = {
        "stock_code": "600519",
        "market": "cn",
        "source_type": "analysis",
        "source_report_id": 101,
        "trace_id": "trace-policy-101",
        "trigger_source": "analysis",
        "action": "buy",
        "horizon": "10d",
        "research_snapshot_hash": "a" * 64,
        **_enabled_payload(),
    }
    payload.update(overrides)
    return payload


def test_decision_signal_service_uses_frozen_replay_mode(isolated_db) -> None:
    service = DecisionSignalService(
        db_manager=isolated_db,
        config=_config("enforce"),
    )

    created = service.create_signal(
        _signal_payload(
            policy_replay_contract=build_portfolio_policy_replay_contract(
                "shadow"
            )
        )
    )

    assert created["item"]["policy_mode"] == "shadow"
    assert created["item"]["account_action"] == "open_candidate"


def test_legacy_signal_does_not_persist_personal_policy_lineage(isolated_db) -> None:
    service = DecisionSignalService(
        db_manager=isolated_db,
        config=_config("shadow"),
    )

    created = service.create_signal(
        {
            "stock_code": "600519",
            "market": "cn",
            "source_type": "analysis",
            "source_report_id": 1101,
            "trace_id": "trace-legacy-policy-1101",
            "trigger_source": "analysis",
            "action": "buy",
            "horizon": "10d",
        }
    )

    assert created["created"] is True
    assert created["item"]["policy_evaluation_hash"] is None
    assert created["item"]["policy_mode"] is None
    with isolated_db.get_session() as session:
        assert session.execute(select(PortfolioPolicyEvaluationRecord)).scalars().all() == []


def test_signal_and_policy_evaluation_are_atomic_and_idempotent(isolated_db) -> None:
    service = DecisionSignalService(
        db_manager=isolated_db,
        config=_config("shadow"),
    )

    first = service.create_signal(_signal_payload())
    second = service.create_signal(_signal_payload())

    assert first["created"] is True
    assert second["created"] is False
    assert first["item"]["id"] == second["item"]["id"]
    assert first["item"]["policy_mode"] == "shadow"
    assert first["item"]["would_block"] is False
    with isolated_db.get_session() as session:
        signals = session.execute(select(DecisionSignalRecord)).scalars().all()
        evaluations = session.execute(
            select(PortfolioPolicyEvaluationRecord)
        ).scalars().all()
    assert len(signals) == 1
    assert len(evaluations) == 1
    assert evaluations[0].signal_id == signals[0].id
    assert json.loads(evaluations[0].portfolio_context_json) == {
        "current_position_weight_pct": 0.0,
        "incomplete_reason_codes": [],
        "portfolio_complete": True,
        "portfolio_snapshot_ref": "portfolio-snapshot-1",
        "position_risk_pct": 1.0,
        "projected_position_weight_pct": 5.0,
        "projected_sector_weight_pct": 20.0,
    }


def test_policy_evaluation_is_database_immutable(isolated_db) -> None:
    service = DecisionSignalService(
        db_manager=isolated_db,
        config=_config("shadow"),
    )
    created = service.create_signal(_signal_payload())
    evaluation_hash = created["item"]["policy_evaluation_hash"]

    with pytest.raises(Exception, match="portfolio policy evaluation is immutable"):
        with isolated_db.get_session() as session:
            session.execute(
                text(
                    "UPDATE portfolio_policy_evaluations "
                    "SET portfolio_context_json = '{}' "
                    "WHERE evaluation_hash = :digest"
                ),
                {"digest": evaluation_hash},
            )

    with pytest.raises(Exception, match="portfolio policy evaluation is immutable"):
        with isolated_db.get_session() as session:
            session.execute(
                text(
                    "DELETE FROM portfolio_policy_evaluations "
                    "WHERE evaluation_hash = :digest"
                ),
                {"digest": evaluation_hash},
            )


def test_changed_formal_evaluation_creates_a_new_immutable_signal(isolated_db) -> None:
    service = DecisionSignalService(
        db_manager=isolated_db,
        config=_config("shadow"),
    )

    first = service.create_signal(_signal_payload())
    second = service.create_signal(
        _signal_payload(
            research_snapshot_hash="b" * 64,
            policy_context={
                **_enabled_payload()["policy_context"],
                "portfolio_snapshot_ref": "portfolio-snapshot-2",
                "projected_position_weight_pct": 8,
            },
        )
    )

    assert first["created"] is True
    assert second["created"] is True
    assert first["item"]["id"] != second["item"]["id"]
    assert first["item"]["policy_evaluation_hash"] != second["item"]["policy_evaluation_hash"]
    with isolated_db.get_session() as session:
        signals = session.execute(
            select(DecisionSignalRecord).order_by(DecisionSignalRecord.id.asc())
        ).scalars().all()
        evaluations = session.execute(
            select(PortfolioPolicyEvaluationRecord).order_by(
                PortfolioPolicyEvaluationRecord.id.asc()
            )
        ).scalars().all()
    assert len(signals) == 2
    assert len(evaluations) == 2
    assert [row.signal_id for row in evaluations] == [row.id for row in signals]


def test_policy_audit_failure_rolls_back_signal_write(isolated_db, monkeypatch) -> None:
    service = DecisionSignalService(
        db_manager=isolated_db,
        config=_config("shadow"),
    )

    def fail_audit(**_kwargs):
        raise RuntimeError("injected policy audit failure")

    monkeypatch.setattr(
        PortfolioPolicyEvaluationRepository,
        "ensure_in_session",
        fail_audit,
    )
    with pytest.raises(RuntimeError, match="injected policy audit"):
        service.create_signal(_signal_payload())

    with isolated_db.get_session() as session:
        assert session.execute(select(DecisionSignalRecord)).scalars().all() == []
        assert session.execute(select(PortfolioPolicyEvaluationRecord)).scalars().all() == []
