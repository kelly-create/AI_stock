"""Focused storage tests for independent personal-research Decision Outcome v2."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
import sqlite3
import threading

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from src.config import Config
from src.core.decision_outcome_v2_evaluator import (
    BenchmarkOutcomeV2,
    DecisionOutcomeV2Evaluation,
    DecisionOutcomeV2Evaluator,
)
from src.repositories.decision_outcome_v2_repo import (
    DecisionOutcomeV2ConflictError,
    DecisionOutcomeV2Repository,
)
from src.storage import (
    DatabaseManager,
    DecisionOutcomeV2Record,
    DecisionSignalRecord,
    PortfolioPolicyEvaluationRecord,
    ResearchDatasetSnapshotRecord,
)


ENGINE = "personal-research-outcome-v2"
DATASET_HASH = "d" * 64


@pytest.fixture()
def outcome_db(tmp_path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABASE_MIGRATION_MODE", "auto")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    database_path = tmp_path / "decision-outcome-v2.db"
    db = DatabaseManager(db_url=f"sqlite:///{database_path.as_posix()}")
    with db.session_scope() as session:
        session.add(
            ResearchDatasetSnapshotRecord(
                dataset="daily",
                scope_type="stock",
                scope_value="fixture",
                market="cn",
                provider="fixture",
                schema_version="fixture-v1",
                data_as_of=datetime(2026, 8, 1, 0, 0, 0),
                available_at=datetime(2026, 8, 1, 0, 0, 0),
                observed_at=datetime(2026, 8, 1, 0, 0, 0),
                status="available",
                normalized_json="[]",
                content_hash=DATASET_HASH,
            )
        )
    try:
        yield db, database_path
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()


def _seed_signal(
    db: DatabaseManager,
    *,
    index: int,
    final_action: str = "open_candidate",
    proposed_action: str | None = None,
    created_at: datetime | None = None,
) -> int:
    evaluation_hash = f"{index + 100:064x}"
    policy_hash = f"{index + 200:064x}"
    research_hash = f"{index + 300:064x}"
    family_action = {
        "open_candidate": "buy",
        "add_candidate": "buy",
        "reduce_candidate": "sell",
        "exit_candidate": "sell",
        "observe": "hold",
        "hold": "hold",
    }[final_action]
    with db.session_scope() as session:
        signal = DecisionSignalRecord(
            stock_code=f"60{index:04d}",
            stock_name="Outcome fixture",
            market="cn",
            source_type="personal_research",
            trace_id=f"outcome-v2-trace-{index}",
            decision_profile="balanced",
            trigger_source="scheduled_job",
            action=family_action,
            confidence=0.8,
            score=82,
            horizon="20d",
            research_stance="bullish",
            account_action=final_action,
            value_quality_score=81,
            trend_timing_score=72,
            catalyst_score=63,
            risk_score=44,
            evidence_quality_score=90,
            research_snapshot_hash=research_hash,
            policy_version="portfolio-policy-v1",
            policy_hash=policy_hash,
            policy_evaluation_hash=evaluation_hash,
            portfolio_snapshot_ref=f"portfolio:{index}",
            prompt_version="personal-research-v1",
            policy_mode="shadow",
            policy_decision=(
                "no_action" if final_action in {"observe", "hold"} else "allow"
            ),
            would_block=False,
            plan_quality="complete",
            status="active",
            created_at=created_at or datetime(2026, 8, 1, 8, index, 0),
        )
        session.add(signal)
        session.flush()
        session.add(
            PortfolioPolicyEvaluationRecord(
                evaluation_hash=evaluation_hash,
                signal_id=signal.id,
                stock_code=signal.stock_code,
                market=signal.market,
                mode="shadow",
                policy_version=signal.policy_version,
                policy_hash=policy_hash,
                research_snapshot_hash=research_hash,
                portfolio_snapshot_ref=signal.portfolio_snapshot_ref,
                input_hash=f"{index + 400:064x}",
                output_hash=f"{index + 500:064x}",
                research_stance=signal.research_stance,
                proposed_account_action=proposed_action or final_action,
                final_account_action=final_action,
                verdict=signal.policy_decision,
                allowed=True,
                would_block=False,
                reasons_json="[]",
                component_scores_json="{}",
                limits_json="{}",
            )
        )
        session.flush()
        return int(signal.id)


def _unavailable_benchmark(
    *,
    code: str | None,
    name: str | None,
    reason: str,
) -> BenchmarkOutcomeV2:
    return BenchmarkOutcomeV2(
        code=code,
        name=name,
        status="unavailable",
        reason=reason,
    )


def _pending(
    *,
    horizon: str = "5d",
    engine: str = ENGINE,
    family: str = "long",
) -> DecisionOutcomeV2Evaluation:
    return DecisionOutcomeV2Evaluation(
        horizon=horizon,
        engine_version=engine,
        final_action_family=family,
        eval_status="pending",
        reason="insufficient_xshg_sessions",
        signal_session=date(2026, 8, 3),
        dataset_hashes=(DATASET_HASH,),
    )


def _evaluated(
    *,
    horizon: str = "5d",
    engine: str = ENGINE,
    family: str = "long",
) -> DecisionOutcomeV2Evaluation:
    return DecisionOutcomeV2Evaluation(
        horizon=horizon,
        engine_version=engine,
        final_action_family=family,
        eval_status="evaluated",
        outcome="hit",
        direction_correct=True,
        signal_session=date(2026, 8, 3),
        entry_trade_date=date(2026, 8, 4),
        end_trade_date=date(2026, 8, 8),
        trading_day_count=int(horizon[:-1]),
        entry_raw_open=100.0,
        entry_adj_factor=1.2,
        end_adjusted_close=105.0,
        stock_return_pct=5.0,
        directional_return_pct=5.0,
        mfe_pct=7.0,
        mae_pct=2.0,
        dataset_hashes=(DATASET_HASH,),
        csi300=BenchmarkOutcomeV2(
            code="000300.SH",
            name="CSI 300",
            status="available",
            return_pct=2.0,
            stock_excess_return_pct=3.0,
            directional_excess_return_pct=3.0,
        ),
        sw1=_unavailable_benchmark(
            code="801120.SI",
            name="Food & Beverage",
            reason="missing_sw1_end_bar",
        ),
    )


def _observational() -> DecisionOutcomeV2Evaluation:
    return DecisionOutcomeV2Evaluation(
        horizon="5d",
        engine_version=ENGINE,
        final_action_family="observational",
        eval_status="observational",
        signal_session=date(2026, 8, 3),
        entry_trade_date=date(2026, 8, 4),
        end_trade_date=date(2026, 8, 8),
        trading_day_count=5,
        entry_raw_open=100.0,
        entry_adj_factor=1.0,
        end_adjusted_close=101.0,
        stock_return_pct=1.0,
        dataset_hashes=(DATASET_HASH,),
        csi300=BenchmarkOutcomeV2(
            code="000300.SH",
            name="CSI 300",
            status="available",
            return_pct=0.5,
            stock_excess_return_pct=0.5,
        ),
        sw1=_unavailable_benchmark(
            code=None,
            name=None,
            reason="missing_sw1_membership",
        ),
    )


def test_repository_transitions_pending_once_and_terminal_is_immutable(
    outcome_db,
) -> None:
    db, _path = outcome_db
    signal_id = _seed_signal(db, index=1)
    repository = DecisionOutcomeV2Repository(db)

    pending = repository.persist_evaluation(
        signal_id=signal_id,
        evaluation=_pending(),
    )
    transitioned = repository.persist_evaluation(
        signal_id=signal_id,
        evaluation=_evaluated(),
    )
    replay = repository.persist_evaluation(
        signal_id=signal_id,
        evaluation=_evaluated(),
    )

    assert pending.created is True
    assert transitioned.transitioned is True
    assert replay.disposition == "unchanged"
    assert replay.row.id == transitioned.row.id == pending.row.id
    assert replay.row.eval_status == "evaluated"
    assert replay.row.sw1_status == "unavailable"
    assert replay.row.sw1_reason_code == "missing_sw1_end_bar"
    assert replay.row.csi300_directional_excess_return_pct == pytest.approx(3.0)

    original = _evaluated()
    changed = replace(
        original,
        end_adjusted_close=106.0,
        stock_return_pct=6.0,
        directional_return_pct=6.0,
        csi300=replace(
            original.csi300,
            stock_excess_return_pct=4.0,
            directional_excess_return_pct=4.0,
        ),
    )
    with pytest.raises(
        DecisionOutcomeV2ConflictError,
        match="terminal outcome conflicts",
    ):
        repository.persist_evaluation(signal_id=signal_id, evaluation=changed)

    session = db.get_session()
    try:
        with pytest.raises(IntegrityError, match="terminal decision outcome v2"):
            session.connection().exec_driver_sql(
                "UPDATE decision_outcomes_v2 SET reason_code = 'tampered' "
                "WHERE id = ?",
                (replay.row.id,),
            )
        session.rollback()
        with pytest.raises(IntegrityError, match="decision outcome v2 is immutable"):
            session.connection().exec_driver_sql(
                "DELETE FROM decision_outcomes_v2 WHERE id = ?",
                (replay.row.id,),
            )
    finally:
        session.rollback()
        session.close()


def test_pending_outcome_keeps_frozen_status_after_signal_expires(
    outcome_db,
) -> None:
    db, _path = outcome_db
    signal_id = _seed_signal(db, index=12)
    repository = DecisionOutcomeV2Repository(db)

    pending = repository.persist_evaluation(
        signal_id=signal_id,
        evaluation=_pending(),
    )
    assert pending.row.signal_status == "active"

    with db.session_scope() as session:
        signal = session.get(DecisionSignalRecord, signal_id)
        assert signal is not None
        signal.status = "expired"

    transitioned = repository.persist_evaluation(
        signal_id=signal_id,
        evaluation=_evaluated(),
    )

    assert transitioned.transitioned is True
    assert transitioned.row.id == pending.row.id
    assert transitioned.row.eval_status == "evaluated"
    assert transitioned.row.signal_status == "active"
    with db.session_scope() as session:
        signal = session.get(DecisionSignalRecord, signal_id)
        assert signal is not None
        assert signal.status == "expired"


def test_repository_accepts_the_pure_evaluator_dto_without_adapter(outcome_db) -> None:
    db, _path = outcome_db
    signal_id = _seed_signal(db, index=11)
    sessions = [date(2026, 8, day) for day in range(4, 9)]
    bars = [
        {
            "trade_date": trade_date,
            "open": 100.0,
            "high": 106.0,
            "low": 98.0,
            "close": 105.0 if index == 4 else 100.0,
            "adj_factor": 1.0,
        }
        for index, trade_date in enumerate(sessions)
    ]
    evaluation = DecisionOutcomeV2Evaluator.evaluate(
        final_action_family="long",
        horizon="5d",
        signal_session=date(2026, 8, 3),
        xshg_sessions=sessions,
        stock_bars=bars,
        stk_limit_rows=[
            {"trade_date": sessions[0], "up_limit": 110.0, "down_limit": 90.0}
        ],
        csi300_bars=[
            {"trade_date": sessions[0], "open": 100.0, "close": 100.0},
            {"trade_date": sessions[-1], "open": 102.0, "close": 102.0},
        ],
        dataset_hashes=(DATASET_HASH,),
    )

    result = DecisionOutcomeV2Repository(db).persist_evaluation(
        signal_id=signal_id,
        evaluation=evaluation,
    )

    assert result.row.eval_status == "evaluated"
    assert result.row.stock_return_pct == pytest.approx(5.0)
    assert result.row.csi300_stock_excess_return_pct == pytest.approx(3.0)


def test_candidates_are_fair_across_missing_and_pending_and_engine_bumps(
    outcome_db,
) -> None:
    db, _path = outcome_db
    first_id = _seed_signal(
        db,
        index=2,
        created_at=datetime(2026, 8, 1, 8, 0, 0),
    )
    second_id = _seed_signal(
        db,
        index=3,
        created_at=datetime(2026, 8, 1, 9, 0, 0),
    )
    repository = DecisionOutcomeV2Repository(db)
    repository.persist_evaluation(signal_id=first_id, evaluation=_pending())

    candidates = repository.list_candidate_keys(
        horizons=("5d", "10d"),
        engine_version=ENGINE,
        limit=4,
    )
    assert [(item.signal.id, item.horizon) for item in candidates] == [
        (first_id, "10d"),
        (second_id, "5d"),
        (second_id, "10d"),
        (first_id, "5d"),
    ]
    assert candidates[-1].existing_outcome.eval_status == "pending"

    repository.persist_evaluation(signal_id=second_id, evaluation=_evaluated())
    bumped = "personal-research-outcome-v2.1"
    bumped_candidates = repository.list_candidate_keys(
        horizons=("5d",),
        engine_version=bumped,
        limit=10,
    )
    assert {item.signal.id for item in bumped_candidates} == {first_id, second_id}
    bumped_write = repository.persist_evaluation(
        signal_id=second_id,
        evaluation=_evaluated(engine=bumped),
    )
    assert bumped_write.created is True
    assert len(repository.list_for_signal(second_id)) == 2


def test_terminal_nullable_contracts_and_dataset_lineage_are_fail_closed(
    outcome_db,
) -> None:
    db, _path = outcome_db
    evaluated_id = _seed_signal(db, index=4)
    observational_id = _seed_signal(db, index=5, final_action="hold")
    unexecutable_id = _seed_signal(db, index=6)
    unable_id = _seed_signal(db, index=7)
    repository = DecisionOutcomeV2Repository(db)

    evaluated = repository.persist_evaluation(
        signal_id=evaluated_id,
        evaluation=_evaluated(),
    ).row
    observational = repository.persist_evaluation(
        signal_id=observational_id,
        evaluation=_observational(),
    ).row
    unexecutable = repository.persist_evaluation(
        signal_id=unexecutable_id,
        evaluation=DecisionOutcomeV2Evaluation(
            horizon="5d",
            engine_version=ENGINE,
            final_action_family="long",
            eval_status="unexecutable",
            reason="entry_one_price_limit_up",
            signal_session=date(2026, 8, 3),
            entry_trade_date=date(2026, 8, 4),
            end_trade_date=date(2026, 8, 8),
            trading_day_count=5,
            entry_raw_open=100.0,
            entry_adj_factor=1.0,
            dataset_hashes=(DATASET_HASH,),
        ),
    ).row
    unable = repository.persist_evaluation(
        signal_id=unable_id,
        evaluation=DecisionOutcomeV2Evaluation(
            horizon="5d",
            engine_version=ENGINE,
            final_action_family="long",
            eval_status="unable",
            reason="missing_dataset_hashes",
            signal_session=date(2026, 8, 3),
        ),
    ).row

    assert evaluated.outcome == "hit" and evaluated.direction_correct is True
    assert observational.outcome is None
    assert observational.direction_correct is None
    assert observational.directional_return_pct is None
    assert observational.mfe_pct is None and observational.mae_pct is None
    assert unexecutable.execution_status == "unexecutable"
    assert unexecutable.stock_return_pct is None
    assert unable.execution_status == "unavailable"
    assert unable.dataset_hashes_json == "[]"

    pending_id = _seed_signal(db, index=8)
    pending = repository.persist_evaluation(
        signal_id=pending_id,
        evaluation=_pending(),
    ).row
    session = db.get_session()
    try:
        with pytest.raises(
            IntegrityError,
            match="decision outcome v2 (lineage mismatch|frozen fields are immutable)",
        ):
            session.connection().exec_driver_sql(
                "UPDATE decision_outcomes_v2 SET decision_profile = 'aggressive' "
                "WHERE id = ?",
                (pending.id,),
            )
        session.rollback()
        with pytest.raises(
            IntegrityError,
            match="decision outcome v2 dataset lineage invalid",
        ):
            session.connection().exec_driver_sql(
                "UPDATE decision_outcomes_v2 SET dataset_hashes_json = ? "
                "WHERE id = ?",
                (f'[\"{"b" * 64}\",\"{"a" * 64}\"]', pending.id),
            )
        session.rollback()
        with pytest.raises(
            IntegrityError,
            match="decision outcome v2 dataset lineage invalid",
        ):
            session.connection().exec_driver_sql(
                "UPDATE decision_outcomes_v2 SET dataset_hashes_json = ? "
                "WHERE id = ?",
                (f'[\"{"a" * 64}\"]', pending.id),
            )
        session.rollback()
        with pytest.raises(
            IntegrityError,
            match="decision outcome v2 dataset snapshot is restricted",
        ):
            session.connection().exec_driver_sql(
                "UPDATE research_dataset_snapshots SET content_hash = ? "
                "WHERE content_hash = ?",
                ("e" * 64, DATASET_HASH),
            )
        session.rollback()
        with pytest.raises(
            IntegrityError,
            match="decision outcome v2 dataset snapshot is restricted",
        ):
            session.connection().exec_driver_sql(
                "DELETE FROM research_dataset_snapshots WHERE content_hash = ?",
                (DATASET_HASH,),
            )
    finally:
        session.rollback()
        session.close()

    with pytest.raises(ValueError, match="sorted and unique"):
        repository.persist_evaluation(
            signal_id=pending_id,
            evaluation={
                **_pending().to_fields(),
                "dataset_hashes": ["b" * 64, "a" * 64],
            },
        )


def test_signal_fk_restricts_delete_and_v1_storage_is_independent(outcome_db) -> None:
    db, database_path = outcome_db
    signal_id = _seed_signal(db, index=9)
    row = DecisionOutcomeV2Repository(db).persist_evaluation(
        signal_id=signal_id,
        evaluation=_evaluated(),
    ).row
    db._engine.dispose()

    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        foreign_keys = connection.execute(
            "PRAGMA foreign_key_list('decision_outcomes_v2')"
        ).fetchall()
        assert any(
            item[2] == "decision_signals"
            and item[3] == "signal_id"
            and item[4] == "id"
            and item[6].upper() == "RESTRICT"
            for item in foreign_keys
        )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="decision outcome v2 signal is restricted",
        ):
            connection.execute(
                "DELETE FROM decision_signals WHERE id = ?", (signal_id,)
            )
        assert connection.execute(
            "SELECT eval_status FROM decision_outcomes_v2 WHERE id = ?", (row.id,)
        ).fetchone() == ("evaluated",)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name = 'decision_signal_outcomes'"
        ).fetchone() == ("decision_signal_outcomes",)


def test_concurrent_identical_writers_create_one_terminal_row(outcome_db) -> None:
    db, _path = outcome_db
    signal_id = _seed_signal(db, index=10)
    repository = DecisionOutcomeV2Repository(db)
    barrier = threading.Barrier(2)
    results = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            results.append(
                repository.persist_evaluation(
                    signal_id=signal_id,
                    evaluation=_evaluated(),
                )
            )
        except BaseException as exc:  # pragma: no cover - assertion reports it
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    assert sorted(item.disposition for item in results) == ["created", "unchanged"]
    with db.get_session() as session:
        count = session.execute(
            select(func.count(DecisionOutcomeV2Record.id)).where(
                DecisionOutcomeV2Record.signal_id == signal_id
            )
        ).scalar_one()
    assert count == 1
