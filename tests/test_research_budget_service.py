from __future__ import annotations

import os
import threading
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select, text

from src.config import Config
from src.repositories.research_budget_repo import ResearchBudgetConflictError
from src.services.research_budget_service import ResearchBudgetService
from src.storage import DatabaseManager, ResearchBudgetReservationRecord


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(tmp_path / "research-budget.db")
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


def _config(*, quick=50, standard_deep=20, debate=8) -> Config:
    return Config(
        stock_list=["600519"],
        research_quick_daily_budget=quick,
        research_standard_deep_daily_budget=standard_deep,
        research_debate_daily_budget=debate,
    )


@pytest.mark.parametrize(
    ("priority", "expected"),
    [(0, "quick"), (39, "quick"), (40, "standard"), (79, "standard"), (80, "deep"), (100, "deep")],
)
def test_auto_mode_resolution_is_deterministic(priority, expected) -> None:
    assert ResearchBudgetService.resolve_mode("auto", priority=priority) == expected


def test_same_task_can_reserve_multiple_stocks_and_debate_bucket(isolated_db) -> None:
    service = ResearchBudgetService(config=_config(), repo=None)
    service.repo.db = isolated_db
    today = date(2026, 8, 10)

    first = service.reserve(
        task_id="batch-1",
        stock_code="600519",
        market="cn",
        requested_mode="standard",
        trigger_source="schedule",
        budget_date=today,
    )
    second = service.reserve(
        task_id="batch-1",
        stock_code="000001",
        market="cn",
        requested_mode="deep",
        trigger_source="schedule",
        budget_date=today,
    )
    debate = service.reserve(
        task_id="batch-1",
        stock_code="600519",
        market="cn",
        requested_mode="debate",
        trigger_source="conflict_policy",
        budget_date=today,
    )

    assert first["bucket"] == second["bucket"] == "standard_deep"
    assert debate["bucket"] == "debate"
    with isolated_db.get_session() as session:
        rows = session.execute(select(ResearchBudgetReservationRecord)).scalars().all()
    assert len(rows) == 3


def test_idempotent_reservation_and_conflicting_reuse(isolated_db) -> None:
    service = ResearchBudgetService(config=_config())
    service.repo.db = isolated_db
    kwargs = dict(
        task_id="task-1",
        stock_code="600519",
        market="cn",
        requested_mode="quick",
        trigger_source="manual",
        priority=30,
        budget_date=date(2026, 8, 10),
    )

    first = service.reserve(**kwargs)
    second = service.reserve(**kwargs)

    assert first["created"] is True
    assert second["created"] is False
    assert first["id"] == second["id"]
    with pytest.raises(ResearchBudgetConflictError, match="immutable fields"):
        service.reserve(**{**kwargs, "priority": 31})


def test_retry_on_next_day_reuses_original_task_budget_date(isolated_db) -> None:
    service = ResearchBudgetService(config=_config())
    service.repo.db = isolated_db
    common = dict(
        task_id="task-cross-day",
        stock_code="600519",
        market="cn",
        requested_mode="deep",
        trigger_source="personal_research_api",
        priority=90,
    )

    first = service.reserve(**common, budget_date=date(2026, 8, 10))
    retried = service.reserve(**common, budget_date=date(2026, 8, 11))

    assert retried["created"] is False
    assert retried["id"] == first["id"]
    assert retried["budget_date"] == "2026-08-10"


def test_manual_override_only_bypasses_daily_counter(isolated_db) -> None:
    service = ResearchBudgetService(config=_config(quick=1))
    service.repo.db = isolated_db
    today = date(2026, 8, 10)
    common = dict(
        stock_code="600519",
        market="cn",
        requested_mode="quick",
        trigger_source="manual",
        budget_date=today,
    )
    service.reserve(task_id="task-1", **common)

    with pytest.raises(ResearchBudgetConflictError, match="exhausted"):
        service.reserve(task_id="task-2", **common)
    override = service.reserve(
        task_id="task-3",
        manual_daily_override=True,
        **common,
    )
    assert override["created"] is True
    assert override["manual_daily_override"] is True


def test_reservation_state_transitions_are_one_way(isolated_db) -> None:
    service = ResearchBudgetService(config=_config())
    service.repo.db = isolated_db
    reserved = service.reserve(
        task_id="task-state",
        stock_code="600519",
        market="cn",
        requested_mode="deep",
        trigger_source="manual",
        budget_date=date(2026, 8, 10),
    )

    assert service.consume(reserved["id"])["status"] == "consumed"
    assert service.consume(reserved["id"])["status"] == "consumed"
    with pytest.raises(ResearchBudgetConflictError, match="Cannot transition"):
        service.release(reserved["id"])


def test_database_rejects_budget_identity_mutation_and_delete(isolated_db) -> None:
    service = ResearchBudgetService(config=_config())
    service.repo.db = isolated_db
    reserved = service.reserve(
        task_id="task-immutable",
        stock_code="600519",
        market="cn",
        requested_mode="quick",
        trigger_source="manual",
        budget_date=date(2026, 8, 10),
    )
    service.consume(reserved["id"])

    with pytest.raises(Exception, match="research budget reservation is immutable"):
        with isolated_db.get_session() as session:
            session.execute(
                text(
                    "UPDATE research_budget_reservations "
                    "SET budget_date = '2026-08-11' WHERE id = :id"
                ),
                {"id": reserved["id"]},
            )
    with pytest.raises(Exception, match="research budget reservation is immutable"):
        with isolated_db.get_session() as session:
            session.execute(
                text(
                    "UPDATE research_budget_reservations "
                    "SET status = 'released' WHERE id = :id"
                ),
                {"id": reserved["id"]},
            )
    with pytest.raises(Exception, match="research budget reservation is immutable"):
        with isolated_db.get_session() as session:
            session.execute(
                text("DELETE FROM research_budget_reservations WHERE id = :id"),
                {"id": reserved["id"]},
            )


def test_database_requires_monotonic_transition_audit_time(isolated_db) -> None:
    service = ResearchBudgetService(config=_config())
    service.repo.db = isolated_db
    reserved = service.reserve(
        task_id="task-audit-time",
        stock_code="600519",
        market="cn",
        requested_mode="quick",
        trigger_source="manual",
        budget_date=date(2026, 8, 10),
    )

    with pytest.raises(Exception, match="research budget reservation is immutable"):
        with isolated_db.get_session() as session:
            session.execute(
                text(
                    "UPDATE research_budget_reservations "
                    "SET status = 'consumed' WHERE id = :id"
                ),
                {"id": reserved["id"]},
            )
    with pytest.raises(Exception, match="research budget reservation is immutable"):
        with isolated_db.get_session() as session:
            session.execute(
                text(
                    "UPDATE research_budget_reservations "
                    "SET updated_at = '2099-01-01 00:00:00' WHERE id = :id"
                ),
                {"id": reserved["id"]},
            )

    consumed = service.consume(reserved["id"])
    assert consumed["status"] == "consumed"
    assert consumed["updated_at"] > reserved["updated_at"]


def test_default_budget_date_uses_market_local_date(isolated_db, monkeypatch) -> None:
    service = ResearchBudgetService(config=_config())
    service.repo.db = isolated_db
    market_now = datetime(2026, 8, 10, 0, 1, tzinfo=ZoneInfo("Asia/Shanghai"))
    monkeypatch.setattr(
        "src.services.research_budget_service.get_market_now",
        lambda market: market_now if market == "cn" else None,
    )

    reserved = service.reserve(
        task_id="task-local-date",
        stock_code="600519",
        market="cn",
        requested_mode="quick",
        trigger_source="manual",
    )

    assert reserved["budget_date"] == "2026-08-10"


def test_released_identity_cannot_be_silently_reused(isolated_db) -> None:
    service = ResearchBudgetService(config=_config())
    service.repo.db = isolated_db
    kwargs = dict(
        task_id="task-released",
        stock_code="600519",
        market="cn",
        requested_mode="quick",
        trigger_source="manual",
        budget_date=date(2026, 8, 10),
    )
    reserved = service.reserve(**kwargs)
    service.release(reserved["id"])

    with pytest.raises(ResearchBudgetConflictError, match="cannot be reused"):
        service.reserve(**kwargs)


def test_concurrent_final_slot_has_one_winner(isolated_db) -> None:
    service = ResearchBudgetService(config=_config(quick=1))
    service.repo.db = isolated_db
    barrier = threading.Barrier(2)
    results = []

    def worker(task_id: str) -> None:
        try:
            barrier.wait(timeout=5)
            results.append(
                service.reserve(
                    task_id=task_id,
                    stock_code="600519",
                    market="cn",
                    requested_mode="quick",
                    trigger_source="schedule",
                    budget_date=date(2026, 8, 10),
                )
            )
        except BaseException as exc:  # assertion below reports exact type
            results.append(exc)

    threads = [threading.Thread(target=worker, args=(f"task-{index}",)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not any(thread.is_alive() for thread in threads)
    assert sum(isinstance(item, dict) for item in results) == 1
    assert sum(isinstance(item, ResearchBudgetConflictError) for item in results) == 1
