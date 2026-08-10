# -*- coding: utf-8 -*-
"""Durable daily budget reservations for personal research work."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import and_, func, select
from sqlalchemy.exc import OperationalError

from src.storage import DatabaseManager, ResearchBudgetReservationRecord, utc_naive_now


class ResearchBudgetBusyError(RuntimeError):
    """Raised when the SQLite reservation transaction cannot be serialized."""


class ResearchBudgetConflictError(RuntimeError):
    """Raised when an idempotency identity is reused with different facts."""


@dataclass(frozen=True)
class ResearchBudgetReservationResult:
    row: ResearchBudgetReservationRecord
    created: bool


class ResearchBudgetRepository:
    """Atomic count-and-reserve operations under ``BEGIN IMMEDIATE``."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self.db = db_manager or DatabaseManager.get_instance()

    def reserve(
        self,
        *,
        task_id: str,
        budget_date: date,
        stock_code: str,
        market: str,
        mode: str,
        bucket: str,
        trigger_source: str,
        priority: int,
        manual_daily_override: bool,
        daily_limit: int,
    ) -> ResearchBudgetReservationResult:
        session = self.db.get_session()
        try:
            try:
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            except OperationalError as exc:
                raise ResearchBudgetBusyError(
                    "Research budget ledger is busy; retry the durable task."
                ) from exc

            existing = session.execute(
                select(ResearchBudgetReservationRecord)
                .where(
                    and_(
                        ResearchBudgetReservationRecord.task_id == task_id,
                        ResearchBudgetReservationRecord.market == market,
                        ResearchBudgetReservationRecord.stock_code == stock_code,
                        ResearchBudgetReservationRecord.bucket == bucket,
                    )
                )
                .limit(1)
            ).scalar_one_or_none()
            if existing is not None:
                expected = {
                    "mode": mode,
                    "trigger_source": trigger_source,
                    "priority": priority,
                    "manual_daily_override": manual_daily_override,
                }
                mismatched = [
                    field_name
                    for field_name, expected_value in expected.items()
                    if getattr(existing, field_name) != expected_value
                ]
                if mismatched:
                    raise ResearchBudgetConflictError(
                        "Research budget identity conflicts with immutable fields: "
                        + ",".join(sorted(mismatched))
                    )
                if existing.status == "released":
                    raise ResearchBudgetConflictError(
                        "A released reservation cannot be reused; create a new durable task."
                    )
                session.commit()
                session.refresh(existing)
                session.expunge(existing)
                return ResearchBudgetReservationResult(existing, created=False)

            used = int(
                session.execute(
                    select(func.count(ResearchBudgetReservationRecord.id)).where(
                        and_(
                            ResearchBudgetReservationRecord.budget_date == budget_date,
                            ResearchBudgetReservationRecord.bucket == bucket,
                            ResearchBudgetReservationRecord.status.in_(("reserved", "consumed")),
                        )
                    )
                ).scalar()
                or 0
            )
            if not manual_daily_override and used >= daily_limit:
                raise ResearchBudgetConflictError(
                    f"Daily research budget exhausted for {bucket}: {used}/{daily_limit}"
                )

            row = ResearchBudgetReservationRecord(
                task_id=task_id,
                budget_date=budget_date,
                stock_code=stock_code,
                market=market,
                mode=mode,
                bucket=bucket,
                trigger_source=trigger_source,
                priority=priority,
                manual_daily_override=manual_daily_override,
                status="reserved",
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            session.expunge(row)
            return ResearchBudgetReservationResult(row, created=True)
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()

    def transition(
        self,
        *,
        reservation_id: int,
        target_status: str,
    ) -> ResearchBudgetReservationRecord:
        if target_status not in {"consumed", "released"}:
            raise ValueError("target_status must be consumed or released")
        session = self.db.get_session()
        try:
            session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            row = session.execute(
                select(ResearchBudgetReservationRecord)
                .where(ResearchBudgetReservationRecord.id == reservation_id)
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                raise ValueError(f"Research budget reservation not found: {reservation_id}")
            if row.status == target_status:
                session.commit()
                session.refresh(row)
                session.expunge(row)
                return row
            if row.status != "reserved":
                raise ResearchBudgetConflictError(
                    f"Cannot transition research budget from {row.status} to {target_status}"
                )
            row.status = target_status
            transitioned_at = utc_naive_now()
            if row.updated_at is not None:
                transitioned_at = max(
                    transitioned_at,
                    row.updated_at + timedelta(milliseconds=2),
                )
            row.updated_at = transitioned_at
            session.commit()
            session.refresh(row)
            session.expunge(row)
            return row
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()
