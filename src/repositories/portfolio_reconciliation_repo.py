# -*- coding: utf-8 -*-
"""Persistence helpers for Portfolio opening/reconciliation events."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, delete, func, select

from src.storage import (
    DatabaseManager,
    PortfolioDailySnapshot,
    PortfolioPosition,
    PortfolioPositionLot,
    PortfolioReconciliationAdjustmentRecord,
    PortfolioReconciliationRecord,
    utc_naive_now,
)


class PortfolioReconciliationRepository:
    """Read/write reconciliation rows on caller-owned Portfolio transactions."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    @staticmethod
    def create_preview_in_session(
        *,
        session: Any,
        account_id: int,
        event_type: str,
        effective_date: date,
        preview_token_hash: str,
        input_hash: str,
        request_json: str,
        diff_json: str,
        note: Optional[str],
        expires_at: datetime,
    ) -> PortfolioReconciliationRecord:
        row = PortfolioReconciliationRecord(
            account_id=account_id,
            event_type=event_type,
            status="preview",
            effective_date=effective_date,
            preview_token=preview_token_hash,
            input_hash=input_hash,
            request_json=request_json,
            diff_json=diff_json,
            note=note,
            expires_at=expires_at,
        )
        session.add(row)
        session.flush()
        return row

    @staticmethod
    def get_by_token_hash_in_session(
        *,
        session: Any,
        account_id: int,
        preview_token_hash: str,
    ) -> Optional[PortfolioReconciliationRecord]:
        return session.execute(
            select(PortfolioReconciliationRecord)
            .where(
                and_(
                    PortfolioReconciliationRecord.account_id == account_id,
                    PortfolioReconciliationRecord.preview_token == preview_token_hash,
                )
            )
            .limit(1)
        ).scalar_one_or_none()

    @staticmethod
    def get_by_idempotency_key_in_session(
        *,
        session: Any,
        account_id: int,
        idempotency_key: str,
    ) -> Optional[PortfolioReconciliationRecord]:
        return session.execute(
            select(PortfolioReconciliationRecord)
            .where(
                and_(
                    PortfolioReconciliationRecord.account_id == account_id,
                    PortfolioReconciliationRecord.idempotency_key == idempotency_key,
                )
            )
            .limit(1)
        ).scalar_one_or_none()

    @staticmethod
    def has_applied_opening_in_session(*, session: Any, account_id: int) -> bool:
        count = session.execute(
            select(func.count())
            .select_from(PortfolioReconciliationRecord)
            .where(
                and_(
                    PortfolioReconciliationRecord.account_id == account_id,
                    PortfolioReconciliationRecord.event_type == "opening",
                    PortfolioReconciliationRecord.status == "applied",
                )
            )
        ).scalar_one()
        return int(count or 0) > 0

    @staticmethod
    def has_any_applied_in_session(*, session: Any, account_id: int) -> bool:
        count = session.execute(
            select(func.count())
            .select_from(PortfolioReconciliationRecord)
            .where(
                and_(
                    PortfolioReconciliationRecord.account_id == account_id,
                    PortfolioReconciliationRecord.status == "applied",
                )
            )
        ).scalar_one()
        return int(count or 0) > 0

    @staticmethod
    def next_event_version_in_session(*, session: Any, account_id: int) -> int:
        current = session.execute(
            select(func.max(PortfolioReconciliationRecord.event_version)).where(
                and_(
                    PortfolioReconciliationRecord.account_id == account_id,
                    PortfolioReconciliationRecord.status == "applied",
                )
            )
        ).scalar_one()
        return int(current or 0) + 1

    @staticmethod
    def apply_preview_in_session(
        *,
        session: Any,
        row: PortfolioReconciliationRecord,
        event_version: int,
        idempotency_key: str,
        adjustments: List[Dict[str, Any]],
    ) -> PortfolioReconciliationRecord:
        for item in adjustments:
            session.add(
                PortfolioReconciliationAdjustmentRecord(
                    reconciliation_id=int(row.id),
                    account_id=int(row.account_id),
                    identity_key=item["identity_key"],
                    adjustment_type=item["adjustment_type"],
                    stock_code=item.get("stock_code"),
                    market=item.get("market"),
                    currency=item["currency"],
                    quantity_delta=float(item.get("quantity_delta") or 0.0),
                    total_cost_delta=float(item.get("total_cost_delta") or 0.0),
                    cash_delta=float(item.get("cash_delta") or 0.0),
                    before_json=item["before_json"],
                    after_json=item["after_json"],
                )
            )
        # The schema closes child mutation once the parent is applied.  Flush
        # children while the preview is still open, then seal the header in a
        # second flush inside the same BEGIN IMMEDIATE transaction.
        session.flush()
        row.status = "applied"
        row.event_version = event_version
        row.idempotency_key = idempotency_key
        row.applied_at = utc_naive_now()
        session.flush()
        PortfolioReconciliationRepository.invalidate_account_cache_in_session(
            session=session,
            account_id=int(row.account_id),
            from_date=row.effective_date,
        )
        return row

    @staticmethod
    def mark_expired_in_session(*, session: Any, row: PortfolioReconciliationRecord) -> None:
        if row.status == "preview":
            row.status = "expired"
            session.flush()

    @staticmethod
    def expire_due_previews_in_session(
        *,
        session: Any,
        account_id: int,
        now: datetime,
    ) -> int:
        rows = session.execute(
            select(PortfolioReconciliationRecord).where(
                and_(
                    PortfolioReconciliationRecord.account_id == account_id,
                    PortfolioReconciliationRecord.status == "preview",
                    PortfolioReconciliationRecord.expires_at <= now,
                )
            )
        ).scalars().all()
        for row in rows:
            row.status = "expired"
        if rows:
            session.flush()
        return len(rows)

    @staticmethod
    def invalidate_account_cache_in_session(
        *,
        session: Any,
        account_id: int,
        from_date: date,
    ) -> None:
        session.execute(delete(PortfolioPositionLot).where(PortfolioPositionLot.account_id == account_id))
        session.execute(delete(PortfolioPosition).where(PortfolioPosition.account_id == account_id))
        session.execute(
            delete(PortfolioDailySnapshot).where(
                and_(
                    PortfolioDailySnapshot.account_id == account_id,
                    PortfolioDailySnapshot.snapshot_date >= from_date,
                )
            )
        )

    def list_applied_events(
        self,
        *,
        account_id: int,
        as_of: date,
        session: Optional[Any] = None,
    ) -> List[Tuple[PortfolioReconciliationRecord, List[PortfolioReconciliationAdjustmentRecord]]]:
        if session is not None:
            return self.list_applied_events_in_session(
                session=session,
                account_id=account_id,
                as_of=as_of,
            )
        with self.db.get_session() as owned_session:
            result = self.list_applied_events_in_session(
                session=owned_session,
                account_id=account_id,
                as_of=as_of,
            )
            for header, adjustments in result:
                owned_session.expunge(header)
                for adjustment in adjustments:
                    owned_session.expunge(adjustment)
            return result

    @staticmethod
    def list_applied_events_in_session(
        *,
        session: Any,
        account_id: int,
        as_of: date,
    ) -> List[Tuple[PortfolioReconciliationRecord, List[PortfolioReconciliationAdjustmentRecord]]]:
        headers = session.execute(
            select(PortfolioReconciliationRecord)
            .where(
                and_(
                    PortfolioReconciliationRecord.account_id == account_id,
                    PortfolioReconciliationRecord.status == "applied",
                    PortfolioReconciliationRecord.effective_date <= as_of,
                )
            )
            .order_by(
                PortfolioReconciliationRecord.effective_date.asc(),
                PortfolioReconciliationRecord.event_version.asc(),
                PortfolioReconciliationRecord.id.asc(),
            )
        ).scalars().all()
        if not headers:
            return []
        header_ids = [int(row.id) for row in headers]
        adjustment_rows = session.execute(
            select(PortfolioReconciliationAdjustmentRecord)
            .where(PortfolioReconciliationAdjustmentRecord.reconciliation_id.in_(header_ids))
            .order_by(
                PortfolioReconciliationAdjustmentRecord.reconciliation_id.asc(),
                PortfolioReconciliationAdjustmentRecord.id.asc(),
            )
        ).scalars().all()
        grouped: Dict[int, List[PortfolioReconciliationAdjustmentRecord]] = {
            header_id: [] for header_id in header_ids
        }
        for adjustment in adjustment_rows:
            grouped[int(adjustment.reconciliation_id)].append(adjustment)
        return [(header, grouped[int(header.id)]) for header in headers]

    def list_records(
        self,
        *,
        account_id: int,
        include_previews: bool,
    ) -> List[PortfolioReconciliationRecord]:
        with self.db.get_session() as session:
            query = select(PortfolioReconciliationRecord).where(
                PortfolioReconciliationRecord.account_id == account_id
            )
            if not include_previews:
                query = query.where(PortfolioReconciliationRecord.status == "applied")
            rows = session.execute(
                query.order_by(
                    PortfolioReconciliationRecord.effective_date.desc(),
                    PortfolioReconciliationRecord.event_version.desc(),
                    PortfolioReconciliationRecord.id.desc(),
                )
            ).scalars().all()
            for row in rows:
                session.expunge(row)
            return list(rows)

    def get_record(
        self,
        *,
        account_id: int,
        reconciliation_id: int,
    ) -> Optional[Tuple[PortfolioReconciliationRecord, List[PortfolioReconciliationAdjustmentRecord]]]:
        with self.db.get_session() as session:
            header = session.execute(
                select(PortfolioReconciliationRecord)
                .where(
                    and_(
                        PortfolioReconciliationRecord.id == reconciliation_id,
                        PortfolioReconciliationRecord.account_id == account_id,
                    )
                )
                .limit(1)
            ).scalar_one_or_none()
            if header is None:
                return None
            adjustments = session.execute(
                select(PortfolioReconciliationAdjustmentRecord)
                .where(
                    PortfolioReconciliationAdjustmentRecord.reconciliation_id == reconciliation_id
                )
                .order_by(PortfolioReconciliationAdjustmentRecord.id.asc())
            ).scalars().all()
            session.expunge(header)
            for adjustment in adjustments:
                session.expunge(adjustment)
            return header, list(adjustments)
