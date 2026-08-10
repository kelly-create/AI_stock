# -*- coding: utf-8 -*-
"""Repository for enhanced personal-research watchlist metadata."""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import and_, select

from src.storage import DatabaseManager, ResearchWatchlistItemRecord, utc_naive_now


class ResearchWatchlistRepository:
    """Persist the metadata overlay used by the effective research universe."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def list_items(self, *, include_inactive: bool = False) -> List[ResearchWatchlistItemRecord]:
        with self.db.get_session() as session:
            query = select(ResearchWatchlistItemRecord)
            if not include_inactive:
                query = query.where(ResearchWatchlistItemRecord.is_active.is_(True))
            rows = session.execute(
                query.order_by(
                    ResearchWatchlistItemRecord.priority.desc(),
                    ResearchWatchlistItemRecord.id.asc(),
                )
            ).scalars().all()
            for row in rows:
                session.expunge(row)
            return list(rows)

    def get_item(
        self,
        *,
        market: str,
        stock_code: str,
    ) -> Optional[ResearchWatchlistItemRecord]:
        with self.db.get_session() as session:
            row = self.get_item_in_session(
                session=session,
                market=market,
                stock_code=stock_code,
            )
            if row is not None:
                session.expunge(row)
            return row

    @staticmethod
    def get_item_in_session(
        *,
        session,
        market: str,
        stock_code: str,
    ) -> Optional[ResearchWatchlistItemRecord]:
        return session.execute(
            select(ResearchWatchlistItemRecord)
            .where(
                and_(
                    ResearchWatchlistItemRecord.market == market,
                    ResearchWatchlistItemRecord.stock_code == stock_code,
                )
            )
            .limit(1)
        ).scalar_one_or_none()

    def upsert_item(
        self,
        *,
        market: str,
        stock_code: str,
        source: str,
        reason: Optional[str],
        priority: int,
        analysis_tier: str,
        next_review_at: Optional[datetime],
        is_active: bool,
    ) -> ResearchWatchlistItemRecord:
        with self.db.get_session() as session:
            row = self.get_item_in_session(
                session=session,
                market=market,
                stock_code=stock_code,
            )
            if row is None:
                row = ResearchWatchlistItemRecord(
                    market=market,
                    stock_code=stock_code,
                    source=source,
                    reason=reason,
                    priority=priority,
                    analysis_tier=analysis_tier,
                    next_review_at=next_review_at,
                    is_active=is_active,
                )
                session.add(row)
            else:
                row.source = source
                row.reason = reason
                row.priority = priority
                row.analysis_tier = analysis_tier
                row.next_review_at = next_review_at
                row.is_active = is_active
                row.updated_at = utc_naive_now()
            session.commit()
            session.refresh(row)
            session.expunge(row)
            return row

    def set_active(
        self,
        *,
        market: str,
        stock_code: str,
        active: bool,
        source: str = "manual",
    ) -> ResearchWatchlistItemRecord:
        """Set overlay membership, creating an explicit tombstone when absent."""

        with self.db.get_session() as session:
            row = self.get_item_in_session(
                session=session,
                market=market,
                stock_code=stock_code,
            )
            if row is None:
                row = ResearchWatchlistItemRecord(
                    market=market,
                    stock_code=stock_code,
                    source=source,
                    priority=50,
                    analysis_tier="quick",
                    is_active=active,
                )
                session.add(row)
            else:
                row.is_active = active
                if active and row.source == "legacy":
                    row.source = source
                row.updated_at = utc_naive_now()
            session.commit()
            session.refresh(row)
            session.expunge(row)
            return row
