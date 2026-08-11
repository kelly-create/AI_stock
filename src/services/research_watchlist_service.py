# -*- coding: utf-8 -*-
"""Enhanced watchlist metadata and effective personal-research universe."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from data_provider.base import canonical_stock_code, normalize_stock_code
from src.core.trading_calendar import get_market_for_stock
from src.repositories.research_watchlist_repo import ResearchWatchlistRepository
from src.services.stock_list_parser import ParseStatus, parse_analysis_target
from src.storage import utc_naive_now


VALID_WATCHLIST_MARKETS = {"cn", "hk", "us", "jp", "kr", "tw"}
VALID_ANALYSIS_TIERS = {"quick", "standard", "deep"}
WATCHLIST_SOURCE_ORDER = {"enhanced": 0, "legacy": 1, "holding": 2}


class ResearchWatchlistConflictError(Exception):
    """Raised when a watchlist write conflicts with concurrent state."""


def normalize_research_identity(
    stock_code: str,
    market: Optional[str] = None,
) -> Tuple[str, str]:
    """Return the stable ``(market, stock_code)`` identity used by PR3.

    The parser is deliberately shared with the analysis entrypoint so the
    enhanced watchlist does not introduce a narrower code grammar than the
    analysis pipeline itself.
    """

    raw = (stock_code or "").strip()
    if not raw:
        raise ValueError("stock_code is required")
    explicit_market = (market or "").strip().lower() or None
    if explicit_market is not None and explicit_market not in VALID_WATCHLIST_MARKETS:
        raise ValueError("market must be one of: cn, hk, us, jp, kr, tw")

    raw_target = parse_analysis_target(raw)
    if explicit_market is None and raw_target.asset_type != ParseStatus.STOCK:
        reason = raw_target.unsupported_reason or "only stock targets are supported"
        raise ValueError(f"Unsupported stock_code: {reason}")

    if explicit_market is not None:
        if raw_target.asset_type == ParseStatus.INDEX:
            raise ValueError("stock_code must resolve to a stock target")
        raw_upper = raw.upper()
        normalized = canonical_stock_code(normalize_stock_code(raw))
        inferred_market = get_market_for_stock(normalized)
        has_explicit_exchange = (
            "." in raw_upper
            or raw_upper.startswith(("HK", "SH", "SZ", "SS", "BJ"))
            or any(character.isalpha() for character in raw_upper)
        )
        if (
            inferred_market is not None
            and inferred_market != explicit_market
            and has_explicit_exchange
        ):
            raise ValueError(
                f"market {explicit_market!r} conflicts with stock_code market {inferred_market!r}"
            )

        if explicit_market == "cn":
            candidate = normalize_stock_code(raw).upper()
            if not candidate.isdigit() or len(candidate) != 6:
                raise ValueError("cn stock_code must be a 6-digit stock identity")
        elif explicit_market == "hk":
            digits = normalized[2:] if normalized.startswith("HK") else normalized
            if not digits.isdigit() or not 1 <= len(digits) <= 5:
                raise ValueError("hk stock_code must be a 1-5 digit stock identity")
            candidate = f"HK{digits.zfill(5)}"
        elif explicit_market == "jp":
            if normalized.endswith(".T"):
                base = normalized[:-2]
            elif normalized.isdigit():
                base = normalized
            else:
                raise ValueError("jp stock_code must use a numeric code or .T suffix")
            if not base.isdigit() or len(base) not in {4, 5}:
                raise ValueError("jp stock_code must contain 4-5 digits")
            candidate = f"{base}.T"
        elif explicit_market == "kr":
            suffix = next((item for item in (".KS", ".KQ") if normalized.endswith(item)), None)
            base = normalized[: -len(suffix)] if suffix else normalized
            if not base.isdigit() or len(base) != 6:
                raise ValueError("kr stock_code must contain 6 digits")
            # Market-only broker rows cannot distinguish KOSPI/KOSDAQ. Keep a
            # deterministic KOSPI default while preserving an explicit .KQ.
            candidate = f"{base}{suffix or '.KS'}"
        elif explicit_market == "tw":
            suffix = next((item for item in (".TWO", ".TW") if normalized.endswith(item)), None)
            base = normalized[: -len(suffix)] if suffix else normalized
            if not base.isdigit() or not 4 <= len(base) <= 6:
                raise ValueError("tw stock_code must contain 4-6 digits")
            # Market-only broker rows default to TWSE; callers can preserve
            # TPEx identity by supplying the explicit .TWO suffix.
            candidate = f"{base}{suffix or '.TW'}"
        else:
            candidate = normalized
            if get_market_for_stock(candidate) != "us":
                raise ValueError("us stock_code must be a supported ticker identity")

        canonical_target = parse_analysis_target(candidate)
        if canonical_target.asset_type != ParseStatus.STOCK:
            raise ValueError("stock_code must resolve to a stock target")
        return explicit_market, canonical_stock_code(candidate)

    target = raw_target
    normalized = canonical_stock_code(normalize_stock_code(raw))
    inferred_market = get_market_for_stock(normalized)
    if inferred_market is None:
        exchange_market = {
            "SH": "cn",
            "SZ": "cn",
            "BJ": "cn",
            "HK": "hk",
            "US": "us",
        }.get(str(target.exchange or "").upper())
        inferred_market = exchange_market
    resolved_market = inferred_market
    if resolved_market is None:
        raise ValueError("Unable to infer market from stock_code; provide market explicitly")

    if resolved_market == "cn":
        normalized = normalize_stock_code(raw).upper()
    elif resolved_market == "hk":
        normalized = normalize_stock_code(raw).upper()
        if normalized.isdigit():
            normalized = f"HK{normalized.zfill(5)}"
    else:
        normalized = canonical_stock_code(normalized)
    if not normalized:
        raise ValueError("stock_code is required")
    return resolved_market, normalized


class ResearchWatchlistService:
    """Resolve enhanced metadata, legacy membership, and ledger holdings."""

    def __init__(
        self,
        repo: Optional[ResearchWatchlistRepository] = None,
        portfolio_service: Optional[Any] = None,
    ) -> None:
        self.repo = repo or ResearchWatchlistRepository()
        self._portfolio_service = portfolio_service

    def list_watchlist(self, *, include_inactive: bool = False) -> Dict[str, Any]:
        rows = self.repo.list_items(include_inactive=include_inactive)
        items = [
            self._row_to_item(row, sources=["enhanced"], is_holding=False)
            for row in rows
            if str(row.source) != "legacy"
        ]
        return {
            "items": self._sort_items(items),
            "holdings_freshness": "ledger",
        }

    def upsert_item(
        self,
        *,
        stock_code: str,
        market: Optional[str],
        reason: Optional[str],
        priority: int,
        analysis_tier: str,
        next_review_at: Optional[datetime],
        source: str = "manual",
    ) -> Dict[str, Any]:
        resolved_market, normalized_code = normalize_research_identity(stock_code, market)
        priority_value = int(priority)
        if isinstance(priority, bool) or priority_value < 0 or priority_value > 100:
            raise ValueError("priority must be an integer in [0, 100]")
        tier = (analysis_tier or "").strip().lower()
        if tier not in VALID_ANALYSIS_TIERS:
            raise ValueError("analysis_tier must be quick, standard, or deep")
        source_value = (source or "manual").strip().lower()
        if not source_value or len(source_value) > 32:
            raise ValueError("source must be 1-32 characters")
        reason_value = (reason or "").strip() or None
        if reason_value is not None and len(reason_value) > 4000:
            raise ValueError("reason must be at most 4000 characters")
        review_value = next_review_at
        if review_value is not None and review_value.tzinfo is not None:
            review_value = review_value.astimezone(timezone.utc).replace(tzinfo=None)

        row = self.repo.upsert_item(
            market=resolved_market,
            stock_code=normalized_code,
            source=source_value,
            reason=reason_value,
            priority=priority_value,
            analysis_tier=tier,
            next_review_at=review_value,
            is_active=True,
        )
        return self._row_to_item(row, sources=["enhanced"], is_holding=False)

    def set_active(
        self,
        *,
        stock_code: str,
        market: Optional[str],
        active: bool,
        source: str = "manual",
    ) -> Dict[str, Any]:
        resolved_market, normalized_code = normalize_research_identity(stock_code, market)
        row = self.repo.set_active(
            market=resolved_market,
            stock_code=normalized_code,
            active=bool(active),
            source=source,
        )
        return self._row_to_item(row, sources=["enhanced"], is_holding=False)

    def build_effective_universe(
        self,
        *,
        legacy_codes: Iterable[str],
        as_of: Optional[date] = None,
    ) -> Dict[str, Any]:
        """Merge overlay, legacy setting, and current ledger-held identities.

        An inactive overlay row is a tombstone for the legacy source, while a
        live holding remains effective regardless of watchlist membership.
        """

        rows = self.repo.list_items(include_inactive=True)
        overlay: Dict[Tuple[str, str], Any] = {
            (str(row.market), str(row.stock_code)): row for row in rows
        }
        merged: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for identity, row in overlay.items():
            if not bool(row.is_active) or str(row.source) == "legacy":
                continue
            merged[identity] = self._row_to_item(
                row,
                sources=["enhanced"],
                is_holding=False,
            )

        for raw_code in legacy_codes:
            try:
                identity = normalize_research_identity(str(raw_code))
            except ValueError:
                continue
            row = overlay.get(identity)
            if row is not None and not bool(row.is_active):
                continue
            item = merged.get(identity)
            if item is None:
                if row is not None:
                    item = self._row_to_item(row, sources=[], is_holding=False)
                else:
                    item = self._virtual_item(identity, is_active=True)
                merged[identity] = item
            self._append_source(item, "legacy")

        for market, stock_code in self._list_ledger_holdings(as_of=as_of):
            try:
                identity = normalize_research_identity(stock_code, market)
            except ValueError:
                continue
            row = overlay.get(identity)
            item = merged.get(identity)
            if item is None:
                if row is not None:
                    item = self._row_to_item(row, sources=[], is_holding=True)
                else:
                    item = self._virtual_item(identity, is_active=False)
                merged[identity] = item
            item["is_holding"] = True
            self._append_source(item, "holding")

        return {
            "items": self._sort_items(list(merged.values())),
            "holdings_freshness": "ledger",
        }

    def _list_ledger_holdings(self, *, as_of: Optional[date]) -> List[Tuple[str, str]]:
        service = self._portfolio_service
        if service is None:
            from src.services.portfolio_service import PortfolioService

            service = PortfolioService()
        return list(service.list_open_position_identities(as_of=as_of or date.today()))

    @staticmethod
    def _row_to_item(row: Any, *, sources: List[str], is_holding: bool) -> Dict[str, Any]:
        return {
            "stock_code": str(row.stock_code),
            "market": str(row.market),
            "sources": sorted(set(sources), key=lambda item: WATCHLIST_SOURCE_ORDER[item]),
            "reason": row.reason,
            "priority": int(row.priority),
            "analysis_tier": str(row.analysis_tier),
            "next_review_at": f"{row.next_review_at.isoformat()}Z" if row.next_review_at else None,
            "is_active": bool(row.is_active),
            "is_holding": bool(is_holding),
        }

    @staticmethod
    def _virtual_item(identity: Tuple[str, str], *, is_active: bool) -> Dict[str, Any]:
        market, stock_code = identity
        return {
            "stock_code": stock_code,
            "market": market,
            "sources": [],
            "reason": None,
            "priority": 50,
            "analysis_tier": "quick",
            "next_review_at": None,
            "is_active": bool(is_active),
            "is_holding": False,
        }

    @staticmethod
    def _append_source(item: Dict[str, Any], source: str) -> None:
        sources = set(item.get("sources") or [])
        sources.add(source)
        item["sources"] = sorted(sources, key=lambda value: WATCHLIST_SOURCE_ORDER[value])

    @staticmethod
    def _sort_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        now = utc_naive_now()

        def sort_key(item: Dict[str, Any]):
            raw_review = item.get("next_review_at")
            review_at: Optional[datetime] = None
            if isinstance(raw_review, str) and raw_review:
                try:
                    normalized_review = raw_review[:-1] + "+00:00" if raw_review.endswith("Z") else raw_review
                    review_at = datetime.fromisoformat(normalized_review)
                    if review_at.tzinfo is not None:
                        review_at = review_at.astimezone(timezone.utc).replace(tzinfo=None)
                except ValueError:
                    review_at = None
            due_rank = 0 if review_at is not None and review_at <= now else 1
            review_rank = review_at or datetime.max
            return (
                due_rank,
                review_rank,
                -int(item.get("priority") or 0),
                str(item.get("market") or ""),
                str(item.get("stock_code") or ""),
            )

        return sorted(items, key=sort_key)
