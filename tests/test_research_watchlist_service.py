"""PR3 enhanced watchlist merge semantics."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest

from src.services.research_watchlist_service import (
    ResearchWatchlistService,
    normalize_research_identity,
)
from src.storage import utc_naive_now


class FakeWatchlistRepo:
    def __init__(self, rows):
        self.rows = list(rows)

    def list_items(self, *, include_inactive=False):
        if include_inactive:
            return list(self.rows)
        return [row for row in self.rows if row.is_active]


class FakePortfolioService:
    def __init__(self, identities):
        self.identities = list(identities)

    def list_open_position_identities(self, *, as_of):
        assert isinstance(as_of, date)
        return list(self.identities)


def row(code, market, *, active=True, priority=50, review=None, source="manual"):
    return SimpleNamespace(
        id=1,
        stock_code=code,
        market=market,
        source=source,
        reason="tracked",
        priority=priority,
        analysis_tier="standard",
        next_review_at=review,
        is_active=active,
    )


def test_effective_universe_uses_tombstone_but_never_hides_holding() -> None:
    service = ResearchWatchlistService(
        repo=FakeWatchlistRepo(
            [
                row("600519", "cn", active=False),
                row("AAPL", "us", active=True, priority=80),
            ]
        ),
        portfolio_service=FakePortfolioService([("cn", "600519"), ("hk", "HK00700")]),
    )

    result = service.build_effective_universe(
        legacy_codes=["600519", "aapl", "00700.HK"],
        as_of=date(2026, 8, 10),
    )
    by_code = {item["stock_code"]: item for item in result["items"]}

    assert result["holdings_freshness"] == "ledger"
    assert by_code["600519"]["sources"] == ["holding"]
    assert by_code["600519"]["is_active"] is False
    assert by_code["600519"]["is_holding"] is True
    assert by_code["AAPL"]["sources"] == ["enhanced", "legacy"]
    assert by_code["HK00700"]["sources"] == ["legacy", "holding"]


def test_legacy_and_enhanced_are_independent_sources_but_tombstone_wins() -> None:
    service = ResearchWatchlistService(
        repo=FakeWatchlistRepo(
            [
                row("AAPL", "us", active=True, source="manual"),
                row("MSFT", "us", active=True, source="legacy"),
                row("NVDA", "us", active=False, source="manual"),
            ]
        ),
        portfolio_service=FakePortfolioService([]),
    )

    result = service.build_effective_universe(
        # Simulates a direct Settings update: legacy membership changes while
        # explicit enhanced metadata/tombstones remain independent.
        legacy_codes=["MSFT", "NVDA"],
        as_of=date(2026, 8, 10),
    )
    by_code = {item["stock_code"]: item for item in result["items"]}

    assert by_code["AAPL"]["sources"] == ["enhanced"]
    assert by_code["MSFT"]["sources"] == ["legacy"]
    assert "NVDA" not in by_code
    listed = service.list_watchlist(include_inactive=True)
    assert {item["stock_code"] for item in listed["items"]} == {"AAPL", "NVDA"}


def test_effective_universe_orders_due_then_priority_with_stable_identity() -> None:
    now = datetime.now()
    service = ResearchWatchlistService(
        repo=FakeWatchlistRepo(
            [
                row("MSFT", "us", priority=100, review=now + timedelta(days=1)),
                row("AAPL", "us", priority=20, review=now - timedelta(days=1)),
                row("600519", "cn", priority=90, review=now - timedelta(days=1)),
            ]
        ),
        portfolio_service=FakePortfolioService([]),
    )

    result = service.build_effective_universe(legacy_codes=[])

    assert [item["stock_code"] for item in result["items"]] == ["600519", "AAPL", "MSFT"]


def test_next_review_uses_utc_naive_storage_and_emits_explicit_utc() -> None:
    now = utc_naive_now()
    service = ResearchWatchlistService(
        repo=FakeWatchlistRepo(
            [
                row("MSFT", "us", priority=100, review=now + timedelta(hours=1)),
                row("AAPL", "us", priority=1, review=now - timedelta(minutes=1)),
            ]
        ),
        portfolio_service=FakePortfolioService([]),
    )

    result = service.build_effective_universe(legacy_codes=[])

    assert [item["stock_code"] for item in result["items"]] == ["AAPL", "MSFT"]
    assert all(item["next_review_at"].endswith("Z") for item in result["items"])


@pytest.mark.parametrize(
    ("raw", "market", "expected"),
    [
        ("600519.SH", None, ("cn", "600519")),
        ("hk700", None, ("hk", "HK00700")),
        ("aapl", None, ("us", "AAPL")),
        ("7203.T", None, ("jp", "7203.T")),
        ("700", "hk", ("hk", "HK00700")),
        ("7203", "jp", ("jp", "7203.T")),
        ("005930", "kr", ("kr", "005930.KS")),
        ("035720.KQ", "kr", ("kr", "035720.KQ")),
        ("2330", "tw", ("tw", "2330.TW")),
        ("6505.TWO", "tw", ("tw", "6505.TWO")),
    ],
)
def test_identity_normalization_matches_analysis_markets(raw, market, expected) -> None:
    assert normalize_research_identity(raw, market) == expected


def test_identity_rejects_explicit_market_conflict() -> None:
    with pytest.raises(ValueError, match="conflicts"):
        normalize_research_identity("AAPL", "cn")


def test_effective_universe_keeps_market_aware_portfolio_holdings() -> None:
    service = ResearchWatchlistService(
        repo=FakeWatchlistRepo([]),
        portfolio_service=FakePortfolioService(
            [
                ("hk", "700"),
                ("jp", "7203"),
                ("kr", "005930"),
                ("tw", "2330"),
            ]
        ),
    )

    result = service.build_effective_universe(legacy_codes=[], as_of=date(2026, 8, 10))

    assert {(item["market"], item["stock_code"]) for item in result["items"]} == {
        ("hk", "HK00700"),
        ("jp", "7203.T"),
        ("kr", "005930.KS"),
        ("tw", "2330.TW"),
    }
    assert all(item["sources"] == ["holding"] for item in result["items"])
