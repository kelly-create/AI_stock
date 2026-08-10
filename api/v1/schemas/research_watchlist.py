"""API contracts for enhanced personal-research watchlist metadata."""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


ResearchMarket = Literal["cn", "hk", "us", "jp", "kr", "tw"]
ResearchAnalysisTier = Literal["quick", "standard", "deep"]
ResearchWatchlistSource = Literal["enhanced", "legacy", "holding"]


class ResearchWatchlistUpsertRequest(BaseModel):
    reason: Optional[str] = Field(..., max_length=4000)
    priority: int = Field(..., ge=0, le=100)
    analysis_tier: ResearchAnalysisTier
    next_review_at: Optional[datetime] = Field(...)

    @field_validator("priority", mode="before")
    @classmethod
    def reject_boolean_priority(cls, value):
        if isinstance(value, bool):
            raise ValueError("priority must be an integer")
        return value


class ResearchWatchlistItem(BaseModel):
    stock_code: str
    market: ResearchMarket
    sources: List[ResearchWatchlistSource] = Field(default_factory=list)
    reason: Optional[str] = None
    priority: int
    analysis_tier: ResearchAnalysisTier
    next_review_at: Optional[str] = None
    is_active: bool
    is_holding: bool


class ResearchWatchlistListResponse(BaseModel):
    items: List[ResearchWatchlistItem] = Field(default_factory=list)
    holdings_freshness: Literal["ledger"] = "ledger"


class ResearchWatchlistDeleteResponse(BaseModel):
    deleted: int
