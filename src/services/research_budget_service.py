# -*- coding: utf-8 -*-
"""Deterministic research-mode resolution and daily budget accounting."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from data_provider.base import canonical_stock_code, normalize_stock_code
from src.config import Config, get_config
from src.core.trading_calendar import get_market_now
from src.repositories.research_budget_repo import ResearchBudgetRepository
from src.services.research.research_task_policy import resolve_research_task_mode


RESEARCH_MODES = frozenset({"auto", "quick", "standard", "deep", "debate"})
RESOLVED_RESEARCH_MODES = frozenset({"quick", "standard", "deep", "debate"})
MODE_BUCKETS = {
    "quick": "quick",
    "standard": "standard_deep",
    "deep": "standard_deep",
    "debate": "debate",
}


class ResearchBudgetService:
    """Reserve the approved Quick/Standard+Deep/Debate daily quotas."""

    def __init__(
        self,
        *,
        repo: Optional[ResearchBudgetRepository] = None,
        config: Optional[Config] = None,
    ) -> None:
        self.repo = repo or ResearchBudgetRepository()
        self.config = config or get_config()

    @staticmethod
    def resolve_mode(requested_mode: str, *, priority: int) -> str:
        return resolve_research_task_mode(
            requested_mode,
            priority=priority,
        ).resolved_mode

    def reserve(
        self,
        *,
        task_id: str,
        stock_code: str,
        market: str,
        requested_mode: str = "auto",
        trigger_source: str,
        priority: int = 50,
        manual_daily_override: bool = False,
        budget_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        task = str(task_id or "").strip()
        if not task or len(task) > 64:
            raise ValueError("task_id is required and must be at most 64 characters")
        market_norm = str(market or "").strip().lower()
        if market_norm not in {"cn", "hk", "us", "jp", "kr", "tw"}:
            raise ValueError("market must be one of cn, hk, us, jp, kr, tw")
        code = canonical_stock_code(normalize_stock_code(str(stock_code or "").strip()))
        if not code or len(code) > 16:
            raise ValueError("stock_code is required and must be at most 16 characters")
        trigger = str(trigger_source or "").strip()
        if not trigger or len(trigger) > 64:
            raise ValueError("trigger_source is required and must be at most 64 characters")
        if not isinstance(manual_daily_override, bool):
            raise ValueError("manual_daily_override must be a boolean")

        if type(priority) is not int:
            raise ValueError("priority must be an integer between 0 and 100")
        priority_int = priority
        resolved_mode = self.resolve_mode(requested_mode, priority=priority_int)
        bucket = MODE_BUCKETS[resolved_mode]
        limits = {
            "quick": int(getattr(self.config, "research_quick_daily_budget", 50)),
            "standard_deep": int(
                getattr(self.config, "research_standard_deep_daily_budget", 20)
            ),
            "debate": int(getattr(self.config, "research_debate_daily_budget", 8)),
        }
        daily_limit = limits[bucket]
        if daily_limit < 0:
            raise ValueError(
                f"Configured daily research budget for {bucket} must be non-negative"
            )

        # The manual override is intentionally scoped to the daily counter.
        # Durable lease/concurrency checks remain caller-owned and unchanged.
        try:
            from src.services.durable_job_handlers import (
                get_optional_durable_execution_context,
            )

            durable_context = get_optional_durable_execution_context()
        except (ImportError, RuntimeError):
            durable_context = None
        if durable_context is not None:
            durable_context.checkpoint()

        result = self.repo.reserve(
            task_id=task,
            budget_date=budget_date or get_market_now(market_norm).date(),
            stock_code=code,
            market=market_norm,
            mode=resolved_mode,
            bucket=bucket,
            trigger_source=trigger,
            priority=priority_int,
            manual_daily_override=manual_daily_override,
            daily_limit=daily_limit,
        )
        return {
            "id": int(result.row.id),
            "created": result.created,
            "task_id": result.row.task_id,
            "budget_date": result.row.budget_date.isoformat(),
            "stock_code": result.row.stock_code,
            "market": result.row.market,
            "requested_mode": str(requested_mode or "auto").strip().lower(),
            "resolved_mode": result.row.mode,
            "bucket": result.row.bucket,
            "daily_limit": daily_limit,
            "manual_daily_override": bool(result.row.manual_daily_override),
            "status": result.row.status,
            "created_at": result.row.created_at.isoformat(),
            "updated_at": result.row.updated_at.isoformat(),
        }

    def consume(self, reservation_id: int) -> Dict[str, Any]:
        return self._transition(reservation_id, "consumed")

    def release(self, reservation_id: int) -> Dict[str, Any]:
        return self._transition(reservation_id, "released")

    def _transition(self, reservation_id: int, status: str) -> Dict[str, Any]:
        row = self.repo.transition(
            reservation_id=int(reservation_id),
            target_status=status,
        )
        return {
            "id": int(row.id),
            "status": row.status,
            "updated_at": row.updated_at.isoformat(),
        }
