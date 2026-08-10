# -*- coding: utf-8 -*-
"""
===================================
API v1 Endpoints 模块初始化
===================================

职责：
1. 声明所有 endpoint 路由模块
"""

from api.v1.endpoints import (
    health,
    analysis,
    history,
    stocks,
    backtest,
    system_config,
    auth,
    agent,
    usage,
    portfolio,
    alerts,
    decision_outcomes_v2,
    decision_signals,
    screening,
    personal_research_artifacts,
    personal_research_runs,
    research_watchlist,
)
__all__ = [
    "health",
    "analysis",
    "history",
    "stocks",
    "backtest",
    "system_config",
    "auth",
    "agent",
    "usage",
    "portfolio",
    "alerts",
    "decision_outcomes_v2",
    "decision_signals",
    "screening",
    "personal_research_artifacts",
    "personal_research_runs",
    "research_watchlist",
]
