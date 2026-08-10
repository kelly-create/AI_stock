# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 存储层
===================================

职责：
1. 管理 SQLite 数据库连接（单例模式）
2. 定义 ORM 数据模型
3. 提供数据存取接口
4. 实现智能更新逻辑（断点续传）
"""

import atexit
from contextlib import contextmanager
import hashlib
import json
import logging
import math
import threading
import time
from datetime import datetime, date, timedelta, timezone
from typing import Optional, List, Dict, Any, TYPE_CHECKING, Tuple, Callable, TypeVar, Union

import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    CHAR,
    String,
    Float,
    Boolean,
    Date,
    DateTime,
    Integer,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    UniqueConstraint,
    CheckConstraint,
    Text,
    text,
    select,
    and_,
    or_,
    delete,
    desc,
    event,
    func,
    inspect,
    MetaData,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.schema import CreateIndex, CreateTable
from sqlalchemy.orm import (
    declarative_base,
    sessionmaker,
    Session,
)
from sqlalchemy.exc import IntegrityError, OperationalError

from src.agent.provider_trace import PROVIDER_TRACE_RETENTION_LIMIT
from src.config import get_config
from src.migrations import (
    BASELINE_SCHEMA_VERSION,
    PR0_CONVERGENCE_SCHEMA_VERSION,
    apply_migrations_locked,
    check_migration_state,
    migration_writer_lock,
)
from src.schemas.decision_profile import extract_legacy_decision_profile
from src.utils.sniper_points import extract_sniper_points, parse_sniper_value

logger = logging.getLogger(__name__)
T = TypeVar("T")
CURRENT_SCHEMA_VERSION = BASELINE_SCHEMA_VERSION
INTELLIGENCE_ITEM_NULL_SCOPE_VALUE = "__dsa_null_scope__"


class DatabaseMigrationRequired(RuntimeError):
    """Raised when an explicit-migration service sees a non-current database."""


# SQLAlchemy ORM 基类
Base = declarative_base()

if TYPE_CHECKING:
    from src.search_service import SearchResponse


def utc_naive_now() -> datetime:
    """Return current UTC time without tzinfo for SQLite DateTime columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_utc_naive_datetime(value: datetime) -> datetime:
    """Normalize aware datetimes to UTC-naive; treat naive values as UTC-naive."""
    if value.tzinfo is not None and value.utcoffset() is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


# === 数据模型定义 ===

class DatabaseSchemaMigration(Base):
    """Applied database schema version marker."""

    __tablename__ = 'schema_migrations'

    version = Column(String(64), primary_key=True)
    description = Column(String(255), nullable=False)
    applied_at = Column(DateTime, default=datetime.now, nullable=False, index=True)


class StockDaily(Base):
    """
    股票日线数据模型
    
    存储每日行情数据和计算的技术指标
    支持多股票、多日期的唯一约束
    """
    __tablename__ = 'stock_daily'
    
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 股票代码（如 600519, 000001）
    code = Column(String(10), nullable=False, index=True)
    
    # 交易日期
    date = Column(Date, nullable=False, index=True)
    
    # OHLC 数据
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    
    # 成交数据
    volume = Column(Float)  # 成交量（股）
    amount = Column(Float)  # 成交额（元）
    pct_chg = Column(Float)  # 涨跌幅（%）
    
    # 技术指标
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma20 = Column(Float)
    volume_ratio = Column(Float)  # 量比
    
    # 数据来源
    data_source = Column(String(50))  # 记录数据来源（如 AkshareFetcher）
    
    # 更新时间
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # 唯一约束：同一股票同一日期只能有一条数据
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
        Index('ix_code_date', 'code', 'date'),
    )
    
    def __repr__(self):
        return f"<StockDaily(code={self.code}, date={self.date}, close={self.close})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }


class NewsIntel(Base):
    """
    新闻情报数据模型

    存储搜索到的新闻情报条目，用于后续分析与查询
    """
    __tablename__ = 'news_intel'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 关联用户查询操作
    query_id = Column(String(64), index=True)

    # 股票信息
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))

    # 搜索上下文
    dimension = Column(String(32), index=True)  # latest_news / risk_check / earnings / market_analysis / industry
    query = Column(String(255))
    provider = Column(String(32), index=True)

    # 新闻内容
    title = Column(String(300), nullable=False)
    snippet = Column(Text)
    url = Column(String(1000), nullable=False)
    source = Column(String(100))
    published_date = Column(DateTime, index=True)

    # 入库时间
    fetched_at = Column(DateTime, default=datetime.now, index=True)
    query_source = Column(String(32), index=True)  # bot/web/cli/system
    requester_platform = Column(String(20))
    requester_user_id = Column(String(64))
    requester_user_name = Column(String(64))
    requester_chat_id = Column(String(64))
    requester_message_id = Column(String(64))
    requester_query = Column(String(255))

    __table_args__ = (
        UniqueConstraint('url', name='uix_news_url'),
        Index('ix_news_code_pub', 'code', 'published_date'),
    )

    def __repr__(self) -> str:
        return f"<NewsIntel(code={self.code}, title={self.title[:20]}...)>"


class IntelligenceSource(Base):
    """可配置资讯源。"""

    __tablename__ = 'intelligence_sources'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True, index=True)
    source_type = Column(String(32), nullable=False, default='rss', index=True)
    url = Column(String(1000), nullable=False)
    enabled = Column(Boolean, nullable=False, default=True, index=True)
    scope_type = Column(String(32), nullable=False, default='market', index=True)
    scope_value = Column(String(64), index=True)
    market = Column(String(32), nullable=False, default='cn', index=True)
    description = Column(Text)
    last_status = Column(String(32))
    last_error = Column(Text)
    last_fetched_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        Index('ix_intel_source_scope', 'scope_type', 'scope_value', 'market'),
    )


class IntelligenceItem(Base):
    """沉淀后的资讯 / 情报条目。"""

    __tablename__ = 'intelligence_items'

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey('intelligence_sources.id', ondelete='SET NULL'), nullable=True, index=True)
    source_name = Column(String(100), index=True)
    source_type = Column(String(32), nullable=False, default='rss', index=True)
    title = Column(String(300), nullable=False)
    summary = Column(Text)
    url = Column(String(1000), nullable=False, index=True)
    source = Column(String(100))
    published_at = Column(DateTime, index=True)
    fetched_at = Column(DateTime, default=datetime.now, index=True)
    scope_type = Column(String(32), nullable=False, default='market', index=True)
    scope_value = Column(String(64), nullable=False, default=INTELLIGENCE_ITEM_NULL_SCOPE_VALUE, index=True)
    market = Column(String(32), nullable=False, default='cn', index=True)
    raw_payload = Column(Text)

    __table_args__ = (
        UniqueConstraint(
            'source_id',
            'url',
            'scope_type',
            'scope_value',
            'market',
            name='uix_intel_item_source_scope_url',
        ),
        Index('ix_intel_item_scope_time', 'scope_type', 'scope_value', 'market', 'published_at'),
        Index('ix_intel_item_fetch_time', 'fetched_at'),
    )


class FundamentalSnapshot(Base):
    """
    基本面上下文快照（P0 write-only）。

    仅用于写入，主链路不依赖读取该表，便于后续回测/画像扩展。
    """
    __tablename__ = 'fundamental_snapshot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    query_id = Column(String(64), nullable=False, index=True)
    code = Column(String(10), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    source_chain = Column(Text)
    coverage = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_fundamental_snapshot_query_code', 'query_id', 'code'),
        Index('ix_fundamental_snapshot_created', 'created_at'),
    )

    def __repr__(self) -> str:
        return f"<FundamentalSnapshot(query_id={self.query_id}, code={self.code})>"


class ScreeningRun(Base):
    """A completed built-in screening run persisted by DSA."""

    __tablename__ = 'screening_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(64), nullable=False, unique=True, index=True)
    strategy = Column(String(64), nullable=False, index=True)
    market = Column(String(16), nullable=False, index=True)
    snapshot_source = Column(String(64), index=True)
    snapshot_count = Column(Integer)
    after_filter_count = Column(Integer)
    candidate_count = Column(Integer, nullable=False, default=0)
    llm_ranked = Column(Boolean)
    daily_enriched = Column(Boolean)
    source_errors_json = Column(Text)
    warnings_json = Column(Text)
    result_json = Column(Text, nullable=False)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)

    __table_args__ = (
        Index('ix_screening_run_strategy_created', 'strategy', 'created_at'),
        Index('ix_screening_run_market_created', 'market', 'created_at'),
    )


class AnalysisHistory(Base):
    """
    分析结果历史记录模型

    保存每次分析结果，支持按 query_id/股票代码检索
    """
    __tablename__ = 'analysis_history'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 关联查询链路
    query_id = Column(String(64), index=True)
    # Durable job linkage is nullable so legacy/flag-off writes remain valid.
    job_id = Column(String(64), nullable=True)

    # 股票信息
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    report_type = Column(String(16), index=True)

    # 核心结论
    sentiment_score = Column(Integer)
    operation_advice = Column(String(20))
    trend_prediction = Column(String(50))
    analysis_summary = Column(Text)

    # 详细数据
    raw_result = Column(Text)
    news_content = Column(Text)
    context_snapshot = Column(Text)

    # 狙击点位（用于回测）
    ideal_buy = Column(Float)
    secondary_buy = Column(Float)
    stop_loss = Column(Float)
    take_profit = Column(Float)

    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_analysis_code_time', 'code', 'created_at'),
        Index(
            'uix_analysis_history_job_code_report_type',
            'job_id',
            'code',
            'report_type',
            unique=True,
            sqlite_where=text('job_id IS NOT NULL'),
        ),
    )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'query_id': self.query_id,
            'job_id': self.job_id,
            'code': self.code,
            'name': self.name,
            'report_type': self.report_type,
            'sentiment_score': self.sentiment_score,
            'operation_advice': self.operation_advice,
            'trend_prediction': self.trend_prediction,
            'analysis_summary': self.analysis_summary,
            'raw_result': self.raw_result,
            'news_content': self.news_content,
            'context_snapshot': self.context_snapshot,
            'ideal_buy': self.ideal_buy,
            'secondary_buy': self.secondary_buy,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class BacktestResult(Base):
    """单条分析记录的回测结果。"""

    __tablename__ = 'backtest_results'

    id = Column(Integer, primary_key=True, autoincrement=True)

    analysis_history_id = Column(
        Integer,
        ForeignKey('analysis_history.id'),
        nullable=False,
        index=True,
    )

    # 冗余字段，便于按股票筛选
    code = Column(String(10), nullable=False, index=True)
    analysis_date = Column(Date, index=True)

    # 回测参数
    eval_window_days = Column(Integer, nullable=False, default=10)
    engine_version = Column(String(16), nullable=False, default='v1')

    # 状态
    eval_status = Column(String(16), nullable=False, default='pending')
    evaluated_at = Column(DateTime, default=datetime.now, index=True)

    # 建议快照（避免未来分析字段变化导致回测不可解释）
    operation_advice = Column(String(20))
    position_recommendation = Column(String(8))  # long/cash

    # 价格与收益
    start_price = Column(Float)
    end_close = Column(Float)
    max_high = Column(Float)
    min_low = Column(Float)
    stock_return_pct = Column(Float)

    # 方向与结果
    direction_expected = Column(String(16))  # up/down/flat/not_down
    direction_correct = Column(Boolean, nullable=True)
    outcome = Column(String(16))  # win/loss/neutral

    # 目标价命中（仅 long 且配置了止盈/止损时有意义）
    stop_loss = Column(Float)
    take_profit = Column(Float)
    hit_stop_loss = Column(Boolean)
    hit_take_profit = Column(Boolean)
    first_hit = Column(String(16))  # take_profit/stop_loss/ambiguous/neither/not_applicable
    first_hit_date = Column(Date)
    first_hit_trading_days = Column(Integer)

    # 模拟执行（long-only）
    simulated_entry_price = Column(Float)
    simulated_exit_price = Column(Float)
    simulated_exit_reason = Column(String(24))  # stop_loss/take_profit/window_end/cash/ambiguous_stop_loss
    simulated_return_pct = Column(Float)

    __table_args__ = (
        UniqueConstraint(
            'analysis_history_id',
            'eval_window_days',
            'engine_version',
            name='uix_backtest_analysis_window_version',
        ),
        Index('ix_backtest_code_date', 'code', 'analysis_date'),
    )


class BacktestSummary(Base):
    """回测汇总指标（按股票或全局）。"""

    __tablename__ = 'backtest_summaries'

    id = Column(Integer, primary_key=True, autoincrement=True)

    scope = Column(String(16), nullable=False, index=True)  # overall/stock
    code = Column(String(16), index=True)

    eval_window_days = Column(Integer, nullable=False, default=10)
    engine_version = Column(String(16), nullable=False, default='v1')
    computed_at = Column(DateTime, default=datetime.now, index=True)

    # 计数
    total_evaluations = Column(Integer, default=0)
    completed_count = Column(Integer, default=0)
    insufficient_count = Column(Integer, default=0)
    long_count = Column(Integer, default=0)
    cash_count = Column(Integer, default=0)

    win_count = Column(Integer, default=0)
    loss_count = Column(Integer, default=0)
    neutral_count = Column(Integer, default=0)

    # 准确率/胜率
    direction_accuracy_pct = Column(Float)
    win_rate_pct = Column(Float)
    neutral_rate_pct = Column(Float)

    # 收益
    avg_stock_return_pct = Column(Float)
    avg_simulated_return_pct = Column(Float)

    # 目标价触发统计（仅 long 且配置止盈/止损时统计）
    stop_loss_trigger_rate = Column(Float)
    take_profit_trigger_rate = Column(Float)
    ambiguous_rate = Column(Float)
    avg_days_to_first_hit = Column(Float)

    # 诊断字段（JSON 字符串）
    advice_breakdown_json = Column(Text)
    diagnostics_json = Column(Text)

    __table_args__ = (
        UniqueConstraint(
            'scope',
            'code',
            'eval_window_days',
            'engine_version',
            name='uix_backtest_summary_scope_code_window_version',
        ),
    )


class PortfolioAccount(Base):
    """Portfolio account metadata."""

    __tablename__ = 'portfolio_accounts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(String(64), index=True)
    name = Column(String(64), nullable=False)
    broker = Column(String(64))
    market = Column(String(8), nullable=False, default='cn', index=True)  # cn/hk/us
    base_currency = Column(String(8), nullable=False, default='CNY')
    is_active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        Index('ix_portfolio_account_owner_active', 'owner_id', 'is_active'),
    )


class PortfolioTrade(Base):
    """Executed trade events used as the source of truth for replay."""

    __tablename__ = 'portfolio_trades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    trade_uid = Column(String(128))
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    trade_date = Column(Date, nullable=False, index=True)
    side = Column(String(8), nullable=False)  # buy/sell
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    fee = Column(Float, default=0.0)
    tax = Column(Float, default=0.0)
    note = Column(String(255))
    dedup_hash = Column(String(64), index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint('account_id', 'trade_uid', name='uix_portfolio_trade_uid'),
        UniqueConstraint('account_id', 'dedup_hash', name='uix_portfolio_trade_dedup_hash'),
        Index('ix_portfolio_trade_account_date', 'account_id', 'trade_date'),
    )


class PortfolioCashLedger(Base):
    """Cash in/out events."""

    __tablename__ = 'portfolio_cash_ledger'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    event_date = Column(Date, nullable=False, index=True)
    direction = Column(String(8), nullable=False)  # in/out
    amount = Column(Float, nullable=False)
    currency = Column(String(8), nullable=False, default='CNY')
    note = Column(String(255))
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_cash_account_date', 'account_id', 'event_date'),
    )


class PortfolioCorporateAction(Base):
    """Corporate actions that impact cash or share quantity."""

    __tablename__ = 'portfolio_corporate_actions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    effective_date = Column(Date, nullable=False, index=True)
    action_type = Column(String(24), nullable=False)  # cash_dividend/split_adjustment
    cash_dividend_per_share = Column(Float)
    split_ratio = Column(Float)
    note = Column(String(255))
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_ca_account_date', 'account_id', 'effective_date'),
    )


class PortfolioPosition(Base):
    """Latest replayed position snapshot for each symbol in one account."""

    __tablename__ = 'portfolio_positions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    quantity = Column(Float, nullable=False, default=0.0)
    avg_cost = Column(Float, nullable=False, default=0.0)
    total_cost = Column(Float, nullable=False, default=0.0)
    last_price = Column(Float, nullable=False, default=0.0)
    market_value_base = Column(Float, nullable=False, default=0.0)
    unrealized_pnl_base = Column(Float, nullable=False, default=0.0)
    valuation_currency = Column(String(8), nullable=False, default='CNY')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'account_id',
            'symbol',
            'market',
            'currency',
            'cost_method',
            name='uix_portfolio_position_account_symbol_market_currency',
        ),
    )


class PortfolioPositionLot(Base):
    """Lot-level remaining quantities used by FIFO replay."""

    __tablename__ = 'portfolio_position_lots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    open_date = Column(Date, nullable=False, index=True)
    remaining_quantity = Column(Float, nullable=False, default=0.0)
    unit_cost = Column(Float, nullable=False, default=0.0)
    source_trade_id = Column(Integer, ForeignKey('portfolio_trades.id'))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_lot_account_symbol', 'account_id', 'symbol'),
    )


class PortfolioDailySnapshot(Base):
    """Daily account snapshot generated by read-time replay."""

    __tablename__ = 'portfolio_daily_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    snapshot_date = Column(Date, nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')  # fifo/avg
    base_currency = Column(String(8), nullable=False, default='CNY')
    total_cash = Column(Float, nullable=False, default=0.0)
    total_market_value = Column(Float, nullable=False, default=0.0)
    total_equity = Column(Float, nullable=False, default=0.0)
    unrealized_pnl = Column(Float, nullable=False, default=0.0)
    realized_pnl = Column(Float, nullable=False, default=0.0)
    fee_total = Column(Float, nullable=False, default=0.0)
    tax_total = Column(Float, nullable=False, default=0.0)
    fx_stale = Column(Boolean, nullable=False, default=False)
    payload = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            'account_id',
            'snapshot_date',
            'cost_method',
            name='uix_portfolio_snapshot_account_date_method',
        ),
    )


class PortfolioFxRate(Base):
    """Cached FX rates used for cross-currency portfolio conversion."""

    __tablename__ = 'portfolio_fx_rates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    from_currency = Column(String(8), nullable=False, index=True)
    to_currency = Column(String(8), nullable=False, index=True)
    rate_date = Column(Date, nullable=False, index=True)
    rate = Column(Float, nullable=False)
    source = Column(String(32), nullable=False, default='manual')
    is_stale = Column(Boolean, nullable=False, default=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            'from_currency',
            'to_currency',
            'rate_date',
            name='uix_portfolio_fx_pair_date',
        ),
    )


class ResearchWatchlistItemRecord(Base):
    """Versioned metadata layered on the legacy ``STOCK_LIST`` watchlist.

    Portfolio holdings are deliberately not copied into this table.  The
    effective research universe is resolved at read time from active rows,
    the legacy setting, and the current Portfolio source of truth.
    """

    __tablename__ = 'research_watchlist_items'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn', index=True)
    source = Column(String(32), nullable=False, default='manual', index=True)
    reason = Column(Text)
    priority = Column(Integer, nullable=False, default=50, index=True)
    analysis_tier = Column(String(16), nullable=False, default='quick', index=True)
    next_review_at = Column(DateTime, index=True)
    is_active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'market',
            'stock_code',
            name='uix_research_watchlist_market_stock',
        ),
        CheckConstraint(
            'priority >= 0 AND priority <= 100',
            name='ck_research_watchlist_priority',
        ),
        CheckConstraint(
            "analysis_tier IN ('quick','standard','deep')",
            name='ck_research_watchlist_analysis_tier',
        ),
        Index(
            'ix_research_watchlist_active_priority_review',
            'is_active',
            'priority',
            'next_review_at',
        ),
    )


class PortfolioReconciliationRecord(Base):
    """Append-only opening/reconciliation preview and applied event header."""

    __tablename__ = 'portfolio_reconciliations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(
        Integer,
        ForeignKey('portfolio_accounts.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    event_type = Column(String(16), nullable=False, index=True)
    status = Column(String(16), nullable=False, default='preview', index=True)
    event_version = Column(Integer)
    effective_date = Column(Date, nullable=False, index=True)
    preview_token = Column(String(64), nullable=False, unique=True)
    idempotency_key = Column(String(128))
    input_hash = Column(String(64), nullable=False)
    request_json = Column(Text, nullable=False)
    diff_json = Column(Text, nullable=False)
    note = Column(String(255))
    expires_at = Column(DateTime, nullable=False, index=True)
    applied_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'account_id',
            'event_version',
            name='uix_portfolio_reconciliation_account_version',
        ),
        UniqueConstraint(
            'account_id',
            'idempotency_key',
            name='uix_portfolio_reconciliation_account_idempotency',
        ),
        CheckConstraint(
            "event_type IN ('opening','adjustment')",
            name='ck_portfolio_reconciliation_event_type',
        ),
        CheckConstraint(
            "status IN ('preview','applied','expired','cancelled')",
            name='ck_portfolio_reconciliation_status',
        ),
        CheckConstraint(
            "(status = 'applied' AND event_version IS NOT NULL AND applied_at IS NOT NULL) "
            "OR (status <> 'applied' AND event_version IS NULL AND applied_at IS NULL)",
            name='ck_portfolio_reconciliation_applied_fields',
        ),
        Index(
            'ix_portfolio_reconciliation_account_status_created',
            'account_id',
            'status',
            'created_at',
        ),
        Index(
            'uix_portfolio_reconciliation_applied_opening',
            'account_id',
            unique=True,
            sqlite_where=text(
                "status = 'applied' AND event_type = 'opening'"
            ),
        ),
    )


class PortfolioReconciliationAdjustmentRecord(Base):
    """Immutable deltas consumed by Portfolio replay without forging trades."""

    __tablename__ = 'portfolio_reconciliation_adjustments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    reconciliation_id = Column(
        Integer,
        ForeignKey('portfolio_reconciliations.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    account_id = Column(
        Integer,
        ForeignKey('portfolio_accounts.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    identity_key = Column(String(128), nullable=False)
    adjustment_type = Column(String(16), nullable=False, index=True)
    stock_code = Column(String(16), index=True)
    market = Column(String(8))
    currency = Column(String(8), nullable=False, default='CNY')
    quantity_delta = Column(Float, nullable=False, default=0.0)
    total_cost_delta = Column(Float, nullable=False, default=0.0)
    cash_delta = Column(Float, nullable=False, default=0.0)
    before_json = Column(Text, nullable=False)
    after_json = Column(Text, nullable=False)
    created_at = Column(DateTime, default=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'reconciliation_id',
            'identity_key',
            name='uix_portfolio_reconciliation_adjustment_identity',
        ),
        CheckConstraint(
            "adjustment_type IN ('position','cash')",
            name='ck_portfolio_reconciliation_adjustment_type',
        ),
        CheckConstraint(
            "(adjustment_type = 'position' AND stock_code IS NOT NULL AND market IS NOT NULL) "
            "OR (adjustment_type = 'cash' AND stock_code IS NULL AND market IS NULL)",
            name='ck_portfolio_reconciliation_adjustment_identity',
        ),
        Index(
            'ix_portfolio_reconciliation_adjustment_account_stock',
            'account_id',
            'stock_code',
        ),
    )


class ConversationMessage(Base):
    """
    Agent 对话历史记录表
    """
    __tablename__ = 'conversation_messages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), index=True, nullable=False)
    role = Column(String(20), nullable=False)  # user, assistant, system
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now, index=True)


class ConversationSessionState(Base):
    """Persisted user selections for an Agent chat session."""

    __tablename__ = 'conversation_session_states'

    session_id = Column(String(100), primary_key=True)
    selected_skill_ids_json = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)


class ConversationSummary(Base):
    """Rolling summary for visible Agent chat history."""

    __tablename__ = 'conversation_summaries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), nullable=False, unique=True, index=True)
    summary = Column(Text, nullable=False)
    covered_message_id = Column(Integer, nullable=False, default=0)
    source_message_count = Column(Integer, nullable=False, default=0)
    estimated_tokens = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)


class AgentProviderTurn(Base):
    """Provider protocol trace required for thinking/tool-call roundtrip."""

    __tablename__ = 'agent_provider_turns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), nullable=False, index=True)
    run_id = Column(String(64), nullable=False, index=True)
    provider = Column(String(64), nullable=False, index=True)
    model = Column(String(160), nullable=False, index=True)
    anchor_user_message_id = Column(Integer, nullable=False, index=True)
    anchor_assistant_message_id = Column(Integer, nullable=False, index=True)
    messages_json = Column(Text, nullable=False)
    contains_reasoning = Column(Boolean, nullable=False, default=False)
    contains_tool_calls = Column(Boolean, nullable=False, default=False)
    contains_thinking_blocks = Column(Boolean, nullable=False, default=False)
    must_roundtrip = Column(Boolean, nullable=False, default=False, index=True)
    estimated_tokens = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_agent_provider_turn_bucket', 'session_id', 'provider', 'model', 'must_roundtrip'),
    )


class AnalysisJobRecord(Base):
    """Durable analysis job claimed by a single leased worker."""

    __tablename__ = 'analysis_jobs'

    task_id = Column(String(64), primary_key=True)
    job_type = Column(String(64), nullable=False)
    stock_code = Column(String(16), nullable=True)
    stock_name = Column(String(64), nullable=True)
    dedupe_key = Column(String(128), nullable=True)
    status = Column(
        String(32),
        nullable=False,
        server_default=text("'pending'"),
    )
    stage = Column(String(64), nullable=True)
    progress = Column(Integer, nullable=False, server_default=text('0'))
    message = Column(Text, nullable=True)
    payload_json = Column(Text, nullable=False, server_default=text("'{}'"))
    payload_version = Column(
        String(32),
        nullable=False,
        server_default=text("'1'"),
    )
    notify = Column(Boolean, nullable=False, server_default=text('0'))
    result_json = Column(Text, nullable=True)
    error_code = Column(String(64), nullable=True)
    error_message_sanitized = Column(Text, nullable=True)
    report_type = Column(String(32), nullable=True)
    analysis_phase = Column(String(32), nullable=True)
    query_source = Column(String(32), nullable=True)
    trace_id = Column(String(64), nullable=False)
    idempotency_key = Column(String(128), nullable=True)
    priority = Column(Integer, nullable=False, server_default=text('0'))
    attempt = Column(Integer, nullable=False, server_default=text('0'))
    max_attempts = Column(Integer, nullable=False, server_default=text('4'))
    available_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )
    lease_owner = Column(String(128), nullable=True)
    lease_token = Column(String(64), nullable=True)
    lease_expires_at = Column(DateTime, nullable=True)
    heartbeat_at = Column(DateTime, nullable=True)
    cancel_requested_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            'progress >= 0 AND progress <= 100',
            name='ck_analysis_jobs_progress_range',
        ),
        CheckConstraint(
            'attempt >= 0 AND max_attempts >= 1',
            name='ck_analysis_jobs_attempts',
        ),
        Index(
            'ix_analysis_jobs_claim',
            'status',
            'available_at',
            'priority',
            'created_at',
        ),
        Index('ix_analysis_jobs_lease', 'status', 'lease_expires_at'),
        Index(
            'ix_analysis_jobs_stock_status_created',
            'stock_code',
            'status',
            'created_at',
        ),
        Index('ix_analysis_jobs_trace_id', 'trace_id'),
        Index(
            'uix_analysis_jobs_idempotency_key',
            'idempotency_key',
            unique=True,
            sqlite_where=text('idempotency_key IS NOT NULL'),
        ),
        Index(
            'uix_analysis_jobs_active_dedupe_key',
            'dedupe_key',
            unique=True,
            sqlite_where=text(
                "dedupe_key IS NOT NULL AND status IN "
                "('pending', 'processing', 'cancel_requested')"
            ),
        ),
    )


class JobEventRecord(Base):
    """Append-only durable task event; ``id`` is the global SSE sequence."""

    __tablename__ = 'job_events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='CASCADE'),
        nullable=False,
    )
    event_type = Column(String(64), nullable=False)
    stage = Column(String(64), nullable=True)
    payload_json = Column(Text, nullable=False, server_default=text("'{}'"))
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        Index('ix_job_events_job_id_id', 'job_id', 'id'),
        Index('ix_job_events_created_at', 'created_at'),
    )


class NotificationOutboxRecord(Base):
    """Per-channel notification delivery state isolated from job completion."""

    __tablename__ = 'notification_outbox'

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='SET NULL'),
        nullable=True,
    )
    trace_id = Column(String(64), nullable=False)
    logical_notification_id = Column(String(128), nullable=False)
    notification_type = Column(String(64), nullable=False)
    channel = Column(String(32), nullable=False)
    route = Column(String(256), nullable=False)
    severity = Column(String(32), nullable=False)
    recipient = Column(String(256), nullable=True)
    payload_json = Column(Text, nullable=False)
    content_sha256 = Column(String(64), nullable=False)
    idempotency_key = Column(String(160), nullable=False)
    status = Column(
        String(32),
        nullable=False,
        server_default=text("'pending'"),
    )
    priority = Column(Integer, nullable=False, server_default=text('0'))
    attempt = Column(Integer, nullable=False, server_default=text('0'))
    max_attempts = Column(Integer, nullable=False, server_default=text('4'))
    available_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )
    lease_owner = Column(String(128), nullable=True)
    lease_token = Column(String(64), nullable=True)
    lease_expires_at = Column(DateTime, nullable=True)
    provider_message_id = Column(String(256), nullable=True)
    error_code = Column(String(64), nullable=True)
    error_message_sanitized = Column(Text, nullable=True)
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )
    sent_at = Column(DateTime, nullable=True)

    __table_args__ = (
        CheckConstraint(
            'attempt >= 0 AND max_attempts >= 1',
            name='ck_notification_outbox_attempts',
        ),
        Index(
            'uix_notification_outbox_idempotency_key',
            'idempotency_key',
            unique=True,
        ),
        Index(
            'uix_notification_outbox_logical_channel_route',
            'logical_notification_id',
            'channel',
            'route',
            unique=True,
        ),
        Index(
            'ix_notification_outbox_claim',
            'status',
            'available_at',
            'priority',
            'created_at',
        ),
        Index('ix_notification_outbox_lease', 'status', 'lease_expires_at'),
        Index('ix_notification_outbox_job_id', 'job_id'),
    )


class ProviderHealthRecord(Base):
    """Latest durable health/circuit state for one provider capability."""

    __tablename__ = 'provider_health'

    id = Column(Integer, primary_key=True, autoincrement=True)
    kind = Column(String(32), nullable=False)
    provider_key = Column(String(128), nullable=False)
    scope = Column(String(128), nullable=False)
    status = Column(
        String(32),
        nullable=False,
        server_default=text("'unknown'"),
    )
    success_count = Column(Integer, nullable=False, server_default=text('0'))
    failure_count = Column(Integer, nullable=False, server_default=text('0'))
    consecutive_failures = Column(Integer, nullable=False, server_default=text('0'))
    consecutive_successes = Column(Integer, nullable=False, server_default=text('0'))
    latency_ewma_ms = Column(Float, nullable=True)
    last_latency_ms = Column(Float, nullable=True)
    last_error_code = Column(String(64), nullable=True)
    last_error_message_sanitized = Column(Text, nullable=True)
    metadata_json = Column(Text, nullable=False, server_default=text("'{}'"))
    retry_after_at = Column(DateTime, nullable=True)
    circuit_open_until = Column(DateTime, nullable=True)
    last_checked_at = Column(DateTime, nullable=True)
    last_success_at = Column(DateTime, nullable=True)
    last_failure_at = Column(DateTime, nullable=True)
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            'success_count >= 0 AND failure_count >= 0 '
            'AND consecutive_failures >= 0 AND consecutive_successes >= 0',
            name='ck_provider_health_counters',
        ),
        Index(
            'uix_provider_health_kind_key_scope',
            'kind',
            'provider_key',
            'scope',
            unique=True,
        ),
        Index('ix_provider_health_status_updated', 'status', 'updated_at'),
        Index('ix_provider_health_retry_after_at', 'retry_after_at'),
    )


_RESEARCH_DATA_STATUS_VALUES = (
    'available',
    'empty',
    'partial',
    'stale',
    'permission_denied',
    'not_supported',
    'fetch_failed',
)
_RESEARCH_DATA_STATUS_SQL = ', '.join(
    f"'{status}'" for status in _RESEARCH_DATA_STATUS_VALUES
)
_RESEARCH_EVIDENCE_STATUS_VALUES = (
    'available',
    'empty',
    'partial',
    'fetch_failed',
)
_RESEARCH_EVIDENCE_STATUS_SQL = ', '.join(
    f"'{status}'" for status in _RESEARCH_EVIDENCE_STATUS_VALUES
)
_RESEARCH_DEBATE_STATUS_VALUES = (
    'available',
    'partial',
    'empty',
    'generation_failed',
)
_RESEARCH_DEBATE_STATUS_SQL = ', '.join(
    f"'{status}'" for status in _RESEARCH_DEBATE_STATUS_VALUES
)


class ResearchDatasetSnapshotRecord(Base):
    """Immutable normalized provider dataset available to research jobs."""

    __tablename__ = 'research_dataset_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset = Column(String(64), nullable=False)
    scope_type = Column(String(32), nullable=False)
    scope_value = Column(String(128), nullable=False)
    market = Column(String(16), nullable=False)
    provider = Column(String(64), nullable=False)
    schema_version = Column(String(64), nullable=False)
    trade_date = Column(Date, nullable=True)
    report_date = Column(Date, nullable=True)
    announcement_date = Column(Date, nullable=True)
    data_as_of = Column(DateTime, nullable=False)
    available_at = Column(DateTime, nullable=False)
    observed_at = Column(DateTime, nullable=False)
    status = Column(String(32), nullable=False)
    normalized_json = Column(Text, nullable=True)
    content_hash = Column(CHAR(64), nullable=False)
    raw_ref_json = Column(Text, nullable=True)
    error_code = Column(String(64), nullable=True)
    error_message_sanitized = Column(Text, nullable=True)
    supersedes_hash = Column(String(64), nullable=True)
    origin_job_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='SET NULL'),
        nullable=True,
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            f'status IN ({_RESEARCH_DATA_STATUS_SQL})',
            name='ck_research_dataset_snapshots_status',
        ),
        CheckConstraint(
            "status IN ('permission_denied', 'not_supported', 'fetch_failed') "
            'OR normalized_json IS NOT NULL',
            name='ck_research_dataset_snapshots_payload_required',
        ),
        CheckConstraint(
            'length(content_hash) = 64',
            name='ck_research_dataset_snapshots_hash_length',
        ),
        Index(
            'uix_research_dataset_snapshots_content_hash',
            'content_hash',
            unique=True,
        ),
        Index(
            'ix_research_dataset_snapshots_dataset_scope_asof',
            'dataset',
            'scope_type',
            'scope_value',
            'data_as_of',
            'id',
        ),
        Index(
            'ix_research_dataset_snapshots_scope_available',
            'scope_type',
            'scope_value',
            'available_at',
            'id',
        ),
    )


class ResearchFactorSnapshotRecord(Base):
    """Immutable output of the deterministic research factor engines."""

    __tablename__ = 'research_factor_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False)
    market = Column(String(16), nullable=False)
    company_profile = Column(String(32), nullable=False)
    primary_horizon = Column(
        Integer,
        nullable=False,
        server_default=text('10'),
    )
    engine_bundle_version = Column(String(64), nullable=False)
    value_score = Column(Float, nullable=True)
    quality_score = Column(Float, nullable=True)
    trend_score = Column(Float, nullable=True)
    catalyst_score = Column(Float, nullable=True)
    risk_penalty = Column(Float, nullable=True)
    factor_json = Column(Text, nullable=False)
    input_dataset_hashes_json = Column(Text, nullable=False)
    status = Column(String(32), nullable=False)
    coverage = Column(Float, nullable=False)
    unknowns_json = Column(Text, nullable=False)
    as_of = Column(DateTime, nullable=False)
    available_at = Column(DateTime, nullable=False)
    content_hash = Column(CHAR(64), nullable=False)
    origin_job_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='SET NULL'),
        nullable=True,
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            f'status IN ({_RESEARCH_DATA_STATUS_SQL})',
            name='ck_research_factor_snapshots_status',
        ),
        CheckConstraint(
            'primary_horizon > 0',
            name='ck_research_factor_snapshots_horizon',
        ),
        CheckConstraint(
            'coverage >= 0 AND coverage <= 1',
            name='ck_research_factor_snapshots_coverage',
        ),
        CheckConstraint(
            '(value_score IS NULL OR (value_score >= 0 AND value_score <= 100)) '
            'AND (quality_score IS NULL OR (quality_score >= 0 AND quality_score <= 100)) '
            'AND (trend_score IS NULL OR (trend_score >= 0 AND trend_score <= 100)) '
            'AND (catalyst_score IS NULL OR (catalyst_score >= 0 AND catalyst_score <= 100)) '
            'AND (risk_penalty IS NULL OR (risk_penalty >= 0 AND risk_penalty <= 100))',
            name='ck_research_factor_snapshots_scores',
        ),
        CheckConstraint(
            'length(content_hash) = 64',
            name='ck_research_factor_snapshots_hash_length',
        ),
        Index(
            'uix_research_factor_snapshots_content_hash',
            'content_hash',
            unique=True,
        ),
        Index(
            'ix_research_factor_snapshots_stock_asof',
            'stock_code',
            'as_of',
        ),
        Index(
            'ix_research_factor_snapshots_stock_profile_asof',
            'stock_code',
            'company_profile',
            'as_of',
        ),
    )


class ResearchEvidenceSnapshotRecord(Base):
    """Immutable, content-addressed research evidence and citation snapshot."""

    __tablename__ = 'research_evidence_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False)
    market = Column(String(16), nullable=False)
    evidence_engine_version = Column(String(64), nullable=False)
    claim_policy_version = Column(String(64), nullable=False)
    as_of = Column(DateTime, nullable=False)
    available_at = Column(DateTime, nullable=False)
    status = Column(String(32), nullable=False)
    coverage = Column(Float, nullable=False)
    claim_count = Column(Integer, nullable=False)
    citation_count = Column(Integer, nullable=False)
    canonical_json = Column(Text, nullable=False)
    input_dataset_hashes_json = Column(Text, nullable=False)
    factor_snapshot_hash = Column(String(64), nullable=False)
    evidence_hash = Column(CHAR(64), nullable=False)
    origin_job_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='SET NULL'),
        nullable=True,
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            f'status IN ({_RESEARCH_EVIDENCE_STATUS_SQL})',
            name='ck_research_evidence_snapshots_status',
        ),
        CheckConstraint(
            'coverage >= 0 AND coverage <= 1',
            name='ck_research_evidence_snapshots_coverage',
        ),
        CheckConstraint(
            'claim_count >= 0 AND citation_count >= 0',
            name='ck_research_evidence_snapshots_counts',
        ),
        CheckConstraint(
            'length(evidence_hash) = 64',
            name='ck_research_evidence_snapshots_hash_length',
        ),
        Index(
            'uix_research_evidence_snapshots_evidence_hash',
            'evidence_hash',
            unique=True,
        ),
        Index(
            'ix_research_evidence_snapshots_stock_asof',
            'stock_code',
            'as_of',
        ),
        Index(
            'ix_research_evidence_snapshots_factor_hash',
            'factor_snapshot_hash',
        ),
    )


class ResearchDebateRequestRecord(Base):
    """Immutable, pre-LLM prompt request frozen for deterministic resume."""

    __tablename__ = 'research_debate_requests'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False)
    market = Column(String(16), nullable=False)
    debate_engine_version = Column(String(64), nullable=False)
    output_schema_version = Column(String(64), nullable=False)
    prompt_version = Column(String(64), nullable=False)
    evidence_snapshot_hash = Column(String(64), nullable=False)
    model_route_fingerprint = Column(String(128), nullable=False)
    as_of = Column(DateTime, nullable=False)
    available_at = Column(DateTime, nullable=False)
    canonical_json = Column(Text, nullable=False)
    request_hash = Column(CHAR(64), nullable=False)
    origin_job_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='SET NULL'),
        nullable=True,
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            'length(evidence_snapshot_hash) = 64 '
            'AND length(request_hash) = 64',
            name='ck_research_debate_requests_hash_lengths',
        ),
        Index(
            'uix_research_debate_requests_request_hash',
            'request_hash',
            unique=True,
        ),
        Index(
            'ix_research_debate_requests_stock_asof',
            'stock_code',
            'as_of',
            'id',
        ),
        Index(
            'ix_research_debate_requests_evidence_route',
            'evidence_snapshot_hash',
            'prompt_version',
            'model_route_fingerprint',
        ),
    )


class ResearchDebateTurnRecord(Base):
    """Immutable, content-addressed output from one bounded debate stance."""

    __tablename__ = 'research_debate_turns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False)
    market = Column(String(16), nullable=False)
    stance = Column(String(16), nullable=False)
    round_no = Column(Integer, nullable=False, server_default=text('1'))
    debate_engine_version = Column(String(64), nullable=False)
    output_schema_version = Column(String(64), nullable=False)
    prompt_version = Column(String(64), nullable=False)
    evidence_snapshot_hash = Column(String(64), nullable=False)
    request_hash = Column(
        String(64),
        ForeignKey(
            'research_debate_requests.request_hash',
            ondelete='RESTRICT',
        ),
        nullable=False,
    )
    prompt_fingerprint = Column(CHAR(64), nullable=False)
    model_route_fingerprint = Column(String(128), nullable=False)
    model_used = Column(String(128), nullable=False)
    as_of = Column(DateTime, nullable=False)
    available_at = Column(DateTime, nullable=False)
    canonical_json = Column(Text, nullable=False)
    turn_hash = Column(CHAR(64), nullable=False)
    origin_job_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='SET NULL'),
        nullable=True,
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            "stance IN ('bull', 'bear')",
            name='ck_research_debate_turns_stance',
        ),
        CheckConstraint(
            'round_no = 1',
            name='ck_research_debate_turns_round_no',
        ),
        CheckConstraint(
            'length(evidence_snapshot_hash) = 64 '
            'AND length(request_hash) = 64 '
            'AND length(prompt_fingerprint) = 64 '
            'AND length(turn_hash) = 64',
            name='ck_research_debate_turns_hash_lengths',
        ),
        Index(
            'uix_research_debate_turns_turn_hash',
            'turn_hash',
            unique=True,
        ),
        Index(
            'ix_research_debate_turns_stock_asof',
            'stock_code',
            'as_of',
            'id',
        ),
        Index(
            'ix_research_debate_turns_resume',
            'stock_code',
            'evidence_snapshot_hash',
            'request_hash',
            'stance',
            'prompt_version',
            'prompt_fingerprint',
            'model_route_fingerprint',
        ),
    )


class ResearchDebateSnapshotRecord(Base):
    """Immutable bounded-debate snapshot assembled from validated turns."""

    __tablename__ = 'research_debate_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False)
    market = Column(String(16), nullable=False)
    debate_engine_version = Column(String(64), nullable=False)
    output_schema_version = Column(String(64), nullable=False)
    prompt_version = Column(String(64), nullable=False)
    evidence_snapshot_hash = Column(String(64), nullable=False)
    request_hash = Column(
        String(64),
        ForeignKey(
            'research_debate_requests.request_hash',
            ondelete='RESTRICT',
        ),
        nullable=False,
    )
    model_route_fingerprint = Column(String(128), nullable=False)
    as_of = Column(DateTime, nullable=False)
    available_at = Column(DateTime, nullable=False)
    status = Column(String(32), nullable=False)
    bull_turn_hash = Column(String(64), nullable=True)
    bear_turn_hash = Column(String(64), nullable=True)
    bull_argument_count = Column(Integer, nullable=False)
    bear_argument_count = Column(Integer, nullable=False)
    open_question_count = Column(Integer, nullable=False)
    canonical_json = Column(Text, nullable=False)
    debate_hash = Column(CHAR(64), nullable=False)
    origin_job_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='SET NULL'),
        nullable=True,
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            f'status IN ({_RESEARCH_DEBATE_STATUS_SQL})',
            name='ck_research_debate_snapshots_status',
        ),
        CheckConstraint(
            'bull_argument_count >= 0 AND bear_argument_count >= 0 '
            'AND open_question_count >= 0',
            name='ck_research_debate_snapshots_counts',
        ),
        CheckConstraint(
            'length(evidence_snapshot_hash) = 64 '
            'AND length(request_hash) = 64 '
            'AND (bull_turn_hash IS NULL OR length(bull_turn_hash) = 64) '
            'AND (bear_turn_hash IS NULL OR length(bear_turn_hash) = 64) '
            'AND length(debate_hash) = 64',
            name='ck_research_debate_snapshots_hash_lengths',
        ),
        CheckConstraint(
            "(status = 'available' AND bull_turn_hash IS NOT NULL "
            "AND bear_turn_hash IS NOT NULL) OR "
            "(status = 'partial' AND ((bull_turn_hash IS NOT NULL "
            "AND bear_turn_hash IS NULL) OR (bull_turn_hash IS NULL "
            "AND bear_turn_hash IS NOT NULL))) OR "
            "(status IN ('empty', 'generation_failed') "
            "AND bull_turn_hash IS NULL AND bear_turn_hash IS NULL)",
            name='ck_research_debate_snapshots_turn_presence',
        ),
        Index(
            'uix_research_debate_snapshots_debate_hash',
            'debate_hash',
            unique=True,
        ),
        Index(
            'ix_research_debate_snapshots_stock_asof',
            'stock_code',
            'as_of',
            'id',
        ),
        Index(
            'ix_research_debate_snapshots_evidence_asof',
            'evidence_snapshot_hash',
            'as_of',
            'id',
        ),
        Index(
            'ix_research_debate_snapshots_request_hash',
            'request_hash',
        ),
    )


class ResearchSnapshotRecord(Base):
    """Immutable, versioned AnalysisContextPack research snapshot."""

    __tablename__ = 'research_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False)
    market = Column(String(16), nullable=False)
    snapshot_version = Column(String(64), nullable=False)
    field_dictionary_version = Column(String(64), nullable=False)
    factor_engine_version = Column(String(64), nullable=False)
    pack_version = Column(String(64), nullable=False)
    prompt_version = Column(String(64), nullable=False)
    policy_version = Column(String(64), nullable=False)
    model_route_fingerprint = Column(String(128), nullable=False)
    as_of = Column(DateTime, nullable=False)
    available_at = Column(DateTime, nullable=False)
    status = Column(String(32), nullable=False)
    canonical_json = Column(Text, nullable=False)
    snapshot_hash = Column(CHAR(64), nullable=False)
    factor_snapshot_hash = Column(String(64), nullable=True)
    evidence_snapshot_hash = Column(String(64), nullable=True)
    debate_snapshot_hash = Column(String(64), nullable=True)
    origin_job_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='SET NULL'),
        nullable=True,
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            f'status IN ({_RESEARCH_DATA_STATUS_SQL})',
            name='ck_research_snapshots_status',
        ),
        CheckConstraint(
            'length(snapshot_hash) = 64',
            name='ck_research_snapshots_hash_length',
        ),
        Index(
            'uix_research_snapshots_snapshot_hash',
            'snapshot_hash',
            unique=True,
        ),
        Index(
            'ix_research_snapshots_stock_asof',
            'stock_code',
            'as_of',
        ),
        Index(
            'ix_research_snapshots_evidence_hash',
            'evidence_snapshot_hash',
        ),
        Index(
            'ix_research_snapshots_debate_hash',
            'debate_snapshot_hash',
        ),
    )


class LLMUsage(Base):
    """One row per litellm.completion() call — token-usage audit log."""

    __tablename__ = 'llm_usage'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # 'analysis' | 'agent' | 'market_review'
    call_type = Column(String(32), nullable=False, index=True)
    model = Column(String(128), nullable=False)
    stock_code = Column(String(16), nullable=True)
    provider = Column(String(64), nullable=True)
    prompt_tokens = Column(Integer, nullable=False, default=0)
    completion_tokens = Column(Integer, nullable=False, default=0)
    total_tokens = Column(Integer, nullable=False, default=0)

    # Durable-job correlation remains nullable for legacy and flag-off calls.
    job_id = Column(String(64), nullable=True)
    stage = Column(String(64), nullable=True)
    trace_id = Column(String(64), nullable=True)
    prompt_version = Column(String(64), nullable=True)
    snapshot_hash = Column(String(128), nullable=True)
    latency_ms = Column(Integer, nullable=True)
    status = Column(String(32), nullable=True)
    error_code = Column(String(64), nullable=True)
    error_message_sanitized = Column(Text, nullable=True)
    # Unknown provider pricing remains NULL instead of being reported as zero.
    estimated_cost_usd = Column(Float, nullable=True)
    cost_source = Column(String(32), nullable=True)
    attempt_no = Column(Integer, nullable=True)

    # Sanitized provider usage snapshot; raw prompts, messages, headers, and
    # tokenizer free-text fields are intentionally not persisted here.
    provider_usage_json = Column(Text, nullable=True)
    provider_usage_schema_name = Column(String(64), nullable=True)
    provider_usage_schema_version = Column(String(32), nullable=True)
    provider_usage_observed_at = Column(String(32), nullable=True)

    # Normalized telemetry values are derived from provider usage and may stay
    # NULL when the provider payload is absent or explicitly invalid.
    normalized_prompt_tokens = Column(Integer, nullable=True)
    normalized_completion_tokens = Column(Integer, nullable=True)
    normalized_total_tokens = Column(Integer, nullable=True)
    normalized_cache_read_tokens = Column(Integer, nullable=True)
    normalized_cache_write_tokens = Column(Integer, nullable=True)
    normalized_cache_miss_tokens = Column(Integer, nullable=True)
    normalized_uncached_input_tokens = Column(Integer, nullable=True)
    normalized_cache_eligible_input_tokens = Column(Integer, nullable=True)
    normalized_cache_hit_ratio = Column(Float, nullable=True)
    normalized_cache_write_ratio = Column(Float, nullable=True)
    cache_capability = Column(String(32), nullable=True)
    cache_eligibility = Column(String(32), nullable=True)
    cache_observation = Column(String(32), nullable=True)
    estimated_prefix_tokens = Column(Integer, nullable=True)
    provider_reported_prompt_tokens = Column(Integer, nullable=True)
    provider_reported_cached_tokens = Column(Integer, nullable=True)
    provider_min_cache_tokens = Column(Integer, nullable=True)
    eligibility_confidence = Column(String(32), nullable=True)

    # Kept nullable for schema compatibility; new writes do not store provider
    # or proxy tokenizer free-text values.
    tokenizer_name = Column(String(128), nullable=True)
    tokenizer_version = Column(String(64), nullable=True)

    # HMAC fingerprints let deployments compare message shapes without storing
    # raw prompt/message content.
    messages_hmac = Column(String(64), nullable=True)
    system_message_hmac = Column(String(64), nullable=True)
    user_message_hmac = Column(String(64), nullable=True)
    hmac_key_version = Column(String(64), nullable=True)
    hmac_domain = Column(String(32), nullable=True)
    hash_scope = Column(String(32), nullable=True)

    # P0.5a internal legacy message stability audit. These diagnostics are
    # stored locally only and are not returned by public usage APIs.
    language = Column(String(16), nullable=True)
    market_group = Column(String(16), nullable=True)
    analysis_mode = Column(String(64), nullable=True)
    legacy_prompt_mode = Column(String(32), nullable=True)
    skill_config_hmac = Column(String(64), nullable=True)
    transport = Column(String(64), nullable=True)
    message_count = Column(Integer, nullable=True)
    estimated_total_prompt_tokens = Column(Integer, nullable=True)
    approx_common_prefix_chars = Column(Integer, nullable=True)
    approx_common_prefix_tokens = Column(Integer, nullable=True)
    known_dynamic_marker_positions = Column(Text, nullable=True)
    called_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_llm_usage_job_stage_called_at', 'job_id', 'stage', 'called_at'),
        Index('ix_llm_usage_trace_called_at', 'trace_id', 'called_at'),
        Index('ix_llm_usage_status_called_at', 'status', 'called_at'),
    )


_LLM_USAGE_TELEMETRY_COLUMN_SQL: Dict[str, str] = {
    "provider_usage_json": "TEXT",
    "provider": "VARCHAR(64)",
    "provider_usage_schema_name": "VARCHAR(64)",
    "provider_usage_schema_version": "VARCHAR(32)",
    "provider_usage_observed_at": "VARCHAR(32)",
    "normalized_prompt_tokens": "INTEGER",
    "normalized_completion_tokens": "INTEGER",
    "normalized_total_tokens": "INTEGER",
    "normalized_cache_read_tokens": "INTEGER",
    "normalized_cache_write_tokens": "INTEGER",
    "normalized_cache_miss_tokens": "INTEGER",
    "normalized_uncached_input_tokens": "INTEGER",
    "normalized_cache_eligible_input_tokens": "INTEGER",
    "normalized_cache_hit_ratio": "FLOAT",
    "normalized_cache_write_ratio": "FLOAT",
    "cache_capability": "VARCHAR(32)",
    "cache_eligibility": "VARCHAR(32)",
    "cache_observation": "VARCHAR(32)",
    "estimated_prefix_tokens": "INTEGER",
    "provider_reported_prompt_tokens": "INTEGER",
    "provider_reported_cached_tokens": "INTEGER",
    "provider_min_cache_tokens": "INTEGER",
    "eligibility_confidence": "VARCHAR(32)",
    "tokenizer_name": "VARCHAR(128)",
    "tokenizer_version": "VARCHAR(64)",
    "messages_hmac": "VARCHAR(64)",
    "system_message_hmac": "VARCHAR(64)",
    "user_message_hmac": "VARCHAR(64)",
    "hmac_key_version": "VARCHAR(64)",
    "hmac_domain": "VARCHAR(32)",
    "hash_scope": "VARCHAR(32)",
    "language": "VARCHAR(16)",
    "market_group": "VARCHAR(16)",
    "analysis_mode": "VARCHAR(64)",
    "legacy_prompt_mode": "VARCHAR(32)",
    "skill_config_hmac": "VARCHAR(64)",
    "transport": "VARCHAR(64)",
    "message_count": "INTEGER",
    "estimated_total_prompt_tokens": "INTEGER",
    "approx_common_prefix_chars": "INTEGER",
    "approx_common_prefix_tokens": "INTEGER",
    "known_dynamic_marker_positions": "TEXT",
}
_LLM_USAGE_DURABLE_COLUMN_SQL: Dict[str, str] = {
    "job_id": "VARCHAR(64)",
    "stage": "VARCHAR(64)",
    "trace_id": "VARCHAR(64)",
    "prompt_version": "VARCHAR(64)",
    "snapshot_hash": "VARCHAR(128)",
    "latency_ms": "INTEGER",
    "status": "VARCHAR(32)",
    "error_code": "VARCHAR(64)",
    "error_message_sanitized": "TEXT",
    "estimated_cost_usd": "FLOAT",
    "cost_source": "VARCHAR(32)",
    "attempt_no": "INTEGER",
}
_LLM_USAGE_FORBIDDEN_GENERIC_AUDIT_COLUMNS = {
    "latency",
    "error",
    "cost",
    "attempt",
}
_PR1_EXISTING_TABLE_COLUMN_SQL: Dict[str, Dict[str, str]] = {
    "llm_usage": _LLM_USAGE_DURABLE_COLUMN_SQL,
    "analysis_history": {"job_id": "VARCHAR(64)"},
    "decision_signals": {"idempotency_key": "VARCHAR(128)"},
}
_LLM_USAGE_INTEGER_TELEMETRY_COLUMNS = {
    column
    for column, column_type in _LLM_USAGE_TELEMETRY_COLUMN_SQL.items()
    if column_type == "INTEGER"
}
_LLM_USAGE_DROPPED_FREE_TEXT_COLUMNS = {"tokenizer_name", "tokenizer_version"}
_LLM_PROMPT_CACHE_TELEMETRY_DISABLED_ATTR = "prompt_cache_telemetry_disabled"
_LLM_PROMPT_CACHE_TELEMETRY_COLUMNS = {
    "provider_usage_json",
    "provider_usage_schema_name",
    "provider_usage_schema_version",
    "provider_usage_observed_at",
    "normalized_cache_read_tokens",
    "normalized_cache_write_tokens",
    "normalized_cache_miss_tokens",
    "normalized_uncached_input_tokens",
    "normalized_cache_eligible_input_tokens",
    "normalized_cache_hit_ratio",
    "normalized_cache_write_ratio",
    "cache_capability",
    "cache_eligibility",
    "cache_observation",
    "estimated_prefix_tokens",
    "provider_reported_cached_tokens",
    "provider_min_cache_tokens",
    "eligibility_confidence",
}


class AlertRuleRecord(Base):
    """Persisted alert rule managed through the Alert API."""

    __tablename__ = 'alert_rules'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    target_scope = Column(String(32), nullable=False, default='single_symbol', index=True)
    target = Column(String(64), nullable=False, index=True)
    alert_type = Column(String(32), nullable=False, index=True)
    parameters = Column(Text, nullable=False, default='{}')
    severity = Column(String(16), nullable=False, default='warning', index=True)
    enabled = Column(Boolean, nullable=False, default=True, index=True)
    source = Column(String(16), nullable=False, default='api', index=True)
    cooldown_policy = Column(Text)
    notification_policy = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        Index('ix_alert_rule_type_target', 'alert_type', 'target'),
    )


class AlertTriggerRecord(Base):
    """Alert trigger history row.

    P1 exposes read APIs and table shape; runtime writer integration lands in
    later phases.
    """

    __tablename__ = 'alert_triggers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, index=True)
    target = Column(String(64), nullable=False, index=True)
    observed_value = Column(Float)
    threshold = Column(Float)
    reason = Column(Text)
    data_source = Column(String(64))
    data_timestamp = Column(DateTime, index=True)
    triggered_at = Column(DateTime, default=datetime.now, index=True)
    status = Column(String(16), nullable=False, default='triggered', index=True)
    diagnostics = Column(Text)

    __table_args__ = (
        Index('ix_alert_trigger_rule_time', 'rule_id', 'triggered_at'),
    )


class AlertNotificationRecord(Base):
    """Notification attempt row for alert triggers.

    P1 exposes read APIs and table shape; runtime writer integration lands in
    later phases.
    """

    __tablename__ = 'alert_notifications'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trigger_id = Column(Integer, index=True)
    channel = Column(String(32), nullable=False, index=True)
    attempt = Column(Integer, nullable=False, default=1)
    success = Column(Boolean, nullable=False, default=False, index=True)
    error_code = Column(String(64))
    retryable = Column(Boolean, nullable=False, default=False)
    latency_ms = Column(Integer)
    diagnostics = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_alert_notification_trigger_channel', 'trigger_id', 'channel'),
    )


class AlertCooldownRecord(Base):
    """Persisted alert cooldown state for DB-managed alert rules."""

    __tablename__ = 'alert_cooldowns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, index=True)
    # Reserved for future non-DB/expanded-scope rules; P4 queries by rule_id.
    rule_key = Column(String(255), index=True)
    target = Column(String(64), nullable=False, index=True)
    severity = Column(String(16), nullable=False, default='warning', index=True)
    last_triggered_at = Column(DateTime, index=True)
    cooldown_until = Column(DateTime, index=True)
    reason = Column(Text)
    state = Column(String(16), nullable=False, default='active', index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint('rule_id', 'target', 'severity', name='uix_alert_cooldown_rule_target_severity'),
    )


class DecisionSignalRecord(Base):
    """Persisted AI decision signal asset for Issue #1390 P1."""

    __tablename__ = 'decision_signals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False, index=True)
    stock_name = Column(String(64))
    market = Column(String(8), nullable=False, index=True)
    source_type = Column(String(32), nullable=False, index=True)
    source_agent = Column(String(64))
    source_report_id = Column(Integer, index=True)
    trace_id = Column(String(64), index=True)
    idempotency_key = Column(String(128), nullable=True)
    decision_profile = Column(String(16), index=True)
    market_phase = Column(String(24), index=True)
    trigger_source = Column(String(64), nullable=False, index=True)
    action = Column(String(16), nullable=False, index=True)
    action_label = Column(String(32))
    confidence = Column(Float)
    score = Column(Integer)
    horizon = Column(String(16), index=True)
    entry_low = Column(Float)
    entry_high = Column(Float)
    stop_loss = Column(Float)
    target_price = Column(Float)
    invalidation = Column(Text)
    watch_conditions = Column(Text)
    reason = Column(Text)
    risk_summary = Column(Text)
    catalyst_summary = Column(Text)
    evidence_json = Column(Text)
    data_quality_summary_json = Column(Text)
    research_stance = Column(String(24), index=True)
    account_action = Column(String(24), index=True)
    value_quality_score = Column(Float)
    trend_timing_score = Column(Float)
    catalyst_score = Column(Float)
    risk_score = Column(Float)
    evidence_quality_score = Column(Float)
    research_snapshot_hash = Column(String(64), index=True)
    policy_version = Column(String(64), index=True)
    policy_hash = Column(String(64), index=True)
    policy_evaluation_hash = Column(String(64), index=True)
    portfolio_snapshot_ref = Column(String(128), index=True)
    prompt_version = Column(String(64))
    catalysts_json = Column(Text)
    invalidators_json = Column(Text)
    unknowns_json = Column(Text)
    evidence_refs_json = Column(Text)
    policy_mode = Column(String(16), index=True)
    policy_decision = Column(String(16), index=True)
    would_block = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text('0'),
        index=True,
    )
    policy_reasons_json = Column(Text)
    plan_quality = Column(String(16), nullable=False, default='unknown', index=True)
    status = Column(String(16), nullable=False, default='active', index=True)
    expires_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)
    metadata_json = Column(Text)

    __table_args__ = (
        Index('ix_decision_signal_stock_status_time', 'stock_code', 'status', 'created_at'),
        Index('ix_decision_signal_market_status_time', 'market', 'status', 'created_at'),
        Index(
            'ix_decision_signal_report_type_market_stock_action_horizon_phase',
            'source_report_id',
            'source_type',
            'market',
            'stock_code',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_trace_type_market_stock_action_horizon_phase',
            'trace_id',
            'source_type',
            'market',
            'stock_code',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_report_type_market_stock_profile_action_horizon_phase',
            'source_report_id',
            'source_type',
            'market',
            'stock_code',
            'decision_profile',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_trace_type_market_stock_profile_action_horizon_phase',
            'trace_id',
            'source_type',
            'market',
            'stock_code',
            'decision_profile',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_market_stock_profile_created',
            'market',
            'stock_code',
            'decision_profile',
            'created_at',
        ),
        Index(
            'uix_decision_signals_idempotency_key',
            'idempotency_key',
            unique=True,
            sqlite_where=text('idempotency_key IS NOT NULL'),
        ),
    )


class PortfolioPolicyEvaluationRecord(Base):
    """Immutable deterministic Portfolio/Research policy decision audit."""

    __tablename__ = 'portfolio_policy_evaluations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    evaluation_hash = Column(String(64), nullable=False, unique=True)
    job_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='SET NULL'),
        index=True,
    )
    signal_id = Column(
        Integer,
        ForeignKey('decision_signals.id', ondelete='RESTRICT'),
        index=True,
    )
    stock_code = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    mode = Column(String(16), nullable=False, index=True)
    policy_version = Column(String(64), nullable=False, index=True)
    policy_hash = Column(String(64), nullable=False, index=True)
    research_snapshot_hash = Column(String(64), index=True)
    portfolio_snapshot_ref = Column(String(128), index=True)
    portfolio_context_json = Column(
        Text,
        nullable=False,
        default='{}',
        server_default=text("'{}'"),
    )
    input_hash = Column(String(64), nullable=False)
    output_hash = Column(String(64), nullable=False)
    research_stance = Column(String(24), nullable=False, index=True)
    proposed_account_action = Column(String(24), nullable=False, index=True)
    final_account_action = Column(String(24), nullable=False, index=True)
    verdict = Column(String(16), nullable=False, index=True)
    allowed = Column(Boolean, nullable=False, index=True)
    would_block = Column(Boolean, nullable=False, index=True)
    reasons_json = Column(Text, nullable=False)
    component_scores_json = Column(Text, nullable=False)
    limits_json = Column(Text, nullable=False)
    created_at = Column(DateTime, default=utc_naive_now, index=True)

    __table_args__ = (
        CheckConstraint(
            "mode IN ('off','shadow','enforce')",
            name='ck_portfolio_policy_evaluation_mode',
        ),
        CheckConstraint(
            "json_valid(portfolio_context_json) "
            "AND json_type(portfolio_context_json) = 'object'",
            name='ck_portfolio_policy_context_json',
        ),
        CheckConstraint(
            "research_stance IN ('strong_bullish','bullish','watch','neutral','bearish','avoid')",
            name='ck_portfolio_policy_research_stance',
        ),
        CheckConstraint(
            "proposed_account_action IN ('observe','open_candidate','add_candidate','hold','reduce_candidate','exit_candidate')",
            name='ck_portfolio_policy_proposed_action',
        ),
        CheckConstraint(
            "final_account_action IN ('observe','open_candidate','add_candidate','hold','reduce_candidate','exit_candidate')",
            name='ck_portfolio_policy_final_action',
        ),
        CheckConstraint(
            "verdict IN ('allow','downgrade','block','no_action')",
            name='ck_portfolio_policy_verdict',
        ),
        CheckConstraint(
            "(mode = 'enforce' AND allowed = 0 AND would_block = 1 AND final_account_action = 'observe') "
            "OR mode <> 'enforce' OR allowed = 1",
            name='ck_portfolio_policy_enforce_block',
        ),
        CheckConstraint(
            "would_block = (NOT allowed)",
            name='ck_portfolio_policy_would_block_matches_allowed',
        ),
        CheckConstraint(
            "mode = 'enforce' OR final_account_action = proposed_account_action",
            name='ck_portfolio_policy_shadow_does_not_mutate_action',
        ),
        Index(
            'ix_portfolio_policy_stock_created',
            'market',
            'stock_code',
            'created_at',
        ),
    )


class ResearchBudgetReservationRecord(Base):
    """Durable daily research-budget reservation bound to one task."""

    __tablename__ = 'research_budget_reservations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(64), nullable=False, index=True)
    budget_date = Column(Date, nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn', index=True)
    mode = Column(String(16), nullable=False, index=True)
    bucket = Column(String(24), nullable=False, index=True)
    trigger_source = Column(String(64), nullable=False, index=True)
    priority = Column(Integer, nullable=False, default=50, index=True)
    manual_daily_override = Column(Boolean, nullable=False, default=False)
    status = Column(String(16), nullable=False, default='reserved', index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'task_id',
            'market',
            'stock_code',
            'bucket',
            name='uix_research_budget_task_stock_bucket',
        ),
        CheckConstraint(
            "mode IN ('auto','quick','standard','deep','debate')",
            name='ck_research_budget_mode',
        ),
        CheckConstraint(
            "bucket IN ('quick','standard_deep','debate')",
            name='ck_research_budget_bucket',
        ),
        CheckConstraint(
            "status IN ('reserved','consumed','released')",
            name='ck_research_budget_status',
        ),
        CheckConstraint(
            'priority >= 0 AND priority <= 100',
            name='ck_research_budget_priority',
        ),
        Index(
            'ix_research_budget_date_bucket_status',
            'budget_date',
            'bucket',
            'status',
        ),
    )


_PERSONAL_RESEARCH_SKILL_CONTRACT_ROWS = (
    (
        'personal-value-quality',
        '1.0.0',
        'value_quality_score',
    ),
    (
        'personal-trend-timing',
        '1.0.0',
        'trend_timing_score',
    ),
    (
        'personal-catalyst',
        '1.0.0',
        'catalyst_score',
    ),
    (
        'personal-risk',
        '1.0.0',
        'risk_score',
    ),
    (
        'personal-evidence-quality',
        '1.0.0',
        'evidence_quality_score',
    ),
)
_PERSONAL_RESEARCH_SKILL_CONTRACT_SQL = ' OR '.join(
    '('
    f"skill_id = '{skill_id}' AND skill_version = '{skill_version}' "
    f"AND score_field = '{score_field}'"
    ')'
    for skill_id, skill_version, score_field
    in _PERSONAL_RESEARCH_SKILL_CONTRACT_ROWS
)
_PERSONAL_RESEARCH_SCORE_FIELD_SQL = ' OR '.join(
    f"(skill_id = '{skill_id}' AND score_field = '{score_field}')"
    for skill_id, _skill_version, score_field
    in _PERSONAL_RESEARCH_SKILL_CONTRACT_ROWS
)


class PersonalResearchSkillContractRecord(Base):
    """Seeded, immutable registry for the five approved Skill contracts."""

    __tablename__ = 'personal_research_skill_contracts'

    skill_id = Column(String(64), primary_key=True)
    skill_version = Column(String(64), primary_key=True)
    contract_hash = Column(CHAR(64), primary_key=True)
    score_field = Column(String(64), nullable=False)
    canonical_json = Column(Text, nullable=False)
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            _PERSONAL_RESEARCH_SKILL_CONTRACT_SQL,
            name='ck_personal_research_skill_contract_exact',
        ),
        CheckConstraint(
            "length(contract_hash) = 64 AND contract_hash NOT GLOB '*[^0-9a-f]*'",
            name='ck_personal_research_skill_contract_hash',
        ),
        CheckConstraint(
            "json_valid(canonical_json) = 1 AND json_type(canonical_json) = 'object'",
            name='ck_personal_research_skill_contract_json',
        ),
        Index(
            'uix_personal_research_skill_contract_hash',
            'contract_hash',
            unique=True,
        ),
        Index(
            'uix_personal_research_skill_contract_version',
            'skill_id',
            'skill_version',
            unique=True,
        ),
    )


class PersonalResearchSkillExecutionRecord(Base):
    """One terminal, immutable execution of an approved personal Skill."""

    __tablename__ = 'personal_research_skill_executions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_hash = Column(CHAR(64), nullable=False)
    task_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='RESTRICT'),
        nullable=False,
    )
    stock_code = Column(String(16), nullable=False)
    market = Column(String(16), nullable=False)
    skill_id = Column(String(64), nullable=False)
    skill_version = Column(String(64), nullable=False)
    contract_hash = Column(CHAR(64), nullable=False)
    score_field = Column(String(64), nullable=False)
    research_snapshot_hash = Column(
        String(64),
        ForeignKey('research_snapshots.snapshot_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    factor_snapshot_hash = Column(
        String(64),
        ForeignKey('research_factor_snapshots.content_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    evidence_snapshot_hash = Column(
        String(64),
        ForeignKey('research_evidence_snapshots.evidence_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    dataset_snapshot_hashes_json = Column(Text, nullable=False)
    dataset_lineage_hash = Column(CHAR(64), nullable=False)
    canonical_input_json = Column(Text, nullable=False)
    input_hash = Column(CHAR(64), nullable=False)
    result_status = Column(String(16), nullable=False)
    canonical_output_json = Column(Text, nullable=False)
    output_hash = Column(CHAR(64), nullable=False)
    score = Column(Float, nullable=True)
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ('skill_id', 'skill_version', 'contract_hash'),
            (
                'personal_research_skill_contracts.skill_id',
                'personal_research_skill_contracts.skill_version',
                'personal_research_skill_contracts.contract_hash',
            ),
            ondelete='RESTRICT',
        ),
        CheckConstraint(
            _PERSONAL_RESEARCH_SCORE_FIELD_SQL,
            name='ck_personal_research_skill_execution_score_field',
        ),
        CheckConstraint(
            "result_status IN ('succeeded', 'failed')",
            name='ck_personal_research_skill_execution_status',
        ),
        CheckConstraint(
            "(result_status = 'succeeded' AND score IS NOT NULL "
            "AND score >= 0 AND score <= 100) OR "
            "(result_status = 'failed' AND score IS NULL)",
            name='ck_personal_research_skill_execution_result',
        ),
        CheckConstraint(
            "json_valid(dataset_snapshot_hashes_json) = 1 "
            "AND json_type(dataset_snapshot_hashes_json) = 'array' "
            "AND json_array_length(dataset_snapshot_hashes_json) > 0 "
            "AND json_valid(canonical_input_json) = 1 "
            "AND json_type(canonical_input_json) = 'object' "
            "AND json_valid(canonical_output_json) = 1 "
            "AND json_type(canonical_output_json) = 'object'",
            name='ck_personal_research_skill_execution_json',
        ),
        CheckConstraint(
            "length(execution_hash) = 64 AND execution_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(contract_hash) = 64 AND contract_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(research_snapshot_hash) = 64 "
            "AND research_snapshot_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(factor_snapshot_hash) = 64 "
            "AND factor_snapshot_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(evidence_snapshot_hash) = 64 "
            "AND evidence_snapshot_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(dataset_lineage_hash) = 64 "
            "AND dataset_lineage_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(input_hash) = 64 AND input_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(output_hash) = 64 AND output_hash NOT GLOB '*[^0-9a-f]*'",
            name='ck_personal_research_skill_execution_hashes',
        ),
        UniqueConstraint(
            'task_id',
            'market',
            'stock_code',
            'skill_id',
            name='uix_personal_research_skill_execution_task_skill',
        ),
        Index(
            'uix_personal_research_skill_execution_hash',
            'execution_hash',
            unique=True,
        ),
        Index(
            'ix_personal_research_skill_execution_snapshot',
            'research_snapshot_hash',
            'skill_id',
        ),
        Index(
            'ix_personal_research_skill_execution_stock_created',
            'market',
            'stock_code',
            'created_at',
        ),
    )


class PersonalResearchDebateReviewRecord(Base):
    """Immutable deterministic Verifier/Judge review of one Debate snapshot."""

    __tablename__ = 'personal_research_debate_reviews'

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_hash = Column(CHAR(64), nullable=False)
    task_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='RESTRICT'),
        nullable=False,
    )
    stock_code = Column(String(16), nullable=False)
    market = Column(String(16), nullable=False)
    debate_snapshot_hash = Column(
        String(64),
        ForeignKey('research_debate_snapshots.debate_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    evidence_snapshot_hash = Column(
        String(64),
        ForeignKey('research_evidence_snapshots.evidence_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    verifier_version = Column(String(64), nullable=False)
    verifier_input_json = Column(Text, nullable=False)
    verifier_input_hash = Column(CHAR(64), nullable=False)
    verifier_output_json = Column(Text, nullable=False)
    verifier_output_hash = Column(CHAR(64), nullable=False)
    verifier_valid = Column(Boolean, nullable=False)
    verifier_fail_closed = Column(Boolean, nullable=False)
    verifier_reason_codes_json = Column(Text, nullable=False)
    judge_version = Column(String(64), nullable=False)
    judge_policy_hash = Column(CHAR(64), nullable=False)
    judge_input_json = Column(Text, nullable=False)
    judge_input_hash = Column(CHAR(64), nullable=False)
    judge_output_json = Column(Text, nullable=False)
    judge_output_hash = Column(CHAR(64), nullable=False)
    judge_fail_closed = Column(Boolean, nullable=False)
    judge_reason_codes_json = Column(Text, nullable=False)
    verdict = Column(String(16), nullable=False)
    winner = Column(String(16), nullable=True)
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            "verdict IN ('bull', 'bear', 'balanced', 'fail_closed')",
            name='ck_personal_research_debate_review_verdict',
        ),
        CheckConstraint(
            "(verdict IN ('bull', 'bear') AND winner = verdict "
            "AND judge_fail_closed = 0) OR "
            "(verdict = 'balanced' AND winner IS NULL AND judge_fail_closed = 0) OR "
            "(verdict = 'fail_closed' AND winner IS NULL AND judge_fail_closed = 1)",
            name='ck_personal_research_debate_review_winner',
        ),
        CheckConstraint(
            "(verifier_valid = 1 AND verifier_fail_closed = 0) OR "
            "(verifier_valid = 0 AND verifier_fail_closed = 1)",
            name='ck_personal_research_debate_review_verifier_state',
        ),
        CheckConstraint(
            "json_valid(verifier_input_json) = 1 "
            "AND json_type(verifier_input_json) = 'object' "
            "AND json_valid(verifier_output_json) = 1 "
            "AND json_type(verifier_output_json) = 'object' "
            "AND json_valid(verifier_reason_codes_json) = 1 "
            "AND json_type(verifier_reason_codes_json) = 'array' "
            "AND json_valid(judge_input_json) = 1 "
            "AND json_type(judge_input_json) = 'object' "
            "AND json_valid(judge_output_json) = 1 "
            "AND json_type(judge_output_json) = 'object' "
            "AND json_valid(judge_reason_codes_json) = 1 "
            "AND json_type(judge_reason_codes_json) = 'array'",
            name='ck_personal_research_debate_review_json',
        ),
        CheckConstraint(
            "length(review_hash) = 64 AND review_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(debate_snapshot_hash) = 64 "
            "AND debate_snapshot_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(evidence_snapshot_hash) = 64 "
            "AND evidence_snapshot_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(verifier_input_hash) = 64 "
            "AND verifier_input_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(verifier_output_hash) = 64 "
            "AND verifier_output_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(judge_policy_hash) = 64 "
            "AND judge_policy_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(judge_input_hash) = 64 "
            "AND judge_input_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(judge_output_hash) = 64 "
            "AND judge_output_hash NOT GLOB '*[^0-9a-f]*'",
            name='ck_personal_research_debate_review_hashes',
        ),
        UniqueConstraint(
            'task_id',
            'market',
            'stock_code',
            'debate_snapshot_hash',
            'verifier_version',
            'judge_version',
            'judge_policy_hash',
            name='uix_personal_research_debate_review_identity',
        ),
        Index(
            'uix_personal_research_debate_review_hash',
            'review_hash',
            unique=True,
        ),
        Index(
            'ix_personal_research_debate_review_stock_created',
            'market',
            'stock_code',
            'created_at',
        ),
    )


class PersonalResearchThesisRecord(Base):
    """Immutable personal Research Thesis with complete upstream lineage."""

    __tablename__ = 'personal_research_theses'

    id = Column(Integer, primary_key=True, autoincrement=True)
    thesis_hash = Column(CHAR(64), nullable=False)
    thesis_version = Column(String(64), nullable=False)
    task_id = Column(
        String(64),
        ForeignKey('analysis_jobs.task_id', ondelete='RESTRICT'),
        nullable=False,
    )
    stock_code = Column(String(16), nullable=False)
    market = Column(String(16), nullable=False)
    research_snapshot_hash = Column(
        String(64),
        ForeignKey('research_snapshots.snapshot_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    value_quality_execution_hash = Column(
        String(64),
        ForeignKey('personal_research_skill_executions.execution_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    trend_timing_execution_hash = Column(
        String(64),
        ForeignKey('personal_research_skill_executions.execution_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    catalyst_execution_hash = Column(
        String(64),
        ForeignKey('personal_research_skill_executions.execution_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    risk_execution_hash = Column(
        String(64),
        ForeignKey('personal_research_skill_executions.execution_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    evidence_quality_execution_hash = Column(
        String(64),
        ForeignKey('personal_research_skill_executions.execution_hash', ondelete='RESTRICT'),
        nullable=False,
    )
    debate_snapshot_hash = Column(
        String(64),
        ForeignKey('research_debate_snapshots.debate_hash', ondelete='RESTRICT'),
        nullable=True,
    )
    debate_review_hash = Column(
        String(64),
        ForeignKey('personal_research_debate_reviews.review_hash', ondelete='RESTRICT'),
        nullable=True,
    )
    decision_signal_id = Column(
        Integer,
        ForeignKey('decision_signals.id', ondelete='RESTRICT'),
        nullable=True,
    )
    policy_evaluation_hash = Column(
        String(64),
        ForeignKey('portfolio_policy_evaluations.evaluation_hash', ondelete='RESTRICT'),
        nullable=True,
    )
    policy_version = Column(String(64), nullable=True)
    policy_hash = Column(String(64), nullable=True)
    portfolio_snapshot_ref = Column(String(128), nullable=True)
    stance = Column(String(24), nullable=False)
    account_action = Column(String(24), nullable=False)
    scores_json = Column(Text, nullable=False)
    catalysts_json = Column(Text, nullable=False)
    invalidators_json = Column(Text, nullable=False)
    unknowns_json = Column(Text, nullable=False)
    evidence_refs_json = Column(Text, nullable=False)
    canonical_content_json = Column(Text, nullable=False)
    content_hash = Column(CHAR(64), nullable=False)
    supersedes_thesis_hash = Column(
        String(64),
        ForeignKey('personal_research_theses.thesis_hash', ondelete='RESTRICT'),
        nullable=True,
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        CheckConstraint(
            "stance IN ('strong_bullish', 'bullish', 'watch', 'neutral', 'bearish', 'avoid')",
            name='ck_personal_research_thesis_stance',
        ),
        CheckConstraint(
            "account_action IN ('observe', 'open_candidate', 'add_candidate', 'hold', "
            "'reduce_candidate', 'exit_candidate')",
            name='ck_personal_research_thesis_account_action',
        ),
        CheckConstraint(
            "(debate_snapshot_hash IS NULL AND debate_review_hash IS NULL) OR "
            "(debate_snapshot_hash IS NOT NULL AND debate_review_hash IS NOT NULL)",
            name='ck_personal_research_thesis_debate_lineage',
        ),
        CheckConstraint(
            "(policy_evaluation_hash IS NULL AND policy_version IS NULL "
            "AND policy_hash IS NULL AND portfolio_snapshot_ref IS NULL) OR "
            "(decision_signal_id IS NOT NULL AND policy_evaluation_hash IS NOT NULL "
            "AND policy_version IS NOT NULL AND policy_hash IS NOT NULL "
            "AND portfolio_snapshot_ref IS NOT NULL)",
            name='ck_personal_research_thesis_policy_lineage',
        ),
        CheckConstraint(
            "json_valid(scores_json) = 1 AND json_type(scores_json) = 'object' "
            "AND json_type(scores_json, '$.value_quality_score') IN ('integer', 'real') "
            "AND json_extract(scores_json, '$.value_quality_score') BETWEEN 0 AND 100 "
            "AND json_type(scores_json, '$.trend_timing_score') IN ('integer', 'real') "
            "AND json_extract(scores_json, '$.trend_timing_score') BETWEEN 0 AND 100 "
            "AND json_type(scores_json, '$.catalyst_score') IN ('integer', 'real') "
            "AND json_extract(scores_json, '$.catalyst_score') BETWEEN 0 AND 100 "
            "AND json_type(scores_json, '$.risk_score') IN ('integer', 'real') "
            "AND json_extract(scores_json, '$.risk_score') BETWEEN 0 AND 100 "
            "AND json_type(scores_json, '$.evidence_quality_score') IN ('integer', 'real') "
            "AND json_extract(scores_json, '$.evidence_quality_score') BETWEEN 0 AND 100 "
            "AND json_valid(catalysts_json) = 1 AND json_type(catalysts_json) = 'array' "
            "AND json_valid(invalidators_json) = 1 AND json_type(invalidators_json) = 'array' "
            "AND json_valid(unknowns_json) = 1 AND json_type(unknowns_json) = 'array' "
            "AND json_valid(evidence_refs_json) = 1 "
            "AND json_type(evidence_refs_json) = 'array' "
            "AND json_array_length(evidence_refs_json) > 0 "
            "AND json_valid(canonical_content_json) = 1 "
            "AND json_type(canonical_content_json) = 'object'",
            name='ck_personal_research_thesis_json',
        ),
        CheckConstraint(
            "length(thesis_hash) = 64 AND thesis_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(research_snapshot_hash) = 64 "
            "AND research_snapshot_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(value_quality_execution_hash) = 64 "
            "AND value_quality_execution_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(trend_timing_execution_hash) = 64 "
            "AND trend_timing_execution_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(catalyst_execution_hash) = 64 "
            "AND catalyst_execution_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(risk_execution_hash) = 64 "
            "AND risk_execution_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(evidence_quality_execution_hash) = 64 "
            "AND evidence_quality_execution_hash NOT GLOB '*[^0-9a-f]*' "
            "AND (debate_snapshot_hash IS NULL OR (length(debate_snapshot_hash) = 64 "
            "AND debate_snapshot_hash NOT GLOB '*[^0-9a-f]*')) "
            "AND (debate_review_hash IS NULL OR (length(debate_review_hash) = 64 "
            "AND debate_review_hash NOT GLOB '*[^0-9a-f]*')) "
            "AND (policy_evaluation_hash IS NULL OR (length(policy_evaluation_hash) = 64 "
            "AND policy_evaluation_hash NOT GLOB '*[^0-9a-f]*')) "
            "AND (policy_hash IS NULL OR (length(policy_hash) = 64 "
            "AND policy_hash NOT GLOB '*[^0-9a-f]*')) "
            "AND length(content_hash) = 64 AND content_hash NOT GLOB '*[^0-9a-f]*' "
            "AND (supersedes_thesis_hash IS NULL OR (length(supersedes_thesis_hash) = 64 "
            "AND supersedes_thesis_hash NOT GLOB '*[^0-9a-f]*')) "
            "AND (supersedes_thesis_hash IS NULL OR supersedes_thesis_hash <> thesis_hash)",
            name='ck_personal_research_thesis_hashes',
        ),
        Index(
            'uix_personal_research_thesis_hash',
            'thesis_hash',
            unique=True,
        ),
        Index(
            'uix_personal_research_thesis_supersedes',
            'supersedes_thesis_hash',
            unique=True,
            sqlite_where=text('supersedes_thesis_hash IS NOT NULL'),
        ),
        Index(
            'ix_personal_research_thesis_stock_created',
            'market',
            'stock_code',
            'created_at',
        ),
        Index(
            'ix_personal_research_thesis_snapshot',
            'research_snapshot_hash',
            'created_at',
        ),
    )


class DecisionSignalOutcomeRecord(Base):
    """Signal-level forward outcome for Issue #1390 P5."""

    __tablename__ = 'decision_signal_outcomes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(Integer, nullable=False, index=True)
    horizon = Column(String(16), nullable=False, index=True)
    engine_version = Column(String(32), nullable=False, index=True)
    eval_status = Column(String(24), nullable=False, default='unable', index=True)
    outcome = Column(String(16), index=True)
    direction_expected = Column(String(16), index=True)
    direction_correct = Column(Boolean)
    unable_reason = Column(String(64), index=True)
    anchor_date = Column(Date, index=True)
    eval_window_days = Column(Integer)
    start_price = Column(Float)
    end_close = Column(Float)
    max_high = Column(Float)
    min_low = Column(Float)
    stock_return_pct = Column(Float)

    action = Column(String(16), index=True)
    market = Column(String(8), index=True)
    market_phase = Column(String(24), index=True)
    source_type = Column(String(32), index=True)
    source_agent = Column(String(64), index=True)
    plan_quality = Column(String(16), index=True)
    data_quality_level = Column(String(24), index=True)
    holding_state = Column(String(16), nullable=False, default='unknown', index=True)

    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint('signal_id', 'horizon', 'engine_version', name='uix_decision_signal_outcome_key'),
        Index('ix_decision_signal_outcome_stats_action', 'engine_version', 'action', 'horizon'),
        Index('ix_decision_signal_outcome_stats_market', 'engine_version', 'market', 'horizon'),
    )


class DecisionOutcomeV2Record(Base):
    """Immutable personal-research Decision Outcome v2 observation."""

    __tablename__ = 'decision_outcomes_v2'

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(
        Integer,
        ForeignKey('decision_signals.id', ondelete='RESTRICT'),
        nullable=False,
        index=True,
    )
    outcome_contract = Column(String(32), nullable=False)
    horizon = Column(String(16), nullable=False, index=True)
    engine_version = Column(String(64), nullable=False, index=True)
    eval_status = Column(String(24), nullable=False, index=True)
    final_action_family = Column(String(16), nullable=False, index=True)
    outcome = Column(String(16), index=True)
    direction_correct = Column(Boolean)
    reason_code = Column(String(128), index=True)

    # Frozen DecisionSignal and Portfolio Policy facts.  These columns are
    # deliberately duplicated: later profile/policy edits must not rewrite the
    # historical population used for calibration.
    signal_created_at = Column(DateTime, nullable=False)
    signal_session = Column(Date)
    stock_code = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    source_type = Column(String(32), nullable=False)
    signal_action = Column(String(16), nullable=False)
    signal_horizon = Column(String(16))
    signal_status = Column(String(16), nullable=False)
    decision_profile = Column(String(16), nullable=False, index=True)
    research_snapshot_hash = Column(String(64), nullable=False, index=True)
    policy_version = Column(String(64), nullable=False)
    policy_hash = Column(CHAR(64), nullable=False)
    policy_evaluation_hash = Column(CHAR(64), nullable=False, index=True)
    portfolio_snapshot_ref = Column(String(128), nullable=False)
    prompt_version = Column(String(64))
    research_stance = Column(String(24), nullable=False, index=True)
    proposed_account_action = Column(String(24), nullable=False)
    final_account_action = Column(String(24), nullable=False, index=True)
    policy_mode = Column(String(16), nullable=False)
    policy_verdict = Column(String(16), nullable=False)
    policy_allowed = Column(Boolean, nullable=False)
    would_block = Column(Boolean, nullable=False)
    confidence = Column(Float)
    signal_score = Column(Float)
    value_quality_score = Column(Float, nullable=False)
    trend_timing_score = Column(Float, nullable=False)
    catalyst_score = Column(Float, nullable=False)
    risk_score = Column(Float, nullable=False)
    evidence_quality_score = Column(Float, nullable=False)

    # Frozen T+1 execution and forward-observation facts.
    execution_status = Column(String(24), nullable=False, index=True)
    entry_trade_date = Column(Date, index=True)
    entry_raw_open = Column(Float)
    entry_adj_factor = Column(Float)
    end_trade_date = Column(Date, index=True)
    trading_day_count = Column(Integer)
    end_adjusted_close = Column(Float)
    stock_return_pct = Column(Float)
    directional_return_pct = Column(Float)
    mfe_pct = Column(Float)
    mae_pct = Column(Float)

    csi300_code = Column(String(32), nullable=False)
    csi300_name = Column(String(128))
    csi300_status = Column(String(16), nullable=False)
    csi300_reason_code = Column(String(128))
    csi300_return_pct = Column(Float)
    csi300_stock_excess_return_pct = Column(Float)
    csi300_directional_excess_return_pct = Column(Float)

    sw1_code = Column(String(32))
    sw1_name = Column(String(128))
    sw1_status = Column(String(16), nullable=False)
    sw1_reason_code = Column(String(128))
    sw1_return_pct = Column(Float)
    sw1_stock_excess_return_pct = Column(Float)
    sw1_directional_excess_return_pct = Column(Float)

    dataset_hashes_json = Column(Text, nullable=False)
    observation_json = Column(Text, nullable=False)
    observation_hash = Column(CHAR(64), nullable=False, index=True)
    evaluated_at = Column(DateTime, index=True)
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
        index=True,
    )
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
        index=True,
    )

    __table_args__ = (
        UniqueConstraint(
            'signal_id',
            'horizon',
            'engine_version',
            name='uix_decision_outcome_v2_identity',
        ),
        CheckConstraint(
            "outcome_contract = 'decision-outcome-v2'",
            name='ck_decision_outcome_v2_contract',
        ),
        CheckConstraint(
            "horizon IN ('5d', '10d', '20d')",
            name='ck_decision_outcome_v2_horizon',
        ),
        CheckConstraint(
            "length(engine_version) BETWEEN 1 AND 64 "
            "AND length(stock_code) BETWEEN 1 AND 16 "
            "AND length(market) BETWEEN 1 AND 8 "
            "AND length(decision_profile) BETWEEN 1 AND 16",
            name='ck_decision_outcome_v2_identifiers',
        ),
        CheckConstraint(
            "eval_status IN ('pending', 'evaluated', 'observational', "
            "'unexecutable', 'unable')",
            name='ck_decision_outcome_v2_eval_status',
        ),
        CheckConstraint(
            "final_action_family IN ('long', 'defensive', 'observational')",
            name='ck_decision_outcome_v2_action_family',
        ),
        CheckConstraint(
            "execution_status IN ('pending', 'executable', 'unexecutable', "
            "'unavailable')",
            name='ck_decision_outcome_v2_execution_status',
        ),
        CheckConstraint(
            "(final_account_action IN ('open_candidate', 'add_candidate') "
            "AND final_action_family = 'long') OR "
            "(final_account_action IN ('reduce_candidate', 'exit_candidate') "
            "AND final_action_family = 'defensive') OR "
            "(final_account_action IN ('observe', 'hold') "
            "AND final_action_family = 'observational')",
            name='ck_decision_outcome_v2_final_action_family',
        ),
        CheckConstraint(
            "policy_mode IN ('off', 'shadow', 'enforce') "
            "AND policy_verdict IN ('allow', 'downgrade', 'block', 'no_action') "
            "AND would_block = (NOT policy_allowed)",
            name='ck_decision_outcome_v2_policy',
        ),
        CheckConstraint(
            "research_stance IN ('strong_bullish', 'bullish', 'watch', "
            "'neutral', 'bearish', 'avoid')",
            name='ck_decision_outcome_v2_stance',
        ),
        CheckConstraint(
            "proposed_account_action IN ('observe', 'open_candidate', "
            "'add_candidate', 'hold', 'reduce_candidate', 'exit_candidate')",
            name='ck_decision_outcome_v2_proposed_action',
        ),
        CheckConstraint(
            "(confidence IS NULL OR (confidence >= 0 AND confidence <= 1)) "
            "AND (signal_score IS NULL OR "
            "(signal_score >= 0 AND signal_score <= 100)) "
            "AND value_quality_score BETWEEN 0 AND 100 "
            "AND trend_timing_score BETWEEN 0 AND 100 "
            "AND catalyst_score BETWEEN 0 AND 100 "
            "AND risk_score BETWEEN 0 AND 100 "
            "AND evidence_quality_score BETWEEN 0 AND 100",
            name='ck_decision_outcome_v2_scores',
        ),
        CheckConstraint(
            "length(research_snapshot_hash) = 64 "
            "AND research_snapshot_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(policy_hash) = 64 "
            "AND policy_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(policy_evaluation_hash) = 64 "
            "AND policy_evaluation_hash NOT GLOB '*[^0-9a-f]*' "
            "AND length(observation_hash) = 64 "
            "AND observation_hash NOT GLOB '*[^0-9a-f]*'",
            name='ck_decision_outcome_v2_hashes',
        ),
        CheckConstraint(
            "json_valid(dataset_hashes_json) = 1 "
            "AND json_type(dataset_hashes_json) = 'array' "
            "AND json_array_length(dataset_hashes_json) <= 128 "
            "AND json_valid(observation_json) = 1 "
            "AND json_type(observation_json) = 'object'",
            name='ck_decision_outcome_v2_json',
        ),
        CheckConstraint(
            "(trading_day_count IS NULL OR "
            "(horizon = '5d' AND trading_day_count = 5) OR "
            "(horizon = '10d' AND trading_day_count = 10) OR "
            "(horizon = '20d' AND trading_day_count = 20)) "
            "AND (signal_session IS NULL OR entry_trade_date IS NULL "
            "OR entry_trade_date > signal_session) "
            "AND (entry_trade_date IS NULL OR end_trade_date IS NULL "
            "OR end_trade_date >= entry_trade_date) "
            "AND (entry_raw_open IS NULL OR entry_raw_open > 0) "
            "AND (entry_adj_factor IS NULL OR entry_adj_factor > 0) "
            "AND (end_adjusted_close IS NULL OR end_adjusted_close > 0) "
            "AND (mfe_pct IS NULL OR mfe_pct >= 0) "
            "AND (mae_pct IS NULL OR mae_pct >= 0)",
            name='ck_decision_outcome_v2_metrics',
        ),
        CheckConstraint(
            "(eval_status = 'pending' AND execution_status = 'pending' "
            "AND outcome IS NULL AND direction_correct IS NULL "
            "AND end_adjusted_close IS NULL "
            "AND stock_return_pct IS NULL "
            "AND directional_return_pct IS NULL "
            "AND mfe_pct IS NULL AND mae_pct IS NULL "
            "AND evaluated_at IS NULL AND reason_code IS NOT NULL) OR "
            "(eval_status = 'evaluated' "
            "AND final_action_family IN ('long', 'defensive') "
            "AND execution_status = 'executable' "
            "AND ((outcome = 'hit' AND direction_correct = 1) "
            "OR (outcome = 'miss' AND direction_correct = 0)) "
            "AND reason_code IS NULL AND signal_session IS NOT NULL "
            "AND entry_trade_date IS NOT NULL AND entry_raw_open IS NOT NULL "
            "AND entry_adj_factor IS NOT NULL AND end_trade_date IS NOT NULL "
            "AND trading_day_count IS NOT NULL "
            "AND end_adjusted_close IS NOT NULL "
            "AND stock_return_pct IS NOT NULL "
            "AND directional_return_pct IS NOT NULL "
            "AND mfe_pct IS NOT NULL AND mae_pct IS NOT NULL "
            "AND json_array_length(dataset_hashes_json) > 0 "
            "AND evaluated_at IS NOT NULL) OR "
            "(eval_status = 'observational' "
            "AND final_action_family = 'observational' "
            "AND execution_status = 'executable' "
            "AND outcome IS NULL AND direction_correct IS NULL "
            "AND reason_code IS NULL AND signal_session IS NOT NULL "
            "AND entry_trade_date IS NOT NULL AND entry_raw_open IS NOT NULL "
            "AND entry_adj_factor IS NOT NULL AND end_trade_date IS NOT NULL "
            "AND trading_day_count IS NOT NULL "
            "AND end_adjusted_close IS NOT NULL "
            "AND stock_return_pct IS NOT NULL "
            "AND directional_return_pct IS NULL "
            "AND mfe_pct IS NULL AND mae_pct IS NULL "
            "AND json_array_length(dataset_hashes_json) > 0 "
            "AND evaluated_at IS NOT NULL) OR "
            "(eval_status = 'unexecutable' "
            "AND final_action_family IN ('long', 'defensive') "
            "AND execution_status = 'unexecutable' "
            "AND outcome IS NULL AND direction_correct IS NULL "
            "AND reason_code IN ('entry_suspended', "
            "'entry_one_price_limit_up', 'entry_one_price_limit_down') "
            "AND signal_session IS NOT NULL "
            "AND entry_trade_date IS NOT NULL "
            "AND end_trade_date IS NOT NULL "
            "AND trading_day_count IS NOT NULL "
            "AND end_adjusted_close IS NULL "
            "AND stock_return_pct IS NULL "
            "AND directional_return_pct IS NULL "
            "AND mfe_pct IS NULL AND mae_pct IS NULL "
            "AND json_array_length(dataset_hashes_json) > 0 "
            "AND evaluated_at IS NOT NULL) OR "
            "(eval_status = 'unable' "
            "AND execution_status IN ('executable', 'unavailable') "
            "AND outcome IS NULL AND direction_correct IS NULL "
            "AND reason_code IS NOT NULL "
            "AND end_adjusted_close IS NULL "
            "AND stock_return_pct IS NULL "
            "AND directional_return_pct IS NULL "
            "AND mfe_pct IS NULL AND mae_pct IS NULL "
            "AND evaluated_at IS NOT NULL)",
            name='ck_decision_outcome_v2_state',
        ),
        CheckConstraint(
            "(eval_status NOT IN ('evaluated', 'observational') OR "
            "abs(stock_return_pct - "
            "(((end_adjusted_close / entry_raw_open) - 1) * 100)) <= 0.00000001) "
            "AND (eval_status <> 'evaluated' OR "
            "abs(directional_return_pct - "
            "(CASE WHEN final_action_family = 'long' THEN stock_return_pct "
            "ELSE -stock_return_pct END)) <= 0.00000001) "
            "AND (eval_status <> 'evaluated' OR "
            "(directional_return_pct > 0 AND outcome = 'hit' "
            "AND direction_correct = 1) OR "
            "(directional_return_pct <= 0 AND outcome = 'miss' "
            "AND direction_correct = 0))",
            name='ck_decision_outcome_v2_result_math',
        ),
        CheckConstraint(
            "csi300_code = '000300.SH' "
            "AND csi300_status IN ('available', 'unavailable') "
            "AND sw1_status IN ('available', 'unavailable')",
            name='ck_decision_outcome_v2_benchmark_status',
        ),
        CheckConstraint(
            "(csi300_status = 'available' "
            "AND eval_status IN ('evaluated', 'observational') "
            "AND csi300_reason_code IS NULL "
            "AND csi300_return_pct IS NOT NULL "
            "AND csi300_stock_excess_return_pct IS NOT NULL "
            "AND abs(csi300_stock_excess_return_pct - "
            "(stock_return_pct - csi300_return_pct)) <= 0.00000001 "
            "AND ((eval_status = 'evaluated' "
            "AND csi300_directional_excess_return_pct IS NOT NULL "
            "AND abs(csi300_directional_excess_return_pct - "
            "(directional_return_pct - (csi300_return_pct * "
            "(CASE WHEN final_action_family = 'long' THEN 1 ELSE -1 END)))) "
            "<= 0.00000001) "
            "OR (eval_status <> 'evaluated' "
            "AND csi300_directional_excess_return_pct IS NULL))) OR "
            "(csi300_status = 'unavailable' "
            "AND csi300_reason_code IS NOT NULL "
            "AND csi300_return_pct IS NULL "
            "AND csi300_stock_excess_return_pct IS NULL "
            "AND csi300_directional_excess_return_pct IS NULL)",
            name='ck_decision_outcome_v2_csi300',
        ),
        CheckConstraint(
            "(sw1_status = 'available' "
            "AND eval_status IN ('evaluated', 'observational') "
            "AND sw1_code IS NOT NULL "
            "AND sw1_reason_code IS NULL AND sw1_return_pct IS NOT NULL "
            "AND sw1_stock_excess_return_pct IS NOT NULL "
            "AND abs(sw1_stock_excess_return_pct - "
            "(stock_return_pct - sw1_return_pct)) <= 0.00000001 "
            "AND ((eval_status = 'evaluated' "
            "AND sw1_directional_excess_return_pct IS NOT NULL "
            "AND abs(sw1_directional_excess_return_pct - "
            "(directional_return_pct - (sw1_return_pct * "
            "(CASE WHEN final_action_family = 'long' THEN 1 ELSE -1 END)))) "
            "<= 0.00000001) "
            "OR (eval_status <> 'evaluated' "
            "AND sw1_directional_excess_return_pct IS NULL))) OR "
            "(sw1_status = 'unavailable' "
            "AND sw1_reason_code IS NOT NULL "
            "AND sw1_return_pct IS NULL "
            "AND sw1_stock_excess_return_pct IS NULL "
            "AND sw1_directional_excess_return_pct IS NULL)",
            name='ck_decision_outcome_v2_sw1',
        ),
        Index(
            'ix_decision_outcome_v2_candidates',
            'engine_version',
            'eval_status',
            'updated_at',
        ),
        Index(
            'ix_decision_outcome_v2_calibration',
            'engine_version',
            'horizon',
            'decision_profile',
            'final_action_family',
            'eval_status',
        ),
        Index(
            'ix_decision_outcome_v2_signal_engine',
            'signal_id',
            'engine_version',
        ),
    )


class DecisionSignalFeedbackRecord(Base):
    """Latest user feedback for a decision signal."""

    __tablename__ = 'decision_signal_feedback'

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(Integer, nullable=False, unique=True, index=True)
    feedback_value = Column(String(16), nullable=False, index=True)
    reason_code = Column(String(64), index=True)
    note = Column(Text)
    source = Column(String(16), nullable=False, default='api', index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)


class SkillOpinionSampleRecord(Base):
    """Immutable, low-sensitivity skill opinion sample for Issue #1904 P2 PR1."""

    __tablename__ = 'skill_opinion_samples'

    id = Column(Integer, primary_key=True, autoincrement=True)
    analysis_history_id = Column(
        Integer,
        ForeignKey('analysis_history.id'),
        nullable=False,
        index=True,
    )
    stock_code = Column(String(16), nullable=False, index=True)
    skill_id = Column(String(128), nullable=False, index=True)
    skill_version = Column(String(64), index=True)
    signal = Column(String(16), nullable=False, index=True)
    confidence = Column(Float, nullable=False)
    horizon = Column(String(16), index=True)
    data_quality_level = Column(String(24), index=True)
    opinion_created_at = Column(DateTime, index=True)
    sample_schema_version = Column(String(32), nullable=False, index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'analysis_history_id',
            'skill_id',
            'sample_schema_version',
            name='uix_skill_opinion_sample_key',
        ),
        Index(
            'ix_skill_opinion_sample_skill_horizon_created',
            'skill_id',
            'horizon',
            'created_at',
        ),
        Index(
            'ix_skill_opinion_sample_stock_created',
            'stock_code',
            'created_at',
        ),
    )


class SkillOpinionOutcomeRecord(Base):
    """Forward outcome for one immutable skill opinion sample and horizon."""

    __tablename__ = 'skill_opinion_outcomes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    skill_opinion_sample_id = Column(
        Integer,
        ForeignKey('skill_opinion_samples.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    horizon = Column(String(16), nullable=False, index=True)
    engine_version = Column(String(32), nullable=False, index=True)
    eval_status = Column(String(24), nullable=False, default='pending', index=True)
    outcome = Column(String(16), index=True)
    direction_correct = Column(Boolean)
    unable_reason = Column(String(64), index=True)
    analysis_date = Column(Date, index=True)
    start_trade_date = Column(Date, index=True)
    end_trade_date = Column(Date, index=True)
    start_price = Column(Float)
    end_close = Column(Float)
    stock_return_pct = Column(Float)
    directional_return_pct = Column(Float)
    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'skill_opinion_sample_id',
            'horizon',
            'engine_version',
            name='uix_skill_opinion_outcome_key',
        ),
        CheckConstraint(
            "horizon IN ('1d', '3d', '5d', '10d')",
            name='ck_skill_opinion_outcome_horizon',
        ),
        CheckConstraint(
            "eval_status IN ('pending', 'evaluated', 'observational', 'unable')",
            name='ck_skill_opinion_outcome_eval_status',
        ),
        CheckConstraint(
            "outcome IS NULL OR outcome IN ('hit', 'miss', 'observational')",
            name='ck_skill_opinion_outcome_value',
        ),
        CheckConstraint(
            "(eval_status IN ('pending', 'unable') "
            "AND outcome IS NULL "
            "AND direction_correct IS NULL "
            "AND directional_return_pct IS NULL) "
            "OR (eval_status = 'observational' "
            "AND outcome = 'observational' "
            "AND direction_correct IS NULL "
            "AND directional_return_pct IS NULL) "
            "OR (eval_status = 'evaluated' "
            "AND outcome IN ('hit', 'miss') "
            "AND direction_correct IS NOT NULL "
            "AND directional_return_pct IS NOT NULL)",
            name='ck_skill_opinion_outcome_state_fields',
        ),
        Index(
            'ix_skill_opinion_outcome_candidate',
            'engine_version',
            'eval_status',
            'updated_at',
        ),
        Index(
            'ix_skill_opinion_outcome_horizon_status',
            'engine_version',
            'horizon',
            'eval_status',
        ),
    )


class _DatabaseManagerMeta(type):
    """Serialize DatabaseManager construction across __new__ and __init__."""

    def __call__(cls, *args, **kwargs):
        with cls._init_lock:
            return super().__call__(*args, **kwargs)


class DatabaseManager(metaclass=_DatabaseManagerMeta):
    """
    数据库管理器 - 单例模式
    
    职责：
    1. 管理数据库连接池
    2. 提供 Session 上下文管理
    3. 封装数据存取操作
    """
    
    _instance: Optional['DatabaseManager'] = None
    _init_lock = threading.RLock()
    _initialized: bool = False
    
    def __new__(cls, *args, **kwargs):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_url: Optional[str] = None):
        """
        初始化数据库管理器
        
        Args:
            db_url: 数据库连接 URL（可选，默认从配置读取）
        """
        if getattr(self, '_initialized', False):
            return

        created_engine = None

        try:
            config = get_config()
            if db_url is None:
                db_url = config.get_db_url()

            self._db_url = db_url
            self._sqlite_wal_enabled = config.sqlite_wal_enabled
            self._sqlite_busy_timeout_ms = config.sqlite_busy_timeout_ms
            self._sqlite_write_retry_max = config.sqlite_write_retry_max
            self._sqlite_write_retry_base_delay = config.sqlite_write_retry_base_delay

            engine_kwargs = {
                "echo": False,
                "pool_pre_ping": True,
            }
            if str(db_url).startswith("sqlite:") and self._sqlite_busy_timeout_ms > 0:
                engine_kwargs["connect_args"] = {
                    "timeout": self._sqlite_busy_timeout_ms / 1000,
                }

            # 创建数据库引擎
            created_engine = create_engine(
                db_url,
                **engine_kwargs,
            )
            self._engine = created_engine
            self._is_sqlite_engine = self._engine.url.get_backend_name() == 'sqlite'
            self._sqlite_file_db = self._is_sqlite_engine and self._is_file_sqlite_database()
            self._install_sqlite_pragma_handler()

            # 创建 Session 工厂
            self._SessionLocal = sessionmaker(
                bind=self._engine,
                autocommit=False,
                autoflush=False,
            )

            migration_mode = getattr(config, "database_migration_mode", "auto")
            if migration_mode == "explicit":
                state = check_migration_state(str(self._engine.url))
                if not state.is_current or not state.is_compatible or state.error:
                    detail = state.error or (
                        "pending=" + ",".join(state.pending_versions)
                    )
                    raise DatabaseMigrationRequired(
                        "Database schema is not current in explicit migration mode; "
                        "run `python -m src.migrations --apply` before starting "
                        f"this service ({detail})"
                    )
            else:
                # Non-Compose installations retain automatic convergence.  The
                # versioned migrator and this compatibility path share one lock.
                migration_lock_timeout = max(
                    30.0,
                    self._sqlite_busy_timeout_ms / 1000.0,
                )
                with migration_writer_lock(
                    str(self._engine.url),
                    timeout_seconds=migration_lock_timeout,
                ):
                    applied_now = apply_migrations_locked(self._engine)
                    if PR0_CONVERGENCE_SCHEMA_VERSION not in applied_now:
                        # Preserve historical repair-on-start behavior for old
                        # non-Compose deployments and out-of-band schema drift.
                        self._ensure_llm_usage_telemetry_columns()
                        self._ensure_decision_signal_profile_schema()
                        self._ensure_intelligence_item_scope_values()
                        self._ensure_intelligence_items_unique_index()

            self._initialized = True
            logger.info(f"数据库初始化完成: {db_url}")

            # 注册退出钩子，确保程序退出时关闭数据库连接
            atexit.register(DatabaseManager._cleanup_engine, self._engine)
        except Exception:
            self._initialized = False
            try:
                if created_engine is not None:
                    created_engine.dispose()
            except Exception as cleanup_exc:
                logger.warning("数据库初始化失败后的引擎清理也失败: %s", cleanup_exc)
            self._engine = None
            self._SessionLocal = None
            self.__class__._instance = None
            raise

    def _ensure_schema_migration_record(self) -> None:
        session = self._SessionLocal()
        values = {
            "version": CURRENT_SCHEMA_VERSION,
            "description": "Baseline schema created through SQLAlchemy metadata.create_all",
        }
        try:
            if self._is_sqlite_engine:
                statement = sqlite_insert(DatabaseSchemaMigration).values(**values)
                statement = statement.on_conflict_do_nothing(index_elements=["version"])
                session.execute(statement)
            else:
                session.execute(DatabaseSchemaMigration.__table__.insert().values(**values))
            session.commit()
        except IntegrityError:
            session.rollback()
            with self._SessionLocal() as verify_session:
                existing = verify_session.get(DatabaseSchemaMigration, CURRENT_SCHEMA_VERSION)
            if existing is None:
                raise
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _ensure_decision_signal_profile_schema(self) -> None:
        """Add and backfill nullable decision_profile for existing SQLite DBs."""

        if not self._is_sqlite_engine:
            return
        inspector = inspect(self._engine)
        if not inspector.has_table(DecisionSignalRecord.__tablename__):
            return

        try:
            existing = {
                column["name"]
                for column in inspector.get_columns(DecisionSignalRecord.__tablename__)
            }
        except Exception as exc:
            logger.error(
                "[DecisionSignal] failed to inspect decision_profile column; "
                "profile migration cannot continue safely: %s",
                exc,
            )
            raise

        if "decision_profile" not in existing:
            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(
                        f"ALTER TABLE {DecisionSignalRecord.__tablename__} "
                        "ADD COLUMN decision_profile VARCHAR(16)"
                    )
            except OperationalError as exc:
                if not self._is_sqlite_duplicate_column_error(exc, "decision_profile"):
                    raise

        self._ensure_decision_signal_profile_indexes()
        self._backfill_decision_signal_profile_from_metadata()

    def _ensure_decision_signal_profile_indexes(self) -> None:
        """Create profile-aware indexes without dropping legacy indexes."""

        expected_indexes = {
            "ix_decision_signals_decision_profile": ["decision_profile"],
            "ix_decision_signal_market_stock_profile_created": [
                "market", "stock_code", "decision_profile", "created_at",
            ],
            "ix_decision_signal_report_type_market_stock_profile_action_horizon_phase": [
                "source_report_id", "source_type", "market", "stock_code",
                "decision_profile", "action", "horizon", "market_phase",
            ],
            "ix_decision_signal_trace_type_market_stock_profile_action_horizon_phase": [
                "trace_id", "source_type", "market", "stock_code",
                "decision_profile", "action", "horizon", "market_phase",
            ],
        }
        with self._engine.begin() as connection:
            for index_name, columns in expected_indexes.items():
                connection.exec_driver_sql(
                    f"CREATE INDEX IF NOT EXISTS {index_name} "
                    f"ON decision_signals ({', '.join(columns)})"
                )

        actual_indexes = {
            index["name"]: index["column_names"]
            for index in inspect(self._engine).get_indexes(
                DecisionSignalRecord.__tablename__
            )
        }
        for index_name, expected_columns in expected_indexes.items():
            if actual_indexes.get(index_name) != expected_columns:
                raise RuntimeError(
                    "decision_profile index verification failed: "
                    f"index={index_name} expected={expected_columns} "
                    f"actual={actual_indexes.get(index_name)}"
                )

    def _backfill_decision_signal_profile_from_metadata(self) -> None:
        stats = {
            "candidate_count": 0,
            "backfilled_count": 0,
            "guard_skipped_count": 0,
            "missing_metadata_count": 0,
            "missing_profile_count": 0,
            "invalid_json_count": 0,
            "non_object_count": 0,
            "invalid_profile_count": 0,
            "skipped_existing_profile_count": 0,
        }
        with self._engine.begin() as connection:
            stats["skipped_existing_profile_count"] = connection.execute(
                text(
                    "SELECT COUNT(*) FROM decision_signals "
                    "WHERE decision_profile IS NOT NULL"
                )
            ).scalar_one()
            candidate_rows = [
                (row["id"], row["metadata_json"])
                for row in connection.execute(
                    text(
                        "SELECT id, metadata_json FROM decision_signals "
                        "WHERE decision_profile IS NULL ORDER BY id"
                    )
                ).mappings()
            ]
            stats["candidate_count"] = len(candidate_rows)

            for signal_id, metadata_json in candidate_rows:
                if metadata_json is None:
                    stats["missing_metadata_count"] += 1
                    continue
                try:
                    metadata = json.loads(metadata_json)
                except (TypeError, ValueError, RecursionError):
                    stats["invalid_json_count"] += 1
                    continue
                if not isinstance(metadata, dict):
                    stats["non_object_count"] += 1
                    continue

                raw_profile = metadata.get("decision_profile")
                if raw_profile is None or (
                    isinstance(raw_profile, str) and not raw_profile.strip()
                ):
                    stats["missing_profile_count"] += 1
                    continue
                profile = extract_legacy_decision_profile(metadata)
                if profile is None:
                    stats["invalid_profile_count"] += 1
                    continue

                result = connection.execute(
                    text(
                        "UPDATE decision_signals "
                        "SET decision_profile = :decision_profile "
                        "WHERE id = :signal_id AND decision_profile IS NULL"
                    ),
                    {"decision_profile": profile, "signal_id": signal_id},
                )
                if result.rowcount == 1:
                    stats["backfilled_count"] += 1
                elif result.rowcount == 0:
                    stats["guard_skipped_count"] += 1
                else:
                    raise RuntimeError(
                        "decision_profile backfill updated an unexpected number "
                        f"of rows for signal_id={signal_id}: {result.rowcount}"
                    )

            classified_count = sum(
                stats[key]
                for key in (
                    "backfilled_count",
                    "guard_skipped_count",
                    "missing_metadata_count",
                    "missing_profile_count",
                    "invalid_json_count",
                    "non_object_count",
                    "invalid_profile_count",
                )
            )
            if classified_count != stats["candidate_count"]:
                raise RuntimeError(
                    "decision_profile migration stats did not classify every "
                    f"candidate: candidates={stats['candidate_count']} "
                    f"classified={classified_count}"
                )
        logger.info(
            "[DecisionSignal] decision_profile migration stats: "
            "candidate_count=%s backfilled_count=%s guard_skipped_count=%s "
            "missing_metadata_count=%s missing_profile_count=%s "
            "invalid_json_count=%s non_object_count=%s invalid_profile_count=%s "
            "skipped_existing_profile_count=%s",
            stats["candidate_count"],
            stats["backfilled_count"],
            stats["guard_skipped_count"],
            stats["missing_metadata_count"],
            stats["missing_profile_count"],
            stats["invalid_json_count"],
            stats["non_object_count"],
            stats["invalid_profile_count"],
            stats["skipped_existing_profile_count"],
        )

    def _ensure_intelligence_items_unique_index(self) -> None:
        if not self._is_sqlite_engine:
            return

        if not inspect(self._engine).has_table("intelligence_items"):
            return

        try:
            unique_indexes = self._list_sqlite_unique_indexes("intelligence_items")
        except Exception as exc:
            if getattr(self, "_strict_schema_convergence", False):
                raise RuntimeError(
                    "Failed to inspect intelligence_items unique indexes during "
                    "schema convergence"
                ) from exc
            logger.warning(
                "[Intelligence items] failed to inspect unique indexes; "
                "skip migration/repair: %s",
                exc,
            )
            return

        target_columns = ("source_id", "url", "scope_type", "scope_value", "market")
        has_target_index = any(tuple(cols) == target_columns for cols in unique_indexes)
        has_legacy_url_unique = any(tuple(cols) == ("url",) for cols in unique_indexes)
        has_source_foreign_key = self._has_intelligence_item_source_foreign_key()
        unknown_unique_shapes = [
            tuple(columns)
            for columns in unique_indexes
            if tuple(columns) not in {target_columns, ("url",)}
        ]

        if has_source_foreign_key is False and unknown_unique_shapes and getattr(
            self,
            "_strict_schema_convergence",
            False,
        ):
            raise RuntimeError(
                "Cannot safely rebuild intelligence_items with an unknown unique "
                f"index contract: {unknown_unique_shapes}"
            )

        if has_source_foreign_key is False and not unknown_unique_shapes:
            # Older repair code created the scoped unique index through a
            # columns-only temporary table, which silently lost this FK.  A
            # second model-complete rebuild repairs that deployed shape.
            self._rebuild_intelligence_items_table()
        elif not has_target_index and unique_indexes and not has_legacy_url_unique:
            # Table has other unique index shapes; avoid aggressive changes and add
            # the expected scoped uniqueness directly.
            self._ensure_intelligence_items_scoped_unique_index_once()
        elif not has_target_index:
            self._rebuild_intelligence_items_table()

        # ``create_all`` cannot repair indexes on a table that already exists.
        # Keep every query index declared by the ORM present after either the
        # legacy rebuild path or an out-of-band partial schema change.
        self._ensure_intelligence_item_model_indexes()

    def _rebuild_intelligence_items_table(self) -> None:
        temporary_table = f"intelligence_items_recreate_tmp_{int(time.time() * 1_000_000_000)}"
        columns = [column.name for column in IntelligenceItem.__table__.columns]
        select_clause = ", ".join(f'"{column}"' for column in columns)

        tmp_metadata = MetaData()
        # Clone the referenced table into the temporary metadata so SQLAlchemy
        # can compile the outgoing source_id foreign key on the replacement.
        IntelligenceSource.__table__.to_metadata(tmp_metadata)
        tmp_table = IntelligenceItem.__table__.to_metadata(
            tmp_metadata,
            name=temporary_table,
        )
        logger.info("Rebuilding intelligence_items table to align composite uniqueness constraints.")
        with self._engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{temporary_table}"'))
            # CreateTable preserves table-level constraints (including the
            # source_id foreign key and scoped unique constraint) without
            # prematurely creating globally named SQLite indexes that still
            # belong to the old table.  The model indexes are recreated after
            # the old table has been dropped and the replacement renamed.
            connection.execute(CreateTable(tmp_table))
            connection.execute(
                text(
                    f"INSERT INTO \"{temporary_table}\" ({select_clause}) "
                    f"SELECT {select_clause} FROM intelligence_items"
                )
            )
            connection.execute(text('DROP TABLE "intelligence_items"'))
            connection.execute(
                text(f'ALTER TABLE "{temporary_table}" RENAME TO intelligence_items')
            )
            self._create_intelligence_item_model_indexes(connection)

    @staticmethod
    def _create_intelligence_item_model_indexes(connection) -> None:
        for index in sorted(
            IntelligenceItem.__table__.indexes,
            key=lambda item: item.name or "",
        ):
            index.create(bind=connection, checkfirst=True)

    def _ensure_intelligence_item_model_indexes(self) -> None:
        with self._engine.begin() as connection:
            self._create_intelligence_item_model_indexes(connection)

    def _has_intelligence_item_source_foreign_key(self) -> Optional[bool]:
        try:
            foreign_keys = inspect(self._engine).get_foreign_keys(
                IntelligenceItem.__tablename__
            )
        except Exception as exc:
            if getattr(self, "_strict_schema_convergence", False):
                raise RuntimeError(
                    "Failed to inspect intelligence_items foreign keys during "
                    "schema convergence"
                ) from exc
            logger.warning(
                "[Intelligence items] failed to inspect foreign keys; "
                "skip foreign-key repair: %s",
                exc,
            )
            return None
        return any(
            tuple(foreign_key.get("constrained_columns") or ()) == ("source_id",)
            and foreign_key.get("referred_table") == "intelligence_sources"
            and tuple(foreign_key.get("referred_columns") or ()) == ("id",)
            and str((foreign_key.get("options") or {}).get("ondelete", "")).upper()
            == "SET NULL"
            for foreign_key in foreign_keys
        )

    def _ensure_intelligence_items_scoped_unique_index_once(self) -> None:
        target_index_name = "uix_intel_item_scope"
        with self._engine.begin() as connection:
            rows = connection.execute(
                text("PRAGMA index_list(intelligence_items)")
            ).fetchall()
            for row in rows:
                if row[1] == target_index_name:
                    return
            index_columns = ", ".join(["source_id", "url", "scope_type", "scope_value", "market"])
            connection.execute(
                text(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {target_index_name} ON "
                    f"intelligence_items ({index_columns})"
                )
            )

    def _list_sqlite_unique_indexes(self, table_name: str):
        with self._engine.connect() as connection:
            rows = connection.execute(
                text(f"PRAGMA index_list({table_name})")
            ).fetchall()
            unique_indexes = []
            for row in rows:
                # row: (seq, name, unique, origin, partial)
                if int(row[2]) != 1:
                    continue
                index_name = row[1]
                index_columns = []
                for index_info in connection.execute(
                    text(f"PRAGMA index_xinfo({index_name})")
                ).fetchall():
                    # index_xinfo: (seqno, cid, name, desc, coll, key, ... )
                    column_name = index_info[2]
                    if column_name is None:
                        continue
                    index_columns.append(column_name)
                unique_indexes.append(index_columns)
            return unique_indexes

    def _ensure_llm_usage_telemetry_columns(self) -> None:
        """Add nullable P0a usage telemetry columns to existing SQLite DBs."""
        if not self._is_sqlite_engine:
            return
        try:
            existing = {
                column["name"]
                for column in inspect(self._engine).get_columns(LLMUsage.__tablename__)
            }
        except Exception as exc:
            if getattr(self, "_strict_schema_convergence", False):
                raise RuntimeError(
                    "Failed to inspect llm_usage telemetry columns during schema convergence"
                ) from exc
            logger.warning(
                "[LLM usage] failed to inspect telemetry columns; "
                "skipping best-effort SQLite telemetry column backfill: %s",
                exc,
            )
            return

        max_retries = self._sqlite_write_retry_max
        for column, column_type in _LLM_USAGE_TELEMETRY_COLUMN_SQL.items():
            if column in existing:
                continue
            for attempt in range(max_retries + 1):
                try:
                    with self._engine.begin() as connection:
                        connection.exec_driver_sql(
                            f"ALTER TABLE {LLMUsage.__tablename__} "
                            f"ADD COLUMN {column} {column_type}"
                        )
                    existing.add(column)
                    break
                except OperationalError as exc:
                    if self._is_sqlite_duplicate_column_error(exc, column):
                        existing.add(column)
                        break
                    if self._is_sqlite_locked_error(exc) and attempt < max_retries:
                        delay = self._sqlite_write_retry_base_delay * (2 ** attempt)
                        logger.warning(
                            "[LLM usage] SQLite telemetry column backfill locked, "
                            "retrying: %s (%s/%s, %.2fs)",
                            column,
                            attempt + 1,
                            max_retries,
                            delay,
                        )
                        if delay > 0:
                            time.sleep(delay)
                        continue
                    raise

    def _ensure_intelligence_item_scope_values(self) -> None:
        """Backfill nullable intelligence item scopes so SQLite unique keys work."""
        if not self._is_sqlite_engine:
            return
        try:
            existing = {
                column["name"]
                for column in inspect(self._engine).get_columns(IntelligenceItem.__tablename__)
            }
        except Exception as exc:
            if getattr(self, "_strict_schema_convergence", False):
                raise RuntimeError(
                    "Failed to inspect intelligence_items scope_value during schema convergence"
                ) from exc
            logger.warning("资讯池 scope_value 回填检查失败，已跳过: %s", exc)
            return
        if "scope_value" not in existing:
            return
        try:
            with self._engine.begin() as connection:
                connection.exec_driver_sql(
                    f"UPDATE {IntelligenceItem.__tablename__} "
                    "SET scope_value = ? "
                    "WHERE scope_value IS NULL OR scope_value = ''",
                    (INTELLIGENCE_ITEM_NULL_SCOPE_VALUE,),
                )
        except Exception as exc:
            if getattr(self, "_strict_schema_convergence", False):
                raise RuntimeError(
                    "Failed to backfill intelligence_items scope_value during schema convergence"
                ) from exc
            logger.warning("资讯池 scope_value 回填失败，已跳过: %s", exc)

    @classmethod
    def get_instance(cls) -> 'DatabaseManager':
        """获取单例实例"""
        with cls._init_lock:
            if cls._instance is None:
                cls()
            return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（用于测试）"""
        with cls._init_lock:
            if cls._instance is not None:
                instance = cls._instance
                try:
                    if hasattr(instance, '_engine') and instance._engine is not None:
                        instance._engine.dispose()
                finally:
                    instance._initialized = False
                    cls._instance = None

    @classmethod
    def _cleanup_engine(cls, engine) -> None:
        """
        清理数据库引擎（atexit 钩子）

        确保程序退出时关闭所有数据库连接，避免 ResourceWarning

        Args:
            engine: SQLAlchemy 引擎对象
        """
        try:
            if engine is not None:
                engine.dispose()
                logger.debug("数据库引擎已清理")
        except Exception as e:
            logger.warning(f"清理数据库引擎时出错: {e}")

    def _install_sqlite_pragma_handler(self) -> None:
        """为 SQLite 连接安装竞争保护参数。"""
        if not self._is_sqlite_engine:
            return

        @event.listens_for(self._engine, "connect")
        def _configure_sqlite_connection(dbapi_connection, _connection_record) -> None:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(f"PRAGMA busy_timeout={int(self._sqlite_busy_timeout_ms)}")
                if self._sqlite_file_db and self._sqlite_wal_enabled:
                    cursor.execute("PRAGMA journal_mode=WAL")
            except Exception as exc:
                logger.warning("初始化 SQLite PRAGMA 失败: %s", exc)
            finally:
                cursor.close()

    def _is_file_sqlite_database(self) -> bool:
        database = (self._engine.url.database or "").strip()
        return bool(database) and database.lower() != ":memory:"

    def _run_write_transaction(
        self,
        operation_name: str,
        write_operation: Callable[[Session], T],
    ) -> T:
        max_retries = self._sqlite_write_retry_max if self._is_sqlite_engine else 0

        for attempt in range(max_retries + 1):
            session = self.get_session()
            try:
                if self._is_sqlite_engine:
                    # Acquire the SQLite writer lock before any reads inside
                    # `write_operation()` so pre-write existence checks and the
                    # later upsert share one consistent write window.
                    session.connection().exec_driver_sql("BEGIN IMMEDIATE")
                result = write_operation(session)
                session.commit()
                return result
            except OperationalError as exc:
                session.rollback()
                if (
                    self._is_sqlite_engine
                    and self._is_sqlite_locked_error(exc)
                    and attempt < max_retries
                ):
                    delay = self._sqlite_write_retry_base_delay * (2 ** attempt)
                    logger.warning(
                        "SQLite 写入锁冲突，准备重试: %s (%s/%s, %.2fs)",
                        operation_name,
                        attempt + 1,
                        max_retries,
                        delay,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue
                raise
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

    @staticmethod
    def _is_sqlite_locked_error(exc: OperationalError) -> bool:
        err_text = str(getattr(exc, "orig", exc)).lower()
        return any(
            token in err_text
            for token in (
                "database is locked",
                "database schema is locked",
                "database table is locked",
            )
        )

    @staticmethod
    def _is_sqlite_duplicate_column_error(exc: OperationalError, column: str) -> bool:
        err_text = str(getattr(exc, "orig", exc)).lower()
        return "duplicate column name" in err_text and column.lower() in err_text

    @staticmethod
    def _normalize_daily_date(value: Any) -> Any:
        if isinstance(value, str):
            return datetime.strptime(value, '%Y-%m-%d').date()
        if isinstance(value, pd.Timestamp):
            return value.date()
        if isinstance(value, datetime):
            return value.date()
        return value

    @staticmethod
    def _normalize_sql_value(value: Any) -> Any:
        return None if pd.isna(value) else value
    
    def get_session(self) -> Session:
        """
        获取数据库 Session
        
        使用示例:
            with db.get_session() as session:
                # 执行查询
                session.commit()  # 如果需要
        """
        if not getattr(self, '_initialized', False) or not hasattr(self, '_SessionLocal'):
            raise RuntimeError(
                "DatabaseManager 未正确初始化。"
                "请确保通过 DatabaseManager.get_instance() 获取实例。"
            )
        session = self._SessionLocal()
        try:
            return session
        except Exception:
            session.close()
            raise

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def has_today_data(self, code: str, target_date: Optional[date] = None) -> bool:
        """
        检查是否已有指定日期的数据
        
        用于断点续传逻辑：如果已有数据则跳过网络请求
        
        Args:
            code: 股票代码
            target_date: 目标日期（默认今天）
            
        Returns:
            是否存在数据
        """
        if target_date is None:
            target_date = date.today()
        # 注意：这里的 target_date 语义是“自然日”，而不是“最新交易日”。
        # 在周末/节假日/非交易日运行时，即使数据库已有最新交易日数据，这里也会返回 False。
        # 该行为目前保留（按需求不改逻辑）。
        
        with self.get_session() as session:
            result = session.execute(
                select(StockDaily).where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date == target_date
                    )
                )
            ).scalar_one_or_none()
            
            return result is not None
    
    def get_latest_data(
        self, 
        code: str, 
        days: int = 2
    ) -> List[StockDaily]:
        """
        获取最近 N 天的数据
        
        用于计算"相比昨日"的变化
        
        Args:
            code: 股票代码
            days: 获取天数
            
        Returns:
            StockDaily 对象列表（按日期降序）
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(StockDaily.code == code)
                .order_by(desc(StockDaily.date))
                .limit(days)
            ).scalars().all()
            
            return list(results)

    def save_news_intel(
        self,
        code: str,
        name: str,
        dimension: str,
        query: str,
        response: 'SearchResponse',
        query_context: Optional[Dict[str, str]] = None
    ) -> int:
        """
        保存新闻情报到数据库

        去重策略：
        - 优先按 URL 去重（唯一约束）
        - URL 缺失时按 title + source + published_date 进行软去重

        关联策略：
        - query_context 记录用户查询信息（平台、用户、会话、原始指令等）
        """
        if not response or not response.results:
            return 0

        saved_count = 0
        query_ctx = query_context or {}
        current_query_id = (query_ctx.get("query_id") or "").strip()

        def _write(session: Session) -> int:
            local_saved_count = 0

            for item in response.results:
                title = (item.title or '').strip()
                url = (item.url or '').strip()
                source = (item.source or '').strip()
                snippet = (item.snippet or '').strip()
                published_date = self._parse_published_date(item.published_date)

                if not title and not url:
                    continue

                url_key = url or self._build_fallback_url_key(
                    code=code,
                    title=title,
                    source=source,
                    published_date=published_date
                )

                existing = session.execute(
                    select(NewsIntel).where(NewsIntel.url == url_key)
                ).scalar_one_or_none()

                if existing:
                    existing.name = name or existing.name
                    existing.dimension = dimension or existing.dimension
                    existing.query = query or existing.query
                    existing.provider = response.provider or existing.provider
                    existing.snippet = snippet or existing.snippet
                    existing.source = source or existing.source
                    existing.published_date = published_date or existing.published_date
                    existing.fetched_at = datetime.now()

                    if query_context:
                        if not existing.query_id and current_query_id:
                            existing.query_id = current_query_id
                        existing.query_source = (
                            query_context.get("query_source") or existing.query_source
                        )
                        existing.requester_platform = (
                            query_context.get("requester_platform") or existing.requester_platform
                        )
                        existing.requester_user_id = (
                            query_context.get("requester_user_id") or existing.requester_user_id
                        )
                        existing.requester_user_name = (
                            query_context.get("requester_user_name") or existing.requester_user_name
                        )
                        existing.requester_chat_id = (
                            query_context.get("requester_chat_id") or existing.requester_chat_id
                        )
                        existing.requester_message_id = (
                            query_context.get("requester_message_id") or existing.requester_message_id
                        )
                        existing.requester_query = (
                            query_context.get("requester_query") or existing.requester_query
                        )
                    continue

                try:
                    with session.begin_nested():
                        record = NewsIntel(
                            code=code,
                            name=name,
                            dimension=dimension,
                            query=query,
                            provider=response.provider,
                            title=title,
                            snippet=snippet,
                            url=url_key,
                            source=source,
                            published_date=published_date,
                            fetched_at=datetime.now(),
                            query_id=current_query_id or None,
                            query_source=query_ctx.get("query_source"),
                            requester_platform=query_ctx.get("requester_platform"),
                            requester_user_id=query_ctx.get("requester_user_id"),
                            requester_user_name=query_ctx.get("requester_user_name"),
                            requester_chat_id=query_ctx.get("requester_chat_id"),
                            requester_message_id=query_ctx.get("requester_message_id"),
                            requester_query=query_ctx.get("requester_query"),
                        )
                        session.add(record)
                        session.flush()
                    local_saved_count += 1
                except IntegrityError:
                    logger.debug("新闻情报重复（已跳过）: %s %s", code, url_key)

            return local_saved_count

        try:
            saved_count = self._run_write_transaction(
                f"save_news_intel[{code}]",
                _write,
            )
            logger.info(f"保存新闻情报成功: {code}, 新增 {saved_count} 条")
        except Exception as e:
            logger.error(f"保存新闻情报失败: {e}")
            raise

        return saved_count

    def save_fundamental_snapshot(
        self,
        query_id: str,
        code: str,
        payload: Optional[Dict[str, Any]],
        source_chain: Optional[Any] = None,
        coverage: Optional[Any] = None,
    ) -> int:
        """
        保存基本面快照（P0 write-only）。失败不抛异常，返回写入条数 0/1。
        """
        if not query_id or not code or payload is None:
            return 0

        try:
            def _write(session: Session) -> int:
                session.add(
                    FundamentalSnapshot(
                        query_id=query_id,
                        code=code,
                        payload=self._safe_json_dumps(payload),
                        source_chain=self._safe_json_dumps(source_chain or []),
                        coverage=self._safe_json_dumps(coverage or {}),
                    )
                )
                return 1
            return self._run_write_transaction(
                f"save_fundamental_snapshot[{query_id}:{code}]",
                _write,
            )
        except Exception as e:
            logger.debug(
                "基本面快照写入失败（fail-open）: query_id=%s code=%s err=%s",
                query_id,
                code,
                e,
            )
            return 0

    def get_latest_fundamental_snapshot(
        self,
        query_id: str,
        code: str,
    ) -> Optional[Dict[str, Any]]:
        """
        获取指定 query_id + code 的最新基本面快照 payload。

        读取失败或不存在时返回 None（fail-open）。
        """
        if not query_id or not code:
            return None

        with self.get_session() as session:
            try:
                row = session.execute(
                    select(FundamentalSnapshot)
                    .where(
                        and_(
                            FundamentalSnapshot.query_id == query_id,
                            FundamentalSnapshot.code == code,
                        )
                    )
                    .order_by(desc(FundamentalSnapshot.created_at))
                    .limit(1)
                ).scalar_one_or_none()
            except Exception as e:
                logger.debug(
                    "基本面快照读取失败（fail-open）: query_id=%s code=%s err=%s",
                    query_id,
                    code,
                    e,
                )
                return None

            if row is None:
                return None
            try:
                payload = json.loads(row.payload or "{}")
                return payload if isinstance(payload, dict) else None
            except Exception:
                return None

    def save_screening_run(self, payload: Dict[str, Any]) -> int:
        """Persist one completed screening response without blocking screening on DB errors."""
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id:
            return 0
        normalized_payload = dict(payload)
        warnings = self._screening_warning_values(normalized_payload)
        normalized_payload["warnings"] = warnings

        values = {
            "strategy": str(normalized_payload.get("strategy") or "").strip() or "unknown",
            "market": str(normalized_payload.get("market") or "").strip() or "cn",
            "snapshot_source": str(normalized_payload.get("snapshot_source") or "").strip() or None,
            "snapshot_count": self._optional_int(normalized_payload.get("snapshot_count")),
            "after_filter_count": self._optional_int(normalized_payload.get("after_filter_count")),
            "candidate_count": self._optional_int(normalized_payload.get("candidate_count")) or 0,
            "llm_ranked": self._optional_bool(normalized_payload.get("llm_ranked")),
            "daily_enriched": self._optional_bool(normalized_payload.get("daily_enriched")),
            "source_errors_json": self._safe_json_dumps(normalized_payload.get("source_errors") or []),
            "warnings_json": self._safe_json_dumps(warnings),
            "result_json": self._safe_json_dumps(normalized_payload),
        }

        try:
            def _write(session: Session) -> int:
                row = session.execute(
                    select(ScreeningRun).where(ScreeningRun.run_id == run_id)
                ).scalar_one_or_none()
                if row is None:
                    session.add(ScreeningRun(run_id=run_id, **values))
                else:
                    for key, value in values.items():
                        setattr(row, key, value)
                return 1

            return self._run_write_transaction(
                f"save_screening_run[{run_id}]",
                _write,
            )
        except Exception as exc:
            logger.warning(
                "选股运行历史写入失败（fail-open）: run_id=%s err=%s",
                run_id,
                exc,
            )
            return 0

    def list_screening_runs(
        self,
        *,
        limit: int = 20,
        strategy: Optional[str] = None,
        market: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List recent screening runs as compact summaries."""
        normalized_limit = max(0, min(int(limit), 100))
        if normalized_limit <= 0:
            return []

        with self.get_session() as session:
            statement = select(ScreeningRun)
            if strategy:
                statement = statement.where(ScreeningRun.strategy == str(strategy).strip())
            if market:
                statement = statement.where(ScreeningRun.market == str(market).strip())
            rows = session.execute(
                statement.order_by(desc(ScreeningRun.created_at), desc(ScreeningRun.id)).limit(normalized_limit)
            ).scalars().all()
            return [self._screening_run_to_dict(row, include_result=False) for row in rows]

    def get_screening_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Load a completed screening run by its stable run id."""
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return None
        with self.get_session() as session:
            row = session.execute(
                select(ScreeningRun).where(ScreeningRun.run_id == normalized_run_id)
            ).scalar_one_or_none()
            if row is None:
                return None
            return self._screening_run_to_dict(row, include_result=True)

    @staticmethod
    def _optional_int(value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _optional_bool(value: Any) -> Optional[bool]:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "on"}:
                return True
            if normalized in {"false", "0", "no", "off"}:
                return False
            return None
        return bool(value)

    @staticmethod
    def _screening_json_list(value: Optional[str]) -> List[Any]:
        try:
            decoded = json.loads(value or "[]")
        except (TypeError, ValueError):
            return []
        return decoded if isinstance(decoded, list) else []

    @staticmethod
    def _screening_text_list(value: Any) -> List[str]:
        if isinstance(value, list):
            result = []
            for item in value:
                text = str(item or "").strip()
                if text:
                    result.append(text)
            return result
        text = str(value or "").strip()
        return [text] if text else []

    @classmethod
    def _screening_warning_values(cls, payload: Dict[str, Any]) -> List[str]:
        warnings: List[str] = []
        seen: set[str] = set()
        for key in ("warnings", "degradation"):
            for item in cls._screening_text_list(payload.get(key)):
                if item in seen:
                    continue
                seen.add(item)
                warnings.append(item)
        return warnings

    @classmethod
    def _screening_run_to_dict(
        cls,
        row: ScreeningRun,
        *,
        include_result: bool,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "run_id": row.run_id,
            "strategy": row.strategy,
            "market": row.market,
            "snapshot_source": row.snapshot_source or "",
            "snapshot_count": row.snapshot_count,
            "after_filter_count": row.after_filter_count,
            "candidate_count": row.candidate_count,
            "llm_ranked": row.llm_ranked,
            "daily_enriched": row.daily_enriched,
            "source_errors": cls._screening_json_list(row.source_errors_json),
            "warnings": cls._screening_json_list(row.warnings_json),
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        if include_result:
            try:
                result = json.loads(row.result_json or "{}")
            except (TypeError, ValueError):
                result = {}
            payload["result"] = result if isinstance(result, dict) else {}
        return payload

    def get_recent_news(self, code: str, days: int = 7, limit: int = 20) -> List[NewsIntel]:
        """
        获取指定股票最近 N 天的新闻情报
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            results = session.execute(
                select(NewsIntel)
                .where(
                    and_(
                        NewsIntel.code == code,
                        NewsIntel.fetched_at >= cutoff_date
                    )
                )
                .order_by(desc(NewsIntel.fetched_at))
                .limit(limit)
            ).scalars().all()

            return list(results)

    def get_news_intel_by_query_id(self, query_id: str, limit: int = 20) -> List[NewsIntel]:
        """
        根据 query_id 获取新闻情报列表

        Args:
            query_id: 分析记录唯一标识
            limit: 返回数量限制

        Returns:
            NewsIntel 列表（按发布时间或抓取时间倒序）
        """
        from sqlalchemy import func

        with self.get_session() as session:
            results = session.execute(
                select(NewsIntel)
                .where(NewsIntel.query_id == query_id)
                .order_by(
                    desc(func.coalesce(NewsIntel.published_date, NewsIntel.fetched_at)),
                    desc(NewsIntel.fetched_at)
                )
                .limit(limit)
            ).scalars().all()

            return list(results)

    def save_analysis_history(
        self,
        result: Any,
        query_id: str,
        report_type: str,
        news_content: Optional[str],
        context_snapshot: Optional[Dict[str, Any]] = None,
        save_snapshot: bool = True
    ) -> int:
        """
        保存分析结果历史记录。

        Returns:
            新保存的 AnalysisHistory.id；保存失败返回 0。
        """
        if result is None:
            return 0

        durable_context = None
        try:
            # Imported lazily to keep storage independent of the worker
            # runtime during legacy/flag-off startup.
            from src.services.durable_job_handlers import (
                get_optional_durable_execution_context,
            )

            durable_context = get_optional_durable_execution_context()
        except (ImportError, RuntimeError):
            durable_context = None
        durable_job_id = durable_context.job_id if durable_context is not None else None

        sniper_points = self._extract_sniper_points(result)
        raw_result = self._build_raw_result(result)
        context_text = None
        if save_snapshot and context_snapshot is not None:
            context_text = self._safe_json_dumps(context_snapshot)

        try:
            def _write(session: Session) -> Tuple[int, Optional[Dict[str, Any]]]:
                if durable_context is not None:
                    live_job_id = session.execute(
                        select(AnalysisJobRecord.task_id).where(
                            AnalysisJobRecord.task_id == durable_context.job_id,
                            AnalysisJobRecord.status == "processing",
                            AnalysisJobRecord.lease_owner == durable_context.worker_id,
                            AnalysisJobRecord.lease_token == durable_context.lease_token,
                            AnalysisJobRecord.lease_expires_at > utc_naive_now(),
                        )
                    ).scalar_one_or_none()
                    if live_job_id is None:
                        from src.services.durable_jobs import StaleLeaseError

                        raise StaleLeaseError(
                            "durable analysis history write rejected after lease loss"
                        )
                    existing = session.execute(
                        select(AnalysisHistory).where(
                            AnalysisHistory.job_id == durable_context.job_id,
                            AnalysisHistory.code == result.code,
                            AnalysisHistory.report_type == report_type,
                        )
                    ).scalar_one_or_none()
                    if existing is not None:
                        return (
                            int(existing.id),
                            self._snapshot_frozen_analysis_history(existing),
                        )
                history = AnalysisHistory(
                    query_id=query_id,
                    job_id=durable_job_id,
                    code=result.code,
                    name=result.name,
                    report_type=report_type,
                    sentiment_score=result.sentiment_score,
                    operation_advice=result.operation_advice,
                    trend_prediction=result.trend_prediction,
                    analysis_summary=result.analysis_summary,
                    raw_result=self._safe_json_dumps(raw_result),
                    news_content=news_content,
                    context_snapshot=context_text,
                    ideal_buy=sniper_points.get("ideal_buy"),
                    secondary_buy=sniper_points.get("secondary_buy"),
                    stop_loss=sniper_points.get("stop_loss"),
                    take_profit=sniper_points.get("take_profit"),
                    created_at=datetime.now(),
                )
                session.add(history)
                session.flush()
                return int(history.id or 0), None
            history_id, frozen_history = self._run_write_transaction(
                f"save_analysis_history[{result.code}]",
                _write,
            )
            if frozen_history is not None:
                self._restore_analysis_result_from_frozen_history(
                    result,
                    frozen_history,
                )
            return history_id
        except Exception as e:
            logger.error(f"保存分析历史失败: {e}")
            if durable_context is not None:
                # Durable completion is invalid without its auditable report.
                # Let the worker retry/fail under the job attempt budget;
                # flag-off callers retain the historical zero return below.
                raise
            return 0

    def update_analysis_history_diagnostics(
        self,
        *,
        query_id: str,
        code: Optional[str] = None,
        diagnostics: Optional[Dict[str, Any]] = None,
        notification_runs: Optional[List[Dict[str, Any]]] = None,
    ) -> int:
        """
        更新已保存分析历史的运行诊断快照。

        通知结果通常在分析历史落库后才产生，因此这里仅补写
        context_snapshot.diagnostics，不改变报告正文或其它历史字段。
        """
        if not query_id or (diagnostics is None and not notification_runs):
            return 0

        try:
            def _write(session: Session) -> int:
                conditions = [AnalysisHistory.query_id == query_id]
                if code:
                    conditions.append(AnalysisHistory.code == code)

                row = session.execute(
                    select(AnalysisHistory)
                    .where(and_(*conditions))
                    .order_by(desc(AnalysisHistory.created_at))
                    .limit(1)
                ).scalars().first()
                if row is None:
                    return 0

                context_snapshot: Dict[str, Any] = {}
                if row.context_snapshot:
                    try:
                        parsed = json.loads(row.context_snapshot)
                        if isinstance(parsed, dict):
                            context_snapshot = parsed
                    except Exception:
                        context_snapshot = {}

                if diagnostics is not None:
                    context_snapshot["diagnostics"] = diagnostics
                else:
                    existing_diagnostics = context_snapshot.get("diagnostics")
                    if not isinstance(existing_diagnostics, dict):
                        existing_diagnostics = {
                            "query_id": query_id,
                            "stock_code": code,
                            "notification_runs": [],
                        }
                    runs = existing_diagnostics.get("notification_runs")
                    if not isinstance(runs, list):
                        runs = []
                    trace_id = existing_diagnostics.get("trace_id")
                    for run in notification_runs or []:
                        if isinstance(run, dict):
                            run_payload = dict(run)
                            if trace_id and not run_payload.get("trace_id"):
                                run_payload["trace_id"] = trace_id
                            runs.append(run_payload)
                    existing_diagnostics["notification_runs"] = runs
                    context_snapshot["diagnostics"] = existing_diagnostics
                row.context_snapshot = self._safe_json_dumps(context_snapshot)
                return 1

            return self._run_write_transaction(
                f"update_analysis_history_diagnostics[{query_id}:{code or '*'}]",
                _write,
            )
        except Exception as e:
            logger.warning(
                "更新分析历史诊断快照失败（fail-open）: query_id=%s code=%s err=%s",
                query_id,
                code,
                e,
            )
            return 0

    def get_analysis_history(
        self,
        code: Optional[str] = None,
        query_id: Optional[str] = None,
        days: int = 30,
        limit: int = 50,
        exclude_query_id: Optional[str] = None,
    ) -> List[AnalysisHistory]:
        """
        Query analysis history records.

        Notes:
        - If query_id is provided, perform exact lookup and ignore days window.
        - If query_id is not provided, apply days-based time filtering.
        - exclude_query_id: exclude records with this query_id (for history comparison).
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            conditions = []

            if query_id:
                conditions.append(AnalysisHistory.query_id == query_id)
            else:
                conditions.append(AnalysisHistory.created_at >= cutoff_date)

            if code:
                conditions.append(AnalysisHistory.code == code)

            # exclude_query_id only applies when not doing exact lookup (query_id is None)
            if exclude_query_id and not query_id:
                conditions.append(AnalysisHistory.query_id != exclude_query_id)

            results = session.execute(
                select(AnalysisHistory)
                .where(and_(*conditions))
                .order_by(desc(AnalysisHistory.created_at))
                .limit(limit)
            ).scalars().all()

            return list(results)

    def get_latest_analysis_history_id(
        self,
        *,
        query_id: str,
        code: str,
        report_type: str,
    ) -> Optional[int]:
        """Return the latest matching history id for read-only lookups.

        P2 automatic DecisionSignal extraction receives the freshly saved id
        directly from ``save_analysis_history()`` and does not use this helper.
        """

        if not query_id or not code or not report_type:
            return None

        with self.get_session() as session:
            return session.execute(
                select(AnalysisHistory.id)
                .where(
                    AnalysisHistory.query_id == query_id,
                    AnalysisHistory.code == code,
                    AnalysisHistory.report_type == report_type,
                )
                .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
                .limit(1)
            ).scalar_one_or_none()
    
    def get_analysis_history_paginated(
        self,
        code: Optional[Union[str, List[str]]] = None,
        report_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Tuple[List[AnalysisHistory], int]:
        """
        分页查询分析历史记录（带总数）
        
        Args:
            code: 股票代码筛选
            report_type: 报告类型筛选
            start_date: 开始日期（含）
            end_date: 结束日期（含）
            offset: 偏移量（跳过前 N 条）
            limit: 每页数量
            
        Returns:
            Tuple[List[AnalysisHistory], int]: (记录列表, 总数)
        """
        from sqlalchemy import func
        
        with self.get_session() as session:
            conditions = []
            
            if code:
                if isinstance(code, list):
                    codes = [c for c in code if c]
                    if codes:
                        conditions.append(AnalysisHistory.code.in_(codes))
                else:
                    conditions.append(AnalysisHistory.code == code)
            if report_type:
                conditions.append(AnalysisHistory.report_type == report_type)
            if start_date:
                # created_at >= start_date 00:00:00
                conditions.append(AnalysisHistory.created_at >= datetime.combine(start_date, datetime.min.time()))
            if end_date:
                # created_at < end_date+1 00:00:00 (即 <= end_date 23:59:59)
                conditions.append(AnalysisHistory.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time()))
            
            # 构建 where 子句
            where_clause = and_(*conditions) if conditions else True
            
            # 查询总数
            total_query = select(func.count(AnalysisHistory.id)).where(where_clause)
            total = session.execute(total_query).scalar() or 0
            
            # 查询分页数据
            data_query = (
                select(AnalysisHistory)
                .where(where_clause)
                .order_by(desc(AnalysisHistory.created_at))
                .offset(offset)
                .limit(limit)
            )
            results = session.execute(data_query).scalars().all()
            
            return list(results), total
    
    def get_analysis_history_by_id(self, record_id: int) -> Optional[AnalysisHistory]:
        """
        根据数据库主键 ID 查询单条分析历史记录
        
        由于 query_id 可能重复（批量分析时多条记录共享同一 query_id），
        使用主键 ID 确保精确查询唯一记录。
        
        Args:
            record_id: 分析历史记录的主键 ID
            
        Returns:
            AnalysisHistory 对象，不存在返回 None
        """
        with self.get_session() as session:
            result = session.execute(
                select(AnalysisHistory).where(AnalysisHistory.id == record_id)
            ).scalars().first()
            return result

    def delete_analysis_history_records(self, record_ids: List[int]) -> int:
        """
        删除指定的分析历史记录。

        同时清理依赖这些历史记录的回测结果和分析来源决策信号，避免
        依赖历史记录的派生数据残留。DecisionSignal 的 source_report_id
        允许弱引用，因此这里只清理 source_type=analysis 的真实历史绑定信号。

        Args:
            record_ids: 要删除的历史记录主键 ID 列表

        Returns:
            实际删除的历史记录数量
        """
        ids = sorted({int(record_id) for record_id in record_ids if record_id is not None})
        if not ids:
            return 0

        def _write(session: Session) -> int:
            existing_ids = sorted(
                session.execute(
                    select(AnalysisHistory.id).where(AnalysisHistory.id.in_(ids))
                ).scalars().all()
            )
            if not existing_ids:
                return 0

            # Formal personal-research signals are immutable lineage assets.
            # ``source_report_id`` is a weak provenance reference, so deleting
            # an old rendered report must preserve the signal and its Policy,
            # Thesis, and Outcome v2 descendants. Legacy report-bound signals
            # retain the historical cleanup behavior.
            linked_signal_ids = sorted(
                session.execute(
                    select(DecisionSignalRecord.id).where(
                        and_(
                            DecisionSignalRecord.source_type == "analysis",
                            DecisionSignalRecord.source_report_id.in_(existing_ids),
                            DecisionSignalRecord.research_snapshot_hash.is_(None),
                            DecisionSignalRecord.policy_evaluation_hash.is_(None),
                        )
                    )
                ).scalars().all()
            )
            if linked_signal_ids:
                session.execute(
                    delete(DecisionSignalOutcomeRecord).where(
                        DecisionSignalOutcomeRecord.signal_id.in_(linked_signal_ids)
                    )
                )
                session.execute(
                    delete(DecisionSignalFeedbackRecord).where(
                        DecisionSignalFeedbackRecord.signal_id.in_(linked_signal_ids)
                    )
                )
                session.execute(
                    delete(DecisionSignalRecord).where(DecisionSignalRecord.id.in_(linked_signal_ids))
                )
            session.execute(
                delete(BacktestResult).where(BacktestResult.analysis_history_id.in_(existing_ids))
            )
            linked_skill_sample_ids = sorted(
                session.execute(
                    select(SkillOpinionSampleRecord.id).where(
                        SkillOpinionSampleRecord.analysis_history_id.in_(existing_ids)
                    )
                ).scalars().all()
            )
            if linked_skill_sample_ids:
                session.execute(
                    delete(SkillOpinionOutcomeRecord).where(
                        SkillOpinionOutcomeRecord.skill_opinion_sample_id.in_(
                            linked_skill_sample_ids
                        )
                    )
                )
            session.execute(
                delete(SkillOpinionSampleRecord).where(
                    SkillOpinionSampleRecord.analysis_history_id.in_(existing_ids)
                )
            )
            result = session.execute(
                delete(AnalysisHistory).where(AnalysisHistory.id.in_(existing_ids))
            )
            return result.rowcount or 0

        return self._run_write_transaction(
            "delete analysis history records",
            _write,
        )

    def get_distinct_stocks_from_history(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 200,
        include_market_review: bool = False,
    ) -> List[AnalysisHistory]:
        """
        获取历史记录中的不重复股票列表，每只股票取最新一条记录。

        使用子查询按 code 分组取 MAX(id)，再 JOIN 回查完整记录。
        默认排除大盘复盘，避免混入普通个股栏。

        Args:
            start_date: 开始日期
            end_date: 结束日期
            limit: 最大返回数量
            include_market_review: 是否包含大盘复盘记录

        Returns:
            每条股票最新一条 AnalysisHistory 记录列表
        """
        with self.get_session() as session:
            subq = (
                select(
                    AnalysisHistory.code,
                    func.max(AnalysisHistory.id).label("max_id"),
                )
            )
            if start_date:
                subq = subq.where(
                    AnalysisHistory.created_at >= datetime.combine(start_date, datetime.min.time())
                )
            if end_date:
                subq = subq.where(
                    AnalysisHistory.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time())
                )
            if not include_market_review:
                subq = subq.where(
                    and_(
                        AnalysisHistory.code != "MARKET",
                        or_(
                            AnalysisHistory.report_type.is_(None),
                            AnalysisHistory.report_type != "market_review",
                        ),
                    )
                )
            subq = subq.group_by(AnalysisHistory.code).subquery()

            results = (
                session.execute(
                    select(AnalysisHistory)
                    .join(subq, AnalysisHistory.id == subq.c.max_id)
                    .order_by(
                        desc(AnalysisHistory.created_at),
                    )
                    .limit(limit)
                )
                .scalars()
                .all()
            )
            return list(results)

    def get_latest_analysis_by_query_id(
        self,
        query_id: str,
        *,
        code: Optional[str] = None,
        report_type: Optional[str] = None,
    ) -> Optional[AnalysisHistory]:
        """
        根据 query_id 查询最新一条分析历史记录

        query_id 在批量分析时可能重复，故返回最近创建的一条。

        Args:
            query_id: 分析记录关联的 query_id
            code: 可选股票代码过滤，用于区分同一 query_id 下的 MARKET 与个股记录
            report_type: 可选报告类型过滤

        Returns:
            AnalysisHistory 对象，不存在返回 None
        """
        with self.get_session() as session:
            conditions = [AnalysisHistory.query_id == query_id]
            if code:
                conditions.append(AnalysisHistory.code == code)
            if report_type:
                conditions.append(AnalysisHistory.report_type == report_type)

            result = session.execute(
                select(AnalysisHistory)
                .where(and_(*conditions))
                .order_by(desc(AnalysisHistory.created_at))
                .limit(1)
            ).scalars().first()
            return result
    
    def get_data_range(
        self, 
        code: str, 
        start_date: date, 
        end_date: date
    ) -> List[StockDaily]:
        """
        获取指定日期范围的数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            StockDaily 对象列表
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date >= start_date,
                        StockDaily.date <= end_date
                    )
                )
                .order_by(StockDaily.date)
            ).scalars().all()
            
            return list(results)
    
    def save_daily_data(
        self, 
        df: pd.DataFrame, 
        code: str,
        data_source: str = "Unknown",
        *,
        lease_fence: Optional[Any] = None,
    ) -> int:
        """
        保存日线数据到数据库
        
        策略：
        - 按 `(code, date)` 做批量 UPSERT，已存在记录会覆盖更新
        - 同一批次内若存在重复日期，以最后一条记录为准
        - SQLite 分支按 chunk 写入以避免绑定参数上限
        
        Args:
            df: 包含日线数据的 DataFrame
            code: 股票代码
            data_source: 数据来源名称
            
        Returns:
            本次实际新增的记录数（不含更新）
        """
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0

        now = datetime.now()
        records_by_date: Dict[date, Dict[str, Any]] = {}
        for row in df.to_dict(orient='records'):
            row_date = self._normalize_daily_date(row.get('date'))
            records_by_date[row_date] = {
                'code': code,
                'date': row_date,
                'open': self._normalize_sql_value(row.get('open')),
                'high': self._normalize_sql_value(row.get('high')),
                'low': self._normalize_sql_value(row.get('low')),
                'close': self._normalize_sql_value(row.get('close')),
                'volume': self._normalize_sql_value(row.get('volume')),
                'amount': self._normalize_sql_value(row.get('amount')),
                'pct_chg': self._normalize_sql_value(row.get('pct_chg')),
                'ma5': self._normalize_sql_value(row.get('ma5')),
                'ma10': self._normalize_sql_value(row.get('ma10')),
                'ma20': self._normalize_sql_value(row.get('ma20')),
                'volume_ratio': self._normalize_sql_value(row.get('volume_ratio')),
                'data_source': data_source,
                'created_at': now,
                'updated_at': now,
            }

        if not records_by_date:
            return 0

        records = list(records_by_date.values())
        batch_dates = list(records_by_date.keys())

        def _write(session: Session) -> int:
            if lease_fence is not None:
                job_id = str(getattr(lease_fence, 'job_id', '') or '').strip()
                worker_id = str(getattr(lease_fence, 'worker_id', '') or '').strip()
                lease_token = str(
                    getattr(lease_fence, 'lease_token', '') or ''
                ).strip()
                if not job_id or not worker_id or not lease_token:
                    raise ValueError(
                        'lease_fence must expose job_id, worker_id, and lease_token'
                    )
                live_job_id = session.execute(
                    select(AnalysisJobRecord.task_id).where(
                        AnalysisJobRecord.task_id == job_id,
                        AnalysisJobRecord.status == 'processing',
                        AnalysisJobRecord.cancel_requested_at.is_(None),
                        AnalysisJobRecord.lease_owner == worker_id,
                        AnalysisJobRecord.lease_token == lease_token,
                        AnalysisJobRecord.lease_expires_at.is_not(None),
                        AnalysisJobRecord.lease_expires_at > utc_naive_now(),
                    )
                ).scalar_one_or_none()
                if live_job_id is None:
                    from src.services.durable_jobs import StaleLeaseError

                    raise StaleLeaseError(
                        'daily data write rejected after durable lease loss'
                    )
            if self._is_sqlite_engine:
                # SQLite has a per-statement bind-parameter limit (commonly 999).
                # Each record has ~15 columns, so chunk upserts to stay within bounds.
                _SQLITE_CHUNK = 50
                # `_run_write_transaction()` opens SQLite writes with
                # `BEGIN IMMEDIATE`, so existence checks and upsert execute
                # within one stable write window.
                existing_dates = set()
                _COUNT_CHUNK = 500
                for j in range(0, len(batch_dates), _COUNT_CHUNK):
                    chunk_dates = batch_dates[j : j + _COUNT_CHUNK]
                    if not chunk_dates:
                        continue
                    existing_dates.update(
                        session.execute(
                            select(StockDaily.date).where(
                                and_(
                                    StockDaily.code == code,
                                    StockDaily.date.in_(chunk_dates),
                                )
                            )
                        ).scalars().all()
                    )
                new_records = [
                    record for record in records if record['date'] not in existing_dates
                ]
                for i in range(0, len(records), _SQLITE_CHUNK):
                    chunk = records[i : i + _SQLITE_CHUNK]
                    stmt = sqlite_insert(StockDaily).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=['code', 'date'],
                            set_={
                                'open': excluded.open,
                                'high': excluded.high,
                                'low': excluded.low,
                                'close': excluded.close,
                                'volume': excluded.volume,
                                'amount': excluded.amount,
                                'pct_chg': excluded.pct_chg,
                                'ma5': excluded.ma5,
                                'ma10': excluded.ma10,
                                'ma20': excluded.ma20,
                                'volume_ratio': excluded.volume_ratio,
                                'data_source': excluded.data_source,
                                'updated_at': excluded.updated_at,
                            },
                        )
                    )
                return len(new_records)
            else:
                existing_rows = {
                    row.date: row
                    for row in session.execute(
                        select(StockDaily).where(
                            and_(
                                StockDaily.code == code,
                                StockDaily.date.in_(batch_dates),
                            )
                        )
                    ).scalars().all()
                }
                new_count = 0
                for record in records:
                    existing = existing_rows.get(record['date'])
                    if existing is None:
                        session.add(StockDaily(**record))
                        new_count += 1
                        continue
                    existing.open = record['open']
                    existing.high = record['high']
                    existing.low = record['low']
                    existing.close = record['close']
                    existing.volume = record['volume']
                    existing.amount = record['amount']
                    existing.pct_chg = record['pct_chg']
                    existing.ma5 = record['ma5']
                    existing.ma10 = record['ma10']
                    existing.ma20 = record['ma20']
                    existing.volume_ratio = record['volume_ratio']
                    existing.data_source = record['data_source']
                    existing.updated_at = record['updated_at']
                return new_count

        try:
            saved_count = self._run_write_transaction(
                f"save_daily_data[{code}]",
                _write,
            )
            logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条")
            return saved_count
        except Exception as e:
            logger.error(f"保存 {code} 数据失败: {e}")
            raise
    
    def get_analysis_context(
        self, 
        code: str,
        target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取分析所需的上下文数据
        
        返回今日数据 + 昨日数据的对比信息
        
        Args:
            code: 股票代码
            target_date: 目标日期（默认今天）
            
        Returns:
            包含今日数据、昨日对比等信息的字典
        """
        if target_date is None:
            target_date = date.today()
        # Treat target_date as a strict knowledge boundary. Historical replay
        # must not consume bars written after that date.
        with self.get_session() as session:
            recent_data = list(
                session.execute(
                    select(StockDaily)
                    .where(
                        StockDaily.code == code,
                        StockDaily.date <= target_date,
                    )
                    .order_by(StockDaily.date.desc())
                    .limit(2)
                ).scalars().all()
            )
        
        if not recent_data:
            logger.warning(f"未找到 {code} 的数据")
            return None
        
        today_data = recent_data[0]
        yesterday_data = recent_data[1] if len(recent_data) > 1 else None
        
        context = {
            'code': code,
            'date': today_data.date.isoformat(),
            'today': today_data.to_dict(),
        }
        
        if yesterday_data:
            context['yesterday'] = yesterday_data.to_dict()
            
            # 计算相比昨日的变化
            if yesterday_data.volume and yesterday_data.volume > 0:
                context['volume_change_ratio'] = round(
                    today_data.volume / yesterday_data.volume, 2
                )
            
            if yesterday_data.close and yesterday_data.close > 0:
                context['price_change_ratio'] = round(
                    (today_data.close - yesterday_data.close) / yesterday_data.close * 100, 2
                )
            
            # 均线形态判断
            context['ma_status'] = self._analyze_ma_status(today_data)
        
        return context
    
    def _analyze_ma_status(self, data: StockDaily) -> str:
        """
        分析均线形态
        
        判断条件：
        - 多头排列：close > ma5 > ma10 > ma20
        - 空头排列：close < ma5 < ma10 < ma20
        - 震荡整理：其他情况
        """
        # 注意：这里的均线形态判断基于“close/ma5/ma10/ma20”静态比较，
        # 未考虑均线拐点、斜率、或不同数据源复权口径差异。
        # 该行为目前保留（按需求不改逻辑）。
        close = data.close or 0
        ma5 = data.ma5 or 0
        ma10 = data.ma10 or 0
        ma20 = data.ma20 or 0
        
        if close > ma5 > ma10 > ma20 > 0:
            return "多头排列 📈"
        elif close < ma5 < ma10 < ma20 and ma20 > 0:
            return "空头排列 📉"
        elif close > ma5 and ma5 > ma10:
            return "短期向好 🔼"
        elif close < ma5 and ma5 < ma10:
            return "短期走弱 🔽"
        else:
            return "震荡整理 ↔️"

    @staticmethod
    def _parse_published_date(value: Optional[str]) -> Optional[datetime]:
        """
        解析发布时间字符串（失败返回 None）
        """
        if not value:
            return None

        if isinstance(value, datetime):
            return value

        text = str(value).strip()
        if not text:
            return None

        # 优先尝试 ISO 格式
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue

        return None

    @staticmethod
    def _safe_json_dumps(data: Any) -> str:
        """
        安全序列化为 JSON 字符串
        """
        try:
            return json.dumps(data, ensure_ascii=False, default=str)
        except Exception:
            return json.dumps(str(data), ensure_ascii=False)

    @staticmethod
    def _build_raw_result(result: Any) -> Dict[str, Any]:
        """
        生成完整分析结果字典
        """
        data = result.to_dict() if hasattr(result, "to_dict") else {}
        data.update({
            'data_sources': getattr(result, 'data_sources', ''),
            'raw_response': getattr(result, 'raw_response', None),
        })
        return data

    @staticmethod
    def _snapshot_frozen_analysis_history(row: AnalysisHistory) -> Dict[str, Any]:
        """Detach the first committed durable report for retry convergence."""

        try:
            raw_result = json.loads(row.raw_result or "")
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "existing durable analysis history contains invalid raw_result"
            ) from exc
        if not isinstance(raw_result, dict):
            raise RuntimeError(
                "existing durable analysis history raw_result must be an object"
            )

        context_snapshot: Optional[Dict[str, Any]] = None
        if row.context_snapshot:
            try:
                parsed_context = json.loads(row.context_snapshot)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "existing durable analysis history contains invalid context_snapshot"
                ) from exc
            if not isinstance(parsed_context, dict):
                raise RuntimeError(
                    "existing durable analysis history context_snapshot must be an object"
                )
            context_snapshot = parsed_context

        return {
            "raw_result": raw_result,
            "context_snapshot": context_snapshot,
            "columns": {
                "code": row.code,
                "name": row.name,
                "sentiment_score": row.sentiment_score,
                "operation_advice": row.operation_advice,
                "trend_prediction": row.trend_prediction,
                "analysis_summary": row.analysis_summary,
            },
        }

    @staticmethod
    def _restore_analysis_result_from_frozen_history(
        result: Any,
        frozen_history: Dict[str, Any],
    ) -> None:
        """Make a recovered attempt return the first persisted report.

        A provider retry may produce different prose or a different action.
        Once the first attempt committed a report, that immutable report is the
        authority for later signal extraction and the terminal job response.
        """

        raw_result = frozen_history["raw_result"]
        for key, value in raw_result.items():
            if isinstance(key, str) and hasattr(result, key):
                setattr(result, key, value)
        for key, value in frozen_history["columns"].items():
            if hasattr(result, key):
                setattr(result, key, value)
        context_snapshot = frozen_history.get("context_snapshot")
        if context_snapshot is not None:
            setattr(result, "diagnostic_context_snapshot", context_snapshot)
        setattr(result, "_durable_history_reused", True)

    @staticmethod
    def _parse_sniper_value(value: Any) -> Optional[float]:
        return parse_sniper_value(value)

    def _extract_sniper_points(self, result: Any) -> Dict[str, Optional[float]]:
        """Extract normalized sniper point values from an AnalysisResult."""

        return extract_sniper_points(result)

    @staticmethod
    def _build_fallback_url_key(
        code: str,
        title: str,
        source: str,
        published_date: Optional[datetime]
    ) -> str:
        """
        生成无 URL 时的去重键（确保稳定且较短）
        """
        date_str = published_date.isoformat() if published_date else ""
        raw_key = f"{code}|{title}|{source}|{date_str}"
        digest = hashlib.md5(raw_key.encode("utf-8")).hexdigest()
        return f"no-url:{code}:{digest}"

    def save_conversation_message(self, session_id: str, role: str, content: str) -> int:
        """
        保存 Agent 对话消息
        """
        with self.session_scope() as session:
            msg = ConversationMessage(
                session_id=session_id,
                role=role,
                content=content
            )
            session.add(msg)
            session.flush()
            return int(msg.id)

    def save_conversation_user_turn(
        self,
        session_id: str,
        content: str,
        selected_skill_ids: Optional[List[str]] = None,
    ) -> int:
        """Persist a user message and an optional session Skill selection atomically."""
        with self.session_scope() as session:
            msg = ConversationMessage(
                session_id=session_id,
                role="user",
                content=content,
            )
            session.add(msg)
            session.flush()

            if selected_skill_ids is not None:
                now = datetime.now()
                values = {
                    "session_id": session_id,
                    "selected_skill_ids_json": json.dumps(selected_skill_ids, ensure_ascii=False),
                    "created_at": now,
                    "updated_at": now,
                }
                stmt = sqlite_insert(ConversationSessionState).values(**values)
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["session_id"],
                        set_={
                            "selected_skill_ids_json": values["selected_skill_ids_json"],
                            "updated_at": now,
                        },
                    )
                )

            return int(msg.id)

    def get_conversation_session_selected_skill_ids(
        self,
        session_id: str,
    ) -> Optional[List[str]]:
        """Return the saved Skill selection, or None when the session has no state row."""
        with self.session_scope() as session:
            state = session.get(ConversationSessionState, session_id)
            if state is None:
                return None
            return json.loads(state.selected_skill_ids_json)

    def get_conversation_history(self, session_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        获取 Agent 对话历史
        """
        with self.session_scope() as session:
            stmt = select(ConversationMessage).filter(
                ConversationMessage.session_id == session_id
            ).order_by(ConversationMessage.created_at.desc()).limit(limit)
            messages = session.execute(stmt).scalars().all()

            # 倒序返回，保证时间顺序
            return [{"role": msg.role, "content": msg.content} for msg in reversed(messages)]

    def get_visible_conversation_messages(self, session_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return visible user/assistant conversation messages in chronological order."""
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage)
                .where(
                    and_(
                        ConversationMessage.session_id == session_id,
                        ConversationMessage.role.in_(["user", "assistant"]),
                    )
                )
                .order_by(ConversationMessage.created_at, ConversationMessage.id)
            )
            if limit is not None:
                stmt = (
                    stmt.order_by(None)
                    .order_by(ConversationMessage.created_at.desc(), ConversationMessage.id.desc())
                    .limit(limit)
                )
            messages = session.execute(stmt).scalars().all()
            if limit is not None:
                messages = list(reversed(messages))
            return [
                {
                    "id": msg.id,
                    "role": msg.role,
                    "content": msg.content,
                    "created_at": msg.created_at,
                }
                for msg in messages
                if msg.content
            ]

    def get_conversation_summary(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Return the rolling summary for a conversation session, if present."""
        with self.session_scope() as session:
            stmt = select(ConversationSummary).where(
                ConversationSummary.session_id == session_id
            )
            row = session.execute(stmt).scalar_one_or_none()
            if row is None:
                return None
            return {
                "id": row.id,
                "session_id": row.session_id,
                "summary": row.summary,
                "covered_message_id": row.covered_message_id,
                "source_message_count": row.source_message_count,
                "estimated_tokens": row.estimated_tokens,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }

    def save_agent_provider_turn(
        self,
        *,
        session_id: str,
        run_id: str,
        provider: str,
        model: str,
        anchor_user_message_id: int,
        anchor_assistant_message_id: int,
        messages: List[Dict[str, Any]],
        contains_reasoning: bool,
        contains_tool_calls: bool,
        contains_thinking_blocks: bool,
        must_roundtrip: bool,
        estimated_tokens: int,
    ) -> int:
        """Persist one provider protocol trace and enforce per-model retention."""
        with self.session_scope() as session:
            row = AgentProviderTurn(
                session_id=session_id,
                run_id=run_id,
                provider=provider,
                model=model,
                anchor_user_message_id=int(anchor_user_message_id or 0),
                anchor_assistant_message_id=int(anchor_assistant_message_id or 0),
                messages_json=json.dumps(messages or [], ensure_ascii=False, default=str),
                contains_reasoning=bool(contains_reasoning),
                contains_tool_calls=bool(contains_tool_calls),
                contains_thinking_blocks=bool(contains_thinking_blocks),
                must_roundtrip=bool(must_roundtrip),
                estimated_tokens=int(estimated_tokens or 0),
            )
            session.add(row)
            session.flush()
            row_id = int(row.id)
            if row.must_roundtrip:
                self._trim_agent_provider_turns(
                    session=session,
                    session_id=session_id,
                    provider=provider,
                    model=model,
                    keep=PROVIDER_TRACE_RETENTION_LIMIT,
                )
            return row_id

    def get_agent_provider_turns(
        self,
        session_id: str,
        *,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        must_roundtrip_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return provider trace turns in chronological order."""
        with self.session_scope() as session:
            conditions = [AgentProviderTurn.session_id == session_id]
            if provider:
                conditions.append(AgentProviderTurn.provider == provider)
            if model:
                conditions.append(AgentProviderTurn.model == model)
            if must_roundtrip_only:
                conditions.append(AgentProviderTurn.must_roundtrip.is_(True))
            stmt = (
                select(AgentProviderTurn)
                .where(and_(*conditions))
                .order_by(AgentProviderTurn.created_at, AgentProviderTurn.id)
            )
            rows = session.execute(stmt).scalars().all()
            result = []
            for row in rows:
                try:
                    messages = json.loads(row.messages_json or "[]")
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "Invalid provider trace messages_json skipped for session %s turn %s: %s",
                        row.session_id,
                        row.id,
                        exc,
                    )
                    messages = []
                result.append({
                    "id": row.id,
                    "session_id": row.session_id,
                    "run_id": row.run_id,
                    "provider": row.provider,
                    "model": row.model,
                    "anchor_user_message_id": row.anchor_user_message_id,
                    "anchor_assistant_message_id": row.anchor_assistant_message_id,
                    "messages": messages if isinstance(messages, list) else [],
                    "messages_json": row.messages_json,
                    "contains_reasoning": row.contains_reasoning,
                    "contains_tool_calls": row.contains_tool_calls,
                    "contains_thinking_blocks": row.contains_thinking_blocks,
                    "must_roundtrip": row.must_roundtrip,
                    "estimated_tokens": row.estimated_tokens,
                    "created_at": row.created_at,
                })
            return result

    def _trim_agent_provider_turns(
        self,
        *,
        session: Session,
        session_id: str,
        provider: str,
        model: str,
        keep: int,
    ) -> int:
        old_ids_stmt = (
            select(AgentProviderTurn.id)
            .where(
                and_(
                    AgentProviderTurn.session_id == session_id,
                    AgentProviderTurn.provider == provider,
                    AgentProviderTurn.model == model,
                    AgentProviderTurn.must_roundtrip.is_(True),
                )
            )
            .order_by(AgentProviderTurn.created_at.desc(), AgentProviderTurn.id.desc())
            .offset(max(0, int(keep)))
        )
        old_ids = list(session.execute(old_ids_stmt).scalars().all())
        if not old_ids:
            return 0
        result = session.execute(
            delete(AgentProviderTurn).where(AgentProviderTurn.id.in_(old_ids))
        )
        return int(result.rowcount or 0)

    def upsert_conversation_summary(
        self,
        session_id: str,
        summary: str,
        covered_message_id: int,
        source_message_count: int,
        estimated_tokens: int,
    ) -> None:
        """Create or update the rolling summary for a conversation session."""
        with self.session_scope() as session:
            now = datetime.now()
            values = {
                "session_id": session_id,
                "summary": summary,
                "covered_message_id": int(covered_message_id or 0),
                "source_message_count": int(source_message_count or 0),
                "estimated_tokens": int(estimated_tokens or 0),
                "updated_at": now,
            }
            stmt = sqlite_insert(ConversationSummary).values(**values)
            session.execute(
                stmt.on_conflict_do_update(
                    index_elements=["session_id"],
                    set_=values,
                )
            )

    def conversation_session_exists(self, session_id: str) -> bool:
        """Return True when at least one message exists for the given session."""
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage.id)
                .where(ConversationMessage.session_id == session_id)
                .limit(1)
            )
            return session.execute(stmt).scalar() is not None

    def get_chat_sessions(
        self,
        limit: int = 50,
        session_prefix: Optional[str] = None,
        extra_session_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取聊天会话列表（从 conversation_messages 聚合）

        Args:
            limit: Maximum number of sessions to return.
            session_prefix: If provided, only return sessions whose session_id
                starts with this prefix.  Used for per-user isolation (e.g.
                ``"telegram_12345"``).
            extra_session_ids: Optional exact session ids to include in
                addition to the scoped prefix.

        Returns:
            按最近活跃时间倒序的会话列表，每条包含 session_id, title, message_count, last_active
        """
        from sqlalchemy import func

        with self.session_scope() as session:
            normalized_prefix = None
            if session_prefix:
                normalized_prefix = session_prefix if session_prefix.endswith(":") else f"{session_prefix}:"
            exact_ids = [sid for sid in (extra_session_ids or []) if sid]

            # 聚合每个 session 的消息数和最后活跃时间
            base = (
                select(
                    ConversationMessage.session_id,
                    func.count(ConversationMessage.id).label("message_count"),
                    func.min(ConversationMessage.created_at).label("created_at"),
                    func.max(ConversationMessage.created_at).label("last_active"),
                )
            )
            conditions = []
            if normalized_prefix:
                conditions.append(ConversationMessage.session_id.startswith(normalized_prefix))
            if exact_ids:
                conditions.append(ConversationMessage.session_id.in_(exact_ids))
            if conditions:
                base = base.where(or_(*conditions))
            stmt = (
                base
                .group_by(ConversationMessage.session_id)
                .order_by(desc(func.max(ConversationMessage.created_at)))
                .limit(limit)
            )
            rows = session.execute(stmt).all()

            results = []
            for row in rows:
                sid = row.session_id
                # 取该会话第一条 user 消息作为标题
                first_user_msg = session.execute(
                    select(ConversationMessage.content)
                    .where(
                        and_(
                            ConversationMessage.session_id == sid,
                            ConversationMessage.role == "user",
                        )
                    )
                    .order_by(ConversationMessage.created_at)
                    .limit(1)
                ).scalar()
                title = (first_user_msg or "新对话")[:60]

                results.append({
                    "session_id": sid,
                    "title": title,
                    "message_count": row.message_count,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "last_active": row.last_active.isoformat() if row.last_active else None,
                })
            return results

    def get_conversation_messages(self, session_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        获取单个会话的完整消息列表（用于前端恢复历史）
        """
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage)
                .where(ConversationMessage.session_id == session_id)
                .order_by(ConversationMessage.created_at)
                .limit(limit)
            )
            messages = session.execute(stmt).scalars().all()
            return [
                {
                    "id": str(msg.id),
                    "role": msg.role,
                    "content": msg.content,
                    "created_at": msg.created_at.isoformat() if msg.created_at else None,
                }
                for msg in messages
            ]

    def delete_conversation_session(self, session_id: str) -> int:
        """
        删除指定会话的所有消息

        Returns:
            删除的消息数
        """
        with self.session_scope() as session:
            session.execute(
                delete(ConversationSessionState).where(
                    ConversationSessionState.session_id == session_id
                )
            )
            session.execute(
                delete(AgentProviderTurn).where(
                    AgentProviderTurn.session_id == session_id
                )
            )
            session.execute(
                delete(ConversationSummary).where(
                    ConversationSummary.session_id == session_id
                )
            )
            result = session.execute(
                delete(ConversationMessage).where(
                    ConversationMessage.session_id == session_id
                )
            )
            return result.rowcount

    # ------------------------------------------------------------------
    # LLM usage tracking
    # ------------------------------------------------------------------

    def record_llm_usage(
        self,
        call_type: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        stock_code: Optional[str] = None,
        **telemetry: Any,
    ) -> None:
        """Append one LLM call record to llm_usage."""
        row_values: Dict[str, Any] = {
            "call_type": call_type,
            "model": model or "unknown",
            "stock_code": stock_code,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }
        for column in _LLM_USAGE_TELEMETRY_COLUMN_SQL:
            row_values[column] = None if column in _LLM_USAGE_DROPPED_FREE_TEXT_COLUMNS else telemetry.get(column)
        for column in _LLM_USAGE_DURABLE_COLUMN_SQL:
            row_values[column] = telemetry.get(column)
        row = LLMUsage(**row_values)
        with self.session_scope() as session:
            session.add(row)

    def get_llm_usage_summary(
        self,
        from_dt: datetime,
        to_dt: datetime,
    ) -> Dict[str, Any]:
        """Return aggregated token usage between from_dt and to_dt.

        Returns a dict with keys:
          total_calls, total_prompt_tokens, total_completion_tokens, total_tokens,
          by_call_type: list of {call_type, calls, prompt_tokens,
            completion_tokens, total_tokens},
          by_model: list of {model, calls, prompt_tokens, completion_tokens,
            total_tokens, max_total_tokens}
        """
        with self.session_scope() as session:
            base_filter = and_(
                LLMUsage.called_at >= from_dt,
                LLMUsage.called_at <= to_dt,
            )

            # Overall totals
            totals = session.execute(
                select(
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.prompt_tokens), 0).label("prompt_tokens"),
                    func.coalesce(func.sum(LLMUsage.completion_tokens), 0).label("completion_tokens"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                ).where(base_filter)
            ).one()

            # Breakdown by call_type
            by_type_rows = session.execute(
                select(
                    LLMUsage.call_type,
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.prompt_tokens), 0).label("prompt_tokens"),
                    func.coalesce(func.sum(LLMUsage.completion_tokens), 0).label("completion_tokens"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                )
                .where(base_filter)
                .group_by(LLMUsage.call_type)
                .order_by(desc(func.sum(LLMUsage.total_tokens)))
            ).all()

            # Breakdown by model
            by_model_rows = session.execute(
                select(
                    LLMUsage.model,
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.prompt_tokens), 0).label("prompt_tokens"),
                    func.coalesce(func.sum(LLMUsage.completion_tokens), 0).label("completion_tokens"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                    func.coalesce(func.max(LLMUsage.total_tokens), 0).label("max_total_tokens"),
                )
                .where(base_filter)
                .group_by(LLMUsage.model)
                .order_by(desc(func.sum(LLMUsage.total_tokens)))
            ).all()

        return {
            "total_calls": totals.calls,
            "total_prompt_tokens": totals.prompt_tokens,
            "total_completion_tokens": totals.completion_tokens,
            "total_tokens": totals.tokens,
            "by_call_type": [
                {
                    "call_type": r.call_type,
                    "calls": r.calls,
                    "prompt_tokens": r.prompt_tokens,
                    "completion_tokens": r.completion_tokens,
                    "total_tokens": r.tokens,
                }
                for r in by_type_rows
            ],
            "by_model": [
                {
                    "model": r.model,
                    "calls": r.calls,
                    "prompt_tokens": r.prompt_tokens,
                    "completion_tokens": r.completion_tokens,
                    "total_tokens": r.tokens,
                    "max_total_tokens": r.max_total_tokens,
                }
                for r in by_model_rows
            ],
        }

    def get_llm_usage_records(
        self,
        from_dt: datetime,
        to_dt: datetime,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Return recent LLM usage audit rows between from_dt and to_dt.

        Each row contains id, call_type, model, stock_code, prompt_tokens,
        completion_tokens, total_tokens, and called_at. Results are ordered by
        newest call first, and limit is clamped to the public API range.
        """
        normalized_limit = max(1, min(int(limit or 50), 200))
        with self.session_scope() as session:
            rows = session.execute(
                select(
                    LLMUsage.id,
                    LLMUsage.call_type,
                    LLMUsage.model,
                    LLMUsage.stock_code,
                    LLMUsage.prompt_tokens,
                    LLMUsage.completion_tokens,
                    LLMUsage.total_tokens,
                    LLMUsage.called_at,
                )
                .where(
                    and_(
                        LLMUsage.called_at >= from_dt,
                        LLMUsage.called_at <= to_dt,
                    )
                )
                .order_by(desc(LLMUsage.called_at), desc(LLMUsage.id))
                .limit(normalized_limit)
            ).all()

        return [
            {
                "id": r.id,
                "call_type": r.call_type,
                "model": r.model,
                "stock_code": r.stock_code,
                "prompt_tokens": r.prompt_tokens,
                "completion_tokens": r.completion_tokens,
                "total_tokens": r.total_tokens,
                "called_at": r.called_at,
            }
            for r in rows
        ]


_PR1_DURABLE_TABLES = (
    AnalysisJobRecord.__table__,
    JobEventRecord.__table__,
    NotificationOutboxRecord.__table__,
    ProviderHealthRecord.__table__,
)
_PR2_RESEARCH_TABLES = (
    ResearchDatasetSnapshotRecord.__table__,
    ResearchFactorSnapshotRecord.__table__,
    ResearchSnapshotRecord.__table__,
)
_PR3_RESEARCH_EVIDENCE_TABLES = (
    ResearchEvidenceSnapshotRecord.__table__,
)
_PR4_RESEARCH_DEBATE_TABLES = (
    ResearchDebateRequestRecord.__table__,
    ResearchDebateTurnRecord.__table__,
    ResearchDebateSnapshotRecord.__table__,
)
_PERSONAL_RESEARCH_POLICY_TABLES = (
    ResearchWatchlistItemRecord.__table__,
    PortfolioReconciliationRecord.__table__,
    PortfolioReconciliationAdjustmentRecord.__table__,
    PortfolioPolicyEvaluationRecord.__table__,
    ResearchBudgetReservationRecord.__table__,
)
_PERSONAL_RESEARCH_SKILL_TABLES = (
    PersonalResearchSkillContractRecord.__table__,
    PersonalResearchSkillExecutionRecord.__table__,
    PersonalResearchDebateReviewRecord.__table__,
    PersonalResearchThesisRecord.__table__,
)
_DECISION_OUTCOME_V2_TABLES = (
    DecisionOutcomeV2Record.__table__,
)
_PERSONAL_RESEARCH_DECISION_SIGNAL_COLUMN_SQL: Dict[str, str] = {
    'research_stance': 'VARCHAR(24)',
    'account_action': 'VARCHAR(24)',
    'value_quality_score': 'FLOAT',
    'trend_timing_score': 'FLOAT',
    'catalyst_score': 'FLOAT',
    'risk_score': 'FLOAT',
    'evidence_quality_score': 'FLOAT',
    'research_snapshot_hash': 'VARCHAR(64)',
    'policy_version': 'VARCHAR(64)',
    'policy_hash': 'VARCHAR(64)',
    'policy_evaluation_hash': 'VARCHAR(64)',
    'portfolio_snapshot_ref': 'VARCHAR(128)',
    'prompt_version': 'VARCHAR(64)',
    'catalysts_json': 'TEXT',
    'invalidators_json': 'TEXT',
    'unknowns_json': 'TEXT',
    'evidence_refs_json': 'TEXT',
    'policy_mode': 'VARCHAR(16)',
    'policy_decision': 'VARCHAR(16)',
    'would_block': 'BOOLEAN NOT NULL DEFAULT 0',
    'policy_reasons_json': 'TEXT',
}
_PERSONAL_RESEARCH_DECISION_SIGNAL_INDEX_NAMES = {
    'ix_decision_signals_research_stance',
    'ix_decision_signals_account_action',
    'ix_decision_signals_research_snapshot_hash',
    'ix_decision_signals_policy_version',
    'ix_decision_signals_policy_hash',
    'ix_decision_signals_policy_evaluation_hash',
    'ix_decision_signals_portfolio_snapshot_ref',
    'ix_decision_signals_policy_mode',
    'ix_decision_signals_policy_decision',
    'ix_decision_signals_would_block',
}
_PERSONAL_RESEARCH_ACCOUNT_TRIGGER_SQL = {
    'trg_portfolio_reconciliation_header_account_update': """
        CREATE TRIGGER IF NOT EXISTS trg_portfolio_reconciliation_header_account_update
        BEFORE UPDATE OF account_id ON portfolio_reconciliations
        FOR EACH ROW
        WHEN NEW.account_id <> OLD.account_id
        BEGIN
            SELECT RAISE(ABORT, 'reconciliation header account is immutable');
        END
    """,
    'trg_portfolio_reconciliation_applied_update': """
        CREATE TRIGGER IF NOT EXISTS trg_portfolio_reconciliation_applied_update
        BEFORE UPDATE ON portfolio_reconciliations
        FOR EACH ROW
        WHEN OLD.status = 'applied'
        BEGIN
            SELECT RAISE(ABORT, 'applied reconciliation is immutable');
        END
    """,
    'trg_portfolio_reconciliation_applied_delete': """
        CREATE TRIGGER IF NOT EXISTS trg_portfolio_reconciliation_applied_delete
        BEFORE DELETE ON portfolio_reconciliations
        FOR EACH ROW
        WHEN OLD.status = 'applied'
        BEGIN
            SELECT RAISE(ABORT, 'applied reconciliation is immutable');
        END
    """,
    'trg_portfolio_reconciliation_adjustment_account_insert': """
        CREATE TRIGGER IF NOT EXISTS trg_portfolio_reconciliation_adjustment_account_insert
        BEFORE INSERT ON portfolio_reconciliation_adjustments
        FOR EACH ROW
        WHEN NOT EXISTS (
            SELECT 1 FROM portfolio_reconciliations
            WHERE id = NEW.reconciliation_id AND account_id = NEW.account_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'reconciliation adjustment account mismatch');
        END
    """,
    'trg_portfolio_reconciliation_adjustment_closed_insert': """
        CREATE TRIGGER IF NOT EXISTS trg_portfolio_reconciliation_adjustment_closed_insert
        BEFORE INSERT ON portfolio_reconciliation_adjustments
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1 FROM portfolio_reconciliations
            WHERE id = NEW.reconciliation_id AND status = 'applied'
        )
        BEGIN
            SELECT RAISE(ABORT, 'applied reconciliation adjustments are closed');
        END
    """,
    'trg_portfolio_reconciliation_adjustment_account_update': """
        CREATE TRIGGER IF NOT EXISTS trg_portfolio_reconciliation_adjustment_account_update
        BEFORE UPDATE OF reconciliation_id, account_id
        ON portfolio_reconciliation_adjustments
        FOR EACH ROW
        WHEN NOT EXISTS (
            SELECT 1 FROM portfolio_reconciliations
            WHERE id = NEW.reconciliation_id AND account_id = NEW.account_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'reconciliation adjustment account mismatch');
        END
    """,
    'trg_portfolio_reconciliation_adjustment_immutable_update': """
        CREATE TRIGGER IF NOT EXISTS trg_portfolio_reconciliation_adjustment_immutable_update
        BEFORE UPDATE ON portfolio_reconciliation_adjustments
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'reconciliation adjustment is immutable');
        END
    """,
    'trg_portfolio_reconciliation_adjustment_immutable_delete': """
        CREATE TRIGGER IF NOT EXISTS trg_portfolio_reconciliation_adjustment_immutable_delete
        BEFORE DELETE ON portfolio_reconciliation_adjustments
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'reconciliation adjustment is immutable');
        END
    """,
}
_PERSONAL_RESEARCH_POLICY_CONTEXT_COLUMN_SQL = (
    "TEXT NOT NULL DEFAULT '{}' "
    "CHECK (json_valid(portfolio_context_json) "
    "AND json_type(portfolio_context_json) = 'object')"
)
_PERSONAL_RESEARCH_POLICY_CONTEXT_TRIGGER_SQL = {
    'trg_portfolio_policy_evaluation_update': """
        CREATE TRIGGER IF NOT EXISTS trg_portfolio_policy_evaluation_update
        BEFORE UPDATE ON portfolio_policy_evaluations
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'portfolio policy evaluation is immutable');
        END
    """,
    'trg_portfolio_policy_evaluation_delete': """
        CREATE TRIGGER IF NOT EXISTS trg_portfolio_policy_evaluation_delete
        BEFORE DELETE ON portfolio_policy_evaluations
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'portfolio policy evaluation is immutable');
        END
    """,
}
_RESEARCH_BUDGET_TRIGGER_SQL = {
    'trg_research_budget_update_contract': """
        CREATE TRIGGER IF NOT EXISTS trg_research_budget_update_contract
        BEFORE UPDATE ON research_budget_reservations
        FOR EACH ROW
        WHEN OLD.task_id IS NOT NEW.task_id
          OR OLD.budget_date IS NOT NEW.budget_date
          OR OLD.stock_code IS NOT NEW.stock_code
          OR OLD.market IS NOT NEW.market
          OR OLD.mode IS NOT NEW.mode
          OR OLD.bucket IS NOT NEW.bucket
          OR OLD.trigger_source IS NOT NEW.trigger_source
          OR OLD.priority IS NOT NEW.priority
          OR OLD.manual_daily_override IS NOT NEW.manual_daily_override
          OR OLD.created_at IS NOT NEW.created_at
          OR NOT (
              (
                  NEW.status = OLD.status
                  AND NEW.updated_at IS OLD.updated_at
              )
              OR (
                  OLD.status = 'reserved'
                  AND NEW.status IN ('consumed', 'released')
                  AND OLD.updated_at IS NOT NULL
                  AND NEW.updated_at IS NOT NULL
                  AND julianday(NEW.updated_at) > julianday(OLD.updated_at)
              )
          )
        BEGIN
            SELECT RAISE(ABORT, 'research budget reservation is immutable');
        END
    """,
    'trg_research_budget_delete': """
        CREATE TRIGGER IF NOT EXISTS trg_research_budget_delete
        BEFORE DELETE ON research_budget_reservations
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'research budget reservation is immutable');
        END
    """,
}
_PERSONAL_RESEARCH_SKILL_TRIGGER_SQL = {
    'trg_personal_research_skill_contract_update': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_skill_contract_update
        BEFORE UPDATE ON personal_research_skill_contracts
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'personal research skill contract is immutable');
        END
    """,
    'trg_personal_research_skill_contract_delete': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_skill_contract_delete
        BEFORE DELETE ON personal_research_skill_contracts
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'personal research skill contract is immutable');
        END
    """,
    'trg_personal_research_skill_execution_lineage': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_skill_execution_lineage
        BEFORE INSERT ON personal_research_skill_executions
        FOR EACH ROW
        WHEN NOT EXISTS (
            SELECT 1 FROM analysis_jobs WHERE task_id = NEW.task_id
        ) OR NOT EXISTS (
            SELECT 1 FROM personal_research_skill_contracts
            WHERE skill_id = NEW.skill_id
              AND skill_version = NEW.skill_version
              AND contract_hash = NEW.contract_hash
              AND score_field = NEW.score_field
        ) OR NOT EXISTS (
            SELECT 1 FROM research_snapshots
            WHERE snapshot_hash = NEW.research_snapshot_hash
              AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
              AND factor_snapshot_hash = NEW.factor_snapshot_hash
              AND evidence_snapshot_hash = NEW.evidence_snapshot_hash
        ) OR NOT EXISTS (
            SELECT 1 FROM research_factor_snapshots
            WHERE content_hash = NEW.factor_snapshot_hash
              AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
              AND input_dataset_hashes_json = NEW.dataset_snapshot_hashes_json
        ) OR NOT EXISTS (
            SELECT 1 FROM research_evidence_snapshots
            WHERE evidence_hash = NEW.evidence_snapshot_hash
              AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
              AND factor_snapshot_hash = NEW.factor_snapshot_hash
              AND input_dataset_hashes_json = NEW.dataset_snapshot_hashes_json
        )
        BEGIN
            SELECT RAISE(ABORT, 'personal research skill lineage mismatch');
        END
    """,
    'trg_personal_research_skill_execution_update': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_skill_execution_update
        BEFORE UPDATE ON personal_research_skill_executions
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'personal research skill execution is immutable');
        END
    """,
    'trg_personal_research_skill_execution_delete': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_skill_execution_delete
        BEFORE DELETE ON personal_research_skill_executions
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'personal research skill execution is immutable');
        END
    """,
    'trg_personal_research_debate_review_lineage': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_debate_review_lineage
        BEFORE INSERT ON personal_research_debate_reviews
        FOR EACH ROW
        WHEN NOT EXISTS (
            SELECT 1 FROM analysis_jobs WHERE task_id = NEW.task_id
        ) OR NOT EXISTS (
            SELECT 1 FROM research_debate_snapshots
            WHERE debate_hash = NEW.debate_snapshot_hash
              AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
              AND evidence_snapshot_hash = NEW.evidence_snapshot_hash
        )
        BEGIN
            SELECT RAISE(ABORT, 'personal research debate review lineage mismatch');
        END
    """,
    'trg_personal_research_debate_review_update': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_debate_review_update
        BEFORE UPDATE ON personal_research_debate_reviews
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'personal research debate review is immutable');
        END
    """,
    'trg_personal_research_debate_review_delete': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_debate_review_delete
        BEFORE DELETE ON personal_research_debate_reviews
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'personal research debate review is immutable');
        END
    """,
    'trg_personal_research_thesis_skill_lineage': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_thesis_skill_lineage
        BEFORE INSERT ON personal_research_theses
        FOR EACH ROW
        WHEN NOT EXISTS (
            SELECT 1 FROM personal_research_skill_executions
            WHERE execution_hash = NEW.value_quality_execution_hash
              AND task_id = NEW.task_id AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
              AND research_snapshot_hash = NEW.research_snapshot_hash
              AND skill_id = 'personal-value-quality'
              AND result_status = 'succeeded'
        ) OR NOT EXISTS (
            SELECT 1 FROM personal_research_skill_executions
            WHERE execution_hash = NEW.trend_timing_execution_hash
              AND task_id = NEW.task_id AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
              AND research_snapshot_hash = NEW.research_snapshot_hash
              AND skill_id = 'personal-trend-timing'
              AND result_status = 'succeeded'
        ) OR NOT EXISTS (
            SELECT 1 FROM personal_research_skill_executions
            WHERE execution_hash = NEW.catalyst_execution_hash
              AND task_id = NEW.task_id AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
              AND research_snapshot_hash = NEW.research_snapshot_hash
              AND skill_id = 'personal-catalyst'
              AND result_status = 'succeeded'
        ) OR NOT EXISTS (
            SELECT 1 FROM personal_research_skill_executions
            WHERE execution_hash = NEW.risk_execution_hash
              AND task_id = NEW.task_id AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
              AND research_snapshot_hash = NEW.research_snapshot_hash
              AND skill_id = 'personal-risk'
              AND result_status = 'succeeded'
        ) OR NOT EXISTS (
            SELECT 1 FROM personal_research_skill_executions
            WHERE execution_hash = NEW.evidence_quality_execution_hash
              AND task_id = NEW.task_id AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
              AND research_snapshot_hash = NEW.research_snapshot_hash
              AND skill_id = 'personal-evidence-quality'
              AND result_status = 'succeeded'
        )
        BEGIN
            SELECT RAISE(ABORT, 'personal research thesis skill lineage mismatch');
        END
    """,
    'trg_personal_research_thesis_debate_lineage': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_thesis_debate_lineage
        BEFORE INSERT ON personal_research_theses
        FOR EACH ROW
        WHEN NEW.debate_review_hash IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM personal_research_debate_reviews
            WHERE review_hash = NEW.debate_review_hash
              AND debate_snapshot_hash = NEW.debate_snapshot_hash
              AND task_id = NEW.task_id
              AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
              AND verifier_fail_closed = 0
              AND judge_fail_closed = 0
        )
        BEGIN
            SELECT RAISE(ABORT, 'personal research thesis debate lineage mismatch');
        END
    """,
    'trg_personal_research_thesis_policy_lineage': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_thesis_policy_lineage
        BEFORE INSERT ON personal_research_theses
        FOR EACH ROW
        WHEN NEW.decision_signal_id IS NOT NULL AND (
            NOT EXISTS (
                SELECT 1 FROM decision_signals AS signal
                WHERE signal.id = NEW.decision_signal_id
                  AND signal.trace_id = NEW.task_id
                  AND signal.stock_code = NEW.stock_code
                  AND lower(signal.market) = lower(NEW.market)
                  AND signal.research_snapshot_hash = NEW.research_snapshot_hash
                  AND signal.research_stance = NEW.stance
                  AND signal.account_action = NEW.account_action
                  AND signal.value_quality_score = json_extract(
                      NEW.scores_json, '$.value_quality_score'
                  )
                  AND signal.trend_timing_score = json_extract(
                      NEW.scores_json, '$.trend_timing_score'
                  )
                  AND signal.catalyst_score = json_extract(
                      NEW.scores_json, '$.catalyst_score'
                  )
                  AND signal.risk_score = json_extract(
                      NEW.scores_json, '$.risk_score'
                  )
                  AND signal.evidence_quality_score = json_extract(
                      NEW.scores_json, '$.evidence_quality_score'
                  )
            ) OR (
                NEW.policy_evaluation_hash IS NOT NULL AND NOT EXISTS (
                    SELECT 1
                    FROM portfolio_policy_evaluations AS policy
                    JOIN decision_signals AS signal
                      ON signal.id = NEW.decision_signal_id
                    WHERE policy.evaluation_hash = NEW.policy_evaluation_hash
                      AND policy.signal_id = NEW.decision_signal_id
                      AND policy.job_id = NEW.task_id
                      AND policy.stock_code = NEW.stock_code
                      AND lower(policy.market) = lower(NEW.market)
                      AND policy.research_snapshot_hash = NEW.research_snapshot_hash
                      AND policy.research_stance = NEW.stance
                      AND policy.final_account_action = NEW.account_action
                      AND policy.policy_version = NEW.policy_version
                      AND policy.policy_hash = NEW.policy_hash
                      AND policy.portfolio_snapshot_ref = NEW.portfolio_snapshot_ref
                      AND signal.policy_evaluation_hash = NEW.policy_evaluation_hash
                      AND signal.policy_version = NEW.policy_version
                      AND signal.policy_hash = NEW.policy_hash
                      AND signal.portfolio_snapshot_ref = NEW.portfolio_snapshot_ref
                )
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'personal research thesis policy lineage mismatch');
        END
    """,
    'trg_personal_research_thesis_supersedes_lineage': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_thesis_supersedes_lineage
        BEFORE INSERT ON personal_research_theses
        FOR EACH ROW
        WHEN NEW.supersedes_thesis_hash IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM personal_research_theses
            WHERE thesis_hash = NEW.supersedes_thesis_hash
              AND stock_code = NEW.stock_code
              AND lower(market) = lower(NEW.market)
        )
        BEGIN
            SELECT RAISE(ABORT, 'personal research thesis supersedes lineage mismatch');
        END
    """,
    'trg_personal_research_thesis_update': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_thesis_update
        BEFORE UPDATE ON personal_research_theses
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'personal research thesis is immutable');
        END
    """,
    'trg_personal_research_thesis_delete': """
        CREATE TRIGGER IF NOT EXISTS trg_personal_research_thesis_delete
        BEFORE DELETE ON personal_research_theses
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'personal research thesis is immutable');
        END
    """,
}
_DECISION_OUTCOME_V2_LINEAGE_SQL = """
    SELECT 1
    FROM decision_signals AS signal
    JOIN portfolio_policy_evaluations AS policy
      ON policy.signal_id = signal.id
     AND policy.evaluation_hash = signal.policy_evaluation_hash
    WHERE signal.id = NEW.signal_id
      AND signal.stock_code = NEW.stock_code
      AND lower(signal.market) = lower(NEW.market)
      AND signal.source_type = NEW.source_type
      AND signal.action = NEW.signal_action
      AND signal.horizon IS NEW.signal_horizon
      AND signal.status = NEW.signal_status
      AND signal.decision_profile = NEW.decision_profile
      AND signal.research_snapshot_hash = NEW.research_snapshot_hash
      AND signal.policy_version = NEW.policy_version
      AND signal.policy_hash = NEW.policy_hash
      AND signal.policy_evaluation_hash = NEW.policy_evaluation_hash
      AND signal.portfolio_snapshot_ref = NEW.portfolio_snapshot_ref
      AND signal.prompt_version IS NEW.prompt_version
      AND signal.research_stance = NEW.research_stance
      AND signal.account_action = NEW.final_account_action
      AND signal.policy_mode = NEW.policy_mode
      AND signal.policy_decision = NEW.policy_verdict
      AND signal.would_block = NEW.would_block
      AND signal.confidence IS NEW.confidence
      AND signal.score IS NEW.signal_score
      AND signal.value_quality_score = NEW.value_quality_score
      AND signal.trend_timing_score = NEW.trend_timing_score
      AND signal.catalyst_score = NEW.catalyst_score
      AND signal.risk_score = NEW.risk_score
      AND signal.evidence_quality_score = NEW.evidence_quality_score
      AND policy.stock_code = NEW.stock_code
      AND lower(policy.market) = lower(NEW.market)
      AND policy.mode = NEW.policy_mode
      AND policy.policy_version = NEW.policy_version
      AND policy.policy_hash = NEW.policy_hash
      AND policy.research_snapshot_hash = NEW.research_snapshot_hash
      AND policy.portfolio_snapshot_ref = NEW.portfolio_snapshot_ref
      AND policy.research_stance = NEW.research_stance
      AND policy.proposed_account_action = NEW.proposed_account_action
      AND policy.final_account_action = NEW.final_account_action
      AND policy.verdict = NEW.policy_verdict
      AND policy.allowed = NEW.policy_allowed
      AND policy.would_block = NEW.would_block
"""
_DECISION_OUTCOME_V2_DATASET_INVALID_SQL = """
    EXISTS (
        SELECT 1 FROM json_each(NEW.dataset_hashes_json)
        WHERE type <> 'text'
           OR length(value) <> 64
           OR value GLOB '*[^0-9a-f]*'
    )
    OR (
        SELECT count(*) FROM json_each(NEW.dataset_hashes_json)
    ) <> (
        SELECT count(DISTINCT value) FROM json_each(NEW.dataset_hashes_json)
    )
    OR EXISTS (
        SELECT 1
        FROM json_each(NEW.dataset_hashes_json) AS current_item
        JOIN json_each(NEW.dataset_hashes_json) AS next_item
          ON CAST(next_item.key AS INTEGER) = CAST(current_item.key AS INTEGER) + 1
        WHERE current_item.value >= next_item.value
    )
    OR EXISTS (
        SELECT 1
        FROM json_each(NEW.dataset_hashes_json) AS dataset_item
        WHERE NOT EXISTS (
            SELECT 1
            FROM research_dataset_snapshots AS dataset_snapshot
            WHERE dataset_snapshot.content_hash = dataset_item.value
        )
    )
"""
_DECISION_OUTCOME_V2_FROZEN_UPDATE_SQL = " OR ".join(
    f"NEW.{column_name} IS NOT OLD.{column_name}"
    for column_name in (
        'signal_id',
        'outcome_contract',
        'horizon',
        'engine_version',
        'final_action_family',
        'signal_created_at',
        'signal_session',
        'stock_code',
        'market',
        'source_type',
        'signal_action',
        'signal_horizon',
        'signal_status',
        'decision_profile',
        'research_snapshot_hash',
        'policy_version',
        'policy_hash',
        'policy_evaluation_hash',
        'portfolio_snapshot_ref',
        'prompt_version',
        'research_stance',
        'proposed_account_action',
        'final_account_action',
        'policy_mode',
        'policy_verdict',
        'policy_allowed',
        'would_block',
        'confidence',
        'signal_score',
        'value_quality_score',
        'trend_timing_score',
        'catalyst_score',
        'risk_score',
        'evidence_quality_score',
        'created_at',
    )
)
_DECISION_OUTCOME_V2_TRIGGER_SQL = {
    'trg_decision_outcome_v2_lineage_insert': f"""
        CREATE TRIGGER IF NOT EXISTS trg_decision_outcome_v2_lineage_insert
        BEFORE INSERT ON decision_outcomes_v2
        FOR EACH ROW
        WHEN NOT EXISTS ({_DECISION_OUTCOME_V2_LINEAGE_SQL})
        BEGIN
            SELECT RAISE(ABORT, 'decision outcome v2 lineage mismatch');
        END
    """,
    'trg_decision_outcome_v2_dataset_insert': f"""
        CREATE TRIGGER IF NOT EXISTS trg_decision_outcome_v2_dataset_insert
        BEFORE INSERT ON decision_outcomes_v2
        FOR EACH ROW
        WHEN {_DECISION_OUTCOME_V2_DATASET_INVALID_SQL}
        BEGIN
            SELECT RAISE(ABORT, 'decision outcome v2 dataset lineage invalid');
        END
    """,
    'trg_decision_outcome_v2_terminal_update': """
        CREATE TRIGGER IF NOT EXISTS trg_decision_outcome_v2_terminal_update
        BEFORE UPDATE ON decision_outcomes_v2
        FOR EACH ROW
        WHEN OLD.eval_status <> 'pending'
        BEGIN
            SELECT RAISE(ABORT, 'terminal decision outcome v2 is immutable');
        END
    """,
    'trg_decision_outcome_v2_frozen_update': f"""
        CREATE TRIGGER IF NOT EXISTS trg_decision_outcome_v2_frozen_update
        BEFORE UPDATE ON decision_outcomes_v2
        FOR EACH ROW
        WHEN OLD.eval_status = 'pending' AND (
            {_DECISION_OUTCOME_V2_FROZEN_UPDATE_SQL}
        )
        BEGIN
            SELECT RAISE(ABORT, 'decision outcome v2 frozen fields are immutable');
        END
    """,
    'trg_decision_outcome_v2_lineage_update': f"""
        CREATE TRIGGER IF NOT EXISTS trg_decision_outcome_v2_lineage_update
        BEFORE UPDATE ON decision_outcomes_v2
        FOR EACH ROW
        WHEN OLD.eval_status = 'pending'
         AND NOT EXISTS ({_DECISION_OUTCOME_V2_LINEAGE_SQL})
        BEGIN
            SELECT RAISE(ABORT, 'decision outcome v2 lineage mismatch');
        END
    """,
    'trg_decision_outcome_v2_dataset_update': f"""
        CREATE TRIGGER IF NOT EXISTS trg_decision_outcome_v2_dataset_update
        BEFORE UPDATE ON decision_outcomes_v2
        FOR EACH ROW
        WHEN OLD.eval_status = 'pending'
         AND ({_DECISION_OUTCOME_V2_DATASET_INVALID_SQL})
        BEGIN
            SELECT RAISE(ABORT, 'decision outcome v2 dataset lineage invalid');
        END
    """,
    'trg_decision_outcome_v2_delete': """
        CREATE TRIGGER IF NOT EXISTS trg_decision_outcome_v2_delete
        BEFORE DELETE ON decision_outcomes_v2
        FOR EACH ROW
        BEGIN
            SELECT RAISE(ABORT, 'decision outcome v2 is immutable');
        END
    """,
    'trg_decision_outcome_v2_signal_delete_restrict': """
        CREATE TRIGGER IF NOT EXISTS trg_decision_outcome_v2_signal_delete_restrict
        BEFORE DELETE ON decision_signals
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1 FROM decision_outcomes_v2 WHERE signal_id = OLD.id
        )
        BEGIN
            SELECT RAISE(ABORT, 'decision outcome v2 signal is restricted');
        END
    """,
    'trg_decision_outcome_v2_dataset_snapshot_update_restrict': """
        CREATE TRIGGER IF NOT EXISTS trg_decision_outcome_v2_dataset_snapshot_update_restrict
        BEFORE UPDATE OF content_hash ON research_dataset_snapshots
        FOR EACH ROW
        WHEN OLD.content_hash IS NOT NEW.content_hash
         AND EXISTS (
            SELECT 1
            FROM decision_outcomes_v2 AS outcome,
                 json_each(outcome.dataset_hashes_json) AS dataset_item
            WHERE dataset_item.value = OLD.content_hash
         )
        BEGIN
            SELECT RAISE(ABORT, 'decision outcome v2 dataset snapshot is restricted');
        END
    """,
    'trg_decision_outcome_v2_dataset_snapshot_delete_restrict': """
        CREATE TRIGGER IF NOT EXISTS trg_decision_outcome_v2_dataset_snapshot_delete_restrict
        BEFORE DELETE ON research_dataset_snapshots
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1
            FROM decision_outcomes_v2 AS outcome,
                 json_each(outcome.dataset_hashes_json) AS dataset_item
            WHERE dataset_item.value = OLD.content_hash
        )
        BEGIN
            SELECT RAISE(ABORT, 'decision outcome v2 dataset snapshot is restricted');
        END
    """,
}
_PR1_EXTENSION_INDEX_NAMES = {
    'ix_llm_usage_job_stage_called_at',
    'ix_llm_usage_trace_called_at',
    'ix_llm_usage_status_called_at',
    'uix_analysis_history_job_code_report_type',
    'uix_decision_signals_idempotency_key',
}


def _normalize_sql_contract(value: Optional[str]) -> str:
    """Normalize generated SQLite DDL for strict, whitespace-insensitive checks."""

    return ' '.join((value or '').replace('"', '').replace('`', '').split()).lower()


def _verify_pr1_durable_schema_contract(connection) -> None:
    """Fail closed when the durable-job schema is absent or structurally different."""

    existing_tables = {
        row[0]
        for row in connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).all()
    }
    required_tables = {
        table.name for table in _PR1_DURABLE_TABLES
    }.union(_PR1_EXISTING_TABLE_COLUMN_SQL)
    missing_tables = sorted(required_tables.difference(existing_tables))
    if missing_tables:
        raise RuntimeError(
            'PR1 durable schema is incomplete: missing tables='
            + ','.join(missing_tables)
        )

    for table in _PR1_DURABLE_TABLES:
        pragma_rows = connection.exec_driver_sql(
            f"PRAGMA table_info('{table.name}')"
        ).all()
        actual_columns = {row[1]: row for row in pragma_rows}
        expected_columns = {column.name: column for column in table.columns}
        missing_columns = sorted(set(expected_columns).difference(actual_columns))
        unexpected_columns = sorted(set(actual_columns).difference(expected_columns))
        if missing_columns or unexpected_columns:
            raise RuntimeError(
                f'PR1 durable schema table {table.name} has incompatible columns: '
                f'missing={missing_columns}, unexpected={unexpected_columns}'
            )
        for column_name, column in expected_columns.items():
            actual = actual_columns[column_name]
            actual_type = _normalize_sql_contract(actual[2])
            expected_type = _normalize_sql_contract(
                column.type.compile(dialect=connection.dialect)
            )
            if actual_type != expected_type:
                raise RuntimeError(
                    f'PR1 durable schema column {table.name}.{column_name} '
                    f'has type {actual[2]!r}; expected {expected_type!r}'
                )
            if not column.primary_key and bool(actual[3]) != (not column.nullable):
                raise RuntimeError(
                    f'PR1 durable schema column {table.name}.{column_name} '
                    'has incompatible nullability'
                )
            if column.server_default is not None and actual[4] is None:
                raise RuntimeError(
                    f'PR1 durable schema column {table.name}.{column_name} '
                    'is missing its server default'
                )

    for table_name, expected in _PR1_EXISTING_TABLE_COLUMN_SQL.items():
        actual = {
            row[1]: _normalize_sql_contract(row[2])
            for row in connection.exec_driver_sql(
                f"PRAGMA table_info('{table_name}')"
            ).all()
        }
        missing = sorted(set(expected).difference(actual))
        invalid_types = {
            column: {'actual': actual.get(column), 'expected': sql_type.lower()}
            for column, sql_type in expected.items()
            if column in actual
            and actual[column] != _normalize_sql_contract(sql_type)
        }
        if missing or invalid_types:
            raise RuntimeError(
                f'PR1 durable schema table {table_name} is incompatible: '
                f'missing={missing}, invalid_types={invalid_types}'
            )
        if table_name == LLMUsage.__tablename__:
            forbidden = sorted(
                _LLM_USAGE_FORBIDDEN_GENERIC_AUDIT_COLUMNS.intersection(actual)
            )
            if forbidden:
                raise RuntimeError(
                    'PR1 durable schema llm_usage contains ambiguous audit columns: '
                    + ','.join(forbidden)
                )

    expected_indexes = [
        index
        for table in _PR1_DURABLE_TABLES
        for index in table.indexes
    ]
    for table in (
        LLMUsage.__table__,
        AnalysisHistory.__table__,
        DecisionSignalRecord.__table__,
    ):
        expected_indexes.extend(
            index
            for index in table.indexes
            if index.name in _PR1_EXTENSION_INDEX_NAMES
        )

    for index in expected_indexes:
        table_name = index.table.name
        index_rows = {
            row[1]: row
            for row in connection.exec_driver_sql(
                f"PRAGMA index_list('{table_name}')"
            ).all()
        }
        actual = index_rows.get(index.name)
        if actual is None:
            raise RuntimeError(f'PR1 durable schema is missing index {index.name}')
        actual_columns = tuple(
            row[2]
            for row in connection.exec_driver_sql(
                f"PRAGMA index_info('{index.name}')"
            ).all()
        )
        expected_columns = tuple(column.name for column in index.columns)
        if actual_columns != expected_columns or bool(actual[2]) != bool(index.unique):
            raise RuntimeError(
                f'PR1 durable schema index {index.name} is incompatible: '
                f'columns={actual_columns}, unique={bool(actual[2])}'
            )

        expected_where = index.dialect_options['sqlite'].get('where')
        is_partial = bool(actual[4])
        if is_partial != (expected_where is not None):
            raise RuntimeError(
                f'PR1 durable schema index {index.name} has incompatible partial semantics'
            )
        if expected_where is not None:
            actual_sql_row = connection.exec_driver_sql(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                (index.name,),
            ).first()
            actual_sql = actual_sql_row[0] if actual_sql_row else ''
            expected_sql = str(CreateIndex(index).compile(dialect=connection.dialect))
            actual_predicate = _normalize_sql_contract(actual_sql).split(' where ', 1)[-1]
            expected_predicate = _normalize_sql_contract(expected_sql).split(' where ', 1)[-1]
            if actual_predicate != expected_predicate:
                raise RuntimeError(
                    f'PR1 durable schema index {index.name} has incompatible predicate'
                )

    expected_foreign_keys = {
        'job_events': ('analysis_jobs', 'job_id', 'task_id', 'CASCADE'),
        'notification_outbox': ('analysis_jobs', 'job_id', 'task_id', 'SET NULL'),
    }
    for table_name, expected in expected_foreign_keys.items():
        foreign_keys = {
            (row[2], row[3], row[4], str(row[6]).upper())
            for row in connection.exec_driver_sql(
                f"PRAGMA foreign_key_list('{table_name}')"
            ).all()
        }
        if expected not in foreign_keys:
            raise RuntimeError(
                f'PR1 durable schema table {table_name} is missing foreign key {expected}'
            )

    expected_check_constraints = {
        'analysis_jobs': (
            'ck_analysis_jobs_progress_range',
            'ck_analysis_jobs_attempts',
        ),
        'notification_outbox': ('ck_notification_outbox_attempts',),
        'provider_health': ('ck_provider_health_counters',),
    }
    for table_name, constraint_names in expected_check_constraints.items():
        table_sql_row = connection.exec_driver_sql(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).first()
        table_sql = _normalize_sql_contract(table_sql_row[0] if table_sql_row else '')
        missing_constraints = [
            name for name in constraint_names if name.lower() not in table_sql
        ]
        if missing_constraints:
            raise RuntimeError(
                f'PR1 durable schema table {table_name} is missing constraints '
                + ','.join(missing_constraints)
            )


def run_pr1_durable_jobs_schema_upgrade(engine) -> None:
    """Create and verify PR1 durable tables/columns in one SQLite transaction."""

    if engine.url.get_backend_name() != 'sqlite':
        raise RuntimeError('PR1 durable jobs migration only supports SQLite')

    # Python's sqlite3 legacy transaction mode does not start a transaction for
    # DDL.  Explicit BEGIN IMMEDIATE is therefore required; otherwise a failed
    # final contract check can leave partially-created tables without a version
    # marker, defeating crash-safe retry semantics.
    with engine.connect() as connection:
        connection.exec_driver_sql('BEGIN IMMEDIATE')
        try:
            _apply_pr1_durable_jobs_schema(connection)
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()


def _apply_pr1_durable_jobs_schema(connection) -> None:
    """Apply PR1 DDL on a caller-owned explicit SQLite transaction."""

    existing_tables = {
        row[0]
        for row in connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).all()
    }
    missing_prerequisites = sorted(
        set(_PR1_EXISTING_TABLE_COLUMN_SQL).difference(existing_tables)
    )
    if missing_prerequisites:
        raise RuntimeError(
            'PR1 durable jobs migration requires the PR0 schema; missing tables='
            + ','.join(missing_prerequisites)
        )

    for table in _PR1_DURABLE_TABLES:
        table.create(bind=connection, checkfirst=True)

    for table_name, columns in _PR1_EXISTING_TABLE_COLUMN_SQL.items():
        existing_columns = {
            row[1]
            for row in connection.exec_driver_sql(
                f"PRAGMA table_info('{table_name}')"
            ).all()
        }
        for column_name, column_type in columns.items():
            if column_name in existing_columns:
                continue
            connection.exec_driver_sql(
                f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}'
            )
            existing_columns.add(column_name)

    for table in (
        LLMUsage.__table__,
        AnalysisHistory.__table__,
        DecisionSignalRecord.__table__,
    ):
        for index in table.indexes:
            if index.name in _PR1_EXTENSION_INDEX_NAMES:
                index.create(bind=connection, checkfirst=True)

    _verify_pr1_durable_schema_contract(connection)


def _normalize_sql_default(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = _normalize_sql_contract(str(value))
    while normalized.startswith('(') and normalized.endswith(')'):
        normalized = normalized[1:-1].strip()
    if len(normalized) >= 2 and normalized[0] == normalized[-1] == "'":
        normalized = normalized[1:-1]
    return normalized


def _verify_research_schema_tables_contract(
    connection,
    tables,
    *,
    contract_name: str,
    ignored_columns_by_table: Optional[Dict[str, set[str]]] = None,
    ignored_indexes_by_table: Optional[Dict[str, set[str]]] = None,
) -> None:
    """Fail closed when immutable research tables drift from ORM contracts."""

    existing_tables = {
        row[0]
        for row in connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).all()
    }
    missing_tables = sorted(
        {table.name for table in tables}.difference(existing_tables)
    )
    if missing_tables:
        raise RuntimeError(
            f'{contract_name} research schema is incomplete: missing tables='
            + ','.join(missing_tables)
        )

    for table in tables:
        ignored_columns = (ignored_columns_by_table or {}).get(table.name, set())
        ignored_indexes = (ignored_indexes_by_table or {}).get(table.name, set())
        pragma_rows = connection.exec_driver_sql(
            f"PRAGMA table_info('{table.name}')"
        ).all()
        actual_columns = {
            row[1]: row for row in pragma_rows if row[1] not in ignored_columns
        }
        expected_columns = {
            column.name: column
            for column in table.columns
            if column.name not in ignored_columns
        }
        missing_columns = sorted(set(expected_columns).difference(actual_columns))
        unexpected_columns = sorted(set(actual_columns).difference(expected_columns))
        if missing_columns or unexpected_columns:
            raise RuntimeError(
                f'{contract_name} research schema table {table.name} has incompatible columns: '
                f'missing={missing_columns}, unexpected={unexpected_columns}'
            )

        for column_name, column in expected_columns.items():
            actual = actual_columns[column_name]
            actual_type = _normalize_sql_contract(actual[2])
            expected_type = _normalize_sql_contract(
                column.type.compile(dialect=connection.dialect)
            )
            if actual_type != expected_type:
                raise RuntimeError(
                    f'{contract_name} research schema column {table.name}.{column_name} '
                    f'has type {actual[2]!r}; expected {expected_type!r}'
                )
            if not column.primary_key and bool(actual[3]) != (not column.nullable):
                raise RuntimeError(
                    f'{contract_name} research schema column {table.name}.{column_name} '
                    'has incompatible nullability'
                )
            actual_default = _normalize_sql_default(actual[4])
            expected_default = _normalize_sql_default(
                column.server_default.arg if column.server_default is not None else None
            )
            if actual_default != expected_default:
                raise RuntimeError(
                    f'{contract_name} research schema column {table.name}.{column_name} '
                    f'has default {actual_default!r}; expected {expected_default!r}'
                )

        expected_indexes = {
            index.name: index
            for index in table.indexes
            if index.name not in ignored_indexes
        }
        index_rows = {
            row[1]: row
            for row in connection.exec_driver_sql(
                f"PRAGMA index_list('{table.name}')"
            ).all()
            if not str(row[1]).startswith('sqlite_autoindex_')
            and row[1] not in ignored_indexes
        }
        if set(index_rows) != set(expected_indexes):
            raise RuntimeError(
                f'{contract_name} research schema table {table.name} has incompatible indexes: '
                f'actual={sorted(index_rows)}, expected={sorted(expected_indexes)}'
            )
        for index_name, index in expected_indexes.items():
            actual = index_rows[index_name]
            actual_columns_for_index = tuple(
                row[2]
                for row in connection.exec_driver_sql(
                    f"PRAGMA index_info('{index_name}')"
                ).all()
            )
            expected_columns_for_index = tuple(
                column.name for column in index.columns
            )
            expected_where = index.dialect_options['sqlite'].get('where')
            if (
                actual_columns_for_index != expected_columns_for_index
                or bool(actual[2]) != bool(index.unique)
                or bool(actual[4]) != (expected_where is not None)
            ):
                raise RuntimeError(
                    f'{contract_name} research schema index {index_name} is incompatible: '
                    f'columns={actual_columns_for_index}, unique={bool(actual[2])}, '
                    f'partial={bool(actual[4])}'
                )
            if expected_where is not None:
                actual_sql_row = connection.exec_driver_sql(
                    "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                    (index_name,),
                ).first()
                actual_sql = actual_sql_row[0] if actual_sql_row else ''
                expected_sql = str(
                    CreateIndex(index).compile(dialect=connection.dialect)
                )
                actual_predicate = _normalize_sql_contract(actual_sql).split(
                    ' where ', 1
                )[-1]
                expected_predicate = _normalize_sql_contract(expected_sql).split(
                    ' where ', 1
                )[-1]
                if actual_predicate != expected_predicate:
                    raise RuntimeError(
                        f'{contract_name} research schema index {index_name} '
                        'has incompatible predicate'
                    )

        table_sql_row = connection.exec_driver_sql(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table.name,),
        ).first()
        table_sql = _normalize_sql_contract(table_sql_row[0] if table_sql_row else '')
        for constraint in table.constraints:
            if not isinstance(constraint, CheckConstraint):
                continue
            predicate = _normalize_sql_contract(str(constraint.sqltext))
            if (
                not constraint.name
                or constraint.name.lower() not in table_sql
                or predicate not in table_sql
            ):
                raise RuntimeError(
                    f'{contract_name} research schema table {table.name} is missing check '
                    f'{constraint.name}'
                )

        expected_foreign_keys = {
            (
                foreign_key.column.table.name,
                foreign_key.parent.name,
                foreign_key.column.name,
                str(foreign_key.ondelete or '').upper(),
            )
            for foreign_key in table.foreign_keys
        }
        actual_foreign_keys = {
            (row[2], row[3], row[4], str(row[6]).upper())
            for row in connection.exec_driver_sql(
                f"PRAGMA foreign_key_list('{table.name}')"
            ).all()
        }
        if actual_foreign_keys != expected_foreign_keys:
            raise RuntimeError(
                f'{contract_name} research schema table {table.name} has incompatible foreign keys: '
                f'actual={sorted(actual_foreign_keys)}, '
                f'expected={sorted(expected_foreign_keys)}'
            )


def _verify_pr2_research_schema_contract(
    connection,
    *,
    ignore_pr4_extension: bool = False,
) -> None:
    """Fail closed when a PR2 immutable research table drifts."""

    _verify_research_schema_tables_contract(
        connection,
        _PR2_RESEARCH_TABLES,
        contract_name='PR2',
        ignored_columns_by_table=(
            {'research_snapshots': {'debate_snapshot_hash'}}
            if ignore_pr4_extension
            else None
        ),
        ignored_indexes_by_table=(
            {'research_snapshots': {'ix_research_snapshots_debate_hash'}}
            if ignore_pr4_extension
            else None
        ),
    )


def run_pr2_research_schema_upgrade(engine) -> None:
    """Create and verify PR2 immutable research tables in one transaction."""

    if engine.url.get_backend_name() != 'sqlite':
        raise RuntimeError('PR2 research migration only supports SQLite')

    with engine.connect() as connection:
        connection.exec_driver_sql('BEGIN IMMEDIATE')
        try:
            _apply_pr2_research_schema(connection)
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()


def _apply_pr2_research_schema(connection) -> None:
    """Apply PR2 DDL on a caller-owned explicit SQLite transaction."""

    analysis_jobs_exists = connection.exec_driver_sql(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'analysis_jobs'"
    ).first()
    if analysis_jobs_exists is None:
        raise RuntimeError(
            'PR2 research migration requires the PR1 analysis_jobs table'
        )

    for table in _PR2_RESEARCH_TABLES:
        table.create(bind=connection, checkfirst=True)

    _verify_pr2_research_schema_contract(connection)


def _verify_pr3_research_evidence_schema_contract(connection) -> None:
    """Verify the PR3 evidence table and its research snapshot reference."""

    _verify_pr2_research_schema_contract(
        connection,
        ignore_pr4_extension=True,
    )
    _verify_research_schema_tables_contract(
        connection,
        _PR3_RESEARCH_EVIDENCE_TABLES,
        contract_name='PR3',
    )


def run_pr3_research_evidence_schema_upgrade(engine) -> None:
    """Create and verify PR3 immutable research evidence storage atomically."""

    if engine.url.get_backend_name() != 'sqlite':
        raise RuntimeError('PR3 research evidence migration only supports SQLite')

    with engine.connect() as connection:
        connection.exec_driver_sql('BEGIN IMMEDIATE')
        try:
            _apply_pr3_research_evidence_schema(connection)
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()


def _apply_pr3_research_evidence_schema(connection) -> None:
    """Apply PR3 DDL on a caller-owned explicit SQLite transaction."""

    research_snapshots_exists = connection.exec_driver_sql(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'research_snapshots'"
    ).first()
    if research_snapshots_exists is None:
        raise RuntimeError(
            'PR3 research evidence migration requires the PR2 research_snapshots table'
        )

    research_snapshot_columns = {
        row[1]
        for row in connection.exec_driver_sql(
            "PRAGMA table_info('research_snapshots')"
        ).all()
    }
    if 'evidence_snapshot_hash' not in research_snapshot_columns:
        connection.exec_driver_sql(
            'ALTER TABLE research_snapshots '
            'ADD COLUMN evidence_snapshot_hash VARCHAR(64)'
        )

    evidence_index = next(
        index
        for index in ResearchSnapshotRecord.__table__.indexes
        if index.name == 'ix_research_snapshots_evidence_hash'
    )
    evidence_index.create(bind=connection, checkfirst=True)
    for table in _PR3_RESEARCH_EVIDENCE_TABLES:
        table.create(bind=connection, checkfirst=True)

    _verify_pr3_research_evidence_schema_contract(connection)


def _verify_pr4_research_debate_schema_contract(connection) -> None:
    """Verify PR4 request, turn, debate, and final snapshot linkage."""

    _verify_pr2_research_schema_contract(connection)
    _verify_research_schema_tables_contract(
        connection,
        _PR3_RESEARCH_EVIDENCE_TABLES,
        contract_name='PR3',
    )
    _verify_research_schema_tables_contract(
        connection,
        _PR4_RESEARCH_DEBATE_TABLES,
        contract_name='PR4',
    )


def run_pr4_research_debate_schema_upgrade(engine) -> None:
    """Create and verify PR4 immutable bounded-debate storage atomically."""

    if engine.url.get_backend_name() != 'sqlite':
        raise RuntimeError('PR4 research debate migration only supports SQLite')

    with engine.connect() as connection:
        connection.exec_driver_sql('BEGIN IMMEDIATE')
        try:
            _apply_pr4_research_debate_schema(connection)
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()


def _apply_pr4_research_debate_schema(connection) -> None:
    """Apply PR4 DDL on a caller-owned explicit SQLite transaction."""

    required_tables = {'analysis_jobs', 'research_evidence_snapshots', 'research_snapshots'}
    existing_tables = {
        row[0]
        for row in connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).all()
    }
    missing_tables = sorted(required_tables.difference(existing_tables))
    if missing_tables:
        raise RuntimeError(
            'PR4 research debate migration requires PR3 tables; missing='
            + ','.join(missing_tables)
        )

    research_snapshot_columns = {
        row[1]
        for row in connection.exec_driver_sql(
            "PRAGMA table_info('research_snapshots')"
        ).all()
    }
    if 'debate_snapshot_hash' not in research_snapshot_columns:
        connection.exec_driver_sql(
            'ALTER TABLE research_snapshots '
            'ADD COLUMN debate_snapshot_hash VARCHAR(64)'
        )

    debate_index = next(
        index
        for index in ResearchSnapshotRecord.__table__.indexes
        if index.name == 'ix_research_snapshots_debate_hash'
    )
    debate_index.create(bind=connection, checkfirst=True)
    for table in _PR4_RESEARCH_DEBATE_TABLES:
        table.create(bind=connection, checkfirst=True)

    _verify_pr4_research_debate_schema_contract(connection)


def _verify_personal_research_policy_schema_contract(connection) -> None:
    """Verify original-plan PR3 watchlist, reconciliation, policy, and budget storage."""

    _verify_pr4_research_debate_schema_contract(connection)
    _verify_research_schema_tables_contract(
        connection,
        _PERSONAL_RESEARCH_POLICY_TABLES,
        contract_name='PersonalResearchPolicy',
    )

    actual_triggers = {
        row[0]: row[1]
        for row in connection.exec_driver_sql(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
        ).all()
    }
    for trigger_name, expected_sql in _PERSONAL_RESEARCH_ACCOUNT_TRIGGER_SQL.items():
        actual_sql = actual_triggers.get(trigger_name)
        if actual_sql is None:
            raise RuntimeError(
                f'PersonalResearchPolicy trigger is missing: {trigger_name}'
            )
        normalized_actual = _normalize_sql_contract(actual_sql).replace(
            'create trigger if not exists',
            'create trigger',
            1,
        )
        normalized_expected = _normalize_sql_contract(expected_sql).replace(
            'create trigger if not exists',
            'create trigger',
            1,
        )
        if normalized_actual != normalized_expected:
            raise RuntimeError(
                f'PersonalResearchPolicy trigger is incompatible: {trigger_name}'
            )

    actual_columns = {
        row[1]: row
        for row in connection.exec_driver_sql(
            "PRAGMA table_info('decision_signals')"
        ).all()
    }
    missing_columns = sorted(
        set(_PERSONAL_RESEARCH_DECISION_SIGNAL_COLUMN_SQL).difference(actual_columns)
    )
    if missing_columns:
        raise RuntimeError(
            'Personal research DecisionSignal schema is incomplete: missing='
            + ','.join(missing_columns)
        )
    for column_name, ddl in _PERSONAL_RESEARCH_DECISION_SIGNAL_COLUMN_SQL.items():
        actual = actual_columns[column_name]
        expected_type = _normalize_sql_contract(ddl.split(' NOT NULL', 1)[0])
        actual_type = _normalize_sql_contract(actual[2])
        if actual_type != expected_type:
            raise RuntimeError(
                'Personal research DecisionSignal column '
                f'{column_name} has type {actual[2]!r}; expected {expected_type!r}'
            )
        expected_not_null = ' NOT NULL' in ddl
        if bool(actual[3]) != expected_not_null:
            raise RuntimeError(
                'Personal research DecisionSignal column '
                f'{column_name} has incompatible nullability'
            )
        expected_default = '0' if ' DEFAULT 0' in ddl else None
        if _normalize_sql_default(actual[4]) != expected_default:
            raise RuntimeError(
                'Personal research DecisionSignal column '
                f'{column_name} has incompatible default {actual[4]!r}'
            )

    actual_indexes = {
        row[1]: row
        for row in connection.exec_driver_sql(
            "PRAGMA index_list('decision_signals')"
        ).all()
    }
    missing_indexes = sorted(
        _PERSONAL_RESEARCH_DECISION_SIGNAL_INDEX_NAMES.difference(actual_indexes)
    )
    if missing_indexes:
        raise RuntimeError(
            'Personal research DecisionSignal indexes are incomplete: missing='
            + ','.join(missing_indexes)
        )
    expected_index_columns = {
        index.name: tuple(column.name for column in index.columns)
        for index in DecisionSignalRecord.__table__.indexes
        if index.name in _PERSONAL_RESEARCH_DECISION_SIGNAL_INDEX_NAMES
    }
    for index_name, expected_columns in expected_index_columns.items():
        actual = actual_indexes[index_name]
        actual_columns_for_index = tuple(
            row[2]
            for row in connection.exec_driver_sql(
                f"PRAGMA index_info('{index_name}')"
            ).all()
        )
        if (
            actual_columns_for_index != expected_columns
            or bool(actual[2])
            or bool(actual[4])
        ):
            raise RuntimeError(
                'Personal research DecisionSignal index '
                f'{index_name} is incompatible: columns={actual_columns_for_index}, '
                f'unique={bool(actual[2])}, partial={bool(actual[4])}'
            )


def run_personal_research_policy_schema_upgrade(engine) -> None:
    """Create and verify the original-plan PR3 storage contract atomically."""

    if engine.url.get_backend_name() != 'sqlite':
        raise RuntimeError('Personal research policy migration only supports SQLite')

    with engine.connect() as connection:
        connection.exec_driver_sql('BEGIN IMMEDIATE')
        try:
            _apply_personal_research_policy_schema(connection)
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()


def _apply_personal_research_policy_schema(connection) -> None:
    """Apply original-plan PR3 DDL on a caller-owned SQLite transaction."""

    required_tables = {
        'analysis_jobs',
        'decision_signals',
        'portfolio_accounts',
        'research_debate_snapshots',
    }
    existing_tables = {
        row[0]
        for row in connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).all()
    }
    missing_tables = sorted(required_tables.difference(existing_tables))
    if missing_tables:
        raise RuntimeError(
            'Personal research policy migration prerequisites are missing='
            + ','.join(missing_tables)
        )

    for table in _PERSONAL_RESEARCH_POLICY_TABLES:
        table.create(bind=connection, checkfirst=True)

    for trigger_sql in _PERSONAL_RESEARCH_ACCOUNT_TRIGGER_SQL.values():
        connection.exec_driver_sql(trigger_sql)

    existing_signal_columns = {
        row[1]
        for row in connection.exec_driver_sql(
            "PRAGMA table_info('decision_signals')"
        ).all()
    }
    for column_name, ddl in _PERSONAL_RESEARCH_DECISION_SIGNAL_COLUMN_SQL.items():
        if column_name in existing_signal_columns:
            continue
        connection.exec_driver_sql(
            f'ALTER TABLE decision_signals ADD COLUMN {column_name} {ddl}'
        )
        existing_signal_columns.add(column_name)

    for index in DecisionSignalRecord.__table__.indexes:
        if index.name in _PERSONAL_RESEARCH_DECISION_SIGNAL_INDEX_NAMES:
            index.create(bind=connection, checkfirst=True)

    _verify_personal_research_policy_schema_contract(connection)


def _expected_personal_research_skill_contract_rows() -> list[tuple[str, ...]]:
    """Project the code contracts into the exact immutable seed rows."""

    from src.services.research.canonical import canonical_json
    from src.services.research.personal_skill_contract import (
        PERSONAL_RESEARCH_SKILL_CONTRACTS,
    )

    actual_contract_identity = tuple(
        (
            contract.skill_id,
            contract.version,
            contract.score_field,
        )
        for contract in PERSONAL_RESEARCH_SKILL_CONTRACTS.values()
    )
    if actual_contract_identity != _PERSONAL_RESEARCH_SKILL_CONTRACT_ROWS:
        raise RuntimeError(
            'Personal research Skill storage constants drift from the code contracts'
        )
    return [
        (
            contract.skill_id,
            contract.version,
            contract.content_hash,
            contract.score_field,
            canonical_json(contract.content_payload(), exclude_volatile=False),
        )
        for contract in PERSONAL_RESEARCH_SKILL_CONTRACTS.values()
    ]


def _verify_personal_research_skills_schema_contract(connection) -> None:
    """Verify immutable Skill execution, Debate review, and Thesis storage."""

    _verify_personal_research_policy_schema_contract(connection)
    _verify_research_schema_tables_contract(
        connection,
        _PERSONAL_RESEARCH_SKILL_TABLES,
        contract_name='PersonalResearchSkills',
    )

    expected_contract_rows = _expected_personal_research_skill_contract_rows()
    actual_contract_rows = [
        tuple(row)
        for row in connection.exec_driver_sql(
            'SELECT skill_id, skill_version, contract_hash, score_field, canonical_json '
            'FROM personal_research_skill_contracts '
            'ORDER BY rowid'
        ).all()
    ]
    if actual_contract_rows != expected_contract_rows:
        raise RuntimeError(
            'Personal research Skill contract registry is incompatible'
        )

    actual_triggers = {
        row[0]: row[1]
        for row in connection.exec_driver_sql(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
        ).all()
    }
    for trigger_name, expected_sql in _PERSONAL_RESEARCH_SKILL_TRIGGER_SQL.items():
        actual_sql = actual_triggers.get(trigger_name)
        if actual_sql is None:
            raise RuntimeError(
                f'PersonalResearchSkills trigger is missing: {trigger_name}'
            )
        normalized_actual = _normalize_sql_contract(actual_sql).replace(
            'create trigger if not exists',
            'create trigger',
            1,
        )
        normalized_expected = _normalize_sql_contract(expected_sql).replace(
            'create trigger if not exists',
            'create trigger',
            1,
        )
        if normalized_actual != normalized_expected:
            raise RuntimeError(
                f'PersonalResearchSkills trigger is incompatible: {trigger_name}'
            )


def run_personal_research_skills_schema_upgrade(engine) -> None:
    """Create and verify immutable personal-research PR4 storage atomically."""

    if engine.url.get_backend_name() != 'sqlite':
        raise RuntimeError('Personal research Skills migration only supports SQLite')

    with engine.connect() as connection:
        connection.exec_driver_sql('BEGIN IMMEDIATE')
        try:
            _apply_personal_research_skills_schema(connection)
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()


def _apply_personal_research_skills_schema(connection) -> None:
    """Apply personal-research PR4 DDL on a caller-owned transaction."""

    required_tables = {
        'analysis_jobs',
        'decision_signals',
        'portfolio_policy_evaluations',
        'research_dataset_snapshots',
        'research_factor_snapshots',
        'research_evidence_snapshots',
        'research_debate_snapshots',
        'research_snapshots',
    }
    existing_tables = {
        row[0]
        for row in connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).all()
    }
    missing_tables = sorted(required_tables.difference(existing_tables))
    if missing_tables:
        raise RuntimeError(
            'Personal research Skills migration prerequisites are missing='
            + ','.join(missing_tables)
        )

    for table in _PERSONAL_RESEARCH_SKILL_TABLES:
        table.create(bind=connection, checkfirst=True)

    for row in _expected_personal_research_skill_contract_rows():
        connection.exec_driver_sql(
            'INSERT OR IGNORE INTO personal_research_skill_contracts '
            '(skill_id, skill_version, contract_hash, score_field, canonical_json) '
            'VALUES (?, ?, ?, ?, ?)',
            row,
        )

    for trigger_sql in _PERSONAL_RESEARCH_SKILL_TRIGGER_SQL.values():
        connection.exec_driver_sql(trigger_sql)

    _verify_personal_research_skills_schema_contract(connection)


def _verify_decision_outcome_v2_schema_contract(connection) -> None:
    """Verify the independent, immutable Decision Outcome v2 schema."""

    _verify_personal_research_skills_schema_contract(connection)
    _verify_research_schema_tables_contract(
        connection,
        _DECISION_OUTCOME_V2_TABLES,
        contract_name='DecisionOutcomeV2',
    )

    actual_triggers = {
        row[0]: row[1]
        for row in connection.exec_driver_sql(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
        ).all()
    }
    for trigger_name, expected_sql in _DECISION_OUTCOME_V2_TRIGGER_SQL.items():
        actual_sql = actual_triggers.get(trigger_name)
        if actual_sql is None:
            raise RuntimeError(
                f'DecisionOutcomeV2 trigger is missing: {trigger_name}'
            )
        normalized_actual = _normalize_sql_contract(actual_sql).replace(
            'create trigger if not exists',
            'create trigger',
            1,
        )
        normalized_expected = _normalize_sql_contract(expected_sql).replace(
            'create trigger if not exists',
            'create trigger',
            1,
        )
        if normalized_actual != normalized_expected:
            raise RuntimeError(
                f'DecisionOutcomeV2 trigger is incompatible: {trigger_name}'
            )


def run_decision_outcome_v2_schema_upgrade(engine) -> None:
    """Create and verify Decision Outcome v2 storage atomically."""

    if engine.url.get_backend_name() != 'sqlite':
        raise RuntimeError('Decision Outcome v2 migration only supports SQLite')

    with engine.connect() as connection:
        connection.exec_driver_sql('BEGIN IMMEDIATE')
        try:
            _apply_decision_outcome_v2_schema(connection)
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()


def _apply_decision_outcome_v2_schema(connection) -> None:
    """Apply Decision Outcome v2 DDL on a caller-owned transaction."""

    required_tables = {
        'decision_signals',
        'portfolio_policy_evaluations',
        'personal_research_theses',
        'research_dataset_snapshots',
    }
    existing_tables = {
        row[0]
        for row in connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).all()
    }
    missing_tables = sorted(required_tables.difference(existing_tables))
    if missing_tables:
        raise RuntimeError(
            'Decision Outcome v2 migration prerequisites are missing='
            + ','.join(missing_tables)
        )

    for table in _DECISION_OUTCOME_V2_TABLES:
        table.create(bind=connection, checkfirst=True)
    for trigger_sql in _DECISION_OUTCOME_V2_TRIGGER_SQL.values():
        connection.exec_driver_sql(trigger_sql)

    _verify_decision_outcome_v2_schema_contract(connection)


def _verify_personal_research_policy_context_schema_contract(connection) -> None:
    """Verify replayable immutable Portfolio Policy evaluation audits."""

    _verify_decision_outcome_v2_schema_contract(connection)
    columns = {
        row[1]: row
        for row in connection.exec_driver_sql(
            "PRAGMA table_info('portfolio_policy_evaluations')"
        ).all()
    }
    column = columns.get('portfolio_context_json')
    if column is None:
        raise RuntimeError(
            'Portfolio Policy context schema is incomplete: '
            'portfolio_context_json is missing'
        )
    if (
        _normalize_sql_contract(column[2]) != 'text'
        or not bool(column[3])
        or _normalize_sql_default(column[4]) != '{}'
    ):
        raise RuntimeError(
            'Portfolio Policy context column has incompatible type, '
            'nullability, or default'
        )
    invalid_count = connection.exec_driver_sql(
        "SELECT count(*) FROM portfolio_policy_evaluations "
        "WHERE NOT json_valid(portfolio_context_json) "
        "OR json_type(portfolio_context_json) <> 'object'"
    ).scalar_one()
    if int(invalid_count) != 0:
        raise RuntimeError('Portfolio Policy context contains invalid JSON rows')

    actual_triggers = {
        row[0]: row[1]
        for row in connection.exec_driver_sql(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
        ).all()
    }
    for trigger_name, expected_sql in (
        _PERSONAL_RESEARCH_POLICY_CONTEXT_TRIGGER_SQL.items()
    ):
        actual_sql = actual_triggers.get(trigger_name)
        if actual_sql is None:
            raise RuntimeError(
                f'Portfolio Policy context trigger is missing: {trigger_name}'
            )
        normalized_actual = _normalize_sql_contract(actual_sql).replace(
            'create trigger if not exists',
            'create trigger',
            1,
        )
        normalized_expected = _normalize_sql_contract(expected_sql).replace(
            'create trigger if not exists',
            'create trigger',
            1,
        )
        if normalized_actual != normalized_expected:
            raise RuntimeError(
                f'Portfolio Policy context trigger is incompatible: {trigger_name}'
            )
    for trigger_name, expected_sql in _RESEARCH_BUDGET_TRIGGER_SQL.items():
        actual_sql = actual_triggers.get(trigger_name)
        if actual_sql is None:
            raise RuntimeError(
                f'Research budget trigger is missing: {trigger_name}'
            )
        normalized_actual = _normalize_sql_contract(actual_sql).replace(
            'create trigger if not exists',
            'create trigger',
            1,
        )
        normalized_expected = _normalize_sql_contract(expected_sql).replace(
            'create trigger if not exists',
            'create trigger',
            1,
        )
        if normalized_actual != normalized_expected:
            raise RuntimeError(
                f'Research budget trigger is incompatible: {trigger_name}'
            )


def run_personal_research_policy_context_schema_upgrade(engine) -> None:
    """Add and verify replayable immutable Portfolio Policy context."""

    if engine.url.get_backend_name() != 'sqlite':
        raise RuntimeError(
            'Personal research Policy context migration only supports SQLite'
        )
    with engine.connect() as connection:
        connection.exec_driver_sql('BEGIN IMMEDIATE')
        try:
            columns = {
                row[1]
                for row in connection.exec_driver_sql(
                    "PRAGMA table_info('portfolio_policy_evaluations')"
                ).all()
            }
            if 'portfolio_context_json' not in columns:
                connection.exec_driver_sql(
                    'ALTER TABLE portfolio_policy_evaluations ADD COLUMN '
                    'portfolio_context_json '
                    + _PERSONAL_RESEARCH_POLICY_CONTEXT_COLUMN_SQL
                )
            for trigger_sql in (
                _PERSONAL_RESEARCH_POLICY_CONTEXT_TRIGGER_SQL.values()
            ):
                connection.exec_driver_sql(trigger_sql)
            for trigger_sql in _RESEARCH_BUDGET_TRIGGER_SQL.values():
                connection.exec_driver_sql(trigger_sql)
            _verify_personal_research_policy_context_schema_contract(connection)
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()


class _StorageSchemaConvergence(DatabaseManager):
    """Non-singleton adapter for the explicit versioned migrator.

    The adapter deliberately reuses the historical schema repair methods so
    the explicit migrator and legacy automatic startup cannot drift into two
    independent implementations.  Strict mode converts their historical
    best-effort inspection fallbacks into migration failures: a version marker
    is only recorded after every contract is verified.
    """

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls)

    def __init__(self, engine, config) -> None:
        self._engine = engine
        self._db_url = str(engine.url)
        self._is_sqlite_engine = engine.url.get_backend_name() == "sqlite"
        self._sqlite_file_db = self._is_sqlite_engine and self._is_file_sqlite_database()
        self._sqlite_wal_enabled = config.sqlite_wal_enabled
        self._sqlite_busy_timeout_ms = config.sqlite_busy_timeout_ms
        self._sqlite_write_retry_max = config.sqlite_write_retry_max
        self._sqlite_write_retry_base_delay = config.sqlite_write_retry_base_delay
        self._strict_schema_convergence = True
        self._install_sqlite_pragma_handler()

    def run(self) -> None:
        if not self._is_sqlite_engine:
            raise RuntimeError("Storage schema convergence only supports SQLite")

        Base.metadata.create_all(bind=self._engine)
        self._ensure_llm_usage_telemetry_columns()
        self._ensure_decision_signal_profile_schema()
        self._ensure_intelligence_item_scope_values()
        self._ensure_intelligence_items_unique_index()
        self._verify_contracts()

    def _verify_contracts(self) -> None:
        inspector = inspect(self._engine)

        llm_columns = {
            column["name"]
            for column in inspector.get_columns(LLMUsage.__tablename__)
        }
        missing_llm_columns = sorted(
            set(_LLM_USAGE_TELEMETRY_COLUMN_SQL).difference(llm_columns)
        )
        if missing_llm_columns:
            raise RuntimeError(
                "llm_usage telemetry convergence is incomplete: missing="
                + ",".join(missing_llm_columns)
            )

        decision_columns = {
            column["name"]
            for column in inspector.get_columns(DecisionSignalRecord.__tablename__)
        }
        if "decision_profile" not in decision_columns:
            raise RuntimeError(
                "decision_signals convergence is incomplete: decision_profile is missing"
            )

        intelligence_columns = {
            column["name"]
            for column in inspector.get_columns(IntelligenceItem.__tablename__)
        }
        if "scope_value" not in intelligence_columns:
            raise RuntimeError(
                "intelligence_items convergence is incomplete: scope_value is missing"
            )
        with self._engine.connect() as connection:
            empty_scope_count = connection.execute(
                text(
                    "SELECT COUNT(*) FROM intelligence_items "
                    "WHERE scope_value IS NULL OR scope_value = ''"
                )
            ).scalar_one()
        if empty_scope_count:
            raise RuntimeError(
                "intelligence_items convergence is incomplete: "
                f"{empty_scope_count} rows still have an empty scope_value"
            )

        expected_unique_columns = (
            "source_id",
            "url",
            "scope_type",
            "scope_value",
            "market",
        )
        unique_indexes = self._list_sqlite_unique_indexes(
            IntelligenceItem.__tablename__
        )
        if not any(
            tuple(columns) == expected_unique_columns
            for columns in unique_indexes
        ):
            raise RuntimeError(
                "intelligence_items convergence is incomplete: scoped unique index is missing"
            )

        actual_indexes = {
            index["name"]: tuple(index.get("column_names") or ())
            for index in inspector.get_indexes(IntelligenceItem.__tablename__)
            if index.get("name")
        }
        expected_indexes = {
            index.name: tuple(column.name for column in index.columns)
            for index in IntelligenceItem.__table__.indexes
            if index.name
        }
        invalid_indexes = {
            name: {
                "expected": columns,
                "actual": actual_indexes.get(name),
            }
            for name, columns in expected_indexes.items()
            if actual_indexes.get(name) != columns
        }
        if invalid_indexes:
            raise RuntimeError(
                "intelligence_items convergence is incomplete: model indexes "
                f"are missing or invalid: {invalid_indexes}"
            )

        has_source_foreign_key = self._has_intelligence_item_source_foreign_key()
        if not has_source_foreign_key:
            raise RuntimeError(
                "intelligence_items convergence is incomplete: source_id foreign "
                "key with ON DELETE SET NULL is missing"
            )


def run_storage_schema_convergence(engine, config=None) -> None:
    """Converge and verify every schema repair that predates migrations."""

    runtime_config = config or get_config()
    _StorageSchemaConvergence(engine, runtime_config).run()


# 便捷函数
def get_db() -> DatabaseManager:
    """获取数据库管理器实例的快捷方式"""
    return DatabaseManager.get_instance()


def persist_llm_usage(
    usage: Dict[str, Any],
    model: str,
    call_type: str,
    stock_code: Optional[str] = None,
) -> None:
    """Fire-and-forget: write one LLM call record to llm_usage. Never raises."""
    try:
        if usage is None:
            usage = {}
        prompt_cache_telemetry_disabled = bool(
            getattr(usage, _LLM_PROMPT_CACHE_TELEMETRY_DISABLED_ATTR, False)
        )
        prompt_tokens = _coerce_llm_usage_non_negative_int(usage.get("prompt_tokens")) or 0
        completion_tokens = _coerce_llm_usage_non_negative_int(usage.get("completion_tokens")) or 0
        total_tokens = _coerce_llm_usage_non_negative_int(usage.get("total_tokens")) or 0
        telemetry = {
            column: usage.get(column)
            for column in _LLM_USAGE_TELEMETRY_COLUMN_SQL
        }
        durable_telemetry = {
            column: usage.get(column)
            for column in _LLM_USAGE_DURABLE_COLUMN_SQL
        }
        try:
            # Import lazily: storage is imported by the diagnostics module.
            from src.services.run_diagnostics import (
                get_current_diagnostic_context,
                sanitize_diagnostic_text,
            )

            diagnostic_context = get_current_diagnostic_context()
        except Exception:
            diagnostic_context = None
            sanitize_diagnostic_text = None

        if diagnostic_context is not None:
            durable_telemetry["job_id"] = durable_telemetry.get("job_id") or diagnostic_context.task_id
            durable_telemetry["trace_id"] = durable_telemetry.get("trace_id") or diagnostic_context.trace_id
            durable_telemetry["stage"] = durable_telemetry.get("stage") or diagnostic_context.stage
            durable_telemetry["prompt_version"] = (
                durable_telemetry.get("prompt_version") or diagnostic_context.prompt_version
            )
            durable_telemetry["snapshot_hash"] = (
                durable_telemetry.get("snapshot_hash") or diagnostic_context.snapshot_hash
            )
            durable_telemetry["attempt_no"] = (
                durable_telemetry.get("attempt_no") or diagnostic_context.attempt_no
            )

        durable_telemetry["latency_ms"] = _coerce_llm_usage_non_negative_int(
            durable_telemetry.get("latency_ms")
        )
        durable_telemetry["attempt_no"] = _coerce_llm_usage_positive_int(
            durable_telemetry.get("attempt_no")
        )
        durable_telemetry["estimated_cost_usd"] = _coerce_llm_usage_cost(
            durable_telemetry.get("estimated_cost_usd")
        )
        if durable_telemetry["estimated_cost_usd"] is None:
            durable_telemetry["cost_source"] = None
        error_message = durable_telemetry.get("error_message_sanitized")
        if error_message is not None:
            if sanitize_diagnostic_text is not None:
                error_message = sanitize_diagnostic_text(error_message, max_length=300)
            else:
                error_message = str(error_message)[:300]
        durable_telemetry["error_message_sanitized"] = error_message
        if prompt_cache_telemetry_disabled:
            for column in _LLM_PROMPT_CACHE_TELEMETRY_COLUMNS:
                telemetry[column] = None
        for column in _LLM_USAGE_INTEGER_TELEMETRY_COLUMNS:
            telemetry[column] = _coerce_llm_usage_non_negative_int(telemetry.get(column))
        telemetry["normalized_prompt_tokens"] = (
            telemetry.get("normalized_prompt_tokens")
            if telemetry.get("normalized_prompt_tokens") is not None
            else prompt_tokens
        )
        telemetry["normalized_completion_tokens"] = (
            telemetry.get("normalized_completion_tokens")
            if telemetry.get("normalized_completion_tokens") is not None
            else completion_tokens
        )
        telemetry["normalized_total_tokens"] = (
            telemetry.get("normalized_total_tokens")
            if telemetry.get("normalized_total_tokens") is not None
            else total_tokens
        )
        has_usage_payload = bool(usage.get("provider_usage_json")) or any(
            key in usage
            for key in (
                "prompt_tokens",
                "completion_tokens",
                "total_tokens",
                "normalized_prompt_tokens",
                "normalized_completion_tokens",
                "normalized_total_tokens",
            )
        )
        if not prompt_cache_telemetry_disabled:
            telemetry["cache_capability"] = usage.get("cache_capability") or "unknown"
            telemetry["cache_eligibility"] = usage.get("cache_eligibility") or "unknown"
            telemetry["cache_observation"] = usage.get("cache_observation") or (
                "no_usage" if not has_usage_payload else "unknown"
            )
        if not durable_telemetry.get("status"):
            durable_telemetry["status"] = "success"
        telemetry.update(durable_telemetry)
        db = DatabaseManager.get_instance()
        db.record_llm_usage(
            call_type=call_type,
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            stock_code=stock_code,
            **telemetry,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("[LLM usage] failed to persist usage record: %s", exc)


def _coerce_llm_usage_non_negative_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        if value < 0 or not value.is_integer():
            return None
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text or not text.isdigit():
            return None
        return int(text)
    return None


def _coerce_llm_usage_positive_int(value: Any) -> Optional[int]:
    coerced = _coerce_llm_usage_non_negative_int(value)
    return coerced if coerced is not None and coerced > 0 else None


def _coerce_llm_usage_cost(value: Any) -> Optional[float]:
    """Return a finite non-negative USD estimate, otherwise unknown (NULL)."""

    if value is None or isinstance(value, bool):
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(coerced) or coerced < 0:
        return None
    return coerced


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    db = get_db()
    
    print("=== 数据库测试 ===")
    print(f"数据库初始化成功")
    
    # 测试检查今日数据
    has_data = db.has_today_data('600519')
    print(f"茅台今日是否有数据: {has_data}")
    
    # 测试保存数据
    test_df = pd.DataFrame({
        'date': [date.today()],
        'open': [1800.0],
        'high': [1850.0],
        'low': [1780.0],
        'close': [1820.0],
        'volume': [10000000],
        'amount': [18200000000],
        'pct_chg': [1.5],
        'ma5': [1810.0],
        'ma10': [1800.0],
        'ma20': [1790.0],
        'volume_ratio': [1.2],
    })
    
    saved = db.save_daily_data(test_df, '600519', 'TestSource')
    print(f"保存测试数据: {saved} 条")
    
    # 测试获取上下文
    context = db.get_analysis_context('600519')
    print(f"分析上下文: {context}")
