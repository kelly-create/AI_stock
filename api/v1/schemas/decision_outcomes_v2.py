"""API contracts for immutable personal-research Decision Outcome v2."""

from __future__ import annotations

from typing import List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator


DecisionOutcomeV2Horizon = Literal["5d", "10d", "20d"]
DecisionOutcomeV2Status = Literal[
    "pending",
    "evaluated",
    "observational",
    "unexecutable",
    "unable",
]
DecisionOutcomeV2ActionFamily = Literal["long", "defensive", "observational"]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DecisionOutcomeV2RunRequest(_StrictModel):
    """Select immutable signal/horizon identities for asynchronous evaluation."""

    signal_id: Optional[int] = Field(None, gt=0)
    horizons: List[DecisionOutcomeV2Horizon] = Field(
        default_factory=lambda: ["5d", "10d", "20d"],
        min_length=1,
        max_length=3,
    )
    stock_code: Optional[str] = Field(None, min_length=1, max_length=16)
    decision_profile: Optional[str] = Field(None, min_length=1, max_length=16)
    limit: int = Field(100, ge=1, le=500)
    notify: bool = False

    @field_validator("horizons")
    @classmethod
    def _unique_horizons(
        cls,
        value: List[DecisionOutcomeV2Horizon],
    ) -> List[DecisionOutcomeV2Horizon]:
        if len(value) != len(set(value)):
            raise ValueError("horizons must not contain duplicates")
        return value


class DecisionOutcomeV2RunAccepted(_StrictModel):
    accepted: Literal[True] = True
    task_id: str
    status: str
    deduplicated: bool
    engine_version: str
    horizons: List[DecisionOutcomeV2Horizon]


class DecisionOutcomeV2BenchmarkItem(_StrictModel):
    code: Optional[str] = None
    name: Optional[str] = None
    status: Literal["available", "unavailable"]
    reason_code: Optional[str] = None
    return_pct: Optional[float] = None
    stock_excess_return_pct: Optional[float] = None
    directional_excess_return_pct: Optional[float] = None


class DecisionOutcomeV2Item(_StrictModel):
    id: int
    signal_id: int
    outcome_contract: Literal["decision-outcome-v2"]
    horizon: DecisionOutcomeV2Horizon
    engine_version: str
    eval_status: DecisionOutcomeV2Status
    final_action_family: DecisionOutcomeV2ActionFamily
    outcome: Optional[Literal["hit", "miss"]] = None
    direction_correct: Optional[bool] = None
    reason_code: Optional[str] = None
    execution_status: Literal[
        "pending",
        "executable",
        "unexecutable",
        "unavailable",
    ]

    signal_created_at: str
    signal_session: Optional[str] = None
    stock_code: str
    market: str
    source_type: str
    signal_action: str
    signal_horizon: Optional[str] = None
    signal_status: str
    decision_profile: str
    research_snapshot_hash: str
    policy_version: str
    policy_hash: str
    policy_evaluation_hash: str
    portfolio_snapshot_ref: str
    prompt_version: Optional[str] = None
    research_stance: str
    proposed_account_action: str
    final_account_action: str
    policy_mode: str
    policy_verdict: str
    policy_allowed: bool
    would_block: bool
    confidence: Optional[float] = None
    signal_score: Optional[float] = None
    value_quality_score: float
    trend_timing_score: float
    catalyst_score: float
    risk_score: float
    evidence_quality_score: float

    entry_trade_date: Optional[str] = None
    entry_raw_open: Optional[float] = None
    entry_adj_factor: Optional[float] = None
    end_trade_date: Optional[str] = None
    trading_day_count: Optional[int] = None
    end_adjusted_close: Optional[float] = None
    stock_return_pct: Optional[float] = None
    directional_return_pct: Optional[float] = None
    mfe_pct: Optional[float] = None
    mae_pct: Optional[float] = None

    csi300: DecisionOutcomeV2BenchmarkItem
    sw1: DecisionOutcomeV2BenchmarkItem
    dataset_hashes: List[str]
    observation_hash: str
    evaluated_at: Optional[str] = None
    created_at: str
    updated_at: str


class DecisionOutcomeV2ListResponse(_StrictModel):
    contract: Literal["decision-outcome-v2-collection"] = (
        "decision-outcome-v2-collection"
    )
    version: Literal["v1"] = "v1"
    items: List[DecisionOutcomeV2Item] = Field(default_factory=list)
    total: int = Field(ge=0)
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=100)


class DecisionOutcomeV2StatsDimensions(_StrictModel):
    engine: str = Field(min_length=1)
    horizon: DecisionOutcomeV2Horizon
    profile: str = Field(min_length=1)
    final_action_family: DecisionOutcomeV2ActionFamily


class DecisionOutcomeV2CalibrationBin(_StrictModel):
    index: int = Field(ge=0, le=4)
    lower_bound: float = Field(ge=0.0, le=1.0)
    upper_bound: float = Field(ge=0.0, le=1.0)
    upper_inclusive: bool
    count: int = Field(ge=0)
    average_confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    empirical_accuracy: Optional[float] = Field(None, ge=0.0, le=1.0)
    absolute_gap: Optional[float] = Field(None, ge=0.0, le=1.0)


class DecisionOutcomeV2BenchmarkStats(_StrictModel):
    samples: int = Field(ge=0)
    sample_sufficient: bool
    calibration_samples: int = Field(ge=0)
    calibration_sample_sufficient: bool
    accuracy: Optional[float] = Field(None, ge=0.0, le=1.0)
    ece: Optional[float] = Field(None, ge=0.0, le=1.0)
    brier_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    bins: List[DecisionOutcomeV2CalibrationBin] = Field(
        min_length=5,
        max_length=5,
    )
    mean_directional_excess_return_pct: Optional[float] = None
    outperformance_rate: Optional[float] = Field(None, ge=0.0, le=1.0)
    non_underperformance_rate: Optional[float] = Field(None, ge=0.0, le=1.0)


class DecisionOutcomeV2BenchmarkStatsGroup(_StrictModel):
    csi300: DecisionOutcomeV2BenchmarkStats
    sw1: DecisionOutcomeV2BenchmarkStats


class DecisionOutcomeV2StatsBucket(_StrictModel):
    dimensions: DecisionOutcomeV2StatsDimensions
    total: int = Field(ge=0)
    evaluated_directional_outcomes: int = Field(ge=0)
    calibration_samples: int = Field(ge=0)
    sample_sufficient: bool
    accuracy: Optional[float] = Field(None, ge=0.0, le=1.0)
    ece: Optional[float] = Field(None, ge=0.0, le=1.0)
    brier_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    bins: List[DecisionOutcomeV2CalibrationBin] = Field(
        min_length=5,
        max_length=5,
    )
    benchmarks: DecisionOutcomeV2BenchmarkStatsGroup


class DecisionOutcomeV2StatsResponse(_StrictModel):
    contract: Literal["decision-outcome-v2-stats"] = "decision-outcome-v2-stats"
    version: Literal["v1"] = "v1"
    engine_version: str
    horizons: List[DecisionOutcomeV2Horizon]
    bucket_dimensions: Tuple[
        Literal["engine"],
        Literal["horizon"],
        Literal["profile"],
        Literal["final_action_family"],
    ]
    minimum_completed_sample_size: int = Field(ge=1)
    calibration_bin_count: Literal[5]
    buckets: List[DecisionOutcomeV2StatsBucket] = Field(default_factory=list)


__all__ = [
    "DecisionOutcomeV2BenchmarkItem",
    "DecisionOutcomeV2BenchmarkStats",
    "DecisionOutcomeV2CalibrationBin",
    "DecisionOutcomeV2Item",
    "DecisionOutcomeV2ListResponse",
    "DecisionOutcomeV2RunAccepted",
    "DecisionOutcomeV2RunRequest",
    "DecisionOutcomeV2StatsResponse",
    "DecisionOutcomeV2StatsBucket",
]
