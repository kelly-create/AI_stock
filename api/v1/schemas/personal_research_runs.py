"""API contracts for durable personal-research runs."""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PersonalResearchRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stock_code: str = Field(min_length=1, max_length=32)
    requested_mode: Literal["auto", "quick", "standard", "deep", "debate"] = "auto"
    priority: int = Field(50, ge=0, le=100)
    manual_daily_override: bool = False
    manual_daily_override_ack: Optional[
        Literal["I_ACCEPT_DAILY_BUDGET_OVERRIDE"]
    ] = None
    report_type: Optional[Literal["brief", "detailed", "full"]] = None
    notify: bool = True
    report_language: Optional[str] = Field(None, max_length=16)
    policy_account_id: Optional[int] = Field(None, gt=0)
    target_weight_pct: Optional[float] = Field(
        None,
        ge=0.0,
        le=100.0,
        allow_inf_nan=False,
    )

    @model_validator(mode="after")
    def validate_manual_override(self) -> "PersonalResearchRunRequest":
        if self.manual_daily_override and (
            self.manual_daily_override_ack != "I_ACCEPT_DAILY_BUDGET_OVERRIDE"
        ):
            raise ValueError(
                "manual_daily_override requires explicit daily-budget acknowledgement"
            )
        if not self.manual_daily_override and self.manual_daily_override_ack is not None:
            raise ValueError(
                "manual_daily_override_ack is only valid when the override is enabled"
            )
        if (self.policy_account_id is None) != (self.target_weight_pct is None):
            raise ValueError(
                "policy_account_id and target_weight_pct must be supplied together"
            )
        return self


class PersonalResearchRunAccepted(BaseModel):
    task_id: str
    trace_id: str
    status: str
    created: bool
    deduplicated: bool
    stock_code: str
    market: Literal["cn"] = "cn"
    requested_mode: Literal["auto", "quick", "standard", "deep", "debate"]
    resolved_mode: Literal["quick", "standard", "deep", "debate"]
    priority: int


__all__ = ["PersonalResearchRunAccepted", "PersonalResearchRunRequest"]
