"""Market-entry direction and plan models."""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


MarketEntryDirectionStatus = Literal["ok", "no_candidates", "partial", "stale_input", "error"]
MarketEntryDecisionKind = Literal["LONG_MARKET", "SHORT_MARKET", "NO_MARKET_ENTRY"]
MarketEntryDirectionKind = Literal["LONG", "SHORT", "NONE"]
MarketEntryActionKind = Literal["ENTER_MARKET", "NO_TRADE"]
MarketEntryPlanStatus = Literal["ok", "no_entries", "partial", "error"]


class MarketEntryDirectionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    decision_tf: str = "15m"
    decision: MarketEntryDecisionKind
    direction: MarketEntryDirectionKind
    action: MarketEntryActionKind
    confidence: int = Field(ge=0, le=100)
    reason_codes: list[str] = Field(default_factory=list)
    guards: dict[str, Any] = Field(default_factory=dict)
    input_refs: dict[str, Any] = Field(default_factory=dict)


class MarketEntryDirectionDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    generated_at: str
    source: Literal["market_entry_direction_gate"] = "market_entry_direction_gate"
    status: MarketEntryDirectionStatus
    count: int
    decisions: list[MarketEntryDirectionItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _count_matches_decisions(self) -> Self:
        if self.count != len(self.decisions):
            raise ValueError(f"count must equal len(decisions); got {self.count} vs {len(self.decisions)}")
        return self


class MarketEntryPlanItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    decision_tf: str = "15m"
    decision: MarketEntryDecisionKind
    direction: MarketEntryDirectionKind
    action: MarketEntryActionKind
    entry_mode: Literal["MARKET", "NONE"]
    estimated_entry_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    risk_per_unit: float | None = None
    reward_per_unit: float | None = None
    rr: float | None = None
    executable: bool
    reason_codes: list[str] = Field(default_factory=list)
    guards: dict[str, Any] = Field(default_factory=dict)
    input_refs: dict[str, Any] = Field(default_factory=dict)


class MarketEntryPlanDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    generated_at: str
    source: Literal["market_entry_sl_tp_planner"] = "market_entry_sl_tp_planner"
    status: MarketEntryPlanStatus
    count: int
    executable_count: int
    plans: list[MarketEntryPlanItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _counts_match_plans(self) -> Self:
        if self.count != len(self.plans):
            raise ValueError(f"count must equal len(plans); got {self.count} vs {len(self.plans)}")
        exe = sum(1 for it in self.plans if it.executable)
        if self.executable_count != exe:
            raise ValueError(f"executable_count must match plans; got {self.executable_count} vs {exe}")
        return self
