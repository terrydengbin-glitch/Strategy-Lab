"""P10 independent trade plan line models."""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


TradePlanLineSource = Literal[
    "trade_plan_without_micro",
    "trade_plan_micro_fast",
    "trade_plan_micro_full",
    "trade_plan_strategy4",
    "trade_plan_strategy5",
    "trade_plan_strategy6",
]
TradePlanMicroMode = Literal["none", "fast", "full", "strategy4_observe", "strategy5_evidence", "strategy6_market_accepted"]
TradePlanStatus = Literal["ok", "partial", "no_entries", "stale_input", "blocked", "error"]
TradePlanDecision = Literal["LONG", "SHORT", "NO_TRADE"]
TradePlanAction = Literal["ENTER_MARKET", "ENTER_LIMIT", "WAIT", "NO_TRADE"]
TradePlanEntryMode = Literal[
    "MARKET",
    "LIMIT_PULLBACK",
    "LIMIT_REBOUND",
    "BREAKOUT_TRIGGER",
    "BREAKDOWN_TRIGGER",
    "WAIT_PULLBACK",
    "WAIT_REBOUND",
    "WAIT_CONFIRMATION",
    "NONE",
]


class TradePlanItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    decision_tf: str = "15m"
    decision: TradePlanDecision
    action: TradePlanAction
    entry_mode: TradePlanEntryMode
    estimated_entry_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    risk_per_unit: float | None = None
    reward_per_unit: float | None = None
    rr: float | None = None
    executable: bool
    confidence: int = Field(ge=0, le=100)
    reason_codes: list[str] = Field(default_factory=list)
    position_sizing: dict[str, Any] | None = None
    guards: dict[str, Any] = Field(default_factory=dict)
    input_refs: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _executable_has_complete_prices(self) -> Self:
        if self.executable:
            allowed = {
                ("ENTER_MARKET", "MARKET"),
                ("ENTER_LIMIT", "LIMIT_PULLBACK"),
                ("ENTER_LIMIT", "LIMIT_REBOUND"),
            }
            if (self.action, self.entry_mode) not in allowed:
                raise ValueError("executable plan must be market or limit executable")
            required = [
                self.estimated_entry_price,
                self.stop_loss,
                self.take_profit,
                self.risk_per_unit,
                self.reward_per_unit,
                self.rr,
            ]
            if any(v is None for v in required):
                raise ValueError("executable plan must include entry, SL, TP, risk, reward, and RR")
            if self.position_sizing is not None:
                qty = self.position_sizing.get("quantity")
                notional = self.position_sizing.get("notional_usdt")
                margin = self.position_sizing.get("margin_usdt")
                if qty is not None and float(qty) <= 0:
                    raise ValueError("position_sizing.quantity must be positive")
                if notional is not None and float(notional) <= 0:
                    raise ValueError("position_sizing.notional_usdt must be positive")
                if margin is not None and float(margin) <= 0:
                    raise ValueError("position_sizing.margin_usdt must be positive")
        if self.decision == "NO_TRADE":
            if self.executable or self.action == "ENTER_MARKET" or self.entry_mode == "MARKET":
                raise ValueError("NO_TRADE must not be executable")
        return self


class TradePlanLineDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    generated_at: str
    run_id: str | None = None
    cycle_id: str | None = None
    source: TradePlanLineSource
    micro_mode: TradePlanMicroMode
    status: TradePlanStatus
    count: int
    executable_count: int
    input_refs: dict[str, Any] = Field(default_factory=dict)
    candidate_alignment: dict[str, Any] = Field(default_factory=dict)
    plans: list[TradePlanItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _contract_matches_line(self) -> Self:
        expected_mode = {
            "trade_plan_without_micro": "none",
            "trade_plan_micro_fast": "fast",
            "trade_plan_micro_full": "full",
            "trade_plan_strategy4": "none",
            "trade_plan_strategy5": "strategy5_evidence",
            "trade_plan_strategy6": "strategy6_market_accepted",
        }[self.source]
        if self.micro_mode != expected_mode:
            raise ValueError(f"micro_mode must be {expected_mode!r} for source={self.source!r}")
        if self.count != len(self.plans):
            raise ValueError(f"count must equal len(plans); got {self.count} vs {len(self.plans)}")
        exe = sum(1 for it in self.plans if it.executable)
        if self.executable_count != exe:
            raise ValueError(f"executable_count must match plans; got {self.executable_count} vs {exe}")
        return self
