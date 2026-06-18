"""STEP5.0 latest_decisions.json Pydantic models. docs/STEP5.0_SL_TP_Planner_and_Final_Decisions_任务卡.md."""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from laoma_signal_engine.decision.models import DecisionKind, DirectionGateStatus

FinalDecisionsStatus = DirectionGateStatus
RiskPlanPlanStatus = Literal[
    "executable",
    "pending_trigger",
    "observe_only",
    "no_trade",
    "risk_rejected",
]
EntryPriceBasis = Literal[
    "last_price",
    "breakout_level",
    "pullback_level",
    "breakdown_level",
    "rebound_level",
]


class RiskPlanBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plan_status: RiskPlanPlanStatus
    entry_price_basis: EntryPriceBasis | None = None
    entry_zone_low: float | None = None
    entry_zone_high: float | None = None
    stop_loss: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    rr_to_tp1: float | None = None
    rr_to_tp2: float | None = None
    time_stop_minutes: int | None = None
    invalid_condition: str = ""
    estimated_entry_zone_low: float | None = None
    estimated_entry_zone_high: float | None = None
    trigger_condition: str = ""

    @field_validator("invalid_condition", "trigger_condition", mode="before")
    @classmethod
    def _ascii_plan_text(cls, v: object) -> object:
        if isinstance(v, str) and any(ord(ch) > 127 for ch in v):
            msg = "risk_plan text must be ASCII-only"
            raise ValueError(msg)
        return v


class FinalDecisionsMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    direction_generated_at: str = ""
    factor_snapshot_generated_at: str = ""
    planner_config_version: str = "5.0-mvp"
    trade_plan_sources: dict[str, str] = Field(default_factory=dict)


class FinalDecisionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    base_asset: str
    cashtag: str
    decision_tf: str = "15m"
    decision: DecisionKind
    direction: Literal["LONG", "SHORT", "HOLD", "NONE"]
    action: Literal["ENTER", "WAIT", "HOLD", "REJECT"]
    entry_mode: Literal["NOW", "WAIT_PULLBACK", "WAIT_REBOUND", "WATCH", "NONE"]
    confidence: int = Field(ge=0, le=100)
    risk_plan: RiskPlanBlock
    reason_codes: list[str] = Field(default_factory=list)
    guards: dict[str, Any] = Field(default_factory=dict)
    input_refs: dict[str, Any] = Field(default_factory=dict)
    summary_for_orchestrator: str
    llm_hint: str | None = None

    @field_validator("summary_for_orchestrator", "llm_hint", mode="before")
    @classmethod
    def _ascii_strings(cls, v: object) -> object:
        if v is None:
            return v
        if isinstance(v, str) and any(ord(ch) > 127 for ch in v):
            msg = "must be ASCII-only"
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def _wait_not_executable(self) -> Self:
        d = self.decision
        rp = self.risk_plan
        if d in ("LONG_WAIT_PULLBACK", "SHORT_WAIT_REBOUND"):
            if self.action == "ENTER" or self.entry_mode == "NOW" or rp.plan_status == "executable":
                raise ValueError("WAIT decision must not be ENTER/NOW/executable risk_plan")
        return self


class RejectedDecisionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    original_decision: DecisionKind
    reject_reason_codes: list[str] = Field(default_factory=list)
    input_refs: dict[str, Any] = Field(default_factory=dict)
    summary_for_orchestrator: str

    @field_validator("summary_for_orchestrator", mode="before")
    @classmethod
    def _summary_ascii(cls, v: object) -> object:
        if isinstance(v, str) and any(ord(ch) > 127 for ch in v):
            msg = "summary_for_orchestrator must be ASCII-only"
            raise ValueError(msg)
        return v


class FinalDecisionsDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.6"] = "1.6"
    generated_at: str
    source: Literal["final_decision_planner"] = "final_decision_planner"
    status: FinalDecisionsStatus
    count: int
    decisions: list[FinalDecisionItem] = Field(default_factory=list)
    rejected: list[RejectedDecisionItem] = Field(default_factory=list)
    meta: FinalDecisionsMeta = Field(default_factory=FinalDecisionsMeta)

    @model_validator(mode="after")
    def _count_matches_decisions(self) -> Self:
        if self.count != len(self.decisions):
            raise ValueError(f"count must equal len(decisions); got {self.count} vs {len(self.decisions)}")
        return self
