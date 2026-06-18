"""Direction Gate output models (STEP4). docs/STEP4.0_Decision_Layer_Phase1_任务卡.md."""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

DirectionGateStatus = Literal["ok", "no_candidates", "partial", "stale_input", "error"]
DecisionKind = Literal[
    "LONG_NOW",
    "LONG_WAIT_PULLBACK",
    "SHORT_NOW",
    "SHORT_WAIT_REBOUND",
    "HOLD_WATCH",
    "HOLD_NO_TRADE",
    "REJECT",
]
DirectionKind = Literal["LONG", "SHORT", "HOLD", "NONE"]
ActionKind = Literal["ENTER", "WAIT", "HOLD", "REJECT"]
EntryModeKind = Literal["NOW", "WAIT_PULLBACK", "WAIT_REBOUND", "WATCH", "NONE"]


class DirectionDecisionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    decision_tf: str = "15m"
    decision: DecisionKind
    direction: DirectionKind
    action: ActionKind
    entry_mode: EntryModeKind
    confidence: int = Field(ge=0, le=100)
    reason_codes: list[str] = Field(default_factory=list)
    guards: dict[str, Any] = Field(default_factory=dict)
    input_refs: dict[str, Any] = Field(default_factory=dict)
    summary_for_orchestrator: str

    @field_validator("summary_for_orchestrator", mode="before")
    @classmethod
    def _summary_ascii(cls, v: object) -> object:
        if isinstance(v, str) and any(ord(ch) > 127 for ch in v):
            msg = "summary_for_orchestrator must be ASCII-only"
            raise ValueError(msg)
        return v


class DirectionGateDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.6"] = "1.6"
    generated_at: str
    source: Literal["direction_gate"] = "direction_gate"
    status: DirectionGateStatus
    count: int
    decisions: list[DirectionDecisionItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _count_matches_decisions(self) -> Self:
        if self.count != len(self.decisions):
            raise ValueError(f"count must equal len(decisions); got {self.count} vs {len(self.decisions)}")
        return self
