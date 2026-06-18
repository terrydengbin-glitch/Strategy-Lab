"""Current-state contract for the persistent micro daemon (STEP10.7)."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


TargetChurnState = Literal["new", "kept", "promoted", "demoted", "retiring", "removed"]
TargetStatus = Literal["active", "retiring"]


class MicroDaemonStreamSubscriptionState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required: bool = False
    desired: bool = False
    active: bool = False
    last_event_ts_sec: int | None = None
    last_ack_ts_sec: int | None = None
    missing_reason: str | None = None


class MicroDaemonSymbolState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    target_status: TargetStatus = "active"
    source_state: str | None = None
    move_side: str | None = None
    priority: int | None = None
    first_seen_at: str
    last_seen_at: str
    continuous_collect_sec: int
    seen_cycle_count: int = 1
    fast_ready: bool
    full_ready: bool
    fast_reason_codes: list[str] = Field(default_factory=list)
    full_reason_codes: list[str] = Field(default_factory=list)
    full_ready_eta_sec: int
    last_micro_generated_at: str
    target_churn_state: TargetChurnState = "kept"
    consumer_safe: bool
    consumer_reason_codes: list[str] = Field(default_factory=list)
    subscription_state: dict[str, MicroDaemonStreamSubscriptionState] = Field(default_factory=dict)
    health_guard_state: dict[str, object] = Field(default_factory=dict)


class MicroDaemonStateDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    generated_at: str
    source: Literal["persistent_micro_daemon_state"] = "persistent_micro_daemon_state"
    daemon_status: Literal["running", "error"] = "running"
    health_state: str = ""
    target_generated_at: str
    target_version: str
    target_age_sec: int
    active_symbol_count: int
    state_ready_for_consumers: bool
    reason_codes: list[str] = Field(default_factory=list)
    symbols: list[MicroDaemonSymbolState] = Field(default_factory=list)

    @model_validator(mode="after")
    def _count_matches_symbols(self) -> Self:
        if self.active_symbol_count != len(self.symbols):
            raise ValueError(
                f"active_symbol_count must equal len(symbols); got {self.active_symbol_count} vs {len(self.symbols)}",
            )
        return self
