"""STEP4.3A pre-decision candidate refresh models."""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


DecisionRefreshStatus = Literal["ok", "no_candidates", "partial", "stale_input", "error"]


class DecisionRefreshItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    base_asset: str
    move_side: str | None = None
    source_state: str | None = None
    last_price: float | None = None
    refresh_age_sec: int
    direction_still_valid: bool
    range_room_ok: bool
    range_gate: dict[str, Any] = Field(default_factory=dict)
    liquidity_ok: bool | None = None
    liquidity_age_sec: int | None = None
    reason_codes: list[str] = Field(default_factory=list)
    primary_15m: dict[str, Any] = Field(default_factory=dict)
    trigger_5m: dict[str, Any] = Field(default_factory=dict)
    entry_1m: dict[str, Any] = Field(default_factory=dict)
    background: dict[str, Any] = Field(default_factory=dict)
    data_quality: dict[str, Any] = Field(default_factory=dict)
    liquidity: dict[str, Any] | None = None


class DecisionRefreshDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0", "1.1"] = "1.0"
    generated_at: str
    source: Literal["pre_decision_candidate_refresh"] = "pre_decision_candidate_refresh"
    line: str | None = None
    run_id: str | None = None
    cycle_id: str | None = None
    input_refs: dict[str, Any] = Field(default_factory=dict)
    compat_view: bool = False
    canonical_per_line_path: str | None = None
    status: DecisionRefreshStatus
    input_light_generated_at: str | None = None
    input_factor_generated_at: str | None = None
    input_liquidity_generated_at: str | None = None
    max_refresh_age_sec: int
    max_liquidity_age_sec: int = 180
    long_max_range_pos: float = 0.82
    short_min_range_pos: float = 0.18
    candidate_count: int
    refreshed_count: int
    stale_count: int
    items: list[DecisionRefreshItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _counts_match_items(self) -> Self:
        if self.refreshed_count != len(self.items):
            raise ValueError(
                f"refreshed_count must equal len(items); got {self.refreshed_count} vs {len(self.items)}",
            )
        stale = sum(1 for it in self.items if "refresh_stale" in it.reason_codes)
        if self.stale_count != stale:
            raise ValueError(f"stale_count must match items; got {self.stale_count} vs {stale}")
        return self
