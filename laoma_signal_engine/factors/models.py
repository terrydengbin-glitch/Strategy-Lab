"""STEP3B Factor Snapshot Pydantic models. docs/STEP4.0_Decision_Layer_Phase1_任务卡.md."""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from laoma_signal_engine.micro.assembly.models import Micro15mBlock, MicroQualityBlock, MicroSignalBlock

FactorSnapshotStatus = Literal["ok", "no_candidates", "partial", "blocked", "error"]

FactorSnapshotSource = Literal["factor_snapshot", "factor_snapshot_without_ofi_cvd"]

OIQuadrant = Literal["Q1", "Q2", "Q3", "Q4", "unknown"]

FundingBucket = Literal["NEUTRAL", "WARM", "OVERHEATED", "NEGATIVE_EXTREME"]

BasisState = Literal["NEUTRAL", "PREMIUM_MILD", "PREMIUM_WIDE", "DISCOUNT_MILD", "DISCOUNT_WIDE"]


class FactorQualityBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ready: bool
    reason_codes: list[str] = Field(default_factory=list)
    input_warnings: list[str] = Field(default_factory=list)


class OI15mBlock(BaseModel):
    """Open interest 15m context for factor snapshot (STEP4.1)."""

    model_config = ConfigDict(extra="forbid")

    ready: bool
    reason: str
    oi_pct_change: float | None = None
    oi_z: float | None = None
    oi_percentile: float | None = None
    oi_quadrant: OIQuadrant | None = None
    oi_state: str | None = None
    oi_conflict: bool = False


class FundingContextBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ready: bool
    reason: str
    funding_rate_raw: float | None = None
    funding_bucket: FundingBucket | None = None
    funding_extreme_flag: bool = False
    hours_to_settlement: float | None = None


class Basis15mBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ready: bool
    reason: str
    spot_perp_basis_bps: float | None = None
    mark_index_basis_bps: float | None = None
    basis_change_bps: float | None = None
    basis_state: BasisState | None = None
    basis_extreme: bool = False


class FactorSnapshotItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    base_asset: str
    decision_tf: str = "15m"
    source_state: str
    move_side: str
    scan_score: int
    market_entry_suitability_score: int = 0
    market_entry_suitability: str = "unknown"
    market_entry_reason_codes: list[str] = Field(default_factory=list)
    trigger_type: str
    primary_15m: dict[str, Any] = Field(default_factory=dict)
    trigger_5m: dict[str, Any] = Field(default_factory=dict)
    entry_1m: dict[str, Any] = Field(default_factory=dict)
    background: dict[str, Any] = Field(default_factory=dict)
    micro_15m: Micro15mBlock
    micro_quality: MicroQualityBlock
    micro_fast_signal: MicroSignalBlock | None = None
    micro_full_signal: MicroSignalBlock | None = None
    oi_15m: OI15mBlock = Field(
        default_factory=lambda: OI15mBlock(ready=False, reason="not_implemented"),
    )
    funding_context: FundingContextBlock = Field(
        default_factory=lambda: FundingContextBlock(ready=False, reason="not_implemented"),
    )
    basis_15m: Basis15mBlock = Field(
        default_factory=lambda: Basis15mBlock(ready=False, reason="not_implemented"),
    )
    factor_quality: FactorQualityBlock


class FactorSnapshotDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.6"] = "1.6"
    generated_at: str
    source: FactorSnapshotSource = "factor_snapshot"
    status: FactorSnapshotStatus
    count: int
    input_refs: dict[str, Any] = Field(default_factory=dict)
    candidate_alignment: dict[str, Any] = Field(default_factory=dict)
    items: list[FactorSnapshotItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _count_matches_items(self) -> Self:
        if self.count != len(self.items):
            raise ValueError(f"count must equal len(items); got {self.count} vs {len(self.items)}")
        return self
