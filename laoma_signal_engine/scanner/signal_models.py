"""Pydantic models for Step 2 abnormal scanner outputs (raw/watch/strong JSON)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from laoma_signal_engine.market.light_snapshot_models import (
    BackgroundBlock,
    Primary15mBlock,
    TradabilityProfileBlock,
    Trigger5mBlock,
)
from laoma_signal_engine.core.models import SymbolRiskProfileBlock, UniverseProfileBlock


class ScoreBreakdownBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    price_score: int
    volume_score: int
    kline_cvd_score: int
    trigger_5m_score: int
    liquidity_score: int
    background_penalty: int


class CandidateCountsBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    preferred: int = 0
    allowed: int = 0
    observe: int = 0
    avoid: int = 0
    unknown: int = 0


class AbnormalSignalEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    base_asset: str
    futures_symbol: str
    has_um_futures: bool
    decision_tf: str = "15m"
    source_tags: list[str] = Field(default_factory=list)
    universe_profile: UniverseProfileBlock = Field(default_factory=UniverseProfileBlock)
    risk_profile: SymbolRiskProfileBlock = Field(default_factory=SymbolRiskProfileBlock)
    tradability_profile: TradabilityProfileBlock = Field(default_factory=TradabilityProfileBlock)
    primary_pool: str = "unknown"
    pool_tags: list[str] = Field(default_factory=list)
    scan_priority: int = 0
    state: str
    move_side: str
    scan_score: int
    market_entry_suitability_score: int = 0
    market_entry_suitability: str = "unknown"
    market_entry_reason_codes: list[str] = Field(default_factory=list)
    trade_candidate_rank_score: int = 0
    trade_candidate_bucket: str = "unknown"
    trade_candidate_reason_codes: list[str] = Field(default_factory=list)
    promoted_from_raw: bool = False
    score_breakdown: ScoreBreakdownBlock
    input_snapshot_generated_at: str
    trigger_type: str
    primary_15m: Primary15mBlock
    trigger_5m: Trigger5mBlock
    background: BackgroundBlock
    reason_codes: list[str] = Field(default_factory=list)
    next_stage: str


class AbnormalTierDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str
    generated_at: str
    source: str = "abnormal_scanner"
    tier: str
    status: str
    input_snapshot_generated_at: str
    input_snapshot_age_sec: int
    input_freshness: str
    stale_warning: bool = False
    reason_codes: list[str] = Field(default_factory=list)
    count: int
    market_entry_counts: CandidateCountsBlock = Field(default_factory=CandidateCountsBlock)
    trade_candidate_counts: CandidateCountsBlock = Field(default_factory=CandidateCountsBlock)
    signals: list[AbnormalSignalEntry] = Field(default_factory=list)
