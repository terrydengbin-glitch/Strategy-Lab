"""Pydantic models for DATA/micro/micro_targets.json (Step 2.5)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from laoma_signal_engine.core.models import SymbolRiskProfileBlock, UniverseProfileBlock
from laoma_signal_engine.market.light_snapshot_models import TradabilityProfileBlock


class InputCountsBlock(BaseModel):
    model_config = ConfigDict(extra="ignore")

    raw: int
    watch: int
    strong: int


class RoutedCountsBlock(BaseModel):
    model_config = ConfigDict(extra="ignore")

    tier1: int
    tier2: int


class TruncatedBlock(BaseModel):
    model_config = ConfigDict(extra="ignore")

    tier1: bool
    tier2: bool


class CandidateAlignmentBlock(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: str = "1.0"
    mode: str = "micro_targets_authoritative"
    include_tier1: bool = True
    include_tier2: bool = True
    include_ready_cache: bool = False
    ready_cache_max_age_sec: int = 0
    generated_at: str


class MicroTargetEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    symbol: str
    base_asset: str
    source_state: str
    priority: int
    scan_score: int
    market_entry_suitability_score: int = 0
    market_entry_suitability: str = "unknown"
    trade_candidate_rank_score: int = 0
    trade_candidate_bucket: str = "unknown"
    universe_profile: UniverseProfileBlock = Field(default_factory=UniverseProfileBlock)
    risk_profile: SymbolRiskProfileBlock = Field(default_factory=SymbolRiskProfileBlock)
    tradability_profile: TradabilityProfileBlock = Field(default_factory=TradabilityProfileBlock)
    primary_pool: str = "unknown"
    pool_tags: list[str] = Field(default_factory=list)
    scan_priority: int = 0
    profile_priority_boost: int = 0
    promoted_from_raw: bool = False
    move_side: str
    trigger_type: str
    subscribe: list[str]
    target_ready_tf: str
    min_collect_seconds: int
    ttl_seconds: int
    sticky_source: str = "current"
    sticky_age_sec: int = 0
    sticky_cycle_count: int = 1
    retained_reason: str = "current_candidate"
    sticky_plan_candidate: bool = True


class MicroTargetsDocument(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: str
    generated_at: str
    source: str = "micro_target_router"
    status: str
    warm_watch_limit: int
    active_strong_limit: int
    max_active_micro_symbols: int = 0
    priority_mode: str = "scan_score"
    promoted_raw_count: int = 0
    excluded_trade_avoid_count: int = 0
    candidate_quality_counts: dict[str, int] = Field(default_factory=dict)
    input_watch_status: str = ""
    input_strong_status: str = ""
    input_snapshot_generated_at: str
    input_snapshot_age_sec: int
    step2_reported_input_snapshot_age_sec: int = -1
    router_computed_input_snapshot_age_sec: int = -1
    router_freshness_ok: bool
    input_counts: InputCountsBlock
    routed_counts: RoutedCountsBlock
    truncated: TruncatedBlock
    skip_reasons: list[str] = Field(default_factory=list)
    block_downstream: bool = False
    block_reason: str = ""
    step2_current_freshness: dict[str, object] = Field(default_factory=dict)
    target_set_id: str = ""
    candidate_hash: str = ""
    target_symbols: list[str] = Field(default_factory=list)
    target_count: int = 0
    plan_candidate_symbols: list[str] = Field(default_factory=list)
    plan_candidate_count: int = 0
    candidate_alignment: CandidateAlignmentBlock | None = None
    sticky_pool: dict[str, object] = Field(default_factory=dict)
    raw_fill: dict[str, object] = Field(default_factory=dict)
    target_source_distribution: dict[str, int] = Field(default_factory=dict)
    tier1_warm_watch: list[MicroTargetEntry] = Field(default_factory=list)
    tier2_active_strong: list[MicroTargetEntry] = Field(default_factory=list)
