"""Pydantic models for futures_light_snapshot.json (Step 1.5)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from laoma_signal_engine.core.models import SymbolRiskProfileBlock, UniverseProfileBlock


class TimeframeContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    primary_tf: str
    trigger_tf: str
    entry_tf: str
    background_tfs: list[str]
    decision_basis: str


class SnapshotErrorEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    error_code: str
    stage: str


class DataQualityBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kline_1m_ready: bool = False
    kline_5m_ready: bool = False
    kline_15m_ready: bool = False
    kline_1h_ready: bool = False
    ticker_24h_ready: bool = False
    last_closed_kline_age_sec: float | None = None
    snapshot_age_sec: float | None = None
    uses_open_kline_for_rolling: bool = False
    error_code: str | None = None
    error_message: str | None = None


class Primary15mBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    price_ret: float | None = None
    volume_ratio: float | None = None
    quote_volume: float | None = None
    atr: float | None = None
    range_pos: float | None = None
    range_pos_raw: float | None = None
    range_pos_clamped: float | None = None
    range_break_state: str = "inside"
    structure_state: str = "unknown"
    volatility_state: str = "normal"

    taker_buy_volume: float | None = None
    taker_sell_volume: float | None = None
    taker_buy_ratio: float | None = None
    kline_cvd_delta: float | None = None
    kline_cvd_state: str = "unavailable"

    recent_swing_high: float | None = None
    recent_swing_low: float | None = None
    breakout_level: float | None = None
    breakdown_level: float | None = None
    ready: bool = False


class Trigger5mBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    price_ret: float | None = None
    volume_ratio: float | None = None
    acceleration_state: str = "neutral"
    pullback_state: str = "not_yet"
    rebound_state: str = "none"


class Entry1mBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    atr: float | None = None
    last_pullback_low: float | None = None
    last_breakout_high: float | None = None
    last_rebound_high: float | None = None
    last_breakdown_low: float | None = None


class BackgroundBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    price_ret_1h: float | None = None
    price_ret_24h: float | None = None
    quote_volume_24h: float | None = None
    is_top_gainer_24h: bool = False
    is_top_loser_24h: bool = False
    background_overheat: bool = False


class TradabilityProfileBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    activity_score: int = Field(0, ge=0, le=100)
    hotness_score: int = Field(0, ge=0, le=100)
    light_liquidity_score: int = Field(0, ge=0, le=100)
    volatility_score: int = Field(0, ge=0, le=100)
    volume_accel_score: int = Field(0, ge=0, le=100)
    tradability_score: int = Field(0, ge=0, le=100)
    tradability_tier: str = "unknown"
    market_entry_score: int = Field(0, ge=0, le=100)
    hf_stop_score: int = Field(0, ge=0, le=100)
    slippage_risk_score: int = Field(0, ge=0, le=100)
    depth_stability_score: int = Field(0, ge=0, le=100)
    volume_activity_score: int = Field(0, ge=0, le=100)
    spread_quality_score: int = Field(0, ge=0, le=100)
    trade_quality_tier: str = "unknown"
    scan_priority: int = Field(0, ge=0, le=100)
    reason_codes: list[str] = Field(default_factory=list)


class LightSnapshotItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    base_asset: str
    last_price: float | None = None
    decision_tf: str = "15m"
    primary_15m: Primary15mBlock
    trigger_5m: Trigger5mBlock
    entry_1m: Entry1mBlock
    background: BackgroundBlock
    reason_codes: list[str] = Field(default_factory=list)
    data_quality: DataQualityBlock
    universe_profile: UniverseProfileBlock = Field(default_factory=UniverseProfileBlock)
    risk_profile: SymbolRiskProfileBlock = Field(default_factory=SymbolRiskProfileBlock)
    tradability_profile: TradabilityProfileBlock = Field(default_factory=TradabilityProfileBlock)
    primary_pool: str = "unknown"
    pool_tags: list[str] = Field(default_factory=list)
    item_snapshot_source: str | None = None
    item_snapshot_age_sec: float | None = None
    item_freshness_sla_sec: int | None = None
    item_freshness_status: str | None = None
    item_downstream_allowed: bool | None = None
    item_downstream_scope: str | None = None
    last_live_refresh_at: str | None = None
    websocket_cache_generated_at: str | None = None
    rest_cache_generated_at: str | None = None
    snapshot_source_priority: str | None = None
    shard_id: str | None = None


class FuturesLightSnapshotDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str
    generated_at: str
    source: str
    universe_generated_at: str
    universe_age_sec: int
    universe_count: int
    eligible_futures_count: int
    snapshot_count: int
    success_count: int
    failed_count: int
    skipped_count: int
    timeframe_contract: TimeframeContract
    items: list[LightSnapshotItem]
    errors: list[SnapshotErrorEntry] = Field(default_factory=list)
    pools: dict[str, list[str]] = Field(default_factory=dict)
    snapshot_quality: dict[str, object] = Field(default_factory=dict)
