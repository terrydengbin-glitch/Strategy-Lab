"""Pydantic models for CANDIDATE_UNIVERSE and related JSON artifacts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class UniverseCounts(BaseModel):
    """Break down union universe so total_pairs is not mistaken for perp-only size."""

    model_config = ConfigDict(extra="forbid")

    total_pairs: int = Field(..., ge=0)
    futures_count: int = Field(..., ge=0)
    spot_count: int = Field(..., ge=0)
    both_spot_and_futures: int = Field(..., ge=0)
    futures_only: int = Field(..., ge=0)
    spot_only: int = Field(..., ge=0)
    neither_spot_nor_futures: int = Field(
        0,
        ge=0,
        description="Rows with no TRADING spot and no USDT-M perp (e.g. manual-only placeholder)",
    )


class UniverseProfileBlock(BaseModel):
    """Static symbol governance profile from Step 1."""

    model_config = ConfigDict(extra="forbid")

    universe_tier: str = "tier_X_excluded"
    universe_priority_score: int = Field(0, ge=0, le=100)
    scan_tier: str = "ignore"
    business_pool: str = "unknown"
    scan_eligibility: str = "ignore"
    symbol_risk_tags: list[str] = Field(default_factory=list)
    trade_symbol: str | None = None
    display_asset: str | None = None
    social_cashtag: str | None = None
    contract_multiplier: int = Field(1, ge=1)
    is_multiplier_contract: bool = False
    manual_mode: str = ""
    manual_priority: int = Field(0, ge=0, le=100)
    manual_reason: str = ""


class SymbolRiskProfileBlock(BaseModel):
    """Static execution/risk hints; downstream uses these as auditable guards."""

    model_config = ConfigDict(extra="forbid")

    liquidity_tier: str = "unknown"
    volatility_tier: str = "unknown"
    execution_tier: str = "unknown"
    rr_policy: str = "normal"
    sl_template: str = "normal"
    rr_template: str = "standard"
    sizing_template: str = "normal"
    feishu_policy: str = "send"
    min_stop_bps: float | None = None
    min_target_bps: float | None = None
    max_chase_bps: float | None = None


class UniversePairRow(BaseModel):
    """One row in CANDIDATE_UNIVERSE pairs list."""

    model_config = ConfigDict(extra="forbid")

    base_asset: str = Field(..., description="exchangeInfo baseAsset (may include 1000/10000 prefix)")
    display_base_asset: str = Field(
        ...,
        description="Human display base after stripping common multiplier prefixes",
    )
    cashtag: str = Field(..., description="Social-style cashtag, e.g. $PEPE")
    spot_cashtag_symbol: str = Field(
        ...,
        description="Normalized spot-style USDT label for copy (display_base + USDT)",
    )
    symbol_safe_id: str | None = Field(
        None,
        description="Prefer futures_symbol; else spot_symbol; stable machine id for the row",
    )
    spot_symbol: str | None = Field(None, description="Spot symbol if listed, e.g. BTCUSDT")
    futures_symbol: str | None = Field(None, description="USDT-M perpetual symbol if listed")
    has_spot: bool = False
    has_um_futures: bool = False
    eligible_for_signal_engine: bool = Field(
        ...,
        description="True when this row should be fed to the signal engine (perp + trade analysis)",
    )
    eligible_for_post: bool = Field(
        ...,
        description=(
            "True only when both USDT spot and USDT-M perp exist (dual-leg, futures-first posts). "
            "False for spot-only (no perp leg) and futures-only (no spot leg)."
        ),
    )
    eligible_for_trade_analysis: bool = Field(
        ...,
        description="True when USDT-M perpetual exists and is part of the trade-analysis universe",
    )
    quote_volume_24h_futures: float | None = None
    price_change_24h_futures: float | None = None
    rank_futures_volume: int | None = None
    rank_futures_gainer: int | None = None
    rank_futures_loser: int | None = None
    source_tags: list[str] = Field(default_factory=list)
    universe_profile: UniverseProfileBlock = Field(default_factory=UniverseProfileBlock)
    risk_profile: SymbolRiskProfileBlock = Field(default_factory=SymbolRiskProfileBlock)


class CandidateUniverseDocument(BaseModel):
    """Top-level document written to CANDIDATE_UNIVERSE.json."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str
    generated_at: str
    expires_at: str
    ttl_seconds: int = 86400
    status: str = "fresh"
    source: str = "binance"
    profile_schema_version: str = "step1.61-business-pool-v1"
    profile_hydration_source: str = "step1_candidate_universe"
    profile_hydration_status: str = "ok"
    profile_hydration_reason_codes: list[str] = Field(default_factory=list)
    profile_hydration_counts: dict[str, Any] = Field(default_factory=dict)
    count: int
    counts: UniverseCounts
    pairs: list[UniversePairRow]


class ManualWatchlistFile(BaseModel):
    """Optional manual_watchlist.json shape."""

    model_config = ConfigDict(extra="ignore")

    schema_version: str | None = None
    bases: list[str] = Field(default_factory=list)
    entries: list["ManualWatchlistEntry"] = Field(default_factory=list)


class ManualWatchlistEntry(BaseModel):
    """Optional richer manual watchlist row."""

    model_config = ConfigDict(extra="ignore")

    base: str
    mode: str = "watch_only"
    priority: int = Field(0, ge=0, le=100)
    reason: str = ""
