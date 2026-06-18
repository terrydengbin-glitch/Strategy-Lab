"""Pydantic models for STEP1.6 market-entry liquidity snapshot."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Self


MarketEntryLiquidityStatus = Literal["ok", "partial", "no_symbols", "error"]


class MarketEntryLiquidityItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    last_price: float | None = None
    bid_price: float | None = None
    ask_price: float | None = None
    spread_bps: float | None = None
    top_bid_depth_usdt: float | None = None
    top_ask_depth_usdt: float | None = None
    estimated_market_buy_slippage_bps: float | None = None
    estimated_market_sell_slippage_bps: float | None = None
    liquidity_ok_for_market_entry: bool
    buy_liquidity_ok_for_market_entry: bool = False
    sell_liquidity_ok_for_market_entry: bool = False
    notional_usdt: float | None = None
    max_spread_bps: float | None = None
    max_estimated_slippage_bps: float | None = None
    min_top_depth_usdt: float | None = None
    min_quote_volume_24h: float | None = None
    buy_reason_codes: list[str] = Field(default_factory=list)
    sell_reason_codes: list[str] = Field(default_factory=list)
    reason_codes: list[str] = Field(default_factory=list)


class MarketEntryLiquidityDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.6"] = "1.6"
    generated_at: str
    source: Literal["market_entry_liquidity_snapshot"] = "market_entry_liquidity_snapshot"
    status: MarketEntryLiquidityStatus
    count: int
    max_spread_bps: float
    max_estimated_slippage_bps: float
    min_top_depth_usdt: float
    min_quote_volume_24h: float
    margin_usdt: float = 100.0
    leverage: float = 20.0
    notional_usdt: float = 2_000.0
    items: list[MarketEntryLiquidityItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _count_matches_items(self) -> Self:
        if self.count != len(self.items):
            raise ValueError(f"count must equal len(items); got {self.count} vs {len(self.items)}")
        return self
