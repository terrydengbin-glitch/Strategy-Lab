"""STEP1.6 market-entry liquidity snapshot."""

from __future__ import annotations

import json
from pathlib import Path

from laoma_signal_engine.market.light_snapshot_models import (
    BackgroundBlock,
    DataQualityBlock,
    Entry1mBlock,
    FuturesLightSnapshotDocument,
    LightSnapshotItem,
    Primary15mBlock,
    TimeframeContract,
    Trigger5mBlock,
)
from laoma_signal_engine.market.market_entry_liquidity import (
    MarketEntryLiquidityConfig,
    build_liquidity_item,
    build_market_entry_liquidity_document,
)
from laoma_signal_engine.market.market_entry_liquidity_models import MarketEntryLiquidityDocument
from laoma_signal_engine.cli import _build_parser


def _light_doc() -> FuturesLightSnapshotDocument:
    return FuturesLightSnapshotDocument(
        schema_version="1.6",
        generated_at="2026-01-01T00:00:00Z",
        source="futures_light_snapshot",
        universe_generated_at="2026-01-01T00:00:00Z",
        universe_age_sec=1,
        universe_count=1,
        eligible_futures_count=1,
        snapshot_count=1,
        success_count=1,
        failed_count=0,
        skipped_count=0,
        timeframe_contract=TimeframeContract(
            primary_tf="15m",
            trigger_tf="5m",
            entry_tf="1m",
            background_tfs=["1h", "24h"],
            decision_basis="rolling_15m",
        ),
        items=[
            LightSnapshotItem(
                symbol="AAAUSDT",
                base_asset="AAA",
                last_price=100.0,
                primary_15m=Primary15mBlock(),
                trigger_5m=Trigger5mBlock(),
                entry_1m=Entry1mBlock(),
                background=BackgroundBlock(quote_volume_24h=5_000_000.0),
                data_quality=DataQualityBlock(),
            )
        ],
        errors=[],
    )


def test_build_liquidity_item_ok() -> None:
    cfg = MarketEntryLiquidityConfig(
        max_spread_bps=20,
        max_estimated_slippage_bps=20,
        min_top_depth_usdt=1000,
        min_quote_volume_24h=1000,
        notional_usdt=500,
    )
    item = build_liquidity_item(
        symbol="AAAUSDT",
        last_price=100.0,
        quote_volume_24h=5_000_000.0,
        book={"bidPrice": "99.95", "askPrice": "100.05"},
        depth={
            "bids": [["99.95", "20"]],
            "asks": [["100.05", "20"]],
        },
        cfg=cfg,
    )
    assert item.liquidity_ok_for_market_entry is True
    assert item.spread_bps is not None and item.spread_bps > 0
    assert item.reason_codes == []


def test_build_liquidity_item_reasons() -> None:
    cfg = MarketEntryLiquidityConfig(
        max_spread_bps=1,
        max_estimated_slippage_bps=1,
        min_top_depth_usdt=1_000_000,
        min_quote_volume_24h=10_000_000,
        notional_usdt=5_000,
    )
    item = build_liquidity_item(
        symbol="AAAUSDT",
        last_price=100.0,
        quote_volume_24h=1000.0,
        book={"bidPrice": "99", "askPrice": "101"},
        depth={"bids": [["99", "1"]], "asks": [["101", "1"]]},
        cfg=cfg,
    )
    assert item.liquidity_ok_for_market_entry is False
    assert "spread_too_wide" in item.reason_codes
    assert "quote_volume_too_low" in item.reason_codes


def test_build_liquidity_item_side_specific_ok_for_buy_only() -> None:
    cfg = MarketEntryLiquidityConfig(
        max_spread_bps=20,
        max_estimated_slippage_bps=20,
        min_top_depth_usdt=6000,
        min_quote_volume_24h=1000,
        notional_usdt=2000,
    )
    item = build_liquidity_item(
        symbol="AAAUSDT",
        last_price=100.0,
        quote_volume_24h=5_000_000.0,
        book={"bidPrice": "99.95", "askPrice": "100.05"},
        depth={
            "bids": [["99.95", "5"]],
            "asks": [["100.05", "30"]],
        },
        cfg=cfg,
    )
    assert item.liquidity_ok_for_market_entry is False
    assert item.buy_liquidity_ok_for_market_entry is True
    assert item.sell_liquidity_ok_for_market_entry is False
    assert item.buy_reason_codes == []
    assert "depth_not_enough_for_notional" in item.sell_reason_codes


def test_document_roundtrip(tmp_path: Path) -> None:
    cfg = MarketEntryLiquidityConfig(min_top_depth_usdt=1000, min_quote_volume_24h=1000, notional_usdt=500)
    doc = build_market_entry_liquidity_document(
        light=_light_doc(),
        book_by_symbol={"AAAUSDT": {"bidPrice": "99.95", "askPrice": "100.05"}},
        depth_by_symbol={"AAAUSDT": {"bids": [["99.95", "20"]], "asks": [["100.05", "20"]]}},
        cfg=cfg,
        generated_at="2026-01-01T00:01:00Z",
    )
    p = tmp_path / "liq.json"
    p.write_text(json.dumps(doc.model_dump(mode="json")), encoding="utf-8")
    loaded = MarketEntryLiquidityDocument.model_validate(json.loads(p.read_text(encoding="utf-8")))
    assert loaded.count == 1
    assert loaded.items[0].symbol == "AAAUSDT"


def test_step16_cli_parse() -> None:
    parser = _build_parser()
    args = parser.parse_args(["fetch-market-entry-liquidity", "--symbols", "BTCUSDT,ETHUSDT", "--stdout-json"])
    assert args.command == "fetch-market-entry-liquidity"
    assert args.symbols == "BTCUSDT,ETHUSDT"
    assert args.stdout_json is True
