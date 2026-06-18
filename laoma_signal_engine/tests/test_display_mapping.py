"""Display / cashtag mapping for multiplier-style base assets."""

from __future__ import annotations

from laoma_signal_engine.universe.display_mapping import (
    cashtag_from_display,
    display_base_asset_from_internal,
    spot_cashtag_symbol_from_display,
)


def test_strip_1000_prefix() -> None:
    assert display_base_asset_from_internal("1000PEPE") == "PEPE"
    assert display_base_asset_from_internal("1000BONK") == "BONK"
    assert display_base_asset_from_internal("1000SHIB") == "SHIB"


def test_no_prefix_btc() -> None:
    assert display_base_asset_from_internal("BTC") == "BTC"


def test_cashtag_helpers() -> None:
    display = display_base_asset_from_internal("1000PEPE")
    assert cashtag_from_display(display) == "$PEPE"
    assert spot_cashtag_symbol_from_display(display) == "PEPEUSDT"
