"""Spot USDT TRADING universe from exchangeInfo (T06)."""

from __future__ import annotations

from typing import Any


def spot_usdt_trading_by_base(exchange_info: dict[str, Any]) -> dict[str, str]:
    """Map base_asset -> symbol (e.g. BTC -> BTCUSDT) for USDT spot pairs in TRADING."""
    symbols = exchange_info.get("symbols")
    if not isinstance(symbols, list):
        return {}
    out: dict[str, str] = {}
    for row in symbols:
        if not isinstance(row, dict):
            continue
        if row.get("status") != "TRADING":
            continue
        if row.get("quoteAsset") != "USDT":
            continue
        base = row.get("baseAsset")
        sym = row.get("symbol")
        if not isinstance(base, str) or not isinstance(sym, str):
            continue
        out[base.upper()] = sym.upper()
    return out
