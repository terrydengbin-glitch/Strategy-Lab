"""1m candle provider helpers for paper matching."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import httpx

from laoma_signal_engine.paper.models import Candle


FUTURES_REST = "https://fapi.binance.com"


def candle_from_binance_row(symbol: str, row: list[Any]) -> Candle:
    return Candle(
        symbol=symbol.upper(),
        open_time_ms=int(row[0]),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]) if len(row) > 5 else 0.0,
    )


def fetch_binance_1m_candles(symbol: str, *, limit: int = 5, client: httpx.Client | None = None) -> list[Candle]:
    close_client = client is None
    got = client or httpx.Client(timeout=10.0)
    try:
        resp = got.get(
            f"{FUTURES_REST}/fapi/v1/klines",
            params={"symbol": symbol.upper(), "interval": "1m", "limit": int(limit)},
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return []
        return [candle_from_binance_row(symbol, row) for row in data]
    finally:
        if close_client:
            got.close()


class StaticCandleProvider:
    """Deterministic provider used by tests and manual smoke runs."""

    def __init__(self, candles_by_symbol: dict[str, Iterable[Candle]]) -> None:
        self._candles_by_symbol = {k.upper(): list(v) for k, v in candles_by_symbol.items()}

    def get_1m(self, symbol: str, *, limit: int = 5) -> list[Candle]:
        return self._candles_by_symbol.get(symbol.upper(), [])[-limit:]


class BinanceCandleProvider:
    def get_1m(self, symbol: str, *, limit: int = 5) -> list[Candle]:
        return fetch_binance_1m_candles(symbol, limit=limit)

