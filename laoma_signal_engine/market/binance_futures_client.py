"""Binance USDT-M Futures REST: exchangeInfo + 24h ticker."""

from __future__ import annotations

from typing import Any

import httpx

from laoma_signal_engine.market.rest_support import get_json

FUTURES_REST = "https://fapi.binance.com"


def fetch_exchange_info(client: httpx.Client) -> dict[str, Any]:
    return get_json(client, f"{FUTURES_REST}/fapi/v1/exchangeInfo")


def fetch_ticker_24h_all(client: httpx.Client) -> list[dict[str, Any]]:
    data = get_json(client, f"{FUTURES_REST}/fapi/v1/ticker/24hr")
    if not isinstance(data, list):
        raise TypeError("expected list from futures ticker/24hr")
    return data
