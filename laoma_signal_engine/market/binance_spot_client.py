"""Binance Spot REST: exchangeInfo + 24h ticker."""

from __future__ import annotations

from typing import Any

import httpx

from laoma_signal_engine.market.rest_support import get_json

SPOT_REST = "https://api.binance.com"


def fetch_exchange_info(client: httpx.Client) -> dict[str, Any]:
    return get_json(client, f"{SPOT_REST}/api/v3/exchangeInfo")


def fetch_ticker_24h_all(client: httpx.Client) -> list[dict[str, Any]]:
    data = get_json(client, f"{SPOT_REST}/api/v3/ticker/24hr")
    if not isinstance(data, list):
        raise TypeError("expected list from spot ticker/24hr")
    return data
