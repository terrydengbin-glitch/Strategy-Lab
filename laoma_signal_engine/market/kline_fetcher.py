"""Binance USDT-M futures klines (HTTP)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from laoma_signal_engine.market.rest_support import _should_retry

FUTURES_REST = "https://fapi.binance.com"


@dataclass(frozen=True)
class KlineBar:
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time_ms: int
    quote_volume: float
    taker_buy_base: float


def _row_to_bar(row: list[Any]) -> KlineBar:
    return KlineBar(
        open_time_ms=int(row[0]),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        close_time_ms=int(row[6]),
        quote_volume=float(row[7]),
        taker_buy_base=float(row[9]) if len(row) > 9 else 0.0,
    )


@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=12),
    retry=retry_if_exception(_should_retry),
)
def fetch_klines(
    client: httpx.Client,
    symbol: str,
    interval: str,
    limit: int,
) -> list[KlineBar]:
    url = f"{FUTURES_REST}/fapi/v1/klines"
    r = client.get(url, params={"symbol": symbol, "interval": interval, "limit": limit})
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise TypeError("klines response must be a list")
    return [_row_to_bar(row) for row in data if isinstance(row, list) and len(row) >= 10]


@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=12),
    retry=retry_if_exception(_should_retry),
)
def fetch_ticker_24h_all(client: httpx.Client) -> list[dict[str, Any]]:
    url = f"{FUTURES_REST}/fapi/v1/ticker/24hr"
    r = client.get(url)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise TypeError("ticker/24hr response must be a list")
    return [x for x in data if isinstance(x, dict)]


def ticker_by_symbol_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        sym = row.get("symbol")
        if isinstance(sym, str):
            out[sym.upper()] = row
    return out


def parse_klines_response(data: Any) -> list[KlineBar]:
    if not isinstance(data, list):
        raise TypeError("klines response must be a list")
    return [_row_to_bar(row) for row in data if isinstance(row, list) and len(row) >= 10]


async def fetch_klines_async(
    client: httpx.AsyncClient,
    symbol: str,
    interval: str,
    limit: int,
) -> list[KlineBar]:
    url = f"{FUTURES_REST}/fapi/v1/klines"
    r = await client.get(url, params={"symbol": symbol, "interval": interval, "limit": limit})
    r.raise_for_status()
    return parse_klines_response(r.json())


async def fetch_ticker_24h_all_async(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    url = f"{FUTURES_REST}/fapi/v1/ticker/24hr"
    r = await client.get(url)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise TypeError("ticker/24hr response must be a list")
    return [x for x in data if isinstance(x, dict)]


def request_weight_limit_1m_from_exchange_info(data: Any) -> int | None:
    """Parse REQUEST_WEIGHT per minute from /fapi/v1/exchangeInfo JSON."""
    if not isinstance(data, dict):
        return None
    rows = data.get("rateLimits")
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("rateLimitType", "")).upper() != "REQUEST_WEIGHT":
            continue
        if str(row.get("interval", "")).upper() != "MINUTE":
            continue
        if int(row.get("intervalNum", 0)) != 1:
            continue
        lim = row.get("limit")
        if lim is not None:
            try:
                return int(lim)
            except (TypeError, ValueError):
                return None
    return None


async def fetch_request_weight_limit_1m_async(client: httpx.AsyncClient) -> int | None:
    url = f"{FUTURES_REST}/fapi/v1/exchangeInfo"
    r = await client.get(url)
    r.raise_for_status()
    return request_weight_limit_1m_from_exchange_info(r.json())
