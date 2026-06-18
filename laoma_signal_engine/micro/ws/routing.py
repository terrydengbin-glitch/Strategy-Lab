"""STEP3.8B stream routing. docs/STEP3.8B_Real_Binance_WS_任务卡.md."""

from __future__ import annotations

LOGICAL_STREAM_TO_ROUTE: dict[str, str] = {
    "aggTrade": "market",
    "bookTicker": "public",
    "partialDepth5": "public",
}

LOGICAL_STREAM_TO_TEMPLATE: dict[str, str] = {
    "aggTrade": "{symbol_lower}@aggTrade",
    "bookTicker": "{symbol_lower}@bookTicker",
    "partialDepth5": "{symbol_lower}@depth5@100ms",
}

MAX_STREAMS_PER_CONNECTION_HARD_CAP: int = 1024


def binance_stream_name(symbol_upper: str, logical: str) -> str:
    if logical not in LOGICAL_STREAM_TO_TEMPLATE:
        msg = f"unknown logical stream {logical!r}"
        raise KeyError(msg)
    sl = symbol_upper.strip().upper().lower()
    return LOGICAL_STREAM_TO_TEMPLATE[logical].format(symbol_lower=sl)


def parse_binance_combined_stream(binance_stream: str) -> tuple[str, str] | None:
    """Return (symbol_upper, logical stream_type) or None."""
    s = binance_stream.strip()
    low = s.lower()
    if low.endswith("@aggtrade"):
        return low[: -len("@aggtrade")].upper(), "aggTrade"
    if low.endswith("@bookticker"):
        return low[: -len("@bookticker")].upper(), "bookTicker"
    if low.endswith("@depth5@100ms"):
        return low[: -len("@depth5@100ms")].upper(), "partialDepth5"
    return None


def stream_binance_route(stream_name: str) -> str | None:
    """Return 'market'|'public' or None if unknown."""
    p = parse_binance_combined_stream(stream_name)
    if p is None:
        return None
    _sym, logical = p
    return LOGICAL_STREAM_TO_ROUTE.get(logical)


def http_path_for_route(route: str, *, public_path: str, market_path: str) -> str:
    if route == "public":
        return public_path.rstrip("/") or "/public"
    if route == "market":
        return market_path.rstrip("/") or "/market"
    msg = f"unknown route {route!r}"
    raise ValueError(msg)


def combined_stream_ws_url(
    base_url: str,
    route: str,
    streams: list[str],
    *,
    public_path: str = "/public",
    market_path: str = "/market",
) -> str:
    if not streams:
        msg = "combined_stream_ws_url requires non-empty streams"
        raise ValueError(msg)
    root = base_url.strip().rstrip("/")
    path = http_path_for_route(route, public_path=public_path, market_path=market_path)
    ordered = "/".join(sorted(streams))
    return f"{root}{path}/stream?streams={ordered}"


def partition_sorted_streams(streams: list[str], limit: int) -> list[list[str]]:
    if limit <= 0:
        msg = "per_connection_stream_limit must be positive"
        raise ValueError(msg)
    if limit > MAX_STREAMS_PER_CONNECTION_HARD_CAP:
        msg = "per_connection_stream_limit exceeds Binance hard cap 1024"
        raise ValueError(msg)
    ordered = sorted(streams)
    return [ordered[i : i + limit] for i in range(0, len(ordered), limit)]


def group_streams_by_route(
    streams: list[str],
) -> tuple[dict[str, list[str]], list[str]]:
    """Split streams into public/market buckets; unknown names go to invalid list."""
    invalid: list[str] = []
    out: dict[str, list[str]] = {"public": [], "market": []}
    for s in streams:
        r = stream_binance_route(s)
        if r is None:
            invalid.append(s)
            continue
        out[r].append(s)
    out["public"] = sorted(out["public"])
    out["market"] = sorted(out["market"])
    return out, invalid
