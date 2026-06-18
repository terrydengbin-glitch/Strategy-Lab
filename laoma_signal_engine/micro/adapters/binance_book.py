"""Binance bookTicker / partialDepth5 -> NormalizedBook. docs/STEP3.1_任务卡.md section 5.4."""

from __future__ import annotations

from typing import Any

from laoma_signal_engine.micro.adapters.binance_common import normalize_binance_symbol
from laoma_signal_engine.micro.normalized_models import NormalizedBook


def _parse_levels(side_rows: object, need: int) -> list[tuple[float, float]]:
    if not isinstance(side_rows, list):
        msg = "depth bids/asks must be a list"
        raise ValueError(msg)
    if len(side_rows) < need:
        msg = "depth insufficient rows"
        raise ValueError(msg)
    out: list[tuple[float, float]] = []
    for row in side_rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            msg = "depth row must be [price, qty]"
            raise ValueError(msg)
        try:
            out.append((float(row[0]), float(row[1])))
        except (TypeError, ValueError) as e:
            msg = "depth price/qty must be float-convertible"
            raise ValueError(msg) from e
    return out


def _sort_bids_asks(
    bids: list[tuple[float, float]], asks: list[tuple[float, float]], keep: int
) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    sb = sorted(bids, key=lambda x: x[0], reverse=True)
    sa = sorted(asks, key=lambda x: x[0])
    if len(sb) < keep or len(sa) < keep:
        msg = "insufficient levels after sort"
        raise ValueError(msg)
    return sb[:keep], sa[:keep]


def normalize_book_ticker(event: dict[str, Any]) -> NormalizedBook:
    if not isinstance(event, dict):
        msg = "bookTicker event must be a dict"
        raise TypeError(msg)

    sym_raw = event.get("s")
    if sym_raw is None:
        msg = "bookTicker missing s"
        raise ValueError(msg)
    symbol = normalize_binance_symbol(sym_raw)

    if "E" not in event or event["E"] is None:
        msg = "bookTicker missing E"
        raise ValueError(msg)
    try:
        ts_ms = int(event["E"])
    except (TypeError, ValueError) as e:
        msg = "bookTicker E must be int-convertible"
        raise ValueError(msg) from e

    try:
        bids = [(float(event["b"]), float(event["B"]))]
        asks = [(float(event["a"]), float(event["A"]))]
    except KeyError as e:
        missing = e.args[0]
        msg = f"bookTicker missing {missing}"
        raise ValueError(msg) from e
    except (TypeError, ValueError) as e:
        msg = "bookTicker b/B/a/A must be float-convertible"
        raise ValueError(msg) from e

    bids2, asks2 = _sort_bids_asks(bids, asks, 1)
    return NormalizedBook(
        symbol=symbol, ts_ms=ts_ms, bids=bids2, asks=asks2, levels=1
    )


def normalize_partial_depth5(
    event: dict[str, Any],
    symbol_if_missing: str | None = None,
) -> NormalizedBook:
    if not isinstance(event, dict):
        msg = "partialDepth event must be a dict"
        raise TypeError(msg)

    sym_raw = event.get("s")
    if sym_raw is None:
        if symbol_if_missing is None:
            msg = "partialDepth missing s and symbol_if_missing"
            raise ValueError(msg)
        symbol = normalize_binance_symbol(symbol_if_missing)
    else:
        symbol = normalize_binance_symbol(sym_raw)

    if "E" not in event or event["E"] is None:
        msg = "partialDepth missing E"
        raise ValueError(msg)
    try:
        ts_ms = int(event["E"])
    except (TypeError, ValueError) as e:
        msg = "partialDepth E must be int-convertible"
        raise ValueError(msg) from e

    braw = event.get("b")
    araw = event.get("a")
    bids = _parse_levels(braw, 5)
    asks = _parse_levels(araw, 5)
    bids2, asks2 = _sort_bids_asks(bids, asks, 5)
    return NormalizedBook(
        symbol=symbol, ts_ms=ts_ms, bids=bids2, asks=asks2, levels=5
    )
