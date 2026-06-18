"""Load WSEventEnvelope list from JSON (fixture mode). docs/STEP3.8_任务卡.md."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.micro.normalized_models import NormalizedBook, NormalizedTrade
from laoma_signal_engine.micro.ws.subscription_manager import WSEventEnvelope


def load_fixture_envelopes(path: Path) -> list[WSEventEnvelope]:
    """JSON root: list of dicts (WSEventEnvelope fields, normalized: trade|book dict)."""
    raw_any: Any = read_json_object(path)
    if not isinstance(raw_any, list):
        msg = "fixture must be a JSON array"
        raise ValueError(msg)
    out: list[WSEventEnvelope] = []
    for item in raw_any:
        if not isinstance(item, dict):
            msg = "each fixture entry must be an object"
            raise ValueError(msg)
        out.append(_envelope_from_dict(item))
    return out


def load_fixture_envelopes_from_text(text: str) -> list[WSEventEnvelope]:
    raw_any = json.loads(text)
    if not isinstance(raw_any, list):
        msg = "fixture must be a JSON array"
        raise ValueError(msg)
    return [_envelope_from_dict(item) for item in raw_any if isinstance(item, dict)]


def _envelope_from_dict(d: dict[str, Any]) -> WSEventEnvelope:
    sym = str(d.get("symbol", "")).strip().upper()
    stream_type = str(d.get("stream_type", ""))
    ev = d.get("event_ts_ms")
    rv = d.get("recv_ts_ms")
    if not isinstance(ev, int) or not isinstance(rv, int):
        msg = "event_ts_ms and recv_ts_ms must be int"
        raise ValueError(msg)
    norm_raw = d.get("normalized")
    if not isinstance(norm_raw, dict):
        msg = "normalized must be an object"
        raise ValueError(msg)
    kind = str(norm_raw.get("type", ""))
    normalized: NormalizedTrade | NormalizedBook
    if kind == "trade":
        normalized = _trade_from_dict(norm_raw)
    elif kind == "book":
        normalized = _book_from_dict(norm_raw)
    else:
        msg = f"normalized.type must be trade|book, got {kind!r}"
        raise ValueError(msg)
    return WSEventEnvelope(
        symbol=sym,
        stream_type=stream_type,
        event_ts_ms=ev,
        recv_ts_ms=rv,
        normalized=normalized,
    )


def _trade_from_dict(d: dict[str, Any]) -> NormalizedTrade:
    side_raw = str(d.get("side", ""))
    side: Literal["buy", "sell"]
    if side_raw == "buy":
        side = "buy"
    elif side_raw == "sell":
        side = "sell"
    else:
        msg = "trade side must be buy|sell"
        raise ValueError(msg)
    return NormalizedTrade(
        symbol=str(d.get("symbol", "")).strip().upper(),
        ts_ms=int(d["ts_ms"]),
        price=float(d["price"]),
        qty=float(d["qty"]),
        side=side,
    )


def _book_from_dict(d: dict[str, Any]) -> NormalizedBook:
    bids_raw = d.get("bids")
    asks_raw = d.get("asks")
    if not isinstance(bids_raw, list) or not isinstance(asks_raw, list):
        msg = "bids and asks must be arrays"
        raise ValueError(msg)
    bids: list[tuple[float, float]] = []
    for row in bids_raw:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            bids.append((float(row[0]), float(row[1])))
    asks: list[tuple[float, float]] = []
    for row in asks_raw:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            asks.append((float(row[0]), float(row[1])))
    return NormalizedBook(
        symbol=str(d.get("symbol", "")).strip().upper(),
        ts_ms=int(d["ts_ms"]),
        bids=bids,
        asks=asks,
        levels=int(d["levels"]),
    )
