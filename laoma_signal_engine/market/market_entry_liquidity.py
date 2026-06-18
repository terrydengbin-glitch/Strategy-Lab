"""STEP1.6 market-entry liquidity snapshot."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import orjson

from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.core.config_loader import package_root
from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.market.light_snapshot_models import FuturesLightSnapshotDocument
from laoma_signal_engine.market.market_entry_liquidity_models import (
    MarketEntryLiquidityDocument,
    MarketEntryLiquidityItem,
)

log = logging.getLogger(__name__)

FUTURES_REST = "https://fapi.binance.com"


@dataclass(frozen=True)
class MarketEntryLiquidityConfig:
    max_spread_bps: float = 8.0
    max_estimated_slippage_bps: float = 15.0
    min_top_depth_usdt: float = 20_000.0
    min_quote_volume_24h: float = 3_000_000.0
    depth_limit: int = 5
    margin_usdt: float = 100.0
    leverage: float = 20.0
    notional_usdt: float = 2_000.0


def load_market_entry_liquidity_config() -> MarketEntryLiquidityConfig:
    import yaml

    path = package_root() / "config" / "default.yaml"
    try:
        doc: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except OSError:
        return MarketEntryLiquidityConfig()
    raw = doc.get("market_entry_liquidity") or {}
    return MarketEntryLiquidityConfig(
        max_spread_bps=float(raw.get("max_spread_bps", 8.0)),
        max_estimated_slippage_bps=float(raw.get("max_estimated_slippage_bps", 15.0)),
        min_top_depth_usdt=float(raw.get("min_top_depth_usdt", 20_000.0)),
        min_quote_volume_24h=float(raw.get("min_quote_volume_24h", 3_000_000.0)),
        depth_limit=int(raw.get("depth_limit", 5)),
        margin_usdt=float(raw.get("margin_usdt", 100.0)),
        leverage=float(raw.get("leverage", 20.0)),
        notional_usdt=float(raw.get("notional_usdt", 2_000.0)),
    )


def fetch_book_ticker(client: httpx.Client, symbol: str) -> dict[str, Any]:
    r = client.get(f"{FUTURES_REST}/fapi/v1/ticker/bookTicker", params={"symbol": symbol}, timeout=10.0)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise TypeError("bookTicker response must be object")
    return data


def fetch_depth(client: httpx.Client, symbol: str, *, limit: int = 5) -> dict[str, Any]:
    r = client.get(f"{FUTURES_REST}/fapi/v1/depth", params={"symbol": symbol, "limit": limit}, timeout=10.0)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise TypeError("depth response must be object")
    return data


def _f(raw: Any) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _levels_usdt(levels: Any) -> float | None:
    if not isinstance(levels, list):
        return None
    total = 0.0
    ok = False
    for row in levels:
        if not isinstance(row, list | tuple) or len(row) < 2:
            continue
        px = _f(row[0])
        qty = _f(row[1])
        if px is None or qty is None:
            continue
        total += px * qty
        ok = True
    return total if ok else None


def _slippage_bps(levels: Any, *, side: str, notional_usdt: float, mid: float) -> float | None:
    if not isinstance(levels, list) or mid <= 0 or notional_usdt <= 0:
        return None
    remaining = notional_usdt
    qty_total = 0.0
    cost_total = 0.0
    for row in levels:
        if not isinstance(row, list | tuple) or len(row) < 2:
            continue
        px = _f(row[0])
        qty = _f(row[1])
        if px is None or qty is None:
            continue
        level_notional = px * qty
        take_notional = min(remaining, level_notional)
        if take_notional <= 0:
            continue
        take_qty = take_notional / px
        qty_total += take_qty
        cost_total += take_notional
        remaining -= take_notional
        if remaining <= 1e-9:
            break
    if remaining > 1e-9 or qty_total <= 0:
        return None
    avg_px = cost_total / qty_total
    if side == "buy":
        return max(0.0, (avg_px - mid) / mid * 10_000.0)
    return max(0.0, (mid - avg_px) / mid * 10_000.0)


def build_liquidity_item(
    *,
    symbol: str,
    last_price: float | None,
    quote_volume_24h: float | None,
    book: dict[str, Any] | None,
    depth: dict[str, Any] | None,
    cfg: MarketEntryLiquidityConfig,
) -> MarketEntryLiquidityItem:
    reasons: list[str] = []
    bid = _f(book.get("bidPrice")) if book else None
    ask = _f(book.get("askPrice")) if book else None
    mid = ((bid + ask) / 2.0) if bid is not None and ask is not None else last_price
    spread = ((ask - bid) / mid * 10_000.0) if bid is not None and ask is not None and mid and mid > 0 else None
    bids = depth.get("bids") if depth else None
    asks = depth.get("asks") if depth else None
    bid_depth = _levels_usdt(bids)
    ask_depth = _levels_usdt(asks)
    buy_slip = _slippage_bps(asks, side="buy", notional_usdt=cfg.notional_usdt, mid=mid or 0.0)
    sell_slip = _slippage_bps(bids, side="sell", notional_usdt=cfg.notional_usdt, mid=mid or 0.0)

    shared_reasons: list[str] = []
    buy_reasons: list[str] = []
    sell_reasons: list[str] = []
    if spread is None:
        reasons.append("spread_missing")
        shared_reasons.append("spread_missing")
    elif spread > cfg.max_spread_bps:
        reasons.append("spread_too_wide")
        shared_reasons.append("spread_too_wide")
    if bid_depth is None or ask_depth is None:
        reasons.append("depth_missing")
    if bid_depth is None:
        sell_reasons.append("depth_missing")
    else:
        if bid_depth < cfg.min_top_depth_usdt:
            reasons.append("bid_depth_too_thin")
        if bid_depth < cfg.notional_usdt:
            sell_reasons.append("depth_not_enough_for_notional")
    if ask_depth is None:
        buy_reasons.append("depth_missing")
    else:
        if ask_depth < cfg.min_top_depth_usdt:
            reasons.append("ask_depth_too_thin")
        if ask_depth < cfg.notional_usdt:
            buy_reasons.append("depth_not_enough_for_notional")
    if buy_slip is None or sell_slip is None:
        reasons.append("slippage_missing")
        if buy_slip is None:
            buy_reasons.append("slippage_missing")
        if sell_slip is None:
            sell_reasons.append("slippage_missing")
    else:
        if buy_slip > cfg.max_estimated_slippage_bps:
            reasons.append("buy_slippage_too_high")
        if buy_slip > cfg.max_estimated_slippage_bps:
            buy_reasons.append("slippage_too_high")
        if sell_slip > cfg.max_estimated_slippage_bps:
            reasons.append("sell_slippage_too_high")
        if sell_slip > cfg.max_estimated_slippage_bps:
            sell_reasons.append("slippage_too_high")
    if quote_volume_24h is None:
        reasons.append("quote_volume_missing")
        shared_reasons.append("quote_volume_missing")
    elif quote_volume_24h < cfg.min_quote_volume_24h:
        reasons.append("quote_volume_too_low")
        shared_reasons.append("quote_volume_too_low")

    buy_reason_codes = list(dict.fromkeys([*shared_reasons, *buy_reasons]))
    sell_reason_codes = list(dict.fromkeys([*shared_reasons, *sell_reasons]))
    buy_ok = len(buy_reason_codes) == 0
    sell_ok = len(sell_reason_codes) == 0

    return MarketEntryLiquidityItem(
        symbol=symbol,
        last_price=last_price,
        bid_price=bid,
        ask_price=ask,
        spread_bps=spread,
        top_bid_depth_usdt=bid_depth,
        top_ask_depth_usdt=ask_depth,
        estimated_market_buy_slippage_bps=buy_slip,
        estimated_market_sell_slippage_bps=sell_slip,
        liquidity_ok_for_market_entry=buy_ok and sell_ok,
        buy_liquidity_ok_for_market_entry=buy_ok,
        sell_liquidity_ok_for_market_entry=sell_ok,
        notional_usdt=cfg.notional_usdt,
        max_spread_bps=cfg.max_spread_bps,
        max_estimated_slippage_bps=cfg.max_estimated_slippage_bps,
        min_top_depth_usdt=cfg.min_top_depth_usdt,
        min_quote_volume_24h=cfg.min_quote_volume_24h,
        buy_reason_codes=buy_reason_codes,
        sell_reason_codes=sell_reason_codes,
        reason_codes=list(dict.fromkeys(reasons)),
    )


def build_market_entry_liquidity_document(
    *,
    light: FuturesLightSnapshotDocument,
    book_by_symbol: dict[str, dict[str, Any]],
    depth_by_symbol: dict[str, dict[str, Any]],
    cfg: MarketEntryLiquidityConfig,
    generated_at: str | None = None,
    symbols: list[str] | None = None,
) -> MarketEntryLiquidityDocument:
    wanted = {s.strip().upper() for s in symbols or [] if s.strip()} if symbols else None
    items: list[MarketEntryLiquidityItem] = []
    for row in light.items:
        sym = row.symbol.upper()
        if wanted is not None and sym not in wanted:
            continue
        qv = row.background.quote_volume_24h
        items.append(
            build_liquidity_item(
                symbol=sym,
                last_price=row.last_price,
                quote_volume_24h=qv,
                book=book_by_symbol.get(sym),
                depth=depth_by_symbol.get(sym),
                cfg=cfg,
            )
        )
    if not items:
        status = "no_symbols"
    elif all(it.liquidity_ok_for_market_entry for it in items):
        status = "ok"
    elif any(it.liquidity_ok_for_market_entry for it in items):
        status = "partial"
    else:
        status = "partial"
    return MarketEntryLiquidityDocument(
        generated_at=generated_at or to_iso_z(utc_now()),
        status=status,
        count=len(items),
        max_spread_bps=cfg.max_spread_bps,
        max_estimated_slippage_bps=cfg.max_estimated_slippage_bps,
        min_top_depth_usdt=cfg.min_top_depth_usdt,
        min_quote_volume_24h=cfg.min_quote_volume_24h,
        margin_usdt=cfg.margin_usdt,
        leverage=cfg.leverage,
        notional_usdt=cfg.notional_usdt,
        items=items,
    )


def _write_doc(path: Path, doc: MarketEntryLiquidityDocument) -> None:
    data = orjson.dumps(doc.model_dump(mode="json"), option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
    write_file_atomic(path, data)


def run_fetch_market_entry_liquidity_safe(
    *,
    project_root: Path | None = None,
    light_path: Path | None = None,
    output_path: Path | None = None,
    symbols: list[str] | None = None,
    stdout_json: bool = False,
) -> int:
    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    lp = light_path.resolve() if light_path else (pr / "DATA" / "market" / "futures_light_snapshot.json").resolve()
    out = output_path.resolve() if output_path else (pr / "DATA" / "market" / "latest_market_entry_liquidity.json").resolve()
    cfg = load_market_entry_liquidity_config()
    try:
        light = FuturesLightSnapshotDocument.model_validate(read_json_object(lp))
    except Exception as exc:
        log.error("market entry liquidity load light failed: %s", exc)
        return EXIT_CONFIG

    wanted = [s.strip().upper() for s in symbols or [] if s.strip()] if symbols else [it.symbol for it in light.items]
    book_by: dict[str, dict[str, Any]] = {}
    depth_by: dict[str, dict[str, Any]] = {}
    try:
        with httpx.Client() as client:
            for sym in wanted:
                try:
                    book_by[sym] = fetch_book_ticker(client, sym)
                    depth_by[sym] = fetch_depth(client, sym, limit=cfg.depth_limit)
                except Exception as exc:
                    log.warning("market entry liquidity fetch failed %s: %s", sym, exc)
    except Exception as exc:
        log.error("market entry liquidity http client failed: %s", exc)
        return EXIT_INTERNAL

    doc = build_market_entry_liquidity_document(
        light=light,
        book_by_symbol=book_by,
        depth_by_symbol=depth_by,
        cfg=cfg,
        symbols=wanted,
    )
    try:
        _write_doc(out, doc)
    except OSError as exc:
        log.error("market entry liquidity write failed: %s", exc)
        return EXIT_CONFIG
    if stdout_json:
        summary = {
            "schema_version": doc.schema_version,
            "source": doc.source,
            "status": doc.status,
            "count": doc.count,
            "ok_count": sum(1 for it in doc.items if it.liquidity_ok_for_market_entry),
            "output_file": str(out.relative_to(pr)) if out.is_relative_to(pr) else str(out),
        }
        sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))
        sys.stdout.buffer.flush()
    log.info("market entry liquidity status=%s count=%s out=%s", doc.status, doc.count, out)
    return EXIT_SUCCESS
