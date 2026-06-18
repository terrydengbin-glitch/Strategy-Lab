"""Open interest 15m block (STEP4.1)."""

from __future__ import annotations

import statistics
from typing import Any

from laoma_signal_engine.context.binance_context_client import BinanceFuturesContextClient
from laoma_signal_engine.context.constants import OI_PCT_EPS, PRIMARY_PRICE_NEUTRAL_EPS
from laoma_signal_engine.factors.models import OI15mBlock, OIQuadrant


def _price_up(primary_15m: dict[str, Any]) -> bool:
    pr = primary_15m.get("price_ret")
    if isinstance(pr, (int, float)) and float(pr) > PRIMARY_PRICE_NEUTRAL_EPS:
        return True
    return False


def _quadrant(price_up: bool, oi_up: bool) -> OIQuadrant:
    if price_up and oi_up:
        return "Q1"
    if price_up and not oi_up:
        return "Q2"
    if not price_up and oi_up:
        return "Q4"
    return "Q3"


def _state_label(q: OIQuadrant) -> str:
    return {
        "Q1": "price_up_oi_up_new_positions",
        "Q2": "price_up_oi_down_short_covering",
        "Q3": "price_down_oi_down_long_delever",
        "Q4": "price_down_oi_up_new_shorts",
        "unknown": "unknown",
    }[q]


def _compute_z_percentile(values: list[float]) -> tuple[float | None, float | None]:
    if len(values) < 2:
        return None, None
    latest = values[-1]
    history = values[:-1]
    mean = statistics.fmean(history)
    stdev = statistics.pstdev(history) if len(history) >= 2 else 0.0
    z = None if stdev <= 0 else round((latest - mean) / stdev, 4)
    pct = round(sum(1 for v in values if v <= latest) / len(values), 4)
    return z, pct


def build_oi_15m_block(
    symbol: str,
    primary_15m: dict[str, Any],
    move_side: str,
    client: BinanceFuturesContextClient,
) -> OI15mBlock:
    sym = symbol.upper().strip()
    ms = move_side.strip().lower()
    try:
        cur = client.get_json("/fapi/v1/openInterest", {"symbol": sym})
        if not isinstance(cur, dict):
            return OI15mBlock(ready=False, reason="open_interest_bad_shape")

        hist = client.get_json(
            "/futures/data/openInterestHist",
            {"symbol": sym, "period": "15m", "limit": 16},
        )
        if not isinstance(hist, list) or len(hist) < 2:
            return OI15mBlock(ready=False, reason="open_interest_hist_short")

        oi_vals: list[float] = []
        for row in hist:
            if not isinstance(row, dict):
                continue
            s = row.get("sumOpenInterest")
            if s is None:
                continue
            try:
                oi_vals.append(float(s))
            except (TypeError, ValueError):
                continue

        if len(oi_vals) < 2:
            return OI15mBlock(ready=False, reason="open_interest_hist_parse")

        first = oi_vals[0]
        last = oi_vals[-1]
        denom = max(abs(first), 1.0)
        pct_ch = (last - first) / denom
        oi_flat = -OI_PCT_EPS <= pct_ch <= OI_PCT_EPS
        oi_up = pct_ch > OI_PCT_EPS

        pup = _price_up(primary_15m)
        if oi_flat:
            quad = _quadrant(pup, False)
        else:
            quad = _quadrant(pup, oi_up)

        z, pct = _compute_z_percentile(oi_vals)

        conflict = False
        if ms == "up" and quad == "Q2":
            conflict = True
        if ms == "down" and quad == "Q3":
            conflict = True

        return OI15mBlock(
            ready=True,
            reason="ok",
            oi_pct_change=round(pct_ch, 6),
            oi_z=z,
            oi_percentile=pct,
            oi_quadrant=quad,
            oi_state=_state_label(quad),
            oi_conflict=conflict,
        )
    except Exception as exc:
        return OI15mBlock(ready=False, reason=f"oi_fetch_error:{type(exc).__name__}")
