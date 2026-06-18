"""Basis context from premiumIndex row (STEP4.1)."""

from __future__ import annotations

from typing import Any

from laoma_signal_engine.context.constants import BASIS_EXTREME_ABS_BPS
from laoma_signal_engine.factors.models import Basis15mBlock, BasisState


def _basis_state(bps: float) -> BasisState:
    ax = abs(bps)
    if ax < 5:
        return "NEUTRAL"
    if bps > 0:
        return "PREMIUM_WIDE" if ax >= 20 else "PREMIUM_MILD"
    return "DISCOUNT_WIDE" if ax >= 20 else "DISCOUNT_MILD"


def build_basis_15m_from_premium_row(row: dict[str, Any] | None) -> Basis15mBlock:
    if not row:
        return Basis15mBlock(ready=False, reason="missing_premium_row")

    mp = row.get("markPrice")
    ip = row.get("indexPrice")
    if mp is None or ip is None:
        return Basis15mBlock(ready=False, reason="missing_mark_or_index")

    try:
        mark_f = float(mp)
        idx_f = float(ip)
    except (TypeError, ValueError):
        return Basis15mBlock(ready=False, reason="invalid_prices")

    if idx_f == 0:
        return Basis15mBlock(ready=False, reason="index_price_zero")

    mib = (mark_f - idx_f) / idx_f * 10000.0
    st = _basis_state(mib)
    extreme = abs(mib) >= BASIS_EXTREME_ABS_BPS

    return Basis15mBlock(
        ready=True,
        reason="ok",
        spot_perp_basis_bps=None,
        mark_index_basis_bps=round(mib, 6),
        basis_change_bps=None,
        basis_state=st,
        basis_extreme=extreme,
    )
