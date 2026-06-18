"""Fee and slippage model for paper fills."""

from __future__ import annotations

from laoma_signal_engine.paper.models import FillCost


def adverse_fill_price(reference_price: float, side: str, action: str, slippage_bps: float) -> float:
    """Return a conservative fill price for market-like paper fills."""
    if reference_price <= 0:
        return reference_price
    side = side.upper()
    action = action.lower()
    bps = slippage_bps / 10_000
    entry_long_or_exit_short = (action == "entry" and side == "LONG") or (action != "entry" and side == "SHORT")
    if entry_long_or_exit_short:
        return reference_price * (1 + bps)
    return reference_price * (1 - bps)


def build_fill_cost(
    *,
    reference_price: float,
    fill_price: float,
    side: str,
    action: str,
    quantity: float,
    fee_bps: float,
    slippage_bps: float,
    cost_source: str,
    planned_entry_price: float | None = None,
    entry_drift_bps: float = 0.0,
    fill_delay_sec: float | None = None,
    fill_model: str = "fixed_1m",
    slippage_source: str = "default",
    liquidity_penalty_bps: float = 0.0,
    volatility_penalty_bps: float = 0.0,
    same_candle_policy: str = "sl_first",
    source_generated_at: str | None = None,
    consumed_at: str | None = None,
) -> FillCost:
    notional = abs(fill_price * quantity)
    fee_usdt = notional * fee_bps / 10_000
    slippage_usdt = abs(fill_price - reference_price) * abs(quantity)
    return FillCost(
        reference_price=reference_price,
        fill_price=fill_price,
        fee_bps=fee_bps,
        fee_usdt=fee_usdt,
        slippage_bps=slippage_bps,
        slippage_usdt=slippage_usdt,
        notional_usdt=notional,
        cost_source=cost_source,
        planned_entry_price=planned_entry_price,
        entry_drift_bps=entry_drift_bps,
        fill_delay_sec=fill_delay_sec,
        fill_model=fill_model,
        slippage_source=slippage_source,
        liquidity_penalty_bps=liquidity_penalty_bps,
        volatility_penalty_bps=volatility_penalty_bps,
        same_candle_policy=same_candle_policy,
        source_generated_at=source_generated_at,
        consumed_at=consumed_at,
    )


def paper_pnl(side: str, entry_price: float, exit_price: float, quantity: float) -> float:
    if side.upper() == "LONG":
        return (exit_price - entry_price) * quantity
    return (entry_price - exit_price) * quantity
