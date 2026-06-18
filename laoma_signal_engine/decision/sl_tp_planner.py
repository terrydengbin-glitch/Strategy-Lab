"""SL/TP planner (T20): direction + factor (+ optional last price) -> risk_plan. docs/STEP5.0."""

from __future__ import annotations

from laoma_signal_engine.decision.final_models import EntryPriceBasis, RiskPlanBlock, RiskPlanPlanStatus
from laoma_signal_engine.decision.models import DecisionKind, DirectionDecisionItem
from laoma_signal_engine.decision.step5_config import SlTpPlannerConfig
from laoma_signal_engine.factors.models import FactorSnapshotItem


def infer_base_asset(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[: -len("USDT")]
    return symbol


def map_decision_to_plan_status(decision: DecisionKind) -> RiskPlanPlanStatus:
    if decision in ("LONG_NOW", "SHORT_NOW"):
        return "executable"
    if decision in ("LONG_WAIT_PULLBACK", "SHORT_WAIT_REBOUND"):
        return "pending_trigger"
    if decision == "HOLD_WATCH":
        return "observe_only"
    return "no_trade"


def build_risk_plan(
    direction: DirectionDecisionItem,
    factor: FactorSnapshotItem | None,
    last_price: float | None,
    cfg: SlTpPlannerConfig,
    max_sl_atr_multiple: float,
) -> RiskPlanBlock:
    decision = direction.decision
    status = map_decision_to_plan_status(decision)
    if status == "no_trade" or status == "observe_only":
        return RiskPlanBlock(
            plan_status=status,
            time_stop_minutes=None,
            invalid_condition="" if status == "observe_only" else "direction blocks trade",
        )
    if status == "pending_trigger":
        return _build_wait_risk_plan(decision, factor, cfg)
    return _build_now_risk_plan(decision, factor, last_price, cfg, max_sl_atr_multiple)


def _f(v: object) -> float | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _build_now_risk_plan(
    decision: DecisionKind,
    factor: FactorSnapshotItem | None,
    last_price: float | None,
    cfg: SlTpPlannerConfig,
    max_sl_atr_multiple: float,
) -> RiskPlanBlock:
    if factor is None:
        return RiskPlanBlock(
            plan_status="executable",
            entry_price_basis="last_price",
            invalid_condition="missing_factor_snapshot_item",
        )

    p15 = factor.primary_15m if isinstance(factor.primary_15m, dict) else {}

    atr = _f(p15.get("atr"))
    swing_low = _f(p15.get("recent_swing_low"))
    swing_high = _f(p15.get("recent_swing_high"))
    breakout = _f(p15.get("breakout_level"))
    breakdown = _f(p15.get("breakdown_level"))

    entry: float | None = None
    basis: EntryPriceBasis = "last_price"
    if last_price is not None:
        entry = float(last_price)
        basis = "last_price"
    elif decision == "LONG_NOW":
        if breakout is not None:
            entry = breakout
            basis = "breakout_level"
    elif decision == "SHORT_NOW":
        if breakdown is not None:
            entry = breakdown
            basis = "breakdown_level"

    if entry is None:
        return RiskPlanBlock(
            plan_status="executable",
            entry_price_basis=basis,
            invalid_condition="missing_last_price_and_structure_level",
            time_stop_minutes=cfg.time_stop_minutes,
        )

    if atr is None or atr <= 0:
        return RiskPlanBlock(
            plan_status="executable",
            entry_price_basis=basis,
            entry_zone_low=entry,
            entry_zone_high=entry,
            invalid_condition="missing_or_invalid_atr",
            time_stop_minutes=cfg.time_stop_minutes,
        )

    half_width = max(entry * 1e-6, cfg.entry_zone_atr_fraction * atr)
    zone_low = entry - half_width
    zone_high = entry + half_width

    buf = cfg.swing_sl_atr_buffer * atr
    max_sl_dist = max_sl_atr_multiple * atr
    stop: float | None = None
    if decision == "LONG_NOW":
        if swing_low is not None:
            stop = swing_low - buf
        if stop is None or stop >= entry:
            stop = entry - max_sl_dist
    else:
        if swing_high is not None:
            stop = swing_high + buf
        if stop is None or stop <= entry:
            stop = entry + max_sl_dist

    risk_one_r = abs(entry - stop)
    if risk_one_r <= 0:
        return RiskPlanBlock(
            plan_status="executable",
            entry_price_basis=basis,
            entry_zone_low=zone_low,
            entry_zone_high=zone_high,
            invalid_condition="stop_collapsed_relative_to_entry",
            time_stop_minutes=cfg.time_stop_minutes,
        )

    if decision == "LONG_NOW":
        tp1 = entry + cfg.rr_to_tp1 * risk_one_r
        tp2 = entry + cfg.rr_to_tp2 * risk_one_r
    else:
        tp1 = entry - cfg.rr_to_tp1 * risk_one_r
        tp2 = entry - cfg.rr_to_tp2 * risk_one_r

    rr1 = cfg.rr_to_tp1
    rr2 = cfg.rr_to_tp2

    return RiskPlanBlock(
        plan_status="executable",
        entry_price_basis=basis,
        entry_zone_low=zone_low,
        entry_zone_high=zone_high,
        stop_loss=stop,
        tp1=tp1,
        tp2=tp2,
        rr_to_tp1=rr1,
        rr_to_tp2=rr2,
        time_stop_minutes=cfg.time_stop_minutes,
        invalid_condition="",
    )


def _build_wait_risk_plan(
    decision: DecisionKind,
    factor: FactorSnapshotItem | None,
    cfg: SlTpPlannerConfig,
) -> RiskPlanBlock:
    est_low: float | None = None
    est_high: float | None = None
    trig = ""
    if factor is not None:
        p15 = factor.primary_15m if isinstance(factor.primary_15m, dict) else {}
        e1 = factor.entry_1m if isinstance(factor.entry_1m, dict) else {}
        if decision == "LONG_WAIT_PULLBACK":
            est_low = _f(e1.get("last_pullback_low")) or _f(p15.get("recent_swing_low"))
            est_high = _f(p15.get("recent_swing_high")) or _f(e1.get("last_breakout_high"))
            trig = (
                "Wait pullback toward support zone; require 5m hold above swing_low "
                "before ENTER; invalidate on breakdown_level."
            )
        else:
            est_high = _f(e1.get("last_rebound_high")) or _f(p15.get("recent_swing_high"))
            est_low = _f(p15.get("recent_swing_low")) or _f(e1.get("last_breakdown_low"))
            trig = (
                "Wait rebound toward resistance zone; require 5m hold below swing_high "
                "before ENTER; invalidate on breakout_level."
            )
    if (
        est_low is not None
        and est_high is not None
        and float(est_low) > float(est_high)
    ):
        est_low, est_high = est_high, est_low
    invalid_condition = "" if factor is not None else "missing_factor_snapshot_item"
    return RiskPlanBlock(
        plan_status="pending_trigger",
        time_stop_minutes=cfg.time_stop_minutes,
        estimated_entry_zone_low=est_low,
        estimated_entry_zone_high=est_high,
        trigger_condition=trig,
        invalid_condition=invalid_condition,
    )
