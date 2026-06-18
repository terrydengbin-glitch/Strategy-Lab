"""Risk gate (T19): executable plans only; rejects go to rejected[]. docs/STEP5.0."""

from __future__ import annotations

from laoma_signal_engine.decision.final_models import FinalDecisionItem, RejectedDecisionItem
from laoma_signal_engine.decision.step5_config import RiskGateConfig
from laoma_signal_engine.factors.models import FactorSnapshotItem


def _atr_from_factor(factor: FactorSnapshotItem | None) -> float | None:
    if factor is None:
        return None
    p15 = factor.primary_15m if isinstance(factor.primary_15m, dict) else {}
    raw = p15.get("atr")
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        v = float(raw)
        return v if v > 0 else None
    return None


def _entry_mid(rp: FinalDecisionItem) -> float | None:
    lo, hi = rp.risk_plan.entry_zone_low, rp.risk_plan.entry_zone_high
    if lo is not None and hi is not None:
        return (float(lo) + float(hi)) / 2.0
    if lo is not None:
        return float(lo)
    if hi is not None:
        return float(hi)
    return None


def apply_risk_gate(
    item: FinalDecisionItem,
    factor: FactorSnapshotItem | None,
    cfg: RiskGateConfig,
) -> RejectedDecisionItem | None:
    rp = item.risk_plan
    if rp.plan_status != "executable":
        return None

    reasons: list[str] = []

    entry = _entry_mid(item)
    if entry is None:
        reasons.append("risk_missing_entry_zone")
    if rp.stop_loss is None:
        reasons.append("risk_missing_stop_loss")
    if rp.tp1 is None:
        reasons.append("risk_missing_tp1")
    if rp.rr_to_tp1 is None:
        reasons.append("risk_missing_rr")
    elif float(rp.rr_to_tp1) + 1e-9 < float(cfg.min_rr_to_tp1):
        reasons.append("risk_rr_below_min")

    atr = _atr_from_factor(factor)
    if atr is None:
        reasons.append("risk_missing_atr")

    if reasons:
        return _reject(item, reasons)

    assert entry is not None and rp.stop_loss is not None and atr is not None

    if item.direction == "LONG":
        risk_dist = entry - float(rp.stop_loss)
        if risk_dist <= 0:
            reasons.append("risk_long_stop_not_below_entry")
        elif risk_dist > cfg.max_sl_atr_multiple * atr + 1e-9:
            reasons.append("risk_sl_distance_above_max_atr")
        tp1f = float(rp.tp1) if rp.tp1 is not None else 0.0
        if tp1f <= entry + 1e-9:
            reasons.append("risk_tp1_not_above_entry")
    elif item.direction == "SHORT":
        risk_dist = float(rp.stop_loss) - entry
        if risk_dist <= 0:
            reasons.append("risk_short_stop_not_above_entry")
        elif risk_dist > cfg.max_sl_atr_multiple * atr + 1e-9:
            reasons.append("risk_sl_distance_above_max_atr")
        tp1f = float(rp.tp1) if rp.tp1 is not None else 0.0
        if tp1f >= entry - 1e-9:
            reasons.append("risk_tp1_not_below_entry")
    else:
        reasons.append("risk_executable_requires_long_or_short")
        return _reject(item, reasons)
    return None


def _reject(item: FinalDecisionItem, codes: list[str]) -> RejectedDecisionItem:
    joined = ",".join(codes)
    return RejectedDecisionItem(
        symbol=item.symbol,
        original_decision=item.decision,
        reject_reason_codes=list(codes),
        input_refs=dict(item.input_refs),
        summary_for_orchestrator=f"RiskGate rejected: {joined}",
    )

