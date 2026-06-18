"""STEP5.1 market-entry SL/TP planner."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object, write_json_bytes
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.decision.market_entry_models import (
    MarketEntryDirectionDocument,
    MarketEntryDirectionItem,
    MarketEntryPlanDocument,
    MarketEntryPlanItem,
)
from laoma_signal_engine.market.decision_refresh_models import DecisionRefreshDocument, DecisionRefreshItem


@dataclass(frozen=True)
class MarketEntrySLTPConfig:
    stop_atr_mult: float = 1.2
    swing_buffer_atr_mult: float = 0.2
    target_rr: float = 1.25
    min_rr: float = 1.0
    max_stop_atr_mult: float = 2.2


def _default_direction_path(root: Path) -> Path:
    return root / "DATA" / "decisions" / "latest_market_entry_direction_decisions.json"


def _default_refresh_path(root: Path) -> Path:
    return root / "DATA" / "market" / "latest_decision_refresh_snapshot.json"


def _default_output_path(root: Path) -> Path:
    return root / "DATA" / "decisions" / "latest_market_entry_decisions.json"


def _key(symbol: str) -> str:
    return symbol.strip().upper()


def _num(d: dict[str, Any], key: str) -> float | None:
    v = d.get(key)
    if isinstance(v, bool) or v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _plan_prices(
    *,
    direction: str,
    entry: float,
    refresh: DecisionRefreshItem,
    cfg: MarketEntrySLTPConfig,
) -> tuple[float | None, float | None, list[str]]:
    reasons: list[str] = []
    atr = _num(refresh.entry_1m, "atr")
    if atr is None or atr <= 0:
        return None, None, ["entry_1m_atr_missing"]

    if direction == "LONG":
        atr_stop = entry - cfg.stop_atr_mult * atr
        pullback = _num(refresh.entry_1m, "last_pullback_low")
        swing_stop = None if pullback is None else pullback - cfg.swing_buffer_atr_mult * atr
        stop = min(atr_stop, swing_stop) if swing_stop is not None else atr_stop
        if stop >= entry:
            reasons.append("invalid_long_stop")
            return None, None, reasons
        risk = entry - stop
        take = entry + cfg.target_rr * risk
    elif direction == "SHORT":
        atr_stop = entry + cfg.stop_atr_mult * atr
        rebound = _num(refresh.entry_1m, "last_rebound_high")
        swing_stop = None if rebound is None else rebound + cfg.swing_buffer_atr_mult * atr
        stop = max(atr_stop, swing_stop) if swing_stop is not None else atr_stop
        if stop <= entry:
            reasons.append("invalid_short_stop")
            return None, None, reasons
        risk = stop - entry
        take = entry - cfg.target_rr * risk
    else:
        return None, None, ["no_market_direction"]

    if risk > cfg.max_stop_atr_mult * atr:
        reasons.append("stop_too_wide")
    return stop, take, reasons


def build_market_entry_plan_item(
    decision: MarketEntryDirectionItem,
    *,
    refresh: DecisionRefreshItem | None,
    direction_doc: MarketEntryDirectionDocument,
    refresh_doc: DecisionRefreshDocument,
    cfg: MarketEntrySLTPConfig | None = None,
) -> MarketEntryPlanItem:
    c = cfg or MarketEntrySLTPConfig()
    reasons = list(decision.reason_codes)
    if decision.action != "ENTER_MARKET":
        reasons.append("direction_gate_no_entry")
    if refresh is None:
        reasons.append("refresh_missing")
        entry = None
        stop = None
        take = None
    else:
        entry = refresh.last_price
        if entry is None or entry <= 0:
            reasons.append("entry_price_missing")
            stop = None
            take = None
        else:
            stop, take, price_reasons = _plan_prices(
                direction=decision.direction,
                entry=entry,
                refresh=refresh,
                cfg=c,
            )
            reasons.extend(price_reasons)

    risk: float | None = None
    reward: float | None = None
    rr: float | None = None
    if entry is not None and stop is not None and take is not None:
        if decision.direction == "LONG":
            risk = entry - stop
            reward = take - entry
        elif decision.direction == "SHORT":
            risk = stop - entry
            reward = entry - take
        if risk is not None and risk > 0 and reward is not None:
            rr = reward / risk
            if rr < c.min_rr:
                reasons.append("rr_too_low")
    executable = decision.action == "ENTER_MARKET" and not reasons and rr is not None
    return MarketEntryPlanItem(
        symbol=decision.symbol,
        decision_tf=decision.decision_tf,
        decision=decision.decision,
        direction=decision.direction,
        action=decision.action if executable else "NO_TRADE",
        entry_mode="MARKET" if executable else "NONE",
        estimated_entry_price=entry if executable else None,
        stop_loss=stop if executable else None,
        take_profit=take if executable else None,
        risk_per_unit=risk if executable else None,
        reward_per_unit=reward if executable else None,
        rr=rr if executable else None,
        executable=executable,
        reason_codes=sorted(set(reasons)),
        guards={
            "stop_atr_mult": c.stop_atr_mult,
            "swing_buffer_atr_mult": c.swing_buffer_atr_mult,
            "target_rr": c.target_rr,
            "min_rr": c.min_rr,
            "max_stop_atr_mult": c.max_stop_atr_mult,
        },
        input_refs={
            "direction_generated_at": direction_doc.generated_at,
            "refresh_generated_at": refresh_doc.generated_at,
        },
    )


def build_market_entry_plan_document(
    *,
    direction_doc: MarketEntryDirectionDocument,
    refresh_doc: DecisionRefreshDocument,
    generated_at: str,
    cfg: MarketEntrySLTPConfig | None = None,
) -> MarketEntryPlanDocument:
    refresh_by_symbol = {_key(it.symbol): it for it in refresh_doc.items}
    plans = [
        build_market_entry_plan_item(
            d,
            refresh=refresh_by_symbol.get(_key(d.symbol)),
            direction_doc=direction_doc,
            refresh_doc=refresh_doc,
            cfg=cfg,
        )
        for d in direction_doc.decisions
    ]
    exe = sum(1 for p in plans if p.executable)
    if not plans:
        status = "no_entries"
    elif exe == 0:
        status = "no_entries"
    elif exe < len(plans):
        status = "partial"
    else:
        status = "ok"
    return MarketEntryPlanDocument(
        generated_at=generated_at,
        status=status,  # type: ignore[arg-type]
        count=len(plans),
        executable_count=exe,
        plans=plans,
    )


def run_apply_market_entry_sl_tp_planner_safe(
    *,
    project_root: Path | None = None,
    direction_path: Path | None = None,
    refresh_path: Path | None = None,
    output_path: Path | None = None,
    stdout_json: bool = False,
) -> int:
    root = (project_root or Path.cwd()).resolve()
    direction_p = direction_path or _default_direction_path(root)
    refresh_p = refresh_path or _default_refresh_path(root)
    out_p = output_path or _default_output_path(root)
    try:
        direction_doc = MarketEntryDirectionDocument.model_validate(read_json_object(direction_p))
        refresh_doc = DecisionRefreshDocument.model_validate(read_json_object(refresh_p))
        doc = build_market_entry_plan_document(
            direction_doc=direction_doc,
            refresh_doc=refresh_doc,
            generated_at=to_iso_z(utc_now()),
        )
        out_p.parent.mkdir(parents=True, exist_ok=True)
        write_json_bytes(out_p, doc.model_dump(mode="json"))
        if stdout_json:
            print(
                json.dumps(
                    {
                        "step": "STEP5.1",
                        "status": doc.status,
                        "count": doc.count,
                        "executable_count": doc.executable_count,
                        "output": str(out_p),
                    },
                    ensure_ascii=False,
                ),
            )
        return EXIT_SUCCESS
    except (OSError, ValueError, ValidationError) as e:
        print(f"[ERROR] market entry SL/TP planner failed: {e}", file=sys.stderr)
        return EXIT_CONFIG if isinstance(e, ValidationError) else EXIT_INTERNAL
