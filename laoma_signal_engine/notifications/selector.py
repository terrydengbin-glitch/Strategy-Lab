from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.notifications.config import FeishuConfig, STRATEGY_LINES


LINE_TO_PATH = {
    "without_micro": "DATA/decisions/latest_trade_plan_without_micro.json",
    "micro_fast": "DATA/decisions/latest_trade_plan_micro_fast.json",
    "micro_full": "DATA/decisions/latest_trade_plan_micro_full.json",
    "strategy4": "DATA/decisions/latest_trade_plan_strategy4.json",
}


def load_trade_plan_docs(project_root: Path, *, line: str | None = None) -> dict[str, dict[str, Any]]:
    docs: dict[str, dict[str, Any]] = {}
    wanted = (line,) if line else tuple(LINE_TO_PATH)
    for item in wanted:
        rel = LINE_TO_PATH.get(item)
        if rel is None:
            continue
        path = project_root / rel
        if path.exists():
            data = read_json_object(path)
            if isinstance(data, dict):
                docs[item] = data | {"__source_path": str(path)}
    return docs


def select_trade_plan_signals(
    docs: dict[str, dict[str, Any]],
    *,
    config: FeishuConfig,
    paper_summary: dict[str, Any] | None = None,
    line: str | None = None,
) -> dict[str, Any]:
    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    counts = {line: 0 for line in STRATEGY_LINES}
    wanted = (line,) if line else STRATEGY_LINES
    for item in wanted:
        if item not in STRATEGY_LINES:
            continue
        doc = docs.get(item)
        if not doc:
            skipped.append({"strategy_line": item, "reason": "source_missing"})
            continue
        if item not in config.notify_lines:
            skipped.append({"strategy_line": item, "reason": "line_disabled"})
            continue
        for plan in doc.get("plans") or []:
            signal, reason = _signal_from_plan(item, doc, plan, config=config, paper_summary=paper_summary)
            if signal:
                selected.append(signal)
                counts[item] += 1
            else:
                skipped.append(
                    {
                        "strategy_line": item,
                        "symbol": str(plan.get("symbol") or "").upper(),
                        "decision": str(plan.get("decision") or "").upper(),
                        "reason": reason,
                    }
                )
    return {"selected": selected, "selected_counts": counts, "skipped": skipped}


def mock_trade_plan_docs() -> dict[str, dict[str, Any]]:
    return {
        "without_micro": _mock_doc("without_micro", "OPGUSDT", "LONG", 0.2339, 0.2298, 0.2412),
        "micro_fast": _mock_doc("micro_fast", "ENAUSDT", "SHORT", 0.712, 0.724, 0.688),
        "micro_full": _mock_doc("micro_full", "SUIUSDT", "LONG", 3.42, 3.31, 3.68),
    }


def _signal_from_plan(
    line: str,
    doc: dict[str, Any],
    plan: dict[str, Any],
    *,
    config: FeishuConfig,
    paper_summary: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str]:
    side = str(plan.get("decision") or "").upper()
    if side not in {"LONG", "SHORT"}:
        return None, "non_entry_decision"
    if not bool(plan.get("executable")):
        return None, "non_executable"
    action = str(plan.get("action") or "").upper()
    entry_mode = str(plan.get("entry_mode") or "").upper()
    if action != "ENTER_MARKET" and entry_mode != "MARKET":
        return None, "not_market_entry"
    entry = _float(plan.get("estimated_entry_price") or plan.get("entry_price"))
    stop = _float(plan.get("stop_loss"))
    take = _float(plan.get("take_profit"))
    if entry is None:
        return None, "missing_entry"
    if stop is None:
        return None, "missing_stop_loss"
    if take is None:
        return None, "missing_take_profit"
    stats = _paper_stats(line, paper_summary)
    position_sizing = plan.get("position_sizing") if isinstance(plan.get("position_sizing"), dict) else {}
    signal = {
        "strategy_line": line,
        "strategy_name": config.strategy_name(line),
        "source_path": doc.get("__source_path") or LINE_TO_PATH[line],
        "run_id": doc.get("run_id"),
        "cycle_id": doc.get("cycle_id"),
        "generated_at": doc.get("generated_at"),
        "symbol": str(plan.get("symbol") or "").upper(),
        "side": side,
        "side_text": "多" if side == "LONG" else "空",
        "entry_price": entry,
        "stop_loss": stop,
        "take_profit": take,
        "risk_budget_usdt": position_sizing.get("risk_budget_usdt"),
        "notional_usdt": position_sizing.get("notional_usdt"),
        "margin_usdt": position_sizing.get("margin_usdt"),
        "leverage": position_sizing.get("leverage"),
        "estimated_max_loss_usdt": position_sizing.get("estimated_max_loss_usdt"),
        "source_plan_hash": _source_plan_hash(line, doc, plan, entry, stop, take),
        "target_set_id": ((plan.get("input_refs") or {}).get("micro_target_set_id") or (doc.get("input_refs") or {}).get("micro_target_set_id")),
        "paper_total_orders": stats.get("total_orders"),
        "paper_win_rate": stats.get("win_rate"),
    }
    return signal, ""


def _paper_stats(line: str, paper_summary: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(paper_summary, dict):
        return {}
    stats = paper_summary.get("stats") if isinstance(paper_summary.get("stats"), dict) else {}
    by_line = stats.get("by_line") if isinstance(stats.get("by_line"), dict) else {}
    got = by_line.get(line)
    return got if isinstance(got, dict) else {}


def _source_plan_hash(line: str, doc: dict[str, Any], plan: dict[str, Any], entry: float, stop: float, take: float) -> str:
    payload = "|".join(
        str(x or "")
        for x in (
            line,
            doc.get("run_id"),
            doc.get("cycle_id"),
            doc.get("generated_at"),
            plan.get("symbol"),
            plan.get("decision"),
            entry,
            stop,
            take,
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        got = float(value)
    except (TypeError, ValueError):
        return None
    return got if got > 0 else None


def _mock_doc(line: str, symbol: str, side: str, entry: float, stop: float, take: float) -> dict[str, Any]:
    return {
        "source": f"trade_plan_{line}",
        "run_id": "run_feishu_mock",
        "cycle_id": "cycle_feishu_mock",
        "generated_at": "2026-05-26T00:00:00Z",
        "__source_path": f"mock://{line}",
        "plans": [
            {
                "symbol": symbol,
                "decision": side,
                "action": "ENTER_MARKET",
                "entry_mode": "MARKET",
                "executable": True,
                "estimated_entry_price": entry,
                "stop_loss": stop,
                "take_profit": take,
            }
        ],
    }
