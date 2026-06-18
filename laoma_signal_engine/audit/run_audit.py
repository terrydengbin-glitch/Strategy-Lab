from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now

PIPELINE_STRATEGY_LINES = ("without_micro", "micro_fast", "micro_full", "strategy5", "strategy6")
SIDECAR_STRATEGY_LINES = ("strategy4",)
STRATEGY_LINES = (*PIPELINE_STRATEGY_LINES, *SIDECAR_STRATEGY_LINES)

LINE_PLAN_PATHS = {
    "without_micro": "DATA/decisions/latest_trade_plan_without_micro.json",
    "micro_fast": "DATA/decisions/latest_trade_plan_micro_fast.json",
    "micro_full": "DATA/decisions/latest_trade_plan_micro_full.json",
    "strategy5": "DATA/decisions/latest_trade_plan_strategy5.json",
    "strategy6": "DATA/decisions/latest_trade_plan_strategy6.json",
    "strategy4": "DATA/decisions/latest_trade_plan_strategy4.json",
}

LINE_REFRESH_PATHS = {
    "without_micro": "DATA/market/latest_decision_refresh_without_micro_snapshot.json",
    "micro_fast": "DATA/market/latest_decision_refresh_micro_fast_snapshot.json",
    "micro_full": "DATA/market/latest_decision_refresh_micro_full_snapshot.json",
    "strategy5": "DATA/market/latest_decision_refresh_without_micro_snapshot.json",
    "strategy6": "DATA/market/latest_decision_refresh_without_micro_snapshot.json",
    "strategy4": "DATA/market/latest_decision_refresh_snapshot.json",
}

LINE_LIFECYCLE_PATHS = {
    "micro_fast": "DATA/micro/latest_micro_lifecycle_micro_fast.json",
    "micro_full": "DATA/micro/latest_micro_lifecycle_micro_full.json",
}


def _without_micro_like(line: str) -> bool:
    return line in {"without_micro", "strategy4", "strategy5", "strategy6"}

ARTIFACT_PATHS = {
    "strategy_report": "DATA/reports/latest_strategy_pipeline_report.json",
    "abc_audit": "DATA/reports/latest_trade_plan_lines_compare.json",
    "json_stage_audit": "DATA/reports/latest_current_json_chain_audit_summary.json",
    "independent_downstream_audit": "DATA/reports/latest_independent_three_line_downstream_audit.json",
    "universe": "DATA/universe/CANDIDATE_UNIVERSE.json",
    "light_snapshot": "DATA/market/futures_light_snapshot.json",
    "raw_candidates": "DATA/raw_signals/latest_raw_candidates.json",
    "watch_signals": "DATA/raw_signals/latest_watch_signals.json",
    "strong_candidates": "DATA/raw_signals/latest_strong_candidates.json",
    "micro_targets": "DATA/micro/micro_targets.json",
    "factor_with_micro": "DATA/factors/latest_factor_snapshot.json",
    "factor_without_micro": "DATA/factors/latest_factor_snapshot_withoutoficvd.json",
    "latest_decisions": "DATA/decisions/latest_decisions.json",
    "feishu_latest_report": "DATA/notifications/latest_delivery_report.json",
    "feishu_delivery_history": "DATA/notifications/delivery_history.json",
    "paper_summary": "DATA/paper/latest_paper_state.json",
}


def _root(project_root: Path | None = None) -> Path:
    return Path(project_root).resolve() if project_root else Path.cwd().resolve()


def _read_json(path: Path) -> Any | None:
    try:
        return read_json_object(path)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _digest(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _artifact(root: Path, key: str, rel: str) -> dict[str, Any]:
    path = root / rel
    doc = _read_json(path)
    return {
        "key": key,
        "path": str(path),
        "exists": path.exists(),
        "sha256": _digest(path),
        "generated_at": doc.get("generated_at") if isinstance(doc, dict) else None,
        "run_id": doc.get("run_id") if isinstance(doc, dict) else None,
        "cycle_id": doc.get("cycle_id") if isinstance(doc, dict) else None,
        "source": doc.get("source") if isinstance(doc, dict) else None,
    }


def _items(doc: Any) -> list[dict[str, Any]]:
    if not isinstance(doc, dict):
        return []
    rows = doc.get("items")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    rows = doc.get("plans")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _symbol(row: dict[str, Any]) -> str | None:
    value = row.get("symbol") or row.get("s")
    return str(value).upper() if value else None


def _symbols_from_doc(doc: Any) -> set[str]:
    symbols: set[str] = set()
    if isinstance(doc, dict):
        for key in ("symbols", "target_symbols", "plan_candidate_symbols"):
            raw = doc.get(key)
            if isinstance(raw, list):
                symbols.update(str(x).upper() for x in raw if x)
        for row in _items(doc):
            sym = _symbol(row)
            if sym:
                symbols.add(sym)
        for key in ("raw", "watch", "strong", "candidates"):
            raw = doc.get(key)
            if isinstance(raw, list):
                for row in raw:
                    if isinstance(row, dict):
                        sym = _symbol(row)
                        if sym:
                            symbols.add(sym)
    return symbols


def _refresh_index(doc: Any) -> dict[str, dict[str, Any]]:
    return {sym: row for row in _items(doc) if (sym := _symbol(row))}


def _lifecycle_index(doc: Any) -> dict[str, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(doc, dict):
        raw = doc.get("symbols") or doc.get("items") or doc.get("lifecycles") or []
        if isinstance(raw, list):
            rows = [row for row in raw if isinstance(row, dict)]
    return {sym: row for row in rows if (sym := _symbol(row))}


def _plan_status(plan: dict[str, Any]) -> dict[str, Any]:
    guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
    return {
        "symbol": _symbol(plan),
        "decision": plan.get("decision"),
        "action": plan.get("action"),
        "entry_mode": plan.get("entry_mode"),
        "executable": bool(plan.get("executable")),
        "entry_price": plan.get("estimated_entry_price") or plan.get("entry_price"),
        "stop_loss": plan.get("stop_loss"),
        "take_profit": plan.get("take_profit"),
        "rr": plan.get("rr"),
        "confidence": plan.get("confidence"),
        "opportunity_type": plan.get("opportunity_type"),
        "reason_codes": list(plan.get("reason_codes") or []),
        "refresh_fresh": guards.get("refresh_fresh"),
        "direction_still_valid": guards.get("direction_still_valid"),
        "range_room_ok": guards.get("range_room_ok"),
        "liquidity_ok": guards.get("liquidity_ok"),
        "micro_ready": guards.get("micro_ready"),
        "micro_alignment_ok": guards.get("micro_alignment_ok") or guards.get("micro_direction_confirmed"),
        "micro_symbol_confirmed": guards.get("micro_symbol_confirmed"),
        "micro_direction_confirmed": guards.get("micro_direction_confirmed"),
        "micro_exec_allowed": guards.get("micro_exec_allowed"),
        "micro_lifecycle_state": guards.get("micro_lifecycle_state"),
        "micro_policy_relaxed": guards.get("micro_policy_relaxed"),
        "micro_confirmation_strength": guards.get("micro_confirmation_strength"),
        "micro_consumption_policy": guards.get("micro_consumption_policy")
        or (guards.get("gate_config_snapshot") if isinstance(guards.get("gate_config_snapshot"), dict) else {}).get("micro_consumption_policy"),
        "allow_weak_micro_consumption": guards.get("allow_weak_micro_consumption")
        or (guards.get("gate_config_snapshot") if isinstance(guards.get("gate_config_snapshot"), dict) else {}).get("allow_weak_micro_consumption"),
        "trade_plan_consumable": guards.get("trade_plan_consumable"),
        "consumption_block_reason": guards.get("consumption_block_reason"),
        "guards": guards,
    }


def _line_stage_status(stages: list[dict[str, Any]], line: str) -> dict[str, Any]:
    matched = [row for row in stages if isinstance(row, dict) and str(row.get("name") or "").endswith(line)]
    wait = [row for row in stages if isinstance(row, dict) and str(row.get("name") or "") == f"wait_micro_ready_{line}"]
    return {
        "stage_count": len(matched) + len(wait),
        "ok": all(bool(row.get("ok")) for row in [*matched, *wait]) if (matched or wait) else None,
        "failed_stages": [row.get("name") for row in [*matched, *wait] if not row.get("ok")],
        "wait_detail": wait[-1].get("detail") if wait else None,
    }


def _paper_rows(root: Path, run_id: str | None) -> dict[str, Any]:
    db = root / "DATA/paper/paper_trading.db"
    out: dict[str, Any] = {"db_path": str(db), "exists": db.exists(), "orders": [], "positions": [], "fills": []}
    if not db.exists():
        return out
    try:
        with sqlite3.connect(db) as conn:
            conn.row_factory = sqlite3.Row
            for table, key in (
                ("paper_orders", "orders"),
                ("paper_positions", "positions"),
                ("paper_fills", "fills"),
                ("paper_skip_ledger", "skips"),
            ):
                try:
                    cols = [row["name"] for row in conn.execute(f"pragma table_info({table})").fetchall()]
                    if not cols:
                        continue
                    if run_id and "source_run_id" in cols:
                        rows = conn.execute(f"select * from {table} where source_run_id = ? limit 500", (run_id,)).fetchall()
                    elif run_id and "run_id" in cols:
                        rows = conn.execute(f"select * from {table} where run_id = ? limit 500", (run_id,)).fetchall()
                    else:
                        rows = conn.execute(f"select * from {table} order by rowid desc limit 100").fetchall()
                    out[key] = [dict(row) for row in rows]
                except sqlite3.Error:
                    continue
    except sqlite3.Error as exc:
        out["error"] = str(exc)
    return out


def _feishu_rows(root: Path, run_id: str | None) -> dict[str, Any]:
    history = _read_json(root / "DATA/notifications/delivery_history.json")
    latest = _read_json(root / "DATA/notifications/latest_delivery_report.json")
    rows = []
    if isinstance(history, dict) and isinstance(history.get("deliveries"), list):
        rows = [row for row in history["deliveries"] if isinstance(row, dict)]
    if run_id:
        rows = [row for row in rows if row.get("run_id") == run_id or row.get("source_run_id") == run_id]
    return {"latest_report": latest if isinstance(latest, dict) else None, "deliveries": rows[-200:]}


def _number_or_zero(value: Any) -> float:
    if value is None or isinstance(value, bool):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _order_pnl(order: dict[str, Any]) -> float:
    for key in ("realized_pnl_usdt", "pnl_usdt", "pnl", "net_pnl"):
        if key in order:
            return _number_or_zero(order.get(key))
    return 0.0


def _build_consumable_to_executable_funnel(
    *,
    line_payloads: dict[str, dict[str, Any]],
    symbol_rows: list[dict[str, Any]],
    downstream: dict[str, Any],
    lifecycle_docs: dict[str, Any],
) -> dict[str, Any]:
    paper_orders = (downstream.get("paper") or {}).get("rows", {}).get("orders") or []
    paper_skips = (downstream.get("paper") or {}).get("rows", {}).get("skips") or []
    out: dict[str, Any] = {}
    for line in PIPELINE_STRATEGY_LINES:
        line_symbols = [row for row in symbol_rows if row.get("strategy_line") == line]
        lifecycle = lifecycle_docs.get(line) if isinstance(lifecycle_docs.get(line), dict) else {}
        lifecycle_items = lifecycle.get("items") if isinstance(lifecycle.get("items"), list) else []
        lifecycle_consumable_symbols = {
            _symbol(row)
            for row in lifecycle_items
            if isinstance(row, dict) and row.get("trade_plan_consumable") is True and _symbol(row)
        }
        generated_symbols = {_symbol(row.get("plan") or {}) for row in line_symbols if _symbol(row.get("plan") or {})}
        executable_symbols = {
            str(row.get("symbol") or "").upper()
            for row in line_symbols
            if row.get("executable") is True and row.get("symbol")
        }
        orders = [
            row
            for row in paper_orders
            if str(row.get("strategy_line") or row.get("line") or "").lower() == line
        ]
        closed_orders = [row for row in orders if str(row.get("status") or "").lower() == "closed"]
        skips = [
            row
            for row in paper_skips
            if str(row.get("strategy_line") or row.get("line") or "").lower() == line
            and row.get("executable") in (1, True)
        ]
        skip_reason_counts = Counter(str(row.get("skip_reason") or "skipped_unknown") for row in skips)
        order_symbols = {
            str(row.get("symbol") or "").upper()
            for row in orders
            if row.get("symbol")
        }
        skipped_symbols = {
            str(row.get("symbol") or "").upper()
            for row in skips
            if row.get("symbol")
        }
        reason_counts: Counter[str] = Counter()
        non_executable_symbols: list[dict[str, Any]] = []
        for row in line_symbols:
            plan = row.get("plan") if isinstance(row.get("plan"), dict) else {}
            if row.get("executable"):
                continue
            reasons = list(row.get("reason_codes") or plan.get("reason_codes") or [])
            reason_counts.update(str(r) for r in reasons)
            if not _without_micro_like(line) and row.get("trade_plan_consumable") is True:
                non_executable_symbols.append(
                    {
                        "symbol": row.get("symbol"),
                        "reason_codes": reasons,
                        "action": row.get("action"),
                        "entry_mode": row.get("entry_mode"),
                    },
                )
        out[line] = {
            "lifecycle_consumable_count": len(lifecycle_consumable_symbols) if not _without_micro_like(line) else None,
            "generated_plan_count": len(line_symbols),
            "executable_count": len(executable_symbols),
            "paper_order_count": len(orders),
            "paper_skip_count": len(skips),
            "paper_skipped_reason_counts": dict(skip_reason_counts.most_common(20)),
            "executable_skipped_symbols": sorted(skipped_symbols),
            "closed_count": len(closed_orders),
            "net_pnl": round(sum(_order_pnl(row) for row in closed_orders), 6),
            "consumable_missing_plan_symbols": sorted(lifecycle_consumable_symbols - generated_symbols),
            "executable_missing_paper_symbols": sorted(executable_symbols - order_symbols - skipped_symbols),
            "non_executable_consumable_symbols": non_executable_symbols[:50],
            "blocked_reason_counts": dict(reason_counts.most_common(20)),
            "line_status": (line_payloads.get(line) or {}).get("status"),
        }
    return out


def _step(name: str, ok: bool | None, *, severity: str = "info", detail: Any = None) -> dict[str, Any]:
    status = "pass" if ok is True else ("fail" if ok is False else "unknown")
    return {"name": name, "status": status, "ok": ok, "severity": severity, "detail": detail}


def _selected_strategy_lines(strategy_report: dict[str, Any]) -> tuple[set[str], set[str]]:
    selected_raw = strategy_report.get("selected_lines")
    skipped_raw = strategy_report.get("skipped_lines")
    selected = {str(x) for x in selected_raw if x in PIPELINE_STRATEGY_LINES} if isinstance(selected_raw, list) else set(PIPELINE_STRATEGY_LINES)
    skipped = {str(x) for x in skipped_raw if x in PIPELINE_STRATEGY_LINES} if isinstance(skipped_raw, list) else set()
    if not selected:
        selected = set(PIPELINE_STRATEGY_LINES) - skipped
    return selected, skipped


def _micro_trade_plan_policy_violations(plan: dict[str, Any], lifecycle: dict[str, Any]) -> list[str]:
    if plan.get("trade_plan_consumable") is not True:
        return ["micro_trade_plan_consumable_false"]
    state = str(plan.get("micro_lifecycle_state") or lifecycle.get("state") or lifecycle.get("status") or "")
    if state in {"observing", "timeout", "rejected", "not_ready", "blocked"}:
        return ["micro_forbidden_lifecycle_state_consumed_by_trade_plan"]
    policy = str(plan.get("micro_consumption_policy") or "confirmed_only")
    if policy not in {"confirmed_only", "ready_signal_usable", "weak_ready_test", "audit_only"}:
        return ["micro_unknown_consumption_policy"]
    if policy == "audit_only":
        return ["micro_audit_only_consumed_by_trade_plan"]
    if policy == "confirmed_only":
        out: list[str] = []
        if plan.get("micro_symbol_confirmed") is not True and state not in {"confirmed", "emitted"}:
            out.append("micro_confirmed_only_symbol_not_confirmed")
        if plan.get("micro_direction_confirmed") is not True:
            out.append("micro_confirmed_only_direction_not_confirmed")
        if plan.get("micro_exec_allowed") is not True:
            out.append("micro_confirmed_only_exec_not_allowed")
        return out
    if plan.get("micro_symbol_confirmed") is True:
        return []
    if plan.get("allow_weak_micro_consumption") is not True:
        return ["micro_relaxed_policy_not_enabled"]
    if not (plan.get("micro_policy_relaxed") is True or str(plan.get("micro_confirmation_strength") or "") == "weak"):
        return ["micro_relaxed_policy_missing_evidence"]
    if str(plan.get("consumption_block_reason") or ""):
        return ["micro_relaxed_policy_has_block_reason"]
    return []


BUSINESS_WARNING_NAMES = {
    "line.no_executable",
    "line.micro_wait_trade_plan_ready_count.aligned",
    "line.pipeline_stages",
    "funnel.consumable_missing_plan",
    "funnel.executable_missing_paper",
    "funnel.executable_missing_paper_pending_settlement",
}

PAPER_SETTLEMENT_COMPLETE_STATUSES = {"complete", "no_executable"}
PAPER_SETTLEMENT_FINAL_MISSING_STATUSES = {"missing_after_settlement"}
PAPER_SETTLEMENT_PENDING_STATUSES = {"pending", "timeout", "error", "not_started", "not_available", "unknown"}


def _warning_class(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "")
    detail = row.get("detail")
    detail_text = json.dumps(detail, ensure_ascii=False, sort_keys=True) if isinstance(detail, (dict, list)) else str(detail or "")
    if "paper_settlement" in name or "paper_settlement" in detail_text:
        return "paper_settlement"
    if name in BUSINESS_WARNING_NAMES:
        return "business"
    if "business_no_signal" in detail_text or "skipped_not_selected" in detail_text or "no_entries" in detail_text:
        return "business"
    if "skipped_" in detail_text:
        return "explained_skip"
    return "contract"


def _paper_settlement_from_report(strategy_report: dict[str, Any]) -> dict[str, Any]:
    settlement = strategy_report.get("paper_settlement_barrier")
    if isinstance(settlement, dict):
        status = str(settlement.get("status") or "unknown")
        return {**settlement, "status": status}
    return {"status": "not_available", "source": "missing_paper_settlement_barrier"}


def _sqlite_count(db: Path, table: str, where: str = "", params: tuple[Any, ...] = ()) -> int:
    if not db.exists():
        return 0
    try:
        with sqlite3.connect(db) as conn:
            row = conn.execute(
                f"SELECT count(*) FROM {table}{(' WHERE ' + where) if where else ''}",
                params,
            ).fetchone()
        return int(row[0] if row else 0)
    except sqlite3.Error:
        return 0


def _sqlite_group_counts(db: Path, table: str, column: str, where: str = "", params: tuple[Any, ...] = ()) -> dict[str, int]:
    if not db.exists():
        return {}
    try:
        with sqlite3.connect(db) as conn:
            rows = conn.execute(
                f"SELECT {column}, count(*) FROM {table}{(' WHERE ' + where) if where else ''} GROUP BY {column}",
                params,
            ).fetchall()
    except sqlite3.Error:
        return {}
    return {str(key or "unknown"): int(count or 0) for key, count in rows}


def _strategy4_trade_quality_count(root: Path) -> int:
    candidates = [
        root / "DATA/paper/paper_trading.db",
        root / "DATA/trade_quality/trade_quality.db",
        root / "DATA/trade_quality/trade_quality_diagnostics.db",
    ]
    for db in candidates:
        count = _sqlite_count(db, "trade_quality_diagnostic_samples", "strategy_line=?", ("strategy4",))
        if count:
            return count
        legacy_count = _sqlite_count(db, "trade_quality_samples", "strategy_line=?", ("strategy4",))
        if legacy_count:
            return legacy_count
    return 0


def _strategy4_paper_counts(root: Path) -> dict[str, int]:
    db = root / "DATA/paper/paper_trading.db"
    return {
        "paper_orders": _sqlite_count(db, "paper_orders", "strategy_line=?", ("strategy4",)),
        "paper_closed": _sqlite_count(db, "paper_orders", "strategy_line=? AND status='closed'", ("strategy4",)),
        "paper_skips": _sqlite_count(db, "paper_skip_ledger", "strategy_line=?", ("strategy4",)),
        "paper_intents": _sqlite_count(db, "paper_intent_inbox", "strategy_line=?", ("strategy4",)),
        "trade_quality_samples": _strategy4_trade_quality_count(root),
    }


def _strategy4_attempt_evidence(root: Path) -> dict[str, Any]:
    db = root / "DATA/strategy4/strategy4_observe.db"
    return {
        "db_path": str(db),
        "attempt_count": _sqlite_count(db, "strategy4_attempts"),
        "attempt_executable_count": _sqlite_count(db, "strategy4_attempts", "executable=1"),
        "attempt_status_counts": _sqlite_group_counts(db, "strategy4_attempts", "status"),
        "attempt_action_counts": _sqlite_group_counts(db, "strategy4_attempts", "action"),
    }


def _strategy4_sidecar_lines(root: Path, *, run_id: str | None, cycle_id: str | None, artifacts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    pool_path = root / "DATA/decisions/strategy4_observe_pool.json"
    plan_path = root / LINE_PLAN_PATHS["strategy4"]
    status_path = root / "DATA/runtime/strategy4_daemon_status.json"
    heartbeat_path = root / "DATA/runtime/strategy4_heartbeat.json"
    pool_doc = _read_json(pool_path) if pool_path.exists() else {}
    plan_doc = _read_json(plan_path) if plan_path.exists() else {}
    status_doc = _read_json(status_path) if status_path.exists() else {}
    heartbeat_doc = _read_json(heartbeat_path) if heartbeat_path.exists() else {}
    pool_items = _items(pool_doc)
    plan_items = _items(plan_doc)
    status_counts = pool_doc.get("status_counts") if isinstance(pool_doc, dict) and isinstance(pool_doc.get("status_counts"), dict) else {}
    if not status_counts:
        status_counts = dict(Counter(str(row.get("status") or "unknown") for row in pool_items))

    plan_count = int(plan_doc.get("count", len(plan_items)) if isinstance(plan_doc, dict) else 0)
    plan_executable_count = int(
        plan_doc.get("executable_count", sum(1 for row in plan_items if bool(row.get("executable"))))
        if isinstance(plan_doc, dict)
        else 0
    )
    output_run_id = plan_doc.get("run_id") if isinstance(plan_doc, dict) else None
    output_cycle_id = plan_doc.get("cycle_id") if isinstance(plan_doc, dict) else None
    output_fresh = bool(run_id and cycle_id and output_run_id == run_id and output_cycle_id == cycle_id)
    stale_reason = None if output_fresh else ("output_missing" if not plan_doc else "sidecar_output_not_selected_run")

    attempt = _strategy4_attempt_evidence(root)
    downstream = _strategy4_paper_counts(root)
    daemon_state = (
        status_doc.get("state")
        or status_doc.get("status")
        or heartbeat_doc.get("state")
        or heartbeat_doc.get("status")
        or "unknown"
    ) if isinstance(status_doc, dict) or isinstance(heartbeat_doc, dict) else "unknown"
    reason_codes: list[str] = []
    if not isinstance(pool_doc, dict) or not pool_doc:
        reason_codes.append("strategy4_pool_missing")
    if not isinstance(plan_doc, dict) or not plan_doc:
        reason_codes.append("strategy4_trade_plan_missing")
    if str(daemon_state).lower() not in {"ok", "running", "healthy"}:
        reason_codes.append("strategy4_daemon_state_check")

    return {
        "strategy4": {
            "display_name": "异动肆号",
            "strategy_line": "strategy4",
            "mode": "observe_daemon",
            "source_line": "without_micro",
            "pipeline_selected": False,
            "audit_scope": "sidecar_latest_evidence",
            "daemon_state": daemon_state,
            "heartbeat": heartbeat_doc if isinstance(heartbeat_doc, dict) else {},
            "pool_count": int(pool_doc.get("count", len(pool_items)) if isinstance(pool_doc, dict) else 0),
            "status_counts": status_counts,
            "attempt_count": attempt["attempt_count"],
            "attempt_executable_count": attempt["attempt_executable_count"],
            "attempt_status_counts": attempt["attempt_status_counts"],
            "attempt_action_counts": attempt["attempt_action_counts"],
            "latest_trade_plan": {
                "path": str(plan_path),
                "count": plan_count,
                "executable_count": plan_executable_count,
                "output_run_id": output_run_id,
                "output_cycle_id": output_cycle_id,
                "output_fresh": output_fresh,
                "stale_output_reason": stale_reason,
                "generated_at": plan_doc.get("generated_at") if isinstance(plan_doc, dict) else None,
            },
            "downstream": downstream,
            "artifact_refs": {
                "observe_pool": {"path": str(pool_path), "exists": pool_path.exists(), "sha256": _digest(pool_path)},
                "trade_plan": artifacts.get("trade_plan_strategy4") or _artifact(root, "trade_plan_strategy4", LINE_PLAN_PATHS["strategy4"]),
                "daemon_status": {"path": str(status_path), "exists": status_path.exists(), "sha256": _digest(status_path)},
                "heartbeat": {"path": str(heartbeat_path), "exists": heartbeat_path.exists(), "sha256": _digest(heartbeat_path)},
                "attempt_db": {"path": attempt["db_path"], "exists": (root / "DATA/strategy4/strategy4_observe.db").exists()},
            },
            "reason_codes": reason_codes,
        }
    }


def build_run_level_audit(project_root: Path | None = None, *, run_id: str | None = None, cycle_id: str | None = None) -> dict[str, Any]:
    root = _root(project_root)
    generated_at = to_iso_z(utc_now())
    artifacts: dict[str, dict[str, Any]] = {}
    for key, rel in ARTIFACT_PATHS.items():
        artifacts[key] = _artifact(root, key, rel)
    for line, rel in LINE_PLAN_PATHS.items():
        artifacts[f"trade_plan_{line}"] = _artifact(root, f"trade_plan_{line}", rel)
    for line, rel in LINE_REFRESH_PATHS.items():
        artifacts[f"refresh_{line}"] = _artifact(root, f"refresh_{line}", rel)
    for line, rel in LINE_LIFECYCLE_PATHS.items():
        artifacts[f"micro_lifecycle_{line}"] = _artifact(root, f"micro_lifecycle_{line}", rel)

    docs = {key: _read_json(Path(meta["path"])) for key, meta in artifacts.items()}
    strategy_report = docs.get("strategy_report") if isinstance(docs.get("strategy_report"), dict) else {}
    if not run_id:
        run_id = strategy_report.get("run_id") or next((meta.get("run_id") for meta in artifacts.values() if meta.get("run_id")), None)
    if not cycle_id:
        cycle_id = strategy_report.get("cycle_id") or next((meta.get("cycle_id") for meta in artifacts.values() if meta.get("cycle_id")), None)
    stages = strategy_report.get("stages") if isinstance(strategy_report.get("stages"), list) else []
    selected_lines, skipped_lines = _selected_strategy_lines(strategy_report)
    paper_settlement = _paper_settlement_from_report(strategy_report)
    paper_settlement_status = str(paper_settlement.get("status") or "unknown")

    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    shared_steps = [
        _step("run_id.present", bool(run_id), severity="p0", detail={"run_id": run_id, "cycle_id": cycle_id}),
        _step("strategy_report.present", bool(strategy_report), severity="p0", detail=artifacts["strategy_report"]),
        _step("step1.universe.current_json", artifacts["universe"]["exists"], severity="p0", detail=artifacts["universe"]),
        _step("step1_5.light_snapshot.current_json", artifacts["light_snapshot"]["exists"], severity="p0", detail=artifacts["light_snapshot"]),
        _step("step2.watch_and_strong.current_json", artifacts["watch_signals"]["exists"] and artifacts["strong_candidates"]["exists"], severity="p0"),
        _step("step2_5.micro_targets.current_json", artifacts["micro_targets"]["exists"], severity="warn", detail=artifacts["micro_targets"]),
    ]
    for row in shared_steps:
        if row["ok"] is False and row["severity"] == "p0":
            failures.append({"scope": "shared", **row})

    candidate_symbols = set()
    for key in (
        "raw_candidates",
        "watch_signals",
        "strong_candidates",
        "micro_targets",
        "factor_without_micro",
        "factor_with_micro",
        "refresh_without_micro",
        "refresh_micro_fast",
        "refresh_micro_full",
    ):
        candidate_symbols.update(_symbols_from_doc(docs.get(key)))

    line_payloads: dict[str, Any] = {}
    symbol_rows: list[dict[str, Any]] = []
    for line in PIPELINE_STRATEGY_LINES:
        selected_line = line in selected_lines and line not in skipped_lines
        plan_doc = docs.get(f"trade_plan_{line}") if isinstance(docs.get(f"trade_plan_{line}"), dict) else {}
        refresh_doc = docs.get(f"refresh_{line}") if isinstance(docs.get(f"refresh_{line}"), dict) else {}
        lifecycle_doc = docs.get(f"micro_lifecycle_{line}") if isinstance(docs.get(f"micro_lifecycle_{line}"), dict) else {}
        refs = plan_doc.get("input_refs") if isinstance(plan_doc.get("input_refs"), dict) else {}
        plan_rows = [_plan_status(row) for row in _items(plan_doc)] if selected_line else []
        refresh_by_symbol = _refresh_index(refresh_doc) if selected_line else {}
        lifecycle_by_symbol = _lifecycle_index(lifecycle_doc) if selected_line else {}
        line_symbols = {row["symbol"] for row in plan_rows if row.get("symbol")}
        line_symbols.update(refresh_by_symbol)
        line_symbols.update(lifecycle_by_symbol)
        stage_status = _line_stage_status(stages, line)
        if selected_line:
            line_steps = [
                _step("line.run_id.matches", bool(run_id and plan_doc.get("run_id") == run_id), severity="p0", detail=plan_doc.get("run_id")),
                _step("line.cycle_id.matches", bool(cycle_id and plan_doc.get("cycle_id") == cycle_id), severity="p0", detail=plan_doc.get("cycle_id")),
                _step("line.pre_trade_refresh.present", bool(refresh_doc), severity="p0", detail=artifacts.get(f"refresh_{line}")),
                _step("line.pipeline_stages", stage_status["ok"], severity="warn", detail=stage_status),
            ]
        else:
            line_steps = [
                _step(
                    "line.skipped_not_selected",
                    True,
                    severity="info",
                    detail={"selected_lines": sorted(selected_lines), "skipped_lines": sorted(skipped_lines), "stage_status": stage_status},
                ),
            ]
        if not _without_micro_like(line):
            if selected_line:
                line_steps.append(
                    _step("line.micro_lifecycle.present", bool(lifecycle_doc), severity="p0", detail=artifacts.get(f"micro_lifecycle_{line}")),
                )
            wait_detail = stage_status.get("wait_detail") if isinstance(stage_status.get("wait_detail"), dict) else {}
            if selected_line and isinstance(wait_detail, dict) and wait_detail.get("wait_evidence_path"):
                line_steps.append(
                    _step(
                        "line.micro_wait_evidence.consumed",
                        refs.get("micro_wait_evidence_used") is True,
                        severity="p0",
                        detail={
                            "wait_evidence_path": wait_detail.get("wait_evidence_path"),
                            "plan_evidence_path": refs.get("micro_wait_evidence_path"),
                        },
                    ),
                )
            symbol_counts = refs.get("symbol_counts") if isinstance(refs.get("symbol_counts"), dict) else {}
            wait_counts = wait_detail.get("symbol_counts") if isinstance(wait_detail.get("symbol_counts"), dict) else {}
            wait_usable = wait_counts.get("usable_ready")
            plan_ready = symbol_counts.get("ready")
            plan_consumable = symbol_counts.get("consumable")
            if selected_line and wait_usable is not None and plan_ready is not None:
                line_steps.append(
                    _step(
                        "line.micro_wait_trade_plan_ready_count.aligned",
                        int(wait_usable) == int(plan_ready),
                        severity="warn",
                        detail={
                            "wait_usable_ready": wait_usable,
                            "plan_ready": plan_ready,
                            "plan_consumable": plan_consumable,
                            "classification": "business_readiness_semantics_drift",
                        },
                    ),
                )
            observing_symbols = [
                str(row.get("symbol"))
                for row in (lifecycle_doc.get("items") if isinstance(lifecycle_doc.get("items"), list) else [])
                if isinstance(row, dict) and str(row.get("state") or "") == "observing"
            ]
            if selected_line:
                line_steps.append(
                    _step(
                        "line.micro_no_observing_after_run_finished",
                        not observing_symbols,
                        severity="p0",
                        detail={"observing_symbols": observing_symbols[:50], "count": len(observing_symbols)},
                    ),
                )
        counts = Counter(str(row.get("action") or row.get("entry_mode") or "unknown") for row in plan_rows)
        for row in line_steps:
            if row["ok"] is False and row["severity"] == "p0":
                failures.append({"scope": line, **row})
            elif row["ok"] is False:
                warnings.append({"scope": line, **row})

        for plan in plan_rows:
            sym = plan.get("symbol")
            refresh = refresh_by_symbol.get(str(sym), {})
            lifecycle = lifecycle_by_symbol.get(str(sym), {})
            p0: list[str] = []
            warn: list[str] = []
            if sym and candidate_symbols and sym not in candidate_symbols:
                p0.append("symbol_not_in_candidate_set")
            if plan["executable"]:
                if not (plan.get("entry_price") and plan.get("stop_loss") and plan.get("take_profit")):
                    p0.append("executable_missing_entry_sl_tp")
                if refresh and refresh.get("direction_still_valid") is False:
                    p0.append("executable_refresh_direction_invalid")
                if not refresh:
                    p0.append("executable_missing_refresh_evidence")
                if not _without_micro_like(line):
                    p0.extend(_micro_trade_plan_policy_violations(plan, lifecycle))
            elif not _without_micro_like(line):
                p0.extend(_micro_trade_plan_policy_violations(plan, lifecycle))
            elif not refresh:
                warn.append("non_executable_missing_refresh_evidence")
            if p0:
                failures.append({"scope": line, "symbol": sym, "name": "symbol.p0", "severity": "p0", "reason_codes": p0})
            if warn:
                warnings.append({"scope": line, "symbol": sym, "name": "symbol.warn", "severity": "warn", "reason_codes": warn})
            symbol_rows.append(
                {
                    "run_id": run_id,
                    "cycle_id": cycle_id,
                    "strategy_line": line,
                    "symbol": sym,
                    "decision": plan.get("decision"),
                    "action": plan.get("action"),
                    "entry_mode": plan.get("entry_mode"),
                    "executable": plan["executable"],
                    "status": "failed" if p0 else ("warn" if warn else "ok"),
                    "reason_codes": [*plan.get("reason_codes", []), *p0, *warn],
                    "plan": plan,
                    "refresh": {
                        "present": bool(refresh),
                        "direction_still_valid": refresh.get("direction_still_valid"),
                        "refresh_age_sec": refresh.get("refresh_age_sec"),
                        "last_price": refresh.get("last_price"),
                        "range_room_ok": refresh.get("range_room_ok"),
                        "liquidity_ok": refresh.get("liquidity_ok"),
                        "reason_codes": refresh.get("reason_codes") or [],
                    },
                    "micro_lifecycle": lifecycle if not _without_micro_like(line) else None,
                },
            )

        line_payloads[line] = {
            "run_id": plan_doc.get("run_id") if selected_line else run_id,
            "cycle_id": plan_doc.get("cycle_id") if selected_line else cycle_id,
            "status": plan_doc.get("status") if selected_line else "skipped_not_selected",
            "selected": selected_line,
            "count": plan_doc.get("count", len(plan_rows)) if selected_line else 0,
            "executable_count": plan_doc.get("executable_count", sum(1 for row in plan_rows if row["executable"])) if selected_line else 0,
            "symbols": sorted(line_symbols),
            "action_distribution": dict(counts),
            "steps": line_steps,
            "stage_status": stage_status,
            "artifact_refs": {
                "trade_plan": artifacts[f"trade_plan_{line}"],
                "trade_plan_archive": {
                    "exists": bool(refs.get("trade_plan_archive_path") and Path(str(refs.get("trade_plan_archive_path"))).is_file()),
                    "path": refs.get("trade_plan_archive_path"),
                    "manifest_path": refs.get("trade_plan_archive_manifest_path"),
                    "source_plan_hashes": refs.get("trade_plan_source_plan_hashes") or [],
                },
                "refresh": artifacts[f"refresh_{line}"],
                "micro_lifecycle": artifacts.get(f"micro_lifecycle_{line}"),
            },
        }
        if selected_line and line_payloads[line]["executable_count"] == 0:
            warnings.append({"scope": line, "name": "line.no_executable", "severity": "warn", "detail": line_payloads[line]["status"]})

    paper = _paper_rows(root, run_id)
    feishu = _feishu_rows(root, run_id)
    downstream = {
        "paper": {
            "order_count": len(paper.get("orders") or []),
            "position_count": len(paper.get("positions") or []),
            "fill_count": len(paper.get("fills") or []),
            "rows": paper,
        },
        "feishu": {
            "delivery_count": len(feishu.get("deliveries") or []),
            "latest_status": (feishu.get("latest_report") or {}).get("status") if isinstance(feishu.get("latest_report"), dict) else None,
            "latest_report": feishu.get("latest_report"),
            "deliveries": feishu.get("deliveries"),
        },
    }
    paper_non_exec = []
    executable_symbols = {(row["strategy_line"], row["symbol"]) for row in symbol_rows if row.get("executable")}
    for order in paper.get("orders") or []:
        line = order.get("strategy_line")
        sym = order.get("symbol")
        if line and sym and (line, sym) not in executable_symbols:
            paper_non_exec.append({"strategy_line": line, "symbol": sym, "order_id": order.get("order_id") or order.get("id")})
    if paper_non_exec:
        failures.append({"scope": "paper", "name": "paper.non_executable_consumed", "severity": "p0", "detail": paper_non_exec[:20]})
    lifecycle_docs = {
        "micro_fast": docs.get("micro_lifecycle_micro_fast"),
        "micro_full": docs.get("micro_lifecycle_micro_full"),
    }
    consumable_funnel = _build_consumable_to_executable_funnel(
        line_payloads=line_payloads,
        symbol_rows=symbol_rows,
        downstream=downstream,
        lifecycle_docs=lifecycle_docs,
    )
    sidecar_lines = _strategy4_sidecar_lines(root, run_id=run_id, cycle_id=cycle_id, artifacts=artifacts)
    for line, row in consumable_funnel.items():
        if row.get("consumable_missing_plan_symbols"):
            warnings.append(
                {
                    "scope": line,
                    "name": "funnel.consumable_missing_plan",
                    "severity": "warn",
                    "detail": row.get("consumable_missing_plan_symbols"),
                },
            )
        if row.get("executable_missing_paper_symbols"):
            detail = {
                "symbols": row.get("executable_missing_paper_symbols"),
                "paper_settlement_status": paper_settlement_status,
                "paper_settlement": paper_settlement,
            }
            if paper_settlement_status in PAPER_SETTLEMENT_FINAL_MISSING_STATUSES or paper_settlement_status in PAPER_SETTLEMENT_COMPLETE_STATUSES:
                failures.append(
                    {
                        "scope": line,
                        "name": "funnel.executable_missing_paper",
                        "severity": "p0",
                        "detail": detail,
                    },
                )
            elif paper_settlement_status in PAPER_SETTLEMENT_PENDING_STATUSES:
                warnings.append(
                    {
                        "scope": line,
                        "name": "funnel.executable_missing_paper_pending_settlement",
                        "severity": "warn",
                        "detail": detail,
                    },
                )
            else:
                warnings.append(
                    {
                        "scope": line,
                        "name": "funnel.executable_missing_paper_pending_settlement",
                        "severity": "warn",
                        "detail": detail,
                    },
                )

    for row in warnings:
        row.setdefault("warning_class", _warning_class(row))
    for row in failures:
        row.setdefault("failure_class", "technical")

    status = "failed" if failures else ("warning" if warnings else "ok")
    business_warning_count = sum(1 for row in warnings if row.get("warning_class") in {"business", "explained_skip"})
    contract_warning_count = len(warnings) - business_warning_count
    return {
        "schema_version": "7.14",
        "source": "run_level_chain_audit",
        "generated_at": generated_at,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "status": status,
        "failure_count": len(failures),
        "warning_count": len(warnings),
        "technical_failure_count": len(failures),
        "business_warning_count": business_warning_count,
        "contract_warning_count": contract_warning_count,
        "failures": failures,
        "warnings": warnings,
        "shared_steps": shared_steps,
        "strategy_lines": line_payloads,
        "sidecar_lines": sidecar_lines,
        "symbols": symbol_rows,
        "downstream": downstream,
        "paper_settlement": paper_settlement,
        "consumable_to_executable_funnel": consumable_funnel,
        "artifact_refs": artifacts,
        "summary": {
            "candidate_symbol_count": len(candidate_symbols),
            "symbol_row_count": len(symbol_rows),
            "line_status": {line: line_payloads[line]["status"] for line in PIPELINE_STRATEGY_LINES},
            "line_selected": {line: line_payloads[line].get("selected") for line in PIPELINE_STRATEGY_LINES},
            "executable_count": {line: line_payloads[line]["executable_count"] for line in PIPELINE_STRATEGY_LINES},
            "sidecar_line_status": {
                line: {
                    "daemon_state": row.get("daemon_state"),
                    "pool_count": row.get("pool_count"),
                    "attempt_count": row.get("attempt_count"),
                    "executable_count": (row.get("latest_trade_plan") or {}).get("executable_count"),
                    "pipeline_selected": row.get("pipeline_selected"),
                }
                for line, row in sidecar_lines.items()
            },
            "technical_failure_count": len(failures),
            "business_warning_count": business_warning_count,
            "contract_warning_count": contract_warning_count,
            "paper_settlement_status": paper_settlement_status,
            "paper_settlement": paper_settlement,
            "pending_paper_settlement_count": sum(
                1 for row in warnings if row.get("name") == "funnel.executable_missing_paper_pending_settlement"
            ),
            "funnel": {
                line: {
                    key: row.get(key)
                    for key in ("lifecycle_consumable_count", "generated_plan_count", "executable_count", "paper_order_count", "closed_count", "net_pnl")
                }
                for line, row in consumable_funnel.items()
            },
        },
    }


def write_run_level_audit(
    project_root: Path | None = None,
    *,
    output_path: Path | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
) -> dict[str, Any]:
    root = _root(project_root)
    payload = build_run_level_audit(root, run_id=run_id, cycle_id=cycle_id)
    latest = output_path or root / "DATA/reports/latest_run_audit.json"
    write_json_atomic(latest, payload)
    if payload.get("run_id"):
        run_dir = root / "DATA/reports/runs" / str(payload["run_id"])
        write_json_atomic(run_dir / "run_audit.json", payload)
    return payload


def run_build_run_audit_safe(
    *,
    project_root: Path | None = None,
    output_path: Path | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    stdout_json: bool = False,
) -> int:
    try:
        payload = write_run_level_audit(project_root, output_path=output_path, run_id=run_id, cycle_id=cycle_id)
        if stdout_json:
            print(json.dumps(payload, ensure_ascii=False))
        else:
            print(f"run audit status={payload.get('status')} run_id={payload.get('run_id')}")
        return 0
    except Exception as exc:
        if stdout_json:
            print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False))
        else:
            print(f"run audit failed: {exc}")
        return 1


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_run_audit_db(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.executescript(
            """
            create table if not exists audit_runs (
              run_id text primary key,
              cycle_id text,
              status text,
              generated_at text,
              failure_count integer,
              warning_count integer,
              summary_json text,
              payload_json text
            );
            create table if not exists audit_artifacts (
              run_id text not null,
              artifact_key text not null,
              path text,
              exists_flag integer,
              generated_at text,
              sha256 text,
              source text,
              run_id_in_file text,
              cycle_id_in_file text,
              json_text text,
              primary key (run_id, artifact_key)
            );
            create table if not exists audit_steps (
              run_id text not null,
              scope text not null,
              name text not null,
              status text,
              severity text,
              detail_json text
            );
            create table if not exists audit_symbols (
              run_id text not null,
              cycle_id text,
              strategy_line text,
              symbol text,
              decision text,
              action text,
              entry_mode text,
              executable integer,
              status text,
              reason_codes_json text,
              payload_json text
            );
            create table if not exists audit_downstream_events (
              run_id text not null,
              event_type text,
              strategy_line text,
              symbol text,
              status text,
              payload_json text
            );
            """
        )


def ingest_failed_pipeline_run_to_sqlite(
    project_root: Path | None = None,
    *,
    report: dict[str, Any],
    db_path: Path | None = None,
) -> dict[str, Any]:
    root = _root(project_root)
    run_id = str(report.get("run_id") or "")
    if not run_id:
        raise ValueError("failed pipeline report missing run_id")
    status = str(report.get("status") or "")
    if status != "failed":
        raise ValueError(f"failed pipeline report status must be failed, got {status!r}")
    db = db_path or root / "DATA/audit/run_audit.db"
    init_run_audit_db(db)
    first_failed_stage = report.get("first_failed_stage")
    summary = {
        "source": "failed_pipeline_minimal_ledger",
        "line": report.get("line"),
        "selected_lines": report.get("selected_lines") or [],
        "skipped_lines": report.get("skipped_lines") or [],
        "first_failed_stage": first_failed_stage,
        "first_failed_stage_status": report.get("first_failed_stage_status"),
        "first_failed_stage_rc": report.get("first_failed_stage_rc"),
        "failure_domain": report.get("failure_domain") or "pipeline_stage",
        "failure_reason": report.get("failure_reason") or report.get("exception_summary"),
        "pipeline_report_path": ((report.get("outputs") or {}).get("pipeline_report_archive")),
        "latest_report_path": ((report.get("outputs") or {}).get("strategy_report")),
        "micro_runtime_rows": 0,
        "micro_runtime_missing_reason": "pipeline_failed_before_micro_runtime_ingest",
    }
    with _connect(db) as conn:
        conn.execute("delete from audit_downstream_events where run_id = ? and event_type = ?", (run_id, "pipeline_failed"))
        conn.execute(
            """
            insert into audit_runs(run_id, cycle_id, status, generated_at, failure_count, warning_count, summary_json, payload_json)
            values(?,?,?,?,?,?,?,?)
            on conflict(run_id) do update set
              cycle_id=excluded.cycle_id,
              status=excluded.status,
              generated_at=excluded.generated_at,
              failure_count=excluded.failure_count,
              warning_count=excluded.warning_count,
              summary_json=excluded.summary_json,
              payload_json=excluded.payload_json
            """,
            (
                run_id,
                report.get("cycle_id"),
                "failed",
                report.get("generated_at") or report.get("finished_at") or to_iso_z(utc_now()),
                1,
                0,
                json.dumps(summary, ensure_ascii=False),
                json.dumps(report, ensure_ascii=False),
            ),
        )
        conn.execute(
            "insert into audit_downstream_events(run_id, event_type, strategy_line, symbol, status, payload_json) values(?,?,?,?,?,?)",
            (
                run_id,
                "pipeline_failed",
                report.get("line") or "all",
                None,
                "failed",
                json.dumps(summary, ensure_ascii=False),
            ),
        )
    return {"status": "ok", "db_path": str(db), "run_id": run_id, "event_type": "pipeline_failed"}


def ingest_run_audit_to_sqlite(project_root: Path | None = None, *, audit_path: Path | None = None, db_path: Path | None = None) -> dict[str, Any]:
    root = _root(project_root)
    audit_file = audit_path or root / "DATA/reports/latest_run_audit.json"
    payload = _read_json(audit_file)
    if not isinstance(payload, dict):
        raise ValueError(f"run audit JSON missing or invalid: {audit_file}")
    run_id = str(payload.get("run_id") or "")
    if not run_id:
        raise ValueError("run audit payload missing run_id")
    db = db_path or root / "DATA/audit/run_audit.db"
    init_run_audit_db(db)
    with _connect(db) as conn:
        conn.execute("delete from audit_steps where run_id = ?", (run_id,))
        conn.execute("delete from audit_symbols where run_id = ?", (run_id,))
        conn.execute("delete from audit_downstream_events where run_id = ?", (run_id,))
        conn.execute(
            """
            insert into audit_runs(run_id, cycle_id, status, generated_at, failure_count, warning_count, summary_json, payload_json)
            values(?,?,?,?,?,?,?,?)
            on conflict(run_id) do update set
              cycle_id=excluded.cycle_id,
              status=excluded.status,
              generated_at=excluded.generated_at,
              failure_count=excluded.failure_count,
              warning_count=excluded.warning_count,
              summary_json=excluded.summary_json,
              payload_json=excluded.payload_json
            """,
            (
                run_id,
                payload.get("cycle_id"),
                payload.get("status"),
                payload.get("generated_at"),
                int(payload.get("failure_count") or 0),
                int(payload.get("warning_count") or 0),
                json.dumps(payload.get("summary") or {}, ensure_ascii=False),
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        for key, meta in (payload.get("artifact_refs") or {}).items():
            path = Path(str(meta.get("path") or ""))
            json_text = None
            if path.exists() and path.is_file():
                try:
                    json_text = path.read_text(encoding="utf-8", errors="replace")
                except OSError:
                    json_text = None
            conn.execute(
                """
                insert into audit_artifacts(run_id, artifact_key, path, exists_flag, generated_at, sha256, source, run_id_in_file, cycle_id_in_file, json_text)
                values(?,?,?,?,?,?,?,?,?,?)
                on conflict(run_id, artifact_key) do update set
                  path=excluded.path,
                  exists_flag=excluded.exists_flag,
                  generated_at=excluded.generated_at,
                  sha256=excluded.sha256,
                  source=excluded.source,
                  run_id_in_file=excluded.run_id_in_file,
                  cycle_id_in_file=excluded.cycle_id_in_file,
                  json_text=excluded.json_text
                """,
                (
                    run_id,
                    key,
                    meta.get("path"),
                    1 if meta.get("exists") else 0,
                    meta.get("generated_at"),
                    meta.get("sha256"),
                    meta.get("source"),
                    meta.get("run_id"),
                    meta.get("cycle_id"),
                    json_text,
                ),
            )
        for step in payload.get("shared_steps") or []:
            conn.execute(
                "insert into audit_steps(run_id, scope, name, status, severity, detail_json) values(?,?,?,?,?,?)",
                (run_id, "shared", step.get("name"), step.get("status"), step.get("severity"), json.dumps(step.get("detail"), ensure_ascii=False)),
            )
        for line, line_payload in (payload.get("strategy_lines") or {}).items():
            for step in line_payload.get("steps") or []:
                conn.execute(
                    "insert into audit_steps(run_id, scope, name, status, severity, detail_json) values(?,?,?,?,?,?)",
                    (
                        run_id,
                        line,
                        step.get("name"),
                        step.get("status"),
                        step.get("severity"),
                        json.dumps(step.get("detail"), ensure_ascii=False),
                    ),
                )
        for row in payload.get("symbols") or []:
            conn.execute(
                """
                insert into audit_symbols(run_id, cycle_id, strategy_line, symbol, decision, action, entry_mode, executable, status, reason_codes_json, payload_json)
                values(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    row.get("cycle_id"),
                    row.get("strategy_line"),
                    row.get("symbol"),
                    row.get("decision"),
                    row.get("action"),
                    row.get("entry_mode"),
                    1 if row.get("executable") else 0,
                    row.get("status"),
                    json.dumps(row.get("reason_codes") or [], ensure_ascii=False),
                    json.dumps(row, ensure_ascii=False),
                ),
            )
        for row in ((payload.get("downstream") or {}).get("feishu") or {}).get("deliveries") or []:
            conn.execute(
                "insert into audit_downstream_events(run_id, event_type, strategy_line, symbol, status, payload_json) values(?,?,?,?,?,?)",
                (
                    run_id,
                    row.get("event_type"),
                    row.get("strategy_line"),
                    row.get("symbol"),
                    row.get("status"),
                    json.dumps(row, ensure_ascii=False),
                ),
            )
        for row in (((payload.get("downstream") or {}).get("paper") or {}).get("rows") or {}).get("orders") or []:
            conn.execute(
                "insert into audit_downstream_events(run_id, event_type, strategy_line, symbol, status, payload_json) values(?,?,?,?,?,?)",
                (
                    run_id,
                    "paper_order",
                    row.get("strategy_line"),
                    row.get("symbol"),
                    row.get("status"),
                    json.dumps(row, ensure_ascii=False),
                ),
            )
    return {"status": "ok", "db_path": str(db), "run_id": run_id, "symbol_count": len(payload.get("symbols") or [])}


def run_ingest_run_audit_safe(
    *,
    project_root: Path | None = None,
    audit_path: Path | None = None,
    db_path: Path | None = None,
    stdout_json: bool = False,
) -> int:
    try:
        payload = ingest_run_audit_to_sqlite(project_root, audit_path=audit_path, db_path=db_path)
        if stdout_json:
            print(json.dumps(payload, ensure_ascii=False))
        else:
            print(f"run audit ingested run_id={payload.get('run_id')} db={payload.get('db_path')}")
        return 0
    except Exception as exc:
        if stdout_json:
            print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False))
        else:
            print(f"run audit ingest failed: {exc}")
        return 1


def _row_payload(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    for key in ("payload_json", "summary_json", "detail_json", "reason_codes_json"):
        if key in data and isinstance(data[key], str):
            try:
                data[key.replace("_json", "")] = json.loads(data[key])
            except json.JSONDecodeError:
                pass
    return data


def list_run_audits(project_root: Path | None = None, *, limit: int = 20, status: str | None = None) -> dict[str, Any]:
    root = _root(project_root)
    db = root / "DATA/audit/run_audit.db"
    if db.exists():
        with _connect(db) as conn:
            if status:
                rows = conn.execute(
                    "select run_id, cycle_id, status, generated_at, failure_count, warning_count, summary_json from audit_runs where status = ? order by generated_at desc limit ?",
                    (status, int(limit)),
                ).fetchall()
            else:
                rows = conn.execute(
                    "select run_id, cycle_id, status, generated_at, failure_count, warning_count, summary_json from audit_runs order by generated_at desc limit ?",
                    (int(limit),),
                ).fetchall()
        return {"source": "sqlite", "db_path": str(db), "runs": [_row_payload(row) for row in rows]}
    latest = root / "DATA/reports/latest_run_audit.json"
    doc = _read_json(latest)
    runs = [doc] if isinstance(doc, dict) else []
    return {"source": "json_fallback", "runs": runs[:limit]}


def get_run_audit(project_root: Path | None = None, *, run_id: str | None = None) -> dict[str, Any]:
    root = _root(project_root)
    db = root / "DATA/audit/run_audit.db"
    if db.exists():
        with _connect(db) as conn:
            row = None
            if run_id:
                row = conn.execute("select payload_json from audit_runs where run_id = ?", (run_id,)).fetchone()
            else:
                row = conn.execute("select payload_json from audit_runs order by generated_at desc limit 1").fetchone()
            if row and row["payload_json"]:
                payload = json.loads(row["payload_json"])
                payload["_source"] = "sqlite"
                payload["_db_path"] = str(db)
                return payload
    path = root / "DATA/reports/latest_run_audit.json" if not run_id else root / "DATA/reports/runs" / run_id / "run_audit.json"
    doc = _read_json(path)
    if isinstance(doc, dict):
        doc["_source"] = "json_fallback"
        doc["_path"] = str(path)
        return doc
    raise FileNotFoundError(f"run audit not found: {run_id or 'latest'}")
