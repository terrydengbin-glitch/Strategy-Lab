"""STEP35 candidate signal / gate / result sidecar ledger.

This module is intentionally sidecar-only.  It reads paper ledgers and trade
plan snapshots in read-only mode, then writes candidate/gate/result artifacts
under DATA/research/candidate_ledger or a sandbox run mirror directory.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.decision.trade_plan_archive import trade_plan_source_plan_hash
from laoma_signal_engine.paper.models import Candle, PaperConfig
from laoma_signal_engine.paper.fill_model import adverse_fill_price, paper_pnl


SCHEMA_VERSION = "step35_candidate_gate_result_ledger_v1"
FEATURE_SCHEMA_VERSION = "step29_decision_time_input_v2"
KNOWN_AT_POLICY_VERSION = "step29_known_at_policy_v2"
CANDIDATE_SET_HASH_ALGORITHM = "step35_candidate_set_hash_v1"
SIDE_CAR_ONLY = True
FORBIDDEN_DECISION_FEATURE_FIELDS = {
    "net_R",
    "MFE_R",
    "MAE_R",
    "pnl",
    "exit_price",
    "exit_time",
    "exit_reason",
    "quality_label",
    "root_cause_label",
    "realized_slippage",
    "realized_fee",
    "future_bar_high",
    "future_bar_low",
}

DDL = [
    """
    CREATE TABLE IF NOT EXISTS trade_candidates (
      candidate_id TEXT PRIMARY KEY,
      run_id TEXT NOT NULL,
      sandbox_id TEXT,
      pipeline_run_id TEXT,
      source_mode TEXT NOT NULL,
      strategy_line TEXT NOT NULL,
      strategy_version TEXT,
      symbol TEXT NOT NULL,
      side TEXT NOT NULL,
      decision_time_ms INTEGER,
      candle_open_time_ms INTEGER,
      source_plan_hash TEXT NOT NULL,
      candidate_status TEXT NOT NULL,
      candidate_reason_codes_json TEXT NOT NULL DEFAULT '[]',
      intended_order_type TEXT,
      intended_size REAL,
      entry_price_hint REAL,
      limit_price REAL,
      stop_loss REAL,
      take_profit REAL,
      planned_rr REAL,
      decision_time_features_json TEXT NOT NULL DEFAULT '{}',
      price_context_json TEXT NOT NULL DEFAULT '{}',
      risk_context_json TEXT NOT NULL DEFAULT '{}',
      cost_context_json TEXT NOT NULL DEFAULT '{}',
      market_regime_ref TEXT,
      feature_schema_version TEXT,
      known_at_policy_version TEXT,
      feature_timestamp_cutoff INTEGER,
      known_at_pass INTEGER NOT NULL DEFAULT 0,
      source_refs_json TEXT NOT NULL DEFAULT '[]',
      field_lineage_json TEXT NOT NULL DEFAULT '{}',
      source_json TEXT NOT NULL DEFAULT '{}',
      missing_fields_json TEXT NOT NULL DEFAULT '[]',
      created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gate_decisions (
      gate_decision_id TEXT PRIMARY KEY,
      candidate_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      gate_source TEXT NOT NULL,
      gate_policy_version TEXT,
      gate_decision TEXT NOT NULL,
      gate_reason_codes_json TEXT NOT NULL DEFAULT '[]',
      gate_rule_hits_json TEXT NOT NULL DEFAULT '{}',
      threshold_policy_version TEXT,
      score_ref TEXT,
      bad_trade_risk REAL,
      calibrated_probability REAL,
      original_size REAL,
      adjusted_size REAL,
      size_multiplier REAL,
      decision_time_ms INTEGER,
      decided_at_ms INTEGER,
      audit_trace_id TEXT,
      idempotency_key TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(candidate_id) REFERENCES trade_candidates(candidate_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trade_results (
      trade_result_id TEXT PRIMARY KEY,
      candidate_id TEXT NOT NULL,
      order_id TEXT,
      position_id TEXT,
      run_id TEXT NOT NULL,
      gated_run_id TEXT,
      execution_source TEXT NOT NULL,
      executed INTEGER NOT NULL DEFAULT 0,
      not_executed_reason TEXT,
      entry_time_ms INTEGER,
      exit_time_ms INTEGER,
      entry_price REAL,
      exit_price REAL,
      quantity REAL,
      fee_bps REAL,
      realized_slippage_bps REAL,
      net_R REAL,
      MFE_R REAL,
      MAE_R REAL,
      holding_time_sec REAL,
      exit_reason TEXT,
      root_cause_label TEXT,
      quality_label TEXT,
      label_policy_version TEXT,
      outcome_source TEXT NOT NULL,
      outcome_confidence TEXT NOT NULL,
      result_refs_json TEXT NOT NULL DEFAULT '[]',
      created_at TEXT NOT NULL,
      FOREIGN KEY(candidate_id) REFERENCES trade_candidates(candidate_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS candidate_order_links (
      link_id TEXT PRIMARY KEY,
      candidate_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      sandbox_id TEXT,
      pipeline_run_id TEXT,
      strategy_line TEXT NOT NULL,
      symbol TEXT NOT NULL,
      source_plan_hash TEXT NOT NULL,
      intent_id TEXT,
      order_id TEXT,
      position_id TEXT,
      skip_ledger_id TEXT,
      result_id TEXT,
      link_status TEXT NOT NULL,
      link_confidence TEXT NOT NULL,
      created_at TEXT NOT NULL,
      FOREIGN KEY(candidate_id) REFERENCES trade_candidates(candidate_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS candidate_ledger_manifest (
      manifest_id TEXT PRIMARY KEY,
      run_id TEXT NOT NULL,
      source_mode TEXT NOT NULL,
      schema_version TEXT NOT NULL,
      manifest_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS candidate_ledger_audit (
      audit_id TEXT PRIMARY KEY,
      run_id TEXT NOT NULL,
      audit_type TEXT NOT NULL,
      audit_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_step35_candidates_run_line_symbol_time ON trade_candidates(run_id, source_mode, strategy_line, symbol, decision_time_ms)",
    "CREATE INDEX IF NOT EXISTS idx_step35_candidates_sandbox_run ON trade_candidates(sandbox_id, run_id)",
    "CREATE INDEX IF NOT EXISTS idx_step35_links_run_status ON candidate_order_links(run_id, link_status)",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def stable_id(prefix: str, *parts: Any, size: int = 32) -> str:
    return f"{prefix}_{stable_hash(parts)[:size]}"


def safe_part(value: Any, default: str = "unknown") -> str:
    text = re.sub(r"[^A-Za-z0-9_.:-]+", "_", str(value or default)).strip("_")
    return text or default


def project_rel(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def parse_time_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except ValueError:
        return None


def read_json(raw: Any, default: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    if raw in (None, ""):
        return default
    try:
        return json.loads(str(raw))
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def candidate_ledger_dir(root: Path, *, run_id: str, source_mode: str, sandbox_id: str | None = None) -> Path:
    base = Path(root) / "DATA" / "research" / "candidate_ledger"
    if sandbox_id:
        return base / "sandbox_exports" / safe_part(sandbox_id) / safe_part(run_id)
    if str(source_mode).startswith("baseline"):
        return base / "baseline" / safe_part(run_id)
    return base / safe_part(source_mode) / safe_part(run_id)


def sandbox_candidate_ledger_mirror_dir(
    root: Path,
    *,
    sandbox_id: str | None,
    run_id: str,
    run_root_rel: str | None = None,
) -> Path | None:
    if run_root_rel:
        return Path(root) / run_root_rel / "candidate_ledger"
    if sandbox_id:
        return Path(root) / "DATA" / "sandboxes" / str(sandbox_id) / "runtime" / "pipeline_runs" / str(run_id) / "candidate_ledger"
    return None


def connect_candidate_ledger(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    for ddl in DDL:
        con.execute(ddl)
    con.commit()
    return con


def connect_ro(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _rows(con: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    if not _table_exists(con, table):
        return []
    return [dict(row) for row in con.execute(f"SELECT * FROM {table}").fetchall()]


def _doc_plan_candidates(
    docs: dict[str, dict[str, Any]] | None,
    *,
    run_id: str,
    sandbox_id: str | None,
    source_mode: str,
    cycle_id: str | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for line, doc in (docs or {}).items():
        if not isinstance(doc, dict):
            continue
        for idx, plan in enumerate(doc.get("plans") or []):
            if not isinstance(plan, dict):
                continue
            source_plan_hash = trade_plan_source_plan_hash(line, doc, plan)
            symbol = str(plan.get("symbol") or "").upper()
            side = str(plan.get("decision") or plan.get("side") or "").upper()
            guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
            position_sizing = plan.get("position_sizing") if isinstance(plan.get("position_sizing"), dict) else {}
            decision_time_ms = parse_time_ms(plan.get("decision_time_ms") or doc.get("generated_at"))
            candidate_id = stable_id(
                "cand",
                run_id,
                sandbox_id,
                source_mode,
                line,
                symbol,
                side,
                decision_time_ms,
                source_plan_hash,
                idx,
            )
            reason_codes = list(plan.get("reason_codes") or [])
            executable = bool(plan.get("executable"))
            entry = _float(plan.get("estimated_entry_price") or plan.get("entry_price"))
            stop_loss = _float(plan.get("stop_loss"))
            take_profit = _float(plan.get("take_profit"))
            planned_rr = _float(plan.get("rr") or plan.get("planned_rr"))
            source_refs = [
                {
                    "source_table": "trade_plan_document",
                    "source_row_id": f"{line}:{idx}",
                    "source_time_ms": decision_time_ms,
                    "source_hash": stable_hash(plan),
                    "source_db_path": None,
                }
            ]
            out[(str(line), source_plan_hash)] = {
                "candidate_id": candidate_id,
                "run_id": run_id,
                "sandbox_id": sandbox_id,
                "pipeline_run_id": run_id,
                "source_mode": source_mode,
                "strategy_line": str(line),
                "strategy_version": str(doc.get("schema_version") or ""),
                "symbol": symbol,
                "side": side,
                "decision_time_ms": decision_time_ms,
                "candle_open_time_ms": _int(plan.get("candle_open_time_ms")),
                "source_plan_hash": source_plan_hash,
                "candidate_status": "generated" if executable else "source_gate_blocked",
                "candidate_reason_codes_json": reason_codes,
                "intended_order_type": str(plan.get("entry_mode") or plan.get("order_type") or ""),
                "intended_size": _float(position_sizing.get("planned_quantity") or position_sizing.get("quantity")),
                "entry_price_hint": entry,
                "limit_price": _float(plan.get("limit_price")),
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "planned_rr": planned_rr,
                "decision_time_features_json": _decision_features(plan, guards),
                "price_context_json": {"entry_price_hint": entry, "stop_loss": stop_loss, "take_profit": take_profit},
                "risk_context_json": {"position_sizing": position_sizing, "guards": guards},
                "cost_context_json": _dict_subset(guards, ["spread_bps", "slippage_bps", "estimated_slippage_bps"]),
                "market_regime_ref": str(guards.get("market_regime_ref") or ""),
                "feature_schema_version": str(plan.get("feature_schema_version") or ""),
                "known_at_policy_version": str(plan.get("known_at_policy_version") or ""),
                "feature_timestamp_cutoff": decision_time_ms,
                "known_at_pass": True,
                "source_refs_json": source_refs,
                "field_lineage_json": {},
                "source_json": plan | {"source_doc": _dict_subset(doc, ["schema_version", "run_id", "cycle_id", "generated_at", "source"])},
                "missing_fields_json": [],
                "_plan_index": idx,
                "_cycle_id": cycle_id or doc.get("cycle_id"),
            }
    return out


def _decision_features(plan: dict[str, Any], guards: dict[str, Any]) -> dict[str, Any]:
    features: dict[str, Any] = {}
    for key in ("confidence", "rr", "decision_tf"):
        if key in plan:
            features[key] = plan.get(key)
    for key in ("symbol_execution_tier", "liquidity_tier", "spread_bps", "market_regime_ref"):
        if key in guards:
            features[key] = guards.get(key)
    return features


def _dict_subset(row: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    return {key: row.get(key) for key in keys if key in row}


def _int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _merge_paper_rows(
    con: sqlite3.Connection,
    candidates: dict[tuple[str, str], dict[str, Any]],
    *,
    root: Path,
    source_db_path: Path,
    run_id: str,
    sandbox_id: str | None,
    source_mode: str,
) -> None:
    for table in ("paper_trade_plans", "paper_orders", "paper_skip_ledger"):
        for row in _rows(con, table):
            line = str(row.get("strategy_line") or "")
            source_plan_hash = str(row.get("source_plan_hash") or "")
            if not line or not source_plan_hash:
                continue
            key = (line, source_plan_hash)
            source_json = read_json(row.get("source_json"), {})
            plan = source_json if isinstance(source_json, dict) else {}
            symbol = str(row.get("symbol") or plan.get("symbol") or "").upper()
            side = str(row.get("side") or plan.get("decision") or plan.get("side") or "").upper()
            decision_time_ms = parse_time_ms(row.get("source_generated_at") or row.get("created_at"))
            candidate = candidates.get(key)
            if candidate is None:
                candidate_id = stable_id(
                    "cand",
                    run_id,
                    sandbox_id,
                    source_mode,
                    line,
                    symbol,
                    side,
                    decision_time_ms,
                    source_plan_hash,
                    table,
                )
                candidate = {
                    "candidate_id": candidate_id,
                    "run_id": run_id,
                    "sandbox_id": sandbox_id,
                    "pipeline_run_id": run_id,
                    "source_mode": source_mode,
                    "strategy_line": line,
                    "strategy_version": "",
                    "symbol": symbol,
                    "side": side,
                    "decision_time_ms": decision_time_ms,
                    "candle_open_time_ms": None,
                    "source_plan_hash": source_plan_hash,
                    "candidate_status": "generated",
                    "candidate_reason_codes_json": [],
                    "intended_order_type": str(row.get("order_type") or plan.get("entry_mode") or ""),
                    "intended_size": _float(row.get("planned_quantity")),
                    "entry_price_hint": _float(row.get("entry_price") or plan.get("estimated_entry_price")),
                    "limit_price": _float(plan.get("limit_price")),
                    "stop_loss": _float(row.get("stop_loss") or plan.get("stop_loss")),
                    "take_profit": _float(row.get("take_profit") or plan.get("take_profit")),
                    "planned_rr": _float(plan.get("rr")),
                    "decision_time_features_json": _decision_features(plan, plan.get("guards") if isinstance(plan.get("guards"), dict) else {}),
                    "price_context_json": {},
                    "risk_context_json": {},
                    "cost_context_json": {},
                    "market_regime_ref": "",
                    "feature_schema_version": "",
                    "known_at_policy_version": "",
                    "feature_timestamp_cutoff": decision_time_ms,
                    "known_at_pass": True,
                    "source_refs_json": [],
                    "field_lineage_json": {},
                    "source_json": plan,
                    "missing_fields_json": [],
                }
                candidates[key] = candidate
            candidate["source_refs_json"].append(
                {
                    "source_db_path": project_rel(root, source_db_path),
                    "source_table": table,
                    "source_row_id": str(row.get("id") or source_plan_hash),
                    "source_time_ms": decision_time_ms,
                    "source_hash": stable_hash(dict(row)),
                }
            )
            if table == "paper_orders":
                candidate["candidate_status"] = "executed"
                candidate["known_at_pass"] = True
            elif table == "paper_skip_ledger" and candidate.get("candidate_status") != "executed":
                candidate["candidate_status"] = "source_gate_blocked"
                candidate["candidate_reason_codes_json"] = sorted(
                    set(list(candidate.get("candidate_reason_codes_json") or []) + [str(row.get("skip_reason") or "skipped")])
                )
                if not candidate.get("source_json"):
                    candidate["source_json"] = plan


def _normalize_gate_decision(raw: Any, *, blocked: bool) -> str:
    text = str(raw or "").lower()
    if text in {"allow", "allowed", "pass", "passed"}:
        return "allow"
    if text in {"block", "blocked", "reject", "rejected"}:
        return "block"
    if text in {"reduce", "reduce_size"}:
        return "reduce_size"
    if text == "review":
        return "review"
    return "block" if blocked else "allow"


def _gate_records(candidates: list[dict[str, Any]], skips_by_key: dict[tuple[str, str], dict[str, Any]], orders_by_key: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    created = now_iso()
    for cand in candidates:
        key = (str(cand["strategy_line"]), str(cand["source_plan_hash"]))
        skip = skips_by_key.get(key)
        order = orders_by_key.get(key)
        blocked = skip is not None or str(cand.get("candidate_status")) == "source_gate_blocked"
        source_row = skip or order or {}
        gate_decision = _normalize_gate_decision(source_row.get("gate_decision"), blocked=blocked)
        reason_codes = list(cand.get("candidate_reason_codes_json") or [])
        if skip and skip.get("skip_reason"):
            reason_codes.append(str(skip["skip_reason"]))
        out.append(
            {
                "gate_decision_id": stable_id("gate", cand["candidate_id"], "source_rule_gate"),
                "candidate_id": cand["candidate_id"],
                "run_id": cand["run_id"],
                "gate_source": "source_rule_gate",
                "gate_policy_version": str(source_row.get("gate_candidate_id") or "source_rule_gate_v1"),
                "gate_decision": gate_decision,
                "gate_reason_codes_json": sorted(set(reason_codes)),
                "gate_rule_hits_json": read_json(source_row.get("gate_rule_json"), {}),
                "threshold_policy_version": str(source_row.get("gate_candidate_id") or ""),
                "score_ref": None,
                "bad_trade_risk": None,
                "calibrated_probability": None,
                "original_size": _float(cand.get("intended_size")),
                "adjusted_size": _float(cand.get("intended_size")) if gate_decision == "allow" else 0.0,
                "size_multiplier": 1.0 if gate_decision == "allow" else 0.0,
                "decision_time_ms": cand.get("decision_time_ms"),
                "decided_at_ms": cand.get("decision_time_ms"),
                "audit_trace_id": stable_id("audit", cand["candidate_id"], "source_rule_gate"),
                "idempotency_key": stable_id("idem", cand["candidate_id"], "source_rule_gate"),
                "created_at": created,
            }
        )
    return out


def _result_records(
    candidates: list[dict[str, Any]],
    *,
    orders_by_key: dict[tuple[str, str], dict[str, Any]],
    positions_by_order: dict[str, dict[str, Any]],
    fills_by_order: dict[str, list[dict[str, Any]]],
    tq_by_order: dict[str, dict[str, Any]],
    counterfactual_results_by_candidate: dict[str, dict[str, Any]],
    root: Path,
    source_db_path: Path,
    execution_source: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    created = now_iso()
    for cand in candidates:
        key = (str(cand["strategy_line"]), str(cand["source_plan_hash"]))
        order = orders_by_key.get(key)
        if order:
            order_id = str(order.get("id") or "")
            fills = fills_by_order.get(order_id, [])
            entry_fill = next((row for row in fills if str(row.get("action")).lower() == "entry"), None)
            exit_fill = next((row for row in fills if str(row.get("action")).lower() != "entry"), None)
            tq = tq_by_order.get(order_id, {})
            pnl = _float(order.get("realized_pnl_usdt"))
            result_refs = [
                {
                    "source_db_path": project_rel(root, source_db_path),
                    "source_table": "paper_orders",
                    "source_row_id": order_id,
                    "source_time_ms": parse_time_ms(order.get("closed_at") or order.get("updated_at") or order.get("created_at")),
                    "source_hash": stable_hash(order),
                }
            ]
            out.append(
                {
                    "trade_result_id": stable_id("result", cand["candidate_id"], order_id or "executed"),
                    "candidate_id": cand["candidate_id"],
                    "order_id": order_id,
                    "position_id": str((positions_by_order.get(order_id) or {}).get("id") or ""),
                    "run_id": cand["run_id"],
                    "gated_run_id": None,
                    "execution_source": execution_source,
                    "executed": True,
                    "not_executed_reason": None,
                    "entry_time_ms": parse_time_ms(order.get("opened_at") or (entry_fill or {}).get("filled_at")),
                    "exit_time_ms": parse_time_ms(order.get("closed_at") or (exit_fill or {}).get("filled_at")),
                    "entry_price": _float(order.get("filled_entry_price") or order.get("entry_price") or (entry_fill or {}).get("fill_price")),
                    "exit_price": _float(order.get("exit_price") or (exit_fill or {}).get("fill_price")),
                    "quantity": _float(order.get("quantity") or (entry_fill or {}).get("quantity")),
                    "fee_bps": _float((exit_fill or entry_fill or {}).get("fee_bps")),
                    "realized_slippage_bps": _float((exit_fill or entry_fill or {}).get("slippage_bps")),
                    "net_R": _float(tq.get("net_R")),
                    "MFE_R": _float(tq.get("MFE_R")),
                    "MAE_R": _float(tq.get("MAE_R")),
                    "holding_time_sec": _float(tq.get("holding_time_sec")),
                    "exit_reason": str(order.get("exit_reason") or tq.get("exit_reason") or ""),
                    "root_cause_label": str(tq.get("root_cause_label") or ""),
                    "quality_label": "winner" if (pnl or 0.0) > 0 else "loser" if pnl is not None else None,
                    "label_policy_version": "winner_loser_v1",
                    "outcome_source": "executed",
                    "outcome_confidence": "high",
                    "result_refs_json": result_refs,
                    "created_at": created,
                }
            )
        else:
            out.append(counterfactual_results_by_candidate.get(str(cand["candidate_id"])) or _no_outcome_result(cand, execution_source, created))
    return out


def _link_records(
    candidates: list[dict[str, Any]],
    *,
    orders_by_key: dict[tuple[str, str], dict[str, Any]],
    skips_by_key: dict[tuple[str, str], dict[str, Any]],
    intents_by_key: dict[tuple[str, str], dict[str, Any]],
    positions_by_order: dict[str, dict[str, Any]],
    results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    result_by_candidate = {str(row.get("candidate_id")): row for row in results}
    created = now_iso()
    links: list[dict[str, Any]] = []
    for cand in candidates:
        key = (str(cand.get("strategy_line")), str(cand.get("source_plan_hash")))
        order = orders_by_key.get(key) or {}
        skip = skips_by_key.get(key) or {}
        intent = intents_by_key.get(key) or {}
        order_id = str(order.get("id") or "")
        result = result_by_candidate.get(str(cand.get("candidate_id"))) or {}
        if order_id:
            link_status = "executed"
        elif skip or str(cand.get("candidate_status")) == "source_gate_blocked":
            link_status = "blocked" if str(cand.get("candidate_status")) == "source_gate_blocked" else "skipped"
        else:
            link_status = "no_order"
        links.append(
            {
                "link_id": stable_id("link", cand.get("candidate_id"), key),
                "candidate_id": cand.get("candidate_id"),
                "run_id": cand.get("run_id"),
                "sandbox_id": cand.get("sandbox_id"),
                "pipeline_run_id": cand.get("pipeline_run_id"),
                "strategy_line": cand.get("strategy_line"),
                "symbol": cand.get("symbol"),
                "source_plan_hash": cand.get("source_plan_hash"),
                "intent_id": str(intent.get("intent_id") or order.get("intent_id") or ""),
                "order_id": order_id,
                "position_id": str((positions_by_order.get(order_id) or {}).get("id") or result.get("position_id") or ""),
                "skip_ledger_id": str(skip.get("id") or ""),
                "result_id": str(result.get("trade_result_id") or ""),
                "link_status": link_status,
                "link_confidence": "exact",
                "created_at": created,
            }
        )
    return links


def _no_outcome_result(cand: dict[str, Any], execution_source: str, created: str) -> dict[str, Any]:
    return {
        "trade_result_id": stable_id("result", cand["candidate_id"], "no_outcome"),
        "candidate_id": cand["candidate_id"],
        "order_id": None,
        "position_id": None,
        "run_id": cand["run_id"],
        "gated_run_id": None,
        "execution_source": execution_source,
        "executed": False,
        "not_executed_reason": "counterfactual_replay_unavailable",
        "entry_time_ms": None,
        "exit_time_ms": None,
        "entry_price": None,
        "exit_price": None,
        "quantity": None,
        "fee_bps": None,
        "realized_slippage_bps": None,
        "net_R": None,
        "MFE_R": None,
        "MAE_R": None,
        "holding_time_sec": None,
        "exit_reason": None,
        "root_cause_label": None,
        "quality_label": None,
        "label_policy_version": None,
        "outcome_source": "not_executed_no_outcome",
        "outcome_confidence": "unknown",
        "result_refs_json": [],
        "created_at": created,
    }


class _CandidateHistoricalCandleProvider:
    historical_time_mode = True

    def __init__(self, candles_by_symbol: dict[str, list[Any]]) -> None:
        self._candles_by_symbol = {
            str(symbol).upper(): sorted((_as_candle(str(symbol), row) for row in rows), key=lambda item: item.open_time_ms)
            for symbol, rows in candles_by_symbol.items()
        }
        self.timeline = sorted({row.open_time_ms for rows in self._candles_by_symbol.values() for row in rows})
        self.current_open_time_ms: int | None = None

    def advance_to(self, open_time_ms: int) -> None:
        self.current_open_time_ms = int(open_time_ms)

    def get_1m(self, symbol: str, *, limit: int = 5) -> list[Candle]:
        rows = self._candles_by_symbol.get(str(symbol).upper(), [])
        if self.current_open_time_ms is None:
            return rows[-int(limit) :]
        eligible = [row for row in rows if row.open_time_ms <= int(self.current_open_time_ms)]
        return eligible[-int(limit) :]


def _as_candle(symbol: str, row: Any) -> Candle:
    if isinstance(row, Candle):
        return row
    return Candle(
        symbol=str(row.get("symbol") or symbol).upper(),
        open_time_ms=int(row.get("open_time_ms") or row.get("open_time") or 0),
        open=float(row.get("open")),
        high=float(row.get("high")),
        low=float(row.get("low")),
        close=float(row.get("close")),
        volume=float(row.get("volume") or 0.0),
    )


def _counterfactual_results(
    project_root: Path,
    *,
    primary_dir: Path,
    candidates: list[dict[str, Any]],
    orders_by_key: dict[tuple[str, str], dict[str, Any]],
    candles_by_symbol: dict[str, list[Any]] | None,
    execution_source: str,
) -> dict[str, dict[str, Any]]:
    if not candles_by_symbol:
        return {}
    pending: list[dict[str, Any]] = []
    candle_symbols = {str(k).upper() for k in candles_by_symbol}
    for cand in candidates:
        key = (str(cand["strategy_line"]), str(cand["source_plan_hash"]))
        if key in orders_by_key:
            continue
        symbol = str(cand.get("symbol") or "").upper()
        if symbol not in candle_symbols:
            continue
        plan = cand.get("source_json") if isinstance(cand.get("source_json"), dict) else {}
        if plan:
            pending.append(cand)
    out: dict[str, dict[str, Any]] = {}
    for cand in pending:
        result = _fast_counterfactual_result(
            cand,
            candles=candles_by_symbol.get(str(cand.get("symbol") or "").upper()) or [],
            project_root=Path(project_root).resolve(),
            primary_dir=primary_dir,
            execution_source=execution_source,
        )
        if result:
            out[str(cand["candidate_id"])] = result
    if out:
        fast_dir = primary_dir / "cf_fast"
        fast_dir.mkdir(parents=True, exist_ok=True)
        write_jsonl(fast_dir / "trade_results.jsonl", list(out.values()))
    return out


def _fast_counterfactual_result(
    cand: dict[str, Any],
    *,
    candles: list[Any],
    project_root: Path,
    primary_dir: Path,
    execution_source: str,
) -> dict[str, Any] | None:
    symbol = str(cand.get("symbol") or "").upper()
    rows = sorted((_as_candle(symbol, row) for row in candles), key=lambda item: item.open_time_ms)
    if not rows:
        return None
    side = str(cand.get("side") or "").upper()
    entry = _float(cand.get("entry_price_hint"))
    sl = _float(cand.get("stop_loss"))
    tp = _float(cand.get("take_profit"))
    if side not in {"LONG", "SHORT"} or entry is None or sl is None or tp is None or entry <= 0 or sl <= 0 or tp <= 0:
        return None
    decision_ms = parse_time_ms(cand.get("decision_time_ms")) or parse_time_ms(cand.get("candle_open_time_ms")) or rows[0].open_time_ms
    entry_index = next((idx for idx, row in enumerate(rows) if row.open_time_ms >= int(decision_ms)), None)
    if entry_index is None:
        return None
    cfg = PaperConfig()
    fee_bps = float(cfg.taker_fee_bps)
    slippage_bps = float(cfg.default_slippage_bps)
    entry_candle = rows[entry_index]
    entry_fill = adverse_fill_price(entry, side, "entry", slippage_bps)
    plan = cand.get("source_json") if isinstance(cand.get("source_json"), dict) else {}
    position_sizing = plan.get("position_sizing") if isinstance(plan.get("position_sizing"), dict) else {}
    planned_notional = _float(position_sizing.get("planned_notional_usdt") if position_sizing else None)
    if planned_notional is None or planned_notional <= 0:
        intended_size = _float(cand.get("intended_size"))
        planned_notional = intended_size if intended_size and intended_size > 0 else cfg.default_margin_usdt * cfg.default_leverage
    qty = planned_notional / entry_fill if entry_fill else 0.0
    if qty <= 0:
        return None

    exit_reason = None
    exit_ref = None
    exit_candle = None
    high_seen = entry_candle.high
    low_seen = entry_candle.low
    for row in rows[entry_index:]:
        high_seen = max(high_seen, row.high)
        low_seen = min(low_seen, row.low)
        if side == "LONG":
            if row.low <= sl:
                exit_reason, exit_ref, exit_candle = "SL", sl, row
                break
            if row.high >= tp:
                exit_reason, exit_ref, exit_candle = "TP", tp, row
                break
        else:
            if row.high >= sl:
                exit_reason, exit_ref, exit_candle = "SL", sl, row
                break
            if row.low <= tp:
                exit_reason, exit_ref, exit_candle = "TP", tp, row
                break
    if exit_reason is None or exit_ref is None or exit_candle is None:
        return None

    exit_fill = adverse_fill_price(exit_ref, side, exit_reason.lower(), slippage_bps)
    gross = paper_pnl(side, entry_fill, exit_fill, qty)
    entry_fee = abs(entry_fill * qty) * fee_bps / 10_000
    exit_fee = abs(exit_fill * qty) * fee_bps / 10_000
    pnl = gross - entry_fee - exit_fee
    risk = abs(entry_fill - sl) * qty
    net_r = round(pnl / risk, 6) if risk > 0 else None
    risk_per_unit = abs(entry_fill - sl)
    if side == "LONG":
        mfe_r = round(max(0.0, high_seen - entry_fill) / risk_per_unit, 6) if risk_per_unit > 0 else None
        mae_r = round(max(0.0, entry_fill - low_seen) / risk_per_unit, 6) if risk_per_unit > 0 else None
    else:
        mfe_r = round(max(0.0, entry_fill - low_seen) / risk_per_unit, 6) if risk_per_unit > 0 else None
        mae_r = round(max(0.0, high_seen - entry_fill) / risk_per_unit, 6) if risk_per_unit > 0 else None
    created = now_iso()
    result_ref = {
        "source_db_path": project_rel(project_root, primary_dir / "cf_fast" / "trade_results.jsonl"),
        "source_table": "counterfactual_fast_estimator",
        "source_row_id": str(cand["candidate_id"]),
        "source_time_ms": int(exit_candle.open_time_ms),
        "source_hash": stable_hash(
            {
                "candidate_id": cand["candidate_id"],
                "entry_time_ms": entry_candle.open_time_ms,
                "exit_time_ms": exit_candle.open_time_ms,
                "entry_price": entry_fill,
                "exit_price": exit_fill,
                "pnl": pnl,
            }
        ),
    }
    return {
        "trade_result_id": stable_id("result", cand["candidate_id"], "counterfactual_fast", exit_candle.open_time_ms),
        "candidate_id": cand["candidate_id"],
        "order_id": None,
        "position_id": None,
        "run_id": cand["run_id"],
        "gated_run_id": None,
        "execution_source": execution_source,
        "executed": False,
        "not_executed_reason": "source_gate_blocked_counterfactual_estimated",
        "entry_time_ms": int(entry_candle.open_time_ms),
        "exit_time_ms": int(exit_candle.open_time_ms),
        "entry_price": round(entry_fill, 12),
        "exit_price": round(exit_fill, 12),
        "quantity": round(qty, 12),
        "fee_bps": fee_bps,
        "realized_slippage_bps": slippage_bps,
        "net_R": net_r,
        "MFE_R": mfe_r,
        "MAE_R": mae_r,
        "holding_time_sec": max(0.0, (int(exit_candle.open_time_ms) - int(entry_candle.open_time_ms)) / 1000),
        "exit_reason": exit_reason,
        "root_cause_label": "",
        "quality_label": "winner" if pnl > 0 else "loser",
        "label_policy_version": "winner_loser_v1",
        "outcome_source": "counterfactual_replay_estimated",
        "outcome_confidence": "medium",
        "result_refs_json": [result_ref],
        "created_at": created,
    }


def _counterfactual_result_from_db(db_path: Path, cand: dict[str, Any], *, root: Path, execution_source: str) -> dict[str, Any] | None:
    if not db_path.exists():
        return None
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        order_row = con.execute(
            """
            SELECT * FROM paper_orders
            WHERE strategy_line=? AND source_plan_hash=?
            ORDER BY rowid DESC LIMIT 1
            """,
            (str(cand.get("strategy_line")), str(cand.get("source_plan_hash"))),
        ).fetchone()
        if not order_row:
            return None
        order = dict(order_row)
        fills = [dict(row) for row in con.execute("SELECT * FROM paper_fills WHERE order_id=? ORDER BY rowid", (order.get("id"),)).fetchall()]
        pos_row = con.execute("SELECT * FROM paper_positions WHERE order_id=? ORDER BY rowid DESC LIMIT 1", (order.get("id"),)).fetchone()
    if str(order.get("status")) != "closed":
        return None
    entry_fill = next((row for row in fills if str(row.get("action")).lower() == "entry"), None)
    exit_fill = next((row for row in fills if str(row.get("action")).lower() != "entry"), None)
    pnl = _float(order.get("realized_pnl_usdt"))
    risk = abs(float(order.get("entry_price") or 0) - float(order.get("stop_loss") or 0)) * float(order.get("quantity") or 0)
    net_r = round(float(pnl) / risk, 6) if pnl is not None and risk > 0 else None
    created = now_iso()
    return {
        "trade_result_id": stable_id("result", cand["candidate_id"], "counterfactual", order.get("id")),
        "candidate_id": cand["candidate_id"],
        "order_id": str(order.get("id") or ""),
        "position_id": str(dict(pos_row).get("id") if pos_row else ""),
        "run_id": cand["run_id"],
        "gated_run_id": None,
        "execution_source": execution_source,
        "executed": False,
        "not_executed_reason": "source_rule_gate_blocked_original_order",
        "entry_time_ms": parse_time_ms(order.get("opened_at") or (entry_fill or {}).get("filled_at")),
        "exit_time_ms": parse_time_ms(order.get("closed_at") or (exit_fill or {}).get("filled_at")),
        "entry_price": _float(order.get("filled_entry_price") or order.get("entry_price") or (entry_fill or {}).get("fill_price")),
        "exit_price": _float(order.get("exit_price") or (exit_fill or {}).get("fill_price")),
        "quantity": _float(order.get("quantity") or (entry_fill or {}).get("quantity")),
        "fee_bps": _float((exit_fill or entry_fill or {}).get("fee_bps")),
        "realized_slippage_bps": _float((exit_fill or entry_fill or {}).get("slippage_bps")),
        "net_R": net_r,
        "MFE_R": None,
        "MAE_R": None,
        "holding_time_sec": None,
        "exit_reason": str(order.get("exit_reason") or ""),
        "root_cause_label": None,
        "quality_label": "winner" if (pnl or 0.0) > 0 else "loser" if pnl is not None else None,
        "label_policy_version": "winner_loser_v1",
        "outcome_source": "counterfactual_replay_estimated",
        "outcome_confidence": "medium",
        "result_refs_json": [
            {
                "source_db_path": project_rel(root, db_path),
                "source_table": "paper_orders",
                "source_row_id": str(order.get("id") or ""),
                "source_time_ms": parse_time_ms(order.get("closed_at") or order.get("updated_at") or order.get("created_at")),
                "source_hash": stable_hash(order),
            }
        ],
        "created_at": created,
    }


def _forbidden_violations(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for cand in candidates:
        keys = set(_walk_keys(cand.get("decision_time_features_json") or {}))
        got = sorted(keys & FORBIDDEN_DECISION_FEATURE_FIELDS)
        if got:
            out.append({"candidate_id": cand["candidate_id"], "fields": got})
    return out


def _walk_keys(value: Any) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            keys.add(str(key))
            keys.update(_walk_keys(item))
    elif isinstance(value, list):
        for item in value:
            keys.update(_walk_keys(item))
    return keys


def _insert_rows(con: sqlite3.Connection, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    keys = list(rows[0])
    placeholders = ",".join("?" for _ in keys)
    columns = ",".join(keys)
    values = []
    for row in rows:
        item = []
        for key in keys:
            value = row.get(key)
            if isinstance(value, (dict, list)):
                value = canonical_json(value)
            elif isinstance(value, bool):
                value = int(value)
            item.append(value)
        values.append(tuple(item))
    con.executemany(f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})", values)


def _copy_tree_files(src_dir: Path, dst_dir: Path, filenames: list[str]) -> None:
    if not dst_dir:
        return
    dst_dir.mkdir(parents=True, exist_ok=True)
    for name in filenames:
        src = src_dir / name
        if src.exists():
            shutil.copy2(src, dst_dir / name)


def sync_candidate_ledger_from_paper_sqlite(
    project_root: Path,
    *,
    source_db_path: Path,
    run_id: str,
    source_mode: str,
    sandbox_id: str | None = None,
    cycle_id: str | None = None,
    docs: dict[str, dict[str, Any]] | None = None,
    counterfactual_candles_by_symbol: dict[str, list[Any]] | None = None,
    mirror_dir: Path | None = None,
    execution_source: str | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    source_db = Path(source_db_path)
    if not source_db.is_absolute():
        source_db = root / source_db
    primary_dir = candidate_ledger_dir(root, run_id=run_id, source_mode=source_mode, sandbox_id=sandbox_id)
    db_path = primary_dir / "candidate_ledger.db"

    candidates_by_key = _doc_plan_candidates(
        docs,
        run_id=run_id,
        sandbox_id=sandbox_id,
        source_mode=source_mode,
        cycle_id=cycle_id,
    )
    with connect_ro(source_db) as ro:
        _merge_paper_rows(
            ro,
            candidates_by_key,
            root=root,
            source_db_path=source_db,
            run_id=run_id,
            sandbox_id=sandbox_id,
            source_mode=source_mode,
        )
        orders = _rows(ro, "paper_orders")
        skips = _rows(ro, "paper_skip_ledger")
        intents = _rows(ro, "paper_intent_inbox")
        positions = _rows(ro, "paper_positions")
        fills = _rows(ro, "paper_fills")
        tq_rows = _rows(ro, "trade_quality_samples")

    candidates = sorted(candidates_by_key.values(), key=lambda row: (str(row.get("strategy_line")), str(row.get("source_plan_hash"))))
    created = now_iso()
    for cand in candidates:
        cand["created_at"] = created
        cand["known_at_pass"] = bool(cand.get("known_at_pass"))
        cand["feature_schema_version"] = str(cand.get("feature_schema_version") or FEATURE_SCHEMA_VERSION)
        cand["known_at_policy_version"] = str(cand.get("known_at_policy_version") or KNOWN_AT_POLICY_VERSION)
        cand["missing_fields_json"] = sorted(set(cand.get("missing_fields_json") or []))

    orders_by_key = {(str(row.get("strategy_line")), str(row.get("source_plan_hash"))): row for row in orders if row.get("source_plan_hash")}
    skips_by_key = {(str(row.get("strategy_line")), str(row.get("source_plan_hash"))): row for row in skips if row.get("source_plan_hash")}
    intents_by_key = {(str(row.get("strategy_line")), str(row.get("source_plan_hash"))): row for row in intents if row.get("source_plan_hash")}
    positions_by_order = {str(row.get("order_id")): row for row in positions if row.get("order_id")}
    fills_by_order: dict[str, list[dict[str, Any]]] = {}
    for row in fills:
        fills_by_order.setdefault(str(row.get("order_id")), []).append(row)
    tq_by_order = {str(row.get("order_id")): row for row in tq_rows if row.get("order_id")}

    gates = _gate_records(candidates, skips_by_key, orders_by_key)
    counterfactual_by_candidate = _counterfactual_results(
        root,
        primary_dir=primary_dir,
        candidates=candidates,
        orders_by_key=orders_by_key,
        candles_by_symbol=counterfactual_candles_by_symbol,
        execution_source=execution_source or source_mode,
    )
    results = _result_records(
        candidates,
        orders_by_key=orders_by_key,
        positions_by_order=positions_by_order,
        fills_by_order=fills_by_order,
        tq_by_order=tq_by_order,
        counterfactual_results_by_candidate=counterfactual_by_candidate,
        root=root,
        source_db_path=source_db,
        execution_source=execution_source or source_mode,
    )
    links = _link_records(
        candidates,
        orders_by_key=orders_by_key,
        skips_by_key=skips_by_key,
        intents_by_key=intents_by_key,
        positions_by_order=positions_by_order,
        results=results,
    )
    leakage_violations = _forbidden_violations(candidates)
    blocked = [row for row in candidates if row.get("candidate_status") == "source_gate_blocked"]
    blocked_bad_labels = [
        row
        for row in results
        if row.get("executed") is False
        and row.get("outcome_source") == "not_executed_no_outcome"
        and row.get("quality_label") in {"loser", "bad"}
        and row.get("candidate_id") in {cand["candidate_id"] for cand in blocked}
    ]
    candidate_count = len(candidates)
    gate_count = len(gates)
    result_count = len(results)
    executed_count = sum(1 for row in results if row.get("executed") is True)
    no_outcome_count = sum(1 for row in results if row.get("outcome_source") == "not_executed_no_outcome")
    counterfactual_count = sum(1 for row in results if row.get("outcome_source") == "counterfactual_replay_estimated")
    counterfactual_candidate_count = max(0, candidate_count - len(orders_by_key))
    counterfactual_fast_results_path = primary_dir / "cf_fast" / "trade_results.jsonl"
    link_count = len(links)
    executed_link_count = sum(1 for row in links if row.get("link_status") == "executed")
    skip_link_count = sum(1 for row in links if row.get("skip_ledger_id"))
    intent_link_count = sum(1 for row in links if row.get("intent_id"))
    candidate_set_hash = stable_hash([_candidate_hash_payload(row) for row in candidates])
    baseline_comparable = False
    baseline_not_comparable_reason = "ai_gated_replay_not_run"
    coverage = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "sandbox_id": sandbox_id,
        "source_mode": source_mode,
        "candidate_count": candidate_count,
        "candidate_id_unique": len({row["candidate_id"] for row in candidates}) == candidate_count,
        "source_gate_decision_count": gate_count,
        "candidate_to_source_gate_link_rate": round(gate_count / candidate_count, 6) if candidate_count else 1.0,
        "trade_result_count": result_count,
        "candidate_to_result_link_rate": round(result_count / candidate_count, 6) if candidate_count else 1.0,
        "executed_candidate_count": executed_count,
        "executed_candidate_result_rate": 1.0 if executed_count else 1.0,
        "blocked_candidate_count": len(blocked),
        "blocked_without_outcome_count": no_outcome_count,
        "blocked_with_counterfactual_outcome_count": counterfactual_count,
        "counterfactual_replay_mode": "fast_sidecar_paper_equivalent_estimator",
        "counterfactual_candidate_count": counterfactual_candidate_count,
        "counterfactual_fast_results_path": project_rel(root, counterfactual_fast_results_path) if counterfactual_fast_results_path.exists() else "",
        "blocked_outcome_coverage_rate": round(counterfactual_count / len(blocked), 6) if blocked else 1.0,
        "candidate_known_at_pass_rate": round(sum(1 for row in candidates if row.get("known_at_pass")) / candidate_count, 6) if candidate_count else 1.0,
        "candidate_forbidden_field_violation_count": len(leakage_violations),
        "blocked_without_outcome_is_not_labeled_bad": not blocked_bad_labels,
        "blocked_without_outcome_training_eligible": False,
        "blocked_with_counterfactual_training_eligible": "evaluation_only",
        "candidate_order_link_count": link_count,
        "paper_orders_to_candidate_link_rate": round(executed_link_count / len(orders), 6) if orders else 1.0,
        "paper_skipped_intents_to_candidate_link_rate": round(intent_link_count / len(intents), 6) if intents else 1.0,
        "paper_skip_ledger_to_candidate_link_rate": round(skip_link_count / len(skips), 6) if skips else 1.0,
        "candidate_set_hash": candidate_set_hash,
        "candidate_set_hash_algorithm": CANDIDATE_SET_HASH_ALGORITHM,
        "baseline_candidate_set_hash": candidate_set_hash if str(source_mode).startswith("baseline") else "",
        "gated_candidate_set_hash": "",
        "baseline_candidate_count": candidate_count if str(source_mode).startswith("baseline") else 0,
        "gated_candidate_count": 0,
        "candidate_set_diff_count": None,
        "baseline_vs_gated_comparable": baseline_comparable,
        "baseline_vs_gated_not_comparable_reason": baseline_not_comparable_reason,
    }
    leakage = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "source_mode": source_mode,
        "forbidden_fields": sorted(FORBIDDEN_DECISION_FEATURE_FIELDS),
        "violations": leakage_violations,
        "post_trade_leakage_count": len(leakage_violations),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "candidate_ledger_version": SCHEMA_VERSION,
        "sidecar_only": SIDE_CAR_ONLY,
        "run_id": run_id,
        "sandbox_id": sandbox_id,
        "cycle_id": cycle_id,
        "source_mode": source_mode,
        "source_db_path": project_rel(root, source_db),
        "candidate_ledger_dir": project_rel(root, primary_dir),
        "candidate_ledger_db_path": project_rel(root, db_path),
        "candidate_signals_path": project_rel(root, primary_dir / "candidate_signals.jsonl"),
        "gate_decisions_path": project_rel(root, primary_dir / "gate_decisions.jsonl"),
        "trade_results_path": project_rel(root, primary_dir / "trade_results.jsonl"),
        "candidate_order_links_path": project_rel(root, primary_dir / "candidate_order_links.jsonl"),
        "coverage_audit_path": project_rel(root, primary_dir / "candidate_gate_result_coverage_audit.json"),
        "leakage_audit_path": project_rel(root, primary_dir / "candidate_gate_result_leakage_audit.json"),
        "candidate_count": candidate_count,
        "source_gate_decision_count": gate_count,
        "ai_gate_decision_count": 0,
        "executed_result_count": executed_count,
        "blocked_without_outcome_count": no_outcome_count,
        "counterfactual_result_count": counterfactual_count,
        "counterfactual_replay_mode": "fast_sidecar_paper_equivalent_estimator",
        "counterfactual_candidate_count": counterfactual_candidate_count,
        "counterfactual_fast_results_path": project_rel(root, counterfactual_fast_results_path) if counterfactual_fast_results_path.exists() else "",
        "candidate_to_gate_link_rate": coverage["candidate_to_source_gate_link_rate"],
        "candidate_to_result_link_rate": coverage["candidate_to_result_link_rate"],
        "executed_candidate_result_rate": coverage["executed_candidate_result_rate"],
        "blocked_candidate_outcome_policy": "blocked candidates are no_outcome unless sidecar counterfactual replay can estimate; never auto-labeled loser",
        "blocked_without_outcome_training_eligible": False,
        "blocked_with_counterfactual_training_eligible": "evaluation_only",
        "candidate_order_link_count": link_count,
        "paper_orders_to_candidate_link_rate": coverage["paper_orders_to_candidate_link_rate"],
        "paper_skipped_intents_to_candidate_link_rate": coverage["paper_skipped_intents_to_candidate_link_rate"],
        "paper_skip_ledger_to_candidate_link_rate": coverage["paper_skip_ledger_to_candidate_link_rate"],
        "candidate_set_hash": candidate_set_hash,
        "candidate_set_hash_algorithm": CANDIDATE_SET_HASH_ALGORITHM,
        "baseline_candidate_set_hash": coverage["baseline_candidate_set_hash"],
        "gated_candidate_set_hash": coverage["gated_candidate_set_hash"],
        "baseline_candidate_count": coverage["baseline_candidate_count"],
        "gated_candidate_count": coverage["gated_candidate_count"],
        "candidate_set_diff_count": coverage["candidate_set_diff_count"],
        "baseline_vs_gated_comparable": baseline_comparable,
        "baseline_vs_gated_not_comparable_reason": baseline_not_comparable_reason,
        "gate_decision_mapping": {"paper.pass": "allow", "paper.blocked": "block", "paper.null": "not_evaluated"},
        "source_gate_decision_count": gate_count,
        "final_gate_decision_count": 0,
        "manual_review_gate_decision_count": 0,
        "training_ready": False,
        "training_ready_owner": "ai_trader",
        "created_at": created,
    }

    primary_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(primary_dir / "candidate_signals.jsonl", candidates)
    write_jsonl(primary_dir / "gate_decisions.jsonl", gates)
    write_jsonl(primary_dir / "trade_results.jsonl", results)
    write_jsonl(primary_dir / "candidate_order_links.jsonl", links)
    write_json(primary_dir / "candidate_gate_result_coverage_audit.json", coverage)
    write_json(primary_dir / "candidate_gate_result_leakage_audit.json", leakage)

    with connect_candidate_ledger(db_path) as sidecar:
        for table in ("candidate_order_links", "trade_results", "gate_decisions", "trade_candidates", "candidate_ledger_manifest", "candidate_ledger_audit"):
            sidecar.execute(f"DELETE FROM {table} WHERE run_id=?", (run_id,))
        _insert_rows(sidecar, "trade_candidates", [_db_candidate(row) for row in candidates])
        _insert_rows(sidecar, "gate_decisions", gates)
        _insert_rows(sidecar, "trade_results", results)
        _insert_rows(sidecar, "candidate_order_links", links)
        _insert_rows(
            sidecar,
            "candidate_ledger_manifest",
            [
                {
                    "manifest_id": stable_id("manifest", run_id, source_mode),
                    "run_id": run_id,
                    "source_mode": source_mode,
                    "schema_version": SCHEMA_VERSION,
                    "manifest_json": manifest,
                    "created_at": created,
                }
            ],
        )
        _insert_rows(
            sidecar,
            "candidate_ledger_audit",
            [
                {
                    "audit_id": stable_id("audit", run_id, source_mode, "coverage"),
                    "run_id": run_id,
                    "audit_type": "coverage",
                    "audit_json": coverage,
                    "created_at": created,
                },
                {
                    "audit_id": stable_id("audit", run_id, source_mode, "leakage"),
                    "run_id": run_id,
                    "audit_type": "leakage",
                    "audit_json": leakage,
                    "created_at": created,
                },
            ],
        )
        sidecar.commit()

    filenames = [
        "candidate_signals.jsonl",
        "gate_decisions.jsonl",
        "trade_results.jsonl",
        "candidate_order_links.jsonl",
        "candidate_gate_result_manifest.json",
        "candidate_gate_result_coverage_audit.json",
        "candidate_gate_result_leakage_audit.json",
        "candidate_ledger.db",
    ]
    hash_files = [
        "candidate_signals.jsonl",
        "gate_decisions.jsonl",
        "trade_results.jsonl",
        "candidate_order_links.jsonl",
        "candidate_gate_result_coverage_audit.json",
        "candidate_gate_result_leakage_audit.json",
        "candidate_ledger.db",
    ]
    artifact_hashes = {name: file_sha256(primary_dir / name) for name in hash_files}
    manifest["artifact_hashes"] = artifact_hashes
    manifest["candidate_signals_hash"] = artifact_hashes["candidate_signals.jsonl"]
    manifest["gate_decisions_hash"] = artifact_hashes["gate_decisions.jsonl"]
    manifest["trade_results_hash"] = artifact_hashes["trade_results.jsonl"]
    manifest["candidate_order_links_hash"] = artifact_hashes["candidate_order_links.jsonl"]
    manifest["candidate_ledger_db_hash"] = artifact_hashes["candidate_ledger.db"]
    manifest["coverage_audit_hash"] = artifact_hashes["candidate_gate_result_coverage_audit.json"]
    manifest["leakage_audit_hash"] = artifact_hashes["candidate_gate_result_leakage_audit.json"]
    write_json(primary_dir / "candidate_gate_result_manifest.json", manifest)

    mirror_info: dict[str, Any] = {}
    if mirror_dir is not None:
        mirror_path = Path(mirror_dir)
        _copy_tree_files(primary_dir, mirror_path, filenames)
        mirror_hashes = {name: file_sha256(mirror_path / name) for name in hash_files}
        mirror_hash_match = mirror_hashes == artifact_hashes
        manifest["runtime_mirror_hashes"] = mirror_hashes
        manifest["runtime_mirror_hash_match"] = mirror_hash_match
        manifest["runtime_mirror_dir"] = project_rel(root, mirror_path)
        write_json(primary_dir / "candidate_gate_result_manifest.json", manifest)
        shutil.copy2(primary_dir / "candidate_gate_result_manifest.json", mirror_path / "candidate_gate_result_manifest.json")
        mirror_info = {
            "candidate_ledger_mirror_dir": project_rel(root, mirror_path),
            "candidate_ledger_mirror_db_path": project_rel(root, mirror_path / "candidate_ledger.db"),
            "candidate_ledger_mirror_manifest_path": project_rel(root, mirror_path / "candidate_gate_result_manifest.json"),
            "runtime_mirror_hash_match": mirror_hash_match,
        }
    else:
        write_json(primary_dir / "candidate_gate_result_manifest.json", manifest)

    return {
        "candidate_ledger_status": "completed",
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "sandbox_id": sandbox_id,
        "source_mode": source_mode,
        "candidate_count": candidate_count,
        "source_gate_decision_count": gate_count,
        "trade_result_count": result_count,
        "candidate_order_link_count": link_count,
        "executed_result_count": executed_count,
        "blocked_without_outcome_count": no_outcome_count,
        "counterfactual_result_count": counterfactual_count,
        "counterfactual_replay_mode": "fast_sidecar_paper_equivalent_estimator",
        "counterfactual_candidate_count": counterfactual_candidate_count,
        "counterfactual_fast_results_path": project_rel(root, counterfactual_fast_results_path) if counterfactual_fast_results_path.exists() else "",
        "candidate_ledger_dir": project_rel(root, primary_dir),
        "candidate_ledger_db_path": project_rel(root, db_path),
        "candidate_signals_path": project_rel(root, primary_dir / "candidate_signals.jsonl"),
        "gate_decisions_path": project_rel(root, primary_dir / "gate_decisions.jsonl"),
        "trade_results_path": project_rel(root, primary_dir / "trade_results.jsonl"),
        "candidate_order_links_path": project_rel(root, primary_dir / "candidate_order_links.jsonl"),
        "candidate_gate_result_manifest_path": project_rel(root, primary_dir / "candidate_gate_result_manifest.json"),
        "coverage_audit_path": project_rel(root, primary_dir / "candidate_gate_result_coverage_audit.json"),
        "leakage_audit_path": project_rel(root, primary_dir / "candidate_gate_result_leakage_audit.json"),
        "candidate_set_hash": candidate_set_hash,
        "artifact_hashes": artifact_hashes,
        "coverage": coverage,
        "leakage": leakage,
        **mirror_info,
    }


def _candidate_hash_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_line": row.get("strategy_line"),
        "symbol": row.get("symbol"),
        "side": row.get("side"),
        "decision_time_ms": row.get("decision_time_ms"),
        "source_plan_hash": row.get("source_plan_hash"),
    }


def _db_candidate(row: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "candidate_id",
        "run_id",
        "sandbox_id",
        "pipeline_run_id",
        "source_mode",
        "strategy_line",
        "strategy_version",
        "symbol",
        "side",
        "decision_time_ms",
        "candle_open_time_ms",
        "source_plan_hash",
        "candidate_status",
        "candidate_reason_codes_json",
        "intended_order_type",
        "intended_size",
        "entry_price_hint",
        "limit_price",
        "stop_loss",
        "take_profit",
        "planned_rr",
        "decision_time_features_json",
        "price_context_json",
        "risk_context_json",
        "cost_context_json",
        "market_regime_ref",
        "feature_schema_version",
        "known_at_policy_version",
        "feature_timestamp_cutoff",
        "known_at_pass",
        "source_refs_json",
        "field_lineage_json",
        "source_json",
        "missing_fields_json",
        "created_at",
    }
    return {key: row.get(key) for key in allowed}


def candidate_ledger_table_counts(db_path: Path) -> dict[str, int]:
    if not Path(db_path).exists():
        return {}
    out: dict[str, int] = {}
    with sqlite3.connect(db_path) as con:
        for table in (
            "trade_candidates",
            "gate_decisions",
            "trade_results",
            "candidate_order_links",
            "candidate_ledger_manifest",
            "candidate_ledger_audit",
        ):
            try:
                out[table] = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            except sqlite3.Error:
                out[table] = 0
    return out
