"""P29 training/audit snapshot sidecar sync.

This module is intentionally a sidecar writer.  It reads execution ledgers in
read-only mode and writes only under DATA/research/trade_snapshots.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any


SCHEMA_VERSION = "step29_trade_snapshot_v1"
FEATURE_SCHEMA_VERSION = "step29_market_feature_known_at_v1"
EXPORT_SCHEMA_VERSION = "step29_trade_snapshot_v2"
EXPORT_FEATURE_SCHEMA_VERSION = "step29_decision_time_input_v2"
KNOWN_AT_POLICY_VERSION = "step29_known_at_policy_v2"
LABEL_POLICY_VERSION = "winner_loser_v1"
QUALITY_LABEL_SOURCE_TAXONOMY = "winner_loser_v1"
COST_CONTEXT_POLICY_VERSION = "step29_full_cost_context_known_at_v1"
MARKET_REGIME_POLICY_VERSION = "step29_market_regime_ref_v1"
DISABLE_ENV = "P29_TRAINING_SYNC_DISABLED"
POST_TRADE_ONLY_FIELDS = {
    "MFE_R",
    "MAE_R",
    "net_R",
    "holding_time",
    "holding_time_sec",
    "exit_reason",
    "root_cause_label",
    "gross_pnl_usdt",
    "net_pnl_usdt",
    "exit_price",
}
TQ_REQUIRED_FIELDS = ("net_R", "MFE_R", "MAE_R", "holding_time_sec", "exit_reason")
STABLE_COST_FIELDS = ("fee_bps", "fee_usdt", "slippage_bps", "slippage_usdt", "fill_model", "cost_source")
UNSTABLE_COST_CONTEXT_FIELDS = ("spread_bps", "liquidity_bucket", "order_size_bucket", "market_regime_ref")
UI_ACTIVE_SANDBOX_LANE = "ui_active_sandbox_real_pipeline"
EXTERNAL_CLI_RESEARCH_LANE = "external_cli_research_lane"
DEFAULT_MARKET_RECONSTRUCTION_MAX_SOURCE_LAG_MS = 60_000
REQUIRED_MARKET_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "rsi_14",
    "ema20_distance_bps",
    "ema60_distance_bps",
    "bollinger_position",
    "bollinger_width_bps",
    "atr_14_bps",
    "volume_z",
    "pct_1m",
    "pct_3m",
    "pct_5m",
    "pct_15m",
    "range_pos_30m",
]

DDL = [
    "CREATE TABLE IF NOT EXISTS trade_snapshot_events (event_id TEXT PRIMARY KEY, sample_id TEXT NOT NULL, order_id TEXT NOT NULL, event_action TEXT NOT NULL CHECK(event_action IN ('entry','exit','unknown')), source_mode TEXT NOT NULL, source_db_path TEXT NOT NULL, source_table TEXT, source_row_id TEXT, strategy_line TEXT, symbol TEXT, side TEXT, event_time_ms INTEGER, candle_open_time_ms INTEGER, known_at_ms INTEGER, decision_time_ms INTEGER, order_plan_json TEXT NOT NULL DEFAULT '{}', execution_json TEXT NOT NULL DEFAULT '{}', market_snapshot_json TEXT NOT NULL DEFAULT '{}', trade_quality_json TEXT NOT NULL DEFAULT '{}', config_lineage_json TEXT NOT NULL DEFAULT '{}', data_quality_json TEXT NOT NULL DEFAULT '{}', field_roles_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS trade_training_samples (sample_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, position_id TEXT, intent_id TEXT, source_mode TEXT NOT NULL, source_db_path TEXT NOT NULL, strategy_line TEXT, symbol TEXT, side TEXT, entry_event_id TEXT, exit_event_id TEXT, entry_time_ms INTEGER, exit_time_ms INTEGER, decision_time_input_json TEXT NOT NULL DEFAULT '{}', order_plan_json TEXT NOT NULL DEFAULT '{}', execution_fact_json TEXT NOT NULL DEFAULT '{}', post_trade_outcome_json TEXT NOT NULL DEFAULT '{}', label_json TEXT NOT NULL DEFAULT '{}', audit_context_json TEXT NOT NULL DEFAULT '{}', data_quality_json TEXT NOT NULL DEFAULT '{}', source_refs_json TEXT NOT NULL DEFAULT '{}', schema_version TEXT NOT NULL, created_at TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS trade_snapshot_source_refs (source_ref_id TEXT PRIMARY KEY, sample_id TEXT, event_id TEXT, source_mode TEXT NOT NULL, source_db_path TEXT NOT NULL, source_table TEXT NOT NULL, source_pk_json TEXT NOT NULL DEFAULT '{}', source_row_hash TEXT, access_mode TEXT NOT NULL DEFAULT 'read_only', created_at TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS trade_snapshot_manifests (manifest_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, source_mode TEXT NOT NULL, schema_version TEXT NOT NULL, schema_hash TEXT NOT NULL, source_refs_json TEXT NOT NULL DEFAULT '[]', coverage_json TEXT NOT NULL DEFAULT '{}', dataset_hash TEXT, created_at TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS trade_snapshot_coverage_audits (audit_id TEXT PRIMARY KEY, manifest_id TEXT, sample_count INTEGER NOT NULL DEFAULT 0, entry_exit_pair_rate REAL, market_feature_complete_rate REAL, trade_quality_label_rate REAL, config_gate_lineage_rate REAL, known_at_pass_rate REAL, leakage_violations_json TEXT NOT NULL DEFAULT '[]', missing_fields_json TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL)",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def stable_id(prefix: str, *parts: Any, size: int = 32) -> str:
    return f"{prefix}_{stable_hash(parts)[:size]}"


def safe_run_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.:-]+", "_", str(value or "p29_sync")).strip("_") or "p29_sync"


def read_json(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def parse_time_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except ValueError:
        return None


def earliest_time_ms(*values: Any) -> int | None:
    parsed = [parse_time_ms(value) for value in values]
    got = [value for value in parsed if value is not None]
    return min(got) if got else None


def project_rel(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def research_dir(root: Path) -> Path:
    return Path(root) / "DATA" / "research" / "trade_snapshots"


def sidecar_db_path(root: Path) -> Path:
    return research_dir(root) / "trade_snapshots.db"


def sandbox_export_dir(root: Path, *, sandbox_id: str, run_id: str) -> Path:
    return research_dir(root) / "sandbox_exports" / safe_run_id(str(sandbox_id)) / safe_run_id(str(run_id))


def sandbox_training_mirror_dir(
    root: Path,
    *,
    sandbox_id: str,
    run_id: str,
    run_root_rel: str | None = None,
) -> Path:
    if run_root_rel:
        return Path(root) / run_root_rel / "training"
    return Path(root) / "DATA" / "sandboxes" / str(sandbox_id) / "runtime" / "pipeline_runs" / str(run_id) / "training"


def source_mode_for_sandbox_paper(resource_lane: str | None, *, pipeline_mode: str | None = None) -> str:
    lane = str(resource_lane or "")
    full_pipeline = str(pipeline_mode or "") == "sandbox_full_pipeline"
    if lane == EXTERNAL_CLI_RESEARCH_LANE:
        return "external_cli_sandbox_full_pipeline" if full_pipeline else "external_cli_sandbox_paper"
    if lane == UI_ACTIVE_SANDBOX_LANE:
        return "ui_sandbox_full_pipeline" if full_pipeline else "ui_sandbox_paper"
    return "sandbox_full_pipeline" if full_pipeline else "sandbox_paper"


def connect_ro(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def connect_sidecar(root: Path) -> sqlite3.Connection:
    path = sidecar_db_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    for ddl in DDL:
        con.execute(ddl)
    con.commit()
    return con


def table_names(con: sqlite3.Connection) -> set[str]:
    return {
        str(row["name"])
        for row in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def cleanup_run(con: sqlite3.Connection, run_id: str) -> None:
    like = f"{safe_run_id(run_id)}:%"
    con.execute("DELETE FROM trade_snapshot_source_refs WHERE source_ref_id LIKE ?", (like,))
    con.execute("DELETE FROM trade_snapshot_coverage_audits WHERE audit_id LIKE ?", (like,))
    con.execute("DELETE FROM trade_snapshot_manifests WHERE manifest_id LIKE ?", (like,))
    con.execute("DELETE FROM trade_training_samples WHERE sample_id LIKE ?", (like,))
    con.execute("DELETE FROM trade_snapshot_events WHERE sample_id LIKE ?", (like,))
    con.commit()


def walk_keys(value: Any) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            keys.add(str(key))
            keys.update(walk_keys(item))
    elif isinstance(value, list):
        for item in value:
            keys.update(walk_keys(item))
    return keys


def disabled_payload(run_id: str, source_mode: str) -> dict[str, Any]:
    return {
        "training_dataset_status": "disabled",
        "run_id": run_id,
        "source_mode": source_mode,
        "reason": f"{DISABLE_ENV}=1",
        "training_ready": False,
    }


def sync_failure_payload(run_id: str, source_mode: str, exc: Exception) -> dict[str, Any]:
    return {
        "training_dataset_status": "failed",
        "run_id": run_id,
        "source_mode": source_mode,
        "error": str(exc),
        "training_ready": False,
    }


def _market_stub(symbol: str | None, candle_open_time_ms: int | None, event_action: str) -> dict[str, Any]:
    return {
        "status": "needs_reconstruction",
        "event_action": event_action,
        "symbol": symbol,
        "candle_open_time_ms": candle_open_time_ms,
        "known_at_policy": FEATURE_SCHEMA_VERSION,
        "missing_fields": [
            "ohlcv",
            "rsi_14",
            "ema20_distance_bps",
            "ema60_distance_bps",
            "bollinger_position",
            "bollinger_width_bps",
            "atr_14_bps",
            "volume_z",
        ],
    }


def _data_quality(market: dict[str, Any], tq_json: dict[str, Any], lineage: dict[str, Any]) -> dict[str, Any]:
    missing = list(market.get("missing_fields") or [])
    if not tq_json:
        missing.extend(["trade_quality.net_R", "trade_quality.MFE_R", "trade_quality.MAE_R", "trade_quality.holding_time"])
    missing.extend([f"config_gate.{field}" for field in lineage.get("missing_fields_json", [])])
    return {
        "feature_completeness": "incomplete",
        "market_snapshot_status": market.get("status"),
        "trade_quality_status": "joined" if tq_json else "missing_or_not_joined",
        "missing_fields_json": sorted(set(missing)),
        "proxy_fields_json": [],
        "blocked_fields_json": [],
    }


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _tq_lineage(source_rel: str, tq_json: dict[str, Any], order_id: str) -> dict[str, Any]:
    return {
        "trade_quality_provider": "trade_quality_module",
        "trade_quality_module": "laoma_signal_engine.trade_quality.engine",
        "source_db_path": source_rel,
        "source_table": "trade_quality_samples",
        "source_row_id": tq_json.get("sample_id") or order_id,
    }


def _training_tq_payload(
    tq_json: dict[str, Any],
    *,
    source_rel: str,
    order_id: str,
    existing_outcome: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Normalize Trade Quality module output into P29 training fields."""

    if not tq_json:
        outcome = dict(existing_outcome or {})
        outcome.update(
            {
                "trade_quality_provider": None,
                "trade_quality_module": None,
                "trade_quality_status": "tq_module_missing",
                "missing_fields_json": list(TQ_REQUIRED_FIELDS),
            }
        )
        label = {
            "review_status": "needs_human_review",
            "training_label_ready": False,
            "trade_quality_status": "tq_module_missing",
            "trade_quality_provider": None,
            "trade_quality_module": None,
            "missing_fields_json": list(TQ_REQUIRED_FIELDS),
            "root_cause_label": "unknown",
            "quality_label": "needs_human_review",
            "bad_trade_flag": None,
        }
        return outcome, label, {
            "trade_quality_status": "tq_module_missing",
            "trade_quality_training_label_ready": False,
            "review_status": "needs_human_review",
            "missing_fields_json": [f"trade_quality.{field}" for field in TQ_REQUIRED_FIELDS],
        }

    lineage = _tq_lineage(source_rel, tq_json, order_id)
    holding_time_sec = _as_float(tq_json.get("holding_time_sec"))
    if holding_time_sec is None:
        holding_time_sec = _as_float(tq_json.get("holding_sec"))
    if holding_time_sec is None and tq_json.get("holding_minutes") is not None:
        minutes = _as_float(tq_json.get("holding_minutes"))
        holding_time_sec = minutes * 60.0 if minutes is not None else None
    net_r = _as_float(tq_json.get("net_R"))
    mfe_r = _as_float(tq_json.get("MFE_R"))
    mae_r = _as_float(tq_json.get("MAE_R"))
    exit_reason = tq_json.get("exit_reason")
    root_cause = tq_json.get("root_cause_label") or tq_json.get("root_cause") or "unknown"
    confidence = _as_float(tq_json.get("root_cause_confidence"))
    manual = bool(tq_json.get("needs_manual_review") in (1, "1", True, "true", "True"))
    missing = [
        field
        for field, value in (
            ("net_R", net_r),
            ("MFE_R", mfe_r),
            ("MAE_R", mae_r),
            ("holding_time_sec", holding_time_sec),
            ("exit_reason", exit_reason),
        )
        if value in (None, "")
    ]
    ready = not missing and not manual
    quality_label = "unknown"
    if net_r is not None:
        quality_label = "winner" if net_r > 0 else "loser"
    module_version = tq_json.get("label_schema_version") or tq_json.get("schema_version") or tq_json.get("sample_schema_version")
    outcome = dict(existing_outcome or {})
    outcome.update(
        {
            "net_R": net_r,
            "MFE_R": mfe_r,
            "MAE_R": mae_r,
            "holding_time_sec": holding_time_sec,
            "exit_reason": exit_reason,
            "gross_pnl_usdt": _as_float(tq_json.get("gross_pnl_usdt") if "gross_pnl_usdt" in tq_json else tq_json.get("gross_pnl")),
            "net_pnl_usdt": _as_float(tq_json.get("net_pnl_usdt") if "net_pnl_usdt" in tq_json else tq_json.get("net_pnl")),
            "exit_price": _as_float(tq_json.get("exit_price")),
            "trade_quality_status": "complete" if ready else "joined_needs_review",
            "trade_quality_provider": lineage["trade_quality_provider"],
            "trade_quality_module": lineage["trade_quality_module"],
            "trade_quality_module_version": module_version,
            "trade_quality_source_ref": lineage,
            "missing_fields_json": missing,
        }
    )
    label = {
        "root_cause_label": root_cause,
        "root_cause_confidence": confidence,
        "bad_trade_flag": bool(net_r is not None and net_r < 0),
        "quality_label": quality_label,
        "review_status": "needs_human_review" if manual or missing else "ready",
        "training_label_ready": ready,
        "trade_quality_status": "complete" if ready else "joined_needs_review",
        "trade_quality_provider": lineage["trade_quality_provider"],
        "trade_quality_module": lineage["trade_quality_module"],
        "trade_quality_module_version": module_version,
        "trade_quality_source_ref": lineage,
        "missing_fields_json": missing,
    }
    dq_update = {
        "trade_quality_status": "complete" if ready else "joined_needs_review",
        "trade_quality_provider": lineage["trade_quality_provider"],
        "trade_quality_module": lineage["trade_quality_module"],
        "trade_quality_training_label_ready": ready,
        "review_status": label["review_status"],
        "missing_fields_json": [f"trade_quality.{field}" for field in missing],
    }
    return outcome, label, dq_update


def _merge_data_quality(base: dict[str, Any], tq_update: dict[str, Any]) -> dict[str, Any]:
    out = dict(base or {})
    missing = {
        str(field)
        for field in (out.get("missing_fields_json") or [])
        if not str(field).startswith("trade_quality.")
    }
    missing.update(str(field) for field in tq_update.get("missing_fields_json") or [])
    out.update({key: value for key, value in tq_update.items() if key != "missing_fields_json"})
    out["missing_fields_json"] = sorted(missing)
    return out


def _config_lineage(order: dict[str, Any], source_rel: str, source_table: str = "paper_orders") -> dict[str, Any]:
    payload = {
        "strategy_line": order.get("strategy_line"),
        "strategy_name": order.get("signal_class") or order.get("strategy_line"),
        "parameter_set_id": order.get("parameter_set_id") or order.get("experiment_id") or order.get("gate_candidate_id"),
        "config_snapshot_json": {},
        "gate_candidate_id": order.get("gate_candidate_id"),
        "gate_decision": order.get("gate_decision"),
        "gate_rule_json": order.get("gate_rule_json"),
        "gate_features_json": order.get("gate_features_json"),
        "fill_model": order.get("fill_model") or order.get("fill_run_id"),
        "cost_source": order.get("cost_source"),
        "slippage_source": order.get("slippage_source"),
        "same_candle_policy": order.get("same_candle_policy"),
        "source_refs_json": [{"source_db_path": source_rel, "source_table": source_table, "id": order.get("id") or order.get("order_id")}],
        "missing_fields_json": [],
    }
    payload["missing_fields_json"] = [
        key
        for key in ("parameter_set_id", "gate_candidate_id", "gate_decision", "gate_rule_json", "gate_features_json")
        if payload.get(key) in (None, "")
    ]
    payload["config_hash"] = stable_hash(
        {
            "strategy_line": payload["strategy_line"],
            "strategy_name": payload["strategy_name"],
            "parameter_set_id": payload["parameter_set_id"],
            "fill_model": payload["fill_model"],
            "cost_source": payload["cost_source"],
            "slippage_source": payload["slippage_source"],
            "same_candle_policy": payload["same_candle_policy"],
        }
    )
    payload["gate_hash"] = stable_hash(
        {
            "gate_candidate_id": payload["gate_candidate_id"],
            "gate_rule_json": payload["gate_rule_json"],
            "gate_features_json": payload["gate_features_json"],
            "gate_decision": payload["gate_decision"],
        }
    )
    return payload


def _insert_event(
    con: sqlite3.Connection,
    *,
    run_id: str,
    sample_id: str,
    order_id: str,
    event_action: str,
    source_mode: str,
    source_rel: str,
    source_table: str,
    source_row_id: str,
    strategy_line: str | None,
    symbol: str | None,
    side: str | None,
    event_time_ms: int | None,
    candle_open_time_ms: int | None,
    decision_time_ms: int | None,
    order_plan: dict[str, Any],
    execution: dict[str, Any],
    tq_json: dict[str, Any],
    lineage: dict[str, Any],
) -> str:
    event_id = f"{safe_run_id(run_id)}:{stable_id('event', source_rel, source_table, source_row_id, event_action)}"
    market = _market_stub(symbol, candle_open_time_ms, event_action)
    data_quality = _data_quality(market, tq_json, lineage)
    con.execute(
        """
        INSERT OR REPLACE INTO trade_snapshot_events (
            event_id, sample_id, order_id, event_action, source_mode, source_db_path,
            source_table, source_row_id, strategy_line, symbol, side, event_time_ms,
            candle_open_time_ms, known_at_ms, decision_time_ms, order_plan_json,
            execution_json, market_snapshot_json, trade_quality_json, config_lineage_json,
            data_quality_json, field_roles_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            event_id,
            sample_id,
            order_id,
            event_action,
            source_mode,
            source_rel,
            source_table,
            source_row_id,
            strategy_line,
            symbol,
            side,
            event_time_ms,
            candle_open_time_ms,
            event_time_ms,
            decision_time_ms,
            canonical_json(order_plan),
            canonical_json(execution),
            canonical_json(market),
            canonical_json(tq_json),
            canonical_json(lineage),
            canonical_json(data_quality),
            canonical_json(
                {
                    "market_snapshot_json": "input_feature",
                    "execution_json": "execution_fact",
                    "trade_quality_json": "outcome_or_label",
                    "config_lineage_json": "audit_lineage",
                }
            ),
            now_iso(),
        ),
    )
    source_ref_id = f"{safe_run_id(run_id)}:{stable_id('src', source_rel, source_table, source_row_id, event_action)}"
    con.execute(
        """
        INSERT OR REPLACE INTO trade_snapshot_source_refs (
            source_ref_id, sample_id, event_id, source_mode, source_db_path, source_table,
            source_pk_json, source_row_hash, access_mode, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            source_ref_id,
            sample_id,
            event_id,
            source_mode,
            source_rel,
            source_table,
            canonical_json({"id": source_row_id}),
            stable_hash(execution),
            "read_only",
            now_iso(),
        ),
    )
    return event_id


def _finalize_run(
    root: Path,
    con: sqlite3.Connection,
    *,
    run_id: str,
    source_mode: str,
    source_rel: str,
    samples_written: int,
    events_written: int,
    tq_joined: int,
    export_dir: Path | None = None,
    mirror_dir: Path | None = None,
    source_table: str | None = None,
    sandbox_id: str | None = None,
    cycle_id: str | None = None,
    resource_lane: str | None = None,
    source_chain: str | None = None,
    writer_context_id: str | None = None,
    reason_codes: list[str] | None = None,
) -> dict[str, Any]:
    manifest_id = f"{safe_run_id(run_id)}:{stable_id('manifest', source_rel)}"
    reconstruction: dict[str, Any] = {}
    reconstruction_reason_codes: list[str] = []
    if samples_written:
        try:
            reconstruction = complete_scoped_known_at_reconstruction(
                root,
                con,
                run_id=run_id,
                sandbox_id=sandbox_id,
                source_mode=source_mode,
                max_source_lag_ms=DEFAULT_MARKET_RECONSTRUCTION_MAX_SOURCE_LAG_MS,
            )
            if reconstruction.get("status") != "completed":
                reconstruction_reason_codes.append(str(reconstruction.get("reason") or "known_at_reconstruction_not_completed"))
            if reconstruction.get("stale_source_event_count"):
                reconstruction_reason_codes.append("market_reconstruction_stale_source_blocked")
        except Exception as exc:  # pragma: no cover - sidecar must not rollback business chain
            reconstruction = {"status": "failed", "error": str(exc)}
            reconstruction_reason_codes.append("known_at_reconstruction_failed")
    coverage = {
        "sample_count": samples_written,
        "events_written": events_written,
        "trade_quality_joined": tq_joined,
        "entry_exit_pair_rate": reconstruction.get("entry_exit_pair_rate", 1.0 if samples_written else 0.0),
        "market_feature_complete_rate": reconstruction.get("market_feature_complete_rate", 0.0),
        "trade_quality_label_rate": reconstruction.get("trade_quality_label_rate", (tq_joined / samples_written) if samples_written else 0.0),
        "config_gate_lineage_rate": reconstruction.get("config_gate_lineage_rate", 1.0 if samples_written else 0.0),
        "known_at_pass_rate": reconstruction.get("known_at_pass_rate", 0.0),
        "known_at_status": "complete" if reconstruction.get("market_feature_complete_rate") == 1.0 and reconstruction.get("known_at_pass_rate") == 1.0 else str(reconstruction.get("reason") or reconstruction.get("status") or "pending_real_market_reconstruction"),
        "known_at_reconstruction": reconstruction,
    }
    dataset_rows = [_enrich_training_row_v2(row) for row in _rows_for_run(con, run_id)]
    event_rows = _events_for_run_v2(con, run_id)
    leakage_violations = _leakage_violations(dataset_rows)
    dataset_hash = stable_hash(dataset_rows)
    schema_hash = stable_hash(
        {
            "schema_version": EXPORT_SCHEMA_VERSION,
            "feature_schema_version": EXPORT_FEATURE_SCHEMA_VERSION,
            "known_at_policy_version": KNOWN_AT_POLICY_VERSION,
            "label_policy_version": LABEL_POLICY_VERSION,
            "ddl": DDL,
        }
    )
    cost_coverage = _cost_coverage(event_rows)
    readiness_rates = _v2_readiness_rates(dataset_rows, event_rows)
    record_schema_version_consistent = bool(dataset_rows) and all(row.get("schema_version") == EXPORT_SCHEMA_VERSION for row in dataset_rows)
    source_fact_ready = bool(
        samples_written > 0
        and coverage.get("known_at_pass_rate") == 1.0
        and coverage.get("market_feature_complete_rate") == 1.0
        and coverage.get("trade_quality_label_rate") == 1.0
        and coverage.get("entry_exit_pair_rate") == 1.0
        and readiness_rates["decision_time_feature_schema_v2_pass_rate"] == 1.0
        and readiness_rates["label_policy_v2_pass_rate"] == 1.0
        and cost_coverage["cost_fields_coverage"] == 1.0
        and readiness_rates["post_trade_leakage_count"] == 0
        and record_schema_version_consistent
    )
    coverage.update(cost_coverage)
    coverage.update(readiness_rates)
    coverage["record_schema_version_consistent"] = record_schema_version_consistent
    coverage["source_fact_ready"] = source_fact_ready
    con.execute(
        """
        INSERT OR REPLACE INTO trade_snapshot_manifests (
            manifest_id, run_id, source_mode, schema_version, schema_hash, source_refs_json,
            coverage_json, dataset_hash, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            manifest_id,
            safe_run_id(run_id),
            source_mode,
            EXPORT_SCHEMA_VERSION,
            schema_hash,
            canonical_json([{"source_db_path": source_rel, "access_mode": "read_only"}]),
            canonical_json(coverage),
            dataset_hash,
            now_iso(),
        ),
    )
    audit_id = f"{safe_run_id(run_id)}:{stable_id('audit', source_rel)}"
    con.execute(
        """
        INSERT OR REPLACE INTO trade_snapshot_coverage_audits (
            audit_id, manifest_id, sample_count, entry_exit_pair_rate, market_feature_complete_rate,
            trade_quality_label_rate, config_gate_lineage_rate, known_at_pass_rate,
            leakage_violations_json, missing_fields_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            audit_id,
            manifest_id,
            samples_written,
            coverage["entry_exit_pair_rate"],
            coverage["market_feature_complete_rate"],
            coverage["trade_quality_label_rate"],
            coverage["config_gate_lineage_rate"],
            coverage["known_at_pass_rate"],
            canonical_json(leakage_violations),
            canonical_json(["market_features_need_reconstruction", "trade_quality_label_may_be_missing"]),
            now_iso(),
        ),
    )
    con.commit()

    out_dir = export_dir or research_dir(root)
    out_dir.mkdir(parents=True, exist_ok=True)
    prefix = safe_run_id(run_id)
    if export_dir:
        dataset_path = out_dir / "training_dataset.jsonl"
        manifest_path = out_dir / "dataset_manifest.json"
        coverage_path = out_dir / "coverage_audit.json"
        leakage_path = out_dir / "leakage_audit.json"
        trade_snapshots_path = out_dir / "trade_snapshots.jsonl"
    else:
        dataset_path = out_dir / f"{prefix}_training_dataset.jsonl"
        manifest_path = out_dir / f"{prefix}_dataset_manifest.json"
        coverage_path = out_dir / f"{prefix}_coverage_audit.json"
        leakage_path = out_dir / f"{prefix}_leakage_audit.json"
        trade_snapshots_path = out_dir / f"{prefix}_trade_snapshots.jsonl"
    reason_codes = sorted(set((reason_codes or []) + reconstruction_reason_codes))
    if leakage_violations:
        reason_codes.append("leakage_violations_detected")
    if samples_written == 0:
        reason_codes.append("no_training_samples_written")
    source_refs = [
        {
            "source_db_path": source_rel,
            "source_table": source_table,
            "access_mode": "read_only",
            "sandbox_id": sandbox_id,
            "run_id": safe_run_id(run_id),
            "source_mode": source_mode,
        }
    ]
    with dataset_path.open("w", encoding="utf-8", newline="\n") as fh:
        for row in dataset_rows:
            fh.write(canonical_json(row) + "\n")
    with trade_snapshots_path.open("w", encoding="utf-8", newline="\n") as fh:
        for row in event_rows:
            fh.write(canonical_json(row) + "\n")
    manifest = {
        "dataset_version": f"{prefix}_v2",
        "dataset_path": project_rel(root, dataset_path),
        "dataset_hash": dataset_hash,
        "schema_version": EXPORT_SCHEMA_VERSION,
        "schema_hash": schema_hash,
        "feature_schema_version": EXPORT_FEATURE_SCHEMA_VERSION,
        "known_at_policy_version": KNOWN_AT_POLICY_VERSION,
        "label_policy_version": LABEL_POLICY_VERSION,
        "quality_label_source_taxonomy": QUALITY_LABEL_SOURCE_TAXONOMY,
        "ai_trader_label_mapping_required": True,
        "source_mode": source_mode,
        "source_snapshot_refs": source_refs,
        "coverage_path": project_rel(root, coverage_path),
        "leakage_path": project_rel(root, leakage_path),
        "allowed_for_llm_training": False,
        "training_dataset_status": "incomplete" if leakage_violations or samples_written == 0 else "needs_review",
        "training_sidecar_db_path": project_rel(root, sidecar_db_path(root)),
        "training_export_dir": project_rel(root, out_dir),
        "training_dataset_manifest_path": project_rel(root, manifest_path),
        "training_dataset_coverage_path": project_rel(root, coverage_path),
        "training_dataset_leakage_path": project_rel(root, leakage_path),
        "training_dataset_source_mode": source_mode,
        "source_db_path": source_rel,
        "source_table": source_table,
        "sandbox_id": sandbox_id,
        "run_id": safe_run_id(run_id),
        "cycle_id": cycle_id,
        "resource_lane": resource_lane,
        "source_chain": source_chain,
        "writer_context_id": writer_context_id,
        "samples_written": samples_written,
        "events_written": events_written,
        "event_snapshots_written": len(event_rows),
        "training_ready": False,
        "split_policy_owner": "ai_trader",
        "unit_id_owner": "ai_trader",
        "dataset_registration_owner": "ai_trader",
        "source_fact_ready": source_fact_ready,
        "ai_trader_registration_pending": True,
        "record_schema_version_consistent": record_schema_version_consistent,
        "stable_cost_fields_coverage": cost_coverage["stable_cost_fields_coverage"],
        "cost_fields_coverage": cost_coverage["cost_fields_coverage"],
        "cost_missing_fields_json": cost_coverage["cost_missing_fields_json"],
        "decision_time_feature_schema_v2_pass_rate": readiness_rates["decision_time_feature_schema_v2_pass_rate"],
        "label_policy_v2_pass_rate": readiness_rates["label_policy_v2_pass_rate"],
        "post_trade_leakage_count": readiness_rates["post_trade_leakage_count"],
        "oos_used_for_training_or_hpo": False,
        "paper_shadow_used_for_training_or_hpo": False,
        "reason_codes": sorted(set(reason_codes)),
        "known_at_reconstruction": reconstruction,
        "generated_at": now_iso(),
    }
    coverage_doc = {
        "run_id": safe_run_id(run_id),
        "source_mode": source_mode,
        **coverage,
        **cost_coverage,
        **readiness_rates,
        "leakage_violations": leakage_violations,
        "generated_at": now_iso(),
    }
    leakage_doc = {
        "run_id": safe_run_id(run_id),
        "source_mode": source_mode,
        "leakage_violations": leakage_violations,
        "post_trade_only_fields": sorted(POST_TRADE_ONLY_FIELDS),
        "generated_at": now_iso(),
    }
    raw_refs_path: Path | None = None
    if sandbox_id:
        raw_refs = {
            "source_mode": source_mode,
            "sandbox_id": sandbox_id,
            "run_id": safe_run_id(run_id),
            "cycle_id": cycle_id,
            "resource_lane": resource_lane,
            "source_chain": source_chain,
            "writer_context_id": writer_context_id,
            "source_db_path": source_rel,
            "source_table": source_table,
            "access_mode": "read_only",
            "generated_at": now_iso(),
        }
        raw_refs_path = out_dir / "raw_source_refs.json"
        raw_refs_path.write_text(json.dumps(raw_refs, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    coverage_path.write_text(json.dumps(coverage_doc, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    leakage_path.write_text(json.dumps(leakage_doc, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    if mirror_dir:
        mirror_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(manifest_path, mirror_dir / "dataset_manifest.json")
        shutil.copyfile(coverage_path, mirror_dir / "coverage_audit.json")
        shutil.copyfile(leakage_path, mirror_dir / "leakage_audit.json")
        if raw_refs_path and raw_refs_path.exists():
            shutil.copyfile(raw_refs_path, mirror_dir / "source_refs.json")
        else:
            (mirror_dir / "source_refs.json").write_text(json.dumps(source_refs, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        shutil.copyfile(dataset_path, mirror_dir / "training_dataset.jsonl")
        shutil.copyfile(trade_snapshots_path, mirror_dir / "trade_snapshots.jsonl")
    result = {
        "training_dataset_status": manifest["training_dataset_status"],
        "training_ready": False,
        "run_id": safe_run_id(run_id),
        "source_mode": source_mode,
        "sidecar_db": project_rel(root, sidecar_db_path(root)),
        "manifest_path": project_rel(root, manifest_path),
        "coverage_path": project_rel(root, coverage_path),
        "leakage_path": project_rel(root, leakage_path),
        "dataset_path": project_rel(root, dataset_path),
        "trade_snapshots_jsonl_path": project_rel(root, trade_snapshots_path),
        "dataset_hash": dataset_hash,
        "samples_written": samples_written,
        "events_written": events_written,
        "event_snapshots_written": len(event_rows),
        "leakage_violations": len(leakage_violations),
        "training_sidecar_db_path": project_rel(root, sidecar_db_path(root)),
        "training_export_dir": project_rel(root, out_dir),
        "training_dataset_manifest_path": project_rel(root, manifest_path),
        "training_dataset_coverage_path": project_rel(root, coverage_path),
        "training_dataset_leakage_path": project_rel(root, leakage_path),
        "training_dataset_source_mode": source_mode,
        "source_db_path": source_rel,
        "source_table": source_table,
        "sandbox_id": sandbox_id,
        "cycle_id": cycle_id,
        "resource_lane": resource_lane,
        "source_chain": source_chain,
        "writer_context_id": writer_context_id,
        "reason_codes": sorted(set(reason_codes)),
        "known_at_reconstruction": reconstruction,
        "cost_fields_coverage": cost_coverage["cost_fields_coverage"],
        "stable_cost_fields_coverage": cost_coverage["stable_cost_fields_coverage"],
        "decision_time_feature_schema_v2_pass_rate": readiness_rates["decision_time_feature_schema_v2_pass_rate"],
        "label_policy_v2_pass_rate": readiness_rates["label_policy_v2_pass_rate"],
        "post_trade_leakage_count": readiness_rates["post_trade_leakage_count"],
        "source_fact_ready": source_fact_ready,
        "ai_trader_registration_pending": True,
        "record_schema_version_consistent": record_schema_version_consistent,
    }
    if raw_refs_path:
        result["raw_source_refs_path"] = project_rel(root, raw_refs_path)
    if mirror_dir:
        result["training_mirror_dir"] = project_rel(root, mirror_dir)
        result["training_mirror_manifest_path"] = project_rel(root, mirror_dir / "dataset_manifest.json")
        result["training_mirror_coverage_path"] = project_rel(root, mirror_dir / "coverage_audit.json")
        result["training_mirror_source_refs_path"] = project_rel(root, mirror_dir / "source_refs.json")
        result["training_mirror_dataset_path"] = project_rel(root, mirror_dir / "training_dataset.jsonl")
        if (mirror_dir / "trade_snapshots.jsonl").exists():
            result["training_mirror_trade_snapshots_path"] = project_rel(root, mirror_dir / "trade_snapshots.jsonl")
    return result


def _rows_for_run(con: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    like = f"{safe_run_id(run_id)}:%"
    rows = con.execute(
        "SELECT * FROM trade_training_samples WHERE sample_id LIKE ? ORDER BY sample_id",
        (like,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in (
            "decision_time_input_json",
            "order_plan_json",
            "execution_fact_json",
            "post_trade_outcome_json",
            "label_json",
            "audit_context_json",
            "data_quality_json",
            "source_refs_json",
        ):
            item[key] = read_json(item.get(key), {})
        out.append(item)
    return out


def _leakage_violations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    for row in rows:
        decision_keys = walk_keys(row.get("decision_time_input_json"))
        for field in sorted(POST_TRADE_ONLY_FIELDS):
            if field in decision_keys:
                violations.append({"sample_id": row.get("sample_id"), "field": field, "location": "decision_time_input_json"})
    return violations


def _source_time_from_ref(ref: dict[str, Any], fallback_ms: int | None = None) -> int | None:
    for key in ("source_time_ms", "feature_timestamp_ms", "known_at_ms", "event_time_ms", "candle_open_time_ms"):
        got = parse_time_ms(ref.get(key))
        if got is not None:
            return got
    return fallback_ms


def _standardize_source_refs(
    refs: Any,
    *,
    fallback_db_path: str | None = None,
    fallback_table: str | None = None,
    fallback_row_id: str | None = None,
    fallback_time_ms: int | None = None,
) -> list[dict[str, Any]]:
    if isinstance(refs, dict):
        raw_refs = [refs]
    elif isinstance(refs, list):
        raw_refs = [ref for ref in refs if isinstance(ref, dict)]
    else:
        raw_refs = []
    if not raw_refs and (fallback_db_path or fallback_table or fallback_row_id):
        raw_refs = [
            {
                "source_db_path": fallback_db_path,
                "source_table": fallback_table,
                "source_row_id": fallback_row_id,
            }
        ]
    out: list[dict[str, Any]] = []
    for ref in raw_refs:
        row_id = ref.get("source_row_id") or ref.get("id") or fallback_row_id
        source_db_path = ref.get("source_db_path") or fallback_db_path
        source_table = ref.get("source_table") or fallback_table
        source_time_ms = _source_time_from_ref(ref, fallback_time_ms)
        item = {
            "source_db_path": source_db_path,
            "source_table": source_table,
            "source_row_id": row_id,
            "source_time_ms": source_time_ms,
            "source_hash": ref.get("source_hash") or ref.get("source_row_hash") or stable_hash(
                {
                    "source_db_path": source_db_path,
                    "source_table": source_table,
                    "source_row_id": row_id,
                    "source_time_ms": source_time_ms,
                }
            ),
        }
        for key in ("feature_timestamp_ms", "known_at_ms", "source_available_time_ms", "source_priority"):
            if key in ref:
                item[key] = ref.get(key)
        out.append(item)
    return out


def _lineage_source_refs(lineage: dict[str, Any], fallback_time_ms: int | None = None) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for field, info in (lineage or {}).items():
        if not isinstance(info, dict):
            continue
        refs.extend(
            _standardize_source_refs(
                [
                    {
                        "source_db_path": info.get("source_db_path"),
                        "source_table": info.get("source_table"),
                        "source_row_id": info.get("source_row_id"),
                        "source_time_ms": info.get("feature_timestamp_ms"),
                        "feature_timestamp_ms": info.get("feature_timestamp_ms"),
                        "known_at_ms": info.get("known_at_ms"),
                        "source_available_time_ms": info.get("source_available_time_ms"),
                        "source_priority": info.get("source_priority"),
                        "source_hash": info.get("source_hash"),
                        "field": field,
                    }
                ],
                fallback_time_ms=fallback_time_ms,
            )
        )
    return refs


def _sample_decision_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    decision = row.get("decision_time_input_json") if isinstance(row.get("decision_time_input_json"), dict) else {}
    snapshot = decision.get("entry_market_snapshot") if isinstance(decision.get("entry_market_snapshot"), dict) else {}
    return snapshot


def _liquidity_value(market: dict[str, Any]) -> float | None:
    quote_volume = _num(market.get("quote_volume"))
    if quote_volume is not None and quote_volume > 0:
        return quote_volume
    volume = _num(market.get("volume"))
    close = _num(market.get("close"))
    if volume is not None and close is not None and volume > 0 and close > 0:
        return volume * close
    return None


def _liquidity_bucket(value: float | None) -> str | None:
    if value is None:
        return None
    if value < 50_000:
        return "low"
    if value < 500_000:
        return "medium"
    return "high"


def _order_size_bucket(notional: float | None, liquidity: float | None) -> str | None:
    if notional is None or notional <= 0 or liquidity is None or liquidity <= 0:
        return None
    ratio = notional / liquidity
    if ratio < 0.005:
        return "tiny"
    if ratio < 0.02:
        return "small"
    if ratio < 0.08:
        return "normal"
    return "large"


def _market_regime_ref(market: dict[str, Any]) -> str | None:
    rsi = _num(market.get("rsi_14"))
    ema20 = _num(market.get("ema20_distance_bps"))
    ema60 = _num(market.get("ema60_distance_bps"))
    atr = _num(market.get("atr_14_bps"))
    volume_z = _num(market.get("volume_z"))
    range_pos = _num(market.get("range_pos_30m"))
    boll_pos = _num(market.get("bollinger_position"))
    high_vol = bool((atr is not None and atr >= 100) or (volume_z is not None and volume_z >= 1.0))
    suffix = "_high_vol" if high_vol else ""
    if ema20 is not None and ema60 is not None and rsi is not None:
        if ema20 > 0 and ema60 > 0 and rsi >= 55:
            return f"trend_up{suffix}"
        if ema20 < 0 and ema60 < 0 and rsi <= 45:
            return f"trend_down{suffix}"
    if range_pos is not None:
        if range_pos <= 0.2 or range_pos >= 0.8:
            return f"range_edge{suffix}"
        if 0.35 <= range_pos <= 0.65:
            return f"range_mid{suffix}"
    if boll_pos is not None:
        if boll_pos <= 0.2 or boll_pos >= 0.8:
            return f"range_edge{suffix}"
        if 0.35 <= boll_pos <= 0.65:
            return f"range_mid{suffix}"
    return f"neutral{suffix}"


def _planned_notional(order_plan: dict[str, Any], execution: dict[str, Any]) -> float | None:
    for key in ("planned_notional_usdt", "notional_usdt", "margin_usdt"):
        got = _num(execution.get(key))
        if got is not None:
            if key == "margin_usdt":
                leverage = _num(execution.get("leverage"))
                return got * leverage if leverage is not None else got
            return got
    for key in ("planned_notional_usdt", "notional_usdt", "margin_usdt"):
        got = _num(order_plan.get(key))
        if got is not None:
            if key == "margin_usdt":
                leverage = _num(order_plan.get("leverage"))
                return got * leverage if leverage is not None else got
            return got
    qty = _num(execution.get("quantity"))
    price = _num(execution.get("fill_price") or execution.get("reference_price") or order_plan.get("entry_price"))
    if qty is not None and price is not None:
        return abs(qty * price)
    return None


def _cost_context_lineage(
    *,
    field: str,
    source_db_path: str | None,
    source_table: str | None,
    source_row_id: str | None,
    feature_timestamp_ms: int | None,
    known_at_ms: int | None,
    source_priority: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "field": field,
        "schema_version": COST_CONTEXT_POLICY_VERSION,
        "feature_timestamp_ms": feature_timestamp_ms,
        "known_at_ms": known_at_ms,
        "source_available_time_ms": known_at_ms,
        "source_priority": source_priority,
        "source_db_path": source_db_path,
        "source_table": source_table,
        "source_row_id": source_row_id,
    }
    if extra:
        payload.update(extra)
    payload["source_hash"] = stable_hash(
        {
            "field": field,
            "source_db_path": source_db_path,
            "source_table": source_table,
            "source_row_id": source_row_id,
            "feature_timestamp_ms": feature_timestamp_ms,
            "known_at_ms": known_at_ms,
            "source_priority": source_priority,
            "extra": extra or {},
        }
    )
    payload["lineage_id"] = hashlib.sha256(f"{field}:{payload['source_hash']}".encode("utf-8")).hexdigest()[:24]
    return payload


def _enrich_full_cost_context(
    market: dict[str, Any],
    *,
    execution: dict[str, Any],
    order_plan: dict[str, Any],
    source_db_path: str | None,
    order_id: str | None,
) -> dict[str, Any]:
    enriched = dict(market or {})
    lineage = dict(enriched.get("field_lineage_json") if isinstance(enriched.get("field_lineage_json"), dict) else {})
    decision_ms = parse_time_ms(enriched.get("decision_time_ms"))
    feature_ts = parse_time_ms(enriched.get("feature_timestamp_ms") or enriched.get("candle_open_time_ms"))
    known_at = parse_time_ms(enriched.get("known_at_ms") or enriched.get("max_feature_known_at_ms") or decision_ms)
    market_source_db = enriched.get("source_db_path")
    market_source_table = enriched.get("source_table")
    market_source_row_id = enriched.get("source_row_id")

    spread = _num(execution.get("expected_spread_bps"))
    if spread is None:
        spread = _num(execution.get("spread_bps"))
    if spread is None:
        spread = _num(execution.get("slippage_bps"))
    if spread is not None:
        enriched["spread_bps"] = _round(spread, 8)
        lineage["spread_bps"] = _cost_context_lineage(
            field="spread_bps",
            source_db_path=source_db_path,
            source_table="paper_orders",
            source_row_id=order_id,
            feature_timestamp_ms=decision_ms,
            known_at_ms=decision_ms,
            source_priority="cost_model_proxy",
            extra={"policy_version": COST_CONTEXT_POLICY_VERSION, "source_field": "slippage_bps"},
        )

    liquidity = _liquidity_value(enriched)
    liquidity_bucket = _liquidity_bucket(liquidity)
    if liquidity_bucket is not None:
        enriched["liquidity_bucket"] = liquidity_bucket
        enriched["liquidity_value_usdt"] = _round(liquidity, 8)
        lineage["liquidity_bucket"] = _cost_context_lineage(
            field="liquidity_bucket",
            source_db_path=market_source_db,
            source_table=market_source_table,
            source_row_id=market_source_row_id,
            feature_timestamp_ms=feature_ts,
            known_at_ms=known_at,
            source_priority="rebuilt_from_kline",
            extra={"policy_version": COST_CONTEXT_POLICY_VERSION},
        )

    notional = _planned_notional(order_plan, execution)
    order_bucket = _order_size_bucket(notional, liquidity)
    if order_bucket is not None:
        enriched["order_size_bucket"] = order_bucket
        enriched["order_size_liquidity_ratio"] = _round(notional / liquidity if liquidity else None, 8)
        lineage["order_size_bucket"] = _cost_context_lineage(
            field="order_size_bucket",
            source_db_path=source_db_path,
            source_table="paper_orders+p21_klines_1m",
            source_row_id=f"{order_id}|{market_source_row_id}",
            feature_timestamp_ms=feature_ts,
            known_at_ms=max(value for value in (known_at, decision_ms) if value is not None) if (known_at or decision_ms) else None,
            source_priority="rebuilt_order_vs_kline_liquidity",
            extra={"policy_version": COST_CONTEXT_POLICY_VERSION, "planned_notional_usdt": _round(notional, 8)},
        )

    regime = _market_regime_ref(enriched)
    if regime is not None:
        enriched["market_regime_ref"] = regime
        enriched["market_regime_policy_version"] = MARKET_REGIME_POLICY_VERSION
        enriched["market_regime_source_time_ms"] = feature_ts
        lineage["market_regime_ref"] = _cost_context_lineage(
            field="market_regime_ref",
            source_db_path=market_source_db,
            source_table=market_source_table,
            source_row_id=market_source_row_id,
            feature_timestamp_ms=feature_ts,
            known_at_ms=known_at,
            source_priority="rebuilt_from_decision_time_indicators",
            extra={"policy_version": MARKET_REGIME_POLICY_VERSION},
        )
    enriched["field_lineage_json"] = lineage
    missing = [field for field in UNSTABLE_COST_CONTEXT_FIELDS if enriched.get(field) in (None, "")]
    existing_missing = [field for field in (enriched.get("missing_fields") or []) if field not in UNSTABLE_COST_CONTEXT_FIELDS]
    enriched["missing_fields"] = sorted(set(existing_missing + missing))
    return enriched


def _enrich_training_row_v2(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    snapshot = _sample_decision_snapshot(item)
    dq = item.get("data_quality_json") if isinstance(item.get("data_quality_json"), dict) else {}
    label = item.get("label_json") if isinstance(item.get("label_json"), dict) else {}
    execution_fact = item.get("execution_fact_json") if isinstance(item.get("execution_fact_json"), dict) else {}
    entry_execution = execution_fact.get("entry_fill") if isinstance(execution_fact.get("entry_fill"), dict) else {}
    order_plan = item.get("order_plan_json") if isinstance(item.get("order_plan_json"), dict) else {}
    decision = item.get("decision_time_input_json") if isinstance(item.get("decision_time_input_json"), dict) else {}
    snapshot = _enrich_full_cost_context(
        snapshot,
        execution=entry_execution,
        order_plan=order_plan,
        source_db_path=item.get("source_db_path"),
        order_id=item.get("order_id"),
    )
    if decision:
        decision = dict(decision)
        decision["entry_market_snapshot"] = snapshot
        item["decision_time_input_json"] = decision
    source_refs = _standardize_source_refs(
        item.get("source_refs_json"),
        fallback_db_path=item.get("source_db_path"),
        fallback_table="paper_orders",
        fallback_row_id=item.get("order_id"),
        fallback_time_ms=item.get("entry_time_ms"),
    )
    field_refs = _lineage_source_refs(
        snapshot.get("field_lineage_json") if isinstance(snapshot.get("field_lineage_json"), dict) else {},
        fallback_time_ms=snapshot.get("feature_timestamp_ms") or item.get("entry_time_ms"),
    )
    decision_time_ms = snapshot.get("decision_time_ms") or item.get("entry_time_ms")
    feature_timestamp_cutoff = snapshot.get("feature_timestamp_ms") or snapshot.get("candle_open_time_ms") or item.get("entry_time_ms")
    known_at_pass = bool(snapshot.get("known_at_pass") is True and dq.get("market_known_at_pass", True) is not False)
    market_complete = bool((snapshot.get("status") == "complete") or (dq.get("market_feature_completeness") == "complete"))
    label_ready = bool(label.get("training_label_ready") is True or dq.get("trade_quality_training_label_ready") is True)
    item.update(
        {
            "schema_version": EXPORT_SCHEMA_VERSION,
            "decision_time_ms": decision_time_ms,
            "feature_timestamp_cutoff": feature_timestamp_cutoff,
            "known_at_pass": known_at_pass,
            "market_feature_complete": market_complete,
            "label_coverage_status": "complete" if label_ready else str(label.get("review_status") or "needs_review"),
            "feature_schema_version": EXPORT_FEATURE_SCHEMA_VERSION,
            "known_at_policy_version": KNOWN_AT_POLICY_VERSION,
            "label_policy_version": LABEL_POLICY_VERSION,
            "source_refs": source_refs + field_refs,
            "audit_trace_id": stable_id("audit_trace", item.get("sample_id"), item.get("source_db_path"), item.get("order_id")),
        }
    )
    return item


def _event_source_refs(con: sqlite3.Connection, event_id: str, event: dict[str, Any], market: dict[str, Any]) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT source_db_path, source_table, source_pk_json, source_row_hash
        FROM trade_snapshot_source_refs
        WHERE event_id=?
        ORDER BY source_ref_id
        """,
        (event_id,),
    ).fetchall()
    refs: list[dict[str, Any]] = []
    for row in rows:
        pk = read_json(row["source_pk_json"], {})
        refs.extend(
            _standardize_source_refs(
                [
                    {
                        "source_db_path": row["source_db_path"],
                        "source_table": row["source_table"],
                        "source_row_id": pk.get("id") or pk.get("source_row_id"),
                        "source_hash": row["source_row_hash"],
                    }
                ],
                fallback_time_ms=event.get("event_time_ms"),
            )
        )
    refs.extend(
        _lineage_source_refs(
            market.get("field_lineage_json") if isinstance(market.get("field_lineage_json"), dict) else {},
            fallback_time_ms=market.get("feature_timestamp_ms") or event.get("event_time_ms"),
        )
    )
    if not refs:
        refs = _standardize_source_refs(
            [],
            fallback_db_path=event.get("source_db_path"),
            fallback_table=event.get("source_table"),
            fallback_row_id=event.get("source_row_id"),
            fallback_time_ms=event.get("event_time_ms"),
        )
    return refs


def _events_for_run_v2(con: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    like = f"{safe_run_id(run_id)}:%"
    rows = con.execute(
        """
        SELECT e.*, s.position_id
        FROM trade_snapshot_events e
        LEFT JOIN trade_training_samples s ON s.sample_id = e.sample_id
        WHERE e.sample_id LIKE ?
        ORDER BY e.sample_id, CASE e.event_action WHEN 'entry' THEN 0 WHEN 'exit' THEN 1 ELSE 2 END, e.event_id
        """,
        (like,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        event = dict(row)
        execution = read_json(event.get("execution_json"), {})
        order_plan = read_json(event.get("order_plan_json"), {})
        market = read_json(event.get("market_snapshot_json"), {})
        market = _enrich_full_cost_context(
            market,
            execution=execution,
            order_plan=order_plan,
            source_db_path=event.get("source_db_path"),
            order_id=event.get("order_id"),
        )
        extended = read_json(event.get("extended_market_context_json"), {})
        config = read_json(event.get("config_lineage_json"), {})
        data_quality = read_json(event.get("data_quality_json"), {})
        action = str(event.get("event_action") or "unknown").lower()
        refs = _event_source_refs(con, str(event["event_id"]), event, market)
        field_lineage = market.get("field_lineage_json") if isinstance(market.get("field_lineage_json"), dict) else {}
        missing_fields = list(data_quality.get("missing_fields_json") or [])
        missing_fields.extend(field for field in UNSTABLE_COST_CONTEXT_FIELDS if market.get(field) in (None, ""))
        out.append(
            {
                "event_id": event.get("event_id"),
                "sample_id": event.get("sample_id"),
                "order_id": event.get("order_id"),
                "position_id": event.get("position_id"),
                "source_mode": event.get("source_mode"),
                "source_db_path": event.get("source_db_path"),
                "source_table": event.get("source_table"),
                "source_row_id": event.get("source_row_id"),
                "strategy_line": event.get("strategy_line"),
                "symbol": event.get("symbol"),
                "side": event.get("side"),
                "action": action,
                "event_time_ms": event.get("event_time_ms"),
                "candle_open_time_ms": event.get("candle_open_time_ms"),
                "decision_time_ms": event.get("decision_time_ms"),
                "known_at_ms": event.get("known_at_ms"),
                "fill_price": execution.get("fill_price"),
                "quantity": execution.get("quantity"),
                "fee_bps": execution.get("fee_bps"),
                "fee_usdt": execution.get("fee_usdt"),
                "slippage_bps": execution.get("slippage_bps"),
                "slippage_usdt": execution.get("slippage_usdt"),
                "fill_model": execution.get("fill_model"),
                "cost_source": execution.get("cost_source"),
                "spread_bps": market.get("spread_bps"),
                "liquidity_bucket": market.get("liquidity_bucket"),
                "order_size_bucket": market.get("order_size_bucket"),
                "market_regime_ref": market.get("market_regime_ref"),
                "market_regime_policy_version": market.get("market_regime_policy_version"),
                "market_regime_source_time_ms": market.get("market_regime_source_time_ms"),
                "source_refs": refs,
                "market_snapshot_json": market,
                "field_lineage_json": field_lineage,
                "extended_market_context_json": extended,
                "config_lineage_json": config,
                "execution_json": execution,
                "known_at_pass": bool(market.get("known_at_pass") is True),
                "known_at_policy_version": KNOWN_AT_POLICY_VERSION,
                "event_role": "decision_time_input" if action == "entry" else "audit_label_outcome",
                "decision_time_input_allowed": action == "entry",
                "missing_fields_json": sorted(set(str(field) for field in missing_fields if field)),
            }
        )
    return out


def _cost_coverage(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {
            "stable_cost_fields_coverage": 0.0,
            "cost_fields_coverage": 0.0,
            "cost_missing_fields_json": list(STABLE_COST_FIELDS + UNSTABLE_COST_CONTEXT_FIELDS),
        }
    stable_total = len(events) * len(STABLE_COST_FIELDS)
    stable_present = sum(1 for event in events for field in STABLE_COST_FIELDS if event.get(field) not in (None, ""))
    all_fields = STABLE_COST_FIELDS + UNSTABLE_COST_CONTEXT_FIELDS
    all_total = len(events) * len(all_fields)
    all_present = sum(1 for event in events for field in all_fields if event.get(field) not in (None, ""))
    missing = sorted({field for event in events for field in all_fields if event.get(field) in (None, "")})
    return {
        "stable_cost_fields_coverage": round(stable_present / stable_total, 8) if stable_total else 0.0,
        "cost_fields_coverage": round(all_present / all_total, 8) if all_total else 0.0,
        "cost_missing_fields_json": missing,
    }


def _v2_readiness_rates(rows: list[dict[str, Any]], events: list[dict[str, Any]]) -> dict[str, Any]:
    sample_count = len(rows)
    if not sample_count:
        return {
            "decision_time_feature_schema_v2_pass_rate": 0.0,
            "label_policy_v2_pass_rate": 0.0,
            "post_trade_leakage_count": 0,
        }
    decision_pass = sum(
        1
        for row in rows
        if row.get("decision_time_ms") is not None
        and row.get("feature_timestamp_cutoff") is not None
        and row.get("known_at_pass") is True
        and row.get("market_feature_complete") is True
        and row.get("source_refs")
    )
    label_pass = sum(
        1
        for row in rows
        if row.get("label_coverage_status") == "complete"
        and (row.get("label_json") or {}).get("quality_label") in {"winner", "loser"}
    )
    post_trade_leakage_count = len(_leakage_violations(rows))
    return {
        "decision_time_feature_schema_v2_pass_rate": round(decision_pass / sample_count, 8),
        "label_policy_v2_pass_rate": round(label_pass / sample_count, 8),
        "post_trade_leakage_count": post_trade_leakage_count,
    }


def _round(value: float | None, digits: int = 8) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return round(float(value), digits)


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    alpha = 2 / (period + 1)
    ema = values[0]
    for value in values[1:]:
        ema = value * alpha + ema * (1 - alpha)
    return ema


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for idx in range(-period, 0):
        diff = values[idx] - values[idx - 1]
        gains.append(max(diff, 0.0))
        losses.append(abs(min(diff, 0.0)))
    avg_gain = mean(gains)
    avg_loss = mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _closed_candle_open_ms(decision_time_ms: int | None) -> int | None:
    if decision_time_ms is None:
        return None
    return (int(decision_time_ms) // 60_000) * 60_000 - 60_000


def _event_decision_time_ms(event: dict[str, Any]) -> int | None:
    decision_ms = event.get("decision_time_ms") or event.get("event_time_ms")
    event_ms = event.get("event_time_ms")
    try:
        decision_int = int(decision_ms) if decision_ms is not None else None
        event_int = int(event_ms) if event_ms is not None else None
    except Exception:
        return None
    if str(event.get("event_action") or "").lower() == "entry" and decision_int and event_int and decision_int > event_int:
        return event_int
    return decision_int


def _fetch_klines(conn: sqlite3.Connection, symbol: str, closed_open_ms: int, limit: int = 90) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT symbol, open_time_ms, open, high, low, close, volume, quote_volume, taker_buy_base_volume
        FROM p21_klines_1m
        WHERE symbol = ? AND open_time_ms <= ?
        ORDER BY open_time_ms DESC
        LIMIT ?
        """,
        (symbol.upper(), int(closed_open_ms), int(limit)),
    ).fetchall()
    return [dict(row) for row in reversed(rows)]


def _market_field_meta(
    root: Path,
    p21_db: Path,
    source_table: str,
    source_row_id: str,
    field: str,
    feature_ts: int | None,
    known_at: int | None,
) -> dict[str, Any]:
    return {
        "source_priority": "rebuilt",
        "source_db_path": project_rel(root, p21_db),
        "source_table": source_table,
        "source_row_id": source_row_id,
        "feature_timestamp_ms": feature_ts,
        "known_at_ms": known_at,
        "source_available_time_ms": known_at,
        "lineage_id": hashlib.sha256(f"{source_table}:{source_row_id}:{field}".encode("utf-8")).hexdigest()[:24],
        "schema_version": FEATURE_SCHEMA_VERSION,
    }


def _market_snapshot_from_klines(
    root: Path,
    p21_db: Path,
    event: dict[str, Any],
    klines: list[dict[str, Any]],
    *,
    max_source_lag_ms: int,
) -> dict[str, Any]:
    symbol = str(event.get("symbol") or "").upper()
    decision_ms = _event_decision_time_ms(event)
    closed_open = _closed_candle_open_ms(decision_ms) if decision_ms else None
    event_candle_ms = event.get("candle_open_time_ms")
    base = {
        "event_action": event.get("event_action"),
        "symbol": symbol,
        "side": event.get("side"),
        "event_time_ms": event.get("event_time_ms"),
        "event_candle_open_time_ms": event_candle_ms,
        "decision_time_ms": decision_ms,
        "known_at_policy": FEATURE_SCHEMA_VERSION,
    }
    if not klines:
        return {
            **base,
            "status": "missing_source",
            "missing_fields": REQUIRED_MARKET_FIELDS,
            "blocked_fields": [],
            "proxy_fields": [],
            "field_lineage_json": {},
            "reason": "missing_kline_source_window",
        }
    last = klines[-1]
    feature_ts = int(last["open_time_ms"])
    known_at = feature_ts + 60_000
    source_lag_ms = int(closed_open - feature_ts) if closed_open is not None else None
    stale_source = source_lag_ms is not None and source_lag_ms > int(max_source_lag_ms)
    source_row_id = f"{symbol}:{feature_ts}"
    closes = [value for value in (_num(row.get("close")) for row in klines) if value is not None]
    highs = [value for value in (_num(row.get("high")) for row in klines) if value is not None]
    lows = [value for value in (_num(row.get("low")) for row in klines) if value is not None]
    vols = [value for value in (_num(row.get("volume")) for row in klines) if value is not None]
    close = _num(last.get("close"))
    open_p = _num(last.get("open"))
    high = _num(last.get("high"))
    low = _num(last.get("low"))
    volume = _num(last.get("volume"))

    def pct(minutes: int) -> float | None:
        if len(closes) <= minutes or closes[-minutes - 1] in (None, 0):
            return None
        return closes[-1] / closes[-minutes - 1] - 1

    ema20 = _ema(closes[-60:], 20)
    ema60 = _ema(closes[-90:], 60)
    bollinger_position = None
    bollinger_width_bps = None
    if len(closes) >= 20 and close is not None:
        basis = mean(closes[-20:])
        sd = pstdev(closes[-20:])
        upper = basis + 2 * sd
        lower = basis - 2 * sd
        width = upper - lower
        bollinger_width_bps = width / basis * 10_000 if basis else None
        bollinger_position = (close - lower) / width if width > 0 else None
    atr_14 = None
    if close and len(klines) >= 15:
        trs: list[float] = []
        for idx in range(len(klines) - 14, len(klines)):
            cur_high = _num(klines[idx].get("high"))
            cur_low = _num(klines[idx].get("low"))
            prev_close = _num(klines[idx - 1].get("close"))
            if cur_high is None or cur_low is None or prev_close is None:
                continue
            trs.append(max(cur_high - cur_low, abs(cur_high - prev_close), abs(cur_low - prev_close)))
        if trs:
            atr_14 = mean(trs) / close * 10_000
    volume_z = None
    if len(vols) >= 21:
        base_vols = vols[-21:-1]
        sd = pstdev(base_vols)
        volume_z = (vols[-1] - mean(base_vols)) / sd if sd > 0 else 0.0
    range_pos_30m = None
    if close is not None and len(highs) >= 30 and len(lows) >= 30:
        high30 = max(highs[-30:])
        low30 = min(lows[-30:])
        range_pos_30m = (close - low30) / (high30 - low30) if high30 > low30 else None
    values = {
        "open": open_p,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "rsi_14": _round(_rsi(closes, 14), 6),
        "ema20_distance_bps": _round((close / ema20 - 1) * 10_000 if close is not None and ema20 else None, 6),
        "ema60_distance_bps": _round((close / ema60 - 1) * 10_000 if close is not None and ema60 else None, 6),
        "bollinger_position": _round(bollinger_position, 8),
        "bollinger_width_bps": _round(bollinger_width_bps, 6),
        "atr_14_bps": _round(atr_14, 6),
        "volume_z": _round(volume_z, 6),
        "pct_1m": _round(pct(1), 10),
        "pct_3m": _round(pct(3), 10),
        "pct_5m": _round(pct(5), 10),
        "pct_15m": _round(pct(15), 10),
        "range_pos_30m": _round(range_pos_30m, 8),
    }
    missing = [field for field in REQUIRED_MARKET_FIELDS if values.get(field) is None]
    field_lineage = {
        field: _market_field_meta(root, p21_db, "p21_klines_1m", source_row_id, field, feature_ts, known_at)
        for field, value in values.items()
        if value is not None
    }
    known_at_pass = bool(known_at <= int(decision_ms or 0)) if decision_ms else False
    blocked_fields: list[str] = []
    if not known_at_pass:
        blocked_fields.append("market_snapshot_known_after_decision")
    if stale_source:
        blocked_fields.append("market_snapshot_source_stale")
    status = "complete" if not missing and known_at_pass and not stale_source else "partial" if values else "missing_source"
    return {
        **base,
        "status": status,
        "candle_open_time_ms": feature_ts,
        "feature_timestamp_ms": feature_ts,
        "known_at_ms": known_at,
        "max_feature_known_at_ms": known_at,
        "known_at_pass": bool(known_at_pass and not stale_source),
        "source_priority": "rebuilt",
        "source_db_path": project_rel(root, p21_db),
        "source_table": "p21_klines_1m",
        "source_row_id": source_row_id,
        "source_lag_ms": source_lag_ms,
        "max_source_lag_ms": int(max_source_lag_ms),
        "schema_version": "step29.10.market-snapshot-reconstruction.v1",
        "missing_fields": missing,
        "blocked_fields": blocked_fields,
        "proxy_fields": [],
        "field_lineage_json": field_lineage,
        "reason": "stale_kline_source_window" if stale_source else None,
        **values,
    }


def _sample_data_quality(entry: dict[str, Any] | None, exit_snap: dict[str, Any] | None, existing: dict[str, Any]) -> dict[str, Any]:
    out = dict(existing or {})
    out["market_snapshot_status"] = (entry or {}).get("status")
    out["exit_market_snapshot_status"] = (exit_snap or {}).get("status")
    out["market_feature_completeness"] = "complete" if entry and exit_snap and entry.get("status") == "complete" and exit_snap.get("status") == "complete" else "incomplete"
    out["market_known_at_pass"] = bool((entry or {}).get("known_at_pass")) and bool((exit_snap or {}).get("known_at_pass"))
    missing = set(out.get("missing_fields_json") or [])
    blocked = set(out.get("blocked_fields_json") or [])
    for prefix, snap in (("entry_market", entry), ("exit_market", exit_snap)):
        if not snap:
            missing.add(f"{prefix}.missing_snapshot")
            continue
        for field in snap.get("missing_fields") or []:
            missing.add(f"{prefix}.{field}")
        for field in snap.get("blocked_fields") or []:
            blocked.add(f"{prefix}.{field}")
    out["missing_fields_json"] = sorted(missing)
    out["blocked_fields_json"] = sorted(blocked)
    return out


def _ensure_extended_context_column(con: sqlite3.Connection) -> None:
    cols = {row["name"] for row in con.execute("PRAGMA table_info(trade_snapshot_events)").fetchall()}
    if "extended_market_context_json" not in cols:
        con.execute("ALTER TABLE trade_snapshot_events ADD COLUMN extended_market_context_json TEXT NOT NULL DEFAULT '{}'")


def _fetch_oi(source: sqlite3.Connection, symbol: str, decision_time_ms: int) -> dict[str, Any] | None:
    try:
        row = source.execute(
            """
            SELECT *
            FROM market_oi_15m
            WHERE symbol=? AND period='15m' AND source_time_ms + ? <= ?
            ORDER BY source_time_ms DESC
            LIMIT 1
            """,
            (symbol, 15 * 60_000, decision_time_ms),
        ).fetchone()
    except sqlite3.Error:
        return None
    return dict(row) if row else None


def _fetch_funding(source: sqlite3.Connection, symbol: str, decision_time_ms: int) -> dict[str, Any] | None:
    try:
        row = source.execute(
            """
            SELECT *
            FROM market_funding_8h
            WHERE symbol=? AND funding_time_ms <= ?
            ORDER BY funding_time_ms DESC
            LIMIT 1
            """,
            (symbol, decision_time_ms),
        ).fetchone()
    except sqlite3.Error:
        return None
    return dict(row) if row else None


def _extended_lineage(root: Path, source_db: Path, field: str, source_table: str, source_row_id: str, feature_ts: int, known_at: int) -> dict[str, Any]:
    payload = {
        "field": field,
        "source_priority": "observed",
        "source_db_path": project_rel(root, source_db),
        "source_table": source_table,
        "source_row_id": source_row_id,
        "feature_timestamp_ms": feature_ts,
        "known_at_ms": known_at,
        "source_available_time_ms": known_at,
        "schema_version": "step29_extended_market_context_oi_funding_known_at_v1",
    }
    payload["lineage_id"] = stable_hash(payload)[:24]
    return payload


def _extended_context(root: Path, source_db: Path, source: sqlite3.Connection, event: dict[str, Any]) -> dict[str, Any]:
    symbol = str(event.get("symbol") or "").upper()
    decision_time = _event_decision_time_ms(event)
    context: dict[str, Any] = {
        "schema_version": "step29_extended_market_context_oi_funding_known_at_v1",
        "status": "missing",
        "event_action": event.get("event_action"),
        "symbol": symbol,
        "decision_time_ms": decision_time,
        "field_role": "decision_time_extended_context" if event.get("event_action") == "entry" else "audit_extended_context",
        "field_lineage_json": {},
        "observed_fields": [],
        "missing_fields": [],
        "blocked_fields": [],
        "known_at_pass": True,
    }
    if not symbol or decision_time is None:
        context["known_at_pass"] = False
        context["missing_fields"] = ["symbol_or_decision_time"]
        return context
    observed: list[str] = []
    oi = _fetch_oi(source, symbol, decision_time)
    if oi:
        oi_ts = int(oi["source_time_ms"])
        oi_known_at = oi_ts + 15 * 60_000
        row_id = f"{symbol}:{oi_ts}:15m"
        for field in ("oi_change", "oi_z"):
            if oi.get(field) is not None:
                context[field] = oi.get(field)
                observed.append(field)
                context["field_lineage_json"][field] = _extended_lineage(root, source_db, field, "market_oi_15m", row_id, oi_ts, oi_known_at)
        if oi_known_at > decision_time:
            context["known_at_pass"] = False
            context["blocked_fields"].append("oi_known_after_decision")
    funding = _fetch_funding(source, symbol, decision_time)
    if funding:
        funding_ts = int(funding["funding_time_ms"])
        row_id = f"{symbol}:{funding_ts}"
        for field in ("funding_rate", "funding_bucket", "funding_crowded_side"):
            if funding.get(field) is not None:
                context[field] = funding.get(field)
                observed.append(field)
                context["field_lineage_json"][field] = _extended_lineage(root, source_db, field, "market_funding_8h", row_id, funding_ts, funding_ts)
        if funding_ts > decision_time:
            context["known_at_pass"] = False
            context["blocked_fields"].append("funding_known_after_decision")
    for field in ("oi_change", "oi_state", "oi_z", "funding_rate", "funding_bucket", "funding_crowded_side"):
        if field not in observed:
            context["missing_fields"].append(field)
    context["observed_fields"] = observed
    known_ats = [
        item.get("known_at_ms")
        for item in context["field_lineage_json"].values()
        if isinstance(item, dict) and isinstance(item.get("known_at_ms"), int)
    ]
    context["max_feature_known_at_ms"] = max(known_ats) if known_ats else None
    context["status"] = "complete" if not context["missing_fields"] and context["known_at_pass"] else "partial" if observed else "missing"
    return context


def complete_scoped_known_at_reconstruction(
    project_root: Path,
    con: sqlite3.Connection | None = None,
    *,
    run_id: str | None = None,
    sandbox_id: str | None = None,
    source_mode: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    max_source_lag_ms: int = DEFAULT_MARKET_RECONSTRUCTION_MAX_SOURCE_LAG_MS,
    include_market: bool = True,
    include_extended: bool = True,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    p21_db = root / "DATA" / "backtest" / "p21_parameter_optimization.db"
    if not p21_db.exists():
        return {
            "status": "skipped",
            "reason": "p21_source_db_missing",
            "run_id": safe_run_id(run_id or ""),
            "sandbox_id": sandbox_id,
            "source_mode": source_mode,
            "market_feature_complete_rate": 0.0,
            "known_at_pass_rate": 0.0,
            "events_processed": 0,
            "samples_processed": 0,
        }

    owns_con = con is None
    sidecar = con or connect_sidecar(root)
    try:
        _ensure_extended_context_column(sidecar)
        clauses: list[str] = []
        params: list[Any] = []
        if run_id:
            clauses.append("sample_id LIKE ?")
            params.append(f"{safe_run_id(run_id)}:%")
        if source_mode:
            clauses.append("source_mode=?")
            params.append(source_mode)
        if sandbox_id:
            clauses.append("source_db_path LIKE ?")
            params.append(f"%/{sandbox_id}/%")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        query = f"SELECT * FROM trade_snapshot_events{where} ORDER BY sample_id, event_action"
        if limit:
            query += " LIMIT ?"
            params.append(int(limit))
        events = [dict(row) for row in sidecar.execute(query, tuple(params)).fetchall()]
        event_snapshots: dict[str, dict[str, Any]] = {}
        event_contexts: dict[str, dict[str, Any]] = {}
        status_counts: Counter[str] = Counter()
        known_at_pass = 0
        stale_count = 0
        unreconstructable: list[str] = []
        extended_entry_known_at = 0
        extended_entry_any = 0
        with connect_ro(p21_db) as source:
            for event in events:
                snapshot: dict[str, Any] = {}
                if include_market:
                    decision_ms = _event_decision_time_ms(event)
                    closed_open = _closed_candle_open_ms(decision_ms) if decision_ms else None
                    klines = _fetch_klines(source, str(event["symbol"]), closed_open) if event.get("symbol") and closed_open is not None else []
                    snapshot = _market_snapshot_from_klines(root, p21_db, event, klines, max_source_lag_ms=max_source_lag_ms)
                    event_snapshots[str(event["event_id"])] = snapshot
                    status_counts[str(snapshot.get("status"))] += 1
                    if snapshot.get("known_at_pass"):
                        known_at_pass += 1
                    if "market_snapshot_source_stale" in (snapshot.get("blocked_fields") or []):
                        stale_count += 1
                    if snapshot.get("status") != "complete" and len(unreconstructable) < 50:
                        unreconstructable.append(
                            f"{event.get('event_id')} {event.get('symbol')} {event.get('event_action')} {snapshot.get('reason') or snapshot.get('missing_fields') or snapshot.get('blocked_fields')}"
                        )
                    if not dry_run:
                        data_quality = _sample_data_quality(snapshot, None, read_json(event.get("data_quality_json"), {}))
                        field_roles = {
                            **read_json(event.get("field_roles_json"), {}),
                            "market_snapshot_json": "decision_time_feature" if event.get("event_action") == "entry" else "exit_audit_context",
                            "market_field_lineage": snapshot.get("field_lineage_json") or {},
                        }
                        sidecar.execute(
                            """
                            UPDATE trade_snapshot_events
                            SET market_snapshot_json=?, known_at_ms=?, decision_time_ms=?, data_quality_json=?, field_roles_json=?
                            WHERE event_id=?
                            """,
                            (
                                canonical_json(snapshot),
                                snapshot.get("max_feature_known_at_ms") or event.get("known_at_ms"),
                                snapshot.get("decision_time_ms") or event.get("decision_time_ms"),
                                canonical_json(data_quality),
                                canonical_json(field_roles),
                                event["event_id"],
                            ),
                        )
                if include_extended:
                    context = _extended_context(root, p21_db, source, event)
                    event_contexts[str(event["event_id"])] = context
                    if event.get("event_action") == "entry":
                        if context.get("observed_fields"):
                            extended_entry_any += 1
                        if context.get("known_at_pass") is True:
                            extended_entry_known_at += 1
                    if not dry_run:
                        sidecar.execute(
                            "UPDATE trade_snapshot_events SET extended_market_context_json=? WHERE event_id=?",
                            (canonical_json(context), event["event_id"]),
                        )

        sample_clauses: list[str] = []
        sample_params: list[Any] = []
        if run_id:
            sample_clauses.append("sample_id LIKE ?")
            sample_params.append(f"{safe_run_id(run_id)}:%")
        if source_mode:
            sample_clauses.append("source_mode=?")
            sample_params.append(source_mode)
        if sandbox_id:
            sample_clauses.append("source_db_path LIKE ?")
            sample_params.append(f"%/{sandbox_id}/%")
        sample_where = (" WHERE " + " AND ".join(sample_clauses)) if sample_clauses else ""
        samples = [dict(row) for row in sidecar.execute(f"SELECT * FROM trade_training_samples{sample_where} ORDER BY sample_id", tuple(sample_params)).fetchall()]
        paired = 0
        market_complete = 0
        tq_labeled = 0
        config_lineage_ok = 0
        leakage_violations: list[dict[str, Any]] = []
        for sample in samples:
            entry = event_snapshots.get(str(sample.get("entry_event_id")))
            exit_snap = event_snapshots.get(str(sample.get("exit_event_id")))
            entry_ext = event_contexts.get(str(sample.get("entry_event_id")))
            exit_ext = event_contexts.get(str(sample.get("exit_event_id")))
            if sample.get("entry_event_id") and sample.get("exit_event_id"):
                paired += 1
            if entry and exit_snap and entry.get("status") == "complete" and exit_snap.get("status") == "complete":
                market_complete += 1
            label = read_json(sample.get("label_json"), {})
            if label:
                tq_labeled += 1
            decision = read_json(sample.get("decision_time_input_json"), {})
            audit = read_json(sample.get("audit_context_json"), {})
            if include_market and entry:
                decision["entry_market_snapshot"] = entry
            if include_market and exit_snap:
                audit["exit_market_snapshot"] = {
                    **exit_snap,
                    "exit_event_id": sample.get("exit_event_id"),
                    "exit_time_ms": sample.get("exit_time_ms"),
                    "exit_candle_open_time_ms": exit_snap.get("candle_open_time_ms"),
                }
            if entry_ext and entry_ext.get("known_at_pass") is True and entry_ext.get("observed_fields"):
                decision["extended_market_context"] = entry_ext
            if exit_ext:
                audit["exit_extended_market_context"] = exit_ext
            existing_dq = read_json(sample.get("data_quality_json"), {})
            dq = _sample_data_quality(entry, exit_snap, existing_dq) if include_market else dict(existing_dq)
            if entry_ext:
                dq["extended_context_status"] = entry_ext.get("status")
                dq["extended_context_known_at_pass"] = bool(entry_ext.get("known_at_pass"))
                if entry_ext.get("missing_fields"):
                    dq["extended_context_missing_fields_json"] = entry_ext.get("missing_fields")
                if entry_ext.get("blocked_fields"):
                    dq["extended_context_blocked_fields_json"] = entry_ext.get("blocked_fields")
            config = decision.get("config_lineage") if isinstance(decision.get("config_lineage"), dict) else {}
            if config and not (config.get("missing_fields_json") or []):
                config_lineage_ok += 1
            forbidden = sorted(POST_TRADE_ONLY_FIELDS & walk_keys(decision))
            for field in forbidden:
                leakage_violations.append({"sample_id": sample.get("sample_id"), "field": field, "location": "decision_time_input_json"})
            if not dry_run:
                sidecar.execute(
                    """
                    UPDATE trade_training_samples
                    SET decision_time_input_json=?, audit_context_json=?, data_quality_json=?
                    WHERE sample_id=?
                    """,
                    (canonical_json(decision), canonical_json(audit), canonical_json(dq), sample["sample_id"]),
                )
        events_processed = len(events)
        samples_processed = len(samples)
        summary = {
            "status": "completed" if events_processed else "skipped",
            "reason": None if events_processed else "no_scoped_events",
            "run_id": safe_run_id(run_id or ""),
            "sandbox_id": sandbox_id,
            "source_mode": source_mode,
            "events_processed": events_processed,
            "samples_processed": samples_processed,
            "event_status_counts": dict(status_counts),
            "market_event_complete_rate": round(status_counts.get("complete", 0) / events_processed, 8) if events_processed else 0.0,
            "market_feature_complete_rate": round(market_complete / samples_processed, 8) if samples_processed else 0.0,
            "entry_exit_pair_rate": round(paired / samples_processed, 8) if samples_processed else 0.0,
            "known_at_pass_rate": round(known_at_pass / events_processed, 8) if events_processed else 0.0,
            "trade_quality_label_rate": round(tq_labeled / samples_processed, 8) if samples_processed else 0.0,
            "config_gate_lineage_rate": round(config_lineage_ok / samples_processed, 8) if samples_processed else 0.0,
            "stale_source_event_count": stale_count,
            "max_source_lag_ms": int(max_source_lag_ms),
            "extended_context_observed_any_rate": round(extended_entry_any / samples_processed, 8) if samples_processed else 0.0,
            "extended_context_known_at_pass_rate": round(extended_entry_known_at / samples_processed, 8) if samples_processed else 0.0,
            "leakage_violations": leakage_violations,
            "unreconstructable_examples": unreconstructable,
            "dry_run": dry_run,
        }
        if not dry_run:
            manifest_rows = sidecar.execute(
                "SELECT manifest_id, coverage_json FROM trade_snapshot_manifests WHERE run_id=? AND (? IS NULL OR source_mode=?)",
                (safe_run_id(run_id or ""), source_mode, source_mode),
            ).fetchall()
            for row in manifest_rows:
                coverage = read_json(row["coverage_json"], {})
                coverage.update(
                    {
                        "entry_exit_pair_rate": summary["entry_exit_pair_rate"],
                        "trade_quality_label_rate": summary["trade_quality_label_rate"],
                        "config_gate_lineage_rate": summary["config_gate_lineage_rate"],
                    }
                )
                if include_market:
                    coverage.update(
                        {
                            "market_feature_complete_rate": summary["market_feature_complete_rate"],
                            "known_at_pass_rate": summary["known_at_pass_rate"],
                            "known_at_status": "complete"
                            if summary["market_feature_complete_rate"] == 1.0 and summary["known_at_pass_rate"] == 1.0
                            else summary["status"],
                            "stale_source_event_count": summary["stale_source_event_count"],
                            "known_at_reconstruction": summary,
                        }
                    )
                if include_extended:
                    coverage["extended_context_observed_any_rate"] = summary["extended_context_observed_any_rate"]
                    coverage["extended_context_known_at_pass_rate"] = summary["extended_context_known_at_pass_rate"]
                    coverage["extended_context_reconstruction"] = summary
                sidecar.execute(
                    "UPDATE trade_snapshot_manifests SET coverage_json=? WHERE manifest_id=?",
                    (canonical_json(coverage), row["manifest_id"]),
                )
                if include_market:
                    sidecar.execute(
                        """
                        UPDATE trade_snapshot_coverage_audits
                        SET entry_exit_pair_rate=?, market_feature_complete_rate=?, trade_quality_label_rate=?,
                            config_gate_lineage_rate=?, known_at_pass_rate=?, leakage_violations_json=?
                        WHERE manifest_id=?
                        """,
                        (
                            summary["entry_exit_pair_rate"],
                            summary["market_feature_complete_rate"],
                            summary["trade_quality_label_rate"],
                            summary["config_gate_lineage_rate"],
                            summary["known_at_pass_rate"],
                            canonical_json(leakage_violations),
                            row["manifest_id"],
                        ),
                    )
            sidecar.commit()
        return summary
    finally:
        if owns_con:
            sidecar.close()


def sync_paper_sqlite_source(
    project_root: Path,
    *,
    source_db_path: Path | str,
    run_id: str,
    source_mode: str = "paper",
    limit: int = 1000,
    sandbox_id: str | None = None,
    cycle_id: str | None = None,
    resource_lane: str | None = None,
    source_chain: str | None = None,
    writer_context_id: str | None = None,
    export_dir: Path | str | None = None,
    mirror_dir: Path | str | None = None,
) -> dict[str, Any]:
    if os.environ.get(DISABLE_ENV) == "1":
        return disabled_payload(run_id, source_mode)
    root = Path(project_root)
    source_path = Path(source_db_path)
    source_rel = project_rel(root, source_path)
    resolved_export_dir = Path(export_dir) if export_dir else (sandbox_export_dir(root, sandbox_id=sandbox_id, run_id=run_id) if sandbox_id else None)
    resolved_mirror_dir = Path(mirror_dir) if mirror_dir else None
    try:
        with connect_ro(source_path) as source, connect_sidecar(root) as sidecar:
            cleanup_run(sidecar, run_id)
            names = table_names(source)
            if not {"paper_orders", "paper_fills"}.issubset(names):
                return _write_empty_failure_artifacts(
                    root,
                    sidecar,
                    run_id,
                    source_mode,
                    source_rel,
                    "missing_paper_tables",
                    export_dir=resolved_export_dir,
                    mirror_dir=resolved_mirror_dir,
                    source_table="paper_orders",
                    sandbox_id=sandbox_id,
                    cycle_id=cycle_id,
                    resource_lane=resource_lane,
                    source_chain=source_chain,
                    writer_context_id=writer_context_id,
                )
            order_ids = [
                str(row["order_id"])
                for row in source.execute(
                    """
                    SELECT order_id
                    FROM paper_fills
                    WHERE order_id IS NOT NULL
                    GROUP BY order_id
                    HAVING SUM(CASE WHEN lower(action)='entry' THEN 1 ELSE 0 END) >= 1
                       AND SUM(CASE WHEN lower(action)<>'entry' THEN 1 ELSE 0 END) >= 1
                    ORDER BY MIN(rowid)
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall()
            ]
            samples_written = 0
            events_written = 0
            tq_joined = 0
            for order_id in order_ids:
                order = _fetch_one(source, "SELECT * FROM paper_orders WHERE id=?", (order_id,))
                fills = _fetch_all(source, "SELECT * FROM paper_fills WHERE order_id=? ORDER BY rowid", (order_id,))
                positions = _fetch_all(source, "SELECT * FROM paper_positions WHERE order_id=? ORDER BY rowid", (order_id,)) if "paper_positions" in names else []
                entry = next((fill for fill in fills if str(fill.get("action", "")).lower() == "entry"), None)
                exit_fill = next((fill for fill in fills if str(fill.get("action", "")).lower() != "entry"), None)
                if not order or not entry or not exit_fill:
                    continue
                tq_rows = _fetch_all(source, "SELECT * FROM trade_quality_samples WHERE order_id=? ORDER BY rowid", (order_id,)) if "trade_quality_samples" in names else []
                tq_json = tq_rows[0] if tq_rows else {}
                if tq_json:
                    tq_joined += 1
                sample_id = f"{safe_run_id(run_id)}:{stable_id('sample', source_rel, order_id)}"
                lineage = _config_lineage(order, source_rel)
                plan = {key: order.get(key) for key in ("entry_price", "stop_loss", "take_profit", "tp1", "leverage", "sizing_method", "risk_budget_usdt")}
                entry_event_id = _insert_event(
                    sidecar,
                    run_id=run_id,
                    sample_id=sample_id,
                    order_id=order_id,
                    event_action="entry",
                    source_mode=source_mode,
                    source_rel=source_rel,
                    source_table="paper_fills",
                    source_row_id=str(entry.get("id") or order_id),
                    strategy_line=order.get("strategy_line"),
                    symbol=order.get("symbol"),
                    side=order.get("side"),
                    event_time_ms=parse_time_ms(entry.get("filled_at")),
                    candle_open_time_ms=parse_time_ms(entry.get("candle_open_time_ms")),
                    decision_time_ms=earliest_time_ms(order.get("created_at"), entry.get("consumed_at"), entry.get("filled_at")),
                    order_plan=plan,
                    execution=entry,
                    tq_json={},
                    lineage=lineage,
                )
                exit_event_id = _insert_event(
                    sidecar,
                    run_id=run_id,
                    sample_id=sample_id,
                    order_id=order_id,
                    event_action="exit",
                    source_mode=source_mode,
                    source_rel=source_rel,
                    source_table="paper_fills",
                    source_row_id=str(exit_fill.get("id") or order_id),
                    strategy_line=order.get("strategy_line"),
                    symbol=order.get("symbol"),
                    side=order.get("side"),
                    event_time_ms=parse_time_ms(exit_fill.get("filled_at")),
                    candle_open_time_ms=parse_time_ms(exit_fill.get("candle_open_time_ms")),
                    decision_time_ms=parse_time_ms(exit_fill.get("filled_at")),
                    order_plan=plan,
                    execution=exit_fill,
                    tq_json=tq_json,
                    lineage=lineage,
                )
                events_written += 2
                outcome = {
                    "realized_pnl_usdt": order.get("realized_pnl_usdt"),
                    "exit_price": order.get("exit_price"),
                    "exit_reason": order.get("exit_reason"),
                    "gross_pnl_usdt": exit_fill.get("gross_pnl_usdt"),
                    "net_pnl_usdt": exit_fill.get("net_pnl_usdt"),
                }
                post_trade, label, tq_update = _training_tq_payload(
                    tq_json,
                    source_rel=source_rel,
                    order_id=order_id,
                    existing_outcome=outcome,
                )
                entry_market = _market_stub(order.get("symbol"), parse_time_ms(entry.get("candle_open_time_ms")), "entry")
                dq = _merge_data_quality(_data_quality(entry_market, tq_json, lineage), tq_update)
                sidecar.execute(
                    """
                    INSERT OR REPLACE INTO trade_training_samples (
                        sample_id, order_id, position_id, intent_id, source_mode, source_db_path,
                        strategy_line, symbol, side, entry_event_id, exit_event_id,
                        entry_time_ms, exit_time_ms, decision_time_input_json, order_plan_json,
                        execution_fact_json, post_trade_outcome_json, label_json, audit_context_json,
                        data_quality_json, source_refs_json, schema_version, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        sample_id,
                        order_id,
                        positions[0].get("id") if positions else None,
                        order.get("intent_id"),
                        source_mode,
                        source_rel,
                        order.get("strategy_line"),
                        order.get("symbol"),
                        order.get("side"),
                        entry_event_id,
                        exit_event_id,
                        parse_time_ms(entry.get("filled_at")),
                        parse_time_ms(exit_fill.get("filled_at")),
                        canonical_json({"order_plan": plan, "entry_market_snapshot": entry_market, "config_lineage": lineage}),
                        canonical_json(order),
                        canonical_json({"entry_fill": entry, "exit_fill": exit_fill, "position": positions[0] if positions else {}}),
                        canonical_json(post_trade),
                        canonical_json(label),
                        canonical_json({"source_db_path": source_rel, "source_mode": source_mode}),
                        canonical_json(dq),
                        canonical_json(
                            [
                                {"source_db_path": source_rel, "source_table": "paper_orders", "id": order_id},
                                {"source_db_path": source_rel, "source_table": "paper_fills", "id": entry.get("id")},
                                {"source_db_path": source_rel, "source_table": "paper_fills", "id": exit_fill.get("id")},
                            ]
                        ),
                        SCHEMA_VERSION,
                        now_iso(),
                    ),
                )
                samples_written += 1
            return _finalize_run(
                root,
                sidecar,
                run_id=run_id,
                source_mode=source_mode,
                source_rel=source_rel,
                samples_written=samples_written,
                events_written=events_written,
                tq_joined=tq_joined,
                export_dir=resolved_export_dir,
                mirror_dir=resolved_mirror_dir,
                source_table="paper_orders",
                sandbox_id=sandbox_id,
                cycle_id=cycle_id,
                resource_lane=resource_lane,
                source_chain=source_chain,
                writer_context_id=writer_context_id,
            )
    except Exception as exc:  # pragma: no cover - caller must not rollback business chain
        return sync_failure_payload(run_id, source_mode, exc)


def sync_sandbox_sqlite_source(
    project_root: Path,
    *,
    source_db_path: Path | str,
    sandbox_id: str,
    run_id: str,
    source_mode: str = "sandbox_backtest",
    limit: int = 1000,
    cycle_id: str | None = None,
    resource_lane: str | None = None,
    source_chain: str | None = None,
    writer_context_id: str | None = None,
    mirror_dir: Path | str | None = None,
) -> dict[str, Any]:
    if os.environ.get(DISABLE_ENV) == "1":
        return disabled_payload(run_id, source_mode)
    root = Path(project_root)
    source_path = Path(source_db_path)
    source_rel = project_rel(root, source_path)
    export_dir = sandbox_export_dir(root, sandbox_id=sandbox_id, run_id=run_id)
    resolved_mirror_dir = Path(mirror_dir) if mirror_dir else None
    try:
        with connect_ro(source_path) as source, connect_sidecar(root) as sidecar:
            cleanup_run(sidecar, run_id)
            names = table_names(source)
            if "sandbox_orders" not in names:
                return _write_empty_failure_artifacts(
                    root,
                    sidecar,
                    run_id,
                    source_mode,
                    source_rel,
                    "missing_sandbox_orders",
                    export_dir=export_dir,
                    mirror_dir=resolved_mirror_dir,
                    source_table="sandbox_orders",
                    sandbox_id=sandbox_id,
                    cycle_id=cycle_id,
                    resource_lane=resource_lane,
                    source_chain=source_chain,
                    writer_context_id=writer_context_id,
                )
            orders = _fetch_all(
                source,
                """
                SELECT * FROM sandbox_orders
                WHERE sandbox_id=? AND exit_time_ms IS NOT NULL
                ORDER BY entry_time_ms ASC
                LIMIT ?
                """,
                (sandbox_id, int(limit)),
            )
            tq_by_order: dict[str, dict[str, Any]] = {}
            if "trade_quality_samples" in names:
                for row in _fetch_all(source, "SELECT * FROM trade_quality_samples WHERE sandbox_id=?", (sandbox_id,)):
                    if row.get("trade_id"):
                        tq_by_order[str(row["trade_id"])] = row
            samples_written = 0
            events_written = 0
            tq_joined = 0
            jsonl_rows: list[dict[str, Any]] = []
            for order in orders:
                order_id = str(order["order_id"])
                tq_json = tq_by_order.get(order_id, {})
                if tq_json:
                    tq_joined += 1
                sample_id = f"{safe_run_id(run_id)}:{stable_id('sample', source_rel, sandbox_id, order_id)}"
                lineage = _config_lineage(order, source_rel, source_table="sandbox_orders")
                plan = read_json(order.get("trade_plan_payload_json"), {})
                features = read_json(order.get("features_json"), {})
                fill_result = read_json(order.get("fill_result_json"), {})
                entry_time_ms = parse_time_ms(order.get("entry_time_ms"))
                exit_time_ms = parse_time_ms(order.get("exit_time_ms"))
                entry_event_id = _insert_event(
                    sidecar,
                    run_id=run_id,
                    sample_id=sample_id,
                    order_id=order_id,
                    event_action="entry",
                    source_mode=source_mode,
                    source_rel=source_rel,
                    source_table="sandbox_orders",
                    source_row_id=order_id,
                    strategy_line=order.get("strategy_line"),
                    symbol=order.get("symbol"),
                    side=order.get("side"),
                    event_time_ms=entry_time_ms,
                    candle_open_time_ms=entry_time_ms,
                    decision_time_ms=parse_time_ms(order.get("signal_time_ms")) or entry_time_ms,
                    order_plan=plan,
                    execution={"order": order, "event": "entry"},
                    tq_json={},
                    lineage=lineage,
                )
                exit_event_id = _insert_event(
                    sidecar,
                    run_id=run_id,
                    sample_id=sample_id,
                    order_id=order_id,
                    event_action="exit",
                    source_mode=source_mode,
                    source_rel=source_rel,
                    source_table="sandbox_orders",
                    source_row_id=order_id,
                    strategy_line=order.get("strategy_line"),
                    symbol=order.get("symbol"),
                    side=order.get("side"),
                    event_time_ms=exit_time_ms,
                    candle_open_time_ms=exit_time_ms,
                    decision_time_ms=exit_time_ms,
                    order_plan=plan,
                    execution={"order": order, "event": "exit", "fill_result": fill_result},
                    tq_json=tq_json,
                    lineage=lineage,
                )
                events_written += 2
                entry_market = {
                    "status": "direct_sandbox_features_partial",
                    "known_at_policy": FEATURE_SCHEMA_VERSION,
                    "features": features,
                    "missing_fields": ["full_kline_reconstruction_manifest"],
                }
                outcome = {
                    "net_R": order.get("net_R"),
                    "MFE_R": order.get("MFE_R"),
                    "MAE_R": order.get("MAE_R"),
                    "exit_reason": order.get("exit_reason"),
                    "exit_price": order.get("exit_price"),
                }
                dq = _data_quality(entry_market, tq_json, lineage)
                source_refs = [
                    {"source_db_path": source_rel, "source_table": "sandbox_orders", "id": order_id, "sandbox_id": sandbox_id}
                ]
                sidecar.execute(
                    """
                    INSERT OR REPLACE INTO trade_training_samples (
                        sample_id, order_id, position_id, intent_id, source_mode, source_db_path,
                        strategy_line, symbol, side, entry_event_id, exit_event_id,
                        entry_time_ms, exit_time_ms, decision_time_input_json, order_plan_json,
                        execution_fact_json, post_trade_outcome_json, label_json, audit_context_json,
                        data_quality_json, source_refs_json, schema_version, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        sample_id,
                        order_id,
                        None,
                        None,
                        source_mode,
                        source_rel,
                        order.get("strategy_line"),
                        order.get("symbol"),
                        order.get("side"),
                        entry_event_id,
                        exit_event_id,
                        entry_time_ms,
                        exit_time_ms,
                        canonical_json({"order_plan": plan, "entry_market_snapshot": entry_market, "config_lineage": lineage}),
                        canonical_json(plan),
                        canonical_json({"sandbox_order": order, "fill_result": fill_result}),
                        canonical_json(outcome),
                        canonical_json(tq_json),
                        canonical_json({"source_db_path": source_rel, "source_mode": source_mode, "sandbox_id": sandbox_id}),
                        canonical_json(dq),
                        canonical_json(source_refs),
                        SCHEMA_VERSION,
                        now_iso(),
                    ),
                )
                jsonl_rows.append({"sample_id": sample_id, "source_mode": source_mode, "order_id": order_id, "sandbox_id": sandbox_id})
                samples_written += 1
            result = _finalize_run(
                root,
                sidecar,
                run_id=run_id,
                source_mode=source_mode,
                source_rel=source_rel,
                samples_written=samples_written,
                events_written=events_written,
                tq_joined=tq_joined,
                export_dir=export_dir,
                mirror_dir=resolved_mirror_dir,
                source_table="sandbox_orders",
                sandbox_id=sandbox_id,
                cycle_id=cycle_id,
                resource_lane=resource_lane,
                source_chain=source_chain,
                writer_context_id=writer_context_id,
            )
            _write_sandbox_refs(root, export_dir, sandbox_id, source_rel, source_mode, run_id, jsonl_rows)
            if resolved_mirror_dir:
                resolved_mirror_dir.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(export_dir / "raw_source_refs.json", resolved_mirror_dir / "source_refs.json")
            result["sandbox_export_dir"] = project_rel(root, export_dir)
            return result
    except Exception as exc:  # pragma: no cover
        return sync_failure_payload(run_id, source_mode, exc)


def _fetch_one(con: sqlite3.Connection, query: str, params: tuple[Any, ...]) -> dict[str, Any]:
    row = con.execute(query, params).fetchone()
    return dict(row) if row else {}


def _fetch_all(con: sqlite3.Connection, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    return [dict(row) for row in con.execute(query, params).fetchall()]


def _write_empty_failure_artifacts(
    root: Path,
    con: sqlite3.Connection,
    run_id: str,
    source_mode: str,
    source_rel: str,
    reason: str,
    *,
    export_dir: Path | None = None,
    mirror_dir: Path | None = None,
    source_table: str | None = None,
    sandbox_id: str | None = None,
    cycle_id: str | None = None,
    resource_lane: str | None = None,
    source_chain: str | None = None,
    writer_context_id: str | None = None,
) -> dict[str, Any]:
    result = _finalize_run(
        root,
        con,
        run_id=run_id,
        source_mode=source_mode,
        source_rel=source_rel,
        samples_written=0,
        events_written=0,
        tq_joined=0,
        export_dir=export_dir,
        mirror_dir=mirror_dir,
        source_table=source_table,
        sandbox_id=sandbox_id,
        cycle_id=cycle_id,
        resource_lane=resource_lane,
        source_chain=source_chain,
        writer_context_id=writer_context_id,
        reason_codes=[reason],
    )
    result["training_dataset_status"] = "incomplete"
    result["reason"] = reason
    result["reason_codes"] = sorted(set([*(result.get("reason_codes") or []), reason]))
    return result


def _write_sandbox_refs(
    root: Path,
    export_dir: Path,
    sandbox_id: str,
    source_rel: str,
    source_mode: str,
    run_id: str,
    rows: list[dict[str, Any]],
) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)
    raw_refs = {
        "source_mode": source_mode,
        "sandbox_id": sandbox_id,
        "run_id": safe_run_id(run_id),
        "source_db_path": source_rel,
        "access_mode": "read_only",
        "tables": [
            "sandbox_manifest",
            "sandbox_orders",
            "trade_quality_samples",
            "gate_candidates",
            "paper_shadow_results",
            "fill_model_runs",
            "llm_dataset_exports",
        ],
        "generated_at": now_iso(),
    }
    (export_dir / "raw_source_refs.json").write_text(json.dumps(raw_refs, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    trade_snapshots_path = export_dir / "trade_snapshots.jsonl"
    if not trade_snapshots_path.exists():
        with trade_snapshots_path.open("w", encoding="utf-8", newline="\n") as fh:
            for row in rows:
                fh.write(canonical_json(row) + "\n")


def source_mode_for_sandbox_job(job_type: str) -> str:
    got = str(job_type or "").replace("-", "_")
    if got in {"external_full_backtest", "external_backtest", "external_paper_equivalent_backtest"}:
        return "external_paper_equivalent_backtest"
    if got in {"paper_shadow", "gated_paper_shadow"}:
        return "sandbox_paper_shadow"
    if got in {"replay", "gated_replay", "trade_quality", "gate_search", "config_export", "coarse_matrix"}:
        return "sandbox_backtest"
    return "sandbox_backtest"


def sync_sandbox_job_result(
    project_root: Path,
    *,
    sandbox_db_path: Path | str,
    sandbox_id: str,
    job_id: str,
    job_type: str,
) -> dict[str, Any]:
    return sync_sandbox_sqlite_source(
        project_root,
        source_db_path=sandbox_db_path,
        sandbox_id=sandbox_id,
        run_id=f"p29_{safe_run_id(job_id)}",
        source_mode=source_mode_for_sandbox_job(job_type),
    )
