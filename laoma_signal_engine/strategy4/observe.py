"""STEP17 Strategy4 persistent WAIT observe pool.

Strategy4 is a lightweight, persistent re-check layer for Strategy1
without_micro WAIT/retryable rejects. It reuses the without_micro evaluator
through trade_plan_lines line="strategy4" and never copies strategy logic.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import add_ttl_seconds, to_iso_z, utc_now
from laoma_signal_engine.decision.trade_plan_archive import archive_trade_plan_line_payload
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanItem, TradePlanLineDocument
from laoma_signal_engine.decision.trade_plan_lines import (
    _default_factor_path,
    _default_refresh_path,
    build_trade_plan_line_document,
    default_output_path,
    load_trade_plan_line_config,
)
from laoma_signal_engine.factors.models import FactorSnapshotDocument
from laoma_signal_engine.market.decision_refresh_models import DecisionRefreshDocument
from laoma_signal_engine.market.market_entry_liquidity_models import MarketEntryLiquidityDocument

OBSERVE_SCHEMA_VERSION = "17.1"
ATTEMPT_SCHEMA_VERSION = "17.6"
STRATEGY_LINE = "strategy4"
WITHOUT_MICRO_SOURCE = "trade_plan_without_micro"

RETRYABLE_REASON_CODES = {
    "wait_plan_only",
    "WAIT_REBOUND",
    "WAIT_PULLBACK",
    "WAIT_CONFIRMATION",
    "WAIT_FOR_RETEST",
    "better_entry_required",
    "better_entry_required_for_net_rr",
    "limit_entry_available",
    "breakout_trigger_required",
    "market_only_no_pending",
    "short_now_rebound_required",
    "long_now_pullback_required",
    "market_entry_bad_price_wait_rebound",
    "market_entry_bad_price_wait_pullback",
    "short_now_market_entry_bad_price_wait_rebound",
    "long_now_market_entry_bad_price_wait_pullback",
    "range_position_not_ready",
    "range_room_insufficient_after_refresh",
    "net_rr_too_low_but_waitable",
    "stop_too_wide_but_waitable",
}

HARD_DENY_REASON_CODES = {
    "score_too_low",
    "direction_invalid",
    "direction_invalid_after_refresh",
    "symbol_contract_invalid",
    "invalid_exchange_symbol",
    "execution_tier_no_trade",
    "business_pool_no_trade",
    "manual_blacklist",
    "risk_profile_hard_block",
    "liquidity_hard_block",
    "refresh_unrecoverably_stale",
    "liquidity_not_ok",
    "liquidity_stale",
}


def _now_iso() -> str:
    return to_iso_z(utc_now())


def _future_iso(seconds: int) -> str:
    return to_iso_z(add_ttl_seconds(utc_now(), seconds))


def _iso_ts(value: str | None) -> float:
    if not value:
        return 0.0
    text = value.replace("Z", "+00:00")
    try:
        from datetime import datetime

        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return 0.0


def _clean_reason_codes(values: Any) -> list[str]:
    if not isinstance(values, list | tuple | set):
        return []
    return sorted({str(value) for value in values if str(value or "").strip()})


def _fallback_wait_reason_codes(item: dict[str, Any], plan: TradePlanItem | None = None) -> list[str]:
    reasons = _clean_reason_codes(getattr(plan, "reason_codes", []) if plan is not None else [])
    if reasons:
        return reasons

    source_reasons = set(_clean_reason_codes(item.get("source_reason_codes")))
    last_reasons = set(_clean_reason_codes(item.get("last_reason_codes")))
    inherited = source_reasons | last_reasons
    if "refresh_missing" in inherited or "refresh_unrecoverably_stale" in inherited:
        return ["strategy4_refresh_missing"]
    if {"liquidity_missing", "liquidity_not_ok", "liquidity_stale"} & inherited:
        return ["strategy4_liquidity_missing"]
    if "factor_missing" in inherited:
        return ["strategy4_factor_missing"]
    if plan is None:
        return ["strategy4_plan_missing_for_due_symbol"]
    return ["strategy4_no_action_wait"]


def _root(project_root: Path | None = None) -> Path:
    return (project_root or Path.cwd()).resolve()


def pool_path(project_root: Path | None = None) -> Path:
    return _root(project_root) / "DATA" / "decisions" / "strategy4_observe_pool.json"


def status_path(project_root: Path | None = None) -> Path:
    return _root(project_root) / "DATA" / "runtime" / "strategy4_daemon_status.json"


def heartbeat_path(project_root: Path | None = None) -> Path:
    return _root(project_root) / "DATA" / "runtime" / "strategy4_heartbeat.json"


def pid_path(project_root: Path | None = None) -> Path:
    return _root(project_root) / "DATA" / "runtime" / "strategy4_daemon.pid"


def db_path(project_root: Path | None = None) -> Path:
    cfg = load_strategy4_config(project_root)
    raw = cfg.get("db_path") or "DATA/strategy4/strategy4_observe.db"
    return _root(project_root) / str(raw)


def load_strategy4_config(project_root: Path | None = None) -> dict[str, Any]:
    root = _root(project_root)
    path = root / "laoma_signal_engine" / "config" / "default.yaml"
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except OSError:
        raw = {}
    cfg = raw.get("strategy4") if isinstance(raw.get("strategy4"), dict) else {}
    return {
        "enabled": bool(cfg.get("enabled", True)),
        "inherit_from": str(cfg.get("inherit_from") or "without_micro"),
        "observe_interval_sec": int(cfg.get("observe_interval_sec") or 300),
        "max_symbols": int(cfg.get("max_symbols") or 200),
        "max_observe_age_sec": int(cfg.get("max_observe_age_sec") or 4 * 60 * 60),
        "retain_retryable_wait_after_ttl": bool(cfg.get("retain_retryable_wait_after_ttl", True)),
        "max_observe_attempts": int(cfg.get("max_observe_attempts") or 0),
        "rejudge_direction_each_attempt": bool(cfg.get("rejudge_direction_each_attempt", True)),
        "inherit_side": bool(cfg.get("inherit_side", False)),
        "observe_symbol_only": bool(cfg.get("observe_symbol_only", True)),
        "db_path": str(cfg.get("db_path") or "DATA/strategy4/strategy4_observe.db"),
    }


def _empty_pool(generated_at: str | None = None) -> dict[str, Any]:
    return {
        "schema_version": OBSERVE_SCHEMA_VERSION,
        "source": "strategy4_observe_pool",
        "generated_at": generated_at or _now_iso(),
        "status": "empty",
        "count": 0,
        "status_counts": {},
        "items": [],
        "rejected_items": [],
        "input_refs": {},
    }


def load_pool(project_root: Path | None = None) -> dict[str, Any]:
    path = pool_path(project_root)
    try:
        raw = read_json_object(path)
    except (OSError, ValueError, TypeError):
        return _empty_pool()
    return raw if isinstance(raw, dict) else _empty_pool()


def _pool_items_by_symbol(pool: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in pool.get("items") or []:
        if isinstance(row, dict) and row.get("symbol"):
            out[str(row["symbol"]).upper()] = dict(row)
    return out


def _write_pool(project_root: Path | None, payload: dict[str, Any]) -> None:
    items = [row for row in payload.get("items") or [] if isinstance(row, dict)]
    counts = Counter(str(row.get("status") or "unknown") for row in items)
    payload["count"] = len(items)
    payload["status_counts"] = dict(sorted(counts.items()))
    payload["status"] = "ok" if items else "empty"
    write_json_atomic(pool_path(project_root), payload)


def _conn(project_root: Path | None = None) -> sqlite3.Connection:
    path = db_path(project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    _init_db(con)
    return con


def _init_db(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy4_observe_pool (
            symbol TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            first_seen_at TEXT,
            updated_at TEXT,
            next_check_at TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            source_run_id TEXT,
            source_cycle_id TEXT,
            source_reason_codes_json TEXT,
            original_side TEXT,
            current_side TEXT,
            side_changed INTEGER NOT NULL DEFAULT 0,
            last_decision TEXT,
            last_action TEXT,
            last_entry_mode TEXT,
            last_reason_codes_json TEXT,
            evict_reason TEXT,
            ttl_age_sec INTEGER,
            ttl_expired INTEGER NOT NULL DEFAULT 0,
            retention_policy TEXT,
            lineage_json TEXT
        )
        """,
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy4_attempts (
            attempt_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            run_id TEXT,
            cycle_id TEXT,
            attempted_at TEXT,
            status TEXT NOT NULL,
            decision TEXT,
            original_side TEXT,
            current_side TEXT,
            side_changed INTEGER NOT NULL DEFAULT 0,
            action TEXT,
            entry_mode TEXT,
            executable INTEGER NOT NULL DEFAULT 0,
            reason_codes_json TEXT,
            plan_json TEXT,
            lineage_json TEXT
        )
        """,
    )
    for table, column_sql in (
        ("strategy4_observe_pool", "original_side TEXT"),
        ("strategy4_observe_pool", "current_side TEXT"),
        ("strategy4_observe_pool", "side_changed INTEGER NOT NULL DEFAULT 0"),
        ("strategy4_attempts", "original_side TEXT"),
        ("strategy4_attempts", "current_side TEXT"),
        ("strategy4_attempts", "side_changed INTEGER NOT NULL DEFAULT 0"),
        ("strategy4_observe_pool", "ttl_age_sec INTEGER"),
        ("strategy4_observe_pool", "ttl_expired INTEGER NOT NULL DEFAULT 0"),
        ("strategy4_observe_pool", "retention_policy TEXT"),
    ):
        col = column_sql.split()[0]
        exists = {
            str(row["name"])
            for row in con.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if col not in exists:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {column_sql}")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_strategy4_attempt_symbol_time ON strategy4_attempts(symbol, attempted_at)",
    )
    con.commit()


def _upsert_pool_rows(project_root: Path | None, items: list[dict[str, Any]]) -> None:
    with _conn(project_root) as con:
        for item in items:
            con.execute(
                """
                INSERT INTO strategy4_observe_pool (
                    symbol, status, first_seen_at, updated_at, next_check_at,
                    attempt_count, source_run_id, source_cycle_id,
                    source_reason_codes_json, original_side, current_side, side_changed,
                    last_decision, last_action,
                    last_entry_mode, last_reason_codes_json, evict_reason,
                    ttl_age_sec, ttl_expired, retention_policy, lineage_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    status=excluded.status,
                    updated_at=excluded.updated_at,
                    next_check_at=excluded.next_check_at,
                    attempt_count=excluded.attempt_count,
                    source_run_id=excluded.source_run_id,
                    source_cycle_id=excluded.source_cycle_id,
                    source_reason_codes_json=excluded.source_reason_codes_json,
                    original_side=excluded.original_side,
                    current_side=excluded.current_side,
                    side_changed=excluded.side_changed,
                    last_decision=excluded.last_decision,
                    last_action=excluded.last_action,
                    last_entry_mode=excluded.last_entry_mode,
                    last_reason_codes_json=excluded.last_reason_codes_json,
                    evict_reason=excluded.evict_reason,
                    ttl_age_sec=excluded.ttl_age_sec,
                    ttl_expired=excluded.ttl_expired,
                    retention_policy=excluded.retention_policy,
                    lineage_json=excluded.lineage_json
                """,
                (
                    item.get("symbol"),
                    item.get("status"),
                    item.get("first_seen_at"),
                    item.get("updated_at"),
                    item.get("next_check_at"),
                    int(item.get("attempt_count") or 0),
                    item.get("source_run_id"),
                    item.get("source_cycle_id"),
                    json.dumps(item.get("source_reason_codes") or [], ensure_ascii=False),
                    item.get("original_side"),
                    item.get("current_side"),
                    1 if item.get("side_changed") else 0,
                    item.get("last_decision"),
                    item.get("last_action"),
                    item.get("last_entry_mode"),
                    json.dumps(item.get("last_reason_codes") or [], ensure_ascii=False),
                    item.get("evict_reason"),
                    int(item.get("ttl_age_sec") or 0),
                    1 if item.get("ttl_expired") else 0,
                    item.get("retention_policy"),
                    json.dumps(item.get("lineage") or {}, ensure_ascii=False),
                ),
            )
        con.commit()


def _insert_attempt(project_root: Path | None, row: dict[str, Any]) -> None:
    with _conn(project_root) as con:
        con.execute(
            """
            INSERT OR REPLACE INTO strategy4_attempts (
                attempt_id, symbol, run_id, cycle_id, attempted_at, status,
                decision, original_side, current_side, side_changed,
                action, entry_mode, executable, reason_codes_json,
                plan_json, lineage_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("attempt_id"),
                row.get("symbol"),
                row.get("run_id"),
                row.get("cycle_id"),
                row.get("attempted_at"),
                row.get("status"),
                row.get("decision"),
                row.get("original_side"),
                row.get("current_side"),
                1 if row.get("side_changed") else 0,
                row.get("action"),
                row.get("entry_mode"),
                1 if row.get("executable") else 0,
                json.dumps(row.get("reason_codes") or [], ensure_ascii=False),
                json.dumps(row.get("plan") or {}, ensure_ascii=False),
                json.dumps(row.get("lineage") or {}, ensure_ascii=False),
            ),
        )
        con.commit()


def classify_strategy1_plan(plan: dict[str, Any]) -> dict[str, Any]:
    reasons = {str(x) for x in plan.get("reason_codes") or [] if str(x)}
    guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
    action = str(plan.get("action") or "")
    entry_mode = str(plan.get("entry_mode") or "")
    executable = bool(plan.get("executable"))
    decision = str(plan.get("decision") or "")
    hard = bool(reasons & HARD_DENY_REASON_CODES) or decision == "NO_TRADE"
    retryable = (
        not executable
        and not hard
        and (
            action == "WAIT"
            or entry_mode.startswith("WAIT")
            or bool(reasons & RETRYABLE_REASON_CODES)
            or bool(guards.get("better_entry_required"))
        )
    )
    if executable:
        state = "ignore_executable"
    elif hard:
        state = "hard_denied"
    elif retryable:
        state = "observe"
    else:
        state = "rejected_non_wait"
    return {
        "state": state,
        "retryable": retryable,
        "hard_deny": hard,
        "reason_codes": sorted(reasons),
    }


def sync_observe_pool_from_without_micro(
    *,
    project_root: Path | None = None,
    trade_plan_doc: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = _root(project_root)
    generated_at = _now_iso()
    if trade_plan_doc is None:
        trade_plan_doc = read_json_object(root / "DATA" / "decisions" / "latest_trade_plan_without_micro.json")
    if str(trade_plan_doc.get("source") or "") != WITHOUT_MICRO_SOURCE:
        raise ValueError("strategy4 admission requires latest without_micro trade plan source")
    old_pool = load_pool(root)
    old_by_symbol = _pool_items_by_symbol(old_pool)
    items = old_by_symbol
    rejected_items: list[dict[str, Any]] = []
    run_id = trade_plan_doc.get("run_id")
    cycle_id = trade_plan_doc.get("cycle_id")
    for plan in trade_plan_doc.get("plans") or []:
        if not isinstance(plan, dict) or not plan.get("symbol"):
            continue
        symbol = str(plan["symbol"]).upper()
        classified = classify_strategy1_plan(plan)
        if classified["retryable"]:
            prev = items.get(symbol, {})
            prev_status = str(prev.get("status") or "")
            restart_epoch = prev_status in {"evicted", "hard_denied", "executable"}
            first_seen = generated_at if restart_epoch else str(prev.get("first_seen_at") or generated_at)
            original_side = str((None if restart_epoch else prev.get("original_side")) or plan.get("decision") or "")
            items[symbol] = {
                "symbol": symbol,
                "status": "observing",
                "first_seen_at": first_seen,
                "updated_at": generated_at,
                "next_check_at": generated_at if restart_epoch else prev.get("next_check_at") or generated_at,
                "attempt_count": 0 if restart_epoch else int(prev.get("attempt_count") or 0),
                "source_run_id": run_id,
                "source_cycle_id": cycle_id,
                "source_reason_codes": classified["reason_codes"],
                "original_side": original_side,
                "current_side": str((None if restart_epoch else prev.get("current_side")) or plan.get("decision") or ""),
                "side_changed": False if restart_epoch else bool(prev.get("side_changed")),
                "last_decision": plan.get("decision"),
                "last_action": plan.get("action"),
                "last_entry_mode": plan.get("entry_mode"),
                "last_reason_codes": classified["reason_codes"],
                "evict_reason": "",
                "ttl_age_sec": 0,
                "ttl_expired": False,
                "retention_policy": "new_epoch_after_reentry" if restart_epoch else prev.get("retention_policy") or "normal",
                "lineage": {
                    "admission_source": "strategy1_without_micro",
                    "source_plan_hash": (plan.get("input_refs") or {}).get("source_plan_hash"),
                    "source_plan_run_id": run_id,
                    "source_plan_cycle_id": cycle_id,
                    "inherit_side": False,
                    "rejudge_direction_each_attempt": True,
                    "strategy4_restarted_observe_epoch": restart_epoch,
                },
            }
        else:
            rejected_items.append(
                {
                    "symbol": symbol,
                    "state": classified["state"],
                    "reason_codes": classified["reason_codes"],
                    "decision": plan.get("decision"),
                    "action": plan.get("action"),
                    "entry_mode": plan.get("entry_mode"),
                },
            )
    payload = {
        "schema_version": OBSERVE_SCHEMA_VERSION,
        "source": "strategy4_observe_pool",
        "generated_at": generated_at,
        "status": "ok",
        "count": 0,
        "status_counts": {},
        "input_refs": {
            "admission_source": WITHOUT_MICRO_SOURCE,
            "source_generated_at": trade_plan_doc.get("generated_at"),
            "source_run_id": run_id,
            "source_cycle_id": cycle_id,
            "retryable_reason_codes": sorted(RETRYABLE_REASON_CODES),
            "hard_deny_reason_codes": sorted(HARD_DENY_REASON_CODES),
        },
        "items": sorted(items.values(), key=lambda row: (str(row.get("next_check_at") or ""), str(row.get("symbol") or ""))),
        "rejected_items": rejected_items,
    }
    _write_pool(root, payload)
    _upsert_pool_rows(root, payload["items"])
    write_status(project_root=root, state="pool_synced", extra={"pool_count": payload["count"]})
    return payload


def _due_items(project_root: Path | None, pool: dict[str, Any]) -> list[dict[str, Any]]:
    cfg = load_strategy4_config(project_root)
    now_ts = time.time()
    max_age = int(cfg.get("max_observe_age_sec") or 0)
    retain_retryable = bool(cfg.get("retain_retryable_wait_after_ttl", True))
    max_attempts = int(cfg.get("max_observe_attempts") or 0)
    due: list[dict[str, Any]] = []
    retained: list[dict[str, Any]] = []
    changed = False
    for raw in pool.get("items") or []:
        if not isinstance(raw, dict):
            continue
        item = dict(raw)
        status = str(item.get("status") or "observing")
        first_seen_ts = _iso_ts(str(item.get("first_seen_at") or ""))
        age_sec = int(now_ts - first_seen_ts) if first_seen_ts > 0 else 0
        item["ttl_age_sec"] = max(0, age_sec)
        if max_age > 0 and first_seen_ts > 0 and age_sec > max_age:
            item["ttl_expired"] = True
        if max_attempts > 0 and int(item.get("attempt_count") or 0) >= max_attempts:
            item["status"] = "evicted"
            item["evict_reason"] = "max_attempts_expired"
            item["retention_policy"] = "max_attempts_evict"
            item["updated_at"] = _now_iso()
            changed = True
            retained.append(item)
            continue
        if max_age > 0 and first_seen_ts > 0 and now_ts - first_seen_ts > max_age:
            if status in {"observing", "still_wait"} and retain_retryable:
                item["retention_policy"] = "retryable_wait_retained_after_ttl"
                item["evict_reason"] = ""
                changed = True
                if _iso_ts(str(item.get("next_check_at") or "")) <= now_ts:
                    due.append(item)
                retained.append(item)
                continue
            item["status"] = "evicted"
            item["evict_reason"] = "observe_ttl_expired"
            item["retention_policy"] = "ttl_evict"
            item["updated_at"] = _now_iso()
            changed = True
            retained.append(item)
            continue
        item["ttl_expired"] = bool(item.get("ttl_expired")) if max_age <= 0 else False
        item["retention_policy"] = item.get("retention_policy") or "normal"
        if status in {"observing", "still_wait"} and _iso_ts(str(item.get("next_check_at") or "")) <= now_ts:
            due.append(item)
        retained.append(item)
    if changed:
        pool["items"] = retained
        pool["generated_at"] = _now_iso()
        _write_pool(project_root, pool)
        _upsert_pool_rows(project_root, retained)
    return due[: int(cfg.get("max_symbols") or 200)]


def _subset_factor_doc(project_root: Path | None, symbols: set[str]) -> FactorSnapshotDocument:
    root = _root(project_root)
    path = _default_factor_path(root, "strategy4")
    doc = FactorSnapshotDocument.model_validate(read_json_object(path))
    items = [item for item in doc.items if item.symbol.upper() in symbols]
    return doc.model_copy(
        update={
            "count": len(items),
            "items": items,
            "input_refs": {
                **dict(doc.input_refs or {}),
                "strategy4_subset_symbols": sorted(symbols),
                "strategy4_subset_source": str(path),
            },
        },
    )


def _read_optional_refresh(project_root: Path | None) -> DecisionRefreshDocument:
    root = _root(project_root)
    return DecisionRefreshDocument.model_validate(read_json_object(_default_refresh_path(root, "strategy4")))


def _read_optional_liquidity(project_root: Path | None) -> MarketEntryLiquidityDocument | None:
    root = _root(project_root)
    path = root / "DATA" / "market" / "latest_market_entry_liquidity.json"
    if not path.is_file():
        return None
    return MarketEntryLiquidityDocument.model_validate(read_json_object(path))


def _update_item_from_plan(item: dict[str, Any], plan: TradePlanItem | None, now: str, interval_sec: int) -> dict[str, Any]:
    out = dict(item)
    if plan is None:
        fallback_reasons = _fallback_wait_reason_codes(out, plan=None)
        out.update(
            {
                "status": "still_wait",
                "updated_at": now,
                "next_check_at": _future_iso(interval_sec),
                "last_action": out.get("last_action") or "WAIT",
                "last_entry_mode": out.get("last_entry_mode") or "WAIT_EVIDENCE_MISSING",
                "last_reason_codes": fallback_reasons,
                "evict_reason": "",
            },
        )
        return out
    reason_codes = _fallback_wait_reason_codes(out, plan)
    reasons = set(reason_codes)
    hard = bool(reasons & HARD_DENY_REASON_CODES) or plan.decision == "NO_TRADE"
    if plan.executable:
        status = "executable"
        next_check_at = ""
        evict_reason = ""
    elif hard:
        status = "hard_denied"
        next_check_at = ""
        evict_reason = ",".join(sorted(reasons & HARD_DENY_REASON_CODES)) or "hard_deny"
    else:
        status = "still_wait"
        next_check_at = _future_iso(interval_sec)
        evict_reason = ""
    out.update(
        {
            "status": status,
            "updated_at": now,
            "next_check_at": next_check_at,
            "attempt_count": int(out.get("attempt_count") or 0) + 1,
            "original_side": out.get("original_side") or plan.decision,
            "current_side": plan.decision,
            "side_changed": bool((out.get("original_side") or plan.decision) and (out.get("original_side") or plan.decision) != plan.decision),
            "last_decision": plan.decision,
            "last_action": plan.action,
            "last_entry_mode": plan.entry_mode,
            "last_reason_codes": reason_codes,
            "evict_reason": evict_reason,
            "lineage": {
                **dict(out.get("lineage") or {}),
                "last_strategy4_generated_at": now,
                "strategy4_source": "trade_plan_strategy4",
            },
        },
    )
    return out


def run_strategy4_once(
    *,
    project_root: Path | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    max_symbols: int | None = None,
) -> dict[str, Any]:
    root = _root(project_root)
    cfg = load_strategy4_config(root)
    if not cfg.get("enabled", True):
        write_status(project_root=root, state="disabled")
        return {"status": "disabled", "count": 0, "executable_count": 0}
    pool = load_pool(root)
    due = _due_items(root, pool)
    if max_symbols is not None:
        due = due[:max_symbols]
    generated_at = _now_iso()
    if not due:
        doc = TradePlanLineDocument(
            generated_at=generated_at,
            run_id=run_id,
            cycle_id=cycle_id,
            source="trade_plan_strategy4",
            micro_mode="none",
            status="no_entries",
            count=0,
            executable_count=0,
            input_refs={"strategy4_due_count": 0, "observe_pool_path": str(pool_path(root))},
            plans=[],
        )
        return _write_strategy4_doc(root, doc, generated_at, due)
    due_symbols = {str(item["symbol"]).upper() for item in due if item.get("symbol")}
    factor_doc = _subset_factor_doc(root, due_symbols)
    refresh_doc = _read_optional_refresh(root)
    liquidity_doc = _read_optional_liquidity(root)
    doc = build_trade_plan_line_document(
        line="strategy4",
        factor_doc=factor_doc,
        refresh_doc=refresh_doc,
        liquidity_doc=liquidity_doc,
        micro_doc=None,
        micro_state_doc=None,
        generated_at=generated_at,
        run_id=run_id,
        cycle_id=cycle_id,
        cfg=load_trade_plan_line_config(root, "strategy4"),
        project_root=root,
    )
    plans: list[TradePlanItem] = []
    lineage_by_symbol = {str(item.get("symbol") or "").upper(): dict(item.get("lineage") or {}) for item in due}
    for plan in doc.plans:
        pool_item = next((item for item in due if str(item.get("symbol") or "").upper() == plan.symbol.upper()), {})
        next_attempt_count = int(pool_item.get("attempt_count") or 0) + 1 if isinstance(pool_item, dict) else 1
        side_changed = bool(
            isinstance(pool_item, dict)
            and (pool_item.get("original_side") or plan.decision)
            and (pool_item.get("original_side") or plan.decision) != plan.decision
        )
        attempt_id = f"{generated_at}_{plan.symbol.upper()}_{next_attempt_count}".replace(":", "").replace("-", "")
        lineage = {
            **lineage_by_symbol.get(plan.symbol.upper(), {}),
            "origin_run_id": (pool_item or {}).get("source_run_id") if isinstance(pool_item, dict) else None,
            "origin_cycle_id": (pool_item or {}).get("source_cycle_id") if isinstance(pool_item, dict) else None,
            "origin_strategy_line": "without_micro",
            "origin_reason_codes": list((pool_item or {}).get("source_reason_codes") or []) if isinstance(pool_item, dict) else [],
            "sidecar_strategy_line": "strategy4",
            "sidecar_attempt_id": attempt_id,
            "sidecar_attempted_at": generated_at,
            "sidecar_generated_at": generated_at,
            "observe_pool_updated_at": (pool_item or {}).get("updated_at") if isinstance(pool_item, dict) else None,
            "original_side": (pool_item or {}).get("original_side") if isinstance(pool_item, dict) else None,
            "current_side": plan.decision,
            "side_changed": side_changed,
        }
        guards = dict(plan.guards)
        guards.update(
            {
                "strategy4_observe": True,
                "strategy4_rejudge_direction": True,
                "strategy4_observe_interval_sec": cfg["observe_interval_sec"],
                "strategy4_lineage": lineage,
            },
        )
        input_refs = dict(plan.input_refs)
        input_refs.update(
            {
                "strategy4_lineage": lineage,
                "strategy4_observe_pool_path": str(pool_path(root)),
                "strategy4_observe_db_path": str(db_path(root)),
            },
        )
        plans.append(plan.model_copy(update={"guards": guards, "input_refs": input_refs}))
    doc = doc.model_copy(
        update={
            "plans": plans,
            "count": len(plans),
            "executable_count": sum(1 for plan in plans if plan.executable),
            "input_refs": {
                **dict(doc.input_refs or {}),
                "strategy4_due_symbols": sorted(due_symbols),
                "strategy4_due_count": len(due),
                "observe_pool_path": str(pool_path(root)),
                "observe_db_path": str(db_path(root)),
                "strategy4_reuses_without_micro_evaluator": True,
            },
        },
    )
    return _write_strategy4_doc(root, doc, generated_at, due)


def _write_strategy4_doc(root: Path, doc: TradePlanLineDocument, generated_at: str, due: list[dict[str, Any]]) -> dict[str, Any]:
    out_p = default_output_path(root, "strategy4")
    payload = archive_trade_plan_line_payload(
        root=root,
        line="strategy4",
        payload=doc.model_dump(mode="json"),
        latest_path=out_p,
    )
    write_json_atomic(out_p, payload)
    _settle_pool_after_attempt(root, doc, due, generated_at)
    write_status(
        project_root=root,
        state="ok",
        extra={
            "last_run_id": doc.run_id,
            "last_cycle_id": doc.cycle_id,
            "last_generated_at": generated_at,
            "due_count": len(due),
            "plan_count": doc.count,
            "executable_count": doc.executable_count,
            "latest_trade_plan_path": str(out_p),
        },
    )
    return payload


def _settle_pool_after_attempt(project_root: Path | None, doc: TradePlanLineDocument, due: list[dict[str, Any]], now: str) -> None:
    pool = load_pool(project_root)
    by_symbol = _pool_items_by_symbol(pool)
    plan_by_symbol = {plan.symbol.upper(): plan for plan in doc.plans}
    interval = int(load_strategy4_config(project_root).get("observe_interval_sec") or 300)
    updated: list[dict[str, Any]] = []
    for item in due:
        symbol = str(item.get("symbol") or "").upper()
        if not symbol:
            continue
        plan = plan_by_symbol.get(symbol)
        new_item = _update_item_from_plan(by_symbol.get(symbol, item), plan, now, interval)
        by_symbol[symbol] = new_item
        updated.append(new_item)
        attempt_id = f"{now}_{symbol}_{int(new_item.get('attempt_count') or 0)}".replace(":", "").replace("-", "")
        _insert_attempt(
            project_root,
            {
                "attempt_id": attempt_id,
                "symbol": symbol,
                "run_id": doc.run_id,
                "cycle_id": doc.cycle_id,
                "attempted_at": now,
                "status": new_item.get("status"),
                "decision": getattr(plan, "decision", None),
                "original_side": new_item.get("original_side"),
                "current_side": new_item.get("current_side"),
                "side_changed": bool(new_item.get("side_changed")),
                "action": getattr(plan, "action", None) or new_item.get("last_action"),
                "entry_mode": getattr(plan, "entry_mode", None) or new_item.get("last_entry_mode"),
                "executable": getattr(plan, "executable", False),
                "reason_codes": new_item.get("last_reason_codes") or _fallback_wait_reason_codes(new_item, plan),
                "plan": plan.model_dump(mode="json") if plan else {},
                "lineage": new_item.get("lineage") or {},
            },
        )
    pool["items"] = sorted(by_symbol.values(), key=lambda row: (str(row.get("status") or ""), str(row.get("symbol") or "")))
    pool["generated_at"] = now
    _write_pool(project_root, pool)
    _upsert_pool_rows(project_root, list(by_symbol.values()))


def write_status(*, project_root: Path | None = None, state: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "schema_version": "17.8",
        "source": "strategy4_runtime_health",
        "generated_at": _now_iso(),
        "state": state,
        "pid": os.getpid(),
        "observe_pool_path": str(pool_path(project_root)),
        "heartbeat_path": str(heartbeat_path(project_root)),
        "db_path": str(db_path(project_root)),
        **(extra or {}),
    }
    write_json_atomic(status_path(project_root), payload)
    write_json_atomic(heartbeat_path(project_root), {"generated_at": payload["generated_at"], "pid": os.getpid(), "state": state})
    return payload


def runtime_status(project_root: Path | None = None) -> dict[str, Any]:
    root = _root(project_root)
    try:
        status = read_json_object(status_path(root))
    except (OSError, ValueError, TypeError):
        status = {}
    try:
        heartbeat = read_json_object(heartbeat_path(root))
    except (OSError, ValueError, TypeError):
        heartbeat = {}
    pool = load_pool(root)
    return {
        "schema_version": "17.8",
        "source": "strategy4_runtime_health",
        "generated_at": _now_iso(),
        "status": status,
        "heartbeat": heartbeat,
        "pool": {
            "count": pool.get("count", 0),
            "status_counts": pool.get("status_counts") or {},
            "generated_at": pool.get("generated_at"),
        },
    }


def run_daemon(*, project_root: Path | None = None, once: bool = False) -> int:
    root = _root(project_root)
    cfg = load_strategy4_config(root)
    interval = int(cfg.get("observe_interval_sec") or 300)
    pid_path(root).parent.mkdir(parents=True, exist_ok=True)
    pid_path(root).write_text(str(os.getpid()), encoding="utf-8")
    try:
        while True:
            write_status(project_root=root, state="running")
            run_strategy4_once(project_root=root)
            if once:
                return EXIT_SUCCESS
            time.sleep(interval)
    except KeyboardInterrupt:
        write_status(project_root=root, state="stopped")
        return EXIT_SUCCESS
    except (OSError, ValueError, ValidationError) as exc:
        write_status(project_root=root, state="error", extra={"error": str(exc)})
        print(f"[ERROR] strategy4 daemon failed: {exc}", file=sys.stderr)
        return EXIT_CONFIG if isinstance(exc, ValidationError) else EXIT_INTERNAL


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Strategy4 observe pool utilities")
    parser.add_argument("action", choices=["sync", "run-once", "status", "daemon"])
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--cycle-id", default=None)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--stdout-json", action="store_true")
    ns = parser.parse_args(argv)
    try:
        if ns.action == "sync":
            result = sync_observe_pool_from_without_micro(project_root=ns.project_root)
        elif ns.action == "run-once":
            result = run_strategy4_once(
                project_root=ns.project_root,
                run_id=ns.run_id,
                cycle_id=ns.cycle_id,
                max_symbols=ns.max_symbols,
            )
        elif ns.action == "status":
            result = runtime_status(ns.project_root)
        else:
            return run_daemon(project_root=ns.project_root)
        if ns.stdout_json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return EXIT_SUCCESS
    except (OSError, ValueError, ValidationError) as exc:
        print(f"[ERROR] strategy4 action failed: {exc}", file=sys.stderr)
        return EXIT_CONFIG if isinstance(exc, ValidationError) else EXIT_INTERNAL


if __name__ == "__main__":
    raise SystemExit(main())
