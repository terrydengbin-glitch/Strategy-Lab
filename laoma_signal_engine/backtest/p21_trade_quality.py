from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.research_db import upsert_backtest_tq_sample_native
from laoma_signal_engine.backtest.p21_v2 import _connect, _iso_from_ms, _loads, _num, ensure_p21_v2_tables
from laoma_signal_engine.trade_quality.diagnostics import (
    _aggregate_row,
    _dimension_attribution,
    _performance_stats,
    _root_cause_attribution,
)

BACKTEST_TQ_SOURCE = "backtest_p21_v2"
BACKTEST_TQ_SCHEMA_VERSION = "21.19-backtest-trade-quality"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _stable_id(prefix: str, payload: Any, size: int = 20) -> str:
    raw = _json(payload).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(raw).hexdigest()[:size]}"


def _read_connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True, timeout=2)
    conn.execute("PRAGMA busy_timeout=2000")
    conn.row_factory = sqlite3.Row
    return conn


def package_key(experiment_id: str, strategy_line: str | None = None, parameter_set_id: str | None = None) -> str:
    parts = ["backtest", experiment_id]
    if strategy_line:
        parts.append(strategy_line)
    if parameter_set_id:
        parts.append(parameter_set_id)
    return ":".join(parts)


def ensure_backtest_trade_quality_tables(db_path: Path) -> None:
    ensure_p21_v2_tables(db_path)
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS backtest_trade_quality_samples(
              diagnostic_id TEXT PRIMARY KEY,
              source TEXT NOT NULL,
              package_key TEXT NOT NULL,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              order_id TEXT NOT NULL,
              trade_id TEXT NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              entry_time TEXT,
              exit_time TEXT,
              entry_time_ms INTEGER,
              exit_time_ms INTEGER,
              entry_price REAL,
              exit_price REAL,
              planned_SL REAL,
              planned_TP REAL,
              planned_RR REAL,
              holding_minutes REAL,
              gross_pnl REAL,
              fee REAL,
              net_pnl REAL,
              net_R REAL,
              MFE_R REAL,
              MAE_R REAL,
              replay_status TEXT,
              root_cause TEXT,
              root_cause_confidence REAL,
              entry_quality_label TEXT,
              entry_context_v3_label TEXT,
              exit_reason TEXT,
              evidence_json TEXT NOT NULL,
              source_payload_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              UNIQUE(experiment_id, parameter_set_id, order_id)
            );
            CREATE INDEX IF NOT EXISTS idx_bt_tq_samples_package
              ON backtest_trade_quality_samples(package_key, root_cause, symbol, side);
            CREATE INDEX IF NOT EXISTS idx_bt_tq_samples_exp_param
              ON backtest_trade_quality_samples(experiment_id, strategy_line, parameter_set_id);
            CREATE INDEX IF NOT EXISTS idx_bt_tq_samples_symbol
              ON backtest_trade_quality_samples(symbol, side, exit_time);

            CREATE TABLE IF NOT EXISTS backtest_trade_quality_rollups(
              rollup_id TEXT PRIMARY KEY,
              package_key TEXT NOT NULL,
              experiment_id TEXT NOT NULL,
              strategy_line TEXT,
              parameter_set_id TEXT,
              dimension TEXT NOT NULL,
              key TEXT NOT NULL,
              sample_count INTEGER NOT NULL,
              metrics_json TEXT NOT NULL,
              evidence_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(package_key, dimension, key)
            );
            CREATE INDEX IF NOT EXISTS idx_bt_tq_rollups_package
              ON backtest_trade_quality_rollups(package_key, dimension, sample_count);
            """
        )


def _where(filters: dict[str, Any], *, table_alias: str = "") -> tuple[str, list[Any]]:
    prefix = f"{table_alias}." if table_alias else ""
    clauses: list[str] = []
    params: list[Any] = []
    for key in (
        "package_key",
        "experiment_id",
        "strategy_line",
        "parameter_set_id",
        "symbol",
        "side",
        "exit_reason",
        "root_cause",
        "entry_quality_label",
        "entry_context_v3_label",
    ):
        value = filters.get(key)
        if value in (None, "", "all"):
            continue
        clauses.append(f"{prefix}{key} = ?")
        params.append(value)
    return (" WHERE " + " AND ".join(clauses) if clauses else ""), params


def _rollup_where(filters: dict[str, Any]) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    for key in ("package_key", "experiment_id", "strategy_line", "parameter_set_id", "dimension", "key"):
        value = filters.get(key)
        if value in (None, "", "all"):
            continue
        clauses.append(f"{key} = ?")
        params.append(value)
    return (" WHERE " + " AND ".join(clauses) if clauses else ""), params


def _select_top_parameter_sets(
    conn: sqlite3.Connection,
    *,
    experiment_id: str,
    strategy_line: str | None = None,
    top_n: int = 1,
) -> list[str]:
    clauses = ["experiment_id = ?"]
    params: list[Any] = [experiment_id]
    if strategy_line and strategy_line != "all":
        clauses.append("strategy_line = ?")
        params.append(strategy_line)
    rows = conn.execute(
        f"""
        SELECT parameter_set_id, metrics_json
        FROM p21_v2_30d_metrics
        WHERE {' AND '.join(clauses)}
        """,
        params,
    ).fetchall()
    scored: list[tuple[float, float, int, str]] = []
    for row in rows:
        metrics = _loads(row["metrics_json"], {})
        pf = _num(metrics.get("profit_factor"), 0.0)
        expectancy = _num(metrics.get("expectancy_R"), 0.0)
        trades = int(_num(metrics.get("trade_count"), 0))
        scored.append((pf, expectancy, trades, str(row["parameter_set_id"])))
    scored.sort(key=lambda item: (-item[0], -item[1], -item[2], item[3]))
    return [item[3] for item in scored[: max(1, top_n)]]


def _safe_price(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _exit_price_from_payload(row: dict[str, Any]) -> float | None:
    payload = _loads(row.get("fill_result_json"), {})
    for key in ("exit_price", "close_price", "matched_exit_price", "price"):
        value = _safe_price(payload.get(key))
        if value is not None:
            return value
    net_r = _safe_price(row.get("net_R"))
    entry = _safe_price(row.get("entry_price"))
    stop = _safe_price(row.get("stop_loss"))
    side = str(row.get("side") or "").upper()
    if net_r is None or entry is None or stop is None:
        return None
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    return round(entry + risk * net_r, 12) if side == "LONG" else round(entry - risk * net_r, 12)


def _excursions_from_1m(conn: sqlite3.Connection, row: dict[str, Any]) -> tuple[float | None, float | None, str]:
    entry = _safe_price(row.get("entry_price"))
    stop = _safe_price(row.get("stop_loss"))
    entry_ms = row.get("entry_time_ms")
    exit_ms = row.get("exit_time_ms")
    if entry is None or stop is None or not entry_ms or not exit_ms:
        return None, None, "missing_time_or_risk"
    risk = abs(entry - stop)
    if risk <= 0:
        return None, None, "invalid_initial_risk"
    candles = conn.execute(
        """
        SELECT high, low
        FROM p21_klines_1m
        WHERE symbol = ? AND open_time_ms >= ? AND open_time_ms <= ?
        ORDER BY open_time_ms
        """,
        (row["symbol"], int(entry_ms), int(exit_ms)),
    ).fetchall()
    if not candles:
        return None, None, "missing_1m_replay"
    highs = [_num(candle["high"]) for candle in candles]
    lows = [_num(candle["low"]) for candle in candles]
    side = str(row.get("side") or "").upper()
    if side == "SHORT":
        mfe = max(entry - low for low in lows)
        mae = max(high - entry for high in highs)
    else:
        mfe = max(high - entry for high in highs)
        mae = max(entry - low for low in lows)
    return round(max(0.0, mfe) / risk, 8), round(max(0.0, mae) / risk, 8), "replayed_1m"


def _root_cause(row: dict[str, Any]) -> tuple[str, float]:
    net_r = _safe_price(row.get("net_R"))
    mfe = _safe_price(row.get("MFE_R"))
    mae = _safe_price(row.get("MAE_R"))
    planned_rr = _safe_price(row.get("planned_RR"))
    exit_reason = str(row.get("exit_reason") or "").upper()
    payload = row.get("source_payload") if isinstance(row.get("source_payload"), dict) else {}
    features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    reasons = {str(item) for item in (payload.get("reasons") or [])}
    side = str(row.get("side") or "").upper()
    pct_1m = _safe_price(features.get("pct_1m_bps")) or 0.0
    pct_3m = _safe_price(features.get("pct_3m_bps")) or 0.0
    range_pos = _safe_price(features.get("range_pos_30m"))
    volume_z = _safe_price(features.get("volume_z"))
    direction_state = str(features.get("strategy6_direction_state") or "")
    entry_state = str(features.get("strategy6_entry_quality_state") or "")
    wait_state = str(features.get("strategy6_wait_state") or "")
    adaptive_tier = str(features.get("strategy6_adaptive_exit_tier") or "")
    adverse_1m = (side == "LONG" and pct_1m < -8.0) or (side == "SHORT" and pct_1m > 8.0)
    adverse_3m = (side == "LONG" and pct_3m < -18.0) or (side == "SHORT" and pct_3m > 18.0)
    range_extreme = range_pos is not None and ((side == "LONG" and range_pos >= 0.88) or (side == "SHORT" and range_pos <= 0.12))
    low_volume_follow = volume_z is not None and volume_z < 0.35
    if mfe is None or mae is None:
        return "needs_replay", 0.0
    if net_r is not None and net_r > 0:
        return "profitable_trade", 0.8
    if any("cost_dominates" in reason or "reward_too_close_to_spread" in reason or "tp_after_cost_too_small" in reason for reason in reasons):
        return "spread_or_reward_bad", 0.78
    if any("wait_rebound" in reason or "wait_pullback" in reason for reason in reasons) and mae >= 0.7 and (net_r is None or net_r < 0):
        return "wait_rebound_failed", 0.72
    if direction_state in {"uncertain_direction", "denied_direction"} and (net_r is None or net_r < 0):
        return "direction_wrong", 0.8
    if adverse_1m and adverse_3m and mae >= 0.7:
        return "direction_wrong", 0.84
    if mfe < 0.3 and mae >= 0.8:
        return "direction_wrong", 0.82
    if range_extreme and mfe < 0.7 and mae >= 0.55:
        return "range_extreme", 0.7
    if adverse_1m and mfe < 0.5:
        return "late_chase", 0.68
    if mfe < 0.3:
        return "signal_no_edge", 0.75
    if low_volume_follow and mfe < 0.6 and mae >= 0.5:
        return "weak_followthrough", 0.62
    if entry_state in {"entry_price_chased", "entry_price_weak"} and mae >= 0.6:
        return "entered_too_early", 0.72
    if wait_state not in {"", "NONE", "READY"} and mae >= 0.6 and mfe < 0.8:
        return "wait_rebound_failed", 0.66
    if mae > 0.6 and mfe > 0.8:
        return "entered_too_early", 0.72
    if mfe > 0.8 and (net_r is None or net_r <= 0):
        return "exit_too_late", 0.76
    if exit_reason == "SL" and mfe > 1.0:
        return "stop_too_tight", 0.72
    if planned_rr is not None and planned_rr >= 1.2 and 0.3 <= mfe <= 0.7:
        return "tp_too_far", 0.7
    if adaptive_tier == "low_quality" and net_r is not None and net_r < 0:
        return "weak_followthrough", 0.58
    if net_r is not None and net_r < 0:
        return "loss_unclassified", 0.4
    return "unknown", 0.2


def _entry_label(root: str) -> str:
    return {
        "direction_wrong": "entry_direction_wrong",
        "signal_no_edge": "entry_signal_no_edge",
        "entered_too_early": "entry_too_early",
        "entered_too_late": "entry_too_late",
        "late_chase": "entry_chase_tail",
        "range_extreme": "entry_range_extreme",
        "weak_followthrough": "entry_weak_followthrough",
        "fake_breakout": "entry_fake_breakout",
        "spread_or_reward_bad": "entry_cost_reward_bad",
        "wait_rebound_failed": "entry_wait_failed",
        "profitable_trade": "entry_supported",
        "needs_replay": "entry_replay_missing",
    }.get(root, "entry_quality_observed")


def _stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    stats = _performance_stats(rows)
    gross_profit = _num(stats.get("gross_profit_R"), 0.0)
    gross_loss = _num(stats.get("gross_loss_R"), 0.0)
    stats["profit_factor"] = round(gross_profit / gross_loss, 8) if gross_loss else (None if gross_profit == 0 else 999.0)
    stats["total_R"] = round(
        sum(_num(row.get("net_R"), 0.0) for row in rows if row.get("net_R") is not None),
        8,
    )
    mfe_values = [_num(row.get("MFE_R")) for row in rows if row.get("MFE_R") is not None]
    mae_values = [_num(row.get("MAE_R")) for row in rows if row.get("MAE_R") is not None]
    stats["avg_MFE_R"] = round(sum(mfe_values) / len(mfe_values), 8) if mfe_values else 0.0
    stats["avg_MAE_R"] = round(sum(mae_values) / len(mae_values), 8) if mae_values else 0.0
    return stats


def _sample_from_order(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    order = dict(row)
    experiment_id = str(order["experiment_id"])
    strategy_line = str(order["strategy_line"])
    parameter_set_id = str(order["parameter_set_id"])
    entry_ms = int(order["entry_time_ms"]) if order.get("entry_time_ms") else None
    exit_ms = int(order["exit_time_ms"]) if order.get("exit_time_ms") else None
    holding_minutes = round((exit_ms - entry_ms) / 60000, 8) if entry_ms and exit_ms else None
    mfe_r, mae_r, replay_status = _excursions_from_1m(conn, order)
    sample: dict[str, Any] = {
        "diagnostic_id": _stable_id("btq", [experiment_id, parameter_set_id, order["order_id"]]),
        "source": BACKTEST_TQ_SOURCE,
        "package_key": package_key(experiment_id, strategy_line, parameter_set_id),
        "experiment_id": experiment_id,
        "parameter_set_id": parameter_set_id,
        "strategy_line": strategy_line,
        "order_id": order["order_id"],
        "trade_id": order["order_id"],
        "symbol": order["symbol"],
        "side": str(order["side"] or "").upper(),
        "entry_time": _iso_from_ms(entry_ms) if entry_ms else None,
        "exit_time": _iso_from_ms(exit_ms) if exit_ms else None,
        "entry_time_ms": entry_ms,
        "exit_time_ms": exit_ms,
        "entry_price": order.get("entry_price"),
        "exit_price": _exit_price_from_payload(order),
        "planned_SL": order.get("stop_loss"),
        "planned_TP": order.get("take_profit"),
        "planned_RR": order.get("planned_rr"),
        "holding_minutes": holding_minutes,
        "gross_pnl": None,
        "fee": None,
        "net_pnl": None,
        "net_R": order.get("net_R"),
        "MFE_R": mfe_r,
        "MAE_R": mae_r,
        "replay_status": replay_status,
        "exit_reason": order.get("exit_reason"),
        "entry_context_v3_label": "backtest_context_pending",
        "evidence": {
            "source": BACKTEST_TQ_SOURCE,
            "lineage_mode": order.get("lineage_mode"),
            "source_contract_version": order.get("source_contract_version"),
            "replay_status": replay_status,
            "has_trade_plan_payload": bool(order.get("trade_plan_payload_json")),
            "has_fill_result_payload": bool(order.get("fill_result_json")),
        },
        "source_payload": {
            "score": order.get("score"),
            "reasons": _loads(order.get("reasons_json"), []),
            "features": _loads(order.get("features_json"), {}),
            "config_patch": _loads(order.get("config_patch_json"), {}),
            "fast_exit_policy": _loads(order.get("fast_exit_policy_json"), {}),
            "trade_plan_payload": _loads(order.get("trade_plan_payload_json"), {}),
            "fill_result": _loads(order.get("fill_result_json"), {}),
        },
    }
    root, confidence = _root_cause(sample)
    sample["root_cause"] = root
    sample["root_cause_confidence"] = confidence
    sample["entry_quality_label"] = _entry_label(root)
    return sample


def _insert_samples(conn: sqlite3.Connection, samples: list[dict[str, Any]]) -> None:
    now = _now()
    rows = []
    for sample in samples:
        rows.append(
            (
                sample["diagnostic_id"],
                sample["source"],
                sample["package_key"],
                sample["experiment_id"],
                sample["parameter_set_id"],
                sample["strategy_line"],
                sample["order_id"],
                sample["trade_id"],
                sample["symbol"],
                sample["side"],
                sample.get("entry_time"),
                sample.get("exit_time"),
                sample.get("entry_time_ms"),
                sample.get("exit_time_ms"),
                sample.get("entry_price"),
                sample.get("exit_price"),
                sample.get("planned_SL"),
                sample.get("planned_TP"),
                sample.get("planned_RR"),
                sample.get("holding_minutes"),
                sample.get("gross_pnl"),
                sample.get("fee"),
                sample.get("net_pnl"),
                sample.get("net_R"),
                sample.get("MFE_R"),
                sample.get("MAE_R"),
                sample.get("replay_status"),
                sample.get("root_cause"),
                sample.get("root_cause_confidence"),
                sample.get("entry_quality_label"),
                sample.get("entry_context_v3_label"),
                sample.get("exit_reason"),
                _json(sample.get("evidence") or {}),
                _json(sample.get("source_payload") or {}),
                BACKTEST_TQ_SCHEMA_VERSION,
                now,
                now,
            )
        )
    conn.executemany(
        """
        INSERT INTO backtest_trade_quality_samples(
          diagnostic_id, source, package_key, experiment_id, parameter_set_id, strategy_line,
          order_id, trade_id, symbol, side, entry_time, exit_time, entry_time_ms, exit_time_ms,
          entry_price, exit_price, planned_SL, planned_TP, planned_RR, holding_minutes,
          gross_pnl, fee, net_pnl, net_R, MFE_R, MAE_R, replay_status, root_cause,
          root_cause_confidence, entry_quality_label, entry_context_v3_label, exit_reason,
          evidence_json, source_payload_json, schema_version, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(experiment_id, parameter_set_id, order_id) DO UPDATE SET
          package_key = excluded.package_key,
          entry_time = excluded.entry_time,
          exit_time = excluded.exit_time,
          exit_price = excluded.exit_price,
          holding_minutes = excluded.holding_minutes,
          net_R = excluded.net_R,
          MFE_R = excluded.MFE_R,
          MAE_R = excluded.MAE_R,
          replay_status = excluded.replay_status,
          root_cause = excluded.root_cause,
          root_cause_confidence = excluded.root_cause_confidence,
          entry_quality_label = excluded.entry_quality_label,
          entry_context_v3_label = excluded.entry_context_v3_label,
          exit_reason = excluded.exit_reason,
          evidence_json = excluded.evidence_json,
          source_payload_json = excluded.source_payload_json,
          schema_version = excluded.schema_version,
          updated_at = excluded.updated_at
        """,
        rows,
    )
    for sample in samples:
        upsert_backtest_tq_sample_native(conn, sample)


def _sample_rows(conn: sqlite3.Connection, **filters: Any) -> list[dict[str, Any]]:
    where, params = _where(filters)
    limit = int(filters.get("limit") or 5000)
    offset = int(filters.get("offset") or 0)
    rows = conn.execute(
        f"""
        SELECT *
        FROM backtest_trade_quality_samples
        {where}
        ORDER BY exit_time DESC, diagnostic_id DESC
        LIMIT ? OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()
    return [_decode_sample(dict(row)) for row in rows]


def _decode_sample(row: dict[str, Any]) -> dict[str, Any]:
    row["evidence"] = _loads(row.pop("evidence_json", None), {})
    row["source_payload"] = _loads(row.pop("source_payload_json", None), {})
    return row


def _rollup_dimensions(rows: list[dict[str, Any]]) -> list[tuple[str, str, list[dict[str, Any]]]]:
    dimensions = {
        "experiment_id": lambda row: row.get("experiment_id"),
        "strategy_line": lambda row: row.get("strategy_line"),
        "parameter_set_id": lambda row: row.get("parameter_set_id"),
        "symbol": lambda row: row.get("symbol"),
        "side": lambda row: row.get("side"),
        "exit_reason": lambda row: row.get("exit_reason"),
        "root_cause": lambda row: row.get("root_cause"),
        "entry_quality_label": lambda row: row.get("entry_quality_label"),
        "entry_context_v3_label": lambda row: row.get("entry_context_v3_label"),
        "day": lambda row: str(row.get("exit_time") or row.get("entry_time") or "")[:10] or "unknown",
    }
    out: list[tuple[str, str, list[dict[str, Any]]]] = []
    for dimension, getter in dimensions.items():
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[str(getter(row) or "unknown")].append(row)
        out.extend((dimension, key, items) for key, items in groups.items())
    return out


def rebuild_backtest_quality_rollups(
    conn: sqlite3.Connection,
    *,
    package_keys: list[str],
) -> int:
    total = 0
    now = _now()
    for pkg in package_keys:
        rows = _sample_rows(conn, package_key=pkg, limit=100000)
        conn.execute("DELETE FROM backtest_trade_quality_rollups WHERE package_key = ?", (pkg,))
        if not rows:
            continue
        sample = rows[0]
        inserts = []
        for dimension, key, items in _rollup_dimensions(rows):
            aggregate = _aggregate_row(dimension, key, items)
            metrics = {**dict(aggregate), **_stats(items)}
            evidence = metrics.pop("evidence", {})
            rollup_id = _stable_id("btqr", [pkg, dimension, key])
            inserts.append(
                (
                    rollup_id,
                    pkg,
                    sample["experiment_id"],
                    sample.get("strategy_line"),
                    sample.get("parameter_set_id"),
                    dimension,
                    key,
                    len(items),
                    _json(metrics),
                    _json(evidence),
                    BACKTEST_TQ_SCHEMA_VERSION,
                    now,
                )
            )
        conn.executemany(
            """
            INSERT OR REPLACE INTO backtest_trade_quality_rollups(
              rollup_id, package_key, experiment_id, strategy_line, parameter_set_id,
              dimension, key, sample_count, metrics_json, evidence_json, schema_version, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )
        total += len(inserts)
    return total


def materialize_payload(
    project_root: Path,
    *,
    experiment_id: str,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    top_n: int = 1,
    limit: int = 5000,
    dry_run: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_backtest_trade_quality_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        parameter_sets = [parameter_set_id] if parameter_set_id else _select_top_parameter_sets(
            conn,
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            top_n=top_n,
        )
        if not parameter_sets:
            return {
                "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
                "source": BACKTEST_TQ_SOURCE,
                "dry_run": dry_run,
                "experiment_id": experiment_id,
                "selected_parameter_sets": [],
                "selected_order_count": 0,
                "materialized_count": 0,
                "rollup_count": 0,
                "package_keys": [],
            }
        clauses = ["experiment_id = ?"]
        params: list[Any] = [experiment_id]
        if strategy_line and strategy_line != "all":
            clauses.append("strategy_line = ?")
            params.append(strategy_line)
        placeholders = ",".join("?" for _ in parameter_sets)
        clauses.append(f"parameter_set_id IN ({placeholders})")
        params.extend(parameter_sets)
        selected_count = conn.execute(
            f"SELECT COUNT(*) FROM p21_v2_shadow_orders WHERE {' AND '.join(clauses)}",
            params,
        ).fetchone()[0]
        package_keys = [
            package_key(
                experiment_id,
                (None if strategy_line == "all" else strategy_line) or (conn.execute(
                    "SELECT strategy_line FROM p21_v2_shadow_orders WHERE experiment_id = ? AND parameter_set_id = ? LIMIT 1",
                    (experiment_id, param),
                ).fetchone() or {"strategy_line": "unknown"})["strategy_line"],
                param,
            )
            for param in parameter_sets
        ]
        if dry_run:
            return {
                "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
                "source": BACKTEST_TQ_SOURCE,
                "dry_run": True,
                "experiment_id": experiment_id,
                "selected_parameter_sets": parameter_sets,
                "selected_order_count": int(selected_count),
                "materialized_count": 0,
                "rollup_count": 0,
                "package_keys": package_keys,
                "limit": limit,
            }
        if force:
            for pkg in package_keys:
                conn.execute("DELETE FROM backtest_trade_quality_samples WHERE package_key = ?", (pkg,))
        rows = conn.execute(
            f"""
            SELECT *
            FROM p21_v2_shadow_orders
            WHERE {' AND '.join(clauses)}
            ORDER BY rowid
            LIMIT ?
            """,
            [*params, int(limit)],
        ).fetchall()
        samples = [_sample_from_order(conn, row) for row in rows]
        _insert_samples(conn, samples)
        rollup_count = rebuild_backtest_quality_rollups(conn, package_keys=sorted({sample["package_key"] for sample in samples}))
        conn.commit()
    return {
        "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
        "source": BACKTEST_TQ_SOURCE,
        "dry_run": False,
        "experiment_id": experiment_id,
        "selected_parameter_sets": parameter_sets,
        "selected_order_count": int(selected_count),
        "materialized_count": len(samples),
        "rollup_count": rollup_count,
        "package_keys": sorted({sample["package_key"] for sample in samples}) or package_keys,
        "limit": limit,
    }


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "performance_stats": _stats(rows),
        "root_cause_attribution": _root_cause_attribution(rows),
        "dimension_attribution": _dimension_attribution(rows),
        "entry_quality_attribution": {
            "items": [_aggregate_row("entry_quality_label", key, items) for key, items in _group(rows, "entry_quality_label").items()]
        },
        "entry_context_v3_attribution": {
            "items": [_aggregate_row("entry_context_v3_label", key, items) for key, items in _group(rows, "entry_context_v3_label").items()]
        },
        "replay_coverage": {
            "total": len(rows),
            "replayed_1m": sum(1 for row in rows if row.get("replay_status") == "replayed_1m"),
            "ratio": round(sum(1 for row in rows if row.get("replay_status") == "replayed_1m") / len(rows), 8) if rows else 0.0,
        },
    }


def _group(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row.get(key) or "unknown")].append(row)
    return groups


def summary_payload(project_root: Path, **filters: Any) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_backtest_trade_quality_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = _sample_rows(conn, **{**filters, "limit": min(int(filters.get("limit") or 5000), 20000)})
        where, params = _rollup_where(filters)
        total = conn.execute(f"SELECT COUNT(*) FROM backtest_trade_quality_samples {where}", params).fetchone()[0]
    return {
        "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
        "source": BACKTEST_TQ_SOURCE,
        "total": int(total),
        "sampled": len(rows),
        "filters": {key: value for key, value in filters.items() if value not in (None, "", "all")},
        "summary": _summary(rows),
    }


def aggregates_payload(project_root: Path, **filters: Any) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_backtest_trade_quality_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        where, params = _where(filters)
        rows = conn.execute(
            f"""
            SELECT *
            FROM backtest_trade_quality_rollups
            {where}
            ORDER BY sample_count DESC, dimension, key
            LIMIT ?
            """,
            [*params, int(filters.get("limit") or 200)],
        ).fetchall()
        decoded = []
        for row in rows:
            item = dict(row)
            item["metrics"] = _loads(item.pop("metrics_json", None), {})
            item["evidence"] = _loads(item.pop("evidence_json", None), {})
            decoded.append(item)
    return {
        "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
        "source": BACKTEST_TQ_SOURCE,
        "count": len(decoded),
        "aggregates": decoded,
    }


def samples_payload(project_root: Path, *, limit: int = 200, offset: int = 0, **filters: Any) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_backtest_trade_quality_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = _sample_rows(conn, **{**filters, "limit": min(limit, 1000), "offset": offset})
        where, params = _where(filters)
        total = conn.execute(f"SELECT COUNT(*) FROM backtest_trade_quality_samples {where}", params).fetchone()[0]
    return {
        "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
        "source": BACKTEST_TQ_SOURCE,
        "total": int(total),
        "limit": min(limit, 1000),
        "offset": offset,
        "samples": rows,
    }


def _metrics_score(metrics: dict[str, Any]) -> tuple[float, float, int, float]:
    pf_raw = metrics.get("profit_factor")
    pf = -999.0 if pf_raw in (None, "") else _num(pf_raw, -999.0)
    expectancy = _num(metrics.get("expectancy_R"), 0.0)
    trades = int(_num(metrics.get("trade_count"), 0))
    total_r = _num(metrics.get("total_R"), 0.0)
    return pf, expectancy, trades, total_r


def _candidate_rank_limit(strategy_line: str | None, limit: int | None) -> tuple[str, int]:
    scope = "strategy_top10" if strategy_line and strategy_line != "all" else "global_top30"
    default_limit = 10 if scope == "strategy_top10" else 30
    requested = int(limit or default_limit)
    return scope, max(1, min(requested, default_limit))


def _materialized_counts(conn: sqlite3.Connection) -> dict[tuple[str, str, str], tuple[str, int, str | None]]:
    rows = conn.execute(
        """
        SELECT experiment_id, parameter_set_id, strategy_line, package_key,
               COUNT(*) AS sample_count, MAX(updated_at) AS updated_at
        FROM backtest_trade_quality_samples
        GROUP BY experiment_id, parameter_set_id, strategy_line, package_key
        """
    ).fetchall()
    out: dict[tuple[str, str, str], tuple[str, int, str | None]] = {}
    for row in rows:
        out[(str(row["experiment_id"]), str(row["parameter_set_id"]), str(row["strategy_line"]))] = (
            str(row["package_key"]),
            int(row["sample_count"] or 0),
            row["updated_at"],
        )
    return out


def _shadow_order_count(conn: sqlite3.Connection, experiment_id: str, parameter_set_id: str, strategy_line: str) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM p21_v2_shadow_orders
            WHERE experiment_id = ? AND parameter_set_id = ? AND strategy_line = ?
            """,
            (experiment_id, parameter_set_id, strategy_line),
        ).fetchone()[0]
    )


def _materialized_status(
    conn: sqlite3.Connection,
    experiment_id: str,
    parameter_set_id: str,
    strategy_line: str,
) -> tuple[str, int, str | None]:
    row = conn.execute(
        """
        SELECT package_key, COUNT(*) AS sample_count, MAX(updated_at) AS updated_at
        FROM backtest_trade_quality_samples
        WHERE experiment_id = ? AND parameter_set_id = ? AND strategy_line = ?
        GROUP BY package_key
        ORDER BY sample_count DESC
        LIMIT 1
        """,
        (experiment_id, parameter_set_id, strategy_line),
    ).fetchone()
    if not row:
        return package_key(experiment_id, strategy_line, parameter_set_id), 0, None
    return str(row["package_key"]), int(row["sample_count"] or 0), row["updated_at"]


def _leaderboard_candidate_packages(
    conn: sqlite3.Connection,
    *,
    experiment_id: str | None,
    strategy_line: str | None,
    limit: int | None,
) -> dict[str, Any]:
    rank_scope, resolved_limit = _candidate_rank_limit(strategy_line, limit)
    candidate_items: list[dict[str, Any]] = []
    try:
        rec_clauses: list[str] = []
        rec_params: list[Any] = []
        if experiment_id:
            rec_clauses.append("experiment_id = ?")
            rec_params.append(experiment_id)
        rec_where = " WHERE " + " AND ".join(rec_clauses) if rec_clauses else ""
        rec_rows = conn.execute(
            f"""
            SELECT *
            FROM p21_v2_recommendations
            {rec_where}
            ORDER BY priority ASC, generated_at DESC
            LIMIT ?
            """,
            [*rec_params, max(200, resolved_limit * 20)],
        ).fetchall()
    except sqlite3.OperationalError as exc:
        rec_rows = []
        rec_error = str(exc)
    else:
        rec_error = ""
    for row in rec_rows:
        metrics = _loads(row["metrics_json"], {})
        parameters = _loads(row["parameters_json"], {})
        line = str(parameters.get("strategy_line") or metrics.get("strategy_line") or "")
        if strategy_line and strategy_line != "all" and line != strategy_line:
            continue
        if not line:
            continue
        candidate_items.append(
            {
                "experiment_id": row["experiment_id"],
                "parameter_set_id": row["parameter_set_id"],
                "strategy_line": line,
                "metrics": metrics,
                "parameters": parameters,
                "generated_at": row["generated_at"],
                "candidate_source": "recommendation",
            }
        )
    if not candidate_items:
        clauses: list[str] = []
        params: list[Any] = []
        if experiment_id:
            clauses.append("experiment_id = ?")
            params.append(experiment_id)
        if strategy_line and strategy_line != "all":
            clauses.append("strategy_line = ?")
            params.append(strategy_line)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        try:
            rows = conn.execute(
                f"""
                SELECT *
                FROM p21_v2_30d_metrics
                {where}
                ORDER BY CAST(json_extract(metrics_json, '$.profit_factor') AS REAL) DESC,
                         CAST(json_extract(metrics_json, '$.expectancy_R') AS REAL) DESC,
                         CAST(json_extract(metrics_json, '$.trade_count') AS INTEGER) DESC,
                         CAST(json_extract(metrics_json, '$.total_R') AS REAL) DESC,
                         parameter_set_id ASC
                LIMIT ?
                """,
                [*params, resolved_limit],
            ).fetchall()
        except sqlite3.OperationalError as exc:
            return {
                "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
                "source": BACKTEST_TQ_SOURCE,
                "mode": "leaderboard_candidates",
                "rank_scope": rank_scope,
                "limit": resolved_limit,
                "count": 0,
                "packages": [],
                "status": "sqlite_busy_or_missing",
                "error": rec_error or str(exc),
            }
        for row in rows:
            item = dict(row)
            candidate_items.append(
                {
                    "experiment_id": item["experiment_id"],
                    "parameter_set_id": item["parameter_set_id"],
                    "strategy_line": item["strategy_line"],
                    "metrics": _loads(item["metrics_json"], {}),
                    "parameters": _loads(item["parameters_json"], {}),
                    "generated_at": item["generated_at"],
                    "candidate_source": "metrics",
                }
            )
    raw_packages: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in candidate_items:
        exp = str(item["experiment_id"])
        param = str(item["parameter_set_id"])
        line = str(item["strategy_line"])
        identity = (exp, param, line)
        if identity in seen:
            continue
        seen.add(identity)
        pkg_key = package_key(exp, line, param)
        try:
            existing_pkg_key, sample_count, updated_at = _materialized_status(conn, exp, param, line)
        except sqlite3.OperationalError:
            existing_pkg_key, sample_count, updated_at = pkg_key, 0, None
        try:
            shadow_count = _shadow_order_count(conn, exp, param, line)
        except sqlite3.OperationalError:
            shadow_count = 0
        has_shadow_orders = shadow_count > 0
        if sample_count > 0:
            status = "materialized"
        elif has_shadow_orders:
            status = "ready_to_materialize"
        else:
            status = "metrics_only_no_trade_samples"
        raw_packages.append(
            {
                "source": BACKTEST_TQ_SOURCE,
                "package_key": existing_pkg_key or pkg_key,
                "experiment_id": exp,
                "experiment_id_short": exp[:12],
                "strategy_line": line,
                "parameter_set_id": param,
                "rank_scope": rank_scope,
                "metrics": item["metrics"],
                "parameters": item["parameters"],
                "candidate_source": item.get("candidate_source") or "metrics",
                "materialized": sample_count > 0,
                "materialized_sample_count": sample_count,
                "sample_count": sample_count,
                "diagnostic_sample_count": sample_count,
                "has_shadow_orders": has_shadow_orders,
                "shadow_order_count": shadow_count,
                "sample_status": status,
                "updated_at": updated_at or item["generated_at"],
                "generated_at": item["generated_at"],
            }
        )
    status_rank = {
        "materialized": 0,
        "ready_to_materialize": 1,
        "metrics_only_no_trade_samples": 2,
    }
    raw_packages.sort(
        key=lambda item: (
            status_rank.get(str(item.get("sample_status") or ""), 9),
            -_metrics_score(item.get("metrics") or {})[0],
            -_metrics_score(item.get("metrics") or {})[1],
            -_metrics_score(item.get("metrics") or {})[2],
            -float((item.get("metrics") or {}).get("total_R") or 0.0),
            str(item.get("parameter_set_id") or ""),
            str(item.get("experiment_id") or ""),
        )
    )
    packages = []
    for rank, item in enumerate(raw_packages[:resolved_limit], start=1):
        packages.append({**item, "rank": rank})
    return {
        "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
        "source": BACKTEST_TQ_SOURCE,
        "mode": "leaderboard_candidates",
        "rank_scope": rank_scope,
        "limit": resolved_limit,
        "count": len(packages),
        "packages": packages,
    }


def packages_payload(
    project_root: Path,
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    mode: str = "materialized",
    limit: int = 50,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    if str(mode or "materialized") == "leaderboard_candidates":
        if not db_path.exists():
            rank_scope, resolved_limit = _candidate_rank_limit(strategy_line, limit)
            return {
                "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
                "source": BACKTEST_TQ_SOURCE,
                "mode": "leaderboard_candidates",
                "rank_scope": rank_scope,
                "limit": resolved_limit,
                "count": 0,
                "packages": [],
                "status": "db_missing",
            }
        try:
            with _read_connect(db_path) as conn:
                return _leaderboard_candidate_packages(
                    conn,
                    experiment_id=experiment_id,
                    strategy_line=strategy_line,
                    limit=limit,
                )
        except sqlite3.OperationalError as exc:
            rank_scope, resolved_limit = _candidate_rank_limit(strategy_line, limit)
            return {
                "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
                "source": BACKTEST_TQ_SOURCE,
                "mode": "leaderboard_candidates",
                "rank_scope": rank_scope,
                "limit": resolved_limit,
                "count": 0,
                "packages": [],
                "status": "sqlite_busy_or_missing",
                "error": str(exc),
            }
    ensure_backtest_trade_quality_tables(db_path)
    clauses: list[str] = []
    params: list[Any] = []
    if experiment_id:
        clauses.append("experiment_id = ?")
        params.append(experiment_id)
    if strategy_line and strategy_line != "all":
        clauses.append("strategy_line = ?")
        params.append(strategy_line)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT package_key, experiment_id, strategy_line, parameter_set_id,
                   COUNT(*) AS sample_count, MAX(updated_at) AS updated_at
            FROM backtest_trade_quality_samples
            {where}
            GROUP BY package_key, experiment_id, strategy_line, parameter_set_id
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()
        metrics_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
        for metric_row in conn.execute("SELECT * FROM p21_v2_30d_metrics").fetchall():
            metrics_by_key[
                (
                    str(metric_row["experiment_id"]),
                    str(metric_row["parameter_set_id"]),
                    str(metric_row["strategy_line"]),
                )
            ] = {
                "metrics": _loads(metric_row["metrics_json"], {}),
                "parameters": _loads(metric_row["parameters_json"], {}),
                "generated_at": metric_row["generated_at"],
            }
        packages = []
        for row in rows:
            item = dict(row)
            meta = metrics_by_key.get((str(item["experiment_id"]), str(item["parameter_set_id"]), str(item["strategy_line"])), {})
            sample_count = int(item.get("sample_count") or 0)
            item.update(
                {
                    "source": BACKTEST_TQ_SOURCE,
                    "rank_scope": "materialized",
                    "rank": None,
                    "metrics": meta.get("metrics", {}),
                    "parameters": meta.get("parameters", {}),
                    "materialized": sample_count > 0,
                    "materialized_sample_count": sample_count,
                    "diagnostic_sample_count": sample_count,
                    "sample_status": "materialized" if sample_count > 0 else "empty",
                    "has_shadow_orders": True,
                    "shadow_order_count": None,
                }
            )
            packages.append(item)
    return {
        "schema_version": BACKTEST_TQ_SCHEMA_VERSION,
        "source": BACKTEST_TQ_SOURCE,
        "mode": "materialized",
        "count": len(rows),
        "packages": packages,
    }
