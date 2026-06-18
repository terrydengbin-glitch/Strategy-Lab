from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.research_db import upsert_backtest_order_native
from laoma_signal_engine.backtest.p21_real_evaluator import ENGINE_MODE, SOURCE_CONTRACT_VERSION, evaluate_signal_offline
from laoma_signal_engine.backtest.p21_v2 import (
    DEFAULT_WINDOW_DAYS,
    HistoricalSignal,
    _avg,
    _connect,
    _group_by_day,
    _group_by_symbol,
    _iso_from_ms,
    _json,
    _loads,
    _metrics,
    _ms_from_dt,
    _num,
    _pct,
    _rows_for_symbol,
    _stable_id,
    default_parameter_sets,
    ensure_p21_v2_tables,
    load_runtime_line_config,
    simulate_1m_fill,
    universe_symbols,
)
from laoma_signal_engine.strategy4.observe import HARD_DENY_REASON_CODES, RETRYABLE_REASON_CODES, classify_strategy1_plan


SCHEMA_VERSION = "21.36-strategy4-persistent-observe-replay"
REPLAY_MODE = "strategy4_persistent_observe_replay"
LEGACY_MODE = "strategy4_simplified_wait_band"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_tables(db_path: Path) -> None:
    ensure_p21_v2_tables(db_path)
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS p21_strategy4_replay_pool(
              pool_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              source_parameter_set_id TEXT NOT NULL,
              symbol TEXT NOT NULL,
              status TEXT NOT NULL,
              first_seen_time_ms INTEGER NOT NULL,
              last_checked_time_ms INTEGER,
              next_check_time_ms INTEGER,
              attempt_count INTEGER NOT NULL DEFAULT 0,
              source_signal_id TEXT,
              source_plan_hash TEXT,
              source_reason_codes_json TEXT NOT NULL,
              original_side TEXT,
              current_side TEXT,
              side_changed INTEGER NOT NULL DEFAULT 0,
              latest_action TEXT,
              latest_entry_mode TEXT,
              latest_executable INTEGER NOT NULL DEFAULT 0,
              latest_reason_codes_json TEXT NOT NULL,
              ttl_age_sec INTEGER,
              evict_reason TEXT,
              raw_pool_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_p21_s4_pool_exp_param
              ON p21_strategy4_replay_pool(experiment_id, parameter_set_id, status);
            CREATE TABLE IF NOT EXISTS p21_strategy4_replay_attempts(
              attempt_id TEXT PRIMARY KEY,
              pool_id TEXT NOT NULL,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              symbol TEXT NOT NULL,
              attempt_index INTEGER NOT NULL,
              recheck_time_ms INTEGER NOT NULL,
              decision TEXT,
              action TEXT,
              entry_mode TEXT,
              executable INTEGER NOT NULL DEFAULT 0,
              reason_codes_json TEXT NOT NULL,
              original_side TEXT,
              current_side TEXT,
              side_changed INTEGER NOT NULL DEFAULT 0,
              entry_price REAL,
              stop_loss REAL,
              take_profit REAL,
              planned_rr REAL,
              raw_plan_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_p21_s4_attempts_exp_param
              ON p21_strategy4_replay_attempts(experiment_id, parameter_set_id, symbol, recheck_time_ms);
            CREATE TABLE IF NOT EXISTS p21_strategy4_replay_events(
              event_id TEXT PRIMARY KEY,
              pool_id TEXT,
              attempt_id TEXT,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              symbol TEXT,
              event_type TEXT NOT NULL,
              event_time_ms INTEGER,
              evidence_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            """
        )


def _rolling_avg(values: list[float], end: int, length: int) -> float:
    start = max(0, end - length)
    window = values[start:end]
    return sum(window) / len(window) if window else 0.0


def _signal_at_index(rows: list[dict[str, Any]], *, symbol: str, strategy_line: str, idx: int, signal_id_prefix: str) -> HistoricalSignal | None:
    if idx < 30 or idx >= len(rows) - 1:
        return None
    closes = [_num(row["close"]) for row in rows]
    highs = [_num(row["high"]) for row in rows]
    lows = [_num(row["low"]) for row in rows]
    volumes = [_num(row["volume"]) for row in rows]
    close = closes[idx]
    pct_1m = _pct(close, closes[idx - 1])
    pct_3m = _pct(close, closes[idx - 3])
    pct_5m = _pct(close, closes[idx - 5])
    pct_15m = _pct(close, closes[idx - 15])
    side = "LONG" if pct_3m > 0 else "SHORT" if pct_3m < 0 else None
    if side is None:
        return None
    avg_vol = _rolling_avg(volumes, idx, 30)
    volume_z = volumes[idx] / avg_vol if avg_vol else 0.0
    high_30 = max(highs[max(0, idx - 30) : idx + 1])
    low_30 = min(lows[max(0, idx - 30) : idx + 1])
    range_pos = (close - low_30) / (high_30 - low_30) if high_30 > low_30 else 0.5
    atr_bps = _avg([abs(highs[j] - lows[j]) / closes[j] * 10000 for j in range(max(1, idx - 14), idx + 1) if closes[j]])
    score = min(100.0, abs(pct_3m) * 1.6 + max(0.0, volume_z - 1.0) * 16 + abs(pct_15m) * 0.25)
    payload = {"prefix": signal_id_prefix, "line": strategy_line, "symbol": symbol, "time": rows[idx]["open_time_ms"], "score": score, "side": side}
    return HistoricalSignal(
        signal_id=_stable_id("s4sig", payload, 24),
        strategy_line=strategy_line,
        symbol=symbol.upper(),
        side=side,
        index=idx,
        signal_time_ms=int(rows[idx]["open_time_ms"]),
        score=round(score, 6),
        features={
            "pct_1m_bps": round(pct_1m, 6),
            "pct_3m_bps": round(pct_3m, 6),
            "pct_5m_bps": round(pct_5m, 6),
            "pct_15m_bps": round(pct_15m, 6),
            "volume_z": round(volume_z, 6),
            "range_pos_30m": round(range_pos, 6),
            "atr_1m_bps": round(atr_bps, 6),
            "close": close,
        },
    )


def _candidate_indices(rows: list[dict[str, Any]], *, min_gap: int = 5) -> list[int]:
    out: list[int] = []
    last = -999
    for idx in range(30, len(rows) - 6):
        if idx - last < min_gap:
            continue
        close = _num(rows[idx]["close"])
        pct_3m = _pct(close, _num(rows[idx - 3]["close"]))
        pct_15m = _pct(close, _num(rows[idx - 15]["close"]))
        volumes = [_num(row["volume"]) for row in rows]
        avg_vol = _rolling_avg(volumes, idx, 30)
        volume_z = volumes[idx] / avg_vol if avg_vol else 0.0
        score = min(100.0, abs(pct_3m) * 1.6 + max(0.0, volume_z - 1.0) * 16 + abs(pct_15m) * 0.25)
        if score >= 20.0 and pct_3m != 0:
            out.append(idx)
            last = idx
    return out


def _first_plan(evaluated: dict[str, Any]) -> dict[str, Any]:
    payload = evaluated.get("trade_plan_payload") if isinstance(evaluated.get("trade_plan_payload"), dict) else {}
    plans = payload.get("plans") if isinstance(payload.get("plans"), list) else []
    return dict(plans[0]) if plans and isinstance(plans[0], dict) else {}


def _pool_row(
    *,
    experiment_id: str,
    parameter_set_id: str,
    source_parameter_set_id: str,
    signal: HistoricalSignal,
    plan: dict[str, Any],
    classified: dict[str, Any],
    observe_interval_min: int,
) -> dict[str, Any]:
    plan_hash = _stable_id("plan", plan, 20)
    pool_id = _stable_id("s4pool", {"e": experiment_id, "p": parameter_set_id, "sig": signal.signal_id, "plan": plan_hash}, 24)
    reasons = list(classified.get("reason_codes") or plan.get("reason_codes") or [])
    return {
        "pool_id": pool_id,
        "experiment_id": experiment_id,
        "parameter_set_id": parameter_set_id,
        "source_parameter_set_id": source_parameter_set_id,
        "symbol": signal.symbol,
        "status": "observing",
        "first_seen_time_ms": signal.signal_time_ms,
        "last_checked_time_ms": None,
        "next_check_time_ms": signal.signal_time_ms + observe_interval_min * 60_000,
        "attempt_count": 0,
        "source_signal_id": signal.signal_id,
        "source_plan_hash": plan_hash,
        "source_reason_codes": reasons,
        "original_side": str(plan.get("decision") or signal.side),
        "current_side": str(plan.get("decision") or signal.side),
        "side_changed": False,
        "latest_action": plan.get("action"),
        "latest_entry_mode": plan.get("entry_mode"),
        "latest_executable": False,
        "latest_reason_codes": reasons,
        "ttl_age_sec": 0,
        "evict_reason": "",
        "lineage": {
            "strategy4_replay_mode": REPLAY_MODE,
            "admission_source": "offline_without_micro_materialized_plan",
            "source_signal_id": signal.signal_id,
            "source_plan_hash": plan_hash,
            "inherit_side": False,
            "rejudge_direction_each_attempt": True,
        },
    }


def _write_pool(conn: Any, row: dict[str, Any], generated_at: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO p21_strategy4_replay_pool(
          pool_id, experiment_id, parameter_set_id, source_parameter_set_id, symbol, status,
          first_seen_time_ms, last_checked_time_ms, next_check_time_ms, attempt_count,
          source_signal_id, source_plan_hash, source_reason_codes_json, original_side,
          current_side, side_changed, latest_action, latest_entry_mode, latest_executable,
          latest_reason_codes_json, ttl_age_sec, evict_reason, raw_pool_json, generated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["pool_id"],
            row["experiment_id"],
            row["parameter_set_id"],
            row["source_parameter_set_id"],
            row["symbol"],
            row["status"],
            row["first_seen_time_ms"],
            row.get("last_checked_time_ms"),
            row.get("next_check_time_ms"),
            row["attempt_count"],
            row.get("source_signal_id"),
            row.get("source_plan_hash"),
            _json(row.get("source_reason_codes") or []),
            row.get("original_side"),
            row.get("current_side"),
            1 if row.get("side_changed") else 0,
            row.get("latest_action"),
            row.get("latest_entry_mode"),
            1 if row.get("latest_executable") else 0,
            _json(row.get("latest_reason_codes") or []),
            row.get("ttl_age_sec"),
            row.get("evict_reason"),
            _json(row),
            generated_at,
        ),
    )
    upsert_backtest_order_native(
        conn,
        experiment_id=experiment_id,
        parameter_set_id=parameter_set_id,
        strategy_line="strategy4",
        parameters=params,
        order={**filled, "strategy_line": "strategy4", "source_contract_version": SOURCE_CONTRACT_VERSION},
        generated_at=generated_at,
    )


def _write_attempt(conn: Any, row: dict[str, Any], generated_at: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO p21_strategy4_replay_attempts(
          attempt_id, pool_id, experiment_id, parameter_set_id, symbol, attempt_index,
          recheck_time_ms, decision, action, entry_mode, executable, reason_codes_json,
          original_side, current_side, side_changed, entry_price, stop_loss, take_profit,
          planned_rr, raw_plan_json, generated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["attempt_id"],
            row["pool_id"],
            row["experiment_id"],
            row["parameter_set_id"],
            row["symbol"],
            row["attempt_index"],
            row["recheck_time_ms"],
            row.get("decision"),
            row.get("action"),
            row.get("entry_mode"),
            1 if row.get("executable") else 0,
            _json(row.get("reason_codes") or []),
            row.get("original_side"),
            row.get("current_side"),
            1 if row.get("side_changed") else 0,
            row.get("entry_price"),
            row.get("stop_loss"),
            row.get("take_profit"),
            row.get("planned_rr"),
            _json(row.get("raw_plan") or {}),
            generated_at,
        ),
    )


def _write_event(conn: Any, *, experiment_id: str, parameter_set_id: str, event_type: str, symbol: str | None = None, pool_id: str | None = None, attempt_id: str | None = None, event_time_ms: int | None = None, evidence: dict[str, Any] | None = None, generated_at: str) -> None:
    event_id = _stable_id("s4evt", {"e": experiment_id, "p": parameter_set_id, "t": event_type, "s": symbol, "pool": pool_id, "a": attempt_id, "time": event_time_ms, "ev": evidence}, 24)
    conn.execute(
        "INSERT OR REPLACE INTO p21_strategy4_replay_events(event_id, pool_id, attempt_id, experiment_id, parameter_set_id, symbol, event_type, event_time_ms, evidence_json, generated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (event_id, pool_id, attempt_id, experiment_id, parameter_set_id, symbol, event_type, event_time_ms, _json(evidence or {}), generated_at),
    )


def _insert_order(conn: Any, *, experiment_id: str, parameter_set_id: str, params: dict[str, Any], filled: dict[str, Any], generated_at: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO p21_v2_shadow_orders(
          order_id, experiment_id, parameter_set_id, strategy_line, symbol, side,
          signal_time_ms, entry_time_ms, exit_time_ms, entry_price, stop_loss,
          take_profit, planned_rr, net_R, exit_reason, score, reasons_json,
          features_json, lineage_mode, source_contract_version, config_patch_json,
          trade_plan_payload_json, fill_result_json, entry_mode, effective_rr,
          fast_exit_policy_json, generated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            filled["order_id"],
            experiment_id,
            parameter_set_id,
            "strategy4",
            filled["symbol"],
            filled["side"],
            filled["signal_time_ms"],
            filled["entry_time_ms"],
            filled.get("exit_time_ms"),
            filled["entry_price"],
            filled["stop_loss"],
            filled["take_profit"],
            filled["planned_rr"],
            filled.get("net_R"),
            filled.get("exit_reason"),
            filled.get("score", 0.0),
            _json(filled.get("reasons") or []),
            _json(filled.get("features") or {}),
            REPLAY_MODE,
            SOURCE_CONTRACT_VERSION,
            _json(filled.get("config_patch") or params),
            _json(filled.get("trade_plan_payload") or {}),
            _json(filled.get("fill_result") or {}),
            filled.get("entry_mode"),
            filled.get("effective_rr"),
            _json(filled.get("fast_exit_policy") or {}),
            generated_at,
        ),
    )


def _persist_metrics(conn: Any, *, experiment_id: str, parameter_set_id: str, params: dict[str, Any], orders: list[dict[str, Any]], reason_counter: Counter[str], symbol_count: int, days: int, pool_counts: dict[str, int], attempt_count: int, generated_at: str) -> dict[str, Any]:
    metrics = _metrics(orders)
    metrics.update(
        {
            "accepted_count": len(orders),
            "blocked_count": sum(reason_counter.values()),
            "symbol_count": symbol_count,
            "days": days,
            "strategy4_replay_mode": REPLAY_MODE,
            "legacy_mode": False,
            "pool_counts": pool_counts,
            "attempt_count": attempt_count,
            "admitted_count": sum(pool_counts.values()),
            "executable_pool_count": pool_counts.get("executable", 0),
            "still_wait_count": pool_counts.get("still_wait", 0) + pool_counts.get("observing", 0),
            "hard_denied_count": pool_counts.get("hard_denied", 0),
            "expired_count": pool_counts.get("expired", 0),
        }
    )
    metric_id = _stable_id("p21s4m", {"e": experiment_id, "p": parameter_set_id}, 24)
    conn.execute(
        "INSERT OR REPLACE INTO p21_v2_30d_metrics(metric_id, experiment_id, parameter_set_id, strategy_line, metrics_json, parameters_json, generated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
        (metric_id, experiment_id, parameter_set_id, "strategy4", _json(metrics), _json(params), generated_at),
    )
    for day, day_orders in _group_by_day(orders).items():
        conn.execute(
            "INSERT OR REPLACE INTO p21_v2_daily_metrics(metric_id, experiment_id, parameter_set_id, strategy_line, day, metrics_json, generated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
            (_stable_id("p21s4d", {"e": experiment_id, "p": parameter_set_id, "d": day}, 24), experiment_id, parameter_set_id, "strategy4", day, _json(_metrics(day_orders)), generated_at),
        )
    for symbol, symbol_orders in _group_by_symbol(orders).items():
        conn.execute(
            "INSERT OR REPLACE INTO p21_v2_symbol_metrics(metric_id, experiment_id, parameter_set_id, strategy_line, symbol, metrics_json, generated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
            (_stable_id("p21s4s", {"e": experiment_id, "p": parameter_set_id, "s": symbol}, 24), experiment_id, parameter_set_id, "strategy4", symbol, _json(_metrics(symbol_orders)), generated_at),
        )
    return metrics


def run_strategy4_replay_payload(
    project_root: Path,
    *,
    symbols: list[str] | None = None,
    days: int = DEFAULT_WINDOW_DAYS,
    max_symbols: int = 5,
    max_sets: int = 1,
    max_admissions_per_symbol: int = 20,
    max_attempts: int = 12,
    observe_interval_min: int = 5,
    write: bool = True,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    _ensure_tables(db_path)
    selected = [s.upper() for s in (symbols or universe_symbols(project_root, limit=max_symbols))[: max(1, int(max_symbols or 5))]]
    end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=max(1, int(days)))
    start_ms = _ms_from_dt(start_dt)
    end_ms = _ms_from_dt(end_dt)
    generated_at = _now()
    experiment_id = _stable_id("p21s4exp", {"symbols": selected, "days": days, "generated_at": generated_at, "mode": REPLAY_MODE}, 20)
    parameter_sets = default_parameter_sets(project_root, strategy_line="without_micro", max_sets=max_sets)[: max(1, int(max_sets or 1))]
    base = load_runtime_line_config(project_root, "without_micro")
    base_min_score = _num(base.get("min_score"), 68.0)
    leaderboard: list[dict[str, Any]] = []

    with _connect(db_path) as conn:
        symbol_rows = {sym: _rows_for_symbol(conn, sym, start_ms, end_ms) for sym in selected}
        if write:
            conn.execute(
                """
                INSERT OR REPLACE INTO p21_v2_experiments(
                  experiment_id, strategy_line, symbols_json, start_time, end_time, days,
                  parameter_set_count, trade_count, best_parameter_set_id, best_profit_factor,
                  best_expectancy_R, status, schema_version, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    experiment_id,
                    "strategy4",
                    _json(selected),
                    start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    days,
                    len(parameter_sets),
                    0,
                    None,
                    None,
                    None,
                    "running",
                    SCHEMA_VERSION,
                    generated_at,
                ),
            )

        for pset in parameter_sets:
            source_params = dict(pset.get("parameters") or pset)
            source_params["strategy_line"] = "without_micro"
            params = dict(source_params)
            params["strategy_line"] = "strategy4"
            source_parameter_set_id = pset.get("parameter_set_id") or _stable_id("p21v2", source_params, 16)
            parameter_set_id = _stable_id("p21s4", {"source": source_parameter_set_id, "params": params, "mode": REPLAY_MODE}, 16)
            if write:
                conn.execute(
                    "INSERT OR REPLACE INTO p21_v2_parameter_sets(parameter_set_id, experiment_id, strategy_line, parameters_json, generated_at) VALUES(?, ?, ?, ?, ?)",
                    (parameter_set_id, experiment_id, "strategy4", _json({**params, "source_parameter_set_id": source_parameter_set_id, "strategy4_replay_mode": REPLAY_MODE}), generated_at),
                )
            pools: dict[str, dict[str, Any]] = {}
            orders: list[dict[str, Any]] = []
            reasons: Counter[str] = Counter()
            attempt_count = 0
            for sym in selected:
                rows = symbol_rows.get(sym) or []
                admissions = 0
                for idx in _candidate_indices(rows):
                    if admissions >= max_admissions_per_symbol:
                        break
                    signal = _signal_at_index(rows, symbol=sym, strategy_line="without_micro", idx=idx, signal_id_prefix="s1_admission")
                    if signal is None or signal.score < max(20.0, base_min_score * 0.55):
                        continue
                    evaluated = evaluate_signal_offline(signal, rows, source_params)
                    plan = _first_plan(evaluated)
                    if not plan:
                        reasons.update(evaluated.get("reason_codes") or ["without_micro_no_plan"])
                        continue
                    classified = classify_strategy1_plan(plan)
                    if not classified.get("retryable"):
                        reasons.update(classified.get("reason_codes") or [classified.get("state") or "not_retryable"])
                        continue
                    pool = _pool_row(
                        experiment_id=experiment_id,
                        parameter_set_id=parameter_set_id,
                        source_parameter_set_id=source_parameter_set_id,
                        signal=signal,
                        plan=plan,
                        classified=classified,
                        observe_interval_min=observe_interval_min,
                    )
                    pools[pool["pool_id"]] = pool
                    admissions += 1
                    if write:
                        _write_pool(conn, pool, generated_at)
                        _write_event(conn, experiment_id=experiment_id, parameter_set_id=parameter_set_id, event_type="admitted", symbol=sym, pool_id=pool["pool_id"], event_time_ms=signal.signal_time_ms, evidence={"classified": classified}, generated_at=generated_at)

                    for attempt_index in range(1, max(1, int(max_attempts or 1)) + 1):
                        due_ms = int(pool["next_check_time_ms"] or 0)
                        due_idx = next((i for i, row in enumerate(rows) if int(row["open_time_ms"]) >= due_ms), -1)
                        recheck = _signal_at_index(rows, symbol=sym, strategy_line="strategy4", idx=due_idx, signal_id_prefix=f"{pool['pool_id']}_{attempt_index}") if due_idx >= 0 else None
                        if recheck is None:
                            pool.update({"status": "expired", "evict_reason": "replay_input_exhausted", "ttl_age_sec": int((due_ms - pool["first_seen_time_ms"]) / 1000) if due_ms else None})
                            break
                        evaluated4 = evaluate_signal_offline(recheck, rows, params)
                        plan4 = _first_plan(evaluated4)
                        reason_codes = list(evaluated4.get("reason_codes") or (plan4.get("reason_codes") if plan4 else []) or ["strategy4_no_plan"])
                        hard = bool(set(reason_codes) & HARD_DENY_REASON_CODES) or str(plan4.get("decision") if plan4 else "") == "NO_TRADE"
                        executable = bool(evaluated4.get("executable"))
                        current_side = str(plan4.get("decision") or recheck.side if plan4 else recheck.side)
                        side_changed = bool(pool.get("original_side") and pool.get("original_side") != current_side)
                        attempt_id = _stable_id("s4att", {"pool": pool["pool_id"], "idx": attempt_index, "time": recheck.signal_time_ms}, 24)
                        attempt_row = {
                            "attempt_id": attempt_id,
                            "pool_id": pool["pool_id"],
                            "experiment_id": experiment_id,
                            "parameter_set_id": parameter_set_id,
                            "symbol": sym,
                            "attempt_index": attempt_index,
                            "recheck_time_ms": recheck.signal_time_ms,
                            "decision": current_side,
                            "action": plan4.get("action") if plan4 else None,
                            "entry_mode": plan4.get("entry_mode") if plan4 else None,
                            "executable": executable,
                            "reason_codes": reason_codes,
                            "original_side": pool.get("original_side"),
                            "current_side": current_side,
                            "side_changed": side_changed,
                            "entry_price": plan4.get("estimated_entry_price") if plan4 else None,
                            "stop_loss": plan4.get("stop_loss") if plan4 else None,
                            "take_profit": plan4.get("take_profit") if plan4 else None,
                            "planned_rr": plan4.get("rr") if plan4 else None,
                            "raw_plan": plan4,
                        }
                        attempt_count += 1
                        if write:
                            _write_attempt(conn, attempt_row, generated_at)
                        pool.update(
                            {
                                "last_checked_time_ms": recheck.signal_time_ms,
                                "attempt_count": attempt_index,
                                "current_side": current_side,
                                "side_changed": side_changed,
                                "latest_action": attempt_row["action"],
                                "latest_entry_mode": attempt_row["entry_mode"],
                                "latest_executable": executable,
                                "latest_reason_codes": reason_codes,
                                "ttl_age_sec": int((recheck.signal_time_ms - pool["first_seen_time_ms"]) / 1000),
                            }
                        )
                        if executable:
                            order = dict(evaluated4.get("order") or {})
                            if order:
                                filled = simulate_1m_fill(order, rows, params)
                                filled.update(
                                    {
                                        "order_id": _stable_id("p21s4ord", {"p": parameter_set_id, "attempt": attempt_id}, 24),
                                        "parameter_set_id": parameter_set_id,
                                        "reasons": reason_codes,
                                        "lineage_mode": REPLAY_MODE,
                                        "source_contract_version": SOURCE_CONTRACT_VERSION,
                                        "config_patch": params,
                                        "trade_plan_payload": evaluated4.get("trade_plan_payload") or {},
                                        "fill_result": {
                                            "exit_time_ms": filled.get("exit_time_ms"),
                                            "exit_price": filled.get("exit_price"),
                                            "exit_reason": filled.get("exit_reason"),
                                            "net_R": filled.get("net_R"),
                                        },
                                        "features": {
                                            **dict(filled.get("features") or {}),
                                            "strategy4_replay_mode": REPLAY_MODE,
                                            "observe_pool_id": pool["pool_id"],
                                            "attempt_id": attempt_id,
                                            "attempt_index": attempt_index,
                                            "origin_strategy_line": "without_micro",
                                            "source_parameter_set_id": source_parameter_set_id,
                                            "original_side": pool.get("original_side"),
                                            "current_side": current_side,
                                            "side_changed": side_changed,
                                        },
                                    }
                                )
                                orders.append(filled)
                                if write:
                                    _insert_order(conn, experiment_id=experiment_id, parameter_set_id=parameter_set_id, params=params, filled=filled, generated_at=generated_at)
                            pool.update({"status": "executable", "next_check_time_ms": None, "evict_reason": ""})
                            break
                        if hard:
                            pool.update({"status": "hard_denied", "next_check_time_ms": None, "evict_reason": ",".join(sorted(set(reason_codes) & HARD_DENY_REASON_CODES)) or "hard_deny"})
                            break
                        pool.update({"status": "still_wait", "next_check_time_ms": recheck.signal_time_ms + observe_interval_min * 60_000})
                    if pool.get("status") in {"observing", "still_wait"} and pool.get("attempt_count", 0) >= max_attempts:
                        pool.update({"status": "expired", "evict_reason": "max_attempts_reached"})
                    if write:
                        _write_pool(conn, pool, generated_at)
                        _write_event(conn, experiment_id=experiment_id, parameter_set_id=parameter_set_id, event_type=str(pool["status"]), symbol=sym, pool_id=pool["pool_id"], event_time_ms=pool.get("last_checked_time_ms") or pool.get("first_seen_time_ms"), evidence={"latest_reason_codes": pool.get("latest_reason_codes") or [], "evict_reason": pool.get("evict_reason")}, generated_at=generated_at)
            pool_counts = dict(Counter(str(row.get("status") or "unknown") for row in pools.values()))
            if write:
                metrics = _persist_metrics(
                    conn,
                    experiment_id=experiment_id,
                    parameter_set_id=parameter_set_id,
                    params={**params, "source_parameter_set_id": source_parameter_set_id, "strategy4_replay_mode": REPLAY_MODE},
                    orders=orders,
                    reason_counter=reasons,
                    symbol_count=len(selected),
                    days=days,
                    pool_counts=pool_counts,
                    attempt_count=attempt_count,
                    generated_at=generated_at,
                )
            else:
                metrics = _metrics(orders)
                metrics.update(
                    {
                        "accepted_count": len(orders),
                        "blocked_count": sum(reasons.values()),
                        "symbol_count": len(selected),
                        "days": days,
                        "strategy4_replay_mode": REPLAY_MODE,
                        "legacy_mode": False,
                        "pool_counts": pool_counts,
                        "attempt_count": attempt_count,
                        "admitted_count": sum(pool_counts.values()),
                        "executable_pool_count": pool_counts.get("executable", 0),
                        "still_wait_count": pool_counts.get("still_wait", 0) + pool_counts.get("observing", 0),
                        "hard_denied_count": pool_counts.get("hard_denied", 0),
                        "expired_count": pool_counts.get("expired", 0),
                    }
                )
            leaderboard.append(
                {
                    "experiment_id": experiment_id,
                    "parameter_set_id": parameter_set_id,
                    "source_parameter_set_id": source_parameter_set_id,
                    "strategy_line": "strategy4",
                    "strategy4_replay_mode": REPLAY_MODE,
                    "metrics": metrics,
                    "pool_counts": pool_counts,
                    "attempt_count": attempt_count,
                }
            )
        if write:
            best = max(leaderboard, key=lambda row: ((row["metrics"].get("profit_factor") or -999), row["metrics"].get("expectancy_R") or -999), default=None)
            conn.execute(
                """
                UPDATE p21_v2_experiments
                SET trade_count = ?, best_parameter_set_id = ?, best_profit_factor = ?, best_expectancy_R = ?,
                    status = ?, schema_version = ?, generated_at = ?
                WHERE experiment_id = ?
                """,
                (
                    sum(int(row["metrics"].get("trade_count") or 0) for row in leaderboard),
                    best.get("parameter_set_id") if best else None,
                    best.get("metrics", {}).get("profit_factor") if best else None,
                    best.get("metrics", {}).get("expectancy_R") if best else None,
                    "completed",
                    SCHEMA_VERSION,
                    generated_at,
                    experiment_id,
                ),
            )
    leaderboard.sort(key=lambda row: ((row["metrics"].get("profit_factor") or -999), row["metrics"].get("expectancy_R") or -999), reverse=True)
    return {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": experiment_id,
        "strategy4_replay_mode": REPLAY_MODE,
        "legacy_mode": False,
        "symbols": selected,
        "days": days,
        "parameter_set_count": len(parameter_sets),
        "leaderboard": leaderboard,
        "generated_at": generated_at,
    }


def strategy4_replay_summary_payload(project_root: Path, *, experiment_id: str | None = None, parameter_set_id: str | None = None) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    _ensure_tables(db_path)
    where: list[str] = []
    args: list[Any] = []
    if experiment_id:
        where.append("experiment_id = ?")
        args.append(experiment_id)
    if parameter_set_id:
        where.append("parameter_set_id = ?")
        args.append(parameter_set_id)
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        pool_rows = [dict(row) for row in conn.execute(f"SELECT status, COUNT(*) AS n FROM p21_strategy4_replay_pool {clause} GROUP BY status", args).fetchall()]
        attempts = int(conn.execute(f"SELECT COUNT(*) FROM p21_strategy4_replay_attempts {clause}", args).fetchone()[0])
        executable = int(conn.execute(f"SELECT COUNT(*) FROM p21_strategy4_replay_attempts {clause}{' AND' if clause else 'WHERE'} executable = 1", args).fetchone()[0])
        metrics = [
            dict(row)
            for row in conn.execute(
                f"SELECT * FROM p21_v2_30d_metrics WHERE strategy_line = 'strategy4' {'AND experiment_id = ?' if experiment_id else ''} ORDER BY generated_at DESC LIMIT 20",
                ([experiment_id] if experiment_id else []),
            ).fetchall()
        ]
    for row in metrics:
        row["metrics"] = _loads(row.pop("metrics_json", None), {})
        row["parameters"] = _loads(row.pop("parameters_json", None), {})
    return {
        "schema_version": SCHEMA_VERSION,
        "strategy4_replay_mode": REPLAY_MODE,
        "pool_counts": {row["status"]: int(row["n"]) for row in pool_rows},
        "attempt_count": attempts,
        "executable_attempt_count": executable,
        "metrics": metrics,
        "generated_at": _now(),
    }


def strategy4_replay_pool_payload(project_root: Path, *, experiment_id: str | None = None, parameter_set_id: str | None = None, status: str | None = None, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    _ensure_tables(db_path)
    where: list[str] = []
    args: list[Any] = []
    if experiment_id:
        where.append("experiment_id = ?")
        args.append(experiment_id)
    if parameter_set_id:
        where.append("parameter_set_id = ?")
        args.append(parameter_set_id)
    if status and status != "all":
        where.append("status = ?")
        args.append(status)
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    limit_i = max(1, min(int(limit or 100), 500))
    offset_i = max(0, int(offset or 0))
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total = int(conn.execute(f"SELECT COUNT(*) FROM p21_strategy4_replay_pool {clause}", args).fetchone()[0])
        rows = [dict(row) for row in conn.execute(f"SELECT * FROM p21_strategy4_replay_pool {clause} ORDER BY first_seen_time_ms DESC LIMIT ? OFFSET ?", [*args, limit_i, offset_i]).fetchall()]
    for row in rows:
        row["source_reason_codes"] = _loads(row.pop("source_reason_codes_json", None), [])
        row["latest_reason_codes"] = _loads(row.pop("latest_reason_codes_json", None), [])
        row["raw_pool"] = _loads(row.pop("raw_pool_json", None), {})
    return {"schema_version": SCHEMA_VERSION, "total": total, "count": len(rows), "limit": limit_i, "offset": offset_i, "pool": rows}


def strategy4_replay_attempts_payload(project_root: Path, *, experiment_id: str | None = None, parameter_set_id: str | None = None, symbol: str | None = None, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    _ensure_tables(db_path)
    where: list[str] = []
    args: list[Any] = []
    if experiment_id:
        where.append("experiment_id = ?")
        args.append(experiment_id)
    if parameter_set_id:
        where.append("parameter_set_id = ?")
        args.append(parameter_set_id)
    if symbol:
        where.append("symbol = ?")
        args.append(symbol.upper())
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    limit_i = max(1, min(int(limit or 100), 500))
    offset_i = max(0, int(offset or 0))
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total = int(conn.execute(f"SELECT COUNT(*) FROM p21_strategy4_replay_attempts {clause}", args).fetchone()[0])
        rows = [dict(row) for row in conn.execute(f"SELECT * FROM p21_strategy4_replay_attempts {clause} ORDER BY recheck_time_ms DESC LIMIT ? OFFSET ?", [*args, limit_i, offset_i]).fetchall()]
    for row in rows:
        row["reason_codes"] = _loads(row.pop("reason_codes_json", None), [])
        row["raw_plan"] = _loads(row.pop("raw_plan_json", None), {})
    return {"schema_version": SCHEMA_VERSION, "total": total, "count": len(rows), "limit": limit_i, "offset": offset_i, "attempts": rows}
