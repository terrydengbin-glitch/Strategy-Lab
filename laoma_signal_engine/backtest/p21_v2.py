from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
import sqlite3
import subprocess
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Callable, Iterable

import httpx
import yaml

from laoma_signal_engine.backtest.execution_contract import (
    DEFAULT_BACKTEST_EXECUTION_CONTRACT,
    LEGACY_BACKTEST_EXECUTION_CONTRACT,
    RERUN_REQUIRED_BLOCK_REASON,
    legacy_backtest_metadata,
)
from laoma_signal_engine.backtest.p21 import P21_DB_RELATIVE, p21_db_path
from laoma_signal_engine.research_db import upsert_backtest_order_native
from laoma_signal_engine.backtest.p21_real_evaluator import ENGINE_MODE, evaluate_signal_offline
from laoma_signal_engine.market.kline_fetcher import FUTURES_REST

SCHEMA_VERSION = "21.2-config-matrix-v2"
TARGET_STRATEGY_LINES = ("without_micro", "strategy4", "strategy5", "strategy6")
DEFAULT_INTERVAL = "1m"
DEFAULT_WINDOW_DAYS = 30
DEFAULT_MAX_HOLD_MINUTES = 120
CONFIG_PATH = Path("laoma_signal_engine/config/default.yaml")
UNIVERSE_PATH = Path("DATA/universe/CANDIDATE_UNIVERSE.json")
CANDIDATE_EXPORT_PATH = Path("DATA/backtest/p21_v2_latest_config_candidate.json")
STEP7_95_PROGRESS_PATH = Path("DATA/backtest/step7_95_full_universe_progress.json")
JOB_LOG_DIR = Path("DATA/backtest/jobs")
P21_V2_LEGACY_REASON = (
    "P21 V2 config matrix uses evaluate_signal_offline + simulate_1m_fill and writes p21_v2_shadow_orders; "
    "it bypasses paper.adapter, PaperEngine, paper V5 gate lineage, and paper ledger state."
)


def _legacy_contract_metadata() -> dict[str, Any]:
    return legacy_backtest_metadata(reason=P21_V2_LEGACY_REASON, engine_mode=ENGINE_MODE)


def _decorate_legacy_contract(row: dict[str, Any]) -> dict[str, Any]:
    row.update(_legacy_contract_metadata())
    row["legacy_mode"] = True
    return row


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _loads(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.OperationalError:
        # A running writer may hold the database briefly; busy_timeout still covers
        # ordinary reads/writes, and the next connection can enable WAL.
        pass
    return conn


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 8) if values else 0.0


def _med(values: list[float]) -> float:
    return round(float(median(values)), 8) if values else 0.0


def _ratio(part: int | float, whole: int | float) -> float:
    return round(float(part) / float(whole), 8) if whole else 0.0


def _dt_from_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _ms_from_dt(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def _iso_from_ms(value: int) -> str:
    return _dt_from_ms(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stable_id(prefix: str, payload: Any, size: int = 20) -> str:
    return f"{prefix}_{hashlib.sha256(_json(payload).encode('utf-8')).hexdigest()[:size]}"


def ensure_p21_v2_tables(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS p21_klines_1m(
              symbol TEXT NOT NULL,
              open_time_ms INTEGER NOT NULL,
              open_time TEXT NOT NULL,
              open REAL NOT NULL,
              high REAL NOT NULL,
              low REAL NOT NULL,
              close REAL NOT NULL,
              volume REAL NOT NULL,
              quote_volume REAL NOT NULL,
              trade_count INTEGER NOT NULL DEFAULT 0,
              taker_buy_base_volume REAL NOT NULL DEFAULT 0,
              taker_buy_quote_volume REAL NOT NULL DEFAULT 0,
              source TEXT NOT NULL,
              download_batch_id TEXT,
              inserted_at TEXT NOT NULL,
              PRIMARY KEY(symbol, open_time_ms)
            );
            CREATE INDEX IF NOT EXISTS idx_p21_klines_symbol_time
              ON p21_klines_1m(symbol, open_time_ms);
            CREATE TABLE IF NOT EXISTS p21_kline_download_ledger(
              batch_id TEXT PRIMARY KEY,
              symbol TEXT NOT NULL,
              interval TEXT NOT NULL,
              start_time TEXT NOT NULL,
              end_time TEXT NOT NULL,
              requested_rows INTEGER NOT NULL,
              fetched_rows INTEGER NOT NULL,
              status TEXT NOT NULL,
              reason TEXT,
              evidence_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_v2_experiments(
              experiment_id TEXT PRIMARY KEY,
              strategy_line TEXT NOT NULL,
              symbols_json TEXT NOT NULL,
              start_time TEXT NOT NULL,
              end_time TEXT NOT NULL,
              days INTEGER NOT NULL,
              parameter_set_count INTEGER NOT NULL,
              trade_count INTEGER NOT NULL,
              best_parameter_set_id TEXT,
              best_profit_factor REAL,
              best_expectancy_R REAL,
              status TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_v2_parameter_sets(
              parameter_set_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              parameters_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_v2_shadow_orders(
              order_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              signal_time_ms INTEGER NOT NULL,
              entry_time_ms INTEGER NOT NULL,
              exit_time_ms INTEGER,
              entry_price REAL NOT NULL,
              stop_loss REAL NOT NULL,
              take_profit REAL NOT NULL,
              planned_rr REAL NOT NULL,
              net_R REAL,
              exit_reason TEXT,
              score REAL NOT NULL,
              reasons_json TEXT NOT NULL,
              features_json TEXT NOT NULL,
              lineage_mode TEXT,
              source_contract_version TEXT,
              config_patch_json TEXT,
              trade_plan_payload_json TEXT,
              fill_result_json TEXT,
              entry_mode TEXT,
              effective_rr REAL,
              fast_exit_policy_json TEXT,
              generated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_p21_v2_orders_exp_param
              ON p21_v2_shadow_orders(experiment_id, parameter_set_id, strategy_line, symbol);
            CREATE TABLE IF NOT EXISTS p21_v2_daily_metrics(
              metric_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              day TEXT NOT NULL,
              metrics_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_v2_symbol_metrics(
              metric_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              symbol TEXT NOT NULL,
              metrics_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_v2_30d_metrics(
              metric_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              metrics_json TEXT NOT NULL,
              parameters_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_v2_recommendations(
              recommendation_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              status TEXT NOT NULL,
              priority INTEGER NOT NULL,
              summary TEXT NOT NULL,
              metrics_json TEXT NOT NULL,
              parameters_json TEXT NOT NULL,
              risks_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_v2_jobs(
              job_id TEXT PRIMARY KEY,
              job_type TEXT NOT NULL,
              status TEXT NOT NULL,
              phase TEXT NOT NULL,
              pid INTEGER,
              request_json TEXT NOT NULL,
              progress_json TEXT NOT NULL,
              log_path TEXT,
              last_error TEXT,
              started_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              finished_at TEXT
            );
            CREATE TABLE IF NOT EXISTS p21_v2_job_events(
              event_id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL,
              status TEXT NOT NULL,
              phase TEXT NOT NULL,
              message TEXT,
              evidence_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_v2_matrix_shards(
              shard_id TEXT PRIMARY KEY,
              job_id TEXT,
              experiment_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              symbol_start INTEGER NOT NULL,
              symbol_end INTEGER NOT NULL,
              symbols_json TEXT NOT NULL,
              status TEXT NOT NULL,
              order_count INTEGER NOT NULL DEFAULT 0,
              metrics_json TEXT NOT NULL,
              error TEXT,
              started_at TEXT NOT NULL,
              finished_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_p21_v2_matrix_shards_exp_param
              ON p21_v2_matrix_shards(experiment_id, parameter_set_id, status);
            """
        )
        _ensure_columns(
            conn,
            "p21_v2_shadow_orders",
            {
                "lineage_mode": "TEXT",
                "source_contract_version": "TEXT",
                "config_patch_json": "TEXT",
                "trade_plan_payload_json": "TEXT",
                "fill_result_json": "TEXT",
                "entry_mode": "TEXT",
                "effective_rr": "REAL",
                "fast_exit_policy_json": "TEXT",
            },
        )


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def universe_symbols(project_root: Path, *, limit: int | None = None) -> list[str]:
    data = _read_json(project_root / UNIVERSE_PATH, {})
    symbols: list[str] = []
    for row in data.get("pairs") or []:
        if not isinstance(row, dict):
            continue
        sym = row.get("futures_symbol") or row.get("symbol") or row.get("pair")
        if not sym:
            continue
        sym = str(sym).upper()
        if sym.endswith("USDT") and sym not in symbols:
            symbols.append(sym)
    if not symbols:
        symbols = ["BTCUSDT", "ETHUSDT"]
    return symbols[:limit] if limit else symbols


def load_runtime_line_config(project_root: Path, line: str = "without_micro") -> dict[str, Any]:
    path = project_root / CONFIG_PATH
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    lines = ((data.get("trade_plan_lines") or {}) if isinstance(data, dict) else {})
    raw = dict(lines.get(line) or {})
    inherit = raw.get("inherit_from")
    if inherit and inherit in lines:
        merged = dict(lines.get(inherit) or {})
        merged.update(raw)
        raw = merged
    return raw


def _fetch_binance_klines(
    client: httpx.Client,
    symbol: str,
    *,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
) -> list[list[Any]]:
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": max(1, min(int(limit), 1500)),
    }
    response: httpx.Response | None = None
    for attempt in range(1, 6):
        response = client.get(f"{FUTURES_REST}/fapi/v1/klines", params=params, timeout=20)
        if response.status_code not in {418, 429}:
            response.raise_for_status()
            break
        retry_after = response.headers.get("Retry-After")
        try:
            wait_sec = float(retry_after) if retry_after else 0.0
        except ValueError:
            wait_sec = 0.0
        wait_sec = max(wait_sec, min(90.0, 5.0 * attempt))
        time.sleep(wait_sec)
    else:
        assert response is not None
        response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise TypeError("Binance kline response must be a list")
    return [row for row in data if isinstance(row, list) and len(row) >= 11]


def _insert_kline_rows(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    rows: list[list[Any]],
    batch_id: str,
    source: str = "binance",
) -> int:
    inserted_at = _now()
    count = 0
    for row in rows:
        open_time_ms = int(row[0])
        conn.execute(
            """
            INSERT OR REPLACE INTO p21_klines_1m(
              symbol, open_time_ms, open_time, open, high, low, close, volume,
              quote_volume, trade_count, taker_buy_base_volume, taker_buy_quote_volume,
              source, download_batch_id, inserted_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol.upper(),
                open_time_ms,
                _iso_from_ms(open_time_ms),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
                float(row[7]) if len(row) > 7 else 0.0,
                int(row[8]) if len(row) > 8 else 0,
                float(row[9]) if len(row) > 9 else 0.0,
                float(row[10]) if len(row) > 10 else 0.0,
                source,
                batch_id,
                inserted_at,
            ),
        )
        count += 1
    return count


def kline_cache_status_payload(
    project_root: Path,
    *,
    symbols: list[str] | None = None,
    days: int = DEFAULT_WINDOW_DAYS,
    max_symbols: int = 50,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=max(1, int(days)))
    start_ms = _ms_from_dt(start_dt)
    selected = symbols or universe_symbols(project_root, limit=max_symbols)
    rows: list[dict[str, Any]] = []
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        for sym in selected:
            row = conn.execute(
                """
                SELECT COUNT(*) AS row_count, MIN(open_time_ms) AS min_ms, MAX(open_time_ms) AS max_ms
                FROM p21_klines_1m WHERE symbol = ? AND open_time_ms >= ?
                """,
                (sym.upper(), start_ms),
            ).fetchone()
            count = int(row["row_count"] or 0)
            expected = max(1, int(days) * 1440)
            rows.append(
                {
                    "symbol": sym.upper(),
                    "row_count": count,
                    "expected_rows": expected,
                    "coverage": round(count / expected, 6),
                    "first_open_time": _iso_from_ms(int(row["min_ms"])) if row["min_ms"] else None,
                    "last_open_time": _iso_from_ms(int(row["max_ms"])) if row["max_ms"] else None,
                    "status": "ready" if count >= expected * 0.95 else "missing",
                }
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "days": days,
        "count": len(rows),
        "ready_count": sum(1 for row in rows if row["status"] == "ready"),
        "symbols": rows,
        "generated_at": _now(),
    }


def download_kline_cache_payload(
    project_root: Path,
    *,
    symbols: list[str] | None = None,
    days: int = DEFAULT_WINDOW_DAYS,
    max_symbols: int = 10,
    interval: str = DEFAULT_INTERVAL,
    dry_run: bool = False,
    sleep_sec: float = 0.05,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    selected = symbols or universe_symbols(project_root, limit=max_symbols)
    selected = [sym.upper() for sym in selected[: max(1, int(max_symbols or len(selected)))]]
    end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=max(1, int(days)))
    start_ms = _ms_from_dt(start_dt)
    end_ms = _ms_from_dt(end_dt)
    batch_root = _stable_id("p21dl", {"symbols": selected, "start": start_ms, "end": end_ms, "dry_run": dry_run}, 16)
    ledger: list[dict[str, Any]] = []
    own_client = client is None
    http = client or httpx.Client()
    try:
        with _connect(db_path) as conn:
            for sym in selected:
                batch_id = f"{batch_root}_{sym}"
                fetched_rows = 0
                status = "dry_run" if dry_run else "ok"
                reason = ""
                cursor = start_ms
                try:
                    while cursor < end_ms:
                        chunk_end = min(end_ms, cursor + 1500 * 60_000)
                        if dry_run:
                            rows: list[list[Any]] = []
                        else:
                            rows = _fetch_binance_klines(http, sym, interval=interval, start_ms=cursor, end_ms=chunk_end - 1)
                            fetched_rows += _insert_kline_rows(conn, symbol=sym, rows=rows, batch_id=batch_id)
                        if rows:
                            cursor = int(rows[-1][0]) + 60_000
                        else:
                            cursor = chunk_end
                        if sleep_sec and not dry_run:
                            time.sleep(max(0.0, float(sleep_sec)))
                except Exception as exc:
                    status = "error"
                    reason = str(exc)
                evidence = {"start_ms": start_ms, "end_ms": end_ms, "dry_run": dry_run}
                conn.execute(
                    """
                    INSERT OR REPLACE INTO p21_kline_download_ledger(
                      batch_id, symbol, interval, start_time, end_time, requested_rows, fetched_rows,
                      status, reason, evidence_json, generated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        sym,
                        interval,
                        start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        int(days) * 1440,
                        fetched_rows,
                        status,
                        reason,
                        _json(evidence),
                        _now(),
                    ),
                )
                ledger.append({"batch_id": batch_id, "symbol": sym, "status": status, "fetched_rows": fetched_rows, "reason": reason})
    finally:
        if own_client:
            http.close()
    return {
        "schema_version": SCHEMA_VERSION,
        "batch_id": batch_root,
        "symbols": selected,
        "days": days,
        "dry_run": dry_run,
        "ledger": ledger,
        "generated_at": _now(),
    }


def _rows_for_symbol(conn: sqlite3.Connection, symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT * FROM p21_klines_1m
        WHERE symbol = ? AND open_time_ms >= ? AND open_time_ms < ?
        ORDER BY open_time_ms ASC
        """,
        (symbol.upper(), start_ms, end_ms),
    ).fetchall()
    return [dict(row) for row in rows]


def _rolling_avg(values: list[float], end: int, length: int) -> float:
    start = max(0, end - length)
    window = values[start:end]
    return sum(window) / len(window) if window else 0.0


def _pct(new: float, old: float) -> float:
    return (new / old - 1.0) * 10000.0 if old else 0.0


@dataclass(frozen=True)
class HistoricalSignal:
    signal_id: str
    strategy_line: str
    symbol: str
    side: str
    index: int
    signal_time_ms: int
    score: float
    features: dict[str, Any]


def _signal_side(pct_3m: float, pct_15m: float, *, strategy_line: str) -> str | None:
    if strategy_line in {"strategy5", "strategy6"}:
        if pct_3m > 0 and pct_15m >= 0:
            return "LONG"
        if pct_3m < 0 and pct_15m <= 0:
            return "SHORT"
        return None
    if pct_3m > 0:
        return "LONG"
    if pct_3m < 0:
        return "SHORT"
    return None


def build_historical_inputs(
    rows: list[dict[str, Any]],
    *,
    symbol: str,
    strategy_line: str,
    base_min_score: float = 60.0,
) -> list[HistoricalSignal]:
    if len(rows) < 40:
        return []
    closes = [_num(row["close"]) for row in rows]
    highs = [_num(row["high"]) for row in rows]
    lows = [_num(row["low"]) for row in rows]
    volumes = [_num(row["volume"]) for row in rows]
    out: list[HistoricalSignal] = []
    last_signal_idx = -999
    for idx in range(30, len(rows) - 3):
        close = closes[idx]
        pct_1m = _pct(close, closes[idx - 1])
        pct_3m = _pct(close, closes[idx - 3])
        pct_5m = _pct(close, closes[idx - 5])
        pct_15m = _pct(close, closes[idx - 15])
        avg_vol = _rolling_avg(volumes, idx, 30)
        volume_z = volumes[idx] / avg_vol if avg_vol else 0.0
        high_30 = max(highs[max(0, idx - 30) : idx + 1])
        low_30 = min(lows[max(0, idx - 30) : idx + 1])
        range_pos = (close - low_30) / (high_30 - low_30) if high_30 > low_30 else 0.5
        atr_bps = _avg([abs(highs[j] - lows[j]) / closes[j] * 10000 for j in range(max(1, idx - 14), idx + 1) if closes[j]])
        impulse = abs(pct_3m)
        score = min(100.0, impulse * 1.6 + max(0.0, volume_z - 1.0) * 16 + abs(pct_15m) * 0.25)
        side = _signal_side(pct_3m, pct_15m, strategy_line=strategy_line)
        if side is None:
            continue
        if idx - last_signal_idx < 5:
            continue
        if strategy_line == "strategy4":
            # Strategy4 observes WAIT-like names that were close but not strong enough for strategy1.
            if not (base_min_score * 0.55 <= score < base_min_score):
                continue
        elif score < max(20.0, base_min_score * 0.55):
            continue
        last_signal_idx = idx
        features = {
            "pct_1m_bps": round(pct_1m, 6),
            "pct_3m_bps": round(pct_3m, 6),
            "pct_5m_bps": round(pct_5m, 6),
            "pct_15m_bps": round(pct_15m, 6),
            "volume_z": round(volume_z, 6),
            "range_pos_30m": round(range_pos, 6),
            "atr_1m_bps": round(atr_bps, 6),
            "close": close,
        }
        payload = {"line": strategy_line, "symbol": symbol, "time": rows[idx]["open_time_ms"], "score": score, "side": side}
        out.append(
            HistoricalSignal(
                signal_id=_stable_id("sig", payload, 20),
                strategy_line=strategy_line,
                symbol=symbol.upper(),
                side=side,
                index=idx,
                signal_time_ms=int(rows[idx]["open_time_ms"]),
                score=round(score, 6),
                features=features,
            )
        )
    return out


def config_matrix_contract_payload(project_root: Path, *, strategy_line: str = "all", max_sets: int = 240) -> dict[str, Any]:
    runtime = load_runtime_line_config(project_root, "without_micro")
    sets = default_parameter_sets(project_root, strategy_line=strategy_line, max_sets=max_sets)
    return {
        "schema_version": SCHEMA_VERSION,
        "scope": "config_writable_parameters_only",
        **_legacy_contract_metadata(),
        "target_strategy_lines": list(TARGET_STRATEGY_LINES if strategy_line == "all" else [strategy_line]),
        "default_backtest_base": DEFAULT_BACKTEST_EXECUTION_CONTRACT,
        "current_runner_execution_contract": LEGACY_BACKTEST_EXECUTION_CONTRACT,
        "current_runner_promotion_allowed": False,
        "current_runner_promotion_block_reason": RERUN_REQUIRED_BLOCK_REASON,
        "runtime_baseline": {
            "min_score": runtime.get("min_score"),
            "target_rr": runtime.get("target_rr"),
            "min_net_rr": runtime.get("min_net_rr"),
            "min_effective_rr": runtime.get("min_effective_rr"),
            "min_reachable_reward_bps": runtime.get("min_reachable_reward_bps"),
            "stop_atr_mult": runtime.get("stop_atr_mult"),
            "max_stop_bps": runtime.get("max_stop_bps"),
            "tp_target_policy": runtime.get("tp_target_policy") or {},
        },
        "parameter_set_count": len(sets),
        "parameter_sets": sets,
        "generated_at": _now(),
    }


def default_parameter_sets(project_root: Path, *, strategy_line: str = "all", max_sets: int = 240) -> list[dict[str, Any]]:
    base = load_runtime_line_config(project_root, "without_micro")
    base_min_score = _num(base.get("min_score"), 68.0)
    base_target_rr = _num(base.get("target_rr"), 0.75)
    lines = list(TARGET_STRATEGY_LINES if strategy_line == "all" else [strategy_line])
    min_scores = sorted({max(20.0, base_min_score - 8), base_min_score, min(95.0, base_min_score + 6)})
    target_rrs = sorted({0.6, base_target_rr, 1.0})
    min_rrs = sorted({0.5, _num(base.get("min_rr"), 1.0), 1.0})
    min_net_rrs = sorted({0.55, _num(base.get("min_net_rr"), 0.9), 1.0})
    stop_atr_mults = sorted({0.9, _num(base.get("stop_atr_mult"), 1.2), 1.5})
    max_stop_bps_values = sorted({90.0, _num(base.get("max_stop_bps"), 180.0), 240.0})
    tp_modes = ["structure", "fast_capped_rr"]
    range_long_max = [0.72, 0.82, 0.9]
    range_short_min = [0.1, 0.18, 0.28]
    out: list[dict[str, Any]] = []
    for line, min_score, target_rr, min_rr, min_net_rr, stop_atr, max_stop, tp_mode, long_max, short_min in itertools.product(
        lines,
        min_scores,
        target_rrs,
        min_rrs,
        min_net_rrs,
        stop_atr_mults,
        max_stop_bps_values,
        tp_modes,
        range_long_max,
        range_short_min,
    ):
        params = {
            "strategy_line": line,
            "min_score": round(float(min_score), 4),
            "target_rr": round(float(target_rr), 4),
            "min_rr": round(float(min_rr), 4),
            "min_net_rr": round(float(min_net_rr), 4),
            "min_effective_rr": round(max(0.2, float(min_net_rr) - 0.05), 4),
            "stop_atr_mult": round(float(stop_atr), 4),
            "max_stop_bps": round(float(max_stop), 4),
            "min_stop_bps": _num(base.get("min_stop_bps"), 3.0),
            "min_reachable_reward_bps": _num(base.get("min_reachable_reward_bps"), 18.0),
            "tp_target_policy": {
                "mode": tp_mode,
                "target_net_rr": 1.0 if tp_mode == "fast_capped_rr" else None,
                "target_rr_cap": 1.0 if tp_mode == "fast_capped_rr" else _num((base.get("tp_target_policy") or {}).get("target_rr_cap"), 0.9),
            },
            "range_room": {"long_max_range_pos": long_max, "short_min_range_pos": short_min},
            "taker_fee_bps": _num(base.get("taker_fee_bps"), 5.0),
            "slippage_bps": 2.0,
            "max_hold_minutes": DEFAULT_MAX_HOLD_MINUTES,
        }
        out.append({"parameter_set_id": _stable_id("p21v2", params, 16), "parameters": params})
        if len(out) >= max_sets:
            return out
    return out[:max_sets]


def _apply_config(signal: HistoricalSignal, params: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if signal.score < _num(params.get("min_score"), 60.0):
        reasons.append("score_below_min")
    range_pos = _num(signal.features.get("range_pos_30m"), 0.5)
    if signal.side == "LONG" and range_pos > _num((params.get("range_room") or {}).get("long_max_range_pos"), 0.82):
        reasons.append("long_range_room_low")
    if signal.side == "SHORT" and range_pos < _num((params.get("range_room") or {}).get("short_min_range_pos"), 0.18):
        reasons.append("short_range_room_low")
    if abs(_num(signal.features.get("pct_3m_bps"))) < 8:
        reasons.append("impulse_too_small")
    return not reasons, reasons


def _build_order(signal: HistoricalSignal, rows: list[dict[str, Any]], params: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    entry_idx = signal.index + 1
    if entry_idx >= len(rows):
        return None, ["missing_entry_candle"]
    entry = _num(rows[entry_idx]["open"])
    atr_bps = max(_num(signal.features.get("atr_1m_bps"), 10.0), _num(params.get("min_stop_bps"), 3.0))
    stop_bps = min(_num(params.get("max_stop_bps"), 180.0), max(_num(params.get("min_stop_bps"), 3.0), atr_bps * _num(params.get("stop_atr_mult"), 1.2)))
    target_rr = _num(params.get("target_rr"), 0.75)
    policy = params.get("tp_target_policy") or {}
    if policy.get("mode") == "fast_capped_rr":
        target_rr = min(target_rr, _num(policy.get("target_net_rr"), 1.0) or 1.0, _num(policy.get("target_rr_cap"), 1.0))
    target_bps = max(_num(params.get("min_reachable_reward_bps"), 12.0), stop_bps * target_rr)
    cost_bps = _num(params.get("taker_fee_bps"), 5.0) * 2 + _num(params.get("slippage_bps"), 2.0)
    effective_rr = (target_bps - cost_bps) / stop_bps if stop_bps else 0.0
    if effective_rr < _num(params.get("min_effective_rr"), 0.5):
        return None, ["effective_rr_below_min"]
    if signal.side == "LONG":
        stop = entry * (1.0 - stop_bps / 10000)
        target = entry * (1.0 + target_bps / 10000)
    else:
        stop = entry * (1.0 + stop_bps / 10000)
        target = entry * (1.0 - target_bps / 10000)
    return (
        {
            "symbol": signal.symbol,
            "strategy_line": signal.strategy_line,
            "side": signal.side,
            "signal_time_ms": signal.signal_time_ms,
            "entry_time_ms": int(rows[entry_idx]["open_time_ms"]),
            "entry_idx": entry_idx,
            "entry_price": entry,
            "stop_loss": stop,
            "take_profit": target,
            "stop_bps": stop_bps,
            "target_bps": target_bps,
            "planned_rr": target_bps / stop_bps if stop_bps else 0.0,
            "cost_bps": cost_bps,
            "score": signal.score,
            "features": signal.features,
        },
        [],
    )


def _strategy6_exit_param(params: dict[str, Any], key: str, default: Any = None) -> Any:
    block = params.get("strategy6") if isinstance(params.get("strategy6"), dict) else {}
    if key in block:
        return block.get(key)
    return params.get(key, default)


def _strategy6_exit_protection_enabled(order: dict[str, Any], params: dict[str, Any]) -> bool:
    if str(order.get("strategy_line") or params.get("strategy_line") or "").lower() != "strategy6":
        return False
    adaptive = _strategy6_exit_param(params, "strategy6_adaptive_exit_enabled")
    if adaptive is None:
        adaptive = _strategy6_exit_param(params, "adaptive_exit_enabled", False)
    if bool(adaptive):
        return True
    enabled = _strategy6_exit_param(params, "strategy6_exit_protection_enabled")
    if enabled is None:
        enabled = _strategy6_exit_param(params, "exit_protection_enabled", False)
    return bool(enabled)


def _adverse_close_bps(side: str, entry: float, close: float) -> float:
    if entry <= 0:
        return 0.0
    if side == "LONG":
        return max(0.0, (entry - close) / entry * 10000.0)
    if side == "SHORT":
        return max(0.0, (close - entry) / entry * 10000.0)
    return 0.0


def _r_from_price(side: str, entry: float, price: float, risk: float) -> float:
    if risk <= 0:
        return 0.0
    return (price - entry) / risk if side == "LONG" else (entry - price) / risk


def _simulate_strategy6_exit_protected_fill(order: dict[str, Any], rows: list[dict[str, Any]], params: dict[str, Any]) -> dict[str, Any]:
    max_hold = max(1, int(_num(params.get("max_hold_minutes"), DEFAULT_MAX_HOLD_MINUTES)))
    entry_idx = int(order["entry_idx"])
    side = str(order["side"]).upper()
    stop = _num(order["stop_loss"])
    target = _num(order["take_profit"])
    entry = _num(order["entry_price"])
    risk = abs(entry - stop)
    cost_r = (entry * _num(order.get("cost_bps"), 12.0) / 10000) / risk if risk else 0.0
    exit_price = _num(rows[min(len(rows) - 1, entry_idx)]["close"])
    exit_reason = "time_stop"
    exit_time_ms = int(rows[min(len(rows) - 1, entry_idx)]["open_time_ms"])
    max_mfe_r = 0.0
    max_mae_r = 0.0
    protection_reason: str | None = None
    adaptive_enabled = bool(
        _strategy6_exit_param(params, "strategy6_adaptive_exit_enabled")
        if _strategy6_exit_param(params, "strategy6_adaptive_exit_enabled") is not None
        else _strategy6_exit_param(params, "adaptive_exit_enabled", False)
    )
    features_in = dict(order.get("features") or {})
    adaptive_tier = str(
        _strategy6_exit_param(params, "adaptive_exit_tier")
        or features_in.get("strategy6_adaptive_exit_tier")
        or "medium_quality"
    )

    max_loss_r_cap = _num(_strategy6_exit_param(params, "max_loss_R_cap"), 0.0)
    first_tp_r = _num(_strategy6_exit_param(params, "first_tp_R"), 0.0)
    protect_after_mfe_r = _num(_strategy6_exit_param(params, "protect_after_mfe_R"), 0.0)
    trail_after_mfe_r = _num(_strategy6_exit_param(params, "trail_after_mfe_R"), 0.0)
    abort_if_mfe_lt_r = _num(_strategy6_exit_param(params, "abort_if_mfe_lt_R"), 0.0)
    abort_if_mae_gt_r = _num(_strategy6_exit_param(params, "abort_if_mae_gt_R"), 0.0)
    abort_window_min = max(1, int(_num(_strategy6_exit_param(params, "abort_window_min"), 3)))
    mfe_fail_exit_r = _num(_strategy6_exit_param(params, "mfe_fail_exit_R"), 0.0)
    max_initial_adverse_r = _num(_strategy6_exit_param(params, "max_initial_adverse_R"), 0.0)
    adverse_1m_exit_bps = _num(_strategy6_exit_param(params, "adverse_1m_exit_bps"), 0.0)
    adverse_3m_exit_bps = _num(_strategy6_exit_param(params, "adverse_3m_exit_bps"), 0.0)
    v3_5_scratch_after_min = int(_num(_strategy6_exit_param(params, "v3_5_scratch_after_minutes"), 0))
    v3_5_min_push_r = _num(_strategy6_exit_param(params, "v3_5_min_push_R"), 0.0)
    v3_5_early_adverse_r = _num(_strategy6_exit_param(params, "v3_5_early_adverse_R"), 0.0)
    v3_5_lock_trigger_r = _num(_strategy6_exit_param(params, "v3_5_lock_trigger_R"), 0.0)
    v3_5_lock_floor_r = _num(_strategy6_exit_param(params, "v3_5_lock_floor_R"), 0.0)

    if adaptive_enabled:
        if adaptive_tier == "v3_5_fast_scratch":
            max_loss_r_cap = max_loss_r_cap or _num(_strategy6_exit_param(params, "v3_5_fast_scratch_loss_cap_R"), 0.65)
            first_tp_r = first_tp_r or _num(_strategy6_exit_param(params, "v3_5_fast_scratch_first_tp_R"), 0.55)
            abort_window_min = max(1, v3_5_scratch_after_min or int(_num(_strategy6_exit_param(params, "v3_5_fast_scratch_window_min"), 2)))
            abort_if_mfe_lt_r = abort_if_mfe_lt_r or _num(_strategy6_exit_param(params, "v3_5_fast_scratch_abort_if_mfe_lt_R"), 0.08)
            abort_if_mae_gt_r = abort_if_mae_gt_r or _num(_strategy6_exit_param(params, "v3_5_fast_scratch_abort_if_mae_gt_R"), 0.35)
            mfe_fail_exit_r = mfe_fail_exit_r or v3_5_min_push_r or _num(_strategy6_exit_param(params, "v3_5_fast_scratch_min_push_R"), 0.12)
            max_initial_adverse_r = max_initial_adverse_r or v3_5_early_adverse_r or _num(_strategy6_exit_param(params, "v3_5_fast_scratch_early_adverse_R"), 0.45)
        elif adaptive_tier == "v3_5_rebound":
            max_loss_r_cap = max_loss_r_cap or _num(_strategy6_exit_param(params, "v3_5_rebound_loss_cap_R"), 0.75)
            first_tp_r = first_tp_r or _num(_strategy6_exit_param(params, "v3_5_rebound_first_tp_R"), 0.60)
            protect_after_mfe_r = protect_after_mfe_r or v3_5_lock_trigger_r or _num(_strategy6_exit_param(params, "v3_5_rebound_protect_after_mfe_R"), 0.45)
            trail_after_mfe_r = trail_after_mfe_r or _num(_strategy6_exit_param(params, "v3_5_rebound_trail_after_mfe_R"), 0.35)
            abort_window_min = max(1, v3_5_scratch_after_min or abort_window_min)
            mfe_fail_exit_r = mfe_fail_exit_r or v3_5_min_push_r
            max_initial_adverse_r = max_initial_adverse_r or v3_5_early_adverse_r
        elif adaptive_tier == "v3_5_profit_lock":
            max_loss_r_cap = max_loss_r_cap or _num(_strategy6_exit_param(params, "v3_5_profit_lock_loss_cap_R"), 0.80)
            first_tp_r = first_tp_r or _num(_strategy6_exit_param(params, "v3_5_profit_lock_first_tp_R"), 0.70)
            protect_after_mfe_r = protect_after_mfe_r or v3_5_lock_trigger_r or _num(_strategy6_exit_param(params, "v3_5_profit_lock_trigger_R"), 0.45)
            trail_after_mfe_r = trail_after_mfe_r or _num(_strategy6_exit_param(params, "v3_5_profit_lock_trail_R"), 0.30)
            abort_window_min = max(1, v3_5_scratch_after_min or abort_window_min)
            mfe_fail_exit_r = mfe_fail_exit_r or v3_5_min_push_r
            max_initial_adverse_r = max_initial_adverse_r or v3_5_early_adverse_r
        elif adaptive_tier == "v3_5_normal":
            max_loss_r_cap = max_loss_r_cap or _num(_strategy6_exit_param(params, "v3_5_normal_loss_cap_R"), 0.85)
            first_tp_r = first_tp_r or _num(_strategy6_exit_param(params, "v3_5_normal_first_tp_R"), 0.65)
            protect_after_mfe_r = protect_after_mfe_r or v3_5_lock_trigger_r
            mfe_fail_exit_r = mfe_fail_exit_r or v3_5_min_push_r
            max_initial_adverse_r = max_initial_adverse_r or v3_5_early_adverse_r
        elif adaptive_tier == "high_quality":
            max_loss_r_cap = max_loss_r_cap or _num(_strategy6_exit_param(params, "high_quality_loss_cap_R"), 1.05)
            protect_after_mfe_r = protect_after_mfe_r or _num(_strategy6_exit_param(params, "high_quality_protect_after_mfe_R"), 0.85)
            trail_after_mfe_r = trail_after_mfe_r or _num(_strategy6_exit_param(params, "high_quality_trail_after_mfe_R"), 0.65)
            abort_if_mfe_lt_r = abort_if_mfe_lt_r or _num(_strategy6_exit_param(params, "high_quality_abort_if_mfe_lt_R"), 0.08)
            abort_if_mae_gt_r = abort_if_mae_gt_r or _num(_strategy6_exit_param(params, "high_quality_abort_if_mae_gt_R"), 0.75)
        elif adaptive_tier == "low_quality":
            max_loss_r_cap = max_loss_r_cap or _num(_strategy6_exit_param(params, "low_quality_loss_cap_R"), 0.75)
            first_tp_r = first_tp_r or _num(_strategy6_exit_param(params, "low_quality_first_tp_R"), 0.45)
            protect_after_mfe_r = protect_after_mfe_r or _num(_strategy6_exit_param(params, "low_quality_protect_after_mfe_R"), 0.40)
            trail_after_mfe_r = trail_after_mfe_r or _num(_strategy6_exit_param(params, "low_quality_trail_after_mfe_R"), 0.30)
            abort_if_mfe_lt_r = abort_if_mfe_lt_r or _num(_strategy6_exit_param(params, "low_quality_abort_if_mfe_lt_R"), 0.12)
            abort_if_mae_gt_r = abort_if_mae_gt_r or _num(_strategy6_exit_param(params, "low_quality_abort_if_mae_gt_R"), 0.45)
        else:
            max_loss_r_cap = max_loss_r_cap or _num(_strategy6_exit_param(params, "medium_quality_loss_cap_R"), 0.95)
            first_tp_r = first_tp_r or _num(_strategy6_exit_param(params, "medium_quality_first_tp_R"), 0.65)
            protect_after_mfe_r = protect_after_mfe_r or _num(_strategy6_exit_param(params, "medium_quality_protect_after_mfe_R"), 0.55)
            trail_after_mfe_r = trail_after_mfe_r or _num(_strategy6_exit_param(params, "medium_quality_trail_after_mfe_R"), 0.45)
            abort_if_mfe_lt_r = abort_if_mfe_lt_r or _num(_strategy6_exit_param(params, "medium_quality_abort_if_mfe_lt_R"), 0.10)
            abort_if_mae_gt_r = abort_if_mae_gt_r or _num(_strategy6_exit_param(params, "medium_quality_abort_if_mae_gt_R"), 0.60)

    cap_stop: float | None = None
    if risk > 0 and max_loss_r_cap > 0:
        gross_cap_r = max(0.0, max_loss_r_cap - cost_r)
        if gross_cap_r < 1.0:
            cap_stop = entry - risk * gross_cap_r if side == "LONG" else entry + risk * gross_cap_r
    first_tp: float | None = None
    if risk > 0 and first_tp_r > 0:
        first_tp = entry + risk * first_tp_r if side == "LONG" else entry - risk * first_tp_r
    lock_floor: float | None = None
    lock_trigger_r = v3_5_lock_trigger_r or protect_after_mfe_r
    if risk > 0 and v3_5_lock_floor_r > 0:
        lock_floor = entry + risk * v3_5_lock_floor_r if side == "LONG" else entry - risk * v3_5_lock_floor_r

    for idx in range(entry_idx, min(len(rows), entry_idx + max_hold)):
        high = _num(rows[idx]["high"])
        low = _num(rows[idx]["low"])
        close = _num(rows[idx]["close"])
        exit_time_ms = int(rows[idx]["open_time_ms"])
        elapsed = idx - entry_idx + 1
        if side == "LONG":
            candle_mfe = max(0.0, (high - entry) / risk) if risk else 0.0
            candle_mae = max(0.0, (entry - low) / risk) if risk else 0.0
            close_favorable = max(0.0, (close - entry) / risk) if risk else 0.0
            hit_stop = low <= stop
            hit_tp = high >= target
            hit_cap = cap_stop is not None and low <= cap_stop
            hit_first = first_tp is not None and high >= first_tp
        else:
            candle_mfe = max(0.0, (entry - low) / risk) if risk else 0.0
            candle_mae = max(0.0, (high - entry) / risk) if risk else 0.0
            close_favorable = max(0.0, (entry - close) / risk) if risk else 0.0
            hit_stop = high >= stop
            hit_tp = low <= target
            hit_cap = cap_stop is not None and high >= cap_stop
            hit_first = first_tp is not None and low <= first_tp

        max_mfe_r = max(max_mfe_r, candle_mfe)
        max_mae_r = max(max_mae_r, candle_mae)

        if hit_cap and (hit_tp or hit_first):
            exit_price = float(cap_stop)
            exit_reason = "strategy6_loss_R_cap_same_candle"
            protection_reason = "max_loss_R_cap"
            break
        if hit_cap:
            exit_price = float(cap_stop)
            exit_reason = "strategy6_loss_R_cap"
            protection_reason = "max_loss_R_cap"
            break
        if hit_stop and (hit_tp or hit_first):
            exit_price = stop
            exit_reason = "SL_same_candle"
            break
        if hit_stop:
            exit_price = stop
            exit_reason = "SL"
            break
        if hit_first:
            exit_price = float(first_tp)
            exit_reason = "strategy6_first_tp"
            protection_reason = "first_tp_R"
            break
        if hit_tp:
            exit_price = target
            exit_reason = "TP"
            break

        if lock_floor is not None and lock_trigger_r > 0 and max_mfe_r >= lock_trigger_r:
            hit_lock_floor = low <= lock_floor if side == "LONG" else high >= lock_floor
            if hit_lock_floor:
                exit_price = float(lock_floor)
                exit_reason = "strategy6_profit_lock_floor"
                protection_reason = "v3_5_profit_lock_floor"
                break

        adverse_bps = _adverse_close_bps(side, entry, close)
        if elapsed <= 1 and adverse_1m_exit_bps > 0 and adverse_bps >= adverse_1m_exit_bps:
            exit_price = close
            exit_reason = "strategy6_adverse_1m_exit"
            protection_reason = "adverse_1m_exit_bps"
            break
        if elapsed <= 3 and adverse_3m_exit_bps > 0 and adverse_bps >= adverse_3m_exit_bps:
            exit_price = close
            exit_reason = "strategy6_adverse_3m_exit"
            protection_reason = "adverse_3m_exit_bps"
            break
        if elapsed <= abort_window_min and abort_if_mfe_lt_r > 0 and abort_if_mae_gt_r > 0:
            if max_mfe_r < abort_if_mfe_lt_r and max_mae_r >= abort_if_mae_gt_r:
                exit_price = close
                exit_reason = "strategy6_direction_wrong_early_abort"
                protection_reason = "direction_wrong_early_abort"
                break
        if elapsed <= abort_window_min and max_initial_adverse_r > 0 and max_mae_r >= max_initial_adverse_r:
            exit_price = close
            exit_reason = "strategy6_initial_adverse_exit"
            protection_reason = "max_initial_adverse_R"
            break
        if elapsed >= abort_window_min and mfe_fail_exit_r > 0 and max_mfe_r < mfe_fail_exit_r:
            exit_price = close
            exit_reason = "strategy6_mfe_fail_exit"
            protection_reason = "mfe_fail_exit_R"
            break
        if protect_after_mfe_r > 0 and trail_after_mfe_r > 0 and max_mfe_r >= protect_after_mfe_r:
            if max_mfe_r - close_favorable >= trail_after_mfe_r:
                exit_price = close
                exit_reason = "strategy6_profit_protect_trail"
                protection_reason = "protect_after_mfe_R"
                break

        exit_price = close

    gross = (exit_price - entry) if side == "LONG" else (entry - exit_price)
    net_r = gross / risk - cost_r if risk else 0.0
    out = dict(order)
    features = dict(out.get("features") or {})
    features.update(
        {
            "strategy6_exit_protection_enabled": True,
            "strategy6_adaptive_exit_enabled": adaptive_enabled,
            "strategy6_adaptive_exit_tier": adaptive_tier if adaptive_enabled else features_in.get("strategy6_adaptive_exit_tier"),
            "strategy6_exit_protection_reason": protection_reason,
            "strategy6_exit_max_MFE_R": round(max_mfe_r, 8),
            "strategy6_exit_max_MAE_R": round(max_mae_r, 8),
        }
    )
    policy = dict(out.get("fast_exit_policy") or {})
    policy["strategy6_exit_protection"] = {
        "enabled": True,
        "adaptive_enabled": adaptive_enabled,
        "adaptive_tier": adaptive_tier if adaptive_enabled else None,
        "reason": protection_reason,
        "max_loss_R_cap": max_loss_r_cap or None,
        "first_tp_R": first_tp_r or None,
        "protect_after_mfe_R": protect_after_mfe_r or None,
        "trail_after_mfe_R": trail_after_mfe_r or None,
        "abort_if_mfe_lt_R": abort_if_mfe_lt_r or None,
        "abort_if_mae_gt_R": abort_if_mae_gt_r or None,
        "abort_window_min": abort_window_min,
        "mfe_fail_exit_R": mfe_fail_exit_r or None,
        "max_initial_adverse_R": max_initial_adverse_r or None,
        "adverse_1m_exit_bps": adverse_1m_exit_bps or None,
        "adverse_3m_exit_bps": adverse_3m_exit_bps or None,
        "v3_5_scratch_after_minutes": v3_5_scratch_after_min or None,
        "v3_5_min_push_R": v3_5_min_push_r or None,
        "v3_5_early_adverse_R": v3_5_early_adverse_r or None,
        "v3_5_lock_trigger_R": v3_5_lock_trigger_r or None,
        "v3_5_lock_floor_R": v3_5_lock_floor_r or None,
    }
    out.update(
        {
            "exit_time_ms": exit_time_ms,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "net_R": round(net_r, 8),
            "features": features,
            "fast_exit_policy": policy,
            "exit_protection_reason": protection_reason,
        }
    )
    return out


def simulate_1m_fill(order: dict[str, Any], rows: list[dict[str, Any]], params: dict[str, Any]) -> dict[str, Any]:
    if _strategy6_exit_protection_enabled(order, params):
        return _simulate_strategy6_exit_protected_fill(order, rows, params)
    max_hold = max(1, int(_num(params.get("max_hold_minutes"), DEFAULT_MAX_HOLD_MINUTES)))
    entry_idx = int(order["entry_idx"])
    side = order["side"]
    stop = _num(order["stop_loss"])
    target = _num(order["take_profit"])
    entry = _num(order["entry_price"])
    exit_price = _num(rows[min(len(rows) - 1, entry_idx)]["close"])
    exit_reason = "time_stop"
    exit_time_ms = int(rows[min(len(rows) - 1, entry_idx)]["open_time_ms"])
    for idx in range(entry_idx, min(len(rows), entry_idx + max_hold)):
        high = _num(rows[idx]["high"])
        low = _num(rows[idx]["low"])
        exit_time_ms = int(rows[idx]["open_time_ms"])
        if side == "LONG":
            hit_stop = low <= stop
            hit_tp = high >= target
        else:
            hit_stop = high >= stop
            hit_tp = low <= target
        if hit_stop and hit_tp:
            exit_price = stop
            exit_reason = "SL_same_candle"
            break
        if hit_stop:
            exit_price = stop
            exit_reason = "SL"
            break
        if hit_tp:
            exit_price = target
            exit_reason = "TP"
            break
        exit_price = _num(rows[idx]["close"])
    risk = abs(entry - stop)
    gross = (exit_price - entry) if side == "LONG" else (entry - exit_price)
    cost_r = (entry * _num(order.get("cost_bps"), 12.0) / 10000) / risk if risk else 0.0
    net_r = gross / risk - cost_r if risk else 0.0
    out = dict(order)
    out.update({"exit_time_ms": exit_time_ms, "exit_price": exit_price, "exit_reason": exit_reason, "net_R": round(net_r, 8)})
    return out


def _metrics(orders: list[dict[str, Any]]) -> dict[str, Any]:
    values = [_num(row.get("net_R")) for row in orders]
    wins = [v for v in values if v > 0]
    losses = [abs(v) for v in values if v < 0]
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    gross_profit = round(sum(wins), 8)
    gross_loss_abs = round(sum(losses), 8)
    return {
        "trade_count": len(values),
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": _ratio(len(wins), len(values)),
        "gross_profit_R": gross_profit,
        "gross_loss_R": round(-gross_loss_abs, 8),
        "profit_factor": round(gross_profit / gross_loss_abs, 8) if gross_loss_abs else (999.0 if gross_profit else None),
        "total_R": round(sum(values), 8),
        "expectancy_R": _avg(values),
        "median_R": _med(values),
        "avg_win_R": _avg(wins),
        "avg_loss_R": _avg(losses),
        "max_drawdown_R": round(max_dd, 8),
    }


def _group_by_day(orders: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for order in orders:
        out[_iso_from_ms(int(order["entry_time_ms"]))[:10]].append(order)
    return out


def _group_by_symbol(orders: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for order in orders:
        out[str(order["symbol"]).upper()].append(order)
    return out


def _orders_from_db(conn: sqlite3.Connection, *, experiment_id: str, parameter_set_id: str) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT symbol, entry_time_ms, exit_time_ms, net_R, exit_reason
            FROM p21_v2_shadow_orders
            WHERE experiment_id = ? AND parameter_set_id = ?
            ORDER BY entry_time_ms ASC, order_id ASC
            """,
            (experiment_id, parameter_set_id),
        ).fetchall()
    ]
    return rows


def _insert_shadow_orders_batch(
    conn: sqlite3.Connection,
    *,
    experiment_id: str,
    parameter_set_id: str,
    strategy_line: str,
    parameters: dict[str, Any],
    orders: list[dict[str, Any]],
    generated_at: str,
) -> None:
    for order in orders:
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
                order["order_id"],
                experiment_id,
                parameter_set_id,
                strategy_line,
                order["symbol"],
                order["side"],
                order["signal_time_ms"],
                order["entry_time_ms"],
                order.get("exit_time_ms"),
                order["entry_price"],
                order["stop_loss"],
                order["take_profit"],
                order["planned_rr"],
                order.get("net_R"),
                order.get("exit_reason"),
                order.get("score", 0.0),
                _json(order.get("reasons") or []),
                _json(order.get("features") or {}),
                order.get("lineage_mode") or ENGINE_MODE,
                order.get("source_contract_version"),
                _json(order.get("config_patch") or parameters),
                _json(order.get("trade_plan_payload") or {}),
                _json(order.get("fill_result") or {}),
                order.get("entry_mode"),
                order.get("effective_rr"),
                _json(order.get("fast_exit_policy") or {}),
                generated_at,
            ),
        )
        upsert_backtest_order_native(
            conn,
            experiment_id=experiment_id,
            parameter_set_id=parameter_set_id,
            strategy_line=strategy_line,
            parameters=parameters,
            order=order,
            generated_at=generated_at,
        )


def _persist_parameter_metrics_from_orders(
    conn: sqlite3.Connection,
    *,
    experiment_id: str,
    parameter_set_id: str,
    strategy_line: str,
    parameters: dict[str, Any],
    reason_counter: Counter[str],
    symbol_count: int,
    days: int,
    generated_at: str,
) -> dict[str, Any]:
    orders = _orders_from_db(conn, experiment_id=experiment_id, parameter_set_id=parameter_set_id)
    metrics = _metrics(orders)
    metrics.update(
        {
            "accepted_count": len(orders),
            "blocked_count": sum(reason_counter.values()),
            "symbol_count": symbol_count,
            "days": days,
        }
    )
    metric_id = _stable_id("p21v2m", {"e": experiment_id, "p": parameter_set_id}, 24)
    conn.execute(
        """
        INSERT OR REPLACE INTO p21_v2_30d_metrics(
          metric_id, experiment_id, parameter_set_id, strategy_line, metrics_json, parameters_json, generated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (metric_id, experiment_id, parameter_set_id, strategy_line, _json(metrics), _json(parameters), generated_at),
    )
    for day, day_orders in _group_by_day(orders).items():
        conn.execute(
            "INSERT OR REPLACE INTO p21_v2_daily_metrics(metric_id, experiment_id, parameter_set_id, strategy_line, day, metrics_json, generated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
            (_stable_id("p21v2d", {"e": experiment_id, "p": parameter_set_id, "d": day}, 24), experiment_id, parameter_set_id, strategy_line, day, _json(_metrics(day_orders)), generated_at),
        )
    for symbol, symbol_orders in _group_by_symbol(orders).items():
        conn.execute(
            "INSERT OR REPLACE INTO p21_v2_symbol_metrics(metric_id, experiment_id, parameter_set_id, strategy_line, symbol, metrics_json, generated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
            (_stable_id("p21v2s", {"e": experiment_id, "p": parameter_set_id, "s": symbol}, 24), experiment_id, parameter_set_id, strategy_line, symbol, _json(_metrics(symbol_orders)), generated_at),
        )
    return {
        "experiment_id": experiment_id,
        "parameter_set_id": parameter_set_id,
        "strategy_line": strategy_line,
        **_legacy_contract_metadata(),
        "parameters": parameters,
        "metrics": metrics,
        "reasons": dict(reason_counter),
    }


def _symbol_shards(symbols: list[str], shard_size: int) -> Iterable[tuple[int, int, list[str]]]:
    size = max(1, int(shard_size or 25))
    for start in range(0, len(symbols), size):
        end = min(len(symbols), start + size)
        yield start, end, symbols[start:end]


def _evaluate_matrix_shard_worker(payload: dict[str, Any]) -> dict[str, Any]:
    eval_started = time.perf_counter()
    db_path = Path(payload["db_path"])
    shard_symbols = [str(s).upper() for s in payload.get("shard_symbols") or []]
    start_ms = int(payload["start_ms"])
    end_ms = int(payload["end_ms"])
    params = dict(payload.get("params") or {})
    param_line = str(payload["param_line"])
    parameter_set_id = str(payload["parameter_set_id"])
    base_min_score = float(payload.get("base_min_score") or 68.0)
    shard_orders: list[dict[str, Any]] = []
    shard_reasons: Counter[str] = Counter()
    with _connect(db_path) as conn:
        symbol_rows = {sym: _rows_for_symbol(conn, sym, start_ms, end_ms) for sym in shard_symbols}
    for sym, rows in symbol_rows.items():
        signals = build_historical_inputs(rows, symbol=sym, strategy_line=param_line, base_min_score=base_min_score)
        for signal in signals:
            evaluated = evaluate_signal_offline(signal, rows, params)
            if not evaluated.get("executable"):
                shard_reasons.update(evaluated.get("reason_codes") or ["not_executable"])
                continue
            order = dict(evaluated.get("order") or {})
            if not order:
                shard_reasons.update(evaluated.get("reason_codes") or ["missing_order"])
                continue
            filled = simulate_1m_fill(order, rows, params)
            filled["order_id"] = _stable_id("p21v2ord", {"p": parameter_set_id, "s": signal.signal_id}, 24)
            filled["parameter_set_id"] = parameter_set_id
            filled["reasons"] = list(evaluated.get("reason_codes") or [])
            filled["lineage_mode"] = evaluated.get("lineage_mode") or ENGINE_MODE
            filled["source_contract_version"] = evaluated.get("source_contract_version")
            filled["config_patch"] = evaluated.get("config_patch") or params
            filled["trade_plan_payload"] = evaluated.get("trade_plan_payload") or order.get("trade_plan_payload") or {}
            filled["fill_result"] = {
                "exit_time_ms": filled.get("exit_time_ms"),
                "exit_price": filled.get("exit_price"),
                "exit_reason": filled.get("exit_reason"),
                "net_R": filled.get("net_R"),
                "exit_protection_reason": filled.get("exit_protection_reason"),
                "strategy6_exit_protection": (filled.get("fast_exit_policy") or {}).get("strategy6_exit_protection"),
            }
            shard_orders.append(filled)
    return {
        "shard_id": payload["shard_id"],
        "parameter_set_id": parameter_set_id,
        "strategy_line": param_line,
        "symbol_start": payload["symbol_start"],
        "symbol_end": payload["symbol_end"],
        "shard_symbols": shard_symbols,
        "orders": shard_orders,
        "reasons": dict(shard_reasons),
        "worker_id": payload.get("worker_id"),
        "eval_sec": round(time.perf_counter() - eval_started, 6),
    }


def _p95(values: list[float]) -> float | None:
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return None
    index = min(len(clean) - 1, max(0, math.ceil(len(clean) * 0.95) - 1))
    return round(clean[index], 6)


def run_config_matrix_payload(
    project_root: Path,
    *,
    symbols: list[str] | None = None,
    strategy_line: str = "all",
    days: int = DEFAULT_WINDOW_DAYS,
    max_symbols: int = 20,
    max_sets: int = 120,
    write: bool = True,
    parameter_grid: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    selected_symbols = [s.upper() for s in (symbols or universe_symbols(project_root, limit=max_symbols))[:max_symbols]]
    end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=max(1, int(days)))
    start_ms = _ms_from_dt(start_dt)
    end_ms = _ms_from_dt(end_dt)
    generated_at = _now()
    experiment_seed = {"symbols": selected_symbols, "strategy_line": strategy_line, "days": days, "generated_at": generated_at}
    experiment_id = _stable_id("p21v2exp", experiment_seed, 20)
    parameter_sets = parameter_grid or default_parameter_sets(project_root, strategy_line=strategy_line, max_sets=max_sets)
    parameter_sets = parameter_sets[: max(1, min(int(max_sets or 120), 1000))]
    base_config = load_runtime_line_config(project_root, "without_micro")
    base_min_score = _num(base_config.get("min_score"), 68.0)
    lines = list(TARGET_STRATEGY_LINES if strategy_line == "all" else [strategy_line])
    with _connect(db_path) as conn:
        symbol_rows = {sym: _rows_for_symbol(conn, sym, start_ms, end_ms) for sym in selected_symbols}
    signals_by_line_symbol: dict[tuple[str, str], list[HistoricalSignal]] = {}
    for line in lines:
        for sym, rows in symbol_rows.items():
            signals_by_line_symbol[(line, sym)] = build_historical_inputs(rows, symbol=sym, strategy_line=line, base_min_score=base_min_score)
    leaderboard: list[dict[str, Any]] = []
    orders_by_param: dict[str, list[dict[str, Any]]] = {}
    reasons_by_param: dict[str, Counter[str]] = {}
    for item in parameter_sets:
        params = dict(item.get("parameters") or item)
        param_line = params.get("strategy_line") or "without_micro"
        if param_line not in lines:
            continue
        parameter_set_id = item.get("parameter_set_id") or _stable_id("p21v2", params, 16)
        orders: list[dict[str, Any]] = []
        reason_counter: Counter[str] = Counter()
        for sym in selected_symbols:
            rows = symbol_rows.get(sym) or []
            for signal in signals_by_line_symbol.get((param_line, sym), []):
                evaluated = evaluate_signal_offline(signal, rows, params)
                if not evaluated.get("executable"):
                    reason_counter.update(evaluated.get("reason_codes") or ["not_executable"])
                    continue
                order = dict(evaluated.get("order") or {})
                if not order:
                    reason_counter.update(evaluated.get("reason_codes") or ["missing_order"])
                    continue
                filled = simulate_1m_fill(order, rows, params)
                filled["order_id"] = _stable_id("p21v2ord", {"p": parameter_set_id, "s": signal.signal_id}, 24)
                filled["parameter_set_id"] = parameter_set_id
                filled["reasons"] = list(evaluated.get("reason_codes") or [])
                filled["lineage_mode"] = evaluated.get("lineage_mode") or ENGINE_MODE
                filled["source_contract_version"] = evaluated.get("source_contract_version")
                filled["config_patch"] = evaluated.get("config_patch") or params
                filled["trade_plan_payload"] = evaluated.get("trade_plan_payload") or order.get("trade_plan_payload") or {}
                filled["fill_result"] = {
                    "exit_time_ms": filled.get("exit_time_ms"),
                    "exit_price": filled.get("exit_price"),
                    "exit_reason": filled.get("exit_reason"),
                    "net_R": filled.get("net_R"),
                    "exit_protection_reason": filled.get("exit_protection_reason"),
                    "strategy6_exit_protection": (filled.get("fast_exit_policy") or {}).get("strategy6_exit_protection"),
                }
                orders.append(filled)
        metrics = _metrics(orders)
        metrics.update(
            {
                "accepted_count": len(orders),
                "blocked_count": sum(reason_counter.values()),
                "symbol_count": len(selected_symbols),
                "days": days,
            }
        )
        leaderboard.append(
            {
                "experiment_id": experiment_id,
                "parameter_set_id": parameter_set_id,
                "strategy_line": param_line,
                **_legacy_contract_metadata(),
                "parameters": params,
                "metrics": metrics,
                "reasons": dict(reason_counter),
            }
        )
        orders_by_param[parameter_set_id] = orders
        reasons_by_param[parameter_set_id] = reason_counter
    leaderboard.sort(
        key=lambda item: (
            item["metrics"].get("profit_factor") if item["metrics"].get("profit_factor") is not None else -999,
            item["metrics"].get("expectancy_R") or -999,
            item["metrics"].get("trade_count") or 0,
            -item["metrics"].get("max_drawdown_R", 0),
        ),
        reverse=True,
    )
    best = leaderboard[0] if leaderboard else None
    recommendations = _recommendations(experiment_id, leaderboard)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "engine_mode": ENGINE_MODE,
        **_legacy_contract_metadata(),
        "experiment_id": experiment_id,
        "strategy_line": strategy_line,
        "symbols": selected_symbols,
        "start_time": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_time": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": days,
        "parameter_set_count": len(leaderboard),
        "trade_count": sum(item["metrics"].get("trade_count") or 0 for item in leaderboard),
        "leaderboard": leaderboard[:100],
        "best": best,
        "recommendations": recommendations,
        "generated_at": generated_at,
    }
    if write:
        _persist_experiment(project_root, payload, orders_by_param)
    return payload


def _run_config_matrix_global_queue_payload(
    project_root: Path,
    *,
    symbols: list[str] | None = None,
    strategy_line: str = "all",
    days: int = DEFAULT_WINDOW_DAYS,
    max_symbols: int = 20,
    max_sets: int = 120,
    write: bool = True,
    parameter_grid: list[dict[str, Any]] | None = None,
    symbol_shard_size: int = 25,
    job_id: str | None = None,
    resume_experiment_id: str | None = None,
    max_workers: int = 1,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    selected_symbols = [s.upper() for s in (symbols or universe_symbols(project_root, limit=max_symbols))[:max_symbols]]
    end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=max(1, int(days)))
    start_ms = _ms_from_dt(start_dt)
    end_ms = _ms_from_dt(end_dt)
    generated_at = _now()
    experiment_seed = {
        "symbols": selected_symbols,
        "strategy_line": strategy_line,
        "days": days,
        "generated_at": generated_at,
        "mode": "global_queue",
        "job_id": job_id,
    }
    experiment_id = str(resume_experiment_id or "").strip() or _stable_id("p21v2exp", experiment_seed, 20)
    worker_count = max(1, min(int(max_workers or 1), max(1, (os.cpu_count() or 2))))
    parameter_sets = parameter_grid or default_parameter_sets(project_root, strategy_line=strategy_line, max_sets=max_sets)
    parameter_sets = parameter_sets[: max(1, min(int(max_sets or 120), 5000))]
    base_config = load_runtime_line_config(project_root, "without_micro")
    base_min_score = _num(base_config.get("min_score"), 68.0)
    lines = list(TARGET_STRATEGY_LINES if strategy_line == "all" else [strategy_line])
    shards = list(_symbol_shards(selected_symbols, symbol_shard_size))

    filtered_sets: list[tuple[int, str, dict[str, Any], str]] = []
    for param_index, item in enumerate(parameter_sets, start=1):
        params = dict(item.get("parameters") or item)
        param_line = params.get("strategy_line") or "without_micro"
        if param_line in lines:
            filtered_sets.append((param_index, item.get("parameter_set_id") or _stable_id("p21v2", params, 16), params, param_line))

    total_shards = len(filtered_sets) * max(1, len(shards))
    done_shards = 0
    tasks: list[dict[str, Any]] = []
    reason_by_param: dict[str, Counter[str]] = {parameter_set_id: Counter() for _, parameter_set_id, _, _ in filtered_sets}
    meta_by_param: dict[str, tuple[dict[str, Any], str]] = {
        parameter_set_id: (params, param_line) for _, parameter_set_id, params, param_line in filtered_sets
    }
    eval_secs: list[float] = []
    write_secs: list[float] = []

    if write:
        with _connect(db_path) as conn:
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
                    strategy_line,
                    _json(selected_symbols),
                    start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    days,
                    len(filtered_sets),
                    0,
                    None,
                    None,
                    None,
                    "running",
                    SCHEMA_VERSION,
                    generated_at,
                ),
            )
            if resume_experiment_id:
                conn.execute(
                    """
                    UPDATE p21_v2_matrix_shards
                    SET status = ?, error = ?, finished_at = COALESCE(finished_at, ?)
                    WHERE experiment_id = ? AND status = ? AND COALESCE(job_id, '') != ?
                    """,
                    (
                        "stale_interrupted",
                        "adopted_by_global_queue_resume_before_recompute",
                        _now(),
                        experiment_id,
                        "running",
                        job_id or "",
                    ),
                )

    for param_position, (param_index, parameter_set_id, params, param_line) in enumerate(filtered_sets, start=1):
        if write:
            with _connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO p21_v2_parameter_sets(
                      parameter_set_id, experiment_id, strategy_line, parameters_json, generated_at
                    ) VALUES(?, ?, ?, ?, ?)
                    """,
                    (parameter_set_id, experiment_id, param_line, _json(params), generated_at),
                )
        for shard_index, (symbol_start, symbol_end, shard_symbols) in enumerate(shards, start=1):
            shard_id = _stable_id(
                "p21v2shard",
                {
                    "e": experiment_id,
                    "p": parameter_set_id,
                    "start": symbol_start,
                    "end": symbol_end,
                    "symbols": shard_symbols,
                },
                24,
            )
            if write:
                with _connect(db_path) as conn:
                    existing = conn.execute(
                        "SELECT status, metrics_json FROM p21_v2_matrix_shards WHERE shard_id = ?",
                        (shard_id,),
                    ).fetchone()
                    if existing and existing[0] == "completed":
                        metrics = _loads(existing[1], {})
                        reason_by_param[parameter_set_id].update(metrics.get("reasons") or {})
                        done_shards += 1
                        if progress_callback:
                            progress_callback(
                                {
                                    "phase": "matrix",
                                    "done_count": done_shards,
                                    "total_count": total_shards,
                                    "current_strategy_line": param_line,
                                    "current_parameter_set_id": parameter_set_id,
                                    "current_symbol": shard_symbols[-1] if shard_symbols else None,
                                    "current_symbol_shard": f"{symbol_start}:{symbol_end}",
                                    "parameter_set_index": param_position,
                                    "parameter_set_total": len(filtered_sets),
                                    "symbol_shard_index": shard_index,
                                    "symbol_shard_total": len(shards),
                                    "persisted_order_count": metrics.get("order_count") or 0,
                                    "execution_mode": "sharded_global_queue",
                                    "memory_guard_status": "global_queue_single_writer",
                                    "max_workers": worker_count,
                                    "active_workers": 0,
                                    "idle_workers": worker_count,
                                    "writer_queue_size": 0,
                                    "last_checkpoint_at": _now(),
                                }
                            )
                        continue
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO p21_v2_matrix_shards(
                          shard_id, job_id, experiment_id, strategy_line, parameter_set_id,
                          symbol_start, symbol_end, symbols_json, status, order_count,
                          metrics_json, error, started_at, finished_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            shard_id,
                            job_id,
                            experiment_id,
                            param_line,
                            parameter_set_id,
                            symbol_start,
                            symbol_end,
                            _json(shard_symbols),
                            "running",
                            0,
                            _json({"worker_id": None, "execution_mode": "sharded_global_queue"}),
                            None,
                            _now(),
                            None,
                        ),
                    )
            tasks.append(
                {
                    "db_path": str(db_path),
                    "shard_id": shard_id,
                    "parameter_set_id": parameter_set_id,
                    "param_line": param_line,
                    "params": params,
                    "symbol_start": symbol_start,
                    "symbol_end": symbol_end,
                    "shard_symbols": shard_symbols,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "base_min_score": base_min_score,
                    "worker_id": f"w{(len(tasks) % worker_count) + 1}",
                    "parameter_set_index": param_position,
                    "parameter_set_total": len(filtered_sets),
                    "shard_index": shard_index,
                    "shard_total": len(shards),
                }
            )

    orders_by_param: dict[str, list[dict[str, Any]]] = defaultdict(list)

    def _write_result(result: dict[str, Any], task: dict[str, Any], pending: int) -> None:
        nonlocal done_shards
        started = time.perf_counter()
        parameter_set_id = str(result["parameter_set_id"])
        params, param_line = meta_by_param[parameter_set_id]
        orders = list(result.get("orders") or [])
        shard_reasons = Counter(result.get("reasons") or {})
        reason_by_param[parameter_set_id].update(shard_reasons)
        eval_sec = _num(result.get("eval_sec"), 0.0)
        if eval_sec:
            eval_secs.append(eval_sec)
        shard_metrics = {
            "order_count": len(orders),
            "reasons": dict(shard_reasons),
            "worker_id": result.get("worker_id"),
            "execution_mode": "sharded_global_queue",
            "eval_sec": eval_sec,
        }
        if write:
            with _connect(db_path) as conn:
                _insert_shadow_orders_batch(
                    conn,
                    experiment_id=experiment_id,
                    parameter_set_id=parameter_set_id,
                    strategy_line=param_line,
                    parameters=params,
                    orders=orders,
                    generated_at=generated_at,
                )
                conn.execute(
                    """
                    UPDATE p21_v2_matrix_shards
                    SET status = ?, order_count = ?, metrics_json = ?, error = NULL, finished_at = ?
                    WHERE shard_id = ?
                    """,
                    ("completed", len(orders), _json(shard_metrics), _now(), result["shard_id"]),
                )
        else:
            orders_by_param[parameter_set_id].extend(orders)
        write_sec = time.perf_counter() - started
        write_secs.append(write_sec)
        done_shards += 1
        if progress_callback:
            active = min(worker_count, max(0, pending))
            progress_callback(
                {
                    "phase": "matrix",
                    "done_count": done_shards,
                    "total_count": total_shards,
                    "current_strategy_line": param_line,
                    "current_parameter_set_id": parameter_set_id,
                    "current_symbol": (result.get("shard_symbols") or [None])[-1],
                    "current_symbol_shard": f"{result.get('symbol_start')}:{result.get('symbol_end')}",
                    "parameter_set_index": task.get("parameter_set_index"),
                    "parameter_set_total": len(filtered_sets),
                    "symbol_shard_index": task.get("shard_index"),
                    "symbol_shard_total": len(shards),
                    "persisted_order_count": len(orders),
                    "execution_mode": "sharded_global_queue",
                    "memory_guard_status": "global_queue_single_writer",
                    "max_workers": worker_count,
                    "active_workers": active,
                    "idle_workers": max(0, worker_count - active),
                    "writer_queue_size": 0,
                    "avg_shard_sec": round(sum(eval_secs) / len(eval_secs), 6) if eval_secs else None,
                    "p95_shard_sec": _p95(eval_secs),
                    "sqlite_write_sec": round(write_sec, 6),
                    "eval_sec": eval_sec,
                    "last_checkpoint_at": _now(),
                }
            )
        del orders

    if tasks:
        if worker_count <= 1 or len(tasks) <= 1:
            for task in tasks:
                task["worker_id"] = "w1"
                try:
                    _write_result(_evaluate_matrix_shard_worker(task), task, pending=len(tasks) - done_shards - 1)
                except Exception as exc:
                    if write:
                        with _connect(db_path) as conn:
                            conn.execute(
                                "UPDATE p21_v2_matrix_shards SET status = ?, error = ?, finished_at = ? WHERE shard_id = ?",
                                ("failed", str(exc), _now(), task["shard_id"]),
                            )
                    raise
        else:
            with ProcessPoolExecutor(max_workers=worker_count) as pool:
                future_to_task = {pool.submit(_evaluate_matrix_shard_worker, task): task for task in tasks}
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        _write_result(future.result(), task, pending=len(future_to_task) - done_shards - 1)
                    except Exception as exc:
                        if write:
                            with _connect(db_path) as conn:
                                conn.execute(
                                    "UPDATE p21_v2_matrix_shards SET status = ?, error = ?, finished_at = ? WHERE shard_id = ?",
                                    ("failed", str(exc), _now(), task["shard_id"]),
                                )
                        raise

    leaderboard: list[dict[str, Any]] = []
    if write:
        with _connect(db_path) as conn:
            for parameter_set_id, (params, param_line) in meta_by_param.items():
                leaderboard.append(
                    _persist_parameter_metrics_from_orders(
                        conn,
                        experiment_id=experiment_id,
                        parameter_set_id=parameter_set_id,
                        strategy_line=param_line,
                        parameters=params,
                        reason_counter=reason_by_param[parameter_set_id],
                        symbol_count=len(selected_symbols),
                        days=days,
                        generated_at=generated_at,
                    )
                )
    else:
        for parameter_set_id, (params, param_line) in meta_by_param.items():
            orders = orders_by_param.get(parameter_set_id) or []
            metrics = _metrics(orders)
            metrics.update(
                {
                    "accepted_count": len(orders),
                    "blocked_count": sum(reason_by_param[parameter_set_id].values()),
                    "symbol_count": len(selected_symbols),
                    "days": days,
                }
            )
            leaderboard.append(
                {
                    "experiment_id": experiment_id,
                    "parameter_set_id": parameter_set_id,
                    "strategy_line": param_line,
                    **_legacy_contract_metadata(),
                    "parameters": params,
                    "metrics": metrics,
                    "reasons": dict(reason_by_param[parameter_set_id]),
                }
            )

    leaderboard.sort(
        key=lambda item: (
            item["metrics"].get("profit_factor") if item["metrics"].get("profit_factor") is not None else -999,
            item["metrics"].get("expectancy_R") or -999,
            item["metrics"].get("trade_count") or 0,
            -item["metrics"].get("max_drawdown_R", 0),
        ),
        reverse=True,
    )
    best = leaderboard[0] if leaderboard else None
    recommendations = _recommendations(experiment_id, leaderboard)
    total_trades = sum(item["metrics"].get("trade_count") or 0 for item in leaderboard)
    if write:
        with _connect(db_path) as conn:
            for item in recommendations:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO p21_v2_recommendations(
                      recommendation_id, experiment_id, parameter_set_id, status, priority, summary,
                      metrics_json, parameters_json, risks_json, generated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["recommendation_id"],
                        item["experiment_id"],
                        item["parameter_set_id"],
                        item["status"],
                        item["priority"],
                        item["summary"],
                        _json(item["metrics"]),
                        _json(item["parameters"]),
                        _json(item["risks"]),
                        generated_at,
                    ),
                )
            conn.execute(
                """
                UPDATE p21_v2_experiments
                SET parameter_set_count = ?, trade_count = ?, best_parameter_set_id = ?,
                    best_profit_factor = ?, best_expectancy_R = ?, status = ?
                WHERE experiment_id = ?
                """,
                (
                    len(leaderboard),
                    total_trades,
                    (best or {}).get("parameter_set_id"),
                    ((best or {}).get("metrics") or {}).get("profit_factor"),
                    ((best or {}).get("metrics") or {}).get("expectancy_R"),
                    "completed",
                    experiment_id,
                ),
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "engine_mode": ENGINE_MODE,
        **_legacy_contract_metadata(),
        "execution_mode": "sharded_global_queue",
        "memory_guard_status": "global_queue_single_writer",
        "experiment_id": experiment_id,
        "strategy_line": strategy_line,
        "symbols": selected_symbols,
        "start_time": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_time": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": days,
        "parameter_set_count": len(leaderboard),
        "trade_count": total_trades,
        "leaderboard": leaderboard[:100],
        "best": best,
        "recommendations": recommendations,
        "symbol_shard_size": max(1, int(symbol_shard_size or 25)),
        "max_workers": worker_count,
        "resume_experiment_id": resume_experiment_id,
        "shard_count": total_shards,
        "avg_shard_sec": round(sum(eval_secs) / len(eval_secs), 6) if eval_secs else None,
        "p95_shard_sec": _p95(eval_secs),
        "sqlite_write_sec": round(sum(write_secs), 6) if write_secs else 0.0,
        "writer_queue_size": 0,
        "generated_at": generated_at,
    }


def run_config_matrix_streaming_payload(
    project_root: Path,
    *,
    symbols: list[str] | None = None,
    strategy_line: str = "all",
    days: int = DEFAULT_WINDOW_DAYS,
    max_symbols: int = 20,
    max_sets: int = 120,
    write: bool = True,
    parameter_grid: list[dict[str, Any]] | None = None,
    symbol_shard_size: int = 25,
    job_id: str | None = None,
    resume_experiment_id: str | None = None,
    max_workers: int = 1,
    scheduler_mode: str = "parameter_batch",
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    mode = str(scheduler_mode or "parameter_batch").strip().lower()
    if mode in {"global_queue", "async_writer", "sharded_global_queue"}:
        return _run_config_matrix_global_queue_payload(
            project_root,
            symbols=symbols,
            strategy_line=strategy_line,
            days=days,
            max_symbols=max_symbols,
            max_sets=max_sets,
            write=write,
            parameter_grid=parameter_grid,
            symbol_shard_size=symbol_shard_size,
            job_id=job_id,
            resume_experiment_id=resume_experiment_id,
            max_workers=max_workers,
            progress_callback=progress_callback,
        )
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    selected_symbols = [s.upper() for s in (symbols or universe_symbols(project_root, limit=max_symbols))[:max_symbols]]
    end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=max(1, int(days)))
    start_ms = _ms_from_dt(start_dt)
    end_ms = _ms_from_dt(end_dt)
    generated_at = _now()
    experiment_seed = {
        "symbols": selected_symbols,
        "strategy_line": strategy_line,
        "days": days,
        "generated_at": generated_at,
        "mode": "streaming",
        "job_id": job_id,
    }
    experiment_id = str(resume_experiment_id or "").strip() or _stable_id("p21v2exp", experiment_seed, 20)
    worker_count = max(1, min(int(max_workers or 1), max(1, (os.cpu_count() or 2))))
    parameter_sets = parameter_grid or default_parameter_sets(project_root, strategy_line=strategy_line, max_sets=max_sets)
    parameter_sets = parameter_sets[: max(1, min(int(max_sets or 120), 5000))]
    base_config = load_runtime_line_config(project_root, "without_micro")
    base_min_score = _num(base_config.get("min_score"), 68.0)
    lines = list(TARGET_STRATEGY_LINES if strategy_line == "all" else [strategy_line])
    shards = list(_symbol_shards(selected_symbols, symbol_shard_size))
    filtered_sets: list[tuple[str, dict[str, Any], str]] = []
    for item in parameter_sets:
        params = dict(item.get("parameters") or item)
        param_line = params.get("strategy_line") or "without_micro"
        if param_line in lines:
            filtered_sets.append((item.get("parameter_set_id") or _stable_id("p21v2", params, 16), params, param_line))
    total_shards = len(filtered_sets) * max(1, len(shards))
    done_shards = 0

    if write:
        with _connect(db_path) as conn:
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
                    strategy_line,
                    _json(selected_symbols),
                    start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    days,
                    len(filtered_sets),
                    0,
                    None,
                    None,
                    None,
                    "running",
                    SCHEMA_VERSION,
                    generated_at,
                ),
            )
            if resume_experiment_id:
                conn.execute(
                    """
                    UPDATE p21_v2_matrix_shards
                    SET status = ?, error = ?, finished_at = COALESCE(finished_at, ?)
                    WHERE experiment_id = ? AND status = ? AND COALESCE(job_id, '') != ?
                    """,
                    (
                        "stale_interrupted",
                        "adopted_by_resume_runner_before_recompute",
                        _now(),
                        experiment_id,
                        "running",
                        job_id or "",
                    ),
                )

    leaderboard: list[dict[str, Any]] = []
    for param_index, (parameter_set_id, params, param_line) in enumerate(filtered_sets, start=1):
        reason_counter: Counter[str] = Counter()
        if write:
            with _connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO p21_v2_parameter_sets(
                      parameter_set_id, experiment_id, strategy_line, parameters_json, generated_at
                    ) VALUES(?, ?, ?, ?, ?)
                    """,
                    (parameter_set_id, experiment_id, param_line, _json(params), generated_at),
                )
        shard_tasks: list[dict[str, Any]] = []
        for shard_index, (symbol_start, symbol_end, shard_symbols) in enumerate(shards, start=1):
            shard_id = _stable_id(
                "p21v2shard",
                {
                    "e": experiment_id,
                    "p": parameter_set_id,
                    "start": symbol_start,
                    "end": symbol_end,
                    "symbols": shard_symbols,
                },
                24,
            )
            started_at = _now()
            if write:
                with _connect(db_path) as conn:
                    existing = conn.execute("SELECT status, metrics_json FROM p21_v2_matrix_shards WHERE shard_id = ?", (shard_id,)).fetchone()
                    if existing and existing[0] == "completed":
                        metrics = _loads(existing[1], {})
                        reason_counter.update(metrics.get("reasons") or {})
                        done_shards += 1
                        if progress_callback:
                            progress_callback(
                                {
                                    "phase": "matrix",
                                    "done_count": done_shards,
                                    "total_count": total_shards,
                                    "current_strategy_line": param_line,
                                    "current_parameter_set_id": parameter_set_id,
                                    "current_symbol": shard_symbols[-1] if shard_symbols else None,
                                    "current_symbol_shard": f"{symbol_start}:{symbol_end}",
                                    "parameter_set_index": param_index,
                                    "parameter_set_total": len(filtered_sets),
                                    "symbol_shard_index": shard_index,
                                    "symbol_shard_total": len(shards),
                                    "persisted_order_count": metrics.get("order_count") or 0,
                                    "memory_guard_status": "streaming_parallel" if worker_count > 1 else "streaming",
                                    "max_workers": worker_count,
                                    "active_workers": 0,
                                    "last_checkpoint_at": _now(),
                                }
                            )
                        continue
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO p21_v2_matrix_shards(
                          shard_id, job_id, experiment_id, strategy_line, parameter_set_id,
                          symbol_start, symbol_end, symbols_json, status, order_count,
                          metrics_json, error, started_at, finished_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (shard_id, job_id, experiment_id, param_line, parameter_set_id, symbol_start, symbol_end, _json(shard_symbols), "running", 0, _json({"worker_id": None}), None, started_at, None),
                    )
            shard_tasks.append(
                {
                    "db_path": str(db_path),
                    "shard_id": shard_id,
                    "parameter_set_id": parameter_set_id,
                    "param_line": param_line,
                    "params": params,
                    "symbol_start": symbol_start,
                    "symbol_end": symbol_end,
                    "shard_symbols": shard_symbols,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "base_min_score": base_min_score,
                    "worker_id": f"w{((len(shard_tasks)) % worker_count) + 1}",
                    "shard_index": shard_index,
                    "shard_total": len(shards),
                }
            )

        def _persist_shard_result(result: dict[str, Any]) -> None:
            nonlocal done_shards
            orders = list(result.get("orders") or [])
            shard_reasons = Counter(result.get("reasons") or {})
            reason_counter.update(shard_reasons)
            shard_metrics = {"order_count": len(orders), "reasons": dict(shard_reasons), "worker_id": result.get("worker_id")}
            if write:
                with _connect(db_path) as conn:
                    _insert_shadow_orders_batch(
                        conn,
                        experiment_id=experiment_id,
                        parameter_set_id=parameter_set_id,
                        strategy_line=param_line,
                        parameters=params,
                        orders=orders,
                        generated_at=generated_at,
                    )
                    conn.execute(
                        """
                        UPDATE p21_v2_matrix_shards
                        SET status = ?, order_count = ?, metrics_json = ?, error = NULL, finished_at = ?
                        WHERE shard_id = ?
                        """,
                        ("completed", len(orders), _json(shard_metrics), _now(), result["shard_id"]),
                    )
            done_shards += 1
            if progress_callback:
                progress_callback(
                    {
                        "phase": "matrix",
                        "done_count": done_shards,
                        "total_count": total_shards,
                        "current_strategy_line": param_line,
                        "current_parameter_set_id": parameter_set_id,
                        "current_symbol": (result.get("shard_symbols") or [None])[-1],
                        "current_symbol_shard": f"{result.get('symbol_start')}:{result.get('symbol_end')}",
                        "parameter_set_index": param_index,
                        "parameter_set_total": len(filtered_sets),
                        "symbol_shard_index": min(len(shards), int(result.get("shard_index") or done_shards)),
                        "symbol_shard_total": len(shards),
                        "persisted_order_count": len(orders),
                        "memory_guard_status": "streaming_parallel" if worker_count > 1 else "streaming",
                        "max_workers": worker_count,
                        "active_workers": 0,
                        "last_checkpoint_at": _now(),
                    }
                )
            del orders

        if worker_count <= 1 or len(shard_tasks) <= 1:
            for task in shard_tasks:
                task["worker_id"] = "w1"
                try:
                    result = _evaluate_matrix_shard_worker(task)
                    result["shard_index"] = task.get("shard_index")
                    _persist_shard_result(result)
                except Exception as exc:
                    if write:
                        with _connect(db_path) as conn:
                            conn.execute(
                                "UPDATE p21_v2_matrix_shards SET status = ?, error = ?, finished_at = ? WHERE shard_id = ?",
                                ("failed", str(exc), _now(), task["shard_id"]),
                            )
                    raise
        else:
            with ProcessPoolExecutor(max_workers=worker_count) as pool:
                future_to_task = {pool.submit(_evaluate_matrix_shard_worker, task): task for task in shard_tasks}
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result()
                        result["shard_index"] = task.get("shard_index")
                        _persist_shard_result(result)
                    except Exception as exc:
                        if write:
                            with _connect(db_path) as conn:
                                conn.execute(
                                    "UPDATE p21_v2_matrix_shards SET status = ?, error = ?, finished_at = ? WHERE shard_id = ?",
                                    ("failed", str(exc), _now(), task["shard_id"]),
                                )
                        raise
        if write:
            with _connect(db_path) as conn:
                item = _persist_parameter_metrics_from_orders(
                    conn,
                    experiment_id=experiment_id,
                    parameter_set_id=parameter_set_id,
                    strategy_line=param_line,
                    parameters=params,
                    reason_counter=reason_counter,
                    symbol_count=len(selected_symbols),
                    days=days,
                    generated_at=generated_at,
                )
        else:
            item = {
                "experiment_id": experiment_id,
                "parameter_set_id": parameter_set_id,
                "strategy_line": param_line,
                **_legacy_contract_metadata(),
                "parameters": params,
                "metrics": {"trade_count": 0, "profit_factor": None, "expectancy_R": 0.0},
                "reasons": dict(reason_counter),
            }
        leaderboard.append(item)

    leaderboard.sort(
        key=lambda item: (
            item["metrics"].get("profit_factor") if item["metrics"].get("profit_factor") is not None else -999,
            item["metrics"].get("expectancy_R") or -999,
            item["metrics"].get("trade_count") or 0,
            -item["metrics"].get("max_drawdown_R", 0),
        ),
        reverse=True,
    )
    best = leaderboard[0] if leaderboard else None
    recommendations = _recommendations(experiment_id, leaderboard)
    total_trades = sum(item["metrics"].get("trade_count") or 0 for item in leaderboard)
    if write:
        with _connect(db_path) as conn:
            for item in recommendations:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO p21_v2_recommendations(
                      recommendation_id, experiment_id, parameter_set_id, status, priority, summary,
                      metrics_json, parameters_json, risks_json, generated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["recommendation_id"],
                        item["experiment_id"],
                        item["parameter_set_id"],
                        item["status"],
                        item["priority"],
                        item["summary"],
                        _json(item["metrics"]),
                        _json(item["parameters"]),
                        _json(item["risks"]),
                        generated_at,
                    ),
                )
            conn.execute(
                """
                UPDATE p21_v2_experiments
                SET parameter_set_count = ?, trade_count = ?, best_parameter_set_id = ?,
                    best_profit_factor = ?, best_expectancy_R = ?, status = ?
                WHERE experiment_id = ?
                """,
                (
                    len(leaderboard),
                    total_trades,
                    (best or {}).get("parameter_set_id"),
                    ((best or {}).get("metrics") or {}).get("profit_factor"),
                    ((best or {}).get("metrics") or {}).get("expectancy_R"),
                    "completed",
                    experiment_id,
                ),
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "engine_mode": ENGINE_MODE,
        **_legacy_contract_metadata(),
        "execution_mode": "sharded_streaming",
        "experiment_id": experiment_id,
        "strategy_line": strategy_line,
        "symbols": selected_symbols,
        "start_time": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_time": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": days,
        "parameter_set_count": len(leaderboard),
        "trade_count": total_trades,
        "leaderboard": leaderboard[:100],
        "best": best,
        "recommendations": recommendations,
        "symbol_shard_size": max(1, int(symbol_shard_size or 25)),
        "max_workers": worker_count,
        "resume_experiment_id": resume_experiment_id,
        "shard_count": total_shards,
        "generated_at": generated_at,
    }


def _recommendations(experiment_id: str, leaderboard: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(leaderboard[:20], start=1):
        metrics = item.get("metrics") or {}
        pf = metrics.get("profit_factor")
        risks: list[str] = []
        if (metrics.get("trade_count") or 0) < 20:
            risks.append("small_trade_count")
        if (metrics.get("max_drawdown_R") or 0) > 10:
            risks.append("large_drawdown")
        risks.append(RERUN_REQUIRED_BLOCK_REASON)
        status = "candidate_pf_gt_1" if pf and pf > 1 else "watch_only"
        out.append(
            {
                "recommendation_id": _stable_id("p21v2rec", {"e": experiment_id, "p": item["parameter_set_id"], "i": idx}, 20),
                "experiment_id": experiment_id,
                "parameter_set_id": item["parameter_set_id"],
                **_legacy_contract_metadata(),
                "status": status,
                "priority": idx,
                "summary": f"{item['strategy_line']} PF={pf} expectancy={metrics.get('expectancy_R')} trades={metrics.get('trade_count')}",
                "metrics": metrics,
                "parameters": item.get("parameters") or {},
                "risks": risks,
            }
        )
    return out


def _persist_experiment(project_root: Path, payload: dict[str, Any], orders_by_param: dict[str, list[dict[str, Any]]]) -> None:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    best = payload.get("best") or {}
    generated_at = payload["generated_at"]
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO p21_v2_experiments(
              experiment_id, strategy_line, symbols_json, start_time, end_time, days,
              parameter_set_count, trade_count, best_parameter_set_id, best_profit_factor,
              best_expectancy_R, status, schema_version, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["experiment_id"],
                payload["strategy_line"],
                _json(payload["symbols"]),
                payload["start_time"],
                payload["end_time"],
                payload["days"],
                payload["parameter_set_count"],
                payload["trade_count"],
                best.get("parameter_set_id"),
                (best.get("metrics") or {}).get("profit_factor"),
                (best.get("metrics") or {}).get("expectancy_R"),
                "completed",
                SCHEMA_VERSION,
                generated_at,
            ),
        )
        for item in payload.get("leaderboard") or []:
            parameter_set_id = item["parameter_set_id"]
            conn.execute(
                """
                INSERT OR REPLACE INTO p21_v2_parameter_sets(
                  parameter_set_id, experiment_id, strategy_line, parameters_json, generated_at
                ) VALUES(?, ?, ?, ?, ?)
                """,
                (parameter_set_id, payload["experiment_id"], item["strategy_line"], _json(item["parameters"]), generated_at),
            )
            metric_id = _stable_id("p21v2m", {"e": payload["experiment_id"], "p": parameter_set_id}, 24)
            conn.execute(
                """
                INSERT OR REPLACE INTO p21_v2_30d_metrics(
                  metric_id, experiment_id, parameter_set_id, strategy_line, metrics_json, parameters_json, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (metric_id, payload["experiment_id"], parameter_set_id, item["strategy_line"], _json(item["metrics"]), _json(item["parameters"]), generated_at),
            )
            for order in orders_by_param.get(parameter_set_id, []):
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
                        order["order_id"],
                        payload["experiment_id"],
                        parameter_set_id,
                        order["strategy_line"],
                        order["symbol"],
                        order["side"],
                        order["signal_time_ms"],
                        order["entry_time_ms"],
                        order.get("exit_time_ms"),
                        order["entry_price"],
                        order["stop_loss"],
                        order["take_profit"],
                        order["planned_rr"],
                        order.get("net_R"),
                        order.get("exit_reason"),
                        order.get("score", 0.0),
                        _json(order.get("reasons") or []),
                        _json(order.get("features") or {}),
                        order.get("lineage_mode") or ENGINE_MODE,
                        order.get("source_contract_version"),
                        _json(order.get("config_patch") or item.get("parameters") or {}),
                        _json(order.get("trade_plan_payload") or {}),
                        _json(order.get("fill_result") or {}),
                        order.get("entry_mode"),
                        order.get("effective_rr"),
                        _json(order.get("fast_exit_policy") or {}),
                        generated_at,
                    ),
                )
                upsert_backtest_order_native(
                    conn,
                    experiment_id=payload["experiment_id"],
                    parameter_set_id=parameter_set_id,
                    strategy_line=order["strategy_line"],
                    parameters=item.get("parameters") or {},
                    order=order,
                    generated_at=generated_at,
                )
            for day, day_orders in _group_by_day(orders_by_param.get(parameter_set_id, [])).items():
                conn.execute(
                    "INSERT OR REPLACE INTO p21_v2_daily_metrics(metric_id, experiment_id, parameter_set_id, strategy_line, day, metrics_json, generated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (_stable_id("p21v2d", {"e": payload["experiment_id"], "p": parameter_set_id, "d": day}, 24), payload["experiment_id"], parameter_set_id, item["strategy_line"], day, _json(_metrics(day_orders)), generated_at),
                )
            for symbol, symbol_orders in _group_by_symbol(orders_by_param.get(parameter_set_id, [])).items():
                conn.execute(
                    "INSERT OR REPLACE INTO p21_v2_symbol_metrics(metric_id, experiment_id, parameter_set_id, strategy_line, symbol, metrics_json, generated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (_stable_id("p21v2s", {"e": payload["experiment_id"], "p": parameter_set_id, "s": symbol}, 24), payload["experiment_id"], parameter_set_id, item["strategy_line"], symbol, _json(_metrics(symbol_orders)), generated_at),
                )
        for item in payload.get("recommendations") or []:
            conn.execute(
                """
                INSERT OR REPLACE INTO p21_v2_recommendations(
                  recommendation_id, experiment_id, parameter_set_id, status, priority, summary,
                  metrics_json, parameters_json, risks_json, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["recommendation_id"],
                    item["experiment_id"],
                    item["parameter_set_id"],
                    item["status"],
                    item["priority"],
                    item["summary"],
                    _json(item["metrics"]),
                    _json(item["parameters"]),
                    _json(item["risks"]),
                    generated_at,
                ),
            )


def experiments_payload(project_root: Path, *, limit: int = 50) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(
            "SELECT * FROM p21_v2_experiments ORDER BY generated_at DESC LIMIT ?",
            (max(1, min(int(limit or 50), 200)),),
        ).fetchall()]
    for row in rows:
        row["symbols"] = _loads(row.pop("symbols_json", None), [])
        _decorate_legacy_contract(row)
    return {"schema_version": SCHEMA_VERSION, **_legacy_contract_metadata(), "count": len(rows), "experiments": rows}


def experiment_detail_payload(project_root: Path, experiment_id: str) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        exp = conn.execute("SELECT * FROM p21_v2_experiments WHERE experiment_id = ?", (experiment_id,)).fetchone()
        rows = [dict(row) for row in conn.execute(
            "SELECT * FROM p21_v2_30d_metrics WHERE experiment_id = ? ORDER BY json_extract(metrics_json, '$.profit_factor') DESC",
            (experiment_id,),
        ).fetchall()]
        daily = [dict(row) for row in conn.execute(
            "SELECT * FROM p21_v2_daily_metrics WHERE experiment_id = ? ORDER BY day ASC LIMIT 2000",
            (experiment_id,),
        ).fetchall()]
        symbol = [dict(row) for row in conn.execute(
            "SELECT * FROM p21_v2_symbol_metrics WHERE experiment_id = ? ORDER BY json_extract(metrics_json, '$.total_R') DESC LIMIT 500",
            (experiment_id,),
        ).fetchall()]
    if not exp:
        return {"schema_version": SCHEMA_VERSION, "found": False, "experiment_id": experiment_id}
    exp_dict = dict(exp)
    exp_dict["symbols"] = _loads(exp_dict.pop("symbols_json", None), [])
    _decorate_legacy_contract(exp_dict)
    for coll in (rows, daily, symbol):
        for row in coll:
            row["metrics"] = _loads(row.pop("metrics_json", None), {})
            if "parameters_json" in row:
                row["parameters"] = _loads(row.pop("parameters_json", None), {})
            _decorate_legacy_contract(row)
    return {"schema_version": SCHEMA_VERSION, **_legacy_contract_metadata(), "found": True, "experiment": exp_dict, "leaderboard": rows, "daily": daily, "symbols": symbol}


def _decorate_strategy4_mode(row: dict[str, Any]) -> dict[str, Any]:
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    parameters = row.get("parameters") if isinstance(row.get("parameters"), dict) else {}
    if row.get("strategy_line") == "strategy4":
        mode = metrics.get("strategy4_replay_mode") or parameters.get("strategy4_replay_mode")
        if mode == "strategy4_persistent_observe_replay":
            row["strategy4_replay_mode"] = mode
        else:
            row["strategy4_replay_mode"] = "strategy4_simplified_wait_band"
    return _decorate_legacy_contract(row)


def leaderboard_payload(project_root: Path, *, limit: int = 50, exclude_legacy: bool = True) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(
            """
            SELECT * FROM p21_v2_30d_metrics
            ORDER BY json_extract(metrics_json, '$.profit_factor') DESC,
                     json_extract(metrics_json, '$.expectancy_R') DESC
            LIMIT ?
            """,
            (max(1, min(int(limit or 50), 200)),),
        ).fetchall()]
    decorated: list[dict[str, Any]] = []
    for row in rows:
        row["metrics"] = _loads(row.pop("metrics_json", None), {})
        row["parameters"] = _loads(row.pop("parameters_json", None), {})
        _decorate_strategy4_mode(row)
        if exclude_legacy and row.get("legacy_mode"):
            continue
        decorated.append(row)
    return {"schema_version": SCHEMA_VERSION, **_legacy_contract_metadata(), "count": len(decorated), "leaderboard": decorated, "exclude_legacy": exclude_legacy}


def _job_progress_defaults(job_type: str) -> dict[str, Any]:
    return {
        "phase": "queued",
        "job_type": job_type,
        "done_count": 0,
        "total_count": 0,
        "current_strategy_line": None,
        "current_symbol": None,
        "current_parameter_set_id": None,
        "engine_mode": ENGINE_MODE,
        **_legacy_contract_metadata(),
        "eta_sec": None,
        "last_error": None,
    }


def _process_alive(pid: int | None) -> bool:
    if not pid:
        return False
    if os.name == "nt":
        try:
            completed = subprocess.run(
                ["tasklist", "/FI", f"PID eq {int(pid)}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=3,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            return str(int(pid)) in (completed.stdout or "")
        except Exception:
            return False
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False
    except Exception:
        return False


def _iso_epoch(value: Any) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _age_sec(value: Any) -> int | None:
    epoch = _iso_epoch(value)
    if not epoch:
        return None
    return max(0, int(datetime.now(timezone.utc).timestamp() - epoch))


def _python_process_rows() -> list[dict[str, Any]]:
    if os.name != "nt":
        return []
    try:
        completed = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "Get-CimInstance Win32_Process -Filter \"name='python.exe'\" | "
                "Select-Object ProcessId,ParentProcessId,CreationDate,CommandLine | ConvertTo-Json -Compress",
            ],
            capture_output=True,
            text=True,
            timeout=4,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        raw = (completed.stdout or "").strip()
        if not raw:
            return []
        payload = json.loads(raw)
        if isinstance(payload, dict):
            payload = [payload]
        return [row for row in payload if isinstance(row, dict)]
    except Exception:
        return []


def _infer_step7_95_runtime(progress: dict[str, Any]) -> dict[str, Any]:
    rows = _python_process_rows()
    raw_pid = progress.get("pid")
    pid: int | None = None
    try:
        pid = int(raw_pid) if raw_pid else None
    except Exception:
        pid = None
    if not pid:
        candidates: list[dict[str, Any]] = []
        for row in rows:
            cmd = str(row.get("CommandLine") or "")
            if "step7_95_full_universe_30d_config_matrix_runner.py" in cmd:
                candidates.append(row)
        if candidates:
            # Prefer the newest script process when old progress has no PID.
            selected = candidates[-1]
            try:
                pid = int(selected.get("ProcessId") or 0) or None
            except Exception:
                pid = None
    children: list[int] = []
    if pid:
        for row in rows:
            try:
                if int(row.get("ParentProcessId") or 0) == int(pid):
                    children.append(int(row.get("ProcessId") or 0))
            except Exception:
                continue
    return {
        "pid": pid,
        "pid_alive": _process_alive(pid),
        "worker_pids": sorted({item for item in children if item}),
        "worker_active_count": len({item for item in children if item}),
        "worker_last_seen_at": _now() if children else progress.get("worker_last_seen_at"),
    }


def _script_progress_health(progress: dict[str, Any]) -> dict[str, Any]:
    runtime = _infer_step7_95_runtime(progress)
    updated_age = _age_sec(progress.get("updated_at"))
    last_progress_age = _age_sec(progress.get("last_progress_at") or progress.get("last_checkpoint_at") or progress.get("updated_at"))
    max_workers = int(progress.get("max_workers") or 0)
    worker_count = int(runtime.get("worker_active_count") or progress.get("active_workers") or 0)
    reason_codes: list[str] = []
    if not runtime.get("pid_alive") and str(progress.get("status") or "").lower() == "running":
        reason_codes.append("orphan_main_process")
    if updated_age is not None and updated_age > 180 and str(progress.get("status") or "").lower() == "running":
        reason_codes.append("progress_stale")
    if last_progress_age is not None and last_progress_age > 300 and str(progress.get("status") or "").lower() == "running":
        reason_codes.append("no_recent_done_progress")
    if max_workers > 1 and worker_count <= 0 and str(progress.get("phase") or "") in {"matrix", "evaluator"}:
        reason_codes.append("worker_pool_missing")
    status = str(progress.get("status") or "running")
    if "orphan_main_process" in reason_codes:
        health_status = "stalled"
    elif reason_codes:
        health_status = "running_degraded"
    else:
        health_status = status
    return {
        "status": health_status,
        "pid": runtime.get("pid"),
        "pid_alive": runtime.get("pid_alive"),
        "progress_age_sec": updated_age,
        "stalled_sec": last_progress_age,
        "worker_active_count": worker_count,
        "worker_pids": runtime.get("worker_pids") or progress.get("worker_pids") or [],
        "worker_last_seen_at": runtime.get("worker_last_seen_at"),
        "reason_codes": reason_codes,
    }


def _insert_job_event(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    status: str,
    phase: str,
    message: str,
    evidence: dict[str, Any],
) -> None:
    generated_at = _now()
    event_id = _stable_id("p21v2jobevt", {"job_id": job_id, "status": status, "phase": phase, "at": generated_at, "message": message}, 24)
    conn.execute(
        """
        INSERT OR REPLACE INTO p21_v2_job_events(
          event_id, job_id, status, phase, message, evidence_json, generated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (event_id, job_id, status, phase, message, _json(evidence), generated_at),
    )


def _script_progress(project_root: Path) -> dict[str, Any]:
    progress_path = project_root / STEP7_95_PROGRESS_PATH
    if not progress_path.exists():
        return {}
    raw = _read_json(progress_path, {})
    if not isinstance(raw, dict):
        return {}
    if not raw:
        return {}
    status = str(raw.get("status") or "").strip() or None
    target_strategy_lines = raw.get("target_strategy_lines")
    phase = str(raw.get("phase") or "unknown")
    if phase == "download":
        done = int(raw.get("download_index") or 0)
        total = int(raw.get("download_total") or 0)
        return {
            "status": status,
            "phase": "kline_download",
            "done_count": done,
            "total_count": total,
            "current_symbol": raw.get("current_symbol"),
            "current_strategy_line": None,
            "current_parameter_set_id": None,
            "engine_mode": ENGINE_MODE,
            "target_strategy_lines": target_strategy_lines,
            "job_id": raw.get("job_id"),
            "pid": raw.get("pid"),
            "progress_source": raw.get("progress_source") or "step7_95_script_progress",
            "last_progress_at": raw.get("last_progress_at"),
            "last_done_count": raw.get("last_done_count"),
            "shards_per_min": raw.get("shards_per_min"),
            "updated_at": raw.get("updated_at"),
        }
    if phase == "matrix":
        done = int(raw.get("done_count") or raw.get("matrix_index") or 0)
        total = int(raw.get("total_count") or raw.get("matrix_total") or 0)
        return {
            "status": status,
            "phase": "evaluator",
            "done_count": done,
            "total_count": total,
            "current_symbol": raw.get("current_symbol"),
            "current_strategy_line": raw.get("current_strategy_line"),
            "current_parameter_set_id": raw.get("current_parameter_set_id"),
            "engine_mode": ENGINE_MODE,
            "target_strategy_lines": target_strategy_lines,
            "job_id": raw.get("job_id"),
            "execution_mode": raw.get("execution_mode"),
            "memory_guard_status": raw.get("memory_guard_status"),
            "matrix_index": raw.get("matrix_index"),
            "matrix_total": raw.get("matrix_total"),
            "strategy_line_index": raw.get("strategy_line_index"),
            "strategy_line_total": raw.get("strategy_line_total"),
            "parameter_set_index": raw.get("parameter_set_index"),
            "parameter_set_total": raw.get("parameter_set_total"),
            "current_symbol_shard": raw.get("current_symbol_shard"),
            "symbol_shard_index": raw.get("symbol_shard_index"),
            "symbol_shard_total": raw.get("symbol_shard_total"),
            "persisted_order_count": raw.get("persisted_order_count"),
            "max_workers": raw.get("max_workers"),
            "active_workers": raw.get("active_workers"),
            "idle_workers": raw.get("idle_workers"),
            "writer_queue_size": raw.get("writer_queue_size"),
            "avg_shard_sec": raw.get("avg_shard_sec"),
            "p95_shard_sec": raw.get("p95_shard_sec"),
            "sqlite_write_sec": raw.get("sqlite_write_sec"),
            "eval_sec": raw.get("eval_sec"),
            "resume_experiment_id": raw.get("resume_experiment_id"),
            "last_checkpoint_at": raw.get("last_checkpoint_at"),
            "pid": raw.get("pid"),
            "progress_source": raw.get("progress_source") or "step7_95_script_progress",
            "last_progress_at": raw.get("last_progress_at"),
            "last_done_count": raw.get("last_done_count"),
            "shards_per_min": raw.get("shards_per_min"),
            "worker_pids": raw.get("worker_pids"),
            "worker_last_seen_at": raw.get("worker_last_seen_at"),
            "stalled_sec": raw.get("stalled_sec"),
            "updated_at": raw.get("updated_at"),
        }
    if phase in {"done", "complete"}:
        done = int(raw.get("matrix_index") or raw.get("download_index") or 0)
        total = int(raw.get("matrix_total") or raw.get("download_total") or 0)
        return {
            **raw,
            "phase": "done",
            "done_count": done,
            "total_count": total,
            "engine_mode": ENGINE_MODE,
            "updated_at": raw.get("updated_at"),
        }
    return {**raw, "engine_mode": ENGINE_MODE}


def _script_job_id(project_root: Path, progress: dict[str, Any] | None = None) -> str:
    progress = progress if isinstance(progress, dict) else _script_progress(project_root)
    raw_job_id = str((progress or {}).get("job_id") or "").strip()
    if raw_job_id:
        return raw_job_id
    return "step7_95_script_progress"


def _script_job_payload(project_root: Path) -> dict[str, Any] | None:
    progress = _script_progress(project_root)
    if not progress:
        return None
    health = _script_progress_health(progress)
    if health.get("pid") and not progress.get("pid"):
        progress["pid"] = health.get("pid")
    if health.get("worker_pids"):
        progress["worker_pids"] = health.get("worker_pids")
        progress["active_workers"] = health.get("worker_active_count")
        progress["idle_workers"] = max(0, int(progress.get("max_workers") or 0) - int(health.get("worker_active_count") or 0))
        progress["worker_last_seen_at"] = health.get("worker_last_seen_at")
    progress["progress_age_sec"] = health.get("progress_age_sec")
    progress["stalled_sec"] = health.get("stalled_sec")
    progress["health_status"] = health.get("status")
    progress["health_reason_codes"] = health.get("reason_codes") or []
    status = str(progress.get("status") or "").strip() or "running"
    if status not in {"running", "queued", "stopping", "paused", "download_complete", "complete", "done", "blocked", "failed", "error"}:
        status = "running"
    if status == "running" and health.get("status") in {"running_degraded", "stalled"}:
        status = str(health.get("status"))
    target_lines = progress.get("target_strategy_lines")
    if isinstance(target_lines, list) and target_lines:
        strategy_line = ",".join(str(line) for line in target_lines)
    else:
        strategy_line = str(progress.get("current_strategy_line") or "all")
    return {
        "job_id": _script_job_id(project_root, progress),
        "job_type": "matrix_backtest",
        "status": status,
        "phase": progress.get("phase") or "running",
        "pid": health.get("pid"),
        "request": {
            "strategy_line": strategy_line,
            "engine_mode": ENGINE_MODE,
            "source": "step7_95_progress_json",
        },
        "progress": progress,
        "health": health,
        "last_error": progress.get("last_error"),
        "started_at": progress.get("started_at"),
        "updated_at": progress.get("updated_at"),
        "finished_at": progress.get("finished_at"),
        "synthetic": True,
    }


def start_job_payload(
    project_root: Path,
    *,
    job_type: str,
    symbols: list[str] | None = None,
    strategy_line: str = "all",
    days: int = DEFAULT_WINDOW_DAYS,
    max_symbols: int = 20,
    max_sets: int = 120,
    sleep_sec: float = 0.6,
    symbol_shard_size: int = 25,
    max_workers: int = 1,
    scheduler_mode: str = "parameter_batch",
    resume_experiment_id: str | None = None,
) -> dict[str, Any]:
    if job_type not in {"kline_download", "matrix_backtest"}:
        return {"schema_version": SCHEMA_VERSION, "status": "invalid_job_type", "job_type": job_type}
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    now = _now()
    request = {
        "job_type": job_type,
        "symbols": symbols or [],
        "strategy_line": strategy_line,
        "days": max(1, min(int(days or DEFAULT_WINDOW_DAYS), 90)),
        "max_symbols": max(1, min(int(max_symbols or 20), 600)),
        "max_sets": max(1, min(int(max_sets or 120), 5000)),
        "sleep_sec": max(0.0, min(float(0.6 if sleep_sec is None else sleep_sec), 10.0)),
        "symbol_shard_size": max(1, min(int(symbol_shard_size or 25), 200)),
        "max_workers": max(1, min(int(max_workers or 1), max(1, os.cpu_count() or 1))),
        "scheduler_mode": str(scheduler_mode or "parameter_batch").strip() or "parameter_batch",
        "resume_experiment_id": str(resume_experiment_id or "").strip() or None,
        "engine_mode": ENGINE_MODE,
        **_legacy_contract_metadata(),
    }
    job_id = _stable_id("p21v2job", {"request": request, "started_at": now}, 20)
    log_dir = project_root / JOB_LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{job_id}.log"
    script = project_root / "scripts" / "step7_95_full_universe_30d_config_matrix_runner.py"
    cmd = [
        sys.executable,
        str(script),
        "--days",
        str(request["days"]),
        "--max-symbols",
        str(request["max_symbols"]),
        "--max-sets-per-line",
        str(request["max_sets"]),
        "--sleep-sec",
        str(request["sleep_sec"]),
        "--symbol-shard-size",
        str(request["symbol_shard_size"]),
        "--max-workers",
        str(request["max_workers"]),
        "--scheduler-mode",
        str(request["scheduler_mode"]),
    ]
    if request["resume_experiment_id"]:
        cmd.extend(["--resume-experiment-id", str(request["resume_experiment_id"])])
    if request["strategy_line"] and request["strategy_line"] != "all":
        cmd.extend(["--strategy-lines", str(request["strategy_line"])])
    if job_type == "kline_download":
        cmd.append("--download-only")
    progress = {**_job_progress_defaults(job_type), "phase": "starting", "engine_mode": ENGINE_MODE, **_legacy_contract_metadata()}
    progress_seed = {
        **progress,
        "schema_version": SCHEMA_VERSION,
        "status": "running",
        "job_id": job_id,
        "started_at": now,
        "updated_at": now,
        "days": request["days"],
        "symbol_count": request["max_symbols"],
        "max_sets_per_line": request["max_sets"],
        "symbol_shard_size": request["symbol_shard_size"],
        "max_workers": request["max_workers"],
        "scheduler_mode": request["scheduler_mode"],
        "resume_experiment_id": request["resume_experiment_id"],
    }
    progress_path = project_root / STEP7_95_PROGRESS_PATH
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(json.dumps(progress_seed, ensure_ascii=False, indent=2), encoding="utf-8")
    with log_path.open("a", encoding="utf-8") as log_file:
        popen_kwargs: dict[str, Any] = {"cwd": str(project_root), "stdout": log_file, "stderr": subprocess.STDOUT}
        if os.name == "nt":
            popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        proc = subprocess.Popen(cmd, **popen_kwargs)
    progress_seed["pid"] = proc.pid
    progress_seed["progress_source"] = "fastapi_job_registry"
    progress_path.write_text(json.dumps(progress_seed, ensure_ascii=False, indent=2), encoding="utf-8")
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO p21_v2_jobs(
              job_id, job_type, status, phase, pid, request_json, progress_json,
              log_path, last_error, started_at, updated_at, finished_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                job_type,
                "running",
                "starting",
                proc.pid,
                _json(request),
                _json(progress),
                str(log_path),
                None,
                now,
                now,
                None,
            ),
        )
        _insert_job_event(conn, job_id=job_id, status="running", phase="starting", message="job started", evidence={"cmd": cmd, "pid": proc.pid})
    return {"schema_version": SCHEMA_VERSION, "status": "running", "job_id": job_id, "pid": proc.pid, "request": request, "progress": progress, "log_path": str(log_path)}


def job_status_payload(project_root: Path, job_id: str) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM p21_v2_jobs WHERE job_id = ?", (job_id,)).fetchone()
        if not row:
            script_job = _script_job_payload(project_root)
            if script_job and str(job_id) == str(script_job.get("job_id")):
                return {
                    "schema_version": SCHEMA_VERSION,
                    "found": True,
                    "alive": bool((script_job.get("health") or {}).get("pid_alive")),
                    "events": [],
                    **script_job,
                }
            return {"schema_version": SCHEMA_VERSION, "found": False, "job_id": job_id}
        data = dict(row)
        request = _loads(data.get("request_json"), {})
        progress = _loads(data.get("progress_json"), _job_progress_defaults(data.get("job_type") or "unknown"))
        script_progress = _script_progress(project_root)
        if script_progress:
            progress.update({key: value for key, value in script_progress.items() if value is not None})
        alive = _process_alive(data.get("pid"))
        status = str(data.get("status") or "unknown")
        progress_status = str(progress.get("status") or "").lower()
        terminal_progress = progress_status in {"complete", "done", "download_complete", "blocked", "failed", "error"}
        if status in {"done", "failed", "stopped"} and alive and not terminal_progress:
            status = "running"
            data["status"] = status
            data["finished_at"] = None
            data["last_error"] = None
            data["phase"] = str(progress.get("phase") or data.get("phase") or "running")
            conn.execute(
                "UPDATE p21_v2_jobs SET status = ?, phase = ?, progress_json = ?, updated_at = ?, finished_at = NULL, last_error = NULL WHERE job_id = ?",
                (status, data["phase"], _json(progress), _now(), job_id),
            )
            _insert_job_event(
                conn,
                job_id=job_id,
                status="running",
                phase=str(progress.get("phase") or data.get("phase") or "running"),
                message="job status reconciled from premature terminal state",
                evidence={"alive": alive, "pid": data.get("pid"), "previous_status": data.get("status")},
            )
        if status == "running" and (terminal_progress or not alive):
            if progress_status in {"blocked", "failed", "error"}:
                status = "failed"
            elif progress_status in {"complete", "done", "download_complete"}:
                status = "done"
            else:
                status = "failed"
                data["last_error"] = data.get("last_error") or "job_exited_before_terminal_progress"
            phase = progress.get("phase") or data.get("phase") or status
            finished_at = _now()
            conn.execute(
                "UPDATE p21_v2_jobs SET status = ?, phase = ?, progress_json = ?, updated_at = ?, finished_at = COALESCE(finished_at, ?), last_error = ? WHERE job_id = ?",
                (status, phase, _json(progress), finished_at, finished_at, data.get("last_error"), job_id),
            )
            _insert_job_event(conn, job_id=job_id, status=status, phase=str(phase), message="job process exited", evidence={"alive": alive, "pid": data.get("pid")})
        elif status == "running":
            conn.execute(
                "UPDATE p21_v2_jobs SET phase = ?, progress_json = ?, updated_at = ? WHERE job_id = ?",
                (str(progress.get("phase") or data.get("phase") or "running"), _json(progress), _now(), job_id),
            )
        events = [
            dict(item)
            for item in conn.execute(
                "SELECT * FROM p21_v2_job_events WHERE job_id = ? ORDER BY generated_at DESC LIMIT 20",
                (job_id,),
            ).fetchall()
        ]
    for item in events:
        item["evidence"] = _loads(item.pop("evidence_json", None), {})
    if status in {"done", "failed", "stopped"}:
        alive = False
    return {
        "schema_version": SCHEMA_VERSION,
        "found": True,
        "job_id": job_id,
        "job_type": data.get("job_type"),
        "status": status,
        "phase": progress.get("phase") or data.get("phase"),
        "pid": data.get("pid"),
        "alive": alive,
        "request": request,
        "progress": progress,
        "log_path": data.get("log_path"),
        "last_error": data.get("last_error"),
        "started_at": data.get("started_at"),
        "updated_at": data.get("updated_at"),
        "finished_at": data.get("finished_at"),
        "events": events,
    }


def stop_job_payload(project_root: Path, job_id: str) -> dict[str, Any]:
    status = job_status_payload(project_root, job_id)
    if not status.get("found"):
        return status
    db_path = p21_db_path(project_root)
    pid = status.get("pid")
    stopped = False
    message = "job not running"
    if status.get("alive") and pid:
        try:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, text=True, timeout=10)
            else:
                os.kill(int(pid), 15)
            stopped = True
            message = "stop signal sent"
        except Exception as exc:
            message = str(exc)
    with _connect(db_path) as conn:
        now = _now()
        conn.execute(
            "UPDATE p21_v2_jobs SET status = ?, phase = ?, updated_at = ?, finished_at = COALESCE(finished_at, ?), last_error = ? WHERE job_id = ?",
            ("stopped", "stopped", now, now, None if stopped else message, job_id),
        )
        _insert_job_event(conn, job_id=job_id, status="stopped", phase="stopped", message=message, evidence={"pid": pid, "stopped": stopped})
    return job_status_payload(project_root, job_id)


def jobs_payload(project_root: Path, *, limit: int = 20) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(
            "SELECT * FROM p21_v2_jobs ORDER BY started_at DESC LIMIT ?",
            (max(1, min(int(limit or 20), 100)),),
        ).fetchall()]
    out = []
    script_job = _script_job_payload(project_root)
    if script_job and script_job.get("status") in {"running", "running_degraded", "stalled", "queued", "stopping", "paused"}:
        out.append(script_job)
    for row in rows:
        if script_job and str(row.get("job_id")) == str(script_job.get("job_id")):
            continue
        out.append(
            {
                "job_id": row.get("job_id"),
                "job_type": row.get("job_type"),
                "status": row.get("status"),
                "phase": row.get("phase"),
                "pid": row.get("pid"),
                "request": _loads(row.get("request_json"), {}),
                "progress": _loads(row.get("progress_json"), {}),
                "last_error": row.get("last_error"),
                "started_at": row.get("started_at"),
                "updated_at": row.get("updated_at"),
                "finished_at": row.get("finished_at"),
            }
        )
    limit_n = max(1, min(int(limit or 20), 100))
    out = out[:limit_n]
    return {"schema_version": SCHEMA_VERSION, "count": len(out), "jobs": out}


def experiment_orders_payload(
    project_root: Path,
    experiment_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
    strategy_line: str | None = None,
    symbol: str | None = None,
    parameter_set_id: str | None = None,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    where = ["experiment_id = ?"]
    args: list[Any] = [experiment_id]
    if strategy_line and strategy_line != "all":
        where.append("strategy_line = ?")
        args.append(strategy_line)
    if symbol:
        where.append("symbol = ?")
        args.append(symbol.upper())
    if parameter_set_id:
        where.append("parameter_set_id = ?")
        args.append(parameter_set_id)
    clause = " AND ".join(where)
    limit_i = max(1, min(int(limit or 100), 500))
    offset_i = max(0, int(offset or 0))
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total = int(conn.execute(f"SELECT COUNT(*) FROM p21_v2_shadow_orders WHERE {clause}", args).fetchone()[0])
        rows = [dict(row) for row in conn.execute(
            f"SELECT * FROM p21_v2_shadow_orders WHERE {clause} ORDER BY signal_time_ms ASC LIMIT ? OFFSET ?",
            [*args, limit_i, offset_i],
        ).fetchall()]
    for row in rows:
        row["reasons"] = _loads(row.pop("reasons_json", None), [])
        row["features"] = _loads(row.pop("features_json", None), {})
        row["config_patch"] = _loads(row.pop("config_patch_json", None), {})
        row["trade_plan_payload"] = _loads(row.pop("trade_plan_payload_json", None), {})
        row["fill_result"] = _loads(row.pop("fill_result_json", None), {})
        row["fast_exit_policy"] = _loads(row.pop("fast_exit_policy_json", None), {})
        _decorate_legacy_contract(row)
    return {
        "schema_version": SCHEMA_VERSION,
        **_legacy_contract_metadata(),
        "experiment_id": experiment_id,
        "count": len(rows),
        "total": total,
        "limit": limit_i,
        "offset": offset_i,
        "orders": rows,
    }


def _metric_page_payload(
    project_root: Path,
    experiment_id: str,
    *,
    table: str,
    order_by: str,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_v2_tables(db_path)
    limit_i = max(1, min(int(limit or 100), 500))
    offset_i = max(0, int(offset or 0))
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total = int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE experiment_id = ?", (experiment_id,)).fetchone()[0])
        rows = [dict(row) for row in conn.execute(
            f"SELECT * FROM {table} WHERE experiment_id = ? ORDER BY {order_by} LIMIT ? OFFSET ?",
            (experiment_id, limit_i, offset_i),
        ).fetchall()]
    for row in rows:
        row["metrics"] = _loads(row.pop("metrics_json", None), {})
    return {"schema_version": SCHEMA_VERSION, "experiment_id": experiment_id, "count": len(rows), "total": total, "limit": limit_i, "offset": offset_i, "rows": rows}


def experiment_daily_payload(project_root: Path, experiment_id: str, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    return _metric_page_payload(project_root, experiment_id, table="p21_v2_daily_metrics", order_by="day ASC", limit=limit, offset=offset)


def experiment_symbols_payload(project_root: Path, experiment_id: str, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    return _metric_page_payload(
        project_root,
        experiment_id,
        table="p21_v2_symbol_metrics",
        order_by="json_extract(metrics_json, '$.total_R') DESC",
        limit=limit,
        offset=offset,
    )


def export_config_candidate_payload(project_root: Path, *, experiment_id: str, parameter_set_id: str | None = None) -> dict[str, Any]:
    detail = experiment_detail_payload(project_root, experiment_id)
    if not detail.get("found"):
        return {"status": "not_found", "experiment_id": experiment_id}
    rows = detail.get("leaderboard") or []
    selected = None
    if parameter_set_id:
        selected = next((row for row in rows if row.get("parameter_set_id") == parameter_set_id), None)
    if selected is None and rows:
        selected = rows[0]
    if selected is None:
        return {"status": "empty", "experiment_id": experiment_id}
    candidate = {
        "schema_version": SCHEMA_VERSION,
        "status": "shadow_config_candidate",
        **_legacy_contract_metadata(),
        "experiment_id": experiment_id,
        "parameter_set_id": selected["parameter_set_id"],
        "strategy_line": selected["strategy_line"],
        "parameters": selected.get("parameters") or {},
        "metrics": selected.get("metrics") or {},
        "note": "P21 V2 legacy candidate only. Rerun under paper_equivalent before paper/prod promotion.",
        "generated_at": _now(),
    }
    path = project_root / CANDIDATE_EXPORT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json(candidate), encoding="utf-8")
    return {"status": "ok", "path": str(path), "candidate": candidate}
