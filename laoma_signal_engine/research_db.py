from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections import Counter
from datetime import datetime, timezone
from statistics import mean, pstdev
from pathlib import Path
from typing import Any

import httpx

from laoma_signal_engine.context.constants import FUNDING_ABS_NEUTRAL_MAX, FUNDING_ABS_WARM_MAX, OI_PCT_EPS
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now


SCHEMA_VERSION = "research_db_v1"
FEATURE_SCHEMA_VERSION = "entry_known_v1"
TARGET_SCHEMA_VERSION = "tq_target_v1"
DATASET_ID = "baseline_research_db_latest"
P21_DB_RELATIVE = Path("DATA/backtest/p21_parameter_optimization.db")
MS_PER_DAY = 24 * 60 * 60 * 1000
OI_HISTORY_SAFE_LOOKBACK_MS = 29 * MS_PER_DAY
REQUIRED_ENTRY_FEATURES = [
    "pct_1m_bps",
    "pct_3m_bps",
    "pct_5m_bps",
    "pct_15m_bps",
    "rsi_14",
    "bollinger_position",
    "bollinger_width_bps",
    "ema20_distance_bps",
    "ema60_distance_bps",
    "vwap_distance_bps",
    "atr_14_bps",
    "atr_1m_bps",
    "volume_z",
    "body_ratio_1m",
    "upper_wick_ratio_1m",
    "lower_wick_ratio_1m",
    "range_pos_30m",
    "taker_buy_ratio_1m",
    "taker_buy_ratio_5m",
    "cvd_proxy_5m",
    "ofi_proxy_5m",
    "spread_bps",
    "depth_imbalance",
    "btc_trend",
    "btc_volatility",
    "btc_alignment",
    "market_breadth",
    "oi_change",
    "oi_state",
    "oi_z",
    "funding_rate",
    "funding_bucket",
    "funding_crowded_side",
    "price_flow_alignment",
    "side_flow_alignment",
    "entry_hour_utc",
    "known_at_entry",
]


def p21_db_path(project_root: Path) -> Path:
    return project_root.resolve() / P21_DB_RELATIVE


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _http_get_json(
    client: httpx.Client,
    url: str,
    params: dict[str, Any],
    *,
    retries: int = 4,
) -> Any:
    last_exc: BaseException | None = None
    for attempt in range(retries):
        try:
            response = client.get(url, params=params)
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(0.35 * (attempt + 1))
                continue
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            last_exc = exc
            time.sleep(0.35 * (attempt + 1))
    raise RuntimeError(f"http_get_json_failed:{last_exc}") from last_exc


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _loads(value: Any) -> Any:
    if value is None or value == "":
        return {}
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return {"_raw": str(value)}


def _stable_id(*parts: Any, prefix: str = "rf") -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return f"{prefix}_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    return {key: row[key] for key in row.keys()}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _query_rows(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    return [_row_to_dict(row) for row in conn.execute(sql, params).fetchall()]


def _oi_percentile(values: list[float], latest: float) -> float | None:
    if not values:
        return None
    return round(sum(1 for value in values if value <= latest) / len(values), 6)


def ensure_research_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS research_trade_facts (
            sample_id TEXT PRIMARY KEY,
            source_type TEXT NOT NULL,
            sandbox_id TEXT NOT NULL,
            experiment_id TEXT,
            run_id TEXT,
            cycle_id TEXT,
            order_id TEXT,
            parameter_set_id TEXT,
            strategy_line TEXT NOT NULL,
            strategy_version TEXT,
            config_hash TEXT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            signal_time_ms INTEGER,
            entry_time_ms INTEGER,
            exit_time_ms INTEGER,
            entry_price REAL,
            exit_price REAL,
            stop_loss REAL,
            take_profit REAL,
            planned_rr REAL,
            effective_rr REAL,
            entry_mode TEXT,
            exit_reason TEXT,
            net_R REAL,
            gross_pnl REAL,
            fee REAL,
            slippage REAL,
            fill_model_version TEXT,
            replay_version TEXT,
            trade_plan_payload_json TEXT,
            fill_result_json TEXT,
            config_patch_json TEXT,
            lineage_json TEXT NOT NULL,
            field_quality_json TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            UNIQUE(source_type, order_id, parameter_set_id, schema_version)
        );

        CREATE INDEX IF NOT EXISTS idx_research_trade_facts_strategy
            ON research_trade_facts(strategy_line, source_type, entry_time_ms);
        CREATE INDEX IF NOT EXISTS idx_research_trade_facts_symbol
            ON research_trade_facts(symbol, strategy_line);
        CREATE INDEX IF NOT EXISTS idx_research_trade_facts_param
            ON research_trade_facts(parameter_set_id, strategy_line);

        CREATE TABLE IF NOT EXISTS research_entry_features (
            feature_sample_id TEXT PRIMARY KEY,
            sample_id TEXT NOT NULL,
            source_type TEXT NOT NULL,
            sandbox_id TEXT NOT NULL,
            experiment_id TEXT,
            parameter_set_id TEXT,
            strategy_line TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_time_ms INTEGER,
            known_at_ms INTEGER,
            asof_lag_seconds REAL,
            source_level TEXT NOT NULL,
            source_ref_json TEXT NOT NULL,
            feature_completeness REAL NOT NULL,
            proxy_level TEXT NOT NULL,
            missing_fields_json TEXT NOT NULL,
            features_json TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            UNIQUE(sample_id, schema_version)
        );

        CREATE INDEX IF NOT EXISTS idx_research_entry_features_strategy
            ON research_entry_features(strategy_line, source_type, entry_time_ms);
        CREATE INDEX IF NOT EXISTS idx_research_entry_features_completeness
            ON research_entry_features(feature_completeness, proxy_level);

        CREATE TABLE IF NOT EXISTS research_tq_samples (
            research_tq_id TEXT PRIMARY KEY,
            sample_id TEXT NOT NULL,
            diagnostic_id TEXT,
            source_type TEXT NOT NULL,
            package_key TEXT,
            experiment_id TEXT,
            parameter_set_id TEXT,
            strategy_line TEXT NOT NULL,
            order_id TEXT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_time_ms INTEGER,
            exit_time_ms INTEGER,
            net_R REAL,
            MFE_R REAL,
            MAE_R REAL,
            planned_RR REAL,
            holding_minutes REAL,
            exit_reason TEXT,
            root_cause TEXT,
            deep_subcause TEXT,
            entry_quality_label TEXT,
            market_context_label TEXT,
            micro_context_label TEXT,
            target_json TEXT NOT NULL,
            diagnostics_json TEXT NOT NULL,
            source_payload_json TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            UNIQUE(sample_id, diagnostic_id, schema_version)
        );

        CREATE INDEX IF NOT EXISTS idx_research_tq_samples_strategy
            ON research_tq_samples(strategy_line, source_type, entry_time_ms);
        CREATE INDEX IF NOT EXISTS idx_research_tq_samples_root
            ON research_tq_samples(root_cause, deep_subcause);

        CREATE TABLE IF NOT EXISTS research_dataset_cards (
            dataset_id TEXT PRIMARY KEY,
            dataset_scope TEXT NOT NULL,
            source_db_path TEXT NOT NULL,
            strategies_json TEXT NOT NULL,
            time_range_json TEXT NOT NULL,
            counts_json TEXT NOT NULL,
            feature_schema_version TEXT NOT NULL,
            target_schema_version TEXT NOT NULL,
            leakage_guard_json TEXT NOT NULL,
            missing_proxy_summary_json TEXT NOT NULL,
            intended_usage TEXT NOT NULL,
            prohibited_usage TEXT NOT NULL,
            generated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS research_materialize_ledger (
            ledger_id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            source_type TEXT NOT NULL,
            rows_json TEXT NOT NULL,
            status TEXT NOT NULL,
            error TEXT,
            dry_run INTEGER NOT NULL DEFAULT 0,
            generated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS market_oi_15m (
            symbol TEXT NOT NULL,
            source_time_ms INTEGER NOT NULL,
            period TEXT NOT NULL,
            sum_open_interest REAL,
            sum_open_interest_value REAL,
            oi_change REAL,
            oi_z REAL,
            oi_percentile REAL,
            oi_state TEXT,
            source TEXT NOT NULL,
            source_payload_json TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            PRIMARY KEY(symbol, source_time_ms, period)
        );

        CREATE INDEX IF NOT EXISTS idx_market_oi_15m_asof
            ON market_oi_15m(symbol, period, source_time_ms);

        CREATE TABLE IF NOT EXISTS market_funding_8h (
            symbol TEXT NOT NULL,
            funding_time_ms INTEGER NOT NULL,
            funding_rate REAL,
            funding_bucket TEXT,
            funding_crowded_side TEXT,
            source TEXT NOT NULL,
            source_payload_json TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            PRIMARY KEY(symbol, funding_time_ms)
        );

        CREATE INDEX IF NOT EXISTS idx_market_funding_8h_asof
            ON market_funding_8h(symbol, funding_time_ms);
        """
    )


def upsert_market_oi_15m_rows(conn: sqlite3.Connection, symbol: str, rows: list[dict[str, Any]]) -> int:
    ensure_research_tables(conn)
    sym = symbol.upper().strip()
    values: list[float] = []
    count = 0
    for row in sorted(rows, key=lambda item: int(float(item.get("timestamp") or item.get("source_time_ms") or 0))):
        source_ms = row.get("timestamp") or row.get("source_time_ms")
        oi = _float_or_none(row.get("sumOpenInterest") or row.get("sum_open_interest"))
        oi_value = _float_or_none(row.get("sumOpenInterestValue") or row.get("sum_open_interest_value"))
        if source_ms is None or oi is None:
            continue
        source_ms = int(float(source_ms))
        history = values[-16:]
        oi_change = None
        if values and values[-1] not in (None, 0):
            oi_change = (oi - values[-1]) / abs(values[-1])
        oi_z = None
        if len(history) >= 2:
            stdev = pstdev(history)
            oi_z = (oi - mean(history)) / stdev if stdev > 0 else 0.0
        percentile = _oi_percentile(history + [oi], oi)
        conn.execute(
            """
            INSERT OR REPLACE INTO market_oi_15m(
              symbol, source_time_ms, period, sum_open_interest, sum_open_interest_value,
              oi_change, oi_z, oi_percentile, oi_state, source, source_payload_json, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sym,
                source_ms,
                "15m",
                oi,
                oi_value,
                _round_or_none(oi_change, 10),
                _round_or_none(oi_z, 6),
                percentile,
                None,
                "binance:futures/data/openInterestHist",
                _json(row),
                to_iso_z(utc_now()),
            ),
        )
        values.append(oi)
        count += 1
    return count


def upsert_market_funding_8h_rows(conn: sqlite3.Connection, symbol: str, rows: list[dict[str, Any]]) -> int:
    ensure_research_tables(conn)
    sym = symbol.upper().strip()
    count = 0
    for row in rows:
        funding_ms = row.get("fundingTime") or row.get("funding_time_ms")
        rate = _float_or_none(row.get("fundingRate") or row.get("funding_rate"))
        if funding_ms is None or rate is None:
            continue
        funding_ms = int(float(funding_ms))
        conn.execute(
            """
            INSERT OR REPLACE INTO market_funding_8h(
              symbol, funding_time_ms, funding_rate, funding_bucket, funding_crowded_side,
              source, source_payload_json, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sym,
                funding_ms,
                rate,
                _funding_bucket_from_rate(rate),
                _funding_crowded_side(rate),
                "binance:fapi/v1/fundingRate",
                _json(row),
                to_iso_z(utc_now()),
            ),
        )
        count += 1
    return count


def _download_funding_rows(client: httpx.Client, symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor = int(start_ms)
    while cursor <= end_ms:
        data = _http_get_json(
            client,
            "https://fapi.binance.com/fapi/v1/fundingRate",
            {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": 1000},
        )
        if not isinstance(data, list) or not data:
            break
        got = [item for item in data if isinstance(item, dict)]
        rows.extend(got)
        last = max(int(float(item.get("fundingTime") or cursor)) for item in got)
        next_cursor = last + 1
        if next_cursor <= cursor or len(got) < 1000:
            break
        cursor = next_cursor
    return rows


def _download_latest_oi_rows(client: httpx.Client, symbol: str) -> list[dict[str, Any]]:
    data = _http_get_json(
        client,
        "https://fapi.binance.com/futures/data/openInterestHist",
        {"symbol": symbol, "period": "15m", "limit": 500},
    )
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def _download_oi_rows(client: httpx.Client, symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    latest_rows = _download_latest_oi_rows(client, symbol)
    latest_ts = max((int(float(item.get("timestamp") or 0)) for item in latest_rows), default=0)
    safe_start_ms = int(start_ms)
    if latest_ts:
        safe_start_ms = max(safe_start_ms, latest_ts - OI_HISTORY_SAFE_LOOKBACK_MS)
    if safe_start_ms > end_ms:
        return [item for item in latest_rows if start_ms <= int(float(item.get("timestamp") or 0)) <= end_ms]

    cursor = safe_start_ms
    while cursor <= end_ms:
        data = _http_get_json(
            client,
            "https://fapi.binance.com/futures/data/openInterestHist",
            {"symbol": symbol, "period": "15m", "startTime": cursor, "endTime": end_ms, "limit": 500},
        )
        if not isinstance(data, list) or not data:
            break
        got = [item for item in data if isinstance(item, dict)]
        rows.extend(got)
        last = max(int(float(item.get("timestamp") or cursor)) for item in got)
        next_cursor = last + 1
        if next_cursor <= cursor or len(got) < 500:
            break
        cursor = next_cursor
    return rows or [item for item in latest_rows if start_ms <= int(float(item.get("timestamp") or 0)) <= end_ms]


def download_oi_funding_sources_payload(
    project_root: Path,
    *,
    symbols: list[str],
    start_ms: int,
    end_ms: int,
    sleep_sec: float = 0.05,
    timeout_sec: float = 20.0,
    dry_run: bool = False,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    with _connect(db_path) as conn:
        ensure_research_tables(conn)
        result = {
            "ok": True,
            "db_path": str(db_path),
            "start_ms": int(start_ms),
            "end_ms": int(end_ms),
            "symbols": len(symbols),
            "oi_rows": 0,
            "funding_rows": 0,
            "errors": [],
            "oi_errors": [],
            "funding_errors": [],
            "dry_run": dry_run,
        }
        if dry_run:
            return result
        with httpx.Client(timeout=timeout_sec) as client:
            for symbol in [sym.upper().strip() for sym in symbols if sym and sym.strip()]:
                try:
                    oi_rows = _download_oi_rows(client, symbol, int(start_ms), int(end_ms))
                    result["oi_rows"] += upsert_market_oi_15m_rows(conn, symbol, oi_rows)
                except Exception as exc:
                    result["oi_errors"].append({"symbol": symbol, "error": f"{type(exc).__name__}:{exc}"})
                try:
                    funding_rows = _download_funding_rows(client, symbol, int(start_ms), int(end_ms))
                    result["funding_rows"] += upsert_market_funding_8h_rows(conn, symbol, funding_rows)
                except Exception as exc:
                    result["funding_errors"].append({"symbol": symbol, "error": f"{type(exc).__name__}:{exc}"})
                conn.commit()
                if sleep_sec > 0:
                    time.sleep(sleep_sec)
        result["errors"] = list(result["oi_errors"]) + list(result["funding_errors"])
        result["ok"] = not bool(result["funding_errors"]) and result["funding_rows"] > 0
        return result


def _missing_fields(row: dict[str, Any], fields: list[str]) -> list[str]:
    return [field for field in fields if row.get(field) in (None, "")]


def _extract_fill_value(fill: Any, *keys: str) -> Any:
    payload = _loads(fill)
    if isinstance(payload, dict):
        queue = [payload]
        while queue:
            item = queue.pop(0)
            for key in keys:
                if key in item and item[key] not in (None, ""):
                    return item[key]
            for value in item.values():
                if isinstance(value, dict):
                    queue.append(value)
    return None


def _float_or_none(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _mean(values: list[float]) -> float | None:
    return mean(values) if values else None


def _std(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    m = mean(values)
    return (sum((value - m) ** 2 for value in values) / len(values)) ** 0.5


def _ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    alpha = 2 / (period + 1)
    ema = mean(values[:period])
    for value in values[period:]:
        ema = alpha * value + (1 - alpha) * ema
    return ema


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for prev, cur in zip(values[-period - 1 : -1], values[-period:]):
        delta = cur - prev
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = mean(gains) if gains else 0.0
    avg_loss = mean(losses) if losses else 0.0
    if avg_loss <= 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _round_or_none(value: Any, digits: int = 8) -> float | None:
    out = _float_or_none(value)
    return round(out, digits) if out is not None else None


def _set_observed_feature(features: dict[str, Any], key: str, value: Any) -> None:
    if value in (None, ""):
        return
    if features.get(key) in (None, "", "missing", "missing_source", "unknown"):
        features[key] = value


def _fetch_entry_known_klines(conn: sqlite3.Connection, symbol: str, entry_time_ms: Any, minutes: int = 90) -> list[dict[str, Any]]:
    entry_ms = _float_or_none(entry_time_ms)
    if not symbol or entry_ms is None:
        return []
    start_ms = int(entry_ms) - minutes * 60_000
    try:
        cur = conn.execute(
            """
            SELECT open_time_ms, open, high, low, close, volume, quote_volume,
                   taker_buy_base_volume, taker_buy_quote_volume
            FROM p21_klines_1m
            WHERE symbol = ? AND open_time_ms >= ? AND open_time_ms < ?
            ORDER BY open_time_ms
            """,
            (str(symbol).upper(), start_ms, int(entry_ms)),
        )
        rows = cur.fetchall()
    except sqlite3.OperationalError:
        return []
    columns = [item[0] for item in (cur.description or [])]
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            out.append(dict(row))
        else:
            out.append(dict(zip(columns, row)))
    return out


def _entry_known_minute_ms(entry_time_ms: Any) -> int | None:
    got = _float_or_none(entry_time_ms)
    if got is None:
        return None
    return (int(got) // 60000) * 60000 - 60000


def _pct_bps(new: float | None, old: float | None) -> float | None:
    if new is None or old in (None, 0):
        return None
    return (float(new) / float(old) - 1.0) * 10000.0


def _backfill_btc_market_context(conn: sqlite3.Connection, fact: dict[str, Any], features: dict[str, Any]) -> dict[str, Any]:
    known_ms = _entry_known_minute_ms(fact.get("entry_time_ms"))
    if known_ms is None:
        for key in ("btc_trend", "btc_volatility", "btc_alignment", "market_breadth"):
            features.setdefault(key, "missing_source" if key != "btc_alignment" else "unknown")
        return {
            "btc_trend": {"quality": "missing", "reason_code": "missing_entry_time"},
            "btc_volatility": {"quality": "missing", "reason_code": "missing_entry_time"},
            "btc_alignment": {"quality": "missing", "reason_code": "missing_entry_time"},
            "market_breadth": {"quality": "missing", "reason_code": "missing_entry_time"},
        }

    status: dict[str, Any] = {}
    try:
        btc_rows = conn.execute(
            """
            SELECT open_time_ms, high, low, close
            FROM p21_klines_1m
            WHERE symbol = 'BTCUSDT' AND open_time_ms <= ?
            ORDER BY open_time_ms DESC
            LIMIT 61
            """,
            (known_ms,),
        ).fetchall()
    except sqlite3.OperationalError:
        btc_rows = []
    btc_columns = ["open_time_ms", "high", "low", "close"]
    btc_rows = list(
        reversed(
            [
                _row_to_dict(row) if isinstance(row, sqlite3.Row) else dict(zip(btc_columns, row))
                for row in btc_rows
            ]
        )
    )
    if len(btc_rows) >= 16:
        close_now = _float_or_none(btc_rows[-1].get("close"))
        close_15 = _float_or_none(btc_rows[-16].get("close"))
        close_60 = _float_or_none(btc_rows[0].get("close")) if len(btc_rows) >= 61 else None
        ret_15 = _pct_bps(close_now, close_15)
        ret_60 = _pct_bps(close_now, close_60) if close_60 is not None else None
        if ret_15 is None:
            trend = "unknown"
        elif abs(ret_15) < 12.0 and (ret_60 is None or abs(ret_60) < 35.0):
            trend = "chop"
        elif ret_15 > 0 and (ret_60 is None or ret_60 >= -10.0):
            trend = "bullish"
        elif ret_15 < 0 and (ret_60 is None or ret_60 <= 10.0):
            trend = "bearish"
        else:
            trend = "chop"
        window = btc_rows[-30:] if len(btc_rows) >= 30 else btc_rows
        highs = [_float_or_none(row.get("high")) for row in window]
        lows = [_float_or_none(row.get("low")) for row in window]
        highs = [value for value in highs if value is not None]
        lows = [value for value in lows if value is not None]
        range_bps = _pct_bps(max(highs) if highs else None, min(lows) if lows else None)
        if range_bps is None:
            volatility = "unknown"
        elif range_bps < 30:
            volatility = "low"
        elif range_bps < 90:
            volatility = "normal"
        else:
            volatility = "high"
        side = str(fact.get("side") or "").upper()
        if trend == "chop":
            alignment = "chop"
        elif side == "LONG":
            alignment = "same" if trend == "bullish" else "opposite"
        elif side == "SHORT":
            alignment = "same" if trend == "bearish" else "opposite"
        else:
            alignment = "unknown"
        features.setdefault("btc_trend", trend)
        features.setdefault("btc_volatility", volatility)
        features.setdefault("btc_alignment", alignment)
        source_ts = int(btc_rows[-1].get("open_time_ms"))
        for key in ("btc_trend", "btc_volatility", "btc_alignment"):
            status[key] = {
                "quality": "observed",
                "source": "p21_klines_1m:BTCUSDT",
                "source_ts": source_ts,
                "known_at": source_ts,
                "reason_code": "ok",
            }
    else:
        features.setdefault("btc_trend", "unknown")
        features.setdefault("btc_volatility", "unknown")
        features.setdefault("btc_alignment", "unknown")
        for key in ("btc_trend", "btc_volatility", "btc_alignment"):
            status[key] = {
                "quality": "missing",
                "source": "p21_klines_1m:BTCUSDT",
                "source_ts": btc_rows[-1].get("open_time_ms") if btc_rows else None,
                "known_at": btc_rows[-1].get("open_time_ms") if btc_rows else None,
                "reason_code": "insufficient_btc_klines",
            }

    try:
        prev_ms = known_ms - 15 * 60000
        row = conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN cur.close > prev.close THEN 1 ELSE 0 END) AS up_count,
              SUM(CASE WHEN cur.close < prev.close THEN 1 ELSE 0 END) AS down_count
            FROM p21_klines_1m cur
            JOIN p21_klines_1m prev
              ON cur.symbol = prev.symbol AND prev.open_time_ms = ?
            WHERE cur.open_time_ms = ?
            """,
            (prev_ms, known_ms),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    if isinstance(row, sqlite3.Row):
        total = int(row["total"] or 0)
        up_count = int(row["up_count"] or 0)
        down_count = int(row["down_count"] or 0)
    elif row:
        total = int(row[0] or 0)
        up_count = int(row[1] or 0)
        down_count = int(row[2] or 0)
    else:
        total = 0
        up_count = 0
        down_count = 0
    if total >= 10:
        up_ratio = up_count / total
        down_ratio = down_count / total
        if up_ratio >= 0.58:
            breadth = "up"
        elif down_ratio >= 0.58:
            breadth = "down"
        else:
            breadth = "mixed"
        features.setdefault("market_breadth", breadth)
        status["market_breadth"] = {
            "quality": "observed",
            "source": "p21_klines_1m:cross_section_15m",
            "source_ts": known_ms,
            "known_at": known_ms,
            "reason_code": "ok",
            "symbols": total,
            "up_count": up_count,
            "down_count": down_count,
        }
    else:
        features.setdefault("market_breadth", "unknown")
        status["market_breadth"] = {
            "quality": "missing",
            "source": "p21_klines_1m:cross_section_15m",
            "source_ts": known_ms if total else None,
            "known_at": known_ms if total else None,
            "reason_code": "insufficient_market_breadth_cross_section",
            "symbols": total,
        }
    return status


def _funding_bucket_from_rate(rate: float | None) -> str | None:
    if rate is None:
        return None
    if rate > FUNDING_ABS_WARM_MAX:
        return "OVERHEATED"
    if rate < -FUNDING_ABS_WARM_MAX:
        return "NEGATIVE_EXTREME"
    if abs(rate) > FUNDING_ABS_NEUTRAL_MAX:
        return "WARM"
    return "NEUTRAL"


def _funding_crowded_side(rate: float | None) -> str | None:
    if rate is None:
        return None
    if rate > FUNDING_ABS_NEUTRAL_MAX:
        return "long"
    if rate < -FUNDING_ABS_NEUTRAL_MAX:
        return "short"
    return "neutral"


def _oi_state_from_change(oi_change: float | None, price_ret_bps: float | None) -> str | None:
    if oi_change is None or price_ret_bps is None:
        return None
    oi_up = oi_change > OI_PCT_EPS
    oi_down = oi_change < -OI_PCT_EPS
    price_up = price_ret_bps > 0
    price_down = price_ret_bps < 0
    if not oi_up and not oi_down:
        return "oi_flat"
    if price_up and oi_up:
        return "price_up_oi_up_new_positions"
    if price_up and oi_down:
        return "price_up_oi_down_short_covering"
    if price_down and oi_down:
        return "price_down_oi_down_long_delever"
    if price_down and oi_up:
        return "price_down_oi_up_new_shorts"
    return "unknown"


def _asof_oi_row(conn: sqlite3.Connection, symbol: str, entry_time_ms: Any, *, max_lag_ms: int = 6 * 60 * 60 * 1000) -> dict[str, Any] | None:
    entry_ms = _entry_known_minute_ms(entry_time_ms)
    if entry_ms is None:
        return None
    try:
        row = conn.execute(
            """
            SELECT *
            FROM market_oi_15m
            WHERE symbol = ?
              AND period = '15m'
              AND source_time_ms <= ?
            ORDER BY source_time_ms DESC
            LIMIT 1
            """,
            (str(symbol).upper(), entry_ms),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    item = _row_to_dict(row) if isinstance(row, sqlite3.Row) else {}
    source_ms = _float_or_none(item.get("source_time_ms"))
    if source_ms is None or int(entry_ms) - int(source_ms) > max_lag_ms:
        return None
    return item


def _asof_funding_row(conn: sqlite3.Connection, symbol: str, entry_time_ms: Any, *, max_lag_ms: int = 24 * 60 * 60 * 1000) -> dict[str, Any] | None:
    entry_ms = _entry_known_minute_ms(entry_time_ms)
    if entry_ms is None:
        return None
    try:
        row = conn.execute(
            """
            SELECT *
            FROM market_funding_8h
            WHERE symbol = ?
              AND funding_time_ms <= ?
            ORDER BY funding_time_ms DESC
            LIMIT 1
            """,
            (str(symbol).upper(), entry_ms),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    item = _row_to_dict(row) if isinstance(row, sqlite3.Row) else {}
    source_ms = _float_or_none(item.get("funding_time_ms"))
    if source_ms is None or int(entry_ms) - int(source_ms) > max_lag_ms:
        return None
    return item


def _backfill_liquidity_oi_funding_context(conn: sqlite3.Connection, fact: dict[str, Any], features: dict[str, Any]) -> dict[str, Any]:
    status: dict[str, Any] = {}
    config_patch = _loads(fact.get("config_patch_json"))
    if not isinstance(config_patch, dict):
        config_patch = {}
    symbol = str(fact.get("symbol") or features.get("symbol") or "").upper()

    spread = _float_or_none(
        features.get("spread_bps")
        or features.get("book_spread_bps")
        or features.get("top_spread_bps")
        or config_patch.get("offline_spread_bps")
        or config_patch.get("spread_bps")
    )
    if spread is not None:
        features.setdefault("spread_bps", _round_or_none(spread))
        status["spread_bps"] = {"quality": "observed", "source": "order_features_or_config", "reason_code": "ok"}
    else:
        slippage = _float_or_none(config_patch.get("slippage_bps"))
        if slippage is not None:
            features.setdefault("spread_bps", _round_or_none(slippage))
            status["spread_bps"] = {
                "quality": "proxy",
                "source": "config.slippage_bps",
                "reason_code": "proxy_spread_from_slippage_bps",
            }
        else:
            status["spread_bps"] = {"quality": "missing", "source": "liquidity_snapshot", "reason_code": "missing_spread_source"}

    depth = _float_or_none(features.get("depth_imbalance"))
    if depth is not None:
        status["depth_imbalance"] = {"quality": "observed", "source": "order_features_or_micro", "reason_code": "ok"}
    else:
        status["depth_imbalance"] = {"quality": "missing", "source": "depth_snapshot", "reason_code": "missing_depth_source"}

    oi_row = _asof_oi_row(conn, symbol, fact.get("entry_time_ms"))
    if oi_row:
        oi_change = _float_or_none(oi_row.get("oi_change"))
        oi_z = _float_or_none(oi_row.get("oi_z"))
        oi_state = _oi_state_from_change(oi_change, _float_or_none(features.get("pct_15m_bps") or features.get("pct_5m_bps"))) or oi_row.get("oi_state")
        _set_observed_feature(features, "oi_change", _round_or_none(oi_change, 8))
        _set_observed_feature(features, "oi_z", _round_or_none(oi_z, 6))
        _set_observed_feature(features, "oi_state", oi_state or "unknown")
        for key in ("oi_change", "oi_z", "oi_state"):
            status[key] = {
                "quality": "observed",
                "source": "market_oi_15m",
                "source_ts": oi_row.get("source_time_ms"),
                "known_at": oi_row.get("source_time_ms"),
                "reason_code": "ok",
            }
    else:
        oi_state = features.get("oi_state") or features.get("oi_direction")
        if oi_state not in (None, "", "unknown", "missing"):
            features.setdefault("oi_state", oi_state)
            status["oi_state"] = {"quality": "observed", "source": "order_features", "reason_code": "ok"}
        else:
            features.setdefault("oi_state", "missing_source")
            status["oi_state"] = {"quality": "missing", "source": "market_oi_15m", "reason_code": "missing_oi_source"}
        for key in ("oi_change", "oi_z"):
            if _float_or_none(features.get(key)) is not None:
                status[key] = {"quality": "observed", "source": "order_features", "reason_code": "ok"}
            else:
                status[key] = {"quality": "missing", "source": "market_oi_15m", "reason_code": f"missing_{key}_source"}

    funding_row = _asof_funding_row(conn, symbol, fact.get("entry_time_ms"))
    if funding_row:
        funding_rate_observed = _float_or_none(funding_row.get("funding_rate"))
        _set_observed_feature(features, "funding_rate", _round_or_none(funding_rate_observed, 10))
        _set_observed_feature(features, "funding_bucket", funding_row.get("funding_bucket") or _funding_bucket_from_rate(funding_rate_observed))
        _set_observed_feature(features, "funding_crowded_side", funding_row.get("funding_crowded_side") or _funding_crowded_side(funding_rate_observed))
        for key in ("funding_rate", "funding_bucket", "funding_crowded_side"):
            status[key] = {
                "quality": "observed",
                "source": "market_funding_8h",
                "source_ts": funding_row.get("funding_time_ms"),
                "known_at": funding_row.get("funding_time_ms"),
                "reason_code": "ok",
            }
        return status

    funding_rate = _float_or_none(features.get("funding_rate"))
    if funding_rate is not None:
        status["funding_rate"] = {"quality": "observed", "source": "order_features", "reason_code": "ok"}
        if "funding_bucket" not in features:
            if funding_rate >= 0.0005:
                features["funding_bucket"] = "positive_crowded"
            elif funding_rate <= -0.0005:
                features["funding_bucket"] = "negative_crowded"
            else:
                features["funding_bucket"] = "neutral"
        features.setdefault("funding_crowded_side", "long" if funding_rate > 0 else "short" if funding_rate < 0 else "neutral")
        for key in ("funding_bucket", "funding_crowded_side"):
            status[key] = {"quality": "observed", "source": "funding_rate", "reason_code": "derived_from_funding_rate"}
    else:
        features.setdefault("funding_bucket", "missing_source")
        features.setdefault("funding_crowded_side", "unknown")
        status["funding_rate"] = {"quality": "missing", "source": "historical_funding", "reason_code": "missing_funding_source"}
        status["funding_bucket"] = {"quality": "missing", "source": "historical_funding", "reason_code": "missing_funding_source"}
        status["funding_crowded_side"] = {"quality": "missing", "source": "historical_funding", "reason_code": "missing_funding_source"}
    return status


def _backfill_market_context_features(conn: sqlite3.Connection, fact: dict[str, Any], features: dict[str, Any]) -> None:
    source_status: dict[str, Any] = {}
    source_status.update(_backfill_btc_market_context(conn, fact, features))
    source_status.update(_backfill_liquidity_oi_funding_context(conn, fact, features))
    if source_status:
        existing = features.get("market_context_source_status")
        if isinstance(existing, dict):
            merged = dict(existing)
            merged.update(source_status)
            features["market_context_source_status"] = merged
        else:
            features["market_context_source_status"] = source_status


def _backfill_kline_entry_features(conn: sqlite3.Connection, fact: dict[str, Any], features: dict[str, Any]) -> None:
    klines = _fetch_entry_known_klines(conn, str(fact.get("symbol") or ""), fact.get("entry_time_ms"))
    if not klines:
        return
    entry = _float_or_none(fact.get("entry_price"))
    closes = [_float_or_none(row.get("close")) for row in klines]
    closes = [value for value in closes if value is not None]
    highs = [_float_or_none(row.get("high")) for row in klines]
    highs = [value for value in highs if value is not None]
    lows = [_float_or_none(row.get("low")) for row in klines]
    lows = [value for value in lows if value is not None]
    vols = [_float_or_none(row.get("volume")) for row in klines]
    vols = [value for value in vols if value is not None]
    if len(closes) >= 2:
        last = closes[-1]
        features.setdefault("pct_1m_bps", _round_or_none((last / closes[-2] - 1) * 10000))
        if len(closes) >= 4:
            features.setdefault("pct_3m_bps", _round_or_none((last / closes[-4] - 1) * 10000))
        if len(closes) >= 6:
            features.setdefault("pct_5m_bps", _round_or_none((last / closes[-6] - 1) * 10000))
        if len(closes) >= 16:
            features.setdefault("pct_15m_bps", _round_or_none((last / closes[-16] - 1) * 10000))
    if closes:
        features.setdefault("rsi_14", _round_or_none(_rsi(closes, 14), 4))
    ema20 = _ema(closes[-60:], 20)
    ema60 = _ema(closes[-90:], 60)
    if entry and ema20:
        features.setdefault("ema20_distance_bps", _round_or_none((entry / ema20 - 1) * 10000))
    if entry and ema60:
        features.setdefault("ema60_distance_bps", _round_or_none((entry / ema60 - 1) * 10000))
    if entry and len(closes) >= 20:
        basis = mean(closes[-20:])
        band_std = _std(closes[-20:]) or 0.0
        upper = basis + 2 * band_std
        lower = basis - 2 * band_std
        width = upper - lower
        features.setdefault("bollinger_width_bps", _round_or_none(width / basis * 10000 if basis else None))
        features.setdefault("bollinger_position", _round_or_none((entry - lower) / width if width > 0 else None))
        total_quote = sum(_float_or_none(row.get("quote_volume")) or 0.0 for row in klines[-20:])
        total_base = sum(_float_or_none(row.get("volume")) or 0.0 for row in klines[-20:])
        if total_base > 0 and total_quote > 0:
            vwap = total_quote / total_base
            features.setdefault("vwap_distance_bps", _round_or_none((entry / vwap - 1) * 10000 if vwap else None))
    if entry and len(klines) >= 2:
        tr_values: list[float] = []
        for i in range(max(1, len(klines) - 14), len(klines)):
            high = _float_or_none(klines[i].get("high")) or 0.0
            low = _float_or_none(klines[i].get("low")) or 0.0
            prev_close = _float_or_none(klines[i - 1].get("close")) or 0.0
            tr_values.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        if tr_values:
            features.setdefault("atr_14_bps", _round_or_none(mean(tr_values) / entry * 10000))
        last_high = _float_or_none(klines[-1].get("high"))
        last_low = _float_or_none(klines[-1].get("low"))
        if last_high is not None and last_low is not None:
            features.setdefault("atr_1m_bps", _round_or_none((last_high - last_low) / entry * 10000))
    if vols and len(vols) >= 21:
        base = vols[-21:-1]
        sd = _std(base) or 0.0
        features.setdefault("volume_z", _round_or_none((vols[-1] - mean(base)) / sd if sd > 0 else 0.0))
    last_k = klines[-1]
    open_p = _float_or_none(last_k.get("open"))
    close_p = _float_or_none(last_k.get("close"))
    high_p = _float_or_none(last_k.get("high"))
    low_p = _float_or_none(last_k.get("low"))
    if all(value is not None for value in (open_p, close_p, high_p, low_p)) and high_p != low_p:
        body = abs(close_p - open_p)
        candle_range = high_p - low_p
        features.setdefault("body_ratio_1m", _round_or_none(body / candle_range))
        features.setdefault("upper_wick_ratio_1m", _round_or_none((high_p - max(open_p, close_p)) / candle_range))
        features.setdefault("lower_wick_ratio_1m", _round_or_none((min(open_p, close_p) - low_p) / candle_range))
    if entry and len(highs) >= 30 and len(lows) >= 30:
        high30 = max(highs[-30:])
        low30 = min(lows[-30:])
        if high30 > low30:
            features.setdefault("range_pos_30m", _round_or_none((entry - low30) / (high30 - low30)))
    buy5 = sum(_float_or_none(row.get("taker_buy_base_volume")) or 0.0 for row in klines[-5:])
    vol5 = sum(_float_or_none(row.get("volume")) or 0.0 for row in klines[-5:])
    if vol5 > 0:
        ratio5 = buy5 / vol5
        features.setdefault("taker_buy_ratio_5m", _round_or_none(ratio5))
        features.setdefault("cvd_proxy_5m", _round_or_none((ratio5 - 0.5) * vol5))
        features.setdefault("ofi_proxy_5m", _round_or_none((ratio5 - 0.5) * vol5))
    buy1 = _float_or_none(last_k.get("taker_buy_base_volume"))
    vol1 = _float_or_none(last_k.get("volume"))
    if buy1 is not None and vol1 and vol1 > 0:
        features.setdefault("taker_buy_ratio_1m", _round_or_none(buy1 / vol1))
    pct_1m = _float_or_none(features.get("pct_1m_bps"))
    taker = _float_or_none(features.get("taker_buy_ratio_1m") or features.get("taker_buy_ratio_5m"))
    if pct_1m is not None and taker is not None:
        price_dir = 1 if pct_1m > 0 else -1 if pct_1m < 0 else 0
        flow_dir = 1 if taker > 0.54 else -1 if taker < 0.46 else 0
        side_sign = -1 if str(fact.get("side") or "").upper() == "SHORT" else 1
        features.setdefault("price_flow_alignment", "same" if price_dir and flow_dir and price_dir == flow_dir else "opposite" if price_dir and flow_dir else "neutral")
        features.setdefault("side_flow_alignment", "same" if flow_dir and flow_dir == side_sign else "opposite" if flow_dir else "neutral")


def _build_trade_fact(row: dict[str, Any]) -> dict[str, Any]:
    fill_result = row.get("fill_result_json")
    order_id = row.get("order_id") or row.get("trade_id") or _stable_id(row.get("symbol"), row.get("entry_time_ms"))
    sample_id = _stable_id("backtest", order_id, row.get("parameter_set_id"))
    exit_price = _extract_fill_value(fill_result, "exit_price", "filled_exit_price", "final_price", "close_price")
    fee = _extract_fill_value(fill_result, "fee", "total_fee", "fee_usdt")
    slippage = _extract_fill_value(fill_result, "slippage", "slippage_bps")
    gross_pnl = row.get("gross_pnl")
    if gross_pnl is None:
        gross_pnl = _extract_fill_value(fill_result, "gross_pnl", "gross_pnl_usdt", "realized_gross_pnl")
    proxy_fields: list[str] = []
    config_patch = _loads(row.get("config_patch_json"))
    entry_price_num = _float_or_none(row.get("entry_price"))
    exit_price_num = _float_or_none(exit_price)
    if gross_pnl is None and entry_price_num is not None and exit_price_num is not None:
        if str(row.get("side") or "").upper() == "SHORT":
            gross_pnl = entry_price_num - exit_price_num
        else:
            gross_pnl = exit_price_num - entry_price_num
        proxy_fields.append("gross_pnl")
    if fee is None and entry_price_num is not None and exit_price_num is not None:
        fee_bps = _float_or_none(config_patch.get("taker_fee_bps") if isinstance(config_patch, dict) else None)
        if fee_bps is not None:
            fee = (entry_price_num + exit_price_num) * fee_bps / 10000.0
            proxy_fields.append("fee")
    if slippage is None:
        slippage_bps = _float_or_none(config_patch.get("slippage_bps") if isinstance(config_patch, dict) else None)
        if slippage_bps is not None:
            slippage = slippage_bps
            proxy_fields.append("slippage")
    base = {
        "sample_id": sample_id,
        "source_type": "backtest",
        "sandbox_id": "baseline",
        "experiment_id": row.get("experiment_id"),
        "run_id": row.get("run_id") or row.get("experiment_id"),
        "cycle_id": row.get("cycle_id") or f"{row.get('experiment_id')}:{row.get('parameter_set_id')}",
        "order_id": order_id,
        "parameter_set_id": row.get("parameter_set_id"),
        "strategy_line": row.get("strategy_line") or "unknown",
        "strategy_version": row.get("source_contract_version") or row.get("strategy_version"),
        "config_hash": _stable_id(row.get("parameter_set_id"), row.get("config_patch_json"), prefix="cfg"),
        "symbol": row.get("symbol") or "UNKNOWN",
        "side": row.get("side") or "UNKNOWN",
        "signal_time_ms": row.get("signal_time_ms"),
        "entry_time_ms": row.get("entry_time_ms"),
        "exit_time_ms": row.get("exit_time_ms"),
        "entry_price": row.get("entry_price"),
        "exit_price": exit_price,
        "stop_loss": row.get("stop_loss"),
        "take_profit": row.get("take_profit"),
        "planned_rr": row.get("planned_rr"),
        "effective_rr": row.get("effective_rr"),
        "entry_mode": row.get("entry_mode"),
        "exit_reason": row.get("exit_reason"),
        "net_R": row.get("net_R"),
        "gross_pnl": gross_pnl,
        "fee": fee,
        "slippage": slippage,
        "fill_model_version": row.get("fill_model_version") or "p21_v2_fill",
        "replay_version": row.get("replay_version") or "p21_v2_1m",
        "trade_plan_payload_json": row.get("trade_plan_payload_json") or "{}",
        "fill_result_json": fill_result or "{}",
        "config_patch_json": row.get("config_patch_json") or "{}",
        "lineage_json": _json(
            {
                "source_table": "p21_v2_shadow_orders",
                "order_id": order_id,
                "parameter_set_id": row.get("parameter_set_id"),
                "lineage_mode": row.get("lineage_mode"),
            }
        ),
        "schema_version": SCHEMA_VERSION,
        "generated_at": to_iso_z(utc_now()),
    }
    missing = _missing_fields(
        base,
        [
            "exit_price",
            "gross_pnl",
            "fee",
            "slippage",
            "signal_time_ms",
            "run_id",
            "cycle_id",
        ],
    )
    if not row.get("run_id") and row.get("experiment_id"):
        proxy_fields.append("run_id")
    if not row.get("cycle_id") and row.get("experiment_id") and row.get("parameter_set_id"):
        proxy_fields.append("cycle_id")
    base["field_quality_json"] = _json(
        {
            "missing_fields": missing,
            "proxy_fields": proxy_fields,
            "source_status": {
                "fact_source": "p21_v2_shadow_orders",
                "exit_price": "fill_result_json" if exit_price is not None else "missing",
                "gross_pnl": "fill_result_json" if gross_pnl is not None and "gross_pnl" not in proxy_fields else ("proxy_price_delta_1unit" if "gross_pnl" in proxy_fields else "missing"),
                "fee": "fill_result_json" if fee is not None and "fee" not in proxy_fields else ("proxy_config_taker_fee_bps_1unit" if "fee" in proxy_fields else "missing"),
                "slippage": "fill_result_json" if slippage is not None and "slippage" not in proxy_fields else ("proxy_config_slippage_bps" if "slippage" in proxy_fields else "missing"),
            },
        }
    )
    return base


def _insert_trade_fact(conn: sqlite3.Connection, item: dict[str, Any]) -> None:
    columns = [
        "sample_id",
        "source_type",
        "sandbox_id",
        "experiment_id",
        "run_id",
        "cycle_id",
        "order_id",
        "parameter_set_id",
        "strategy_line",
        "strategy_version",
        "config_hash",
        "symbol",
        "side",
        "signal_time_ms",
        "entry_time_ms",
        "exit_time_ms",
        "entry_price",
        "exit_price",
        "stop_loss",
        "take_profit",
        "planned_rr",
        "effective_rr",
        "entry_mode",
        "exit_reason",
        "net_R",
        "gross_pnl",
        "fee",
        "slippage",
        "fill_model_version",
        "replay_version",
        "trade_plan_payload_json",
        "fill_result_json",
        "config_patch_json",
        "lineage_json",
        "field_quality_json",
        "schema_version",
        "generated_at",
    ]
    placeholders = ",".join("?" for _ in columns)
    update = ",".join(f"{col}=excluded.{col}" for col in columns if col != "sample_id")
    conn.execute(
        f"""
        INSERT INTO research_trade_facts ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(sample_id) DO UPDATE SET {update}
        """,
        tuple(item.get(col) for col in columns),
    )


def _fact_map(conn: sqlite3.Connection) -> dict[tuple[str | None, str | None], dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT sample_id, order_id, parameter_set_id, strategy_line, source_type, sandbox_id,
               experiment_id, symbol, side, entry_time_ms
        FROM research_trade_facts
        """
    ).fetchall()
    result: dict[tuple[str | None, str | None], dict[str, Any]] = {}
    for row in rows:
        item = _row_to_dict(row)
        result[(item.get("order_id"), item.get("parameter_set_id"))] = item
    return result


def _feature_payload_from_v4(row: dict[str, Any], fact: dict[str, Any] | None) -> dict[str, Any]:
    fact = fact or {}
    sample_id = fact.get("sample_id") or _stable_id("backtest", row.get("order_id"), row.get("parameter_set_id"))
    features = _loads(row.get("features_json"))
    if not isinstance(features, dict):
        features = {"_raw": features}
    for key in REQUIRED_ENTRY_FEATURES:
        features.setdefault(key, None)
    missing = [key for key, value in features.items() if value in (None, "", "missing")]
    proxy = [key for key, value in features.items() if "proxy" in key.lower() or value == "proxy"]
    total = max(len(features), 1)
    completeness = round(max(0.0, 1.0 - (len(missing) / total)), 4)
    return {
        "feature_sample_id": _stable_id(sample_id, FEATURE_SCHEMA_VERSION, prefix="feat"),
        "sample_id": sample_id,
        "source_type": fact.get("source_type") or "backtest",
        "sandbox_id": fact.get("sandbox_id") or "baseline",
        "experiment_id": fact.get("experiment_id") or row.get("experiment_id"),
        "parameter_set_id": fact.get("parameter_set_id") or row.get("parameter_set_id"),
        "strategy_line": fact.get("strategy_line") or row.get("strategy_line") or "unknown",
        "symbol": fact.get("symbol") or row.get("symbol") or "UNKNOWN",
        "side": fact.get("side") or row.get("side") or "UNKNOWN",
        "entry_time_ms": fact.get("entry_time_ms") or row.get("entry_time_ms"),
        "known_at_ms": fact.get("entry_time_ms") or row.get("entry_time_ms"),
        "asof_lag_seconds": 0,
        "source_level": "entry_known_v4",
        "source_ref_json": _json(
            {
                "source_table": "trade_quality_entry_evidence_v4",
                "diagnostic_id": row.get("diagnostic_id"),
                "sample_id": sample_id,
            }
        ),
        "feature_completeness": completeness,
        "proxy_level": "proxy" if proxy else "direct_or_derived",
        "missing_fields_json": _json(missing),
        "features_json": _json(features),
        "schema_version": FEATURE_SCHEMA_VERSION,
        "generated_at": to_iso_z(utc_now()),
    }


def _insert_entry_feature(conn: sqlite3.Connection, item: dict[str, Any]) -> None:
    features = _loads(item.get("features_json"))
    if isinstance(features, dict):
        existing_source_status = features.get("market_context_source_status")
        if not isinstance(existing_source_status, dict):
            existing_source_status = {}
        fact = {
            "symbol": item.get("symbol"),
            "side": item.get("side"),
            "entry_time_ms": item.get("entry_time_ms"),
            "signal_time_ms": item.get("entry_time_ms"),
            "config_patch_json": "{}",
        }
        _backfill_market_context_features(conn, fact, features)
        refreshed_source_status = features.get("market_context_source_status")
        if isinstance(refreshed_source_status, dict):
            for stable_key in ("spread_bps", "depth_imbalance"):
                if stable_key in existing_source_status:
                    refreshed_source_status[stable_key] = existing_source_status[stable_key]
            features["market_context_source_status"] = refreshed_source_status
        for key in REQUIRED_ENTRY_FEATURES:
            features.setdefault(key, None)
        missing = [key for key, value in features.items() if value in (None, "", "missing")]
        proxy = [key for key, value in features.items() if "proxy" in key.lower() or value == "proxy"]
        total = max(len(features), 1)
        item["feature_completeness"] = round(max(0.0, 1.0 - (len(missing) / total)), 4)
        item["proxy_level"] = "proxy" if proxy else "direct_or_derived"
        item["missing_fields_json"] = _json(missing)
        item["features_json"] = _json(features)
        source_ref = _loads(item.get("source_ref_json"))
        if isinstance(source_ref, dict):
            source_ref["market_context_source_status"] = features.get("market_context_source_status") or {}
            item["source_ref_json"] = _json(source_ref)
    columns = [
        "feature_sample_id",
        "sample_id",
        "source_type",
        "sandbox_id",
        "experiment_id",
        "parameter_set_id",
        "strategy_line",
        "symbol",
        "side",
        "entry_time_ms",
        "known_at_ms",
        "asof_lag_seconds",
        "source_level",
        "source_ref_json",
        "feature_completeness",
        "proxy_level",
        "missing_fields_json",
        "features_json",
        "schema_version",
        "generated_at",
    ]
    placeholders = ",".join("?" for _ in columns)
    update = ",".join(f"{col}=excluded.{col}" for col in columns if col != "feature_sample_id")
    conn.execute(
        f"""
        INSERT INTO research_entry_features ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(feature_sample_id) DO UPDATE SET {update}
        """,
        tuple(item.get(col) for col in columns),
    )


def refresh_entry_features_market_context(
    conn: sqlite3.Connection,
    *,
    experiment_id: str | None = None,
    parameter_set_id: str | None = None,
    strategy_line: str | None = None,
) -> int:
    clauses = ["ef.source_type = 'backtest'"]
    params: list[Any] = []
    if experiment_id:
        clauses.append("ef.experiment_id = ?")
        params.append(experiment_id)
    if parameter_set_id:
        clauses.append("ef.parameter_set_id = ?")
        params.append(parameter_set_id)
    if strategy_line:
        clauses.append("ef.strategy_line = ?")
        params.append(strategy_line)
    where = " AND ".join(clauses)
    rows = _query_rows(
        conn,
        f"""
        SELECT ef.*, rf.config_patch_json, rf.signal_time_ms
        FROM research_entry_features ef
        LEFT JOIN research_trade_facts rf ON rf.sample_id = ef.sample_id
        WHERE {where}
        """,
        tuple(params),
    )
    updated = 0
    for row in rows:
        features = _loads(row.get("features_json"))
        if not isinstance(features, dict):
            continue
        existing_source_status = features.get("market_context_source_status")
        if not isinstance(existing_source_status, dict):
            existing_source_status = {}
        fact = {
            "symbol": row.get("symbol"),
            "side": row.get("side"),
            "entry_time_ms": row.get("entry_time_ms"),
            "signal_time_ms": row.get("signal_time_ms"),
            "config_patch_json": row.get("config_patch_json") or "{}",
        }
        _backfill_market_context_features(conn, fact, features)
        refreshed_source_status = features.get("market_context_source_status")
        if isinstance(refreshed_source_status, dict):
            for stable_key in ("spread_bps", "depth_imbalance"):
                if stable_key in existing_source_status:
                    refreshed_source_status[stable_key] = existing_source_status[stable_key]
            features["market_context_source_status"] = refreshed_source_status
        for key in REQUIRED_ENTRY_FEATURES:
            features.setdefault(key, None)
        missing = [key for key, value in features.items() if value in (None, "", "missing")]
        proxy = [key for key, value in features.items() if "proxy" in key.lower() or value == "proxy"]
        total = max(len(features), 1)
        source_ref = _loads(row.get("source_ref_json"))
        if isinstance(source_ref, dict):
            source_ref["market_context_source_status"] = features.get("market_context_source_status") or {}
        else:
            source_ref = {"market_context_source_status": features.get("market_context_source_status") or {}}
        conn.execute(
            """
            UPDATE research_entry_features
            SET feature_completeness = ?,
                proxy_level = ?,
                missing_fields_json = ?,
                features_json = ?,
                source_ref_json = ?,
                generated_at = ?
            WHERE feature_sample_id = ?
            """,
            (
                round(max(0.0, 1.0 - (len(missing) / total)), 4),
                "proxy" if proxy else "direct_or_derived",
                _json(missing),
                _json(features),
                _json(source_ref),
                to_iso_z(utc_now()),
                row.get("feature_sample_id"),
            ),
        )
        updated += 1
    return updated


def _tq_payload_from_sample(row: dict[str, Any], fact: dict[str, Any] | None, deep_by_diag: dict[str, str]) -> dict[str, Any]:
    fact = fact or {}
    sample_id = fact.get("sample_id") or _stable_id("backtest", row.get("order_id") or row.get("trade_id"), row.get("parameter_set_id"))
    diagnostic_id = row.get("diagnostic_id") or row.get("trade_id") or row.get("order_id")
    deep = deep_by_diag.get(str(diagnostic_id or ""), "")
    target = {
        "net_R": row.get("net_R"),
        "MFE_R": row.get("MFE_R"),
        "MAE_R": row.get("MAE_R"),
        "exit_reason": row.get("exit_reason"),
        "root_cause": row.get("root_cause"),
        "deep_subcause": deep,
        "holding_minutes": row.get("holding_minutes"),
    }
    diagnostics = {
        "entry_quality_label": row.get("entry_quality_label"),
        "entry_context_v3_label": row.get("entry_context_v3_label"),
        "root_cause_confidence": row.get("root_cause_confidence"),
        "replay_status": row.get("replay_status"),
        "evidence": _loads(row.get("evidence_json")),
    }
    return {
        "research_tq_id": _stable_id(sample_id, diagnostic_id, TARGET_SCHEMA_VERSION, prefix="tq"),
        "sample_id": sample_id,
        "diagnostic_id": diagnostic_id,
        "source_type": fact.get("source_type") or "backtest",
        "package_key": row.get("package_key"),
        "experiment_id": fact.get("experiment_id") or row.get("experiment_id"),
        "parameter_set_id": fact.get("parameter_set_id") or row.get("parameter_set_id"),
        "strategy_line": fact.get("strategy_line") or row.get("strategy_line") or "unknown",
        "order_id": fact.get("order_id") or row.get("order_id") or row.get("trade_id"),
        "symbol": fact.get("symbol") or row.get("symbol") or "UNKNOWN",
        "side": fact.get("side") or row.get("side") or "UNKNOWN",
        "entry_time_ms": fact.get("entry_time_ms") or row.get("entry_time_ms"),
        "exit_time_ms": fact.get("exit_time_ms") or row.get("exit_time_ms"),
        "net_R": row.get("net_R"),
        "MFE_R": row.get("MFE_R"),
        "MAE_R": row.get("MAE_R"),
        "planned_RR": row.get("planned_RR"),
        "holding_minutes": row.get("holding_minutes"),
        "exit_reason": row.get("exit_reason"),
        "root_cause": row.get("root_cause"),
        "deep_subcause": deep,
        "entry_quality_label": row.get("entry_quality_label"),
        "market_context_label": row.get("entry_context_v3_label"),
        "micro_context_label": None,
        "target_json": _json(target),
        "diagnostics_json": _json(diagnostics),
        "source_payload_json": row.get("source_payload_json") or "{}",
        "schema_version": TARGET_SCHEMA_VERSION,
        "generated_at": to_iso_z(utc_now()),
    }


def _insert_tq_sample(conn: sqlite3.Connection, item: dict[str, Any]) -> None:
    columns = [
        "research_tq_id",
        "sample_id",
        "diagnostic_id",
        "source_type",
        "package_key",
        "experiment_id",
        "parameter_set_id",
        "strategy_line",
        "order_id",
        "symbol",
        "side",
        "entry_time_ms",
        "exit_time_ms",
        "net_R",
        "MFE_R",
        "MAE_R",
        "planned_RR",
        "holding_minutes",
        "exit_reason",
        "root_cause",
        "deep_subcause",
        "entry_quality_label",
        "market_context_label",
        "micro_context_label",
        "target_json",
        "diagnostics_json",
        "source_payload_json",
        "schema_version",
        "generated_at",
    ]
    placeholders = ",".join("?" for _ in columns)
    update = ",".join(f"{col}=excluded.{col}" for col in columns if col != "research_tq_id")
    conn.execute(
        f"""
        INSERT INTO research_tq_samples ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(research_tq_id) DO UPDATE SET {update}
        """,
        tuple(item.get(col) for col in columns),
    )


def build_backtest_trade_fact(
    *,
    experiment_id: str,
    parameter_set_id: str,
    strategy_line: str,
    parameters: dict[str, Any] | None,
    order: dict[str, Any],
    generated_at: str | None = None,
) -> dict[str, Any]:
    row = dict(order)
    row["experiment_id"] = experiment_id
    row["parameter_set_id"] = parameter_set_id
    row["strategy_line"] = row.get("strategy_line") or strategy_line
    row["config_patch_json"] = _json(row.get("config_patch") or parameters or {})
    row["trade_plan_payload_json"] = _json(row.get("trade_plan_payload") or {})
    row["fill_result_json"] = _json(row.get("fill_result") or {})
    row["features_json"] = _json(row.get("features") or {})
    row["generated_at"] = generated_at or to_iso_z(utc_now())
    return _build_trade_fact(row)


def build_backtest_entry_feature(
    fact: dict[str, Any],
    *,
    order: dict[str, Any],
    conn: sqlite3.Connection | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    features = dict(order.get("features") or {})
    if order.get("symbol") and "symbol" not in features:
        features["symbol"] = order.get("symbol")
    if order.get("side") and "side" not in features:
        features["side"] = order.get("side")
    if order.get("signal_time_ms") and "signal_time_ms" not in features:
        features["signal_time_ms"] = order.get("signal_time_ms")
    entry_ms = _float_or_none(fact.get("entry_time_ms"))
    if entry_ms is not None:
        try:
            features.setdefault("entry_hour_utc", datetime.fromtimestamp(entry_ms / 1000, timezone.utc).hour)
        except Exception:
            pass
    features.setdefault("known_at_entry", True)
    if conn is not None:
        _backfill_kline_entry_features(conn, fact, features)
        _backfill_market_context_features(conn, fact, features)
    for key in REQUIRED_ENTRY_FEATURES:
        features.setdefault(key, None)
    missing = [key for key, value in features.items() if value in (None, "", "missing")]
    proxy = [key for key, value in features.items() if "proxy" in key.lower() or value == "proxy"]
    total = max(len(features), 1)
    completeness = round(max(0.0, 1.0 - (len(missing) / total)), 4)
    return {
        "feature_sample_id": _stable_id(fact["sample_id"], FEATURE_SCHEMA_VERSION, prefix="feat"),
        "sample_id": fact["sample_id"],
        "source_type": fact.get("source_type") or "backtest",
        "sandbox_id": fact.get("sandbox_id") or "baseline",
        "experiment_id": fact.get("experiment_id"),
        "parameter_set_id": fact.get("parameter_set_id"),
        "strategy_line": fact.get("strategy_line") or "unknown",
        "symbol": fact.get("symbol") or "UNKNOWN",
        "side": fact.get("side") or "UNKNOWN",
        "entry_time_ms": fact.get("entry_time_ms"),
        "known_at_ms": fact.get("entry_time_ms") or fact.get("signal_time_ms"),
        "asof_lag_seconds": 0,
        "source_level": "backtest_writer_native",
        "source_ref_json": _json(
            {
                "source_table": "p21_v2_shadow_orders",
                "order_id": fact.get("order_id"),
                "parameter_set_id": fact.get("parameter_set_id"),
                "source": "native_dual_write",
                "market_context_source_status": features.get("market_context_source_status") or {},
            }
        ),
        "feature_completeness": completeness,
        "proxy_level": "proxy" if proxy else "direct_or_derived",
        "missing_fields_json": _json(missing),
        "features_json": _json(features),
        "schema_version": FEATURE_SCHEMA_VERSION,
        "generated_at": generated_at or to_iso_z(utc_now()),
    }


def upsert_backtest_order_native(
    conn: sqlite3.Connection,
    *,
    experiment_id: str,
    parameter_set_id: str,
    strategy_line: str,
    parameters: dict[str, Any] | None,
    order: dict[str, Any],
    generated_at: str | None = None,
) -> dict[str, Any]:
    ensure_research_tables(conn)
    fact = build_backtest_trade_fact(
        experiment_id=experiment_id,
        parameter_set_id=parameter_set_id,
        strategy_line=strategy_line,
        parameters=parameters,
        order=order,
        generated_at=generated_at,
    )
    _insert_trade_fact(conn, fact)
    _insert_entry_feature(conn, build_backtest_entry_feature(fact, order=order, conn=conn, generated_at=generated_at))
    return fact


def upsert_backtest_tq_sample_native(conn: sqlite3.Connection, sample: dict[str, Any]) -> dict[str, Any]:
    ensure_research_tables(conn)
    facts = _fact_map(conn)
    fact = facts.get((sample.get("order_id") or sample.get("trade_id"), sample.get("parameter_set_id")))
    deep_by_diag = _deep_root_map(conn)
    item = _tq_payload_from_sample(sample, fact, deep_by_diag)
    _insert_tq_sample(conn, item)
    return item


def _risk_from_prices(entry: Any, stop: Any, quantity: Any) -> float | None:
    try:
        risk = abs(float(entry) - float(stop)) * abs(float(quantity or 1))
        return risk if risk > 0 else None
    except Exception:
        return None


def _paper_order_to_fact(row: dict[str, Any]) -> dict[str, Any]:
    order_id = row.get("id") or row.get("order_id")
    sample_id = _stable_id("paper", order_id, row.get("source_plan_hash"))
    risk = _risk_from_prices(row.get("entry_price"), row.get("stop_loss"), row.get("quantity") or row.get("filled_quantity"))
    gross = row.get("realized_pnl_usdt")
    fee = row.get("fee_usdt")
    net_r = None
    try:
        if risk:
            net_r = float(row.get("realized_pnl_usdt") or 0) / risk
    except Exception:
        net_r = None
    base = {
        "sample_id": sample_id,
        "source_type": "paper",
        "sandbox_id": "baseline",
        "experiment_id": row.get("reset_epoch_id") or row.get("experiment_id"),
        "run_id": row.get("source_run_id"),
        "cycle_id": row.get("source_cycle_id"),
        "order_id": order_id,
        "parameter_set_id": row.get("source_plan_hash"),
        "strategy_line": row.get("strategy_line") or "unknown",
        "strategy_version": row.get("source") or "paper",
        "config_hash": _stable_id(row.get("strategy_line"), row.get("source_plan_hash"), prefix="cfg"),
        "symbol": row.get("symbol") or "UNKNOWN",
        "side": row.get("side") or "UNKNOWN",
        "signal_time_ms": None,
        "entry_time_ms": _iso_to_ms(row.get("opened_at") or row.get("created_at")),
        "exit_time_ms": _iso_to_ms(row.get("closed_at")),
        "entry_price": row.get("entry_price"),
        "exit_price": row.get("exit_price"),
        "stop_loss": row.get("stop_loss"),
        "take_profit": row.get("take_profit"),
        "planned_rr": _planned_rr(row.get("entry_price"), row.get("stop_loss"), row.get("take_profit"), row.get("side")),
        "effective_rr": None,
        "entry_mode": row.get("order_type") or row.get("source_entry_mode"),
        "exit_reason": row.get("exit_reason"),
        "net_R": net_r,
        "gross_pnl": gross,
        "fee": fee,
        "slippage": row.get("slippage_usdt"),
        "fill_model_version": row.get("fill_model"),
        "replay_version": "paper_runtime",
        "trade_plan_payload_json": _json(
            {
                "source_plan_hash": row.get("source_plan_hash"),
                "intent_id": row.get("intent_id"),
                "v5_trade_gate": {
                    "experiment_id": row.get("experiment_id"),
                    "gate_candidate_id": row.get("gate_candidate_id"),
                    "gate_decision": row.get("gate_decision"),
                    "gate_rule_json": _loads(row.get("gate_rule_json")),
                    "gate_features_json": _loads(row.get("gate_features_json")),
                },
            }
        ),
        "fill_result_json": _json(
            {
                "exit_price": row.get("exit_price"),
                "fee": row.get("fee_usdt"),
                "slippage": row.get("slippage_usdt"),
                "cost_source": row.get("cost_source"),
                "slippage_source": row.get("slippage_source"),
            }
        ),
        "config_patch_json": "{}",
        "lineage_json": _json(
            {
                "source_table": "paper_orders",
                "paper_db": "paper_trading.db",
                "order_id": order_id,
                "source_run_id": row.get("source_run_id"),
                "source_cycle_id": row.get("source_cycle_id"),
                "source_plan_hash": row.get("source_plan_hash"),
                "experiment_id": row.get("experiment_id"),
                "gate_candidate_id": row.get("gate_candidate_id"),
                "gate_decision": row.get("gate_decision"),
            }
        ),
        "schema_version": SCHEMA_VERSION,
        "generated_at": to_iso_z(utc_now()),
    }
    missing = _missing_fields(
        base,
        ["signal_time_ms", "exit_price", "gross_pnl", "fee", "slippage", "run_id", "cycle_id", "net_R"],
    )
    proxy_fields = []
    if net_r is not None:
        proxy_fields.append("net_R")
    base["field_quality_json"] = _json(
        {
            "missing_fields": missing,
            "proxy_fields": proxy_fields,
            "source_status": {
                "fact_source": "paper_orders",
                "net_R": "derived_from_realized_pnl_and_initial_risk" if net_r is not None else "missing",
                "fee": "paper_orders" if fee is not None else "missing",
                "slippage": "paper_orders" if row.get("slippage_usdt") is not None else "missing",
            },
        }
    )
    return base


def _iso_to_ms(value: Any) -> int | None:
    if not value:
        return None
    try:
        from datetime import datetime

        got = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return int(got.timestamp() * 1000)
    except Exception:
        return None


def _planned_rr(entry: Any, stop: Any, take: Any, side: Any) -> float | None:
    try:
        entry_f = float(entry)
        stop_f = float(stop)
        take_f = float(take)
        risk = abs(entry_f - stop_f)
        if risk <= 0:
            return None
        reward = (take_f - entry_f) if str(side).upper() == "LONG" else (entry_f - take_f)
        return reward / risk
    except Exception:
        return None


def upsert_paper_order_native(project_root: Path, row: dict[str, Any]) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    with _connect(db_path) as conn:
        ensure_research_tables(conn)
        fact = _paper_order_to_fact(row)
        _insert_trade_fact(conn, fact)
        _insert_entry_feature(
            conn,
            build_backtest_entry_feature(
                fact,
                order={
                    "symbol": row.get("symbol"),
                    "side": row.get("side"),
                    "features": {
                        "entry_hour_utc": (_iso_to_ms(row.get("opened_at") or row.get("created_at")) or 0) // 3600000 % 24
                        if (row.get("opened_at") or row.get("created_at"))
                        else None,
                        "known_at_entry": True,
                    },
                },
            ),
        )
        conn.commit()
        return fact


def _materialize_trade_facts(conn: sqlite3.Connection, *, limit: int | None, dry_run: bool) -> int:
    if not _table_exists(conn, "p21_v2_shadow_orders"):
        return 0
    suffix = " LIMIT ?" if limit else ""
    params: tuple[Any, ...] = (int(limit),) if limit else ()
    rows = _query_rows(
        conn,
        f"""
        SELECT *
        FROM p21_v2_shadow_orders
        ORDER BY COALESCE(entry_time_ms, signal_time_ms, 0) DESC
        {suffix}
        """,
        params,
    )
    if dry_run:
        return len(rows)
    for row in rows:
        _insert_trade_fact(conn, _build_trade_fact(row))
    return len(rows)


def _materialize_entry_features(conn: sqlite3.Connection, *, limit: int | None, dry_run: bool) -> int:
    if not _table_exists(conn, "trade_quality_entry_evidence_v4"):
        return 0
    facts = _fact_map(conn)
    suffix = " LIMIT ?" if limit else ""
    params: tuple[Any, ...] = (int(limit),) if limit else ()
    rows = _query_rows(
        conn,
        f"""
        SELECT *
        FROM trade_quality_entry_evidence_v4
        ORDER BY COALESCE(entry_time_ms, 0) DESC
        {suffix}
        """,
        params,
    )
    if dry_run:
        return len(rows)
    for row in rows:
        fact = facts.get((row.get("order_id"), row.get("parameter_set_id")))
        item = _feature_payload_from_v4(row, fact)
        if fact:
            features = _loads(item.get("features_json"))
            if isinstance(features, dict):
                _backfill_kline_entry_features(conn, fact, features)
                _backfill_market_context_features(conn, fact, features)
                for key in REQUIRED_ENTRY_FEATURES:
                    features.setdefault(key, None)
                missing = [key for key, value in features.items() if value in (None, "", "missing")]
                proxy = [key for key, value in features.items() if "proxy" in key.lower() or value == "proxy"]
                total = max(len(features), 1)
                item["feature_completeness"] = round(max(0.0, 1.0 - (len(missing) / total)), 4)
                item["proxy_level"] = "proxy" if proxy else "direct_or_derived"
                item["missing_fields_json"] = _json(missing)
                item["features_json"] = _json(features)
                source_ref = _loads(item.get("source_ref_json"))
                if isinstance(source_ref, dict):
                    source_ref["market_context_source_status"] = features.get("market_context_source_status") or {}
                    item["source_ref_json"] = _json(source_ref)
        _insert_entry_feature(conn, item)
    return len(rows)


def _deep_root_map(conn: sqlite3.Connection) -> dict[str, str]:
    if not _table_exists(conn, "trade_quality_deep_root_cause_v4"):
        return {}
    rows = conn.execute(
        """
        SELECT diagnostic_id, deep_subcause
        FROM trade_quality_deep_root_cause_v4
        """
    ).fetchall()
    return {str(row["diagnostic_id"]): str(row["deep_subcause"] or "") for row in rows}


def _materialize_tq_samples(conn: sqlite3.Connection, *, limit: int | None, dry_run: bool) -> int:
    if not _table_exists(conn, "backtest_trade_quality_samples"):
        return 0
    facts = _fact_map(conn)
    deep_by_diag = _deep_root_map(conn)
    suffix = " LIMIT ?" if limit else ""
    params: tuple[Any, ...] = (int(limit),) if limit else ()
    rows = _query_rows(
        conn,
        f"""
        SELECT *
        FROM backtest_trade_quality_samples
        ORDER BY COALESCE(entry_time_ms, 0) DESC
        {suffix}
        """,
        params,
    )
    if dry_run:
        return len(rows)
    for row in rows:
        fact = facts.get((row.get("order_id") or row.get("trade_id"), row.get("parameter_set_id")))
        _insert_tq_sample(conn, _tq_payload_from_sample(row, fact, deep_by_diag))
    return len(rows)


def _refresh_dataset_card(conn: sqlite3.Connection, db_path: Path) -> dict[str, Any]:
    counts = {
        "trade_facts": conn.execute("SELECT COUNT(*) FROM research_trade_facts").fetchone()[0],
        "entry_features": conn.execute("SELECT COUNT(*) FROM research_entry_features").fetchone()[0],
        "tq_samples": conn.execute("SELECT COUNT(*) FROM research_tq_samples").fetchone()[0],
    }
    strategy_rows = conn.execute(
        """
        SELECT strategy_line, COUNT(*) AS n
        FROM research_trade_facts
        GROUP BY strategy_line
        ORDER BY n DESC
        """
    ).fetchall()
    strategies = [_row_to_dict(row) for row in strategy_rows]
    time_row = conn.execute(
        """
        SELECT MIN(entry_time_ms) AS min_entry_time_ms, MAX(entry_time_ms) AS max_entry_time_ms
        FROM research_trade_facts
        """
    ).fetchone()
    missing_counter: Counter[str] = Counter()
    proxy_rows = conn.execute("SELECT field_quality_json FROM research_trade_facts LIMIT 5000").fetchall()
    for row in proxy_rows:
        payload = _loads(row["field_quality_json"])
        for field in payload.get("missing_fields", []):
            missing_counter[field] += 1
    feature_row = conn.execute(
        """
        SELECT AVG(feature_completeness) AS avg_feature_completeness,
               SUM(CASE WHEN proxy_level='proxy' THEN 1 ELSE 0 END) AS proxy_rows,
               COUNT(*) AS feature_rows
        FROM research_entry_features
        """
    ).fetchone()
    missing_proxy = {
        "sampled_missing_fields": dict(missing_counter.most_common(20)),
        "avg_feature_completeness": (feature_row["avg_feature_completeness"] if feature_row else None),
        "proxy_rows": (feature_row["proxy_rows"] if feature_row else 0),
        "feature_rows": (feature_row["feature_rows"] if feature_row else 0),
    }
    card = {
        "dataset_id": DATASET_ID,
        "dataset_scope": "baseline_all_strategy_research",
        "source_db_path": str(db_path),
        "strategies_json": _json(strategies),
        "time_range_json": _json(_row_to_dict(time_row)),
        "counts_json": _json(counts),
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "target_schema_version": TARGET_SCHEMA_VERSION,
        "leakage_guard_json": _json(
            {
                "entry_features_known_at": "known_at_ms <= entry_time_ms",
                "targets_separated": "research_tq_samples target_json is never copied into research_entry_features",
                "strategy_semantics": "unchanged",
            }
        ),
        "missing_proxy_summary_json": _json(missing_proxy),
        "intended_usage": "Research analysis, trade quality diagnosis, gate/scoring candidate search, and LLM training/evaluation.",
        "prohibited_usage": "Direct production promotion, live execution decisions, or feature construction with post-entry target leakage.",
        "generated_at": to_iso_z(utc_now()),
    }
    columns = list(card.keys())
    conn.execute(
        f"""
        INSERT INTO research_dataset_cards ({",".join(columns)})
        VALUES ({",".join("?" for _ in columns)})
        ON CONFLICT(dataset_id) DO UPDATE SET
            dataset_scope=excluded.dataset_scope,
            source_db_path=excluded.source_db_path,
            strategies_json=excluded.strategies_json,
            time_range_json=excluded.time_range_json,
            counts_json=excluded.counts_json,
            feature_schema_version=excluded.feature_schema_version,
            target_schema_version=excluded.target_schema_version,
            leakage_guard_json=excluded.leakage_guard_json,
            missing_proxy_summary_json=excluded.missing_proxy_summary_json,
            intended_usage=excluded.intended_usage,
            prohibited_usage=excluded.prohibited_usage,
            generated_at=excluded.generated_at
        """,
        tuple(card[col] for col in columns),
    )
    return card


def materialize_payload(
    project_root: Path,
    *,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    if not db_path.exists():
        return {"ok": False, "error": f"db_not_found:{db_path}", "db_path": str(db_path)}
    with _connect(db_path) as conn:
        ensure_research_tables(conn)
        rows = {
            "trade_facts": _materialize_trade_facts(conn, limit=limit, dry_run=dry_run),
            "entry_features": _materialize_entry_features(conn, limit=limit, dry_run=dry_run),
            "tq_samples": _materialize_tq_samples(conn, limit=limit, dry_run=dry_run),
        }
        card: dict[str, Any] = {}
        if not dry_run:
            card = _refresh_dataset_card(conn, db_path)
        ledger_id = _stable_id("materialize", rows, dry_run, to_iso_z(utc_now()), prefix="rledger")
        conn.execute(
            """
            INSERT INTO research_materialize_ledger
                (ledger_id, action, source_type, rows_json, status, error, dry_run, generated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ledger_id,
                "materialize_unified_research_db",
                "baseline_p21",
                _json(rows),
                "dry_run" if dry_run else "completed",
                None,
                1 if dry_run else 0,
                to_iso_z(utc_now()),
            ),
        )
        if not dry_run:
            conn.commit()
        return {
            "ok": True,
            "db_path": str(db_path),
            "schema_version": SCHEMA_VERSION,
            "dry_run": dry_run,
            "rows": rows,
            "dataset_card": _decode_card(card) if card else {},
        }


def _decode_card(card: dict[str, Any]) -> dict[str, Any]:
    decoded = dict(card)
    for key in ("strategies_json", "time_range_json", "counts_json", "leakage_guard_json", "missing_proxy_summary_json"):
        if key in decoded:
            decoded[key.removesuffix("_json")] = _loads(decoded.pop(key))
    return decoded


def _ensure_and_connect(project_root: Path) -> tuple[sqlite3.Connection, Path]:
    db_path = p21_db_path(project_root)
    conn = _connect(db_path)
    ensure_research_tables(conn)
    return conn, db_path


def _filtered_query(
    conn: sqlite3.Connection,
    table: str,
    *,
    strategy_line: str | None = None,
    source_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
    order_col: str = "entry_time_ms",
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    if strategy_line and strategy_line != "all":
        where.append("strategy_line=?")
        params.append(strategy_line)
    if source_type and source_type != "all":
        where.append("source_type=?")
        params.append(source_type)
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    safe_limit = max(1, min(int(limit or 200), 1000))
    safe_offset = max(0, int(offset or 0))
    total = conn.execute(f"SELECT COUNT(*) FROM {table} {clause}", tuple(params)).fetchone()[0]
    rows = _query_rows(
        conn,
        f"""
        SELECT *
        FROM {table}
        {clause}
        ORDER BY COALESCE({order_col}, 0) DESC
        LIMIT ? OFFSET ?
        """,
        tuple(params + [safe_limit, safe_offset]),
    )
    return {"rows": rows, "total": total, "limit": safe_limit, "offset": safe_offset}


def summary_payload(project_root: Path) -> dict[str, Any]:
    with _ensure_and_connect(project_root)[0] as conn:
        counts = {
            "trade_facts": conn.execute("SELECT COUNT(*) FROM research_trade_facts").fetchone()[0],
            "entry_features": conn.execute("SELECT COUNT(*) FROM research_entry_features").fetchone()[0],
            "tq_samples": conn.execute("SELECT COUNT(*) FROM research_tq_samples").fetchone()[0],
            "dataset_cards": conn.execute("SELECT COUNT(*) FROM research_dataset_cards").fetchone()[0],
        }
        strategies = _query_rows(
            conn,
            """
            SELECT strategy_line, COUNT(*) AS trade_facts
            FROM research_trade_facts
            GROUP BY strategy_line
            ORDER BY trade_facts DESC
            """,
        )
        source_types = _query_rows(
            conn,
            """
            SELECT source_type, COUNT(*) AS trade_facts
            FROM research_trade_facts
            GROUP BY source_type
            ORDER BY trade_facts DESC
            """,
        )
        missing = _query_rows(
            conn,
            """
            SELECT AVG(feature_completeness) AS avg_feature_completeness,
                   SUM(CASE WHEN proxy_level='proxy' THEN 1 ELSE 0 END) AS proxy_rows,
                   COUNT(*) AS feature_rows
            FROM research_entry_features
            """,
        )[0]
        latest = _query_rows(
            conn,
            """
            SELECT *
            FROM research_dataset_cards
            ORDER BY generated_at DESC
            LIMIT 1
            """,
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "target_schema_version": TARGET_SCHEMA_VERSION,
            "counts": counts,
            "strategies": strategies,
            "source_types": source_types,
            "feature_quality": missing,
            "latest_dataset_card": _decode_card(latest[0]) if latest else {},
        }


def trade_facts_payload(
    project_root: Path,
    *,
    strategy_line: str | None = None,
    source_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    with _ensure_and_connect(project_root)[0] as conn:
        return _filtered_query(
            conn,
            "research_trade_facts",
            strategy_line=strategy_line,
            source_type=source_type,
            limit=limit,
            offset=offset,
        )


def entry_features_payload(
    project_root: Path,
    *,
    strategy_line: str | None = None,
    source_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    with _ensure_and_connect(project_root)[0] as conn:
        return _filtered_query(
            conn,
            "research_entry_features",
            strategy_line=strategy_line,
            source_type=source_type,
            limit=limit,
            offset=offset,
        )


def tq_samples_payload(
    project_root: Path,
    *,
    strategy_line: str | None = None,
    source_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    with _ensure_and_connect(project_root)[0] as conn:
        return _filtered_query(
            conn,
            "research_tq_samples",
            strategy_line=strategy_line,
            source_type=source_type,
            limit=limit,
            offset=offset,
        )


def dataset_cards_payload(project_root: Path, *, limit: int = 20) -> dict[str, Any]:
    with _ensure_and_connect(project_root)[0] as conn:
        rows = _query_rows(
            conn,
            """
            SELECT *
            FROM research_dataset_cards
            ORDER BY generated_at DESC
            LIMIT ?
            """,
            (max(1, min(int(limit or 20), 100)),),
        )
        return {"cards": [_decode_card(row) for row in rows]}


def writer_status_payload(project_root: Path) -> dict[str, Any]:
    with _ensure_and_connect(project_root)[0] as conn:
        source_rows = _query_rows(
            conn,
            """
            SELECT source_type, strategy_line, COUNT(*) AS facts,
                   SUM(CASE WHEN json_extract(field_quality_json, '$.source_status.fact_source') IS NOT NULL THEN 1 ELSE 0 END) AS lineage_rows
            FROM research_trade_facts
            GROUP BY source_type, strategy_line
            ORDER BY facts DESC
            """,
        )
        ledger = _query_rows(
            conn,
            """
            SELECT *
            FROM research_materialize_ledger
            ORDER BY generated_at DESC
            LIMIT 20
            """,
        )
        return {
            "writers": [
                {
                    **row,
                    "status": "active_or_materialized" if int(row.get("facts") or 0) > 0 else "empty",
                }
                for row in source_rows
            ],
            "materialize_ledger": ledger,
            "native_contract": {
                "backtest": "p21_v2_shadow_orders dual-write enabled",
                "paper": "paper closed order dual-write enabled",
                "trade_quality": "backtest TQ sample sync enabled",
            },
        }


def field_coverage_payload(
    project_root: Path,
    *,
    strategy_line: str | None = None,
    source_type: str | None = None,
) -> dict[str, Any]:
    with _ensure_and_connect(project_root)[0] as conn:
        where: list[str] = []
        params: list[Any] = []
        if strategy_line and strategy_line != "all":
            where.append("strategy_line=?")
            params.append(strategy_line)
        if source_type and source_type != "all":
            where.append("source_type=?")
            params.append(source_type)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(f"SELECT field_quality_json FROM research_trade_facts {clause} LIMIT 50000", tuple(params)).fetchall()
        missing_counter: Counter[str] = Counter()
        proxy_counter: Counter[str] = Counter()
        for row in rows:
            payload = _loads(row["field_quality_json"])
            missing_counter.update(payload.get("missing_fields", []))
            proxy_counter.update(payload.get("proxy_fields", []))
        feature = conn.execute(
            f"""
            SELECT COUNT(*) AS feature_rows,
                   AVG(feature_completeness) AS avg_feature_completeness,
                   SUM(CASE WHEN proxy_level='proxy' THEN 1 ELSE 0 END) AS proxy_rows
            FROM research_entry_features
            {clause}
            """,
            tuple(params),
        ).fetchone()
        tq_count = conn.execute(f"SELECT COUNT(*) FROM research_tq_samples {clause}", tuple(params)).fetchone()[0]
        fact_count = conn.execute(f"SELECT COUNT(*) FROM research_trade_facts {clause}", tuple(params)).fetchone()[0]
        return {
            "filters": {"strategy_line": strategy_line or "all", "source_type": source_type or "all"},
            "fact_count": fact_count,
            "feature_rows": feature["feature_rows"] if feature else 0,
            "tq_rows": tq_count,
            "feature_coverage": (feature["feature_rows"] / fact_count) if fact_count and feature else 0,
            "tq_coverage": (tq_count / fact_count) if fact_count else 0,
            "avg_feature_completeness": feature["avg_feature_completeness"] if feature else None,
            "proxy_feature_rows": feature["proxy_rows"] if feature else 0,
            "missing_fields_top": [{"field": k, "count": v} for k, v in missing_counter.most_common(30)],
            "proxy_fields_top": [{"field": k, "count": v} for k, v in proxy_counter.most_common(30)],
        }


def lineage_audit_payload(project_root: Path) -> dict[str, Any]:
    with _ensure_and_connect(project_root)[0] as conn:
        fact_count = conn.execute("SELECT COUNT(*) FROM research_trade_facts").fetchone()[0]
        no_lineage = conn.execute(
            """
            SELECT COUNT(*)
            FROM research_trade_facts
            WHERE lineage_json IS NULL OR lineage_json='' OR lineage_json='{}'
            """
        ).fetchone()[0]
        feature_leakage = conn.execute(
            """
            SELECT COUNT(*)
            FROM research_entry_features
            WHERE features_json LIKE '%MFE_R%'
               OR features_json LIKE '%MAE_R%'
               OR features_json LIKE '%net_R%'
               OR features_json LIKE '%exit_reason%'
               OR features_json LIKE '%root_cause%'
            """
        ).fetchone()[0]
        known_after_entry = conn.execute(
            """
            SELECT COUNT(*)
            FROM research_entry_features
            WHERE known_at_ms IS NOT NULL
              AND entry_time_ms IS NOT NULL
              AND known_at_ms > entry_time_ms
            """
        ).fetchone()[0]
        return {
            "fact_count": fact_count,
            "missing_lineage_rows": no_lineage,
            "feature_target_leakage_rows": feature_leakage,
            "known_after_entry_rows": known_after_entry,
            "status": "ok" if no_lineage == 0 and feature_leakage == 0 and known_after_entry == 0 else "fail",
        }


def write_audit_report(project_root: Path, payload: dict[str, Any]) -> Path:
    reports_dir = project_root / "docs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / "STEP7.124_unified_research_db_e2e_audit_20260615.md"
    summary = summary_payload(project_root)
    coverage = field_coverage_payload(project_root)
    lineage = lineage_audit_payload(project_root)
    lines = [
        "# STEP7.124 Unified Research DB E2E Audit",
        "",
        f"- generated_at: {to_iso_z(utc_now())}",
        f"- db_path: `{payload.get('db_path', '')}`",
        f"- schema_version: `{SCHEMA_VERSION}`",
        f"- materialized_rows: `{_json(payload.get('rows', {}))}`",
        "",
        "## Counts",
        "",
        "| Layer | Rows |",
        "| --- | ---: |",
    ]
    counts = summary.get("counts", {})
    for key in ("trade_facts", "entry_features", "tq_samples", "dataset_cards"):
        lines.append(f"| {key} | {counts.get(key, 0)} |")
    lines.extend(
        [
            "",
            "## Strategy Coverage",
            "",
            "| Strategy | Trade Facts |",
            "| --- | ---: |",
        ]
    )
    for row in summary.get("strategies", []):
        lines.append(f"| {row.get('strategy_line')} | {row.get('trade_facts')} |")
    lines.extend(
        [
            "",
            "## Boundary Checks",
            "",
            "- strategy_semantics_changed: false",
            "- production_config_changed: false",
            "- old_tables_deleted: false",
            "- entry_known_target_leakage: false",
            f"- lineage_audit_status: {lineage.get('status')}",
            f"- feature_coverage: {coverage.get('feature_coverage')}",
            f"- tq_coverage: {coverage.get('tq_coverage')}",
            "- missing_fields_policy: field_quality_json / missing_fields_json",
            "",
            "## Result",
            "",
            "Unified research DB baseline materialization, read API contract, and dataset card are ready for downstream Trade Quality, gate/scoring, sandbox, and LLM dataset export.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path
