from __future__ import annotations

import json
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_trade_quality import (
    materialize_payload as tq_materialize_payload,
    packages_payload as tq_packages_payload,
)
from laoma_signal_engine.backtest.p21_v2 import _connect, _loads, _num, ensure_p21_v2_tables

SCHEMA_VERSION = "21.59-strategy5-6-trade-gate"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _stable_id(prefix: str, payload: Any, size: int = 20) -> str:
    import hashlib

    return f"{prefix}_{hashlib.sha256(_json(payload).encode('utf-8')).hexdigest()[:size]}"


def ensure_gate_scoring_tables(db_path: Path) -> None:
    ensure_p21_v2_tables(db_path)
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS backtest_tq_materialize_batches(
              batch_id TEXT PRIMARY KEY,
              experiment_id TEXT,
              strategy_line TEXT,
              top_n INTEGER NOT NULL,
              bounded_limit_per_package INTEGER NOT NULL,
              selected_packages INTEGER NOT NULL,
              materialized_packages INTEGER NOT NULL,
              materialized_samples INTEGER NOT NULL,
              skipped_packages_json TEXT NOT NULL,
              package_results_json TEXT NOT NULL,
              status TEXT NOT NULL,
              dry_run INTEGER NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS backtest_gate_feature_samples(
              sample_id TEXT PRIMARY KEY,
              diagnostic_id TEXT NOT NULL,
              package_key TEXT NOT NULL,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              order_id TEXT NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              entry_time TEXT,
              entry_time_ms INTEGER,
              train_split TEXT NOT NULL,
              feature_completeness TEXT NOT NULL,
              features_json TEXT NOT NULL,
              targets_json TEXT NOT NULL,
              diagnostics_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(experiment_id, parameter_set_id, order_id)
            );
            CREATE INDEX IF NOT EXISTS idx_bt_gate_features_exp_param
              ON backtest_gate_feature_samples(experiment_id, strategy_line, parameter_set_id, train_split);
            CREATE INDEX IF NOT EXISTS idx_bt_gate_features_package
              ON backtest_gate_feature_samples(package_key, strategy_line, symbol, side);

            CREATE TABLE IF NOT EXISTS backtest_gate_market_regime_features(
              sample_id TEXT PRIMARY KEY,
              diagnostic_id TEXT,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              entry_time_ms INTEGER,
              btc_trend TEXT NOT NULL,
              btc_volatility TEXT NOT NULL,
              btc_alignment TEXT NOT NULL,
              market_breadth TEXT NOT NULL,
              funding_regime TEXT NOT NULL,
              oi_direction TEXT NOT NULL,
              regime_quality TEXT NOT NULL,
              regime_source TEXT NOT NULL,
              source_status_json TEXT NOT NULL,
              asof_lag_seconds REAL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_bt_gate_regime_exp_line
              ON backtest_gate_market_regime_features(experiment_id, strategy_line, parameter_set_id);
            CREATE INDEX IF NOT EXISTS idx_bt_gate_regime_entry
              ON backtest_gate_market_regime_features(strategy_line, entry_time_ms);
            CREATE INDEX IF NOT EXISTS idx_p21_klines_time_symbol
              ON p21_klines_1m(open_time_ms, symbol);

            CREATE TABLE IF NOT EXISTS backtest_gate_bucket_rollups(
              bucket_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT,
              strategy_line TEXT,
              dimension TEXT NOT NULL,
              bucket_key TEXT NOT NULL,
              sample_period TEXT NOT NULL,
              sample_count INTEGER NOT NULL,
              metrics_json TEXT NOT NULL,
              evidence_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(experiment_id, parameter_set_id, strategy_line, dimension, bucket_key, sample_period)
            );
            CREATE INDEX IF NOT EXISTS idx_bt_gate_bucket_lookup
              ON backtest_gate_bucket_rollups(experiment_id, strategy_line, dimension, sample_period, sample_count);

            CREATE TABLE IF NOT EXISTS backtest_gate_score_validations(
              validation_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT,
              strategy_line TEXT NOT NULL,
              score_name TEXT NOT NULL,
              bucket_count INTEGER NOT NULL,
              train_monotonic INTEGER NOT NULL,
              validation_monotonic INTEGER NOT NULL,
              test_monotonic INTEGER NOT NULL,
              best_cutoff TEXT NOT NULL,
              pf_before REAL,
              pf_after_test REAL,
              expectancy_after_test REAL,
              trade_coverage_test REAL,
              overfit_risk TEXT NOT NULL,
              metrics_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(experiment_id, parameter_set_id, strategy_line, score_name)
            );

            CREATE TABLE IF NOT EXISTS backtest_gate_candidates(
              candidate_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT,
              strategy_line TEXT NOT NULL,
              gate_type TEXT NOT NULL,
              status TEXT NOT NULL,
              rule_json TEXT NOT NULL,
              config_patch_preview_json TEXT NOT NULL,
              train_metrics_json TEXT NOT NULL,
              validation_metrics_json TEXT NOT NULL,
              test_metrics_json TEXT NOT NULL,
              pf_before REAL,
              pf_after_test REAL,
              trade_coverage_test REAL,
              overfit_risk TEXT NOT NULL,
              evidence_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(experiment_id, parameter_set_id, strategy_line, gate_type, rule_json)
            );
            CREATE INDEX IF NOT EXISTS idx_bt_gate_candidates_rank
              ON backtest_gate_candidates(status, overfit_risk, pf_after_test);
            CREATE INDEX IF NOT EXISTS idx_bt_gate_candidates_line_type
              ON backtest_gate_candidates(strategy_line, gate_type, status, pf_after_test);
            """
        )


def _latest_experiment(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        """
        SELECT experiment_id, MAX(generated_at) AS generated_at
        FROM p21_v2_30d_metrics
        GROUP BY experiment_id
        ORDER BY generated_at DESC
        LIMIT 1
        """
    ).fetchone()
    return str(row["experiment_id"]) if row else None


def _status_from_result(results: list[dict[str, Any]], skipped: list[dict[str, Any]]) -> str:
    if not results and skipped:
        return "blocked"
    if skipped:
        return "partial"
    return "ok"


def batch_materialize_payload(
    project_root: Path,
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = "all",
    top_n: int = 5,
    limit: int = 500,
    dry_run: bool = True,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        exp = experiment_id or _latest_experiment(conn)
    if not exp:
        return {"status": "empty", "selected_packages": 0, "materialized_samples": 0, "packages": []}
    package_payload = tq_packages_payload(
        project_root,
        experiment_id=exp,
        strategy_line=strategy_line,
        mode="leaderboard_candidates",
        limit=max(1, min(50, top_n * 3)),
    )
    selected = []
    skipped = []
    for pkg in package_payload.get("packages") or []:
        if len(selected) >= max(1, top_n):
            break
        if pkg.get("sample_status") == "metrics_only_no_trade_samples" or not pkg.get("has_shadow_orders"):
            skipped.append({"package_key": pkg.get("package_key"), "reason": "metrics_only_no_trade_samples"})
            continue
        selected.append(pkg)
    results = []
    materialized_samples = 0
    for pkg in selected:
        result = tq_materialize_payload(
            project_root,
            experiment_id=str(pkg["experiment_id"]),
            strategy_line=str(pkg["strategy_line"]),
            parameter_set_id=str(pkg["parameter_set_id"]),
            top_n=1,
            limit=max(1, min(5000, limit)),
            dry_run=dry_run,
            force=False,
        )
        materialized_samples += int(result.get("materialized_count") or 0)
        results.append({"package": pkg, "result": result})
    status = _status_from_result(results, skipped)
    batch_id = _stable_id(
        "btgatebatch",
        {"experiment_id": exp, "strategy_line": strategy_line, "top_n": top_n, "limit": limit, "dry_run": dry_run, "ts": _now()},
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "batch_id": batch_id,
        "experiment_id": exp,
        "strategy_line": strategy_line or "all",
        "top_n": top_n,
        "bounded_limit_per_package": limit,
        "selected_packages": len(selected),
        "materialized_packages": len(results),
        "materialized_samples": materialized_samples,
        "skipped_packages": skipped,
        "package_results": results,
        "status": status,
        "dry_run": dry_run,
        "generated_at": _now(),
    }
    if not dry_run:
        with _connect(db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO backtest_tq_materialize_batches(
                  batch_id, experiment_id, strategy_line, top_n, bounded_limit_per_package,
                  selected_packages, materialized_packages, materialized_samples,
                  skipped_packages_json, package_results_json, status, dry_run, schema_version, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    exp,
                    strategy_line or "all",
                    top_n,
                    limit,
                    len(selected),
                    len(results),
                    materialized_samples,
                    _json(skipped),
                    _json(results),
                    status,
                    1 if dry_run else 0,
                    SCHEMA_VERSION,
                    payload["generated_at"],
                ),
            )
    return payload


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _session(hour: int | None) -> str:
    if hour is None:
        return "unknown"
    if 0 <= hour <= 7:
        return "asia"
    if 8 <= hour <= 15:
        return "europe"
    return "us"


def _symbol_group(symbol: str) -> str:
    value = str(symbol or "").upper()
    if value in {"BTCUSDT", "ETHUSDT"}:
        return "major"
    if value.startswith("1000") or value.startswith("1000000"):
        return "small_alt"
    return "alt"


def _bucket(value: float | None, edges: tuple[float, float], labels: tuple[str, str, str]) -> str:
    if value is None:
        return "unknown"
    if value < edges[0]:
        return labels[0]
    if value < edges[1]:
        return labels[1]
    return labels[2]


def _entry_minute_ms(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        ms = int(float(value))
    except Exception:
        return None
    return (ms // 60000) * 60000


def _ret_bps(new: float | None, old: float | None) -> float | None:
    if new is None or old in (None, 0):
        return None
    return (float(new) / float(old) - 1.0) * 10000.0


def _btc_regime_for_entry(
    conn: sqlite3.Connection,
    entry_time_ms: int | None,
    side: str | None,
    cache: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    minute_ms = _entry_minute_ms(entry_time_ms)
    if minute_ms is None:
        return {
            "btc_trend": "unknown",
            "btc_volatility": "unknown",
            "btc_alignment": "unknown",
            "source_status": {"btc": "missing_entry_time"},
            "asof_lag_seconds": None,
        }
    cached = cache.get(minute_ms)
    if cached is None:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT open_time_ms, high, low, close
            FROM p21_klines_1m
            WHERE symbol = 'BTCUSDT' AND open_time_ms <= ?
            ORDER BY open_time_ms DESC
            LIMIT 61
            """,
            (minute_ms,),
        ).fetchall()
        rows = list(reversed([dict(row) for row in rows]))
        if len(rows) < 16:
            cached = {
                "btc_trend": "unknown",
                "btc_volatility": "unknown",
                "btc_direction": "chop",
                "source_time_ms": rows[-1]["open_time_ms"] if rows else None,
                "source_status": {"btc": "insufficient_kline_rows", "rows": len(rows)},
            }
        else:
            close_now = _num(rows[-1].get("close"), None)
            close_15 = _num(rows[-16].get("close"), None)
            close_60 = _num(rows[0].get("close"), None) if len(rows) >= 61 else None
            ret_15 = _ret_bps(close_now, close_15)
            ret_60 = _ret_bps(close_now, close_60) if close_60 is not None else None
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
            window = rows[-30:] if len(rows) >= 30 else rows
            highs = [_num(row.get("high"), 0.0) for row in window]
            lows = [_num(row.get("low"), 0.0) for row in window]
            range_bps = _ret_bps(max(highs) if highs else None, min(lows) if lows else None)
            if range_bps is None:
                volatility = "unknown"
            elif range_bps < 30:
                volatility = "low"
            elif range_bps < 90:
                volatility = "normal"
            else:
                volatility = "high"
            cached = {
                "btc_trend": trend,
                "btc_volatility": volatility,
                "btc_direction": trend,
                "source_time_ms": rows[-1]["open_time_ms"],
                "source_status": {
                    "btc": "ok",
                    "rows": len(rows),
                    "ret_15_bps": round(ret_15, 8) if ret_15 is not None else None,
                    "ret_60_bps": round(ret_60, 8) if ret_60 is not None else None,
                    "range_30m_bps": round(range_bps, 8) if range_bps is not None else None,
                },
            }
        cache[minute_ms] = cached
    trend = str(cached.get("btc_direction") or "chop")
    norm_side = str(side or "").upper()
    if trend == "chop":
        alignment = "chop"
    elif norm_side == "LONG":
        alignment = "same" if trend == "bullish" else "opposite"
    elif norm_side == "SHORT":
        alignment = "same" if trend == "bearish" else "opposite"
    else:
        alignment = "unknown"
    source_time_ms = cached.get("source_time_ms")
    return {
        "btc_trend": cached.get("btc_trend") or "unknown",
        "btc_volatility": cached.get("btc_volatility") or "unknown",
        "btc_alignment": alignment,
        "source_status": cached.get("source_status") or {"btc": "unknown"},
        "asof_lag_seconds": round((minute_ms - int(source_time_ms)) / 1000.0, 3) if source_time_ms is not None else None,
    }


def _market_breadth_for_entry(
    conn: sqlite3.Connection,
    entry_time_ms: int | None,
    cache: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    minute_ms = _entry_minute_ms(entry_time_ms)
    if minute_ms is None:
        return {"market_breadth": "unknown", "source_status": {"market_breadth": "missing_entry_time"}}
    cached = cache.get(minute_ms)
    if cached is None:
        prev_ms = minute_ms - 15 * 60000
        conn.row_factory = sqlite3.Row
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
            (prev_ms, minute_ms),
        ).fetchone()
        total = int(row["total"] or 0) if row else 0
        up_count = int(row["up_count"] or 0) if row else 0
        down_count = int(row["down_count"] or 0) if row else 0
        if total < 25:
            breadth = "unknown"
            status = "insufficient_kline_cross_section"
        else:
            up_ratio = up_count / total
            down_ratio = down_count / total
            if up_ratio >= 0.58:
                breadth = "up"
            elif down_ratio >= 0.58:
                breadth = "down"
            else:
                breadth = "mixed"
            status = "ok"
        cached = {
            "market_breadth": breadth,
            "source_status": {
                "market_breadth": status,
                "symbols": total,
                "up_count": up_count,
                "down_count": down_count,
            },
        }
        cache[minute_ms] = cached
    return cached


def _score_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 40:
        return "q1_low"
    if value < 55:
        return "q2_mid_low"
    if value < 70:
        return "q3_mid"
    if value < 85:
        return "q4_high"
    return "q5_top"


def _target_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rs = [_num(row.get("target_net_R"), 0.0) for row in rows]
    wins = [r for r in rs if r > 0]
    losses = [r for r in rs if r <= 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    pf = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in rs:
        equity += r
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    count = len(rs)
    return {
        "trade_count": count,
        "win_rate": len(wins) / count if count else 0.0,
        "gross_profit_R": round(gross_profit, 8),
        "gross_loss_R": round(-gross_loss, 8),
        "profit_factor": round(pf, 8),
        "expectancy_R": round(sum(rs) / count, 8) if count else 0.0,
        "avg_win_R": round(sum(wins) / len(wins), 8) if wins else 0.0,
        "avg_loss_R": round(abs(sum(losses) / len(losses)), 8) if losses else 0.0,
        "total_R": round(sum(rs), 8),
        "max_drawdown_R": round(max_dd, 8),
        "avg_MFE_R": round(sum(_num(row.get("target_MFE_R"), 0.0) for row in rows) / count, 8) if count else 0.0,
        "avg_MAE_R": round(sum(_num(row.get("target_MAE_R"), 0.0) for row in rows) / count, 8) if count else 0.0,
    }


def _split_metrics(rows: list[dict[str, Any]], rule: dict[str, Any]) -> dict[str, Any]:
    before = list(rows)
    excluded = [row for row in before if _rule_matches(row, rule)]
    kept = [row for row in before if not _rule_matches(row, rule)]
    before_metrics = _target_metrics(before)
    after_metrics = _target_metrics(kept)
    excluded_metrics = _target_metrics(excluded)
    count = before_metrics["trade_count"]
    kept_count = after_metrics["trade_count"]
    excluded_count = excluded_metrics["trade_count"]
    return {
        "before": before_metrics,
        "after": after_metrics,
        "excluded": excluded_metrics,
        "profit_factor": after_metrics["profit_factor"],
        "expectancy_R": after_metrics["expectancy_R"],
        "trade_count": kept_count,
        "excluded_trade_count": excluded_count,
        "kept_coverage": round(kept_count / count, 8) if count else 0.0,
        "excluded_coverage": round(excluded_count / count, 8) if count else 0.0,
    }


def _group_by_split(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {"train": [], "validation": [], "test": []}
    for row in rows:
        split = str(row.get("train_split") or "unknown")
        groups.setdefault(split, []).append(row)
    return groups


def _gate_value(row: dict[str, Any], dimension: str) -> str:
    features = row.get("features") or {}
    if dimension == "symbol_side":
        return f"{features.get('symbol') or row.get('symbol') or 'unknown'}:{features.get('side') or row.get('side') or 'unknown'}"
    if dimension == "hour_side":
        return f"{features.get('hour_utc') if features.get('hour_utc') is not None else 'unknown'}:{features.get('side') or row.get('side') or 'unknown'}"
    if dimension == "session_side":
        return f"{features.get('session') or 'unknown'}:{features.get('side') or row.get('side') or 'unknown'}"
    value = features.get(dimension)
    if value is None:
        value = row.get(dimension)
    return str(value if value not in (None, "") else "unknown")


def _rule_matches(row: dict[str, Any], rule: dict[str, Any]) -> bool:
    if rule.get("mode") != "exclude_bucket":
        return False
    return _gate_value(row, str(rule.get("dimension") or "")) == str(rule.get("bucket_key") or "")


def _root_cause_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        diagnostics = row.get("diagnostics") or {}
        key = str(diagnostics.get("root_cause") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return counts


def _known_at_entry_rule(dimension: str) -> bool:
    return dimension in GATE_DIMENSIONS_KNOWN_AT_ENTRY


def _gate_type_for_dimension(dimension: str) -> str:
    if dimension in {"symbol", "symbol_group", "symbol_side"}:
        return "symbol_gate"
    if dimension in {"hour_utc", "session", "weekday", "hour_side", "session_side"}:
        return "time_gate"
    if dimension in {"volatility_regime", "btc_trend", "btc_volatility", "btc_alignment", "market_breadth", "funding_regime", "oi_direction"}:
        return "regime_gate"
    if dimension in {"cost_bucket", "entry_mode"}:
        return "cost_liquidity_gate"
    if dimension in {"side"}:
        return "side_gate"
    return "feature_bucket_gate"


def _candidate_overfit_risk(
    train_metrics: dict[str, Any],
    validation_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    *,
    min_test_pf: float,
    min_coverage: float,
) -> str:
    train_before = _num(train_metrics.get("before", {}).get("profit_factor"), 0.0)
    val_before = _num(validation_metrics.get("before", {}).get("profit_factor"), 0.0)
    test_before = _num(test_metrics.get("before", {}).get("profit_factor"), 0.0)
    train_after = _num(train_metrics.get("after", {}).get("profit_factor"), 0.0)
    val_after = _num(validation_metrics.get("after", {}).get("profit_factor"), 0.0)
    test_after = _num(test_metrics.get("after", {}).get("profit_factor"), 0.0)
    coverage = _num(test_metrics.get("kept_coverage"), 0.0)
    test_trades = int(_num(test_metrics.get("after", {}).get("trade_count"), 0))
    if coverage < min_coverage or test_trades < 5:
        return "high"
    if val_after <= val_before or test_after <= test_before:
        return "high"
    if test_after < min_test_pf or train_after - test_after > 0.5:
        return "medium"
    return "low"


def _feature_from_sample(row: sqlite3.Row, split: str) -> dict[str, Any]:
    sample = dict(row)
    source_payload = _loads(sample.get("source_payload_json"), {})
    config = source_payload.get("config_patch") or {}
    source_features = source_payload.get("features") or {}
    dt = _parse_dt(sample.get("entry_time"))
    hour = dt.hour if dt else None
    weekday = dt.weekday() if dt else None
    score = _num(source_payload.get("score"), None)
    taker_fee = _num(config.get("taker_fee_bps"), 0.0)
    slippage = _num(config.get("slippage_bps"), 0.0)
    cost_bps = taker_fee + slippage
    planned_rr = _num(sample.get("planned_RR"), None)
    features = {
        "strategy_line": sample.get("strategy_line"),
        "symbol": sample.get("symbol"),
        "symbol_group": _symbol_group(sample.get("symbol")),
        "side": sample.get("side"),
        "hour_utc": hour,
        "weekday": weekday,
        "session": _session(hour),
        "signal_score": score,
        "score_bucket": _score_bucket(score),
        "planned_rr": planned_rr,
        "planned_rr_bucket": _bucket(planned_rr, (0.8, 1.2), ("rr_low", "rr_mid", "rr_high")),
        "taker_fee_bps": taker_fee,
        "slippage_bps": slippage,
        "cost_bps": cost_bps,
        "cost_bucket": _bucket(cost_bps, (4.0, 8.0), ("low_cost", "mid_cost", "high_cost")),
        "entry_mode": (source_payload.get("trade_plan_payload") or {}).get("entry_mode") or (source_payload.get("fill_result") or {}).get("entry_mode") or "unknown",
        "entry_quality_label": sample.get("entry_quality_label") or "unknown",
        "entry_context_v3_label": sample.get("entry_context_v3_label") or "unknown",
        "volume_z": _num(source_features.get("volume_z"), None),
        "volatility_regime": source_features.get("volatility_regime") or "unknown",
        "btc_trend": source_features.get("btc_trend") or "unknown",
        "btc_alignment": source_features.get("btc_alignment") or "unknown",
        "market_breadth": source_features.get("market_breadth") or "unknown",
        "funding_regime": source_features.get("funding_regime") or "unknown",
        "oi_direction": source_features.get("oi_direction") or "unknown",
    }
    missing = [key for key, value in features.items() if value in (None, "", "unknown")]
    targets = {
        "net_R": _num(sample.get("net_R"), 0.0),
        "MFE_R": _num(sample.get("MFE_R"), 0.0),
        "MAE_R": _num(sample.get("MAE_R"), 0.0),
        "exit_reason": sample.get("exit_reason"),
        "holding_minutes": _num(sample.get("holding_minutes"), 0.0),
    }
    diagnostics = {
        "root_cause": sample.get("root_cause"),
        "root_cause_confidence": sample.get("root_cause_confidence"),
        "replay_status": sample.get("replay_status"),
    }
    return {
        "sample_id": _stable_id("btgatefeat", {"experiment_id": sample.get("experiment_id"), "parameter_set_id": sample.get("parameter_set_id"), "order_id": sample.get("order_id")}, 24),
        "diagnostic_id": sample.get("diagnostic_id"),
        "package_key": sample.get("package_key"),
        "experiment_id": sample.get("experiment_id"),
        "parameter_set_id": sample.get("parameter_set_id"),
        "strategy_line": sample.get("strategy_line"),
        "order_id": sample.get("order_id"),
        "symbol": sample.get("symbol"),
        "side": sample.get("side"),
        "entry_time": sample.get("entry_time"),
        "entry_time_ms": sample.get("entry_time_ms"),
        "train_split": split,
        "feature_completeness": "partial" if missing else "complete",
        "features": features,
        "targets": targets,
        "diagnostics": diagnostics,
        "target_net_R": targets["net_R"],
        "target_MFE_R": targets["MFE_R"],
        "target_MAE_R": targets["MAE_R"],
    }


def materialize_features_payload(
    project_root: Path,
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    limit: int = 5000,
    dry_run: bool = True,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    clauses = []
    params: list[Any] = []
    if experiment_id:
        clauses.append("experiment_id = ?")
        params.append(experiment_id)
    if strategy_line and strategy_line != "all":
        clauses.append("strategy_line = ?")
        params.append(strategy_line)
    if parameter_set_id:
        clauses.append("parameter_set_id = ?")
        params.append(parameter_set_id)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT *
            FROM backtest_trade_quality_samples
            {where}
            ORDER BY entry_time_ms ASC, diagnostic_id ASC
            LIMIT ?
            """,
            [*params, max(1, min(100000, limit))],
        ).fetchall()
        features = []
        total = len(rows)
        for idx, row in enumerate(rows):
            ratio = idx / max(1, total)
            split = "train" if ratio < 0.6 else "validation" if ratio < 0.8 else "test"
            features.append(_feature_from_sample(row, split))
        if not dry_run:
            now = _now()
            conn.executemany(
                """
                INSERT OR REPLACE INTO backtest_gate_feature_samples(
                  sample_id, diagnostic_id, package_key, experiment_id, parameter_set_id, strategy_line,
                  order_id, symbol, side, entry_time, entry_time_ms, train_split,
                  feature_completeness, features_json, targets_json, diagnostics_json, schema_version, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item["sample_id"],
                        item["diagnostic_id"],
                        item["package_key"],
                        item["experiment_id"],
                        item["parameter_set_id"],
                        item["strategy_line"],
                        item["order_id"],
                        item["symbol"],
                        item["side"],
                        item["entry_time"],
                        item["entry_time_ms"],
                        item["train_split"],
                        item["feature_completeness"],
                        _json(item["features"]),
                        _json(item["targets"]),
                        _json(item["diagnostics"]),
                        SCHEMA_VERSION,
                        now,
                    )
                    for item in features
                ],
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "dry_run": dry_run,
        "selected_samples": len(features),
        "materialized_features": 0 if dry_run else len(features),
        "feature_count": len(features),
        "feature_completeness": dict(_counts(item["feature_completeness"] for item in features)),
        "generated_at": _now(),
    }


def _counts(values: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in values:
        key = str(value or "unknown")
        out[key] = out.get(key, 0) + 1
    return out


def _feature_rows(conn: sqlite3.Connection, **filters: Any) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    clauses = []
    params: list[Any] = []
    for key in ("experiment_id", "parameter_set_id", "strategy_line", "symbol", "side", "train_split"):
        value = filters.get(key)
        if value in (None, "", "all"):
            continue
        clauses.append(f"{key} = ?")
        params.append(value)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    limit = max(1, min(100000, int(filters.get("limit") or 5000)))
    rows = conn.execute(
        f"SELECT * FROM backtest_gate_feature_samples {where} ORDER BY entry_time_ms ASC LIMIT ?",
        [*params, limit],
    ).fetchall()
    sidecar_by_sample: dict[str, dict[str, Any]] = {}
    sample_ids = [str(row["sample_id"]) for row in rows if row["sample_id"]]
    for start in range(0, len(sample_ids), 500):
        chunk = sample_ids[start : start + 500]
        if not chunk:
            continue
        placeholders = ",".join("?" for _ in chunk)
        regime_rows = conn.execute(
            f"""
            SELECT *
            FROM backtest_gate_market_regime_features
            WHERE sample_id IN ({placeholders})
            """,
            chunk,
        ).fetchall()
        for regime_row in regime_rows:
            sidecar_by_sample[str(regime_row["sample_id"])] = dict(regime_row)
    out = []
    for row in rows:
        item = dict(row)
        features = _loads(item.pop("features_json"), {})
        targets = _loads(item.pop("targets_json"), {})
        diagnostics = _loads(item.pop("diagnostics_json"), {})
        sidecar = sidecar_by_sample.get(str(item.get("sample_id") or ""))
        if sidecar:
            for key in (
                "btc_trend",
                "btc_volatility",
                "btc_alignment",
                "market_breadth",
                "funding_regime",
                "oi_direction",
                "regime_quality",
                "regime_source",
                "asof_lag_seconds",
            ):
                value = sidecar.get(key)
                if value not in (None, ""):
                    features[key] = value
            features["regime_source_status"] = _loads(sidecar.get("source_status_json"), {})
        item.update({f"feature_{k}": v for k, v in features.items()})
        item.update({f"target_{k}": v for k, v in targets.items()})
        item["features"] = features
        item["targets"] = targets
        item["diagnostics"] = diagnostics
        out.append(item)
    return out


def features_payload(project_root: Path, *, limit: int = 200, **filters: Any) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    with _connect(db_path) as conn:
        rows = _feature_rows(conn, limit=limit, **filters)
    return {"schema_version": SCHEMA_VERSION, "count": len(rows), "features": rows, "generated_at": _now()}


BUCKET_DIMENSIONS = (
    "strategy_line",
    "symbol",
    "side",
    "symbol_side",
    "symbol_group",
    "hour_utc",
    "hour_side",
    "session",
    "session_side",
    "weekday",
    "entry_mode",
    "cost_bucket",
    "volatility_regime",
    "btc_trend",
    "btc_volatility",
    "btc_alignment",
    "market_breadth",
    "funding_regime",
    "oi_direction",
    "planned_rr_bucket",
    "score_bucket",
)

GATE_DIMENSIONS_KNOWN_AT_ENTRY = frozenset(
    {
        "symbol",
        "side",
        "symbol_side",
        "symbol_group",
        "hour_utc",
        "hour_side",
        "session",
        "session_side",
        "weekday",
        "entry_mode",
        "cost_bucket",
        "volatility_regime",
        "btc_trend",
        "btc_volatility",
        "btc_alignment",
        "market_breadth",
        "funding_regime",
        "oi_direction",
        "planned_rr_bucket",
        "score_bucket",
    }
)


def rebuild_buckets_payload(
    project_root: Path,
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    min_samples: int = 5,
    dry_run: bool = True,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    with _connect(db_path) as conn:
        rows = _feature_rows(conn, experiment_id=experiment_id, strategy_line=strategy_line, parameter_set_id=parameter_set_id, limit=100000)
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            for period in ("all", row.get("train_split") or "unknown"):
                for dim in BUCKET_DIMENSIONS:
                    key = _gate_value(row, dim)
                    groups[(period, dim, key)].append(row)
        rollups = []
        now = _now()
        exp = experiment_id or (rows[0]["experiment_id"] if rows else "")
        param = parameter_set_id or None
        line = strategy_line if strategy_line and strategy_line != "all" else None
        for (period, dim, key), items in groups.items():
            metrics = _target_metrics(items)
            evidence = {
                "min_samples": min_samples,
                "sample_status": "ok" if len(items) >= min_samples else "insufficient_samples",
            }
            rollup = {
                "bucket_id": _stable_id("btgatebucket", {"exp": exp, "param": param, "line": line, "period": period, "dim": dim, "key": key}, 24),
                "experiment_id": exp,
                "parameter_set_id": param,
                "strategy_line": line,
                "dimension": dim,
                "bucket_key": key,
                "sample_period": period,
                "sample_count": len(items),
                "metrics": metrics,
                "evidence": evidence,
                "generated_at": now,
            }
            rollups.append(rollup)
        dimension_order = {dimension: idx for idx, dimension in enumerate(BUCKET_DIMENSIONS)}
        rollups.sort(
            key=lambda row: (
                row["sample_period"],
                dimension_order.get(row["dimension"], 999),
                -row["sample_count"],
                row["bucket_key"],
            )
        )
        if not dry_run:
            if exp:
                clause = "experiment_id = ?"
                args: list[Any] = [exp]
                if parameter_set_id:
                    clause += " AND parameter_set_id = ?"
                    args.append(parameter_set_id)
                if strategy_line and strategy_line != "all":
                    clause += " AND strategy_line = ?"
                    args.append(strategy_line)
                conn.execute(f"DELETE FROM backtest_gate_bucket_rollups WHERE {clause}", args)
            conn.executemany(
                """
                INSERT OR REPLACE INTO backtest_gate_bucket_rollups(
                  bucket_id, experiment_id, parameter_set_id, strategy_line, dimension, bucket_key,
                  sample_period, sample_count, metrics_json, evidence_json, schema_version, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["bucket_id"],
                        row["experiment_id"],
                        row["parameter_set_id"],
                        row["strategy_line"],
                        row["dimension"],
                        row["bucket_key"],
                        row["sample_period"],
                        row["sample_count"],
                        _json(row["metrics"]),
                        _json(row["evidence"]),
                        SCHEMA_VERSION,
                        now,
                    )
                    for row in rollups
                ],
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "dry_run": dry_run,
        "source_samples": len(rows),
        "rollup_count": len(rollups),
        "bucket_count": len(rollups),
        "rollups": rollups[:200],
        "generated_at": _now(),
    }


def buckets_payload(project_root: Path, *, limit: int = 200, **filters: Any) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    clauses = []
    params: list[Any] = []
    for key in ("experiment_id", "parameter_set_id", "strategy_line", "dimension", "sample_period"):
        value = filters.get(key)
        if value in (None, "", "all"):
            continue
        clauses.append(f"{key} = ?")
        params.append(value)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"SELECT * FROM backtest_gate_bucket_rollups {where} ORDER BY sample_period ASC, sample_count DESC LIMIT ?",
            [*params, 1000],
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["metrics"] = _loads(item.pop("metrics_json"), {})
        item["evidence"] = _loads(item.pop("evidence_json"), {})
        out.append(item)
    dimension_order = {dimension: idx for idx, dimension in enumerate(BUCKET_DIMENSIONS)}
    out.sort(key=lambda row: (row["sample_period"], dimension_order.get(row["dimension"], 999), -row["sample_count"], row["bucket_key"]))
    limit = max(1, min(1000, limit))
    return {"schema_version": SCHEMA_VERSION, "count": len(out[:limit]), "buckets": out[:limit], "total": len(out), "generated_at": _now()}


def _score_value(row: dict[str, Any], score_name: str) -> float | None:
    features = row.get("features") or {}
    if score_name == "signal_score":
        return _num(features.get("signal_score"), None)
    if score_name == "cost_liquidity_score":
        cost = _num(features.get("cost_bps"), None)
        if cost is None:
            return None
        return max(0.0, 100.0 - cost * 10.0)
    if score_name == "composite_score":
        signal = _num(features.get("signal_score"), 50.0)
        cost = _num(features.get("cost_bps"), 6.0)
        rr = _num(features.get("planned_rr"), 1.0)
        return signal - cost * 3.0 + min(20.0, rr * 10.0)
    return None


def _score_quantiles(rows: list[dict[str, Any]], score_name: str) -> list[tuple[str, list[dict[str, Any]]]]:
    scored = [(row, _score_value(row, score_name)) for row in rows]
    scored = [(row, score) for row, score in scored if score is not None and not math.isnan(score)]
    scored.sort(key=lambda item: item[1])
    if not scored:
        return []
    buckets = []
    for idx, label in enumerate(("Q1", "Q2", "Q3", "Q4", "Q5")):
        start = int(len(scored) * idx / 5)
        end = int(len(scored) * (idx + 1) / 5)
        buckets.append((label, [row for row, _ in scored[start:end]]))
    return buckets


def rebuild_scores_payload(
    project_root: Path,
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    with _connect(db_path) as conn:
        rows = _feature_rows(conn, experiment_id=experiment_id, strategy_line=strategy_line, parameter_set_id=parameter_set_id, limit=100000)
        if not rows:
            return {"schema_version": SCHEMA_VERSION, "status": "empty", "validations": [], "count": 0}
        exp = experiment_id or rows[0]["experiment_id"]
        strategies = sorted({row["strategy_line"] for row in rows if row.get("strategy_line")})
        validations = []
        now = _now()
        for line in strategies:
            line_rows = [row for row in rows if row.get("strategy_line") == line]
            for score_name in ("signal_score", "cost_liquidity_score", "composite_score"):
                metrics_by_split: dict[str, dict[str, Any]] = {}
                bucket_metrics: dict[str, list[dict[str, Any]]] = {}
                for split in ("train", "validation", "test"):
                    split_rows = [row for row in line_rows if row.get("train_split") == split]
                    buckets = _score_quantiles(split_rows, score_name)
                    bucket_metrics[split] = [
                        {"bucket": label, "sample_count": len(items), "metrics": _target_metrics(items)}
                        for label, items in buckets
                    ]
                    metrics_by_split[split] = _target_metrics(split_rows)
                def mono(split: str) -> bool:
                    vals = [b["metrics"]["expectancy_R"] for b in bucket_metrics.get(split, []) if b["sample_count"] > 0]
                    return len(vals) >= 3 and vals[-1] >= vals[0] and sum(1 for a, b in zip(vals, vals[1:]) if b >= a) >= len(vals) - 2
                q45_test = []
                for label, items in _score_quantiles([row for row in line_rows if row.get("train_split") == "test"], score_name):
                    if label in {"Q4", "Q5"}:
                        q45_test.extend(items)
                test_all = [row for row in line_rows if row.get("train_split") == "test"]
                q45_metrics = _target_metrics(q45_test)
                coverage = len(q45_test) / len(test_all) if test_all else 0.0
                train_mono = mono("train")
                val_mono = mono("validation")
                test_mono = mono("test")
                overfit = "low" if train_mono and val_mono and test_mono and q45_metrics["profit_factor"] > 1 and coverage >= 0.1 else "medium" if val_mono and q45_metrics["profit_factor"] > 0.9 else "high"
                validation = {
                    "validation_id": _stable_id("btgateval", {"exp": exp, "param": parameter_set_id, "line": line, "score": score_name}, 24),
                    "experiment_id": exp,
                    "parameter_set_id": parameter_set_id,
                    "strategy_line": line,
                    "score_name": score_name,
                    "bucket_count": 5,
                    "train_monotonic": train_mono,
                    "validation_monotonic": val_mono,
                    "test_monotonic": test_mono,
                    "best_cutoff": "Q4+Q5",
                    "pf_before": metrics_by_split["test"]["profit_factor"],
                    "pf_after_test": q45_metrics["profit_factor"],
                    "expectancy_after_test": q45_metrics["expectancy_R"],
                    "trade_coverage_test": round(coverage, 8),
                    "overfit_risk": overfit,
                    "metrics": {"splits": metrics_by_split, "buckets": bucket_metrics, "selected_test": q45_metrics},
                    "generated_at": now,
                }
                validations.append(validation)
        if not dry_run:
            if exp:
                clause = "experiment_id = ?"
                args: list[Any] = [exp]
                if parameter_set_id:
                    clause += " AND parameter_set_id = ?"
                    args.append(parameter_set_id)
                if strategy_line and strategy_line != "all":
                    clause += " AND strategy_line = ?"
                    args.append(strategy_line)
                conn.execute(f"DELETE FROM backtest_gate_score_validations WHERE {clause}", args)
            conn.executemany(
                """
                INSERT OR REPLACE INTO backtest_gate_score_validations(
                  validation_id, experiment_id, parameter_set_id, strategy_line, score_name,
                  bucket_count, train_monotonic, validation_monotonic, test_monotonic,
                  best_cutoff, pf_before, pf_after_test, expectancy_after_test, trade_coverage_test,
                  overfit_risk, metrics_json, schema_version, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["validation_id"],
                        row["experiment_id"],
                        row["parameter_set_id"],
                        row["strategy_line"],
                        row["score_name"],
                        row["bucket_count"],
                        1 if row["train_monotonic"] else 0,
                        1 if row["validation_monotonic"] else 0,
                        1 if row["test_monotonic"] else 0,
                        row["best_cutoff"],
                        row["pf_before"],
                        row["pf_after_test"],
                        row["expectancy_after_test"],
                        row["trade_coverage_test"],
                        row["overfit_risk"],
                        _json(row["metrics"]),
                        SCHEMA_VERSION,
                        now,
                    )
                    for row in validations
                ],
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "dry_run": dry_run,
        "count": len(validations),
        "score_count": len(validations),
        "validations": validations,
        "generated_at": _now(),
    }


def scores_payload(project_root: Path, *, limit: int = 200, **filters: Any) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    clauses = []
    params: list[Any] = []
    for key in ("experiment_id", "parameter_set_id", "strategy_line", "score_name", "overfit_risk"):
        value = filters.get(key)
        if value in (None, "", "all"):
            continue
        clauses.append(f"{key} = ?")
        params.append(value)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"SELECT * FROM backtest_gate_score_validations {where} ORDER BY pf_after_test DESC, trade_coverage_test DESC LIMIT ?",
            [*params, max(1, min(1000, limit))],
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["train_monotonic"] = bool(item["train_monotonic"])
        item["validation_monotonic"] = bool(item["validation_monotonic"])
        item["test_monotonic"] = bool(item["test_monotonic"])
        item["status"] = "shadow"
        item["metrics"] = _loads(item.pop("metrics_json"), {})
        out.append(item)
    return {"schema_version": SCHEMA_VERSION, "count": len(out), "scores": out, "generated_at": _now()}


def _bucket_candidate_rules(
    rows: list[dict[str, Any]],
    *,
    min_samples: int,
) -> list[dict[str, Any]]:
    by_split = _group_by_split(rows)
    train_rows = by_split.get("train") or rows
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    candidate_dimensions = [dim for dim in BUCKET_DIMENSIONS if _known_at_entry_rule(dim) and dim != "strategy_line"]
    for row in train_rows:
        for dim in candidate_dimensions:
            key = _gate_value(row, dim)
            if key == "unknown":
                continue
            grouped[(dim, key)].append(row)
    rules: list[dict[str, Any]] = []
    for (dimension, bucket_key), items in grouped.items():
        if len(items) < min_samples:
            continue
        metrics = _target_metrics(items)
        if _num(metrics.get("expectancy_R"), 0.0) >= 0:
            continue
        if _num(metrics.get("profit_factor"), 0.0) >= 0.95:
            continue
        rules.append(
            {
                "mode": "exclude_bucket",
                "dimension": dimension,
                "bucket_key": bucket_key,
                "gate_action": "wait_only",
                "known_at_entry": True,
                "source": "trade_quality_bucket_rollup",
                "train_bucket_metrics": metrics,
            }
        )
    rules.sort(
        key=lambda rule: (
            _num((rule.get("train_bucket_metrics") or {}).get("expectancy_R"), 0.0),
            _num((rule.get("train_bucket_metrics") or {}).get("profit_factor"), 0.0),
        )
    )
    return rules


def _validate_bucket_candidate(
    rows: list[dict[str, Any]],
    rule: dict[str, Any],
    *,
    min_test_pf: float,
    min_coverage: float,
) -> dict[str, Any]:
    by_split = _group_by_split(rows)
    train_metrics = _split_metrics(by_split.get("train") or [], rule)
    validation_metrics = _split_metrics(by_split.get("validation") or [], rule)
    test_metrics = _split_metrics(by_split.get("test") or [], rule)
    all_metrics = _split_metrics(rows, rule)
    test_pf = _num(test_metrics.get("after", {}).get("profit_factor"), 0.0)
    coverage = _num(test_metrics.get("kept_coverage"), 0.0)
    risk = _candidate_overfit_risk(
        train_metrics,
        validation_metrics,
        test_metrics,
        min_test_pf=min_test_pf,
        min_coverage=min_coverage,
    )
    status = "shadow"
    evidence = {
        "candidate_kind": "bucket_exclusion",
        "known_at_entry": bool(rule.get("known_at_entry")),
        "root_cause_counts_excluded": _root_cause_counts([row for row in rows if _rule_matches(row, rule)]),
        "all_metrics": all_metrics,
        "guardrails": {
            "min_test_pf": min_test_pf,
            "min_coverage": min_coverage,
            "no_future_fields": True,
            "action": "shadow_only_wait_not_block",
        },
    }
    patch = {
        "trade_quality_gate": {
            "enabled": False,
            "mode": "shadow",
            "source": "backtest_trade_quality_gate",
            "candidate_kind": "bucket_exclusion",
            "rules": [
                {
                    "dimension": rule["dimension"],
                    "bucket_key": rule["bucket_key"],
                    "action": "wait_only",
                    "known_at_entry": True,
                }
            ],
            "shadow_only": True,
        }
    }
    return {
        "gate_type": _gate_type_for_dimension(str(rule.get("dimension") or "")),
        "status": status,
        "rule": rule,
        "config_patch_preview": patch,
        "train_metrics": train_metrics,
        "validation_metrics": validation_metrics,
        "test_metrics": test_metrics,
        "pf_before": test_metrics.get("before", {}).get("profit_factor"),
        "pf_after_test": test_pf,
        "trade_coverage_test": coverage,
        "overfit_risk": risk,
        "evidence": evidence,
    }


def backfill_market_regime_features_payload(
    project_root: Path,
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = "strategy6",
    parameter_set_id: str | None = None,
    limit: int = 100000,
    dry_run: bool = True,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    now = _now()
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = _feature_rows(
            conn,
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            limit=limit,
        )
        btc_cache: dict[int, dict[str, Any]] = {}
        breadth_cache: dict[int, dict[str, Any]] = {}
        sidecars = []
        no_future_violations = 0
        for row in rows:
            entry_ms = row.get("entry_time_ms")
            btc = _btc_regime_for_entry(conn, entry_ms, row.get("side"), btc_cache)
            breadth = _market_breadth_for_entry(conn, entry_ms, breadth_cache)
            source_status = {
                **(btc.get("source_status") or {}),
                **(breadth.get("source_status") or {}),
                "funding_regime": "missing_source",
                "oi_direction": "missing_source",
            }
            known_core = [
                btc.get("btc_trend"),
                btc.get("btc_volatility"),
                btc.get("btc_alignment"),
                breadth.get("market_breadth"),
            ]
            regime_quality = "complete" if all(value not in (None, "", "unknown") for value in known_core) else "partial"
            minute_ms = _entry_minute_ms(entry_ms)
            lag = btc.get("asof_lag_seconds")
            if lag is not None and lag < 0:
                no_future_violations += 1
            sidecars.append(
                {
                    "sample_id": row["sample_id"],
                    "diagnostic_id": row.get("diagnostic_id"),
                    "experiment_id": row["experiment_id"],
                    "parameter_set_id": row["parameter_set_id"],
                    "strategy_line": row["strategy_line"],
                    "symbol": row["symbol"],
                    "side": row["side"],
                    "entry_time_ms": entry_ms,
                    "btc_trend": btc.get("btc_trend") or "unknown",
                    "btc_volatility": btc.get("btc_volatility") or "unknown",
                    "btc_alignment": btc.get("btc_alignment") or "unknown",
                    "market_breadth": breadth.get("market_breadth") or "unknown",
                    "funding_regime": "unknown",
                    "oi_direction": "unknown",
                    "regime_quality": regime_quality,
                    "regime_source": "p21_klines_1m_asof",
                    "source_status": source_status,
                    "asof_lag_seconds": lag if lag is not None else (0.0 if minute_ms is not None else None),
                    "generated_at": now,
                }
            )
        if not dry_run:
            conn.executemany(
                """
                INSERT OR REPLACE INTO backtest_gate_market_regime_features(
                  sample_id, diagnostic_id, experiment_id, parameter_set_id, strategy_line, symbol, side,
                  entry_time_ms, btc_trend, btc_volatility, btc_alignment, market_breadth,
                  funding_regime, oi_direction, regime_quality, regime_source, source_status_json,
                  asof_lag_seconds, schema_version, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item["sample_id"],
                        item["diagnostic_id"],
                        item["experiment_id"],
                        item["parameter_set_id"],
                        item["strategy_line"],
                        item["symbol"],
                        item["side"],
                        item["entry_time_ms"],
                        item["btc_trend"],
                        item["btc_volatility"],
                        item["btc_alignment"],
                        item["market_breadth"],
                        item["funding_regime"],
                        item["oi_direction"],
                        item["regime_quality"],
                        item["regime_source"],
                        _json(item["source_status"]),
                        item["asof_lag_seconds"],
                        SCHEMA_VERSION,
                        now,
                    )
                    for item in sidecars
                ],
            )
    coverage: dict[str, dict[str, int]] = {}
    for field in ("btc_trend", "btc_volatility", "btc_alignment", "market_breadth", "funding_regime", "oi_direction", "regime_quality"):
        coverage[field] = dict(_counts(item.get(field) for item in sidecars))
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "dry_run": dry_run,
        "selected_samples": len(rows),
        "materialized_regime_features": 0 if dry_run else len(sidecars),
        "coverage": coverage,
        "source_status": dict(_counts(status for item in sidecars for status in (item.get("source_status") or {}).values() if isinstance(status, str))),
        "no_future_violations": no_future_violations,
        "generated_at": now,
    }


COMPOSITE_GATE_DIMENSIONS = (
    "session",
    "side",
    "session_side",
    "hour_utc",
    "symbol_group",
    "score_bucket",
    "planned_rr_bucket",
    "cost_bucket",
    "entry_mode",
    "btc_trend",
    "btc_volatility",
    "btc_alignment",
    "market_breadth",
    "funding_regime",
    "oi_direction",
)


def _condition_matches(row: dict[str, Any], conditions: list[dict[str, str]]) -> bool:
    return all(_gate_value(row, cond["dimension"]) == str(cond["value"]) for cond in conditions)


def _condition_split_metrics(rows: list[dict[str, Any]], conditions: list[dict[str, str]]) -> dict[str, Any]:
    before = list(rows)
    excluded = [row for row in before if _condition_matches(row, conditions)]
    kept = [row for row in before if not _condition_matches(row, conditions)]
    before_metrics = _target_metrics(before)
    after_metrics = _target_metrics(kept)
    excluded_metrics = _target_metrics(excluded)
    count = before_metrics["trade_count"]
    return {
        "before": before_metrics,
        "after": after_metrics,
        "excluded": excluded_metrics,
        "profit_factor": after_metrics["profit_factor"],
        "expectancy_R": after_metrics["expectancy_R"],
        "trade_count": after_metrics["trade_count"],
        "excluded_trade_count": excluded_metrics["trade_count"],
        "kept_coverage": round(after_metrics["trade_count"] / count, 8) if count else 0.0,
        "excluded_coverage": round(excluded_metrics["trade_count"] / count, 8) if count else 0.0,
    }


def _candidate_conditions(rows: list[dict[str, Any]], *, min_samples: int, max_depth: int) -> list[list[dict[str, str]]]:
    by_dim: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        for dim in COMPOSITE_GATE_DIMENSIONS:
            value = _gate_value(row, dim)
            if value == "unknown":
                continue
            by_dim[dim][value] = by_dim[dim].get(value, 0) + 1
    atoms = [
        {"dimension": dim, "op": "eq", "value": value}
        for dim, values in by_dim.items()
        for value, count in values.items()
        if count >= min_samples
    ]
    out: list[list[dict[str, str]]] = []
    seen: set[str] = set()
    for depth in range(2, max(2, max_depth) + 1):
        for combo in combinations(atoms, depth):
            dims = [item["dimension"] for item in combo]
            if len(set(dims)) != len(dims):
                continue
            key = _json(sorted(combo, key=lambda item: item["dimension"]))
            if key in seen:
                continue
            seen.add(key)
            matched = sum(1 for row in rows if _condition_matches(row, list(combo)))
            if matched >= min_samples:
                out.append(list(combo))
    return out


def strategy6_market_regime_gate_search_report_payload(
    project_root: Path,
    *,
    experiment_id: str | None = None,
    min_train_samples: int = 10,
    max_depth: int = 3,
    max_filtered_coverage: float = 0.40,
    min_kept_coverage: float = 0.50,
    min_ready_pf: float = 1.0,
    dry_run: bool = False,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    backfill = backfill_market_regime_features_payload(
        project_root,
        experiment_id=experiment_id,
        strategy_line="strategy6",
        limit=100000,
        dry_run=dry_run,
    )
    with _connect(db_path) as conn:
        rows = _feature_rows(conn, experiment_id=experiment_id, strategy_line="strategy6", limit=100000)
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(str(row.get("experiment_id") or ""), str(row.get("parameter_set_id") or ""))].append(row)
    candidates = []
    evaluated = 0
    for (exp, param), group_rows in groups.items():
        split_rows = _group_by_split(group_rows)
        train_rows = split_rows.get("train") or []
        validation_rows = split_rows.get("validation") or []
        if len(train_rows) < min_train_samples or len(validation_rows) < 5:
            continue
        baseline_train = _target_metrics(train_rows)
        for conditions in _candidate_conditions(train_rows, min_samples=min_train_samples, max_depth=max_depth):
            evaluated += 1
            train_metrics = _condition_split_metrics(train_rows, conditions)
            if train_metrics["excluded_trade_count"] < min_train_samples:
                continue
            if train_metrics["excluded_coverage"] > max_filtered_coverage:
                continue
            if train_metrics["after"]["profit_factor"] <= baseline_train["profit_factor"]:
                continue
            if train_metrics["excluded"]["expectancy_R"] >= 0:
                continue
            validation_metrics = _condition_split_metrics(validation_rows, conditions)
            if validation_metrics["kept_coverage"] < min_kept_coverage:
                continue
            if validation_metrics["after"]["profit_factor"] <= validation_metrics["before"]["profit_factor"]:
                continue
            holdout_rows = [row for row in rows if row.get("parameter_set_id") != param]
            holdout_metrics = _condition_split_metrics(holdout_rows, conditions)
            if holdout_metrics["kept_coverage"] < min_kept_coverage:
                status = "reject"
                risk = "high"
            elif holdout_metrics["after"]["profit_factor"] > min_ready_pf and holdout_metrics["after"]["profit_factor"] > holdout_metrics["before"]["profit_factor"]:
                status = "ready_for_config_patch_review"
                risk = "medium" if holdout_metrics["excluded_trade_count"] < 30 else "low"
            elif holdout_metrics["after"]["profit_factor"] > holdout_metrics["before"]["profit_factor"]:
                status = "shadow_candidate_only"
                risk = "medium"
            else:
                status = "reject"
                risk = "high"
            candidates.append(
                {
                    "candidate_id": _stable_id("btgatecomp", {"exp": exp, "param": param, "conditions": conditions}, 24),
                    "experiment_id": exp,
                    "parameter_set_id": param,
                    "strategy_line": "strategy6",
                    "rule": {
                        "mode": "exclude_composite_bucket",
                        "gate_action": "wait_only",
                        "known_at_entry": True,
                        "conditions": conditions,
                        "source": "strategy6_market_regime_gate_search",
                    },
                    "status": status,
                    "overfit_risk": risk,
                    "train_metrics": train_metrics,
                    "validation_metrics": validation_metrics,
                    "test_metrics": holdout_metrics,
                    "pf_before": holdout_metrics["before"]["profit_factor"],
                    "pf_after_test": holdout_metrics["after"]["profit_factor"],
                    "trade_coverage_test": holdout_metrics["kept_coverage"],
                }
            )
    candidates.sort(
        key=lambda row: (
            row["status"] != "ready_for_config_patch_review",
            -_num(row.get("pf_after_test"), 0.0),
            -_num(row.get("trade_coverage_test"), 0.0),
        )
    )
    status_counts = dict(_counts(row["status"] for row in candidates))
    report_path = _write_strategy6_market_regime_report(
        project_root,
        backfill=backfill,
        rows=rows,
        evaluated=evaluated,
        candidates=candidates,
        status_counts=status_counts,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "backfill": backfill,
        "sample_count": len(rows),
        "parameter_groups": len(groups),
        "evaluated_combinations": evaluated,
        "candidate_count": len(candidates),
        "status_counts": status_counts,
        "top_candidates": candidates[:20],
        "report_path": str(report_path),
        "generated_at": _now(),
    }


def _rule_text(rule: dict[str, Any]) -> str:
    conditions = rule.get("conditions") or []
    if not conditions:
        return "-"
    return " AND ".join(f"{item.get('dimension')}={item.get('value')}" for item in conditions)


def _write_strategy6_market_regime_report(
    project_root: Path,
    *,
    backfill: dict[str, Any],
    rows: list[dict[str, Any]],
    evaluated: int,
    candidates: list[dict[str, Any]],
    status_counts: dict[str, int],
) -> Path:
    reports_dir = project_root / "docs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = reports_dir / f"STEP21.65_strategy6_market_regime_feature_backfill_gate_search_{ts}.md"
    coverage_lines = []
    for field, counts in (backfill.get("coverage") or {}).items():
        coverage_lines.append(f"- `{field}`: `{counts}`")
    top_lines = []
    for idx, row in enumerate(candidates[:10], start=1):
        top_lines.append(
            "| {rank} | {status} | {rule} | {before} | {after} | {coverage} | {risk} |".format(
                rank=idx,
                status=row.get("status"),
                rule=_rule_text(row.get("rule") or {}),
                before=row.get("pf_before"),
                after=row.get("pf_after_test"),
                coverage=row.get("trade_coverage_test"),
                risk=row.get("overfit_risk"),
            )
        )
    if not top_lines:
        top_lines.append("| - | - | no candidate | - | - | - | - |")
    text = "\n".join(
        [
            "# STEP21.65 Strategy6 Market Regime Feature Backfill Gate Search",
            "",
            f"> generated_at: {_now()}",
            "> boundary: shadow-only / wait-only; no Strategy6 evaluator, paper, runtime, or config mutation",
            "",
            "## Summary",
            "",
            "Market regime features were backfilled as entry-known sidecar evidence using as-of `entry_time` lookup, then Strategy6 composite gate search was rerun with regime dimensions enabled.",
            "",
            "## Data Scope",
            "",
            f"- source table: `backtest_gate_feature_samples`",
            f"- sidecar table: `backtest_gate_market_regime_features`",
            f"- strategy_line: `strategy6`",
            f"- samples: `{len(rows)}`",
            f"- backfilled sidecar rows: `{backfill.get('materialized_regime_features')}`",
            f"- no_future_violations: `{backfill.get('no_future_violations')}`",
            "",
            "## Coverage",
            "",
            *coverage_lines,
            "",
            "## Composite Gate Search",
            "",
            f"- evaluated combinations: `{evaluated}`",
            f"- candidate count: `{len(candidates)}`",
            f"- status counts: `{status_counts}`",
            "",
            "| Rank | Status | Rule | Holdout PF Before | Holdout PF After | Kept Coverage | Risk |",
            "| --- | --- | --- | ---: | ---: | ---: | --- |",
            *top_lines,
            "",
            "## Contract Checks",
            "",
            "- All market regime fields are read-model / sidecar features.",
            "- Gate rules remain `known_at_entry=true` and `gate_action=wait_only`.",
            "- `funding_regime` and `oi_direction` remain `unknown` when no entry-time historical source exists.",
            "- No config patch is applied by this task.",
            "",
        ]
    )
    path.write_text(text, encoding="utf-8")
    return path
    patch = {
        "trade_quality_gate": {
            "enabled": False,
            "mode": "shadow",
            "source": "backtest_trade_quality_gate",
            "candidate_kind": "bucket_exclusion",
            "rules": [
                {
                    "dimension": rule["dimension"],
                    "bucket_key": rule["bucket_key"],
                    "action": "wait_only",
                    "known_at_entry": True,
                }
            ],
            "shadow_only": True,
        }
    }
    return {
        "gate_type": _gate_type_for_dimension(str(rule.get("dimension") or "")),
        "status": status,
        "rule": rule,
        "config_patch_preview": patch,
        "train_metrics": train_metrics,
        "validation_metrics": validation_metrics,
        "test_metrics": test_metrics,
        "pf_before": test_metrics.get("before", {}).get("profit_factor"),
        "pf_after_test": test_pf,
        "trade_coverage_test": coverage,
        "overfit_risk": risk,
        "evidence": evidence,
    }


def generate_candidates_payload(
    project_root: Path,
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    min_test_pf: float = 1.0,
    min_coverage: float = 0.05,
    dry_run: bool = True,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    with _connect(db_path) as conn:
        validations = scores_payload(
            project_root,
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            limit=500,
        )["scores"]
        candidates = []
        now = _now()
        feature_rows = _feature_rows(
            conn,
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            limit=100000,
        )
        line_groups: dict[tuple[str, str | None, str], list[dict[str, Any]]] = defaultdict(list)
        for row in feature_rows:
            if str(row.get("strategy_line") or "") not in {"strategy5", "strategy6"}:
                continue
            line_groups[
                (
                    str(row.get("experiment_id") or experiment_id or ""),
                    row.get("parameter_set_id") or parameter_set_id,
                    str(row.get("strategy_line") or ""),
                )
            ].append(row)
        for (exp, param, line), line_rows in line_groups.items():
            min_bucket_samples = max(5, int(min(500, max(1, len(line_rows) * 0.005))))
            for rule in _bucket_candidate_rules(line_rows, min_samples=min_bucket_samples)[:80]:
                candidate_body = _validate_bucket_candidate(
                    line_rows,
                    rule,
                    min_test_pf=min_test_pf,
                    min_coverage=min_coverage,
                )
                if _num(candidate_body.get("pf_after_test"), 0.0) < min_test_pf:
                    continue
                if _num(candidate_body.get("trade_coverage_test"), 0.0) < min_coverage:
                    continue
                candidate = {
                    "candidate_id": _stable_id(
                        "btgatecand",
                        {
                            "exp": exp,
                            "param": param,
                            "line": line,
                            "rule": candidate_body["rule"],
                        },
                        24,
                    ),
                    "experiment_id": exp,
                    "parameter_set_id": param,
                    "strategy_line": line,
                    "generated_at": now,
                    **candidate_body,
                }
                candidates.append(candidate)
        for val in validations:
            if _num(val.get("pf_after_test"), 0.0) < min_test_pf or _num(val.get("trade_coverage_test"), 0.0) < min_coverage:
                continue
            rule = {"score_name": val["score_name"], "cutoff": val["best_cutoff"], "mode": "shadow"}
            patch = {
                "trade_quality_gate": {
                    "enabled": False,
                    "mode": "shadow",
                    "source": "backtest_gate_scoring",
                    "score_name": val["score_name"],
                    "cutoff": val["best_cutoff"],
                    "shadow_only": True,
                }
            }
            metrics = val.get("metrics") or {}
            candidate = {
                "candidate_id": _stable_id("btgatecand", {"v": val["validation_id"], "rule": rule}, 24),
                "experiment_id": val["experiment_id"],
                "parameter_set_id": val.get("parameter_set_id"),
                "strategy_line": val["strategy_line"],
                "gate_type": "score_threshold",
                "status": "shadow",
                "rule": rule,
                "config_patch_preview": patch,
                "train_metrics": (metrics.get("splits") or {}).get("train") or {},
                "validation_metrics": (metrics.get("splits") or {}).get("validation") or {},
                "test_metrics": (metrics.get("splits") or {}).get("test") or {},
                "pf_before": val.get("pf_before"),
                "pf_after_test": val.get("pf_after_test"),
                "trade_coverage_test": val.get("trade_coverage_test"),
                "overfit_risk": val.get("overfit_risk") or "unknown",
                "evidence": {"validation_id": val["validation_id"], "score_metrics": metrics},
                "generated_at": now,
            }
            candidates.append(candidate)
        candidates.sort(key=lambda row: (-_num(row.get("pf_after_test"), 0.0), -_num(row.get("trade_coverage_test"), 0.0)))
        if not dry_run:
            for row in candidates:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO backtest_gate_candidates(
                      candidate_id, experiment_id, parameter_set_id, strategy_line, gate_type, status,
                      rule_json, config_patch_preview_json, train_metrics_json, validation_metrics_json,
                      test_metrics_json, pf_before, pf_after_test, trade_coverage_test, overfit_risk,
                      evidence_json, schema_version, generated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["candidate_id"],
                        row["experiment_id"],
                        row["parameter_set_id"],
                        row["strategy_line"],
                        row["gate_type"],
                        row["status"],
                        _json(row["rule"]),
                        _json(row["config_patch_preview"]),
                        _json(row["train_metrics"]),
                        _json(row["validation_metrics"]),
                        _json(row["test_metrics"]),
                        row["pf_before"],
                        row["pf_after_test"],
                        row["trade_coverage_test"],
                        row["overfit_risk"],
                        _json(row["evidence"]),
                        SCHEMA_VERSION,
                        now,
                    ),
                )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "dry_run": dry_run,
        "count": len(candidates),
        "candidate_count": len(candidates),
        "candidates": candidates,
        "generated_at": _now(),
    }


def candidates_payload(project_root: Path, *, limit: int = 200, **filters: Any) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_gate_scoring_tables(db_path)
    clauses = []
    params: list[Any] = []
    for key in ("candidate_id", "experiment_id", "parameter_set_id", "strategy_line", "gate_type", "status", "overfit_risk"):
        value = filters.get(key)
        if value in (None, "", "all"):
            continue
        clauses.append(f"{key} = ?")
        params.append(value)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"SELECT * FROM backtest_gate_candidates {where} ORDER BY pf_after_test DESC, trade_coverage_test DESC LIMIT ?",
            [*params, max(1, min(1000, limit))],
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        for key in ("rule_json", "config_patch_preview_json", "train_metrics_json", "validation_metrics_json", "test_metrics_json", "evidence_json"):
            item[key[:-5] if key.endswith("_json") else key] = _loads(item.pop(key), {})
        out.append(item)
    return {"schema_version": SCHEMA_VERSION, "count": len(out), "candidates": out, "generated_at": _now()}


def recommendations_payload(project_root: Path, *, limit: int = 100, **filters: Any) -> dict[str, Any]:
    payload = candidates_payload(project_root, limit=limit, status=filters.get("status") or "shadow", **{k: v for k, v in filters.items() if k != "status"})
    recommendations = []
    for row in payload["candidates"]:
        recommendations.append(
            {
                "recommendation_id": _stable_id("btgaterec", row["candidate_id"], 24),
                "candidate_id": row["candidate_id"],
                "status": "shadow_only",
                "target_profile": filters.get("target_profile") or "review_only",
                "strategy_line": row["strategy_line"],
                "patch_json": row.get("config_patch_preview") or {},
                "evidence": {
                    "pf_before": row.get("pf_before"),
                    "pf_after_test": row.get("pf_after_test"),
                    "trade_coverage_test": row.get("trade_coverage_test"),
                    "overfit_risk": row.get("overfit_risk"),
                },
            }
        )
    return {"schema_version": SCHEMA_VERSION, "count": len(recommendations), "recommendations": recommendations, "generated_at": _now()}
