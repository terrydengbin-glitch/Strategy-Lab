from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21_v2 import (
    download_kline_cache_payload,
    kline_cache_status_payload,
    run_config_matrix_streaming_payload,
    universe_symbols,
)
from laoma_signal_engine.research_db import (
    REQUIRED_ENTRY_FEATURES,
    download_oi_funding_sources_payload,
    p21_db_path as research_db_path,
    refresh_entry_features_market_context,
)

TARGET_PARAMETER_SET_ID = "s6v32_edcd6b1030331422"
GRID_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_focused_60params_STEP21_54.json"
PROGRESS_PATH = ROOT / "DATA" / "backtest" / "strategy6_s6v32_p24_oi_funding_STEP7_128_progress.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_s6v32_p24_oi_funding_STEP7_128_result.json"
SOURCE_PATH = ROOT / "DATA" / "backtest" / "strategy6_s6v32_p24_oi_funding_STEP7_128_source.json"
REPORT_DIR = ROOT / "docs" / "reports"
MARKET_CONTEXT_FIELDS = {
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
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_target() -> dict[str, Any]:
    data = json.loads(GRID_PATH.read_text(encoding="utf-8"))
    for item in data.get("parameter_sets", []):
        if item.get("parameter_set_id") == TARGET_PARAMETER_SET_ID:
            return item
    raise RuntimeError(f"parameter set not found: {TARGET_PARAMETER_SET_ID}")


def _json_loads(raw: Any) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _source_window(symbols: list[str]) -> dict[str, Any]:
    db = research_db_path(ROOT)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" for _ in symbols)
        row = conn.execute(
            f"""
            SELECT MIN(open_time_ms) AS min_ms, MAX(open_time_ms) AS max_ms
            FROM p21_klines_1m
            WHERE symbol IN ({placeholders})
            """,
            tuple(symbols),
        ).fetchone()
        return {"min_ms": row["min_ms"], "max_ms": row["max_ms"]}
    finally:
        conn.close()


def _source_counts(symbols: list[str]) -> dict[str, Any]:
    db = research_db_path(ROOT)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" for _ in symbols)
        oi = conn.execute(
            f"""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT symbol) AS symbols,
                   MIN(source_time_ms) AS min_ms, MAX(source_time_ms) AS max_ms
            FROM market_oi_15m
            WHERE symbol IN ({placeholders})
            """,
            tuple(symbols),
        ).fetchone()
        funding = conn.execute(
            f"""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT symbol) AS symbols,
                   MIN(funding_time_ms) AS min_ms, MAX(funding_time_ms) AS max_ms
            FROM market_funding_8h
            WHERE symbol IN ({placeholders})
            """,
            tuple(symbols),
        ).fetchone()
        return {"oi": dict(oi), "funding": dict(funding)}
    finally:
        conn.close()


def _p24_audit(experiment_id: str) -> dict[str, Any]:
    db = research_db_path(ROOT)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        facts = conn.execute(
            """
            SELECT *
            FROM research_trade_facts
            WHERE source_type = 'backtest'
              AND strategy_line = 'strategy6'
              AND experiment_id = ?
              AND parameter_set_id = ?
            """,
            (experiment_id, TARGET_PARAMETER_SET_ID),
        ).fetchall()
        features = conn.execute(
            """
            SELECT *
            FROM research_entry_features
            WHERE source_type = 'backtest'
              AND strategy_line = 'strategy6'
              AND parameter_set_id = ?
              AND sample_id IN (
                SELECT sample_id
                FROM research_trade_facts
                WHERE experiment_id = ?
                  AND parameter_set_id = ?
              )
            """,
            (TARGET_PARAMETER_SET_ID, experiment_id, TARGET_PARAMETER_SET_ID),
        ).fetchall()
    finally:
        conn.close()

    missing_fact = Counter()
    proxy_fact = Counter()
    for row in facts:
        fq = _json_loads(row["field_quality_json"]) or {}
        missing_fact.update(fq.get("missing_fields", []) or [])
        proxy_fact.update(fq.get("proxy_fields", []) or [])

    required_missing = Counter()
    source_quality: dict[str, Counter[str]] = defaultdict(Counter)
    no_lookahead_violations = 0
    source_after_entry_violations = 0
    max_feature_key_count = 0
    for row in features:
        payload = _json_loads(row["features_json"]) or {}
        max_feature_key_count = max(max_feature_key_count, len(payload))
        missing_fields = _json_loads(row["missing_fields_json"]) or []
        for field in missing_fields:
            if field in REQUIRED_ENTRY_FEATURES:
                required_missing[field] += 1
        status = payload.get("market_context_source_status") or {}
        if isinstance(status, dict):
            for field in MARKET_CONTEXT_FIELDS:
                item = status.get(field) or {}
                if isinstance(item, dict):
                    source_quality[field][str(item.get("quality") or "unknown")] += 1
                    known_at = item.get("known_at")
                    if known_at is not None and row["entry_time_ms"] is not None and int(known_at) > int(row["entry_time_ms"]):
                        source_after_entry_violations += 1
                else:
                    source_quality[field]["missing_lineage"] += 1
        known_at_ms = row["known_at_ms"]
        entry_time_ms = row["entry_time_ms"]
        if known_at_ms is not None and entry_time_ms is not None and int(known_at_ms) > int(entry_time_ms):
            no_lookahead_violations += 1

    return {
        "research_db": str(db),
        "fact_rows": len(facts),
        "feature_rows": len(features),
        "feature_coverage": round((len(features) / len(facts)) if facts else 0.0, 6),
        "max_feature_key_count": max_feature_key_count,
        "missing_fact_fields_top": missing_fact.most_common(20),
        "proxy_fact_fields_top": proxy_fact.most_common(20),
        "missing_required_feature_fields_top": required_missing.most_common(30),
        "market_context_source_quality": {field: dict(counter) for field, counter in sorted(source_quality.items())},
        "no_lookahead_violations": no_lookahead_violations,
        "source_after_entry_violations": source_after_entry_violations,
    }


def _write_report(result: dict[str, Any], source: dict[str, Any], source_counts: dict[str, Any], p24: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP7.128_strategy6_s6v32_p24_oi_funding_observed_reaudit_{ts}.md"
    best = result.get("best") or {}
    metrics = best.get("metrics") or {}
    lines = [
        "# STEP7.128 Strategy6 s6v32 P24 OI / Funding Observed Reaudit",
        "",
        f"- generated_at: `{_now()}`",
        f"- experiment_id: `{result.get('experiment_id')}`",
        f"- parameter_set_id: `{TARGET_PARAMETER_SET_ID}`",
        f"- research_db: `{p24.get('research_db')}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        f"- source_json: `{SOURCE_PATH.relative_to(ROOT)}`",
        f"- progress_json: `{PROGRESS_PATH.relative_to(ROOT)}`",
        "",
        "## Source Store Download",
        "",
        f"- ok: `{source.get('ok')}`",
        f"- requested_symbols: `{source.get('symbols')}`",
        f"- downloaded_oi_rows: `{source.get('oi_rows')}`",
        f"- downloaded_funding_rows: `{source.get('funding_rows')}`",
        f"- errors: `{len(source.get('errors') or [])}`",
        f"- source_counts: `{json.dumps(source_counts, ensure_ascii=False)}`",
        "",
        "## Backtest Result",
        "",
        f"- strategy_line: `strategy6`",
        f"- symbols: `{metrics.get('symbol_count') or result.get('symbol_count')}`",
        f"- trade_count: `{result.get('trade_count')}`",
        f"- profit_factor: `{metrics.get('profit_factor')}`",
        f"- expectancy_R: `{metrics.get('expectancy_R')}`",
        f"- win_rate: `{metrics.get('win_rate')}`",
        f"- total_R: `{metrics.get('total_R')}`",
        "",
        "## P24 Writer Audit",
        "",
        f"- research_trade_facts rows: `{p24.get('fact_rows')}`",
        f"- research_entry_features rows: `{p24.get('feature_rows')}`",
        f"- feature coverage: `{p24.get('feature_coverage')}`",
        f"- max feature key count: `{p24.get('max_feature_key_count')}`",
        f"- no-lookahead violations: `{p24.get('no_lookahead_violations')}`",
        f"- source-after-entry violations: `{p24.get('source_after_entry_violations')}`",
        "",
        "## Missing Required P24 Feature Fields",
        "",
        "| field | rows |",
        "| --- | ---: |",
    ]
    for field, count in p24.get("missing_required_feature_fields_top") or []:
        lines.append(f"| `{field}` | {count} |")
    if not p24.get("missing_required_feature_fields_top"):
        lines.append("| none | 0 |")
    lines.extend(["", "## Market Context Source Quality", "", "| field | observed | proxy | missing | other |", "| --- | ---: | ---: | ---: | ---: |"])
    for field, counts in (p24.get("market_context_source_quality") or {}).items():
        observed = int(counts.get("observed") or 0)
        proxy = int(counts.get("proxy") or 0)
        missing = int(counts.get("missing") or 0)
        other = sum(int(v or 0) for k, v in counts.items() if k not in {"observed", "proxy", "missing"})
        lines.append(f"| `{field}` | {observed} | {proxy} | {missing} | {other} |")
    lines.extend(
        [
            "",
            "## Judgment",
            "",
            "- OI / funding are expected to be observed when Binance historical source covers the entry_time.",
            "- Depth imbalance remains outside this task because historical order book is not available from regular Binance REST.",
            "- This audit does not change strategy logic, configuration, paper runtime, or promotion status.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-symbols", type=int, default=100)
    parser.add_argument("--max-workers", type=int, default=6)
    parser.add_argument("--symbol-shard-size", type=int, default=10)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-source-download", action="store_true")
    args = parser.parse_args()

    target = _load_target()
    symbols = [s.upper() for s in universe_symbols(ROOT, limit=args.max_symbols)[: args.max_symbols]]
    if not args.skip_download:
        status = kline_cache_status_payload(ROOT, symbols=symbols, days=30, max_symbols=len(symbols))
        missing = [row["symbol"] for row in status.get("symbols", []) if row.get("status") != "ready"]
        if missing:
            print(json.dumps({"phase": "download_missing_klines", "missing_count": len(missing), "symbols": missing[:10]}, ensure_ascii=False), flush=True)
            download_kline_cache_payload(ROOT, symbols=missing, days=30, max_symbols=len(missing), sleep_sec=0.02)

    window = _source_window(symbols)
    if not window.get("min_ms") or not window.get("max_ms"):
        raise RuntimeError("kline_source_window_missing")
    source_start = max(0, int(window["min_ms"]) - 24 * 60 * 60 * 1000)
    source_end = int(window["max_ms"])
    if args.skip_source_download:
        source_payload = {"ok": True, "skipped": True, "symbols": len(symbols), "oi_rows": 0, "funding_rows": 0, "errors": []}
    else:
        print(json.dumps({"phase": "download_oi_funding", "symbols": len(symbols), "start_ms": source_start, "end_ms": source_end}, ensure_ascii=False), flush=True)
        source_payload = download_oi_funding_sources_payload(
            ROOT,
            symbols=symbols,
            start_ms=source_start,
            end_ms=source_end,
            sleep_sec=0.03,
        )
    SOURCE_PATH.write_text(json.dumps(source_payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    source_counts = _source_counts(symbols)
    print(json.dumps({"phase": "source_counts", **source_counts}, ensure_ascii=False), flush=True)

    last = {"done": 0, "t": 0.0}

    def cb(progress: dict[str, Any]) -> None:
        payload = dict(progress)
        payload["updated_at"] = _now()
        PROGRESS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        done = int(payload.get("done_count") or 0)
        total = int(payload.get("total_count") or 0)
        now_ts = time.time()
        if done == 1 or done == total or done - int(last["done"]) >= 2 or now_ts - float(last["t"]) >= 30:
            last["done"] = done
            last["t"] = now_ts
            print(json.dumps({"phase": "backtest", "done_count": done, "total_count": total}, ensure_ascii=False), flush=True)

    payload = run_config_matrix_streaming_payload(
        ROOT,
        symbols=symbols,
        strategy_line="strategy6",
        days=30,
        max_symbols=len(symbols),
        max_sets=1,
        parameter_grid=[target],
        write=True,
        symbol_shard_size=args.symbol_shard_size,
        max_workers=args.max_workers,
        scheduler_mode="global_queue",
        progress_callback=cb,
    )
    result = {
        "schema_version": "step7.128-strategy6-s6v32-p24-oi-funding-observed-reaudit-result-v1",
        "generated_at": _now(),
        "target_parameter_set_id": TARGET_PARAMETER_SET_ID,
        "grid_path": str(GRID_PATH),
        "progress_path": str(PROGRESS_PATH),
        **payload,
    }
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    with sqlite3.connect(research_db_path(ROOT)) as conn:
        conn.row_factory = sqlite3.Row
        refreshed = refresh_entry_features_market_context(
            conn,
            experiment_id=str(result.get("experiment_id")),
            parameter_set_id=TARGET_PARAMETER_SET_ID,
            strategy_line="strategy6",
        )
        conn.commit()
    print(json.dumps({"phase": "refresh_p24_market_context", "rows": refreshed}, ensure_ascii=False), flush=True)
    p24 = _p24_audit(str(result.get("experiment_id")))
    report = _write_report(result, source_payload, source_counts, p24)
    print(json.dumps({"ok": True, "report": str(report), "source": source_payload, "p24": p24}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
