from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter
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
from laoma_signal_engine.research_db import REQUIRED_ENTRY_FEATURES, p21_db_path as research_db_path

TARGET_PARAMETER_SET_ID = "s6v32_edcd6b1030331422"
GRID_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_focused_60params_STEP21_54.json"
PROGRESS_PATH = ROOT / "DATA" / "backtest" / "strategy6_s6v32_p24_audit_STEP7_126_progress.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_s6v32_p24_audit_STEP7_126_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_target() -> dict[str, Any]:
    data = json.loads(GRID_PATH.read_text(encoding="utf-8"))
    for item in data.get("parameter_sets", []):
        if item.get("parameter_set_id") == TARGET_PARAMETER_SET_ID:
            return item
    raise RuntimeError(f"parameter set not found: {TARGET_PARAMETER_SET_ID}")


def _json_loads(raw: str | None) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _p24_audit(experiment_id: str) -> dict[str, Any]:
    db = research_db_path(ROOT)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        params = (experiment_id, TARGET_PARAMETER_SET_ID)
        facts = conn.execute(
            """
            SELECT *
            FROM research_trade_facts
            WHERE source_type = 'backtest'
              AND strategy_line = 'strategy6'
              AND experiment_id = ?
              AND parameter_set_id = ?
            """,
            params,
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

    missing = Counter()
    proxy = Counter()
    for row in facts:
        fq = _json_loads(row["field_quality_json"]) or {}
        for field in fq.get("missing_fields", []) or []:
            missing[field] += 1
        for field in fq.get("proxy_fields", []) or []:
            proxy[field] += 1
    feature_missing = Counter()
    required_feature_missing = Counter()
    feature_key_count = 0
    no_lookahead_violations = 0
    for row in features:
        payload = _json_loads(row["features_json"]) or {}
        feature_key_count = max(feature_key_count, len(payload))
        missing_fields = _json_loads(row["missing_fields_json"]) or []
        for field in missing_fields:
            feature_missing[field] += 1
            if field in REQUIRED_ENTRY_FEATURES:
                required_feature_missing[field] += 1
        known_at_ms = row["known_at_ms"]
        entry_time_ms = row["entry_time_ms"]
        if known_at_ms is not None and entry_time_ms is not None and int(known_at_ms) > int(entry_time_ms):
            no_lookahead_violations += 1

    return {
        "research_db": str(db),
        "fact_rows": len(facts),
        "feature_rows": len(features),
        "feature_coverage": round((len(features) / len(facts)) if facts else 0.0, 6),
        "max_feature_key_count": feature_key_count,
        "missing_fact_fields_top": missing.most_common(20),
        "proxy_fact_fields_top": proxy.most_common(20),
        "missing_required_feature_fields_top": required_feature_missing.most_common(20),
        "missing_feature_fields_top": feature_missing.most_common(20),
        "no_lookahead_violations": no_lookahead_violations,
    }


def _write_report(result: dict[str, Any], p24: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP7.126_strategy6_s6v32_p24_writer_audit_{ts}.md"
    best = result.get("best") or {}
    metrics = best.get("metrics") or {}
    lines = [
        "# STEP7.126 Strategy6 s6v32 P24 Backtest Writer Audit",
        "",
        f"- generated_at: `{_now()}`",
        f"- experiment_id: `{result.get('experiment_id')}`",
        f"- parameter_set_id: `{TARGET_PARAMETER_SET_ID}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        f"- progress_json: `{PROGRESS_PATH.relative_to(ROOT)}`",
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
        "",
        "## Missing Fact Fields",
        "",
        "| field | rows |",
        "| --- | ---: |",
    ]
    for field, count in p24.get("missing_fact_fields_top") or []:
        lines.append(f"| `{field}` | {count} |")
    lines.extend(["", "## Proxy Fact Fields", "", "| field | rows |", "| --- | ---: |"])
    for field, count in p24.get("proxy_fact_fields_top") or []:
        lines.append(f"| `{field}` | {count} |")
    lines.extend(["", "## Missing Required P24 Feature Fields", "", "| field | rows |", "| --- | ---: |"])
    for field, count in p24.get("missing_required_feature_fields_top") or []:
        lines.append(f"| `{field}` | {count} |")
    if not p24.get("missing_required_feature_fields_top"):
        lines.append("| none | 0 |")
    lines.extend(
        [
            "",
            "## Missing Optional / Strategy-Version Feature Fields",
            "",
            "These are version-specific optional keys present in the Strategy6 feature payload shape, not the baseline P24 required feature set.",
            "",
            "| field | rows |",
            "| --- | ---: |",
        ]
    )
    for field, count in p24.get("missing_feature_fields_top") or []:
        lines.append(f"| `{field}` | {count} |")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Single parameter Strategy6 backtest only.",
            "- No strategy evaluator, strategy config, paper ledger, runtime daemon, or production chain was changed.",
            "- This audit checks writer/data-chain correctness, not production promotion quality.",
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
    args = parser.parse_args()

    target = _load_target()
    symbols = [s.upper() for s in universe_symbols(ROOT, limit=args.max_symbols)[: args.max_symbols]]
    if not args.skip_download:
        status = kline_cache_status_payload(ROOT, symbols=symbols, days=30, max_symbols=len(symbols))
        missing = [row["symbol"] for row in status.get("symbols", []) if row.get("status") != "ready"]
        if missing:
            print(json.dumps({"phase": "download_missing", "missing_count": len(missing), "symbols": missing[:10]}, ensure_ascii=False), flush=True)
            download_kline_cache_payload(ROOT, symbols=missing, days=30, max_symbols=len(missing), sleep_sec=0.02)

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
        "schema_version": "step7.126-strategy6-s6v32-p24-audit-result-v1",
        "generated_at": _now(),
        "target_parameter_set_id": TARGET_PARAMETER_SET_ID,
        "grid_path": str(GRID_PATH),
        "progress_path": str(PROGRESS_PATH),
        **payload,
    }
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    p24 = _p24_audit(str(result.get("experiment_id")))
    report = _write_report(result, p24)
    print(
        json.dumps(
            {
                "result_path": str(RESULT_PATH),
                "report_path": str(report),
                "experiment_id": result.get("experiment_id"),
                "trade_count": result.get("trade_count"),
                "p24": p24,
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
