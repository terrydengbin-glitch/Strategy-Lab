from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.research_db import download_oi_funding_sources_payload, ensure_research_tables
from scripts.step28_2_bounded_paper_equivalent_parameter_search import _shadow_orders, _top_candidates
from scripts.step7_143_strategy5_6_best_params_v5_trade_gate_e2e_backtest import _entry_features_from_order


TASK_ID = "STEP28.7"
SCHEMA_VERSION = "step28.7.s5-s6-gate-feature-lineage-repair.v1"
OUT_DIR = ROOT / "DATA" / "backtest" / "step28"
REPORT_DIR = ROOT / "docs" / "reports"
OUTPUT_JSON = OUT_DIR / "step28_7_s5_s6_gate_feature_lineage_repair.json"
TARGET_LINES = ("strategy5", "strategy6")
REQUIRED_FIELDS = {
    "strategy5": ("price_flow_alignment", "side_flow_alignment"),
    "strategy6": ("funding_bucket", "funding_crowded_side"),
}
UNAVAILABLE_VALUES = {"missing", "missing_source", "unknown", "feature_unavailable_for_scope", ""}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(p21_db_path(ROOT))
    conn.row_factory = sqlite3.Row
    return conn


def _feature_state(features: dict[str, Any], line: str) -> tuple[dict[str, Any], list[str], list[str]]:
    status = features.get("market_context_source_status")
    missing: list[str] = []
    unavailable: list[str] = []
    values: dict[str, Any] = {}
    for field in REQUIRED_FIELDS[line]:
        value = features.get(field)
        values[field] = value
        if value is None or value == "":
            missing.append(field)
            continue
        if str(value) in UNAVAILABLE_VALUES:
            unavailable.append(field)
        if isinstance(status, dict):
            item = status.get(field)
            if isinstance(item, dict) and item.get("quality") == "missing":
                unavailable.append(field)
    return values, sorted(set(missing)), sorted(set(unavailable))


def _repair_order_features(conn: sqlite3.Connection, candidate: dict[str, Any], order: dict[str, Any], generated_at: str) -> dict[str, Any]:
    fallback = order.get("features") if isinstance(order.get("features"), dict) else {}
    try:
        repaired = _entry_features_from_order(
            conn,
            str(candidate.get("experiment_id") or order.get("experiment_id") or ""),
            str(candidate.get("parameter_set_id") or order.get("parameter_set_id") or ""),
            str(candidate.get("strategy_line") or order.get("strategy_line") or ""),
            candidate.get("parameters") if isinstance(candidate.get("parameters"), dict) else {},
            {**order, "features": fallback},
            generated_at,
        )
    except Exception as exc:
        return {**fallback, "feature_repair_error": f"{type(exc).__name__}:{exc}"}
    return {**fallback, **(repaired if isinstance(repaired, dict) else {})}


def _candidate_orders(conn: sqlite3.Connection, *, top_per_line: int, max_orders: int) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    candidates = [row for row in _top_candidates(conn, top_per_line=top_per_line) if row.get("strategy_line") in TARGET_LINES]
    out: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
    for candidate in candidates:
        out.append(
            (
                candidate,
                _shadow_orders(
                    conn,
                    experiment_id=str(candidate["experiment_id"]),
                    parameter_set_id=str(candidate["parameter_set_id"]),
                    strategy_line=str(candidate["strategy_line"]),
                    max_orders=max_orders,
                ),
            )
        )
    return out


def _source_window(candidate_orders: list[tuple[dict[str, Any], list[dict[str, Any]]]]) -> dict[str, Any]:
    symbols: set[str] = set()
    times: list[int] = []
    for candidate, orders in candidate_orders:
        if candidate.get("strategy_line") != "strategy6":
            continue
        for order in orders:
            symbol = str(order.get("symbol") or "").upper()
            if symbol:
                symbols.add(symbol)
            got = order.get("entry_time_ms") or order.get("signal_time_ms")
            if got:
                times.append(int(got))
    if not symbols or not times:
        return {"symbols": [], "start_ms": None, "end_ms": None}
    return {
        "symbols": sorted(symbols),
        "start_ms": min(times) - 8 * 60 * 60 * 1000,
        "end_ms": max(times) + 60 * 60 * 1000,
    }


def _audit(conn: sqlite3.Connection, candidate_orders: list[tuple[dict[str, Any], list[dict[str, Any]]]], generated_at: str) -> dict[str, Any]:
    per_line: dict[str, Any] = {}
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate, orders in candidate_orders:
        line = str(candidate.get("strategy_line"))
        raw_missing = Counter()
        repaired_missing = Counter()
        repaired_unavailable = Counter()
        raw_available = Counter()
        repaired_available = Counter()
        for order in orders:
            raw = order.get("features") if isinstance(order.get("features"), dict) else {}
            _, missing_raw, unavailable_raw = _feature_state(raw, line)
            raw_missing.update(missing_raw or unavailable_raw)
            raw_available.update(field for field in REQUIRED_FIELDS[line] if field not in set(missing_raw + unavailable_raw))
            repaired = _repair_order_features(conn, candidate, order, generated_at)
            values, missing, unavailable = _feature_state(repaired, line)
            repaired_missing.update(missing)
            repaired_unavailable.update(unavailable)
            repaired_available.update(field for field in REQUIRED_FIELDS[line] if field not in set(missing + unavailable))
            if len(examples[line]) < 5:
                examples[line].append(
                    {
                        "symbol": order.get("symbol"),
                        "side": order.get("side"),
                        "entry_time_ms": order.get("entry_time_ms"),
                        "values": values,
                        "missing": missing,
                        "unavailable": unavailable,
                    }
                )
        per_line[line] = {
            "strategy_line": line,
            "parameter_set_id": candidate.get("parameter_set_id"),
            "orders_checked": len(orders),
            "required_fields": list(REQUIRED_FIELDS[line]),
            "raw_missing_or_unavailable_counts": dict(raw_missing),
            "raw_available_counts": dict(raw_available),
            "repaired_missing_counts": dict(repaired_missing),
            "repaired_unavailable_counts": dict(repaired_unavailable),
            "repaired_available_counts": dict(repaired_available),
            "examples": examples[line],
        }
    return per_line


def _write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"STEP28.7_s5_s6_gate_feature_lineage_repair_{_stamp()}.md"
    lines = [
        "# STEP28.7 S5/S6 Gate Feature Lineage Repair",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- output_json: `{OUTPUT_JSON.relative_to(ROOT)}`",
        f"- top_per_line: `{payload['request']['top_per_line']}`",
        f"- max_orders_per_candidate: `{payload['request']['max_orders_per_candidate']}`",
        f"- funding_download_ok: `{(payload.get('market_context_download') or {}).get('ok')}`",
        "",
        "## Per Line",
        "",
        "| strategy_line | parameter_set_id | checked | raw missing/unavailable | repaired missing | repaired unavailable | repaired available |",
        "| --- | --- | ---: | --- | --- | --- | --- |",
    ]
    for row in payload.get("lineage_audit", {}).values():
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                row.get("strategy_line"),
                row.get("parameter_set_id"),
                row.get("orders_checked"),
                json.dumps(row.get("raw_missing_or_unavailable_counts") or {}, ensure_ascii=False),
                json.dumps(row.get("repaired_missing_counts") or {}, ensure_ascii=False),
                json.dumps(row.get("repaired_unavailable_counts") or {}, ensure_ascii=False),
                json.dumps(row.get("repaired_available_counts") or {}, ensure_ascii=False),
            )
        )
    lines.extend(
        [
            "",
            "## Contract",
            "",
            "- S5 flow alignment is rebuilt from entry-known P21 kline/taker flow features.",
            "- S6 funding features are only accepted when `market_funding_8h` has entry-known rows.",
            "- Missing funding source is reported as unavailable, not promoted as a valid gate signal.",
            "- No runtime paper config or source business DB schema is changed.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-per-line", type=int, default=2)
    parser.add_argument("--max-orders-per-candidate", type=int, default=100)
    parser.add_argument("--sleep-sec", type=float, default=0.02)
    parser.add_argument("--skip-market-download", action="store_true")
    args = parser.parse_args()
    generated_at = _now()
    with _connect() as conn:
        ensure_research_tables(conn)
        candidate_orders = _candidate_orders(conn, top_per_line=args.top_per_line, max_orders=args.max_orders_per_candidate)
        window = _source_window(candidate_orders)
    market_context_download: dict[str, Any] = {"ok": None, "skipped": True, "reason": "skip_market_download"}
    if not args.skip_market_download and window.get("symbols") and window.get("start_ms") and window.get("end_ms"):
        market_context_download = download_oi_funding_sources_payload(
            ROOT,
            symbols=list(window["symbols"]),
            start_ms=int(window["start_ms"]),
            end_ms=int(window["end_ms"]),
            sleep_sec=float(args.sleep_sec),
        )
    with _connect() as conn:
        ensure_research_tables(conn)
        lineage = _audit(conn, candidate_orders, generated_at)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "generated_at": generated_at,
        "request": {
            "top_per_line": int(args.top_per_line),
            "max_orders_per_candidate": int(args.max_orders_per_candidate),
            "target_lines": list(TARGET_LINES),
        },
        "source_window": window,
        "market_context_download": market_context_download,
        "lineage_audit": lineage,
        "next_steps": [
            "rerun STEP28.2 with the repaired feature materializer",
            "rerun STEP28.3 and compare feature_missing counts",
        ],
    }
    _write_json(OUTPUT_JSON, payload)
    report = _write_report(payload)
    print(json.dumps({"status": "ok", "output": str(OUTPUT_JSON), "report": str(report)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
