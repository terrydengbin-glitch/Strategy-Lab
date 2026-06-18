from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.paper_equivalent import (
    EXECUTION_CONTRACT,
    EXECUTION_CONTRACT_VERSION,
    PAPER_ADAPTER_VERSION,
    PAPER_GATE_VERSION,
    PaperEquivalentBacktestSession,
    default_paper_equivalent_config,
)
from laoma_signal_engine.paper.config import load_paper_config
from laoma_signal_engine.paper.models import Candle
from scripts.step7_146_strategy5_6_v5_gate_paper_equivalent_backtest import (
    _metrics_from_paper_orders,
    _paper_orders,
    _paper_skips,
    _trade_plan_doc,
)
from scripts.step7_143_strategy5_6_best_params_v5_trade_gate_e2e_backtest import _entry_features_from_order


TASK_ID = "STEP28.2"
SCHEMA_VERSION = "step28.2.bounded-paper-equivalent-parameter-search.v1"
OUT_DIR = ROOT / "DATA" / "backtest" / "step28"
REPORT_DIR = ROOT / "docs" / "reports"
OUTPUT_JSON = OUT_DIR / "step28_2_bounded_paper_equivalent_parameter_search.json"
TARGET_LINES = ("without_micro", "strategy4", "strategy5", "strategy6")
GATE_FEATURE_REPAIR_LINES = {"strategy5", "strategy6"}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _loads(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        got = float(value)
        if got != got or got in (float("inf"), float("-inf")):
            return default
        return got
    except Exception:
        return default


def _bucket_num(value: Any, cuts: list[float], labels: list[str]) -> str:
    got = _num(value, float("nan"))
    if got != got:
        return "unknown"
    for cut, label in zip(cuts, labels):
        if got <= cut:
            return label
    return labels[-1]


def _hour_session(hour: int) -> str:
    if 0 <= hour < 8:
        return "asia"
    if 8 <= hour < 16:
        return "eu"
    return "us"


def _materialize_gate_search_features(features: dict[str, Any], order: dict[str, Any]) -> dict[str, Any]:
    out = dict(features)
    side = str(order.get("side") or out.get("side") or "unknown").upper()
    symbol = str(order.get("symbol") or out.get("symbol") or "unknown").upper()
    out.setdefault("symbol", symbol)
    out.setdefault("side", side)
    out.setdefault("symbol_side", f"{symbol}:{side}")
    signal_ms = int(order.get("signal_time_ms") or order.get("entry_time_ms") or 0)
    if signal_ms > 0:
        dt = datetime.fromtimestamp(signal_ms / 1000, timezone.utc)
        out.setdefault("hour_utc", str(dt.hour))
        out.setdefault("session", _hour_session(dt.hour))
        out.setdefault("weekday", str(dt.weekday()))
        out.setdefault("session_side", f"{out.get('session')}:{side}")
    out.setdefault(
        "score_bucket",
        _bucket_num(order.get("score") or out.get("confidence"), [60, 70, 80, 90], ["score_le_60", "score_60_70", "score_70_80", "score_80_90", "score_gt_90"]),
    )
    entry = _num(order.get("entry_price"), 0.0)
    stop = _num(order.get("stop_loss"), 0.0)
    take = _num(order.get("take_profit"), 0.0)
    rr = order.get("planned_rr")
    if rr is None and entry and stop and take and abs(entry - stop) > 0:
        rr = abs(take - entry) / abs(entry - stop)
    out.setdefault("planned_rr_bucket", _bucket_num(rr, [0.5, 0.8, 1.0, 1.5], ["rr_le_0_5", "rr_0_5_0_8", "rr_0_8_1_0", "rr_1_0_1_5", "rr_gt_1_5"]))
    out.setdefault("atr_1m_bps_bucket", _bucket_num(out.get("atr_1m_bps"), [20, 50, 100], ["atr_le_20", "atr_20_50", "atr_50_100", "atr_gt_100"]))
    out.setdefault("pct_1m_bps_bucket", _bucket_num(out.get("pct_1m_bps"), [-50, -10, 10, 50], ["pct1m_lt_m50", "pct1m_m50_m10", "pct1m_m10_10", "pct1m_10_50", "pct1m_gt_50"]))
    out.setdefault("pct_5m_bps_bucket", _bucket_num(out.get("pct_5m_bps"), [-100, -30, 30, 100], ["pct5m_lt_m100", "pct5m_m100_m30", "pct5m_m30_30", "pct5m_30_100", "pct5m_gt_100"]))
    return out


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(p21_db_path(ROOT))
    conn.row_factory = sqlite3.Row
    return conn


def _top_candidates(conn: sqlite3.Connection, *, top_per_line: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in TARGET_LINES:
        rows = conn.execute(
            """
            SELECT *
            FROM p21_v2_30d_metrics
            WHERE strategy_line = ?
            ORDER BY json_extract(metrics_json, '$.profit_factor') DESC,
                     json_extract(metrics_json, '$.trade_count') DESC
            LIMIT ?
            """,
            (line, max(1, int(top_per_line))),
        ).fetchall()
        for row in rows:
            item = dict(row)
            item["metrics"] = _loads(item.pop("metrics_json", None), {})
            item["parameters"] = _loads(item.pop("parameters_json", None), {})
            out.append(item)
    return out


def _shadow_orders(
    conn: sqlite3.Connection,
    *,
    experiment_id: str,
    parameter_set_id: str,
    strategy_line: str,
    max_orders: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM p21_v2_shadow_orders
        WHERE experiment_id = ? AND parameter_set_id = ? AND strategy_line = ?
        ORDER BY signal_time_ms ASC, order_id ASC
        LIMIT ?
        """,
        (experiment_id, parameter_set_id, strategy_line, max(1, int(max_orders))),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["reasons"] = _loads(item.pop("reasons_json", None), [])
        item["features"] = _loads(item.pop("features_json", None), {})
        item["config_patch"] = _loads(item.pop("config_patch_json", None), {})
        item["trade_plan_payload"] = _loads(item.pop("trade_plan_payload_json", None), {})
        item["fill_result"] = _loads(item.pop("fill_result_json", None), {})
        item["fast_exit_policy"] = _loads(item.pop("fast_exit_policy_json", None), {})
        out.append(item)
    return out


def _candles_for_orders(conn: sqlite3.Connection, orders: list[dict[str, Any]]) -> dict[str, list[Candle]]:
    out: dict[str, list[Candle]] = {}
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for order in orders:
        by_symbol.setdefault(str(order.get("symbol") or "").upper(), []).append(order)
    for symbol, rows_for_symbol in by_symbol.items():
        if not symbol:
            continue
        min_ms = min(int(row.get("signal_time_ms") or row.get("entry_time_ms") or 0) for row in rows_for_symbol)
        max_ms = max(int(row.get("exit_time_ms") or row.get("entry_time_ms") or min_ms) for row in rows_for_symbol)
        before_ms = 60 * 60 * 1000
        after_ms = 6 * 60 * 60 * 1000
        rows = conn.execute(
            """
            SELECT symbol, open_time_ms, open, high, low, close, volume
            FROM p21_klines_1m
            WHERE symbol = ? AND open_time_ms BETWEEN ? AND ?
            ORDER BY open_time_ms ASC
            """,
            (symbol, min_ms - before_ms, max_ms + after_ms),
        ).fetchall()
        out[symbol] = [
            Candle(
                symbol=str(row["symbol"]).upper(),
                open_time_ms=int(row["open_time_ms"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"] or 0.0),
            )
            for row in rows
        ]
    return out


def _run_candidate(
    conn: sqlite3.Connection,
    candidate: dict[str, Any],
    *,
    max_orders: int,
    run_id_tag: str | None = None,
) -> dict[str, Any]:
    line = str(candidate["strategy_line"])
    parameter_set_id = str(candidate["parameter_set_id"])
    experiment_id = str(candidate["experiment_id"])
    orders = _shadow_orders(
        conn,
        experiment_id=experiment_id,
        parameter_set_id=parameter_set_id,
        strategy_line=line,
        max_orders=max_orders,
    )
    tag = f"_{run_id_tag}" if run_id_tag else ""
    run_id = f"step28_2_{line}_{parameter_set_id[:12]}{tag}_{_stamp()}"
    generated_at = _now()
    if not orders:
        return {
            "strategy_line": line,
            "parameter_set_id": parameter_set_id,
            "experiment_id": experiment_id,
            "paper_equivalent_run_id": run_id,
            "status": "blocked",
            "promotion_allowed": False,
            "promotion_block_reason": "no_shadow_orders_for_candidate",
        }
    candles_by_symbol = _candles_for_orders(conn, orders)
    missing_candles = sorted(symbol for symbol, candles in candles_by_symbol.items() if not candles)
    usable_orders = [order for order in orders if candles_by_symbol.get(str(order.get("symbol") or "").upper())]
    if not usable_orders:
        return {
            "strategy_line": line,
            "parameter_set_id": parameter_set_id,
            "experiment_id": experiment_id,
            "paper_equivalent_run_id": run_id,
            "status": "blocked",
            "promotion_allowed": False,
            "promotion_block_reason": "no_usable_candles_for_shadow_orders",
            "missing_candle_symbols": missing_candles[:20],
        }
    paper_config = default_paper_equivalent_config(run_id=run_id, base=load_paper_config(ROOT))
    db_path = ROOT / paper_config.db_path
    if db_path.exists():
        db_path.unlink()
    summary_path = ROOT / paper_config.summary_path
    if summary_path.exists():
        summary_path.unlink()
    session = PaperEquivalentBacktestSession(
        ROOT,
        run_id=run_id,
        config=paper_config,
        candles_by_symbol=candles_by_symbol,
    )
    consumed = 0
    consume_created = 0
    consume_skipped = 0
    reason_counter: Counter[str] = Counter()
    feature_repair_counter: Counter[str] = Counter()
    feature_unavailable_counter: Counter[str] = Counter()
    for order in usable_orders:
        features = order.get("features") if isinstance(order.get("features"), dict) else {}
        if line in GATE_FEATURE_REPAIR_LINES:
            features, feature_audit = _repaired_gate_features(
                conn,
                candidate=candidate,
                order=order,
                generated_at=generated_at,
                fallback=features,
            )
            feature_repair_counter.update(feature_audit.get("status_counts") or {})
            feature_unavailable_counter.update(feature_audit.get("unavailable_fields") or [])
        features = _materialize_gate_search_features(features, order)
        try:
            doc = _trade_plan_doc(
                root=ROOT,
                line=line,
                order=order,
                features=features,
                run_id=f"{run_id}_{order.get('order_id')}",
                cycle_id=f"cycle_{run_id}_{order.get('order_id')}",
                generated_at=generated_at,
            )
            result = session.consume_trade_plan(
                {line: doc},
                at_ms=int(order.get("signal_time_ms") or order.get("entry_time_ms") or 0),
            )
            consumed += 1
            consume_created += int(result.get("created") or 0)
            consume_skipped += len(result.get("skipped") or [])
        except Exception as exc:
            reason_counter[f"consume_error:{type(exc).__name__}"] += 1
    finished = session.finish()
    orders_rows = _paper_orders(ROOT, paper_config.db_path, line=line)
    skip_rows = _paper_skips(ROOT, paper_config.db_path, line=line)
    metrics = _metrics_from_paper_orders(orders_rows)
    gate_decisions = Counter(str(row.get("gate_decision") or "none") for row in [*orders_rows, *skip_rows])
    training = finished.get("training_dataset") if isinstance(finished, dict) else {}
    trade_count = int(metrics.get("trade_count") or metrics.get("closed_orders") or 0)
    pf = metrics.get("profit_factor")
    return {
        "strategy_line": line,
        "parameter_set_id": parameter_set_id,
        "experiment_id": experiment_id,
        "paper_equivalent_run_id": run_id,
        "status": "ok",
        "execution_contract": EXECUTION_CONTRACT,
        "execution_contract_version": EXECUTION_CONTRACT_VERSION,
        "paper_adapter_version": PAPER_ADAPTER_VERSION,
        "paper_gate_version": PAPER_GATE_VERSION,
        "db_path": str(db_path),
        "summary_path": str(summary_path),
        "source_shadow_pf": (candidate.get("metrics") or {}).get("profit_factor"),
        "source_shadow_trade_count": (candidate.get("metrics") or {}).get("trade_count"),
        "shadow_orders_selected": len(orders),
        "shadow_orders_replayed": len(usable_orders),
        "consumed_plans": consumed,
        "consume_created": consume_created,
        "consume_skipped": consume_skipped,
        "created_orders": len(orders_rows),
        "skip_rows": len(skip_rows),
        "gate_decisions": dict(gate_decisions),
        "missing_candle_symbols": missing_candles[:20],
        "consume_errors": dict(reason_counter),
        "gate_feature_lineage_repair": {
            "enabled": line in GATE_FEATURE_REPAIR_LINES,
            "status_counts": dict(feature_repair_counter),
            "unavailable_fields": dict(feature_unavailable_counter),
            "contract": "entry_known_rebuild_from_p21_shadow_order_and_p21_market_context",
        },
        "metrics": metrics,
        "profit_factor": pf,
        "trade_count": trade_count,
        "training_dataset": training,
        "training_dataset_status": (training or {}).get("training_dataset_status"),
        "promotion_allowed": False,
        "promotion_block_reason": "bounded_step28_2_candidate_requires_step28_3_gate_and_step28_4_validation",
    }


def _repaired_gate_features(
    conn: sqlite3.Connection,
    *,
    candidate: dict[str, Any],
    order: dict[str, Any],
    generated_at: str,
    fallback: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    line = str(candidate.get("strategy_line") or order.get("strategy_line") or "")
    required = {
        "strategy5": ("price_flow_alignment", "side_flow_alignment"),
        "strategy6": ("funding_bucket", "funding_crowded_side"),
    }.get(line, ())
    try:
        repaired = _entry_features_from_order(
            conn,
            str(candidate.get("experiment_id") or order.get("experiment_id") or ""),
            str(candidate.get("parameter_set_id") or order.get("parameter_set_id") or ""),
            line,
            candidate.get("parameters") if isinstance(candidate.get("parameters"), dict) else {},
            {**order, "features": fallback},
            generated_at,
        )
    except Exception as exc:
        return dict(fallback), {
            "status_counts": {f"repair_error:{type(exc).__name__}": 1},
            "unavailable_fields": list(required),
        }
    features = {**fallback, **(repaired if isinstance(repaired, dict) else {})}
    source_status = features.get("market_context_source_status")
    unavailable: list[str] = []
    if isinstance(source_status, dict):
        for key in required:
            item = source_status.get(key)
            if isinstance(item, dict) and item.get("quality") == "missing":
                unavailable.append(key)
    for key in required:
        if features.get(key) in (None, "", "missing"):
            unavailable.append(key)
    if unavailable:
        features["feature_unavailable_for_scope"] = sorted(set(unavailable))
    return features, {
        "status_counts": {"repaired": 1},
        "unavailable_fields": sorted(set(unavailable)),
    }


def _rank(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        results,
        key=lambda row: (
            float(row.get("profit_factor") or -1),
            int(row.get("trade_count") or 0),
            int(row.get("created_orders") or 0),
        ),
        reverse=True,
    )


def _write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"STEP28.2_bounded_paper_equivalent_parameter_search_{_stamp()}.md"
    lines = [
        "# STEP28.2 Bounded Paper-Equivalent Parameter Search",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- status: `{payload['status']}`",
        f"- output_json: `{OUTPUT_JSON.relative_to(ROOT)}`",
        f"- top_per_line: `{payload['request']['top_per_line']}`",
        f"- max_orders_per_candidate: `{payload['request']['max_orders_per_candidate']}`",
        "",
        "## Results",
        "",
        "| rank | strategy_line | parameter_set_id | source PF | paper-eq PF | trades | orders | skips | training |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for idx, row in enumerate(payload.get("leaderboard") or [], start=1):
        metrics = row.get("metrics") or {}
        training = row.get("training_dataset") or {}
        lines.append(
            "| {} | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                idx,
                row.get("strategy_line"),
                row.get("parameter_set_id"),
                row.get("source_shadow_pf", "-"),
                row.get("profit_factor", "-"),
                metrics.get("trade_count") or metrics.get("closed_orders") or row.get("trade_count") or 0,
                row.get("created_orders", 0),
                row.get("skip_rows", 0),
                training.get("training_dataset_status") or row.get("training_dataset_status") or "-",
            )
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This is a bounded STEP28.2 search run, not a promotion package.",
            "- All replayed candidates use paper-equivalent isolated ledgers.",
            "- Promotion remains blocked until STEP28.3 gate search and STEP28.4 joint validation.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-per-line", type=int, default=1)
    parser.add_argument("--max-orders-per-candidate", type=int, default=40)
    args = parser.parse_args()
    with _connect() as conn:
        candidates = _top_candidates(conn, top_per_line=args.top_per_line)
        results = [_run_candidate(conn, candidate, max_orders=args.max_orders_per_candidate) for candidate in candidates]
    leaderboard = _rank([row for row in results if row.get("status") == "ok"])
    payload = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "generated_at": _now(),
        "status": "ok" if results else "blocked",
        "request": {
            "top_per_line": int(args.top_per_line),
            "max_orders_per_candidate": int(args.max_orders_per_candidate),
            "target_lines": list(TARGET_LINES),
        },
        "execution_contract": EXECUTION_CONTRACT,
        "execution_contract_version": EXECUTION_CONTRACT_VERSION,
        "feature_scope": "kline_only",
        "per_strategy_config_policy": "independent",
        "search_mode": "bounded_topN_from_p21_candidates",
        "results": results,
        "leaderboard": leaderboard,
    }
    _write_json(OUTPUT_JSON, payload)
    report = _write_report(payload)
    print(json.dumps({"status": payload["status"], "output": str(OUTPUT_JSON), "report": str(report)}, ensure_ascii=False))
    return 0 if results else 2


if __name__ == "__main__":
    raise SystemExit(main())
