from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TASK_ID = "STEP28.3"
SCHEMA_VERSION = "step28.3.trade-quality-fast-gate-candidate-search.v1"
STEP28_2_JSON = ROOT / "DATA" / "backtest" / "step28" / "step28_2_bounded_paper_equivalent_parameter_search.json"
OUT_DIR = ROOT / "DATA" / "backtest" / "step28"
OUTPUT_JSON = OUT_DIR / "step28_3_trade_quality_fast_gate_candidate_search.json"
REPORT_DIR = ROOT / "docs" / "reports"

MICROSTRUCTURE_DENYLIST = {
    "order_book",
    "depth",
    "spread",
    "cvd",
    "ofi",
    "micro_lifecycle_state",
    "mfe_after_entry",
    "mae_after_entry",
    "future_pnl",
    "exit_reason_as_entry_feature",
}
UNAVAILABLE_FEATURE_VALUES = {"missing", "missing_source", "unknown", "feature_unavailable_for_scope", ""}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _loads(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(str(raw))
    except Exception:
        return default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        got = float(value)
        if math.isnan(got) or math.isinf(got):
            return default
        return got
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = [row for row in rows if _num(row.get("net_R"), 0.0) > 0]
    losses = [row for row in rows if _num(row.get("net_R"), 0.0) < 0]
    gross_profit = sum(_num(row.get("net_R"), 0.0) for row in wins)
    gross_loss = abs(sum(_num(row.get("net_R"), 0.0) for row in losses))
    total = sum(_num(row.get("net_R"), 0.0) for row in rows)
    return {
        "trade_count": len(rows),
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": round(len(wins) / len(rows), 8) if rows else 0.0,
        "gross_profit_R": round(gross_profit, 8),
        "gross_loss_R": round(gross_loss, 8),
        "profit_factor": round(gross_profit / gross_loss, 8) if gross_loss > 0 else (None if gross_profit == 0 else 999.0),
        "total_R": round(total, 8),
        "expectancy_R": round(total / len(rows), 8) if rows else 0.0,
    }


def _net_r(order: dict[str, Any]) -> float:
    entry = _num(order.get("filled_entry_price") or order.get("entry_price"), 0.0)
    stop = _num(order.get("stop_loss"), 0.0)
    qty = abs(_num(order.get("quantity") or order.get("planned_quantity"), 0.0))
    risk = abs(entry - stop) * qty
    pnl = _num(order.get("realized_pnl_usdt"), 0.0)
    if risk <= 0:
        risk = abs(_num(order.get("estimated_max_loss_usdt"), 0.0))
    return pnl / risk if risk > 0 else 0.0


def _bucket_num(value: Any, cuts: list[float], labels: list[str]) -> str:
    got = _num(value, float("nan"))
    if math.isnan(got):
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


def _dt_parts(value: Any) -> tuple[str, str, str]:
    text = str(value or "").replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        hour = dt.hour
        return str(hour), _hour_session(hour), str(dt.weekday())
    except Exception:
        return "unknown", "unknown", "unknown"


def _features_from_row(row: dict[str, Any]) -> dict[str, str]:
    source = _loads(row.get("source_json"), {})
    guards = _loads(row.get("guards_json"), {})
    if not guards and isinstance(source, dict):
        guards = source.get("guards") if isinstance(source.get("guards"), dict) else {}
    gate_features = _loads(row.get("gate_features_json"), {})
    generated_at = row.get("source_generated_at") or (source.get("generated_at") if isinstance(source, dict) else None) or row.get("opened_at")
    hour, session, weekday = _dt_parts(generated_at)
    confidence = source.get("confidence") if isinstance(source, dict) else None
    rr = source.get("rr") if isinstance(source, dict) else None
    if rr is None:
        entry = _num(row.get("entry_price"), 0.0)
        stop = _num(row.get("stop_loss"), 0.0)
        take = _num(row.get("take_profit"), 0.0)
        risk = abs(entry - stop)
        rr = abs(take - entry) / risk if risk > 0 else None
    out = {
        "symbol": str(row.get("symbol") or "unknown").upper(),
        "side": str(row.get("side") or "unknown").upper(),
        "symbol_side": f"{str(row.get('symbol') or 'unknown').upper()}:{str(row.get('side') or 'unknown').upper()}",
        "hour_utc": hour,
        "session": session,
        "session_side": f"{session}:{str(row.get('side') or 'unknown').upper()}",
        "weekday": weekday,
        "entry_mode": str((source.get("entry_mode") if isinstance(source, dict) else None) or row.get("source_entry_mode") or "unknown"),
        "score_bucket": _bucket_num(confidence, [60, 70, 80, 90], ["score_le_60", "score_60_70", "score_70_80", "score_80_90", "score_gt_90"]),
        "planned_rr_bucket": _bucket_num(rr, [0.5, 0.8, 1.0, 1.5], ["rr_le_0_5", "rr_0_5_0_8", "rr_0_8_1_0", "rr_1_0_1_5", "rr_gt_1_5"]),
        "atr_1m_bps_bucket": _bucket_num(guards.get("atr_1m_bps"), [20, 50, 100], ["atr_le_20", "atr_20_50", "atr_50_100", "atr_gt_100"]),
        "pct_1m_bps_bucket": _bucket_num(guards.get("pct_1m_bps"), [-50, -10, 10, 50], ["pct1m_lt_m50", "pct1m_m50_m10", "pct1m_m10_10", "pct1m_10_50", "pct1m_gt_50"]),
        "pct_5m_bps_bucket": _bucket_num(guards.get("pct_5m_bps"), [-100, -30, 30, 100], ["pct5m_lt_m100", "pct5m_m100_m30", "pct5m_m30_30", "pct5m_30_100", "pct5m_gt_100"]),
    }
    for key in ("funding_bucket", "funding_crowded_side", "side_flow_alignment", "price_flow_alignment"):
        if isinstance(gate_features, dict) and gate_features.get(key) not in (None, ""):
            out[key] = str(gate_features.get(key))
    return out


def _load_closed_orders(db_path: Path, strategy_line: str) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT o.*, tp.source_json AS source_json, tp.guards_json AS guards_json
            FROM paper_orders o
            LEFT JOIN paper_trade_plans tp ON tp.id = o.plan_id
            WHERE o.strategy_line = ? AND o.status = 'closed'
            ORDER BY COALESCE(o.opened_at, o.created_at), o.id
            """,
            (strategy_line,),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["net_R"] = _net_r(item)
        item["features"] = _features_from_row(item)
        out.append(item)
    return out


def _feature_missing_diagnostics(db_path: Path, strategy_line: str) -> dict[str, Any]:
    if not db_path.exists():
        return {"skip_rows": 0, "feature_missing_rows": 0, "missing_feature_counts": {}}
    counts: Counter[str] = Counter()
    gate_decisions: Counter[str] = Counter()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT gate_decision, skip_detail_json FROM paper_skip_ledger WHERE strategy_line = ?",
            (strategy_line,),
        ).fetchall()
    for row in rows:
        gate_decisions[str(row["gate_decision"] or "none")] += 1
        detail = _loads(row["skip_detail_json"], {})
        for key in detail.get("missing_features") or []:
            counts[str(key)] += 1
    return {
        "skip_rows": len(rows),
        "gate_decisions": dict(gate_decisions),
        "feature_missing_rows": gate_decisions.get("feature_missing", 0),
        "missing_feature_counts": dict(counts),
    }


def _rule_matches(row: dict[str, Any], rule: dict[str, Any]) -> bool:
    return str(row.get("features", {}).get(rule["field"], "unknown")) == str(rule["value"])


def _split_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    if not rows:
        return {"train": [], "validation": [], "test": []}
    n = len(rows)
    train_end = max(1, int(n * 0.6))
    val_end = max(train_end + 1, int(n * 0.8)) if n >= 3 else train_end
    return {
        "train": rows[:train_end],
        "validation": rows[train_end:val_end],
        "test": rows[val_end:],
    }


def _after_metrics(rows: list[dict[str, Any]], rule: dict[str, Any]) -> dict[str, Any]:
    kept = [row for row in rows if not _rule_matches(row, rule)]
    return {
        "before": _metrics(rows),
        "after": _metrics(kept),
        "blocked_count": len(rows) - len(kept),
        "pass_count": len(kept),
        "kept_coverage": round(len(kept) / len(rows), 8) if rows else 0.0,
    }


def _candidate_rules(rows: list[dict[str, Any]], *, min_samples: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for field, value in row.get("features", {}).items():
            if str(value) in UNAVAILABLE_FEATURE_VALUES or field in MICROSTRUCTURE_DENYLIST:
                continue
            grouped[(field, str(value))].append(row)
    rules: list[dict[str, Any]] = []
    for (field, value), items in grouped.items():
        if len(items) < min_samples:
            continue
        got = _metrics(items)
        if _num(got.get("expectancy_R"), 0.0) < 0 or (_num(got.get("profit_factor"), 999.0) < 0.9):
            rules.append(
                {
                    "field": field,
                    "op": "eq",
                    "value": value,
                    "action": "block_or_wait",
                    "known_at_entry": True,
                    "feature_scope": "kline_only",
                    "microstructure_fields_used": [],
                    "train_bucket_metrics": got,
                }
            )
    rules.sort(key=lambda rule: (_num(rule["train_bucket_metrics"].get("expectancy_R"), 0.0), _num(rule["train_bucket_metrics"].get("profit_factor"), 0.0)))
    return rules


def _evaluate_line(result: dict[str, Any], *, min_samples: int, min_coverage: float) -> dict[str, Any]:
    line = str(result.get("strategy_line"))
    db_path = Path(str(result.get("db_path") or ""))
    rows = _load_closed_orders(db_path, line)
    split = _split_rows(rows)
    rules = _candidate_rules(split["train"], min_samples=min_samples)
    diagnostics = _feature_missing_diagnostics(db_path, line)
    candidates: list[dict[str, Any]] = []
    for idx, rule in enumerate(rules[:20], start=1):
        train = _after_metrics(split["train"], rule)
        validation = _after_metrics(split["validation"], rule)
        test = _after_metrics(split["test"], rule)
        all_metrics = _after_metrics(rows, rule)
        test_pf = test["after"].get("profit_factor")
        coverage = _num(test.get("kept_coverage"), 0.0)
        overfit_risk = "high"
        if len(rows) >= 30 and len(split["test"]) >= 10 and coverage >= min_coverage and _num(test_pf, 0.0) > _num(test["before"].get("profit_factor"), 0.0):
            overfit_risk = "medium"
        candidate_id = f"step28_3_{line}_{idx}_{rule['field']}_{abs(hash(str(rule['value']))) % 100000}"
        candidates.append(
            {
                "gate_candidate_id": candidate_id,
                "strategy_line": line,
                "parameter_set_id": result.get("parameter_set_id"),
                "paper_equivalent_run_id": result.get("paper_equivalent_run_id"),
                "rule_json": {"operator": "AND", "rules": [{"field": rule["field"], "op": "eq", "value": rule["value"]}], "action": "block_or_wait"},
                "known_at_entry": True,
                "feature_scope": "kline_only",
                "microstructure_fields_used": [],
                "feature_completeness": "partial" if diagnostics.get("feature_missing_rows") else "available",
                "pf_before": all_metrics["before"].get("profit_factor"),
                "pf_after_train": train["after"].get("profit_factor"),
                "pf_after_validation": validation["after"].get("profit_factor"),
                "pf_after_test": test["after"].get("profit_factor"),
                "trade_coverage_test": coverage,
                "overfit_risk": overfit_risk,
                "train_metrics": train,
                "validation_metrics": validation,
                "test_metrics": test,
                "all_metrics": all_metrics,
                "promotion_allowed": False,
                "promotion_block_reason": "requires_step28_4_joint_validation" if overfit_risk != "high" else "high_overfit_or_small_sample",
            }
        )
    reason = None
    if not rows:
        reason = "no_closed_paper_equivalent_orders"
    elif len(rows) < min_samples:
        reason = "insufficient_closed_orders_for_gate_search"
    elif not candidates:
        reason = "no_entry_known_loss_bucket_candidate"
    return {
        "strategy_line": line,
        "parameter_set_id": result.get("parameter_set_id"),
        "paper_equivalent_run_id": result.get("paper_equivalent_run_id"),
        "db_path": result.get("db_path"),
        "closed_order_count": len(rows),
        "base_metrics": _metrics(rows),
        "feature_missing_diagnostics": diagnostics,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "no_candidate_reason": reason,
        "training_dataset_ref": (result.get("training_dataset") or {}).get("dataset_path"),
        "training_coverage_audit_ref": (result.get("training_dataset") or {}).get("coverage_path"),
    }


def build_payload(*, min_samples: int = 2, min_coverage: float = 0.25) -> dict[str, Any]:
    source = _loads(STEP28_2_JSON.read_text(encoding="utf-8"), {})
    results = source.get("results") if isinstance(source.get("results"), list) else []
    lines = [_evaluate_line(row, min_samples=min_samples, min_coverage=min_coverage) for row in results]
    all_candidates = [candidate for line in lines for candidate in line.get("candidates") or []]
    all_candidates.sort(key=lambda row: (_num(row.get("pf_after_test"), 0.0), _num(row.get("trade_coverage_test"), 0.0)), reverse=True)
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "generated_at": _now(),
        "status": "ok",
        "source_step28_2": str(STEP28_2_JSON.relative_to(ROOT)),
        "constraints": {
            "known_at_entry": True,
            "feature_scope": "kline_only",
            "microstructure_denylist": sorted(MICROSTRUCTURE_DENYLIST),
            "min_samples": min_samples,
            "min_coverage": min_coverage,
        },
        "line_results": lines,
        "candidate_count": len(all_candidates),
        "leaderboard": all_candidates[:20],
    }


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"STEP28.3_trade_quality_fast_gate_candidate_search_{_stamp()}.md"
    lines = [
        "# STEP28.3 Trade Quality Fast Gate Candidate Search",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- output_json: `{OUTPUT_JSON.relative_to(ROOT)}`",
        f"- candidate_count: `{payload['candidate_count']}`",
        f"- feature_scope: `{payload['constraints']['feature_scope']}`",
        f"- known_at_entry: `{payload['constraints']['known_at_entry']}`",
        "",
        "## Per Line",
        "",
        "| strategy_line | closed | base PF | candidates | no_candidate_reason | feature_missing | missing_features |",
        "| --- | ---: | ---: | ---: | --- | ---: | --- |",
    ]
    for row in payload["line_results"]:
        diag = row.get("feature_missing_diagnostics") or {}
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                row.get("strategy_line"),
                row.get("closed_order_count"),
                (row.get("base_metrics") or {}).get("profit_factor"),
                row.get("candidate_count"),
                row.get("no_candidate_reason") or "",
                diag.get("feature_missing_rows", 0),
                json.dumps(diag.get("missing_feature_counts") or {}, ensure_ascii=False),
            )
        )
    lines.extend(["", "## Candidate Leaderboard", "", "| rank | strategy_line | rule | pf_before | pf_after_test | coverage | risk |", "| ---: | --- | --- | ---: | ---: | ---: | --- |"])
    for idx, row in enumerate(payload.get("leaderboard") or [], start=1):
        lines.append(
            "| {} | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                idx,
                row.get("strategy_line"),
                json.dumps(row.get("rule_json"), ensure_ascii=False),
                row.get("pf_before"),
                row.get("pf_after_test"),
                row.get("trade_coverage_test"),
                row.get("overfit_risk"),
            )
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This is a fast gate discovery pass only.",
            "- No runtime config was changed.",
            "- High-overfit candidates are not promotion candidates.",
            "- Feature missing / unavailable diagnostics are retained for lineage governance.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    payload = build_payload()
    _write_json(OUTPUT_JSON, payload)
    report = write_report(payload)
    print(json.dumps({"status": payload["status"], "candidate_count": payload["candidate_count"], "output": str(OUTPUT_JSON), "report": str(report)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
