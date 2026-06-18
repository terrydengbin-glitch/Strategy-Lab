from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_trade_quality_v5 import (
    COMBO_GATE_SCHEMA_VERSION,
    ENTRY_KNOWN_RULE_FIELDS,
    SCHEMA_VERSION,
    _metrics,
    _safe_float,
    _split,
)


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _loads(raw: Any, fallback: Any = None) -> Any:
    if raw in (None, ""):
        return {} if fallback is None else fallback
    try:
        return json.loads(raw)
    except Exception:
        return {} if fallback is None else fallback


def _fmt(value: Any, digits: int = 3) -> str:
    v = _safe_float(value)
    if v is None:
        return "-"
    return f"{v:.{digits}f}"


def _pct(value: Any) -> str:
    v = _safe_float(value)
    if v is None:
        return "-"
    return f"{v * 100:.1f}%"


def _rule_text(rule: dict[str, Any]) -> str:
    items = rule.get("rules") or []
    if not items:
        return "-"
    return " AND ".join(f"{item.get('field')}={item.get('value')}" for item in items)


def _feature_value(row: dict[str, Any], field: str) -> Any:
    if field in row:
        return row.get(field)
    return (row.get("entry_features") or {}).get(field)


def _value_matches(actual: Any, expected: Any) -> bool:
    if actual is None:
        return False
    af = _safe_float(actual)
    ef = _safe_float(expected)
    if af is not None and ef is not None:
        return abs(af - ef) <= 1e-9
    return str(actual) == str(expected)


def _rule_matches(row: dict[str, Any], rule: dict[str, Any]) -> bool:
    if str(rule.get("operator") or "AND").upper() != "AND":
        return False
    rules = rule.get("rules") or []
    if not rules:
        return False
    for item in rules:
        field = str(item.get("field") or "")
        if field not in ENTRY_KNOWN_RULE_FIELDS:
            return False
        if not _value_matches(_feature_value(row, field), item.get("value")):
            return False
    return True


def _load_candidates(conn: sqlite3.Connection, top_per_strategy: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM trade_quality_combo_gate_validations_v5
        WHERE schema_version = ?
          AND strategy_line IN ('strategy5', 'strategy6')
          AND recommendation IN ('paper_shadow_ready', 'watch')
          AND overfit_risk IN ('low', 'medium')
        ORDER BY
          strategy_line,
          CASE recommendation WHEN 'paper_shadow_ready' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
          CASE overfit_risk WHEN 'low' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
          json_extract(split_metrics_json, '$.test.after.pf') DESC,
          json_extract(aggregate_metrics_json, '$.after.pf') DESC
        """,
        (COMBO_GATE_SCHEMA_VERSION,),
    ).fetchall()
    by_strategy: dict[str, list[dict[str, Any]]] = {}
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        item = dict(row)
        rule = _loads(item.get("rule_json"), {})
        key = (item["strategy_line"], item["parameter_set_id"], json.dumps(rule, sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        item["rule"] = rule
        item["split_metrics"] = _loads(item.get("split_metrics_json"), {})
        item["aggregate_metrics"] = _loads(item.get("aggregate_metrics_json"), {})
        by_strategy.setdefault(item["strategy_line"], []).append(item)
    selected: list[dict[str, Any]] = []
    for strategy in ("strategy5", "strategy6"):
        selected.extend(by_strategy.get(strategy, [])[:top_per_strategy])
    return selected


def _load_rejected_reference(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM trade_quality_combo_gate_validations_v5
        WHERE schema_version = ?
          AND strategy_line IN ('strategy5', 'strategy6')
          AND recommendation = 'reject'
        ORDER BY
          json_extract(split_metrics_json, '$.test.after.pf') DESC,
          json_extract(aggregate_metrics_json, '$.after.pf') DESC
        LIMIT 5
        """,
        (COMBO_GATE_SCHEMA_VERSION,),
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["rule"] = _loads(item.get("rule_json"), {})
        item["split_metrics"] = _loads(item.get("split_metrics_json"), {})
        item["aggregate_metrics"] = _loads(item.get("aggregate_metrics_json"), {})
        out.append(item)
    return out


def _load_samples(conn: sqlite3.Connection, candidate: dict[str, Any]) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM trade_quality_causal_factors_v5
        WHERE schema_version = ?
          AND package_key = ?
          AND strategy_line = ?
          AND parameter_set_id = ?
        ORDER BY entry_time_ms, causal_id
        """,
        (
            SCHEMA_VERSION,
            candidate["package_key"],
            candidate["strategy_line"],
            candidate["parameter_set_id"],
        ),
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        entry_features = _loads(item.get("entry_known_feature_set_json"), {})
        targets = _loads(item.get("target_diagnostic_set_json"), {})
        item["entry_features"] = entry_features
        item["targets"] = targets
        item["net_R"] = _safe_float(targets.get("net_R"))
        for field in ENTRY_KNOWN_RULE_FIELDS:
            if field in entry_features and field not in item:
                item[field] = entry_features[field]
        out.append(item)
    return out


def _evaluate_candidate(samples: list[dict[str, Any]], rule: dict[str, Any]) -> dict[str, Any]:
    matched = [row for row in samples if _rule_matches(row, rule)]
    kept = [row for row in samples if not _rule_matches(row, rule)]
    split_rows = _split(samples)
    split_metrics: dict[str, Any] = {}
    for split_name, rows in split_rows.items():
        split_matched = [row for row in rows if _rule_matches(row, rule)]
        split_kept = [row for row in rows if not _rule_matches(row, rule)]
        split_metrics[split_name] = {
            "before": _metrics(rows),
            "after": _metrics(split_kept),
            "removed": _metrics(split_matched),
            "removed_coverage": (len(split_matched) / len(rows)) if rows else 0.0,
        }
    return {
        "before": _metrics(samples),
        "after": _metrics(kept),
        "removed": _metrics(matched),
        "removed_coverage": (len(matched) / len(samples)) if samples else 0.0,
        "split_metrics": split_metrics,
        "samples": len(samples),
        "kept": len(kept),
        "removed_rows": len(matched),
    }


def _pf_improvement(result: dict[str, Any]) -> float | None:
    before = _safe_float((result.get("before") or {}).get("pf"))
    after = _safe_float((result.get("after") or {}).get("pf"))
    if before is None or after is None:
        return None
    return after - before


def _report(project_root: Path, db_path: Path, results: list[dict[str, Any]], rejected: list[dict[str, Any]]) -> Path:
    reports = project_root / "docs" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    path = reports / f"STEP7.133_strategy5_6_v5_trade_gate_backtest_experiment_{_stamp()}.md"
    pf_gt_one = [
        row
        for row in results
        if _safe_float((row["result"].get("split_metrics") or {}).get("test", {}).get("after", {}).get("pf")) is not None
        and _safe_float((row["result"].get("split_metrics") or {}).get("test", {}).get("after", {}).get("pf")) >= 1
    ]
    lines = [
        "# STEP7.133 Strategy5/6 V5 Trade Gate Backtest Experiment Audit",
        "",
        f"- generated_at: `{datetime.now(timezone.utc).isoformat()}`",
        f"- research_db: `{db_path}`",
        f"- combo_gate_schema: `{COMBO_GATE_SCHEMA_VERSION}`",
        f"- causal_schema: `{SCHEMA_VERSION}`",
        f"- evaluated_candidates: `{len(results)}`",
        f"- test_pf_gt_1: `{len(pf_gt_one)}`",
        "",
        "## Contract",
        "",
        "- Baseline analysis experiment only; no strategy/config/paper mutation.",
        "- Gate predicates use entry-known V5/P24 fields only.",
        "- This is a gate-filtered historical backtest over existing strategy5/strategy6 samples, not a live paper promotion.",
        "- Gate action is modeled as `shadow_block_or_downweight` by removing matched trades from the result set.",
        "",
        "## Gate-On Backtest Results",
        "",
        "| strategy | parameter_set | rule | before PF | after PF | test PF | PF delta | trades before | trades after | removed | removed % | WR after | expectancy after | max DD after | risk | recommendation |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    ordered = sorted(
        results,
        key=lambda row: (
            _safe_float((row["result"].get("split_metrics") or {}).get("test", {}).get("after", {}).get("pf")) or -1,
            _safe_float((row["result"].get("after") or {}).get("pf")) or -1,
        ),
        reverse=True,
    )
    for row in ordered:
        result = row["result"]
        after = result.get("after") or {}
        test_after = (result.get("split_metrics") or {}).get("test", {}).get("after", {})
        lines.append(
            "| `{strategy}` | `{param}` | `{rule}` | {before_pf} | {after_pf} | {test_pf} | {delta} | {before_n} | {after_n} | {removed_n} | {removed_pct} | {wr} | {exp} | {dd} | `{risk}` | `{rec}` |".format(
                strategy=row["strategy_line"],
                param=row["parameter_set_id"],
                rule=_rule_text(row["rule"]),
                before_pf=_fmt((result.get("before") or {}).get("pf")),
                after_pf=_fmt(after.get("pf")),
                test_pf=_fmt(test_after.get("pf")),
                delta=_fmt(_pf_improvement(result)),
                before_n=int((result.get("before") or {}).get("trades") or 0),
                after_n=int(after.get("trades") or 0),
                removed_n=int(result.get("removed_rows") or 0),
                removed_pct=_pct(result.get("removed_coverage")),
                wr=_pct(after.get("win_rate")),
                exp=_fmt(after.get("expectancy_R")),
                dd=_fmt(after.get("max_drawdown_R")),
                risk=row["overfit_risk"],
                rec=row["recommendation"],
            )
        )
    lines.extend(["", "## Split Detail", ""])
    for row in ordered[:10]:
        lines.extend(
            [
                f"### {row['strategy_line']} / {row['parameter_set_id']} / {_rule_text(row['rule'])}",
                "",
                "| split | before PF | after PF | removed PF | trades before | trades after | removed % | after total R |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for split_name in ("train", "validation", "test"):
            split = (row["result"].get("split_metrics") or {}).get(split_name, {})
            before = split.get("before") or {}
            after = split.get("after") or {}
            removed = split.get("removed") or {}
            lines.append(
                f"| `{split_name}` | {_fmt(before.get('pf'))} | {_fmt(after.get('pf'))} | {_fmt(removed.get('pf'))} | "
                f"{int(before.get('trades') or 0)} | {int(after.get('trades') or 0)} | {_pct(split.get('removed_coverage'))} | {_fmt(after.get('total_R'))} |"
            )
        lines.append("")
    if rejected:
        lines.extend(
            [
                "## Rejected High-Risk Reference",
                "",
                "These candidates had good-looking test PF but were not used in the gate-on experiment because the prior holdout search marked them as `reject`.",
                "",
                "| strategy | rule | test PF | after PF | risk | recommendation |",
                "| --- | --- | ---: | ---: | --- | --- |",
            ]
        )
        for row in rejected:
            split = row.get("split_metrics") or {}
            agg = row.get("aggregate_metrics") or {}
            lines.append(
                f"| `{row['strategy_line']}` | `{_rule_text(row['rule'])}` | "
                f"{_fmt((split.get('test') or {}).get('after', {}).get('pf'))} | {_fmt((agg.get('after') or {}).get('pf'))} | "
                f"`{row['overfit_risk']}` | `{row['recommendation']}` |"
            )
    lines.extend(["", "## Judgment", ""])
    if pf_gt_one:
        lines.append("- At least one shadow gate reached test PF > 1. It still requires paper-shadow validation before any config promotion.")
    else:
        lines.append("- No evaluated shadow gate reached test PF > 1. The gates improved PF, but not enough for promotion.")
    lines.append("- Best current use is watch/downweight evidence and further gate-feature research, not immediate production gating.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def run(project_root: Path, *, top_per_strategy: int) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    candidates = _load_candidates(conn, top_per_strategy=top_per_strategy)
    rejected = _load_rejected_reference(conn)
    results: list[dict[str, Any]] = []
    for candidate in candidates:
        samples = _load_samples(conn, candidate)
        if not samples:
            continue
        result = _evaluate_candidate(samples, candidate["rule"])
        results.append(
            {
                "strategy_line": candidate["strategy_line"],
                "parameter_set_id": candidate["parameter_set_id"],
                "package_key": candidate["package_key"],
                "rule": candidate["rule"],
                "recommendation": candidate["recommendation"],
                "overfit_risk": candidate["overfit_risk"],
                "validation_id": candidate["validation_id"],
                "result": result,
            }
        )
    report_path = _report(project_root, db_path, results, rejected)
    best_by_strategy: dict[str, dict[str, Any]] = {}
    for row in results:
        strategy = row["strategy_line"]
        score = _safe_float((row["result"].get("split_metrics") or {}).get("test", {}).get("after", {}).get("pf")) or -1
        current = best_by_strategy.get(strategy)
        current_score = (
            _safe_float((current["result"].get("split_metrics") or {}).get("test", {}).get("after", {}).get("pf"))
            if current
            else None
        )
        if current is None or score > (current_score if current_score is not None else -1):
            best_by_strategy[strategy] = row
    return {
        "status": "ok",
        "report": str(report_path),
        "evaluated_candidates": len(results),
        "best_by_strategy": {
            strategy: {
                "parameter_set_id": row["parameter_set_id"],
                "rule": _rule_text(row["rule"]),
                "before_pf": (row["result"].get("before") or {}).get("pf"),
                "after_pf": (row["result"].get("after") or {}).get("pf"),
                "test_pf": (row["result"].get("split_metrics") or {}).get("test", {}).get("after", {}).get("pf"),
                "removed_coverage": row["result"].get("removed_coverage"),
            }
            for strategy, row in sorted(best_by_strategy.items())
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--top-per-strategy", type=int, default=10)
    args = parser.parse_args()
    payload = run(Path(args.project_root).resolve(), top_per_strategy=max(1, int(args.top_per_strategy)))
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
