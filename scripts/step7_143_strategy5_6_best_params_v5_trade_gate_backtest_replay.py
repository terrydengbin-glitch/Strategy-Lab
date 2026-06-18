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
from scripts.step7_133_strategy5_6_v5_trade_gate_backtest_experiment import (
    _evaluate_candidate,
    _fmt,
    _load_samples,
    _loads,
    _pct,
    _rule_text,
    _safe_float,
)


TASK_ID = "STEP7.143"
OUTPUT_JSON = "step7_143_strategy5_6_best_params_v5_trade_gate_backtest_replay.json"
TARGET_VALIDATION_IDS = (
    "tqv5combo_99ef989cfd6a75fd26c46a",
    "tqv5combo_b62820ba88465531d7e991",
)


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_fixed_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in TARGET_VALIDATION_IDS)
    rows = conn.execute(
        f"""
        SELECT *
        FROM trade_quality_combo_gate_validations_v5
        WHERE validation_id IN ({placeholders})
        ORDER BY strategy_line, validation_id
        """,
        TARGET_VALIDATION_IDS,
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["rule"] = _loads(item.get("rule_json"), {})
        item["split_metrics"] = _loads(item.get("split_metrics_json"), {})
        item["aggregate_metrics"] = _loads(item.get("aggregate_metrics_json"), {})
        out.append(item)
    return out


def _metric_view(metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "trades": int(metrics.get("trades") or 0),
        "pf": metrics.get("pf"),
        "win_rate": metrics.get("win_rate"),
        "expectancy_R": metrics.get("expectancy_R"),
        "total_R": metrics.get("total_R"),
        "max_drawdown_R": metrics.get("max_drawdown_R"),
    }


def _compact_result(row: dict[str, Any]) -> dict[str, Any]:
    result = row["result"]
    split_metrics: dict[str, Any] = {}
    for split_name, split in (result.get("split_metrics") or {}).items():
        split_metrics[split_name] = {
            "before": _metric_view(split.get("before") or {}),
            "after": _metric_view(split.get("after") or {}),
            "removed": _metric_view(split.get("removed") or {}),
            "removed_coverage": split.get("removed_coverage"),
        }
    return {
        "strategy_line": row["strategy_line"],
        "parameter_set_id": row["parameter_set_id"],
        "validation_id": row["validation_id"],
        "package_key": row["package_key"],
        "recommendation": row["recommendation"],
        "overfit_risk": row["overfit_risk"],
        "rule": row["rule"],
        "rule_text": _rule_text(row["rule"]),
        "before": _metric_view(result.get("before") or {}),
        "after": _metric_view(result.get("after") or {}),
        "removed": _metric_view(result.get("removed") or {}),
        "samples": result.get("samples"),
        "kept": result.get("kept"),
        "removed_rows": result.get("removed_rows"),
        "removed_coverage": result.get("removed_coverage"),
        "split_metrics": split_metrics,
    }


def _write_report(project_root: Path, db_path: Path, rows: list[dict[str, Any]], output_json: Path) -> Path:
    report_dir = project_root / "docs" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"STEP7.143_strategy5_6_best_params_v5_trade_gate_backtest_replay_{_stamp()}.md"
    pf_gt_one = [
        row
        for row in rows
        if (_safe_float((row["result"].get("after") or {}).get("pf")) or 0.0) >= 1.0
        or (
            _safe_float((row["result"].get("split_metrics") or {}).get("test", {}).get("after", {}).get("pf"))
            or 0.0
        )
        >= 1.0
    ]
    lines = [
        "# STEP7.143 Strategy5/6 Best Params V5 Trade Gate Backtest Replay",
        "",
        f"- generated_at: `{datetime.now(timezone.utc).isoformat()}`",
        f"- research_db: `{db_path}`",
        f"- output_json: `{output_json}`",
        f"- fixed_candidates: `{len(rows)}`",
        f"- pf_gt_1_candidates: `{len(pf_gt_one)}`",
        "",
        "## Contract",
        "",
        "- Fixed candidate replay only; no strategy/config/paper mutation.",
        "- Gate predicates use entry-known V5/P24 fields only.",
        "- Gate action is modeled as `shadow_block_or_downweight` by removing matched trades from the result set.",
        "- This is the pre-paper-shadow replay for STEP7.134 / STEP7.135, not production promotion evidence.",
        "",
        "## Results",
        "",
        "| strategy | parameter_set | validation | rule | before PF | after PF | test PF | trades before | trades after | removed | removed % | WR after | expectancy after | max DD after | risk | recommendation |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in sorted(rows, key=lambda item: item["strategy_line"]):
        result = row["result"]
        after = result.get("after") or {}
        test_after = (result.get("split_metrics") or {}).get("test", {}).get("after", {})
        lines.append(
            "| `{strategy}` | `{param}` | `{validation}` | `{rule}` | {before_pf} | {after_pf} | {test_pf} | {before_n} | {after_n} | {removed_n} | {removed_pct} | {wr} | {exp} | {dd} | `{risk}` | `{rec}` |".format(
                strategy=row["strategy_line"],
                param=row["parameter_set_id"],
                validation=row["validation_id"],
                rule=_rule_text(row["rule"]),
                before_pf=_fmt((result.get("before") or {}).get("pf")),
                after_pf=_fmt(after.get("pf")),
                test_pf=_fmt(test_after.get("pf")),
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
    for row in sorted(rows, key=lambda item: item["strategy_line"]):
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
    lines.extend(["## Judgment", ""])
    if pf_gt_one:
        lines.append("- At least one fixed replay candidate reached PF >= 1. It still requires paper-shadow validation before promotion.")
    else:
        lines.append("- No fixed replay candidate reached PF >= 1. These gates remain paper-shadow candidates, not production promotion evidence.")
    lines.append("- Continue with STEP7.134 only as isolated paper shadow if live feature completeness is acceptable.")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def run(project_root: Path) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    candidates = _load_fixed_candidates(conn)
    found_ids = {row["validation_id"] for row in candidates}
    missing_ids = [validation_id for validation_id in TARGET_VALIDATION_IDS if validation_id not in found_ids]
    results: list[dict[str, Any]] = []
    for candidate in candidates:
        samples = _load_samples(conn, candidate)
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
    output_dir = project_root / "DATA" / "backtest"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_json = output_dir / OUTPUT_JSON
    payload = {
        "task_id": TASK_ID,
        "status": "ok" if not missing_ids else "missing_candidates",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "research_db": str(db_path),
        "target_validation_ids": list(TARGET_VALIDATION_IDS),
        "missing_validation_ids": missing_ids,
        "results": [_compact_result(row) for row in results],
    }
    output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report_path = _write_report(project_root, db_path, results, output_json)
    payload["report"] = str(report_path)
    output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    args = parser.parse_args()
    payload = run(Path(args.project_root).resolve())
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
