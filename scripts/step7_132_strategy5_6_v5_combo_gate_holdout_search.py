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
    TARGET_ONLY_FIELDS,
    generate_combo_gate_candidates_v5_payload,
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


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def _rule_text(rule: dict[str, Any]) -> str:
    return " AND ".join(f"{item.get('field')}={item.get('value')}" for item in rule.get("rules", []))


def _combo_rows(project_root: Path, limit: int = 80) -> list[dict[str, Any]]:
    conn = sqlite3.connect(p21_db_path(project_root))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT *
        FROM trade_quality_combo_gate_validations_v5
        WHERE schema_version = ?
        ORDER BY
          CASE recommendation WHEN 'paper_shadow_ready' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
          CASE overfit_risk WHEN 'low' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
          json_extract(split_metrics_json, '$.test.after.pf') DESC,
          json_extract(aggregate_metrics_json, '$.pf_improvement') DESC
        LIMIT ?
        """,
        (COMBO_GATE_SCHEMA_VERSION, limit),
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["rule"] = _loads(item.get("rule_json"), {})
        item["split_metrics"] = _loads(item.get("split_metrics_json"), {})
        item["aggregate_metrics"] = _loads(item.get("aggregate_metrics_json"), {})
        item["factor_explanation"] = _loads(item.get("factor_explanation_json"), {})
        out.append(item)
    return out


def _write_report(project_root: Path, payload: dict[str, Any], rows: list[dict[str, Any]]) -> Path:
    reports = project_root / "docs" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    path = reports / f"STEP7.132_strategy5_6_v5_combo_gate_holdout_search_{_stamp()}.md"
    by_strategy: dict[str, int] = {}
    pf_gt_one = []
    for row in rows:
        by_strategy[row["strategy_line"]] = by_strategy.get(row["strategy_line"], 0) + 1
        test_pf = row.get("split_metrics", {}).get("test", {}).get("after", {}).get("pf")
        if test_pf is not None and test_pf >= 1:
            pf_gt_one.append(row)
    lines = [
        "# STEP7.132 Strategy5/6 V5 Combo Gate Holdout Search",
        "",
        f"- generated_at: `{datetime.now(timezone.utc).isoformat()}`",
        f"- schema: `{COMBO_GATE_SCHEMA_VERSION}`",
        f"- status: `{payload.get('status')}`",
        f"- candidate_count: `{payload.get('candidate_count')}`",
        f"- test_pf_gt_1: `{len(pf_gt_one)}`",
        "",
        "## Contract",
        "",
        "- Baseline analysis module only; no strategy/config/paper mutation.",
        "- Gate predicates use entry-known fields only.",
        "- V5 causal factors are explanation-only and not used as predicates.",
        f"- allowed_fields: `{len(ENTRY_KNOWN_RULE_FIELDS)}`",
        f"- target_fields_excluded: `{len(TARGET_ONLY_FIELDS)}`",
        "",
        "## Candidate Coverage",
        "",
        "| strategy | candidates |",
        "| --- | ---: |",
    ]
    for strategy, count in sorted(by_strategy.items()):
        lines.append(f"| `{strategy}` | {count} |")
    lines.extend(
        [
            "",
            "## Top Combo Gate Candidates",
            "",
            "| strategy | combo | rule | before PF | after PF | test PF | removed | after trades | risk | recommendation |",
            "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in rows[:30]:
        agg = row.get("aggregate_metrics", {})
        split = row.get("split_metrics", {})
        lines.append(
            "| `{strategy}` | {combo} | `{rule}` | {before} | {after} | {test} | {removed} | {trades} | `{risk}` | `{rec}` |".format(
                strategy=row["strategy_line"],
                combo=row["combo_size"],
                rule=_rule_text(row.get("rule", {})),
                before=_fmt((agg.get("before") or {}).get("pf")),
                after=_fmt((agg.get("after") or {}).get("pf")),
                test=_fmt(((split.get("test") or {}).get("after") or {}).get("pf")),
                removed=_fmt(agg.get("removed_coverage")),
                trades=_fmt((agg.get("after") or {}).get("trades")),
                risk=row["overfit_risk"],
                rec=row["recommendation"],
            )
        )
    lines.extend(["", "## Top Candidates By Strategy", ""])
    for strategy in sorted(by_strategy):
        lines.extend(
            [
                f"### {strategy}",
                "",
                "| combo | rule | before PF | after PF | test PF | removed | risk | recommendation |",
                "| ---: | --- | ---: | ---: | ---: | ---: | --- | --- |",
            ]
        )
        strategy_rows = [row for row in rows if row["strategy_line"] == strategy]
        strategy_rows.sort(
            key=lambda row: (
                (((row.get("split_metrics") or {}).get("test") or {}).get("after") or {}).get("pf") or 0,
                ((row.get("aggregate_metrics") or {}).get("after") or {}).get("pf") or 0,
            ),
            reverse=True,
        )
        for row in strategy_rows[:10]:
            agg = row.get("aggregate_metrics", {})
            split = row.get("split_metrics", {})
            lines.append(
                "| {combo} | `{rule}` | {before} | {after} | {test} | {removed} | `{risk}` | `{rec}` |".format(
                    combo=row["combo_size"],
                    rule=_rule_text(row.get("rule", {})),
                    before=_fmt((agg.get("before") or {}).get("pf")),
                    after=_fmt((agg.get("after") or {}).get("pf")),
                    test=_fmt(((split.get("test") or {}).get("after") or {}).get("pf")),
                    removed=_fmt(agg.get("removed_coverage")),
                    risk=row["overfit_risk"],
                    rec=row["recommendation"],
                )
            )
        lines.append("")
    lines.extend(
        [
            "",
            "## Judgment",
            "",
        ]
    )
    if pf_gt_one:
        lines.append("- At least one combo candidate reached test PF > 1. It remains shadow-only and needs paper-shadow validation.")
    else:
        lines.append("- No combo candidate reached test PF > 1 in this run. Best robust candidates are still useful for downweight/watch lists.")
    lines.append("- Reject high-risk candidates with large removed coverage or unstable test behavior.")
    lines.append("- Next step, if useful: expose combo candidates in Trade Gate UI and run paper-shadow validation for low-risk `watch` candidates.")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--strategy-line", default="all")
    parser.add_argument("--min-samples", type=int, default=50)
    parser.add_argument("--limit", type=int, default=120)
    parser.add_argument("--max-combo-size", type=int, default=3)
    parser.add_argument("--max-seeds-per-strategy", type=int, default=8)
    args = parser.parse_args()
    project_root = Path(args.project_root).resolve()
    payload = generate_combo_gate_candidates_v5_payload(
        project_root,
        strategy_line=None if args.strategy_line == "all" else args.strategy_line,
        min_samples=args.min_samples,
        limit=args.limit,
        max_combo_size=args.max_combo_size,
        max_seeds_per_strategy=args.max_seeds_per_strategy,
    )
    rows = _combo_rows(project_root, limit=args.limit)
    report = _write_report(project_root, payload, rows)
    print({"status": payload.get("status"), "candidate_count": payload.get("candidate_count"), "report": str(report)})


if __name__ == "__main__":
    main()
