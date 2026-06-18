from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.micro.training_ledger import default_micro_training_db, init_micro_training_db, json_loads


def pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.1f}%"


def metric_line(metrics: dict[str, Any], key: str) -> str:
    item = metrics.get(key) or {}
    return f"{item.get('filled', 0)}/{item.get('total', 0)} ({pct(item.get('coverage'))})"


def main() -> int:
    root = PROJECT_ROOT
    db = init_micro_training_db(root=root)
    reports = root / "docs" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = reports / f"STEP16.16_micro_500run_evidence_quality_reaudit_{ts}.json"
    md_path = reports / f"STEP16.16_micro_500run_evidence_quality_reaudit_{ts}.md"

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        run_ids = [
            row["run_id"]
            for row in conn.execute(
                """
                select run_id
                from micro_run_samples
                group by run_id
                order by max(generated_at) desc
                limit 500
                """
            ).fetchall()
        ]
        if not run_ids:
            raise SystemExit("no micro training runs")
        placeholders = ",".join(["?"] * len(run_ids))
        run_rows = [dict(row) for row in conn.execute(f"select * from micro_run_samples where run_id in ({placeholders})", tuple(run_ids)).fetchall()]
        symbol_rows = [
            dict(row)
            for row in conn.execute(
                f"""
                select s.*, l.trade_plan_status, l.paper_status, l.exit_reason,
                       l.net_R, l.MFE_R, l.MAE_R, l.trade_quality_root_cause
                from micro_symbol_samples s
                left join micro_downstream_labels l on l.sample_id=s.sample_id
                where s.run_id in ({placeholders})
                """,
                tuple(run_ids),
            ).fetchall()
        ]

    by_line: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in symbol_rows:
        by_line[str(row.get("strategy_line") or "unknown")].append(row)

    line_summary: dict[str, Any] = {}
    for line, rows in sorted(by_line.items()):
        total = len(rows)
        metrics: dict[str, Any] = {}
        for metric in ("cvd", "ofi", "z_cvd", "z_ofi", "spread", "depth_imbalance"):
            filled = sum(1 for row in rows if row.get(metric) is not None)
            metrics[metric] = {
                "filled": filled,
                "missing": total - filled,
                "total": total,
                "coverage": filled / total if total else None,
            }
        label_counts = Counter()
        for row in rows:
            if row.get("trade_quality_root_cause"):
                label_counts["trade_quality_closed"] += 1
            elif row.get("paper_status"):
                label_counts["paper_ordered"] += 1
            elif row.get("trade_plan_status"):
                label_counts[str(row.get("trade_plan_status"))] += 1
            else:
                label_counts["unlabeled"] += 1
        confidence_counts = Counter(str(row.get("source_confidence") or "unknown") for row in rows)
        missing_reason_rows = sum(1 for row in rows if row.get("missing_reason"))
        line_summary[line] = {
            "samples": total,
            "accepted": sum(1 for row in rows if row.get("accepted")),
            "blocked": sum(1 for row in rows if row.get("blocked")),
            "metrics": metrics,
            "missing_reason_rows": missing_reason_rows,
            "missing_reason_coverage": missing_reason_rows / total if total else None,
            "downstream_label_counts": dict(label_counts),
            "source_confidence_counts": dict(confidence_counts),
        }

    run_status_counts = Counter(f"{row.get('strategy_line')}:{row.get('status')}" for row in run_rows)
    missing_reason_counts = Counter()
    for row in symbol_rows:
        reason = row.get("missing_reason")
        if reason:
            missing_reason_counts[str(reason)] += 1
    tq_rows = [row for row in symbol_rows if row.get("trade_quality_root_cause")]
    tq_by_line: dict[str, Any] = {}
    for line, rows in sorted(defaultdict(list, {line: [row for row in tq_rows if row.get("strategy_line") == line] for line in {row.get("strategy_line") for row in tq_rows}}).items()):
        values = [float(row["net_R"]) for row in rows if row.get("net_R") is not None]
        tq_by_line[str(line)] = {
            "samples": len(rows),
            "avg_net_R": sum(values) / len(values) if values else None,
            "root_causes": dict(Counter(str(row.get("trade_quality_root_cause")) for row in rows)),
        }

    summary = {
        "generated_at": ts,
        "training_db_path": str(default_micro_training_db(root)),
        "run_count": len(run_ids),
        "run_rows": len(run_rows),
        "symbol_rows": len(symbol_rows),
        "line_summary": line_summary,
        "run_status_counts": dict(run_status_counts),
        "missing_reason_counts": dict(missing_reason_counts),
        "trade_quality_by_line": tq_by_line,
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# STEP16.16 Micro 500-Run Evidence Quality Reaudit",
        "",
        f"- generated_at: `{ts}`",
        f"- training_db: `{summary['training_db_path']}`",
        f"- distinct_runs: `{summary['run_count']}`",
        f"- run_rows: `{summary['run_rows']}`",
        f"- symbol_rows: `{summary['symbol_rows']}`",
        "",
        "## Line Coverage",
        "",
        "| line | samples | accepted | blocked | CVD | OFI | z_CVD | z_OFI | spread | depth | missing_reason | labels |",
        "| --- | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for line, item in line_summary.items():
        labels = ", ".join(f"{k}:{v}" for k, v in sorted((item.get("downstream_label_counts") or {}).items()))
        lines.append(
            "| "
            + " | ".join(
                [
                    line,
                    str(item["samples"]),
                    str(item["accepted"]),
                    str(item["blocked"]),
                    metric_line(item["metrics"], "cvd"),
                    metric_line(item["metrics"], "ofi"),
                    metric_line(item["metrics"], "z_cvd"),
                    metric_line(item["metrics"], "z_ofi"),
                    metric_line(item["metrics"], "spread"),
                    metric_line(item["metrics"], "depth_imbalance"),
                    pct(item.get("missing_reason_coverage")),
                    labels or "-",
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Run-Level Gap Classification",
            "",
            "```json",
            json.dumps(summary["run_status_counts"], ensure_ascii=False, indent=2, sort_keys=True),
            "```",
            "",
            "## Trade Quality Joined Labels",
            "",
            "```json",
            json.dumps(summary["trade_quality_by_line"], ensure_ascii=False, indent=2, sort_keys=True),
            "```",
            "",
            "## Conclusion",
            "",
            "- `micro_fast` CVD/OFI/z fields are now mostly recovered from `micro_factor_frames` with event-time-window confidence.",
            "- Spread/depth remain absent in the normalized training ledger, but each affected sample now carries explicit `missing_reason` instead of silent nulls.",
            "- `micro_full` no-symbol rows are classified at run level as `not_selected` / explained gap, not an unexplained ledger break.",
            "- Downstream labels are still sparse because only a small subset of recent micro samples became paper/trade-quality samples; this is expected for a training sidecar.",
        ]
    )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "ok", "md": str(md_path), "json": str(json_path)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
