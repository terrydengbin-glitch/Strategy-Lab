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

from laoma_signal_engine.micro.training_ledger import default_micro_training_db, init_micro_training_db


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
    json_path = reports / f"STEP16.17_micro_spread_depth_evidence_reaudit_{ts}.json"
    md_path = reports / f"STEP16.17_micro_spread_depth_evidence_reaudit_{ts}.md"

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
        run_rows = [
            dict(row)
            for row in conn.execute(
                f"select * from micro_run_samples where run_id in ({placeholders})",
                tuple(run_ids),
            ).fetchall()
        ]
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
        for metric in (
            "cvd",
            "ofi",
            "z_cvd",
            "z_ofi",
            "spread",
            "spread_bps",
            "depth_imbalance",
            "bid_depth_usdt",
            "ask_depth_usdt",
        ):
            filled = sum(1 for row in rows if row.get(metric) is not None)
            metrics[metric] = {
                "filled": filled,
                "missing": total - filled,
                "total": total,
                "coverage": filled / total if total else None,
            }
        depth_source_counts = Counter(str(row.get("depth_source") or "missing") for row in rows)
        depth_missing_reason_counts = Counter(str(row.get("depth_missing_reason") or "none") for row in rows)
        book_cost_confidence_counts = Counter(str(row.get("book_cost_confidence") or "none") for row in rows)
        line_summary[line] = {
            "samples": total,
            "accepted": sum(1 for row in rows if row.get("accepted")),
            "blocked": sum(1 for row in rows if row.get("blocked")),
            "metrics": metrics,
            "depth_source_counts": dict(depth_source_counts),
            "depth_missing_reason_counts": dict(depth_missing_reason_counts),
            "book_cost_confidence_counts": dict(book_cost_confidence_counts),
            "missing_reason_rows": sum(1 for row in rows if row.get("missing_reason")),
        }

    summary = {
        "generated_at": ts,
        "training_db_path": str(default_micro_training_db(root)),
        "run_count": len(run_ids),
        "run_rows": len(run_rows),
        "symbol_rows": len(symbol_rows),
        "line_summary": line_summary,
        "run_status_counts": dict(Counter(f"{row.get('strategy_line')}:{row.get('status')}" for row in run_rows)),
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# STEP16.17 Micro Spread / Depth Evidence Reaudit",
        "",
        f"- generated_at: `{ts}`",
        f"- training_db: `{summary['training_db_path']}`",
        f"- distinct_runs: `{summary['run_count']}`",
        f"- run_rows: `{summary['run_rows']}`",
        f"- symbol_rows: `{summary['symbol_rows']}`",
        "",
        "## Line Coverage",
        "",
        "| line | samples | accepted | blocked | CVD | OFI | spread | spread_bps | depth | bid_depth | ask_depth | missing rows |",
        "| --- | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | ---: |",
    ]
    for line, item in line_summary.items():
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
                    metric_line(item["metrics"], "spread"),
                    metric_line(item["metrics"], "spread_bps"),
                    metric_line(item["metrics"], "depth_imbalance"),
                    metric_line(item["metrics"], "bid_depth_usdt"),
                    metric_line(item["metrics"], "ask_depth_usdt"),
                    str(item["missing_reason_rows"]),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Depth Source / Missing Reason",
            "",
            "```json",
            json.dumps(
                {
                    line: {
                        "depth_source_counts": item["depth_source_counts"],
                        "depth_missing_reason_counts": item["depth_missing_reason_counts"],
                        "book_cost_confidence_counts": item["book_cost_confidence_counts"],
                    }
                    for line, item in line_summary.items()
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## Conclusion",
            "",
            "- STEP16.17 keeps strategy behavior untouched and only enriches the micro training sidecar.",
            "- If historical bookTicker/depth payloads are absent, spread/depth remain null by design; the ledger now records `depth_missing_reason` instead of silent nulls.",
            "- Future runtime/factor-frame payloads carrying bid/ask/depth fields will fill `spread_bps`, `bid_depth_usdt`, `ask_depth_usdt`, and `depth_imbalance` automatically.",
        ]
    )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "ok", "md": str(md_path), "json": str(json_path)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
