from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_ops import (
    enqueue_tq_materialization_job,
    process_next_tq_materialization_job,
    tq_materialization_jobs_payload,
)


DEFAULT_PARAM = "s6v32_edcd6b1030331422"


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _count(conn: sqlite3.Connection, table: str, clauses: list[str], params: list[Any]) -> int:
    try:
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}{where}", params).fetchone()[0] or 0)
    except sqlite3.Error:
        return 0


def _select_package(project_root: Path, strategy_line: str, parameter_set_id: str | None) -> dict[str, Any]:
    with _connect(p21_db_path(project_root)) as conn:
        clauses = ["strategy_line = ?"]
        params: list[Any] = [strategy_line]
        if parameter_set_id:
            clauses.append("parameter_set_id = ?")
            params.append(parameter_set_id)
        where = " AND ".join(clauses)
        row = conn.execute(
            f"""
            SELECT experiment_id, strategy_line, parameter_set_id, COUNT(*) AS trade_count
            FROM p21_v2_shadow_orders
            WHERE {where}
            GROUP BY experiment_id, strategy_line, parameter_set_id
            ORDER BY trade_count DESC, experiment_id DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
    if not row:
        raise SystemExit(f"no package found for strategy_line={strategy_line} parameter_set_id={parameter_set_id or '*'}")
    return dict(row)


def _counts(project_root: Path, package: dict[str, Any]) -> dict[str, int]:
    clauses = ["experiment_id = ?", "strategy_line = ?", "parameter_set_id = ?"]
    params: list[Any] = [package["experiment_id"], package["strategy_line"], package["parameter_set_id"]]
    with _connect(p21_db_path(project_root)) as conn:
        return {
            "shadow_orders": _count(conn, "p21_v2_shadow_orders", clauses, params),
            "research_trade_facts": _count(conn, "research_trade_facts", clauses, params),
            "research_entry_features": _count(conn, "research_entry_features", clauses, params),
            "backtest_trade_quality_samples": _count(conn, "backtest_trade_quality_samples", clauses, params),
            "trade_quality_entry_evidence_v4": _count(conn, "trade_quality_entry_evidence_v4", clauses, params),
            "trade_quality_causal_factors_v5": _count(conn, "trade_quality_causal_factors_v5", clauses, params),
            "trade_quality_gate_validations_v5": _count(conn, "trade_quality_gate_validations_v5", clauses, params),
        }


def _report(project_root: Path, package: dict[str, Any], enqueue: dict[str, Any], processed: dict[str, Any], jobs: dict[str, Any], counts: dict[str, int]) -> Path:
    reports_dir = project_root / "docs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / f"STEP7.131_async_tq_v4_v5_materialization_e2e_audit_{_now_stamp()}.md"
    status = "PASS" if (
        counts["shadow_orders"] > 0
        and counts["backtest_trade_quality_samples"] > 0
        and counts["trade_quality_entry_evidence_v4"] > 0
        and counts["trade_quality_causal_factors_v5"] > 0
        and processed.get("status") == "done"
    ) else "FAIL"
    lines = [
        "# STEP7.131 Async TQ V4/V5 Materialization E2E Audit",
        "",
        f"- status: {status}",
        f"- experiment_id: `{package['experiment_id']}`",
        f"- strategy_line: `{package['strategy_line']}`",
        f"- parameter_set_id: `{package['parameter_set_id']}`",
        f"- job_id: `{enqueue.get('job_id')}`",
        f"- process_status: `{processed.get('status')}`",
        "",
        "## Row Counts",
        "",
        "| table | rows |",
        "|---|---:|",
    ]
    lines.extend([f"| `{key}` | {value} |" for key, value in counts.items()])
    lines.extend(
        [
            "",
            "## Queue Evidence",
            "",
            f"- enqueue_status: `{enqueue.get('status')}`",
            f"- latest_job_count: {jobs.get('count')}",
            f"- error: `{processed.get('error') or ''}`",
            "",
            "## Boundary",
            "",
            "- No strategy logic/config/paper execution semantics were changed.",
            "- V5 gate candidates remain shadow-only.",
            "- FastAPI request threads should enqueue/process jobs instead of synchronous full materialization.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--strategy-line", default="strategy6")
    parser.add_argument("--parameter-set-id", default=DEFAULT_PARAM)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--top-n", type=int, default=1)
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    package = _select_package(project_root, args.strategy_line, args.parameter_set_id)
    enqueue = enqueue_tq_materialization_job(
        project_root,
        {
            "source_type": "backtest",
            "experiment_id": package["experiment_id"],
            "strategy_line": package["strategy_line"],
            "parameter_set_id": package["parameter_set_id"],
            "top_n": args.top_n,
            "limit": args.limit,
            "dry_run": False,
            "force": True,
            "include_v5": True,
            "include_gates": True,
            "min_samples": 50,
            "gate_limit": 120,
        },
    )
    processed = process_next_tq_materialization_job(project_root)
    jobs = tq_materialization_jobs_payload(project_root, limit=5)
    counts = _counts(project_root, package)
    report = _report(project_root, package, enqueue, processed, jobs, counts)
    print(
        {
            "status": processed.get("status"),
            "report": str(report),
            "job_id": enqueue.get("job_id"),
            "counts": counts,
        }
    )


if __name__ == "__main__":
    main()
