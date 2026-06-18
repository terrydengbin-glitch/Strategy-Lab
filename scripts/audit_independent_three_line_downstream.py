from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import urllib.error
import urllib.request
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.notifications.config import FeishuConfig, STRATEGY_LINES
from laoma_signal_engine.notifications.delivery import read_delivery_history
from laoma_signal_engine.notifications.selector import mock_trade_plan_docs, select_trade_plan_signals


LINE_CONTRACT = {
    "without_micro": ("trade_plan_without_micro", "none", "DATA/decisions/latest_trade_plan_without_micro.json"),
    "micro_fast": ("trade_plan_micro_fast", "fast", "DATA/decisions/latest_trade_plan_micro_fast.json"),
    "micro_full": ("trade_plan_micro_full", "full", "DATA/decisions/latest_trade_plan_micro_full.json"),
}


def _now() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = read_json_object(path)
    except (OSError, ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _check(name: str, ok: bool, detail: Any = None, severity: str = "HIGH") -> dict[str, Any]:
    return {"name": name, "ok": bool(ok), "severity": severity, "detail": detail}


def _paper_sqlite_counts(root: Path, run_id: str | None) -> dict[str, Any]:
    db = root / "DATA" / "paper" / "paper_trading.db"
    if not db.is_file():
        return {"exists": False, "path": str(db), "tables": {}}
    tables: dict[str, Any] = {}
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        for table in ("paper_trade_plans", "paper_orders", "paper_positions", "paper_fills"):
            try:
                total = conn.execute(f"select count(*) as n from {table}").fetchone()["n"]
                by_line = {
                    row["strategy_line"]: row["n"]
                    for row in conn.execute(
                        f"select strategy_line, count(*) as n from {table} group by strategy_line",
                    ).fetchall()
                }
                current = 0
                if run_id and table in {"paper_trade_plans", "paper_orders"}:
                    current = conn.execute(
                        f"select count(*) as n from {table} where source_run_id = ?",
                        (run_id,),
                    ).fetchone()["n"]
                tables[table] = {"total": total, "by_line": by_line, "current_run": current}
            except sqlite3.Error as exc:
                tables[table] = {"error": str(exc)}
    return {"exists": True, "path": str(db), "tables": tables}


def _fastapi_status() -> dict[str, Any]:
    url = "http://127.0.0.1:8000/api/pipeline/status/latest"
    try:
        with urllib.request.urlopen(url, timeout=2.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (OSError, urllib.error.URLError, TimeoutError, ValueError) as exc:
        return {"reachable": False, "url": url, "error": str(exc)}
    return {"reachable": True, "url": url, "payload": data}


def build_audit(root: Path) -> dict[str, Any]:
    docs: dict[str, dict[str, Any] | None] = {}
    checks: list[dict[str, Any]] = []
    per_line: dict[str, Any] = {}
    for line, (source, mode, rel) in LINE_CONTRACT.items():
        path = root / rel
        doc = _load_json(path)
        docs[line] = doc
        checks.append(_check(f"{line}.output_exists", doc is not None, str(path)))
        if not doc:
            continue
        plans = doc.get("plans") if isinstance(doc.get("plans"), list) else []
        executable_count = sum(1 for plan in plans if isinstance(plan, dict) and bool(plan.get("executable")))
        input_refs = doc.get("input_refs") if isinstance(doc.get("input_refs"), dict) else {}
        per_line[line] = {
            "path": str(path),
            "run_id": doc.get("run_id"),
            "cycle_id": doc.get("cycle_id"),
            "generated_at": doc.get("generated_at"),
            "source": doc.get("source"),
            "micro_mode": doc.get("micro_mode"),
            "count": doc.get("count"),
            "executable_count": doc.get("executable_count"),
            "computed_executable_count": executable_count,
            "liquidity_generated_at": input_refs.get("liquidity_generated_at"),
            "micro_state_generated_at": input_refs.get("micro_state_generated_at"),
            "opportunity_distribution": dict(
                Counter(
                    str((plan.get("guards") or {}).get("opportunity_type", "missing"))
                    for plan in plans
                    if isinstance(plan, dict)
                ),
            ),
        }
        checks.extend(
            [
                _check(f"{line}.source_mode", doc.get("source") == source and doc.get("micro_mode") == mode, {
                    "source": doc.get("source"),
                    "micro_mode": doc.get("micro_mode"),
                }),
                _check(f"{line}.executable_count_matches", doc.get("executable_count") == executable_count, {
                    "doc": doc.get("executable_count"),
                    "computed": executable_count,
                }),
                _check(f"{line}.liquidity_ref_present", bool(input_refs.get("liquidity_generated_at")), input_refs),
            ],
        )
        if line == "without_micro":
            checks.append(
                _check(
                    "without_micro.no_micro_refs",
                    not any(k.startswith("micro") for k in input_refs),
                    input_refs,
                ),
            )
        else:
            checks.append(_check(f"{line}.micro_state_ref_present", bool(input_refs.get("micro_state_generated_at")), input_refs))

    run_ids = {line: row.get("run_id") for line, row in per_line.items()}
    cycle_ids = {line: row.get("cycle_id") for line, row in per_line.items()}
    current_run_id = next((str(v) for v in run_ids.values() if v), None)
    current_cycle_id = next((str(v) for v in cycle_ids.values() if v), None)
    unique_run_ids = {str(v) for v in run_ids.values() if v}
    unique_cycle_ids = {str(v) for v in cycle_ids.values() if v}
    lineage_status = (
        "aligned"
        if len(unique_run_ids) == 1 and len(unique_cycle_ids) == 1 and len(run_ids) == len(LINE_CONTRACT)
        else "mixed_or_missing"
    )
    paper_summary = _load_json(root / "DATA" / "paper" / "latest_paper_state.json") or {}
    paper_sqlite = _paper_sqlite_counts(root, current_run_id)
    selected = select_trade_plan_signals(
        {line: doc for line, doc in docs.items() if doc},
        config=FeishuConfig(),
        paper_summary=paper_summary,
    )
    mock_selected = select_trade_plan_signals(
        mock_trade_plan_docs(),
        config=FeishuConfig(),
        paper_summary=paper_summary,
    )
    deliveries_payload = read_delivery_history(root)
    deliveries = deliveries_payload.get("deliveries") if isinstance(deliveries_payload.get("deliveries"), list) else []
    delivery_counts = dict(Counter(str(row.get("strategy_line")) for row in deliveries if isinstance(row, dict)))
    executable_total = sum(int((row or {}).get("executable_count") or 0) for row in per_line.values())
    checks.append(
        _check(
            "feishu.current_selected_matches_executable",
            sum(selected["selected_counts"].values()) == executable_total,
            {"selected": selected["selected_counts"], "executable_total": executable_total},
            "HIGH",
        ),
    )
    checks.append(
        _check(
            "feishu.executable_fixture_selects_three_lines",
            mock_selected["selected_counts"] == {"without_micro": 1, "micro_fast": 1, "micro_full": 1},
            mock_selected["selected_counts"],
            "HIGH",
        ),
    )
    checks.append(
        _check(
            "paper.sqlite_exists",
            bool(paper_sqlite.get("exists")),
            paper_sqlite.get("path"),
            "HIGH",
        ),
    )
    fastapi = _fastapi_status()
    checks.append(_check("fastapi.pipeline_status_reachable", bool(fastapi.get("reachable")), fastapi, "MEDIUM"))
    failure_count = sum(1 for item in checks if not item["ok"] and item["severity"] == "HIGH")
    warning_count = sum(1 for item in checks if not item["ok"] and item["severity"] != "HIGH")
    return {
        "schema_version": "7.8",
        "source": "independent_three_line_downstream_audit",
        "generated_at": _now(),
        "run_id": current_run_id if len(unique_run_ids) == 1 else None,
        "cycle_id": current_cycle_id if len(unique_cycle_ids) == 1 else None,
        "audit_subject": {
            "strategy_report_run_id": current_run_id if len(unique_run_ids) == 1 else None,
            "strategy_report_cycle_id": current_cycle_id if len(unique_cycle_ids) == 1 else None,
            "line_run_ids": run_ids,
            "line_cycle_ids": cycle_ids,
            "lineage_status": lineage_status,
        },
        "status": "ok" if failure_count == 0 else "failed",
        "failure_count": failure_count,
        "warning_count": warning_count,
        "current_run_id": current_run_id,
        "per_line": per_line,
        "paper": {
            "summary_exists": bool(paper_summary),
            "stats": (paper_summary.get("stats") if isinstance(paper_summary.get("stats"), dict) else {}),
            "sqlite": paper_sqlite,
        },
        "feishu": {
            "selected_counts": selected["selected_counts"],
            "skipped_count": len(selected["skipped"]),
            "mock_executable_selected_counts": mock_selected["selected_counts"],
            "delivery_counts_by_line": delivery_counts,
            "delivery_total": len(deliveries),
        },
        "fastapi": fastapi,
        "checks": checks,
    }


def write_markdown(report: dict[str, Any], path: Path) -> None:
    lines = [
        "# STEP7.8 Independent Three-Line Downstream Audit",
        "",
        f"- Status: `{report['status']}`",
        f"- Generated at: `{report['generated_at']}`",
        f"- Current run: `{report.get('current_run_id')}`",
        f"- Blocking failures: `{report['failure_count']}`",
        f"- Warnings: `{report['warning_count']}`",
        "",
        "## Per-Line",
        "",
    ]
    for line, row in report.get("per_line", {}).items():
        lines.append(
            f"- `{line}` count={row.get('count')} executable={row.get('executable_count')} "
            f"liquidity_ref={row.get('liquidity_generated_at')} micro_ref={row.get('micro_state_generated_at')}",
        )
    lines.extend(["", "## Paper", ""])
    sqlite_info = report.get("paper", {}).get("sqlite", {})
    lines.append(f"- SQLite exists: `{sqlite_info.get('exists')}`")
    for table, row in (sqlite_info.get("tables") or {}).items():
        lines.append(f"- `{table}` total={row.get('total')} current_run={row.get('current_run')} by_line={row.get('by_line')}")
    lines.extend(["", "## Feishu", ""])
    feishu = report.get("feishu", {})
    lines.append(f"- Current selected: `{feishu.get('selected_counts')}`")
    lines.append(f"- Executable fixture selected: `{feishu.get('mock_executable_selected_counts')}`")
    lines.append(f"- Delivery counts by line: `{feishu.get('delivery_counts_by_line')}`")
    lines.extend(["", "## Failed Checks", ""])
    failed = [item for item in report.get("checks", []) if not item.get("ok")]
    if not failed:
        lines.append("- None")
    else:
        for item in failed:
            lines.append(f"- `{item.get('severity')}` `{item.get('name')}`: `{item.get('detail')}`")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output", default="DATA/reports/latest_independent_three_line_downstream_audit.json")
    parser.add_argument("--markdown", default=None)
    parser.add_argument("--stdout-json", action="store_true")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    report = build_audit(root)
    out = (root / args.output).resolve()
    write_json_atomic(out, report)
    md = Path(args.markdown).resolve() if args.markdown else root / "docs" / "reports" / f"independent_three_line_downstream_audit_{report.get('current_run_id') or 'latest'}.md"
    write_markdown(report, md)
    if args.stdout_json:
        print(json.dumps({"status": report["status"], "failure_count": report["failure_count"], "output": str(out), "markdown": str(md)}, ensure_ascii=False))
    return 0 if report["status"] == "ok" else 40


if __name__ == "__main__":
    raise SystemExit(main())
