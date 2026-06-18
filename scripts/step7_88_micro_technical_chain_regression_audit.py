from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.micro.training_ledger import (
    columns,
    coverage_payload,
    default_micro_training_db,
    init_micro_training_db,
    json_loads,
    table_exists,
)


def _dict_counts(rows: list[sqlite3.Row], key: str) -> dict[str, int]:
    return dict(Counter(str(row[key] if row[key] is not None else "unknown") for row in rows))


def _reason_counts(rows: list[sqlite3.Row], key: str = "reason_codes_json") -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        for reason in json_loads(row[key], []):
            counts[str(reason)] += 1
    return dict(counts.most_common(30))


def _pct(num: int | float, den: int | float) -> float | None:
    return None if not den else round(float(num) / float(den), 6)


def _load_rows(db: Path, limit_runs: int) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
    with sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        if not table_exists(conn, "micro_symbol_samples"):
            return [], []
        run_ids = [
            row["run_id"]
            for row in conn.execute(
                """
                select run_id
                from micro_run_samples
                group by run_id
                order by max(generated_at) desc
                limit ?
                """,
                (max(1, int(limit_runs or 100)),),
            ).fetchall()
        ]
        if not run_ids:
            return [], []
        placeholders = ",".join(["?"] * len(run_ids))
        symbol_rows = conn.execute(
            f"select * from micro_symbol_samples where run_id in ({placeholders})",
            tuple(run_ids),
        ).fetchall()
        reliability_rows: list[sqlite3.Row] = []
        if table_exists(conn, "micro_technical_reliability"):
            reliability_rows = conn.execute(
                f"select * from micro_technical_reliability where run_id in ({placeholders})",
                tuple(run_ids),
            ).fetchall()
    return symbol_rows, reliability_rows


def build_report(root: Path, *, limit_runs: int = 100) -> dict[str, Any]:
    db = init_micro_training_db(root=root)
    coverage = coverage_payload(root, db_path=db)
    symbol_rows, reliability_rows = _load_rows(db, limit_runs)
    total = len(symbol_rows)
    ready = sum(1 for row in symbol_rows if str(row["ready_state"]).lower() in {"ready", "accepted", "ok"})
    technical_blocked = sum(1 for row in symbol_rows if str(row["technical_status"] or row["ready_state"]).lower() in {"technical_blocked", "stale", "timeout"})
    data_plane_ready = sum(1 for row in symbol_rows if row["micro_data_plane_ready"] == 1)
    training_usable = sum(1 for row in symbol_rows if row["is_training_usable"] == 1)
    spread_filled = sum(1 for row in symbol_rows if row["spread_bps"] is not None or row["spread"] is not None)
    depth_filled = sum(1 for row in symbol_rows if row["depth_imbalance"] is not None)
    ofi_lag = sum(1 for row in symbol_rows if "ofi_cvd_lag_high" in str(row["reason_codes_json"] or ""))
    stale = sum(
        1
        for row in symbol_rows
        if "ofi_stale" in str(row["reason_codes_json"] or "") or "cvd_stale" in str(row["reason_codes_json"] or "")
    )
    verdict = "PASS"
    findings: list[str] = []
    if total == 0:
        verdict = "FAIL"
        findings.append("micro_symbol_samples_empty")
    if total and not reliability_rows:
        verdict = "FAIL"
        findings.append("micro_technical_reliability_empty")
    if total and training_usable == 0:
        verdict = "PARTIAL" if verdict == "PASS" else verdict
        findings.append("no_training_usable_micro_samples_in_scope")
    if total and ofi_lag:
        verdict = "PARTIAL" if verdict == "PASS" else verdict
        findings.append("ofi_cvd_lag_high_still_present_but_now_counted")
    if total and spread_filled == 0:
        verdict = "PARTIAL" if verdict == "PASS" else verdict
        findings.append("spread_depth_event_time_frames_not_backfilled_for_historical_samples")
    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "verdict": verdict,
        "findings": findings,
        "scope": {"limit_runs": limit_runs, "symbol_samples": total, "reliability_samples": len(reliability_rows)},
        "coverage": coverage,
        "summary": {
            "ready_count": ready,
            "ready_ratio": _pct(ready, total),
            "technical_blocked_count": technical_blocked,
            "technical_blocked_ratio": _pct(technical_blocked, total),
            "data_plane_ready_count": data_plane_ready,
            "data_plane_ready_ratio": _pct(data_plane_ready, total),
            "ofi_cvd_lag_high_count": ofi_lag,
            "stale_reason_count": stale,
            "training_usable_count": training_usable,
            "training_usable_ratio": _pct(training_usable, total),
            "spread_filled_count": spread_filled,
            "spread_coverage": _pct(spread_filled, total),
            "depth_filled_count": depth_filled,
            "depth_coverage": _pct(depth_filled, total),
        },
        "distributions": {
            "strategy_line": _dict_counts(symbol_rows, "strategy_line"),
            "technical_status": _dict_counts(symbol_rows, "technical_status"),
            "alignment_state": _dict_counts(symbol_rows, "alignment_state"),
            "z_state": _dict_counts(symbol_rows, "z_state"),
            "book_cost_confidence": _dict_counts(symbol_rows, "book_cost_confidence"),
            "top_reasons": _reason_counts(symbol_rows),
        },
    }


def write_report(root: Path, report: dict[str, Any]) -> tuple[Path, Path]:
    out_json = root / "docs" / "reports" / f"STEP7.88_micro_technical_chain_regression_audit_{report['generated_at']}.json"
    out_md = root / "docs" / "reports" / f"STEP7.88_micro_technical_chain_regression_audit_{report['generated_at']}.md"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    summary = report["summary"]
    lines = [
        "# STEP7.88 Micro Technical Chain Regression Audit",
        "",
        f"- generated_at: `{report['generated_at']}`",
        f"- verdict: `{report['verdict']}`",
        f"- training_db: `{default_micro_training_db(root)}`",
        "",
        "## Scope",
        "",
        f"- limit_runs: `{report['scope']['limit_runs']}`",
        f"- symbol_samples: `{report['scope']['symbol_samples']}`",
        f"- reliability_samples: `{report['scope']['reliability_samples']}`",
        "",
        "## Summary",
        "",
        f"- ready: `{summary['ready_count']}` / `{summary['ready_ratio']}`",
        f"- technical_blocked: `{summary['technical_blocked_count']}` / `{summary['technical_blocked_ratio']}`",
        f"- data_plane_ready: `{summary['data_plane_ready_count']}` / `{summary['data_plane_ready_ratio']}`",
        f"- ofi_cvd_lag_high: `{summary['ofi_cvd_lag_high_count']}`",
        f"- stale_reason_count: `{summary['stale_reason_count']}`",
        f"- training_usable: `{summary['training_usable_count']}` / `{summary['training_usable_ratio']}`",
        f"- spread_coverage: `{summary['spread_coverage']}`",
        f"- depth_coverage: `{summary['depth_coverage']}`",
        "",
        "## Distributions",
        "",
        "### Technical Status",
        "",
    ]
    for key, value in report["distributions"]["technical_status"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "### Alignment State", ""])
    for key, value in report["distributions"]["alignment_state"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "### Z State", ""])
    for key, value in report["distributions"]["z_state"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "### Top Reasons", ""])
    for key, value in report["distributions"]["top_reasons"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Findings", ""])
    if report["findings"]:
        lines.extend([f"- {item}" for item in report["findings"]])
    else:
        lines.append("- none")
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_md, out_json


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP7.88 micro technical chain regression audit")
    parser.add_argument("--root", default=".", help="project root")
    parser.add_argument("--limit-runs", type=int, default=100)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    report = build_report(root, limit_runs=args.limit_runs)
    out_md, out_json = write_report(root, report)
    print(json.dumps({"status": "ok", "verdict": report["verdict"], "report": str(out_md), "json": str(out_json)}, ensure_ascii=False))
    return 0 if report["verdict"] != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
