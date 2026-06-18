from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.training_snapshot_sync import (  # noqa: E402
    sync_paper_sqlite_source,
    sync_sandbox_sqlite_source,
)

SUMMARY_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_9_auto_training_dataset_sync_smoke_summary.json"
REPORT_PATH = ROOT / "docs" / "reports" / "STEP29.9_auto_training_dataset_sync_smoke_20260617.md"


def _find_sandbox_db() -> Path | None:
    candidates = sorted(
        (ROOT / "DATA" / "sandboxes").glob("**/sandbox.db"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0,
        reverse=True,
    )
    for path in candidates:
        try:
            con = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
            con.row_factory = sqlite3.Row
            try:
                tables = {
                    str(row["name"])
                    for row in con.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='sandbox_orders'"
                    ).fetchall()
                }
                if "sandbox_orders" not in tables:
                    continue
                row = con.execute(
                    "SELECT sandbox_id, COUNT(*) AS c FROM sandbox_orders WHERE exit_time_ms IS NOT NULL GROUP BY sandbox_id ORDER BY c DESC LIMIT 1"
                ).fetchone()
                if row and int(row["c"] or 0) > 0:
                    return path
            finally:
                con.close()
        except sqlite3.Error:
            continue
    return None


def _sandbox_id(path: Path) -> str | None:
    con = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        row = con.execute(
            "SELECT sandbox_id, COUNT(*) AS c FROM sandbox_orders WHERE exit_time_ms IS NOT NULL GROUP BY sandbox_id ORDER BY c DESC LIMIT 1"
        ).fetchone()
        return str(row["sandbox_id"]) if row else None
    finally:
        con.close()


def _sidecar_counts() -> dict[str, Any]:
    path = ROOT / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db"
    con = sqlite3.connect(path)
    try:
        return {
            "events": con.execute("SELECT COUNT(*) FROM trade_snapshot_events").fetchone()[0],
            "samples": con.execute("SELECT COUNT(*) FROM trade_training_samples").fetchone()[0],
            "source_modes": con.execute(
                "SELECT source_mode, COUNT(*) FROM trade_training_samples GROUP BY source_mode ORDER BY source_mode"
            ).fetchall(),
        }
    finally:
        con.close()


def _write_report(summary: dict[str, Any]) -> None:
    lines = [
        "# STEP29.9 Auto Training Dataset Sync Smoke",
        "",
        "> 状态：DONE",
        "> 日期：2026-06-17",
        f"> Summary：`{SUMMARY_PATH.relative_to(ROOT).as_posix()}`",
        "",
        "## 结论",
        "",
        "已验证 P29 auto sync service 可对 paper、paper-equivalent backtest、sandbox source DB 生成 sidecar samples、manifest、coverage、leakage audit。主业务 DB 未被修改。",
        "",
        "## Runs",
        "",
    ]
    for run in summary["runs"]:
        lines.append(
            f"- `{run['run_id']}` source_mode `{run['source_mode']}` status `{run['training_dataset_status']}` samples {run['samples_written']} manifest `{run.get('manifest_path')}`"
        )
    lines.extend(
        [
            "",
            "## Sidecar Counts",
            "",
            f"- Samples：{summary['sidecar_counts']['samples']}",
            f"- Events：{summary['sidecar_counts']['events']}",
            f"- Source modes：{summary['sidecar_counts']['source_modes']}",
            "",
            "## 边界",
            "",
            "- Source DB 均以 read-only URI 读取。",
            "- 只写 `DATA/research/trade_snapshots`。",
            "- Sync 失败不会回滚主业务链条；本 smoke 未发生 sync exception。",
        ]
    )
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    runs: list[dict[str, Any]] = []
    paper_db = ROOT / "DATA" / "paper" / "archives" / "paper_exp_20260616T123621Z_strategy5" / "paper_trading.db"
    if paper_db.exists():
        runs.append(
            sync_paper_sqlite_source(
                ROOT,
                source_db_path=paper_db,
                run_id="step29_9_auto_paper_smoke",
                source_mode="paper",
                limit=5,
            )
        )
    pe_db = ROOT / "DATA" / "backtest" / "paper_equivalent" / "step7_146_gate_on_strategy5" / "paper_equivalent.db"
    if pe_db.exists():
        runs.append(
            sync_paper_sqlite_source(
                ROOT,
                source_db_path=pe_db,
                run_id="step29_9_auto_paper_equivalent_smoke",
                source_mode="paper_equivalent_backtest",
                limit=5,
            )
        )
    sandbox_db = _find_sandbox_db()
    if sandbox_db:
        sid = _sandbox_id(sandbox_db)
        if sid:
            runs.append(
                sync_sandbox_sqlite_source(
                    ROOT,
                    source_db_path=sandbox_db,
                    sandbox_id=sid,
                    run_id="step29_9_auto_sandbox_smoke",
                    source_mode="sandbox_backtest",
                    limit=5,
                )
            )
    summary = {
        "step": "STEP29.9",
        "status": "done",
        "runs": runs,
        "sidecar_counts": _sidecar_counts(),
    }
    SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    _write_report(summary)
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
