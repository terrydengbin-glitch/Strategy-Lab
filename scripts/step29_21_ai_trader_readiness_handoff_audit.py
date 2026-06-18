from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.training_readiness.handoff_audit import build_handoff_summary
from laoma_signal_engine.training_snapshot_sync import sidecar_db_path


TASK_ID = "STEP29.21"
SIDECAR_DB = sidecar_db_path(ROOT)
OUT_DIR = ROOT / "DATA" / "research" / "trade_snapshots"
STATUS_JSON = OUT_DIR / "step29_20_training_readiness_manifest_v2_status.json"
MANIFEST_JSON = OUT_DIR / "step29_20_training_readiness_manifest_v2.json"
HANDOFF_JSON = OUT_DIR / "step29_21_ai_trader_readiness_handoff_audit.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sidecar_read_only_audit() -> dict[str, Any]:
    con = sqlite3.connect(f"file:{SIDECAR_DB.resolve().as_posix()}?mode=ro", uri=True)
    try:
        con.row_factory = sqlite3.Row
        tables = {
            row["name"]
            for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        required = {
            "trade_training_samples",
            "trade_snapshot_events",
            "trade_snapshot_source_refs",
            "trade_snapshot_manifests",
            "trade_snapshot_coverage_audits",
        }
        sample_count = con.execute("SELECT COUNT(*) FROM trade_training_samples").fetchone()[0]
        event_count = con.execute("SELECT COUNT(*) FROM trade_snapshot_events").fetchone()[0]
        source_ref_count = con.execute("SELECT COUNT(*) FROM trade_snapshot_source_refs").fetchone()[0]
        source_modes = {
            str(row[0]): int(row[1])
            for row in con.execute("SELECT source_mode, COUNT(*) FROM trade_training_samples GROUP BY source_mode").fetchall()
        }
        return {
            "read_only_open": True,
            "tables_missing": sorted(required - tables),
            "sample_count": sample_count,
            "event_count": event_count,
            "source_ref_count": source_ref_count,
            "source_mode_counts": source_modes,
        }
    finally:
        con.close()


def write_report(summary: dict[str, Any]) -> Path:
    path = ROOT / "docs" / "reports" / f"STEP29.21_ai_trader_readiness_handoff_audit_{stamp()}.md"
    lines = [
        "# STEP29.21 AI Trader Readiness Handoff Audit",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- handoff_status: `{summary['handoff_status']}`",
        f"- dataset_status: `{summary['dataset_status']}`",
        f"- allowed_for_training: `{summary['allowed_for_training']}`",
        f"- sidecar_db_path: `{summary['sidecar_db_path']}`",
        f"- status_path: `{summary['status_path']}`",
        f"- manifest_path: `{summary['manifest_path']}`",
        f"- dataset_hash: `{summary['dataset_hash']}`",
        "",
        "## Coverage",
        "",
    ]
    for key, value in summary["coverage_json"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Blocking Reasons", ""])
    if summary["blocking_reasons"]:
        for item in summary["blocking_reasons"]:
            lines.append(f"- `{item}`: {summary['blocking_task_hints'].get(item)}")
    else:
        lines.append("- None.")
    lines.extend(["", "## Source Modes", ""])
    for key, value in sorted(summary["source_mode_counts"].items()):
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Read-Only Contract",
            "",
            "- AI Trader may read the sidecar DB and v2 manifest/status artifacts.",
            "- This project does not write AI Trader DB.",
            "- AI Trader must not write back into the sidecar/source business DB.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    status = read_json(STATUS_JSON)
    manifest = read_json(MANIFEST_JSON)
    summary = build_handoff_summary(status, manifest)
    summary.update(
        {
            "task_id": TASK_ID,
            "generated_at": now_iso(),
            "sidecar_read_only_audit": sidecar_read_only_audit(),
        }
    )
    report = write_report(summary)
    summary["report_path"] = report.relative_to(ROOT).as_posix()
    if not args.dry_run:
        HANDOFF_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"status": "ok", "handoff_path": str(HANDOFF_JSON), "report": str(report), **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
