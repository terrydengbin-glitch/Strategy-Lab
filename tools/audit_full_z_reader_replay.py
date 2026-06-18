"""Replay persisted full-z reader decisions for a run.

The script is read-only. It re-runs the full-z store reader for micro_full rows
already present in the micro evidence runtime ledger, making store consumption
traceable without touching strategy output.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.micro.data_quality_attribution import get_micro_evidence_runtime_v2
from laoma_signal_engine.micro.factor_frame_store import default_micro_factor_db, full_z_window_from_store


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def replay(project_root: Path, *, run_id: str | None = None) -> dict[str, Any]:
    runtime = get_micro_evidence_runtime_v2(project_root, run_id=run_id) if run_id else get_micro_evidence_runtime_v2(project_root)
    rows: list[dict[str, Any]] = []
    for row in runtime.get("symbols") or []:
        if not isinstance(row, dict) or row.get("strategy_line") != "micro_full":
            continue
        evidence = row.get("runtime_evidence") if isinstance(row.get("runtime_evidence"), dict) else {}
        timeline = evidence.get("timeline") if isinstance(evidence.get("timeline"), dict) else {}
        z_runtime = evidence.get("z_history_runtime") if isinstance(evidence.get("z_history_runtime"), dict) else {}
        expected = _num(z_runtime.get("required_length")) or 900
        now_bucket = _num(timeline.get("last_processed_bucket_ts_sec")) or _num(timeline.get("reference_bucket_ts_sec"))
        store = full_z_window_from_store(
            db_path=default_micro_factor_db(project_root),
            strategy_line="micro_full",
            symbol=str(row.get("symbol") or ""),
            now_bucket_ts_sec=int(now_bucket) if now_bucket is not None else None,
            window_sec=int(expected),
            min_valid_bucket_ratio=0.7,
            max_gap_sec=15,
        )
        rows.append(
            {
                "run_id": row.get("run_id"),
                "cycle_id": row.get("cycle_id"),
                "symbol": row.get("symbol"),
                "state": row.get("state"),
                "attributed_reason": row.get("attributed_reason"),
                "reader_replay": store,
            }
        )
    return {
        "source": "full_z_reader_offline_replay",
        "run_id": runtime.get("run_id"),
        "cycle_id": runtime.get("cycle_id"),
        "row_count": len(rows),
        "symbols": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".", help="project root")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    payload = replay(Path(args.project_root).resolve(), run_id=args.run_id)
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
