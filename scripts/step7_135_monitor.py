from __future__ import annotations

import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen


EXPERIMENT_ID = "paper_exp_step7_135_strategy5_6_v5_gate_20260616"
LINES = ("strategy5", "strategy6")


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor STEP7.135 Strategy5/6 V5 gated paper experiment.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--api-base", default="http://127.0.0.1:8000")
    parser.add_argument("--duration-sec", type=int, default=36000)
    parser.add_argument("--first-hour-sec", type=int, default=3600)
    parser.add_argument("--first-interval-sec", type=int, default=60)
    parser.add_argument("--later-interval-sec", type=int, default=1800)
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    runtime_dir = project_root / "DATA" / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = runtime_dir / "step7_135_monitor.jsonl"
    latest_path = runtime_dir / "step7_135_latest.json"

    started = time.monotonic()
    while True:
        elapsed = time.monotonic() - started
        payload = build_snapshot(project_root, args.api_base, elapsed)
        with jsonl_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
        latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        if elapsed >= args.duration_sec:
            break
        interval = args.first_interval_sec if elapsed < args.first_hour_sec else args.later_interval_sec
        time.sleep(max(5, interval))
    return 0


def build_snapshot(project_root: Path, api_base: str, elapsed_sec: float) -> dict[str, Any]:
    paper_db = project_root / "DATA" / "paper" / "paper_trading.db"
    gate_config = read_json(project_root / "DATA" / "paper" / "v5_trade_gate_experiment.json")
    return {
        "source": "step7_135_v5_gated_paper_monitor",
        "experiment_id": EXPERIMENT_ID,
        "generated_at": utc_now_iso(),
        "elapsed_sec": round(elapsed_sec, 3),
        "api": {
            "runtime_status": api_get(api_base, "/api/runtime/status"),
            "pipeline_latest": api_get(api_base, "/api/pipeline/status/latest"),
            "pipeline_progress": api_get(api_base, "/api/pipeline/progress"),
            "paper_summary": api_get(api_base, "/api/paper/summary"),
        },
        "gate_config": {
            "enabled": gate_config.get("enabled"),
            "experiment_id": gate_config.get("experiment_id"),
            "line_epochs": gate_config.get("line_epochs"),
            "rule_ids": {
                line: (((gate_config.get("rules") or {}).get(line) or {}).get("gate_candidate_id"))
                for line in LINES
            },
        },
        "paper_ledger": paper_counts(paper_db),
    }


def api_get(api_base: str, path: str) -> dict[str, Any]:
    try:
        with urlopen(f"{api_base.rstrip('/')}{path}", timeout=8) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        got = json.loads(body)
        if isinstance(got, dict):
            return {"ok": True, "payload": got}
        return {"ok": True, "payload": {"value": got}}
    except (OSError, URLError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": str(exc)}


def paper_counts(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"ok": False, "error": f"missing db: {db_path}"}
    out: dict[str, Any] = {"ok": True}
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        out["orders_by_gate"] = grouped(
            conn,
            """
            SELECT strategy_line, COALESCE(gate_decision, 'missing') AS gate_decision, COUNT(*) AS count
            FROM paper_orders
            WHERE experiment_id = ?
            GROUP BY strategy_line, COALESCE(gate_decision, 'missing')
            ORDER BY strategy_line, gate_decision
            """,
            (EXPERIMENT_ID,),
        )
        out["skips_by_gate"] = grouped(
            conn,
            """
            SELECT strategy_line,
                   COALESCE(gate_decision, 'missing') AS gate_decision,
                   COALESCE(skip_reason, 'missing') AS skip_reason,
                   COUNT(*) AS count
            FROM paper_skip_ledger
            WHERE experiment_id = ?
            GROUP BY strategy_line, COALESCE(gate_decision, 'missing'), COALESCE(skip_reason, 'missing')
            ORDER BY strategy_line, gate_decision, skip_reason
            """,
            (EXPERIMENT_ID,),
        )
        out["latest_orders"] = grouped(
            conn,
            """
            SELECT id AS order_id, strategy_line, symbol, side, status, gate_decision, gate_candidate_id, created_at
            FROM paper_orders
            WHERE experiment_id = ?
            ORDER BY created_at DESC
            LIMIT 10
            """,
            (EXPERIMENT_ID,),
        )
        out["latest_skips"] = grouped(
            conn,
            """
            SELECT strategy_line, symbol, skip_reason, gate_decision, gate_candidate_id, created_at
            FROM paper_skip_ledger
            WHERE experiment_id = ?
            ORDER BY created_at DESC
            LIMIT 20
            """,
            (EXPERIMENT_ID,),
        )
    return out


def grouped(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def read_json(path: Path) -> dict[str, Any]:
    try:
        got = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return got if isinstance(got, dict) else {}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
