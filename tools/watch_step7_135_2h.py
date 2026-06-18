from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen


EXPERIMENT_ID = "paper_exp_step7_135_strategy5_6_v5_gate_20260616"
LINES = ("strategy5", "strategy6")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_api(path: str) -> dict:
    try:
        with urlopen("http://127.0.0.1:8000" + path, timeout=8) as resp:
            got = json.loads(resp.read().decode("utf-8", errors="replace"))
        return got.get("data") if isinstance(got, dict) else {}
    except Exception as exc:
        return {"error": repr(exc)}


def read_json(path: Path) -> dict:
    try:
        got = json.loads(path.read_text(encoding="utf-8"))
        return got if isinstance(got, dict) else {}
    except Exception:
        return {}


def paper_counts(root: Path) -> dict:
    db_path = root / "DATA" / "paper" / "paper_trading.db"
    out: dict = {}
    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        for table in ("paper_intent_inbox", "paper_orders"):
            out[table] = con.execute(
                f"select count(*) c from {table} where experiment_id=?",
                (EXPERIMENT_ID,),
            ).fetchone()["c"]
        try:
            out["skip_groups"] = [
                dict(row)
                for row in con.execute(
                    """
                    select strategy_line, gate_decision, skip_reason, count(*) c, max(created_at) mx
                    from paper_skip_ledger
                    where experiment_id=?
                    group by strategy_line, gate_decision, skip_reason
                    """,
                    (EXPERIMENT_ID,),
                ).fetchall()
            ]
        except Exception as exc:
            out["skip_groups_error"] = repr(exc)
        con.close()
    except Exception as exc:
        out["error"] = repr(exc)
    return out


def trade_plans(root: Path) -> dict:
    out: dict = {}
    for line in LINES:
        doc = read_json(root / "DATA" / "decisions" / f"latest_trade_plan_{line}.json")
        items = doc.get("plans") or doc.get("trade_plans") or doc.get("items") or []
        out[line] = {
            "run_id": doc.get("run_id"),
            "cycle_id": doc.get("cycle_id"),
            "generated_at": doc.get("generated_at"),
            "count": doc.get("count"),
            "executable_count": doc.get("executable_count"),
            "plans_len": len(items) if isinstance(items, list) else None,
            "sample_symbols": [
                item.get("symbol")
                for item in (items[:5] if isinstance(items, list) else [])
                if isinstance(item, dict)
            ],
        }
    return out


def snapshot(root: Path, started: float) -> dict:
    status = read_api("/api/pipeline/status/latest")
    runtime = read_api("/api/runtime/status")
    return {
        "checked_at": utc_now(),
        "elapsed_sec": round(time.monotonic() - started, 3),
        "pipeline": {
            key: status.get(key)
            for key in ("display_state", "cycle_enabled", "next_cycle_eta_sec", "selected_lines")
        },
        "active_interval": {
            key: (status.get("active_interval") or {}).get(key)
            for key in ("pid", "status", "run_id", "cycle_id", "selected_lines", "next_run_at", "pid_running")
        },
        "progress": {
            key: (status.get("progress") or {}).get(key)
            for key in ("run_id", "cycle_id", "status", "overall_percent", "updated_at")
        },
        "runtime": {
            name: {
                key: (runtime.get(name) or {}).get(key)
                for key in ("status", "pid", "pid_running", "heartbeat_age_sec", "stale", "watchdog_status", "health_state", "data_plane_status")
            }
            for name in ("paper_daemon", "micro_daemon", "snapshot_daemon")
        },
        "plans": trade_plans(root),
        "ledger": paper_counts(root),
    }


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    out = root / "DATA" / "runtime" / "step7_135_2h_watch.jsonl"
    latest = root / "DATA" / "runtime" / "step7_135_2h_watch_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    while time.monotonic() - started <= 7200:
        payload = snapshot(root, started)
        with out.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
        latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        time.sleep(60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
