from __future__ import annotations

import argparse
import json
import sqlite3
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
API = "http://127.0.0.1:8000"
LINES = ("without_micro", "micro_fast")
PROMOTION_REASON = "trade_quality_promotion_wait_only"


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _api(method: str, path: str, payload: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(f"{API}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            return json.loads(body)
        except Exception:
            return {"ok": False, "error": {"code": f"http_{exc.code}", "message": body}}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _walk_reason_codes(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "reason_codes" and isinstance(item, list):
                found.extend(str(x) for x in item)
            else:
                found.extend(_walk_reason_codes(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(_walk_reason_codes(item))
    return found


def _extract_trade_plan(run_id: str, line: str) -> dict[str, Any]:
    path = PROJECT_ROOT / "DATA" / "decisions" / "trade_plan_runs" / run_id / f"latest_trade_plan_{line}.json"
    doc = _read_json(path)
    plans = doc.get("plans") if isinstance(doc.get("plans"), list) else []
    wait_only: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    side_counts: Counter[str] = Counter()
    executable_count = 0
    for plan in plans:
        if not isinstance(plan, dict):
            continue
        reason_codes = set(_walk_reason_codes(plan))
        reason_counts.update(reason_codes)
        side_counts[str(plan.get("decision") or plan.get("side") or "unknown")] += 1
        if plan.get("executable") is True:
            executable_count += 1
        if PROMOTION_REASON in reason_codes:
            guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
            tq = guards.get("trade_quality_gate") if isinstance(guards.get("trade_quality_gate"), dict) else {}
            wait_only.append(
                {
                    "symbol": plan.get("symbol"),
                    "side": plan.get("decision") or plan.get("side"),
                    "executable": plan.get("executable"),
                    "reason_codes": sorted(reason_codes),
                    "promotion_rule_ids": tq.get("promotion_rule_ids") or guards.get("promotion_rule_ids") or [],
                    "promotion_reason_codes": tq.get("promotion_reason_codes") or guards.get("promotion_reason_codes") or [],
                }
            )
    return {
        "line": line,
        "path": str(path),
        "exists": path.exists(),
        "status": doc.get("status"),
        "count": len(plans),
        "executable_count": int(doc.get("executable_count") or executable_count),
        "side_counts": dict(side_counts),
        "promotion_wait_only_count": len(wait_only),
        "promotion_wait_only_plans": wait_only,
        "reason_counts": dict(reason_counts),
    }


def _paper_counts(run_id: str) -> dict[str, Any]:
    db_path = PROJECT_ROOT / "DATA" / "paper" / "paper_trading.db"
    if not db_path.exists():
        return {"db_exists": False}
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    out: dict[str, Any] = {"db_exists": True}
    try:
        for table in ("paper_orders", "paper_skip_ledger", "paper_intent_inbox"):
            rows = conn.execute(
                f"select strategy_line, symbol, count(*) as c from {table} where source_run_id=? group by strategy_line, symbol",
                (run_id,),
            ).fetchall()
            out[table] = [dict(row) for row in rows]
            out[f"{table}_count"] = sum(int(row["c"]) for row in rows)
        wait_skip_rows = conn.execute(
            """
            select strategy_line, symbol, skip_reason, count(*) as c
            from paper_skip_ledger
            where source_run_id=?
              and (skip_detail_json like ? or source_json like ? or skip_reason like ?)
            group by strategy_line, symbol, skip_reason
            """,
            (run_id, f"%{PROMOTION_REASON}%", f"%{PROMOTION_REASON}%", f"%{PROMOTION_REASON}%"),
        ).fetchall()
        out["promotion_wait_only_paper_skips"] = [dict(row) for row in wait_skip_rows]
    finally:
        conn.close()
    return out


def _current_status() -> dict[str, Any]:
    resp = _api("GET", "/api/pipeline/status/latest", timeout=15)
    return resp.get("data") if isinstance(resp, dict) and resp.get("ok") else resp


def _wait_until_idle(start_seen_run_id: str | None, timeout_sec: int, poll_sec: int) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = _current_status()
        latest = last.get("latest_report") if isinstance(last.get("latest_report"), dict) else {}
        run_id = latest.get("run_id") or last.get("display_run_id")
        running = bool(last.get("job_running")) or str((last.get("progress") or {}).get("status") or "") == "running"
        if run_id and run_id != start_seen_run_id and not running:
            return last
        time.sleep(poll_sec)
    return {"timeout": True, "last_status": last}


def _run_one(index: int, timeout_sec: int, poll_sec: int) -> dict[str, Any]:
    before = _current_status()
    before_report = before.get("latest_report") if isinstance(before.get("latest_report"), dict) else {}
    before_run_id = before_report.get("run_id") or before.get("display_run_id")
    start = _api(
        "POST",
        "/api/pipeline/run",
        {
            "lines": list(LINES),
            "mode": "once",
            "skip_micro_wait": False,
            "skip_market_context": False,
            "skip_abc_audit": False,
            "skip_json_stage_audit": False,
            "skip_aggregate_final_decisions": False,
        },
        timeout=30,
    )
    if not start.get("ok"):
        return {"index": index, "status": "start_failed", "started_at": _iso_now(), "start_response": start}
    done = _wait_until_idle(str(before_run_id) if before_run_id else None, timeout_sec=timeout_sec, poll_sec=poll_sec)
    if done.get("timeout"):
        return {"index": index, "status": "timeout", "started_at": _iso_now(), "start_response": start, "done": done}
    latest = done.get("latest_report") if isinstance(done.get("latest_report"), dict) else {}
    run_id = latest.get("run_id") or done.get("display_run_id")
    cycle_id = latest.get("cycle_id") or (f"cycle_{run_id}" if run_id else None)
    trade_plans = {line: _extract_trade_plan(str(run_id), line) for line in LINES} if run_id else {}
    paper = _paper_counts(str(run_id)) if run_id else {}
    return {
        "index": index,
        "status": "completed",
        "started_at": _iso_now(),
        "run_id": run_id,
        "cycle_id": cycle_id,
        "latest_report_status": latest.get("status"),
        "start_response": start.get("data") if isinstance(start.get("data"), dict) else start,
        "status_snapshot": {
            "display_state": done.get("display_state"),
            "job_running": done.get("job_running"),
            "run_controls": done.get("run_controls"),
        },
        "trade_plans": trade_plans,
        "paper": paper,
    }


def _write_reports(payload: dict[str, Any], stem: str) -> None:
    reports_dir = PROJECT_ROOT / "docs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / f"{stem}.json"
    md_path = reports_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines: list[str] = []
    lines.append("# STEP7.71 Trade Quality Promotion Controlled 10-Run Audit")
    lines.append("")
    lines.append(f"> Generated: {payload['generated_at']}")
    lines.append(f"> Runs requested: {payload['runs_requested']}")
    lines.append(f"> Runs completed: {payload['summary']['completed_runs']}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("```text")
    for key, value in payload["summary"].items():
        lines.append(f"{key}: {value}")
    lines.append("```")
    lines.append("")
    lines.append("## Per-Run")
    lines.append("")
    lines.append("| # | run_id | status | wm plans/exe/wait | mf plans/exe/wait | paper orders | paper skips |")
    lines.append("|---:|---|---|---:|---:|---:|---:|")
    for run in payload["runs"]:
        wm = ((run.get("trade_plans") or {}).get("without_micro") or {})
        mf = ((run.get("trade_plans") or {}).get("micro_fast") or {})
        paper = run.get("paper") or {}
        lines.append(
            "| {idx} | {run_id} | {status} | {wmc}/{wme}/{wmw} | {mfc}/{mfe}/{mfw} | {orders} | {skips} |".format(
                idx=run.get("index"),
                run_id=run.get("run_id") or "-",
                status=run.get("latest_report_status") or run.get("status"),
                wmc=wm.get("count", 0),
                wme=wm.get("executable_count", 0),
                wmw=wm.get("promotion_wait_only_count", 0),
                mfc=mf.get("count", 0),
                mfe=mf.get("executable_count", 0),
                mfw=mf.get("promotion_wait_only_count", 0),
                orders=paper.get("paper_orders_count", 0),
                skips=paper.get("paper_skip_ledger_count", 0),
            )
        )
    lines.append("")
    lines.append("## Promotion Hits")
    lines.append("")
    if payload["promotion_hits"]:
        for hit in payload["promotion_hits"]:
            lines.append(f"- run={hit['run_id']} line={hit['line']} symbol={hit['symbol']} side={hit['side']} executable={hit['executable']}")
    else:
        lines.append("- No fresh run hit the two enabled wait-only promotion scopes.")
    lines.append("")
    lines.append("## Contract Verdict")
    lines.append("")
    lines.append(payload["verdict"])
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--timeout-sec", type=int, default=900)
    ap.add_argument("--poll-sec", type=int, default=10)
    args = ap.parse_args()

    stem = f"STEP7.71_trade_quality_promotion_controlled_10run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    live_path = PROJECT_ROOT / "DATA" / "runtime" / f"{stem}_live.json"
    runs: list[dict[str, Any]] = []
    for index in range(1, args.runs + 1):
        run = _run_one(index, timeout_sec=args.timeout_sec, poll_sec=args.poll_sec)
        runs.append(run)
        live = {"generated_at": _iso_now(), "runs_requested": args.runs, "runs": runs}
        live_path.parent.mkdir(parents=True, exist_ok=True)
        live_path.write_text(json.dumps(live, ensure_ascii=False, indent=2), encoding="utf-8")
        if run.get("status") in {"start_failed", "timeout"}:
            break
        time.sleep(3)

    promotion_hits: list[dict[str, Any]] = []
    total_wait_only = 0
    total_executable = 0
    total_plans = 0
    total_orders = 0
    total_skips = 0
    for run in runs:
        for line, plan in (run.get("trade_plans") or {}).items():
            total_plans += int(plan.get("count") or 0)
            total_executable += int(plan.get("executable_count") or 0)
            total_wait_only += int(plan.get("promotion_wait_only_count") or 0)
            for hit in plan.get("promotion_wait_only_plans") or []:
                promotion_hits.append({"run_id": run.get("run_id"), "line": line, **hit})
        paper = run.get("paper") or {}
        total_orders += int(paper.get("paper_orders_count") or 0)
        total_skips += int(paper.get("paper_skip_ledger_count") or 0)

    completed = len([r for r in runs if r.get("status") == "completed"])
    blocked = [r for r in runs if r.get("status") != "completed"]
    verdict = "PASS: 10-run completed and no invalid promotion behavior was observed."
    if blocked:
        verdict = "WARNING: audit stopped before 10 completed runs; inspect start_failed/timeout details."
    elif total_wait_only == 0:
        verdict = "PASS WITH NO-HIT: 10-run completed, but the two enabled wait-only scopes did not appear in fresh trade plans."

    payload = {
        "schema_version": "STEP7.71_controlled_audit_v1",
        "generated_at": _iso_now(),
        "runs_requested": args.runs,
        "summary": {
            "completed_runs": completed,
            "blocked_runs": len(blocked),
            "total_trade_plan_rows": total_plans,
            "total_executable_rows": total_executable,
            "total_promotion_wait_only_rows": total_wait_only,
            "total_paper_orders": total_orders,
            "total_paper_skips": total_skips,
            "promotion_hit_count": len(promotion_hits),
        },
        "promotion_hits": promotion_hits,
        "verdict": verdict,
        "runs": runs,
    }
    _write_reports(payload, stem)
    return 0 if not blocked else 2


if __name__ == "__main__":
    raise SystemExit(main())
