from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
API = "http://127.0.0.1:8000"
LINES = ("without_micro", "micro_fast")
PROFILE = "relaxed_profit"


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _api(method: str, path: str, payload: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
    data = None
    headers = {"Content-Type": "application/json; charset=utf-8"}
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
    except Exception as exc:
        return {"ok": False, "error": {"code": type(exc).__name__, "message": str(exc)}}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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


def _status() -> dict[str, Any]:
    resp = _api("GET", "/api/pipeline/status/latest", timeout=20)
    return resp.get("data") if resp.get("ok") else resp


def _wait_idle(before_run_id: str | None, timeout_sec: int, poll_sec: int) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = _status()
        latest = last.get("latest_report") if isinstance(last.get("latest_report"), dict) else {}
        run_id = latest.get("run_id") or last.get("display_run_id")
        running = bool(last.get("job_running")) or str((last.get("progress") or {}).get("status") or "") == "running"
        if run_id and run_id != before_run_id and not running:
            return last
        time.sleep(poll_sec)
    return {"timeout": True, "last_status": last}


def _extract_trade_plan(run_id: str, line: str) -> dict[str, Any]:
    path = PROJECT_ROOT / "DATA" / "decisions" / "trade_plan_runs" / run_id / f"latest_trade_plan_{line}.json"
    doc = _read_json(path)
    plans = doc.get("plans") if isinstance(doc.get("plans"), list) else []
    reason_counts: Counter[str] = Counter()
    side_counts: Counter[str] = Counter()
    executable = 0
    wait_like = 0
    sample: list[dict[str, Any]] = []
    for plan in plans:
        if not isinstance(plan, dict):
            continue
        reasons = sorted(set(_walk_reason_codes(plan)))
        reason_counts.update(reasons)
        side = str(plan.get("decision") or plan.get("side") or "unknown")
        side_counts[side] += 1
        if plan.get("executable") is True:
            executable += 1
        if any("wait" in r.lower() or "pullback" in r.lower() or "rebound" in r.lower() for r in reasons):
            wait_like += 1
        if len(sample) < 8:
            sample.append(
                {
                    "symbol": plan.get("symbol"),
                    "side": side,
                    "action": plan.get("action"),
                    "entry_mode": plan.get("entry_mode"),
                    "executable": bool(plan.get("executable")),
                    "reason_codes": reasons[:20],
                }
            )
    return {
        "line": line,
        "path": str(path),
        "exists": path.exists(),
        "status": doc.get("status"),
        "run_id": doc.get("run_id"),
        "cycle_id": doc.get("cycle_id"),
        "count": len(plans),
        "executable_count": int(doc.get("executable_count") or executable),
        "wait_like_count": wait_like,
        "side_counts": dict(side_counts),
        "reason_counts": dict(reason_counts.most_common(20)),
        "sample": sample,
    }


def _strategy4_snapshot() -> dict[str, Any]:
    runtime = _api("GET", "/api/strategy4/runtime", timeout=20)
    pool = _api("GET", "/api/strategy4/observe-pool", timeout=20)
    attempts = _api("GET", "/api/strategy4/attempts?limit=200", timeout=20)
    plan = _read_json(PROJECT_ROOT / "DATA" / "decisions" / "latest_trade_plan_strategy4.json")
    plans = plan.get("plans") if isinstance(plan.get("plans"), list) else []
    return {
        "runtime_ok": runtime.get("ok"),
        "runtime": runtime.get("data") if runtime.get("ok") else runtime,
        "pool_ok": pool.get("ok"),
        "pool_count": (pool.get("data") or {}).get("count") if pool.get("ok") else None,
        "pool_status_counts": (pool.get("data") or {}).get("status_counts") if pool.get("ok") else {},
        "attempts_ok": attempts.get("ok"),
        "attempt_count_visible": (attempts.get("data") or {}).get("count") if attempts.get("ok") else None,
        "latest_plan": {
            "exists": bool(plan),
            "run_id": plan.get("run_id"),
            "cycle_id": plan.get("cycle_id"),
            "count": len(plans),
            "executable_count": int(plan.get("executable_count") or sum(1 for p in plans if isinstance(p, dict) and p.get("executable"))),
            "status": plan.get("status"),
            "generated_at": plan.get("generated_at"),
        },
    }


def _paper_counts(run_id: str) -> dict[str, Any]:
    db_path = PROJECT_ROOT / "DATA" / "paper" / "paper_trading.db"
    if not db_path.exists():
        return {"db_exists": False}
    out: dict[str, Any] = {"db_exists": True}
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        for table in ("paper_intent_inbox", "paper_orders", "paper_skip_ledger"):
            try:
                rows = con.execute(
                    f"select strategy_line, count(*) as count from {table} where source_run_id=? group by strategy_line",
                    (run_id,),
                ).fetchall()
            except sqlite3.Error as exc:
                out[table] = {"error": str(exc)}
                continue
            items = [dict(row) for row in rows]
            out[table] = items
            out[f"{table}_count"] = sum(int(row["count"]) for row in items)
    finally:
        con.close()
    return out


def _run_audit_snapshot() -> dict[str, Any]:
    try:
        from laoma_signal_engine.audit.run_audit import ingest_run_audit_to_sqlite, write_run_level_audit

        doc = write_run_level_audit(PROJECT_ROOT)
        ingest = ingest_run_audit_to_sqlite(PROJECT_ROOT)
        sidecar = ((doc.get("sidecar_lines") or {}).get("strategy4") or {}) if isinstance(doc, dict) else {}
        return {
            "ok": True,
            "path": str(PROJECT_ROOT / "DATA" / "reports" / "latest_run_audit.json"),
            "db_ingest": ingest,
            "run_id": doc.get("run_id"),
            "cycle_id": doc.get("cycle_id"),
            "status": doc.get("status"),
            "sidecar_strategy4_present": bool(sidecar),
            "sidecar_strategy4": {
                "pool_count": sidecar.get("pool_count"),
                "attempt_count": sidecar.get("attempt_count"),
                "plan_count": (sidecar.get("latest_trade_plan") or {}).get("count")
                if isinstance(sidecar.get("latest_trade_plan"), dict)
                else None,
                "executable_count": (sidecar.get("latest_trade_plan") or {}).get("executable_count")
                if isinstance(sidecar.get("latest_trade_plan"), dict)
                else None,
            },
        }
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def _runtime_snapshot() -> dict[str, Any]:
    runtime = _api("GET", "/api/runtime/status", timeout=30)
    status = runtime.get("data") if runtime.get("ok") else runtime
    return {
        "runtime_ok": runtime.get("ok"),
        "status": status.get("status") if isinstance(status, dict) else None,
        "errors": status.get("errors") if isinstance(status, dict) else None,
        "micro": (status.get("micro_daemon") or {}) if isinstance(status, dict) else {},
        "paper": (status.get("paper_daemon") or {}) if isinstance(status, dict) else {},
        "snapshot": (status.get("snapshot_daemon") or {}) if isinstance(status, dict) else {},
    }


def _run_one(index: int, timeout_sec: int, poll_sec: int) -> dict[str, Any]:
    before = _status()
    latest_before = before.get("latest_report") if isinstance(before.get("latest_report"), dict) else {}
    before_run_id = latest_before.get("run_id") or before.get("display_run_id")
    started_at = _iso_now()
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
    if not start.get("ok") and ((start.get("error") or {}).get("code") == "pipeline_already_running"):
        time.sleep(20)
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
        return {"index": index, "status": "start_failed", "started_at": started_at, "start_response": start}
    done = _wait_idle(str(before_run_id) if before_run_id else None, timeout_sec=timeout_sec, poll_sec=poll_sec)
    if done.get("timeout"):
        return {"index": index, "status": "timeout", "started_at": started_at, "done": done}
    latest = done.get("latest_report") if isinstance(done.get("latest_report"), dict) else {}
    run_id = latest.get("run_id") or done.get("display_run_id")
    cycle_id = latest.get("cycle_id") or (f"cycle_{run_id}" if run_id else None)
    trade_plans = {line: _extract_trade_plan(str(run_id), line) for line in LINES} if run_id else {}
    strategy4 = _strategy4_snapshot()
    audit = _run_audit_snapshot()
    return {
        "index": index,
        "status": "completed",
        "started_at": started_at,
        "completed_at": _iso_now(),
        "run_id": run_id,
        "cycle_id": cycle_id,
        "latest_report_status": latest.get("status"),
        "selected_lines": latest.get("selected_lines") or latest.get("lines"),
        "display_state": done.get("display_state"),
        "trade_plans": trade_plans,
        "strategy4": strategy4,
        "paper": _paper_counts(str(run_id)) if run_id else {},
        "audit": audit,
        "runtime": _runtime_snapshot(),
    }


def _summarize(runs: list[dict[str, Any]]) -> dict[str, Any]:
    completed = [r for r in runs if r.get("status") == "completed"]
    totals = {
        "completed_runs": len(completed),
        "blocked_runs": len(runs) - len(completed),
        "without_micro_plans": 0,
        "without_micro_executable": 0,
        "micro_fast_plans": 0,
        "micro_fast_executable": 0,
        "strategy4_pool_last": None,
        "strategy4_attempt_visible_last": None,
        "strategy4_plan_last": None,
        "paper_orders": 0,
        "paper_skips": 0,
        "audit_sidecar_present_runs": 0,
        "runtime_unhealthy_runs": 0,
    }
    for run in completed:
        wm = ((run.get("trade_plans") or {}).get("without_micro") or {})
        mf = ((run.get("trade_plans") or {}).get("micro_fast") or {})
        totals["without_micro_plans"] += int(wm.get("count") or 0)
        totals["without_micro_executable"] += int(wm.get("executable_count") or 0)
        totals["micro_fast_plans"] += int(mf.get("count") or 0)
        totals["micro_fast_executable"] += int(mf.get("executable_count") or 0)
        paper = run.get("paper") or {}
        totals["paper_orders"] += int(paper.get("paper_orders_count") or 0)
        totals["paper_skips"] += int(paper.get("paper_skip_ledger_count") or 0)
        s4 = run.get("strategy4") or {}
        totals["strategy4_pool_last"] = s4.get("pool_count")
        totals["strategy4_attempt_visible_last"] = s4.get("attempt_count_visible")
        totals["strategy4_plan_last"] = (s4.get("latest_plan") or {}).get("count")
        if (run.get("audit") or {}).get("sidecar_strategy4_present"):
            totals["audit_sidecar_present_runs"] += 1
        if (run.get("runtime") or {}).get("status") != "ok":
            totals["runtime_unhealthy_runs"] += 1
    return totals


def _write_report(payload: dict[str, Any], stem: str) -> tuple[Path, Path]:
    reports = PROJECT_ROOT / "docs" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    json_path = reports / f"{stem}.json"
    md_path = reports / f"{stem}.md"
    _write_json(json_path, payload)
    lines = [
        "# STEP7.79 Relaxed Profit Strategy1 / Strategy2 / Strategy4 20-Run E2E Audit",
        "",
        f"> Generated: {payload['generated_at']}",
        f"> Active profile: {payload['active_profile']}",
        f"> Runs requested: {payload['runs_requested']}",
        "",
        "## Summary",
        "",
        "```text",
    ]
    for key, value in payload["summary"].items():
        lines.append(f"{key}: {value}")
    lines.extend(
        [
            "```",
            "",
            "## Per Run",
            "",
            "| # | run_id | status | wm plan/exe | mf plan/exe | s4 pool/attempt/plan | paper order/skip | audit s4 | runtime |",
            "|---:|---|---|---:|---:|---:|---:|---|---|",
        ]
    )
    for run in payload["runs"]:
        wm = ((run.get("trade_plans") or {}).get("without_micro") or {})
        mf = ((run.get("trade_plans") or {}).get("micro_fast") or {})
        s4 = run.get("strategy4") or {}
        paper = run.get("paper") or {}
        audit = run.get("audit") or {}
        runtime = run.get("runtime") or {}
        lines.append(
            "| {idx} | {run_id} | {status} | {wmc}/{wme} | {mfc}/{mfe} | {s4p}/{s4a}/{s4t} | {po}/{ps} | {audit_s4} | {rt} |".format(
                idx=run.get("index"),
                run_id=run.get("run_id") or "-",
                status=run.get("latest_report_status") or run.get("status"),
                wmc=wm.get("count", 0),
                wme=wm.get("executable_count", 0),
                mfc=mf.get("count", 0),
                mfe=mf.get("executable_count", 0),
                s4p=s4.get("pool_count"),
                s4a=s4.get("attempt_count_visible"),
                s4t=(s4.get("latest_plan") or {}).get("count"),
                po=paper.get("paper_orders_count", 0),
                ps=paper.get("paper_skip_ledger_count", 0),
                audit_s4="yes" if audit.get("sidecar_strategy4_present") else "no",
                rt=runtime.get("status") or "-",
            )
        )
    lines.extend(["", "## Verdict", "", payload["verdict"], ""])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--timeout-sec", type=int, default=900)
    parser.add_argument("--poll-sec", type=int, default=10)
    parser.add_argument("--live-path", default="")
    args = parser.parse_args()

    stem = f"STEP7.79_relaxed_profit_strategy1_2_4_20run_{_stamp()}"
    live_path = Path(args.live_path) if args.live_path else PROJECT_ROOT / "DATA" / "runtime" / f"{stem}_live.json"
    profiles = _api("GET", "/api/config/profiles", timeout=20)
    active = ((profiles.get("data") or {}).get("active_profile") if profiles.get("ok") else None)
    if active != PROFILE:
        apply = _api("POST", f"/api/config/profiles/{PROFILE}/apply", timeout=30)
        active = ((apply.get("data") or {}).get("active_profile") if apply.get("ok") else active)

    runs: list[dict[str, Any]] = []
    for index in range(1, args.runs + 1):
        run = _run_one(index, timeout_sec=args.timeout_sec, poll_sec=args.poll_sec)
        runs.append(run)
        live = {
            "schema_version": "STEP7.79_live_v1",
            "generated_at": _iso_now(),
            "active_profile": active,
            "runs_requested": args.runs,
            "summary": _summarize(runs),
            "runs": runs,
        }
        _write_json(live_path, live)
        print(json.dumps({"event": "run_complete", "index": index, "run_id": run.get("run_id"), "status": run.get("status")}, ensure_ascii=False), flush=True)
        if run.get("status") != "completed":
            break
        time.sleep(3)

    summary = _summarize(runs)
    verdict = "PASS: requested 20 runs completed with Strategy1/2/4 evidence captured."
    if summary["completed_runs"] < args.runs:
        verdict = "BLOCKED: run loop stopped before requested count; inspect live JSON."
    elif summary["audit_sidecar_present_runs"] < summary["completed_runs"]:
        verdict = "WARNING: completed runs but Strategy4 sidecar audit evidence was not present for every run."
    elif summary["runtime_unhealthy_runs"]:
        verdict = "WARNING: completed runs but runtime health was not ok in one or more runs."
    payload = {
        "schema_version": "STEP7.79_report_v1",
        "generated_at": _iso_now(),
        "active_profile": active,
        "runs_requested": args.runs,
        "summary": summary,
        "verdict": verdict,
        "live_path": str(live_path),
        "runs": runs,
    }
    json_path, md_path = _write_report(payload, stem)
    _write_json(live_path, payload)
    print(json.dumps({"event": "report_written", "json": str(json_path), "md": str(md_path), "verdict": verdict}, ensure_ascii=False), flush=True)
    return 0 if summary["completed_runs"] == args.runs else 2


if __name__ == "__main__":
    raise SystemExit(main())
