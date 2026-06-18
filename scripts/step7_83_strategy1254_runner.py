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
PIPELINE_LINES = ("without_micro", "micro_fast", "strategy5")
OBSERVED_LINES = ("without_micro", "micro_fast", "strategy4", "strategy5")


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _api(method: str, path: str, payload: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
    data = None
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8", newline="\n")


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
        progress = last.get("progress") if isinstance(last.get("progress"), dict) else {}
        running = bool(last.get("job_running")) or str(progress.get("status") or "") == "running"
        if run_id and run_id != before_run_id and not running:
            return last
        time.sleep(poll_sec)
    return {"timeout": True, "last_status": last}


def _trade_plan_path(run_id: str, line: str) -> Path:
    suffix = line
    return PROJECT_ROOT / "DATA" / "decisions" / "trade_plan_runs" / run_id / f"latest_trade_plan_{suffix}.json"


def _extract_trade_plan(run_id: str, line: str) -> dict[str, Any]:
    path = _trade_plan_path(run_id, line)
    if line == "strategy4" and not path.exists():
        path = PROJECT_ROOT / "DATA" / "decisions" / "latest_trade_plan_strategy4.json"
    doc = _read_json(path)
    plans = doc.get("plans") if isinstance(doc.get("plans"), list) else []
    reason_counts: Counter[str] = Counter()
    side_counts: Counter[str] = Counter()
    executable = 0
    samples: list[dict[str, Any]] = []
    for plan in plans:
        if not isinstance(plan, dict):
            continue
        reasons = sorted(set(_walk_reason_codes(plan)))
        reason_counts.update(reasons)
        side = str(plan.get("decision") or plan.get("side") or "unknown")
        side_counts[side] += 1
        if plan.get("executable") is True:
            executable += 1
        if len(samples) < 8:
            guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
            samples.append(
                {
                    "symbol": plan.get("symbol"),
                    "side": side,
                    "action": plan.get("action"),
                    "entry_mode": plan.get("entry_mode"),
                    "executable": bool(plan.get("executable")),
                    "strategy5_label": guards.get("strategy5_shadow_label"),
                    "strategy5_side": guards.get("strategy5_shadow_hypothesis_side"),
                    "reason_codes": reasons[:20],
                }
            )
    return {
        "line": line,
        "path": str(path),
        "exists": path.exists(),
        "status": doc.get("status") if doc else "missing",
        "run_id": doc.get("run_id") if doc else None,
        "cycle_id": doc.get("cycle_id") if doc else None,
        "count": len(plans),
        "executable_count": int(doc.get("executable_count") or executable) if doc else 0,
        "side_counts": dict(side_counts),
        "reason_counts": dict(reason_counts.most_common(20)),
        "sample": samples,
    }


def _strategy4_snapshot() -> dict[str, Any]:
    runtime = _api("GET", "/api/strategy4/runtime", timeout=20)
    pool = _api("GET", "/api/strategy4/observe-pool", timeout=20)
    attempts = _api("GET", "/api/strategy4/attempts?limit=200", timeout=20)
    return {
        "runtime_ok": runtime.get("ok"),
        "runtime_state": (((runtime.get("data") or {}).get("status") or {}).get("state") if runtime.get("ok") else None),
        "pool_ok": pool.get("ok"),
        "pool_count": (pool.get("data") or {}).get("count") if pool.get("ok") else None,
        "pool_status_counts": (pool.get("data") or {}).get("status_counts") if pool.get("ok") else {},
        "attempts_ok": attempts.get("ok"),
        "attempt_count_visible": (attempts.get("data") or {}).get("count") if attempts.get("ok") else None,
    }


def _strategy5_runtime_snapshot() -> dict[str, Any]:
    runtime = _api("GET", "/api/strategy5/runtime?limit=20", timeout=20)
    evidence = _api("GET", "/api/strategy5/evidence?limit=20", timeout=20)
    return {
        "runtime_ok": runtime.get("ok"),
        "latest_trade_plan": (runtime.get("data") or {}).get("latest_trade_plan") if runtime.get("ok") else {},
        "ledger_count": (runtime.get("data") or {}).get("ledger_count") if runtime.get("ok") else None,
        "evidence_ok": evidence.get("ok"),
        "evidence_count": (evidence.get("data") or {}).get("count") if evidence.get("ok") else None,
        "evidence_display_count": (evidence.get("data") or {}).get("display_count") if evidence.get("ok") else None,
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


def _run_audit_snapshot() -> dict[str, Any]:
    try:
        from laoma_signal_engine.audit.run_audit import ingest_run_audit_to_sqlite, write_run_level_audit

        doc = write_run_level_audit(PROJECT_ROOT)
        ingest = ingest_run_audit_to_sqlite(PROJECT_ROOT)
        lines = doc.get("strategy_lines") if isinstance(doc.get("strategy_lines"), dict) else {}
        sidecars = doc.get("sidecar_lines") if isinstance(doc.get("sidecar_lines"), dict) else {}
        return {
            "ok": True,
            "db_ingest": ingest,
            "run_id": doc.get("run_id"),
            "cycle_id": doc.get("cycle_id"),
            "status": doc.get("status"),
            "strategy_lines_present": sorted(lines.keys()),
            "strategy5_present": "strategy5" in lines,
            "strategy4_sidecar_present": "strategy4" in sidecars,
        }
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def _closed_deal_count() -> int | None:
    db_path = PROJECT_ROOT / "DATA" / "paper" / "paper_trading.db"
    if not db_path.exists():
        return None
    con = sqlite3.connect(db_path)
    try:
        for table in ("paper_orders", "orders"):
            try:
                row = con.execute(f"select count(*) from {table} where status in ('closed','CLOSED')").fetchone()
            except sqlite3.Error:
                continue
            if row:
                return int(row[0])
    finally:
        con.close()
    return None


def _run_one(index: int, timeout_sec: int, poll_sec: int) -> dict[str, Any]:
    before = _status()
    latest_before = before.get("latest_report") if isinstance(before.get("latest_report"), dict) else {}
    before_run_id = latest_before.get("run_id") or before.get("display_run_id")
    started_at = _iso_now()
    start = _api(
        "POST",
        "/api/pipeline/run",
        {
            "lines": list(PIPELINE_LINES),
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
                "lines": list(PIPELINE_LINES),
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
    trade_plans = {line: _extract_trade_plan(str(run_id), line) for line in OBSERVED_LINES} if run_id else {}
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
        "strategy4": _strategy4_snapshot(),
        "strategy5_runtime": _strategy5_runtime_snapshot(),
        "paper": _paper_counts(str(run_id)) if run_id else {},
        "audit": _run_audit_snapshot(),
        "runtime": _runtime_snapshot(),
        "closed_deal_count": _closed_deal_count(),
    }


def _summarize(runs: list[dict[str, Any]]) -> dict[str, Any]:
    completed = [r for r in runs if r.get("status") == "completed"]
    totals: dict[str, Any] = {
        "completed_runs": len(completed),
        "blocked_runs": len(runs) - len(completed),
        "paper_orders": 0,
        "paper_skips": 0,
        "runtime_unhealthy_runs": 0,
        "audit_strategy5_present_runs": 0,
        "audit_strategy4_sidecar_present_runs": 0,
        "strategy4_pool_last": None,
        "strategy4_attempt_visible_last": None,
        "closed_deal_count_last": None,
    }
    for line in OBSERVED_LINES:
        totals[f"{line}_plans"] = 0
        totals[f"{line}_executable"] = 0
        totals[f"{line}_missing_runs"] = 0
        totals[f"{line}_status_counts"] = {}

    status_counters: dict[str, Counter[str]] = {line: Counter() for line in OBSERVED_LINES}
    for run in completed:
        for line in OBSERVED_LINES:
            row = ((run.get("trade_plans") or {}).get(line) or {})
            totals[f"{line}_plans"] += int(row.get("count") or 0)
            totals[f"{line}_executable"] += int(row.get("executable_count") or 0)
            if not row.get("exists"):
                totals[f"{line}_missing_runs"] += 1
            status_counters[line][str(row.get("status") or "missing")] += 1
        paper = run.get("paper") or {}
        totals["paper_orders"] += int(paper.get("paper_orders_count") or 0)
        totals["paper_skips"] += int(paper.get("paper_skip_ledger_count") or 0)
        s4 = run.get("strategy4") or {}
        totals["strategy4_pool_last"] = s4.get("pool_count")
        totals["strategy4_attempt_visible_last"] = s4.get("attempt_count_visible")
        audit = run.get("audit") or {}
        if audit.get("strategy5_present"):
            totals["audit_strategy5_present_runs"] += 1
        if audit.get("strategy4_sidecar_present"):
            totals["audit_strategy4_sidecar_present_runs"] += 1
        runtime = run.get("runtime") or {}
        if runtime.get("status") != "ok":
            totals["runtime_unhealthy_runs"] += 1
        if run.get("closed_deal_count") is not None:
            totals["closed_deal_count_last"] = run.get("closed_deal_count")
    for line, counter in status_counters.items():
        totals[f"{line}_status_counts"] = dict(counter)
    return totals


def _write_report(payload: dict[str, Any], stem: str) -> tuple[Path, Path]:
    reports = PROJECT_ROOT / "docs" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    json_path = reports / f"{stem}.json"
    md_path = reports / f"{stem}.md"
    _write_json(json_path, payload)
    lines = [
        "# STEP7.83 Strategy1/2/4/5 Strict E2E Audit",
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
            "| # | run_id | status | wm plan/exe | mf plan/exe | s4 plan/exe | s5 plan/exe | paper order/skip | audit s5/s4 | runtime |",
            "|---:|---|---|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    for run in payload["runs"]:
        plans = run.get("trade_plans") or {}
        wm = plans.get("without_micro") or {}
        mf = plans.get("micro_fast") or {}
        s4 = plans.get("strategy4") or {}
        s5 = plans.get("strategy5") or {}
        paper = run.get("paper") or {}
        audit = run.get("audit") or {}
        runtime = run.get("runtime") or {}
        lines.append(
            "| {idx} | {run_id} | {status} | {wmc}/{wme} | {mfc}/{mfe} | {s4c}/{s4e} | {s5c}/{s5e} | {po}/{ps} | {as5}/{as4} | {rt} |".format(
                idx=run.get("index"),
                run_id=run.get("run_id") or "-",
                status=run.get("latest_report_status") or run.get("status"),
                wmc=wm.get("count", 0),
                wme=wm.get("executable_count", 0),
                mfc=mf.get("count", 0),
                mfe=mf.get("executable_count", 0),
                s4c=s4.get("count", 0),
                s4e=s4.get("executable_count", 0),
                s5c=s5.get("count", 0),
                s5e=s5.get("executable_count", 0),
                po=paper.get("paper_orders_count", 0),
                ps=paper.get("paper_skip_ledger_count", 0),
                as5="yes" if audit.get("strategy5_present") else "no",
                as4="yes" if audit.get("strategy4_sidecar_present") else "no",
                rt=runtime.get("status") or "-",
            )
        )
    lines.extend(["", "## Verdict", "", payload["verdict"], ""])
    md_path.write_text("\n".join(lines), encoding="utf-8", newline="\n")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=50)
    parser.add_argument("--timeout-sec", type=int, default=1200)
    parser.add_argument("--poll-sec", type=int, default=10)
    parser.add_argument("--live-path", default="")
    args = parser.parse_args()

    stem = f"STEP7.83_strategy1_2_4_5_strict_e2e_audit_{_stamp()}"
    live_path = Path(args.live_path) if args.live_path else PROJECT_ROOT / "DATA" / "runtime" / f"{stem}_live.json"
    profiles = _api("GET", "/api/config/profiles", timeout=20)
    active = ((profiles.get("data") or {}).get("active_profile") if profiles.get("ok") else None)

    runs: list[dict[str, Any]] = []
    for index in range(1, args.runs + 1):
        run = _run_one(index, timeout_sec=args.timeout_sec, poll_sec=args.poll_sec)
        runs.append(run)
        live = {
            "schema_version": "STEP7.83_live_v1",
            "generated_at": _iso_now(),
            "active_profile": active,
            "pipeline_lines": list(PIPELINE_LINES),
            "observed_lines": list(OBSERVED_LINES),
            "runs_requested": args.runs,
            "summary": _summarize(runs),
            "runs": runs,
        }
        _write_json(live_path, live)
        print(
            json.dumps(
                {
                    "event": "run_complete",
                    "index": index,
                    "run_id": run.get("run_id"),
                    "status": run.get("status"),
                    "latest_report_status": run.get("latest_report_status"),
                    "summary": live["summary"],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        if run.get("status") != "completed":
            break
        time.sleep(3)

    summary = _summarize(runs)
    verdict = "PASS: requested controlled Strategy1/2/4/5 runs completed."
    if summary["completed_runs"] < args.runs:
        verdict = "BLOCKED: run loop stopped before requested count; inspect live JSON."
    elif summary["strategy5_missing_runs"]:
        verdict = "FAILED: Strategy5 missing in one or more runs."
    elif summary["audit_strategy5_present_runs"] < summary["completed_runs"]:
        verdict = "WARNING: completed runs but audit did not expose Strategy5 for every run."
    elif summary["runtime_unhealthy_runs"]:
        verdict = "WARNING: completed runs but runtime health was not ok in one or more runs."
    payload = {
        "schema_version": "STEP7.83_report_v1",
        "generated_at": _iso_now(),
        "active_profile": active,
        "pipeline_lines": list(PIPELINE_LINES),
        "observed_lines": list(OBSERVED_LINES),
        "runs_requested": args.runs,
        "summary": summary,
        "verdict": verdict,
        "live_path": str(live_path),
        "runs": runs,
    }
    json_path, md_path = _write_report(payload, stem)
    _write_json(live_path, payload)
    print(json.dumps({"event": "report_written", "json": str(json_path), "md": str(md_path), "verdict": verdict}, ensure_ascii=False), flush=True)
    return 0 if summary["completed_runs"] == args.runs and not summary["strategy5_missing_runs"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
