from __future__ import annotations

import argparse
import json
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


API = "http://127.0.0.1:8000"
LINES = ["without_micro", "micro_fast"]


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_error": str(exc), "_path": str(path)}


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def api_get(path: str, timeout: int = 8) -> dict[str, Any]:
    url = f"{API}{path}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return {"ok": True, "status": resp.status, "url": url, "payload": json.loads(raw)}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "status": exc.code, "url": url, "error": body}
    except Exception as exc:
        return {"ok": False, "status": None, "url": url, "error": str(exc)}


def api_post(path: str, body: dict[str, Any], timeout: int = 12) -> dict[str, Any]:
    url = f"{API}{path}"
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return {"ok": True, "status": resp.status, "url": url, "payload": json.loads(raw)}
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "status": exc.code, "url": url, "error": body_text}
    except Exception as exc:
        return {"ok": False, "status": None, "url": url, "error": str(exc)}


def unwrap(response: dict[str, Any]) -> Any:
    payload = response.get("payload")
    if isinstance(payload, dict) and "data" in payload:
        return payload.get("data")
    return payload


def run_id_from_status(status_response: dict[str, Any]) -> str | None:
    data = unwrap(status_response)
    if not isinstance(data, dict):
        return None
    latest = data.get("latest_report")
    if isinstance(latest, dict) and latest.get("run_id"):
        return str(latest["run_id"])
    if data.get("display_run_id"):
        return str(data["display_run_id"])
    return None


def latest_report_status(status_response: dict[str, Any]) -> str | None:
    data = unwrap(status_response)
    if not isinstance(data, dict):
        return None
    latest = data.get("latest_report")
    if isinstance(latest, dict):
        return str(latest.get("status") or "")
    return None


def latest_report_from_status(status_response: dict[str, Any]) -> dict[str, Any]:
    data = unwrap(status_response)
    if not isinstance(data, dict):
        return {}
    latest = data.get("latest_report")
    return latest if isinstance(latest, dict) else {}


def sqlite_query(db_path: Path, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    if not db_path.exists():
        return {"ok": False, "error": "db_missing", "db_path": str(db_path)}
    try:
        con = sqlite3.connect(str(db_path))
        con.row_factory = sqlite3.Row
        rows = [dict(row) for row in con.execute(sql, params).fetchall()]
        con.close()
        return {"ok": True, "rows": rows, "db_path": str(db_path)}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "db_path": str(db_path), "sql": sql}


def collect_preflight(root: Path) -> dict[str, Any]:
    return {
        "generated_at": iso_now(),
        "health": api_get("/api/health"),
        "runtime_status": api_get("/api/runtime/status"),
        "pipeline_status": api_get("/api/pipeline/status/latest"),
        "pipeline_watchdog": api_get("/api/pipeline/watchdog"),
        "config": api_get("/api/config"),
        "config_profiles": api_get("/api/config/profiles"),
        "paper_summary": api_get("/api/paper/summary"),
        "paper_daemon": api_get("/api/paper/daemon/status"),
        "runtime_files": {
            "interval_pid": read_json(root / "DATA/runtime/api_pipeline_interval.pid"),
            "progress": read_json(root / "DATA/runtime/strategy_pipeline_progress.json"),
        },
    }


def collect_run_packet(root: Path, run_id: str) -> dict[str, Any]:
    run_dir = root / "DATA/decisions/trade_plan_runs" / run_id
    packet = {
        "collected_at": iso_now(),
        "run_id": run_id,
        "pipeline_status": api_get("/api/pipeline/status/latest"),
        "pipeline_watchdog": api_get("/api/pipeline/watchdog"),
        "runtime_status": api_get("/api/runtime/status"),
        "run_audit": api_get(f"/api/audit/runs/{run_id}"),
        "micro_quality": api_get(f"/api/audit/micro-quality/{run_id}"),
        "micro_evidence": api_get(f"/api/audit/micro-evidence/{run_id}"),
        "micro_fast_runtime": api_get(f"/api/audit/micro-fast-runtime/{run_id}"),
        "micro_fast_judgeable": api_get(f"/api/audit/micro-fast-runtime/judgeable/{run_id}"),
        "micro_fast_coverage_split": api_get(f"/api/audit/micro-fast-runtime/coverage-split/{run_id}"),
        "micro_fast_valid_bucket": api_get(f"/api/audit/micro-fast-runtime/valid-bucket/{run_id}"),
        "trade_plans_api": api_get("/api/decisions/trade-plans"),
        "paper_summary": api_get("/api/paper/summary"),
        "paper_orders": api_get("/api/paper/orders"),
        "paper_positions": api_get("/api/paper/positions"),
        "paper_fills": api_get("/api/paper/fills"),
        "paper_stats": api_get("/api/paper/stats"),
        "latest_report_file": read_json(root / "DATA/reports/latest_strategy_pipeline_report.json"),
        "progress_file": read_json(root / "DATA/runtime/strategy_pipeline_progress.json"),
        "trade_plan_archives": {
            "manifest": read_json(run_dir / "manifest.json"),
            "without_micro": read_json(run_dir / "latest_trade_plan_without_micro.json"),
            "micro_fast": read_json(run_dir / "latest_trade_plan_micro_fast.json"),
        },
        "latest_trade_plans": {
            "without_micro": read_json(root / "DATA/decisions/latest_trade_plan_without_micro.json"),
            "micro_fast": read_json(root / "DATA/decisions/latest_trade_plan_micro_fast.json"),
        },
        "micro_files": {
            "state": read_json(root / "DATA/micro/latest_micro_state.json"),
            "features": read_json(root / "DATA/micro/latest_micro_features.json"),
            "lifecycle_micro_fast": read_json(root / "DATA/micro/latest_micro_lifecycle_micro_fast.json"),
            "wait_pass_micro_fast": read_json(root / "DATA/micro/evidence/latest_wait_pass_micro_fast.json"),
        },
        "sqlite": {
            "run_audit_runs": sqlite_query(
                root / "DATA/audit/run_audit.db",
                "select run_id, cycle_id, audit_status, fail_count, warn_count, symbol_count, generated_at from run_audits where run_id = ?",
                (run_id,),
            ),
            "paper_plans": sqlite_query(
                root / "DATA/paper/paper_trading.db",
                "select id, source_run_id, source_cycle_id, strategy_line, symbol, source_executable, source_plan_hash, status, created_at from paper_trade_plans where source_run_id = ? order by created_at",
                (run_id,),
            ),
            "paper_orders": sqlite_query(
                root / "DATA/paper/paper_trading.db",
                "select id, source_run_id, source_cycle_id, strategy_line, symbol, side, status, source_plan_hash, created_at from paper_orders where source_run_id = ? order by created_at",
                (run_id,),
            ),
        },
    }
    return packet


def summarize_trade_plan(line_payload: Any) -> dict[str, Any]:
    if not isinstance(line_payload, dict):
        return {"plan_count": 0, "executable_count": 0, "bad_step1063": 0}
    plans = line_payload.get("plans") or line_payload.get("trade_plans") or []
    if not isinstance(plans, list):
        plans = []
    bad = 0
    executable = 0
    trade_worthiness_enter_now = 0
    for plan in plans:
        if not isinstance(plan, dict):
            continue
        guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
        if plan.get("executable") is True:
            executable += 1
            if str(guards.get("sl_tp_model_version")) != "10.63":
                bad += 1
            if guards.get("single_tp_reachable") is not True:
                bad += 1
            if guards.get("fallback_target_only") is True:
                bad += 1
            try:
                if float(guards.get("effective_rr", -999)) < float(guards.get("min_effective_rr", 0)):
                    bad += 1
            except Exception:
                bad += 1
        if str(guards.get("trade_worthiness") or "") == "enter_now":
            trade_worthiness_enter_now += 1
    return {
        "plan_count": len(plans),
        "executable_count": executable,
        "bad_step1063": bad,
        "trade_worthiness_enter_now": trade_worthiness_enter_now,
    }


def summarize_run(packet: dict[str, Any]) -> dict[str, Any]:
    status_data = unwrap(packet.get("pipeline_status", {}))
    latest = status_data.get("latest_report") if isinstance(status_data, dict) else {}
    progress = status_data.get("progress") if isinstance(status_data, dict) else {}
    line_progress = progress.get("lines") if isinstance(progress, dict) else {}
    archives = packet.get("trade_plan_archives") or {}
    without_summary = summarize_trade_plan(archives.get("without_micro"))
    micro_summary = summarize_trade_plan(archives.get("micro_fast"))
    paper_orders = packet.get("sqlite", {}).get("paper_orders", {}).get("rows", [])
    paper_order_count = len(paper_orders) if isinstance(paper_orders, list) else 0
    latest_file = packet.get("latest_report_file")
    if isinstance(latest_file, dict):
        settlement = latest_file.get("paper_settlement")
        if isinstance(settlement, dict) and isinstance(settlement.get("order_count"), int):
            paper_order_count = max(paper_order_count, int(settlement.get("order_count") or 0))
    return {
        "run_id": packet.get("run_id"),
        "status": latest.get("status") if isinstance(latest, dict) else None,
        "started_at": latest.get("started_at") if isinstance(latest, dict) else None,
        "finished_at": latest.get("finished_at") if isinstance(latest, dict) else None,
        "duration_sec": latest.get("duration_sec") if isinstance(latest, dict) else None,
        "selected_lines": latest.get("selected_lines") if isinstance(latest, dict) else None,
        "skipped_lines": latest.get("skipped_lines") if isinstance(latest, dict) else None,
        "line_progress": {
            line: {
                "stage": (line_progress.get(line) or {}).get("stage") if isinstance(line_progress, dict) else None,
                "output_fresh": (line_progress.get(line) or {}).get("output_fresh") if isinstance(line_progress, dict) else None,
                "trade_plan_allowed": (line_progress.get(line) or {}).get("trade_plan_allowed") if isinstance(line_progress, dict) else None,
                "effective_executable_count": (line_progress.get(line) or {}).get("effective_executable_count") if isinstance(line_progress, dict) else None,
                "line_exec_status": (line_progress.get(line) or {}).get("line_exec_status") if isinstance(line_progress, dict) else None,
                "line_lifecycle_status": (line_progress.get(line) or {}).get("line_lifecycle_status") if isinstance(line_progress, dict) else None,
            }
            for line in ["without_micro", "micro_fast", "micro_full"]
        },
        "trade_plan": {
            "without_micro": without_summary,
            "micro_fast": micro_summary,
        },
        "paper_order_count": paper_order_count,
    }


def build_findings(run_summaries: list[dict[str, Any]], packets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for summary, packet in zip(run_summaries, packets):
        run_id = summary.get("run_id")
        if summary.get("status") not in {"ok", "completed", "warning"}:
            findings.append({"severity": "P0", "run_id": run_id, "code": "pipeline_not_ok", "detail": summary.get("status")})
        if summary.get("skipped_lines") != ["micro_full"]:
            findings.append({"severity": "P0", "run_id": run_id, "code": "micro_full_not_skipped", "detail": summary.get("skipped_lines")})
        for line in LINES:
            line_state = (summary.get("line_progress") or {}).get(line) or {}
            if line_state.get("output_fresh") is False:
                findings.append({"severity": "P0", "run_id": run_id, "code": f"{line}_output_not_fresh", "detail": line_state})
        for line, tp in (summary.get("trade_plan") or {}).items():
            if tp.get("bad_step1063"):
                findings.append({"severity": "P0", "run_id": run_id, "code": f"{line}_bad_step1063_executable", "detail": tp})
        latest = packet.get("latest_report_file")
        if isinstance(latest, dict):
            settlement = latest.get("paper_settlement")
            if isinstance(settlement, dict) and settlement.get("missing_count", 0):
                findings.append({"severity": "P0", "run_id": run_id, "code": "paper_missing_executable", "detail": settlement})
    return findings


def write_markdown(report_path: Path, payload: dict[str, Any]) -> None:
    run_summaries = payload.get("run_summaries") or []
    findings = payload.get("findings") or []
    lines = [
        "# STEP7.50 Five-Hour Full-Chain Forensic Audit",
        "",
        f"- started_at: `{payload.get('started_at')}`",
        f"- ended_at: `{payload.get('ended_at')}`",
        f"- duration_sec: `{payload.get('duration_sec')}`",
        f"- selected_lines: `{', '.join(LINES)}`",
        f"- completed_runs: `{len(run_summaries)}`",
        f"- findings: `P0={sum(1 for f in findings if f.get('severity') == 'P0')}`, `P1={sum(1 for f in findings if f.get('severity') == 'P1')}`, `P2={sum(1 for f in findings if f.get('severity') == 'P2')}`",
        "",
        "## Run Index",
        "",
        "| run_id | status | duration | without plans/exe | micro_fast plans/exe | paper orders | micro_fast state |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for s in run_summaries:
        without_tp = (s.get("trade_plan") or {}).get("without_micro") or {}
        micro_tp = (s.get("trade_plan") or {}).get("micro_fast") or {}
        mf = ((s.get("line_progress") or {}).get("micro_fast") or {})
        lines.append(
            "| {run_id} | {status} | {duration} | {wp}/{we} | {mp}/{me} | {po} | {state} |".format(
                run_id=s.get("run_id"),
                status=s.get("status"),
                duration=s.get("duration_sec"),
                wp=without_tp.get("plan_count"),
                we=without_tp.get("executable_count"),
                mp=micro_tp.get("plan_count"),
                me=micro_tp.get("executable_count"),
                po=s.get("paper_order_count"),
                state=mf.get("line_exec_status") or mf.get("line_lifecycle_status"),
            )
        )
    lines.extend(["", "## Findings", ""])
    if not findings:
        lines.append("No P0/P1/P2 findings generated by the automated audit rules.")
    else:
        for f in findings:
            lines.append(f"- `{f.get('severity')}` `{f.get('code')}` run=`{f.get('run_id')}` detail=`{f.get('detail')}`")
    lines.extend(
        [
            "",
            "## Evidence Files",
            "",
            f"- run packets: `{payload.get('run_packets_path')}`",
            f"- full json: `{payload.get('json_path')}`",
            f"- findings json: `{payload.get('findings_path')}`",
            "",
            "## Notes",
            "",
            "This report is generated by `tools/run_step7_50_forensic_audit.py` and does not change strategy logic.",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--duration-sec", type=int, default=5 * 60 * 60)
    parser.add_argument("--sample-sec", type=int, default=30)
    parser.add_argument("--interval-sec", type=int, default=60)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--no-start", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    start_ts = utc_ts()
    out_dir = root / "docs/reports" / f"STEP7.50_five_hour_forensic_audit_{start_ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "runner.log"

    def log(msg: str) -> None:
        line = f"{iso_now()} {msg}"
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    started_at = iso_now()
    preflight = collect_preflight(root)
    write_json(out_dir / f"STEP7.50_preflight_{start_ts}.json", preflight)
    log(f"preflight written: {out_dir}")

    start_response = None
    if not args.no_start:
        body: dict[str, Any] = {
            "mode": "interval",
            "lines": LINES,
            "interval_sec": args.interval_sec,
        }
        if args.max_cycles is not None:
            body["max_cycles"] = args.max_cycles
        start_response = api_post("/api/pipeline/run", body)
        write_json(out_dir / f"STEP7.50_start_response_{start_ts}.json", start_response)
        log(f"pipeline start response ok={start_response.get('ok')} status={start_response.get('status')}")
        if not start_response.get("ok"):
            return 2

    deadline = time.time() + args.duration_sec
    samples: list[dict[str, Any]] = []
    run_packets: list[dict[str, Any]] = []
    run_summaries: list[dict[str, Any]] = []
    collected_runs: set[str] = set()
    last_seen_run: str | None = None

    try:
        while time.time() < deadline:
            status = api_get("/api/pipeline/status/latest")
            watchdog = api_get("/api/pipeline/watchdog")
            runtime = api_get("/api/runtime/status")
            sample = {
                "sampled_at": iso_now(),
                "pipeline_status": status,
                "watchdog": watchdog,
                "runtime_status": runtime,
            }
            samples.append(sample)
            current_run = run_id_from_status(status)
            current_status = latest_report_status(status)
            if current_run and current_run != last_seen_run:
                log(f"observed run_id={current_run} status={current_status}")
                last_seen_run = current_run
            latest_report = latest_report_from_status(status)
            latest_run = str(latest_report.get("run_id") or "") if latest_report else ""
            latest_started_at = str(latest_report.get("started_at") or "") if latest_report else ""
            latest_status = str(latest_report.get("status") or "") if latest_report else ""
            should_collect_latest = (
                bool(latest_run)
                and latest_run not in collected_runs
                and latest_status in {"ok", "failed", "warning"}
                and latest_started_at >= started_at
            )
            if should_collect_latest:
                log(f"collecting run packet for {latest_run}")
                time.sleep(8)
                packet = collect_run_packet(root, latest_run)
                summary = summarize_run(packet)
                run_packets.append(packet)
                run_summaries.append(summary)
                collected_runs.add(latest_run)
                write_json(out_dir / "STEP7.50_run_packets_live.json", run_packets)
                write_json(out_dir / "STEP7.50_run_index_live.json", run_summaries)
                log(
                    "run {run_id} status={status} without={wp}/{we} micro_fast={mp}/{me} paper={po}".format(
                        run_id=latest_run,
                        status=summary.get("status"),
                        wp=((summary.get("trade_plan") or {}).get("without_micro") or {}).get("plan_count"),
                        we=((summary.get("trade_plan") or {}).get("without_micro") or {}).get("executable_count"),
                        mp=((summary.get("trade_plan") or {}).get("micro_fast") or {}).get("plan_count"),
                        me=((summary.get("trade_plan") or {}).get("micro_fast") or {}).get("executable_count"),
                        po=summary.get("paper_order_count"),
                    )
                )
            time.sleep(max(1, args.sample_sec))
    finally:
        stop_response = api_post("/api/pipeline/stop", {})
        write_json(out_dir / f"STEP7.50_stop_response_{utc_ts()}.json", stop_response)
        log(f"pipeline stop response ok={stop_response.get('ok')} status={stop_response.get('status')}")

    ended_at = iso_now()
    findings = build_findings(run_summaries, run_packets)
    ts = utc_ts()
    run_packets_path = out_dir / f"STEP7.50_per_run_packets_{ts}.json"
    run_index_path = out_dir / f"STEP7.50_run_index_{ts}.json"
    samples_path = out_dir / f"STEP7.50_samples_{ts}.json"
    findings_path = out_dir / f"STEP7.50_findings_{ts}.json"
    full_path = out_dir / f"STEP7.50_full_payload_{ts}.json"
    report_path = out_dir / f"STEP7.50_final_report_{ts}.md"

    write_json(run_packets_path, run_packets)
    write_json(run_index_path, run_summaries)
    write_json(samples_path, samples)
    write_json(findings_path, findings)
    payload = {
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_sec": args.duration_sec,
        "preflight": preflight,
        "start_response": start_response,
        "run_summaries": run_summaries,
        "findings": findings,
        "run_packets_path": str(run_packets_path),
        "json_path": str(full_path),
        "findings_path": str(findings_path),
        "report_path": str(report_path),
    }
    write_json(full_path, payload)
    write_markdown(report_path, payload)
    log(f"final report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
