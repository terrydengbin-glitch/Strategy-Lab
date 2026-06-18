from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.micro.training_ledger import coverage_payload, latest_training_payload
from scripts.step7_83_strategy1254_runner import (
    OBSERVED_LINES,
    PIPELINE_LINES,
    _api,
    _iso_now,
    _run_one,
    _stamp,
    _summarize,
    _write_json,
)


def _micro_training_snapshot(limit: int = 20) -> dict[str, Any]:
    try:
        latest = latest_training_payload(PROJECT_ROOT, symbol_limit=limit)
        coverage = coverage_payload(PROJECT_ROOT)
        metric = latest.get("run_metric_coverage") or latest.get("metric_coverage") or {}
        return {
            "ok": True,
            "schema_version": latest.get("schema_version"),
            "latest_run_id": latest.get("run_id") or latest.get("latest_run_id"),
            "symbol_count": latest.get("symbol_count") or latest.get("symbol_sample_count"),
            "metric_coverage": metric,
            "global_metric_coverage": coverage.get("metric_coverage"),
        }
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def _run_enrichment(limit_runs: int) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "step16_22_micro_technical_reliability_enrich.py"),
        "--limit-runs",
        str(limit_runs),
    ]
    started = _iso_now()
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, text=True, capture_output=True, timeout=180)
    payload: dict[str, Any] = {
        "started_at": started,
        "completed_at": _iso_now(),
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }
    try:
        payload["json"] = json.loads(proc.stdout)
    except Exception:
        payload["json"] = None
    return payload


def _run_step7_88(limit_runs: int) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "step7_88_micro_technical_chain_regression_audit.py"),
        "--limit-runs",
        str(limit_runs),
    ]
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, text=True, capture_output=True, timeout=120)
    out: dict[str, Any] = {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    try:
        out["json"] = json.loads(proc.stdout)
    except Exception:
        out["json"] = None
    return out


def _write_step7_89_report(payload: dict[str, Any], stem: str) -> tuple[Path, Path]:
    reports = PROJECT_ROOT / "docs" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    json_path = reports / f"{stem}.json"
    md_path = reports / f"{stem}.md"
    _write_json(json_path, payload)
    summary = payload.get("summary") or {}
    micro = payload.get("final_micro_training") or {}
    metric = micro.get("global_metric_coverage") or micro.get("metric_coverage") or {}
    lines = [
        "# STEP7.89 Micro Technical Strategy1/2/4/5 100-Run E2E Audit",
        "",
        f"> Generated: {payload.get('generated_at')}",
        f"> Active profile: {payload.get('active_profile')}",
        f"> Runs requested: {payload.get('runs_requested')}",
        f"> Verdict: {payload.get('verdict')}",
        "",
        "## Run Summary",
        "",
        "```text",
    ]
    for key, value in summary.items():
        lines.append(f"{key}: {value}")
    lines.extend(["```", "", "## Micro Technical Summary", ""])
    lines.append(f"- schema_version: `{micro.get('schema_version')}`")
    lines.append(f"- latest_run_id: `{micro.get('latest_run_id')}`")
    lines.append(f"- data_plane_ready_count: `{metric.get('data_plane_ready_count')}`")
    lines.append(f"- training_usable_count: `{metric.get('training_usable_count')}`")
    lines.append(f"- avg_technical_reliability_score: `{metric.get('avg_technical_reliability_score')}`")
    lines.append(f"- alignment_state_counts: `{metric.get('alignment_state_counts')}`")
    lines.append(f"- z_state_counts: `{metric.get('z_state_counts')}`")
    lines.append(f"- technical_status_counts: `{metric.get('technical_status_counts')}`")
    lines.append("")
    lines.extend(["## Per Run", ""])
    lines.append("| # | run_id | status | wm exe | mf exe | s4 exe | s5 exe | paper order/skip | micro usable | runtime |")
    lines.append("|---:|---|---|---:|---:|---:|---:|---:|---:|---|")
    for run in payload.get("runs") or []:
        plans = run.get("trade_plans") or {}
        micro_metric = ((run.get("micro_training") or {}).get("metric_coverage") or {})
        paper = run.get("paper") or {}
        runtime = run.get("runtime") or {}
        lines.append(
            "| {idx} | {run_id} | {status} | {wm} | {mf} | {s4} | {s5} | {po}/{ps} | {tu} | {rt} |".format(
                idx=run.get("index"),
                run_id=run.get("run_id") or "-",
                status=run.get("latest_report_status") or run.get("status"),
                wm=(plans.get("without_micro") or {}).get("executable_count", 0),
                mf=(plans.get("micro_fast") or {}).get("executable_count", 0),
                s4=(plans.get("strategy4") or {}).get("executable_count", 0),
                s5=(plans.get("strategy5") or {}).get("executable_count", 0),
                po=paper.get("paper_orders_count", 0),
                ps=paper.get("paper_skip_ledger_count", 0),
                tu=micro_metric.get("training_usable_count", "-"),
                rt=runtime.get("status") or "-",
            )
        )
    lines.extend(["", "## Enrichment Events", ""])
    for item in payload.get("enrichment_events") or []:
        lines.append(f"- after_run `{item.get('after_run')}` returncode=`{item.get('returncode')}`")
    lines.extend(["", "## STEP7.88 Follow-Up", ""])
    step7_88 = payload.get("step7_88") or {}
    lines.append(f"- returncode: `{step7_88.get('returncode')}`")
    lines.append(f"- result: `{step7_88.get('json')}`")
    lines.extend(["", "## Findings", ""])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    if not payload.get("findings"):
        lines.append("- none")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP7.89 100-run micro technical Strategy1/2/4/5 audit")
    parser.add_argument("--runs", type=int, default=100)
    parser.add_argument("--timeout-sec", type=int, default=1200)
    parser.add_argument("--poll-sec", type=int, default=10)
    parser.add_argument("--enrich-every", type=int, default=10)
    parser.add_argument("--live-path", default="")
    args = parser.parse_args()

    stem = f"STEP7.89_micro_technical_strategy1_2_4_5_100run_e2e_audit_{_stamp()}"
    live_path = Path(args.live_path) if args.live_path else PROJECT_ROOT / "DATA" / "runtime" / f"{stem}_live.json"
    profiles = _api("GET", "/api/config/profiles", timeout=20)
    active = ((profiles.get("data") or {}).get("active_profile") if profiles.get("ok") else None)
    runtime_before = _api("GET", "/api/runtime/status", timeout=30)
    pipeline_before = _api("GET", "/api/pipeline/status/latest", timeout=20)
    micro_before = _micro_training_snapshot()

    runs: list[dict[str, Any]] = []
    enrichment_events: list[dict[str, Any]] = []
    for index in range(1, args.runs + 1):
        run = _run_one(index, timeout_sec=args.timeout_sec, poll_sec=args.poll_sec)
        run["micro_training"] = _micro_training_snapshot(limit=5)
        runs.append(run)
        if args.enrich_every > 0 and (index % args.enrich_every == 0 or run.get("status") != "completed"):
            event = _run_enrichment(max(100, index))
            event["after_run"] = index
            enrichment_events.append(event)
            run["post_enrichment_micro_training"] = _micro_training_snapshot(limit=5)
        live = {
            "schema_version": "STEP7.89_live_v1",
            "generated_at": _iso_now(),
            "active_profile": active,
            "pipeline_lines": list(PIPELINE_LINES),
            "observed_lines": list(OBSERVED_LINES),
            "runs_requested": args.runs,
            "runtime_before": runtime_before,
            "pipeline_before": pipeline_before,
            "micro_training_before": micro_before,
            "summary": _summarize(runs),
            "enrichment_events": enrichment_events,
            "runs": runs,
        }
        _write_json(live_path, live)
        print(
            json.dumps(
                {
                    "event": "step7_89_run_complete",
                    "index": index,
                    "run_id": run.get("run_id"),
                    "status": run.get("status"),
                    "latest_report_status": run.get("latest_report_status"),
                    "micro_training": run.get("micro_training", {}).get("metric_coverage", {}),
                    "summary": live["summary"],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        if run.get("status") != "completed":
            break
        time.sleep(3)

    final_enrichment = _run_enrichment(max(100, len(runs)))
    final_enrichment["after_run"] = len(runs)
    enrichment_events.append(final_enrichment)
    step7_88 = _run_step7_88(limit_runs=100)
    final_micro = _micro_training_snapshot(limit=20)
    summary = _summarize(runs)
    findings: list[str] = []
    if summary.get("completed_runs") != args.runs:
        findings.append("requested_100_runs_not_completed")
    if not final_micro.get("ok"):
        findings.append("micro_training_snapshot_failed")
    metric = final_micro.get("global_metric_coverage") or final_micro.get("metric_coverage") or {}
    if not metric.get("training_usable_count"):
        findings.append("latest_micro_training_has_no_usable_samples")
    if step7_88.get("returncode") not in {0, None}:
        findings.append("step7_88_followup_returned_nonzero")
    if summary.get("runtime_unhealthy_runs"):
        findings.append("runtime_unhealthy_runs_present")
    verdict = "PASS" if not findings else "PASS_WITH_FINDINGS"
    if summary.get("completed_runs") == 0:
        verdict = "FAIL"
    payload = {
        "schema_version": "STEP7.89_report_v1",
        "generated_at": _iso_now(),
        "active_profile": active,
        "pipeline_lines": list(PIPELINE_LINES),
        "observed_lines": list(OBSERVED_LINES),
        "runs_requested": args.runs,
        "runtime_before": runtime_before,
        "pipeline_before": pipeline_before,
        "micro_training_before": micro_before,
        "summary": summary,
        "final_micro_training": final_micro,
        "enrichment_events": enrichment_events,
        "step7_88": step7_88,
        "findings": findings,
        "verdict": verdict,
        "live_path": str(live_path),
        "runs": runs,
    }
    json_path, md_path = _write_step7_89_report(payload, stem)
    _write_json(live_path, payload)
    print(json.dumps({"event": "step7_89_report_written", "json": str(json_path), "md": str(md_path), "verdict": verdict}, ensure_ascii=False), flush=True)
    return 0 if verdict != "FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
