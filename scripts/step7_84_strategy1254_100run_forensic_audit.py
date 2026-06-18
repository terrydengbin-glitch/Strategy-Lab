from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "docs" / "reports"
AUDIT_DB = ROOT / "DATA" / "audit" / "run_audit.db"
PAPER_DB = ROOT / "DATA" / "paper" / "paper_trading.db"
STRATEGY5_DB = ROOT / "DATA" / "strategy5" / "strategy5.db"
EXPECTED_LINES = ("without_micro", "micro_fast", "strategy4", "strategy5")
PIPELINE_LINES = ("without_micro", "micro_fast", "strategy5")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def ro_connect(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def fetch_api(path: str, timeout: float = 5.0) -> dict[str, Any]:
    url = f"http://127.0.0.1:8000{path}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        return {"ok": True, "path": path, "status": getattr(resp, "status", None), "data": json.loads(body)}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "path": path, "status": exc.code, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - audit must capture any runtime/API shape failure.
        return {"ok": False, "path": path, "error": f"{type(exc).__name__}: {exc}"}


def scalar(con: sqlite3.Connection | None, sql: str, params: tuple[Any, ...] = ()) -> int:
    if con is None:
        return 0
    row = con.execute(sql, params).fetchone()
    if row is None:
        return 0
    return int(row[0] or 0)


def rows(con: sqlite3.Connection | None, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    if con is None:
        return []
    return [dict(row) for row in con.execute(sql, params).fetchall()]


def group_counts(con: sqlite3.Connection | None, sql: str, params: tuple[Any, ...] = ()) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows(con, sql, params):
        key = str(row.get("k") or row.get("strategy_line") or row.get("status") or row.get("skip_reason") or "unknown")
        out[key] = int(row.get("n") or 0)
    return out


def chunked(values: list[str], size: int = 250) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def in_query(con: sqlite3.Connection | None, table: str, key: str, values: list[str], extra: str = "") -> list[dict[str, Any]]:
    if con is None or not values:
        return []
    out: list[dict[str, Any]] = []
    for part in chunked(values):
        placeholders = ",".join("?" for _ in part)
        sql = f"select * from {table} where {key} in ({placeholders}) {extra}"
        out.extend(rows(con, sql, tuple(part)))
    return out


def safe_plan_load(summary: dict[str, Any]) -> tuple[bool, dict[str, Any] | None, str | None]:
    raw_path = summary.get("path")
    if not raw_path:
        return False, None, "missing_path"
    path = Path(str(raw_path))
    if not path.exists():
        return False, None, "path_not_found"
    try:
        return True, read_json(path), None
    except Exception as exc:  # noqa: BLE001
        return False, None, f"{type(exc).__name__}:{exc}"


def normalize_plan_items(doc: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(doc, dict):
        return []
    for key in ("plans", "trade_plans", "items", "data"):
        value = doc.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    if isinstance(doc.get("lines"), list):
        return [x for x in doc["lines"] if isinstance(x, dict)]
    return []


def reason_counts_from_items(items: list[dict[str, Any]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for item in items:
        reasons = item.get("reason_codes") or item.get("reasons") or []
        if isinstance(reasons, str):
            reasons = [reasons]
        if isinstance(reasons, list):
            counts.update(str(x) for x in reasons if x)
    return counts


def collect_report_experiment(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    profile = str(payload.get("active_profile") or payload.get("profile") or path.stem)
    runs = payload.get("runs") if isinstance(payload.get("runs"), list) else []
    return {
        "profile": profile,
        "path": str(path),
        "generated_at": payload.get("generated_at"),
        "summary": payload.get("summary") or {},
        "verdict": payload.get("verdict"),
        "runs": runs,
        "run_ids": [str(r.get("run_id")) for r in runs if r.get("run_id")],
    }


def audit_experiment(
    exp: dict[str, Any],
    audit_con: sqlite3.Connection | None,
    paper_con: sqlite3.Connection | None,
    strategy5_con: sqlite3.Connection | None,
) -> dict[str, Any]:
    profile = exp["profile"]
    run_ids = exp["run_ids"]
    profile_out: dict[str, Any] = {
        "profile": profile,
        "report_path": exp["path"],
        "generated_at": exp.get("generated_at"),
        "verdict": exp.get("verdict"),
        "run_count": len(run_ids),
        "status_counts": Counter(),
        "selected_line_counts": Counter(),
        "line_summary": {},
        "per_run_failures": [],
        "paper_by_strategy": {},
        "paper_skip_reasons": {},
        "paper_order_status": {},
        "audit_db": {},
        "strategy5_db": {},
        "strategy4": {},
    }

    line_rollup: dict[str, dict[str, Any]] = {}
    for line in EXPECTED_LINES:
        line_rollup[line] = {
            "missing_report_runs": 0,
            "missing_file_runs": 0,
            "json_load_failures": 0,
            "run_id_mismatch": 0,
            "cycle_id_mismatch": 0,
            "status_counts": Counter(),
            "count_total": 0,
            "executable_total": 0,
            "side_counts": Counter(),
            "reason_counts": Counter(),
            "doc_item_total": 0,
            "doc_executable_total": 0,
        }

    for run in exp["runs"]:
        run_id = str(run.get("run_id") or "")
        cycle_id = str(run.get("cycle_id") or "")
        status = str(run.get("status") or "unknown")
        profile_out["status_counts"][status] += 1
        for selected in run.get("selected_lines") or []:
            profile_out["selected_line_counts"][str(selected)] += 1

        plans = run.get("trade_plans") if isinstance(run.get("trade_plans"), dict) else {}
        for line in EXPECTED_LINES:
            line_entry = plans.get(line)
            roll = line_rollup[line]
            if not isinstance(line_entry, dict):
                roll["missing_report_runs"] += 1
                profile_out["per_run_failures"].append({"run_id": run_id, "line": line, "failure": "missing_report_line"})
                continue

            roll["status_counts"][str(line_entry.get("status") or "unknown")] += 1
            roll["count_total"] += int(line_entry.get("count") or 0)
            roll["executable_total"] += int(line_entry.get("executable_count") or 0)
            if isinstance(line_entry.get("side_counts"), dict):
                roll["side_counts"].update({str(k): int(v or 0) for k, v in line_entry["side_counts"].items()})
            if isinstance(line_entry.get("reason_counts"), dict):
                roll["reason_counts"].update({str(k): int(v or 0) for k, v in line_entry["reason_counts"].items()})

            ok, doc, error = safe_plan_load(line_entry)
            if not ok:
                if error == "path_not_found":
                    roll["missing_file_runs"] += 1
                else:
                    roll["json_load_failures"] += 1
                profile_out["per_run_failures"].append({"run_id": run_id, "line": line, "failure": error})
                continue
            if isinstance(doc, dict):
                doc_run = doc.get("run_id")
                doc_cycle = doc.get("cycle_id")
                if line != "strategy4" and doc_run and str(doc_run) != run_id:
                    roll["run_id_mismatch"] += 1
                    profile_out["per_run_failures"].append(
                        {"run_id": run_id, "line": line, "failure": "doc_run_id_mismatch", "doc_run_id": doc_run}
                    )
                if line != "strategy4" and doc_cycle and str(doc_cycle) != cycle_id:
                    roll["cycle_id_mismatch"] += 1
                items = normalize_plan_items(doc)
                roll["doc_item_total"] += len(items)
                roll["doc_executable_total"] += sum(1 for item in items if item.get("executable") is True)
                if not line_entry.get("reason_counts"):
                    roll["reason_counts"].update(reason_counts_from_items(items))

    for line, roll in line_rollup.items():
        profile_out["line_summary"][line] = {
            **{k: v for k, v in roll.items() if not isinstance(v, Counter)},
            "status_counts": dict(roll["status_counts"]),
            "side_counts": dict(roll["side_counts"].most_common()),
            "top_reason_counts": dict(roll["reason_counts"].most_common(20)),
        }

    if run_ids:
        paper_orders = in_query(paper_con, "paper_orders", "source_run_id", run_ids)
        paper_skips = in_query(paper_con, "paper_skip_ledger", "source_run_id", run_ids)
        paper_intents = in_query(paper_con, "paper_intent_inbox", "source_run_id", run_ids)
        paper_trade_plans = in_query(paper_con, "paper_trade_plans", "source_run_id", run_ids)

        order_by_strategy: Counter[str] = Counter(str(x.get("strategy_line") or "unknown") for x in paper_orders)
        skip_by_strategy: Counter[str] = Counter(str(x.get("strategy_line") or "unknown") for x in paper_skips)
        intent_by_strategy: Counter[str] = Counter(str(x.get("strategy_line") or "unknown") for x in paper_intents)
        trade_plan_by_strategy: Counter[str] = Counter(str(x.get("strategy_line") or "unknown") for x in paper_trade_plans)
        profile_out["paper_by_strategy"] = {
            "orders": dict(order_by_strategy),
            "skips": dict(skip_by_strategy),
            "intents": dict(intent_by_strategy),
            "trade_plans": dict(trade_plan_by_strategy),
            "total_orders": len(paper_orders),
            "total_skips": len(paper_skips),
            "total_intents": len(paper_intents),
            "total_trade_plans": len(paper_trade_plans),
        }
        profile_out["paper_skip_reasons"] = dict(Counter(str(x.get("skip_reason") or "unknown") for x in paper_skips).most_common(20))
        profile_out["paper_order_status"] = dict(Counter(str(x.get("status") or "unknown") for x in paper_orders).most_common())
        profile_out["paper_order_samples"] = [
            {
                "source_run_id": x.get("source_run_id"),
                "strategy_line": x.get("strategy_line"),
                "symbol": x.get("symbol"),
                "side": x.get("side"),
                "status": x.get("status"),
                "exit_reason": x.get("exit_reason"),
                "realized_pnl_usdt": x.get("realized_pnl_usdt"),
            }
            for x in paper_orders[:15]
        ]

        audit_run_rows = in_query(audit_con, "audit_runs", "run_id", run_ids)
        audit_symbol_rows = in_query(audit_con, "audit_symbols", "run_id", run_ids)
        audit_artifact_rows = in_query(audit_con, "audit_artifacts", "run_id", run_ids)
        profile_out["audit_db"] = {
            "audit_runs": len(audit_run_rows),
            "audit_symbols": len(audit_symbol_rows),
            "audit_artifacts": len(audit_artifact_rows),
            "audit_symbols_by_strategy": dict(Counter(str(x.get("strategy_line") or "unknown") for x in audit_symbol_rows)),
            "missing_audit_runs": [rid for rid in run_ids if not any(x.get("run_id") == rid for x in audit_run_rows)],
        }

        strategy5_runs = in_query(strategy5_con, "strategy5_runs", "run_id", run_ids)
        strategy5_evidence = in_query(strategy5_con, "strategy5_evidence", "run_id", run_ids)
        profile_out["strategy5_db"] = {
            "runs": len(strategy5_runs),
            "evidence": len(strategy5_evidence),
            "missing_runs": [rid for rid in run_ids if not any(x.get("run_id") == rid for x in strategy5_runs)],
            "status_counts": dict(Counter(str(x.get("status") or "unknown") for x in strategy5_runs)),
            "label_counts": dict(Counter(str(x.get("label") or "unknown") for x in strategy5_evidence).most_common(20)),
            "executable_evidence": sum(1 for x in strategy5_evidence if int(x.get("executable") or 0) == 1),
        }

        strategy4_states = [run.get("strategy4") or {} for run in exp["runs"]]
        profile_out["strategy4"] = {
            "runtime_ok_runs": sum(1 for x in strategy4_states if x.get("runtime_ok") is True),
            "pool_ok_runs": sum(1 for x in strategy4_states if x.get("pool_ok") is True),
            "attempts_ok_runs": sum(1 for x in strategy4_states if x.get("attempts_ok") is True),
            "last_pool_count": (strategy4_states[-1] or {}).get("pool_count") if strategy4_states else None,
            "last_pool_status_counts": (strategy4_states[-1] or {}).get("pool_status_counts") if strategy4_states else None,
            "runtime_states": dict(Counter(str(x.get("runtime_state") or "unknown") for x in strategy4_states)),
        }

    return profile_out


def add_verdicts(payload: dict[str, Any]) -> None:
    findings: list[dict[str, Any]] = []
    for exp in payload["experiments"]:
        profile = exp["profile"]
        if exp["run_count"] != 50:
            findings.append({"severity": "P1", "profile": profile, "title": "run_count_not_50", "detail": exp["run_count"]})
        if exp["audit_db"].get("missing_audit_runs"):
            findings.append({"severity": "P1", "profile": profile, "title": "audit_runs_missing", "detail": exp["audit_db"]["missing_audit_runs"][:10]})
        for line, summary in exp["line_summary"].items():
            if summary.get("missing_file_runs") or summary.get("json_load_failures"):
                findings.append(
                    {
                        "severity": "P1",
                        "profile": profile,
                        "title": f"{line}_plan_archive_unreadable",
                        "detail": {
                            "missing_file_runs": summary.get("missing_file_runs"),
                            "json_load_failures": summary.get("json_load_failures"),
                        },
                    }
                )
            if line != "strategy4" and summary.get("run_id_mismatch"):
                findings.append(
                    {"severity": "P1", "profile": profile, "title": f"{line}_run_id_mismatch", "detail": summary.get("run_id_mismatch")}
                )
        if exp["strategy5_db"].get("missing_runs"):
            findings.append({"severity": "P1", "profile": profile, "title": "strategy5_sqlite_missing_runs", "detail": exp["strategy5_db"]["missing_runs"][:10]})
        if exp["strategy4"].get("runtime_ok_runs") != exp["run_count"]:
            findings.append(
                {
                    "severity": "P1",
                    "profile": profile,
                    "title": "strategy4_sidecar_runtime_not_ok_for_all_runs",
                    "detail": exp["strategy4"],
                }
            )
        if profile == "relaxed_profit":
            s4_exec = exp["line_summary"]["strategy4"]["executable_total"]
            if s4_exec == 0:
                findings.append(
                    {
                        "severity": "P2",
                        "profile": profile,
                        "title": "strategy4_zero_executable_in_relaxed_50_runs",
                        "detail": "observe sidecar is healthy but did not create executable plans; classify as business outcome unless slot/capacity task expects more.",
                    }
                )
            if exp["paper_by_strategy"].get("total_orders", 0) == 0:
                findings.append({"severity": "P1", "profile": profile, "title": "relaxed_paper_orders_missing", "detail": exp["paper_by_strategy"]})
        if profile == "production_strict" and exp["paper_by_strategy"].get("total_orders", 0) != 0:
            findings.append({"severity": "P1", "profile": profile, "title": "strict_should_not_have_paper_orders_in_report_window", "detail": exp["paper_by_strategy"]})

    api = payload.get("fastapi_current") or {}
    for item in api.get("checks", []):
        if not item.get("ok"):
            findings.append({"severity": "P2", "profile": "current_api", "title": f"api_check_failed:{item.get('path')}", "detail": item})
    payload["findings"] = findings
    payload["verdict"] = "PASS_WITH_FINDINGS" if not any(x["severity"] == "P1" for x in findings) else "PASS_WITH_P1_FINDINGS"


def render_md(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# STEP7.84 Strategy1/2/4/5 100-Run Full-Chain Forensic Audit")
    lines.append("")
    lines.append(f"- generated_at: `{payload['generated_at']}`")
    lines.append(f"- verdict: `{payload['verdict']}`")
    lines.append("- mode: read-only forensic audit; no new run/cycle; no strategy/config mutation")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    for exp in payload["experiments"]:
        lines.append(
            f"- `{exp['profile']}`: runs={exp['run_count']}, paper_orders={exp['paper_by_strategy'].get('total_orders', 0)}, "
            f"paper_skips={exp['paper_by_strategy'].get('total_skips', 0)}, strategy4_runtime_ok={exp['strategy4'].get('runtime_ok_runs')}/{exp['run_count']}, "
            f"strategy5_sqlite_runs={exp['strategy5_db'].get('runs', 0)}/{exp['run_count']}"
        )
    lines.append("")
    lines.append("## Strategy Line Results")
    lines.append("")
    for exp in payload["experiments"]:
        lines.append(f"### {exp['profile']}")
        lines.append("")
        lines.append("| line | status_counts | plans | executable | doc_items | doc_exec | top reasons |")
        lines.append("| --- | --- | ---: | ---: | ---: | ---: | --- |")
        for line in EXPECTED_LINES:
            s = exp["line_summary"][line]
            top = ", ".join(f"{k}:{v}" for k, v in list(s["top_reason_counts"].items())[:6])
            lines.append(
                f"| `{line}` | `{json.dumps(s['status_counts'], ensure_ascii=False)}` | {s['count_total']} | "
                f"{s['executable_total']} | {s['doc_item_total']} | {s['doc_executable_total']} | {top} |"
            )
        lines.append("")
    lines.append("## Paper Trace")
    lines.append("")
    for exp in payload["experiments"]:
        lines.append(f"### {exp['profile']}")
        lines.append("")
        lines.append(f"- by_strategy: `{json.dumps(exp['paper_by_strategy'], ensure_ascii=False)}`")
        lines.append(f"- skip_reasons: `{json.dumps(exp['paper_skip_reasons'], ensure_ascii=False)}`")
        lines.append(f"- order_status: `{json.dumps(exp['paper_order_status'], ensure_ascii=False)}`")
        if exp.get("paper_order_samples"):
            lines.append("- order_samples:")
            for sample in exp["paper_order_samples"][:8]:
                lines.append(
                    f"  - `{sample.get('strategy_line')}` {sample.get('symbol')} {sample.get('side')} "
                    f"status={sample.get('status')} exit={sample.get('exit_reason')} run={sample.get('source_run_id')}"
                )
        lines.append("")
    lines.append("## SQLite / Sidecar Coverage")
    lines.append("")
    for exp in payload["experiments"]:
        lines.append(f"### {exp['profile']}")
        lines.append("")
        lines.append(f"- audit_db: `{json.dumps(exp['audit_db'], ensure_ascii=False)}`")
        lines.append(f"- strategy5_db: `{json.dumps(exp['strategy5_db'], ensure_ascii=False)}`")
        lines.append(f"- strategy4: `{json.dumps(exp['strategy4'], ensure_ascii=False)}`")
        lines.append("")
    lines.append("## FastAPI Current State")
    lines.append("")
    for check in payload["fastapi_current"]["checks"]:
        lines.append(f"- `{check['path']}` ok={check.get('ok')} status={check.get('status')} error={check.get('error')}")
    lines.append("")
    lines.append("## Findings")
    lines.append("")
    if not payload["findings"]:
        lines.append("- No P1/P2 findings.")
    else:
        for f in payload["findings"]:
            detail = f.get("detail")
            if not isinstance(detail, str):
                detail = json.dumps(detail, ensure_ascii=False)
            lines.append(f"- **{f['severity']}** `{f.get('profile')}` {f['title']}: {detail}")
    lines.append("")
    lines.append("## Business Verdict")
    lines.append("")
    lines.append("- Strategy1/2/5 per-run JSON archive coverage is checked against the STEP7.83 reports and SQLite ledgers.")
    lines.append("- Strategy4 is treated as sidecar evidence, not as a selected pipeline slot; zero executable is reported separately from runtime health.")
    lines.append("- Paper orders/skips are traced by `source_run_id` and `strategy_line`; current paper state after later runs is not mixed into the archived 100-run verdict.")
    lines.append("- FastAPI checks are current-state smoke checks only; archived JSON/SQLite remain the primary evidence for the 100 runs.")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict-report", type=Path, required=True)
    parser.add_argument("--relaxed-report", type=Path, required=True)
    args = parser.parse_args(argv)

    audit_con = ro_connect(AUDIT_DB)
    paper_con = ro_connect(PAPER_DB)
    strategy5_con = ro_connect(STRATEGY5_DB)
    stamp = utc_stamp()
    experiments = [
        collect_report_experiment((ROOT / args.strict_report).resolve() if not args.strict_report.is_absolute() else args.strict_report),
        collect_report_experiment((ROOT / args.relaxed_report).resolve() if not args.relaxed_report.is_absolute() else args.relaxed_report),
    ]
    audited = [audit_experiment(exp, audit_con, paper_con, strategy5_con) for exp in experiments]
    api_checks = [
        fetch_api("/api/health"),
        fetch_api("/api/runtime/status"),
        fetch_api("/api/pipeline/status/latest"),
        fetch_api("/api/config/profiles"),
        fetch_api("/api/strategy4/runtime"),
    ]
    payload: dict[str, Any] = {
        "schema_version": "STEP7.84.v1",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "root": str(ROOT),
        "evidence": {
            "strict_report": str(args.strict_report),
            "relaxed_report": str(args.relaxed_report),
            "audit_db": str(AUDIT_DB),
            "paper_db": str(PAPER_DB),
            "strategy5_db": str(STRATEGY5_DB),
        },
        "experiments": audited,
        "fastapi_current": {"checks": api_checks},
    }
    add_verdicts(payload)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORTS_DIR / f"STEP7.84_strategy1_2_4_5_100run_full_chain_forensic_audit_{stamp}.json"
    md_path = REPORTS_DIR / f"STEP7.84_strategy1_2_4_5_100run_full_chain_forensic_audit_{stamp}.md"
    write_json(json_path, payload)
    md_path.write_text(render_md(payload), encoding="utf-8")
    print(json.dumps({"ok": True, "json": str(json_path), "md": str(md_path), "verdict": payload["verdict"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
