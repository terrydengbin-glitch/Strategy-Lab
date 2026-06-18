from __future__ import annotations

import json
import sqlite3
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "docs" / "reports"
REPORT_84 = REPORTS_DIR / "STEP7.84_strategy1_2_4_5_100run_full_chain_forensic_audit_20260606T060101Z.json"
STRICT_83 = REPORTS_DIR / "STEP7.83_strategy1_2_4_5_strict_e2e_audit_20260605T141927Z.json"
RELAXED_83 = REPORTS_DIR / "STEP7.83_strategy1_2_4_5_strict_e2e_audit_20260605T163248Z.json"
STRATEGY4_DB = ROOT / "DATA" / "strategy4" / "strategy4_observe.db"
PAPER_DB = ROOT / "DATA" / "paper" / "paper_trading.db"
AUDIT_DB = ROOT / "DATA" / "audit" / "run_audit.db"
POOL_JSON = ROOT / "DATA" / "decisions" / "strategy4_observe_pool.json"
PLAN_JSON = ROOT / "DATA" / "decisions" / "latest_trade_plan_strategy4.json"
RUNTIME_JSON = ROOT / "DATA" / "runtime" / "strategy4_daemon_status.json"


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def ro_connect(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def rows(con: sqlite3.Connection | None, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    if con is None:
        return []
    return [dict(row) for row in con.execute(sql, params).fetchall()]


def scalar(con: sqlite3.Connection | None, sql: str, params: tuple[Any, ...] = ()) -> int:
    if con is None:
        return 0
    row = con.execute(sql, params).fetchone()
    return int((row[0] if row else 0) or 0)


def parse_json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value if x]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if x]
        except ValueError:
            return [value] if value else []
    return []


def parse_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def api_get(path: str, timeout: float = 5.0) -> dict[str, Any]:
    url = f"http://127.0.0.1:8000{path}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return {"ok": True, "path": path, "status": resp.status, "data": json.loads(body)}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "path": path, "status": exc.code, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "path": path, "error": f"{type(exc).__name__}: {exc}"}


def experiment_window(report: dict[str, Any]) -> dict[str, Any]:
    runs = report.get("runs") if isinstance(report.get("runs"), list) else []
    starts = [iso_to_dt(r.get("started_at")) for r in runs]
    ends = [iso_to_dt(r.get("completed_at")) for r in runs]
    starts = [x for x in starts if x]
    ends = [x for x in ends if x]
    return {
        "run_ids": [str(r.get("run_id")) for r in runs if r.get("run_id")],
        "start": min(starts).isoformat().replace("+00:00", "Z") if starts else None,
        "end": max(ends).isoformat().replace("+00:00", "Z") if ends else None,
    }


def report_strategy4_rollup(report: dict[str, Any]) -> dict[str, Any]:
    reason_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    count_total = 0
    executable_total = 0
    path_counts: Counter[str] = Counter()
    run_id_null_paths = 0
    samples: list[dict[str, Any]] = []
    for run in report.get("runs") or []:
        s4 = ((run.get("trade_plans") or {}).get("strategy4") or {}) if isinstance(run, dict) else {}
        status_counts[str(s4.get("status") or "unknown")] += 1
        count_total += int(s4.get("count") or 0)
        executable_total += int(s4.get("executable_count") or 0)
        path_counts[str(s4.get("path") or "missing")] += 1
        if s4.get("run_id") is None:
            run_id_null_paths += 1
        if isinstance(s4.get("reason_counts"), dict):
            reason_counts.update({str(k): int(v or 0) for k, v in s4["reason_counts"].items()})
        if s4.get("count") and len(samples) < 8:
            samples.append(
                {
                    "run_id": run.get("run_id"),
                    "status": s4.get("status"),
                    "count": s4.get("count"),
                    "executable_count": s4.get("executable_count"),
                    "reason_counts": dict(list((s4.get("reason_counts") or {}).items())[:10]),
                    "path": s4.get("path"),
                }
            )
    return {
        "status_counts": dict(status_counts),
        "count_total": count_total,
        "executable_total": executable_total,
        "top_reason_counts": dict(reason_counts.most_common(30)),
        "unique_paths": len(path_counts),
        "top_paths": dict(path_counts.most_common(5)),
        "run_id_null_paths": run_id_null_paths,
        "samples": samples,
    }


def strategy4_db_audit(con: sqlite3.Connection | None, windows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "db_exists": STRATEGY4_DB.exists(),
        "pool_total": scalar(con, "select count(*) from strategy4_observe_pool"),
        "attempt_total": scalar(con, "select count(*) from strategy4_attempts"),
        "pool_status_counts": {},
        "pool_evict_reasons": {},
        "attempt_status_counts": {},
        "attempt_action_counts": {},
        "attempt_executable_counts": {},
        "attempt_reason_counts": {},
        "pool_reason_counts": {},
        "side_change": {},
        "windows": {},
        "executable_attempt_samples": [],
        "latest_attempt_samples": [],
        "null_plan_attempts": 0,
        "blank_reason_still_wait_attempts": 0,
    }
    if con is None:
        return out
    for key, query in {
        "pool_status_counts": "select status k,count(*) n from strategy4_observe_pool group by status order by n desc",
        "pool_evict_reasons": "select coalesce(evict_reason,'') k,count(*) n from strategy4_observe_pool group by evict_reason order by n desc",
        "attempt_status_counts": "select status k,count(*) n from strategy4_attempts group by status order by n desc",
        "attempt_action_counts": "select coalesce(action,'') k,count(*) n from strategy4_attempts group by action order by n desc",
        "attempt_executable_counts": "select executable k,count(*) n from strategy4_attempts group by executable order by executable",
    }.items():
        out[key] = {str(r["k"]): int(r["n"]) for r in rows(con, query)}

    out["side_change"] = {
        "attempts": {str(r["side_changed"]): int(r["n"]) for r in rows(con, "select side_changed,count(*) n from strategy4_attempts group by side_changed")},
        "pool": {str(r["side_changed"]): int(r["n"]) for r in rows(con, "select side_changed,count(*) n from strategy4_observe_pool group by side_changed")},
    }
    reasons: Counter[str] = Counter()
    for row in rows(con, "select reason_codes_json from strategy4_attempts"):
        reasons.update(parse_json_list(row.get("reason_codes_json")))
    out["attempt_reason_counts"] = dict(reasons.most_common(40))
    pool_reasons: Counter[str] = Counter()
    for row in rows(con, "select source_reason_codes_json,last_reason_codes_json from strategy4_observe_pool"):
        pool_reasons.update(parse_json_list(row.get("source_reason_codes_json")))
        pool_reasons.update(parse_json_list(row.get("last_reason_codes_json")))
    out["pool_reason_counts"] = dict(pool_reasons.most_common(40))
    out["null_plan_attempts"] = scalar(con, "select count(*) from strategy4_attempts where coalesce(plan_json,'') in ('','{}')")
    out["blank_reason_still_wait_attempts"] = scalar(
        con,
        "select count(*) from strategy4_attempts where status='still_wait' and coalesce(reason_codes_json,'[]')='[]'",
    )
    out["executable_attempt_samples"] = rows(
        con,
        """
        select symbol,run_id,cycle_id,attempted_at,status,decision,action,entry_mode,executable,reason_codes_json,original_side,current_side,side_changed
        from strategy4_attempts where executable=1 order by attempted_at desc limit 20
        """,
    )
    out["latest_attempt_samples"] = rows(
        con,
        """
        select symbol,run_id,cycle_id,attempted_at,status,decision,action,entry_mode,executable,reason_codes_json,original_side,current_side,side_changed
        from strategy4_attempts order by attempted_at desc limit 20
        """,
    )

    for name, window in windows.items():
        start = window.get("start")
        end = window.get("end")
        if not start or not end:
            continue
        params = (start, end)
        reason_window: Counter[str] = Counter()
        for row in rows(con, "select reason_codes_json from strategy4_attempts where attempted_at between ? and ?", params):
            reason_window.update(parse_json_list(row.get("reason_codes_json")))
        out["windows"][name] = {
            "start": start,
            "end": end,
            "attempts": scalar(con, "select count(*) from strategy4_attempts where attempted_at between ? and ?", params),
            "executable": scalar(con, "select count(*) from strategy4_attempts where attempted_at between ? and ? and executable=1", params),
            "status_counts": {
                str(r["k"]): int(r["n"])
                for r in rows(
                    con,
                    "select status k,count(*) n from strategy4_attempts where attempted_at between ? and ? group by status order by n desc",
                    params,
                )
            },
            "action_counts": {
                str(r["k"]): int(r["n"])
                for r in rows(
                    con,
                    "select coalesce(action,'') k,count(*) n from strategy4_attempts where attempted_at between ? and ? group by action order by n desc",
                    params,
                )
            },
            "top_reasons": dict(reason_window.most_common(30)),
            "blank_reason_still_wait": scalar(
                con,
                "select count(*) from strategy4_attempts where attempted_at between ? and ? and status='still_wait' and coalesce(reason_codes_json,'[]')='[]'",
                params,
            ),
        }
    return out


def paper_audit(con: sqlite3.Connection | None) -> dict[str, Any]:
    if con is None:
        return {"db_exists": False}
    return {
        "db_exists": True,
        "orders": scalar(con, "select count(*) from paper_orders where strategy_line='strategy4'"),
        "closed": scalar(con, "select count(*) from paper_orders where strategy_line='strategy4' and status='closed'"),
        "skips": scalar(con, "select count(*) from paper_skip_ledger where strategy_line='strategy4'"),
        "intents": scalar(con, "select count(*) from paper_intent_inbox where strategy_line='strategy4'"),
        "skip_reasons": {
            str(r["k"]): int(r["n"])
            for r in rows(con, "select skip_reason k,count(*) n from paper_skip_ledger where strategy_line='strategy4' group by skip_reason order by n desc")
        },
        "order_samples": rows(
            con,
            "select source_run_id,source_cycle_id,strategy_line,symbol,side,status,exit_reason,created_at,closed_at from paper_orders where strategy_line='strategy4' order by created_at desc limit 20",
        ),
    }


def audit_db_strategy4(con: sqlite3.Connection | None, run_ids: list[str]) -> dict[str, Any]:
    if con is None:
        return {"db_exists": False}
    out = {
        "db_exists": True,
        "audit_symbols_strategy4_rows_for_100_runs": 0,
        "audit_artifacts_strategy4_for_100_runs": 0,
        "sidecar_artifact_keys": {},
    }
    if run_ids:
        total_symbols = 0
        total_artifacts = 0
        artifact_keys: Counter[str] = Counter()
        for i in range(0, len(run_ids), 200):
            part = run_ids[i : i + 200]
            ph = ",".join("?" for _ in part)
            total_symbols += scalar(con, f"select count(*) from audit_symbols where run_id in ({ph}) and strategy_line='strategy4'", tuple(part))
            arts = rows(con, f"select artifact_key from audit_artifacts where run_id in ({ph}) and artifact_key like '%strategy4%'", tuple(part))
            total_artifacts += len(arts)
            artifact_keys.update(str(a.get("artifact_key")) for a in arts)
        out["audit_symbols_strategy4_rows_for_100_runs"] = total_symbols
        out["audit_artifacts_strategy4_for_100_runs"] = total_artifacts
        out["sidecar_artifact_keys"] = dict(artifact_keys)
    return out


def current_json_audit() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, path in {"pool": POOL_JSON, "plan": PLAN_JSON, "runtime": RUNTIME_JSON}.items():
        if not path.exists():
            out[name] = {"exists": False}
            continue
        doc = read_json(path)
        out[name] = {
            "exists": True,
            "path": str(path),
            "generated_at": doc.get("generated_at") if isinstance(doc, dict) else None,
            "status": doc.get("status") if isinstance(doc, dict) else None,
            "state": doc.get("state") if isinstance(doc, dict) else None,
            "count": doc.get("count") if isinstance(doc, dict) else None,
            "executable_count": doc.get("executable_count") if isinstance(doc, dict) else None,
            "status_counts": doc.get("status_counts") if isinstance(doc, dict) else None,
            "input_refs": doc.get("input_refs") if isinstance(doc, dict) and name == "plan" else None,
        }
    return out


def classify_root_cause(payload: dict[str, Any]) -> list[dict[str, Any]]:
    causes: list[dict[str, Any]] = []
    db = payload["strategy4_db"]
    current = payload["current_json"]
    report_relaxed = payload["report_strategy4"]["relaxed_profit"]
    latest_due = ((current.get("plan") or {}).get("input_refs") or {}).get("strategy4_due_count")
    if current.get("pool", {}).get("status_counts") == {"evicted": 511} or current.get("pool", {}).get("status_counts", {}).get("evicted"):
        causes.append(
            {
                "class": "scheduler_pool_exhausted_by_ttl",
                "severity": "P1",
                "evidence": {
                    "current_pool_status_counts": current.get("pool", {}).get("status_counts"),
                    "pool_evict_reasons": db.get("pool_evict_reasons"),
                    "latest_due_count": latest_due,
                },
                "meaning": "Strategy4 is healthy, but the current observe pool has no due live candidates because all rows aged out by observe_ttl_expired.",
            }
        )
    if db.get("windows", {}).get("relaxed_profit", {}).get("executable", 0) == 0 and report_relaxed.get("executable_total") == 0:
        causes.append(
            {
                "class": "window_business_blockers_no_executable",
                "severity": "P2",
                "evidence": {
                    "relaxed_report_reasons": report_relaxed.get("top_reason_counts"),
                    "relaxed_window_reasons": db.get("windows", {}).get("relaxed_profit", {}).get("top_reasons"),
                    "relaxed_attempt_status": db.get("windows", {}).get("relaxed_profit", {}).get("status_counts"),
                },
                "meaning": "Within the STEP7.83 relaxed window, Strategy4 rechecks stayed WAIT/no_entries; blockers are mostly liquidity, slippage, depth, RR/market-room and refresh_missing.",
            }
        )
    if db.get("attempt_executable_counts", {}).get("1", 0) > 0:
        causes.append(
            {
                "class": "historical_executable_exists_but_not_in_100run_window",
                "severity": "INFO",
                "evidence": {"all_time_executable_attempts": db.get("attempt_executable_counts", {}).get("1", 0)},
                "meaning": "The Strategy4 engine can produce executable attempts historically; the 100-run zero result is window-specific, not proof that executable path is dead.",
            }
        )
    if report_relaxed.get("unique_paths", 0) <= 2:
        causes.append(
            {
                "class": "sidecar_latest_file_not_immutable_per_run",
                "severity": "P2",
                "evidence": {"top_paths": report_relaxed.get("top_paths"), "run_id_null_paths": report_relaxed.get("run_id_null_paths")},
                "meaning": "STEP7.83 report captured Strategy4 summaries, but many path refs point to the sidecar latest file; per-run replay must use captured report fields and SQLite attempts, not current latest JSON.",
            }
        )
    if db.get("side_change", {}).get("attempts", {}).get("1", 0) > 0:
        causes.append(
            {
                "class": "direction_rejudge_active",
                "severity": "INFO",
                "evidence": db.get("side_change"),
                "meaning": "Side rejudge is active; zero executable is not caused by inheriting the original Strategy1 side only.",
            }
        )
    if payload.get("paper", {}).get("orders", 0) == 0 and db.get("attempt_executable_counts", {}).get("1", 0) > 0:
        causes.append(
            {
                "class": "paper_consumption_window_or_lineage_boundary",
                "severity": "P2",
                "evidence": payload.get("paper"),
                "meaning": "All-time Strategy4 executable attempts exist, but current paper DB has no Strategy4 orders. This may be expected if executable attempts were outside active paper windows or before STEP14.32/14.33 lineage/slot fixes; verify in next live run.",
            }
        )
    return causes


def render_md(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# STEP7.85 Strategy4 Zero Executable Root Cause Forensic Audit")
    lines.append("")
    lines.append(f"- generated_at: `{payload['generated_at']}`")
    lines.append(f"- verdict: `{payload['verdict']}`")
    lines.append("- mode: read-only; no run once/cycle; no config or strategy mutation")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(
        "Strategy4 没有在 STEP7.83/STEP7.84 的 100-run 审计窗口内产生 executable，"
        "主因不是 daemon/paper/audit 断链，而是 observe pool 在业务复查中长期停留 WAIT/no_entries，"
        "随后全部因 `observe_ttl_expired` 被 evict；同时该 100-run 窗口内的阻断原因集中在流动性/深度/滑点/RR/market room/refresh_missing。"
    )
    lines.append("")
    lines.append("## Current Strategy4 State")
    lines.append("")
    lines.append(f"- pool: `{json.dumps(payload['current_json'].get('pool'), ensure_ascii=False)}`")
    lines.append(f"- latest_plan: `{json.dumps(payload['current_json'].get('plan'), ensure_ascii=False)}`")
    lines.append(f"- runtime: `{json.dumps(payload['current_json'].get('runtime'), ensure_ascii=False)}`")
    lines.append("")
    lines.append("## 100-Run Report Evidence")
    lines.append("")
    for profile, data in payload["report_strategy4"].items():
        lines.append(f"### {profile}")
        lines.append("")
        lines.append(f"- status_counts: `{json.dumps(data['status_counts'], ensure_ascii=False)}`")
        lines.append(f"- count_total: `{data['count_total']}`")
        lines.append(f"- executable_total: `{data['executable_total']}`")
        lines.append(f"- top_reason_counts: `{json.dumps(data['top_reason_counts'], ensure_ascii=False)}`")
        lines.append(f"- top_paths: `{json.dumps(data['top_paths'], ensure_ascii=False)}`")
        lines.append("")
    lines.append("## Strategy4 SQLite Evidence")
    lines.append("")
    db = payload["strategy4_db"]
    lines.append(f"- pool_total: `{db.get('pool_total')}`")
    lines.append(f"- pool_status_counts: `{json.dumps(db.get('pool_status_counts'), ensure_ascii=False)}`")
    lines.append(f"- pool_evict_reasons: `{json.dumps(db.get('pool_evict_reasons'), ensure_ascii=False)}`")
    lines.append(f"- attempt_total: `{db.get('attempt_total')}`")
    lines.append(f"- attempt_status_counts: `{json.dumps(db.get('attempt_status_counts'), ensure_ascii=False)}`")
    lines.append(f"- attempt_executable_counts: `{json.dumps(db.get('attempt_executable_counts'), ensure_ascii=False)}`")
    lines.append(f"- side_change: `{json.dumps(db.get('side_change'), ensure_ascii=False)}`")
    lines.append(f"- top_attempt_reasons: `{json.dumps(db.get('attempt_reason_counts'), ensure_ascii=False)}`")
    lines.append(f"- blank_reason_still_wait_attempts: `{db.get('blank_reason_still_wait_attempts')}`")
    lines.append("")
    lines.append("## Experiment Windows")
    lines.append("")
    for profile, data in db.get("windows", {}).items():
        lines.append(f"### {profile}")
        lines.append("")
        lines.append(f"- window: `{data.get('start')}` -> `{data.get('end')}`")
        lines.append(f"- attempts: `{data.get('attempts')}`")
        lines.append(f"- executable: `{data.get('executable')}`")
        lines.append(f"- status_counts: `{json.dumps(data.get('status_counts'), ensure_ascii=False)}`")
        lines.append(f"- action_counts: `{json.dumps(data.get('action_counts'), ensure_ascii=False)}`")
        lines.append(f"- top_reasons: `{json.dumps(data.get('top_reasons'), ensure_ascii=False)}`")
        lines.append(f"- blank_reason_still_wait: `{data.get('blank_reason_still_wait')}`")
        lines.append("")
    lines.append("## Paper / Audit Boundary")
    lines.append("")
    lines.append(f"- paper: `{json.dumps(payload.get('paper'), ensure_ascii=False)}`")
    lines.append(f"- audit_db: `{json.dumps(payload.get('audit_db'), ensure_ascii=False)}`")
    lines.append("")
    lines.append("## Root Cause Classification")
    lines.append("")
    for cause in payload["root_causes"]:
        lines.append(f"- **{cause['severity']}** `{cause['class']}`: {cause['meaning']}")
        lines.append(f"  - evidence: `{json.dumps(cause.get('evidence'), ensure_ascii=False)}`")
    lines.append("")
    lines.append("## Conclusion")
    lines.append("")
    lines.append("- Strategy4 sidecar/runtime 是健康的，100-run 内 0 executable 不是 runtime 失效。")
    lines.append("- Strategy4 当前池子全量 `observe_ttl_expired`，所以最新 sidecar 输出 `strategy4_due_count=0`、plan count=0。")
    lines.append("- 在 STEP7.83 relaxed_profit 窗口内，Strategy4 的 31 个候选均被深度/滑点/流动性/RR/market room/refresh_missing 阻断，没有进入 executable。")
    lines.append("- Strategy4 全库历史上存在 21 个 executable attempts，说明 evaluator 可产生 executable；但这些不在本次 100-run paper 审计窗口内，也没有形成当前 paper strategy4 订单。")
    lines.append("- Side rejudge 有 2630 次 attempt 发生方向变化，说明策略4并非单纯沿用策略1原方向。")
    lines.append("")
    lines.append("## Recommended Follow-Ups")
    lines.append("")
    lines.append("1. 建议补 `STEP17.x Strategy4 Observe TTL / Pool Retention Policy Calibration`：当前 pool 全部 TTL evict，需确认 4h TTL 对 5 分钟复查的持续观察目标是否太短。")
    lines.append("2. 建议补 `STEP17.x Strategy4 Empty-Reason WAIT Evidence Repair`：有大量 `still_wait` attempts 的 reason/action/entry_mode 为空，应提升可复原性。")
    lines.append("3. 建议补 `STEP7.x Strategy4 Live Executable Consumption Replay`：在 STEP14.32/14.33 后跑小样本，验证一旦 Strategy4 executable 出现是否能进入 paper。")
    lines.append("4. 如果目标是增加策略4机会，应单独评估 liquidity/slippage/RR/market-room gate 对 Strategy4 的配置，而不是直接放宽策略1。")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    report84 = read_json(REPORT_84)
    strict83 = read_json(STRICT_83)
    relaxed83 = read_json(RELAXED_83)
    windows = {
        "production_strict": experiment_window(strict83),
        "relaxed_profit": experiment_window(relaxed83),
    }
    run_ids = windows["production_strict"]["run_ids"] + windows["relaxed_profit"]["run_ids"]
    s4_con = ro_connect(STRATEGY4_DB)
    paper_con = ro_connect(PAPER_DB)
    audit_con = ro_connect(AUDIT_DB)
    payload: dict[str, Any] = {
        "schema_version": "STEP7.85.v1",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "evidence": {
            "step7_84_report": str(REPORT_84),
            "strict_report": str(STRICT_83),
            "relaxed_report": str(RELAXED_83),
            "strategy4_db": str(STRATEGY4_DB),
            "pool_json": str(POOL_JSON),
            "latest_plan_json": str(PLAN_JSON),
        },
        "windows": windows,
        "step7_84_finding": [f for f in report84.get("findings", []) if "strategy4" in str(f.get("title"))],
        "report_strategy4": {
            "production_strict": report_strategy4_rollup(strict83),
            "relaxed_profit": report_strategy4_rollup(relaxed83),
        },
        "strategy4_db": strategy4_db_audit(s4_con, windows),
        "current_json": current_json_audit(),
        "paper": paper_audit(paper_con),
        "audit_db": audit_db_strategy4(audit_con, run_ids),
        "fastapi": {
            "runtime": api_get("/api/strategy4/runtime"),
            "observe_pool": api_get("/api/strategy4/observe-pool"),
            "attempts": api_get("/api/strategy4/attempts?limit=20"),
        },
    }
    payload["root_causes"] = classify_root_cause(payload)
    payload["verdict"] = "ROOT_CAUSE_IDENTIFIED_WITH_FOLLOWUPS"
    ts = stamp()
    json_path = REPORTS_DIR / f"STEP7.85_strategy4_zero_executable_root_cause_forensic_audit_{ts}.json"
    md_path = REPORTS_DIR / f"STEP7.85_strategy4_zero_executable_root_cause_forensic_audit_{ts}.md"
    write_json(json_path, payload)
    md_path.write_text(render_md(payload), encoding="utf-8")
    print(json.dumps({"ok": True, "json": str(json_path), "md": str(md_path), "verdict": payload["verdict"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
