from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now


STRATEGY_LINES = ("without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6")
PIPELINE_LINES = ("without_micro", "micro_fast", "micro_full", "strategy5", "strategy6")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = read_json_object(path)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _items_count(doc: dict[str, Any], *keys: str) -> int:
    for key in keys:
        value = doc.get(key)
        if isinstance(value, list):
            return len(value)
    try:
        return int(doc.get("count") or 0)
    except Exception:
        return 0


def _reason_codes(*values: Any) -> list[str]:
    out: list[str] = []
    for value in values:
        if isinstance(value, str) and value:
            out.append(value)
        elif isinstance(value, list):
            out.extend(str(x) for x in value if x)
        elif isinstance(value, dict):
            out.extend(str(x) for x in value.get("reason_codes", []) if x)
    seen: set[str] = set()
    deduped: list[str] = []
    for item in out:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _sqlite_count(db_path: Path, table: str, where: str = "", params: tuple[Any, ...] = ()) -> int:
    if not db_path.exists():
        return 0
    try:
        with sqlite3.connect(db_path) as con:
            cur = con.cursor()
            cur.execute(f"select count(*) from {table} {where}", params)
            return int(cur.fetchone()[0] or 0)
    except Exception:
        return 0


def _sqlite_scalar(db_path: Path, query: str, params: tuple[Any, ...] = ()) -> Any:
    if not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as con:
            cur = con.cursor()
            cur.execute(query, params)
            row = cur.fetchone()
            return row[0] if row else None
    except Exception:
        return None


def _paper_counts(project_root: Path, *, line: str, run_id: str | None, experiment_id: str | None) -> dict[str, int]:
    db_path = project_root / "DATA" / "paper" / "paper_trading.db"
    if not db_path.exists():
        return {
            "paper_gate_evaluated_count": 0,
            "paper_gate_pass_count": 0,
            "paper_gate_block_count": 0,
            "paper_gate_missing_feature_count": 0,
            "paper_intent_count": 0,
            "paper_order_count": 0,
            "paper_skip_count": 0,
            "paper_fill_count": 0,
        }
    clauses = ["strategy_line=?"]
    params: list[Any] = [line]
    if experiment_id:
        clauses.append("experiment_id=?")
        params.append(experiment_id)
    elif run_id:
        clauses.append("source_run_id=?")
        params.append(run_id)
    where = "where " + " and ".join(clauses)
    p = tuple(params)
    gate_eval = 0
    gate_pass = 0
    gate_block = 0
    gate_missing = 0
    for table in ("paper_trade_plans", "paper_intent_inbox", "paper_orders", "paper_skip_ledger"):
        gate_eval += _sqlite_count(db_path, table, f"{where} and gate_decision is not null", p)
        gate_pass += _sqlite_count(db_path, table, f"{where} and gate_decision in ('pass','gate_pass','allow')", p)
        gate_block += _sqlite_count(db_path, table, f"{where} and gate_decision in ('block','gate_blocked','blocked')", p)
        gate_missing += _sqlite_count(db_path, table, f"{where} and gate_decision in ('feature_missing','gate_feature_missing')", p)
    return {
        "paper_gate_evaluated_count": gate_eval,
        "paper_gate_pass_count": gate_pass,
        "paper_gate_block_count": gate_block,
        "paper_gate_missing_feature_count": gate_missing,
        "paper_intent_count": _sqlite_count(db_path, "paper_intent_inbox", where, p),
        "paper_order_count": _sqlite_count(db_path, "paper_orders", where, p),
        "paper_skip_count": _sqlite_count(db_path, "paper_skip_ledger", where, p),
        "paper_fill_count": _sqlite_count(db_path, "paper_fills", where, p),
    }


def _tq_p24_counts(project_root: Path, *, line: str, run_id: str | None) -> dict[str, int]:
    paper_db = project_root / "DATA" / "paper" / "paper_trading.db"
    research_db = project_root / "DATA" / "backtest" / "p21_parameter_optimization.db"
    clauses = ["strategy_line=?"]
    params: list[Any] = [line]
    if run_id:
        clauses.append("source_run_id=?")
        params.append(run_id)
    where = "where " + " and ".join(clauses)
    p = tuple(params)
    return {
        "tq_sample_count": _sqlite_count(paper_db, "trade_quality_diagnostic_samples", where, p),
        "p24_trade_fact_count": _sqlite_count(research_db, "research_trade_facts", where, p),
    }


def _strategy5_counts(project_root: Path, run_id: str | None) -> dict[str, int]:
    db_path = project_root / "DATA" / "strategy5" / "strategy5.db"
    usable_count = 0
    incomplete_count = 0
    if run_id:
        if db_path.exists():
            try:
                with sqlite3.connect(db_path) as con:
                    cur = con.cursor()
                    cur.execute("select evidence_json from strategy5_evidence where run_id=?", (run_id,))
                    for (raw,) in cur.fetchall():
                        try:
                            ev = json.loads(raw) if isinstance(raw, str) else {}
                        except Exception:
                            ev = {}
                        if ev.get("evidence_quality", {}).get("usable") is True:
                            usable_count += 1
                        else:
                            incomplete_count += 1
            except Exception:
                usable_count = 0
                incomplete_count = 0
        return {
            "evidence_count": int(_sqlite_scalar(db_path, "select evidence_count from strategy5_runs where run_id=?", (run_id,)) or 0),
            "evidence_usable_count": usable_count,
            "evidence_incomplete_count": incomplete_count,
            "decision_count": 0,
            "wait_count": 0,
            "observe_pool_count": 0,
            "observe_due_count": 0,
        }
    doc = _read_json(project_root / "DATA" / "strategy5" / "latest_direction_evidence.json")
    for row in _items(doc):
        if row.get("evidence_quality", {}).get("usable") is True:
            usable_count += 1
        else:
            incomplete_count += 1
    return {
        "evidence_count": _items_count(doc, "items"),
        "evidence_usable_count": usable_count,
        "evidence_incomplete_count": incomplete_count,
        "decision_count": 0,
        "wait_count": 0,
        "observe_pool_count": 0,
        "observe_due_count": 0,
    }


def _strategy6_counts(project_root: Path, run_id: str | None) -> dict[str, int]:
    db_path = project_root / "DATA" / "strategy6" / "strategy6.db"
    evidence_doc = _read_json(project_root / "DATA" / "strategy6" / "latest_evidence.json")
    decisions_doc = _read_json(project_root / "DATA" / "strategy6" / "latest_decisions.json")
    wait_doc = _read_json(project_root / "DATA" / "strategy6" / "latest_wait_pool.json")
    if run_id:
        evidence_count = int(_sqlite_scalar(db_path, "select evidence_count from strategy6_runs where run_id=?", (run_id,)) or _items_count(evidence_doc, "items"))
        decision_count = int(_sqlite_scalar(db_path, "select decision_count from strategy6_runs where run_id=?", (run_id,)) or _items_count(decisions_doc, "items"))
        wait_count = int(_sqlite_scalar(db_path, "select wait_count from strategy6_runs where run_id=?", (run_id,)) or _items_count(wait_doc, "items"))
    else:
        evidence_count = _items_count(evidence_doc, "items")
        decision_count = _items_count(decisions_doc, "items")
        wait_count = _items_count(wait_doc, "items")
    active_where = "where status in ('OBSERVING','WAIT_CONFIRM','WAIT_REBOUND','WAIT_MARKET_ACCEPTANCE','TECHNICAL_BLOCKED')"
    return {
        "evidence_count": evidence_count,
        "decision_count": decision_count,
        "wait_count": wait_count,
        "observe_pool_count": _sqlite_count(db_path, "strategy6_observe_pool", active_where),
        "observe_due_count": _sqlite_count(db_path, "strategy6_observe_pool", f"{active_where} and (next_check_at is null or next_check_at <= ?)", (to_iso_z(utc_now()),)),
    }


def _strategy4_counts(project_root: Path) -> dict[str, int]:
    db_path = project_root / "DATA" / "strategy4" / "strategy4.db"
    active_where = "where status in ('OBSERVING','WAIT_CONFIRM','WAIT_REBOUND','WAIT_MARKET_ACCEPTANCE','TECHNICAL_BLOCKED')"
    return {
        "evidence_count": 0,
        "decision_count": 0,
        "wait_count": 0,
        "observe_pool_count": _sqlite_count(db_path, "strategy4_observe_pool", active_where),
        "observe_due_count": _sqlite_count(db_path, "strategy4_observe_pool", f"{active_where} and (next_check_at is null or next_check_at <= ?)", (to_iso_z(utc_now()),)),
    }


def _trade_plan_counts(project_root: Path, line: str) -> tuple[dict[str, Any], dict[str, int]]:
    doc = _read_json(project_root / "DATA" / "decisions" / f"latest_trade_plan_{line}.json")
    plans = doc.get("plans") if isinstance(doc.get("plans"), list) else doc.get("items") if isinstance(doc.get("items"), list) else []
    plan_count = int(doc.get("count") or len(plans) or 0)
    executable = doc.get("executable_count")
    if executable is None:
        executable = sum(1 for row in plans if bool(row.get("executable")))
    return doc, {"trade_plan_count": plan_count, "trade_plan_executable_count": int(executable or 0)}


def _stage_status(stage: str, count: int | None, *, applicable: bool = True, fresh: bool = True, blocked: bool = False) -> str:
    if not applicable:
        return "not_applicable"
    if not fresh:
        return "stale"
    if blocked:
        return "blocked"
    if count is not None and int(count) == 0:
        return "empty"
    return "ok"


def _classify(row: dict[str, Any]) -> tuple[str, list[str]]:
    line = str(row.get("strategy_line") or "")
    if row.get("selected") is False:
        return "skipped_not_selected", ["strategy_line_not_selected"]
    if int(row.get("scan_raw_count") or 0) == 0 and int(row.get("scan_watch_count") or 0) == 0 and int(row.get("scan_strong_count") or 0) == 0:
        return "scan_empty", ["scan_empty"]
    if line in {"strategy5", "strategy6"} and int(row.get("scan_strong_count") or 0) == 0 and int(row.get("evidence_count") or 0) == 0:
        return "strong_empty", ["scan_strong_count_zero"]
    if int(row.get("factor_count") or 0) == 0:
        return "factor_empty", ["factor_count_zero"]
    if line in {"strategy5", "strategy6"} and int(row.get("evidence_count") or 0) == 0:
        return "evidence_empty", ["strategy_evidence_empty"]
    if line == "strategy5" and int(row.get("evidence_count") or 0) > 0 and int(row.get("evidence_usable_count") or 0) == 0:
        return "evidence_incomplete", ["strategy5_evidence_not_usable"]
    if line == "strategy6" and int(row.get("decision_count") or 0) > 0 and int(row.get("trade_plan_executable_count") or 0) == 0:
        return "decision_wait_only", ["strategy6_decisions_not_executable"]
    if line in {"strategy4", "strategy6"} and int(row.get("observe_pool_count") or 0) == 0 and int(row.get("trade_plan_executable_count") or 0) == 0:
        return "observe_pool_empty", ["observe_pool_empty"]
    if int(row.get("trade_plan_count") or 0) == 0:
        return "trade_plan_empty", ["trade_plan_count_zero"]
    if int(row.get("trade_plan_executable_count") or 0) == 0:
        return "trade_plan_no_executable", ["trade_plan_executable_count_zero"]
    gate_eval = int(row.get("paper_gate_evaluated_count") or 0)
    gate_block = int(row.get("paper_gate_block_count") or 0)
    gate_missing = int(row.get("paper_gate_missing_feature_count") or 0)
    if gate_eval > 0 and gate_missing >= gate_eval:
        return "paper_gate_feature_missing", ["paper_gate_feature_missing"]
    if gate_eval > 0 and gate_block >= gate_eval:
        return "paper_gate_all_blocked", ["paper_gate_all_blocked"]
    if int(row.get("paper_intent_count") or 0) == 0:
        return "paper_intent_empty", ["paper_intent_empty"]
    if int(row.get("paper_order_count") or 0) == 0:
        return "paper_order_empty", ["paper_order_empty"]
    if int(row.get("paper_fill_count") or 0) == 0:
        return "paper_fill_empty", ["paper_fill_empty"]
    if int(row.get("tq_sample_count") or 0) == 0:
        return "tq_missing", ["trade_quality_sample_missing"]
    if int(row.get("p24_trade_fact_count") or 0) == 0:
        return "p24_missing", ["p24_trade_fact_missing"]
    return "ok", []


def build_cross_strategy_funnel_snapshot(project_root: Path, *, write: bool = True) -> dict[str, Any]:
    root = Path(project_root)
    generated_at = to_iso_z(utc_now())
    report = _read_json(root / "DATA" / "reports" / "latest_strategy_pipeline_report.json")
    progress = _read_json(root / "DATA" / "runtime" / "strategy_pipeline_progress.json")
    gate_cfg = _read_json(root / "DATA" / "paper" / "v5_trade_gate_experiment.json")
    upstream = report.get("upstream_refresh") if isinstance(report.get("upstream_refresh"), dict) else {}
    scan_raw = int(upstream.get("raw_count") or upstream.get("count") or 0)
    scan_watch = int(upstream.get("watch_count") or 0)
    scan_strong = int(upstream.get("strong_count") or 0)
    selected_lines = report.get("selected_lines") if isinstance(report.get("selected_lines"), list) else progress.get("selected_lines") if isinstance(progress.get("selected_lines"), list) else list(PIPELINE_LINES)
    selected_set = {str(x) for x in selected_lines}
    factor_doc = _read_json(root / "DATA" / "factors" / "latest_factor_snapshot_withoutoficvd.json")
    factor_count = _items_count(factor_doc, "items")
    lines: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    experiment_id = str(gate_cfg.get("experiment_id") or "") or None
    for line in STRATEGY_LINES:
        trade_doc, trade_counts = _trade_plan_counts(root, line)
        run_id = trade_doc.get("run_id") or trade_doc.get("output_run_id") or report.get("run_id")
        cycle_id = trade_doc.get("cycle_id") or trade_doc.get("output_cycle_id") or report.get("cycle_id")
        if line == "strategy5":
            strategy_counts = _strategy5_counts(root, str(run_id) if run_id else None)
        elif line == "strategy6":
            strategy_counts = _strategy6_counts(root, str(run_id) if run_id else None)
        elif line == "strategy4":
            strategy_counts = _strategy4_counts(root)
        else:
            strategy_counts = {
                "evidence_count": 0,
                "decision_count": 0,
                "wait_count": 0,
                "observe_pool_count": 0,
                "observe_due_count": 0,
            }
        paper = _paper_counts(root, line=line, run_id=str(run_id) if run_id else None, experiment_id=experiment_id)
        tq_p24 = _tq_p24_counts(root, line=line, run_id=str(run_id) if run_id else None)
        selected = line in selected_set
        row: dict[str, Any] = {
            "strategy_line": line,
            "run_id": run_id,
            "cycle_id": cycle_id,
            "generated_at": generated_at,
            "experiment_id": experiment_id,
            "paper_epoch_id": (gate_cfg.get("line_epochs") or {}).get(line),
            "selected": selected,
            "freshness": upstream.get("current_freshness") or "unknown",
            "scan_raw_count": scan_raw,
            "scan_watch_count": scan_watch,
            "scan_strong_count": scan_strong,
            "factor_count": factor_count,
            **strategy_counts,
            **trade_counts,
            **paper,
            **tq_p24,
            "source_paths": {
                "latest_report": str(root / "DATA" / "reports" / "latest_strategy_pipeline_report.json"),
                "progress": str(root / "DATA" / "runtime" / "strategy_pipeline_progress.json"),
                "factor": str(root / "DATA" / "factors" / "latest_factor_snapshot_withoutoficvd.json"),
                "trade_plan": str(root / "DATA" / "decisions" / f"latest_trade_plan_{line}.json"),
            },
        }
        breakpoint_stage, breakpoint_reasons = _classify(row)
        row["breakpoint_stage"] = breakpoint_stage
        row["breakpoint_reason_codes"] = breakpoint_reasons
        row["stage_cards"] = _stage_cards(row, trade_doc)
        lines[line] = row
        rows.append(row)
    payload = {
        "schema_version": "7.137-cross-strategy-funnel-v1",
        "source": "cross_strategy_funnel",
        "generated_at": generated_at,
        "run_id": report.get("run_id") or progress.get("run_id"),
        "cycle_id": report.get("cycle_id") or progress.get("cycle_id"),
        "experiment_id": experiment_id,
        "selected_lines": selected_lines,
        "lines": lines,
        "items": rows,
        "summary": {
            "line_count": len(rows),
            "ok_count": sum(1 for r in rows if r.get("breakpoint_stage") == "ok"),
            "breakpoints": {stage: sum(1 for r in rows if r.get("breakpoint_stage") == stage) for stage in sorted({str(r.get("breakpoint_stage")) for r in rows})},
        },
    }
    if write:
        latest = root / "DATA" / "runtime" / "latest_cross_strategy_funnel_snapshot.json"
        history = root / "DATA" / "runtime" / "cross_strategy_funnel_history.jsonl"
        write_json_atomic(latest, payload)
        history.parent.mkdir(parents=True, exist_ok=True)
        with history.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    return payload


def _stage_cards(row: dict[str, Any], trade_doc: dict[str, Any]) -> list[dict[str, Any]]:
    line = str(row.get("strategy_line") or "")
    selected = bool(row.get("selected"))
    p3_applicable = line in {"micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"}
    observe_applicable = line in {"strategy4", "strategy6"}
    stages = [
        {
            "code": "P1",
            "name": "Universe / Scan",
            "status": _stage_status("P1", int(row.get("scan_watch_count") or 0) + int(row.get("scan_strong_count") or 0), applicable=selected),
            "counts": {"raw": row.get("scan_raw_count"), "watch": row.get("scan_watch_count"), "strong": row.get("scan_strong_count")},
            "reason_codes": _reason_codes("strategy_line_not_selected" if not selected else []),
        },
        {
            "code": "P2",
            "name": "Snapshot / Factor",
            "status": _stage_status("P2", int(row.get("factor_count") or 0), applicable=selected),
            "counts": {"factor": row.get("factor_count")},
            "reason_codes": [],
        },
        {
            "code": "P3",
            "name": "Evidence / Decision",
            "status": _stage_status("P3", int(row.get("evidence_count") or 0) + int(row.get("decision_count") or 0) + int(row.get("wait_count") or 0) + int(row.get("observe_due_count") or 0), applicable=selected and p3_applicable),
            "counts": {
                "evidence": row.get("evidence_count"),
                "usable": row.get("evidence_usable_count") if line == "strategy5" else "n/a",
                "incomplete": row.get("evidence_incomplete_count") if line == "strategy5" else "n/a",
                "decisions": row.get("decision_count"),
                "wait": row.get("wait_count"),
                "observe": row.get("observe_pool_count") if observe_applicable else "n/a",
                "due": row.get("observe_due_count") if observe_applicable else "n/a",
            },
            "reason_codes": [],
        },
        {
            "code": "P4",
            "name": "Plan",
            "status": _stage_status("P4", int(row.get("trade_plan_count") or 0), applicable=selected, blocked=str(trade_doc.get("status") or "") == "blocked"),
            "counts": {"plans": row.get("trade_plan_count"), "executable": row.get("trade_plan_executable_count")},
            "reason_codes": _reason_codes(trade_doc.get("reason_codes"), trade_doc.get("status") if trade_doc.get("status") in {"blocked", "no_entries"} else []),
        },
        {
            "code": "P5",
            "name": "Paper Gate / Paper",
            "status": _stage_status("P5", int(row.get("paper_gate_evaluated_count") or 0) + int(row.get("paper_intent_count") or 0) + int(row.get("paper_order_count") or 0), applicable=selected),
            "counts": {
                "gate": row.get("paper_gate_evaluated_count"),
                "pass": row.get("paper_gate_pass_count"),
                "block": row.get("paper_gate_block_count"),
                "missing": row.get("paper_gate_missing_feature_count"),
                "intent": row.get("paper_intent_count"),
                "order": row.get("paper_order_count"),
                "skip": row.get("paper_skip_count"),
            },
            "reason_codes": [],
        },
        {
            "code": "P6",
            "name": "TQ / P24",
            "status": _stage_status("P6", int(row.get("paper_fill_count") or 0) + int(row.get("tq_sample_count") or 0) + int(row.get("p24_trade_fact_count") or 0), applicable=selected),
            "counts": {"fills": row.get("paper_fill_count"), "tq": row.get("tq_sample_count"), "p24": row.get("p24_trade_fact_count")},
            "reason_codes": [],
        },
    ]
    breakpoint_stage = row.get("breakpoint_stage")
    stage_for_breakpoint = {
        "scan_empty": "P1",
        "strong_empty": "P1",
        "factor_empty": "P2",
        "evidence_empty": "P3",
        "evidence_incomplete": "P3",
        "decision_wait_only": "P3",
        "observe_pool_empty": "P3",
        "observe_pool_expired": "P3",
        "trade_plan_empty": "P4",
        "trade_plan_no_executable": "P4",
        "paper_gate_all_blocked": "P5",
        "paper_gate_feature_missing": "P5",
        "paper_intent_empty": "P5",
        "paper_order_empty": "P5",
        "paper_fill_empty": "P6",
        "tq_missing": "P6",
        "p24_missing": "P6",
    }.get(str(breakpoint_stage))
    for stage in stages:
        stage["breakpoint"] = stage["code"] == stage_for_breakpoint
        if stage["breakpoint"]:
            stage["status"] = "blocked" if breakpoint_stage not in {"scan_empty", "factor_empty", "trade_plan_empty"} else "empty"
            stage["reason_codes"] = _reason_codes(stage.get("reason_codes"), row.get("breakpoint_reason_codes"))
    return stages


def latest_cross_strategy_funnel_payload(project_root: Path, *, refresh: bool = True) -> dict[str, Any]:
    latest = Path(project_root) / "DATA" / "runtime" / "latest_cross_strategy_funnel_snapshot.json"
    if refresh or not latest.exists():
        return build_cross_strategy_funnel_snapshot(Path(project_root), write=True)
    return _read_json(latest)


def cross_strategy_funnel_history_payload(project_root: Path, *, limit: int = 50) -> dict[str, Any]:
    history = Path(project_root) / "DATA" / "runtime" / "cross_strategy_funnel_history.jsonl"
    rows: list[dict[str, Any]] = []
    if history.exists():
        lines = history.read_text(encoding="utf-8", errors="replace").splitlines()
        for line in lines[-max(1, int(limit)):]:
            try:
                row = json.loads(line)
                if isinstance(row, dict):
                    rows.append(row)
            except Exception:
                continue
    return {
        "schema_version": "7.137-cross-strategy-funnel-history-v1",
        "source": "cross_strategy_funnel_history",
        "generated_at": to_iso_z(utc_now()),
        "count": len(rows),
        "items": rows,
    }
