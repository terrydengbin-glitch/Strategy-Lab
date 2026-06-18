from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.api.services import config_effective_payload

RUNTIME_DIR = ROOT / "DATA" / "runtime"
DECISIONS_DIR = ROOT / "DATA" / "decisions"
PAPER_DIR = ROOT / "DATA" / "paper"
REPORT_DIR = ROOT / "docs" / "reports"

LINES = ["without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"]
STEP7_135_EXPERIMENT_ID = "paper_exp_step7_135_strategy5_6_v5_gate_20260616"


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def compact_fields(rows: list[dict[str, Any]], limit: int = 12) -> list[str]:
    return [str(row.get("field_path")) for row in rows[:limit] if row.get("field_path")]


def load_trade_plan(line: str) -> dict[str, Any]:
    path = DECISIONS_DIR / f"latest_trade_plan_{line}.json"
    payload = read_json(path, {})
    plans = payload.get("plans") or []
    if not isinstance(plans, list):
        plans = []
    reason_counter: Counter[str] = Counter()
    action_counter: Counter[str] = Counter()
    entry_mode_counter: Counter[str] = Counter()
    executable = 0
    sample_guards: dict[str, Any] = {}
    sample_plan: dict[str, Any] = {}
    for plan in plans:
        if plan.get("executable"):
            executable += 1
        action_counter[str(plan.get("action") or "unknown")] += 1
        entry_mode_counter[str(plan.get("entry_mode") or "unknown")] += 1
        for reason in plan.get("reason_codes") or []:
            reason_counter[str(reason)] += 1
        if not sample_plan:
            sample_plan = {
                "symbol": plan.get("symbol"),
                "action": plan.get("action"),
                "entry_mode": plan.get("entry_mode"),
                "executable": plan.get("executable"),
                "reason_codes": plan.get("reason_codes") or [],
            }
            guards = plan.get("guards") or {}
            cfg = guards.get("gate_config_snapshot") or {}
            sample_guards = {
                key: cfg.get(key)
                for key in [
                    "min_score",
                    "require_liquidity_ok",
                    "require_range_room_ok",
                    "min_net_rr",
                    "min_effective_rr",
                    "tp_target_policy_mode",
                    "allow_wait_plan",
                    "require_micro_ready",
                    "require_micro_alignment",
                ]
                if key in cfg
            }
    return {
        "path": str(path.relative_to(ROOT)),
        "exists": path.exists(),
        "run_id": payload.get("run_id"),
        "cycle_id": payload.get("cycle_id"),
        "generated_at": payload.get("generated_at"),
        "plan_count": len(plans),
        "executable_count": executable,
        "action_counts": dict(action_counter.most_common()),
        "entry_mode_counts": dict(entry_mode_counter.most_common()),
        "top_reason_codes": dict(reason_counter.most_common(12)),
        "sample_plan": sample_plan,
        "sample_gate_config": sample_guards,
    }


def fetch_counts(con: sqlite3.Connection, table: str, line: str, where: str = "", params: tuple[Any, ...] = ()) -> int:
    sql = f"select count(*) from {table} where strategy_line = ?"
    values: tuple[Any, ...] = (line,)
    if where:
        sql += f" and {where}"
        values += params
    row = con.execute(sql, values).fetchone()
    return int(row[0] or 0)


def load_paper_counts() -> dict[str, Any]:
    db_path = PAPER_DIR / "paper_trading.db"
    result: dict[str, Any] = {
        "db_path": str(db_path.relative_to(ROOT)),
        "exists": db_path.exists(),
        "lines": {},
    }
    if not db_path.exists():
        return result
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        for line in LINES:
            line_counts: dict[str, Any] = {
                "trade_plans_total": fetch_counts(con, "paper_trade_plans", line),
                "orders_total": fetch_counts(con, "paper_orders", line),
                "fills_total": fetch_counts(con, "paper_fills", line),
                "skips_total": fetch_counts(con, "paper_skip_ledger", line),
                "experiment_trade_plans": fetch_counts(
                    con,
                    "paper_trade_plans",
                    line,
                    "experiment_id = ?",
                    (STEP7_135_EXPERIMENT_ID,),
                ),
                "experiment_orders": fetch_counts(
                    con,
                    "paper_orders",
                    line,
                    "experiment_id = ?",
                    (STEP7_135_EXPERIMENT_ID,),
                ),
                "experiment_skips": fetch_counts(
                    con,
                    "paper_skip_ledger",
                    line,
                    "experiment_id = ?",
                    (STEP7_135_EXPERIMENT_ID,),
                ),
            }
            gates = [
                dict(row)
                for row in con.execute(
                    """
                    select gate_decision, count(*) as count
                    from paper_orders
                    where strategy_line = ? and experiment_id = ?
                    group by gate_decision
                    order by count desc
                    """,
                    (line, STEP7_135_EXPERIMENT_ID),
                )
            ]
            line_counts["experiment_order_gate_counts"] = gates
            result["lines"][line] = line_counts
    finally:
        con.close()
    return result


def classify_root_cause(line: str, funnel_line: dict[str, Any], plan: dict[str, Any]) -> str:
    stage = funnel_line.get("breakpoint_stage")
    selected = funnel_line.get("selected")
    reasons = set(plan.get("top_reason_codes") or {})
    if not selected:
        return "expected_not_selected"
    if stage == "decision_wait_only":
        return "expected_wait_or_wait_due_adapter"
    if "strategy5_base_trade_plan_not_executable" in reasons:
        return "lineage_base_plan"
    if any(reason in reasons for reason in ["liquidity_not_ok", "depth_not_enough_for_notional", "slippage_missing"]):
        return "market_liquidity_or_config_threshold"
    if any("range" in reason for reason in reasons):
        return "market_room_or_config_threshold"
    if stage == "trade_plan_no_executable":
        return "trade_plan_executable"
    return "unknown"


def build_diagnostic() -> dict[str, Any]:
    funnel = read_json(RUNTIME_DIR / "latest_cross_strategy_funnel_snapshot.json", {})
    summary = read_json(RUNTIME_DIR / "config_field_used_by_map_summary.json", {})
    full_map = read_json(RUNTIME_DIR / "config_field_used_by_map.json", {})
    step7_135 = read_json(RUNTIME_DIR / "step7_135_latest.json", {})
    gate_experiment = read_json(PAPER_DIR / "v5_trade_gate_experiment.json", {})
    paper_counts = load_paper_counts()

    fields = full_map.get("fields") or []
    config_by_line: dict[str, Any] = {}
    for line in LINES:
        effective = config_effective_payload(line)
        config_by_line[line] = {
            "inherits_from": effective.get("inherits_from"),
            "live_executable_source": effective.get("live_executable_source"),
            "counts": effective.get("counts"),
            "notes": effective.get("notes"),
            "direct_executable_examples": compact_fields(effective.get("direct_executable_fields") or []),
            "paper_only_examples": compact_fields(effective.get("paper_only_fields") or []),
            "legacy_examples": compact_fields(effective.get("legacy_fields") or []),
        }

    line_diagnostics: dict[str, Any] = {}
    for line in LINES:
        funnel_line = (funnel.get("lines") or {}).get(line, {})
        plan = load_trade_plan(line)
        line_diagnostics[line] = {
            "strategy_line": line,
            "selected": funnel_line.get("selected"),
            "run_id": funnel_line.get("run_id") or plan.get("run_id"),
            "cycle_id": funnel_line.get("cycle_id") or plan.get("cycle_id"),
            "experiment_id": funnel_line.get("experiment_id") or gate_experiment.get("experiment_id"),
            "paper_epoch_id": funnel_line.get("paper_epoch_id"),
            "config": config_by_line[line],
            "funnel": {
                key: funnel_line.get(key)
                for key in [
                    "freshness",
                    "scan_raw_count",
                    "scan_watch_count",
                    "scan_strong_count",
                    "factor_count",
                    "evidence_count",
                    "evidence_usable_count",
                    "evidence_incomplete_count",
                    "decision_count",
                    "wait_count",
                    "observe_pool_count",
                    "observe_due_count",
                    "trade_plan_count",
                    "trade_plan_executable_count",
                    "paper_gate_evaluated_count",
                    "paper_gate_pass_count",
                    "paper_gate_block_count",
                    "paper_gate_missing_feature_count",
                    "paper_intent_count",
                    "paper_order_count",
                    "paper_skip_count",
                    "paper_fill_count",
                    "tq_sample_count",
                    "p24_trade_fact_count",
                    "breakpoint_stage",
                    "breakpoint_reason_codes",
                ]
            },
            "trade_plan": plan,
            "paper": (paper_counts.get("lines") or {}).get(line, {}),
            "breakpoint": {
                "stage": funnel_line.get("breakpoint_stage"),
                "reason_codes": funnel_line.get("breakpoint_reason_codes") or [],
                "root_cause_class": classify_root_cause(line, funnel_line, plan),
            },
        }

    return {
        "schema_version": "7.142-all-strategy-chain-config-diagnostic-v1",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source": "step7_142_all_strategy_chain_config_diagnostic",
        "config_summary": {
            "generated_at": summary.get("generated_at"),
            "field_count": summary.get("field_count"),
            "direct_executable_field_count": summary.get("direct_executable_field_count"),
            "paper_only_field_count": summary.get("paper_only_field_count"),
            "legacy_or_disabled_field_count": summary.get("legacy_or_disabled_field_count"),
            "unknown_field_count": summary.get("unknown_field_count"),
            "business_stage_counts": summary.get("business_stage_counts"),
            "important_findings": summary.get("important_findings"),
        },
        "field_map_count": len(fields),
        "funnel_summary": {
            "generated_at": funnel.get("generated_at"),
            "run_id": funnel.get("run_id"),
            "cycle_id": funnel.get("cycle_id"),
            "selected_lines": funnel.get("selected_lines"),
            "summary": funnel.get("summary"),
        },
        "step7_135_monitor": {
            "generated_at": step7_135.get("generated_at"),
            "elapsed_sec": step7_135.get("elapsed_sec"),
            "experiment_id": step7_135.get("experiment_id"),
            "gate_config": step7_135.get("gate_config"),
            "paper_ledger": step7_135.get("paper_ledger"),
        },
        "gate_experiment": gate_experiment,
        "paper_counts": paper_counts,
        "lines": line_diagnostics,
    }


def md_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    output = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        output.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return output


def render_report(diag: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# STEP7.142 All Strategy Chain & Config Diagnostic Audit")
    lines.append("")
    lines.append(f"- generated_at: `{diag['generated_at']}`")
    lines.append(f"- funnel_generated_at: `{diag['funnel_summary'].get('generated_at')}`")
    lines.append(f"- funnel_run_id: `{diag['funnel_summary'].get('run_id')}`")
    lines.append(f"- selected_lines: `{', '.join(diag['funnel_summary'].get('selected_lines') or [])}`")
    lines.append(f"- step7_135_experiment_id: `{STEP7_135_EXPERIMENT_ID}`")
    lines.append("")
    lines.append("## Executive Judgment")
    lines.append("")
    lines.append("- NO-GO for starting a blind 10h STEP7.135 run from the current snapshot.")
    lines.append("- The blocker is not proven to be V5 gate strictness. The current evidence points to pre-paper executable flow: strategy5 is blocked by base-plan executable lineage, while strategy6 is still WAIT-only despite due observe rows.")
    lines.append("- Config governance is usable for diagnosis: no unknown fields, and legacy trade_quality_gate/sl_tp_quality are disabled/legacy rather than current live V5 gate controls.")
    lines.append("")

    cfg = diag["config_summary"]
    lines.append("## Config Impact Summary")
    lines.append("")
    lines.extend(
        md_table(
            ["metric", "value"],
            [
                ["field_count", cfg.get("field_count")],
                ["direct_executable_field_count", cfg.get("direct_executable_field_count")],
                ["paper_only_field_count", cfg.get("paper_only_field_count")],
                ["legacy_or_disabled_field_count", cfg.get("legacy_or_disabled_field_count")],
                ["unknown_field_count", cfg.get("unknown_field_count")],
            ],
        )
    )
    lines.append("")
    for finding in cfg.get("important_findings") or []:
        lines.append(f"- {finding}")
    lines.append("")

    rows = []
    for line, item in diag["lines"].items():
        funnel = item["funnel"]
        paper = item["paper"]
        rows.append(
            [
                line,
                funnel.get("freshness"),
                item.get("selected"),
                funnel.get("factor_count"),
                funnel.get("evidence_count"),
                funnel.get("decision_count"),
                funnel.get("wait_count"),
                funnel.get("observe_due_count"),
                funnel.get("trade_plan_count"),
                funnel.get("trade_plan_executable_count"),
                funnel.get("paper_gate_pass_count"),
                funnel.get("paper_order_count"),
                paper.get("experiment_orders"),
                funnel.get("paper_fill_count"),
                item["breakpoint"].get("stage"),
                item["breakpoint"].get("root_cause_class"),
            ]
        )
    lines.append("## Funnel + Paper Matrix")
    lines.append("")
    lines.extend(
        md_table(
            [
                "line",
                "fresh",
                "selected",
                "factor",
                "evidence",
                "decision",
                "wait",
                "due",
                "plans",
                "exec",
                "gate_pass",
                "orders_funnel",
                "orders_db_exp",
                "fills",
                "breakpoint",
                "root_class",
            ],
            rows,
        )
    )
    lines.append("")

    lines.append("## Per-Line Diagnosis")
    lines.append("")
    for line, item in diag["lines"].items():
        cfg_line = item["config"]
        plan = item["trade_plan"]
        paper = item["paper"]
        lines.append(f"### {line}")
        lines.append("")
        lines.append(f"- config_inherits_from: `{cfg_line.get('inherits_from')}`")
        lines.append(f"- live_executable_source: `{json.dumps(cfg_line.get('live_executable_source'), ensure_ascii=False)}`")
        lines.append(f"- config_counts: `{json.dumps(cfg_line.get('counts'), ensure_ascii=False)}`")
        lines.append(f"- breakpoint: `{item['breakpoint'].get('stage')}` / `{item['breakpoint'].get('root_cause_class')}`")
        lines.append(f"- top_reason_codes: `{json.dumps(plan.get('top_reason_codes'), ensure_ascii=False)}`")
        lines.append(f"- action_counts: `{json.dumps(plan.get('action_counts'), ensure_ascii=False)}`")
        lines.append(f"- sample_gate_config: `{json.dumps(plan.get('sample_gate_config'), ensure_ascii=False)}`")
        lines.append(f"- paper_counts: `{json.dumps(paper, ensure_ascii=False)}`")
        notes = cfg_line.get("notes") or []
        for note in notes:
            lines.append(f"- config_note: {note}")
        lines.append("")

    lines.append("## STEP7.135 Readiness")
    lines.append("")
    lines.append("- `strategy5`: NO-GO for 10h quality validation. It has usable evidence and V5 gate pass/order lineage, but current trade plan remains non-executable and reason codes include `strategy5_base_trade_plan_not_executable`. Next implementation should focus on base-plan inheritance versus line-specific executable adapter semantics before interpreting paper performance.")
    lines.append("- `strategy6`: NO-GO for 10h quality validation. It has evidence/decisions and observe due rows, but all decisions remain WAIT and no paper gate/orders are reached. Next implementation should focus on WAIT due / confirmation trigger plan handoff before treating V5 gate as the bottleneck.")
    lines.append("- `without_micro` and `micro_fast`: both selected but currently blocked at trade_plan_no_executable; their reason codes are liquidity/range/entry-quality oriented and should be treated as market/config-threshold evidence, not paper-gate evidence.")
    lines.append("- `micro_full` and `strategy4`: currently skipped/not selected in the latest funnel; they are observable but not valid for live executable conclusions in this snapshot.")
    lines.append("")

    lines.append("## Evidence Files")
    lines.append("")
    lines.append("- `DATA/runtime/latest_cross_strategy_funnel_snapshot.json`")
    lines.append("- `DATA/runtime/config_field_used_by_map.json`")
    lines.append("- `DATA/runtime/config_field_used_by_map_summary.json`")
    lines.append("- `DATA/runtime/step7_135_latest.json`")
    lines.append("- `DATA/paper/v5_trade_gate_experiment.json`")
    lines.append("- `DATA/paper/paper_trading.db`")
    lines.append("")

    lines.append("## Boundary")
    lines.append("")
    lines.append("No strategy logic, config value, paper ledger row, daemon state, or notification behavior was changed. This audit is read-only except for writing this report.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    diag = build_diagnostic()
    json_path = RUNTIME_DIR / "step7_142_all_strategy_chain_config_diagnostic.json"
    report_path = REPORT_DIR / f"STEP7.142_all_strategy_chain_config_diagnostic_{utc_stamp()}.md"
    json_path.write_text(json.dumps(diag, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(render_report(diag), encoding="utf-8")
    print(json.dumps({"ok": True, "json": str(json_path), "report": str(report_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
