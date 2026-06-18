from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "laoma_signal_engine" / "config" / "default.yaml"
RUNTIME_DIR = ROOT / "DATA" / "runtime"
REPORT_DIR = ROOT / "docs" / "reports"

STRATEGY_LINES = ["without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"]
TRADE_PLAN_LINES = set(STRATEGY_LINES)

SOURCE_DIRS = [
    ROOT / "laoma_signal_engine",
    ROOT / "scripts",
    ROOT / "web" / "src",
]

EXCLUDED_SOURCE_PARTS = {
    "__pycache__",
    ".pytest_cache",
}


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def flatten(obj: Any, prefix: str = "") -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            rows.extend(flatten(value, child))
    elif isinstance(obj, list):
        rows.append((prefix, obj))
    else:
        rows.append((prefix, obj))
    return rows


def source_files() -> list[Path]:
    files: list[Path] = []
    for source_dir in SOURCE_DIRS:
        if not source_dir.exists():
            continue
        for path in source_dir.rglob("*"):
            if any(part in EXCLUDED_SOURCE_PARTS for part in path.parts):
                continue
            if path.suffix.lower() in {".py", ".vue", ".ts", ".js", ".yaml", ".yml"}:
                files.append(path)
    return files


def build_source_index(files: list[Path]) -> dict[str, list[str]]:
    index: dict[str, list[str]] = defaultdict(list)
    tokens = {
        "trade_plan_lines",
        "strategy_pipeline",
        "paper",
        "strategy4",
        "strategy5",
        "strategy6",
        "tp_target_policy",
        "trade_quality_gate",
        "sl_tp_quality",
        "market_now_calibration",
        "position_sizing",
        "trade_plan_risk",
        "market_entry_liquidity",
        "decision_refresh",
        "micro_router",
        "micro_daemon",
        "backtest",
        "sandbox",
        "v5_trade_gate",
    }
    for path in files:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        rel = str(path.relative_to(ROOT)).replace("\\", "/")
        for token in tokens:
            if token in text:
                index[token].append(rel)
    return dict(index)


def refs_for_field(field_path: str, source_index: dict[str, list[str]]) -> list[str]:
    parts = field_path.split(".")
    candidates: list[str] = []
    for part in reversed(parts):
        if part in source_index:
            candidates.extend(source_index[part])
            break
    for token in (
        "trade_plan_lines",
        "strategy_pipeline",
        "paper",
        "backtest",
        "sandbox",
        "strategy5",
        "strategy6",
        "strategy4",
        "micro_router",
        "market_entry_liquidity",
        "decision_refresh",
    ):
        if token in field_path and token in source_index:
            candidates.extend(source_index[token])
    return sorted(set(candidates))[:12]


def line_runtime_observation(line: str, funnel: dict[str, Any]) -> dict[str, Any]:
    lines = funnel.get("lines")
    if not isinstance(lines, dict):
        return {}
    row = lines.get(line)
    if not isinstance(row, dict):
        return {}
    return {
        "selected": row.get("selected"),
        "breakpoint_stage": row.get("breakpoint_stage"),
        "breakpoint_reason_codes": row.get("breakpoint_reason_codes"),
        "trade_plan_count": row.get("trade_plan_count"),
        "trade_plan_executable_count": row.get("trade_plan_executable_count"),
        "paper_gate_evaluated_count": row.get("paper_gate_evaluated_count"),
        "paper_gate_pass_count": row.get("paper_gate_pass_count"),
        "paper_order_count": row.get("paper_order_count"),
    }


def trade_plan_reason_counts(line: str) -> dict[str, int]:
    filename = {
        "without_micro": "latest_trade_plan_without_micro.json",
        "micro_fast": "latest_trade_plan_micro_fast.json",
        "micro_full": "latest_trade_plan_micro_full.json",
        "strategy4": "latest_trade_plan_strategy4.json",
        "strategy5": "latest_trade_plan_strategy5.json",
        "strategy6": "latest_trade_plan_strategy6.json",
    }.get(line)
    if not filename:
        return {}
    doc = read_json(ROOT / "DATA" / "decisions" / filename)
    plans = doc.get("plans")
    if not isinstance(plans, list):
        return {}
    counts: Counter[str] = Counter()
    for plan in plans:
        if isinstance(plan, dict):
            for code in plan.get("reason_codes") or []:
                counts[str(code)] += 1
    return dict(counts.most_common(12))


def infer_trade_plan_leaf(field_path: str, line: str, value: Any, funnel: dict[str, Any], source_index: dict[str, list[str]]) -> dict[str, Any]:
    subpath = field_path.split(".", 2)[2] if field_path.count(".") >= 2 else ""
    disabled = False
    status = "active"
    ui = "primary"
    stages = ["trade_plan_executable"]
    direct_executable = True
    paper_impact = False
    backtest_impact = line in {"without_micro", "strategy4", "strategy5", "strategy6"}
    sandbox_impact = True
    notes: list[str] = []

    if line in {"strategy4", "strategy5", "strategy6"}:
        notes.append("line inherits from without_micro unless its adapter builds a fresh line config")
    if line == "strategy4":
        stages.extend(["strategy4_observe"])
        notes.append("strategy4 persistent observe currently starts from strategy1/without_micro wait candidates")
    if line == "strategy5":
        stages.extend(["strategy5_evidence"])
        notes.append("current strategy5 live plan overlays strategy5 evidence onto base without_micro trade plan")
    if line == "strategy6":
        stages.extend(["strategy6_evidence_decision_observe"])
        notes.append("current strategy6 live executable requires base without_micro executable plus strategy6 EXECUTABLE state")

    if any(key in subpath for key in ("require_micro_", "micro_consumption", "weak_micro_", "max_micro_age_sec")):
        stages.append("micro_fast_full")
        ui = "advanced"
        if line in {"without_micro", "strategy4", "strategy5", "strategy6"}:
            notes.append("micro field is mostly inert for without-micro-like lines unless reused through shared config snapshots")
    if subpath.startswith("market_now_calibration") or subpath.startswith("short_now_calibration"):
        stages.append("market_now_entry_gate")
        ui = "advanced"
    if subpath.startswith("tp_target_policy"):
        stages.append("exit_rr_policy")
        ui = "compact"
        backtest_impact = True
    if subpath.startswith("trade_quality_gate"):
        status = "disabled" if ".enabled" in field_path and value is False else "legacy"
        ui = "hide_legacy"
        stages.append("legacy_trade_quality_gate")
        direct_executable = bool("enabled" in subpath and value is True)
        notes.append("legacy YAML TQ gate exists in trade_plan_lines, but current V5 paper gate uses DATA/paper/v5_trade_gate_experiment.json")
    if subpath.startswith("sl_tp_quality"):
        status = "disabled" if ".enabled" in field_path and value is False else "legacy"
        ui = "hide_legacy"
        stages.append("legacy_sl_tp_adjustment")
        notes.append("SL/TP quality adjustment is currently disabled and should be hidden from primary Config")
    if "position_sizing" in subpath or "planned_loss" in subpath or "risk_budget" in subpath:
        stages.append("position_sizing")
        ui = "advanced"
    if subpath in {"allow_wait_plan", "conditional_plan_expire_sec", "allow_limit_pullback", "allow_breakout_trigger", "max_pullback_bps"}:
        stages.append("wait_limit_trigger_plan")
        ui = "advanced"
    if subpath in {"min_score", "min_net_rr", "min_effective_rr", "require_liquidity_ok", "require_range_room_ok", "require_refresh_fresh", "require_direction_still_valid", "max_stop_bps", "max_stop_atr_mult", "min_reachable_reward_bps"}:
        ui = "primary"

    if status == "active" and value is False and subpath.endswith("enabled"):
        status = "disabled"
        direct_executable = False
    if status in {"disabled", "legacy"}:
        direct_executable = False

    effective = [line]
    if line == "without_micro":
        effective = ["without_micro", "strategy4", "strategy5", "strategy6"]
        notes.append("without_micro base trade plan currently gates strategy4/5/6 live executable lineage")
    if line in {"strategy5", "strategy6"}:
        effective = ["backtest"] if backtest_impact else []
        notes.append("live effectiveness must be audited because current adapter may not load this line config directly")

    return {
        "field_path": field_path,
        "section": "trade_plan_lines",
        "status": status,
        "used_by_strategies": [line],
        "effective_for_strategies": effective,
        "inherits_from": "without_micro" if line in {"strategy4", "strategy5", "strategy6"} else None,
        "business_stages": sorted(set(stages)),
        "direct_executable_impact": direct_executable,
        "paper_impact": paper_impact,
        "backtest_impact": backtest_impact,
        "sandbox_impact": sandbox_impact,
        "ui_recommendation": ui,
        "evidence": {
            "source_files": refs_for_field(field_path, source_index),
            "runtime_paths": [
                f"DATA/decisions/latest_trade_plan_{'without_micro' if line == 'without_micro' else line}.json",
                "DATA/runtime/latest_cross_strategy_funnel_snapshot.json",
            ],
            "latest_run_observation": line_runtime_observation(line, funnel),
            "top_reason_codes": trade_plan_reason_counts(line),
        },
        "notes": "; ".join(dict.fromkeys(notes)),
    }


def infer_field(field_path: str, value: Any, funnel: dict[str, Any], source_index: dict[str, list[str]]) -> dict[str, Any]:
    parts = field_path.split(".")
    section = parts[0] if parts else ""
    if len(parts) >= 2 and section == "trade_plan_lines" and parts[1] in TRADE_PLAN_LINES:
        return infer_trade_plan_leaf(field_path, parts[1], value, funnel, source_index)

    status = "active"
    used_by: list[str] = []
    effective: list[str] = []
    stages: list[str] = []
    direct_executable = False
    paper_impact = False
    backtest_impact = False
    sandbox_impact = True
    ui = "advanced"
    notes: list[str] = []

    if section in {"paths", "data_root", "schema_version", "source", "active_profile"}:
        status = "read_only"
        stages = ["runtime_contract"]
        ui = "hide_legacy"
        sandbox_impact = True
    elif section == "universe":
        used_by = STRATEGY_LINES.copy()
        effective = STRATEGY_LINES.copy()
        stages = ["scan_universe"]
        ui = "advanced"
    elif section == "freshness":
        used_by = STRATEGY_LINES.copy()
        effective = STRATEGY_LINES.copy()
        stages = ["scan_universe", "decision_refresh_liquidity", "trade_plan_executable"]
        direct_executable = "max_age" in field_path or "strict" in field_path
        ui = "advanced"
    elif section == "step2":
        used_by = STRATEGY_LINES.copy()
        effective = STRATEGY_LINES.copy()
        stages = ["scan_universe", "decision_refresh_liquidity"]
        ui = "advanced"
    elif section in {"market_entry_liquidity", "decision_refresh", "market_entry_direction"}:
        used_by = STRATEGY_LINES.copy()
        effective = STRATEGY_LINES.copy()
        stages = ["decision_refresh_liquidity", "trade_plan_executable"]
        direct_executable = True
        ui = "primary" if section == "market_entry_liquidity" else "advanced"
    elif section == "micro_router" or field_path.startswith("micro_daemon") or field_path.startswith("wait_until_ready"):
        used_by = ["micro_fast", "micro_full"]
        effective = ["micro_fast", "micro_full"]
        stages = ["micro_fast_full", "strategy_pipeline"]
        direct_executable = "require" in field_path or "target" in field_path or "ready" in field_path
        ui = "advanced"
    elif section == "strategy4":
        used_by = ["strategy4"]
        effective = ["strategy4"]
        stages = ["strategy4_observe"]
        direct_executable = field_path.endswith(".enabled") or "max_" in field_path or "observe" in field_path
        ui = "advanced"
    elif section == "strategy6":
        used_by = ["strategy6"]
        effective = ["strategy6"]
        stages = ["strategy6_evidence_decision_observe"]
        direct_executable = any(key in field_path for key in ("score", "deny", "wait", "range", "spread", "version", "action"))
        ui = "primary" if any(key in field_path for key in ("score", "strategy6_version")) else "advanced"
    elif section == "strategy_pipeline":
        used_by = STRATEGY_LINES.copy()
        effective = STRATEGY_LINES.copy()
        stages = ["strategy_pipeline"]
        direct_executable = "run_lines" in field_path or "require_refresh" in field_path or "micro." in field_path
        ui = "primary" if "run_lines" in field_path else "advanced"
    elif section == "paper":
        used_by = STRATEGY_LINES.copy()
        effective = STRATEGY_LINES.copy()
        stages = ["paper_order_fill"]
        paper_impact = True
        ui = "primary" if field_path in {"paper.lines", "paper.enabled"} else "advanced"
    elif section in {"trade_plan_risk", "position_sizing"}:
        used_by = STRATEGY_LINES.copy()
        effective = STRATEGY_LINES.copy()
        stages = ["trade_plan_executable", "paper_order_fill", "position_sizing"]
        direct_executable = True
        paper_impact = True
        ui = "primary" if section == "trade_plan_risk" else "advanced"
    elif section == "feishu":
        used_by = STRATEGY_LINES.copy()
        stages = ["notification"]
        paper_impact = False
        ui = "advanced"
        status = "disabled" if field_path == "feishu.enabled" and value is False else "active"
    elif section in {"sl_tp_planner", "risk_gate"}:
        used_by = ["legacy_step_pipeline"]
        stages = ["legacy_decision"]
        status = "legacy"
        ui = "hide_legacy"
        notes.append("legacy planner/gate section; current trade_plan_lines single-TP logic is the main executable path")
    elif section == "project_runtime" or section == "scheduler_5m" or section == "micro_daemon_cli":
        used_by = STRATEGY_LINES.copy()
        stages = ["runtime_daemon"]
        ui = "advanced"
    elif section == "pipeline":
        used_by = STRATEGY_LINES.copy()
        stages = ["legacy_pipeline_flags"]
        ui = "advanced"
    else:
        status = "unknown"
        stages = ["unknown"]
        ui = "advanced"

    if value is False and field_path.endswith(".enabled") and status == "active":
        status = "disabled"
    if status in {"disabled", "legacy"}:
        direct_executable = False

    return {
        "field_path": field_path,
        "section": section,
        "status": status,
        "used_by_strategies": used_by,
        "effective_for_strategies": effective,
        "inherits_from": None,
        "business_stages": sorted(set(stages)),
        "direct_executable_impact": bool(direct_executable),
        "paper_impact": bool(paper_impact),
        "backtest_impact": bool(backtest_impact),
        "sandbox_impact": bool(sandbox_impact),
        "ui_recommendation": ui,
        "evidence": {
            "source_files": refs_for_field(field_path, source_index),
            "runtime_paths": ["DATA/runtime/latest_cross_strategy_funnel_snapshot.json"],
            "latest_run_observation": {},
        },
        "notes": "; ".join(dict.fromkeys(notes)),
    }


def summarize(rows: list[dict[str, Any]], funnel: dict[str, Any]) -> dict[str, Any]:
    by_status = Counter(row["status"] for row in rows)
    by_ui = Counter(row["ui_recommendation"] for row in rows)
    by_stage: Counter[str] = Counter()
    direct = []
    paper_only = []
    legacy = []
    uncertain = []
    for row in rows:
        for stage in row.get("business_stages") or []:
            by_stage[stage] += 1
        if row.get("direct_executable_impact"):
            direct.append(row["field_path"])
        if row.get("paper_impact") and not row.get("direct_executable_impact"):
            paper_only.append(row["field_path"])
        if row.get("status") in {"legacy", "disabled"} or row.get("ui_recommendation") == "hide_legacy":
            legacy.append(row["field_path"])
        if row.get("status") == "unknown":
            uncertain.append(row["field_path"])
    strategy_effective: dict[str, list[str]] = {line: [] for line in STRATEGY_LINES}
    for row in rows:
        for line in row.get("effective_for_strategies") or []:
            if line in strategy_effective:
                strategy_effective[line].append(row["field_path"])

    return {
        "schema_version": "7.140-config-used-by-summary-v1",
        "generated_at": iso_now(),
        "config_path": str(CONFIG_PATH.relative_to(ROOT)).replace("\\", "/"),
        "field_count": len(rows),
        "status_counts": dict(by_status),
        "ui_recommendation_counts": dict(by_ui),
        "business_stage_counts": dict(by_stage),
        "direct_executable_field_count": len(direct),
        "paper_only_field_count": len(paper_only),
        "legacy_or_disabled_field_count": len(legacy),
        "unknown_field_count": len(uncertain),
        "selected_runtime": {
            "run_id": funnel.get("run_id"),
            "cycle_id": funnel.get("cycle_id"),
            "generated_at": funnel.get("generated_at"),
            "selected_lines": funnel.get("selected_lines"),
            "breakpoints": (funnel.get("summary") or {}).get("breakpoints"),
        },
        "top_direct_executable_fields": direct[:80],
        "paper_only_fields": paper_only[:80],
        "legacy_or_disabled_fields": legacy[:80],
        "unknown_fields": uncertain[:80],
        "effective_field_counts_by_strategy": {line: len(fields) for line, fields in strategy_effective.items()},
        "important_findings": [
            "strategy5 and strategy6 have dedicated YAML fields, but current live pipeline overlays their evidence onto the without_micro base trade plan.",
            "without_micro trade_plan_lines fields currently gate strategy4/5/6 live executable lineage through base plan inheritance.",
            "legacy trade_quality_gate and sl_tp_quality are present in trade_plan_lines but disabled in current config; V5 paper gate is a separate paper pre-order gate.",
            "tp_target_policy/fast exit remains active for backtest and can affect executable through reward/market-room checks when mode is fast_capped_rr or structure_or_capped_rr.",
        ],
    }


def write_report(rows: list[dict[str, Any]], summary: dict[str, Any], out_path: Path) -> None:
    direct = [row for row in rows if row.get("direct_executable_impact")]
    legacy = [row for row in rows if row.get("status") in {"legacy", "disabled"} or row.get("ui_recommendation") == "hide_legacy"]
    strategy5 = [row for row in rows if "strategy5" in (row.get("used_by_strategies") or [])]
    strategy6 = [row for row in rows if "strategy6" in (row.get("used_by_strategies") or [])]
    lines = [
        "# STEP7.140 Config Field Used-By Impact Map Audit",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- config_fields: `{summary['field_count']}`",
        f"- direct_executable_fields: `{summary['direct_executable_field_count']}`",
        f"- legacy_or_disabled_fields: `{summary['legacy_or_disabled_field_count']}`",
        f"- runtime_run_id: `{summary['selected_runtime'].get('run_id')}`",
        "",
        "## Executive Summary",
        "",
        "本次审计只生成字段影响图，不修改策略、不修改配置、不修改 UI。",
        "",
        "核心发现：",
        "",
        "1. `trade_plan_lines.without_micro.*` 不只影响策略1，也会通过 base trade plan 影响策略4/5/6 的 live executable lineage。",
        "2. 策略5/6 虽然有自己的 `trade_plan_lines.strategy5/strategy6.*` 配置，但当前 live adapter 主要是在 `without_micro` base plan 上 overlay evidence；这些字段在 live executable 上需要谨慎标注为“未完全直连”。",
        "3. 旧 `trade_quality_gate` 与 `sl_tp_quality` 当前 disabled/legacy；V5 gate 是 paper 前置 gate，不直接生成 trade plan executable。",
        "4. `tp_target_policy` / Fast Exit 仍然参与 backtest，并在 fast mode 下通过 reward、market room、spread reward ratio 影响 executable，不能直接删除。",
        "",
        "## Runtime Evidence",
        "",
        f"- selected_lines: `{summary['selected_runtime'].get('selected_lines')}`",
        f"- breakpoints: `{summary['selected_runtime'].get('breakpoints')}`",
        "",
        "## Status Counts",
        "",
        "```json",
        json.dumps(summary["status_counts"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## UI Recommendation Counts",
        "",
        "```json",
        json.dumps(summary["ui_recommendation_counts"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## Direct Executable Impact: Key Fields",
        "",
    ]
    for row in direct[:60]:
        lines.append(f"- `{row['field_path']}` -> strategies={row.get('effective_for_strategies')} stages={row.get('business_stages')} ui={row.get('ui_recommendation')}")
    lines.extend([
        "",
        "## Strategy5 / Strategy6 Config Caveat",
        "",
        "这些字段存在，但当前 live 链条要重点防止误读：",
        "",
    ])
    for row in (strategy5 + strategy6)[:80]:
        lines.append(f"- `{row['field_path']}` status={row.get('status')} effective={row.get('effective_for_strategies')} note={row.get('notes')}")
    lines.extend([
        "",
        "## Legacy / Disabled / Hide Candidates",
        "",
    ])
    for row in legacy[:80]:
        lines.append(f"- `{row['field_path']}` status={row.get('status')} ui={row.get('ui_recommendation')} note={row.get('notes')}")
    lines.extend([
        "",
        "## Output Files",
        "",
        "- `DATA/runtime/config_field_used_by_map.json`",
        "- `DATA/runtime/config_field_used_by_map_summary.json`",
        "",
        "## Config Page Simplification Recommendation",
        "",
        "- 主页面保留：profile、run lines、entry/executable gate、Exit/RR policy、risk/sizing、paper runtime。",
        "- 折叠到 Advanced：market_now_calibration 细项、micro lifecycle 细项、runtime daemon 细项。",
        "- 隐藏到 Legacy：旧 `trade_quality_gate`、`sl_tp_quality`、旧 planner/risk_gate。",
        "- 对 strategy5/6 配置加提示：当前 live executable 仍依赖 without_micro base trade plan；必须先修清 effective config read model，再做用户可编辑主面板。",
        "",
        "## DoD Check",
        "",
        "- [x] 每个主要 config 字段有 used-by / business stage 标注。",
        "- [x] 明确直接影响 executable 的字段。",
        "- [x] 明确 paper-only / legacy / disabled 字段。",
        "- [x] 明确 strategy5/6 当前 base-plan 继承风险。",
        "- [x] 未修改策略代码、未修改配置值、未修改 UI。",
        "",
    ])
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    fields = flatten(raw)
    source_index = build_source_index(source_files())
    funnel = read_json(ROOT / "DATA" / "runtime" / "latest_cross_strategy_funnel_snapshot.json")
    rows = [infer_field(path, value, funnel, source_index) for path, value in fields]
    rows = sorted(rows, key=lambda row: row["field_path"])
    summary = summarize(rows, funnel)

    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    map_path = RUNTIME_DIR / "config_field_used_by_map.json"
    summary_path = RUNTIME_DIR / "config_field_used_by_map_summary.json"
    report_path = REPORT_DIR / f"STEP7.140_config_field_used_by_impact_map_audit_{utc_stamp()}.md"

    map_payload = {
        "schema_version": "7.140-config-used-by-map-v1",
        "generated_at": summary["generated_at"],
        "config_path": summary["config_path"],
        "fields": rows,
    }
    map_path.write_text(json.dumps(map_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(rows, summary, report_path)

    print(json.dumps({"map": str(map_path), "summary": str(summary_path), "report": str(report_path), "field_count": len(rows)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
