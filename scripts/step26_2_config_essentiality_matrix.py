from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "laoma_signal_engine" / "config" / "default.yaml"
USED_BY_MAP = ROOT / "DATA" / "runtime" / "config_field_used_by_map.json"
SUMMARY_MAP = ROOT / "DATA" / "runtime" / "config_field_used_by_map_summary.json"
OUTPUT_JSON = ROOT / "DATA" / "runtime" / "config_essentiality_matrix.json"
REPORT_DIR = ROOT / "docs" / "reports"
V5_GATE_CONFIG = ROOT / "DATA" / "paper" / "v5_trade_gate_experiment.json"

LINES = ["without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"]

IDENTITY_KEYS = {
    "enabled",
    "inherit_from",
    "parameter_set_id",
    "min_score",
    "target_rr",
    "min_rr",
    "min_net_rr",
    "min_effective_rr",
    "min_reachable_reward_bps",
    "min_stop_bps",
    "preferred_stop_bps",
    "max_stop_bps",
    "stop_atr_mult",
    "max_stop_atr_mult",
    "first_tp_R",
    "max_loss_R_cap",
}

RISK_TOKENS = {
    "risk",
    "sizing",
    "margin",
    "notional",
    "planned_loss",
    "paper_fallback_notional",
    "leverage",
    "fee",
    "slippage",
    "fill",
    "position",
    "account",
    "loss_cap",
}

RUNTIME_TOKENS = {
    "fresh",
    "age",
    "stale",
    "ttl",
    "daemon",
    "watchdog",
    "heartbeat",
    "lock",
    "wait_until_ready",
    "health",
    "runtime",
    "interval",
    "concurrency",
    "catchup",
    "archive",
    "metadata",
}

OPTIONAL_FILTER_TOKENS = {
    "profile_gate",
    "market_now_calibration",
    "short_now_calibration",
    "weak_micro",
    "micro_consumption",
    "range_room",
    "bad_symbols",
    "bad_hours",
    "bad_sides",
    "quality_filter",
    "wait_confirm",
    "wait_pullback",
    "wait_rebound",
    "reverse_1m",
    "reverse_3m",
    "adverse_1m",
    "distance_from_mean",
    "max_chase",
    "entry_price_quality",
    "direction_context",
    "market_acceptance",
}

LEGACY_TOKENS = {
    "trade_quality_gate",
    "sl_tp_quality",
    "legacy",
    "shadow",
}


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            out.update(flatten(value, child))
    elif isinstance(obj, list):
        out[prefix] = obj
    else:
        out[prefix] = obj
    return out


def load_default_values() -> dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return flatten(data)


def leaf(field_path: str) -> str:
    return field_path.rsplit(".", 1)[-1]


def has_any(text: str, tokens: set[str]) -> bool:
    return any(token in text for token in tokens)


def business_step(field: dict[str, Any]) -> str:
    stages = field.get("business_stages") or []
    path = str(field.get("field_path") or "")
    section = str(field.get("section") or "")
    text = f"{section}.{path}".lower()
    if "paper" in section or "paper_order_fill" in stages:
        if "fill" in text or "fee" in text or "slippage" in text:
            return "fill / cost / latency"
        return "PaperEngine / order state machine"
    if section in {"trade_plan_risk", "position_sizing"} or has_any(text, RISK_TOKENS):
        return "risk / sizing"
    if "v5_trade_gate" in text or "trade_gate" in text:
        return "trade gate"
    if section == "freshness" or "decision_refresh" in section or "liquidity" in section:
        return "direction / refresh / liquidity"
    if section in {"step2", "micro_router"}:
        return "signal generation"
    if section == "strategy_pipeline" or has_any(text, RUNTIME_TOKENS):
        return "event clock / runtime safety"
    if section == "feishu" or "audit" in text or "report" in text:
        return "audit / report / UI"
    if section == "trade_plan_lines" or section.startswith("strategy"):
        return "trade plan line evaluator"
    return "market data / universe"


def environment_scope(field: dict[str, Any]) -> list[str]:
    envs: list[str] = []
    if field.get("backtest_impact"):
        envs.extend(["backtest", "replay"])
    if field.get("paper_impact"):
        envs.append("paper")
    if field.get("direct_executable_impact") or field.get("business_stages"):
        envs.append("live")
    if field.get("sandbox_impact"):
        envs.append("sandbox")
    if not envs:
        envs.append("config_only")
    return sorted(set(envs))


def classify(field: dict[str, Any], value: Any) -> tuple[str, str, bool, str, str, str]:
    path = str(field.get("field_path") or "")
    section = str(field.get("section") or "")
    status = str(field.get("status") or "unknown")
    ui = str(field.get("ui_recommendation") or "")
    path_l = path.lower()
    leaf_key = leaf(path)
    direct = bool(field.get("direct_executable_impact"))
    paper = bool(field.get("paper_impact"))
    backtest = bool(field.get("backtest_impact"))

    if section in {"paths", "data_root"}:
        return (
            "core_required",
            "keep",
            False,
            "disable only when replaced by an equivalent path/source contract",
            "missing source path can break signal generation or audit lineage",
            "high: source path and data availability comparability",
        )

    if section in {"risk_gate", "sl_tp_planner"}:
        return (
            "risk_required",
            "keep",
            False,
            "disable only after confirming the planner/gate has no current consumer",
            "may change SL/TP/risk semantics or historical compatibility",
            "medium/high: risk and exit-model comparability",
        )

    if section == "pipeline":
        return (
            "core_required",
            "keep",
            False,
            "disable only with a pipeline-stage equivalence report",
            "may skip factor/final-decision stages and change signal availability",
            "high: signal generation chain",
        )

    if section == "feishu":
        return (
            "disable_candidate",
            "exclude_from_minimal_profile",
            True,
            "safe for minimal trading profile while `feishu.enabled=false`; keep secrets out of profile output",
            "no trading decision delta expected; notification/audit delivery changes only if enabled",
            "low for trading equivalence, medium for notification audit coverage",
        )

    if path == "DATA.paper.v5_trade_gate_experiment":
        return (
            "promotion_gate",
            "keep_explicit_gate",
            False,
            "only disable when experiment intentionally runs without V5 gate",
            "gate pass/block lineage disappears; promotion evidence is not comparable",
            "preserves cross-environment promotion gate semantics",
        )

    if status in {"legacy", "read_only"} or ui == "hide_legacy" or has_any(path_l, LEGACY_TOKENS):
        safe = not direct and not paper and not backtest
        return (
            "disable_candidate" if safe else "legacy_shadow",
            "exclude_from_minimal_profile" if safe else "keep_as_shadow_documented",
            safe,
            "hide/remove only from minimal profile after no current consumer is confirmed",
            "no executable delta expected" if safe else "may change historical/shadow diagnostics",
            "comparison_only unless field-level mapping proves otherwise",
        )

    if section in {"paper", "trade_plan_risk", "position_sizing"}:
        if has_any(path_l, RUNTIME_TOKENS) and section == "paper":
            return (
                "runtime_safety",
                "keep",
                False,
                "do not disable while paper daemon or catch-up execution is active",
                "stale daemon/order lifecycle risk",
                "same_chain requires runtime safety to remain explicit",
            )
        return (
            "risk_required",
            "keep",
            False,
            "disable only in a dedicated risk-contract migration",
            "paper adapter may reject intents or change order/fill economics",
            "high: affects paper/backtest/live comparability",
        )

    if section in {"freshness", "strategy_pipeline", "project_runtime", "micro_daemon_state", "micro_daemon_cli", "wait_until_ready", "scheduler_5m"} or has_any(path_l, RUNTIME_TOKENS):
        return (
            "runtime_safety",
            "keep",
            False,
            "disable only for isolated debugging with stale-data report",
            "may allow stale inputs or change event clock",
            "high: event clock/data availability comparability",
        )

    if section == "trade_plan_lines" and leaf_key in IDENTITY_KEYS:
        return (
            "strategy_identity",
            "keep",
            False,
            "changing requires new parameter set / validation id",
            "changes executable count and selected trade distribution",
            "high: strategy identity changes",
        )

    if section in {"strategy4", "strategy5", "strategy6"} and leaf_key in IDENTITY_KEYS:
        return (
            "strategy_identity",
            "keep",
            False,
            "changing requires strategy-specific validation",
            "changes strategy identity or selected branch",
            "high: strategy identity changes",
        )

    if section in {"strategy4", "strategy6"} and leaf_key in {"db_path", "db"}:
        return (
            "runtime_safety",
            "keep",
            False,
            "disable only if the state store is replaced by an equivalent ledger",
            "observe/wait state may be lost",
            "high: state-machine comparability",
        )

    if section == "strategy4" and leaf_key in {"inherit_side", "rejudge_direction_each_attempt"}:
        return (
            "strategy_identity",
            "keep",
            False,
            "changing requires strategy4 observe validation",
            "changes observe/rejudge semantics",
            "medium/high: strategy4 identity",
        )

    if section == "strategy6" and any(token in path_l for token in ("first_tp", "loss_cap", "protect_after_mfe", "trail_after_mfe", "adaptive_exit")):
        return (
            "risk_required",
            "keep",
            False,
            "disable only in a strategy6 exit-model migration",
            "changes exit, loss cap, and realized R distribution",
            "high: fill/exit and Trade Quality comparability",
        )

    if section in {"step2", "micro_router", "decision_refresh", "market_entry_liquidity", "market_entry_direction"}:
        if has_any(path_l, OPTIONAL_FILTER_TOKENS):
            return (
                "optional_filter",
                "keep_for_now_audit_candidate",
                False,
                "can be relaxed only after reason-code delta report",
                "likely increases candidate/executable volume",
                "medium: upstream signal distribution changes",
            )
        return (
            "core_required",
            "keep",
            False,
            "disable only when replaced by equivalent source contract",
            "breaks or broadens signal generation / liquidity checks",
            "high: signal generation and data availability",
        )

    if has_any(path_l, OPTIONAL_FILTER_TOKENS):
        safe = not paper and not ("enabled" == leaf_key and value is True)
        return (
            "optional_filter",
            "audit_for_disable",
            safe,
            "disable after A/B paper-equivalent replay and reason-code delta report",
            "expected executable/order count increase; quality may degrade",
            "medium: field-mapped report required",
        )

    if status == "disabled" or (leaf_key == "enabled" and value is False):
        return (
            "disable_candidate",
            "exclude_from_minimal_profile",
            not direct and not paper,
            "confirm no active consumer before removing from minimal profile",
            "no runtime delta expected if truly disabled",
            "low if disabled state is verified",
        )

    if direct:
        return (
            "core_required",
            "keep",
            False,
            "disable only with field-level equivalence report",
            "changes executable true/false decisions",
            "high: executable comparability",
        )

    if backtest and not paper:
        return (
            "optional_filter",
            "audit_for_disable",
            True,
            "disable only after backtest/paper-equivalent delta report",
            "changes historical selection or replay metrics",
            "medium: backtest-to-paper mapping required",
        )

    return (
        "unknown_requires_audit",
        "keep_until_classified",
        False,
        "manual owner review required",
        "unknown",
        "unknown",
    )


def enrich_field(field: dict[str, Any], values: dict[str, Any]) -> dict[str, Any]:
    path = str(field.get("field_path") or "")
    value = values.get(path)
    klass, action, safe, condition, delta, eq = classify(field, value)
    return {
        "field_path": path,
        "section": field.get("section"),
        "strategy_lines": field.get("effective_for_strategies") or field.get("used_by_strategies") or [],
        "used_by_strategies": field.get("used_by_strategies") or [],
        "business_step": business_step(field),
        "business_stages": field.get("business_stages") or [],
        "environment_scope": environment_scope(field),
        "necessity_class": klass,
        "default_action": action,
        "safe_to_disable": bool(safe),
        "disable_condition": condition,
        "expected_reason_code_delta": delta,
        "equivalence_impact": eq,
        "owner_task": "STEP26.2",
        "status": field.get("status"),
        "ui_recommendation": field.get("ui_recommendation"),
        "direct_executable_impact": bool(field.get("direct_executable_impact")),
        "paper_impact": bool(field.get("paper_impact")),
        "backtest_impact": bool(field.get("backtest_impact")),
        "sandbox_impact": bool(field.get("sandbox_impact")),
        "default_value": value,
        "notes": field.get("notes") or "",
    }


def gate_pseudo_field() -> dict[str, Any] | None:
    if not V5_GATE_CONFIG.exists():
        return None
    gate = read_json(V5_GATE_CONFIG, {})
    rules = gate.get("rules") if isinstance(gate, dict) else {}
    lines = sorted(str(line) for line in rules.keys()) if isinstance(rules, dict) else []
    base = {
        "field_path": "DATA.paper.v5_trade_gate_experiment",
        "section": "trade_gate",
        "status": "active" if gate.get("enabled") else "disabled",
        "used_by_strategies": lines,
        "effective_for_strategies": lines,
        "business_stages": ["paper_v5_gate", "promotion_gate"],
        "direct_executable_impact": False,
        "paper_impact": True,
        "backtest_impact": True,
        "sandbox_impact": True,
        "ui_recommendation": "primary",
        "notes": f"experiment_id={gate.get('experiment_id')}; mode={gate.get('mode')}",
    }
    enriched = enrich_field(base, {"DATA.paper.v5_trade_gate_experiment": gate.get("enabled")})
    enriched["gate_config"] = {
        "path": str(V5_GATE_CONFIG.relative_to(ROOT)),
        "experiment_id": gate.get("experiment_id"),
        "paper_epoch_id": gate.get("paper_epoch_id"),
        "mode": gate.get("mode"),
        "enabled": gate.get("enabled"),
        "lines": lines,
    }
    return enriched


def summarize(fields: list[dict[str, Any]]) -> dict[str, Any]:
    by_class = Counter(row["necessity_class"] for row in fields)
    by_section = Counter(row["section"] for row in fields)
    by_step = Counter(row["business_step"] for row in fields)
    by_line: dict[str, Counter[str]] = {line: Counter() for line in LINES}
    for row in fields:
        for line in row.get("strategy_lines") or []:
            if line in by_line:
                by_line[line][row["necessity_class"]] += 1
    return {
        "field_count": len(fields),
        "by_necessity_class": dict(by_class.most_common()),
        "by_section_top20": dict(by_section.most_common(20)),
        "by_business_step": dict(by_step.most_common()),
        "by_strategy_line": {line: dict(counter.most_common()) for line, counter in by_line.items()},
        "safe_to_disable_count": sum(1 for row in fields if row["safe_to_disable"]),
        "disable_candidate_count": sum(1 for row in fields if row["necessity_class"] == "disable_candidate"),
        "unknown_requires_audit_count": sum(1 for row in fields if row["necessity_class"] == "unknown_requires_audit"),
    }


def minimal_profile(fields: list[dict[str, Any]]) -> dict[str, Any]:
    keep_classes = {"core_required", "strategy_identity", "risk_required", "promotion_gate", "runtime_safety"}
    disable_rows = [
        row
        for row in fields
        if row["necessity_class"] == "disable_candidate" or row["default_action"] in {"exclude_from_minimal_profile", "audit_for_disable"}
    ]
    keep_rows = [row for row in fields if row["necessity_class"] in keep_classes]
    by_group: dict[str, list[str]] = defaultdict(list)
    for row in disable_rows:
        by_group[str(row["section"])].append(row["field_path"])
    return {
        "profile_id": "minimal_chain_profile_draft_step26_2",
        "status": "draft_no_config_write",
        "keep_necessity_classes": sorted(keep_classes),
        "keep_field_count": len(keep_rows),
        "disable_or_audit_field_count": len(disable_rows),
        "keep_sections": sorted({str(row["section"]) for row in keep_rows}),
        "first_disable_candidate_groups": {key: values[:20] for key, values in sorted(by_group.items())[:20]},
        "hard_rules": [
            "Do not disable runtime_safety fields as strategy filters.",
            "Do not disable risk_required fields without a risk-contract migration.",
            "Keep V5 trade gate as promotion_gate, not as an optional filter.",
            "No config value is changed by STEP26.2.",
        ],
    }


def top_rows(fields: list[dict[str, Any]], klass: str, limit: int = 20) -> list[dict[str, Any]]:
    rows = [row for row in fields if row["necessity_class"] == klass]
    rows.sort(key=lambda row: (str(row["section"]), row["field_path"]))
    return rows[:limit]


def write_report(payload: dict[str, Any]) -> Path:
    stamp = utc_stamp()
    path = REPORT_DIR / f"STEP26.2_config_essentiality_matrix_{stamp}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = payload["summary"]
    lines = [
        "# STEP26.2 Config Essentiality Matrix",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- config_path: `{payload['config_path']}`",
        f"- source_used_by_map: `{payload['source_used_by_map']}`",
        f"- output_json: `{payload['output_json']}`",
        f"- field_count: `{summary['field_count']}`",
        f"- safe_to_disable_count: `{summary['safe_to_disable_count']}`",
        f"- disable_candidate_count: `{summary['disable_candidate_count']}`",
        f"- unknown_requires_audit_count: `{summary['unknown_requires_audit_count']}`",
        "",
        "## Judgment",
        "",
        "- STEP26.2 did not change `default.yaml` or runtime config values.",
        "- The matrix separates core strategy identity, risk/sizing, runtime safety, promotion gates, optional filters, legacy shadow fields, and disable candidates.",
        "- Disable candidates are profile candidates only; actual shutdown requires a follow-up task and paper-equivalent delta report.",
        "",
        "## Necessity Class Summary",
        "",
        "| class | count |",
        "| --- | ---: |",
    ]
    for key, value in summary["by_necessity_class"].items():
        lines.append(f"| `{key}` | {value} |")
    lines.extend(["", "## Business Step Summary", "", "| step | count |", "| --- | ---: |"])
    for key, value in summary["by_business_step"].items():
        lines.append(f"| `{key}` | {value} |")
    lines.extend(["", "## Strategy Line Summary", "", "| strategy_line | class_counts |", "| --- | --- |"])
    for line, counts in summary["by_strategy_line"].items():
        lines.append(f"| `{line}` | `{counts}` |")
    lines.extend(["", "## First Disable Candidates", "", "| field | section | reason |", "| --- | --- | --- |"])
    for row in top_rows(payload["fields"], "disable_candidate", 40):
        lines.append(f"| `{row['field_path']}` | `{row['section']}` | {row['disable_condition']} |")
    lines.extend(["", "## Optional Filters To Audit", "", "| field | section | expected delta |", "| --- | --- | --- |"])
    for row in top_rows(payload["fields"], "optional_filter", 40):
        lines.append(f"| `{row['field_path']}` | `{row['section']}` | {row['expected_reason_code_delta']} |")
    lines.extend(
        [
            "",
            "## Minimal Profile Draft",
            "",
            f"- profile_id: `{payload['minimal_chain_profile']['profile_id']}`",
            f"- keep_field_count: `{payload['minimal_chain_profile']['keep_field_count']}`",
            f"- disable_or_audit_field_count: `{payload['minimal_chain_profile']['disable_or_audit_field_count']}`",
            f"- keep_sections: `{payload['minimal_chain_profile']['keep_sections']}`",
            "",
            "## Next Steps",
            "",
            "1. Review `disable_candidate` and `optional_filter` groups by strategy line.",
            "2. Create a follow-up task to produce a non-default minimal profile file.",
            "3. Run STEP7.146/STEP7.149 paper-equivalent delta tests before disabling any field in production defaults.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def build_payload() -> dict[str, Any]:
    used_by = read_json(USED_BY_MAP, {})
    summary_map = read_json(SUMMARY_MAP, {})
    values = load_default_values()
    source_fields = used_by.get("fields") or []
    if not isinstance(source_fields, list):
        raise RuntimeError("config_field_used_by_map.fields must be a list")
    fields = [enrich_field(field, values) for field in source_fields if isinstance(field, dict)]
    gate = gate_pseudo_field()
    if gate:
        fields.append(gate)
    payload = {
        "schema_version": "step26.2-config-essentiality-matrix-v1",
        "generated_at": iso_now(),
        "task_id": "STEP26.2",
        "config_path": str(CONFIG_PATH.relative_to(ROOT)),
        "source_used_by_map": str(USED_BY_MAP.relative_to(ROOT)),
        "source_used_by_map_schema": used_by.get("schema_version"),
        "source_used_by_map_generated_at": used_by.get("generated_at"),
        "source_summary": summary_map,
        "output_json": str(OUTPUT_JSON.relative_to(ROOT)),
        "fields": fields,
        "summary": summarize(fields),
        "minimal_chain_profile": minimal_profile(fields),
        "config_write_policy": "no_default_yaml_change",
    }
    report = write_report(payload)
    payload["report"] = str(report.relative_to(ROOT))
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def main() -> None:
    payload = build_payload()
    print(json.dumps({"output_json": payload["output_json"], "report": payload["report"], "summary": payload["summary"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    sys.exit(main())
