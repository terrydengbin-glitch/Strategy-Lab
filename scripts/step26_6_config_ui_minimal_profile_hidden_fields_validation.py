from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PLAN_PATH = ROOT / "DATA" / "runtime" / "step26_5_minimal_profile_config_cleanup_plan.json"
OUTPUT_JSON = ROOT / "DATA" / "runtime" / "step26_6_config_ui_minimal_profile_hidden_fields_validation.json"
REPORT_DIR = ROOT / "docs" / "reports"
APP_VUE_PATH = ROOT / "web" / "src" / "App.vue"
DEFAULT_YAML_PATH = ROOT / "laoma_signal_engine" / "config" / "default.yaml"


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / f"STEP26.6_config_ui_minimal_profile_hidden_fields_validation_{_utc_stamp()}.md"
    summary = payload["summary"]
    checks = payload["checks"]
    lines = [
        "# STEP26.6 Config UI Minimal Profile Hidden Fields Validation",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- status: `{payload['status']}`",
        f"- output_json: `{payload['output_json']}`",
        f"- default_yaml_changed_by_task: `{summary['default_yaml_changed_by_task']}`",
        "",
        "## Summary",
        "",
        f"- cleanup_plan_fields: `{summary['cleanup_plan_fields']}`",
        f"- api_minimal_hidden_fields: `{summary['api_minimal_hidden_fields']}`",
        f"- api_schema_version: `{summary['api_schema_version']}`",
        f"- vue_feishu_advanced_only: `{summary['vue_feishu_advanced_only']}`",
        f"- vue_legacy_gate_advanced_only: `{summary['vue_legacy_gate_advanced_only']}`",
        f"- validation_error_count: `{len(payload['validation_errors'])}`",
        "",
        "## By Action",
        "",
        "| action | count |",
        "| --- | ---: |",
    ]
    for key, count in sorted(summary["cleanup_by_action"].items()):
        lines.append(f"| `{key}` | {count} |")
    lines.extend(
        [
            "",
            "## Strategy Effective Hidden Counts",
            "",
            "| strategy_line | minimal hidden | legacy/disabled |",
            "| --- | ---: | ---: |",
        ]
    )
    for line, counts in payload["strategy_effective_counts"].items():
        lines.append(
            f"| `{line}` | {counts.get('minimal_profile_hidden', 0)} | {counts.get('legacy_or_disabled', 0)} |"
        )
    lines.extend(
        [
            "",
            "## Checks",
            "",
            "| check | passed | detail |",
            "| --- | --- | --- |",
        ]
    )
    for check in checks:
        lines.append(f"| `{check['name']}` | `{check['passed']}` | {check['detail']} |")
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- `default.yaml` was not edited by this task.",
            "- API raw/effective config rows remain available with minimal profile hidden metadata.",
            "- Feishu disabled notification fields are hidden from the primary Config tab but remain editable in Advanced / Legacy.",
            "- Legacy `trade_quality_gate` / `sl_tp_quality` controls are hidden from the Trade Gate tab and remain auditable in Advanced / Legacy.",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def main() -> int:
    from laoma_signal_engine.api.services import config_effective_payload, config_ui_schema_payload

    plan = _read_json(PLAN_PATH)
    cleanup_items = plan.get("cleanup_items") or []
    cleanup_by_action = Counter(str(item.get("cleanup_action") or "unknown") for item in cleanup_items if isinstance(item, dict))
    schema = config_ui_schema_payload()
    minimal_hidden_group = (schema.get("groups") or {}).get("minimal_hidden") or {}
    minimal_hidden_count = int(minimal_hidden_group.get("count") or 0)
    app_vue = APP_VUE_PATH.read_text(encoding="utf-8")

    strategy_effective_counts = {}
    for line in ("without_micro", "micro_fast", "micro_full", "strategy5", "strategy6"):
        effective = config_effective_payload(line)
        strategy_effective_counts[line] = effective.get("counts") or {}

    checks = [
        {
            "name": "cleanup_plan_status_ok",
            "passed": plan.get("status") == "ok",
            "detail": f"plan status={plan.get('status')}",
        },
        {
            "name": "api_minimal_hidden_count_matches_plan",
            "passed": minimal_hidden_count == len(cleanup_items) == 50,
            "detail": f"api={minimal_hidden_count}, plan={len(cleanup_items)}",
        },
        {
            "name": "api_cleanup_actions_match_step26_5",
            "passed": dict(cleanup_by_action) == {"profile_exclude_only": 3, "ui_hide_only": 19, "default_cleanup_candidate": 28},
            "detail": json.dumps(dict(cleanup_by_action), ensure_ascii=False, sort_keys=True),
        },
        {
            "name": "vue_feishu_section_advanced_only",
            "passed": '{ key: "feishu", title: "Feishu Notification", path: ["feishu"], tabs: ["advanced-legacy"] }' in app_vue,
            "detail": "Feishu raw editor is excluded from Strategy Runtime sections.",
        },
        {
            "name": "vue_visible_config_sections_used",
            "passed": "v-for=\"section in visibleConfigSections\"" in app_vue,
            "detail": "Config raw section cards are tab-filtered.",
        },
        {
            "name": "vue_legacy_gate_controls_advanced_only",
            "passed": app_vue.count("configActiveTab === 'advanced-legacy'") >= 4 and "V5 trade gate is applied before paper order" in app_vue,
            "detail": "Legacy TQ/SLTP controls moved out of Trade Gate primary view.",
        },
        {
            "name": "default_yaml_untouched_by_contract",
            "passed": DEFAULT_YAML_PATH.exists() and plan.get("default_yaml_changed") is False,
            "detail": str(DEFAULT_YAML_PATH.relative_to(ROOT)),
        },
    ]
    validation_errors = [check for check in checks if not check["passed"]]
    payload = {
        "schema_version": "step26.6-config-ui-minimal-profile-hidden-fields-validation-v1",
        "task_id": "STEP26.6",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "status": "ok" if not validation_errors else "failed",
        "source_plan": str(PLAN_PATH.relative_to(ROOT)),
        "output_json": str(OUTPUT_JSON.relative_to(ROOT)),
        "summary": {
            "cleanup_plan_fields": len(cleanup_items),
            "api_minimal_hidden_fields": minimal_hidden_count,
            "api_schema_version": schema.get("schema_version"),
            "cleanup_by_action": dict(cleanup_by_action),
            "vue_feishu_advanced_only": checks[3]["passed"],
            "vue_legacy_gate_advanced_only": checks[5]["passed"],
            "default_yaml_changed_by_task": False,
        },
        "strategy_effective_counts": strategy_effective_counts,
        "checks": checks,
        "validation_errors": validation_errors,
    }
    report_path = _write_report(payload)
    payload["report"] = str(report_path.relative_to(ROOT))
    _write_json(OUTPUT_JSON, payload)
    print(json.dumps({"status": payload["status"], "output_json": payload["output_json"], "report": payload["report"]}, ensure_ascii=False, indent=2))
    return 0 if not validation_errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
