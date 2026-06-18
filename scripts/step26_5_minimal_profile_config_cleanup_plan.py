from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "DATA" / "runtime" / "minimal_chain_config_profile_candidate.json"
VALIDATION_PATH = ROOT / "DATA" / "runtime" / "step26_4_minimal_profile_ab_delta_validation.json"
MATRIX_PATH = ROOT / "DATA" / "runtime" / "config_essentiality_matrix.json"
OUTPUT_JSON = ROOT / "DATA" / "runtime" / "step26_5_minimal_profile_config_cleanup_plan.json"
REPORT_DIR = ROOT / "docs" / "reports"

PROTECTED_CLASSES = {
    "core_required",
    "strategy_identity",
    "risk_required",
    "runtime_safety",
    "promotion_gate",
    "optional_filter",
    "unknown_requires_audit",
}


def iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def classify_action(row: dict[str, Any]) -> tuple[str, str, str, str]:
    field_path = str(row.get("field_path") or "")
    section = str(row.get("section") or "")
    if field_path in {"schema_version", "source", "active_profile"}:
        return (
            "profile_exclude_only",
            "non_trading_metadata",
            "low",
            "Keep parser compatibility in default.yaml; exclude from minimal profile and UI primary surfaces only.",
        )
    if section == "feishu":
        return (
            "ui_hide_only",
            "notification_disabled",
            "low_trading_medium_audit_delivery",
            "Hide from minimal trading profile while Feishu is disabled; do not delete secrets/notification schema from default config.",
        )
    if "trade_quality_gate" in field_path or "sl_tp_quality" in field_path:
        return (
            "default_cleanup_candidate",
            "disabled_legacy_quality_gate",
            "medium",
            "Eligible for future YAML cleanup only after parser fallback and STEP7.146/STEP7.149 delta validation.",
        )
    return (
        "manual_review",
        "unclassified_disable_candidate",
        "unknown",
        "Review manually before any UI/profile/default cleanup.",
    )


def build_plan() -> dict[str, Any]:
    profile = read_json(PROFILE_PATH)
    validation = read_json(VALIDATION_PATH)
    matrix = read_json(MATRIX_PATH)
    matrix_by_path = {
        str(row.get("field_path")): row
        for row in matrix.get("fields") or []
        if isinstance(row, dict) and row.get("field_path")
    }
    errors: list[str] = []
    if validation.get("status") != "ok":
        errors.append("step26_4_validation_not_ok")
    if validation.get("trade_plan_line_config_comparison", {}).get("line_delta_count") != 0:
        errors.append("line_config_delta_nonzero")
    if validation.get("protected_field_comparison", {}).get("delta_count") != 0:
        errors.append("protected_field_delta_nonzero")

    cleanup_items: list[dict[str, Any]] = []
    for removed in profile.get("removed_fields") or []:
        field_path = str(removed.get("field_path") or "")
        matrix_row = matrix_by_path.get(field_path, {})
        klass = str(matrix_row.get("necessity_class") or removed.get("necessity_class") or "")
        if klass in PROTECTED_CLASSES:
            errors.append(f"protected_class_in_cleanup:{field_path}:{klass}")
        action, group, risk, note = classify_action(removed)
        cleanup_items.append(
            {
                "field_path": field_path,
                "section": removed.get("section"),
                "current_value": removed.get("current_value"),
                "necessity_class": klass,
                "cleanup_action": action,
                "cleanup_group": group,
                "risk": risk,
                "default_yaml_action": "no_change" if action in {"ui_hide_only", "profile_exclude_only"} else "future_task_required",
                "ui_action": "hide_from_primary_minimal_profile",
                "profile_action": "exclude_from_minimal_profile_candidate",
                "rollback_source": removed.get("rollback_source"),
                "required_validation": removed.get("required_delta_test"),
                "application_note": note,
            }
        )

    by_action = Counter(item["cleanup_action"] for item in cleanup_items)
    by_group = Counter(item["cleanup_group"] for item in cleanup_items)
    default_cleanup = [item for item in cleanup_items if item["cleanup_action"] == "default_cleanup_candidate"]
    payload = {
        "schema_version": "step26.5-minimal-profile-config-cleanup-plan-v1",
        "generated_at": iso_now(),
        "task_id": "STEP26.5",
        "status": "ok" if not errors else "blocked",
        "source_profile": str(PROFILE_PATH.relative_to(ROOT)),
        "source_validation": str(VALIDATION_PATH.relative_to(ROOT)),
        "source_matrix": str(MATRIX_PATH.relative_to(ROOT)),
        "output_json": str(OUTPUT_JSON.relative_to(ROOT)),
        "default_yaml_changed": False,
        "cleanup_items": cleanup_items,
        "summary": {
            "cleanup_item_count": len(cleanup_items),
            "by_action": dict(by_action.most_common()),
            "by_group": dict(by_group.most_common()),
            "default_cleanup_candidate_count": len(default_cleanup),
            "ui_or_profile_only_count": sum(
                1 for item in cleanup_items if item["cleanup_action"] in {"ui_hide_only", "profile_exclude_only"}
            ),
        },
        "guardrails": {
            "optional_filter_included": any(item["necessity_class"] == "optional_filter" for item in cleanup_items),
            "protected_classes": sorted(PROTECTED_CLASSES),
            "actual_default_cleanup_requires_followup_task": True,
            "paper_risk_gate_runtime_safety_untouched": True,
        },
        "validation_errors": errors,
    }
    report = write_report(payload)
    payload["report"] = str(report.relative_to(ROOT))
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def write_report(payload: dict[str, Any]) -> Path:
    stamp = utc_stamp()
    path = REPORT_DIR / f"STEP26.5_minimal_profile_config_cleanup_plan_{stamp}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = payload["summary"]
    lines = [
        "# STEP26.5 Minimal Profile Config Cleanup Application Plan",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- status: `{payload['status']}`",
        f"- output_json: `{payload['output_json']}`",
        f"- default_yaml_changed: `{payload['default_yaml_changed']}`",
        "",
        "## Summary",
        "",
        f"- cleanup_item_count: `{summary['cleanup_item_count']}`",
        f"- ui_or_profile_only_count: `{summary['ui_or_profile_only_count']}`",
        f"- default_cleanup_candidate_count: `{summary['default_cleanup_candidate_count']}`",
        f"- validation_error_count: `{len(payload['validation_errors'])}`",
        "",
        "## By Action",
        "",
        "| action | count |",
        "| --- | ---: |",
    ]
    for action, count in summary["by_action"].items():
        lines.append(f"| `{action}` | {count} |")
    lines.extend(["", "## By Group", "", "| group | count |", "| --- | ---: |"])
    for group, count in summary["by_group"].items():
        lines.append(f"| `{group}` | {count} |")
    lines.extend(
        [
            "",
            "## Cleanup Items",
            "",
            "| field | action | group | default YAML | required validation |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for item in payload["cleanup_items"]:
        lines.append(
            f"| `{item['field_path']}` | `{item['cleanup_action']}` | `{item['cleanup_group']}` | "
            f"`{item['default_yaml_action']}` | {item['required_validation']} |"
        )
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- No `optional_filter` is included in this cleanup plan.",
            "- `paper.*`, `trade_plan_risk.*`, `position_sizing.*`, runtime safety, and V5 trade gate remain untouched.",
            "- This plan does not modify `default.yaml`.",
            "- Default YAML cleanup candidates require a separate implementation task and rollback diff.",
        ]
    )
    if payload["validation_errors"]:
        lines.extend(["", "## Validation Errors", ""])
        lines.extend(f"- `{error}`" for error in payload["validation_errors"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    payload = build_plan()
    print(
        json.dumps(
            {
                "status": payload["status"],
                "output_json": payload["output_json"],
                "report": payload["report"],
                "summary": payload["summary"],
                "validation_errors": payload["validation_errors"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if payload["validation_errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    sys.exit(main())
