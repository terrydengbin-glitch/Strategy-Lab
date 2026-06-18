from __future__ import annotations

import copy
import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "laoma_signal_engine" / "config" / "default.yaml"
MATRIX_PATH = ROOT / "DATA" / "runtime" / "config_essentiality_matrix.json"
OUTPUT_JSON = ROOT / "DATA" / "runtime" / "minimal_chain_config_profile_candidate.json"
REPORT_DIR = ROOT / "docs" / "reports"

FORBIDDEN_EXCLUDE_PREFIXES = (
    "paper.",
    "trade_plan_risk.",
    "position_sizing.",
    "freshness.",
    "strategy_pipeline.",
    "project_runtime.",
    "micro_daemon_state.",
    "micro_daemon_cli.",
    "wait_until_ready.",
    "DATA.paper.v5_trade_gate_experiment",
)

FORBIDDEN_EXCLUDE_CLASSES = {
    "core_required",
    "strategy_identity",
    "risk_required",
    "runtime_safety",
    "promotion_gate",
    "optional_filter",
    "unknown_requires_audit",
}

PROTECTED_KEEP_CLASSES = {
    "core_required",
    "strategy_identity",
    "risk_required",
    "runtime_safety",
    "promotion_gate",
    "optional_filter",
    "legacy_shadow",
    "unknown_requires_audit",
}


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} must contain a YAML mapping")
    return data


def lookup(root: dict[str, Any], field_path: str) -> tuple[bool, Any]:
    cur: Any = root
    for part in field_path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return False, None
    return True, cur


def remove_path(root: dict[str, Any], field_path: str) -> bool:
    parts = field_path.split(".")
    cur: Any = root
    for part in parts[:-1]:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return False
    if isinstance(cur, dict) and parts[-1] in cur:
        del cur[parts[-1]]
        return True
    return False


def prune_empty_dicts(obj: Any) -> Any:
    if isinstance(obj, dict):
        pruned: dict[str, Any] = {}
        for key, value in obj.items():
            child = prune_empty_dicts(value)
            if isinstance(child, dict) and not child:
                continue
            pruned[key] = child
        return pruned
    if isinstance(obj, list):
        return [prune_empty_dicts(item) for item in obj]
    return obj


def is_forbidden_exclude(row: dict[str, Any]) -> bool:
    field_path = str(row.get("field_path") or "")
    klass = str(row.get("necessity_class") or "")
    if klass in FORBIDDEN_EXCLUDE_CLASSES:
        return True
    return field_path.startswith(FORBIDDEN_EXCLUDE_PREFIXES)


def required_delta_test(row: dict[str, Any]) -> str:
    field_path = str(row.get("field_path") or "")
    section = str(row.get("section") or "")
    if section == "feishu":
        return "notification-only smoke; no trading delta expected while feishu.enabled=false"
    if "trade_quality_gate" in field_path or "sl_tp_quality" in field_path:
        return "STEP7.146 targeted paper-equivalent replay plus reason-code delta before production removal"
    if field_path in {"schema_version", "source", "active_profile"}:
        return "config parser / effective config preview smoke"
    return "manual owner review"


def build_disable_batches(excluded: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in excluded:
        field_path = str(row.get("field_path") or "")
        if field_path.startswith("feishu."):
            groups["batch_1_notification_disabled"].append(row)
        elif "trade_quality_gate" in field_path or "sl_tp_quality" in field_path:
            groups["batch_2_legacy_quality_gates_disabled"].append(row)
        else:
            groups["batch_0_non_trading_metadata"].append(row)
    batches: list[dict[str, Any]] = []
    order = [
        "batch_0_non_trading_metadata",
        "batch_1_notification_disabled",
        "batch_2_legacy_quality_gates_disabled",
    ]
    for batch_id in order:
        rows = groups.get(batch_id, [])
        if not rows:
            continue
        if batch_id == "batch_0_non_trading_metadata":
            test = "config parser / effective config preview smoke"
            risk = "low"
        elif batch_id == "batch_1_notification_disabled":
            test = "notification config smoke; trading chain should remain identical"
            risk = "low_trading_medium_audit_delivery"
        else:
            test = "STEP7.146 targeted paper-equivalent A/B; verify executable/order/gate deltas"
            risk = "medium"
        batches.append(
            {
                "batch_id": batch_id,
                "status": "planned_not_applied",
                "risk": risk,
                "field_count": len(rows),
                "fields": [row["field_path"] for row in rows],
                "required_delta_test": test,
                "rollback_source": str(CONFIG_PATH.relative_to(ROOT)),
            }
        )
    return batches


def validate_candidate(excluded: list[dict[str, Any]], retained_rows: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    for row in excluded:
        if str(row.get("necessity_class")) != "disable_candidate":
            errors.append(f"excluded_non_disable_candidate:{row.get('field_path')}:{row.get('necessity_class')}")
        if is_forbidden_exclude(row):
            errors.append(f"excluded_forbidden_field:{row.get('field_path')}:{row.get('necessity_class')}")
    retained_by_path = {str(row.get("field_path")): row for row in retained_rows}
    for required in (
        "paper.enabled",
        "paper.db_path",
        "paper.daemon.enabled",
        "trade_plan_risk.paper_fallback_notional_allowed",
        "position_sizing.enabled",
        "DATA.paper.v5_trade_gate_experiment",
    ):
        row = retained_by_path.get(required)
        if not row:
            errors.append(f"required_field_not_retained:{required}")
        elif row.get("necessity_class") not in PROTECTED_KEEP_CLASSES:
            errors.append(f"required_field_wrong_class:{required}:{row.get('necessity_class')}")
    return errors


def write_report(payload: dict[str, Any]) -> Path:
    stamp = utc_stamp()
    path = REPORT_DIR / f"STEP26.3_minimal_chain_config_profile_candidate_{stamp}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = payload["summary"]
    lines = [
        "# STEP26.3 Minimal Chain Config Profile Candidate",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- status: `{payload['status']}`",
        f"- profile_id: `{payload['profile_id']}`",
        f"- output_json: `{payload['output_json']}`",
        f"- source_config: `{payload['source_config']}`",
        f"- source_matrix: `{payload['source_matrix']}`",
        f"- default_yaml_changed: `{payload['default_yaml_changed']}`",
        "",
        "## Summary",
        "",
        f"- original_leaf_field_count: `{summary['original_leaf_field_count']}`",
        f"- candidate_removed_field_count: `{summary['candidate_removed_field_count']}`",
        f"- retained_matrix_field_count: `{summary['retained_matrix_field_count']}`",
        f"- validation_error_count: `{len(payload['validation_errors'])}`",
        "",
        "## Removed By Section",
        "",
        "| section | count |",
        "| --- | ---: |",
    ]
    for section, count in summary["removed_by_section"].items():
        lines.append(f"| `{section}` | {count} |")
    lines.extend(["", "## Disable Batches", "", "| batch | fields | risk | required test |", "| --- | ---: | --- | --- |"])
    for batch in payload["disable_batches"]:
        lines.append(
            f"| `{batch['batch_id']}` | {batch['field_count']} | `{batch['risk']}` | {batch['required_delta_test']} |"
        )
    lines.extend(["", "## Removed Fields", "", "| field | section | current value | delta test |", "| --- | --- | --- | --- |"])
    for row in payload["removed_fields"]:
        value = json.dumps(row.get("current_value"), ensure_ascii=False)
        if len(value) > 90:
            value = value[:87] + "..."
        lines.append(f"| `{row['field_path']}` | `{row['section']}` | `{value}` | {row['required_delta_test']} |")
    lines.extend(
        [
            "",
            "## Protected Fields",
            "",
            "- `paper.*`, `trade_plan_risk.*`, `position_sizing.*`, V5 trade gate, freshness, runtime safety, strategy identity, and optional filters were not removed.",
            "- This candidate is not production config and is not auto-loaded.",
            "- Any real shutdown must be done by a follow-up task with STEP7.146/STEP7.149 delta evidence.",
        ]
    )
    if payload["validation_errors"]:
        lines.extend(["", "## Validation Errors", ""])
        lines.extend(f"- `{err}`" for err in payload["validation_errors"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def build_payload() -> dict[str, Any]:
    config = read_yaml(CONFIG_PATH)
    matrix = read_json(MATRIX_PATH)
    fields = matrix.get("fields") or []
    if not isinstance(fields, list):
        raise RuntimeError("config_essentiality_matrix.fields must be a list")

    candidate_config = copy.deepcopy(config)
    removed_fields: list[dict[str, Any]] = []
    retained_rows: list[dict[str, Any]] = []
    skipped_missing: list[str] = []

    for row in fields:
        if not isinstance(row, dict):
            continue
        field_path = str(row.get("field_path") or "")
        if field_path.startswith("DATA."):
            retained_rows.append(row)
            continue
        if row.get("necessity_class") == "disable_candidate" and not is_forbidden_exclude(row):
            exists, current_value = lookup(config, field_path)
            removed = remove_path(candidate_config, field_path) if exists else False
            if not exists:
                skipped_missing.append(field_path)
            removed_fields.append(
                {
                    "field_path": field_path,
                    "section": row.get("section"),
                    "current_value": current_value,
                    "necessity_class": row.get("necessity_class"),
                    "disable_reason": row.get("disable_condition"),
                    "rollback_source": str(CONFIG_PATH.relative_to(ROOT)),
                    "required_delta_test": required_delta_test(row),
                    "removed_from_candidate": removed,
                    "safe_to_disable": bool(row.get("safe_to_disable")),
                    "equivalence_impact": row.get("equivalence_impact"),
                }
            )
        else:
            retained_rows.append(row)

    candidate_config = prune_empty_dicts(candidate_config)
    removed_by_section = Counter(str(row.get("section")) for row in removed_fields)
    retained_by_class = Counter(str(row.get("necessity_class")) for row in retained_rows)
    validation_errors = validate_candidate(removed_fields, retained_rows)

    payload = {
        "schema_version": "step26.3-minimal-chain-config-profile-candidate-v1",
        "generated_at": iso_now(),
        "task_id": "STEP26.3",
        "status": "ok" if not validation_errors else "validation_failed",
        "profile_id": "minimal_chain_config_profile_candidate_step26_3",
        "profile_status": "candidate_not_applied",
        "source_config": str(CONFIG_PATH.relative_to(ROOT)),
        "source_matrix": str(MATRIX_PATH.relative_to(ROOT)),
        "output_json": str(OUTPUT_JSON.relative_to(ROOT)),
        "default_yaml_changed": False,
        "config_write_policy": "no_default_yaml_change",
        "candidate_config": candidate_config,
        "removed_fields": removed_fields,
        "retained_summary": {
            "retained_by_necessity_class": dict(retained_by_class.most_common()),
            "retained_protected_classes": sorted(PROTECTED_KEEP_CLASSES),
        },
        "disable_batches": build_disable_batches(removed_fields),
        "skipped_missing_fields": skipped_missing,
        "validation_errors": validation_errors,
        "summary": {
            "original_leaf_field_count": len([row for row in fields if isinstance(row, dict) and not str(row.get("field_path") or "").startswith("DATA.")]),
            "candidate_removed_field_count": len(removed_fields),
            "retained_matrix_field_count": len(retained_rows),
            "removed_by_section": dict(removed_by_section.most_common()),
        },
    }
    report = write_report(payload)
    payload["report"] = str(report.relative_to(ROOT))
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def main() -> None:
    payload = build_payload()
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
