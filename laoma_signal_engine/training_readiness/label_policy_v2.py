"""STEP29.17 cost-aware training label policy v2 contract.

This module is intentionally pure: it validates JSON payloads already present
in the P29 sidecar and does not read or write any business/source DB.
"""

from __future__ import annotations

from typing import Any


LABEL_POLICY_VERSION = "cost_aware_quality_label_v2"

LABEL_COVERAGE_COMPLETE = "complete"
LABEL_COVERAGE_NEEDS_REVIEW = "needs_review"
LABEL_COVERAGE_COUNTERFACTUAL_UNAVAILABLE = "counterfactual_unavailable"
LABEL_COVERAGE_SOURCE_COST_MISSING = "source_cost_missing"
LABEL_COVERAGE_EXCLUDED_FROM_TRAINING = "excluded_from_training"

LABEL_COVERAGE_STATUSES = {
    LABEL_COVERAGE_COMPLETE,
    LABEL_COVERAGE_NEEDS_REVIEW,
    LABEL_COVERAGE_COUNTERFACTUAL_UNAVAILABLE,
    LABEL_COVERAGE_SOURCE_COST_MISSING,
    LABEL_COVERAGE_EXCLUDED_FROM_TRAINING,
}

REQUIRED_LABEL_FIELDS = {
    "label_policy_version",
    "label_coverage_status",
    "label_observation_end_time",
    "reason_codes",
    "quality_label",
    "bad_trade_flag",
}

REQUIRED_COST_FIELDS = {
    "false_allow_cost",
    "false_block_cost",
    "fee_bps",
    "realized_slippage_bps",
}

FORBIDDEN_DECISION_TIME_FIELDS = {
    "false_allow_cost",
    "false_block_cost",
    "realized_slippage_bps",
    "net_R",
    "MFE_R",
    "MAE_R",
    "exit_reason",
    "root_cause_label",
    "quality_label",
}


def _is_missing(value: Any) -> bool:
    return value is None or value == ""


def _as_float(value: Any) -> float | None:
    try:
        if _is_missing(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def merged_label_payload(label_json: dict[str, Any] | None, post_trade_outcome_json: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a read-only merged view where label fields override outcome fields."""

    out = dict(post_trade_outcome_json or {})
    out.update(dict(label_json or {}))
    return out


def walk_keys(value: Any, prefix: str = "") -> list[str]:
    out: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            out.append(path)
            out.extend(walk_keys(item, path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            out.extend(walk_keys(item, f"{prefix}[{idx}]"))
    return out


def decision_time_forbidden_violations(decision_time_input_json: Any) -> list[dict[str, str]]:
    violations: list[dict[str, str]] = []
    for path in walk_keys(decision_time_input_json):
        leaf = path.split(".")[-1].split("[")[0]
        if leaf in FORBIDDEN_DECISION_TIME_FIELDS:
            violations.append({"field": leaf, "field_path": path, "reason": "post_trade_or_cost_label_in_decision_time_input"})
    return violations


def validate_cost_aware_label_v2(
    label_json: dict[str, Any] | None,
    *,
    post_trade_outcome_json: dict[str, Any] | None = None,
    decision_time_input_json: Any | None = None,
) -> dict[str, Any]:
    """Validate STEP29.17 cost-aware label v2 contract.

    `contract_valid` means the sample states its readiness honestly.
    `label_policy_v2_pass` means the sample is complete enough for training.
    """

    payload = merged_label_payload(label_json, post_trade_outcome_json)
    reason_codes: list[str] = []
    missing_fields: list[str] = []

    for field in sorted(REQUIRED_LABEL_FIELDS):
        if field not in payload or _is_missing(payload.get(field)):
            missing_fields.append(field)
    if missing_fields:
        reason_codes.append("label_policy_v2_required_fields_missing")

    if payload.get("label_policy_version") != LABEL_POLICY_VERSION:
        reason_codes.append("label_policy_version_mismatch")

    coverage_status = str(payload.get("label_coverage_status") or "")
    if coverage_status not in LABEL_COVERAGE_STATUSES:
        reason_codes.append("label_coverage_status_invalid")

    cost_missing = [field for field in sorted(REQUIRED_COST_FIELDS) if field not in payload or _is_missing(payload.get(field))]
    false_allow = _as_float(payload.get("false_allow_cost"))
    false_block = _as_float(payload.get("false_block_cost"))
    if false_allow == 0.0 and false_block == 0.0 and coverage_status == LABEL_COVERAGE_COMPLETE:
        reason_codes.append("false_allow_false_block_default_zero_disallowed")

    if coverage_status == LABEL_COVERAGE_COMPLETE:
        if cost_missing:
            reason_codes.append("cost_fields_missing_for_complete_label")
        if false_block is not None and not (payload.get("counterfactual_evidence_ref") or payload.get("false_block_cost_source")):
            reason_codes.append("false_block_cost_counterfactual_evidence_missing")
    elif cost_missing:
        reason_codes.append("cost_fields_incomplete_not_training_ready")

    forbidden = decision_time_forbidden_violations(decision_time_input_json or {})
    if forbidden:
        reason_codes.append("decision_time_forbidden_fields_present")

    bad_trade_flag = payload.get("bad_trade_flag")
    if not isinstance(bad_trade_flag, bool):
        reason_codes.append("bad_trade_flag_not_boolean")

    reason_value = payload.get("reason_codes")
    if not isinstance(reason_value, list):
        reason_codes.append("reason_codes_not_list")

    contract_blockers = {
        "label_policy_v2_required_fields_missing",
        "label_policy_version_mismatch",
        "label_coverage_status_invalid",
        "false_allow_false_block_default_zero_disallowed",
        "decision_time_forbidden_fields_present",
        "bad_trade_flag_not_boolean",
        "reason_codes_not_list",
    }
    training_blockers = {
        "cost_fields_missing_for_complete_label",
        "false_block_cost_counterfactual_evidence_missing",
        "cost_fields_incomplete_not_training_ready",
    }
    contract_valid = not any(code in contract_blockers for code in reason_codes)
    label_policy_v2_pass = (
        contract_valid
        and coverage_status == LABEL_COVERAGE_COMPLETE
        and not any(code in training_blockers for code in reason_codes)
        and not cost_missing
    )
    return {
        "schema_version": "STEP29.17_cost_aware_label_policy_v2_contract",
        "label_policy_version": LABEL_POLICY_VERSION,
        "contract_valid": contract_valid,
        "label_policy_v2_pass": label_policy_v2_pass,
        "label_coverage_status": coverage_status or None,
        "cost_fields_complete": not cost_missing,
        "missing_fields": sorted(set(missing_fields + cost_missing)),
        "forbidden_decision_time_fields": forbidden,
        "reason_codes": sorted(set(reason_codes)),
    }
