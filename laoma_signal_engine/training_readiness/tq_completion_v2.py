"""STEP29.18 Trade Quality module completion classification.

The helpers in this module are pure and sidecar-only.  They do not infer trade
quality labels from fills or PnL; they only classify whether the official Trade
Quality module already supplied enough post-trade facts to make a sample
training-eligible under the cost-aware v2 label contract.
"""

from __future__ import annotations

from typing import Any

from .label_policy_v2 import (
    LABEL_COVERAGE_COMPLETE,
    LABEL_COVERAGE_EXCLUDED_FROM_TRAINING,
    LABEL_COVERAGE_NEEDS_REVIEW,
    LABEL_COVERAGE_SOURCE_COST_MISSING,
    LABEL_POLICY_VERSION,
    REQUIRED_COST_FIELDS,
    validate_cost_aware_label_v2,
)


TQ_COMPLETION_POLICY_VERSION = "step29_trade_quality_module_completion_backfill_v2"
OFFICIAL_TQ_PROVIDERS = {"trade_quality_module", "backtest_trade_quality_module"}
TQ_REQUIRED_FIELDS = ("net_R", "MFE_R", "MAE_R", "holding_time_sec", "exit_reason")


def _missing(value: Any) -> bool:
    return value is None or value == ""


def _first_text(*values: Any) -> str | None:
    for value in values:
        if not _missing(value):
            return str(value)
    return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "ready", "complete"}
    return False


def _reason_list(*values: Any) -> list[str]:
    out: list[str] = []
    for value in values:
        if isinstance(value, list):
            out.extend(str(item) for item in value if not _missing(item))
        elif isinstance(value, str) and value:
            out.append(value)
    return sorted(set(out))


def _keep_manual_reason(code: str) -> bool:
    auto_prefixes = (
        "trade_quality_",
        "cost_missing_",
        "label_policy_v2_",
    )
    auto_codes = {
        "source_cost_missing",
        "excluded_from_training",
        "cost_fields_incomplete_not_training_ready",
        "cost_fields_missing_for_complete_label",
    }
    return code not in auto_codes and not code.startswith(auto_prefixes)


def _source_ref(label_json: dict[str, Any], post_trade_outcome_json: dict[str, Any]) -> Any:
    return (
        label_json.get("trade_quality_source_ref")
        or post_trade_outcome_json.get("trade_quality_source_ref")
        or label_json.get("source_ref")
        or post_trade_outcome_json.get("source_ref")
    )


def tq_required_missing(label_json: dict[str, Any], post_trade_outcome_json: dict[str, Any]) -> list[str]:
    payload = dict(post_trade_outcome_json or {})
    payload.update(dict(label_json or {}))
    return [field for field in TQ_REQUIRED_FIELDS if _missing(payload.get(field))]


def cost_fields_missing(label_json: dict[str, Any], post_trade_outcome_json: dict[str, Any]) -> list[str]:
    payload = dict(post_trade_outcome_json or {})
    payload.update(dict(label_json or {}))
    return [field for field in sorted(REQUIRED_COST_FIELDS) if _missing(payload.get(field))]


def classify_tq_completion_v2(
    label_json: dict[str, Any] | None,
    post_trade_outcome_json: dict[str, Any] | None,
    data_quality_json: dict[str, Any] | None = None,
    *,
    decision_time_input_json: Any | None = None,
) -> dict[str, Any]:
    label = dict(label_json or {})
    outcome = dict(post_trade_outcome_json or {})
    dq = dict(data_quality_json or {})
    provider = _first_text(label.get("trade_quality_provider"), outcome.get("trade_quality_provider"), dq.get("trade_quality_provider"))
    module = _first_text(label.get("trade_quality_module"), outcome.get("trade_quality_module"), dq.get("trade_quality_module"))
    source_ref = _source_ref(label, outcome)
    required_missing = tq_required_missing(label, outcome)
    costs_missing = cost_fields_missing(label, outcome)
    has_official_provider = provider in OFFICIAL_TQ_PROVIDERS and bool(module or source_ref)

    reason_codes = [code for code in _reason_list(label.get("reason_codes"), dq.get("reason_codes")) if _keep_manual_reason(code)]
    if not has_official_provider:
        coverage_status = LABEL_COVERAGE_EXCLUDED_FROM_TRAINING
        review_status = "needs_human_review"
        training_label_ready = False
        module_complete = False
        reason_codes.extend(["trade_quality_module_missing", "excluded_from_training"])
    elif required_missing:
        coverage_status = LABEL_COVERAGE_NEEDS_REVIEW
        review_status = "needs_human_review"
        training_label_ready = False
        module_complete = False
        reason_codes.extend(["trade_quality_module_joined_needs_review"])
        reason_codes.extend(f"trade_quality_missing_{field}" for field in required_missing)
    elif costs_missing:
        coverage_status = LABEL_COVERAGE_SOURCE_COST_MISSING
        review_status = "needs_human_review"
        training_label_ready = False
        module_complete = True
        reason_codes.extend(["trade_quality_module_complete", "source_cost_missing"])
        reason_codes.extend(f"cost_missing_{field}" for field in costs_missing)
    else:
        coverage_status = LABEL_COVERAGE_COMPLETE
        review_status = "ready"
        training_label_ready = True
        module_complete = True
        reason_codes.append("trade_quality_module_complete")

    label.update(
        {
            "label_policy_version": LABEL_POLICY_VERSION,
            "label_coverage_status": coverage_status,
            "review_status": review_status,
            "training_label_ready": training_label_ready,
            "trade_quality_provider": provider,
            "trade_quality_module": module,
            "reason_codes": sorted(set(reason_codes)),
        }
    )
    if _missing(label.get("label_observation_end_time")):
        label["label_observation_end_time"] = outcome.get("exit_time_ms") or outcome.get("close_time_ms") or outcome.get("event_time_ms")
    if "bad_trade_flag" not in label or label.get("bad_trade_flag") is None:
        label["bad_trade_flag"] = False if label.get("quality_label") in {"winner", "good"} else bool(label.get("quality_label") in {"loser", "bad"})
    if not label.get("quality_label"):
        label["quality_label"] = "needs_human_review"

    dq_missing = set(dq.get("missing_fields_json") or [])
    for field in required_missing:
        dq_missing.add(f"trade_quality.{field}")
    for field in costs_missing:
        dq_missing.add(f"label_policy_v2.{field}")

    validation = validate_cost_aware_label_v2(
        label,
        post_trade_outcome_json=outcome,
        decision_time_input_json=decision_time_input_json or {},
    )
    dq.update(
        {
            "trade_quality_completion_policy_version": TQ_COMPLETION_POLICY_VERSION,
            "trade_quality_module_complete": module_complete,
            "trade_quality_training_label_ready": training_label_ready,
            "label_policy_version": LABEL_POLICY_VERSION,
            "label_coverage_status": coverage_status,
            "label_policy_v2_contract_valid": validation["contract_valid"],
            "label_policy_v2_pass": validation["label_policy_v2_pass"],
            "label_policy_v2_reason_codes": validation["reason_codes"],
            "trade_quality_provider": provider,
            "trade_quality_module": module,
            "review_status": review_status,
            "missing_fields_json": sorted(dq_missing),
        }
    )
    return {
        "label_json": label,
        "post_trade_outcome_json": outcome,
        "data_quality_json": dq,
        "trade_quality_module_complete": module_complete,
        "label_policy_v2_contract_valid": validation["contract_valid"],
        "label_policy_v2_pass": validation["label_policy_v2_pass"],
        "label_coverage_status": coverage_status,
        "reason_codes": sorted(set(reason_codes + validation["reason_codes"])),
        "required_missing_fields": required_missing,
        "cost_missing_fields": costs_missing,
    }
