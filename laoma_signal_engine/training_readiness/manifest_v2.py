"""STEP29.20 training readiness manifest v2 gate helpers."""

from __future__ import annotations

from typing import Any


MANIFEST_SCHEMA_VERSION = "step29_training_readiness_manifest_v2"

REQUIRED_FULL_RATE_FIELDS = (
    "entry_exit_pair_rate",
    "market_feature_complete_rate",
    "known_at_pass_rate",
    "trade_quality_module_complete_rate",
    "cost_fields_coverage",
    "decision_time_feature_schema_v2_pass_rate",
    "label_policy_v2_pass_rate",
)


def gate_training_readiness_v2(status: dict[str, Any]) -> dict[str, Any]:
    blocking: list[str] = []
    for field in REQUIRED_FULL_RATE_FIELDS:
        if float(status.get(field) or 0.0) < 1.0:
            blocking.append(field)
    if int(status.get("post_trade_leakage_count") or 0) != 0:
        blocking.append("post_trade_leakage_count")
    if int(status.get("duplicate_sample_ids") or 0) != 0:
        blocking.append("duplicate_sample_ids")
    if int(status.get("duplicate_event_ids") or 0) != 0:
        blocking.append("duplicate_event_ids")
    if int(status.get("samples_without_source_ref") or 0) != 0:
        blocking.append("samples_without_source_ref")
    if bool(status.get("oos_used_for_training_or_hpo")):
        blocking.append("oos_used_for_training_or_hpo")
    if bool(status.get("paper_shadow_used_for_training_or_hpo")):
        blocking.append("paper_shadow_used_for_training_or_hpo")
    allowed = int(status.get("sample_count") or 0) > 0 and not blocking
    return {
        "allowed_for_training": allowed,
        "allowed_for_llm_training": allowed,
        "dataset_status": "training_ready" if allowed else "blocked",
        "blocking_reasons": blocking,
    }
