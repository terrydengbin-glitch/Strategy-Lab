"""STEP29.21 AI Trader readiness handoff helpers."""

from __future__ import annotations

from typing import Any


BLOCKER_TASK_HINTS = {
    "market_feature_complete_rate": "Re-run/repair STEP29.10/29.19 market feature reconstruction for missing entry snapshots.",
    "known_at_pass_rate": "Re-run/repair STEP29.11/29.19 known-at validation for every decision-time field.",
    "trade_quality_module_complete_rate": "Repair STEP29.18 official Trade Quality module coverage.",
    "cost_fields_coverage": "Add cost-aware label fields before training release.",
    "decision_time_feature_schema_v2_pass_rate": "Complete STEP29.19 v2 fields: spread, expected slippage, expected fee, market regime.",
    "label_policy_v2_pass_rate": "Complete STEP29.17/29.18 cost-aware labels.",
    "post_trade_leakage_count": "Remove post-trade fields from decision_time_input_json.",
}


def build_handoff_summary(status: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    blockers = list(status.get("blocking_reasons") or [])
    allowed = bool(status.get("allowed_for_training"))
    return {
        "handoff_status": "PASS" if allowed else "BLOCKED",
        "dataset_status": status.get("dataset_status"),
        "allowed_for_training": allowed,
        "allowed_for_llm_training": bool(status.get("allowed_for_llm_training")),
        "sidecar_db_path": status.get("sidecar_db"),
        "status_path": manifest.get("status_path"),
        "manifest_path": "DATA/research/trade_snapshots/step29_20_training_readiness_manifest_v2.json",
        "manifest_id": manifest.get("manifest_id"),
        "dataset_hash": manifest.get("dataset_hash") or status.get("dataset_hash"),
        "split_manifest_hash": manifest.get("split_manifest_hash") or status.get("split_manifest_hash"),
        "sample_count": status.get("sample_count"),
        "source_mode_counts": status.get("source_mode_counts") or {},
        "coverage_json": {
            "entry_exit_pair_rate": status.get("entry_exit_pair_rate"),
            "market_feature_complete_rate": status.get("market_feature_complete_rate"),
            "known_at_pass_rate": status.get("known_at_pass_rate"),
            "trade_quality_module_complete_rate": status.get("trade_quality_module_complete_rate"),
            "cost_fields_coverage": status.get("cost_fields_coverage"),
            "decision_time_feature_schema_v2_pass_rate": status.get("decision_time_feature_schema_v2_pass_rate"),
            "label_policy_v2_pass_rate": status.get("label_policy_v2_pass_rate"),
            "post_trade_leakage_count": status.get("post_trade_leakage_count"),
            "duplicate_sample_ids": status.get("duplicate_sample_ids"),
            "duplicate_event_ids": status.get("duplicate_event_ids"),
            "samples_without_source_ref": status.get("samples_without_source_ref"),
        },
        "blocking_reasons": blockers,
        "blocking_task_hints": {blocker: BLOCKER_TASK_HINTS.get(blocker, "Inspect STEP29.20 v2 status.") for blocker in blockers},
        "read_only_contract": {
            "ai_trader_may_read": True,
            "ai_trader_may_write_source_db": False,
            "abnormal_enchanced_writes_ai_trader_db": False,
        },
    }
