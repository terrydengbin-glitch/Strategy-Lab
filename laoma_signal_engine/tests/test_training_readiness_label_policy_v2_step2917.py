from __future__ import annotations

from laoma_signal_engine.training_readiness.label_policy_v2 import (
    LABEL_COVERAGE_COMPLETE,
    LABEL_COVERAGE_COUNTERFACTUAL_UNAVAILABLE,
    LABEL_POLICY_VERSION,
    decision_time_forbidden_violations,
    validate_cost_aware_label_v2,
)


def _complete_label() -> dict:
    return {
        "label_policy_version": LABEL_POLICY_VERSION,
        "label_coverage_status": LABEL_COVERAGE_COMPLETE,
        "label_observation_end_time": "2026-06-18T00:00:00Z",
        "reason_codes": [],
        "quality_label": "bad_after_cost",
        "bad_trade_flag": True,
        "false_allow_cost": 1.2,
        "false_block_cost": 0.3,
        "false_block_cost_source": "observed_holdout_counterfactual",
        "fee_bps": 5.0,
        "realized_slippage_bps": 2.5,
    }


def test_step2917_complete_cost_aware_label_passes_contract_and_training_gate() -> None:
    result = validate_cost_aware_label_v2(
        _complete_label(),
        decision_time_input_json={"entry_market_snapshot": {"rsi_14": 51.0}},
    )

    assert result["contract_valid"] is True
    assert result["label_policy_v2_pass"] is True
    assert result["reason_codes"] == []


def test_step2917_forbidden_cost_or_outcome_fields_in_decision_input_fail_contract() -> None:
    result = validate_cost_aware_label_v2(
        _complete_label(),
        decision_time_input_json={
            "entry_market_snapshot": {"rsi_14": 51.0},
            "quality_label": "bad_after_cost",
            "nested": {"false_allow_cost": 1.2},
        },
    )

    assert result["contract_valid"] is False
    assert result["label_policy_v2_pass"] is False
    assert "decision_time_forbidden_fields_present" in result["reason_codes"]
    fields = {item["field"] for item in result["forbidden_decision_time_fields"]}
    assert {"quality_label", "false_allow_cost"}.issubset(fields)


def test_step2917_false_allow_and_false_block_default_zero_is_not_valid() -> None:
    label = _complete_label()
    label["false_allow_cost"] = 0
    label["false_block_cost"] = 0

    result = validate_cost_aware_label_v2(label)

    assert result["contract_valid"] is False
    assert result["label_policy_v2_pass"] is False
    assert "false_allow_false_block_default_zero_disallowed" in result["reason_codes"]


def test_step2917_counterfactual_unavailable_is_honest_but_not_training_ready() -> None:
    label = {
        "label_policy_version": LABEL_POLICY_VERSION,
        "label_coverage_status": LABEL_COVERAGE_COUNTERFACTUAL_UNAVAILABLE,
        "label_observation_end_time": "2026-06-18T00:00:00Z",
        "reason_codes": ["counterfactual_evidence_missing"],
        "quality_label": "needs_review",
        "bad_trade_flag": False,
        "false_allow_cost": 0.8,
        "fee_bps": 5.0,
        "realized_slippage_bps": 2.0,
    }

    result = validate_cost_aware_label_v2(label)

    assert result["contract_valid"] is True
    assert result["label_policy_v2_pass"] is False
    assert result["label_coverage_status"] == LABEL_COVERAGE_COUNTERFACTUAL_UNAVAILABLE
    assert "cost_fields_incomplete_not_training_ready" in result["reason_codes"]


def test_step2917_decision_time_violation_helper_reports_leaf_paths() -> None:
    violations = decision_time_forbidden_violations({"a": {"b": {"net_R": -1.0}}, "ok": 1})

    assert violations == [
        {"field": "net_R", "field_path": "a.b.net_R", "reason": "post_trade_or_cost_label_in_decision_time_input"}
    ]
