from laoma_signal_engine.training_readiness.label_policy_v2 import LABEL_COVERAGE_SOURCE_COST_MISSING
from laoma_signal_engine.training_readiness.tq_completion_v2 import classify_tq_completion_v2


def test_step2918_official_tq_without_cost_is_module_complete_but_not_training_ready() -> None:
    result = classify_tq_completion_v2(
        {
            "training_label_ready": True,
            "quality_label": "winner",
            "bad_trade_flag": False,
            "trade_quality_provider": "trade_quality_module",
            "trade_quality_module": "laoma_signal_engine.trade_quality.engine",
            "trade_quality_source_ref": {"source_table": "trade_quality_samples"},
        },
        {"net_R": 1.2, "MFE_R": 1.8, "MAE_R": -0.2, "holding_time_sec": 120, "exit_reason": "TP"},
        {"trade_quality_training_label_ready": True},
    )

    assert result["trade_quality_module_complete"] is True
    assert result["label_policy_v2_pass"] is False
    assert result["label_json"]["training_label_ready"] is False
    assert result["label_coverage_status"] == LABEL_COVERAGE_SOURCE_COST_MISSING


def test_step2918_missing_tq_provider_is_excluded() -> None:
    result = classify_tq_completion_v2(
        {"quality_label": "winner", "training_label_ready": True},
        {"net_R": 1.0, "MFE_R": 1.2, "MAE_R": -0.1, "holding_time_sec": 60, "exit_reason": "TP"},
        {"trade_quality_training_label_ready": True},
    )

    assert result["trade_quality_module_complete"] is False
    assert result["label_json"]["training_label_ready"] is False
    assert result["label_coverage_status"] == "excluded_from_training"
    assert "trade_quality_module_missing" in result["reason_codes"]


def test_step2918_incomplete_tq_fields_need_review() -> None:
    result = classify_tq_completion_v2(
        {
            "training_label_ready": True,
            "quality_label": "loser",
            "bad_trade_flag": True,
            "trade_quality_provider": "backtest_trade_quality_module",
            "trade_quality_module": "laoma_signal_engine.backtest.p21_trade_quality",
            "trade_quality_source_ref": {"source_table": "backtest_trade_quality_samples"},
        },
        {"net_R": -0.5, "MFE_R": 0.1, "MAE_R": -0.7, "exit_reason": "SL"},
        {"trade_quality_training_label_ready": True},
    )

    assert result["trade_quality_module_complete"] is False
    assert result["label_coverage_status"] == "needs_review"
    assert "holding_time_sec" in result["required_missing_fields"]
