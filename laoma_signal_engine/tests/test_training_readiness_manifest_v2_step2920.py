from laoma_signal_engine.training_readiness.manifest_v2 import REQUIRED_FULL_RATE_FIELDS, gate_training_readiness_v2


def _passing_status() -> dict[str, object]:
    status: dict[str, object] = {field: 1.0 for field in REQUIRED_FULL_RATE_FIELDS}
    status.update(
        {
            "sample_count": 3,
            "post_trade_leakage_count": 0,
            "duplicate_sample_ids": 0,
            "duplicate_event_ids": 0,
            "samples_without_source_ref": 0,
            "oos_used_for_training_or_hpo": False,
            "paper_shadow_used_for_training_or_hpo": False,
        }
    )
    return status


def test_step2920_all_v2_rates_required_for_training_ready() -> None:
    result = gate_training_readiness_v2(_passing_status())

    assert result["allowed_for_training"] is True
    assert result["blocking_reasons"] == []


def test_step2920_cost_coverage_blocks_manifest_v2() -> None:
    status = _passing_status()
    status["cost_fields_coverage"] = 0.5

    result = gate_training_readiness_v2(status)

    assert result["allowed_for_training"] is False
    assert "cost_fields_coverage" in result["blocking_reasons"]


def test_step2920_paper_shadow_pollution_blocks_manifest_v2() -> None:
    status = _passing_status()
    status["paper_shadow_used_for_training_or_hpo"] = True

    result = gate_training_readiness_v2(status)

    assert result["allowed_for_training"] is False
    assert "paper_shadow_used_for_training_or_hpo" in result["blocking_reasons"]
