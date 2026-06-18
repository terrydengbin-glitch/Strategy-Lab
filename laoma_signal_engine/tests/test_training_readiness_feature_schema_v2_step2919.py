from laoma_signal_engine.training_readiness.feature_schema_v2 import (
    ENTRY_MARKET_FIELDS,
    ROOT_DECISION_FIELDS,
    validate_decision_time_feature_schema_v2,
)


def _lineage() -> dict[str, object]:
    return {
        "source_db_path": "DATA/source.db",
        "source_table": "features",
        "feature_timestamp_ms": 1000,
        "known_at_ms": 1060,
        "source_available_time_ms": 1060,
        "source_priority": "rebuilt",
    }


def _valid_decision() -> dict[str, object]:
    entry = {field: 1.0 for field in ENTRY_MARKET_FIELDS}
    entry["field_lineage_json"] = {field: _lineage() for field in ENTRY_MARKET_FIELDS}
    decision = {"entry_market_snapshot": entry}
    for field in ROOT_DECISION_FIELDS:
        decision[field] = 1.0 if field != "market_regime_ref" else "trend_up"
    decision["field_lineage_json"] = {field: _lineage() for field in ROOT_DECISION_FIELDS}
    return decision


def test_step2919_valid_decision_time_feature_schema_v2_passes() -> None:
    result = validate_decision_time_feature_schema_v2(_valid_decision(), decision_time_ms=1100)

    assert result["decision_time_feature_schema_v2_pass"] is True
    assert result["missing_fields"] == []
    assert result["missing_lineage_fields"] == []


def test_step2919_missing_expected_cost_fields_blocks_schema_v2() -> None:
    decision = _valid_decision()
    decision.pop("expected_fee_bps")

    result = validate_decision_time_feature_schema_v2(decision, decision_time_ms=1100)

    assert result["decision_time_feature_schema_v2_pass"] is False
    assert "expected_fee_bps" in result["missing_fields"]


def test_step2919_realized_slippage_in_decision_input_is_forbidden() -> None:
    decision = _valid_decision()
    decision["realized_slippage_bps"] = 3.0

    result = validate_decision_time_feature_schema_v2(decision, decision_time_ms=1100)

    assert result["decision_time_feature_schema_v2_pass"] is False
    assert result["forbidden_decision_time_fields"][0]["field"] == "realized_slippage_bps"


def test_step2919_known_at_after_decision_time_blocks_schema_v2() -> None:
    decision = _valid_decision()
    decision["entry_market_snapshot"]["field_lineage_json"]["rsi_14"]["known_at_ms"] = 1200

    result = validate_decision_time_feature_schema_v2(decision, decision_time_ms=1100)

    assert result["decision_time_feature_schema_v2_pass"] is False
    assert {"field": "entry_market_snapshot.rsi_14", "reason": "known_at_after_decision_time"} in result[
        "known_at_violations"
    ]
