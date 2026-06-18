from __future__ import annotations

from laoma_signal_engine.backtest import p21_trade_quality_v5 as v5


def test_v5_rule_allowlist_excludes_target_fields() -> None:
    assert "net_R" not in v5.ENTRY_KNOWN_RULE_FIELDS
    assert "MFE_R" not in v5.ENTRY_KNOWN_RULE_FIELDS
    assert "MAE_R" not in v5.ENTRY_KNOWN_RULE_FIELDS
    assert "root_cause" not in v5.ENTRY_KNOWN_RULE_FIELDS
    assert "direction_factor_v5" not in v5.ENTRY_KNOWN_RULE_FIELDS
    assert v5.TARGET_ONLY_FIELDS.isdisjoint(v5.ENTRY_KNOWN_RULE_FIELDS)


def test_v5_direction_wrong_flow_against_classification() -> None:
    row = {
        "side": "LONG",
        "root_cause": "direction_wrong",
        "deep_subcause": "aggressive_flow_against_side_proxy",
    }
    features = {
        "side_flow_alignment": "opposite",
        "price_flow_alignment": "opposite",
        "pct_1m_bps": -12,
        "entry_hour_utc": 9,
    }
    targets = {"root_cause": "direction_wrong", "net_R": -0.8, "MFE_R": 0.1, "MAE_R": 1.1}
    out = v5._classify_causal_factor(row, features, targets, {"p24_match": "observed"})
    assert out["direction_factor_v5"] == "direction_flow_against"
    assert out["confidence_v5"] >= 0.8


def test_v5_entry_timing_overextended_classification() -> None:
    row = {
        "side": "SHORT",
        "root_cause": "entered_too_early",
        "deep_subcause": "adverse_excursion_before_favorable_move",
    }
    features = {
        "ema20_distance_bps": -130,
        "vwap_distance_bps": -110,
        "bollinger_position": 0.05,
        "entry_hour_utc": 18,
    }
    targets = {"root_cause": "entered_too_early", "net_R": -0.6, "MFE_R": 0.9, "MAE_R": 1.0}
    out = v5._classify_causal_factor(row, features, targets, {"p24_match": "observed"})
    assert out["entry_timing_factor_v5"] == "entry_overextended_from_mean"


def test_v5_candidate_rules_use_only_entry_known_fields() -> None:
    rows = []
    for idx in range(80):
        rows.append(
            {
                "causal_id": f"bad_{idx}",
                "package_key": "pkg",
                "experiment_id": "exp",
                "parameter_set_id": "param",
                "strategy_line": "strategy6",
                "entry_time_ms": idx,
                "net_R": -0.5,
                "direction_factor_v5": "direction_flow_against",
                "entry_features": {"side": "LONG", "entry_hour_utc": 9, "symbol": "BADUSDT"},
            }
        )
    for idx in range(80):
        rows.append(
            {
                "causal_id": f"good_{idx}",
                "package_key": "pkg",
                "experiment_id": "exp",
                "parameter_set_id": "param",
                "strategy_line": "strategy6",
                "entry_time_ms": 1000 + idx,
                "net_R": 0.4,
                "direction_factor_v5": "profit_reference_pattern",
                "entry_features": {"side": "LONG", "entry_hour_utc": 10, "symbol": "GOODUSDT"},
            }
        )
    candidates = v5._candidate_rules(rows, min_samples=20, limit=10)
    assert candidates
    for candidate in candidates:
        rule = v5._loads(candidate["rule_json"], {})
        assert rule["field"] in v5.ENTRY_KNOWN_RULE_FIELDS
        assert rule["field"] not in v5.TARGET_ONLY_FIELDS


def test_v5_combo_candidate_rules_use_only_entry_known_fields() -> None:
    rows = []
    for idx in range(90):
        rows.append(
            {
                "causal_id": f"bad_combo_{idx}",
                "package_key": "pkg",
                "experiment_id": "exp",
                "parameter_set_id": "param",
                "strategy_line": "strategy6",
                "entry_time_ms": idx,
                "net_R": -0.7,
                "direction_factor_v5": "direction_flow_against",
                "entry_features": {
                    "btc_volatility": "normal",
                    "funding_bucket": "NEGATIVE_EXTREME",
                    "market_breadth": "up",
                    "side": "LONG",
                },
            }
        )
    for idx in range(90):
        rows.append(
            {
                "causal_id": f"good_combo_{idx}",
                "package_key": "pkg",
                "experiment_id": "exp",
                "parameter_set_id": "param",
                "strategy_line": "strategy6",
                "entry_time_ms": 1000 + idx,
                "net_R": 0.6,
                "direction_factor_v5": "profit_reference_pattern",
                "entry_features": {
                    "btc_volatility": "high",
                    "funding_bucket": "neutral",
                    "market_breadth": "down",
                    "side": "LONG",
                },
            }
        )
    candidates = v5._combo_candidate_rules(
        rows,
        seed_rules={"strategy6": [("btc_volatility", "normal"), ("funding_bucket", "NEGATIVE_EXTREME"), ("market_breadth", "up")]},
        min_samples=20,
        limit=10,
        max_combo_size=3,
    )
    assert candidates
    for candidate in candidates:
        rule = v5._loads(candidate["rule_json"], {})
        assert rule["operator"] == "AND"
        for item in rule["rules"]:
            assert item["field"] in v5.ENTRY_KNOWN_RULE_FIELDS
            assert item["field"] not in v5.TARGET_ONLY_FIELDS
