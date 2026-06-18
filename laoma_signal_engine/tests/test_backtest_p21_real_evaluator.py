from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from laoma_signal_engine.backtest.p21_v2 import simulate_1m_fill
from laoma_signal_engine.backtest.p21_real_evaluator import ENGINE_MODE, evaluate_signal_offline


@dataclass(frozen=True)
class _Signal:
    signal_id: str
    strategy_line: str
    symbol: str
    side: str
    index: int
    signal_time_ms: int
    score: float
    features: dict


def _rows(minutes: int = 80) -> list[dict]:
    start = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=minutes + 5)
    out = []
    price = 100.0
    for idx in range(minutes):
        price *= 1.001 if idx % 3 else 0.999
        open_time_ms = int((start + timedelta(minutes=idx)).timestamp() * 1000)
        out.append(
            {
                "open_time_ms": open_time_ms,
                "open": price * 0.999,
                "high": price * 1.004,
                "low": price * 0.996,
                "close": price,
                "volume": 1000.0,
            }
        )
    return out


def _signal(line: str = "without_micro", score: float = 80.0) -> _Signal:
    rows = _rows()
    idx = 40
    return _Signal(
        signal_id=f"sig_{line}",
        strategy_line=line,
        symbol="BTCUSDT",
        side="LONG",
        index=idx,
        signal_time_ms=int(rows[idx]["open_time_ms"]),
        score=score,
        features={
            "pct_1m_bps": 12.0,
            "pct_3m_bps": 38.0,
            "pct_5m_bps": 45.0,
            "pct_15m_bps": 80.0,
            "volume_z": 3.0,
            "range_pos_30m": 0.45,
            "atr_1m_bps": 18.0,
            "close": rows[idx]["close"],
        },
    )


def _params(line: str = "without_micro") -> dict:
    return {
        "strategy_line": line,
        "min_score": 20,
        "target_rr": 0.8,
        "min_rr": 0.2,
        "min_net_rr": 0.2,
        "min_effective_rr": 0.2,
        "stop_atr_mult": 1.0,
        "max_stop_bps": 240,
        "min_stop_bps": 3,
        "min_reachable_reward_bps": 5,
        "min_tp_after_cost_bps": 0,
        "tp_target_policy": {
            "mode": "fast_capped_rr",
            "target_net_rr": 1.0,
            "target_rr_cap": 1.0,
            "min_reward_bps": 1,
            "require_market_room": False,
            "allow_structure_runner": True,
        },
        "range_room": {"long_max_range_pos": 1.0, "short_min_range_pos": 0.0},
        "taker_fee_bps": 5,
        "slippage_bps": 1,
    }


def test_offline_real_evaluator_without_micro_outputs_trade_plan_payload() -> None:
    result = evaluate_signal_offline(_signal("without_micro"), _rows(), _params("without_micro"))
    assert result["lineage_mode"] == ENGINE_MODE
    assert result["trade_plan_payload"]["source"] == "trade_plan_without_micro"
    assert result["order"] is not None
    assert result["order"]["entry_mode"] == "MARKET"


def test_offline_real_evaluator_strategy4_uses_strategy4_contract() -> None:
    result = evaluate_signal_offline(_signal("strategy4", score=50.0), _rows(), _params("strategy4"))
    assert result["lineage_mode"] == ENGINE_MODE
    assert result["trade_plan_payload"]["source"] == "trade_plan_strategy4"
    assert result["trade_plan_payload"]["micro_mode"] == "none"


def test_offline_real_evaluator_strategy5_adds_evidence_contract() -> None:
    result = evaluate_signal_offline(_signal("strategy5"), _rows(), _params("strategy5"))
    assert result["lineage_mode"] == ENGINE_MODE
    assert result["trade_plan_payload"]["source"] == "trade_plan_strategy5"
    assert result["order"] is not None
    assert result["order"]["trade_plan_payload"]["plans"][0]["guards"]["strategy5_adapter_mode"] == ENGINE_MODE


def test_offline_real_evaluator_strategy6_adds_market_acceptance_contract() -> None:
    result = evaluate_signal_offline(_signal("strategy6"), _rows(), _params("strategy6"))
    assert result["lineage_mode"] == ENGINE_MODE
    assert result["trade_plan_payload"]["source"] == "trade_plan_strategy6"
    assert result["trade_plan_payload"]["micro_mode"] == "strategy6_market_accepted"
    assert result["order"] is not None
    guards = result["order"]["trade_plan_payload"]["plans"][0]["guards"]
    assert guards["line"] == "strategy6"
    assert guards["strategy6_adapter_mode"] == ENGINE_MODE


def test_offline_real_evaluator_strategy6_v2_writes_triage_contract() -> None:
    params = _params("strategy6")
    params.update(
        {
            "strategy6_version": "v2",
            "v2_max_chase_bps": 120,
            "v2_distance_from_mean_max_bps": 120,
        }
    )
    result = evaluate_signal_offline(_signal("strategy6"), _rows(), params)
    assert result["order"] is not None
    guards = result["order"]["trade_plan_payload"]["plans"][0]["guards"]
    features = result["order"]["features"]
    assert guards["strategy6_version"] == "v2"
    assert guards["strategy6_direction_state"] in {"accepted_direction", "uncertain_direction", "denied_direction"}
    assert guards["strategy6_entry_quality_state"]
    assert features["strategy6_version"] == "v2"
    assert "strategy6_adaptive_exit_tier" in features


def test_offline_real_evaluator_strategy6_v3_1_writes_calibration_contract() -> None:
    params = _params("strategy6")
    params.update(
        {
            "strategy6_version": "v3_1",
            "v3_1_reverse_1m_deny_bps": 10,
            "v3_1_low_followthrough_min_volume_z": 0.9,
        }
    )
    result = evaluate_signal_offline(_signal("strategy6"), _rows(), params)
    assert result["order"] is not None
    features = result["order"]["features"]
    guards = result["order"]["trade_plan_payload"]["plans"][0]["guards"]
    assert guards["strategy6_version"] == "v3_1"
    assert features["strategy6_version"] == "v3_1"
    assert "strategy6_v3_1_adverse_1m_bps" in features
    assert "strategy6_v3_1_low_followthrough" in features


def test_offline_real_evaluator_strategy6_v3_2_writes_side_context_contract() -> None:
    params = _params("strategy6")
    params.update(
        {
            "strategy6_version": "v3_2",
            "v3_2_long_min_direction_context_score": 58,
            "v3_2_long_btc_against_action": "wait",
            "v3_2_quality_filter_mode": "shadow",
        }
    )
    result = evaluate_signal_offline(_signal("strategy6"), _rows(), params)
    assert result["order"] is not None
    features = result["order"]["features"]
    guards = result["order"]["trade_plan_payload"]["plans"][0]["guards"]
    assert guards["strategy6_version"] == "v3_2"
    assert features["strategy6_version"] == "v3_2"
    assert "strategy6_v3_2_side_profile" in features
    assert "strategy6_v3_2_quality_filter_state" in features


def test_offline_real_evaluator_strategy6_v3_3_writes_no_lookahead_contract() -> None:
    params = _params("strategy6")
    params.update(
        {
            "strategy6_version": "v3_3",
            "v3_3_long_min_direction_context_score": 58,
            "v3_3_adverse_1m_wait_bps": 20,
            "v3_3_adverse_3m_deny_bps": 30,
        }
    )
    result = evaluate_signal_offline(_signal("strategy6"), _rows(), params)
    assert result["order"] is not None
    features = result["order"]["features"]
    guards = result["order"]["trade_plan_payload"]["plans"][0]["guards"]
    assert guards["strategy6_version"] == "v3_3"
    assert guards["strategy6_v3_3_no_lookahead"] is True
    assert features["strategy6_v3_3_no_lookahead"] is True
    assert guards["strategy6_v3_3_known_at_contract"]["pct_3m_bps"] == "entry_time"


def test_strategy6_v3_3_decision_is_stable_when_future_bars_change() -> None:
    rows = _rows()
    signal = _signal("strategy6")
    params = _params("strategy6")
    params.update(
        {
            "strategy6_version": "v3_3",
            "v3_3_long_min_direction_context_score": 58,
            "v3_3_adverse_1m_wait_bps": 20,
            "v3_3_adverse_3m_deny_bps": 30,
        }
    )
    mutated = [dict(row) for row in rows]
    for row in mutated[signal.index + 5 :]:
        row["high"] = float(row["high"]) * 1.5
        row["low"] = float(row["low"]) * 0.5
        row["close"] = float(row["close"]) * 1.2

    base = evaluate_signal_offline(signal, rows, params)
    changed = evaluate_signal_offline(signal, mutated, params)

    base_guards = base["trade_plan_payload"]["plans"][0]["guards"]
    changed_guards = changed["trade_plan_payload"]["plans"][0]["guards"]
    assert base_guards["strategy6_decision_state"] == changed_guards["strategy6_decision_state"]
    assert base_guards["strategy6_v3_3_aligned_1m_bps"] == changed_guards["strategy6_v3_3_aligned_1m_bps"]
    assert base_guards["strategy6_v3_3_no_lookahead"] is True


def test_offline_real_evaluator_strategy6_v3_4_writes_causal_gate_contract() -> None:
    params = _params("strategy6")
    params.update(
        {
            "strategy6_version": "v3_4",
            "v3_3_long_min_direction_context_score": 58,
            "v3_4_min_followthrough_5m_bps": 80,
        }
    )
    result = evaluate_signal_offline(_signal("strategy6"), _rows(), params)
    guards = result["trade_plan_payload"]["plans"][0]["guards"]
    assert guards["strategy6_version"] == "v3_4"
    assert guards["strategy6_v3_4_no_lookahead"] is True
    assert guards["strategy6_v3_4_known_at_contract"]["pct_5m_bps"] == "entry_time"
    assert "strategy6_v3_4_gate_hits" in guards
    if result["order"] is not None:
        assert result["order"]["features"]["strategy6_version"] == "v3_4"


def test_strategy6_v3_4_decision_is_stable_when_future_bars_change() -> None:
    rows = _rows()
    signal = _signal("strategy6")
    params = _params("strategy6")
    params.update(
        {
            "strategy6_version": "v3_4",
            "v3_3_long_min_direction_context_score": 58,
            "v3_4_min_followthrough_5m_bps": 80,
        }
    )
    mutated = [dict(row) for row in rows]
    for row in mutated[signal.index + 5 :]:
        row["high"] = float(row["high"]) * 1.5
        row["low"] = float(row["low"]) * 0.5
        row["close"] = float(row["close"]) * 1.2

    base = evaluate_signal_offline(signal, rows, params)
    changed = evaluate_signal_offline(signal, mutated, params)

    base_guards = base["trade_plan_payload"]["plans"][0]["guards"]
    changed_guards = changed["trade_plan_payload"]["plans"][0]["guards"]
    assert base_guards["strategy6_decision_state"] == changed_guards["strategy6_decision_state"]
    assert base_guards["strategy6_v3_4_gate_hits"] == changed_guards["strategy6_v3_4_gate_hits"]
    assert base_guards["strategy6_v3_4_no_lookahead"] is True


def test_offline_real_evaluator_strategy6_backtest_rr_cap_is_opt_in() -> None:
    params = _params("strategy6")
    params.update(
        {
            "target_rr": 0.6,
            "strategy6_backtest_max_effective_planned_rr": 1.0,
        }
    )
    params["tp_target_policy"] = {
        "mode": "structure",
        "target_net_rr": None,
        "target_rr_cap": 10.0,
        "min_reward_bps": 1,
        "require_market_room": False,
        "allow_structure_runner": True,
    }
    result = evaluate_signal_offline(_signal("strategy6"), _rows(), params)
    assert result["order"] is not None
    assert result["order"]["planned_rr"] <= 1.0
    guards = result["order"]["trade_plan_payload"]["plans"][0]["guards"]
    assert guards["strategy6_backtest_rr_guard_applied"] is True
    assert "strategy6_backtest_rr_capped" in result["reason_codes"]


def test_strategy6_exit_protection_is_opt_in_for_fill_simulator() -> None:
    rows = _rows(8)
    rows[2]["low"] = 98.8
    rows[2]["close"] = 98.9
    order = {
        "symbol": "BTCUSDT",
        "strategy_line": "strategy6",
        "side": "LONG",
        "entry_time_ms": rows[1]["open_time_ms"],
        "entry_idx": 1,
        "entry_price": 100.0,
        "stop_loss": 98.8,
        "take_profit": 102.4,
        "planned_rr": 2.0,
        "cost_bps": 0.0,
        "features": {},
        "fast_exit_policy": {},
    }

    base = simulate_1m_fill(order, rows, {"strategy_line": "strategy6", "max_hold_minutes": 4})
    protected = simulate_1m_fill(
        order,
        rows,
        {
            "strategy_line": "strategy6",
            "max_hold_minutes": 4,
            "strategy6_exit_protection_enabled": True,
            "max_loss_R_cap": 0.5,
        },
    )

    assert base["exit_reason"] in {"SL", "SL_same_candle"}
    assert protected["exit_reason"] == "strategy6_loss_R_cap"
    assert protected["net_R"] >= -0.5
    assert protected["features"]["strategy6_exit_protection_enabled"] is True
    assert protected["exit_protection_reason"] == "max_loss_R_cap"


def test_strategy6_loss_cap_precedes_structure_stop_on_intrabar_ambiguity() -> None:
    rows = _rows(8)
    rows[1]["low"] = 98.7
    rows[1]["high"] = 100.8
    rows[1]["close"] = 99.4
    order = {
        "symbol": "BTCUSDT",
        "strategy_line": "strategy6",
        "side": "LONG",
        "entry_time_ms": rows[1]["open_time_ms"],
        "entry_idx": 1,
        "entry_price": 100.0,
        "stop_loss": 98.8,
        "take_profit": 102.0,
        "planned_rr": 2.0,
        "cost_bps": 0.0,
        "features": {},
        "fast_exit_policy": {},
    }

    protected = simulate_1m_fill(
        order,
        rows,
        {
            "strategy_line": "strategy6",
            "max_hold_minutes": 4,
            "strategy6_exit_protection_enabled": True,
            "max_loss_R_cap": 0.5,
            "first_tp_R": 0.5,
        },
    )

    assert protected["exit_reason"] == "strategy6_loss_R_cap_same_candle"
    assert protected["net_R"] >= -0.5
    assert protected["exit_protection_reason"] == "max_loss_R_cap"


def test_strategy6_first_tp_exit_protection_takes_quick_profit() -> None:
    rows = _rows(8)
    rows[1]["high"] = 100.55
    rows[1]["close"] = 100.3
    order = {
        "symbol": "BTCUSDT",
        "strategy_line": "strategy6",
        "side": "LONG",
        "entry_time_ms": rows[1]["open_time_ms"],
        "entry_idx": 1,
        "entry_price": 100.0,
        "stop_loss": 99.0,
        "take_profit": 102.0,
        "planned_rr": 2.0,
        "cost_bps": 0.0,
        "features": {},
        "fast_exit_policy": {},
    }

    protected = simulate_1m_fill(
        order,
        rows,
        {
            "strategy_line": "strategy6",
            "max_hold_minutes": 4,
            "strategy6_exit_protection_enabled": True,
            "first_tp_R": 0.5,
        },
    )

    assert protected["exit_reason"] == "strategy6_first_tp"
    assert protected["net_R"] == 0.5
    assert protected["exit_protection_reason"] == "first_tp_R"


def test_strategy6_adaptive_exit_medium_quality_uses_tier_defaults() -> None:
    rows = _rows(8)
    rows[1]["high"] = 100.8
    rows[1]["close"] = 100.3
    order = {
        "symbol": "BTCUSDT",
        "strategy_line": "strategy6",
        "side": "LONG",
        "entry_time_ms": rows[1]["open_time_ms"],
        "entry_idx": 1,
        "entry_price": 100.0,
        "stop_loss": 99.0,
        "take_profit": 102.0,
        "planned_rr": 2.0,
        "cost_bps": 0.0,
        "features": {"strategy6_adaptive_exit_tier": "medium_quality"},
        "fast_exit_policy": {},
    }

    protected = simulate_1m_fill(
        order,
        rows,
        {
            "strategy_line": "strategy6",
            "max_hold_minutes": 4,
            "strategy6_adaptive_exit_enabled": True,
            "medium_quality_first_tp_R": 0.65,
        },
    )

    assert protected["exit_reason"] == "strategy6_first_tp"
    assert protected["net_R"] == 0.65
    assert protected["features"]["strategy6_adaptive_exit_enabled"] is True
    assert protected["features"]["strategy6_adaptive_exit_tier"] == "medium_quality"


def test_strategy6_v3_5_profit_lock_floor_after_trigger() -> None:
    rows = _rows(8)
    rows[1]["high"] = 100.55
    rows[1]["low"] = 100.05
    rows[1]["close"] = 100.2
    order = {
        "symbol": "BTCUSDT",
        "strategy_line": "strategy6",
        "side": "LONG",
        "entry_time_ms": rows[1]["open_time_ms"],
        "entry_idx": 1,
        "entry_price": 100.0,
        "stop_loss": 99.0,
        "take_profit": 102.0,
        "planned_rr": 2.0,
        "cost_bps": 0.0,
        "features": {"strategy6_adaptive_exit_tier": "v3_5_profit_lock"},
        "fast_exit_policy": {},
    }

    protected = simulate_1m_fill(
        order,
        rows,
        {
            "strategy_line": "strategy6",
            "max_hold_minutes": 4,
            "strategy6_adaptive_exit_enabled": True,
            "v3_5_lock_trigger_R": 0.5,
            "v3_5_lock_floor_R": 0.1,
            "first_tp_R": 0,
        },
    )

    assert protected["exit_reason"] == "strategy6_profit_lock_floor"
    assert protected["net_R"] == 0.1
    assert protected["exit_protection_reason"] == "v3_5_profit_lock_floor"


def test_strategy6_v3_5_fast_scratch_exits_early_adverse() -> None:
    rows = _rows(8)
    rows[1]["high"] = 100.05
    rows[1]["low"] = 99.55
    rows[1]["close"] = 99.55
    order = {
        "symbol": "BTCUSDT",
        "strategy_line": "strategy6",
        "side": "LONG",
        "entry_time_ms": rows[1]["open_time_ms"],
        "entry_idx": 1,
        "entry_price": 100.0,
        "stop_loss": 99.0,
        "take_profit": 102.0,
        "planned_rr": 2.0,
        "cost_bps": 0.0,
        "features": {"strategy6_adaptive_exit_tier": "v3_5_fast_scratch"},
        "fast_exit_policy": {},
    }

    protected = simulate_1m_fill(
        order,
        rows,
        {
            "strategy_line": "strategy6",
            "max_hold_minutes": 4,
            "strategy6_adaptive_exit_enabled": True,
            "v3_5_early_adverse_R": 0.4,
            "v3_5_scratch_after_minutes": 2,
            "first_tp_R": 0,
        },
    )

    assert protected["exit_reason"] in {"strategy6_initial_adverse_exit", "strategy6_direction_wrong_early_abort"}
    assert protected["net_R"] > -0.5
    assert protected["exit_protection_reason"] in {"max_initial_adverse_R", "direction_wrong_early_abort"}
