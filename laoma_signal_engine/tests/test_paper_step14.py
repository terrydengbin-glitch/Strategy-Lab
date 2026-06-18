from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

import laoma_signal_engine.api.services as api_services
from laoma_signal_engine.api.app import app
from laoma_signal_engine.paper.candles import StaticCandleProvider
from laoma_signal_engine.paper.archive import archive_reset_strategy
from laoma_signal_engine.paper.daemon import inspect_tick_lock, run_once as paper_daemon_run_once
from laoma_signal_engine.paper.engine import PaperEngine
from laoma_signal_engine.paper.models import Candle, PaperConfig
from laoma_signal_engine.paper.storage import PaperStore
from laoma_signal_engine.cli import main


def _doc(line: str, *, symbol: str = "OPGUSDT", entry: float = 1.0, sl: float = 0.9, tp: float = 1.1) -> dict:
    source = {
        "without_micro": "trade_plan_without_micro",
        "micro_fast": "trade_plan_micro_fast",
        "micro_full": "trade_plan_micro_full",
    }[line]
    mode = {"without_micro": "none", "micro_fast": "fast", "micro_full": "full"}[line]
    return {
        "schema_version": "1.0",
        "generated_at": "2026-05-26T00:00:00Z",
        "run_id": "run_test",
        "cycle_id": "cycle_test",
        "source": source,
        "micro_mode": mode,
        "status": "ok",
        "count": 1,
        "executable_count": 1,
        "input_refs": {},
        "plans": [
            {
                "symbol": symbol,
                "decision_tf": "15m",
                "decision": "LONG",
                "action": "ENTER_MARKET",
                "entry_mode": "MARKET",
                "estimated_entry_price": entry,
                "stop_loss": sl,
                "take_profit": tp,
                "risk_per_unit": entry - sl,
                "reward_per_unit": tp - entry,
                "rr": 1.0,
                "executable": True,
                "confidence": 80,
                "reason_codes": [],
                "guards": {
                    "line": line,
                    "margin_usdt": 100,
                    "leverage": 20,
                    **(
                        {}
                        if line == "without_micro"
                        else {
                            "micro_symbol_confirmed": True,
                            "micro_direction_confirmed": True,
                            "micro_exec_allowed": True,
                            "micro_exec_allowed_reason": "allowed",
                            "trade_plan_consumable": True,
                        }
                    ),
                },
                "input_refs": {},
            }
        ],
    }


def _conditional_doc(line: str, *, symbol: str = "OPGUSDT") -> dict:
    doc = _doc(line, symbol=symbol)
    doc["executable_count"] = 0
    plan = doc["plans"][0]
    plan.update(
        {
            "action": "ENTER_LIMIT",
            "entry_mode": "LIMIT_PULLBACK",
            "estimated_entry_price": None,
            "stop_loss": None,
            "take_profit": None,
            "risk_per_unit": None,
            "reward_per_unit": None,
            "rr": None,
            "executable": False,
            "reason_codes": ["limit_entry_available"],
            "guards": {
                "line": line,
                "conditional_entry_allowed": True,
                "limit_entry_allowed": True,
                "better_entry_price": 0.98,
                "structure_stop": 0.94,
                "tp2": 1.08,
                "tp1": 1.04,
                "opportunity_type": "LIMIT_PULLBACK",
                "margin_usdt": 100,
                "leverage": 20,
            },
        },
    )
    return doc


def _config() -> PaperConfig:
    return PaperConfig(
        db_path="DATA/paper/test_paper.db",
        summary_path="DATA/paper/latest_paper_state.json",
        default_slippage_bps=0,
        taker_fee_bps=5,
    )


def _strategy_v5_doc(
    line: str,
    *,
    symbol: str = "BTCUSDT",
    entry: float = 100.0,
    sl: float = 99.0,
    tp: float = 101.0,
    side_flow_alignment: str = "same",
    price_flow_alignment: str = "same",
) -> dict:
    return {
        "schema_version": "1.0",
        "generated_at": "2026-06-15T00:00:00Z",
        "run_id": "run_v5_gate",
        "cycle_id": "cycle_v5_gate",
        "source": f"trade_plan_{line}",
        "micro_mode": "none",
        "status": "ok",
        "count": 1,
        "executable_count": 1,
        "input_refs": {},
        "plans": [
            {
                "symbol": symbol,
                "decision_tf": "15m",
                "decision": "LONG",
                "action": "ENTER_MARKET",
                "entry_mode": "MARKET",
                "estimated_entry_price": entry,
                "stop_loss": sl,
                "take_profit": tp,
                "risk_per_unit": entry - sl,
                "reward_per_unit": tp - entry,
                "rr": 1.0,
                "executable": True,
                "confidence": 80,
                "reason_codes": [],
                "guards": {
                    "line": line,
                    "margin_usdt": 100,
                    "leverage": 20,
                    "side_flow_alignment": side_flow_alignment,
                    "price_flow_alignment": price_flow_alignment,
                },
                "input_refs": {},
            }
        ],
    }


def _write_v5_gate_config(tmp_path: Path) -> None:
    gate_dir = tmp_path / "DATA" / "paper"
    gate_dir.mkdir(parents=True, exist_ok=True)
    (gate_dir / "v5_trade_gate_experiment.json").write_text(
        """
        {
          "enabled": true,
          "experiment_id": "paper_exp_step7_135_test",
          "paper_epoch_id": "epoch_step7_135_test",
          "mode": "paper_experiment",
          "feature_missing_policy": "block",
          "rules": {
            "strategy5": {
              "parameter_set_id": "p21v2_72340cb432fa7977",
              "gate_candidate_id": "strategy5_opposite_flow_gate",
              "action": "block",
              "rule_json": {
                "operator": "AND",
                "rules": [
                  {"field": "side_flow_alignment", "op": "eq", "value": "opposite"},
                  {"field": "price_flow_alignment", "op": "eq", "value": "opposite"}
                ]
              }
            }
          }
        }
        """,
        encoding="utf-8",
    )


def test_step14_sqlite_initializes_strategy_ledgers(tmp_path: Path) -> None:
    store = PaperStore(tmp_path, _config())

    store.initialize()
    accounts = store.row_dicts("paper_accounts", limit=10)

    assert {row["strategy_line"] for row in accounts} == {
        "without_micro",
        "micro_fast",
        "micro_full",
        "strategy4",
        "strategy5",
        "strategy6",
    }


def test_step14_v5_trade_gate_blocks_strategy5_before_paper_order(tmp_path: Path) -> None:
    _write_v5_gate_config(tmp_path)
    engine = PaperEngine(tmp_path, config=_config())

    result = engine.consume_trade_plans(
        {
            "strategy5": _strategy_v5_doc(
                "strategy5",
                side_flow_alignment="opposite",
                price_flow_alignment="opposite",
            )
        }
    )

    assert result["created"] == 0
    assert any(row["skip_reason"] == "v5_trade_gate_blocked" for row in result["skipped"])
    skips = engine.store.row_dicts("paper_skip_ledger", line="strategy5", limit=5)
    assert skips[0]["skip_reason"] == "skipped_v5_trade_gate_blocked"
    assert skips[0]["experiment_id"] == "paper_exp_step7_135_test"
    assert skips[0]["gate_candidate_id"] == "strategy5_opposite_flow_gate"
    assert skips[0]["gate_decision"] == "blocked"
    inbox = engine.store.intent_rows(line="strategy5", limit=5)
    assert inbox[0]["gate_candidate_id"] == "strategy5_opposite_flow_gate"
    assert inbox[0]["gate_decision"] == "blocked"


def test_step14_v5_trade_gate_passes_strategy5_with_lineage(tmp_path: Path) -> None:
    _write_v5_gate_config(tmp_path)
    engine = PaperEngine(tmp_path, config=_config())

    result = engine.consume_trade_plans({"strategy5": _strategy_v5_doc("strategy5")})

    assert result["created"] == 1
    orders = engine.store.row_dicts("paper_orders", line="strategy5", limit=5)
    assert orders[0]["experiment_id"] == "paper_exp_step7_135_test"
    assert orders[0]["gate_candidate_id"] == "strategy5_opposite_flow_gate"
    assert orders[0]["gate_decision"] == "pass"
    inbox = engine.store.intent_rows(line="strategy5", limit=5)
    assert inbox[0]["gate_candidate_id"] == "strategy5_opposite_flow_gate"
    assert inbox[0]["gate_decision"] == "pass"


def test_step2915_paper_tick_runs_trade_quality_before_training_sync(tmp_path: Path) -> None:
    engine = PaperEngine(
        tmp_path,
        config=_config(),
        candle_provider=StaticCandleProvider(
            {"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.12, 0.99, 1.1), Candle("OPGUSDT", 2, 1.1, 1.12, 1.0, 1.1)]}
        ),
    )

    result = engine.tick({"without_micro": _doc("without_micro")})

    assert result["entries"]["entered"] == 1
    assert result["closes"]["closed"] == 1
    assert result["trade_quality_completion"]["trade_quality_completion_status"] == "ok"
    assert result["trade_quality_completion"]["sample_count"] == 1
    assert result["training_dataset"]["samples_written"] == 1
    with sqlite3.connect(tmp_path / "DATA" / "paper" / "test_paper.db") as conn:
        tq_count = conn.execute("SELECT COUNT(*) FROM trade_quality_samples").fetchone()[0]
    assert tq_count == 1
    sidecar = tmp_path / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db"
    with sqlite3.connect(sidecar) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT decision_time_input_json, label_json, data_quality_json FROM trade_training_samples").fetchone()
    decision = json.loads(row["decision_time_input_json"])
    label = json.loads(row["label_json"])
    dq = json.loads(row["data_quality_json"])
    assert "root_cause_label" not in json.dumps(decision)
    assert label["trade_quality_provider"] == "trade_quality_module"
    assert dq["trade_quality_provider"] == "trade_quality_module"


def test_step14_v5_trade_gate_derives_missing_features_from_factor_snapshot(tmp_path: Path) -> None:
    _write_v5_gate_config(tmp_path)
    factor_dir = tmp_path / "DATA" / "factors"
    factor_dir.mkdir(parents=True, exist_ok=True)
    (factor_dir / "latest_factor_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-06-15T00:00:01Z",
                "items": [
                    {
                        "symbol": "BTCUSDT",
                        "primary_15m": {
                            "price_ret": 1.2,
                            "taker_buy_ratio": 0.42,
                            "kline_cvd_state": "sell_dominant",
                        },
                        "funding_context": {
                            "funding_rate_raw": -0.0006,
                            "funding_bucket": "NEGATIVE_EXTREME",
                        },
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    engine = PaperEngine(tmp_path, config=_config())
    doc = _strategy_v5_doc("strategy5")
    doc["plans"][0]["guards"].pop("side_flow_alignment", None)
    doc["plans"][0]["guards"].pop("price_flow_alignment", None)

    result = engine.consume_trade_plans({"strategy5": doc})

    assert result["created"] == 0
    skips = engine.store.row_dicts("paper_skip_ledger", line="strategy5", limit=5)
    assert skips[0]["gate_decision"] == "blocked"
    assert "side_flow_alignment" in skips[0]["gate_features_json"]
    assert "price_flow_alignment" in skips[0]["gate_features_json"]


def test_step14_same_symbol_can_be_active_across_strategy_lines(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)

    result = engine.tick({"micro_fast": _doc("micro_fast"), "micro_full": _doc("micro_full")})

    assert result["consume"]["created"] == 2
    orders = engine.store.row_dicts("paper_orders", limit=10)
    assert {row["strategy_line"] for row in orders} == {"micro_fast", "micro_full"}
    assert all(row["symbol"] == "OPGUSDT" for row in orders)


def test_step1055_paper_uses_per_run_archive_source_path(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    doc = _doc("micro_fast")
    archive_path = tmp_path / "DATA/decisions/trade_plan_runs/run_test/latest_trade_plan_micro_fast.json"
    doc["input_refs"]["trade_plan_archive_path"] = str(archive_path)
    doc["plans"][0]["input_refs"]["trade_plan_archive_path"] = str(archive_path)
    doc["plans"][0]["input_refs"]["source_plan_hash"] = "hash_from_archive_contract"

    result = engine.tick({"micro_fast": doc})

    assert result["consume"]["created"] == 1
    plan = engine.store.row_dicts("paper_trade_plans", limit=1)[0]
    assert plan["source_path"] == str(archive_path)
    assert plan["source_plan_hash"] == "hash_from_archive_contract"


def test_step1041_paper_consumes_position_sizing_not_fixed_margin(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    doc = _doc("micro_fast", entry=1.0, sl=0.99, tp=1.02)
    doc["plans"][0]["position_sizing"] = {
        "method": "fixed_risk",
        "risk_budget_usdt": 10,
        "quantity": 1000,
        "notional_usdt": 1000,
        "margin_usdt": 50,
        "leverage": 20,
        "estimated_max_loss_usdt": 11,
        "sizing_reject_reason": None,
    }

    result = engine.tick({"micro_fast": doc})

    assert result["consume"]["created"] == 1
    order = engine.store.row_dicts("paper_orders", limit=1)[0]
    assert order["margin_usdt"] == 50
    assert order["planned_notional_usdt"] == 1000
    assert order["risk_budget_usdt"] == 10
    assert order["notional_usdt"] == 1000
    assert order["quantity"] == 1000


def test_step1064_paper_rejects_missing_sizing_when_fallback_disallowed(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    cfg = PaperConfig(
        db_path="DATA/paper/test_paper.db",
        summary_path="DATA/paper/latest_paper_state.json",
        default_slippage_bps=0,
        taker_fee_bps=5,
        paper_fallback_notional_allowed=False,
    )
    engine = PaperEngine(tmp_path, config=cfg, candle_provider=provider)

    result = engine.tick({"micro_fast": _doc("micro_fast", entry=1.0, sl=0.9, tp=1.1)})

    assert result["consume"]["created"] == 0
    assert result["consume"]["skipped"][0]["skip_reason"] == "paper_fallback_notional_disallowed"
    assert engine.store.row_dicts("paper_orders", limit=10) == []


def test_step14_same_line_symbol_is_deduped_by_active_slot(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)

    first = engine.tick({"micro_fast": _doc("micro_fast")})
    second = engine.tick({"micro_fast": _doc("micro_fast")})

    assert first["consume"]["created"] == 1
    assert second["consume"]["created"] == 0
    assert second["consume"]["skipped"][0]["reason"] == "source_plan_hash_consumed"
    skips = engine.store.row_dicts("paper_skip_ledger", limit=10)
    assert len(skips) == 1
    assert skips[0]["skip_reason"] == "skipped_duplicate_plan_hash"
    assert skips[0]["executable"] == 1


def test_step1412_same_line_symbol_new_signal_is_blocked_by_active_slot(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    first_doc = _doc("micro_fast")
    second_doc = _doc("micro_fast")
    second_doc["generated_at"] = "2026-05-26T00:05:00Z"

    first = engine.tick({"micro_fast": first_doc})
    second = engine.tick({"micro_fast": second_doc})

    assert first["consume"]["created"] == 1
    assert second["consume"]["created"] == 0
    assert second["consume"]["skipped"][0]["reason"] == "active_slot_occupied"
    assert second["consume"]["skipped"][0]["slot_key"] == "micro_fast:OPGUSDT"
    skips = engine.store.row_dicts("paper_skip_ledger", limit=10)
    assert len(skips) == 1
    assert skips[0]["skip_reason"] == "skipped_same_symbol_open"
    assert skips[0]["source_run_id"] == "run_test"


def test_step1432_strategy4_sidecar_uses_origin_run_lineage(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    lineage = {
        "origin_run_id": "run_origin",
        "origin_cycle_id": "cycle_origin",
        "origin_strategy_line": "without_micro",
        "sidecar_strategy_line": "strategy4",
        "sidecar_attempt_id": "attempt_1",
        "sidecar_attempted_at": "2026-06-04T00:05:00Z",
        "original_side": "LONG",
        "current_side": "SHORT",
        "side_changed": True,
    }
    doc = {
        **_doc("without_micro"),
        "source": "trade_plan_strategy4",
        "run_id": None,
        "cycle_id": None,
    }
    doc["plans"][0]["decision"] = "SHORT"
    doc["plans"][0]["guards"] = {
        **doc["plans"][0]["guards"],
        "line": "strategy4",
        "strategy4_lineage": lineage,
    }

    result = engine.tick({"strategy4": doc})

    assert result["consume"]["created"] == 1
    intents = engine.store.row_dicts("paper_intent_inbox", line="strategy4", limit=10)
    orders = engine.store.row_dicts("paper_orders", line="strategy4", limit=10)
    assert intents[0]["source_run_id"] == "run_origin"
    assert intents[0]["source_cycle_id"] == "cycle_origin"
    assert orders[0]["source_run_id"] == "run_origin"
    assert orders[0]["source_cycle_id"] == "cycle_origin"
    assert intents[0]["source"]["strategy4_lineage"]["sidecar_attempt_id"] == "attempt_1"


def test_step1433_strategy4_uses_independent_slot_when_primary_line_is_open(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    primary = _doc("without_micro")
    strategy4 = {
        **_doc("without_micro"),
        "source": "trade_plan_strategy4",
        "run_id": None,
        "cycle_id": None,
        "generated_at": "2026-05-26T00:05:00Z",
    }
    strategy4["plans"][0]["guards"] = {
        **strategy4["plans"][0]["guards"],
        "line": "strategy4",
        "strategy4_lineage": {
            "origin_run_id": "run_origin_slot",
            "origin_cycle_id": "cycle_origin_slot",
            "origin_strategy_line": "without_micro",
            "sidecar_strategy_line": "strategy4",
            "sidecar_attempt_id": "attempt_slot",
        },
    }

    first = engine.tick({"without_micro": primary})
    second = engine.tick({"strategy4": strategy4})

    assert first["consume"]["created"] == 1
    assert second["consume"]["created"] == 1
    assert second["consume"]["skipped"] == []
    orders = engine.store.row_dicts("paper_orders", limit=10)
    assert [row["strategy_line"] for row in orders] == ["strategy4", "without_micro"]
    strategy4_order = orders[0]
    assert strategy4_order["source_run_id"] == "run_origin_slot"
    plans = engine.store.row_dicts("paper_trade_plans", line="strategy4", limit=1)
    assert plans[0]["source"]["cross_line_active_slot_observed"] is True
    assert plans[0]["source"]["paper_slot_evidence"]["slot_scope"] == "strategy_line"
    skips = engine.store.row_dicts("paper_skip_ledger", line="strategy4", limit=10)
    assert skips == []


def test_step1433_strategy4_same_line_slot_still_blocks_reentry(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    first_doc = {
        **_doc("without_micro"),
        "source": "trade_plan_strategy4",
        "run_id": None,
        "cycle_id": None,
    }
    first_doc["plans"][0]["guards"] = {**first_doc["plans"][0]["guards"], "line": "strategy4"}
    second_doc = {
        **_doc("without_micro"),
        "source": "trade_plan_strategy4",
        "run_id": None,
        "cycle_id": None,
        "generated_at": "2026-05-26T00:06:00Z",
    }
    second_doc["plans"][0]["confidence"] = 81
    second_doc["plans"][0]["guards"] = {**second_doc["plans"][0]["guards"], "line": "strategy4"}

    first = engine.tick({"strategy4": first_doc})
    second = engine.tick({"strategy4": second_doc})

    assert first["consume"]["created"] == 1
    assert second["consume"]["created"] == 0
    assert second["consume"]["skipped"][0]["reason"] == "active_slot_occupied"
    skips = engine.store.row_dicts("paper_skip_ledger", line="strategy4", limit=10)
    assert len(skips) == 1
    assert skips[0]["skip_reason"] == "skipped_same_symbol_open"


def test_step1411_conditional_limit_is_passed_not_paper_order(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)

    result = engine.tick({"micro_fast": _conditional_doc("micro_fast")})

    assert result["consume"]["intents"] == 0
    assert result["consume"]["created"] == 0
    assert result["consume"]["skipped"][0]["skip_reason"] == "non_executable"
    orders = engine.store.row_dicts("paper_orders", limit=10)
    assert orders == []


def test_step1411_executable_limit_is_rejected_as_pending_not_allowed(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    doc = _doc("micro_fast")
    doc["plans"][0]["action"] = "ENTER_LIMIT"
    doc["plans"][0]["entry_mode"] = "LIMIT_PULLBACK"

    result = engine.tick({"micro_fast": doc})

    assert result["consume"]["intents"] == 0
    assert result["consume"]["created"] == 0
    assert result["consume"]["skipped"][0]["skip_reason"] == "pending_not_allowed"
    assert engine.store.row_dicts("paper_orders", limit=10) == []
    skips = engine.store.row_dicts("paper_skip_ledger", limit=10)
    assert len(skips) == 1
    assert skips[0]["skip_reason"] == "skipped_adapter_invalid"


def test_step1053_paper_rejects_micro_consumable_false_even_if_executable(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"CLOUSDT": [Candle("CLOUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    doc = _doc("micro_fast", symbol="CLOUSDT")
    guards = doc["plans"][0]["guards"]
    guards.update(
        {
            "micro_symbol_confirmed": False,
            "micro_direction_confirmed": False,
            "micro_exec_allowed": False,
            "micro_exec_allowed_reason": "blocked_one_z_hint_only",
            "trade_plan_consumable": False,
            "consumption_block_reason": "micro_policy_blocked_by_config_or_evidence",
        }
    )
    doc["plans"][0]["reason_codes"] = ["fast_one_z_available_weak_only"]

    result = engine.tick({"micro_fast": doc})

    assert result["consume"]["intents"] == 0
    assert result["consume"]["created"] == 0
    skipped = result["consume"]["skipped"][0]
    assert skipped["skip_reason"] == "paper_reject_micro_consumable_false"
    assert skipped["micro_exec_allowed"] is False
    assert skipped["micro_exec_allowed_reason"] == "blocked_one_z_hint_only"
    assert engine.store.row_dicts("paper_orders", limit=10) == []


def test_step54_paper_rejects_invalid_exchange_symbol_even_if_executable(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    doc = _doc("without_micro", symbol="我踏马来了USDT")

    result = engine.tick({"without_micro": doc})

    assert result["consume"]["created"] == 0
    skipped = result["consume"]["skipped"][0]
    assert skipped["skip_reason"] == "paper_reject_invalid_exchange_symbol"
    assert skipped["symbol_contract_ok"] is False
    assert skipped["symbol_contract_reason"] == "invalid_symbol_format"
    assert engine.store.row_dicts("paper_orders", limit=10) == []


def test_step54_paper_rejects_symbol_not_in_exchange_whitelist(tmp_path: Path) -> None:
    market_dir = tmp_path / "DATA" / "market"
    market_dir.mkdir(parents=True)
    (market_dir / "futures_light_snapshot.json").write_text(
        '{"schema_version":"1.6","items":[{"symbol":"BTCUSDT"}]}',
        encoding="utf-8",
    )
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    doc = _doc("without_micro", symbol="OPGUSDT")
    doc["__project_root"] = str(tmp_path)

    result = engine.tick({"without_micro": doc})

    assert result["consume"]["created"] == 0
    skipped = result["consume"]["skipped"][0]
    assert skipped["skip_reason"] == "paper_reject_invalid_exchange_symbol"
    assert skipped["symbol_contract_reason"] == "not_in_exchange_whitelist"
    assert skipped["symbol_contract_source"] == "futures_light_snapshot"
    assert engine.store.row_dicts("paper_orders", limit=10) == []


def test_step1053_paper_allows_relaxed_micro_when_policy_and_evidence_allow(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"CLOUSDT": [Candle("CLOUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    doc = _doc("micro_fast", symbol="CLOUSDT")
    guards = doc["plans"][0]["guards"]
    guards.update(
        {
            "micro_consumption_policy": "ready_signal_usable",
            "allow_weak_micro_consumption": True,
            "micro_symbol_confirmed": False,
            "micro_direction_confirmed": False,
            "micro_exec_allowed": False,
            "micro_exec_allowed_reason": "blocked_one_z_hint_only",
            "trade_plan_consumable": True,
            "micro_policy_relaxed": True,
            "micro_confirmation_strength": "weak",
            "micro_lifecycle_state": "confirmed",
            "consumption_block_reason": "",
        }
    )
    doc["plans"][0]["reason_codes"] = ["fast_one_z_available_weak_only"]

    result = engine.tick({"micro_fast": doc})

    assert result["consume"]["created"] == 1
    order = engine.store.row_dicts("paper_orders", limit=10)[0]
    assert order["strategy_line"] == "micro_fast"
    assert order["symbol"] == "CLOUSDT"
    assert order["signal_class"] == "relaxed_micro_test"


def test_step1053_paper_keeps_confirmed_only_strict_for_weak_micro(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"CLOUSDT": [Candle("CLOUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    doc = _doc("micro_fast", symbol="CLOUSDT")
    guards = doc["plans"][0]["guards"]
    guards.update(
        {
            "micro_consumption_policy": "confirmed_only",
            "allow_weak_micro_consumption": True,
            "micro_symbol_confirmed": False,
            "micro_direction_confirmed": False,
            "micro_exec_allowed": False,
            "micro_exec_allowed_reason": "blocked_one_z_hint_only",
            "trade_plan_consumable": True,
            "micro_policy_relaxed": True,
            "micro_confirmation_strength": "weak",
            "micro_lifecycle_state": "confirmed",
            "consumption_block_reason": "",
        }
    )

    result = engine.tick({"micro_fast": doc})

    assert result["consume"]["created"] == 0
    skipped = result["consume"]["skipped"][0]
    assert skipped["skip_reason"] == "paper_reject_confirmed_only_non_confirmed_micro_symbol"
    assert engine.store.row_dicts("paper_orders", limit=10) == []


def test_step1052_without_micro_does_not_require_micro_guards(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)

    result = engine.tick({"without_micro": _doc("without_micro")})

    assert result["consume"]["created"] == 1
    assert engine.store.row_dicts("paper_orders", limit=10)[0]["strategy_line"] == "without_micro"


def test_step14_market_entry_then_take_profit_updates_summary(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.2, 0.99, 1.12)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)

    result = engine.tick({"micro_fast": _doc("micro_fast", tp=1.1)})

    assert result["entries"]["entered"] == 1
    assert result["closes"]["closed"] == 1
    summary = result["summary"]
    stats = summary["stats"]["by_line"]["micro_fast"]
    assert stats["total_orders"] == 1
    assert stats["closed_orders"] == 1
    assert stats["winning_trades"] == 1
    assert stats["fee_usdt"] > 0
    assert (tmp_path / "DATA/paper/latest_paper_state.json").exists()


def test_step1413_closed_and_open_ledgers_use_event_time_order(tmp_path: Path) -> None:
    provider = StaticCandleProvider(
        {
            "AAAUSDT": [Candle("AAAUSDT", 1, 1.0, 1.2, 0.99, 1.12)],
            "BBBUSDT": [Candle("BBBUSDT", 1, 1.0, 1.2, 0.99, 1.12)],
            "CCCUSDT": [Candle("CCCUSDT", 1, 1.0, 1.05, 0.95, 1.01)],
            "DDDUSDT": [Candle("DDDUSDT", 1, 1.0, 1.05, 0.95, 1.01)],
        }
    )
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    engine.tick({"micro_fast": _doc("micro_fast", symbol="AAAUSDT", tp=1.1)})
    engine.tick({"micro_fast": _doc("micro_fast", symbol="BBBUSDT", tp=1.1)})
    engine.tick({"without_micro": _doc("without_micro", symbol="CCCUSDT", sl=0.8, tp=1.3)})
    engine.tick({"without_micro": _doc("without_micro", symbol="DDDUSDT", sl=0.8, tp=1.3)})

    with engine.store.connect() as conn:
        aaa = conn.execute("SELECT id FROM paper_orders WHERE symbol='AAAUSDT'").fetchone()["id"]
        bbb = conn.execute("SELECT id FROM paper_orders WHERE symbol='BBBUSDT'").fetchone()["id"]
        ccc = conn.execute("SELECT id FROM paper_positions WHERE symbol='CCCUSDT'").fetchone()["id"]
        ddd = conn.execute("SELECT id FROM paper_positions WHERE symbol='DDDUSDT'").fetchone()["id"]
        conn.execute("UPDATE paper_orders SET closed_at='2026-05-26T00:10:00Z', updated_at='2026-05-26T00:10:00Z' WHERE id=?", (aaa,))
        conn.execute("UPDATE paper_orders SET closed_at='2026-05-26T00:01:00Z', updated_at='2026-05-26T00:01:00Z' WHERE id=?", (bbb,))
        conn.execute("UPDATE paper_positions SET closed_at='2026-05-26T00:10:00Z', updated_at='2026-05-26T00:10:00Z' WHERE order_id=?", (aaa,))
        conn.execute("UPDATE paper_positions SET closed_at='2026-05-26T00:01:00Z', updated_at='2026-05-26T00:01:00Z' WHERE order_id=?", (bbb,))
        conn.execute("UPDATE paper_positions SET opened_at='2026-05-26T00:10:00Z', updated_at='2026-05-26T00:10:00Z' WHERE id=?", (ccc,))
        conn.execute("UPDATE paper_positions SET opened_at='2026-05-26T00:01:00Z', updated_at='2026-05-26T00:01:00Z' WHERE id=?", (ddd,))

    summary = engine.store.write_summary()

    assert summary["closed_orders"]["micro_fast"][0]["symbol"] == "AAAUSDT"
    assert summary["settled_positions"]["micro_fast"][0]["symbol"] == "AAAUSDT"
    assert summary["open_positions"]["without_micro"][0]["symbol"] == "CCCUSDT"
    assert summary["stats"]["by_line"]["micro_fast"]["closed_orders"] == 2


def test_step1411_summary_exposes_passed_signals(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.02, 0.98, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)

    result = engine.tick({"micro_fast": _conditional_doc("micro_fast")})

    assert result["summary"]["summary"]["skipped_signals"] == 1
    assert result["summary"]["skipped_signals"][0]["symbol"] == "OPGUSDT"


def test_step1411_quarantines_legacy_non_market_active_orders(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.01, 0.99, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    engine.tick({"micro_fast": _doc("micro_fast")})
    order = engine.store.row_dicts("paper_orders", limit=1)[0]

    with engine.store.connect() as conn:
        conn.execute(
            "UPDATE paper_orders SET source_executable=0, order_type='limit', source_action='ENTER_LIMIT', source_entry_mode='LIMIT_PULLBACK' WHERE id=?",
            (order["id"],),
        )

    result = engine.tick({})

    assert result["quarantine"]["cancelled_orders"] == 1
    assert engine.store.row_dicts("paper_positions", limit=1)[0]["status"] == "cancelled"


def test_step1410_executable_market_is_paper_and_notify_eligible(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.01, 0.99, 1.0)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)

    result = engine.tick({"without_micro": _doc("without_micro"), "micro_fast": _doc("micro_fast")})

    assert result["consume"]["created"] == 2
    orders = engine.store.row_dicts("paper_orders", limit=10)
    assert {row["strategy_line"] for row in orders} == {"without_micro", "micro_fast"}
    assert all(row["signal_class"] == "executable" for row in orders)
    assert all(row["paper_eligible"] == 1 for row in orders)
    assert all(row["notify_eligible"] == 1 for row in orders)
    assert all(row["source_executable"] == 1 for row in orders)
    assert all(row["source_action"] == "ENTER_MARKET" for row in orders)


def test_step12_paper_api_line_filter_and_invalid_line(tmp_path: Path, monkeypatch) -> None:
    provider = StaticCandleProvider({"OPGUSDT": [Candle("OPGUSDT", 1, 1.0, 1.2, 0.99, 1.12)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    engine.tick({"micro_fast": _doc("micro_fast", tp=1.1)})
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    client = TestClient(app)

    got = client.get("/api/paper/summary?line=micro_fast")
    bad = client.get("/api/paper/summary?line=bad_line")

    assert got.status_code == 200
    payload = got.json()
    assert payload["ok"] is True
    assert payload["data"]["line"] == "micro_fast"
    assert payload["data"]["stats"]["total_orders"] == 1
    assert bad.status_code == 400
    assert bad.json()["error"]["code"] == "invalid_strategy_line"


def test_step14_paper_daemon_cli_status_and_run_once(tmp_path: Path, capsys) -> None:
    code = main(["paper-daemon", "status", "--project-root", str(tmp_path), "--stdout-json"])

    assert code == 0
    out = capsys.readouterr().out
    assert "stopped" in out

    run_code = main(["paper-daemon", "run-once", "--project-root", str(tmp_path), "--stdout-json"])

    assert run_code == 0
    run_out = capsys.readouterr().out
    assert '"consume"' in run_out
    assert '"summary"' in run_out
    assert (tmp_path / "DATA/paper/latest_paper_state.json").exists()


def test_step1424_paper_daemon_recovers_stale_tick_lock(tmp_path: Path) -> None:
    cfg = _config()
    tick_lock = tmp_path / "DATA/runtime/paper_daemon.lock.tick"
    tick_lock.parent.mkdir(parents=True, exist_ok=True)
    tick_lock.write_text("99999999", encoding="utf-8")

    result = paper_daemon_run_once(tmp_path, config=cfg)

    assert result["tick_lock"]["acquired"] is True
    assert result["tick_lock"]["status"] == "stale_recovered"
    assert "paper_tick_lock_stale_recovered" in result["tick_lock"]["reason_codes"]
    assert not tick_lock.exists()


def test_step1428_paper_daemon_retries_recoverable_unlink_failure(tmp_path: Path, monkeypatch) -> None:
    cfg = _config()
    tick_lock = tmp_path / "DATA/runtime/paper_daemon.lock.tick"
    tick_lock.parent.mkdir(parents=True, exist_ok=True)
    tick_lock.write_text("99999999", encoding="utf-8")
    original_unlink = Path.unlink
    calls = {"count": 0}

    def flaky_unlink(self: Path, *args, **kwargs):
        if self == tick_lock and calls["count"] == 0:
            calls["count"] += 1
            raise OSError("simulated unlink race")
        return original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", flaky_unlink)

    result = paper_daemon_run_once(tmp_path, config=cfg)

    assert "consume" in result
    assert "summary" in result
    assert result["tick_lock"]["acquired"] is True
    assert result["tick_lock"]["status"] == "retry_acquired"
    assert "paper_tick_lock_unlink_failed_recovered" in result["tick_lock"]["reason_codes"]
    assert "paper_tick_lock_retry_acquired" in result["tick_lock"]["reason_codes"]
    assert result["tick_lock"]["retry_count"] >= 1
    assert not tick_lock.exists()


def test_step1424_paper_daemon_reports_alive_tick_lock_busy(tmp_path: Path) -> None:
    cfg = _config()
    tick_lock = tmp_path / "DATA/runtime/paper_daemon.lock.tick"
    tick_lock.parent.mkdir(parents=True, exist_ok=True)
    tick_lock.write_text(str(__import__("os").getpid()), encoding="utf-8")

    result = paper_daemon_run_once(tmp_path, config=cfg)

    assert result["status"] == "skipped"
    assert result["reason"] == "paper_tick_lock_alive_busy"
    assert result["tick_lock"]["pid_alive"] is True
    assert tick_lock.exists()
    tick_lock.unlink()


def test_step1428_paper_daemon_reports_other_alive_tick_lock_busy(tmp_path: Path) -> None:
    cfg = _config()
    tick_lock = tmp_path / "DATA/runtime/paper_daemon.lock.tick"
    tick_lock.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(10)"])
    try:
        tick_lock.write_text(str(proc.pid), encoding="utf-8")

        result = paper_daemon_run_once(tmp_path, config=cfg)

        assert result["status"] == "skipped"
        assert result["reason"] == "paper_tick_lock_alive_busy"
        assert result["tick_lock"]["pid"] == proc.pid
        assert result["tick_lock"]["pid_alive"] is True
        assert tick_lock.exists()
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        tick_lock.unlink(missing_ok=True)


def test_step1424_inspect_tick_lock_exposes_runtime_detail(tmp_path: Path) -> None:
    cfg = _config()
    tick_lock = tmp_path / "DATA/runtime/paper_daemon.lock.tick"
    tick_lock.parent.mkdir(parents=True, exist_ok=True)
    tick_lock.write_text("not-a-pid", encoding="utf-8")

    detail = inspect_tick_lock(tmp_path, cfg)

    assert detail["exists"] is True
    assert detail["parse_status"] == "malformed"
    assert detail["pid_alive"] is False


def test_step1415_archive_reset_force_closes_only_one_strategy_line(tmp_path: Path, monkeypatch) -> None:
    provider = StaticCandleProvider(
        {
            "AAAUSDT": [Candle("AAAUSDT", 1, 1.0, 1.05, 0.95, 1.01)],
            "BBBUSDT": [Candle("BBBUSDT", 1, 1.0, 1.05, 0.95, 1.01)],
        }
    )
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    engine.tick(
        {
            "micro_fast": _doc("micro_fast", symbol="AAAUSDT", sl=0.8, tp=1.3),
            "without_micro": _doc("without_micro", symbol="BBBUSDT", sl=0.8, tp=1.3),
        }
    )

    monkeypatch.setattr(
        "laoma_signal_engine.paper.archive.fetch_binance_1m_candles",
        lambda symbol, limit=1: [Candle(symbol, 123, 1.0, 1.02, 0.98, 1.02)],
    )
    result = archive_reset_strategy(tmp_path, strategy_line="micro_fast", profile_name="relaxed_test", config=_config())

    assert result["status"] == "archived_and_reset"
    assert result["experiment"]["forced_closed_positions"] == 1
    summary = result["summary_after_reset"]
    assert summary["stats"]["by_line"]["micro_fast"]["total_orders"] == 0
    assert summary["open_positions"]["micro_fast"] == []
    assert summary["stats"]["by_line"]["without_micro"]["total_orders"] == 1
    assert len(summary["open_positions"]["without_micro"]) == 1
    assert (tmp_path / result["experiment"]["paths"]["db_backup_path"]).exists()
    assert (tmp_path / result["experiment"]["paths"]["summary_snapshot_path"]).exists()

    archive_db = tmp_path / result["experiment"]["paths"]["db_backup_path"]
    with sqlite3.connect(archive_db) as conn:
        conn.row_factory = sqlite3.Row
        order = conn.execute("SELECT * FROM paper_orders WHERE strategy_line='micro_fast'").fetchone()
        fill = conn.execute("SELECT * FROM paper_fills WHERE strategy_line='micro_fast' AND action='archive_reset_forced_close'").fetchone()
    assert order["status"] == "closed"
    assert order["exit_reason"] == "archive_reset_forced_close"
    assert fill is not None


def test_step1222_paper_archive_reset_api_and_experiment_list(tmp_path: Path, monkeypatch) -> None:
    provider = StaticCandleProvider({"AAAUSDT": [Candle("AAAUSDT", 1, 1.0, 1.05, 0.95, 1.01)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    engine.tick({"micro_fast": _doc("micro_fast", symbol="AAAUSDT", sl=0.8, tp=1.3)})
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        "laoma_signal_engine.paper.archive.fetch_binance_1m_candles",
        lambda symbol, limit=1: [Candle(symbol, 123, 1.0, 1.02, 0.98, 1.02)],
    )
    client = TestClient(app)

    archived = client.post("/api/paper/archive-reset", json={"strategy_line": "micro_fast", "profile_name": "test"})
    experiments = client.get("/api/paper/experiments?line=micro_fast")

    assert archived.status_code == 200
    assert archived.json()["data"]["status"] == "archived_and_reset"
    assert experiments.status_code == 200
    rows = experiments.json()["data"]["experiments"]
    assert len(rows) == 1
    detail = client.get(f"/api/paper/experiments/{rows[0]['experiment_id']}")
    assert detail.status_code == 200
    assert detail.json()["data"]["metadata"]["strategy_line"] == "micro_fast"


def test_step1419_archive_epoch_blocks_old_current_json_after_reset(tmp_path: Path, monkeypatch) -> None:
    provider = StaticCandleProvider({"AAAUSDT": [Candle("AAAUSDT", 1, 1.0, 1.05, 0.95, 1.01)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    old_doc = _doc("micro_fast", symbol="AAAUSDT", sl=0.8, tp=1.3)
    first = engine.tick({"micro_fast": old_doc})
    assert first["consume"]["created"] == 1
    monkeypatch.setattr(
        "laoma_signal_engine.paper.archive.fetch_binance_1m_candles",
        lambda symbol, limit=1: [Candle(symbol, 123, 1.0, 1.02, 0.98, 1.02)],
    )
    archive_reset_strategy(tmp_path, strategy_line="micro_fast", profile_name="test", config=_config())

    after = engine.tick({"micro_fast": old_doc})

    assert after["consume"]["created"] == 0
    assert after["consume"]["skipped"][0]["reason"] == "source_trade_plan_before_archive_epoch"
    assert engine.store.row_dicts("paper_orders", line="micro_fast", limit=10) == []
    skips = engine.store.row_dicts("paper_skip_ledger", line="micro_fast", limit=10)
    assert skips[0]["skip_reason"] == "skipped_before_archive_epoch"


def test_step1420_entry_and_exit_are_idempotent(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"AAAUSDT": [Candle("AAAUSDT", 1, 1.0, 1.05, 0.95, 1.01)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)
    engine.consume_trade_plans({"micro_fast": _doc("micro_fast", symbol="AAAUSDT", sl=0.9, tp=1.1)})
    order = engine.store.row_dicts("paper_orders", line="micro_fast", limit=1)[0]
    candle = provider.get_1m("AAAUSDT", limit=1)[0]
    cost = engine._cost(1.0, "LONG", "entry", 100)

    first_position = engine.store.execute_entry(order, cost, quantity=100, at="2026-05-26T00:00:01Z", candle_ms=candle.open_time_ms)
    second_position = engine.store.execute_entry(order, cost, quantity=100, at="2026-05-26T00:00:02Z", candle_ms=candle.open_time_ms)

    assert first_position
    assert second_position is None
    assert len(engine.store.row_dicts("paper_positions", line="micro_fast", limit=10)) == 1
    position = engine.store.row_dicts("paper_positions", line="micro_fast", limit=1)[0]
    exit_cost = engine._cost(0.9, "LONG", "sl", 90)
    first_close = engine.store.close_position(position, exit_cost, gross_pnl=-10, exit_reason="SL", at="2026-05-26T00:01:00Z", candle_ms=2)
    second_close = engine.store.close_position(position, exit_cost, gross_pnl=-10, exit_reason="SL", at="2026-05-26T00:01:01Z", candle_ms=2)

    assert first_close is True
    assert second_close is False
    fills = engine.store.row_dicts("paper_fills", line="micro_fast", limit=10)
    assert len([row for row in fills if row["action"] == "entry"]) == 1
    assert len([row for row in fills if row["action"] == "SL"]) == 1


def test_step1422_reentry_cooldown_blocks_recent_sl(tmp_path: Path) -> None:
    cfg = PaperConfig(
        db_path="DATA/paper/test_paper.db",
        summary_path="DATA/paper/latest_paper_state.json",
        default_slippage_bps=0,
        taker_fee_bps=5,
        reentry_cooldown_sec=999999999,
    )
    provider = StaticCandleProvider({"AAAUSDT": [Candle("AAAUSDT", 1, 1.0, 1.05, 0.95, 1.01)]})
    engine = PaperEngine(tmp_path, config=cfg, candle_provider=provider)
    engine.tick({"micro_fast": _doc("micro_fast", symbol="AAAUSDT", sl=0.9, tp=1.1)})
    position = engine.store.row_dicts("paper_positions", line="micro_fast", limit=1)[0]
    engine.store.close_position(position, engine._cost(0.9, "LONG", "sl", 90), gross_pnl=-10, exit_reason="SL", at="2026-05-26T00:01:00Z", candle_ms=2)
    next_doc = _doc("micro_fast", symbol="AAAUSDT", sl=0.9, tp=1.1)
    next_doc["generated_at"] = "2026-05-26T00:02:00Z"

    result = engine.tick({"micro_fast": next_doc})

    assert result["consume"]["created"] == 0
    assert result["consume"]["skipped"][0]["reason"] == "reentry_cooldown_after_sl"


def test_step1243_paper_intents_and_epochs_api(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    provider = StaticCandleProvider({"AAAUSDT": [Candle("AAAUSDT", 1, 1.0, 1.05, 0.95, 1.01)]})
    engine = PaperEngine(tmp_path, config=PaperConfig(default_slippage_bps=0, taker_fee_bps=5), candle_provider=provider)
    engine.tick({"micro_fast": _doc("micro_fast", symbol="AAAUSDT", sl=0.8, tp=1.3)})
    client = TestClient(app)

    intents = client.get("/api/paper/intents?line=micro_fast")
    trace = client.get("/api/paper/trace?line=micro_fast&symbol=AAAUSDT")

    assert intents.status_code == 200
    assert intents.json()["data"]["rows"][0]["symbol"] == "AAAUSDT"
    assert trace.status_code == 200
    assert trace.json()["data"]["orders"][0]["source_run_id"] == "run_test"


def test_step1429_realistic_fill_contract_keeps_legacy_defaults(tmp_path: Path) -> None:
    provider = StaticCandleProvider({"AAAUSDT": [Candle("AAAUSDT", 1, 1.0, 1.05, 0.95, 1.01)]})
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=provider)

    result = engine.tick({"micro_fast": _doc("micro_fast", symbol="AAAUSDT", sl=0.8, tp=1.3)})

    fill = result["summary"]["recent_fills"]["micro_fast"][0]
    assert "planned_entry_price" in fill
    assert "entry_drift_bps" in fill
    assert "fill_model" in fill
    assert fill["fill_model"] == "fixed_1m"
    assert fill["cost_source"] == "paper_default"


def test_step1430_realistic_entry_uses_conservative_reference_and_dynamic_slippage(tmp_path: Path) -> None:
    cfg = PaperConfig(
        db_path="DATA/paper/test_paper.db",
        summary_path="DATA/paper/latest_paper_state.json",
        default_slippage_bps=5,
        taker_fee_bps=5,
        fill_model_mode="realistic_1m",
        use_trade_plan_slippage=True,
        use_liquidity_profile=True,
        max_entry_drift_bps=80,
        default_market_slippage_bps=5,
        volatility_slippage_mult=0,
        thin_book_slippage_mult=2,
    )
    provider = StaticCandleProvider({"AAAUSDT": [Candle("AAAUSDT", 1, 1.01, 1.05, 0.95, 1.01)]})
    engine = PaperEngine(tmp_path, config=cfg, candle_provider=provider)
    doc = _doc("micro_fast", symbol="AAAUSDT", entry=1.0, sl=0.8, tp=1.3)
    doc["plans"][0]["guards"]["estimated_slippage_bps"] = 12
    doc["plans"][0]["guards"]["symbol_execution_tier"] = "thin"

    result = engine.tick({"micro_fast": doc})

    fill = [row for row in result["summary"]["recent_fills"]["micro_fast"] if row["action"] == "entry"][0]
    assert fill["reference_price"] > 1.0
    assert fill["entry_drift_bps"] == 80.0
    assert fill["slippage_bps"] > 12
    assert fill["slippage_source"] == "trade_plan_dynamic"
    assert fill["cost_source"] == "paper_realistic_1m"


def test_step1249_paper_realism_api_contract(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    provider = StaticCandleProvider({"AAAUSDT": [Candle("AAAUSDT", 1, 1.0, 1.05, 0.95, 1.01)]})
    engine = PaperEngine(tmp_path, config=PaperConfig(default_slippage_bps=0, taker_fee_bps=5), candle_provider=provider)
    engine.tick({"micro_fast": _doc("micro_fast", symbol="AAAUSDT", sl=0.8, tp=1.3)})
    order = engine.store.row_dicts("paper_orders", line="micro_fast", limit=1)[0]
    client = TestClient(app)

    metrics = client.get("/api/paper/realism-metrics?line=micro_fast")
    trace = client.get(f"/api/paper/order-trace?order_id={order['id']}")
    reconciliation = client.get("/api/paper/reconciliation?run_id=run_test")

    assert metrics.status_code == 200
    assert metrics.json()["data"]["schema_version"] == "12.49"
    assert metrics.json()["data"]["metrics"]["fill_count"] >= 1
    assert trace.status_code == 200
    assert trace.json()["data"]["orders"][0]["id"] == order["id"]
    assert reconciliation.status_code == 200
    assert reconciliation.json()["data"]["counts"]["orders"] >= 1
