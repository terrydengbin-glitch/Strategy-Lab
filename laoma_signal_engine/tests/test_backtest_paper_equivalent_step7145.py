from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from laoma_signal_engine.backtest.paper_equivalent import (
    HistoricalCandleProvider,
    PaperEquivalentBacktestSession,
    default_paper_equivalent_config,
    run_paper_equivalent_backtest,
)
from laoma_signal_engine.paper.adapter import adapt_documents
from laoma_signal_engine.paper.models import Candle, STRATEGY_LINES, PaperConfig
from scripts.step7_146_strategy5_6_v5_gate_paper_equivalent_backtest import _selected_symbols, _trade_plan_doc
from scripts.step7_150_strategy1_2_3_4_minimal_paper_equivalent_smoke import run_smoke as run_step7150_smoke


def _doc(line: str, *, symbol: str, side: str = "LONG", entry: float = 100.0, sl: float = 95.0, tp: float = 105.0) -> dict:
    source = {
        "without_micro": "trade_plan_without_micro",
        "micro_fast": "trade_plan_micro_fast",
        "micro_full": "trade_plan_micro_full",
        "strategy4": "trade_plan_strategy4",
        "strategy5": "trade_plan_strategy5",
        "strategy6": "trade_plan_strategy6",
    }[line]
    guards = {
        "line": line,
        "margin_usdt": 100,
        "leverage": 20,
        "side_flow_alignment": "same",
        "price_flow_alignment": "same",
    }
    if line in {"micro_fast", "micro_full"}:
        guards.update(
            {
                "micro_symbol_confirmed": True,
                "micro_direction_confirmed": True,
                "micro_exec_allowed": True,
                "micro_exec_allowed_reason": "allowed",
                "trade_plan_consumable": True,
            }
        )
    return {
        "schema_version": "1.0",
        "generated_at": "2026-06-16T00:00:00Z",
        "run_id": f"run_{line}",
        "cycle_id": f"cycle_{line}",
        "source": source,
        "micro_mode": line,
        "status": "ok",
        "count": 1,
        "executable_count": 1,
        "input_refs": {},
        "plans": [
            {
                "symbol": symbol,
                "decision_tf": "15m",
                "decision": side,
                "action": "ENTER_MARKET",
                "entry_mode": "MARKET",
                "estimated_entry_price": entry,
                "stop_loss": sl,
                "take_profit": tp,
                "risk_per_unit": abs(entry - sl),
                "reward_per_unit": abs(tp - entry),
                "rr": 1.0,
                "executable": True,
                "confidence": 80,
                "reason_codes": [],
                "guards": guards,
                "input_refs": {},
            }
        ],
    }


def _write_strategy5_gate(root: Path, *, block: bool = False) -> None:
    path = root / "DATA" / "paper"
    path.mkdir(parents=True, exist_ok=True)
    value = "opposite" if block else "never_match"
    (path / "v5_trade_gate_experiment.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "experiment_id": "paper_equiv_gate_test",
                "paper_epoch_id": "paper_equiv_epoch",
                "feature_missing_policy": "block",
                "rules": {
                    "strategy5": {
                        "parameter_set_id": "p21v2_72340cb432fa7977",
                        "gate_candidate_id": "strategy5_opposite_flow_gate",
                        "action": "block",
                        "rule_json": {
                            "operator": "AND",
                            "rules": [
                                {"field": "side_flow_alignment", "op": "eq", "value": value},
                                {"field": "price_flow_alignment", "op": "eq", "value": value},
                            ],
                        },
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_step7145_historical_candle_provider_advances_by_cursor() -> None:
    provider = HistoricalCandleProvider(
        {"AAAUSDT": [Candle("AAAUSDT", 1, 100, 101, 99, 100), Candle("AAAUSDT", 2, 100, 106, 99, 105)]}
    )

    provider.advance_to(1)
    assert provider.get_1m("AAAUSDT")[-1].open_time_ms == 1
    provider.advance_to(2)
    assert provider.get_1m("AAAUSDT")[-1].open_time_ms == 2


def test_step7145_all_lines_adapt_to_paper_intents() -> None:
    docs = {line: _doc(line, symbol=f"PEQ{idx}USDT") for idx, line in enumerate(STRATEGY_LINES)}

    adapted = adapt_documents(docs, config=PaperConfig())

    assert {intent.strategy_line for intent in adapted["intents"]} == set(STRATEGY_LINES)
    assert adapted["skipped"] == []


def test_step7145_paper_equivalent_uses_isolated_ledger_and_closes_trade(tmp_path: Path) -> None:
    _write_strategy5_gate(tmp_path)
    docs = {"strategy5": _doc("strategy5", symbol="PEQUSDT")}
    result = run_paper_equivalent_backtest(
        tmp_path,
        docs=docs,
        candles_by_symbol={
            "PEQUSDT": [
                Candle("PEQUSDT", 1, 100, 101, 99, 100),
                Candle("PEQUSDT", 2, 100, 106, 99, 105),
            ]
        },
        run_id="peq_close",
        config=default_paper_equivalent_config(run_id="peq_close", base=PaperConfig(default_slippage_bps=0, taker_fee_bps=0)),
    )

    assert result["execution_contract"] == "paper_equivalent"
    assert result["default_backtest_execution_contract"] == "paper_equivalent"
    assert result["promotion_allowed"] is True
    assert result["equivalence_claim"] == "field_mapped_equivalent_to_paper_execution_chain"
    assert result["field_comparability"]["order_intent"].startswith("paper.adapter")
    assert result["consume"]["created"] == 1
    assert result["counts"]["paper_orders"] == 1
    assert result["counts"]["paper_positions"] == 1
    assert result["counts"]["paper_fills"] == 2
    assert result["trade_quality_completion"]["trade_quality_completion_status"] == "ok"
    assert result["trade_quality_completion"]["sample_count"] == 1
    assert result["training_dataset"]["samples_written"] == 1
    assert result["db_path"].endswith("DATA\\backtest\\paper_equivalent\\peq_close\\paper_equivalent.db") or result[
        "db_path"
    ].endswith("DATA/backtest/paper_equivalent/peq_close/paper_equivalent.db")
    assert not (tmp_path / "DATA" / "paper" / "paper_trading.db").exists()
    with sqlite3.connect(result["db_path"]) as conn:
        tq_count = conn.execute("SELECT COUNT(*) FROM trade_quality_samples").fetchone()[0]
    assert tq_count == 1
    sidecar = tmp_path / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db"
    with sqlite3.connect(sidecar) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT decision_time_input_json, post_trade_outcome_json, label_json, data_quality_json FROM trade_training_samples"
        ).fetchone()
    decision = json.loads(row["decision_time_input_json"])
    outcome = json.loads(row["post_trade_outcome_json"])
    label = json.loads(row["label_json"])
    dq = json.loads(row["data_quality_json"])
    assert "net_R" not in json.dumps(decision)
    assert "MFE_R" not in json.dumps(decision)
    assert outcome["trade_quality_provider"] == "trade_quality_module"
    assert label["trade_quality_module"] == "laoma_signal_engine.trade_quality.engine"
    assert label["training_label_ready"] is True
    assert dq["trade_quality_training_label_ready"] is True
    assert dq["trade_quality_provider"] == "trade_quality_module"


def test_step7145_paper_equivalent_preserves_v5_gate_block_lineage(tmp_path: Path) -> None:
    _write_strategy5_gate(tmp_path, block=True)
    docs = {"strategy5": _doc("strategy5", symbol="BLKUSDT")}
    docs["strategy5"]["plans"][0]["guards"]["side_flow_alignment"] = "opposite"
    docs["strategy5"]["plans"][0]["guards"]["price_flow_alignment"] = "opposite"

    result = run_paper_equivalent_backtest(
        tmp_path,
        docs=docs,
        candles_by_symbol={"BLKUSDT": [Candle("BLKUSDT", 1, 100, 101, 99, 100)]},
        run_id="peq_block",
    )

    assert result["consume"]["created"] == 0
    assert result["counts"]["paper_skip_ledger"] == 1
    assert result["consume"]["skipped"][0]["skip_reason"] == "v5_trade_gate_blocked"


def test_step7145_session_consumes_chronological_plans_with_same_symbol_slot(tmp_path: Path) -> None:
    _write_strategy5_gate(tmp_path)
    cfg = default_paper_equivalent_config(
        run_id="peq_session",
        base=PaperConfig(default_slippage_bps=0, taker_fee_bps=0),
    )
    session = PaperEquivalentBacktestSession(
        tmp_path,
        run_id="peq_session",
        config=cfg,
        candles_by_symbol={
            "SEQUSDT": [
                Candle("SEQUSDT", 1, 100, 101, 99, 100),
                Candle("SEQUSDT", 2, 100, 106, 99, 105),
                Candle("SEQUSDT", 3, 100, 101, 99, 100),
                Candle("SEQUSDT", 4, 100, 106, 99, 105),
            ]
        },
    )

    first = _doc("strategy5", symbol="SEQUSDT")
    second = _doc("strategy5", symbol="SEQUSDT")
    second["run_id"] = "run_second"
    second["cycle_id"] = "cycle_second"
    second["plans"][0]["confidence"] = 81

    assert session.consume_trade_plan({"strategy5": first}, at_ms=1)["created"] == 1
    assert session.consume_trade_plan({"strategy5": second}, at_ms=3)["created"] == 1
    result = session.finish()

    assert result["counts"]["paper_orders"] == 2
    assert result["counts"]["paper_fills"] == 4


def test_step7148_historical_trade_plan_wrapper_adds_paper_sizing_contract(tmp_path: Path) -> None:
    order = {
        "order_id": "hist_order_1",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "entry_price": 100.0,
        "stop_loss": 95.0,
        "take_profit": 105.0,
        "score": 80,
        "reasons": [],
        "entry_time_ms": 1,
        "trade_plan_payload": {
            "symbol": "BTCUSDT",
            "decision": "LONG",
            "guards": {"line": "strategy5"},
            "input_refs": {},
        },
    }

    doc = _trade_plan_doc(
        root=Path.cwd(),
        line="strategy5",
        order=order,
        features={},
        run_id="hist_run",
        cycle_id="hist_cycle",
        generated_at="2026-06-16T00:00:00Z",
    )
    plan = doc["plans"][0]
    sizing = plan["position_sizing"]

    assert sizing["planned_quantity"] > 0
    assert sizing["planned_notional_usdt"] > 0
    assert sizing["margin_usdt"] > 0
    adapted = adapt_documents({"strategy5": doc}, config=PaperConfig(paper_fallback_notional_allowed=False))
    assert len(adapted["intents"]) == 1
    assert adapted["skipped"] == []


def test_step7146_runner_supports_explicit_symbol_selection() -> None:
    selected, meta = _selected_symbols(
        ["BTCUSDT", "BEATUSDT", "BSBUSDT"],
        max_symbols=1,
        symbols=["beatusdt", "missingusdt", "bsbusdt"],
    )

    assert selected == ["BEATUSDT", "BSBUSDT"]
    assert meta == {
        "mode": "explicit",
        "requested": ["BEATUSDT", "MISSINGUSDT", "BSBUSDT"],
        "missing": ["MISSINGUSDT"],
    }


def test_step7150_minimal_smoke_runner_covers_strategy1_2_3_4(tmp_path: Path) -> None:
    payload = run_step7150_smoke(tmp_path, stamp="pytest")

    by_line = {row["strategy_line"]: row for row in payload["results"]}

    assert set(by_line) == {"without_micro", "micro_fast", "micro_full", "strategy4"}
    assert {row["equivalence_status"] for row in by_line.values()} == {"field_mapped_equivalent"}
    for row in by_line.values():
        counts = row["counts"]
        assert counts["paper_intent_inbox"] == 1
        assert counts["paper_orders"] == 1
        assert counts["paper_positions"] == 1
        assert counts["paper_fills"] == 2
        assert Path(row["db_path"]).exists()
        assert "DATA/backtest/paper_equivalent" in row["db_path"].replace("\\", "/")
    assert not (tmp_path / "DATA" / "paper" / "paper_trading.db").exists()
    assert (tmp_path / "DATA" / "backtest" / "step7_150_strategy1_2_3_4_minimal_paper_equivalent_smoke.json").exists()
