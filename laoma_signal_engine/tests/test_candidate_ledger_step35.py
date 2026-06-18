from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from laoma_signal_engine.candidate_ledger import (
    FORBIDDEN_DECISION_FEATURE_FIELDS,
    candidate_ledger_dir,
    candidate_ledger_table_counts,
)
from laoma_signal_engine.strategy_sandbox.full_pipeline import run_sandbox_full_pipeline
from laoma_signal_engine.strategy_sandbox.resource_governor import start_ui_sandbox_pipeline_context


def _docs() -> dict:
    return {
        "without_micro": {
            "schema_version": "STEP35_fixture_trade_plan_v1",
            "generated_at": "2026-06-18T00:00:00Z",
            "run_id": "run_step35",
            "cycle_id": "cycle_step35",
            "source": "trade_plan_without_micro",
            "plans": [
                {
                    "symbol": "OPGUSDT",
                    "decision": "LONG",
                    "action": "ENTER_MARKET",
                    "entry_mode": "MARKET",
                    "estimated_entry_price": 1.0,
                    "stop_loss": 0.9,
                    "take_profit": 1.1,
                    "rr": 1.0,
                    "executable": True,
                    "confidence": 80,
                    "reason_codes": ["allow_fixture"],
                    "guards": {"margin_usdt": 100, "leverage": 20},
                },
                {
                    "symbol": "OPGUSDT",
                    "decision": "SHORT",
                    "action": "ENTER_MARKET",
                    "entry_mode": "MARKET",
                    "estimated_entry_price": 1.0,
                    "stop_loss": 1.1,
                    "take_profit": 0.9,
                    "rr": 1.0,
                    "executable": False,
                    "confidence": 20,
                    "reason_codes": ["blocked_fixture"],
                    "guards": {"margin_usdt": 100, "leverage": 20},
                },
            ],
        }
    }


def _docs_two_blocked() -> dict:
    doc = _docs()
    doc["without_micro"]["plans"].append(
        {
            "symbol": "OPGUSDT",
            "decision": "LONG",
            "action": "ENTER_MARKET",
            "entry_mode": "MARKET",
            "estimated_entry_price": 1.0,
            "stop_loss": 0.9,
            "take_profit": 1.12,
            "rr": 1.2,
            "executable": False,
            "confidence": 15,
            "reason_codes": ["blocked_fixture_second"],
            "guards": {"margin_usdt": 100, "leverage": 20},
        }
    )
    return doc


def test_step351_candidate_ledger_path_is_sidecar(tmp_path: Path) -> None:
    baseline = candidate_ledger_dir(tmp_path, run_id="run_a", source_mode="baseline_backtest")
    sandbox = candidate_ledger_dir(tmp_path, run_id="run_b", source_mode="ui_sandbox_full_pipeline", sandbox_id="sb_a")

    assert baseline.as_posix().endswith("DATA/research/candidate_ledger/baseline/run_a")
    assert sandbox.as_posix().endswith("DATA/research/candidate_ledger/sandbox_exports/sb_a/run_b")
    assert "paper_trading.db" not in (sandbox / "candidate_ledger.db").as_posix()
    assert "exit_price" in FORBIDDEN_DECISION_FEATURE_FIELDS


def test_step352_to_356_sandbox_full_pipeline_writes_candidate_sidecar_without_polluting_paper_db(tmp_path: Path) -> None:
    run = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_step35",
        active_sandbox_id="sb_step35",
        dry_run=False,
        options={"run_id": "run_step35", "strategy_line": "without_micro", "symbol": "OPGUSDT", "pipeline_mode": "sandbox_full_pipeline"},
    )
    result = run_sandbox_full_pipeline(
        tmp_path,
        sandbox_id="sb_step35",
        run_id="run_step35",
        cycle_id=run["writer_context"]["context"]["cycle_id"],
        writer_context=run["writer_context"],
        options={
            "docs": _docs(),
            "symbol": "OPGUSDT",
            "max_ticks": 2,
            "candles_by_symbol": {
                "OPGUSDT": [
                    {"symbol": "OPGUSDT", "open_time_ms": 1_781_740_800_000, "open": 1.0, "high": 1.02, "low": 0.99, "close": 1.01, "volume": 1000},
                    {"symbol": "OPGUSDT", "open_time_ms": 1_781_740_860_000, "open": 1.01, "high": 1.12, "low": 1.0, "close": 1.1, "volume": 1000},
                ]
            },
        },
    )

    candidate = result["candidate_ledger"]
    assert candidate["candidate_ledger_status"] == "completed"
    assert candidate["candidate_count"] == 2
    assert candidate["source_gate_decision_count"] == 2
    assert candidate["trade_result_count"] == 2
    assert candidate["candidate_order_link_count"] == 2
    assert candidate["executed_result_count"] == 1
    assert candidate["blocked_without_outcome_count"] == 0
    assert candidate["counterfactual_result_count"] == 1
    assert candidate["coverage"]["candidate_to_source_gate_link_rate"] == 1.0
    assert candidate["coverage"]["candidate_to_result_link_rate"] == 1.0
    assert candidate["coverage"]["candidate_forbidden_field_violation_count"] == 0
    assert candidate["coverage"]["blocked_without_outcome_is_not_labeled_bad"] is True

    db_path = tmp_path / candidate["candidate_ledger_db_path"]
    assert db_path.exists()
    assert candidate_ledger_table_counts(db_path)["trade_candidates"] == 2
    assert candidate_ledger_table_counts(db_path)["candidate_order_links"] == 2
    assert (tmp_path / candidate["candidate_ledger_mirror_db_path"]).exists()

    manifest = json.loads((tmp_path / candidate["candidate_gate_result_manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["sidecar_only"] is True
    assert manifest["candidate_count"] == 2
    assert manifest["blocked_candidate_outcome_policy"]
    assert manifest["blocked_without_outcome_training_eligible"] is False
    assert manifest["blocked_with_counterfactual_training_eligible"] == "evaluation_only"
    assert manifest["baseline_vs_gated_comparable"] is False
    assert manifest["baseline_vs_gated_not_comparable_reason"] == "ai_gated_replay_not_run"
    assert manifest["candidate_order_links_hash"]
    assert manifest["runtime_mirror_hash_match"] is True

    paper_db = tmp_path / result["paper_db_path"]
    with sqlite3.connect(paper_db) as con:
        paper_orders = con.execute("SELECT COUNT(*) FROM paper_orders").fetchone()[0]
        paper_fills = con.execute("SELECT COUNT(*) FROM paper_fills").fetchone()[0]
    assert paper_orders == 1
    assert paper_fills == 2

    results = [
        json.loads(line)
        for line in (tmp_path / candidate["trade_results_path"]).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    blocked = [row for row in results if row["executed"] is False]
    assert blocked and blocked[0]["outcome_source"] == "counterfactual_replay_estimated"
    assert blocked[0]["outcome_confidence"] == "medium"
    assert blocked[0]["quality_label"] in {"winner", "loser"}

    candidates = [
        json.loads(line)
        for line in (tmp_path / candidate["candidate_signals_path"]).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert all(row["feature_schema_version"] == "step29_decision_time_input_v2" for row in candidates)
    assert all(row["known_at_policy_version"] == "step29_known_at_policy_v2" for row in candidates)
    links = [
        json.loads(line)
        for line in (tmp_path / candidate["candidate_order_links_path"]).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {row["link_status"] for row in links} == {"executed", "blocked"}
    assert all(row["link_confidence"] == "exact" for row in links)


def test_step358_candidate_ledger_batches_counterfactual_replay(tmp_path: Path) -> None:
    run = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_step358",
        active_sandbox_id="sb_step358",
        dry_run=False,
        options={"run_id": "run_step358", "strategy_line": "without_micro", "symbol": "OPGUSDT", "pipeline_mode": "sandbox_full_pipeline"},
    )
    result = run_sandbox_full_pipeline(
        tmp_path,
        sandbox_id="sb_step358",
        run_id="run_step358",
        cycle_id=run["writer_context"]["context"]["cycle_id"],
        writer_context=run["writer_context"],
        options={
            "docs": _docs_two_blocked(),
            "symbol": "OPGUSDT",
            "max_ticks": 2,
            "candles_by_symbol": {
                "OPGUSDT": [
                    {"symbol": "OPGUSDT", "open_time_ms": 1_781_740_800_000, "open": 1.0, "high": 1.02, "low": 0.99, "close": 1.01, "volume": 1000},
                    {"symbol": "OPGUSDT", "open_time_ms": 1_781_740_860_000, "open": 1.01, "high": 1.12, "low": 0.88, "close": 1.1, "volume": 1000},
                ]
            },
        },
    )

    candidate = result["candidate_ledger"]
    assert candidate["candidate_count"] == 3
    assert candidate["executed_result_count"] == 1
    assert candidate["counterfactual_result_count"] == 2
    assert candidate["counterfactual_replay_mode"] == "fast_sidecar_paper_equivalent_estimator"
    fast_results = tmp_path / candidate["counterfactual_fast_results_path"]
    assert fast_results.exists()
    assert fast_results.as_posix().endswith("cf_fast/trade_results.jsonl")

    old_cf_dir = (tmp_path / candidate["candidate_ledger_dir"]) / "cf"
    assert not old_cf_dir.exists()
    results = [
        json.loads(line)
        for line in (tmp_path / candidate["trade_results_path"]).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    blocked = [row for row in results if row["executed"] is False]
    assert len(blocked) == 2
    assert {row["outcome_source"] for row in blocked} == {"counterfactual_replay_estimated"}
