from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from laoma_signal_engine.training_snapshot_sync import sync_paper_sqlite_source


def _build_p21_db(root: Path, *, symbol: str = "TESTUSDT") -> None:
    db = root / "DATA" / "backtest" / "p21_parameter_optimization.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db) as con:
        con.execute(
            """
            CREATE TABLE p21_klines_1m (
                symbol TEXT, open_time_ms INTEGER, open REAL, high REAL, low REAL,
                close REAL, volume REAL, quote_volume REAL, taker_buy_base_volume REAL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE market_oi_15m (
                symbol TEXT, period TEXT, source_time_ms INTEGER, oi_change REAL, oi_z REAL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE market_funding_8h (
                symbol TEXT, funding_time_ms INTEGER, funding_rate REAL,
                funding_bucket TEXT, funding_crowded_side TEXT
            )
            """
        )
        rows = []
        for idx, open_ms in enumerate(range(0, 5_520_001, 60_000)):
            price = 1.0 + idx * 0.001
            rows.append((symbol, open_ms, price, price + 0.02, price - 0.01, price + 0.01, 1000 + idx, 1000, 500))
        con.executemany("INSERT INTO p21_klines_1m VALUES (?,?,?,?,?,?,?,?,?)", rows)
        con.execute("INSERT INTO market_oi_15m VALUES (?,?,?,?,?)", (symbol, "15m", 4_500_000, 0.12, 1.5))
        con.execute("INSERT INTO market_funding_8h VALUES (?,?,?,?,?)", (symbol, 4_800_000, 0.0001, "positive", "long"))
        con.commit()


def _build_paper_db(root: Path, *, symbol: str = "TESTUSDT") -> Path:
    db = root / "DATA" / "sandboxes" / "sb_step2923" / "runtime" / "pipeline_runs" / "run_step2923" / "paper" / "paper_trading.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db) as con:
        con.execute(
            """
            CREATE TABLE paper_orders (
                id TEXT PRIMARY KEY, intent_id TEXT, strategy_line TEXT, symbol TEXT, side TEXT,
                created_at INTEGER, entry_price REAL, stop_loss REAL, take_profit REAL,
                exit_price REAL, exit_reason TEXT, realized_pnl_usdt REAL,
                gate_candidate_id TEXT, gate_decision TEXT, gate_rule_json TEXT,
                gate_features_json TEXT, fill_model TEXT, cost_source TEXT,
                slippage_source TEXT, same_candle_policy TEXT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE paper_fills (
                id TEXT PRIMARY KEY, order_id TEXT, action TEXT, filled_at INTEGER,
                candle_open_time_ms INTEGER, fill_price REAL, quantity REAL,
                fee_bps REAL, fee_usdt REAL, slippage_bps REAL, slippage_usdt REAL,
                fill_model TEXT, cost_source TEXT, gross_pnl_usdt REAL, net_pnl_usdt REAL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE paper_positions (
                id TEXT PRIMARY KEY, order_id TEXT, status TEXT, opened_at INTEGER, closed_at INTEGER
            )
            """
        )
        con.execute(
            """
            CREATE TABLE trade_quality_samples (
                sample_id TEXT, order_id TEXT, net_R REAL, MFE_R REAL, MAE_R REAL,
                holding_time_sec REAL, exit_reason TEXT, root_cause_label TEXT,
                label_schema_version TEXT, exit_price REAL
            )
            """
        )
        con.execute(
            "INSERT INTO paper_orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "ord1",
                "intent1",
                "strategy5",
                symbol,
                "LONG",
                5_460_000,
                1.0,
                0.9,
                1.1,
                1.1,
                "TP",
                10.0,
                "gate1",
                "pass",
                '{"rules":[]}',
                '{"weekday":"3"}',
                "fixed_1m",
                "paper_default",
                "default",
                "sl_first",
            ),
        )
        con.execute(
            "INSERT INTO paper_fills VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("fill_entry", "ord1", "entry", 5_460_000, 5_400_000, 1.001, 100.0, 5.0, 0.5, 4.0, 0.4, "fixed_1m", "paper_default", 0.0, -0.5),
        )
        con.execute(
            "INSERT INTO paper_fills VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("fill_exit", "ord1", "TP", 5_520_000, 5_460_000, 1.1, 100.0, 5.0, 0.55, 4.0, 0.44, "fixed_1m", "paper_default", 10.0, 9.0),
        )
        con.execute("INSERT INTO paper_positions VALUES (?,?,?,?,?)", ("pos1", "ord1", "closed", 5_460_000, 5_520_000))
        con.execute(
            "INSERT INTO trade_quality_samples VALUES (?,?,?,?,?,?,?,?,?,?)",
            ("tq1", "ord1", 1.0, 1.2, -0.2, 60.0, "TP", "tp_hit_good_trade", "tq_v1", 1.1),
        )
        con.commit()
    return db


def test_step2923_training_export_v2_event_snapshots_and_manifest(tmp_path: Path) -> None:
    _build_p21_db(tmp_path)
    paper_db = _build_paper_db(tmp_path)

    result = sync_paper_sqlite_source(
        tmp_path,
        source_db_path=paper_db,
        run_id="run_step2923",
        source_mode="ui_sandbox_full_pipeline",
        sandbox_id="sb_step2923",
        cycle_id="cycle_step2923",
        resource_lane="ui_active_sandbox_real_pipeline",
    )

    assert result["samples_written"] == 1
    assert result["events_written"] == 2
    assert result["event_snapshots_written"] == 2

    manifest = json.loads((tmp_path / result["training_dataset_manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "step29_trade_snapshot_v2"
    assert manifest["feature_schema_version"] == "step29_decision_time_input_v2"
    assert manifest["known_at_policy_version"] == "step29_known_at_policy_v2"
    assert manifest["label_policy_version"] == "winner_loser_v1"
    assert manifest["quality_label_source_taxonomy"] == "winner_loser_v1"
    assert manifest["training_ready"] is False
    assert manifest["allowed_for_llm_training"] is False
    assert manifest["split_policy_owner"] == "ai_trader"
    assert manifest["unit_id_owner"] == "ai_trader"
    assert manifest["event_snapshots_written"] == 2
    assert manifest["stable_cost_fields_coverage"] == 1.0
    assert manifest["cost_fields_coverage"] == 1.0
    assert manifest["cost_missing_fields_json"] == []
    assert manifest["source_fact_ready"] is True
    assert manifest["ai_trader_registration_pending"] is True
    assert manifest["ai_trader_label_mapping_required"] is True
    assert manifest["record_schema_version_consistent"] is True

    events = [
        json.loads(line)
        for line in (tmp_path / result["trade_snapshots_jsonl_path"]).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [event["action"] for event in events] == ["entry", "exit"]
    for event in events:
        assert event["event_id"]
        assert event["sample_id"]
        assert event["source_refs"]
        assert event["known_at_policy_version"] == "step29_known_at_policy_v2"
        assert event["known_at_pass"] is True
        assert event["market_snapshot_json"]["status"] == "complete"
        assert event["field_lineage_json"]
        assert event["fee_bps"] == 5.0
        assert event["fee_usdt"] is not None
        assert event["slippage_bps"] == 4.0
        assert event["slippage_usdt"] is not None
        assert event["spread_bps"] == 4.0
        assert event["liquidity_bucket"]
        assert event["order_size_bucket"]
        assert event["market_regime_ref"]
        assert "spread_bps" not in event["missing_fields_json"]
        for field in ("spread_bps", "liquidity_bucket", "order_size_bucket", "market_regime_ref"):
            lineage = event["field_lineage_json"][field]
            assert lineage["feature_timestamp_ms"] <= event["decision_time_ms"]
            assert lineage["known_at_ms"] <= event["decision_time_ms"]
            assert lineage["source_available_time_ms"] <= event["decision_time_ms"]
            assert lineage["source_hash"]
    assert events[0]["decision_time_input_allowed"] is True
    assert events[1]["decision_time_input_allowed"] is False

    samples = [
        json.loads(line)
        for line in (tmp_path / result["dataset_path"]).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(samples) == 1
    sample = samples[0]
    assert sample["schema_version"] == "step29_trade_snapshot_v2"
    assert sample["decision_time_ms"] is not None
    assert sample["feature_timestamp_cutoff"] is not None
    assert sample["known_at_pass"] is True
    assert sample["market_feature_complete"] is True
    assert sample["label_coverage_status"] == "complete"
    assert sample["feature_schema_version"] == "step29_decision_time_input_v2"
    assert sample["known_at_policy_version"] == "step29_known_at_policy_v2"
    assert sample["label_policy_version"] == "winner_loser_v1"
    assert sample["source_refs"]
    assert sample["audit_trace_id"]
    entry_snapshot = sample["decision_time_input_json"]["entry_market_snapshot"]
    for field in ("spread_bps", "liquidity_bucket", "order_size_bucket", "market_regime_ref"):
        assert entry_snapshot[field] not in (None, "")
