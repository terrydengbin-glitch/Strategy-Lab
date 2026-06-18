from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_v2 import (
    config_matrix_contract_payload,
    ensure_p21_v2_tables,
    experiment_daily_payload,
    experiment_detail_payload,
    experiment_orders_payload,
    experiment_symbols_payload,
    jobs_payload,
    kline_cache_status_payload,
    leaderboard_payload,
    run_config_matrix_payload,
    run_config_matrix_streaming_payload,
    export_config_candidate_payload,
)
from laoma_signal_engine.backtest.p21_trade_quality import (
    aggregates_payload as quality_aggregates_payload,
    materialize_payload as quality_materialize_payload,
    packages_payload as quality_packages_payload,
    samples_payload as quality_samples_payload,
    summary_payload as quality_summary_payload,
)
from laoma_signal_engine.backtest.p21_gate_scoring import (
    backfill_market_regime_features_payload as gate_backfill_market_regime_features_payload,
    batch_materialize_payload as gate_batch_materialize_payload,
    buckets_payload as gate_buckets_payload,
    candidates_payload as gate_candidates_payload,
    ensure_gate_scoring_tables,
    features_payload as gate_features_payload,
    generate_candidates_payload as gate_generate_candidates_payload,
    materialize_features_payload as gate_materialize_features_payload,
    rebuild_buckets_payload as gate_rebuild_buckets_payload,
    rebuild_scores_payload as gate_rebuild_scores_payload,
    recommendations_payload as gate_recommendations_payload,
    scores_payload as gate_scores_payload,
    strategy6_market_regime_gate_search_report_payload,
)
from laoma_signal_engine.backtest.p21_ops import (
    enqueue_tq_materialization_job,
    enhanced_validation_payload,
    export_candidate_audit_package,
    footprint_payload as ops_footprint_payload,
    process_next_tq_materialization_job,
    rebuild_serving_read_model_payload,
    retention_manifest_payload,
    serving_summary_payload,
    tq_materialization_jobs_payload,
    write_footprint_report,
)
from laoma_signal_engine.research_db import (
    upsert_backtest_order_native,
    upsert_market_funding_8h_rows,
    upsert_market_oi_15m_rows,
)


def _insert_fixture_klines(root: Path, symbol: str = "BTCUSDT", minutes: int = 240) -> None:
    db_path = p21_db_path(root)
    ensure_p21_v2_tables(db_path)
    start = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=minutes + 5)
    price = 100.0
    rows = []
    for idx in range(minutes):
        if idx % 35 in {10, 11, 12}:
            price *= 1.004
            volume = 600.0
        elif idx % 35 in {25, 26, 27}:
            price *= 0.996
            volume = 620.0
        else:
            price *= 1.0002 if idx % 2 == 0 else 0.9998
            volume = 100.0 + (idx % 7) * 5
        open_price = price * 0.9995
        close = price
        high = max(open_price, close) * 1.004
        low = min(open_price, close) * 0.996
        open_time_ms = int((start + timedelta(minutes=idx)).timestamp() * 1000)
        rows.append(
            (
                symbol,
                open_time_ms,
                datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                open_price,
                high,
                low,
                close,
                volume,
                volume * close,
                100 + idx,
                volume * 0.52,
                volume * close * 0.52,
                "fixture",
                "fixture_batch",
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        )
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO p21_klines_1m(
              symbol, open_time_ms, open_time, open, high, low, close, volume,
              quote_volume, trade_count, taker_buy_base_volume, taker_buy_quote_volume,
              source, download_batch_id, inserted_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def test_p21_v2_kline_cache_and_matrix(tmp_path: Path) -> None:
    _insert_fixture_klines(tmp_path, "BTCUSDT")
    _insert_fixture_klines(tmp_path, "ETHUSDT")

    status = kline_cache_status_payload(tmp_path, symbols=["BTCUSDT", "ETHUSDT"], days=1, max_symbols=2)
    assert status["count"] == 2
    assert status["symbols"][0]["row_count"] > 0

    contract = config_matrix_contract_payload(tmp_path, strategy_line="without_micro", max_sets=5)
    assert contract["parameter_set_count"] == 5
    assert contract["parameter_sets"][0]["parameters"]["min_score"] is not None
    assert contract["default_backtest_execution_contract"] == "paper_equivalent"
    assert contract["execution_contract"] == "legacy_backtest_only"
    assert contract["promotion_allowed"] is False

    result = run_config_matrix_payload(
        tmp_path,
        symbols=["BTCUSDT", "ETHUSDT"],
        strategy_line="without_micro",
        days=1,
        max_symbols=2,
        max_sets=5,
        parameter_grid=[
            {
                "parameter_set_id": "fixture_relaxed",
                "parameters": {
                    "strategy_line": "without_micro",
                    "min_score": 20,
                    "target_rr": 0.8,
                    "min_rr": 0.2,
                    "min_net_rr": 0.2,
                    "min_effective_rr": 0.2,
                    "stop_atr_mult": 1.0,
                    "max_stop_bps": 240,
                    "min_stop_bps": 3,
                    "min_reachable_reward_bps": 5,
                    "tp_target_policy": {"mode": "fast_capped_rr", "target_net_rr": 1.0, "target_rr_cap": 1.0},
                    "range_room": {"long_max_range_pos": 1.0, "short_min_range_pos": 0.0},
                    "taker_fee_bps": 5,
                    "slippage_bps": 1,
                    "max_hold_minutes": 60,
                },
            }
        ],
        write=True,
    )
    assert result["parameter_set_count"] == 1
    assert result["execution_contract"] == "legacy_backtest_only"
    assert result["promotion_allowed"] is False
    assert result["promotion_block_reason"] == "rerun_required_under_paper_equivalent"
    assert result["leaderboard"]
    assert result["leaderboard"][0]["execution_contract"] == "legacy_backtest_only"
    assert result["leaderboard"][0]["promotion_allowed"] is False
    assert result["leaderboard"][0]["metrics"]["trade_count"] > 0

    detail = experiment_detail_payload(tmp_path, result["experiment_id"])
    assert detail["found"] is True
    assert detail["execution_contract"] == "legacy_backtest_only"
    assert detail["promotion_allowed"] is False
    assert detail["leaderboard"]
    assert detail["leaderboard"][0]["legacy_mode"] is True

    db_path = p21_db_path(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        order = conn.execute(
            "SELECT * FROM p21_v2_shadow_orders WHERE experiment_id = ? LIMIT 1",
            (result["experiment_id"],),
        ).fetchone()
    assert order is not None
    row = dict(order)
    assert row["lineage_mode"] == "offline_real_evaluator"
    assert row["source_contract_version"] == "21.15"
    assert row["trade_plan_payload_json"]
    assert row["config_patch_json"]

    orders = experiment_orders_payload(tmp_path, result["experiment_id"], limit=5)
    assert orders["execution_contract"] == "legacy_backtest_only"
    assert orders["promotion_allowed"] is False
    assert orders["total"] > 0
    assert orders["orders"][0]["trade_plan_payload"]
    assert orders["orders"][0]["lineage_mode"] == "offline_real_evaluator"
    assert orders["orders"][0]["promotion_allowed"] is False

    visible_leaderboard = leaderboard_payload(tmp_path, exclude_legacy=True)
    assert visible_leaderboard["leaderboard"] == []
    research_leaderboard = leaderboard_payload(tmp_path, exclude_legacy=False)
    assert research_leaderboard["leaderboard"]
    assert research_leaderboard["leaderboard"][0]["promotion_block_reason"] == "rerun_required_under_paper_equivalent"

    exported = export_config_candidate_payload(tmp_path, experiment_id=result["experiment_id"], parameter_set_id="fixture_relaxed")
    assert exported["candidate"]["execution_contract"] == "legacy_backtest_only"
    assert exported["candidate"]["promotion_allowed"] is False

    daily = experiment_daily_payload(tmp_path, result["experiment_id"], limit=5)
    assert daily["total"] > 0
    assert daily["rows"][0]["metrics"]

    symbols = experiment_symbols_payload(tmp_path, result["experiment_id"], limit=5)
    assert symbols["total"] > 0
    assert symbols["rows"][0]["metrics"]

    jobs = jobs_payload(tmp_path)
    assert jobs["count"] == 0


def test_p21_v2_streaming_matrix_persists_shards(tmp_path: Path) -> None:
    _insert_fixture_klines(tmp_path, "BTCUSDT")
    _insert_fixture_klines(tmp_path, "ETHUSDT")

    parameter_grid = [
        {
            "parameter_set_id": "stream_relaxed_a",
            "parameters": {
                "strategy_line": "without_micro",
                "min_score": 20,
                "target_rr": 0.8,
                "min_rr": 0.2,
                "min_net_rr": 0.2,
                "min_effective_rr": 0.2,
                "stop_atr_mult": 1.0,
                "max_stop_bps": 240,
                "min_stop_bps": 3,
                "min_reachable_reward_bps": 5,
                "tp_target_policy": {"mode": "fast_capped_rr", "target_net_rr": 1.0, "target_rr_cap": 1.0},
                "range_room": {"long_max_range_pos": 1.0, "short_min_range_pos": 0.0},
                "taker_fee_bps": 5,
                "slippage_bps": 1,
                "max_hold_minutes": 60,
            },
        },
        {
            "parameter_set_id": "stream_relaxed_b",
            "parameters": {
                "strategy_line": "without_micro",
                "min_score": 30,
                "target_rr": 1.0,
                "min_rr": 0.2,
                "min_net_rr": 0.2,
                "min_effective_rr": 0.2,
                "stop_atr_mult": 1.2,
                "max_stop_bps": 260,
                "min_stop_bps": 3,
                "min_reachable_reward_bps": 5,
                "tp_target_policy": {"mode": "structure", "target_net_rr": None, "target_rr_cap": 1.0},
                "range_room": {"long_max_range_pos": 1.0, "short_min_range_pos": 0.0},
                "taker_fee_bps": 5,
                "slippage_bps": 1,
                "max_hold_minutes": 60,
            },
        },
    ]
    progress = []
    result = run_config_matrix_streaming_payload(
        tmp_path,
        symbols=["BTCUSDT", "ETHUSDT"],
        strategy_line="without_micro",
        days=1,
        max_symbols=2,
        max_sets=2,
        parameter_grid=parameter_grid,
        symbol_shard_size=1,
        job_id="fixture_job",
        progress_callback=progress.append,
    )
    assert result["execution_mode"] == "sharded_streaming"
    assert result["shard_count"] == 4
    assert result["parameter_set_count"] == 2
    assert result["leaderboard"]
    assert progress
    assert progress[-1]["done_count"] == progress[-1]["total_count"] == 4

    db_path = p21_db_path(tmp_path)
    with sqlite3.connect(db_path) as conn:
        shard_count = conn.execute(
            "SELECT COUNT(*) FROM p21_v2_matrix_shards WHERE experiment_id = ? AND status = 'completed'",
            (result["experiment_id"],),
        ).fetchone()[0]
        order_count = conn.execute(
            "SELECT COUNT(*) FROM p21_v2_shadow_orders WHERE experiment_id = ?",
            (result["experiment_id"],),
        ).fetchone()[0]
        metrics_count = conn.execute(
            "SELECT COUNT(*) FROM p21_v2_30d_metrics WHERE experiment_id = ?",
            (result["experiment_id"],),
        ).fetchone()[0]
    assert shard_count == 4
    assert order_count > 0
    assert metrics_count == 2

    resumed = run_config_matrix_streaming_payload(
        tmp_path,
        symbols=["BTCUSDT", "ETHUSDT"],
        strategy_line="without_micro",
        days=1,
        max_symbols=2,
        max_sets=2,
        parameter_grid=parameter_grid,
        symbol_shard_size=1,
        job_id="fixture_job_resumed",
        resume_experiment_id=result["experiment_id"],
        max_workers=2,
    )
    assert resumed["experiment_id"] == result["experiment_id"]
    assert resumed["max_workers"] == 2
    with sqlite3.connect(db_path) as conn:
        resumed_shards = conn.execute(
            "SELECT COUNT(*) FROM p21_v2_matrix_shards WHERE experiment_id = ? AND status = 'completed'",
            (result["experiment_id"],),
        ).fetchone()[0]
        resumed_orders = conn.execute(
            "SELECT COUNT(*) FROM p21_v2_shadow_orders WHERE experiment_id = ?",
            (result["experiment_id"],),
        ).fetchone()[0]
    assert resumed_shards == shard_count
    assert resumed_orders == order_count


def test_p21_v2_global_queue_matrix_persists_and_resumes(tmp_path: Path) -> None:
    _insert_fixture_klines(tmp_path, "BTCUSDT")
    _insert_fixture_klines(tmp_path, "ETHUSDT")

    parameter_grid = [
        {
            "parameter_set_id": "global_relaxed_a",
            "parameters": {
                "strategy_line": "without_micro",
                "min_score": 20,
                "target_rr": 0.8,
                "min_rr": 0.2,
                "min_net_rr": 0.2,
                "min_effective_rr": 0.2,
                "stop_atr_mult": 1.0,
                "max_stop_bps": 240,
                "min_stop_bps": 3,
                "min_reachable_reward_bps": 5,
                "tp_target_policy": {"mode": "fast_capped_rr", "target_net_rr": 1.0, "target_rr_cap": 1.0},
                "range_room": {"long_max_range_pos": 1.0, "short_min_range_pos": 0.0},
                "taker_fee_bps": 5,
                "slippage_bps": 1,
                "max_hold_minutes": 60,
            },
        },
        {
            "parameter_set_id": "global_relaxed_b",
            "parameters": {
                "strategy_line": "without_micro",
                "min_score": 30,
                "target_rr": 1.0,
                "min_rr": 0.2,
                "min_net_rr": 0.2,
                "min_effective_rr": 0.2,
                "stop_atr_mult": 1.2,
                "max_stop_bps": 260,
                "min_stop_bps": 3,
                "min_reachable_reward_bps": 5,
                "tp_target_policy": {"mode": "structure", "target_net_rr": None, "target_rr_cap": 1.0},
                "range_room": {"long_max_range_pos": 1.0, "short_min_range_pos": 0.0},
                "taker_fee_bps": 5,
                "slippage_bps": 1,
                "max_hold_minutes": 60,
            },
        },
    ]

    progress = []
    result = run_config_matrix_streaming_payload(
        tmp_path,
        symbols=["BTCUSDT", "ETHUSDT"],
        strategy_line="without_micro",
        days=1,
        max_symbols=2,
        max_sets=2,
        parameter_grid=parameter_grid,
        symbol_shard_size=1,
        max_workers=2,
        scheduler_mode="global_queue",
        job_id="fixture_global_job",
        progress_callback=progress.append,
    )
    assert result["execution_mode"] == "sharded_global_queue"
    assert result["memory_guard_status"] == "global_queue_single_writer"
    assert result["shard_count"] == 4
    assert result["parameter_set_count"] == 2
    assert progress
    assert progress[-1]["done_count"] == progress[-1]["total_count"] == 4
    assert "idle_workers" in progress[-1]
    assert "writer_queue_size" in progress[-1]
    assert "avg_shard_sec" in progress[-1]

    db_path = p21_db_path(tmp_path)
    with sqlite3.connect(db_path) as conn:
        shard_count = conn.execute(
            "SELECT COUNT(*) FROM p21_v2_matrix_shards WHERE experiment_id = ? AND status = 'completed'",
            (result["experiment_id"],),
        ).fetchone()[0]
        order_count = conn.execute(
            "SELECT COUNT(*) FROM p21_v2_shadow_orders WHERE experiment_id = ?",
            (result["experiment_id"],),
        ).fetchone()[0]
        metrics_count = conn.execute(
            "SELECT COUNT(*) FROM p21_v2_30d_metrics WHERE experiment_id = ?",
            (result["experiment_id"],),
        ).fetchone()[0]
    assert shard_count == 4
    assert order_count > 0
    assert metrics_count == 2

    resumed = run_config_matrix_streaming_payload(
        tmp_path,
        symbols=["BTCUSDT", "ETHUSDT"],
        strategy_line="without_micro",
        days=1,
        max_symbols=2,
        max_sets=2,
        parameter_grid=parameter_grid,
        symbol_shard_size=1,
        max_workers=2,
        scheduler_mode="global_queue",
        job_id="fixture_global_job_resumed",
        resume_experiment_id=result["experiment_id"],
    )
    assert resumed["experiment_id"] == result["experiment_id"]
    assert resumed["max_workers"] == 2
    with sqlite3.connect(db_path) as conn:
        resumed_orders = conn.execute(
            "SELECT COUNT(*) FROM p21_v2_shadow_orders WHERE experiment_id = ?",
            (result["experiment_id"],),
        ).fetchone()[0]
    assert resumed_orders == order_count


def test_p21_v2_backtest_trade_quality_materializer(tmp_path: Path) -> None:
    _insert_fixture_klines(tmp_path, "BTCUSDT")
    result = run_config_matrix_payload(
        tmp_path,
        symbols=["BTCUSDT"],
        strategy_line="without_micro",
        days=1,
        max_symbols=1,
        max_sets=1,
        parameter_grid=[
            {
                "parameter_set_id": "quality_relaxed",
                "parameters": {
                    "strategy_line": "without_micro",
                    "min_score": 20,
                    "target_rr": 0.8,
                    "min_rr": 0.2,
                    "min_net_rr": 0.2,
                    "min_effective_rr": 0.2,
                    "stop_atr_mult": 1.0,
                    "max_stop_bps": 240,
                    "min_stop_bps": 3,
                    "min_reachable_reward_bps": 5,
                    "tp_target_policy": {"mode": "fast_capped_rr", "target_net_rr": 1.0, "target_rr_cap": 1.0},
                    "range_room": {"long_max_range_pos": 1.0, "short_min_range_pos": 0.0},
                    "taker_fee_bps": 5,
                    "slippage_bps": 1,
                    "max_hold_minutes": 60,
                },
            }
        ],
        write=True,
    )
    db_path = p21_db_path(tmp_path)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO p21_v2_recommendations(
              recommendation_id, experiment_id, parameter_set_id, status, priority, summary,
              metrics_json, parameters_json, risks_json, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "quality_metrics_only_rec",
                result["experiment_id"],
                "quality_metrics_only",
                "candidate",
                0,
                "metrics-only fixture",
                json.dumps({"strategy_line": "without_micro", "profit_factor": 999.0, "expectancy_R": 9.0, "trade_count": 99, "total_R": 999.0}),
                json.dumps({"strategy_line": "without_micro"}),
                "{}",
                generated_at,
            ),
        )

    dry = quality_materialize_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        parameter_set_id="quality_relaxed",
        dry_run=True,
    )
    assert dry["source"] == "backtest_p21_v2"
    assert dry["selected_order_count"] > 0
    assert dry["materialized_count"] == 0

    candidates = quality_packages_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        mode="leaderboard_candidates",
        strategy_line="all",
    )
    assert candidates["mode"] == "leaderboard_candidates"
    assert candidates["rank_scope"] == "global_top30"
    assert len(candidates["packages"]) <= 30
    assert candidates["packages"][0]["parameter_set_id"] == "quality_relaxed"
    assert candidates["packages"][0]["materialized"] is False
    assert candidates["packages"][0]["has_shadow_orders"] is True
    assert candidates["packages"][0]["sample_status"] == "ready_to_materialize"
    assert any(pkg["parameter_set_id"] == "quality_metrics_only" and pkg["sample_status"] == "metrics_only_no_trade_samples" for pkg in candidates["packages"])
    assert len({(pkg["experiment_id"], pkg["strategy_line"], pkg["parameter_set_id"]) for pkg in candidates["packages"]}) == len(candidates["packages"])

    strategy_candidates = quality_packages_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        mode="leaderboard_candidates",
        strategy_line="without_micro",
    )
    assert strategy_candidates["rank_scope"] == "strategy_top10"
    assert len(strategy_candidates["packages"]) <= 10

    run = quality_materialize_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        parameter_set_id="quality_relaxed",
        dry_run=False,
        limit=50,
    )
    assert run["materialized_count"] > 0
    assert run["rollup_count"] > 0

    again = quality_materialize_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        parameter_set_id="quality_relaxed",
        dry_run=False,
        limit=50,
    )
    assert again["materialized_count"] == run["materialized_count"]

    summary = quality_summary_payload(tmp_path, experiment_id=result["experiment_id"], parameter_set_id="quality_relaxed")
    assert summary["source"] == "backtest_p21_v2"
    assert summary["total"] == run["materialized_count"]
    assert summary["summary"]["performance_stats"]["trade_count"] == run["materialized_count"]
    assert summary["summary"]["replay_coverage"]["replayed_1m"] == run["materialized_count"]

    aggregates = quality_aggregates_payload(tmp_path, experiment_id=result["experiment_id"], parameter_set_id="quality_relaxed")
    assert aggregates["aggregates"]
    assert any(row["dimension"] == "root_cause" for row in aggregates["aggregates"])

    samples = quality_samples_payload(tmp_path, experiment_id=result["experiment_id"], parameter_set_id="quality_relaxed", limit=5)
    assert samples["samples"]
    assert samples["samples"][0]["package_key"].startswith(f"backtest:{result['experiment_id']}:without_micro:quality_relaxed")

    packages = quality_packages_payload(tmp_path, experiment_id=result["experiment_id"])
    assert packages["packages"]
    after_candidates = quality_packages_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        mode="leaderboard_candidates",
        strategy_line="without_micro",
    )
    assert after_candidates["packages"][0]["materialized"] is True
    assert after_candidates["packages"][0]["materialized_sample_count"] == run["materialized_count"]


def test_p21_v2_gate_scoring_shadow_candidate_chain(tmp_path: Path) -> None:
    _insert_fixture_klines(tmp_path, "BTCUSDT")
    result = run_config_matrix_payload(
        tmp_path,
        symbols=["BTCUSDT"],
        strategy_line="without_micro",
        days=1,
        max_symbols=1,
        max_sets=1,
        parameter_grid=[
            {
                "parameter_set_id": "gate_relaxed",
                "parameters": {
                    "strategy_line": "without_micro",
                    "min_score": 20,
                    "target_rr": 0.8,
                    "min_rr": 0.2,
                    "min_net_rr": 0.2,
                    "min_effective_rr": 0.2,
                    "stop_atr_mult": 1.0,
                    "max_stop_bps": 240,
                    "min_stop_bps": 3,
                    "min_reachable_reward_bps": 5,
                    "tp_target_policy": {"mode": "fast_capped_rr", "target_net_rr": 1.0, "target_rr_cap": 1.0},
                    "range_room": {"long_max_range_pos": 1.0, "short_min_range_pos": 0.0},
                    "taker_fee_bps": 5,
                    "slippage_bps": 1,
                    "max_hold_minutes": 60,
                },
            }
        ],
        write=True,
    )

    batch = gate_batch_materialize_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        strategy_line="without_micro",
        top_n=1,
        limit=80,
        dry_run=False,
    )
    assert batch["dry_run"] is False
    assert batch["materialized_samples"] > 0
    assert batch["materialized_packages"] == 1

    feature_build = gate_materialize_features_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        strategy_line="without_micro",
        parameter_set_id="gate_relaxed",
        limit=1000,
        dry_run=False,
    )
    assert feature_build["feature_count"] == batch["materialized_samples"]
    assert feature_build["feature_count"] > 0

    features = gate_features_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        strategy_line="without_micro",
        parameter_set_id="gate_relaxed",
        limit=5,
    )
    assert features["features"]
    assert features["features"][0]["target_net_R"] is not None
    assert "signal_score" in features["features"][0]["features"]
    assert features["features"][0]["features"]["cost_bucket"]

    bucket_build = gate_rebuild_buckets_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        strategy_line="without_micro",
        parameter_set_id="gate_relaxed",
        dry_run=False,
    )
    assert bucket_build["bucket_count"] > 0
    buckets = gate_buckets_payload(tmp_path, experiment_id=result["experiment_id"], limit=20)
    assert buckets["buckets"]
    assert any(row["dimension"] == "side" for row in buckets["buckets"])

    score_build = gate_rebuild_scores_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        strategy_line="without_micro",
        parameter_set_id="gate_relaxed",
        dry_run=False,
    )
    assert score_build["score_count"] > 0
    scores = gate_scores_payload(tmp_path, experiment_id=result["experiment_id"], limit=10)
    assert scores["scores"]
    assert scores["scores"][0]["status"] == "shadow"

    candidate_build = gate_generate_candidates_payload(
        tmp_path,
        experiment_id=result["experiment_id"],
        strategy_line="without_micro",
        parameter_set_id="gate_relaxed",
        min_test_pf=0.0,
        min_coverage=0.0,
        dry_run=False,
    )
    assert candidate_build["candidate_count"] >= 0
    candidates = gate_candidates_payload(tmp_path, experiment_id=result["experiment_id"], limit=10)
    recommendations = gate_recommendations_payload(tmp_path, experiment_id=result["experiment_id"], limit=10)
    if candidate_build["candidate_count"]:
        assert candidates["candidates"]
        assert recommendations["recommendations"]
        assert candidates["candidates"][0]["status"] == "shadow"
        assert "config_patch_preview" in candidates["candidates"][0]


def test_p21_v2_trade_gate_bucket_candidates_are_entry_known(tmp_path: Path) -> None:
    db_path = p21_db_path(tmp_path)
    ensure_gate_scoring_tables(db_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    splits = ["train"] * 24 + ["validation"] * 8 + ["test"] * 8
    for idx, split in enumerate(splits):
        bad = idx % 2 == 0
        symbol = "BADUSDT" if bad else "GOODUSDT"
        net_r = -1.0 if bad else 1.0
        features = {
            "strategy_line": "strategy5",
            "symbol": symbol,
            "symbol_group": "alt",
            "side": "long",
            "hour_utc": 9,
            "weekday": 1,
            "session": "europe",
            "signal_score": 65,
            "score_bucket": "q3_mid",
            "planned_rr": 1.0,
            "planned_rr_bucket": "rr_mid",
            "taker_fee_bps": 5,
            "slippage_bps": 1,
            "cost_bps": 6,
            "cost_bucket": "mid_cost",
            "entry_mode": "market",
            "volatility_regime": "normal",
            "btc_trend": "chop",
            "btc_alignment": "same",
            "market_breadth": "mixed",
            "funding_regime": "neutral",
            "oi_direction": "flat",
        }
        targets = {
            "net_R": net_r,
            "MFE_R": 0.2 if bad else 1.2,
            "MAE_R": 1.0 if bad else 0.2,
            "exit_reason": "SL" if bad else "TP",
            "holding_minutes": 12,
        }
        diagnostics = {"root_cause": "direction_wrong" if bad else "profitable_trade", "replay_status": "ok"}
        rows.append(
            (
                f"sample_{idx}",
                f"diag_{idx}",
                "pkg_strategy5_smoke",
                "exp_gate_smoke",
                "p_strategy5_gate",
                "strategy5",
                f"order_{idx}",
                symbol,
                "long",
                now,
                idx,
                split,
                "complete",
                json.dumps(features),
                json.dumps(targets),
                json.dumps(diagnostics),
                "test",
                now,
            )
        )
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO backtest_gate_feature_samples(
              sample_id, diagnostic_id, package_key, experiment_id, parameter_set_id, strategy_line,
              order_id, symbol, side, entry_time, entry_time_ms, train_split,
              feature_completeness, features_json, targets_json, diagnostics_json, schema_version, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    result = gate_generate_candidates_payload(
        tmp_path,
        experiment_id="exp_gate_smoke",
        strategy_line="strategy5",
        parameter_set_id="p_strategy5_gate",
        min_test_pf=0.0,
        min_coverage=0.0,
        dry_run=False,
    )
    assert result["candidate_count"] > 0
    candidates = gate_candidates_payload(tmp_path, experiment_id="exp_gate_smoke", strategy_line="strategy5", limit=20)
    bucket_candidates = [row for row in candidates["candidates"] if (row.get("evidence") or {}).get("candidate_kind") == "bucket_exclusion"]
    assert bucket_candidates
    assert bucket_candidates[0]["rule"]["known_at_entry"] is True
    assert bucket_candidates[0]["rule"]["dimension"] in {"symbol", "symbol_side"}
    assert bucket_candidates[0]["test_metrics"]["after"]["profit_factor"] >= bucket_candidates[0]["test_metrics"]["before"]["profit_factor"]


def test_p21_v2_strategy6_market_regime_backfill_merges_entry_known_sidecar(tmp_path: Path) -> None:
    _insert_fixture_klines(tmp_path, "BTCUSDT", minutes=90)
    db_path = p21_db_path(tmp_path)
    ensure_gate_scoring_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        entry = conn.execute(
            "SELECT open_time_ms, open_time FROM p21_klines_1m WHERE symbol = 'BTCUSDT' ORDER BY open_time_ms DESC LIMIT 1"
        ).fetchone()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        features = {
            "strategy_line": "strategy6",
            "symbol": "ALTUSDT",
            "symbol_group": "alt",
            "side": "LONG",
            "hour_utc": 9,
            "weekday": 1,
            "session": "europe",
            "score_bucket": "q4_high",
            "planned_rr_bucket": "rr_mid",
            "cost_bucket": "mid_cost",
            "btc_trend": "unknown",
            "btc_alignment": "unknown",
            "market_breadth": "unknown",
            "funding_regime": "unknown",
            "oi_direction": "unknown",
        }
        targets = {"net_R": -1.0, "MFE_R": 0.1, "MAE_R": 1.0}
        conn.execute(
            """
            INSERT OR REPLACE INTO backtest_gate_feature_samples(
              sample_id, diagnostic_id, package_key, experiment_id, parameter_set_id, strategy_line,
              order_id, symbol, side, entry_time, entry_time_ms, train_split,
              feature_completeness, features_json, targets_json, diagnostics_json, schema_version, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "sample_s6_regime",
                "diag_s6_regime",
                "pkg_s6_regime",
                "exp_s6_regime",
                "param_s6_regime",
                "strategy6",
                "order_s6_regime",
                "ALTUSDT",
                "LONG",
                entry["open_time"],
                entry["open_time_ms"],
                "train",
                "partial",
                json.dumps(features),
                json.dumps(targets),
                json.dumps({"root_cause": "direction_wrong"}),
                "test",
                now,
            ),
        )

    backfill = gate_backfill_market_regime_features_payload(
        tmp_path,
        experiment_id="exp_s6_regime",
        strategy_line="strategy6",
        dry_run=False,
    )
    assert backfill["materialized_regime_features"] == 1
    assert backfill["no_future_violations"] == 0
    payload = gate_features_payload(tmp_path, experiment_id="exp_s6_regime", strategy_line="strategy6", limit=5)
    sample = payload["features"][0]
    assert sample["features"]["btc_trend"] in {"bullish", "bearish", "chop"}
    assert sample["features"]["btc_alignment"] in {"same", "opposite", "chop"}
    assert sample["features"]["funding_regime"] == "unknown"
    assert sample["features"]["regime_source_status"]["funding_regime"] == "missing_source"


def test_p24_native_writer_backfills_market_context_with_lineage(tmp_path: Path) -> None:
    _insert_fixture_klines(tmp_path, "BTCUSDT", minutes=120)
    for idx in range(12):
        _insert_fixture_klines(tmp_path, f"ALT{idx}USDT", minutes=120)
    db_path = p21_db_path(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        entry = conn.execute(
            "SELECT open_time_ms, open FROM p21_klines_1m WHERE symbol = 'ALT1USDT' ORDER BY open_time_ms DESC LIMIT 1"
        ).fetchone()
        upsert_backtest_order_native(
            conn,
            experiment_id="exp_p24_market_context",
            parameter_set_id="param_p24_market_context",
            strategy_line="strategy6",
            parameters={"strategy_line": "strategy6", "slippage_bps": 2.0, "taker_fee_bps": 5.0},
            order={
                "order_id": "order_p24_market_context",
                "symbol": "ALT1USDT",
                "strategy_line": "strategy6",
                "side": "LONG",
                "signal_time_ms": int(entry["open_time_ms"]) - 60000,
                "entry_time_ms": int(entry["open_time_ms"]),
                "exit_time_ms": int(entry["open_time_ms"]) + 60000,
                "entry_price": float(entry["open"]),
                "stop_loss": float(entry["open"]) * 0.99,
                "take_profit": float(entry["open"]) * 1.01,
                "planned_rr": 1.0,
                "net_R": 0.5,
                "exit_reason": "TP",
                "features": {},
                "fill_result": {"exit_price": float(entry["open"]) * 1.01},
            },
        )
        row = conn.execute(
            """
            SELECT *
            FROM research_entry_features
            WHERE sample_id IN (
              SELECT sample_id FROM research_trade_facts WHERE order_id = 'order_p24_market_context'
            )
            """
        ).fetchone()
    assert row is not None
    features = json.loads(row["features_json"])
    missing = set(json.loads(row["missing_fields_json"]))
    source_ref = json.loads(row["source_ref_json"])
    source_status = features["market_context_source_status"]
    assert features["btc_trend"] in {"bullish", "bearish", "chop"}
    assert features["btc_alignment"] in {"same", "opposite", "chop"}
    assert features["market_breadth"] in {"up", "down", "mixed"}
    assert features["spread_bps"] == 2.0
    assert source_status["spread_bps"]["quality"] == "proxy"
    assert source_status["funding_rate"]["quality"] == "missing"
    assert source_status["oi_change"]["quality"] == "missing"
    assert source_ref["market_context_source_status"]["btc_trend"]["quality"] == "observed"
    assert "btc_trend" not in missing
    assert "market_breadth" not in missing
    assert "spread_bps" not in missing
    assert "funding_rate" in missing


def test_p24_native_writer_uses_observed_oi_funding_asof_without_future(tmp_path: Path) -> None:
    _insert_fixture_klines(tmp_path, "BTCUSDT", minutes=120)
    _insert_fixture_klines(tmp_path, "ALT1USDT", minutes=120)
    db_path = p21_db_path(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        entry = conn.execute(
            "SELECT open_time_ms, open FROM p21_klines_1m WHERE symbol = 'ALT1USDT' ORDER BY open_time_ms DESC LIMIT 1"
        ).fetchone()
        entry_ms = int(entry["open_time_ms"])
        upsert_market_oi_15m_rows(
            conn,
            "ALT1USDT",
            [
                {"source_time_ms": entry_ms - 45 * 60000, "sum_open_interest": 990, "sum_open_interest_value": 99000},
                {"source_time_ms": entry_ms - 30 * 60000, "sum_open_interest": 1000, "sum_open_interest_value": 100000},
                {"source_time_ms": entry_ms - 15 * 60000, "sum_open_interest": 1020, "sum_open_interest_value": 102000},
                {"source_time_ms": entry_ms + 15 * 60000, "sum_open_interest": 5000, "sum_open_interest_value": 500000},
            ],
        )
        upsert_market_funding_8h_rows(
            conn,
            "ALT1USDT",
            [
                {"funding_time_ms": entry_ms - 8 * 60 * 60000, "funding_rate": 0.0006},
                {"funding_time_ms": entry_ms + 8 * 60 * 60000, "funding_rate": -0.001},
            ],
        )
        upsert_backtest_order_native(
            conn,
            experiment_id="exp_p24_oi_funding",
            parameter_set_id="param_p24_oi_funding",
            strategy_line="strategy6",
            parameters={"strategy_line": "strategy6", "slippage_bps": 2.0, "taker_fee_bps": 5.0},
            order={
                "order_id": "order_p24_oi_funding",
                "symbol": "ALT1USDT",
                "strategy_line": "strategy6",
                "side": "LONG",
                "signal_time_ms": entry_ms - 60000,
                "entry_time_ms": entry_ms,
                "exit_time_ms": entry_ms + 60000,
                "entry_price": float(entry["open"]),
                "stop_loss": float(entry["open"]) * 0.99,
                "take_profit": float(entry["open"]) * 1.01,
                "planned_rr": 1.0,
                "net_R": 0.5,
                "exit_reason": "TP",
                "features": {},
                "fill_result": {"exit_price": float(entry["open"]) * 1.01},
            },
        )
        row = conn.execute(
            """
            SELECT *
            FROM research_entry_features
            WHERE sample_id IN (
              SELECT sample_id FROM research_trade_facts WHERE order_id = 'order_p24_oi_funding'
            )
            """
        ).fetchone()
    assert row is not None
    features = json.loads(row["features_json"])
    missing = set(json.loads(row["missing_fields_json"]))
    status = features["market_context_source_status"]
    assert round(float(features["oi_change"]), 6) == 0.02
    assert features["oi_state"] in {"price_up_oi_up_new_positions", "price_down_oi_up_new_shorts", "unknown"}
    assert float(features["funding_rate"]) == 0.0006
    assert features["funding_bucket"] == "OVERHEATED"
    assert features["funding_crowded_side"] == "long"
    assert status["oi_change"]["quality"] == "observed"
    assert status["funding_rate"]["quality"] == "observed"
    assert status["oi_change"]["source_ts"] == entry_ms - 15 * 60000
    assert status["funding_rate"]["source_ts"] == entry_ms - 8 * 60 * 60000
    assert "oi_change" not in missing
    assert "oi_z" not in missing
    assert "funding_rate" not in missing


def test_p21_v2_strategy6_market_regime_gate_report_smoke(tmp_path: Path) -> None:
    _insert_fixture_klines(tmp_path, "BTCUSDT", minutes=120)
    db_path = p21_db_path(tmp_path)
    ensure_gate_scoring_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        entries = conn.execute(
            "SELECT open_time_ms, open_time FROM p21_klines_1m WHERE symbol = 'BTCUSDT' ORDER BY open_time_ms ASC LIMIT 40 OFFSET 60"
        ).fetchall()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows = []
        splits = ["train"] * 24 + ["validation"] * 8 + ["test"] * 8
        for idx, (entry, split) in enumerate(zip(entries, splits, strict=False)):
            side = "LONG" if idx % 2 == 0 else "SHORT"
            features = {
                "strategy_line": "strategy6",
                "symbol": "ALTUSDT",
                "symbol_group": "alt",
                "side": side,
                "session": "europe",
                "score_bucket": "q4_high",
                "planned_rr_bucket": "rr_mid",
                "cost_bucket": "mid_cost",
                "entry_mode": "market",
            }
            net_r = -1.0 if side == "LONG" else 0.8
            rows.append(
                (
                    f"sample_s6_regime_{idx}",
                    f"diag_s6_regime_{idx}",
                    "pkg_s6_regime",
                    "exp_s6_regime",
                    "param_s6_regime",
                    "strategy6",
                    f"order_s6_regime_{idx}",
                    "ALTUSDT",
                    side,
                    entry["open_time"],
                    entry["open_time_ms"],
                    split,
                    "partial",
                    json.dumps(features),
                    json.dumps({"net_R": net_r, "MFE_R": 0.1 if net_r < 0 else 1.0, "MAE_R": 1.0 if net_r < 0 else 0.2}),
                    json.dumps({"root_cause": "direction_wrong" if net_r < 0 else "profitable_trade"}),
                    "test",
                    now,
                )
            )
        conn.executemany(
            """
            INSERT OR REPLACE INTO backtest_gate_feature_samples(
              sample_id, diagnostic_id, package_key, experiment_id, parameter_set_id, strategy_line,
              order_id, symbol, side, entry_time, entry_time_ms, train_split,
              feature_completeness, features_json, targets_json, diagnostics_json, schema_version, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    result = strategy6_market_regime_gate_search_report_payload(
        tmp_path,
        experiment_id="exp_s6_regime",
        min_train_samples=5,
        max_depth=2,
        dry_run=False,
    )
    assert result["sample_count"] == 40
    assert result["backfill"]["no_future_violations"] == 0
    assert Path(result["report_path"]).exists()


def test_p21_v2_ops_footprint_retention_serving_queue_and_audit_package(tmp_path: Path) -> None:
    _insert_fixture_klines(tmp_path, "BTCUSDT")
    result = run_config_matrix_payload(
        tmp_path,
        symbols=["BTCUSDT"],
        strategy_line="without_micro",
        days=1,
        max_symbols=1,
        max_sets=1,
        parameter_grid=[
            {
                "parameter_set_id": "ops_relaxed",
                "parameters": {
                    "strategy_line": "without_micro",
                    "min_score": 20,
                    "target_rr": 0.8,
                    "min_rr": 0.2,
                    "min_net_rr": 0.2,
                    "min_effective_rr": 0.2,
                    "stop_atr_mult": 1.0,
                    "max_stop_bps": 240,
                    "min_stop_bps": 3,
                    "min_reachable_reward_bps": 5,
                    "tp_target_policy": {"mode": "fast_capped_rr", "target_net_rr": 1.0, "target_rr_cap": 1.0},
                    "range_room": {"long_max_range_pos": 1.0, "short_min_range_pos": 0.0},
                    "taker_fee_bps": 5,
                    "slippage_bps": 1,
                    "max_hold_minutes": 60,
                },
            }
        ],
        write=True,
    )
    experiment_id = result["experiment_id"]
    parameter_set_id = "ops_relaxed"

    footprint = ops_footprint_payload(tmp_path, row_count_budget=100_000)
    assert footprint["status"] == "ok"
    assert footprint["db_size_bytes"] > 0
    assert any(row["table"] == "p21_v2_shadow_orders" for row in footprint["tables"])

    footprint_report = write_footprint_report(tmp_path)
    assert Path(footprint_report["report_path"]).exists()

    manifest = retention_manifest_payload(tmp_path, min_trade_count=1, write=True)
    assert manifest["status"] == "dry_run"
    assert manifest["retained_parameter_sets"]
    assert Path(manifest["manifest_path"]).exists()
    assert manifest["validation"]["metrics_count_before"] >= manifest["validation"]["metrics_count_after"]

    enqueue = enqueue_tq_materialization_job(
        tmp_path,
        {
            "experiment_id": experiment_id,
            "strategy_line": "without_micro",
            "parameter_set_id": parameter_set_id,
            "limit": 20,
            "dry_run": True,
        },
    )
    assert enqueue["status"] == "queued"
    processed = process_next_tq_materialization_job(tmp_path)
    assert processed["status"] in {"succeeded", "done"}
    assert (
        processed["result"].get("dry_run") is True
        or ((processed["result"].get("stages") or {}).get("tq_samples") or {}).get("dry_run") is True
    )
    jobs = tq_materialization_jobs_payload(tmp_path)
    assert jobs["jobs"][0]["status"] in {"succeeded", "done"}

    quality_materialize_payload(
        tmp_path,
        experiment_id=experiment_id,
        strategy_line="without_micro",
        parameter_set_id=parameter_set_id,
        limit=50,
        dry_run=False,
    )
    gate_materialize_features_payload(
        tmp_path,
        experiment_id=experiment_id,
        strategy_line="without_micro",
        parameter_set_id=parameter_set_id,
        dry_run=False,
    )
    gate_rebuild_scores_payload(
        tmp_path,
        experiment_id=experiment_id,
        strategy_line="without_micro",
        parameter_set_id=parameter_set_id,
        dry_run=False,
    )
    enhanced = enhanced_validation_payload(
        tmp_path,
        experiment_id=experiment_id,
        strategy_line="without_micro",
        parameter_set_id=parameter_set_id,
        min_test_trade_count=1,
        min_coverage=0,
    )
    assert enhanced["validations"]
    assert enhanced["validations"][0]["split_policy"]["test_set_touched_during_selection"] is False
    assert "cost_stress" in enhanced["validations"][0]

    db_path = p21_db_path(tmp_path)
    candidate_id = "ops_candidate_fixture"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO backtest_gate_candidates(
              candidate_id, experiment_id, parameter_set_id, strategy_line, gate_type, status,
              rule_json, config_patch_preview_json, train_metrics_json, validation_metrics_json,
              test_metrics_json, pf_before, pf_after_test, trade_coverage_test, overfit_risk,
              evidence_json, schema_version, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                candidate_id,
                experiment_id,
                parameter_set_id,
                "without_micro",
                "score_gate",
                "shadow",
                json.dumps({"score_name": "composite_score", "cutoff": "Q4"}),
                json.dumps({"profiles": {"review_only": {"trade_quality_gate": {"enabled": False}}}}),
                json.dumps({"profit_factor": 1.1}),
                json.dumps({"profit_factor": 1.05}),
                json.dumps({"profit_factor": 1.02}),
                0.9,
                1.02,
                0.25,
                "medium",
                json.dumps({"fixture": True}),
                "test",
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            ),
        )

    serving = rebuild_serving_read_model_payload(tmp_path)
    assert serving["status"] == "ok"
    summary = serving_summary_payload(tmp_path)
    assert summary["candidates"]

    export = export_candidate_audit_package(tmp_path, candidate_id=candidate_id)
    assert export["status"] == "exported"
    out_dir = Path(export["output_dir"])
    assert (out_dir / "lineage.json").exists()
    assert (out_dir / "config_patch.yaml").exists()
