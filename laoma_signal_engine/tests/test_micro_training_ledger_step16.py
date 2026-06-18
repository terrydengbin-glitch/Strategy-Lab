from __future__ import annotations

import sqlite3
from pathlib import Path

from laoma_signal_engine.micro.training_ledger import (
    backfill_from_audit,
    classify_run_sample_gaps,
    coverage_payload,
    enrich_downstream_labels,
    enrich_from_audit_factor_frames,
    enrich_spread_depth_missing_reasons,
    ingest_runtime_v2_payload,
    init_micro_training_db,
    latest_training_payload,
)


def test_micro_training_runtime_payload_sidecar(tmp_path: Path) -> None:
    payload = {
        "run_id": "20260606T010101Z",
        "cycle_id": "cycle_20260606T010101Z",
        "generated_at": "2026-06-06T01:01:30Z",
        "source": "micro_evidence_runtime_v2",
        "summary": {"symbols": 1},
        "symbols": [
            {
                "strategy_line": "micro_fast",
                "symbol": "BTCUSDT",
                "status": "ready",
                "state": "ready",
                "raw_reasons": ["accepted"],
                "factor_frame": {"cvd": 1.2, "ofi": -0.4, "spread": 0.01, "depth_imbalance": 0.3},
                "stream_heartbeat": {
                    "aggTrade": {"active": True, "last_age_ms": 10, "last_event_ts_sec": 1780000000, "route": "market"},
                    "bookTicker": {"active": True, "last_age_ms": 20, "last_event_ts_sec": 1780000000, "route": "public"},
                    "partialDepth5": {"active": True, "last_age_ms": 25, "last_event_ts_sec": 1780000000, "route": "public"},
                },
                "runtime_evidence": {
                    "book_depth_runtime": {"depth_age_ms": 30},
                    "bucket_alignment": {
                        "cvd_bucket_ts_sec": 1780000000,
                        "ofi_bucket_ts_sec": 1780000000,
                        "common_bucket_ts_sec": 1780000000,
                        "alignment_status": "aligned",
                    },
                    "data_plane": {"ready": True, "target_set_hydrated": True, "ready_age_sec": 12},
                },
            }
        ],
    }

    result = ingest_runtime_v2_payload(tmp_path, payload=payload)
    assert result["status"] == "ok"
    latest = latest_training_payload(tmp_path)
    assert latest["run_id"] == "20260606T010101Z"
    assert latest["symbol_count"] == 1
    row = latest["symbols"][0]
    assert row["strategy_line"] == "micro_fast"
    assert row["micro_mode"] == "fast"
    assert row["accepted"] == 1
    assert row["cvd"] == 1.2
    assert row["micro_data_plane_ready"] == 1
    assert row["alignment_state"] == "aligned"
    assert row["common_bucket_ts"] == 1780000000
    assert row["is_training_usable"] == 1
    assert latest["metric_coverage"]["data_plane_ready_count"] == 1


def test_step16_22_records_technical_reliability_contract(tmp_path: Path) -> None:
    payload = {
        "run_id": "20260606T070707Z",
        "cycle_id": "cycle_20260606T070707Z",
        "generated_at": "2026-06-06T07:07:30Z",
        "symbols": [
            {
                "strategy_line": "micro_fast",
                "symbol": "XRPUSDT",
                "status": "technical_blocked",
                "state": "technical_blocked",
                "raw_reasons": ["ofi_cvd_lag_high", "ofi_stale"],
                "factor_frame": {"cvd": 1.0},
                "stream_heartbeat": {
                    "aggTrade": {"active": True, "last_age_ms": 10, "last_event_ts_sec": 1780000300, "route": "market"},
                    "bookTicker": {"active": True, "last_age_ms": 120000, "route": "public"},
                },
                "runtime_evidence": {
                    "bucket_alignment": {
                        "cvd_bucket_ts_sec": 1780000300,
                        "ofi_bucket_ts_sec": 1780000200,
                        "ofi_cvd_lag_bucket_sec": 100,
                        "alignment_status": "lagged",
                        "true_alignment_reason": "ofi_new_cvd_old",
                    },
                    "data_plane": {"ready": False, "readiness_block_reason": "ofi_stale"},
                    "z_history_runtime": {"valid_bucket_ratio": 0.25, "missing_reason": "valid_bucket_ratio_low"},
                },
            }
        ],
    }

    ingest_runtime_v2_payload(tmp_path, payload=payload)
    latest = latest_training_payload(tmp_path)
    row = latest["symbols"][0]
    assert row["technical_status"] == "technical_blocked"
    assert row["technical_severity"] == "P0"
    assert row["micro_data_plane_ready"] == 0
    assert row["alignment_state"] == "lagged"
    assert row["bucket_lag_sec"] == 100
    assert row["z_state"] == "z_valid_ratio_low"
    assert row["is_training_usable"] == 0
    assert row["not_training_usable_reason"] == "ofi_stale"
    assert latest["metric_coverage"]["training_not_usable_count"] == 1


def test_micro_training_backfill_from_audit_sqlite(tmp_path: Path) -> None:
    audit_db = tmp_path / "DATA" / "audit" / "run_audit.db"
    audit_db.parent.mkdir(parents=True)
    with sqlite3.connect(audit_db) as conn:
        conn.execute(
            """
            create table audit_runs(
              run_id text primary key,
              generated_at text
            )
            """
        )
        conn.execute(
            """
            create table micro_evidence_runtime_v2_runs(
              run_id text primary key,
              cycle_id text,
              generated_at text,
              schema_version text,
              summary_json text,
              source text
            )
            """
        )
        conn.execute(
            """
            create table micro_evidence_runtime_v2_symbols(
              run_id text,
              cycle_id text,
              strategy_line text,
              symbol text,
              state text,
              status text,
              severity text,
              raw_reasons_json text,
              attributed_reasons_json text,
              categories_json text,
              factor_frame_json text,
              stream_heartbeat_json text,
              z_window_json text,
              runtime_evidence_json text,
              recommended_actions_json text,
              missing_evidence_fields_json text,
              generated_at text
            )
            """
        )
        conn.execute("insert into audit_runs values(?,?)", ("20260606T020202Z", "2026-06-06T02:02:30Z"))
        conn.execute(
            "insert into micro_evidence_runtime_v2_runs values(?,?,?,?,?,?)",
            ("20260606T020202Z", "cycle_20260606T020202Z", "2026-06-06T02:02:30Z", "v2", "{}", "test"),
        )
        conn.execute(
            """
            insert into micro_evidence_runtime_v2_symbols values(
              ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                "20260606T020202Z",
                "cycle_20260606T020202Z",
                "micro_full",
                "ETHUSDT",
                "not_ready",
                "not_ready",
                "P1",
                '["ofi_missing"]',
                "[]",
                "[]",
                '{"z_cvd": 0.7, "z_ofi": -0.2}',
                "{}",
                "{}",
                "{}",
                "[]",
                "[]",
                "2026-06-06T02:02:30Z",
            ),
        )

    dry = backfill_from_audit(tmp_path, dry_run=True)
    assert dry["run_rows"] == 1
    assert dry["symbol_rows"] == 1
    applied = backfill_from_audit(tmp_path)
    assert applied["label_rows"] == 1
    coverage = coverage_payload(tmp_path)
    assert coverage["run_count"] == 1
    assert coverage["symbol_sample_count"] == 1
    assert coverage["run_coverage_ratio"] == 1


def test_micro_training_schema_init(tmp_path: Path) -> None:
    db = init_micro_training_db(root=tmp_path)
    assert db.is_file()
    with sqlite3.connect(db) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "select name from sqlite_master where type='table' and name like 'micro_%'"
            ).fetchall()
        }
    assert "micro_run_samples" in tables
    assert "micro_symbol_samples" in tables
    assert "micro_downstream_labels" in tables


def test_step16_16_enriches_factor_frames_by_event_time(tmp_path: Path) -> None:
    ingest_runtime_v2_payload(
        tmp_path,
        payload={
            "run_id": "20260606T030303Z",
            "cycle_id": "cycle_20260606T030303Z",
            "generated_at": "2026-06-06T03:03:30Z",
            "symbols": [
                {
                    "strategy_line": "micro_fast",
                    "symbol": "BNBUSDT",
                    "status": "not_ready",
                    "state": "not_ready",
                    "raw_reasons": ["cvd_missing"],
                }
            ],
        },
    )
    audit_db = tmp_path / "DATA" / "audit" / "run_audit.db"
    audit_db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(audit_db) as conn:
        conn.execute(
            """
            create table micro_factor_frames(
              strategy_line text,
              symbol text,
              bucket_ts_sec integer,
              generated_at text,
              cvd real,
              ofi real,
              z_cvd real,
              z_ofi real,
              cvd_available integer,
              ofi_available integer,
              z_cvd_available integer,
              z_ofi_available integer,
              payload_json text
            )
            """
        )
        conn.execute(
            "insert into micro_factor_frames values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "micro_fast",
                "BNBUSDT",
                180178220,
                "2026-06-06T03:03:00Z",
                12.5,
                -2.25,
                1.1,
                -0.7,
                1,
                1,
                1,
                1,
                "{}",
            ),
        )

    result = enrich_from_audit_factor_frames(tmp_path, limit_runs=1, max_lag_sec=120)
    assert result["updated_samples"] == 1
    latest = latest_training_payload(tmp_path)
    row = latest["symbols"][0]
    assert row["cvd"] == 12.5
    assert row["ofi"] == -2.25
    assert row["source_confidence"] == "direct_run_id+event_time_window"
    assert "spread" in row["missing_reason"]
    assert "depth_imbalance" in row["missing_reason"]


def test_step16_16_classifies_micro_full_gap_and_joins_downstream(tmp_path: Path) -> None:
    ingest_runtime_v2_payload(
        tmp_path,
        payload={
            "run_id": "20260606T040404Z",
            "cycle_id": "cycle_20260606T040404Z",
            "generated_at": "2026-06-06T04:04:30Z",
            "symbols": [
                {
                    "strategy_line": "micro_fast",
                    "symbol": "ETHUSDT",
                    "status": "ready",
                    "state": "ready",
                    "factor_frame": {"cvd": 2.0, "ofi": 1.0},
                }
            ],
        },
    )
    gap = classify_run_sample_gaps(tmp_path, limit_runs=1)
    assert gap["not_selected_lines"] == 1

    paper_db = tmp_path / "DATA" / "paper" / "paper_trading.db"
    paper_db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(paper_db) as conn:
        conn.execute(
            """
            create table paper_orders(
              id text,
              source_run_id text,
              strategy_line text,
              symbol text,
              status text,
              exit_reason text,
              source_executable integer
            )
            """
        )
        conn.execute(
            """
            create table trade_quality_diagnostic_samples(
              diagnostic_id text,
              run_id text,
              strategy_line text,
              symbol text,
              order_id text,
              exit_time text,
              exit_reason text,
              net_R real,
              MFE_R real,
              MAE_R real,
              root_cause text
            )
            """
        )
        conn.execute(
            "insert into paper_orders values(?,?,?,?,?,?,?)",
            ("ord-1", "20260606T040404Z", "micro_fast", "ETHUSDT", "closed", "TP", 1),
        )
        conn.execute(
            "insert into trade_quality_diagnostic_samples values(?,?,?,?,?,?,?,?,?,?,?)",
            (
                "diag-1",
                "20260606T040404Z",
                "micro_fast",
                "ETHUSDT",
                "ord-1",
                "2026-06-06T04:10:00Z",
                "TP",
                0.8,
                1.1,
                0.2,
                "profitable_trade",
            ),
        )

    labels = enrich_downstream_labels(tmp_path, limit_runs=1)
    assert labels["updated_labels"] == 1
    latest = latest_training_payload(tmp_path)
    fast_row = [row for row in latest["symbols"] if row["strategy_line"] == "micro_fast"][0]
    assert fast_row["paper_status"] == "closed"
    assert fast_row["trade_quality_root_cause"] == "profitable_trade"
    full_row = [row for row in latest["run_samples"] if row["strategy_line"] == "micro_full"][0]
    assert full_row["status"] == "not_selected"
    assert full_row["missing_reason"] == "line_not_selected_or_no_runtime_symbols"


def test_step16_17_captures_runtime_spread_depth_payload(tmp_path: Path) -> None:
    ingest_runtime_v2_payload(
        tmp_path,
        payload={
            "run_id": "20260606T050505Z",
            "cycle_id": "cycle_20260606T050505Z",
            "generated_at": "2026-06-06T05:05:30Z",
            "symbols": [
                {
                    "strategy_line": "micro_fast",
                    "symbol": "SOLUSDT",
                    "status": "ready",
                    "state": "ready",
                    "factor_frame": {
                        "best_bid": 99.0,
                        "best_ask": 101.0,
                        "bid_depth_usdt": 12000.0,
                        "ask_depth_usdt": 8000.0,
                    },
                    "stream_heartbeat": {
                        "bookTicker": {"last_age_ms": 25},
                        "partialDepth5": {"last_age_ms": 40},
                    },
                }
            ],
        },
    )
    latest = latest_training_payload(tmp_path)
    row = latest["symbols"][0]
    assert round(float(row["spread_bps"]), 6) == 200.0
    assert round(float(row["spread"]), 6) == 200.0
    assert round(float(row["depth_imbalance"]), 6) == 0.2
    assert row["bid_depth_usdt"] == 12000.0
    assert row["ask_depth_usdt"] == 8000.0
    assert row["depth_source"] == "bookticker_depth_payload"
    assert row["book_cost_confidence"] == "direct_run_id"
    assert row["depth_missing_reason"] is None


def test_step16_17_records_fine_grained_depth_missing_reason(tmp_path: Path) -> None:
    ingest_runtime_v2_payload(
        tmp_path,
        payload={
            "run_id": "20260606T060606Z",
            "cycle_id": "cycle_20260606T060606Z",
            "generated_at": "2026-06-06T06:06:30Z",
            "symbols": [
                {
                    "strategy_line": "micro_fast",
                    "symbol": "ADAUSDT",
                    "status": "not_ready",
                    "state": "not_ready",
                    "stream_heartbeat": {
                        "bookTicker": {"active": False},
                        "partialDepth5": {"active": False, "missing_reason": "subscription_disabled"},
                    },
                }
            ],
        },
    )
    result = enrich_spread_depth_missing_reasons(tmp_path, limit_runs=1)
    assert result["updated_samples"] >= 0
    latest = latest_training_payload(tmp_path)
    row = latest["symbols"][0]
    assert row["depth_source"] == "missing"
    assert "bookticker_inactive" in row["depth_missing_reason"]
    assert "depth5_subscription_disabled" in row["depth_missing_reason"]
