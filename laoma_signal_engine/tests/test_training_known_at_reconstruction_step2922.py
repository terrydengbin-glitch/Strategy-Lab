from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from laoma_signal_engine.training_snapshot_sync import (
    complete_scoped_known_at_reconstruction,
    connect_sidecar,
    sync_paper_sqlite_source,
)


def _build_p21_db(root: Path, *, symbol: str = "TESTUSDT", last_open_ms: int = 5_460_000) -> None:
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
        for idx, open_ms in enumerate(range(0, last_open_ms + 1, 60_000)):
            price = 1.0 + idx * 0.001
            rows.append((symbol, open_ms, price, price + 0.02, price - 0.01, price + 0.01, 1000 + idx, 1000, 500))
        con.executemany("INSERT INTO p21_klines_1m VALUES (?,?,?,?,?,?,?,?,?)", rows)
        con.execute("INSERT INTO market_oi_15m VALUES (?,?,?,?,?)", (symbol, "15m", 4_500_000, 0.12, 1.5))
        con.execute("INSERT INTO market_funding_8h VALUES (?,?,?,?,?)", (symbol, 4_800_000, 0.0001, "positive", "long"))
        con.commit()


def _build_paper_db(root: Path, *, symbol: str = "TESTUSDT") -> Path:
    db = root / "paper_trading.db"
    with sqlite3.connect(db) as con:
        con.execute(
            """
            CREATE TABLE paper_orders (
                id TEXT PRIMARY KEY, intent_id TEXT, strategy_line TEXT, symbol TEXT, side TEXT,
                created_at INTEGER, entry_price REAL, stop_loss REAL, take_profit REAL,
                exit_price REAL, exit_reason TEXT, realized_pnl_usdt REAL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE paper_fills (
                id TEXT PRIMARY KEY, order_id TEXT, action TEXT, filled_at INTEGER,
                candle_open_time_ms INTEGER, gross_pnl_usdt REAL, net_pnl_usdt REAL
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
            "INSERT INTO paper_orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("ord1", "intent1", "without_micro", symbol, "LONG", 5_460_000, 1.0, 0.9, 1.1, 1.1, "take_profit", 10.0),
        )
        con.execute("INSERT INTO paper_fills VALUES (?,?,?,?,?,?,?)", ("fill_entry", "ord1", "entry", 5_460_000, 5_400_000, 0.0, 0.0))
        con.execute("INSERT INTO paper_fills VALUES (?,?,?,?,?,?,?)", ("fill_exit", "ord1", "take_profit", 5_520_000, 5_460_000, 10.0, 9.8))
        con.execute(
            "INSERT INTO trade_quality_samples VALUES (?,?,?,?,?,?,?,?,?,?)",
            ("tq1", "ord1", 1.0, 1.2, -0.2, 60.0, "take_profit", "good_entry", "test_v1", 1.1),
        )
        con.commit()
    return db


def test_step2922_sync_auto_runs_scoped_known_at_reconstruction(tmp_path: Path) -> None:
    _build_p21_db(tmp_path)
    paper_db = _build_paper_db(tmp_path)

    result = sync_paper_sqlite_source(
        tmp_path,
        source_db_path=paper_db,
        run_id="run_step2922_auto",
        source_mode="paper",
    )

    assert result["samples_written"] == 1
    recon = result["known_at_reconstruction"]
    assert recon["status"] == "completed"
    assert recon["events_processed"] == 2
    assert recon["market_feature_complete_rate"] == 1.0
    assert recon["known_at_pass_rate"] == 1.0
    with sqlite3.connect(tmp_path / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db") as con:
        coverage = json.loads(
            con.execute(
                "SELECT coverage_json FROM trade_snapshot_manifests WHERE run_id=?",
                ("run_step2922_auto",),
            ).fetchone()[0]
        )
    assert coverage["known_at_status"] == "complete"
    assert coverage["market_feature_complete_rate"] == 1.0


def test_step2922_stale_kline_source_is_not_marked_complete(tmp_path: Path) -> None:
    _build_p21_db(tmp_path, last_open_ms=5_400_000)
    with connect_sidecar(tmp_path) as con:
        sample_id = "run_step2922_stale:sample1"
        con.execute(
            """
            INSERT INTO trade_snapshot_events (
                event_id, sample_id, order_id, event_action, source_mode, source_db_path,
                source_table, source_row_id, strategy_line, symbol, side,
                event_time_ms, candle_open_time_ms, known_at_ms, decision_time_ms,
                created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "event_stale",
                sample_id,
                "ord_stale",
                "entry",
                "paper",
                "paper.db",
                "paper_fills",
                "fill_stale",
                "without_micro",
                "TESTUSDT",
                "LONG",
                5_700_000,
                5_640_000,
                None,
                5_700_000,
                "2026-06-17T00:00:00Z",
            ),
        )
        con.execute(
            """
            INSERT INTO trade_training_samples (
                sample_id, order_id, source_mode, source_db_path, strategy_line, symbol, side,
                entry_event_id, exit_event_id, entry_time_ms, exit_time_ms, schema_version, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                sample_id,
                "ord_stale",
                "paper",
                "paper.db",
                "without_micro",
                "TESTUSDT",
                "LONG",
                "event_stale",
                "event_stale",
                5_700_000,
                5_700_000,
                "step29_trade_snapshot_v1",
                "2026-06-17T00:00:00Z",
            ),
        )
        con.commit()

    result = complete_scoped_known_at_reconstruction(tmp_path, run_id="run_step2922_stale", source_mode="paper")

    assert result["events_processed"] == 1
    assert result["stale_source_event_count"] == 1
    assert result["known_at_pass_rate"] == 0.0
    with sqlite3.connect(tmp_path / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db") as con:
        raw = con.execute("SELECT market_snapshot_json FROM trade_snapshot_events WHERE event_id='event_stale'").fetchone()[0]
    snapshot = json.loads(raw)
    assert snapshot["status"] == "partial"
    assert "market_snapshot_source_stale" in snapshot["blocked_fields"]
