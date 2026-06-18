from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone

import yaml
from fastapi.testclient import TestClient

import laoma_signal_engine.api.services as api_services
from laoma_signal_engine.api.app import app
from laoma_signal_engine.core.config_loader import package_root
from laoma_signal_engine.paper.candles import StaticCandleProvider
from laoma_signal_engine.paper.engine import PaperEngine
from laoma_signal_engine.paper.models import Candle, PaperConfig
from laoma_signal_engine.trade_quality import analyze_paper_trades
from laoma_signal_engine.trade_quality.diagnostics import (
    diagnostic_backfill_payload,
    diagnostic_archive_packages_payload,
    diagnostic_entry_context_v3_payload,
    diagnostic_entry_feature_payload,
    diagnostic_entry_market_context_payload,
    diagnostic_entry_microstructure_payload,
    diagnostic_replay_payload,
    diagnostic_samples_payload,
    diagnostic_summary_payload,
    diagnostic_sync_status_payload,
)


class _RangeCandleProvider:
    def __init__(self, candles: list[Candle]) -> None:
        self._candles = candles

    def get_range_1m(self, symbol: str, opened_at: str | None, closed_at: str | None) -> list[Candle]:
        return self._candles


def _config() -> PaperConfig:
    return PaperConfig(
        db_path="DATA/paper/test_trade_quality_p19.db",
        summary_path="DATA/paper/latest_paper_state.json",
        default_slippage_bps=0,
        taker_fee_bps=0,
    )


def _doc(*, symbol: str = "P19USDT", side: str = "LONG", line: str = "without_micro") -> dict:
    return {
        "schema_version": "1.0",
        "generated_at": "2026-06-04T00:00:00Z",
        "run_id": "run_p19",
        "cycle_id": "cycle_p19",
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
                "decision": side,
                "action": "ENTER_MARKET",
                "entry_mode": "MARKET",
                "estimated_entry_price": 100,
                "stop_loss": 95 if side == "LONG" else 105,
                "take_profit": 110 if side == "LONG" else 90,
                "risk_per_unit": 5,
                "reward_per_unit": 10,
                "rr": 2.0,
                "executable": True,
                "confidence": 80,
                "reason_codes": [],
                "guards": {"line": line, "margin_usdt": 100, "leverage": 20},
                "input_refs": {"source_plan_hash": f"run_p19_{line}_{symbol}_{side}"},
            }
        ],
    }


def _ms(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


def _prepare_p19_db(tmp_path: Path, monkeypatch=None) -> Path:
    if monkeypatch is not None:
        monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
        src = package_root() / "config" / "default.yaml"
        dst = tmp_path / "default.yaml"
        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        monkeypatch.setattr(api_services, "CONFIG_PATH", dst)
        raw = yaml.safe_load(dst.read_text(encoding="utf-8"))
        raw.setdefault("paper", {})
        raw["paper"]["db_path"] = _config().db_path
        raw["paper"]["summary_path"] = _config().summary_path
        dst.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8")

    doc = _doc()
    engine = PaperEngine(
        tmp_path,
        config=_config(),
        candle_provider=StaticCandleProvider({"P19USDT": [Candle("P19USDT", 1, 100, 101, 99, 100)]}),
    )
    engine.tick({"without_micro": doc})
    engine.candle_provider = StaticCandleProvider({"P19USDT": [Candle("P19USDT", 2, 100, 112, 97, 110)]})
    engine.tick({})
    analyze_paper_trades(
        tmp_path,
        config=_config(),
        candle_provider=StaticCandleProvider(
            {"P19USDT": [Candle("P19USDT", 1, 100, 101, 99, 100), Candle("P19USDT", 2, 100, 112, 97, 110)]}
        ),
        persist=True,
    )
    return tmp_path / _config().db_path


def test_step191_192_diagnostic_contract_migrates_legacy_sample(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)

    result = diagnostic_backfill_payload(tmp_path, write=True, config=_config())
    samples = diagnostic_samples_payload(db_path, limit=10)

    assert result["candidate_samples"] == 1
    assert samples["total"] == 1
    row = samples["samples"][0]
    assert row["trade_id"]
    assert row["symbol"] == "P19USDT"
    assert row["net_R"] is not None
    assert row["planned_SL"] == 95
    assert row["planned_TP"] == 110
    assert row["planned_RR"] == 2.0
    assert row["replay_status"] == "candle_1m_replay"
    assert "net_R_positive" in row["quality_tags"]


def test_step194_root_cause_uses_r_path_not_usdt(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trade_quality_samples
            SET net_pnl_usdt=-10, net_R=-1.0, MFE_R=0.1, MAE_R=1.1,
                exit_reason='SL', root_cause_label='legacy_direction_wrong'
            """
        )

    diagnostic_backfill_payload(tmp_path, write=True, config=_config())
    summary = diagnostic_summary_payload(db_path)
    sample = diagnostic_samples_payload(db_path, limit=1)["samples"][0]

    assert sample["root_cause"] == "direction_wrong"
    assert "immediate_adverse" in sample["quality_tags"]
    assert summary["summary"]["phenomena"][0]["code"] == "mfe_lt_0.3"


def test_step1911_summary_returns_r_first_performance_stats(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)
    diagnostic_backfill_payload(tmp_path, write=True, config=_config())
    sequence = [
        ("perf_1", 1.0, 10.0, 1.0, 10, "2026-06-04T00:01:00Z"),
        ("perf_2", -0.5, -5.0, 0.5, 20, "2026-06-04T00:02:00Z"),
        ("perf_3", -1.0, -10.0, 0.5, 30, "2026-06-04T00:03:00Z"),
        ("perf_4", 0.5, 5.0, 1.0, 40, "2026-06-04T00:04:00Z"),
        ("perf_5", -0.25, -2.5, 0.25, 50, "2026-06-04T00:05:00Z"),
        ("perf_6", -0.25, -2.5, 0.25, 60, "2026-06-04T00:06:00Z"),
        ("perf_7", -0.25, -2.5, 0.25, 70, "2026-06-04T00:07:00Z"),
    ]
    with sqlite3.connect(db_path) as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(trade_quality_diagnostic_samples)").fetchall()]
        base = conn.execute("SELECT * FROM trade_quality_diagnostic_samples LIMIT 1").fetchone()
        assert base is not None
        base_map = dict(zip(columns, base))
        conn.execute("DELETE FROM trade_quality_diagnostic_samples")
        placeholders = ",".join("?" for _ in columns)
        for trade_id, net_r, net_pnl, fee, holding, exit_time in sequence:
            row = dict(base_map)
            row.update(
                {
                    "diagnostic_id": f"diag_{trade_id}",
                    "trade_id": trade_id,
                    "net_R": net_r,
                    "net_pnl": net_pnl,
                    "fee": fee,
                    "holding_minutes": holding,
                    "exit_time": exit_time,
                }
            )
            conn.execute(
                f"INSERT INTO trade_quality_diagnostic_samples({','.join(columns)}) VALUES({placeholders})",
                [row[column] for column in columns],
            )

    summary = diagnostic_summary_payload(db_path)
    stats = summary["summary"]["performance_stats"]

    assert stats["trade_count"] == 7
    assert stats["win_count"] == 2
    assert stats["loss_count"] == 5
    assert stats["win_rate"] == round(2 / 7, 6)
    assert stats["avg_win_R"] == 0.75
    assert stats["avg_loss_R"] == 0.45
    assert stats["profit_loss_ratio"] == round(0.75 / 0.45, 8)
    assert stats["expectancy_R"] == round((2 / 7) * 0.75 - (5 / 7) * 0.45, 8)
    assert stats["max_drawdown_R"] == 1.75
    assert stats["max_losing_streak"] == 3
    assert stats["losing_streak_distribution"] == {"2": 1, "3": 1}
    assert stats["fee_total"] == 3.75
    assert stats["gross_profit_usdt"] == 15.0
    assert stats["fee_to_gross_profit_ratio"] == 0.25
    assert stats["avg_holding_minutes"] == 40.0
    assert stats["median_holding_minutes"] == 40.0
    assert summary["summary"]["expectancy_R"] == stats["expectancy_R"]


def test_step1912_summary_returns_root_cause_attribution(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)
    diagnostic_backfill_payload(tmp_path, write=True, config=_config())
    sequence = [
        ("attr_1", "direction_wrong", -1.0, 0.1, 1.0, 0.86),
        ("attr_2", "direction_wrong", -0.8, 0.2, 0.9, 0.84),
        ("attr_3", "entered_too_early", -0.4, 1.1, 0.8, 0.78),
        ("attr_4", "tp_too_far", -0.2, 0.5, 0.3, 0.74),
        ("attr_5", "profitable_trade", 0.7, 1.2, 0.2, 0.7),
    ]
    with sqlite3.connect(db_path) as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(trade_quality_diagnostic_samples)").fetchall()]
        base = conn.execute("SELECT * FROM trade_quality_diagnostic_samples LIMIT 1").fetchone()
        assert base is not None
        base_map = dict(zip(columns, base))
        conn.execute("DELETE FROM trade_quality_diagnostic_samples")
        placeholders = ",".join("?" for _ in columns)
        for idx, (trade_id, root, net_r, mfe_r, mae_r, confidence) in enumerate(sequence, start=1):
            row = dict(base_map)
            row.update(
                {
                    "diagnostic_id": f"diag_{trade_id}",
                    "trade_id": trade_id,
                    "root_cause": root,
                    "root_cause_confidence": confidence,
                    "net_R": net_r,
                    "MFE_R": mfe_r,
                    "MAE_R": mae_r,
                    "planned_RR": 1.5,
                    "exit_time": f"2026-06-04T00:0{idx}:00Z",
                    "quality_tags_json": f'["{root}"]',
                }
            )
            conn.execute(
                f"INSERT INTO trade_quality_diagnostic_samples({','.join(columns)}) VALUES({placeholders})",
                [row[column] for column in columns],
            )

    attribution = diagnostic_summary_payload(db_path)["summary"]["root_cause_attribution"]
    first = attribution["items"][0]
    by_root = {row["root_cause"]: row for row in attribution["items"]}

    assert attribution["sample_count"] == 5
    assert attribution["loss_sample_count"] == 4
    assert attribution["top_loss_root_cause"] == "direction_wrong"
    assert first["root_cause"] == "direction_wrong"
    assert first["count"] == 2
    assert first["loss_count"] == 2
    assert first["ratio"] == 0.4
    assert first["loss_ratio"] == 0.5
    assert first["avg_net_R"] == -0.9
    assert first["avg_MFE_R"] == 0.15
    assert first["avg_MAE_R"] == 0.95
    assert first["confidence_avg"] == 0.85
    assert "direction confirmation" in first["optimization"]
    assert by_root["profitable_trade"]["loss_count"] == 0


def test_step1913_summary_returns_dimension_attribution(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)
    diagnostic_backfill_payload(tmp_path, write=True, config=_config())
    sequence = [
        ("dim_1", "BTCUSDT", "LONG", "direction_wrong", -1.0, -10.0, 1.0, 0.1, 1.0, 2, "2026-06-04T00:01:00Z"),
        ("dim_2", "BTCUSDT", "LONG", "direction_wrong", 0.5, 5.0, 0.5, 1.0, 0.2, 7, "2026-06-04T00:07:00Z"),
        ("dim_3", "ETHUSDT", "SHORT", "tp_too_far", -0.4, -4.0, 0.25, 0.5, 0.4, 20, "2026-06-04T01:20:00Z"),
        ("dim_4", "ETHUSDT", "SHORT", "entered_too_early", -0.2, -2.0, 0.25, 1.1, 0.8, 75, "2026-06-04T02:15:00Z"),
    ]
    with sqlite3.connect(db_path) as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(trade_quality_diagnostic_samples)").fetchall()]
        base = conn.execute("SELECT * FROM trade_quality_diagnostic_samples LIMIT 1").fetchone()
        assert base is not None
        base_map = dict(zip(columns, base))
        conn.execute("DELETE FROM trade_quality_diagnostic_samples")
        placeholders = ",".join("?" for _ in columns)
        for trade_id, symbol, side, root, net_r, net_pnl, fee, mfe_r, mae_r, holding, exit_time in sequence:
            row = dict(base_map)
            row.update(
                {
                    "diagnostic_id": f"diag_{trade_id}",
                    "trade_id": trade_id,
                    "symbol": symbol,
                    "side": side,
                    "root_cause": root,
                    "net_R": net_r,
                    "net_pnl": net_pnl,
                    "fee": fee,
                    "MFE_R": mfe_r,
                    "MAE_R": mae_r,
                    "holding_minutes": holding,
                    "entry_time": "2026-06-04T00:00:00Z",
                    "exit_time": exit_time,
                    "quality_tags_json": f'["{root}"]',
                }
            )
            conn.execute(
                f"INSERT INTO trade_quality_diagnostic_samples({','.join(columns)}) VALUES({placeholders})",
                [row[column] for column in columns],
            )

    attribution = diagnostic_summary_payload(db_path)["summary"]["dimension_attribution"]
    by_symbol = {row["key"]: row for row in attribution["symbol"]}
    by_hour = {row["key"]: row for row in attribution["hour_bucket"]}
    by_holding = {row["key"]: row for row in attribution["holding_bucket"]}
    by_side = {row["key"]: row for row in attribution["side"]}

    assert by_symbol["BTCUSDT"]["trade_count"] == 2
    assert by_symbol["BTCUSDT"]["loss_count"] == 1
    assert by_symbol["BTCUSDT"]["win_rate"] == 0.5
    assert by_symbol["BTCUSDT"]["avg_R"] == -0.25
    assert by_symbol["BTCUSDT"]["total_R"] == -0.5
    assert by_symbol["BTCUSDT"]["fee_ratio"] == round(1.5 / 5.0, 8)
    assert by_symbol["BTCUSDT"]["top_root_cause"] == "direction_wrong"
    assert by_symbol["BTCUSDT"]["root_cause_counts"] == {"direction_wrong": 2}
    assert by_hour["UTC 00"]["trade_count"] == 2
    assert by_hour["UTC 01"]["trade_count"] == 1
    assert by_holding["0-3m"]["trade_count"] == 1
    assert by_holding["3-10m"]["trade_count"] == 1
    assert by_holding["10-30m"]["trade_count"] == 1
    assert by_holding["60m+"]["trade_count"] == 1
    assert by_side["LONG"]["trade_count"] == 2
    assert by_side["SHORT"]["trade_count"] == 2
    assert attribution["market_context"]["status"] == "pending_market_context_enrichment"


def test_step1914_1916_entry_feature_backfill_and_attribution(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)
    diagnostic_backfill_payload(tmp_path, write=True, source="current_paper", config=_config())
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trade_quality_diagnostic_samples
            SET entry_time='2026-06-04T00:05:00Z',
                exit_time='2026-06-04T00:10:00Z',
                entry_price=104,
                MFE_R=0.2,
                MAE_R=1.0,
                net_R=-1.0,
                side='LONG'
            """
        )
    symbol_candles = [
        Candle("P19USDT", _ms("2026-06-04T00:00:00Z"), 100, 101, 99, 100, 10),
        Candle("P19USDT", _ms("2026-06-04T00:01:00Z"), 100, 101, 99, 101, 10),
        Candle("P19USDT", _ms("2026-06-04T00:02:00Z"), 101, 102, 100, 102, 10),
        Candle("P19USDT", _ms("2026-06-04T00:03:00Z"), 102, 103, 101, 103, 10),
        Candle("P19USDT", _ms("2026-06-04T00:04:00Z"), 103, 104, 102, 104, 10),
        Candle("P19USDT", _ms("2026-06-04T00:05:00Z"), 104, 105, 103, 104, 80),
    ]
    btc_candles = [
        Candle("BTCUSDT", _ms("2026-06-04T00:00:00Z"), 100, 101, 99, 100, 10),
        Candle("BTCUSDT", _ms("2026-06-04T00:03:00Z"), 100, 101, 99, 101, 10),
        Candle("BTCUSDT", _ms("2026-06-04T00:05:00Z"), 101, 102, 100, 102, 10),
    ]
    provider = StaticCandleProvider({"P19USDT": symbol_candles, "BTCUSDT": btc_candles})

    dry = diagnostic_entry_feature_payload(tmp_path, write=False, source="current_paper", config=_config(), candle_provider=provider)
    assert dry["candidate_count"] == 1
    assert dry["updated_count"] == 0
    assert dry["samples"][0]["entry_quality_label"] == "impulse_exhausted"

    run = diagnostic_entry_feature_payload(tmp_path, write=True, source="current_paper", config=_config(), candle_provider=provider)
    summary = diagnostic_summary_payload(db_path, source="current_paper")["summary"]
    sample = diagnostic_samples_payload(db_path, source="current_paper", limit=1)["samples"][0]
    entry = summary["entry_quality_attribution"]

    assert run["updated_count"] == 1
    assert sample["entry_quality_label"] == "impulse_exhausted"
    assert sample["entry_features"]["entry_feature_coverage"] == "complete"
    assert sample["entry_features"]["pre_5m_return"] > 0.03
    assert entry["feature_coverage"] == 1.0
    assert entry["top_bad_entry_pattern"] == "impulse_exhausted"
    assert entry["items"][0]["label"] == "impulse_exhausted"
    assert entry["items"][0]["loss_count"] == 1


def test_step1917_1920_entry_microstructure_backfill_strategy_boundary(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)
    diagnostic_backfill_payload(tmp_path, write=True, source="current_paper", config=_config())
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trade_quality_diagnostic_samples
            SET entry_time='2026-06-04T00:05:00Z',
                exit_time='2026-06-04T00:10:00Z',
                entry_price=104,
                MFE_R=0.2,
                MAE_R=1.0,
                net_R=-1.0,
                side='LONG',
                strategy_line='micro_fast',
                run_id='run_v2',
                cycle_id='cycle_v2'
            """
        )
    diagnostic_entry_feature_payload(tmp_path, write=True, source="current_paper", config=_config(), candle_provider=StaticCandleProvider({
        "P19USDT": [
            Candle("P19USDT", _ms("2026-06-04T00:00:00Z"), 100, 101, 99, 100, 10),
            Candle("P19USDT", _ms("2026-06-04T00:04:00Z"), 103, 104, 102, 104, 80),
            Candle("P19USDT", _ms("2026-06-04T00:05:00Z"), 104, 105, 103, 104, 80),
        ],
        "BTCUSDT": [
            Candle("BTCUSDT", _ms("2026-06-04T00:00:00Z"), 100, 101, 99, 100, 10),
            Candle("BTCUSDT", _ms("2026-06-04T00:05:00Z"), 100, 101, 99, 101, 10),
        ],
    }))
    audit_db = tmp_path / "DATA" / "audit" / "run_audit.db"
    audit_db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(audit_db) as conn:
        conn.execute(
            """
            CREATE TABLE micro_factor_frames (
              strategy_line TEXT, symbol TEXT, bucket_ts_sec INTEGER, generated_at TEXT,
              cvd REAL, ofi REAL, z_cvd REAL, z_ofi REAL,
              cvd_available INTEGER, ofi_available INTEGER,
              z_cvd_available INTEGER, z_ofi_available INTEGER,
              payload_json TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO micro_factor_frames VALUES(
              'micro_fast', 'P19USDT', ?, '2026-06-04T00:05:01Z',
              -10, -5, -1.2, -0.8, 1, 1, 1, 1,
              '{"spread_bps": 4, "depth_imbalance": -0.2}'
            )
            """,
            (int(_ms("2026-06-04T00:05:00Z") / 1000),),
        )

    run = diagnostic_entry_microstructure_payload(tmp_path, write=True, source="current_paper", config=_config())
    sample = diagnostic_samples_payload(db_path, source="current_paper", limit=1)["samples"][0]
    summary = diagnostic_summary_payload(db_path, source="current_paper")["summary"]

    assert run["updated_count"] == 1
    assert sample["entry_microstructure"]["evidence_status"] == "complete"
    assert sample["entry_quality_v2_label"] in {"price_move_not_confirmed_by_cvd", "breakout_not_confirmed_by_ofi", "depth_imbalance_against_entry"}
    assert summary["entry_microstructure_attribution"]["sample_count"] == 1
    assert summary["entry_microstructure_attribution"]["by_strategy_line"][0]["strategy_line"] == "micro_fast"


def test_step1921_1923_entry_market_context_and_v3_are_shadow_only(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)
    diagnostic_backfill_payload(tmp_path, write=True, source="current_paper", config=_config())
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trade_quality_diagnostic_samples
            SET entry_time='2026-06-04T00:05:00Z',
                exit_time='2026-06-04T00:10:00Z',
                entry_price=104,
                MFE_R=0.2,
                MAE_R=1.0,
                net_R=-1.0,
                side='LONG',
                strategy_line='without_micro'
            """
        )
    diagnostic_entry_feature_payload(
        tmp_path,
        write=True,
        source="current_paper",
        config=_config(),
        candle_provider=StaticCandleProvider(
            {
                "P19USDT": [
                    Candle("P19USDT", _ms("2026-06-04T00:00:00Z"), 100, 101, 99, 100, 10),
                    Candle("P19USDT", _ms("2026-06-04T00:04:00Z"), 103, 104, 102, 104, 80),
                    Candle("P19USDT", _ms("2026-06-04T00:05:00Z"), 104, 105, 103, 104, 80),
                ],
                "BTCUSDT": [
                    Candle("BTCUSDT", _ms("2026-06-04T00:00:00Z"), 100, 101, 99, 100, 10),
                    Candle("BTCUSDT", _ms("2026-06-04T00:05:00Z"), 100, 101, 99, 99, 10),
                ],
            }
        ),
    )
    factor_dir = tmp_path / "DATA" / "factors"
    factor_dir.mkdir(parents=True, exist_ok=True)
    (factor_dir / "latest_factor_snapshot.json").write_text(
        json.dumps(
            {
                "items": [
                    {
                        "symbol": "P19USDT",
                        "generated_at": "2026-06-04T00:05:01Z",
                        "oi_15m": {"oi_pct_change": -0.03, "oi_z": -1.1},
                        "funding_context": {"funding_rate_raw": 0.0007, "funding_bucket": "OVERHEATED"},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    market = diagnostic_entry_market_context_payload(tmp_path, write=True, source="current_paper", config=_config())
    v3 = diagnostic_entry_context_v3_payload(tmp_path, write=True, source="current_paper", config=_config())
    summary = diagnostic_summary_payload(db_path, source="current_paper")["summary"]
    sample = diagnostic_samples_payload(db_path, source="current_paper", limit=1)["samples"][0]

    assert market["updated_count"] == 1
    assert v3["updated_count"] == 1
    assert sample["entry_market_context"]["market_context_status"] == "complete"
    assert sample["entry_market_context"]["oi_direction"] == "down"
    assert sample["entry_context_v3"]["entry_context_v3_label"] in {
        "oi_not_supporting_move",
        "btc_opposite_pressure",
        "funding_crowded_against_entry",
    }
    assert summary["entry_market_context_attribution"]["sample_count"] == 1
    assert summary["entry_context_v3_attribution"]["by_strategy_line"][0]["strategy_line"] == "without_micro"


def test_step1253_api_exposes_p19_diagnostics(tmp_path: Path, monkeypatch) -> None:
    _prepare_p19_db(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "laoma_signal_engine.api.app.trade_quality_entry_features_backfill_service",
        lambda **kwargs: {
            "schema_version": "19.14",
            "mode": "dry_run" if kwargs.get("dry_run") else "run",
            "candidate_count": 1,
            "updated_count": 0 if kwargs.get("dry_run") else 1,
            "missing_candle_count": 0,
            "feature_version": "19.14",
            "reason_counts": {"complete": 1},
        },
    )
    monkeypatch.setattr(
        "laoma_signal_engine.api.app.trade_quality_entry_microstructure_backfill_service",
        lambda **kwargs: {
            "schema_version": "19.17",
            "mode": "dry_run" if kwargs.get("dry_run") else "run",
            "candidate_count": 1,
            "updated_count": 0 if kwargs.get("dry_run") else 1,
            "evidence_status_counts": {"micro_evidence_not_required": 1},
            "label_counts": {"not_applicable_without_micro": 1},
        },
    )
    monkeypatch.setattr(
        "laoma_signal_engine.api.app.trade_quality_entry_market_context_backfill_service",
        lambda **kwargs: {
            "schema_version": "19.21",
            "mode": "dry_run" if kwargs.get("dry_run") else "run",
            "candidate_count": 1,
            "updated_count": 0 if kwargs.get("dry_run") else 1,
            "status_counts": {"complete": 1},
            "label_counts": {"market_context_supported": 1},
        },
    )
    monkeypatch.setattr(
        "laoma_signal_engine.api.app.trade_quality_entry_context_v3_backfill_service",
        lambda **kwargs: {
            "schema_version": "19.23",
            "mode": "dry_run" if kwargs.get("dry_run") else "run",
            "candidate_count": 1,
            "updated_count": 0 if kwargs.get("dry_run") else 1,
            "label_counts": {"market_context_supported": 1},
        },
    )
    monkeypatch.setattr(
        "laoma_signal_engine.api.app.trade_quality_diagnostics_refresh_enrich_service",
        lambda **kwargs: {
            "schema_version": "12.57",
            "mode": "dry_run" if kwargs.get("dry_run") else "run",
            "status": "ok",
            "stages": [{"stage": "entry_context_v3", "status": "ok"}],
            "summary": {"sample_count": 1},
        },
    )
    client = TestClient(app)

    sync = client.post("/api/trade-quality/diagnostics/sync/run")
    summary = client.get("/api/trade-quality/diagnostics/summary")
    samples = client.get("/api/trade-quality/diagnostics/samples?symbol=P19USDT")
    aggregates = client.get("/api/trade-quality/diagnostics/aggregates")
    entry_dry = client.post("/api/trade-quality/entry-features/backfill/dry-run?source=current_paper&limit=1")
    entry_run = client.post("/api/trade-quality/entry-features/backfill/run?source=current_paper&limit=1")
    micro_dry = client.post("/api/trade-quality/entry-microstructure/backfill/dry-run?source=current_paper&limit=1")
    micro_run = client.post("/api/trade-quality/entry-microstructure/backfill/run?source=current_paper&limit=1")
    market_run = client.post("/api/trade-quality/entry-market-context/backfill/run?source=current_paper&limit=1")
    v3_run = client.post("/api/trade-quality/entry-context-v3/backfill/run?source=current_paper&limit=1")
    refresh_enrich = client.post("/api/trade-quality/diagnostics/refresh-enrich?source=current_paper&limit=1")

    assert sync.status_code == 200
    assert summary.status_code == 200
    assert samples.status_code == 200
    assert aggregates.status_code == 200
    assert entry_dry.status_code == 200
    assert entry_run.status_code == 200
    assert micro_dry.status_code == 200
    assert micro_run.status_code == 200
    assert market_run.status_code == 200
    assert v3_run.status_code == 200
    assert refresh_enrich.status_code == 200
    summary_data = summary.json()["data"]
    assert summary_data["payload_kind"] == "summary_only"
    assert summary_data["summary"]["sample_count"] == 1
    assert "root_cause_attribution" in summary_data["summary"]
    assert "dimension_attribution" in summary_data["summary"]
    assert "entry_quality_attribution" in summary_data["summary"]
    assert "entry_microstructure_attribution" in summary_data["summary"]
    assert "entry_market_context_attribution" in summary_data["summary"]
    assert "entry_context_v3_attribution" in summary_data["summary"]
    assert "samples" not in summary_data
    assert samples.json()["data"]["samples"][0]["trade_id"]
    assert any(row["dimension"] == "root_cause" for row in aggregates.json()["data"]["aggregates"])
    assert entry_dry.json()["data"]["mode"] == "dry_run"
    assert entry_run.json()["data"]["updated_count"] == 1
    assert micro_dry.json()["data"]["mode"] == "dry_run"
    assert micro_run.json()["data"]["updated_count"] == 1
    assert market_run.json()["data"]["updated_count"] == 1
    assert v3_run.json()["data"]["updated_count"] == 1
    assert refresh_enrich.json()["data"]["status"] == "ok"


def test_step198_current_paper_sync_scopes_closed_paper_orders_only(tmp_path: Path, monkeypatch) -> None:
    db_path = _prepare_p19_db(tmp_path, monkeypatch)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO paper_orders(
              id, strategy_line, source_run_id, source_cycle_id, source_plan_hash,
              symbol, side, status, order_type, entry_price, filled_entry_price,
              stop_loss, take_profit, margin_usdt, leverage, quantity,
              remaining_quantity, notional_usdt, realized_pnl_usdt,
              unrealized_pnl_usdt, fee_usdt, slippage_usdt, fee_bps,
              slippage_bps, exit_price, exit_reason, opened_at, closed_at,
              created_at, updated_at
            ) VALUES(
              'paper_order_p19_second', 'micro_fast', 'run_p19_b', 'cycle_p19_b',
              'hash_p19_second', 'P20USDT', 'SHORT', 'closed', 'market',
              100, 100, 105, 90, 100, 20, 10,
              0, 1000, -20, 0, 1, 0.5, 0, 0, 102, 'SL',
              '2026-06-04T00:05:00Z', '2026-06-04T00:15:00Z',
              '2026-06-04T00:05:00Z', '2026-06-04T00:15:00Z'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO paper_orders(
              id, strategy_line, source_run_id, source_cycle_id, source_plan_hash,
              symbol, side, status, order_type, entry_price, filled_entry_price,
              stop_loss, take_profit, margin_usdt, leverage, quantity,
              remaining_quantity, notional_usdt, realized_pnl_usdt,
              unrealized_pnl_usdt, fee_usdt, slippage_usdt, fee_bps,
              slippage_bps, exit_price, exit_reason, opened_at, closed_at,
              created_at, updated_at
            ) VALUES(
              'paper_order_p19_open', 'micro_fast', 'run_p19_c', 'cycle_p19_c',
              'hash_p19_open', 'P21USDT', 'LONG', 'open', 'market',
              100, 100, 95, 110, 100, 20, 10,
              10, 1000, 0, 0, 1, 0.5, 0, 0, NULL, NULL,
              '2026-06-04T00:06:00Z', NULL,
              '2026-06-04T00:06:00Z', '2026-06-04T00:06:00Z'
            )
            """
        )

    dry = diagnostic_backfill_payload(tmp_path, write=False, source="current_paper", config=_config())
    assert dry["source_counts"] == {"current_paper": 2}
    assert dry["candidate_samples"] == 2

    client = TestClient(app)
    sync = client.post("/api/trade-quality/diagnostics/sync/run?source=current_paper")
    summary = client.get("/api/trade-quality/diagnostics/summary?source=current_paper")
    samples = client.get("/api/trade-quality/diagnostics/samples?source=current_paper&limit=10")

    assert sync.status_code == 200
    assert sync.json()["data"]["source_counts"] == {"current_paper": 2}
    assert summary.json()["data"]["summary"]["sample_count"] == 2
    got = samples.json()["data"]["samples"]
    assert {row["symbol"] for row in got} == {"P19USDT", "P20USDT"}
    assert all(row["source"] == "current_paper" for row in got)


def test_step199_current_paper_replay_updates_p19_phenomena(tmp_path: Path, monkeypatch) -> None:
    _prepare_p19_db(tmp_path, monkeypatch)
    client = TestClient(app)
    sync = client.post("/api/trade-quality/diagnostics/sync/run?source=current_paper")
    assert sync.status_code == 200
    before = client.get("/api/trade-quality/diagnostics/samples?source=current_paper&limit=1").json()["data"]["samples"][0]
    opened = datetime.fromisoformat(str(before["entry_time"]).replace("Z", "+00:00"))
    closed = datetime.fromisoformat(str(before["exit_time"]).replace("Z", "+00:00"))

    replay = diagnostic_replay_payload(
        tmp_path,
        write=True,
        source="current_paper",
        config=_config(),
        candle_provider=_RangeCandleProvider(
            [
                Candle("P19USDT", int(opened.timestamp() * 1000), 100, 112, 99, 110),
                Candle("P19USDT", int(closed.timestamp() * 1000), 100, 113, 98, 111),
            ]
        ),
    )
    summary = client.get("/api/trade-quality/diagnostics/summary?source=current_paper")
    samples = client.get("/api/trade-quality/diagnostics/samples?source=current_paper&limit=10")

    assert replay["current_paper_replay"]["updated_samples"] == 1
    summary_data = summary.json()["data"]["summary"]
    assert summary_data["phenomenon_sample_count"] == 1
    assert summary_data["replay_status_counts"]["candle_1m_replay"] == 1
    row = samples.json()["data"]["samples"][0]
    assert row["replay_status"] == "candle_1m_replay"
    assert row["MFE_R"] is not None
    assert row["MAE_R"] is not None


def test_step1910_current_paper_sync_preserves_replay_phenomena(tmp_path: Path, monkeypatch) -> None:
    _prepare_p19_db(tmp_path, monkeypatch)
    client = TestClient(app)
    sync = client.post("/api/trade-quality/diagnostics/sync/run?source=current_paper")
    assert sync.status_code == 200
    before = client.get("/api/trade-quality/diagnostics/samples?source=current_paper&limit=1").json()["data"]["samples"][0]
    opened = datetime.fromisoformat(str(before["entry_time"]).replace("Z", "+00:00"))
    closed = datetime.fromisoformat(str(before["exit_time"]).replace("Z", "+00:00"))

    replay = diagnostic_replay_payload(
        tmp_path,
        write=True,
        source="current_paper",
        config=_config(),
        candle_provider=_RangeCandleProvider(
            [
                Candle("P19USDT", int(opened.timestamp() * 1000), 100, 112, 99, 110),
                Candle("P19USDT", int(closed.timestamp() * 1000), 100, 113, 98, 111),
            ]
        ),
    )
    assert replay["current_paper_replay"]["updated_samples"] == 1

    resync = client.post("/api/trade-quality/diagnostics/sync/run?source=current_paper")
    assert resync.status_code == 200
    summary_data = client.get("/api/trade-quality/diagnostics/summary?source=current_paper").json()["data"]["summary"]
    row = client.get("/api/trade-quality/diagnostics/samples?source=current_paper&limit=1").json()["data"]["samples"][0]

    assert summary_data["phenomenon_sample_count"] == 1
    assert summary_data["replay_status_counts"]["candle_1m_replay"] == 1
    assert row["replay_status"] == "candle_1m_replay"
    assert row["MFE_R"] is not None
    assert row["MAE_R"] is not None
    assert row["root_cause"] not in {None, "unknown"}


def test_step1924_current_paper_epoch_scope_excludes_pre_archive_cache(tmp_path: Path, monkeypatch) -> None:
    db_path = _prepare_p19_db(tmp_path, monkeypatch)
    client = TestClient(app)
    first_sync = client.post("/api/trade-quality/diagnostics/sync/run?source=current_paper")
    assert first_sync.status_code == 200

    with sqlite3.connect(db_path) as conn:
        old_diag_count = conn.execute(
            "SELECT count(*) FROM trade_quality_diagnostic_samples WHERE source='current_paper'"
        ).fetchone()[0]
        assert old_diag_count == 1
        conn.execute("DELETE FROM paper_orders WHERE strategy_line='without_micro'")
        conn.execute(
            """
            INSERT INTO paper_reset_epochs(
              strategy_line, reset_epoch_id, experiment_id, reset_at,
              reset_after_run_id, reason, detail_json
            ) VALUES(
              'without_micro', 'paper_epoch_step1924_without_micro',
              'paper_exp_step1924_without_micro', '2026-06-04T01:00:00Z',
              'run_before_archive', 'archive_reset', '{}'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO paper_orders(
              id, strategy_line, source_run_id, source_cycle_id, source_plan_hash,
              intent_id, reset_epoch_id, symbol, side, status, order_type,
              entry_price, filled_entry_price, stop_loss, take_profit,
              margin_usdt, leverage, quantity, remaining_quantity, notional_usdt,
              realized_pnl_usdt, unrealized_pnl_usdt, fee_usdt, slippage_usdt,
              fee_bps, slippage_bps, exit_price, exit_reason, opened_at,
              closed_at, created_at, updated_at
            ) VALUES(
              'paper_order_step1924_new', 'without_micro', 'run_after_archive',
              'cycle_after_archive', 'hash_after_archive', 'intent_after_archive',
              'paper_epoch_step1924_without_micro', 'NEWP19USDT', 'LONG', 'closed',
              'market', 100, 100, 95, 106, 100, 20, 10, 0, 1000,
              12, 0, 0, 0, 0, 0, 106, 'TP',
              '2026-06-04T01:05:00Z', '2026-06-04T01:15:00Z',
              '2026-06-04T01:05:00Z', '2026-06-04T01:15:00Z'
            )
            """
        )

    resync = client.post("/api/trade-quality/diagnostics/sync/run?source=current_paper")
    summary = client.get("/api/trade-quality/diagnostics/summary?source=current_paper")
    samples = client.get("/api/trade-quality/diagnostics/samples?source=current_paper&limit=10")

    assert resync.status_code == 200
    assert summary.status_code == 200
    assert samples.status_code == 200
    summary_data = summary.json()["data"]["summary"]
    got = samples.json()["data"]["samples"]
    assert summary_data["sample_count"] == 1
    assert summary_data["excluded_stale_current_paper_samples"] == 1
    assert summary_data["stale_current_paper_warning"] == "current_paper_epoch_scope_excluded_stale_samples"
    assert {row["symbol"] for row in got} == {"NEWP19USDT"}
    assert got[0]["paper_reset_epoch_id"] == "paper_epoch_step1924_without_micro"

    with sqlite3.connect(db_path) as conn:
        retained = conn.execute(
            "SELECT count(*) FROM trade_quality_diagnostic_samples WHERE source='current_paper'"
        ).fetchone()[0]
    assert retained == 2


def test_step1254_archive_packages_and_sync_status(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)
    archive_dir = tmp_path / "DATA" / "paper" / "archives" / "paper_exp_20260604T010203Z_without_micro"
    archive_dir.mkdir(parents=True)
    (archive_dir / "metadata.json").write_text(
        '{"archived_at":"2026-06-04T01:02:03Z","forced_close_rows":[{"symbol":"P19USDT"}]}',
        encoding="utf-8",
    )
    diagnostic_backfill_payload(tmp_path, write=True, config=_config())
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trade_quality_diagnostic_samples
            SET source='archive',
                archive_id='paper_exp_20260604T010203Z_without_micro',
                archive_path=?
            """,
            (str(archive_dir),),
        )

    packages = diagnostic_archive_packages_payload(tmp_path, config=_config())
    status = diagnostic_sync_status_payload(tmp_path, config=_config())

    assert packages["count"] == 1
    pkg = packages["packages"][0]
    assert pkg["archive_id"] == "paper_exp_20260604T010203Z_without_micro"
    assert pkg["diagnostic_sample_count"] == 1
    assert pkg["replay_count"] == 1
    assert status["sample_count"] == 1
    assert status["stale"] is False


def test_step196_archive_replay_resyncs_selected_package(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)
    archive_id = "paper_exp_20260604T020304Z_without_micro"
    archive_dir = tmp_path / "DATA" / "paper" / "archives" / archive_id
    archive_dir.mkdir(parents=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_archive_ingest_ledger(
              dedup_key TEXT PRIMARY KEY,
              sample_id TEXT,
              order_id TEXT,
              source_plan_hash TEXT,
              source_run_id TEXT,
              source_cycle_id TEXT,
              strategy_line TEXT,
              symbol TEXT,
              side TEXT,
              archive_path TEXT,
              archive_mtime TEXT,
              archive_hash TEXT,
              ingest_status TEXT,
              skip_reason TEXT,
              schema_version TEXT,
              ingested_at TEXT
            )
            """
        )
        sample_id = conn.execute("SELECT sample_id FROM trade_quality_samples LIMIT 1").fetchone()[0]
        conn.execute(
            """
            UPDATE trade_quality_samples
            SET excursion_model='proxy_from_close', MFE_R=NULL, MAE_R=1.0
            WHERE sample_id=?
            """,
            (sample_id,),
        )
        conn.execute(
            """
            INSERT INTO trade_quality_archive_ingest_ledger(
              dedup_key, sample_id, order_id, source_plan_hash, source_run_id, source_cycle_id,
              strategy_line, symbol, side, archive_path, archive_mtime, archive_hash,
              ingest_status, skip_reason, schema_version, ingested_at
            ) VALUES('dedup-p19-196', ?, 'order-p19-196', 'hash', 'run', 'cycle',
              'without_micro', 'P19USDT', 'LONG', ?, '2026-06-04T02:03:04Z', 'hash',
              'inserted', NULL, '18.9', '2026-06-04T02:03:04Z')
            """,
            (sample_id, str(archive_dir)),
        )
        opened_at = conn.execute("SELECT opened_at FROM trade_quality_samples WHERE sample_id=?", (sample_id,)).fetchone()[0]

    before = diagnostic_backfill_payload(tmp_path, write=True, source="archive", archive_id=archive_id, config=_config())
    before_summary = diagnostic_summary_payload(db_path, source="archive", archive_id=archive_id)["summary"]

    result = diagnostic_replay_payload(
        tmp_path,
        write=True,
        limit=1,
        source="archive",
        archive_id=archive_id,
        config=_config(),
        candle_provider=_RangeCandleProvider(
            [
                Candle(
                    "P19USDT",
                    int(datetime.fromisoformat(str(opened_at).replace("Z", "+00:00")).timestamp() * 1000),
                    100,
                    112,
                    97,
                    110,
                )
            ]
        ),
    )
    after_summary = diagnostic_summary_payload(db_path, source="archive", archive_id=archive_id)["summary"]
    sample = diagnostic_samples_payload(db_path, source="archive", archive_id=archive_id, limit=1)["samples"][0]

    assert before["candidate_samples"] == 1
    assert before_summary["replay_coverage"] == 0
    assert before_summary["phenomenon_sample_count"] == 0
    assert result["diagnostic_sync"]["archive_id"] == archive_id
    assert result["diagnostic_sync"]["candidate_samples"] == 1
    assert sample["replay_status"] == "candle_1m_replay"
    assert after_summary["replay_coverage"] == 1
    assert after_summary["phenomenon_sample_count"] == 1


def test_step196_phenomena_exclude_proxy_samples(tmp_path: Path) -> None:
    db_path = _prepare_p19_db(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trade_quality_samples
            SET excursion_model='proxy_from_close',
                MFE_R=0.1,
                MAE_R=1.2,
                net_R=-1.0
            """
        )

    diagnostic_backfill_payload(tmp_path, write=True, config=_config())
    summary = diagnostic_summary_payload(db_path)["summary"]

    assert summary["sample_count"] == 1
    assert summary["replay_coverage"] == 0
    assert summary["phenomenon_sample_count"] == 0
    assert summary["phenomenon_replay_required"] is True
    assert all(row["count"] == 0 for row in summary["phenomena"])
