from __future__ import annotations

import sqlite3
from pathlib import Path

from laoma_signal_engine.backtest.p21 import baseline_payload, run_matrix_payload
from laoma_signal_engine.paper.models import PaperConfig
from laoma_signal_engine.trade_quality.diagnostics import ensure_diagnostic_tables


def _insert_sample(
    db_path: Path,
    *,
    trade_id: str,
    strategy_line: str,
    net_R: float,
    mfe_R: float,
    mae_R: float,
    root_cause: str,
) -> None:
    now = "2026-06-07T00:00:00Z"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO trade_quality_diagnostic_samples(
              diagnostic_id, trade_id, source, strategy_line, symbol, side,
              entry_time, exit_time, holding_minutes, exit_reason,
              fee, net_pnl, initial_risk_usdt, net_R, planned_RR,
              MFE_R, MAE_R, replay_status, root_cause, quality_tags_json,
              diagnostic_version, evidence_json, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"diag_{trade_id}",
                trade_id,
                "archive",
                strategy_line,
                f"{trade_id.upper()}USDT",
                "LONG",
                "2026-06-07T00:00:00Z",
                "2026-06-07T00:05:00Z",
                5,
                "SL" if net_R < 0 else "TP",
                0,
                net_R * 10,
                10,
                net_R,
                1.0,
                mfe_R,
                mae_R,
                "complete",
                root_cause,
                "[]",
                "test",
                "{}",
                now,
                now,
            ),
        )


def test_p21_baseline_and_matrix_find_pf_candidate(tmp_path: Path) -> None:
    db_path = tmp_path / "DATA" / "paper" / "paper_trading.db"
    ensure_diagnostic_tables(db_path)
    _insert_sample(db_path, trade_id="win1", strategy_line="without_micro", net_R=1.0, mfe_R=1.2, mae_R=0.2, root_cause="profitable_trade")
    _insert_sample(db_path, trade_id="win2", strategy_line="strategy4", net_R=0.8, mfe_R=1.0, mae_R=0.3, root_cause="profitable_trade")
    _insert_sample(db_path, trade_id="loss1", strategy_line="strategy5", net_R=-1.2, mfe_R=0.1, mae_R=1.1, root_cause="direction_wrong")
    _insert_sample(db_path, trade_id="loss2", strategy_line="micro_fast", net_R=-5.0, mfe_R=0.0, mae_R=2.0, root_cause="outside_scope")
    cfg = PaperConfig(db_path="DATA/paper/paper_trading.db")

    baseline = baseline_payload(tmp_path, source="all", strategy_line="all", config=cfg)
    assert baseline["sample_count"] == 3
    assert baseline["metrics"]["profit_factor"] == 1.5

    matrix = run_matrix_payload(tmp_path, source="all", strategy_line="all", max_sets=20, config=cfg)
    assert matrix["sample_count"] == 3
    assert matrix["leaderboard"]
    assert any((item["metrics"]["profit_factor"] or 0) > 1 for item in matrix["leaderboard"])
    assert (tmp_path / "DATA" / "backtest" / "p21_parameter_optimization.db").exists()

