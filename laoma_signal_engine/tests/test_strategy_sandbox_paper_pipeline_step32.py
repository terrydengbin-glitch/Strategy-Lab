from __future__ import annotations

import sqlite3
from pathlib import Path

import laoma_signal_engine.api.services as api_services
from laoma_signal_engine.strategy_sandbox.paper_pipeline import run_sandbox_paper_pipeline
from laoma_signal_engine.strategy_sandbox.resource_governor import (
    resource_run_payload,
    start_external_research_context,
    start_ui_sandbox_pipeline_context,
)
from laoma_signal_engine.training_snapshot_sync import (
    source_mode_for_sandbox_paper,
)


def _doc(line: str = "without_micro", *, symbol: str = "OPGUSDT", entry: float = 1.0, sl: float = 0.9, tp: float = 1.1) -> dict:
    return {
        "schema_version": "1.0",
        "generated_at": "2026-06-17T00:00:00Z",
        "run_id": "run_step32_input",
        "cycle_id": "cycle_step32_input",
        "source": "trade_plan_without_micro",
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
                "risk_per_unit": abs(entry - sl),
                "reward_per_unit": abs(tp - entry),
                "rr": abs(tp - entry) / abs(entry - sl),
                "executable": True,
                "confidence": 80,
                "reason_codes": [],
                "guards": {"line": line, "margin_usdt": 100, "leverage": 20},
                "input_refs": {},
            }
        ],
    }


def _candles(symbol: str = "OPGUSDT") -> dict[str, list[dict]]:
    return {
        symbol: [
            {"symbol": symbol, "open_time_ms": 1_000, "open": 1.0, "high": 1.02, "low": 0.99, "close": 1.01, "volume": 1000},
            {"symbol": symbol, "open_time_ms": 61_000, "open": 1.01, "high": 1.12, "low": 1.0, "close": 1.1, "volume": 1000},
        ]
    }


def test_step321_sandbox_paper_runner_generates_tq_and_training_dataset(tmp_path: Path) -> None:
    context = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_step32",
        active_sandbox_id="sb_step32",
        dry_run=False,
        options={"run_id": "run_step32", "strategy_line": "without_micro", "symbol": "OPGUSDT"},
    )
    result = run_sandbox_paper_pipeline(
        tmp_path,
        sandbox_id="sb_step32",
        run_id="run_step32",
        cycle_id="cycle_run_step32",
        writer_context=context["writer_context"],
        docs={"without_micro": _doc()},
        candles_by_symbol=_candles(),
    )

    assert result["execution_contract"] == "paper_engine_sandbox_scoped"
    assert result["status"] == "completed"
    assert result["main_chain_mutation_allowed"] is False
    assert result["counts"]["paper_orders"] == 1
    assert result["counts"]["paper_fills"] >= 2
    assert result["trade_quality_completion"]["trade_quality_completion_status"] == "ok"
    assert result["training_dataset"]["source_mode"] == "ui_sandbox_paper"
    assert result["training_dataset"]["training_dataset_source_mode"] == "ui_sandbox_paper"
    assert result["training_dataset"]["training_export_dir"].endswith("DATA/research/trade_snapshots/sandbox_exports/sb_step32/run_step32")
    assert result["training_dataset"]["training_dataset_manifest_path"].endswith("sandbox_exports/sb_step32/run_step32/dataset_manifest.json")
    assert result["training_dataset"]["training_mirror_manifest_path"].endswith("DATA/sandboxes/sb_step32/runtime/pipeline_runs/run_step32/training/dataset_manifest.json")
    assert result["training_dataset"]["samples_written"] >= 1
    assert (tmp_path / result["paper_db_path"]).exists()
    assert (tmp_path / result["input_snapshot"]["input_snapshot_path"]).exists()
    assert (tmp_path / result["training_dataset"]["training_dataset_manifest_path"]).exists()
    assert (tmp_path / result["training_dataset"]["training_mirror_manifest_path"]).exists()
    assert not (tmp_path / "DATA" / "backtest" / "p21_parameter_optimization.db").exists()
    with sqlite3.connect(tmp_path / result["paper_db_path"]) as conn:
        null_entry_candles = conn.execute(
            "SELECT COUNT(*) FROM paper_fills WHERE lower(action) = 'entry' AND candle_open_time_ms IS NULL"
        ).fetchone()[0]
    assert null_entry_candles == 0


def test_step3411_historical_market_entry_waits_when_symbol_candle_missing(tmp_path: Path) -> None:
    context = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_step3411",
        active_sandbox_id="sb_step3411",
        dry_run=False,
        options={"run_id": "run_step3411", "strategy_line": "without_micro", "symbol": "OPGUSDT"},
    )
    doc = _doc(symbol="OPGUSDT")
    missing_symbol_plan = _doc(symbol="MISSUSDT")["plans"][0]
    doc["plans"].append(missing_symbol_plan)
    doc["count"] = 2
    doc["executable_count"] = 2

    result = run_sandbox_paper_pipeline(
        tmp_path,
        sandbox_id="sb_step3411",
        run_id="run_step3411",
        cycle_id="cycle_run_step3411",
        writer_context=context["writer_context"],
        docs={"without_micro": doc},
        candles_by_symbol=_candles("OPGUSDT"),
    )

    assert result["status"] == "completed"
    assert result["counts"]["paper_orders"] == 2
    with sqlite3.connect(tmp_path / result["paper_db_path"]) as conn:
        null_entry_candles = conn.execute(
            "SELECT COUNT(*) FROM paper_fills WHERE lower(action) = 'entry' AND candle_open_time_ms IS NULL"
        ).fetchone()[0]
        missing_pending = conn.execute(
            "SELECT COUNT(*) FROM paper_orders WHERE symbol = 'MISSUSDT' AND status = 'pending_entry'"
        ).fetchone()[0]
        filled_symbols = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT symbol FROM paper_fills WHERE lower(action) = 'entry'"
            ).fetchall()
        }

    assert null_entry_candles == 0
    assert missing_pending == 1
    assert filled_symbols == {"OPGUSDT"}


def test_step2916_source_mode_resolver_and_external_sandbox_paths(tmp_path: Path) -> None:
    assert source_mode_for_sandbox_paper("ui_active_sandbox_real_pipeline") == "ui_sandbox_paper"
    assert source_mode_for_sandbox_paper("external_cli_research_lane") == "external_cli_sandbox_paper"
    assert source_mode_for_sandbox_paper("ui_active_sandbox_real_pipeline", pipeline_mode="sandbox_full_pipeline") == "ui_sandbox_full_pipeline"
    assert source_mode_for_sandbox_paper("external_cli_research_lane", pipeline_mode="sandbox_full_pipeline") == "external_cli_sandbox_full_pipeline"
    context = start_external_research_context(
        project_root=tmp_path,
        sandbox_id="sb_ext32",
        run_id="run_ext32",
        options={"run_id": "run_ext32", "strategy_line": "without_micro", "symbol": "OPGUSDT"},
    )
    result = run_sandbox_paper_pipeline(
        tmp_path,
        sandbox_id="sb_ext32",
        run_id="run_ext32",
        cycle_id="cycle_run_ext32",
        writer_context=context["writer_context"],
        docs={"without_micro": _doc()},
        candles_by_symbol=_candles(),
    )

    assert result["status"] == "completed"
    assert result["training_dataset"]["source_mode"] == "external_cli_sandbox_paper"
    assert result["training_dataset"]["resource_lane"] == "external_cli_research_lane"
    assert result["training_dataset"]["training_export_dir"].endswith("DATA/research/trade_snapshots/sandbox_exports/sb_ext32/run_ext32")
    assert (tmp_path / result["training_dataset"]["training_dataset_manifest_path"]).exists()


def test_step321_runner_blocks_without_writer_context(tmp_path: Path) -> None:
    result = run_sandbox_paper_pipeline(
        tmp_path,
        sandbox_id="sb_missing",
        run_id="run_missing",
        cycle_id="cycle_missing",
        writer_context={},
        docs={"without_micro": _doc()},
        candles_by_symbol=_candles(),
    )

    assert result["status"] == "blocked"
    assert result["reason_code"] == "sandbox_writer_context_required"


def test_step323_pipeline_service_runs_and_records_finished_event(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(api_services, "sandbox_active_payload", lambda: {"active_sandbox_id": "sb_api"})

    payload = {
        "sandbox_id": "sb_api",
        "dry_run": False,
        "requires_live_rest": False,
        "cache_hit": True,
        "options": {
            "run_id": "run_api_step32",
            "strategy_line": "without_micro",
            "symbol": "OPGUSDT",
            "docs": {"without_micro": _doc()},
            "candles_by_symbol": _candles(),
        },
    }
    result = api_services.strategy_sandbox_pipeline_run_service(payload)
    detail = resource_run_payload(tmp_path, run_id="run_api_step32")

    assert result["accepted"] is True
    assert result["execution_result"]["execution_contract"] == "paper_engine_sandbox_scoped"
    assert result["finish"]["released"] is True
    assert detail["latest"]["event_type"] == "run_finished"
    assert detail["latest"]["result"]["paper_db_path"].endswith("paper_trading.db")
    with sqlite3.connect(tmp_path / result["execution_result"]["paper_db_path"]) as conn:
        fills = conn.execute("SELECT COUNT(*) FROM paper_fills").fetchone()[0]
    assert fills >= 2
