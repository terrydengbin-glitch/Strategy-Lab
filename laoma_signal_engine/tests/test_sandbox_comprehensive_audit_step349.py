from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from laoma_signal_engine.backtest.paper_equivalent import default_paper_equivalent_config, run_paper_equivalent_backtest
from laoma_signal_engine.cli import main as cli_main
from laoma_signal_engine.paper.candles import StaticCandleProvider
from laoma_signal_engine.paper.engine import PaperEngine
from laoma_signal_engine.paper.models import Candle, PaperConfig
from laoma_signal_engine.strategy_sandbox.full_pipeline import run_sandbox_full_pipeline
from laoma_signal_engine.strategy_sandbox.paper_pipeline import run_sandbox_paper_pipeline
from laoma_signal_engine.strategy_sandbox.resource_governor import (
    start_external_research_context,
    start_ui_sandbox_pipeline_context,
)
from laoma_signal_engine.strategy_sandbox.service import create_sandbox_payload


POST_TRADE_ONLY_FIELDS = ("net_R", "MFE_R", "MAE_R", "exit_reason", "gross_pnl_usdt", "net_pnl_usdt")


def _doc(
    line: str = "without_micro",
    *,
    symbol: str = "S349USDT",
    run_id: str = "run_step349_input",
    cycle_id: str = "cycle_step349_input",
    entry: float = 1.0,
    sl: float = 0.9,
    tp: float = 1.1,
) -> dict:
    return {
        "schema_version": "1.0",
        "generated_at": "2026-06-17T00:00:00Z",
        "run_id": run_id,
        "cycle_id": cycle_id,
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
                "reason_codes": ["step34_9_fixture_trade_plan"],
                "guards": {"line": line, "margin_usdt": 100, "leverage": 20},
                "input_refs": {},
            }
        ],
    }


def _candles(symbol: str = "S349USDT") -> dict[str, list[dict]]:
    return {
        symbol: [
            {"symbol": symbol, "open_time_ms": 1_000, "open": 1.0, "high": 1.02, "low": 0.99, "close": 1.01, "volume": 1000},
            {"symbol": symbol, "open_time_ms": 61_000, "open": 1.01, "high": 1.12, "low": 1.0, "close": 1.1, "volume": 1000},
        ]
    }


def _baseline_paper_config() -> PaperConfig:
    return PaperConfig(
        db_path="DATA/paper/step34_9_baseline_paper.db",
        summary_path="DATA/paper/step34_9_latest_paper_state.json",
        default_slippage_bps=0,
        taker_fee_bps=0,
    )


def _assert_training_dataset(
    root: Path,
    payload: dict,
    *,
    source_mode: str,
    run_id: str | None = None,
    sandbox_id: str | None = None,
    resource_lane: str | None = None,
) -> None:
    training = payload["training_dataset"] if "training_dataset" in payload else payload
    assert training["source_mode"] == source_mode
    assert training["training_dataset_source_mode"] == source_mode
    assert training["training_sidecar_db_path"] == "DATA/research/trade_snapshots/trade_snapshots.db"
    assert training["source_table"] in {"paper_orders", "sandbox_orders"}
    assert training["samples_written"] >= 1
    assert training["events_written"] >= 2
    assert training["training_ready"] is False
    assert training["training_dataset_status"] in {"needs_review", "incomplete"}
    if run_id:
        assert training["run_id"] == run_id
    if sandbox_id:
        assert training["sandbox_id"] == sandbox_id
        assert f"sandbox_exports/{sandbox_id}/{run_id}" in training["training_export_dir"].replace("\\", "/")
    if resource_lane:
        assert training["resource_lane"] == resource_lane
    assert (root / training["training_sidecar_db_path"]).exists()
    assert (root / training["training_dataset_manifest_path"]).exists()
    assert (root / training["training_dataset_coverage_path"]).exists()
    assert (root / training["training_dataset_leakage_path"]).exists()


def _assert_no_post_trade_leakage(root: Path) -> None:
    db_path = root / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db"
    assert db_path.exists()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT sample_id, decision_time_input_json FROM trade_training_samples").fetchall()
    assert rows
    for row in rows:
        decision_keys = _walk_keys(json.loads(row["decision_time_input_json"]))
        for field in POST_TRADE_ONLY_FIELDS:
            assert field not in decision_keys, f"{field} leaked into decision_time_input_json for {row['sample_id']}"


def _sidecar_source_modes(root: Path) -> set[str]:
    with sqlite3.connect(root / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db") as conn:
        return {str(row[0]) for row in conn.execute("SELECT DISTINCT source_mode FROM trade_training_samples").fetchall()}


def _walk_keys(value) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            keys.add(str(key))
            keys.update(_walk_keys(item))
    elif isinstance(value, list):
        for item in value:
            keys.update(_walk_keys(item))
    return keys


def test_step349_baseline_ui_and_external_lane_paper_audit(tmp_path: Path) -> None:
    peq = run_paper_equivalent_backtest(
        tmp_path,
        docs={"without_micro": _doc(symbol="PEQ349USDT", entry=100, sl=95, tp=105)},
        candles_by_symbol={
            "PEQ349USDT": [
                Candle("PEQ349USDT", 1_000, 100, 101, 99, 100),
                Candle("PEQ349USDT", 61_000, 100, 106, 99, 105),
            ]
        },
        run_id="step349_peq",
        config=default_paper_equivalent_config(run_id="step349_peq", base=PaperConfig(default_slippage_bps=0, taker_fee_bps=0)),
    )
    assert peq["execution_contract"] == "paper_equivalent"
    assert peq["trade_quality_completion"]["trade_quality_completion_status"] == "ok"
    _assert_training_dataset(tmp_path, peq, source_mode="paper_equivalent_backtest", run_id="paper_equivalent_step349_peq")
    assert "sandbox_exports" not in peq["training_dataset"]["training_export_dir"]

    engine = PaperEngine(
        tmp_path,
        config=_baseline_paper_config(),
        candle_provider=StaticCandleProvider(
            {"PAPER349USDT": [Candle("PAPER349USDT", 1_000, 1.0, 1.02, 0.99, 1.01), Candle("PAPER349USDT", 61_000, 1.01, 1.12, 1.0, 1.1)]}
        ),
    )
    paper = engine.tick({"without_micro": _doc(symbol="PAPER349USDT")})
    assert paper["entries"]["entered"] == 1
    assert paper["closes"]["closed"] == 1
    assert paper["trade_quality_completion"]["trade_quality_completion_status"] == "ok"
    _assert_training_dataset(tmp_path, paper, source_mode="paper")
    assert (tmp_path / "DATA" / "paper" / "step34_9_baseline_paper.db").exists()
    assert not (tmp_path / "DATA" / "sandboxes" / "baseline").exists()

    ui_context = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb349uip",
        active_sandbox_id="sb349uip",
        dry_run=False,
        options={"run_id": "r349uip", "strategy_line": "without_micro", "symbol": "UIP349USDT"},
    )
    ui_paper = run_sandbox_paper_pipeline(
        tmp_path,
        sandbox_id="sb349uip",
        run_id="r349uip",
        cycle_id="cycle_r349uip",
        writer_context=ui_context["writer_context"],
        docs={"without_micro": _doc(symbol="UIP349USDT")},
        candles_by_symbol=_candles("UIP349USDT"),
    )
    assert ui_paper["execution_contract"] == "paper_engine_sandbox_scoped"
    assert ui_paper["status"] == "completed"
    assert ui_paper["trade_quality_completion"]["trade_quality_completion_status"] == "ok"
    _assert_training_dataset(
        tmp_path,
        ui_paper,
        source_mode="ui_sandbox_paper",
        run_id="r349uip",
        sandbox_id="sb349uip",
        resource_lane="ui_active_sandbox_real_pipeline",
    )
    assert (tmp_path / ui_paper["training_dataset"]["training_mirror_manifest_path"]).exists()
    assert not (tmp_path / "DATA" / "paper" / "paper_trading.db").exists()

    external_context = start_external_research_context(
        project_root=tmp_path,
        sandbox_id="sb349clip",
        run_id="r349clip",
        options={"strategy_line": "without_micro", "symbol": "CLIP349USDT"},
    )
    cli_lane_paper = run_sandbox_paper_pipeline(
        tmp_path,
        sandbox_id="sb349clip",
        run_id="r349clip",
        cycle_id="cycle_r349clip",
        writer_context=external_context["writer_context"],
        docs={"without_micro": _doc(symbol="CLIP349USDT")},
        candles_by_symbol=_candles("CLIP349USDT"),
    )
    assert cli_lane_paper["status"] == "completed"
    _assert_training_dataset(
        tmp_path,
        cli_lane_paper,
        source_mode="external_cli_sandbox_paper",
        run_id="r349clip",
        sandbox_id="sb349clip",
        resource_lane="external_cli_research_lane",
    )

    modes = _sidecar_source_modes(tmp_path)
    assert {"paper_equivalent_backtest", "paper", "ui_sandbox_paper", "external_cli_sandbox_paper"}.issubset(modes)
    _assert_no_post_trade_leakage(tmp_path)


def test_step349_ui_full_pipeline_cli_full_pipeline_and_cli_backtest_audit(tmp_path: Path, capsys) -> None:
    ui_context = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb349uif",
        active_sandbox_id="sb349uif",
        dry_run=False,
        options={"run_id": "r349uif", "strategy_line": "without_micro", "symbol": "UIF349USDT", "pipeline_mode": "sandbox_full_pipeline"},
    )
    ui_full = run_sandbox_full_pipeline(
        tmp_path,
        sandbox_id="sb349uif",
        run_id="r349uif",
        cycle_id="cycle_r349uif",
        writer_context=ui_context["writer_context"],
        options={"strategy_line": "without_micro", "symbol": "UIF349USDT"},
    )
    assert ui_full["status"] == "completed"
    assert [stage["stage_name"] for stage in ui_full["stages"]] == ["snapshot", "micro", "strategy", "paper"]
    assert ui_full["trade_quality_completion"]["trade_quality_completion_status"] == "ok"
    _assert_training_dataset(
        tmp_path,
        ui_full,
        source_mode="ui_sandbox_full_pipeline",
        run_id="r349uif",
        sandbox_id="sb349uif",
        resource_lane="ui_active_sandbox_real_pipeline",
    )
    assert (tmp_path / ui_full["artifact_manifest_path"]).exists()
    assert (tmp_path / ui_full["training_dataset"]["training_mirror_manifest_path"]).exists()

    rc = cli_main(
        [
            "sandbox",
            "--project-root",
            str(tmp_path),
            "--external",
            "full-pipeline-run",
            "--sandbox-id",
            "sb349clif",
            "--run-id",
            "r349clif",
            "--strategy-line",
            "without_micro",
            "--symbol",
            "CLIF349USDT",
        ]
    )
    cli_full = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert cli_full["ok"] is True
    cli_result = cli_full["data"]["execution_result"]
    assert cli_result["status"] == "completed"
    assert cli_full["data"]["resource_lane"]["resource_lane"] == "external_cli_research_lane"
    _assert_training_dataset(
        tmp_path,
        cli_result,
        source_mode="external_cli_sandbox_full_pipeline",
        run_id="r349clif",
        sandbox_id="sb349clif",
        resource_lane="external_cli_research_lane",
    )

    sandbox = create_sandbox_payload(
        strategy_line="without_micro",
        strategy_version="step34_9",
        root=tmp_path / "DATA" / "sandboxes",
        operation_context={"source_surface": "pytest", "caller_type": "step34_9_audit"},
    )
    sandbox_id = sandbox["sandbox"]["sandbox_id"]
    rc = cli_main(
        [
            "sandbox",
            "--project-root",
            str(tmp_path),
            "--external",
            "job",
            "--sandbox-id",
            sandbox_id,
            "--job-type",
            "trade_quality",
            "--strategy-line",
            "without_micro",
            "--options-json",
            json.dumps({"pytest": True, "symbols": ["BTCAUDITUSDT"]}),
        ]
    )
    cli_job = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert cli_job["ok"] is True
    job = cli_job["data"]["job"]
    assert job["status"] == "completed"
    assert job["job_type"] == "trade_quality"
    training = job["training_dataset"]
    assert training["source_mode"] == "sandbox_backtest"
    assert training["training_dataset_source_mode"] == "sandbox_backtest"
    assert training["sandbox_id"] == sandbox_id
    assert training["source_table"] == "sandbox_orders"
    assert training["samples_written"] >= 1
    assert (tmp_path / training["training_dataset_manifest_path"]).exists()
    assert (tmp_path / training["raw_source_refs_path"]).exists()

    assert not (tmp_path / "DATA" / "micro" / "latest_micro_state.json").exists()
    assert not (tmp_path / "DATA" / "decisions" / "latest_trade_plan_without_micro.json").exists()
    assert not (tmp_path / "DATA" / "paper" / "paper_trading.db").exists()
    modes = _sidecar_source_modes(tmp_path)
    assert {"ui_sandbox_full_pipeline", "external_cli_sandbox_full_pipeline", "sandbox_backtest"}.issubset(modes)
    _assert_no_post_trade_leakage(tmp_path)
