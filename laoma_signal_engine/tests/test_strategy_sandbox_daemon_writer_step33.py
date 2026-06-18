from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import laoma_signal_engine.api.services as api_services
from laoma_signal_engine.api.app import app
from laoma_signal_engine.cli import main as cli_main
from laoma_signal_engine.strategy_sandbox.daemon_writer import (
    daemon_writer_status_payload,
    micro_writer_targets,
    paper_daemon_config,
    resolve_daemon_writer_target,
    snapshot_writer_targets,
    strategy_writer_targets,
    write_daemon_writer_inventory,
    write_daemon_writer_inventory_report,
)
from laoma_signal_engine.strategy_sandbox.resource_governor import start_ui_sandbox_pipeline_context
from laoma_signal_engine.strategy_sandbox.writer_context import SandboxWriterContextError


def _writer_context(tmp_path: Path, *, run_id: str = "run_step33") -> dict:
    run = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_step33",
        active_sandbox_id="sb_step33",
        dry_run=False,
        options={"run_id": run_id, "strategy_line": "strategy6", "symbol": "BTCUSDT"},
    )
    return run["writer_context"]


def test_step331_inventory_has_required_fields_and_report(tmp_path: Path) -> None:
    payload = write_daemon_writer_inventory(tmp_path)
    report = write_daemon_writer_inventory_report(tmp_path, suffix="pytest")

    required = {
        "component",
        "function",
        "path_or_db",
        "main_chain_mutation",
        "sandbox_migration_status",
        "risk_level",
        "migration_task",
    }
    assert payload["count"] >= 8
    assert all(required.issubset(row) for row in payload["writers"])
    assert (tmp_path / payload["inventory_path"]).exists()
    assert (tmp_path / report["report_path"]).exists()


def test_step332_context_required_for_sandbox_and_production_unchanged(tmp_path: Path) -> None:
    with pytest.raises(SandboxWriterContextError) as exc:
        resolve_daemon_writer_target(
            tmp_path,
            component="micro",
            logical_name="latest_state",
            production_path=Path("DATA") / "micro" / "latest_micro_state.json",
            sandbox_required=True,
        )
    assert "sandbox_writer_context_required:micro:latest_state" in str(exc.value)

    prod = resolve_daemon_writer_target(
        tmp_path,
        component="micro",
        logical_name="latest_state",
        production_path=Path("DATA") / "micro" / "latest_micro_state.json",
    ).to_payload(tmp_path)
    assert prod["mode"] == "production_default_lane"
    assert prod["main_chain_mutation_allowed"] is True
    assert prod["path"] == "DATA/micro/latest_micro_state.json"


def test_step336_paper_daemon_config_uses_sandbox_paths(tmp_path: Path) -> None:
    ctx = _writer_context(tmp_path, run_id="run_paper")
    cfg = paper_daemon_config(tmp_path, writer_context=ctx)

    assert cfg.db_path.endswith("DATA/sandboxes/sb_step33/runtime/pipeline_runs/run_paper/paper/paper_trading.db")
    assert cfg.summary_path.endswith("DATA/sandboxes/sb_step33/runtime/pipeline_runs/run_paper/paper/latest_paper_state.json")
    assert cfg.daemon_status_path.endswith("DATA/sandboxes/sb_step33/runtime/pipeline_runs/run_paper/paper/paper_daemon_status.json")


def test_step333_snapshot_targets_are_sandbox_scoped_but_rest_budget_shared(tmp_path: Path) -> None:
    targets = snapshot_writer_targets(tmp_path, writer_context=_writer_context(tmp_path, run_id="run_snapshot"))

    assert targets["latest_snapshot"]["mode"] == "sandbox_scoped_daemon_writer"
    assert targets["latest_snapshot"]["main_chain_mutation_allowed"] is False
    assert targets["latest_snapshot"]["path"].endswith(
        "DATA/sandboxes/sb_step33/runtime/pipeline_runs/run_snapshot/daemon_outputs/snapshot/futures_light_snapshot.json"
    )
    assert targets["rest_circuit"]["mode"] == "shared_governance_observed"
    assert targets["rest_circuit"]["path"] == "DATA/runtime/rest_circuit.json"


def test_step334_micro_targets_are_sandbox_scoped(tmp_path: Path) -> None:
    targets = micro_writer_targets(tmp_path, writer_context=_writer_context(tmp_path, run_id="run_micro"))

    assert targets["latest_state"]["mode"] == "sandbox_scoped_daemon_writer"
    assert targets["latest_state"]["path"].endswith(
        "DATA/sandboxes/sb_step33/runtime/pipeline_runs/run_micro/daemon_outputs/micro/latest_micro_state.json"
    )
    assert targets["latest_features"]["main_chain_mutation_allowed"] is False


def test_step335_strategy456_targets_are_sandbox_scoped(tmp_path: Path) -> None:
    ctx = _writer_context(tmp_path, run_id="run_strategy")
    for strategy in ("strategy4", "strategy5", "strategy6"):
        targets = strategy_writer_targets(tmp_path, strategy_id=strategy, writer_context=ctx)
        assert targets["latest_trade_plan"]["mode"] == "sandbox_scoped_daemon_writer"
        assert targets["latest_trade_plan"]["path"].endswith(
            f"DATA/sandboxes/sb_step33/runtime/pipeline_runs/run_strategy/daemon_outputs/{strategy}/latest_trade_plan_{strategy}.json"
        )
        assert targets["evidence"]["main_chain_mutation_allowed"] is False


def test_step337_status_payload_exposes_observer_modes(tmp_path: Path) -> None:
    payload = daemon_writer_status_payload(tmp_path, writer_context=_writer_context(tmp_path, run_id="run_observer"))

    assert payload["inventory_count"] >= 8
    assert "ui_active_sandbox_real_pipeline" in payload["observer_modes"]
    assert "external_cli_research_lane" in payload["observer_modes"]
    assert payload["adapters"]["paper"]["mode"] == "sandbox_scoped_daemon_writer"
    assert payload["adapters"]["strategy6"]["latest_trade_plan"]["path"].endswith("latest_trade_plan_strategy6.json")


def test_step337_api_and_cli_observer_smoke(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    _writer_context(tmp_path, run_id="run_observe_surface")

    client = TestClient(app)
    response = client.get("/api/strategy-sandbox/daemon-writers/status?run_id=run_observe_surface")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["data"]["adapters"]["paper"]["mode"] == "sandbox_scoped_daemon_writer"

    rc = cli_main(["sandbox", "--project-root", str(tmp_path), "daemon-writer-status", "--run-id", "run_observe_surface"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "sandbox_scoped_daemon_writer" in out
