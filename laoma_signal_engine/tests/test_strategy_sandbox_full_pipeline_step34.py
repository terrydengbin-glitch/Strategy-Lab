from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import laoma_signal_engine.api.services as api_services
from laoma_signal_engine.api.app import app
from laoma_signal_engine.cli import main as cli_main
from laoma_signal_engine.strategy_sandbox.full_pipeline import run_sandbox_full_pipeline
from laoma_signal_engine.strategy_sandbox.resource_governor import (
    resource_run_payload,
    start_ui_sandbox_pipeline_context,
)
from laoma_signal_engine.strategy_sandbox.writer_context import SandboxWriterContextError


def _context(tmp_path: Path, *, run_id: str = "run_step34", line: str = "without_micro") -> dict:
    run = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_step34",
        active_sandbox_id="sb_step34",
        dry_run=False,
        options={"run_id": run_id, "strategy_line": line, "symbol": "OPGUSDT", "pipeline_mode": "sandbox_full_pipeline"},
    )
    assert run["accepted"] is True
    return run


def test_step341_full_pipeline_requires_explicit_writer_context(tmp_path: Path) -> None:
    with pytest.raises(SandboxWriterContextError) as exc:
        run_sandbox_full_pipeline(
            tmp_path,
            sandbox_id="sb_missing",
            run_id="run_missing",
            cycle_id="cycle_missing",
            writer_context={},
            options={"strategy_line": "without_micro"},
        )
    assert "sandbox_writer_context_required:sandbox_full_pipeline" in str(exc.value)


def test_step342_to_345_full_pipeline_writes_sandbox_artifacts_and_paper_tq(tmp_path: Path) -> None:
    run = _context(tmp_path, run_id="run_full_chain")
    result = run_sandbox_full_pipeline(
        tmp_path,
        sandbox_id="sb_step34",
        run_id="run_full_chain",
        cycle_id="cycle_run_full_chain",
        writer_context=run["writer_context"],
        options={"strategy_line": "without_micro", "symbol": "OPGUSDT"},
    )

    assert result["execution_contract"] == "sandbox_full_pipeline_explicit_context"
    assert result["pipeline_mode"] == "sandbox_full_pipeline"
    assert result["status"] == "completed"
    assert result["main_chain_mutation_allowed"] is False
    stage_names = [stage["stage_name"] for stage in result["stages"]]
    assert stage_names == ["snapshot", "micro", "strategy", "paper"]
    assert result["paper_db_path"].endswith("paper/paper_trading.db")
    assert result["trade_quality_completion"]["trade_quality_completion_status"] == "ok"
    assert result["training_dataset"]["source_mode"] == "ui_sandbox_full_pipeline"
    assert result["training_dataset"]["training_export_dir"].endswith("DATA/research/trade_snapshots/sandbox_exports/sb_step34/run_full_chain")
    assert result["training_dataset"]["training_mirror_manifest_path"].endswith("DATA/sandboxes/sb_step34/runtime/pipeline_runs/run_full_chain/training/dataset_manifest.json")
    assert (tmp_path / result["artifact_manifest_path"]).exists()
    assert (tmp_path / result["paper_db_path"]).exists()
    assert (tmp_path / result["training_dataset"]["training_dataset_manifest_path"]).exists()
    assert (tmp_path / result["training_dataset"]["training_mirror_manifest_path"]).exists()
    assert not (tmp_path / "DATA" / "market" / "futures_light_snapshot.json").exists()
    assert not (tmp_path / "DATA" / "micro" / "latest_micro_state.json").exists()
    assert not (tmp_path / "DATA" / "decisions" / "latest_trade_plan_without_micro.json").exists()
    assert not (tmp_path / "DATA" / "paper" / "paper_trading.db").exists()


def test_step346_api_full_pipeline_runner(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(api_services, "sandbox_active_payload", lambda: {"active_sandbox_id": "sb_api34"})
    client = TestClient(app)

    response = client.post(
        "/api/strategy-sandbox/pipeline/full-run",
        json={
            "sandbox_id": "sb_api34",
            "dry_run": False,
            "cache_hit": True,
            "options": {"run_id": "run_api34", "strategy_line": "without_micro", "symbol": "OPGUSDT"},
        },
    )
    body = response.json()

    assert response.status_code == 200
    assert body["ok"] is True
    data = body["data"]
    assert data["execution_result"]["pipeline_mode"] == "sandbox_full_pipeline"
    assert data["execution_result"]["status"] == "completed"
    detail = resource_run_payload(tmp_path, run_id="run_api34")
    assert detail["latest"]["event_type"] == "run_finished"
    assert detail["latest"]["result"]["artifact_manifest_path"].endswith("artifact_manifest.json")


def test_step346_cli_external_full_pipeline_runner(tmp_path: Path, capsys) -> None:
    rc = cli_main(
        [
            "sandbox",
            "--project-root",
            str(tmp_path),
            "--external",
            "full-pipeline-run",
            "--sandbox-id",
            "sb_cli34",
            "--run-id",
            "run_cli34",
            "--strategy-line",
            "without_micro",
            "--symbol",
            "OPGUSDT",
        ]
    )
    out = capsys.readouterr().out
    payload = json.loads(out)

    assert rc == 0
    assert payload["ok"] is True
    assert payload["data"]["execution_result"]["pipeline_mode"] == "sandbox_full_pipeline"
    assert payload["data"]["execution_result"]["status"] == "completed"
    assert payload["data"]["resource_lane"]["resource_lane"] == "external_cli_research_lane"
    assert payload["data"]["execution_result"]["training_dataset"]["source_mode"] == "external_cli_sandbox_full_pipeline"
    assert payload["data"]["execution_result"]["training_dataset"]["training_export_dir"].endswith("DATA/research/trade_snapshots/sandbox_exports/sb_cli34/run_cli34")
    assert (tmp_path / payload["data"]["execution_result"]["artifact_manifest_path"]).exists()
    assert (tmp_path / payload["data"]["execution_result"]["training_dataset"]["training_dataset_manifest_path"]).exists()
