from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from laoma_signal_engine.api.app import app
from laoma_signal_engine.cli import main as cli_main
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.market.rest_circuit import write_rest_circuit_open
from laoma_signal_engine.strategy_sandbox.resource_governor import (
    EXTERNAL_CLI_RESEARCH_LANE,
    UI_ACTIVE_SANDBOX_LANE,
    acquire_lane,
    finish_external_research_context,
    governor_status,
    lane_lock_path,
    release_lane,
    resource_run_payload,
    resource_runs_payload,
    rest_budget_snapshot,
    start_external_research_context,
    start_ui_sandbox_pipeline_context,
    stop_ui_sandbox_pipeline_context,
)


def test_step311_resource_governor_rejects_same_lane_reentry(tmp_path: Path) -> None:
    first = acquire_lane(
        UI_ACTIVE_SANDBOX_LANE,
        project_root=tmp_path,
        sandbox_id="sb_ui",
        run_id="run_a",
        active_context_at_start="sb_ui",
    )
    assert first["acquired"] is True

    second = acquire_lane(
        UI_ACTIVE_SANDBOX_LANE,
        project_root=tmp_path,
        sandbox_id="sb_ui",
        run_id="run_b",
        active_context_at_start="sb_ui",
    )
    assert second["acquired"] is False
    assert second["reason_code"] == "resource_lane_already_running"


def test_step311_resource_governor_allows_two_declared_lanes(tmp_path: Path) -> None:
    ui = acquire_lane(
        UI_ACTIVE_SANDBOX_LANE,
        project_root=tmp_path,
        sandbox_id="sb_ui",
        run_id="run_ui",
        active_context_at_start="sb_ui",
    )
    external = acquire_lane(
        EXTERNAL_CLI_RESEARCH_LANE,
        project_root=tmp_path,
        sandbox_id="sb_cli",
        run_id="run_cli",
        caller_surface="cli",
        caller_type="external_cli",
    )
    status = governor_status(tmp_path)

    assert ui["acquired"] is True
    assert external["acquired"] is True
    assert status["lanes"][UI_ACTIVE_SANDBOX_LANE]["status"] == "running"
    assert status["lanes"][EXTERNAL_CLI_RESEARCH_LANE]["status"] == "running"


def test_step311_resource_governor_recovers_stale_lane_lock(tmp_path: Path) -> None:
    stale_path = lane_lock_path(UI_ACTIVE_SANDBOX_LANE, tmp_path)
    stale_path.parent.mkdir(parents=True, exist_ok=True)
    stale_path.write_text(
        (
            "{\n"
            f'  "resource_lane": "{UI_ACTIVE_SANDBOX_LANE}",\n'
            '  "owner_pid": 99999999,\n'
            '  "run_id": "old_run",\n'
            f'  "expires_at": "{to_iso_z(utc_now() - timedelta(seconds=60))}"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    acquired = acquire_lane(
        UI_ACTIVE_SANDBOX_LANE,
        project_root=tmp_path,
        sandbox_id="sb_ui",
        run_id="new_run",
        active_context_at_start="sb_ui",
    )

    assert acquired["acquired"] is True
    assert acquired["recovered_stale_lock"] is True
    assert acquired["lock"]["run_id"] == "new_run"


def test_step311_resource_governor_blocks_live_rest_when_circuit_open(tmp_path: Path) -> None:
    write_rest_circuit_open(
        tmp_path,
        status_code=429,
        endpoint="/fapi/v1/klines",
        source_stage="pytest",
        retry_after_sec=180,
    )

    budget = rest_budget_snapshot(tmp_path, requires_live_rest=True)
    acquired = acquire_lane(
        EXTERNAL_CLI_RESEARCH_LANE,
        project_root=tmp_path,
        sandbox_id="sb_cli",
        run_id="run_cli",
        caller_surface="cli",
        caller_type="external_cli",
        requires_live_rest=True,
    )

    assert budget["rest_circuit_state"] == "open"
    assert budget["live_rest_available"] is False
    assert acquired["acquired"] is False
    assert acquired["reason_code"] == "rest_circuit_live_rest_unavailable"


def test_step311_release_lane_removes_lock(tmp_path: Path) -> None:
    acquired = acquire_lane(
        EXTERNAL_CLI_RESEARCH_LANE,
        project_root=tmp_path,
        sandbox_id="sb_cli",
        run_id="run_cli",
        caller_surface="cli",
        caller_type="external_cli",
    )
    assert acquired["acquired"] is True

    released = release_lane(EXTERNAL_CLI_RESEARCH_LANE, project_root=tmp_path, run_id="run_cli")
    status = governor_status(tmp_path)

    assert released["released"] is True
    assert status["lanes"][EXTERNAL_CLI_RESEARCH_LANE]["status"] == "idle"


def test_step311_resource_governor_status_readable_from_api_and_cli(tmp_path: Path) -> None:
    client = TestClient(app)
    api_resp = client.get("/api/strategy-sandbox/resource-governor/status")
    assert api_resp.status_code == 200
    api_data = api_resp.json()["data"]
    assert api_data["source"] == "sandbox_resource_governor"
    assert UI_ACTIVE_SANDBOX_LANE in api_data["lanes"]
    assert EXTERNAL_CLI_RESEARCH_LANE in api_data["lanes"]

    rc = cli_main(["sandbox", "--sandbox-root", str(tmp_path), "resource-status"])
    assert rc == 0


def test_step312_active_sandbox_pipeline_requires_active_context(tmp_path: Path) -> None:
    missing = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_a",
        active_sandbox_id=None,
    )
    mismatch = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_b",
        active_sandbox_id="sb_a",
    )
    accepted = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_a",
        active_sandbox_id="sb_a",
        options={"run_id": "ui_run_a"},
    )
    run = resource_run_payload(tmp_path, run_id="ui_run_a")

    assert missing["accepted"] is False
    assert missing["reason_code"] == "active_sandbox_context_required"
    assert mismatch["accepted"] is False
    assert mismatch["reason_code"] == "active_sandbox_mismatch"
    assert accepted["accepted"] is True
    assert accepted["resource_lane"] == UI_ACTIVE_SANDBOX_LANE
    assert accepted["training_readiness"]["allowed_for_training"] is False
    assert run["latest"]["sandbox_id"] == "sb_a"
    assert run["latest"]["resource_lane"] == UI_ACTIVE_SANDBOX_LANE


def test_step313_switch_cancel_clears_only_ui_lane(tmp_path: Path) -> None:
    ui = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_a",
        active_sandbox_id="sb_a",
        options={"run_id": "ui_run_switch"},
    )
    external = start_external_research_context(
        project_root=tmp_path,
        sandbox_id="sb_cli",
        run_id="cli_run_keep",
    )
    canceled = stop_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        run_id="ui_run_switch",
        cancel_reason="active_sandbox_switch",
    )
    status = governor_status(tmp_path)

    assert ui["accepted"] is True
    assert external["accepted"] is True
    assert canceled["canceled"] is True
    assert status["lanes"][UI_ACTIVE_SANDBOX_LANE]["status"] == "idle"
    assert status["lanes"][EXTERNAL_CLI_RESEARCH_LANE]["status"] == "running"


def test_step314_external_cli_research_lane_parallel_and_reentry(tmp_path: Path) -> None:
    ui = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_a",
        active_sandbox_id="sb_a",
    )
    external = start_external_research_context(
        project_root=tmp_path,
        sandbox_id="sb_cli",
        run_id="cli_run_a",
        caller_surface="cli",
        caller_type="external_cli",
    )
    blocked = start_external_research_context(
        project_root=tmp_path,
        sandbox_id="sb_cli",
        run_id="cli_run_b",
        caller_surface="cli",
        caller_type="external_cli",
    )
    finish = finish_external_research_context(
        project_root=tmp_path,
        run_id="cli_run_a",
        sandbox_id="sb_cli",
        result={"ok": True},
    )

    assert ui["accepted"] is True
    assert external["accepted"] is True
    assert blocked["accepted"] is False
    assert blocked["lane"]["reason_code"] == "resource_lane_already_running"
    assert finish["released"] is True


def test_step315_resource_runs_observer_lists_external_progress(tmp_path: Path) -> None:
    started = start_external_research_context(
        project_root=tmp_path,
        sandbox_id="sb_cli",
        run_id="cli_observe",
    )
    finish_external_research_context(
        project_root=tmp_path,
        run_id="cli_observe",
        sandbox_id="sb_cli",
        status="completed",
        result={"job_type": "paper_shadow"},
    )

    runs = resource_runs_payload(tmp_path, resource_lane=EXTERNAL_CLI_RESEARCH_LANE, sandbox_id="sb_cli")
    detail = resource_run_payload(tmp_path, run_id="cli_observe")

    assert started["accepted"] is True
    assert runs["count"] >= 2
    assert detail["count"] == 2
    assert detail["latest"]["status"] == "completed"


def test_step316_rest_budget_cache_hit_allowed_when_circuit_open(tmp_path: Path) -> None:
    write_rest_circuit_open(
        tmp_path,
        status_code=429,
        endpoint="/fapi/v1/klines",
        source_stage="pytest",
        retry_after_sec=180,
    )
    blocked = start_external_research_context(
        project_root=tmp_path,
        sandbox_id="sb_cli",
        run_id="cli_live_miss",
        requires_live_rest=True,
        cache_hit=False,
    )
    allowed = start_external_research_context(
        project_root=tmp_path,
        sandbox_id="sb_cli",
        run_id="cli_cache_hit",
        requires_live_rest=True,
        cache_hit=True,
    )

    assert blocked["accepted"] is False
    assert blocked["lane"]["reason_code"] == "rest_circuit_live_rest_unavailable"
    assert allowed["accepted"] is True
    assert allowed["lane"]["rest_budget"]["cache_hit_bypasses_live_rest_budget"] is True


def test_step317_training_readiness_requires_tq_and_sidecar(tmp_path: Path) -> None:
    run = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_train",
        active_sandbox_id="sb_train",
        options={"run_id": "ui_training"},
    )
    readiness = run["training_readiness"]

    assert readiness["training_dataset_status"] == "incomplete"
    assert readiness["allowed_for_training"] is False
    assert readiness["trade_quality_label_source"] == "trade_quality_module_required"
    assert "trade_quality_completion_required" in readiness["reason_codes"]
