from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from laoma_signal_engine.strategy_sandbox.resource_governor import (
    EXTERNAL_CLI_RESEARCH_LANE,
    UI_ACTIVE_SANDBOX_LANE,
    resource_run_payload,
    start_external_research_context,
    start_ui_sandbox_pipeline_context,
)
from laoma_signal_engine.strategy_sandbox.writer_context import (
    SandboxWriterContextError,
    create_writer_context,
    fallback_sandbox_db_path,
    validate_writer_context_payload,
)


def test_step319_writer_context_requires_identity_fields() -> None:
    with pytest.raises(SandboxWriterContextError) as exc:
        validate_writer_context_payload(
            {
                "sandbox_id": "sb_a",
                "resource_lane": UI_ACTIVE_SANDBOX_LANE,
                "run_id": "",
                "cycle_id": "cycle_a",
                "source_chain": "ui_active_sandbox_real_pipeline",
            }
        )

    assert "missing_required_writer_context_fields:run_id" in str(exc.value)


def test_step319_writer_context_rejects_main_chain_target() -> None:
    with pytest.raises(SandboxWriterContextError) as exc:
        validate_writer_context_payload(
            {
                "sandbox_id": "sb_a",
                "resource_lane": UI_ACTIVE_SANDBOX_LANE,
                "run_id": "run_a",
                "cycle_id": "cycle_a",
                "source_chain": "ui_active_sandbox_real_pipeline",
                "writer_target": "main_paper_ledger",
            }
        )

    assert "main_chain_writer_target_denied:main_paper_ledger" in str(exc.value)


def test_step319_create_writer_context_persists_sandbox_ledger(tmp_path: Path) -> None:
    record = create_writer_context(
        tmp_path,
        sandbox_id="sb_contract",
        resource_lane=UI_ACTIVE_SANDBOX_LANE,
        run_id="run_contract",
        cycle_id="cycle_contract",
        source_chain="ui_active_sandbox_real_pipeline",
        strategy_line="strategy6",
        symbol="BTCUSDT",
    )
    db_path = fallback_sandbox_db_path(tmp_path, "sb_contract")

    assert record["context"]["sandbox_id"] == "sb_contract"
    assert record["writer_targets"]["main_chain_mutation_allowed"] is False
    assert db_path.exists()
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT context_json, target_paths_json, main_chain_mutation_allowed
            FROM sandbox_pipeline_writer_contexts
            WHERE sandbox_id=? AND run_id=?
            """,
            ("sb_contract", "run_contract"),
        ).fetchone()

    assert row is not None
    context = json.loads(row[0])
    targets = json.loads(row[1])
    assert context["resource_lane"] == UI_ACTIVE_SANDBOX_LANE
    assert context["strategy_line"] == "strategy6"
    assert targets["sandbox_db_path"].endswith("sandbox.db")
    assert row[2] == 0


def test_step319_ui_pipeline_run_records_writer_target(tmp_path: Path) -> None:
    run = start_ui_sandbox_pipeline_context(
        project_root=tmp_path,
        sandbox_id="sb_ui",
        active_sandbox_id="sb_ui",
        dry_run=False,
        options={"run_id": "ui_writer_run", "strategy_line": "strategy5", "symbol": "ETHUSDT"},
    )
    detail = resource_run_payload(tmp_path, run_id="ui_writer_run")
    db_path = fallback_sandbox_db_path(tmp_path, "sb_ui")

    assert run["accepted"] is True
    assert run["status"] == "sandbox_writer_guard_ready"
    assert run["writer_context"]["context"]["run_id"] == "ui_writer_run"
    assert run["writer_target"]["main_chain_mutation_allowed"] is False
    assert detail["latest"]["writer_target"]["sandbox_db_path"].endswith("sandbox.db")
    with sqlite3.connect(db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM sandbox_pipeline_writer_contexts WHERE run_id=?",
            ("ui_writer_run",),
        ).fetchone()[0]
    assert count == 1


def test_step319_external_lane_records_independent_writer_target(tmp_path: Path) -> None:
    run = start_external_research_context(
        project_root=tmp_path,
        sandbox_id="sb_cli",
        run_id="cli_writer_run",
        options={"strategy_line": "strategy4", "symbol": "SOLUSDT"},
    )

    assert run["accepted"] is True
    assert run["resource_lane"] == EXTERNAL_CLI_RESEARCH_LANE
    assert run["writer_context"]["context"]["resource_lane"] == EXTERNAL_CLI_RESEARCH_LANE
    assert run["writer_target"]["main_chain_mutation_allowed"] is False
