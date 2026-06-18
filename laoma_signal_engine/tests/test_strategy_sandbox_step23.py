from __future__ import annotations

import json
import sqlite3

from laoma_signal_engine.cli import main as cli_main
from laoma_signal_engine.strategy_sandbox.service import (
    add_code_patch_payload,
    active_sandbox_payload,
    build_runtime_payload,
    branches_payload,
    code_overlay_payload,
    create_sandbox_payload,
    create_code_overlay_payload,
    create_full_backtest_run_payload,
    delete_sandbox_payload,
    db_health_payload,
    external_integration_audit_events_payload,
    external_integration_health_payload,
    external_integration_run_payload,
    full_backtest_run_payload,
    gated_orders_payload,
    gated_paper_shadow_payload,
    gated_performance_payload,
    gated_replay_payload,
    gated_trade_quality_samples_payload,
    get_sandbox_payload,
    ingest_gate_action_payload,
    job_payload,
    gate_compare_payload,
    leaderboard_payload,
    list_sandboxes_payload,
    cancel_full_backtest_run_payload,
    resume_full_backtest_run_payload,
    set_active_sandbox_payload,
    summary_payload,
    runtime_smoke_payload,
    trade_quality_compare_payload,
    trade_candidates_payload,
    universe_payload,
)


def _json_stdout(capsys) -> dict:
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


def test_step301_sandbox_cli_management_gateway_defaults(tmp_path, capsys):
    root = tmp_path / "sandboxes"
    rc = cli_main(
        [
            "sandbox",
            "--sandbox-root",
            str(root),
            "create",
            "--strategy-line",
            "strategy6",
            "--strategy-version",
            "cli-test",
            "--data-scope-json",
            '{"days":1,"symbols":["BTCUSDT"]}',
            "--config-scope-json",
            '{"mode":"shadow_only"}',
            "--tags",
            "pytest,cli",
        ]
    )
    assert rc == 0
    payload = _json_stdout(capsys)
    sandbox_id = payload["data"]["sandbox"]["sandbox_id"]
    assert payload["data"]["source_surface"] == "cli"
    assert payload["data"]["active_changed"] is False
    assert active_sandbox_payload(root=root)["active"] is None

    with sqlite3.connect(payload["data"]["sandbox"]["db_path"]) as conn:
        audit_count = conn.execute(
            "SELECT COUNT(*) FROM external_integration_audit_events WHERE event_type='sandbox_management_create'"
        ).fetchone()[0]
    assert audit_count == 1

    rc = cli_main(["sandbox", "--sandbox-root", str(root), "set-active", "--sandbox-id", sandbox_id])
    assert rc == 0
    active = _json_stdout(capsys)
    assert active["data"]["active_sandbox_id"] == sandbox_id
    assert active["data"]["active_changed"] is True

    rc = cli_main(["sandbox", "--sandbox-root", str(root), "delete", "--sandbox-id", sandbox_id, "--reason", "pytest"])
    assert rc == 0
    deleted = _json_stdout(capsys)
    assert deleted["data"]["status"] == "deleted"
    assert deleted["data"]["mode"] == "soft_delete"
    assert deleted["data"]["active_changed"] is True
    assert active_sandbox_payload(root=root)["active"] is None


def test_step305_external_cli_boundary_rejects_context_write_by_default(tmp_path, capsys):
    root = tmp_path / "sandboxes"
    rc = cli_main(
        [
            "sandbox",
            "--sandbox-root",
            str(root),
            "--external",
            "create",
            "--strategy-line",
            "strategy6",
            "--strategy-version",
            "external-cli-denied",
            "--set-active",
        ]
    )
    assert rc != 0
    denied = _json_stdout(capsys)
    assert denied["ok"] is False
    assert "external_active_context_write_denied" in denied["error"]
    assert list_sandboxes_payload(root=root)["count"] == 0

    rc = cli_main(
        [
            "sandbox",
            "--sandbox-root",
            str(root),
            "--external",
            "--caller-id",
            "external-cli-smoke",
            "create",
            "--strategy-line",
            "strategy6",
            "--strategy-version",
            "external-cli-ok",
        ]
    )
    assert rc == 0
    created = _json_stdout(capsys)
    sandbox_id = created["data"]["sandbox"]["sandbox_id"]
    assert created["data"]["source_surface"] == "external_connector"
    assert created["data"]["caller_type"] == "external_ai_trader"
    assert created["data"]["caller_id"] == "external-cli-smoke"
    assert created["data"]["active_changed"] is False
    assert active_sandbox_payload(root=root)["active"] is None

    rc = cli_main(["sandbox", "--sandbox-root", str(root), "--external", "set-active", "--sandbox-id", sandbox_id])
    assert rc != 0
    denied_switch = _json_stdout(capsys)
    assert denied_switch["ok"] is False
    assert "external_active_context_write_denied" in denied_switch["error"]
    assert active_sandbox_payload(root=root)["active"] is None

    rc = cli_main(["sandbox", "--sandbox-root", str(root), "set-active", "--sandbox-id", sandbox_id])
    assert rc == 0
    active = _json_stdout(capsys)
    assert active["data"]["active_changed"] is True


def test_step304_sandbox_delete_purge_path_and_running_job_guards(tmp_path):
    created = create_sandbox_payload(
        strategy_line="strategy6",
        strategy_version="delete-guard-test",
        data_scope={"days": 1, "symbols": ["BTCUSDT"]},
        config_scope={"mode": "shadow_only"},
        tags=["pytest", "delete-guard"],
        root=tmp_path,
    )
    sandbox_id = created["sandbox"]["sandbox_id"]
    db_path = created["sandbox"]["db_path"]

    try:
        delete_sandbox_payload(sandbox_id, mode="purge", reason="pytest", root=tmp_path)
    except ValueError as exc:
        assert "purge_requires_confirm_true" in str(exc)
    else:
        raise AssertionError("purge without confirm should be rejected")

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO sandbox_jobs(job_id, sandbox_id, job_type, status, progress_json, result_json, created_at, updated_at)
            VALUES('job_running_guard', ?, 'backtest', 'running', '{}', '{}', 'now', 'now')
            """,
            (sandbox_id,),
        )
        conn.commit()
    try:
        delete_sandbox_payload(sandbox_id, mode="soft_delete", reason="pytest", root=tmp_path)
    except ValueError as exc:
        assert "sandbox_has_running_job" in str(exc)
    else:
        raise AssertionError("running sandbox delete should be rejected")
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM sandbox_jobs WHERE job_id='job_running_guard'")
        conn.commit()

    registry_path = tmp_path / "sandbox_registry.db"
    with sqlite3.connect(registry_path) as conn:
        conn.execute("UPDATE sandbox_registry SET root_path=? WHERE sandbox_id=?", (str(tmp_path.parent / "outside" / sandbox_id), sandbox_id))
        conn.commit()
    try:
        delete_sandbox_payload(sandbox_id, mode="purge", reason="pytest", confirm=True, root=tmp_path)
    except ValueError as exc:
        assert "purge path outside sandbox root" in str(exc)
    else:
        raise AssertionError("purge outside sandbox root should be rejected")
    with sqlite3.connect(registry_path) as conn:
        status = conn.execute("SELECT status FROM sandbox_registry WHERE sandbox_id=?", (sandbox_id,)).fetchone()[0]
    assert status == "created"


def test_strategy_sandbox_create_switch_and_contract_jobs(tmp_path):
    created = create_sandbox_payload(
        strategy_line="strategy6",
        strategy_version="test-v1",
        data_scope={"days": 7, "symbols": ["BTCUSDT"]},
        config_scope={"mode": "shadow_only", "target": "pf"},
        tags=["pytest"],
        root=tmp_path,
    )
    sandbox_id = created["sandbox"]["sandbox_id"]
    assert created["sandbox"]["db_path"].endswith("sandbox.db")

    listed = list_sandboxes_payload(root=tmp_path)
    assert listed["count"] == 1
    assert listed["sandboxes"][0]["sandbox_id"] == sandbox_id

    active = set_active_sandbox_payload(sandbox_id, root=tmp_path)
    assert active["active_sandbox_id"] == sandbox_id
    assert active_sandbox_payload(root=tmp_path)["active"]["strategy_line"] == "strategy6"

    for job_type in (
        "backtest",
        "replay",
        "trade_quality",
        "gate-search",
        "holdout",
        "config-export",
        "paper-shadow",
        "llm-export",
    ):
        result = job_payload(sandbox_id, job_type, {"pytest": True}, root=tmp_path)
        assert result["status"] == "completed"

    summary = summary_payload(sandbox_id, root=tmp_path)["summary"]
    assert summary["counts"]["sandbox_jobs"] == 8
    assert summary["counts"]["trade_quality_samples"] >= 1
    assert summary["counts"]["gate_candidates"] >= 1
    assert summary["counts"]["llm_dataset_exports"] >= 1

    detail = get_sandbox_payload(sandbox_id, root=tmp_path)
    assert detail["sandbox"]["sandbox_id"] == sandbox_id

    health = db_health_payload(sandbox_id, root=tmp_path)
    assert health["health"]["integrity_check"] == "ok"


def test_step271_external_full_backtest_manifest_and_progress_ledger(tmp_path):
    created = create_sandbox_payload(
        strategy_line="strategy6",
        strategy_version="step27-1-test",
        data_scope={"days": 7, "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]},
        config_scope={"mode": "shadow_only"},
        tags=["pytest", "step27.1"],
        root=tmp_path,
    )
    sandbox_id = created["sandbox"]["sandbox_id"]

    universe = universe_payload("strategy6", sandbox_id=sandbox_id, root=tmp_path)
    assert universe["symbols"] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert universe["data_quality_summary"]["fallback_universe"] is False
    assert universe["universe_hash"]

    run = create_full_backtest_run_payload(
        sandbox_id,
        {
            "strategy_line": "strategy6",
            "symbols": ["BTCUSDT", "SOLUSDT"],
            "time_start": "2026-06-01T00:00:00Z",
            "time_end": "2026-06-02T00:00:00Z",
            "batch_size": 1,
            "idempotency_key": "pytest-step27-1",
            "resource_budget": {"max_workers": 1},
        },
        root=tmp_path,
    )
    run_id = run["run_id"]
    assert run["status"] == "manifest_ready"
    assert run["external_full_backtest_run_id"] == run_id
    assert run["expected_batches"] == 2
    assert run["completed_batches"] == 0
    assert run["failed_batches"] == 0
    assert run["retryable"] is True
    assert run["resume_token"]
    assert run["scope_hash"]
    assert run["universe_hash"]
    assert run["coverage"]["requested_symbols"] == 2

    replay = create_full_backtest_run_payload(
        sandbox_id,
        {"strategy_line": "strategy6", "idempotency_key": "pytest-step27-1"},
        root=tmp_path,
    )
    assert replay["run_id"] == run_id
    assert replay["idempotent_replay"] is True

    fetched = full_backtest_run_payload(sandbox_id, run_id, root=tmp_path)
    assert fetched["batches"][0]["status"] == "queued"

    canceled = cancel_full_backtest_run_payload(sandbox_id, run_id, root=tmp_path)
    assert canceled["status"] == "canceled"
    assert {batch["status"] for batch in canceled["batches"]} == {"canceled"}

    resumed = resume_full_backtest_run_payload(sandbox_id, run_id, root=tmp_path)
    assert resumed["status"] == "manifest_ready"
    assert {batch["status"] for batch in resumed["batches"]} == {"queued"}

    with sqlite3.connect(created["sandbox"]["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        counts = {
            name: conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            for name in (
                "external_full_backtest_runs",
                "external_full_backtest_batches",
                "external_full_backtest_events",
            )
        }
    assert counts["external_full_backtest_runs"] == 1
    assert counts["external_full_backtest_batches"] == 2
    assert counts["external_full_backtest_events"] >= 3


def test_step272_external_trade_candidate_export_and_gate_action_ingestion(tmp_path):
    created = create_sandbox_payload(
        strategy_line="strategy6",
        strategy_version="step27-2-test",
        data_scope={"days": 7, "symbols": ["BTCUSDT", "ETHUSDT"]},
        config_scope={"mode": "shadow_only"},
        tags=["pytest", "step27.2"],
        root=tmp_path,
    )
    sandbox_id = created["sandbox"]["sandbox_id"]
    job = job_payload(sandbox_id, "backtest", {"pytest": True, "symbols": ["BTCUSDT", "ETHUSDT"]}, root=tmp_path)
    assert job["status"] == "completed"

    exported = trade_candidates_payload(sandbox_id, "strategy6", limit=10, root=tmp_path)
    assert exported["count"] >= 1
    candidate = exported["candidates"][0]
    assert candidate["candidate_id"]
    assert candidate["run_id"]
    assert candidate["sandbox_id"] == sandbox_id
    assert candidate["strategy_line"] == "strategy6"
    assert candidate["decision_time_features"]
    assert candidate["leakage_status"] == "pass"
    assert "net_R" not in candidate["decision_time_features"]
    assert "_sandbox_rows" not in candidate["decision_time_features"]

    action = ingest_gate_action_payload(
        sandbox_id,
        "strategy6",
        {
            "run_id": candidate["run_id"],
            "candidate_id": candidate["candidate_id"],
            "unit_id": "unit_pytest",
            "unit_version": "v1",
            "selection_id": "sel_pytest",
            "scorer_output_ref": "ai_trader://scorer/test",
            "final_gate_decision_ref": "ai_trader://final-gate/test",
            "gate_decision": "block",
            "gate_action_payload": {
                "action": "block",
                "threshold_policy_version": "pytest",
                "calibration_status": "ok",
                "bad_trade_risk": 0.91,
                "deterministic": True,
                "final_gate_decision_by_llm": False,
            },
            "reason_codes": ["pytest_high_risk"],
            "audit_trace_id": "trace_pytest",
            "idempotency_key": "pytest-step27-2-action",
        },
        root=tmp_path,
    )
    assert action["accepted"] is True
    assert action["status"] == "accepted"
    assert action["gate_decision"] == "block"
    assert action["applied_policy"]["final_gate_decision_by_llm"] is False

    duplicate = ingest_gate_action_payload(
        sandbox_id,
        "strategy6",
        {
            "run_id": candidate["run_id"],
            "candidate_id": candidate["candidate_id"],
            "unit_id": "unit_pytest",
            "unit_version": "v1",
            "gate_decision": "block",
            "idempotency_key": "pytest-step27-2-action",
        },
        root=tmp_path,
    )
    assert duplicate["accepted"] is True
    assert duplicate["status"] == "duplicate"
    assert duplicate["gate_action_id"] == action["gate_action_id"]

    conflict = ingest_gate_action_payload(
        sandbox_id,
        "strategy6",
        {
            "run_id": candidate["run_id"],
            "candidate_id": candidate["candidate_id"],
            "unit_id": "unit_pytest",
            "unit_version": "v1",
            "gate_decision": "allow",
            "idempotency_key": "pytest-step27-2-action",
        },
        root=tmp_path,
    )
    assert conflict["accepted"] is False
    assert conflict["error_code"] == "idempotency_conflict"

    missing = ingest_gate_action_payload(
        sandbox_id,
        "strategy6",
        {
            "run_id": candidate["run_id"],
            "candidate_id": "missing_candidate",
            "unit_id": "unit_pytest",
            "unit_version": "v1",
            "gate_decision": "review",
            "idempotency_key": "pytest-step27-2-missing",
        },
        root=tmp_path,
    )
    assert missing["accepted"] is False
    assert missing["error_code"] == "candidate_missing"

    with sqlite3.connect(created["sandbox"]["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        counts = {
            name: conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            for name in (
                "external_trade_candidates",
                "external_gate_actions",
                "external_gate_action_events",
            )
        }
    assert counts["external_trade_candidates"] >= 1
    assert counts["external_gate_actions"] == 1
    assert counts["external_gate_action_events"] >= 3


def test_step272_external_trade_candidate_leakage_guard(tmp_path):
    created = create_sandbox_payload(
        strategy_line="strategy6",
        strategy_version="step27-2-leak-test",
        data_scope={"days": 7, "symbols": ["BTCUSDT"]},
        config_scope={"mode": "shadow_only"},
        tags=["pytest", "step27.2"],
        root=tmp_path,
    )
    sandbox_id = created["sandbox"]["sandbox_id"]
    job_payload(sandbox_id, "backtest", {"pytest": True, "symbols": ["BTCUSDT"]}, root=tmp_path)
    with sqlite3.connect(created["sandbox"]["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT order_id, features_json FROM sandbox_orders LIMIT 1").fetchone()
        features = json.loads(row["features_json"])
        features["net_R"] = 1.23
        conn.execute("UPDATE sandbox_orders SET features_json=? WHERE order_id=?", (json.dumps(features), row["order_id"]))
        conn.commit()

    try:
        trade_candidates_payload(sandbox_id, "strategy6", limit=1, root=tmp_path)
    except ValueError as exc:
        assert "feature_leakage_detected" in str(exc)
        assert "net_R" in str(exc)
    else:
        raise AssertionError("feature leakage should be rejected")


def test_step273_274_external_gated_replay_paper_shadow_performance_and_audit(tmp_path):
    created = create_sandbox_payload(
        strategy_line="strategy6",
        strategy_version="step27-3-test",
        data_scope={"days": 7, "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]},
        config_scope={"mode": "shadow_only"},
        tags=["pytest", "step27.3", "step27.4"],
        root=tmp_path,
    )
    sandbox_id = created["sandbox"]["sandbox_id"]
    job = job_payload(sandbox_id, "trade_quality", {"pytest": True, "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]}, root=tmp_path)
    assert job["status"] == "completed"
    exported = trade_candidates_payload(sandbox_id, "strategy6", limit=10, root=tmp_path)
    assert exported["count"] >= 2
    candidates = exported["candidates"][:2]
    decisions = ["block", "reduce_size"]
    for idx, candidate in enumerate(candidates):
        ingest_gate_action_payload(
            sandbox_id,
            "strategy6",
            {
                "run_id": candidate["run_id"],
                "candidate_id": candidate["candidate_id"],
                "unit_id": "unit_pytest",
                "unit_version": "v1",
                "selection_id": "sel_pytest",
                "scorer_output_ref": f"ai_trader://scorer/{idx}",
                "final_gate_decision_ref": f"ai_trader://final-gate/{idx}",
                "gate_decision": decisions[idx],
                "gate_action_payload": {
                    "action": decisions[idx],
                    "original_size": candidate["intended_size"],
                    "size_multiplier": 0.5,
                    "deterministic": True,
                    "final_gate_decision_by_llm": False,
                },
                "reason_codes": [f"pytest_{decisions[idx]}"],
                "audit_trace_id": f"trace_pytest_{idx}",
                "idempotency_key": f"pytest-step27-3-action-{idx}",
            },
            root=tmp_path,
        )
    run_id = candidates[0]["run_id"]
    replay = gated_replay_payload(
        sandbox_id,
        "strategy6",
        {"run_id": run_id, "baseline_run_id": run_id, "execution_policy": {"missing_gate_action_policy": "review"}},
        root=tmp_path,
    )
    assert replay["status"] == "completed"
    assert replay["candidate_count"] >= 2
    assert replay["blocked_count"] == 1
    assert replay["reduced_count"] == 1
    assert replay["order_count"] == 1
    gated_run_id = replay["gated_run_id"]

    orders = gated_orders_payload(sandbox_id, "strategy6", gated_run_id=gated_run_id, root=tmp_path)
    assert orders["count"] == replay["candidate_count"]
    assert {row["gate_decision"] for row in orders["orders"]}.issuperset({"block", "reduce_size"})

    samples = gated_trade_quality_samples_payload(sandbox_id, "strategy6", gated_run_id=gated_run_id, root=tmp_path)
    assert samples["count"] == replay["candidate_count"]
    assert all("net_R" in row for row in samples["samples"])

    performance = gated_performance_payload(sandbox_id, "strategy6", gated_run_id=gated_run_id, root=tmp_path)
    assert performance["gated_run_id"] == gated_run_id
    assert performance["coverage"]["candidate_count"] == replay["candidate_count"]
    assert "delta_metrics" in performance

    shadow = gated_paper_shadow_payload(
        sandbox_id,
        "strategy6",
        {"run_id": run_id, "baseline_run_id": run_id},
        root=tmp_path,
    )
    assert shadow["status"] == "completed"
    assert shadow["candidate_count"] >= 2

    health = external_integration_health_payload(root=tmp_path)
    assert health["status"] == "ok"
    assert health["aggregate"]["external_gated_runs"] >= 2
    assert health["external_sqlite_write_allowed"] is False

    run_lookup = external_integration_run_payload(gated_run_id, root=tmp_path)
    assert run_lookup["count"] >= 1
    assert run_lookup["runs"][0]["type"] == "gated_run"

    events = external_integration_audit_events_payload(gated_run_id=gated_run_id, root=tmp_path)
    event_types = {row["event_type"] for row in events["events"]}
    assert "gated_replay_completed" in event_types

    with sqlite3.connect(created["sandbox"]["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        counts = {
            name: conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            for name in (
                "external_gated_runs",
                "external_gated_orders",
                "external_gated_results",
                "external_gated_performance",
                "external_integration_audit_events",
                "external_contract_versions",
            )
        }
    assert counts["external_gated_runs"] >= 2
    assert counts["external_gated_orders"] >= replay["candidate_count"]
    assert counts["external_gated_results"] >= replay["candidate_count"]
    assert counts["external_gated_performance"] >= 2
    assert counts["external_integration_audit_events"] >= 1
    assert counts["external_contract_versions"] >= 4


def test_strategy_sandbox_real_chain_isolated_and_deletable(tmp_path):
    first = create_sandbox_payload(
        strategy_line="strategy6",
        strategy_version="test-v1",
        data_scope={"days": 7, "symbols": ["BTCUSDT", "ETHUSDT"]},
        config_scope={"mode": "shadow_only", "target": "pf"},
        tags=["pytest", "first"],
        root=tmp_path,
    )
    second = create_sandbox_payload(
        strategy_line="strategy6",
        strategy_version="test-v1",
        data_scope={"days": 7, "symbols": ["BTCUSDT", "ETHUSDT"]},
        config_scope={"mode": "shadow_only", "target": "pf"},
        tags=["pytest", "second"],
        root=tmp_path,
    )
    first_id = first["sandbox"]["sandbox_id"]
    second_id = second["sandbox"]["sandbox_id"]

    assert first["sandbox"]["baseline_context_id"]
    assert second["sandbox"]["baseline_context_id"]
    assert first["sandbox"]["derived_from_sandbox_id"] is None
    assert second["sandbox"]["derived_from_sandbox_id"] is None
    assert first["sandbox"]["write_scope"] == "sandbox_only"
    assert second["sandbox"]["write_scope"] == "sandbox_only"

    for job_type in ("backtest", "replay", "trade_quality", "gate-search", "config-export", "paper-shadow"):
        result = job_payload(first_id, job_type, {"pytest": True}, root=tmp_path)
        assert result["status"] == "completed"

    first_summary = summary_payload(first_id, root=tmp_path)["summary"]["counts"]
    second_summary = summary_payload(second_id, root=tmp_path)["summary"]["counts"]
    assert first_summary["sandbox_orders"] >= 1
    assert first_summary["fill_model_runs"] >= 1
    assert first_summary["trade_quality_samples"] >= 1
    assert first_summary["gate_candidates"] >= 1
    assert first_summary["config_candidates"] >= 1
    assert first_summary["trade_plan_candidates"] >= 1
    assert first_summary["paper_shadow_results"] >= 1
    assert second_summary["sandbox_orders"] == 0
    assert second_summary["trade_quality_samples"] == 0

    with sqlite3.connect(first["sandbox"]["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        sample = conn.execute("SELECT root_cause FROM trade_quality_samples LIMIT 1").fetchone()
        gate = conn.execute("SELECT rule_json, status FROM gate_candidates LIMIT 1").fetchone()
        cfg = conn.execute("SELECT promotion_state FROM config_candidates LIMIT 1").fetchone()
        shadow = conn.execute("SELECT status FROM paper_shadow_results LIMIT 1").fetchone()
    assert sample is not None
    assert sample["root_cause"] != "contract_only"
    assert gate is not None
    assert '"entry_known_features_only": true' in gate["rule_json"]
    assert gate["status"] == "shadow_candidate"
    assert cfg is not None
    assert cfg["promotion_state"] == "shadow_review"
    assert shadow is not None
    assert shadow["status"] == "paper_shadow_smoke_complete"

    set_active_sandbox_payload(first_id, root=tmp_path)
    deleted = delete_sandbox_payload(first_id, mode="soft_delete", reason="pytest", root=tmp_path)
    assert deleted["status"] == "deleted"
    assert list_sandboxes_payload(root=tmp_path)["count"] == 1
    assert active_sandbox_payload(root=tmp_path)["active"] is None


def test_step303_sandbox_create_always_uses_clean_main_baseline(tmp_path):
    first = create_sandbox_payload(
        strategy_line="experiment",
        strategy_lines=["strategy5", "strategy6"],
        strategy_version="baseline-a",
        data_scope={"days": 2, "symbols": ["BTCUSDT", "ETHUSDT"]},
        config_scope={"mode": "shadow_only"},
        tags=["pytest", "baseline-a"],
        root=tmp_path,
    )
    first_id = first["sandbox"]["sandbox_id"]
    set_active_sandbox_payload(first_id, root=tmp_path)

    for job_type in ("backtest", "trade-quality", "gate-search", "config-export", "paper-shadow"):
        result = job_payload(first_id, job_type, {"pytest": True, "symbols": ["BTCUSDT"]}, root=tmp_path)
        assert result["status"] == "completed"
    overlay = create_code_overlay_payload(first_id, "strategy6", root=tmp_path)
    assert overlay["created"] is True

    second = create_sandbox_payload(
        strategy_line="experiment",
        strategy_lines=["strategy5", "strategy6"],
        strategy_version="baseline-b",
        data_scope={"days": 2, "symbols": ["BTCUSDT", "ETHUSDT"]},
        config_scope={"mode": "shadow_only"},
        tags=["pytest", "baseline-b"],
        root=tmp_path,
    )
    second_id = second["sandbox"]["sandbox_id"]
    assert second["active_changed"] is False
    assert active_sandbox_payload(root=tmp_path)["active"]["sandbox_id"] == first_id
    assert second["sandbox"]["baseline_parent_type"] == "main_system"
    assert second["sandbox"]["derived_from_sandbox_id"] is None
    assert second["sandbox"]["reset_mode"] == "reset_from_main_baseline"

    counts = summary_payload(second_id, root=tmp_path)["summary"]["counts"]
    assert counts["sandbox_orders"] == 0
    assert counts["trade_quality_samples"] == 0
    assert counts["gate_candidates"] == 0
    assert counts["config_candidates"] == 0
    assert counts["trade_plan_candidates"] == 0
    assert counts["paper_shadow_results"] == 0
    assert counts["sandbox_code_overlays"] == 0
    assert counts["sandbox_code_patches"] == 0
    assert counts["sandbox_jobs"] == 0


def test_strategy_sandbox_experiment_topology_branches_share_one_db(tmp_path):
    created = create_sandbox_payload(
        strategy_line="experiment",
        strategy_lines=["strategy4", "strategy5", "strategy6"],
        strategy_version="test-v2",
        data_scope={"days": 7, "symbols": ["BTCUSDT", "ETHUSDT"]},
        config_scope={"mode": "shadow_only", "target": "pf"},
        tags=["pytest", "experiment"],
        root=tmp_path,
    )
    sandbox = created["sandbox"]
    sandbox_id = sandbox["sandbox_id"]
    db_path = sandbox["db_path"]

    assert sandbox["strategy_line"] == "experiment"
    assert sandbox["legacy_strategy_line"] is None
    assert sandbox["branches_summary"]["strategy_lines"] == ["strategy4", "strategy5", "strategy6"]
    assert all(branch["sandbox_id"] == sandbox_id for branch in created["branches"])
    branch_ids = {branch["branch_id"] for branch in created["branches"]}
    assert len(branch_ids) == 3
    assert all(branch_id.startswith("br_") for branch_id in branch_ids)

    filtered = list_sandboxes_payload(strategy_line="strategy5", root=tmp_path)
    assert filtered["count"] == 1
    assert filtered["sandboxes"][0]["sandbox_id"] == sandbox_id

    branch_payload = branches_payload(sandbox_id, root=tmp_path)
    assert branch_payload["count"] == 3
    assert {row["strategy_line"] for row in branch_payload["branches"]} == {"strategy4", "strategy5", "strategy6"}

    multi = job_payload(sandbox_id, "trade-quality", {"pytest": True, "symbols": ["BTCUSDT", "ETHUSDT"]}, root=tmp_path)
    assert multi["status"] == "completed"
    assert multi["result"]["sandbox_topology"] == "multi_strategy_single_sandbox"
    assert multi["result"]["branch_count"] == 3

    leaderboard = leaderboard_payload(sandbox_id, root=tmp_path)
    assert leaderboard["count"] == 3
    assert {row["strategy_line"] for row in leaderboard["leaderboard"]} == {"strategy4", "strategy5", "strategy6"}
    assert sum(row["trade_count"] for row in leaderboard["leaderboard"]) >= 1

    tq_compare = trade_quality_compare_payload(sandbox_id, root=tmp_path)
    tq_lines = {row["strategy_line"] for row in tq_compare["items"]}
    assert tq_lines
    assert tq_lines.issubset({"strategy4", "strategy5", "strategy6"})

    multi_gate = job_payload(sandbox_id, "gate-search", {"pytest": True, "symbols": ["BTCUSDT", "ETHUSDT"]}, root=tmp_path)
    assert multi_gate["status"] == "completed"
    assert multi_gate["result"]["branch_count"] == 3

    branch_job = job_payload(sandbox_id, "gate-search", {"strategy_line": "strategy5", "pytest": True}, root=tmp_path)
    assert branch_job["status"] == "completed"
    assert branch_job["result"]["status"] == "shadow_candidate"

    gate_compare = gate_compare_payload(sandbox_id, root=tmp_path)
    gate_lines = {row["strategy_line"] for row in gate_compare["items"]}
    assert "strategy5" in gate_lines

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        branch_lines = {
            row["strategy_line"]
            for row in conn.execute("SELECT strategy_line FROM sandbox_strategy_branches").fetchall()
        }
        tq_lines = {
            row["strategy_line"]
            for row in conn.execute("SELECT DISTINCT strategy_line FROM trade_quality_samples").fetchall()
        }
        job_lines = {
            row["strategy_line"]
            for row in conn.execute("SELECT DISTINCT strategy_line FROM sandbox_jobs").fetchall()
        }
        gate_lines_all = {
            row["strategy_line"]
            for row in conn.execute("SELECT DISTINCT strategy_line FROM gate_candidates").fetchall()
        }
    assert branch_lines == {"strategy4", "strategy5", "strategy6"}
    assert tq_lines
    assert tq_lines.issubset({"strategy4", "strategy5", "strategy6"})
    assert {"strategy4", "strategy5", "strategy6"}.issubset(gate_lines_all)
    assert {"all", "strategy5"}.issubset(job_lines)


def test_step7118_coarse_matrix_keeps_parameter_set_lineage(tmp_path):
    created = create_sandbox_payload(
        strategy_line="experiment",
        strategy_lines=["strategy4", "strategy5", "strategy6"],
        strategy_version="step7.118-test",
        data_scope={"days": 14, "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]},
        config_scope={"mode": "shadow_only", "objective": "max_profit_factor"},
        tags=["pytest", "step7.118"],
        root=tmp_path,
    )
    sandbox_id = created["sandbox"]["sandbox_id"]
    db_path = created["sandbox"]["db_path"]

    result = job_payload(
        sandbox_id,
        "coarse-matrix",
        {
            "pytest": True,
            "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            "matrix_profile": "coarse_pf_discovery",
            "max_sets": 2,
        },
        root=tmp_path,
    )
    assert result["status"] == "completed"
    assert result["result"]["sandbox_topology"] == "multi_strategy_single_sandbox"
    assert result["result"]["parameter_set_count"] == 6
    assert {row["strategy_line"] for row in result["result"]["branches"]} == {"strategy4", "strategy5", "strategy6"}
    assert sum(row["parameter_set_count"] for row in result["result"]["branches"]) == 6

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        parameter_sets = conn.execute(
            "SELECT strategy_line, parameter_set_id, status FROM sandbox_parameter_sets"
        ).fetchall()
        orders = conn.execute(
            "SELECT DISTINCT strategy_line, parameter_set_id FROM sandbox_orders WHERE parameter_set_id IS NOT NULL"
        ).fetchall()
        tq = conn.execute(
            "SELECT DISTINCT strategy_line, parameter_set_id FROM trade_quality_samples WHERE parameter_set_id IS NOT NULL"
        ).fetchall()
        gates = conn.execute(
            "SELECT DISTINCT strategy_line, parameter_set_id FROM gate_candidates WHERE parameter_set_id IS NOT NULL"
        ).fetchall()
        configs = conn.execute(
            "SELECT DISTINCT strategy_line, parameter_set_id FROM config_candidates WHERE parameter_set_id IS NOT NULL"
        ).fetchall()
        paper = conn.execute(
            "SELECT DISTINCT strategy_line, parameter_set_id FROM paper_shadow_results WHERE parameter_set_id IS NOT NULL"
        ).fetchall()

    assert len(parameter_sets) == 6
    assert {row["status"] for row in parameter_sets} == {"completed"}
    assert {row["strategy_line"] for row in parameter_sets} == {"strategy4", "strategy5", "strategy6"}
    assert {row["strategy_line"] for row in orders}.issubset({"strategy4", "strategy5", "strategy6"})
    assert {row["strategy_line"] for row in tq}.issubset({"strategy4", "strategy5", "strategy6"})
    assert {row["strategy_line"] for row in gates} == {"strategy4", "strategy5", "strategy6"}
    assert {row["strategy_line"] for row in configs} == {"strategy4", "strategy5", "strategy6"}
    assert {row["strategy_line"] for row in paper} == {"strategy4", "strategy5", "strategy6"}


def test_step2321_code_overlay_runtime_isolated_and_lineaged(tmp_path):
    baseline_file = tmp_path / "baseline_should_not_change.py"
    baseline_file.write_text("BASELINE = 1\n", encoding="utf-8")
    before_hash = baseline_file.read_text(encoding="utf-8")
    created = create_sandbox_payload(
        strategy_line="experiment",
        strategy_lines=["strategy5", "strategy6"],
        strategy_version="code-overlay-test",
        data_scope={"days": 1, "symbols": ["BTCUSDT"]},
        config_scope={"mode": "shadow_only"},
        tags=["pytest", "code-overlay"],
        root=tmp_path,
    )
    sandbox_id = created["sandbox"]["sandbox_id"]
    other = create_sandbox_payload(
        strategy_line="experiment",
        strategy_lines=["strategy5"],
        strategy_version="code-overlay-test",
        data_scope={"days": 1, "symbols": ["BTCUSDT"]},
        config_scope={"mode": "shadow_only"},
        tags=["pytest", "other"],
        root=tmp_path,
    )

    overlay = create_code_overlay_payload(sandbox_id, "strategy6", root=tmp_path)
    assert overlay["created"] is True
    assert overlay["active_overlay"]["strategy_line"] == "strategy6"
    assert overlay["active_overlay"]["overlay_path"].endswith("branches\\strategy6\\code_overlay") or overlay["active_overlay"]["overlay_path"].endswith("branches/strategy6/code_overlay")

    patched = add_code_patch_payload(
        sandbox_id,
        "strategy6",
        {
            "target_relpath": "notes/safe_patch.md",
            "patch_type": "manifest_note",
            "note": "change only inside sandbox overlay",
            "content": "sandbox-only content\n",
            "diff_text": "+ sandbox-only content",
        },
        root=tmp_path,
    )
    assert patched["patch_count"] == 1
    patch_id = patched["code_patch_id"]

    try:
        add_code_patch_payload(
            sandbox_id,
            "strategy6",
            {"target_relpath": "laoma_signal_engine/strategy5.py", "note": "bad"},
            root=tmp_path,
        )
    except ValueError as exc:
        assert "baseline_path_forbidden" in str(exc)
    else:
        raise AssertionError("baseline path patch should be rejected")

    try:
        add_code_patch_payload(
            sandbox_id,
            "strategy6",
            {"target_relpath": "../escape.py", "note": "bad"},
            root=tmp_path,
        )
    except ValueError as exc:
        assert "target_relpath_must_stay_inside_overlay" in str(exc)
    else:
        raise AssertionError("path traversal patch should be rejected")

    built = build_runtime_payload(sandbox_id, "strategy6", root=tmp_path)
    runtime_id = built["runtime_id"]
    assert built["runtimes"][0]["runtime_id"] == runtime_id
    assert built["runtimes"][0]["code_patch_id"] == patch_id
    assert built["runtime_manifest"]["dynamic_import_enabled"] is False
    assert built["runtime_manifest"]["baseline_mutation_allowed"] is False
    assert built["runtimes"][0]["import_map"]["patch_manifest_only"] is True

    smoke = runtime_smoke_payload(sandbox_id, "strategy6", {"symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]}, root=tmp_path)
    assert smoke["status"] == "smoke_passed"
    with sqlite3.connect(created["sandbox"]["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        run = conn.execute("SELECT code_overlay_id, code_patch_id, runtime_id FROM evaluator_runs LIMIT 1").fetchone()
        order = conn.execute("SELECT code_patch_id, runtime_id FROM sandbox_orders LIMIT 1").fetchone()
    assert run is not None
    assert run["code_patch_id"] == patch_id
    assert run["runtime_id"] == runtime_id
    assert order is not None
    assert order["runtime_id"] == runtime_id

    other_overlay = code_overlay_payload(other["sandbox"]["sandbox_id"], "strategy5", root=tmp_path)
    assert other_overlay["overlay_count"] == 0
    assert baseline_file.read_text(encoding="utf-8") == before_hash
