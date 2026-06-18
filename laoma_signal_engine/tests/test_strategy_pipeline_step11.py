from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.core.exit_codes import EXIT_WAIT_UNTIL_READY_TIMEOUT
import laoma_signal_engine.strategy_pipeline as strategy_pipeline
from laoma_signal_engine.strategy_pipeline import run_strategy_pipeline_safe


def test_strategy_pipeline_without_micro_writes_report(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_without_ofi_cvd_safe",
        lambda **_: calls.append("assemble_without") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="without_micro",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == ["pre", "assemble_without", "refresh", "plan_without_micro"]
    report = json.loads(
        (tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"),
    )
    assert report["source"] == "strategy_pipeline"
    assert report["line"] == "without_micro"
    assert report["status"] == "ok"
    assert "upstream_refresh" in report
    assert report["run_audit_ledger"]["status"] == "ok"
    import sqlite3

    with sqlite3.connect(tmp_path / "DATA/audit/run_audit.db") as conn:
        audit_row = conn.execute(
            "select status from audit_runs where run_id = ?",
            (report["run_id"],),
        ).fetchone()
    assert audit_row is not None
    archive = tmp_path / "DATA" / "reports" / "pipeline_runs" / report["run_id"] / "strategy_pipeline_report.json"
    assert archive.is_file()


def test_step1428_paper_once_retries_alive_tick_lock_after_release(tmp_path: Path, monkeypatch) -> None:
    calls = {"paper": 0, "inspect": 0}

    def fake_run_paper_once(root: Path, config=None):
        calls["paper"] += 1
        if calls["paper"] == 1:
            return {
                "status": "skipped",
                "reason": "paper_tick_lock_alive_busy",
                "reason_codes": ["paper_tick_lock_alive_busy"],
                "tick_lock": {"exists": True, "pid": 123, "pid_alive": True, "status": "busy"},
            }
        return {
            "consume": {"created": 1, "skipped": []},
            "summary": {"generated_at": "2026-06-02T00:00:00Z"},
            "tick_lock": {"exists": True, "status": "acquired", "acquired": True},
        }

    def fake_inspect_tick_lock(root: Path, config=None):
        calls["inspect"] += 1
        return {"exists": False, "pid": None, "pid_alive": False, "status": "clear"}

    monkeypatch.setattr(strategy_pipeline, "run_paper_once", fake_run_paper_once)
    monkeypatch.setattr(strategy_pipeline, "inspect_tick_lock", fake_inspect_tick_lock)
    monkeypatch.setattr(strategy_pipeline.time, "sleep", lambda _: None)

    result = strategy_pipeline._run_paper_once_with_tick_retry(tmp_path, max_wait_sec=5)

    assert calls["paper"] == 2
    assert result["consume"]["created"] == 1
    assert "paper_inline_wakeup_retried" in result["reason_codes"]
    assert result["inline_retry"]["attempts"] == 1


def test_step1045_selected_lines_skip_unselected_current_json(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_without_ofi_cvd_safe",
        lambda **_: calls.append("assemble_without") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        lines=["without_micro", "micro_fast"],
        mode="once",
        skip_micro_wait=True,
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == [
        "pre",
        "assemble_without",
        "refresh",
        "plan_without_micro",
        "assemble_with",
        "refresh",
        "plan_micro_fast",
    ]
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    assert report["selected_lines"] == ["without_micro", "micro_fast"]
    assert report["skipped_lines"] == ["micro_full"]
    skipped = json.loads((tmp_path / "DATA/decisions/latest_trade_plan_micro_full.json").read_text(encoding="utf-8"))
    assert skipped["run_id"] == report["run_id"]
    assert skipped["status"] == "blocked"
    assert skipped["executable_count"] == 0
    assert skipped["input_refs"]["blocked_reason"] == "strategy_line_not_selected"
    progress = json.loads((tmp_path / "DATA/runtime/strategy_pipeline_progress.json").read_text(encoding="utf-8"))
    assert progress["selected_lines"] == ["without_micro", "micro_fast"]
    assert progress["lines"]["micro_full"]["skipped"] is True
    assert progress["lines"]["micro_full"]["stage"] == "skipped_not_selected"


def test_step12_35_failed_pipeline_archives_first_failed_stage_and_sqlite_ledger(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._make_run_id",
        lambda: "run_failed_1",
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: 1,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        lines=["without_micro", "micro_fast"],
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc != 0
    latest = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    assert latest["run_id"] == "run_failed_1"
    assert latest["status"] == "failed"
    assert latest["first_failed_stage"] == "common_upstream_step1_to_step2_5"
    archive = tmp_path / "DATA/reports/pipeline_runs/run_failed_1/strategy_pipeline_report.json"
    failed_stage = tmp_path / "DATA/reports/pipeline_runs/run_failed_1/strategy_pipeline_failed_stage.json"
    assert archive.is_file()
    assert failed_stage.is_file()
    archived = json.loads(archive.read_text(encoding="utf-8"))
    assert archived["first_failed_stage"] == "common_upstream_step1_to_step2_5"
    assert archived["outputs"]["pipeline_report_archive"] == str(archive)

    import sqlite3

    with sqlite3.connect(tmp_path / "DATA/audit/run_audit.db") as conn:
        row = conn.execute("select status, summary_json from audit_runs where run_id = ?", ("run_failed_1",)).fetchone()
        event_count = conn.execute(
            "select count(*) from audit_downstream_events where run_id = ? and event_type = ?",
            ("run_failed_1", "pipeline_failed"),
        ).fetchone()[0]
    assert row is not None
    assert row[0] == "failed"
    assert json.loads(row[1])["first_failed_stage"] == "common_upstream_step1_to_step2_5"
    assert event_count == 1


def test_step12_35_failed_archive_survives_latest_overwrite(tmp_path: Path, monkeypatch) -> None:
    run_ids = iter(["run_failed_old", "run_ok_new"])
    monkeypatch.setattr("laoma_signal_engine.strategy_pipeline._make_run_id", lambda: next(run_ids))
    outcomes = iter([1, 0])
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: next(outcomes),
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_without_ofi_cvd_safe",
        lambda **_: 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **_: 0,
    )

    first_rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="without_micro",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )
    second_rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="without_micro",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert first_rc != 0
    assert second_rc == 0
    latest = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    assert latest["run_id"] == "run_ok_new"
    old_archive = tmp_path / "DATA/reports/pipeline_runs/run_failed_old/strategy_pipeline_report.json"
    assert old_archive.is_file()
    assert json.loads(old_archive.read_text(encoding="utf-8"))["status"] == "failed"


def test_step1046_micro_full_selection_keeps_fixed_post_run_cooldown(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        lines=["micro_full"],
        mode="once",
        interval_sec=300,
        skip_micro_wait=True,
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    assert report["selected_lines"] == ["micro_full"]
    assert report["requested_interval_sec"] == 300
    assert report["effective_interval_sec"] == 300
    assert report["post_run_cooldown_sec"] == 300
    assert report["interval_semantics"] == "post_run_cooldown"
    assert report["line_runtime_budgets"]["micro_full"] >= 1200


def test_strategy_pipeline_all_can_run_bounded_interval_without_micro_wait(
    tmp_path: Path,
    monkeypatch,
) -> None:
    planned: list[str] = []

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: planned.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_without_ofi_cvd_safe",
        lambda **_: planned.append("assemble_without") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: planned.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: planned.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: planned.append(f"plan_{kwargs['line']}") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_audit_trade_plan_lines_safe",
        lambda **_: planned.append("abc") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_final_decisions_from_trade_plans_safe",
        lambda **_: planned.append("aggregate") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="all",
        mode="interval",
        max_cycles=1,
        skip_micro_wait=True,
        run_json_stage_audit=False,
    )

    assert rc == 0
    assert planned == [
        "pre",
        "assemble_without",
        "refresh",
        "plan_without_micro",
        "assemble_with",
        "refresh",
        "plan_micro_fast",
        "assemble_with",
        "refresh",
        "plan_micro_full",
        "abc",
        "aggregate",
    ]
    report = json.loads(
        (tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"),
    )
    assert report["mode"] == "interval"
    assert report["next_run_at"] is not None


def test_strategy_pipeline_lock_skip_writes_report(tmp_path: Path) -> None:
    lock = tmp_path / "DATA/runtime/strategy_pipeline.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    now = utc_now()
    lock.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "lock_owner_pid": os.getpid(),
                "run_id": "old",
                "cycle_id": "old",
                "started_at": to_iso_z(now),
                "expires_at": to_iso_z(now + timedelta(minutes=5)),
                "stage": "test",
            },
        ),
        encoding="utf-8",
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="all",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    report = json.loads(
        (tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"),
    )
    assert report["status"] == "skipped"
    assert report["skip_reason"] == "scheduler_skipped_previous_cycle_running"


def test_step1213_strategy_pipeline_renews_lock_during_micro_wait(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )

    def fake_wait(**kwargs) -> int:
        calls.append("wait")
        kwargs["on_poll"](1.0)
        return 0

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        fake_wait,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    progress = json.loads((tmp_path / "DATA/runtime/strategy_pipeline_progress.json").read_text(encoding="utf-8"))
    assert progress["lines"]["micro_fast"]["done"] is True
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    assert report["status"] == "ok"


def test_step1030_micro_fast_timeout_is_degraded_when_ready_not_required(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[str] = []
    cfg_dir = tmp_path / "laoma_signal_engine" / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "default.yaml").write_text(
        "trade_plan_lines:\n  micro_fast:\n    require_micro_ready: false\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: {"status": "running", "pid_running": True, "stale": False, "heartbeat_exists": True},
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        lambda **_: calls.append("wait_timeout") or 2,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == ["pre", "wait_timeout", "assemble_with", "refresh", "plan_micro_fast"]
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    wait_stage = next(s for s in report["stages"] if s["name"] == "wait_micro_ready_micro_fast")
    assert wait_stage["ok"] is True
    assert wait_stage["original_rc"] == 2
    assert wait_stage["status"] == "degraded"


def test_step1012_micro_unhealthy_blocks_micro_line_without_wait(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._micro_health_inline_recovery",
        lambda **kwargs: (kwargs["initial_health"], None),
    )

    def fail_wait(**_: object) -> int:
        raise AssertionError("micro wait should not run when daemon is unhealthy")

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        fail_wait,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == ["pre"]
    doc = json.loads((tmp_path / "DATA/decisions/latest_trade_plan_micro_fast.json").read_text(encoding="utf-8"))
    assert doc["status"] == "blocked"
    assert doc["run_id"]
    assert doc["input_refs"]["blocked_reason"] == "micro_daemon_unhealthy"
    progress = json.loads((tmp_path / "DATA/runtime/strategy_pipeline_progress.json").read_text(encoding="utf-8"))
    assert progress["lines"]["micro_fast"]["done"] is True
    assert progress["lines"]["micro_fast"]["stage"] == "blocked_micro_unhealthy_micro_fast"


def test_step164_micro_inline_recovery_continues_current_line(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[str] = []
    health_checks = [
        {
            "name": "micro_daemon",
            "status": "stopped",
            "pid": 111,
            "pid_running": False,
            "heartbeat_exists": True,
            "heartbeat_age_sec": 999,
            "stale": True,
            "state_generated_at": to_iso_z(utc_now() - timedelta(seconds=999)),
            "features_generated_at": to_iso_z(utc_now() - timedelta(seconds=999)),
        },
        {
            "name": "micro_daemon",
            "status": "running",
            "pid": 222,
            "pid_running": True,
            "heartbeat_exists": True,
            "heartbeat_age_sec": 1,
            "stale": False,
            "state_generated_at": to_iso_z(utc_now()),
            "features_generated_at": to_iso_z(utc_now()),
        },
    ]

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: health_checks.pop(0) if health_checks else {
            "status": "running",
            "pid": 222,
            "pid_running": True,
            "heartbeat_exists": True,
            "heartbeat_age_sec": 1,
            "stale": False,
            "state_generated_at": to_iso_z(utc_now()),
            "features_generated_at": to_iso_z(utc_now()),
        },
    )
    monkeypatch.setattr("laoma_signal_engine.strategy_pipeline.time.sleep", lambda _: None)
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._micro_daemon_control_action",
        lambda cfg, action: calls.append(f"daemon_{action}") or {
            "status": "completed",
            "returncode": 0,
            "payload": {"action": action, "status": "ok", "pid": 222},
        },
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        lambda **_: calls.append("wait") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == [
        "pre",
        "daemon_stop",
        "daemon_start",
        "wait",
        "assemble_with",
        "refresh",
        "plan_micro_fast",
    ]
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    recovery = next(s for s in report["stages"] if s["name"] == "micro_inline_recovery_micro_fast")
    assert recovery["ok"] is True
    assert recovery["status"] == "recovered"
    assert recovery["old_pid"] == 111
    assert recovery["new_pid"] == 222
    assert recovery["reason_codes"] == []
    assert any(s["name"] == "wait_micro_ready_micro_fast" for s in report["stages"])


def test_step1610_micro_alive_data_plane_stale_triggers_inline_recovery(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[str] = []
    health_checks = [
        {
            "name": "micro_daemon",
            "status": "stale",
            "health_state": "degraded_transport_ok_data_stale",
            "data_plane_status": "stale",
            "pid": 111,
            "pid_running": True,
            "heartbeat_exists": True,
            "heartbeat_age_sec": 120,
            "stale": True,
            "ws_connected": True,
            "reason_codes": ["micro_alive_but_not_emitting", "micro_ws_connected_but_no_emit"],
            "state_generated_at": to_iso_z(utc_now() - timedelta(seconds=120)),
            "features_generated_at": to_iso_z(utc_now() - timedelta(seconds=120)),
        },
        {
            "name": "micro_daemon",
            "status": "running",
            "health_state": "healthy",
            "data_plane_status": "fresh",
            "pid": 222,
            "pid_running": True,
            "heartbeat_exists": True,
            "heartbeat_age_sec": 1,
            "stale": False,
            "state_generated_at": to_iso_z(utc_now()),
            "features_generated_at": to_iso_z(utc_now()),
        },
    ]

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: health_checks.pop(0) if health_checks else {
            "status": "running",
            "health_state": "healthy",
            "data_plane_status": "fresh",
            "pid": 222,
            "pid_running": True,
            "heartbeat_exists": True,
            "heartbeat_age_sec": 1,
            "stale": False,
            "state_generated_at": to_iso_z(utc_now()),
            "features_generated_at": to_iso_z(utc_now()),
        },
    )
    monkeypatch.setattr("laoma_signal_engine.strategy_pipeline.time.sleep", lambda _: None)
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._micro_daemon_control_action",
        lambda cfg, action: calls.append(f"daemon_{action}") or {
            "status": "completed",
            "returncode": 0,
            "payload": {"action": action, "status": "ok", "pid": 222},
        },
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        lambda **_: calls.append("wait") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == ["pre", "daemon_stop", "daemon_start", "wait", "assemble_with", "refresh", "plan_micro_fast"]
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    recovery = next(s for s in report["stages"] if s["name"] == "micro_inline_recovery_micro_fast")
    assert recovery["ok"] is True
    assert recovery["old_pid"] == 111
    assert recovery["new_pid"] == 222


def test_step1612_micro_fast_wait_stale_recovery_extends_wait(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[str] = []
    fresh_health = {
        "name": "micro_daemon",
        "status": "running",
        "health_state": "healthy",
        "data_plane_status": "fresh",
        "pid": 222,
        "pid_running": True,
        "heartbeat_exists": True,
        "heartbeat_age_sec": 1,
        "stale": False,
        "state_generated_at": to_iso_z(utc_now()),
        "features_generated_at": to_iso_z(utc_now()),
    }
    stale_health = {
        "name": "micro_daemon",
        "status": "stale",
        "health_state": "degraded_transport_ok_data_stale",
        "data_plane_status": "stale",
        "pid": 222,
        "pid_running": True,
        "heartbeat_exists": True,
        "heartbeat_age_sec": 120,
        "stale": True,
        "reason_codes": ["micro_alive_but_not_emitting", "micro_ws_connected_but_no_emit"],
        "state_generated_at": to_iso_z(utc_now() - timedelta(seconds=120)),
        "features_generated_at": to_iso_z(utc_now() - timedelta(seconds=120)),
    }
    health_checks = [fresh_health, stale_health]

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: health_checks.pop(0) if health_checks else fresh_health,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._micro_health_inline_recovery",
        lambda **_: (
            fresh_health,
            {
                "name": "micro_inline_recovery_micro_fast",
                "ok": True,
                "rc": 0,
                "status": "recovered",
                "old_pid": 222,
                "new_pid": 333,
                "reason_codes": [],
            },
        ),
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )

    wait_results = [EXIT_WAIT_UNTIL_READY_TIMEOUT, 0]

    def fake_wait(**kwargs: object) -> int:
        wait_cfg = kwargs["cfg"]
        calls.append(f"wait_{int(float(wait_cfg.max_wait_sec))}")  # type: ignore[attr-defined]
        return wait_results.pop(0)

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        fake_wait,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls[0] == "pre"
    assert len([c for c in calls if c.startswith("wait_")]) == 2
    assert calls[-3:] == ["assemble_with", "refresh", "plan_micro_fast"]
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    recovery = next(s for s in report["stages"] if s["name"] == "micro_inline_recovery_micro_fast")
    assert recovery["ok"] is True
    assert "micro_wait_recovery_attempted" in recovery["reason_codes"]
    assert "micro_wait_recovery_success" in recovery["reason_codes"]
    extension = next(s for s in report["stages"] if s["name"] == "wait_micro_ready_micro_fast_recovered_extension")
    assert extension["ok"] is True
    assert extension["detail"]["wait_recovery"]["success"] is True
    assert extension["detail"]["wait_recovery"]["extended_wait_sec"] > 0
    assert "micro_wait_extended_after_recovery" in extension["detail"]["reason_codes"]


def test_step1612_micro_fast_wait_stale_recovery_failure_marks_technical_blocked(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[str] = []
    fresh_health = {
        "name": "micro_daemon",
        "status": "running",
        "health_state": "healthy",
        "data_plane_status": "fresh",
        "pid": 222,
        "pid_running": True,
        "heartbeat_exists": True,
        "heartbeat_age_sec": 1,
        "stale": False,
        "state_generated_at": to_iso_z(utc_now()),
        "features_generated_at": to_iso_z(utc_now()),
    }
    stale_health = {
        "name": "micro_daemon",
        "status": "stale",
        "health_state": "degraded_transport_ok_data_stale",
        "data_plane_status": "stale",
        "pid": 222,
        "pid_running": True,
        "heartbeat_exists": True,
        "heartbeat_age_sec": 120,
        "stale": True,
        "reason_codes": ["micro_alive_but_not_emitting", "micro_ws_connected_but_no_emit"],
        "state_generated_at": to_iso_z(utc_now() - timedelta(seconds=120)),
        "features_generated_at": to_iso_z(utc_now() - timedelta(seconds=120)),
    }
    health_checks = [fresh_health, stale_health]

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: health_checks.pop(0) if health_checks else stale_health,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._micro_health_inline_recovery",
        lambda **_: (
            stale_health,
            {
                "name": "micro_inline_recovery_micro_fast",
                "ok": False,
                "rc": 2,
                "status": "failed",
                "old_pid": 222,
                "new_pid": 222,
                "reason_codes": ["micro_inline_recovery_failed", "micro_features_not_fresh"],
            },
        ),
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        lambda **_: calls.append("wait") or EXIT_WAIT_UNTIL_READY_TIMEOUT,
    )

    def fail_downstream(**_: object) -> int:
        raise AssertionError("micro_fast technical_blocked must stop before downstream stages")

    monkeypatch.setattr("laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe", fail_downstream)
    monkeypatch.setattr("laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe", fail_downstream)
    monkeypatch.setattr("laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe", fail_downstream)

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == ["pre", "wait"]
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    recovery = next(s for s in report["stages"] if s["name"] == "micro_inline_recovery_micro_fast")
    assert recovery["ok"] is False
    assert "micro_wait_recovery_attempted" in recovery["reason_codes"]
    blocked = next(s for s in report["stages"] if s["name"] == "blocked_micro_wait_technical_micro_fast")
    assert blocked["ok"] is True

    lifecycle = json.loads((tmp_path / "DATA/micro/latest_micro_lifecycle_micro_fast.json").read_text(encoding="utf-8"))
    assert lifecycle["line_exec_status"] == "technical_blocked"
    assert lifecycle["line_lifecycle_status"] == "technical_blocked"
    assert lifecycle["technical_blocked"] is True
    assert lifecycle["technical_block_reason"] == "micro_daemon_stale_during_wait"
    assert "technical_blocked_micro_daemon_stale" in lifecycle["reason_codes"]

    plan = json.loads((tmp_path / "DATA/decisions/latest_trade_plan_micro_fast.json").read_text(encoding="utf-8"))
    assert plan["status"] == "blocked"
    assert plan["count"] == 0
    assert plan["input_refs"]["blocked_reason"] == "micro_daemon_stale_during_wait"
    assert plan["input_refs"]["line_exec_status"] == "technical_blocked"
    assert plan["input_refs"]["runtime_health"]["technical_blocked"] is True

    progress = json.loads((tmp_path / "DATA/runtime/strategy_pipeline_progress.json").read_text(encoding="utf-8"))
    row = progress["lines"]["micro_fast"]
    assert row["done"] is True
    assert row["line_exec_status"] == "technical_blocked"
    assert row["stage_status_class"] == "technical_failed"
    assert row["technical_blocked"] is True


def test_step164_micro_inline_recovery_failure_blocks_line(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: {
            "name": "micro_daemon",
            "status": "stopped",
            "pid": 111,
            "pid_running": False,
            "heartbeat_exists": True,
            "heartbeat_age_sec": 999,
            "stale": True,
            "state_generated_at": None,
            "features_generated_at": None,
        },
    )
    monkeypatch.setattr("laoma_signal_engine.strategy_pipeline.time.sleep", lambda _: None)
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._micro_daemon_control_action",
        lambda cfg, action: calls.append(f"daemon_{action}") or {
            "status": "failed" if action == "start" else "completed",
            "returncode": 1 if action == "start" else 0,
            "payload": {"action": action},
        },
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )

    def fail_wait(**_: object) -> int:
        raise AssertionError("micro wait should not run when inline recovery fails")

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        fail_wait,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == ["pre", "daemon_stop", "daemon_start"]
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    recovery = next(s for s in report["stages"] if s["name"] == "micro_inline_recovery_micro_fast")
    assert recovery["ok"] is False
    assert recovery["status"] == "failed"
    assert "micro_inline_recovery_failed" in recovery["reason_codes"]
    assert any(s["name"] == "blocked_micro_unhealthy_micro_fast" for s in report["stages"])


def test_step1218_micro_health_grace_recheck_recovers_before_block(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[str] = []
    health_checks = [
        {
            "name": "micro_daemon",
            "status": "stopped",
            "pid": 111,
            "pid_running": False,
            "heartbeat_exists": True,
            "heartbeat_age_sec": 12,
            "stale": False,
        },
        {
            "name": "micro_daemon",
            "status": "running",
            "pid": 222,
            "pid_running": True,
            "heartbeat_exists": True,
            "heartbeat_age_sec": 1,
            "stale": False,
        },
    ]

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: health_checks.pop(0) if health_checks else {
            "status": "running",
            "pid": 222,
            "pid_running": True,
            "heartbeat_exists": True,
            "heartbeat_age_sec": 1,
            "stale": False,
        },
    )
    monkeypatch.setattr("laoma_signal_engine.strategy_pipeline.time.sleep", lambda _: None)
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        lambda **_: calls.append("wait") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == ["pre", "wait", "assemble_with", "refresh", "plan_micro_fast"]
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    grace = next(s for s in report["stages"] if s["name"] == "micro_health_grace_recheck_micro_fast")
    assert grace["ok"] is True
    assert grace["status"] == "recovered"
    assert grace["old_pid"] == 111
    assert grace["new_pid"] == 222


def test_step1037_micro_wait_records_partial_ready_line_status(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []
    micro_dir = tmp_path / "DATA" / "micro"
    micro_dir.mkdir(parents=True, exist_ok=True)
    target_generated_at = to_iso_z(utc_now())
    (micro_dir / "micro_targets.json").write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": target_generated_at,
                "target_set_id": "target_partial",
                "target_symbols": ["AAAUSDT", "BBBUSDT"],
                "tier1_warm_watch": [
                    {
                        "symbol": "AAAUSDT",
                        "sticky_source": "daemon_state",
                        "retained_reason": "ready_cache",
                    },
                    {"symbol": "BBBUSDT"},
                ],
                "tier2_active_strong": [],
            },
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: {"status": "running", "pid_running": True, "stale": False, "heartbeat_exists": True},
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )

    def fake_wait(**kwargs) -> int:
        calls.append("wait")
        kwargs["latest_path"].write_text(
            json.dumps(
                {
                    "schema_version": "1.6",
                    "generated_at": to_iso_z(utc_now()),
                    "status": "ok",
                    "target_generated_at": target_generated_at,
                    "target_status": "fresh",
                    "ws_status": "connected",
                    "last_ws_message_age_sec": 0,
                    "ready_count": 1,
                    "fast_ready_count": 1,
                    "full_ready_count": 0,
                    "items": [
                        {
                            "symbol": "AAAUSDT",
                            "micro_quality": {"ready": True, "reason_codes": []},
                            "micro_fast_quality": {"ready": True, "reason_codes": []},
                            "micro_fast_signal": {
                                "micro_direction_confirmed": True,
                                "micro_exec_allowed": True,
                            },
                            "micro_full_quality": {"ready": False, "reason_codes": ["warmup_not_met"]},
                        },
                        {
                            "symbol": "BBBUSDT",
                            "micro_quality": {"ready": False, "reason_codes": ["z_missing"]},
                            "micro_fast_quality": {"ready": False, "reason_codes": ["warmup_not_met"]},
                            "micro_full_quality": {"ready": False, "reason_codes": ["warmup_not_met"]},
                        },
                    ],
                },
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        fake_wait,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == ["pre", "wait", "assemble_with", "refresh", "plan_micro_fast"]
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    wait_stage = next(s for s in report["stages"] if s["name"] == "wait_micro_ready_micro_fast")
    assert wait_stage["detail"]["line_exec_status"] == "usable_partial"
    assert wait_stage["detail"]["line_lifecycle_status"] == "partial_ready"
    assert wait_stage["detail"]["trade_plan_allowed"] is True
    assert wait_stage["detail"]["line_lifecycle_complete"] is False
    assert wait_stage["detail"]["unfinished_symbol_count"] == 1
    assert wait_stage["detail"]["ready_source_counts"]["sticky_ready_cache"] == 1
    progress = json.loads((tmp_path / "DATA/runtime/strategy_pipeline_progress.json").read_text(encoding="utf-8"))
    row = progress["lines"]["micro_fast"]
    assert row["done"] is True
    assert row["stage"] == "completed_with_unfinished_symbols"
    assert row["line_exec_status"] == "usable_partial"
    assert row["line_lifecycle_status"] == "partial_ready"
    assert row["stage_status_class"] == "business_partial_consumable"


def test_step1047_micro_fast_quality_ready_without_consumable_writes_blocked_doc(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[str] = []
    micro_dir = tmp_path / "DATA" / "micro"
    micro_dir.mkdir(parents=True, exist_ok=True)
    target_generated_at = to_iso_z(utc_now())
    (micro_dir / "micro_targets.json").write_text(
        json.dumps(
                {
                    "schema_version": "1.6",
                    "generated_at": target_generated_at,
                    "target_set_id": "target_quality_only",
                    "target_symbols": ["AAAUSDT", "BBBUSDT"],
                    "tier1_warm_watch": [{"symbol": "AAAUSDT"}, {"symbol": "BBBUSDT"}],
                    "tier2_active_strong": [],
                },
            ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: {"status": "running", "pid_running": True, "stale": False, "heartbeat_exists": True},
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )

    def fake_wait(**kwargs) -> int:
        calls.append("wait")
        kwargs["latest_path"].write_text(
            json.dumps(
                {
                    "schema_version": "1.6",
                    "generated_at": to_iso_z(utc_now()),
                    "status": "ok",
                    "target_generated_at": target_generated_at,
                    "target_status": "fresh",
                    "ready_count": 1,
                    "fast_ready_count": 1,
                    "full_ready_count": 0,
                    "items": [
                        {
                            "symbol": "AAAUSDT",
                            "micro_quality": {"ready": True, "reason_codes": []},
                            "micro_fast_quality": {"ready": True, "reason_codes": []},
                            "micro_fast_signal": {
                                "micro_direction_confirmed": False,
                                "micro_exec_allowed": False,
                                "reason_codes": ["micro_direction_conflict"],
                            },
                        },
                        {
                            "symbol": "BBBUSDT",
                            "micro_quality": {"ready": False, "reason_codes": ["warmup_not_met"]},
                            "micro_fast_quality": {
                                "ready": False,
                                "warmup_age_sec": 41,
                                "required_observed_sec": 180,
                                "reason_codes": ["warmup_not_met"],
                            },
                            "micro_fast_signal": {
                                "micro_direction_confirmed": False,
                                "micro_exec_allowed": False,
                                "reason_codes": ["micro_signal_not_ready"],
                            },
                        },
                    ],
                },
            ),
            encoding="utf-8",
        )
        kwargs["evidence_path"].parent.mkdir(parents=True, exist_ok=True)
        latest_micro = json.loads(kwargs["latest_path"].read_text(encoding="utf-8"))
        kwargs["evidence_path"].write_text(
            json.dumps(
                {
                    "schema_version": "10.38",
                    "source": "micro_wait_pass_evidence",
                    "strategy_line": "micro_fast",
                    "run_id": kwargs.get("run_id"),
                    "cycle_id": kwargs.get("cycle_id"),
                    "target_set_id": "target_quality_only",
                    "generated_at": to_iso_z(utc_now()),
                    "micro_generated_at": latest_micro["generated_at"],
                    "micro_state_generated_at": "",
                    "wait_predicate": "min_fast_ready_count",
                    "ready_symbols": ["AAAUSDT"],
                    "fast_ready_symbols": ["AAAUSDT"],
                    "full_ready_symbols": [],
                    "quality_ready_symbols": ["AAAUSDT"],
                    "confirmed_symbols": [],
                    "consumable_symbols": [],
                    "quality_ready_count": 1,
                    "confirmed_ready_count": 0,
                    "consumable_ready_count": 0,
                    "micro_features": latest_micro,
                    "micro_state": None,
                },
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        fake_wait,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == ["pre", "wait"]
    doc = json.loads((tmp_path / "DATA/decisions/latest_trade_plan_micro_fast.json").read_text(encoding="utf-8"))
    assert doc["status"] == "blocked"
    assert doc["executable_count"] == 0
    assert doc["plans"] == []
    assert doc["input_refs"]["blocked_reason"] == "micro_fast_quality_ready_but_no_confirmed_symbol"
    assert doc["input_refs"]["runtime_health"]["trade_plan_allowed"] is False
    assert doc["input_refs"]["runtime_health"]["quality_ready_count"] == 1
    assert doc["input_refs"]["runtime_health"]["consumable_ready_count"] == 0
    assert doc["input_refs"]["micro_wait_evidence_used"] is True
    assert doc["input_refs"]["micro_wait_predicate"] == "min_fast_ready_count"
    assert doc["input_refs"]["line_lifecycle_status"] == "terminalized_no_consumable"
    assert doc["input_refs"]["line_lifecycle_complete"] is True
    assert doc["input_refs"]["unfinished_symbol_count"] == 0
    lifecycle = json.loads(
        (tmp_path / "DATA/micro/latest_micro_lifecycle_micro_fast.json").read_text(encoding="utf-8"),
    )
    assert lifecycle["schema_version"] == "10.35"
    assert lifecycle["run_id"] == doc["run_id"]
    assert lifecycle["cycle_id"] == doc["cycle_id"]
    assert lifecycle["status"] == "blocked"
    assert lifecycle["blocked_reason"] == "micro_fast_quality_ready_but_no_confirmed_symbol"
    assert lifecycle["state_counts"]["rejected"] == 1
    assert lifecycle["state_counts"]["not_ready"] == 1
    assert "observing" not in lifecycle["state_counts"]
    assert lifecycle["line_lifecycle_status"] == "terminalized_no_consumable"
    assert lifecycle["line_lifecycle_complete"] is True
    assert lifecycle["unfinished_symbol_count"] == 0
    assert lifecycle["items"][0]["symbol"] == "AAAUSDT"
    assert lifecycle["items"][0]["state"] == "rejected"
    assert lifecycle["items"][0]["trade_plan_consumable"] is False
    assert lifecycle["items"][1]["symbol"] == "BBBUSDT"
    assert lifecycle["items"][1]["state"] == "not_ready"
    assert lifecycle["items"][1]["terminal"] is True
    assert "micro_symbol_warmup_incomplete_terminalized" in lifecycle["items"][1]["reason_codes"]
    assert lifecycle["items"][0]["reason_codes"][-2:] == [
        "micro_fast_quality_ready_but_no_confirmed_symbol",
        "micro_fast_no_consumable_symbol",
    ]
    progress = json.loads((tmp_path / "DATA/runtime/strategy_pipeline_progress.json").read_text(encoding="utf-8"))
    row = progress["lines"]["micro_fast"]
    assert row["done"] is True
    assert row["percent"] == 100
    assert row["stage"] == "blocked_micro_fast_no_consumable_symbol"
    assert row["line_exec_status"] == "no_confirmed"
    assert row["line_lifecycle_status"] == "terminalized_no_consumable"
    assert row["stage_status_class"] == "business_no_signal"
    assert row["business_terminal_reason"] == "no_confirmed"
    assert row["unfinished_symbol_count"] == 0
    assert progress["overall_percent"] == 100


def test_step1047_refresh_failure_overwrites_stale_micro_fast_plan_with_error_doc(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[str] = []
    decisions_dir = tmp_path / "DATA" / "decisions"
    decisions_dir.mkdir(parents=True, exist_ok=True)
    (decisions_dir / "latest_trade_plan_micro_fast.json").write_text(
        json.dumps(
            {
                "schema_version": "10.45",
                "run_id": "old_run",
                "cycle_id": "old_cycle",
                "line": "micro_fast",
                "status": "ok",
                "count": 1,
                "executable_count": 1,
                "plans": [{"symbol": "OLDUSDT", "executable": True}],
            },
        ),
        encoding="utf-8",
    )
    micro_dir = tmp_path / "DATA" / "micro"
    micro_dir.mkdir(parents=True, exist_ok=True)
    target_generated_at = to_iso_z(utc_now())
    (micro_dir / "micro_targets.json").write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": target_generated_at,
                "target_set_id": "target_refresh_fail",
                "target_symbols": ["AAAUSDT"],
                "tier1_warm_watch": [{"symbol": "AAAUSDT"}],
                "tier2_active_strong": [],
            },
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: {"status": "running", "pid_running": True, "stale": False, "heartbeat_exists": True},
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )

    def fake_wait(**kwargs) -> int:
        calls.append("wait")
        kwargs["latest_path"].write_text(
            json.dumps(
                {
                    "schema_version": "1.6",
                    "generated_at": to_iso_z(utc_now()),
                    "status": "ok",
                    "target_generated_at": target_generated_at,
                    "target_status": "fresh",
                    "ready_count": 1,
                    "fast_ready_count": 1,
                    "full_ready_count": 0,
                    "items": [
                        {
                            "symbol": "AAAUSDT",
                            "micro_quality": {"ready": True, "reason_codes": []},
                            "micro_fast_quality": {"ready": True, "reason_codes": []},
                            "micro_fast_signal": {
                                "micro_direction_confirmed": True,
                                "micro_exec_allowed": True,
                            },
                        },
                    ],
                },
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        fake_wait,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 30,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        lambda **kwargs: calls.append(f"plan_{kwargs['line']}") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 40
    assert calls == ["pre", "wait", "assemble_with", "refresh"]
    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    doc = json.loads((decisions_dir / "latest_trade_plan_micro_fast.json").read_text(encoding="utf-8"))
    assert doc["status"] == "error"
    assert doc["run_id"] == report["run_id"]
    assert doc["cycle_id"] == report["cycle_id"]
    assert doc["executable_count"] == 0
    assert doc["plans"] == []
    assert doc["input_refs"]["failed_stage"] == "refresh_micro_fast"
    assert doc["input_refs"]["failed_rc"] == 30
    assert "refresh_rc_30" in doc["input_refs"]["reason_codes"]


def test_step1221_pipeline_progress_reconciles_terminal_lifecycle(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []
    micro_dir = tmp_path / "DATA" / "micro"
    micro_dir.mkdir(parents=True, exist_ok=True)
    (micro_dir / "micro_targets.json").write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": to_iso_z(utc_now()),
                "target_set_id": "target_set_reconcile",
                "candidate_hash": "hash_reconcile",
                "target_symbols": ["AAAUSDT", "BBBUSDT"],
                "target_count": 2,
                "tier1_warm_watch": [{"symbol": "AAAUSDT"}, {"symbol": "BBBUSDT"}],
                "tier2_active_strong": [],
            },
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: {"status": "running", "pid_running": True, "stale": False, "heartbeat_exists": True},
    )

    def fake_wait(**kwargs) -> int:
        calls.append("wait")
        latest_path = kwargs["latest_path"]
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(
            json.dumps(
                {
                    "schema_version": "1.6",
                    "generated_at": to_iso_z(utc_now()),
                    "status": "ok",
                    "target_status": "fresh",
                    "ready_count": 0,
                    "full_ready_count": 0,
                    "items": [
                        {
                            "symbol": "AAAUSDT",
                            "micro_fast_quality": {"ready": True, "reason_codes": []},
                            "micro_fast_signal": {
                                "micro_direction_confirmed": True,
                                "micro_exec_allowed": True,
                            },
                        },
                        {"symbol": "BBBUSDT", "micro_fast_quality": {"ready": False, "reason_codes": ["warmup_not_met"]}},
                    ],
                },
            ),
            encoding="utf-8",
        )
        return 0

    def fake_plan(**kwargs) -> int:
        calls.append("plan")
        line = kwargs["line"]
        lifecycle_path = tmp_path / "DATA" / "micro" / f"latest_micro_lifecycle_{line}.json"
        lifecycle_path.write_text(
            json.dumps(
                {
                    "schema_version": "10.35",
                    "source": "symbol_level_micro_lifecycle",
                    "strategy_line": line,
                    "run_id": kwargs["run_id"],
                    "cycle_id": kwargs["cycle_id"],
                    "target_set_id": "target_set_reconcile",
                    "generated_at": to_iso_z(utc_now()),
                    "count": 2,
                    "state_counts": {"rejected": 1, "not_ready": 1},
                    "items": [
                        {
                            "symbol": "AAAUSDT",
                            "state": "rejected",
                            "terminal": True,
                            "ready": True,
                            "confirmed": False,
                            "trade_plan_consumable": False,
                        },
                        {
                            "symbol": "BBBUSDT",
                            "state": "not_ready",
                            "terminal": True,
                            "ready": False,
                            "confirmed": False,
                            "trade_plan_consumable": False,
                        },
                    ],
                },
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        fake_wait,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pre_decision_candidate_refresh_safe",
        lambda **_: calls.append("refresh") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_apply_trade_plan_line_safe",
        fake_plan,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_fast",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    progress = json.loads((tmp_path / "DATA/runtime/strategy_pipeline_progress.json").read_text(encoding="utf-8"))
    row = progress["lines"]["micro_fast"]
    assert row["done"] is True
    assert row["stage"] == "completed_terminalized"
    assert row["line_lifecycle_status"] == "terminalized_no_consumable"
    assert row["unfinished_symbol_count"] == 0
    assert row["terminalized_symbol_count"] == 2
    assert row["consumable_symbol_count"] == 0
    assert row["rejected_count"] == 1
    assert row["not_ready_count"] == 1


def test_step1028_micro_full_timeout_writes_blocked_plan(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []
    micro_dir = tmp_path / "DATA" / "micro"
    micro_dir.mkdir(parents=True, exist_ok=True)
    (micro_dir / "micro_targets.json").write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": to_iso_z(utc_now()),
                "target_set_id": "target_set_x",
                "candidate_hash": "hash_x",
                "target_symbols": ["AAAUSDT"],
                "target_count": 1,
                "tier1_warm_watch": [
                    {
                        "symbol": "AAAUSDT",
                        "source_state": "watch_candidate",
                        "move_side": "up",
                        "min_collect_seconds": 900,
                    },
                ],
                "tier2_active_strong": [],
            },
        ),
        encoding="utf-8",
    )
    (micro_dir / "latest_micro_state.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "generated_at": to_iso_z(utc_now()),
                "symbols": [
                    {
                        "symbol": "AAAUSDT",
                        "continuous_collect_sec": 85,
                        "full_ready": False,
                        "full_reason_codes": ["coverage_aggtrade_weak"],
                        "consumer_safe": True,
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_pipeline_pre_micro_safe",
        lambda **_: calls.append("pre") or 0,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline._pipeline_micro_health",
        lambda _: {"status": "running", "pid_running": True, "stale": False, "heartbeat_exists": True},
    )

    def fake_wait(**kwargs) -> int:
        wait_cfg = kwargs["cfg"]
        calls.append(f"wait:{wait_cfg.mode}:{wait_cfg.min_ready_count}")
        latest_path = kwargs["latest_path"]
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(
            json.dumps(
                {
                    "schema_version": "1.6",
                    "generated_at": to_iso_z(utc_now()),
                    "status": "ok",
                    "target_generated_at": to_iso_z(utc_now()),
                    "target_status": "fresh",
                    "ws_status": "connected",
                    "last_ws_message_age_sec": 0,
                    "ready_count": 1,
                    "full_ready_count": 0,
                    "items": [
                        {
                            "symbol": "AAAUSDT",
                            "micro_quality": {"ready": True, "reason_codes": []},
                            "micro_full_quality": {
                                "ready": False,
                                "reason_codes": ["warmup_not_met"],
                            },
                            "input_refs": {"full_ready_eta_sec": 815},
                        }
                    ],
                },
            ),
            encoding="utf-8",
        )
        return EXIT_WAIT_UNTIL_READY_TIMEOUT

    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_wait_until_ready_orchestration",
        fake_wait,
    )
    monkeypatch.setattr(
        "laoma_signal_engine.strategy_pipeline.run_assemble_factor_snapshot_safe",
        lambda **_: calls.append("assemble_with") or 0,
    )

    rc = run_strategy_pipeline_safe(
        project_root=tmp_path,
        line="micro_full",
        mode="once",
        run_abc_audit=False,
        run_json_stage_audit=False,
        aggregate_final_decisions=False,
    )

    assert rc == 0
    assert calls == ["pre", "wait:min_full_ready_count:1"]
    doc = json.loads((tmp_path / "DATA/decisions/latest_trade_plan_micro_full.json").read_text(encoding="utf-8"))
    assert doc["status"] == "blocked"
    assert doc["input_refs"]["blocked_reason"] == "micro_full_wait_timeout"
    assert "full_warmup_incomplete" in doc["input_refs"]["reason_codes"]
    assert doc["input_refs"]["runtime_health"]["full_ready_count"] == 0
    assert doc["input_refs"]["runtime_health"]["max_full_ready_eta_sec"] == 815
    lifecycle = json.loads(
        (tmp_path / "DATA/micro/latest_micro_lifecycle_micro_full.json").read_text(encoding="utf-8"),
    )
    assert lifecycle["strategy_line"] == "micro_full"
    assert lifecycle["run_id"] == doc["run_id"]
    assert lifecycle["cycle_id"] == doc["cycle_id"]
    assert lifecycle["target_set_id"] == "target_set_x"
    assert lifecycle["state_counts"]["timeout"] == 1
    assert lifecycle["items"][0]["symbol"] == "AAAUSDT"
    assert lifecycle["items"][0]["state"] == "timeout"
    assert lifecycle["items"][0]["trade_plan_emitted"] is False
    assert "coverage_aggtrade_weak" in lifecycle["items"][0]["reason_codes"]
    assert "micro_full_wait_timeout" in lifecycle["items"][0]["reason_codes"]

    report = json.loads((tmp_path / "DATA/reports/latest_strategy_pipeline_report.json").read_text(encoding="utf-8"))
    wait_stage = next(s for s in report["stages"] if s["name"] == "wait_micro_ready_micro_full")
    assert wait_stage["ok"] is False
    assert wait_stage["detail"]["wait_policy"] == "strict_until_ready"
    assert wait_stage["detail"]["full_ready_count"] == 0
    assert any(s["name"] == "blocked_micro_full_wait_timeout_micro_full" for s in report["stages"])
