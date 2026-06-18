"""STEP10.6 scheduler safety gate tests."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.scheduler_5m import acquire_scheduler_lock, inspect_scheduler_lock, run_trade_plan_cycle_safe


def test_scheduler_lock_skips_when_previous_cycle_running(tmp_path: Path) -> None:
    lock = tmp_path / "scheduler.lock"
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
    got = acquire_scheduler_lock(
        lock_path=lock,
        run_id="new",
        cycle_id="cycle_new",
        ttl_sec=60,
        overlap_policy="skip",
    )
    assert got.acquired is False
    assert got.skipped is True
    assert got.reason == "scheduler_skipped_previous_cycle_running"


def test_scheduler_lock_recovers_dead_pid_stale_lock(tmp_path: Path) -> None:
    lock = tmp_path / "scheduler.lock"
    now = utc_now()
    lock.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "lock_owner_pid": 999999,
                "run_id": "old",
                "cycle_id": "old",
                "started_at": to_iso_z(now),
                "expires_at": to_iso_z(now + timedelta(minutes=5)),
                "stage": "test",
            },
        ),
        encoding="utf-8",
    )

    before = inspect_scheduler_lock(lock)
    got = acquire_scheduler_lock(
        lock_path=lock,
        run_id="new",
        cycle_id="cycle_new",
        ttl_sec=60,
        overlap_policy="skip",
    )

    assert before["lock_stale"] is True
    assert "pipeline_lock_stale_dead_pid" in before["reason_codes"]
    assert got.acquired is True
    assert got.reason == "pipeline_lock_stale_auto_recovered"
    raw = json.loads(lock.read_text(encoding="utf-8"))
    assert raw["run_id"] == "new"


def test_scheduler_cycle_skipped_report_does_not_require_data(tmp_path: Path) -> None:
    lock = tmp_path / "scheduler.lock"
    report = tmp_path / "report.json"
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
    code = run_trade_plan_cycle_safe(
        project_root=tmp_path,
        lock_path=lock,
        report_path=report,
        overlap_policy="skip",
    )
    assert code == 0
    raw = json.loads(report.read_text(encoding="utf-8"))
    assert raw["status"] == "skipped"
    assert raw["skip_reason"] == "scheduler_skipped_previous_cycle_running"
