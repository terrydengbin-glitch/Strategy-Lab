"""STEP10.6 5-minute scheduler safety gate for P10 trade plan lines."""

from __future__ import annotations

import json
import os
import sys
import time
import ctypes
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import parse_iso_z, to_iso_z, utc_now
from laoma_signal_engine.decision.trade_plan_lines import run_apply_trade_plan_line_safe
from laoma_signal_engine.decision.trade_plan_lines_audit import run_audit_trade_plan_lines_safe
from laoma_signal_engine.paper.config import load_paper_config
from laoma_signal_engine.paper.daemon import run_once as run_paper_once

OverlapPolicy = Literal["skip", "merge"]


@dataclass(frozen=True)
class SchedulerLock:
    lock_path: Path
    run_id: str
    cycle_id: str
    acquired: bool
    skipped: bool = False
    reason: str | None = None


def default_lock_path(root: Path) -> Path:
    return root / "DATA" / "runtime" / "scheduler_5m.lock"


def default_report_path(root: Path) -> Path:
    return root / "DATA" / "reports" / "latest_scheduler_5m_report.json"


def _make_run_id(now: datetime) -> str:
    return now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def _lock_payload(*, run_id: str, cycle_id: str, started_at: str, ttl_sec: int, stage: str) -> dict[str, Any]:
    start = parse_iso_z(started_at)
    return {
        "schema_version": "1.0",
        "lock_owner_pid": os.getpid(),
        "run_id": run_id,
        "cycle_id": cycle_id,
        "started_at": started_at,
        "expires_at": to_iso_z(start + timedelta(seconds=ttl_sec)),
        "stage": stage,
    }


def _is_lock_expired(path: Path, now: datetime) -> bool:
    try:
        raw = read_json_object(path)
        exp = raw.get("expires_at") if isinstance(raw, dict) else None
        if not isinstance(exp, str):
            return True
        return parse_iso_z(exp) <= now
    except (OSError, ValueError, TypeError):
        return True


def _lock_owner_pid(payload: dict[str, Any] | None) -> int | None:
    if not isinstance(payload, dict):
        return None
    try:
        return int(payload.get("lock_owner_pid"))
    except (TypeError, ValueError):
        return None


def _pid_running(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    if os.name == "nt":
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def inspect_scheduler_lock(path: Path, *, now: datetime | None = None) -> dict[str, Any]:
    payload: dict[str, Any] | None = None
    exists = path.exists()
    malformed = False
    if exists:
        try:
            raw = read_json_object(path)
            payload = raw if isinstance(raw, dict) else None
            malformed = payload is None
        except (OSError, ValueError, TypeError):
            malformed = True
    pid = _lock_owner_pid(payload)
    pid_running = _pid_running(pid)
    expired = False
    expires_at = payload.get("expires_at") if isinstance(payload, dict) else None
    if isinstance(expires_at, str):
        try:
            expired = parse_iso_z(expires_at) <= (now or utc_now())
        except (TypeError, ValueError):
            expired = True
    elif exists:
        expired = True
    stale_dead_pid = bool(exists and pid and not pid_running)
    stale = bool(exists and (malformed or expired or stale_dead_pid))
    reason_codes: list[str] = []
    if malformed:
        reason_codes.append("pipeline_lock_malformed")
    if stale_dead_pid:
        reason_codes.append("pipeline_lock_stale_dead_pid")
    if expired:
        reason_codes.append("pipeline_lock_expired")
    if exists and pid_running and not stale:
        reason_codes.append("pipeline_lock_alive_busy")
    return {
        "lock_exists": exists,
        "path": str(path),
        "payload": payload,
        "lock_owner_pid": pid,
        "lock_pid_running": pid_running,
        "lock_stale": stale,
        "lock_expired": expired,
        "lock_malformed": malformed,
        "reason_codes": reason_codes,
    }


def _is_lock_recoverable_stale(path: Path, now: datetime) -> bool:
    state = inspect_scheduler_lock(path, now=now)
    return bool(state.get("lock_exists") and state.get("lock_stale") and not state.get("lock_pid_running"))


def acquire_scheduler_lock(
    *,
    lock_path: Path,
    run_id: str,
    cycle_id: str,
    ttl_sec: int,
    overlap_policy: OverlapPolicy = "skip",
) -> SchedulerLock:
    now = utc_now()
    started_at = to_iso_z(now)
    payload = _lock_payload(
        run_id=run_id,
        cycle_id=cycle_id,
        started_at=started_at,
        ttl_sec=ttl_sec,
        stage="acquire",
    )
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(str(lock_path), flags)
    except FileExistsError:
        if _is_lock_recoverable_stale(lock_path, now):
            lock_path.unlink(missing_ok=True)
            fd = os.open(str(lock_path), flags)
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.write("\n")
            return SchedulerLock(lock_path, run_id, cycle_id, acquired=True, reason="pipeline_lock_stale_auto_recovered")
        if overlap_policy == "skip":
            return SchedulerLock(
                lock_path,
                run_id,
                cycle_id,
                acquired=False,
                skipped=True,
                reason="scheduler_skipped_previous_cycle_running",
            )
        return SchedulerLock(
            lock_path,
            run_id,
            cycle_id,
            acquired=False,
            skipped=True,
            reason="scheduler_merge_not_implemented",
        )
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return SchedulerLock(lock_path, run_id, cycle_id, acquired=True)


def release_scheduler_lock(lock: SchedulerLock) -> None:
    if lock.acquired:
        lock.lock_path.unlink(missing_ok=True)


def _write_report(path: Path, report: dict[str, Any]) -> None:
    write_json_atomic(path, report)


def _target_generated_at(root: Path) -> str | None:
    p = root / "DATA" / "micro" / "micro_targets.json"
    if not p.is_file():
        return None
    try:
        raw = read_json_object(p)
    except (OSError, ValueError, TypeError):
        return None
    if isinstance(raw, dict) and isinstance(raw.get("generated_at"), str):
        return raw["generated_at"]
    return None


def _state_target_acked(root: Path, target_generated_at: str | None) -> bool:
    if target_generated_at is None:
        return False
    p = root / "DATA" / "micro" / "latest_micro_state.json"
    if not p.is_file():
        return False
    try:
        raw = read_json_object(p)
    except (OSError, ValueError, TypeError):
        return False
    return isinstance(raw, dict) and raw.get("target_generated_at") == target_generated_at


def wait_micro_state_target_ack(root: Path, *, timeout_sec: float = 10.0, poll_sec: float = 1.0) -> bool:
    target_gen = _target_generated_at(root)
    deadline = time.monotonic() + max(0.0, timeout_sec)
    while time.monotonic() <= deadline:
        if _state_target_acked(root, target_gen):
            return True
        time.sleep(max(0.05, poll_sec))
    return False


def run_trade_plan_cycle_safe(
    *,
    project_root: Path | None = None,
    lock_path: Path | None = None,
    report_path: Path | None = None,
    overlap_policy: OverlapPolicy = "skip",
    lock_ttl_sec: int = 540,
    wait_target_ack: bool = False,
    target_ack_timeout_sec: float = 10.0,
    stdout_json: bool = False,
) -> int:
    root = (project_root or Path.cwd()).resolve()
    now = utc_now()
    run_id = _make_run_id(now)
    cycle_id = f"cycle_{run_id}"
    lp = lock_path or default_lock_path(root)
    rp = report_path or default_report_path(root)
    started_at = to_iso_z(now)
    lock = acquire_scheduler_lock(
        lock_path=lp,
        run_id=run_id,
        cycle_id=cycle_id,
        ttl_sec=lock_ttl_sec,
        overlap_policy=overlap_policy,
    )
    stages: list[dict[str, Any]] = []
    if not lock.acquired:
        report = {
            "schema_version": "1.0",
            "generated_at": to_iso_z(utc_now()),
            "source": "scheduler_5m",
            "run_id": run_id,
            "cycle_id": cycle_id,
            "status": "skipped",
            "overlap_policy": overlap_policy,
            "started_at": started_at,
            "finished_at": to_iso_z(utc_now()),
            "duration_sec": 0,
            "stages": [],
            "outputs": {},
            "skip_reason": lock.reason,
        }
        _write_report(rp, report)
        if stdout_json:
            print(json.dumps({"step": "STEP10.6", "status": "skipped", "reason": lock.reason}, ensure_ascii=False))
        return EXIT_SUCCESS
    try:
        acked = None
        if wait_target_ack:
            t0 = time.monotonic()
            acked = wait_micro_state_target_ack(root, timeout_sec=target_ack_timeout_sec)
            stages.append({"name": "wait_micro_state_target_ack", "ok": acked, "duration_sec": round(time.monotonic() - t0, 3)})

        outputs: dict[str, str] = {}
        for line in ("without_micro", "micro_fast", "micro_full"):
            t0 = time.monotonic()
            rc = run_apply_trade_plan_line_safe(
                line=line,  # type: ignore[arg-type]
                project_root=root,
                run_id=run_id,
                cycle_id=cycle_id,
            )
            stages.append({"name": f"apply_trade_plan_{line}", "ok": rc == EXIT_SUCCESS, "rc": rc, "duration_sec": round(time.monotonic() - t0, 3)})
            outputs[line] = str(root / "DATA" / "decisions" / f"latest_trade_plan_{line}.json")
            if rc != EXIT_SUCCESS:
                status = "partial"
                break
            t1 = time.monotonic()
            try:
                paper_result = run_paper_once(root, config=load_paper_config(root))
                paper_ok = str(paper_result.get("status") or "ok") != "error" if isinstance(paper_result, dict) else True
            except Exception:
                paper_ok = False
            stages.append({"name": f"paper_wakeup_{line}", "ok": paper_ok, "duration_sec": round(time.monotonic() - t1, 3)})
        else:
            t0 = time.monotonic()
            audit_rc = run_audit_trade_plan_lines_safe(project_root=root)
            stages.append({"name": "audit_trade_plan_lines", "ok": audit_rc == EXIT_SUCCESS, "rc": audit_rc, "duration_sec": round(time.monotonic() - t0, 3)})
            outputs["abc_audit"] = str(root / "DATA" / "reports" / "latest_trade_plan_lines_compare.json")
            status = "ok" if audit_rc == EXIT_SUCCESS else "partial"

        finished_at = to_iso_z(utc_now())
        duration = max(0, int((parse_iso_z(finished_at) - parse_iso_z(started_at)).total_seconds()))
        report = {
            "schema_version": "1.0",
            "generated_at": finished_at,
            "source": "scheduler_5m",
            "run_id": run_id,
            "cycle_id": cycle_id,
            "status": status,
            "overlap_policy": overlap_policy,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_sec": duration,
            "target_ack_ok": acked,
            "stages": stages,
            "outputs": outputs,
            "skip_reason": None,
        }
        _write_report(rp, report)
        if stdout_json:
            print(json.dumps({"step": "STEP10.6", "status": status, "run_id": run_id, "output": str(rp)}, ensure_ascii=False))
        return EXIT_SUCCESS if status == "ok" else EXIT_CONFIG
    except Exception as e:
        print(f"[ERROR] scheduler 5m cycle failed: {e}", file=sys.stderr)
        return EXIT_INTERNAL
    finally:
        release_scheduler_lock(lock)
