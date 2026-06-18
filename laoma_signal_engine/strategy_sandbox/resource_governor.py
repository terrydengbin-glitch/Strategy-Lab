"""P31 sandbox resource lane governor.

This module is intentionally a thin file-backed contract layer. It does not
replace the existing strategy pipeline lock; later P31 tasks can wrap concrete
runners with these lane locks.
"""

from __future__ import annotations

import ctypes
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import orjson

from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.core.time_utils import parse_iso_z, to_iso_z, utc_now
from laoma_signal_engine.market.rest_circuit import read_rest_circuit
from laoma_signal_engine.strategy_sandbox.writer_context import create_writer_context

SCHEMA_VERSION = "STEP31.2_resource_governor_v2"
RUNTIME_REL = Path("DATA/runtime/sandbox_resource_governor")

UI_ACTIVE_SANDBOX_LANE = "ui_active_sandbox_real_pipeline"
EXTERNAL_CLI_RESEARCH_LANE = "external_cli_research_lane"

LANE_POLICIES: dict[str, dict[str, Any]] = {
    UI_ACTIVE_SANDBOX_LANE: {
        "max_parallelism": 1,
        "active_context_required": True,
        "daemon_access_policy": "exclusive_real_daemon_control",
        "cancel_policy": "cancel_on_active_switch",
        "rest_budget_policy": "shared_binance_rest_circuit",
    },
    EXTERNAL_CLI_RESEARCH_LANE: {
        "max_parallelism": 1,
        "active_context_required": False,
        "daemon_access_policy": "cache_first_research_no_ui_daemon_control",
        "cancel_policy": "manual_or_runner_lifecycle",
        "rest_budget_policy": "shared_binance_rest_circuit_cache_first",
    },
}


class ResourceLaneError(ValueError):
    """Raised when a resource lane request violates the P31 contract."""


def governor_dir(project_root: Path | None = None) -> Path:
    return ((project_root or Path.cwd()).resolve() / RUNTIME_REL).resolve()


def lane_lock_path(resource_lane: str, project_root: Path | None = None) -> Path:
    return governor_dir(project_root) / f"{_validate_lane(resource_lane)}.lock.json"


def lane_events_path(project_root: Path | None = None) -> Path:
    return governor_dir(project_root) / "events.jsonl"


def lane_runs_path(project_root: Path | None = None) -> Path:
    return governor_dir(project_root) / "runs.jsonl"


def _validate_lane(resource_lane: str) -> str:
    lane = str(resource_lane or "").strip()
    if lane not in LANE_POLICIES:
        raise ResourceLaneError(f"unsupported_resource_lane: {lane}")
    return lane


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


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        raw = orjson.loads(path.read_bytes())
    except (OSError, orjson.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    write_file_atomic(path, orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE))


def _append_event(project_root: Path | None, event: dict[str, Any]) -> None:
    path = lane_events_path(project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "ab") as f:
        f.write(orjson.dumps(event, option=orjson.OPT_APPEND_NEWLINE))


def _append_run_record(project_root: Path | None, record: dict[str, Any]) -> None:
    path = lane_runs_path(project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "ab") as f:
        f.write(orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_bytes().splitlines()
    except OSError:
        return []
    out: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            row = orjson.loads(line)
        except orjson.JSONDecodeError:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def _lock_stale(payload: dict[str, Any] | None, *, now: datetime | None = None) -> tuple[bool, list[str]]:
    if not isinstance(payload, dict):
        return True, ["lane_lock_malformed"]
    reasons: list[str] = []
    pid = _safe_int(payload.get("owner_pid"))
    pid_running = _pid_running(pid)
    expires_at = payload.get("expires_at")
    expired = False
    if isinstance(expires_at, str):
        try:
            expired = parse_iso_z(expires_at) <= (now or utc_now())
        except (TypeError, ValueError):
            expired = True
    else:
        expired = True
    if pid and not pid_running:
        reasons.append("lane_lock_stale_dead_pid")
    if expired:
        reasons.append("lane_lock_expired")
    if not reasons and pid_running:
        reasons.append("lane_lock_alive_busy")
    return bool("lane_lock_stale_dead_pid" in reasons or expired), reasons


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def rest_budget_snapshot(
    project_root: Path | None = None,
    *,
    requires_live_rest: bool = False,
    cache_hit: bool = False,
) -> dict[str, Any]:
    root = (project_root or Path.cwd()).resolve()
    circuit = read_rest_circuit(root)
    live_allowed = bool(circuit.get("live_rest_allowed"))
    effective_requires_live = bool(requires_live_rest and not cache_hit)
    return {
        "schema_version": SCHEMA_VERSION,
        "rest_budget_policy": "shared_binance_rest_circuit",
        "rest_circuit_state": circuit.get("rest_circuit_state"),
        "rest_circuit_until": circuit.get("rest_circuit_until"),
        "rest_circuit_remaining_sec": circuit.get("rest_circuit_remaining_sec"),
        "live_rest_allowed": live_allowed,
        "requires_live_rest": bool(requires_live_rest),
        "cache_hit": bool(cache_hit),
        "cache_hit_bypasses_live_rest_budget": bool(cache_hit),
        "live_rest_available": bool(live_allowed or not effective_requires_live),
        "block_reason": None if live_allowed or not effective_requires_live else "rest_circuit_live_rest_unavailable",
        "source_path": circuit.get("source_path"),
    }


def acquire_lane(
    resource_lane: str,
    *,
    project_root: Path | None = None,
    sandbox_id: str,
    run_id: str,
    cycle_id: str | None = None,
    caller_surface: str = "fastapi",
    caller_type: str = "local_ui",
    ttl_sec: int = 3600,
    requires_live_rest: bool = False,
    cache_hit: bool = False,
    active_context_at_start: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lane = _validate_lane(resource_lane)
    policy = LANE_POLICIES[lane]
    rest_budget = rest_budget_snapshot(project_root, requires_live_rest=requires_live_rest, cache_hit=cache_hit)
    if requires_live_rest and not rest_budget["live_rest_available"]:
        return {
            "schema_version": SCHEMA_VERSION,
            "resource_lane": lane,
            "acquired": False,
            "status": "blocked",
            "reason_code": "rest_circuit_live_rest_unavailable",
            "rest_budget": rest_budget,
            "policy": policy,
        }
    if policy["active_context_required"] and not active_context_at_start:
        return {
            "schema_version": SCHEMA_VERSION,
            "resource_lane": lane,
            "acquired": False,
            "status": "blocked",
            "reason_code": "active_sandbox_context_required",
            "rest_budget": rest_budget,
            "policy": policy,
        }
    path = lane_lock_path(lane, project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    now = utc_now()
    lock_id = f"lane_{lane}_{run_id}"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "resource_lane": lane,
        "lane_lock_id": lock_id,
        "sandbox_id": str(sandbox_id),
        "run_id": str(run_id),
        "cycle_id": cycle_id,
        "caller_surface": str(caller_surface),
        "caller_type": str(caller_type),
        "status": "running",
        "owner_pid": os.getpid(),
        "started_at": to_iso_z(now),
        "heartbeat_at": to_iso_z(now),
        "expires_at": to_iso_z(now + timedelta(seconds=max(1, int(ttl_sec)))),
        "rest_budget_policy": policy["rest_budget_policy"],
        "max_parallelism": policy["max_parallelism"],
        "cancel_policy": policy["cancel_policy"],
        "daemon_access_policy": policy["daemon_access_policy"],
        "active_context_required": policy["active_context_required"],
        "active_context_at_start": active_context_at_start,
        "rest_circuit_state_at_start": rest_budget["rest_circuit_state"],
        "metadata": metadata or {},
    }
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(str(path), flags)
    except FileExistsError:
        existing = _read_json(path)
        stale, reasons = _lock_stale(existing, now=now)
        if stale:
            path.unlink(missing_ok=True)
            fd = os.open(str(path), flags)
            recovered = True
        else:
            return {
                "schema_version": SCHEMA_VERSION,
                "resource_lane": lane,
                "acquired": False,
                "status": "busy",
                "reason_code": "resource_lane_already_running",
                "reason_codes": reasons,
                "active_lock": existing,
                "rest_budget": rest_budget,
                "policy": policy,
            }
    else:
        recovered = False
    with os.fdopen(fd, "wb") as f:
        f.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE))
    event = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "lane_acquired",
        "resource_lane": lane,
        "lane_lock_id": lock_id,
        "sandbox_id": sandbox_id,
        "run_id": run_id,
        "status": "acquired",
        "recovered_stale_lock": recovered,
        "created_at": to_iso_z(now),
    }
    _append_event(project_root, event)
    return {
        "schema_version": SCHEMA_VERSION,
        "resource_lane": lane,
        "acquired": True,
        "status": "running",
        "lane_lock_id": lock_id,
        "lock_path": str(path),
        "lock": payload,
        "recovered_stale_lock": recovered,
        "rest_budget": rest_budget,
        "policy": policy,
    }


def _training_readiness_placeholder(*, run_id: str, source: str) -> dict[str, Any]:
    return {
        "training_dataset_status": "incomplete",
        "allowed_for_training": False,
        "source": source,
        "reason_codes": ["trade_quality_completion_required", "p29_sidecar_materialization_required"],
        "trade_quality_label_source": "trade_quality_module_required",
        "decision_time_input_policy": "post_trade_outcome_forbidden",
        "run_id": run_id,
    }


def _run_id(prefix: str) -> str:
    return f"{prefix}_{utc_now().strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:10]}"


def start_ui_sandbox_pipeline_context(
    *,
    project_root: Path | None = None,
    sandbox_id: str | None,
    active_sandbox_id: str | None,
    caller_surface: str = "fastapi",
    caller_type: str = "local_ui",
    dry_run: bool = True,
    requires_live_rest: bool = False,
    cache_hit: bool = False,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not active_sandbox_id:
        return {
            "schema_version": SCHEMA_VERSION,
            "accepted": False,
            "status": "blocked",
            "reason_code": "active_sandbox_context_required",
            "resource_lane": UI_ACTIVE_SANDBOX_LANE,
        }
    requested = str(sandbox_id or active_sandbox_id)
    if requested != str(active_sandbox_id):
        return {
            "schema_version": SCHEMA_VERSION,
            "accepted": False,
            "status": "blocked",
            "reason_code": "active_sandbox_mismatch",
            "requested_sandbox_id": requested,
            "active_sandbox_id": active_sandbox_id,
            "resource_lane": UI_ACTIVE_SANDBOX_LANE,
        }
    run_id = str((options or {}).get("run_id") or _run_id("uisbx"))
    cycle_id = str((options or {}).get("cycle_id") or f"cycle_{run_id}")
    acquired = acquire_lane(
        UI_ACTIVE_SANDBOX_LANE,
        project_root=project_root,
        sandbox_id=requested,
        run_id=run_id,
        cycle_id=cycle_id,
        caller_surface=caller_surface,
        caller_type=caller_type,
        requires_live_rest=requires_live_rest,
        cache_hit=cache_hit,
        active_context_at_start=active_sandbox_id,
        metadata={"dry_run": bool(dry_run), "options": options or {}},
    )
    accepted = bool(acquired.get("acquired"))
    writer_context: dict[str, Any] | None = None
    writer_error: str | None = None
    if accepted:
        try:
            writer_context = create_writer_context(
                project_root=project_root,
                sandbox_id=requested,
                resource_lane=UI_ACTIVE_SANDBOX_LANE,
                run_id=run_id,
                cycle_id=cycle_id,
                source_chain="ui_active_sandbox_real_pipeline",
                writer_target="sandbox_db",
                strategy_line=(options or {}).get("strategy_line"),
                strategy_id=(options or {}).get("strategy_id"),
                symbol=(options or {}).get("symbol"),
                training_dataset_id=(options or {}).get("training_dataset_id"),
            )
        except Exception as exc:
            writer_error = str(exc)
            release_lane(UI_ACTIVE_SANDBOX_LANE, project_root=project_root, run_id=run_id, status="writer_context_failed")
            accepted = False
    status = (
        "blocked"
        if writer_error
        else ("dry_run_scaffold_ready" if dry_run and accepted else ("sandbox_writer_guard_ready" if accepted else acquired.get("status", "blocked")))
    )
    record = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "run_started" if accepted else "run_blocked",
        "resource_lane": UI_ACTIVE_SANDBOX_LANE,
        "sandbox_id": requested,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "caller_surface": caller_surface,
        "caller_type": caller_type,
        "status": status,
        "accepted": accepted,
        "dry_run": bool(dry_run),
        "reason_code": None if accepted else (writer_error and "sandbox_writer_context_failed") or acquired.get("reason_code"),
        "reason_codes": [] if accepted else ([str(writer_error)] if writer_error else []),
        "active_context_at_start": active_sandbox_id,
        "daemon_access_policy": LANE_POLICIES[UI_ACTIVE_SANDBOX_LANE]["daemon_access_policy"],
        "rest_budget": acquired.get("rest_budget"),
        "writer_context": writer_context,
        "writer_target": (writer_context or {}).get("writer_targets") if writer_context else None,
        "training_readiness": _training_readiness_placeholder(run_id=run_id, source="ui_active_sandbox_real_pipeline"),
        "created_at": to_iso_z(utc_now()),
    }
    _append_run_record(project_root, record)
    return {
        "schema_version": SCHEMA_VERSION,
        "accepted": accepted,
        "status": status,
        "resource_lane": UI_ACTIVE_SANDBOX_LANE,
        "sandbox_id": requested,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "dry_run": bool(dry_run),
        "lane": acquired,
        "writer_context": writer_context,
        "writer_target": (writer_context or {}).get("writer_targets") if writer_context else None,
        "training_readiness": record["training_readiness"],
        "reason_code": record["reason_code"],
        "reason_codes": record["reason_codes"],
    }


def stop_ui_sandbox_pipeline_context(
    *,
    project_root: Path | None = None,
    run_id: str | None = None,
    cancel_reason: str = "manual_stop",
) -> dict[str, Any]:
    status_before = governor_status(project_root)["lanes"][UI_ACTIVE_SANDBOX_LANE]
    canceled = cancel_lane(UI_ACTIVE_SANDBOX_LANE, project_root=project_root, run_id=run_id, cancel_reason=cancel_reason)
    lock = status_before.get("active_lock") if isinstance(status_before, dict) else None
    record = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "run_canceled",
        "resource_lane": UI_ACTIVE_SANDBOX_LANE,
        "sandbox_id": (lock or {}).get("sandbox_id"),
        "run_id": run_id or (lock or {}).get("run_id"),
        "cycle_id": (lock or {}).get("cycle_id"),
        "status": "canceled" if canceled.get("canceled") or canceled.get("released") else canceled.get("status"),
        "cancel_reason": cancel_reason,
        "created_at": to_iso_z(utc_now()),
    }
    _append_run_record(project_root, record)
    return {**canceled, "record": record}


def finish_ui_sandbox_pipeline_context(
    *,
    project_root: Path | None = None,
    run_id: str,
    sandbox_id: str,
    status: str = "completed",
    result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    release = release_lane(UI_ACTIVE_SANDBOX_LANE, project_root=project_root, run_id=run_id, status=status)
    record = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "run_finished",
        "resource_lane": UI_ACTIVE_SANDBOX_LANE,
        "sandbox_id": sandbox_id,
        "run_id": run_id,
        "status": status,
        "result": result or {},
        "training_readiness": (result or {}).get("training_dataset")
        or _training_readiness_placeholder(run_id=run_id, source="ui_active_sandbox_real_pipeline"),
        "created_at": to_iso_z(utc_now()),
    }
    _append_run_record(project_root, record)
    return {**release, "record": record}


def start_external_research_context(
    *,
    project_root: Path | None = None,
    sandbox_id: str,
    run_id: str | None = None,
    caller_surface: str = "cli",
    caller_type: str = "external_cli",
    requires_live_rest: bool = False,
    cache_hit: bool = True,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rid = run_id or _run_id("clisbx")
    acquired = acquire_lane(
        EXTERNAL_CLI_RESEARCH_LANE,
        project_root=project_root,
        sandbox_id=sandbox_id,
        run_id=rid,
        cycle_id=f"cycle_{rid}",
        caller_surface=caller_surface,
        caller_type=caller_type,
        requires_live_rest=requires_live_rest,
        cache_hit=cache_hit,
        metadata={"cache_first": True, "options": options or {}},
    )
    accepted = bool(acquired.get("acquired"))
    writer_context: dict[str, Any] | None = None
    writer_error: str | None = None
    if accepted:
        try:
            writer_context = create_writer_context(
                project_root=project_root,
                sandbox_id=sandbox_id,
                resource_lane=EXTERNAL_CLI_RESEARCH_LANE,
                run_id=rid,
                cycle_id=f"cycle_{rid}",
                source_chain="external_cli_research_lane",
                writer_target="sandbox_db",
                strategy_line=(options or {}).get("strategy_line"),
                strategy_id=(options or {}).get("strategy_id"),
                symbol=(options or {}).get("symbol"),
                training_dataset_id=(options or {}).get("training_dataset_id"),
            )
        except Exception as exc:
            writer_error = str(exc)
            release_lane(EXTERNAL_CLI_RESEARCH_LANE, project_root=project_root, run_id=rid, status="writer_context_failed")
            accepted = False
    record = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "run_started" if accepted else "run_blocked",
        "resource_lane": EXTERNAL_CLI_RESEARCH_LANE,
        "sandbox_id": sandbox_id,
        "run_id": rid,
        "cycle_id": f"cycle_{rid}",
        "caller_surface": caller_surface,
        "caller_type": caller_type,
        "status": "running" if accepted else ("blocked" if writer_error else acquired.get("status", "blocked")),
        "accepted": accepted,
        "cache_first": True,
        "reason_code": None if accepted else (writer_error and "sandbox_writer_context_failed") or acquired.get("reason_code"),
        "reason_codes": [] if accepted else ([str(writer_error)] if writer_error else []),
        "daemon_access_policy": LANE_POLICIES[EXTERNAL_CLI_RESEARCH_LANE]["daemon_access_policy"],
        "rest_budget": acquired.get("rest_budget"),
        "writer_context": writer_context,
        "writer_target": (writer_context or {}).get("writer_targets") if writer_context else None,
        "training_readiness": _training_readiness_placeholder(run_id=rid, source="external_cli_research_lane"),
        "created_at": to_iso_z(utc_now()),
    }
    _append_run_record(project_root, record)
    return {
        "schema_version": SCHEMA_VERSION,
        "accepted": accepted,
        "status": record["status"],
        "resource_lane": EXTERNAL_CLI_RESEARCH_LANE,
        "sandbox_id": sandbox_id,
        "run_id": rid,
        "lane": acquired,
        "writer_context": writer_context,
        "writer_target": (writer_context or {}).get("writer_targets") if writer_context else None,
        "training_readiness": record["training_readiness"],
        "reason_code": record["reason_code"],
        "reason_codes": record["reason_codes"],
    }


def finish_external_research_context(
    *,
    project_root: Path | None = None,
    run_id: str,
    sandbox_id: str,
    status: str = "completed",
    result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    release = release_lane(EXTERNAL_CLI_RESEARCH_LANE, project_root=project_root, run_id=run_id, status=status)
    record = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "run_finished",
        "resource_lane": EXTERNAL_CLI_RESEARCH_LANE,
        "sandbox_id": sandbox_id,
        "run_id": run_id,
        "status": status,
        "result": result or {},
        "training_readiness": _training_readiness_placeholder(run_id=run_id, source="external_cli_research_lane"),
        "created_at": to_iso_z(utc_now()),
    }
    _append_run_record(project_root, record)
    return {**release, "record": record}


def heartbeat_lane(
    resource_lane: str,
    *,
    project_root: Path | None = None,
    ttl_sec: int = 3600,
) -> dict[str, Any]:
    lane = _validate_lane(resource_lane)
    path = lane_lock_path(lane, project_root)
    payload = _read_json(path)
    if not payload:
        return {"schema_version": SCHEMA_VERSION, "resource_lane": lane, "updated": False, "status": "missing"}
    now = utc_now()
    payload["heartbeat_at"] = to_iso_z(now)
    payload["expires_at"] = to_iso_z(now + timedelta(seconds=max(1, int(ttl_sec))))
    _write_json(path, payload)
    return {"schema_version": SCHEMA_VERSION, "resource_lane": lane, "updated": True, "status": payload.get("status"), "lock": payload}


def release_lane(
    resource_lane: str,
    *,
    project_root: Path | None = None,
    run_id: str | None = None,
    status: str = "released",
) -> dict[str, Any]:
    lane = _validate_lane(resource_lane)
    path = lane_lock_path(lane, project_root)
    payload = _read_json(path)
    if run_id and payload and str(payload.get("run_id")) != str(run_id):
        return {
            "schema_version": SCHEMA_VERSION,
            "resource_lane": lane,
            "released": False,
            "status": "mismatch",
            "reason_code": "lane_run_id_mismatch",
            "active_lock": payload,
        }
    path.unlink(missing_ok=True)
    _append_event(
        project_root,
        {
            "schema_version": SCHEMA_VERSION,
            "event_type": "lane_released",
            "resource_lane": lane,
            "run_id": run_id or (payload or {}).get("run_id"),
            "status": status,
            "created_at": to_iso_z(utc_now()),
        },
    )
    return {"schema_version": SCHEMA_VERSION, "resource_lane": lane, "released": True, "status": status}


def cancel_lane(
    resource_lane: str,
    *,
    project_root: Path | None = None,
    run_id: str | None = None,
    cancel_reason: str = "manual_cancel",
) -> dict[str, Any]:
    lane = _validate_lane(resource_lane)
    path = lane_lock_path(lane, project_root)
    payload = _read_json(path)
    if not payload:
        return {"schema_version": SCHEMA_VERSION, "resource_lane": lane, "canceled": False, "status": "missing"}
    if run_id and str(payload.get("run_id")) != str(run_id):
        return {
            "schema_version": SCHEMA_VERSION,
            "resource_lane": lane,
            "canceled": False,
            "status": "mismatch",
            "reason_code": "lane_run_id_mismatch",
            "active_lock": payload,
        }
    payload["status"] = "canceled"
    payload["cancel_reason"] = cancel_reason
    payload["canceled_at"] = to_iso_z(utc_now())
    _write_json(path, payload)
    release = release_lane(lane, project_root=project_root, run_id=str(payload.get("run_id")), status="canceled")
    release["canceled"] = True
    release["cancel_reason"] = cancel_reason
    return release


def governor_status(project_root: Path | None = None) -> dict[str, Any]:
    root = (project_root or Path.cwd()).resolve()
    lanes: dict[str, Any] = {}
    for lane, policy in LANE_POLICIES.items():
        path = lane_lock_path(lane, root)
        payload = _read_json(path)
        stale, reasons = _lock_stale(payload) if payload else (False, [])
        lanes[lane] = {
            "resource_lane": lane,
            "policy": policy,
            "lock_exists": bool(payload),
            "status": payload.get("status") if payload else "idle",
            "lock_stale": stale,
            "reason_codes": reasons,
            "active_lock": payload,
            "lock_path": str(path),
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "sandbox_resource_governor",
        "project_root": str(root),
        "lanes": lanes,
        "rest_budget": rest_budget_snapshot(root),
        "events_path": str(lane_events_path(root)),
        "runs_path": str(lane_runs_path(root)),
    }


def resource_runs_payload(
    project_root: Path | None = None,
    *,
    resource_lane: str | None = None,
    sandbox_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    root = (project_root or Path.cwd()).resolve()
    rows = _read_jsonl(lane_runs_path(root))
    if resource_lane:
        rows = [row for row in rows if row.get("resource_lane") == resource_lane]
    if sandbox_id:
        rows = [row for row in rows if row.get("sandbox_id") == sandbox_id]
    rows = list(reversed(rows))[: max(1, min(1000, int(limit)))]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "sandbox_resource_governor_runs",
        "runs": rows,
        "count": len(rows),
        "runs_path": str(lane_runs_path(root)),
    }


def resource_run_payload(project_root: Path | None = None, *, run_id: str) -> dict[str, Any]:
    root = (project_root or Path.cwd()).resolve()
    rows = [row for row in _read_jsonl(lane_runs_path(root)) if str(row.get("run_id")) == str(run_id)]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "sandbox_resource_governor_run",
        "run_id": run_id,
        "events": rows,
        "count": len(rows),
        "latest": rows[-1] if rows else None,
        "runs_path": str(lane_runs_path(root)),
    }
