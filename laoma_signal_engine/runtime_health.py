"""Runtime health helpers shared by API and strategy pipeline."""

from __future__ import annotations

import csv
import os
import subprocess
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.time_utils import parse_iso_z, utc_now


def pid_running(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {int(pid)}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=3,
                check=False,
            )
        except (OSError, subprocess.SubprocessError, ValueError):
            return False
        for row in csv.reader((result.stdout or "").splitlines()):
            if len(row) >= 2 and row[1].strip() == str(int(pid)):
                return True
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def read_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = read_json_object(path)
    except (OSError, ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def json_pid(path: Path) -> int | None:
    data = read_optional_json(path)
    if not isinstance(data, dict):
        return None
    try:
        return int(data.get("pid"))
    except (TypeError, ValueError):
        return None


def iso_from_payload(payload: dict[str, Any] | None, *keys: str) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def age_sec(iso_text: str | None) -> int | None:
    if not iso_text:
        return None
    try:
        return max(0, int((utc_now() - parse_iso_z(iso_text)).total_seconds()))
    except Exception:
        return None


def micro_daemon_health(
    *,
    pid_path: Path,
    heartbeat_path: Path,
    state_path: Path,
    features_path: Path,
    heartbeat_stale_sec: int,
) -> dict[str, Any]:
    pid = json_pid(pid_path)
    heartbeat = read_optional_json(heartbeat_path)
    state = read_optional_json(state_path)
    features = read_optional_json(features_path)
    heartbeat_at = iso_from_payload(heartbeat, "heartbeat_at", "generated_at")
    heartbeat_age = age_sec(heartbeat_at)
    state_generated_at = iso_from_payload(state, "generated_at")
    features_generated_at = iso_from_payload(features, "generated_at")
    state_age = age_sec(state_generated_at)
    features_age = age_sec(features_generated_at)
    stale = heartbeat_age is None or heartbeat_age > heartbeat_stale_sec
    running = pid_running(pid)
    heartbeat_fresh = heartbeat_age is not None and heartbeat_age <= heartbeat_stale_sec
    ws_connected = bool(heartbeat.get("ws_connected")) if isinstance(heartbeat, dict) else False
    if running:
        process_registry_status = "running"
    elif pid_path.exists():
        process_registry_status = "pid_stale"
    else:
        process_registry_status = "missing"
    state_known = state_path.exists()
    features_known = features_path.exists()
    state_fresh = (not state_known) or (state_age is not None and state_age <= heartbeat_stale_sec)
    features_fresh = (not features_known) or (features_age is not None and features_age <= heartbeat_stale_sec)
    data_plane_fresh = heartbeat_fresh and state_fresh and features_fresh
    reason_codes: list[str] = []
    if not heartbeat_fresh:
        reason_codes.append("micro_heartbeat_stale" if heartbeat_path.exists() else "micro_heartbeat_missing")
    if heartbeat_fresh and not state_fresh:
        reason_codes.append("micro_state_write_stale")
    if heartbeat_fresh and not features_fresh:
        reason_codes.append("micro_features_write_stale")
    if running and ws_connected and not data_plane_fresh:
        reason_codes.append("micro_alive_but_not_emitting")
        reason_codes.append("micro_ws_connected_but_no_emit")
    if data_plane_fresh:
        data_plane_status = "fresh"
    elif heartbeat_path.exists() or state_path.exists() or features_path.exists():
        data_plane_status = "stale"
    else:
        data_plane_status = "missing"
    if running and data_plane_fresh:
        health_state = "healthy"
        status = "running"
    elif (not running) and data_plane_fresh and ws_connected:
        health_state = "data_plane_healthy_pid_stale"
        status = "running"
    elif running and ws_connected and not data_plane_fresh:
        health_state = "degraded_transport_ok_data_stale"
        status = "stale"
    elif running and stale:
        health_state = "stale"
        status = "stale"
    elif pid_path.exists() and not running:
        health_state = "stale" if heartbeat_path.exists() else "down"
        status = "stopped"
    else:
        health_state = "down"
        status = "stopped"
    active_targets = None
    if isinstance(heartbeat, dict):
        active_targets = heartbeat.get("active_symbol_count") or heartbeat.get("managed_symbol_count")
    return {
        "name": "micro_daemon",
        "status": status,
        "pid": pid,
        "pid_path": str(pid_path),
        "pid_exists": pid_path.exists(),
        "pid_running": running,
        "process_registry_status": process_registry_status,
        "heartbeat_path": str(heartbeat_path),
        "heartbeat_exists": heartbeat_path.exists(),
        "heartbeat_at": heartbeat_at,
        "heartbeat_age_sec": heartbeat_age,
        "stale": stale,
        "data_plane_status": data_plane_status,
        "data_plane_fresh": data_plane_fresh,
        "reason_codes": reason_codes,
        "health_state": health_state,
        "ws_connected": ws_connected,
        "state_path": str(state_path),
        "state_generated_at": state_generated_at,
        "state_age_sec": state_age,
        "features_path": str(features_path),
        "features_generated_at": features_generated_at,
        "features_age_sec": features_age,
        "heartbeat_seq": heartbeat.get("heartbeat_seq") if isinstance(heartbeat, dict) else None,
        "last_successful_emit_at": iso_from_payload(heartbeat, "last_successful_emit_at", "last_emit_at"),
        "last_ws_event_at": iso_from_payload(heartbeat, "last_ws_event_at", "last_message_at"),
        "target_generation": heartbeat.get("target_generation") if isinstance(heartbeat, dict) else None,
        "write_error_count": heartbeat.get("write_error_count") if isinstance(heartbeat, dict) else None,
        "last_exception": heartbeat.get("last_exception") if isinstance(heartbeat, dict) else None,
        "active_targets": active_targets,
    }
