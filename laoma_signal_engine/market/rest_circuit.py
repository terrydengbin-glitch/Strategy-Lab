"""Shared Binance REST circuit state.

This is intentionally small and file-backed so CLI runs, FastAPI, and the
pipeline can see the same cooldown boundary without requiring a daemon.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import orjson

from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now

DEFAULT_CIRCUIT_PATH = Path("DATA/runtime/binance_rest_circuit.json")
DEFAULT_418_COOLDOWN_SEC = 60 * 60
DEFAULT_429_COOLDOWN_SEC = 3 * 60


def _path(project_root: Path) -> Path:
    return (project_root / DEFAULT_CIRCUIT_PATH).resolve()


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _read_raw(project_root: Path) -> dict[str, Any]:
    path = _path(project_root)
    try:
        raw = orjson.loads(path.read_bytes())
    except (OSError, orjson.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def read_rest_circuit(project_root: Path) -> dict[str, Any]:
    raw = _read_raw(project_root)
    now = utc_now()
    until_dt = _parse_ts(raw.get("rest_circuit_until"))
    state = str(raw.get("rest_circuit_state") or "closed")
    remaining = int((until_dt - now).total_seconds()) if until_dt else 0
    if state == "open" and remaining <= 0:
        state = "half_open"
        remaining = 0
    if state not in {"closed", "half_open", "open"}:
        state = "closed"
    return {
        "schema_version": "STEP1.64_rest_circuit_v1",
        "rest_circuit_state": state,
        "rest_circuit_opened_at": raw.get("rest_circuit_opened_at"),
        "rest_circuit_until": raw.get("rest_circuit_until") if state == "open" else None,
        "rest_circuit_remaining_sec": max(0, remaining),
        "rest_circuit_reason": raw.get("rest_circuit_reason"),
        "rest_circuit_source_stage": raw.get("rest_circuit_source_stage"),
        "rest_circuit_source_endpoint": raw.get("rest_circuit_source_endpoint"),
        "http_429_count": int(raw.get("http_429_count") or 0),
        "http_418_count": int(raw.get("http_418_count") or 0),
        "retry_after_sec": raw.get("retry_after_sec"),
        "live_rest_allowed": state in {"closed", "half_open"},
        "degraded_mode_allowed": state == "open",
        "source_path": str(_path(project_root)),
        "updated_at": raw.get("updated_at"),
    }


def write_rest_circuit_open(
    project_root: Path,
    *,
    status_code: int,
    endpoint: str,
    source_stage: str,
    retry_after_sec: int | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    now = utc_now()
    cooldown = int(retry_after_sec or (DEFAULT_418_COOLDOWN_SEC if int(status_code) == 418 else DEFAULT_429_COOLDOWN_SEC))
    until = now + timedelta(seconds=max(1, cooldown))
    prev = _read_raw(project_root)
    payload = {
        "schema_version": "STEP1.64_rest_circuit_v1",
        "rest_circuit_state": "open",
        "rest_circuit_opened_at": to_iso_z(now),
        "rest_circuit_until": to_iso_z(until),
        "rest_circuit_reason": reason or f"http_{status_code}",
        "rest_circuit_source_stage": source_stage,
        "rest_circuit_source_endpoint": endpoint,
        "http_429_count": int(prev.get("http_429_count") or 0) + (1 if int(status_code) == 429 else 0),
        "http_418_count": int(prev.get("http_418_count") or 0) + (1 if int(status_code) == 418 else 0),
        "retry_after_sec": retry_after_sec,
        "updated_at": to_iso_z(now),
    }
    p = _path(project_root)
    p.parent.mkdir(parents=True, exist_ok=True)
    write_file_atomic(p, orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE))
    return read_rest_circuit(project_root)


def close_rest_circuit(project_root: Path, *, reason: str = "manual_close") -> dict[str, Any]:
    now = utc_now()
    prev = _read_raw(project_root)
    payload = {
        "schema_version": "STEP1.64_rest_circuit_v1",
        "rest_circuit_state": "closed",
        "rest_circuit_opened_at": prev.get("rest_circuit_opened_at"),
        "rest_circuit_until": None,
        "rest_circuit_reason": reason,
        "rest_circuit_source_stage": prev.get("rest_circuit_source_stage"),
        "rest_circuit_source_endpoint": prev.get("rest_circuit_source_endpoint"),
        "http_429_count": int(prev.get("http_429_count") or 0),
        "http_418_count": int(prev.get("http_418_count") or 0),
        "retry_after_sec": None,
        "updated_at": to_iso_z(now),
    }
    p = _path(project_root)
    p.parent.mkdir(parents=True, exist_ok=True)
    write_file_atomic(p, orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE))
    return read_rest_circuit(project_root)
