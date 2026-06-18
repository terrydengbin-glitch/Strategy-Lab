"""Heartbeat artifact (Pydantic + atomic write). docs/STEP3.8_任务卡.md."""

from __future__ import annotations

import json
import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from laoma_signal_engine.micro.assembly.models import TargetStatus


class HeartbeatDroppedEvents(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trade: int = Field(ge=0)
    book: int = Field(ge=0)
    depth: int = Field(ge=0)


class MicroCollectorHeartbeat(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = "1.0"
    generated_at: str
    source: str = "micro_collector_daemon"
    daemon_status: str = "running"
    process_uptime_sec: int = Field(ge=0)
    last_loop_ok: bool
    last_error: str | None = None
    target_reload_last_at: str
    target_manager_status: str
    target_status: TargetStatus
    target_generated_at: str = ""
    target_age_sec: float | None = None
    managed_symbol_count: int = Field(ge=0)
    active_symbol_count: int = Field(default=0, ge=0)
    ready_count: int = Field(default=0, ge=0)
    ws_connected: bool
    ws_status: str = ""
    ws_last_message_age_sec: float | None = None
    dropped_events: HeartbeatDroppedEvents
    last_output_generated_at: str | None = None
    latest_features_written_at: str | None = None
    log_rotation_enabled: bool | None = None
    log_file_size_bytes: int | None = Field(default=None, ge=0)


def atomic_write_heartbeat(path: Path, heartbeat: MicroCollectorHeartbeat) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = heartbeat.model_dump(mode="json")
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    try:
        with open(tmp, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise
