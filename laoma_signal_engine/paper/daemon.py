"""Paper daemon lifecycle helpers.

The long-running loop is intentionally thin: the durable behavior is in
PaperEngine.tick(), and this module provides singleton/status contracts for
CLI/API integration.
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any
from contextlib import contextmanager

from laoma_signal_engine.paper.engine import PaperEngine
from laoma_signal_engine.paper.models import PaperConfig
from laoma_signal_engine.paper.utils import atomic_write_json, read_json, utc_now_iso


def resolve_path(project_root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else project_root / path


def daemon_paths(project_root: Path, config: PaperConfig) -> dict[str, Path]:
    return {
        "lock": resolve_path(project_root, config.daemon_lock_path),
        "pid": resolve_path(project_root, config.daemon_pid_path),
        "log": resolve_path(project_root, config.daemon_log_path),
        "heartbeat": resolve_path(project_root, config.daemon_heartbeat_path),
        "status": resolve_path(project_root, config.daemon_status_path),
    }


def read_status(project_root: Path, config: PaperConfig | None = None) -> dict[str, Any]:
    cfg = config or PaperConfig()
    paths = daemon_paths(project_root, cfg)
    status = {"status": "stopped", "enabled": True, "tick_interval_sec": cfg.daemon_tick_interval_sec}
    if paths["status"].exists():
        try:
            data = read_json(paths["status"])
            if isinstance(data, dict):
                status.update(data)
        except Exception:
            status["status"] = "error"
            status["last_error"] = "status_read_failed"
    status["lock_exists"] = paths["lock"].exists()
    status["pid_exists"] = paths["pid"].exists()
    status["heartbeat_exists"] = paths["heartbeat"].exists()
    status["tick_lock"] = inspect_tick_lock(project_root, cfg)
    if paths["pid"].exists() and "pid" not in status:
        try:
            data = read_json(paths["pid"])
            if isinstance(data, dict):
                status["pid"] = data.get("pid")
        except Exception:
            pass
    return status


def write_status(project_root: Path, config: PaperConfig, status: dict[str, Any]) -> dict[str, Any]:
    paths = daemon_paths(project_root, config)
    payload = {
        "source": "paper_daemon",
        "updated_at": utc_now_iso(),
        "tick_interval_sec": config.daemon_tick_interval_sec,
        **status,
    }
    atomic_write_json(paths["status"], payload)
    if payload.get("status") == "running":
        atomic_write_json(paths["heartbeat"], {"source": "paper_daemon", "heartbeat_at": utc_now_iso(), "pid": os.getpid()})
    return payload


def _start_heartbeat_thread(project_root: Path, config: PaperConfig, stop_event: threading.Event) -> threading.Thread:
    paths = daemon_paths(project_root, config)
    interval = max(5, min(15, int(config.daemon_tick_interval_sec)))

    def _loop() -> None:
        while not stop_event.is_set():
            atomic_write_json(paths["heartbeat"], {"source": "paper_daemon", "heartbeat_at": utc_now_iso(), "pid": os.getpid()})
            stop_event.wait(interval)

    thread = threading.Thread(target=_loop, name="paper-daemon-heartbeat", daemon=True)
    thread.start()
    return thread


def run_once(project_root: Path, *, config: PaperConfig | None = None, candle_provider: Any | None = None) -> dict[str, Any]:
    cfg = config or PaperConfig()
    with _tick_guard(project_root, cfg) as guard:
        if not guard.get("acquired"):
            return {
                "status": "skipped",
                "reason": guard.get("reason") or "paper_tick_lock_busy",
                "reason_codes": guard.get("reason_codes") or ["paper_tick_lock_busy_alive_pid"],
                "tick_lock": guard,
            }
        engine = PaperEngine(project_root, config=cfg, candle_provider=candle_provider)
        result = engine.tick()
    status = write_status(
        project_root,
        cfg,
        {
            "status": "running",
            "pid": os.getpid(),
            "last_tick_at": utc_now_iso(),
            "next_tick_at": None,
            "last_error": None,
        },
    )
    result["tick_lock"] = guard
    result["daemon_status"] = status
    return result


def mark_stopped(project_root: Path, *, config: PaperConfig | None = None) -> dict[str, Any]:
    cfg = config or PaperConfig()
    paths = daemon_paths(project_root, cfg)
    paths["lock"].unlink(missing_ok=True)
    paths["pid"].unlink(missing_ok=True)
    return write_status(project_root, cfg, {"status": "stopped", "pid": None, "last_stopped_at": utc_now_iso()})


def run_forever(project_root: Path, *, config: PaperConfig | None = None) -> int:
    cfg = config or PaperConfig()
    paths = daemon_paths(project_root, cfg)
    paths["lock"].parent.mkdir(parents=True, exist_ok=True)
    payload = {"source": "paper_daemon", "pid": os.getpid(), "started_at": utc_now_iso()}
    atomic_write_json(paths["lock"], payload)
    atomic_write_json(paths["pid"], payload)
    stop_event = threading.Event()
    _start_heartbeat_thread(project_root, cfg, stop_event)
    engine = PaperEngine(project_root, config=cfg)
    try:
        while True:
            started = utc_now_iso()
            try:
                with _tick_guard(project_root, cfg) as guard:
                    if not guard.get("acquired"):
                        write_status(
                            project_root,
                            cfg,
                            {
                                "status": "running",
                                "pid": os.getpid(),
                                "last_error": guard.get("reason") or "paper_tick_lock_busy",
                                "last_tick_at": started,
                                "tick_lock": guard,
                                "reason_codes": guard.get("reason_codes") or ["paper_tick_lock_busy_alive_pid"],
                            },
                        )
                        time.sleep(max(1, int(cfg.daemon_tick_interval_sec)))
                        continue
                    result = engine.tick()
                status = {
                    "status": "running",
                    "pid": os.getpid(),
                    "last_tick_at": started,
                    "next_tick_at": None,
                    "active_symbols": _active_symbols(result.get("summary") if isinstance(result, dict) else {}),
                    "last_error": None,
                    "tick_lock": guard,
                }
                write_status(project_root, cfg, status)
            except Exception as exc:
                write_status(project_root, cfg, {"status": "error", "pid": os.getpid(), "last_error": str(exc), "last_tick_at": started})
            time.sleep(max(1, int(cfg.daemon_tick_interval_sec)))
    except KeyboardInterrupt:
        stop_event.set()
        mark_stopped(project_root, config=cfg)
        return 0


def _active_symbols(summary: Any) -> int:
    if not isinstance(summary, dict):
        return 0
    positions = summary.get("positions") or {}
    if not isinstance(positions, dict):
        return 0
    symbols: set[str] = set()
    for rows in positions.values():
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict) and row.get("status") == "open":
                    symbols.add(str(row.get("symbol") or ""))
    return len([x for x in symbols if x])


def _tick_lock_path(project_root: Path, config: PaperConfig) -> Path:
    paths = daemon_paths(project_root, config)
    return paths["lock"].with_suffix(paths["lock"].suffix + ".tick")


def _pid_is_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            process_query_limited_information = 0x1000
            still_active = 259
            handle = ctypes.windll.kernel32.OpenProcess(process_query_limited_information, False, int(pid))
            if not handle:
                return False
            try:
                exit_code = ctypes.c_ulong()
                ok = ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
                return bool(ok) and exit_code.value == still_active
            finally:
                ctypes.windll.kernel32.CloseHandle(handle)
        except Exception:
            pass
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _read_tick_lock(path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "pid": None,
        "pid_alive": False,
        "age_sec": None,
        "raw": None,
        "parse_status": "missing",
    }
    if not path.exists():
        return payload
    try:
        stat = path.stat()
        payload["age_sec"] = max(0, int(time.time() - stat.st_mtime))
    except OSError:
        payload["age_sec"] = None
    try:
        raw = path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        raw = ""
    payload["raw"] = raw
    pid: int | None = None
    if raw:
        try:
            import json

            data = json.loads(raw)
            if isinstance(data, dict):
                pid = int(data.get("pid") or 0) or None
                payload["started_at"] = data.get("started_at")
                payload["schema_version"] = data.get("schema_version")
            elif isinstance(data, int):
                pid = data or None
        except (ValueError, TypeError):
            try:
                pid = int(raw)
            except ValueError:
                pid = None
    payload["pid"] = pid
    payload["pid_alive"] = _pid_is_alive(pid)
    payload["parse_status"] = "ok" if pid else "malformed"
    return payload


def inspect_tick_lock(project_root: Path, config: PaperConfig | None = None) -> dict[str, Any]:
    cfg = config or PaperConfig()
    return _read_tick_lock(_tick_lock_path(project_root, cfg))


def _stale_tick_lock_max_age_sec(config: PaperConfig) -> int:
    return max(120, int(config.daemon_tick_interval_sec) * 2)


def _lock_is_stale(info: dict[str, Any], config: PaperConfig) -> tuple[bool, str]:
    if not info.get("exists"):
        return False, ""
    if info.get("parse_status") == "malformed":
        return True, "paper_tick_lock_malformed_recovered"
    if not info.get("pid_alive"):
        return True, "paper_tick_lock_stale_recovered"
    age = info.get("age_sec")
    if isinstance(age, int) and age > _stale_tick_lock_max_age_sec(config):
        return True, "paper_tick_lock_expired_recovered"
    return False, ""


def _tick_lock_reason_codes(reason: str, info: dict[str, Any] | None = None) -> list[str]:
    codes = [reason] if reason else []
    detail = info or {}
    if detail.get("exists") and detail.get("parse_status") == "malformed":
        codes.append("paper_tick_lock_malformed_recovered")
    if detail.get("exists") and detail.get("pid") and not detail.get("pid_alive"):
        codes.append("paper_tick_lock_stale_dead_pid")
    return sorted(set(codes))


@contextmanager
def _tick_guard(project_root: Path, config: PaperConfig):
    tick_lock = _tick_lock_path(project_root, config)
    tick_lock.parent.mkdir(parents=True, exist_ok=True)
    handle: int | None = None
    recovered: dict[str, Any] | None = None
    last_info: dict[str, Any] | None = None
    retry_count = 0
    had_unlink_failure = False
    max_retries = 3
    try:
        for attempt in range(max_retries + 1):
            try:
                handle = os.open(str(tick_lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                retry_count = attempt
                break
            except FileExistsError:
                pass
            info = _read_tick_lock(tick_lock)
            last_info = info
            stale, reason = _lock_is_stale(info, config)
            if not stale:
                info.update(
                    {
                        "acquired": False,
                        "status": "busy",
                        "reason": "paper_tick_lock_alive_busy",
                        "reason_codes": ["paper_tick_lock_alive_busy", "paper_tick_lock_busy_alive_pid"],
                        "reconcile_action": "blocked_alive",
                        "retry_count": attempt,
                    }
                )
                yield info
                return
            try:
                tick_lock.unlink(missing_ok=True)
                recovered = {
                    **info,
                    "recovered": True,
                    "recovery_reason": reason,
                    "recovery_reason_codes": _tick_lock_reason_codes(reason, info),
                    "unlink_failed_before_recovery": had_unlink_failure,
                    "retry_count": attempt,
                }
            except OSError:
                had_unlink_failure = True
                last_info = {
                    **info,
                    "acquired": False,
                    "status": "retrying",
                    "reason": "paper_tick_lock_unlink_failed",
                    "reason_codes": [
                        "paper_tick_lock_unlink_failed",
                        *_tick_lock_reason_codes(reason, info),
                    ],
                    "reconcile_action": "retry",
                    "retry_count": attempt,
                }
                if attempt >= max_retries:
                    break
                time.sleep(0.05 * (attempt + 1))
                continue
            if attempt >= max_retries:
                break
            time.sleep(0.02)
        if handle is None:
            failed = last_info or _read_tick_lock(tick_lock)
            reason_codes = list(failed.get("reason_codes") or [])
            if not reason_codes:
                reason_codes = ["paper_tick_lock_retry_exhausted"]
            if "paper_tick_lock_retry_exhausted" not in reason_codes:
                reason_codes.append("paper_tick_lock_retry_exhausted")
            failed.update(
                {
                    "acquired": False,
                    "status": "failed",
                    "reason": failed.get("reason") or "paper_tick_lock_retry_exhausted",
                    "reason_codes": sorted(set(reason_codes)),
                    "reconcile_action": "failed",
                    "retry_count": max_retries,
                    "recovered_previous": recovered,
                }
            )
            yield failed
            return
        payload = {
            "schema_version": "14.24",
            "source": "paper_tick_lock",
            "pid": os.getpid(),
            "started_at": utc_now_iso(),
        }
        import json

        os.write(handle, json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"))
        detail = {
            "path": str(tick_lock),
            "exists": True,
            "pid": os.getpid(),
            "pid_alive": True,
            "acquired": True,
            "status": "retry_acquired" if had_unlink_failure else ("stale_recovered" if recovered else "acquired"),
            "reason": None,
            "reason_codes": (
                sorted(
                    set(
                        [
                            *(recovered.get("recovery_reason_codes") or [recovered["recovery_reason"]]),
                            *(["paper_tick_lock_unlink_failed_recovered", "paper_tick_lock_retry_acquired"] if had_unlink_failure else []),
                        ]
                    )
                )
                if recovered
                else (["paper_tick_lock_retry_acquired"] if had_unlink_failure else [])
            ),
            "recovered_previous": recovered,
            "reconcile_action": "retry" if had_unlink_failure else ("unlink_stale" if recovered else "none"),
            "retry_count": retry_count,
        }
        yield detail
    finally:
        if handle is not None:
            os.close(handle)
            try:
                tick_lock.unlink()
            except FileNotFoundError:
                pass
