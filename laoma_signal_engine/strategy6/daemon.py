"""Strategy6 persistent observe daemon.

This daemon only rechecks Strategy6 WAIT pool rows. It does not control the
main pipeline, micro daemons, paper daemon, or P21 backtest jobs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from laoma_signal_engine.strategy6.evidence import (
    _pid_is_alive,
    load_strategy6_config,
    paths,
    run_strategy6_observe_once,
    strategy6_daemon_status,
    strategy6_watchdog,
    write_daemon_heartbeat,
)


def _pid_alive(pid: int) -> bool:
    return _pid_is_alive(pid)


def start_daemon(project_root: Path) -> dict[str, object]:
    import subprocess

    root = Path(project_root)
    p = paths(root)
    p.daemon_lock.parent.mkdir(parents=True, exist_ok=True)
    current = strategy6_daemon_status(root)
    if current.get("pid_alive"):
        return {"status": "already_running", "pid": current.get("pid"), "heartbeat": current}
    try:
        if p.daemon_stop.exists():
            p.daemon_stop.unlink()
    except OSError:
        pass
    proc = subprocess.Popen(
        [sys.executable, "-m", "laoma_signal_engine.strategy6.daemon", "--project-root", str(root), "--loop"],
        cwd=str(root),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    return {"status": "started", "pid": proc.pid}


def stop_daemon(project_root: Path) -> dict[str, object]:
    root = Path(project_root)
    p = paths(root)
    p.daemon_stop.parent.mkdir(parents=True, exist_ok=True)
    p.daemon_stop.write_text(str(time.time()), encoding="utf-8")
    status = strategy6_daemon_status(root)
    pid = int(status.get("pid") or 0)
    if _pid_alive(pid):
        return {"status": "stop_requested", "pid": pid}
    return {"status": "not_running", "pid": pid or None}


def run_loop(project_root: Path) -> int:
    root = Path(project_root)
    p = paths(root)
    cfg = load_strategy6_config(root)
    interval = max(5, int(cfg.get("observe_interval_sec") or 300))
    p.daemon_lock.parent.mkdir(parents=True, exist_ok=True)
    p.daemon_lock.write_text(str(os.getpid()), encoding="utf-8")
    write_daemon_heartbeat(root, status="running", pid=os.getpid(), payload={"phase": "daemon_start"})
    try:
        while True:
            if p.daemon_stop.exists():
                write_daemon_heartbeat(root, status="stopped", pid=os.getpid(), payload={"phase": "stop_requested"})
                return 0
            try:
                run_strategy6_observe_once(root)
            except Exception as exc:  # pragma: no cover - defensive daemon boundary
                write_daemon_heartbeat(root, status="error", pid=os.getpid(), last_error=str(exc))
            slept = 0
            while slept < interval:
                if p.daemon_stop.exists():
                    write_daemon_heartbeat(root, status="stopped", pid=os.getpid(), payload={"phase": "stop_requested"})
                    return 0
                time.sleep(min(5, interval - slept))
                slept += min(5, interval - slept)
                write_daemon_heartbeat(root, status="idle", pid=os.getpid(), payload={"phase": "sleep", "sleep_elapsed_sec": slept})
    finally:
        try:
            p.daemon_lock.unlink()
        except OSError:
            pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--start", action="store_true")
    parser.add_argument("--stop", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--watchdog", action="store_true")
    parser.add_argument("--recover", action="store_true")
    args = parser.parse_args(argv)
    root = Path(args.project_root).resolve()
    if args.start:
        print(json.dumps(start_daemon(root), ensure_ascii=False))
        return 0
    if args.stop:
        print(json.dumps(stop_daemon(root), ensure_ascii=False))
        return 0
    if args.status:
        print(json.dumps(strategy6_daemon_status(root), ensure_ascii=False))
        return 0
    if args.watchdog:
        print(json.dumps(strategy6_watchdog(root, recover=bool(args.recover)), ensure_ascii=False))
        return 0
    if args.loop:
        return run_loop(root)
    print(json.dumps(run_strategy6_observe_once(root), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
