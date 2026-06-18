"""Runtime logging helpers for long-lived micro daemon processes."""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any

_LOCK = threading.Lock()
_LAST_PRINT_AT: dict[str, float] = {}
_COUNTS: dict[str, int] = {}
_MAX_VALUES: dict[str, float] = {}
_ONCE_KEYS: set[str] = set()


def log_once(key: str, message: str) -> None:
    """Print a process-local message once."""
    with _LOCK:
        if key in _ONCE_KEYS:
            return
        _ONCE_KEYS.add(key)
    print(message, flush=True)


def rate_limited_summary(
    key: str,
    *,
    prefix: str,
    window_sec: int = 60,
    max_value: float | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    """Aggregate noisy events and print a compact summary at most once per window."""
    now = time.monotonic()
    with _LOCK:
        _COUNTS[key] = _COUNTS.get(key, 0) + 1
        if max_value is not None:
            _MAX_VALUES[key] = max(float(max_value), _MAX_VALUES.get(key, float(max_value)))
        last = _LAST_PRINT_AT.get(key, 0.0)
        if now - last < max(1, window_sec):
            return
        count = _COUNTS.pop(key, 0)
        peak = _MAX_VALUES.pop(key, None)
        _LAST_PRINT_AT[key] = now

    parts = [prefix, f"count={count}", f"window_sec={max(1, window_sec)}"]
    if peak is not None:
        parts.append(f"max_value={peak:g}")
    for k, v in (extra or {}).items():
        parts.append(f"{k}={v}")
    print(" ".join(parts), flush=True)


def rotate_file_if_needed(path: Path, *, max_bytes: int, backup_count: int) -> bool:
    """Rotate an inactive log file before a daemon process opens it."""
    if max_bytes <= 0 or backup_count <= 0 or not path.exists():
        return False
    try:
        if path.stat().st_size < max_bytes:
            return False
    except OSError:
        return False

    path.parent.mkdir(parents=True, exist_ok=True)
    oldest = path.with_name(f"{path.name}.{backup_count}")
    if oldest.exists():
        oldest.unlink(missing_ok=True)
    for idx in range(backup_count - 1, 0, -1):
        src = path.with_name(f"{path.name}.{idx}")
        dst = path.with_name(f"{path.name}.{idx + 1}")
        if src.exists():
            os.replace(src, dst)
    os.replace(path, path.with_name(f"{path.name}.1"))
    return True


def file_size_or_none(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except OSError:
        return None

