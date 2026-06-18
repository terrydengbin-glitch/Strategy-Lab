"""Atomic rename-after-write for local JSON artifacts."""

from __future__ import annotations

import os
import time
from pathlib import Path


def write_file_atomic(path: Path, data: bytes, *, windows_retries: int = 8) -> None:
    """Write bytes to path using a temp file in the same directory, then os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    attempts = windows_retries if os.name == "nt" else 1
    try:
        for attempt in range(attempts):
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            try:
                os.replace(tmp, path)
            except PermissionError:
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
                if attempt + 1 >= attempts:
                    raise
                time.sleep(0.05 * (attempt + 1))
                continue
            return
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise
