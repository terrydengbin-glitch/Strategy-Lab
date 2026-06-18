"""JSON read/write using orjson (UTF-8)."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import orjson


def read_json_object(path: Path) -> Any:
    """Load JSON object from UTF-8 file (replace errors for untrusted input)."""
    raw = path.read_text(encoding="utf-8", errors="replace")
    return json.loads(raw)


def write_json_bytes(path: Path, obj: Any) -> None:
    """Serialize to UTF-8 bytes via orjson (compact, no ASCII-only escape for non-ASCII field values)."""
    payload = orjson.dumps(obj, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
    path.write_bytes(payload)


def write_json_atomic(path: Path, obj: Any, *, windows_retries: int = 8) -> None:
    """Serialize JSON and atomically replace path with a same-directory temp file."""
    payload = orjson.dumps(obj, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    replace_attempts = windows_retries if os.name == "nt" else 1
    try:
        for attempt in range(replace_attempts):
            with open(tmp, "wb") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            try:
                os.replace(tmp, path)
            except PermissionError:
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
                if attempt + 1 >= replace_attempts:
                    raise
                time.sleep(0.05 * (attempt + 1))
                continue
            return
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise
