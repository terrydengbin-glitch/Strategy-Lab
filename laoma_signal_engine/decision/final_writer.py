"""Atomic write for STEP5.0 latest_decisions.json."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from laoma_signal_engine.decision.final_models import FinalDecisionsDocument


def atomic_write_latest_decisions(path: Path, document: FinalDecisionsDocument) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = document.model_dump(mode="json")
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    replace_attempts = 8 if os.name == "nt" else 1
    try:
        for attempt in range(replace_attempts):
            with open(tmp, "w", encoding="utf-8", newline="\n") as f:
                f.write(text)
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
