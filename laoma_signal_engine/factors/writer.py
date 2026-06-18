"""Atomic write for STEP3B factor snapshot. docs/STEP4.0_Decision_Layer_Phase1_任务卡.md."""

from __future__ import annotations

import json
import os
from pathlib import Path

from laoma_signal_engine.factors.models import FactorSnapshotDocument


def atomic_write_factor_snapshot(path: Path, document: FactorSnapshotDocument) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = document.model_dump(mode="json")
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
