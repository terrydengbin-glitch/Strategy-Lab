"""Optional manual_watchlist.json: bases list (invalid file -> empty + caller logs)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from pydantic import ValidationError

from laoma_signal_engine.core.models import ManualWatchlistEntry, ManualWatchlistFile

log = logging.getLogger(__name__)


def _load_manual_file(path: Path) -> ManualWatchlistFile | None:
    if not path.is_file():
        return None
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
        data = json.loads(raw)
        if isinstance(data, list):
            data = {"bases": data}
        parsed = ManualWatchlistFile.model_validate(data)
    except (json.JSONDecodeError, OSError, ValidationError) as exc:
        log.warning("manual_watchlist invalid or unreadable at %s (%s); skipped", path, exc)
        return None
    return parsed


def load_manual_entries(path: Path) -> dict[str, ManualWatchlistEntry]:
    """Return richer manual entries keyed by uppercase base; missing/invalid -> empty."""
    parsed = _load_manual_file(path)
    if parsed is None:
        return {}
    out: dict[str, ManualWatchlistEntry] = {}
    for base in parsed.bases:
        if isinstance(base, str) and base.strip():
            b = base.strip().upper()
            out[b] = ManualWatchlistEntry(base=b, mode="watch_only", priority=0, reason="")
    for entry in parsed.entries:
        b = entry.base.strip().upper()
        if not b:
            continue
        out[b] = entry.model_copy(update={"base": b, "mode": entry.mode.strip().lower()})
    return out


def load_manual_bases(path: Path) -> set[str]:
    """Return uppercase base assets; missing file -> empty; invalid JSON/schema -> warn and empty."""
    entries = load_manual_entries(path)
    if not entries:
        return set()
    return set(entries.keys())
