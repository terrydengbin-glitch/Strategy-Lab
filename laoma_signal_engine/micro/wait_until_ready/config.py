"""wait_until_ready YAML fragment -> dataclass. docs/STEP3.8D_*.md."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from laoma_signal_engine.core.config_loader import package_root

_VALID_MODES = frozenset(
    {
        "min_ready_count",
        "min_fast_ready_count",
        "min_full_ready_count",
        "min_ready_strong",
        "symbols",
        "strict_coverage",
    }
)
_VALID_REQ = frozenset({"any", "all"})


def recommended_target_stale_sec(*, max_wait_sec: float | int, buffer_sec: int = 300) -> int:
    """target_stale_sec must cover max_wait_sec + buffer (STEP3.8D section 9)."""
    mw = float(max_wait_sec)
    b = max(0, int(buffer_sec))
    return max(420, int(mw + b))


@dataclass(frozen=True)
class WaitUntilReadyConfig:
    enabled: bool = False
    mode: str = "min_ready_count"
    min_ready_count: int = 1
    min_ready_strong_count: int = 1
    symbols: tuple[str, ...] = ()
    require_symbols: str = "any"
    poll_interval_sec: float = 5.0
    max_wait_sec: float = 2400.0
    max_consecutive_read_failures: int = 20
    ws_message_max_age_sec: float = 5.0
    require_target_fresh: bool = True
    require_ws_connected: bool = True
    target_stale_buffer_sec: int = 300
    strict_coverage_min_by_stream: tuple[tuple[str, int], ...] = ()

    @classmethod
    def from_mapping(cls, m: Mapping[str, Any] | None) -> WaitUntilReadyConfig:
        if not m:
            return cls()
        mode = str(m.get("mode", "min_ready_count")).strip()
        if mode not in _VALID_MODES:
            mode = "min_ready_count"
        req = str(m.get("require_symbols", "any")).strip().lower()
        if req not in _VALID_REQ:
            req = "any"
        syms_raw = m.get("symbols") or []
        if not isinstance(syms_raw, list):
            syms_raw = []
        symbols = tuple(str(x).strip().upper() for x in syms_raw if str(x).strip())
        sc_raw = m.get("strict_coverage_streams") or {}
        sc_pairs: list[tuple[str, int]] = []
        if isinstance(sc_raw, dict):
            for k, v in sc_raw.items():
                try:
                    sc_pairs.append((str(k), int(v)))
                except (TypeError, ValueError):
                    continue
        sc_pairs.sort(key=lambda x: x[0])
        return cls(
            enabled=bool(m.get("enabled", False)),
            mode=mode,
            min_ready_count=int(m.get("min_ready_count", 1)),
            min_ready_strong_count=int(m.get("min_ready_strong_count", 1)),
            symbols=symbols,
            require_symbols=req,
            poll_interval_sec=float(m.get("poll_interval_sec", 5.0)),
            max_wait_sec=float(m.get("max_wait_sec", 2400.0)),
            max_consecutive_read_failures=int(m.get("max_consecutive_read_failures", 20)),
            ws_message_max_age_sec=float(m.get("ws_message_max_age_sec", 5.0)),
            require_target_fresh=bool(m.get("require_target_fresh", True)),
            require_ws_connected=bool(m.get("require_ws_connected", True)),
            target_stale_buffer_sec=int(m.get("target_stale_buffer_sec", 300)),
            strict_coverage_min_by_stream=tuple(sc_pairs),
        )


def load_wait_until_ready_config(project_root: Path | None = None) -> WaitUntilReadyConfig:
    """Load `wait_until_ready` from packaged default.yaml (same file as EngineConfig)."""
    _ = project_root  # reserved for future per-project override
    cfg_path = package_root() / "config" / "default.yaml"
    raw_text = cfg_path.read_text(encoding="utf-8")
    doc: dict[str, Any] = yaml.safe_load(raw_text) or {}
    frag = doc.get("wait_until_ready")
    if frag is not None and not isinstance(frag, dict):
        return WaitUntilReadyConfig()
    return WaitUntilReadyConfig.from_mapping(frag if isinstance(frag, dict) else None)
