"""STEP3.2 intent and reload result types. See docs/STEP3.2_任务卡.md."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ReloadResult:
    loaded: bool
    status: str
    target_status: str
    target_age_sec: float | None
    added: tuple[str, ...] = ()
    updated: tuple[str, ...] = ()
    retiring: tuple[str, ...] = ()
    evicted: tuple[str, ...] = ()
    expired: tuple[str, ...] = ()
    blocked_new_due_stale: tuple[str, ...] = ()
    blocked_new_due_cap: tuple[str, ...] = ()
    duplicate_symbol_tier2_wins: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class SubscribeIntent:
    symbol: str
    symbol_safe_id: str
    tier_key: str
    source_state: str
    streams: tuple[str, ...]
    priority: int
    scan_score: int
    move_side: str
    trigger_type: str
    min_collect_seconds: int
    ttl_seconds: int
    lifecycle: str
    first_seen_at: str
    last_target_seen_at: str


@dataclass(frozen=True)
class RetireIntent:
    symbol: str
    symbol_safe_id: str
    reason: str
    unsubscribe_deadline: str | None


@dataclass
class TargetManagerSettings:
    """Defaults align with docs/STEP3.0_MicroCollector_Phase.md section 12."""

    target_stale_sec: int = 420
    unsubscribe_grace_sec: int = 600
    max_managed_symbols: int = 100


def build_symbol_safe_id(symbol: str) -> str:
    """docs/STEP3.2_任务卡.md section 5.5."""
    if not symbol or not str(symbol).strip():
        msg = "empty symbol"
        raise ValueError(msg)
    raw = symbol.strip()
    s = raw.upper()
    base = re.sub(r"[^A-Za-z0-9_.-]", "_", s)
    if base != s:
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
        return f"{base}_{digest}"
    return s
