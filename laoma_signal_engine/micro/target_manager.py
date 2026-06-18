"""STEP3.2 Micro Target Manager. See docs/STEP3.2_任务卡.md."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Callable

from pydantic import ValidationError

from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.time_utils import parse_iso_z, to_iso_z, utc_now
from laoma_signal_engine.micro.micro_target_models import MicroTargetEntry, MicroTargetsDocument
from laoma_signal_engine.micro.target_intent_models import (
    ReloadResult,
    RetireIntent,
    SubscribeIntent,
    TargetManagerSettings,
    build_symbol_safe_id,
)


@dataclass
class _Managed:
    symbol: str
    symbol_safe_id: str
    tier_key: str
    priority: int
    source_state: str
    scan_score: int
    move_side: str
    trigger_type: str
    subscribe: tuple[str, ...]
    min_collect_seconds: int
    ttl_seconds: int
    target_ready_tf: str
    lifecycle: str
    first_seen_at: datetime
    last_target_seen_at: datetime
    ttl_deadline: datetime
    retiring_since: datetime | None
    unsubscribe_deadline: datetime | None


class MicroTargetManager:
    """Reads micro_targets.json; maintains managed symbols and subscribe/retire intents."""

    def __init__(
        self,
        targets_path: Path,
        settings: TargetManagerSettings | None = None,
        *,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._path = targets_path
        self._settings = settings if settings is not None else TargetManagerSettings()
        self._now = now_fn if now_fn is not None else utc_now
        self._managed: dict[str, _Managed] = {}
        self._cap_retire_pending: dict[str, RetireIntent] = {}
        self._last_reload: ReloadResult | None = None
        self._last_target_age_sec: float | None = None
        self._last_target_status: str = "unknown"

    def reload(self) -> ReloadResult:
        now = self._now()
        added: list[str] = []
        updated: list[str] = []
        retiring_n: list[str] = []
        evicted: list[str] = []
        blocked_stale: list[str] = []
        blocked_cap: list[str] = []
        dup_wins: list[str] = []
        errors: list[str] = []

        loaded = False
        doc: MicroTargetsDocument | None = None
        reload_status = "missing"
        target_status = "unknown"
        target_age_sec: float | None = None

        if not self._path.is_file():
            self._last_reload = ReloadResult(
                loaded=False,
                status="missing",
                target_status="unknown",
                target_age_sec=None,
                errors=("file missing",),
            )
            self._last_target_age_sec = None
            self._last_target_status = "unknown"
            return self._last_reload

        try:
            raw: Any = read_json_object(self._path)
            loaded = True
            doc = MicroTargetsDocument.model_validate(raw)
            reload_status = "ok"
        except ValidationError as e:
            errors.append(f"parse_error: {e}")
            reload_status = "parse_error"
            target_status = "invalid_targets"
            doc = None
        except (OSError, JSONDecodeError, ValueError, TypeError) as e:
            errors.append(f"parse_error: {e}")
            reload_status = "parse_error"
            target_status = "invalid_targets"
            doc = None

        if doc is not None:
            gen_at = parse_iso_z(doc.generated_at)
            target_age_sec = (now - gen_at).total_seconds()
            stale = target_age_sec > float(self._settings.target_stale_sec)
            if stale:
                reload_status = "stale"
                target_status = "stale"
            elif doc.status in ("stale_input", "partial_input_stale"):
                reload_status = "stale"
                target_status = "stale"
            elif doc.status not in ("ok", "no_targets"):
                reload_status = "invalid_status"
                target_status = "invalid_targets"
            else:
                reload_status = "ok"
                target_status = "fresh"

        self._last_target_age_sec = target_age_sec
        self._last_target_status = target_status

        trust_ok = bool(
            doc is not None and reload_status == "ok" and target_status == "fresh"
        )

        if trust_ok:
            assert doc is not None
            desired, dup_wins = self._build_desired(doc, dup_wins)
            desired_keys = set(desired.keys())

            for symbol in list(self._managed.keys()):
                if symbol not in desired_keys:
                    m = self._managed[symbol]
                    if m.lifecycle != "retiring":
                        m.lifecycle = "retiring"
                        m.retiring_since = now
                        m.unsubscribe_deadline = now + timedelta(
                            seconds=self._settings.unsubscribe_grace_sec
                        )
                        retiring_n.append(symbol)

            for symbol, (entry, tier_key) in desired.items():
                if symbol in self._managed:
                    m = self._managed[symbol]
                    m.last_target_seen_at = now
                    m.ttl_deadline = m.last_target_seen_at + timedelta(seconds=entry.ttl_seconds)
                    m.tier_key = tier_key
                    m.priority = entry.priority
                    m.source_state = entry.source_state
                    m.scan_score = entry.scan_score
                    m.move_side = entry.move_side
                    m.trigger_type = entry.trigger_type
                    m.subscribe = tuple(entry.subscribe)
                    m.min_collect_seconds = entry.min_collect_seconds
                    m.ttl_seconds = entry.ttl_seconds
                    m.target_ready_tf = entry.target_ready_tf
                    if m.lifecycle == "retiring":
                        m.lifecycle = "warming"
                        m.retiring_since = None
                        m.unsubscribe_deadline = None
                    updated.append(symbol)
                else:
                    ok_add, ev_sym = self._try_add_new(symbol, entry, tier_key, now, blocked_cap)
                    if ok_add:
                        added.append(symbol)
                        if ev_sym:
                            evicted.append(ev_sym)
                    elif ev_sym is None:
                        pass

        else:
            if doc is not None and loaded and reload_status in ("stale", "invalid_status"):
                try:
                    dmap, _ = self._build_desired(doc, [])
                    blocked_stale = sorted(s for s in dmap if s not in self._managed)
                except (ValidationError, ValueError):
                    blocked_stale = []

        self._last_reload = ReloadResult(
            loaded=loaded,
            status=reload_status,
            target_status=target_status,
            target_age_sec=target_age_sec,
            added=tuple(added),
            updated=tuple(updated),
            retiring=tuple(retiring_n),
            evicted=tuple(evicted),
            expired=tuple(),
            blocked_new_due_stale=tuple(blocked_stale),
            blocked_new_due_cap=tuple(blocked_cap),
            duplicate_symbol_tier2_wins=tuple(dup_wins),
            errors=tuple(errors),
        )
        return self._last_reload

    def _build_desired(
        self,
        doc: MicroTargetsDocument,
        dup_accum: list[str],
    ) -> tuple[dict[str, tuple[MicroTargetEntry, str]], list[str]]:
        out: dict[str, tuple[MicroTargetEntry, str]] = {}
        for e in doc.tier1_warm_watch:
            sym = e.symbol.strip().upper()
            if not sym:
                msg = "empty symbol in tier1"
                raise ValueError(msg)
            out[sym] = (e, "tier1_warm_watch")
        for e in doc.tier2_active_strong:
            sym = e.symbol.strip().upper()
            if not sym:
                msg = "empty symbol in tier2"
                raise ValueError(msg)
            if sym in out:
                dup_accum.append(sym)
                prev_e, _ = out[sym]
                merged = e.model_copy(
                    update={
                        "priority": max(prev_e.priority, e.priority),
                        "source_state": "strong_candidate",
                    }
                )
                out[sym] = (merged, "tier2_active_strong")
            else:
                out[sym] = (e.model_copy(), "tier2_active_strong")
        return out, dup_accum

    def _try_add_new(
        self,
        symbol: str,
        entry: MicroTargetEntry,
        tier_key: str,
        now: datetime,
        blocked_cap: list[str],
    ) -> tuple[bool, str | None]:
        cap = self._settings.max_managed_symbols
        if len(self._managed) < cap:
            self._managed[symbol] = self._new_managed(symbol, entry, tier_key, now)
            return True, None

        retiring = [m for m in self._managed.values() if m.lifecycle == "retiring"]
        if not retiring:
            blocked_cap.append(symbol)
            return False, None

        retiring.sort(key=lambda m: (m.priority, m.symbol))
        victim = retiring[0]
        if entry.priority <= victim.priority:
            blocked_cap.append(symbol)
            return False, None

        del self._managed[victim.symbol]
        self._cap_retire_pending[victim.symbol] = RetireIntent(
            symbol=victim.symbol,
            symbol_safe_id=victim.symbol_safe_id,
            reason="cap_eviction",
            unsubscribe_deadline=to_iso_z(now),
        )
        self._managed[symbol] = self._new_managed(symbol, entry, tier_key, now)
        return True, victim.symbol

    def _new_managed(
        self,
        symbol: str,
        entry: MicroTargetEntry,
        tier_key: str,
        now: datetime,
    ) -> _Managed:
        sid = build_symbol_safe_id(symbol)
        return _Managed(
            symbol=symbol,
            symbol_safe_id=sid,
            tier_key=tier_key,
            priority=entry.priority,
            source_state=entry.source_state,
            scan_score=entry.scan_score,
            move_side=entry.move_side,
            trigger_type=entry.trigger_type,
            subscribe=tuple(entry.subscribe),
            min_collect_seconds=entry.min_collect_seconds,
            ttl_seconds=entry.ttl_seconds,
            target_ready_tf=entry.target_ready_tf,
            lifecycle="new",
            first_seen_at=now,
            last_target_seen_at=now,
            ttl_deadline=now + timedelta(seconds=entry.ttl_seconds),
            retiring_since=None,
            unsubscribe_deadline=None,
        )

    def get_subscribe_intents(self) -> list[SubscribeIntent]:
        now = self._now()
        out: list[SubscribeIntent] = []
        for m in self._managed.values():
            if self._is_due_retire(m, now):
                continue
            out.append(self._to_subscribe_intent(m))
        return sorted(out, key=lambda x: x.symbol)

    def get_retire_intents(self) -> list[RetireIntent]:
        now = self._now()
        intents: dict[str, RetireIntent] = {}
        for sym, ri in self._cap_retire_pending.items():
            intents[sym] = ri
        for m in self._managed.values():
            if not self._is_due_retire(m, now):
                continue
            if now >= m.ttl_deadline:
                reason = "ttl_expired"
            else:
                reason = "missing_from_file"
            intents[m.symbol] = RetireIntent(
                symbol=m.symbol,
                symbol_safe_id=m.symbol_safe_id,
                reason=reason,
                unsubscribe_deadline=(
                    to_iso_z(m.unsubscribe_deadline) if m.unsubscribe_deadline else None
                ),
            )
        return [intents[k] for k in sorted(intents.keys())]

    def _is_due_retire(self, m: _Managed, now: datetime) -> bool:
        if now >= m.ttl_deadline:
            return True
        if m.lifecycle == "retiring" and m.unsubscribe_deadline is not None:
            if now >= m.unsubscribe_deadline:
                return True
        return False

    def _to_subscribe_intent(self, m: _Managed) -> SubscribeIntent:
        return SubscribeIntent(
            symbol=m.symbol,
            symbol_safe_id=m.symbol_safe_id,
            tier_key=m.tier_key,
            source_state=m.source_state,
            streams=m.subscribe,
            priority=m.priority,
            scan_score=m.scan_score,
            move_side=m.move_side,
            trigger_type=m.trigger_type,
            min_collect_seconds=m.min_collect_seconds,
            ttl_seconds=m.ttl_seconds,
            lifecycle=m.lifecycle,
            first_seen_at=to_iso_z(m.first_seen_at),
            last_target_seen_at=to_iso_z(m.last_target_seen_at),
        )

    def mark_subscribed(self, symbol: str) -> None:
        sym = symbol.strip().upper()
        m = self._managed.get(sym)
        if m is None:
            return
        if m.lifecycle == "new":
            m.lifecycle = "warming"

    def mark_unsubscribed(self, symbol: str) -> None:
        sym = symbol.strip().upper()
        self._cap_retire_pending.pop(sym, None)
        self._managed.pop(sym, None)

    def update_quality_state(
        self,
        symbol: str,
        ready: bool,
        last_event_at: datetime | None = None,
    ) -> None:
        _ = last_event_at
        sym = symbol.strip().upper()
        m = self._managed.get(sym)
        if m is None:
            return
        if m.lifecycle == "retiring":
            return
        if ready:
            m.lifecycle = "active"
        elif m.lifecycle == "active":
            m.lifecycle = "warming"

    def get_target_view_for_heartbeat(self) -> dict[str, Any]:
        lr = self._last_reload
        return {
            "target_age_sec": self._last_target_age_sec,
            "target_status": self._last_target_status,
            "reload_status": lr.status if lr else "unknown",
            "reload_loaded": lr.loaded if lr else False,
        }
