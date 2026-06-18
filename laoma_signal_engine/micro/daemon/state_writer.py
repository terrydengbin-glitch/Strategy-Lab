"""Build and write DATA/micro/latest_micro_state.json for STEP10.7."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from laoma_signal_engine.core.json_io import write_json_atomic
from laoma_signal_engine.core.time_utils import parse_iso_z, to_iso_z
from laoma_signal_engine.micro.daemon.state_models import (
    MicroDaemonStateDocument,
    MicroDaemonSymbolState,
    TargetChurnState,
)
from laoma_signal_engine.micro.quality.models import MicroQualitySnapshot
from laoma_signal_engine.micro.target_intent_models import SubscribeIntent


def _target_version(target_generated_at: str) -> str:
    return target_generated_at.replace("-", "").replace(":", "").replace("Z", "Z")


def _churn_state(intent: SubscribeIntent) -> TargetChurnState:
    if intent.lifecycle == "new":
        return "new"
    if intent.lifecycle == "retiring":
        return "retiring"
    return "kept"


def _age_sec(now: datetime, started_at: str) -> int:
    try:
        start = parse_iso_z(started_at)
    except ValueError:
        return 0
    return max(0, int((now - start).total_seconds()))


def build_micro_daemon_state_document(
    *,
    generated_at: str,
    now_dt: datetime,
    target_generated_at: str,
    target_age_sec: int,
    intents: list[SubscribeIntent],
    full_quality_by_symbol: dict[str, MicroQualitySnapshot],
    fast_quality_by_symbol: dict[str, MicroQualitySnapshot],
    full_min_collect_sec: int = 900,
    daemon_ok: bool = True,
    health_state_override: str | None = None,
    root_reason_codes: list[str] | None = None,
    subscription_state_by_symbol: dict[str, dict[str, dict[str, object]]] | None = None,
    health_guard_state_by_symbol: dict[str, dict[str, object]] | None = None,
) -> MicroDaemonStateDocument:
    symbols: list[MicroDaemonSymbolState] = []
    root_reasons: list[str] = list(root_reason_codes or [])
    for it in sorted(intents, key=lambda x: x.symbol):
        sym = it.symbol.strip().upper()
        full_q = full_quality_by_symbol.get(sym)
        fast_q = fast_quality_by_symbol.get(sym)
        continuous = _age_sec(now_dt, it.first_seen_at)
        full_ready = bool(full_q.ready) if full_q is not None else False
        fast_ready = bool(fast_q.ready) if fast_q is not None else False
        full_reasons = list(full_q.reason_codes) if full_q is not None else ["full_quality_missing"]
        fast_reasons = list(fast_q.reason_codes) if fast_q is not None else ["fast_quality_missing"]
        eta = 0 if full_ready else max(0, int(full_min_collect_sec) - continuous)
        consumer_reasons: list[str] = []
        if fast_q is None or full_q is None:
            consumer_reasons.append("quality_missing")
        if it.lifecycle == "new":
            consumer_reasons.append("target_new_warmup")
        consumer_safe = "quality_missing" not in consumer_reasons
        symbols.append(
            MicroDaemonSymbolState(
                symbol=sym,
                target_status="retiring" if it.lifecycle == "retiring" else "active",
                source_state=it.source_state,
                move_side=it.move_side,
                priority=it.priority,
                first_seen_at=it.first_seen_at,
                last_seen_at=it.last_target_seen_at,
                continuous_collect_sec=continuous,
                seen_cycle_count=max(1, continuous // 300 + 1),
                fast_ready=fast_ready,
                full_ready=full_ready,
                fast_reason_codes=fast_reasons,
                full_reason_codes=full_reasons,
                full_ready_eta_sec=eta,
                last_micro_generated_at=generated_at,
                target_churn_state=_churn_state(it),
                consumer_safe=consumer_safe,
                consumer_reason_codes=consumer_reasons,
                subscription_state=subscription_state_by_symbol.get(sym, {}) if subscription_state_by_symbol else {},
                health_guard_state=health_guard_state_by_symbol.get(sym, {}) if health_guard_state_by_symbol else {},
            )
        )
    if not daemon_ok:
        root_reasons.append("daemon_error")
    elif not symbols:
        root_reasons.append("idle_no_valid_targets")
    health_state = health_state_override or (
        "healthy_idle" if daemon_ok and not symbols else ("running" if daemon_ok else "error")
    )
    return MicroDaemonStateDocument(
        generated_at=generated_at,
        daemon_status="running" if daemon_ok else "error",
        health_state=health_state,
        target_generated_at=target_generated_at,
        target_version=_target_version(target_generated_at),
        target_age_sec=target_age_sec,
        active_symbol_count=len(symbols),
        state_ready_for_consumers=daemon_ok and bool(symbols),
        reason_codes=root_reasons,
        symbols=symbols,
    )


def atomic_write_micro_state(path: Path, document: MicroDaemonStateDocument) -> None:
    write_json_atomic(path, document.model_dump(mode="json"))
