"""Step1.5 persistent snapshot daemon helpers.

The daemon keeps the existing futures_light_snapshot.json contract as the
single downstream entry while spreading live REST refresh across small shards.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import orjson

from laoma_signal_engine.core.config_loader import EngineConfig
from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import parse_iso_z, to_iso_z, utc_now
from laoma_signal_engine.core.models import CandidateUniverseDocument
from laoma_signal_engine.market.light_snapshot_async import run_fetch_light_snapshot_async
from laoma_signal_engine.market.light_snapshot_models import FuturesLightSnapshotDocument, LightSnapshotItem
from laoma_signal_engine.market.light_snapshot_settings import LightSnapshotSettings, load_light_snapshot_settings
from laoma_signal_engine.market.rest_circuit import close_rest_circuit, read_rest_circuit
from laoma_signal_engine.universe.step15_symbols import futures_symbols_for_step_1_5

SNAPSHOT_DAEMON_SCHEMA = "STEP1.69_step15_snapshot_daemon_v1"


def _resolve_runtime_path(project_root: Path, configured: str) -> Path:
    path = Path(configured)
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def pid_path(project_root: Path, settings: LightSnapshotSettings | None = None) -> Path:
    settings = settings or load_light_snapshot_settings()
    return _resolve_runtime_path(project_root, settings.step15_daemon_pid_path)


def heartbeat_path(project_root: Path, settings: LightSnapshotSettings | None = None) -> Path:
    settings = settings or load_light_snapshot_settings()
    return _resolve_runtime_path(project_root, settings.step15_daemon_heartbeat_path)


def status_path(project_root: Path, settings: LightSnapshotSettings | None = None) -> Path:
    settings = settings or load_light_snapshot_settings()
    return _resolve_runtime_path(project_root, settings.step15_daemon_status_path)


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        raw = orjson.loads(path.read_bytes())
    except (OSError, orjson.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _pid_alive(pid: Any) -> bool:
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        return False
    if pid_int <= 0:
        return False
    try:
        os.kill(pid_int, 0)
        return True
    except OSError:
        return False


def _age_sec(value: Any) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return max(0, int((utc_now() - parse_iso_z(value)).total_seconds()))
    except ValueError:
        return None


def _write_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, payload)


def _write_pid(project_root: Path, settings: LightSnapshotSettings) -> None:
    _write_payload(
        pid_path(project_root, settings),
        {
            "schema_version": SNAPSHOT_DAEMON_SCHEMA,
            "pid": os.getpid(),
            "updated_at": to_iso_z(utc_now()),
        },
    )


def _sla_for_item(item: LightSnapshotItem, settings: LightSnapshotSettings) -> tuple[str, int, int]:
    pool = str(item.primary_pool or item.universe_profile.business_pool or "").lower()
    tier = str(item.tradability_profile.trade_quality_tier or item.tradability_profile.tradability_tier or "").lower()
    if pool in {"core", "active_exec", "tradeable_hot"} or tier in {"excellent", "good", "tradeable_hot"}:
        return "tradeable_hot", settings.freshness_tradeable_hot_fresh_sec, settings.freshness_tradeable_hot_stale_usable_sec
    if pool in {"active", "emerging", "active_watch"} or tier in {"watch", "medium"}:
        return "active_watch", settings.freshness_active_watch_fresh_sec, settings.freshness_active_watch_stale_usable_sec
    if pool in {"watch", "watch_only"}:
        return "watch_only", settings.freshness_watch_only_fresh_sec, settings.freshness_watch_only_stale_usable_sec
    return "low_quality", settings.freshness_low_quality_fresh_sec, settings.freshness_low_quality_stale_usable_sec


def _with_freshness(
    item: LightSnapshotItem,
    *,
    settings: LightSnapshotSettings,
    source: str,
    age_sec: int | None,
    live_at: str | None,
    shard_id: str,
) -> LightSnapshotItem:
    profile, fresh_sec, stale_usable_sec = _sla_for_item(item, settings)
    age = int(age_sec) if age_sec is not None else stale_usable_sec + 1
    if age <= fresh_sec:
        freshness_status = "fresh"
        downstream_allowed = True
        downstream_scope = "tradeable"
    elif age <= stale_usable_sec:
        freshness_status = "stale_usable"
        downstream_allowed = True
        downstream_scope = "tradeable"
    else:
        freshness_status = "stale_blocked"
        downstream_allowed = False
        downstream_scope = "blocked"
    reasons = list(item.reason_codes or [])
    if source != "live_shard" and "market_snapshot_cache_used" not in reasons:
        reasons.append("market_snapshot_cache_used")
    if freshness_status == "stale_blocked" and "snapshot_stale_blocked" not in reasons:
        reasons.append("snapshot_stale_blocked")
    if source != "websocket_cache" and "websocket_snapshot_missing" not in reasons:
        reasons.append("websocket_snapshot_missing")
    source_priority = {
        "websocket_cache": "p1_websocket_cache",
        "live_shard": "p2_live_shard",
        "cache_merged": "p3_cache_merged",
        "stale_blocked": "p4_stale_blocked",
    }.get(source, "p5_missing")
    return item.model_copy(
        update={
            "reason_codes": sorted(set(reasons)),
            "item_snapshot_source": source,
            "item_snapshot_age_sec": age,
            "item_freshness_sla_sec": int(stale_usable_sec),
            "item_freshness_status": freshness_status,
            "item_downstream_allowed": downstream_allowed,
            "item_downstream_scope": downstream_scope,
            "last_live_refresh_at": live_at,
            "websocket_cache_generated_at": live_at if source == "websocket_cache" else None,
            "rest_cache_generated_at": live_at if source in {"live_shard", "cache_merged"} else item.rest_cache_generated_at,
            "snapshot_source_priority": source_priority,
            "shard_id": shard_id,
        }
    )


def _select_shard(symbols: list[str], *, cursor: int, size: int) -> tuple[list[str], int]:
    if not symbols:
        return [], 0
    start = max(0, cursor) % len(symbols)
    size = max(1, min(int(size), len(symbols)))
    out = [symbols[(start + idx) % len(symbols)] for idx in range(size)]
    return out, (start + size) % len(symbols)


def _shard_size(settings: LightSnapshotSettings, circuit_state: str) -> int:
    if circuit_state == "half_open":
        return max(1, settings.step15_daemon_shard_size_half_open)
    if circuit_state == "open":
        return 0
    return max(1, settings.step15_daemon_shard_size_normal)


def _recovery_state(prev_status: dict[str, Any], settings: LightSnapshotSettings, circuit_state: str) -> dict[str, Any]:
    streak = int(prev_status.get("rest_consecutive_successful_shards") or 0)
    closed_streak = int(prev_status.get("rest_closed_successful_shards") or 0)
    steps = sorted({max(1, int(x)) for x in (settings.step15_daemon_half_open_expand_steps or [settings.step15_daemon_shard_size_half_open])})
    if circuit_state == "open":
        return {
            "rest_recovery_stage": "open_cooldown",
            "rest_consecutive_successful_shards": 0,
            "rest_closed_successful_shards": closed_streak,
            "current_shard_size": 0,
            "next_shard_size": 0,
            "rest_success_required_for_close": int(settings.step15_daemon_close_after_successful_shards),
            "half_open_success_required": int(settings.step15_daemon_half_open_success_required),
        }
    if circuit_state == "half_open":
        # Expand in small deterministic steps. The first N successes stay at the
        # initial half-open shard size; later streaks move through configured steps.
        idx = min(len(steps) - 1, max(0, streak // max(1, int(settings.step15_daemon_half_open_success_required))))
        size = steps[idx]
        next_idx = min(len(steps) - 1, max(0, (streak + 1) // max(1, int(settings.step15_daemon_half_open_success_required))))
        return {
            "rest_recovery_stage": "half_open_probe",
            "rest_consecutive_successful_shards": streak,
            "rest_closed_successful_shards": closed_streak,
            "current_shard_size": size,
            "next_shard_size": steps[next_idx],
            "rest_success_required_for_close": int(settings.step15_daemon_close_after_successful_shards),
            "half_open_success_required": int(settings.step15_daemon_half_open_success_required),
        }
    return {
        "rest_recovery_stage": "closed_normal",
        "rest_consecutive_successful_shards": streak,
        "rest_closed_successful_shards": closed_streak,
        "current_shard_size": max(1, int(settings.step15_daemon_shard_size_normal)),
        "next_shard_size": max(1, int(settings.step15_daemon_shard_size_normal)),
        "rest_success_required_for_close": int(settings.step15_daemon_close_after_successful_shards),
        "half_open_success_required": int(settings.step15_daemon_half_open_success_required),
    }


def _recovery_after_success(project_root: Path, prev_status: dict[str, Any], settings: LightSnapshotSettings) -> tuple[dict[str, Any], list[str]]:
    circuit = read_rest_circuit(project_root)
    state = str(circuit.get("rest_circuit_state") or "closed")
    reasons: list[str] = []
    streak = int(prev_status.get("rest_consecutive_successful_shards") or 0)
    closed_streak = int(prev_status.get("rest_closed_successful_shards") or 0)
    if state == "half_open":
        streak += 1
        closed_streak += 1
        if closed_streak >= int(settings.step15_daemon_close_after_successful_shards):
            close_rest_circuit(project_root, reason="snapshot_daemon_recovery_success_streak")
            state = "closed"
            streak = 0
            reasons.append("rest_circuit_closed_by_success_streak")
        else:
            reasons.append("rest_circuit_half_open_success")
    elif state == "closed":
        streak = 0
        closed_streak += 1
    else:
        streak = 0
    enriched_prev = {
        **prev_status,
        "rest_consecutive_successful_shards": streak,
        "rest_closed_successful_shards": closed_streak,
    }
    recovery = _recovery_state(enriched_prev, settings, state)
    return recovery, reasons


def _rest_budget_ledger_path(project_root: Path) -> Path:
    return project_root / "DATA" / "logs" / "snapshot_rest_budget.jsonl"


def _append_rest_budget_event(project_root: Path, record: dict[str, Any]) -> None:
    path = _rest_budget_ledger_path(project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE).decode("utf-8")
    with open(path, "a", encoding="utf-8", newline="") as fp:
        fp.write(payload)


def _latest_perf_record(project_root: Path, settings: LightSnapshotSettings) -> dict[str, Any]:
    path = Path(settings.async_perf_log_path)
    if not path.is_absolute():
        path = project_root / path
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    for line in reversed(lines[-50:]):
        try:
            doc = orjson.loads(line)
        except orjson.JSONDecodeError:
            continue
        if isinstance(doc, dict):
            return doc
    return {}


def _rest_budget_from_perf(perf: dict[str, Any], *, shard_size: int, shard_id: str, circuit: dict[str, Any]) -> dict[str, Any]:
    http_418 = int(perf.get("http_418_count") or 0)
    http_429 = int(perf.get("http_429_count") or 0)
    endpoint_counts = perf.get("rest_endpoint_counts") if isinstance(perf.get("rest_endpoint_counts"), dict) else {}
    status_counts = perf.get("rest_status_code_counts") if isinstance(perf.get("rest_status_code_counts"), dict) else {}
    estimated_requests = int(perf.get("request_count") or perf.get("latency_count") or perf.get("requested_count") or 0)
    return {
        "schema_version": "STEP1.75_rest_budget_v1",
        "generated_at": to_iso_z(utc_now()),
        "source": "snapshot_daemon",
        "shard_id": shard_id,
        "shard_size": int(shard_size),
        "rest_circuit_state": circuit.get("rest_circuit_state"),
        "rest_request_count": estimated_requests,
        "rest_weight_used": perf.get("used_weight_1m") or perf.get("rest_weight_used"),
        "rest_endpoint_counts": endpoint_counts,
        "rest_status_code_counts": status_counts,
        "status_418_count": http_418,
        "status_429_count": http_429,
        "retry_after_sec": circuit.get("retry_after_sec"),
        "cooldown_until": circuit.get("rest_circuit_until"),
        "shard_success_count": 1 if not (http_418 or http_429) else 0,
        "shard_failure_count": 1 if (http_418 or http_429) else 0,
        "hidden_caller_suspects": [],
        "caller_component": "step15_snapshot_daemon",
    }


def _source_counts(items: list[LightSnapshotItem]) -> dict[str, int]:
    counts = {
        "live_shard_count": 0,
        "cache_merged_count": 0,
        "websocket_cache_count": 0,
        "stale_blocked_count": 0,
        "missing_count": 0,
    }
    for item in items:
        source = item.item_snapshot_source or "missing"
        if source == "live_shard":
            counts["live_shard_count"] += 1
        elif source == "websocket_cache":
            counts["websocket_cache_count"] += 1
        elif source == "cache_merged":
            counts["cache_merged_count"] += 1
        else:
            counts["missing_count"] += 1
        if item.item_freshness_status == "stale_blocked":
            counts["stale_blocked_count"] += 1
    return counts


def _freshness_counts(items: list[LightSnapshotItem]) -> dict[str, int]:
    counts = {"fresh": 0, "stale_usable": 0, "stale_blocked": 0, "unknown": 0}
    for item in items:
        key = item.item_freshness_status or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _merge_shard_document(
    *,
    project_root: Path,
    settings: LightSnapshotSettings,
    current_path: Path,
    shard_path: Path,
    selected_symbols: list[str],
    next_cursor: int,
    shard_id: str,
) -> FuturesLightSnapshotDocument:
    shard_doc = FuturesLightSnapshotDocument.model_validate(read_json_object(shard_path))
    current_doc: FuturesLightSnapshotDocument | None = None
    if current_path.exists():
        try:
            current_doc = FuturesLightSnapshotDocument.model_validate(read_json_object(current_path))
        except Exception:
            current_doc = None
    now = utc_now()
    now_s = to_iso_z(now)
    selected = {s.upper() for s in selected_symbols}
    by_symbol: dict[str, LightSnapshotItem] = {}
    if current_doc:
        for item in current_doc.items:
            live_at = item.last_live_refresh_at or current_doc.generated_at
            age = _age_sec(live_at)
            by_symbol[item.symbol.upper()] = _with_freshness(
                item,
                settings=settings,
                source="cache_merged",
                age_sec=age,
                live_at=live_at,
                shard_id=item.shard_id or "previous",
            )
    for item in shard_doc.items:
        source = "live_shard" if item.symbol.upper() in selected else "cache_merged"
        by_symbol[item.symbol.upper()] = _with_freshness(
            item,
            settings=settings,
            source=source,
            age_sec=0,
            live_at=now_s,
            shard_id=shard_id,
        )
    ordered_symbols = list(by_symbol)
    try:
        cfg = EngineConfig.load(project_root)
        univ_doc = CandidateUniverseDocument.model_validate(read_json_object(cfg.candidate_universe_path))
        order = futures_symbols_for_step_1_5(univ_doc)
        ordered_symbols = [s for s in order if s in by_symbol] + sorted(set(by_symbol) - set(order))
    except Exception:
        ordered_symbols = sorted(by_symbol)
    items = [by_symbol[s] for s in ordered_symbols]
    success = sum(1 for item in items if item.primary_15m.ready and item.item_downstream_allowed is not False)
    failed = len(items) - success
    pools: dict[str, list[str]] = {}
    for item in items:
        pools.setdefault(item.primary_pool or "unknown", []).append(item.symbol.upper())
    pools = {k: sorted(set(v)) for k, v in sorted(pools.items())}
    source_mix = _source_counts(items)
    fresh_counts = _freshness_counts(items)
    reason_codes = set(str(x) for x in (shard_doc.snapshot_quality or {}).get("reason_codes") or [])
    reason_codes.add("step15_snapshot_daemon_merged")
    if source_mix["stale_blocked_count"]:
        reason_codes.add("snapshot_stale_blocked")
    circuit = read_rest_circuit(project_root)
    quality = {
        **(current_doc.snapshot_quality if current_doc else {}),
        **(shard_doc.snapshot_quality or {}),
        "snapshot_status": "ok" if failed == 0 else "partial",
        "snapshot_success_count": success,
        "snapshot_failed_count": failed,
        "downstream_candidate_count": success,
        "market_snapshot_source": "daemon_merged",
        "market_snapshot_live_attempted": True,
        "market_snapshot_freshness_tier": "fresh" if fresh_counts.get("stale_blocked", 0) == 0 else "mixed",
        "step15_daemon_enabled": True,
        "snapshot_runtime_mode": "daemon",
        "daemon_status": "running",
        "daemon_heartbeat_at": now_s,
        "daemon_heartbeat_age_sec": 0,
        "daemon_cycle_id": shard_id,
        "current_shard_id": shard_id,
        "next_shard_cursor": next_cursor,
        "planned_symbols": selected_symbols,
        "live_refreshed_symbols": [item.symbol.upper() for item in shard_doc.items],
        "symbol_source_mix": source_mix,
        "freshness_counts": fresh_counts,
        "rest_circuit_state": circuit.get("rest_circuit_state"),
        "rest_circuit_until": circuit.get("rest_circuit_until"),
        "rest_circuit_remaining_sec": circuit.get("rest_circuit_remaining_sec"),
        "rest_circuit_reason": circuit.get("rest_circuit_reason"),
        "reason_codes": sorted(reason_codes),
    }
    return FuturesLightSnapshotDocument(
        schema_version=shard_doc.schema_version,
        generated_at=now_s,
        source="binance_um_futures_daemon_merged",
        universe_generated_at=shard_doc.universe_generated_at,
        universe_age_sec=shard_doc.universe_age_sec,
        universe_count=shard_doc.universe_count,
        eligible_futures_count=shard_doc.eligible_futures_count,
        snapshot_count=len(items),
        success_count=success,
        failed_count=failed,
        skipped_count=max(0, int(shard_doc.eligible_futures_count or 0) - len(items)),
        timeframe_contract=shard_doc.timeframe_contract,
        items=items,
        errors=shard_doc.errors,
        pools=pools,
        snapshot_quality=quality,
    )


def _fallback_merge_current_document(
    *,
    settings: LightSnapshotSettings,
    current_path: Path,
    shard_id: str,
    reason: str,
) -> FuturesLightSnapshotDocument | None:
    if not current_path.exists():
        return None
    try:
        current_doc = FuturesLightSnapshotDocument.model_validate(read_json_object(current_path))
    except Exception:
        return None
    now_s = to_iso_z(utc_now())
    items: list[LightSnapshotItem] = []
    for item in current_doc.items:
        live_at = item.last_live_refresh_at or current_doc.generated_at
        items.append(
            _with_freshness(
                item,
                settings=settings,
                source=item.item_snapshot_source or "cache_merged",
                age_sec=_age_sec(live_at),
                live_at=live_at,
                shard_id=item.shard_id or "previous",
            )
        )
    success = sum(1 for item in items if item.primary_15m.ready and item.item_downstream_allowed is not False)
    failed = len(items) - success
    source_mix = _source_counts(items)
    fresh_counts = _freshness_counts(items)
    reason_codes = set(str(x) for x in (current_doc.snapshot_quality or {}).get("reason_codes") or [])
    reason_codes.add(reason)
    reason_codes.add("step15_snapshot_daemon_cache_fallback")
    if source_mix["stale_blocked_count"]:
        reason_codes.add("snapshot_stale_blocked")
    quality = {
        **(current_doc.snapshot_quality or {}),
        "snapshot_status": "degraded_cache" if failed == 0 else "partial",
        "snapshot_success_count": success,
        "snapshot_failed_count": failed,
        "downstream_candidate_count": success,
        "market_snapshot_source": "daemon_cache_fallback",
        "market_snapshot_live_attempted": False,
        "market_snapshot_freshness_tier": "fresh" if fresh_counts.get("stale_blocked", 0) == 0 else "mixed",
        "step15_daemon_enabled": True,
        "snapshot_runtime_mode": "daemon",
        "daemon_status": "degraded_cache",
        "daemon_heartbeat_at": now_s,
        "daemon_heartbeat_age_sec": 0,
        "daemon_cycle_id": shard_id,
        "current_shard_id": shard_id,
        "symbol_source_mix": source_mix,
        "freshness_counts": fresh_counts,
        "reason_codes": sorted(reason_codes),
    }
    return current_doc.model_copy(
        update={
            "generated_at": now_s,
            "source": "binance_um_futures_daemon_cache_fallback",
            "snapshot_count": len(items),
            "success_count": success,
            "failed_count": failed,
            "skipped_count": max(0, int(current_doc.eligible_futures_count or 0) - len(items)),
            "items": items,
            "snapshot_quality": quality,
        }
    )


def _status_payload(
    *,
    project_root: Path,
    settings: LightSnapshotSettings,
    daemon_status: str,
    watchdog_status: str,
    watchdog_action: str,
    current_shard_id: str | None,
    next_shard_cursor: int | None,
    next_shard_at: str | None,
    queue_depth: int,
    source_mix: dict[str, Any] | None,
    freshness_counts: dict[str, Any] | None,
    reason_codes: list[str],
    last_successful_shard_at: str | None = None,
    last_error: str | None = None,
    restart_count: int = 0,
    recovery: dict[str, Any] | None = None,
    rest_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now_s = to_iso_z(utc_now())
    circuit = read_rest_circuit(project_root)
    pid = os.getpid()
    recovery = recovery or _recovery_state({}, settings, str(circuit.get("rest_circuit_state") or "closed"))
    rest_budget = rest_budget or {}
    return {
        "schema_version": SNAPSHOT_DAEMON_SCHEMA,
        "generated_at": now_s,
        "daemon_status": daemon_status,
        "pid": pid,
        "pid_alive": True,
        "heartbeat_at": now_s,
        "heartbeat_age_sec": 0,
        "stale_after_sec": int(settings.step15_daemon_stale_after_sec),
        "last_tick_at": now_s,
        "last_successful_shard_at": last_successful_shard_at,
        "current_shard_id": current_shard_id,
        "next_shard_cursor": next_shard_cursor,
        "next_shard_at": next_shard_at,
        "queue_depth": queue_depth,
        "source_mix": source_mix or {},
        "freshness_counts": freshness_counts or {},
        "rest_budget_state": circuit.get("rest_circuit_state"),
        "rest_circuit_state": circuit.get("rest_circuit_state"),
        "rest_recovery_stage": recovery.get("rest_recovery_stage"),
        "rest_consecutive_successful_shards": recovery.get("rest_consecutive_successful_shards"),
        "rest_closed_successful_shards": recovery.get("rest_closed_successful_shards"),
        "rest_success_required_for_close": recovery.get("rest_success_required_for_close"),
        "half_open_success_required": recovery.get("half_open_success_required"),
        "current_shard_size": recovery.get("current_shard_size"),
        "next_shard_size": recovery.get("next_shard_size"),
        "rest_cooldown_until": circuit.get("rest_circuit_until"),
        "rest_circuit_remaining_sec": circuit.get("rest_circuit_remaining_sec"),
        "rest_request_count": rest_budget.get("rest_request_count", 0),
        "rest_weight_used": rest_budget.get("rest_weight_used"),
        "rest_endpoint_counts": rest_budget.get("rest_endpoint_counts", {}),
        "rest_status_code_counts": rest_budget.get("rest_status_code_counts", {}),
        "status_418_count": rest_budget.get("status_418_count", 0),
        "status_429_count": rest_budget.get("status_429_count", 0),
        "retry_after_sec": rest_budget.get("retry_after_sec"),
        "cooldown_until": rest_budget.get("cooldown_until"),
        "watchdog_status": watchdog_status,
        "watchdog_action": watchdog_action,
        "restart_count": restart_count,
        "last_error": last_error,
        "reason_codes": sorted(set(reason_codes)),
    }


def _write_status_and_heartbeat(project_root: Path, settings: LightSnapshotSettings, payload: dict[str, Any]) -> None:
    _write_payload(status_path(project_root, settings), payload)
    _write_payload(heartbeat_path(project_root, settings), payload)


async def run_snapshot_daemon_tick(
    *,
    project_root: Path | None = None,
    settings: LightSnapshotSettings | None = None,
) -> int:
    cfg = EngineConfig.load(project_root)
    root = cfg.project_root
    settings = settings or load_light_snapshot_settings()
    _write_pid(root, settings)
    prev_status = _read_json_dict(status_path(root, settings))
    restart_count = int(prev_status.get("restart_count") or 0)
    if not settings.step15_daemon_enabled:
        payload = _status_payload(
            project_root=root,
            settings=settings,
            daemon_status="disabled",
            watchdog_status="disabled",
            watchdog_action="none",
            current_shard_id=None,
            next_shard_cursor=int(prev_status.get("next_shard_cursor") or 0),
            next_shard_at=None,
            queue_depth=0,
            source_mix=None,
            freshness_counts=None,
            reason_codes=["step15_snapshot_daemon_disabled"],
            restart_count=restart_count,
        )
        _write_status_and_heartbeat(root, settings, payload)
        return EXIT_SUCCESS
    circuit = read_rest_circuit(root)
    recovery = _recovery_state(prev_status, settings, str(circuit.get("rest_circuit_state") or "closed"))
    if circuit.get("rest_circuit_state") == "open":
        payload = _status_payload(
            project_root=root,
            settings=settings,
            daemon_status="paused",
            watchdog_status="paused",
            watchdog_action="pause_live_refresh",
            current_shard_id=prev_status.get("current_shard_id"),
            next_shard_cursor=int(prev_status.get("next_shard_cursor") or 0),
            next_shard_at=circuit.get("rest_circuit_until"),
            queue_depth=0,
            source_mix=prev_status.get("source_mix") if isinstance(prev_status.get("source_mix"), dict) else {},
            freshness_counts=prev_status.get("freshness_counts") if isinstance(prev_status.get("freshness_counts"), dict) else {},
            reason_codes=["rest_circuit_open", "snapshot_daemon_live_refresh_paused"],
            last_successful_shard_at=prev_status.get("last_successful_shard_at"),
            restart_count=restart_count,
            recovery=recovery,
        )
        _write_status_and_heartbeat(root, settings, payload)
        return EXIT_SUCCESS
    try:
        univ = CandidateUniverseDocument.model_validate(read_json_object(cfg.candidate_universe_path))
    except Exception as exc:
        payload = _status_payload(
            project_root=root,
            settings=settings,
            daemon_status="stale",
            watchdog_status="stale",
            watchdog_action="alert",
            current_shard_id=None,
            next_shard_cursor=int(prev_status.get("next_shard_cursor") or 0),
            next_shard_at=None,
            queue_depth=0,
            source_mix=None,
            freshness_counts=None,
            reason_codes=["candidate_universe_missing", "snapshot_daemon_tick_failed"],
            last_error=str(exc)[:300],
            restart_count=restart_count,
        )
        _write_status_and_heartbeat(root, settings, payload)
        return EXIT_CONFIG
    symbols = futures_symbols_for_step_1_5(univ)
    cursor = int(prev_status.get("next_shard_cursor") or 0)
    recovery = _recovery_state(prev_status, settings, str(circuit.get("rest_circuit_state") or "closed"))
    size = int(recovery.get("current_shard_size") or _shard_size(settings, str(circuit.get("rest_circuit_state") or "closed")))
    shard_symbols, next_cursor = _select_shard(symbols, cursor=cursor, size=size)
    shard_id = f"shard_{utc_now():%Y%m%dT%H%M%SZ}_{cursor}_{next_cursor}"
    runtime_dir = root / "DATA" / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    tmp_shard_path = runtime_dir / "step15_snapshot_daemon_last_shard.json"
    current_path = cfg.futures_light_snapshot_path
    live_settings = replace(settings, market_snapshot_cache_first_enabled=False)
    rc = await run_fetch_light_snapshot_async(
        project_root=root,
        symbols_filter=shard_symbols,
        output_path=tmp_shard_path,
        settings=live_settings,
        perf_fetch_mode="snapshot_daemon_shard",
    )
    if rc != EXIT_SUCCESS:
        fallback = _fallback_merge_current_document(
            settings=settings,
            current_path=current_path,
            shard_id=shard_id,
            reason="snapshot_daemon_shard_refresh_failed",
        )
        if fallback is not None:
            _write_payload(current_path, fallback.model_dump(mode="json"))
            source_mix = _source_counts(fallback.items)
            fresh_counts = _freshness_counts(fallback.items)
            payload = _status_payload(
                project_root=root,
                settings=settings,
                daemon_status="degraded_cache",
                watchdog_status="warn",
                watchdog_action="cache_fallback",
                current_shard_id=shard_id,
                next_shard_cursor=cursor,
                next_shard_at=to_iso_z(utc_now() + timedelta(seconds=settings.step15_daemon_tick_sec)),
                queue_depth=len(symbols),
                source_mix=source_mix,
                freshness_counts=fresh_counts,
                reason_codes=["snapshot_daemon_shard_refresh_failed", "step15_snapshot_daemon_cache_fallback"],
                last_successful_shard_at=prev_status.get("last_successful_shard_at"),
                last_error=f"shard_refresh_exit_{rc}",
                restart_count=restart_count,
                recovery=_recovery_state(
                    {**prev_status, "rest_consecutive_successful_shards": 0},
                    settings,
                    str(read_rest_circuit(root).get("rest_circuit_state") or "closed"),
                ),
            )
            _write_status_and_heartbeat(root, settings, payload)
            return EXIT_SUCCESS
        payload = _status_payload(
            project_root=root,
            settings=settings,
            daemon_status="stale",
            watchdog_status="stale",
            watchdog_action="alert",
            current_shard_id=shard_id,
            next_shard_cursor=cursor,
            next_shard_at=None,
            queue_depth=len(symbols),
            source_mix=prev_status.get("source_mix") if isinstance(prev_status.get("source_mix"), dict) else {},
            freshness_counts=prev_status.get("freshness_counts") if isinstance(prev_status.get("freshness_counts"), dict) else {},
            reason_codes=["snapshot_daemon_shard_refresh_failed"],
            last_successful_shard_at=prev_status.get("last_successful_shard_at"),
            last_error=f"shard_refresh_exit_{rc}",
            restart_count=restart_count,
            recovery=_recovery_state({**prev_status, "rest_consecutive_successful_shards": 0}, settings, str(read_rest_circuit(root).get("rest_circuit_state") or "closed")),
        )
        _write_status_and_heartbeat(root, settings, payload)
        return rc
    try:
        merged = _merge_shard_document(
            project_root=root,
            settings=settings,
            current_path=cfg.futures_light_snapshot_path,
            shard_path=tmp_shard_path,
            selected_symbols=shard_symbols,
            next_cursor=next_cursor,
            shard_id=shard_id,
        )
        write_json_atomic(cfg.futures_light_snapshot_path, merged.model_dump(mode="json"))
    except Exception as exc:
        payload = _status_payload(
            project_root=root,
            settings=settings,
            daemon_status="stale",
            watchdog_status="stale",
            watchdog_action="alert",
            current_shard_id=shard_id,
            next_shard_cursor=cursor,
            next_shard_at=None,
            queue_depth=len(symbols),
            source_mix=None,
            freshness_counts=None,
            reason_codes=["snapshot_daemon_merge_failed"],
            last_error=str(exc)[:300],
            restart_count=restart_count,
        )
        _write_status_and_heartbeat(root, settings, payload)
        return EXIT_INTERNAL
    recovery_after, recovery_reasons = _recovery_after_success(root, prev_status, settings)
    perf = _latest_perf_record(root, settings)
    rest_budget = _rest_budget_from_perf(perf, shard_size=len(shard_symbols), shard_id=shard_id, circuit=read_rest_circuit(root))
    try:
        _append_rest_budget_event(root, rest_budget)
    except OSError:
        pass
    quality = merged.snapshot_quality or {}
    quality.update(
        {
            "rest_recovery_stage": recovery_after.get("rest_recovery_stage"),
            "rest_consecutive_successful_shards": recovery_after.get("rest_consecutive_successful_shards"),
            "rest_closed_successful_shards": recovery_after.get("rest_closed_successful_shards"),
            "rest_success_required_for_close": recovery_after.get("rest_success_required_for_close"),
            "current_shard_size": recovery_after.get("current_shard_size"),
            "next_shard_size": recovery_after.get("next_shard_size"),
            "rest_request_count": rest_budget.get("rest_request_count"),
            "rest_weight_used": rest_budget.get("rest_weight_used"),
            "rest_endpoint_counts": rest_budget.get("rest_endpoint_counts"),
            "rest_status_code_counts": rest_budget.get("rest_status_code_counts"),
            "status_418_count": rest_budget.get("status_418_count"),
            "status_429_count": rest_budget.get("status_429_count"),
        }
    )
    merged = merged.model_copy(update={"snapshot_quality": quality})
    write_json_atomic(cfg.futures_light_snapshot_path, merged.model_dump(mode="json"))
    next_shard_at = to_iso_z(utc_now())
    payload = _status_payload(
        project_root=root,
        settings=settings,
        daemon_status="running",
        watchdog_status="healthy",
        watchdog_action="none",
        current_shard_id=shard_id,
        next_shard_cursor=next_cursor,
        next_shard_at=next_shard_at,
        queue_depth=len(symbols),
        source_mix=quality.get("symbol_source_mix") if isinstance(quality.get("symbol_source_mix"), dict) else {},
        freshness_counts=quality.get("freshness_counts") if isinstance(quality.get("freshness_counts"), dict) else {},
        reason_codes=["snapshot_daemon_tick_ok", *recovery_reasons],
        last_successful_shard_at=merged.generated_at,
        restart_count=restart_count,
        recovery=recovery_after,
        rest_budget=rest_budget,
    )
    _write_status_and_heartbeat(root, settings, payload)
    return EXIT_SUCCESS


def run_snapshot_daemon_tick_safe(*, project_root: Path | None = None) -> int:
    try:
        return asyncio.run(run_snapshot_daemon_tick(project_root=project_root))
    except Exception:
        return EXIT_INTERNAL


async def run_snapshot_daemon_forever(*, project_root: Path | None = None, max_ticks: int | None = None) -> int:
    settings = load_light_snapshot_settings()
    tick = max(1, int(settings.step15_daemon_tick_sec))
    count = 0
    while True:
        rc = await run_snapshot_daemon_tick(project_root=project_root, settings=settings)
        count += 1
        if max_ticks is not None and count >= max_ticks:
            return rc
        await asyncio.sleep(tick)


def run_snapshot_daemon_forever_safe(*, project_root: Path | None = None, max_ticks: int | None = None) -> int:
    try:
        return asyncio.run(run_snapshot_daemon_forever(project_root=project_root, max_ticks=max_ticks))
    except KeyboardInterrupt:
        return EXIT_SUCCESS
    except Exception:
        return EXIT_INTERNAL


def snapshot_daemon_status(project_root: Path | None = None) -> dict[str, Any]:
    cfg = EngineConfig.load(project_root)
    root = cfg.project_root
    settings = load_light_snapshot_settings()
    status = _read_json_dict(status_path(root, settings))
    heartbeat = _read_json_dict(heartbeat_path(root, settings))
    pid_doc = _read_json_dict(pid_path(root, settings))
    heartbeat_at = heartbeat.get("heartbeat_at") or status.get("heartbeat_at")
    age = _age_sec(heartbeat_at)
    stale_after = int(status.get("stale_after_sec") or settings.step15_daemon_stale_after_sec)
    pid = status.get("pid") or pid_doc.get("pid")
    pid_alive = _pid_alive(pid)
    daemon_status = str(status.get("daemon_status") or ("missing" if not status and not heartbeat else "unknown"))
    if age is None:
        daemon_status = "missing"
        health_status = "fail"
        watchdog_status = "stale"
        watchdog_action = "alert"
    elif age > stale_after:
        daemon_status = "stale" if daemon_status == "running" else daemon_status
        health_status = "fail"
        watchdog_status = "stale"
        watchdog_action = "alert"
    elif daemon_status in {"paused"}:
        health_status = "warn"
        watchdog_status = "paused"
        watchdog_action = "pause_live_refresh"
    else:
        health_status = "ok" if pid_alive or daemon_status in {"running", "disabled"} else "warn"
        watchdog_status = str(status.get("watchdog_status") or "healthy")
        watchdog_action = str(status.get("watchdog_action") or "none")
    circuit = read_rest_circuit(root)
    return {
        "schema_version": SNAPSHOT_DAEMON_SCHEMA,
        "source": "step15_snapshot_daemon_status",
        "source_path": str(status_path(root, settings)),
        "heartbeat_path": str(heartbeat_path(root, settings)),
        "pid_path": str(pid_path(root, settings)),
        "generated_at": to_iso_z(utc_now()),
        "status": health_status,
        "daemon_status": daemon_status,
        "heartbeat_at": heartbeat_at,
        "heartbeat_age_sec": age,
        "stale_after_sec": stale_after,
        "pid": pid,
        "pid_alive": pid_alive,
        "watchdog_status": watchdog_status,
        "watchdog_action": watchdog_action,
        "restart_count": int(status.get("restart_count") or 0),
        "last_tick_at": status.get("last_tick_at"),
        "last_successful_shard_at": status.get("last_successful_shard_at"),
        "current_shard_id": status.get("current_shard_id"),
        "next_shard_at": status.get("next_shard_at"),
        "next_shard_cursor": status.get("next_shard_cursor"),
        "queue_depth": int(status.get("queue_depth") or 0),
        "source_mix": status.get("source_mix") if isinstance(status.get("source_mix"), dict) else {},
        "freshness_counts": status.get("freshness_counts") if isinstance(status.get("freshness_counts"), dict) else {},
        "rest_budget_state": circuit.get("rest_circuit_state"),
        "rest_circuit_state": circuit.get("rest_circuit_state"),
        "rest_recovery_stage": status.get("rest_recovery_stage"),
        "rest_consecutive_successful_shards": status.get("rest_consecutive_successful_shards"),
        "rest_closed_successful_shards": status.get("rest_closed_successful_shards"),
        "rest_success_required_for_close": status.get("rest_success_required_for_close"),
        "half_open_success_required": status.get("half_open_success_required"),
        "current_shard_size": status.get("current_shard_size"),
        "next_shard_size": status.get("next_shard_size"),
        "rest_cooldown_until": circuit.get("rest_circuit_until"),
        "rest_circuit_remaining_sec": circuit.get("rest_circuit_remaining_sec"),
        "rest_request_count": status.get("rest_request_count", 0),
        "rest_weight_used": status.get("rest_weight_used"),
        "rest_endpoint_counts": status.get("rest_endpoint_counts") if isinstance(status.get("rest_endpoint_counts"), dict) else {},
        "rest_status_code_counts": status.get("rest_status_code_counts") if isinstance(status.get("rest_status_code_counts"), dict) else {},
        "status_418_count": status.get("status_418_count", 0),
        "status_429_count": status.get("status_429_count", 0),
        "retry_after_sec": status.get("retry_after_sec"),
        "cooldown_until": status.get("cooldown_until"),
        "last_error": status.get("last_error"),
        "reason_codes": status.get("reason_codes") if isinstance(status.get("reason_codes"), list) else [],
        "raw": status,
    }
