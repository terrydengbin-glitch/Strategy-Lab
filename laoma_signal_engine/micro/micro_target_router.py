"""Step 2.5 Micro Target Router (docs/STEP2.5_任务卡.md)."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import orjson

from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.core.config_loader import EngineConfig
from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.time_utils import parse_iso_z, to_iso_z, utc_now
from laoma_signal_engine.micro.micro_target_models import (
    CandidateAlignmentBlock,
    InputCountsBlock,
    MicroTargetEntry,
    MicroTargetsDocument,
    RoutedCountsBlock,
    TruncatedBlock,
)
from laoma_signal_engine.micro.target_source_ledger import ingest_target_source_ledger
from laoma_signal_engine.scanner.freshness_gate import snapshot_age_sec
from laoma_signal_engine.scanner.signal_models import AbnormalSignalEntry, AbnormalTierDocument
from laoma_signal_engine.scanner.current_freshness import build_step2_current_freshness
from laoma_signal_engine.universe.manual_watchlist import load_manual_bases

log = logging.getLogger(__name__)

SKIP_WATCH_STALE = "watch_input_stale"
SKIP_STRONG_STALE = "strong_input_stale"
SKIP_RAW_STALE = "raw_input_stale"
SKIP_SNAPSHOT_MISMATCH = "input_snapshot_mismatch"
SKIP_TIER1_TRUNC = "tier1_truncated"
SKIP_TIER2_TRUNC = "tier2_truncated"
SKIP_TRADE_AVOID = "market_entry_avoid_excluded"
SKIP_STICKY_TRUNC = "sticky_pool_truncated"


def _rel_project_path(project_root: Path, path: Path) -> str:
    return path.resolve().relative_to(project_root.resolve()).as_posix()


def _input_routable(
    doc: AbnormalTierDocument,
    max_age_sec: int,
    *,
    router_age_sec: int,
) -> bool:
    if doc.status in ("stale_input", "ok_dev_stale_allowed"):
        return False
    if router_age_sec > max_age_sec:
        return False
    return True


def _sort_key_signal(
    sig: AbnormalSignalEntry,
    *,
    manual_bases: set[str],
    bonus: int,
    priority_mode: str = "scan_score",
) -> tuple[float, float, float, float, str]:
    base_priority = sig.trade_candidate_rank_score if priority_mode == "trade_candidate_rank" else sig.scan_score
    profile_priority = int(getattr(sig, "scan_priority", 0) or 0)
    eff_pri = min(100, max(base_priority, profile_priority) + (bonus if sig.base_asset.strip().upper() in manual_bases else 0))
    vr = sig.primary_15m.volume_ratio
    qv = sig.background.quote_volume_24h
    vr_k = -(float(vr) if vr is not None else float("-inf"))
    qv_k = -(float(qv) if qv is not None else float("-inf"))
    return (-eff_pri, -float(sig.trade_candidate_rank_score), -float(sig.scan_score), vr_k, sig.symbol.upper())


def _candidate_quality_counts(signals: list[AbnormalSignalEntry]) -> dict[str, int]:
    counts = {"preferred": 0, "allowed": 0, "observe": 0, "avoid": 0, "unknown": 0}
    for sig in signals:
        key = sig.trade_candidate_bucket if sig.trade_candidate_bucket in counts else "unknown"
        counts[key] += 1
    return counts


def _filter_for_micro(
    signals: list[AbnormalSignalEntry],
    *,
    exclude_market_entry_avoid: bool,
) -> tuple[list[AbnormalSignalEntry], int]:
    if not exclude_market_entry_avoid:
        kept = signals
    else:
        kept = [s for s in signals if s.market_entry_suitability != "avoid" and s.trade_candidate_bucket != "avoid"]
    before = len(kept)
    kept = [
        s
        for s in kept
        if s.risk_profile.execution_tier != "no_trade"
        and s.universe_profile.manual_mode not in {"blacklist", "no_trade", "exclude"}
        and s.universe_profile.scan_eligibility != "block"
        and s.universe_profile.business_pool != "no_trade"
        and s.tradability_profile.tradability_tier != "no_trade"
    ]
    if not exclude_market_entry_avoid:
        return kept, len(signals) - len(kept)
    return kept, (len(signals) - before) + (before - len(kept))


def _signal_to_entry(
    sig: AbnormalSignalEntry,
    *,
    cfg: EngineConfig,
    source_state: str,
    subscribe: tuple[str, ...],
    manual_bases: set[str],
) -> MicroTargetEntry:
    bonus_amt = cfg.mr_manual_watchlist_priority_bonus if sig.base_asset.strip().upper() in manual_bases else 0
    if sig.base_asset.strip().upper() in manual_bases and cfg.mr_manual_watchlist_priority_bonus > 0:
        log.debug("manual_watchlist_boosted symbol=%s base=%s", sig.symbol, sig.base_asset)
    base_priority = sig.trade_candidate_rank_score if cfg.mr_priority_mode == "trade_candidate_rank" else sig.scan_score
    profile_priority = int(getattr(sig, "scan_priority", 0) or 0)
    profile_priority = max(
        profile_priority,
        int(getattr(sig.tradability_profile, "market_entry_score", 0) or 0),
        int(getattr(sig.tradability_profile, "hf_stop_score", 0) or 0),
    )
    profile_boost = max(0, profile_priority - int(base_priority or 0))
    pri = min(100, max(base_priority, profile_priority) + bonus_amt)
    promoted_from_raw = bool(
        source_state == "raw_candidate"
        and cfg.step2_promote_raw_market_entry_allowed
        and sig.market_entry_suitability_score >= cfg.step2_raw_promote_min_market_entry_score
        and sig.scan_score >= cfg.step2_raw_promote_min_scan_score
    )
    ttl = (
        cfg.mr_ttl_seconds_tier2 if source_state == "strong_candidate" else cfg.mr_ttl_seconds_tier1
    )
    return MicroTargetEntry(
        symbol=sig.symbol.upper(),
        base_asset=sig.base_asset,
        source_state=source_state,
        priority=pri,
        scan_score=sig.scan_score,
        market_entry_suitability_score=sig.market_entry_suitability_score,
        market_entry_suitability=sig.market_entry_suitability,
        trade_candidate_rank_score=sig.trade_candidate_rank_score,
        trade_candidate_bucket=sig.trade_candidate_bucket,
        universe_profile=sig.universe_profile,
        risk_profile=sig.risk_profile,
        tradability_profile=sig.tradability_profile,
        primary_pool=sig.primary_pool,
        pool_tags=list(sig.pool_tags),
        scan_priority=profile_priority,
        profile_priority_boost=profile_boost,
        promoted_from_raw=promoted_from_raw or sig.promoted_from_raw,
        move_side=sig.move_side,
        trigger_type=sig.trigger_type,
        subscribe=list(subscribe),
        target_ready_tf=cfg.mr_target_ready_tf,
        min_collect_seconds=cfg.mr_min_collect_seconds,
        ttl_seconds=ttl,
    )


def _merged_snapshot_meta(
    watch_doc: AbnormalTierDocument,
    strong_doc: AbnormalTierDocument,
    *,
    router_age_watch: int,
    router_age_strong: int,
) -> tuple[str, int, int]:
    tw = parse_iso_z(watch_doc.input_snapshot_generated_at)
    ts = parse_iso_z(strong_doc.input_snapshot_generated_at)
    merged_dt = tw if tw <= ts else ts
    merged_gen_s = to_iso_z(merged_dt)
    merged_router_age = max(router_age_watch, router_age_strong)
    step2_rep = max(watch_doc.input_snapshot_age_sec, strong_doc.input_snapshot_age_sec)
    return merged_gen_s, merged_router_age, step2_rep


def _write_doc(path: Path, doc: MicroTargetsDocument) -> None:
    payload = doc.model_dump(mode="json")
    data = orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
    write_file_atomic(path, data)


def _load_previous_targets(path: Path) -> MicroTargetsDocument | None:
    try:
        return MicroTargetsDocument.model_validate(read_json_object(path))
    except (OSError, TypeError, ValueError):
        return None


def _load_daemon_symbols(path: Path) -> dict[str, dict[str, Any]]:
    try:
        doc = read_json_object(path)
    except (OSError, TypeError, ValueError):
        return {}
    if not isinstance(doc, dict):
        return {}
    symbols = doc.get("symbols")
    if not isinstance(symbols, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in symbols:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").upper().strip()
        if sym:
            out[sym] = row
    return out


def _entry_sticky_age_sec(
    entry: MicroTargetEntry,
    *,
    previous_doc: MicroTargetsDocument,
    now: datetime,
) -> int:
    try:
        doc_age = max(0, int((now - parse_iso_z(previous_doc.generated_at)).total_seconds()))
    except (TypeError, ValueError):
        doc_age = 0
    return max(doc_age, int(entry.sticky_age_sec or 0) + doc_age)


def _state_ready(state: dict[str, Any] | None) -> bool:
    if not state:
        return False
    if state.get("consumer_safe") is False:
        return False
    return bool(state.get("fast_ready") or state.get("full_ready"))


def _sticky_cycle_count(entry: MicroTargetEntry, state: dict[str, Any] | None) -> int:
    cycle = int(entry.sticky_cycle_count or 1)
    if state and state.get("seen_cycle_count") is not None:
        try:
            cycle = max(cycle, int(state.get("seen_cycle_count")))
        except (TypeError, ValueError):
            pass
    return max(1, cycle + 1)


def _daemon_state_to_entry(
    state: dict[str, Any],
    *,
    cfg: EngineConfig,
) -> MicroTargetEntry | None:
    sym = str(state.get("symbol") or "").upper().strip()
    if not sym:
        return None
    source_state = str(state.get("source_state") or "watch_candidate")
    is_strong = source_state == "strong_candidate"
    base = sym[:-4] if sym.endswith("USDT") else sym
    try:
        priority = int(state.get("priority") or 0)
    except (TypeError, ValueError):
        priority = 0
    return MicroTargetEntry(
        symbol=sym,
        base_asset=base,
        source_state=source_state,
        priority=priority,
        scan_score=priority,
        market_entry_suitability_score=0,
        market_entry_suitability="unknown",
        trade_candidate_rank_score=priority,
        trade_candidate_bucket="unknown",
        promoted_from_raw=False,
        move_side=str(state.get("move_side") or "unknown"),
        trigger_type="sticky_daemon_state",
        subscribe=list(cfg.mr_tier2_subscribe if is_strong else cfg.mr_tier1_subscribe),
        target_ready_tf=cfg.mr_target_ready_tf,
        min_collect_seconds=cfg.mr_min_collect_seconds,
        ttl_seconds=cfg.mr_ttl_seconds_tier2 if is_strong else cfg.mr_ttl_seconds_tier1,
    )


def _apply_sticky_pool(
    *,
    tier1_entries: list[MicroTargetEntry],
    tier2_entries: list[MicroTargetEntry],
    previous_doc: MicroTargetsDocument | None,
    daemon_symbols: dict[str, dict[str, Any]],
    cfg: EngineConfig,
    now: datetime,
    total_cap: int,
) -> tuple[list[MicroTargetEntry], list[MicroTargetEntry], dict[str, object], bool]:
    enabled = bool(cfg.strategy_pipeline_micro_sticky_pool_enabled)
    ttl_sec = max(0, int(cfg.strategy_pipeline_micro_sticky_ttl_sec))
    min_cycles = max(1, int(cfg.strategy_pipeline_micro_sticky_min_cycles))
    max_cycles = max(min_cycles, int(cfg.strategy_pipeline_micro_sticky_max_cycles))
    reason_codes: list[str] = []

    current_entries = [*tier2_entries, *tier1_entries]
    previous_entries = [*(previous_doc.tier2_active_strong if previous_doc else []), *(previous_doc.tier1_warm_watch if previous_doc else [])]
    previous_by_symbol = {e.symbol.upper().strip(): e for e in previous_entries}

    records: list[dict[str, Any]] = []
    current_symbols: set[str] = set()
    for tier, entries in ((2, tier2_entries), (1, tier1_entries)):
        for entry in entries:
            sym = entry.symbol.upper().strip()
            current_symbols.add(sym)
            prev = previous_by_symbol.get(sym)
            state = daemon_symbols.get(sym)
            cycle = max(1, int(prev.sticky_cycle_count or 1) + 1) if prev else 1
            if state and state.get("seen_cycle_count") is not None:
                try:
                    cycle = max(cycle, int(state.get("seen_cycle_count")))
                except (TypeError, ValueError):
                    pass
            records.append(
                {
                    "entry": entry.model_copy(
                        update={
                            "sticky_source": "current",
                            "sticky_age_sec": 0,
                            "sticky_cycle_count": min(cycle, max_cycles),
                            "retained_reason": "current_candidate",
                            "sticky_plan_candidate": True,
                        }
                    ),
                    "tier": tier,
                    "current": True,
                    "ready": _state_ready(state),
                    "age": 0,
                }
            )

    sticky_candidates: dict[str, dict[str, Any]] = {}
    if enabled and previous_doc:
        for entry in previous_entries:
            sym = entry.symbol.upper().strip()
            if not sym or sym in current_symbols:
                continue
            state = daemon_symbols.get(sym)
            age_sec = _entry_sticky_age_sec(entry, previous_doc=previous_doc, now=now)
            ready = _state_ready(state)
            cycle = _sticky_cycle_count(entry, state)
            if age_sec > ttl_sec:
                reason_codes.append("sticky_expired")
                continue
            if cycle > max_cycles and not (ready and cfg.strategy_pipeline_micro_sticky_include_ready_symbols):
                reason_codes.append("sticky_max_cycles_reached")
                continue
            retained_reason = "ready_cache" if ready else "sticky_warmup"
            sticky_candidates[sym] = {
                "entry": entry.model_copy(
                    update={
                        "sticky_source": "previous_target",
                        "sticky_age_sec": age_sec,
                        "sticky_cycle_count": min(cycle, max_cycles),
                        "retained_reason": retained_reason,
                        "sticky_plan_candidate": True,
                    }
                ),
                "tier": 2 if entry.source_state == "strong_candidate" else 1,
                "current": False,
                "ready": ready,
                "age": age_sec,
            }

    if enabled:
        for sym, state in daemon_symbols.items():
            if sym in current_symbols or sym in sticky_candidates:
                continue
            if state.get("consumer_safe") is False:
                reason_codes.append("daemon_symbol_consumer_unsafe")
                continue
            entry = _daemon_state_to_entry(state, cfg=cfg)
            if entry is None:
                continue
            ready = _state_ready(state)
            try:
                age_sec = max(0, int(state.get("continuous_collect_sec") or 0))
            except (TypeError, ValueError):
                age_sec = 0
            if age_sec > ttl_sec and not (ready and cfg.strategy_pipeline_micro_sticky_include_ready_symbols):
                reason_codes.append("daemon_symbol_expired")
                continue
            try:
                cycle = max(1, int(state.get("seen_cycle_count") or 1))
            except (TypeError, ValueError):
                cycle = 1
            sticky_candidates[sym] = {
                "entry": entry.model_copy(
                    update={
                        "sticky_source": "daemon_state",
                        "sticky_age_sec": min(age_sec, ttl_sec),
                        "sticky_cycle_count": min(cycle, max_cycles),
                        "retained_reason": "ready_cache" if ready else "sticky_warmup",
                        "sticky_plan_candidate": True,
                    }
                ),
                "tier": 2 if entry.source_state == "strong_candidate" else 1,
                "current": False,
                "ready": ready,
                "age": min(age_sec, ttl_sec),
            }

    records.extend(sticky_candidates.values())

    def _record_key(row: dict[str, Any]) -> tuple[int, int, int, int, str]:
        entry: MicroTargetEntry = row["entry"]
        if row["current"] and row["tier"] == 2:
            group = 0
        elif row["current"]:
            group = 1
        elif row["ready"]:
            group = 2
        else:
            group = 3
        return (
            group,
            -int(entry.priority),
            -int(entry.trade_candidate_rank_score),
            int(row["age"]),
            entry.symbol.upper(),
        )

    selected = sorted(records, key=_record_key)[: max(0, total_cap)]
    selected_symbols = {row["entry"].symbol.upper().strip() for row in selected}
    evicted_count = max(0, len(records) - len(selected))
    sticky_truncated = evicted_count > 0 and len(sticky_candidates) > 0
    if sticky_truncated:
        reason_codes.append("sticky_pool_truncated")

    new_tier2 = [row["entry"] for row in selected if row["tier"] == 2]
    new_tier1 = [row["entry"] for row in selected if row["tier"] == 1]
    retained_count = sum(1 for row in selected if not row["current"])
    sticky_pool = {
        "enabled": enabled,
        "sticky_ttl_sec": ttl_sec,
        "sticky_min_cycles": min_cycles,
        "sticky_max_cycles": max_cycles,
        "sticky_include_ready_symbols": bool(cfg.strategy_pipeline_micro_sticky_include_ready_symbols),
        "current_candidate_count": len(current_entries),
        "retained_count": retained_count,
        "evicted_count": evicted_count,
        "final_target_count": len(selected_symbols),
        "reason_codes": list(dict.fromkeys(reason_codes)),
    }
    return new_tier1, new_tier2, sticky_pool, sticky_truncated


def run_micro_target_router(
    *,
    project_root: Path | None = None,
    raw_path: Path | None = None,
    watch_path: Path | None = None,
    strong_path: Path | None = None,
    output_path: Path | None = None,
    stdout_json: bool = False,
) -> int:
    cfg = EngineConfig.load(project_root)
    now: datetime = utc_now()
    gen_at = to_iso_z(now)
    out_p = output_path or cfg.micro_targets_path
    pr = cfg.project_root.resolve()

    rp = raw_path or cfg.latest_raw_candidates_path
    wp = watch_path or cfg.latest_watch_signals_path
    sp = strong_path or cfg.latest_strong_candidates_path
    previous_doc = _load_previous_targets(out_p)
    daemon_symbols = _load_daemon_symbols(cfg.micro_daemon_cli_state_path)

    try:
        raw_doc = AbnormalTierDocument.model_validate(read_json_object(rp))
        watch_doc = AbnormalTierDocument.model_validate(read_json_object(wp))
        strong_doc = AbnormalTierDocument.model_validate(read_json_object(sp))
    except (OSError, TypeError, ValueError) as exc:
        log.error("micro router input load failed: %s", exc)
        err_doc = MicroTargetsDocument(
            schema_version=cfg.schema_version,
            generated_at=gen_at,
            source="micro_target_router",
            status="error",
            warm_watch_limit=cfg.mr_warm_watch_limit,
            active_strong_limit=cfg.mr_active_strong_limit,
            max_active_micro_symbols=cfg.mr_max_active_micro_symbols,
            priority_mode=cfg.mr_priority_mode,
            promoted_raw_count=0,
            excluded_trade_avoid_count=0,
            candidate_quality_counts={},
            input_watch_status="",
            input_strong_status="",
            input_snapshot_generated_at="",
            input_snapshot_age_sec=-1,
            step2_reported_input_snapshot_age_sec=-1,
            router_computed_input_snapshot_age_sec=-1,
            router_freshness_ok=False,
            input_counts=InputCountsBlock(raw=0, watch=0, strong=0),
            routed_counts=RoutedCountsBlock(tier1=0, tier2=0),
            truncated=TruncatedBlock(tier1=False, tier2=False),
            skip_reasons=[],
            tier1_warm_watch=[],
            tier2_active_strong=[],
        )
        try:
            _write_doc(out_p, err_doc)
        except OSError as wexc:
            log.error("micro router write failed: %s", wexc)
        return EXIT_CONFIG

    router_age_raw = snapshot_age_sec(raw_doc.input_snapshot_generated_at, now=now)
    router_age_watch = snapshot_age_sec(watch_doc.input_snapshot_generated_at, now=now)
    router_age_strong = snapshot_age_sec(strong_doc.input_snapshot_generated_at, now=now)

    skip: list[str] = []
    max_age = cfg.step2_signal_max_age_sec
    rout_w = _input_routable(watch_doc, max_age, router_age_sec=router_age_watch)
    rout_s = _input_routable(strong_doc, max_age, router_age_sec=router_age_strong)
    rout_raw = _input_routable(raw_doc, max_age, router_age_sec=router_age_raw)

    if not rout_w:
        skip.append(SKIP_WATCH_STALE)
    if not rout_s:
        skip.append(SKIP_STRONG_STALE)
    if cfg.mr_include_raw_in_warm_pool and not rout_raw:
        skip.append(SKIP_RAW_STALE)

    if watch_doc.input_snapshot_generated_at != strong_doc.input_snapshot_generated_at:
        skip.append(SKIP_SNAPSHOT_MISMATCH)

    try:
        merged_gen_s, merged_router_age, step2_rep_max = _merged_snapshot_meta(
            watch_doc,
            strong_doc,
            router_age_watch=router_age_watch,
            router_age_strong=router_age_strong,
        )
    except (TypeError, ValueError):
        merged_gen_s = watch_doc.input_snapshot_generated_at
        merged_router_age = max(router_age_watch, router_age_strong)
        step2_rep_max = max(watch_doc.input_snapshot_age_sec, strong_doc.input_snapshot_age_sec)

    manual_bases = load_manual_bases(cfg.manual_watchlist_path)
    bonus = cfg.mr_manual_watchlist_priority_bonus
    priority_mode = cfg.mr_priority_mode if cfg.mr_allow_trade_rank_priority else "scan_score"
    exclude_trade_avoid_count = 0

    tier2_entries: list[MicroTargetEntry] = []
    tier1_entries: list[MicroTargetEntry] = []
    raw_fill_summary: dict[str, object] = {
        "enabled": bool(cfg.mr_include_raw_in_warm_pool),
        "raw_fill_count": 0,
        "raw_fill_symbols": [],
        "reason": "",
        "warm_watch_limit": int(cfg.mr_warm_watch_limit),
        "remaining_capacity_before_raw": 0,
    }
    trunc_t1 = False
    trunc_t2 = False
    total_cap = max(0, int(cfg.mr_max_active_micro_symbols))

    if rout_s and strong_doc.signals:
        strong_pool, excluded = _filter_for_micro(
            list(strong_doc.signals),
            exclude_market_entry_avoid=cfg.mr_exclude_market_entry_avoid_from_micro,
        )
        exclude_trade_avoid_count += excluded
        if excluded:
            skip.append(SKIP_TRADE_AVOID)
        pool = sorted(
            strong_pool,
            key=lambda s: _sort_key_signal(s, manual_bases=manual_bases, bonus=bonus, priority_mode=priority_mode),
        )
        cap = min(max(0, cfg.mr_active_strong_limit), total_cap)
        picked = pool[:cap]
        tier2_entries = [
            _signal_to_entry(
                x,
                cfg=cfg,
                source_state="strong_candidate",
                subscribe=cfg.mr_tier2_subscribe,
                manual_bases=manual_bases,
            )
            for x in picked
        ]
        if len(pool) > cap:
            trunc_t2 = True
            skip.append(SKIP_TIER2_TRUNC)

    if rout_w:
        watch_pool, excluded = _filter_for_micro(
            list(watch_doc.signals),
            exclude_market_entry_avoid=cfg.mr_exclude_market_entry_avoid_from_micro,
        )
        exclude_trade_avoid_count += excluded
        if excluded:
            skip.append(SKIP_TRADE_AVOID)
        pool_w = sorted(
            watch_pool,
            key=lambda s: _sort_key_signal(s, manual_bases=manual_bases, bonus=bonus, priority_mode=priority_mode),
        )
        remaining_total = max(0, total_cap - len(tier2_entries))
        cap1 = min(max(0, cfg.mr_warm_watch_limit), remaining_total)
        tier1_watch = pool_w[:cap1]
        tier1_entries.extend(
            [
                _signal_to_entry(
                    x,
                    cfg=cfg,
                    source_state="watch_candidate",
                    subscribe=cfg.mr_tier1_subscribe,
                    manual_bases=manual_bases,
                )
                for x in tier1_watch
            ]
        )
        trunc1_watch = len(pool_w) > cap1
        remaining = cap1 - len(tier1_watch)
        raw_fill_summary["remaining_capacity_before_raw"] = remaining

        trunc1_raw = False
        raw_fill_allowed = cfg.mr_include_raw_in_warm_pool and not tier2_entries
        if raw_fill_allowed and rout_raw and remaining > 0 and raw_doc.signals:
            raw_pool, excluded = _filter_for_micro(
                list(raw_doc.signals),
                exclude_market_entry_avoid=cfg.mr_exclude_market_entry_avoid_from_micro,
            )
            exclude_trade_avoid_count += excluded
            if excluded:
                skip.append(SKIP_TRADE_AVOID)
            pool_r = sorted(
                raw_pool,
                key=lambda s: _sort_key_signal(s, manual_bases=manual_bases, bonus=bonus, priority_mode=priority_mode),
            )
            raw_limit = remaining
            raw_pick = pool_r[:raw_limit]
            tier1_entries.extend(
                [
                    _signal_to_entry(
                        x,
                        cfg=cfg,
                        source_state="raw_candidate",
                        subscribe=cfg.mr_tier1_subscribe,
                        manual_bases=manual_bases,
                    )
                    for x in raw_pick
                ]
            )
            raw_fill_summary.update(
                {
                    "raw_fill_count": len(raw_pick),
                    "raw_fill_symbols": [s.symbol.upper() for s in raw_pick],
                    "reason": "warm_pool_remaining_capacity" if raw_pick else "no_raw_candidate_selected",
                }
            )
            trunc1_raw = len(pool_r) > remaining
        elif cfg.mr_include_raw_in_warm_pool and tier2_entries:
            raw_fill_summary["reason"] = "active_strong_present"
        elif cfg.mr_include_raw_in_warm_pool and remaining > 0:
            raw_fill_summary["reason"] = "raw_input_not_routable" if not rout_raw else "no_raw_candidates"
        elif cfg.mr_include_raw_in_warm_pool:
            raw_fill_summary["reason"] = "no_remaining_capacity"
        else:
            raw_fill_summary["reason"] = "raw_fill_disabled"

        if trunc1_watch or trunc1_raw:
            trunc_t1 = True
            skip.append(SKIP_TIER1_TRUNC)

    tier1_entries, tier2_entries, sticky_pool, sticky_truncated = _apply_sticky_pool(
        tier1_entries=tier1_entries,
        tier2_entries=tier2_entries,
        previous_doc=previous_doc,
        daemon_symbols=daemon_symbols,
        cfg=cfg,
        now=now,
        total_cap=total_cap,
    )
    if sticky_truncated:
        skip.append(SKIP_STICKY_TRUNC)

    routed_sum = len(tier1_entries) + len(tier2_entries)
    if rout_w and rout_s:
        top_status = "ok" if routed_sum > 0 else "no_targets"
    elif not rout_w and not rout_s:
        top_status = "stale_input"
    else:
        top_status = "partial_input_stale"

    router_ok = rout_w and rout_s

    plan_candidate_symbols = list(
        dict.fromkeys([e.symbol.upper().strip() for e in [*tier1_entries, *tier2_entries]])
    )
    target_source_distribution: dict[str, int] = {}
    for entry in [*tier1_entries, *tier2_entries]:
        source_key = str(entry.retained_reason or entry.source_state or "unknown")
        if entry.source_state == "raw_candidate":
            source_key = "raw_fill"
        elif entry.sticky_source != "current" and source_key == "current_candidate":
            source_key = str(entry.sticky_source or "sticky_retained")
        target_source_distribution[source_key] = target_source_distribution.get(source_key, 0) + 1
    candidate_hash = hashlib.sha1("\n".join(sorted(plan_candidate_symbols)).encode("utf-8")).hexdigest()[:16]
    target_set_id = f"{gen_at.replace('-', '').replace(':', '').replace('Z', 'Z')}:{candidate_hash}"
    block_downstream = top_status == "stale_input"
    block_reason = "step2_stale" if block_downstream else ""
    step2_current = build_step2_current_freshness(project_root=pr)

    doc = MicroTargetsDocument(
        schema_version=cfg.schema_version,
        generated_at=gen_at,
        source="micro_target_router",
        status=top_status,
        warm_watch_limit=cfg.mr_warm_watch_limit,
        active_strong_limit=cfg.mr_active_strong_limit,
        max_active_micro_symbols=cfg.mr_max_active_micro_symbols,
        priority_mode=priority_mode,
        promoted_raw_count=sum(1 for e in tier1_entries if e.source_state == "raw_candidate" and e.promoted_from_raw),
        excluded_trade_avoid_count=exclude_trade_avoid_count,
        candidate_quality_counts=_candidate_quality_counts(
            list(raw_doc.signals) + list(watch_doc.signals) + list(strong_doc.signals)
        ),
        input_watch_status=watch_doc.status,
        input_strong_status=strong_doc.status,
        input_snapshot_generated_at=merged_gen_s,
        input_snapshot_age_sec=merged_router_age,
        step2_reported_input_snapshot_age_sec=step2_rep_max,
        router_computed_input_snapshot_age_sec=merged_router_age,
        router_freshness_ok=router_ok,
        input_counts=InputCountsBlock(
            raw=len(raw_doc.signals),
            watch=len(watch_doc.signals),
            strong=len(strong_doc.signals),
        ),
        routed_counts=RoutedCountsBlock(tier1=len(tier1_entries), tier2=len(tier2_entries)),
        truncated=TruncatedBlock(tier1=trunc_t1, tier2=trunc_t2),
        skip_reasons=list(dict.fromkeys(skip)),
        block_downstream=block_downstream,
        block_reason=block_reason,
        step2_current_freshness=step2_current,
        target_set_id=target_set_id,
        candidate_hash=candidate_hash,
        target_symbols=plan_candidate_symbols,
        target_count=len(plan_candidate_symbols),
        plan_candidate_symbols=plan_candidate_symbols,
        plan_candidate_count=len(plan_candidate_symbols),
        candidate_alignment=CandidateAlignmentBlock(
            generated_at=gen_at,
            include_tier1=True,
            include_tier2=True,
            include_ready_cache=bool(sticky_pool.get("retained_count", 0)),
            ready_cache_max_age_sec=int(sticky_pool.get("sticky_ttl_sec", 0)),
        ),
        sticky_pool=sticky_pool,
        raw_fill=raw_fill_summary,
        target_source_distribution=target_source_distribution,
        tier1_warm_watch=tier1_entries,
        tier2_active_strong=tier2_entries,
    )

    try:
        _write_doc(out_p, doc)
    except OSError as exc:
        log.error("micro router write failed: %s", exc)
        return EXIT_CONFIG
    try:
        ingest_target_source_ledger(
            doc.model_dump(mode="json"),
            db_path=pr / "DATA" / "audit" / "run_audit.db",
        )
    except Exception as exc:
        log.warning("micro target source ledger ingest failed: %s", exc)

    log.info(
        "micro router status=%s tier1=%s tier2=%s router_age_sec=%s out=%s",
        top_status,
        len(tier1_entries),
        len(tier2_entries),
        merged_router_age,
        out_p,
    )

    if stdout_json:
        import sys

        summary = {
            "schema_version": cfg.schema_version,
            "source": "micro_target_router",
            "status": top_status,
            "max_active_micro_symbols": cfg.mr_max_active_micro_symbols,
            "priority_mode": priority_mode,
            "promoted_raw_count": doc.promoted_raw_count,
            "excluded_trade_avoid_count": doc.excluded_trade_avoid_count,
            "candidate_quality_counts": doc.candidate_quality_counts,
            "router_freshness_ok": router_ok,
            "input_snapshot_generated_at": merged_gen_s,
            "input_snapshot_age_sec": merged_router_age,
            "step2_reported_input_snapshot_age_sec": step2_rep_max,
            "router_computed_input_snapshot_age_sec": merged_router_age,
            "block_downstream": doc.block_downstream,
            "block_reason": doc.block_reason,
            "step2_current_freshness": doc.step2_current_freshness,
            "routed_counts": {"tier1": len(tier1_entries), "tier2": len(tier2_entries)},
            "plan_candidate_count": doc.plan_candidate_count,
            "input_counts": {
                "raw": len(raw_doc.signals),
                "watch": len(watch_doc.signals),
                "strong": len(strong_doc.signals),
            },
            "truncated": {"tier1": trunc_t1, "tier2": trunc_t2},
            "skip_reasons": doc.skip_reasons,
            "sticky_pool": doc.sticky_pool,
            "raw_fill": doc.raw_fill,
            "target_source_distribution": doc.target_source_distribution,
            "output_file": _rel_project_path(pr, out_p),
        }
        sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))
        sys.stdout.buffer.flush()
    return EXIT_SUCCESS


def run_micro_target_router_safe(**kwargs: Any) -> int:
    try:
        return run_micro_target_router(**kwargs)
    except Exception as exc:
        log.exception("micro target router failed: %s", exc)
        return EXIT_INTERNAL
