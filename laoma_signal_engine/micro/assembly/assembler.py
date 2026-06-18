"""STEP3.7 feature assembly. docs/STEP3.7_任务卡.md.

Engine dict (get_latest_cvd / get_latest_ofi) -> micro_15m field mapping (MVP):
- cvd: float from key "cvd" if present, else null when dict missing
- z_cvd: float or null from key "z_cvd"
- cvd_state: string from key "cvd_state" if non-empty str, else "unknown"
- ofi: float from key "ofi" if present
- z_ofi: float or null from key "z_ofi"
- ofi_state / ofi_pressure: same rule as cvd_state

micro_15m.ready must equal micro_quality.ready (also enforced in Pydantic root).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping, Protocol, Sequence, runtime_checkable

from laoma_signal_engine.micro.adapters.binance_common import normalize_binance_symbol
from laoma_signal_engine.micro.assembly.models import (
    CoverageSummaryBlock,
    DroppedEventsBlock,
    LatestMicroFeaturesDocument,
    LatestMicroStatus,
    Micro15mBlock,
    MicroFeatureItem,
    MicroQualityBlock,
    MicroSignalBlock,
    TargetStatus,
)
from laoma_signal_engine.micro.quality.models import MicroQualitySnapshot


@runtime_checkable
class CvdOfiLatestProvider(Protocol):
    def get_latest_cvd(self, symbol: str) -> dict[str, Any] | None: ...

    def get_latest_ofi(self, symbol: str) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class AssemblyTargetRow:
    symbol: str
    ofi_levels: Literal[1, 5]
    tier: str | None = None
    symbol_safe_id: str | None = None
    source_state: str | None = None
    move_side: str | None = None
    priority: int | None = None
    scan_score: int | None = None
    trigger_type: str | None = None


def _blank_to_none_str(v: str | None) -> str | None:
    if v is None:
        return None
    if not str(v).strip():
        return None
    return v


def snapshot_to_micro_quality(snapshot: MicroQualitySnapshot) -> MicroQualityBlock:
    coverage = {
        k: CoverageSummaryBlock(
            stream_type=cs.stream_type,
            window_sec=cs.window_sec,
            expected_seconds=cs.expected_seconds,
            covered_seconds=cs.covered_seconds,
            coverage_ratio=cs.coverage_ratio,
        )
        for k, cs in snapshot.coverage.items()
    }
    return MicroQualityBlock(
        ready=snapshot.ready,
        reason_codes=list(snapshot.reason_codes),
        reference_ts_sec=snapshot.reference_ts_sec,
        collect_started_ts_sec=snapshot.collect_started_ts_sec,
        warmup_age_sec=snapshot.warmup_age_sec,
        cvd_update_age_sec=snapshot.cvd_update_age_sec,
        ofi_update_age_sec=snapshot.ofi_update_age_sec,
        last_update_age_sec=snapshot.last_update_age_sec,
        max_lag_sec=snapshot.max_lag_sec,
        last_cvd_update_bucket_ts_sec=snapshot.last_cvd_update_bucket_ts_sec,
        last_ofi_update_bucket_ts_sec=snapshot.last_ofi_update_bucket_ts_sec,
        last_processed_bucket_ts_sec=snapshot.last_processed_bucket_ts_sec,
        ofi_cvd_lag_side=snapshot.ofi_cvd_lag_side,
        reference_bucket_ts_sec=snapshot.reference_bucket_ts_sec,
        cvd_age_bucket_sec=snapshot.cvd_age_bucket_sec,
        ofi_age_bucket_sec=snapshot.ofi_age_bucket_sec,
        ofi_cvd_lag_bucket_sec=snapshot.ofi_cvd_lag_bucket_sec,
        data_quality_root_cause_class=snapshot.data_quality_root_cause_class,
        reason_root_causes=dict(snapshot.reason_root_causes or {}),
        coverage=coverage,
        driver_metrics_summary=dict(snapshot.driver_metrics_summary),
    )


def _pick_float(d: dict[str, Any] | None, key: str) -> float | None:
    if d is None or key not in d:
        return None
    raw = d[key]
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _pick_engine_state_str(d: dict[str, Any] | None, key: str) -> str:
    if d is None:
        return "unknown"
    raw = d.get(key)
    if raw is None:
        return "unknown"
    if not isinstance(raw, str):
        return "unknown"
    if raw == "":
        return "unknown"
    return raw


def build_micro_15m_block(
    driver: CvdOfiLatestProvider,
    symbol: str,
    quality_ready: bool,
) -> Micro15mBlock:
    sym = normalize_binance_symbol(symbol)
    cvd_d = driver.get_latest_cvd(sym)
    ofi_d = driver.get_latest_ofi(sym)
    return Micro15mBlock(
        ready=quality_ready,
        cvd=_pick_float(cvd_d, "cvd"),
        z_cvd=_pick_float(cvd_d, "z_cvd"),
        cvd_state=_pick_engine_state_str(cvd_d, "cvd_state"),
        ofi=_pick_float(ofi_d, "ofi"),
        z_ofi=_pick_float(ofi_d, "z_ofi"),
        ofi_state=_pick_engine_state_str(ofi_d, "ofi_state"),
        ofi_pressure=_pick_engine_state_str(ofi_d, "ofi_pressure"),
        fusion_score=None,
        fusion_consistency=None,
        fusion_signal=None,
        fusion_ready=False,
        micro_state="not_ready",
    )


def _z_sign(v: float | None) -> int | None:
    if v is None:
        return None
    if v > 0:
        return 1
    if v < 0:
        return -1
    return 0


def build_micro_signal_block(
    *,
    mode: Literal["fast", "full"],
    move_side: str | None,
    micro: Micro15mBlock,
    quality: MicroQualityBlock,
) -> MicroSignalBlock:
    reasons: list[str] = []
    data_ready = bool(quality.ready)
    if not data_ready:
        reasons.extend(quality.reason_codes or [f"{mode}_quality_not_ready"])

    z_cvd = micro.z_cvd
    z_ofi = micro.z_ofi
    cvd_sign = _z_sign(z_cvd)
    ofi_sign = _z_sign(z_ofi)
    z_count = int(z_cvd is not None) + int(z_ofi is not None)

    if mode == "full":
        stat_ready = z_count == 2
        if not stat_ready:
            reasons.append("full_z_missing")
    else:
        stat_ready = z_count >= 1
        if z_count == 0:
            reasons.append("fast_z_missing")
        elif z_count == 1:
            reasons.append("fast_one_z_available_weak_only")

    usable = data_ready and stat_ready
    if not usable:
        state = "data_quality_blocked" if not data_ready else "insufficient"
        return MicroSignalBlock(
            micro_data_ready=data_ready,
            micro_stat_ready=stat_ready,
            micro_signal_usable=False,
            micro_direction_confirmed=False,
            micro_exec_allowed=False,
            micro_alignment_state=state,
            micro_strength="none",
            micro_confirmation_level="none",
            micro_exec_allowed_reason="blocked_data_quality" if not data_ready else "blocked_stat_missing",
            micro_confidence_score=0,
            micro_confirmation_penalty_bps=0.0,
            price_response_ok=None,
            persistence_ok=False if mode == "full" else None,
            reason_codes=list(dict.fromkeys(reasons)),
        )

    side = (move_side or "").strip().lower()
    expected = 1 if side == "up" else (-1 if side == "down" else None)
    if expected is None:
        reasons.append("micro_no_move_side")
        return MicroSignalBlock(
            micro_data_ready=data_ready,
            micro_stat_ready=stat_ready,
            micro_signal_usable=True,
            micro_direction_confirmed=False,
            micro_exec_allowed=False,
            micro_alignment_state="insufficient",
            micro_strength="none",
            micro_confirmation_level="none",
            micro_exec_allowed_reason="blocked_no_move_side",
            micro_confidence_score=0,
            micro_confirmation_penalty_bps=0.0,
            price_response_ok=None,
            persistence_ok=False if mode == "full" else None,
            reason_codes=list(dict.fromkeys(reasons)),
        )

    signs = [s for s in (cvd_sign, ofi_sign) if s is not None and s != 0]
    same_side = sum(1 for s in signs if s == expected)
    opposite = sum(1 for s in signs if s == -expected)

    price_response_ok = None
    persistence_ok = None
    confirmed = False
    exec_allowed = False
    strength: Literal["none", "weak", "medium", "strong"] = "none"
    confirmation_level: Literal["none", "hint", "weak", "strong", "conflict"] = "none"
    exec_reason = ""
    confidence_score = 0
    confirmation_penalty_bps = 0.0

    if opposite and same_side:
        state = "mixed"
        confirmation_level = "hint"
        exec_reason = "blocked_mixed_cvd_ofi"
        confidence_score = 20
        reasons.append("micro_mixed_cvd_ofi")
    elif opposite and not same_side:
        state = "conflict"
        confirmation_level = "conflict"
        exec_reason = "blocked_conflict"
        confidence_score = 0
        reasons.append("micro_direction_conflict")
    elif same_side == 2:
        strong_abs = abs(float(z_cvd or 0.0)) >= 1.0 and abs(float(z_ofi or 0.0)) >= 1.0
        state = "aligned_strong" if strong_abs else "aligned_weak"
        strength = "strong" if strong_abs else "medium"
        if mode == "full":
            persistence_ok = True
            reasons.append("full_persistence_proxy")
            confirmation_level = "strong" if strong_abs else "weak"
            exec_reason = "full_strong_alignment" if strong_abs else "full_weak_alignment_with_persistence"
            confidence_score = 90 if strong_abs else 70
            confirmation_penalty_bps = 0.0 if strong_abs else 5.0
        else:
            price_response_ok = True if strong_abs else None
            if strong_abs:
                reasons.append("fast_price_response_proxy")
                confirmation_level = "strong"
                exec_reason = "fast_strong_alignment"
                confidence_score = 90
            else:
                reasons.append("fast_weak_alignment_confirmed")
                confirmation_level = "weak"
                exec_reason = "fast_weak_alignment"
                confidence_score = 65
                confirmation_penalty_bps = 5.0
    elif same_side == 1:
        state = "aligned_weak"
        strength = "weak"
        reasons.append(f"{mode}_one_z_available_weak_only")
        confirmation_level = "hint"
        exec_reason = "blocked_one_z_hint_only"
        confidence_score = 45
        confirmation_penalty_bps = 10.0
    else:
        state = "insufficient"
        exec_reason = "blocked_neutral_or_missing"
        confidence_score = 0
        reasons.append("micro_z_neutral_or_missing")

    if mode == "fast":
        confirmed = confirmation_level in ("weak", "strong")
    else:
        confirmed = state in ("aligned_weak", "aligned_strong") and persistence_ok is True
    exec_allowed = confirmed and usable
    if exec_allowed and not exec_reason:
        exec_reason = f"{mode}_{confirmation_level}_alignment"

    return MicroSignalBlock(
        micro_data_ready=data_ready,
        micro_stat_ready=stat_ready,
        micro_signal_usable=usable,
        micro_direction_confirmed=confirmed,
        micro_exec_allowed=exec_allowed,
        micro_alignment_state=state,
        micro_strength=strength,
        micro_confirmation_level=confirmation_level,
        micro_exec_allowed_reason=exec_reason,
        micro_confidence_score=confidence_score,
        micro_confirmation_penalty_bps=confirmation_penalty_bps,
        price_response_ok=price_response_ok,
        persistence_ok=persistence_ok,
        reason_codes=list(dict.fromkeys(reasons)),
    )


def build_document(
    *,
    targets: Sequence[AssemblyTargetRow],
    quality_by_symbol: Mapping[str, MicroQualitySnapshot],
    fast_quality_by_symbol: Mapping[str, MicroQualitySnapshot] | None = None,
    driver: CvdOfiLatestProvider,
    generated_at: str,
    status: LatestMicroStatus,
    target_generated_at: str,
    target_age_sec: int,
    target_status: TargetStatus,
    dropped_events_trade: int,
    dropped_events_book: int,
    dropped_events_depth: int,
    ws_status: str = "unknown",
    last_ws_message_age_sec: int | None = None,
    reason_codes: list[str] | None = None,
) -> LatestMicroFeaturesDocument:
    items: list[MicroFeatureItem] = []
    for row in targets:
        sym = normalize_binance_symbol(row.symbol)
        snap = quality_by_symbol.get(sym)
        if snap is None:
            msg = f"missing MicroQualitySnapshot for symbol={sym!r}"
            raise ValueError(msg)
        mq = snapshot_to_micro_quality(snap)
        m15 = build_micro_15m_block(driver, sym, mq.ready)
        fast_mq: MicroQualityBlock | None = None
        fast_m15: Micro15mBlock | None = None
        if fast_quality_by_symbol is not None:
            fast_snap = fast_quality_by_symbol.get(sym)
            if fast_snap is None:
                msg = f"missing fast MicroQualitySnapshot for symbol={sym!r}"
                raise ValueError(msg)
            fast_mq = snapshot_to_micro_quality(fast_snap)
            fast_m15 = build_micro_15m_block(driver, sym, fast_mq.ready)
        fast_signal = (
            build_micro_signal_block(mode="fast", move_side=row.move_side, micro=fast_m15, quality=fast_mq)
            if fast_m15 is not None and fast_mq is not None
            else None
        )
        full_signal = build_micro_signal_block(mode="full", move_side=row.move_side, micro=m15, quality=mq)
        item = MicroFeatureItem(
            symbol=sym,
            symbol_safe_id=_blank_to_none_str(row.symbol_safe_id),
            tier=_blank_to_none_str(row.tier),
            source_state=_blank_to_none_str(row.source_state),
            move_side=_blank_to_none_str(row.move_side),
            priority=row.priority,
            scan_score=row.scan_score,
            trigger_type=_blank_to_none_str(row.trigger_type),
            ofi_levels=row.ofi_levels,
            micro_15m=m15,
            micro_quality=mq,
            micro_fast_15m=fast_m15,
            micro_fast_quality=fast_mq,
            micro_fast_signal=fast_signal,
            micro_full_15m=m15,
            micro_full_quality=mq,
            micro_full_signal=full_signal,
        )
        items.append(item)

    n = len(items)
    rc = sum(1 for it in items if it.micro_quality.ready)
    fast_rc = sum(1 for it in items if it.micro_fast_quality and it.micro_fast_quality.ready)
    nrc = n - rc
    return LatestMicroFeaturesDocument(
        generated_at=generated_at,
        status=status,
        target_generated_at=target_generated_at,
        target_age_sec=target_age_sec,
        target_status=target_status,
        symbol_count=n,
        ready_count=rc,
        not_ready_count=nrc,
        fast_ready_count=fast_rc,
        full_ready_count=rc,
        ws_status=ws_status,
        last_ws_message_age_sec=last_ws_message_age_sec,
        dropped_events=DroppedEventsBlock(
            trade=dropped_events_trade,
            book=dropped_events_book,
            depth=dropped_events_depth,
        ),
        reason_codes=list(reason_codes or []),
        items=items,
    )
