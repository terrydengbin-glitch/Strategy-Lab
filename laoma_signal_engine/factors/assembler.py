"""Assemble latest_factor_snapshot.json (STEP3B). docs/STEP4.0_Decision_Layer_Phase1_任务卡.md."""

from __future__ import annotations

import logging
from typing import Any

from laoma_signal_engine.context.binance_context_client import (
    BinanceFuturesContextClient,
    fetch_premium_index_all,
    premium_index_by_symbol,
)
from laoma_signal_engine.context.basis_provider import build_basis_15m_from_premium_row
from laoma_signal_engine.context.funding_provider import build_funding_context_from_premium_row
from laoma_signal_engine.context.oi_provider import build_oi_15m_block
from datetime import datetime

from laoma_signal_engine.core.time_utils import age_sec_from_iso_z, to_iso_z, utc_now
from laoma_signal_engine.factors.models import (
    Basis15mBlock,
    FactorQualityBlock,
    FactorSnapshotDocument,
    FactorSnapshotItem,
    FactorSnapshotSource,
    FactorSnapshotStatus,
    FundingContextBlock,
    OI15mBlock,
)
from laoma_signal_engine.factors.reason_order import (
    factor_ready_from_reasons,
    sort_reason_codes,
)
from laoma_signal_engine.market.light_snapshot_models import FuturesLightSnapshotDocument
from laoma_signal_engine.micro.assembly.models import (
    LatestMicroFeaturesDocument,
    Micro15mBlock,
    MicroFeatureItem,
    MicroQualityBlock,
    MicroSignalBlock,
)
from laoma_signal_engine.micro.micro_target_models import MicroTargetEntry
from laoma_signal_engine.scanner.signal_models import AbnormalSignalEntry, AbnormalTierDocument

log = logging.getLogger(__name__)


def _synthetic_micro_quality_pipeline_skipped() -> MicroQualityBlock:
    return MicroQualityBlock(
        ready=False,
        reason_codes=["micro_pipeline_skipped"],
        reference_ts_sec=0,
        collect_started_ts_sec=0,
        warmup_age_sec=0,
        cvd_update_age_sec=None,
        ofi_update_age_sec=None,
        last_update_age_sec=None,
        max_lag_sec=None,
        coverage={},
        driver_metrics_summary={},
    )


def _synthetic_micro_quality_missing() -> MicroQualityBlock:
    return MicroQualityBlock(
        ready=False,
        reason_codes=[],
        reference_ts_sec=0,
        collect_started_ts_sec=0,
        warmup_age_sec=0,
        cvd_update_age_sec=None,
        ofi_update_age_sec=None,
        last_update_age_sec=None,
        max_lag_sec=None,
        coverage={},
        driver_metrics_summary={},
    )


def _synthetic_micro_15m_missing() -> Micro15mBlock:
    return Micro15mBlock(ready=False)


def _synthetic_micro_signal_block(*, reason_codes: list[str]) -> MicroSignalBlock:
    return MicroSignalBlock(
        micro_data_ready=False,
        micro_stat_ready=False,
        micro_signal_usable=False,
        micro_direction_confirmed=False,
        micro_exec_allowed=False,
        micro_alignment_state="data_quality_blocked" if reason_codes else "insufficient",
        micro_strength="none",
        micro_confirmation_level="none",
        micro_exec_allowed_reason="blocked_data_quality" if reason_codes else "",
        micro_confidence_score=0,
        micro_confirmation_penalty_bps=0.0,
        price_response_ok=None,
        persistence_ok=False,
        reason_codes=sort_reason_codes(reason_codes),
    )


def _default_context_triplet() -> tuple[OI15mBlock, FundingContextBlock, Basis15mBlock]:
    return (
        OI15mBlock(ready=False, reason="not_implemented"),
        FundingContextBlock(ready=False, reason="not_implemented"),
        Basis15mBlock(ready=False, reason="not_implemented"),
    )


MICRO_STALE_REASONS = frozenset(
    {
        "micro_features_stale",
        "micro_target_anchor_stale",
        "micro_generated_at_invalid",
        "micro_target_generated_at_invalid",
    },
)


def _micro_doc_input_reasons(
    micro: LatestMicroFeaturesDocument,
    *,
    now: datetime | None = None,
    micro_features_max_age_sec: int | None = None,
    micro_target_max_age_sec: int | None = None,
) -> list[str]:
    reasons: list[str] = []
    if micro.target_status not in ("fresh", "stale_observing"):
        reasons.append("micro_input_not_fresh")
    if micro.status not in ("ok", "observing_stale_targets"):
        reasons.append("micro_input_invalid")
    if micro_features_max_age_sec is not None:
        try:
            age = age_sec_from_iso_z(micro.generated_at, now=now)
        except (TypeError, ValueError):
            reasons.append("micro_generated_at_invalid")
        else:
            if age > micro_features_max_age_sec:
                reasons.append("micro_features_stale")
    if micro_target_max_age_sec is not None:
        try:
            target_age = age_sec_from_iso_z(micro.target_generated_at, now=now)
        except (TypeError, ValueError):
            reasons.append("micro_target_generated_at_invalid")
        else:
            if target_age > micro_target_max_age_sec:
                reasons.append("micro_target_anchor_stale")
    return reasons


def _micro_reasons_force_not_ready(reasons: list[str]) -> bool:
    return bool(MICRO_STALE_REASONS & set(reasons))


def _merge_signals(
    watch: AbnormalTierDocument,
    strong: AbnormalTierDocument,
) -> dict[str, AbnormalSignalEntry]:
    out: dict[str, AbnormalSignalEntry] = {}
    for sig in watch.signals:
        key = sig.symbol.upper().strip()
        out[key] = sig
    for sig in strong.signals:
        key = sig.symbol.upper().strip()
        out[key] = sig
    return out


def _signal_from_micro_target(entry: MicroTargetEntry, *, generated_at: str) -> AbnormalSignalEntry:
    return AbnormalSignalEntry.model_validate(
        {
            "symbol": entry.symbol,
            "base_asset": entry.base_asset,
            "futures_symbol": entry.symbol,
            "has_um_futures": True,
            "decision_tf": entry.target_ready_tf or "15m",
            "source_tags": ["micro_target_sticky"],
            "state": entry.source_state,
            "move_side": entry.move_side,
            "scan_score": entry.scan_score,
            "market_entry_suitability_score": entry.market_entry_suitability_score,
            "market_entry_suitability": entry.market_entry_suitability,
            "trade_candidate_rank_score": entry.trade_candidate_rank_score,
            "trade_candidate_bucket": entry.trade_candidate_bucket,
            "score_breakdown": {
                "price_score": 0,
                "volume_score": 0,
                "kline_cvd_score": 0,
                "trigger_5m_score": 0,
                "liquidity_score": 0,
                "background_penalty": 0,
            },
            "input_snapshot_generated_at": generated_at,
            "trigger_type": entry.trigger_type,
            "primary_15m": {"ready": False},
            "trigger_5m": {},
            "background": {},
            "reason_codes": ["synthetic_from_micro_target_sticky"],
            "next_stage": "micro_confirm",
        }
    )


def _attach_market_context(
    *,
    merged: dict[str, AbnormalSignalEntry],
    client: BinanceFuturesContextClient,
) -> dict[str, tuple[OI15mBlock, FundingContextBlock, Basis15mBlock]]:
    out: dict[str, tuple[OI15mBlock, FundingContextBlock, Basis15mBlock]] = {}
    try:
        premium_rows = fetch_premium_index_all(client)
        pmap = premium_index_by_symbol(premium_rows)
    except Exception as exc:
        log.warning("factor_snapshot premium_index fetch failed: %s", exc)
        pmap = {}

    for sym in sorted(merged.keys()):
        sig = merged[sym]
        primary = sig.primary_15m.model_dump(mode="json")
        row = pmap.get(sym)

        fb = build_funding_context_from_premium_row(row)
        bb = build_basis_15m_from_premium_row(row)

        try:
            ob = build_oi_15m_block(sym, primary, sig.move_side, client)
        except Exception as exc:
            log.warning("factor_snapshot oi fetch failed %s: %s", sym, exc)
            ob = OI15mBlock(ready=False, reason=f"oi_fetch_error:{type(exc).__name__}")

        out[sym] = (ob, fb, bb)

    return out


def build_factor_snapshot_document(
    *,
    watch: AbnormalTierDocument,
    strong: AbnormalTierDocument,
    light: FuturesLightSnapshotDocument,
    micro: LatestMicroFeaturesDocument | None,
    generated_at: str | None = None,
    fetch_market_context: bool = False,
    market_context_client: BinanceFuturesContextClient | None = None,
    now: datetime | None = None,
    micro_features_max_age_sec: int | None = None,
    micro_target_max_age_sec: int | None = None,
    micro_plan_candidate_symbols: set[str] | None = None,
    micro_target_entries: dict[str, MicroTargetEntry] | None = None,
    micro_target_generated_at: str | None = None,
    micro_target_version: str | None = None,
) -> FactorSnapshotDocument:
    """Merge watch+strong with light snapshot; optional micro (None = no OFI/CVD pipeline)."""
    gen_at = generated_at or to_iso_z(utc_now())
    merged = _merge_signals(watch, strong)
    original_merged_count = len(merged)
    candidate_alignment: dict[str, Any] = {}
    if micro is not None and micro_plan_candidate_symbols is not None:
        allowed = {s.upper().strip() for s in micro_plan_candidate_symbols if s}
        excluded = sorted(set(merged) - allowed)
        merged = {sym: sig for sym, sig in merged.items() if sym in allowed}
        synthetic_added: list[str] = []
        if micro_target_entries:
            for sym in sorted(allowed - set(merged)):
                entry = micro_target_entries.get(sym)
                if entry is None:
                    continue
                merged[sym] = _signal_from_micro_target(entry, generated_at=micro_target_generated_at or gen_at)
                synthetic_added.append(sym)
        candidate_alignment = {
            "mode": "micro_targets_authoritative",
            "source": "micro_targets.plan_candidate_symbols",
            "input_symbol_count": original_merged_count,
            "allowed_symbol_count": len(allowed),
            "output_symbol_count": len(merged),
            "excluded_not_in_micro_target": len(excluded),
            "excluded_symbols": excluded[:50],
            "synthetic_sticky_symbol_count": len(synthetic_added),
            "synthetic_sticky_symbols": synthetic_added[:50],
            "micro_target_generated_at": micro_target_generated_at,
            "micro_target_version": micro_target_version,
        }
    src: FactorSnapshotSource = (
        "factor_snapshot_without_ofi_cvd" if micro is None else "factor_snapshot"
    )
    if not merged:
        return FactorSnapshotDocument(
            schema_version="1.6",
            generated_at=gen_at,
            source=src,
            status="no_candidates",
            count=0,
            input_refs=_factor_input_refs(
                light=light,
                micro=micro,
                watch=watch,
                strong=strong,
                micro_target_generated_at=micro_target_generated_at,
                micro_target_version=micro_target_version,
                micro_plan_candidate_count=len(micro_plan_candidate_symbols or []),
            ),
            candidate_alignment=candidate_alignment,
            items=[],
        )

    light_by_symbol = {it.symbol.upper().strip(): it for it in light.items}
    micro_by_symbol: dict[str, MicroFeatureItem] = {}
    if micro is not None:
        micro_by_symbol = {it.symbol.upper().strip(): it for it in micro.items}
    doc_level_micro_reasons: list[str] = (
        ["micro_pipeline_skipped"]
        if micro is None
        else _micro_doc_input_reasons(
            micro,
            now=now,
            micro_features_max_age_sec=micro_features_max_age_sec,
            micro_target_max_age_sec=micro_target_max_age_sec,
        )
    )
    force_micro_not_ready = _micro_reasons_force_not_ready(doc_level_micro_reasons)

    context_by_symbol: dict[str, tuple[OI15mBlock, FundingContextBlock, Basis15mBlock]] = {}
    ctx_client_owned: BinanceFuturesContextClient | None = None
    if fetch_market_context:
        ctx_client_owned = market_context_client or BinanceFuturesContextClient()
        try:
            context_by_symbol = _attach_market_context(merged=merged, client=ctx_client_owned)
        finally:
            if market_context_client is None and ctx_client_owned is not None:
                ctx_client_owned.close()

    items: list[FactorSnapshotItem] = []

    for sym in sorted(merged.keys()):
        sig = merged[sym]
        ls_item = light_by_symbol.get(sym)
        entry_1m: dict[str, Any]
        if ls_item is not None:
            entry_1m = ls_item.entry_1m.model_dump(mode="json")
        else:
            entry_1m = {}

        micro_row = micro_by_symbol.get(sym) if micro is not None else None
        if micro is None:
            m15 = _synthetic_micro_15m_missing()
            mq = _synthetic_micro_quality_pipeline_skipped()
            fast_signal = None
            full_signal = None
            fq_reasons = sort_reason_codes(list(doc_level_micro_reasons))
        elif force_micro_not_ready:
            m15 = _synthetic_micro_15m_missing()
            mq = _synthetic_micro_quality_missing()
            fast_signal = _synthetic_micro_signal_block(reason_codes=list(doc_level_micro_reasons))
            full_signal = _synthetic_micro_signal_block(reason_codes=list(doc_level_micro_reasons))
            fq_reasons = sort_reason_codes(list(doc_level_micro_reasons))
        elif micro_row is None:
            m15 = _synthetic_micro_15m_missing()
            mq = _synthetic_micro_quality_missing()
            signal_reasons = ["micro_missing"] + doc_level_micro_reasons
            fast_signal = _synthetic_micro_signal_block(reason_codes=signal_reasons)
            full_signal = _synthetic_micro_signal_block(reason_codes=signal_reasons)
            fq_reasons = sort_reason_codes(signal_reasons)
        else:
            m15 = Micro15mBlock.model_validate(micro_row.micro_15m.model_dump(mode="json"))
            mq = MicroQualityBlock.model_validate(micro_row.micro_quality.model_dump(mode="json"))
            fast_signal = (
                MicroSignalBlock.model_validate(micro_row.micro_fast_signal.model_dump(mode="json"))
                if micro_row.micro_fast_signal is not None
                else _synthetic_micro_signal_block(reason_codes=["micro_fast_signal_missing"])
            )
            full_signal = (
                MicroSignalBlock.model_validate(micro_row.micro_full_signal.model_dump(mode="json"))
                if micro_row.micro_full_signal is not None
                else _synthetic_micro_signal_block(reason_codes=["micro_full_signal_missing"])
            )
            item_reasons = list(doc_level_micro_reasons)
            if not mq.ready:
                item_reasons.append("micro_15m_not_ready")
            fq_reasons = sort_reason_codes(item_reasons)

        factor_q = FactorQualityBlock(
            ready=factor_ready_from_reasons(fq_reasons),
            reason_codes=fq_reasons,
            input_warnings=[],
        )

        if fetch_market_context and sym in context_by_symbol:
            oi_ph, fund_ph, basis_ph = context_by_symbol[sym]
        else:
            oi_ph, fund_ph, basis_ph = _default_context_triplet()

        items.append(
            FactorSnapshotItem(
                symbol=sig.symbol,
                base_asset=sig.base_asset,
                decision_tf=sig.decision_tf or "15m",
                source_state=sig.state,
                move_side=sig.move_side,
                scan_score=sig.scan_score,
                market_entry_suitability_score=sig.market_entry_suitability_score,
                market_entry_suitability=sig.market_entry_suitability,
                market_entry_reason_codes=list(sig.market_entry_reason_codes),
                trigger_type=sig.trigger_type,
                primary_15m=sig.primary_15m.model_dump(mode="json"),
                trigger_5m=sig.trigger_5m.model_dump(mode="json"),
                entry_1m=entry_1m,
                background=sig.background.model_dump(mode="json"),
                micro_15m=m15,
                micro_quality=mq,
                micro_fast_signal=fast_signal,
                micro_full_signal=full_signal,
                oi_15m=oi_ph,
                funding_context=fund_ph,
                basis_15m=basis_ph,
                factor_quality=factor_q,
            )
        )

    status = _compute_top_status(items, doc_level_micro_reasons)
    return FactorSnapshotDocument(
        schema_version="1.6",
        generated_at=gen_at,
        source=src,
        status=status,
        count=len(items),
        input_refs=_factor_input_refs(
            light=light,
            micro=micro,
            watch=watch,
            strong=strong,
            micro_target_generated_at=micro_target_generated_at,
            micro_target_version=micro_target_version,
            micro_plan_candidate_count=len(micro_plan_candidate_symbols or []),
        ),
        candidate_alignment=candidate_alignment,
        items=items,
    )


def _factor_input_refs(
    *,
    light: FuturesLightSnapshotDocument,
    micro: LatestMicroFeaturesDocument | None,
    watch: AbnormalTierDocument,
    strong: AbnormalTierDocument,
    micro_target_generated_at: str | None = None,
    micro_target_version: str | None = None,
    micro_plan_candidate_count: int | None = None,
) -> dict[str, Any]:
    refs: dict[str, Any] = {
        "light_generated_at": light.generated_at,
        "watch_generated_at": watch.generated_at,
        "strong_generated_at": strong.generated_at,
    }
    if micro is None:
        refs["micro_pipeline"] = "skipped"
    else:
        refs.update(
            {
                "micro_generated_at": micro.generated_at,
                "micro_target_generated_at": micro_target_generated_at or micro.target_generated_at,
                "micro_target_version": micro_target_version,
                "micro_plan_candidate_count": micro_plan_candidate_count,
                "micro_status": micro.status,
                "micro_target_status": micro.target_status,
                "micro_fast_ready_count": micro.fast_ready_count,
                "micro_full_ready_count": micro.full_ready_count,
            },
        )
    return refs


def _compute_top_status(
    items: list[FactorSnapshotItem],
    doc_level_micro_reasons: list[str],
) -> FactorSnapshotStatus:
    if doc_level_micro_reasons:
        return "partial"
    for it in items:
        rc = it.factor_quality.reason_codes
        if "micro_missing" in rc or "micro_pipeline_skipped" in rc or "micro_15m_not_ready" in rc:
            return "partial"
    return "ok"


def attach_error_status(
    *,
    generated_at: str,
    message: str,
) -> FactorSnapshotDocument:
    """Minimal error document (empty items) for fatal load failures."""
    _ = message
    log.error("factor_snapshot error status: %s", message)
    return FactorSnapshotDocument(
        schema_version="1.6",
        generated_at=generated_at,
        source="factor_snapshot",
        status="error",
        count=0,
        items=[],
    )
