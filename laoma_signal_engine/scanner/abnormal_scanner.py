"""Step 2.0 abnormal scanner: consume futures_light_snapshot, emit raw/watch/strong JSON."""



from __future__ import annotations



import logging

from pathlib import Path

from typing import Any



import orjson



from laoma_signal_engine.core.atomic_writer import write_file_atomic

from laoma_signal_engine.core.config_loader import EngineConfig

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS

from laoma_signal_engine.core.json_io import read_json_object

from laoma_signal_engine.core.models import CandidateUniverseDocument

from laoma_signal_engine.core.time_utils import to_iso_z, utc_now

from laoma_signal_engine.market.light_snapshot_models import (

    FuturesLightSnapshotDocument,

    LightSnapshotItem,

)
from laoma_signal_engine.market.market_entry_liquidity_models import MarketEntryLiquidityDocument

from laoma_signal_engine.scanner.freshness_gate import (

    classify_input_freshness,

    decide_freshness_gate,

    effective_hard_stale_sec,

    snapshot_age_sec,

)

from laoma_signal_engine.scanner.scan_compute import (

    ScanParts,

    compute_market_entry_suitability,

    compute_scan_parts,

    compute_trade_candidate_rank,

    derive_trigger_type,

    meets_raw_candidate,

    meets_strong_candidate,

    meets_watch_candidate,

    merge_reason_codes,

    move_side_from_price_ret,

    next_stage_for_tier,

    resolve_tier,

)

from laoma_signal_engine.scanner.signal_models import (

    AbnormalSignalEntry,

    CandidateCountsBlock,

    AbnormalTierDocument,

    ScoreBreakdownBlock,

)



log = logging.getLogger(__name__)





def raw_signal_paths(project_root: Path) -> tuple[Path, Path, Path]:

    base = project_root / "DATA" / "raw_signals"

    return (

        base / "latest_raw_candidates.json",

        base / "latest_watch_signals.json",

        base / "latest_strong_candidates.json",

    )





def _rel_project_path(project_root: Path, path: Path) -> str:

    return path.resolve().relative_to(project_root.resolve()).as_posix()





def _pair_index_univ(project_root: Path | None, universe_path: Path | None) -> tuple[dict[str, Any], bool]:

    cfg = EngineConfig.load(project_root)

    path = universe_path or cfg.candidate_universe_path

    try:

        raw = read_json_object(path)

        doc = CandidateUniverseDocument.model_validate(raw)

    except (OSError, TypeError, ValueError) as exc:

        log.warning("universe load skipped: %s", exc)

        return {}, True

    out: dict[str, Any] = {}

    for p in doc.pairs:

        if p.futures_symbol:

            out[p.futures_symbol.upper()] = p

    return out, False


def _load_market_entry_liquidity_flags(project_root: Path) -> dict[str, bool]:
    path = project_root / "DATA" / "market" / "latest_market_entry_liquidity.json"
    if not path.is_file():
        return {}
    try:
        doc = MarketEntryLiquidityDocument.model_validate(read_json_object(path))
    except (OSError, TypeError, ValueError) as exc:
        log.warning("market entry liquidity flags skipped: %s", exc)
        return {}
    return {it.symbol.upper(): bool(it.liquidity_ok_for_market_entry) for it in doc.items}





def _build_signal(

    item: LightSnapshotItem,

    *,

    tier: str,

    scan_score: int,

    parts: ScanParts,

    move: str,

    trigger_type: str,

    reason_codes: list[str],

    has_um_futures: bool,

    snapshot_at: str,
    market_entry_suitability_score: int = 0,
    market_entry_suitability: str = "unknown",
    market_entry_reason_codes: list[str] | None = None,
    trade_candidate_rank_score: int = 0,
    trade_candidate_bucket: str = "unknown",
    trade_candidate_reason_codes: list[str] | None = None,
    promoted_from_raw: bool = False,

) -> AbnormalSignalEntry:

    bd = ScoreBreakdownBlock(

        price_score=parts.price_score,

        volume_score=parts.volume_score,

        kline_cvd_score=parts.kline_cvd_score,

        trigger_5m_score=parts.trigger_5m_score,

        liquidity_score=parts.liquidity_score,

        background_penalty=parts.background_penalty,

    )

    return AbnormalSignalEntry(

        symbol=item.symbol,

        base_asset=item.base_asset,

        futures_symbol=item.symbol,

        has_um_futures=has_um_futures,

        decision_tf=item.decision_tf,

        source_tags=[],  # filled by caller if pair present
        universe_profile=item.universe_profile,
        risk_profile=item.risk_profile,
        tradability_profile=item.tradability_profile,
        primary_pool=item.primary_pool,
        pool_tags=list(item.pool_tags),
        scan_priority=item.tradability_profile.scan_priority,

        state=tier,

        move_side=move,

        scan_score=scan_score,
        market_entry_suitability_score=market_entry_suitability_score,
        market_entry_suitability=market_entry_suitability,
        market_entry_reason_codes=list(market_entry_reason_codes or []),
        trade_candidate_rank_score=trade_candidate_rank_score,
        trade_candidate_bucket=trade_candidate_bucket,
        trade_candidate_reason_codes=list(trade_candidate_reason_codes or []),
        promoted_from_raw=promoted_from_raw,

        score_breakdown=bd,

        input_snapshot_generated_at=snapshot_at,

        trigger_type=trigger_type,

        primary_15m=item.primary_15m,

        trigger_5m=item.trigger_5m,

        background=item.background,

        reason_codes=reason_codes,

        next_stage=next_stage_for_tier(tier),

    )





def _run_scan_body(

    snap: FuturesLightSnapshotDocument,

    pairs: dict[str, Any],

    universe_missing: bool,
    liquidity_by_symbol: dict[str, bool] | None = None,

) -> dict[str, list[AbnormalSignalEntry]]:

    buckets: dict[str, list[AbnormalSignalEntry]] = {

        "strong_candidate": [],

        "watch_candidate": [],

        "raw_candidate": [],

    }

    for item in snap.items:

        if not item.primary_15m.ready:

            continue

        if item.item_downstream_allowed is False or item.item_freshness_status == "stale_blocked":

            continue

        sym_u = item.symbol.upper()

        pair = pairs.get(sym_u)

        if pair is not None and not pair.has_um_futures:

            continue
        if (
            item.risk_profile.execution_tier == "no_trade"
            or item.universe_profile.manual_mode in {"blacklist", "no_trade", "exclude"}
            or item.universe_profile.scan_eligibility == "block"
            or item.universe_profile.business_pool == "no_trade"
            or item.tradability_profile.tradability_tier == "no_trade"
        ):
            continue

        has_um = bool(pair.has_um_futures) if pair is not None else True

        pr15 = item.primary_15m.price_ret

        vr = item.primary_15m.volume_ratio

        move = move_side_from_price_ret(pr15)

        rank_vol = pair.rank_futures_volume if pair is not None else None

        reason_extra: list[str] = []

        parts = compute_scan_parts(

            price_ret_15m=pr15,

            volume_ratio=vr,

            move=move,

            kline_cvd_state=item.primary_15m.kline_cvd_state,

            acceleration_state=item.trigger_5m.acceleration_state,

            overheat=item.background.background_overheat,

            rank_futures_volume=rank_vol,

            universe_missing=universe_missing,

            reason_acc=reason_extra,

        )

        reasons = merge_reason_codes(item.reason_codes, reason_extra)

        trig = derive_trigger_type(scan_score=parts.scan_score, price_ret_15m=pr15, volume_ratio=vr)
        mes = compute_market_entry_suitability(
            scan_score=parts.scan_score,
            move=move,
            price_ret_15m=pr15,
            volume_ratio_15m=vr,
            acceleration_state=item.trigger_5m.acceleration_state,
            range_pos=item.primary_15m.range_pos,
            liquidity_ok=(liquidity_by_symbol or {}).get(sym_u),
        )
        trade_rank = compute_trade_candidate_rank(
            scan_score=parts.scan_score,
            market_entry_suitability_score=mes.score,
            market_entry_suitability=mes.bucket,
            liquidity_ok=(liquidity_by_symbol or {}).get(sym_u),
        )

        strong_ok = meets_strong_candidate(

            scan_score=parts.scan_score,

            primary_ready=item.primary_15m.ready,

            volume_ratio=vr,

            price_ret_15m=pr15,

            kline_cvd_state=item.primary_15m.kline_cvd_state,

            acceleration_state=item.trigger_5m.acceleration_state,

            structure_state=item.primary_15m.structure_state,

            move=move,

            overheat=item.background.background_overheat,

        )

        watch_ok = meets_watch_candidate(

            scan_score=parts.scan_score,

            volume_ratio=vr,

            price_ret_15m=pr15,

            overheat=item.background.background_overheat,

        )

        raw_ok = meets_raw_candidate(

            scan_score=parts.scan_score,

            volume_ratio=vr,

            price_ret_15m=pr15,

        )

        tier = resolve_tier(strong_ok=strong_ok, watch_ok=watch_ok, raw_ok=raw_ok)

        if tier is None:

            continue

        sig = _build_signal(

            item,

            tier=tier,

            scan_score=parts.scan_score,

            parts=parts,

            move=move,

            trigger_type=trig,

            reason_codes=reasons,

            has_um_futures=has_um,

            snapshot_at=snap.generated_at,
            market_entry_suitability_score=mes.score,
            market_entry_suitability=mes.bucket,
            market_entry_reason_codes=list(mes.reason_codes),
            trade_candidate_rank_score=trade_rank.score,
            trade_candidate_bucket=trade_rank.bucket,
            trade_candidate_reason_codes=list(trade_rank.reason_codes),

        )

        if pair is not None:

            sig = sig.model_copy(update={"source_tags": list(pair.source_tags)})
        sig = sig.model_copy(
            update={
                "scan_priority": max(
                    sig.scan_priority,
                    int(getattr(sig, "trade_candidate_rank_score", 0) or 0),
                    int(getattr(sig.tradability_profile, "market_entry_score", 0) or 0),
                    int(getattr(sig.tradability_profile, "hf_stop_score", 0) or 0),
                ),
            }
        )

        buckets[tier].append(sig)

    for tier in buckets:
        buckets[tier].sort(
            key=lambda s: (
                -int(getattr(s, "scan_priority", 0) or 0),
                -int(getattr(s, "trade_candidate_rank_score", 0) or 0),
                -int(s.scan_score),
                s.symbol,
            )
        )
    return buckets


def _candidate_counts(signals: list[AbnormalSignalEntry], attr_name: str) -> CandidateCountsBlock:
    counts = {"preferred": 0, "allowed": 0, "observe": 0, "avoid": 0, "unknown": 0}
    for sig in signals:
        value = str(getattr(sig, attr_name, "unknown") or "unknown")
        key = value if value in counts else "unknown"
        counts[key] += 1
    return CandidateCountsBlock(**counts)





def run_abnormal_scan(

    *,

    project_root: Path | None = None,

    snapshot_path: Path | None = None,

    universe_path: Path | None = None,

    stdout_json: bool = False,

    allow_stale_input: bool = False,

    strict_freshness_cli: bool = False,

    max_snapshot_age_sec: int | None = None,

    strict_freshness_override: bool | None = None,

) -> int:

    cfg = EngineConfig.load(project_root)

    snap_p = snapshot_path or cfg.futures_light_snapshot_path

    try:

        snap_raw = read_json_object(snap_p)

        snap = FuturesLightSnapshotDocument.model_validate(snap_raw)

    except (OSError, TypeError, ValueError) as exc:

        log.error("cannot load snapshot: %s", exc)

        return EXIT_CONFIG



    age_sec = snapshot_age_sec(snap.generated_at)

    effective_hard = effective_hard_stale_sec(

        config_hard=cfg.light_snapshot_hard_stale_sec,

        max_age_sec=cfg.light_snapshot_max_age_sec,

        cli_override=max_snapshot_age_sec,

    )

    input_freshness = classify_input_freshness(

        age_sec=age_sec,

        max_age_sec=cfg.light_snapshot_max_age_sec,

        effective_hard_sec=effective_hard,

    )

    if strict_freshness_override is not None:

        strict_eff = strict_freshness_override

    else:

        strict_eff = bool(cfg.strict_freshness or strict_freshness_cli)



    gate = decide_freshness_gate(

        input_freshness=input_freshness,

        strict_freshness=strict_eff,

        allow_stale_input=allow_stale_input,

    )



    pairs, universe_missing = _pair_index_univ(project_root, universe_path)
    liquidity_by_symbol = _load_market_entry_liquidity_flags(cfg.project_root)

    if gate.scan_allowed:

        buckets = _run_scan_body(snap, pairs, universe_missing, liquidity_by_symbol)

    else:

        buckets = {

            "strong_candidate": [],

            "watch_candidate": [],

            "raw_candidate": [],

        }



    raw_path, watch_path, strong_path = raw_signal_paths(cfg.project_root)

    gen_at = to_iso_z(utc_now())

    tier_paths = {

        "raw_candidate": raw_path,

        "watch_candidate": watch_path,

        "strong_candidate": strong_path,

    }

    try:

        for tier, path in tier_paths.items():

            doc = AbnormalTierDocument(

                schema_version=cfg.schema_version,

                generated_at=gen_at,

                source="abnormal_scanner",

                tier=tier,

                status=gate.status,

                input_snapshot_generated_at=snap.generated_at,

                input_snapshot_age_sec=age_sec,

                input_freshness=gate.input_freshness,

                stale_warning=gate.stale_warning,

                reason_codes=list(gate.top_reason_codes),

                count=len(buckets[tier]),
                market_entry_counts=_candidate_counts(buckets[tier], "market_entry_suitability"),
                trade_candidate_counts=_candidate_counts(buckets[tier], "trade_candidate_bucket"),

                signals=buckets[tier],

            )

            payload = doc.model_dump(mode="json")

            data = orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)

            write_file_atomic(path, data)

    except OSError as exc:

        log.error("write raw signals failed: %s", exc)

        return EXIT_CONFIG



    log.info(

        "scan status=%s freshness=%s age_sec=%s raw=%s watch=%s strong=%s snapshot=%s",

        gate.status,

        gate.input_freshness,

        age_sec,

        len(buckets["raw_candidate"]),

        len(buckets["watch_candidate"]),

        len(buckets["strong_candidate"]),

        snap_p,

    )

    if stdout_json:

        import sys



        pr = cfg.project_root.resolve()

        summary = {

            "schema_version": cfg.schema_version,

            "source": "abnormal_scanner",

            "status": gate.status,

            "input_snapshot_generated_at": snap.generated_at,

            "input_snapshot_age_sec": age_sec,

            "input_freshness": gate.input_freshness,

            "stale_warning": gate.stale_warning,

            "raw_count": len(buckets["raw_candidate"]),

            "watch_count": len(buckets["watch_candidate"]),

            "strong_count": len(buckets["strong_candidate"]),

            "output_files": {

                "raw": _rel_project_path(pr, raw_path),

                "watch": _rel_project_path(pr, watch_path),

                "strong": _rel_project_path(pr, strong_path),

            },

        }

        sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))

        sys.stdout.buffer.flush()

    return EXIT_SUCCESS





def run_abnormal_scan_safe(**kwargs: Any) -> int:

    try:

        return run_abnormal_scan(**kwargs)

    except Exception as exc:

        log.exception("abnormal scan failed: %s", exc)

        return EXIT_INTERNAL

