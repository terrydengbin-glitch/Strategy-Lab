"""Step 1.5: build DATA/market/futures_light_snapshot.json."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import orjson
import yaml

from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.core.config_loader import EngineConfig, package_root
from laoma_signal_engine.core.exit_codes import EXIT_BINANCE, EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.models import CandidateUniverseDocument, UniversePairRow
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.market.kline_fetcher import (
    KlineBar,
    fetch_klines,
    fetch_ticker_24h_all,
    ticker_by_symbol_map,
)
from laoma_signal_engine.market.light_snapshot_compute import (
    aggregate_last_n_1m,
    atr_mean_tr_on_closed_1m,
    atr_on_closed_15m,
    build_reason_codes,
    clamp01,
    finalize_structure_state,
    infer_acceleration_state,
    infer_structure_state,
    infer_volatility_state,
    kline_cvd_state,
    mean_quote_volume_closed_15m,
    price_ret_1h_from_closed_1h,
    price_ret_pct,
    range_break_state_from_raw,
    swing_range_from_closed_15m,
    taker_sell_volume,
    volume_ratio_5m_from_1m,
)
from laoma_signal_engine.market.light_snapshot_models import (
    BackgroundBlock,
    DataQualityBlock,
    Entry1mBlock,
    FuturesLightSnapshotDocument,
    LightSnapshotItem,
    Primary15mBlock,
    SnapshotErrorEntry,
    TimeframeContract,
    TradabilityProfileBlock,
    Trigger5mBlock,
)
from laoma_signal_engine.market.light_snapshot_settings import LightSnapshotSettings, load_light_snapshot_settings
from laoma_signal_engine.pipeline import LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT
from laoma_signal_engine.universe.step15_symbols import futures_symbols_for_step_1_5

log = logging.getLogger(__name__)

ENTRY_1M_ATR_PERIOD = 14
MIN_1M_BARS_FOR_5M_VOLUME_RATIO = 105
KLINES_1M_LIMIT = 150


def _extend_reasons_unique(reasons: list[str], *extra: str) -> None:
    seen = set(reasons)
    for e in extra:
        if e not in seen:
            seen.add(e)
            reasons.append(e)


def _apply_kline_clock_to_data_quality(
    k1: list[KlineBar],
    generated_at_ms: int,
    dq: DataQualityBlock,
) -> tuple[DataQualityBlock, list[str]]:
    """Resolve last-closed 1m age vs document clock; tag unknown/stale paths."""
    extra_reasons: list[str] = []
    if not k1:
        dq2 = dq.model_copy(
            update={
                "last_closed_kline_age_sec": None,
                "snapshot_age_sec": None,
                "uses_open_kline_for_rolling": False,
                "error_code": dq.error_code or "KLINE_TIME_UNKNOWN",
            }
        )
        extra_reasons.append("kline_time_unknown")
        return dq2, extra_reasons

    last_closed: KlineBar | None = None
    for b in reversed(k1):
        if b.close_time_ms <= generated_at_ms:
            last_closed = b
            break
    uses_open = k1[-1].close_time_ms > generated_at_ms

    if last_closed is None:
        dq2 = dq.model_copy(
            update={
                "last_closed_kline_age_sec": None,
                "snapshot_age_sec": None,
                "uses_open_kline_for_rolling": uses_open,
                "error_code": dq.error_code or "KLINE_TIME_UNKNOWN",
            }
        )
        extra_reasons.append("kline_time_unknown")
        return dq2, extra_reasons

    last_closed_age = max(0.0, (generated_at_ms - last_closed.close_time_ms) / 1000.0)
    if uses_open:
        snap_age = max(0.0, (generated_at_ms - k1[-1].open_time_ms) / 1000.0)
    else:
        snap_age = last_closed_age

    dq2 = dq.model_copy(
        update={
            "last_closed_kline_age_sec": last_closed_age,
            "snapshot_age_sec": snap_age,
            "uses_open_kline_for_rolling": uses_open,
            "error_code": dq.error_code,
        }
    )
    if snap_age > 60.0:
        extra_reasons.append("snapshot_stale")
    return dq2, extra_reasons


def _parse_univ_time(iso_s: str) -> datetime:
    s = iso_s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _load_timeframe_contract() -> TimeframeContract:
    path = package_root() / "config" / "timeframe.yaml"
    raw = path.read_text(encoding="utf-8")
    doc: dict[str, Any] = yaml.safe_load(raw) or {}
    return TimeframeContract(
        primary_tf=str(doc.get("primary_tf", "15m")),
        trigger_tf=str(doc.get("trigger_tf", "5m")),
        entry_tf=str(doc.get("entry_tf", "1m")),
        background_tfs=list(doc.get("background_tfs") or ["1h", "24h"]),
        decision_basis=str(doc.get("decision_basis", "rolling_15m")),
    )


def _pair_index(doc: CandidateUniverseDocument) -> dict[str, UniversePairRow]:
    out: dict[str, UniversePairRow] = {}
    for p in doc.pairs:
        if p.futures_symbol:
            out[p.futures_symbol.upper()] = p
    return out


def _float_field(row: dict[str, Any], key: str) -> float | None:
    v = row.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clamp_score(value: float) -> int:
    return int(max(0, min(100, round(value))))


def _tradability_for(
    pair: UniversePairRow,
    *,
    price_ret_15m: float | None = None,
    volume_ratio_15m: float | None = None,
    volume_ratio_5m: float | None = None,
    quote_volume_15m: float | None = None,
    ticker_quote_volume_24h: float | None = None,
    data_ready: bool = False,
) -> tuple[TradabilityProfileBlock, str, list[str]]:
    qv24 = ticker_quote_volume_24h if ticker_quote_volume_24h is not None else pair.quote_volume_24h_futures
    qv = float(qv24 or 0.0)
    activity = _clamp_score(20 + min(70, (qv / 500_000_000.0) * 70)) if qv > 0 else 0
    if qv >= 500_000_000:
        activity = max(activity, 90)
    elif qv >= 50_000_000:
        activity = max(activity, 70)
    elif qv >= 5_000_000:
        activity = max(activity, 45)

    hotness = _clamp_score(abs(float(price_ret_15m or 0.0)) * 7.0 + min(40, float(volume_ratio_15m or 0.0) * 12.0))
    light_liq = _clamp_score(activity * 0.75 + min(25, float(quote_volume_15m or 0.0) / 1_000_000.0 * 10.0))
    volatility = _clamp_score(abs(float(price_ret_15m or 0.0)) * 10.0)
    accel = _clamp_score(float(volume_ratio_5m or 0.0) * 25.0)
    spread_quality = _clamp_score(light_liq * 0.65 + activity * 0.35)
    depth_stability = _clamp_score(light_liq * 0.70 + (100 - min(100, volatility)) * 0.30)
    slippage_risk = _clamp_score(100 - (spread_quality * 0.55 + depth_stability * 0.45))
    market_entry = _clamp_score(
        light_liq * 0.35
        + activity * 0.25
        + hotness * 0.15
        + accel * 0.10
        + spread_quality * 0.15
        - slippage_risk * 0.15
    )
    hf_stop = _clamp_score(depth_stability * 0.35 + light_liq * 0.30 + min(100, volatility) * 0.20 + accel * 0.15)
    base_priority = int(pair.universe_profile.universe_priority_score or 0)
    score = _clamp_score(activity * 0.25 + hotness * 0.20 + light_liq * 0.20 + accel * 0.10 + market_entry * 0.25)
    scan_priority = max(score, base_priority)

    reasons: list[str] = []
    if not data_ready:
        reasons.append("snapshot_not_ready")
    if pair.risk_profile.execution_tier == "no_trade":
        reasons.append("execution_no_trade")
    if pair.universe_profile.is_multiplier_contract:
        reasons.append("multiplier_contract")
    if pair.universe_profile.manual_mode:
        reasons.append(f"manual_{pair.universe_profile.manual_mode}")

    if pair.risk_profile.execution_tier == "no_trade":
        tier = "no_trade"
        pool = "risk_watch_pool"
    elif pair.universe_profile.manual_mode in {"force_scan", "force_micro", "watch_only"}:
        tier = "manual_watch"
        pool = "manual_watch_pool"
    elif score >= 70:
        tier = "market_ok"
        pool = "active_mover_pool"
    elif pair.universe_profile.universe_tier == "tier_A_core":
        tier = "market_ok"
        pool = "core_liquid_pool"
    elif score >= 45:
        tier = "market_careful"
        pool = "emerging_pool"
    elif score >= 25:
        tier = "watch_only"
        pool = "watch_pool"
    else:
        tier = "no_trade_hint"
        pool = "risk_watch_pool"

    if tier in {"no_trade", "no_trade_hint"}:
        trade_quality_tier = "blocked" if tier == "no_trade" else "observe_only"
    elif market_entry >= 70 and hf_stop >= 55 and slippage_risk <= 35:
        trade_quality_tier = "market_entry_fit"
    elif market_entry >= 45 and slippage_risk <= 60:
        trade_quality_tier = "market_entry_careful"
    elif hf_stop >= 45:
        trade_quality_tier = "observe_hf_stop"
    else:
        trade_quality_tier = "observe_only"

    profile = TradabilityProfileBlock(
        activity_score=activity,
        hotness_score=hotness,
        light_liquidity_score=light_liq,
        volatility_score=volatility,
        volume_accel_score=accel,
        tradability_score=score,
        tradability_tier=tier,
        market_entry_score=market_entry,
        hf_stop_score=hf_stop,
        slippage_risk_score=slippage_risk,
        depth_stability_score=depth_stability,
        volume_activity_score=activity,
        spread_quality_score=spread_quality,
        trade_quality_tier=trade_quality_tier,
        scan_priority=scan_priority,
        reason_codes=sorted(set(reasons)),
    )
    return profile, pool, sorted(set([pool, tier]))


def _failed_item(
    symbol: str,
    pair: UniversePairRow,
    *,
    code: str,
    message: str,
    stage: str,
    reasons: list[str],
    k1: list[KlineBar] | None = None,
    generated_at_ms: int | None = None,
) -> tuple[LightSnapshotItem | None, SnapshotErrorEntry]:
    dq = DataQualityBlock(
        kline_1m_ready=False,
        error_code=code,
        error_message=message[:500],
    )
    merged_reasons = list(reasons)
    if k1 is not None and generated_at_ms is not None:
        dq, time_extra = _apply_kline_clock_to_data_quality(k1, generated_at_ms, dq)
        _extend_reasons_unique(merged_reasons, *time_extra)
    item = LightSnapshotItem(
        symbol=symbol,
        base_asset=pair.base_asset,
        last_price=None,
        primary_15m=Primary15mBlock(ready=False),
        trigger_5m=Trigger5mBlock(),
        entry_1m=Entry1mBlock(),
        background=BackgroundBlock(),
        reason_codes=merged_reasons,
        data_quality=dq,
        universe_profile=pair.universe_profile,
        risk_profile=pair.risk_profile,
        tradability_profile=_tradability_for(pair, data_ready=False)[0],
        primary_pool=_tradability_for(pair, data_ready=False)[1],
        pool_tags=_tradability_for(pair, data_ready=False)[2],
    )
    err = SnapshotErrorEntry(symbol=symbol, error_code=code, stage=stage)
    return item, err


def _build_item_for_symbol(
    symbol: str,
    pair: UniversePairRow,
    k1: list[KlineBar],
    k15: list[KlineBar],
    k1h: list[KlineBar],
    t_row: dict[str, Any] | None,
    settings: LightSnapshotSettings,
    generated_at_ms: int,
) -> tuple[LightSnapshotItem, SnapshotErrorEntry | None]:
    diag: list[str] = []
    err_entry: SnapshotErrorEntry | None = None

    dq = DataQualityBlock(
        kline_1m_ready=len(k1) >= 15,
        kline_5m_ready=len(k1) >= MIN_1M_BARS_FOR_5M_VOLUME_RATIO,
        kline_15m_ready=len(k15) >= 21,
        kline_1h_ready=len(k1h) >= 3,
        ticker_24h_ready=t_row is not None,
    )
    dq, time_reasons = _apply_kline_clock_to_data_quality(k1, generated_at_ms, dq)

    win15 = aggregate_last_n_1m(k1, 15)
    win5 = aggregate_last_n_1m(k1, 5)
    if win15 is None or win5 is None:
        diag.append("kline_window_insufficient")
        item, err = _failed_item(
            symbol,
            pair,
            code="KLINE_INSUFFICIENT",
            message="need at least 15 1m bars",
            stage="aggregate_1m",
            reasons=diag,
            k1=k1,
            generated_at_ms=generated_at_ms,
        )
        return item, err

    last_price = win15.close
    price_15_ago = k1[-15].open
    price_5_ago = k1[-5].open
    pr_15 = price_ret_pct(last_price, price_15_ago)
    pr_5 = price_ret_pct(last_price, price_5_ago)

    closed_15 = k15[:-1] if len(k15) > 1 else []
    mean20 = mean_quote_volume_closed_15m(closed_15, 20)
    vol_ratio: float | None = None
    if mean20 is not None and mean20 > 0:
        vol_ratio = win15.quote_volume / mean20
    else:
        diag.append("volume_ratio_baseline_insufficient")

    vol_ratio_5m = volume_ratio_5m_from_1m(k1, 20)
    if vol_ratio_5m is None:
        diag.append("volume_ratio_5m_baseline_insufficient")

    vol = win15.volume
    taker_buy = win15.taker_buy_base
    taker_sell = taker_sell_volume(vol, taker_buy)
    tbr: float | None = (taker_buy / vol) if vol > 0 else None
    cvd_delta = taker_buy - taker_sell if vol > 0 else None
    cvd_st = kline_cvd_state(vol, tbr)

    atr_val = atr_on_closed_15m(closed_15, settings.atr_period_15m)
    if atr_val is None:
        diag.append("atr_insufficient")

    closed_1m = k1[:-1] if len(k1) > 1 else []
    atr_1m = atr_mean_tr_on_closed_1m(closed_1m, ENTRY_1M_ATR_PERIOD)
    if atr_1m is None:
        diag.append("entry_atr_1m_insufficient")

    swing = swing_range_from_closed_15m(closed_15, 12) if closed_15 else None
    range_raw: float | None = None
    range_clamped: float | None = None
    brk_st = "inside"
    r_hi: float | None = None
    r_lo: float | None = None
    range_invalid = False
    if swing is not None:
        r_hi, r_lo = swing
        if r_hi == r_lo:
            range_invalid = True
            range_raw = None
            range_clamped = None
            brk_st = "inside"
            diag.append("range_invalid")
        else:
            range_raw = (last_price - r_lo) / (r_hi - r_lo)
            range_clamped = clamp01(range_raw)
            brk_st = range_break_state_from_raw(range_raw)

    accel = infer_acceleration_state(pr_5, pr_15)
    struct_base = infer_structure_state(pr_15, cvd_st)
    struct_st = finalize_structure_state(
        struct_base,
        range_raw,
        pr_15,
        vol_ratio,
        accel,
    )
    vol_st = infer_volatility_state(vol_ratio)

    n_e = min(settings.entry_1m_bars, len(k1))
    entry_bars = k1[-n_e:]
    elow = min(b.low for b in entry_bars)
    ehigh = max(b.high for b in entry_bars)

    closed_1h = k1h[:-1] if len(k1h) > 1 else []
    pr_1h = price_ret_1h_from_closed_1h(closed_1h)

    pr_24: float | None = None
    qv_24: float | None = None
    if t_row:
        pr_24 = _float_field(t_row, "priceChangePercent")
        qv_24 = _float_field(t_row, "quoteVolume")

    top_g = False
    top_l = False
    if pair.rank_futures_gainer is not None and pair.rank_futures_gainer <= settings.top_gainer_rank_threshold:
        top_g = True
    if pair.rank_futures_loser is not None and pair.rank_futures_loser <= settings.top_loser_rank_threshold:
        top_l = True

    overheat = False
    if pr_24 is not None and pr_24 > 40.0:
        overheat = True

    primary = Primary15mBlock(
        price_ret=pr_15,
        volume_ratio=vol_ratio,
        quote_volume=win15.quote_volume,
        atr=atr_val,
        range_pos=range_clamped,
        range_pos_raw=range_raw,
        range_pos_clamped=range_clamped,
        range_break_state=brk_st,
        structure_state=struct_st,
        volatility_state=vol_st,
        taker_buy_volume=taker_buy if vol > 0 else None,
        taker_sell_volume=taker_sell if vol > 0 else None,
        taker_buy_ratio=tbr,
        kline_cvd_delta=cvd_delta,
        kline_cvd_state=cvd_st,
        recent_swing_high=r_hi,
        recent_swing_low=r_lo,
        breakout_level=r_hi,
        breakdown_level=r_lo,
        ready=True,
    )

    ready = (
        dq.kline_1m_ready
        and dq.kline_15m_ready
        and mean20 is not None
        and vol > 0
        and t_row is not None
        and atr_val is not None
        and not range_invalid
    )
    if not ready:
        primary = primary.model_copy(update={"ready": False})

    trigger = Trigger5mBlock(
        price_ret=pr_5,
        volume_ratio=vol_ratio_5m,
        acceleration_state=accel,
    )
    entry = Entry1mBlock(
        atr=atr_1m,
        last_pullback_low=elow,
        last_breakout_high=ehigh,
    )
    bg = BackgroundBlock(
        price_ret_1h=pr_1h,
        price_ret_24h=pr_24,
        quote_volume_24h=qv_24,
        is_top_gainer_24h=top_g,
        is_top_loser_24h=top_l,
        background_overheat=overheat,
    )

    reason_codes = build_reason_codes(
        price_ret_15m=pr_15,
        volume_ratio_15m=vol_ratio,
        kline_cvd_state=cvd_st,
        acceleration_state=accel,
        structure_state=struct_st,
        background_overheat=overheat,
        diag_tags=diag,
    )
    _extend_reasons_unique(reason_codes, *time_reasons)
    tradability, primary_pool, pool_tags = _tradability_for(
        pair,
        price_ret_15m=pr_15,
        volume_ratio_15m=vol_ratio,
        volume_ratio_5m=vol_ratio_5m,
        quote_volume_15m=win15.quote_volume,
        ticker_quote_volume_24h=qv_24,
        data_ready=ready,
    )

    item = LightSnapshotItem(
        symbol=symbol,
        base_asset=pair.base_asset,
        last_price=last_price,
        primary_15m=primary,
        trigger_5m=trigger,
        entry_1m=entry,
        background=bg,
        reason_codes=reason_codes,
        data_quality=dq,
        universe_profile=pair.universe_profile,
        risk_profile=pair.risk_profile,
        tradability_profile=tradability,
        primary_pool=primary_pool,
        pool_tags=pool_tags,
    )
    if not ready and not reason_codes:
        item = item.model_copy(update={"reason_codes": ["not_ready"]})
    return item, err_entry


def _legacy_fetch_klines_bundle(
    symbol: str,
    settings: LightSnapshotSettings,
) -> tuple[list[KlineBar], list[KlineBar], list[KlineBar]]:
    timeout = httpx.Timeout(settings.request_timeout_sec)
    with httpx.Client(timeout=timeout) as client:
        k1 = fetch_klines(client, symbol, "1m", KLINES_1M_LIMIT)
        k15 = fetch_klines(client, symbol, "15m", 45)
        k1h = fetch_klines(client, symbol, "1h", 8)
    time.sleep(settings.batch_sleep_ms / 1000.0)
    return k1, k15, k1h


def run_fetch_light_snapshot(
    *,
    project_root: Path | None = None,
    limit: int | None = None,
    symbols_filter: list[str] | None = None,
    max_concurrency: int | None = None,
    output_path: Path | None = None,
    settings: LightSnapshotSettings | None = None,
    fetch_mode: str = LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT,
    dry_run_plan: bool = False,
    stdout_json: bool = False,
) -> int:
    fm_raw = (fetch_mode or LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT).strip().lower()
    fm = "async" if fm_raw == "distributed" else fm_raw
    if dry_run_plan:
        from laoma_signal_engine.market.light_snapshot_async import dry_run_plan_dict, dry_run_plan_text

        import sys

        if stdout_json:
            rec = dry_run_plan_dict(
                project_root=project_root,
                limit=limit,
                symbols_filter=symbols_filter,
                max_concurrency=max_concurrency,
                fetch_mode=fm_raw,
            )
            sys.stdout.buffer.write(orjson.dumps(rec, option=orjson.OPT_APPEND_NEWLINE))
        else:
            sys.stderr.write(
                dry_run_plan_text(
                    project_root=project_root,
                    limit=limit,
                    symbols_filter=symbols_filter,
                    max_concurrency=max_concurrency,
                    fetch_mode=fm_raw,
                )
            )
        return EXIT_SUCCESS

    if fm == "async":
        from laoma_signal_engine.market.light_snapshot_async import run_fetch_light_snapshot_async_safe

        return run_fetch_light_snapshot_async_safe(
            project_root=project_root,
            limit=limit,
            symbols_filter=symbols_filter,
            max_concurrency=max_concurrency,
            output_path=output_path,
            settings=settings,
            perf_fetch_mode=fm_raw,
        )

    cfg = EngineConfig.load(project_root)
    ls = settings or load_light_snapshot_settings()
    out = output_path or cfg.futures_light_snapshot_path

    try:
        univ_raw = read_json_object(cfg.candidate_universe_path)
        doc = CandidateUniverseDocument.model_validate(univ_raw)
    except (OSError, TypeError, ValueError) as exc:
        log.error("cannot load universe: %s", exc)
        return EXIT_CONFIG

    full_eligible = futures_symbols_for_step_1_5(doc)
    if doc.counts.futures_count != len(full_eligible):
        log.warning(
            "Universe counts.futures_count=%s but step15 symbol list length=%s",
            doc.counts.futures_count,
            len(full_eligible),
        )
    pairs = _pair_index(doc)

    symbols: list[str] = list(full_eligible)
    if symbols_filter:
        allow = {s.strip().upper() for s in symbols_filter if s.strip()}
        symbols = [s for s in symbols if s in allow]
    if limit is not None and limit > 0:
        symbols = symbols[:limit]
    symbols = [s for s in symbols if s in pairs]

    skipped = len(full_eligible) - len(symbols)

    workers = max_concurrency if max_concurrency is not None else ls.max_concurrency
    workers = max(1, min(workers, 32))

    timeout = httpx.Timeout(ls.request_timeout_sec)
    try:
        with httpx.Client(timeout=timeout) as client:
            all_tickers = fetch_ticker_24h_all(client)
    except (httpx.HTTPError, OSError, TypeError, ValueError) as exc:
        log.error("ticker/24hr failed: %s", exc)
        return EXIT_BINANCE

    ticker_map = ticker_by_symbol_map(all_tickers)
    gen_dt = _parse_univ_time(doc.generated_at)
    uni_age = int((utc_now() - gen_dt).total_seconds())

    items: list[LightSnapshotItem] = []
    errors: list[SnapshotErrorEntry] = []

    if not symbols:
        log.warning("no symbols to scan")
    else:
        sym_klines: dict[str, tuple[list[KlineBar], list[KlineBar], list[KlineBar]]] = {}
        sym_fetch_fail: dict[str, tuple[LightSnapshotItem, SnapshotErrorEntry]] = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {
                pool.submit(_legacy_fetch_klines_bundle, sym, ls): sym
                for sym in symbols
                if sym in pairs
            }
            for fut in as_completed(futs):
                sym = futs[fut]
                try:
                    sym_klines[sym] = fut.result()
                except (httpx.HTTPError, OSError, TypeError, ValueError) as exc:
                    log.warning("kline fetch failed %s: %s", sym, exc)
                    time.sleep(ls.batch_sleep_ms / 1000.0)
                    p = pairs.get(sym)
                    if p:
                        it, er = _failed_item(
                            sym,
                            p,
                            code="KLINE_FETCH_FAILED",
                            message=str(exc),
                            stage="fetch_klines",
                            reasons=["kline_fetch_failed"],
                        )
                        sym_fetch_fail[sym] = (it, er)
                except Exception as exc:
                    log.exception("worker %s: %s", sym, exc)
                    p = pairs.get(sym)
                    if p:
                        it, er = _failed_item(
                            sym,
                            p,
                            code="INTERNAL",
                            message=str(exc),
                            stage="worker",
                            reasons=["internal_error"],
                        )
                        sym_fetch_fail[sym] = (it, er)

        snapshot_ref_ms = int(time.time() * 1000)
        for sym in symbols:
            if sym not in pairs:
                continue
            if sym in sym_fetch_fail:
                fit, fer = sym_fetch_fail[sym]
                items.append(fit)
                errors.append(fer)
            elif sym in sym_klines:
                k1, k15, k1h = sym_klines[sym]
                t_row = ticker_map.get(sym.upper())
                item, err = _build_item_for_symbol(
                    sym, pairs[sym], k1, k15, k1h, t_row, ls, snapshot_ref_ms
                )
                items.append(item)
                if err:
                    errors.append(err)

    success = sum(1 for it in items if it.primary_15m.ready)
    failed = len(items) - success
    failed_symbols = sorted(
        {
            str(err.symbol).upper()
            for err in errors
            if getattr(err, "symbol", None)
        }
    )
    quality_reason_codes: list[str] = []
    if failed > 0:
        quality_reason_codes.append("light_snapshot_partial_failed_symbols")
    snapshot_quality = {
        "snapshot_status": "partial" if failed > 0 else "ok",
        "snapshot_success_count": success,
        "snapshot_failed_count": failed,
        "snapshot_failed_symbols": failed_symbols[:200],
        "snapshot_failed_symbol_count": len(failed_symbols),
        "requested_count": len(symbols),
        "eligible_futures_count": len(full_eligible),
        "skipped_count": max(0, skipped),
        "downstream_candidate_count": success,
        "weight_throttle_count": 0,
        "http_429_count": 0,
        "http_418_count": 0,
        "cache_fallback_count": 0,
        "exchange_info_source": "live",
        "exchange_info_live_error": None,
        "reason_codes": quality_reason_codes,
    }
    tc = _load_timeframe_contract()
    pools: dict[str, list[str]] = {}
    for it in items:
        pools.setdefault(it.primary_pool or "unknown", []).append(it.symbol.upper())
    pools = {k: sorted(set(v)) for k, v in sorted(pools.items())}

    snapshot = FuturesLightSnapshotDocument(
        schema_version=cfg.schema_version,
        generated_at=to_iso_z(utc_now()),
        source="binance_um_futures",
        universe_generated_at=doc.generated_at,
        universe_age_sec=uni_age,
        universe_count=doc.count,
        eligible_futures_count=len(full_eligible),
        snapshot_count=len(items),
        success_count=success,
        failed_count=failed,
        skipped_count=max(0, skipped),
        timeframe_contract=tc,
        items=items,
        errors=errors,
        pools=pools,
        snapshot_quality=snapshot_quality,
    )

    payload = snapshot.model_dump(mode="json")
    try:
        data = orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
        write_file_atomic(out, data)
    except OSError as exc:
        log.error("write failed: %s", exc)
        return EXIT_CONFIG

    log.info(
        "wrote %s snapshot_count=%s success=%s failed=%s skipped=%s",
        out,
        len(items),
        success,
        failed,
        skipped,
    )
    return EXIT_SUCCESS


def run_fetch_light_snapshot_safe(**kwargs: Any) -> int:
    try:
        return run_fetch_light_snapshot(**kwargs)
    except Exception as exc:
        log.exception("light snapshot failed: %s", exc)
        return EXIT_INTERNAL


def futures_symbols_for_light_snapshot(doc: CandidateUniverseDocument) -> list[str]:
    """Canonical Step 1.5 symbol list (same filter as step15_symbols)."""
    return futures_symbols_for_step_1_5(doc)
