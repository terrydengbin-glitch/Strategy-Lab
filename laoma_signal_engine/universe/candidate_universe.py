"""Merge spot/futures/manual into CANDIDATE_UNIVERSE (T08)."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import orjson
import yaml

from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.core.config_loader import EngineConfig, package_root
from laoma_signal_engine.core.exit_codes import (
    EXIT_BINANCE,
    EXIT_CONFIG,
    EXIT_INTERNAL,
    EXIT_SUCCESS,
)
from laoma_signal_engine.core.models import (
    CandidateUniverseDocument,
    ManualWatchlistEntry,
    SymbolRiskProfileBlock,
    UniverseCounts,
    UniversePairRow,
    UniverseProfileBlock,
)
from laoma_signal_engine.core.time_utils import add_ttl_seconds, to_iso_z, utc_now
from laoma_signal_engine.market import binance_futures_client, binance_spot_client
from laoma_signal_engine.universe.cache import universe_cache_is_fresh
from laoma_signal_engine.universe.cache import (
    PROFILE_SCHEMA_VERSION,
    universe_profile_contract_status,
)
from laoma_signal_engine.universe.display_mapping import (
    cashtag_from_display,
    contract_multiplier_from_internal,
    display_base_asset_from_internal,
    spot_cashtag_symbol_from_display,
)
from laoma_signal_engine.universe.futures_universe import futures_usdt_perp_trading_by_base
from laoma_signal_engine.universe.manual_watchlist import load_manual_bases, load_manual_entries
from laoma_signal_engine.universe.spot_universe import spot_usdt_trading_by_base

log = logging.getLogger(__name__)

_TAG_ORDER = (
    "manual_watchlist",
    "spot_universe",
    "futures_universe",
    "futures_top_volume",
    "futures_top_gainer",
    "futures_top_loser",
)

_NO_TRADE_MANUAL_MODES = {"blacklist", "no_trade", "exclude"}


def _volatility_tier(price_change: float | None) -> str:
    if price_change is None:
        return "unknown"
    ap = abs(price_change)
    if ap >= 35:
        return "extreme"
    if ap >= 15:
        return "high"
    if ap >= 5:
        return "normal"
    return "quiet"


def _business_contract_for(
    *,
    tier: str,
    scan_tier: str,
    liquidity_tier: str,
    execution_tier: str,
    tags: list[str],
    mode: str,
) -> tuple[str, str, str, str, str, str]:
    """Static Step1 business contract consumed by scanner/router/trade-plan.

    This is deliberately based only on listing/manual/24h-static metadata.
    Live market-entry fitness is owned by Step1.5.
    """
    if execution_tier == "no_trade" or mode in _NO_TRADE_MANUAL_MODES:
        return ("no_trade", "block", "disabled", "disabled", "disabled", "suppress")
    if execution_tier == "watch_only" or mode == "watch_only":
        return ("watch_only", "observe_only", "wide", "watch", "micro", "audit_only")
    if mode in {"force_scan", "force_micro"}:
        return ("manual_priority", "scan", "normal", "standard", "normal", "send")
    if "multiplier_contract" in tags:
        return ("multiplier_high_risk", "scan", "wide", "high_rr", "reduced", "audit_only")
    if liquidity_tier in {"A"} or tier == "tier_A_core":
        return ("liquid_major", "scan", "normal", "standard", "normal", "send")
    if liquidity_tier in {"B"}:
        return ("active_alt", "scan", "normal", "standard", "normal", "send")
    if liquidity_tier in {"C"} or scan_tier == "emerging_watch":
        return ("emerging_watch", "scan", "wide", "high_rr", "reduced", "audit_only")
    return ("risk_watch", "observe_only", "wide", "high_rr", "micro", "audit_only")


def _profile_blocks(
    *,
    base: str,
    display: str,
    cashtag: str,
    fut_sym: str | None,
    has_fut: bool,
    has_spot: bool,
    qv_f: float | None,
    pchg: float | None,
    manual_entry: ManualWatchlistEntry | None,
) -> tuple[UniverseProfileBlock, SymbolRiskProfileBlock]:
    multiplier = contract_multiplier_from_internal(base)
    mode = (manual_entry.mode.strip().lower() if manual_entry else "")
    manual_priority = int(manual_entry.priority) if manual_entry else 0
    manual_reason = manual_entry.reason if manual_entry else ""

    tags: list[str] = []
    if multiplier > 1:
        tags.append("multiplier_contract")
    if manual_entry:
        tags.append("manual_watchlist")
    if has_fut and not has_spot:
        tags.append("futures_only")
    if has_spot and not has_fut:
        tags.append("spot_only")
    if pchg is not None and abs(pchg) >= 30:
        tags.append("high_24h_move")

    qv = float(qv_f or 0.0)
    if not has_fut or mode in _NO_TRADE_MANUAL_MODES:
        tier = "tier_X_excluded"
        score = 0
        scan_tier = "ignore"
        liquidity_tier = "none"
        execution_tier = "no_trade"
    elif mode == "watch_only":
        tier = "tier_C_watch_only"
        score = max(35, manual_priority)
        scan_tier = "manual_watch"
        liquidity_tier = "manual"
        execution_tier = "watch_only"
    elif qv >= 500_000_000:
        tier = "tier_A_core"
        score = 95
        scan_tier = "core_liquid"
        liquidity_tier = "A"
        execution_tier = "market_ok"
    elif qv >= 50_000_000:
        tier = "tier_B_active_alt"
        score = 80
        scan_tier = "active_mover"
        liquidity_tier = "B"
        execution_tier = "market_ok"
    elif qv >= 5_000_000:
        tier = "tier_C_watch_only"
        score = 55
        scan_tier = "emerging_watch"
        liquidity_tier = "C"
        execution_tier = "market_careful"
    else:
        tier = "tier_D_high_risk"
        score = 30
        scan_tier = "risk_watch"
        liquidity_tier = "D"
        execution_tier = "watch_only"

    if mode in {"force_scan", "force_micro"}:
        score = max(score, manual_priority or 75)
        scan_tier = mode
        tags.append(mode)

    business_pool, scan_eligibility, sl_template, rr_template, sizing_template, feishu_policy = _business_contract_for(
        tier=tier,
        scan_tier=scan_tier,
        liquidity_tier=liquidity_tier,
        execution_tier=execution_tier,
        tags=tags,
        mode=mode,
    )
    risk = SymbolRiskProfileBlock(
        liquidity_tier=liquidity_tier,
        volatility_tier=_volatility_tier(pchg),
        execution_tier=execution_tier,
        rr_policy="conservative" if "multiplier_contract" in tags or liquidity_tier in {"C", "D"} else "normal",
        sl_template=sl_template,
        rr_template=rr_template,
        sizing_template=sizing_template,
        feishu_policy=feishu_policy,
        min_stop_bps=25.0 if execution_tier == "market_ok" else 35.0,
        min_target_bps=50.0 if execution_tier == "market_ok" else 70.0,
        max_chase_bps=80.0 if execution_tier == "market_ok" else 45.0,
    )
    profile = UniverseProfileBlock(
        universe_tier=tier,
        universe_priority_score=min(100, max(0, score)),
        scan_tier=scan_tier,
        business_pool=business_pool,
        scan_eligibility=scan_eligibility,
        symbol_risk_tags=sorted(set(tags)),
        trade_symbol=fut_sym.upper() if fut_sym else None,
        display_asset=display,
        social_cashtag=cashtag,
        contract_multiplier=multiplier,
        is_multiplier_contract=multiplier > 1,
        manual_mode=mode,
        manual_priority=manual_priority,
        manual_reason=manual_reason,
    )
    return profile, risk


def _tag_sort_key(tag: str) -> int:
    try:
        return _TAG_ORDER.index(tag)
    except ValueError:
        return len(_TAG_ORDER)


def load_symbols_exclude() -> tuple[set[str], set[str]]:
    path = package_root() / "config" / "symbols_exclude.yaml"
    raw = path.read_text(encoding="utf-8")
    doc: dict[str, Any] = yaml.safe_load(raw) or {}
    spot = {str(x).upper() for x in (doc.get("spot") or []) if x is not None}
    fut = {str(x).upper() for x in (doc.get("futures") or []) if x is not None}
    return spot, fut


def futures_ticker_quote_and_change(rows: list[dict[str, Any]]) -> dict[str, tuple[float, float]]:
    """Map futures symbol -> (quote_volume_24h, price_change_percent)."""
    out: dict[str, tuple[float, float]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        sym = r.get("symbol")
        if not isinstance(sym, str):
            continue
        sym_u = sym.upper()
        qv = r.get("quoteVolume")
        pc = r.get("priceChangePercent")
        try:
            qf = float(qv) if qv is not None else 0.0
            pf = float(pc) if pc is not None else 0.0
        except (TypeError, ValueError):
            continue
        out[sym_u] = (qf, pf)
    return out


def _compute_counts(rows: list[UniversePairRow]) -> UniverseCounts:
    total = len(rows)
    futures_count = sum(1 for r in rows if r.has_um_futures)
    spot_count = sum(1 for r in rows if r.has_spot)
    both = sum(1 for r in rows if r.has_spot and r.has_um_futures)
    futures_only = sum(1 for r in rows if r.has_um_futures and not r.has_spot)
    spot_only = sum(1 for r in rows if r.has_spot and not r.has_um_futures)
    neither = sum(1 for r in rows if not r.has_spot and not r.has_um_futures)
    return UniverseCounts(
        total_pairs=total,
        futures_count=futures_count,
        spot_count=spot_count,
        both_spot_and_futures=both,
        futures_only=futures_only,
        spot_only=spot_only,
        neither_spot_nor_futures=neither,
    )


def build_candidate_document_from_maps(
    *,
    spot_by_base: dict[str, str],
    futures_by_base: dict[str, str],
    fut_ticker_by_symbol: dict[str, tuple[float, float]],
    manual_bases: set[str],
    exclude_spot: set[str],
    exclude_futures: set[str],
    schema_version: str,
    source: str,
    ttl_seconds: int,
    top_tag_rank: int,
    manual_entries: dict[str, ManualWatchlistEntry] | None = None,
    generated_at: datetime | None = None,
) -> CandidateUniverseDocument:
    gen = generated_at or utc_now()
    expires = add_ttl_seconds(gen, ttl_seconds)

    bases: set[str] = set()
    bases |= set(spot_by_base.keys())
    bases |= set(futures_by_base.keys())
    bases |= set(manual_bases)
    if manual_entries:
        bases |= set(manual_entries.keys())

    rows: list[UniversePairRow] = []
    for base in sorted(bases):
        spot_sym = spot_by_base.get(base)
        if spot_sym and spot_sym.upper() in exclude_spot:
            spot_sym = None
        fut_sym = futures_by_base.get(base)
        if fut_sym and fut_sym.upper() in exclude_futures:
            fut_sym = None

        has_spot = bool(spot_sym)
        has_fut = bool(fut_sym)

        qv_f: float | None = None
        pchg: float | None = None
        if fut_sym:
            tup = fut_ticker_by_symbol.get(fut_sym.upper())
            if tup is not None:
                qv_f, pchg = tup
                if qv_f <= 0:
                    qv_f = None

        eligible_ta = has_fut
        eligible_post = has_fut and has_spot
        eligible_se = has_fut and eligible_ta

        display = display_base_asset_from_internal(base)
        cashtag = cashtag_from_display(display)
        spot_cashtag = spot_cashtag_symbol_from_display(display)
        manual_entry = (manual_entries or {}).get(base)
        if has_fut and fut_sym:
            sym_safe: str | None = fut_sym.upper()
        elif has_spot and spot_sym:
            sym_safe = spot_sym.upper()
        else:
            sym_safe = None

        universe_profile, risk_profile = _profile_blocks(
            base=base,
            display=display,
            cashtag=cashtag,
            fut_sym=fut_sym,
            has_fut=has_fut,
            has_spot=has_spot,
            qv_f=qv_f,
            pchg=pchg,
            manual_entry=manual_entry,
        )
        if universe_profile.manual_mode in _NO_TRADE_MANUAL_MODES:
            eligible_ta = False
            eligible_post = False
            eligible_se = False

        rows.append(
            UniversePairRow(
                base_asset=base,
                display_base_asset=display,
                cashtag=cashtag,
                spot_cashtag_symbol=spot_cashtag,
                symbol_safe_id=sym_safe,
                spot_symbol=spot_sym,
                futures_symbol=fut_sym,
                has_spot=has_spot,
                has_um_futures=has_fut,
                eligible_for_signal_engine=eligible_se,
                eligible_for_post=eligible_post,
                eligible_for_trade_analysis=eligible_ta,
                quote_volume_24h_futures=qv_f,
                price_change_24h_futures=pchg if has_fut else None,
                source_tags=[],
                universe_profile=universe_profile,
                risk_profile=risk_profile,
            )
        )

    fut_indices = [i for i, r in enumerate(rows) if r.has_um_futures and r.quote_volume_24h_futures]
    fut_indices.sort(
        key=lambda i: (-(rows[i].quote_volume_24h_futures or 0.0), rows[i].base_asset)
    )
    for rank, idx in enumerate(fut_indices, start=1):
        rows[idx] = rows[idx].model_copy(update={"rank_futures_volume": rank})

    pos_idx = [
        i
        for i, r in enumerate(rows)
        if r.has_um_futures
        and r.price_change_24h_futures is not None
        and r.price_change_24h_futures > 0
    ]
    pos_idx.sort(
        key=lambda i: (-(rows[i].price_change_24h_futures or 0.0), rows[i].base_asset)
    )
    for rank, idx in enumerate(pos_idx, start=1):
        rows[idx] = rows[idx].model_copy(update={"rank_futures_gainer": rank})

    neg_idx = [
        i
        for i, r in enumerate(rows)
        if r.has_um_futures
        and r.price_change_24h_futures is not None
        and r.price_change_24h_futures < 0
    ]
    neg_idx.sort(key=lambda i: (rows[i].price_change_24h_futures or 0.0, rows[i].base_asset))
    for rank, idx in enumerate(neg_idx, start=1):
        rows[idx] = rows[idx].model_copy(update={"rank_futures_loser": rank})

    for i, r in enumerate(rows):
        tags: list[str] = []
        if r.base_asset in manual_bases or (manual_entries and r.base_asset in manual_entries):
            tags.append("manual_watchlist")
        if r.has_spot:
            tags.append("spot_universe")
        if r.has_um_futures:
            tags.append("futures_universe")
        rv = rows[i].rank_futures_volume
        if rv is not None and rv <= top_tag_rank:
            tags.append("futures_top_volume")
        rg = rows[i].rank_futures_gainer
        if rg is not None and rg <= top_tag_rank:
            tags.append("futures_top_gainer")
        rl = rows[i].rank_futures_loser
        if rl is not None and rl <= top_tag_rank:
            tags.append("futures_top_loser")
        tags = sorted(set(tags), key=_tag_sort_key)
        rows[i] = r.model_copy(update={"source_tags": tags})

    rows.sort(key=lambda r: (-(r.quote_volume_24h_futures or 0.0), r.base_asset))

    counts = _compute_counts(rows)
    profile_payload = {
        "pairs": [row.model_dump(mode="json") for row in rows],
    }
    profile_status = universe_profile_contract_status(profile_payload)

    return CandidateUniverseDocument(
        schema_version=schema_version,
        generated_at=to_iso_z(gen),
        expires_at=to_iso_z(expires),
        ttl_seconds=ttl_seconds,
        status="fresh",
        source=source,
        profile_schema_version=PROFILE_SCHEMA_VERSION,
        profile_hydration_source="step1_candidate_universe",
        profile_hydration_status=str(profile_status.get("status") or "incomplete"),
        profile_hydration_reason_codes=list(profile_status.get("reason_codes") or []),
        profile_hydration_counts=dict(profile_status.get("counts") or {}),
        count=len(rows),
        counts=counts,
        pairs=rows,
    )


def write_candidate_universe(doc: CandidateUniverseDocument, path: Path) -> None:
    payload = doc.model_dump(mode="json")
    data = orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
    write_file_atomic(path, data)


def run_build_universe(*, force: bool, project_root: Path | None = None) -> int:
    cfg = EngineConfig.load(project_root)
    now = utc_now()

    if not force and universe_cache_is_fresh(cfg.candidate_universe_path, cfg.schema_version, now):
        log.info(
            "Universe cache is fresh, skip rebuild (use --force). path=%s",
            cfg.candidate_universe_path,
        )
        return EXIT_SUCCESS

    exclude_spot, exclude_futures = load_symbols_exclude()
    manual_entries = load_manual_entries(cfg.manual_watchlist_path)
    manual = set(manual_entries.keys()) or load_manual_bases(cfg.manual_watchlist_path)

    try:
        with httpx.Client(timeout=60.0) as client:
            spot_info = binance_spot_client.fetch_exchange_info(client)
            fut_info = binance_futures_client.fetch_exchange_info(client)
            fut_tick = binance_futures_client.fetch_ticker_24h_all(client)
    except (httpx.HTTPError, TypeError, ValueError) as exc:
        log.error("Binance request failed: %s", exc)
        return EXIT_BINANCE

    spot_by_base = spot_usdt_trading_by_base(spot_info)
    fut_by_base = futures_usdt_perp_trading_by_base(fut_info)
    fut_ticker_map = futures_ticker_quote_and_change(fut_tick)

    doc = build_candidate_document_from_maps(
        spot_by_base=spot_by_base,
        futures_by_base=fut_by_base,
        fut_ticker_by_symbol=fut_ticker_map,
        manual_bases=manual,
        manual_entries=manual_entries,
        exclude_spot=exclude_spot,
        exclude_futures=exclude_futures,
        schema_version=cfg.schema_version,
        source=cfg.source,
        ttl_seconds=cfg.universe_ttl_seconds,
        top_tag_rank=cfg.universe_top_tag_rank,
        generated_at=now,
    )

    try:
        write_candidate_universe(doc, cfg.candidate_universe_path)
    except OSError as exc:
        log.error("failed to write universe file: %s", exc)
        return EXIT_CONFIG

    log.info(
        "Wrote %s total_pairs=%s futures_count=%s",
        cfg.candidate_universe_path,
        doc.count,
        doc.counts.futures_count,
    )
    return EXIT_SUCCESS


def run_build_universe_safe(*, force: bool, project_root: Path | None = None) -> int:
    """Catch-all wrapper for CLI."""
    try:
        return run_build_universe(force=force, project_root=project_root)
    except Exception as exc:
        log.exception("internal error during build-universe: %s", exc)
        return EXIT_INTERNAL
