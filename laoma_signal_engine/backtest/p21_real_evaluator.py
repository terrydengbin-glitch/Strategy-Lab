"""Offline real-evaluator adapter for P21 config matrix backtests.

The adapter converts cached 1m klines into minimal historical factor /
refresh / liquidity documents, then calls the real trade-plan evaluator.
It deliberately does not write live latest JSON files or wake paper.
"""

from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from laoma_signal_engine.decision.trade_plan_lines import (
    DEFAULT_CONFIGS,
    TradePlanLineConfig,
    build_trade_plan_line_document,
)
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanItem
from laoma_signal_engine.factors.models import (
    FactorQualityBlock,
    FactorSnapshotDocument,
    FactorSnapshotItem,
)
from laoma_signal_engine.market.decision_refresh_models import DecisionRefreshDocument, DecisionRefreshItem
from laoma_signal_engine.market.market_entry_liquidity_models import (
    MarketEntryLiquidityDocument,
    MarketEntryLiquidityItem,
)
from laoma_signal_engine.micro.assembly.models import CoverageSummaryBlock, Micro15mBlock, MicroQualityBlock
from laoma_signal_engine.strategy5.evidence import build_evidence_vector, score_hypothesis
from laoma_signal_engine.strategy6.evidence import build_feature_vector as build_strategy6_feature_vector
from laoma_signal_engine.strategy6.evidence import score_strategy6 as score_strategy6_market_acceptance


ENGINE_MODE = "offline_real_evaluator"
SOURCE_CONTRACT_VERSION = "21.15"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
        if out != out or out in (float("inf"), float("-inf")):
            return default
        return out
    except Exception:
        return default


def _iso_from_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _side_to_move(side: str) -> str:
    return "up" if str(side).upper() == "LONG" else "down" if str(side).upper() == "SHORT" else "neutral"


def _base_asset(symbol: str) -> str:
    return symbol.upper().removesuffix("USDT")


def _config_from_params(strategy_line: str, params: dict[str, Any]) -> TradePlanLineConfig:
    base_line = "without_micro" if strategy_line in {"strategy4", "strategy5", "strategy6"} else strategy_line
    base = DEFAULT_CONFIGS.get(base_line, DEFAULT_CONFIGS["without_micro"])
    allowed = set(asdict(base).keys())
    patch: dict[str, Any] = {}
    flat_map = {
        "min_score": int,
        "target_rr": float,
        "min_rr": float,
        "min_net_rr": float,
        "min_effective_rr": float,
        "stop_atr_mult": float,
        "max_stop_atr_mult": float,
        "min_stop_bps": float,
        "preferred_stop_bps": float,
        "max_stop_bps": float,
        "min_reachable_reward_bps": float,
        "min_tp_after_cost_bps": float,
        "taker_fee_bps": float,
        "maker_fee_bps": float,
    }
    for key, caster in flat_map.items():
        if key in params and key in allowed and params.get(key) is not None:
            patch[key] = caster(params[key])
    policy = params.get("tp_target_policy") if isinstance(params.get("tp_target_policy"), dict) else {}
    policy_map = {
        "mode": ("tp_target_policy_mode", str),
        "target_rr": ("tp_target_policy_target_rr", float),
        "target_rr_cap": ("tp_target_policy_target_rr_cap", float),
        "target_rr_basis": ("tp_target_policy_target_rr_basis", str),
        "target_net_rr": ("tp_target_policy_target_net_rr", float),
        "min_target_net_rr": ("tp_target_policy_min_target_net_rr", float),
        "max_target_net_rr": ("tp_target_policy_max_target_net_rr", float),
        "min_reward_bps": ("tp_target_policy_min_reward_bps", float),
        "require_market_room": ("tp_target_policy_require_market_room", bool),
        "market_room_buffer_bps": ("tp_target_policy_market_room_buffer_bps", float),
        "allow_structure_runner": ("tp_target_policy_allow_structure_runner", bool),
        "reward_to_spread_min": ("tp_target_policy_reward_to_spread_min", float),
        "max_loss_net_r": ("tp_target_policy_max_loss_net_r", float),
        "sizing_basis": ("tp_target_policy_sizing_basis", str),
    }
    for src, (dst, caster) in policy_map.items():
        if src in policy and dst in allowed and policy.get(src) is not None:
            patch[dst] = caster(policy[src])
    return replace(base, **patch)


def _quality_block(reason: str = "offline_without_micro") -> MicroQualityBlock:
    coverage = {
        "trade": CoverageSummaryBlock(stream_type="trade", window_sec=0, expected_seconds=0, covered_seconds=0),
        "book": CoverageSummaryBlock(stream_type="book", window_sec=0, expected_seconds=0, covered_seconds=0),
        "depth": CoverageSummaryBlock(stream_type="depth", window_sec=0, expected_seconds=0, covered_seconds=0),
    }
    return MicroQualityBlock(
        ready=False,
        reason_codes=[reason],
        reference_ts_sec=0,
        collect_started_ts_sec=0,
        warmup_age_sec=0,
        cvd_update_age_sec=None,
        ofi_update_age_sec=None,
        last_update_age_sec=None,
        max_lag_sec=None,
        coverage=coverage,
        driver_metrics_summary={},
    )


def _factor_doc(signal: Any) -> FactorSnapshotDocument:
    features = dict(getattr(signal, "features", {}) or {})
    close = _num(features.get("close"), 1.0)
    atr_bps = max(1.0, _num(features.get("atr_1m_bps"), 10.0))
    atr_price = close * atr_bps / 10000.0
    side = str(getattr(signal, "side", "")).upper()
    move_side = _side_to_move(side)
    score = int(round(_num(getattr(signal, "score", 0), 0)))
    primary = {
        "price_ret": _num(features.get("pct_15m_bps")) / 100.0,
        "volume_ratio": max(0.1, _num(features.get("volume_z"), 1.0)),
        "range_pos": _num(features.get("range_pos_30m"), 0.5),
        "atr": atr_price,
        "kline_cvd_state": "buy_dominant" if side == "LONG" else "sell_dominant",
    }
    trigger = {
        "price_ret": _num(features.get("pct_5m_bps")) / 100.0,
        "trigger_state": "impulse",
    }
    entry = {
        "price_ret": _num(features.get("pct_1m_bps")) / 100.0,
        "price_ret_3m": _num(features.get("pct_3m_bps")),
        "pct_3m_bps": _num(features.get("pct_3m_bps")),
        "atr": atr_price,
        "last_price": close,
        "distance_to_vwap_bps": _num(features.get("distance_to_vwap_bps"), 0.0),
        "distance_to_ema_bps": _num(features.get("distance_to_ema_bps"), 0.0),
    }
    item = FactorSnapshotItem(
        symbol=str(getattr(signal, "symbol", "")).upper(),
        base_asset=_base_asset(str(getattr(signal, "symbol", ""))),
        source_state="historical_kline",
        move_side=move_side,
        scan_score=score,
        market_entry_suitability_score=score,
        market_entry_suitability="preferred" if score >= 75 else "allowed",
        trigger_type="historical_impulse",
        primary_15m=primary,
        trigger_5m=trigger,
        entry_1m=entry,
        background={"source": "p21_30d_kline_cache"},
        micro_15m=Micro15mBlock(ready=False),
        micro_quality=_quality_block(),
        factor_quality=FactorQualityBlock(ready=True, reason_codes=[]),
    )
    return FactorSnapshotDocument(
        generated_at=_iso_from_ms(int(getattr(signal, "signal_time_ms", 0))),
        source="factor_snapshot_without_ofi_cvd",
        status="ok",
        count=1,
        input_refs={"lineage_mode": ENGINE_MODE, "source_contract_version": SOURCE_CONTRACT_VERSION},
        items=[item],
    )


def _refresh_doc(signal: Any, params: dict[str, Any]) -> DecisionRefreshDocument:
    features = dict(getattr(signal, "features", {}) or {})
    symbol = str(getattr(signal, "symbol", "")).upper()
    close = _num(features.get("close"), 1.0)
    side = str(getattr(signal, "side", "")).upper()
    range_pos = _num(features.get("range_pos_30m"), 0.5)
    room_cfg = params.get("range_room") if isinstance(params.get("range_room"), dict) else {}
    long_max = _num(room_cfg.get("long_max_range_pos"), 0.82)
    short_min = _num(room_cfg.get("short_min_range_pos"), 0.18)
    range_room_ok = range_pos <= long_max if side == "LONG" else range_pos >= short_min if side == "SHORT" else False
    reason_codes = [] if range_room_ok else ["range_room_insufficient_after_refresh"]
    item = DecisionRefreshItem(
        symbol=symbol,
        base_asset=_base_asset(symbol),
        move_side=_side_to_move(side),
        source_state="historical_kline",
        last_price=close,
        refresh_age_sec=0,
        direction_still_valid=True,
        range_room_ok=range_room_ok,
        range_gate={
            "source": "p21_offline_kline_range_room",
            "range_pos": range_pos,
            "long_max_range_pos": long_max,
            "short_min_range_pos": short_min,
            "ok": range_room_ok,
        },
        liquidity_ok=True,
        liquidity_age_sec=0,
        reason_codes=reason_codes,
        primary_15m={
            "price_ret": _num(features.get("pct_15m_bps")) / 100.0,
            "range_pos": range_pos,
            "atr": close * max(1.0, _num(features.get("atr_1m_bps"), 10.0)) / 10000.0,
        },
        trigger_5m={"price_ret": _num(features.get("pct_5m_bps")) / 100.0},
        entry_1m={"price_ret": _num(features.get("pct_1m_bps")) / 100.0, "atr": close * max(1.0, _num(features.get("atr_1m_bps"), 10.0)) / 10000.0},
        data_quality={"offline_source": "p21_klines_1m"},
        liquidity={"source": "offline_estimated"},
    )
    return DecisionRefreshDocument(
        generated_at=_iso_from_ms(int(getattr(signal, "signal_time_ms", 0))),
        line=str(getattr(signal, "strategy_line", "")),
        status="ok",
        input_refs={"lineage_mode": ENGINE_MODE, "range_room_source": "matrix_params"},
        max_refresh_age_sec=999999,
        max_liquidity_age_sec=999999,
        long_max_range_pos=long_max,
        short_min_range_pos=short_min,
        candidate_count=1,
        refreshed_count=1,
        stale_count=0,
        items=[item],
    )


def _liquidity_doc(signal: Any, params: dict[str, Any]) -> MarketEntryLiquidityDocument:
    features = dict(getattr(signal, "features", {}) or {})
    symbol = str(getattr(signal, "symbol", "")).upper()
    close = _num(features.get("close"), 1.0)
    spread_bps = _num(params.get("offline_spread_bps"), _num(params.get("slippage_bps"), 2.0))
    slippage_bps = _num(params.get("slippage_bps"), 2.0)
    item = MarketEntryLiquidityItem(
        symbol=symbol,
        last_price=close,
        bid_price=close * (1.0 - spread_bps / 20000.0),
        ask_price=close * (1.0 + spread_bps / 20000.0),
        spread_bps=spread_bps,
        top_bid_depth_usdt=1_000_000.0,
        top_ask_depth_usdt=1_000_000.0,
        estimated_market_buy_slippage_bps=slippage_bps,
        estimated_market_sell_slippage_bps=slippage_bps,
        liquidity_ok_for_market_entry=True,
        buy_liquidity_ok_for_market_entry=True,
        sell_liquidity_ok_for_market_entry=True,
        notional_usdt=2_000.0,
        max_spread_bps=max(8.0, spread_bps),
        max_estimated_slippage_bps=max(15.0, slippage_bps),
        min_top_depth_usdt=20_000.0,
        min_quote_volume_24h=3_000_000.0,
        reason_codes=["offline_liquidity_estimated"],
    )
    return MarketEntryLiquidityDocument(
        generated_at=_iso_from_ms(int(getattr(signal, "signal_time_ms", 0))),
        status="ok",
        count=1,
        max_spread_bps=max(8.0, spread_bps),
        max_estimated_slippage_bps=max(15.0, slippage_bps),
        min_top_depth_usdt=20_000.0,
        min_quote_volume_24h=3_000_000.0,
        items=[item],
    )


def _strategy5_plan(plan: TradePlanItem, factor_doc: FactorSnapshotDocument, run_id: str) -> TradePlanItem:
    factor = factor_doc.items[0].model_dump(mode="json")
    ev = build_evidence_vector(factor)
    hyp = score_hypothesis(ev)
    legacy_side = str(plan.decision or "NO_TRADE").upper()
    agrees = legacy_side in {"LONG", "SHORT"} and hyp["shadow_hypothesis_side"] == legacy_side
    allow = (
        bool(plan.executable)
        and agrees
        and hyp["shadow_recommendation"] == "allow_if_legacy_agrees"
        and ev.get("evidence_quality", {}).get("usable") is True
    )
    reasons = list(dict.fromkeys([*list(plan.reason_codes or []), *hyp["reason_codes"]]))
    update: dict[str, Any] = {}
    if not allow:
        if plan.executable:
            reasons.append("strategy5_shadow_blocked_not_promoted")
        update.update(
            {
                "executable": False,
                "action": "WAIT" if legacy_side in {"LONG", "SHORT"} else "NO_TRADE",
                "entry_mode": "WAIT_CONFIRMATION" if legacy_side in {"LONG", "SHORT"} else "NONE",
                "estimated_entry_price": None,
                "stop_loss": None,
                "take_profit": None,
                "risk_per_unit": None,
                "reward_per_unit": None,
                "rr": None,
                "position_sizing": None,
            }
        )
    guards = dict(plan.guards or {})
    guards.update(
        {
            "line": "strategy5",
            "strategy5_evidence_id": f"{run_id}:{plan.symbol}:strategy5",
            "strategy5_shadow_hypothesis_side": hyp["shadow_hypothesis_side"],
            "strategy5_shadow_label": hyp["shadow_label"],
            "strategy5_shadow_recommendation": hyp["shadow_recommendation"],
            "strategy5_continuation_score": hyp["continuation_score"],
            "strategy5_exhaustion_score": hyp["exhaustion_score"],
            "strategy5_legacy_side": legacy_side,
            "strategy5_legacy_agrees": agrees,
            "strategy5_adapter_mode": ENGINE_MODE,
        }
    )
    refs = dict(plan.input_refs or {})
    refs.update({"strategy5_source": "offline_strategy5_evidence", "strategy5_evidence": {**ev, **hyp}})
    update.update({"reason_codes": list(dict.fromkeys(reasons)), "guards": guards, "input_refs": refs})
    return plan.model_copy(update=update)


def _strategy6_cfg(params: dict[str, Any]) -> dict[str, Any]:
    block = params.get("strategy6") if isinstance(params.get("strategy6"), dict) else {}
    def _block_or_param(key: str, default: Any) -> Any:
        return block.get(key, params.get(key, default))

    return {
        "strategy6_version": str(_block_or_param("strategy6_version", "v1")),
        "min_direction_acceptance_score": int(_block_or_param("min_direction_acceptance_score", 58)),
        "min_entry_price_quality_score": int(_block_or_param("min_entry_price_quality_score", 56)),
        "min_market_acceptance_score": int(_block_or_param("min_market_acceptance_score", 58)),
        "hard_deny_direction_score": int(_block_or_param("hard_deny_direction_score", 38)),
        "v2_min_direction_acceptance_score": int(_block_or_param("v2_min_direction_acceptance_score", _block_or_param("min_direction_acceptance_score", 62))),
        "v2_uncertain_direction_score": int(_block_or_param("v2_uncertain_direction_score", 48)),
        "v2_hard_deny_direction_score": int(_block_or_param("v2_hard_deny_direction_score", _block_or_param("hard_deny_direction_score", 38))),
        "long_max_range_pos": float(_block_or_param("long_max_range_pos", 0.78)),
        "short_min_range_pos": float(_block_or_param("short_min_range_pos", 0.22)),
        "max_spread_bps": float(_block_or_param("max_spread_bps", 45.0)),
        "max_abs_1m_chase_bps": float(_block_or_param("max_abs_1m_chase_bps", 80.0)),
        "v2_max_chase_bps": float(_block_or_param("v2_max_chase_bps", _block_or_param("max_abs_1m_chase_bps", 55.0))),
        "v2_adverse_1m_deny_bps": float(_block_or_param("v2_adverse_1m_deny_bps", 24.0)),
        "v2_reversal_1m_wait_bps": float(_block_or_param("v2_reversal_1m_wait_bps", 10.0)),
        "v2_distance_from_mean_max_bps": float(_block_or_param("v2_distance_from_mean_max_bps", 85.0)),
        "v2_high_quality_score": int(_block_or_param("v2_high_quality_score", 74)),
        "v2_medium_quality_score": int(_block_or_param("v2_medium_quality_score", 62)),
        "v3_min_direction_context_score": int(_block_or_param("v3_min_direction_context_score", _block_or_param("v2_min_direction_acceptance_score", 62))),
        "v3_uncertain_direction_context_score": int(_block_or_param("v3_uncertain_direction_context_score", 52)),
        "v3_hard_deny_context_score": int(_block_or_param("v3_hard_deny_context_score", _block_or_param("v2_hard_deny_direction_score", 38))),
        "v3_reverse_1m_deny_bps": float(_block_or_param("v3_reverse_1m_deny_bps", 12.0)),
        "v3_reverse_3m_deny_bps": float(_block_or_param("v3_reverse_3m_deny_bps", 24.0)),
        "v3_fake_breakout_range_pos": float(_block_or_param("v3_fake_breakout_range_pos", 0.88)),
        "v3_second_acceptance_min_bps": float(_block_or_param("v3_second_acceptance_min_bps", 4.0)),
        "v3_max_entry_slippage_bps": float(_block_or_param("v3_max_entry_slippage_bps", 45.0)),
        "v3_quality_filter_mode": str(_block_or_param("v3_quality_filter_mode", "shadow")),
        "v3_bad_symbols": list(_block_or_param("v3_bad_symbols", [])) if isinstance(_block_or_param("v3_bad_symbols", []), list) else [],
        "v3_bad_sides": list(_block_or_param("v3_bad_sides", [])) if isinstance(_block_or_param("v3_bad_sides", []), list) else [],
        "v3_1_min_direction_context_score": int(_block_or_param("v3_1_min_direction_context_score", _block_or_param("v3_min_direction_context_score", _block_or_param("v2_min_direction_acceptance_score", 62)))),
        "v3_1_uncertain_direction_context_score": int(_block_or_param("v3_1_uncertain_direction_context_score", _block_or_param("v3_uncertain_direction_context_score", 52))),
        "v3_1_hard_deny_context_score": int(_block_or_param("v3_1_hard_deny_context_score", _block_or_param("v3_hard_deny_context_score", _block_or_param("v2_hard_deny_direction_score", 38)))),
        "v3_1_reverse_1m_deny_bps": float(_block_or_param("v3_1_reverse_1m_deny_bps", _block_or_param("v3_reverse_1m_deny_bps", 12.0))),
        "v3_1_reverse_3m_deny_bps": float(_block_or_param("v3_1_reverse_3m_deny_bps", _block_or_param("v3_reverse_3m_deny_bps", 24.0))),
        "v3_1_low_followthrough_min_volume_z": float(_block_or_param("v3_1_low_followthrough_min_volume_z", 0.85)),
        "v3_1_low_followthrough_min_5m_bps": float(_block_or_param("v3_1_low_followthrough_min_5m_bps", 8.0)),
        "v3_1_range_extreme_pos": float(_block_or_param("v3_1_range_extreme_pos", 0.72)),
        "v3_1_btc_against_action": str(_block_or_param("v3_1_btc_against_action", "wait")),
        "v3_2_long_min_direction_context_score": int(_block_or_param("v3_2_long_min_direction_context_score", _block_or_param("v3_min_direction_context_score", 64))),
        "v3_2_short_min_direction_context_score": int(_block_or_param("v3_2_short_min_direction_context_score", _block_or_param("v3_min_direction_context_score", 58))),
        "v3_2_long_reverse_1m_deny_bps": float(_block_or_param("v3_2_long_reverse_1m_deny_bps", _block_or_param("v3_reverse_1m_deny_bps", 10.0))),
        "v3_2_short_reverse_1m_deny_bps": float(_block_or_param("v3_2_short_reverse_1m_deny_bps", _block_or_param("v3_reverse_1m_deny_bps", 16.0))),
        "v3_2_long_reverse_3m_deny_bps": float(_block_or_param("v3_2_long_reverse_3m_deny_bps", _block_or_param("v3_reverse_3m_deny_bps", 22.0))),
        "v3_2_short_reverse_3m_deny_bps": float(_block_or_param("v3_2_short_reverse_3m_deny_bps", _block_or_param("v3_reverse_3m_deny_bps", 30.0))),
        "v3_2_long_btc_against_action": str(_block_or_param("v3_2_long_btc_against_action", "wait")),
        "v3_2_short_btc_against_action": str(_block_or_param("v3_2_short_btc_against_action", "shadow")),
        "v3_2_quality_filter_mode": str(_block_or_param("v3_2_quality_filter_mode", "shadow")),
        "v3_2_bad_symbols": list(_block_or_param("v3_2_bad_symbols", [])) if isinstance(_block_or_param("v3_2_bad_symbols", []), list) else [],
        "v3_2_bad_sides": list(_block_or_param("v3_2_bad_sides", [])) if isinstance(_block_or_param("v3_2_bad_sides", []), list) else [],
        "v3_2_bad_hours": list(_block_or_param("v3_2_bad_hours", [])) if isinstance(_block_or_param("v3_2_bad_hours", []), list) else [],
        "v3_3_long_min_direction_context_score": int(_block_or_param("v3_3_long_min_direction_context_score", _block_or_param("v3_2_long_min_direction_context_score", 66))),
        "v3_3_short_min_direction_context_score": int(_block_or_param("v3_3_short_min_direction_context_score", _block_or_param("v3_2_short_min_direction_context_score", 58))),
        "v3_3_adverse_1m_wait_bps": float(_block_or_param("v3_3_adverse_1m_wait_bps", 6.0)),
        "v3_3_adverse_3m_deny_bps": float(_block_or_param("v3_3_adverse_3m_deny_bps", 18.0)),
        "v3_3_weak_followthrough_wait_bps": float(_block_or_param("v3_3_weak_followthrough_wait_bps", 4.0)),
        "v3_3_min_volume_z": float(_block_or_param("v3_3_min_volume_z", 0.6)),
        "v3_3_early_abort_enabled": bool(_block_or_param("v3_3_early_abort_enabled", True)),
        "v3_3_abort_if_mfe_lt_R": float(_block_or_param("v3_3_abort_if_mfe_lt_R", 0.10)),
        "v3_3_abort_if_mae_gt_R": float(_block_or_param("v3_3_abort_if_mae_gt_R", 0.45)),
        "v3_3_abort_window_min": int(_block_or_param("v3_3_abort_window_min", 3)),
        "v3_3_max_initial_adverse_R": float(_block_or_param("v3_3_max_initial_adverse_R", 0.75)),
        "v3_4_min_aligned_1m_bps": float(_block_or_param("v3_4_min_aligned_1m_bps", -2.0)),
        "v3_4_min_aligned_3m_bps": float(_block_or_param("v3_4_min_aligned_3m_bps", -4.0)),
        "v3_4_min_followthrough_5m_bps": float(_block_or_param("v3_4_min_followthrough_5m_bps", 6.0)),
        "v3_4_min_volume_z": float(_block_or_param("v3_4_min_volume_z", 0.8)),
        "v3_4_long_max_range_pos": float(_block_or_param("v3_4_long_max_range_pos", 0.86)),
        "v3_4_short_min_range_pos": float(_block_or_param("v3_4_short_min_range_pos", 0.14)),
        "v3_4_max_distance_to_mean_bps": float(_block_or_param("v3_4_max_distance_to_mean_bps", 72.0)),
        "v3_4_no_edge_action": str(_block_or_param("v3_4_no_edge_action", "wait")),
        "v3_4_range_noise_action": str(_block_or_param("v3_4_range_noise_action", "wait_rebound")),
        "v3_4_btc_against_action": str(_block_or_param("v3_4_btc_against_action", "wait")),
        "v3_5_no_edge_aligned_5m_bps": float(_block_or_param("v3_5_no_edge_aligned_5m_bps", 4.0)),
        "v3_5_no_edge_volume_z": float(_block_or_param("v3_5_no_edge_volume_z", 0.7)),
        "v3_5_hard_wrong_adverse_1m_bps": float(_block_or_param("v3_5_hard_wrong_adverse_1m_bps", 8.0)),
        "v3_5_hard_wrong_adverse_3m_bps": float(_block_or_param("v3_5_hard_wrong_adverse_3m_bps", 18.0)),
        "v3_5_rebound_range_long": float(_block_or_param("v3_5_rebound_range_long", 0.82)),
        "v3_5_rebound_range_short": float(_block_or_param("v3_5_rebound_range_short", 0.18)),
        "v3_5_profit_lock_min_aligned_5m_bps": float(_block_or_param("v3_5_profit_lock_min_aligned_5m_bps", 8.0)),
        "v3_6_hard_wrong_1m_bps": float(_block_or_param("v3_6_hard_wrong_1m_bps", 10.0)),
        "v3_6_hard_wrong_3m_bps": float(_block_or_param("v3_6_hard_wrong_3m_bps", 22.0)),
        "v3_6_min_followthrough_5m_bps": float(_block_or_param("v3_6_min_followthrough_5m_bps", 6.0)),
        "v3_6_min_volume_z": float(_block_or_param("v3_6_min_volume_z", 0.9)),
        "v3_6_no_edge_action": str(_block_or_param("v3_6_no_edge_action", "wait")),
        "v3_6_base_v3_4_followthrough_bps": float(_block_or_param("v3_6_base_v3_4_followthrough_bps", -999.0)),
        "v3_6_base_v3_4_min_volume_z": float(_block_or_param("v3_6_base_v3_4_min_volume_z", 0.0)),
        "market_score_direction_weight": float(_block_or_param("market_score_direction_weight", 0.58)),
        "market_score_entry_weight": float(_block_or_param("market_score_entry_weight", 0.42)),
        "market_acceptance_mode": "offline_backtest",
    }


def _strategy6_backtest_rr_cap(params: dict[str, Any]) -> float | None:
    block = params.get("strategy6") if isinstance(params.get("strategy6"), dict) else {}
    raw = block.get(
        "strategy6_backtest_max_effective_planned_rr",
        block.get(
            "max_effective_planned_rr",
            params.get("strategy6_backtest_max_effective_planned_rr", params.get("max_effective_planned_rr")),
        ),
    )
    if raw in (None, "", False):
        return None
    cap = _num(raw, 0.0)
    return cap if cap > 0 else None


def _apply_strategy6_backtest_rr_guard(plan: TradePlanItem, params: dict[str, Any]) -> TradePlanItem:
    cap = _strategy6_backtest_rr_cap(params)
    if cap is None or not plan.executable or plan.rr is None:
        return plan
    if _num(plan.rr) <= cap:
        return plan
    entry = _num(plan.estimated_entry_price)
    stop = _num(plan.stop_loss)
    if entry <= 0 or stop <= 0:
        return plan
    risk = abs(entry - stop)
    if risk <= 0:
        return plan
    side = str(plan.decision or "").upper()
    reward = risk * cap
    if side == "LONG":
        take_profit = entry + reward
    elif side == "SHORT":
        take_profit = entry - reward
    else:
        return plan
    guards = dict(plan.guards or {})
    guards.update(
        {
            "strategy6_backtest_rr_guard_applied": True,
            "strategy6_backtest_original_rr": plan.rr,
            "strategy6_backtest_original_take_profit": plan.take_profit,
            "strategy6_backtest_max_effective_planned_rr": cap,
        }
    )
    refs = dict(plan.input_refs or {})
    refs["strategy6_backtest_rr_guard"] = {
        "source": "p21_offline_backtest_only",
        "max_effective_planned_rr": cap,
        "original_rr": plan.rr,
    }
    reasons = list(dict.fromkeys([*list(plan.reason_codes or []), "strategy6_backtest_rr_capped"]))
    return plan.model_copy(
        update={
            "take_profit": take_profit,
            "reward_per_unit": reward,
            "rr": cap,
            "guards": guards,
            "input_refs": refs,
            "reason_codes": reasons,
        }
    )


def _strategy6_plan(plan: TradePlanItem, factor_doc: FactorSnapshotDocument, run_id: str, params: dict[str, Any]) -> TradePlanItem:
    factor = factor_doc.items[0].model_dump(mode="json")
    ev = build_strategy6_feature_vector(plan.model_dump(mode="json"), factor)
    score = score_strategy6_market_acceptance(ev, _strategy6_cfg(params))
    allow = bool(plan.executable) and score["decision_state"] == "EXECUTABLE"
    reasons = list(dict.fromkeys([*list(plan.reason_codes or []), *list(score["reason_codes"])]))
    update: dict[str, Any] = {}
    if not allow:
        if plan.executable:
            reasons.append("strategy6_gate_blocked_executable")
        state = str(score["decision_state"])
        legacy_side = str(plan.decision or "NO_TRADE").upper()
        if state == "DENY_DIRECTION_CONFLICT":
            update.update(
                {
                    "decision": "NO_TRADE",
                    "executable": False,
                    "action": "NO_TRADE",
                    "entry_mode": "NONE",
                    "estimated_entry_price": None,
                    "stop_loss": None,
                    "take_profit": None,
                    "risk_per_unit": None,
                    "reward_per_unit": None,
                    "rr": None,
                    "position_sizing": None,
                }
            )
        else:
            update.update(
                {
                    "executable": False,
                    "action": "WAIT" if legacy_side in {"LONG", "SHORT"} else "NO_TRADE",
                    "entry_mode": "WAIT_REBOUND" if state == "WAIT_REBOUND" else "WAIT_CONFIRMATION",
                }
            )
    guards = dict(plan.guards or {})
    guards.update(
        {
            "line": "strategy6",
            "strategy6_evidence_id": f"{run_id}:{plan.symbol}:strategy6",
            "strategy6_legacy_side": str(plan.decision or "NO_TRADE").upper(),
            "strategy6_decision_state": score["decision_state"],
            "strategy6_wait_state": score["wait_state"],
            "strategy6_version": score.get("strategy6_version", "v1"),
            "strategy6_direction_state": score.get("direction_state"),
            "strategy6_entry_quality_state": score.get("entry_quality_state"),
            "strategy6_adaptive_exit_tier": score.get("adaptive_exit_tier"),
            "strategy6_direction_acceptance_score": score["direction_acceptance_score"],
            "strategy6_entry_price_quality_score": score["entry_price_quality_score"],
            "strategy6_market_acceptance_score": score["market_acceptance_score"],
            "strategy6_direction_context_score": score.get("direction_context_score"),
            "strategy6_btc_alignment": score.get("btc_alignment"),
            "strategy6_reverse_momentum_bps_1m": score.get("reverse_momentum_bps_1m"),
            "strategy6_reverse_momentum_bps_3m": score.get("reverse_momentum_bps_3m"),
            "strategy6_fake_breakout_flag": score.get("fake_breakout_flag"),
            "strategy6_direction_gate_state": score.get("direction_gate_state"),
            "strategy6_entry_confirmation_state": score.get("entry_confirmation_state"),
            "strategy6_quality_filter_state": score.get("quality_filter_state"),
            "strategy6_v3_1_adverse_1m_bps": score.get("v3_1_adverse_1m_bps"),
            "strategy6_v3_1_adverse_3m_bps": score.get("v3_1_adverse_3m_bps"),
            "strategy6_v3_1_range_extreme": score.get("v3_1_range_extreme"),
            "strategy6_v3_1_low_followthrough": score.get("v3_1_low_followthrough"),
            "strategy6_v3_1_btc_against": score.get("v3_1_btc_against"),
            "strategy6_v3_2_side_profile": score.get("v3_2_side_profile"),
            "strategy6_v3_2_btc_against": score.get("v3_2_btc_against"),
            "strategy6_v3_2_btc_action": score.get("v3_2_btc_action"),
            "strategy6_v3_2_quality_filter_state": score.get("v3_2_quality_filter_state"),
            "strategy6_v3_2_quality_filter_hits": score.get("v3_2_quality_filter_hits"),
            "strategy6_v3_3_no_lookahead": score.get("v3_3_no_lookahead"),
            "strategy6_v3_3_known_at_contract": score.get("v3_3_known_at_contract"),
            "strategy6_v3_3_aligned_1m_bps": score.get("v3_3_aligned_1m_bps"),
            "strategy6_v3_3_aligned_3m_bps": score.get("v3_3_aligned_3m_bps"),
            "strategy6_v3_3_aligned_5m_bps": score.get("v3_3_aligned_5m_bps"),
            "strategy6_v3_3_adverse_1m_bps": score.get("v3_3_adverse_1m_bps"),
            "strategy6_v3_3_adverse_3m_bps": score.get("v3_3_adverse_3m_bps"),
            "strategy6_v3_3_weak_followthrough": score.get("v3_3_weak_followthrough"),
            "strategy6_v3_3_weak_volume": score.get("v3_3_weak_volume"),
            "strategy6_v3_3_early_abort_enabled": score.get("v3_3_early_abort_enabled"),
            "strategy6_v3_3_abort_if_mfe_lt_R": score.get("v3_3_abort_if_mfe_lt_R"),
            "strategy6_v3_3_abort_if_mae_gt_R": score.get("v3_3_abort_if_mae_gt_R"),
            "strategy6_v3_3_abort_window_min": score.get("v3_3_abort_window_min"),
            "strategy6_v3_3_max_initial_adverse_R": score.get("v3_3_max_initial_adverse_R"),
            "strategy6_v3_4_no_lookahead": score.get("v3_4_no_lookahead"),
            "strategy6_v3_4_known_at_contract": score.get("v3_4_known_at_contract"),
            "strategy6_v3_4_gate_profile": score.get("v3_4_gate_profile"),
            "strategy6_v3_4_gate_hits": score.get("v3_4_gate_hits"),
            "strategy6_v3_4_aligned_1m_bps": score.get("v3_4_aligned_1m_bps"),
            "strategy6_v3_4_aligned_3m_bps": score.get("v3_4_aligned_3m_bps"),
            "strategy6_v3_4_aligned_5m_bps": score.get("v3_4_aligned_5m_bps"),
            "strategy6_v3_4_volume_z": score.get("v3_4_volume_z"),
            "strategy6_v3_4_range_extreme": score.get("v3_4_range_extreme"),
            "strategy6_v3_4_distance_to_mean_bps": score.get("v3_4_distance_to_mean_bps"),
            "strategy6_v3_4_no_edge": score.get("v3_4_no_edge"),
            "strategy6_v3_5_no_lookahead": score.get("v3_5_no_lookahead"),
            "strategy6_v3_5_known_at_contract": score.get("v3_5_known_at_contract"),
            "strategy6_v3_5_loss_mode": score.get("v3_5_loss_mode"),
            "strategy6_v3_5_route_reason_codes": score.get("v3_5_route_reason_codes"),
            "strategy6_v3_5_aligned_1m_bps": score.get("v3_5_aligned_1m_bps"),
            "strategy6_v3_5_aligned_3m_bps": score.get("v3_5_aligned_3m_bps"),
            "strategy6_v3_5_aligned_5m_bps": score.get("v3_5_aligned_5m_bps"),
            "strategy6_v3_5_volume_z": score.get("v3_5_volume_z"),
            "strategy6_v3_5_range_pos": score.get("v3_5_range_pos"),
            "strategy6_v3_6_no_lookahead": score.get("v3_6_no_lookahead"),
            "strategy6_v3_6_known_at_contract": score.get("v3_6_known_at_contract"),
            "strategy6_v3_6_direction_gate": score.get("v3_6_direction_gate"),
            "strategy6_v3_6_signal_edge_gate": score.get("v3_6_signal_edge_gate"),
            "strategy6_v3_6_hard_wrong": score.get("v3_6_hard_wrong"),
            "strategy6_v3_6_no_edge": score.get("v3_6_no_edge"),
            "strategy6_v3_6_aligned_1m_bps": score.get("v3_6_aligned_1m_bps"),
            "strategy6_v3_6_aligned_3m_bps": score.get("v3_6_aligned_3m_bps"),
            "strategy6_v3_6_aligned_5m_bps": score.get("v3_6_aligned_5m_bps"),
            "strategy6_v3_6_volume_z": score.get("v3_6_volume_z"),
            "strategy6_adapter_mode": ENGINE_MODE,
        }
    )
    refs = dict(plan.input_refs or {})
    refs.update({"strategy6_source": "offline_strategy6_market_accepted_entry", "strategy6_evidence": {**ev, **score}})
    update.update({"reason_codes": list(dict.fromkeys(reasons)), "guards": guards, "input_refs": refs})
    return plan.model_copy(update=update)


def _row_num(row: dict[str, Any], key: str, default: float = 0.0) -> float:
    return _num(row.get(key), default)


def _pct(new: float, old: float) -> float:
    return (new / old - 1.0) * 10000.0 if old else 0.0


def _strategy6_wait_features(rows: list[dict[str, Any]], idx: int) -> dict[str, Any]:
    close = _row_num(rows[idx], "close")
    vol_start = max(0, idx - 30)
    vol_window = [_row_num(rows[j], "volume") for j in range(vol_start, idx)]
    avg_vol = sum(vol_window) / max(1, len(vol_window))
    range_start = max(0, idx - 30)
    high_30 = max(_row_num(rows[j], "high") for j in range(range_start, idx + 1))
    low_30 = min(_row_num(rows[j], "low") for j in range(range_start, idx + 1))
    range_pos = (close - low_30) / (high_30 - low_30) if high_30 > low_30 else 0.5
    atr_items = [
        abs(_row_num(rows[j], "high") - _row_num(rows[j], "low")) / _row_num(rows[j], "close") * 10000
        for j in range(max(1, idx - 14), idx + 1)
        if _row_num(rows[j], "close")
    ]
    atr_bps = sum(atr_items) / len(atr_items) if atr_items else 10.0
    return {
        "pct_1m_bps": round(_pct(close, _row_num(rows[idx - 1], "close")), 6) if idx >= 1 else 0.0,
        "pct_3m_bps": round(_pct(close, _row_num(rows[idx - 3], "close")), 6) if idx >= 3 else 0.0,
        "pct_5m_bps": round(_pct(close, _row_num(rows[idx - 5], "close")), 6) if idx >= 5 else 0.0,
        "pct_15m_bps": round(_pct(close, _row_num(rows[idx - 15], "close")), 6) if idx >= 15 else 0.0,
        "volume_z": round(_row_num(rows[idx], "volume") / avg_vol, 6) if avg_vol else 0.0,
        "range_pos_30m": round(range_pos, 6),
        "atr_1m_bps": round(atr_bps, 6),
        "close": close,
    }


def _strategy6_wait_score(features: dict[str, Any]) -> float:
    impulse = abs(_num(features.get("pct_3m_bps")))
    volume_z = _num(features.get("volume_z"), 1.0)
    pct_15m = abs(_num(features.get("pct_15m_bps")))
    return min(100.0, impulse * 1.6 + max(0.0, volume_z - 1.0) * 16.0 + pct_15m * 0.25)


def _strategy6_signal_at(signal: Any, rows: list[dict[str, Any]], idx: int, *, wait_minutes: int) -> SimpleNamespace:
    features = _strategy6_wait_features(rows, idx)
    return SimpleNamespace(
        signal_id=f"{getattr(signal, 'signal_id', 'signal')}:wait{wait_minutes}",
        strategy_line="strategy6",
        symbol=str(getattr(signal, "symbol", "")).upper(),
        side=str(getattr(signal, "side", "")).upper(),
        index=idx,
        signal_time_ms=int(rows[idx]["open_time_ms"]),
        score=_strategy6_wait_score(features),
        features=features,
    )


def _strategy6_adverse_bps(side: str, original: float, current: float) -> float:
    if original <= 0:
        return 0.0
    if side == "LONG":
        return max(0.0, (original - current) / original * 10000.0)
    if side == "SHORT":
        return max(0.0, (current - original) / original * 10000.0)
    return 0.0


def _strategy6_favorable_bps(side: str, original: float, current: float) -> float:
    if original <= 0:
        return 0.0
    if side == "LONG":
        return max(0.0, (current - original) / original * 10000.0)
    if side == "SHORT":
        return max(0.0, (original - current) / original * 10000.0)
    return 0.0


def _strategy6_pullback_bps(side: str, original: float, current: float) -> float:
    return _strategy6_adverse_bps(side, original, current)


def _strategy6_continuation_ok(rows: list[dict[str, Any]], idx: int, side: str, bars: int) -> bool:
    bars = max(0, int(bars or 0))
    if bars <= 0:
        return True
    if idx + bars >= len(rows):
        return False
    base = _row_num(rows[idx], "close")
    for step in range(1, bars + 1):
        close = _row_num(rows[idx + step], "close")
        if side == "LONG" and close <= base:
            return False
        if side == "SHORT" and close >= base:
            return False
    return True


def _strategy6_wait_plan_from_base(
    plan: TradePlanItem,
    wait_signal: Any,
    rows: list[dict[str, Any]],
    params: dict[str, Any],
    *,
    wait_minutes: int,
    pullback_bps: float,
    favorable_bps: float,
    initial_reasons: list[str],
) -> TradePlanItem | None:
    entry_idx = int(getattr(wait_signal, "index", 0)) + 1
    if entry_idx >= len(rows):
        return None
    old_entry = _num(plan.estimated_entry_price)
    old_stop = _num(plan.stop_loss)
    old_rr = _num(plan.rr)
    if old_entry <= 0 or old_stop <= 0 or old_rr <= 0:
        return None
    stop_bps = abs(old_entry - old_stop) / old_entry * 10000.0
    if stop_bps <= 0:
        return None
    rr_cap = _strategy6_backtest_rr_cap(params)
    planned_rr = min(old_rr, rr_cap) if rr_cap is not None else old_rr
    entry = _row_num(rows[entry_idx], "open")
    side = str(plan.decision or "").upper()
    if side == "LONG":
        stop = entry * (1.0 - stop_bps / 10000.0)
        take_profit = entry * (1.0 + stop_bps * planned_rr / 10000.0)
    elif side == "SHORT":
        stop = entry * (1.0 + stop_bps / 10000.0)
        take_profit = entry * (1.0 - stop_bps * planned_rr / 10000.0)
    else:
        return None
    risk = abs(entry - stop)
    reward = abs(take_profit - entry)
    guards = dict(plan.guards or {})
    guards.update(
        {
            "strategy6_wait_backtest_applied": True,
            "strategy6_wait_minutes": wait_minutes,
            "strategy6_wait_pullback_bps": round(pullback_bps, 6),
            "strategy6_wait_favorable_bps": round(favorable_bps, 6),
            "strategy6_wait_mode": plan.entry_mode,
        }
    )
    if rr_cap is not None and old_rr > rr_cap:
        guards.update(
            {
                "strategy6_backtest_rr_guard_applied": True,
                "strategy6_backtest_original_rr": plan.rr,
                "strategy6_backtest_original_take_profit": plan.take_profit,
                "strategy6_backtest_max_effective_planned_rr": rr_cap,
            }
        )
    refs = dict(plan.input_refs or {})
    refs["strategy6_wait_backtest"] = {
        "source": "p21_offline_wait_reprice",
        "wait_minutes": wait_minutes,
        "pullback_bps": round(pullback_bps, 6),
        "favorable_bps": round(favorable_bps, 6),
    }
    reasons = list(
        dict.fromkeys(
            [
                *initial_reasons,
                "strategy6_wait_rebound_rechecked",
                *list(plan.reason_codes or []),
                "strategy6_wait_backtest_repriced_entry",
            ]
        )
    )
    if rr_cap is not None and old_rr > rr_cap:
        reasons.append("strategy6_backtest_rr_capped")
    return plan.model_copy(
        update={
            "action": "ENTER_MARKET",
            "entry_mode": "MARKET",
            "estimated_entry_price": entry,
            "stop_loss": stop,
            "take_profit": take_profit,
            "risk_per_unit": risk,
            "reward_per_unit": reward,
            "rr": planned_rr,
            "executable": True,
            "reason_codes": list(dict.fromkeys(reasons)),
            "guards": guards,
            "input_refs": refs,
        }
    )


def _order_from_plan(
    plan: TradePlanItem,
    signal: Any,
    rows: list[dict[str, Any]],
    params: dict[str, Any],
    cfg: TradePlanLineConfig,
    plan_payload: dict[str, Any],
) -> dict[str, Any] | None:
    entry_idx = int(getattr(signal, "index", 0)) + 1
    if entry_idx >= len(rows):
        return None
    features = dict(getattr(signal, "features", {}) or {})
    if str(params.get("strategy_line") or getattr(signal, "strategy_line", "")).lower() == "strategy6":
        guards = dict(plan.guards or {})
        for key in (
            "strategy6_version",
            "strategy6_decision_state",
            "strategy6_wait_state",
            "strategy6_direction_state",
            "strategy6_entry_quality_state",
            "strategy6_adaptive_exit_tier",
            "strategy6_direction_acceptance_score",
            "strategy6_entry_price_quality_score",
            "strategy6_market_acceptance_score",
            "strategy6_direction_context_score",
            "strategy6_btc_alignment",
            "strategy6_reverse_momentum_bps_1m",
            "strategy6_reverse_momentum_bps_3m",
            "strategy6_fake_breakout_flag",
            "strategy6_direction_gate_state",
            "strategy6_entry_confirmation_state",
            "strategy6_quality_filter_state",
            "strategy6_v3_1_adverse_1m_bps",
            "strategy6_v3_1_adverse_3m_bps",
            "strategy6_v3_1_range_extreme",
            "strategy6_v3_1_low_followthrough",
            "strategy6_v3_1_btc_against",
            "strategy6_v3_2_side_profile",
            "strategy6_v3_2_btc_against",
            "strategy6_v3_2_btc_action",
            "strategy6_v3_2_quality_filter_state",
            "strategy6_v3_2_quality_filter_hits",
            "strategy6_v3_3_no_lookahead",
            "strategy6_v3_3_known_at_contract",
            "strategy6_v3_3_aligned_1m_bps",
            "strategy6_v3_3_aligned_3m_bps",
            "strategy6_v3_3_aligned_5m_bps",
            "strategy6_v3_3_adverse_1m_bps",
            "strategy6_v3_3_adverse_3m_bps",
            "strategy6_v3_3_weak_followthrough",
            "strategy6_v3_3_weak_volume",
            "strategy6_v3_3_early_abort_enabled",
            "strategy6_v3_3_abort_if_mfe_lt_R",
            "strategy6_v3_3_abort_if_mae_gt_R",
            "strategy6_v3_3_abort_window_min",
            "strategy6_v3_3_max_initial_adverse_R",
            "strategy6_v3_4_no_lookahead",
            "strategy6_v3_4_known_at_contract",
            "strategy6_v3_4_gate_profile",
            "strategy6_v3_4_gate_hits",
            "strategy6_v3_4_aligned_1m_bps",
            "strategy6_v3_4_aligned_3m_bps",
            "strategy6_v3_4_aligned_5m_bps",
            "strategy6_v3_4_volume_z",
            "strategy6_v3_4_range_extreme",
            "strategy6_v3_4_distance_to_mean_bps",
            "strategy6_v3_4_no_edge",
            "strategy6_v3_5_no_lookahead",
            "strategy6_v3_5_known_at_contract",
            "strategy6_v3_5_loss_mode",
            "strategy6_v3_5_route_reason_codes",
            "strategy6_v3_5_aligned_1m_bps",
            "strategy6_v3_5_aligned_3m_bps",
            "strategy6_v3_5_aligned_5m_bps",
            "strategy6_v3_5_volume_z",
            "strategy6_v3_5_range_pos",
            "strategy6_v3_6_no_lookahead",
            "strategy6_v3_6_known_at_contract",
            "strategy6_v3_6_direction_gate",
            "strategy6_v3_6_signal_edge_gate",
            "strategy6_v3_6_hard_wrong",
            "strategy6_v3_6_no_edge",
            "strategy6_v3_6_aligned_1m_bps",
            "strategy6_v3_6_aligned_3m_bps",
            "strategy6_v3_6_aligned_5m_bps",
            "strategy6_v3_6_volume_z",
        ):
            if key in guards:
                features[key] = guards[key]
    return {
        "symbol": plan.symbol,
        "strategy_line": str(params.get("strategy_line") or getattr(signal, "strategy_line", "without_micro")),
        "side": plan.decision,
        "signal_time_ms": int(getattr(signal, "signal_time_ms", 0)),
        "entry_time_ms": int(rows[entry_idx]["open_time_ms"]),
        "entry_idx": entry_idx,
        "entry_price": float(plan.estimated_entry_price or rows[entry_idx]["open"]),
        "stop_loss": float(plan.stop_loss),
        "take_profit": float(plan.take_profit),
        "stop_bps": abs(float(plan.estimated_entry_price) - float(plan.stop_loss)) / float(plan.estimated_entry_price) * 10000,
        "target_bps": abs(float(plan.take_profit) - float(plan.estimated_entry_price)) / float(plan.estimated_entry_price) * 10000,
        "planned_rr": float(plan.rr or 0.0),
        "cost_bps": float(params.get("taker_fee_bps") or cfg.taker_fee_bps) * 2 + float(params.get("slippage_bps") or 0.0),
        "score": float(getattr(signal, "score", 0.0)),
        "features": features,
        "lineage_mode": ENGINE_MODE,
        "source_contract_version": SOURCE_CONTRACT_VERSION,
        "trade_plan_payload": plan_payload,
        "config_patch": params,
        "entry_mode": plan.entry_mode,
        "effective_rr": plan.guards.get("effective_rr"),
        "fast_exit_policy": {
            "mode": plan.guards.get("tp_target_policy_mode"),
            "scope": plan.guards.get("tp_target_policy_scope"),
            "basis": plan.guards.get("tp_target_policy_basis"),
        },
    }


def _try_strategy6_wait_rebound(
    signal: Any,
    rows: list[dict[str, Any]],
    params: dict[str, Any],
    plan: TradePlanItem | None,
    initial_reasons: list[str],
    plan_payload: dict[str, Any],
    cfg: TradePlanLineConfig,
) -> dict[str, Any] | None:
    if params.get("_strategy6_wait_attempt") or not bool(params.get("strategy6_wait_rebound_enabled")):
        return None
    if plan is None:
        return None
    state = str((plan.guards or {}).get("strategy6_decision_state") or "")
    entry_mode = str(plan.entry_mode or "")
    base_wait_allowed = bool(params.get("strategy6_wait_allow_base_wait"))
    if not base_wait_allowed and "strategy6_gate_blocked_executable" not in set(initial_reasons):
        return None
    if state == "DENY_DIRECTION_CONFLICT" or entry_mode not in {"WAIT_REBOUND", "WAIT_CONFIRMATION"}:
        return None
    start_idx = int(getattr(signal, "index", 0))
    if start_idx < 1 or start_idx >= len(rows) - 2:
        return None
    side = str(getattr(signal, "side", "")).upper()
    original_close = _row_num(rows[start_idx], "close")
    interval = max(1, int(_num(params.get("wait_check_interval_min"), 1)))
    max_wait = max(interval, int(_num(params.get("max_wait_minutes"), 5)))
    min_rebound_score = _num(params.get("min_rebound_score"), 0.0)
    pullback_min = _num(params.get("pullback_min_bps"), 0.0)
    pullback_max = _num(params.get("pullback_max_bps"), 999999.0)
    max_chase = _num(params.get("max_chase_after_wait_bps"), 999999.0)
    deny_1m = _num(params.get("deny_if_adverse_1m_bps"), 999999.0)
    deny_3m = _num(params.get("deny_if_adverse_3m_bps"), 999999.0)
    confirm_bars = int(_num(params.get("continuation_confirm_bars"), 0))
    attempted = 0
    for wait_minutes in range(interval, max_wait + 1, interval):
        idx = start_idx + wait_minutes
        if idx >= len(rows) - 1:
            break
        current_close = _row_num(rows[idx], "close")
        adverse = _strategy6_adverse_bps(side, original_close, current_close)
        if wait_minutes <= 1 and adverse > deny_1m:
            return None
        if wait_minutes >= 3 and adverse > deny_3m:
            return None
        pullback = _strategy6_pullback_bps(side, original_close, current_close)
        favorable = _strategy6_favorable_bps(side, original_close, current_close)
        if entry_mode == "WAIT_REBOUND" and not (pullback_min <= pullback <= pullback_max):
            continue
        if favorable > max_chase:
            continue
        if not _strategy6_continuation_ok(rows, idx, side, confirm_bars):
            continue
        wait_signal = _strategy6_signal_at(signal, rows, idx, wait_minutes=wait_minutes)
        if _num(getattr(wait_signal, "score", 0.0)) < min_rebound_score:
            continue
        attempted += 1
        wait_plan = _strategy6_wait_plan_from_base(
            plan,
            wait_signal,
            rows,
            params,
            wait_minutes=wait_minutes,
            pullback_bps=pullback,
            favorable_bps=favorable,
            initial_reasons=initial_reasons,
        )
        if wait_plan is None:
            continue
        wait_payload = dict(plan_payload)
        wait_payload["status"] = "ok"
        wait_payload["executable_count"] = 1
        wait_payload["plans"] = [wait_plan.model_dump(mode="json")]
        order = _order_from_plan(wait_plan, wait_signal, rows, params, cfg, wait_payload)
        if order is None:
            continue
        features = dict(order.get("features") or {})
        features.update(
            {
                "strategy6_wait_minutes": wait_minutes,
                "strategy6_wait_attempts": attempted,
                "strategy6_wait_pullback_bps": round(pullback, 6),
                "strategy6_wait_favorable_bps": round(favorable, 6),
            }
        )
        clean_params = {k: v for k, v in params.items() if not str(k).startswith("_strategy6_")}
        order["features"] = features
        order["entry_mode"] = entry_mode
        order["config_patch"] = clean_params
        return {
            "lineage_mode": ENGINE_MODE,
            "source_contract_version": SOURCE_CONTRACT_VERSION,
            "trade_plan_payload": wait_payload,
            "config_patch": clean_params,
            "reason_codes": list(wait_plan.reason_codes or []),
            "executable": True,
            "order": order,
        }
    return None


def evaluate_signal_offline(signal: Any, rows: list[dict[str, Any]], params: dict[str, Any]) -> dict[str, Any]:
    strategy_line = str(params.get("strategy_line") or getattr(signal, "strategy_line", "without_micro"))
    eval_line = strategy_line if strategy_line in {"strategy5", "strategy6"} else "strategy4" if strategy_line == "strategy4" else "without_micro"
    cfg = _config_from_params(eval_line, params)
    factor = _factor_doc(signal)
    refresh = _refresh_doc(signal, params)
    liquidity = _liquidity_doc(signal, params)
    run_id = f"p21_offline_{getattr(signal, 'signal_id', 'signal')}"
    doc = build_trade_plan_line_document(
        line=eval_line,  # type: ignore[arg-type]
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=liquidity,
        micro_doc=None,
        micro_state_doc=None,
        generated_at=_iso_from_ms(int(getattr(signal, "signal_time_ms", 0))),
        run_id=run_id,
        cycle_id=None,
        cfg=cfg,
        project_root=None,
    )
    plan = doc.plans[0] if doc.plans else None
    if plan is not None and strategy_line == "strategy5":
        plan = _strategy5_plan(plan, factor, run_id)
        doc = doc.model_copy(update={"plans": [plan], "count": 1, "executable_count": 1 if plan.executable else 0})
    if plan is not None and strategy_line == "strategy6":
        plan = _strategy6_plan(plan, factor, run_id, params)
        if plan.executable:
            plan = _apply_strategy6_backtest_rr_guard(plan, params)
        doc = doc.model_copy(update={"plans": [plan], "count": 1, "executable_count": 1 if plan.executable else 0})
    reasons = list(plan.reason_codes if plan is not None else ["no_plan"])
    plan_payload = doc.model_dump(mode="json")
    common = {
        "lineage_mode": ENGINE_MODE,
        "source_contract_version": SOURCE_CONTRACT_VERSION,
        "trade_plan_payload": plan_payload,
        "config_patch": params,
        "reason_codes": reasons,
        "executable": bool(plan.executable) if plan is not None else False,
    }
    if plan is None or not plan.executable:
        if strategy_line == "strategy6":
            waited = _try_strategy6_wait_rebound(signal, rows, params, plan, reasons, plan_payload, cfg)
            if waited is not None:
                return waited
        return {**common, "order": None}
    entry_idx = int(getattr(signal, "index", 0)) + 1
    if entry_idx >= len(rows):
        return {**common, "order": None, "reason_codes": [*reasons, "missing_entry_candle"], "executable": False}
    order = _order_from_plan(plan, signal, rows, params, cfg, plan_payload)
    if order is None:
        return {**common, "order": None, "reason_codes": [*reasons, "missing_entry_candle"], "executable": False}
    return {**common, "order": order}
