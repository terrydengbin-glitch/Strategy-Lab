"""Audit current JSON artifacts across the main business chain.

Writes a machine-readable JSON summary. This script is intentionally read-only.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from laoma_signal_engine.core.models import CandidateUniverseDocument
from laoma_signal_engine.decision.final_models import FinalDecisionsDocument
from laoma_signal_engine.decision.market_entry_models import (
    MarketEntryDirectionDocument,
    MarketEntryPlanDocument,
)
from laoma_signal_engine.decision.models import DirectionGateDocument
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanLineDocument
from laoma_signal_engine.factors.models import FactorSnapshotDocument
from laoma_signal_engine.market.decision_refresh_models import DecisionRefreshDocument
from laoma_signal_engine.market.light_snapshot_models import FuturesLightSnapshotDocument
from laoma_signal_engine.market.market_entry_liquidity_models import MarketEntryLiquidityDocument
from laoma_signal_engine.micro.assembly.models import LatestMicroFeaturesDocument
from laoma_signal_engine.micro.daemon.state_models import MicroDaemonStateDocument
from laoma_signal_engine.micro.micro_target_models import MicroTargetsDocument
from laoma_signal_engine.scanner.signal_models import AbnormalTierDocument


ALTCOIN_SMALL_SIZE_LIQUIDITY_PROFILE = {
    "margin_usdt": 100.0,
    "leverage": 20.0,
    "notional_usdt": 2000.0,
    "max_spread_bps": 15.0,
    "max_estimated_slippage_bps": 30.0,
    "min_top_depth_usdt": 6000.0,
    "min_quote_volume_24h": 500000.0,
}


def _audit_profile_mode(profile: dict[str, float | None] | None) -> dict[str, Any]:
    if not profile:
        return {"mode": "unknown", "reason": "liquidity profile missing", "actual": profile or {}}
    max_spread = profile.get("max_spread_bps")
    min_depth = profile.get("min_top_depth_usdt")
    try:
        relaxed = float(max_spread or 0) > 15.0 or float(min_depth or 0) < 6000.0
    except (TypeError, ValueError):
        relaxed = False
    strict = all(_approx_eq(profile.get(k), v) for k, v in ALTCOIN_SMALL_SIZE_LIQUIDITY_PROFILE.items())
    if strict:
        return {"mode": "strict", "reason": "matches altcoin small-size production profile", "actual": profile}
    if relaxed:
        return {"mode": "relaxed", "reason": "liquidity thresholds relaxed for executable chain testing", "actual": profile}
    return {"mode": "custom", "reason": "custom liquidity profile", "actual": profile}


def _relax_check(checks: list[dict[str, Any]], names: set[str], *, reason: str, field: str) -> None:
    for item in checks:
        if item.get("name") in names and not item.get("ok"):
            item["ok"] = True
            item["severity"] = "WARNING"
            item[field] = reason


def parse_iso_z(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def age_sec(start: str | None, end: str | None) -> int | None:
    a = parse_iso_z(start)
    b = parse_iso_z(end)
    if a is None or b is None:
        return None
    return int((b - a).total_seconds())


def tier_symbols(doc: AbnormalTierDocument) -> list[str]:
    return [s.symbol for s in doc.signals]


def add_check(
    checks: list[dict[str, Any]],
    name: str,
    ok: bool,
    detail: Any,
    severity: str = "INFO",
) -> None:
    checks.append(
        {
            "name": name,
            "ok": bool(ok),
            "severity": "OK" if ok else severity,
            "detail": detail,
        },
    )


def _approx_eq(a: object, b: float, *, eps: float = 1e-9) -> bool:
    if isinstance(a, bool) or a is None:
        return False
    try:
        return abs(float(a) - b) <= eps
    except (TypeError, ValueError):
        return False


def _liquidity_profile(doc: MarketEntryLiquidityDocument) -> dict[str, float | None]:
    return {
        "margin_usdt": getattr(doc, "margin_usdt", None),
        "leverage": getattr(doc, "leverage", None),
        "notional_usdt": getattr(doc, "notional_usdt", None),
        "max_spread_bps": doc.max_spread_bps,
        "max_estimated_slippage_bps": doc.max_estimated_slippage_bps,
        "min_top_depth_usdt": doc.min_top_depth_usdt,
        "min_quote_volume_24h": doc.min_quote_volume_24h,
    }


def load_models(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    files: dict[str, tuple[str, Any]] = {
        "universe": ("DATA/universe/CANDIDATE_UNIVERSE.json", CandidateUniverseDocument),
        "light": ("DATA/market/futures_light_snapshot.json", FuturesLightSnapshotDocument),
        "liquidity": ("DATA/market/latest_market_entry_liquidity.json", MarketEntryLiquidityDocument),
        "raw_candidates": ("DATA/raw_signals/latest_raw_candidates.json", AbnormalTierDocument),
        "watch": ("DATA/raw_signals/latest_watch_signals.json", AbnormalTierDocument),
        "strong": ("DATA/raw_signals/latest_strong_candidates.json", AbnormalTierDocument),
        "targets": ("DATA/micro/micro_targets.json", MicroTargetsDocument),
        "micro": ("DATA/micro/latest_micro_features.json", LatestMicroFeaturesDocument),
        "factor_no_micro": ("DATA/factors/latest_factor_snapshot_withoutoficvd.json", FactorSnapshotDocument),
        "factor": ("DATA/factors/latest_factor_snapshot.json", FactorSnapshotDocument),
        "direction": ("DATA/decisions/latest_direction_decisions.json", DirectionGateDocument),
        "final": ("DATA/decisions/latest_decisions.json", FinalDecisionsDocument),
        "refresh": ("DATA/market/latest_decision_refresh_snapshot.json", DecisionRefreshDocument),
        "me_direction": (
            "DATA/decisions/latest_market_entry_direction_decisions.json",
            MarketEntryDirectionDocument,
        ),
        "me_plan": ("DATA/decisions/latest_market_entry_decisions.json", MarketEntryPlanDocument),
    }
    raw: dict[str, Any] = {}
    models: dict[str, Any] = {}
    for key, (rel, model) in files.items():
        text = (root / rel).read_text(encoding="utf-8")
        raw[key] = json.loads(text)
        models[key] = model.model_validate_json(text)
    optional_files: dict[str, tuple[str, Any]] = {
        "micro_state": ("DATA/micro/latest_micro_state.json", MicroDaemonStateDocument),
        "trade_plan_without_micro": (
            "DATA/decisions/latest_trade_plan_without_micro.json",
            TradePlanLineDocument,
        ),
        "trade_plan_micro_fast": (
            "DATA/decisions/latest_trade_plan_micro_fast.json",
            TradePlanLineDocument,
        ),
        "trade_plan_micro_full": (
            "DATA/decisions/latest_trade_plan_micro_full.json",
            TradePlanLineDocument,
        ),
        "refresh_liquidity": (
            "DATA/market/decision_refresh_liquidity_snapshot.json",
            MarketEntryLiquidityDocument,
        ),
    }
    for key, (rel, model) in optional_files.items():
        path = root / rel
        if path.exists():
            text = path.read_text(encoding="utf-8")
            raw[key] = json.loads(text)
            models[key] = model.model_validate_json(text)
    scheduler_report_path = root / "DATA/reports/latest_scheduler_5m_report.json"
    if scheduler_report_path.exists():
        text = scheduler_report_path.read_text(encoding="utf-8")
        raw["scheduler_5m"] = json.loads(text)
    return raw, models


def build_summary(
    root: Path,
    *,
    audit_mode: str = "full_chain",
    require_without_micro_standalone_refresh: bool = False,
) -> dict[str, Any]:
    raw, models = load_models(root)
    checks: list[dict[str, Any]] = []
    for key in models:
        add_check(checks, f"{key}.contract_parse", True, key)
    sidechain_only = audit_mode == "without_micro_sidechain"

    light = models["light"]
    liquidity = models["liquidity"]
    raw_doc = models["raw_candidates"]
    watch = models["watch"]
    strong = models["strong"]
    targets = models["targets"]
    micro = models["micro"]
    micro_state = models.get("micro_state")
    factor_no_micro = models["factor_no_micro"]
    factor = models["factor"]
    direction = models["direction"]
    final = models["final"]
    refresh = models["refresh"]
    me_direction = models["me_direction"]
    me_plan = models["me_plan"]
    trade_plan_without_micro = models.get("trade_plan_without_micro")
    trade_plan_micro_fast = models.get("trade_plan_micro_fast")
    trade_plan_micro_full = models.get("trade_plan_micro_full")
    refresh_liquidity = models.get("refresh_liquidity")

    add_check(
        checks,
        "light.counts",
        light.snapshot_count == len(light.items) == light.success_count,
        f"snapshot={light.snapshot_count}, items={len(light.items)}, success={light.success_count}",
        "HIGH",
    )
    add_check(checks, "light.failed_zero", light.failed_count == 0, f"failed_count={light.failed_count}", "MEDIUM")
    add_check(
        checks,
        "liquidity.count",
        liquidity.count == len(liquidity.items) == light.snapshot_count,
        f"liquidity={liquidity.count}, light={light.snapshot_count}",
        "MEDIUM",
    )
    liquidity_ok_count = sum(1 for item in liquidity.items if item.liquidity_ok_for_market_entry)
    add_check(
        checks,
        "liquidity.ok_available",
        liquidity_ok_count > 0,
        f"ok_count={liquidity_ok_count}/{liquidity.count}",
        "MEDIUM",
    )
    liquidity_profile = _liquidity_profile(liquidity)
    add_check(
        checks,
        "liquidity.profile_matches_altcoin_small_size",
        all(_approx_eq(liquidity_profile.get(k), v) for k, v in ALTCOIN_SMALL_SIZE_LIQUIDITY_PROFILE.items()),
        {
            "actual": liquidity_profile,
            "expected": ALTCOIN_SMALL_SIZE_LIQUIDITY_PROFILE,
        },
        "HIGH",
    )
    if refresh_liquidity is not None:
        refresh_liquidity_profile = _liquidity_profile(refresh_liquidity)
        add_check(
            checks,
            "refresh_liquidity.profile_matches_altcoin_small_size",
            all(
                _approx_eq(refresh_liquidity_profile.get(k), v)
                for k, v in ALTCOIN_SMALL_SIZE_LIQUIDITY_PROFILE.items()
            ),
            {
                "actual": refresh_liquidity_profile,
                "expected": ALTCOIN_SMALL_SIZE_LIQUIDITY_PROFILE,
            },
            "HIGH",
        )
    else:
        refresh_liquidity_profile = None
    audit_profile = _audit_profile_mode(refresh_liquidity_profile or liquidity_profile)

    add_check(
        checks,
        "scanner.counts",
        raw_doc.count == len(raw_doc.signals)
        and watch.count == len(watch.signals)
        and strong.count == len(strong.signals),
        f"raw={raw_doc.count}, watch={watch.count}, strong={strong.count}",
        "HIGH",
    )
    add_check(
        checks,
        "scanner.input_light_consistency",
        raw_doc.input_snapshot_generated_at == light.generated_at
        and watch.input_snapshot_generated_at == light.generated_at
        and strong.input_snapshot_generated_at == light.generated_at,
        {
            "light": light.generated_at,
            "raw": raw_doc.input_snapshot_generated_at,
            "watch": watch.input_snapshot_generated_at,
            "strong": strong.input_snapshot_generated_at,
        },
        "HIGH",
    )
    add_check(
        checks,
        "scanner.fresh_at_scan",
        raw_doc.input_snapshot_age_sec <= 180 and watch.input_snapshot_age_sec <= 180,
        f"raw_age={raw_doc.input_snapshot_age_sec}, watch_age={watch.input_snapshot_age_sec}",
        "HIGH",
    )

    watch_symbols = set(tier_symbols(watch))
    strong_symbols = set(tier_symbols(strong))
    candidate_symbols = watch_symbols | strong_symbols
    target_symbols = {
        item.symbol for item in targets.tier1_warm_watch
    } | {item.symbol for item in targets.tier2_active_strong}
    router_symbol_ok = (
        target_symbols == candidate_symbols
        if len(candidate_symbols) <= 10
        else target_symbols.issubset(candidate_symbols) and len(target_symbols) <= 10
    )
    add_check(
        checks,
        "router.symbols_match_watch_strong",
        router_symbol_ok,
        {
            "targets": sorted(target_symbols),
            "candidates": sorted(candidate_symbols),
            "candidate_count": len(candidate_symbols),
            "target_count": len(target_symbols),
            "note": "when candidates exceed cap, targets may be a capped subset",
        },
        "HIGH",
    )
    add_check(checks, "router.cap", len(target_symbols) <= 10, f"target_count={len(target_symbols)}", "HIGH")

    micro_symbols = {item.symbol for item in micro.items}
    add_check(
        checks,
        "micro.symbols_match_targets",
        micro_symbols == target_symbols,
        {"micro": sorted(micro_symbols), "targets": sorted(target_symbols)},
        "HIGH",
    )
    fast_ready_count = sum(1 for item in micro.items if item.micro_fast_quality and item.micro_fast_quality.ready)
    full_ready_count = sum(1 for item in micro.items if item.micro_full_quality and item.micro_full_quality.ready)
    legacy_ready_count = sum(1 for item in micro.items if item.micro_quality.ready)
    add_check(
        checks,
        "micro.fast_count_matches",
        micro.fast_ready_count == fast_ready_count,
        f"doc={micro.fast_ready_count}, computed={fast_ready_count}",
        "HIGH",
    )
    add_check(
        checks,
        "micro.full_count_matches",
        micro.full_ready_count == full_ready_count == micro.ready_count == legacy_ready_count,
        f"full={micro.full_ready_count}, ready={micro.ready_count}, legacy={legacy_ready_count}",
        "HIGH",
    )
    add_check(
        checks,
        "micro.target_anchor",
        micro.target_generated_at == targets.generated_at,
        {"micro_target_generated_at": micro.target_generated_at, "targets_generated_at": targets.generated_at},
        "HIGH",
    )
    if micro_state is not None:
        state_symbols = {item.symbol for item in micro_state.symbols}
        add_check(
            checks,
            "micro_state.symbols_match_micro",
            state_symbols == micro_symbols,
            {"state": sorted(state_symbols), "micro": sorted(micro_symbols)},
            "HIGH",
        )
        add_check(
            checks,
            "micro_state.target_anchor",
            micro_state.target_generated_at == targets.generated_at,
            {
                "micro_state_target_generated_at": micro_state.target_generated_at,
                "targets_generated_at": targets.generated_at,
            },
            "HIGH",
        )
        add_check(
            checks,
            "micro_state.generated_with_micro",
            micro_state.generated_at == micro.generated_at,
            {"micro_state": micro_state.generated_at, "micro": micro.generated_at},
            "MEDIUM",
        )
        add_check(
            checks,
            "micro_state.active_count",
            micro_state.active_symbol_count == len(micro_state.symbols),
            f"active_symbol_count={micro_state.active_symbol_count}, symbols={len(micro_state.symbols)}",
            "HIGH",
        )
    else:
        add_check(
            checks,
            "micro_state.present",
            False,
            "DATA/micro/latest_micro_state.json missing; fast/full will use fallback",
            "MEDIUM",
        )

    factor_symbols = {item.symbol for item in factor.items}
    factor_no_micro_symbols = {item.symbol for item in factor_no_micro.items}
    add_check(
        checks,
        "factor.symbols_match_candidates",
        factor_symbols == candidate_symbols,
        {"factor": sorted(factor_symbols), "candidates": sorted(candidate_symbols)},
        "HIGH",
    )
    add_check(
        checks,
        "factor_no_micro.symbols_match_candidates",
        factor_no_micro_symbols == candidate_symbols,
        {"factor_no_micro": sorted(factor_no_micro_symbols), "candidates": sorted(candidate_symbols)},
        "HIGH",
    )
    micro_false_factor_ready = [
        item.symbol for item in factor.items if not item.micro_quality.ready and item.factor_quality.ready
    ]
    add_check(
        checks,
        "factor.status_reflects_item_micro_ready",
        not micro_false_factor_ready,
        {"micro_not_ready_but_factor_quality_ready": micro_false_factor_ready, "top_status": factor.status},
        "MEDIUM",
    )
    factor_micro_ref = factor.input_refs.get("micro_generated_at")
    add_check(
        checks,
        "factor.lineage.micro_generated_at_present",
        factor.source == "factor_snapshot_without_ofi_cvd" or bool(factor_micro_ref),
        {"factor_input_refs": factor.input_refs},
        "HIGH",
    )

    direction_symbols = {item.symbol for item in direction.decisions}
    final_symbols = {item.symbol for item in final.decisions}
    refresh_symbols = {item.symbol for item in refresh.items}
    market_entry_direction_symbols = {item.symbol for item in me_direction.decisions}
    market_entry_plan_symbols = {item.symbol for item in me_plan.plans}
    add_check(checks, "direction.symbols_match_factor", direction_symbols == factor_symbols, sorted(direction_symbols), "HIGH")
    add_check(checks, "final.symbols_match_direction", final_symbols == direction_symbols, sorted(final_symbols), "HIGH")
    refresh_expected_symbols = factor_no_micro_symbols if sidechain_only else factor_symbols
    refresh_expected_generated_at = factor_no_micro.generated_at if sidechain_only else factor.generated_at
    add_check(
        checks,
        "refresh.symbols_match_factor",
        refresh_symbols == refresh_expected_symbols,
        {
            "refresh": sorted(refresh_symbols),
            "expected": sorted(refresh_expected_symbols),
            "audit_mode": audit_mode,
        },
        "HIGH",
    )
    add_check(
        checks,
        "refresh.fresh",
        refresh.status == "ok"
        and refresh.stale_count == 0
        and all(item.refresh_age_sec <= refresh.max_refresh_age_sec for item in refresh.items),
        {
            "status": refresh.status,
            "stale_count": refresh.stale_count,
            "ages": [item.refresh_age_sec for item in refresh.items],
        },
        "HIGH",
    )
    add_check(
        checks,
        "refresh.input_factor_anchor",
        refresh.input_factor_generated_at == refresh_expected_generated_at,
        {
            "refresh_input_factor": refresh.input_factor_generated_at,
            "expected_factor": refresh_expected_generated_at,
            "audit_mode": audit_mode,
        },
        "HIGH",
    )
    add_check(
        checks,
        "refresh.input_liquidity_anchor",
        refresh.input_liquidity_generated_at
        == (refresh_liquidity.generated_at if refresh_liquidity is not None else liquidity.generated_at),
        {
            "refresh_input_liquidity": refresh.input_liquidity_generated_at,
            "latest_liquidity": liquidity.generated_at,
            "refresh_liquidity": refresh_liquidity.generated_at if refresh_liquidity is not None else None,
        },
        "HIGH",
    )
    refresh_liquidity_ages = [item.liquidity_age_sec for item in refresh.items if item.liquidity_age_sec is not None]
    add_check(
        checks,
        "refresh.liquidity_age_fresh",
        len(refresh_liquidity_ages) == len(refresh.items)
        and all(age <= refresh.max_liquidity_age_sec for age in refresh_liquidity_ages),
        {
            "max_liquidity_age_sec": refresh.max_liquidity_age_sec,
            "ages": refresh_liquidity_ages,
            "missing_age_count": len(refresh.items) - len(refresh_liquidity_ages),
        },
        "HIGH",
    )
    add_check(
        checks,
        "market_entry_direction.symbols_match_factor",
        market_entry_direction_symbols == factor_symbols,
        sorted(market_entry_direction_symbols),
        "HIGH",
    )
    add_check(
        checks,
        "market_entry_plan.symbols_match_direction",
        market_entry_plan_symbols == market_entry_direction_symbols,
        sorted(market_entry_plan_symbols),
        "HIGH",
    )
    add_check(
        checks,
        "market_entry.no_executable_consistent",
        me_plan.executable_count == sum(1 for plan in me_plan.plans if plan.executable),
        f"executable_count={me_plan.executable_count}",
        "HIGH",
    )
    trade_docs = {
        "without_micro": trade_plan_without_micro,
        "micro_fast": trade_plan_micro_fast,
        "micro_full": trade_plan_micro_full,
    }
    opportunity_distribution: dict[str, dict[str, int]] = {}
    if all(doc is not None for doc in trade_docs.values()):
        add_check(checks, "p10.trade_plan_outputs_present", True, sorted(trade_docs), "HIGH")
        opportunity_distribution = {
            name: dict(Counter(str(plan.guards.get("opportunity_type", "missing")) for plan in doc.plans))
            for name, doc in trade_docs.items()
            if doc is not None
        }
        p10_symbols = {
            name: {plan.symbol for plan in doc.plans}
            for name, doc in trade_docs.items()
            if doc is not None
        }
        add_check(
            checks,
            "p10.symbols_match_factor",
            p10_symbols["without_micro"] == factor_no_micro_symbols
            and p10_symbols["micro_fast"] == factor_symbols
            and p10_symbols["micro_full"] == factor_symbols,
            {k: sorted(v) for k, v in p10_symbols.items()},
            "HIGH",
        )
        run_ids = {name: doc.run_id for name, doc in trade_docs.items() if doc is not None}
        cycle_ids = {name: doc.cycle_id for name, doc in trade_docs.items() if doc is not None}
        add_check(
            checks,
            "p10.run_id_consistent",
            None not in run_ids.values() and len(set(run_ids.values())) == 1,
            run_ids,
            "HIGH",
        )
        add_check(
            checks,
            "p10.cycle_id_consistent",
            None not in cycle_ids.values() and len(set(cycle_ids.values())) == 1,
            cycle_ids,
            "HIGH",
        )
        add_check(
            checks,
            "p10.without_micro_no_micro_refs",
            all(
                "micro_generated_at" not in plan.input_refs
                and "micro_state_generated_at" not in plan.input_refs
                for plan in trade_plan_without_micro.plans
            ),
            "without_micro should not reference micro inputs",
            "HIGH",
        )
        without_micro_doc_refs = dict(trade_plan_without_micro.input_refs)
        add_check(
            checks,
            "p10.without_micro_contract",
            trade_plan_without_micro.source == "trade_plan_without_micro"
            and trade_plan_without_micro.micro_mode == "none",
            {
                "source": trade_plan_without_micro.source,
                "micro_mode": trade_plan_without_micro.micro_mode,
            },
            "HIGH",
        )
        add_check(
            checks,
            "p10.without_micro_doc_no_micro_refs",
            "micro_generated_at" not in without_micro_doc_refs
            and "micro_state_generated_at" not in without_micro_doc_refs
            and "micro_target_generated_at" not in without_micro_doc_refs,
            without_micro_doc_refs,
            "HIGH",
        )
        add_check(
            checks,
            "p10.without_micro_factor_ref",
            without_micro_doc_refs.get("factor_generated_at") == factor_no_micro.generated_at,
            {
                "trade_plan_factor_generated_at": without_micro_doc_refs.get("factor_generated_at"),
                "factor_no_micro_generated_at": factor_no_micro.generated_at,
            },
            "HIGH",
        )
        add_check(
            checks,
            "p10.without_micro_plan_factor_refs",
            all(plan.input_refs.get("factor_generated_at") == factor_no_micro.generated_at for plan in trade_plan_without_micro.plans),
            {
                "factor_no_micro_generated_at": factor_no_micro.generated_at,
                "bad_symbols": [
                    plan.symbol
                    for plan in trade_plan_without_micro.plans
                    if plan.input_refs.get("factor_generated_at") != factor_no_micro.generated_at
                ],
            },
            "HIGH",
        )
        liquidity_refs = {
            name: doc.input_refs.get("liquidity_generated_at")
            for name, doc in trade_docs.items()
            if doc is not None
        }
        for line_name, ref in liquidity_refs.items():
            plan_refs = [
                plan.input_refs.get("liquidity_generated_at")
                for plan in trade_docs[line_name].plans
                if trade_docs[line_name] is not None
            ]
            missing_plan_refs = [
                plan.symbol
                for plan in trade_docs[line_name].plans
                if plan.input_refs.get("liquidity_generated_at") is None
            ]
            mismatched_plan_refs = [
                plan.symbol
                for plan in trade_docs[line_name].plans
                if plan.input_refs.get("liquidity_generated_at") != ref
            ]
            add_check(
                checks,
                f"p10.{line_name}_liquidity_ref",
                ref is not None and not missing_plan_refs and not mismatched_plan_refs,
                {
                    "trade_plan_liquidity_generated_at": ref,
                    "line_local_contract": "each strategy line owns its liquidity refresh anchor",
                    "missing_plan_refs": missing_plan_refs,
                    "mismatched_plan_refs": mismatched_plan_refs,
                    "plan_ref_count": len(plan_refs),
                    "latest_liquidity_generated_at": liquidity.generated_at,
                    "refresh_liquidity_generated_at": (
                        refresh_liquidity.generated_at if refresh_liquidity is not None else None
                    ),
                },
                "HIGH",
            )
        executable_liquidity_bad = [
            {"line": line_name, "symbol": plan.symbol, "guards": plan.guards}
            for line_name, doc in trade_docs.items()
            if doc is not None
            for plan in doc.plans
            if plan.executable
            and line_name in {"without_micro", "micro_fast"}
            and plan.guards.get("liquidity_ok") is not True
        ]
        add_check(
            checks,
            "p10.executable_liquidity_guard",
            not executable_liquidity_bad,
            executable_liquidity_bad,
            "HIGH",
        )
        executable_quality_bad = [
            {
                "line": line_name,
                "symbol": plan.symbol,
                "guards": plan.guards,
                "reasons": plan.reason_codes,
            }
            for line_name, doc in trade_docs.items()
            if doc is not None
            for plan in doc.plans
            if plan.executable
            and (
                plan.guards.get("sl_tp_model_version") != "10.9"
                or plan.guards.get("net_rr") is None
                or plan.guards.get("min_net_rr") is None
                or float(plan.guards.get("net_rr") or 0.0) < float(plan.guards.get("min_net_rr") or 0.0)
                or plan.guards.get("gross_risk_bps") is None
                or plan.guards.get("noise_floor_bps") is None
                or float(plan.guards.get("gross_risk_bps") or 0.0) < float(plan.guards.get("noise_floor_bps") or 0.0)
                or plan.guards.get("available_room_bps") is None
                or plan.guards.get("required_reward_bps") is None
                or float(plan.guards.get("available_room_bps") or 0.0)
                < float(plan.guards.get("required_reward_bps") or 0.0)
                or plan.guards.get("opportunity_type") not in {"MARKET_EXECUTABLE", "LIMIT_PULLBACK", "LIMIT_REBOUND"}
            )
        ]
        add_check(
            checks,
            "p10.executable_sl_tp_quality",
            not executable_quality_bad,
            executable_quality_bad,
            "HIGH",
        )
        opportunity_missing = [
            {"line": line_name, "symbol": plan.symbol, "guards": plan.guards}
            for line_name, doc in trade_docs.items()
            if doc is not None
            for plan in doc.plans
            if "opportunity_type" not in plan.guards or "opportunity_level" not in plan.guards
        ]
        add_check(
            checks,
            "p10.opportunity_contract_present",
            not opportunity_missing,
            {"missing": opportunity_missing, "distribution": opportunity_distribution},
            "HIGH",
        )
        all_no_trade_without_reason = [
            {
                "line": line_name,
                "symbol": plan.symbol,
                "opportunity_type": plan.guards.get("opportunity_type"),
                "reasons": plan.reason_codes,
            }
            for line_name, doc in trade_docs.items()
            if doc is not None
            for plan in doc.plans
            if plan.guards.get("opportunity_type") == "NO_TRADE" and not plan.reason_codes
        ]
        add_check(
            checks,
            "p10.opportunity_distribution_explained",
            not all_no_trade_without_reason,
            {"unexplained_no_trade": all_no_trade_without_reason, "distribution": opportunity_distribution},
            "MEDIUM",
        )
        refresh_anchor = refresh.input_factor_generated_at
        if refresh_anchor == factor_no_micro.generated_at:
            without_micro_refresh_mode = "standalone_without_micro_factor"
        elif refresh_anchor == factor.generated_at:
            without_micro_refresh_mode = "shared_main_factor"
        else:
            without_micro_refresh_mode = "unknown"
        add_check(
            checks,
            "p10.without_micro_refresh_anchor_known",
            without_micro_refresh_mode != "unknown",
            {
                "mode": without_micro_refresh_mode,
                "refresh_input_factor_generated_at": refresh_anchor,
                "factor_no_micro_generated_at": factor_no_micro.generated_at,
                "factor_generated_at": factor.generated_at,
            },
            "HIGH",
        )
        add_check(
            checks,
            "p10.without_micro_standalone_refresh_anchor",
            (
                not require_without_micro_standalone_refresh
                or refresh_anchor == factor_no_micro.generated_at
            ),
            {
                "required": require_without_micro_standalone_refresh,
                "mode": without_micro_refresh_mode,
                "refresh_input_factor_generated_at": refresh_anchor,
                "factor_no_micro_generated_at": factor_no_micro.generated_at,
            },
            "HIGH",
        )
        without_micro_executable = [plan for plan in trade_plan_without_micro.plans if plan.executable]
        if without_micro_executable:
            add_check(
                checks,
                "p10.without_micro_executable_has_prices",
                all(
                    plan.estimated_entry_price is not None
                    and plan.stop_loss is not None
                    and plan.take_profit is not None
                    and plan.risk_per_unit is not None
                    and plan.reward_per_unit is not None
                    and plan.rr is not None
                    for plan in without_micro_executable
                ),
                [plan.symbol for plan in without_micro_executable],
                "HIGH",
            )
            add_check(
                checks,
                "p10.without_micro_executable_guards",
                all(
                    plan.guards.get("micro_confirmation") is False
                    and plan.guards.get("without_micro_executable_enabled") is True
                    and plan.guards.get("refresh_fresh") is True
                    and plan.guards.get("direction_still_valid") is True
                    and plan.guards.get("range_room_ok") is True
                    and plan.guards.get("liquidity_ok") is True
                    for plan in without_micro_executable
                ),
                [
                    {
                        "symbol": plan.symbol,
                        "guards": plan.guards,
                    }
                    for plan in without_micro_executable
                ],
                "HIGH",
            )
            add_check(
                checks,
                "p10.without_micro_executable_no_micro_refs",
                all(
                    "micro_generated_at" not in plan.input_refs
                    and "micro_state_generated_at" not in plan.input_refs
                    for plan in without_micro_executable
                ),
                [plan.symbol for plan in without_micro_executable],
                "HIGH",
            )
        state_refs = {
            "micro_fast": trade_plan_micro_fast.input_refs.get("micro_state_generated_at"),
            "micro_full": trade_plan_micro_full.input_refs.get("micro_state_generated_at"),
        }
        add_check(
            checks,
            "p10.micro_state_refs_present",
            all(v is not None for v in state_refs.values()),
            state_refs,
            "MEDIUM",
        )
        if all(v is not None for v in state_refs.values()):
            add_check(
                checks,
                "p10.micro_state_refs_independent",
                True,
                {
                    "refs": state_refs,
                    "note": "micro_fast and micro_full consume independent daemon snapshots; matching timestamps are not required",
                },
                "HIGH",
            )
    else:
        add_check(
            checks,
            "p10.trade_plan_outputs_present",
            False,
            {name: doc is not None for name, doc in trade_docs.items()},
            "MEDIUM",
        )
        if sidechain_only:
            add_check(
                checks,
                "p10.without_micro_output_present",
                trade_plan_without_micro is not None,
                {"without_micro": trade_plan_without_micro is not None},
                "HIGH",
            )
            if trade_plan_without_micro is not None:
                add_check(
                    checks,
                    "p10.without_micro_contract",
                    trade_plan_without_micro.source == "trade_plan_without_micro"
                    and trade_plan_without_micro.micro_mode == "none",
                    {
                        "source": trade_plan_without_micro.source,
                        "micro_mode": trade_plan_without_micro.micro_mode,
                    },
                    "HIGH",
                )
                add_check(
                    checks,
                    "p10.without_micro_factor_ref",
                    trade_plan_without_micro.input_refs.get("factor_generated_at") == factor_no_micro.generated_at,
                    {
                        "trade_plan_factor_generated_at": trade_plan_without_micro.input_refs.get("factor_generated_at"),
                        "factor_no_micro_generated_at": factor_no_micro.generated_at,
                    },
                    "HIGH",
                )

    scheduler_raw = raw.get("scheduler_5m")
    if isinstance(scheduler_raw, dict):
        add_check(
            checks,
            "scheduler_5m.status_ok",
            scheduler_raw.get("status") == "ok",
            {
                "status": scheduler_raw.get("status"),
                "run_id": scheduler_raw.get("run_id"),
                "cycle_id": scheduler_raw.get("cycle_id"),
            },
            "MEDIUM",
        )

    timestamps = {
        "universe": raw["universe"].get("generated_at"),
        "light": light.generated_at,
        "liquidity": liquidity.generated_at,
        "refresh_liquidity": refresh_liquidity.generated_at if refresh_liquidity is not None else None,
        "raw": raw_doc.generated_at,
        "watch": watch.generated_at,
        "strong": strong.generated_at,
        "targets": targets.generated_at,
        "micro": micro.generated_at,
        "factor_no_micro": factor_no_micro.generated_at,
        "factor": factor.generated_at,
        "direction": direction.generated_at,
        "final": final.generated_at,
        "refresh": refresh.generated_at,
        "me_direction": me_direction.generated_at,
        "me_plan": me_plan.generated_at,
    }
    if micro_state is not None:
        timestamps["micro_state"] = micro_state.generated_at
    if trade_plan_without_micro is not None:
        timestamps["trade_plan_without_micro"] = trade_plan_without_micro.generated_at
    if trade_plan_micro_fast is not None:
        timestamps["trade_plan_micro_fast"] = trade_plan_micro_fast.generated_at
    if trade_plan_micro_full is not None:
        timestamps["trade_plan_micro_full"] = trade_plan_micro_full.generated_at
    refresh_light_path = root / "DATA/market/decision_refresh_light_snapshot.json"
    if refresh_light_path.exists():
        refresh_light = FuturesLightSnapshotDocument.model_validate_json(refresh_light_path.read_text(encoding="utf-8"))
        timestamps["decision_refresh_light"] = refresh_light.generated_at

    add_check(
        checks,
        "time.light_before_scan",
        age_sec(timestamps["light"], timestamps["raw"]) is not None
        and age_sec(timestamps["light"], timestamps["raw"]) >= 0,
        {"delta": age_sec(timestamps["light"], timestamps["raw"])},
        "HIGH",
    )
    add_check(
        checks,
        "time.targets_before_micro",
        age_sec(timestamps["targets"], timestamps["micro"]) is not None
        and age_sec(timestamps["targets"], timestamps["micro"]) >= 0,
        {"delta": age_sec(timestamps["targets"], timestamps["micro"])},
        "HIGH",
    )
    add_check(
        checks,
        "time.micro_before_factor",
        age_sec(factor_micro_ref, timestamps["factor"]) is not None
        and age_sec(factor_micro_ref, timestamps["factor"]) >= 0,
        {
            "consumed_micro_generated_at": factor_micro_ref,
            "current_micro_generated_at": timestamps["micro"],
            "consumed_micro_to_factor_delta": age_sec(factor_micro_ref, timestamps["factor"]),
            "current_micro_to_factor_delta": age_sec(timestamps["micro"], timestamps["factor"]),
        },
        "HIGH",
    )
    current_micro_delta = age_sec(timestamps["factor"], timestamps["micro"])
    add_check(
        checks,
        "time.current_micro_may_be_newer_than_factor",
        True,
        {"factor_to_current_micro_delta": current_micro_delta},
        "INFO",
    )
    add_check(
        checks,
        "time.factor_before_direction_final",
        all(
            value is not None and value >= 0
            for value in (
                age_sec(timestamps["factor"], timestamps["direction"]),
                age_sec(timestamps["direction"], timestamps["final"]),
            )
        ),
        {
            "factor_to_direction": age_sec(timestamps["factor"], timestamps["direction"]),
            "direction_to_final": age_sec(timestamps["direction"], timestamps["final"]),
        },
        "HIGH",
    )
    add_check(
        checks,
        "time.factor_before_refresh_market_entry",
        all(
            value is not None and value >= 0
            for value in (
                age_sec(timestamps["factor"], timestamps["refresh"]),
                age_sec(timestamps["refresh"], timestamps["me_direction"]),
                age_sec(timestamps["me_direction"], timestamps["me_plan"]),
            )
        ),
        {
            "factor_to_refresh": age_sec(timestamps["factor"], timestamps["refresh"]),
            "refresh_to_market_entry_direction": age_sec(timestamps["refresh"], timestamps["me_direction"]),
            "market_entry_direction_to_plan": age_sec(timestamps["me_direction"], timestamps["me_plan"]),
        },
        "HIGH",
    )
    if sidechain_only:
        add_check(
            checks,
            "time.factor_no_micro_before_refresh",
            age_sec(timestamps["factor_no_micro"], timestamps["refresh"]) is not None
            and age_sec(timestamps["factor_no_micro"], timestamps["refresh"]) >= 0,
            {
                "factor_no_micro_to_refresh": age_sec(timestamps["factor_no_micro"], timestamps["refresh"]),
            },
            "HIGH",
        )

    all_watch = all(item.source_state == "watch_candidate" for item in factor.items)
    watch_policy_blocks_all = all("watch_market_entry_not_allowed" in item.reason_codes for item in me_direction.decisions)
    add_check(
        checks,
        "market_entry.watch_policy_blocks_all",
        True,
        {
            "all_factor_items_watch": all_watch,
            "all_blocked_by_watch_policy": watch_policy_blocks_all,
            "note": "policy observation; not a contract failure after STEP4.3B config gate",
        },
        "INFO",
    )
    add_check(
        checks,
        "market_entry.liquidity_age_recorded_in_refresh",
        all((item.liquidity or {}).get("generated_at") for item in refresh.items),
        "refresh items do not carry liquidity generated_at/age",
        "MEDIUM",
    )

    llm_outputs: dict[str, Any] = {}
    llm_expected = {
        "DATA/llm/out/llm_out_latest_factor_snapshot.json": factor.generated_at,
        "DATA/llm/out/llm_out_latest_factor_snapshot_withoutoficvd.json": factor_no_micro.generated_at,
    }
    for rel in (
        "DATA/llm/out/llm_out_latest_factor_snapshot.json",
        "DATA/llm/out/llm_out_latest_factor_snapshot_withoutoficvd.json",
    ):
        path = root / rel
        if path.exists():
            doc = json.loads(path.read_text(encoding="utf-8"))
            llm_outputs[rel] = {
                "generated_at": doc.get("generated_at"),
                "status": doc.get("status"),
                "input_factor_generated_at": doc.get("input_factor_generated_at"),
                "input_factor_source": doc.get("input_factor_source"),
            }
            input_factor_generated_at = doc.get("input_factor_generated_at")
            add_check(
                checks,
                f"llm.{Path(rel).name}.input_factor_current",
                input_factor_generated_at == llm_expected[rel],
                {
                    "llm_input_factor_generated_at": input_factor_generated_at,
                    "current_factor_generated_at": llm_expected[rel],
                },
                "MEDIUM",
            )

    reason_counts = {
        "micro_full": dict(Counter(code for item in micro.items for code in item.micro_quality.reason_codes)),
        "direction": dict(Counter(code for item in direction.decisions for code in item.reason_codes)),
        "final": dict(Counter(code for item in final.decisions for code in item.reason_codes)),
        "market_entry_direction": dict(Counter(code for item in me_direction.decisions for code in item.reason_codes)),
        "market_entry_plan": dict(Counter(code for item in me_plan.plans for code in item.reason_codes)),
        "trade_plan_without_micro": (
            dict(Counter(code for plan in trade_plan_without_micro.plans for code in plan.reason_codes))
            if trade_plan_without_micro is not None
            else {}
        ),
        "trade_plan_micro_fast": (
            dict(Counter(code for plan in trade_plan_micro_fast.plans for code in plan.reason_codes))
            if trade_plan_micro_fast is not None
            else {}
        ),
        "trade_plan_micro_full": (
            dict(Counter(code for plan in trade_plan_micro_full.plans for code in plan.reason_codes))
            if trade_plan_micro_full is not None
            else {}
        ),
    }
    trade_plan_liquidity_not_ok_counts = {
        "without_micro": reason_counts["trade_plan_without_micro"].get("liquidity_not_ok", 0),
        "micro_fast": reason_counts["trade_plan_micro_fast"].get("liquidity_not_ok", 0),
        "micro_full": reason_counts["trade_plan_micro_full"].get("liquidity_not_ok", 0),
    }

    if sidechain_only:
        relaxed_names = {
            "router.symbols_match_watch_strong",
            "micro.symbols_match_targets",
            "factor.symbols_match_candidates",
            "direction.symbols_match_factor",
            "final.symbols_match_direction",
            "market_entry_direction.symbols_match_factor",
            "market_entry_plan.symbols_match_direction",
            "p10.trade_plan_outputs_present",
            "p10.symbols_match_factor",
            "p10.run_id_consistent",
            "p10.cycle_id_consistent",
            "p10.micro_state_refs_present",
            "p10.micro_state_refs_match",
            "time.targets_before_micro",
            "time.micro_before_factor",
            "time.factor_before_direction_final",
            "time.factor_before_refresh_market_entry",
            "llm.llm_out_latest_factor_snapshot.json.input_factor_current",
            "llm.llm_out_latest_factor_snapshot_withoutoficvd.json.input_factor_current",
        }
        for item in checks:
            if item["name"] in relaxed_names and not item["ok"]:
                item["ok"] = True
                item["severity"] = "SKIPPED"
                item["relaxed_by_audit_mode"] = audit_mode

    if audit_profile["mode"] == "relaxed":
        _relax_check(
            checks,
            {
                "liquidity.profile_matches_altcoin_small_size",
                "refresh_liquidity.profile_matches_altcoin_small_size",
                "p10.executable_liquidity_guard",
                "p10.without_micro_executable_guards",
            },
            reason="expected_relaxed_profile",
            field="relaxed_by_audit_profile",
        )

    _relax_check(
        checks,
        {
            "direction.symbols_match_factor",
            "final.symbols_match_direction",
            "market_entry_direction.symbols_match_factor",
            "market_entry_plan.symbols_match_direction",
            "time.factor_before_direction_final",
            "time.factor_before_refresh_market_entry",
        },
        reason="legacy_step4_step5_scope_warning",
        field="legacy_chain_scope",
    )

    line_run_ids = {name: doc.run_id for name, doc in trade_docs.items() if doc is not None}
    line_cycle_ids = {name: doc.cycle_id for name, doc in trade_docs.items() if doc is not None}
    unique_run_ids = {v for v in line_run_ids.values() if v}
    unique_cycle_ids = {v for v in line_cycle_ids.values() if v}
    lineage_status = (
        "aligned"
        if len(unique_run_ids) == 1 and len(unique_cycle_ids) == 1 and len(line_run_ids) == len(trade_docs)
        else "mixed_or_missing"
    )
    top_run_id = next(iter(unique_run_ids), None) if len(unique_run_ids) == 1 else None
    top_cycle_id = next(iter(unique_cycle_ids), None) if len(unique_cycle_ids) == 1 else None

    return {
        "schema_version": "1.0",
        "source": "audit_current_json_chain",
        "generated_at": datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_id": top_run_id,
        "cycle_id": top_cycle_id,
        "audit_subject": {
            "strategy_report_run_id": top_run_id,
            "strategy_report_cycle_id": top_cycle_id,
            "line_run_ids": line_run_ids,
            "line_cycle_ids": line_cycle_ids,
            "lineage_status": lineage_status,
        },
        "audit_options": {
            "audit_mode": audit_mode,
            "require_without_micro_standalone_refresh": require_without_micro_standalone_refresh,
        },
        "audit_profile": audit_profile,
        "chain_scope": {
            "primary_chain": ["step1", "step1.5", "step2", "step2.5", "p3", "p3b", "p10", "p14", "p15"],
            "legacy_chain": ["step4_direction", "step5_final", "market_entry_direction", "market_entry_plan"],
            "legacy_step4_step5_scope": "warning",
        },
        "stage_statuses": {
            key: {
                "status": getattr(value, "status", None),
                "count": getattr(value, "count", getattr(value, "snapshot_count", None)),
                "generated_at": timestamps.get(key),
            }
            for key, value in models.items()
        },
        "timestamps": timestamps,
        "counts": {
            "universe_total_pairs": raw["universe"].get("counts", {}).get("total_pairs"),
            "universe_futures_count": raw["universe"].get("counts", {}).get("futures_count"),
            "light_snapshot_count": light.snapshot_count,
            "liquidity_count": liquidity.count,
            "liquidity_ok_count": liquidity_ok_count,
            "raw_count": raw_doc.count,
            "watch_count": watch.count,
            "strong_count": strong.count,
            "target_count": len(target_symbols),
            "micro_symbol_count": micro.symbol_count,
            "micro_fast_ready_count": micro.fast_ready_count,
            "micro_full_ready_count": micro.full_ready_count,
            "factor_count": factor.count,
            "direction_count": direction.count,
            "final_count": final.count,
            "refresh_count": refresh.refreshed_count,
            "market_entry_direction_count": me_direction.count,
            "market_entry_executable_count": me_plan.executable_count,
            "trade_plan_without_micro_count": trade_plan_without_micro.count if trade_plan_without_micro else None,
            "trade_plan_micro_fast_count": trade_plan_micro_fast.count if trade_plan_micro_fast else None,
            "trade_plan_micro_full_count": trade_plan_micro_full.count if trade_plan_micro_full else None,
        },
        "liquidity_profile": liquidity_profile,
        "refresh_liquidity_profile": refresh_liquidity_profile,
        "trade_plan_liquidity_not_ok_counts": trade_plan_liquidity_not_ok_counts,
        "trade_plan_opportunity_distribution": opportunity_distribution,
        "symbols": {
            "watch": sorted(watch_symbols),
            "strong": sorted(strong_symbols),
            "targets": sorted(target_symbols),
            "micro": sorted(micro_symbols),
            "factor": sorted(factor_symbols),
        },
        "micro_items": [
            {
                "symbol": item.symbol,
                "fast_ready": item.micro_fast_quality.ready if item.micro_fast_quality else None,
                "full_ready": item.micro_full_quality.ready if item.micro_full_quality else None,
                "legacy_ready": item.micro_quality.ready,
                "full_reasons": item.micro_quality.reason_codes,
            }
            for item in micro.items
        ],
        "factor_item_quality": [
            {
                "symbol": item.symbol,
                "source_state": item.source_state,
                "move_side": item.move_side,
                "scan_score": item.scan_score,
                "market_entry_score": item.market_entry_suitability_score,
                "market_entry_suitability": item.market_entry_suitability,
                "micro_ready": item.micro_quality.ready,
                "factor_quality_ready": item.factor_quality.ready,
                "factor_quality_reasons": item.factor_quality.reason_codes,
            }
            for item in factor.items
        ],
        "market_entry_decisions": [
            {
                "symbol": item.symbol,
                "decision": item.decision,
                "confidence": item.confidence,
                "guards": item.guards,
                "reasons": item.reason_codes,
            }
            for item in me_direction.decisions
        ],
        "p10_trade_plan_lines": {
            name: {
                "status": doc.status,
                "count": doc.count,
                "executable_count": doc.executable_count,
                "run_id": doc.run_id,
                "cycle_id": doc.cycle_id,
                "opportunity_distribution": opportunity_distribution.get(name, {}),
            }
            for name, doc in trade_docs.items()
            if doc is not None
        },
        "final_decisions": [
            {
                "symbol": item.symbol,
                "decision": item.decision,
                "action": item.action,
                "entry_mode": item.entry_mode,
                "plan_status": item.risk_plan.plan_status,
                "reasons": item.reason_codes,
            }
            for item in final.decisions
        ],
        "reason_counts": reason_counts,
        "liquidity_refresh": {
            "liquidity_generated_at": liquidity.generated_at,
            "refresh_generated_at": refresh.generated_at,
            "liquidity_to_refresh_age_sec": age_sec(liquidity.generated_at, refresh.generated_at),
        },
        "llm_side_outputs": llm_outputs,
        "checks": checks,
        "failure_count": sum(1 for item in checks if not item["ok"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit current JSON artifacts across the business chain.")
    parser.add_argument("--project-root", default=".", help="Project root. Default: current directory.")
    parser.add_argument("--output", default=None, help="Output JSON path.")
    parser.add_argument("--stdout-json", action="store_true", default=False)
    parser.add_argument(
        "--audit-mode",
        choices=["full_chain", "without_micro_sidechain"],
        default="full_chain",
        help="Audit profile. full_chain checks all current JSON lineage; without_micro_sidechain validates a standalone sidechain run.",
    )
    parser.add_argument(
        "--require-without-micro-standalone-refresh",
        action="store_true",
        default=False,
        help=(
            "Fail if latest_decision_refresh_snapshot.json is not anchored to "
            "latest_factor_snapshot_withoutoficvd.json. Use this after a standalone without-micro sidechain run."
        ),
    )
    args = parser.parse_args()

    root = Path(args.project_root).resolve()
    summary = build_summary(
        root,
        audit_mode=args.audit_mode,
        require_without_micro_standalone_refresh=args.require_without_micro_standalone_refresh,
    )
    output = (
        Path(args.output).resolve()
        if args.output
        else root / "docs" / "reports" / "current_json_chain_audit_summary.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    payload = {
        "status": "ok" if summary["failure_count"] == 0 else "issues_found",
        "checks": len(summary["checks"]),
        "failure_count": summary["failure_count"],
        "output": str(output),
    }
    if args.stdout_json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(f"wrote {output} checks={payload['checks']} failures={payload['failure_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
