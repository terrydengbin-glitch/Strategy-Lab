"""P10 independent without-micro / micro-fast / micro-full trade plan lines."""

from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import ValidationError

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.models import CandidateUniverseDocument
from laoma_signal_engine.core.symbol_contract import validate_exchange_symbol
from laoma_signal_engine.core.time_utils import age_sec_from_iso_z, to_iso_z, utc_now
from laoma_signal_engine.decision.trade_plan_archive import archive_trade_plan_line_payload
from laoma_signal_engine.decision.trade_plan_line_models import (
    TradePlanItem,
    TradePlanLineDocument,
    TradePlanLineSource,
)
from laoma_signal_engine.factors.models import FactorSnapshotDocument, FactorSnapshotItem
from laoma_signal_engine.market.decision_refresh_models import DecisionRefreshDocument, DecisionRefreshItem
from laoma_signal_engine.market.market_entry_liquidity_models import (
    MarketEntryLiquidityDocument,
    MarketEntryLiquidityItem,
)
from laoma_signal_engine.micro.assembly.models import (
    LatestMicroFeaturesDocument,
    Micro15mBlock,
    MicroFeatureItem,
    MicroSignalBlock,
)
from laoma_signal_engine.micro.data_quality_contract import build_micro_data_quality_contract
from laoma_signal_engine.micro.daemon.state_models import MicroDaemonStateDocument, MicroDaemonSymbolState

TradePlanLineName = Literal["without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"]
MICRO_CONSUMPTION_POLICIES = {"confirmed_only", "ready_signal_usable", "weak_ready_test", "audit_only"}
MICRO_WEAK_MIN_STATES = {"ready", "signal_usable"}
DEFAULT_MICRO_WEAK_BLOCK_REASONS = (
    "micro_direction_conflict",
    "micro_mixed_cvd_ofi",
    "data_quality_blocked",
    "PRICE_NO_RESPONSE",
    "SELL_ABSORPTION",
    "BUY_ABSORPTION",
)
TRADE_QUALITY_GATE_MODES = {"off", "shadow", "warn", "wait_only", "block_executable"}
SL_TP_QUALITY_MODES = {"off", "shadow", "warn", "apply"}
TP_TARGET_POLICY_MODES = {"structure", "fast_capped_rr", "structure_or_capped_rr"}
PROMOTION_GATE_RULE_TYPES = {"cost_liquidity", "symbol_quality_tier"}


def _universe_profiles_by_symbol(project_root: Path | None) -> dict[str, dict[str, Any]]:
    if project_root is None:
        return {}
    path = Path(project_root) / "DATA" / "universe" / "CANDIDATE_UNIVERSE.json"
    try:
        doc = CandidateUniverseDocument.model_validate(read_json_object(path))
    except (OSError, TypeError, ValueError, ValidationError):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in doc.pairs:
        if not row.futures_symbol:
            continue
        out[row.futures_symbol.upper()] = {
            "universe_profile": row.universe_profile.model_dump(mode="json"),
            "risk_profile": row.risk_profile.model_dump(mode="json"),
        }
    return out


def _light_profiles_by_symbol(project_root: Path | None) -> dict[str, dict[str, Any]]:
    if project_root is None:
        return {}
    path = Path(project_root) / "DATA" / "market" / "futures_light_snapshot.json"
    try:
        raw = read_json_object(path)
    except (OSError, TypeError, ValueError):
        return {}
    items = raw.get("items") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in items:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").upper().strip()
        if not sym:
            continue
        out[sym] = {
            "tradability_profile": row.get("tradability_profile") or {},
            "primary_pool": row.get("primary_pool") or "unknown",
            "pool_tags": list(row.get("pool_tags") or []),
            "light_snapshot_generated_at": raw.get("generated_at"),
        }
    return out


def _governance_profiles_by_symbol(project_root: Path | None) -> dict[str, dict[str, Any]]:
    profiles = _universe_profiles_by_symbol(project_root)
    for sym, light in _light_profiles_by_symbol(project_root).items():
        profiles.setdefault(sym, {}).update(light)
    return profiles


def _quality_bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
    sample_count = len(rows)
    total_r = sum(float(row.get("net_R") or 0.0) for row in rows)
    mfe_vals = [float(row.get("MFE_R")) for row in rows if row.get("MFE_R") is not None]
    mae_vals = [float(row.get("MAE_R")) for row in rows if row.get("MAE_R") is not None]
    causes: dict[str, dict[str, Any]] = {}
    for row in rows:
        cause = str(row.get("root_cause_label") or "unknown")
        got = causes.setdefault(cause, {"sample_count": 0, "total_R": 0.0})
        got["sample_count"] += 1
        got["total_R"] += float(row.get("net_R") or 0.0)
    for got in causes.values():
        got["avg_R"] = got["total_R"] / got["sample_count"] if got["sample_count"] else None
    worst_cause = ""
    if causes:
        worst_cause = min(causes.items(), key=lambda item: float(item[1].get("total_R") or 0.0))[0]
    return {
        "sample_count": sample_count,
        "total_R": total_r,
        "avg_R": total_r / sample_count if sample_count else None,
        "avg_MFE_R": sum(mfe_vals) / len(mfe_vals) if mfe_vals else None,
        "avg_MAE_R": sum(mae_vals) / len(mae_vals) if mae_vals else None,
        "worst_root_cause": worst_cause,
        "root_causes": causes,
    }


def _trade_quality_prior_for_symbols(
    project_root: Path | None,
    *,
    line: TradePlanLineName,
    symbols: set[str],
) -> dict[str, Any]:
    if project_root is None or not symbols:
        return {"available": False, "symbols": {}, "sides": {}, "root_causes": {}, "promotions": {}, "source": "none"}
    db_path = Path(project_root) / "DATA" / "paper" / "paper_trading.db"
    if not db_path.is_file():
        return {"available": False, "symbols": {}, "sides": {}, "root_causes": {}, "promotions": {}, "source": str(db_path)}
    placeholders = ",".join("?" for _ in symbols)
    sql = (
        "SELECT symbol, side, root_cause_label, net_R, MFE_R, MAE_R "
        "FROM trade_quality_samples "
        f"WHERE strategy_line = ? AND symbol IN ({placeholders})"
    )
    params: list[Any] = [line, *sorted(symbols)]
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_quality_samples'",
            ).fetchone()
            if table is None:
                return {"available": False, "symbols": {}, "sides": {}, "root_causes": {}, "promotions": {}, "source": str(db_path)}
            rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    except sqlite3.Error:
        return {"available": False, "symbols": {}, "sides": {}, "root_causes": {}, "promotions": {}, "source": str(db_path)}

    by_symbol: dict[str, list[dict[str, Any]]] = {}
    by_side: dict[str, list[dict[str, Any]]] = {}
    by_root: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        sym = _key(str(row.get("symbol") or ""))
        side = str(row.get("side") or "").upper()
        root = str(row.get("root_cause_label") or "unknown")
        by_symbol.setdefault(sym, []).append(row)
        by_side.setdefault(side, []).append(row)
        by_root.setdefault(root, []).append(row)
    return {
        "available": True,
        "source": str(db_path),
        "sample_count": len(rows),
        "symbols": {sym: _quality_bucket(got) for sym, got in by_symbol.items()},
        "sides": {side: _quality_bucket(got) for side, got in by_side.items()},
        "root_causes": {root: _quality_bucket(got) for root, got in by_root.items()},
        "promotions": _trade_quality_promotions_for_symbols(project_root, line=line, symbols=symbols),
    }


def _active_config_profile(project_root: Path | None) -> str:
    if project_root is None:
        return "custom"
    path = Path(project_root) / "laoma_signal_engine" / "config" / "default.yaml"
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return "custom"
    return str(data.get("active_profile") or "custom")


def _trade_quality_promotions_for_symbols(
    project_root: Path | None,
    *,
    line: TradePlanLineName,
    symbols: set[str],
) -> dict[str, Any]:
    if project_root is None or not symbols:
        return {"available": False, "active_profile": "custom", "by_symbol": {}}
    db_path = Path(project_root) / "DATA" / "paper" / "paper_trading.db"
    active_profile = _active_config_profile(project_root)
    if not db_path.is_file():
        return {"available": False, "active_profile": active_profile, "by_symbol": {}}
    placeholders = ",".join("?" for _ in symbols)
    sql = f"""
        SELECT p.promotion_id, p.rule_id, p.profile, p.strategy_line AS promotion_strategy_line,
          p.mode AS promotion_mode, p.reason AS promotion_reason, p.updated_at AS promotion_updated_at,
          r.rule_type, r.scope_key, r.symbol, r.side, r.sample_source, r.sample_count, r.total_R,
          r.avg_R, r.win_rate, r.recommendation, r.severity, r.confidence, r.evidence_json
        FROM trade_quality_recommendation_promotions p
        JOIN trade_quality_recommendation_rules r ON p.rule_id = r.rule_id
        WHERE p.enabled=1
          AND p.mode='wait_only'
          AND r.rule_type IN ('cost_liquidity', 'symbol_quality_tier')
          AND (p.profile=? OR p.profile='all')
          AND (p.strategy_line IS NULL OR p.strategy_line=?)
          AND (r.strategy_line IS NULL OR r.strategy_line=?)
          AND (r.symbol IS NULL OR upper(r.symbol) IN ({placeholders}))
    """
    params: list[Any] = [active_profile, line, line, *sorted(symbols)]
    by_symbol: dict[str, list[dict[str, Any]]] = {sym: [] for sym in symbols}
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_quality_recommendation_promotions'",
            ).fetchone()
            if table is None:
                return {"available": False, "active_profile": active_profile, "by_symbol": {}}
            rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    except sqlite3.Error:
        return {"available": False, "active_profile": active_profile, "by_symbol": {}}
    for row in rows:
        sym = _key(str(row.get("symbol") or ""))
        evidence = {}
        try:
            evidence = json.loads(row.get("evidence_json") or "{}")
        except Exception:
            evidence = {}
        item = {k: v for k, v in row.items() if k != "evidence_json"}
        item["evidence"] = evidence
        if sym:
            by_symbol.setdefault(sym, []).append(item)
        else:
            for candidate in symbols:
                by_symbol.setdefault(candidate, []).append(item)
    return {"available": True, "active_profile": active_profile, "by_symbol": by_symbol}


def _non_executable_with_profile_reason(
    plan: TradePlanItem,
    *,
    guards: dict[str, Any],
    input_refs: dict[str, Any],
    reason: str,
    action: str = "WAIT",
    entry_mode: str = "WAIT_CONFIRMATION",
) -> TradePlanItem:
    return plan.model_copy(
        update={
            "action": action,
            "entry_mode": entry_mode,
            "executable": False,
            "position_sizing": None,
            "reason_codes": sorted(set([*plan.reason_codes, reason])),
            "guards": guards,
            "input_refs": input_refs,
        }
    )


def _apply_symbol_risk_contract(
    plan: TradePlanItem,
    profile: dict[str, Any] | None,
    *,
    cfg: "TradePlanLineConfig | None" = None,
) -> TradePlanItem:
    if not profile:
        return plan
    up = profile.get("universe_profile") if isinstance(profile, dict) else None
    rp = profile.get("risk_profile") if isinstance(profile, dict) else None
    tp = profile.get("tradability_profile") if isinstance(profile, dict) else None
    guards = dict(plan.guards)
    input_refs = dict(plan.input_refs)
    hydration_reasons: list[str] = []
    if isinstance(up, dict):
        business_pool = up.get("business_pool") or "unknown"
        scan_eligibility = up.get("scan_eligibility") or "ignore"
        if business_pool == "unknown":
            hydration_reasons.append("business_pool_missing")
        if scan_eligibility == "ignore":
            hydration_reasons.append("scan_eligibility_missing")
        guards["universe_tier"] = up.get("universe_tier")
        guards["scan_tier"] = up.get("scan_tier")
        guards["business_pool"] = business_pool
        guards["scan_eligibility"] = scan_eligibility
        guards["manual_mode"] = up.get("manual_mode") or ""
        guards["symbol_risk_tags"] = list(up.get("symbol_risk_tags") or [])
        input_refs["universe_profile"] = up
    if isinstance(rp, dict):
        if (rp.get("execution_tier") or "unknown") == "unknown":
            hydration_reasons.append("execution_tier_missing")
        if not (rp.get("sl_template") and rp.get("rr_template") and rp.get("sizing_template")):
            hydration_reasons.append("risk_template_missing")
        guards["symbol_execution_tier"] = rp.get("execution_tier")
        guards["symbol_liquidity_tier"] = rp.get("liquidity_tier")
        guards["symbol_rr_policy"] = rp.get("rr_policy")
        guards["sl_template"] = rp.get("sl_template") or "normal"
        guards["rr_template"] = rp.get("rr_template") or "standard"
        guards["sizing_template"] = rp.get("sizing_template") or "normal"
        guards["feishu_policy"] = rp.get("feishu_policy") or "send"
        input_refs["risk_profile"] = rp
        if rp.get("execution_tier") == "no_trade" or rp.get("sizing_template") == "disabled":
            hydration_status = "ok" if not sorted(set(hydration_reasons)) else "incomplete"
            guards["profile_hydration_status"] = hydration_status
            guards["profile_hydration_reason_codes"] = sorted(set(hydration_reasons))
            input_refs["profile_hydration"] = {
                "source": "candidate_universe",
                "status": hydration_status,
                "reason_codes": sorted(set(hydration_reasons)),
            }
            guards["trade_plan_consumable"] = False
            guards["symbol_risk_profile_blocked"] = True
            guards["symbol_risk_profile_block_reason"] = (
                "sizing_template_disabled" if rp.get("sizing_template") == "disabled" else "execution_tier_no_trade"
            )
            return plan.model_copy(
                update={
                    "action": "NO_TRADE",
                    "entry_mode": "NONE",
                    "executable": False,
                    "reason_codes": sorted(set([*plan.reason_codes, "symbol_execution_tier_no_trade"])),
                    "guards": guards,
                    "input_refs": input_refs,
                }
            )
        if rp.get("execution_tier") in {"watch_only", "no_market"} and plan.executable:
            hydration_status = "ok" if not sorted(set(hydration_reasons)) else "incomplete"
            guards["profile_hydration_status"] = hydration_status
            guards["profile_hydration_reason_codes"] = sorted(set(hydration_reasons))
            input_refs["profile_hydration"] = {
                "source": "candidate_universe",
                "status": hydration_status,
                "reason_codes": sorted(set(hydration_reasons)),
            }
            guards["trade_plan_consumable"] = False
            guards["symbol_risk_profile_blocked"] = True
            guards["symbol_risk_profile_block_reason"] = f"execution_tier_{rp.get('execution_tier')}"
            return _non_executable_with_profile_reason(
                plan,
                guards=guards,
                input_refs=input_refs,
                reason=f"symbol_execution_tier_{rp.get('execution_tier')}",
            )
    hydration_reasons = sorted(set(hydration_reasons))
    if isinstance(up, dict) or isinstance(rp, dict):
        hydration_status = "ok" if not hydration_reasons else "incomplete"
        guards["profile_hydration_status"] = hydration_status
        guards["profile_hydration_reason_codes"] = hydration_reasons
        input_refs["profile_hydration"] = {
            "source": "candidate_universe",
            "status": hydration_status,
            "reason_codes": hydration_reasons,
        }
    if isinstance(tp, dict):
        input_refs["tradability_profile"] = tp
        guards["primary_pool"] = profile.get("primary_pool") or "unknown"
        guards["pool_tags"] = list(profile.get("pool_tags") or [])
        guards["trade_quality_tier"] = tp.get("trade_quality_tier") or "unknown"
        guards["market_entry_score"] = tp.get("market_entry_score")
        guards["hf_stop_score"] = tp.get("hf_stop_score")
        guards["slippage_risk_score"] = tp.get("slippage_risk_score")
        guards["depth_stability_score"] = tp.get("depth_stability_score")
        input_refs["light_snapshot_generated_at"] = profile.get("light_snapshot_generated_at")
        trade_quality_tier = str(tp.get("trade_quality_tier") or "unknown")
        enforce_live_tradability = trade_quality_tier != "unknown" or bool(tp.get("tradability_score"))
        c = cfg or DEFAULT_CONFIGS.get(plan.line, DEFAULT_CONFIGS["without_micro"])
        guards["profile_gate_enabled"] = c.profile_gate_enabled
        guards["min_profile_market_entry_score"] = c.min_profile_market_entry_score
        guards["min_profile_hf_stop_score"] = c.min_profile_hf_stop_score
        guards["max_profile_slippage_risk_score"] = c.max_profile_slippage_risk_score
        try:
            market_entry_score = int(tp.get("market_entry_score") or 0)
            hf_stop_score = int(tp.get("hf_stop_score") or 0)
            slippage_risk_score = int(tp.get("slippage_risk_score") or 0)
        except (TypeError, ValueError):
            market_entry_score = 0
            hf_stop_score = 0
            slippage_risk_score = 100
        if (
            c.profile_gate_enabled
            and enforce_live_tradability
            and plan.executable
            and market_entry_score < c.min_profile_market_entry_score
        ):
            guards["trade_plan_consumable"] = False
            guards["symbol_tradability_profile_blocked"] = True
            guards["symbol_tradability_profile_block_reason"] = "market_entry_score_too_low"
            return _non_executable_with_profile_reason(
                plan,
                guards=guards,
                input_refs=input_refs,
                reason="profile_market_entry_score_too_low",
            )
        if (
            c.profile_gate_enabled
            and enforce_live_tradability
            and plan.executable
            and c.min_profile_hf_stop_score > 0
            and hf_stop_score < c.min_profile_hf_stop_score
        ):
            guards["trade_plan_consumable"] = False
            guards["symbol_tradability_profile_blocked"] = True
            guards["symbol_tradability_profile_block_reason"] = "hf_stop_score_too_low"
            return _non_executable_with_profile_reason(
                plan,
                guards=guards,
                input_refs=input_refs,
                reason="profile_hf_stop_score_too_low",
            )
        if (
            c.profile_gate_enabled
            and enforce_live_tradability
            and plan.executable
            and slippage_risk_score > c.max_profile_slippage_risk_score
        ):
            guards["trade_plan_consumable"] = False
            guards["symbol_tradability_profile_blocked"] = True
            guards["symbol_tradability_profile_block_reason"] = "slippage_risk_too_high"
            return _non_executable_with_profile_reason(
                plan,
                guards=guards,
                input_refs=input_refs,
                reason="profile_slippage_risk_too_high",
            )
    return plan.model_copy(update={"guards": guards, "input_refs": input_refs})


def _ensure_profile_guard_contract(
    plan: TradePlanItem,
    profile: dict[str, Any] | None,
    *,
    cfg: "TradePlanLineConfig | None" = None,
) -> TradePlanItem:
    """Keep Step1/1.5 governance fields explicit for downstream audit consumers."""
    guards = dict(plan.guards)
    input_refs = dict(plan.input_refs)
    c = cfg or DEFAULT_CONFIGS.get(plan.line, DEFAULT_CONFIGS["without_micro"])

    profile = profile if isinstance(profile, dict) else {}
    up = profile.get("universe_profile") if isinstance(profile.get("universe_profile"), dict) else {}
    rp = profile.get("risk_profile") if isinstance(profile.get("risk_profile"), dict) else {}
    tp = profile.get("tradability_profile") if isinstance(profile.get("tradability_profile"), dict) else {}

    guards.setdefault("business_pool", up.get("business_pool") or "unknown")
    guards.setdefault("scan_eligibility", up.get("scan_eligibility") or "ignore")
    guards.setdefault("symbol_execution_tier", rp.get("execution_tier") or "unknown")
    guards.setdefault("symbol_liquidity_tier", rp.get("liquidity_tier") or "unknown")
    guards.setdefault("symbol_rr_policy", rp.get("rr_policy") or "unknown")
    guards.setdefault("sl_template", rp.get("sl_template") or "normal")
    guards.setdefault("rr_template", rp.get("rr_template") or "standard")
    guards.setdefault("sizing_template", rp.get("sizing_template") or "normal")
    guards.setdefault("feishu_policy", rp.get("feishu_policy") or "send")

    guards.setdefault("primary_pool", profile.get("primary_pool") or "unknown")
    guards.setdefault("pool_tags", list(profile.get("pool_tags") or []))
    guards.setdefault("trade_quality_tier", tp.get("trade_quality_tier") or "unknown")
    guards.setdefault("market_entry_score", tp.get("market_entry_score"))
    guards.setdefault("hf_stop_score", tp.get("hf_stop_score"))
    guards.setdefault("slippage_risk_score", tp.get("slippage_risk_score"))
    guards.setdefault("depth_stability_score", tp.get("depth_stability_score"))

    guards.setdefault("profile_gate_enabled", c.profile_gate_enabled)
    guards.setdefault("min_profile_market_entry_score", c.min_profile_market_entry_score)
    guards.setdefault("min_profile_hf_stop_score", c.min_profile_hf_stop_score)
    guards.setdefault("max_profile_slippage_risk_score", c.max_profile_slippage_risk_score)

    hydration_reasons: list[str] = []
    if not up:
        hydration_reasons.append("universe_profile_missing")
    if not rp:
        hydration_reasons.append("risk_profile_missing")
    if not tp:
        hydration_reasons.append("tradability_profile_missing")
    if "profile_hydration_status" not in guards:
        guards["profile_hydration_status"] = "ok" if not hydration_reasons else "missing"
    if "profile_hydration_reason_codes" not in guards:
        guards["profile_hydration_reason_codes"] = hydration_reasons
    input_refs.setdefault(
        "profile_hydration",
        {
            "source": "candidate_universe+futures_light_snapshot",
            "status": guards.get("profile_hydration_status"),
            "reason_codes": list(guards.get("profile_hydration_reason_codes") or []),
        },
    )
    if up:
        input_refs.setdefault("universe_profile", up)
    if rp:
        input_refs.setdefault("risk_profile", rp)
    if tp:
        input_refs.setdefault("tradability_profile", tp)
    if profile.get("light_snapshot_generated_at"):
        input_refs.setdefault("light_snapshot_generated_at", profile.get("light_snapshot_generated_at"))

    return plan.model_copy(update={"guards": guards, "input_refs": input_refs})


@dataclass(frozen=True)
class TradePlanLineConfig:
    allow_market_entry: bool
    allow_wait_plan: bool
    min_score: int
    require_refresh_fresh: bool
    require_direction_still_valid: bool
    require_range_room_ok: bool
    require_liquidity_ok: bool
    require_micro_ready: bool
    require_micro_alignment: bool
    max_refresh_age_sec: int
    max_liquidity_age_sec: int
    max_micro_age_sec: int
    target_rr: float
    min_rr: float
    stop_atr_mult: float
    max_stop_atr_mult: float
    min_stop_bps: float = 50.0
    preferred_stop_bps: float = 80.0
    max_stop_bps: float = 180.0
    min_net_rr: float = 1.2
    min_effective_rr: float = 1.0
    min_reachable_reward_bps: float = 12.0
    tp_target_policy_mode: str = "structure"
    tp_target_policy_target_rr: float | None = None
    tp_target_policy_target_rr_cap: float | None = None
    tp_target_policy_target_rr_basis: str = "gross"
    tp_target_policy_target_net_rr: float | None = None
    tp_target_policy_min_target_net_rr: float = 0.25
    tp_target_policy_max_target_net_rr: float = 3.0
    tp_target_policy_min_reward_bps: float = 8.0
    tp_target_policy_require_market_room: bool = True
    tp_target_policy_market_room_buffer_bps: float = 2.0
    tp_target_policy_allow_structure_runner: bool = False
    tp_target_policy_reward_to_spread_min: float = 2.5
    tp_target_policy_include_entry_fee: bool = True
    tp_target_policy_include_exit_fee: bool = True
    tp_target_policy_include_slippage_reserve: bool = False
    tp_target_policy_slippage_reserve_bps: float = 0.0
    tp_target_policy_max_loss_net_r: float = 1.10
    tp_target_policy_sizing_basis: str = "gross_stop"
    allow_fallback_target_for_executable: bool = False
    min_tp_after_cost_bps: float = 20.0
    taker_fee_bps: float = 5.0
    maker_fee_bps: float = 2.0
    atr_1m_mult: float = 1.4
    atr_5m_mult: float = 0.8
    allow_limit_pullback: bool = True
    allow_breakout_trigger: bool = True
    allow_market_now: bool = True
    max_pullback_bps: float = 100.0
    conditional_plan_expire_sec: int = 300
    require_micro_symbol_lifecycle_confirmed: bool = True
    micro_consumption_policy: str = "confirmed_only"
    allow_weak_micro_consumption: bool = False
    weak_micro_min_state: str = "ready"
    weak_micro_require_signal_usable: bool = True
    weak_micro_require_direction_not_conflict: bool = True
    weak_micro_block_reasons: tuple[str, ...] = DEFAULT_MICRO_WEAK_BLOCK_REASONS
    position_sizing_enabled: bool = True
    position_sizing_method: str = "fixed_risk"
    planned_loss_guard_enabled: bool = True
    planned_loss_sizing_policy: str = "notional_by_loss_cap"
    base_notional_usdt: float = 2000.0
    target_planned_loss_usdt: float = 50.0
    max_planned_loss_usdt: float = 80.0
    allow_notional_resize: bool = True
    paper_fallback_notional_allowed: bool = False
    account_equity_usdt: float = 1000.0
    default_leverage: float = 20.0
    risk_budget_usdt: float = 10.0
    risk_pct_equity: float = 0.01
    min_risk_budget_usdt: float = 3.0
    max_risk_budget_usdt: float = 20.0
    max_margin_usdt: float = 100.0
    min_notional_usdt: float = 20.0
    max_notional_usdt: float = 2000.0
    include_fee_in_risk_budget: bool = True
    reject_if_capped_below_min_risk: bool = True
    short_now_calibration_enabled: bool = False
    short_now_min_range_pos: float = 0.18
    short_now_max_range_pos: float = 0.82
    short_now_min_available_room_bps: float = 45.0
    short_now_max_stop_bps: float = 420.0
    short_now_max_stop_atr_mult: float = 4.0
    short_now_min_net_rr: float = 0.75
    short_now_allow_if_liquidity_missing: bool = False
    short_now_max_spread_bps: float = 80.0
    short_now_max_slippage_bps: float = 150.0
    short_now_require_recent_down_impulse: bool = True
    short_now_reject_if_rebound_required: bool = True
    market_now_calibration_enabled: bool = False
    market_now_legacy_short_now_fallback: bool = True
    long_now_min_range_pos: float = 0.18
    long_now_max_range_pos: float = 0.82
    long_now_min_available_room_bps: float = 45.0
    long_now_max_stop_bps: float = 420.0
    long_now_max_stop_atr_mult: float = 4.0
    long_now_min_net_rr: float = 0.75
    long_now_allow_if_liquidity_missing: bool = False
    long_now_max_spread_bps: float = 80.0
    long_now_max_slippage_bps: float = 150.0
    long_now_require_recent_up_impulse: bool = True
    long_now_reject_if_pullback_required: bool = True
    profile_gate_enabled: bool = True
    min_profile_market_entry_score: int = 35
    min_profile_hf_stop_score: int = 0
    max_profile_slippage_risk_score: int = 80
    trade_quality_gate_enabled: bool = False
    trade_quality_gate_mode: str = "off"
    trade_quality_gate_min_samples_per_symbol: int = 3
    trade_quality_gate_min_samples_per_root_cause: int = 5
    trade_quality_gate_max_negative_expectancy_R: float = -0.6
    trade_quality_gate_signal_no_edge_wait_enabled: bool = True
    trade_quality_gate_side_specific_enabled: bool = True
    sl_tp_quality_enabled: bool = False
    sl_tp_quality_mode: str = "off"
    sl_tp_quality_single_tp_only: bool = True
    sl_tp_quality_min_samples_per_cluster: int = 5
    sl_tp_quality_stop_too_tight_widen_factor: float = 1.15
    sl_tp_quality_tp_too_far_reduce_factor: float = 0.90
    sl_tp_quality_entered_too_early_wait_enabled: bool = True


DEFAULT_CONFIGS: dict[TradePlanLineName, TradePlanLineConfig] = {
    "without_micro": TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=75,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=True,
        require_liquidity_ok=True,
        require_micro_ready=False,
        require_micro_alignment=False,
        max_refresh_age_sec=180,
        max_liquidity_age_sec=180,
        max_micro_age_sec=0,
        target_rr=1.25,
        min_rr=1.0,
        stop_atr_mult=1.2,
        max_stop_atr_mult=2.2,
        min_stop_bps=70.0,
        preferred_stop_bps=100.0,
        max_stop_bps=180.0,
        min_net_rr=1.30,
        min_effective_rr=1.20,
        min_reachable_reward_bps=18.0,
        min_tp_after_cost_bps=25.0,
        allow_limit_pullback=False,
        allow_breakout_trigger=False,
        allow_market_now=True,
        max_pullback_bps=120.0,
        conditional_plan_expire_sec=300,
    ),
    "micro_fast": TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=65,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=True,
        require_liquidity_ok=True,
        require_micro_ready=True,
        require_micro_alignment=True,
        max_refresh_age_sec=180,
        max_liquidity_age_sec=180,
        max_micro_age_sec=180,
        target_rr=1.25,
        min_rr=1.0,
        stop_atr_mult=1.2,
        max_stop_atr_mult=2.2,
        min_stop_bps=50.0,
        preferred_stop_bps=80.0,
        max_stop_bps=160.0,
        min_net_rr=1.20,
        min_effective_rr=1.00,
        min_reachable_reward_bps=12.0,
        min_tp_after_cost_bps=20.0,
        allow_limit_pullback=False,
        allow_breakout_trigger=False,
        allow_market_now=True,
        max_pullback_bps=90.0,
        conditional_plan_expire_sec=180,
    ),
    "micro_full": TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=60,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=False,
        require_liquidity_ok=False,
        require_micro_ready=True,
        require_micro_alignment=True,
        max_refresh_age_sec=300,
        max_liquidity_age_sec=300,
        max_micro_age_sec=1500,
        target_rr=1.5,
        min_rr=1.0,
        stop_atr_mult=1.5,
        max_stop_atr_mult=2.5,
        min_stop_bps=80.0,
        preferred_stop_bps=120.0,
        max_stop_bps=240.0,
        min_net_rr=1.30,
        min_effective_rr=1.20,
        min_reachable_reward_bps=18.0,
        min_tp_after_cost_bps=30.0,
        allow_limit_pullback=False,
        allow_breakout_trigger=False,
        allow_market_now=True,
        max_pullback_bps=150.0,
        conditional_plan_expire_sec=600,
    ),
}


def _read_micro_target_lineage(project_root: Path) -> dict[str, Any]:
    path = project_root / "DATA" / "micro" / "micro_targets.json"
    try:
        raw = read_json_object(path)
    except (OSError, ValueError, TypeError):
        return {}
    symbols = raw.get("target_symbols")
    if not isinstance(symbols, list):
        symbols = []
        for key in ("tier1_warm_watch", "tier2_active_strong"):
            rows = raw.get(key)
            if not isinstance(rows, list):
                continue
            for row in rows:
                if isinstance(row, dict) and row.get("symbol"):
                    symbols.append(str(row["symbol"]).upper())
    return {
        "micro_target_set_id": str(raw.get("target_set_id") or ""),
        "micro_candidate_hash": str(raw.get("candidate_hash") or ""),
        "micro_target_generated_at": str(raw.get("generated_at") or ""),
        "micro_ready_scope": "target_set",
        "micro_target_count": int(raw.get("target_count") or len(symbols)),
    }


def _read_micro_target_items(project_root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = project_root / "DATA" / "micro" / "micro_targets.json"
    try:
        raw = read_json_object(path)
    except (OSError, ValueError, TypeError):
        return [], {}
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for key in ("tier1_warm_watch", "tier2_active_strong"):
        rows = raw.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict) or not row.get("symbol"):
                continue
            symbol = _key(str(row["symbol"]))
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            got = dict(row)
            got["symbol"] = symbol
            got["tier"] = key
            items.append(got)
    if not items:
        symbols = raw.get("target_symbols")
        if isinstance(symbols, list):
            for raw_symbol in symbols:
                symbol = _key(str(raw_symbol))
                if not symbol or symbol in seen:
                    continue
                seen.add(symbol)
                items.append({"symbol": symbol, "tier": "target_symbols"})
    return items, raw if isinstance(raw, dict) else {}

LINE_SOURCE: dict[TradePlanLineName, TradePlanLineSource] = {
    "without_micro": "trade_plan_without_micro",
    "micro_fast": "trade_plan_micro_fast",
    "micro_full": "trade_plan_micro_full",
    "strategy4": "trade_plan_strategy4",
    "strategy5": "trade_plan_strategy5",
    "strategy6": "trade_plan_strategy6",
}


def _without_micro_like(line: str) -> bool:
    return line in {"without_micro", "strategy4", "strategy5", "strategy6"}


def _default_factor_path(root: Path, line: TradePlanLineName) -> Path:
    if _without_micro_like(line):
        return root / "DATA" / "factors" / "latest_factor_snapshot_withoutoficvd.json"
    return root / "DATA" / "factors" / "latest_factor_snapshot.json"


def _default_refresh_path(root: Path, line: TradePlanLineName | None = None) -> Path:
    if line:
        got = root / "DATA" / "market" / f"latest_decision_refresh_{line}_snapshot.json"
        if got.is_file():
            return got
    return root / "DATA" / "market" / "latest_decision_refresh_snapshot.json"


def _default_liquidity_path(root: Path, line: TradePlanLineName | None = None) -> Path:
    if line:
        line_refreshed = root / "DATA" / "market" / f"decision_refresh_{line}_liquidity_snapshot.json"
        if line_refreshed.is_file():
            return line_refreshed
    refreshed = root / "DATA" / "market" / "decision_refresh_liquidity_snapshot.json"
    if refreshed.is_file():
        return refreshed
    return root / "DATA" / "market" / "latest_market_entry_liquidity.json"


def _default_micro_path(root: Path) -> Path:
    return root / "DATA" / "micro" / "latest_micro_features.json"


def _default_micro_state_path(root: Path) -> Path:
    return root / "DATA" / "micro" / "latest_micro_state.json"


def _wait_pass_evidence_path(root: Path, line: TradePlanLineName) -> Path:
    return root / "DATA" / "micro" / "evidence" / f"latest_wait_pass_{line}.json"


def _blocked_micro_evidence_path(root: Path, line: TradePlanLineName) -> Path:
    return root / "DATA" / "micro" / "evidence" / f"latest_blocked_{line}.json"


def default_output_path(root: Path, line: TradePlanLineName) -> Path:
    filename = {
        "without_micro": "latest_trade_plan_without_micro.json",
        "micro_fast": "latest_trade_plan_micro_fast.json",
        "micro_full": "latest_trade_plan_micro_full.json",
        "strategy4": "latest_trade_plan_strategy4.json",
        "strategy5": "latest_trade_plan_strategy5.json",
        "strategy6": "latest_trade_plan_strategy6.json",
    }[line]
    return root / "DATA" / "decisions" / filename


def load_trade_plan_line_config(project_root: Path, line: TradePlanLineName) -> TradePlanLineConfig:
    base_line = "without_micro" if line in {"strategy4", "strategy5", "strategy6"} else line
    base = DEFAULT_CONFIGS[base_line]
    cfg_path = project_root / "laoma_signal_engine" / "config" / "default.yaml"
    try:
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except OSError:
        raw = {}
    if line in {"strategy4", "strategy5", "strategy6"}:
        strategy_raw = raw.get(line) if isinstance(raw.get(line), dict) else {}
        inherit_from = str(strategy_raw.get("inherit_from") or "without_micro")
        inherit_frag = ((raw.get("trade_plan_lines") or {}).get(inherit_from)) or {}
        strategy_frag = ((raw.get("trade_plan_lines") or {}).get(line)) or {}
        frag = {**inherit_frag, **strategy_frag}
    else:
        frag = ((raw.get("trade_plan_lines") or {}).get(line)) or {}
    global_sizing = raw.get("position_sizing") if isinstance(raw.get("position_sizing"), dict) else {}
    global_trade_risk = raw.get("trade_plan_risk") if isinstance(raw.get("trade_plan_risk"), dict) else {}
    line_sizing = frag.get("position_sizing") if isinstance(frag.get("position_sizing"), dict) else {}
    line_trade_risk = frag.get("trade_plan_risk") if isinstance(frag.get("trade_plan_risk"), dict) else {}
    sizing = {**global_sizing, **line_sizing}
    trade_risk = {**global_trade_risk, **line_trade_risk}
    market_now = frag.get("market_now_calibration") if isinstance(frag.get("market_now_calibration"), dict) else {}
    market_now_long = market_now.get("long") if isinstance(market_now.get("long"), dict) else {}
    market_now_short = market_now.get("short") if isinstance(market_now.get("short"), dict) else {}
    legacy_short_now = frag.get("short_now_calibration") if isinstance(frag.get("short_now_calibration"), dict) else {}
    short_now = market_now_short or legacy_short_now
    trade_quality_gate = frag.get("trade_quality_gate") if isinstance(frag.get("trade_quality_gate"), dict) else {}
    sl_tp_quality = frag.get("sl_tp_quality") if isinstance(frag.get("sl_tp_quality"), dict) else {}
    tp_target_policy = frag.get("tp_target_policy") if isinstance(frag.get("tp_target_policy"), dict) else {}
    block_reasons = frag.get("weak_micro_block_reasons", base.weak_micro_block_reasons)
    if not isinstance(block_reasons, (list, tuple)):
        block_reasons = base.weak_micro_block_reasons
    return TradePlanLineConfig(
        allow_market_entry=bool(frag.get("allow_market_entry", base.allow_market_entry)),
        allow_wait_plan=bool(frag.get("allow_wait_plan", base.allow_wait_plan)),
        min_score=int(frag.get("min_score", base.min_score)),
        require_refresh_fresh=bool(frag.get("require_refresh_fresh", base.require_refresh_fresh)),
        require_direction_still_valid=bool(
            frag.get("require_direction_still_valid", base.require_direction_still_valid),
        ),
        require_range_room_ok=bool(frag.get("require_range_room_ok", base.require_range_room_ok)),
        require_liquidity_ok=bool(frag.get("require_liquidity_ok", base.require_liquidity_ok)),
        require_micro_ready=bool(frag.get("require_micro_ready", base.require_micro_ready)),
        require_micro_alignment=bool(frag.get("require_micro_alignment", base.require_micro_alignment)),
        max_refresh_age_sec=int(frag.get("max_refresh_age_sec", base.max_refresh_age_sec)),
        max_liquidity_age_sec=int(frag.get("max_liquidity_age_sec", base.max_liquidity_age_sec)),
        max_micro_age_sec=int(frag.get("max_micro_age_sec", base.max_micro_age_sec)),
        target_rr=float(frag.get("target_rr", base.target_rr)),
        min_rr=float(frag.get("min_rr", base.min_rr)),
        stop_atr_mult=float(frag.get("stop_atr_mult", base.stop_atr_mult)),
        max_stop_atr_mult=float(frag.get("max_stop_atr_mult", base.max_stop_atr_mult)),
        min_stop_bps=float(frag.get("min_stop_bps", base.min_stop_bps)),
        preferred_stop_bps=float(frag.get("preferred_stop_bps", base.preferred_stop_bps)),
        max_stop_bps=float(frag.get("max_stop_bps", base.max_stop_bps)),
        min_net_rr=float(frag.get("min_net_rr", base.min_net_rr)),
        min_effective_rr=float(frag.get("min_effective_rr", base.min_effective_rr)),
        min_reachable_reward_bps=float(
            frag.get("min_reachable_reward_bps", base.min_reachable_reward_bps),
        ),
        tp_target_policy_mode=str(tp_target_policy.get("mode", base.tp_target_policy_mode)),
        tp_target_policy_target_rr=(
            float(tp_target_policy["target_rr"])
            if tp_target_policy.get("target_rr") is not None
            else base.tp_target_policy_target_rr
        ),
        tp_target_policy_target_rr_cap=(
            float(tp_target_policy["target_rr_cap"])
            if tp_target_policy.get("target_rr_cap") is not None
            else base.tp_target_policy_target_rr_cap
        ),
        tp_target_policy_target_rr_basis=str(
            tp_target_policy.get("target_rr_basis", base.tp_target_policy_target_rr_basis),
        ),
        tp_target_policy_target_net_rr=(
            float(tp_target_policy["target_net_rr"])
            if tp_target_policy.get("target_net_rr") is not None
            else base.tp_target_policy_target_net_rr
        ),
        tp_target_policy_min_target_net_rr=float(
            tp_target_policy.get("min_target_net_rr", base.tp_target_policy_min_target_net_rr),
        ),
        tp_target_policy_max_target_net_rr=float(
            tp_target_policy.get("max_target_net_rr", base.tp_target_policy_max_target_net_rr),
        ),
        tp_target_policy_min_reward_bps=float(
            tp_target_policy.get("min_reward_bps", base.tp_target_policy_min_reward_bps),
        ),
        tp_target_policy_require_market_room=bool(
            tp_target_policy.get("require_market_room", base.tp_target_policy_require_market_room),
        ),
        tp_target_policy_market_room_buffer_bps=float(
            tp_target_policy.get(
                "market_room_buffer_bps",
                base.tp_target_policy_market_room_buffer_bps,
            ),
        ),
        tp_target_policy_allow_structure_runner=bool(
            tp_target_policy.get("allow_structure_runner", base.tp_target_policy_allow_structure_runner),
        ),
        tp_target_policy_reward_to_spread_min=float(
            tp_target_policy.get(
                "reward_to_spread_min",
                base.tp_target_policy_reward_to_spread_min,
            ),
        ),
        tp_target_policy_include_entry_fee=bool(
            tp_target_policy.get("include_entry_fee", base.tp_target_policy_include_entry_fee),
        ),
        tp_target_policy_include_exit_fee=bool(
            tp_target_policy.get("include_exit_fee", base.tp_target_policy_include_exit_fee),
        ),
        tp_target_policy_include_slippage_reserve=bool(
            tp_target_policy.get(
                "include_slippage_reserve",
                base.tp_target_policy_include_slippage_reserve,
            ),
        ),
        tp_target_policy_slippage_reserve_bps=float(
            tp_target_policy.get(
                "slippage_reserve_bps",
                base.tp_target_policy_slippage_reserve_bps,
            ),
        ),
        tp_target_policy_max_loss_net_r=float(
            tp_target_policy.get("max_loss_net_r", base.tp_target_policy_max_loss_net_r),
        ),
        tp_target_policy_sizing_basis=str(
            tp_target_policy.get("sizing_basis", base.tp_target_policy_sizing_basis),
        ),
        allow_fallback_target_for_executable=bool(
            frag.get("allow_fallback_target_for_executable", base.allow_fallback_target_for_executable),
        ),
        min_tp_after_cost_bps=float(frag.get("min_tp_after_cost_bps", base.min_tp_after_cost_bps)),
        taker_fee_bps=float(frag.get("taker_fee_bps", base.taker_fee_bps)),
        maker_fee_bps=float(frag.get("maker_fee_bps", base.maker_fee_bps)),
        atr_1m_mult=float(frag.get("atr_1m_mult", base.atr_1m_mult)),
        atr_5m_mult=float(frag.get("atr_5m_mult", base.atr_5m_mult)),
        allow_limit_pullback=bool(frag.get("allow_limit_pullback", base.allow_limit_pullback)),
        allow_breakout_trigger=bool(frag.get("allow_breakout_trigger", base.allow_breakout_trigger)),
        allow_market_now=bool(frag.get("allow_market_now", base.allow_market_now)),
        max_pullback_bps=float(frag.get("max_pullback_bps", base.max_pullback_bps)),
        conditional_plan_expire_sec=int(
            frag.get("conditional_plan_expire_sec", base.conditional_plan_expire_sec),
        ),
        require_micro_symbol_lifecycle_confirmed=bool(
            frag.get(
                "require_micro_symbol_lifecycle_confirmed",
                base.require_micro_symbol_lifecycle_confirmed,
            ),
        ),
        micro_consumption_policy=str(frag.get("micro_consumption_policy", base.micro_consumption_policy)),
        allow_weak_micro_consumption=bool(
            frag.get("allow_weak_micro_consumption", base.allow_weak_micro_consumption),
        ),
        weak_micro_min_state=str(frag.get("weak_micro_min_state", base.weak_micro_min_state)),
        weak_micro_require_signal_usable=bool(
            frag.get("weak_micro_require_signal_usable", base.weak_micro_require_signal_usable),
        ),
        weak_micro_require_direction_not_conflict=bool(
            frag.get(
                "weak_micro_require_direction_not_conflict",
                base.weak_micro_require_direction_not_conflict,
            ),
        ),
        weak_micro_block_reasons=tuple(str(v) for v in block_reasons),
        position_sizing_enabled=bool(sizing.get("enabled", base.position_sizing_enabled)),
        position_sizing_method=str(sizing.get("method", base.position_sizing_method)),
        planned_loss_guard_enabled=bool(
            trade_risk.get("planned_loss_guard_enabled", base.planned_loss_guard_enabled),
        ),
        planned_loss_sizing_policy=str(
            trade_risk.get("sizing_policy", base.planned_loss_sizing_policy),
        ),
        base_notional_usdt=float(trade_risk.get("base_notional_usdt", sizing.get("max_notional_usdt", base.base_notional_usdt))),
        target_planned_loss_usdt=float(
            trade_risk.get("target_planned_loss_usdt", base.target_planned_loss_usdt),
        ),
        max_planned_loss_usdt=float(
            trade_risk.get("max_planned_loss_usdt", base.max_planned_loss_usdt),
        ),
        allow_notional_resize=bool(trade_risk.get("allow_notional_resize", base.allow_notional_resize)),
        paper_fallback_notional_allowed=bool(
            trade_risk.get("paper_fallback_notional_allowed", base.paper_fallback_notional_allowed),
        ),
        account_equity_usdt=float(sizing.get("account_equity_usdt", base.account_equity_usdt)),
        default_leverage=float(sizing.get("default_leverage", base.default_leverage)),
        risk_budget_usdt=float(sizing.get("risk_budget_usdt", base.risk_budget_usdt)),
        risk_pct_equity=float(sizing.get("risk_pct_equity", base.risk_pct_equity)),
        min_risk_budget_usdt=float(sizing.get("min_risk_budget_usdt", base.min_risk_budget_usdt)),
        max_risk_budget_usdt=float(sizing.get("max_risk_budget_usdt", base.max_risk_budget_usdt)),
        max_margin_usdt=float(sizing.get("max_margin_usdt", base.max_margin_usdt)),
        min_notional_usdt=float(sizing.get("min_notional_usdt", base.min_notional_usdt)),
        max_notional_usdt=float(sizing.get("max_notional_usdt", base.max_notional_usdt)),
        include_fee_in_risk_budget=bool(
            sizing.get("include_fee_in_risk_budget", base.include_fee_in_risk_budget),
        ),
        reject_if_capped_below_min_risk=bool(
            sizing.get("reject_if_capped_below_min_risk", base.reject_if_capped_below_min_risk),
        ),
        short_now_calibration_enabled=bool(
            short_now.get(
                "enabled",
                frag.get("short_now_calibration_enabled", base.short_now_calibration_enabled),
            ),
        ),
        short_now_min_range_pos=float(
            short_now.get("min_range_pos", frag.get("short_now_min_range_pos", base.short_now_min_range_pos)),
        ),
        short_now_max_range_pos=float(
            short_now.get("max_range_pos", frag.get("short_now_max_range_pos", base.short_now_max_range_pos)),
        ),
        short_now_min_available_room_bps=float(
            short_now.get(
                "min_available_room_bps",
                frag.get("short_now_min_available_room_bps", base.short_now_min_available_room_bps),
            ),
        ),
        short_now_max_stop_bps=float(
            short_now.get("max_stop_bps", frag.get("short_now_max_stop_bps", base.short_now_max_stop_bps)),
        ),
        short_now_max_stop_atr_mult=float(
            short_now.get(
                "max_stop_atr_mult",
                frag.get("short_now_max_stop_atr_mult", base.short_now_max_stop_atr_mult),
            ),
        ),
        short_now_min_net_rr=float(
            short_now.get("min_net_rr", frag.get("short_now_min_net_rr", base.short_now_min_net_rr)),
        ),
        short_now_allow_if_liquidity_missing=bool(
            short_now.get(
                "allow_if_liquidity_missing",
                frag.get("short_now_allow_if_liquidity_missing", base.short_now_allow_if_liquidity_missing),
            ),
        ),
        short_now_max_spread_bps=float(
            short_now.get("max_spread_bps", frag.get("short_now_max_spread_bps", base.short_now_max_spread_bps)),
        ),
        short_now_max_slippage_bps=float(
            short_now.get(
                "max_slippage_bps",
                frag.get("short_now_max_slippage_bps", base.short_now_max_slippage_bps),
            ),
        ),
        short_now_require_recent_down_impulse=bool(
            short_now.get(
                "require_recent_down_impulse",
                frag.get("short_now_require_recent_down_impulse", base.short_now_require_recent_down_impulse),
            ),
        ),
        short_now_reject_if_rebound_required=bool(
            short_now.get(
                "reject_if_rebound_required",
                frag.get("short_now_reject_if_rebound_required", base.short_now_reject_if_rebound_required),
            ),
        ),
        market_now_calibration_enabled=bool(
            market_now.get("enabled", frag.get("market_now_calibration_enabled", base.market_now_calibration_enabled)),
        ),
        market_now_legacy_short_now_fallback=bool(
            market_now.get(
                "legacy_short_now_fallback",
                frag.get("market_now_legacy_short_now_fallback", base.market_now_legacy_short_now_fallback),
            ),
        ),
        long_now_min_range_pos=float(
            market_now_long.get(
                "min_range_pos",
                frag.get("long_now_min_range_pos", base.long_now_min_range_pos),
            ),
        ),
        long_now_max_range_pos=float(
            market_now_long.get(
                "max_range_pos",
                frag.get("long_now_max_range_pos", base.long_now_max_range_pos),
            ),
        ),
        long_now_min_available_room_bps=float(
            market_now_long.get(
                "min_available_room_bps",
                frag.get("long_now_min_available_room_bps", base.long_now_min_available_room_bps),
            ),
        ),
        long_now_max_stop_bps=float(
            market_now_long.get("max_stop_bps", frag.get("long_now_max_stop_bps", base.long_now_max_stop_bps)),
        ),
        long_now_max_stop_atr_mult=float(
            market_now_long.get(
                "max_stop_atr_mult",
                frag.get("long_now_max_stop_atr_mult", base.long_now_max_stop_atr_mult),
            ),
        ),
        long_now_min_net_rr=float(
            market_now_long.get("min_net_rr", frag.get("long_now_min_net_rr", base.long_now_min_net_rr)),
        ),
        long_now_allow_if_liquidity_missing=bool(
            market_now_long.get(
                "allow_if_liquidity_missing",
                frag.get("long_now_allow_if_liquidity_missing", base.long_now_allow_if_liquidity_missing),
            ),
        ),
        long_now_max_spread_bps=float(
            market_now_long.get("max_spread_bps", frag.get("long_now_max_spread_bps", base.long_now_max_spread_bps)),
        ),
        long_now_max_slippage_bps=float(
            market_now_long.get(
                "max_slippage_bps",
                frag.get("long_now_max_slippage_bps", base.long_now_max_slippage_bps),
            ),
        ),
        long_now_require_recent_up_impulse=bool(
            market_now_long.get(
                "require_recent_up_impulse",
                frag.get("long_now_require_recent_up_impulse", base.long_now_require_recent_up_impulse),
            ),
        ),
        long_now_reject_if_pullback_required=bool(
            market_now_long.get(
                "reject_if_pullback_required",
                frag.get("long_now_reject_if_pullback_required", base.long_now_reject_if_pullback_required),
            ),
        ),
        profile_gate_enabled=bool(frag.get("profile_gate_enabled", base.profile_gate_enabled)),
        min_profile_market_entry_score=int(
            frag.get("min_profile_market_entry_score", base.min_profile_market_entry_score),
        ),
        min_profile_hf_stop_score=int(frag.get("min_profile_hf_stop_score", base.min_profile_hf_stop_score)),
        max_profile_slippage_risk_score=int(
            frag.get("max_profile_slippage_risk_score", base.max_profile_slippage_risk_score),
        ),
        trade_quality_gate_enabled=bool(
            trade_quality_gate.get("enabled", base.trade_quality_gate_enabled),
        ),
        trade_quality_gate_mode=str(
            trade_quality_gate.get("mode", base.trade_quality_gate_mode),
        ),
        trade_quality_gate_min_samples_per_symbol=int(
            trade_quality_gate.get(
                "min_samples_per_symbol",
                base.trade_quality_gate_min_samples_per_symbol,
            ),
        ),
        trade_quality_gate_min_samples_per_root_cause=int(
            trade_quality_gate.get(
                "min_samples_per_root_cause",
                base.trade_quality_gate_min_samples_per_root_cause,
            ),
        ),
        trade_quality_gate_max_negative_expectancy_R=float(
            trade_quality_gate.get(
                "max_negative_expectancy_R",
                base.trade_quality_gate_max_negative_expectancy_R,
            ),
        ),
        trade_quality_gate_signal_no_edge_wait_enabled=bool(
            trade_quality_gate.get(
                "signal_no_edge_wait_enabled",
                base.trade_quality_gate_signal_no_edge_wait_enabled,
            ),
        ),
        trade_quality_gate_side_specific_enabled=bool(
            trade_quality_gate.get(
                "side_specific_enabled",
                base.trade_quality_gate_side_specific_enabled,
            ),
        ),
        sl_tp_quality_enabled=bool(sl_tp_quality.get("enabled", base.sl_tp_quality_enabled)),
        sl_tp_quality_mode=str(sl_tp_quality.get("mode", base.sl_tp_quality_mode)),
        sl_tp_quality_single_tp_only=bool(
            sl_tp_quality.get("single_tp_only", base.sl_tp_quality_single_tp_only),
        ),
        sl_tp_quality_min_samples_per_cluster=int(
            sl_tp_quality.get("min_samples_per_cluster", base.sl_tp_quality_min_samples_per_cluster),
        ),
        sl_tp_quality_stop_too_tight_widen_factor=float(
            sl_tp_quality.get(
                "stop_too_tight_widen_factor",
                base.sl_tp_quality_stop_too_tight_widen_factor,
            ),
        ),
        sl_tp_quality_tp_too_far_reduce_factor=float(
            sl_tp_quality.get("tp_too_far_reduce_factor", base.sl_tp_quality_tp_too_far_reduce_factor),
        ),
        sl_tp_quality_entered_too_early_wait_enabled=bool(
            sl_tp_quality.get(
                "entered_too_early_wait_enabled",
                base.sl_tp_quality_entered_too_early_wait_enabled,
            ),
        ),
    )


def _gate_config_snapshot(
    *,
    line: TradePlanLineName,
    cfg: TradePlanLineConfig,
    liquidity: MarketEntryLiquidityItem | None,
    refresh: DecisionRefreshItem | None,
) -> dict[str, Any]:
    snap = asdict(cfg)
    snap["line"] = line
    snap["liquidity_notional_usdt"] = _liquidity_value(liquidity, refresh, "notional_usdt")
    snap["liquidity_max_spread_bps"] = _liquidity_value(liquidity, refresh, "max_spread_bps")
    snap["liquidity_max_estimated_slippage_bps"] = _liquidity_value(
        liquidity,
        refresh,
        "max_estimated_slippage_bps",
    )
    snap["liquidity_min_top_depth_usdt"] = _liquidity_value(liquidity, refresh, "min_top_depth_usdt")
    if refresh is not None:
        range_gate = getattr(refresh, "range_gate", None) or {}
        snap["long_max_range_pos"] = range_gate.get("long_max_range_pos")
        snap["short_min_range_pos"] = range_gate.get("short_min_range_pos")
    return snap


def _trade_quality_gate_guard(
    *,
    symbol: str,
    decision: str,
    cfg: TradePlanLineConfig,
    prior: dict[str, Any] | None,
) -> dict[str, Any]:
    mode = cfg.trade_quality_gate_mode if cfg.trade_quality_gate_mode in TRADE_QUALITY_GATE_MODES else "off"
    symbol_prior = ((prior or {}).get("symbols") or {}).get(_key(symbol)) or {}
    side_prior = ((prior or {}).get("sides") or {}).get(str(decision).upper()) or {}
    root_prior = (symbol_prior.get("root_causes") or {}).get("signal_no_edge") or {}
    reason_codes: list[str] = []
    avg_r = symbol_prior.get("avg_R")
    if (
        symbol_prior.get("sample_count", 0) >= cfg.trade_quality_gate_min_samples_per_symbol
        and avg_r is not None
        and float(avg_r) <= cfg.trade_quality_gate_max_negative_expectancy_R
    ):
        reason_codes.append("trade_quality_symbol_negative_expectancy")
    if (
        cfg.trade_quality_gate_side_specific_enabled
        and side_prior.get("sample_count", 0) >= cfg.trade_quality_gate_min_samples_per_root_cause
        and side_prior.get("avg_R") is not None
        and float(side_prior["avg_R"]) <= cfg.trade_quality_gate_max_negative_expectancy_R
    ):
        reason_codes.append("trade_quality_side_negative_expectancy")
    if (
        cfg.trade_quality_gate_signal_no_edge_wait_enabled
        and root_prior.get("sample_count", 0) >= cfg.trade_quality_gate_min_samples_per_root_cause
        and root_prior.get("avg_R") is not None
        and float(root_prior["avg_R"]) < 0
    ):
        reason_codes.append("trade_quality_signal_no_edge_prior")
    promotions = ((prior or {}).get("promotions") or {})
    symbol_promotions = ((promotions.get("by_symbol") or {}).get(_key(symbol)) or [])
    promotion_matches = [
        row for row in symbol_promotions
        if str(row.get("promotion_mode") or "") == "wait_only"
        and str(row.get("rule_type") or "") in PROMOTION_GATE_RULE_TYPES
        and (not row.get("side") or str(row.get("side")).upper() == str(decision).upper())
    ]
    if promotion_matches:
        reason_codes.append("trade_quality_promotion_wait_only")
        for row in promotion_matches:
            rule_type = str(row.get("rule_type") or "")
            if rule_type:
                reason_codes.append(f"trade_quality_promotion_{rule_type}")
    enforce = bool(cfg.trade_quality_gate_enabled and mode in {"wait_only", "block_executable"} and reason_codes)
    return {
        "enabled": cfg.trade_quality_gate_enabled,
        "mode": mode,
        "ok": not reason_codes,
        "trade_quality_gate_pass": not enforce,
        "enforced": enforce,
        "reason_codes": reason_codes,
        "root_cause_prior": root_prior,
        "symbol_penalty": float(symbol_prior.get("avg_R") or 0.0),
        "side_penalty": float(side_prior.get("avg_R") or 0.0),
        "sample_count": int(symbol_prior.get("sample_count") or 0),
        "prior_available": bool((prior or {}).get("available")),
        "promotion_policy_active": bool(promotion_matches),
        "promotion_policy_matches": promotion_matches,
        "promotion_rule_ids": [str(row.get("rule_id") or "") for row in promotion_matches],
        "promotion_reason_codes": [
            "trade_quality_promotion_wait_only",
            *[f"trade_quality_promotion_{row.get('rule_type')}" for row in promotion_matches if row.get("rule_type")],
        ] if promotion_matches else [],
        "active_profile": promotions.get("active_profile") or "custom",
    }


def _sl_tp_quality_guard(
    *,
    symbol: str,
    decision: str,
    cfg: TradePlanLineConfig,
    prior: dict[str, Any] | None,
) -> dict[str, Any]:
    mode = cfg.sl_tp_quality_mode if cfg.sl_tp_quality_mode in SL_TP_QUALITY_MODES else "off"
    symbol_prior = ((prior or {}).get("symbols") or {}).get(_key(symbol)) or {}
    root_causes = symbol_prior.get("root_causes") or {}
    reason_codes: list[str] = []
    if (root_causes.get("stop_too_tight") or {}).get("sample_count", 0) >= cfg.sl_tp_quality_min_samples_per_cluster:
        reason_codes.append("sl_tp_quality_stop_too_tight_prior")
    if (root_causes.get("tp_too_far") or {}).get("sample_count", 0) >= cfg.sl_tp_quality_min_samples_per_cluster:
        reason_codes.append("sl_tp_quality_tp_too_far_prior")
    if (root_causes.get("entered_too_early") or {}).get("sample_count", 0) >= cfg.sl_tp_quality_min_samples_per_cluster:
        reason_codes.append("sl_tp_quality_entered_too_early_prior")
    template_id = "single_tp_default"
    if "sl_tp_quality_stop_too_tight_prior" in reason_codes:
        template_id = "single_tp_wider_stop"
    if "sl_tp_quality_tp_too_far_prior" in reason_codes:
        template_id = "single_tp_reduced_target" if template_id == "single_tp_default" else f"{template_id}+reduced_target"
    return {
        "enabled": cfg.sl_tp_quality_enabled,
        "mode": mode,
        "single_tp_only": cfg.sl_tp_quality_single_tp_only,
        "template_id": template_id,
        "reason_codes": reason_codes,
        "prior_MFE_R": symbol_prior.get("avg_MFE_R"),
        "prior_MAE_R": symbol_prior.get("avg_MAE_R"),
        "adjustment_applied": False,
        "prior_available": bool((prior or {}).get("available")),
        "sample_count": int(symbol_prior.get("sample_count") or 0),
        "side": decision,
        "symbol": _key(symbol),
    }


def _apply_sl_tp_quality_adjustment(
    *,
    decision: str,
    entry: float | None,
    stop: float | None,
    take: float | None,
    guards: dict[str, Any],
    cfg: TradePlanLineConfig,
) -> tuple[float | None, float | None, float | None, float | None, float | None, dict[str, Any]]:
    quality = dict(guards.get("sl_tp_quality") or {})
    if (
        not cfg.sl_tp_quality_enabled
        or cfg.sl_tp_quality_mode != "apply"
        or entry is None
        or stop is None
        or take is None
        or entry <= 0
    ):
        return stop, take, None, None, None, quality
    original_stop = stop
    original_take = take
    reasons = set(quality.get("reason_codes") or [])
    if "sl_tp_quality_stop_too_tight_prior" in reasons and cfg.sl_tp_quality_stop_too_tight_widen_factor > 1:
        if decision == "LONG":
            stop = entry - abs(entry - stop) * cfg.sl_tp_quality_stop_too_tight_widen_factor
        elif decision == "SHORT":
            stop = entry + abs(stop - entry) * cfg.sl_tp_quality_stop_too_tight_widen_factor
    if "sl_tp_quality_tp_too_far_prior" in reasons and 0 < cfg.sl_tp_quality_tp_too_far_reduce_factor < 1:
        if decision == "LONG":
            take = entry + abs(take - entry) * cfg.sl_tp_quality_tp_too_far_reduce_factor
        elif decision == "SHORT":
            take = entry - abs(entry - take) * cfg.sl_tp_quality_tp_too_far_reduce_factor
    risk = abs(entry - stop) if stop is not None else None
    reward = abs(take - entry) if take is not None else None
    rr = reward / risk if risk and reward is not None and risk > 0 else None
    quality.update(
        {
            "adjustment_applied": bool(stop != original_stop or take != original_take),
            "original_stop_loss": original_stop,
            "original_take_profit": original_take,
            "adjusted_stop_loss": stop,
            "adjusted_take_profit": take,
            "stop_widen_factor": cfg.sl_tp_quality_stop_too_tight_widen_factor,
            "tp_reduce_factor": cfg.sl_tp_quality_tp_too_far_reduce_factor,
        },
    )
    return stop, take, risk, reward, rr, quality


def _key(symbol: str) -> str:
    return symbol.strip().upper()


def _risk_budget_usdt(cfg: TradePlanLineConfig) -> float:
    pct_budget = cfg.account_equity_usdt * cfg.risk_pct_equity
    raw_budget = cfg.risk_budget_usdt if cfg.risk_budget_usdt > 0 else pct_budget
    return max(cfg.min_risk_budget_usdt, min(cfg.max_risk_budget_usdt, raw_budget))


def _build_position_sizing(
    *,
    cfg: TradePlanLineConfig,
    entry: float | None,
    stop: float | None,
    take: float | None,
    risk_per_unit: float | None,
    reward_per_unit: float | None,
) -> tuple[dict[str, Any] | None, str | None]:
    if entry is None or stop is None or take is None or risk_per_unit is None or reward_per_unit is None:
        return {
            "method": cfg.position_sizing_method if cfg.position_sizing_enabled else cfg.planned_loss_sizing_policy,
            "enabled": True,
            "sizing_reject_reason": "missing_price_contract",
        }, "position_sizing_invalid"
    if entry <= 0 or stop <= 0 or take <= 0 or risk_per_unit <= 0:
        return {
            "method": cfg.position_sizing_method if cfg.position_sizing_enabled else cfg.planned_loss_sizing_policy,
            "enabled": True,
            "entry_price": entry,
            "stop_loss": stop,
            "take_profit": take,
            "risk_per_unit": risk_per_unit,
            "sizing_reject_reason": "invalid_price_or_risk",
        }, "position_sizing_invalid"

    leverage = cfg.default_leverage if cfg.default_leverage > 0 else 1.0
    fee_rate = (cfg.taker_fee_bps * 2 / 10_000) if cfg.include_fee_in_risk_budget else 0.0
    sizing_basis = (
        cfg.tp_target_policy_sizing_basis
        if cfg.tp_target_policy_sizing_basis in {"gross_stop", "net_planned_loss"}
        else "gross_stop"
    )
    stop_distance_bps = abs(entry - stop) / entry * 10_000
    reward_distance_bps = abs(take - entry) / entry * 10_000

    if not cfg.position_sizing_enabled:
        base_notional = cfg.base_notional_usdt if cfg.base_notional_usdt > 0 else cfg.max_notional_usdt
        if base_notional <= 0:
            base_notional = cfg.max_margin_usdt * leverage
        max_notional_from_margin = cfg.max_margin_usdt * leverage if cfg.max_margin_usdt > 0 else base_notional
        base_notional = min(v for v in [base_notional, cfg.max_notional_usdt, max_notional_from_margin] if v > 0)
        raw_quantity = base_notional / entry
        raw_gross_risk_usdt = raw_quantity * risk_per_unit
        raw_gross_reward_usdt = raw_quantity * reward_per_unit
        raw_cost_usdt = base_notional * cfg.taker_fee_bps * 2 / 10_000
        target_loss = cfg.target_planned_loss_usdt
        max_loss = cfg.max_planned_loss_usdt if cfg.max_planned_loss_usdt > 0 else raw_gross_risk_usdt + raw_cost_usdt
        notional = base_notional
        caps_applied: list[str] = []
        loss_cap_applied = False
        if cfg.planned_loss_guard_enabled and cfg.allow_notional_resize and raw_gross_risk_usdt > max_loss:
            risk_fraction = risk_per_unit / entry
            if risk_fraction <= 0:
                return {
                    "method": cfg.planned_loss_sizing_policy,
                    "enabled": True,
                    "sizing_reject_reason": "invalid_risk_fraction",
                }, "position_sizing_invalid"
            notional = max_loss / risk_fraction
            caps_applied.append("max_planned_loss_cap")
            loss_cap_applied = True
        elif cfg.planned_loss_guard_enabled and raw_gross_risk_usdt > max_loss:
            return {
                "method": cfg.planned_loss_sizing_policy,
                "enabled": True,
                "base_notional_usdt": round(base_notional, 8),
                "planned_loss_usdt": round(raw_gross_risk_usdt, 8),
                "max_planned_loss_usdt": round(max_loss, 8),
                "sizing_reject_reason": "planned_loss_usdt_too_high",
            }, "planned_loss_usdt_too_high"
        if notional < cfg.min_notional_usdt:
            return {
                "method": cfg.planned_loss_sizing_policy,
                "enabled": True,
                "base_notional_usdt": round(base_notional, 8),
                "notional_usdt": round(notional, 8),
                "min_notional_usdt": cfg.min_notional_usdt,
                "max_planned_loss_usdt": round(max_loss, 8),
                "sizing_reject_reason": "planned_notional_below_min_after_loss_cap",
            }, "planned_notional_below_min_after_loss_cap"
        quantity = notional / entry
        gross_risk_usdt = quantity * risk_per_unit
        gross_reward_usdt = quantity * reward_per_unit
        estimated_entry_fee_usdt = notional * cfg.taker_fee_bps / 10_000
        estimated_exit_fee_usdt = estimated_entry_fee_usdt
        estimated_round_trip_cost_usdt = estimated_entry_fee_usdt + estimated_exit_fee_usdt
        estimated_max_loss_usdt = gross_risk_usdt + estimated_round_trip_cost_usdt
        planned_initial_risk_usdt = (
            estimated_max_loss_usdt if sizing_basis == "net_planned_loss" else gross_risk_usdt
        )
        margin = notional / leverage
        return {
            "method": cfg.planned_loss_sizing_policy,
            "enabled": True,
            "fixed_risk_enabled": False,
            "planned_loss_guard_enabled": cfg.planned_loss_guard_enabled,
            "paper_fallback_notional_allowed": cfg.paper_fallback_notional_allowed,
            "base_notional_usdt": round(base_notional, 8),
            "target_planned_loss_usdt": round(target_loss, 8),
            "max_planned_loss_usdt": round(max_loss, 8),
            "loss_cap_applied": loss_cap_applied,
            "loss_cap_ratio": round(notional / base_notional, 8) if base_notional > 0 else None,
            "entry_price": entry,
            "stop_loss": stop,
            "take_profit": take,
            "risk_per_unit": risk_per_unit,
            "reward_per_unit": reward_per_unit,
            "stop_distance_abs": abs(entry - stop),
            "stop_distance_bps": round(stop_distance_bps, 8),
            "reward_distance_bps": round(reward_distance_bps, 8),
            "quantity": round(quantity, 12),
            "planned_quantity": round(quantity, 12),
            "notional_usdt": round(notional, 8),
            "planned_notional_usdt": round(notional, 8),
            "margin_usdt": round(margin, 8),
            "leverage": leverage,
            "gross_risk_usdt": round(gross_risk_usdt, 8),
            "gross_reward_usdt": round(gross_reward_usdt, 8),
            "planned_loss_usdt": round(gross_risk_usdt, 8),
            "planned_initial_risk_usdt": round(planned_initial_risk_usdt, 8),
            "planned_net_loss_usdt": round(estimated_max_loss_usdt, 8),
            "planned_profit_usdt": round(gross_reward_usdt, 8),
            "estimated_max_loss_usdt": round(estimated_max_loss_usdt, 8),
            "r_sizing_basis": sizing_basis,
            "estimated_entry_fee_usdt": round(estimated_entry_fee_usdt, 8),
            "estimated_exit_fee_usdt": round(estimated_exit_fee_usdt, 8),
            "estimated_round_trip_cost_usdt": round(estimated_round_trip_cost_usdt, 8),
            "sizing_caps_applied": caps_applied,
            "sizing_reject_reason": None,
        }, None

    risk_budget = _risk_budget_usdt(cfg)
    risk_cost_per_unit = risk_per_unit + entry * fee_rate
    if risk_cost_per_unit <= 0:
        return {
            "method": cfg.position_sizing_method,
            "enabled": True,
            "sizing_reject_reason": "invalid_risk_cost_per_unit",
        }, "position_sizing_invalid"

    raw_quantity = risk_budget / risk_cost_per_unit
    raw_notional = raw_quantity * entry
    max_notional_from_margin = cfg.max_margin_usdt * leverage if cfg.max_margin_usdt > 0 else raw_notional
    max_notional = min(v for v in [cfg.max_notional_usdt, max_notional_from_margin] if v > 0)
    notional = raw_notional
    caps_applied: list[str] = []
    if notional > max_notional:
        notional = max_notional
        caps_applied.append("max_notional_or_margin_cap")
    if notional < cfg.min_notional_usdt:
        return {
            "method": cfg.position_sizing_method,
            "enabled": True,
            "risk_budget_usdt": risk_budget,
            "raw_notional_usdt": raw_notional,
            "notional_usdt": notional,
            "min_notional_usdt": cfg.min_notional_usdt,
            "sizing_reject_reason": "notional_below_min",
        }, "position_sizing_invalid"

    quantity = notional / entry
    gross_risk_usdt = quantity * risk_per_unit
    gross_reward_usdt = quantity * reward_per_unit
    estimated_entry_fee_usdt = notional * cfg.taker_fee_bps / 10_000
    estimated_exit_fee_usdt = estimated_entry_fee_usdt
    estimated_round_trip_cost_usdt = estimated_entry_fee_usdt + estimated_exit_fee_usdt
    estimated_max_loss_usdt = gross_risk_usdt + estimated_round_trip_cost_usdt
    planned_initial_risk_usdt = (
        estimated_max_loss_usdt if sizing_basis == "net_planned_loss" else gross_risk_usdt
    )
    if cfg.reject_if_capped_below_min_risk and gross_risk_usdt < cfg.min_risk_budget_usdt:
        return {
            "method": cfg.position_sizing_method,
            "enabled": True,
            "risk_budget_usdt": risk_budget,
            "gross_risk_usdt": gross_risk_usdt,
            "min_risk_budget_usdt": cfg.min_risk_budget_usdt,
            "sizing_caps_applied": caps_applied,
            "sizing_reject_reason": "capped_below_min_risk",
        }, "position_sizing_invalid"

    return {
        "method": cfg.position_sizing_method,
        "enabled": True,
        "risk_budget_usdt": round(risk_budget, 8),
        "risk_pct_equity": cfg.risk_pct_equity,
        "account_equity_usdt": cfg.account_equity_usdt,
        "entry_price": entry,
        "stop_loss": stop,
        "take_profit": take,
        "stop_distance_abs": abs(entry - stop),
        "stop_distance_bps": round(stop_distance_bps, 8),
        "quantity": round(quantity, 12),
        "notional_usdt": round(notional, 8),
        "margin_usdt": round(notional / leverage, 8),
        "leverage": leverage,
        "gross_risk_usdt": round(gross_risk_usdt, 8),
        "gross_reward_usdt": round(gross_reward_usdt, 8),
        "planned_loss_usdt": round(gross_risk_usdt, 8),
        "planned_initial_risk_usdt": round(planned_initial_risk_usdt, 8),
        "planned_net_loss_usdt": round(estimated_max_loss_usdt, 8),
        "planned_profit_usdt": round(gross_reward_usdt, 8),
        "target_planned_loss_usdt": round(cfg.target_planned_loss_usdt, 8),
        "max_planned_loss_usdt": round(cfg.max_planned_loss_usdt, 8),
        "loss_cap_applied": False,
        "planned_loss_guard_enabled": cfg.planned_loss_guard_enabled,
        "paper_fallback_notional_allowed": cfg.paper_fallback_notional_allowed,
        "estimated_max_loss_usdt": round(estimated_max_loss_usdt, 8),
        "r_sizing_basis": sizing_basis,
        "estimated_entry_fee_usdt": round(estimated_entry_fee_usdt, 8),
        "estimated_exit_fee_usdt": round(estimated_exit_fee_usdt, 8),
        "estimated_round_trip_cost_usdt": round(estimated_round_trip_cost_usdt, 8),
        "sizing_caps_applied": caps_applied,
        "sizing_reject_reason": None,
    }, None


def _f(v: object) -> float | None:
    if isinstance(v, bool) or v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _bps(entry: float, distance: float) -> float:
    if entry <= 0:
        return 0.0
    return abs(distance) / entry * 10_000.0


def _price_from_bps(entry: float, bps: float, *, side: str, direction: str) -> float:
    mult = bps / 10_000.0
    if side == "LONG":
        return entry * (1.0 + mult) if direction == "up" else entry * (1.0 - mult)
    return entry * (1.0 - mult) if direction == "up" else entry * (1.0 + mult)


def _liquidity_value(liquidity: MarketEntryLiquidityItem | None, refresh: DecisionRefreshItem | None, key: str) -> float | None:
    if liquidity is not None:
        return _f(getattr(liquidity, key, None))
    if refresh is not None and isinstance(refresh.liquidity, dict):
        return _f(refresh.liquidity.get(key))
    return None


def _side_liquidity_ok(
    *,
    decision: str,
    liquidity: MarketEntryLiquidityItem | None,
    refresh: DecisionRefreshItem | None,
) -> bool | None:
    source: Any = liquidity
    if source is None and refresh is not None and isinstance(refresh.liquidity, dict):
        source = refresh.liquidity
    if source is None:
        return refresh.liquidity_ok if refresh is not None else None
    explicit_fields = getattr(source, "model_fields_set", set())
    if decision == "LONG":
        val = None
        if isinstance(source, dict):
            val = source.get("buy_liquidity_ok_for_market_entry")
        elif "buy_liquidity_ok_for_market_entry" in explicit_fields:
            val = getattr(source, "buy_liquidity_ok_for_market_entry", None)
        if val is not None:
            return bool(val)
    if decision == "SHORT":
        val = None
        if isinstance(source, dict):
            val = source.get("sell_liquidity_ok_for_market_entry")
        elif "sell_liquidity_ok_for_market_entry" in explicit_fields:
            val = getattr(source, "sell_liquidity_ok_for_market_entry", None)
        if val is not None:
            return bool(val)
    val = getattr(source, "liquidity_ok_for_market_entry", None)
    if val is None and isinstance(source, dict):
        val = source.get("liquidity_ok_for_market_entry")
    return bool(val) if val is not None else None


def _side_liquidity_reasons(
    *,
    decision: str,
    liquidity: MarketEntryLiquidityItem | None,
    refresh: DecisionRefreshItem | None,
) -> list[str]:
    source: Any = liquidity
    if source is None and refresh is not None and isinstance(refresh.liquidity, dict):
        source = refresh.liquidity
    if source is None:
        return []
    explicit_fields = getattr(source, "model_fields_set", set())
    if decision == "LONG":
        val = None
        if isinstance(source, dict):
            val = source.get("buy_reason_codes")
        elif "buy_reason_codes" in explicit_fields:
            val = getattr(source, "buy_reason_codes", None)
        if val is not None:
            return list(val)
    if decision == "SHORT":
        val = None
        if isinstance(source, dict):
            val = source.get("sell_reason_codes")
        elif "sell_reason_codes" in explicit_fields:
            val = getattr(source, "sell_reason_codes", None)
        if val is not None:
            return list(val)
    val = getattr(source, "reason_codes", None)
    if val is None and isinstance(source, dict):
        val = source.get("reason_codes")
    return list(val or [])


def _market_now_side_thresholds(cfg: TradePlanLineConfig, decision: str) -> dict[str, Any]:
    if decision == "LONG":
        return {
            "enabled": cfg.market_now_calibration_enabled,
            "min_range_pos": cfg.long_now_min_range_pos,
            "max_range_pos": cfg.long_now_max_range_pos,
            "min_available_room_bps": cfg.long_now_min_available_room_bps,
            "max_stop_bps": cfg.long_now_max_stop_bps,
            "max_stop_atr_mult": cfg.long_now_max_stop_atr_mult,
            "min_net_rr": cfg.long_now_min_net_rr,
            "allow_if_liquidity_missing": cfg.long_now_allow_if_liquidity_missing,
            "max_spread_bps": cfg.long_now_max_spread_bps,
            "max_slippage_bps": cfg.long_now_max_slippage_bps,
            "reject_if_better_entry_required": cfg.long_now_reject_if_pullback_required,
            "better_entry_reason": "long_now_pullback_required",
            "slippage_field": "estimated_market_buy_slippage_bps",
            "prefix": "long_now",
        }
    return {
        "enabled": cfg.market_now_calibration_enabled or cfg.short_now_calibration_enabled,
        "min_range_pos": cfg.short_now_min_range_pos,
        "max_range_pos": cfg.short_now_max_range_pos,
        "min_available_room_bps": cfg.short_now_min_available_room_bps,
        "max_stop_bps": cfg.short_now_max_stop_bps,
        "max_stop_atr_mult": cfg.short_now_max_stop_atr_mult,
        "min_net_rr": cfg.short_now_min_net_rr,
        "allow_if_liquidity_missing": cfg.short_now_allow_if_liquidity_missing,
        "max_spread_bps": cfg.short_now_max_spread_bps,
        "max_slippage_bps": cfg.short_now_max_slippage_bps,
        "reject_if_better_entry_required": cfg.short_now_reject_if_rebound_required,
        "better_entry_reason": "short_now_rebound_required",
        "slippage_field": "estimated_market_sell_slippage_bps",
        "prefix": "short_now",
    }


def _market_now_calibration_reasons(
    *,
    cfg: TradePlanLineConfig,
    decision: str,
    refresh: DecisionRefreshItem | None,
    liquidity: MarketEntryLiquidityItem | None,
    guards: dict[str, Any],
) -> list[str]:
    thresholds = _market_now_side_thresholds(cfg, decision)
    if not thresholds["enabled"] or decision not in {"LONG", "SHORT"}:
        return []
    prefix = str(thresholds["prefix"])
    reasons: list[str] = []
    range_gate = getattr(refresh, "range_gate", {}) if refresh is not None else {}
    range_pos = _f(range_gate.get("range_pos")) if isinstance(range_gate, dict) else None
    if range_pos is None:
        reasons.append(f"{prefix}_range_missing")
    else:
        if range_pos < float(thresholds["min_range_pos"]):
            reasons.append(f"{prefix}_range_too_low")
        if range_pos > float(thresholds["max_range_pos"]):
            reasons.append(f"{prefix}_range_too_high")
    if thresholds["reject_if_better_entry_required"] and range_pos is not None:
        if decision == "LONG" and range_pos > float(thresholds["max_range_pos"]):
            reasons.append(str(thresholds["better_entry_reason"]))
        if decision == "SHORT" and range_pos < float(thresholds["min_range_pos"]):
            reasons.append(str(thresholds["better_entry_reason"]))

    available_room_bps = _f(guards.get("available_room_bps"))
    if available_room_bps is None or available_room_bps < float(thresholds["min_available_room_bps"]):
        reasons.append(f"{prefix}_room_not_enough")

    gross_risk_bps = _f(guards.get("gross_risk_bps"))
    if gross_risk_bps is None or gross_risk_bps > float(thresholds["max_stop_bps"]):
        reasons.append(f"{prefix}_stop_too_wide")

    net_rr = _f(guards.get("net_rr"))
    if net_rr is None or net_rr < float(thresholds["min_net_rr"]):
        reasons.append(f"{prefix}_net_rr_too_low")

    spread_bps = _liquidity_value(liquidity, refresh, "spread_bps")
    if spread_bps is not None and spread_bps > float(thresholds["max_spread_bps"]):
        reasons.append(f"{prefix}_spread_too_wide")
    slippage_bps = _liquidity_value(liquidity, refresh, str(thresholds["slippage_field"]))
    if slippage_bps is None:
        if not thresholds["allow_if_liquidity_missing"]:
            reasons.append(f"{prefix}_slippage_missing")
    elif slippage_bps > float(thresholds["max_slippage_bps"]):
        reasons.append(f"{prefix}_slippage_too_high")

    return reasons


def _short_now_calibration_reasons(
    *,
    cfg: TradePlanLineConfig,
    refresh: DecisionRefreshItem | None,
    liquidity: MarketEntryLiquidityItem | None,
    guards: dict[str, Any],
) -> list[str]:
    return _market_now_calibration_reasons(
        cfg=cfg,
        decision="SHORT",
        refresh=refresh,
        liquidity=liquidity,
        guards=guards,
    )


def _side_costs(
    *,
    decision: str,
    liquidity: MarketEntryLiquidityItem | None,
    refresh: DecisionRefreshItem | None,
    cfg: TradePlanLineConfig,
) -> tuple[float, float, float, float, float]:
    buy_slip = _liquidity_value(liquidity, refresh, "estimated_market_buy_slippage_bps") or 0.0
    sell_slip = _liquidity_value(liquidity, refresh, "estimated_market_sell_slippage_bps") or 0.0
    expected_slip = buy_slip if decision == "LONG" else sell_slip
    stop_slip = sell_slip if decision == "LONG" else buy_slip
    tp_slip = sell_slip if decision == "LONG" else buy_slip
    entry_cost = expected_slip + cfg.taker_fee_bps
    stop_cost = stop_slip + cfg.taker_fee_bps
    tp_cost = tp_slip + cfg.taker_fee_bps
    return expected_slip, entry_cost, stop_cost, tp_cost, max(buy_slip, sell_slip)


def _side_to_decision(side: str) -> tuple[str, str]:
    s = side.strip().lower()
    if s == "up":
        return "LONG", "WAIT_PULLBACK"
    if s == "down":
        return "SHORT", "WAIT_REBOUND"
    return "NO_TRADE", "NONE"


def _micro_blocks(
    line: TradePlanLineName,
    micro: MicroFeatureItem | None,
) -> tuple[Micro15mBlock | None, MicroSignalBlock | None, bool | None, list[str]]:
    if _without_micro_like(line):
        return None, None, None, []
    if micro is None:
        return None, None, False, ["micro_missing"]
    if line == "micro_fast":
        if micro.micro_fast_15m is None or micro.micro_fast_quality is None:
            return None, None, False, ["micro_fast_missing"]
        signal = micro.micro_fast_signal
        reasons = list(micro.micro_fast_quality.reason_codes)
        if signal is not None:
            reasons.extend(signal.reason_codes)
        return micro.micro_fast_15m, signal, micro.micro_fast_quality.ready, list(dict.fromkeys(reasons))
    block = micro.micro_full_15m or micro.micro_15m
    quality = micro.micro_full_quality or micro.micro_quality
    signal = micro.micro_full_signal
    reasons = list(quality.reason_codes)
    if signal is not None:
        reasons.extend(signal.reason_codes)
    return block, signal, quality.ready, list(dict.fromkeys(reasons))


def _micro_signal_guards(signal: MicroSignalBlock | None) -> dict[str, Any]:
    if signal is None:
        return {
            "micro_signal_missing": True,
            "micro_signal_usable": False,
            "micro_direction_confirmed": False,
            "micro_exec_allowed": False,
            "micro_alignment_state": "insufficient",
            "micro_strength": "none",
            "micro_confirmation_level": "none",
            "micro_exec_allowed_reason": "",
            "micro_confidence_score": 0,
            "micro_confirmation_penalty_bps": 0.0,
            "micro_price_response_ok": None,
            "micro_persistence_ok": None,
        }
    return {
        "micro_signal_missing": False,
        "micro_data_ready": signal.micro_data_ready,
        "micro_stat_ready": signal.micro_stat_ready,
        "micro_signal_usable": signal.micro_signal_usable,
        "micro_direction_confirmed": signal.micro_direction_confirmed,
        "micro_exec_allowed": signal.micro_exec_allowed,
        "micro_alignment_state": signal.micro_alignment_state,
        "micro_strength": signal.micro_strength,
        "micro_confirmation_level": signal.micro_confirmation_level,
        "micro_exec_allowed_reason": signal.micro_exec_allowed_reason,
        "micro_confidence_score": signal.micro_confidence_score,
        "micro_confirmation_penalty_bps": signal.micro_confirmation_penalty_bps,
        "micro_price_response_ok": signal.price_response_ok,
        "micro_persistence_ok": signal.persistence_ok,
    }


def _micro_quality_for_line(
    line: TradePlanLineName,
    micro: MicroFeatureItem | None,
) -> Any | None:
    if micro is None or _without_micro_like(line):
        return None
    if line == "micro_fast":
        return micro.micro_fast_quality
    return micro.micro_full_quality or micro.micro_quality


def _micro_signal_for_line(
    line: TradePlanLineName,
    micro: MicroFeatureItem | None,
) -> MicroSignalBlock | None:
    if micro is None or _without_micro_like(line):
        return None
    if line == "micro_fast":
        return micro.micro_fast_signal
    return micro.micro_full_signal


def _micro_15m_for_line(
    line: TradePlanLineName,
    micro: MicroFeatureItem | None,
) -> Micro15mBlock | None:
    if micro is None or _without_micro_like(line):
        return None
    if line == "micro_fast":
        return micro.micro_fast_15m
    return micro.micro_full_15m or micro.micro_15m


def _micro_data_quality_for_line(
    *,
    line: TradePlanLineName,
    micro: MicroFeatureItem | None,
    micro_state: MicroDaemonSymbolState | None,
    micro_doc: LatestMicroFeaturesDocument | None = None,
) -> dict[str, Any]:
    if _without_micro_like(line):
        return {}
    return build_micro_data_quality_contract(
        line=line,
        quality=_micro_quality_for_line(line, micro),
        micro_15m=_micro_15m_for_line(line, micro),
        signal=_micro_signal_for_line(line, micro),
        state=micro_state,
        features_doc=micro_doc,
    )


def _micro_data_quality_gate_reasons(contract: dict[str, Any]) -> list[str]:
    state = str(contract.get("micro_data_quality_state") or "ok")
    if state == "technical_blocked":
        return ["data_quality_blocked", "technical_not_ready"]
    if state == "config_warmup_incomplete":
        return ["micro_warmup_incomplete"]
    if state == "unknown":
        return ["micro_data_quality_unknown"]
    return []


def _micro_lifecycle_guards(
    *,
    line: TradePlanLineName,
    micro: MicroFeatureItem | None,
    micro_state: MicroDaemonSymbolState | None,
    micro_doc: LatestMicroFeaturesDocument | None = None,
    cfg: TradePlanLineConfig,
) -> dict[str, Any]:
    if _without_micro_like(line):
        return {}
    quality = _micro_quality_for_line(line, micro)
    signal = _micro_signal_for_line(line, micro)
    ready = bool(quality.ready) if quality is not None else False
    signal_usable = bool(signal.micro_signal_usable) if signal is not None else False
    direction_confirmed = bool(signal.micro_direction_confirmed) if signal is not None else False
    exec_allowed = bool(signal.micro_exec_allowed) if signal is not None else False
    confirmed = ready and signal_usable and direction_confirmed and exec_allowed
    reason_codes: list[str] = []
    if quality is None:
        reason_codes.append("micro_quality_missing")
    else:
        reason_codes.extend(list(quality.reason_codes))
    if signal is None:
        reason_codes.append("micro_signal_missing")
    else:
        reason_codes.extend(list(signal.reason_codes))
    if micro_state is None:
        reason_codes.append("micro_state_symbol_missing")
    elif not micro_state.consumer_safe:
        reason_codes.extend(micro_state.consumer_reason_codes or ["micro_consumer_not_safe"])
    dq_contract = _micro_data_quality_for_line(
        line=line,
        micro=micro,
        micro_state=micro_state,
        micro_doc=micro_doc,
    )
    dq_gate_reasons = _micro_data_quality_gate_reasons(dq_contract)
    reason_codes.extend(dq_gate_reasons)

    if quality is None or not ready:
        state = "not_ready"
    elif confirmed:
        state = "confirmed"
    else:
        state = "rejected"
    terminal = state != "observing"
    policy = cfg.micro_consumption_policy
    if policy not in MICRO_CONSUMPTION_POLICIES:
        policy = "confirmed_only"
    block_set = {str(v) for v in cfg.weak_micro_block_reasons}
    has_blocked_reason = any(str(v) in block_set for v in reason_codes) or bool(dq_gate_reasons)
    weak_signal_ok = signal_usable or not cfg.weak_micro_require_signal_usable
    weak_direction_ok = not (cfg.weak_micro_require_direction_not_conflict and has_blocked_reason)
    weak_ready_ok = ready if cfg.weak_micro_min_state == "ready" else signal_usable
    weak_consumable = (
        cfg.allow_weak_micro_consumption
        and weak_ready_ok
        and weak_signal_ok
        and weak_direction_ok
        and state not in {"not_ready", "timeout", "observing"}
    )
    if policy == "confirmed_only":
        consumable = confirmed
    elif policy == "ready_signal_usable":
        consumable = confirmed or weak_consumable
    elif policy == "weak_ready_test":
        consumable = confirmed or (
            cfg.allow_weak_micro_consumption
            and weak_signal_ok
            and weak_direction_ok
            and state not in {"timeout", "observing"}
            and ready
        )
    else:
        consumable = False
    # STEP10.53: config is the authority for relaxed/test consumption. Weak
    # evidence can be downstream-consumable only when the selected policy and
    # allow_weak_micro_consumption explicitly permit it.
    relaxed = bool(consumable and not confirmed)
    if relaxed:
        state = "confirmed"
    block_reason = ""
    if not consumable:
        if policy == "audit_only":
            block_reason = "micro_policy_audit_only"
        else:
            block_reason = {
                "not_ready": "micro_symbol_not_ready",
                "rejected": "micro_symbol_rejected",
                "timeout": "micro_symbol_timeout",
                "observing": "micro_symbol_observing",
            }.get(state, "micro_symbol_not_consumable")
            if state == "rejected" and has_blocked_reason:
                block_reason = "micro_policy_blocked_reason"

    return {
        "micro_lifecycle_state": state,
        "micro_lifecycle_scope": "symbol",
        "micro_lifecycle_terminal": terminal,
        "micro_symbol_ready": ready,
        "micro_symbol_confirmed": confirmed,
        "micro_confirmation_strength": "strong" if confirmed else ("weak" if relaxed else "none"),
        "micro_consumption_policy": policy,
        "allow_weak_micro_consumption": bool(cfg.allow_weak_micro_consumption),
        "weak_micro_min_state": cfg.weak_micro_min_state,
        "weak_micro_require_signal_usable": bool(cfg.weak_micro_require_signal_usable),
        "weak_micro_require_direction_not_conflict": bool(cfg.weak_micro_require_direction_not_conflict),
        "micro_policy_relaxed": relaxed,
        "micro_policy_block_reasons": list(block_set),
        "micro_policy_blocked": bool(not consumable and (policy == "audit_only" or has_blocked_reason)),
        "micro_symbol_trade_plan_emitted": False,
        "trade_plan_consumable": consumable,
        "consumption_block_reason": block_reason,
        **dq_contract,
        "micro_symbol_observed_sec": (
            int(quality.warmup_age_sec)
            if quality is not None
            else (micro_state.continuous_collect_sec if micro_state is not None else None)
        ),
        "micro_symbol_required_observed_sec": None,
        "micro_symbol_reason_codes": list(dict.fromkeys(reason_codes)),
    }


def _micro_line_lifecycle_summary(
    *,
    line: TradePlanLineName,
    plans: list[TradePlanItem],
) -> dict[str, Any]:
    if _without_micro_like(line):
        return {}
    state_counts: dict[str, int] = {}
    ready_count = 0
    confirmed_count = 0
    emitted_count = 0
    consumable_count = 0
    for plan in plans:
        guards = plan.guards
        state = str(guards.get("micro_lifecycle_state") or "queued")
        state_counts[state] = state_counts.get(state, 0) + 1
        if guards.get("micro_symbol_ready"):
            ready_count += 1
        if guards.get("micro_symbol_confirmed"):
            confirmed_count += 1
        if guards.get("micro_symbol_trade_plan_emitted"):
            emitted_count += 1
        if guards.get("trade_plan_consumable"):
            consumable_count += 1
    unfinished_count = int(state_counts.get("queued", 0)) + int(state_counts.get("observing", 0))
    target_count = len(plans)
    if consumable_count > 0 and unfinished_count > 0:
        exec_status = "usable_partial"
        lifecycle_status = "partial_ready"
    elif consumable_count > 0:
        exec_status = "usable_all_ready"
        lifecycle_status = "completed_all_symbols"
    elif target_count > 0 and unfinished_count > 0:
        exec_status = "no_ready"
        lifecycle_status = "observing"
    elif ready_count > 0:
        exec_status = "no_confirmed"
        lifecycle_status = "completed_without_confirmed"
    elif target_count > 0:
        exec_status = "no_ready"
        lifecycle_status = "completed_without_ready"
    else:
        exec_status = "blocked"
        lifecycle_status = "no_targets"
    return {
        "line_exec_status": exec_status,
        "line_lifecycle_status": lifecycle_status,
        "line_lifecycle_complete": unfinished_count == 0 and target_count > 0,
        "trade_plan_allowed": consumable_count > 0,
        "unfinished_symbol_count": unfinished_count,
        "symbol_counts": {
            "target": target_count,
            "ready": ready_count,
            "confirmed": confirmed_count,
            "consumable": consumable_count,
            "emitted": emitted_count,
            "unfinished": unfinished_count,
            "states": state_counts,
        },
    }


def _micro_lifecycle_item_from_plan(plan: TradePlanItem) -> dict[str, Any]:
    g = plan.guards
    state = str(g.get("micro_lifecycle_state") or "not_ready")
    terminal = bool(g.get("micro_lifecycle_terminal", state != "observing"))
    consumable = bool(g.get("trade_plan_consumable"))
    block_reason = str(g.get("consumption_block_reason") or "")
    if not consumable and not block_reason:
        block_reason = {
            "observing": "micro_symbol_observing",
            "not_ready": "micro_symbol_not_ready",
            "rejected": "micro_symbol_rejected",
            "timeout": "micro_symbol_timeout",
        }.get(state, "micro_symbol_not_consumable")
    attributions = g.get("micro_data_quality_attributions")
    if not isinstance(attributions, list):
        attributions = []
    first_attr = attributions[0] if attributions and isinstance(attributions[0], dict) else {}
    return {
        "symbol": plan.symbol,
        "state": state,
        "terminal": terminal,
        "ready": bool(g.get("micro_symbol_ready")),
        "confirmed": bool(g.get("micro_symbol_confirmed")),
        "confirmation_strength": str(g.get("micro_confirmation_strength") or "none"),
        "micro_consumption_policy": str(g.get("micro_consumption_policy") or "confirmed_only"),
        "micro_policy_relaxed": bool(g.get("micro_policy_relaxed")),
        "trade_plan_consumable": consumable,
        "consumption_block_reason": block_reason,
        "trade_plan_emitted": bool(g.get("micro_symbol_trade_plan_emitted")),
        "executable": bool(plan.executable),
        "observed_sec": g.get("micro_symbol_observed_sec"),
        "required_observed_sec": g.get("micro_symbol_required_observed_sec"),
        "micro_signal_usable": bool(g.get("micro_signal_usable")),
        "micro_direction_confirmed": bool(g.get("micro_direction_confirmed")),
        "micro_exec_allowed": bool(g.get("micro_exec_allowed")),
        "reason_codes": list(g.get("micro_symbol_reason_codes") or []),
        "plan_reason_codes": list(plan.reason_codes or []),
        "micro_data_quality_state": g.get("micro_data_quality_state"),
        "micro_data_quality_class": g.get("micro_data_quality_class"),
        "micro_data_quality_reasons": list(g.get("micro_data_quality_reasons") or []),
        "micro_data_quality_attributions": attributions,
        "micro_data_quality_evidence": g.get("micro_data_quality_evidence") or {},
        "raw_reason": first_attr.get("raw_reason"),
        "raw_reasons": list(g.get("micro_data_quality_target_reasons") or []),
        "attributed_reason": first_attr.get("attributed_reason"),
        "category": first_attr.get("category"),
        "evidence": g.get("micro_data_quality_evidence") or {},
        "recommended_action": first_attr.get("recommended_action"),
    }


def _micro_lifecycle_consumption_refs(
    *,
    line: TradePlanLineName,
    all_plans: list[TradePlanItem],
    consumed_plans: list[TradePlanItem],
    cfg: TradePlanLineConfig,
) -> dict[str, Any]:
    if _without_micro_like(line):
        return {}
    consumed_symbols = {p.symbol for p in consumed_plans}
    excluded = [_micro_lifecycle_item_from_plan(p) for p in all_plans if p.symbol not in consumed_symbols]
    counts: dict[str, int] = {}
    for item in excluded:
        state = str(item.get("state") or "unknown")
        counts[state] = counts.get(state, 0) + 1
    return {
        "micro_consumption_policy": cfg.micro_consumption_policy,
        "allow_weak_micro_consumption": cfg.allow_weak_micro_consumption,
        "weak_micro_min_state": cfg.weak_micro_min_state,
        "weak_micro_require_signal_usable": cfg.weak_micro_require_signal_usable,
        "weak_micro_require_direction_not_conflict": cfg.weak_micro_require_direction_not_conflict,
        "micro_lifecycle_consumed_symbols": [p.symbol for p in consumed_plans],
        "micro_lifecycle_excluded_symbols": [str(item.get("symbol")) for item in excluded],
        "micro_lifecycle_excluded_counts": counts,
        "micro_lifecycle_excluded_items": excluded,
    }


def _price_plan(
    *,
    decision: str,
    entry: float | None,
    refresh: DecisionRefreshItem | None,
    liquidity: MarketEntryLiquidityItem | None,
    factor: FactorSnapshotItem,
    cfg: TradePlanLineConfig,
) -> tuple[float | None, float | None, float | None, float | None, float | None, dict[str, Any], list[str]]:
    guards: dict[str, Any] = {
        "sl_tp_model_version": "10.9",
        "opportunity_level": 0,
        "opportunity_type": "NO_TRADE",
        "market_entry_allowed": False,
        "limit_entry_allowed": False,
        "conditional_entry_allowed": False,
        "better_entry_required": False,
        "better_entry_price": None,
        "better_entry_reason": None,
        "trigger_price": None,
        "trigger_condition": None,
        "time_in_force_sec": cfg.conditional_plan_expire_sec,
        "plan_expire_reason": "freshness_expired",
    }
    if entry is None or entry <= 0:
        return None, None, None, None, None, guards, ["entry_price_missing"]
    e1 = refresh.entry_1m if refresh is not None else factor.entry_1m
    p15 = refresh.primary_15m if refresh is not None else factor.primary_15m
    atr_1m = _f(e1.get("atr"))
    atr_15m = _f(p15.get("atr"))
    atr = atr_1m or atr_15m
    if atr is None or atr <= 0:
        return None, None, None, None, None, guards, ["entry_1m_atr_missing"]

    atr_1m_bps = _bps(entry, atr_1m or atr)
    atr_5m_bps = _bps(entry, atr_15m or atr)
    spread_bps = _liquidity_value(liquidity, refresh, "spread_bps") or 0.0
    expected_slip, entry_cost, stop_cost, tp_cost, max_slip = _side_costs(
        decision=decision,
        liquidity=liquidity,
        refresh=refresh,
        cfg=cfg,
    )
    recent_chop = max(
        abs(_f(p15.get("price_ret")) or 0.0) * 0.25,
        abs(_f(refresh.trigger_5m.get("price_ret")) or 0.0) * 0.5 if refresh is not None else 0.0,
    )
    noise_floor_bps = max(
        cfg.min_stop_bps,
        spread_bps * 3.0,
        max_slip * 2.0,
        atr_1m_bps * 0.8,
        recent_chop,
    )
    vol_stop_bps = max(
        atr_1m_bps * cfg.atr_1m_mult,
        atr_5m_bps * cfg.atr_5m_mult,
        recent_chop,
    )
    stop_floor_bps = max(noise_floor_bps, vol_stop_bps, cfg.preferred_stop_bps)
    guards.update(
        {
            "atr_1m_bps": atr_1m_bps,
            "atr_5m_bps": atr_5m_bps,
            "spread_bps": spread_bps,
            "expected_slippage_bps": expected_slip,
            "entry_cost_bps": entry_cost,
            "stop_exit_cost_bps": stop_cost,
            "take_profit_exit_cost_bps": tp_cost,
            "taker_fee_bps": cfg.taker_fee_bps,
            "maker_fee_bps": cfg.maker_fee_bps,
            "noise_floor_bps": noise_floor_bps,
            "vol_stop_bps": vol_stop_bps,
            "min_stop_bps": cfg.min_stop_bps,
            "max_stop_bps": cfg.max_stop_bps,
            "min_net_rr": cfg.min_net_rr,
            "min_tp_after_cost_bps": cfg.min_tp_after_cost_bps,
        },
    )

    if decision == "LONG":
        swing = _f(e1.get("last_pullback_low")) or _f(p15.get("recent_swing_low"))
        structure_stop = swing if swing is not None and swing < entry else entry - cfg.stop_atr_mult * atr
        volatility_stop = _price_from_bps(entry, vol_stop_bps, side="LONG", direction="down")
        noise_stop = _price_from_bps(entry, noise_floor_bps, side="LONG", direction="down")
        stop = min(structure_stop, volatility_stop, noise_stop)
        if stop >= entry:
            return None, None, None, None, None, guards, ["invalid_long_stop"]
        risk = entry - stop
        structure_target = _f(p15.get("recent_swing_high")) or _f(p15.get("breakout_level"))
        structure_target_bps = _bps(entry, structure_target - entry) if structure_target is not None and structure_target > entry else None
        range_room_bps = structure_target_bps
        expected_move_bps = max(atr_1m_bps * 1.2, atr_5m_bps * 0.8, recent_chop, stop_floor_bps)
        fallback_reward_bps = max(cfg.target_rr * _bps(entry, risk), stop_floor_bps)
    elif decision == "SHORT":
        swing = _f(e1.get("last_rebound_high")) or _f(p15.get("recent_swing_high"))
        structure_stop = swing if swing is not None and swing > entry else entry + cfg.stop_atr_mult * atr
        volatility_stop = _price_from_bps(entry, vol_stop_bps, side="SHORT", direction="down")
        noise_stop = _price_from_bps(entry, noise_floor_bps, side="SHORT", direction="down")
        stop = max(structure_stop, volatility_stop, noise_stop)
        if stop <= entry:
            return None, None, None, None, None, guards, ["invalid_short_stop"]
        risk = stop - entry
        structure_target = _f(p15.get("recent_swing_low")) or _f(p15.get("breakdown_level"))
        structure_target_bps = _bps(entry, entry - structure_target) if structure_target is not None and structure_target < entry else None
        range_room_bps = structure_target_bps
        expected_move_bps = max(atr_1m_bps * 1.2, atr_5m_bps * 0.8, recent_chop, stop_floor_bps)
        fallback_reward_bps = max(cfg.target_rr * _bps(entry, risk), stop_floor_bps)
    else:
        return None, None, None, None, None, guards, ["no_market_direction"]

    gross_risk_bps = _bps(entry, risk)
    target_rr_basis = (
        cfg.tp_target_policy_target_rr_basis
        if cfg.tp_target_policy_target_rr_basis in {"gross", "net"}
        else "gross"
    )
    policy_entry_cost_bps = entry_cost if cfg.tp_target_policy_include_entry_fee else 0.0
    policy_tp_cost_bps = tp_cost if cfg.tp_target_policy_include_exit_fee else 0.0
    policy_stop_cost_bps = stop_cost if cfg.tp_target_policy_include_exit_fee else 0.0
    policy_slippage_reserve_bps = (
        max(0.0, cfg.tp_target_policy_slippage_reserve_bps)
        if cfg.tp_target_policy_include_slippage_reserve
        else 0.0
    )
    planned_net_risk_bps = (
        gross_risk_bps
        + policy_entry_cost_bps
        + policy_stop_cost_bps
        + policy_slippage_reserve_bps
    )
    required_reward_bps = cfg.min_net_rr * planned_net_risk_bps + policy_entry_cost_bps + policy_tp_cost_bps
    target_candidates = {
        "structure_target_bps": structure_target_bps,
        "range_room_bps": range_room_bps,
        "expected_move_bps": expected_move_bps,
        "liquidity_target_bps": None,
        "fallback_target_bps": fallback_reward_bps,
    }
    target_source = "fallback_target_bps"
    gross_reward_bps = fallback_reward_bps
    for candidate_key in (
        "structure_target_bps",
        "range_room_bps",
        "liquidity_target_bps",
        "expected_move_bps",
    ):
        candidate_value = target_candidates.get(candidate_key)
        if candidate_value is not None and candidate_value > 0:
            target_source = candidate_key
            gross_reward_bps = candidate_value
            break
    raw_target_source = target_source
    raw_market_room_bps = gross_reward_bps
    policy_reasons: list[str] = []
    policy_mode = cfg.tp_target_policy_mode if cfg.tp_target_policy_mode in TP_TARGET_POLICY_MODES else "structure"
    configured_target_rr = cfg.tp_target_policy_target_rr
    configured_target_rr_cap = cfg.tp_target_policy_target_rr_cap
    configured_target_net_rr = cfg.tp_target_policy_target_net_rr
    desired_reward_bps: float | None = None
    cap_reward_bps: float | None = None
    final_target_source = target_source
    tp_was_capped = False
    tp_cap_reason: str | None = None
    tp_reject_reason: str | None = None
    reward_to_spread_ratio: float | None = (
        gross_reward_bps / spread_bps if spread_bps and spread_bps > 0 else None
    )
    min_policy_reward_bps = max(0.0, cfg.tp_target_policy_min_reward_bps)

    if policy_mode in {"fast_capped_rr", "structure_or_capped_rr"}:
        configured_target_rr = configured_target_rr if configured_target_rr is not None else cfg.target_rr
        configured_target_net_rr = (
            configured_target_net_rr
            if configured_target_net_rr is not None
            else configured_target_rr
        )
        configured_target_rr_cap = (
            configured_target_rr_cap
            if configured_target_rr_cap is not None
            else configured_target_rr
        )
        if target_rr_basis == "net":
            configured_target_net_rr = max(
                cfg.tp_target_policy_min_target_net_rr,
                min(cfg.tp_target_policy_max_target_net_rr, configured_target_net_rr),
            )
            desired_reward_bps = (
                configured_target_net_rr * planned_net_risk_bps
                + policy_entry_cost_bps
                + policy_tp_cost_bps
            )
            cap_reward_bps = (
                configured_target_rr_cap * planned_net_risk_bps
                + policy_entry_cost_bps
                + policy_tp_cost_bps
            )
        else:
            desired_reward_bps = configured_target_rr * gross_risk_bps
            cap_reward_bps = configured_target_rr_cap * gross_risk_bps
        if configured_target_rr <= 0 or configured_target_rr_cap <= 0:
            policy_reasons.append("invalid_fast_exit_rr_policy")
        if configured_target_net_rr is not None and configured_target_net_rr <= 0:
            policy_reasons.append("invalid_fast_exit_net_rr_policy")
        if configured_target_rr_cap < configured_target_rr:
            policy_reasons.append("fast_exit_rr_cap_below_target")
        if target_rr_basis == "net" and configured_target_rr_cap < configured_target_net_rr:
            policy_reasons.append("fast_exit_rr_cap_below_target_net_rr")
        if cap_reward_bps < min_policy_reward_bps:
            policy_reasons.append("reward_floor_exceeds_rr_cap")
        fast_exit_reward_bps = min(max(desired_reward_bps, min_policy_reward_bps), cap_reward_bps)
        market_room_required_bps = fast_exit_reward_bps + cfg.tp_target_policy_market_room_buffer_bps
        use_structure_runner = (
            policy_mode == "structure_or_capped_rr"
            and cfg.tp_target_policy_allow_structure_runner
            and raw_market_room_bps <= cap_reward_bps
            and raw_market_room_bps >= min_policy_reward_bps
        )
        if use_structure_runner:
            gross_reward_bps = raw_market_room_bps
            final_target_source = raw_target_source
            tp_cap_reason = "structure_target_within_fast_exit_cap"
        else:
            if (
                cfg.tp_target_policy_require_market_room
                and raw_market_room_bps < market_room_required_bps
            ):
                policy_reasons.append("market_room_insufficient_for_fast_exit")
            if raw_market_room_bps > fast_exit_reward_bps + cfg.tp_target_policy_market_room_buffer_bps:
                tp_was_capped = True
                tp_cap_reason = "structure_target_capped_to_fast_exit"
            gross_reward_bps = fast_exit_reward_bps
            final_target_source = "fast_capped_rr"
        reward_to_spread_ratio = (
            gross_reward_bps / spread_bps if spread_bps and spread_bps > 0 else None
        )
        if (
            reward_to_spread_ratio is not None
            and reward_to_spread_ratio < cfg.tp_target_policy_reward_to_spread_min
        ):
            policy_reasons.append("reward_too_close_to_spread")
        if policy_reasons:
            tp_reject_reason = policy_reasons[0]
        target_source = final_target_source
    if decision == "LONG":
        take = _price_from_bps(entry, gross_reward_bps, side="LONG", direction="up")
    else:
        take = _price_from_bps(entry, gross_reward_bps, side="SHORT", direction="up")
    reward = abs(take - entry)
    rr = reward / risk if risk > 0 else None
    net_risk_bps = planned_net_risk_bps
    net_reward_bps = gross_reward_bps - policy_entry_cost_bps - policy_tp_cost_bps
    net_rr = net_reward_bps / net_risk_bps if net_risk_bps > 0 else 0.0
    planned_loss_net_r = (
        (gross_risk_bps + policy_entry_cost_bps + policy_stop_cost_bps + policy_slippage_reserve_bps)
        / net_risk_bps
        if net_risk_bps > 0
        else None
    )
    effective_rr = net_rr
    fallback_target_only = raw_target_source == "fallback_target_bps"
    single_tp_reachable = (
        not fallback_target_only or cfg.allow_fallback_target_for_executable
    ) and gross_reward_bps >= cfg.min_reachable_reward_bps and not policy_reasons
    if gross_risk_bps < noise_floor_bps:
        stop_quality = "too_tight"
    elif gross_risk_bps > cfg.max_stop_bps:
        stop_quality = "too_wide"
    else:
        stop_quality = "valid"
    if single_tp_reachable and effective_rr >= cfg.min_effective_rr and stop_quality == "valid":
        trade_worthiness = "enter_now"
    elif stop_quality == "too_wide":
        trade_worthiness = "wait_price"
    elif single_tp_reachable:
        trade_worthiness = "wait_rr"
    elif fallback_target_only:
        trade_worthiness = "watch"
    else:
        trade_worthiness = "wait_price"
    guards.update(
        {
            "sl_tp_model_version": "10.63",
            "tp_model": "single_reachable_tp",
            "structure_stop": structure_stop,
            "structure_stop_bps": _bps(entry, entry - structure_stop),
            "raw_target_source": raw_target_source,
            "raw_market_room_bps": raw_market_room_bps,
            "raw_structure_target_bps": structure_target_bps,
            "raw_range_room_bps": range_room_bps,
            "raw_liquidity_target_bps": target_candidates.get("liquidity_target_bps"),
            "raw_expected_move_bps": expected_move_bps,
            "tp_target_policy_mode": policy_mode,
            "tp_target_policy_scope": "tp_only_after_upstream_sl_tp",
            "tp_target_policy_basis": target_rr_basis,
            "configured_target_rr": configured_target_rr,
            "configured_target_rr_cap": configured_target_rr_cap,
            "configured_target_net_rr": configured_target_net_rr,
            "min_target_net_rr": cfg.tp_target_policy_min_target_net_rr,
            "max_target_net_rr": cfg.tp_target_policy_max_target_net_rr,
            "risk_bps": gross_risk_bps,
            "desired_reward_bps": desired_reward_bps,
            "cap_reward_bps": cap_reward_bps,
            "min_reward_bps": min_policy_reward_bps,
            "market_room_buffer_bps": cfg.tp_target_policy_market_room_buffer_bps,
            "final_reward_bps": gross_reward_bps,
            "final_target_source": final_target_source,
            "tp_was_capped": tp_was_capped,
            "tp_cap_reason": tp_cap_reason,
            "tp_reject_reason": tp_reject_reason,
            "reward_to_spread_ratio": reward_to_spread_ratio,
            "gross_risk_bps": gross_risk_bps,
            "gross_reward_bps": gross_reward_bps,
            "net_risk_bps": net_risk_bps,
            "net_reward_bps": net_reward_bps,
            "policy_entry_cost_bps": policy_entry_cost_bps,
            "policy_tp_cost_bps": policy_tp_cost_bps,
            "policy_stop_cost_bps": policy_stop_cost_bps,
            "policy_slippage_reserve_bps": policy_slippage_reserve_bps,
            "gross_rr": rr,
            "net_rr": net_rr,
            "effective_rr": effective_rr,
            "final_gross_rr": rr,
            "final_net_rr": net_rr,
            "final_effective_rr": effective_rr,
            "planned_loss_net_r": planned_loss_net_r,
            "max_loss_net_r": cfg.tp_target_policy_max_loss_net_r,
            "r_sizing_basis": cfg.tp_target_policy_sizing_basis,
            "r_parity_overlay_enabled": target_rr_basis == "net" and policy_mode != "structure",
            "r_parity_model_version": "10.67",
            "min_effective_rr": cfg.min_effective_rr,
            "valid_risk_bps": net_risk_bps,
            "reachable_reward_bps": gross_reward_bps,
            "min_reachable_reward_bps": cfg.min_reachable_reward_bps,
            "available_room_bps": gross_reward_bps,
            "required_reward_bps": required_reward_bps,
            "single_tp_reachable": single_tp_reachable,
            "target_source": target_source,
            "target_source_candidates": target_candidates,
            "allow_fallback_target_for_executable": cfg.allow_fallback_target_for_executable,
            "fallback_target_only": fallback_target_only,
            "stop_quality": stop_quality,
            "trade_worthiness": trade_worthiness,
            "entry_chase_bps": 0.0,
            "entry_quality": "good" if trade_worthiness == "enter_now" else "chase",
            "tp1": _price_from_bps(entry, gross_risk_bps, side=decision, direction="up"),
            "tp2": None,
            "position_notional_usdt": getattr(liquidity, "notional_usdt", None),
        },
    )

    reasons: list[str] = []
    if gross_risk_bps < noise_floor_bps:
        reasons.append("stop_too_tight_noise_floor")
    if fallback_target_only:
        reasons.append("target_source_missing")
        reasons.append("fallback_target_audit_only")
        if not cfg.allow_fallback_target_for_executable:
            reasons.append("single_tp_not_reachable")
    if gross_reward_bps < cfg.min_reachable_reward_bps:
        reasons.append("target_space_not_enough")
    for reason in policy_reasons:
        if reason not in reasons:
            reasons.append(reason)
    if not single_tp_reachable:
        reasons.append("single_tp_not_reachable")
    if effective_rr < cfg.min_effective_rr:
        reasons.append("effective_rr_below_min")
    market_now_thresholds = _market_now_side_thresholds(cfg, decision)
    market_now_enabled = bool(market_now_thresholds.get("enabled"))
    effective_max_stop_bps = float(market_now_thresholds["max_stop_bps"]) if market_now_enabled else cfg.max_stop_bps
    guards["effective_max_stop_bps"] = effective_max_stop_bps
    if gross_risk_bps > effective_max_stop_bps:
        if market_now_enabled:
            reasons.append(f"{market_now_thresholds['prefix']}_stop_too_wide")
        else:
            reasons.append("short_now_stop_too_wide" if decision == "SHORT" else "stop_too_wide_for_horizon")
    if gross_reward_bps < required_reward_bps:
        reasons.append("range_room_not_enough")
    if net_reward_bps < cfg.min_tp_after_cost_bps:
        reasons.append("tp_after_cost_too_small")
    if net_rr < cfg.min_net_rr:
        reasons.append("net_rr_below_min")
    if entry_cost + tp_cost >= gross_reward_bps:
        reasons.append("market_entry_cost_dominates_reward")

    return stop, take, risk, reward, rr, guards, reasons


def build_trade_plan_item(
    factor: FactorSnapshotItem,
    *,
    line: TradePlanLineName,
    refresh: DecisionRefreshItem | None,
    liquidity: MarketEntryLiquidityItem | None,
    micro: MicroFeatureItem | None,
    micro_state: MicroDaemonSymbolState | None,
    factor_doc: FactorSnapshotDocument,
    refresh_doc: DecisionRefreshDocument | None,
    liquidity_doc: MarketEntryLiquidityDocument | None,
    micro_doc: LatestMicroFeaturesDocument | None,
    micro_state_doc: MicroDaemonStateDocument | None,
    micro_target_lineage: dict[str, Any] | None = None,
    trade_quality_prior: dict[str, Any] | None = None,
    cfg: TradePlanLineConfig,
) -> TradePlanItem:
    reasons: list[str] = []
    guards: dict[str, Any] = {}
    decision, wait_entry_mode = _side_to_decision(factor.move_side)
    if decision == "NO_TRADE":
        reasons.append("no_direction")

    score = int(factor.market_entry_suitability_score or factor.scan_score or 0)
    if score < cfg.min_score:
        reasons.append("score_too_low")

    refresh_fresh = False
    if refresh is None:
        reasons.append("refresh_missing")
        direction_still_valid = False
        range_room_ok = False
    else:
        refresh_fresh = "refresh_stale" not in refresh.reason_codes and refresh.refresh_age_sec <= cfg.max_refresh_age_sec
        direction_still_valid = refresh.direction_still_valid
        range_room_ok = refresh.range_room_ok
        if cfg.require_refresh_fresh and not refresh_fresh:
            reasons.append("refresh_stale")
        if cfg.require_direction_still_valid and not direction_still_valid:
            reasons.append("direction_invalid_after_refresh")
        if cfg.require_range_room_ok and not range_room_ok:
            reasons.append("range_room_insufficient_after_refresh")

    liquidity_ok = _side_liquidity_ok(decision=decision, liquidity=liquidity, refresh=refresh)
    if cfg.require_liquidity_ok:
        if liquidity_ok is not True:
            reasons.append("liquidity_not_ok")
            reasons.extend(_side_liquidity_reasons(decision=decision, liquidity=liquidity, refresh=refresh))
        if refresh is not None and refresh.liquidity_age_sec is not None and refresh.liquidity_age_sec > cfg.max_liquidity_age_sec:
            reasons.append("liquidity_stale")

    m15, micro_signal, micro_ready, micro_reasons = _micro_blocks(line, micro)
    _ = m15
    micro_align = None
    micro_signal_usable = None
    micro_direction_confirmed = None
    micro_exec_allowed = None
    micro_lifecycle = _micro_lifecycle_guards(
        line=line,
        micro=micro,
        micro_state=micro_state,
        micro_doc=micro_doc,
        cfg=cfg,
    )
    if not _without_micro_like(line):
        if micro_state_doc is None:
            guards["micro_state_missing"] = True
        elif micro_state is None:
            reasons.append("micro_state_symbol_missing")
        elif not micro_state.consumer_safe:
            reasons.extend(micro_state.consumer_reason_codes or ["micro_consumer_not_safe"])
        elif line == "micro_fast" and cfg.require_micro_ready and not micro_state.fast_ready:
            reasons.append("micro_state_fast_not_ready")
        elif line == "micro_full" and cfg.require_micro_ready and not micro_state.full_ready:
            reasons.append("full_warmup_incomplete")
            if micro_state.target_churn_state == "new":
                reasons.append("target_new_warmup")
        if cfg.max_micro_age_sec > 0 and micro_doc is not None:
            micro_age = age_sec_from_iso_z(micro_doc.generated_at)
            if micro_age > cfg.max_micro_age_sec:
                reasons.append(f"{line}_stale")
        if cfg.require_micro_ready and not micro_ready:
            reasons.append(f"{line}_not_ready")
        reasons.extend(_micro_data_quality_gate_reasons(micro_lifecycle))
        micro_signal_usable = micro_signal.micro_signal_usable if micro_signal is not None else False
        micro_direction_confirmed = micro_signal.micro_direction_confirmed if micro_signal is not None else False
        micro_exec_allowed = micro_signal.micro_exec_allowed if micro_signal is not None else False
        micro_align = micro_direction_confirmed
        relaxed_micro_consumable = bool(
            micro_lifecycle.get("trade_plan_consumable") and micro_lifecycle.get("micro_policy_relaxed"),
        )
        if cfg.require_micro_alignment and micro_ready and not micro_direction_confirmed and not relaxed_micro_consumable:
            reasons.append(f"{line}_not_confirmed")
        if cfg.require_micro_symbol_lifecycle_confirmed and not micro_lifecycle.get("trade_plan_consumable"):
            reasons.append("micro_symbol_lifecycle_not_confirmed")
            if not micro_lifecycle.get("micro_symbol_ready"):
                reasons.append("micro_symbol_not_ready")
            elif not micro_exec_allowed:
                reasons.append("micro_symbol_exec_not_allowed")

    guards.update(
        {
            "line": line,
            "gate_config_snapshot": _gate_config_snapshot(line=line, cfg=cfg, liquidity=liquidity, refresh=refresh),
            "score": score,
            "min_score": cfg.min_score,
            "refresh_fresh": refresh_fresh,
            "direction_still_valid": direction_still_valid,
            "range_room_ok": range_room_ok,
            "range_gate": getattr(refresh, "range_gate", {}) if refresh is not None else {},
            "liquidity_ok": liquidity_ok,
            "liquidity_gate": {
                "ok": liquidity_ok,
                "side": "buy" if decision == "LONG" else ("sell" if decision == "SHORT" else "unknown"),
                "notional_usdt": _liquidity_value(liquidity, refresh, "notional_usdt"),
                "spread_bps": _liquidity_value(liquidity, refresh, "spread_bps"),
                "estimated_slippage_bps": (
                    _liquidity_value(liquidity, refresh, "estimated_market_buy_slippage_bps")
                    if decision == "LONG"
                    else _liquidity_value(liquidity, refresh, "estimated_market_sell_slippage_bps")
                    if decision == "SHORT"
                    else None
                ),
                "top_depth_usdt": (
                    _liquidity_value(liquidity, refresh, "top_ask_depth_usdt")
                    if decision == "LONG"
                    else _liquidity_value(liquidity, refresh, "top_bid_depth_usdt")
                    if decision == "SHORT"
                    else None
                ),
                "reason_codes": _side_liquidity_reasons(decision=decision, liquidity=liquidity, refresh=refresh),
            },
            "allow_market_entry": cfg.allow_market_entry,
            "allow_market_now": cfg.allow_market_now,
            "allow_limit_pullback": cfg.allow_limit_pullback,
            "allow_breakout_trigger": cfg.allow_breakout_trigger,
            "target_rr": cfg.target_rr,
            "min_rr": cfg.min_rr,
            "stop_atr_mult": cfg.stop_atr_mult,
            "max_stop_atr_mult": cfg.max_stop_atr_mult,
            "min_stop_bps": cfg.min_stop_bps,
            "preferred_stop_bps": cfg.preferred_stop_bps,
            "max_stop_bps": cfg.max_stop_bps,
            "min_net_rr": cfg.min_net_rr,
            "conditional_plan_expire_sec": cfg.conditional_plan_expire_sec,
        },
    )
    guards["trade_quality_gate"] = _trade_quality_gate_guard(
        symbol=factor.symbol,
        decision=decision,
        cfg=cfg,
        prior=trade_quality_prior,
    )
    guards["sl_tp_quality"] = _sl_tp_quality_guard(
        symbol=factor.symbol,
        decision=decision,
        cfg=cfg,
        prior=trade_quality_prior,
    )
    if _without_micro_like(line):
        guards["without_micro_executable_enabled"] = cfg.allow_market_entry
        guards["micro_confirmation"] = False
        guards["without_micro_no_micro_confirmation"] = True
    if not _without_micro_like(line):
        guards["micro_ready"] = micro_ready
        guards["micro_alignment_ok"] = micro_align
        guards["micro_signal_usable"] = micro_signal_usable
        guards["micro_direction_confirmed"] = micro_direction_confirmed
        guards["micro_exec_allowed"] = micro_exec_allowed
        guards.update(micro_lifecycle)
        guards.update(_micro_signal_guards(micro_signal))

    can_enter = cfg.allow_market_entry and cfg.allow_market_now and not reasons
    entry = refresh.last_price if refresh is not None else None
    stop = take = risk = reward = rr = None
    price_reasons: list[str] = []
    price_guards: dict[str, Any] = {}
    market_now_thresholds = _market_now_side_thresholds(cfg, decision)
    market_now_enabled = bool(market_now_thresholds.get("enabled"))
    if market_now_enabled:
        guards["market_now_calibration_status"] = "not_reached"
        guards["market_now_pre_calibration_blocker"] = (reasons[0] if reasons else "")
    if can_enter:
        stop, take, risk, reward, rr, price_guards, price_reasons = _price_plan(
            decision=decision,
            entry=entry,
            refresh=refresh,
            liquidity=liquidity,
            factor=factor,
            cfg=cfg,
        )
        guards.update(price_guards)
        guards["sl_tp_quality"] = {
            **_sl_tp_quality_guard(
                symbol=factor.symbol,
                decision=decision,
                cfg=cfg,
                prior=trade_quality_prior,
            ),
            **dict(guards.get("sl_tp_quality") or {}),
        }
        if cfg.sl_tp_quality_enabled and cfg.sl_tp_quality_mode == "apply":
            stop, take, adjusted_risk, adjusted_reward, adjusted_rr, sl_tp_quality = _apply_sl_tp_quality_adjustment(
                decision=decision,
                entry=entry,
                stop=stop,
                take=take,
                guards=guards,
                cfg=cfg,
            )
            risk = adjusted_risk if adjusted_risk is not None else risk
            reward = adjusted_reward if adjusted_reward is not None else reward
            rr = adjusted_rr if adjusted_rr is not None else rr
            guards["sl_tp_quality"] = sl_tp_quality
            if entry is not None and risk is not None and reward is not None:
                gross_risk_bps = _bps(entry, risk)
                gross_reward_bps = _bps(entry, reward)
                entry_cost = float(guards.get("entry_cost_bps") or 0.0)
                stop_cost = float(guards.get("stop_exit_cost_bps") or 0.0)
                tp_cost = float(guards.get("take_profit_exit_cost_bps") or 0.0)
                net_risk_bps = gross_risk_bps + entry_cost + stop_cost
                net_reward_bps = gross_reward_bps - entry_cost - tp_cost
                net_rr = net_reward_bps / net_risk_bps if net_risk_bps > 0 else 0.0
                guards.update(
                    {
                        "gross_risk_bps": gross_risk_bps,
                        "gross_reward_bps": gross_reward_bps,
                        "net_risk_bps": net_risk_bps,
                        "net_reward_bps": net_reward_bps,
                        "gross_rr": rr,
                        "net_rr": net_rr,
                        "effective_rr": net_rr,
                        "reachable_reward_bps": gross_reward_bps,
                        "available_room_bps": gross_reward_bps,
                    },
                )
        reasons.extend(price_reasons)
        if risk is not None:
            e1 = refresh.entry_1m if refresh is not None else factor.entry_1m
            p15 = refresh.primary_15m if refresh is not None else factor.primary_15m
            atr = _f(e1.get("atr")) or _f(p15.get("atr"))
            market_now_thresholds = _market_now_side_thresholds(cfg, decision)
            market_now_enabled = bool(market_now_thresholds.get("enabled"))
            effective_max_stop_atr_mult = (
                float(market_now_thresholds["max_stop_atr_mult"]) if market_now_enabled else cfg.max_stop_atr_mult
            )
            guards["effective_max_stop_atr_mult"] = effective_max_stop_atr_mult
            if atr is not None and atr > 0 and risk > effective_max_stop_atr_mult * atr:
                if market_now_enabled:
                    reasons.append(f"{market_now_thresholds['prefix']}_stop_atr_too_wide")
                else:
                    reasons.append("short_now_stop_too_wide" if decision == "SHORT" else "stop_too_wide")
        market_now_thresholds = _market_now_side_thresholds(cfg, decision)
        market_now_reasons = _market_now_calibration_reasons(
            cfg=cfg,
            decision=decision,
            refresh=refresh,
            liquidity=liquidity,
            guards=guards,
        )
        if market_now_thresholds.get("enabled"):
            guards["market_now_calibration_status"] = "blocked" if market_now_reasons else "passed"
            guards["market_now_pre_calibration_blocker"] = ""
            guards["market_now_calibration"] = {
                "enabled": True,
                "ok": not market_now_reasons,
                "side": decision,
                "reason_codes": market_now_reasons,
                "min_range_pos": market_now_thresholds["min_range_pos"],
                "max_range_pos": market_now_thresholds["max_range_pos"],
                "min_available_room_bps": market_now_thresholds["min_available_room_bps"],
                "max_stop_bps": market_now_thresholds["max_stop_bps"],
                "max_stop_atr_mult": market_now_thresholds["max_stop_atr_mult"],
                "min_net_rr": market_now_thresholds["min_net_rr"],
                "max_spread_bps": market_now_thresholds["max_spread_bps"],
                "max_slippage_bps": market_now_thresholds["max_slippage_bps"],
                "legacy_short_now_fallback": bool(
                    decision == "SHORT"
                    and cfg.short_now_calibration_enabled
                    and not cfg.market_now_calibration_enabled
                    and cfg.market_now_legacy_short_now_fallback
                ),
            }
            reasons.extend(r for r in market_now_reasons if r not in reasons)
        if decision == "SHORT" and cfg.short_now_calibration_enabled:
            short_now_reasons = _market_now_calibration_reasons(
                cfg=cfg,
                decision="SHORT",
                refresh=refresh,
                liquidity=liquidity,
                guards=guards,
            )
            guards["short_now_calibration"] = {
                "enabled": True,
                "ok": not short_now_reasons,
                "reason_codes": short_now_reasons,
                "min_range_pos": cfg.short_now_min_range_pos,
                "max_range_pos": cfg.short_now_max_range_pos,
                "min_available_room_bps": cfg.short_now_min_available_room_bps,
                "max_stop_bps": cfg.short_now_max_stop_bps,
                "max_stop_atr_mult": cfg.short_now_max_stop_atr_mult,
                "min_net_rr": cfg.short_now_min_net_rr,
                "max_spread_bps": cfg.short_now_max_spread_bps,
                "max_slippage_bps": cfg.short_now_max_slippage_bps,
            }
            reasons.extend(r for r in short_now_reasons if r not in reasons)
        if rr is not None and rr < cfg.min_rr:
            reasons.append("rr_too_low")
        can_enter = not reasons and rr is not None
        quality_gate = _trade_quality_gate_guard(
            symbol=factor.symbol,
            decision=decision,
            cfg=cfg,
            prior=trade_quality_prior,
        )
        guards["trade_quality_gate"] = quality_gate
        if can_enter and quality_gate.get("enforced"):
            mode = str(quality_gate.get("mode") or "off")
            if mode == "block_executable":
                reasons.append("trade_quality_gate_blocked")
            elif quality_gate.get("promotion_policy_active"):
                reasons.append("trade_quality_promotion_wait_only")
            else:
                reasons.append("trade_quality_gate_wait_signal_no_edge")
            can_enter = False
        if (
            not can_enter
            and cfg.allow_market_entry
            and not cfg.allow_limit_pullback
            and decision != "NO_TRADE"
            and entry is not None
            and refresh_fresh
            and direction_still_valid
        ):
            guards.update(
                {
                    "market_entry_allowed": False,
                    "limit_entry_allowed": False,
                    "conditional_entry_allowed": False,
                    "better_entry_required": True,
                    "better_entry_reason": "better_entry_required_for_net_rr",
                },
            )
            bad_price_reason = (
                "short_now_market_entry_bad_price_wait_rebound"
                if decision == "SHORT"
                else "long_now_market_entry_bad_price_wait_pullback"
                if cfg.market_now_calibration_enabled
                else "market_entry_bad_price_wait_pullback"
            )
            if bad_price_reason not in reasons:
                reasons.append(bad_price_reason)
            if "better_entry_required_for_net_rr" not in reasons:
                reasons.append("better_entry_required_for_net_rr")
            if "market_only_no_pending" not in reasons:
                reasons.append("market_only_no_pending")

    if can_enter:
        action = "ENTER_MARKET"
        entry_mode = "MARKET"
        executable = True
        est_entry = entry
        if not _without_micro_like(line):
            guards["micro_lifecycle_state"] = "emitted"
            guards["micro_symbol_trade_plan_emitted"] = True
            guards["micro_lifecycle_terminal"] = True
            guards["trade_plan_consumable"] = True
            guards["consumption_block_reason"] = ""
        guards["opportunity_level"] = 4
        guards["opportunity_type"] = "MARKET_EXECUTABLE"
        guards["market_entry_allowed"] = True
        guards["plan_expire_reason"] = "price_stale"
    elif (
        cfg.allow_market_entry
        and cfg.allow_limit_pullback
        and decision != "NO_TRADE"
        and entry is not None
        and refresh_fresh
        and direction_still_valid
    ):
        limit_mode = "LIMIT_PULLBACK" if decision == "LONG" else "LIMIT_REBOUND"
        action = "ENTER_LIMIT"
        entry_mode = limit_mode
        executable = False
        if not price_guards:
            _, _, _, _, _, price_guards, price_reasons = _price_plan(
                decision=decision,
                entry=entry,
                refresh=refresh,
                liquidity=liquidity,
                factor=factor,
                cfg=cfg,
            )
            guards.update(price_guards)
        pullback_bps = min(cfg.max_pullback_bps, max(cfg.min_stop_bps * 0.5, guards.get("entry_cost_bps", 0.0) * 2.0))
        better_entry = (
            _price_from_bps(entry, pullback_bps, side="LONG", direction="down")
            if decision == "LONG"
            else _price_from_bps(entry, pullback_bps, side="SHORT", direction="down")
        )
        guards.update(
            {
                "opportunity_level": 2,
                "opportunity_type": "LIMIT_PULLBACK" if decision == "LONG" else "LIMIT_REBOUND",
                "market_entry_allowed": False,
                "limit_entry_allowed": True,
                "conditional_entry_allowed": True,
                "better_entry_required": True,
                "better_entry_price": better_entry,
                "better_entry_reason": "better_entry_required_for_net_rr",
                "time_in_force_sec": cfg.conditional_plan_expire_sec,
                "plan_expire_reason": "price_stale",
            },
        )
        est_entry = better_entry
        bad_price_reason = (
            "short_now_market_entry_bad_price_wait_rebound"
            if decision == "SHORT"
            else "market_entry_bad_price_wait_pullback"
        )
        if bad_price_reason not in reasons:
            reasons.append(bad_price_reason)
        if "better_entry_required_for_net_rr" not in reasons:
            reasons.append("better_entry_required_for_net_rr")
        if "limit_entry_available" not in reasons:
            reasons.append("limit_entry_available")
    elif cfg.allow_breakout_trigger and decision != "NO_TRADE" and entry is not None and refresh_fresh:
        action = "WAIT"
        entry_mode = "BREAKOUT_TRIGGER" if decision == "LONG" else "BREAKDOWN_TRIGGER"
        executable = False
        est_entry = None
        p15 = refresh.primary_15m if refresh is not None else factor.primary_15m
        e1 = refresh.entry_1m if refresh is not None else factor.entry_1m
        trigger = (
            _f(p15.get("breakout_level"))
            or _f(p15.get("recent_swing_high"))
            or _f(e1.get("last_breakout_high"))
            or entry
            if decision == "LONG"
            else _f(p15.get("breakdown_level"))
            or _f(p15.get("recent_swing_low"))
            or _f(e1.get("last_breakdown_low"))
            or entry
        )
        guards.update(
            {
                "opportunity_level": 2,
                "opportunity_type": "BREAKOUT_TRIGGER" if decision == "LONG" else "BREAKDOWN_TRIGGER",
                "market_entry_allowed": False,
                "limit_entry_allowed": False,
                "conditional_entry_allowed": True,
                "trigger_price": trigger,
                "trigger_condition": "breakout_confirmed" if decision == "LONG" else "breakdown_confirmed",
                "time_in_force_sec": cfg.conditional_plan_expire_sec,
                "plan_expire_reason": "trigger_timeout",
            },
        )
        if "breakout_trigger_required" not in reasons:
            reasons.append("breakout_trigger_required")
    elif cfg.allow_wait_plan and decision != "NO_TRADE":
        action = "WAIT"
        entry_mode = wait_entry_mode if _without_micro_like(line) else "WAIT_CONFIRMATION"
        executable = False
        est_entry = None
        guards.update(
            {
                "opportunity_level": 1,
                "opportunity_type": "WAIT_FOR_RETEST" if _without_micro_like(line) else "WAIT_FOR_CONFIRMATION",
                "market_entry_allowed": False,
                "limit_entry_allowed": False,
                "conditional_entry_allowed": False,
                "time_in_force_sec": cfg.conditional_plan_expire_sec,
                "plan_expire_reason": "freshness_expired",
            },
        )
        if not reasons:
            reasons.append("wait_plan_only")
    else:
        action = "NO_TRADE"
        entry_mode = "NONE"
        executable = False
        est_entry = None
        guards.update(
            {
                "opportunity_level": 0,
                "opportunity_type": "NO_TRADE",
                "market_entry_allowed": False,
                "limit_entry_allowed": False,
                "conditional_entry_allowed": False,
            },
        )
        if not reasons:
            reasons.append("no_trade")

    position_sizing: dict[str, Any] | None = None
    if executable:
        position_sizing, sizing_reject = _build_position_sizing(
            cfg=cfg,
            entry=est_entry,
            stop=stop,
            take=take,
            risk_per_unit=risk,
            reward_per_unit=reward,
        )
        if sizing_reject is not None:
            executable = False
            reasons.append(sizing_reject)
            guards["position_sizing_reject_reason"] = (
                position_sizing or {}
            ).get("sizing_reject_reason", sizing_reject)
        if position_sizing:
            guards.update(
                {
                    "planned_loss_usdt": position_sizing.get("planned_loss_usdt")
                    or position_sizing.get("gross_risk_usdt"),
                    "planned_profit_usdt": position_sizing.get("planned_profit_usdt")
                    or position_sizing.get("gross_reward_usdt"),
                    "estimated_max_loss_usdt": position_sizing.get("estimated_max_loss_usdt"),
                    "planned_notional_usdt": position_sizing.get("planned_notional_usdt")
                    or position_sizing.get("notional_usdt"),
                    "planned_quantity": position_sizing.get("planned_quantity")
                    or position_sizing.get("quantity"),
                    "target_planned_loss_usdt": position_sizing.get("target_planned_loss_usdt"),
                    "max_planned_loss_usdt": position_sizing.get("max_planned_loss_usdt"),
                    "loss_cap_applied": position_sizing.get("loss_cap_applied"),
                    "sizing_policy": position_sizing.get("method"),
                    "paper_fallback_notional_allowed": position_sizing.get("paper_fallback_notional_allowed"),
                },
            )
    elif cfg.position_sizing_enabled:
        position_sizing = {
            "method": cfg.position_sizing_method,
            "enabled": True,
            "sizing_reject_reason": "plan_not_executable",
        }

    input_refs: dict[str, Any] = {
        "factor_generated_at": factor_doc.generated_at,
        "refresh_generated_at": refresh_doc.generated_at if refresh_doc is not None else None,
        "liquidity_generated_at": liquidity_doc.generated_at if liquidity_doc is not None else None,
    }
    if refresh_doc is not None:
        input_refs["refresh_snapshot_path"] = refresh_doc.input_refs.get("refresh_snapshot_path")
        input_refs["refresh_light_snapshot_path"] = refresh_doc.input_refs.get("light_path")
        input_refs["refresh_liquidity_snapshot_path"] = refresh_doc.input_refs.get("liquidity_path")
    if not _without_micro_like(line):
        input_refs["micro_generated_at"] = micro_doc.generated_at if micro_doc is not None else None
        input_refs["micro_reason_codes"] = micro_reasons
        input_refs["micro_lifecycle_scope"] = "symbol"
        input_refs["micro_lifecycle_state"] = guards.get("micro_lifecycle_state")
        input_refs["micro_lifecycle_path"] = f"DATA/micro/latest_micro_lifecycle_{line}.json"
        input_refs["micro_state_generated_at"] = micro_state_doc.generated_at if micro_state_doc is not None else None
        input_refs["micro_target_generated_at"] = (
            micro_state_doc.target_generated_at if micro_state_doc is not None else None
        )
        input_refs["micro_target_version"] = micro_state_doc.target_version if micro_state_doc is not None else None
        input_refs["micro_consumer_safe"] = micro_state.consumer_safe if micro_state is not None else None
        input_refs["continuous_collect_sec"] = (
            micro_state.continuous_collect_sec if micro_state is not None else None
        )
        input_refs["full_ready_eta_sec"] = micro_state.full_ready_eta_sec if micro_state is not None else None
        if micro_target_lineage:
            input_refs.update(micro_target_lineage)

    return TradePlanItem(
        symbol=factor.symbol,
        decision_tf=factor.decision_tf,
        decision=decision,  # type: ignore[arg-type]
        action=action,  # type: ignore[arg-type]
        entry_mode=entry_mode,  # type: ignore[arg-type]
        estimated_entry_price=est_entry if executable else None,
        stop_loss=stop if executable else None,
        take_profit=take if executable else None,
        risk_per_unit=risk if executable else None,
        reward_per_unit=reward if executable else None,
        rr=rr if executable else None,
        executable=executable,
        confidence=max(0, min(100, int((score + factor.scan_score) / 2))),
        reason_codes=sorted(set(reasons)),
        position_sizing=position_sizing,
        guards=guards,
        input_refs=input_refs,
    )


def build_trade_plan_line_document(
    *,
    line: TradePlanLineName,
    factor_doc: FactorSnapshotDocument,
    refresh_doc: DecisionRefreshDocument | None,
    liquidity_doc: MarketEntryLiquidityDocument | None,
    micro_doc: LatestMicroFeaturesDocument | None,
    micro_state_doc: MicroDaemonStateDocument | None = None,
    generated_at: str,
    run_id: str | None = None,
    cycle_id: str | None = None,
    micro_target_lineage: dict[str, Any] | None = None,
    micro_wait_evidence_refs: dict[str, Any] | None = None,
    cfg: TradePlanLineConfig | None = None,
    project_root: Path | None = None,
) -> TradePlanLineDocument:
    base_line = "without_micro" if line in {"strategy4", "strategy5", "strategy6"} else line
    c = cfg or DEFAULT_CONFIGS[base_line]
    refresh_by_symbol = {_key(it.symbol): it for it in refresh_doc.items} if refresh_doc is not None else {}
    liquidity_by_symbol = {_key(it.symbol): it for it in liquidity_doc.items} if liquidity_doc is not None else {}
    micro_by_symbol = {_key(it.symbol): it for it in micro_doc.items} if micro_doc is not None else {}
    micro_state_by_symbol = (
        {_key(it.symbol): it for it in micro_state_doc.symbols} if micro_state_doc is not None else {}
    )
    trade_quality_prior = _trade_quality_prior_for_symbols(
        project_root,
        line=line,
        symbols={_key(it.symbol) for it in factor_doc.items},
    )
    all_plans_raw = [
        build_trade_plan_item(
            it,
            line=line,
            refresh=refresh_by_symbol.get(_key(it.symbol)),
            liquidity=liquidity_by_symbol.get(_key(it.symbol)),
            micro=micro_by_symbol.get(_key(it.symbol)),
            micro_state=micro_state_by_symbol.get(_key(it.symbol)),
            factor_doc=factor_doc,
            refresh_doc=refresh_doc,
            liquidity_doc=liquidity_doc,
            micro_doc=micro_doc,
            micro_state_doc=micro_state_doc,
            micro_target_lineage=micro_target_lineage,
            trade_quality_prior=trade_quality_prior,
            cfg=c,
        )
        for it in factor_doc.items
    ]
    all_plans: list[TradePlanItem] = []
    invalid_symbol_items: list[dict[str, Any]] = []
    symbol_profiles = _governance_profiles_by_symbol(project_root)
    for plan in all_plans_raw:
        profile = symbol_profiles.get(plan.symbol.upper())
        plan = _apply_symbol_risk_contract(plan, profile, cfg=c)
        plan = _ensure_profile_guard_contract(plan, profile, cfg=c)
        contract = validate_exchange_symbol(
            plan.symbol,
            project_root=project_root,
            fail_closed_on_missing_whitelist=project_root is not None,
        )
        guards = dict(plan.guards)
        guards.update(contract.guards())
        if not contract.ok:
            invalid_symbol_items.append(
                {
                    "symbol": plan.symbol,
                    "symbol_raw": contract.raw_symbol,
                    "symbol_normalized": contract.normalized_symbol,
                    "state": "blocked",
                    "trade_plan_consumable": False,
                    "reason_codes": sorted(set([*plan.reason_codes, "invalid_exchange_symbol", contract.reason])),
                    "symbol_contract_ok": False,
                    "symbol_contract_reason": contract.reason,
                    "symbol_contract_source": contract.source,
                    "executable": False,
                },
            )
            continue
        all_plans.append(plan.model_copy(update={"guards": guards}))
    if _without_micro_like(line):
        plans = all_plans
    else:
        plans = [p for p in all_plans if p.guards.get("trade_plan_consumable") is True]
    exe = sum(1 for p in plans if p.executable)
    if not plans:
        status = "no_entries"
    elif exe == len(plans):
        status = "ok"
    elif any("refresh_stale" in p.reason_codes or f"{line}_stale" in p.reason_codes for p in plans):
        status = "stale_input"
    elif exe == 0:
        status = "no_entries"
    else:
        status = "partial"

    input_refs: dict[str, Any] = {
        "factor_generated_at": factor_doc.generated_at,
        "refresh_generated_at": refresh_doc.generated_at if refresh_doc is not None else None,
        "liquidity_generated_at": liquidity_doc.generated_at if liquidity_doc is not None else None,
        "trade_quality_prior_available": bool(trade_quality_prior.get("available")),
        "trade_quality_prior_sample_count": int(trade_quality_prior.get("sample_count") or 0),
        "trade_quality_prior_source": trade_quality_prior.get("source"),
    }
    if invalid_symbol_items:
        input_refs["invalid_symbol_count"] = len(invalid_symbol_items)
        input_refs["invalid_symbols"] = [str(item.get("symbol")) for item in invalid_symbol_items]
        input_refs["invalid_symbol_items"] = invalid_symbol_items
        input_refs["symbol_contract_status"] = "blocked_invalid_symbols"
    else:
        input_refs["invalid_symbol_count"] = 0
        input_refs["symbol_contract_status"] = "ok"
    if refresh_doc is not None:
        input_refs["refresh_snapshot_path"] = refresh_doc.input_refs.get("refresh_snapshot_path")
        input_refs["refresh_light_snapshot_path"] = refresh_doc.input_refs.get("light_path")
        input_refs["refresh_liquidity_snapshot_path"] = refresh_doc.input_refs.get("liquidity_path")
    if not _without_micro_like(line):
        input_refs["micro_generated_at"] = micro_doc.generated_at if micro_doc is not None else None
        input_refs["micro_lifecycle_path"] = f"DATA/micro/latest_micro_lifecycle_{line}.json"
        input_refs["micro_state_generated_at"] = micro_state_doc.generated_at if micro_state_doc is not None else None
        input_refs["micro_target_generated_at"] = (
            micro_state_doc.target_generated_at if micro_state_doc is not None else None
        )
        input_refs["micro_target_version"] = micro_state_doc.target_version if micro_state_doc is not None else None
        if micro_target_lineage:
            input_refs.update(micro_target_lineage)
        if micro_wait_evidence_refs:
            input_refs.update(micro_wait_evidence_refs)
        input_refs.update(_micro_line_lifecycle_summary(line=line, plans=all_plans))
        input_refs.update(_micro_lifecycle_consumption_refs(line=line, all_plans=all_plans, consumed_plans=plans, cfg=c))
        if invalid_symbol_items:
            micro_excluded = list(input_refs.get("micro_lifecycle_excluded_items") or [])
            micro_excluded.extend(invalid_symbol_items)
            input_refs["micro_lifecycle_excluded_items"] = micro_excluded
            input_refs["micro_lifecycle_excluded_symbols"] = [
                str(item.get("symbol")) for item in micro_excluded if isinstance(item, dict)
            ]
    candidate_alignment: dict[str, Any] = {}
    if line in ("micro_fast", "micro_full"):
        factor_symbols = {_key(it.symbol) for it in factor_doc.items}
        micro_feature_symbols = {_key(it.symbol) for it in micro_doc.items} if micro_doc is not None else set()
        micro_state_symbols = {_key(it.symbol) for it in micro_state_doc.symbols} if micro_state_doc is not None else set()
        factor_alignment = dict(factor_doc.candidate_alignment or {})
        candidate_alignment = {
            "mode": factor_alignment.get("mode") or "micro_targets_authoritative",
            "factor_symbol_count": len(factor_symbols),
            "micro_target_symbol_count": factor_alignment.get("allowed_symbol_count"),
            "missing_micro_feature_count": len(factor_symbols - micro_feature_symbols),
            "missing_micro_state_count": len(factor_symbols - micro_state_symbols),
            "missing_micro_feature_symbols": sorted(factor_symbols - micro_feature_symbols)[:50],
            "missing_micro_state_symbols": sorted(factor_symbols - micro_state_symbols)[:50],
            "factor_candidate_alignment": factor_alignment,
        }

    return TradePlanLineDocument(
        generated_at=generated_at,
        run_id=run_id,
        cycle_id=cycle_id,
        source=LINE_SOURCE[line],
        micro_mode={
            "without_micro": "none",
            "micro_fast": "fast",
            "micro_full": "full",
            "strategy4": "none",
            "strategy5": "strategy5_evidence",
            "strategy6": "strategy6_market_accepted",
        }[line],  # type: ignore[arg-type]
        status=status,  # type: ignore[arg-type]
        count=len(plans),
        executable_count=exe,
        input_refs=input_refs,
        candidate_alignment=candidate_alignment,
        plans=plans,
    )


def _micro_lifecycle_output_path(root: Path, line: TradePlanLineName) -> Path:
    return root / "DATA" / "micro" / f"latest_micro_lifecycle_{line}.json"


def _write_micro_lifecycle_document(
    *,
    root: Path,
    line: TradePlanLineName,
    doc: TradePlanLineDocument,
) -> None:
    if _without_micro_like(line):
        return
    items: list[dict[str, Any]] = []
    for plan in doc.plans:
        items.append(_micro_lifecycle_item_from_plan(plan))
    excluded = doc.input_refs.get("micro_lifecycle_excluded_items")
    if isinstance(excluded, list):
        for row in excluded:
            if isinstance(row, dict) and row.get("symbol"):
                items.append(dict(row))
    counts: dict[str, int] = {}
    for item in items:
        state = str(item.get("state") or "unknown")
        counts[state] = counts.get(state, 0) + 1
    payload = {
        "schema_version": "10.35",
        "source": "symbol_level_micro_lifecycle",
        "strategy_line": line,
        "run_id": doc.run_id,
        "cycle_id": doc.cycle_id,
        "target_set_id": doc.input_refs.get("micro_target_set_id"),
        "generated_at": doc.generated_at,
        "count": len(items),
        "state_counts": counts,
        "line_exec_status": doc.input_refs.get("line_exec_status"),
        "line_lifecycle_status": doc.input_refs.get("line_lifecycle_status"),
        "line_lifecycle_complete": doc.input_refs.get("line_lifecycle_complete"),
        "trade_plan_allowed": doc.input_refs.get("trade_plan_allowed"),
        "micro_consumption_policy": doc.input_refs.get("micro_consumption_policy"),
        "consumed_symbols": doc.input_refs.get("micro_lifecycle_consumed_symbols"),
        "excluded_symbols": doc.input_refs.get("micro_lifecycle_excluded_symbols"),
        "excluded_counts": doc.input_refs.get("micro_lifecycle_excluded_counts"),
        "unfinished_symbol_count": doc.input_refs.get("unfinished_symbol_count"),
        "symbol_counts": doc.input_refs.get("symbol_counts"),
        "items": items,
    }
    out_p = _micro_lifecycle_output_path(root, line)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(out_p, payload)


def _raw_symbol_map(doc: dict[str, Any], key: str = "items") -> dict[str, dict[str, Any]]:
    rows = doc.get(key)
    if not isinstance(rows, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get("symbol"):
            continue
        out[_key(str(row["symbol"]))] = row
    return out


def _raw_reason_codes(value: Any) -> list[str]:
    if isinstance(value, dict):
        got = value.get("reason_codes")
        if isinstance(got, list):
            return [str(x) for x in got if str(x)]
    return []


def _raw_bool(value: Any, key: str, default: bool = False) -> bool:
    if isinstance(value, dict) and key in value:
        return bool(value.get(key))
    return default


def _raw_int(value: Any, key: str) -> int | None:
    if not isinstance(value, dict):
        return None
    raw = value.get(key)
    if isinstance(raw, bool) or raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _required_observed_sec_from_quality(quality: dict[str, Any] | None, fallback: int = 900) -> int:
    if not isinstance(quality, dict):
        return fallback
    coverage = quality.get("coverage")
    vals: list[int] = []
    if isinstance(coverage, dict):
        for row in coverage.values():
            if isinstance(row, dict):
                got = _raw_int(row, "expected_seconds")
                if got is not None:
                    vals.append(got)
    return max(vals) if vals else fallback


def write_micro_timeout_lifecycle_document(
    *,
    line: TradePlanLineName,
    project_root: Path | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    reason_codes: list[str] | None = None,
    runtime_health: dict[str, Any] | None = None,
) -> Path | None:
    if _without_micro_like(line):
        return None
    root = (project_root or Path.cwd()).resolve()
    target_items, target_doc = _read_micro_target_items(root)
    lineage = _read_micro_target_lineage(root)
    feature_doc: dict[str, Any] = {}
    state_doc: dict[str, Any] = {}
    for path, holder in (
        (_default_micro_path(root), "feature"),
        (_default_micro_state_path(root), "state"),
    ):
        try:
            raw = read_json_object(path)
        except (OSError, ValueError, TypeError):
            raw = {}
        if holder == "feature":
            feature_doc = raw if isinstance(raw, dict) else {}
        else:
            state_doc = raw if isinstance(raw, dict) else {}
    feature_by_symbol = _raw_symbol_map(feature_doc, "items")
    state_by_symbol = _raw_symbol_map(state_doc, "symbols")
    plan_reasons = list(reason_codes or ["micro_full_wait_timeout"])
    items: list[dict[str, Any]] = []
    for target in target_items:
        symbol = _key(str(target.get("symbol") or ""))
        if not symbol:
            continue
        feature = feature_by_symbol.get(symbol)
        state = state_by_symbol.get(symbol)
        if line == "micro_fast":
            quality = feature.get("micro_fast_quality") if isinstance(feature, dict) else None
            signal = feature.get("micro_fast_signal") if isinstance(feature, dict) else None
            state_ready_key = "fast_ready"
            state_reasons_key = "fast_reason_codes"
            required_fallback = 180
        else:
            quality = feature.get("micro_full_quality") if isinstance(feature, dict) else None
            signal = feature.get("micro_full_signal") if isinstance(feature, dict) else None
            state_ready_key = "full_ready"
            state_reasons_key = "full_reason_codes"
            required_fallback = 900
        ready = _raw_bool(quality, "ready", _raw_bool(state, state_ready_key, False))
        observed = _raw_int(quality, "warmup_age_sec")
        if observed is None:
            observed = _raw_int(state, "continuous_collect_sec")
        required = _required_observed_sec_from_quality(
            quality if isinstance(quality, dict) else None,
            fallback=required_fallback,
        )
        reasons: list[str] = []
        if isinstance(state, dict) and isinstance(state.get(state_reasons_key), list):
            reasons.extend(str(x) for x in state[state_reasons_key] if str(x))
        reasons.extend(_raw_reason_codes(quality))
        reasons.extend(_raw_reason_codes(signal))
        if feature is None and state is None:
            reasons.append("micro_symbol_missing")
        reasons.extend(plan_reasons)
        items.append(
            {
                "symbol": symbol,
                "state": "timeout",
                "terminal": True,
                "ready": ready,
                "confirmed": False,
                "trade_plan_consumable": False,
                "consumption_block_reason": "micro_symbol_timeout",
                "trade_plan_emitted": False,
                "executable": False,
                "observed_sec": observed,
                "required_observed_sec": required,
                "micro_signal_usable": _raw_bool(signal, "micro_signal_usable", False),
                "micro_direction_confirmed": _raw_bool(signal, "micro_direction_confirmed", False),
                "micro_exec_allowed": _raw_bool(signal, "micro_exec_allowed", False),
                "reason_codes": list(dict.fromkeys(reasons)),
                "plan_reason_codes": list(dict.fromkeys(plan_reasons)),
                "tier": target.get("tier"),
                "source_state": target.get("source_state"),
                "move_side": target.get("move_side"),
            },
        )
    payload = {
        "schema_version": "10.35",
        "source": "symbol_level_micro_lifecycle",
        "strategy_line": line,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "target_set_id": lineage.get("micro_target_set_id") or target_doc.get("target_set_id"),
        "candidate_hash": lineage.get("micro_candidate_hash") or target_doc.get("candidate_hash"),
        "generated_at": to_iso_z(utc_now()),
        "status": "timeout",
        "runtime_health": runtime_health or {},
        "count": len(items),
        "state_counts": {"timeout": len(items)},
        "items": items,
    }
    out_p = _micro_lifecycle_output_path(root, line)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(out_p, payload)
    return out_p


def write_blocked_micro_lifecycle_document(
    *,
    line: TradePlanLineName,
    project_root: Path | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    blocked_reason: str = "micro_no_consumable_symbol",
    reason_codes: list[str] | None = None,
    runtime_health: dict[str, Any] | None = None,
) -> Path | None:
    if _without_micro_like(line):
        return None
    root = (project_root or Path.cwd()).resolve()
    target_items, target_doc = _read_micro_target_items(root)
    lineage = _read_micro_target_lineage(root)
    feature_doc: dict[str, Any] = {}
    state_doc: dict[str, Any] = {}
    for path, holder in (
        (_default_micro_path(root), "feature"),
        (_default_micro_state_path(root), "state"),
    ):
        try:
            raw = read_json_object(path)
        except (OSError, ValueError, TypeError):
            raw = {}
        if holder == "feature":
            feature_doc = raw if isinstance(raw, dict) else {}
        else:
            state_doc = raw if isinstance(raw, dict) else {}

    feature_by_symbol = _raw_symbol_map(feature_doc, "items")
    state_by_symbol = _raw_symbol_map(state_doc, "symbols")
    plan_reasons = list(reason_codes or [blocked_reason])
    if blocked_reason and blocked_reason not in plan_reasons:
        plan_reasons.append(blocked_reason)

    items: list[dict[str, Any]] = []
    for target in target_items:
        symbol = _key(str(target.get("symbol") or ""))
        if not symbol:
            continue
        feature = feature_by_symbol.get(symbol)
        state = state_by_symbol.get(symbol)
        if line == "micro_fast":
            quality = feature.get("micro_fast_quality") if isinstance(feature, dict) else None
            signal = feature.get("micro_fast_signal") if isinstance(feature, dict) else None
            state_ready_key = "fast_ready"
            state_reasons_key = "fast_reason_codes"
            required_fallback = 180
        else:
            quality = feature.get("micro_full_quality") if isinstance(feature, dict) else None
            signal = feature.get("micro_full_signal") if isinstance(feature, dict) else None
            state_ready_key = "full_ready"
            state_reasons_key = "full_reason_codes"
            required_fallback = 900

        ready = _raw_bool(quality, "ready", _raw_bool(state, state_ready_key, False))
        confirmed = _raw_bool(signal, "micro_direction_confirmed", False)
        consumable = _raw_bool(signal, "micro_exec_allowed", False)
        observed = _raw_int(quality, "warmup_age_sec")
        if observed is None:
            observed = _raw_int(state, "continuous_collect_sec")
        required = _required_observed_sec_from_quality(
            quality if isinstance(quality, dict) else None,
            fallback=required_fallback,
        )

        reasons: list[str] = []
        if isinstance(state, dict) and isinstance(state.get(state_reasons_key), list):
            reasons.extend(str(x) for x in state[state_reasons_key] if str(x))
        reasons.extend(_raw_reason_codes(quality))
        reasons.extend(_raw_reason_codes(signal))
        if feature is None and state is None:
            reasons.append("micro_symbol_missing")
        reasons.extend(plan_reasons)
        reasons = list(dict.fromkeys(reasons))

        if consumable or confirmed:
            lifecycle_state = "confirmed"
            block = ""
        elif ready:
            lifecycle_state = "rejected"
            block = "micro_symbol_rejected"
        elif observed is not None and observed < required:
            lifecycle_state = "not_ready"
            block = "micro_symbol_not_ready_warmup_incomplete"
            reasons.append("micro_symbol_warmup_incomplete_terminalized")
        else:
            lifecycle_state = "not_ready"
            block = "micro_symbol_not_ready"

        items.append(
            {
                "symbol": symbol,
                "state": lifecycle_state,
                "terminal": True,
                "ready": ready,
                "confirmed": confirmed,
                "trade_plan_consumable": False,
                "consumption_block_reason": block or blocked_reason,
                "trade_plan_emitted": False,
                "executable": False,
                "observed_sec": observed,
                "required_observed_sec": required,
                "micro_signal_usable": _raw_bool(signal, "micro_signal_usable", False),
                "micro_direction_confirmed": confirmed,
                "micro_exec_allowed": consumable,
                "reason_codes": reasons,
                "plan_reason_codes": list(dict.fromkeys(plan_reasons)),
                "tier": target.get("tier"),
                "source_state": target.get("source_state"),
                "move_side": target.get("move_side"),
            },
        )

    counts: dict[str, int] = {}
    for item in items:
        state = str(item.get("state") or "unknown")
        counts[state] = counts.get(state, 0) + 1
    health = runtime_health or {}
    ready_count = sum(1 for item in items if item.get("ready"))
    confirmed_count = sum(1 for item in items if item.get("confirmed"))
    consumable_count = sum(1 for item in items if item.get("trade_plan_consumable"))
    rejected_count = int(counts.get("rejected", 0))
    not_ready_count = int(counts.get("not_ready", 0))
    timeout_count = int(counts.get("timeout", 0))
    emitted_count = sum(1 for item in items if item.get("trade_plan_emitted"))
    final_symbol_counts = {
        "target": len(items),
        "ready": ready_count,
        "confirmed": confirmed_count,
        "consumable": consumable_count,
        "emitted": emitted_count,
        "rejected": rejected_count,
        "not_ready": not_ready_count,
        "timeout": timeout_count,
        "observing": 0,
        "unfinished": 0,
        "states": counts,
    }
    technical_blocked = blocked_reason in {
        "micro_daemon_stale_during_wait",
        "technical_blocked_micro_daemon_stale",
    } or "micro_fast_technical_blocked" in plan_reasons or "technical_blocked_micro_daemon_stale" in plan_reasons
    if technical_blocked:
        lifecycle_status = "technical_blocked"
        line_exec_status = "technical_blocked"
    else:
        lifecycle_status = "terminalized_no_consumable" if len(items) > 0 else "blocked_no_targets"
        line_exec_status = health.get("line_exec_status")
    payload = {
        "schema_version": "10.35",
        "source": "symbol_level_micro_lifecycle",
        "strategy_line": line,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "target_set_id": lineage.get("micro_target_set_id") or target_doc.get("target_set_id"),
        "candidate_hash": lineage.get("micro_candidate_hash") or target_doc.get("candidate_hash"),
        "generated_at": to_iso_z(utc_now()),
        "status": "blocked",
        "blocked_reason": blocked_reason,
        "reason_codes": list(dict.fromkeys(plan_reasons)),
        "technical_blocked": technical_blocked,
        "technical_block_reason": "micro_daemon_stale_during_wait" if technical_blocked else None,
        "recovery": health.get("recovery") if isinstance(health.get("recovery"), dict) else {},
        "runtime_health": health,
        "count": len(items),
        "state_counts": counts,
        "line_exec_status": line_exec_status,
        "line_lifecycle_status": lifecycle_status,
        "line_lifecycle_complete": bool(items),
        "trade_plan_allowed": False,
        "micro_consumption_policy": "confirmed_only",
        "consumed_symbols": [],
        "excluded_symbols": [item["symbol"] for item in items],
        "unfinished_symbol_count": 0,
        "symbol_counts": final_symbol_counts,
        "items": items,
    }
    out_p = _micro_lifecycle_output_path(root, line)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(out_p, payload)
    return out_p


def _load_micro_wait_pass_evidence(
    *,
    root: Path,
    line: TradePlanLineName,
    expected_target_set_id: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any]]:
    if _without_micro_like(line):
        return None, None, {}
    evidence_path = _wait_pass_evidence_path(root, line)
    refs: dict[str, Any] = {
        "micro_wait_evidence_used": False,
        "micro_wait_evidence_path": str(evidence_path),
    }
    try:
        evidence = read_json_object(evidence_path)
    except (OSError, ValueError, TypeError):
        refs["micro_wait_evidence_missing"] = True
        return None, None, refs
    if not isinstance(evidence, dict) or str(evidence.get("strategy_line") or "") != line:
        refs["micro_wait_evidence_mismatch"] = True
        return None, None, refs
    evidence_target_set_id = str(evidence.get("target_set_id") or "")
    if expected_target_set_id and evidence_target_set_id and evidence_target_set_id != expected_target_set_id:
        refs.update(
            {
                "micro_wait_evidence_target_set_mismatch": True,
                "micro_wait_evidence_target_set_id": evidence_target_set_id,
            },
        )
        return None, None, refs
    feature_doc = evidence.get("micro_features") if isinstance(evidence.get("micro_features"), dict) else None
    state_doc = evidence.get("micro_state") if isinstance(evidence.get("micro_state"), dict) else None
    refs.update(
        {
            "micro_wait_evidence_used": feature_doc is not None,
            "micro_wait_predicate": evidence.get("wait_predicate"),
            "micro_wait_pass_micro_generated_at": evidence.get("micro_generated_at"),
            "micro_wait_pass_micro_state_generated_at": evidence.get("micro_state_generated_at"),
            "micro_wait_pass_ready_symbols": evidence.get("ready_symbols") if isinstance(evidence.get("ready_symbols"), list) else [],
            "micro_wait_pass_fast_ready_symbols": evidence.get("fast_ready_symbols")
            if isinstance(evidence.get("fast_ready_symbols"), list)
            else [],
            "micro_wait_pass_full_ready_symbols": evidence.get("full_ready_symbols")
            if isinstance(evidence.get("full_ready_symbols"), list)
            else [],
        },
    )
    return feature_doc, state_doc, refs


def run_apply_trade_plan_line_safe(
    *,
    line: TradePlanLineName,
    project_root: Path | None = None,
    factor_path: Path | None = None,
    refresh_path: Path | None = None,
    liquidity_path: Path | None = None,
    micro_path: Path | None = None,
    micro_state_path: Path | None = None,
    output_path: Path | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    stdout_json: bool = False,
) -> int:
    root = (project_root or Path.cwd()).resolve()
    factor_p = factor_path or _default_factor_path(root, line)
    refresh_p = refresh_path or _default_refresh_path(root, line)
    liquidity_p = liquidity_path or _default_liquidity_path(root, line)
    micro_p = micro_path or _default_micro_path(root)
    micro_state_p = micro_state_path or _default_micro_state_path(root)
    out_p = output_path or default_output_path(root, line)
    try:
        factor_doc = FactorSnapshotDocument.model_validate(read_json_object(factor_p))
        if line in ("micro_fast", "micro_full") and factor_doc.status == "blocked":
            refs = dict(factor_doc.input_refs or {})
            blocked_reason = str(refs.get("blocked_reason") or "upstream_step2_stale")
            reason_codes = list(refs.get("reason_codes") or [blocked_reason])
            if "upstream_step2_stale" not in reason_codes:
                reason_codes.append("upstream_step2_stale")
            return write_blocked_trade_plan_line(
                line=line,
                project_root=root,
                output_path=out_p,
                run_id=run_id,
                cycle_id=cycle_id,
                blocked_reason="upstream_step2_stale",
                reason_codes=reason_codes,
                runtime_health={
                    "factor_status": factor_doc.status,
                    "factor_generated_at": factor_doc.generated_at,
                    "factor_blocked_reason": blocked_reason,
                    "candidate_alignment": factor_doc.candidate_alignment,
                },
                stdout_json=stdout_json,
            )
        refresh_doc = DecisionRefreshDocument.model_validate(read_json_object(refresh_p))
        liquidity_doc = (
            MarketEntryLiquidityDocument.model_validate(read_json_object(liquidity_p))
            if liquidity_p.is_file()
            else None
        )
        micro_doc = None
        micro_state_doc = None
        micro_wait_evidence_refs: dict[str, Any] = {}
        micro_target_lineage = _read_micro_target_lineage(root) if not _without_micro_like(line) else {}
        if not _without_micro_like(line):
            evidence_feature_raw: dict[str, Any] | None = None
            evidence_state_raw: dict[str, Any] | None = None
            if micro_path is None and micro_state_path is None:
                evidence_feature_raw, evidence_state_raw, micro_wait_evidence_refs = _load_micro_wait_pass_evidence(
                    root=root,
                    line=line,
                    expected_target_set_id=str(micro_target_lineage.get("micro_target_set_id") or ""),
                )
            micro_doc = LatestMicroFeaturesDocument.model_validate(evidence_feature_raw or read_json_object(micro_p))
            if evidence_state_raw is not None:
                micro_state_doc = MicroDaemonStateDocument.model_validate(evidence_state_raw)
            elif micro_state_p.is_file():
                micro_state_doc = MicroDaemonStateDocument.model_validate(read_json_object(micro_state_p))
        doc = build_trade_plan_line_document(
            line=line,
            factor_doc=factor_doc,
            refresh_doc=refresh_doc,
            liquidity_doc=liquidity_doc,
            micro_doc=micro_doc,
            micro_state_doc=micro_state_doc,
            generated_at=to_iso_z(utc_now()),
            run_id=run_id,
            cycle_id=cycle_id,
            micro_target_lineage=micro_target_lineage,
            micro_wait_evidence_refs=micro_wait_evidence_refs,
            cfg=load_trade_plan_line_config(root, line),
            project_root=root,
        )
        out_p.parent.mkdir(parents=True, exist_ok=True)
        payload = archive_trade_plan_line_payload(
            root=root,
            line=line,
            payload=doc.model_dump(mode="json"),
            latest_path=out_p,
        )
        write_json_atomic(out_p, payload)
        if line == "without_micro":
            try:
                from laoma_signal_engine.strategy4.observe import sync_observe_pool_from_without_micro

                sync_observe_pool_from_without_micro(project_root=root, trade_plan_doc=payload)
            except Exception as e:  # Strategy4 admission must not block Strategy1 output.
                print(f"[WARN] strategy4 observe pool sync failed: {e}", file=sys.stderr)
        _write_micro_lifecycle_document(root=root, line=line, doc=doc)
        if stdout_json:
            print(
                json.dumps(
                    {
                        "step": {
                            "without_micro": "STEP10.2",
                            "micro_fast": "STEP10.3",
                            "micro_full": "STEP10.4",
                            "strategy4": "STEP17.5",
                            "strategy5": "STEP20.5",
                            "strategy6": "STEP22.8",
                        }[line],
                        "line": line,
                        "status": doc.status,
                        "count": doc.count,
                        "executable_count": doc.executable_count,
                        "output": str(out_p),
                    },
                    ensure_ascii=False,
                ),
            )
        return EXIT_SUCCESS
    except (OSError, ValueError, ValidationError) as e:
        print(f"[ERROR] trade plan line failed line={line}: {e}", file=sys.stderr)
        return EXIT_CONFIG if isinstance(e, ValidationError) else EXIT_INTERNAL


def write_blocked_trade_plan_line(
    *,
    line: TradePlanLineName,
    project_root: Path | None = None,
    output_path: Path | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    blocked_reason: str = "micro_daemon_unhealthy",
    reason_codes: list[str] | None = None,
    runtime_health: dict[str, Any] | None = None,
    stdout_json: bool = False,
) -> int:
    root = (project_root or Path.cwd()).resolve()
    out_p = output_path or default_output_path(root, line)
    target_lineage: dict[str, Any] = {}
    lifecycle_refs: dict[str, Any] = {}
    wait_refs: dict[str, Any] = {}
    blocked_evidence_refs: dict[str, Any] = {}
    if not _without_micro_like(line):
        target_lineage = _read_micro_target_lineage(root)
        expected_target_set_id = str(target_lineage.get("micro_target_set_id") or "")
        _, _, wait_refs = _load_micro_wait_pass_evidence(
            root=root,
            line=line,
            expected_target_set_id=expected_target_set_id or None,
        )
        lifecycle_path = _micro_lifecycle_output_path(root, line)
        lifecycle_refs["micro_lifecycle_path"] = str(lifecycle_path)
        try:
            lifecycle_doc = read_json_object(lifecycle_path)
        except (OSError, ValueError, TypeError):
            lifecycle_doc = {}
        if isinstance(lifecycle_doc, dict):
            lifecycle_refs.update(
                {
                    "line_exec_status": lifecycle_doc.get("line_exec_status"),
                    "line_lifecycle_status": lifecycle_doc.get("line_lifecycle_status"),
                    "line_lifecycle_complete": lifecycle_doc.get("line_lifecycle_complete"),
                    "trade_plan_allowed": lifecycle_doc.get("trade_plan_allowed"),
                    "unfinished_symbol_count": lifecycle_doc.get("unfinished_symbol_count"),
                    "symbol_counts": lifecycle_doc.get("symbol_counts") if isinstance(lifecycle_doc.get("symbol_counts"), dict) else {},
                    "micro_lifecycle_generated_at": lifecycle_doc.get("generated_at"),
                    "micro_lifecycle_status": lifecycle_doc.get("status"),
                    "micro_lifecycle_blocked_reason": lifecycle_doc.get("blocked_reason"),
                },
            )
        blocked_evidence_path = _blocked_micro_evidence_path(root, line)
        blocked_evidence = {
            "schema_version": "10.51",
            "source": "blocked_micro_trade_plan_evidence",
            "strategy_line": line,
            "run_id": run_id,
            "cycle_id": cycle_id,
            "generated_at": to_iso_z(utc_now()),
            "blocked_reason": blocked_reason,
            "reason_codes": reason_codes or [blocked_reason],
            "target_lineage": target_lineage,
            "wait_evidence": wait_refs,
            "lifecycle": lifecycle_refs,
            "runtime_health": runtime_health or {},
        }
        blocked_evidence_path.parent.mkdir(parents=True, exist_ok=True)
        write_json_atomic(blocked_evidence_path, blocked_evidence)
        blocked_evidence_refs["blocked_micro_evidence_path"] = str(blocked_evidence_path)
        if run_id:
            run_blocked_path = blocked_evidence_path.with_name(f"{blocked_evidence_path.stem}_{run_id}.json")
            write_json_atomic(run_blocked_path, blocked_evidence)
            blocked_evidence_refs["blocked_micro_run_evidence_path"] = str(run_blocked_path)
    input_refs = {
        "blocked_reason": blocked_reason,
        "reason_codes": reason_codes or [blocked_reason],
        "runtime_health": runtime_health or {},
    }
    if not _without_micro_like(line):
        input_refs.update(target_lineage)
        input_refs.update(wait_refs)
        input_refs.update(lifecycle_refs)
        input_refs.update(blocked_evidence_refs)
    doc = TradePlanLineDocument(
        generated_at=to_iso_z(utc_now()),
        run_id=run_id,
        cycle_id=cycle_id,
        source=LINE_SOURCE[line],
        micro_mode={
            "without_micro": "none",
            "micro_fast": "fast",
            "micro_full": "full",
            "strategy4": "none",
            "strategy5": "strategy5_evidence",
            "strategy6": "strategy6_market_accepted",
        }[line],  # type: ignore[arg-type]
        status="blocked",
        count=0,
        executable_count=0,
        input_refs=input_refs,
        plans=[],
    )
    out_p.parent.mkdir(parents=True, exist_ok=True)
    payload = archive_trade_plan_line_payload(
        root=root,
        line=line,
        payload=doc.model_dump(mode="json"),
        latest_path=out_p,
    )
    write_json_atomic(out_p, payload)
    if stdout_json:
        print(
            json.dumps(
                {
                    "step": "STEP10.12",
                    "line": line,
                    "status": doc.status,
                    "blocked_reason": blocked_reason,
                    "output": str(out_p),
                },
                ensure_ascii=False,
            ),
        )
    return EXIT_SUCCESS


def write_failed_trade_plan_line(
    *,
    line: TradePlanLineName,
    project_root: Path | None = None,
    output_path: Path | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    failed_stage: str = "unknown_stage",
    failed_rc: int | None = None,
    reason_codes: list[str] | None = None,
    runtime_health: dict[str, Any] | None = None,
    stdout_json: bool = False,
) -> int:
    root = (project_root or Path.cwd()).resolve()
    out_p = output_path or default_output_path(root, line)
    reasons = reason_codes or [f"{failed_stage}_failed"]
    input_refs = {
        "failed_stage": failed_stage,
        "failed_rc": failed_rc,
        "blocked_reason": f"{failed_stage}_failed",
        "reason_codes": reasons,
        "runtime_health": runtime_health or {},
        "line_exec_status": "failed_before_trade_plan",
        "line_lifecycle_status": f"{failed_stage}_failed",
        "trade_plan_allowed": False,
    }
    if not _without_micro_like(line):
        input_refs.update(_read_micro_target_lineage(root))
    doc = TradePlanLineDocument(
        generated_at=to_iso_z(utc_now()),
        run_id=run_id,
        cycle_id=cycle_id,
        source=LINE_SOURCE[line],
        micro_mode={"without_micro": "none", "micro_fast": "fast", "micro_full": "full", "strategy4": "none"}[line],  # type: ignore[arg-type]
        status="error",
        count=0,
        executable_count=0,
        input_refs=input_refs,
        plans=[],
    )
    out_p.parent.mkdir(parents=True, exist_ok=True)
    payload = archive_trade_plan_line_payload(
        root=root,
        line=line,
        payload=doc.model_dump(mode="json"),
        latest_path=out_p,
    )
    write_json_atomic(out_p, payload)
    if stdout_json:
        print(
            json.dumps(
                {
                    "step": "STEP10.47",
                    "line": line,
                    "status": doc.status,
                    "failed_stage": failed_stage,
                    "failed_rc": failed_rc,
                    "output": str(out_p),
                },
                ensure_ascii=False,
            ),
        )
    return EXIT_SUCCESS
