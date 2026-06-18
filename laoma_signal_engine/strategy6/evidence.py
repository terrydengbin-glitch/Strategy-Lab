"""STEP22 Strategy6 market-accepted entry evaluator.

Strategy6 is an independent normal pipeline line. It reuses the current
without_micro trade plan as a base, then applies direction-acceptance and entry
price-quality gates. It does not consume live micro slots in v1.
"""

from __future__ import annotations

import json
import os
import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import parse_iso_z, to_iso_z, utc_now
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanLineDocument


SCHEMA_VERSION = "22.1"
SOURCE = "strategy6_market_accepted_entry"


@dataclass(frozen=True)
class Strategy6Paths:
    db: Path
    latest_trade_plan: Path
    latest_evidence: Path
    latest_decisions: Path
    latest_wait_pool: Path
    latest_observe_attempts: Path
    daemon_state: Path
    daemon_heartbeat: Path
    daemon_lock: Path
    daemon_stop: Path


def paths(project_root: Path) -> Strategy6Paths:
    root = Path(project_root)
    return Strategy6Paths(
        db=root / "DATA" / "strategy6" / "strategy6.db",
        latest_trade_plan=root / "DATA" / "decisions" / "latest_trade_plan_strategy6.json",
        latest_evidence=root / "DATA" / "strategy6" / "latest_evidence.json",
        latest_decisions=root / "DATA" / "strategy6" / "latest_decisions.json",
        latest_wait_pool=root / "DATA" / "strategy6" / "latest_wait_pool.json",
        latest_observe_attempts=root / "DATA" / "strategy6" / "latest_observe_attempts.json",
        daemon_state=root / "DATA" / "strategy6" / "daemon_state.json",
        daemon_heartbeat=root / "DATA" / "strategy6" / "daemon_heartbeat.json",
        daemon_lock=root / "DATA" / "strategy6" / "strategy6_daemon.lock",
        daemon_stop=root / "DATA" / "strategy6" / "strategy6_daemon.stop",
    )


def _trade_plan_archive_path(project_root: Path, run_id: str | None) -> Path | None:
    if not run_id:
        return None
    return Path(project_root) / "DATA" / "decisions" / "trade_plan_runs" / str(run_id) / "latest_trade_plan_strategy6.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = read_json_object(path)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _items(doc: dict[str, Any]) -> list[dict[str, Any]]:
    rows = doc.get("items")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    rows = doc.get("plans")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        out = float(value)
        if out != out or out in (float("inf"), float("-inf")):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _side_sign(side: str) -> int:
    return -1 if str(side).upper() == "SHORT" else 1


def _side_from_move(move_side: Any, price_ret: float) -> str:
    side = str(move_side or "").lower()
    if side in {"up", "long", "bullish"}:
        return "LONG"
    if side in {"down", "short", "bearish"}:
        return "SHORT"
    if price_ret > 0:
        return "LONG"
    if price_ret < 0:
        return "SHORT"
    return "NO_TRADE"


def _factor_doc(project_root: Path) -> dict[str, Any]:
    root = Path(project_root)
    for rel in (
        "DATA/factors/latest_factor_snapshot_withoutoficvd.json",
        "DATA/factors/latest_factor_snapshot.json",
    ):
        doc = _read_json(root / rel)
        if doc:
            return doc
    return {}


def _base_trade_plan_doc(project_root: Path) -> dict[str, Any]:
    return _read_json(Path(project_root) / "DATA" / "decisions" / "latest_trade_plan_without_micro.json")


def load_strategy6_config(project_root: Path) -> dict[str, Any]:
    path = Path(project_root) / "laoma_signal_engine" / "config" / "default.yaml"
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except OSError:
        raw = {}
    cfg = raw.get("strategy6") if isinstance(raw.get("strategy6"), dict) else {}
    return {
        "min_direction_acceptance_score": int(cfg.get("min_direction_acceptance_score", 58)),
        "min_entry_price_quality_score": int(cfg.get("min_entry_price_quality_score", 56)),
        "min_market_acceptance_score": int(cfg.get("min_market_acceptance_score", 58)),
        "hard_deny_direction_score": int(cfg.get("hard_deny_direction_score", 38)),
        "long_max_range_pos": float(cfg.get("long_max_range_pos", 0.78)),
        "short_min_range_pos": float(cfg.get("short_min_range_pos", 0.22)),
        "max_spread_bps": float(cfg.get("max_spread_bps", 45.0)),
        "max_abs_1m_chase_bps": float(cfg.get("max_abs_1m_chase_bps", 80.0)),
        "market_score_direction_weight": float(cfg.get("market_score_direction_weight", 0.58)),
        "market_score_entry_weight": float(cfg.get("market_score_entry_weight", 0.42)),
        "market_acceptance_mode": str(cfg.get("market_acceptance_mode", "shadow_paper")),
        "daemon_enabled": bool(cfg.get("daemon_enabled", False)),
        "observe_interval_sec": int(cfg.get("observe_interval_sec", 300)),
        "daemon_stale_after_sec": int(cfg.get("daemon_stale_after_sec", max(30, int(cfg.get("observe_interval_sec", 300)) * 2))),
        "daemon_watchdog_enabled": bool(cfg.get("daemon_watchdog_enabled", True)),
        "max_observe_age_sec": int(cfg.get("max_observe_age_sec", 14400)),
        "max_attempts": int(cfg.get("max_attempts", 48)),
        "max_pool_size": int(cfg.get("max_pool_size", 300)),
        "technical_blocked_ttl_sec": int(cfg.get("technical_blocked_ttl_sec", 1800)),
        "strategy6_version": str(cfg.get("strategy6_version", "v1")),
        "v2_min_direction_acceptance_score": int(cfg.get("v2_min_direction_acceptance_score", cfg.get("min_direction_acceptance_score", 62))),
        "v2_uncertain_direction_score": int(cfg.get("v2_uncertain_direction_score", 48)),
        "v2_hard_deny_direction_score": int(cfg.get("v2_hard_deny_direction_score", cfg.get("hard_deny_direction_score", 38))),
        "v2_max_chase_bps": float(cfg.get("v2_max_chase_bps", cfg.get("max_abs_1m_chase_bps", 55.0))),
        "v2_adverse_1m_deny_bps": float(cfg.get("v2_adverse_1m_deny_bps", 24.0)),
        "v2_reversal_1m_wait_bps": float(cfg.get("v2_reversal_1m_wait_bps", 10.0)),
        "v2_distance_from_mean_max_bps": float(cfg.get("v2_distance_from_mean_max_bps", 85.0)),
        "v2_high_quality_score": int(cfg.get("v2_high_quality_score", 74)),
        "v2_medium_quality_score": int(cfg.get("v2_medium_quality_score", 62)),
        "v3_min_direction_context_score": int(cfg.get("v3_min_direction_context_score", cfg.get("v2_min_direction_acceptance_score", 62))),
        "v3_uncertain_direction_context_score": int(cfg.get("v3_uncertain_direction_context_score", 52)),
        "v3_hard_deny_context_score": int(cfg.get("v3_hard_deny_context_score", cfg.get("v2_hard_deny_direction_score", 38))),
        "v3_reverse_1m_deny_bps": float(cfg.get("v3_reverse_1m_deny_bps", 12.0)),
        "v3_reverse_3m_deny_bps": float(cfg.get("v3_reverse_3m_deny_bps", 24.0)),
        "v3_fake_breakout_range_pos": float(cfg.get("v3_fake_breakout_range_pos", 0.88)),
        "v3_second_acceptance_min_bps": float(cfg.get("v3_second_acceptance_min_bps", 4.0)),
        "v3_max_entry_slippage_bps": float(cfg.get("v3_max_entry_slippage_bps", 45.0)),
        "v3_quality_filter_mode": str(cfg.get("v3_quality_filter_mode", "shadow")),
        "v3_bad_symbols": list(cfg.get("v3_bad_symbols", [])) if isinstance(cfg.get("v3_bad_symbols", []), list) else [],
        "v3_bad_sides": list(cfg.get("v3_bad_sides", [])) if isinstance(cfg.get("v3_bad_sides", []), list) else [],
        "v3_1_min_direction_context_score": int(cfg.get("v3_1_min_direction_context_score", cfg.get("v3_min_direction_context_score", cfg.get("v2_min_direction_acceptance_score", 62)))),
        "v3_1_uncertain_direction_context_score": int(cfg.get("v3_1_uncertain_direction_context_score", cfg.get("v3_uncertain_direction_context_score", 52))),
        "v3_1_hard_deny_context_score": int(cfg.get("v3_1_hard_deny_context_score", cfg.get("v3_hard_deny_context_score", cfg.get("v2_hard_deny_direction_score", 38)))),
        "v3_1_reverse_1m_deny_bps": float(cfg.get("v3_1_reverse_1m_deny_bps", cfg.get("v3_reverse_1m_deny_bps", 12.0))),
        "v3_1_reverse_3m_deny_bps": float(cfg.get("v3_1_reverse_3m_deny_bps", cfg.get("v3_reverse_3m_deny_bps", 24.0))),
        "v3_1_low_followthrough_min_volume_z": float(cfg.get("v3_1_low_followthrough_min_volume_z", 0.85)),
        "v3_1_low_followthrough_min_5m_bps": float(cfg.get("v3_1_low_followthrough_min_5m_bps", 8.0)),
        "v3_1_range_extreme_pos": float(cfg.get("v3_1_range_extreme_pos", 0.72)),
        "v3_1_btc_against_action": str(cfg.get("v3_1_btc_against_action", "wait")),
        "v3_2_long_min_direction_context_score": int(cfg.get("v3_2_long_min_direction_context_score", cfg.get("v3_min_direction_context_score", 64))),
        "v3_2_short_min_direction_context_score": int(cfg.get("v3_2_short_min_direction_context_score", cfg.get("v3_min_direction_context_score", 58))),
        "v3_2_long_reverse_1m_deny_bps": float(cfg.get("v3_2_long_reverse_1m_deny_bps", cfg.get("v3_reverse_1m_deny_bps", 10.0))),
        "v3_2_short_reverse_1m_deny_bps": float(cfg.get("v3_2_short_reverse_1m_deny_bps", cfg.get("v3_reverse_1m_deny_bps", 16.0))),
        "v3_2_long_reverse_3m_deny_bps": float(cfg.get("v3_2_long_reverse_3m_deny_bps", cfg.get("v3_reverse_3m_deny_bps", 22.0))),
        "v3_2_short_reverse_3m_deny_bps": float(cfg.get("v3_2_short_reverse_3m_deny_bps", cfg.get("v3_reverse_3m_deny_bps", 30.0))),
        "v3_2_long_btc_against_action": str(cfg.get("v3_2_long_btc_against_action", "wait")),
        "v3_2_short_btc_against_action": str(cfg.get("v3_2_short_btc_against_action", "shadow")),
        "v3_2_quality_filter_mode": str(cfg.get("v3_2_quality_filter_mode", "shadow")),
        "v3_2_bad_symbols": list(cfg.get("v3_2_bad_symbols", [])) if isinstance(cfg.get("v3_2_bad_symbols", []), list) else [],
        "v3_2_bad_sides": list(cfg.get("v3_2_bad_sides", [])) if isinstance(cfg.get("v3_2_bad_sides", []), list) else [],
        "v3_2_bad_hours": list(cfg.get("v3_2_bad_hours", [])) if isinstance(cfg.get("v3_2_bad_hours", []), list) else [],
        "v3_3_long_min_direction_context_score": int(cfg.get("v3_3_long_min_direction_context_score", cfg.get("v3_2_long_min_direction_context_score", cfg.get("v3_min_direction_context_score", 66)))),
        "v3_3_short_min_direction_context_score": int(cfg.get("v3_3_short_min_direction_context_score", cfg.get("v3_2_short_min_direction_context_score", cfg.get("v3_min_direction_context_score", 58)))),
        "v3_3_adverse_1m_wait_bps": float(cfg.get("v3_3_adverse_1m_wait_bps", 6.0)),
        "v3_3_adverse_3m_deny_bps": float(cfg.get("v3_3_adverse_3m_deny_bps", 18.0)),
        "v3_3_weak_followthrough_wait_bps": float(cfg.get("v3_3_weak_followthrough_wait_bps", 4.0)),
        "v3_3_min_volume_z": float(cfg.get("v3_3_min_volume_z", 0.6)),
        "v3_3_early_abort_enabled": bool(cfg.get("v3_3_early_abort_enabled", True)),
        "v3_3_abort_if_mfe_lt_R": float(cfg.get("v3_3_abort_if_mfe_lt_R", 0.10)),
        "v3_3_abort_if_mae_gt_R": float(cfg.get("v3_3_abort_if_mae_gt_R", 0.45)),
        "v3_3_abort_window_min": int(cfg.get("v3_3_abort_window_min", 3)),
        "v3_3_max_initial_adverse_R": float(cfg.get("v3_3_max_initial_adverse_R", 0.75)),
        "v3_4_min_aligned_1m_bps": float(cfg.get("v3_4_min_aligned_1m_bps", -2.0)),
        "v3_4_min_aligned_3m_bps": float(cfg.get("v3_4_min_aligned_3m_bps", -4.0)),
        "v3_4_min_followthrough_5m_bps": float(cfg.get("v3_4_min_followthrough_5m_bps", 6.0)),
        "v3_4_min_volume_z": float(cfg.get("v3_4_min_volume_z", 0.8)),
        "v3_4_long_max_range_pos": float(cfg.get("v3_4_long_max_range_pos", 0.86)),
        "v3_4_short_min_range_pos": float(cfg.get("v3_4_short_min_range_pos", 0.14)),
        "v3_4_max_distance_to_mean_bps": float(cfg.get("v3_4_max_distance_to_mean_bps", 72.0)),
        "v3_4_no_edge_action": str(cfg.get("v3_4_no_edge_action", "wait")),
        "v3_4_range_noise_action": str(cfg.get("v3_4_range_noise_action", "wait_rebound")),
        "v3_4_btc_against_action": str(cfg.get("v3_4_btc_against_action", "wait")),
        "v3_5_no_edge_aligned_5m_bps": float(cfg.get("v3_5_no_edge_aligned_5m_bps", 4.0)),
        "v3_5_no_edge_volume_z": float(cfg.get("v3_5_no_edge_volume_z", 0.7)),
        "v3_5_hard_wrong_adverse_1m_bps": float(cfg.get("v3_5_hard_wrong_adverse_1m_bps", 8.0)),
        "v3_5_hard_wrong_adverse_3m_bps": float(cfg.get("v3_5_hard_wrong_adverse_3m_bps", 18.0)),
        "v3_5_rebound_range_long": float(cfg.get("v3_5_rebound_range_long", 0.82)),
        "v3_5_rebound_range_short": float(cfg.get("v3_5_rebound_range_short", 0.18)),
        "v3_5_profit_lock_min_aligned_5m_bps": float(cfg.get("v3_5_profit_lock_min_aligned_5m_bps", 8.0)),
        "v3_6_hard_wrong_1m_bps": float(cfg.get("v3_6_hard_wrong_1m_bps", 10.0)),
        "v3_6_hard_wrong_3m_bps": float(cfg.get("v3_6_hard_wrong_3m_bps", 22.0)),
        "v3_6_min_followthrough_5m_bps": float(cfg.get("v3_6_min_followthrough_5m_bps", 6.0)),
        "v3_6_min_volume_z": float(cfg.get("v3_6_min_volume_z", 0.9)),
        "v3_6_no_edge_action": str(cfg.get("v3_6_no_edge_action", "wait")),
        "v3_6_base_v3_4_followthrough_bps": float(cfg.get("v3_6_base_v3_4_followthrough_bps", -999.0)),
        "v3_6_base_v3_4_min_volume_z": float(cfg.get("v3_6_base_v3_4_min_volume_z", 0.0)),
    }


def build_feature_vector(base: dict[str, Any], factor: dict[str, Any]) -> dict[str, Any]:
    primary = factor.get("primary_15m") if isinstance(factor.get("primary_15m"), dict) else {}
    trigger = factor.get("trigger_5m") if isinstance(factor.get("trigger_5m"), dict) else {}
    entry = factor.get("entry_1m") if isinstance(factor.get("entry_1m"), dict) else {}
    micro = factor.get("micro_15m") if isinstance(factor.get("micro_15m"), dict) else {}
    liquidity = base.get("guards") if isinstance(base.get("guards"), dict) else {}
    decision = str(base.get("decision") or _side_from_move(factor.get("move_side"), _num(primary.get("price_ret")))).upper()
    return {
        "schema_version": SCHEMA_VERSION,
        "symbol": str(base.get("symbol") or factor.get("symbol") or "").upper(),
        "legacy_side": decision if decision in {"LONG", "SHORT"} else "NO_TRADE",
        "factor_side": _side_from_move(factor.get("move_side"), _num(primary.get("price_ret"))),
        "pct_1m_bps": _num(entry.get("price_ret")) * 100.0,
        "pct_3m_bps": _num(entry.get("price_ret_3m") or entry.get("pct_3m_bps")),
        "pct_5m_bps": _num(trigger.get("price_ret")) * 100.0,
        "pct_15m_bps": _num(primary.get("price_ret")) * 100.0,
        "volume_z": _num(primary.get("volume_ratio"), 1.0),
        "taker_buy_ratio": _num(primary.get("taker_buy_ratio"), 0.5),
        "range_pos": _num(primary.get("range_pos"), 0.5),
        "spread_bps": _num(micro.get("spread_bps") or liquidity.get("spread_bps")),
        "distance_to_vwap_bps": _num(entry.get("distance_to_vwap_bps") or primary.get("distance_to_vwap_bps")),
        "distance_to_ema_bps": _num(entry.get("distance_to_ema_bps") or primary.get("distance_to_ema_bps")),
        "impulse_age_min": _num(entry.get("impulse_age_min") or trigger.get("impulse_age_min")),
        "cvd_direction": str(micro.get("cvd_state") or primary.get("kline_cvd_state") or "missing"),
        "ofi_direction": str(micro.get("ofi_state") or micro.get("ofi_pressure") or "missing"),
        "btc_alignment": str(factor.get("btc_alignment") or factor.get("market_alignment") or "unknown"),
        "local_breakout_state": str(trigger.get("breakout_state") or trigger.get("trigger_state") or "unknown"),
        "feature_quality": {
            "primary_15m": bool(primary),
            "trigger_5m": bool(trigger),
            "entry_1m": bool(entry),
            "micro_optional": bool(micro),
            "micro_optional_missing": not bool(micro),
            "source": "factor_without_micro_plus_trade_plan",
        },
    }


def score_market_acceptance(features: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    side = str(features.get("legacy_side") or "NO_TRADE").upper()
    if side not in {"LONG", "SHORT"}:
        return {
            "direction_acceptance_score": 0,
            "entry_price_quality_score": 0,
            "market_acceptance_score": 0,
            "decision_state": "TECHNICAL_BLOCKED",
            "wait_state": "NONE",
            "reason_codes": ["strategy6_no_legacy_side"],
        }
    sign = _side_sign(side)
    direction = 50.0
    direction += max(-18.0, min(18.0, sign * _num(features.get("pct_5m_bps")) * 0.18))
    direction += max(-12.0, min(12.0, sign * _num(features.get("pct_1m_bps")) * 0.14))
    direction += max(0.0, min(14.0, (_num(features.get("volume_z"), 1.0) - 1.0) * 12.0))
    taker_bias = _num(features.get("taker_buy_ratio"), 0.5) - 0.5
    direction += max(-10.0, min(10.0, sign * taker_bias * 55.0))
    cvd = str(features.get("cvd_direction") or "").lower()
    ofi = str(features.get("ofi_direction") or "").lower()
    if "missing" not in cvd and cvd != "unknown":
        direction += 6.0 if (side == "LONG" and any(x in cvd for x in ("buy", "bull", "positive"))) or (side == "SHORT" and any(x in cvd for x in ("sell", "bear", "negative"))) else -6.0
    if "missing" not in ofi and ofi != "unknown":
        direction += 6.0 if (side == "LONG" and any(x in ofi for x in ("buy", "bid", "bull", "positive"))) or (side == "SHORT" and any(x in ofi for x in ("sell", "ask", "bear", "negative"))) else -6.0

    entry = 72.0
    range_pos = _num(features.get("range_pos"), 0.5)
    if side == "LONG" and range_pos > float(cfg["long_max_range_pos"]):
        entry -= min(30.0, (range_pos - float(cfg["long_max_range_pos"])) * 140.0)
    if side == "SHORT" and range_pos < float(cfg["short_min_range_pos"]):
        entry -= min(30.0, (float(cfg["short_min_range_pos"]) - range_pos) * 140.0)
    entry -= min(18.0, abs(_num(features.get("distance_to_vwap_bps"))) * 0.16)
    entry -= min(12.0, abs(_num(features.get("pct_1m_bps"))) / max(float(cfg["max_abs_1m_chase_bps"]), 1.0) * 12.0)
    spread = _num(features.get("spread_bps"))
    if spread > float(cfg["max_spread_bps"]):
        entry -= min(18.0, (spread - float(cfg["max_spread_bps"])) * 0.45)
    direction_i = max(0, min(100, int(round(direction))))
    entry_i = max(0, min(100, int(round(entry))))
    direction_weight = _num(cfg.get("market_score_direction_weight"), 0.58)
    entry_weight = _num(cfg.get("market_score_entry_weight"), 0.42)
    total_weight = direction_weight + entry_weight
    if total_weight <= 0:
        direction_weight, entry_weight, total_weight = 0.58, 0.42, 1.0
    market_i = int(round((direction_i * direction_weight + entry_i * entry_weight) / total_weight))
    reasons: list[str] = []
    wait_state = "NONE"
    state = "EXECUTABLE"
    if direction_i < int(cfg["hard_deny_direction_score"]):
        state = "DENY_DIRECTION_CONFLICT"
        wait_state = "NONE"
        reasons.append("strategy6_direction_hard_deny")
    elif direction_i < int(cfg["min_direction_acceptance_score"]):
        state = "WAIT_MARKET_ACCEPTANCE"
        wait_state = "WAIT_CONFIRM"
        reasons.append("strategy6_direction_score_low")
    if state == "EXECUTABLE" and entry_i < int(cfg["min_entry_price_quality_score"]):
        state = "WAIT_REBOUND" if (
            (side == "LONG" and range_pos > float(cfg["long_max_range_pos"]))
            or (side == "SHORT" and range_pos < float(cfg["short_min_range_pos"]))
        ) else "WAIT_CONFIRM"
        wait_state = state
        reasons.append("strategy6_entry_price_quality_low")
    if state == "EXECUTABLE" and market_i < int(cfg["min_market_acceptance_score"]):
        state = "WAIT_MARKET_ACCEPTANCE"
        wait_state = "WAIT_CONFIRM"
        reasons.append("strategy6_market_acceptance_score_low")
    if spread > float(cfg["max_spread_bps"]):
        reasons.append("strategy6_spread_too_wide")
    if (side == "LONG" and range_pos > float(cfg["long_max_range_pos"])) or (
        side == "SHORT" and range_pos < float(cfg["short_min_range_pos"])
    ):
        reasons.append("strategy6_range_position_extreme")
    if features.get("feature_quality", {}).get("micro_optional_missing"):
        reasons.append("strategy6_micro_optional_missing")
    return {
        "direction_acceptance_score": direction_i,
        "entry_price_quality_score": entry_i,
        "market_acceptance_score": market_i,
        "strategy6_version": "v1",
        "direction_state": "accepted_direction" if state == "EXECUTABLE" else ("denied_direction" if state == "DENY_DIRECTION_CONFLICT" else "uncertain_direction"),
        "entry_quality_state": "entry_price_ok" if state == "EXECUTABLE" else ("entry_price_needs_rebound" if state == "WAIT_REBOUND" else "entry_price_needs_confirmation"),
        "adaptive_exit_tier": "structure",
        "decision_state": state,
        "wait_state": wait_state,
        "reason_codes": reasons or ["strategy6_market_accepted_entry_ok"],
    }


def score_market_acceptance_v2(features: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Strategy6 V2: split direction, entry timing, and exit tier evidence.

    V2 is intentionally opt-in. It keeps the same executable/WAIT/DENY external
    states as v1 so downstream paper/TQ contracts stay stable.
    """

    base = score_market_acceptance(features, cfg)
    side = str(features.get("legacy_side") or "NO_TRADE").upper()
    if side not in {"LONG", "SHORT"}:
        return {**base, "strategy6_version": "v2", "direction_state": "technical_blocked", "entry_quality_state": "entry_unavailable", "adaptive_exit_tier": "reject"}

    sign = _side_sign(side)
    pct_1m = _num(features.get("pct_1m_bps"))
    pct_3m = _num(features.get("pct_3m_bps"))
    pct_5m = _num(features.get("pct_5m_bps"))
    range_pos = _num(features.get("range_pos"), 0.5)
    spread = _num(features.get("spread_bps"))
    distance = max(abs(_num(features.get("distance_to_vwap_bps"))), abs(_num(features.get("distance_to_ema_bps"))))
    impulse_age = _num(features.get("impulse_age_min"))

    aligned_1m = sign * pct_1m
    aligned_3m = sign * pct_3m
    aligned_5m = sign * pct_5m
    continuation = max(-16.0, min(18.0, aligned_3m * 0.12 + aligned_5m * 0.08))
    reversal_penalty = 0.0
    if aligned_1m <= -float(cfg.get("v2_adverse_1m_deny_bps", 24.0)):
        reversal_penalty += 28.0
    elif aligned_1m <= -float(cfg.get("v2_reversal_1m_wait_bps", 10.0)):
        reversal_penalty += 14.0
    if aligned_5m > 0 and aligned_1m < 0:
        reversal_penalty += 8.0
    if impulse_age and impulse_age > 5:
        reversal_penalty += min(10.0, (impulse_age - 5) * 1.5)

    direction_score = max(0, min(100, int(round(_num(base.get("direction_acceptance_score")) + continuation - reversal_penalty))))
    entry_score = _num(base.get("entry_price_quality_score"))
    chase_bps = max(0.0, aligned_1m)
    if chase_bps > float(cfg.get("v2_max_chase_bps", 55.0)):
        entry_score -= min(24.0, (chase_bps - float(cfg.get("v2_max_chase_bps", 55.0))) * 0.45)
    if distance > float(cfg.get("v2_distance_from_mean_max_bps", 85.0)):
        entry_score -= min(18.0, (distance - float(cfg.get("v2_distance_from_mean_max_bps", 85.0))) * 0.18)
    entry_score = max(0, min(100, int(round(entry_score))))

    reasons = list(base.get("reason_codes") or [])
    hard_deny = int(cfg.get("v2_hard_deny_direction_score", cfg.get("hard_deny_direction_score", 38)))
    min_accept = int(cfg.get("v2_min_direction_acceptance_score", cfg.get("min_direction_acceptance_score", 62)))
    uncertain = int(cfg.get("v2_uncertain_direction_score", 48))
    if direction_score < hard_deny or aligned_1m <= -float(cfg.get("v2_adverse_1m_deny_bps", 24.0)):
        direction_state = "denied_direction"
        decision_state = "DENY_DIRECTION_CONFLICT"
        wait_state = "NONE"
        reasons.append("strategy6_v2_direction_denied")
    elif direction_score < min_accept:
        direction_state = "uncertain_direction"
        decision_state = "WAIT_MARKET_ACCEPTANCE"
        wait_state = "WAIT_CONFIRM"
        reasons.append("strategy6_v2_direction_uncertain")
    elif direction_score < uncertain:
        direction_state = "uncertain_direction"
        decision_state = "WAIT_MARKET_ACCEPTANCE"
        wait_state = "WAIT_CONFIRM"
        reasons.append("strategy6_v2_direction_needs_confirmation")
    else:
        direction_state = "accepted_direction"
        decision_state = "EXECUTABLE"
        wait_state = "NONE"

    entry_quality_state = "entry_price_ok"
    if decision_state == "EXECUTABLE":
        extreme = (side == "LONG" and range_pos > float(cfg["long_max_range_pos"])) or (
            side == "SHORT" and range_pos < float(cfg["short_min_range_pos"])
        )
        if extreme or chase_bps > float(cfg.get("v2_max_chase_bps", 55.0)) or distance > float(cfg.get("v2_distance_from_mean_max_bps", 85.0)):
            entry_quality_state = "entry_price_needs_rebound"
            decision_state = "WAIT_REBOUND"
            wait_state = "WAIT_REBOUND"
            reasons.append("strategy6_v2_entry_wait_rebound")
        elif entry_score < int(cfg.get("min_entry_price_quality_score", 56)):
            entry_quality_state = "entry_price_needs_confirmation"
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
            reasons.append("strategy6_v2_entry_wait_confirm")

    market_score = int(round((direction_score * _num(cfg.get("market_score_direction_weight"), 0.58) + entry_score * _num(cfg.get("market_score_entry_weight"), 0.42)) / max(0.1, _num(cfg.get("market_score_direction_weight"), 0.58) + _num(cfg.get("market_score_entry_weight"), 0.42))))
    combined_quality = min(direction_score, entry_score, market_score)
    if decision_state != "EXECUTABLE":
        adaptive_exit_tier = "reject" if direction_state == "denied_direction" else "wait"
    elif combined_quality >= int(cfg.get("v2_high_quality_score", 74)):
        adaptive_exit_tier = "high_quality"
    elif combined_quality >= int(cfg.get("v2_medium_quality_score", 62)):
        adaptive_exit_tier = "medium_quality"
    else:
        adaptive_exit_tier = "low_quality"
        decision_state = "WAIT_CONFIRM"
        wait_state = "WAIT_CONFIRM"
        entry_quality_state = "entry_price_needs_confirmation"
        reasons.append("strategy6_v2_quality_too_low_for_market_entry")

    if spread > float(cfg["max_spread_bps"]):
        reasons.append("strategy6_v2_spread_too_wide")

    return {
        "direction_acceptance_score": direction_score,
        "entry_price_quality_score": entry_score,
        "market_acceptance_score": max(0, min(100, market_score)),
        "strategy6_version": "v2",
        "direction_state": direction_state,
        "entry_quality_state": entry_quality_state,
        "adaptive_exit_tier": adaptive_exit_tier,
        "decision_state": decision_state,
        "wait_state": wait_state,
        "reason_codes": list(dict.fromkeys(reasons or ["strategy6_v2_market_accepted_entry_ok"])),
    }


def score_market_acceptance_v3(features: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Strategy6 V3: stronger direction context and entry confirmation.

    V3 is opt-in and keeps the same outer decision states as V1/V2. Extra
    fields are evidence-only until the matrix proves the configuration is
    useful enough to promote.
    """

    base = score_market_acceptance_v2(features, {**cfg, "strategy6_version": "v2"})
    side = str(features.get("legacy_side") or "NO_TRADE").upper()
    if side not in {"LONG", "SHORT"}:
        return {
            **base,
            "strategy6_version": "v3",
            "direction_context_score": 0,
            "btc_alignment": str(features.get("btc_alignment") or "unknown"),
            "reverse_momentum_bps_1m": 0.0,
            "reverse_momentum_bps_3m": 0.0,
            "fake_breakout_flag": False,
            "direction_gate_state": "technical_blocked",
            "entry_confirmation_state": "entry_unavailable",
            "quality_filter_state": "not_evaluated",
            "reason_codes": list(dict.fromkeys([*list(base.get("reason_codes") or []), "strategy6_v3_no_legacy_side"])),
        }

    sign = _side_sign(side)
    pct_1m = _num(features.get("pct_1m_bps"))
    pct_3m = _num(features.get("pct_3m_bps"))
    pct_5m = _num(features.get("pct_5m_bps"))
    range_pos = _num(features.get("range_pos"), 0.5)
    distance = max(abs(_num(features.get("distance_to_vwap_bps"))), abs(_num(features.get("distance_to_ema_bps"))))
    aligned_1m = sign * pct_1m
    aligned_3m = sign * pct_3m
    aligned_5m = sign * pct_5m
    reverse_1m = max(0.0, -aligned_1m)
    reverse_3m = max(0.0, -aligned_3m)
    btc_alignment = str(features.get("btc_alignment") or "unknown").lower()
    btc_bonus = 0.0
    if btc_alignment in {"same", "aligned", "with_trend", "trend_aligned"}:
        btc_bonus += 6.0
    elif btc_alignment in {"opposite", "against", "counter", "counter_trend"}:
        btc_bonus -= 10.0

    fake_breakout = False
    range_threshold = float(cfg.get("v3_fake_breakout_range_pos", 0.88))
    if side == "LONG":
        fake_breakout = range_pos >= range_threshold and aligned_1m < 0
    elif side == "SHORT":
        fake_breakout = range_pos <= (1.0 - range_threshold) and aligned_1m < 0

    context_score = _num(base.get("direction_acceptance_score"))
    context_score += btc_bonus
    context_score += max(-12.0, min(8.0, aligned_5m * 0.04))
    context_score -= min(28.0, reverse_1m * 1.2)
    context_score -= min(18.0, reverse_3m * 0.45)
    if fake_breakout:
        context_score -= 14.0
    context_i = max(0, min(100, int(round(context_score))))

    reasons = list(base.get("reason_codes") or [])
    hard_deny = int(cfg.get("v3_hard_deny_context_score", 38))
    min_context = int(cfg.get("v3_min_direction_context_score", 62))
    uncertain_context = int(cfg.get("v3_uncertain_direction_context_score", 52))
    reverse_1m_deny = float(cfg.get("v3_reverse_1m_deny_bps", 12.0))
    reverse_3m_deny = float(cfg.get("v3_reverse_3m_deny_bps", 24.0))

    decision_state = str(base.get("decision_state") or "WAIT_CONFIRM")
    wait_state = str(base.get("wait_state") or "WAIT_CONFIRM")
    direction_state = str(base.get("direction_state") or "uncertain_direction")
    direction_gate_state = "accepted"
    if context_i < hard_deny or (reverse_1m >= reverse_1m_deny and reverse_3m >= reverse_3m_deny):
        direction_state = "denied_direction"
        direction_gate_state = "denied"
        decision_state = "DENY_DIRECTION_CONFLICT"
        wait_state = "NONE"
        reasons.append("strategy6_v3_direction_context_denied")
    elif context_i < uncertain_context or fake_breakout:
        direction_state = "uncertain_direction"
        direction_gate_state = "uncertain"
        decision_state = "WAIT_CONFIRM"
        wait_state = "WAIT_CONFIRM"
        reasons.append("strategy6_v3_direction_context_uncertain")
    elif context_i < min_context:
        direction_state = "uncertain_direction"
        direction_gate_state = "uncertain"
        decision_state = "WAIT_CONFIRM"
        wait_state = "WAIT_CONFIRM"
        reasons.append("strategy6_v3_direction_context_needs_confirmation")
    else:
        reasons.append("strategy6_v3_direction_context_accepted")

    entry_confirmation_state = "confirmed"
    if decision_state == "EXECUTABLE":
        chase_bps = max(0.0, aligned_1m)
        max_slippage = float(cfg.get("v3_max_entry_slippage_bps", 45.0))
        second_acceptance = float(cfg.get("v3_second_acceptance_min_bps", 4.0))
        extreme = (side == "LONG" and range_pos > float(cfg["long_max_range_pos"])) or (
            side == "SHORT" and range_pos < float(cfg["short_min_range_pos"])
        )
        if fake_breakout or reverse_1m > 0:
            entry_confirmation_state = "wait_second_acceptance"
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
            reasons.append("strategy6_v3_wait_second_acceptance")
        elif extreme or chase_bps > max_slippage or distance > float(cfg.get("v2_distance_from_mean_max_bps", 85.0)):
            entry_confirmation_state = "wait_rebound"
            decision_state = "WAIT_REBOUND"
            wait_state = "WAIT_REBOUND"
            reasons.append("strategy6_v3_entry_price_too_far")
        elif aligned_1m < second_acceptance:
            entry_confirmation_state = "wait_second_acceptance"
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
            reasons.append("strategy6_v3_wait_second_acceptance")
        else:
            reasons.append("strategy6_v3_rebound_confirmed")
    elif decision_state in {"WAIT_REBOUND", "WAIT_CONFIRM", "WAIT_MARKET_ACCEPTANCE"}:
        entry_confirmation_state = "waiting"
        chase_bps = max(0.0, aligned_1m)
        max_slippage = float(cfg.get("v3_max_entry_slippage_bps", 45.0))
        extreme = (side == "LONG" and range_pos > float(cfg["long_max_range_pos"])) or (
            side == "SHORT" and range_pos < float(cfg["short_min_range_pos"])
        )
        if decision_state == "WAIT_REBOUND" or extreme or chase_bps > max_slippage or distance > float(cfg.get("v2_distance_from_mean_max_bps", 85.0)):
            reasons.append("strategy6_v3_entry_price_too_far")
        elif reverse_1m > 0 or aligned_1m < float(cfg.get("v3_second_acceptance_min_bps", 4.0)):
            reasons.append("strategy6_v3_wait_second_acceptance")

    quality_filter_state = "pass"
    filter_mode = str(cfg.get("v3_quality_filter_mode", "shadow")).lower()
    symbol = str(features.get("symbol") or "").upper()
    bad_symbols = {str(item).upper() for item in cfg.get("v3_bad_symbols", [])}
    bad_sides = {str(item).upper() for item in cfg.get("v3_bad_sides", [])}
    if symbol in bad_symbols or side in bad_sides:
        quality_filter_state = "shadow_block" if filter_mode != "block" else "blocked"
        reasons.append("strategy6_v3_bad_symbol_shadow" if symbol in bad_symbols else "strategy6_v3_bad_side_shadow")
        if filter_mode == "block" and decision_state == "EXECUTABLE":
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
    else:
        reasons.append("strategy6_v3_quality_filter_pass")

    combined_quality = min(context_i, int(base.get("entry_price_quality_score") or 0), int(base.get("market_acceptance_score") or 0))
    if decision_state == "EXECUTABLE":
        adaptive_exit_tier = "high_quality" if combined_quality >= int(cfg.get("v2_high_quality_score", 74)) else "medium_quality"
    elif decision_state == "DENY_DIRECTION_CONFLICT":
        adaptive_exit_tier = "reject"
    else:
        adaptive_exit_tier = "wait"

    return {
        **base,
        "strategy6_version": "v3",
        "direction_acceptance_score": context_i,
        "market_acceptance_score": max(0, min(100, int(round((_num(base.get("market_acceptance_score")) + context_i) / 2.0)))),
        "direction_context_score": context_i,
        "btc_alignment": str(features.get("btc_alignment") or "unknown"),
        "reverse_momentum_bps_1m": round(reverse_1m, 6),
        "reverse_momentum_bps_3m": round(reverse_3m, 6),
        "fake_breakout_flag": bool(fake_breakout),
        "direction_gate_state": direction_gate_state,
        "entry_confirmation_state": entry_confirmation_state,
        "quality_filter_state": quality_filter_state,
        "direction_state": direction_state,
        "entry_quality_state": "entry_confirmed" if entry_confirmation_state == "confirmed" else "entry_needs_confirmation",
        "adaptive_exit_tier": adaptive_exit_tier,
        "decision_state": decision_state,
        "wait_state": wait_state,
        "reason_codes": list(dict.fromkeys(reasons or ["strategy6_v3_market_accepted_entry_ok"])),
    }


def _btc_against_v3_1(features: dict[str, Any], side: str) -> bool:
    state = str(features.get("btc_alignment") or "unknown").lower()
    if state in {"", "unknown", "missing", "neutral", "same", "aligned", "with_trend", "trend_aligned"}:
        return False
    if "against" in state or "opposite" in state or "counter" in state:
        return True
    if side == "LONG":
        return any(token in state for token in ("bear", "down", "short"))
    if side == "SHORT":
        return any(token in state for token in ("bull", "up", "long"))
    return False


def score_market_acceptance_v3_1(features: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Strategy6 V3.1: opt-in calibration from STEP22.26-22.29.

    V3.1 keeps V3's outer contract and adds stricter side-adjusted context
    checks for direction_wrong and range/low-followthrough buckets.
    """

    v3_cfg = {
        **cfg,
        "strategy6_version": "v3",
        "v3_min_direction_context_score": cfg.get("v3_1_min_direction_context_score", cfg.get("v3_min_direction_context_score", 62)),
        "v3_uncertain_direction_context_score": cfg.get("v3_1_uncertain_direction_context_score", cfg.get("v3_uncertain_direction_context_score", 52)),
        "v3_hard_deny_context_score": cfg.get("v3_1_hard_deny_context_score", cfg.get("v3_hard_deny_context_score", 38)),
        "v3_reverse_1m_deny_bps": cfg.get("v3_1_reverse_1m_deny_bps", cfg.get("v3_reverse_1m_deny_bps", 12.0)),
        "v3_reverse_3m_deny_bps": cfg.get("v3_1_reverse_3m_deny_bps", cfg.get("v3_reverse_3m_deny_bps", 24.0)),
    }
    base = score_market_acceptance_v3(features, v3_cfg)
    side = str(features.get("legacy_side") or "NO_TRADE").upper()
    if side not in {"LONG", "SHORT"}:
        return {**base, "strategy6_version": "v3_1"}

    sign = _side_sign(side)
    pct_1m = _num(features.get("pct_1m_bps"))
    pct_3m = _num(features.get("pct_3m_bps"))
    pct_5m = _num(features.get("pct_5m_bps"))
    aligned_1m = sign * pct_1m
    aligned_3m = sign * pct_3m
    aligned_5m = sign * pct_5m
    adverse_1m = max(0.0, -aligned_1m)
    adverse_3m = max(0.0, -aligned_3m)
    volume_z = _num(features.get("volume_z"), 0.0)
    range_pos = _num(features.get("range_pos"), 0.5)
    range_extreme_pos = float(cfg.get("v3_1_range_extreme_pos", 0.72))
    range_extreme = (side == "LONG" and range_pos >= range_extreme_pos) or (
        side == "SHORT" and range_pos <= 1.0 - range_extreme_pos
    )
    low_followthrough = volume_z < float(cfg.get("v3_1_low_followthrough_min_volume_z", 0.85)) or aligned_5m < float(
        cfg.get("v3_1_low_followthrough_min_5m_bps", 8.0)
    )
    btc_against = _btc_against_v3_1(features, side)
    reasons = list(base.get("reason_codes") or [])
    decision_state = str(base.get("decision_state") or "WAIT_CONFIRM")
    wait_state = str(base.get("wait_state") or "WAIT_CONFIRM")
    direction_state = str(base.get("direction_state") or "uncertain_direction")
    direction_gate_state = str(base.get("direction_gate_state") or "uncertain")
    entry_confirmation_state = str(base.get("entry_confirmation_state") or "waiting")

    if adverse_1m >= float(cfg.get("v3_1_reverse_1m_deny_bps", 12.0)):
        decision_state = "DENY_DIRECTION_CONFLICT"
        wait_state = "NONE"
        direction_state = "denied_direction"
        direction_gate_state = "denied"
        reasons.append("strategy6_v3_1_reverse_1m_denied")
    elif adverse_3m >= float(cfg.get("v3_1_reverse_3m_deny_bps", 24.0)):
        decision_state = "DENY_DIRECTION_CONFLICT"
        wait_state = "NONE"
        direction_state = "denied_direction"
        direction_gate_state = "denied"
        reasons.append("strategy6_v3_1_reverse_3m_denied")
    elif btc_against:
        action = str(cfg.get("v3_1_btc_against_action", "wait")).lower()
        reasons.append("strategy6_v3_1_btc_against")
        if action == "deny":
            decision_state = "DENY_DIRECTION_CONFLICT"
            wait_state = "NONE"
            direction_state = "denied_direction"
            direction_gate_state = "denied"
        elif action == "wait" and decision_state == "EXECUTABLE":
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
            direction_state = "uncertain_direction"
            direction_gate_state = "uncertain"
    elif range_extreme and decision_state == "EXECUTABLE":
        decision_state = "WAIT_REBOUND"
        wait_state = "WAIT_REBOUND"
        entry_confirmation_state = "wait_rebound"
        reasons.append("strategy6_v3_1_range_extreme_wait_rebound")
    elif low_followthrough and decision_state == "EXECUTABLE":
        decision_state = "WAIT_CONFIRM"
        wait_state = "WAIT_CONFIRM"
        entry_confirmation_state = "wait_second_acceptance"
        reasons.append("strategy6_v3_1_low_followthrough_wait_confirm")

    if decision_state == "DENY_DIRECTION_CONFLICT":
        adaptive_exit_tier = "reject"
    elif decision_state == "EXECUTABLE":
        adaptive_exit_tier = base.get("adaptive_exit_tier") if base.get("adaptive_exit_tier") != "wait" else "medium_quality"
    else:
        adaptive_exit_tier = "wait"

    return {
        **base,
        "strategy6_version": "v3_1",
        "direction_state": direction_state,
        "direction_gate_state": direction_gate_state,
        "entry_confirmation_state": entry_confirmation_state,
        "adaptive_exit_tier": adaptive_exit_tier,
        "decision_state": decision_state,
        "wait_state": wait_state,
        "v3_1_adverse_1m_bps": round(adverse_1m, 6),
        "v3_1_adverse_3m_bps": round(adverse_3m, 6),
        "v3_1_range_extreme": bool(range_extreme),
        "v3_1_low_followthrough": bool(low_followthrough),
        "v3_1_btc_against": bool(btc_against),
        "reason_codes": list(dict.fromkeys(reasons or ["strategy6_v3_1_market_accepted_entry_ok"])),
    }


def _as_upper_set(values: Any) -> set[str]:
    if not isinstance(values, list):
        return set()
    return {str(item).strip().upper() for item in values if str(item).strip()}


def _as_int_set(values: Any) -> set[int]:
    out: set[int] = set()
    if not isinstance(values, list):
        return out
    for item in values:
        try:
            out.add(int(item))
        except (TypeError, ValueError):
            continue
    return out


def _feature_hour_utc(features: dict[str, Any]) -> int | None:
    raw = features.get("entry_hour_utc", features.get("hour_utc", features.get("signal_hour_utc")))
    if raw is None:
        raw_time = features.get("entry_time") or features.get("signal_time") or features.get("entry_time_iso")
        if raw_time:
            try:
                return parse_iso_z(str(raw_time)).hour
            except Exception:
                return None
        return None
    try:
        hour = int(raw)
    except (TypeError, ValueError):
        return None
    return hour if 0 <= hour <= 23 else None


def score_market_acceptance_v3_2(features: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Strategy6 V3.2: V3 baseline with side/BTC/quality evidence gates.

    V3.2 deliberately starts from V3 instead of V3.1.  STEP7.106 showed the
    heavier V3.1 filters reduced PF, so V3.2 only adds side-specific direction
    and BTC/quality gates as opt-in, auditable overlays.
    """

    side = str(features.get("legacy_side") or "NO_TRADE").upper()
    if side == "LONG":
        min_context = cfg.get("v3_2_long_min_direction_context_score", cfg.get("v3_min_direction_context_score", 64))
        reverse_1m = cfg.get("v3_2_long_reverse_1m_deny_bps", cfg.get("v3_reverse_1m_deny_bps", 10.0))
        reverse_3m = cfg.get("v3_2_long_reverse_3m_deny_bps", cfg.get("v3_reverse_3m_deny_bps", 22.0))
        btc_action = str(cfg.get("v3_2_long_btc_against_action", "wait")).lower()
        side_profile = "long_strict"
    elif side == "SHORT":
        min_context = cfg.get("v3_2_short_min_direction_context_score", cfg.get("v3_min_direction_context_score", 58))
        reverse_1m = cfg.get("v3_2_short_reverse_1m_deny_bps", cfg.get("v3_reverse_1m_deny_bps", 16.0))
        reverse_3m = cfg.get("v3_2_short_reverse_3m_deny_bps", cfg.get("v3_reverse_3m_deny_bps", 30.0))
        btc_action = str(cfg.get("v3_2_short_btc_against_action", "shadow")).lower()
        side_profile = "short_baseline"
    else:
        base = score_market_acceptance_v3(features, {**cfg, "strategy6_version": "v3"})
        return {
            **base,
            "strategy6_version": "v3_2",
            "v3_2_side_profile": "no_side",
            "v3_2_btc_against": False,
            "v3_2_quality_filter_state": "not_evaluated",
        }

    v3_cfg = {
        **cfg,
        "strategy6_version": "v3",
        "v3_min_direction_context_score": min_context,
        "v3_uncertain_direction_context_score": max(42, int(min_context) - 10),
        "v3_hard_deny_context_score": cfg.get("v3_hard_deny_context_score", 38),
        "v3_reverse_1m_deny_bps": reverse_1m,
        "v3_reverse_3m_deny_bps": reverse_3m,
    }
    base = score_market_acceptance_v3(features, v3_cfg)
    reasons = list(base.get("reason_codes") or [])
    decision_state = str(base.get("decision_state") or "WAIT_CONFIRM")
    wait_state = str(base.get("wait_state") or "WAIT_CONFIRM")
    direction_state = str(base.get("direction_state") or "uncertain_direction")
    direction_gate_state = str(base.get("direction_gate_state") or "uncertain")
    entry_confirmation_state = str(base.get("entry_confirmation_state") or "waiting")

    btc_against = _btc_against_v3_1(features, side)
    if btc_against:
        reasons.append("strategy6_v3_2_btc_against")
        if btc_action == "deny":
            decision_state = "DENY_DIRECTION_CONFLICT"
            wait_state = "NONE"
            direction_state = "denied_direction"
            direction_gate_state = "denied"
        elif btc_action == "wait" and decision_state == "EXECUTABLE":
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
            direction_state = "uncertain_direction"
            direction_gate_state = "uncertain"

    symbol = str(features.get("symbol") or "").upper()
    hour = _feature_hour_utc(features)
    bad_symbols = _as_upper_set(cfg.get("v3_2_bad_symbols", []))
    bad_sides = _as_upper_set(cfg.get("v3_2_bad_sides", []))
    bad_hours = _as_int_set(cfg.get("v3_2_bad_hours", []))
    quality_hits: list[str] = []
    if symbol in bad_symbols:
        quality_hits.append("symbol")
    if side in bad_sides:
        quality_hits.append("side")
    if hour is not None and hour in bad_hours:
        quality_hits.append("hour")

    filter_mode = str(cfg.get("v3_2_quality_filter_mode", "shadow")).lower()
    if quality_hits:
        quality_filter_state = "blocked" if filter_mode == "block" else "shadow_block"
        reasons.append("strategy6_v3_2_quality_filter_" + "_".join(quality_hits))
        if filter_mode == "block" and decision_state == "EXECUTABLE":
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
            entry_confirmation_state = "waiting"
    else:
        quality_filter_state = "pass"
        reasons.append("strategy6_v3_2_quality_filter_pass")

    if decision_state == "DENY_DIRECTION_CONFLICT":
        adaptive_exit_tier = "reject"
    elif decision_state == "EXECUTABLE":
        adaptive_exit_tier = base.get("adaptive_exit_tier") if base.get("adaptive_exit_tier") != "wait" else "medium_quality"
    else:
        adaptive_exit_tier = "wait"

    return {
        **base,
        "strategy6_version": "v3_2",
        "direction_state": direction_state,
        "direction_gate_state": direction_gate_state,
        "entry_confirmation_state": entry_confirmation_state,
        "adaptive_exit_tier": adaptive_exit_tier,
        "decision_state": decision_state,
        "wait_state": wait_state,
        "v3_2_side_profile": side_profile,
        "v3_2_btc_against": bool(btc_against),
        "v3_2_btc_action": btc_action,
        "v3_2_quality_filter_state": quality_filter_state,
        "v3_2_quality_filter_hits": quality_hits,
        "reason_codes": list(dict.fromkeys(reasons or ["strategy6_v3_2_market_accepted_entry_ok"])),
    }


def score_market_acceptance_v3_3(features: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Strategy6 V3.3: causal direction recheck plus sequential exit contract.

    The entry decision uses only fields that are known at signal/entry time.
    Full-path MFE/MAE and realized PnL are intentionally excluded here; those
    are reserved for post-trade audit and walk-forward validation.
    """

    side = str(features.get("legacy_side") or "NO_TRADE").upper()
    if side == "LONG":
        min_context = cfg.get("v3_3_long_min_direction_context_score", cfg.get("v3_2_long_min_direction_context_score", 66))
    elif side == "SHORT":
        min_context = cfg.get("v3_3_short_min_direction_context_score", cfg.get("v3_2_short_min_direction_context_score", 58))
    else:
        base = score_market_acceptance_v3_2(features, {**cfg, "strategy6_version": "v3_2"})
        return {
            **base,
            "strategy6_version": "v3_3",
            "v3_3_no_lookahead": True,
            "v3_3_known_at_contract": _v3_3_known_at_contract(),
        }

    v3_2_cfg = {
        **cfg,
        "strategy6_version": "v3_2",
        "v3_2_long_min_direction_context_score": min_context if side == "LONG" else cfg.get("v3_2_long_min_direction_context_score", 64),
        "v3_2_short_min_direction_context_score": min_context if side == "SHORT" else cfg.get("v3_2_short_min_direction_context_score", 58),
    }
    base = score_market_acceptance_v3_2(features, v3_2_cfg)
    sign = _side_sign(side)
    aligned_1m = sign * _num(features.get("pct_1m_bps"))
    aligned_3m = sign * _num(features.get("pct_3m_bps"))
    aligned_5m = sign * _num(features.get("pct_5m_bps"))
    adverse_1m = max(0.0, -aligned_1m)
    adverse_3m = max(0.0, -aligned_3m)
    weak_followthrough = aligned_5m < _num(cfg.get("v3_3_weak_followthrough_wait_bps"), 4.0)
    weak_volume = _num(features.get("volume_z"), 1.0) < _num(cfg.get("v3_3_min_volume_z"), 0.6)

    reasons = list(base.get("reason_codes") or [])
    decision_state = str(base.get("decision_state") or "WAIT_CONFIRM")
    wait_state = str(base.get("wait_state") or "WAIT_CONFIRM")
    direction_state = str(base.get("direction_state") or "uncertain_direction")
    direction_gate_state = str(base.get("direction_gate_state") or "uncertain")
    entry_confirmation_state = str(base.get("entry_confirmation_state") or "waiting")

    if adverse_3m >= _num(cfg.get("v3_3_adverse_3m_deny_bps"), 18.0):
        decision_state = "DENY_DIRECTION_CONFLICT"
        wait_state = "NONE"
        direction_state = "denied_direction"
        direction_gate_state = "denied"
        entry_confirmation_state = "rejected"
        reasons.append("strategy6_v3_3_causal_adverse_3m_denied")
    elif adverse_1m >= _num(cfg.get("v3_3_adverse_1m_wait_bps"), 6.0):
        if decision_state == "EXECUTABLE":
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
            direction_state = "uncertain_direction"
            direction_gate_state = "uncertain"
            entry_confirmation_state = "waiting"
        reasons.append("strategy6_v3_3_causal_adverse_1m_wait")
    elif decision_state == "EXECUTABLE" and (weak_followthrough or weak_volume):
        decision_state = "WAIT_CONFIRM"
        wait_state = "WAIT_CONFIRM"
        entry_confirmation_state = "waiting"
        if weak_followthrough:
            reasons.append("strategy6_v3_3_weak_followthrough_wait")
        if weak_volume:
            reasons.append("strategy6_v3_3_weak_volume_wait")

    if decision_state == "DENY_DIRECTION_CONFLICT":
        adaptive_exit_tier = "reject"
    elif decision_state == "EXECUTABLE":
        adaptive_exit_tier = "v3_3_causal_confirmed"
    else:
        adaptive_exit_tier = "wait"

    return {
        **base,
        "strategy6_version": "v3_3",
        "direction_state": direction_state,
        "direction_gate_state": direction_gate_state,
        "entry_confirmation_state": entry_confirmation_state,
        "adaptive_exit_tier": adaptive_exit_tier,
        "decision_state": decision_state,
        "wait_state": wait_state,
        "v3_3_no_lookahead": True,
        "v3_3_known_at_contract": _v3_3_known_at_contract(),
        "v3_3_aligned_1m_bps": round(aligned_1m, 6),
        "v3_3_aligned_3m_bps": round(aligned_3m, 6),
        "v3_3_aligned_5m_bps": round(aligned_5m, 6),
        "v3_3_adverse_1m_bps": round(adverse_1m, 6),
        "v3_3_adverse_3m_bps": round(adverse_3m, 6),
        "v3_3_weak_followthrough": bool(weak_followthrough),
        "v3_3_weak_volume": bool(weak_volume),
        "v3_3_early_abort_enabled": bool(cfg.get("v3_3_early_abort_enabled", True)),
        "v3_3_abort_if_mfe_lt_R": _num(cfg.get("v3_3_abort_if_mfe_lt_R"), 0.10),
        "v3_3_abort_if_mae_gt_R": _num(cfg.get("v3_3_abort_if_mae_gt_R"), 0.45),
        "v3_3_abort_window_min": int(_num(cfg.get("v3_3_abort_window_min"), 3)),
        "v3_3_max_initial_adverse_R": _num(cfg.get("v3_3_max_initial_adverse_R"), 0.75),
        "reason_codes": list(dict.fromkeys(reasons or ["strategy6_v3_3_causal_direction_ok"])),
    }


def _v3_3_known_at_contract() -> dict[str, str]:
    return {
        "legacy_side": "signal_time",
        "pct_1m_bps": "entry_time",
        "pct_3m_bps": "entry_time",
        "pct_5m_bps": "entry_time",
        "pct_15m_bps": "entry_time",
        "volume_z": "entry_time",
        "taker_buy_ratio": "entry_time_if_available",
        "range_pos": "entry_time",
        "spread_bps": "entry_time_if_available",
        "distance_to_vwap_bps": "entry_time_if_available",
        "distance_to_ema_bps": "entry_time_if_available",
        "btc_alignment": "entry_time_if_available",
    }


def score_market_acceptance_v3_4(features: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Strategy6 V3.4: loss-cause-informed causal gates.

    V3.4 only adds entry-time gates that map back to STEP19.34 loss buckets.
    MFE/MAE labels remain diagnostics and are not consumed by this function.
    """

    base = score_market_acceptance_v3_3(
        features,
        {
            **cfg,
            "strategy6_version": "v3_3",
            "v3_3_min_volume_z": cfg.get("v3_3_min_volume_z", min(_num(cfg.get("v3_4_min_volume_z"), 0.8), 0.8)),
        },
    )
    side = str(features.get("legacy_side") or "NO_TRADE").upper()
    if side not in {"LONG", "SHORT"}:
        return {
            **base,
            "strategy6_version": "v3_4",
            "v3_4_no_lookahead": True,
            "v3_4_known_at_contract": _v3_4_known_at_contract(),
        }

    sign = _side_sign(side)
    aligned_1m = sign * _num(features.get("pct_1m_bps"))
    aligned_3m = sign * _num(features.get("pct_3m_bps"))
    aligned_5m = sign * _num(features.get("pct_5m_bps"))
    volume_z = _num(features.get("volume_z"), 1.0)
    range_pos = _num(features.get("range_pos"), 0.5)
    distance_mean = max(abs(_num(features.get("distance_to_vwap_bps"))), abs(_num(features.get("distance_to_ema_bps"))))
    range_extreme = (
        side == "LONG" and range_pos >= _num(cfg.get("v3_4_long_max_range_pos"), 0.86)
    ) or (
        side == "SHORT" and range_pos <= _num(cfg.get("v3_4_short_min_range_pos"), 0.14)
    )
    no_edge = aligned_5m < _num(cfg.get("v3_4_min_followthrough_5m_bps"), 6.0) or volume_z < _num(cfg.get("v3_4_min_volume_z"), 0.8)
    weak_1m = aligned_1m < _num(cfg.get("v3_4_min_aligned_1m_bps"), -2.0)
    weak_3m = aligned_3m < _num(cfg.get("v3_4_min_aligned_3m_bps"), -4.0)
    distance_bad = distance_mean > _num(cfg.get("v3_4_max_distance_to_mean_bps"), 72.0)

    reasons = list(base.get("reason_codes") or [])
    decision_state = str(base.get("decision_state") or "WAIT_CONFIRM")
    wait_state = str(base.get("wait_state") or "WAIT_CONFIRM")
    direction_state = str(base.get("direction_state") or "uncertain_direction")
    direction_gate_state = str(base.get("direction_gate_state") or "uncertain")
    entry_confirmation_state = str(base.get("entry_confirmation_state") or "waiting")
    gate_hits: list[str] = []

    if weak_3m and decision_state == "EXECUTABLE":
        decision_state = "WAIT_CONFIRM"
        wait_state = "WAIT_CONFIRM"
        direction_state = "uncertain_direction"
        direction_gate_state = "uncertain"
        gate_hits.append("weak_3m_alignment")
        reasons.append("strategy6_v3_4_weak_3m_alignment_wait")
    if weak_1m and decision_state == "EXECUTABLE":
        decision_state = "WAIT_CONFIRM"
        wait_state = "WAIT_CONFIRM"
        gate_hits.append("weak_1m_alignment")
        reasons.append("strategy6_v3_4_weak_1m_alignment_wait")
    if no_edge and decision_state == "EXECUTABLE":
        action = str(cfg.get("v3_4_no_edge_action", "wait")).lower()
        gate_hits.append("no_edge")
        reasons.append("strategy6_v3_4_no_edge_" + ("deny" if action == "deny" else "wait"))
        if action == "deny":
            decision_state = "DENY_DIRECTION_CONFLICT"
            wait_state = "NONE"
            direction_state = "denied_direction"
            direction_gate_state = "denied"
            entry_confirmation_state = "rejected"
        else:
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
            entry_confirmation_state = "waiting"
    if range_extreme or distance_bad:
        action = str(cfg.get("v3_4_range_noise_action", "wait_rebound")).lower()
        gate_hits.append("range_noise")
        reasons.append("strategy6_v3_4_range_noise_" + ("deny" if action == "deny" else "wait_rebound"))
        if action == "deny" and decision_state == "EXECUTABLE":
            decision_state = "DENY_DIRECTION_CONFLICT"
            wait_state = "NONE"
            entry_confirmation_state = "rejected"
        elif decision_state == "EXECUTABLE":
            decision_state = "WAIT_REBOUND"
            wait_state = "WAIT_REBOUND"
            entry_confirmation_state = "wait_rebound"

    if decision_state == "DENY_DIRECTION_CONFLICT":
        adaptive_exit_tier = "reject"
    elif decision_state == "EXECUTABLE":
        adaptive_exit_tier = "v3_4_causal_confirmed"
    else:
        adaptive_exit_tier = "wait"

    return {
        **base,
        "strategy6_version": "v3_4",
        "decision_state": decision_state,
        "wait_state": wait_state,
        "direction_state": direction_state,
        "direction_gate_state": direction_gate_state,
        "entry_confirmation_state": entry_confirmation_state,
        "adaptive_exit_tier": adaptive_exit_tier,
        "v3_4_no_lookahead": True,
        "v3_4_known_at_contract": _v3_4_known_at_contract(),
        "v3_4_gate_profile": "loss_cause_causal_gate",
        "v3_4_gate_hits": gate_hits,
        "v3_4_aligned_1m_bps": round(aligned_1m, 6),
        "v3_4_aligned_3m_bps": round(aligned_3m, 6),
        "v3_4_aligned_5m_bps": round(aligned_5m, 6),
        "v3_4_volume_z": round(volume_z, 6),
        "v3_4_range_extreme": bool(range_extreme),
        "v3_4_distance_to_mean_bps": round(distance_mean, 6),
        "v3_4_no_edge": bool(no_edge),
        "reason_codes": list(dict.fromkeys(reasons or ["strategy6_v3_4_market_accepted_entry_ok"])),
    }


def _v3_4_known_at_contract() -> dict[str, str]:
    contract = dict(_v3_3_known_at_contract())
    contract.update(
        {
            "v3_4_min_aligned_1m_bps": "config_time",
            "v3_4_min_aligned_3m_bps": "config_time",
            "v3_4_min_followthrough_5m_bps": "config_time",
            "v3_4_min_volume_z": "config_time",
            "v3_4_long_max_range_pos": "config_time",
            "v3_4_short_min_range_pos": "config_time",
            "v3_4_max_distance_to_mean_bps": "config_time",
        }
    )
    return contract


def score_market_acceptance_v3_5(features: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Strategy6 V3.5: loss-mode routing for exit/overlay experiments.

    V3.5 does not consume post-trade MFE/MAE/root-cause labels. It only routes
    the entry-known evidence so the backtest fill layer can test scratch,
    time-stop, and profit-lock overlays without changing live defaults.
    """

    base = score_market_acceptance_v3_4(features, {**cfg, "strategy6_version": "v3_4"})
    side = str(features.get("legacy_side") or "NO_TRADE").upper()
    if side not in {"LONG", "SHORT"}:
        return {
            **base,
            "strategy6_version": "v3_5",
            "v3_5_no_lookahead": True,
            "v3_5_known_at_contract": _v3_5_known_at_contract(),
            "v3_5_loss_mode": "normal",
            "v3_5_route_reason_codes": ["strategy6_v3_5_no_side_normal"],
        }

    sign = _side_sign(side)
    aligned_1m = sign * _num(features.get("pct_1m_bps"))
    aligned_3m = sign * _num(features.get("pct_3m_bps"))
    aligned_5m = sign * _num(features.get("pct_5m_bps"))
    volume_z = _num(features.get("volume_z"), 1.0)
    range_pos = _num(features.get("range_pos"), 0.5)

    hard_wrong = (
        aligned_1m <= -_num(cfg.get("v3_5_hard_wrong_adverse_1m_bps"), 8.0)
        or aligned_3m <= -_num(cfg.get("v3_5_hard_wrong_adverse_3m_bps"), 18.0)
    )
    no_edge = (
        aligned_5m < _num(cfg.get("v3_5_no_edge_aligned_5m_bps"), 4.0)
        or volume_z < _num(cfg.get("v3_5_no_edge_volume_z"), 0.7)
    )
    rebound_candidate = (
        (side == "LONG" and range_pos >= _num(cfg.get("v3_5_rebound_range_long"), 0.82))
        or (side == "SHORT" and range_pos <= _num(cfg.get("v3_5_rebound_range_short"), 0.18))
    )
    profit_lock_candidate = aligned_5m >= _num(cfg.get("v3_5_profit_lock_min_aligned_5m_bps"), 8.0) and volume_z >= 1.0

    if hard_wrong:
        mode = "hard_wrong"
        route_reasons = ["strategy6_v3_5_hard_wrong_route"]
        exit_tier = "v3_5_fast_scratch"
    elif no_edge:
        mode = "no_edge"
        route_reasons = ["strategy6_v3_5_no_edge_route"]
        exit_tier = "v3_5_fast_scratch"
    elif rebound_candidate:
        mode = "rebound_candidate"
        route_reasons = ["strategy6_v3_5_rebound_candidate_route"]
        exit_tier = "v3_5_rebound"
    elif profit_lock_candidate:
        mode = "profit_lock_candidate"
        route_reasons = ["strategy6_v3_5_profit_lock_candidate_route"]
        exit_tier = "v3_5_profit_lock"
    else:
        mode = "normal"
        route_reasons = ["strategy6_v3_5_normal_route"]
        exit_tier = "v3_5_normal"

    reasons = list(dict.fromkeys([*list(base.get("reason_codes") or []), *route_reasons]))
    return {
        **base,
        "strategy6_version": "v3_5",
        "adaptive_exit_tier": exit_tier if str(base.get("decision_state")) == "EXECUTABLE" else base.get("adaptive_exit_tier"),
        "v3_5_no_lookahead": True,
        "v3_5_known_at_contract": _v3_5_known_at_contract(),
        "v3_5_loss_mode": mode,
        "v3_5_route_reason_codes": route_reasons,
        "v3_5_aligned_1m_bps": round(aligned_1m, 6),
        "v3_5_aligned_3m_bps": round(aligned_3m, 6),
        "v3_5_aligned_5m_bps": round(aligned_5m, 6),
        "v3_5_volume_z": round(volume_z, 6),
        "v3_5_range_pos": round(range_pos, 6),
        "reason_codes": reasons,
    }


def _v3_5_known_at_contract() -> dict[str, str]:
    contract = dict(_v3_4_known_at_contract())
    contract.update(
        {
            "v3_5_no_edge_aligned_5m_bps": "config_time",
            "v3_5_no_edge_volume_z": "config_time",
            "v3_5_hard_wrong_adverse_1m_bps": "config_time",
            "v3_5_hard_wrong_adverse_3m_bps": "config_time",
            "v3_5_rebound_range_long": "config_time",
            "v3_5_rebound_range_short": "config_time",
            "v3_5_profit_lock_min_aligned_5m_bps": "config_time",
        }
    )
    return contract


def score_market_acceptance_v3_6(features: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    """Strategy6 V3.6: entry-side hard deny / wait gates for known loss modes.

    V3.6 keeps the V3.5 exit overlay contract, but moves the two clearest
    backtest loss modes (`direction_wrong`, `signal_no_edge`) into entry-known
    gating. It deliberately does not read MFE/MAE/net_R/root-cause labels.
    """

    base_cfg = {
        **cfg,
        "strategy6_version": "v3_5",
        # V3.6 owns the no-edge gate; keep V3.4 from pre-empting it.
        "v3_4_min_followthrough_5m_bps": cfg.get("v3_6_base_v3_4_followthrough_bps", -999.0),
        "v3_4_min_volume_z": cfg.get("v3_6_base_v3_4_min_volume_z", 0.0),
    }
    base = score_market_acceptance_v3_5(features, base_cfg)
    side = str(features.get("legacy_side") or "NO_TRADE").upper()
    if side not in {"LONG", "SHORT"}:
        return {
            **base,
            "strategy6_version": "v3_6",
            "v3_6_no_lookahead": True,
            "v3_6_known_at_contract": _v3_6_known_at_contract(),
            "v3_6_direction_gate": "technical_blocked",
            "v3_6_signal_edge_gate": "not_evaluated",
            "v3_6_hard_wrong": False,
            "v3_6_no_edge": False,
        }

    sign = _side_sign(side)
    aligned_1m = sign * _num(features.get("pct_1m_bps"))
    aligned_3m = sign * _num(features.get("pct_3m_bps"))
    aligned_5m = sign * _num(features.get("pct_5m_bps"))
    volume_z = _num(features.get("volume_z"), 1.0)
    hard_wrong = (
        aligned_1m <= -_num(cfg.get("v3_6_hard_wrong_1m_bps"), 10.0)
        or aligned_3m <= -_num(cfg.get("v3_6_hard_wrong_3m_bps"), 22.0)
    )
    no_edge = (
        aligned_5m < _num(cfg.get("v3_6_min_followthrough_5m_bps"), 6.0)
        or volume_z < _num(cfg.get("v3_6_min_volume_z"), 0.9)
    )
    no_edge_action = str(cfg.get("v3_6_no_edge_action", "wait")).lower()
    reasons = list(base.get("reason_codes") or [])
    decision_state = str(base.get("decision_state") or "WAIT_CONFIRM")
    wait_state = str(base.get("wait_state") or "WAIT_CONFIRM")
    direction_state = str(base.get("direction_state") or "uncertain_direction")
    direction_gate = "accepted"
    signal_edge_gate = "accepted"
    adaptive_exit_tier = base.get("adaptive_exit_tier")

    if hard_wrong:
        decision_state = "DENY_DIRECTION_CONFLICT"
        wait_state = "NONE"
        direction_state = "denied_direction"
        direction_gate = "hard_wrong_denied"
        adaptive_exit_tier = "reject"
        reasons.append("strategy6_v3_6_hard_wrong_deny")
    elif no_edge:
        signal_edge_gate = "no_edge_" + ("deny" if no_edge_action == "deny" else "wait")
        if no_edge_action == "deny":
            decision_state = "DENY_DIRECTION_CONFLICT"
            wait_state = "NONE"
            direction_state = "denied_direction"
            adaptive_exit_tier = "reject"
            reasons.append("strategy6_v3_6_no_edge_deny")
        elif decision_state == "EXECUTABLE":
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
            adaptive_exit_tier = "wait"
            reasons.append("strategy6_v3_6_no_edge_wait")
        else:
            decision_state = "WAIT_CONFIRM"
            wait_state = "WAIT_CONFIRM"
            adaptive_exit_tier = "wait"
            reasons.append("strategy6_v3_6_no_edge_wait")

    return {
        **base,
        "strategy6_version": "v3_6",
        "decision_state": decision_state,
        "wait_state": wait_state,
        "direction_state": direction_state,
        "adaptive_exit_tier": adaptive_exit_tier,
        "v3_6_no_lookahead": True,
        "v3_6_known_at_contract": _v3_6_known_at_contract(),
        "v3_6_direction_gate": direction_gate,
        "v3_6_signal_edge_gate": signal_edge_gate,
        "v3_6_hard_wrong": bool(hard_wrong),
        "v3_6_no_edge": bool(no_edge),
        "v3_6_aligned_1m_bps": round(aligned_1m, 6),
        "v3_6_aligned_3m_bps": round(aligned_3m, 6),
        "v3_6_aligned_5m_bps": round(aligned_5m, 6),
        "v3_6_volume_z": round(volume_z, 6),
        "reason_codes": list(dict.fromkeys(reasons)),
    }


def _v3_6_known_at_contract() -> dict[str, str]:
    contract = dict(_v3_5_known_at_contract())
    contract.update(
        {
            "v3_6_hard_wrong_1m_bps": "config_time",
            "v3_6_hard_wrong_3m_bps": "config_time",
            "v3_6_min_followthrough_5m_bps": "config_time",
            "v3_6_min_volume_z": "config_time",
            "v3_6_no_edge_action": "config_time",
            "v3_6_base_v3_4_followthrough_bps": "config_time",
            "v3_6_base_v3_4_min_volume_z": "config_time",
            "pct_1m_bps": "entry_snapshot",
            "pct_3m_bps": "entry_snapshot",
            "pct_5m_bps": "entry_snapshot",
            "volume_z": "entry_snapshot",
        }
    )
    return contract


def score_strategy6(features: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    version = str(cfg.get("strategy6_version") or "v1").lower()
    if version in {"v3_6", "v3.6"}:
        return score_market_acceptance_v3_6(features, cfg)
    if version in {"v3_5", "v3.5"}:
        return score_market_acceptance_v3_5(features, cfg)
    if version in {"v3_4", "v3.4"}:
        return score_market_acceptance_v3_4(features, cfg)
    if version in {"v3_3", "v3.3"}:
        return score_market_acceptance_v3_3(features, cfg)
    if version in {"v3_2", "v3.2"}:
        return score_market_acceptance_v3_2(features, cfg)
    if version in {"v3_1", "v3.1"}:
        return score_market_acceptance_v3_1(features, cfg)
    if version == "v3":
        return score_market_acceptance_v3(features, cfg)
    if version == "v2":
        return score_market_acceptance_v2(features, cfg)
    return score_market_acceptance(features, cfg)


def _evidence_id(run_id: str | None, symbol: str) -> str:
    return f"{run_id or 'no_run'}:{symbol}:strategy6"


def _pool_id(symbol: str) -> str:
    return f"strategy6:{str(symbol).upper()}"


def _plan_hash(plan: dict[str, Any]) -> str:
    raw = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _safe_iso(value: Any, fallback: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return fallback
    try:
        return to_iso_z(parse_iso_z(raw))
    except Exception:
        return fallback


def _iso_add_seconds(value: str, seconds: int) -> str:
    try:
        return to_iso_z(parse_iso_z(value) + timedelta(seconds=max(0, int(seconds))))
    except Exception:
        return to_iso_z(utc_now() + timedelta(seconds=max(0, int(seconds))))


def _status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or row.get("decision_state") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def _is_wait_state(state: str) -> bool:
    return str(state or "").upper().startswith("WAIT")


def _is_hard_deny_state(state: str, reason_codes: list[Any]) -> bool:
    state_u = str(state or "").upper()
    reasons = {str(x) for x in reason_codes}
    return state_u in {"DENY_DIRECTION_CONFLICT", "HARD_DENY"} or "strategy6_direction_hard_deny" in reasons


def _is_retryable_technical(state: str, reason_codes: list[Any]) -> bool:
    state_u = str(state or "").upper()
    if state_u != "TECHNICAL_BLOCKED":
        return False
    text = " ".join(str(x).lower() for x in reason_codes)
    return any(token in text for token in ("data", "incomplete", "warmup", "stale", "missing", "technical"))


def _age_sec(value: Any) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return max(0, int((utc_now() - parse_iso_z(value)).total_seconds()))
    except Exception:
        return None


def _pid_is_alive(pid: Any) -> bool:
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        return False
    if pid_int <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            process_query_limited_information = 0x1000
            still_active = 259
            handle = ctypes.windll.kernel32.OpenProcess(process_query_limited_information, False, pid_int)
            if not handle:
                return False
            try:
                exit_code = ctypes.c_ulong()
                ok = ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
                return bool(ok) and exit_code.value == still_active
            finally:
                ctypes.windll.kernel32.CloseHandle(handle)
        except Exception:
            pass
    try:
        os.kill(pid_int, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _append_runtime_event(project_root: Path, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    root = Path(project_root)
    p = paths(root)
    _init_db(p.db)
    generated_at = to_iso_z(utc_now())
    event = {
        "schema_version": SCHEMA_VERSION,
        "source": "strategy6_runtime_events",
        "event_id": f"{generated_at}:{event_type}:{hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode('utf-8')).hexdigest()[:12]}",
        "generated_at": generated_at,
        "event_type": event_type,
        "payload": payload,
    }
    try:
        with sqlite3.connect(p.db) as con:
            con.execute(
                """
                insert or replace into strategy6_runtime_events (
                    event_id, generated_at, event_type, payload_json
                ) values (?, ?, ?, ?)
                """,
                (event["event_id"], generated_at, event_type, json.dumps(event, ensure_ascii=False)),
            )
    except sqlite3.Error:
        pass
    return event


def _observe_pool_counts(project_root: Path) -> tuple[int, int]:
    root = Path(project_root)
    p = paths(root)
    if not p.db.is_file():
        return 0, 0
    now = to_iso_z(utc_now())
    try:
        _init_db(p.db)
        with sqlite3.connect(p.db) as con:
            row = con.execute(
                "select count(*) from strategy6_observe_pool where status in ('OBSERVING','WAIT_CONFIRM','WAIT_REBOUND','WAIT_MARKET_ACCEPTANCE','TECHNICAL_BLOCKED')",
            ).fetchone()
            pool_count = int(row[0]) if row else 0
            row = con.execute(
                "select count(*) from strategy6_observe_pool where status in ('OBSERVING','WAIT_CONFIRM','WAIT_REBOUND','WAIT_MARKET_ACCEPTANCE','TECHNICAL_BLOCKED') and (next_check_at is null or next_check_at <= ?)",
                (now,),
            ).fetchone()
            due_count = int(row[0]) if row else 0
            return pool_count, due_count
    except sqlite3.Error:
        return 0, 0


def _init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            create table if not exists strategy6_runs (
                run_id text primary key,
                cycle_id text,
                generated_at text,
                evidence_count integer,
                decision_count integer,
                wait_count integer,
                plan_count integer,
                executable_count integer,
                status text,
                output_path text
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy6_evidence (
                evidence_id text primary key,
                run_id text,
                cycle_id text,
                generated_at text,
                symbol text,
                legacy_side text,
                strategy6_side text,
                direction_acceptance_score integer,
                entry_price_quality_score integer,
                market_acceptance_score integer,
                decision_state text,
                wait_state text,
                executable integer,
                evidence_json text not null,
                plan_json text
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy6_decisions (
                decision_id text primary key,
                run_id text,
                cycle_id text,
                generated_at text,
                symbol text,
                decision_state text,
                wait_state text,
                reason_codes_json text not null,
                executable integer
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy6_wait_pool (
                symbol text primary key,
                run_id text,
                cycle_id text,
                updated_at text,
                legacy_side text,
                wait_state text,
                attempts integer,
                last_reason_codes_json text not null,
                evidence_json text not null
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy6_trade_plan_lineage (
                lineage_id text primary key,
                run_id text,
                cycle_id text,
                generated_at text,
                symbol text,
                base_trade_plan_source text,
                base_trade_plan_run_id text,
                strategy6_evidence_id text,
                source_json text not null
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy6_audit_events (
                event_id text primary key,
                run_id text,
                cycle_id text,
                generated_at text,
                event_type text,
                payload_json text not null
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy6_observe_pool (
                pool_id text primary key,
                symbol text not null,
                status text not null,
                original_run_id text,
                original_cycle_id text,
                original_side text,
                current_side text,
                wait_state text,
                attempts integer not null default 0,
                first_seen_at text,
                last_checked_at text,
                next_check_at text,
                expires_at text,
                last_direction_acceptance_score integer,
                last_entry_price_quality_score integer,
                last_market_acceptance_score integer,
                reason_codes_json text not null,
                evidence_json text not null,
                last_plan_json text,
                source_evidence_id text,
                source_plan_hash text,
                consumed_at text,
                updated_at text not null
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy6_observe_attempts (
                attempt_id text primary key,
                pool_id text,
                run_id text,
                cycle_id text,
                symbol text not null,
                attempt_no integer not null,
                checked_at text not null,
                decision_state text,
                wait_state text,
                action text,
                entry_mode text,
                executable integer not null,
                direction_acceptance_score integer,
                entry_price_quality_score integer,
                market_acceptance_score integer,
                reason_codes_json text not null,
                evidence_json text not null,
                trade_plan_json text
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy6_daemon_heartbeat (
                singleton text primary key,
                status text not null,
                pid integer,
                heartbeat_at text,
                last_check_at text,
                next_check_at text,
                pool_count integer,
                due_count integer,
                last_error text,
                payload_json text not null
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy6_runtime_events (
                event_id text primary key,
                generated_at text not null,
                event_type text not null,
                payload_json text not null
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy6_consumption_lineage (
                lineage_id text primary key,
                pool_id text,
                attempt_id text,
                symbol text,
                source_run_id text,
                source_cycle_id text,
                paper_order_id text,
                generated_at text,
                payload_json text not null
            )
            """,
        )


def _observe_rows_from_db(con: sqlite3.Connection, *, limit: int = 500, statuses: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
    con.row_factory = sqlite3.Row
    params: list[Any] = []
    where = ""
    if statuses:
        where = "where status in ({})".format(",".join("?" for _ in statuses))
        params.extend(statuses)
    params.append(max(1, min(int(limit or 500), 5000)))
    rows = con.execute(
        f"""
        select * from strategy6_observe_pool
        {where}
        order by
          case when next_check_at is null then 1 else 0 end,
          next_check_at asc,
          updated_at desc
        limit ?
        """,
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _attempt_count_for_pool(con: sqlite3.Connection, pool_id: str) -> int:
    row = con.execute("select count(*) from strategy6_observe_attempts where pool_id=?", (pool_id,)).fetchone()
    return int(row[0]) if row else 0


def _admit_or_update_observe_pool(
    con: sqlite3.Connection,
    *,
    row: dict[str, Any],
    cfg: dict[str, Any],
    now: str,
) -> tuple[str, int, str]:
    symbol = str(row["symbol"]).upper()
    pool_id = _pool_id(symbol)
    ev = row["evidence"]
    plan = row["plan"]
    base_plan = row.get("base_plan") if isinstance(row.get("base_plan"), dict) else plan
    reasons = list(ev.get("reason_codes") or [])
    state = str(row["decision_state"] or "")
    wait_state = str(row["wait_state"] or "NONE")
    existing = con.execute(
        "select attempts, first_seen_at, status from strategy6_observe_pool where pool_id=?",
        (pool_id,),
    ).fetchone()
    attempts = int(existing[0]) + 1 if existing else 1
    first_seen = _safe_iso(existing[1], now) if existing else now
    expires_at = _iso_add_seconds(first_seen, int(cfg["max_observe_age_sec"]))
    next_check = _iso_add_seconds(now, int(cfg["observe_interval_sec"]))
    status = wait_state if _is_wait_state(state) else "TECHNICAL_BLOCKED"
    if attempts > int(cfg["max_attempts"]):
        status = "EXPIRED"
        reasons = list(dict.fromkeys([*reasons, "strategy6_max_attempts_expired"]))
    try:
        expired = parse_iso_z(now) >= parse_iso_z(expires_at)
    except Exception:
        expired = False
    if expired:
        status = "EXPIRED"
        reasons = list(dict.fromkeys([*reasons, "strategy6_max_observe_age_expired"]))
    con.execute(
        """
        insert or replace into strategy6_observe_pool (
            pool_id, symbol, status, original_run_id, original_cycle_id, original_side,
            current_side, wait_state, attempts, first_seen_at, last_checked_at, next_check_at,
            expires_at, last_direction_acceptance_score, last_entry_price_quality_score,
            last_market_acceptance_score, reason_codes_json, evidence_json, last_plan_json,
            source_evidence_id, source_plan_hash, consumed_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pool_id,
            symbol,
            status,
            row.get("run_id"),
            row.get("cycle_id"),
            row.get("legacy_side"),
            row.get("strategy6_side"),
            wait_state,
            attempts,
            first_seen,
            now,
            None if status in {"EXPIRED"} else next_check,
            expires_at,
            int(ev.get("direction_acceptance_score") or 0),
            int(ev.get("entry_price_quality_score") or 0),
            int(ev.get("market_acceptance_score") or 0),
            json.dumps(reasons, ensure_ascii=False),
            json.dumps(ev, ensure_ascii=False),
            json.dumps(base_plan, ensure_ascii=False),
            row.get("evidence_id"),
            _plan_hash(base_plan),
            None,
            now,
        ),
    )
    return pool_id, attempts, status


def _mark_pool_terminal(
    con: sqlite3.Connection,
    *,
    row: dict[str, Any],
    status: str,
    now: str,
) -> tuple[str, int, str]:
    symbol = str(row["symbol"]).upper()
    pool_id = _pool_id(symbol)
    ev = row["evidence"]
    plan = row["plan"]
    existing = con.execute(
        "select attempts, first_seen_at from strategy6_observe_pool where pool_id=?",
        (pool_id,),
    ).fetchone()
    attempts = int(existing[0]) + 1 if existing else 1
    first_seen = _safe_iso(existing[1], now) if existing else now
    con.execute(
        """
        insert or replace into strategy6_observe_pool (
            pool_id, symbol, status, original_run_id, original_cycle_id, original_side,
            current_side, wait_state, attempts, first_seen_at, last_checked_at, next_check_at,
            expires_at, last_direction_acceptance_score, last_entry_price_quality_score,
            last_market_acceptance_score, reason_codes_json, evidence_json, last_plan_json,
            source_evidence_id, source_plan_hash, consumed_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pool_id,
            symbol,
            status,
            row.get("run_id"),
            row.get("cycle_id"),
            row.get("legacy_side"),
            row.get("strategy6_side"),
            row.get("wait_state"),
            attempts,
            first_seen,
            now,
            None,
            None,
            int(ev.get("direction_acceptance_score") or 0),
            int(ev.get("entry_price_quality_score") or 0),
            int(ev.get("market_acceptance_score") or 0),
            json.dumps(ev.get("reason_codes") or [], ensure_ascii=False),
            json.dumps(ev, ensure_ascii=False),
            json.dumps(plan, ensure_ascii=False),
            row.get("evidence_id"),
            _plan_hash(plan),
            now if status == "CONSUMED" else None,
            now,
        ),
    )
    return pool_id, attempts, status


def _append_observe_attempt(
    con: sqlite3.Connection,
    *,
    row: dict[str, Any],
    pool_id: str,
    attempt_no: int,
    checked_at: str,
) -> str:
    ev = row["evidence"]
    plan = row["plan"]
    attempt_id = f"{pool_id}:{row.get('run_id') or 'no_run'}:{attempt_no}"
    con.execute(
        """
        insert or replace into strategy6_observe_attempts (
            attempt_id, pool_id, run_id, cycle_id, symbol, attempt_no, checked_at,
            decision_state, wait_state, action, entry_mode, executable,
            direction_acceptance_score, entry_price_quality_score, market_acceptance_score,
            reason_codes_json, evidence_json, trade_plan_json
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            attempt_id,
            pool_id,
            row.get("run_id"),
            row.get("cycle_id"),
            row.get("symbol"),
            attempt_no,
            checked_at,
            row.get("decision_state"),
            row.get("wait_state"),
            plan.get("action"),
            plan.get("entry_mode"),
            1 if row.get("executable") else 0,
            int(ev.get("direction_acceptance_score") or 0),
            int(ev.get("entry_price_quality_score") or 0),
            int(ev.get("market_acceptance_score") or 0),
            json.dumps(ev.get("reason_codes") or [], ensure_ascii=False),
            json.dumps(ev, ensure_ascii=False),
            json.dumps(plan, ensure_ascii=False),
        ),
    )
    return attempt_id


def _active_observe_pool_rows(con: sqlite3.Connection, *, limit: int = 500) -> list[dict[str, Any]]:
    return _observe_rows_from_db(
        con,
        limit=limit,
        statuses=("OBSERVING", "WAIT_CONFIRM", "WAIT_REBOUND", "WAIT_MARKET_ACCEPTANCE", "TECHNICAL_BLOCKED"),
    )


def _write_latest_observe_views(project_root: Path, con: sqlite3.Connection, *, generated_at: str, limit: int = 500) -> dict[str, Any]:
    p = paths(project_root)
    pool_rows = _active_observe_pool_rows(con, limit=limit)
    items: list[dict[str, Any]] = []
    for row in pool_rows:
        reasons = []
        try:
            reasons = json.loads(row.get("reason_codes_json") or "[]")
        except Exception:
            reasons = []
        items.append(
            {
                "pool_id": row.get("pool_id"),
                "symbol": row.get("symbol"),
                "status": row.get("status"),
                "original_side": row.get("original_side"),
                "current_side": row.get("current_side"),
                "wait_state": row.get("wait_state"),
                "attempts": row.get("attempts"),
                "first_seen_at": row.get("first_seen_at"),
                "last_checked_at": row.get("last_checked_at"),
                "next_check_at": row.get("next_check_at"),
                "expires_at": row.get("expires_at"),
                "reason_codes": reasons,
                "direction_acceptance_score": row.get("last_direction_acceptance_score"),
                "entry_price_quality_score": row.get("last_entry_price_quality_score"),
                "market_acceptance_score": row.get("last_market_acceptance_score"),
                "source_evidence_id": row.get("source_evidence_id"),
            }
        )
    attempts = []
    con.row_factory = sqlite3.Row
    for row in con.execute(
        """
        select attempt_id, pool_id, run_id, cycle_id, symbol, attempt_no, checked_at,
               decision_state, wait_state, action, entry_mode, executable,
               direction_acceptance_score, entry_price_quality_score, market_acceptance_score,
               reason_codes_json
        from strategy6_observe_attempts
        order by checked_at desc
        limit ?
        """,
        (max(1, min(int(limit or 500), 1000)),),
    ).fetchall():
        got = dict(row)
        try:
            got["reason_codes"] = json.loads(got.pop("reason_codes_json") or "[]")
        except Exception:
            got["reason_codes"] = []
        attempts.append(got)
    latest_wait = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "source": "strategy6_wait_pool",
        "count": len(items),
        "status_counts": _status_counts(items),
        "items": items,
    }
    latest_attempts = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "source": "strategy6_observe_attempts",
        "count": len(attempts),
        "items": attempts,
    }
    write_json_atomic(p.latest_wait_pool, latest_wait)
    write_json_atomic(p.latest_observe_attempts, latest_attempts)
    return {"wait_pool": latest_wait, "attempts": latest_attempts}


def _due_observe_base_plans(project_root: Path, *, now: str, limit: int) -> list[dict[str, Any]]:
    p = paths(project_root)
    if not p.db.is_file():
        return []
    _init_db(p.db)
    rows: list[dict[str, Any]] = []
    try:
        with sqlite3.connect(p.db) as con:
            con.row_factory = sqlite3.Row
            got = con.execute(
                """
                select * from strategy6_observe_pool
                where status in ('OBSERVING', 'WAIT_CONFIRM', 'WAIT_REBOUND', 'WAIT_MARKET_ACCEPTANCE', 'TECHNICAL_BLOCKED')
                  and (next_check_at is null or next_check_at <= ?)
                order by next_check_at asc, updated_at asc
                limit ?
                """,
                (now, max(1, min(int(limit or 200), 2000))),
            ).fetchall()
            rows = [dict(row) for row in got]
    except sqlite3.Error:
        rows = []
    plans: list[dict[str, Any]] = []
    for row in rows:
        try:
            plan = json.loads(row.get("last_plan_json") or "{}")
        except Exception:
            plan = {}
        if not isinstance(plan, dict) or not plan.get("symbol"):
            continue
        decision = str(plan.get("decision") or row.get("original_side") or row.get("current_side") or "").upper()
        if decision in {"LONG", "SHORT"}:
            plan["decision"] = decision
            plan["executable"] = True
            plan["action"] = "ENTER_MARKET"
            plan["entry_mode"] = "MARKET"
        refs = plan.get("input_refs") if isinstance(plan.get("input_refs"), dict) else {}
        guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
        plan["input_refs"] = {
            **refs,
            "strategy6_pool_id": row.get("pool_id"),
            "strategy6_observe_recheck": True,
            "strategy6_observe_attempts_before": row.get("attempts"),
            "strategy6_original_run_id": row.get("original_run_id"),
            "strategy6_original_cycle_id": row.get("original_cycle_id"),
        }
        plan["guards"] = {**guards, "strategy6_pool_id": row.get("pool_id"), "strategy6_observe_recheck": True}
        plans.append(plan)
    return plans


def build_strategy6_document(
    project_root: Path,
    *,
    run_id: str | None,
    cycle_id: str | None,
    include_observe_pool: bool = False,
    observe_limit: int | None = None,
) -> dict[str, Any]:
    root = Path(project_root)
    now = to_iso_z(utc_now())
    factor = _factor_doc(root)
    base_doc = _base_trade_plan_doc(root)
    cfg = load_strategy6_config(root)
    archive_path = _trade_plan_archive_path(root, run_id)
    factor_by_symbol = {str(row.get("symbol") or "").upper(): row for row in _items(factor) if row.get("symbol")}
    base_plans = [row for row in _items(base_doc) if row.get("symbol")]
    if include_observe_pool:
        seen = {str(row.get("symbol") or "").upper() for row in base_plans}
        for row in _due_observe_base_plans(root, now=now, limit=observe_limit or int(cfg["max_pool_size"])):
            symbol = str(row.get("symbol") or "").upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            base_plans.append(row)
    plans: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    wait_rows: list[dict[str, Any]] = []
    for base in base_plans:
        symbol = str(base.get("symbol") or "").upper()
        source_base_plan = dict(base)
        ev = build_feature_vector(base, factor_by_symbol.get(symbol, {"symbol": symbol, "move_side": base.get("decision")}))
        score = score_strategy6(ev, cfg)
        evidence_id = _evidence_id(run_id, symbol)
        state = str(score["decision_state"])
        allow = bool(base.get("executable")) and state == "EXECUTABLE"
        plan = dict(base)
        legacy_side = str(ev.get("legacy_side") or base.get("decision") or "NO_TRADE").upper()
        plan["decision"] = legacy_side if legacy_side in {"LONG", "SHORT"} else "NO_TRADE"
        reason_codes = list(dict.fromkeys([*list(base.get("reason_codes") or []), *list(score["reason_codes"])]))
        if not allow and bool(base.get("executable")):
            reason_codes.append("strategy6_gate_blocked_executable")
        if not allow:
            plan["executable"] = False
            if plan["decision"] in {"LONG", "SHORT"} and state != "DENY_DIRECTION_CONFLICT":
                plan["action"] = "WAIT"
                plan["entry_mode"] = "WAIT_REBOUND" if state == "WAIT_REBOUND" else "WAIT_CONFIRMATION"
            elif state == "DENY_DIRECTION_CONFLICT":
                plan["action"] = "NO_TRADE"
                plan["entry_mode"] = "NONE"
                plan["estimated_entry_price"] = None
                plan["stop_loss"] = None
                plan["take_profit"] = None
                plan["risk_per_unit"] = None
                plan["reward_per_unit"] = None
                plan["rr"] = None
                plan["position_sizing"] = None
            else:
                plan["action"] = "WAIT"
                plan["entry_mode"] = "WAIT_CONFIRMATION"
        plan["reason_codes"] = list(dict.fromkeys(reason_codes))
        guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
        plan["guards"] = {
            **guards,
            "line": "strategy6",
            "strategy6_evidence_id": evidence_id,
            "strategy6_legacy_side": legacy_side,
            "strategy6_decision_state": state,
            "strategy6_wait_state": score["wait_state"],
            "strategy6_version": score.get("strategy6_version", "v1"),
            "strategy6_direction_state": score.get("direction_state"),
            "strategy6_entry_quality_state": score.get("entry_quality_state"),
            "strategy6_adaptive_exit_tier": score.get("adaptive_exit_tier"),
            "strategy6_direction_acceptance_score": score["direction_acceptance_score"],
            "strategy6_entry_price_quality_score": score["entry_price_quality_score"],
            "strategy6_market_acceptance_score": score["market_acceptance_score"],
            "strategy6_market_acceptance_mode": cfg["market_acceptance_mode"],
            "strategy6_contract": "market_accepted_entry_v2_no_micro_slot" if score.get("strategy6_version") == "v2" else "market_accepted_entry_v1_no_micro_slot",
            "strategy6_allow_reason": "market_accepted" if allow else state.lower(),
        }
        refs = plan.get("input_refs") if isinstance(plan.get("input_refs"), dict) else {}
        plan["input_refs"] = {
            **refs,
            "strategy6_evidence_id": evidence_id,
            "strategy6_source": SOURCE,
            "strategy6_pool_id": refs.get("strategy6_pool_id") or guards.get("strategy6_pool_id") or _pool_id(symbol),
            "strategy6_observe_recheck": bool(refs.get("strategy6_observe_recheck") or guards.get("strategy6_observe_recheck")),
            "strategy6_trade_plan_latest_path": str(paths(root).latest_trade_plan),
            "strategy6_trade_plan_archive_path": str(archive_path) if archive_path is not None else None,
            "trade_plan_archive_path": str(archive_path) if archive_path is not None else None,
            "base_trade_plan_source": base_doc.get("source"),
            "base_trade_plan_run_id": base_doc.get("run_id"),
            "base_trade_plan_cycle_id": base_doc.get("cycle_id"),
            "factor_source": factor.get("source"),
            "factor_generated_at": factor.get("generated_at"),
        }
        plans.append(plan)
        evidence = {**ev, **score}
        row = {
            "evidence_id": evidence_id,
            "run_id": run_id,
            "cycle_id": cycle_id,
            "generated_at": now,
            "symbol": symbol,
            "legacy_side": legacy_side,
            "strategy6_side": plan["decision"],
            "decision_state": state,
            "wait_state": score["wait_state"],
            "executable": bool(plan.get("executable")),
            "evidence": evidence,
            "plan": plan,
            "base_plan": source_base_plan,
        }
        evidence_rows.append(row)
        if state.startswith("WAIT"):
            wait_rows.append(row)
    doc = TradePlanLineDocument(
        generated_at=now,
        run_id=run_id,
        cycle_id=cycle_id,
        source="trade_plan_strategy6",
        micro_mode="strategy6_market_accepted",
        status="ok" if plans else "no_entries",
        count=len(plans),
        executable_count=sum(1 for row in plans if row.get("executable") is True),
        input_refs={
            "source": SOURCE,
            "factor_source": factor.get("source"),
            "factor_generated_at": factor.get("generated_at"),
            "base_trade_plan_source": base_doc.get("source"),
            "base_trade_plan_run_id": base_doc.get("run_id"),
            "base_trade_plan_cycle_id": base_doc.get("cycle_id"),
            "strategy6_contract": "normal_pipeline_line_market_accepted_entry_no_micro_slot",
            "strategy6_observe_mode": "persistent_observe" if include_observe_pool else "pipeline_gate",
            "strategy6_trade_plan_latest_path": str(paths(root).latest_trade_plan),
            "strategy6_trade_plan_archive_path": str(archive_path) if archive_path is not None else None,
            "trade_plan_archive_path": str(archive_path) if archive_path is not None else None,
            "strategy6_config": cfg,
        },
        candidate_alignment={
            "base_count": len(base_plans),
            "factor_count": len(factor_by_symbol),
            "wait_count": len(wait_rows),
        },
        plans=plans,
    ).model_dump(mode="json")
    return {"trade_plan": doc, "evidence_rows": evidence_rows, "wait_rows": wait_rows}


def write_strategy6_outputs(
    project_root: Path,
    *,
    run_id: str | None,
    cycle_id: str | None,
    include_observe_pool: bool = False,
    observe_limit: int | None = None,
) -> dict[str, Any]:
    root = Path(project_root)
    p = paths(root)
    payload = build_strategy6_document(
        root,
        run_id=run_id,
        cycle_id=cycle_id,
        include_observe_pool=include_observe_pool,
        observe_limit=observe_limit,
    )
    doc = payload["trade_plan"]
    evidence_rows = payload["evidence_rows"]
    wait_rows = payload["wait_rows"]
    write_json_atomic(p.latest_trade_plan, doc)
    archive_path = _trade_plan_archive_path(root, run_id)
    if archive_path is not None:
        write_json_atomic(archive_path, doc)
    latest_evidence = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": doc.get("generated_at"),
        "source": SOURCE,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "count": len(evidence_rows),
        "items": [row["evidence"] | {"symbol": row["symbol"], "evidence_id": row["evidence_id"]} for row in evidence_rows],
    }
    latest_decisions = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": doc.get("generated_at"),
        "source": "strategy6_decisions",
        "run_id": run_id,
        "cycle_id": cycle_id,
        "count": len(evidence_rows),
        "items": [
            {
                "symbol": row["symbol"],
                "evidence_id": row["evidence_id"],
                "legacy_side": row["legacy_side"],
                "strategy6_side": row["strategy6_side"],
                "decision_state": row["decision_state"],
                "wait_state": row["wait_state"],
                "executable": row["executable"],
                "reason_codes": row["evidence"].get("reason_codes") or [],
            }
            for row in evidence_rows
        ],
    }
    write_json_atomic(p.latest_evidence, latest_evidence)
    write_json_atomic(p.latest_decisions, latest_decisions)
    _init_db(p.db)
    with sqlite3.connect(p.db) as con:
        attempt_ids: list[str] = []
        for row in evidence_rows:
            ev = row["evidence"]
            con.execute(
                """
                insert or replace into strategy6_evidence (
                    evidence_id, run_id, cycle_id, generated_at, symbol, legacy_side, strategy6_side,
                    direction_acceptance_score, entry_price_quality_score, market_acceptance_score,
                    decision_state, wait_state, executable, evidence_json, plan_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["evidence_id"],
                    row["run_id"],
                    row["cycle_id"],
                    row["generated_at"],
                    row["symbol"],
                    row["legacy_side"],
                    row["strategy6_side"],
                    int(ev.get("direction_acceptance_score") or 0),
                    int(ev.get("entry_price_quality_score") or 0),
                    int(ev.get("market_acceptance_score") or 0),
                    row["decision_state"],
                    row["wait_state"],
                    1 if row["executable"] else 0,
                    json.dumps(ev, ensure_ascii=False),
                    json.dumps(row["plan"], ensure_ascii=False),
                ),
            )
            con.execute(
                """
                insert or replace into strategy6_decisions (
                    decision_id, run_id, cycle_id, generated_at, symbol, decision_state, wait_state,
                    reason_codes_json, executable
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["evidence_id"],
                    row["run_id"],
                    row["cycle_id"],
                    row["generated_at"],
                    row["symbol"],
                    row["decision_state"],
                    row["wait_state"],
                    json.dumps(ev.get("reason_codes") or [], ensure_ascii=False),
                    1 if row["executable"] else 0,
                ),
            )
            con.execute(
                """
                insert or replace into strategy6_trade_plan_lineage (
                    lineage_id, run_id, cycle_id, generated_at, symbol, base_trade_plan_source,
                    base_trade_plan_run_id, strategy6_evidence_id, source_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"{row['evidence_id']}:lineage",
                    row["run_id"],
                    row["cycle_id"],
                    row["generated_at"],
                    row["symbol"],
                    doc.get("input_refs", {}).get("base_trade_plan_source"),
                    doc.get("input_refs", {}).get("base_trade_plan_run_id"),
                    row["evidence_id"],
                    json.dumps(row["plan"], ensure_ascii=False),
                ),
            )
            reasons = list(ev.get("reason_codes") or [])
            state = str(row.get("decision_state") or "")
            if _is_wait_state(state) or _is_retryable_technical(state, reasons):
                pool_id, attempts, _ = _admit_or_update_observe_pool(con, row=row, cfg=load_strategy6_config(root), now=row["generated_at"])
            elif row["executable"]:
                pool_id, attempts, _ = _mark_pool_terminal(con, row=row, status="EXECUTABLE", now=row["generated_at"])
            elif _is_hard_deny_state(state, reasons):
                pool_id, attempts, _ = _mark_pool_terminal(con, row=row, status="HARD_DENY", now=row["generated_at"])
            else:
                pool_id, attempts = _pool_id(str(row["symbol"])), max(1, _attempt_count_for_pool(con, _pool_id(str(row["symbol"]))) + 1)
            attempt_ids.append(
                _append_observe_attempt(
                    con,
                    row=row,
                    pool_id=pool_id,
                    attempt_no=attempts,
                    checked_at=row["generated_at"],
                )
            )
        con.execute(
            """
            insert or replace into strategy6_runs (
                run_id, cycle_id, generated_at, evidence_count, decision_count, wait_count,
                plan_count, executable_count, status, output_path
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                cycle_id,
                doc.get("generated_at"),
                len(evidence_rows),
                len(evidence_rows),
                len(wait_rows),
                int(doc.get("count") or 0),
                int(doc.get("executable_count") or 0),
                doc.get("status"),
                str(archive_path or p.latest_trade_plan),
            ),
        )
        views = _write_latest_observe_views(root, con, generated_at=str(doc.get("generated_at") or to_iso_z(utc_now())))
    return {
        "status": doc.get("status"),
        "run_id": run_id,
        "cycle_id": cycle_id,
        "count": doc.get("count"),
        "executable_count": doc.get("executable_count"),
        "wait_count": len(views.get("wait_pool", {}).get("items", [])),
        "attempt_count": len(views.get("attempts", {}).get("items", [])),
        "output_path": str(p.latest_trade_plan),
        "archive_path": str(archive_path) if archive_path is not None else None,
        "evidence_path": str(p.latest_evidence),
        "decisions_path": str(p.latest_decisions),
        "wait_pool_path": str(p.latest_wait_pool),
        "attempts_path": str(p.latest_observe_attempts),
        "db_path": str(p.db),
    }


def run_strategy6_pipeline_safe(
    *,
    project_root: Path,
    run_id: str | None = None,
    cycle_id: str | None = None,
    stdout_json: bool = False,
) -> int:
    try:
        result = write_strategy6_outputs(Path(project_root), run_id=run_id, cycle_id=cycle_id)
        if stdout_json:
            print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception as exc:  # pragma: no cover - defensive command boundary
        if stdout_json:
            print(json.dumps({"status": "error", "reason": str(exc)}, ensure_ascii=False))
        return 1


def write_daemon_heartbeat(
    project_root: Path,
    *,
    status: str,
    pid: int | None = None,
    last_check_at: str | None = None,
    next_check_at: str | None = None,
    last_error: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(project_root)
    p = paths(root)
    _init_db(p.db)
    now = to_iso_z(utc_now())
    pool_count = 0
    due_count = 0
    try:
        with sqlite3.connect(p.db) as con:
            row = con.execute(
                "select count(*) from strategy6_observe_pool where status in ('OBSERVING','WAIT_CONFIRM','WAIT_REBOUND','WAIT_MARKET_ACCEPTANCE','TECHNICAL_BLOCKED')",
            ).fetchone()
            pool_count = int(row[0]) if row else 0
            row = con.execute(
                "select count(*) from strategy6_observe_pool where status in ('OBSERVING','WAIT_CONFIRM','WAIT_REBOUND','WAIT_MARKET_ACCEPTANCE','TECHNICAL_BLOCKED') and (next_check_at is null or next_check_at <= ?)",
                (now,),
            ).fetchone()
            due_count = int(row[0]) if row else 0
            doc = {
                "schema_version": SCHEMA_VERSION,
                "source": "strategy6_daemon_heartbeat",
                "status": status,
                "pid": int(pid or os.getpid()),
                "heartbeat_at": now,
                "last_check_at": last_check_at,
                "next_check_at": next_check_at,
                "pool_count": pool_count,
                "due_count": due_count,
                "last_error": last_error,
                "payload": payload or {},
            }
            con.execute(
                """
                insert or replace into strategy6_daemon_heartbeat (
                    singleton, status, pid, heartbeat_at, last_check_at, next_check_at,
                    pool_count, due_count, last_error, payload_json
                ) values ('strategy6', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    doc["status"],
                    doc["pid"],
                    doc["heartbeat_at"],
                    doc["last_check_at"],
                    doc["next_check_at"],
                    doc["pool_count"],
                    doc["due_count"],
                    doc["last_error"],
                    json.dumps(doc, ensure_ascii=False),
                ),
            )
    except sqlite3.Error:
        doc = {
            "schema_version": SCHEMA_VERSION,
            "source": "strategy6_daemon_heartbeat",
            "status": status,
            "pid": int(pid or os.getpid()),
            "heartbeat_at": now,
            "last_check_at": last_check_at,
            "next_check_at": next_check_at,
            "pool_count": pool_count,
            "due_count": due_count,
            "last_error": last_error or "sqlite_error",
            "payload": payload or {},
        }
    write_json_atomic(p.daemon_heartbeat, doc)
    write_json_atomic(p.daemon_state, doc)
    return doc


def strategy6_daemon_status(project_root: Path) -> dict[str, Any]:
    root = Path(project_root)
    p = paths(root)
    cfg = load_strategy6_config(root)
    heartbeat = _read_json(p.daemon_heartbeat)
    pid = int(heartbeat.get("pid") or 0) if heartbeat else 0
    pid_alive = _pid_is_alive(pid)
    if not heartbeat:
        heartbeat = {
            "schema_version": SCHEMA_VERSION,
            "source": "strategy6_daemon_heartbeat",
            "status": "stopped",
            "pid": None,
            "heartbeat_at": None,
            "last_check_at": None,
            "next_check_at": None,
            "pool_count": 0,
            "due_count": 0,
            "last_error": None,
        }
    heartbeat = dict(heartbeat)
    heartbeat_age = _age_sec(heartbeat.get("heartbeat_at"))
    stale_after = int(cfg.get("daemon_stale_after_sec") or max(30, int(cfg.get("observe_interval_sec") or 300) * 2))
    stale = heartbeat_age is None or heartbeat_age > stale_after
    status = str(heartbeat.get("status") or "unknown").lower()
    pool_count, due_count = _observe_pool_counts(root)
    heartbeat["pool_count"] = pool_count
    heartbeat["due_count"] = due_count
    reason_codes: list[str] = []
    if not heartbeat.get("heartbeat_at"):
        reason_codes.append("strategy6_daemon_heartbeat_missing")
    if stale:
        reason_codes.append("strategy6_daemon_heartbeat_stale")
    if pid and not pid_alive:
        reason_codes.append("strategy6_daemon_pid_dead")
    if heartbeat.get("last_error") or status == "error":
        reason_codes.append("strategy6_daemon_last_error")
    if not bool(cfg.get("daemon_watchdog_enabled", True)):
        health_status = "disabled"
        watchdog_status = "disabled"
        recommended_action = "none"
    elif status == "stopped":
        health_status = "idle_no_pool" if pool_count == 0 else "stopped_with_pool"
        watchdog_status = "idle" if pool_count == 0 else "needs_start"
        recommended_action = "none" if pool_count == 0 else "start"
    elif not heartbeat.get("heartbeat_at"):
        health_status = "missing"
        watchdog_status = "missing"
        recommended_action = "start" if pool_count or due_count else "none"
    elif pid and not pid_alive:
        health_status = "dead_pid"
        watchdog_status = "dead_pid"
        recommended_action = "start"
    elif stale:
        health_status = "stale"
        watchdog_status = "stale"
        recommended_action = "restart_recommended" if pid_alive else "start"
    elif status == "error":
        health_status = "error"
        watchdog_status = "error"
        recommended_action = "restart_recommended" if pid_alive else "start"
    else:
        health_status = "healthy"
        watchdog_status = "healthy"
        recommended_action = "none"
    heartbeat["pid_alive"] = pid_alive
    heartbeat["heartbeat_age_sec"] = heartbeat_age
    heartbeat["stale_after_sec"] = stale_after
    heartbeat["stale"] = stale
    heartbeat["health_status"] = health_status
    heartbeat["watchdog_status"] = watchdog_status
    heartbeat["watchdog_recommended_action"] = recommended_action
    heartbeat["watchdog_enabled"] = bool(cfg.get("daemon_watchdog_enabled", True))
    heartbeat["reason_codes"] = sorted(set(reason_codes))
    heartbeat["lock_path"] = str(p.daemon_lock)
    heartbeat["stop_path"] = str(p.daemon_stop)
    heartbeat["db_path"] = str(p.db)
    return heartbeat


def strategy6_watchdog(project_root: Path, *, recover: bool = False) -> dict[str, Any]:
    root = Path(project_root)
    status = strategy6_daemon_status(root)
    action = str(status.get("watchdog_recommended_action") or "none")
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "source": "strategy6_daemon_watchdog",
        "recover": bool(recover),
        "watchdog_status": status.get("watchdog_status"),
        "health_status": status.get("health_status"),
        "recommended_action": action,
        "action_taken": "none",
        "status": status,
    }
    if not status.get("watchdog_enabled"):
        result["action_taken"] = "disabled"
        _append_runtime_event(root, "watchdog_disabled", result)
        return result
    if not recover or action in {"none", "restart_recommended"}:
        if action == "restart_recommended" and recover:
            result["action_taken"] = "restart_recommended_live_pid"
        _append_runtime_event(root, "watchdog_check", result)
        return result
    if action == "start":
        from laoma_signal_engine.strategy6.daemon import start_daemon

        started = start_daemon(root)
        result["action_taken"] = "start"
        result["start_result"] = started
        result["status_after"] = strategy6_daemon_status(root)
        _append_runtime_event(root, "watchdog_recover_start", result)
        return result
    _append_runtime_event(root, "watchdog_check", result)
    return result


def run_strategy6_observe_once(
    project_root: Path,
    *,
    run_id: str | None = None,
    cycle_id: str | None = None,
    observe_limit: int | None = None,
) -> dict[str, Any]:
    root = Path(project_root)
    cfg = load_strategy6_config(root)
    now = to_iso_z(utc_now())
    run_id = run_id or f"strategy6_daemon_{now.replace(':', '').replace('-', '')}"
    cycle_id = cycle_id or f"cycle_{run_id}"
    write_daemon_heartbeat(root, status="running", last_check_at=now, payload={"phase": "observe_once_start"})
    result = write_strategy6_outputs(
        root,
        run_id=run_id,
        cycle_id=cycle_id,
        include_observe_pool=True,
        observe_limit=observe_limit or int(cfg["max_pool_size"]),
    )
    next_check = _iso_add_seconds(now, int(cfg["observe_interval_sec"]))
    hb = write_daemon_heartbeat(
        root,
        status="idle",
        last_check_at=now,
        next_check_at=next_check,
        payload={"phase": "observe_once_done", "result": result},
    )
    return {**result, "daemon": hb}
