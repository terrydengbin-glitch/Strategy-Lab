from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_v2 import _connect, _iso_from_ms, _loads, _num, ensure_p21_v2_tables

SCHEMA_VERSION = "19.38-trade-quality-entry-evidence-v4"
STRATEGY5_REF = "p21v2_72340cb432fa7977"
STRATEGY6_REF = "s6v32_edcd6b1030331422"
STRATEGY5_EVIDENCE_DB = Path("DATA/backtest/evidence/strategy5/strategy5_evidence_pack_20260612T083752Z.sqlite")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _stable_id(prefix: str, payload: Any, size: int = 22) -> str:
    return f"{prefix}_{hashlib.sha256(_json(payload).encode('utf-8')).hexdigest()[:size]}"


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _round(value: Any, digits: int = 8) -> float | None:
    out = _safe_float(value)
    return round(out, digits) if out is not None else None


def _side_sign(side: str | None) -> int:
    return -1 if str(side or "").upper() == "SHORT" else 1


def ensure_trade_quality_v4_tables(db_path: Path) -> None:
    ensure_p21_v2_tables(db_path)
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_entry_evidence_v4(
              feature_id TEXT PRIMARY KEY,
              diagnostic_id TEXT NOT NULL,
              source TEXT NOT NULL,
              package_key TEXT NOT NULL,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              order_id TEXT,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              entry_time TEXT,
              entry_time_ms INTEGER,
              lookback_start_ms INTEGER,
              lookback_end_ms INTEGER,
              known_at_entry INTEGER NOT NULL,
              source_level TEXT NOT NULL,
              feature_completeness TEXT NOT NULL,
              proxy_level TEXT NOT NULL,
              features_json TEXT NOT NULL,
              targets_json TEXT NOT NULL,
              quality_flags_json TEXT NOT NULL,
              source_payload_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(diagnostic_id, schema_version)
            );
            CREATE INDEX IF NOT EXISTS idx_tq_v4_feature_pkg
              ON trade_quality_entry_evidence_v4(package_key, strategy_line, parameter_set_id);
            CREATE INDEX IF NOT EXISTS idx_tq_v4_feature_symbol
              ON trade_quality_entry_evidence_v4(symbol, side, entry_time_ms);

            CREATE TABLE IF NOT EXISTS trade_quality_deep_root_cause_v4(
              attribution_id TEXT PRIMARY KEY,
              feature_id TEXT NOT NULL,
              diagnostic_id TEXT NOT NULL,
              package_key TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              root_cause TEXT,
              deep_subcause TEXT NOT NULL,
              subcause_family TEXT NOT NULL,
              confidence REAL NOT NULL,
              evidence_json TEXT NOT NULL,
              target_snapshot_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(diagnostic_id, schema_version)
            );
            CREATE INDEX IF NOT EXISTS idx_tq_v4_root_pkg
              ON trade_quality_deep_root_cause_v4(package_key, strategy_line, root_cause, deep_subcause);

            CREATE TABLE IF NOT EXISTS trade_quality_gate_candidates_v4(
              candidate_id TEXT PRIMARY KEY,
              package_key TEXT NOT NULL,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              gate_type TEXT NOT NULL,
              status TEXT NOT NULL,
              rule_json TEXT NOT NULL,
              feature_scope_json TEXT NOT NULL,
              metrics_before_json TEXT NOT NULL,
              metrics_after_json TEXT NOT NULL,
              evidence_json TEXT NOT NULL,
              config_patch_preview_json TEXT NOT NULL,
              leakage_check_status TEXT NOT NULL,
              overfit_risk TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(package_key, gate_type, rule_json, schema_version)
            );
            CREATE INDEX IF NOT EXISTS idx_tq_v4_gate_rank
              ON trade_quality_gate_candidates_v4(strategy_line, status, overfit_risk);
            """
        )


def _read_strategy5_rows(project_root: Path) -> list[dict[str, Any]]:
    db_path = project_root / STRATEGY5_EVIDENCE_DB
    if not db_path.exists():
        return []
    conn = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM tq_samples
            WHERE parameter_set_id = ?
            ORDER BY rowid
            """,
            (STRATEGY5_REF,),
        ).fetchall()
        return [_normalize_evidence_pack_row(dict(row)) for row in rows]
    finally:
        conn.close()


def _read_strategy6_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM backtest_trade_quality_samples
        WHERE parameter_set_id = ?
        ORDER BY entry_time_ms
        """,
        (STRATEGY6_REF,),
    ).fetchall()
    return [dict(row) for row in rows]


def _normalize_evidence_pack_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = _loads(row.get("payload_json"), {})
    out = {
        "diagnostic_id": row.get("diagnostic_id") or payload.get("diagnostic_id"),
        "source": payload.get("source") or "backtest_p21_v2_evidence_pack",
        "package_key": row.get("package_key") or payload.get("package_key"),
        "experiment_id": row.get("experiment_id") or payload.get("experiment_id"),
        "parameter_set_id": row.get("parameter_set_id") or payload.get("parameter_set_id"),
        "strategy_line": row.get("strategy_line") or payload.get("strategy_line") or "strategy5",
        "order_id": payload.get("order_id") or payload.get("trade_id"),
        "trade_id": payload.get("trade_id") or payload.get("order_id"),
        "symbol": row.get("symbol") or payload.get("symbol"),
        "side": row.get("side") or payload.get("side"),
        "entry_time": payload.get("entry_time"),
        "entry_time_ms": payload.get("entry_time_ms"),
        "exit_time": payload.get("exit_time"),
        "exit_time_ms": payload.get("exit_time_ms"),
        "entry_price": payload.get("entry_price"),
        "exit_price": payload.get("exit_price"),
        "planned_SL": payload.get("planned_SL"),
        "planned_TP": payload.get("planned_TP"),
        "planned_RR": payload.get("planned_RR"),
        "holding_minutes": payload.get("holding_minutes"),
        "net_R": row.get("net_R") if row.get("net_R") is not None else payload.get("net_R"),
        "MFE_R": row.get("MFE_R") if row.get("MFE_R") is not None else payload.get("MFE_R"),
        "MAE_R": row.get("MAE_R") if row.get("MAE_R") is not None else payload.get("MAE_R"),
        "root_cause": row.get("root_cause") or payload.get("root_cause"),
        "exit_reason": payload.get("exit_reason"),
        "evidence_json": payload.get("evidence_json") or "{}",
        "source_payload_json": payload.get("source_payload_json") or "{}",
        "schema_version": payload.get("schema_version") or "21.19-backtest-trade-quality",
    }
    return out


def _extract_nested_payload(row: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source_payload = _loads(row.get("source_payload_json"), {})
    evidence = _loads(row.get("evidence_json"), {})
    features = source_payload.get("features") if isinstance(source_payload.get("features"), dict) else {}
    trade_plan = source_payload.get("trade_plan_payload") if isinstance(source_payload.get("trade_plan_payload"), dict) else {}
    plan = {}
    plans = trade_plan.get("plans") if isinstance(trade_plan, dict) else None
    if isinstance(plans, list) and plans:
        plan = plans[0] if isinstance(plans[0], dict) else {}
    guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
    strategy_evidence = {}
    input_refs = trade_plan.get("input_refs") if isinstance(trade_plan, dict) else {}
    if isinstance(input_refs, dict):
        for key in ("strategy5_evidence", "strategy6_evidence"):
            if isinstance(input_refs.get(key), dict):
                strategy_evidence = input_refs[key]
                break
    return source_payload, {**features, **guards, **strategy_evidence}, evidence


def _fetch_klines(conn: sqlite3.Connection, symbol: str, entry_ms: int | None, minutes: int = 90) -> list[dict[str, Any]]:
    if not entry_ms:
        return []
    start = int(entry_ms) - minutes * 60_000
    rows = conn.execute(
        """
        SELECT open_time_ms, open, high, low, close, volume, quote_volume, trade_count,
               taker_buy_base_volume, taker_buy_quote_volume
        FROM p21_klines_1m
        WHERE symbol = ? AND open_time_ms >= ? AND open_time_ms <= ?
        ORDER BY open_time_ms
        """,
        (symbol, start, int(entry_ms)),
    ).fetchall()
    return [dict(row) for row in rows]


def _ema(values: list[float], period: int) -> float | None:
    if not values:
        return None
    alpha = 2 / (period + 1)
    out = values[0]
    for value in values[1:]:
        out = alpha * value + (1 - alpha) * out
    return out


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains = []
    losses = []
    for prev, cur in zip(values[-period - 1 : -1], values[-period:]):
        delta = cur - prev
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = mean(gains) if gains else 0.0
    avg_loss = mean(losses) if losses else 0.0
    if avg_loss <= 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _std(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    m = mean(values)
    return math.sqrt(sum((v - m) ** 2 for v in values) / len(values))


def _technical_features(row: dict[str, Any], extracted: dict[str, Any], klines: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    features: dict[str, Any] = {}
    quality: dict[str, Any] = {
        "kline_1m_available": bool(klines),
        "kline_window_count": len(klines),
        "missing_groups": [],
        "proxy_groups": [],
    }
    entry = _safe_float(row.get("entry_price")) or _safe_float(extracted.get("close"))
    closes = [_num(k.get("close")) for k in klines if k.get("close") is not None]
    highs = [_num(k.get("high")) for k in klines if k.get("high") is not None]
    lows = [_num(k.get("low")) for k in klines if k.get("low") is not None]
    vols = [_num(k.get("volume")) for k in klines if k.get("volume") is not None]
    for key in ("pct_1m_bps", "pct_3m_bps", "pct_5m_bps", "pct_15m_bps", "volume_z", "atr_1m_bps", "range_pos_30m"):
        if extracted.get(key) is not None:
            features[key] = _round(extracted.get(key), 8)
    if len(closes) >= 6:
        last = closes[-1]
        if len(closes) >= 2:
            features.setdefault("pct_1m_bps", _round((last / closes[-2] - 1) * 10000, 8))
        if len(closes) >= 4:
            features.setdefault("pct_3m_bps", _round((last / closes[-4] - 1) * 10000, 8))
        if len(closes) >= 6:
            features.setdefault("pct_5m_bps", _round((last / closes[-6] - 1) * 10000, 8))
        if len(closes) >= 16:
            features.setdefault("pct_15m_bps", _round((last / closes[-16] - 1) * 10000, 8))
        rsi14 = _rsi(closes, 14)
        if rsi14 is not None:
            features["rsi_14"] = _round(rsi14, 4)
        ema20 = _ema(closes[-60:], 20)
        ema60 = _ema(closes[-90:], 60)
        if ema20 and entry:
            features["ema20_distance_bps"] = _round((entry / ema20 - 1) * 10000, 8)
        if ema60 and entry:
            features["ema60_distance_bps"] = _round((entry / ema60 - 1) * 10000, 8)
        if len(closes) >= 20:
            basis = mean(closes[-20:])
            band_std = _std(closes[-20:]) or 0.0
            upper = basis + 2 * band_std
            lower = basis - 2 * band_std
            width = upper - lower
            features["bollinger_width_bps"] = _round(width / basis * 10000 if basis else None, 8)
            features["bollinger_position"] = _round((entry - lower) / width if entry and width > 0 else None, 8)
            features["vwap_distance_bps"] = _round(
                _vwap_distance_bps(klines[-20:], entry), 8
            )
        if highs and lows and len(closes) >= 2:
            tr_values = []
            for i in range(max(1, len(klines) - 14), len(klines)):
                high = _num(klines[i].get("high"))
                low = _num(klines[i].get("low"))
                prev_close = _num(klines[i - 1].get("close"))
                tr_values.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
            if tr_values and entry:
                features["atr_14_bps"] = _round(mean(tr_values) / entry * 10000, 8)
        if vols and len(vols) >= 20:
            base = vols[-21:-1]
            sd = _std(base) or 0.0
            features.setdefault("volume_z", _round((vols[-1] - mean(base)) / sd if sd > 0 else 0.0, 8))
        last_k = klines[-1] if klines else {}
        open_p = _safe_float(last_k.get("open"))
        close_p = _safe_float(last_k.get("close"))
        high_p = _safe_float(last_k.get("high"))
        low_p = _safe_float(last_k.get("low"))
        if all(v is not None for v in (open_p, close_p, high_p, low_p)) and high_p != low_p:
            body = abs(close_p - open_p)
            candle_range = high_p - low_p
            features["body_ratio_1m"] = _round(body / candle_range, 8)
            features["upper_wick_ratio_1m"] = _round((high_p - max(open_p, close_p)) / candle_range, 8)
            features["lower_wick_ratio_1m"] = _round((min(open_p, close_p) - low_p) / candle_range, 8)
    else:
        quality["missing_groups"].append("technical_1m_window")
    return features, quality


def _vwap_distance_bps(klines: list[dict[str, Any]], entry: float | None) -> float | None:
    if not entry:
        return None
    total_quote = sum(_num(k.get("quote_volume")) for k in klines)
    total_base = sum(_num(k.get("volume")) for k in klines)
    if total_base <= 0 or total_quote <= 0:
        return None
    vwap = total_quote / total_base
    return (entry / vwap - 1) * 10000 if vwap else None


def _flow_features(row: dict[str, Any], extracted: dict[str, Any], klines: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    features: dict[str, Any] = {}
    quality = {"proxy_groups": [], "missing_groups": []}
    if klines:
        buy = sum(_num(k.get("taker_buy_base_volume")) for k in klines[-5:])
        vol = sum(_num(k.get("volume")) for k in klines[-5:])
        if vol > 0:
            ratio = buy / vol
            features["taker_buy_ratio_5m"] = _round(ratio, 8)
            features["cvd_proxy_5m"] = _round((ratio - 0.5) * vol, 8)
            quality["proxy_groups"].append("cvd_from_kline_taker_buy")
        else:
            quality["missing_groups"].append("taker_volume_zero")
        last_buy = _num(klines[-1].get("taker_buy_base_volume"))
        last_vol = _num(klines[-1].get("volume"))
        if last_vol > 0:
            features["taker_buy_ratio_1m"] = _round(last_buy / last_vol, 8)
    if extracted.get("taker_buy_ratio") is not None:
        features.setdefault("taker_buy_ratio_hint", _round(extracted.get("taker_buy_ratio"), 8))
    for key in ("spread_bps", "top_depth_usdt", "depth_imbalance", "oi_change_pct", "funding_rate"):
        value = extracted.get(key)
        if value is not None:
            features[key] = _round(value, 8)
    if "spread_bps" not in features:
        liquidity = extracted.get("liquidity_gate") if isinstance(extracted.get("liquidity_gate"), dict) else {}
        if liquidity.get("spread_bps") is not None:
            features["spread_bps"] = _round(liquidity.get("spread_bps"), 8)
        if liquidity.get("top_depth_usdt") is not None:
            features["top_depth_usdt"] = _round(liquidity.get("top_depth_usdt"), 4)
    if "spread_bps" not in features:
        quality["missing_groups"].append("spread_depth")
    else:
        quality["proxy_groups"].append("spread_depth_from_plan_or_proxy")
    side_sign = _side_sign(row.get("side"))
    pct_1m = _safe_float(extracted.get("pct_1m_bps"))
    taker = _safe_float(features.get("taker_buy_ratio_1m") or features.get("taker_buy_ratio_5m") or extracted.get("taker_buy_ratio"))
    if pct_1m is not None and taker is not None:
        price_dir = 1 if pct_1m > 0 else -1 if pct_1m < 0 else 0
        flow_dir = 1 if taker > 0.54 else -1 if taker < 0.46 else 0
        features["price_flow_alignment"] = "same" if price_dir and flow_dir and price_dir == flow_dir else "opposite" if price_dir and flow_dir else "neutral"
        features["side_flow_alignment"] = "same" if flow_dir and flow_dir == side_sign else "opposite" if flow_dir else "neutral"
    return features, quality


def _build_feature_row(conn: sqlite3.Connection, sample: dict[str, Any]) -> dict[str, Any]:
    source_payload, extracted, source_evidence = _extract_nested_payload(sample)
    symbol = str(sample.get("symbol") or "").upper()
    entry_ms = int(_num(sample.get("entry_time_ms"))) if sample.get("entry_time_ms") else None
    klines = _fetch_klines(conn, symbol, entry_ms)
    technical, technical_quality = _technical_features(sample, extracted, klines)
    flow, flow_quality = _flow_features(sample, {**extracted, **source_payload}, klines)
    features = {
        **technical,
        **flow,
        "side": str(sample.get("side") or "").upper(),
        "symbol": symbol,
        "entry_hour_utc": datetime.fromtimestamp(entry_ms / 1000, timezone.utc).hour if entry_ms else None,
        "known_at_entry": True,
    }
    targets = {
        "net_R": _round(sample.get("net_R"), 8),
        "MFE_R": _round(sample.get("MFE_R"), 8),
        "MAE_R": _round(sample.get("MAE_R"), 8),
        "root_cause": sample.get("root_cause"),
        "exit_reason": sample.get("exit_reason"),
        "planned_RR": _round(sample.get("planned_RR"), 8),
        "holding_minutes": _round(sample.get("holding_minutes"), 4),
    }
    missing = list(dict.fromkeys((technical_quality.get("missing_groups") or []) + (flow_quality.get("missing_groups") or [])))
    proxy = list(dict.fromkeys((technical_quality.get("proxy_groups") or []) + (flow_quality.get("proxy_groups") or [])))
    if not missing:
        completeness = "complete"
    elif len(missing) <= 2 and (technical or flow):
        completeness = "partial"
    else:
        completeness = "sparse"
    proxy_level = "proxy_plus_kline" if proxy else "kline_only" if technical else "missing"
    feature_id = _stable_id("tqv4", [sample.get("diagnostic_id"), sample.get("package_key"), SCHEMA_VERSION])
    return {
        "feature_id": feature_id,
        "diagnostic_id": sample.get("diagnostic_id"),
        "source": sample.get("source") or "backtest_p21_v2",
        "package_key": sample.get("package_key"),
        "experiment_id": sample.get("experiment_id"),
        "parameter_set_id": sample.get("parameter_set_id"),
        "strategy_line": sample.get("strategy_line"),
        "order_id": sample.get("order_id") or sample.get("trade_id"),
        "symbol": symbol,
        "side": str(sample.get("side") or "").upper(),
        "entry_time": sample.get("entry_time") or (_iso_from_ms(entry_ms) if entry_ms else None),
        "entry_time_ms": entry_ms,
        "lookback_start_ms": entry_ms - 90 * 60_000 if entry_ms else None,
        "lookback_end_ms": entry_ms,
        "known_at_entry": 1,
        "source_level": "kline_1m_plus_proxy" if klines else "payload_proxy_only",
        "feature_completeness": completeness,
        "proxy_level": proxy_level,
        "features_json": _json(features),
        "targets_json": _json(targets),
        "quality_flags_json": _json(
            {
                "missing_groups": missing,
                "proxy_groups": proxy,
                "kline_window_count": len(klines),
                "source_evidence": source_evidence,
                "no_lookahead": True,
            }
        ),
        "source_payload_json": _json(source_payload),
        "schema_version": SCHEMA_VERSION,
        "generated_at": _now(),
    }


def _classify_deep_root_cause(feature_row: dict[str, Any]) -> dict[str, Any]:
    features = _loads(feature_row.get("features_json"), {})
    targets = _loads(feature_row.get("targets_json"), {})
    root = str(targets.get("root_cause") or "unknown")
    side = str(feature_row.get("side") or "").upper()
    side_sign = _side_sign(side)
    pct_1m = _safe_float(features.get("pct_1m_bps"))
    pct_3m = _safe_float(features.get("pct_3m_bps"))
    rsi = _safe_float(features.get("rsi_14"))
    boll = _safe_float(features.get("bollinger_position"))
    ema20_dist = _safe_float(features.get("ema20_distance_bps"))
    vwap_dist = _safe_float(features.get("vwap_distance_bps"))
    spread = _safe_float(features.get("spread_bps"))
    taker = _safe_float(features.get("taker_buy_ratio_5m") or features.get("taker_buy_ratio_hint"))
    mfe = _safe_float(targets.get("MFE_R"))
    mae = _safe_float(targets.get("MAE_R"))
    planned_rr = _safe_float(targets.get("planned_RR"))
    evidence: dict[str, Any] = {}
    subcause = "unclassified"
    family = "unknown"
    confidence = 0.45
    if root == "direction_wrong":
        family = "direction"
        if pct_1m is not None and pct_1m * side_sign < -8:
            subcause, confidence = "immediate_price_reversal_1m", 0.82
        elif taker is not None and ((side == "LONG" and taker < 0.46) or (side == "SHORT" and taker > 0.54)):
            subcause, confidence = "aggressive_flow_against_side_proxy", 0.72
        elif pct_3m is not None and pct_3m * side_sign < -18:
            subcause, confidence = "short_window_momentum_against_side", 0.68
        else:
            subcause, confidence = "direction_context_weak_or_missing", 0.5
    elif root == "entered_too_early":
        family = "entry_timing"
        dist = max(abs(ema20_dist or 0.0), abs(vwap_dist or 0.0))
        if mae is not None and mfe is not None and mae > 0.6 and mfe > 0.8:
            subcause, confidence = "adverse_excursion_before_favorable_move", 0.86
        elif dist > 60:
            subcause, confidence = "entry_far_from_mean", 0.72
        elif (side == "LONG" and boll is not None and boll > 0.9) or (side == "SHORT" and boll is not None and boll < 0.1):
            subcause, confidence = "entered_at_bollinger_extreme", 0.7
        else:
            subcause, confidence = "entry_timing_unconfirmed", 0.52
    elif root == "tp_too_far":
        family = "target_realism"
        if planned_rr is not None and mfe is not None and mfe < planned_rr * 0.5:
            subcause, confidence = "mfe_far_below_planned_rr", 0.84
        elif spread is not None and planned_rr is not None and spread > 5:
            subcause, confidence = "cost_or_spread_reduces_reward_realism", 0.66
        elif rsi is not None and ((side == "LONG" and rsi > 72) or (side == "SHORT" and rsi < 28)):
            subcause, confidence = "target_set_after_overextended_impulse", 0.64
        else:
            subcause, confidence = "target_unrealistic_for_observed_mfe", 0.58
    elif _safe_float(targets.get("net_R")) is not None and _safe_float(targets.get("net_R")) > 0:
        family = "positive_reference"
        subcause, confidence = "profitable_reference_pattern", 0.6
    else:
        family = "other"
        subcause, confidence = f"{root}_needs_more_evidence", 0.48
    evidence.update(
        {
            "pct_1m_bps": pct_1m,
            "pct_3m_bps": pct_3m,
            "rsi_14": rsi,
            "bollinger_position": boll,
            "ema20_distance_bps": ema20_dist,
            "vwap_distance_bps": vwap_dist,
            "taker_buy_ratio": taker,
            "spread_bps": spread,
            "feature_completeness": feature_row.get("feature_completeness"),
            "targets_used_for_diagnosis_only": ["net_R", "MFE_R", "MAE_R", "root_cause", "exit_reason"],
        }
    )
    return {
        "attribution_id": _stable_id("tqv4rc", [feature_row.get("feature_id"), root, subcause]),
        "feature_id": feature_row.get("feature_id"),
        "diagnostic_id": feature_row.get("diagnostic_id"),
        "package_key": feature_row.get("package_key"),
        "strategy_line": feature_row.get("strategy_line"),
        "parameter_set_id": feature_row.get("parameter_set_id"),
        "symbol": feature_row.get("symbol"),
        "side": feature_row.get("side"),
        "root_cause": root,
        "deep_subcause": subcause,
        "subcause_family": family,
        "confidence": confidence,
        "evidence_json": _json(evidence),
        "target_snapshot_json": _json(targets),
        "schema_version": SCHEMA_VERSION,
        "generated_at": _now(),
    }


def materialize_v4_payload(project_root: Path, *, strategies: list[str] | None = None, limit: int | None = None) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v4_tables(db_path)
    wanted = set(strategies or ["strategy5", "strategy6"])
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        samples: list[dict[str, Any]] = []
        if "strategy5" in wanted:
            samples.extend(_read_strategy5_rows(project_root))
        if "strategy6" in wanted:
            samples.extend(_read_strategy6_rows(conn))
        if limit:
            samples = samples[: max(0, limit)]
        feature_rows = [_build_feature_row(conn, row) for row in samples]
        root_rows = [_classify_deep_root_cause(row) for row in feature_rows]
        for row in feature_rows:
            conn.execute(
                """
                INSERT OR REPLACE INTO trade_quality_entry_evidence_v4
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["feature_id"],
                    row["diagnostic_id"],
                    row["source"],
                    row["package_key"],
                    row["experiment_id"],
                    row["parameter_set_id"],
                    row["strategy_line"],
                    row["order_id"],
                    row["symbol"],
                    row["side"],
                    row["entry_time"],
                    row["entry_time_ms"],
                    row["lookback_start_ms"],
                    row["lookback_end_ms"],
                    row["known_at_entry"],
                    row["source_level"],
                    row["feature_completeness"],
                    row["proxy_level"],
                    row["features_json"],
                    row["targets_json"],
                    row["quality_flags_json"],
                    row["source_payload_json"],
                    row["schema_version"],
                    row["generated_at"],
                ),
            )
        for row in root_rows:
            conn.execute(
                """
                INSERT OR REPLACE INTO trade_quality_deep_root_cause_v4
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["attribution_id"],
                    row["feature_id"],
                    row["diagnostic_id"],
                    row["package_key"],
                    row["strategy_line"],
                    row["parameter_set_id"],
                    row["symbol"],
                    row["side"],
                    row["root_cause"],
                    row["deep_subcause"],
                    row["subcause_family"],
                    row["confidence"],
                    row["evidence_json"],
                    row["target_snapshot_json"],
                    row["schema_version"],
                    row["generated_at"],
                ),
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "materialized_features": len(feature_rows),
        "materialized_deep_root_causes": len(root_rows),
        "strategies": sorted(wanted),
        "generated_at": _now(),
    }


def _pf(values: list[float]) -> float | None:
    gains = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    if losses <= 0:
        return None if gains <= 0 else 999.0
    return gains / losses


def generate_gate_candidates_v4_payload(
    project_root: Path,
    *,
    strategy_line: str | None = None,
    min_samples: int = 50,
    limit: int = 80,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v4_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        clauses = ["1=1"]
        params: list[Any] = []
        if strategy_line and strategy_line != "all":
            clauses.append("f.strategy_line = ?")
            params.append(strategy_line)
        rows = conn.execute(
            f"""
            SELECT f.*, r.deep_subcause, r.subcause_family
            FROM trade_quality_entry_evidence_v4 f
            LEFT JOIN trade_quality_deep_root_cause_v4 r ON r.feature_id = f.feature_id
            WHERE {' AND '.join(clauses)}
            """,
            params,
        ).fetchall()
        samples = [dict(row) for row in rows]
        candidates = _candidate_rows(samples, min_samples=min_samples, limit=limit)
        if strategy_line and strategy_line != "all":
            conn.execute(
                "DELETE FROM trade_quality_gate_candidates_v4 WHERE strategy_line = ? AND schema_version = ?",
                (strategy_line, SCHEMA_VERSION),
            )
        else:
            conn.execute("DELETE FROM trade_quality_gate_candidates_v4 WHERE schema_version = ?", (SCHEMA_VERSION,))
        for row in candidates:
            conn.execute(
                """
                INSERT OR REPLACE INTO trade_quality_gate_candidates_v4
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["candidate_id"],
                    row["package_key"],
                    row["experiment_id"],
                    row["parameter_set_id"],
                    row["strategy_line"],
                    row["gate_type"],
                    row["status"],
                    row["rule_json"],
                    row["feature_scope_json"],
                    row["metrics_before_json"],
                    row["metrics_after_json"],
                    row["evidence_json"],
                    row["config_patch_preview_json"],
                    row["leakage_check_status"],
                    row["overfit_risk"],
                    row["schema_version"],
                    row["generated_at"],
                ),
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "candidate_count": len(candidates),
        "candidates": candidates[: min(limit, 20)],
        "generated_at": _now(),
    }


def _candidate_rows(samples: list[dict[str, Any]], *, min_samples: int, limit: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str, str], list[float]] = defaultdict(list)
    meta: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    package_values: dict[str, list[float]] = defaultdict(list)
    for row in samples:
        targets = _loads(row.get("targets_json"), {})
        features = _loads(row.get("features_json"), {})
        net_r = _safe_float(targets.get("net_R"))
        if net_r is None:
            continue
        package = str(row.get("package_key") or "")
        line = str(row.get("strategy_line") or "")
        param = str(row.get("parameter_set_id") or "")
        experiment = str(row.get("experiment_id") or "")
        package_values[package].append(net_r)
        buckets = {
            "symbol": row.get("symbol"),
            "entry_hour_utc": features.get("entry_hour_utc"),
            "side": row.get("side"),
        }
        rsi = _safe_float(features.get("rsi_14"))
        if rsi is not None:
            buckets["rsi_bucket"] = "rsi_high" if rsi >= 70 else "rsi_low" if rsi <= 30 else "rsi_mid"
        boll = _safe_float(features.get("bollinger_position"))
        if boll is not None:
            buckets["bollinger_bucket"] = "boll_high" if boll >= 0.85 else "boll_low" if boll <= 0.15 else "boll_mid"
        spread = _safe_float(features.get("spread_bps"))
        if spread is not None:
            buckets["spread_bucket"] = "spread_high" if spread >= 5 else "spread_normal"
        for dim, bucket in buckets.items():
            if bucket in (None, "", "unknown"):
                continue
            key = (package, line, param, dim, str(bucket))
            grouped[key].append(net_r)
            meta[key] = {"experiment_id": experiment}
    output = []
    for (package, line, param, dim, bucket), values in grouped.items():
        if len(values) < min_samples:
            continue
        if package_values.get(package) and len(values) / len(package_values[package]) > 0.75:
            continue
        avg_r = mean(values)
        total_r = sum(values)
        pf_before = _pf(package_values.get(package, []))
        outside = [
            _safe_float(_loads(r.get("targets_json"), {}).get("net_R")) or 0.0
            for r in samples
            if r.get("package_key") == package and _entry_known_bucket_value(r, dim) != bucket
        ]
        if avg_r >= -0.05 or total_r >= 0:
            continue
        pf_after = _pf(outside)
        rule = {"action": "shadow_block_or_downweight", "dimension": dim, "bucket": bucket}
        candidate_id = _stable_id("tqv4gate", [package, line, param, rule])
        output.append(
            {
                "candidate_id": candidate_id,
                "package_key": package,
                "experiment_id": meta[(package, line, param, dim, bucket)]["experiment_id"],
                "parameter_set_id": param,
                "strategy_line": line,
                "gate_type": f"{dim}_shadow_gate",
                "status": "shadow",
                "rule_json": _json(rule),
                "feature_scope_json": _json({"entry_known_only": True, "dimension": dim, "uses_targets": False}),
                "metrics_before_json": _json({"sample_count": len(values), "avg_R": avg_r, "total_R": total_r, "pf_before": pf_before}),
                "metrics_after_json": _json({"remaining_sample_count": len(outside), "pf_after_probe": pf_after}),
                "evidence_json": _json({"reason": "negative entry-known bucket", "blocked_bucket_count": len(values)}),
                "config_patch_preview_json": _json({"trade_quality_gate": {"mode": "shadow", "rules": [rule]}}),
                "leakage_check_status": "pass_entry_known_rule_only",
                "overfit_risk": "high_single_sample_probe",
                "schema_version": SCHEMA_VERSION,
                "generated_at": _now(),
            }
        )
    output.sort(key=lambda row: (_loads(row["metrics_before_json"], {}).get("total_R", 0), -_loads(row["metrics_before_json"], {}).get("sample_count", 0)))
    return output[:limit]


def _entry_known_bucket_value(row: dict[str, Any], dim: str) -> str | None:
    features = _loads(row.get("features_json"), {})
    if dim in ("symbol", "side"):
        return str(row.get(dim) or "") or None
    if dim == "entry_hour_utc":
        value = features.get("entry_hour_utc")
        return str(value) if value is not None else None
    if dim == "rsi_bucket":
        rsi = _safe_float(features.get("rsi_14"))
        if rsi is None:
            return None
        return "rsi_high" if rsi >= 70 else "rsi_low" if rsi <= 30 else "rsi_mid"
    if dim == "bollinger_bucket":
        boll = _safe_float(features.get("bollinger_position"))
        if boll is None:
            return None
        return "boll_high" if boll >= 0.85 else "boll_low" if boll <= 0.15 else "boll_mid"
    if dim == "spread_bucket":
        spread = _safe_float(features.get("spread_bps"))
        if spread is None:
            return None
        return "spread_high" if spread >= 5 else "spread_normal"
    return None


def evidence_payload(project_root: Path, *, strategy_line: str | None = None, parameter_set_id: str | None = None, limit: int = 200) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v4_tables(db_path)
    clauses = ["1=1"]
    params: list[Any] = []
    if strategy_line and strategy_line != "all":
        clauses.append("strategy_line = ?")
        params.append(strategy_line)
    if parameter_set_id and parameter_set_id != "all":
        clauses.append("parameter_set_id = ?")
        params.append(parameter_set_id)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total = conn.execute(f"SELECT COUNT(*) FROM trade_quality_entry_evidence_v4 WHERE {' AND '.join(clauses)}", params).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT *
            FROM trade_quality_entry_evidence_v4
            WHERE {' AND '.join(clauses)}
            ORDER BY generated_at DESC, entry_time_ms DESC
            LIMIT ?
            """,
            [*params, max(1, min(1000, limit))],
        ).fetchall()
        counts = conn.execute(
            f"""
            SELECT strategy_line, feature_completeness, proxy_level, COUNT(*) AS count
            FROM trade_quality_entry_evidence_v4
            WHERE {' AND '.join(clauses)}
            GROUP BY strategy_line, feature_completeness, proxy_level
            """,
            params,
        ).fetchall()
    return {
        "schema_version": SCHEMA_VERSION,
        "total": total,
        "rows": [_decode_v4_row(dict(row)) for row in rows],
        "coverage": [dict(row) for row in counts],
    }


def deep_root_payload(project_root: Path, *, strategy_line: str | None = None, parameter_set_id: str | None = None, limit: int = 200) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v4_tables(db_path)
    clauses = ["1=1"]
    params: list[Any] = []
    if strategy_line and strategy_line != "all":
        clauses.append("strategy_line = ?")
        params.append(strategy_line)
    if parameter_set_id and parameter_set_id != "all":
        clauses.append("parameter_set_id = ?")
        params.append(parameter_set_id)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rollups = conn.execute(
            f"""
            SELECT strategy_line, root_cause, deep_subcause, subcause_family,
                   COUNT(*) AS sample_count,
                   AVG(confidence) AS avg_confidence
            FROM trade_quality_deep_root_cause_v4
            WHERE {' AND '.join(clauses)}
            GROUP BY strategy_line, root_cause, deep_subcause, subcause_family
            ORDER BY sample_count DESC
            LIMIT ?
            """,
            [*params, max(1, min(500, limit))],
        ).fetchall()
        rows = conn.execute(
            f"""
            SELECT *
            FROM trade_quality_deep_root_cause_v4
            WHERE {' AND '.join(clauses)}
            ORDER BY generated_at DESC
            LIMIT ?
            """,
            [*params, max(1, min(1000, limit))],
        ).fetchall()
    return {
        "schema_version": SCHEMA_VERSION,
        "rollups": [dict(row) for row in rollups],
        "rows": [_decode_v4_row(dict(row)) for row in rows],
    }


def gate_candidates_payload(project_root: Path, *, strategy_line: str | None = None, parameter_set_id: str | None = None, limit: int = 200) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v4_tables(db_path)
    clauses = ["1=1"]
    params: list[Any] = []
    if strategy_line and strategy_line != "all":
        clauses.append("strategy_line = ?")
        params.append(strategy_line)
    if parameter_set_id and parameter_set_id != "all":
        clauses.append("parameter_set_id = ?")
        params.append(parameter_set_id)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT *
            FROM trade_quality_gate_candidates_v4
            WHERE {' AND '.join(clauses)}
            ORDER BY generated_at DESC
            LIMIT ?
            """,
            [*params, max(1, min(1000, limit))],
        ).fetchall()
    return {"schema_version": SCHEMA_VERSION, "candidates": [_decode_v4_row(dict(row)) for row in rows]}


def summary_payload(project_root: Path) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v4_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        feature_count = conn.execute("SELECT COUNT(*) FROM trade_quality_entry_evidence_v4").fetchone()[0]
        root_count = conn.execute("SELECT COUNT(*) FROM trade_quality_deep_root_cause_v4").fetchone()[0]
        gate_count = conn.execute("SELECT COUNT(*) FROM trade_quality_gate_candidates_v4").fetchone()[0]
        by_line = conn.execute(
            """
            SELECT strategy_line, COUNT(*) AS feature_count
            FROM trade_quality_entry_evidence_v4
            GROUP BY strategy_line
            ORDER BY feature_count DESC
            """
        ).fetchall()
        top_subcauses = conn.execute(
            """
            SELECT strategy_line, deep_subcause, COUNT(*) AS sample_count
            FROM trade_quality_deep_root_cause_v4
            GROUP BY strategy_line, deep_subcause
            ORDER BY sample_count DESC
            LIMIT 20
            """
        ).fetchall()
    return {
        "schema_version": SCHEMA_VERSION,
        "feature_count": feature_count,
        "deep_root_count": root_count,
        "gate_candidate_count": gate_count,
        "by_strategy_line": [dict(row) for row in by_line],
        "top_subcauses": [dict(row) for row in top_subcauses],
    }


def _decode_v4_row(row: dict[str, Any]) -> dict[str, Any]:
    for key in (
        "features_json",
        "targets_json",
        "quality_flags_json",
        "source_payload_json",
        "evidence_json",
        "target_snapshot_json",
        "rule_json",
        "feature_scope_json",
        "metrics_before_json",
        "metrics_after_json",
        "config_patch_preview_json",
    ):
        if key in row:
            row[key.replace("_json", "")] = _loads(row.get(key), {})
    return row


def reanalysis_markdown(project_root: Path) -> str:
    summary = summary_payload(project_root)
    roots = deep_root_payload(project_root, limit=80)
    gates = gate_candidates_payload(project_root, limit=50)
    lines = [
        "# STEP7.121 Strategy5/6 Deep TQ Reanalysis After V4 Upgrade",
        f"> generated_at: {_now()}",
        "",
        "## Summary",
        "",
        f"- V4 feature rows: {summary['feature_count']}",
        f"- V4 deep root rows: {summary['deep_root_count']}",
        f"- V4 shadow gate candidates: {summary['gate_candidate_count']}",
        "- Boundary: read-only analysis; no live config, paper ledger, strategy code, or sandbox code was mutated.",
        "",
        "## Strategy Coverage",
    ]
    for row in summary.get("by_strategy_line", []):
        lines.append(f"- {row['strategy_line']}: {row['feature_count']} V4 rows")
    lines.extend(["", "## Top Deep Subcauses"])
    for row in roots.get("rollups", [])[:25]:
        lines.append(
            f"- {row['strategy_line']} / {row['root_cause']} / {row['deep_subcause']}: "
            f"{row['sample_count']} samples, confidence {float(row['avg_confidence'] or 0):.2f}"
        )
    lines.extend(["", "## Shadow Gate Candidates"])
    for row in gates.get("candidates", [])[:20]:
        before = row.get("metrics_before") or {}
        after = row.get("metrics_after") or {}
        rule = row.get("rule") or {}
        lines.append(
            f"- {row['strategy_line']} {row['gate_type']} {rule}: "
            f"samples={before.get('sample_count')} avg_R={float(before.get('avg_R') or 0):.3f} "
            f"pf_before={before.get('pf_before')} pf_after_probe={after.get('pf_after_probe')} "
            f"risk={row.get('overfit_risk')}"
        )
    lines.extend(
        [
            "",
            "## Leakage Guard",
            "",
            "- Entry evidence features are marked `known_at_entry=true`.",
            "- `net_R`, `MFE_R`, `MAE_R`, `root_cause`, and `exit_reason` remain diagnostic targets only.",
            "- Gate candidates are `shadow` and use entry-known bucket rules; promotion requires later holdout / paper-shadow validation.",
        ]
    )
    return "\n".join(lines) + "\n"
