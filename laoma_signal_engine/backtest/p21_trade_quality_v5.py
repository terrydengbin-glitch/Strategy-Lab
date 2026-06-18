from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from itertools import combinations
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_trade_quality_v4 import materialize_v4_payload
from laoma_signal_engine.backtest.p21_v2 import _connect, _loads

SCHEMA_VERSION = "19.43-trade-quality-causal-factors-v5"
GATE_SCHEMA_VERSION = "21.67-trade-quality-v5-gate-factor-search"
COMBO_GATE_SCHEMA_VERSION = "21.69-trade-quality-v5-combo-gate-holdout"

ENTRY_KNOWN_RULE_FIELDS = {
    "strategy_line",
    "symbol",
    "side",
    "entry_hour_utc",
    "entry_session",
    "rsi_bucket",
    "bollinger_bucket",
    "spread_bucket",
    "volume_z_bucket",
    "entry_price_context",
    "atr_bucket",
    "wick_profile",
    "side_flow_alignment",
    "price_flow_alignment",
    "cvd_proxy_state",
    "ofi_proxy_state",
    "btc_trend",
    "btc_volatility",
    "btc_alignment",
    "market_breadth",
    "funding_bucket",
    "funding_crowded_side",
    "oi_state",
    "oi_change_bucket",
}

TARGET_ONLY_FIELDS = {
    "net_R",
    "MFE_R",
    "MAE_R",
    "gross_pnl",
    "fee",
    "exit_reason",
    "root_cause",
    "deep_subcause",
    "direction_factor_v5",
    "entry_timing_factor_v5",
    "tp_realism_factor_v5",
    "profit_factor_v5",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _json_or_none(data: Any) -> str | None:
    if data in (None, "", "null"):
        return None
    try:
        return _json(data)
    except Exception:
        return None


def _stable_id(prefix: str, payload: Any, size: int = 22) -> str:
    return f"{prefix}_{hashlib.sha256(_json(payload).encode('utf-8')).hexdigest()[:size]}"


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, "", "unknown"):
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _side_sign(side: str | None) -> int:
    return -1 if str(side or "").upper() == "SHORT" else 1


def _bucket_session(hour: Any) -> str | None:
    try:
        h = int(hour)
    except Exception:
        return None
    if 0 <= h < 8:
        return "asia_early"
    if 8 <= h < 16:
        return "eu_us_overlap"
    return "us_late"


def _bucket_rsi(value: Any) -> str | None:
    v = _safe_float(value)
    if v is None:
        return None
    if v >= 70:
        return "rsi_high"
    if v <= 30:
        return "rsi_low"
    return "rsi_mid"


def _bucket_boll(value: Any) -> str | None:
    v = _safe_float(value)
    if v is None:
        return None
    if v >= 0.85:
        return "boll_high"
    if v <= 0.15:
        return "boll_low"
    return "boll_mid"


def _bucket_spread(value: Any) -> str | None:
    v = _safe_float(value)
    if v is None:
        return None
    if v >= 8:
        return "spread_very_high"
    if v >= 5:
        return "spread_high"
    if v >= 2:
        return "spread_mid"
    return "spread_low"


def _bucket_volume_z(value: Any) -> str | None:
    v = _safe_float(value)
    if v is None:
        return None
    if v >= 6:
        return "volume_extreme"
    if v >= 3:
        return "volume_high"
    if v >= 1:
        return "volume_normal"
    return "volume_low"


def _bucket_atr(value: Any) -> str | None:
    v = _safe_float(value)
    if v is None:
        return None
    if v >= 120:
        return "atr_extreme"
    if v >= 70:
        return "atr_high"
    if v >= 30:
        return "atr_mid"
    return "atr_low"


def _bucket_oi_change(value: Any) -> str | None:
    v = _safe_float(value)
    if v is None:
        return None
    if v >= 0.05:
        return "oi_up_strong"
    if v >= 0.01:
        return "oi_up"
    if v <= -0.05:
        return "oi_down_strong"
    if v <= -0.01:
        return "oi_down"
    return "oi_flat"


def _entry_price_context(features: dict[str, Any]) -> str | None:
    vwap = _safe_float(features.get("vwap_distance_bps"))
    ema = _safe_float(features.get("ema20_distance_bps"))
    value = vwap if vwap is not None else ema
    if value is None:
        return None
    av = abs(value)
    if av >= 120:
        return "far_from_mean"
    if av >= 50:
        return "extended_from_mean"
    return "near_mean"


def _wick_profile(features: dict[str, Any], side: str) -> str | None:
    upper = _safe_float(features.get("upper_wick_ratio_1m"))
    lower = _safe_float(features.get("lower_wick_ratio_1m"))
    body = _safe_float(features.get("body_ratio_1m"))
    if upper is None and lower is None:
        return None
    if side.upper() == "LONG" and upper is not None and upper >= 0.45:
        return "long_upper_rejection"
    if side.upper() == "SHORT" and lower is not None and lower >= 0.45:
        return "short_lower_rejection"
    if body is not None and body >= 0.65:
        return "body_dominant"
    return "balanced_wick"


def _flow_state(value: Any) -> str | None:
    if value in (None, "", "unknown"):
        return None
    return str(value)


def _pf(values: list[float]) -> float | None:
    gross_profit = sum(v for v in values if v > 0)
    gross_loss = abs(sum(v for v in values if v < 0))
    if gross_loss <= 0:
        return None if gross_profit <= 0 else 999.0
    return gross_profit / gross_loss


def _max_drawdown(values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def ensure_trade_quality_v5_tables(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_causal_factors_v5(
              causal_id TEXT PRIMARY KEY,
              feature_id TEXT NOT NULL,
              diagnostic_id TEXT NOT NULL,
              sample_id TEXT,
              source_type TEXT NOT NULL,
              package_key TEXT NOT NULL,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              entry_time_ms INTEGER,
              root_cause TEXT,
              deep_subcause_v4 TEXT,
              direction_factor_v5 TEXT,
              entry_timing_factor_v5 TEXT,
              tp_realism_factor_v5 TEXT,
              profit_factor_v5 TEXT,
              market_regime_factor_v5 TEXT,
              liquidity_cost_factor_v5 TEXT,
              confidence_v5 REAL NOT NULL,
              entry_known_feature_set_json TEXT NOT NULL,
              target_diagnostic_set_json TEXT NOT NULL,
              factor_evidence_json TEXT NOT NULL,
              source_quality_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(feature_id, schema_version)
            );
            CREATE INDEX IF NOT EXISTS idx_tq_v5_factor_pkg
              ON trade_quality_causal_factors_v5(strategy_line, parameter_set_id, root_cause);
            CREATE INDEX IF NOT EXISTS idx_tq_v5_factor_symbol
              ON trade_quality_causal_factors_v5(symbol, side, entry_time_ms);

            CREATE TABLE IF NOT EXISTS trade_quality_gate_validations_v5(
              validation_id TEXT PRIMARY KEY,
              package_key TEXT NOT NULL,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              status TEXT NOT NULL,
              rule_json TEXT NOT NULL,
              feature_scope_json TEXT NOT NULL,
              split_metrics_json TEXT NOT NULL,
              aggregate_metrics_json TEXT NOT NULL,
              factor_explanation_json TEXT NOT NULL,
              config_patch_preview_json TEXT NOT NULL,
              leakage_check_status TEXT NOT NULL,
              overfit_risk TEXT NOT NULL,
              recommendation TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(package_key, parameter_set_id, strategy_line, rule_json, schema_version)
            );
            CREATE INDEX IF NOT EXISTS idx_tq_v5_gate_rank
              ON trade_quality_gate_validations_v5(strategy_line, status, overfit_risk);

            CREATE TABLE IF NOT EXISTS trade_quality_combo_gate_validations_v5(
              validation_id TEXT PRIMARY KEY,
              package_key TEXT NOT NULL,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              status TEXT NOT NULL,
              combo_size INTEGER NOT NULL,
              rule_json TEXT NOT NULL,
              feature_scope_json TEXT NOT NULL,
              split_metrics_json TEXT NOT NULL,
              aggregate_metrics_json TEXT NOT NULL,
              factor_explanation_json TEXT NOT NULL,
              config_patch_preview_json TEXT NOT NULL,
              leakage_check_status TEXT NOT NULL,
              overfit_risk TEXT NOT NULL,
              recommendation TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(package_key, parameter_set_id, strategy_line, rule_json, schema_version)
            );
            CREATE INDEX IF NOT EXISTS idx_tq_v5_combo_gate_rank
              ON trade_quality_combo_gate_validations_v5(strategy_line, combo_size, overfit_risk, recommendation);
            CREATE INDEX IF NOT EXISTS idx_p24_entry_features_tq_v5_lookup
              ON research_entry_features(parameter_set_id, strategy_line, symbol, side, entry_time_ms, generated_at);
            """
        )


def _latest_p24_feature(conn: sqlite3.Connection, row: sqlite3.Row) -> tuple[dict[str, Any], dict[str, Any]]:
    params = (
        row["parameter_set_id"],
        row["strategy_line"],
        row["symbol"],
        row["side"],
        row["entry_time_ms"],
    )
    match = conn.execute(
        """
        SELECT features_json, feature_completeness, proxy_level, missing_fields_json, source_ref_json
        FROM research_entry_features
        WHERE parameter_set_id = ?
          AND strategy_line = ?
          AND symbol = ?
          AND side = ?
          AND entry_time_ms = ?
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        params,
    ).fetchone()
    if not match:
        return {}, {"p24_match": "missing"}
    return _loads(match["features_json"], {}), {
        "p24_match": "observed",
        "feature_completeness": match["feature_completeness"],
        "proxy_level": match["proxy_level"],
        "missing_fields": _loads(match["missing_fields_json"], []),
        "source_ref": _loads(match["source_ref_json"], {}),
    }


def _gate_buckets(features: dict[str, Any], row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    side = str(row["side"] if isinstance(row, sqlite3.Row) else row.get("side") or "").upper()
    hour = features.get("entry_hour_utc")
    return {
        "strategy_line": row["strategy_line"] if isinstance(row, sqlite3.Row) else row.get("strategy_line"),
        "symbol": row["symbol"] if isinstance(row, sqlite3.Row) else row.get("symbol"),
        "side": side,
        "entry_hour_utc": hour,
        "entry_session": _bucket_session(hour),
        "rsi_bucket": _bucket_rsi(features.get("rsi_14")),
        "bollinger_bucket": _bucket_boll(features.get("bollinger_position")),
        "spread_bucket": _bucket_spread(features.get("spread_bps")),
        "volume_z_bucket": _bucket_volume_z(features.get("volume_z")),
        "entry_price_context": _entry_price_context(features),
        "atr_bucket": _bucket_atr(features.get("atr_14_bps")),
        "wick_profile": _wick_profile(features, side),
        "side_flow_alignment": _flow_state(features.get("side_flow_alignment")),
        "price_flow_alignment": _flow_state(features.get("price_flow_alignment")),
        "cvd_proxy_state": _flow_state(features.get("cvd_proxy_state")),
        "ofi_proxy_state": _flow_state(features.get("ofi_proxy_state")),
        "btc_trend": _flow_state(features.get("btc_trend")),
        "btc_volatility": _flow_state(features.get("btc_volatility")),
        "btc_alignment": _flow_state(features.get("btc_alignment")),
        "market_breadth": _flow_state(features.get("market_breadth")),
        "funding_bucket": _flow_state(features.get("funding_bucket")),
        "funding_crowded_side": _flow_state(features.get("funding_crowded_side")),
        "oi_state": _flow_state(features.get("oi_state")),
        "oi_change_bucket": _bucket_oi_change(features.get("oi_change")),
    }


def _classify_causal_factor(row: sqlite3.Row, features: dict[str, Any], targets: dict[str, Any], p24_quality: dict[str, Any]) -> dict[str, Any]:
    side = str(row["side"] or "").upper()
    sign = _side_sign(side)
    root = str(targets.get("root_cause") or row["root_cause"] or "unknown")
    deep_v4 = str(row["deep_subcause"] or "unknown")
    net_r = _safe_float(targets.get("net_R"))
    mfe = _safe_float(targets.get("MFE_R"))
    mae = _safe_float(targets.get("MAE_R"))
    planned_rr = _safe_float(targets.get("planned_RR"))
    pct_1m = _safe_float(features.get("pct_1m_bps"))
    pct_3m = _safe_float(features.get("pct_3m_bps"))
    volume_z = _safe_float(features.get("volume_z"))
    boll = _safe_float(features.get("bollinger_position"))
    ema_dist = _safe_float(features.get("ema20_distance_bps"))
    vwap_dist = _safe_float(features.get("vwap_distance_bps"))
    atr = _safe_float(features.get("atr_14_bps"))
    spread = _safe_float(features.get("spread_bps"))
    taker = _safe_float(features.get("taker_buy_ratio_5m") or features.get("taker_buy_ratio_1m"))
    side_flow = features.get("side_flow_alignment")
    price_flow = features.get("price_flow_alignment")
    btc_alignment = features.get("btc_alignment")
    btc_trend = features.get("btc_trend")
    btc_vol = features.get("btc_volatility")
    breadth = features.get("market_breadth")
    funding_bucket = features.get("funding_bucket")
    funding_crowded_side = features.get("funding_crowded_side")
    oi_state = features.get("oi_state")
    oi_change = _safe_float(features.get("oi_change"))
    upper_wick = _safe_float(features.get("upper_wick_ratio_1m"))
    lower_wick = _safe_float(features.get("lower_wick_ratio_1m"))
    dist = max(abs(ema_dist or 0.0), abs(vwap_dist or 0.0))

    direction = "direction_not_primary"
    entry_timing = "entry_timing_not_primary"
    tp_realism = "tp_not_primary"
    profit = "not_profitable"
    market_regime = "market_regime_unclassified"
    liquidity = "liquidity_cost_unclassified"
    confidence = 0.5

    is_loss = net_r is not None and net_r <= 0
    is_win = net_r is not None and net_r > 0
    if root == "direction_wrong" or (is_loss and mfe is not None and mfe < 0.2):
        if side_flow == "opposite" or price_flow == "opposite":
            direction, confidence = "direction_flow_against", 0.82
        elif btc_alignment == "opposite":
            direction, confidence = "direction_btc_against", 0.75
        elif breadth in {"down", "bearish"} and side == "LONG" or breadth in {"up", "bullish"} and side == "SHORT":
            direction, confidence = "direction_market_breadth_against", 0.72
        elif oi_state in {"price_up_oi_down_short_covering", "price_down_oi_down_long_liquidation"}:
            direction, confidence = "direction_oi_not_confirming", 0.68
        elif funding_bucket in {"OVERHEATED", "POSITIVE_EXTREME", "NEGATIVE_EXTREME"}:
            direction, confidence = "direction_funding_crowded_reversal", 0.66
        elif pct_1m is not None and pct_1m * sign < -8:
            direction, confidence = "direction_immediate_reversal_1m", 0.8
        elif dist >= 80:
            direction, confidence = "direction_overextended_reversal", 0.64
        else:
            direction, confidence = "direction_unexplained_needs_micro", 0.46

    if root == "entered_too_early" or (is_loss and mae is not None and mae > 0.6 and mfe is not None and mfe > 0.5):
        if dist >= 100:
            entry_timing = "entry_overextended_from_mean"
        elif (side == "LONG" and boll is not None and boll >= 0.9) or (side == "SHORT" and boll is not None and boll <= 0.1):
            entry_timing = "entry_bollinger_extreme_chase"
        elif (side == "LONG" and upper_wick is not None and upper_wick >= 0.45) or (side == "SHORT" and lower_wick is not None and lower_wick >= 0.45):
            entry_timing = "entry_wick_rejection"
        elif volume_z is not None and volume_z < 1 and side_flow != "same":
            entry_timing = "entry_low_volume_no_confirmation"
        elif atr is not None and atr >= 100:
            entry_timing = "entry_atr_noise_zone"
        elif btc_trend in {"chop", "sideways"} or btc_vol == "high":
            entry_timing = "entry_btc_chop_wait_required"
        elif funding_bucket in {"OVERHEATED", "POSITIVE_EXTREME", "NEGATIVE_EXTREME"} or (oi_change is not None and abs(oi_change) > 0.04):
            entry_timing = "entry_oi_funding_crowded_wait_required"
        else:
            entry_timing = "entry_wait_recheck_needed"
        confidence = max(confidence, 0.66)

    if root == "tp_too_far" or (planned_rr is not None and mfe is not None and mfe < planned_rr * 0.7):
        if planned_rr is not None and mfe is not None and mfe < planned_rr * 0.5:
            tp_realism = "tp_above_observed_mfe_distribution"
        elif btc_trend in {"chop", "sideways"}:
            tp_realism = "tp_far_in_chop_regime"
        elif atr is not None and atr < 30:
            tp_realism = "tp_far_in_low_atr_regime"
        elif volume_z is not None and volume_z < 1:
            tp_realism = "tp_far_without_volume_followthrough"
        elif dist >= 80:
            tp_realism = "tp_far_after_overextended_entry"
        else:
            tp_realism = "tp_far_unconfirmed_followthrough"
        confidence = max(confidence, 0.68)

    if is_win:
        if side_flow == "same" or price_flow == "same":
            profit = "profit_flow_aligned"
        elif btc_alignment == "same":
            profit = "profit_regime_aligned_good"
        elif spread is not None and spread < 5:
            profit = "profit_liquidity_cost_ok"
        elif mfe is not None and mae is not None and mfe > mae:
            profit = "profit_excursion_asymmetry_good"
        else:
            profit = "profit_reference_pattern"
        confidence = max(confidence, 0.6)

    if btc_alignment in {"same", "opposite"}:
        market_regime = f"btc_alignment_{btc_alignment}"
    elif btc_trend:
        market_regime = f"btc_trend_{btc_trend}"
    elif breadth:
        market_regime = f"breadth_{breadth}"
    if spread is not None and spread >= 8:
        liquidity = "spread_very_high_cost"
    elif spread is not None and spread >= 5:
        liquidity = "spread_high_cost"
    elif p24_quality.get("p24_match") == "observed":
        liquidity = "liquidity_observed_or_proxy_ok"

    return {
        "direction_factor_v5": direction,
        "entry_timing_factor_v5": entry_timing,
        "tp_realism_factor_v5": tp_realism,
        "profit_factor_v5": profit,
        "market_regime_factor_v5": market_regime,
        "liquidity_cost_factor_v5": liquidity,
        "confidence_v5": round(confidence, 4),
        "factor_evidence": {
            "deep_subcause_v4": deep_v4,
            "pct_1m_bps": pct_1m,
            "pct_3m_bps": pct_3m,
            "taker_buy_ratio": taker,
            "side_flow_alignment": side_flow,
            "price_flow_alignment": price_flow,
            "btc_alignment": btc_alignment,
            "btc_trend": btc_trend,
            "market_breadth": breadth,
            "oi_state": oi_state,
            "oi_change": oi_change,
            "funding_bucket": funding_bucket,
            "funding_crowded_side": funding_crowded_side,
            "ema20_distance_bps": ema_dist,
            "vwap_distance_bps": vwap_dist,
            "atr_14_bps": atr,
            "volume_z": volume_z,
            "spread_bps": spread,
            "targets_used_for_diagnosis_only": sorted(TARGET_ONLY_FIELDS),
        },
    }


def _causal_row(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    v4_features = _loads(row["features_json"], {})
    targets = _loads(row["targets_json"], {})
    p24_features, p24_quality = _latest_p24_feature(conn, row)
    features = {**v4_features, **{k: v for k, v in p24_features.items() if v is not None}}
    buckets = {k: v for k, v in _gate_buckets(features, row).items() if v not in (None, "", "unknown")}
    causal = _classify_causal_factor(row, features, targets, p24_quality)
    row_payload = {
        "feature_id": row["feature_id"],
        "root_cause": targets.get("root_cause") or row["root_cause"],
        "deep_subcause": row["deep_subcause"],
        "direction": causal["direction_factor_v5"],
        "entry": causal["entry_timing_factor_v5"],
        "tp": causal["tp_realism_factor_v5"],
        "profit": causal["profit_factor_v5"],
    }
    return {
        "causal_id": _stable_id("tqv5", row_payload),
        "feature_id": row["feature_id"],
        "diagnostic_id": row["diagnostic_id"],
        "sample_id": None,
        "source_type": row["source"],
        "package_key": row["package_key"],
        "experiment_id": row["experiment_id"],
        "parameter_set_id": row["parameter_set_id"],
        "strategy_line": row["strategy_line"],
        "symbol": row["symbol"],
        "side": row["side"],
        "entry_time_ms": row["entry_time_ms"],
        "root_cause": targets.get("root_cause") or row["root_cause"],
        "deep_subcause_v4": row["deep_subcause"],
        "direction_factor_v5": causal["direction_factor_v5"],
        "entry_timing_factor_v5": causal["entry_timing_factor_v5"],
        "tp_realism_factor_v5": causal["tp_realism_factor_v5"],
        "profit_factor_v5": causal["profit_factor_v5"],
        "market_regime_factor_v5": causal["market_regime_factor_v5"],
        "liquidity_cost_factor_v5": causal["liquidity_cost_factor_v5"],
        "confidence_v5": causal["confidence_v5"],
        "entry_known_feature_set_json": _json(buckets),
        "target_diagnostic_set_json": _json(targets),
        "factor_evidence_json": _json(causal["factor_evidence"]),
        "source_quality_json": _json(
            {
                "v4_source_level": row["source_level"],
                "v4_feature_completeness": row["feature_completeness"],
                "v4_proxy_level": row["proxy_level"],
                "p24": p24_quality,
                "no_lookahead": True,
            }
        ),
        "schema_version": SCHEMA_VERSION,
        "generated_at": _now(),
    }


def materialize_v5_payload(project_root: Path, *, strategies: list[str] | None = None, limit: int | None = None) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v5_tables(db_path)
    materialize_v4_payload(project_root, strategies=strategies, limit=None)
    wanted = set(strategies or ["strategy5", "strategy6"])
    destructive_refresh = limit is None
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        clauses = ["1=1"]
        params: list[Any] = []
        if wanted:
            clauses.append("f.strategy_line IN (%s)" % ",".join("?" for _ in wanted))
            params.extend(sorted(wanted))
        sql = f"""
            SELECT f.*, r.root_cause, r.deep_subcause
            FROM trade_quality_entry_evidence_v4 f
            LEFT JOIN trade_quality_deep_root_cause_v4 r ON r.feature_id = f.feature_id
            WHERE {' AND '.join(clauses)}
            ORDER BY f.strategy_line, f.parameter_set_id, f.entry_time_ms
        """
        rows = conn.execute(sql, params).fetchall()
        if limit:
            rows = rows[: max(0, limit)]
        causal_rows = [_causal_row(conn, row) for row in rows]
        if destructive_refresh:
            if wanted:
                conn.execute(
                    f"DELETE FROM trade_quality_causal_factors_v5 WHERE schema_version = ? AND strategy_line IN ({','.join('?' for _ in wanted)})",
                    [SCHEMA_VERSION, *sorted(wanted)],
                )
            else:
                conn.execute("DELETE FROM trade_quality_causal_factors_v5 WHERE schema_version = ?", (SCHEMA_VERSION,))
        for row in causal_rows:
            conn.execute(
                """
                INSERT OR REPLACE INTO trade_quality_causal_factors_v5
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["causal_id"],
                    row["feature_id"],
                    row["diagnostic_id"],
                    row["sample_id"],
                    row["source_type"],
                    row["package_key"],
                    row["experiment_id"],
                    row["parameter_set_id"],
                    row["strategy_line"],
                    row["symbol"],
                    row["side"],
                    row["entry_time_ms"],
                    row["root_cause"],
                    row["deep_subcause_v4"],
                    row["direction_factor_v5"],
                    row["entry_timing_factor_v5"],
                    row["tp_realism_factor_v5"],
                    row["profit_factor_v5"],
                    row["market_regime_factor_v5"],
                    row["liquidity_cost_factor_v5"],
                    row["confidence_v5"],
                    row["entry_known_feature_set_json"],
                    row["target_diagnostic_set_json"],
                    row["factor_evidence_json"],
                    row["source_quality_json"],
                    row["schema_version"],
                    row["generated_at"],
                ),
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "materialized_causal_rows": len(causal_rows),
        "strategies": sorted(wanted),
        "refresh_mode": "full_replace" if destructive_refresh else "bounded_upsert",
        "generated_at": _now(),
    }


def _decode_v5_row(row: dict[str, Any]) -> dict[str, Any]:
    for key in (
        "entry_known_feature_set_json",
        "target_diagnostic_set_json",
        "factor_evidence_json",
        "source_quality_json",
        "rule_json",
        "feature_scope_json",
        "split_metrics_json",
        "aggregate_metrics_json",
        "factor_explanation_json",
        "config_patch_preview_json",
    ):
        if key in row:
            clean = key[:-5] if key.endswith("_json") else key
            row[clean] = _loads(row.get(key), {})
    return row


def _split(values: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    rows = sorted(values, key=lambda r: (r.get("entry_time_ms") or 0, r.get("causal_id") or ""))
    n = len(rows)
    train_end = int(n * 0.6)
    val_end = int(n * 0.8)
    return {"train": rows[:train_end], "validation": rows[train_end:val_end], "test": rows[val_end:]}


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    vals = [_safe_float(r.get("net_R")) for r in rows]
    vals = [float(v) for v in vals if v is not None]
    wins = [v for v in vals if v > 0]
    losses = [v for v in vals if v <= 0]
    return {
        "trades": len(vals),
        "pf": _pf(vals),
        "expectancy_R": mean(vals) if vals else None,
        "win_rate": (len(wins) / len(vals)) if vals else None,
        "total_R": sum(vals),
        "max_drawdown_R": _max_drawdown(vals),
        "avg_win_R": mean(wins) if wins else None,
        "avg_loss_R": mean(losses) if losses else None,
    }


def _load_v5_samples(conn: sqlite3.Connection, strategy_line: str | None = None) -> list[dict[str, Any]]:
    clauses = ["schema_version = ?"]
    params: list[Any] = [SCHEMA_VERSION]
    if strategy_line and strategy_line != "all":
        clauses.append("strategy_line = ?")
        params.append(strategy_line)
    rows = conn.execute(
        f"""
        SELECT *
        FROM trade_quality_causal_factors_v5
        WHERE {' AND '.join(clauses)}
        ORDER BY strategy_line, parameter_set_id, entry_time_ms
        """,
        params,
    ).fetchall()
    out = []
    for row in rows:
        d = dict(row)
        targets = _loads(d.get("target_diagnostic_set_json"), {})
        d["net_R"] = _safe_float(targets.get("net_R"))
        d["entry_features"] = _loads(d.get("entry_known_feature_set_json"), {})
        out.append(d)
    return out


def _factor_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    keys = [
        "direction_factor_v5",
        "entry_timing_factor_v5",
        "tp_realism_factor_v5",
        "profit_factor_v5",
        "market_regime_factor_v5",
        "liquidity_cost_factor_v5",
    ]
    summary: dict[str, Any] = {}
    for key in keys:
        c = Counter(str(r.get(key) or "unknown") for r in rows)
        summary[key] = c.most_common(8)
    root = Counter(str(r.get("root_cause") or "unknown") for r in rows)
    summary["root_cause"] = root.most_common(8)
    return summary


def _candidate_rules(rows: list[dict[str, Any]], *, min_samples: int, limit: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    by_param: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key_param = (row["package_key"], row["strategy_line"], row["parameter_set_id"])
        by_param[key_param].append(row)
        features = row.get("entry_features") or {}
        for field in sorted(ENTRY_KNOWN_RULE_FIELDS):
            value = features.get(field)
            if value in (None, "", "unknown"):
                continue
            grouped[(row["package_key"], row["strategy_line"], row["parameter_set_id"], field, str(value))].append(row)
    candidates = []
    for (package, line, param, field, value), matched in grouped.items():
        if len(matched) < min_samples:
            continue
        universe = by_param[(package, line, param)]
        if len(matched) / max(1, len(universe)) > 0.75:
            continue
        matched_metrics = _metrics(matched)
        if (matched_metrics.get("expectancy_R") or 0) >= -0.03 or (matched_metrics.get("total_R") or 0) >= 0:
            continue
        kept = [r for r in universe if r not in matched]
        if len(kept) < min_samples:
            continue
        before = _metrics(universe)
        after = _metrics(kept)
        improvement = (after.get("pf") or 0) - (before.get("pf") or 0)
        if improvement <= 0:
            continue
        splits = {}
        split_rows = _split(universe)
        stable = True
        for split_name, split_universe in split_rows.items():
            split_kept = [r for r in split_universe if (r.get("entry_features") or {}).get(field) != value]
            split_before = _metrics(split_universe)
            split_after = _metrics(split_kept)
            splits[split_name] = {"before": split_before, "after": split_after}
            if split_name in {"validation", "test"} and split_before.get("trades", 0) >= max(10, min_samples // 3):
                stable = stable and ((split_after.get("pf") or 0) >= (split_before.get("pf") or 0))
        factor_rows = matched
        explanation = _factor_summary(factor_rows)
        risk = "low" if stable and len(matched) >= min_samples * 2 else "medium" if stable else "high"
        recommendation = "paper_shadow_ready" if stable and (splits.get("test", {}).get("after", {}).get("pf") or 0) >= 1 else "watch" if stable else "reject"
        rule = {"action": "shadow_block_or_downweight", "field": field, "value": value}
        candidates.append(
            {
                "validation_id": _stable_id("tqv5gate", [package, line, param, rule]),
                "package_key": package,
                "experiment_id": matched[0]["experiment_id"],
                "parameter_set_id": param,
                "strategy_line": line,
                "status": "shadow",
                "rule_json": _json(rule),
                "feature_scope_json": _json(
                    {
                        "entry_known_only": True,
                        "allowed_rule_fields": sorted(ENTRY_KNOWN_RULE_FIELDS),
                        "target_fields_excluded": sorted(TARGET_ONLY_FIELDS),
                        "causal_factors_used_for_explanation_only": True,
                    }
                ),
                "split_metrics_json": _json(splits),
                "aggregate_metrics_json": _json(
                    {
                        "before": before,
                        "matched_bad_bucket": matched_metrics,
                        "after": after,
                        "pf_improvement": improvement,
                        "removed_coverage": len(matched) / max(1, len(universe)),
                    }
                ),
                "factor_explanation_json": _json(explanation),
                "config_patch_preview_json": _json(
                    {"trade_quality_gate": {"mode": "shadow", "rules": [rule], "source": GATE_SCHEMA_VERSION}}
                ),
                "leakage_check_status": "pass",
                "overfit_risk": risk,
                "recommendation": recommendation,
                "schema_version": GATE_SCHEMA_VERSION,
                "generated_at": _now(),
            }
        )
    candidates.sort(
        key=lambda c: (
            _loads(c["aggregate_metrics_json"], {}).get("pf_improvement") or 0,
            _loads(c["aggregate_metrics_json"], {}).get("after", {}).get("pf") or 0,
        ),
        reverse=True,
    )
    return candidates[:limit]


def _rule_pairs_from_existing_gates(
    conn: sqlite3.Connection,
    *,
    strategy_line: str | None = None,
    max_seeds_per_strategy: int = 8,
) -> dict[str, list[tuple[str, str]]]:
    clauses = ["schema_version = ?", "leakage_check_status = 'pass'"]
    params: list[Any] = [GATE_SCHEMA_VERSION]
    if strategy_line and strategy_line != "all":
        clauses.append("strategy_line = ?")
        params.append(strategy_line)
    rows = conn.execute(
        f"""
        SELECT strategy_line, rule_json, aggregate_metrics_json, split_metrics_json, overfit_risk, recommendation
        FROM trade_quality_gate_validations_v5
        WHERE {' AND '.join(clauses)}
        ORDER BY
          CASE overfit_risk WHEN 'low' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
          json_extract(split_metrics_json, '$.test.after.pf') DESC,
          json_extract(aggregate_metrics_json, '$.pf_improvement') DESC
        """,
        params,
    ).fetchall()
    out: dict[str, list[tuple[str, str]]] = defaultdict(list)
    seen: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for row in rows:
        if row["recommendation"] not in {"watch", "paper_shadow_ready"}:
            continue
        if row["overfit_risk"] == "high":
            continue
        rule = _loads(row["rule_json"], {})
        field = str(rule.get("field") or "")
        value = str(rule.get("value") or "")
        if field not in ENTRY_KNOWN_RULE_FIELDS or field in TARGET_ONLY_FIELDS or not value:
            continue
        pair = (field, value)
        line = str(row["strategy_line"])
        if pair in seen[line]:
            continue
        out[line].append(pair)
        seen[line].add(pair)
        if len(out[line]) >= max_seeds_per_strategy:
            continue
    defaults = {
        "strategy5": [
            ("side_flow_alignment", "opposite"),
            ("price_flow_alignment", "opposite"),
            ("rsi_bucket", "rsi_mid"),
            ("bollinger_bucket", "boll_low"),
            ("entry_session", "us_late"),
            ("volume_z_bucket", "volume_extreme"),
        ],
        "strategy6": [
            ("btc_volatility", "normal"),
            ("funding_bucket", "NEGATIVE_EXTREME"),
            ("funding_crowded_side", "short"),
            ("market_breadth", "up"),
            ("oi_state", "price_up_oi_down_short_covering"),
            ("oi_change_bucket", "oi_flat"),
        ],
    }
    for line, pairs in defaults.items():
        if strategy_line and strategy_line != "all" and line != strategy_line:
            continue
        for pair in pairs:
            if len(out[line]) >= max_seeds_per_strategy:
                break
            if pair not in seen[line]:
                out[line].append(pair)
                seen[line].add(pair)
    return out


def _matches_rule_combo(row: dict[str, Any], rules: tuple[tuple[str, str], ...]) -> bool:
    features = row.get("entry_features") or {}
    return all(str(features.get(field)) == value for field, value in rules)


def _combo_risk_and_recommendation(
    *,
    stable: bool,
    removed_coverage: float,
    test_after: dict[str, Any],
    test_before: dict[str, Any],
    validation_after: dict[str, Any],
) -> tuple[str, str]:
    risk = "low"
    if not stable:
        risk = "high"
    if removed_coverage > 0.60:
        risk = "high"
    if int(test_after.get("trades") or 0) < 300:
        risk = "high"
    if (test_after.get("pf") or 0) < (test_before.get("pf") or 0):
        risk = "high"
    if risk != "high" and int(test_after.get("trades") or 0) < 600:
        risk = "medium"
    recommendation = "reject"
    if risk != "high" and (test_after.get("pf") or 0) >= 1.0 and (validation_after.get("pf") or 0) >= 0.95:
        recommendation = "paper_shadow_ready"
    elif risk != "high":
        recommendation = "watch"
    return risk, recommendation


def _combo_candidate_rules(
    rows: list[dict[str, Any]],
    *,
    seed_rules: dict[str, list[tuple[str, str]]],
    min_samples: int,
    limit: int,
    max_combo_size: int,
) -> list[dict[str, Any]]:
    by_param: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_param[(row["package_key"], row["strategy_line"], row["parameter_set_id"])].append(row)

    candidates: list[dict[str, Any]] = []
    for (package, line, param), universe in by_param.items():
        seeds = seed_rules.get(line, [])
        if len(seeds) < 2:
            continue
        for combo_size in range(2, min(max_combo_size, len(seeds)) + 1):
            for combo in combinations(seeds, combo_size):
                fields = [field for field, _ in combo]
                if len(fields) != len(set(fields)):
                    continue
                if any(field not in ENTRY_KNOWN_RULE_FIELDS or field in TARGET_ONLY_FIELDS for field in fields):
                    continue
                matched = [row for row in universe if _matches_rule_combo(row, combo)]
                if len(matched) < min_samples:
                    continue
                removed_coverage = len(matched) / max(1, len(universe))
                if removed_coverage > 0.80:
                    continue
                matched_metrics = _metrics(matched)
                if (matched_metrics.get("expectancy_R") or 0) >= -0.02 or (matched_metrics.get("total_R") or 0) >= 0:
                    continue
                kept = [row for row in universe if not _matches_rule_combo(row, combo)]
                if len(kept) < min_samples:
                    continue
                before = _metrics(universe)
                after = _metrics(kept)
                improvement = (after.get("pf") or 0) - (before.get("pf") or 0)
                if improvement <= 0:
                    continue
                split_metrics = {}
                stable = True
                for split_name, split_universe in _split(universe).items():
                    split_kept = [row for row in split_universe if not _matches_rule_combo(row, combo)]
                    split_before = _metrics(split_universe)
                    split_after = _metrics(split_kept)
                    split_metrics[split_name] = {"before": split_before, "after": split_after}
                    if split_name in {"validation", "test"} and int(split_before.get("trades") or 0) >= max(50, min_samples // 2):
                        stable = stable and ((split_after.get("pf") or 0) >= (split_before.get("pf") or 0))
                risk, recommendation = _combo_risk_and_recommendation(
                    stable=stable,
                    removed_coverage=removed_coverage,
                    test_after=split_metrics.get("test", {}).get("after", {}),
                    test_before=split_metrics.get("test", {}).get("before", {}),
                    validation_after=split_metrics.get("validation", {}).get("after", {}),
                )
                rules = [{"action": "shadow_block_or_downweight", "field": field, "value": value} for field, value in combo]
                candidates.append(
                    {
                        "validation_id": _stable_id("tqv5combo", [package, line, param, rules]),
                        "package_key": package,
                        "experiment_id": universe[0]["experiment_id"],
                        "parameter_set_id": param,
                        "strategy_line": line,
                        "status": "shadow",
                        "combo_size": combo_size,
                        "rule_json": _json({"operator": "AND", "rules": rules}),
                        "feature_scope_json": _json(
                            {
                                "entry_known_only": True,
                                "allowed_rule_fields": sorted(ENTRY_KNOWN_RULE_FIELDS),
                                "target_fields_excluded": sorted(TARGET_ONLY_FIELDS),
                                "causal_factors_used_for_explanation_only": True,
                            }
                        ),
                        "split_metrics_json": _json(split_metrics),
                        "aggregate_metrics_json": _json(
                            {
                                "before": before,
                                "matched_bad_bucket": matched_metrics,
                                "after": after,
                                "pf_improvement": improvement,
                                "removed_coverage": removed_coverage,
                            }
                        ),
                        "factor_explanation_json": _json(_factor_summary(matched)),
                        "config_patch_preview_json": _json(
                            {"trade_quality_gate": {"mode": "shadow", "rules": rules, "source": COMBO_GATE_SCHEMA_VERSION}}
                        ),
                        "leakage_check_status": "pass",
                        "overfit_risk": risk,
                        "recommendation": recommendation,
                        "schema_version": COMBO_GATE_SCHEMA_VERSION,
                        "generated_at": _now(),
                    }
                )
    candidates.sort(
        key=lambda c: (
            1 if c["recommendation"] == "paper_shadow_ready" else 0,
            1 if c["overfit_risk"] == "low" else 0,
            _loads(c["split_metrics_json"], {}).get("test", {}).get("after", {}).get("pf") or 0,
            _loads(c["aggregate_metrics_json"], {}).get("pf_improvement") or 0,
        ),
        reverse=True,
    )
    return candidates[:limit]


def generate_combo_gate_candidates_v5_payload(
    project_root: Path,
    *,
    strategy_line: str | None = None,
    min_samples: int = 50,
    limit: int = 120,
    max_combo_size: int = 3,
    max_seeds_per_strategy: int = 8,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v5_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        samples = _load_v5_samples(conn, strategy_line)
        seeds = _rule_pairs_from_existing_gates(
            conn,
            strategy_line=strategy_line,
            max_seeds_per_strategy=max(2, min(12, int(max_seeds_per_strategy or 8))),
        )
        candidates = _combo_candidate_rules(
            samples,
            seed_rules=seeds,
            min_samples=max(1, int(min_samples or 50)),
            limit=max(1, int(limit or 120)),
            max_combo_size=max(2, min(3, int(max_combo_size or 3))),
        )
        if strategy_line and strategy_line != "all":
            conn.execute(
                "DELETE FROM trade_quality_combo_gate_validations_v5 WHERE strategy_line = ? AND schema_version = ?",
                (strategy_line, COMBO_GATE_SCHEMA_VERSION),
            )
        else:
            conn.execute(
                "DELETE FROM trade_quality_combo_gate_validations_v5 WHERE schema_version = ?",
                (COMBO_GATE_SCHEMA_VERSION,),
            )
        for row in candidates:
            conn.execute(
                """
                INSERT OR REPLACE INTO trade_quality_combo_gate_validations_v5
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["validation_id"],
                    row["package_key"],
                    row["experiment_id"],
                    row["parameter_set_id"],
                    row["strategy_line"],
                    row["status"],
                    row["combo_size"],
                    row["rule_json"],
                    row["feature_scope_json"],
                    row["split_metrics_json"],
                    row["aggregate_metrics_json"],
                    row["factor_explanation_json"],
                    row["config_patch_preview_json"],
                    row["leakage_check_status"],
                    row["overfit_risk"],
                    row["recommendation"],
                    row["schema_version"],
                    row["generated_at"],
                ),
            )
    return {
        "schema_version": COMBO_GATE_SCHEMA_VERSION,
        "status": "ok",
        "candidate_count": len(candidates),
        "seed_rules": {line: [{"field": f, "value": v} for f, v in pairs] for line, pairs in seeds.items()},
        "candidates": candidates[: min(limit, 20)],
        "generated_at": _now(),
    }


def generate_gate_candidates_v5_payload(
    project_root: Path,
    *,
    strategy_line: str | None = None,
    min_samples: int = 50,
    limit: int = 80,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v5_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        samples = _load_v5_samples(conn, strategy_line)
        candidates = _candidate_rules(samples, min_samples=min_samples, limit=limit)
        if strategy_line and strategy_line != "all":
            conn.execute(
                "DELETE FROM trade_quality_gate_validations_v5 WHERE strategy_line = ? AND schema_version = ?",
                (strategy_line, GATE_SCHEMA_VERSION),
            )
        else:
            conn.execute("DELETE FROM trade_quality_gate_validations_v5 WHERE schema_version = ?", (GATE_SCHEMA_VERSION,))
        for row in candidates:
            conn.execute(
                """
                INSERT OR REPLACE INTO trade_quality_gate_validations_v5
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["validation_id"],
                    row["package_key"],
                    row["experiment_id"],
                    row["parameter_set_id"],
                    row["strategy_line"],
                    row["status"],
                    row["rule_json"],
                    row["feature_scope_json"],
                    row["split_metrics_json"],
                    row["aggregate_metrics_json"],
                    row["factor_explanation_json"],
                    row["config_patch_preview_json"],
                    row["leakage_check_status"],
                    row["overfit_risk"],
                    row["recommendation"],
                    row["schema_version"],
                    row["generated_at"],
                ),
            )
    return {
        "schema_version": GATE_SCHEMA_VERSION,
        "status": "ok",
        "candidate_count": len(candidates),
        "candidates": candidates[: min(limit, 20)],
        "generated_at": _now(),
    }


def causal_factors_payload(
    project_root: Path,
    *,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    root_cause: str | None = None,
    direction_factor_v5: str | None = None,
    entry_timing_factor_v5: str | None = None,
    tp_realism_factor_v5: str | None = None,
    profit_factor_v5: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v5_tables(db_path)
    clauses = ["schema_version = ?"]
    params: list[Any] = [SCHEMA_VERSION]
    filters = {
        "strategy_line": strategy_line,
        "parameter_set_id": parameter_set_id,
        "root_cause": root_cause,
        "direction_factor_v5": direction_factor_v5,
        "entry_timing_factor_v5": entry_timing_factor_v5,
        "tp_realism_factor_v5": tp_realism_factor_v5,
        "profit_factor_v5": profit_factor_v5,
    }
    for key, value in filters.items():
        if value and value != "all":
            clauses.append(f"{key} = ?")
            params.append(value)
    bounded_limit = max(1, min(1000, int(limit or 200)))
    bounded_offset = max(0, int(offset or 0))
    where_sql = " AND ".join(clauses)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total = conn.execute(
            f"SELECT COUNT(*) FROM trade_quality_causal_factors_v5 WHERE {where_sql}",
            params,
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT *
            FROM trade_quality_causal_factors_v5
            WHERE {where_sql}
            ORDER BY generated_at DESC, entry_time_ms DESC
            LIMIT ? OFFSET ?
            """,
            [*params, bounded_limit, bounded_offset],
        ).fetchall()
        rollups = conn.execute(
            f"""
            SELECT strategy_line, root_cause, direction_factor_v5, entry_timing_factor_v5,
                   tp_realism_factor_v5, profit_factor_v5, market_regime_factor_v5,
                   liquidity_cost_factor_v5, COUNT(*) AS rows,
                   AVG(confidence_v5) AS avg_confidence
            FROM trade_quality_causal_factors_v5
            WHERE {where_sql}
            GROUP BY strategy_line, root_cause, direction_factor_v5, entry_timing_factor_v5,
                     tp_realism_factor_v5, profit_factor_v5, market_regime_factor_v5,
                     liquidity_cost_factor_v5
            ORDER BY rows DESC
            LIMIT 80
            """,
            params,
        ).fetchall()
    return {
        "schema_version": SCHEMA_VERSION,
        "total": total,
        "limit": bounded_limit,
        "offset": bounded_offset,
        "rows": [_decode_v5_row(dict(row)) for row in rows],
        "rollups": [dict(row) for row in rollups],
    }


def gate_candidates_payload(
    project_root: Path,
    *,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    recommendation: str | None = None,
    overfit_risk: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v5_tables(db_path)
    clauses = ["schema_version = ?"]
    params: list[Any] = [GATE_SCHEMA_VERSION]
    filters = {
        "strategy_line": strategy_line,
        "parameter_set_id": parameter_set_id,
        "recommendation": recommendation,
        "overfit_risk": overfit_risk,
    }
    for key, value in filters.items():
        if value and value != "all":
            clauses.append(f"{key} = ?")
            params.append(value)
    bounded_limit = max(1, min(1000, int(limit or 200)))
    bounded_offset = max(0, int(offset or 0))
    where_sql = " AND ".join(clauses)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total = conn.execute(
            f"SELECT COUNT(*) FROM trade_quality_gate_validations_v5 WHERE {where_sql}",
            params,
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT *
            FROM trade_quality_gate_validations_v5
            WHERE {where_sql}
            ORDER BY
              json_extract(aggregate_metrics_json, '$.pf_improvement') DESC,
              json_extract(aggregate_metrics_json, '$.after.pf') DESC,
              generated_at DESC
            LIMIT ? OFFSET ?
            """,
            [*params, bounded_limit, bounded_offset],
        ).fetchall()
    decoded = [_decode_v5_row(dict(row)) for row in rows]
    for row in decoded:
        row["rule"] = row.get("rule") or _loads(row.get("rule_json"), {})
        row["split_metrics"] = row.get("split_metrics") or _loads(row.get("split_metrics_json"), {})
        row["aggregate_metrics"] = row.get("aggregate_metrics") or _loads(row.get("aggregate_metrics_json"), {})
        row["factor_explanation"] = row.get("factor_explanation") or _loads(row.get("factor_explanation_json"), {})
        row["config_patch_preview"] = row.get("config_patch_preview") or _loads(row.get("config_patch_preview_json"), {})
    return {
        "schema_version": GATE_SCHEMA_VERSION,
        "total": total,
        "limit": bounded_limit,
        "offset": bounded_offset,
        "candidates": decoded,
        "entry_known_rule_fields": sorted(ENTRY_KNOWN_RULE_FIELDS),
        "target_only_fields": sorted(TARGET_ONLY_FIELDS),
    }


def writer_coverage_payload(project_root: Path) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v5_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('research_trade_facts','research_entry_features','research_tq_samples','trade_quality_causal_factors_v5','trade_quality_gate_validations_v5')"
            ).fetchall()
        }
        counts = {}
        for table in sorted(tables):
            counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        causal_quality = [
            dict(row)
            for row in conn.execute(
                """
                SELECT strategy_line,
                       json_extract(source_quality_json, '$.p24.p24_match') AS p24_match,
                       json_extract(source_quality_json, '$.p24.proxy_level') AS proxy_level,
                       COUNT(*) AS rows
                FROM trade_quality_causal_factors_v5
                WHERE schema_version = ?
                GROUP BY strategy_line, p24_match, proxy_level
                ORDER BY rows DESC
                LIMIT 50
                """,
                (SCHEMA_VERSION,),
            ).fetchall()
        ]
        by_source = []
        if "research_trade_facts" in tables:
            by_source = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT source_type, strategy_line, COUNT(*) AS rows
                    FROM research_trade_facts
                    GROUP BY source_type, strategy_line
                    ORDER BY rows DESC
                    LIMIT 50
                    """
                ).fetchall()
            ]
    return {
        "schema_version": "24.17-p24-tq-v5-writer-coverage",
        "tables": counts,
        "causal_source_quality": causal_quality,
        "trade_fact_sources": by_source,
        "lineage_required_fields": [
            "source_type",
            "package_key",
            "experiment_id",
            "parameter_set_id",
            "strategy_line",
            "symbol",
            "side",
            "entry_time_ms",
        ],
        "status": "ok",
        "generated_at": _now(),
    }


def summary_payload(project_root: Path) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_trade_quality_v5_tables(db_path)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        causal_count = conn.execute(
            "SELECT COUNT(*) FROM trade_quality_causal_factors_v5 WHERE schema_version = ?",
            (SCHEMA_VERSION,),
        ).fetchone()[0]
        gate_count = conn.execute(
            "SELECT COUNT(*) FROM trade_quality_gate_validations_v5 WHERE schema_version = ?",
            (GATE_SCHEMA_VERSION,),
        ).fetchone()[0]
        by_strategy = [
            dict(row)
            for row in conn.execute(
                """
                SELECT strategy_line, COUNT(*) AS rows
                FROM trade_quality_causal_factors_v5
                WHERE schema_version = ?
                GROUP BY strategy_line
                ORDER BY rows DESC
                """,
                (SCHEMA_VERSION,),
            ).fetchall()
        ]
        factors = [
            dict(row)
            for row in conn.execute(
                """
                SELECT strategy_line, root_cause, direction_factor_v5, entry_timing_factor_v5,
                       tp_realism_factor_v5, profit_factor_v5, COUNT(*) AS rows
                FROM trade_quality_causal_factors_v5
                WHERE schema_version = ?
                GROUP BY strategy_line, root_cause, direction_factor_v5, entry_timing_factor_v5,
                         tp_realism_factor_v5, profit_factor_v5
                ORDER BY rows DESC
                LIMIT 30
                """,
                (SCHEMA_VERSION,),
            ).fetchall()
        ]
    return {
        "schema_version": SCHEMA_VERSION,
        "gate_schema_version": GATE_SCHEMA_VERSION,
        "causal_count": causal_count,
        "gate_count": gate_count,
        "by_strategy": by_strategy,
        "top_factor_combinations": factors,
        "entry_known_rule_fields": sorted(ENTRY_KNOWN_RULE_FIELDS),
        "target_only_fields": sorted(TARGET_ONLY_FIELDS),
    }


def audit_markdown(project_root: Path) -> str:
    summary = summary_payload(project_root)
    db_path = p21_db_path(project_root)
    lines = [
        "# STEP7.129 Strategy5/6 P24 TQ V5 Gate PF Holdout Audit",
        "",
        f"- generated_at: `{_now()}`",
        f"- research_db: `{db_path}`",
        f"- causal_rows: `{summary['causal_count']}`",
        f"- gate_candidates: `{summary['gate_count']}`",
        "",
        "## Contract",
        "",
        "- Baseline upgrade only; no strategy logic, config, paper, or sandbox mutation.",
        "- Rule predicates use entry-known fields only.",
        "- V5 causal factors use targets for diagnosis/explanation only and are not used as gate predicates.",
        "",
        "## Coverage",
        "",
        "| strategy | rows |",
        "| --- | ---: |",
    ]
    for row in summary["by_strategy"]:
        lines.append(f"| `{row['strategy_line']}` | {row['rows']} |")
    lines.extend(["", "## Top V5 Factor Combinations", "", "| strategy | root | direction | entry | tp | profit | rows |", "| --- | --- | --- | --- | --- | --- | ---: |"])
    for row in summary["top_factor_combinations"][:20]:
        lines.append(
            f"| `{row['strategy_line']}` | `{row['root_cause']}` | `{row['direction_factor_v5']}` | "
            f"`{row['entry_timing_factor_v5']}` | `{row['tp_realism_factor_v5']}` | `{row['profit_factor_v5']}` | {row['rows']} |"
        )
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        gates = conn.execute(
            """
            SELECT *
            FROM trade_quality_gate_validations_v5
            WHERE schema_version = ?
            ORDER BY
              json_extract(aggregate_metrics_json, '$.pf_improvement') DESC,
              json_extract(aggregate_metrics_json, '$.after.pf') DESC
            LIMIT 20
            """,
            (GATE_SCHEMA_VERSION,),
        ).fetchall()
    lines.extend(["", "## Top Shadow Gate Candidates", "", "| strategy | rule | before PF | after PF | test PF | removed | risk | recommendation |", "| --- | --- | ---: | ---: | ---: | ---: | --- | --- |"])
    for row in gates:
        agg = _loads(row["aggregate_metrics_json"], {})
        split = _loads(row["split_metrics_json"], {})
        rule = _loads(row["rule_json"], {})
        before_pf = agg.get("before", {}).get("pf")
        after_pf = agg.get("after", {}).get("pf")
        test_pf = split.get("test", {}).get("after", {}).get("pf")
        removed = agg.get("removed_coverage")
        lines.append(
            f"| `{row['strategy_line']}` | `{rule.get('field')}={rule.get('value')}` | "
            f"{_fmt(before_pf)} | {_fmt(after_pf)} | {_fmt(test_pf)} | {_fmt(removed)} | "
            f"`{row['overfit_risk']}` | `{row['recommendation']}` |"
        )
    lines.extend(
        [
            "",
            "## Judgment",
            "",
            "- V5 can explain prior coarse causes by separating direction, entry timing, TP realism, market regime, and liquidity/cost factors.",
            "- Candidates remain shadow-only. Any candidate with `paper_shadow_ready` still requires separate paper-shadow validation before config promotion.",
            "- If no test PF exceeds 1, the result is still useful: it identifies which entry-known buckets should be downweighted or investigated next.",
        ]
    )
    return "\n".join(lines) + "\n"


def _fmt(value: Any, digits: int = 3) -> str:
    v = _safe_float(value)
    if v is None:
        return "-"
    return f"{v:.{digits}f}"
