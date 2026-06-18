"""P19 R-first trade quality diagnostic samples.

This module is intentionally diagnostic-only. It normalizes closed paper/archive
trades into P19 tables and never mutates trade plans, paper lifecycle, or config
gating policy.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.models import STRATEGY_LINES, Candle, PaperConfig
from laoma_signal_engine.paper.utils import utc_now_iso
from laoma_signal_engine.trade_quality.archive_backfill import ensure_archive_ingest_tables
from laoma_signal_engine.trade_quality.engine import _holding_sec, _json, _loads, _num, _parse_iso
from laoma_signal_engine.trade_quality.replay_backfill import (
    BinanceHistoricalCandleProvider,
    REPLAY_BACKFILL_SCHEMA_VERSION,
    _provider_name,
    _provider_range,
    replay_backfill_payload,
)


DIAGNOSTIC_SCHEMA_VERSION = "19.1"
DIAGNOSTIC_LABEL_VERSION = "19.4"
DIAGNOSTIC_API_SCHEMA_VERSION = "12.54"
DIAGNOSTIC_REPLAY_SCHEMA_VERSION = "19.3"
DIAGNOSTIC_SYNC_SCHEMA_VERSION = "19.5"
ENTRY_FEATURE_VERSION = "19.14"
ENTRY_MICROSTRUCTURE_VERSION = "19.17"
ENTRY_MARKET_CONTEXT_VERSION = "19.21"
ENTRY_CONTEXT_V3_VERSION = "19.23"


def ensure_diagnostic_tables(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_archive_ingest_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_diagnostic_samples (
              diagnostic_id TEXT PRIMARY KEY,
              trade_id TEXT NOT NULL,
              order_id TEXT,
              legacy_sample_id TEXT,
              source TEXT NOT NULL,
              archive_id TEXT,
              archive_path TEXT,
              paper_reset_epoch_id TEXT,
              paper_experiment_id TEXT,
              paper_epoch_reset_at TEXT,
              paper_epoch_scope_key TEXT,
              run_id TEXT,
              cycle_id TEXT,
              strategy_line TEXT,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              entry_time TEXT,
              exit_time TEXT,
              entry_price REAL,
              exit_price REAL,
              holding_minutes REAL,
              entry_type TEXT,
              exit_reason TEXT,
              gross_pnl REAL,
              fee REAL,
              slippage_cost REAL,
              net_pnl REAL,
              initial_risk_usdt REAL,
              net_R REAL,
              planned_SL REAL,
              planned_TP REAL,
              planned_RR REAL,
              MFE REAL,
              MAE REAL,
              MFE_R REAL,
              MAE_R REAL,
              time_to_MFE_minutes REAL,
              time_to_MAE_minutes REAL,
              max_favorable_price REAL,
              max_adverse_price REAL,
              excursion_model TEXT,
              replay_status TEXT,
              root_cause TEXT,
              root_cause_confidence REAL,
              quality_tags_json TEXT NOT NULL,
              direction_quality TEXT,
              entry_quality TEXT,
              sl_quality TEXT,
              tp_quality TEXT,
              time_exit_quality TEXT,
              diagnostic_version TEXT NOT NULL,
              evidence_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_tq_diag_dedup
              ON trade_quality_diagnostic_samples(source, trade_id, symbol, side, entry_time, exit_time);
            CREATE INDEX IF NOT EXISTS idx_tq_diag_filters
              ON trade_quality_diagnostic_samples(source, strategy_line, symbol, side, exit_time);
            CREATE INDEX IF NOT EXISTS idx_tq_diag_root
              ON trade_quality_diagnostic_samples(root_cause, replay_status, exit_time);

            CREATE TABLE IF NOT EXISTS trade_quality_diagnostic_ingest_ledger (
              dedup_key TEXT PRIMARY KEY,
              diagnostic_id TEXT,
              source TEXT NOT NULL,
              trade_id TEXT,
              order_id TEXT,
              legacy_sample_id TEXT,
              archive_id TEXT,
              strategy_line TEXT,
              symbol TEXT,
              side TEXT,
              ingest_status TEXT NOT NULL,
              skip_reason TEXT,
              schema_version TEXT NOT NULL,
              ingested_at TEXT NOT NULL,
              evidence_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trade_quality_diagnostic_replay_ledger (
              diagnostic_id TEXT PRIMARY KEY,
              trade_id TEXT,
              legacy_sample_id TEXT,
              source TEXT,
              archive_id TEXT,
              symbol TEXT,
              strategy_line TEXT,
              side TEXT,
              replay_status TEXT NOT NULL,
              replay_reason TEXT,
              old_replay_status TEXT,
              old_MFE_R REAL,
              new_MFE_R REAL,
              old_MAE_R REAL,
              new_MAE_R REAL,
              schema_version TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              evidence_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trade_quality_entry_feature_samples (
              diagnostic_id TEXT PRIMARY KEY,
              trade_id TEXT,
              source TEXT,
              archive_id TEXT,
              strategy_line TEXT,
              symbol TEXT,
              side TEXT,
              entry_time TEXT,
              feature_version TEXT,
              pre_1m_return REAL,
              pre_3m_return REAL,
              pre_5m_return REAL,
              volume_z REAL,
              local_breakout INTEGER,
              distance_to_vwap_bps REAL,
              distance_to_ema_bps REAL,
              btc_pre_3m_return REAL,
              btc_alignment TEXT,
              entry_feature_coverage TEXT,
              entry_quality_label TEXT,
              entry_quality_score REAL,
              evidence_json TEXT,
              created_at TEXT,
              updated_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_tq_entry_features_source
              ON trade_quality_entry_feature_samples(source, archive_id, strategy_line, symbol, entry_time);
            CREATE INDEX IF NOT EXISTS idx_tq_entry_features_label
              ON trade_quality_entry_feature_samples(entry_quality_label, entry_feature_coverage, updated_at);

            CREATE TABLE IF NOT EXISTS trade_quality_entry_microstructure_samples (
              diagnostic_id TEXT PRIMARY KEY,
              trade_id TEXT,
              source TEXT,
              archive_id TEXT,
              run_id TEXT,
              cycle_id TEXT,
              strategy_line TEXT,
              symbol TEXT,
              side TEXT,
              entry_time TEXT,
              feature_version TEXT,
              evidence_window_sec INTEGER,
              evidence_source TEXT,
              evidence_status TEXT,
              evidence_gap_sec REAL,
              pre_1m_return REAL,
              pre_3m_return REAL,
              pre_5m_return REAL,
              volume_z REAL,
              local_breakout INTEGER,
              distance_to_vwap_bps REAL,
              distance_to_ema_bps REAL,
              taker_buy_ratio REAL,
              cvd_direction TEXT,
              cvd_delta REAL,
              cvd_z REAL,
              ofi_direction TEXT,
              ofi_value REAL,
              ofi_z REAL,
              spread_bps REAL,
              depth_imbalance REAL,
              depth_snapshot_age_sec REAL,
              oi_change REAL,
              funding_rate REAL,
              funding_regime TEXT,
              btc_pre_3m_return REAL,
              btc_alignment TEXT,
              entry_acceptance_score REAL,
              entry_quality_v2_label TEXT,
              entry_quality_v2_reasons_json TEXT,
              evidence_json TEXT,
              created_at TEXT,
              updated_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_tq_entry_micro_source
              ON trade_quality_entry_microstructure_samples(source, archive_id, strategy_line, symbol, entry_time);
            CREATE INDEX IF NOT EXISTS idx_tq_entry_micro_label
              ON trade_quality_entry_microstructure_samples(entry_quality_v2_label, evidence_status, updated_at);

            CREATE TABLE IF NOT EXISTS trade_quality_entry_market_context_samples (
              diagnostic_id TEXT PRIMARY KEY,
              trade_id TEXT,
              source TEXT,
              archive_id TEXT,
              run_id TEXT,
              cycle_id TEXT,
              strategy_line TEXT,
              symbol TEXT,
              side TEXT,
              entry_time TEXT,
              context_version TEXT,
              evidence_source TEXT,
              market_context_status TEXT,
              oi_change REAL,
              oi_change_z REAL,
              oi_direction TEXT,
              funding_rate REAL,
              funding_regime TEXT,
              funding_crowded_side TEXT,
              btc_pre_3m_return REAL,
              btc_pre_5m_return REAL,
              btc_alignment TEXT,
              market_context_score REAL,
              market_context_label TEXT,
              market_context_reasons_json TEXT,
              evidence_json TEXT,
              created_at TEXT,
              updated_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_tq_entry_market_source
              ON trade_quality_entry_market_context_samples(source, archive_id, strategy_line, symbol, entry_time);
            CREATE INDEX IF NOT EXISTS idx_tq_entry_market_label
              ON trade_quality_entry_market_context_samples(market_context_label, market_context_status, updated_at);

            CREATE TABLE IF NOT EXISTS trade_quality_entry_context_v3_samples (
              diagnostic_id TEXT PRIMARY KEY,
              trade_id TEXT,
              source TEXT,
              archive_id TEXT,
              run_id TEXT,
              cycle_id TEXT,
              strategy_line TEXT,
              symbol TEXT,
              side TEXT,
              entry_time TEXT,
              context_version TEXT,
              market_context_status TEXT,
              micro_context_status TEXT,
              market_context_label TEXT,
              micro_context_label TEXT,
              entry_context_v3_label TEXT,
              entry_context_v3_score REAL,
              entry_context_v3_reasons_json TEXT,
              evidence_json TEXT,
              created_at TEXT,
              updated_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_tq_entry_v3_source
              ON trade_quality_entry_context_v3_samples(source, archive_id, strategy_line, symbol, entry_time);
            CREATE INDEX IF NOT EXISTS idx_tq_entry_v3_label
              ON trade_quality_entry_context_v3_samples(entry_context_v3_label, market_context_status, micro_context_status, updated_at);

            CREATE TABLE IF NOT EXISTS trade_quality_diagnostic_aggregates (
              aggregate_id TEXT PRIMARY KEY,
              package_key TEXT NOT NULL,
              dimension TEXT NOT NULL,
              key TEXT NOT NULL,
              sample_count INTEGER NOT NULL,
              avg_net_R REAL,
              median_net_R REAL,
              win_rate REAL,
              avg_MFE_R REAL,
              avg_MAE_R REAL,
              evidence_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(package_key, dimension, key)
            );

            CREATE TABLE IF NOT EXISTS trade_quality_diagnostic_sync_meta (
              meta_key TEXT PRIMARY KEY,
              meta_value TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            """
        )
        _ensure_columns(
            conn,
            "trade_quality_diagnostic_samples",
            {
                "paper_reset_epoch_id": "TEXT",
                "paper_experiment_id": "TEXT",
                "paper_epoch_reset_at": "TEXT",
                "paper_epoch_scope_key": "TEXT",
            },
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tq_diag_paper_epoch
              ON trade_quality_diagnostic_samples(source, strategy_line, paper_reset_epoch_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tq_diag_paper_scope
              ON trade_quality_diagnostic_samples(source, paper_epoch_scope_key)
            """
        )
        _ensure_columns(
            conn,
            "trade_quality_diagnostic_replay_ledger",
            {
                "archive_id": "TEXT",
            },
        )


def diagnostic_backfill_payload(
    project_root: Path,
    *,
    write: bool = False,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    config: PaperConfig | None = None,
) -> dict[str, Any]:
    db_path = _db_path(project_root, config)
    ensure_diagnostic_tables(db_path)
    rows = _build_diagnostic_rows(db_path, source=_normalize_source(source), archive_id=archive_id, limit=limit)
    now = utc_now_iso()
    status_counts: Counter[str] = Counter()
    normalized_source = _normalize_source(source)
    epoch_scope = _current_paper_epoch_scope_payload(db_path) if normalized_source == "current_paper" else None
    sync_scope = _current_paper_active_package_key(epoch_scope) if epoch_scope else f"source:{normalized_source}"
    if archive_id:
        sync_scope += f"|archive_id:{archive_id}"
    if write:
        with sqlite3.connect(db_path) as conn:
            for row in rows:
                _upsert_diagnostic_sample(conn, row, now)
                _upsert_ingest_ledger(conn, row, now)
                status_counts["upserted"] += 1
            _rebuild_aggregates(conn, rows, now, package_key=sync_scope)
            _upsert_sync_meta(
                conn,
                {
                    "last_synced_at": now,
                    "sync_source": normalized_source,
                    "sync_scope": sync_scope,
                    "sample_count": len(rows),
                    "schema_version": DIAGNOSTIC_SYNC_SCHEMA_VERSION,
                },
                now,
            )
    else:
        status_counts["would_upsert"] = len(rows)
    return {
        "schema_version": DIAGNOSTIC_SCHEMA_VERSION,
        "mode": "run" if write else "dry_run",
        "db_path": str(db_path),
        "source": normalized_source,
        "archive_id": archive_id,
        "paper_epoch_scope": epoch_scope,
        "limit": limit,
        "generated_at": now,
        "candidate_samples": len(rows),
        "status_counts": dict(status_counts),
        "source_counts": dict(Counter(row["source"] for row in rows)),
        "replay_status_counts": dict(Counter(str(row.get("replay_status") or "unknown") for row in rows)),
        "root_cause_counts": dict(Counter(str(row.get("root_cause") or "unknown") for row in rows)),
        "samples": rows[:100],
    }


def diagnostic_replay_payload(
    project_root: Path,
    *,
    write: bool = False,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    config: PaperConfig | None = None,
    candle_provider: Any | None = None,
) -> dict[str, Any]:
    """Run safe legacy replay, then resync P19 diagnostics.

    P19 stores normalized samples. STEP19.3 deliberately reuses the proven P18
    historical 1m replay path for legacy/archive data instead of introducing a
    second Binance fetcher.
    """

    root = project_root.resolve()
    db_path = _db_path(root, config)
    ensure_diagnostic_tables(db_path)
    if _normalize_source(source) == "current_paper":
        if write:
            diagnostic_backfill_payload(root, write=True, limit=limit, source="current_paper", config=config)
        current_replay = _current_paper_diagnostic_replay(
            db_path,
            write=write,
            limit=limit,
            candle_provider=candle_provider,
        )
        return {
            "schema_version": DIAGNOSTIC_REPLAY_SCHEMA_VERSION,
            "mode": "run" if write else "dry_run",
            "db_path": str(db_path),
            "source": "current_paper",
            "archive_id": archive_id,
            "legacy_replay": {
                "schema_version": REPLAY_BACKFILL_SCHEMA_VERSION,
                "mode": "skipped",
                "reason": "current_paper_uses_p19_diagnostic_replay",
            },
            "current_paper_replay": current_replay,
            "diagnostic_sync": diagnostic_backfill_payload(
                root,
                write=False,
                limit=limit,
                source="current_paper",
                config=config,
            ),
        }
    p18_source = {"all": "all", "archive": "archive", "current_paper": "live", "legacy_p18": "all"}.get(
        _normalize_source(source),
        "all",
    )
    replay = replay_backfill_payload(
        root,
        write=write,
        limit=limit,
        sample_source=p18_source,
        archive_id=archive_id,
        config=config or PaperConfig(),
        candle_provider=candle_provider,
        rebuild_rollups=False,
    )
    sync_source = source
    sync_limit = limit
    sync = diagnostic_backfill_payload(
        root,
        write=write,
        limit=sync_limit,
        source=sync_source,
        archive_id=archive_id,
        config=config,
    )
    if write:
        _sync_replay_ledger(db_path, limit=limit, archive_id=archive_id)
    return {
        "schema_version": DIAGNOSTIC_REPLAY_SCHEMA_VERSION,
        "mode": "run" if write else "dry_run",
        "db_path": str(db_path),
        "source": _normalize_source(source),
        "archive_id": archive_id,
        "legacy_replay": replay,
        "diagnostic_sync": sync,
    }


def diagnostic_entry_feature_payload(
    project_root: Path,
    *,
    write: bool = False,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
    config: PaperConfig | None = None,
    candle_provider: Any | None = None,
) -> dict[str, Any]:
    root = project_root.resolve()
    db_path = _db_path(root, config)
    ensure_diagnostic_tables(db_path)
    safe_limit = _safe_limit(limit or 100)
    provider = candle_provider or BinanceHistoricalCandleProvider()
    rows, _ = _query_samples(db_path, limit=safe_limit, offset=0, source=source, archive_id=archive_id)
    candidates = [row for row in rows if force or not row.get("entry_features")]
    now = utc_now_iso()
    results: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    for row in candidates[:safe_limit]:
        result = _entry_feature_result(row, provider, now)
        results.append(result)
        reason_counts[str(result.get("entry_feature_coverage") or "unknown")] += 1
    if write:
        with sqlite3.connect(db_path) as conn:
            for result in results:
                _upsert_entry_feature(conn, result)
    return {
        "schema_version": ENTRY_FEATURE_VERSION,
        "mode": "run" if write else "dry_run",
        "db_path": str(db_path),
        "source": _normalize_source(source),
        "archive_id": archive_id,
        "force": bool(force),
        "limit": safe_limit,
        "generated_at": now,
        "candidate_count": len(candidates[:safe_limit]),
        "updated_count": len(results) if write else 0,
        "missing_candle_count": sum(1 for row in results if str(row.get("entry_feature_coverage") or "").startswith("missing")),
        "feature_version": ENTRY_FEATURE_VERSION,
        "reason_counts": dict(reason_counts),
        "samples": results[:100],
    }


def _entry_feature_result(row: dict[str, Any], provider: Any, now: str) -> dict[str, Any]:
    entry_dt = _parse_iso(row.get("entry_time"))
    if entry_dt is None:
        return _entry_feature_missing(row, now, "missing_entry_time")
    start = _iso_z(entry_dt - timedelta(minutes=5))
    end = _iso_z(entry_dt)
    symbol = str(row.get("symbol") or "")
    side = _side(row.get("side"))
    candles = sorted(_provider_range(provider, symbol, start, end), key=lambda c: int(c.open_time_ms))
    if len(candles) < 2:
        return _entry_feature_missing(row, now, "missing_symbol_candle", candle_count=len(candles))
    btc_candles = sorted(_provider_range(provider, "BTCUSDT", start, end), key=lambda c: int(c.open_time_ms))
    btc_pre_3m = _window_return(btc_candles, entry_dt, 3) if len(btc_candles) >= 2 else None
    btc_alignment = _btc_alignment(side, btc_pre_3m)
    entry_price = _num(row.get("entry_price"))
    feature = {
        "diagnostic_id": row.get("diagnostic_id"),
        "trade_id": row.get("trade_id"),
        "source": row.get("source"),
        "archive_id": row.get("archive_id"),
        "strategy_line": row.get("strategy_line"),
        "symbol": symbol,
        "side": side,
        "entry_time": row.get("entry_time"),
        "feature_version": ENTRY_FEATURE_VERSION,
        "pre_1m_return": _window_return(candles, entry_dt, 1),
        "pre_3m_return": _window_return(candles, entry_dt, 3),
        "pre_5m_return": _window_return(candles, entry_dt, 5),
        "volume_z": _volume_z(candles),
        "local_breakout": _local_breakout(candles, side, entry_price),
        "distance_to_vwap_bps": _bps(entry_price, _vwap(candles)),
        "distance_to_ema_bps": _bps(entry_price, _ema([_num(c.close) for c in candles])),
        "btc_pre_3m_return": btc_pre_3m,
        "btc_alignment": btc_alignment,
        "entry_feature_coverage": "complete" if btc_alignment != "missing_btc_candle" else "partial_missing_btc_candle",
        "created_at": now,
        "updated_at": now,
        "evidence": {
            "symbol_candle_count": len(candles),
            "btc_candle_count": len(btc_candles),
            "window_start": start,
            "window_end": end,
            "provider": _provider_name(provider),
            "diagnostic_only": True,
        },
    }
    label, score = _entry_quality_label(row, feature)
    feature["entry_quality_label"] = label
    feature["entry_quality_score"] = score
    return feature


def _entry_feature_missing(row: dict[str, Any], now: str, reason: str, **extra: Any) -> dict[str, Any]:
    return {
        "diagnostic_id": row.get("diagnostic_id"),
        "trade_id": row.get("trade_id"),
        "source": row.get("source"),
        "archive_id": row.get("archive_id"),
        "strategy_line": row.get("strategy_line"),
        "symbol": row.get("symbol"),
        "side": _side(row.get("side")),
        "entry_time": row.get("entry_time"),
        "feature_version": ENTRY_FEATURE_VERSION,
        "pre_1m_return": None,
        "pre_3m_return": None,
        "pre_5m_return": None,
        "volume_z": None,
        "local_breakout": None,
        "distance_to_vwap_bps": None,
        "distance_to_ema_bps": None,
        "btc_pre_3m_return": None,
        "btc_alignment": "unknown",
        "entry_feature_coverage": reason,
        "entry_quality_label": "entry_feature_missing",
        "entry_quality_score": 0.0,
        "created_at": now,
        "updated_at": now,
        "evidence": {"reason": reason, "diagnostic_only": True, **extra},
    }


def _entry_quality_label(row: dict[str, Any], feature: dict[str, Any]) -> tuple[str, float]:
    if feature.get("entry_feature_coverage") not in {"complete", "partial_missing_btc_candle"}:
        return "entry_feature_missing", 0.0
    side_mult = -1.0 if _side(row.get("side")) == "SHORT" else 1.0
    pre_1m = _num(feature.get("pre_1m_return")) * side_mult
    pre_3m = _num(feature.get("pre_3m_return")) * side_mult
    pre_5m = _num(feature.get("pre_5m_return")) * side_mult
    mfe = _num(row.get("MFE_R"))
    mae = _num(row.get("MAE_R"))
    net_r = _num(row.get("net_R"))
    volume_z = _num(feature.get("volume_z"))
    vwap_bps = abs(_num(feature.get("distance_to_vwap_bps")))
    btc_alignment = str(feature.get("btc_alignment") or "unknown")
    if pre_5m > 0.03 and mfe < 0.3 and mae > 0.7:
        return "impulse_exhausted", 0.85
    if volume_z > 2.0 and mfe < 0.3:
        return "volume_no_followthrough", 0.78
    if btc_alignment == "opposite" and net_r < 0:
        return "btc_opposite_pressure", 0.74
    if vwap_bps > 80 and net_r < 0:
        return "far_from_mean", 0.72
    if pre_3m > 0.015 and mfe < 0.5:
        return "late_chase", 0.7
    if pre_1m > 0.002 and pre_3m > 0.003 and mfe >= 0.8 and mae <= 0.5:
        return "accepted_impulse", 0.82
    return "entry_quality_observed", 0.5


def _window_return(candles: list[Candle], entry_dt: Any, minutes: int) -> float | None:
    if not candles:
        return None
    entry_ms = int(entry_dt.timestamp() * 1000)
    start_ms = entry_ms - minutes * 60 * 1000
    window = [c for c in candles if start_ms <= int(c.open_time_ms) <= entry_ms]
    if len(window) < 2:
        window = candles[-2:]
    first = _num(window[0].open)
    last = _num(window[-1].close)
    if first <= 0:
        return None
    return round((last - first) / first, 8)


def _volume_z(candles: list[Candle]) -> float | None:
    volumes = [_num(c.volume) for c in candles if c.volume is not None]
    if len(volumes) < 2:
        return None
    baseline = volumes[:-1]
    avg = sum(baseline) / len(baseline)
    variance = sum((v - avg) ** 2 for v in baseline) / len(baseline)
    std = variance ** 0.5
    if std <= 0:
        return 0.0
    return round((volumes[-1] - avg) / std, 8)


def _vwap(candles: list[Candle]) -> float | None:
    weighted = sum(_num(c.close) * max(_num(c.volume), 0.0) for c in candles)
    volume = sum(max(_num(c.volume), 0.0) for c in candles)
    if volume > 0:
        return weighted / volume
    closes = [_num(c.close) for c in candles if _num(c.close) > 0]
    return sum(closes) / len(closes) if closes else None


def _ema(values: list[float]) -> float | None:
    values = [v for v in values if v > 0]
    if not values:
        return None
    alpha = 2 / (len(values) + 1)
    ema = values[0]
    for value in values[1:]:
        ema = alpha * value + (1 - alpha) * ema
    return ema


def _bps(price: float, reference: float | None) -> float | None:
    if price <= 0 or not reference or reference <= 0:
        return None
    return round((price - reference) / reference * 10000, 8)


def _local_breakout(candles: list[Candle], side: str, entry_price: float) -> int | None:
    if len(candles) < 2 or entry_price <= 0:
        return None
    previous = candles[:-1]
    if side == "SHORT":
        return 1 if entry_price <= min(_num(c.low) for c in previous) else 0
    return 1 if entry_price >= max(_num(c.high) for c in previous) else 0


def _btc_alignment(side: str, btc_return: float | None) -> str:
    if btc_return is None:
        return "missing_btc_candle"
    if abs(btc_return) < 0.0005:
        return "neutral"
    if side == "SHORT":
        return "same" if btc_return < 0 else "opposite"
    return "same" if btc_return > 0 else "opposite"


def _iso_z(dt: Any) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _upsert_entry_feature(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO trade_quality_entry_feature_samples(
          diagnostic_id, trade_id, source, archive_id, strategy_line, symbol, side,
          entry_time, feature_version, pre_1m_return, pre_3m_return, pre_5m_return,
          volume_z, local_breakout, distance_to_vwap_bps, distance_to_ema_bps,
          btc_pre_3m_return, btc_alignment, entry_feature_coverage,
          entry_quality_label, entry_quality_score, evidence_json, created_at, updated_at
        ) VALUES(
          :diagnostic_id, :trade_id, :source, :archive_id, :strategy_line, :symbol, :side,
          :entry_time, :feature_version, :pre_1m_return, :pre_3m_return, :pre_5m_return,
          :volume_z, :local_breakout, :distance_to_vwap_bps, :distance_to_ema_bps,
          :btc_pre_3m_return, :btc_alignment, :entry_feature_coverage,
          :entry_quality_label, :entry_quality_score, :evidence_json, :created_at, :updated_at
        )
        ON CONFLICT(diagnostic_id) DO UPDATE SET
          trade_id=excluded.trade_id,
          source=excluded.source,
          archive_id=excluded.archive_id,
          strategy_line=excluded.strategy_line,
          symbol=excluded.symbol,
          side=excluded.side,
          entry_time=excluded.entry_time,
          feature_version=excluded.feature_version,
          pre_1m_return=excluded.pre_1m_return,
          pre_3m_return=excluded.pre_3m_return,
          pre_5m_return=excluded.pre_5m_return,
          volume_z=excluded.volume_z,
          local_breakout=excluded.local_breakout,
          distance_to_vwap_bps=excluded.distance_to_vwap_bps,
          distance_to_ema_bps=excluded.distance_to_ema_bps,
          btc_pre_3m_return=excluded.btc_pre_3m_return,
          btc_alignment=excluded.btc_alignment,
          entry_feature_coverage=excluded.entry_feature_coverage,
          entry_quality_label=excluded.entry_quality_label,
          entry_quality_score=excluded.entry_quality_score,
          evidence_json=excluded.evidence_json,
          updated_at=excluded.updated_at
        """,
        {**row, "evidence_json": _json(row.get("evidence") or {})},
    )


def diagnostic_entry_microstructure_payload(
    project_root: Path,
    *,
    write: bool = False,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
    evidence_window_sec: int = 180,
    config: PaperConfig | None = None,
) -> dict[str, Any]:
    root = project_root.resolve()
    db_path = _db_path(root, config)
    audit_db_path = root / "DATA" / "audit" / "run_audit.db"
    ensure_diagnostic_tables(db_path)
    safe_limit = _safe_limit(limit or 100)
    rows, _ = _query_samples(db_path, limit=safe_limit, offset=0, source=source, archive_id=archive_id)
    candidates = [row for row in rows if force or not row.get("entry_microstructure")]
    now = utc_now_iso()
    results: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    label_counts: Counter[str] = Counter()
    for row in candidates[:safe_limit]:
        result = _entry_microstructure_result(row, audit_db_path, now, int(evidence_window_sec or 180))
        results.append(result)
        status_counts[str(result.get("evidence_status") or "unknown")] += 1
        label_counts[str(result.get("entry_quality_v2_label") or "unknown")] += 1
    if write:
        with sqlite3.connect(db_path) as conn:
            for result in results:
                _upsert_entry_microstructure(conn, result)
    return {
        "schema_version": ENTRY_MICROSTRUCTURE_VERSION,
        "mode": "run" if write else "dry_run",
        "db_path": str(db_path),
        "audit_db_path": str(audit_db_path),
        "source": _normalize_source(source),
        "archive_id": archive_id,
        "force": bool(force),
        "limit": safe_limit,
        "evidence_window_sec": int(evidence_window_sec or 180),
        "generated_at": now,
        "candidate_count": len(candidates[:safe_limit]),
        "updated_count": len(results) if write else 0,
        "evidence_status_counts": dict(status_counts),
        "label_counts": dict(label_counts),
        "samples": results[:100],
    }


def _entry_microstructure_result(row: dict[str, Any], audit_db_path: Path, now: str, window_sec: int) -> dict[str, Any]:
    entry_features = row.get("entry_features") or {}
    base = {
        "diagnostic_id": row.get("diagnostic_id"),
        "trade_id": row.get("trade_id"),
        "source": row.get("source"),
        "archive_id": row.get("archive_id"),
        "run_id": row.get("run_id"),
        "cycle_id": row.get("cycle_id"),
        "strategy_line": row.get("strategy_line"),
        "symbol": row.get("symbol"),
        "side": _side(row.get("side")),
        "entry_time": row.get("entry_time"),
        "feature_version": ENTRY_MICROSTRUCTURE_VERSION,
        "evidence_window_sec": int(window_sec),
        "pre_1m_return": entry_features.get("pre_1m_return"),
        "pre_3m_return": entry_features.get("pre_3m_return"),
        "pre_5m_return": entry_features.get("pre_5m_return"),
        "volume_z": entry_features.get("volume_z"),
        "local_breakout": entry_features.get("local_breakout"),
        "distance_to_vwap_bps": entry_features.get("distance_to_vwap_bps"),
        "distance_to_ema_bps": entry_features.get("distance_to_ema_bps"),
        "btc_pre_3m_return": entry_features.get("btc_pre_3m_return"),
        "btc_alignment": entry_features.get("btc_alignment"),
        "created_at": now,
        "updated_at": now,
    }
    strategy_line = str(row.get("strategy_line") or "unknown")
    if strategy_line == "without_micro":
        result = {
            **base,
            "evidence_source": "strategy_line_boundary",
            "evidence_status": "micro_evidence_not_required",
            "evidence_gap_sec": None,
            "taker_buy_ratio": None,
            "cvd_direction": "not_applicable",
            "cvd_delta": None,
            "cvd_z": None,
            "ofi_direction": "not_applicable",
            "ofi_value": None,
            "ofi_z": None,
            "spread_bps": None,
            "depth_imbalance": None,
            "depth_snapshot_age_sec": None,
            "oi_change": None,
            "funding_rate": None,
            "funding_regime": None,
            "evidence": {"reason": "without_micro_does_not_require_micro_evidence", "diagnostic_only": True},
        }
        label, score, reasons = _entry_microstructure_label(row, result)
        result.update({"entry_quality_v2_label": label, "entry_acceptance_score": score, "entry_quality_v2_reasons": reasons})
        return result
    frame = _lookup_micro_factor_frame(audit_db_path, row, window_sec)
    if not frame:
        result = {
            **base,
            "evidence_source": "audit_db",
            "evidence_status": "missing_micro_evidence",
            "evidence_gap_sec": None,
            "taker_buy_ratio": None,
            "cvd_direction": "missing",
            "cvd_delta": None,
            "cvd_z": None,
            "ofi_direction": "missing",
            "ofi_value": None,
            "ofi_z": None,
            "spread_bps": None,
            "depth_imbalance": None,
            "depth_snapshot_age_sec": None,
            "oi_change": None,
            "funding_rate": None,
            "funding_regime": None,
            "evidence": {"reason": "no_micro_factor_frame_in_entry_window", "audit_db_path": str(audit_db_path), "diagnostic_only": True},
        }
        label, score, reasons = _entry_microstructure_label(row, result)
        result.update({"entry_quality_v2_label": label, "entry_acceptance_score": score, "entry_quality_v2_reasons": reasons})
        return result
    payload = _loads(frame.get("payload_json"), {}) if isinstance(frame.get("payload_json"), str) else (frame.get("payload_json") or {})
    result = {
        **base,
        "evidence_source": "audit_db.micro_factor_frames",
        "evidence_status": "complete" if frame.get("cvd_available") and frame.get("ofi_available") else "partial_micro_missing",
        "evidence_gap_sec": frame.get("gap_sec"),
        "taker_buy_ratio": _first_num(payload, ["taker_buy_ratio", "taker_ratio", "buy_ratio"]),
        "cvd_direction": _signed_direction(_num(frame.get("cvd"))),
        "cvd_delta": frame.get("cvd"),
        "cvd_z": frame.get("z_cvd"),
        "ofi_direction": _signed_direction(_num(frame.get("ofi"))),
        "ofi_value": frame.get("ofi"),
        "ofi_z": frame.get("z_ofi"),
        "spread_bps": _first_num(payload, ["spread_bps", "spread"]),
        "depth_imbalance": _first_num(payload, ["depth_imbalance", "book_imbalance"]),
        "depth_snapshot_age_sec": _first_num(payload, ["depth_snapshot_age_sec", "book_age_sec"]),
        "oi_change": _first_num(payload, ["oi_change", "open_interest_change", "oi_delta"]),
        "funding_rate": _first_num(payload, ["funding_rate"]),
        "funding_regime": payload.get("funding_regime") or payload.get("funding_state"),
        "evidence": {
            "audit_db_path": str(audit_db_path),
            "bucket_ts_sec": frame.get("bucket_ts_sec"),
            "generated_at": frame.get("generated_at"),
            "payload_keys": sorted(payload.keys())[:50] if isinstance(payload, dict) else [],
            "diagnostic_only": True,
        },
    }
    label, score, reasons = _entry_microstructure_label(row, result)
    result.update({"entry_quality_v2_label": label, "entry_acceptance_score": score, "entry_quality_v2_reasons": reasons})
    return result


def _lookup_micro_factor_frame(audit_db_path: Path, row: dict[str, Any], window_sec: int) -> dict[str, Any] | None:
    if not audit_db_path.exists():
        return None
    entry_dt = _parse_iso(row.get("entry_time"))
    if entry_dt is None:
        return None
    entry_ts = int(entry_dt.timestamp())
    symbol = str(row.get("symbol") or "").upper()
    strategy_line = str(row.get("strategy_line") or "")
    with sqlite3.connect(audit_db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *, abs(bucket_ts_sec - ?) AS gap_sec
            FROM micro_factor_frames
            WHERE upper(symbol)=? AND strategy_line=?
              AND abs(bucket_ts_sec - ?) <= ?
            ORDER BY gap_sec ASC, generated_at DESC
            LIMIT 1
            """,
            (entry_ts, symbol, strategy_line, entry_ts, int(window_sec)),
        ).fetchall()
        if not rows:
            rows = conn.execute(
                """
                SELECT *, abs(bucket_ts_sec - ?) AS gap_sec
                FROM micro_factor_frames
                WHERE upper(symbol)=?
                  AND abs(bucket_ts_sec - ?) <= ?
                ORDER BY gap_sec ASC, generated_at DESC
                LIMIT 1
                """,
                (entry_ts, symbol, entry_ts, int(window_sec)),
            ).fetchall()
    return dict(rows[0]) if rows else None


def _entry_microstructure_label(row: dict[str, Any], feature: dict[str, Any]) -> tuple[str, float, list[str]]:
    reasons: list[str] = []
    status = str(feature.get("evidence_status") or "")
    if status == "micro_evidence_not_required":
        return "not_applicable_without_micro", 0.5, ["without_micro_strategy_line"]
    if status in {"missing_micro_evidence", "stale_micro_evidence"}:
        return "microstructure_evidence_missing", 0.0, [status]
    side = _side(row.get("side"))
    side_mult = -1.0 if side == "SHORT" else 1.0
    net_r = _num(row.get("net_R"))
    mfe = _num(row.get("MFE_R"))
    mae = _num(row.get("MAE_R"))
    volume_z = _num(feature.get("volume_z"))
    local_breakout = int(_num(feature.get("local_breakout"))) if feature.get("local_breakout") is not None else 0
    cvd_dir = str(feature.get("cvd_direction") or "missing")
    ofi_dir = str(feature.get("ofi_direction") or "missing")
    spread_bps = _num(feature.get("spread_bps"))
    depth_imb = _num(feature.get("depth_imbalance"))
    vwap_bps = abs(_num(feature.get("distance_to_vwap_bps")))
    btc_alignment = str(feature.get("btc_alignment") or "unknown")
    cvd_support = _direction_supports_side(cvd_dir, side)
    ofi_support = _direction_supports_side(ofi_dir, side)
    if volume_z > 2.0 and mfe < 0.3 and cvd_support is False:
        return "price_move_not_confirmed_by_cvd", 0.2, ["volume_z_high", "cvd_not_supporting", "low_mfe"]
    if local_breakout and ofi_support is False and net_r < 0:
        return "breakout_not_confirmed_by_ofi", 0.22, ["local_breakout", "ofi_not_supporting", "loss_trade"]
    if cvd_support is True and vwap_bps > 80 and mae > 0.7:
        return "cvd_strong_but_price_extended", 0.28, ["cvd_supporting", "far_from_vwap", "high_mae"]
    if btc_alignment == "opposite" and net_r < 0:
        return "btc_opposite_pressure", 0.3, ["btc_opposite", "loss_trade"]
    if spread_bps > 15 and str(row.get("entry_type") or "").lower() == "market" and net_r < 0:
        return "spread_too_wide_for_market_entry", 0.25, ["wide_spread", "market_entry", "loss_trade"]
    if depth_imb * side_mult < -0.15 and mae > 0.6:
        return "depth_imbalance_against_entry", 0.3, ["depth_against_side", "high_mae"]
    if cvd_support is True and ofi_support is True and btc_alignment in {"same", "neutral"} and mfe >= 0.8:
        return "microstructure_accepted_impulse", 0.85, ["cvd_supporting", "ofi_supporting", "mfe_reached"]
    return "microstructure_observed", 0.55, reasons or ["no_strong_v2_pattern"]


def _direction_supports_side(direction: str, side: str) -> bool | None:
    direction = str(direction or "missing").lower()
    side = _side(side)
    if direction in {"missing", "not_applicable", "neutral", "unknown"}:
        return None
    if side == "SHORT":
        return direction in {"down", "sell", "negative", "against_bid"}
    return direction in {"up", "buy", "positive", "against_ask"}


def _signed_direction(value: float | None) -> str:
    if value is None:
        return "missing"
    if abs(value) < 1e-12:
        return "neutral"
    return "up" if value > 0 else "down"


def _first_num(payload: dict[str, Any], keys: list[str]) -> float | None:
    if not isinstance(payload, dict):
        return None
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return _num(payload.get(key))
    return None


def _upsert_entry_microstructure(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO trade_quality_entry_microstructure_samples(
          diagnostic_id, trade_id, source, archive_id, run_id, cycle_id, strategy_line, symbol, side, entry_time,
          feature_version, evidence_window_sec, evidence_source, evidence_status, evidence_gap_sec,
          pre_1m_return, pre_3m_return, pre_5m_return, volume_z, local_breakout, distance_to_vwap_bps,
          distance_to_ema_bps, taker_buy_ratio, cvd_direction, cvd_delta, cvd_z, ofi_direction, ofi_value,
          ofi_z, spread_bps, depth_imbalance, depth_snapshot_age_sec, oi_change, funding_rate,
          funding_regime, btc_pre_3m_return, btc_alignment, entry_acceptance_score, entry_quality_v2_label,
          entry_quality_v2_reasons_json, evidence_json, created_at, updated_at
        ) VALUES(
          :diagnostic_id, :trade_id, :source, :archive_id, :run_id, :cycle_id, :strategy_line, :symbol, :side, :entry_time,
          :feature_version, :evidence_window_sec, :evidence_source, :evidence_status, :evidence_gap_sec,
          :pre_1m_return, :pre_3m_return, :pre_5m_return, :volume_z, :local_breakout, :distance_to_vwap_bps,
          :distance_to_ema_bps, :taker_buy_ratio, :cvd_direction, :cvd_delta, :cvd_z, :ofi_direction, :ofi_value,
          :ofi_z, :spread_bps, :depth_imbalance, :depth_snapshot_age_sec, :oi_change, :funding_rate,
          :funding_regime, :btc_pre_3m_return, :btc_alignment, :entry_acceptance_score, :entry_quality_v2_label,
          :entry_quality_v2_reasons_json, :evidence_json, :created_at, :updated_at
        )
        ON CONFLICT(diagnostic_id) DO UPDATE SET
          trade_id=excluded.trade_id, source=excluded.source, archive_id=excluded.archive_id,
          run_id=excluded.run_id, cycle_id=excluded.cycle_id, strategy_line=excluded.strategy_line,
          symbol=excluded.symbol, side=excluded.side, entry_time=excluded.entry_time,
          feature_version=excluded.feature_version, evidence_window_sec=excluded.evidence_window_sec,
          evidence_source=excluded.evidence_source, evidence_status=excluded.evidence_status,
          evidence_gap_sec=excluded.evidence_gap_sec, pre_1m_return=excluded.pre_1m_return,
          pre_3m_return=excluded.pre_3m_return, pre_5m_return=excluded.pre_5m_return,
          volume_z=excluded.volume_z, local_breakout=excluded.local_breakout,
          distance_to_vwap_bps=excluded.distance_to_vwap_bps,
          distance_to_ema_bps=excluded.distance_to_ema_bps, taker_buy_ratio=excluded.taker_buy_ratio,
          cvd_direction=excluded.cvd_direction, cvd_delta=excluded.cvd_delta, cvd_z=excluded.cvd_z,
          ofi_direction=excluded.ofi_direction, ofi_value=excluded.ofi_value, ofi_z=excluded.ofi_z,
          spread_bps=excluded.spread_bps, depth_imbalance=excluded.depth_imbalance,
          depth_snapshot_age_sec=excluded.depth_snapshot_age_sec, oi_change=excluded.oi_change,
          funding_rate=excluded.funding_rate, funding_regime=excluded.funding_regime,
          btc_pre_3m_return=excluded.btc_pre_3m_return, btc_alignment=excluded.btc_alignment,
          entry_acceptance_score=excluded.entry_acceptance_score,
          entry_quality_v2_label=excluded.entry_quality_v2_label,
          entry_quality_v2_reasons_json=excluded.entry_quality_v2_reasons_json,
          evidence_json=excluded.evidence_json, updated_at=excluded.updated_at
        """,
        {
            **row,
            "entry_quality_v2_reasons_json": _json(row.get("entry_quality_v2_reasons") or []),
            "evidence_json": _json(row.get("evidence") or {}),
        },
    )


def diagnostic_entry_market_context_payload(
    project_root: Path,
    *,
    write: bool = False,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
    config: PaperConfig | None = None,
) -> dict[str, Any]:
    root = project_root.resolve()
    db_path = _db_path(root, config)
    ensure_diagnostic_tables(db_path)
    safe_limit = _safe_limit(limit or 100)
    rows, _ = _query_samples(db_path, limit=safe_limit, offset=0, source=source, archive_id=archive_id)
    candidates = [row for row in rows if force or not row.get("entry_market_context")]
    factor_snapshot = _latest_factor_snapshot_by_symbol(root)
    now = utc_now_iso()
    results: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    label_counts: Counter[str] = Counter()
    for row in candidates[:safe_limit]:
        result = _entry_market_context_result(row, factor_snapshot, now)
        results.append(result)
        status_counts[str(result.get("market_context_status") or "unknown")] += 1
        label_counts[str(result.get("market_context_label") or "unknown")] += 1
    if write:
        with sqlite3.connect(db_path) as conn:
            for result in results:
                _upsert_entry_market_context(conn, result)
    return {
        "schema_version": ENTRY_MARKET_CONTEXT_VERSION,
        "mode": "run" if write else "dry_run",
        "db_path": str(db_path),
        "source": _normalize_source(source),
        "archive_id": archive_id,
        "force": bool(force),
        "limit": safe_limit,
        "generated_at": now,
        "candidate_count": len(candidates[:safe_limit]),
        "updated_count": len(results) if write else 0,
        "status_counts": dict(status_counts),
        "label_counts": dict(label_counts),
        "samples": results[:100],
    }


def _entry_market_context_result(row: dict[str, Any], factor_snapshot: dict[str, dict[str, Any]], now: str) -> dict[str, Any]:
    entry_features = row.get("entry_features") or {}
    micro = row.get("entry_microstructure") or {}
    symbol = str(row.get("symbol") or "").upper()
    factor = factor_snapshot.get(symbol) or {}
    oi_block = factor.get("oi_15m") if isinstance(factor.get("oi_15m"), dict) else {}
    funding_block = factor.get("funding_context") if isinstance(factor.get("funding_context"), dict) else {}
    oi_change = _first_present_num([micro.get("oi_change"), oi_block.get("oi_pct_change"), oi_block.get("oi_change")])
    oi_z = _first_present_num([oi_block.get("oi_z"), oi_block.get("z_oi")])
    funding_rate = _first_present_num([micro.get("funding_rate"), funding_block.get("funding_rate_raw"), funding_block.get("funding_rate")])
    funding_regime = (
        micro.get("funding_regime")
        or funding_block.get("funding_bucket")
        or funding_block.get("funding_regime")
        or _funding_regime(funding_rate)
    )
    btc_pre_3m = _first_present_num([entry_features.get("btc_pre_3m_return"), micro.get("btc_pre_3m_return")])
    btc_pre_5m = _first_present_num([entry_features.get("btc_pre_5m_return")])
    btc_alignment = entry_features.get("btc_alignment") or micro.get("btc_alignment") or _btc_alignment(_side(row.get("side")), btc_pre_3m)
    status_parts = {
        "oi": oi_change is not None,
        "funding": funding_rate is not None or bool(funding_regime),
        "btc": btc_alignment not in {"unknown", "missing_btc_candle", None},
    }
    if all(status_parts.values()):
        status = "complete"
    elif any(status_parts.values()):
        missing = [key for key, ok in status_parts.items() if not ok]
        status = "partial_" + "_".join(missing) + "_missing"
    else:
        status = "missing_market_context"
    result = {
        "diagnostic_id": row.get("diagnostic_id"),
        "trade_id": row.get("trade_id"),
        "source": row.get("source"),
        "archive_id": row.get("archive_id"),
        "run_id": row.get("run_id"),
        "cycle_id": row.get("cycle_id"),
        "strategy_line": row.get("strategy_line"),
        "symbol": symbol,
        "side": _side(row.get("side")),
        "entry_time": row.get("entry_time"),
        "context_version": ENTRY_MARKET_CONTEXT_VERSION,
        "evidence_source": _market_evidence_source(status_parts, bool(factor), bool(micro)),
        "market_context_status": status,
        "oi_change": oi_change,
        "oi_change_z": oi_z,
        "oi_direction": _signed_direction(oi_change),
        "funding_rate": funding_rate,
        "funding_regime": funding_regime,
        "funding_crowded_side": _funding_crowded_side(funding_rate, funding_regime),
        "btc_pre_3m_return": btc_pre_3m,
        "btc_pre_5m_return": btc_pre_5m,
        "btc_alignment": btc_alignment,
        "created_at": now,
        "updated_at": now,
        "evidence": {
            "diagnostic_only": True,
            "status_parts": status_parts,
            "factor_snapshot": {"present": bool(factor), "generated_at": factor.get("generated_at")},
            "micro_context_present": bool(micro),
        },
    }
    label, score, reasons = _entry_market_context_label(row, result)
    result.update({"market_context_label": label, "market_context_score": score, "market_context_reasons": reasons})
    return result


def _entry_market_context_label(row: dict[str, Any], ctx: dict[str, Any]) -> tuple[str, float, list[str]]:
    status = str(ctx.get("market_context_status") or "")
    if status == "missing_market_context":
        return "market_context_missing", 0.0, [status]
    side = _side(row.get("side"))
    net_r = _num(row.get("net_R"))
    oi_change = ctx.get("oi_change")
    funding_side = str(ctx.get("funding_crowded_side") or "neutral")
    btc_alignment = str(ctx.get("btc_alignment") or "unknown")
    if funding_side == side.lower() and net_r < 0:
        return "funding_crowded_against_entry", 0.25, ["crowded_funding_same_side", "loss_trade"]
    if oi_change is not None and _num(oi_change) < 0:
        return "oi_not_supporting_move", 0.35, ["oi_falling"]
    if funding_side == side.lower() and oi_change is not None and _num(oi_change) > 0 and net_r < 0:
        return "oi_crowding_reversal_risk", 0.2, ["oi_rising", "crowded_same_side", "loss_trade"]
    if btc_alignment == "opposite" and net_r < 0:
        return "btc_opposite_pressure", 0.3, ["btc_opposite", "loss_trade"]
    if btc_alignment == "neutral":
        return "btc_chop_low_edge", 0.45, ["btc_neutral_or_chop"]
    if status.startswith("partial"):
        return "market_context_partial", 0.45, [status]
    return "market_context_supported", 0.7, ["market_context_available"]


def diagnostic_entry_context_v3_payload(
    project_root: Path,
    *,
    write: bool = False,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
    config: PaperConfig | None = None,
) -> dict[str, Any]:
    root = project_root.resolve()
    db_path = _db_path(root, config)
    ensure_diagnostic_tables(db_path)
    safe_limit = _safe_limit(limit or 100)
    rows, _ = _query_samples(db_path, limit=safe_limit, offset=0, source=source, archive_id=archive_id)
    candidates = [row for row in rows if force or not row.get("entry_context_v3")]
    now = utc_now_iso()
    results: list[dict[str, Any]] = []
    label_counts: Counter[str] = Counter()
    for row in candidates[:safe_limit]:
        result = _entry_context_v3_result(row, now)
        results.append(result)
        label_counts[str(result.get("entry_context_v3_label") or "unknown")] += 1
    if write:
        with sqlite3.connect(db_path) as conn:
            for result in results:
                _upsert_entry_context_v3(conn, result)
    return {
        "schema_version": ENTRY_CONTEXT_V3_VERSION,
        "mode": "run" if write else "dry_run",
        "db_path": str(db_path),
        "source": _normalize_source(source),
        "archive_id": archive_id,
        "force": bool(force),
        "limit": safe_limit,
        "generated_at": now,
        "candidate_count": len(candidates[:safe_limit]),
        "updated_count": len(results) if write else 0,
        "label_counts": dict(label_counts),
        "samples": results[:100],
    }


def _entry_context_v3_result(row: dict[str, Any], now: str) -> dict[str, Any]:
    market = row.get("entry_market_context") or {}
    micro = row.get("entry_microstructure") or {}
    label, score, reasons = _entry_context_v3_label(row, market, micro)
    return {
        "diagnostic_id": row.get("diagnostic_id"),
        "trade_id": row.get("trade_id"),
        "source": row.get("source"),
        "archive_id": row.get("archive_id"),
        "run_id": row.get("run_id"),
        "cycle_id": row.get("cycle_id"),
        "strategy_line": row.get("strategy_line"),
        "symbol": row.get("symbol"),
        "side": _side(row.get("side")),
        "entry_time": row.get("entry_time"),
        "context_version": ENTRY_CONTEXT_V3_VERSION,
        "market_context_status": market.get("market_context_status") or "missing_market_context",
        "micro_context_status": micro.get("evidence_status") or ("micro_evidence_not_required" if str(row.get("strategy_line") or "") == "without_micro" else "missing_micro_evidence"),
        "market_context_label": market.get("market_context_label") or "market_context_missing",
        "micro_context_label": micro.get("entry_quality_v2_label") or ("not_applicable_without_micro" if str(row.get("strategy_line") or "") == "without_micro" else "microstructure_evidence_missing"),
        "entry_context_v3_label": label,
        "entry_context_v3_score": score,
        "entry_context_v3_reasons": reasons,
        "evidence": {
            "diagnostic_only": True,
            "market_context_label": market.get("market_context_label"),
            "micro_context_label": micro.get("entry_quality_v2_label"),
            "net_R": row.get("net_R"),
            "MFE_R": row.get("MFE_R"),
            "MAE_R": row.get("MAE_R"),
        },
        "created_at": now,
        "updated_at": now,
    }


def _entry_context_v3_label(row: dict[str, Any], market: dict[str, Any], micro: dict[str, Any]) -> tuple[str, float, list[str]]:
    strategy_line = str(row.get("strategy_line") or "")
    net_r = _num(row.get("net_R"))
    mfe = _num(row.get("MFE_R"))
    market_label = str(market.get("market_context_label") or "market_context_missing")
    micro_label = str(micro.get("entry_quality_v2_label") or "")
    if market_label in {"funding_crowded_against_entry", "oi_not_supporting_move", "oi_crowding_reversal_risk", "btc_opposite_pressure"}:
        return market_label, 0.25, ["market_context_priority", market_label]
    if strategy_line != "without_micro":
        if micro_label in {"price_move_not_confirmed_by_cvd", "breakout_not_confirmed_by_ofi", "spread_too_wide_for_market_entry", "depth_imbalance_against_entry"}:
            return micro_label, 0.25, ["micro_context_priority", micro_label]
        if micro_label == "microstructure_accepted_impulse" and mfe >= 0.8 and net_r <= 0:
            return "entry_supported_but_exit_problem", 0.75, ["micro_accepted", "mfe_reached", "net_r_non_positive"]
        if micro_label == "microstructure_accepted_impulse":
            return "microstructure_full_acceptance", 0.85, ["micro_accepted"]
        if micro_label == "microstructure_evidence_missing":
            return "entry_context_evidence_gap", 0.0, ["micro_evidence_missing"]
        if micro_label:
            return "microstructure_partial_acceptance", 0.55, [micro_label]
    if market_label == "market_context_supported":
        return "market_context_supported", 0.7, ["market_context_supported"]
    if market_label in {"market_context_missing", "market_context_partial"}:
        return "entry_context_evidence_gap", 0.1, [market_label]
    return "entry_context_observed", 0.5, ["no_strong_v3_pattern"]


def _upsert_entry_market_context(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO trade_quality_entry_market_context_samples(
          diagnostic_id, trade_id, source, archive_id, run_id, cycle_id, strategy_line, symbol, side, entry_time,
          context_version, evidence_source, market_context_status, oi_change, oi_change_z, oi_direction,
          funding_rate, funding_regime, funding_crowded_side, btc_pre_3m_return, btc_pre_5m_return,
          btc_alignment, market_context_score, market_context_label, market_context_reasons_json,
          evidence_json, created_at, updated_at
        ) VALUES(
          :diagnostic_id, :trade_id, :source, :archive_id, :run_id, :cycle_id, :strategy_line, :symbol, :side, :entry_time,
          :context_version, :evidence_source, :market_context_status, :oi_change, :oi_change_z, :oi_direction,
          :funding_rate, :funding_regime, :funding_crowded_side, :btc_pre_3m_return, :btc_pre_5m_return,
          :btc_alignment, :market_context_score, :market_context_label, :market_context_reasons_json,
          :evidence_json, :created_at, :updated_at
        )
        ON CONFLICT(diagnostic_id) DO UPDATE SET
          trade_id=excluded.trade_id, source=excluded.source, archive_id=excluded.archive_id,
          run_id=excluded.run_id, cycle_id=excluded.cycle_id, strategy_line=excluded.strategy_line,
          symbol=excluded.symbol, side=excluded.side, entry_time=excluded.entry_time,
          context_version=excluded.context_version, evidence_source=excluded.evidence_source,
          market_context_status=excluded.market_context_status, oi_change=excluded.oi_change,
          oi_change_z=excluded.oi_change_z, oi_direction=excluded.oi_direction,
          funding_rate=excluded.funding_rate, funding_regime=excluded.funding_regime,
          funding_crowded_side=excluded.funding_crowded_side, btc_pre_3m_return=excluded.btc_pre_3m_return,
          btc_pre_5m_return=excluded.btc_pre_5m_return, btc_alignment=excluded.btc_alignment,
          market_context_score=excluded.market_context_score, market_context_label=excluded.market_context_label,
          market_context_reasons_json=excluded.market_context_reasons_json, evidence_json=excluded.evidence_json,
          updated_at=excluded.updated_at
        """,
        {**row, "market_context_reasons_json": _json(row.get("market_context_reasons") or []), "evidence_json": _json(row.get("evidence") or {})},
    )


def _upsert_entry_context_v3(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO trade_quality_entry_context_v3_samples(
          diagnostic_id, trade_id, source, archive_id, run_id, cycle_id, strategy_line, symbol, side, entry_time,
          context_version, market_context_status, micro_context_status, market_context_label, micro_context_label,
          entry_context_v3_label, entry_context_v3_score, entry_context_v3_reasons_json, evidence_json, created_at, updated_at
        ) VALUES(
          :diagnostic_id, :trade_id, :source, :archive_id, :run_id, :cycle_id, :strategy_line, :symbol, :side, :entry_time,
          :context_version, :market_context_status, :micro_context_status, :market_context_label, :micro_context_label,
          :entry_context_v3_label, :entry_context_v3_score, :entry_context_v3_reasons_json, :evidence_json, :created_at, :updated_at
        )
        ON CONFLICT(diagnostic_id) DO UPDATE SET
          trade_id=excluded.trade_id, source=excluded.source, archive_id=excluded.archive_id,
          run_id=excluded.run_id, cycle_id=excluded.cycle_id, strategy_line=excluded.strategy_line,
          symbol=excluded.symbol, side=excluded.side, entry_time=excluded.entry_time,
          context_version=excluded.context_version, market_context_status=excluded.market_context_status,
          micro_context_status=excluded.micro_context_status, market_context_label=excluded.market_context_label,
          micro_context_label=excluded.micro_context_label, entry_context_v3_label=excluded.entry_context_v3_label,
          entry_context_v3_score=excluded.entry_context_v3_score,
          entry_context_v3_reasons_json=excluded.entry_context_v3_reasons_json,
          evidence_json=excluded.evidence_json, updated_at=excluded.updated_at
        """,
        {**row, "entry_context_v3_reasons_json": _json(row.get("entry_context_v3_reasons") or []), "evidence_json": _json(row.get("evidence") or {})},
    )


def _latest_factor_snapshot_by_symbol(root: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for rel in ("DATA/factors/latest_factor_snapshot.json", "DATA/factors/latest_factor_snapshot_withoutoficvd.json"):
        path = root / rel
        if not path.exists():
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        generated_at = doc.get("generated_at") if isinstance(doc, dict) else None
        for item in (doc.get("items") or []) if isinstance(doc, dict) else []:
            if isinstance(item, dict) and item.get("symbol"):
                row = dict(item)
                row["generated_at"] = generated_at
                out[str(item.get("symbol")).upper()] = row
        if out:
            break
    return out


def _first_present_num(values: list[Any]) -> float | None:
    for value in values:
        if value is not None:
            return _num(value)
    return None


def _funding_regime(rate: float | None) -> str | None:
    if rate is None:
        return None
    if rate >= 0.0005:
        return "OVERHEATED"
    if rate <= -0.0005:
        return "NEGATIVE_EXTREME"
    if abs(rate) >= 0.0001:
        return "WARM"
    return "NEUTRAL"


def _funding_crowded_side(rate: float | None, regime: Any) -> str:
    regime_s = str(regime or "").upper()
    if regime_s in {"OVERHEATED", "POSITIVE_EXTREME"}:
        return "long"
    if regime_s == "NEGATIVE_EXTREME":
        return "short"
    if rate is None:
        return "neutral"
    if rate >= 0.0005:
        return "long"
    if rate <= -0.0005:
        return "short"
    return "neutral"


def _market_evidence_source(parts: dict[str, bool], has_factor: bool, has_micro: bool) -> str:
    sources = []
    if has_micro:
        sources.append("entry_microstructure")
    if has_factor:
        sources.append("latest_factor_snapshot")
    if parts.get("btc"):
        sources.append("entry_features")
    return "+".join(sources) or "missing"


def _current_paper_diagnostic_replay(
    db_path: Path,
    *,
    write: bool,
    limit: int | None,
    candle_provider: Any | None = None,
) -> dict[str, Any]:
    rows = _current_paper_replay_candidates(db_path, limit=limit)
    provider = candle_provider or BinanceHistoricalCandleProvider()
    now = utc_now_iso()
    status_counts: Counter[str] = Counter()
    ledger_rows: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    for row in rows:
        result = _replay_current_paper_diag_row(row, provider, now)
        status_counts[str(result.get("replay_status") or "unknown")] += 1
        ledger_rows.append(result["ledger"])
        if result.get("update"):
            updates.append(result["update"])
    if write:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            for update in updates:
                _update_current_paper_diag_replay(conn, update)
            for ledger in ledger_rows:
                _upsert_current_paper_diag_replay_ledger(conn, ledger)
            active_clause, active_params = _current_paper_active_sql_clause(db_path)
            where = "WHERE source='current_paper'"
            if active_clause:
                where += f" AND {active_clause}"
            refreshed = [
                _decode_diag_row(dict(row))
                for row in conn.execute(
                    f"""
                    SELECT * FROM trade_quality_diagnostic_samples
                    {where}
                    ORDER BY COALESCE(exit_time, entry_time, updated_at) DESC
                    LIMIT 5000
                    """,
                    active_params,
                ).fetchall()
            ]
            _rebuild_aggregates(conn, refreshed, now, package_key=_current_paper_active_package_key(_current_paper_epoch_scope_payload(db_path)))
            _upsert_sync_meta(
                conn,
                {
                    "last_current_paper_replayed_at": now,
                    "current_paper_replay_updated": len(updates),
                    "current_paper_replay_status_counts": dict(status_counts),
                },
                now,
            )
    return {
        "schema_version": DIAGNOSTIC_REPLAY_SCHEMA_VERSION,
        "mode": "run" if write else "dry_run",
        "eligible_samples": len(rows),
        "updated_samples": len(updates) if write else 0,
        "would_update_samples": len(updates),
        "status_counts": dict(status_counts),
        "ledger_rows": ledger_rows[:200],
    }


def _current_paper_replay_candidates(db_path: Path, *, limit: int | None) -> list[dict[str, Any]]:
    active_clause, active_params = _current_paper_active_sql_clause(db_path, alias="d")
    sql = """
        SELECT d.*, o.quantity AS paper_quantity
        FROM trade_quality_diagnostic_samples d
        LEFT JOIN paper_orders o ON o.id = d.order_id
        WHERE d.source='current_paper'
          AND COALESCE(d.replay_status, '') != 'candle_1m_replay'
    """
    params: list[Any] = []
    if active_clause:
        sql += f" AND {active_clause}"
        params.extend(active_params)
    sql += " ORDER BY COALESCE(d.exit_time, d.entry_time, d.updated_at) DESC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(max(0, int(limit)))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _replay_current_paper_diag_row(row: dict[str, Any], provider: Any, now: str) -> dict[str, Any]:
    reason = _current_paper_replay_skip_reason(row)
    if reason:
        status = f"skipped_{reason}"
        return {
            "replay_status": status,
            "ledger": _current_paper_diag_replay_ledger(row, status, reason, now),
            "update": None,
        }
    symbol = str(row.get("symbol") or "").upper()
    try:
        candles = _provider_range(provider, symbol, row.get("entry_time"), row.get("exit_time"))
    except Exception as exc:
        status = "skipped_provider_error"
        return {
            "replay_status": status,
            "ledger": _current_paper_diag_replay_ledger(row, status, type(exc).__name__, now),
            "update": None,
        }
    if not candles:
        status = "skipped_no_candles"
        return {
            "replay_status": status,
            "ledger": _current_paper_diag_replay_ledger(row, status, "no_candles", now),
            "update": None,
        }
    mfe_r, mae_r, max_favorable, max_adverse = _current_paper_mfe_mae(row, candles)
    root = _diagnose(
        {
            **row,
            "MFE_R": mfe_r,
            "MAE_R": mae_r,
            "excursion_model": "candle_1m_replay",
        }
    )
    evidence = {
        **_loads(row.get("evidence_json"), {}),
        "replay_schema_version": DIAGNOSTIC_REPLAY_SCHEMA_VERSION,
        "replay_candle_count": len(candles),
        "replay_candle_source": _provider_name(provider),
        "old_replay_status": row.get("replay_status"),
        "old_MFE_R": row.get("MFE_R"),
        "old_MAE_R": row.get("MAE_R"),
    }
    update = {
        "diagnostic_id": row["diagnostic_id"],
        "MFE_R": mfe_r,
        "MAE_R": mae_r,
        "MFE": mfe_r * _num(row.get("initial_risk_usdt")),
        "MAE": mae_r * _num(row.get("initial_risk_usdt")),
        "max_favorable_price": max_favorable,
        "max_adverse_price": max_adverse,
        "excursion_model": "candle_1m_replay",
        "replay_status": "candle_1m_replay",
        "root_cause": root["root_cause"],
        "root_cause_confidence": root["root_cause_confidence"],
        "quality_tags_json": _json(root["quality_tags"]),
        "direction_quality": root["direction_quality"],
        "entry_quality": root["entry_quality"],
        "sl_quality": root["sl_quality"],
        "tp_quality": root["tp_quality"],
        "time_exit_quality": root["time_exit_quality"],
        "evidence_json": _json(evidence),
        "updated_at": now,
    }
    return {
        "replay_status": "updated",
        "ledger": _current_paper_diag_replay_ledger(
            row,
            "updated",
            "candle_1m_replay",
            now,
            new_mfe_r=mfe_r,
            new_mae_r=mae_r,
        ),
        "update": update,
    }


def _current_paper_replay_skip_reason(row: dict[str, Any]) -> str | None:
    if not row.get("entry_time") or not row.get("exit_time"):
        return "missing_time"
    if _parse_iso(row.get("entry_time")) is None or _parse_iso(row.get("exit_time")) is None:
        return "invalid_time"
    for key in ("symbol", "side", "entry_price", "initial_risk_usdt", "paper_quantity"):
        if not row.get(key):
            return f"missing_{key}"
    if _num(row.get("initial_risk_usdt")) <= 0:
        return "missing_risk"
    return None


def _current_paper_mfe_mae(row: dict[str, Any], candles: list[Any]) -> tuple[float, float, float | None, float | None]:
    entry = _num(row.get("entry_price"))
    qty = _num(row.get("paper_quantity"))
    initial_risk = _num(row.get("initial_risk_usdt"))
    side = _side(row.get("side"))
    if side == "SHORT":
        favorable_price = min(_num(c.low) for c in candles)
        adverse_price = max(_num(c.high) for c in candles)
        favorable = max(0.0, entry - favorable_price) * qty
        adverse = max(0.0, adverse_price - entry) * qty
    else:
        favorable_price = max(_num(c.high) for c in candles)
        adverse_price = min(_num(c.low) for c in candles)
        favorable = max(0.0, favorable_price - entry) * qty
        adverse = max(0.0, entry - adverse_price) * qty
    return favorable / initial_risk, adverse / initial_risk, favorable_price, adverse_price


def _update_current_paper_diag_replay(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        UPDATE trade_quality_diagnostic_samples
        SET MFE_R=?,
            MAE_R=?,
            MFE=?,
            MAE=?,
            max_favorable_price=?,
            max_adverse_price=?,
            excursion_model=?,
            replay_status=?,
            root_cause=?,
            root_cause_confidence=?,
            quality_tags_json=?,
            direction_quality=?,
            entry_quality=?,
            sl_quality=?,
            tp_quality=?,
            time_exit_quality=?,
            evidence_json=?,
            updated_at=?
        WHERE diagnostic_id=?
        """,
        (
            row["MFE_R"],
            row["MAE_R"],
            row["MFE"],
            row["MAE"],
            row["max_favorable_price"],
            row["max_adverse_price"],
            row["excursion_model"],
            row["replay_status"],
            row["root_cause"],
            row["root_cause_confidence"],
            row["quality_tags_json"],
            row["direction_quality"],
            row["entry_quality"],
            row["sl_quality"],
            row["tp_quality"],
            row["time_exit_quality"],
            row["evidence_json"],
            row["updated_at"],
            row["diagnostic_id"],
        ),
    )


def _current_paper_diag_replay_ledger(
    row: dict[str, Any],
    status: str,
    reason: str,
    now: str,
    *,
    new_mfe_r: float | None = None,
    new_mae_r: float | None = None,
) -> dict[str, Any]:
    return {
        "diagnostic_id": row.get("diagnostic_id"),
        "trade_id": row.get("trade_id"),
        "legacy_sample_id": row.get("legacy_sample_id"),
        "source": row.get("source"),
        "archive_id": row.get("archive_id"),
        "symbol": row.get("symbol"),
        "strategy_line": row.get("strategy_line"),
        "side": row.get("side"),
        "replay_status": status,
        "replay_reason": reason,
        "old_replay_status": row.get("replay_status"),
        "old_MFE_R": row.get("MFE_R"),
        "new_MFE_R": new_mfe_r if new_mfe_r is not None else row.get("MFE_R"),
        "old_MAE_R": row.get("MAE_R"),
        "new_MAE_R": new_mae_r if new_mae_r is not None else row.get("MAE_R"),
        "schema_version": DIAGNOSTIC_REPLAY_SCHEMA_VERSION,
        "updated_at": now,
        "evidence_json": _json({"current_paper_replay": True}),
    }


def _upsert_current_paper_diag_replay_ledger(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO trade_quality_diagnostic_replay_ledger(
          diagnostic_id, trade_id, legacy_sample_id, source, archive_id, symbol, strategy_line, side,
          replay_status, replay_reason, old_replay_status, old_MFE_R, new_MFE_R,
          old_MAE_R, new_MAE_R, schema_version, updated_at, evidence_json
        ) VALUES(
          :diagnostic_id, :trade_id, :legacy_sample_id, :source, :archive_id, :symbol, :strategy_line, :side,
          :replay_status, :replay_reason, :old_replay_status, :old_MFE_R, :new_MFE_R,
          :old_MAE_R, :new_MAE_R, :schema_version, :updated_at, :evidence_json
        )
        ON CONFLICT(diagnostic_id) DO UPDATE SET
          replay_status=excluded.replay_status,
          replay_reason=excluded.replay_reason,
          old_replay_status=excluded.old_replay_status,
          old_MFE_R=excluded.old_MFE_R,
          new_MFE_R=excluded.new_MFE_R,
          old_MAE_R=excluded.old_MAE_R,
          new_MAE_R=excluded.new_MAE_R,
          schema_version=excluded.schema_version,
          updated_at=excluded.updated_at,
          evidence_json=excluded.evidence_json
        """,
        row,
    )


def diagnostic_sync_status_payload(project_root: Path, *, config: PaperConfig | None = None) -> dict[str, Any]:
    db_path = _db_path(project_root, config)
    ensure_diagnostic_tables(db_path)
    packages = diagnostic_archive_packages_payload(project_root, config=config)["packages"]
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        meta = {
            row["meta_key"]: _loads(row["meta_value"], row["meta_value"])
            for row in conn.execute("SELECT meta_key, meta_value FROM trade_quality_diagnostic_sync_meta").fetchall()
        }
        sample_count = int(conn.execute("SELECT count(*) FROM trade_quality_diagnostic_samples").fetchone()[0] or 0)
        replay_count = int(
            conn.execute(
                "SELECT count(*) FROM trade_quality_diagnostic_samples WHERE replay_status='candle_1m_replay'"
            ).fetchone()[0]
            or 0
        )
        latest_sample = conn.execute("SELECT max(updated_at) FROM trade_quality_diagnostic_samples").fetchone()[0]
    coverage = _ratio(replay_count, sample_count)
    stale = sample_count == 0 or not meta.get("last_synced_at")
    return {
        "schema_version": DIAGNOSTIC_API_SCHEMA_VERSION,
        "last_synced_at": meta.get("last_synced_at") or latest_sample,
        "sync_source": meta.get("sync_source") or "unknown",
        "sync_scope": meta.get("sync_scope") or "unknown",
        "sample_count": sample_count,
        "archive_package_count": len(packages),
        "replay_coverage": coverage,
        "stale": stale,
        "stale_reason": "no_diagnostic_sync_meta" if stale else None,
        "next_recommended_action": "run_diagnostic_sync" if stale else "read_cache",
    }


def diagnostic_archive_packages_payload(project_root: Path, *, config: PaperConfig | None = None) -> dict[str, Any]:
    db_path = _db_path(project_root, config)
    ensure_diagnostic_tables(db_path)
    archive_root = project_root.resolve() / "DATA" / "paper" / "archives"
    dirs = sorted((p for p in archive_root.glob("paper_exp_*") if p.is_dir()), key=lambda p: p.name, reverse=True) if archive_root.exists() else []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        diag_rows = {
            row["archive_id"]: dict(row)
            for row in conn.execute(
                """
                SELECT archive_id,
                       count(*) AS diagnostic_sample_count,
                       sum(CASE WHEN replay_status='candle_1m_replay' THEN 1 ELSE 0 END) AS replay_count,
                       sum(CASE WHEN replay_status!='candle_1m_replay' OR replay_status IS NULL THEN 1 ELSE 0 END) AS proxy_or_missing_count,
                       max(updated_at) AS last_synced_at
                FROM trade_quality_diagnostic_samples
                WHERE archive_id IS NOT NULL
                GROUP BY archive_id
                """
            ).fetchall()
        }
    packages: list[dict[str, Any]] = []
    for path in dirs:
        archive_id = path.name
        metadata = _read_archive_metadata(path)
        strategy_line = _strategy_from_archive_id(archive_id, metadata)
        closed_count = _closed_order_count(path, metadata)
        diag = diag_rows.get(archive_id, {})
        diagnostic_count = int(diag.get("diagnostic_sample_count") or 0)
        replay_count = int(diag.get("replay_count") or 0)
        proxy_count = int(diag.get("proxy_or_missing_count") or 0)
        coverage = _ratio(replay_count, diagnostic_count)
        stale = diagnostic_count == 0 or (closed_count > 0 and diagnostic_count < closed_count)
        packages.append(
            {
                "archive_id": archive_id,
                "archive_path": str(path),
                "strategy_line": strategy_line,
                "created_at": metadata.get("archived_at") or _mtime_iso(path),
                "closed_order_count": closed_count,
                "diagnostic_sample_count": diagnostic_count,
                "replay_count": replay_count,
                "proxy_or_missing_count": proxy_count,
                "replay_coverage": coverage,
                "last_synced_at": diag.get("last_synced_at"),
                "stale": stale,
                "stale_reason": "not_synced" if diagnostic_count == 0 else ("partial_sync" if stale else None),
            }
        )
    return {
        "schema_version": DIAGNOSTIC_API_SCHEMA_VERSION,
        "count": len(packages),
        "packages": packages,
    }


def diagnostic_samples_payload(
    db_path: Path,
    *,
    source: str | None = None,
    archive_id: str | None = None,
    strategy_line: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    exit_reason: str | None = None,
    root_cause: str | None = None,
    quality_tag: str | None = None,
    replay_status: str | None = None,
    entry_quality_label: str | None = None,
    entry_quality_v2_label: str | None = None,
    market_context_label: str | None = None,
    market_context_status: str | None = None,
    entry_context_v3_label: str | None = None,
    funding_regime: str | None = None,
    oi_direction: str | None = None,
    btc_alignment: str | None = None,
    microstructure_coverage: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    ensure_diagnostic_tables(db_path)
    epoch_scope = _current_paper_epoch_scope_payload(db_path) if _normalize_source(source) == "current_paper" else None
    rows, total = _query_samples(
        db_path,
        source=source,
        archive_id=archive_id,
        strategy_line=strategy_line,
        symbol=symbol,
        side=side,
        exit_reason=exit_reason,
        root_cause=root_cause,
        quality_tag=quality_tag,
        replay_status=replay_status,
        entry_quality_label=entry_quality_label,
        entry_quality_v2_label=entry_quality_v2_label,
        market_context_label=market_context_label,
        market_context_status=market_context_status,
        entry_context_v3_label=entry_context_v3_label,
        funding_regime=funding_regime,
        oi_direction=oi_direction,
        btc_alignment=btc_alignment,
        microstructure_coverage=microstructure_coverage,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )
    return {
        "schema_version": DIAGNOSTIC_API_SCHEMA_VERSION,
        "count": len(rows),
        "total": total,
        "limit": _safe_limit(limit),
        "offset": max(0, int(offset or 0)),
        "paper_epoch_scope": epoch_scope,
        "filters": {
            "source": source,
            "archive_id": archive_id,
            "strategy_line": strategy_line,
            "symbol": symbol,
            "side": side,
            "exit_reason": exit_reason,
            "root_cause": root_cause,
            "quality_tag": quality_tag,
            "replay_status": replay_status,
            "entry_quality_label": entry_quality_label,
            "entry_quality_v2_label": entry_quality_v2_label,
            "market_context_label": market_context_label,
            "market_context_status": market_context_status,
            "entry_context_v3_label": entry_context_v3_label,
            "funding_regime": funding_regime,
            "oi_direction": oi_direction,
            "btc_alignment": btc_alignment,
            "microstructure_coverage": microstructure_coverage,
            "date_from": date_from,
            "date_to": date_to,
        },
        "samples": rows,
    }


def diagnostic_summary_payload(db_path: Path, **filters: Any) -> dict[str, Any]:
    ensure_diagnostic_tables(db_path)
    rows, total = _query_samples(db_path, limit=5000, offset=0, **filters)
    source = _normalize_source(filters.get("source"))
    epoch_scope = _current_paper_epoch_scope_payload(db_path) if source == "current_paper" else None
    replay_rows = [row for row in rows if str(row.get("replay_status") or "") == "candle_1m_replay"]
    r_values = [_num(row.get("net_R")) for row in rows if row.get("net_R") is not None]
    mfe_values = [_num(row.get("MFE_R")) for row in rows if row.get("MFE_R") is not None]
    mae_values = [_num(row.get("MAE_R")) for row in rows if row.get("MAE_R") is not None]
    root_counts = Counter(str(row.get("root_cause") or "unknown") for row in rows)
    replay_counts = Counter(str(row.get("replay_status") or "unknown") for row in rows)
    source_counts = Counter(str(row.get("source") or "unknown") for row in rows)
    phenomena = _phenomenon_counts(replay_rows)
    performance_stats = _performance_stats(rows)
    root_cause_attribution = _root_cause_attribution(rows)
    dimension_attribution = _dimension_attribution(rows)
    entry_quality_attribution = _entry_quality_attribution(rows)
    entry_microstructure_attribution = _entry_microstructure_attribution(rows)
    entry_market_context_attribution = _entry_market_context_attribution(rows)
    entry_context_v3_attribution = _entry_context_v3_attribution(rows)
    return {
        "schema_version": DIAGNOSTIC_API_SCHEMA_VERSION,
        "payload_kind": "summary_only",
        "total": total,
        "summary": {
            "sample_count": len(rows),
            "phenomenon_sample_count": len(replay_rows),
            "phenomenon_scope": "candle_1m_replay_only",
            "phenomenon_replay_required": bool(rows and len(replay_rows) < len(rows)),
            "source_counts": dict(source_counts),
            "win_rate": _ratio(sum(1 for value in r_values if value > 0), len(r_values)),
            "total_net_R": round(sum(r_values), 8),
            "avg_net_R": _avg(r_values),
            "median_net_R": _median(r_values),
            "avg_MFE_R": _avg(mfe_values),
            "avg_MAE_R": _avg(mae_values),
            "replay_status_counts": dict(replay_counts),
            "replay_coverage": _ratio(replay_counts.get("candle_1m_replay", 0), len(rows)),
            "root_cause_counts": dict(root_counts),
            "phenomena": phenomena,
            "performance_stats": performance_stats,
            "root_cause_attribution": root_cause_attribution,
            "dimension_attribution": dimension_attribution,
            "entry_quality_attribution": entry_quality_attribution,
            "entry_microstructure_attribution": entry_microstructure_attribution,
            "entry_market_context_attribution": entry_market_context_attribution,
            "entry_micro_context_attribution": entry_microstructure_attribution,
            "entry_context_v3_attribution": entry_context_v3_attribution,
            "trade_count": performance_stats["trade_count"],
            "win_count": performance_stats["win_count"],
            "loss_count": performance_stats["loss_count"],
            "flat_count": performance_stats["flat_count"],
            "loss_rate": performance_stats["loss_rate"],
            "avg_win_R": performance_stats["avg_win_R"],
            "avg_loss_R": performance_stats["avg_loss_R"],
            "profit_loss_ratio": performance_stats["profit_loss_ratio"],
            "expectancy_R": performance_stats["expectancy_R"],
            "max_drawdown_R": performance_stats["max_drawdown_R"],
            "max_losing_streak": performance_stats["max_losing_streak"],
            "losing_streak_distribution": performance_stats["losing_streak_distribution"],
            "fee_total": performance_stats["fee_total"],
            "gross_profit_usdt": performance_stats["gross_profit_usdt"],
            "fee_to_gross_profit_ratio": performance_stats["fee_to_gross_profit_ratio"],
            "avg_holding_minutes": performance_stats["avg_holding_minutes"],
            "median_holding_minutes": performance_stats["median_holding_minutes"],
            "paper_epoch_scope": epoch_scope,
            "active_strategy_epochs": (epoch_scope or {}).get("active_strategy_epochs") or [],
            "excluded_stale_current_paper_samples": (epoch_scope or {}).get("excluded_stale_current_paper_samples") or 0,
            "stale_current_paper_warning": (epoch_scope or {}).get("stale_current_paper_warning") or "",
        },
    }


def diagnostic_aggregates_payload(db_path: Path, **filters: Any) -> dict[str, Any]:
    ensure_diagnostic_tables(db_path)
    rows, _ = _query_samples(db_path, limit=5000, offset=0, **filters)
    dimensions = {
        "source": lambda r: r.get("source") or "unknown",
        "strategy_line": lambda r: r.get("strategy_line") or "unknown",
        "side": lambda r: r.get("side") or "unknown",
        "root_cause": lambda r: r.get("root_cause") or "unknown",
        "entry_quality_v2_label": lambda r: (r.get("entry_microstructure") or {}).get("entry_quality_v2_label") or "unknown",
        "market_context_label": lambda r: (r.get("entry_market_context") or {}).get("market_context_label") or "unknown",
        "entry_context_v3_label": lambda r: (r.get("entry_context_v3") or {}).get("entry_context_v3_label") or "unknown",
        "funding_regime": lambda r: (r.get("entry_market_context") or {}).get("funding_regime") or "unknown",
        "btc_alignment": lambda r: (r.get("entry_market_context") or {}).get("btc_alignment") or "unknown",
        "exit_reason": lambda r: r.get("exit_reason") or "unknown",
        "symbol": lambda r: r.get("symbol") or "unknown",
        "quality_tag": None,
    }
    aggregates: list[dict[str, Any]] = []
    for dimension, getter in dimensions.items():
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        if dimension == "quality_tag":
            for row in rows:
                for tag in row.get("quality_tags") or ["none"]:
                    groups[str(tag)].append(row)
        else:
            for row in rows:
                groups[str(getter(row))].append(row)  # type: ignore[misc]
        for key, items in groups.items():
            aggregates.append(_aggregate_row(dimension, key, items))
    return {
        "schema_version": DIAGNOSTIC_API_SCHEMA_VERSION,
        "count": len(aggregates),
        "aggregates": sorted(aggregates, key=lambda x: (x["dimension"], -x["sample_count"], x["key"])),
    }


def diagnostic_sample_detail_payload(db_path: Path, trade_id: str) -> dict[str, Any]:
    ensure_diagnostic_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM trade_quality_diagnostic_samples
            WHERE trade_id=? OR diagnostic_id=? OR order_id=?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (trade_id, trade_id, trade_id),
        ).fetchone()
    sample = _decode_diag_row(dict(row)) if row else None
    if sample:
        _attach_entry_features(db_path, [sample])
        _attach_entry_microstructures(db_path, [sample])
        _attach_entry_market_contexts(db_path, [sample])
        _attach_entry_context_v3(db_path, [sample])
    return {
        "schema_version": DIAGNOSTIC_API_SCHEMA_VERSION,
        "sample": sample,
    }


def diagnostic_replay_ledger_payload(db_path: Path, *, limit: int = 200) -> dict[str, Any]:
    ensure_diagnostic_tables(db_path)
    safe_limit = _safe_limit(limit)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM trade_quality_diagnostic_replay_ledger
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
    decoded = []
    for row in rows:
        item = dict(row)
        item["evidence"] = _loads(item.pop("evidence_json", None), {})
        decoded.append(item)
    return {
        "schema_version": DIAGNOSTIC_REPLAY_SCHEMA_VERSION,
        "count": len(decoded),
        "ledger": decoded,
    }


def _db_path(project_root: Path, config: PaperConfig | None) -> Path:
    root = project_root.resolve()
    cfg = config or PaperConfig()
    return root / cfg.db_path


def _build_diagnostic_rows(
    db_path: Path,
    *,
    source: str,
    archive_id: str | None = None,
    limit: int | None,
) -> list[dict[str, Any]]:
    rows = []
    if source in {"all", "legacy_p18", "archive"}:
        rows.extend(_legacy_p18_rows(db_path, source=source, archive_id=archive_id))
    if source in {"all", "current_paper"}:
        rows.extend(_current_paper_rows(db_path))
    rows = _dedup_rows(rows)
    rows.sort(key=lambda r: str(r.get("exit_time") or r.get("entry_time") or ""), reverse=True)
    if limit is not None:
        rows = rows[: max(0, int(limit))]
    return rows


def _legacy_p18_rows(db_path: Path, *, source: str, archive_id: str | None = None) -> list[dict[str, Any]]:
    if not _table_exists(db_path, "trade_quality_samples"):
        return []
    clauses: list[str] = []
    params: list[Any] = []
    if source == "archive":
        clauses.append(
            "sample_id IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
        )
        if archive_id:
            clauses.append(
                "sample_id IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE archive_path LIKE ?)"
            )
            params.append(f"%{archive_id}%")
    elif source == "current_paper":
        clauses.append(
            "sample_id NOT IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
        )
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        samples = [dict(row) for row in conn.execute(f"SELECT * FROM trade_quality_samples {where}", params).fetchall()]
        ledgers = {
            str(row["sample_id"]): dict(row)
            for row in conn.execute(
                "SELECT * FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted'"
            ).fetchall()
        } if _table_exists(db_path, "trade_quality_archive_ingest_ledger") else {}
    out = []
    for row in samples:
        ledger = ledgers.get(str(row.get("sample_id") or ""))
        src = "archive" if ledger else "legacy_p18"
        if source == "legacy_p18" and src != "legacy_p18":
            continue
        out.append(_diagnostic_from_p18(row, ledger))
    return out


def _current_paper_rows(db_path: Path) -> list[dict[str, Any]]:
    if not _table_exists(db_path, "paper_orders"):
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        epochs = _current_paper_epoch_map(conn)
        rows = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM paper_orders WHERE status='closed' ORDER BY COALESCE(closed_at, updated_at, created_at) DESC"
            ).fetchall()
        ]
    active_rows = [row for row in rows if _paper_order_in_active_epoch(row, epochs)]
    return [_diagnostic_from_paper_order(row, epochs) for row in active_rows]


def _current_paper_epoch_map(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='paper_reset_epochs'").fetchone()
    if not exists:
        return {}
    conn.row_factory = sqlite3.Row
    out: dict[str, dict[str, Any]] = {}
    for row in conn.execute("SELECT * FROM paper_reset_epochs").fetchall():
        item = dict(row)
        line = str(item.get("strategy_line") or "")
        if line:
            out[line] = item
    return out


def _paper_order_in_active_epoch(row: dict[str, Any], epochs: dict[str, dict[str, Any]]) -> bool:
    line = str(row.get("strategy_line") or "")
    latest = epochs.get(line)
    order_epoch = str(row.get("reset_epoch_id") or "")
    if latest:
        return order_epoch == str(latest.get("reset_epoch_id") or "")
    return order_epoch == ""


def _paper_epoch_scope_for_line(line: str, epoch: dict[str, Any] | None) -> dict[str, Any]:
    reset_epoch_id = str((epoch or {}).get("reset_epoch_id") or "")
    suffix = reset_epoch_id or "no_epoch"
    return {
        "strategy_line": line,
        "paper_reset_epoch_id": reset_epoch_id or None,
        "paper_experiment_id": (epoch or {}).get("experiment_id"),
        "paper_epoch_reset_at": (epoch or {}).get("reset_at"),
        "paper_epoch_scope_key": f"current_paper:{line}:{suffix}",
    }


def _current_paper_epoch_scope_payload(db_path: Path) -> dict[str, Any]:
    active: list[dict[str, Any]] = []
    stale = 0
    if db_path.exists():
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            epochs = _current_paper_epoch_map(conn)
            for line in STRATEGY_LINES:
                active.append(_paper_epoch_scope_for_line(line, epochs.get(line)))
            if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='trade_quality_diagnostic_samples'").fetchone():
                active_clause, params = _current_paper_active_sql_clause(db_path)
                where = "source='current_paper'"
                if active_clause:
                    where += f" AND NOT ({active_clause})"
                stale = int(conn.execute(f"SELECT count(*) FROM trade_quality_diagnostic_samples WHERE {where}", params).fetchone()[0] or 0)
    else:
        active = [_paper_epoch_scope_for_line(line, None) for line in STRATEGY_LINES]
    return {
        "scope_version": "19.24",
        "source": "current_paper",
        "active_strategy_epochs": active,
        "active_scope_keys": [str(row.get("paper_epoch_scope_key") or "") for row in active],
        "active_package_key": _current_paper_active_package_key({"active_strategy_epochs": active}),
        "excluded_stale_current_paper_samples": stale,
        "stale_current_paper_warning": "current_paper_epoch_scope_excluded_stale_samples" if stale else "",
    }


def _current_paper_active_package_key(scope: dict[str, Any] | None) -> str:
    keys = sorted(str(row.get("paper_epoch_scope_key") or "") for row in (scope or {}).get("active_strategy_epochs", []) if row.get("paper_epoch_scope_key"))
    digest = hashlib.sha256("|".join(keys).encode("utf-8")).hexdigest()[:12] if keys else "no_scope"
    return f"source:current_paper:active:{digest}"


def _current_paper_active_sql_clause(db_path: Path, *, alias: str | None = None) -> tuple[str, list[Any]]:
    if not db_path.exists():
        return "", []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        epochs = _current_paper_epoch_map(conn)
    prefix = f"{alias}." if alias else ""
    parts: list[str] = []
    params: list[Any] = []
    for line in STRATEGY_LINES:
        epoch = epochs.get(line)
        if epoch:
            parts.append(f"({prefix}strategy_line=? AND COALESCE({prefix}paper_reset_epoch_id, '')=?)")
            params.extend([line, str(epoch.get("reset_epoch_id") or "")])
        else:
            parts.append(f"({prefix}strategy_line=? AND COALESCE({prefix}paper_reset_epoch_id, '')='')")
            params.append(line)
    if not parts:
        return "", []
    return f"({' OR '.join(parts)})", params


def _diagnostic_from_p18(row: dict[str, Any], ledger: dict[str, Any] | None) -> dict[str, Any]:
    source = "archive" if ledger else "legacy_p18"
    evidence = _loads(row.get("root_cause_evidence_json"), {})
    source_archive_path = str(ledger.get("archive_path") or "") if ledger else None
    archive_id = _archive_id(source_archive_path)
    trade_id = str(row.get("order_id") or row.get("sample_id") or "")
    entry_price = _num(row.get("entry_price"), None)  # type: ignore[arg-type]
    exit_price = _num(row.get("exit_price"), None)  # type: ignore[arg-type]
    qty = _num(row.get("quantity"))
    initial_risk = _num(row.get("initial_risk_usdt"))
    mfe_r = _none_or_float(row.get("MFE_R"))
    mae_r = _none_or_float(row.get("MAE_R"))
    mfe_abs = mfe_r * initial_risk if mfe_r is not None and initial_risk > 0 else None
    mae_abs = mae_r * initial_risk if mae_r is not None and initial_risk > 0 else None
    root = _diagnose(row)
    return {
        "diagnostic_id": _diagnostic_id(source, trade_id, row.get("symbol"), row.get("side"), row.get("opened_at"), row.get("closed_at")),
        "trade_id": trade_id,
        "order_id": row.get("order_id"),
        "legacy_sample_id": row.get("sample_id"),
        "source": source,
        "archive_id": archive_id,
        "archive_path": source_archive_path,
        "run_id": row.get("source_run_id"),
        "cycle_id": row.get("source_cycle_id"),
        "strategy_line": row.get("strategy_line"),
        "symbol": str(row.get("symbol") or "").upper(),
        "side": _side(row.get("side")),
        "entry_time": row.get("opened_at"),
        "exit_time": row.get("closed_at"),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "holding_minutes": _holding_minutes(row.get("opened_at"), row.get("closed_at"), row.get("holding_sec")),
        "entry_type": evidence.get("entry_type") or evidence.get("order_type") or "market",
        "exit_reason": str(row.get("exit_reason") or "unknown"),
        "gross_pnl": _none_or_float(row.get("gross_pnl_usdt")),
        "fee": _none_or_float(row.get("fee_usdt")),
        "slippage_cost": _none_or_float(row.get("slippage_usdt")),
        "net_pnl": _none_or_float(row.get("net_pnl_usdt")),
        "initial_risk_usdt": initial_risk,
        "net_R": _none_or_float(row.get("net_R")),
        "planned_SL": _none_or_float(row.get("stop_loss")),
        "planned_TP": _none_or_float(row.get("take_profit")),
        "planned_RR": _none_or_float(row.get("planned_RR")),
        "MFE": mfe_abs,
        "MAE": mae_abs,
        "MFE_R": mfe_r,
        "MAE_R": mae_r,
        "time_to_MFE_minutes": evidence.get("time_to_MFE_minutes"),
        "time_to_MAE_minutes": evidence.get("time_to_MAE_minutes"),
        "max_favorable_price": _max_favorable_price(entry_price, qty, mfe_abs, row.get("side")),
        "max_adverse_price": _max_adverse_price(entry_price, qty, mae_abs, row.get("side")),
        "excursion_model": row.get("excursion_model"),
        "replay_status": "candle_1m_replay" if row.get("excursion_model") == "candle_1m_replay" else "proxy_or_missing",
        **root,
        "diagnostic_version": DIAGNOSTIC_LABEL_VERSION,
        "evidence": {
            **evidence,
            "source_schema": row.get("sample_schema_version"),
            "label_schema": row.get("label_schema_version"),
            "legacy_root_cause": row.get("root_cause_label"),
            "legacy_sample_id": row.get("sample_id"),
            "archive_dedup_key": ledger.get("dedup_key") if ledger else None,
        },
    }


def _diagnostic_from_paper_order(row: dict[str, Any], epochs: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    entry = _num(row.get("filled_entry_price") or row.get("entry_price"))
    exit_price = _num(row.get("exit_price"))
    qty = _num(row.get("quantity"))
    gross_initial_risk = abs(entry - _num(row.get("stop_loss"))) * qty if entry and qty else 0.0
    planned_net_loss = _num(row.get("estimated_max_loss_usdt"))
    initial_risk = planned_net_loss if planned_net_loss > 0 else gross_initial_risk
    net_pnl = _num(row.get("realized_pnl_usdt")) - _num(row.get("fee_usdt")) - _num(row.get("slippage_usdt"))
    net_r = net_pnl / initial_risk if initial_risk > 0 else None
    planned_rr = _planned_rr(entry, row.get("stop_loss"), row.get("take_profit"))
    base = {
        "exit_reason": row.get("exit_reason"),
        "net_R": net_r,
        "MFE_R": None,
        "MAE_R": None,
        "planned_RR": planned_rr,
        "excursion_model": "missing_1m_replay",
    }
    root = _diagnose(base)
    line = str(row.get("strategy_line") or "")
    epoch_scope = _paper_epoch_scope_for_line(line, (epochs or {}).get(line))
    return {
        "diagnostic_id": _diagnostic_id("current_paper", row.get("id"), row.get("symbol"), row.get("side"), row.get("opened_at"), row.get("closed_at")),
        "trade_id": str(row.get("id") or ""),
        "order_id": row.get("id"),
        "legacy_sample_id": None,
        "source": "current_paper",
        "archive_id": None,
        "archive_path": None,
        "paper_reset_epoch_id": epoch_scope.get("paper_reset_epoch_id"),
        "paper_experiment_id": epoch_scope.get("paper_experiment_id"),
        "paper_epoch_reset_at": epoch_scope.get("paper_epoch_reset_at"),
        "paper_epoch_scope_key": epoch_scope.get("paper_epoch_scope_key"),
        "run_id": row.get("source_run_id"),
        "cycle_id": row.get("source_cycle_id"),
        "strategy_line": row.get("strategy_line"),
        "symbol": str(row.get("symbol") or "").upper(),
        "side": _side(row.get("side")),
        "entry_time": row.get("opened_at"),
        "exit_time": row.get("closed_at"),
        "entry_price": entry,
        "exit_price": exit_price,
        "holding_minutes": _holding_minutes(row.get("opened_at"), row.get("closed_at"), None),
        "entry_type": str(row.get("source_entry_mode") or row.get("order_type") or "market").lower(),
        "exit_reason": str(row.get("exit_reason") or "unknown"),
        "gross_pnl": _none_or_float(row.get("realized_pnl_usdt")),
        "fee": _none_or_float(row.get("fee_usdt")),
        "slippage_cost": _none_or_float(row.get("slippage_usdt")),
        "net_pnl": net_pnl,
        "initial_risk_usdt": initial_risk,
        "net_R": base["net_R"],
        "planned_SL": _none_or_float(row.get("stop_loss")),
        "planned_TP": _none_or_float(row.get("take_profit")),
        "planned_RR": planned_rr,
        "MFE": None,
        "MAE": None,
        "MFE_R": None,
        "MAE_R": None,
        "time_to_MFE_minutes": None,
        "time_to_MAE_minutes": None,
        "max_favorable_price": None,
        "max_adverse_price": None,
        "excursion_model": "missing_1m_replay",
        "replay_status": "missing_1m_replay",
        **root,
        "diagnostic_version": DIAGNOSTIC_LABEL_VERSION,
        "evidence": {
            "source_plan_hash": row.get("source_plan_hash"),
            "source": "paper_orders",
            "paper_reset_epoch_id": epoch_scope.get("paper_reset_epoch_id"),
            "paper_experiment_id": epoch_scope.get("paper_experiment_id"),
            "paper_epoch_reset_at": epoch_scope.get("paper_epoch_reset_at"),
            "paper_epoch_scope_key": epoch_scope.get("paper_epoch_scope_key"),
            "paper_r_basis": "planned_net_loss" if planned_net_loss > 0 else "gross_stop",
            "planned_initial_risk_usdt": initial_risk,
            "gross_initial_risk_usdt": gross_initial_risk,
            "estimated_max_loss_usdt": planned_net_loss,
            "actual_loss_R": abs(net_r) if net_r is not None and net_r < 0 else None,
            "actual_reward_R": net_r if net_r is not None and net_r > 0 else None,
            "stop_overrun_R": (
                abs(net_r) - 1.0
                if net_r is not None and net_r < -1.0
                else 0.0 if net_r is not None and net_r < 0 else None
            ),
        },
    }


def _diagnose(row: dict[str, Any]) -> dict[str, Any]:
    mfe = _none_or_float(row.get("MFE_R"))
    mae = _none_or_float(row.get("MAE_R"))
    net_r = _none_or_float(row.get("net_R"))
    planned_rr = _none_or_float(row.get("planned_RR"))
    exit_reason = str(row.get("exit_reason") or "").upper()
    tags: list[str] = []
    direction = "unknown"
    entry = "unknown"
    sl = "unknown"
    tp = "unknown"
    time_exit = "unknown"
    root = "needs_replay"
    confidence = 0.35
    evidence: dict[str, Any] = {"mfe_R": mfe, "mae_R": mae, "net_R": net_r, "planned_RR": planned_rr, "exit_reason": exit_reason}
    if mfe is None or mae is None:
        tags.append("missing_mfe_mae")
    elif mfe < 0.3:
        root = "signal_no_edge"
        direction = "weak"
        tags.append("mfe_lt_0.3")
        confidence = 0.82
        if mae >= 0.8:
            tags.append("immediate_adverse")
            root = "direction_wrong"
            direction = "bad"
            confidence = 0.86
    elif mae > 0.6 and mfe > 0.8:
        root = "entered_too_early"
        entry = "early"
        tags.append("mae_gt_0.6_then_recovered")
        confidence = 0.78
    elif mfe >= 0.8 and net_r is not None and net_r <= 0:
        root = "exit_too_late"
        time_exit = "late"
        tags.append("mfe_gte_0.8_final_loss")
        confidence = 0.8
        if exit_reason == "SL":
            root = "stop_too_tight"
            sl = "too_tight_or_bad_entry"
            tags.append("sl_after_price_favorable")
            confidence = 0.82
    elif planned_rr is not None and planned_rr >= 1.2 and 0.3 <= mfe <= 0.7:
        root = "tp_too_far"
        tp = "too_far"
        tags.append("tp_unrealistic")
        confidence = 0.74
    elif net_r is not None and net_r > 0:
        root = "profitable_trade"
        direction = "ok"
        entry = "ok"
        tags.append("net_R_positive")
        confidence = 0.7
    else:
        root = "loss_unclassified"
        tags.append("needs_manual_review")
    return {
        "root_cause": root,
        "root_cause_confidence": confidence,
        "quality_tags": tags,
        "direction_quality": direction,
        "entry_quality": entry,
        "sl_quality": sl,
        "tp_quality": tp,
        "time_exit_quality": time_exit,
        "diagnostic_evidence": evidence,
    }


def _upsert_diagnostic_sample(conn: sqlite3.Connection, row: dict[str, Any], now: str) -> None:
    payload = _db_row(row, now)
    conn.execute(
        """
        INSERT INTO trade_quality_diagnostic_samples(
          diagnostic_id, trade_id, order_id, legacy_sample_id, source, archive_id, archive_path,
          paper_reset_epoch_id, paper_experiment_id, paper_epoch_reset_at, paper_epoch_scope_key,
          run_id, cycle_id, strategy_line, symbol, side, entry_time, exit_time, entry_price,
          exit_price, holding_minutes, entry_type, exit_reason, gross_pnl, fee, slippage_cost,
          net_pnl, initial_risk_usdt, net_R, planned_SL, planned_TP, planned_RR, MFE, MAE,
          MFE_R, MAE_R, time_to_MFE_minutes, time_to_MAE_minutes, max_favorable_price,
          max_adverse_price, excursion_model, replay_status, root_cause, root_cause_confidence,
          quality_tags_json, direction_quality, entry_quality, sl_quality, tp_quality,
          time_exit_quality, diagnostic_version, evidence_json, created_at, updated_at
        ) VALUES(
          :diagnostic_id, :trade_id, :order_id, :legacy_sample_id, :source, :archive_id, :archive_path,
          :paper_reset_epoch_id, :paper_experiment_id, :paper_epoch_reset_at, :paper_epoch_scope_key,
          :run_id, :cycle_id, :strategy_line, :symbol, :side, :entry_time, :exit_time, :entry_price,
          :exit_price, :holding_minutes, :entry_type, :exit_reason, :gross_pnl, :fee, :slippage_cost,
          :net_pnl, :initial_risk_usdt, :net_R, :planned_SL, :planned_TP, :planned_RR, :MFE, :MAE,
          :MFE_R, :MAE_R, :time_to_MFE_minutes, :time_to_MAE_minutes, :max_favorable_price,
          :max_adverse_price, :excursion_model, :replay_status, :root_cause, :root_cause_confidence,
          :quality_tags_json, :direction_quality, :entry_quality, :sl_quality, :tp_quality,
          :time_exit_quality, :diagnostic_version, :evidence_json, :created_at, :updated_at
        )
        ON CONFLICT(diagnostic_id) DO UPDATE SET
          source=excluded.source,
          archive_id=excluded.archive_id,
          archive_path=excluded.archive_path,
          paper_reset_epoch_id=excluded.paper_reset_epoch_id,
          paper_experiment_id=excluded.paper_experiment_id,
          paper_epoch_reset_at=excluded.paper_epoch_reset_at,
          paper_epoch_scope_key=excluded.paper_epoch_scope_key,
          run_id=excluded.run_id,
          cycle_id=excluded.cycle_id,
          strategy_line=excluded.strategy_line,
          entry_price=excluded.entry_price,
          exit_price=excluded.exit_price,
          holding_minutes=excluded.holding_minutes,
          exit_reason=excluded.exit_reason,
          gross_pnl=excluded.gross_pnl,
          fee=excluded.fee,
          slippage_cost=excluded.slippage_cost,
          net_pnl=excluded.net_pnl,
          initial_risk_usdt=excluded.initial_risk_usdt,
          net_R=excluded.net_R,
          planned_SL=excluded.planned_SL,
          planned_TP=excluded.planned_TP,
          planned_RR=excluded.planned_RR,
          MFE=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.MFE
            ELSE excluded.MFE
          END,
          MAE=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.MAE
            ELSE excluded.MAE
          END,
          MFE_R=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.MFE_R
            ELSE excluded.MFE_R
          END,
          MAE_R=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.MAE_R
            ELSE excluded.MAE_R
          END,
          excursion_model=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.excursion_model
            ELSE excluded.excursion_model
          END,
          replay_status=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.replay_status
            ELSE excluded.replay_status
          END,
          root_cause=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.root_cause
            ELSE excluded.root_cause
          END,
          root_cause_confidence=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.root_cause_confidence
            ELSE excluded.root_cause_confidence
          END,
          quality_tags_json=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.quality_tags_json
            ELSE excluded.quality_tags_json
          END,
          direction_quality=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.direction_quality
            ELSE excluded.direction_quality
          END,
          entry_quality=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.entry_quality
            ELSE excluded.entry_quality
          END,
          sl_quality=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.sl_quality
            ELSE excluded.sl_quality
          END,
          tp_quality=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.tp_quality
            ELSE excluded.tp_quality
          END,
          time_exit_quality=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.time_exit_quality
            ELSE excluded.time_exit_quality
          END,
          diagnostic_version=excluded.diagnostic_version,
          evidence_json=CASE
            WHEN trade_quality_diagnostic_samples.replay_status='candle_1m_replay'
              AND COALESCE(excluded.replay_status, '')!='candle_1m_replay'
            THEN trade_quality_diagnostic_samples.evidence_json
            ELSE excluded.evidence_json
          END,
          updated_at=excluded.updated_at
        """,
        payload,
    )


def _upsert_ingest_ledger(conn: sqlite3.Connection, row: dict[str, Any], now: str) -> None:
    dedup = _dedup_key(row)
    conn.execute(
        """
        INSERT INTO trade_quality_diagnostic_ingest_ledger(
          dedup_key, diagnostic_id, source, trade_id, order_id, legacy_sample_id,
          archive_id, strategy_line, symbol, side, ingest_status, skip_reason,
          schema_version, ingested_at, evidence_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'inserted', NULL, ?, ?, ?)
        ON CONFLICT(dedup_key) DO UPDATE SET
          diagnostic_id=excluded.diagnostic_id,
          ingest_status=excluded.ingest_status,
          schema_version=excluded.schema_version,
          ingested_at=excluded.ingested_at,
          evidence_json=excluded.evidence_json
        """,
        (
            dedup,
            row.get("diagnostic_id"),
            row.get("source"),
            row.get("trade_id"),
            row.get("order_id"),
            row.get("legacy_sample_id"),
            row.get("archive_id"),
            row.get("strategy_line"),
            row.get("symbol"),
            row.get("side"),
            DIAGNOSTIC_SCHEMA_VERSION,
            now,
            _json({"source": row.get("source"), "diagnostic_version": row.get("diagnostic_version")}),
        ),
    )


def _sync_replay_ledger(db_path: Path, *, limit: int | None = None, archive_id: str | None = None) -> None:
    if not _table_exists(db_path, "trade_quality_replay_backfill_ledger"):
        return
    sql = """
        SELECT d.*, l.replay_status AS legacy_replay_status, l.replay_reason,
               l.old_MFE_R, l.new_MFE_R, l.old_MAE_R, l.new_MAE_R, l.updated_at AS replay_updated_at
        FROM trade_quality_diagnostic_samples d
        JOIN trade_quality_replay_backfill_ledger l ON l.sample_id = d.legacy_sample_id
        WHERE (? IS NULL OR d.archive_id = ?)
        ORDER BY l.updated_at DESC
    """
    params: list[Any] = [archive_id, archive_id]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(max(0, int(limit)))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
        for row in rows:
            conn.execute(
                """
                INSERT INTO trade_quality_diagnostic_replay_ledger(
                  diagnostic_id, trade_id, legacy_sample_id, source, archive_id, symbol, strategy_line, side,
                  replay_status, replay_reason, old_replay_status, old_MFE_R, new_MFE_R,
                  old_MAE_R, new_MAE_R, schema_version, updated_at, evidence_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(diagnostic_id) DO UPDATE SET
                  replay_status=excluded.replay_status,
                  replay_reason=excluded.replay_reason,
                  archive_id=excluded.archive_id,
                  old_MFE_R=excluded.old_MFE_R,
                  new_MFE_R=excluded.new_MFE_R,
                  old_MAE_R=excluded.old_MAE_R,
                  new_MAE_R=excluded.new_MAE_R,
                  schema_version=excluded.schema_version,
                  updated_at=excluded.updated_at,
                  evidence_json=excluded.evidence_json
                """,
                (
                    row.get("diagnostic_id"),
                    row.get("trade_id"),
                    row.get("legacy_sample_id"),
                    row.get("source"),
                    row.get("archive_id"),
                    row.get("symbol"),
                    row.get("strategy_line"),
                    row.get("side"),
                    row.get("legacy_replay_status"),
                    row.get("replay_reason"),
                    row.get("replay_status"),
                    row.get("old_MFE_R"),
                    row.get("new_MFE_R"),
                    row.get("old_MAE_R"),
                    row.get("new_MAE_R"),
                    REPLAY_BACKFILL_SCHEMA_VERSION,
                    row.get("replay_updated_at") or utc_now_iso(),
                    _json({"legacy_replay_schema": REPLAY_BACKFILL_SCHEMA_VERSION}),
                ),
            )


def _upsert_sync_meta(conn: sqlite3.Connection, values: dict[str, Any], now: str) -> None:
    for key, value in values.items():
        conn.execute(
            """
            INSERT INTO trade_quality_diagnostic_sync_meta(meta_key, meta_value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(meta_key) DO UPDATE SET
              meta_value=excluded.meta_value,
              updated_at=excluded.updated_at
            """,
            (key, _json(value), now),
        )


def _read_archive_metadata(path: Path) -> dict[str, Any]:
    metadata_path = path / "metadata.json"
    if not metadata_path.exists():
        return {}
    try:
        data = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _strategy_from_archive_id(archive_id: str, metadata: dict[str, Any]) -> str:
    explicit = metadata.get("strategy_line") or metadata.get("line")
    if explicit:
        return str(explicit)
    for line in ("without_micro", "micro_fast", "micro_full"):
        if archive_id.endswith(line) or f"_{line}" in archive_id:
            return line
    return "unknown"


def _closed_order_count(path: Path, metadata: dict[str, Any]) -> int:
    forced = metadata.get("forced_close_rows")
    if isinstance(forced, list) and forced:
        return len(forced)
    orders_path = path / "orders.json"
    if not orders_path.exists():
        return 0
    try:
        data = json.loads(orders_path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    rows = data.get("orders") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        return 0
    return sum(1 for row in rows if str((row or {}).get("status") or "").upper() in {"CLOSED", "SL", "TP", "EXITED"})


def _mtime_iso(path: Path) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rebuild_aggregates(conn: sqlite3.Connection, rows: list[dict[str, Any]], now: str, *, package_key: str) -> None:
    conn.execute("DELETE FROM trade_quality_diagnostic_aggregates WHERE package_key=?", (package_key,))
    dimensions = {
        "source": lambda r: r.get("source") or "unknown",
        "strategy_line": lambda r: r.get("strategy_line") or "unknown",
        "side": lambda r: r.get("side") or "unknown",
        "root_cause": lambda r: r.get("root_cause") or "unknown",
        "symbol": lambda r: r.get("symbol") or "unknown",
    }
    for dimension, getter in dimensions.items():
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[str(getter(row))].append(row)
        for key, items in groups.items():
            agg = _aggregate_row(dimension, key, items)
            conn.execute(
                """
                INSERT INTO trade_quality_diagnostic_aggregates(
                  aggregate_id, package_key, dimension, key, sample_count, avg_net_R,
                  median_net_R, win_rate, avg_MFE_R, avg_MAE_R, evidence_json,
                  schema_version, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hashlib.sha256(f"{package_key}|{dimension}|{key}".encode("utf-8")).hexdigest()[:32],
                    package_key,
                    dimension,
                    key,
                    agg["sample_count"],
                    agg["avg_net_R"],
                    agg["median_net_R"],
                    agg["win_rate"],
                    agg["avg_MFE_R"],
                    agg["avg_MAE_R"],
                    _json(agg["evidence"]),
                    DIAGNOSTIC_SCHEMA_VERSION,
                    now,
                ),
            )


def _query_samples(db_path: Path, *, limit: int = 200, offset: int = 0, **filters: Any) -> tuple[list[dict[str, Any]], int]:
    clauses: list[str] = []
    params: list[Any] = []
    mapping = {
        "source": "source",
        "archive_id": "archive_id",
        "strategy_line": "strategy_line",
        "exit_reason": "exit_reason",
        "replay_status": "replay_status",
    }
    for key, col in mapping.items():
        value = filters.get(key)
        if value and str(value).lower() != "all":
            clauses.append(f"{col} = ?")
            params.append(str(value))
    if _normalize_source(filters.get("source")) == "current_paper":
        active_clause, active_params = _current_paper_active_sql_clause(db_path)
        if active_clause:
            clauses.append(active_clause)
            params.extend(active_params)
    root_value = filters.get("root_cause")
    if root_value and str(root_value).lower() != "all":
        normalized_root = _normalize_root_cause_label(str(root_value))
        if normalized_root == "direction_wrong":
            clauses.append("root_cause IN (?, ?)")
            params.extend(["direction_wrong", "direction_wrong_or_chase_tail"])
        else:
            clauses.append("root_cause = ?")
            params.append(str(root_value))
    if filters.get("symbol"):
        clauses.append("upper(symbol) = ?")
        params.append(str(filters["symbol"]).upper())
    if filters.get("side"):
        clauses.append("upper(side) = ?")
        params.append(_side(filters["side"]))
    if filters.get("date_from"):
        clauses.append("COALESCE(exit_time, entry_time) >= ?")
        params.append(str(filters["date_from"]))
    if filters.get("date_to"):
        clauses.append("COALESCE(exit_time, entry_time) <= ?")
        params.append(str(filters["date_to"]))
    if filters.get("quality_tag"):
        clauses.append("quality_tags_json LIKE ?")
        params.append(f"%{filters['quality_tag']}%")
    if filters.get("entry_quality_label"):
        clauses.append(
            "diagnostic_id IN (SELECT diagnostic_id FROM trade_quality_entry_feature_samples WHERE entry_quality_label = ?)"
        )
        params.append(str(filters["entry_quality_label"]))
    if filters.get("entry_quality_v2_label"):
        clauses.append(
            "diagnostic_id IN (SELECT diagnostic_id FROM trade_quality_entry_microstructure_samples WHERE entry_quality_v2_label = ?)"
        )
        params.append(str(filters["entry_quality_v2_label"]))
    if filters.get("microstructure_coverage"):
        clauses.append(
            "diagnostic_id IN (SELECT diagnostic_id FROM trade_quality_entry_microstructure_samples WHERE evidence_status = ?)"
        )
        params.append(str(filters["microstructure_coverage"]))
    if filters.get("market_context_label"):
        clauses.append(
            "diagnostic_id IN (SELECT diagnostic_id FROM trade_quality_entry_market_context_samples WHERE market_context_label = ?)"
        )
        params.append(str(filters["market_context_label"]))
    if filters.get("market_context_status"):
        clauses.append(
            "diagnostic_id IN (SELECT diagnostic_id FROM trade_quality_entry_market_context_samples WHERE market_context_status = ?)"
        )
        params.append(str(filters["market_context_status"]))
    if filters.get("entry_context_v3_label"):
        clauses.append(
            "diagnostic_id IN (SELECT diagnostic_id FROM trade_quality_entry_context_v3_samples WHERE entry_context_v3_label = ?)"
        )
        params.append(str(filters["entry_context_v3_label"]))
    if filters.get("funding_regime"):
        clauses.append(
            "diagnostic_id IN (SELECT diagnostic_id FROM trade_quality_entry_market_context_samples WHERE funding_regime = ?)"
        )
        params.append(str(filters["funding_regime"]))
    if filters.get("oi_direction"):
        clauses.append(
            "diagnostic_id IN (SELECT diagnostic_id FROM trade_quality_entry_market_context_samples WHERE oi_direction = ?)"
        )
        params.append(str(filters["oi_direction"]))
    if filters.get("btc_alignment"):
        clauses.append(
            "diagnostic_id IN (SELECT diagnostic_id FROM trade_quality_entry_market_context_samples WHERE btc_alignment = ?)"
        )
        params.append(str(filters["btc_alignment"]))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    safe_limit = _safe_limit(limit)
    safe_offset = max(0, int(offset or 0))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total = int(conn.execute(f"SELECT count(*) FROM trade_quality_diagnostic_samples {where}", params).fetchone()[0] or 0)
        rows = conn.execute(
            f"""
            SELECT * FROM trade_quality_diagnostic_samples
            {where}
            ORDER BY COALESCE(exit_time, entry_time, updated_at) DESC, diagnostic_id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, safe_limit, safe_offset],
        ).fetchall()
    decoded = [_decode_diag_row(dict(row)) for row in rows]
    _attach_entry_features(db_path, decoded)
    _attach_entry_microstructures(db_path, decoded)
    _attach_entry_market_contexts(db_path, decoded)
    _attach_entry_context_v3(db_path, decoded)
    return decoded, total


def _attach_entry_features(db_path: Path, rows: list[dict[str, Any]]) -> None:
    ids = [str(row.get("diagnostic_id") or "") for row in rows if row.get("diagnostic_id")]
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        feature_rows = conn.execute(
            f"SELECT * FROM trade_quality_entry_feature_samples WHERE diagnostic_id IN ({placeholders})",
            ids,
        ).fetchall()
    by_id: dict[str, dict[str, Any]] = {}
    for feature in feature_rows:
        item = dict(feature)
        item["evidence"] = _loads(item.pop("evidence_json", None), {})
        by_id[str(item.get("diagnostic_id"))] = item
    for row in rows:
        feature = by_id.get(str(row.get("diagnostic_id")))
        row["entry_features"] = feature or {}
        if feature:
            row["entry_quality_label"] = feature.get("entry_quality_label")
            row["entry_quality_score"] = feature.get("entry_quality_score")
            row["entry_feature_coverage"] = feature.get("entry_feature_coverage")
        else:
            row["entry_quality_score"] = None
            row["entry_feature_coverage"] = None


def _attach_entry_microstructures(db_path: Path, rows: list[dict[str, Any]]) -> None:
    ids = [str(row.get("diagnostic_id") or "") for row in rows if row.get("diagnostic_id")]
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        feature_rows = conn.execute(
            f"SELECT * FROM trade_quality_entry_microstructure_samples WHERE diagnostic_id IN ({placeholders})",
            ids,
        ).fetchall()
    by_id: dict[str, dict[str, Any]] = {}
    for feature in feature_rows:
        item = dict(feature)
        item["entry_quality_v2_reasons"] = _loads(item.pop("entry_quality_v2_reasons_json", None), [])
        item["evidence"] = _loads(item.pop("evidence_json", None), {})
        by_id[str(item.get("diagnostic_id"))] = item
    for row in rows:
        feature = by_id.get(str(row.get("diagnostic_id") or ""))
        if not feature:
            row.setdefault("entry_microstructure", {})
            continue
        row["entry_microstructure"] = feature
        row["entry_quality_v2_label"] = feature.get("entry_quality_v2_label")
        row["microstructure_coverage"] = feature.get("evidence_status")
        row["entry_acceptance_score"] = feature.get("entry_acceptance_score")


def _attach_entry_market_contexts(db_path: Path, rows: list[dict[str, Any]]) -> None:
    ids = [str(row.get("diagnostic_id") or "") for row in rows if row.get("diagnostic_id")]
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        feature_rows = conn.execute(
            f"SELECT * FROM trade_quality_entry_market_context_samples WHERE diagnostic_id IN ({placeholders})",
            ids,
        ).fetchall()
    by_id: dict[str, dict[str, Any]] = {}
    for feature in feature_rows:
        item = dict(feature)
        item["market_context_reasons"] = _loads(item.pop("market_context_reasons_json", None), [])
        item["evidence"] = _loads(item.pop("evidence_json", None), {})
        by_id[str(item.get("diagnostic_id"))] = item
    for row in rows:
        feature = by_id.get(str(row.get("diagnostic_id") or ""))
        if not feature:
            row.setdefault("entry_market_context", {})
            continue
        row["entry_market_context"] = feature
        row["market_context_label"] = feature.get("market_context_label")
        row["market_context_status"] = feature.get("market_context_status")


def _attach_entry_context_v3(db_path: Path, rows: list[dict[str, Any]]) -> None:
    ids = [str(row.get("diagnostic_id") or "") for row in rows if row.get("diagnostic_id")]
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        feature_rows = conn.execute(
            f"SELECT * FROM trade_quality_entry_context_v3_samples WHERE diagnostic_id IN ({placeholders})",
            ids,
        ).fetchall()
    by_id: dict[str, dict[str, Any]] = {}
    for feature in feature_rows:
        item = dict(feature)
        item["entry_context_v3_reasons"] = _loads(item.pop("entry_context_v3_reasons_json", None), [])
        item["evidence"] = _loads(item.pop("evidence_json", None), {})
        by_id[str(item.get("diagnostic_id"))] = item
    for row in rows:
        feature = by_id.get(str(row.get("diagnostic_id") or ""))
        if not feature:
            row.setdefault("entry_context_v3", {})
            continue
        row["entry_context_v3"] = feature
        row["entry_context_v3_label"] = feature.get("entry_context_v3_label")


def _decode_diag_row(row: dict[str, Any]) -> dict[str, Any]:
    row["quality_tags"] = _loads(row.pop("quality_tags_json", None), [])
    row["evidence"] = _loads(row.pop("evidence_json", None), {})
    return row


def _db_row(row: dict[str, Any], now: str) -> dict[str, Any]:
    evidence = {**(row.get("evidence") or {}), **(row.get("diagnostic_evidence") or {})}
    out = dict(row)
    for key in ("paper_reset_epoch_id", "paper_experiment_id", "paper_epoch_reset_at", "paper_epoch_scope_key"):
        out.setdefault(key, None)
    out["quality_tags_json"] = _json(out.pop("quality_tags", []))
    out["evidence_json"] = _json(evidence)
    out.pop("evidence", None)
    out.pop("diagnostic_evidence", None)
    out["created_at"] = now
    out["updated_at"] = now
    return out


def _aggregate_row(dimension: str, key: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    r_values = [_num(row.get("net_R")) for row in items if row.get("net_R") is not None]
    mfe_values = [_num(row.get("MFE_R")) for row in items if row.get("MFE_R") is not None]
    mae_values = [_num(row.get("MAE_R")) for row in items if row.get("MAE_R") is not None]
    return {
        "dimension": dimension,
        "key": key,
        "sample_count": len(items),
        "avg_net_R": _avg(r_values),
        "median_net_R": _median(r_values),
        "win_rate": _ratio(sum(1 for value in r_values if value > 0), len(r_values)),
        "avg_MFE_R": _avg(mfe_values),
        "avg_MAE_R": _avg(mae_values),
        "evidence": {
            "root_cause_counts": dict(Counter(str(row.get("root_cause") or "unknown") for row in items)),
            "replay_status_counts": dict(Counter(str(row.get("replay_status") or "unknown") for row in items)),
        },
    }


def _performance_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    items = [row for row in rows if row.get("net_R") is not None]
    items.sort(key=lambda row: (str(row.get("exit_time") or row.get("entry_time") or ""), str(row.get("trade_id") or "")))
    r_values = [_num(row.get("net_R")) for row in items]
    wins = [value for value in r_values if value > 0]
    losses = [abs(value) for value in r_values if value < 0]
    flats = [value for value in r_values if value == 0]
    win_rate = _ratio(len(wins), len(r_values))
    loss_rate = _ratio(len(losses), len(r_values))
    avg_win = _avg(wins)
    avg_loss = _avg(losses)
    profit_loss_ratio = round(avg_win / avg_loss, 8) if avg_loss else None
    expectancy = round((len(wins) / len(r_values)) * avg_win - (len(losses) / len(r_values)) * avg_loss, 8) if r_values else 0.0
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    current_loss_streak = 0
    max_losing_streak = 0
    streak_distribution: Counter[str] = Counter()
    for value in r_values:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
        if value < 0:
            current_loss_streak += 1
            max_losing_streak = max(max_losing_streak, current_loss_streak)
        elif current_loss_streak:
            streak_distribution[str(current_loss_streak)] += 1
            current_loss_streak = 0
    if current_loss_streak:
        streak_distribution[str(current_loss_streak)] += 1
    holding_values = [_num(row.get("holding_minutes")) for row in items if row.get("holding_minutes") is not None]
    fee_total = round(sum(abs(_num(row.get("fee"))) for row in items if row.get("fee") is not None), 8)
    gross_profit_usdt = round(sum(max(_num(row.get("net_pnl")), 0.0) for row in items if row.get("net_pnl") is not None), 8)
    fee_ratio = round(fee_total / gross_profit_usdt, 8) if gross_profit_usdt else None
    loss_overruns = [abs(value) for value in r_values if value < -1.0]
    r_parity_delta = round((avg_win or 0.0) - (avg_loss or 0.0), 8) if r_values else 0.0
    return {
        "trade_count": len(r_values),
        "win_count": len(wins),
        "loss_count": len(losses),
        "flat_count": len(flats),
        "win_rate": win_rate,
        "loss_rate": loss_rate,
        "avg_win_R": avg_win,
        "avg_loss_R": avg_loss,
        "profit_loss_ratio": profit_loss_ratio,
        "expectancy_R": expectancy,
        "max_drawdown_R": round(max_drawdown, 8),
        "max_losing_streak": max_losing_streak,
        "losing_streak_distribution": dict(streak_distribution),
        "gross_profit_R": round(sum(wins), 8),
        "gross_loss_R": round(sum(losses), 8),
        "fee_total": fee_total,
        "gross_profit_usdt": gross_profit_usdt,
        "fee_to_gross_profit_ratio": fee_ratio,
        "avg_holding_minutes": _avg(holding_values),
        "median_holding_minutes": _median(holding_values),
        "r_parity": {
            "target_loss_R": 1.0,
            "avg_win_R": avg_win,
            "avg_loss_R": avg_loss,
            "avg_win_minus_loss_R": r_parity_delta,
            "loss_overrun_count": len(loss_overruns),
            "loss_overrun_ratio": _ratio(len(loss_overruns), len(losses)),
            "max_loss_R": round(max(losses), 8) if losses else 0.0,
            "diagnosis": (
                "loss_R_overrun"
                if loss_overruns
                else "win_loss_R_aligned" if avg_loss and avg_win and abs(r_parity_delta) <= 0.25 else "insufficient_or_unbalanced"
            ),
        },
    }


ROOT_CAUSE_INFO: dict[str, tuple[str, str]] = {
    "needs_replay": ("MFE/MAE evidence is missing", "run or backfill 1m replay before drawing conclusions"),
    "signal_no_edge": ("entry had almost no favorable excursion", "reduce frequency and strengthen momentum/CVD/OFI continuation checks"),
    "direction_wrong": ("trade moved against the position immediately", "strengthen direction confirmation and avoid chase-tail entries"),
    "entered_too_early": ("direction may be right but entry absorbed too much adverse move", "wait for retest, acceptance, or renewed order-flow strength"),
    "entered_too_late": ("entry likely chased after impulse exhaustion", "add impulse-age, VWAP/EMA distance, and continuation-volume limits"),
    "stop_too_tight": ("stop was hit but price later offered enough favorable movement", "calibrate SL by volatility, spread, and invalidation level"),
    "tp_too_far": ("realized MFE was below planned reward target", "lower first TP or use MFE distribution for TP calibration"),
    "exit_too_late": ("trade reached favorable excursion but closed flat or negative", "add profit protection, volume decay exits, and time stop rules"),
    "cost_too_high": ("fees or slippage consume too much edge", "blacklist costly symbols or require larger expected edge"),
    "bad_symbol_candidate": ("symbol-level quality is weak", "review symbol tier before promotion to gating"),
    "bad_time_bucket_candidate": ("time-bucket quality is weak", "review session/time filters before promotion to gating"),
    "bad_regime_candidate": ("market regime quality is weak", "review regime classifier before promotion to gating"),
    "profitable_trade": ("trade ended positive", "use as positive control sample"),
    "loss_unclassified": ("loss does not match a strong first-layer rule", "inspect manually or add more evidence dimensions"),
    "unknown": ("root cause is missing", "resync diagnostics and check replay coverage"),
}


def _normalize_root_cause_label(root: str | None) -> str:
    text = str(root or "unknown")
    if text == "direction_wrong_or_chase_tail":
        return "direction_wrong"
    return text


def _root_cause_attribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    items = [row for row in rows if row.get("net_R") is not None]
    loss_items = [row for row in items if _num(row.get("net_R")) < 0]
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in items:
        groups[_normalize_root_cause_label(str(row.get("root_cause") or "unknown"))].append(row)
    attribution_items: list[dict[str, Any]] = []
    for root, group in groups.items():
        r_values = [_num(row.get("net_R")) for row in group if row.get("net_R") is not None]
        mfe_values = [_num(row.get("MFE_R")) for row in group if row.get("MFE_R") is not None]
        mae_values = [_num(row.get("MAE_R")) for row in group if row.get("MAE_R") is not None]
        planned_rr_values = [_num(row.get("planned_RR")) for row in group if row.get("planned_RR") is not None]
        confidence_values = [_num(row.get("root_cause_confidence")) for row in group if row.get("root_cause_confidence") is not None]
        loss_count = sum(1 for value in r_values if value < 0)
        meaning, optimization = ROOT_CAUSE_INFO.get(root, ROOT_CAUSE_INFO["unknown"])
        attribution_items.append(
            {
                "root_cause": root,
                "meaning": meaning,
                "optimization": optimization,
                "count": len(group),
                "loss_count": loss_count,
                "ratio": _ratio(len(group), len(items)),
                "loss_ratio": _ratio(loss_count, len(loss_items)),
                "avg_net_R": _avg(r_values),
                "avg_MFE_R": _avg(mfe_values),
                "avg_MAE_R": _avg(mae_values),
                "avg_planned_RR": _avg(planned_rr_values),
                "confidence_avg": _avg(confidence_values),
            }
        )
    attribution_items.sort(key=lambda row: (-int(row["loss_count"]), _num(row["avg_net_R"]), -int(row["count"]), str(row["root_cause"])))
    top_loss = next((row["root_cause"] for row in attribution_items if int(row["loss_count"]) > 0), None)
    needs_replay = sum(1 for row in items if str(row.get("root_cause") or "") == "needs_replay")
    attributed = sum(1 for row in items if str(row.get("root_cause") or "") not in {"", "unknown"})
    return {
        "sample_count": len(items),
        "loss_sample_count": len(loss_items),
        "coverage": _ratio(attributed, len(items)),
        "top_loss_root_cause": top_loss,
        "needs_replay_count": needs_replay,
        "items": attribution_items,
    }


def _dimension_attribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    items = [row for row in rows if row.get("net_R") is not None]
    return {
        "symbol": _dimension_group_rows("symbol", items, lambda row: str(row.get("symbol") or "unknown"), limit=30),
        "hour_bucket": _dimension_group_rows("hour_bucket", items, _hour_bucket, limit=24),
        "holding_bucket": _dimension_group_rows("holding_bucket", items, _holding_bucket, limit=10),
        "side": _dimension_group_rows("side", items, lambda row: str(row.get("side") or "unknown"), limit=10),
        "market_context": {
            "status": "pending_market_context_enrichment",
            "reason": "market regime evidence is not yet stored on diagnostic samples",
        },
    }


def _dimension_group_rows(
    dimension: str,
    rows: list[dict[str, Any]],
    getter: Any,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(getter(row) or "unknown")].append(row)
    out = [_dimension_row(dimension, key, group, len(rows)) for key, group in groups.items()]
    out.sort(key=lambda row: (_num(row["avg_R"]), -_num(row["trade_count"]), str(row["key"])))
    return out[:limit]


def _dimension_row(dimension: str, key: str, items: list[dict[str, Any]], total_count: int) -> dict[str, Any]:
    r_values = [_num(row.get("net_R")) for row in items if row.get("net_R") is not None]
    mfe_values = [_num(row.get("MFE_R")) for row in items if row.get("MFE_R") is not None]
    mae_values = [_num(row.get("MAE_R")) for row in items if row.get("MAE_R") is not None]
    holding_values = [_num(row.get("holding_minutes")) for row in items if row.get("holding_minutes") is not None]
    root_counts = Counter(_normalize_root_cause_label(str(row.get("root_cause") or "unknown")) for row in items)
    top_root = sorted(root_counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0] if root_counts else "unknown"
    fee_total = round(sum(abs(_num(row.get("fee"))) for row in items if row.get("fee") is not None), 8)
    gross_profit_usdt = round(sum(max(_num(row.get("net_pnl")), 0.0) for row in items if row.get("net_pnl") is not None), 8)
    loss_count = sum(1 for value in r_values if value < 0)
    return {
        "dimension": dimension,
        "key": key,
        "trade_count": len(items),
        "sample_count": len(items),
        "sample_ratio": _ratio(len(items), total_count),
        "loss_count": loss_count,
        "win_rate": _ratio(sum(1 for value in r_values if value > 0), len(r_values)),
        "avg_R": _avg(r_values),
        "avg_net_R": _avg(r_values),
        "total_R": round(sum(r_values), 8),
        "avg_MFE_R": _avg(mfe_values),
        "avg_MAE_R": _avg(mae_values),
        "fee_ratio": round(fee_total / gross_profit_usdt, 8) if gross_profit_usdt else None,
        "avg_holding_minutes": _avg(holding_values),
        "top_root_cause": top_root,
        "root_cause_counts": dict(root_counts),
    }


def _hour_bucket(row: dict[str, Any]) -> str:
    dt = _parse_iso(row.get("exit_time") or row.get("entry_time"))
    if dt is None:
        return "unknown"
    return f"UTC {dt.hour:02d}"


def _holding_bucket(row: dict[str, Any]) -> str:
    if row.get("holding_minutes") is None:
        return "unknown"
    minutes = _num(row.get("holding_minutes"))
    if minutes < 3:
        return "0-3m"
    if minutes < 10:
        return "3-10m"
    if minutes < 30:
        return "10-30m"
    if minutes < 60:
        return "30-60m"
    return "60m+"


ENTRY_QUALITY_INFO: dict[str, tuple[str, str]] = {
    "entry_feature_missing": ("entry feature evidence is missing", "run entry-feature backfill before drawing entry-quality conclusions"),
    "impulse_exhausted": ("entry likely chased after a completed impulse", "add impulse-age and follow-through checks before market entry"),
    "volume_no_followthrough": ("volume spike did not create favorable excursion", "require post-spike continuation before entry"),
    "btc_opposite_pressure": ("BTC moved against the trade direction", "avoid alt impulse entries under opposite BTC pressure"),
    "far_from_mean": ("entry was far from short-term mean", "limit VWAP/EMA distance or wait for retest"),
    "late_chase": ("entry happened after impulse had likely exhausted", "avoid chasing after large 3m/5m moves without renewed confirmation"),
    "accepted_impulse": ("impulse was accepted and produced favorable excursion", "use as positive-control entry pattern"),
    "entry_quality_observed": ("entry features did not match a strong first-version pattern", "inspect with more microstructure evidence before promoting rules"),
}


def _entry_quality_attribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    items = [row for row in rows if row.get("net_R") is not None]
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    covered = 0
    for row in items:
        features = row.get("entry_features") or {}
        label = str(features.get("entry_quality_label") or row.get("entry_quality_label") or "entry_feature_missing")
        coverage = str(features.get("entry_feature_coverage") or row.get("entry_feature_coverage") or "")
        if coverage in {"complete", "partial_missing_btc_candle"}:
            covered += 1
        groups[label].append(row)
    out: list[dict[str, Any]] = []
    for label, group in groups.items():
        r_values = [_num(row.get("net_R")) for row in group if row.get("net_R") is not None]
        mfe_values = [_num(row.get("MFE_R")) for row in group if row.get("MFE_R") is not None]
        mae_values = [_num(row.get("MAE_R")) for row in group if row.get("MAE_R") is not None]
        scores = [
            _num((row.get("entry_features") or {}).get("entry_quality_score") or row.get("entry_quality_score"))
            for row in group
            if (row.get("entry_features") or {}).get("entry_quality_score") is not None or row.get("entry_quality_score") is not None
        ]
        meaning, optimization = ENTRY_QUALITY_INFO.get(label, ENTRY_QUALITY_INFO["entry_quality_observed"])
        out.append(
            {
                "label": label,
                "meaning": meaning,
                "optimization": optimization,
                "trade_count": len(group),
                "sample_count": len(group),
                "loss_count": sum(1 for value in r_values if value < 0),
                "win_rate": _ratio(sum(1 for value in r_values if value > 0), len(r_values)),
                "avg_R": _avg(r_values),
                "avg_net_R": _avg(r_values),
                "total_R": round(sum(r_values), 8),
                "avg_MFE_R": _avg(mfe_values),
                "avg_MAE_R": _avg(mae_values),
                "avg_score": _avg(scores),
            }
        )
    out.sort(key=lambda row: (-int(row["loss_count"]), _num(row["avg_R"]), -int(row["trade_count"]), str(row["label"])))
    top_bad = next((row["label"] for row in out if int(row["loss_count"]) > 0), None)
    return {
        "sample_count": len(items),
        "feature_covered_count": covered,
        "feature_coverage": _ratio(covered, len(items)),
        "top_bad_entry_pattern": top_bad,
        "items": out,
    }


ENTRY_MICROSTRUCTURE_INFO: dict[str, tuple[str, str]] = {
    "price_move_not_confirmed_by_cvd": ("price move was not confirmed by CVD", "require CVD follow-through before market entry"),
    "breakout_not_confirmed_by_ofi": ("breakout was not confirmed by OFI", "require order-flow support after local breakout"),
    "cvd_strong_but_price_extended": ("CVD supported the side but price was extended", "avoid chasing far from VWAP/EMA"),
    "btc_opposite_pressure": ("BTC moved against the trade side", "downgrade altcoin impulse against BTC pressure"),
    "spread_too_wide_for_market_entry": ("spread was wide for market entry", "wait for spread normalization or avoid market entry"),
    "depth_imbalance_against_entry": ("order book depth leaned against entry", "avoid entering into opposing depth imbalance"),
    "oi_not_supporting_move": ("OI did not support the move", "require OI confirmation before continuation trades"),
    "funding_crowded_against_entry": ("funding looked crowded against entry", "avoid crowded one-sided funding regimes"),
    "microstructure_accepted_impulse": ("microstructure accepted the impulse", "use as positive reference pattern"),
    "microstructure_evidence_missing": ("microstructure evidence was missing", "improve audit/micro evidence retention before drawing conclusions"),
    "not_applicable_without_micro": ("without_micro line does not require micro evidence", "evaluate with candle/BTC/snapshot features instead of micro evidence"),
    "microstructure_observed": ("microstructure did not match a strong V2 pattern", "inspect more samples before promotion"),
}


def _entry_microstructure_attribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    items = [row for row in rows if row.get("net_R") is not None]
    by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    covered = 0
    for row in items:
        strategy_line = str(row.get("strategy_line") or "unknown")
        by_strategy[strategy_line].append(row)
        micro = row.get("entry_microstructure") or {}
        label = str(micro.get("entry_quality_v2_label") or "microstructure_evidence_missing")
        status = str(micro.get("evidence_status") or "")
        if status in {"complete", "partial_micro_missing", "micro_evidence_not_required"}:
            covered += 1
        groups[label].append(row)

    def group_row(label: str, group: list[dict[str, Any]]) -> dict[str, Any]:
        r_values = [_num(row.get("net_R")) for row in group if row.get("net_R") is not None]
        mfe_values = [_num(row.get("MFE_R")) for row in group if row.get("MFE_R") is not None]
        mae_values = [_num(row.get("MAE_R")) for row in group if row.get("MAE_R") is not None]
        scores = [
            _num((row.get("entry_microstructure") or {}).get("entry_acceptance_score"))
            for row in group
            if (row.get("entry_microstructure") or {}).get("entry_acceptance_score") is not None
        ]
        meaning, optimization = ENTRY_MICROSTRUCTURE_INFO.get(label, ENTRY_MICROSTRUCTURE_INFO["microstructure_observed"])
        return {
            "label": label,
            "meaning": meaning,
            "optimization": optimization,
            "trade_count": len(group),
            "sample_count": len(group),
            "loss_count": sum(1 for value in r_values if value < 0),
            "win_rate": _ratio(sum(1 for value in r_values if value > 0), len(r_values)),
            "avg_R": _avg(r_values),
            "avg_net_R": _avg(r_values),
            "total_R": round(sum(r_values), 8),
            "avg_MFE_R": _avg(mfe_values),
            "avg_MAE_R": _avg(mae_values),
            "avg_acceptance_score": _avg(scores),
        }

    out = [group_row(label, group) for label, group in groups.items()]
    out.sort(key=lambda row: (-int(row["loss_count"]), _num(row["avg_R"]), -int(row["trade_count"]), str(row["label"])))
    by_strategy_out = []
    for strategy_line, group in sorted(by_strategy.items()):
        micro_present = sum(1 for row in group if (row.get("entry_microstructure") or {}).get("evidence_status"))
        by_strategy_out.append(
            {
                "strategy_line": strategy_line,
                "sample_count": len(group),
                "microstructure_present_count": micro_present,
                "coverage": _ratio(micro_present, len(group)),
                "items": [
                    group_row(label, [row for row in group if str((row.get("entry_microstructure") or {}).get("entry_quality_v2_label") or "microstructure_evidence_missing") == label])
                    for label in sorted(
                        {
                            str((row.get("entry_microstructure") or {}).get("entry_quality_v2_label") or "microstructure_evidence_missing")
                            for row in group
                        }
                    )
                ],
            }
        )
    top_bad = next((row["label"] for row in out if int(row["loss_count"]) > 0), None)
    return {
        "sample_count": len(items),
        "feature_covered_count": covered,
        "coverage": _ratio(covered, len(items)),
        "top_label": top_bad,
        "items": out,
        "by_strategy_line": by_strategy_out,
        "diagnostic_only": True,
    }


ENTRY_MARKET_CONTEXT_INFO: dict[str, tuple[str, str]] = {
    "market_context_supported": ("OI/funding/BTC context was available and supportive", "use as baseline context; do not promote without sample size"),
    "funding_crowded_against_entry": ("funding looked crowded on the entry side", "downgrade crowded one-sided funding regimes"),
    "oi_not_supporting_move": ("OI was falling or did not support continuation", "avoid treating deleveraging moves as fresh continuation"),
    "oi_crowding_reversal_risk": ("OI rose while funding was crowded and trade lost", "watch for crowded reversal or squeeze risk"),
    "btc_opposite_pressure": ("BTC moved against the trade side", "avoid alt impulses under opposite BTC pressure"),
    "btc_chop_low_edge": ("BTC was neutral/choppy around entry", "lower impulse confidence in chop regimes"),
    "market_context_partial": ("some market context evidence was missing", "improve OI/funding/BTC retention before hard conclusions"),
    "market_context_missing": ("market context evidence was missing", "backfill or retain market context before diagnosis"),
}


ENTRY_CONTEXT_V3_INFO: dict[str, tuple[str, str]] = {
    **ENTRY_MARKET_CONTEXT_INFO,
    "price_move_not_confirmed_by_cvd": ("price move was not confirmed by CVD", "require active-flow CVD confirmation for micro entries"),
    "breakout_not_confirmed_by_ofi": ("breakout was not confirmed by OFI", "require order-book flow support after breakout"),
    "spread_too_wide_for_market_entry": ("spread was wide for market entry", "add spread normalization or avoid market order"),
    "depth_imbalance_against_entry": ("depth imbalance leaned against entry", "avoid entering into opposing book pressure"),
    "microstructure_full_acceptance": ("market and micro context accepted the impulse", "use as positive reference, then inspect exit quality"),
    "microstructure_partial_acceptance": ("micro context was observed but not a full acceptance", "inspect CVD/OFI/spread evidence before promotion"),
    "entry_supported_but_exit_problem": ("entry produced enough MFE but ended non-positive", "optimize protection stop, time stop, or TP capture"),
    "entry_context_evidence_gap": ("entry context evidence is incomplete", "fix retention/backfill before strategy changes"),
    "entry_context_observed": ("V3 context did not match a strong pattern", "collect more samples before promotion"),
}


def _entry_market_context_attribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return _context_attribution(
        rows,
        context_key="entry_market_context",
        label_key="market_context_label",
        status_key="market_context_status",
        score_key="market_context_score",
        info=ENTRY_MARKET_CONTEXT_INFO,
        fallback_label="market_context_missing",
        covered_statuses={"complete", "partial_oi_missing", "partial_funding_missing", "partial_btc_missing"},
    )


def _entry_context_v3_attribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return _context_attribution(
        rows,
        context_key="entry_context_v3",
        label_key="entry_context_v3_label",
        status_key="market_context_status",
        score_key="entry_context_v3_score",
        info=ENTRY_CONTEXT_V3_INFO,
        fallback_label="entry_context_evidence_gap",
        covered_statuses={"complete", "partial_oi_missing", "partial_funding_missing", "partial_btc_missing"},
    )


def _context_attribution(
    rows: list[dict[str, Any]],
    *,
    context_key: str,
    label_key: str,
    status_key: str,
    score_key: str,
    info: dict[str, tuple[str, str]],
    fallback_label: str,
    covered_statuses: set[str],
) -> dict[str, Any]:
    items = [row for row in rows if row.get("net_R") is not None]
    by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    covered = 0
    for row in items:
        strategy_line = str(row.get("strategy_line") or "unknown")
        by_strategy[strategy_line].append(row)
        context = row.get(context_key) or {}
        label = str(context.get(label_key) or fallback_label)
        status = str(context.get(status_key) or "")
        if status in covered_statuses or status.startswith("partial"):
            covered += 1
        groups[label].append(row)

    def group_row(label: str, group: list[dict[str, Any]]) -> dict[str, Any]:
        r_values = [_num(row.get("net_R")) for row in group if row.get("net_R") is not None]
        mfe_values = [_num(row.get("MFE_R")) for row in group if row.get("MFE_R") is not None]
        mae_values = [_num(row.get("MAE_R")) for row in group if row.get("MAE_R") is not None]
        scores = [
            _num((row.get(context_key) or {}).get(score_key))
            for row in group
            if (row.get(context_key) or {}).get(score_key) is not None
        ]
        meaning, optimization = info.get(label, info.get(fallback_label, ("unknown", "inspect manually")))
        return {
            "label": label,
            "meaning": meaning,
            "optimization": optimization,
            "trade_count": len(group),
            "sample_count": len(group),
            "loss_count": sum(1 for value in r_values if value < 0),
            "win_rate": _ratio(sum(1 for value in r_values if value > 0), len(r_values)),
            "avg_R": _avg(r_values),
            "avg_net_R": _avg(r_values),
            "total_R": round(sum(r_values), 8),
            "avg_MFE_R": _avg(mfe_values),
            "avg_MAE_R": _avg(mae_values),
            "avg_score": _avg(scores),
        }

    out = [group_row(label, group) for label, group in groups.items()]
    out.sort(key=lambda row: (-int(row["loss_count"]), _num(row["avg_R"]), -int(row["trade_count"]), str(row["label"])))
    by_strategy_out = []
    for strategy_line, group in sorted(by_strategy.items()):
        present = sum(1 for row in group if (row.get(context_key) or {}).get(label_key))
        labels = sorted({str((row.get(context_key) or {}).get(label_key) or fallback_label) for row in group})
        by_strategy_out.append(
            {
                "strategy_line": strategy_line,
                "sample_count": len(group),
                "present_count": present,
                "coverage": _ratio(present, len(group)),
                "items": [group_row(label, [row for row in group if str((row.get(context_key) or {}).get(label_key) or fallback_label) == label]) for label in labels],
            }
        )
    top_bad = next((row["label"] for row in out if int(row["loss_count"]) > 0), None)
    return {
        "sample_count": len(items),
        "feature_covered_count": covered,
        "coverage": _ratio(covered, len(items)),
        "top_label": top_bad,
        "items": out,
        "by_strategy_line": by_strategy_out,
        "diagnostic_only": True,
    }


def _phenomenon_counts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    specs = [
        ("mfe_lt_0.3", "MFE_R < 0.3", "signal has no momentum edge", lambda r: _num(r.get("MFE_R"), 999) < 0.3),
        (
            "mae_gt_0.7_then_recovered",
            "MAE_R > 0.7 before later favorable movement",
            "entry too early",
            lambda r: _num(r.get("MAE_R"), 0) > 0.7 and _num(r.get("MFE_R"), 0) >= 0.5,
        ),
        (
            "mfe_gte_0.8_final_loss",
            "MFE_R >= 0.8 but final loss",
            "exit too late",
            lambda r: _num(r.get("MFE_R"), 0) >= 0.8 and _num(r.get("net_R"), 0) <= 0,
        ),
        (
            "tp_unrealistic",
            "MFE_R around 0.5 while planned_TP implies 1.2R+",
            "TP unrealistic / tp_too_far",
            lambda r: 0.3 <= _num(r.get("MFE_R"), 999) <= 0.7 and _num(r.get("planned_RR"), 0) >= 1.2,
        ),
        (
            "sl_after_price_favorable",
            "SL then price moves favorable",
            "SL too tight or entry point poor",
            lambda r: str(r.get("exit_reason") or "").upper() == "SL" and _num(r.get("MFE_R"), 0) >= 0.8,
        ),
        (
            "immediate_adverse",
            "Immediate adverse move after entry",
            "chase tail or direction confirmation wrong",
            lambda r: _num(r.get("MAE_R"), 0) >= 0.8 and _num(r.get("MFE_R"), 0) < 0.3,
        ),
    ]
    total = len(rows)
    out = []
    for code, phenomenon, meaning, predicate in specs:
        count = sum(1 for row in rows if predicate(row))
        out.append({"code": code, "phenomenon": phenomenon, "meaning": meaning, "count": count, "ratio": _ratio(count, total)})
    return out


def _table_exists(db_path: Path, table: str) -> bool:
    if not db_path.exists():
        return False
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, spec in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {spec}")


def _dedup_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out = []
    for row in rows:
        key = _trade_identity_key(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _trade_identity_key(row: dict[str, Any]) -> str:
    return hashlib.sha256(
        "|".join(
            str(row.get(k) or "")
            for k in ("trade_id", "symbol", "side", "entry_time", "exit_time")
        ).encode("utf-8")
    ).hexdigest()[:32]


def _dedup_key(row: dict[str, Any]) -> str:
    return hashlib.sha256(
        "|".join(
            str(row.get(k) or "")
            for k in ("source", "trade_id", "symbol", "side", "entry_time", "exit_time")
        ).encode("utf-8")
    ).hexdigest()[:32]


def _diagnostic_id(source: Any, trade_id: Any, symbol: Any, side: Any, entry_time: Any, exit_time: Any) -> str:
    return hashlib.sha256(
        "|".join(str(v or "") for v in (source, trade_id, symbol, side, entry_time, exit_time, DIAGNOSTIC_SCHEMA_VERSION)).encode("utf-8")
    ).hexdigest()[:24]


def _archive_id(path: str | None) -> str | None:
    if not path:
        return None
    parts = Path(path).parts
    for part in parts:
        if part.startswith("paper_exp_"):
            return part
    return Path(path).parent.name if path else None


def _normalize_source(value: str | None) -> str:
    got = str(value or "all").lower()
    return got if got in {"all", "current_paper", "archive", "legacy_p18"} else "all"


def _side(value: Any) -> str:
    got = str(value or "").upper()
    if got in {"SHORT", "SELL"}:
        return "SHORT"
    return "LONG" if got in {"LONG", "BUY"} else got


def _holding_minutes(opened_at: Any, closed_at: Any, holding_sec: Any) -> float | None:
    if holding_sec is not None:
        return round(_num(holding_sec) / 60.0, 6)
    seconds = _holding_sec(str(opened_at) if opened_at else None, str(closed_at) if closed_at else None)
    return round(seconds / 60.0, 6) if seconds is not None else None


def _planned_rr(entry: Any, sl: Any, tp: Any) -> float | None:
    risk = abs(_num(entry) - _num(sl))
    reward = abs(_num(tp) - _num(entry))
    return reward / risk if risk > 0 else None


def _none_or_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _max_favorable_price(entry: Any, qty: float, mfe: float | None, side: Any) -> float | None:
    if entry is None or not qty or mfe is None:
        return None
    return float(entry) - mfe / qty if _side(side) == "SHORT" else float(entry) + mfe / qty


def _max_adverse_price(entry: Any, qty: float, mae: float | None, side: Any) -> float | None:
    if entry is None or not qty or mae is None:
        return None
    return float(entry) + mae / qty if _side(side) == "SHORT" else float(entry) - mae / qty


def _safe_limit(limit: int | None) -> int:
    return max(1, min(int(limit or 200), 5000))


def _avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 8) if values else 0.0


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return round(sorted_values[mid], 8)
    return round((sorted_values[mid - 1] + sorted_values[mid]) / 2.0, 8)


def _ratio(num: int, den: int) -> float:
    return round(num / den, 6) if den else 0.0
