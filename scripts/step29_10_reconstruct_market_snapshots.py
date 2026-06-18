from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.training_snapshot_sync import (
    canonical_json,
    complete_scoped_known_at_reconstruction,
    sidecar_db_path,
    stable_hash,
)


TASK_ID = "STEP29.10"
SCHEMA_VERSION = "step29.10.market-snapshot-reconstruction.v1"
FEATURE_SCHEMA_VERSION = "step29_market_feature_known_at_v1"
SIDECAR_DB = sidecar_db_path(ROOT)
P21_DB = ROOT / "DATA" / "backtest" / "p21_parameter_optimization.db"
OUT_DIR = ROOT / "DATA" / "research" / "trade_snapshots"
REPORT_DIR = ROOT / "docs" / "reports"
OUTPUT_JSON = OUT_DIR / "step29_10_market_snapshot_reconstruction_summary.json"

REQUIRED_MARKET_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "rsi_14",
    "ema20_distance_bps",
    "ema60_distance_bps",
    "bollinger_position",
    "bollinger_width_bps",
    "atr_14_bps",
    "volume_z",
    "pct_1m",
    "pct_3m",
    "pct_5m",
    "pct_15m",
    "range_pos_30m",
]
MARKET_MISSING_ALIASES = {
    "ohlcv",
    "rsi_14",
    "ema20_distance_bps",
    "ema60_distance_bps",
    "bollinger_position",
    "bollinger_width_bps",
    "atr_14_bps",
    "volume_z",
    "pct_1m",
    "pct_3m",
    "pct_5m",
    "pct_15m",
    "range_pos_30m",
}
POST_TRADE_FORBIDDEN_IN_INPUT = {
    "MFE_R",
    "MAE_R",
    "net_R",
    "holding_time",
    "holding_time_sec",
    "exit_reason",
    "root_cause_label",
    "gross_pnl_usdt",
    "net_pnl_usdt",
    "exit_price",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _loads(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(str(raw))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _connect_rw(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=30000")
    return con


def _connect_ro(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def _num(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        got = float(value)
        if math.isnan(got) or math.isinf(got):
            return None
        return got
    except Exception:
        return None


def _round(value: float | None, digits: int = 8) -> float | None:
    return round(value, digits) if value is not None else None


def _ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    alpha = 2 / (period + 1)
    ema = mean(values[:period])
    for value in values[period:]:
        ema = (value - ema) * alpha + ema
    return ema


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for prev, cur in zip(values[-period - 1 : -1], values[-period:]):
        diff = cur - prev
        gains.append(max(diff, 0.0))
        losses.append(abs(min(diff, 0.0)))
    avg_gain = mean(gains)
    avg_loss = mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _closed_candle_open_ms(decision_time_ms: int | None) -> int | None:
    if decision_time_ms is None:
        return None
    return (int(decision_time_ms) // 60000) * 60000 - 60000


def _decision_time_ms(event: dict[str, Any]) -> int | None:
    decision_ms = event.get("decision_time_ms") or event.get("event_time_ms")
    event_ms = event.get("event_time_ms")
    try:
        decision_int = int(decision_ms) if decision_ms is not None else None
        event_int = int(event_ms) if event_ms is not None else None
    except Exception:
        return None
    if str(event.get("event_action") or "").lower() == "entry" and decision_int and event_int and decision_int > event_int:
        return event_int
    return decision_int


def _fetch_klines(conn: sqlite3.Connection, symbol: str, closed_open_ms: int, limit: int = 90) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT symbol, open_time_ms, open, high, low, close, volume, quote_volume, taker_buy_base_volume
        FROM p21_klines_1m
        WHERE symbol = ? AND open_time_ms <= ?
        ORDER BY open_time_ms DESC
        LIMIT ?
        """,
        (symbol.upper(), int(closed_open_ms), int(limit)),
    ).fetchall()
    return [dict(row) for row in reversed(rows)]


def _field_meta(source_table: str, source_row_id: str, field: str, feature_ts: int | None, known_at: int | None) -> dict[str, Any]:
    return {
        "source_priority": "rebuilt",
        "source_db_path": P21_DB.relative_to(ROOT).as_posix(),
        "source_table": source_table,
        "source_row_id": source_row_id,
        "feature_timestamp_ms": feature_ts,
        "known_at_ms": known_at,
        "source_available_time_ms": known_at,
        "lineage_id": hashlib.sha256(f"{source_table}:{source_row_id}:{field}".encode("utf-8")).hexdigest()[:24],
        "schema_version": FEATURE_SCHEMA_VERSION,
    }


def _market_snapshot_from_klines(event: dict[str, Any], klines: list[dict[str, Any]]) -> dict[str, Any]:
    symbol = str(event.get("symbol") or "").upper()
    decision_ms = _decision_time_ms(event)
    event_candle_ms = event.get("candle_open_time_ms")
    if not klines:
        return {
            "status": "missing_source",
            "event_action": event.get("event_action"),
            "symbol": symbol,
            "side": event.get("side"),
            "event_time_ms": event.get("event_time_ms"),
            "event_candle_open_time_ms": event_candle_ms,
            "decision_time_ms": decision_ms,
            "known_at_policy": FEATURE_SCHEMA_VERSION,
            "missing_fields": REQUIRED_MARKET_FIELDS,
            "blocked_fields": [],
            "proxy_fields": [],
            "field_lineage_json": {},
            "reason": "missing_kline_source_window",
        }
    last = klines[-1]
    closes = [_num(row.get("close")) for row in klines]
    closes = [value for value in closes if value is not None]
    highs = [_num(row.get("high")) for row in klines]
    highs = [value for value in highs if value is not None]
    lows = [_num(row.get("low")) for row in klines]
    lows = [value for value in lows if value is not None]
    vols = [_num(row.get("volume")) for row in klines]
    vols = [value for value in vols if value is not None]
    close = _num(last.get("close"))
    open_p = _num(last.get("open"))
    high = _num(last.get("high"))
    low = _num(last.get("low"))
    volume = _num(last.get("volume"))
    feature_ts = int(last["open_time_ms"])
    known_at = feature_ts + 60000
    source_row_id = f"{symbol}:{feature_ts}"

    def pct(minutes: int) -> float | None:
        if len(closes) <= minutes or closes[-minutes - 1] in (None, 0):
            return None
        return closes[-1] / closes[-minutes - 1] - 1

    ema20 = _ema(closes[-60:], 20)
    ema60 = _ema(closes[-90:], 60)
    bollinger_position = None
    bollinger_width_bps = None
    if len(closes) >= 20 and close is not None:
        basis = mean(closes[-20:])
        sd = pstdev(closes[-20:])
        upper = basis + 2 * sd
        lower = basis - 2 * sd
        width = upper - lower
        bollinger_width_bps = width / basis * 10000 if basis else None
        bollinger_position = (close - lower) / width if width > 0 else None
    atr_14 = None
    if close and len(klines) >= 15:
        trs: list[float] = []
        for idx in range(len(klines) - 14, len(klines)):
            cur_high = _num(klines[idx].get("high"))
            cur_low = _num(klines[idx].get("low"))
            prev_close = _num(klines[idx - 1].get("close"))
            if cur_high is None or cur_low is None or prev_close is None:
                continue
            trs.append(max(cur_high - cur_low, abs(cur_high - prev_close), abs(cur_low - prev_close)))
        if trs:
            atr_14 = mean(trs) / close * 10000
    volume_z = None
    if len(vols) >= 21:
        base = vols[-21:-1]
        sd = pstdev(base)
        volume_z = (vols[-1] - mean(base)) / sd if sd > 0 else 0.0
    range_pos_30m = None
    if close is not None and len(highs) >= 30 and len(lows) >= 30:
        high30 = max(highs[-30:])
        low30 = min(lows[-30:])
        range_pos_30m = (close - low30) / (high30 - low30) if high30 > low30 else None

    values = {
        "open": open_p,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "rsi_14": _round(_rsi(closes, 14), 6),
        "ema20_distance_bps": _round((close / ema20 - 1) * 10000 if close is not None and ema20 else None, 6),
        "ema60_distance_bps": _round((close / ema60 - 1) * 10000 if close is not None and ema60 else None, 6),
        "bollinger_position": _round(bollinger_position, 8),
        "bollinger_width_bps": _round(bollinger_width_bps, 6),
        "atr_14_bps": _round(atr_14, 6),
        "volume_z": _round(volume_z, 6),
        "pct_1m": _round(pct(1), 10),
        "pct_3m": _round(pct(3), 10),
        "pct_5m": _round(pct(5), 10),
        "pct_15m": _round(pct(15), 10),
        "range_pos_30m": _round(range_pos_30m, 8),
    }
    missing = [field for field in REQUIRED_MARKET_FIELDS if values.get(field) is None]
    field_lineage = {
        field: _field_meta("p21_klines_1m", source_row_id, field, feature_ts, known_at)
        for field, value in values.items()
        if value is not None
    }
    known_at_pass = bool(known_at <= int(decision_ms or 0)) if decision_ms else False
    return {
        "status": "complete" if not missing and known_at_pass else "partial" if values else "missing_source",
        "event_action": event.get("event_action"),
        "symbol": symbol,
        "side": event.get("side"),
        "event_time_ms": event.get("event_time_ms"),
        "decision_time_ms": decision_ms,
        "event_candle_open_time_ms": event_candle_ms,
        "candle_open_time_ms": feature_ts,
        "feature_timestamp_ms": feature_ts,
        "known_at_ms": known_at,
        "max_feature_known_at_ms": known_at,
        "known_at_policy": FEATURE_SCHEMA_VERSION,
        "known_at_pass": known_at_pass,
        "source_priority": "rebuilt",
        "source_db_path": P21_DB.relative_to(ROOT).as_posix(),
        "source_table": "p21_klines_1m",
        "source_row_id": source_row_id,
        "schema_version": SCHEMA_VERSION,
        "missing_fields": missing,
        "blocked_fields": [] if known_at_pass else ["market_snapshot_known_after_decision"],
        "proxy_fields": [],
        "field_lineage_json": field_lineage,
        **values,
    }


def _reconstruct_event(p21: sqlite3.Connection, event: dict[str, Any]) -> dict[str, Any]:
    decision_ms = _decision_time_ms(event)
    closed_open = _closed_candle_open_ms(int(decision_ms)) if decision_ms else None
    if not event.get("symbol") or closed_open is None:
        return _market_snapshot_from_klines(event, [])
    return _market_snapshot_from_klines(event, _fetch_klines(p21, str(event["symbol"]), closed_open))


def _event_data_quality(snapshot: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
    out = dict(existing or {})
    out["market_snapshot_status"] = snapshot.get("status")
    out["market_missing_fields_json"] = list(snapshot.get("missing_fields") or [])
    out["market_blocked_fields_json"] = list(snapshot.get("blocked_fields") or [])
    out["market_known_at_pass"] = bool(snapshot.get("known_at_pass"))
    out["market_feature_schema_version"] = FEATURE_SCHEMA_VERSION
    return out


def _sample_data_quality(entry: dict[str, Any] | None, exit_snap: dict[str, Any] | None, existing: dict[str, Any]) -> dict[str, Any]:
    out = dict(existing or {})
    existing_missing = [
        item
        for item in list(out.get("missing_fields_json") or [])
        if item not in MARKET_MISSING_ALIASES and not str(item).startswith("market_snapshot.") and not str(item).startswith("exit_market_snapshot.")
    ]
    market_missing = [f"market_snapshot.{field}" for field in ((entry or {}).get("missing_fields") or [])]
    exit_missing = [f"exit_market_snapshot.{field}" for field in ((exit_snap or {}).get("missing_fields") or [])]
    out["missing_fields_json"] = sorted(set(existing_missing + market_missing + exit_missing))
    out["market_entry_missing_fields_json"] = list((entry or {}).get("missing_fields") or [])
    out["market_exit_missing_fields_json"] = list((exit_snap or {}).get("missing_fields") or [])
    out["market_snapshot_status"] = "complete" if entry and exit_snap and not market_missing and not exit_missing else "partial"
    out["market_feature_completeness"] = "complete" if out["market_snapshot_status"] == "complete" else "incomplete"
    out["market_known_at_pass"] = bool((entry or {}).get("known_at_pass")) and bool((exit_snap or {}).get("known_at_pass"))
    out["feature_completeness"] = "complete" if not out["missing_fields_json"] else "incomplete"
    return out


def _walk_keys(value: Any) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            keys.add(str(key))
            keys.update(_walk_keys(item))
    elif isinstance(value, list):
        for item in value:
            keys.update(_walk_keys(item))
    return keys


def _refresh_manifest(sidecar: sqlite3.Connection, *, run_id: str, summary: dict[str, Any]) -> None:
    manifest_id = f"{run_id}:manifest"
    audit_id = f"{run_id}:coverage"
    coverage = {
        "sample_count": summary["samples_processed"],
        "events_processed": summary["events_processed"],
        "entry_exit_pair_rate": summary["entry_exit_pair_rate"],
        "market_feature_complete_rate": summary["market_feature_complete_rate"],
        "market_event_complete_rate": summary["market_event_complete_rate"],
        "known_at_pass_rate": summary["known_at_pass_rate"],
        "leakage_violations": summary["leakage_violations"],
        "missing_fields_json": summary["missing_field_counts"],
        "generated_at": summary["generated_at"],
    }
    dataset_hash = stable_hash(summary)
    schema_hash = stable_hash({"schema_version": SCHEMA_VERSION, "required_fields": REQUIRED_MARKET_FIELDS})
    sidecar.execute(
        """
        INSERT OR REPLACE INTO trade_snapshot_manifests (
            manifest_id, run_id, source_mode, schema_version, schema_hash,
            source_refs_json, coverage_json, dataset_hash, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            manifest_id,
            run_id,
            "sidecar_market_reconstruction",
            SCHEMA_VERSION,
            schema_hash,
            canonical_json([{"source_db_path": P21_DB.relative_to(ROOT).as_posix(), "access_mode": "read_only"}]),
            canonical_json(coverage),
            dataset_hash,
            summary["generated_at"],
        ),
    )
    sidecar.execute(
        """
        INSERT OR REPLACE INTO trade_snapshot_coverage_audits (
            audit_id, manifest_id, sample_count, entry_exit_pair_rate,
            market_feature_complete_rate, trade_quality_label_rate,
            config_gate_lineage_rate, known_at_pass_rate,
            leakage_violations_json, missing_fields_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            audit_id,
            manifest_id,
            summary["samples_processed"],
            summary["entry_exit_pair_rate"],
            summary["market_feature_complete_rate"],
            summary.get("trade_quality_label_rate", 0.0),
            summary.get("config_gate_lineage_rate", 0.0),
            summary["known_at_pass_rate"],
            canonical_json(summary["leakage_violations"]),
            canonical_json(summary["missing_field_counts"]),
            summary["generated_at"],
        ),
    )


def _write_report(summary: dict[str, Any], report_path: Path) -> None:
    lines = [
        "# STEP29.10 Training Readiness Market Snapshot Reconstruction",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- sidecar_db: `{SIDECAR_DB.relative_to(ROOT).as_posix()}`",
        f"- source_kline_db: `{P21_DB.relative_to(ROOT).as_posix()}`",
        f"- dry_run: `{summary['dry_run']}`",
        f"- events_processed: `{summary['events_processed']}`",
        f"- samples_processed: `{summary['samples_processed']}`",
        f"- market_event_complete_rate: `{summary['market_event_complete_rate']}`",
        f"- market_feature_complete_rate: `{summary['market_feature_complete_rate']}`",
        f"- known_at_pass_rate: `{summary['known_at_pass_rate']}`",
        f"- leakage_violations: `{len(summary['leakage_violations'])}`",
        "",
        "## Missing Fields",
        "",
        "| field | count |",
        "| --- | ---: |",
    ]
    for field, count in sorted((summary.get("missing_field_counts") or {}).items(), key=lambda item: (-item[1], item[0]))[:50]:
        lines.append(f"| `{field}` | `{count}` |")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Source paper/backtest/sandbox DBs were not modified.",
            "- P21 kline cache was read-only.",
            "- Post-trade outcome fields were not written into decision-time input.",
            "- Samples with missing source windows remain not training-ready.",
        ]
    )
    if summary.get("unreconstructable_examples"):
        lines.extend(["", "## Unreconstructable Examples", ""])
        for item in summary["unreconstructable_examples"][:20]:
            lines.append(f"- `{item}`")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(*, limit: int | None, dry_run: bool) -> dict[str, Any]:
    generated_at = _now()
    event_snapshots: dict[str, dict[str, Any]] = {}
    missing_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    known_at_pass = 0
    leakage_violations: list[dict[str, Any]] = []
    unreconstructable: list[str] = []
    with _connect_rw(SIDECAR_DB) as sidecar, _connect_ro(P21_DB) as p21:
        query = "SELECT * FROM trade_snapshot_events ORDER BY sample_id, event_action"
        params: tuple[Any, ...] = ()
        if limit:
            query += " LIMIT ?"
            params = (int(limit),)
        events = [dict(row) for row in sidecar.execute(query, params).fetchall()]
        for event in events:
            snapshot = _reconstruct_event(p21, event)
            event_snapshots[str(event["event_id"])] = snapshot
            status_counts[str(snapshot.get("status"))] += 1
            missing_counts.update(snapshot.get("missing_fields") or [])
            if snapshot.get("known_at_pass"):
                known_at_pass += 1
            if snapshot.get("status") != "complete" and len(unreconstructable) < 50:
                unreconstructable.append(
                    f"{event.get('event_id')} {event.get('symbol')} {event.get('event_action')} {snapshot.get('reason') or snapshot.get('missing_fields')}"
                )
            if not dry_run:
                data_quality = _event_data_quality(snapshot, _loads(event.get("data_quality_json"), {}))
                field_roles = {
                    **_loads(event.get("field_roles_json"), {}),
                    "market_snapshot_json": "decision_time_feature" if event.get("event_action") == "entry" else "exit_audit_context",
                    "market_field_lineage": snapshot.get("field_lineage_json") or {},
                }
                sidecar.execute(
                    """
                    UPDATE trade_snapshot_events
                    SET market_snapshot_json=?, known_at_ms=?, decision_time_ms=?, data_quality_json=?, field_roles_json=?
                    WHERE event_id=?
                    """,
                    (
                        canonical_json(snapshot),
                        snapshot.get("max_feature_known_at_ms") or event.get("known_at_ms"),
                        snapshot.get("decision_time_ms") or event.get("decision_time_ms"),
                        canonical_json(data_quality),
                        canonical_json(field_roles),
                        event["event_id"],
                    ),
                )
        sample_query = "SELECT * FROM trade_training_samples ORDER BY sample_id"
        sample_params: tuple[Any, ...] = ()
        if limit:
            sample_query += " LIMIT ?"
            sample_params = (int(limit),)
        samples = [dict(row) for row in sidecar.execute(sample_query, sample_params).fetchall()]
        paired = 0
        market_complete = 0
        tq_labeled = 0
        config_lineage_ok = 0
        for sample in samples:
            entry = event_snapshots.get(str(sample.get("entry_event_id")))
            exit_snap = event_snapshots.get(str(sample.get("exit_event_id")))
            if entry and exit_snap:
                paired += 1
            if entry and exit_snap and entry.get("status") == "complete" and exit_snap.get("status") == "complete":
                market_complete += 1
            if _loads(sample.get("label_json"), {}):
                tq_labeled += 1
            decision = _loads(sample.get("decision_time_input_json"), {})
            audit = _loads(sample.get("audit_context_json"), {})
            decision.setdefault("order_plan", _loads(sample.get("order_plan_json"), {}))
            if entry:
                decision["entry_market_snapshot"] = entry
            if exit_snap:
                audit["exit_market_snapshot"] = {
                    **exit_snap,
                    "exit_event_id": sample.get("exit_event_id"),
                    "exit_time_ms": sample.get("exit_time_ms"),
                    "exit_candle_open_time_ms": exit_snap.get("candle_open_time_ms"),
                }
            dq = _sample_data_quality(entry, exit_snap, _loads(sample.get("data_quality_json"), {}))
            config = decision.get("config_lineage") if isinstance(decision.get("config_lineage"), dict) else {}
            if config and not (config.get("missing_fields_json") or []):
                config_lineage_ok += 1
            forbidden = sorted(POST_TRADE_FORBIDDEN_IN_INPUT & _walk_keys(decision))
            for field in forbidden:
                leakage_violations.append({"sample_id": sample.get("sample_id"), "field": field, "location": "decision_time_input_json"})
            if not dry_run:
                sidecar.execute(
                    """
                    UPDATE trade_training_samples
                    SET decision_time_input_json=?, audit_context_json=?, data_quality_json=?
                    WHERE sample_id=?
                    """,
                    (
                        canonical_json(decision),
                        canonical_json(audit),
                        canonical_json(dq),
                        sample["sample_id"],
                    ),
                )
        events_processed = len(events)
        samples_processed = len(samples)
        summary = {
            "schema_version": SCHEMA_VERSION,
            "task_id": TASK_ID,
            "generated_at": generated_at,
            "dry_run": dry_run,
            "limit": limit,
            "events_processed": events_processed,
            "samples_processed": samples_processed,
            "event_status_counts": dict(status_counts),
            "missing_field_counts": dict(missing_counts),
            "market_event_complete_rate": round(status_counts.get("complete", 0) / events_processed, 8) if events_processed else 0.0,
            "market_feature_complete_rate": round(market_complete / samples_processed, 8) if samples_processed else 0.0,
            "entry_exit_pair_rate": round(paired / samples_processed, 8) if samples_processed else 0.0,
            "known_at_pass_rate": round(known_at_pass / events_processed, 8) if events_processed else 0.0,
            "trade_quality_label_rate": round(tq_labeled / samples_processed, 8) if samples_processed else 0.0,
            "config_gate_lineage_rate": round(config_lineage_ok / samples_processed, 8) if samples_processed else 0.0,
            "leakage_violations": leakage_violations,
            "unreconstructable_examples": unreconstructable,
        }
        if not dry_run:
            run_id = f"step29_10_market_snapshot_reconstruction_{_stamp()}"
            _refresh_manifest(sidecar, run_id=run_id, summary=summary)
            sidecar.commit()
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--sandbox-id", default=None)
    parser.add_argument("--source-mode", default=None)
    parser.add_argument("--max-source-lag-ms", type=int, default=60_000)
    args = parser.parse_args()
    if args.run_id or args.sandbox_id or args.source_mode:
        summary = complete_scoped_known_at_reconstruction(
            ROOT,
            run_id=args.run_id,
            sandbox_id=args.sandbox_id,
            source_mode=args.source_mode,
            limit=args.limit,
            dry_run=bool(args.dry_run),
            max_source_lag_ms=int(args.max_source_lag_ms),
            include_market=True,
            include_extended=False,
        )
        summary = {
            **summary,
            "generated_at": _now(),
            "missing_field_counts": {},
        }
    else:
        summary = run(limit=args.limit, dry_run=bool(args.dry_run))
    _write_json(OUTPUT_JSON, summary)
    report = REPORT_DIR / f"STEP29.10_market_snapshot_reconstruction_{_stamp()}.md"
    _write_report(summary, report)
    print(json.dumps({"status": "ok", "output": str(OUTPUT_JSON), "report": str(report), **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
