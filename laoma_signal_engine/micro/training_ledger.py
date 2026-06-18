"""Micro training evidence ledger.

This module is an observability/training sidecar. It must not alter strategy
decisions, micro readiness semantics, trade plan construction, or paper
consumption.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


MICRO_TRAINING_SCHEMA_VERSION = "16.22-v1"
MICRO_MODES = {"micro_fast": "fast", "micro_full": "full"}
MICRO_SYMBOL_OPTIONAL_COLUMNS: dict[str, str] = {
    "spread_bps": "real",
    "bid_depth_usdt": "real",
    "ask_depth_usdt": "real",
    "top_book_age_ms": "real",
    "depth_source": "text",
    "depth_missing_reason": "text",
    "book_cost_confidence": "text",
    "ws_route_ok": "integer",
    "ws_stream": "text",
    "ws_last_event_at": "text",
    "ws_last_pong_at": "text",
    "ws_silent_age_sec": "real",
    "ws_subscribe_throttle_state": "text",
    "stream_gap_state": "text",
    "route_mismatch": "text",
    "ws_connected_but_no_emit": "integer",
    "micro_data_plane_ready": "integer",
    "post_restart_warming": "integer",
    "data_plane_ready_at_read": "integer",
    "data_plane_ready_age_sec": "real",
    "target_set_hydrated": "integer",
    "warmup_started_at": "text",
    "warmup_completed_at": "text",
    "readiness_block_reason": "text",
    "cvd_bucket_ts": "integer",
    "ofi_bucket_ts": "integer",
    "book_bucket_ts": "integer",
    "depth_bucket_ts": "integer",
    "common_bucket_ts": "integer",
    "bucket_lag_sec": "real",
    "alignment_state": "text",
    "alignment_reason": "text",
    "watermark_age_sec": "real",
    "z_state": "text",
    "z_cvd_state": "text",
    "z_ofi_state": "text",
    "z_window_bucket_count": "integer",
    "z_valid_bucket_ratio": "real",
    "z_target_age_sec": "real",
    "z_dwell_pending_until": "text",
    "z_missing_reason": "text",
    "top_book_event_ts": "integer",
    "depth_event_ts": "integer",
    "book_update_id": "text",
    "depth_update_id": "text",
    "book_gap_state": "text",
    "technical_status": "text",
    "technical_severity": "text",
    "technical_reason_codes_json": "text",
    "technical_reliability_score": "real",
    "is_training_usable": "integer",
    "not_training_usable_reason": "text",
}


def project_root(root: Path | None = None) -> Path:
    return Path(root).resolve() if root else Path.cwd().resolve()


def default_micro_training_db(root: Path | None = None) -> Path:
    return project_root(root) / "DATA" / "micro" / "micro_training.db"


def default_audit_db(root: Path | None = None) -> Path:
    return project_root(root) / "DATA" / "audit" / "run_audit.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)


def json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def sample_id_for(
    *,
    run_id: str,
    strategy_line: str,
    micro_mode: str,
    symbol: str,
    target_set_id: str | None = None,
) -> str:
    raw = "|".join(
        [
            str(run_id or "unknown"),
            str(strategy_line or "unknown"),
            str(micro_mode or "unknown"),
            str(symbol or "unknown").upper(),
            str(target_set_id or ""),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_micro_training_db(db_path: Path | None = None, *, root: Path | None = None) -> Path:
    db = db_path or default_micro_training_db(root)
    with connect(db) as conn:
        conn.execute(
            """
            create table if not exists micro_run_samples (
              run_id text not null,
              cycle_id text,
              strategy_line text not null,
              target_set_id text,
              micro_mode text not null,
              started_at text,
              ended_at text,
              generated_at text,
              status text not null default 'missing',
              health_state text,
              reason_codes_json text not null default '[]',
              source_confidence text not null default 'direct_run_id',
              missing_reason text,
              source_refs_json text not null default '{}',
              payload_json text not null default '{}',
              updated_at text not null,
              primary key(run_id, strategy_line, micro_mode)
            )
            """
        )
        conn.execute(
            """
            create table if not exists micro_symbol_samples (
              sample_id text primary key,
              run_id text not null,
              cycle_id text,
              strategy_line text not null,
              symbol text not null,
              side text,
              target_source text,
              target_set_id text,
              micro_mode text not null,
              ready_state text,
              confirmation_state text,
              accepted integer not null default 0,
              blocked integer not null default 0,
              cvd real,
              ofi real,
              z_cvd real,
              z_ofi real,
              spread real,
              depth_imbalance real,
              bookticker_age_ms real,
              aggtrade_age_ms real,
              depth_age_ms real,
              coverage_flags_json text not null default '{}',
              reason_codes_json text not null default '[]',
              source_confidence text not null default 'direct_run_id',
              missing_reason text,
              generated_at text,
              source_refs_json text not null default '{}',
              payload_json text not null default '{}',
              updated_at text not null
            )
            """
        )
        conn.execute(
            """
            create table if not exists micro_downstream_labels (
              sample_id text primary key,
              run_id text not null,
              cycle_id text,
              strategy_line text not null,
              symbol text not null,
              trade_plan_status text,
              executable integer,
              trade_plan_ref text,
              paper_order_id text,
              paper_status text,
              exit_reason text,
              net_R real,
              MFE_R real,
              MAE_R real,
              trade_quality_root_cause text,
              label_source text,
              label_updated_at text,
              payload_json text not null default '{}'
            )
            """
        )
        conn.execute(
            """
            create table if not exists micro_training_backfill_runs (
              backfill_id text primary key,
              generated_at text not null,
              audit_db_path text,
              training_db_path text,
              mode text not null,
              summary_json text not null
            )
            """
        )
        conn.execute(
            """
            create table if not exists micro_technical_reliability (
              sample_id text primary key,
              run_id text not null,
              cycle_id text,
              strategy_line text not null,
              symbol text not null,
              micro_mode text not null,
              technical_status text,
              technical_severity text,
              data_plane_ready integer,
              alignment_state text,
              z_state text,
              book_cost_confidence text,
              technical_reason_codes_json text not null default '[]',
              technical_reliability_score real,
              is_training_usable integer,
              not_training_usable_reason text,
              payload_json text not null default '{}',
              updated_at text not null
            )
            """
        )
        conn.execute("create index if not exists idx_micro_run_samples_generated on micro_run_samples(generated_at desc)")
        conn.execute("create index if not exists idx_micro_symbol_samples_run on micro_symbol_samples(run_id, strategy_line, symbol)")
        conn.execute("create index if not exists idx_micro_symbol_samples_symbol on micro_symbol_samples(symbol, generated_at desc)")
        conn.execute("create index if not exists idx_micro_downstream_labels_run on micro_downstream_labels(run_id, strategy_line, symbol)")
        conn.execute("create index if not exists idx_micro_technical_reliability_run on micro_technical_reliability(run_id, strategy_line, symbol)")
        existing_symbol_cols = columns(conn, "micro_symbol_samples")
        for name, col_type in MICRO_SYMBOL_OPTIONAL_COLUMNS.items():
            if name not in existing_symbol_cols:
                conn.execute(f"alter table micro_symbol_samples add column {name} {col_type}")
    return db


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("select name from sqlite_master where type='table' and name=?", (table,)).fetchone()
    return row is not None


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row["name"]) for row in conn.execute(f"pragma table_info({table})").fetchall()}


def _mode_for_line(line: str) -> str:
    return MICRO_MODES.get(str(line or ""), str(line or "unknown"))


def _status_from_row(row: dict[str, Any]) -> str:
    status = str(row.get("status") or row.get("state") or row.get("ready_state") or row.get("technical_status") or "").lower()
    severity = str(row.get("severity") or "").upper()
    if status in {"ready", "accepted", "ok", "confirmed"}:
        return "ready"
    if status in {"technical_blocked", "stale", "timeout"}:
        return status
    if severity in {"P0", "P1"}:
        return "technical_blocked"
    if status:
        return "not_ready"
    return "missing"


def _first_nested_number(payloads: list[Any], keys: tuple[str, ...]) -> float | None:
    wanted = {key.lower() for key in keys}

    def walk(value: Any) -> float | None:
        if isinstance(value, dict):
            for key, item in value.items():
                if str(key).lower() in wanted:
                    try:
                        return None if item is None else float(item)
                    except (TypeError, ValueError):
                        pass
                found = walk(item)
                if found is not None:
                    return found
        elif isinstance(value, list):
            for item in value[:20]:
                found = walk(item)
                if found is not None:
                    return found
        return None

    for payload in payloads:
        found = walk(payload)
        if found is not None:
            return found
    return None


def _stream_entry(coverage: dict[str, Any], stream: str) -> dict[str, Any]:
    direct = coverage.get(stream)
    if isinstance(direct, dict):
        return direct
    streams = coverage.get("streams")
    if isinstance(streams, dict) and isinstance(streams.get(stream), dict):
        return streams[stream]
    return {}


def _book_depth_missing_reason(row: dict[str, Any], *, missing_spread: bool, missing_depth: bool) -> str | None:
    if not missing_spread and not missing_depth:
        return None
    coverage = json_loads(row.get("stream_heartbeat_json") or row.get("stream_heartbeat") or row.get("coverage_flags_json"), {})
    runtime = json_loads(row.get("runtime_evidence_json") or row.get("runtime_evidence"), {})
    book = _stream_entry(coverage, "bookTicker")
    depth = _stream_entry(coverage, "partialDepth5")
    runtime_coverage = runtime.get("coverage") if isinstance(runtime.get("coverage"), dict) else {}
    parts: list[str] = []
    if missing_spread:
        book_gap = str(book.get("gap_class") or book.get("root_cause") or "")
        book_ratio = book.get("coverage_ratio")
        if book.get("active") is False or book.get("subscription", {}).get("active") is False:
            parts.append("bookticker_inactive")
        elif book_gap and book_gap not in {"ok", "healthy"}:
            parts.append(f"bookticker_{book_gap}")
        elif book_ratio is None and runtime_coverage.get("bookTicker") is None:
            parts.append("bookticker_missing")
        else:
            parts.append("event_time_window_no_book_frame")
    if missing_depth:
        depth_gap = str(depth.get("gap_class") or depth.get("root_cause") or "")
        depth_sub = depth.get("subscription") if isinstance(depth.get("subscription"), dict) else {}
        if depth.get("active") is False or depth_sub.get("active") is False:
            reason = depth.get("missing_reason") or depth_sub.get("missing_reason") or "inactive"
            parts.append(f"depth5_{reason}")
        elif depth_gap and depth_gap not in {"ok", "healthy"}:
            parts.append(f"depth5_{depth_gap}")
        elif depth.get("coverage_ratio") is None and runtime_coverage.get("partialDepth5") is None:
            parts.append("depth5_missing")
        else:
            parts.append("event_time_window_no_depth_frame")
    return ",".join(sorted(set(parts))) if parts else None


def _book_depth_source(nums: dict[str, float | None]) -> str | None:
    if nums.get("spread") is not None or nums.get("spread_bps") is not None:
        if nums.get("depth_imbalance") is not None:
            return "bookticker_depth_payload"
        return "bookticker_payload"
    if nums.get("depth_imbalance") is not None:
        return "depth_payload"
    return None


def _book_cost_confidence(nums: dict[str, float | None], fallback: str = "direct_run_id") -> str | None:
    return fallback if _book_depth_source(nums) else None


def _extract_numbers(row: dict[str, Any]) -> dict[str, float | None]:
    payload = json_loads(row.get("payload_json") or row.get("payload"), {})
    factor = json_loads(row.get("factor_frame_json") or row.get("factor_frame") or payload.get("factor_frame_json") or payload.get("factor_frame"), {})
    runtime = json_loads(row.get("runtime_evidence_json") or row.get("runtime_evidence") or payload.get("runtime_evidence_json") or payload.get("runtime_evidence"), {})
    coverage = json_loads(
        row.get("stream_heartbeat_json")
        or row.get("stream_heartbeat")
        or row.get("coverage_flags_json")
        or payload.get("stream_heartbeat_json")
        or payload.get("stream_heartbeat")
        or payload.get("coverage_flags_json"),
        {},
    )
    book_depth = runtime.get("book_depth_runtime") if isinstance(runtime.get("book_depth_runtime"), dict) else {}
    agg = runtime.get("aggtrade_runtime") if isinstance(runtime.get("aggtrade_runtime"), dict) else {}
    bookticker = coverage.get("bookTicker") if isinstance(coverage.get("bookTicker"), dict) else {}
    aggtrade = coverage.get("aggTrade") if isinstance(coverage.get("aggTrade"), dict) else {}
    partial_depth = coverage.get("partialDepth5") if isinstance(coverage.get("partialDepth5"), dict) else {}
    streams = coverage.get("streams") if isinstance(coverage.get("streams"), dict) else {}
    bookticker_stream = streams.get("bookTicker") if isinstance(streams.get("bookTicker"), dict) else {}
    aggtrade_stream = streams.get("aggTrade") if isinstance(streams.get("aggTrade"), dict) else {}
    partial_depth_stream = streams.get("partialDepth5") if isinstance(streams.get("partialDepth5"), dict) else {}
    payloads = [factor, runtime, coverage, payload, row]
    bid_price = _first_nested_number(payloads, ("bid_price", "best_bid", "bid", "b"))
    ask_price = _first_nested_number(payloads, ("ask_price", "best_ask", "ask", "a"))
    spread_bps = (
        factor.get("spread_bps")
        or runtime.get("spread_bps")
        or _first_nested_number(payloads, ("spread_bps", "top_spread_bps"))
    )
    if spread_bps is None and bid_price and ask_price and bid_price > 0 and ask_price > 0:
        mid = (bid_price + ask_price) / 2
        spread_bps = ((ask_price - bid_price) / mid) * 10000 if mid > 0 else None
    bid_depth_usdt = _first_nested_number(
        payloads,
        ("bid_depth_usdt", "top_bid_depth_usdt", "bid_notional", "bid_depth_notional", "bid_liquidity_usdt"),
    )
    ask_depth_usdt = _first_nested_number(
        payloads,
        ("ask_depth_usdt", "top_ask_depth_usdt", "ask_notional", "ask_depth_notional", "ask_liquidity_usdt"),
    )
    depth_imbalance = (
        factor.get("depth_imbalance")
        or runtime.get("depth_imbalance")
        or _first_nested_number(payloads, ("depth_imbalance", "book_depth_imbalance", "depth5_imbalance"))
    )
    if depth_imbalance is None and bid_depth_usdt is not None and ask_depth_usdt is not None:
        denom = bid_depth_usdt + ask_depth_usdt
        depth_imbalance = (bid_depth_usdt - ask_depth_usdt) / denom if denom else None
    out = {
        "cvd": factor.get("cvd") if factor.get("cvd") is not None else row.get("cvd"),
        "ofi": factor.get("ofi") if factor.get("ofi") is not None else row.get("ofi"),
        "z_cvd": factor.get("z_cvd") if factor.get("z_cvd") is not None else row.get("z_cvd"),
        "z_ofi": factor.get("z_ofi") if factor.get("z_ofi") is not None else row.get("z_ofi"),
        "spread": factor.get("spread") or runtime.get("spread") or row.get("spread") or spread_bps,
        "spread_bps": spread_bps,
        "depth_imbalance": depth_imbalance if depth_imbalance is not None else row.get("depth_imbalance"),
        "bid_depth_usdt": bid_depth_usdt,
        "ask_depth_usdt": ask_depth_usdt,
        "bookticker_age_ms": book_depth.get("bookticker_age_ms") or bookticker.get("last_age_ms") or bookticker_stream.get("last_age_ms"),
        "aggtrade_age_ms": agg.get("aggtrade_age_ms") or aggtrade.get("last_age_ms") or aggtrade_stream.get("last_age_ms"),
        "depth_age_ms": book_depth.get("depth_age_ms") or partial_depth.get("last_age_ms") or partial_depth_stream.get("last_age_ms"),
        "top_book_age_ms": book_depth.get("bookticker_age_ms") or bookticker.get("last_age_ms") or bookticker_stream.get("last_age_ms"),
    }
    clean: dict[str, float | None] = {}
    for key, value in out.items():
        try:
            clean[key] = None if value is None else float(value)
        except (TypeError, ValueError):
            clean[key] = None
    return clean


def _as_float(value: Any) -> float | None:
    try:
        return None if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    try:
        return None if value is None or value == "" else int(float(value))
    except (TypeError, ValueError):
        return None


def _truthy_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if value else 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "ok", "ready", "fresh", "healthy", "active"}:
        return 1
    if text in {"0", "false", "no", "not_ready", "stale", "inactive", "missing"}:
        return 0
    return None


def _epoch_to_iso(value: Any) -> str | None:
    num = _as_float(value)
    if num is None:
        return None
    if num > 10_000_000_000:
        num = num / 1000.0
    try:
        return datetime.fromtimestamp(num, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (OSError, OverflowError, ValueError):
        return None


def _nested_get(value: Any, path: tuple[str, ...]) -> Any:
    cur = value
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _first_present(payloads: list[Any], paths: tuple[tuple[str, ...], ...]) -> Any:
    for payload in payloads:
        for path in paths:
            found = _nested_get(payload, path)
            if found is not None:
                return found
    return None


def _stream_contract_fields(data: dict[str, Any]) -> dict[str, Any]:
    payload = json_loads(data.get("payload_json") or data.get("payload"), {})
    coverage = json_loads(
        data.get("stream_heartbeat_json")
        or data.get("stream_heartbeat")
        or data.get("coverage_flags_json")
        or payload.get("stream_heartbeat_json")
        or payload.get("stream_heartbeat"),
        {},
    )
    runtime = json_loads(
        data.get("runtime_evidence_json") or data.get("runtime_evidence") or payload.get("runtime_evidence_json") or payload.get("runtime_evidence"),
        {},
    )
    stream_names = ("aggTrade", "bookTicker", "partialDepth5")
    entries = {name: _stream_entry(coverage, name) for name in stream_names}
    streams = coverage.get("streams") if isinstance(coverage.get("streams"), dict) else {}
    active_entries = []
    last_event_values = []
    gap_states = []
    route_mismatches = []
    for name in stream_names:
        entry = entries[name]
        if not entry and isinstance(streams.get(name), dict):
            entry = streams[name]
        active = _truthy_int(entry.get("active") if isinstance(entry, dict) else None)
        if active:
            active_entries.append(name)
        last_event = (
            entry.get("last_event_ts_sec")
            or entry.get("last_event_at")
            or entry.get("last_event_ts")
            or entry.get("last_ts")
        )
        if last_event is not None:
            last_event_values.append(last_event)
        gap = entry.get("gap_class") or entry.get("root_cause") or entry.get("stream_gap_state")
        if gap:
            gap_states.append(str(gap))
        route = str(entry.get("route") or entry.get("actual_route") or "")
        expected_route = "market" if name == "aggTrade" else "public"
        if route and route != expected_route:
            route_mismatches.append(f"{name}:{route}!={expected_route}")
    last_event_raw = max((_as_float(value) or 0 for value in last_event_values), default=0)
    silent_age = _as_float(_first_present([coverage, runtime], (("ws_silent_age_sec",), ("silent_age_sec",), ("stream_silent_age_sec",))))
    if silent_age is None:
        ages = []
        for name in stream_names:
            entry = entries[name]
            age = _as_float(entry.get("last_age_ms") if isinstance(entry, dict) else None)
            if age is not None:
                ages.append(age / 1000.0)
        silent_age = max(ages) if ages else None
    connected_but_no_emit = 1 if active_entries and not last_event_values else 0
    route_ok = 0 if route_mismatches else 1
    stream_label = ",".join(active_entries) if active_entries else ",".join(stream_names)
    return {
        "ws_route_ok": route_ok,
        "ws_stream": stream_label,
        "ws_last_event_at": _epoch_to_iso(last_event_raw) if last_event_raw else None,
        "ws_last_pong_at": _epoch_to_iso(_first_present([coverage, runtime], (("last_pong_ts_sec",), ("ws_last_pong_ts_sec",)))),
        "ws_silent_age_sec": silent_age,
        "ws_subscribe_throttle_state": _first_present([coverage, runtime], (("subscribe_throttle_state",), ("ws_subscribe_throttle_state",))) or "unknown",
        "stream_gap_state": ",".join(sorted(set(gap_states))) if gap_states else "unknown",
        "route_mismatch": ",".join(route_mismatches) if route_mismatches else None,
        "ws_connected_but_no_emit": connected_but_no_emit,
    }


def _data_plane_fields(data: dict[str, Any], status: str, raw_reasons: list[Any]) -> dict[str, Any]:
    payload = json_loads(data.get("payload_json") or data.get("payload"), {})
    runtime = json_loads(
        data.get("runtime_evidence_json") or data.get("runtime_evidence") or payload.get("runtime_evidence_json") or payload.get("runtime_evidence"),
        {},
    )
    health = runtime.get("health_state") if isinstance(runtime.get("health_state"), dict) else {}
    daemon = runtime.get("daemon_status") if isinstance(runtime.get("daemon_status"), dict) else {}
    data_plane = runtime.get("data_plane") if isinstance(runtime.get("data_plane"), dict) else {}
    reason_text = ",".join(str(reason) for reason in raw_reasons)
    ready_value = (
        data_plane.get("ready")
        if data_plane
        else _first_present([runtime, health, daemon], (("micro_data_plane_ready",), ("data_plane_ready",), ("ready",)))
    )
    ready = _truthy_int(ready_value)
    technical_reasons = {"ofi_stale", "cvd_stale", "ofi_cvd_lag_high", "coverage_aggtrade_weak", "coverage_bookticker_weak", "coverage_depth5_weak"}
    if ready is None:
        ready = 0 if status in {"technical_blocked", "stale", "timeout"} or any(reason in reason_text for reason in technical_reasons) else 1
    warming = 1 if status in {"warmup", "data_incomplete"} or "warmup" in reason_text or "target_too_young" in reason_text else 0
    ready_age = _as_float(_first_present([data_plane, runtime, health], (("ready_age_sec",), ("data_plane_ready_age_sec",), ("age_sec",))))
    hydrated = _truthy_int(_first_present([data_plane, runtime, health], (("target_set_hydrated",), ("targets_hydrated",), ("target_set_ready",))))
    if hydrated is None:
        hydrated = 0 if "target_set" in reason_text and "missing" in reason_text else 1
    block_reason = _first_present([data_plane, runtime], (("readiness_block_reason",), ("block_reason",)))
    if not block_reason and ready == 0:
        block_reason = raw_reasons[0] if raw_reasons else status
    return {
        "micro_data_plane_ready": ready,
        "post_restart_warming": warming,
        "data_plane_ready_at_read": ready,
        "data_plane_ready_age_sec": ready_age,
        "target_set_hydrated": hydrated,
        "warmup_started_at": _first_present([data_plane, runtime], (("warmup_started_at",), ("started_at",))),
        "warmup_completed_at": _first_present([data_plane, runtime], (("warmup_completed_at",), ("completed_at",))),
        "readiness_block_reason": str(block_reason) if block_reason else None,
    }


def _bucket_alignment_fields(data: dict[str, Any]) -> dict[str, Any]:
    payload = json_loads(data.get("payload_json") or data.get("payload"), {})
    runtime = json_loads(
        data.get("runtime_evidence_json") or data.get("runtime_evidence") or payload.get("runtime_evidence_json") or payload.get("runtime_evidence"),
        {},
    )
    factor = json_loads(data.get("factor_frame_json") or data.get("factor_frame") or payload.get("factor_frame_json") or payload.get("factor_frame"), {})
    alignment = runtime.get("bucket_alignment") if isinstance(runtime.get("bucket_alignment"), dict) else {}
    coverage = runtime.get("coverage") if isinstance(runtime.get("coverage"), dict) else {}
    cvd_ts = _as_int(alignment.get("cvd_bucket_ts_sec") or alignment.get("cvd_bucket_ts") or factor.get("bucket_ts_sec"))
    ofi_ts = _as_int(alignment.get("ofi_bucket_ts_sec") or alignment.get("ofi_bucket_ts") or factor.get("bucket_ts_sec"))
    book_ts = _as_int(alignment.get("book_bucket_ts_sec") or alignment.get("book_bucket_ts") or coverage.get("bookTicker_bucket_ts"))
    depth_ts = _as_int(alignment.get("depth_bucket_ts_sec") or alignment.get("depth_bucket_ts") or coverage.get("partialDepth5_bucket_ts"))
    present = [value for value in (cvd_ts, ofi_ts, book_ts, depth_ts) if value is not None]
    common = _as_int(alignment.get("common_bucket_ts_sec") or alignment.get("common_bucket_ts"))
    if common is None and present:
        common = min(present)
    lag = _as_float(alignment.get("ofi_cvd_lag_bucket_sec") or alignment.get("bucket_lag_sec"))
    if lag is None and cvd_ts is not None and ofi_ts is not None:
        lag = abs(cvd_ts - ofi_ts)
    state = alignment.get("alignment_status") or alignment.get("commit_barrier_status")
    if not state:
        state = "aligned" if lag is not None and lag <= 1 else "lagged" if lag is not None else "unknown"
    generated = _parse_iso(data.get("generated_at"))
    watermark_age = None
    if generated and common:
        watermark_age = max((generated.timestamp() - common), 0.0)
    return {
        "cvd_bucket_ts": cvd_ts,
        "ofi_bucket_ts": ofi_ts,
        "book_bucket_ts": book_ts,
        "depth_bucket_ts": depth_ts,
        "common_bucket_ts": common,
        "bucket_lag_sec": lag,
        "alignment_state": str(state),
        "alignment_reason": str(alignment.get("true_alignment_reason") or alignment.get("alignment_reason") or "") or None,
        "watermark_age_sec": watermark_age,
    }


def _z_maturity_fields(data: dict[str, Any], nums: dict[str, float | None]) -> dict[str, Any]:
    payload = json_loads(data.get("payload_json") or data.get("payload"), {})
    runtime = json_loads(
        data.get("runtime_evidence_json") or data.get("runtime_evidence") or payload.get("runtime_evidence_json") or payload.get("runtime_evidence"),
        {},
    )
    z_window = json_loads(data.get("z_window_json") or data.get("z_window") or payload.get("z_window_json") or payload.get("z_window"), {})
    z_history = runtime.get("z_history_runtime") if isinstance(runtime.get("z_history_runtime"), dict) else {}
    fast_z = runtime.get("fast_z_continuity") if isinstance(runtime.get("fast_z_continuity"), dict) else {}
    dwell = runtime.get("candidate_dwell") if isinstance(runtime.get("candidate_dwell"), dict) else {}
    count = _as_int(_first_present([z_window, z_history, fast_z], (("bucket_count",), ("available_bucket_count",), ("valid_bucket_count",), ("history_count",))))
    ratio = _as_float(_first_present([z_window, z_history, fast_z], (("valid_bucket_ratio",), ("available_ratio",), ("ratio",))))
    target_age = _as_float(_first_present([z_window, z_history, dwell], (("target_age_sec",), ("age_sec",), ("candidate_age_sec",))))
    z_cvd_state = "z_ready" if nums.get("z_cvd") is not None else "z_missing"
    z_ofi_state = "z_ready" if nums.get("z_ofi") is not None else "z_missing"
    missing_reason = _first_present([z_window, z_history, fast_z], (("missing_reason",), ("reason",), ("root_cause",)))
    if z_cvd_state == "z_ready" and z_ofi_state == "z_ready":
        z_state = "z_ready"
    elif z_cvd_state == "z_ready" or z_ofi_state == "z_ready":
        z_state = "z_partial_one_side"
    elif target_age is not None and target_age < 90:
        z_state = "z_target_too_young"
        missing_reason = missing_reason or "target_too_young"
    elif ratio is not None and ratio < 0.5:
        z_state = "z_valid_ratio_low"
        missing_reason = missing_reason or "valid_bucket_ratio_low"
    else:
        z_state = str(_first_present([z_window, z_history, fast_z], (("z_state",), ("state",)))) if _first_present([z_window, z_history, fast_z], (("z_state",), ("state",))) else "z_feature_missing"
    return {
        "z_state": z_state,
        "z_cvd_state": z_cvd_state,
        "z_ofi_state": z_ofi_state,
        "z_window_bucket_count": count,
        "z_valid_bucket_ratio": ratio,
        "z_target_age_sec": target_age,
        "z_dwell_pending_until": dwell.get("pending_until") if isinstance(dwell, dict) else None,
        "z_missing_reason": str(missing_reason) if missing_reason else None,
    }


def _book_event_fields(data: dict[str, Any], nums: dict[str, float | None]) -> dict[str, Any]:
    payload = json_loads(data.get("payload_json") or data.get("payload"), {})
    coverage = json_loads(
        data.get("stream_heartbeat_json")
        or data.get("stream_heartbeat")
        or data.get("coverage_flags_json")
        or payload.get("stream_heartbeat_json")
        or payload.get("stream_heartbeat"),
        {},
    )
    runtime = json_loads(
        data.get("runtime_evidence_json") or data.get("runtime_evidence") or payload.get("runtime_evidence_json") or payload.get("runtime_evidence"),
        {},
    )
    book = _stream_entry(coverage, "bookTicker")
    depth = _stream_entry(coverage, "partialDepth5")
    book_runtime = runtime.get("book_depth_runtime") if isinstance(runtime.get("book_depth_runtime"), dict) else {}
    top_ts = _as_int(book.get("last_event_ts_sec") or book.get("event_ts_sec") or book_runtime.get("top_book_event_ts"))
    depth_ts = _as_int(depth.get("last_event_ts_sec") or depth.get("event_ts_sec") or book_runtime.get("depth_event_ts"))
    gap = book_runtime.get("book_gap_state") or book_runtime.get("ofi_gap_class") or book.get("gap_class") or depth.get("gap_class")
    return {
        "top_book_event_ts": top_ts,
        "depth_event_ts": depth_ts,
        "book_update_id": str(book.get("update_id") or book_runtime.get("book_update_id") or "") or None,
        "depth_update_id": str(depth.get("update_id") or book_runtime.get("depth_update_id") or "") or None,
        "book_gap_state": str(gap) if gap else None,
        "book_cost_confidence": _book_cost_confidence(nums) or "missing",
    }


def _technical_fields(data: dict[str, Any], nums: dict[str, float | None], status: str, raw_reasons: list[Any]) -> dict[str, Any]:
    stream = _stream_contract_fields(data)
    data_plane = _data_plane_fields(data, status, raw_reasons)
    bucket = _bucket_alignment_fields(data)
    z_fields = _z_maturity_fields(data, nums)
    book = _book_event_fields(data, nums)
    reason_codes = [str(reason) for reason in raw_reasons]
    p0_tokens = ("stale", "lag_high", "coverage_", "missing", "not_ready", "technical")
    severity = "P0" if status in {"technical_blocked", "stale", "timeout"} or any(any(token in reason for token in p0_tokens) for reason in reason_codes) else "P2"
    score = 1.0
    if data_plane.get("micro_data_plane_ready") == 0:
        score -= 0.35
    if bucket.get("alignment_state") not in {"aligned", "ready", "ok", "fresh"}:
        score -= 0.2
    if str(z_fields.get("z_state")) != "z_ready":
        score -= 0.15
    if book.get("book_cost_confidence") == "missing":
        score -= 0.1
    if stream.get("ws_connected_but_no_emit"):
        score -= 0.2
    score = max(round(score, 4), 0.0)
    usable = 1 if score >= 0.7 and data_plane.get("micro_data_plane_ready") == 1 and not stream.get("ws_connected_but_no_emit") else 0
    not_usable_reason = None
    if not usable:
        not_usable_reason = (
            data_plane.get("readiness_block_reason")
            or z_fields.get("z_missing_reason")
            or bucket.get("alignment_reason")
            or book.get("book_gap_state")
            or (reason_codes[0] if reason_codes else "technical_reliability_below_threshold")
        )
    return {
        **stream,
        **data_plane,
        **bucket,
        **z_fields,
        **book,
        "technical_status": status,
        "technical_severity": severity,
        "technical_reason_codes_json": json_dumps(reason_codes),
        "technical_reliability_score": score,
        "is_training_usable": usable,
        "not_training_usable_reason": str(not_usable_reason) if not_usable_reason else None,
    }


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def upsert_run_sample(conn: sqlite3.Connection, row: dict[str, Any], *, source_refs: dict[str, Any] | None = None) -> None:
    now = utc_now_iso()
    run_id = str(row.get("run_id") or "unknown")
    summary = json_loads(row.get("summary_json") or row.get("summary"), {})
    for line in MICRO_MODES:
        conn.execute(
            """
            insert into micro_run_samples(
              run_id, cycle_id, strategy_line, target_set_id, micro_mode,
              started_at, ended_at, generated_at, status, health_state,
              reason_codes_json, source_confidence, missing_reason,
              source_refs_json, payload_json, updated_at
            ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            on conflict(run_id, strategy_line, micro_mode) do update set
              cycle_id=excluded.cycle_id,
              generated_at=excluded.generated_at,
              status=excluded.status,
              health_state=excluded.health_state,
              reason_codes_json=excluded.reason_codes_json,
              source_confidence=excluded.source_confidence,
              missing_reason=excluded.missing_reason,
              source_refs_json=excluded.source_refs_json,
              payload_json=excluded.payload_json,
              updated_at=excluded.updated_at
            """,
            (
                run_id,
                row.get("cycle_id"),
                line,
                None,
                _mode_for_line(line),
                None,
                row.get("generated_at"),
                row.get("generated_at"),
                "missing",
                None,
                "[]",
                "direct_run_id",
                None,
                json_dumps(source_refs or {"audit_table": "micro_evidence_runtime_v2_runs"}),
                json_dumps({"summary": summary, "source": row.get("source")}),
                now,
            ),
        )


def upsert_symbol_sample(conn: sqlite3.Connection, row: dict[str, Any], *, source_refs: dict[str, Any] | None = None) -> str:
    now = utc_now_iso()
    data = dict(row)
    run_id = str(data.get("run_id") or "unknown")
    strategy_line = str(data.get("strategy_line") or data.get("line") or "unknown")
    symbol = str(data.get("symbol") or "").upper()
    micro_mode = _mode_for_line(strategy_line)
    runtime = json_loads(data.get("runtime_evidence_json") or data.get("runtime_evidence"), {})
    raw_reasons = json_loads(data.get("raw_reasons_json") or data.get("raw_reasons"), [])
    factor = json_loads(data.get("factor_frame_json") or data.get("factor_frame"), {})
    target_set_id = factor.get("target_set_id") or runtime.get("target_set_id")
    sample_id = sample_id_for(
        run_id=run_id,
        strategy_line=strategy_line,
        micro_mode=micro_mode,
        symbol=symbol,
        target_set_id=str(target_set_id or ""),
    )
    nums = _extract_numbers(data)
    status = _status_from_row(data)
    accepted = 1 if status == "ready" else 0
    blocked = 1 if status in {"not_ready", "technical_blocked", "stale", "timeout"} else 0
    missing_spread = nums["spread"] is None and nums.get("spread_bps") is None
    missing_depth = nums["depth_imbalance"] is None
    depth_missing_reason = _book_depth_missing_reason(data, missing_spread=missing_spread, missing_depth=missing_depth)
    depth_source = _book_depth_source(nums)
    book_cost_confidence = _book_cost_confidence(nums)
    technical = _technical_fields(data, nums, status, raw_reasons)
    if technical.get("book_cost_confidence") and not book_cost_confidence:
        book_cost_confidence = technical.get("book_cost_confidence")
    conn.execute(
        """
        insert into micro_symbol_samples(
          sample_id, run_id, cycle_id, strategy_line, symbol, side,
          target_source, target_set_id, micro_mode, ready_state, confirmation_state,
          accepted, blocked, cvd, ofi, z_cvd, z_ofi, spread, depth_imbalance,
          bookticker_age_ms, aggtrade_age_ms, depth_age_ms, coverage_flags_json,
          reason_codes_json, source_confidence, missing_reason, generated_at,
          spread_bps, bid_depth_usdt, ask_depth_usdt, top_book_age_ms,
          depth_source, depth_missing_reason, book_cost_confidence,
          source_refs_json, payload_json, updated_at
        ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        on conflict(sample_id) do update set
          cycle_id=excluded.cycle_id,
          ready_state=excluded.ready_state,
          confirmation_state=excluded.confirmation_state,
          accepted=excluded.accepted,
          blocked=excluded.blocked,
          cvd=excluded.cvd,
          ofi=excluded.ofi,
          z_cvd=excluded.z_cvd,
          z_ofi=excluded.z_ofi,
          spread=excluded.spread,
          depth_imbalance=excluded.depth_imbalance,
          bookticker_age_ms=excluded.bookticker_age_ms,
          aggtrade_age_ms=excluded.aggtrade_age_ms,
          depth_age_ms=excluded.depth_age_ms,
          coverage_flags_json=excluded.coverage_flags_json,
          reason_codes_json=excluded.reason_codes_json,
          source_confidence=excluded.source_confidence,
          missing_reason=excluded.missing_reason,
          generated_at=excluded.generated_at,
          spread_bps=excluded.spread_bps,
          bid_depth_usdt=excluded.bid_depth_usdt,
          ask_depth_usdt=excluded.ask_depth_usdt,
          top_book_age_ms=excluded.top_book_age_ms,
          depth_source=excluded.depth_source,
          depth_missing_reason=excluded.depth_missing_reason,
          book_cost_confidence=excluded.book_cost_confidence,
          source_refs_json=excluded.source_refs_json,
          payload_json=excluded.payload_json,
          updated_at=excluded.updated_at
        """,
        (
            sample_id,
            run_id,
            data.get("cycle_id"),
            strategy_line,
            symbol,
            data.get("side"),
            data.get("target_source"),
            target_set_id,
            micro_mode,
            data.get("state") or data.get("status"),
            data.get("confirmation_state"),
            accepted,
            blocked,
            nums["cvd"],
            nums["ofi"],
            nums["z_cvd"],
            nums["z_ofi"],
            nums["spread"],
            nums["depth_imbalance"],
            nums["bookticker_age_ms"],
            nums["aggtrade_age_ms"],
            nums["depth_age_ms"],
            json_dumps(data.get("stream_heartbeat") or json_loads(data.get("stream_heartbeat_json"), {})),
            json_dumps(raw_reasons),
            "direct_run_id",
            depth_missing_reason,
            data.get("generated_at"),
            nums.get("spread_bps"),
            nums.get("bid_depth_usdt"),
            nums.get("ask_depth_usdt"),
            nums.get("top_book_age_ms"),
            depth_source,
            depth_missing_reason,
            book_cost_confidence,
            json_dumps(source_refs or {"audit_table": "micro_evidence_runtime_v2_symbols"}),
            json_dumps({k: v for k, v in data.items() if k not in {"payload_json"}}),
            now,
        ),
    )
    _update_symbol_technical_fields(conn, sample_id, technical, now=now)
    _upsert_technical_reliability(
        conn,
        sample_id=sample_id,
        run_id=run_id,
        cycle_id=data.get("cycle_id"),
        strategy_line=strategy_line,
        symbol=symbol,
        micro_mode=micro_mode,
        technical=technical,
        now=now,
    )
    return sample_id


def _update_symbol_technical_fields(conn: sqlite3.Connection, sample_id: str, technical: dict[str, Any], *, now: str) -> None:
    columns_to_update = [name for name in MICRO_SYMBOL_OPTIONAL_COLUMNS if name in technical]
    if not columns_to_update:
        return
    set_clause = ", ".join([f"{name}=?" for name in columns_to_update] + ["updated_at=?"])
    values = [technical.get(name) for name in columns_to_update] + [now, sample_id]
    conn.execute(f"update micro_symbol_samples set {set_clause} where sample_id=?", values)


def _upsert_technical_reliability(
    conn: sqlite3.Connection,
    *,
    sample_id: str,
    run_id: str,
    cycle_id: Any,
    strategy_line: str,
    symbol: str,
    micro_mode: str,
    technical: dict[str, Any],
    now: str,
) -> None:
    conn.execute(
        """
        insert into micro_technical_reliability(
          sample_id, run_id, cycle_id, strategy_line, symbol, micro_mode,
          technical_status, technical_severity, data_plane_ready,
          alignment_state, z_state, book_cost_confidence,
          technical_reason_codes_json, technical_reliability_score,
          is_training_usable, not_training_usable_reason, payload_json, updated_at
        ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        on conflict(sample_id) do update set
          cycle_id=excluded.cycle_id,
          technical_status=excluded.technical_status,
          technical_severity=excluded.technical_severity,
          data_plane_ready=excluded.data_plane_ready,
          alignment_state=excluded.alignment_state,
          z_state=excluded.z_state,
          book_cost_confidence=excluded.book_cost_confidence,
          technical_reason_codes_json=excluded.technical_reason_codes_json,
          technical_reliability_score=excluded.technical_reliability_score,
          is_training_usable=excluded.is_training_usable,
          not_training_usable_reason=excluded.not_training_usable_reason,
          payload_json=excluded.payload_json,
          updated_at=excluded.updated_at
        """,
        (
            sample_id,
            run_id,
            cycle_id,
            strategy_line,
            symbol,
            micro_mode,
            technical.get("technical_status"),
            technical.get("technical_severity"),
            technical.get("micro_data_plane_ready"),
            technical.get("alignment_state"),
            technical.get("z_state"),
            technical.get("book_cost_confidence"),
            technical.get("technical_reason_codes_json") or "[]",
            technical.get("technical_reliability_score"),
            technical.get("is_training_usable"),
            technical.get("not_training_usable_reason"),
            json_dumps(technical),
            now,
        ),
    )


def ingest_runtime_v2_payload(
    root: Path | None = None,
    *,
    payload: dict[str, Any],
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Mirror one runtime-v2 evidence payload into the training ledger.

    This is a non-authoritative sidecar writer. It intentionally returns counts
    only and does not change the original payload or any strategy decision.
    """
    pr = project_root(root)
    db = init_micro_training_db(db_path, root=pr)
    run_id = str(payload.get("run_id") or "unknown")
    run_row = {
        "run_id": run_id,
        "cycle_id": payload.get("cycle_id"),
        "generated_at": payload.get("generated_at"),
        "summary": payload.get("summary") or {},
        "source": payload.get("source") or "micro_evidence_runtime_v2_payload",
    }
    symbols = payload.get("symbols") or []
    written_symbols = 0
    with connect(db) as conn:
        upsert_run_sample(conn, run_row, source_refs={"payload": "micro_evidence_runtime_v2"})
        for item in symbols:
            row = dict(item)
            row["run_id"] = run_id
            row["cycle_id"] = payload.get("cycle_id")
            row["generated_at"] = payload.get("generated_at")
            sample_id = upsert_symbol_sample(conn, row, source_refs={"payload": "micro_evidence_runtime_v2"})
            status = str(row.get("status") or row.get("state") or "")
            executable = 1 if status in {"ready", "accepted", "confirmed"} else 0 if status else None
            upsert_downstream_label(
                conn,
                {
                    "sample_id": sample_id,
                    "run_id": row.get("run_id"),
                    "cycle_id": row.get("cycle_id"),
                    "strategy_line": row.get("strategy_line") or row.get("line"),
                    "symbol": row.get("symbol"),
                    "executable": executable,
                    "label_source": "runtime_micro_v2_sidecar",
                },
            )
            written_symbols += 1
    return {"status": "ok", "run_id": run_id, "db_path": str(db), "symbol_rows": written_symbols}


def upsert_downstream_label(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        insert into micro_downstream_labels(
          sample_id, run_id, cycle_id, strategy_line, symbol,
          trade_plan_status, executable, trade_plan_ref, paper_order_id,
          paper_status, exit_reason, net_R, MFE_R, MAE_R,
          trade_quality_root_cause, label_source, label_updated_at, payload_json
        ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        on conflict(sample_id) do update set
          trade_plan_status=coalesce(excluded.trade_plan_status, micro_downstream_labels.trade_plan_status),
          executable=coalesce(excluded.executable, micro_downstream_labels.executable),
          trade_plan_ref=coalesce(excluded.trade_plan_ref, micro_downstream_labels.trade_plan_ref),
          paper_order_id=coalesce(excluded.paper_order_id, micro_downstream_labels.paper_order_id),
          paper_status=coalesce(excluded.paper_status, micro_downstream_labels.paper_status),
          exit_reason=coalesce(excluded.exit_reason, micro_downstream_labels.exit_reason),
          net_R=coalesce(excluded.net_R, micro_downstream_labels.net_R),
          MFE_R=coalesce(excluded.MFE_R, micro_downstream_labels.MFE_R),
          MAE_R=coalesce(excluded.MAE_R, micro_downstream_labels.MAE_R),
          trade_quality_root_cause=coalesce(excluded.trade_quality_root_cause, micro_downstream_labels.trade_quality_root_cause),
          label_source=excluded.label_source,
          label_updated_at=excluded.label_updated_at,
          payload_json=excluded.payload_json
        """,
        (
            row["sample_id"],
            row["run_id"],
            row.get("cycle_id"),
            row["strategy_line"],
            row["symbol"],
            row.get("trade_plan_status"),
            row.get("executable"),
            row.get("trade_plan_ref"),
            row.get("paper_order_id"),
            row.get("paper_status"),
            row.get("exit_reason"),
            row.get("net_R"),
            row.get("MFE_R"),
            row.get("MAE_R"),
            row.get("trade_quality_root_cause"),
            row.get("label_source") or "micro_training_backfill",
            now,
            json_dumps(row.get("payload") or {}),
        ),
    )


def backfill_from_audit(
    root: Path | None = None,
    *,
    audit_db_path: Path | None = None,
    training_db_path: Path | None = None,
    limit_runs: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    pr = project_root(root)
    audit_db = audit_db_path or default_audit_db(pr)
    training_db = init_micro_training_db(training_db_path, root=pr)
    generated_at = utc_now_iso()
    summary: dict[str, Any] = {
        "audit_db_path": str(audit_db),
        "training_db_path": str(training_db),
        "dry_run": dry_run,
        "run_rows": 0,
        "symbol_rows": 0,
        "label_rows": 0,
        "missing": [],
    }
    if not audit_db.is_file():
        summary["missing"].append("audit_db_missing")
        return summary
    with sqlite3.connect(f"file:{audit_db.as_posix()}?mode=ro", uri=True) as audit_conn:
        audit_conn.row_factory = sqlite3.Row
        if not table_exists(audit_conn, "micro_evidence_runtime_v2_runs"):
            summary["missing"].append("micro_evidence_runtime_v2_runs_missing")
            return summary
        run_sql = "select * from micro_evidence_runtime_v2_runs order by generated_at desc"
        if limit_runs:
            run_sql += f" limit {max(1, int(limit_runs))}"
        run_rows = [dict(row) for row in audit_conn.execute(run_sql).fetchall()]
        run_ids = [str(row.get("run_id") or "") for row in run_rows if row.get("run_id")]
        symbol_rows: list[dict[str, Any]] = []
        if table_exists(audit_conn, "micro_evidence_runtime_v2_symbols") and run_ids:
            if limit_runs:
                wanted = set(run_ids)
                symbol_rows = [
                    dict(row)
                    for row in audit_conn.execute(
                        "select * from micro_evidence_runtime_v2_symbols order by generated_at desc"
                    ).fetchall()
                    if str(row["run_id"]) in wanted
                ]
            else:
                symbol_rows = [
                    dict(row)
                    for row in audit_conn.execute(
                        "select * from micro_evidence_runtime_v2_symbols order by generated_at desc"
                    ).fetchall()
                ]
        summary["run_rows"] = len(run_rows)
        summary["symbol_rows"] = len(symbol_rows)
        if dry_run:
            return summary
        with connect(training_db) as conn:
            for row in run_rows:
                upsert_run_sample(conn, row)
            for row in symbol_rows:
                sample_id = upsert_symbol_sample(conn, row)
                executable = None
                status = str(row.get("status") or row.get("state") or "")
                if status:
                    executable = 1 if status in {"ready", "accepted", "confirmed"} else 0
                upsert_downstream_label(
                    conn,
                    {
                        "sample_id": sample_id,
                        "run_id": row.get("run_id"),
                        "cycle_id": row.get("cycle_id"),
                        "strategy_line": row.get("strategy_line"),
                        "symbol": row.get("symbol"),
                        "trade_plan_status": None,
                        "executable": executable,
                        "label_source": "audit_micro_runtime_v2",
                    },
                )
                summary["label_rows"] += 1
            backfill_id = hashlib.sha1(f"{generated_at}|{len(run_rows)}|{len(symbol_rows)}".encode("utf-8")).hexdigest()[:16]
            conn.execute(
                """
                insert or replace into micro_training_backfill_runs(
                  backfill_id, generated_at, audit_db_path, training_db_path, mode, summary_json
                ) values(?,?,?,?,?,?)
                """,
                (backfill_id, generated_at, str(audit_db), str(training_db), "apply", json_dumps(summary)),
            )
    return summary


def enrich_from_audit_factor_frames(
    root: Path | None = None,
    *,
    audit_db_path: Path | None = None,
    training_db_path: Path | None = None,
    limit_runs: int = 500,
    max_lag_sec: int = 900,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Fill CVD/OFI/z fields from the persistent factor-frame store.

    Historical runtime evidence did not always carry raw factor values. The
    factor-frame store has symbol/time series but not run_id, so this function
    uses a bounded event-time window and records that weaker confidence.
    """
    pr = project_root(root)
    audit_db = audit_db_path or default_audit_db(pr)
    training_db = init_micro_training_db(training_db_path, root=pr)
    summary: dict[str, Any] = {
        "audit_db_path": str(audit_db),
        "training_db_path": str(training_db),
        "limit_runs": limit_runs,
        "max_lag_sec": max_lag_sec,
        "dry_run": dry_run,
        "candidate_samples": 0,
        "factor_frame_rows": 0,
        "updated_samples": 0,
        "updated_book_cost_samples": 0,
        "still_missing_samples": 0,
        "book_depth_missing_reason_samples": 0,
        "missing": [],
    }
    if not audit_db.is_file():
        summary["missing"].append("audit_db_missing")
        return summary
    with connect(training_db) as train_conn:
        latest_runs = [
            row["run_id"]
            for row in train_conn.execute(
                """
                select run_id
                from micro_run_samples
                group by run_id
                order by max(generated_at) desc
                limit ?
                """,
                (max(1, int(limit_runs or 500)),),
            ).fetchall()
        ]
        if not latest_runs:
            summary["missing"].append("training_runs_missing")
            return summary
        placeholders = ",".join(["?"] * len(latest_runs))
        samples = [
            dict(row)
            for row in train_conn.execute(
                f"""
                select *
                from micro_symbol_samples
                where run_id in ({placeholders})
                  and strategy_line in ('micro_fast', 'micro_full')
                order by generated_at desc
                """,
                tuple(latest_runs),
            ).fetchall()
        ]
    summary["candidate_samples"] = len(samples)
    if not samples:
        return summary
    symbols = sorted({row["symbol"] for row in samples if row.get("symbol")})
    min_dt = min((_parse_iso(row.get("generated_at")) for row in samples if _parse_iso(row.get("generated_at"))), default=None)
    max_dt = max((_parse_iso(row.get("generated_at")) for row in samples if _parse_iso(row.get("generated_at"))), default=None)
    if min_dt is None or max_dt is None:
        summary["missing"].append("sample_generated_at_missing")
        return summary
    start_iso = _iso(min_dt - timedelta(seconds=max_lag_sec))
    end_iso = _iso(max_dt + timedelta(seconds=5))
    frames_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    with sqlite3.connect(f"file:{audit_db.as_posix()}?mode=ro", uri=True) as audit_conn:
        audit_conn.row_factory = sqlite3.Row
        if not table_exists(audit_conn, "micro_factor_frames"):
            summary["missing"].append("micro_factor_frames_missing")
            return summary
        symbol_placeholders = ",".join(["?"] * len(symbols))
        query = f"""
            select strategy_line, symbol, bucket_ts_sec, generated_at, cvd, ofi, z_cvd, z_ofi,
                   cvd_available, ofi_available, z_cvd_available, z_ofi_available, payload_json
            from micro_factor_frames
            where strategy_line in ('micro_fast', 'micro_full')
              and symbol in ({symbol_placeholders})
              and generated_at >= ?
              and generated_at <= ?
            order by strategy_line, symbol, generated_at
        """
        params: list[Any] = [*symbols, start_iso, end_iso]
        for row in audit_conn.execute(query, params):
            item = dict(row)
            frames_by_key.setdefault((str(item["strategy_line"]), str(item["symbol"])), []).append(item)
            summary["factor_frame_rows"] += 1
    frame_times: dict[tuple[str, str], list[datetime]] = {}
    for key, rows in frames_by_key.items():
        frame_times[key] = [_parse_iso(row.get("generated_at")) or datetime.min.replace(tzinfo=timezone.utc) for row in rows]
    if dry_run:
        return summary
    now = utc_now_iso()
    with connect(training_db) as conn:
        for sample in samples:
            sample_dt = _parse_iso(sample.get("generated_at"))
            key = (str(sample.get("strategy_line")), str(sample.get("symbol")))
            frame = None
            if sample_dt and key in frame_times:
                times = frame_times[key]
                idx = bisect_right(times, sample_dt) - 1
                if idx >= 0:
                    candidate = frames_by_key[key][idx]
                    frame_dt = times[idx]
                    if frame_dt and 0 <= (sample_dt - frame_dt).total_seconds() <= max_lag_sec:
                        frame = candidate
            missing_metrics: list[str] = []
            updates: dict[str, Any] = {}
            frame_nums: dict[str, float | None] = {}
            if frame is not None:
                frame_payload = json_loads(frame.get("payload_json"), {})
                frame_nums = _extract_numbers({**frame, "factor_frame": frame_payload})
            for field in ("cvd", "ofi", "z_cvd", "z_ofi"):
                value = sample.get(field)
                if value is None and frame is not None and frame.get(field) is not None:
                    updates[field] = frame.get(field)
                elif value is None and (frame is None or frame.get(field) is None):
                    missing_metrics.append(field)
            for field in ("spread", "spread_bps", "depth_imbalance", "bid_depth_usdt", "ask_depth_usdt", "top_book_age_ms"):
                if sample.get(field) is None and frame_nums.get(field) is not None:
                    updates[field] = frame_nums.get(field)
                elif field in {"spread", "depth_imbalance"} and sample.get(field) is None and frame_nums.get(field) is None:
                    missing_metrics.append(field)
            source_refs = json_loads(sample.get("source_refs_json"), {})
            if updates:
                source_refs["factor_frame_join"] = {
                    "source": "micro_factor_frames",
                    "strategy_line": frame.get("strategy_line") if frame else None,
                    "symbol": frame.get("symbol") if frame else None,
                    "bucket_ts_sec": frame.get("bucket_ts_sec") if frame else None,
                    "generated_at": frame.get("generated_at") if frame else None,
                    "max_lag_sec": max_lag_sec,
                }
            source_confidence = sample.get("source_confidence") or "direct_run_id"
            if updates and source_confidence == "direct_run_id":
                source_confidence = "direct_run_id+event_time_window"
            missing_reason = None
            depth_source = sample.get("depth_source")
            book_cost_confidence = sample.get("book_cost_confidence")
            if any(field in updates for field in ("spread", "spread_bps", "depth_imbalance", "bid_depth_usdt", "ask_depth_usdt")):
                summary["updated_book_cost_samples"] += 1
                synthetic_nums = {key: sample.get(key) for key in ("spread", "spread_bps", "depth_imbalance", "bid_depth_usdt", "ask_depth_usdt", "top_book_age_ms")}
                synthetic_nums.update({key: updates.get(key) for key in synthetic_nums if updates.get(key) is not None})
                depth_source = _book_depth_source(synthetic_nums) or depth_source
                book_cost_confidence = _book_cost_confidence(synthetic_nums, "direct_run_id+event_time_window") or book_cost_confidence
            missing_spread = (updates.get("spread") is None and sample.get("spread") is None) and (updates.get("spread_bps") is None and sample.get("spread_bps") is None)
            missing_depth = updates.get("depth_imbalance") is None and sample.get("depth_imbalance") is None
            depth_missing_reason = _book_depth_missing_reason(sample, missing_spread=missing_spread, missing_depth=missing_depth)
            if depth_missing_reason:
                summary["book_depth_missing_reason_samples"] += 1
            if missing_metrics:
                missing_parts = ["missing_metrics:" + ",".join(sorted(set(missing_metrics)))]
                if depth_missing_reason:
                    missing_parts.append(depth_missing_reason)
                missing_reason = ";".join(missing_parts)
                summary["still_missing_samples"] += 1
            elif depth_missing_reason:
                missing_reason = depth_missing_reason
            conn.execute(
                """
                update micro_symbol_samples
                set cvd=coalesce(?, cvd),
                    ofi=coalesce(?, ofi),
                    z_cvd=coalesce(?, z_cvd),
                    z_ofi=coalesce(?, z_ofi),
                    spread=coalesce(?, spread),
                    spread_bps=coalesce(?, spread_bps),
                    depth_imbalance=coalesce(?, depth_imbalance),
                    bid_depth_usdt=coalesce(?, bid_depth_usdt),
                    ask_depth_usdt=coalesce(?, ask_depth_usdt),
                    top_book_age_ms=coalesce(?, top_book_age_ms),
                    source_confidence=?,
                    missing_reason=?,
                    depth_source=?,
                    depth_missing_reason=?,
                    book_cost_confidence=?,
                    source_refs_json=?,
                    updated_at=?
                where sample_id=?
                """,
                (
                    updates.get("cvd"),
                    updates.get("ofi"),
                    updates.get("z_cvd"),
                    updates.get("z_ofi"),
                    updates.get("spread"),
                    updates.get("spread_bps"),
                    updates.get("depth_imbalance"),
                    updates.get("bid_depth_usdt"),
                    updates.get("ask_depth_usdt"),
                    updates.get("top_book_age_ms"),
                    source_confidence,
                    missing_reason,
                    depth_source,
                    depth_missing_reason,
                    book_cost_confidence,
                    json_dumps(source_refs),
                    now,
                    sample["sample_id"],
                ),
            )
            if updates:
                summary["updated_samples"] += 1
    return summary


def enrich_spread_depth_missing_reasons(
    root: Path | None = None,
    *,
    training_db_path: Path | None = None,
    limit_runs: int = 500,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Normalize book/depth missing evidence for existing training samples.

    This does not fabricate historical spread/depth values. It only records
    whether the sample has usable book-cost evidence and why not.
    """
    pr = project_root(root)
    training_db = init_micro_training_db(training_db_path, root=pr)
    summary: dict[str, Any] = {
        "training_db_path": str(training_db),
        "limit_runs": limit_runs,
        "dry_run": dry_run,
        "samples": 0,
        "updated_samples": 0,
        "book_cost_samples": 0,
        "missing_reason_samples": 0,
        "depth_source_counts": {},
        "depth_missing_reason_counts": {},
    }
    with connect(training_db) as conn:
        latest_runs = [
            row["run_id"]
            for row in conn.execute(
                """
                select run_id
                from micro_run_samples
                group by run_id
                order by max(generated_at) desc
                limit ?
                """,
                (max(1, int(limit_runs or 500)),),
            ).fetchall()
        ]
        if not latest_runs:
            return summary
        placeholders = ",".join(["?"] * len(latest_runs))
        samples = [
            dict(row)
            for row in conn.execute(
                f"""
                select *
                from micro_symbol_samples
                where run_id in ({placeholders})
                  and strategy_line in ('micro_fast', 'micro_full')
                """,
                tuple(latest_runs),
            ).fetchall()
        ]
        summary["samples"] = len(samples)
        now = utc_now_iso()
        depth_source_counts: Counter[str] = Counter()
        depth_missing_reason_counts: Counter[str] = Counter()
        if dry_run:
            for sample in samples:
                nums = _extract_numbers(sample)
                depth_source = _book_depth_source(nums) or sample.get("depth_source") or "missing"
                depth_source_counts[str(depth_source)] += 1
                missing_spread = sample.get("spread") is None and sample.get("spread_bps") is None
                missing_depth = sample.get("depth_imbalance") is None
                reason = _book_depth_missing_reason(sample, missing_spread=missing_spread, missing_depth=missing_depth)
                if reason:
                    depth_missing_reason_counts[str(reason)] += 1
            summary["depth_source_counts"] = dict(depth_source_counts)
            summary["depth_missing_reason_counts"] = dict(depth_missing_reason_counts)
            return summary
        for sample in samples:
            nums = _extract_numbers(sample)
            missing_spread = sample.get("spread") is None and sample.get("spread_bps") is None
            missing_depth = sample.get("depth_imbalance") is None
            depth_source = _book_depth_source(nums) or sample.get("depth_source")
            book_cost_confidence = _book_cost_confidence(nums) or sample.get("book_cost_confidence")
            depth_missing_reason = _book_depth_missing_reason(sample, missing_spread=missing_spread, missing_depth=missing_depth)
            if not depth_source and depth_missing_reason:
                depth_source = "missing"
            if depth_source:
                depth_source_counts[str(depth_source)] += 1
            if depth_missing_reason:
                depth_missing_reason_counts[str(depth_missing_reason)] += 1
            missing_reason = sample.get("missing_reason")
            if depth_missing_reason and depth_missing_reason not in str(missing_reason or ""):
                missing_reason = ";".join(part for part in [str(missing_reason or "").strip(), depth_missing_reason] if part)
            should_update = (
                sample.get("depth_source") != depth_source
                or sample.get("depth_missing_reason") != depth_missing_reason
                or sample.get("book_cost_confidence") != book_cost_confidence
                or sample.get("missing_reason") != missing_reason
            )
            if should_update:
                conn.execute(
                    """
                    update micro_symbol_samples
                    set depth_source=?,
                        depth_missing_reason=?,
                        book_cost_confidence=?,
                        missing_reason=?,
                        updated_at=?
                    where sample_id=?
                    """,
                    (
                        depth_source,
                        depth_missing_reason,
                        book_cost_confidence,
                        missing_reason,
                        now,
                        sample["sample_id"],
                    ),
                )
                summary["updated_samples"] += 1
            if depth_source and depth_source != "missing":
                summary["book_cost_samples"] += 1
            if depth_missing_reason:
                summary["missing_reason_samples"] += 1
        summary["depth_source_counts"] = dict(depth_source_counts)
        summary["depth_missing_reason_counts"] = dict(depth_missing_reason_counts)
    return summary


def refresh_technical_reliability_from_samples(
    root: Path | None = None,
    *,
    training_db_path: Path | None = None,
    limit_runs: int = 500,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Recompute STEP16.18-16.22 technical fields from normalized samples."""
    pr = project_root(root)
    training_db = init_micro_training_db(training_db_path, root=pr)
    summary: dict[str, Any] = {
        "training_db_path": str(training_db),
        "limit_runs": limit_runs,
        "dry_run": dry_run,
        "samples": 0,
        "updated_samples": 0,
        "usable_samples": 0,
        "technical_status_counts": {},
        "alignment_state_counts": {},
        "z_state_counts": {},
    }
    with connect(training_db) as conn:
        latest_runs = [
            row["run_id"]
            for row in conn.execute(
                """
                select run_id
                from micro_run_samples
                group by run_id
                order by max(generated_at) desc
                limit ?
                """,
                (max(1, int(limit_runs or 500)),),
            ).fetchall()
        ]
        if not latest_runs:
            return summary
        placeholders = ",".join(["?"] * len(latest_runs))
        samples = [
            dict(row)
            for row in conn.execute(
                f"select * from micro_symbol_samples where run_id in ({placeholders})",
                tuple(latest_runs),
            ).fetchall()
        ]
        summary["samples"] = len(samples)
        now = utc_now_iso()
        status_counts: Counter[str] = Counter()
        alignment_counts: Counter[str] = Counter()
        z_counts: Counter[str] = Counter()
        for sample in samples:
            raw_reasons = json_loads(sample.get("reason_codes_json"), [])
            status = _status_from_row(sample)
            nums = _extract_numbers(sample)
            technical = _technical_fields(sample, nums, status, raw_reasons)
            status_counts[str(technical.get("technical_status") or "unknown")] += 1
            alignment_counts[str(technical.get("alignment_state") or "unknown")] += 1
            z_counts[str(technical.get("z_state") or "unknown")] += 1
            if technical.get("is_training_usable") == 1:
                summary["usable_samples"] += 1
            if dry_run:
                summary["updated_samples"] += 1
                continue
            _update_symbol_technical_fields(conn, sample["sample_id"], technical, now=now)
            _upsert_technical_reliability(
                conn,
                sample_id=sample["sample_id"],
                run_id=sample["run_id"],
                cycle_id=sample.get("cycle_id"),
                strategy_line=sample["strategy_line"],
                symbol=sample["symbol"],
                micro_mode=sample["micro_mode"],
                technical=technical,
                now=now,
            )
            summary["updated_samples"] += 1
        summary["technical_status_counts"] = dict(status_counts)
        summary["alignment_state_counts"] = dict(alignment_counts)
        summary["z_state_counts"] = dict(z_counts)
    return summary


def enrich_downstream_labels(
    root: Path | None = None,
    *,
    paper_db_path: Path | None = None,
    training_db_path: Path | None = None,
    limit_runs: int = 500,
    dry_run: bool = False,
) -> dict[str, Any]:
    pr = project_root(root)
    paper_db = paper_db_path or pr / "DATA" / "paper" / "paper_trading.db"
    training_db = init_micro_training_db(training_db_path, root=pr)
    summary: dict[str, Any] = {
        "paper_db_path": str(paper_db),
        "training_db_path": str(training_db),
        "limit_runs": limit_runs,
        "dry_run": dry_run,
        "samples": 0,
        "paper_matches": 0,
        "trade_quality_matches": 0,
        "updated_labels": 0,
        "missing": [],
    }
    if not paper_db.is_file():
        summary["missing"].append("paper_db_missing")
        return summary
    with connect(training_db) as conn:
        latest_runs = [
            row["run_id"]
            for row in conn.execute(
                """
                select run_id
                from micro_run_samples
                group by run_id
                order by max(generated_at) desc
                limit ?
                """,
                (max(1, int(limit_runs or 500)),),
            ).fetchall()
        ]
        if not latest_runs:
            summary["missing"].append("training_runs_missing")
            return summary
        placeholders = ",".join(["?"] * len(latest_runs))
        samples = [
            dict(row)
            for row in conn.execute(
                f"select sample_id, run_id, cycle_id, strategy_line, symbol from micro_symbol_samples where run_id in ({placeholders})",
                tuple(latest_runs),
            ).fetchall()
        ]
    summary["samples"] = len(samples)
    if not samples:
        return summary
    sample_by_key = {(row["run_id"], row["strategy_line"], row["symbol"]): row for row in samples}
    with sqlite3.connect(f"file:{paper_db.as_posix()}?mode=ro", uri=True) as paper_conn:
        paper_conn.row_factory = sqlite3.Row
        run_placeholders = ",".join(["?"] * len(latest_runs))
        paper_rows: list[dict[str, Any]] = []
        tq_rows: list[dict[str, Any]] = []
        if table_exists(paper_conn, "paper_orders"):
            paper_rows = [
                dict(row)
                for row in paper_conn.execute(
                    f"select * from paper_orders where source_run_id in ({run_placeholders})",
                    tuple(latest_runs),
                ).fetchall()
            ]
        if table_exists(paper_conn, "trade_quality_diagnostic_samples"):
            tq_rows = [
                dict(row)
                for row in paper_conn.execute(
                    f"select * from trade_quality_diagnostic_samples where run_id in ({run_placeholders})",
                    tuple(latest_runs),
                ).fetchall()
            ]
    label_updates: dict[str, dict[str, Any]] = {}
    for row in paper_rows:
        key = (row.get("source_run_id"), row.get("strategy_line"), row.get("symbol"))
        sample = sample_by_key.get(key)
        if not sample:
            continue
        update = label_updates.setdefault(sample["sample_id"], {**sample, "payload": {}})
        update.update(
            {
                "paper_order_id": row.get("id"),
                "paper_status": row.get("status"),
                "exit_reason": row.get("exit_reason"),
                "trade_plan_status": "paper_ordered",
                "executable": 1 if row.get("source_executable") in {1, True} else update.get("executable"),
                "label_source": "paper_order_join",
            }
        )
        update["payload"]["paper_order"] = {"id": row.get("id"), "status": row.get("status"), "exit_reason": row.get("exit_reason")}
        summary["paper_matches"] += 1
    for row in tq_rows:
        key = (row.get("run_id"), row.get("strategy_line"), row.get("symbol"))
        sample = sample_by_key.get(key)
        if not sample:
            continue
        update = label_updates.setdefault(sample["sample_id"], {**sample, "payload": {}})
        update.update(
            {
                "paper_order_id": row.get("order_id") or update.get("paper_order_id"),
                "paper_status": "closed" if row.get("exit_time") else update.get("paper_status"),
                "exit_reason": row.get("exit_reason") or update.get("exit_reason"),
                "net_R": row.get("net_R"),
                "MFE_R": row.get("MFE_R"),
                "MAE_R": row.get("MAE_R"),
                "trade_quality_root_cause": row.get("root_cause"),
                "trade_plan_status": "trade_quality_closed",
                "label_source": "trade_quality_diagnostic_join",
            }
        )
        update["payload"]["trade_quality"] = {
            "diagnostic_id": row.get("diagnostic_id"),
            "net_R": row.get("net_R"),
            "MFE_R": row.get("MFE_R"),
            "MAE_R": row.get("MAE_R"),
            "root_cause": row.get("root_cause"),
        }
        summary["trade_quality_matches"] += 1
    if dry_run:
        summary["updated_labels"] = len(label_updates)
        return summary
    with connect(training_db) as conn:
        for update in label_updates.values():
            upsert_downstream_label(conn, update)
        summary["updated_labels"] = len(label_updates)
    return summary


def classify_run_sample_gaps(
    root: Path | None = None,
    *,
    training_db_path: Path | None = None,
    limit_runs: int = 500,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Mark selected/not-selected run-level gaps without changing strategy state."""
    pr = project_root(root)
    training_db = init_micro_training_db(training_db_path, root=pr)
    summary: dict[str, Any] = {
        "training_db_path": str(training_db),
        "limit_runs": limit_runs,
        "dry_run": dry_run,
        "run_lines_checked": 0,
        "not_selected_lines": 0,
        "technical_missing_lines": 0,
        "unchanged_lines": 0,
        "updated_lines": 0,
    }
    with connect(training_db) as conn:
        latest_runs = [
            row["run_id"]
            for row in conn.execute(
                """
                select run_id
                from micro_run_samples
                group by run_id
                order by max(generated_at) desc
                limit ?
                """,
                (max(1, int(limit_runs or 500)),),
            ).fetchall()
        ]
        if not latest_runs:
            return summary
        placeholders = ",".join(["?"] * len(latest_runs))
        run_rows = [
            dict(row)
            for row in conn.execute(
                f"select * from micro_run_samples where run_id in ({placeholders})",
                tuple(latest_runs),
            ).fetchall()
        ]
        symbol_counts = {
            (row["run_id"], row["strategy_line"]): row["c"]
            for row in conn.execute(
                f"""
                select run_id, strategy_line, count(*) c
                from micro_symbol_samples
                where run_id in ({placeholders})
                group by run_id, strategy_line
                """,
                tuple(latest_runs),
            ).fetchall()
        }
        now = utc_now_iso()
        for row in run_rows:
            summary["run_lines_checked"] += 1
            key = (row["run_id"], row["strategy_line"])
            count = int(symbol_counts.get(key) or 0)
            status = str(row.get("status") or "")
            next_status = status
            missing_reason = row.get("missing_reason")
            source_confidence = row.get("source_confidence") or "direct_run_id"
            if count <= 0 and row["strategy_line"] == "micro_full":
                next_status = "not_selected"
                missing_reason = "line_not_selected_or_no_runtime_symbols"
                source_confidence = "run_level_gap_classified"
                summary["not_selected_lines"] += 1
            elif count <= 0 and row["strategy_line"] == "micro_fast":
                next_status = "technical_missing"
                missing_reason = "line_selected_but_no_runtime_symbols"
                source_confidence = "run_level_gap_classified"
                summary["technical_missing_lines"] += 1
            else:
                summary["unchanged_lines"] += 1
            if next_status != status or missing_reason != row.get("missing_reason"):
                summary["updated_lines"] += 1
                if not dry_run:
                    conn.execute(
                        """
                        update micro_run_samples
                        set status=?, missing_reason=?, source_confidence=?, updated_at=?
                        where run_id=? and strategy_line=? and micro_mode=?
                        """,
                        (
                            next_status,
                            missing_reason,
                            source_confidence,
                            now,
                            row["run_id"],
                            row["strategy_line"],
                            row["micro_mode"],
                        ),
                    )
    return summary


def _metric_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    metrics = (
        "cvd",
        "ofi",
        "z_cvd",
        "z_ofi",
        "spread",
        "spread_bps",
        "depth_imbalance",
        "bid_depth_usdt",
        "ask_depth_usdt",
        "micro_data_plane_ready",
        "common_bucket_ts",
        "technical_reliability_score",
    )
    out: dict[str, Any] = {"sample_count": total}
    for metric in metrics:
        filled = sum(1 for row in rows if row.get(metric) is not None)
        out[metric] = {
            "filled": filled,
            "missing": max(total - filled, 0),
            "coverage": (filled / total) if total else None,
        }
    out["missing_reason_count"] = sum(1 for row in rows if row.get("missing_reason"))
    out["training_ready_count"] = sum(1 for row in rows if row.get("accepted"))
    out["training_degraded_count"] = sum(
        1 for row in rows if row.get("blocked") and row.get("missing_reason")
    )
    out["technical_missing_count"] = sum(
        1 for row in rows if str(row.get("ready_state") or "").lower() in {"technical_missing", "technical_blocked"}
    )
    out["data_plane_ready_count"] = sum(1 for row in rows if row.get("micro_data_plane_ready") == 1)
    out["data_plane_not_ready_count"] = sum(1 for row in rows if row.get("micro_data_plane_ready") == 0)
    out["training_usable_count"] = sum(1 for row in rows if row.get("is_training_usable") == 1)
    out["training_not_usable_count"] = sum(1 for row in rows if row.get("is_training_usable") == 0)
    out["alignment_state_counts"] = dict(Counter(str(row.get("alignment_state") or "unknown") for row in rows))
    out["z_state_counts"] = dict(Counter(str(row.get("z_state") or "unknown") for row in rows))
    out["technical_status_counts"] = dict(Counter(str(row.get("technical_status") or "unknown") for row in rows))
    scores = [_as_float(row.get("technical_reliability_score")) for row in rows]
    valid_scores = [score for score in scores if score is not None]
    out["avg_technical_reliability_score"] = (sum(valid_scores) / len(valid_scores)) if valid_scores else None
    return out


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    out = dict(row)
    for key in list(out):
        if key.endswith("_json"):
            out[key.removesuffix("_json")] = json_loads(out.get(key), [] if "codes" in key or "flags" in key else {})
    return out


def training_summary(root: Path | None = None, *, db_path: Path | None = None) -> dict[str, Any]:
    db = init_micro_training_db(db_path, root=root)
    with connect(db) as conn:
        run_count = conn.execute("select count(distinct run_id) as c from micro_run_samples").fetchone()["c"]
        symbol_count = conn.execute("select count(*) as c from micro_symbol_samples").fetchone()["c"]
        label_count = conn.execute("select count(*) as c from micro_downstream_labels").fetchone()["c"]
        latest = conn.execute("select run_id from micro_run_samples order by generated_at desc limit 1").fetchone()
        status_counts = dict(
            (row["status"], row["c"])
            for row in conn.execute("select status, count(*) c from micro_run_samples group by status").fetchall()
        )
        line_counts = dict(
            (row["strategy_line"], row["c"])
            for row in conn.execute("select strategy_line, count(*) c from micro_symbol_samples group by strategy_line").fetchall()
        )
        confidence_counts = dict(
            (row["source_confidence"], row["c"])
            for row in conn.execute("select source_confidence, count(*) c from micro_symbol_samples group by source_confidence").fetchall()
        )
        metric_rows = [
            dict(row)
            for row in conn.execute(
                """
                select cvd, ofi, z_cvd, z_ofi, spread, spread_bps, depth_imbalance,
                       bid_depth_usdt, ask_depth_usdt, accepted, blocked, ready_state, missing_reason,
                       micro_data_plane_ready, common_bucket_ts, alignment_state, z_state,
                       technical_status, technical_reliability_score, is_training_usable
                from micro_symbol_samples
                """
            ).fetchall()
        ]
    return {
        "source": "micro_training_sqlite",
        "schema_version": MICRO_TRAINING_SCHEMA_VERSION,
        "db_path": str(db),
        "run_count": run_count,
        "symbol_sample_count": symbol_count,
        "label_count": label_count,
        "latest_run_id": latest["run_id"] if latest else None,
        "status_counts": status_counts,
        "line_counts": line_counts,
        "source_confidence_counts": confidence_counts,
        "metric_coverage": _metric_coverage(metric_rows),
    }


def latest_training_payload(root: Path | None = None, *, db_path: Path | None = None, symbol_limit: int = 100) -> dict[str, Any]:
    summary = training_summary(root, db_path=db_path)
    run_id = summary.get("latest_run_id")
    if not run_id:
        return {**summary, "run_id": None, "runs": [], "symbols": []}
    return run_payload(str(run_id), root=root, db_path=db_path, symbol_limit=symbol_limit)


def run_list(root: Path | None = None, *, db_path: Path | None = None, limit: int = 50) -> dict[str, Any]:
    db = init_micro_training_db(db_path, root=root)
    with connect(db) as conn:
        rows = [
            _row_to_dict(row)
            for row in conn.execute(
                """
                select run_id, max(cycle_id) cycle_id, max(generated_at) generated_at,
                       count(*) line_count,
                       sum(case when status='ready' then 1 else 0 end) ready_lines,
                       sum(case when status='technical_blocked' then 1 else 0 end) technical_blocked_lines
                from micro_run_samples
                group by run_id
                order by max(generated_at) desc
                limit ?
                """,
                (max(1, min(int(limit or 50), 500)),),
            ).fetchall()
        ]
    return {"source": "micro_training_sqlite", "db_path": str(db), "runs": rows, "count": len(rows)}


def run_payload(run_id: str, *, root: Path | None = None, db_path: Path | None = None, symbol_limit: int = 200) -> dict[str, Any]:
    db = init_micro_training_db(db_path, root=root)
    with connect(db) as conn:
        run_rows = [_row_to_dict(row) for row in conn.execute("select * from micro_run_samples where run_id=? order by strategy_line", (run_id,)).fetchall()]
        metric_rows = [
            dict(row)
            for row in conn.execute(
                """
                select cvd, ofi, z_cvd, z_ofi, spread, spread_bps, depth_imbalance,
                       bid_depth_usdt, ask_depth_usdt,
                       accepted, blocked, ready_state, missing_reason,
                       micro_data_plane_ready, common_bucket_ts, alignment_state, z_state,
                       technical_status, technical_reliability_score, is_training_usable
                from micro_symbol_samples
                where run_id=?
                """,
                (run_id,),
            ).fetchall()
        ]
        symbols = [
            _row_to_dict(row)
            for row in conn.execute(
                """
                select s.*, l.trade_plan_status, l.executable, l.paper_status, l.exit_reason,
                       l.net_R, l.MFE_R, l.MAE_R, l.trade_quality_root_cause
                from micro_symbol_samples s
                left join micro_downstream_labels l on l.sample_id = s.sample_id
                where s.run_id=?
                order by s.strategy_line, s.symbol
                limit ?
                """,
                (run_id, max(1, min(int(symbol_limit or 200), 1000))),
            ).fetchall()
        ]
    reason_counts: Counter[str] = Counter()
    for row in symbols:
        for reason in row.get("reason_codes") or []:
            reason_counts[str(reason)] += 1
    run_metric_coverage = _metric_coverage(metric_rows)
    return {
        **training_summary(root, db_path=db_path),
        "run_id": run_id,
        "run_samples": run_rows,
        "symbols": symbols,
        "symbol_count": len(symbols),
        "reason_counts": dict(reason_counts),
        "run_metric_coverage": run_metric_coverage,
    }


def symbol_payload(symbol: str, *, root: Path | None = None, db_path: Path | None = None, limit: int = 100) -> dict[str, Any]:
    db = init_micro_training_db(db_path, root=root)
    clean = symbol.strip().upper()
    with connect(db) as conn:
        rows = [
            _row_to_dict(row)
            for row in conn.execute(
                """
                select s.*, l.trade_plan_status, l.executable, l.paper_status, l.exit_reason,
                       l.net_R, l.MFE_R, l.MAE_R, l.trade_quality_root_cause
                from micro_symbol_samples s
                left join micro_downstream_labels l on l.sample_id = s.sample_id
                where s.symbol=?
                order by s.generated_at desc
                limit ?
                """,
                (clean, max(1, min(int(limit or 100), 1000))),
            ).fetchall()
        ]
    return {"source": "micro_training_sqlite", "db_path": str(db), "symbol": clean, "samples": rows, "count": len(rows)}


def coverage_payload(root: Path | None = None, *, db_path: Path | None = None, audit_db_path: Path | None = None) -> dict[str, Any]:
    pr = project_root(root)
    db = init_micro_training_db(db_path, root=pr)
    audit_db = audit_db_path or default_audit_db(pr)
    summary = training_summary(pr, db_path=db)
    audit_runs = None
    if audit_db.is_file():
        with sqlite3.connect(f"file:{audit_db.as_posix()}?mode=ro", uri=True) as conn:
            audit_runs = conn.execute("select count(distinct run_id) from audit_runs").fetchone()[0] if table_exists(conn, "audit_runs") else None
    coverage = None
    if audit_runs:
        coverage = summary["run_count"] / audit_runs
    return {
        **summary,
        "audit_db_path": str(audit_db),
        "audit_run_count": audit_runs,
        "run_coverage_ratio": coverage,
    }
