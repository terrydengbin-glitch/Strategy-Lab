"""STEP10.43 micro data quality attribution report.

This module is intentionally read-only for strategy decisions. It classifies
micro not-ready reasons into audit categories and writes report artifacts.
"""

from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.micro.factor_frame_store import default_micro_factor_db, full_z_window_from_store

MICRO_LINES = ("micro_fast", "micro_full")
RAW_REASONS = {
    "cvd_never_updated",
    "ofi_never_updated",
    "cvd_stale",
    "ofi_stale",
    "ofi_cvd_lag_high",
    "coverage_aggtrade_weak",
    "coverage_bookticker_weak",
    "coverage_depth5_weak",
    "full_z_missing",
    "fast_z_missing",
    "fast_one_z_available_weak_only",
}

MICRO_FAST_DEPTH5_ROLE = "optional_evidence"
MICRO_FAST_MIN_OBSERVE_SEC_BEFORE_JUDGEMENT = 30
MICRO_FAST_MIN_DWELL_SEC = 60
MICRO_FAST_WARMUP_BUCKET_COUNT = 30


def _root(project_root: Path | None = None) -> Path:
    return Path(project_root).resolve() if project_root else Path.cwd().resolve()


def _read_json(path: Path) -> Any | None:
    try:
        return read_json_object(path)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _items(doc: Any) -> list[dict[str, Any]]:
    if not isinstance(doc, dict):
        return []
    raw = doc.get("items") or doc.get("symbols") or doc.get("lifecycles") or []
    return [row for row in raw if isinstance(row, dict)] if isinstance(raw, list) else []


def _symbol(row: dict[str, Any]) -> str:
    return str(row.get("symbol") or row.get("s") or "").upper()


def _quality_blocks(item: dict[str, Any]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for key in ("micro_fast_quality", "micro_full_quality", "micro_quality"):
        raw = item.get(key)
        if isinstance(raw, dict):
            blocks.append(raw)
    return blocks


def _quality_block_for_line(item: dict[str, Any], line: str) -> dict[str, Any]:
    keys = ("micro_fast_quality", "micro_quality") if line == "micro_fast" else ("micro_full_quality", "micro_quality")
    for key in keys:
        raw = item.get(key)
        if isinstance(raw, dict):
            return raw
    blocks = _quality_blocks(item)
    return blocks[0] if blocks else {}


def _micro_block_for_line(item: dict[str, Any], line: str) -> dict[str, Any]:
    keys = ("micro_fast_15m", "micro_15m") if line == "micro_fast" else ("micro_full_15m", "micro_15m")
    for key in keys:
        raw = item.get(key)
        if isinstance(raw, dict):
            return raw
    return {}


def _feature_index(features_doc: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in _items(features_doc):
        sym = _symbol(item)
        if sym:
            out[sym] = item
    return out


def _state_symbol_index(state_doc: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(state_doc, dict):
        return out
    for item in state_doc.get("symbols") or []:
        if not isinstance(item, dict):
            continue
        sym = _symbol(item)
        if sym:
            out[sym] = item
    return out


def _number(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if not math.isfinite(out):
            return None
        return out
    except (TypeError, ValueError):
        return None


def _raw_float_state(value: Any) -> str:
    if value is None:
        return "null"
    try:
        out = float(value)
    except (TypeError, ValueError):
        return "invalid"
    if math.isnan(out):
        return "nan"
    if math.isinf(out):
        return "inf"
    return "valid"


STREAM_RATIO_KEYS = {
    "aggTrade": "aggtrade_coverage_ratio",
    "bookTicker": "bookticker_coverage_ratio",
    "partialDepth5": "depth5_coverage_ratio",
}


def _coverage(block: dict[str, Any], key: str) -> float | None:
    cov = block.get("coverage")
    if not isinstance(cov, dict):
        return None
    raw = cov.get(key)
    if isinstance(raw, dict):
        if "coverage_ratio" in raw:
            return _number(raw.get("coverage_ratio"))
        expected = _number(raw.get("expected_seconds"))
        covered = _number(raw.get("covered_seconds"))
        if expected and expected > 0 and covered is not None:
            return covered / expected
    return None


def _coverage_entry(
    block: dict[str, Any],
    subscription_state: dict[str, Any],
    key: str,
) -> dict[str, Any]:
    cov = block.get("coverage") if isinstance(block.get("coverage"), dict) else {}
    raw = cov.get(key) if isinstance(cov, dict) else None
    sub_raw = subscription_state.get(key) if isinstance(subscription_state.get(key), dict) else {}
    required = bool(sub_raw.get("required")) if sub_raw else key in cov
    active = bool(sub_raw.get("active")) if sub_raw else bool(raw)
    expected = None
    covered = None
    ratio = None
    missing_reason = None
    if isinstance(raw, dict):
        expected = raw.get("expected_seconds") if raw.get("expected_seconds") is not None else raw.get("expected_bucket_count")
        covered = raw.get("covered_seconds") if raw.get("covered_seconds") is not None else raw.get("covered_bucket_count")
        ratio = _coverage(block, key)
    if ratio is None:
        if required and not active:
            missing_reason = "subscription_missing"
        elif required:
            missing_reason = "coverage_missing"
        elif not raw:
            missing_reason = "not_required"
    elif ratio <= 0:
        if required and not active:
            missing_reason = "subscription_missing"
        elif sub_raw.get("missing_reason"):
            missing_reason = str(sub_raw.get("missing_reason"))
        else:
            missing_reason = "ws_no_event"
    elif ratio < 0.15:
        missing_reason = "coverage_weak"
    return {
        "required": required,
        "active": active,
        "expected_bucket_count": expected,
        "covered_bucket_count": covered,
        "expected_seconds": expected,
        "covered_seconds": covered,
        "coverage_ratio": ratio,
        "first_bucket_ts_sec": raw.get("first_bucket_ts_sec") if isinstance(raw, dict) else None,
        "last_bucket_ts_sec": raw.get("last_bucket_ts_sec") if isinstance(raw, dict) else None,
        "last_event_ts_sec": sub_raw.get("last_event_ts_sec"),
        "missing_reason": missing_reason,
    }


def _classify_stream_gap(
    *,
    logical: str,
    entry: dict[str, Any],
    ws_status: str | None,
    last_ws_age: float | None,
    dropped: dict[str, Any],
) -> str:
    if entry.get("required") is True and entry.get("active") is False:
        return "subscription_missing"
    if ws_status and ws_status not in {"connected", "healthy"}:
        return "ws_gap"
    if last_ws_age is not None and last_ws_age > 60:
        return "ws_gap"
    if logical == "aggTrade" and dropped.get("trade"):
        return "queue_drop"
    if logical in {"bookTicker", "partialDepth5"} and (dropped.get("book") or dropped.get("depth")):
        return "queue_drop"
    reason = str(entry.get("missing_reason") or "")
    if reason in {"subscription_missing", "ws_gap", "queue_drop", "bucket_gap", "low_activity_or_churn", "ws_no_event", "coverage_missing", "coverage_weak"}:
        return reason
    ratio = _number(entry.get("coverage_ratio"))
    if ratio is None:
        return "coverage_missing"
    if ratio <= 0:
        return "ws_no_event"
    if ratio < 0.15:
        return "low_activity_or_churn"
    return "ok"


def _coverage_root_cause(entry: dict[str, Any]) -> str:
    gap_class = str(entry.get("gap_class") or "")
    ratio = _number(entry.get("coverage_ratio"))
    required = bool(entry.get("required"))
    active = bool(entry.get("active"))
    if gap_class in {"subscription_missing"} or (required and not active):
        return "subscription_missing"
    if gap_class in {"ws_gap"}:
        return "websocket_gap"
    if gap_class in {"queue_drop"}:
        return "queue_backpressure"
    if gap_class in {"coverage_missing"}:
        return "coverage_missing"
    if gap_class in {"ws_no_event"}:
        return "low_market_activity"
    if gap_class in {"low_activity_or_churn"}:
        return "low_activity_or_churn"
    if ratio is None:
        return "coverage_evidence_missing"
    if ratio <= 0:
        return "no_bucket_committed"
    if ratio < 0.15:
        return "coverage_ratio_low"
    return "ok"


def _adapter_commit_state(
    metrics: dict[str, Any],
    first_block: dict[str, Any],
) -> dict[str, Any]:
    keys = (
        "processed_bucket_count",
        "processed_trade_bucket_count",
        "processed_book_bucket_count",
        "cvd_update_count",
        "ofi_update_count",
        "cvd_skipped_no_trade",
        "cvd_skipped_missing_last_price",
        "ofi_skipped_no_book",
        "ofi_skipped_level_mismatch",
        "adapter_error_count",
        "late_bucket_skipped",
        "duplicate_bucket_skipped",
        "dropped_trade_delta",
        "dropped_book_delta",
        "dropped_depth_delta",
    )
    out = {key: int(_number(metrics.get(key)) or 0) for key in keys}
    out["last_cvd_commit_bucket_ts_sec"] = first_block.get("last_cvd_update_bucket_ts_sec")
    out["last_ofi_commit_bucket_ts_sec"] = first_block.get("last_ofi_update_bucket_ts_sec")
    out["last_processed_bucket_ts_sec"] = first_block.get("last_processed_bucket_ts_sec")
    return out


def _bucket_alignment(first_block: dict[str, Any]) -> dict[str, Any]:
    reference = first_block.get("reference_bucket_ts_sec")
    processed = first_block.get("last_processed_bucket_ts_sec")
    cvd_ts = first_block.get("last_cvd_update_bucket_ts_sec")
    ofi_ts = first_block.get("last_ofi_update_bucket_ts_sec")
    cvd_age = first_block.get("cvd_age_bucket_sec")
    ofi_age = first_block.get("ofi_age_bucket_sec")
    lag = first_block.get("ofi_cvd_lag_bucket_sec")
    lag_side = first_block.get("ofi_cvd_lag_side")
    lag_num = _number(lag)
    if lag_side is None:
        cvd_num = _number(cvd_age)
        ofi_num = _number(ofi_age)
        if cvd_num is None or ofi_num is None:
            lag_side = "missing"
        elif cvd_num > ofi_num:
            lag_side = "cvd_old"
        elif ofi_num > cvd_num:
            lag_side = "ofi_old"
        else:
            lag_side = "aligned"
    if lag_num is None:
        alignment_status = "missing"
    elif lag_num <= 30:
        alignment_status = "aligned"
    elif lag_num <= 90:
        alignment_status = "lagging"
    else:
        alignment_status = "broken"
    cvd_commit_state = "missing"
    ofi_commit_state = "missing"
    if cvd_ts is not None:
        cvd_commit_state = "updated"
    elif first_block.get("cvd_zero_delta_commit") is True:
        cvd_commit_state = "zero_delta"
    elif processed is not None:
        cvd_commit_state = "missing"
    if ofi_ts is not None:
        ofi_commit_state = "updated"
    elif first_block.get("ofi_zero_delta_commit") is True:
        ofi_commit_state = "zero_delta"
    elif processed is not None:
        ofi_commit_state = "missing"
    if cvd_commit_state == "updated" and ofi_commit_state == "updated" and alignment_status == "aligned":
        barrier = "aligned"
    elif cvd_commit_state in {"updated", "zero_delta"} and ofi_commit_state in {"updated", "zero_delta"} and alignment_status in {"aligned", "lagging"}:
        barrier = alignment_status
    elif cvd_commit_state == "missing" or ofi_commit_state == "missing":
        barrier = "missing"
    else:
        barrier = alignment_status
    if barrier == "aligned":
        true_alignment_reason = "aligned"
    elif cvd_commit_state == "missing":
        true_alignment_reason = "cvd_commit_missing"
    elif ofi_commit_state == "missing":
        true_alignment_reason = "ofi_commit_missing"
    elif lag_side in {"cvd_old", "ofi_old"}:
        true_alignment_reason = f"{lag_side}_bucket_lag"
    elif cvd_commit_state == "zero_delta" or ofi_commit_state == "zero_delta":
        true_alignment_reason = "zero_delta_commit_lag"
    else:
        true_alignment_reason = "alignment_lag"
    return {
        "reference_bucket_ts_sec": reference,
        "bucket_closed": processed is not None,
        "last_processed_bucket_ts_sec": processed,
        "last_cvd_update_bucket_ts_sec": cvd_ts,
        "last_ofi_update_bucket_ts_sec": ofi_ts,
        "cvd_age_bucket_sec": cvd_age,
        "ofi_age_bucket_sec": ofi_age,
        "ofi_cvd_lag_bucket_sec": lag,
        "lag_side": lag_side,
        "alignment_status": alignment_status,
        "cvd_commit_state": cvd_commit_state,
        "ofi_commit_state": ofi_commit_state,
        "cvd_zero_delta_commit": bool(first_block.get("cvd_zero_delta_commit")),
        "ofi_zero_delta_commit": bool(first_block.get("ofi_zero_delta_commit")),
        "commit_barrier_status": barrier,
        "true_alignment_reason": true_alignment_reason,
    }


def _cvd_runtime_state(
    *,
    stream_entry: dict[str, Any],
    adapter_commit_state: dict[str, Any],
    ws_status: str | None,
    last_ws_age: float | None,
    dropped: dict[str, Any],
    target_age_sec: Any,
    warmup_age_sec: Any,
    required_sec: Any,
) -> dict[str, Any]:
    processed_bucket = int(_number(adapter_commit_state.get("processed_bucket_count")) or 0)
    processed_trade = int(_number(adapter_commit_state.get("processed_trade_bucket_count")) or 0)
    cvd_updates = int(_number(adapter_commit_state.get("cvd_update_count")) or 0)
    skipped_no_trade = int(_number(adapter_commit_state.get("cvd_skipped_no_trade")) or 0)
    stream_class = _classify_stream_gap(
        logical="aggTrade",
        entry=stream_entry,
        ws_status=ws_status,
        last_ws_age=last_ws_age,
        dropped=dropped,
    )
    if cvd_updates > 0:
        never_class = "updated"
        actionable = False
    elif stream_class == "subscription_missing":
        never_class = "subscription_missing"
        actionable = True
    elif stream_class in {"ws_gap", "queue_drop"}:
        never_class = stream_class
        actionable = True
    elif processed_bucket <= 0:
        never_class = "bucket_gap"
        actionable = True
    elif processed_trade > 0:
        never_class = "adapter_commit_failed"
        actionable = True
    elif skipped_no_trade > 0 or stream_class in {"ws_no_event", "low_activity_or_churn"}:
        never_class = "low_activity_or_churn"
        actionable = False
    elif required_sec is not None and warmup_age_sec is not None and _number(warmup_age_sec) is not None and _number(required_sec) is not None and float(warmup_age_sec) < float(required_sec):
        never_class = "target_evicted_before_warmup"
        actionable = False
    else:
        never_class = "adapter_not_initialized"
        actionable = True
    return {
        "required": bool(stream_entry.get("required")),
        "stream_active": bool(stream_entry.get("active")),
        "coverage_ratio": stream_entry.get("coverage_ratio"),
        "last_aggtrade_event_ts_sec": stream_entry.get("last_event_ts_sec"),
        "processed_bucket_count": processed_bucket,
        "processed_trade_bucket_count": processed_trade,
        "cvd_update_count": cvd_updates,
        "last_cvd_commit_bucket_ts_sec": adapter_commit_state.get("last_cvd_commit_bucket_ts_sec"),
        "never_updated_class": never_class,
        "stream_gap_class": stream_class,
        "target_age_sec": target_age_sec,
        "actionable": actionable,
    }


def _drop_count(dropped: dict[str, Any], key: str, fallback: Any = None) -> int:
    raw = dropped.get(key)
    if isinstance(raw, dict):
        raw = raw.get("count") or raw.get("dropped") or raw.get("total")
    got = _number(raw)
    if got is None:
        got = _number(fallback)
    return max(0, int(got or 0))


def _gap_from_stream_class(stream_class: str) -> str | None:
    if stream_class in {"subscription_missing", "ws_gap", "queue_drop", "ws_no_event", "coverage_missing", "coverage_weak"}:
        return stream_class
    return None


def _aggtrade_runtime_state(
    *,
    stream_entry: dict[str, Any],
    adapter_commit_state: dict[str, Any],
    ws_status: str | None,
    last_ws_age: float | None,
    dropped: dict[str, Any],
) -> dict[str, Any]:
    processed_bucket = int(_number(adapter_commit_state.get("processed_bucket_count")) or 0)
    processed_trade = int(_number(adapter_commit_state.get("processed_trade_bucket_count")) or 0)
    cvd_commits = int(_number(adapter_commit_state.get("cvd_update_count")) or 0)
    trade_drops = _drop_count(dropped, "trade", adapter_commit_state.get("dropped_trade_delta"))
    stream_class = _classify_stream_gap(
        logical="aggTrade",
        entry=stream_entry,
        ws_status=ws_status,
        last_ws_age=last_ws_age,
        dropped={**dropped, "trade": trade_drops},
    )
    if trade_drops > 0:
        bucket_gap_class = "queue_drop"
    elif _gap_from_stream_class(stream_class):
        bucket_gap_class = str(_gap_from_stream_class(stream_class))
    elif processed_bucket <= 0:
        bucket_gap_class = "bucket_close_gap"
    elif processed_trade <= 0:
        bucket_gap_class = "low_activity_or_churn"
    elif cvd_commits <= 0:
        bucket_gap_class = "adapter_gap"
    else:
        bucket_gap_class = "ok"
    queue_received = processed_trade + trade_drops
    queue_drop_ratio = (trade_drops / queue_received) if queue_received > 0 else 0.0
    return {
        "required": bool(stream_entry.get("required")),
        "stream_active": bool(stream_entry.get("active")),
        "stream_gap_class": stream_class,
        "bucket_gap_class": bucket_gap_class,
        "queue_received_count": queue_received,
        "queue_dropped_count": trade_drops,
        "queue_drop_ratio": queue_drop_ratio,
        "bucket_closed_count": processed_bucket,
        "trade_bucket_commit_count": processed_trade,
        "cvd_bucket_commit_count": cvd_commits,
        "last_event_ts_sec": stream_entry.get("last_event_ts_sec"),
        "last_commit_bucket_ts_sec": adapter_commit_state.get("last_cvd_commit_bucket_ts_sec"),
    }


def _book_depth_runtime_state(
    *,
    book_entry: dict[str, Any],
    depth_entry: dict[str, Any],
    adapter_commit_state: dict[str, Any],
    ws_status: str | None,
    last_ws_age: float | None,
    dropped: dict[str, Any],
) -> dict[str, Any]:
    processed_bucket = int(_number(adapter_commit_state.get("processed_bucket_count")) or 0)
    processed_book = int(_number(adapter_commit_state.get("processed_book_bucket_count")) or 0)
    ofi_commits = int(_number(adapter_commit_state.get("ofi_update_count")) or 0)
    book_drops = _drop_count(dropped, "book", adapter_commit_state.get("dropped_book_delta"))
    depth_drops = _drop_count(dropped, "depth", adapter_commit_state.get("dropped_depth_delta"))
    dropped_for_class = {**dropped, "book": book_drops, "depth": depth_drops}
    book_class = _classify_stream_gap(
        logical="bookTicker",
        entry=book_entry,
        ws_status=ws_status,
        last_ws_age=last_ws_age,
        dropped=dropped_for_class,
    )
    depth_class = _classify_stream_gap(
        logical="partialDepth5",
        entry=depth_entry,
        ws_status=ws_status,
        last_ws_age=last_ws_age,
        dropped=dropped_for_class,
    )
    total_drops = book_drops + depth_drops
    queue_received = processed_book + total_drops
    queue_drop_ratio = (total_drops / queue_received) if queue_received > 0 else 0.0
    if queue_drop_ratio >= 0.10 or total_drops >= 50:
        backpressure = "critical"
    elif total_drops > 0:
        backpressure = "warning"
    else:
        backpressure = "ok"
    if total_drops > 0:
        ofi_gap_class = "queue_drop"
    elif book_class in {"subscription_missing", "ws_gap"} or depth_class in {"subscription_missing", "ws_gap"}:
        ofi_gap_class = "ws_gap"
    elif book_class in {"coverage_missing", "coverage_weak", "ws_no_event"} and depth_class in {"coverage_missing", "coverage_weak", "ws_no_event"}:
        ofi_gap_class = "coverage_gap"
    elif processed_bucket <= 0:
        ofi_gap_class = "bucket_close_gap"
    elif processed_book <= 0:
        ofi_gap_class = "low_activity_or_churn"
    elif ofi_commits <= 0:
        ofi_gap_class = "adapter_gap"
    else:
        ofi_gap_class = "ok"
    return {
        "book_stream_gap_class": book_class,
        "depth_stream_gap_class": depth_class,
        "ofi_gap_class": ofi_gap_class,
        "queue_received_count": queue_received,
        "queue_dropped_count": total_drops,
        "book_queue_dropped_count": book_drops,
        "depth_queue_dropped_count": depth_drops,
        "queue_drop_ratio": queue_drop_ratio,
        "queue_backpressure_state": backpressure,
        "bucket_closed_count": processed_bucket,
        "book_bucket_commit_count": processed_book,
        "ofi_bucket_commit_count": ofi_commits,
        "last_book_event_ts_sec": book_entry.get("last_event_ts_sec"),
        "last_depth_event_ts_sec": depth_entry.get("last_event_ts_sec"),
        "last_commit_bucket_ts_sec": adapter_commit_state.get("last_ofi_commit_bucket_ts_sec"),
    }


def _z_history_runtime_state(
    *,
    line: str,
    symbol: str,
    z_window: dict[str, Any],
    adapter_commit_state: dict[str, Any],
    store_window: dict[str, Any] | None = None,
) -> dict[str, Any]:
    count = int(_number(z_window.get("z_window_count")) or 0)
    required = int(_number(z_window.get("z_window_required_count")) or (1 if line == "micro_fast" else 2))
    missing_reason = str(z_window.get("missing_reason") or "none")
    store = store_window if isinstance(store_window, dict) else {}
    if store and line == "micro_full" and missing_reason != "warmup_incomplete":
        count = int(_number(store.get("valid_bucket_count")) or count)
        required = int(_number(store.get("expected_bucket_count")) or required)
        store_reason = str(store.get("full_z_missing_reason") or "")
        if store.get("full_z_status") == "available":
            history_gap_class = "ok"
        elif store_reason in {"store_missing", "series_not_persisted"}:
            history_gap_class = "series_not_persisted"
        elif store_reason in {"store_read_failed"}:
            history_gap_class = "store_read_failed"
        elif store_reason in {"valid_bucket_ratio_low"}:
            history_gap_class = "valid_bucket_ratio_low"
        elif store_reason in {"bucket_gap"}:
            history_gap_class = "bucket_gap"
        elif store_reason in {"cvd_valid_ratio_low", "ofi_valid_ratio_low"}:
            history_gap_class = store_reason
        elif store_reason in {"zero_variance"}:
            history_gap_class = "zero_variance"
        elif store_reason in {"insufficient_history"}:
            history_gap_class = "insufficient_history"
        else:
            history_gap_class = "unknown_history_gap"
    elif count >= required:
        history_gap_class = "ok"
    elif missing_reason == "warmup_incomplete":
        history_gap_class = "warmup_incomplete"
    elif missing_reason == "target_churn":
        history_gap_class = "target_churn"
    elif missing_reason == "coverage_low":
        history_gap_class = "coverage_low"
    elif missing_reason in {"series_reset", "feature_nan", "adapter_missing"}:
        history_gap_class = missing_reason
    elif missing_reason == "insufficient_history":
        history_gap_class = "insufficient_history"
    else:
        history_gap_class = "unknown_history_gap"
    return {
        "series_key": f"{line}:{symbol}",
        "series_length": count,
        "required_length": required,
        "series_persisted": bool(count > 0),
        "append_success_count": count,
        "append_skip_count": 0 if count >= required else 1,
        "last_append_bucket_ts_sec": adapter_commit_state.get("last_processed_bucket_ts_sec"),
        "series_reset_count": 0 if str(z_window.get("series_reset_reason") or "none") in {"", "none", "null"} else 1,
        "series_reset_reason": z_window.get("series_reset_reason"),
        "history_gap_class": history_gap_class,
        "valid_bucket_ratio": z_window.get("valid_bucket_ratio"),
        "store_window": store,
    }


def _bucket_commit_barrier_evidence(
    *,
    line: str,
    bucket_alignment: dict[str, Any],
    adapter_commit_state: dict[str, Any],
    z_window: dict[str, Any],
) -> dict[str, Any]:
    cvd_state = str(bucket_alignment.get("cvd_commit_state") or "missing")
    ofi_state = str(bucket_alignment.get("ofi_commit_state") or "missing")
    alignment_status = str(bucket_alignment.get("alignment_status") or "missing")
    processed = bucket_alignment.get("last_processed_bucket_ts_sec")
    cvd_ts = bucket_alignment.get("last_cvd_update_bucket_ts_sec")
    ofi_ts = bucket_alignment.get("last_ofi_update_bucket_ts_sec")
    cvd_ok = cvd_state in {"updated", "zero_delta"}
    ofi_ok = ofi_state in {"updated", "zero_delta"}
    factor_frame_appended = bool(processed is not None and (cvd_ok or ofi_ok))
    z_count = int(_number(z_window.get("z_window_count")) or 0)
    z_required = int(_number(z_window.get("z_window_required_count")) or (1 if line == "micro_fast" else 2))
    features_written_after_commit = bool(factor_frame_appended and z_count > 0)
    wait_read_after_commit = bool(
        processed is not None
        and (cvd_ts is None or _number(cvd_ts) <= _number(processed))
        and (ofi_ts is None or _number(ofi_ts) <= _number(processed))
    )
    if not cvd_ok:
        status = "failed"
        failed_stage = "cvd_commit_missing"
    elif not ofi_ok:
        status = "failed"
        failed_stage = "ofi_commit_missing"
    elif alignment_status != "aligned":
        status = "partial"
        failed_stage = "alignment_lag"
    elif z_count < z_required:
        status = "partial"
        failed_stage = "z_window_not_ready"
    else:
        status = "pass"
        failed_stage = None
    return {
        "strategy_line": line,
        "barrier_status": status,
        "failed_stage": failed_stage,
        "reference_bucket_ts_sec": bucket_alignment.get("reference_bucket_ts_sec"),
        "last_processed_bucket_ts_sec": processed,
        "last_cvd_update_bucket_ts_sec": cvd_ts,
        "last_ofi_update_bucket_ts_sec": ofi_ts,
        "cvd_commit_state": cvd_state,
        "ofi_commit_state": ofi_state,
        "alignment_status": alignment_status,
        "commit_barrier_status": bucket_alignment.get("commit_barrier_status"),
        "factor_frame_appended": factor_frame_appended,
        "features_written_after_commit": features_written_after_commit,
        "wait_read_after_commit": wait_read_after_commit,
        "processed_bucket_count": adapter_commit_state.get("processed_bucket_count"),
        "cvd_update_count": adapter_commit_state.get("cvd_update_count"),
        "ofi_update_count": adapter_commit_state.get("ofi_update_count"),
        "z_window_count": z_count,
        "z_window_required_count": z_required,
    }


def _coverage_root_cause_v2(
    *,
    line: str,
    stream_coverage: dict[str, Any],
    aggtrade_runtime: dict[str, Any],
    book_depth_runtime: dict[str, Any],
    z_window: dict[str, Any],
    cvd_runtime: dict[str, Any],
    candidate_dwell: dict[str, Any] | None = None,
) -> dict[str, Any]:
    target_churn = str(z_window.get("target_churn_state") or "stable")
    warmup_age = _number(z_window.get("warmup_age_sec"))
    warmup_required = _number(z_window.get("warmup_required_sec"))
    warmup_short = bool(warmup_age is not None and warmup_required is not None and warmup_age < warmup_required)
    dwell = candidate_dwell if isinstance(candidate_dwell, dict) else {}
    judgement_allowed = dwell.get("technical_finding_allowed") is not False

    def classify(logical: str) -> dict[str, Any]:
        entry = stream_coverage.get(logical) if isinstance(stream_coverage.get(logical), dict) else {}
        gap = str(entry.get("gap_class") or "")
        root = str(entry.get("root_cause") or "")
        ratio = _number(entry.get("coverage_ratio"))
        if logical == "aggTrade":
            runtime_gap = str(aggtrade_runtime.get("bucket_gap_class") or gap)
            never = str(cvd_runtime.get("never_updated_class") or "")
        elif logical == "bookTicker":
            runtime_gap = str(book_depth_runtime.get("book_stream_gap_class") or gap)
            never = ""
        else:
            runtime_gap = str(book_depth_runtime.get("depth_stream_gap_class") or gap)
            never = ""
        required_for_gate = True
        role = "required_for_gate"
        if logical == "partialDepth5" and line == "micro_fast":
            role = MICRO_FAST_DEPTH5_ROLE
            required_for_gate = role == "required_for_ofi"
        if logical == "partialDepth5" and line == "micro_fast" and not required_for_gate:
            if root in {"ok", ""} and runtime_gap in {"ok", ""}:
                class_name = "ok"
            elif role == "disabled":
                class_name = "disabled"
            else:
                class_name = "optional_missing"
        elif not judgement_allowed:
            class_name = str(dwell.get("dwell_state") or "warmup_not_met")
        elif runtime_gap in {"queue_drop", "ws_gap", "subscription_missing", "adapter_gap", "bucket_close_gap"}:
            class_name = "technical_stream_loss"
        elif root in {"queue_backpressure", "websocket_gap", "subscription_missing", "coverage_missing", "no_bucket_committed"}:
            class_name = "technical_stream_loss"
        elif target_churn in {"new_target", "evicted", "reentered"}:
            class_name = "target_churn"
        elif warmup_short:
            class_name = "warmup_not_met"
        elif runtime_gap in {"low_activity_or_churn", "ws_no_event"} or root in {"low_market_activity", "low_activity_or_churn"} or never == "low_activity_or_churn":
            class_name = "market_low_activity"
        elif ratio is not None and ratio < 0.15:
            class_name = "coverage_ratio_low"
        elif runtime_gap in {"ok", ""} and root in {"ok", ""}:
            class_name = "ok"
        else:
            class_name = "unknown"
        return {
            "coverage_class": class_name,
            "role": role,
            "required_for_gate": required_for_gate,
            "technical_stream_loss": class_name == "technical_stream_loss",
            "severity": "P0" if class_name == "technical_stream_loss" else ("P2" if class_name in {"optional_missing", "disabled"} else "P1"),
            "gap_class": gap,
            "runtime_gap_class": runtime_gap,
            "root_cause": root,
            "coverage_ratio": ratio,
            "active": entry.get("active"),
            "required": entry.get("required"),
            "target_churn_state": target_churn,
            "warmup_short": warmup_short,
        }

    return {
        "aggTrade": classify("aggTrade"),
        "bookTicker": classify("bookTicker"),
        "partialDepth5": classify("partialDepth5"),
    }


def _fast_z_continuity_evidence(
    *,
    line: str,
    symbol: str,
    z_window: dict[str, Any],
    z_history_runtime: dict[str, Any],
    bucket_barrier: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    count = int(_number(z_window.get("z_window_count")) or 0)
    required = int(_number(z_window.get("z_window_required_count")) or 1)
    target_churn_state = str(z_window.get("target_churn_state") or "stable")
    missing_reason = str(z_window.get("missing_reason") or "none")
    history_gap = str(z_history_runtime.get("history_gap_class") or "")
    if count >= required:
        continuity_status = "ready"
    elif target_churn_state in {"new_target", "evicted", "reentered"}:
        continuity_status = "target_churn"
    elif missing_reason in {"warmup_incomplete", "target_churn", "coverage_low"}:
        continuity_status = missing_reason
    elif history_gap:
        continuity_status = history_gap
    else:
        continuity_status = "insufficient_history"
    return {
        "symbol": symbol,
        "continuity_status": continuity_status,
        "z_window_count": count,
        "z_window_required_count": required,
        "valid_bucket_ratio": z_window.get("valid_bucket_ratio"),
        "target_retention_sec": z_window.get("target_retention_sec"),
        "target_churn_state": target_churn_state,
        "missing_reason": None if missing_reason == "none" else missing_reason,
        "history_gap_class": history_gap,
        "last_append_bucket_ts_sec": z_history_runtime.get("last_append_bucket_ts_sec"),
        "barrier_status": bucket_barrier.get("barrier_status"),
    }


def _aligned_frame_gate_evidence(
    *,
    line: str,
    bucket_alignment: dict[str, Any],
    z_window: dict[str, Any],
    bucket_barrier: dict[str, Any],
) -> dict[str, Any]:
    lag = _number(bucket_alignment.get("ofi_cvd_lag_bucket_sec"))
    alignment_status = str(bucket_alignment.get("alignment_status") or "missing")
    cvd_state = str(bucket_alignment.get("cvd_commit_state") or "missing")
    ofi_state = str(bucket_alignment.get("ofi_commit_state") or "missing")
    z_count = int(_number(z_window.get("z_window_count")) or 0)
    z_required = int(_number(z_window.get("z_window_required_count")) or (1 if line == "micro_fast" else 2))
    pass_gate = bool(
        alignment_status == "aligned"
        and cvd_state in {"updated", "zero_delta"}
        and ofi_state in {"updated", "zero_delta"}
        and z_count >= z_required
    )
    if pass_gate:
        block_reason = None
    elif alignment_status != "aligned":
        block_reason = "mixed_cvd_ofi_frame"
    elif cvd_state not in {"updated", "zero_delta"}:
        block_reason = "cvd_frame_missing"
    elif ofi_state not in {"updated", "zero_delta"}:
        block_reason = "ofi_frame_missing"
    else:
        block_reason = "z_window_not_ready"
    return {
        "strategy_line": line,
        "aligned_frame_pass": pass_gate,
        "block_reason": block_reason,
        "alignment_status": alignment_status,
        "lag_bucket_sec": lag,
        "lag_side": bucket_alignment.get("lag_side"),
        "cvd_commit_state": cvd_state,
        "ofi_commit_state": ofi_state,
        "z_window_count": z_count,
        "z_window_required_count": z_required,
        "barrier_status": bucket_barrier.get("barrier_status"),
    }


def _candidate_dwell_evidence(
    *,
    line: str,
    symbol: str,
    z_window: dict[str, Any],
    bucket_barrier: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    target_age = _number(z_window.get("target_retention_sec")) or _number(z_window.get("warmup_age_sec")) or 0.0
    z_count = int(_number(z_window.get("z_window_count")) or 0)
    target_churn_state = str(z_window.get("target_churn_state") or "stable")
    if target_churn_state in {"new_target", "reentered"}:
        state = "target_new"
        reason = "target_churn"
        allowed = False
    elif target_churn_state == "evicted":
        state = "target_evicted"
        reason = "target_churn"
        allowed = False
    elif target_age < MICRO_FAST_MIN_OBSERVE_SEC_BEFORE_JUDGEMENT:
        state = "min_observe_pending"
        reason = "target_too_young"
        allowed = False
    elif target_age < MICRO_FAST_MIN_DWELL_SEC:
        state = "dwell_pending"
        reason = "target_dwell_pending"
        allowed = False
    elif z_count < 1:
        state = "judgeable_no_z"
        reason = "z_window_not_ready"
        allowed = True
    else:
        state = "judgeable"
        reason = None
        allowed = True
    return {
        "symbol": symbol,
        "dwell_state": state,
        "block_reason": reason,
        "technical_finding_allowed": allowed,
        "target_age_sec": target_age,
        "min_observe_sec_before_judgement": MICRO_FAST_MIN_OBSERVE_SEC_BEFORE_JUDGEMENT,
        "min_dwell_sec": MICRO_FAST_MIN_DWELL_SEC,
        "warmup_bucket_count": MICRO_FAST_WARMUP_BUCKET_COUNT,
        "target_churn_state": target_churn_state,
        "z_window_count": z_count,
        "barrier_status": bucket_barrier.get("barrier_status"),
    }


def _cvd_commit_missing_trace(
    *,
    line: str,
    symbol: str,
    bucket_barrier: dict[str, Any],
    aggtrade_runtime: dict[str, Any],
    adapter_commit_state: dict[str, Any],
    stream_coverage: dict[str, Any],
    z_window: dict[str, Any],
    candidate_dwell: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    failed_stage = str(bucket_barrier.get("failed_stage") or "")
    cvd_state = str(bucket_barrier.get("cvd_commit_state") or "missing")
    stream = stream_coverage.get("aggTrade") if isinstance(stream_coverage.get("aggTrade"), dict) else {}
    if failed_stage != "cvd_commit_missing" and cvd_state in {"updated", "zero_delta"}:
        root = "none"
        severity = "ok"
    elif candidate_dwell.get("technical_finding_allowed") is False:
        root = str(candidate_dwell.get("block_reason") or "target_too_young")
        severity = "P2"
    elif stream.get("required") is True and stream.get("active") is False:
        root = "subscription_missing"
        severity = "P0"
    elif str(z_window.get("target_churn_state") or "") in {"new_target", "evicted", "reentered"}:
        root = "target_churn_race"
        severity = "P2"
    elif int(adapter_commit_state.get("adapter_error_count") or 0) > 0:
        root = "bucket_writer_error"
        severity = "P0"
    elif int(adapter_commit_state.get("processed_trade_bucket_count") or 0) > 0 and int(adapter_commit_state.get("cvd_update_count") or 0) <= 0:
        root = "adapter_commit_failed"
        severity = "P0"
    elif str(aggtrade_runtime.get("bucket_gap_class") or "") in {"queue_drop", "ws_gap", "subscription_missing", "adapter_gap"}:
        root = str(aggtrade_runtime.get("bucket_gap_class"))
        severity = "P0"
    elif int(adapter_commit_state.get("cvd_skipped_no_trade") or 0) > 0 and int(adapter_commit_state.get("cvd_update_count") or 0) <= 0:
        root = "zero_delta_commit_missing"
        severity = "P0"
    elif _number(stream.get("coverage_ratio")) is not None and (_number(stream.get("coverage_ratio")) or 0) <= 0:
        root = "no_trade_bucket"
        severity = "P1"
    elif _number(stream.get("coverage_ratio")) is not None and (_number(stream.get("coverage_ratio")) or 0) < 0.15:
        root = "low_activity_or_churn"
        severity = "market"
    else:
        root = "unknown_cvd_commit_missing"
        severity = "P0" if failed_stage == "cvd_commit_missing" else "P1"
    return {
        "symbol": symbol,
        "root_cause": root,
        "severity": severity,
        "failed_stage": failed_stage or None,
        "cvd_commit_state": cvd_state,
        "processed_bucket_count": adapter_commit_state.get("processed_bucket_count"),
        "processed_trade_bucket_count": adapter_commit_state.get("processed_trade_bucket_count"),
        "cvd_update_count": adapter_commit_state.get("cvd_update_count"),
        "cvd_skipped_no_trade": adapter_commit_state.get("cvd_skipped_no_trade"),
        "aggtrade_bucket_gap_class": aggtrade_runtime.get("bucket_gap_class"),
        "aggtrade_coverage_ratio": stream.get("coverage_ratio"),
        "target_churn_state": z_window.get("target_churn_state"),
    }


def _fast_z_nan_trace(
    *,
    line: str,
    symbol: str,
    micro_block: dict[str, Any],
    z_window: dict[str, Any],
    z_history_runtime: dict[str, Any],
    candidate_dwell: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    values = {
        "cvd": _raw_float_state(micro_block.get("cvd")),
        "ofi": _raw_float_state(micro_block.get("ofi")),
        "z_cvd": _raw_float_state(micro_block.get("z_cvd")),
        "z_ofi": _raw_float_state(micro_block.get("z_ofi")),
    }
    missing_reason = str(z_window.get("missing_reason") or "")
    history_gap = str(z_history_runtime.get("history_gap_class") or "")
    if candidate_dwell.get("technical_finding_allowed") is False:
        reason = str(candidate_dwell.get("block_reason") or "target_too_young")
        severity = "P2"
    elif values["z_cvd"] in {"nan", "inf", "invalid"}:
        reason = "fast_z_feature_nan_cvd_nan"
        severity = "P0"
    elif values["z_ofi"] in {"nan", "inf", "invalid"}:
        reason = "fast_z_feature_nan_ofi_nan"
        severity = "P0"
    elif values["cvd"] in {"nan", "inf", "invalid"}:
        reason = "fast_z_feature_nan_cvd_input_invalid"
        severity = "P0"
    elif values["ofi"] in {"nan", "inf", "invalid"}:
        reason = "fast_z_feature_nan_ofi_input_invalid"
        severity = "P0"
    elif history_gap == "zero_variance":
        reason = "fast_z_feature_nan_zero_variance"
        severity = "P1"
    elif missing_reason == "feature_nan":
        reason = "fast_z_feature_nan_unknown"
        severity = "P0"
    elif missing_reason:
        reason = missing_reason
        severity = "P1"
    else:
        reason = "ok"
        severity = "ok"
    return {
        "symbol": symbol,
        "reason": reason,
        "severity": severity,
        "value_states": values,
        "missing_reason": None if missing_reason in {"", "none"} else missing_reason,
        "history_gap_class": history_gap,
        "z_window_count": z_window.get("z_window_count"),
        "z_window_required_count": z_window.get("z_window_required_count"),
        "blocked_consumption": reason != "ok",
    }


def _judgeable_scope_evidence(
    *,
    line: str,
    symbol: str,
    candidate_dwell: dict[str, Any],
    fast_z_continuity: dict[str, Any],
    aligned_frame_gate: dict[str, Any],
    cvd_commit_trace: dict[str, Any],
    fast_z_nan_trace: dict[str, Any],
    raw_reason_codes: list[str],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    dwell_state = str(candidate_dwell.get("dwell_state") or "unknown")
    dwell_reason = str(candidate_dwell.get("block_reason") or "")
    z_status = str(fast_z_continuity.get("continuity_status") or "unknown")
    gate_pass = aligned_frame_gate.get("aligned_frame_pass") is True
    cvd_severity = str(cvd_commit_trace.get("severity") or "")
    nan_severity = str(fast_z_nan_trace.get("severity") or "")
    raw = {str(x) for x in raw_reason_codes}
    technical_allowed = candidate_dwell.get("technical_finding_allowed") is not False
    if not technical_allowed:
        scope = "not_judgeable_yet"
        reason = dwell_reason or dwell_state
        countable = False
    elif z_status != "ready":
        scope = "judgeable_but_z_missing"
        reason = z_status
        countable = True
    elif gate_pass and not raw:
        scope = "judgeable_and_ready"
        reason = None
        countable = False
    elif cvd_severity == "P0" or nan_severity == "P0" or aligned_frame_gate.get("block_reason"):
        scope = "judgeable_and_technical_failed"
        reason = str(aligned_frame_gate.get("block_reason") or cvd_commit_trace.get("root_cause") or fast_z_nan_trace.get("reason") or "technical_failed")
        countable = True
    else:
        scope = "judgeable_and_market_not_ready"
        reason = sorted(raw)[0] if raw else "market_not_ready"
        countable = False
    return {
        "symbol": symbol,
        "scope": scope,
        "reason": reason,
        "technical_failure_countable": countable,
        "target_age_sec": candidate_dwell.get("target_age_sec"),
        "min_observe_sec": candidate_dwell.get("min_observe_sec_before_judgement"),
        "min_dwell_sec": candidate_dwell.get("min_dwell_sec"),
        "z_window_count": fast_z_continuity.get("z_window_count"),
        "z_window_required_count": fast_z_continuity.get("z_window_required_count"),
        "aligned_frame_pass": gate_pass,
    }


def _judgeable_throughput_trace(
    *,
    line: str,
    symbol: str,
    candidate_dwell: dict[str, Any],
    fast_z_continuity: dict[str, Any],
    judgeable_scope: dict[str, Any],
    z_window: dict[str, Any],
    state_item: dict[str, Any] | None,
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    scope = str(judgeable_scope.get("scope") or "unknown")
    target_age = _number(candidate_dwell.get("target_age_sec")) or 0.0
    required_observe = _number(candidate_dwell.get("min_observe_sec_before_judgement")) or MICRO_FAST_MIN_OBSERVE_SEC_BEFORE_JUDGEMENT
    required_dwell = _number(candidate_dwell.get("min_dwell_sec")) or MICRO_FAST_MIN_DWELL_SEC
    bucket_count = int(_number(fast_z_continuity.get("z_window_count")) or 0)
    required_bucket_count = int(_number(fast_z_continuity.get("z_window_required_count")) or 1)
    valid_ratio = _number(fast_z_continuity.get("valid_bucket_ratio"))
    target_source = "unknown"
    if isinstance(state_item, dict):
        target_source = str(
            state_item.get("target_source")
            or state_item.get("source")
            or state_item.get("target_source_type")
            or "unknown"
        )
    churn_state = str(z_window.get("target_churn_state") or candidate_dwell.get("target_churn_state") or "stable")
    dwell_state = str(candidate_dwell.get("dwell_state") or "")
    if scope != "not_judgeable_yet":
        reason = "judgeable"
    elif target_age < required_observe:
        reason = "not_judgeable_target_too_young"
    elif target_age < required_dwell or dwell_state in {"dwell_pending", "min_observe_pending"}:
        reason = "not_judgeable_dwell_pending"
    elif bucket_count < required_bucket_count:
        reason = "not_judgeable_bucket_count_short"
    elif valid_ratio is not None and valid_ratio < 0.7:
        reason = "not_judgeable_valid_bucket_ratio_low"
    elif churn_state in {"new_target", "evicted", "reentered"}:
        reason = "not_judgeable_churn_reset"
    elif str(candidate_dwell.get("block_reason") or "") in {"runtime_resyncing", "health_guard_resyncing"}:
        reason = "not_judgeable_runtime_resyncing"
    else:
        reason = str(judgeable_scope.get("reason") or candidate_dwell.get("block_reason") or "not_judgeable_unknown")
        if not reason.startswith("not_judgeable_"):
            reason = f"not_judgeable_{reason}"
    candidate_id = f"{symbol}:{target_source}:{int(target_age)}"
    return {
        "symbol": symbol,
        "candidate_id": candidate_id,
        "target_age_sec": target_age,
        "required_observe_sec": required_observe,
        "dwell_sec": target_age,
        "required_dwell_sec": required_dwell,
        "bucket_count": bucket_count,
        "required_bucket_count": required_bucket_count,
        "valid_bucket_ratio": valid_ratio,
        "target_source": target_source,
        "target_churn_state": churn_state,
        "not_judgeable_reason": reason,
        "judgeable_yield_countable": scope != "not_judgeable_yet",
    }


def _target_cadence_trace(
    *,
    line: str,
    symbol: str,
    judgeable_throughput_trace: dict[str, Any],
    state_item: dict[str, Any] | None,
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    first_seen = state_item.get("first_seen_at") if isinstance(state_item, dict) else None
    last_seen = state_item.get("last_seen_at") if isinstance(state_item, dict) else None
    evict_reason = state_item.get("evict_reason") if isinstance(state_item, dict) else None
    reentered = bool(state_item.get("reentered")) if isinstance(state_item, dict) else False
    reason = str(judgeable_throughput_trace.get("not_judgeable_reason") or "unknown")
    evicted_before_judgeable = bool(reason != "judgeable" and evict_reason)
    return {
        "symbol": symbol,
        "candidate_created": True,
        "candidate_evicted_before_judgeable": evicted_before_judgeable,
        "candidate_reentered": reentered or reason == "not_judgeable_churn_reset",
        "target_age_sec": judgeable_throughput_trace.get("target_age_sec"),
        "dwell_sec": judgeable_throughput_trace.get("dwell_sec"),
        "judgeable_transition": reason == "judgeable",
        "not_judgeable_expired": reason == "not_judgeable_observe_pool_evicted",
        "first_seen_at": first_seen,
        "last_seen_at": last_seen,
        "evict_reason": evict_reason,
    }


def _observe_pool_trace(
    *,
    line: str,
    symbol: str,
    judgeable_throughput_trace: dict[str, Any],
    state_item: dict[str, Any] | None,
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    target_source = str(judgeable_throughput_trace.get("target_source") or "unknown")
    source_text = target_source.lower()
    in_sticky = "sticky" in source_text
    in_observe = source_text not in {"", "unknown", "none"}
    pool_state = "sticky_observe_pool" if in_sticky else ("observe_pool" if in_observe else "untracked")
    if isinstance(state_item, dict) and state_item.get("observe_pool_state"):
        pool_state = str(state_item.get("observe_pool_state"))
    return {
        "symbol": symbol,
        "pool_state": pool_state,
        "in_observe_pool": pool_state in {"observe_pool", "sticky_observe_pool"},
        "in_sticky_observe_pool": pool_state == "sticky_observe_pool",
        "target_source": target_source,
        "target_age_sec": judgeable_throughput_trace.get("target_age_sec"),
        "max_observe_sec": 300,
        "evict_reason": state_item.get("evict_reason") if isinstance(state_item, dict) else None,
    }


def _coverage_market_technical_split(
    *,
    line: str,
    coverage_root_cause_v2: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    out: dict[str, Any] = {}
    for stream, entry in coverage_root_cause_v2.items():
        if not isinstance(entry, dict):
            continue
        klass = str(entry.get("coverage_class") or "unknown")
        root = str(entry.get("root_cause") or "unknown")
        if klass in {"technical_stream_loss"} or root in {
            "queue_backpressure",
            "websocket_gap",
            "subscription_missing",
            "coverage_missing",
            "no_bucket_committed",
        }:
            group = "technical"
        elif klass in {"warmup_not_met", "target_churn"}:
            group = "expected_warmup"
        elif klass in {"market_low_activity", "coverage_ratio_low"} or root in {"low_market_activity", "low_activity_or_churn"}:
            group = "market"
        elif klass in {"optional_missing", "disabled"}:
            group = "optional"
        elif klass == "ok":
            group = "ok"
        else:
            group = "unknown"
        out[stream] = {
            "group": group,
            "coverage_class": klass,
            "root_cause": root,
            "coverage_ratio": entry.get("coverage_ratio"),
            "role": entry.get("role"),
            "required_for_gate": entry.get("required_for_gate"),
        }
    return out


def _valid_bucket_ratio_low_trace(
    *,
    line: str,
    symbol: str,
    fast_z_reader_window_short_trace: dict[str, Any],
    aggtrade_runtime: dict[str, Any],
    book_depth_runtime: dict[str, Any],
    candidate_dwell: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    ratio = _number(fast_z_reader_window_short_trace.get("valid_bucket_ratio"))
    actual = int(_number(fast_z_reader_window_short_trace.get("available_bucket_count")) or 0)
    expected = int(_number(fast_z_reader_window_short_trace.get("required_bucket_count")) or 0)
    valid = int(_number(fast_z_reader_window_short_trace.get("valid_bucket_count")) or actual)
    agg_gap = int(_number(aggtrade_runtime.get("queue_dropped_count")) or 0)
    book_gap = int(_number(book_depth_runtime.get("book_queue_dropped_count")) or 0)
    commit_gap = 1 if str(fast_z_reader_window_short_trace.get("history_gap_class") or "") in {"bucket_gap", "series_reset"} else 0
    low_activity = 1 if str(aggtrade_runtime.get("bucket_gap_class") or "") == "low_activity_or_churn" else 0
    churn_gap = 1 if str(candidate_dwell.get("target_churn_state") or "") in {"new_target", "evicted", "reentered"} else 0
    if ratio is None or ratio >= 0.7:
        root = "ok"
    elif agg_gap > 0:
        root = "aggtrade_gap"
    elif book_gap > 0:
        root = "bookticker_gap"
    elif commit_gap:
        root = "commit_gap"
    elif churn_gap:
        root = "churn"
    elif low_activity:
        root = "low_activity"
    else:
        root = "unknown"
    return {
        "symbol": symbol,
        "expected_bucket_count": expected,
        "actual_bucket_count": actual,
        "valid_bucket_count": valid,
        "valid_bucket_ratio": ratio,
        "aggtrade_gap_count": agg_gap,
        "bookticker_gap_count": book_gap,
        "bucket_commit_gap_count": commit_gap,
        "low_activity_bucket_count": low_activity,
        "target_churn_gap_count": churn_gap,
        "root_cause": root,
    }


def _fast_z_append_read_trace(
    *,
    line: str,
    symbol: str,
    bucket_barrier: dict[str, Any],
    z_window: dict[str, Any],
    z_history_runtime: dict[str, Any],
    fast_z_nan_trace: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    z_count = int(_number(z_window.get("z_window_count")) or 0)
    z_required = int(_number(z_window.get("z_window_required_count")) or 1)
    barrier_status = str(bucket_barrier.get("barrier_status") or "unknown")
    failed_stage = str(bucket_barrier.get("failed_stage") or "")
    append_count = int(_number(z_history_runtime.get("append_success_count")) or 0)
    append_attempted = barrier_status in {"pass", "partial"} or bucket_barrier.get("factor_frame_appended") is True
    append_success = append_count > 0
    reader_window_count = z_count
    if z_count >= z_required:
        trace_status = "ready"
        skip_reason = None
    elif not append_attempted:
        trace_status = "append_not_attempted"
        skip_reason = failed_stage or "bucket_not_committed"
    elif failed_stage in {"cvd_commit_missing", "ofi_commit_missing"}:
        trace_status = "append_skipped_no_commit"
        skip_reason = failed_stage
    elif str(fast_z_nan_trace.get("severity") or "") == "P0":
        trace_status = "append_skipped_invalid_value"
        skip_reason = fast_z_nan_trace.get("reason")
    elif append_success and reader_window_count <= 0:
        trace_status = "append_success_reader_empty"
        skip_reason = str(z_history_runtime.get("history_gap_class") or "reader_empty")
    elif reader_window_count < z_required:
        trace_status = "reader_window_short"
        skip_reason = str(z_window.get("missing_reason") or z_history_runtime.get("history_gap_class") or "insufficient_history")
    else:
        trace_status = "unknown"
        skip_reason = None
    return {
        "series_key": f"{line}:{symbol}",
        "append_attempted": append_attempted,
        "append_success": append_success,
        "append_skip_reason": skip_reason,
        "last_append_bucket_ts_sec": z_history_runtime.get("last_append_bucket_ts_sec"),
        "reader_bucket_ts_sec": bucket_barrier.get("last_processed_bucket_ts_sec"),
        "reader_window_count": reader_window_count,
        "reader_required_count": z_required,
        "run_id_match": True,
        "symbol_match": bool(symbol),
        "trace_status": trace_status,
        "history_gap_class": z_history_runtime.get("history_gap_class"),
    }


def _fast_z_reader_window_short_trace(
    *,
    line: str,
    symbol: str,
    z_window: dict[str, Any],
    z_history_runtime: dict[str, Any],
    candidate_dwell: dict[str, Any],
    fast_z_append_read_trace: dict[str, Any],
    bucket_barrier: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    trace_status = str(fast_z_append_read_trace.get("trace_status") or "")
    z_count = int(_number(z_window.get("z_window_count")) or 0)
    required = int(_number(z_window.get("z_window_required_count")) or 1)
    valid_ratio = _number(z_window.get("valid_bucket_ratio"))
    target_age = _number(candidate_dwell.get("target_age_sec"))
    min_observe = _number(candidate_dwell.get("min_observe_sec_before_judgement"))
    min_dwell = _number(candidate_dwell.get("min_dwell_sec"))
    history_gap = str(z_history_runtime.get("history_gap_class") or "")
    append_count = int(_number(z_history_runtime.get("append_success_count")) or 0)
    last_append = _number(z_history_runtime.get("last_append_bucket_ts_sec"))
    reader_bucket = _number(fast_z_append_read_trace.get("reader_bucket_ts_sec") or bucket_barrier.get("last_processed_bucket_ts_sec"))
    target_too_young = bool(target_age is not None and min_observe is not None and target_age < min_observe)
    dwell_pending = str(candidate_dwell.get("dwell_state") or "") in {"dwell_pending", "min_observe_pending"}
    bucket_mismatch = bool(last_append is not None and reader_bucket is not None and last_append != reader_bucket)

    if trace_status == "ready":
        root = None
    elif target_too_young:
        root = "reader_window_short_target_too_young"
    elif dwell_pending:
        root = "reader_window_short_dwell_pending"
    elif history_gap == "valid_bucket_ratio_low" or (valid_ratio is not None and valid_ratio < 0.7):
        root = "reader_window_short_valid_bucket_ratio_low"
    elif history_gap in {"bucket_gap", "series_reset"}:
        root = "reader_window_short_series_append_gap"
    elif bucket_mismatch:
        root = "reader_window_short_reader_bucket_mismatch"
    elif history_gap in {"series_not_persisted", "store_read_failed"}:
        root = "reader_window_short_history_pruned"
    elif trace_status == "reader_window_short" or z_count < required:
        root = "reader_window_short_unknown"
    else:
        root = None

    return {
        "symbol": symbol,
        "root_cause": root,
        "trace_status": trace_status,
        "required_bucket_count": required,
        "available_bucket_count": z_count,
        "valid_bucket_count": z_count,
        "valid_bucket_ratio": valid_ratio,
        "target_age_sec": target_age,
        "target_dwell_sec": target_age,
        "min_observe_sec": min_observe,
        "min_dwell_sec": min_dwell,
        "first_bucket_ts_sec": z_window.get("first_bucket_ts_sec"),
        "last_bucket_ts_sec": z_window.get("last_bucket_ts_sec") or last_append,
        "reader_start_bucket_ts_sec": z_window.get("reader_start_bucket_ts_sec"),
        "reader_end_bucket_ts_sec": reader_bucket,
        "last_append_bucket_ts_sec": last_append,
        "series_gap_count": 1 if history_gap in {"bucket_gap", "series_reset"} else 0,
        "append_success_count": append_count,
        "history_pruned": history_gap in {"series_not_persisted", "store_read_failed"},
        "history_gap_class": history_gap,
        "reader_bucket_mismatch": bucket_mismatch,
    }


def _fast_z_invalid_value_trace(
    *,
    line: str,
    symbol: str,
    fast_z_nan_trace: dict[str, Any],
    z_history_runtime: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    reason = str(fast_z_nan_trace.get("reason") or "")
    value_states = fast_z_nan_trace.get("value_states") if isinstance(fast_z_nan_trace.get("value_states"), dict) else {}
    history_gap = str(z_history_runtime.get("history_gap_class") or "")
    feature = None
    root = None
    raw_state = None
    normalizer_state = None

    if reason == "fast_z_feature_nan_cvd_nan":
        feature = "z_cvd"
        raw_state = value_states.get("z_cvd")
        root = "fast_z_input_nan" if raw_state == "nan" else "fast_z_input_inf" if raw_state == "inf" else "fast_z_input_invalid"
    elif reason == "fast_z_feature_nan_ofi_nan":
        feature = "z_ofi"
        raw_state = value_states.get("z_ofi")
        root = "fast_z_input_nan" if raw_state == "nan" else "fast_z_input_inf" if raw_state == "inf" else "fast_z_input_invalid"
    elif reason == "fast_z_feature_nan_cvd_input_invalid":
        feature = "cvd"
        raw_state = value_states.get("cvd")
        root = "fast_z_input_invalid"
    elif reason == "fast_z_feature_nan_ofi_input_invalid":
        feature = "ofi"
        raw_state = value_states.get("ofi")
        root = "fast_z_input_invalid"
    elif reason == "fast_z_feature_nan_zero_variance" or history_gap == "zero_variance":
        root = "fast_z_zero_variance"
        normalizer_state = "zero_variance"
    elif reason == "fast_z_feature_nan_unknown":
        root = "fast_z_invalid_unknown"
    elif reason in {"feature_nan", "fast_z_feature_nan"}:
        root = "fast_z_normalizer_invalid"
    elif reason and reason not in {"ok", "target_too_young", "target_dwell_pending", "warmup_incomplete", "coverage_low", "insufficient_history"}:
        root = "fast_z_normalizer_invalid"

    return {
        "symbol": symbol,
        "root_cause": root,
        "feature": feature,
        "reason": reason or None,
        "raw_value_state": raw_state,
        "normalizer_state": normalizer_state,
        "value_states": value_states,
        "append_action": "skipped" if root else "accepted",
        "reader_action": "not_consumable" if root else "consumable",
        "is_consumable": root is None,
        "history_gap_class": history_gap,
    }


def _cvd_commit_tail_trace(
    *,
    line: str,
    symbol: str,
    bucket_barrier: dict[str, Any],
    adapter_commit_state: dict[str, Any],
    aggtrade_runtime: dict[str, Any],
    cvd_ofi_freshness_trace: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    bucket_ts = bucket_barrier.get("last_processed_bucket_ts_sec")
    cvd_state = str(bucket_barrier.get("cvd_commit_state") or "missing")
    freshness_root = str(cvd_ofi_freshness_trace.get("stale_root_cause") or "")
    event_count = int(_number(adapter_commit_state.get("processed_trade_bucket_count")) or 0)
    skipped_no_trade = int(_number(adapter_commit_state.get("cvd_skipped_no_trade")) or 0)
    update_count = int(_number(adapter_commit_state.get("cvd_update_count")) or 0)
    zero_expected = event_count <= 0 or skipped_no_trade > 0
    zero_written = cvd_state == "zero_delta"

    if cvd_state == "zero_delta":
        root = "cvd_tail_zero_delta_commit_ok"
    elif cvd_state == "updated":
        root = None
    elif bucket_ts is None:
        root = "cvd_tail_no_bucket_close"
    elif zero_expected and not zero_written:
        root = "cvd_tail_zero_delta_commit_missing"
    elif freshness_root == "cvd_old":
        root = "cvd_tail_reader_old_frame"
    elif freshness_root == "cvd_missing_commit":
        root = "cvd_tail_commit_write_failed"
    elif str(aggtrade_runtime.get("bucket_gap_class") or "") in {"ws_gap", "queue_drop", "adapter_gap", "bucket_gap"}:
        root = "cvd_tail_aggtrade_gap"
    elif update_count <= 0:
        root = "cvd_tail_commit_write_failed"
    else:
        root = "cvd_tail_unknown"

    return {
        "symbol": symbol,
        "bucket_ts_sec": bucket_ts,
        "bucket_close_seen": bucket_ts is not None,
        "aggtrade_event_count": event_count,
        "zero_delta_commit_expected": zero_expected,
        "zero_delta_commit_written": zero_written,
        "cvd_commit_state": cvd_state,
        "reader_cvd_age_sec": cvd_ofi_freshness_trace.get("cvd_age_bucket_sec"),
        "root_cause": root,
        "cvd_update_count": update_count,
        "cvd_skipped_no_trade": skipped_no_trade,
        "aggtrade_bucket_gap_class": aggtrade_runtime.get("bucket_gap_class"),
    }


def _cvd_ofi_bucket_freshness_trace(
    *,
    line: str,
    bucket_alignment: dict[str, Any],
    bucket_barrier: dict[str, Any],
) -> dict[str, Any]:
    if line != "micro_fast":
        return {}
    cvd_state = str(bucket_barrier.get("cvd_commit_state") or "missing")
    ofi_state = str(bucket_barrier.get("ofi_commit_state") or "missing")
    alignment_status = str(bucket_alignment.get("alignment_status") or "missing")
    lag_side = str(bucket_alignment.get("lag_side") or "missing")
    bucket_closed = bucket_barrier.get("last_processed_bucket_ts_sec") is not None
    cvd_ok = cvd_state in {"updated", "zero_delta"}
    ofi_ok = ofi_state in {"updated", "zero_delta"}
    if not bucket_closed:
        status = "bucket_not_closed"
        root = "bucket_not_closed"
    elif cvd_state == "missing":
        status = "stale"
        root = "cvd_missing_commit"
    elif ofi_state == "missing":
        status = "stale"
        root = "ofi_missing_commit"
    elif cvd_state == "zero_delta" or ofi_state == "zero_delta":
        status = "zero_delta_commit_ok" if alignment_status == "aligned" else "zero_delta_alignment_lag"
        root = None if alignment_status == "aligned" else lag_side
    elif alignment_status == "aligned" and cvd_ok and ofi_ok:
        status = "aligned_fresh"
        root = None
    elif lag_side == "cvd_old":
        status = "stale"
        root = "cvd_old"
    elif lag_side == "ofi_old":
        status = "stale"
        root = "ofi_old"
    elif lag_side == "missing":
        status = "stale"
        root = "freshness_missing"
    else:
        status = "stale"
        root = "both_old"
    return {
        "reference_bucket_ts_sec": bucket_alignment.get("reference_bucket_ts_sec"),
        "bucket_closed": bucket_closed,
        "cvd_commit_state": cvd_state,
        "ofi_commit_state": ofi_state,
        "cvd_age_bucket_sec": bucket_alignment.get("cvd_age_bucket_sec"),
        "ofi_age_bucket_sec": bucket_alignment.get("ofi_age_bucket_sec"),
        "lag_side": lag_side,
        "lag_bucket_sec": bucket_alignment.get("ofi_cvd_lag_bucket_sec"),
        "freshness_status": status,
        "stale_root_cause": root,
    }


def _collect_evidence(
    *,
    lifecycle_item: dict[str, Any],
    line: str,
    feature_item: dict[str, Any] | None,
    features_doc: dict[str, Any] | None,
    state_doc: dict[str, Any] | None,
    state_item: dict[str, Any] | None = None,
    project_root: Path | None = None,
) -> dict[str, Any]:
    fitem = feature_item or {}
    first_block = _quality_block_for_line(fitem, line)
    micro_block = _micro_block_for_line(fitem, line)
    coverage = first_block.get("coverage") if isinstance(first_block.get("coverage"), dict) else {}
    agg_ratio = _coverage(first_block, "aggTrade")
    book_ratio = _coverage(first_block, "bookTicker")
    depth_ratio = _coverage(first_block, "partialDepth5")
    driver_metrics = first_block.get("driver_metrics_summary") if isinstance(first_block.get("driver_metrics_summary"), dict) else {}
    subscription_state = state_item.get("subscription_state") if isinstance(state_item, dict) and isinstance(state_item.get("subscription_state"), dict) else {}
    health_guard_state = state_item.get("health_guard_state") if isinstance(state_item, dict) and isinstance(state_item.get("health_guard_state"), dict) else {}
    dropped_events = (features_doc or {}).get("dropped_events") or {}
    ws_status = (features_doc or {}).get("ws_status")
    last_ws_age = _number((features_doc or {}).get("last_ws_message_age_sec"))
    stream_coverage = {
        key: _coverage_entry(first_block, subscription_state, key)
        for key in ("aggTrade", "bookTicker", "partialDepth5")
    }
    for logical, entry in stream_coverage.items():
        entry["gap_class"] = _classify_stream_gap(
            logical=logical,
            entry=entry,
            ws_status=str(ws_status or ""),
            last_ws_age=last_ws_age,
            dropped=dropped_events if isinstance(dropped_events, dict) else {},
        )
        entry["root_cause"] = _coverage_root_cause(entry)
    adapter_commit_state = _adapter_commit_state(driver_metrics, first_block)
    bucket_alignment = _bucket_alignment(first_block)
    reason_codes = list(lifecycle_item.get("reason_codes") or [])
    blocks = [first_block] if first_block else []
    for block in blocks:
        for code in block.get("reason_codes") or []:
            if code not in reason_codes:
                reason_codes.append(str(code))

    cvd_age = _number(first_block.get("cvd_update_age_sec"))
    ofi_age = _number(first_block.get("ofi_update_age_sec"))
    lag = _number(first_block.get("max_lag_sec"))
    if lag is None and cvd_age is not None and ofi_age is not None:
        lag = abs(cvd_age - ofi_age)
    timeline = {
        "reference_bucket_ts_sec": first_block.get("reference_bucket_ts_sec"),
        "last_processed_bucket_ts_sec": first_block.get("last_processed_bucket_ts_sec"),
        "last_cvd_update_bucket_ts_sec": first_block.get("last_cvd_update_bucket_ts_sec"),
        "last_ofi_update_bucket_ts_sec": first_block.get("last_ofi_update_bucket_ts_sec"),
        "cvd_age_bucket_sec": first_block.get("cvd_age_bucket_sec"),
        "ofi_age_bucket_sec": first_block.get("ofi_age_bucket_sec"),
        "ofi_cvd_lag_bucket_sec": first_block.get("ofi_cvd_lag_bucket_sec"),
    }

    warmup_age = _number(first_block.get("warmup_age_sec")) or _number(lifecycle_item.get("observed_sec"))
    required = _number(lifecycle_item.get("required_observed_sec"))
    if required is None:
        required = _number(first_block.get("min_ready_seconds"))
    z_cvd_available = micro_block.get("z_cvd") is not None
    z_ofi_available = micro_block.get("z_ofi") is not None
    z_count = int(z_cvd_available) + int(z_ofi_available)
    z_required = 1 if line == "micro_fast" else 2
    valid_bucket_ratio = None
    ratios = [x for x in (agg_ratio, book_ratio, depth_ratio) if x is not None]
    if ratios:
        valid_bucket_ratio = min(ratios)
    missing_reason = None
    target_age_sec = (state_doc or {}).get("target_age_sec") or (features_doc or {}).get("target_age_sec")
    target_retention_sec = _number((state_item or {}).get("target_age_sec")) or _number(target_age_sec) or warmup_age
    target_churn_state = str((state_item or {}).get("target_churn_state") or lifecycle_item.get("target_churn_state") or "stable")
    series_reset_reason = str(first_block.get("series_reset_reason") or micro_block.get("series_reset_reason") or "none")
    if z_count < z_required:
        if warmup_age is not None and required is not None and warmup_age < required:
            missing_reason = "warmup_incomplete"
        elif target_churn_state in {"new_target", "evicted", "reentered"}:
            missing_reason = "target_churn"
        elif valid_bucket_ratio is not None and valid_bucket_ratio < 0.15:
            missing_reason = "coverage_low"
        elif series_reset_reason not in {"", "none", "null"}:
            missing_reason = "series_reset"
        elif (
            adapter_commit_state.get("cvd_update_count", 0) <= 0
            and adapter_commit_state.get("ofi_update_count", 0) <= 0
            and (adapter_commit_state.get("processed_bucket_count", 0) > 0 or any((stream_coverage[s].get("coverage_ratio") or 0) > 0 for s in stream_coverage))
        ):
            missing_reason = "adapter_missing"
        elif z_count == 0 and (micro_block.get("cvd") is not None or micro_block.get("ofi") is not None):
            missing_reason = "feature_nan"
        else:
            missing_reason = "insufficient_history"
    z_window = {
        "mode": "fast" if line == "micro_fast" else "full",
        "z_window_count": z_count,
        "z_window_required_count": z_required,
        "valid_bucket_ratio": valid_bucket_ratio,
        "warmup_age_sec": warmup_age,
        "warmup_required_sec": required,
        "target_retention_sec": target_retention_sec,
        "target_churn_state": target_churn_state,
        "series_reset_reason": series_reset_reason,
        "cvd_z_available": z_cvd_available,
        "ofi_z_available": z_ofi_available,
        "missing_reason": missing_reason,
    }
    cvd_runtime = _cvd_runtime_state(
        stream_entry=stream_coverage["aggTrade"],
        adapter_commit_state=adapter_commit_state,
        ws_status=str(ws_status or ""),
        last_ws_age=last_ws_age,
        dropped=dropped_events if isinstance(dropped_events, dict) else {},
        target_age_sec=target_age_sec,
        warmup_age_sec=warmup_age,
        required_sec=required,
    )
    sym = _symbol(lifecycle_item)
    aggtrade_runtime = _aggtrade_runtime_state(
        stream_entry=stream_coverage["aggTrade"],
        adapter_commit_state=adapter_commit_state,
        ws_status=str(ws_status or ""),
        last_ws_age=last_ws_age,
        dropped=dropped_events if isinstance(dropped_events, dict) else {},
    )
    book_depth_runtime = _book_depth_runtime_state(
        book_entry=stream_coverage["bookTicker"],
        depth_entry=stream_coverage["partialDepth5"],
        adapter_commit_state=adapter_commit_state,
        ws_status=str(ws_status or ""),
        last_ws_age=last_ws_age,
        dropped=dropped_events if isinstance(dropped_events, dict) else {},
    )
    store_window: dict[str, Any] | None = None
    if line == "micro_full" and sym:
        window_sec = int(_number(required) or _number(first_block.get("min_ready_seconds")) or 900)
        now_bucket = _number(first_block.get("last_processed_bucket_ts_sec")) or _number(first_block.get("reference_bucket_ts_sec"))
        store_window = full_z_window_from_store(
            db_path=default_micro_factor_db(project_root),
            strategy_line=line,
            symbol=sym,
            now_bucket_ts_sec=int(now_bucket) if now_bucket is not None else None,
            window_sec=window_sec,
            min_valid_bucket_ratio=0.7,
            max_gap_sec=15,
        )
        z_window["store_window"] = store_window
    z_history_runtime = _z_history_runtime_state(
        line=line,
        symbol=sym,
        z_window=z_window,
        adapter_commit_state=adapter_commit_state,
        store_window=store_window,
    )
    bucket_commit_barrier = _bucket_commit_barrier_evidence(
        line=line,
        bucket_alignment=bucket_alignment,
        adapter_commit_state=adapter_commit_state,
        z_window=z_window,
    )
    candidate_dwell = _candidate_dwell_evidence(
        line=line,
        symbol=sym,
        z_window=z_window,
        bucket_barrier=bucket_commit_barrier,
    )
    coverage_root_v2 = _coverage_root_cause_v2(
        line=line,
        stream_coverage=stream_coverage,
        aggtrade_runtime=aggtrade_runtime,
        book_depth_runtime=book_depth_runtime,
        z_window=z_window,
        cvd_runtime=cvd_runtime,
        candidate_dwell=candidate_dwell,
    )
    fast_z_continuity = _fast_z_continuity_evidence(
        line=line,
        symbol=sym,
        z_window=z_window,
        z_history_runtime=z_history_runtime,
        bucket_barrier=bucket_commit_barrier,
    )
    aligned_frame_gate = _aligned_frame_gate_evidence(
        line=line,
        bucket_alignment=bucket_alignment,
        z_window=z_window,
        bucket_barrier=bucket_commit_barrier,
    )
    cvd_commit_trace = _cvd_commit_missing_trace(
        line=line,
        symbol=sym,
        bucket_barrier=bucket_commit_barrier,
        aggtrade_runtime=aggtrade_runtime,
        adapter_commit_state=adapter_commit_state,
        stream_coverage=stream_coverage,
        z_window=z_window,
        candidate_dwell=candidate_dwell,
    )
    fast_z_nan_trace = _fast_z_nan_trace(
        line=line,
        symbol=sym,
        micro_block=micro_block,
        z_window=z_window,
        z_history_runtime=z_history_runtime,
        candidate_dwell=candidate_dwell,
    )
    judgeable_scope = _judgeable_scope_evidence(
        line=line,
        symbol=sym,
        candidate_dwell=candidate_dwell,
        fast_z_continuity=fast_z_continuity,
        aligned_frame_gate=aligned_frame_gate,
        cvd_commit_trace=cvd_commit_trace,
        fast_z_nan_trace=fast_z_nan_trace,
        raw_reason_codes=reason_codes,
    )
    fast_z_append_read = _fast_z_append_read_trace(
        line=line,
        symbol=sym,
        bucket_barrier=bucket_commit_barrier,
        z_window=z_window,
        z_history_runtime=z_history_runtime,
        fast_z_nan_trace=fast_z_nan_trace,
    )
    fast_z_reader_window_short = _fast_z_reader_window_short_trace(
        line=line,
        symbol=sym,
        z_window=z_window,
        z_history_runtime=z_history_runtime,
        candidate_dwell=candidate_dwell,
        fast_z_append_read_trace=fast_z_append_read,
        bucket_barrier=bucket_commit_barrier,
    )
    judgeable_throughput = _judgeable_throughput_trace(
        line=line,
        symbol=sym,
        candidate_dwell=candidate_dwell,
        fast_z_continuity=fast_z_continuity,
        judgeable_scope=judgeable_scope,
        z_window=z_window,
        state_item=state_item,
    )
    target_cadence = _target_cadence_trace(
        line=line,
        symbol=sym,
        judgeable_throughput_trace=judgeable_throughput,
        state_item=state_item,
    )
    observe_pool = _observe_pool_trace(
        line=line,
        symbol=sym,
        judgeable_throughput_trace=judgeable_throughput,
        state_item=state_item,
    )
    coverage_market_technical = _coverage_market_technical_split(
        line=line,
        coverage_root_cause_v2=coverage_root_v2,
    )
    valid_bucket_ratio_low = _valid_bucket_ratio_low_trace(
        line=line,
        symbol=sym,
        fast_z_reader_window_short_trace=fast_z_reader_window_short,
        aggtrade_runtime=aggtrade_runtime,
        book_depth_runtime=book_depth_runtime,
        candidate_dwell=candidate_dwell,
    )
    fast_z_invalid_value = _fast_z_invalid_value_trace(
        line=line,
        symbol=sym,
        fast_z_nan_trace=fast_z_nan_trace,
        z_history_runtime=z_history_runtime,
    )
    cvd_ofi_freshness = _cvd_ofi_bucket_freshness_trace(
        line=line,
        bucket_alignment=bucket_alignment,
        bucket_barrier=bucket_commit_barrier,
    )
    cvd_commit_tail = _cvd_commit_tail_trace(
        line=line,
        symbol=sym,
        bucket_barrier=bucket_commit_barrier,
        adapter_commit_state=adapter_commit_state,
        aggtrade_runtime=aggtrade_runtime,
        cvd_ofi_freshness_trace=cvd_ofi_freshness,
    )

    return {
        "raw_reason_codes": reason_codes,
        "state": lifecycle_item.get("state") or lifecycle_item.get("status"),
        "terminal": lifecycle_item.get("terminal"),
        "trade_plan_consumable": lifecycle_item.get("trade_plan_consumable"),
        "consumption_block_reason": lifecycle_item.get("consumption_block_reason"),
        "cvd_update_age_sec": cvd_age,
        "ofi_update_age_sec": ofi_age,
        "ofi_cvd_lag_sec": lag,
        "ofi_cvd_lag_side": first_block.get("ofi_cvd_lag_side"),
        "last_cvd_update_bucket_ts_sec": first_block.get("last_cvd_update_bucket_ts_sec"),
        "last_ofi_update_bucket_ts_sec": first_block.get("last_ofi_update_bucket_ts_sec"),
        "last_processed_bucket_ts_sec": first_block.get("last_processed_bucket_ts_sec"),
        "reference_bucket_ts_sec": first_block.get("reference_bucket_ts_sec"),
        "cvd_age_bucket_sec": first_block.get("cvd_age_bucket_sec"),
        "ofi_age_bucket_sec": first_block.get("ofi_age_bucket_sec"),
        "ofi_cvd_lag_bucket_sec": first_block.get("ofi_cvd_lag_bucket_sec"),
        "warmup_age_sec": warmup_age,
        "warmup_required_sec": required,
        "aggtrade_coverage_ratio": agg_ratio,
        "bookticker_coverage_ratio": book_ratio,
        "depth5_coverage_ratio": depth_ratio,
        "ws_status": ws_status,
        "last_ws_message_age_sec": (features_doc or {}).get("last_ws_message_age_sec"),
        "dropped_events": dropped_events,
        "daemon_status": (state_doc or {}).get("daemon_status"),
        "health_state": (state_doc or {}).get("health_state"),
        "target_status": (state_doc or {}).get("target_status") or (features_doc or {}).get("target_status"),
        "target_age_sec": target_age_sec,
        "driver_metrics_summary": driver_metrics,
        "stream_coverage": stream_coverage,
        "cvd_runtime": cvd_runtime,
        "aggtrade_runtime": aggtrade_runtime,
        "book_depth_runtime": book_depth_runtime,
        "z_history_runtime": z_history_runtime,
        "bucket_commit_barrier": bucket_commit_barrier,
        "coverage_root_cause_v2": coverage_root_v2,
        "fast_z_continuity": fast_z_continuity,
        "aligned_frame_gate": aligned_frame_gate,
        "candidate_dwell": candidate_dwell,
        "cvd_commit_missing_trace": cvd_commit_trace,
        "fast_z_nan_trace": fast_z_nan_trace,
        "judgeable_scope": judgeable_scope,
        "judgeable_throughput_trace": judgeable_throughput,
        "target_cadence_trace": target_cadence,
        "observe_pool_trace": observe_pool,
        "coverage_market_technical_split": coverage_market_technical,
        "valid_bucket_ratio_low_trace": valid_bucket_ratio_low,
        "fast_z_append_read_trace": fast_z_append_read,
        "fast_z_reader_window_short_trace": fast_z_reader_window_short,
        "fast_z_invalid_value_trace": fast_z_invalid_value,
        "cvd_ofi_bucket_freshness_trace": cvd_ofi_freshness,
        "cvd_commit_tail_trace": cvd_commit_tail,
        "adapter_commit_state": adapter_commit_state,
        "bucket_alignment": bucket_alignment,
        "coverage_keys": sorted(coverage.keys()) if isinstance(coverage, dict) else [],
        "subscription_state": subscription_state,
        "health_guard_state": health_guard_state,
        "timeline": timeline,
        "z_window": z_window,
    }


def attribute_micro_not_ready_reason(raw_reason: str, evidence: dict[str, Any]) -> dict[str, Any]:
    missing: list[str] = []
    raw_codes = set(str(x) for x in evidence.get("raw_reason_codes") or [])
    cvd_age = _number(evidence.get("cvd_update_age_sec"))
    ofi_age = _number(evidence.get("ofi_update_age_sec"))
    lag = _number(evidence.get("ofi_cvd_lag_sec"))
    cvd_age_bucket = _number(evidence.get("cvd_age_bucket_sec"))
    ofi_age_bucket = _number(evidence.get("ofi_age_bucket_sec"))
    lag_bucket = _number(evidence.get("ofi_cvd_lag_bucket_sec"))
    agg_cov = _number(evidence.get("aggtrade_coverage_ratio"))
    book_cov = _number(evidence.get("bookticker_coverage_ratio"))
    depth_cov = _number(evidence.get("depth5_coverage_ratio"))
    warmup = _number(evidence.get("warmup_age_sec"))
    required = _number(evidence.get("warmup_required_sec"))
    ws_status = str(evidence.get("ws_status") or "")
    last_ws_age = _number(evidence.get("last_ws_message_age_sec"))
    dropped = evidence.get("dropped_events") if isinstance(evidence.get("dropped_events"), dict) else {}
    metrics = evidence.get("driver_metrics_summary") if isinstance(evidence.get("driver_metrics_summary"), dict) else {}
    adapter_commit = evidence.get("adapter_commit_state") if isinstance(evidence.get("adapter_commit_state"), dict) else {}
    if adapter_commit:
        metrics = {**metrics, **adapter_commit}
    cvd_runtime = evidence.get("cvd_runtime") if isinstance(evidence.get("cvd_runtime"), dict) else {}
    subscription_state = evidence.get("subscription_state") if isinstance(evidence.get("subscription_state"), dict) else {}
    stream_coverage = evidence.get("stream_coverage") if isinstance(evidence.get("stream_coverage"), dict) else {}
    z_window = evidence.get("z_window") if isinstance(evidence.get("z_window"), dict) else {}
    aggtrade_runtime = evidence.get("aggtrade_runtime") if isinstance(evidence.get("aggtrade_runtime"), dict) else {}
    book_depth_runtime = evidence.get("book_depth_runtime") if isinstance(evidence.get("book_depth_runtime"), dict) else {}
    z_history_runtime = evidence.get("z_history_runtime") if isinstance(evidence.get("z_history_runtime"), dict) else {}
    coverage_root_v2 = evidence.get("coverage_root_cause_v2") if isinstance(evidence.get("coverage_root_cause_v2"), dict) else {}
    candidate_dwell = evidence.get("candidate_dwell") if isinstance(evidence.get("candidate_dwell"), dict) else {}
    cvd_commit_trace = evidence.get("cvd_commit_missing_trace") if isinstance(evidence.get("cvd_commit_missing_trace"), dict) else {}
    fast_z_nan_trace = evidence.get("fast_z_nan_trace") if isinstance(evidence.get("fast_z_nan_trace"), dict) else {}
    judgeable_scope = evidence.get("judgeable_scope") if isinstance(evidence.get("judgeable_scope"), dict) else {}
    fast_z_append_read = evidence.get("fast_z_append_read_trace") if isinstance(evidence.get("fast_z_append_read_trace"), dict) else {}
    fast_z_reader_window_short = (
        evidence.get("fast_z_reader_window_short_trace") if isinstance(evidence.get("fast_z_reader_window_short_trace"), dict) else {}
    )
    fast_z_invalid_value = evidence.get("fast_z_invalid_value_trace") if isinstance(evidence.get("fast_z_invalid_value_trace"), dict) else {}
    cvd_ofi_freshness = evidence.get("cvd_ofi_bucket_freshness_trace") if isinstance(evidence.get("cvd_ofi_bucket_freshness_trace"), dict) else {}
    cvd_commit_tail = evidence.get("cvd_commit_tail_trace") if isinstance(evidence.get("cvd_commit_tail_trace"), dict) else {}
    lag_side = str(evidence.get("ofi_cvd_lag_side") or "")

    def stream_state(logical: str) -> dict[str, Any]:
        raw = subscription_state.get(logical)
        return raw if isinstance(raw, dict) else {}

    def stream_cov(logical: str) -> dict[str, Any]:
        raw = stream_coverage.get(logical)
        return raw if isinstance(raw, dict) else {}

    def result(reason: str, category: str, action: str, confidence: str = "medium") -> dict[str, Any]:
        return {
            "raw_reason": raw_reason,
            "attributed_reason": reason,
            "category": category,
            "recommended_action": action,
            "confidence": confidence,
            "missing_evidence_fields": missing,
        }

    if raw_reason == "cvd_stale":
        tail_root = str(cvd_commit_tail.get("root_cause") or "")
        if tail_root in {"cvd_tail_zero_delta_commit_missing", "cvd_tail_no_bucket_close", "cvd_tail_reader_old_frame", "cvd_tail_aggtrade_gap"}:
            return result(tail_root, "technical_fix", "inspect CVD commit tail trace", "high")
        agg_state = stream_state("aggTrade")
        if agg_state.get("required") is True and agg_state.get("active") is False:
            return result("cvd_stale_symbol_not_subscribed", "technical_fix", "restore aggTrade subscription for symbol", "high")
        if ws_status and ws_status not in {"connected", "healthy"}:
            return result("cvd_stale_ws_gap", "technical_fix", "check aggTrade websocket connection", "high")
        if last_ws_age is not None and last_ws_age > 60:
            return result("cvd_stale_ws_gap", "technical_fix", "check websocket receive loop and reconnect", "high")
        if agg_cov is not None and agg_cov < 0.15:
            return result("cvd_stale_no_trade", "market_accept", "symbol had weak aggTrade coverage in the window", "medium")
        if cvd_age is None:
            sc = stream_cov("aggTrade")
            if sc.get("missing_reason"):
                return result("cvd_stale_stream_coverage_incomplete", "technical_fix", "repair aggTrade coverage evidence", "high")
            missing.append("cvd_update_age_sec")
            return result("cvd_stale_unknown_missing_evidence", "unknown_blocker", "add cvd age evidence")
        if dropped.get("trade"):
            return result("cvd_stale_ws_gap", "technical_fix", "inspect dropped trade events", "high")
        if metrics.get("processed_trade_bucket_count", 0) > 0 and metrics.get("cvd_update_count", 0) == 0:
            return result("technical_bug_cvd_adapter_not_updated", "technical_fix", "inspect CVD adapter update path after trade bucket", "high")
        return result("cvd_stale_bucket_not_updated", "technical_fix", "inspect CVD bucket commit path", "medium")

    if raw_reason == "cvd_never_updated":
        tail_root = str(cvd_commit_tail.get("root_cause") or "")
        if tail_root in {"cvd_tail_zero_delta_commit_missing", "cvd_tail_no_bucket_close", "cvd_tail_reader_old_frame", "cvd_tail_aggtrade_gap"}:
            return result(tail_root, "technical_fix", "inspect CVD commit tail trace", "high")
        trace_root = str(cvd_commit_trace.get("root_cause") or "")
        trace_severity = str(cvd_commit_trace.get("severity") or "")
        if trace_root and trace_root not in {"none", "unknown_cvd_commit_missing"}:
            category = "market_accept" if trace_severity == "market" else ("expected_warmup" if trace_severity == "P2" else "technical_fix")
            return result(f"cvd_never_updated_{trace_root}", category, "inspect CVD commit trace root cause", "high")
        runtime_class = str(cvd_runtime.get("never_updated_class") or "")
        bucket_gap_class = str(aggtrade_runtime.get("bucket_gap_class") or "")
        if bucket_gap_class and bucket_gap_class not in {"ok", "low_activity_or_churn"}:
            return result(
                f"cvd_never_updated_{bucket_gap_class}",
                "technical_fix",
                f"inspect aggTrade runtime gap: {bucket_gap_class}",
                "high",
            )
        if runtime_class and runtime_class != "updated":
            category = "market_accept" if runtime_class in {"low_activity_or_churn", "target_evicted_before_warmup"} else "technical_fix"
            action = {
                "subscription_missing": "restore aggTrade subscription for symbol",
                "ws_gap": "check aggTrade websocket connection and reconnect",
                "queue_drop": "inspect dropped aggTrade events",
                "bucket_gap": "inspect aggTrade bucket ingestion for symbol runtime",
                "adapter_commit_failed": "inspect CVD adapter update path after trade bucket",
                "adapter_not_initialized": "inspect CVD adapter initialization",
                "target_evicted_before_warmup": "symbol left target set before warmup completed",
                "low_activity_or_churn": "symbol had no taker trades or churned during runtime",
            }.get(runtime_class, "inspect CVD runtime evidence")
            return result(f"cvd_never_updated_{runtime_class}", category, action, "high")
        agg_state = stream_state("aggTrade")
        if agg_state.get("required") is True and agg_state.get("active") is False:
            return result("cvd_never_updated_symbol_not_subscribed", "technical_fix", "restore aggTrade subscription for symbol", "high")
        if ws_status and ws_status not in {"connected", "healthy"}:
            return result("cvd_never_updated_ws_gap", "technical_fix", "check aggTrade websocket connection", "high")
        if agg_cov is None:
            sc = stream_cov("aggTrade")
            if sc.get("missing_reason"):
                return result("cvd_never_updated_stream_coverage_incomplete", "technical_fix", "repair aggTrade coverage evidence", "high")
            missing.append("aggtrade_coverage_ratio")
            return result("cvd_never_updated_missing_evidence", "unknown_blocker", "add aggTrade coverage evidence")
        if dropped.get("trade"):
            return result("cvd_never_updated_dropped_trade_events", "technical_fix", "inspect dropped aggTrade events", "high")
        if metrics.get("processed_bucket_count", 0) == 0:
            return result("cvd_never_updated_no_processed_bucket", "technical_fix", "inspect bucket ingestion for symbol runtime", "high")
        if agg_cov <= 0:
            return result("cvd_never_updated_no_aggtrade_bucket", "technical_fix", "inspect aggTrade subscription and bucket routing", "high")
        if metrics.get("processed_trade_bucket_count", 0) > 0 and metrics.get("cvd_update_count", 0) == 0:
            return result("technical_bug_cvd_adapter_not_updated", "technical_fix", "inspect CVD adapter update path after trade bucket", "high")
        if metrics.get("cvd_skipped_no_trade", 0) > 0 and metrics.get("cvd_update_count", 0) == 0:
            return result("cvd_never_updated_no_trade_in_bucket", "market_accept", "symbol had no taker trades in processed buckets", "medium")
        return result("cvd_never_updated_bucket_commit_missing", "technical_fix", "inspect CVD adapter update path", "high")

    if raw_reason == "ofi_stale":
        book_state = stream_state("bookTicker")
        depth_state = stream_state("partialDepth5")
        if book_state.get("required") is True and book_state.get("active") is False:
            return result("ofi_stale_symbol_not_subscribed", "technical_fix", "restore bookTicker subscription for symbol", "high")
        if depth_state.get("required") is True and depth_state.get("active") is False:
            return result("ofi_stale_depth_not_subscribed", "technical_fix", "restore partialDepth5 subscription for symbol", "high")
        if ws_status and ws_status not in {"connected", "healthy"}:
            return result("ofi_stale_ws_gap", "technical_fix", "check book/depth websocket connection", "high")
        if book_cov is not None and book_cov < 0.15 and (depth_cov is None or depth_cov < 0.15):
            return result("ofi_stale_low_book_activity", "market_accept", "symbol had weak book/depth coverage in the window", "medium")
        if ofi_age is None:
            sc_book = stream_cov("bookTicker")
            sc_depth = stream_cov("partialDepth5")
            if sc_book.get("missing_reason") or sc_depth.get("missing_reason"):
                return result("ofi_stale_stream_coverage_incomplete", "technical_fix", "repair book/depth coverage evidence", "high")
            missing.append("ofi_update_age_sec")
            return result("ofi_stale_unknown_missing_evidence", "unknown_blocker", "add OFI age evidence")
        if dropped.get("book") or dropped.get("depth"):
            return result("ofi_stale_dropped_book_events", "technical_fix", "inspect dropped book/depth events", "high")
        ofi_gap_class = str(book_depth_runtime.get("ofi_gap_class") or "")
        if ofi_gap_class and ofi_gap_class not in {"ok", "low_activity_or_churn", "coverage_gap"}:
            return result("ofi_stale_" + ofi_gap_class, "technical_fix", f"inspect OFI runtime gap: {ofi_gap_class}", "high")
        if metrics.get("processed_book_bucket_count", 0) > 0 and metrics.get("ofi_update_count", 0) == 0:
            return result("technical_bug_ofi_adapter_not_updated", "technical_fix", "inspect OFI adapter update path after book/depth bucket", "high")
        if metrics.get("ofi_skipped_no_book", 0) > 0 and metrics.get("ofi_update_count", 0) == 0:
            return result("ofi_stale_no_book_in_bucket", "technical_fix", "inspect book bucket commit path", "high")
        return result("ofi_stale_bucket_not_updated", "technical_fix", "inspect OFI bucket commit path", "medium")

    if raw_reason == "ofi_never_updated":
        book_state = stream_state("bookTicker")
        depth_state = stream_state("partialDepth5")
        if book_state.get("required") is True and book_state.get("active") is False:
            return result("ofi_never_updated_bookticker_not_subscribed", "technical_fix", "restore bookTicker subscription for symbol", "high")
        if depth_state.get("required") is True and depth_state.get("active") is False:
            return result("ofi_never_updated_depth_not_subscribed", "technical_fix", "restore partialDepth5 subscription for symbol", "high")
        if book_cov is None and depth_cov is None:
            sc_book = stream_cov("bookTicker")
            sc_depth = stream_cov("partialDepth5")
            if sc_book.get("missing_reason") or sc_depth.get("missing_reason"):
                return result("ofi_never_updated_stream_coverage_incomplete", "technical_fix", "repair book/depth coverage evidence", "high")
            missing.extend(["bookticker_coverage_ratio", "depth5_coverage_ratio"])
            return result("ofi_never_updated_missing_evidence", "unknown_blocker", "add book/depth coverage evidence")
        if dropped.get("book") or dropped.get("depth"):
            return result("ofi_never_updated_dropped_book_events", "technical_fix", "inspect dropped book/depth events", "high")
        ofi_gap_class = str(book_depth_runtime.get("ofi_gap_class") or "")
        if ofi_gap_class and ofi_gap_class not in {"ok", "low_activity_or_churn", "coverage_gap"}:
            return result(
                f"ofi_never_updated_{ofi_gap_class}",
                "technical_fix",
                f"inspect OFI runtime gap: {ofi_gap_class}",
                "high",
            )
        if metrics.get("processed_bucket_count", 0) == 0:
            return result("ofi_never_updated_no_processed_bucket", "technical_fix", "inspect bucket ingestion for symbol runtime", "high")
        if (book_cov is not None and book_cov <= 0) and (depth_cov is None or depth_cov <= 0):
            return result("ofi_never_updated_no_book_bucket", "technical_fix", "inspect bookTicker/depth subscription and bucket routing", "high")
        if metrics.get("processed_book_bucket_count", 0) > 0 and metrics.get("ofi_update_count", 0) == 0:
            return result("technical_bug_ofi_adapter_not_updated", "technical_fix", "inspect OFI adapter update path after book/depth bucket", "high")
        return result("ofi_never_updated_bucket_commit_missing", "technical_fix", "inspect OFI adapter update path", "high")

    if raw_reason == "ofi_cvd_lag_high":
        if lag is None and lag_bucket is None:
            alignment = evidence.get("bucket_alignment") if isinstance(evidence.get("bucket_alignment"), dict) else {}
            if alignment.get("alignment_status") == "missing":
                return result("alignment_missing", "technical_fix", "add CVD/OFI bucket alignment evidence", "high")
            missing.append("ofi_cvd_lag_sec")
            return result("ofi_cvd_lag_unknown_missing_evidence", "unknown_blocker", "add lag evidence")
        if lag_side == "cvd_old":
            return result("ofi_new_cvd_old", "technical_fix", "inspect aggTrade/CVD freshness", "high")
        if lag_side == "ofi_old":
            return result("cvd_new_ofi_old", "technical_fix", "inspect depth/book OFI freshness", "high")
        if cvd_age_bucket is not None and ofi_age_bucket is not None:
            if cvd_age_bucket > ofi_age_bucket:
                return result("ofi_new_cvd_old", "technical_fix", "inspect aggTrade/CVD freshness", "high")
            if ofi_age_bucket > cvd_age_bucket:
                return result("cvd_new_ofi_old", "technical_fix", "inspect depth/book OFI freshness", "high")
            if cvd_age_bucket > 0 and ofi_age_bucket > 0:
                return result("both_old", "technical_fix", "inspect daemon pump or runtime freeze", "high")
        if cvd_age is not None and ofi_age is not None:
            if cvd_age > ofi_age:
                return result("ofi_new_cvd_old", "technical_fix", "inspect aggTrade/CVD freshness", "high")
            if ofi_age > cvd_age:
                return result("cvd_new_ofi_old", "technical_fix", "inspect depth/book OFI freshness", "high")
        if dropped.get("book") or dropped.get("depth"):
            return result("depth_stream_gap", "technical_fix", "inspect depth/book dropped events", "high")
        fresh_root = str(cvd_ofi_freshness.get("stale_root_cause") or "")
        fresh_status = str(cvd_ofi_freshness.get("freshness_status") or "")
        tail_root = str(cvd_commit_tail.get("root_cause") or "")
        if tail_root in {"cvd_tail_zero_delta_commit_missing", "cvd_tail_no_bucket_close", "cvd_tail_reader_old_frame", "cvd_tail_aggtrade_gap"}:
            return result(tail_root, "technical_fix", "inspect CVD commit tail trace", "high")
        if fresh_root:
            return result(f"bucket_freshness_{fresh_root}", "technical_fix", "inspect CVD/OFI bucket freshness trace", "high")
        if fresh_status == "aligned_fresh":
            return result("bucket_alignment_lag_resolved", "market_accept", "bucket freshness was aligned by event-time trace", "medium")
        return result("bucket_alignment_lag", "technical_fix", "align CVD and OFI bucket close timestamps", "medium")

    if raw_reason in {"coverage_aggtrade_weak", "coverage_bookticker_weak", "coverage_depth5_weak"}:
        stream = {
            "coverage_aggtrade_weak": "aggTrade",
            "coverage_bookticker_weak": "bookTicker",
            "coverage_depth5_weak": "partialDepth5",
        }[raw_reason]
        root_v2 = coverage_root_v2.get(stream) if isinstance(coverage_root_v2.get(stream), dict) else {}
        if stream == "partialDepth5" and root_v2.get("required_for_gate") is False:
            return result(
                f"{raw_reason}_{root_v2.get('coverage_class') or 'optional_evidence'}",
                "market_accept",
                "partialDepth5 is optional evidence for micro_fast gate; do not block consumption",
                "high",
            )
        ratio_key = {
            "coverage_aggtrade_weak": "aggtrade_coverage_ratio",
            "coverage_bookticker_weak": "bookticker_coverage_ratio",
            "coverage_depth5_weak": "depth5_coverage_ratio",
        }[raw_reason]
        ratio = _number(evidence.get(ratio_key))
        sc = stream_cov(stream)
        gap_class = str(sc.get("gap_class") or "")
        if stream == "aggTrade":
            runtime_gap_class = str(aggtrade_runtime.get("bucket_gap_class") or "")
            if runtime_gap_class and runtime_gap_class not in {"ok", "low_activity_or_churn"}:
                return result(f"{raw_reason}_{runtime_gap_class}", "technical_fix", f"inspect {stream} runtime path: {runtime_gap_class}", "high")
        else:
            runtime_gap_class = str(book_depth_runtime.get("ofi_gap_class") or "")
            backpressure = str(book_depth_runtime.get("queue_backpressure_state") or "")
            if backpressure in {"warning", "critical"}:
                return result(f"{raw_reason}_queue_backpressure_{backpressure}", "technical_fix", f"repair {stream} queue backpressure", "high")
            if runtime_gap_class and runtime_gap_class not in {"ok", "low_activity_or_churn", "coverage_gap"}:
                return result(f"{raw_reason}_{runtime_gap_class}", "technical_fix", f"inspect {stream} runtime path: {runtime_gap_class}", "high")
        if gap_class and gap_class != "ok":
            category = "market_accept" if gap_class in {"low_activity_or_churn", "ws_no_event"} else "technical_fix"
            return result(f"{raw_reason}_{gap_class}", category, f"inspect {stream} coverage path: {gap_class}", "high")
        if ratio is None:
            if sc.get("missing_reason"):
                return result(f"{raw_reason}_{sc.get('missing_reason')}", "technical_fix", f"repair {stream} stream coverage evidence", "high")
            missing.append(ratio_key)
            return result(f"{raw_reason}_missing_evidence", "unknown_blocker", f"add {stream} coverage evidence")
        if ratio <= 0:
            return result(f"{raw_reason}_no_bucket", "technical_fix", f"inspect {stream} subscription and bucket routing", "high")
        if ws_status and ws_status not in {"connected", "healthy"}:
            return result(f"{raw_reason}_ws_gap", "technical_fix", f"check {stream} websocket connection", "high")
        return result(f"{raw_reason}_low_activity_or_churn", "market_accept", f"{stream} coverage weak in this window", "medium")

    if raw_reason in {"full_z_missing", "fast_z_missing"}:
        z_count = _number(z_window.get("z_window_count"))
        z_required = _number(z_window.get("z_window_required_count"))
        valid_bucket_ratio = _number(z_window.get("valid_bucket_ratio"))
        mode = str(z_window.get("mode") or ("fast" if raw_reason == "fast_z_missing" else "full"))
        z_missing_reason = str(z_window.get("missing_reason") or "")
        history_gap_class = str(z_history_runtime.get("history_gap_class") or "")
        if raw_reason == "fast_z_missing":
            dwell_state = str(candidate_dwell.get("dwell_state") or "")
            if candidate_dwell.get("technical_finding_allowed") is False:
                return result(
                    f"fast_z_missing_{candidate_dwell.get('block_reason') or dwell_state or 'dwell_pending'}",
                    "expected_warmup",
                    "wait for candidate dwell/warmup before judging fast-z continuity",
                    "high",
                )
            nan_reason = str(fast_z_nan_trace.get("reason") or "")
            if nan_reason and nan_reason not in {"ok", z_missing_reason}:
                category = "technical_fix" if str(fast_z_nan_trace.get("severity") or "") == "P0" else "data_incomplete"
                return result(nan_reason, category, "inspect fast-z feature value/state split", "high")
            trace_status = str(fast_z_append_read.get("trace_status") or "")
            if trace_status and trace_status not in {"ready", "unknown"}:
                reader_root = str(fast_z_reader_window_short.get("root_cause") or "")
                invalid_root = str(fast_z_invalid_value.get("root_cause") or "")
                if reader_root and trace_status == "reader_window_short":
                    return result(reader_root, "config_fix", "inspect fast-z reader window short trace", "high")
                if invalid_root and trace_status == "append_skipped_invalid_value":
                    return result(invalid_root, "technical_fix", "inspect fast-z invalid value trace", "high")
                category = "technical_fix" if trace_status in {"append_not_attempted", "append_skipped_no_commit", "append_skipped_invalid_value", "append_success_reader_empty"} else "config_fix"
                return result(f"fast_z_missing_{trace_status}", category, "inspect fast-z append/read trace", "high")
            scope = str(judgeable_scope.get("scope") or "")
            if scope == "not_judgeable_yet":
                return result("fast_z_missing_not_judgeable_yet", "expected_warmup", "wait until symbol enters judgeable scope", "high")
        if history_gap_class and history_gap_class not in {"ok", "warmup_incomplete"}:
            category = {
                "target_churn": "market_accept",
                "coverage_low": "config_fix",
                "series_reset": "technical_fix",
                "adapter_missing": "technical_fix",
                "feature_nan": "technical_fix",
                "insufficient_history": "config_fix",
                "series_not_persisted": "technical_fix",
                "store_read_failed": "technical_fix",
                "valid_bucket_ratio_low": "config_fix",
                "bucket_gap": "technical_fix",
                "cvd_valid_ratio_low": "technical_fix",
                "ofi_valid_ratio_low": "technical_fix",
                "zero_variance": "config_fix",
            }.get(history_gap_class, "unknown_blocker")
            action = {
                "target_churn": "inspect sticky target retention",
                "coverage_low": "inspect bucket fill ratio and stream coverage",
                "series_reset": "repair rolling z-history persistence reset",
                "adapter_missing": "inspect CVD/OFI adapter commit path",
                "feature_nan": "inspect z-score builder output",
                "insufficient_history": "repair z-window history continuity",
                "series_not_persisted": "repair SQLite factor frame ingestion for this symbol",
                "store_read_failed": "repair SQLite factor frame read path",
                "valid_bucket_ratio_low": "repair persisted bucket continuity and target retention",
                "bucket_gap": "repair bucket commit barrier continuity",
                "cvd_valid_ratio_low": "repair CVD factor frame commit path",
                "ofi_valid_ratio_low": "repair OFI factor frame commit path",
                "zero_variance": "inspect flat z-series baseline and minimum variance guard",
            }.get(history_gap_class, "inspect z-history runtime evidence")
            return result(f"{mode}_z_missing_{history_gap_class}", category, action, "high")
        if z_missing_reason:
            category = {
                "warmup_incomplete": "expected_warmup",
                "low_activity": "market_accept",
                "coverage_low": "config_fix",
                "target_churn": "market_accept",
                "series_reset": "technical_fix",
                "adapter_missing": "technical_fix",
                "feature_nan": "technical_fix",
                "insufficient_history": "config_fix",
            }.get(z_missing_reason, "unknown_blocker")
            action = {
                "warmup_incomplete": "wait until required observation window completes",
                "coverage_low": "inspect bucket fill ratio and stream coverage",
                "target_churn": "inspect target retention and candidate churn",
                "series_reset": "inspect rolling series reset reason",
                "adapter_missing": "inspect CVD/OFI adapter commit path",
                "feature_nan": "inspect z-score builder output",
                "insufficient_history": "inspect z-window history continuity",
            }.get(z_missing_reason, "inspect z-window evidence pipeline")
            return result(f"{mode}_z_missing_{z_missing_reason}", category, action, "high")
        if warmup is not None and required is not None and warmup < required:
            return result(f"{mode}_z_missing_warmup_incomplete", "expected_warmup", "wait until required observation window completes", "high")
        if agg_cov is not None and agg_cov < 0.15 and (book_cov is None or book_cov < 0.15) and (depth_cov is None or depth_cov < 0.15):
            return result(f"{mode}_z_missing_low_activity", "market_accept", "symbol had low trade/book activity", "medium")
        if "coverage_aggtrade_weak" in raw_codes or "coverage_bookticker_weak" in raw_codes or "coverage_depth5_weak" in raw_codes:
            return result(f"{mode}_z_missing_valid_bucket_ratio_low", "config_fix", "inspect bucket fill ratio and z-window count", "medium")
        if z_count is not None and z_required is not None and z_count < z_required:
            return result(f"{mode}_z_missing_insufficient_history", "config_fix", "inspect z-window history and target churn reset", "medium")
        if z_count is not None and z_required is not None and z_count >= z_required:
            return result(f"{mode}_z_missing_feature_nan", "technical_fix", "inspect z-score builder output", "high")
        if warmup is None:
            missing.append("warmup_age_sec")
        if z_count is None:
            missing.append("z_window_count")
        if valid_bucket_ratio is None:
            missing.append("valid_bucket_ratio")
        return result(f"{mode}_z_missing_unknown_missing_evidence", "unknown_blocker", "add z-window evidence")

    if raw_reason == "fast_one_z_available_weak_only":
        if "fast_z_missing" in raw_codes:
            return result("fast_one_z_available_due_to_missing_z", "config_fix", "inspect fast z-window count and bucket fill ratio", "medium")
        return result("fast_one_z_available_market_thin", "market_accept", "only one fast z signal had usable evidence", "medium")

    return result(f"{raw_reason}_unclassified", "unknown_blocker", "add reason classifier", "low")


def _raw_target_reasons(codes: list[str]) -> list[str]:
    out: list[str] = []
    for code in codes:
        if code in RAW_REASONS:
            out.append(code)
    return list(dict.fromkeys(out))


def _normalise_selected_micro_lines(selected_lines: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if not selected_lines:
        return MICRO_LINES
    selected = tuple(line for line in selected_lines if line in MICRO_LINES)
    return selected or MICRO_LINES


def _force_payload_lineage(payload: dict[str, Any], *, run_id: str | None, cycle_id: str | None) -> None:
    if run_id:
        original = payload.get("run_id")
        if original not in {None, "", run_id}:
            payload["original_run_id"] = original
        payload["run_id"] = run_id
    if cycle_id:
        original = payload.get("cycle_id")
        if original not in {None, "", cycle_id}:
            payload["original_cycle_id"] = original
        payload["cycle_id"] = cycle_id
    for row in payload.get("symbols") or []:
        if not isinstance(row, dict):
            continue
        if run_id:
            if row.get("run_id") not in {None, "", run_id}:
                row["original_run_id"] = row.get("run_id")
            row["run_id"] = run_id
        if cycle_id:
            if row.get("cycle_id") not in {None, "", cycle_id}:
                row["original_cycle_id"] = row.get("cycle_id")
            row["cycle_id"] = cycle_id


def build_micro_quality_attribution(
    project_root: Path | None = None,
    *,
    lookback_runs: int = 10,
    selected_lines: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    root = _root(project_root)
    features_doc = _read_json(root / "DATA/micro/latest_micro_features.json")
    state_doc = _read_json(root / "DATA/micro/latest_micro_state.json")
    f_index = _feature_index(features_doc)
    active_lines = _normalise_selected_micro_lines(selected_lines)
    run_id = None
    cycle_id = None
    generated_at = to_iso_z(utc_now())
    symbols: list[dict[str, Any]] = []
    by_line: dict[str, Counter[str]] = defaultdict(Counter)
    raw_counts: Counter[str] = Counter()
    attr_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()

    for line in active_lines:
        path = root / f"DATA/micro/latest_micro_lifecycle_{line}.json"
        lifecycle = _read_json(path)
        s_index = _state_symbol_index(state_doc)
        if isinstance(lifecycle, dict):
            run_id = run_id or lifecycle.get("run_id")
            cycle_id = cycle_id or lifecycle.get("cycle_id")
            generated_at = str(lifecycle.get("generated_at") or generated_at)
        for item in _items(lifecycle):
            sym = _symbol(item)
            if not sym:
                continue
            evidence = _collect_evidence(
                lifecycle_item=item,
                line=line,
                feature_item=f_index.get(sym),
                features_doc=features_doc if isinstance(features_doc, dict) else None,
                state_doc=state_doc if isinstance(state_doc, dict) else None,
                state_item=s_index.get(sym),
                project_root=root,
            )
            raw_reasons = _raw_target_reasons(list(evidence.get("raw_reason_codes") or []))
            if not raw_reasons:
                continue
            attributions = [attribute_micro_not_ready_reason(reason, evidence) for reason in raw_reasons]
            for reason in raw_reasons:
                raw_counts[reason] += 1
                by_line[line][reason] += 1
            for attr in attributions:
                attr_counts[attr["attributed_reason"]] += 1
                category_counts[attr["category"]] += 1
            missing_fields = sorted({field for attr in attributions for field in attr.get("missing_evidence_fields") or []})
            symbols.append(
                {
                    "run_id": run_id,
                    "cycle_id": cycle_id,
                    "line": line,
                    "symbol": sym,
                    "state": evidence.get("state"),
                    "raw_reasons": raw_reasons,
                    "attributions": attributions,
                    "missing_evidence_fields": missing_fields,
                    "evidence": evidence,
                }
            )

    run_id = str(run_id or (features_doc.get("run_id") if isinstance(features_doc, dict) else "") or "unknown")
    cycle_id = str(cycle_id or (features_doc.get("cycle_id") if isinstance(features_doc, dict) else "") or "")
    summary = {
        "total_symbols": len({row["symbol"] for row in symbols}),
        "not_ready_symbols": len(symbols),
        "raw_reason_counts": dict(raw_counts),
        "attribution_counts": dict(attr_counts),
        "category_counts": dict(category_counts),
        "technical_fix_count": category_counts.get("technical_fix", 0),
        "config_fix_count": category_counts.get("config_fix", 0),
        "market_accept_count": category_counts.get("market_accept", 0),
        "expected_warmup_count": category_counts.get("expected_warmup", 0),
        "unknown_blocker_count": category_counts.get("unknown_blocker", 0),
    }
    return {
        "schema_version": "10.43",
        "source": "micro_data_quality_attribution",
        "run_id": run_id,
        "cycle_id": cycle_id,
        "generated_at": generated_at,
        "lookback_runs": int(lookback_runs),
        "selected_lines": list(active_lines),
        "summary": summary,
        "lines": {line: {"raw_reason_counts": dict(counter)} for line, counter in by_line.items()},
        "symbols": symbols,
    }


def _markdown_report(payload: dict[str, Any]) -> str:
    lines = [
        "# STEP10.43 Micro CVD / OFI Data Quality Audit",
        "",
        "## Executive Summary",
        "",
        f"- run_id: `{payload.get('run_id')}`",
        f"- cycle_id: `{payload.get('cycle_id')}`",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- symbols with target reasons: `{len(payload.get('symbols') or [])}`",
        "",
        "## Reason Frequency by raw_reason",
        "",
    ]
    for key, value in (payload.get("summary") or {}).get("raw_reason_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Attribution Frequency by attributed_reason", ""])
    for key, value in (payload.get("summary") or {}).get("attribution_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Root Cause Bucket", ""])
    for key, value in (payload.get("summary") or {}).get("category_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Symbol-Level Funnel", ""])
    lines.append("| line | symbol | state | raw reason | attribution | category | action |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for row in payload.get("symbols") or []:
        attrs = row.get("attributions") or []
        attr = attrs[0] if attrs else {}
        lines.append(
            "| {line} | {symbol} | {state} | {raw} | {attr} | {cat} | {action} |".format(
                line=row.get("line") or "",
                symbol=row.get("symbol") or "",
                state=row.get("state") or "",
                raw=", ".join(row.get("raw_reasons") or []),
                attr=attr.get("attributed_reason") or "",
                cat=attr.get("category") or "",
                action=attr.get("recommended_action") or "",
            )
        )
    lines.extend(["", "## Missing Evidence Fields", ""])
    missing_counter: Counter[str] = Counter()
    for row in payload.get("symbols") or []:
        missing_counter.update(row.get("missing_evidence_fields") or [])
    if missing_counter:
        for key, value in missing_counter.items():
            lines.append(f"- `{key}`: {value}")
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def write_micro_quality_attribution(
    project_root: Path | None = None,
    *,
    output_json: Path | None = None,
    output_md: Path | None = None,
    db_path: Path | None = None,
    lookback_runs: int = 10,
    expected_run_id: str | None = None,
    expected_cycle_id: str | None = None,
    selected_lines: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    root = _root(project_root)
    payload = build_micro_quality_attribution(root, lookback_runs=lookback_runs, selected_lines=selected_lines)
    reason_codes: list[str] = []
    if expected_run_id:
        payload["source_pipeline_run_id"] = expected_run_id
        if payload.get("run_id") in {None, "", "unknown"}:
            payload["run_id"] = expected_run_id
            reason_codes.append("micro_quality_current_run_missing")
        elif str(payload.get("run_id")) != str(expected_run_id):
            reason_codes.append("micro_quality_run_id_mismatch")
            _force_payload_lineage(payload, run_id=expected_run_id, cycle_id=None)
    if expected_cycle_id:
        payload["source_pipeline_cycle_id"] = expected_cycle_id
        if payload.get("cycle_id") in {None, ""}:
            payload["cycle_id"] = expected_cycle_id
        elif str(payload.get("cycle_id")) != str(expected_cycle_id):
            reason_codes.append("micro_quality_cycle_id_mismatch")
            _force_payload_lineage(payload, run_id=None, cycle_id=expected_cycle_id)
    if expected_run_id or expected_cycle_id:
        _force_payload_lineage(payload, run_id=expected_run_id, cycle_id=expected_cycle_id)
    if reason_codes:
        payload["status"] = "warning"
        payload["reason_codes"] = reason_codes
    elif not payload.get("symbols"):
        payload["status"] = "ok_empty"
        payload["reason_codes"] = []
    else:
        payload["status"] = "ok"
        payload["reason_codes"] = []
    reports = root / "docs/reports"
    ts = to_iso_z(utc_now()).replace("-", "").replace(":", "").replace("Z", "Z")
    json_path = output_json or reports / f"STEP10.43_micro_quality_findings_{ts}.json"
    md_path = output_md or reports / f"STEP10.43_micro_quality_funnel_{ts}.md"
    payload["report_path"] = str(md_path)
    payload["findings_path"] = str(json_path)
    write_json_atomic(json_path, payload)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(_markdown_report(payload), encoding="utf-8")
    latest_json = root / "DATA/reports/latest_micro_quality_attribution.json"
    latest_md_ref = root / "DATA/reports/latest_micro_quality_attribution_report.txt"
    write_json_atomic(latest_json, payload)
    latest_md_ref.parent.mkdir(parents=True, exist_ok=True)
    latest_md_ref.write_text(str(md_path), encoding="utf-8")
    ingest_micro_quality_attribution_to_sqlite(root, payload=payload, db_path=db_path)
    ingest_micro_evidence_runtime_v2_to_sqlite(root, quality_payload=payload, db_path=db_path)
    return payload


def init_micro_quality_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            create table if not exists micro_quality_attributions (
              run_id text not null,
              cycle_id text,
              target_set_id text,
              strategy_line text,
              symbol text,
              state text,
              raw_reason text,
              attributed_reason text,
              category text,
              recommended_action text,
              evidence_completeness_ratio real,
              missing_evidence_fields_json text,
              evidence_json text,
              generated_at text,
              source_report_path text
            )
            """
        )


P0_RUNTIME_REASONS = {
    "cvd_never_updated",
    "ofi_never_updated",
    "cvd_stale",
    "ofi_stale",
    "ofi_cvd_lag_high",
}
P1_RUNTIME_REASONS = {
    "coverage_aggtrade_weak",
    "coverage_bookticker_weak",
    "coverage_depth5_weak",
    "fast_z_missing",
    "full_z_missing",
    "fast_one_z_available_weak_only",
}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_loads(value: Any, default: Any = None) -> Any:
    if not isinstance(value, str):
        return value if value is not None else default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _severity_for_reasons(raw_reasons: list[str], categories: list[str]) -> str:
    raw_set = set(raw_reasons)
    if raw_set & P0_RUNTIME_REASONS or "technical_fix" in categories:
        return "P0"
    if raw_set & P1_RUNTIME_REASONS or "config_fix" in categories or "unknown_blocker" in categories:
        return "P1"
    if "expected_warmup" in categories:
        return "P2"
    return "market"


def _bucket_status(age: Any, *, stale_sec: float = 60.0) -> str:
    num = _number(age)
    if num is None:
        return "missing"
    if num <= stale_sec:
        return "fresh"
    return "stale"


def _alignment_status(evidence: dict[str, Any]) -> str:
    lag = _number(evidence.get("ofi_cvd_lag_bucket_sec"))
    if lag is None:
        lag = _number(evidence.get("ofi_cvd_lag_sec"))
    if lag is None:
        return "missing"
    if lag <= 30:
        return "aligned"
    if lag <= 90:
        return "lagging"
    return "broken"


def _coverage_value(evidence: dict[str, Any], key: str) -> float | None:
    return _number(evidence.get(key))


def _stream_heartbeat(evidence: dict[str, Any]) -> dict[str, Any]:
    subscription = evidence.get("subscription_state") if isinstance(evidence.get("subscription_state"), dict) else {}
    coverage = evidence.get("stream_coverage") if isinstance(evidence.get("stream_coverage"), dict) else {}

    def stream_entry(logical: str, ratio_key: str) -> dict[str, Any]:
        raw = coverage.get(logical) if isinstance(coverage.get(logical), dict) else {}
        return {
            **raw,
            "coverage_ratio": raw.get("coverage_ratio") if raw else _coverage_value(evidence, ratio_key),
            "subscription": subscription.get(logical) if isinstance(subscription.get(logical), dict) else {},
        }

    return {
        "ws_status": evidence.get("ws_status"),
        "last_ws_message_age_sec": evidence.get("last_ws_message_age_sec"),
        "target_status": evidence.get("target_status"),
        "target_age_sec": evidence.get("target_age_sec"),
        "streams": {
            "aggTrade": stream_entry("aggTrade", "aggtrade_coverage_ratio"),
            "bookTicker": stream_entry("bookTicker", "bookticker_coverage_ratio"),
            "partialDepth5": stream_entry("partialDepth5", "depth5_coverage_ratio"),
        },
        "dropped_events": evidence.get("dropped_events") if isinstance(evidence.get("dropped_events"), dict) else {},
        "health_guard_state": evidence.get("health_guard_state") if isinstance(evidence.get("health_guard_state"), dict) else {},
    }


def _factor_frame(evidence: dict[str, Any]) -> dict[str, Any]:
    alignment = evidence.get("bucket_alignment") if isinstance(evidence.get("bucket_alignment"), dict) else {}
    return {
        "reference_bucket_ts_sec": alignment.get("reference_bucket_ts_sec", evidence.get("reference_bucket_ts_sec")),
        "bucket_closed": alignment.get("bucket_closed"),
        "last_processed_bucket_ts_sec": alignment.get("last_processed_bucket_ts_sec", evidence.get("last_processed_bucket_ts_sec")),
        "last_cvd_update_bucket_ts_sec": alignment.get("last_cvd_update_bucket_ts_sec", evidence.get("last_cvd_update_bucket_ts_sec")),
        "last_ofi_update_bucket_ts_sec": alignment.get("last_ofi_update_bucket_ts_sec", evidence.get("last_ofi_update_bucket_ts_sec")),
        "cvd_age_bucket_sec": alignment.get("cvd_age_bucket_sec", evidence.get("cvd_age_bucket_sec")),
        "ofi_age_bucket_sec": alignment.get("ofi_age_bucket_sec", evidence.get("ofi_age_bucket_sec")),
        "ofi_cvd_lag_bucket_sec": alignment.get("ofi_cvd_lag_bucket_sec", evidence.get("ofi_cvd_lag_bucket_sec")),
        "cvd_status": _bucket_status(evidence.get("cvd_age_bucket_sec") or evidence.get("cvd_update_age_sec")),
        "ofi_status": _bucket_status(evidence.get("ofi_age_bucket_sec") or evidence.get("ofi_update_age_sec")),
        "alignment_status": alignment.get("alignment_status") or _alignment_status(evidence),
        "commit_barrier_status": alignment.get("commit_barrier_status") or alignment.get("alignment_status") or _alignment_status(evidence),
        "true_alignment_reason": alignment.get("true_alignment_reason"),
        "lag_side": alignment.get("lag_side") or evidence.get("ofi_cvd_lag_side"),
        "cvd_commit_state": alignment.get("cvd_commit_state"),
        "ofi_commit_state": alignment.get("ofi_commit_state"),
        "cvd_zero_delta_commit": alignment.get("cvd_zero_delta_commit"),
        "ofi_zero_delta_commit": alignment.get("ofi_zero_delta_commit"),
        "driver_metrics_summary": evidence.get("driver_metrics_summary") if isinstance(evidence.get("driver_metrics_summary"), dict) else {},
        "adapter_commit_state": evidence.get("adapter_commit_state") if isinstance(evidence.get("adapter_commit_state"), dict) else {},
    }


def _z_window_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    z_window = evidence.get("z_window") if isinstance(evidence.get("z_window"), dict) else {}
    return {
        "mode": z_window.get("mode"),
        "z_window_count": z_window.get("z_window_count"),
        "z_window_required_count": z_window.get("z_window_required_count"),
        "valid_bucket_ratio": z_window.get("valid_bucket_ratio"),
        "warmup_age_sec": z_window.get("warmup_age_sec") or evidence.get("warmup_age_sec"),
        "warmup_required_sec": z_window.get("warmup_required_sec") or evidence.get("warmup_required_sec"),
        "target_retention_sec": z_window.get("target_retention_sec"),
        "target_churn_state": z_window.get("target_churn_state"),
        "series_reset_reason": z_window.get("series_reset_reason"),
        "cvd_z_available": z_window.get("cvd_z_available"),
        "ofi_z_available": z_window.get("ofi_z_available"),
        "missing_reason": z_window.get("missing_reason"),
        "store_window": z_window.get("store_window") if isinstance(z_window.get("store_window"), dict) else {},
    }


def _normalise_quality_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in payload.get("symbols") or []:
        if not isinstance(row, dict):
            continue
        line = str(row.get("strategy_line") or row.get("line") or "")
        symbol = str(row.get("symbol") or "").upper()
        if not line or not symbol:
            continue
        key = (line, symbol)
        target = grouped.setdefault(
            key,
            {
                "line": line,
                "strategy_line": line,
                "symbol": symbol,
                "state": row.get("state"),
                "raw_reasons": [],
                "attributions": [],
                "missing_evidence_fields": [],
                "evidence": row.get("evidence") if isinstance(row.get("evidence"), dict) else {},
            },
        )
        raw_reasons = row.get("raw_reasons") if isinstance(row.get("raw_reasons"), list) else []
        if row.get("raw_reason"):
            raw_reasons = [row.get("raw_reason")]
        for reason in raw_reasons:
            if reason and reason not in target["raw_reasons"]:
                target["raw_reasons"].append(str(reason))
        attributions = row.get("attributions") if isinstance(row.get("attributions"), list) else []
        if row.get("attributed_reason"):
            attributions = [
                {
                    "raw_reason": row.get("raw_reason"),
                    "attributed_reason": row.get("attributed_reason"),
                    "category": row.get("category"),
                    "recommended_action": row.get("recommended_action"),
                }
            ]
        for attr in attributions:
            if isinstance(attr, dict):
                marker = (attr.get("raw_reason"), attr.get("attributed_reason"))
                if not any((old.get("raw_reason"), old.get("attributed_reason")) == marker for old in target["attributions"]):
                    target["attributions"].append(attr)
        missing = row.get("missing_evidence_fields") or row.get("missing_evidence_fields_json") or []
        if isinstance(missing, str):
            missing = _json_loads(missing, [])
        for field in missing if isinstance(missing, list) else []:
            if field not in target["missing_evidence_fields"]:
                target["missing_evidence_fields"].append(str(field))
        if not target["evidence"] and isinstance(row.get("evidence_json"), str):
            target["evidence"] = _json_loads(row.get("evidence_json"), {})
    return list(grouped.values())


def build_micro_evidence_runtime_v2(
    project_root: Path | None = None,
    *,
    run_id: str | None = None,
    quality_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = _root(project_root)
    quality = quality_payload or get_micro_quality_attribution(root, run_id=run_id)
    generated_at = to_iso_z(utc_now())
    rows: list[dict[str, Any]] = []
    severity_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    raw_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()
    for row in _normalise_quality_rows(quality):
        evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
        raw_reasons = [str(x) for x in row.get("raw_reasons") or []]
        attributions = [x for x in row.get("attributions") or [] if isinstance(x, dict)]
        categories = [str(x.get("category")) for x in attributions if x.get("category")]
        severity = _severity_for_reasons(raw_reasons, categories)
        frame = _factor_frame(evidence)
        heartbeat = _stream_heartbeat(evidence)
        z_window = _z_window_evidence(evidence)
        status = "blocked"
        if "expected_warmup" in categories:
            status = "warmup"
        elif severity == "market":
            status = "market_not_ready"
        elif severity == "P0":
            status = "technical_blocked"
        elif severity == "P1":
            status = "data_incomplete"
        for reason in raw_reasons:
            raw_counts[reason] += 1
        for category in categories:
            category_counts[category] += 1
        severity_counts[severity] += 1
        status_counts[status] += 1
        rows.append(
            {
                "run_id": quality.get("run_id"),
                "cycle_id": quality.get("cycle_id"),
                "strategy_line": row.get("strategy_line") or row.get("line"),
                "line": row.get("strategy_line") or row.get("line"),
                "symbol": row.get("symbol"),
                "state": row.get("state"),
                "status": status,
                "severity": severity,
                "raw_reasons": raw_reasons,
                "attributed_reasons": [x.get("attributed_reason") for x in attributions if x.get("attributed_reason")],
                "categories": categories,
                "recommended_actions": [x.get("recommended_action") for x in attributions if x.get("recommended_action")],
                "missing_evidence_fields": row.get("missing_evidence_fields") or [],
                "factor_frame": frame,
                "stream_heartbeat": heartbeat,
                "z_window": z_window,
                "runtime_evidence": {
                    "driver_metrics_summary": frame.get("driver_metrics_summary") or {},
                    "adapter_commit_state": frame.get("adapter_commit_state") or {},
                    "cvd_runtime": evidence.get("cvd_runtime") if isinstance(evidence.get("cvd_runtime"), dict) else {},
                    "aggtrade_runtime": evidence.get("aggtrade_runtime") if isinstance(evidence.get("aggtrade_runtime"), dict) else {},
                    "book_depth_runtime": evidence.get("book_depth_runtime") if isinstance(evidence.get("book_depth_runtime"), dict) else {},
                    "z_history_runtime": evidence.get("z_history_runtime") if isinstance(evidence.get("z_history_runtime"), dict) else {},
                    "bucket_commit_barrier": evidence.get("bucket_commit_barrier") if isinstance(evidence.get("bucket_commit_barrier"), dict) else {},
                    "coverage_root_cause_v2": evidence.get("coverage_root_cause_v2") if isinstance(evidence.get("coverage_root_cause_v2"), dict) else {},
                    "fast_z_continuity": evidence.get("fast_z_continuity") if isinstance(evidence.get("fast_z_continuity"), dict) else {},
                    "aligned_frame_gate": evidence.get("aligned_frame_gate") if isinstance(evidence.get("aligned_frame_gate"), dict) else {},
                    "candidate_dwell": evidence.get("candidate_dwell") if isinstance(evidence.get("candidate_dwell"), dict) else {},
                    "cvd_commit_missing_trace": evidence.get("cvd_commit_missing_trace") if isinstance(evidence.get("cvd_commit_missing_trace"), dict) else {},
                    "fast_z_nan_trace": evidence.get("fast_z_nan_trace") if isinstance(evidence.get("fast_z_nan_trace"), dict) else {},
                    "judgeable_scope": evidence.get("judgeable_scope") if isinstance(evidence.get("judgeable_scope"), dict) else {},
                    "judgeable_throughput_trace": (
                        evidence.get("judgeable_throughput_trace")
                        if isinstance(evidence.get("judgeable_throughput_trace"), dict)
                        else {}
                    ),
                    "target_cadence_trace": evidence.get("target_cadence_trace") if isinstance(evidence.get("target_cadence_trace"), dict) else {},
                    "observe_pool_trace": evidence.get("observe_pool_trace") if isinstance(evidence.get("observe_pool_trace"), dict) else {},
                    "coverage_market_technical_split": (
                        evidence.get("coverage_market_technical_split")
                        if isinstance(evidence.get("coverage_market_technical_split"), dict)
                        else {}
                    ),
                    "valid_bucket_ratio_low_trace": (
                        evidence.get("valid_bucket_ratio_low_trace")
                        if isinstance(evidence.get("valid_bucket_ratio_low_trace"), dict)
                        else {}
                    ),
                    "fast_z_append_read_trace": evidence.get("fast_z_append_read_trace") if isinstance(evidence.get("fast_z_append_read_trace"), dict) else {},
                    "fast_z_reader_window_short_trace": (
                        evidence.get("fast_z_reader_window_short_trace")
                        if isinstance(evidence.get("fast_z_reader_window_short_trace"), dict)
                        else {}
                    ),
                    "fast_z_invalid_value_trace": evidence.get("fast_z_invalid_value_trace") if isinstance(evidence.get("fast_z_invalid_value_trace"), dict) else {},
                    "cvd_ofi_bucket_freshness_trace": evidence.get("cvd_ofi_bucket_freshness_trace") if isinstance(evidence.get("cvd_ofi_bucket_freshness_trace"), dict) else {},
                    "cvd_commit_tail_trace": evidence.get("cvd_commit_tail_trace") if isinstance(evidence.get("cvd_commit_tail_trace"), dict) else {},
                    "bucket_alignment": {
                        "reference_bucket_ts_sec": frame.get("reference_bucket_ts_sec"),
                        "bucket_closed": frame.get("bucket_closed"),
                        "last_processed_bucket_ts_sec": frame.get("last_processed_bucket_ts_sec"),
                        "last_cvd_update_bucket_ts_sec": frame.get("last_cvd_update_bucket_ts_sec"),
                        "last_ofi_update_bucket_ts_sec": frame.get("last_ofi_update_bucket_ts_sec"),
                        "cvd_age_bucket_sec": frame.get("cvd_age_bucket_sec"),
                        "ofi_age_bucket_sec": frame.get("ofi_age_bucket_sec"),
                        "ofi_cvd_lag_bucket_sec": frame.get("ofi_cvd_lag_bucket_sec"),
                        "lag_side": frame.get("lag_side"),
                        "alignment_status": frame.get("alignment_status"),
                        "commit_barrier_status": frame.get("commit_barrier_status"),
                        "true_alignment_reason": frame.get("true_alignment_reason"),
                        "cvd_commit_state": frame.get("cvd_commit_state"),
                        "ofi_commit_state": frame.get("ofi_commit_state"),
                        "cvd_zero_delta_commit": frame.get("cvd_zero_delta_commit"),
                        "ofi_zero_delta_commit": frame.get("ofi_zero_delta_commit"),
                    },
                    "coverage": {
                        "aggTrade": heartbeat["streams"]["aggTrade"],
                        "bookTicker": heartbeat["streams"]["bookTicker"],
                        "partialDepth5": heartbeat["streams"]["partialDepth5"],
                    },
                    "ws_status": heartbeat.get("ws_status"),
                    "target_status": heartbeat.get("target_status"),
                    "daemon_status": evidence.get("daemon_status"),
                    "health_state": evidence.get("health_state"),
                },
            }
        )
    run_id_value = str(quality.get("run_id") or run_id or "unknown")
    return {
        "schema_version": "3.17-3.23-v2",
        "source": "micro_evidence_runtime_v2",
        "run_id": run_id_value,
        "cycle_id": quality.get("cycle_id"),
        "generated_at": generated_at,
        "quality_source": quality.get("source"),
        "summary": {
            "symbol_count": len(rows),
            "severity_counts": dict(severity_counts),
            "status_counts": dict(status_counts),
            "raw_reason_counts": dict(raw_counts),
            "category_counts": dict(category_counts),
            "p0_count": severity_counts.get("P0", 0),
            "p1_count": severity_counts.get("P1", 0),
            "p2_count": severity_counts.get("P2", 0),
            "market_count": severity_counts.get("market", 0),
        },
        "symbols": rows,
    }


def init_micro_evidence_runtime_v2_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            create table if not exists micro_evidence_runtime_v2_runs (
              run_id text primary key,
              cycle_id text,
              generated_at text,
              schema_version text,
              summary_json text,
              source text
            )
            """
        )
        conn.execute(
            """
            create table if not exists micro_evidence_runtime_v2_symbols (
              run_id text not null,
              cycle_id text,
              strategy_line text not null,
              symbol text not null,
              state text,
              status text,
              severity text,
              raw_reasons_json text,
              attributed_reasons_json text,
              categories_json text,
              factor_frame_json text,
              stream_heartbeat_json text,
              z_window_json text,
              runtime_evidence_json text,
              recommended_actions_json text,
              missing_evidence_fields_json text,
              generated_at text,
              primary key(run_id, strategy_line, symbol)
            )
            """
        )
        conn.execute(
            "create index if not exists idx_micro_evidence_runtime_v2_symbol on micro_evidence_runtime_v2_symbols(symbol, generated_at)",
        )


def ingest_micro_evidence_runtime_v2_to_sqlite(
    project_root: Path | None = None,
    *,
    quality_payload: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    root = _root(project_root)
    data = payload or build_micro_evidence_runtime_v2(root, quality_payload=quality_payload)
    db = db_path or root / "DATA/audit/run_audit.db"
    init_micro_evidence_runtime_v2_db(db)
    run_id = str(data.get("run_id") or "unknown")
    count = 0
    with sqlite3.connect(db) as conn:
        conn.execute("delete from micro_evidence_runtime_v2_runs where run_id = ?", (run_id,))
        conn.execute("delete from micro_evidence_runtime_v2_symbols where run_id = ?", (run_id,))
        conn.execute(
            """
            insert into micro_evidence_runtime_v2_runs(run_id, cycle_id, generated_at, schema_version, summary_json, source)
            values(?,?,?,?,?,?)
            """,
            (
                run_id,
                data.get("cycle_id"),
                data.get("generated_at"),
                data.get("schema_version"),
                _json_dumps(data.get("summary") or {}),
                data.get("source"),
            ),
        )
        for row in data.get("symbols") or []:
            conn.execute(
                """
                insert into micro_evidence_runtime_v2_symbols(
                  run_id, cycle_id, strategy_line, symbol, state, status, severity,
                  raw_reasons_json, attributed_reasons_json, categories_json,
                  factor_frame_json, stream_heartbeat_json, z_window_json,
                  runtime_evidence_json, recommended_actions_json,
                  missing_evidence_fields_json, generated_at
                ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    data.get("cycle_id"),
                    row.get("strategy_line") or row.get("line"),
                    row.get("symbol"),
                    row.get("state"),
                    row.get("status"),
                    row.get("severity"),
                    _json_dumps(row.get("raw_reasons") or []),
                    _json_dumps(row.get("attributed_reasons") or []),
                    _json_dumps(row.get("categories") or []),
                    _json_dumps(row.get("factor_frame") or {}),
                    _json_dumps(row.get("stream_heartbeat") or {}),
                    _json_dumps(row.get("z_window") or {}),
                    _json_dumps(row.get("runtime_evidence") or {}),
                    _json_dumps(row.get("recommended_actions") or []),
                    _json_dumps(row.get("missing_evidence_fields") or []),
                    data.get("generated_at"),
                ),
            )
            count += 1
    latest_path = root / "DATA/reports/latest_micro_evidence_runtime_v2.json"
    write_json_atomic(latest_path, data)
    training_status: dict[str, Any]
    try:
        from laoma_signal_engine.micro.training_ledger import ingest_runtime_v2_payload

        training_status = ingest_runtime_v2_payload(root, payload=data)
    except Exception as exc:  # pragma: no cover - observability sidecar must not block runtime evidence.
        training_status = {"status": "error", "error": str(exc)}
    return {
        "status": "ok",
        "run_id": run_id,
        "db_path": str(db),
        "row_count": count,
        "latest_path": str(latest_path),
        "micro_training": training_status,
    }


def _runtime_v2_row(row: sqlite3.Row) -> dict[str, Any]:
    out = dict(row)
    for key in (
        "raw_reasons_json",
        "attributed_reasons_json",
        "categories_json",
        "factor_frame_json",
        "stream_heartbeat_json",
        "z_window_json",
        "runtime_evidence_json",
        "recommended_actions_json",
        "missing_evidence_fields_json",
    ):
        if key in out:
            out[key.removesuffix("_json")] = _json_loads(out.get(key), [] if key.endswith("reasons_json") else {})
    out["line"] = out.get("strategy_line")
    return out


def get_micro_evidence_runtime_v2(
    project_root: Path | None = None,
    *,
    run_id: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    root = _root(project_root)
    db = root / "DATA/audit/run_audit.db"
    if db.exists():
        init_micro_evidence_runtime_v2_db(db)
        with sqlite3.connect(db) as conn:
            conn.row_factory = sqlite3.Row
            target_run_id = run_id
            if not target_run_id:
                latest = conn.execute(
                    "select run_id from micro_evidence_runtime_v2_runs order by generated_at desc limit 1",
                ).fetchone()
                target_run_id = latest["run_id"] if latest else None
            if target_run_id:
                run_row = conn.execute(
                    "select * from micro_evidence_runtime_v2_runs where run_id = ?",
                    (target_run_id,),
                ).fetchone()
                params: tuple[Any, ...]
                if symbol:
                    rows = conn.execute(
                        """
                        select * from micro_evidence_runtime_v2_symbols
                        where symbol = ?
                        order by generated_at desc
                        limit ?
                        """,
                        (symbol.upper(), int(limit)),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        select * from micro_evidence_runtime_v2_symbols
                        where run_id = ?
                        order by strategy_line, symbol
                        """,
                        (target_run_id,),
                    ).fetchall()
                symbols = [_runtime_v2_row(row) for row in rows]
                summary = _json_loads(run_row["summary_json"], {}) if run_row else {}
                return {
                    "source": "sqlite",
                    "db_path": str(db),
                    "schema_version": run_row["schema_version"] if run_row else "3.17-3.23-v2",
                    "run_id": target_run_id,
                    "cycle_id": run_row["cycle_id"] if run_row else None,
                    "generated_at": run_row["generated_at"] if run_row else None,
                    "summary": summary,
                    "symbols": symbols,
                }
    latest = _read_json(root / "DATA/reports/latest_micro_evidence_runtime_v2.json")
    if isinstance(latest, dict):
        if run_id and str(latest.get("run_id") or "") != str(run_id):
            return {"source": "missing_current_run", "run_id": run_id, "summary": {}, "symbols": []}
        if symbol:
            latest["symbols"] = [row for row in latest.get("symbols") or [] if str(row.get("symbol") or "").upper() == symbol.upper()]
        latest["source"] = "json_fallback"
        return latest
    return {"source": "missing", "run_id": run_id, "summary": {}, "symbols": []}


def ingest_micro_quality_attribution_to_sqlite(
    project_root: Path | None = None,
    *,
    payload: dict[str, Any] | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    root = _root(project_root)
    data = payload or _read_json(root / "DATA/reports/latest_micro_quality_attribution.json")
    if not isinstance(data, dict):
        raise ValueError("micro quality attribution payload missing")
    run_id = str(data.get("run_id") or "unknown")
    db = db_path or root / "DATA/audit/run_audit.db"
    init_micro_quality_db(db)
    count = 0
    with sqlite3.connect(db) as conn:
        conn.execute("delete from micro_quality_attributions where run_id = ?", (run_id,))
        for row in data.get("symbols") or []:
            evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
            missing = row.get("missing_evidence_fields") or []
            ratio = 1.0 if not missing else max(0.0, 1.0 - len(missing) / 12.0)
            target_set_id = evidence.get("target_set_id")
            for attr in row.get("attributions") or []:
                conn.execute(
                    """
                    insert into micro_quality_attributions(
                      run_id, cycle_id, target_set_id, strategy_line, symbol, state,
                      raw_reason, attributed_reason, category, recommended_action,
                      evidence_completeness_ratio, missing_evidence_fields_json,
                      evidence_json, generated_at, source_report_path
                    ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        run_id,
                        data.get("cycle_id"),
                        target_set_id,
                        row.get("line"),
                        row.get("symbol"),
                        row.get("state"),
                        attr.get("raw_reason"),
                        attr.get("attributed_reason"),
                        attr.get("category"),
                        attr.get("recommended_action"),
                        ratio,
                        json.dumps(missing, ensure_ascii=False),
                        json.dumps(evidence, ensure_ascii=False),
                        data.get("generated_at"),
                        data.get("report_path"),
                    ),
                )
                count += 1
    return {"status": "ok", "run_id": run_id, "db_path": str(db), "row_count": count}


def _row_payload(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    for key in ("missing_evidence_fields_json", "evidence_json"):
        if key in data and isinstance(data[key], str):
            try:
                data[key.removesuffix("_json")] = json.loads(data[key])
            except json.JSONDecodeError:
                pass
    return data


def get_micro_quality_attribution(project_root: Path | None = None, *, run_id: str | None = None) -> dict[str, Any]:
    root = _root(project_root)
    db = root / "DATA/audit/run_audit.db"
    if db.exists():
        init_micro_quality_db(db)
        with sqlite3.connect(db) as conn:
            conn.row_factory = sqlite3.Row
            if run_id is None:
                row = conn.execute(
                    "select run_id from micro_quality_attributions order by generated_at desc limit 1"
                ).fetchone()
                run_id = row["run_id"] if row else None
            if run_id:
                rows = conn.execute(
                    "select * from micro_quality_attributions where run_id = ? order by strategy_line, symbol",
                    (run_id,),
                ).fetchall()
                symbols = [_row_payload(row) for row in rows]
                if symbols:
                    summary = Counter(row.get("category") for row in symbols)
                    raw = Counter(row.get("raw_reason") for row in symbols)
                    attr = Counter(row.get("attributed_reason") for row in symbols)
                    unique_symbols = len({str(row.get("symbol") or "") for row in symbols if row.get("symbol")})
                    return {
                        "source": "sqlite",
                        "db_path": str(db),
                        "run_id": run_id,
                        "cycle_id": symbols[0].get("cycle_id"),
                        "generated_at": symbols[0].get("generated_at"),
                        "summary": {
                            "total_rows": len(symbols),
                            "total_symbols": unique_symbols,
                            "not_ready_symbols": unique_symbols,
                            "category_counts": dict(summary),
                            "raw_reason_counts": dict(raw),
                            "attribution_counts": dict(attr),
                            "technical_fix_count": summary.get("technical_fix", 0),
                            "config_fix_count": summary.get("config_fix", 0),
                            "market_accept_count": summary.get("market_accept", 0),
                            "expected_warmup_count": summary.get("expected_warmup", 0),
                            "unknown_blocker_count": summary.get("unknown_blocker", 0),
                        },
                        "symbols": symbols,
                        "report_path": symbols[0].get("source_report_path"),
                    }
    doc = _read_json(root / "DATA/reports/latest_micro_quality_attribution.json")
    if isinstance(doc, dict):
        if run_id and str(doc.get("run_id") or "") != str(run_id):
            return {
                "source": "missing_current_run",
                "run_id": run_id,
                "summary": {},
                "symbols": [],
                "reason_codes": ["micro_quality_current_run_missing"],
                "stale_latest_run_id": doc.get("run_id"),
                "stale_latest_generated_at": doc.get("generated_at"),
            }
        doc["source"] = "json_fallback"
        return doc
    return {
        "source": "missing_current_run" if run_id else "missing",
        "run_id": run_id,
        "summary": {},
        "symbols": [],
        "reason_codes": ["micro_quality_current_run_missing"] if run_id else [],
    }


def run_write_micro_quality_attribution_safe(
    *,
    project_root: Path | None = None,
    output_json: Path | None = None,
    output_md: Path | None = None,
    db_path: Path | None = None,
    expected_run_id: str | None = None,
    expected_cycle_id: str | None = None,
    selected_lines: list[str] | tuple[str, ...] | None = None,
    non_blocking: bool = False,
    stdout_json: bool = False,
) -> int:
    try:
        payload = write_micro_quality_attribution(
            project_root,
            output_json=output_json,
            output_md=output_md,
            db_path=db_path,
            expected_run_id=expected_run_id,
            expected_cycle_id=expected_cycle_id,
            selected_lines=selected_lines,
        )
        result = {
            "status": payload.get("status") or "ok",
            "run_id": payload.get("run_id"),
            "cycle_id": payload.get("cycle_id"),
            "reason_codes": payload.get("reason_codes") or [],
            "symbol_count": len(payload.get("symbols") or []),
            "findings_path": payload.get("findings_path"),
            "report_path": payload.get("report_path"),
        }
        print(json.dumps(result, ensure_ascii=False) if stdout_json else f"STEP10.43 report written run_id={result['run_id']}")
        return 0
    except Exception as exc:
        result = {"status": "failed", "error": str(exc)}
        print(json.dumps(result, ensure_ascii=False) if stdout_json else f"STEP10.43 report failed: {exc}")
        return 0 if non_blocking else 1
