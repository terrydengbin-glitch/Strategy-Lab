"""Shared micro data-quality contract helpers for downstream gates."""

from __future__ import annotations

from typing import Any

from laoma_signal_engine.micro.data_quality_attribution import (
    RAW_REASONS,
    attribute_micro_not_ready_reason,
)


def _dump(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    return {}


def _coverage_ratio(block: dict[str, Any], key: str) -> float | None:
    coverage = block.get("coverage")
    if not isinstance(coverage, dict):
        return None
    raw = coverage.get(key)
    if not isinstance(raw, dict):
        return None
    if raw.get("coverage_ratio") is not None:
        try:
            return float(raw["coverage_ratio"])
        except (TypeError, ValueError):
            return None
    try:
        expected = float(raw.get("expected_seconds") or 0)
        covered = float(raw.get("covered_seconds") or 0)
    except (TypeError, ValueError):
        return None
    if expected <= 0:
        return None
    return covered / expected


def _coverage_entry(block: dict[str, Any], state: dict[str, Any], key: str) -> dict[str, Any]:
    coverage = block.get("coverage") if isinstance(block.get("coverage"), dict) else {}
    raw = coverage.get(key) if isinstance(coverage.get(key), dict) else {}
    sub = state.get("subscription_state") if isinstance(state.get("subscription_state"), dict) else {}
    sub_raw = sub.get(key) if isinstance(sub.get(key), dict) else {}
    ratio = _coverage_ratio(block, key)
    expected = raw.get("expected_seconds") if raw.get("expected_seconds") is not None else raw.get("expected_bucket_count")
    covered = raw.get("covered_seconds") if raw.get("covered_seconds") is not None else raw.get("covered_bucket_count")
    required = bool(sub_raw.get("required")) if sub_raw else key in coverage
    active = bool(sub_raw.get("active")) if sub_raw else bool(raw)
    missing_reason = None
    if ratio is None:
        if required and not active:
            missing_reason = str(sub_raw.get("missing_reason") or f"subscription_missing_{key}")
        elif required:
            missing_reason = f"coverage_missing_{key}"
        else:
            missing_reason = "not_required"
    elif ratio <= 0:
        missing_reason = f"no_event_{key}"
    return {
        "required": required,
        "active": active,
        "expected_bucket_count": expected,
        "covered_bucket_count": covered,
        "expected_seconds": expected,
        "covered_seconds": covered,
        "coverage_ratio": ratio,
        "last_event_ts_sec": sub_raw.get("last_event_ts_sec"),
        "missing_reason": missing_reason,
    }


def _reason_codes(*blocks: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for block in blocks:
        for code in block.get("reason_codes") or []:
            code_s = str(code)
            if code_s and code_s not in out:
                out.append(code_s)
    return out


def _z_window(*, line: str, quality: dict[str, Any], micro_15m: dict[str, Any]) -> dict[str, Any]:
    z_cvd = micro_15m.get("z_cvd")
    z_ofi = micro_15m.get("z_ofi")
    z_count = int(z_cvd is not None) + int(z_ofi is not None)
    z_required = 1 if line == "micro_fast" else 2
    ratios = [
        ratio
        for ratio in (
            _coverage_ratio(quality, "aggTrade"),
            _coverage_ratio(quality, "bookTicker"),
            _coverage_ratio(quality, "partialDepth5"),
        )
        if ratio is not None
    ]
    warmup = quality.get("warmup_age_sec")
    required = quality.get("min_ready_seconds")
    missing_reason = None
    if z_count < z_required:
        if warmup is not None and required is not None and warmup < required:
            missing_reason = "warmup_incomplete"
        elif ratios and min(ratios) < 0.15:
            missing_reason = "coverage_low"
        elif z_count == 0 and (micro_15m.get("cvd") is not None or micro_15m.get("ofi") is not None):
            missing_reason = "feature_nan"
        else:
            missing_reason = "insufficient_history"
    return {
        "mode": "fast" if line == "micro_fast" else "full" if line == "micro_full" else "generic",
        "z_window_count": z_count,
        "z_window_required_count": z_required,
        "valid_bucket_ratio": min(ratios) if ratios else None,
        "warmup_age_sec": warmup,
        "warmup_required_sec": required,
        "cvd_z_available": z_cvd is not None,
        "ofi_z_available": z_ofi is not None,
        "missing_reason": missing_reason,
    }


def build_micro_data_quality_contract(
    *,
    line: str,
    quality: Any,
    micro_15m: Any = None,
    signal: Any = None,
    state: Any = None,
    features_doc: Any = None,
) -> dict[str, Any]:
    """Return a stable guard/input_refs payload for micro data-quality state.

    The helper is intentionally classification-only. It does not relax or
    tighten trading eligibility; callers keep their existing consumption gates.
    """

    q = _dump(quality)
    m = _dump(micro_15m)
    s = _dump(signal)
    st = _dump(state)
    fd = _dump(features_doc)
    raw_reason_codes = _reason_codes(q, s)
    target_reasons = [code for code in raw_reason_codes if code in RAW_REASONS]
    subscription_state = st.get("subscription_state") if isinstance(st.get("subscription_state"), dict) else {}
    health_guard_state = st.get("health_guard_state") if isinstance(st.get("health_guard_state"), dict) else {}
    driver_metrics = q.get("driver_metrics_summary") if isinstance(q.get("driver_metrics_summary"), dict) else {}
    adapter_commit_state = {
        **driver_metrics,
        "last_cvd_commit_bucket_ts_sec": q.get("last_cvd_update_bucket_ts_sec"),
        "last_ofi_commit_bucket_ts_sec": q.get("last_ofi_update_bucket_ts_sec"),
        "last_processed_bucket_ts_sec": q.get("last_processed_bucket_ts_sec"),
    }
    bucket_alignment = {
        "reference_bucket_ts_sec": q.get("reference_bucket_ts_sec"),
        "last_processed_bucket_ts_sec": q.get("last_processed_bucket_ts_sec"),
        "last_cvd_update_bucket_ts_sec": q.get("last_cvd_update_bucket_ts_sec"),
        "last_ofi_update_bucket_ts_sec": q.get("last_ofi_update_bucket_ts_sec"),
        "cvd_age_bucket_sec": q.get("cvd_age_bucket_sec"),
        "ofi_age_bucket_sec": q.get("ofi_age_bucket_sec"),
        "ofi_cvd_lag_bucket_sec": q.get("ofi_cvd_lag_bucket_sec"),
        "lag_side": q.get("ofi_cvd_lag_side"),
    }
    stream_coverage = {
        key: _coverage_entry(q, st, key)
        for key in ("aggTrade", "bookTicker", "partialDepth5")
    }
    evidence = {
        "raw_reason_codes": raw_reason_codes,
        "cvd_update_age_sec": q.get("cvd_update_age_sec"),
        "ofi_update_age_sec": q.get("ofi_update_age_sec"),
        "ofi_cvd_lag_sec": q.get("max_lag_sec"),
        "ofi_cvd_lag_side": q.get("ofi_cvd_lag_side"),
        "last_cvd_update_bucket_ts_sec": q.get("last_cvd_update_bucket_ts_sec"),
        "last_ofi_update_bucket_ts_sec": q.get("last_ofi_update_bucket_ts_sec"),
        "last_processed_bucket_ts_sec": q.get("last_processed_bucket_ts_sec"),
        "reference_bucket_ts_sec": q.get("reference_bucket_ts_sec"),
        "cvd_age_bucket_sec": q.get("cvd_age_bucket_sec"),
        "ofi_age_bucket_sec": q.get("ofi_age_bucket_sec"),
        "ofi_cvd_lag_bucket_sec": q.get("ofi_cvd_lag_bucket_sec"),
        "warmup_age_sec": q.get("warmup_age_sec"),
        "warmup_required_sec": q.get("min_ready_seconds"),
        "aggtrade_coverage_ratio": _coverage_ratio(q, "aggTrade"),
        "bookticker_coverage_ratio": _coverage_ratio(q, "bookTicker"),
        "depth5_coverage_ratio": _coverage_ratio(q, "partialDepth5"),
        "ws_status": fd.get("ws_status"),
        "last_ws_message_age_sec": fd.get("last_ws_message_age_sec"),
        "dropped_events": fd.get("dropped_events") or {},
        "daemon_status": st.get("daemon_status"),
        "health_state": st.get("health_state"),
        "driver_metrics_summary": driver_metrics,
        "adapter_commit_state": adapter_commit_state,
        "bucket_alignment": bucket_alignment,
        "stream_coverage": stream_coverage,
        "subscription_state": subscription_state,
        "health_guard_state": health_guard_state,
        "timeline": {
            "reference_bucket_ts_sec": q.get("reference_bucket_ts_sec"),
            "last_processed_bucket_ts_sec": q.get("last_processed_bucket_ts_sec"),
            "last_cvd_update_bucket_ts_sec": q.get("last_cvd_update_bucket_ts_sec"),
            "last_ofi_update_bucket_ts_sec": q.get("last_ofi_update_bucket_ts_sec"),
            "cvd_age_bucket_sec": q.get("cvd_age_bucket_sec"),
            "ofi_age_bucket_sec": q.get("ofi_age_bucket_sec"),
            "ofi_cvd_lag_bucket_sec": q.get("ofi_cvd_lag_bucket_sec"),
        },
        "z_window": _z_window(line=line, quality=q, micro_15m=m),
    }
    attributions = [attribute_micro_not_ready_reason(reason, evidence) for reason in target_reasons]
    categories = {str(row.get("category") or "") for row in attributions}
    if not target_reasons:
        state_name = "ok"
        class_name = "ok"
    elif "technical_fix" in categories:
        state_name = "technical_blocked"
        class_name = "technical_fix"
    elif "expected_warmup" in categories:
        state_name = "config_warmup_incomplete"
        class_name = "expected_warmup"
    elif "market_accept" in categories:
        state_name = "market_accept_low_activity"
        class_name = "market_accept"
    elif "config_fix" in categories:
        state_name = "config_warmup_incomplete"
        class_name = "config_fix"
    else:
        state_name = "unknown"
        class_name = "unknown_blocker"
    return {
        "micro_data_quality_state": state_name,
        "micro_data_quality_class": class_name,
        "micro_data_quality_reasons": raw_reason_codes,
        "micro_data_quality_target_reasons": target_reasons,
        "micro_data_quality_attributions": attributions,
        "micro_data_quality_evidence": evidence,
    }
