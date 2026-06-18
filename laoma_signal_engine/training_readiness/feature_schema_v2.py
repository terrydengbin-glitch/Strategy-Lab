"""STEP29.19 decision-time feature schema v2 validation."""

from __future__ import annotations

from typing import Any

from .label_policy_v2 import decision_time_forbidden_violations


FEATURE_SCHEMA_VERSION = "step29_decision_time_input_v2"

ENTRY_MARKET_FIELDS = (
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
)

ROOT_DECISION_FIELDS = (
    "spread_bps",
    "expected_slippage_bps",
    "expected_fee_bps",
    "market_regime_ref",
)

LINEAGE_REQUIRED_FIELDS = (
    "source_db_path",
    "source_table",
    "feature_timestamp_ms",
    "known_at_ms",
    "source_available_time_ms",
    "source_priority",
)


def _missing(value: Any) -> bool:
    if value is None or value == "":
        return True
    if isinstance(value, dict) and "value" in value:
        return _missing(value.get("value"))
    return False


def _value_at(decision: dict[str, Any], path: str) -> Any:
    value: Any = decision
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    if isinstance(value, dict) and "value" in value:
        return value.get("value")
    return value


def _entry_lineage(decision: dict[str, Any], field: str) -> dict[str, Any]:
    entry = decision.get("entry_market_snapshot") or {}
    if not isinstance(entry, dict):
        return {}
    lineage = entry.get("field_lineage_json") or entry.get("field_lineage") or {}
    if isinstance(lineage, dict) and isinstance(lineage.get(field), dict):
        return dict(lineage[field])
    value = entry.get(field)
    if isinstance(value, dict) and isinstance(value.get("lineage"), dict):
        return dict(value["lineage"])
    return {}


def _root_lineage(decision: dict[str, Any], field: str) -> dict[str, Any]:
    for key in ("field_lineage_json", "field_lineage", "decision_feature_lineage_json", "feature_lineage_json"):
        lineage = decision.get(key) or {}
        if isinstance(lineage, dict) and isinstance(lineage.get(field), dict):
            return dict(lineage[field])
    value = decision.get(field)
    if isinstance(value, dict) and isinstance(value.get("lineage"), dict):
        return dict(value["lineage"])
    return {}


def _lineage_missing(lineage: dict[str, Any]) -> list[str]:
    return [field for field in LINEAGE_REQUIRED_FIELDS if _missing(lineage.get(field))]


def _as_int(value: Any) -> int | None:
    try:
        if _missing(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _lineage_time_violations(lineage: dict[str, Any], decision_time_ms: int | None) -> list[str]:
    out: list[str] = []
    known_at = _as_int(lineage.get("known_at_ms"))
    available = _as_int(lineage.get("source_available_time_ms"))
    feature_ts = _as_int(lineage.get("feature_timestamp_ms"))
    if decision_time_ms is not None and known_at is not None and known_at > decision_time_ms:
        out.append("known_at_after_decision_time")
    if known_at is not None and available is not None and available > known_at:
        out.append("source_available_after_known_at")
    if known_at is not None and feature_ts is not None and feature_ts > known_at:
        out.append("feature_timestamp_after_known_at")
    return out


def validate_decision_time_feature_schema_v2(
    decision_time_input_json: dict[str, Any] | None,
    *,
    decision_time_ms: int | None = None,
) -> dict[str, Any]:
    decision = dict(decision_time_input_json or {})
    if decision_time_ms is None:
        decision_time_ms = _as_int(decision.get("decision_time_ms"))
        entry = decision.get("entry_market_snapshot") or {}
        if decision_time_ms is None and isinstance(entry, dict):
            decision_time_ms = _as_int(entry.get("decision_time_ms"))

    missing_fields: list[str] = []
    missing_lineage: list[str] = []
    time_violations: list[dict[str, str]] = []

    for field in ENTRY_MARKET_FIELDS:
        path = f"entry_market_snapshot.{field}"
        if _missing(_value_at(decision, path)):
            missing_fields.append(path)
            continue
        lineage = _entry_lineage(decision, field)
        for missing in _lineage_missing(lineage):
            missing_lineage.append(f"{path}.{missing}")
        for reason in _lineage_time_violations(lineage, decision_time_ms):
            time_violations.append({"field": path, "reason": reason})

    for field in ROOT_DECISION_FIELDS:
        if _missing(_value_at(decision, field)):
            missing_fields.append(field)
            continue
        lineage = _root_lineage(decision, field)
        for missing in _lineage_missing(lineage):
            missing_lineage.append(f"{field}.{missing}")
        for reason in _lineage_time_violations(lineage, decision_time_ms):
            time_violations.append({"field": field, "reason": reason})

    forbidden = decision_time_forbidden_violations(decision)
    pass_gate = not missing_fields and not missing_lineage and not time_violations and not forbidden
    return {
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "decision_time_feature_schema_v2_pass": pass_gate,
        "missing_fields": sorted(missing_fields),
        "missing_lineage_fields": sorted(missing_lineage),
        "known_at_violations": time_violations,
        "forbidden_decision_time_fields": forbidden,
        "required_field_count": len(ENTRY_MARKET_FIELDS) + len(ROOT_DECISION_FIELDS),
    }
