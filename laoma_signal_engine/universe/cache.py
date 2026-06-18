"""CANDIDATE_UNIVERSE.json cache freshness (Step1 cache rules)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from laoma_signal_engine.core.json_io import read_json_object

PROFILE_SCHEMA_VERSION = "step1.61-business-pool-v1"


def _parse_iso_utc(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def universe_profile_contract_status(data: dict) -> dict:
    """Summarize whether Step1 profile fields are complete enough for downstream use."""
    pairs = data.get("pairs")
    if not isinstance(pairs, list):
        return {
            "status": "incomplete",
            "reason_codes": ["pairs_missing"],
            "counts": {"total_rows": 0, "futures_rows": 0},
        }

    reason_codes: set[str] = set()
    business_pool_counts: dict[str, int] = {}
    counts = {
        "total_rows": len(pairs),
        "futures_rows": 0,
        "missing_universe_profile": 0,
        "missing_risk_profile": 0,
        "business_pool_missing": 0,
        "scan_eligibility_missing": 0,
        "execution_tier_missing": 0,
        "risk_template_missing": 0,
    }
    for row in pairs:
        if not isinstance(row, dict):
            continue
        futures_symbol = str(row.get("futures_symbol") or "").strip()
        has_futures = bool(row.get("has_um_futures") or futures_symbol)
        if not has_futures:
            continue
        counts["futures_rows"] += 1

        universe_profile = row.get("universe_profile")
        if not isinstance(universe_profile, dict):
            counts["missing_universe_profile"] += 1
            reason_codes.add("missing_universe_profile")
            business_pool = "unknown"
        else:
            business_pool = str(universe_profile.get("business_pool") or "unknown")
            scan_eligibility = str(universe_profile.get("scan_eligibility") or "")
            if business_pool == "unknown":
                counts["business_pool_missing"] += 1
                reason_codes.add("business_pool_missing")
            if not scan_eligibility:
                counts["scan_eligibility_missing"] += 1
                reason_codes.add("scan_eligibility_missing")
        business_pool_counts[business_pool] = business_pool_counts.get(business_pool, 0) + 1

        risk_profile = row.get("risk_profile")
        if not isinstance(risk_profile, dict):
            counts["missing_risk_profile"] += 1
            reason_codes.add("missing_risk_profile")
        else:
            execution_tier = str(risk_profile.get("execution_tier") or "unknown")
            sl_template = str(risk_profile.get("sl_template") or "")
            rr_template = str(risk_profile.get("rr_template") or "")
            sizing_template = str(risk_profile.get("sizing_template") or "")
            if execution_tier == "unknown":
                counts["execution_tier_missing"] += 1
                reason_codes.add("execution_tier_missing")
            if not sl_template or not rr_template or not sizing_template:
                counts["risk_template_missing"] += 1
                reason_codes.add("risk_template_missing")

    counts["business_pool"] = business_pool_counts
    status = "ok" if not reason_codes else "incomplete"
    return {"status": status, "reason_codes": sorted(reason_codes), "counts": counts}


def universe_cache_is_fresh(path: Path, expected_schema: str, now: datetime) -> bool:
    """True when file exists, schema/time matches, and Step1 profile contract is usable."""
    if not path.is_file():
        return False
    try:
        data = read_json_object(path)
    except (OSError, TypeError, ValueError, UnicodeDecodeError):
        return False
    if not isinstance(data, dict):
        return False
    if data.get("schema_version") != expected_schema:
        return False
    exp_raw = data.get("expires_at")
    if not isinstance(exp_raw, str):
        return False
    try:
        exp = _parse_iso_utc(exp_raw)
    except ValueError:
        return False
    if exp <= now.astimezone(UTC):
        return False
    return universe_profile_contract_status(data)["status"] == "ok"
