"""Funding context from premiumIndex row (STEP4.1)."""

from __future__ import annotations

import time
from typing import Any

from laoma_signal_engine.context.constants import FUNDING_ABS_NEUTRAL_MAX, FUNDING_ABS_WARM_MAX
from laoma_signal_engine.factors.models import FundingBucket, FundingContextBlock


def _bucket(rate: float) -> FundingBucket:
    if rate > FUNDING_ABS_WARM_MAX:
        return "OVERHEATED"
    if rate < -FUNDING_ABS_WARM_MAX:
        return "NEGATIVE_EXTREME"
    if rate > FUNDING_ABS_NEUTRAL_MAX:
        return "WARM"
    if rate < -FUNDING_ABS_NEUTRAL_MAX:
        return "WARM"
    return "NEUTRAL"


def build_funding_context_from_premium_row(row: dict[str, Any] | None) -> FundingContextBlock:
    if not row:
        return FundingContextBlock(ready=False, reason="missing_premium_row")

    raw_fr = row.get("lastFundingRate")
    if raw_fr is None:
        return FundingContextBlock(ready=False, reason="missing_funding_rate")

    try:
        rate = float(raw_fr)
    except (TypeError, ValueError):
        return FundingContextBlock(ready=False, reason="invalid_funding_rate")

    next_ms = row.get("nextFundingTime")
    hours: float | None = None
    if next_ms is not None:
        try:
            ms = int(next_ms)
            now_ms = int(time.time() * 1000)
            dt = max(0, ms - now_ms)
            hours = round(dt / 3600000.0, 3)
        except (TypeError, ValueError):
            hours = None

    bucket = _bucket(rate)
    extreme = bucket in ("OVERHEATED", "NEGATIVE_EXTREME")

    return FundingContextBlock(
        ready=True,
        reason="ok",
        funding_rate_raw=rate,
        funding_bucket=bucket,
        funding_extreme_flag=extreme,
        hours_to_settlement=hours,
    )
