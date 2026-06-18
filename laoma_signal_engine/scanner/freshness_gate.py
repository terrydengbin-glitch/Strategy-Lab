"""Step 2.1 snapshot age and freshness gate (docs/STEP2.1_任务卡.md)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from laoma_signal_engine.core.time_utils import UTC, parse_iso_z, utc_now

REASON_STALE_SNAPSHOT = "stale_snapshot"

InputFreshness = Literal["fresh", "degraded", "stale"]
TierStatus = Literal["ok", "ok_degraded", "stale_input", "ok_dev_stale_allowed"]


def effective_hard_stale_sec(
    *,
    config_hard: int,
    max_age_sec: int,
    cli_override: int | None,
) -> int:
    """CLI overrides hard line only; clamp so hard >= max_age (STEP2.1)."""
    base = config_hard if cli_override is None else int(cli_override)
    return max(base, int(max_age_sec))


def snapshot_age_sec(generated_at_iso: str, *, now: datetime | None = None) -> int:
    """Seconds since snapshot generated_at (UTC), floored at 0."""
    end = now if now is not None else utc_now()
    start = parse_iso_z(generated_at_iso)
    delta = end - start.astimezone(UTC)
    return max(0, int(delta.total_seconds()))


def classify_input_freshness(
    *,
    age_sec: int,
    max_age_sec: int,
    effective_hard_sec: int,
) -> InputFreshness:
    if age_sec <= max_age_sec:
        return "fresh"
    if age_sec <= effective_hard_sec:
        return "degraded"
    return "stale"


@dataclass(frozen=True)
class FreshnessGateResult:
    scan_allowed: bool
    status: TierStatus
    stale_warning: bool
    input_freshness: InputFreshness
    top_reason_codes: list[str]


def decide_freshness_gate(
    *,
    input_freshness: InputFreshness,
    strict_freshness: bool,
    allow_stale_input: bool,
) -> FreshnessGateResult:
    if input_freshness == "fresh":
        return FreshnessGateResult(
            scan_allowed=True,
            status="ok",
            stale_warning=False,
            input_freshness="fresh",
            top_reason_codes=[],
        )
    if input_freshness == "degraded":
        if strict_freshness:
            return FreshnessGateResult(
                scan_allowed=False,
                status="stale_input",
                stale_warning=False,
                input_freshness="degraded",
                top_reason_codes=[REASON_STALE_SNAPSHOT],
            )
        return FreshnessGateResult(
            scan_allowed=True,
            status="ok_degraded",
            stale_warning=True,
            input_freshness="degraded",
            top_reason_codes=[],
        )
    # stale
    if allow_stale_input:
        return FreshnessGateResult(
            scan_allowed=True,
            status="ok_dev_stale_allowed",
            stale_warning=True,
            input_freshness="stale",
            top_reason_codes=[],
        )
    return FreshnessGateResult(
        scan_allowed=False,
        status="stale_input",
        stale_warning=False,
        input_freshness="stale",
        top_reason_codes=[REASON_STALE_SNAPSHOT],
    )
