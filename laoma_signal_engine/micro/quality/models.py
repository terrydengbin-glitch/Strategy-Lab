"""STEP3.6 micro quality models. docs/STEP3.6_任务卡.md."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

OFILevels = Literal[1, 5]

REASON_ORDER: tuple[str, ...] = (
    "event_queue_overflow",
    "adapter_error_seen",
    "coverage_aggtrade_weak",
    "coverage_bookticker_weak",
    "coverage_depth5_weak",
    "cvd_never_updated",
    "ofi_never_updated",
    "cvd_stale",
    "ofi_stale",
    "ofi_cvd_lag_high",
    "ofi_backpressure_degraded",
    "ofi_backpressure_critical",
    "warmup_not_met",
)

LagSide = Literal["aligned", "cvd_old", "ofi_old"]


def sort_reason_codes(reasons: set[str]) -> tuple[str, ...]:
    return tuple(code for code in REASON_ORDER if code in reasons)


@dataclass(frozen=True)
class MicroQualityConfig:
    window_sec: int = 900
    min_ready_seconds: int = 900

    aggtrade_coverage_min: float = 0.60
    bookticker_coverage_min: float = 0.70
    depth5_coverage_min: float = 0.70

    max_stale_sec: int = 10
    max_lag_sec: int = 3

    event_queue_overflow_hard_fail: bool = True
    adapter_error_hard_fail: bool = True
    ofi_backpressure_degraded_hard_fail: bool = False


@dataclass(frozen=True)
class WSQualitySignal:
    event_queue_overflow_recent: bool = False
    dropped_trade_delta: int = 0
    dropped_book_delta: int = 0
    dropped_depth_delta: int = 0
    ofi_backpressure_state: Literal["ok", "degraded", "critical"] = "ok"


@dataclass(frozen=True)
class CoverageSummary:
    stream_type: str
    window_sec: int
    expected_seconds: int
    covered_seconds: int

    @property
    def coverage_ratio(self) -> float:
        denom = max(self.expected_seconds, 1)
        return float(self.covered_seconds) / float(denom)


@dataclass(frozen=True)
class SymbolQualityInput:
    symbol: str
    ofi_levels: OFILevels
    collect_started_ts_sec: int
    min_ready_seconds: int | None = None


@dataclass(frozen=True)
class MicroQualitySnapshot:
    symbol: str
    ready: bool
    reason_codes: tuple[str, ...]

    reference_ts_sec: int
    collect_started_ts_sec: int
    warmup_age_sec: int

    cvd_update_age_sec: float | None
    ofi_update_age_sec: float | None
    last_update_age_sec: float | None
    max_lag_sec: float | None

    coverage: dict[str, CoverageSummary]
    driver_metrics_summary: dict[str, int]

    last_cvd_update_bucket_ts_sec: int | None = None
    last_ofi_update_bucket_ts_sec: int | None = None
    last_processed_bucket_ts_sec: int | None = None
    ofi_cvd_lag_side: LagSide | None = None
    reference_bucket_ts_sec: int | None = None
    cvd_age_bucket_sec: int | None = None
    ofi_age_bucket_sec: int | None = None
    ofi_cvd_lag_bucket_sec: int | None = None
    data_quality_root_cause_class: str = "ok"
    reason_root_causes: dict[str, str] | None = None
