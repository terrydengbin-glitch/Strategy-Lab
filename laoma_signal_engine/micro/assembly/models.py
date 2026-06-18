"""STEP3.7 latest_micro_features Pydantic models. docs/STEP3.7_任务卡.md."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

LatestMicroStatus = Literal[
    "ok",
    "no_targets",
    "stale_targets",
    "observing_stale_targets",
    "partial",
    "error",
]
TargetStatus = Literal["fresh", "stale", "stale_observing", "invalid_targets", "unknown"]
OFILevels = Literal[1, 5]
MicroStateMvp = Literal["not_ready"]
MicroAlignmentState = Literal[
    "insufficient",
    "aligned_weak",
    "aligned_strong",
    "mixed",
    "conflict",
    "bullish_divergence",
    "bearish_divergence",
    "buy_absorption",
    "sell_absorption",
    "exhaustion",
    "data_quality_blocked",
]
MicroStrength = Literal["none", "weak", "medium", "strong"]
MicroConfirmationLevel = Literal["none", "hint", "weak", "strong", "conflict"]


class DroppedEventsBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trade: int = Field(ge=0)
    book: int = Field(ge=0)
    depth: int = Field(ge=0)


class CoverageSummaryBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stream_type: str
    window_sec: int
    expected_seconds: int
    covered_seconds: int
    coverage_ratio: float | None = None


class MicroQualityBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ready: bool
    reason_codes: list[str]
    reference_ts_sec: int
    collect_started_ts_sec: int
    warmup_age_sec: int
    cvd_update_age_sec: float | None
    ofi_update_age_sec: float | None
    last_update_age_sec: float | None
    max_lag_sec: float | None
    last_cvd_update_bucket_ts_sec: int | None = None
    last_ofi_update_bucket_ts_sec: int | None = None
    last_processed_bucket_ts_sec: int | None = None
    ofi_cvd_lag_side: str | None = None
    reference_bucket_ts_sec: int | None = None
    cvd_age_bucket_sec: int | None = None
    ofi_age_bucket_sec: int | None = None
    ofi_cvd_lag_bucket_sec: int | None = None
    data_quality_root_cause_class: str = "ok"
    reason_root_causes: dict[str, str] = Field(default_factory=dict)
    coverage: dict[str, CoverageSummaryBlock]
    driver_metrics_summary: dict[str, int]

    @field_validator("reason_codes", mode="before")
    @classmethod
    def _reasons_as_list(cls, v: object) -> object:
        if isinstance(v, tuple):
            return list(v)
        return v


class Micro15mBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ready: bool
    cvd: float | None = None
    z_cvd: float | None = None
    cvd_state: str = "unknown"
    ofi: float | None = None
    z_ofi: float | None = None
    ofi_state: str = "unknown"
    ofi_pressure: str = "unknown"
    fusion_score: float | None = None
    fusion_consistency: str | None = None
    fusion_signal: str | None = None
    fusion_ready: bool = False
    micro_state: MicroStateMvp = "not_ready"


class MicroSignalBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    micro_data_ready: bool = False
    micro_stat_ready: bool = False
    micro_signal_usable: bool = False
    micro_direction_confirmed: bool = False
    micro_exec_allowed: bool = False
    micro_alignment_state: MicroAlignmentState = "insufficient"
    micro_strength: MicroStrength = "none"
    micro_confirmation_level: MicroConfirmationLevel = "none"
    micro_exec_allowed_reason: str = ""
    micro_confidence_score: int = 0
    micro_confirmation_penalty_bps: float = 0.0
    price_response_ok: bool | None = None
    persistence_ok: bool | None = None
    reason_codes: list[str] = Field(default_factory=list)


class MicroFeatureItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    symbol_safe_id: str | None = None
    tier: str | None = None
    source_state: str | None = None
    move_side: str | None = None
    priority: int | None = None
    scan_score: int | None = None
    trigger_type: str | None = None
    ofi_levels: OFILevels
    micro_15m: Micro15mBlock
    micro_quality: MicroQualityBlock
    micro_fast_15m: Micro15mBlock | None = None
    micro_fast_quality: MicroQualityBlock | None = None
    micro_fast_signal: MicroSignalBlock | None = None
    micro_full_15m: Micro15mBlock | None = None
    micro_full_quality: MicroQualityBlock | None = None
    micro_full_signal: MicroSignalBlock | None = None

    @field_validator(
        "symbol_safe_id",
        "tier",
        "source_state",
        "move_side",
        "trigger_type",
        mode="before",
    )
    @classmethod
    def _empty_str_to_none(cls, v: object) -> object:
        if v == "":
            return None
        return v


class LatestMicroFeaturesDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.6"] = "1.6"
    generated_at: str
    source: Literal["micro_feature_assembler"] = "micro_feature_assembler"
    status: LatestMicroStatus
    target_generated_at: str
    target_age_sec: int
    target_status: TargetStatus
    symbol_count: int
    ready_count: int
    not_ready_count: int
    fast_ready_count: int = 0
    full_ready_count: int = 0
    ws_status: str
    last_ws_message_age_sec: int | None = None
    dropped_events: DroppedEventsBlock
    reason_codes: list[str] = Field(default_factory=list)
    items: list[MicroFeatureItem]

    @model_validator(mode="after")
    def _validate_root_counts_and_ready_alignment(self) -> Self:
        n = len(self.items)
        if self.symbol_count != n:
            raise ValueError(f"symbol_count must equal len(items); got {self.symbol_count} vs {n}")
        rc = sum(1 for it in self.items if it.micro_quality.ready)
        nrc = n - rc
        if self.ready_count != rc:
            raise ValueError(f"ready_count must match items; got {self.ready_count} vs {rc}")
        if self.not_ready_count != nrc:
            raise ValueError(
                f"not_ready_count must match items; got {self.not_ready_count} vs {nrc}",
            )
        for idx, it in enumerate(self.items):
            if it.micro_15m.ready is not it.micro_quality.ready:
                raise ValueError(
                    f"item {idx} micro_15m.ready must equal micro_quality.ready",
                )
            if it.micro_full_quality is not None and it.micro_full_15m is not None:
                if it.micro_full_quality.ready is not it.micro_quality.ready:
                    raise ValueError(
                        f"item {idx} micro_full_quality.ready must equal micro_quality.ready",
                    )
                if it.micro_full_15m.ready is not it.micro_full_quality.ready:
                    raise ValueError(
                        f"item {idx} micro_full_15m.ready must equal micro_full_quality.ready",
                    )
            if it.micro_fast_quality is not None and it.micro_fast_15m is not None:
                if it.micro_fast_15m.ready is not it.micro_fast_quality.ready:
                    raise ValueError(
                        f"item {idx} micro_fast_15m.ready must equal micro_fast_quality.ready",
                    )
        has_fast = any(it.micro_fast_quality is not None for it in self.items)
        if has_fast:
            frc = sum(1 for it in self.items if it.micro_fast_quality and it.micro_fast_quality.ready)
            if self.fast_ready_count != frc:
                raise ValueError(
                    f"fast_ready_count must match items; got {self.fast_ready_count} vs {frc}",
                )
        has_full = any(it.micro_full_quality is not None for it in self.items)
        if has_full:
            full_rc = sum(1 for it in self.items if it.micro_full_quality and it.micro_full_quality.ready)
            if self.full_ready_count != full_rc:
                raise ValueError(
                    f"full_ready_count must match items; got {self.full_ready_count} vs {full_rc}",
                )
        return self
