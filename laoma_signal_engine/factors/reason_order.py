"""Stable reason_codes ordering per STEP4.0 task card section 13."""

from __future__ import annotations

# docs/STEP4.0_Decision_Layer_Phase1_任务卡.md section 13
REASON_CODE_ORDER: tuple[str, ...] = (
    "input_stale",
    "primary_15m_not_ready",
    "micro_missing",
    "micro_pipeline_skipped",
    "micro_input_not_fresh",
    "micro_input_invalid",
    "micro_15m_not_ready",
    "micro_conflict",
    "kline_cvd_proxy_only",
    "funding_not_ready",
    "funding_overheated",
    "funding_extreme_negative",
    "basis_not_ready",
    "basis_overheated",
    "basis_overheated_short",
    "oi_not_ready",
    "oi_conflict",
    "range_too_high_wait_pullback",
    "range_too_low_wait_rebound",
    "long_now_confirmed",
    "short_now_confirmed",
    "wait_pullback",
    "wait_rebound",
    "hold_watch",
    "reject_no_direction",
)


def sort_reason_codes(codes: list[str]) -> list[str]:
    idx = {c: i for i, c in enumerate(REASON_CODE_ORDER)}
    unique = list(dict.fromkeys(codes))
    return sorted(unique, key=lambda c: (idx.get(c, 10_000), c))


HARDFAIL_FACTOR_READY_REASONS: frozenset[str] = frozenset(
    {
        "micro_missing",
        "micro_pipeline_skipped",
        "micro_input_not_fresh",
        "micro_input_invalid",
        "micro_15m_not_ready",
        "micro_features_stale",
        "micro_target_anchor_stale",
        "micro_generated_at_invalid",
        "micro_target_generated_at_invalid",
    },
)


def factor_ready_from_reasons(reason_codes: list[str]) -> bool:
    return not (set(reason_codes) & HARDFAIL_FACTOR_READY_REASONS)
