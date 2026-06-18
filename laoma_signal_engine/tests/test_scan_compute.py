"""Unit tests for Step 2 scan_compute (docs/STEP2.0_任务卡.md)."""

from __future__ import annotations

from laoma_signal_engine.scanner.scan_compute import (
    NEUTRAL_PRICE_RET_EPS,
    clamp_scan_score,
    compute_scan_parts,
    compute_market_entry_suitability,
    compute_trade_candidate_rank,
    kline_cvd_score_and_flags,
    meets_raw_candidate,
    meets_strong_candidate,
    meets_watch_candidate,
    move_side_from_price_ret,
    price_score,
    resolve_tier,
    trigger_5m_score_and_flags,
    volume_score,
)


def test_move_side_eps() -> None:
    assert move_side_from_price_ret(NEUTRAL_PRICE_RET_EPS / 2) == "neutral"
    assert move_side_from_price_ret(1.0) == "up"
    assert move_side_from_price_ret(-1.0) == "down"
    assert move_side_from_price_ret(None) == "neutral"


def test_price_volume_score_brackets() -> None:
    assert price_score(4.0) == 30
    assert price_score(2.0) == 22
    assert price_score(1.0) == 15
    assert price_score(0.5) == 8
    assert price_score(0.1) == 0
    assert volume_score(3.0) == 25
    assert volume_score(None) == 0


def test_kline_cvd_conflict() -> None:
    r: list[str] = []
    assert kline_cvd_score_and_flags("up", "sell_dominant", r) == 0
    assert "kline_cvd_conflict" in r
    r2: list[str] = []
    assert kline_cvd_score_and_flags("up", "buy_dominant", r2) == 20


def test_trigger_5m_conflict() -> None:
    r: list[str] = []
    assert trigger_5m_score_and_flags("up", "accelerating_down", r) == 0
    assert "trigger_5m_conflict" in r


def test_compute_scan_universe_missing_adds_reason() -> None:
    r: list[str] = []
    parts = compute_scan_parts(
        price_ret_15m=2.0,
        volume_ratio=2.0,
        move="up",
        kline_cvd_state="buy_dominant",
        acceleration_state="accelerating_up",
        overheat=False,
        rank_futures_volume=10,
        universe_missing=True,
        reason_acc=r,
    )
    assert parts.liquidity_score == 0
    assert "universe_missing" in r
    assert parts.scan_score == clamp_scan_score(
        parts.price_score + parts.volume_score + parts.kline_cvd_score + parts.trigger_5m_score + 0 + parts.background_penalty
    )


def test_meets_strong_overheat_blocks() -> None:
    assert not meets_strong_candidate(
        scan_score=80,
        primary_ready=True,
        volume_ratio=2.5,
        price_ret_15m=1.5,
        kline_cvd_state="buy_dominant",
        acceleration_state="accelerating_up",
        structure_state="range",
        move="up",
        overheat=True,
    )


def test_meets_strong_neutral_cvd_blocks() -> None:
    assert not meets_strong_candidate(
        scan_score=80,
        primary_ready=True,
        volume_ratio=2.5,
        price_ret_15m=1.5,
        kline_cvd_state="neutral",
        acceleration_state="accelerating_up",
        structure_state="breakout",
        move="up",
        overheat=False,
    )


def test_resolve_tier_prefers_strong() -> None:
    assert resolve_tier(strong_ok=True, watch_ok=True, raw_ok=True) == "strong_candidate"
    assert resolve_tier(strong_ok=False, watch_ok=True, raw_ok=True) == "watch_candidate"
    assert resolve_tier(strong_ok=False, watch_ok=False, raw_ok=True) == "raw_candidate"
    assert resolve_tier(strong_ok=False, watch_ok=False, raw_ok=False) is None


def test_meets_watch_requires_thresholds() -> None:
    assert not meets_watch_candidate(scan_score=54, volume_ratio=2.0, price_ret_15m=1.0, overheat=False)
    assert meets_watch_candidate(scan_score=55, volume_ratio=1.3, price_ret_15m=0.8, overheat=False)
    assert not meets_watch_candidate(scan_score=60, volume_ratio=1.3, price_ret_15m=0.8, overheat=True)


def test_meets_raw_or_gate() -> None:
    assert meets_raw_candidate(scan_score=35, volume_ratio=1.0, price_ret_15m=0.1)
    assert meets_raw_candidate(scan_score=10, volume_ratio=1.3, price_ret_15m=0.0)
    assert meets_raw_candidate(scan_score=10, volume_ratio=1.0, price_ret_15m=0.8)
    assert not meets_raw_candidate(scan_score=10, volume_ratio=1.0, price_ret_15m=0.1)


def test_market_entry_suitability_preferred() -> None:
    s = compute_market_entry_suitability(
        scan_score=80,
        move="up",
        price_ret_15m=1.5,
        volume_ratio_15m=2.5,
        acceleration_state="accelerating_up",
        range_pos=0.5,
        liquidity_ok=True,
    )
    assert s.bucket == "preferred"
    assert s.score >= 75
    assert s.reason_codes == ()


def test_market_entry_suitability_avoid_when_chasing_and_no_liquidity() -> None:
    s = compute_market_entry_suitability(
        scan_score=80,
        move="up",
        price_ret_15m=5.0,
        volume_ratio_15m=2.5,
        acceleration_state="accelerating_up",
        range_pos=0.9,
        liquidity_ok=False,
    )
    assert s.bucket == "avoid"
    assert "market_chase_extended" in s.reason_codes
    assert "market_liquidity_not_ok" in s.reason_codes


def test_trade_candidate_rank_allowed_keeps_scan_as_component() -> None:
    r = compute_trade_candidate_rank(
        scan_score=40,
        market_entry_suitability_score=90,
        market_entry_suitability="allowed",
        liquidity_ok=True,
    )
    assert r.bucket == "allowed"
    assert r.score >= 55


def test_trade_candidate_rank_avoid_when_liquidity_bad() -> None:
    r = compute_trade_candidate_rank(
        scan_score=90,
        market_entry_suitability_score=90,
        market_entry_suitability="allowed",
        liquidity_ok=False,
    )
    assert r.bucket == "avoid"
    assert "trade_liquidity_not_ok" in r.reason_codes
