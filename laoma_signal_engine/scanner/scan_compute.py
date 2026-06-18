"""Step 2.0 scoring and tier rules (docs/STEP2.0_任务卡.md)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

NEUTRAL_PRICE_RET_EPS = 0.05
STRONG_STRUCTURE_STATES = frozenset({"up_impulse", "down_impulse", "breakout", "breakdown"})


def move_side_from_price_ret(price_ret_15m: float | None) -> Literal["up", "down", "neutral"]:
    if price_ret_15m is None:
        return "neutral"
    if abs(float(price_ret_15m)) < NEUTRAL_PRICE_RET_EPS:
        return "neutral"
    if price_ret_15m > 0:
        return "up"
    return "down"


def price_score(abs_price_ret: float) -> int:
    if abs_price_ret >= 4.0:
        return 30
    if abs_price_ret >= 2.0:
        return 22
    if abs_price_ret >= 1.0:
        return 15
    if abs_price_ret >= 0.5:
        return 8
    return 0


def volume_score(volume_ratio: float | None) -> int:
    if volume_ratio is None:
        return 0
    vr = float(volume_ratio)
    if vr >= 3.0:
        return 25
    if vr >= 2.0:
        return 20
    if vr >= 1.5:
        return 12
    if vr >= 1.2:
        return 6
    return 0


def liquidity_score_from_rank(rank: int | None) -> int:
    if rank is None:
        return 1
    r = int(rank)
    if r <= 50:
        return 5
    if r <= 150:
        return 4
    if r <= 300:
        return 3
    return 1


def kline_cvd_score_and_flags(
    move: Literal["up", "down", "neutral"],
    kline_cvd_state: str,
    reason_acc: list[str],
) -> int:
    st = kline_cvd_state
    if move == "neutral":
        return 5
    if st == "neutral":
        return 5
    if move == "up" and st == "buy_dominant":
        return 20
    if move == "down" and st == "sell_dominant":
        return 20
    if move == "up" and st == "sell_dominant":
        reason_acc.append("kline_cvd_conflict")
        return 0
    if move == "down" and st == "buy_dominant":
        reason_acc.append("kline_cvd_conflict")
        return 0
    if st == "unavailable":
        return 0
    return 5


def trigger_5m_score_and_flags(
    move: Literal["up", "down", "neutral"],
    acceleration_state: str,
    reason_acc: list[str],
) -> int:
    acc = acceleration_state
    if move == "neutral":
        return 5
    if acc == "neutral":
        return 5
    if move == "up" and acc == "accelerating_up":
        return 15
    if move == "down" and acc == "accelerating_down":
        return 15
    if move == "up" and acc == "accelerating_down":
        reason_acc.append("trigger_5m_conflict")
        return 0
    if move == "down" and acc == "accelerating_up":
        reason_acc.append("trigger_5m_conflict")
        return 0
    return 5


def background_penalty(overheat: bool) -> int:
    return -5 if overheat else 0


def clamp_scan_score(total: int) -> int:
    return max(0, min(100, total))


@dataclass(frozen=True)
class ScanParts:
    price_score: int
    volume_score: int
    kline_cvd_score: int
    trigger_5m_score: int
    liquidity_score: int
    background_penalty: int
    scan_score: int


def compute_scan_parts(
    *,
    price_ret_15m: float | None,
    volume_ratio: float | None,
    move: Literal["up", "down", "neutral"],
    kline_cvd_state: str,
    acceleration_state: str,
    overheat: bool,
    rank_futures_volume: int | None,
    universe_missing: bool,
    reason_acc: list[str],
) -> ScanParts:
    abs_pr = abs(float(price_ret_15m)) if price_ret_15m is not None else 0.0
    ps = price_score(abs_pr)
    vs = volume_score(volume_ratio)
    ks = kline_cvd_score_and_flags(move, kline_cvd_state, reason_acc)
    ts = trigger_5m_score_and_flags(move, acceleration_state, reason_acc)
    if universe_missing:
        liq = 0
        if "universe_missing" not in reason_acc:
            reason_acc.append("universe_missing")
    else:
        liq = liquidity_score_from_rank(rank_futures_volume)
    bp = background_penalty(overheat)
    total = ps + vs + ks + ts + liq + bp
    return ScanParts(
        price_score=ps,
        volume_score=vs,
        kline_cvd_score=ks,
        trigger_5m_score=ts,
        liquidity_score=liq,
        background_penalty=bp,
        scan_score=clamp_scan_score(total),
    )


def derive_trigger_type(
    *,
    scan_score: int,
    price_ret_15m: float | None,
    volume_ratio: float | None,
) -> str:
    pr = float(price_ret_15m) if price_ret_15m is not None else 0.0
    vr = float(volume_ratio) if volume_ratio is not None else 0.0
    abs_pr = abs(pr)
    if vr >= 2.0 and abs_pr >= 1.0:
        return "futures_15m_volume_price_spike"
    if vr >= 2.0:
        return "futures_15m_volume_spike"
    if abs_pr >= 1.0:
        return "futures_15m_price_spike"
    if scan_score >= 55:
        return "futures_15m_momentum"
    return "futures_15m_light"


def five_m_aligned(move: str, acceleration_state: str) -> bool:
    if move == "up" and acceleration_state == "accelerating_up":
        return True
    if move == "down" and acceleration_state == "accelerating_down":
        return True
    return False


def meets_strong_candidate(
    *,
    scan_score: int,
    primary_ready: bool,
    volume_ratio: float | None,
    price_ret_15m: float | None,
    kline_cvd_state: str,
    acceleration_state: str,
    structure_state: str,
    move: str,
    overheat: bool,
) -> bool:
    if scan_score < 75:
        return False
    if not primary_ready:
        return False
    vr = float(volume_ratio) if volume_ratio is not None else 0.0
    if vr < 2.0:
        return False
    pr = float(price_ret_15m) if price_ret_15m is not None else 0.0
    if abs(pr) < 1.0:
        return False
    if kline_cvd_state == "neutral" or kline_cvd_state == "unavailable":
        return False
    if overheat:
        return False
    five_ok = five_m_aligned(move, acceleration_state)
    struct_ok = structure_state in STRONG_STRUCTURE_STATES
    if not (five_ok or struct_ok):
        return False
    return True


def meets_watch_candidate(
    *,
    scan_score: int,
    volume_ratio: float | None,
    price_ret_15m: float | None,
    overheat: bool,
) -> bool:
    if scan_score < 55:
        return False
    vr = float(volume_ratio) if volume_ratio is not None else 0.0
    if vr < 1.3:
        return False
    pr = float(price_ret_15m) if price_ret_15m is not None else 0.0
    if abs(pr) < 0.8:
        return False
    if overheat:
        return False
    return True


def meets_raw_candidate(
    *,
    scan_score: int,
    volume_ratio: float | None,
    price_ret_15m: float | None,
) -> bool:
    vr = float(volume_ratio) if volume_ratio is not None else 0.0
    pr = float(price_ret_15m) if price_ret_15m is not None else 0.0
    if scan_score >= 35:
        return True
    if abs(pr) >= 0.8:
        return True
    if vr >= 1.3:
        return True
    return False


@dataclass(frozen=True)
class MarketEntrySuitability:
    score: int
    bucket: Literal["preferred", "allowed", "avoid", "unknown"]
    reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class TradeCandidateRank:
    score: int
    bucket: Literal["preferred", "allowed", "observe", "avoid", "unknown"]
    reason_codes: tuple[str, ...]


def compute_trade_candidate_rank(
    *,
    scan_score: int,
    market_entry_suitability_score: int,
    market_entry_suitability: str,
    liquidity_ok: bool | None,
    input_freshness: str = "fresh",
    preferred_min_score: int = 75,
    allowed_min_score: int = 55,
    observe_min_score: int = 60,
) -> TradeCandidateRank:
    reasons: list[str] = []
    liquidity_component = 100 if liquidity_ok is True else (45 if liquidity_ok is None else 0)
    freshness_component = 100 if input_freshness == "fresh" else (55 if input_freshness == "degraded" else 0)
    raw_score = (
        float(market_entry_suitability_score) * 0.55
        + float(scan_score) * 0.30
        + float(liquidity_component) * 0.10
        + float(freshness_component) * 0.05
    )
    score = clamp_scan_score(int(round(raw_score)))

    if liquidity_ok is False:
        reasons.append("trade_liquidity_not_ok")
    elif liquidity_ok is None:
        reasons.append("trade_liquidity_unknown")
    if input_freshness != "fresh":
        reasons.append(f"trade_input_{input_freshness}")
    if market_entry_suitability == "avoid":
        reasons.append("trade_market_entry_avoid")
    elif market_entry_suitability == "unknown":
        reasons.append("trade_market_entry_unknown")

    if market_entry_suitability == "preferred" and score >= preferred_min_score and not reasons:
        bucket: Literal["preferred", "allowed", "observe", "avoid", "unknown"] = "preferred"
    elif market_entry_suitability in ("preferred", "allowed") and score >= allowed_min_score and liquidity_ok is not False:
        bucket = "allowed"
    elif score >= observe_min_score and liquidity_ok is not False:
        bucket = "observe"
        if "trade_needs_observation" not in reasons:
            reasons.append("trade_needs_observation")
    elif market_entry_suitability == "unknown":
        bucket = "unknown"
    else:
        bucket = "avoid"

    return TradeCandidateRank(score=score, bucket=bucket, reason_codes=tuple(reasons))


def compute_market_entry_suitability(
    *,
    scan_score: int,
    move: str,
    price_ret_15m: float | None,
    volume_ratio_15m: float | None,
    acceleration_state: str,
    range_pos: float | None,
    liquidity_ok: bool | None = None,
) -> MarketEntrySuitability:
    reasons: list[str] = []
    score = 0
    if move not in ("up", "down"):
        return MarketEntrySuitability(0, "avoid", ("market_no_direction",))
    score += 20 if scan_score >= 75 else (12 if scan_score >= 55 else 5)

    vr = float(volume_ratio_15m) if volume_ratio_15m is not None else 0.0
    if vr >= 2.0:
        score += 15
    elif vr >= 1.3:
        score += 8
    else:
        reasons.append("market_volume_weak")

    pr = abs(float(price_ret_15m)) if price_ret_15m is not None else 0.0
    if 0.8 <= pr <= 3.5:
        score += 20
    elif pr > 3.5:
        score += 5
        reasons.append("market_chase_extended")
    else:
        reasons.append("market_price_move_weak")

    aligned = (move == "up" and acceleration_state == "accelerating_up") or (
        move == "down" and acceleration_state == "accelerating_down"
    )
    if aligned:
        score += 25
    else:
        reasons.append("market_5m_not_aligned")

    if isinstance(range_pos, (int, float)):
        rp = float(range_pos)
        if move == "up":
            if rp < 0.72:
                score += 20
            else:
                reasons.append("market_range_too_high")
        else:
            if rp > 0.28:
                score += 20
            else:
                reasons.append("market_range_too_low")
    else:
        reasons.append("market_range_unknown")

    if liquidity_ok is True:
        score += 20
    elif liquidity_ok is False:
        reasons.append("market_liquidity_not_ok")
    else:
        reasons.append("market_liquidity_unknown")

    score = clamp_scan_score(score)
    if score >= 75 and not reasons:
        bucket: Literal["preferred", "allowed", "avoid", "unknown"] = "preferred"
    elif score >= 55 and "market_liquidity_not_ok" not in reasons:
        bucket = "allowed"
    else:
        bucket = "avoid"
    return MarketEntrySuitability(score, bucket, tuple(reasons))


def resolve_tier(
    *,
    strong_ok: bool,
    watch_ok: bool,
    raw_ok: bool,
) -> Literal["strong_candidate", "watch_candidate", "raw_candidate"] | None:
    if strong_ok:
        return "strong_candidate"
    if watch_ok:
        return "watch_candidate"
    if raw_ok:
        return "raw_candidate"
    return None


def next_stage_for_tier(tier: str) -> str:
    if tier == "strong_candidate":
        return "micro_confirm"
    if tier == "watch_candidate":
        return "warm_pool"
    return "none"


def merge_reason_codes(base: list[str], extra: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in list(base) + list(extra):
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out
