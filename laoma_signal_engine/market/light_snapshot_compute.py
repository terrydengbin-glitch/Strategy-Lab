"""Pure math for Step 1.5 light snapshot (rolling windows, CVD proxy, ATR)."""

from __future__ import annotations

from dataclasses import dataclass

from laoma_signal_engine.market.kline_fetcher import KlineBar


@dataclass(frozen=True)
class AggregatedWindow:
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    taker_buy_base: float


def aggregate_last_n_1m(bars: list[KlineBar], n: int) -> AggregatedWindow | None:
    if len(bars) < n:
        return None
    w = bars[-n:]
    return AggregatedWindow(
        open=w[0].open,
        high=max(b.high for b in w),
        low=min(b.low for b in w),
        close=w[-1].close,
        volume=sum(b.volume for b in w),
        quote_volume=sum(b.quote_volume for b in w),
        taker_buy_base=sum(b.taker_buy_base for b in w),
    )


def price_ret_pct(last: float, prior: float) -> float | None:
    if prior == 0:
        return None
    return (last - prior) / prior * 100.0


def true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def atr_on_closed_15m(closed_bars: list[KlineBar], period: int) -> float | None:
    """closed_bars: oldest first, all assumed closed."""
    if len(closed_bars) < period + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(closed_bars)):
        trs.append(true_range(closed_bars[i].high, closed_bars[i].low, closed_bars[i - 1].close))
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / float(period)


def mean_quote_volume_closed_15m(closed_15m: list[KlineBar], last_n: int) -> float | None:
    if len(closed_15m) < last_n:
        return None
    chunk = closed_15m[-last_n:]
    return sum(b.quote_volume for b in chunk) / float(last_n)


def taker_sell_volume(volume: float, taker_buy_base: float) -> float:
    return volume - taker_buy_base


def kline_cvd_state(
    volume: float,
    taker_buy_ratio: float | None,
) -> str:
    if volume <= 0 or taker_buy_ratio is None:
        return "unavailable"
    if taker_buy_ratio >= 0.58:
        return "buy_dominant"
    if taker_buy_ratio <= 0.42:
        return "sell_dominant"
    return "neutral"


VOLUME_EXPAND_THRESHOLD = 1.5


def volume_ratio_5m_from_1m(bars: list[KlineBar], closed_windows: int = 20) -> float | None:
    """Rolling last 5x1m quote_volume / mean of prior `closed_windows` complete 5x1m quote volumes."""
    need = 5 + closed_windows * 5
    if len(bars) < need:
        return None
    cur_qv = sum(b.quote_volume for b in bars[-5:])
    prev_sums: list[float] = []
    for w in range(1, closed_windows + 1):
        start = -(w + 1) * 5
        end = -w * 5
        prev_sums.append(sum(b.quote_volume for b in bars[start:end]))
    mean_prev = sum(prev_sums) / float(closed_windows)
    if mean_prev <= 0:
        return None
    return cur_qv / mean_prev


def atr_mean_tr_on_closed_1m(closed_bars: list[KlineBar], period: int) -> float | None:
    """Mean True Range over last `period` TRs on consecutive closed 1m bars (oldest first)."""
    if len(closed_bars) < period + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(closed_bars)):
        trs.append(true_range(closed_bars[i].high, closed_bars[i].low, closed_bars[i - 1].close))
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / float(period)


def range_break_state_from_raw(raw: float | None) -> str:
    if raw is None:
        return "inside"
    if raw > 1.0:
        return "above_range"
    if raw < 0.0:
        return "below_range"
    return "inside"


def clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def finalize_structure_state(
    base: str,
    range_raw: float | None,
    price_ret_15m: float | None,
    volume_ratio: float | None,
    acceleration_state: str,
    *,
    vol_expand_threshold: float = VOLUME_EXPAND_THRESHOLD,
) -> str:
    """Raise structure to breakout/breakdown or impulse when range_raw vs swing is extreme."""
    vol_expand = volume_ratio is not None and volume_ratio >= vol_expand_threshold
    if range_raw is None or price_ret_15m is None:
        return base

    if range_raw > 1.0 and price_ret_15m > 0:
        if vol_expand and acceleration_state == "accelerating_up":
            return "up_impulse"
        if base == "up_impulse":
            return "up_impulse"
        return "breakout"

    if range_raw < 0.0 and price_ret_15m < 0:
        if vol_expand and acceleration_state == "accelerating_down":
            return "down_impulse"
        if base == "down_impulse":
            return "down_impulse"
        return "breakdown"

    return base


def infer_structure_state(price_ret_15m: float | None, cvd_state: str) -> str:
    if price_ret_15m is None:
        return "unknown"
    if price_ret_15m >= 1.0 and cvd_state == "buy_dominant":
        return "up_impulse"
    if price_ret_15m <= -1.0 and cvd_state == "sell_dominant":
        return "down_impulse"
    if abs(price_ret_15m) < 0.25:
        return "range"
    if abs(price_ret_15m) < 0.8:
        return "chop"
    if price_ret_15m > 0.8:
        return "breakout"
    if price_ret_15m < -0.8:
        return "breakdown"
    return "unknown"


def infer_acceleration_state(price_ret_5m: float | None, price_ret_15m: float | None) -> str:
    if price_ret_5m is None or price_ret_15m is None:
        return "neutral"
    expected = price_ret_15m / 3.0
    if price_ret_5m > expected + 0.15 and price_ret_5m > 0.1:
        return "accelerating_up"
    if price_ret_5m < expected - 0.15 and price_ret_5m < -0.1:
        return "accelerating_down"
    if abs(price_ret_5m) < 0.05:
        return "neutral"
    if (price_ret_5m > 0) != (price_ret_15m > 0):
        if price_ret_5m > 0:
            return "slowing_up"
        return "slowing_down"
    if abs(price_ret_5m) < abs(expected) * 0.5:
        if price_ret_15m > 0:
            return "slowing_up"
        if price_ret_15m < 0:
            return "slowing_down"
    return "neutral"


def infer_volatility_state(volume_ratio: float | None) -> str:
    if volume_ratio is not None and volume_ratio > VOLUME_EXPAND_THRESHOLD:
        return "expanded"
    return "normal"


def structure_reason_code(structure_state: str) -> str | None:
    mapping = {
        "breakout": "structure_breakout",
        "breakdown": "structure_breakdown",
        "up_impulse": "structure_up_impulse",
        "down_impulse": "structure_down_impulse",
        "range": "structure_range",
        "chop": "structure_chop",
    }
    return mapping.get(structure_state)


def acceleration_reason_code(acceleration_state: str) -> str:
    if acceleration_state == "accelerating_up":
        return "futures_5m_accelerating_up"
    if acceleration_state == "accelerating_down":
        return "futures_5m_accelerating_down"
    return "futures_5m_neutral"


def build_reason_codes(
    *,
    price_ret_15m: float | None,
    volume_ratio_15m: float | None,
    kline_cvd_state: str,
    acceleration_state: str,
    structure_state: str,
    background_overheat: bool,
    diag_tags: list[str],
) -> list[str]:
    """Ordered unique reason tags for snapshot item (ASCII identifiers)."""
    out: list[str] = []
    seen: set[str] = set()

    def add(tag: str) -> None:
        if tag not in seen:
            seen.add(tag)
            out.append(tag)

    for t in diag_tags:
        add(t)

    if price_ret_15m is not None:
        if price_ret_15m > 0:
            add("futures_15m_price_up")
        elif price_ret_15m < 0:
            add("futures_15m_price_down")

    if volume_ratio_15m is not None:
        if volume_ratio_15m < 1.0:
            add("volume_ratio_below_1")
        elif volume_ratio_15m >= VOLUME_EXPAND_THRESHOLD:
            add("futures_15m_volume_expand")
        else:
            add("futures_15m_volume_normal")

    if kline_cvd_state == "buy_dominant":
        add("kline_cvd_buy_dominant")
    elif kline_cvd_state == "sell_dominant":
        add("kline_cvd_sell_dominant")
    elif kline_cvd_state == "neutral":
        add("kline_cvd_neutral")

    add(acceleration_reason_code(acceleration_state))
    sc = structure_reason_code(structure_state)
    if sc is not None:
        add(sc)

    if background_overheat:
        add("background_overheat")

    return out


def price_ret_1h_from_closed_1h(closed_1h: list[KlineBar]) -> float | None:
    """Last fully closed hour vs previous closed hour (close-over-close)."""
    if len(closed_1h) < 2:
        return None
    c_prev = closed_1h[-2].close
    c_last = closed_1h[-1].close
    return price_ret_pct(c_last, c_prev)


def swing_range_from_closed_15m(closed_15m: list[KlineBar], max_bars: int) -> tuple[float, float] | None:
    if not closed_15m:
        return None
    chunk = closed_15m[-max_bars:]
    hi = max(b.high for b in chunk)
    lo = min(b.low for b in chunk)
    return hi, lo
