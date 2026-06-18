"""Unit tests for light snapshot pure compute helpers."""

from __future__ import annotations

from laoma_signal_engine.market.kline_fetcher import KlineBar
from laoma_signal_engine.market.light_snapshot_compute import (
    aggregate_last_n_1m,
    atr_mean_tr_on_closed_1m,
    build_reason_codes,
    finalize_structure_state,
    kline_cvd_state,
    mean_quote_volume_closed_15m,
    price_ret_pct,
    volume_ratio_5m_from_1m,
)


def _bar(
    o: float,
    h: float,
    lo: float,
    c: float,
    vol: float,
    qv: float,
    tb: float,
    t_open: int = 0,
) -> KlineBar:
    return KlineBar(
        open_time_ms=t_open,
        open=o,
        high=h,
        low=lo,
        close=c,
        volume=vol,
        close_time_ms=t_open + 60000,
        quote_volume=qv,
        taker_buy_base=tb,
    )


def test_aggregate_last_n_1m() -> None:
    base = []
    for i in range(15):
        base.append(_bar(100 + i, 102 + i, 99 + i, 101 + i, 10, 1000, 6, t_open=i * 60000))
    agg = aggregate_last_n_1m(base, 15)
    assert agg is not None
    assert agg.open == 100.0
    assert agg.close == 101 + 14
    assert agg.volume == 150.0


def test_price_ret_pct() -> None:
    assert price_ret_pct(110.0, 100.0) == 10.0
    assert price_ret_pct(100.0, 0.0) is None


def test_mean_quote_volume_closed_15m() -> None:
    bars = [_bar(1, 2, 1, 2, 1, float(i), 0.5, t_open=i * 900000) for i in range(1, 25)]
    m = mean_quote_volume_closed_15m(bars, 20)
    assert m == sum(range(5, 25)) / 20.0


def test_kline_cvd_state_thresholds() -> None:
    assert kline_cvd_state(100.0, 0.6) == "buy_dominant"
    assert kline_cvd_state(100.0, 0.4) == "sell_dominant"
    assert kline_cvd_state(100.0, 0.5) == "neutral"
    assert kline_cvd_state(0.0, 0.5) == "unavailable"


def test_volume_ratio_5m_from_1m() -> None:
    bars: list[KlineBar] = []
    t = 0
    for i in range(105):
        qv = 1000.0 + float(i % 7)
        bars.append(_bar(100.0, 101.0, 99.0, 100.5, 10.0, qv, 5.0, t_open=t))
        t += 60000
    ratio = volume_ratio_5m_from_1m(bars, 20)
    assert ratio is not None
    assert ratio > 0


def test_atr_mean_tr_on_closed_1m() -> None:
    bars: list[KlineBar] = []
    t = 0
    for i in range(16):
        o = 100.0 + i * 0.1
        bars.append(_bar(o, o + 2.0, o - 1.0, o + 1.0, 1.0, 100.0, 0.5, t_open=t))
        t += 60000
    closed = bars[:-1]
    atr = atr_mean_tr_on_closed_1m(closed, 14)
    assert atr is not None
    assert atr > 0


def test_finalize_structure_state_above_range() -> None:
    st = finalize_structure_state("range", 1.05, 0.5, 2.0, "accelerating_up")
    assert st == "up_impulse"
    st2 = finalize_structure_state("range", 1.05, 0.5, 1.0, "neutral")
    assert st2 == "breakout"


def test_build_reason_codes_includes_tags() -> None:
    r = build_reason_codes(
        price_ret_15m=1.5,
        volume_ratio_15m=2.5,
        kline_cvd_state="buy_dominant",
        acceleration_state="accelerating_up",
        structure_state="up_impulse",
        background_overheat=True,
        diag_tags=[],
    )
    assert "futures_15m_price_up" in r
    assert "futures_15m_volume_expand" in r
    assert "kline_cvd_buy_dominant" in r
    assert "futures_5m_accelerating_up" in r
    assert "structure_up_impulse" in r
    assert "background_overheat" in r
