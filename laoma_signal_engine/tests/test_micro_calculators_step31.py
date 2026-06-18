"""STEP3.1: calculators, Binance normalizers, adapters. docs/STEP3.1_任务卡.md."""

from __future__ import annotations

import pytest

from laoma_signal_engine.micro.adapters import (
    CVDAdapter,
    FusionAdapter,
    OFIAdapter,
    normalize_agg_trade,
    normalize_binance_symbol,
    normalize_book_ticker,
    normalize_partial_depth5,
)
from laoma_signal_engine.micro.calculators.cvd import CVDParams
from laoma_signal_engine.micro.calculators.fusion import FusionParams
from laoma_signal_engine.micro.calculators.ofi import OFIParams
from laoma_signal_engine.micro.normalized_models import NormalizedBook, NormalizedTrade


def test_normalize_binance_symbol_strip_upper() -> None:
    assert normalize_binance_symbol("  btcusdt  ") == "BTCUSDT"


def test_normalize_binance_symbol_empty_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        normalize_binance_symbol("   ")


def test_agg_trade_side_m_mapping() -> None:
    base = {"s": "ETHUSDT", "p": "1.0", "q": "2.0", "T": 100, "E": 99}
    nt = normalize_agg_trade({**base, "m": True})
    assert nt.side == "sell"
    nt2 = normalize_agg_trade({**base, "m": False})
    assert nt2.side == "buy"


def test_agg_trade_ts_prefers_t_over_e() -> None:
    ev = {"s": "ETHUSDT", "p": "1", "q": "1", "T": 500, "E": 400, "m": False}
    assert normalize_agg_trade(ev).ts_ms == 500


def test_agg_trade_ts_fallback_e() -> None:
    ev = {"s": "ETHUSDT", "p": "1", "q": "1", "E": 400, "m": False}
    assert normalize_agg_trade(ev).ts_ms == 400


def test_agg_trade_missing_ts_raises() -> None:
    ev = {"s": "ETHUSDT", "p": "1", "q": "1", "m": False}
    with pytest.raises(ValueError, match="both T and E"):
        normalize_agg_trade(ev)


def test_agg_trade_missing_m_raises() -> None:
    ev = {"s": "ETHUSDT", "p": "1", "q": "1", "T": 1, "E": 1}
    with pytest.raises(ValueError, match="missing m"):
        normalize_agg_trade(ev)


def test_agg_trade_m_non_bool_raises() -> None:
    ev = {"s": "ETHUSDT", "p": "1", "q": "1", "T": 1, "m": 1}
    with pytest.raises(ValueError, match="bool"):
        normalize_agg_trade(ev)


def test_book_ticker_levels_one() -> None:
    ev = {
        "s": "BTCUSDT",
        "E": 1000,
        "b": "100",
        "B": "1.5",
        "a": "101",
        "A": "2.0",
    }
    nb = normalize_book_ticker(ev)
    assert nb.levels == 1
    assert nb.bids == [(100.0, 1.5)]
    assert nb.asks == [(101.0, 2.0)]


def test_book_depth_five_sorts_and_trims() -> None:
    ev = {
        "s": "BTCUSDT",
        "E": 2000,
        "b": [
            ["99", "1"],
            ["100", "2"],
        ]
        + [["98", "3"], ["97", "4"], ["96", "5"]],
        "a": [
            ["102", "1"],
            ["101", "2"],
        ]
        + [["103", "3"], ["104", "4"], ["105", "5"]],
    }
    nb = normalize_partial_depth5(ev)
    assert nb.levels == 5
    assert nb.bids[0][0] == 100.0
    assert nb.bids == sorted(nb.bids, key=lambda x: x[0], reverse=True)
    assert nb.asks == sorted(nb.asks, key=lambda x: x[0])
    assert len(nb.bids) == 5
    assert len(nb.asks) == 5


def test_book_depth_symbol_if_missing() -> None:
    ev = {
        "E": 1,
        "b": [["1", "1"]] * 5,
        "a": [["2", "1"]] * 5,
    }
    nb = normalize_partial_depth5(ev, symbol_if_missing="xxusdt")
    assert nb.symbol == "XXUSDT"


def test_book_depth_insufficient_raises() -> None:
    ev = {
        "s": "BTCUSDT",
        "E": 1,
        "b": [["1", "1"]] * 3,
        "a": [["2", "1"]] * 5,
    }
    with pytest.raises(ValueError):
        normalize_partial_depth5(ev)


def test_cvd_adapter_symbol_mismatch_raises() -> None:
    ad = CVDAdapter("BTCUSDT", CVDParams(z_window=10, warmup_min=0.0))
    nt = NormalizedTrade(
        symbol="ETHUSDT", ts_ms=1, price=1.0, qty=1.0, side="buy"
    )
    with pytest.raises(ValueError, match="symbol"):
        ad.update_trade(nt)


def test_cvd_adapter_updates() -> None:
    ad = CVDAdapter("BTCUSDT", CVDParams(z_window=10, warmup_min=0.0))
    for i in range(15):
        nt = NormalizedTrade(
            symbol="BTCUSDT",
            ts_ms=1000 + i,
            price=100.0,
            qty=1.0,
            side="buy" if i % 2 == 0 else "sell",
        )
        out = ad.update_trade(nt)
    assert "cvd" in out
    assert out["symbol"] == "BTCUSDT"


def test_cvd_adapter_directional_buy_up_sell_down_t1() -> None:
    """T1: consecutive buys increase cvd; consecutive sells decrease cvd (directional)."""
    params = CVDParams(z_window=50, warmup_min=0.0)
    buy_ad = CVDAdapter("BTCUSDT", params)
    prev_cvd = 0.0
    for i in range(5):
        out = buy_ad.update_trade(
            NormalizedTrade(
                symbol="BTCUSDT",
                ts_ms=10_000 + i,
                price=100.0,
                qty=2.0,
                side="buy",
            )
        )
        assert out["cvd"] > prev_cvd
        prev_cvd = out["cvd"]

    sell_ad = CVDAdapter("BTCUSDT", CVDParams(z_window=50, warmup_min=0.0))
    prev_cvd = 0.0
    for i in range(5):
        out = sell_ad.update_trade(
            NormalizedTrade(
                symbol="BTCUSDT",
                ts_ms=20_000 + i,
                price=100.0,
                qty=2.0,
                side="sell",
            )
        )
        assert out["cvd"] < prev_cvd
        prev_cvd = out["cvd"]


def test_normalize_payload_invalid_or_missing_t9() -> None:
    """T9: missing required keys or non-float book/agg fields -> ValueError."""
    base_bt = {
        "s": "BTCUSDT",
        "E": 1000,
        "b": "100",
        "B": "1",
        "a": "101",
        "A": "1",
    }
    for missing in ("b", "B", "a", "A"):
        ev = {k: v for k, v in base_bt.items() if k != missing}
        with pytest.raises(ValueError, match="bookTicker missing"):
            normalize_book_ticker(ev)

    bad_bt = dict(base_bt)
    bad_bt["b"] = "not_a_float"
    with pytest.raises(ValueError, match="float-convertible"):
        normalize_book_ticker(bad_bt)

    base_dp = {
        "s": "BTCUSDT",
        "E": 1,
        "b": [["1", "1"]] * 5,
        "a": [["2", "1"]] * 5,
    }
    for key in ("b", "a"):
        ev = {k: v for k, v in base_dp.items() if k != key}
        with pytest.raises(ValueError):
            normalize_partial_depth5(ev)

    bad_dp = dict(base_dp)
    bad_dp["b"] = [["not_a_float", "1"]] + [["1", "1"]] * 4
    with pytest.raises(ValueError, match="float-convertible"):
        normalize_partial_depth5(bad_dp)

    agg_bad_price = {
        "s": "ETHUSDT",
        "p": "not_a_number",
        "q": "1",
        "T": 1,
        "m": False,
    }
    with pytest.raises(ValueError, match="float-convertible"):
        normalize_agg_trade(agg_bad_price)


def test_ofi_adapter_levels_one() -> None:
    ad = OFIAdapter("BTCUSDT", OFIParams(levels=1, z_window=5))
    out = None
    for i in range(12):
        nb = NormalizedBook(
            symbol="BTCUSDT",
            ts_ms=1000 + i * 100,
            bids=[(100.0 - i * 0.01, 1.0)],
            asks=[(100.5 + i * 0.01, 1.0)],
            levels=1,
        )
        out = ad.update_book(nb)
    assert out is not None
    assert "ofi" in out
    assert out["symbol"] == "BTCUSDT"


def test_ofi_adapter_levels_five() -> None:
    raw = {
        "s": "BTCUSDT",
        "E": 2000,
        "b": [["100", "1"], ["99", "2"], ["98", "3"], ["97", "4"], ["96", "5"]],
        "a": [["101", "1"], ["102", "2"], ["103", "3"], ["104", "4"], ["105", "5"]],
    }
    nb = normalize_partial_depth5(raw)
    ad = OFIAdapter("BTCUSDT", OFIParams(levels=5, z_window=5))
    out = ad.update_book(nb)
    assert "ofi" in out


def test_ofi_adapter_levels_mismatch_raises() -> None:
    ad = OFIAdapter("BTCUSDT", OFIParams(levels=5, z_window=5))
    nb = NormalizedBook(
        symbol="BTCUSDT",
        ts_ms=1,
        bids=[(1.0, 1.0)],
        asks=[(2.0, 1.0)],
        levels=1,
    )
    with pytest.raises(ValueError, match="levels"):
        ad.update_book(nb)


def test_fusion_adapter_smoke() -> None:
    fa = FusionAdapter(FusionParams())
    out = fa.fuse(1_000_000, 1.2, -0.8, lag_sec=0.05)
    assert set(out.keys()) >= {"score", "consistency", "signal", "warmup", "components"}


def test_import_calculators_package() -> None:
    import laoma_signal_engine.micro.calculators as calc

    assert hasattr(calc, "CVDEngine")
    assert hasattr(calc, "OFIEngine")
    assert hasattr(calc, "FusionEngine")