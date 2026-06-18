"""STEP3.5 RealtimeCvdOfiDriver tests O1-O15. docs/STEP3.5_任务卡.md."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from laoma_signal_engine.micro.bucket.bucket_aggregator import OneSecondBucket, TradeBucketStats
from laoma_signal_engine.micro.normalized_models import NormalizedBook
from laoma_signal_engine.micro.realtime.cvd_ofi_driver import RealtimeCvdOfiDriver


SYM = "BTCUSDT"


def _book_l1(ts_ms: int = 1000, *, symbol: str = SYM) -> NormalizedBook:
    return NormalizedBook(
        symbol=symbol,
        ts_ms=ts_ms,
        bids=[(100.0, 1.0)],
        asks=[(100.5, 1.0)],
        levels=1,
    )


def _book_l5(ts_ms: int = 1000, *, symbol: str = SYM) -> NormalizedBook:
    return NormalizedBook(
        symbol=symbol,
        ts_ms=ts_ms,
        bids=[(100.0, 1.0), (99.0, 1.0), (98.0, 1.0), (97.0, 1.0), (96.0, 1.0)],
        asks=[(101.0, 1.0), (102.0, 1.0), (103.0, 1.0), (104.0, 1.0), (105.0, 1.0)],
        levels=5,
    )


def _bucket(
    ts_sec: int,
    *,
    trade: TradeBucketStats,
    t1: NormalizedBook | None = None,
    t2: NormalizedBook | None = None,
    symbol: str = SYM,
) -> OneSecondBucket:
    return OneSecondBucket(
        symbol=symbol,
        bucket_ts_sec=ts_sec,
        trade=trade,
        last_book_tier1=t1,
        last_book_tier2=t2,
    )


def test_o1_new_driver_latest_none() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    assert d.get_latest_cvd(SYM) is None
    assert d.get_latest_ofi(SYM) is None


def test_o2_single_bucket_buy_only_one_cvd() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(
        buy_qty=1.0,
        sell_qty=0.0,
        buy_quote=100.0,
        sell_quote=0.0,
        trade_count=1,
        last_price=100.0,
    )
    d.apply_buckets(SYM, [_bucket(1, trade=tr, t1=_book_l1())])
    m = d.get_metrics(SYM)
    assert m.cvd_update_count == 1
    assert m.processed_bucket_count == 1
    assert m.processed_trade_bucket_count == 1
    assert m.processed_book_bucket_count == 1


def test_o3_buy_then_sell_order() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    rt = d._runtimes[SYM]  # noqa: SLF001
    orig = rt.cvd.update_trade
    spy = MagicMock(side_effect=orig)
    rt.cvd.update_trade = spy
    tr = TradeBucketStats(
        buy_qty=1.0,
        sell_qty=2.0,
        buy_quote=100.0,
        sell_quote=200.0,
        trade_count=2,
        last_price=100.0,
    )
    d.apply_buckets(SYM, [_bucket(1, trade=tr, t1=_book_l1())])
    sides = [c.args[0].side for c in spy.call_args_list]
    assert sides == ["buy", "sell"]


def test_o4_ofi_levels_1_tier1_ok() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(0.0, 0.0, 0.0, 0.0, 0, None)
    d.apply_buckets(SYM, [_bucket(1, trade=tr, t1=_book_l1())])
    m = d.get_metrics(SYM)
    assert m.ofi_update_count == 1
    assert m.processed_trade_bucket_count == 0
    assert m.processed_book_bucket_count == 1
    assert d.get_latest_ofi(SYM) is not None


def test_o5_ofi_levels_5_tier2_ok() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 5)
    tr = TradeBucketStats(0.0, 0.0, 0.0, 0.0, 0, None)
    d.apply_buckets(SYM, [_bucket(1, trade=tr, t2=_book_l5())])
    assert d.get_metrics(SYM).ofi_update_count == 1


def test_o6_ofi_levels_5_only_tier1_skip() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 5)
    tr = TradeBucketStats(0.0, 0.0, 0.0, 0.0, 0, None)
    d.apply_buckets(SYM, [_bucket(1, trade=tr, t1=_book_l1(), t2=None)])
    m = d.get_metrics(SYM)
    assert m.ofi_skipped_no_book == 1
    assert m.ofi_update_count == 0


def test_o7_two_seconds_coherent() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(1.0, 0.0, 100.0, 0.0, 1, 100.0)
    d.apply_buckets(
        SYM,
        [
            _bucket(1, trade=tr, t1=_book_l1(1000)),
            _bucket(2, trade=tr, t1=_book_l1(2000)),
        ],
    )
    assert d.get_metrics(SYM).processed_bucket_count == 2
    assert d.get_latest_cvd(SYM) is not None


def test_o8_missing_last_price_skip_cvd() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(1.0, 0.0, 0.0, 0.0, 1, None)
    d.apply_buckets(SYM, [_bucket(1, trade=tr, t1=_book_l1())])
    m = d.get_metrics(SYM)
    assert m.cvd_skipped_missing_last_price == 1
    assert m.cvd_update_count == 0


def test_o9_symbols_normalized_consistent() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol("btcusdt", 1)
    tr = TradeBucketStats(1.0, 0.0, 100.0, 0.0, 1, 100.0)
    d.apply_buckets(
        "BTCusdt",
        [_bucket(1, trade=tr, t1=_book_l1(), symbol="btcuSDT")],
    )
    assert d.get_metrics("BTCUSDT").processed_bucket_count == 1


def test_o10_no_forbidden_imports_in_driver_source() -> None:
    root = Path(__file__).resolve().parents[1]
    src = (root / "micro" / "realtime" / "cvd_ofi_driver.py").read_text(encoding="utf-8")
    for bad in ("FusionEngine", "latest_micro_features", "websocket", "httpx"):
        assert bad not in src


def test_o11_double_apply_no_double_advance() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(1.0, 0.0, 100.0, 0.0, 1, 100.0)
    b = _bucket(1, trade=tr, t1=_book_l1())
    d.apply_buckets(SYM, [b])
    m1 = d.get_metrics(SYM)
    d.apply_buckets(SYM, [b])
    m2 = d.get_metrics(SYM)
    assert m2.cvd_update_count == m1.cvd_update_count
    assert m2.ofi_update_count == m1.ofi_update_count
    assert m2.duplicate_bucket_skipped == 1
    assert m2.processed_bucket_count == m1.processed_bucket_count


def test_o12_late_bucket_skipped_cursor_unchanged() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(1.0, 0.0, 100.0, 0.0, 1, 100.0)
    d.apply_buckets(SYM, [_bucket(10, trade=tr, t1=_book_l1())])
    d.apply_buckets(SYM, [_bucket(9, trade=tr, t1=_book_l1())])
    rt = d._runtimes[SYM]  # noqa: SLF001
    assert rt.last_processed_bucket_ts_sec == 10
    m = d.get_metrics(SYM)
    assert m.late_bucket_skipped == 1
    assert m.processed_bucket_count == 1


def test_o13_ofi_levels1_only_tier2_no_fallback() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(0.0, 0.0, 0.0, 0.0, 0, None)
    d.apply_buckets(SYM, [_bucket(1, trade=tr, t1=None, t2=_book_l5())])
    m = d.get_metrics(SYM)
    assert m.ofi_skipped_no_book == 1


def test_o14_ofi_adapter_raises_fail_soft_cursor_advances() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(0.0, 0.0, 0.0, 0.0, 0, None)
    rt = d._runtimes[SYM]  # noqa: SLF001

    def boom(*_a, **_k):  # type: ignore[no-untyped-def]
        raise ValueError("mock ofi failure")

    with patch.object(rt.ofi, "update_book", side_effect=boom):
        d.apply_buckets(SYM, [_bucket(5, trade=tr, t1=_book_l1())])
    m = d.get_metrics(SYM)
    assert m.adapter_error_count == 1
    assert m.processed_bucket_count == 1
    assert rt.last_processed_bucket_ts_sec == 5


def test_o15_latest_dicts_after_success() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(1.0, 0.0, 100.0, 0.0, 1, 100.0)
    d.apply_buckets(SYM, [_bucket(1, trade=tr, t1=_book_l1())])
    cvd = d.get_latest_cvd(SYM)
    ofi = d.get_latest_ofi(SYM)
    assert isinstance(cvd, dict)
    assert isinstance(ofi, dict)
    assert "cvd" in cvd
    assert "ofi" in ofi


def test_get_metrics_unknown_symbol_zero_object() -> None:
    d = RealtimeCvdOfiDriver()
    m = d.get_metrics("ETHUSDT")
    assert m.cvd_update_count == 0
    assert m.processed_bucket_count == 0


def test_level_mismatch_counts_skip_not_adapter_error() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(0.0, 0.0, 0.0, 0.0, 0, None)
    bad = NormalizedBook(symbol=SYM, ts_ms=1, bids=[(1.0, 1.0)], asks=[(2.0, 1.0)], levels=5)
    d.apply_buckets(SYM, [_bucket(1, trade=tr, t1=bad)])
    m = d.get_metrics(SYM)
    assert m.ofi_skipped_level_mismatch == 1
    assert m.adapter_error_count == 0


def test_get_global_metrics_sums() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol("AAUSDT", 1)
    d.register_symbol("BBUSDT", 1)
    tr = TradeBucketStats(1.0, 0.0, 100.0, 0.0, 1, 100.0)
    bk_aa = _book_l1(symbol="AAUSDT")
    bk_bb = _book_l1(symbol="BBUSDT")
    d.apply_buckets("AAUSDT", [_bucket(1, trade=tr, t1=bk_aa, symbol="AAUSDT")])
    d.apply_buckets("BBUSDT", [_bucket(1, trade=tr, t1=bk_bb, symbol="BBUSDT")])
    g = d.get_global_metrics()
    assert g.cvd_update_count == 2
    assert g.processed_bucket_count == 2


def test_register_rejects_ofi_params_levels_mismatch() -> None:
    from laoma_signal_engine.micro.calculators.ofi import OFIParams

    d = RealtimeCvdOfiDriver()
    with pytest.raises(ValueError, match="must match"):
        d.register_symbol(SYM, 1, ofi_params=OFIParams(levels=5))


def test_apply_unknown_symbol_raises() -> None:
    d = RealtimeCvdOfiDriver()
    tr = TradeBucketStats(0.0, 0.0, 0.0, 0.0, 0, None)
    with pytest.raises(ValueError, match="not registered"):
        d.apply_buckets(SYM, [_bucket(1, trade=tr)])


def test_bucket_symbol_mismatch_raises() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(0.0, 0.0, 0.0, 0.0, 0, None)
    with pytest.raises(ValueError, match="does not match"):
        d.apply_buckets(SYM, [_bucket(1, trade=tr, symbol="ETHUSDT")])
