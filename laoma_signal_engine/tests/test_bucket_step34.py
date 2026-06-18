"""STEP3.4 bucket aggregator. docs/STEP3.4_任务卡.md B1-B13."""

from __future__ import annotations

import pytest
from typing import Literal

from laoma_signal_engine.micro.bucket.bucket_aggregator import (
    BucketAggregator,
    BucketConfig,
)
from laoma_signal_engine.micro.normalized_models import NormalizedBook, NormalizedTrade
from laoma_signal_engine.micro.ws.subscription_manager import WSEventEnvelope


def _trade(sym: str, ev: int, rv: int, *, price: float, qty: float, side: Literal["buy", "sell"]) -> WSEventEnvelope:
    nt = NormalizedTrade(symbol=sym, ts_ms=ev, price=price, qty=qty, side=side)
    return WSEventEnvelope(
        symbol=sym,
        stream_type="aggTrade",
        event_ts_ms=ev,
        recv_ts_ms=rv,
        normalized=nt,
    )


def _book_tier1(sym: str, ev: int, rv: int) -> WSEventEnvelope:
    nb = NormalizedBook(
        symbol=sym,
        ts_ms=ev,
        bids=[(100.0, 1.0)],
        asks=[(101.0, 1.0)],
        levels=1,
    )
    return WSEventEnvelope(
        symbol=sym,
        stream_type="bookTicker",
        event_ts_ms=ev,
        recv_ts_ms=rv,
        normalized=nb,
    )


def _book_tier2(sym: str, ev: int, rv: int) -> WSEventEnvelope:
    nb = NormalizedBook(
        symbol=sym,
        ts_ms=ev,
        bids=[(100.0, 1.0), (99.0, 2.0)],
        asks=[(101.0, 1.0), (102.0, 2.0)],
        levels=5,
    )
    return WSEventEnvelope(
        symbol=sym,
        stream_type="partialDepth5",
        event_ts_ms=ev,
        recv_ts_ms=rv,
        normalized=nb,
    )


def test_b1_empty() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=1800))
    assert agg.get_buckets("BTCUSDT", 0, 10) == []
    assert agg.buckets_evicted_total == 0
    assert agg.events_ignored_malformed == 0


def test_b2_single_trade_bucket_sec() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=100))
    agg.on_event(_trade("BTCUSDT", 1500, 1600, price=10.0, qty=2.0, side="buy"))
    rows = agg.get_buckets("BTCUSDT", 0, 10)
    assert len(rows) == 1
    assert rows[0].bucket_ts_sec == 1
    assert rows[0].trade.buy_qty == 2.0
    assert rows[0].trade.last_price == 10.0


def test_b3_multi_trades_same_bucket_vwap_and_last() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=100))
    agg.ingest(
        [
            _trade("ETHUSDT", 5000, 5001, price=100.0, qty=1.0, side="buy"),
            _trade("ETHUSDT", 5000, 5002, price=200.0, qty=1.0, side="sell"),
            _trade("ETHUSDT", 5000, 5003, price=150.0, qty=2.0, side="buy"),
        ]
    )
    rows = agg.get_buckets("ETHUSDT", 4, 6)
    assert len(rows) == 1
    t = rows[0].trade
    assert t.buy_qty == 3.0
    assert t.sell_qty == 1.0
    assert t.buy_quote == 100.0 + 300.0
    assert t.sell_quote == 200.0
    assert t.total_qty == 4.0
    assert t.trade_count == 3
    assert t.last_price == 150.0
    assert t.vwap == 150.0


def test_dod_bucket_ts_sec_floor_per_millisecond_boundary() -> None:
    """DoD 9.1: bucket_ts_sec == event_ts_ms // 1000 (999 vs 1000)."""
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=50))
    agg.on_event(_trade("DOdUSDT", 999, 0, price=1.0, qty=1.0, side="buy"))
    agg.on_event(_trade("DOdUSDT", 1000, 0, price=2.0, qty=1.0, side="buy"))
    rows = agg.get_buckets("DOdUSDT", 0, 10)
    assert {r.bucket_ts_sec for r in rows} == {0, 1}
    by_ts = {r.bucket_ts_sec: r for r in rows}
    assert by_ts[0].trade.trade_count == 1
    assert by_ts[1].trade.trade_count == 1


def test_b4_cross_second_boundary() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=100))
    agg.on_event(_trade("XRPUSDT", 1999, 1, price=1.0, qty=1.0, side="buy"))
    agg.on_event(_trade("XRPUSDT", 2000, 2, price=2.0, qty=1.0, side="buy"))
    rows = agg.get_buckets("XRPUSDT", 0, 10)
    assert [r.bucket_ts_sec for r in rows] == [1, 2]


def test_b5_book_tie_break() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=100))
    b_low = NormalizedBook(symbol="AAUSDT", ts_ms=10, bids=[(1.0, 1.0)], asks=[], levels=1)
    b_high = NormalizedBook(symbol="AAUSDT", ts_ms=11, bids=[(2.0, 1.0)], asks=[], levels=1)
    agg.on_event(
        WSEventEnvelope("AAUSDT", "bookTicker", 1000, 2000, b_high),
    )
    agg.on_event(
        WSEventEnvelope("AAUSDT", "bookTicker", 1000, 1000, b_low),
    )
    row = agg.get_buckets("AAUSDT", 1, 2)[0]
    assert row.last_book_tier1 is not None
    assert row.last_book_tier1.bids[0][0] == 2.0


def test_b6_book_and_depth_same_bucket_independent() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=100))
    agg.on_event(_book_tier1("BBUSDT", 3000, 3001))
    agg.on_event(_book_tier2("BBUSDT", 3000, 3002))
    row = agg.get_buckets("BBUSDT", 3, 4)[0]
    assert row.last_book_tier1 is not None
    assert row.last_book_tier2 is not None
    assert row.last_book_tier1.levels == 1
    assert row.last_book_tier2.levels == 5


def test_b7_ring_eviction_watermark() -> None:
    cfg = BucketConfig(ring_buffer_seconds=3)
    agg = BucketAggregator(cfg)
    for sec in (10, 11, 12):
        agg.on_event(_trade("CCUSDT", sec * 1000, 0, price=1.0, qty=1.0, side="buy"))
    keys_before = {r.bucket_ts_sec for r in agg.get_buckets("CCUSDT", 0, 100)}
    assert keys_before == {10, 11, 12}
    agg.on_event(_trade("CCUSDT", 13000, 0, price=1.0, qty=1.0, side="buy"))
    keys_after = {r.bucket_ts_sec for r in agg.get_buckets("CCUSDT", 0, 100)}
    assert keys_after == {11, 12, 13}
    assert agg.buckets_evicted_total >= 1
    assert agg.buckets_evicted_by_symbol["CCUSDT"] >= 1


def test_b8_out_of_order_buckets() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=50))
    agg.on_event(_trade("DDUSDT", 2000, 0, price=2.0, qty=1.0, side="buy"))
    agg.on_event(_trade("DDUSDT", 1000, 0, price=1.0, qty=1.0, side="buy"))
    rows = agg.get_buckets("DDUSDT", 0, 10)
    assert {r.bucket_ts_sec for r in rows} == {1, 2}
    assert sum(r.trade.buy_qty for r in rows) == 2.0


def test_b9_negative_event_ts_ignored() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=10))
    agg.on_event(_trade("EEUSDT", -1, 0, price=1.0, qty=1.0, side="buy"))
    assert agg.events_ignored_malformed == 1
    assert agg.get_buckets("EEUSDT", -5, 5) == []


def test_b10_coverage_gaps() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=100))
    for sec in (0, 1, 5, 6):
        agg.on_event(_trade("FFUSDT", sec * 1000 + 500, 0, price=1.0, qty=1.0, side="buy"))
    snap = agg.get_coverage("FFUSDT", "aggTrade", end_ts_sec=10, window_sec=10)
    assert snap.expected_seconds == 10
    assert snap.covered_seconds == 4
    assert snap.gap_count == 2
    assert snap.max_gap_sec == 3


def test_b11_unknown_stream_ignored() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=10))
    nt = NormalizedTrade(symbol="GGUSDT", ts_ms=0, price=1.0, qty=1.0, side="buy")
    ev = WSEventEnvelope("GGUSDT", "bogus", 1000, 1000, nt)
    agg.on_event(ev)
    assert agg.events_ignored_malformed == 1


def test_b12_type_mismatch_ignored() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=10))
    nb = NormalizedBook(symbol="HHUSDT", ts_ms=0, bids=[], asks=[], levels=1)
    ev = WSEventEnvelope("HHUSDT", "aggTrade", 1000, 1000, nb)
    agg.on_event(ev)
    assert agg.events_ignored_malformed == 1


def test_b13_last_price_ingest_tie_break_same_event_recv() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=100))
    base_ev = 10_000
    base_rv = 20_000
    agg.on_event(
        WSEventEnvelope(
            "ZZUSDT",
            "aggTrade",
            base_ev,
            base_rv,
            NormalizedTrade("ZZUSDT", base_ev, 99.0, 1.0, "buy"),
        )
    )
    agg.on_event(
        WSEventEnvelope(
            "ZZUSDT",
            "aggTrade",
            base_ev,
            base_rv,
            NormalizedTrade("ZZUSDT", base_ev, 101.0, 1.0, "buy"),
        )
    )
    row = agg.get_buckets("ZZUSDT", 10, 11)[0]
    assert row.trade.last_price == 101.0
    assert row.trade.trade_count == 2
    assert row.trade.vwap == (99.0 + 101.0) / 2.0


def test_b13_out_of_order_ingest_same_ts_last_price() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=100))
    base_ev = 50_000
    base_rv = 60_000
    agg.on_event(
        WSEventEnvelope(
            "YYUSDT",
            "aggTrade",
            base_ev,
            base_rv,
            NormalizedTrade("YYUSDT", base_ev, 200.0, 1.0, "buy"),
        )
    )
    agg.on_event(
        WSEventEnvelope(
            "YYUSDT",
            "aggTrade",
            base_ev,
            base_rv,
            NormalizedTrade("YYUSDT", base_ev, 100.0, 1.0, "buy"),
        )
    )
    row = agg.get_buckets("YYUSDT", 50, 51)[0]
    assert row.trade.last_price == 100.0
    assert row.trade.vwap == 150.0


def test_get_buckets_returns_copy_not_alias() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=50))
    agg.on_event(_book_tier1("MMUSDT", 1000, 1000))
    rows = agg.get_buckets("MMUSDT", 0, 10)
    assert rows[0].last_book_tier1 is not None
    rows[0].last_book_tier1.bids.clear()  # type: ignore[union-attr]
    rows2 = agg.get_buckets("MMUSDT", 0, 10)
    assert rows2[0].last_book_tier1 is not None
    assert len(rows2[0].last_book_tier1.bids) == 1


def test_get_coverage_any_and_book_streams() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=30))
    agg.on_event(_book_tier1("NNUSDT", 2000, 0))
    c_book = agg.get_coverage("NNUSDT", "bookTicker", end_ts_sec=5, window_sec=5)
    assert c_book.covered_seconds == 1
    c_trade = agg.get_coverage("NNUSDT", "aggTrade", end_ts_sec=5, window_sec=5)
    assert c_trade.covered_seconds == 0
    c_any = agg.get_coverage("NNUSDT", "any", end_ts_sec=5, window_sec=5)
    assert c_any.covered_seconds == 1


def test_coverage_invalid_stream_type_raises() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=10))
    with pytest.raises(ValueError, match="stream_type"):
        agg.get_coverage("A", "all", 10, 5)  # type: ignore[arg-type]


def test_carry_forward_false_no_book_on_quiet_second() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=50))
    agg.on_event(_book_tier1("PPUSDT", 1000, 0))
    agg.on_event(_trade("PPUSDT", 2500, 0, price=1.0, qty=1.0, side="buy"))
    row = agg.get_buckets("PPUSDT", 2, 3)[0]
    assert row.trade.trade_count == 1
    assert row.last_book_tier1 is None


def test_vwap_none_when_zero_qty() -> None:
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=20))
    agg.on_event(_book_tier1("QQUSDT", 1000, 0))
    row = agg.get_buckets("QQUSDT", 1, 2)[0]
    assert row.trade.trade_count == 0
    assert row.trade.vwap is None
