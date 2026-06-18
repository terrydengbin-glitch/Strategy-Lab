"""STEP3.4 1s bucket aggregator. docs/STEP3.4_任务卡.md."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Literal

from laoma_signal_engine.micro.normalized_models import NormalizedBook, NormalizedTrade
from laoma_signal_engine.micro.ws.subscription_manager import WSEventEnvelope

CoverageStreamType = Literal["aggTrade", "bookTicker", "partialDepth5", "any"]

_COVERAGE_STREAMS: frozenset[str] = frozenset(
    {"aggTrade", "bookTicker", "partialDepth5", "any"}
)


def _ts_ok(value: Any) -> bool:
    return type(value) is int and value >= 0


def _strictly_after(ev2: int, r2: int, s2: int, ev1: int, r1: int, s1: int) -> bool:
    if ev2 != ev1:
        return ev2 > ev1
    if r2 != r1:
        return r2 > r1
    return s2 > s1


def _clone_book(book: NormalizedBook) -> NormalizedBook:
    return NormalizedBook(
        symbol=book.symbol,
        ts_ms=book.ts_ms,
        bids=list(book.bids),
        asks=list(book.asks),
        levels=book.levels,
    )


@dataclass(frozen=True)
class TradeBucketStats:
    buy_qty: float
    sell_qty: float
    buy_quote: float
    sell_quote: float
    trade_count: int
    last_price: float | None

    @property
    def total_qty(self) -> float:
        return self.buy_qty + self.sell_qty

    @property
    def total_quote(self) -> float:
        return self.buy_quote + self.sell_quote

    @property
    def vwap(self) -> float | None:
        tq = self.total_qty
        if tq == 0.0:
            return None
        return self.total_quote / tq


@dataclass(frozen=True)
class OneSecondBucket:
    symbol: str
    bucket_ts_sec: int
    trade: TradeBucketStats
    last_book_tier1: NormalizedBook | None
    last_book_tier2: NormalizedBook | None


@dataclass(frozen=True)
class CoverageSnapshot:
    expected_seconds: int
    covered_seconds: int
    gap_count: int
    max_gap_sec: int


@dataclass
class BucketConfig:
    ring_buffer_seconds: int = 1800
    carry_forward_book: bool = False


class _MutableRingBucket:
    __slots__ = (
        "symbol",
        "bucket_ts_sec",
        "buy_qty",
        "sell_qty",
        "buy_quote",
        "sell_quote",
        "trade_count",
        "last_price",
        "_trade_ev",
        "_trade_recv",
        "_trade_ing",
        "last_book_tier1",
        "_b1_ev",
        "_b1_recv",
        "_b1_ing",
        "last_book_tier2",
        "_b2_ev",
        "_b2_recv",
        "_b2_ing",
    )

    def __init__(self, symbol: str, bucket_ts_sec: int) -> None:
        self.symbol = symbol
        self.bucket_ts_sec = bucket_ts_sec
        self.buy_qty = 0.0
        self.sell_qty = 0.0
        self.buy_quote = 0.0
        self.sell_quote = 0.0
        self.trade_count = 0
        self.last_price: float | None = None
        self._trade_ev = -1
        self._trade_recv = -1
        self._trade_ing = -1
        self.last_book_tier1: NormalizedBook | None = None
        self._b1_ev = -1
        self._b1_recv = -1
        self._b1_ing = -1
        self.last_book_tier2: NormalizedBook | None = None
        self._b2_ev = -1
        self._b2_recv = -1
        self._b2_ing = -1

    def to_immutable(self) -> OneSecondBucket:
        stats = TradeBucketStats(
            buy_qty=self.buy_qty,
            sell_qty=self.sell_qty,
            buy_quote=self.buy_quote,
            sell_quote=self.sell_quote,
            trade_count=self.trade_count,
            last_price=self.last_price,
        )
        b1 = _clone_book(self.last_book_tier1) if self.last_book_tier1 is not None else None
        b2 = _clone_book(self.last_book_tier2) if self.last_book_tier2 is not None else None
        return OneSecondBucket(
            symbol=self.symbol,
            bucket_ts_sec=self.bucket_ts_sec,
            trade=stats,
            last_book_tier1=b1,
            last_book_tier2=b2,
        )


class BucketAggregator:
    """Consumes WSEventEnvelope; 1s buckets + ring + coverage. STEP3.4."""

    def __init__(self, config: BucketConfig | None = None) -> None:
        self.config = config or BucketConfig()
        if self.config.ring_buffer_seconds < 1:
            msg = "ring_buffer_seconds must be >= 1"
            raise ValueError(msg)
        self._rings: dict[str, dict[int, _MutableRingBucket]] = defaultdict(dict)
        self._max_seen_bucket_ts_sec: dict[str, int] = {}
        self._ingest_seq = 0
        self.buckets_evicted_total = 0
        self.buckets_evicted_by_symbol: defaultdict[str, int] = defaultdict(int)
        self.events_ignored_malformed = 0

    def on_event(self, envelope: WSEventEnvelope) -> None:
        self._ingest_one(envelope)

    def ingest(self, events: WSEventEnvelope | Iterable[WSEventEnvelope]) -> None:
        if isinstance(events, WSEventEnvelope):
            self._ingest_one(events)
            return
        for ev in events:
            self._ingest_one(ev)

    def _reject_malformed(self) -> None:
        self.events_ignored_malformed += 1

    def _ingest_one(self, envelope: WSEventEnvelope) -> None:
        if not _ts_ok(envelope.event_ts_ms) or not _ts_ok(envelope.recv_ts_ms):
            self._reject_malformed()
            return
        st = envelope.stream_type
        if st not in ("aggTrade", "bookTicker", "partialDepth5"):
            self._reject_malformed()
            return
        if st == "aggTrade" and not isinstance(envelope.normalized, NormalizedTrade):
            self._reject_malformed()
            return
        if st in ("bookTicker", "partialDepth5") and not isinstance(
            envelope.normalized,
            NormalizedBook,
        ):
            self._reject_malformed()
            return

        self._ingest_seq += 1
        ing = self._ingest_seq
        sym = envelope.symbol.strip().upper()
        bsec = envelope.event_ts_ms // 1000
        ring = self._rings[sym]
        bucket = ring.get(bsec)
        if bucket is None:
            bucket = _MutableRingBucket(sym, bsec)
            ring[bsec] = bucket

        prev_max = self._max_seen_bucket_ts_sec.get(sym, bsec)
        self._max_seen_bucket_ts_sec[sym] = max(prev_max, bsec)

        if st == "aggTrade":
            self._apply_trade(bucket, envelope, ing)
        elif st == "bookTicker":
            self._apply_book(bucket, envelope, ing, tier=1)
        else:
            self._apply_book(bucket, envelope, ing, tier=2)

        self._evict(sym)

    def _apply_trade(
        self,
        bucket: _MutableRingBucket,
        envelope: WSEventEnvelope,
        ing: int,
    ) -> None:
        tr = envelope.normalized
        assert isinstance(tr, NormalizedTrade)
        price = float(tr.price)
        qty = float(tr.qty)
        quote = price * qty
        if tr.side == "buy":
            bucket.buy_qty += qty
            bucket.buy_quote += quote
        else:
            bucket.sell_qty += qty
            bucket.sell_quote += quote
        bucket.trade_count += 1

        ev, rv = envelope.event_ts_ms, envelope.recv_ts_ms
        if bucket._trade_ev < 0 or _strictly_after(ev, rv, ing, bucket._trade_ev, bucket._trade_recv, bucket._trade_ing):
            bucket.last_price = price
            bucket._trade_ev = ev
            bucket._trade_recv = rv
            bucket._trade_ing = ing

    def _apply_book(
        self,
        bucket: _MutableRingBucket,
        envelope: WSEventEnvelope,
        ing: int,
        *,
        tier: Literal[1, 2],
    ) -> None:
        book = envelope.normalized
        assert isinstance(book, NormalizedBook)
        ev, rv = envelope.event_ts_ms, envelope.recv_ts_ms
        if tier == 1:
            if bucket._b1_ev < 0 or _strictly_after(ev, rv, ing, bucket._b1_ev, bucket._b1_recv, bucket._b1_ing):
                bucket.last_book_tier1 = book
                bucket._b1_ev = ev
                bucket._b1_recv = rv
                bucket._b1_ing = ing
        elif bucket._b2_ev < 0 or _strictly_after(ev, rv, ing, bucket._b2_ev, bucket._b2_recv, bucket._b2_ing):
            bucket.last_book_tier2 = book
            bucket._b2_ev = ev
            bucket._b2_recv = rv
            bucket._b2_ing = ing

    def _evict(self, symbol: str) -> None:
        max_seen = self._max_seen_bucket_ts_sec.get(symbol)
        if max_seen is None:
            return
        threshold = max_seen - self.config.ring_buffer_seconds + 1
        ring = self._rings[symbol]
        dead = [ts for ts in ring if ts < threshold]
        for ts in dead:
            del ring[ts]
            self.buckets_evicted_total += 1
            self.buckets_evicted_by_symbol[symbol] += 1
        if not ring:
            self._rings.pop(symbol, None)
            self._max_seen_bucket_ts_sec.pop(symbol, None)

    def get_buckets(
        self,
        symbol: str,
        start_ts_sec: int,
        end_ts_sec: int,
    ) -> list[OneSecondBucket]:
        sym = symbol.strip().upper()
        ring = self._rings.get(sym)
        if not ring:
            return []
        out: list[OneSecondBucket] = []
        for ts in sorted(k for k in ring if start_ts_sec <= k < end_ts_sec):
            out.append(ring[ts].to_immutable())
        return out

    def get_coverage(
        self,
        symbol: str,
        stream_type: CoverageStreamType,
        end_ts_sec: int,
        window_sec: int,
    ) -> CoverageSnapshot:
        if window_sec < 1:
            msg = "window_sec must be >= 1"
            raise ValueError(msg)
        if stream_type not in _COVERAGE_STREAMS:
            msg = f"stream_type must be one of {sorted(_COVERAGE_STREAMS)}"
            raise ValueError(msg)
        start_ts_sec = end_ts_sec - window_sec
        sym = symbol.strip().upper()
        ring = self._rings.get(sym, {})
        covered_flags: list[bool] = []
        for sec in range(start_ts_sec, end_ts_sec):
            b = ring.get(sec)
            covered_flags.append(self._second_covered(b, stream_type))
        covered_seconds = sum(1 for c in covered_flags if c)

        gap_count = 0
        max_gap_sec = 0
        run = 0
        prev_uncovered = False
        for c in covered_flags:
            if not c:
                run += 1
                if not prev_uncovered:
                    gap_count += 1
                prev_uncovered = True
                max_gap_sec = max(max_gap_sec, run)
            else:
                run = 0
                prev_uncovered = False

        return CoverageSnapshot(
            expected_seconds=window_sec,
            covered_seconds=covered_seconds,
            gap_count=gap_count,
            max_gap_sec=max_gap_sec,
        )

    @staticmethod
    def _second_covered(
        bucket: _MutableRingBucket | None,
        stream_type: CoverageStreamType,
    ) -> bool:
        if bucket is None:
            return False
        if stream_type == "any":
            return (
                bucket.trade_count > 0
                or bucket.last_book_tier1 is not None
                or bucket.last_book_tier2 is not None
            )
        if stream_type == "aggTrade":
            return bucket.trade_count > 0
        if stream_type == "bookTicker":
            return bucket.last_book_tier1 is not None
        return bucket.last_book_tier2 is not None
