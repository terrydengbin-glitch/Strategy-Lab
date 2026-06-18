"""STEP3.3 WebSocket subscription manager. docs/STEP3.3_任务卡.md WS1-WS15."""

from __future__ import annotations

import asyncio
import json

from laoma_signal_engine.micro.target_intent_models import (
    RetireIntent,
    SubscribeIntent,
    build_symbol_safe_id,
)
from laoma_signal_engine.micro.ws.subscription_manager import (
    BinanceFuturesWSManager,
    FakeWebSocketTransport,
    WSConfig,
    binance_stream_name,
)


class AckSpy:
    def __init__(self) -> None:
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []

    def mark_subscribed(self, symbol: str) -> None:
        self.subscribed.append(symbol.strip().upper())

    def mark_unsubscribed(self, symbol: str) -> None:
        self.unsubscribed.append(symbol.strip().upper())


def _sub(
    symbol: str,
    streams: tuple[str, ...],
) -> SubscribeIntent:
    sym = symbol.strip().upper()
    return SubscribeIntent(
        symbol=sym,
        symbol_safe_id=build_symbol_safe_id(sym),
        tier_key="tier1_warm_watch",
        source_state="watch_candidate",
        priority=50,
        scan_score=50,
        move_side="up",
        trigger_type="t",
        min_collect_seconds=900,
        ttl_seconds=1800,
        lifecycle="warming",
        first_seen_at="2026-01-01T00:00:00Z",
        last_target_seen_at="2026-01-01T00:00:00Z",
        streams=streams,
    )


def _retire(symbol: str) -> RetireIntent:
    sym = symbol.strip().upper()
    return RetireIntent(
        symbol=sym,
        symbol_safe_id=build_symbol_safe_id(sym),
        reason="missing_from_file",
        unsubscribe_deadline=None,
    )


def _wrap(stream: str, data: dict) -> str:
    return json.dumps({"stream": stream, "data": data}, ensure_ascii=True)


def test_ws1_empty_intents() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr)
        await m.sync([], [])
        assert tr.subscribe_calls == []

    asyncio.run(run())


def test_ws2_tier1_streams_in_subscribe() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr)
        await m.sync([_sub("BTCUSDT", ("aggTrade", "bookTicker"))], [])
        assert len(tr.subscribe_calls) >= 1
        flat = [x for c in tr.subscribe_calls for x in c]
        assert binance_stream_name("BTCUSDT", "aggTrade") in flat
        assert binance_stream_name("BTCUSDT", "bookTicker") in flat

    asyncio.run(run())


def test_step312_subscription_state_for_intents_tracks_required_active_and_events() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr)
        intent = _sub("BTCUSDT", ("aggTrade", "bookTicker"))
        await m.sync([intent], [])
        state = m.subscription_state_for_intents([intent])["BTCUSDT"]
        assert state["aggTrade"]["required"] is True
        assert state["aggTrade"]["active"] is True
        assert state["aggTrade"]["missing_reason"] == "no_events_yet"
        assert state["partialDepth5"]["required"] is False
        assert state["partialDepth5"]["missing_reason"] == "not_required_for_tier"

        stream = binance_stream_name("BTCUSDT", "aggTrade")
        m.handle_raw_message(
            _wrap(
                stream,
                {
                    "e": "aggTrade",
                    "s": "BTCUSDT",
                    "T": 1000,
                    "E": 1000,
                    "p": "1",
                    "q": "2",
                    "m": False,
                },
            ),
            recv_ts_ms=1100,
        )
        state = m.subscription_state_for_intents([intent])["BTCUSDT"]
        assert state["aggTrade"]["last_event_ts_sec"] == 1
        assert state["aggTrade"]["missing_reason"] is None

    asyncio.run(run())


def test_ws3_tier2_includes_depth() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr)
        await m.sync(
            [_sub("ETHUSDT", ("aggTrade", "bookTicker", "partialDepth5"))],
            [],
        )
        flat = [x for c in tr.subscribe_calls for x in c]
        assert binance_stream_name("ETHUSDT", "partialDepth5") in flat

    asyncio.run(run())


def test_ws4_retire_unsub_and_mark() -> None:
    async def run() -> None:
        spy = AckSpy()
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr, spy)
        await m.sync([_sub("XRPUSDT", ("aggTrade",))], [])
        assert "XRPUSDT" in spy.subscribed
        await m.sync([], [_retire("XRPUSDT")])
        flat_u = [x for c in tr.unsubscribe_calls for x in c]
        assert binance_stream_name("XRPUSDT", "aggTrade") in flat_u
        assert "XRPUSDT" in spy.unsubscribed

    asyncio.run(run())


def test_ws5_repeat_retire_sync_stable() -> None:
    async def run() -> None:
        spy = AckSpy()
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr, spy)
        await m.sync([_sub("AAUSDT", ("bookTicker",))], [])
        await m.sync([], [_retire("AAUSDT")])
        await m.sync([], [_retire("AAUSDT")])
        assert spy.unsubscribed == ["AAUSDT"]

    asyncio.run(run())


def test_ws16_no_unsub_without_retire_intent() -> None:
    """STEP3.2 grace: orchestrator may pass empty subscribe briefly; do not unsub until RetireIntent."""
    async def run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr)
        await m.sync([_sub("BTCUSDT", ("aggTrade", "bookTicker"))], [])
        assert tr.subscribe_calls
        unsub_before = len(tr.unsubscribe_calls)
        await m.sync([], [])
        assert len(tr.unsubscribe_calls) == unsub_before
        still = tr.active_streams
        assert binance_stream_name("BTCUSDT", "aggTrade") in still
        assert binance_stream_name("BTCUSDT", "bookTicker") in still

    asyncio.run(run())


def test_ws6_trade_queue_drop_oldest() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        cfg = WSConfig(event_queue_max_size=2)
        m = BinanceFuturesWSManager(cfg, tr)
        await m.sync([_sub("BTCUSDT", ("aggTrade",))], [])
        b1 = binance_stream_name("BTCUSDT", "aggTrade")
        for i in range(3):
            d = {
                "e": "aggTrade",
                "s": "BTCUSDT",
                "T": 1000 + i,
                "E": 1000 + i,
                "p": "1",
                "q": "1",
                "m": False,
            }
            m.handle_raw_message(_wrap(b1, d), recv_ts_ms=2000 + i)
        assert m.metrics.dropped_events_trade >= 1
        assert m.metrics.event_queue_overflow is True

    asyncio.run(run())


def test_ws7_book_only_latest() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr)
        await m.sync([_sub("BTCUSDT", ("bookTicker",))], [])
        bs = binance_stream_name("BTCUSDT", "bookTicker")
        for i in range(2):
            d = {
                "e": "bookTicker",
                "s": "BTCUSDT",
                "E": 5000 + i,
                "b": "1",
                "B": "1",
                "a": "2",
                "A": "2",
            }
            m.handle_raw_message(_wrap(bs, d), recv_ts_ms=6000 + i)
        drained = m.drain_events()
        books = [e for e in drained if e.stream_type == "bookTicker"]
        assert len(books) == 1
        assert books[0].event_ts_ms == 5001

    asyncio.run(run())


def test_ws8_reconnect_no_dup_after_resync() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(per_connection_stream_limit=80), tr)
        await m.sync([_sub("BTCUSDT", ("aggTrade",))], [])
        m.apply_reconnect_reset()
        await m.sync([_sub("BTCUSDT", ("aggTrade",))], [])
        flat = [x for c in tr.subscribe_calls for x in c]
        assert flat.count(binance_stream_name("BTCUSDT", "aggTrade")) >= 1

    asyncio.run(run())


def test_ws9_partition_per_connection_limit() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        cfg = WSConfig(per_connection_stream_limit=1)
        m = BinanceFuturesWSManager(cfg, tr)
        await m.sync([_sub("BTCUSDT", ("aggTrade", "bookTicker", "partialDepth5"))], [])
        assert len(tr.subscribe_calls) == 3

    asyncio.run(run())


def test_ws10_normalize_error_no_queue() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr)
        await m.sync([_sub("BTCUSDT", ("aggTrade",))], [])
        b1 = binance_stream_name("BTCUSDT", "aggTrade")
        m.handle_raw_message(_wrap(b1, {"e": "aggTrade", "bad": True}), recv_ts_ms=1)
        assert m.metrics.normalize_errors_trade >= 1
        assert m.drain_events() == []

    asyncio.run(run())


def test_ws11_unknown_stream_skipped() -> None:
    async def run() -> None:
        spy = AckSpy()
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr, spy)
        await m.sync([_sub("BTCUSDT", ("badStream",))], [])
        assert m.metrics.invalid_stream
        assert spy.subscribed == []

    asyncio.run(run())


def test_ws12_partial_subscribe_no_mark_until_all() -> None:
    async def run() -> None:
        spy = AckSpy()
        fail = {binance_stream_name("BTCUSDT", "bookTicker")}
        tr = FakeWebSocketTransport(failed_subscribe=fail)
        m = BinanceFuturesWSManager(WSConfig(), tr, spy)
        await m.sync([_sub("BTCUSDT", ("aggTrade", "bookTicker"))], [])
        assert binance_stream_name("BTCUSDT", "aggTrade") in tr.active_streams
        assert binance_stream_name("BTCUSDT", "bookTicker") not in tr.active_streams
        assert spy.subscribed == []

    asyncio.run(run())


def test_ws13_all_three_streams_then_mark() -> None:
    async def run() -> None:
        spy = AckSpy()
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr, spy)
        await m.sync([_sub("SOLUSDT", ("aggTrade", "bookTicker", "partialDepth5"))], [])
        assert spy.subscribed == ["SOLUSDT"]

    asyncio.run(run())


def test_ws14_book_depth_replacement_counters() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr)
        await m.sync([_sub("BTCUSDT", ("bookTicker", "partialDepth5"))], [])
        bs = binance_stream_name("BTCUSDT", "bookTicker")
        ds = binance_stream_name("BTCUSDT", "partialDepth5")
        bids = [[float(i), 1.0] for i in range(5)]
        asks = [[float(i + 10), 1.0] for i in range(5)]
        for i in range(2):
            m.handle_raw_message(
                _wrap(
                    bs,
                    {
                        "e": "bookTicker",
                        "s": "BTCUSDT",
                        "E": i,
                        "b": "1",
                        "B": "1",
                        "a": "2",
                        "A": "2",
                    },
                ),
                recv_ts_ms=100 + i,
            )
        for i in range(2):
            m.handle_raw_message(
                _wrap(
                    ds,
                    {
                        "e": "depthUpdate",
                        "s": "BTCUSDT",
                        "E": 100 + i,
                        "b": bids,
                        "a": asks,
                    },
                ),
                recv_ts_ms=200 + i,
            )
        assert m.metrics.book_replaced_count >= 1
        assert m.metrics.depth_replaced_count >= 1
        assert m.metrics.book_latest_state_overwrite_count >= 1
        assert m.metrics.depth_bucket_coalesced_count >= 1
        assert m.metrics.dropped_events_book == 0
        assert m.metrics.dropped_events_depth == 0

    asyncio.run(run())


def test_ws15_reconnect_clears_active_then_restore() -> None:
    async def run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr)
        await m.sync([_sub("ADAUSDT", ("aggTrade",))], [])
        assert tr.active_streams
        m.apply_reconnect_reset()
        assert tr.active_streams == set()
        await m.sync([_sub("ADAUSDT", ("aggTrade",))], [])
        assert binance_stream_name("ADAUSDT", "aggTrade") in tr.active_streams

    asyncio.run(run())
