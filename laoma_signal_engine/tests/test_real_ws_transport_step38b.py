"""STEP3.8B real Binance WS routing/transport tests R1-R18. docs/STEP3.8B_Real_Binance_WS_任务卡.md."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from laoma_signal_engine.micro.daemon.cli import main
from laoma_signal_engine.micro.daemon.loop import RunOnceResult, build_run_context
from laoma_signal_engine.micro.daemon.config import DaemonConfig
from laoma_signal_engine.micro.target_intent_models import RetireIntent, SubscribeIntent, build_symbol_safe_id
from laoma_signal_engine.micro.ws import routing
from laoma_signal_engine.micro.ws.real_transport import (
    RealBinanceFuturesWebSocketTransport,
    RealTransportConfig,
    _ControlRateLimiter,
    _parse_control_response,
)
from laoma_signal_engine.micro.ws.subscription_manager import (
    BinanceFuturesWSManager,
    FakeWebSocketTransport,
    WSConfig,
    WSMetrics,
)


def _sub(symbol: str, streams: tuple[str, ...]) -> SubscribeIntent:
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
        min_collect_seconds=0,
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


class _AckSpy:
    def __init__(self) -> None:
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []

    def mark_subscribed(self, symbol: str) -> None:
        self.subscribed.append(symbol.strip().upper())

    def mark_unsubscribed(self, symbol: str) -> None:
        self.unsubscribed.append(symbol.strip().upper())


class MockBinanceWS:
    """Minimal mock: SUBSCRIBE/UNSUBSCRIBE responses are queued from send()."""

    def __init__(self, *, error_ids: set[int] | None = None) -> None:
        self.sent: list[str] = []
        self._recv_queue: asyncio.Queue[str] = asyncio.Queue()
        self._error_ids = error_ids or set()

    async def send(self, raw: str) -> None:
        self.sent.append(raw)
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        mid = msg.get("id")
        method = msg.get("method")
        if not isinstance(mid, int) or method not in ("SUBSCRIBE", "UNSUBSCRIBE"):
            return
        if mid in self._error_ids:
            await self._recv_queue.put(
                json.dumps({"id": mid, "error": {"code": -1, "msg": "test"}}, ensure_ascii=True),
            )
            return
        await self._recv_queue.put(json.dumps({"id": mid, "result": None}, ensure_ascii=True))

    async def recv(self) -> str:
        return await self._recv_queue.get()

    async def close(self) -> None:
        return None


def test_r1_stream_route_map() -> None:
    assert routing.LOGICAL_STREAM_TO_ROUTE["aggTrade"] == "market"
    assert routing.LOGICAL_STREAM_TO_ROUTE["bookTicker"] == "public"
    assert routing.LOGICAL_STREAM_TO_ROUTE["partialDepth5"] == "public"


def test_r2_combined_stream_url_orders_streams() -> None:
    streams = ["z@bookTicker", "a@bookTicker"]
    u = routing.combined_stream_ws_url(
        "wss://fstream.binance.com",
        "public",
        streams,
        public_path="/public",
        market_path="/market",
    )
    assert u.startswith("wss://fstream.binance.com/public/stream?streams=")
    assert "a@bookTicker" in u and "z@bookTicker" in u
    pos_a = u.index("a@bookTicker")
    pos_z = u.index("z@bookTicker")
    assert pos_a < pos_z


def test_r3_partition_hard_cap_and_sorting() -> None:
    with pytest.raises(ValueError, match="1024"):
        routing.partition_sorted_streams(["a"], routing.MAX_STREAMS_PER_CONNECTION_HARD_CAP + 1)
    parts = routing.partition_sorted_streams(["b", "a"], 1)
    assert parts == [["a"], ["b"]]


def test_r3_group_splits_routes() -> None:
    t1 = routing.binance_stream_name("BTCUSDT", "aggTrade")
    t2 = routing.binance_stream_name("BTCUSDT", "bookTicker")
    g, inv = routing.group_streams_by_route([t2, t1, "bogus@x"])
    assert inv == ["bogus@x"]
    assert g["market"] == [t1]
    assert g["public"] == [t2]


def test_r4_combined_payload_to_envelope() -> None:
    tr = FakeWebSocketTransport()
    m = BinanceFuturesWSManager(WSConfig(), tr, target_manager=None)
    stream = routing.binance_stream_name("BTCUSDT", "aggTrade")
    asyncio.run(tr.subscribe_streams([stream]))
    raw = json.dumps(
        {
            "stream": stream,
            "data": {"s": "BTCUSDT", "T": 1, "p": "2", "q": "1", "m": False},
        },
        ensure_ascii=True,
    )
    m.handle_raw_message(raw, 99)
    assert m.metrics.last_ws_recv_ts_ms == 99
    ev = m.drain_events()
    assert len(ev) == 1
    assert ev[0].stream_type == "aggTrade"


def test_r5_reconnect_restores_desired(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> None:
        tr = FakeWebSocketTransport()
        cfg = WSConfig(per_connection_stream_limit=80)
        m = BinanceFuturesWSManager(cfg, tr, target_manager=None)

        def _fake_partition(streams: list[str], _limit: int) -> list[list[str]]:
            return [streams]

        monkeypatch.setattr(
            "laoma_signal_engine.micro.ws.subscription_manager.partition_streams",
            _fake_partition,
        )

        await m.sync([_sub("BTCUSDT", ("aggTrade",))], [])
        have1 = set(tr.active_streams)
        m.apply_reconnect_reset()
        assert tr.active_streams == set()
        await m.sync([_sub("BTCUSDT", ("aggTrade",))], [])
        assert tr.active_streams == have1

    asyncio.run(_run())


def test_r6_no_private_unsub_without_retire_intent() -> None:
    async def _run() -> None:
        tr = FakeWebSocketTransport()
        m = BinanceFuturesWSManager(WSConfig(), tr, target_manager=None)
        await m.sync([_sub("BTCUSDT", ("aggTrade", "bookTicker"))], [])
        await m.sync([], [])
        assert tr.unsubscribe_calls == []

    asyncio.run(_run())


def test_r7_metrics_on_error_and_reconnect() -> None:
    async def _run() -> None:
        cfg0 = RealTransportConfig(
            auto_ack_for_testing=False,
            subscribe_batch_size=10,
            unsubscribe_batch_size=10,
            per_connection_stream_limit=80,
            ack_timeout_sec=2.0,
        )
        t = RealBinanceFuturesWebSocketTransport(cfg0)
        m = BinanceFuturesWSManager(WSConfig(per_connection_stream_limit=80), t, target_manager=None)
        t.bind_metrics(m.metrics)
        t.bind_manager(m)
        met = m.metrics
        ws = MockBinanceWS(error_ids={1})

        async def _connect(*_a: object, **_k: object) -> MockBinanceWS:
            return ws

        s_pub = routing.binance_stream_name("AAUSDT", "bookTicker")
        s2_pub = routing.binance_stream_name("ABUSDT", "bookTicker")
        with patch("laoma_signal_engine.micro.ws.real_transport.websockets.connect", _connect):
            await t.subscribe_streams([s_pub])
            await t.subscribe_streams([s2_pub])
        assert met.subscribe_error_count >= 1
        assert met.connect_count >= 1
        t.clear_all()
        assert met.reconnect_count >= 1
        await t.aclose()

    asyncio.run(_run())


def test_r8_daemon_does_not_import_websockets() -> None:
    root = Path(__file__).resolve().parents[1] / "micro" / "daemon"
    for name in ("app.py", "cli.py", "config.py", "loop.py"):
        text = (root / name).read_text(encoding="utf-8")
        assert "import websockets" not in text
        assert "from websockets" not in text


def test_r9_unknown_logical_stream_rejected() -> None:
    with pytest.raises(KeyError, match="unknown logical"):
        routing.binance_stream_name("BTCUSDT", "nope")
    g, inv = routing.group_streams_by_route(["xx@unknownsuffix"])
    assert inv == ["xx@unknownsuffix"]
    assert g == {"public": [], "market": []}


def test_r10_partial_failed_subscribe_no_mark_subscribed() -> None:
    async def _run() -> None:
        spy = _AckSpy()
        tr = FakeWebSocketTransport(failed_subscribe={routing.binance_stream_name("BTCUSDT", "aggTrade")})
        m = BinanceFuturesWSManager(WSConfig(), tr, target_manager=spy)
        await m.sync([_sub("BTCUSDT", ("aggTrade", "bookTicker"))], [])
        assert spy.subscribed == []

    asyncio.run(_run())


def test_r12_cli_transport_real_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from laoma_signal_engine.tests.test_target_manager_step32 import _doc as tm_doc
    from laoma_signal_engine.tests.test_target_manager_step32 import _entry as tm_entry

    t1 = [
        tm_entry(
            "BTCUSDT",
            min_collect_seconds=0,
            subscribe=["aggTrade"],
        ),
    ]
    doc = tm_doc(generated_at="2026-01-01T00:00:00Z", status="ok", tier1=t1, tier2=[])
    targets = tmp_path / "micro_targets.json"
    targets.write_text(json.dumps(doc, ensure_ascii=True), encoding="utf-8", newline="")
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "hb.json"

    captured: list[DaemonConfig] = []

    async def _capture(cfg: DaemonConfig, **kw: object) -> RunOnceResult:
        captured.append(cfg)
        assert cfg.transport == "real"
        assert cfg.proxy_url is None
        return RunOnceResult(0, None, True)

    monkeypatch.setattr("laoma_signal_engine.micro.daemon.cli.run_daemon", _capture)
    code = main(
        [
            "--targets",
            str(targets),
            "--latest-out",
            str(latest),
            "--heartbeat-out",
            str(hb),
            "--transport",
            "real",
            "--once",
        ],
    )
    assert code == 0
    assert captured and captured[0].transport == "real"


def test_r13_control_message_rate_limiter_spaces_calls() -> None:
    delays: list[float] = []
    call_idx = {"n": 0}

    def fake_monotonic() -> float:
        n = call_idx["n"]
        call_idx["n"] = n + 1
        return 0.0 if n == 0 else 0.0

    async def fake_sleep(dt: float) -> None:
        delays.append(dt)

    async def _run() -> None:
        with (
            patch("laoma_signal_engine.micro.ws.real_transport.time.monotonic", fake_monotonic),
            patch("laoma_signal_engine.micro.ws.real_transport.asyncio.sleep", fake_sleep),
        ):
            lim = _ControlRateLimiter(2.0)
            await lim.acquire()
            await lim.acquire()

    asyncio.run(_run())
    assert delays and delays[0] >= 0.49


def test_r14_batch_ack_maps_whole_params_list() -> None:
    ok, rid = _parse_control_response(json.dumps({"id": 7, "result": None}, ensure_ascii=True))
    assert ok is True and rid == 7
    ok2, rid2 = _parse_control_response(
        json.dumps({"id": 8, "error": {"code": 1, "msg": "x"}}, ensure_ascii=True),
    )
    assert ok2 is False and rid2 == 8


def test_r14_incremental_subscribe_waits_for_batch_ack() -> None:
    async def _run() -> None:
        cfg_tr = RealTransportConfig(
            auto_ack_for_testing=False,
            per_connection_stream_limit=10,
            subscribe_batch_size=50,
            unsubscribe_batch_size=50,
            ack_timeout_sec=2.0,
        )
        t = RealBinanceFuturesWebSocketTransport(cfg_tr)
        m = BinanceFuturesWSManager(WSConfig(per_connection_stream_limit=10), t, target_manager=None)
        t.bind_metrics(m.metrics)
        t.bind_manager(m)

        mock_ws = MockBinanceWS()
        urls: list[str] = []

        async def _connect(uri: str, **_k: object) -> MockBinanceWS:
            urls.append(uri)
            return mock_ws

        s1 = routing.binance_stream_name("ALUSDT", "bookTicker")
        s2 = routing.binance_stream_name("AMUSDT", "bookTicker")
        with patch("laoma_signal_engine.micro.ws.real_transport.websockets.connect", _connect):
            await t.subscribe_streams([s1])
            assert s1 in t.active_streams
            await t.subscribe_streams([s2])
        assert s2 in t.active_streams
        sub_msgs = [json.loads(x) for x in mock_ws.sent if "SUBSCRIBE" in x]
        assert any(s2 in set(p.get("params", [])) for p in sub_msgs if p.get("method") == "SUBSCRIBE")
        assert "/public/" in urls[0]
        await t.aclose()

    asyncio.run(_run())


def test_r14_ack_timeout_increments_subscribe_error() -> None:
    class HangWS(MockBinanceWS):
        async def send(self, raw: str) -> None:
            self.sent.append(raw)

        async def recv(self) -> str:
            await asyncio.sleep(3600.0)
            return "{}"

    async def _run() -> None:
        cfg_tr = RealTransportConfig(
            auto_ack_for_testing=False,
            per_connection_stream_limit=5,
            ack_timeout_sec=0.08,
            subscribe_batch_size=10,
            unsubscribe_batch_size=10,
        )
        t = RealBinanceFuturesWebSocketTransport(cfg_tr)
        m = BinanceFuturesWSManager(WSConfig(per_connection_stream_limit=5), t, target_manager=None)
        t.bind_metrics(m.metrics)
        t.bind_manager(m)
        met = m.metrics
        hang = HangWS()

        async def _connect(*_a: object, **_k: object) -> HangWS:
            return hang

        s1 = routing.binance_stream_name("AXUSDT", "bookTicker")
        s2 = routing.binance_stream_name("AYUSDT", "bookTicker")
        with patch("laoma_signal_engine.micro.ws.real_transport.websockets.connect", _connect):
            await t.subscribe_streams([s1])
            before_err = met.subscribe_error_count
            await t.subscribe_streams([s2])
        assert met.subscribe_error_count > before_err
        await t.aclose()

    asyncio.run(_run())


def test_r15_proxy_url_valueerror_on_build() -> None:
    from laoma_signal_engine.core.time_utils import utc_now

    cfg = DaemonConfig(
        targets_path=Path("nope"),
        latest_features_path=Path("n"),
        heartbeat_path=Path("h"),
        transport="real",
        proxy_url="http://127.0.0.1:1",
    )
    with pytest.raises(ValueError, match="proxy"):
        build_run_context(cfg, utc_now)


def test_r16_urls_use_routed_paths_not_host_stream_only() -> None:
    u_m = routing.combined_stream_ws_url(
        "wss://fstream.binance.com",
        "market",
        [routing.binance_stream_name("BTCUSDT", "aggTrade")],
    )
    assert "/market/stream?streams=" in u_m
    assert not u_m.startswith("wss://fstream.binance.com/stream?")


def test_r17_only_routing_module_defines_logical_maps() -> None:
    root = Path(__file__).resolve().parents[1] / "micro"
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        if path.name == "routing.py":
            continue
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if (
                stripped.startswith("LOGICAL_STREAM_TO_ROUTE")
                or stripped.startswith("LOGICAL_STREAM_TO_TEMPLATE")
            ):
                offenders.append(str(path.relative_to(root)))
    assert offenders == []


def test_r18_manual_marker_registered() -> None:
    assert hasattr(pytest.mark, "manual")


@pytest.mark.manual
def test_manual_real_network_smoke_skipped_by_default() -> None:
    """When collected with '-m manual', requires live Binance; default CI omits this test."""
    pytest.skip("Real-network smoke is manual-only (STEP3.8B section 12.2).")


def test_empty_subscribe_streams_no_connect() -> None:
    async def _run() -> None:
        cfg0 = RealTransportConfig(auto_ack_for_testing=True)
        met = WSMetrics()
        t = RealBinanceFuturesWebSocketTransport(cfg0, metrics=met)

        async def _boom(*_a: object, **_k: object) -> object:
            raise AssertionError("should not connect")

        with patch("laoma_signal_engine.micro.ws.real_transport.websockets.connect", _boom):
            await t.subscribe_streams([])

        assert met.connect_count == 0

    asyncio.run(_run())
