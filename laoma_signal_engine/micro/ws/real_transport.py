"""Real Binance USD-M Futures WebSocket transport (STEP3.8B). docs/STEP3.8B_Real_Binance_WS_任务卡.md."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import websockets

from laoma_signal_engine.micro.ws import routing

if TYPE_CHECKING:
    from laoma_signal_engine.micro.ws.subscription_manager import (
        BinanceFuturesWSManager,
        WSMetrics,
    )


@dataclass
class RealTransportConfig:
    base_url: str = "wss://fstream.binance.com"
    public_path: str = "/public"
    market_path: str = "/market"
    per_connection_stream_limit: int = 80
    subscribe_batch_size: int = 50
    unsubscribe_batch_size: int = 50
    control_msg_rate_limit_per_sec: float = 5.0
    ack_timeout_sec: float = 10.0
    connect_timeout_sec: float = 10.0
    auto_ack_for_testing: bool = False


class _ControlRateLimiter:
    def __init__(self, per_sec: float) -> None:
        self._interval = 1.0 / per_sec if per_sec > 0 else 0.0
        self._next_ts = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if self._interval <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            if now < self._next_ts:
                await asyncio.sleep(self._next_ts - now)
            self._next_ts = time.monotonic() + self._interval


@dataclass
class _Shard:
    route: str
    shard_id: int
    active: set[str] = field(default_factory=set)
    ws: Any | None = None
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    next_req_id: int = 1
    pending: dict[int, tuple[list[str], str]] = field(default_factory=dict)
    ack_events: dict[int, asyncio.Event] = field(default_factory=dict)
    pump_task: asyncio.Task[Any] | None = None


class RealBinanceFuturesWebSocketTransport:
    """Splits streams across public/market; combined URL on connect; JSON SUB/UNSUB with batched ACK."""

    def __init__(self, config: RealTransportConfig, metrics: WSMetrics | None = None) -> None:
        self._cfg = config
        self._metrics = metrics
        self._shards: list[_Shard] = []
        self._route_shard_seq: dict[str, int] = {"public": 0, "market": 0}
        self._limiter = _ControlRateLimiter(config.control_msg_rate_limit_per_sec)
        self._active_union: set[str] = set()
        self._mgr: BinanceFuturesWSManager | None = None
        self._stop = asyncio.Event()

    def bind_metrics(self, metrics: WSMetrics) -> None:
        self._metrics = metrics

    def bind_manager(self, manager: BinanceFuturesWSManager) -> None:
        self._mgr = manager

    @property
    def active_streams(self) -> set[str]:
        return set(self._active_union)

    def has_live_connections(self) -> bool:
        return any(sh.ws is not None for sh in self._shards)

    def clear_all(self) -> None:
        if self._metrics and self._shards:
            self._metrics.reconnect_count += 1
        self._stop.set()
        for sh in self._shards:
            if sh.pump_task is not None:
                sh.pump_task.cancel()
                sh.pump_task = None
            if sh.ws is not None:
                ws = sh.ws
                sh.ws = None
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(ws.close())
                except RuntimeError:
                    pass
            sh.active.clear()
            sh.pending.clear()
            sh.ack_events.clear()
        self._shards.clear()
        self._active_union.clear()
        self._stop = asyncio.Event()

    async def aclose(self) -> None:
        self._stop.set()
        for sh in self._shards:
            if sh.pump_task is not None:
                sh.pump_task.cancel()
                try:
                    await sh.pump_task
                except asyncio.CancelledError:
                    pass
                sh.pump_task = None
            if sh.ws is not None:
                try:
                    await sh.ws.close()
                except (
                    OSError,
                    asyncio.CancelledError,
                    websockets.exceptions.WebSocketException,
                ):
                    pass
                sh.ws = None
            sh.active.clear()
            sh.pending.clear()
            sh.ack_events.clear()
        self._shards.clear()
        self._active_union.clear()

    async def subscribe_streams(self, streams: list[str]) -> None:
        if not streams:
            return
        grouped, invalid = routing.group_streams_by_route(streams)
        if self._metrics:
            self._metrics.invalid_stream.extend(invalid)
        for route in ("market", "public"):
            add_list = grouped[route]
            if not add_list:
                continue
            await self._subscribe_route_streams(route, add_list)

    async def _subscribe_route_streams(self, route: str, add_list: list[str]) -> None:
        remaining = list(add_list)
        while remaining:
            shard = self._find_shard_with_space(route)
            space = self._cfg.per_connection_stream_limit - len(shard.active) if shard else 0
            if shard and shard.ws and space > 0:
                take = remaining[:space]
                del remaining[: len(take)]
                await self._json_subscribe_batches(shard, take)
            else:
                take = remaining[: self._cfg.per_connection_stream_limit]
                del remaining[: len(take)]
                new_sh = self._new_shard(route)
                await self._connect_shard_initial(new_sh, take)

    def _new_shard(self, route: str) -> _Shard:
        self._route_shard_seq[route] += 1
        sid = self._route_shard_seq[route]
        sh = _Shard(route=route, shard_id=sid)
        self._shards.append(sh)
        return sh

    def _find_shard_with_space(self, route: str) -> _Shard | None:
        for sh in self._shards:
            if sh.route != route or sh.ws is None:
                continue
            if len(sh.active) < self._cfg.per_connection_stream_limit:
                return sh
        return None

    async def _connect_shard_initial(self, shard: _Shard, initial: list[str]) -> None:
        if not initial:
            return
        if self._metrics:
            self._metrics.connect_count += 1
        url = routing.combined_stream_ws_url(
            self._cfg.base_url,
            shard.route,
            sorted(initial),
            public_path=self._cfg.public_path,
            market_path=self._cfg.market_path,
        )
        shard.ws = await websockets.connect(
            uri=url,
            open_timeout=self._cfg.connect_timeout_sec,
            ping_interval=20,
            ping_timeout=60,
            close_timeout=10,
        )
        shard.active.update(initial)
        self._active_union.update(initial)
        if not self._cfg.auto_ack_for_testing:
            if self._mgr is None:
                msg = "real transport requires bind_manager before subscribe when not in test mode"
                raise RuntimeError(msg)
            shard.pump_task = asyncio.create_task(self._pump_shard(shard))

    async def _json_subscribe_batches(self, shard: _Shard, streams: list[str]) -> None:
        if not streams or shard.ws is None:
            return
        for batch in routing.partition_sorted_streams(streams, self._cfg.subscribe_batch_size):
            await self._limiter.acquire()
            req_id = shard.next_req_id
            shard.next_req_id += 1
            payload = {"method": "SUBSCRIBE", "params": list(batch), "id": req_id}
            shard.pending[req_id] = (list(batch), "sub")
            raw = json.dumps(payload, ensure_ascii=True)
            ev = asyncio.Event()
            shard.ack_events[req_id] = ev
            async with shard.send_lock:
                await shard.ws.send(raw)
            if self._cfg.auto_ack_for_testing:
                await self._apply_ack(shard, req_id, ok=True)
                ev.set()
            else:
                try:
                    await asyncio.wait_for(ev.wait(), timeout=self._cfg.ack_timeout_sec)
                except TimeoutError:
                    await self._apply_ack(shard, req_id, ok=False)
                finally:
                    shard.ack_events.pop(req_id, None)

    async def _json_unsubscribe_batches(self, shard: _Shard, streams: list[str]) -> None:
        if not streams or shard.ws is None:
            return
        for batch in routing.partition_sorted_streams(streams, self._cfg.unsubscribe_batch_size):
            await self._limiter.acquire()
            req_id = shard.next_req_id
            shard.next_req_id += 1
            payload = {"method": "UNSUBSCRIBE", "params": list(batch), "id": req_id}
            shard.pending[req_id] = (list(batch), "unsub")
            raw = json.dumps(payload, ensure_ascii=True)
            ev = asyncio.Event()
            shard.ack_events[req_id] = ev
            async with shard.send_lock:
                await shard.ws.send(raw)
            if self._cfg.auto_ack_for_testing:
                await self._apply_ack(shard, req_id, ok=True)
                ev.set()
            else:
                try:
                    await asyncio.wait_for(ev.wait(), timeout=self._cfg.ack_timeout_sec)
                except TimeoutError:
                    await self._apply_ack(shard, req_id, ok=False)
                finally:
                    shard.ack_events.pop(req_id, None)

    async def _pump_shard(self, shard: _Shard) -> None:
        if shard.ws is None:
            return
        while not self._stop.is_set():
            try:
                raw = await asyncio.wait_for(shard.ws.recv(), timeout=60.0)
            except TimeoutError:
                continue
            except (asyncio.CancelledError, websockets.exceptions.ConnectionClosed):
                break
            except OSError:
                break
            if not isinstance(raw, str):
                continue
            ok, rid = _parse_control_response(raw)
            if rid is not None:
                handled = await self._apply_ack(shard, rid, ok=ok if ok is not None else False)
                ev = shard.ack_events.pop(rid, None)
                if ev is not None:
                    ev.set()
                if not handled:
                    pass
                continue
            if self._mgr is not None:
                recv_ts = int(time.time() * 1000)
                self._mgr.handle_raw_message(raw, recv_ts)

    async def unsubscribe_streams(self, streams: list[str]) -> None:
        if not streams:
            return
        by_shard: dict[int, list[str]] = {}
        for s in streams:
            for i, sh in enumerate(self._shards):
                if s in sh.active and sh.ws is not None:
                    by_shard.setdefault(i, []).append(s)
                    break
        for idx, lst in sorted(by_shard.items()):
            sh = self._shards[idx]
            await self._json_unsubscribe_batches(sh, sorted(lst))

    async def _apply_ack(self, shard: _Shard, req_id: int, *, ok: bool) -> bool:
        pending = shard.pending.pop(req_id, None)
        if pending is None:
            return False
        streams, kind = pending
        m = self._metrics
        if ok:
            if kind == "sub":
                shard.active.update(streams)
                self._active_union.update(streams)
            else:
                for s in streams:
                    shard.active.discard(s)
                    self._active_union.discard(s)
        elif m is not None:
            if kind == "sub":
                m.subscribe_error_count += 1
            else:
                m.unsubscribe_error_count += 1
        return True


def _parse_control_response(raw: str) -> tuple[bool | None, int | None]:
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return None, None
    if not isinstance(obj, dict):
        return None, None
    rid = obj.get("id")
    if not isinstance(rid, int):
        return None, None
    if "error" in obj:
        return False, rid
    if obj.get("result") is None:
        return True, rid
    return True, rid
