"""Binance Futures WS subscription manager. docs/STEP3.3_任务卡.md."""

from __future__ import annotations

import json
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Protocol

from laoma_signal_engine.micro.adapters.binance_agg_trade import normalize_agg_trade
from laoma_signal_engine.micro.adapters.binance_book import (
    normalize_book_ticker,
    normalize_partial_depth5,
)
from laoma_signal_engine.micro.normalized_models import NormalizedBook, NormalizedTrade
from laoma_signal_engine.micro.target_intent_models import RetireIntent, SubscribeIntent
from laoma_signal_engine.micro.ws import routing

ALLOWED_LOGICAL_STREAMS: frozenset[str] = frozenset(routing.LOGICAL_STREAM_TO_ROUTE.keys())


class SupportsTargetAck(Protocol):
    def mark_subscribed(self, symbol: str) -> None: ...

    def mark_unsubscribed(self, symbol: str) -> None: ...


def binance_stream_name(symbol_upper: str, logical: str) -> str:
    return routing.binance_stream_name(symbol_upper, logical)


def parse_binance_combined_stream(binance_stream: str) -> tuple[str, str] | None:
    return routing.parse_binance_combined_stream(binance_stream)


def partition_streams(streams: list[str], limit: int) -> list[list[str]]:
    return routing.partition_sorted_streams(streams, limit)


@dataclass(frozen=True)
class WSEventEnvelope:
    symbol: str
    stream_type: str
    event_ts_ms: int
    recv_ts_ms: int
    normalized: NormalizedTrade | NormalizedBook


@dataclass
class WSConfig:
    base_ws_url: str = "wss://fstream.binance.com/stream"
    event_queue_max_size: int = 50000
    per_connection_stream_limit: int = 80
    reconnect_backoff_sec: float = 1.0
    reconnect_backoff_max_sec: float = 30.0


@dataclass
class WSMetrics:
    enqueued_events_trade: int = 0
    enqueued_events_book: int = 0
    enqueued_events_depth: int = 0
    drained_events_trade: int = 0
    drained_events_book: int = 0
    drained_events_depth: int = 0
    dropped_events_trade: int = 0
    dropped_events_book: int = 0
    dropped_events_depth: int = 0
    book_replaced_count: int = 0
    depth_replaced_count: int = 0
    book_latest_state_overwrite_count: int = 0
    depth_bucket_coalesced_count: int = 0
    normalize_errors_trade: int = 0
    normalize_errors_book: int = 0
    normalize_errors_depth: int = 0
    invalid_stream: list[str] = field(default_factory=list)
    event_queue_overflow: bool = False
    subscribe_error_count: int = 0
    unsubscribe_error_count: int = 0
    reconnect_count: int = 0
    connect_count: int = 0
    last_ws_recv_ts_ms: int | None = None


@dataclass(frozen=True)
class SyncResult:
    subscribed_stream_chunks: int
    unsubscribed_stream_chunks: int
    streams_subscribed_total: int
    streams_unsubscribed_total: int


class WSTransport(Protocol):
    @property
    def active_streams(self) -> set[str]: ...

    async def subscribe_streams(self, streams: list[str]) -> None: ...

    async def unsubscribe_streams(self, streams: list[str]) -> None: ...


class FakeWebSocketTransport:
    """Test double: records subscribe/unsubscribe and tracks active_streams."""

    def __init__(
        self,
        *,
        auto_ack: bool = True,
        failed_subscribe: set[str] | None = None,
    ) -> None:
        self._active: set[str] = set()
        self.auto_ack = auto_ack
        self.failed_subscribe: set[str] = set(failed_subscribe or ())
        self.subscribe_calls: list[list[str]] = []
        self.unsubscribe_calls: list[list[str]] = []

    @property
    def active_streams(self) -> set[str]:
        return set(self._active)

    def clear_all(self) -> None:
        self._active.clear()

    async def subscribe_streams(self, streams: list[str]) -> None:
        self.subscribe_calls.append(list(streams))
        if self.auto_ack:
            for s in streams:
                if s not in self.failed_subscribe:
                    self._active.add(s)

    async def unsubscribe_streams(self, streams: list[str]) -> None:
        self.unsubscribe_calls.append(list(streams))
        if self.auto_ack:
            for s in streams:
                self._active.discard(s)


def _norm_err_metrics_key(stream_type: str) -> str:
    if stream_type == "aggTrade":
        return "trade"
    if stream_type == "bookTicker":
        return "book"
    return "depth"


class BinanceFuturesWSManager:
    """Asyncio-oriented WS subscription orchestration (transport-injected)."""

    def __init__(
        self,
        config: WSConfig,
        transport: WSTransport,
        target_manager: SupportsTargetAck | None = None,
    ) -> None:
        self._config = config
        self._transport = transport
        self._tm = target_manager
        self._stream_meta: dict[str, tuple[str, str]] = {}
        self._trade_queue: deque[WSEventEnvelope] = deque()
        self._latest_book: dict[str, WSEventEnvelope] = {}
        self._latest_depth: dict[str, WSEventEnvelope] = {}
        self._last_event_ts_ms: dict[tuple[str, str], int] = {}
        self._last_ack_ts_ms: dict[tuple[str, str], int] = {}
        self.metrics = WSMetrics()
        self._marked_subscribed_syms: set[str] = set()
        self._unsub_ack_for_retire: set[str] = set()

    def drain_events(self) -> list[WSEventEnvelope]:
        out: list[WSEventEnvelope] = []
        while self._trade_queue:
            out.append(self._trade_queue.popleft())
        books = sorted(self._latest_book.values(), key=lambda e: e.symbol)
        depths = sorted(self._latest_depth.values(), key=lambda e: e.symbol)
        out.extend(books)
        out.extend(depths)
        self.metrics.drained_events_trade += len(out) - len(books) - len(depths)
        self.metrics.drained_events_book += len(books)
        self.metrics.drained_events_depth += len(depths)
        return out

    def apply_reconnect_reset(self) -> None:
        if hasattr(self._transport, "clear_all"):
            self._transport.clear_all()

    async def sync(
        self,
        subscribe_intents: list[SubscribeIntent],
        retire_intents: list[RetireIntent],
    ) -> SyncResult:
        desired, per_symbol_required, invalid = self._build_desired(subscribe_intents)
        for inv in invalid:
            self.metrics.invalid_stream.append(inv)

        for sym in per_symbol_required:
            self._unsub_ack_for_retire.discard(sym)

        retire_syms = {r.symbol.strip().upper() for r in retire_intents}
        active = self._transport.active_streams
        to_unsub = sorted(
            s
            for s in active
            if s in self._stream_meta and self._stream_meta[s][0] in retire_syms
        )
        to_sub = sorted(desired - active)

        unsub_total = 0
        sub_total = 0
        unsub_chunks = 0
        sub_chunks = 0

        for chunk in partition_streams(to_unsub, self._config.per_connection_stream_limit):
            await self._transport.unsubscribe_streams(chunk)
            unsub_chunks += 1
            unsub_total += len(chunk)

        for chunk in partition_streams(to_sub, self._config.per_connection_stream_limit):
            await self._transport.subscribe_streams(chunk)
            sub_chunks += 1
            sub_total += len(chunk)

        self._record_active_ack(per_symbol_required)
        self._try_mark_subscribed(per_symbol_required)
        self._try_mark_unsubscribed(retire_syms)

        return SyncResult(
            subscribed_stream_chunks=sub_chunks,
            unsubscribed_stream_chunks=unsub_chunks,
            streams_subscribed_total=sub_total,
            streams_unsubscribed_total=unsub_total,
        )

    def _build_desired(
        self, subscribe_intents: list[SubscribeIntent]
    ) -> tuple[set[str], dict[str, set[str]], list[str]]:
        desired: set[str] = set()
        per_symbol: dict[str, set[str]] = defaultdict(set)
        invalid: list[str] = []
        for intent in subscribe_intents:
            su = intent.symbol.strip().upper()
            for logical in intent.streams:
                if logical not in ALLOWED_LOGICAL_STREAMS:
                    invalid.append(f"{su}:{logical}")
                    continue
                bn = binance_stream_name(su, logical)
                desired.add(bn)
                per_symbol[su].add(bn)
                self._stream_meta[bn] = (su, logical)
        return desired, dict(per_symbol), invalid

    def _active_streams_for_symbol(self, sym: str) -> set[str]:
        active = self._transport.active_streams
        return {
            b
            for b in active
            if b in self._stream_meta and self._stream_meta[b][0] == sym
        }

    def _record_active_ack(self, per_symbol_required: dict[str, set[str]]) -> None:
        active = self._transport.active_streams
        for _sym, required in per_symbol_required.items():
            for stream in required:
                if stream not in active or stream not in self._stream_meta:
                    continue
                sym, logical = self._stream_meta[stream]
                self._last_ack_ts_ms.setdefault((sym, logical), self.metrics.last_ws_recv_ts_ms or 0)

    def subscription_state_for_intents(self, intents: list[SubscribeIntent]) -> dict[str, dict[str, dict[str, Any]]]:
        desired, per_symbol_required, _invalid = self._build_desired(intents)
        active = self._transport.active_streams
        out: dict[str, dict[str, dict[str, Any]]] = {}
        logical_streams = ("aggTrade", "bookTicker", "partialDepth5")
        for intent in intents:
            sym = intent.symbol.strip().upper()
            required_logicals = set(intent.streams)
            state: dict[str, dict[str, Any]] = {}
            for logical in logical_streams:
                required = logical in required_logicals
                desired_stream = binance_stream_name(sym, logical)
                desired_flag = desired_stream in desired
                active_flag = desired_stream in active
                last_event = self._last_event_ts_ms.get((sym, logical))
                last_ack = self._last_ack_ts_ms.get((sym, logical))
                missing_reason = None
                if not required:
                    missing_reason = "not_required_for_tier"
                elif not active_flag:
                    missing_reason = f"subscription_missing_{logical}"
                elif last_event is None:
                    missing_reason = "no_events_yet"
                state[logical] = {
                    "required": required,
                    "desired": desired_flag,
                    "active": active_flag,
                    "last_event_ts_sec": None if last_event is None else int(last_event // 1000),
                    "last_ack_ts_sec": None if not last_ack else int(last_ack // 1000),
                    "missing_reason": missing_reason,
                }
            out[sym] = state
        return out

    def _try_mark_subscribed(self, per_symbol_required: dict[str, set[str]]) -> None:
        if self._tm is None:
            return
        for sym, required in per_symbol_required.items():
            if sym in self._marked_subscribed_syms:
                continue
            have = self._active_streams_for_symbol(sym)
            if required <= have:
                self._tm.mark_subscribed(sym)
                self._marked_subscribed_syms.add(sym)

    def _try_mark_unsubscribed(self, retire_syms: set[str]) -> None:
        if self._tm is None:
            return
        for sym in retire_syms:
            if self._active_streams_for_symbol(sym):
                continue
            if sym in self._unsub_ack_for_retire:
                continue
            self._tm.mark_unsubscribed(sym)
            self._marked_subscribed_syms.discard(sym)
            self._unsub_ack_for_retire.add(sym)

    def handle_raw_message(self, raw: str, recv_ts_ms: int) -> None:
        try:
            obj: Any = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(obj, dict):
            return
        bstream = obj.get("stream")
        data = obj.get("data")
        if not isinstance(bstream, str) or not isinstance(data, dict):
            return
        parsed = parse_binance_combined_stream(bstream)
        if parsed is None:
            return
        sym_upper, stream_type = parsed
        if bstream not in self._transport.active_streams:
            return

        self.metrics.last_ws_recv_ts_ms = recv_ts_ms

        try:
            env = self._normalize_to_envelope(sym_upper, stream_type, data, recv_ts_ms)
        except (TypeError, ValueError):
            key = _norm_err_metrics_key(stream_type)
            if key == "trade":
                self.metrics.normalize_errors_trade += 1
            elif key == "book":
                self.metrics.normalize_errors_book += 1
            else:
                self.metrics.normalize_errors_depth += 1
            return

        if stream_type == "aggTrade":
            self._last_event_ts_ms[(sym_upper, "aggTrade")] = env.event_ts_ms
            self._push_trade(env)
        elif stream_type == "bookTicker":
            self._last_event_ts_ms[(sym_upper, "bookTicker")] = env.event_ts_ms
            self._push_book(env, sym_upper)
        else:
            self._last_event_ts_ms[(sym_upper, "partialDepth5")] = env.event_ts_ms
            self._push_depth(env, sym_upper)

    def _normalize_to_envelope(
        self,
        sym_upper: str,
        stream_type: str,
        data: dict[str, Any],
        recv_ts_ms: int,
    ) -> WSEventEnvelope:
        if stream_type == "aggTrade":
            nt = normalize_agg_trade(data)
            return WSEventEnvelope(
                symbol=nt.symbol,
                stream_type="aggTrade",
                event_ts_ms=nt.ts_ms,
                recv_ts_ms=recv_ts_ms,
                normalized=nt,
            )
        if stream_type == "bookTicker":
            nb = normalize_book_ticker(data)
            return WSEventEnvelope(
                symbol=nb.symbol,
                stream_type="bookTicker",
                event_ts_ms=nb.ts_ms,
                recv_ts_ms=recv_ts_ms,
                normalized=nb,
            )
        nb = normalize_partial_depth5(data, symbol_if_missing=sym_upper)
        return WSEventEnvelope(
            symbol=nb.symbol,
            stream_type="partialDepth5",
            event_ts_ms=nb.ts_ms,
            recv_ts_ms=recv_ts_ms,
            normalized=nb,
        )

    def _push_trade(self, env: WSEventEnvelope) -> None:
        max_sz = self._config.event_queue_max_size
        while len(self._trade_queue) >= max_sz:
            self._trade_queue.popleft()
            self.metrics.dropped_events_trade += 1
            self.metrics.event_queue_overflow = True
        self._trade_queue.append(env)
        self.metrics.enqueued_events_trade += 1

    def _push_book(self, env: WSEventEnvelope, sym_upper: str) -> None:
        if sym_upper in self._latest_book:
            self.metrics.book_replaced_count += 1
            self.metrics.book_latest_state_overwrite_count += 1
        self._latest_book[sym_upper] = env
        self.metrics.enqueued_events_book += 1

    def _push_depth(self, env: WSEventEnvelope, sym_upper: str) -> None:
        if sym_upper in self._latest_depth:
            self.metrics.depth_replaced_count += 1
            self.metrics.depth_bucket_coalesced_count += 1
        self._latest_depth[sym_upper] = env
        self.metrics.enqueued_events_depth += 1
