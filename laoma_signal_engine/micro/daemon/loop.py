"""Daemon main loop and --once orchestration. docs/STEP3.8_任务卡.md."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Literal

from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.micro.assembly import AssemblyTargetRow, atomic_write_json, build_document
from laoma_signal_engine.micro.assembly.models import LatestMicroStatus, TargetStatus
from laoma_signal_engine.micro.bucket.bucket_aggregator import (
    BucketAggregator,
    BucketConfig,
    CoverageStreamType,
)
from laoma_signal_engine.micro.daemon.config import DaemonConfig
from laoma_signal_engine.micro.daemon.fixture_envelopes import load_fixture_envelopes
from laoma_signal_engine.micro.daemon.heartbeat import (
    HeartbeatDroppedEvents,
    MicroCollectorHeartbeat,
    atomic_write_heartbeat,
)
from laoma_signal_engine.micro.daemon.health_guard import MicroRuntimeHealthGuard
from laoma_signal_engine.micro.runtime_logging import file_size_or_none
from laoma_signal_engine.micro.factor_frame_store import (
    default_micro_factor_db,
    infer_project_root_from_output,
    ingest_micro_factor_frames,
)
from laoma_signal_engine.micro.daemon.state_writer import (
    atomic_write_micro_state,
    build_micro_daemon_state_document,
)
from laoma_signal_engine.micro.quality.models import (
    MicroQualityConfig,
    MicroQualitySnapshot,
    SymbolQualityInput,
    WSQualitySignal,
)
from laoma_signal_engine.micro.quality.quality_gate import CoverageProvider, MicroQualityGate
from laoma_signal_engine.micro.realtime.cvd_ofi_driver import RealtimeCvdOfiDriver
from laoma_signal_engine.micro.target_intent_models import SubscribeIntent, TargetManagerSettings
from laoma_signal_engine.micro.target_manager import MicroTargetManager
from laoma_signal_engine.micro.ws.real_transport import (
    RealBinanceFuturesWebSocketTransport,
    RealTransportConfig,
)
from laoma_signal_engine.micro.ws.subscription_manager import (
    BinanceFuturesWSManager,
    FakeWebSocketTransport,
    WSConfig,
    WSMetrics,
)


_VALID_TARGET_STATUS: frozenset[str] = frozenset(
    {"fresh", "stale", "stale_observing", "invalid_targets", "unknown"},
)


@dataclass(frozen=True)
class RunOnceResult:
    exit_code: int
    last_error: str | None
    wrote_latest: bool


@dataclass
class DaemonRunContext:
    config: DaemonConfig
    ack_bridge: CollectStartedAckBridge
    ws_manager: BinanceFuturesWSManager
    aggregator: BucketAggregator
    driver: RealtimeCvdOfiDriver
    quality_gate: MicroQualityGate
    fast_quality_gate: MicroQualityGate
    health_guard: MicroRuntimeHealthGuard
    process_start: datetime
    fixture_events_path: Path | None = None

    async def aclose_optional_real(self) -> None:
        t = self.ws_manager._transport  # noqa: SLF001
        if isinstance(t, RealBinanceFuturesWebSocketTransport):
            await t.aclose()


class CollectStartedAckBridge:
    """Tracks collect_started_ts_sec on mark_subscribed; clears on mark_unsubscribed."""

    def __init__(self, manager: MicroTargetManager, now_fn: Callable[[], datetime]) -> None:
        self._manager = manager
        self._now_fn = now_fn
        self._started: dict[str, int] = {}

    @property
    def manager(self) -> MicroTargetManager:
        return self._manager

    def mark_subscribed(self, symbol: str) -> None:
        sym = symbol.strip().upper()
        if sym not in self._started:
            self._started[sym] = int(self._now_fn().timestamp())
        self._manager.mark_subscribed(symbol)

    def mark_unsubscribed(self, symbol: str) -> None:
        sym = symbol.strip().upper()
        self._started.pop(sym, None)
        self._manager.mark_unsubscribed(symbol)

    def collect_started_ts_sec(self, symbol: str) -> int | None:
        return self._started.get(symbol.strip().upper())


class _AggregatorCoverage(CoverageProvider):
    def __init__(self, agg: BucketAggregator) -> None:
        self._agg = agg

    def get_coverage(
        self,
        symbol: str,
        stream_type: CoverageStreamType,
        end_ts_sec: int,
        window_sec: int,
    ):
        return self._agg.get_coverage(symbol, stream_type, end_ts_sec, window_sec)


def coerce_target_status(raw: str) -> TargetStatus:
    t = raw.strip()
    if t in _VALID_TARGET_STATUS:
        return t  # type: ignore[return-value]
    return "unknown"


def document_status_for(ts: TargetStatus, n_items: int) -> LatestMicroStatus:
    if ts == "fresh":
        return "ok" if n_items > 0 else "no_targets"
    if ts == "stale_observing":
        return "observing_stale_targets" if n_items > 0 else "stale_targets"
    if ts == "stale":
        return "stale_targets"
    return "error"


def ofi_levels_for_tier(tier_key: str) -> Literal[1, 5]:
    if tier_key == "tier1_warm_watch":
        return 1
    return 5


def assembly_rows_from_intents(intents: list[SubscribeIntent]) -> list[AssemblyTargetRow]:
    rows: list[AssemblyTargetRow] = []
    for it in intents:
        rows.append(
            AssemblyTargetRow(
                symbol=it.symbol.strip().upper(),
                ofi_levels=ofi_levels_for_tier(it.tier_key),
                tier=it.tier_key,
                symbol_safe_id=it.symbol_safe_id,
                source_state=it.source_state,
                move_side=it.move_side,
                priority=it.priority,
                scan_score=it.scan_score,
                trigger_type=it.trigger_type,
            ),
        )
    return rows


def _ws_observability_fields(
    ctx: DaemonRunContext,
    reference_ts_sec: int,
) -> tuple[bool, float | None, str, int | None]:
    """Heartbeat + latest doc: connected flag, age (float for hb, int for doc), ws_status."""
    t = ctx.ws_manager._transport
    m = ctx.ws_manager.metrics
    last_recv = m.last_ws_recv_ts_ms
    if isinstance(t, RealBinanceFuturesWebSocketTransport):
        ws_connected = t.has_live_connections()
        ws_status = "connected" if ws_connected else "disconnected"
    else:
        ws_connected = True
        ws_status = "fake"
    doc_age: int | None = None
    hb_age: float | None = None
    if last_recv is not None:
        age_sec = max(0.0, (reference_ts_sec * 1000 - last_recv) / 1000.0)
        doc_age = int(age_sec)
        hb_age = age_sec
    return ws_connected, hb_age, ws_status, doc_age


def ws_signal_from_metrics(m: WSMetrics) -> WSQualitySignal:
    ofi_backpressure_state = "ok"
    if m.event_queue_overflow:
        ofi_backpressure_state = "critical"
    elif m.dropped_events_book > 0 or m.dropped_events_depth > 0:
        ofi_backpressure_state = "degraded"
    return WSQualitySignal(
        event_queue_overflow_recent=m.event_queue_overflow,
        dropped_trade_delta=0,
        dropped_book_delta=m.dropped_events_book,
        dropped_depth_delta=m.dropped_events_depth,
        ofi_backpressure_state=ofi_backpressure_state,  # type: ignore[arg-type]
    )


def default_fast_micro_quality_config() -> MicroQualityConfig:
    return MicroQualityConfig(
        window_sec=180,
        min_ready_seconds=90,
        aggtrade_coverage_min=0.35,
        bookticker_coverage_min=0.45,
        depth5_coverage_min=0.45,
        max_stale_sec=5,
        max_lag_sec=3,
        event_queue_overflow_hard_fail=True,
        adapter_error_hard_fail=True,
    )


def read_micro_targets_generated_at(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        raw = read_json_object(path)
        if isinstance(raw, dict):
            g = raw.get("generated_at")
            if isinstance(g, str) and g.strip():
                return g.strip()
    except (OSError, ValueError, TypeError):
        return None
    return None


def managed_symbol_count(tm: MicroTargetManager) -> int:
    return len(tm._managed)  # noqa: SLF001


def latest_state_path_for_config(cfg: DaemonConfig) -> Path:
    if cfg.latest_state_path is not None:
        return cfg.latest_state_path
    return cfg.latest_features_path.with_name("latest_micro_state.json")


def _persist_micro_factor_frames(ctx: DaemonRunContext, doc: object) -> None:
    if not hasattr(doc, "items"):
        return
    root = infer_project_root_from_output(ctx.config.latest_features_path)
    ingest_micro_factor_frames(doc, db_path=default_micro_factor_db(root))  # type: ignore[arg-type]


def build_run_context(
    cfg: DaemonConfig,
    now_fn: Callable[[], datetime],
    *,
    fixture_events_path: Path | None = None,
    quality_config: MicroQualityConfig | None = None,
    fast_quality_config: MicroQualityConfig | None = None,
) -> DaemonRunContext:
    if cfg.transport == "real" and cfg.proxy_url not in (None, ""):
        msg = "proxy_url is set but MVP real transport does not implement proxy (STEP3.8B)"
        raise ValueError(msg)

    settings = TargetManagerSettings(
        target_stale_sec=cfg.target_stale_sec,
        unsubscribe_grace_sec=cfg.unsubscribe_grace_sec,
        max_managed_symbols=cfg.max_managed_symbols,
    )
    tm = MicroTargetManager(cfg.targets_path, settings, now_fn=now_fn)
    bridge = CollectStartedAckBridge(tm, now_fn)
    ws_cfg = WSConfig(per_connection_stream_limit=cfg.ws_per_connection_stream_limit)
    if cfg.transport == "fake":
        transport: FakeWebSocketTransport | RealBinanceFuturesWebSocketTransport = FakeWebSocketTransport()
    else:
        rtc = RealTransportConfig(
            base_url=cfg.real_base_url,
            public_path=cfg.real_public_path,
            market_path=cfg.real_market_path,
            per_connection_stream_limit=cfg.ws_per_connection_stream_limit,
            subscribe_batch_size=cfg.subscribe_batch_size,
            unsubscribe_batch_size=cfg.unsubscribe_batch_size,
            control_msg_rate_limit_per_sec=cfg.control_msg_rate_limit_per_sec,
            ack_timeout_sec=cfg.ack_timeout_sec,
            connect_timeout_sec=cfg.real_connect_timeout_sec,
            auto_ack_for_testing=False,
        )
        transport = RealBinanceFuturesWebSocketTransport(rtc)
    ws = BinanceFuturesWSManager(ws_cfg, transport, target_manager=bridge)
    if isinstance(transport, RealBinanceFuturesWebSocketTransport):
        transport.bind_metrics(ws.metrics)
        transport.bind_manager(ws)
    agg = BucketAggregator(BucketConfig(ring_buffer_seconds=cfg.ring_buffer_seconds))
    driver = RealtimeCvdOfiDriver()
    qcfg = quality_config or MicroQualityConfig()
    gate = MicroQualityGate(qcfg, _AggregatorCoverage(agg), driver)
    fast_qcfg = fast_quality_config or (quality_config if quality_config is not None else default_fast_micro_quality_config())
    fast_gate = MicroQualityGate(fast_qcfg, _AggregatorCoverage(agg), driver)
    return DaemonRunContext(
        config=cfg,
        ack_bridge=bridge,
        ws_manager=ws,
        aggregator=agg,
        driver=driver,
        quality_gate=gate,
        fast_quality_gate=fast_gate,
        health_guard=MicroRuntimeHealthGuard(),
        process_start=utc_now(),
        fixture_events_path=fixture_events_path,
    )


def _apply_buckets_for_intents(
    agg: BucketAggregator,
    driver: RealtimeCvdOfiDriver,
    intents: list[SubscribeIntent],
    reference_ts_sec: int,
) -> None:
    w = agg.config.ring_buffer_seconds
    start = max(0, reference_ts_sec - w)
    end = reference_ts_sec + 2
    for it in intents:
        sym = it.symbol.strip().upper()
        ol = ofi_levels_for_tier(it.tier_key)
        try:
            driver.register_symbol(sym, ol)
        except ValueError:
            pass
        buckets = agg.get_buckets(sym, start, end)
        if buckets:
            driver.apply_buckets(sym, sorted(buckets, key=lambda b: b.bucket_ts_sec))


def pump_data_once(ctx: DaemonRunContext, subs: list[SubscribeIntent], now_dt: datetime) -> None:
    """High-frequency path: drain WS envelopes, ingest buckets, advance driver (no reload/sync/fixture)."""
    events = ctx.ws_manager.drain_events()
    ctx.aggregator.ingest(events)
    reference_ts_sec = int(now_dt.timestamp())
    _apply_buckets_for_intents(ctx.aggregator, ctx.driver, subs, reference_ts_sec)


async def run_publish_cycle(ctx: DaemonRunContext, *, now_dt: datetime) -> tuple[RunOnceResult, list[SubscribeIntent]]:
    """Low-frequency path: reload, WS sync, ingest (incl. optional fixture), quality, write latest + heartbeat."""
    now_iso = to_iso_z(now_dt)

    lr = ctx.ack_bridge.manager.reload()
    ts_status = coerce_target_status(lr.target_status)

    subs = ctx.ack_bridge.manager.get_subscribe_intents()
    rets = ctx.ack_bridge.manager.get_retire_intents()
    await ctx.ws_manager.sync(subs, rets)

    events = ctx.ws_manager.drain_events()
    if ctx.fixture_events_path is not None:
        events.extend(load_fixture_envelopes(ctx.fixture_events_path))
    ctx.aggregator.ingest(events)

    reference_ts_sec = int(now_dt.timestamp())
    _apply_buckets_for_intents(ctx.aggregator, ctx.driver, subs, reference_ts_sec)

    tgt_gen = read_micro_targets_generated_at(ctx.config.targets_path) or now_iso
    t_age = lr.target_age_sec
    target_age_int = int(t_age) if t_age is not None else 0

    m = ctx.ws_manager.metrics
    dropped_trade = m.dropped_events_trade
    dropped_book = m.dropped_events_book
    dropped_depth = m.dropped_events_depth
    ws_connected, ws_last_msg_age, ws_status, doc_ws_age = _ws_observability_fields(
        ctx,
        reference_ts_sec,
    )

    exit_code = 0
    last_err: str | None = None
    wrote_latest = False
    ready_count = 0

    observe_stale = ts_status == "stale" and bool(subs)
    effective_ts_status: TargetStatus = "stale_observing" if observe_stale else ts_status
    if ts_status != "fresh" and not observe_stale:
        doc_status = document_status_for(ts_status, 0)
        doc = build_document(
            targets=[],
            quality_by_symbol={},
            driver=ctx.driver,
            generated_at=now_iso,
            status=doc_status,
            target_generated_at=tgt_gen,
            target_age_sec=target_age_int,
            target_status=ts_status,
            dropped_events_trade=dropped_trade,
            dropped_events_book=dropped_book,
            dropped_events_depth=dropped_depth,
            ws_status=ws_status,
            last_ws_message_age_sec=doc_ws_age,
        )
        atomic_write_json(ctx.config.latest_features_path, doc)
        _persist_micro_factor_frames(ctx, doc)
        state_doc = build_micro_daemon_state_document(
            generated_at=now_iso,
            now_dt=now_dt,
            target_generated_at=tgt_gen,
            target_age_sec=target_age_int,
            intents=[],
            full_quality_by_symbol={},
            fast_quality_by_symbol={},
            daemon_ok=ts_status == "stale",
        )
        atomic_write_micro_state(latest_state_path_for_config(ctx.config), state_doc)
        wrote_latest = True
    else:
        ws_sig = ws_signal_from_metrics(m)
        quality_by_symbol: dict[str, MicroQualitySnapshot] = {}
        fast_quality_by_symbol: dict[str, MicroQualitySnapshot] = {}
        try:
            for it in subs:
                sym = it.symbol.strip().upper()
                cst = ctx.ack_bridge.collect_started_ts_sec(sym)
                if cst is None:
                    msg = f"missing collect_started_ts_sec for symbol={sym!r}"
                    raise ValueError(msg)
                inp = SymbolQualityInput(
                    symbol=sym,
                    ofi_levels=ofi_levels_for_tier(it.tier_key),
                    collect_started_ts_sec=cst,
                    min_ready_seconds=it.min_collect_seconds,
                )
                snap = ctx.quality_gate.evaluate(reference_ts_sec, inp, ws_sig)
                quality_by_symbol[sym] = snap
                fast_inp = SymbolQualityInput(
                    symbol=sym,
                    ofi_levels=ofi_levels_for_tier(it.tier_key),
                    collect_started_ts_sec=cst,
                )
                fast_quality_by_symbol[sym] = ctx.fast_quality_gate.evaluate(
                    reference_ts_sec,
                    fast_inp,
                    ws_sig,
                )
            rows = assembly_rows_from_intents(subs)
            doc_status = document_status_for(effective_ts_status, len(rows))
            root_reasons = (
                ["target_stale_observing_existing_symbols"] if observe_stale else []
            )
            doc = build_document(
                targets=rows,
                quality_by_symbol=quality_by_symbol,
                fast_quality_by_symbol=fast_quality_by_symbol,
                driver=ctx.driver,
                generated_at=now_iso,
                status=doc_status,
                target_generated_at=tgt_gen,
                target_age_sec=target_age_int,
                target_status=effective_ts_status,
                dropped_events_trade=dropped_trade,
                dropped_events_book=dropped_book,
                dropped_events_depth=dropped_depth,
                ws_status=ws_status,
                last_ws_message_age_sec=doc_ws_age,
                reason_codes=root_reasons,
            )
            ready_count = int(getattr(doc, "ready_count", 0))
            atomic_write_json(ctx.config.latest_features_path, doc)
            _persist_micro_factor_frames(ctx, doc)
            subscription_state = ctx.ws_manager.subscription_state_for_intents(subs)
            health_guard_state = {
                it.symbol.strip().upper(): ctx.health_guard.evaluate_symbol(
                    it.symbol,
                    subscription_state=subscription_state.get(it.symbol.strip().upper(), {}),
                    metrics=ctx.driver.get_metrics(it.symbol),
                ).as_dict()
                for it in subs
            }
            state_doc = build_micro_daemon_state_document(
                generated_at=now_iso,
                now_dt=now_dt,
                target_generated_at=tgt_gen,
                target_age_sec=target_age_int,
                intents=subs,
                full_quality_by_symbol=quality_by_symbol,
                fast_quality_by_symbol=fast_quality_by_symbol,
                daemon_ok=True,
                health_state_override="stale_observing" if observe_stale else None,
                root_reason_codes=root_reasons,
                subscription_state_by_symbol=subscription_state,
                health_guard_state_by_symbol=health_guard_state,
            )
            atomic_write_micro_state(latest_state_path_for_config(ctx.config), state_doc)
            wrote_latest = True
        except ValueError as e:
            last_err = str(e)
            exit_code = 1

    uptime = int((utc_now() - ctx.process_start).total_seconds())
    hb_ok = exit_code == 0
    latest_written_at = now_iso if wrote_latest else None
    hb = MicroCollectorHeartbeat(
        generated_at=now_iso,
        daemon_status="running" if hb_ok else "error",
        process_uptime_sec=uptime,
        last_loop_ok=hb_ok,
        last_error=last_err,
        target_reload_last_at=now_iso,
        target_manager_status=lr.status,
        target_status=effective_ts_status,
        target_generated_at=tgt_gen,
        target_age_sec=lr.target_age_sec,
        managed_symbol_count=managed_symbol_count(ctx.ack_bridge.manager),
        active_symbol_count=len(subs),
        ready_count=ready_count,
        ws_connected=ws_connected,
        ws_status=ws_status,
        ws_last_message_age_sec=ws_last_msg_age,
        dropped_events=HeartbeatDroppedEvents(
            trade=dropped_trade,
            book=dropped_book,
            depth=dropped_depth,
        ),
        last_output_generated_at=latest_written_at,
        latest_features_written_at=latest_written_at,
        log_rotation_enabled=(os.environ.get("MICRO_DAEMON_LOG_ROTATION_ENABLED") == "1"),
        log_file_size_bytes=(
            file_size_or_none(Path(os.environ["MICRO_DAEMON_LOG_PATH"]))
            if os.environ.get("MICRO_DAEMON_LOG_PATH")
            else None
        ),
    )
    atomic_write_heartbeat(ctx.config.heartbeat_path, hb)
    result = RunOnceResult(exit_code=exit_code, last_error=last_err, wrote_latest=wrote_latest)
    return result, subs


async def run_once(ctx: DaemonRunContext) -> RunOnceResult:
    """Single publish cycle: same semantics as pre-STEP3.8C one-shot daemon iteration."""
    now_fn = ctx.ack_bridge.manager._now  # noqa: SLF001
    now_dt = now_fn()
    r, _ = await run_publish_cycle(ctx, now_dt=now_dt)
    return r
