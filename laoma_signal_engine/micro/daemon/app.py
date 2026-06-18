"""Async app entry for Micro Collector daemon. docs/STEP3.8_任务卡.md."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from laoma_signal_engine.core.time_utils import utc_now
from laoma_signal_engine.micro.daemon.config import DaemonConfig
from laoma_signal_engine.micro.daemon.loop import (
    DaemonRunContext,
    RunOnceResult,
    build_run_context,
    pump_data_once,
    run_once,
    run_publish_cycle,
)
from laoma_signal_engine.micro.quality.models import MicroQualityConfig
from laoma_signal_engine.micro.target_intent_models import SubscribeIntent


async def _run_pump_publish_loop(
    ctx: DaemonRunContext,
    cfg: DaemonConfig,
    now_fn: Callable[[], datetime],
    *,
    deadline_monotonic: float | None,
) -> RunOnceResult | None:
    """STEP3.8C: high-frequency pump vs low-frequency publish/write (see docs/STEP3.8C_*.md)."""
    drain_iv = max(0.05, float(cfg.event_drain_interval_sec))
    out_iv = max(0.01, float(cfg.output_interval_sec))
    next_pub = time.monotonic()
    next_pump = time.monotonic()
    last_subs: list[SubscribeIntent] = []
    last: RunOnceResult | None = None
    while deadline_monotonic is None or time.monotonic() < deadline_monotonic:
        now_m = time.monotonic()
        now_dt = now_fn()
        if now_m >= next_pub:
            last, last_subs = await run_publish_cycle(ctx, now_dt=now_dt)
            next_pub += out_iv
            while next_pub <= now_m:
                next_pub += out_iv
            next_pump = now_m + drain_iv
        elif now_m >= next_pump:
            pump_data_once(ctx, last_subs, now_dt)
            next_pump += drain_iv
            while next_pump <= now_m:
                next_pump += drain_iv
        sleep_for = min(next_pub, next_pump) - time.monotonic()
        if deadline_monotonic is not None:
            sleep_for = min(sleep_for, deadline_monotonic - time.monotonic())
        if sleep_for > 0:
            await asyncio.sleep(min(sleep_for, 0.25))
    return last


async def run_daemon(
    cfg: DaemonConfig,
    *,
    now_fn: Callable[[], datetime] | None = None,
    fixture_events_path: Path | None = None,
    quality_config: MicroQualityConfig | None = None,
    once: bool = False,
    short_run_sec: float | None = None,
) -> RunOnceResult | None:
    now = now_fn if now_fn is not None else utc_now
    ctx = build_run_context(
        cfg,
        now,
        fixture_events_path=fixture_events_path,
        quality_config=quality_config,
    )
    try:
        if once:
            return await run_once(ctx)
        if short_run_sec is not None and short_run_sec > 0:
            deadline = time.monotonic() + short_run_sec
            return await _run_pump_publish_loop(ctx, cfg, now, deadline_monotonic=deadline)
        await _run_pump_publish_loop(ctx, cfg, now, deadline_monotonic=None)
        return None
    finally:
        await ctx.aclose_optional_real()
