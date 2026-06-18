"""CLI for Micro Collector daemon (STEP3.8 MVP). docs/STEP3.8_任务卡.md."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from laoma_signal_engine.core.time_utils import utc_now
from laoma_signal_engine.micro.daemon.app import run_daemon
from laoma_signal_engine.micro.daemon.config import DaemonConfig
from laoma_signal_engine.micro.quality.models import MicroQualityConfig


def _smoke_quality_config() -> MicroQualityConfig:
    return MicroQualityConfig(
        window_sec=30,
        min_ready_seconds=0,
        aggtrade_coverage_min=0.01,
        bookticker_coverage_min=0.01,
        depth5_coverage_min=0.01,
        max_stale_sec=999_999,
        max_lag_sec=999_999,
        event_queue_overflow_hard_fail=False,
        adapter_error_hard_fail=False,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="laoma_signal_engine.micro.daemon.cli")
    p.add_argument("--targets", type=Path, required=True, help="Path to micro_targets.json")
    p.add_argument("--latest-out", type=Path, required=True, help="latest_micro_features.json path")
    p.add_argument("--latest-state-out", type=Path, default=None, help="latest_micro_state.json path")
    p.add_argument("--heartbeat-out", type=Path, required=True, help="micro_collector_heartbeat.json path")
    p.add_argument("--once", action="store_true", help="Single iteration then exit")
    p.add_argument(
        "--fixture-events",
        type=Path,
        default=None,
        help="JSON array of WSEventEnvelope-shaped dicts (normalized type trade|book)",
    )
    p.add_argument(
        "--short-run-sec",
        type=float,
        default=None,
        help="Run repeated iterations for this many seconds (test harness)",
    )
    p.add_argument("--target-stale-sec", type=int, default=420)
    p.add_argument("--output-interval-sec", type=int, default=2)
    p.add_argument(
        "--event-drain-interval-sec",
        type=float,
        default=1.0,
        help="STEP3.8C: WS drain -> bucket -> driver cadence in seconds (default 1.0)",
    )
    p.add_argument("--ring-buffer-sec", type=int, default=1800)
    p.add_argument("--transport", choices=["fake", "real"], default="fake")
    p.add_argument(
        "--proxy-url",
        default=None,
        help="Optional proxy URL (MVP real transport raises ValueError if set)",
    )
    p.add_argument(
        "--permissive-quality-smoke",
        action="store_true",
        help="Relax quality gates for real-WS smoke runs only (not for production)",
    )
    args = p.parse_args(argv)

    quality_config: MicroQualityConfig | None = _smoke_quality_config() if args.permissive_quality_smoke else None
    cfg = DaemonConfig(
        targets_path=args.targets,
        latest_features_path=args.latest_out,
        heartbeat_path=args.heartbeat_out,
        latest_state_path=args.latest_state_out,
        target_stale_sec=args.target_stale_sec,
        output_interval_sec=args.output_interval_sec,
        event_drain_interval_sec=args.event_drain_interval_sec,
        ring_buffer_seconds=args.ring_buffer_sec,
        transport=args.transport,
        proxy_url=args.proxy_url,
    )

    async def _run() -> int:
        if args.once:
            r = await run_daemon(
                cfg,
                now_fn=utc_now,
                fixture_events_path=args.fixture_events,
                quality_config=quality_config,
                once=True,
            )
            assert r is not None
            return r.exit_code
        await run_daemon(
            cfg,
            now_fn=utc_now,
            fixture_events_path=args.fixture_events,
            quality_config=quality_config,
            once=False,
            short_run_sec=args.short_run_sec,
        )
        return 0

    try:
        return int(asyncio.run(_run()))
    except NotImplementedError as e:
        print(str(e), file=sys.stderr)
        return 2
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
