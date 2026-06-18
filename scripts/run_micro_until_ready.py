#!/usr/bin/env python3
"""STEP3.8D MVP: run micro daemon until latest_micro_features satisfies wait_until_ready.

Does not modify Step3.6/3.7/4 core. See docs/STEP3.8D_Micro_Collector_Wait_Until_Ready_任务卡.md
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from laoma_signal_engine.core.config_loader import EngineConfig
from laoma_signal_engine.core.exit_codes import EXIT_CONFIG
from laoma_signal_engine.micro.wait_until_ready.config import WaitUntilReadyConfig, load_wait_until_ready_config
from laoma_signal_engine.micro.wait_until_ready.runner import run_wait_until_ready_orchestration


def _merge_cfg(
    base: WaitUntilReadyConfig,
    *,
    mode: str | None,
    max_wait_sec: float | None,
    poll_interval_sec: float | None,
    min_ready_count: int | None,
    min_ready_strong_count: int | None,
    symbols: list[str] | None,
    require_symbols: str | None,
) -> WaitUntilReadyConfig:
    kw: dict[str, object] = {}
    if mode is not None:
        kw["mode"] = mode
    if max_wait_sec is not None:
        kw["max_wait_sec"] = float(max_wait_sec)
    if poll_interval_sec is not None:
        kw["poll_interval_sec"] = float(poll_interval_sec)
    if min_ready_count is not None:
        kw["min_ready_count"] = int(min_ready_count)
    if min_ready_strong_count is not None:
        kw["min_ready_strong_count"] = int(min_ready_strong_count)
    if symbols is not None:
        kw["symbols"] = tuple(s.strip().upper() for s in symbols if s.strip())
    if require_symbols is not None:
        kw["require_symbols"] = str(require_symbols).strip().lower()
    if not kw:
        return base
    return replace(base, **kw)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Micro collector wait-until-ready orchestration (STEP3.8D).")
    p.add_argument("--project-root", type=Path, default=None, help="Repo root (default: cwd).")
    p.add_argument("--latest-path", type=Path, default=None, help="Override latest_micro_features.json path.")
    p.add_argument("--heartbeat-path", type=Path, default=None, help="Override micro_collector_heartbeat.json path.")
    p.add_argument("--targets-path", type=Path, default=None, help="Override micro_targets.json path.")
    p.add_argument("--transport", choices=["fake", "real"], default="real")
    p.add_argument("--no-subprocess", action="store_true", help="Do not spawn daemon (poll existing latest only).")
    p.add_argument("--permissive-quality-smoke", action="store_true", help="Pass through to daemon CLI.")
    p.add_argument("--target-stale-sec", type=int, default=None, help="Override auto target_stale_sec (see STEP3.8D).")
    p.add_argument("--output-interval-sec", type=int, default=2)
    p.add_argument("--event-drain-interval-sec", type=float, default=1.0)
    p.add_argument("--ring-buffer-sec", type=int, default=1800)
    p.add_argument("--mode", default=None, help="Override wait_until_ready.mode.")
    p.add_argument("--max-wait-sec", type=float, default=None)
    p.add_argument("--poll-interval-sec", type=float, default=None)
    p.add_argument("--min-ready-count", type=int, default=None)
    p.add_argument("--min-ready-strong-count", type=int, default=None)
    p.add_argument("--symbols", default=None, help="Comma-separated symbols for symbols mode.")
    p.add_argument("--require-symbols", choices=["any", "all"], default=None)
    args = p.parse_args(argv)

    pr = Path(args.project_root).resolve() if args.project_root else Path.cwd().resolve()
    eng = EngineConfig.load(pr)
    base_w = load_wait_until_ready_config(pr)
    sym_list = [x.strip() for x in args.symbols.split(",")] if args.symbols else None
    cfg = _merge_cfg(
        base_w,
        mode=args.mode,
        max_wait_sec=args.max_wait_sec,
        poll_interval_sec=args.poll_interval_sec,
        min_ready_count=args.min_ready_count,
        min_ready_strong_count=args.min_ready_strong_count,
        symbols=sym_list,
        require_symbols=args.require_symbols,
    )

    if args.latest_path is not None:
        latest = Path(args.latest_path).resolve()
    else:
        latest = (pr / "DATA" / "micro" / "latest_micro_features.json").resolve()
    if args.heartbeat_path is not None:
        hb = Path(args.heartbeat_path).resolve()
    else:
        hb = (pr / "DATA" / "micro" / "micro_collector_heartbeat.json").resolve()
    targets = Path(args.targets_path).resolve() if args.targets_path else eng.micro_targets_path.resolve()

    if not args.no_subprocess and not targets.is_file():
        print(f"[ERROR] micro_targets.json missing: {targets}", file=sys.stderr)
        return EXIT_CONFIG

    return int(
        run_wait_until_ready_orchestration(
            project_root=pr,
            cfg=cfg,
            latest_path=latest,
            heartbeat_path=hb,
            targets_path=targets,
            transport=args.transport,
            start_subprocess=not args.no_subprocess,
            target_stale_sec_override=args.target_stale_sec,
            output_interval_sec=int(args.output_interval_sec),
            event_drain_interval_sec=float(args.event_drain_interval_sec),
            ring_buffer_sec=int(args.ring_buffer_sec),
            permissive_quality_smoke=bool(args.permissive_quality_smoke),
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
