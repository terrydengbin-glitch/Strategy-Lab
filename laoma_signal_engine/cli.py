"""CLI entry: argparse, subcommands, exit codes per tech spec."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_NOT_IMPLEMENTED
from laoma_signal_engine.core.logger import setup_stderr_logging
from laoma_signal_engine.micro.runtime_logging import rotate_file_if_needed
from laoma_signal_engine.pipeline import (
    LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT,
    run_decision_pipeline_safe,
    run_full_pipeline_with_micro_timed_safe,
)
from laoma_signal_engine.runtime_health import pid_running as runtime_pid_running


def _cmd_not_implemented(name: str) -> int:
    print(f"[SKIP] command not implemented yet: {name}", file=sys.stderr)
    return EXIT_NOT_IMPLEMENTED


def _sandbox_root_from_args(args: argparse.Namespace) -> Path | None:
    sandbox_root = getattr(args, "sandbox_root", None)
    if sandbox_root:
        return Path(sandbox_root).resolve()
    project_root = getattr(args, "project_root", None)
    if project_root:
        return Path(project_root).resolve() / "DATA" / "sandboxes"
    return None


def _sandbox_context_from_args(args: argparse.Namespace) -> dict[str, object]:
    source_surface = getattr(args, "source_surface", None) or ("external_connector" if getattr(args, "external", False) else "cli")
    caller_type = getattr(args, "caller_type", None) or ("external_ai_trader" if getattr(args, "external", False) else "cli_user")
    caller_id = getattr(args, "caller_id", None) or ("external_cli" if getattr(args, "external", False) else "laoma_cli")
    policy: dict[str, object] = {}
    if getattr(args, "allow_active_context_write", False):
        policy["allow_active_context_write"] = True
    return {
        "source_surface": source_surface,
        "caller_type": caller_type,
        "caller_id": caller_id,
        "operation_policy": policy,
        "audit_trace_id": getattr(args, "audit_trace_id", None),
    }


def _sandbox_context_is_external(ctx: dict[str, object]) -> bool:
    return str(ctx.get("source_surface") or "") in {"external_connector", "external_ai_trader"} or str(ctx.get("caller_type") or "") in {
        "external",
        "external_ai",
        "external_ai_trader",
    }


def _print_cli_payload(payload: dict[str, object], stdout_json: bool = True) -> None:
    if stdout_json:
        print(json.dumps({"ok": True, "data": payload, "error": None}, ensure_ascii=False, sort_keys=True))
        return
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _run_sandbox_from_args(args: argparse.Namespace) -> int:
    from laoma_signal_engine.strategy_sandbox.service import (
        active_sandbox_payload,
        create_sandbox_payload,
        delete_sandbox_payload,
        get_sandbox_payload,
        job_payload,
        list_sandboxes_payload,
        set_active_sandbox_payload,
    )
    from laoma_signal_engine.strategy_sandbox.resource_governor import (
        finish_external_research_context,
        governor_status as resource_governor_status,
        resource_run_payload,
        resource_runs_payload,
        start_external_research_context,
    )
    from laoma_signal_engine.strategy_sandbox.daemon_writer import daemon_writer_status_payload
    from laoma_signal_engine.strategy_sandbox.full_pipeline import run_sandbox_full_pipeline

    project_root = Path(args.project_root).resolve() if getattr(args, "project_root", None) else None
    previous_cwd = Path.cwd()
    if project_root:
        os.chdir(project_root)
    try:
        root = _sandbox_root_from_args(args)
        ctx = _sandbox_context_from_args(args)
        action = args.sandbox_action
        if action == "list":
            payload = list_sandboxes_payload(
                strategy_line=args.strategy_line,
                status=args.status,
                tag=args.tag,
                limit=args.limit,
                root=root,
                include_deleted=args.include_deleted,
            )
        elif action == "create":
            if args.set_active and _sandbox_context_is_external(ctx) and not ctx.get("operation_policy", {}).get("allow_active_context_write"):
                raise ValueError("sandbox_policy_denied: external_active_context_write_denied")
            strategy_lines = [x.strip() for x in str(args.strategy_lines or "").split(",") if x.strip()] or None
            data_scope = json.loads(args.data_scope_json) if args.data_scope_json else {}
            config_scope = json.loads(args.config_scope_json) if args.config_scope_json else {}
            tags = [x.strip() for x in str(args.tags or "").split(",") if x.strip()]
            payload = create_sandbox_payload(
                strategy_line=args.strategy_line,
                strategy_lines=strategy_lines,
                strategy_version=args.strategy_version,
                data_scope=data_scope,
                config_scope=config_scope,
                tags=tags,
                root=root,
                operation_context=ctx,
            )
            if args.set_active:
                active = set_active_sandbox_payload(payload["sandbox"]["sandbox_id"], root=root, operation_context=ctx)
                payload["active"] = active
                payload["active_changed"] = bool(active.get("active_changed"))
        elif action == "status":
            payload = get_sandbox_payload(args.sandbox_id, root=root)
        elif action == "active":
            payload = active_sandbox_payload(root=root)
        elif action == "resource-status":
            payload = resource_governor_status(project_root=root)
        elif action == "resource-runs":
            payload = resource_runs_payload(
                project_root or previous_cwd,
                resource_lane=args.resource_lane,
                sandbox_id=args.sandbox_id,
                limit=args.limit,
            )
        elif action == "resource-run":
            payload = resource_run_payload(project_root or previous_cwd, run_id=args.run_id)
        elif action == "daemon-writer-status":
            writer_context = None
            if args.run_id:
                run = resource_run_payload(project_root or previous_cwd, run_id=args.run_id)
                latest = run.get("latest") if isinstance(run.get("latest"), dict) else {}
                writer_context = latest.get("writer_context") if isinstance(latest.get("writer_context"), dict) else None
            payload = daemon_writer_status_payload(project_root or previous_cwd, writer_context=writer_context)
        elif action == "full-pipeline-run":
            options = json.loads(args.options_json) if args.options_json else {}
            if args.strategy_line:
                options["strategy_line"] = args.strategy_line
            if args.symbol:
                options["symbol"] = args.symbol
            options.update(ctx)
            gov_root = project_root or previous_cwd
            lane = start_external_research_context(
                project_root=gov_root,
                sandbox_id=args.sandbox_id,
                run_id=args.run_id,
                caller_surface=str(ctx.get("source_surface") or "cli"),
                caller_type=str(ctx.get("caller_type") or "cli_user"),
                requires_live_rest=bool(options.get("requires_live_rest", False)),
                cache_hit=bool(options.get("cache_hit", True)),
                options={"job_type": "sandbox_full_pipeline", "pipeline_mode": "sandbox_full_pipeline", **options},
            )
            if not lane.get("accepted"):
                payload = {"status": "blocked", "resource_lane": lane}
            else:
                try:
                    result = run_sandbox_full_pipeline(
                        gov_root,
                        sandbox_id=args.sandbox_id,
                        run_id=str(lane["run_id"]),
                        cycle_id=str(lane.get("cycle_id") or f"cycle_{lane['run_id']}"),
                        writer_context=lane.get("writer_context") or {},
                        options={"pipeline_mode": "sandbox_full_pipeline", **options},
                    )
                    finish = finish_external_research_context(
                        project_root=gov_root,
                        run_id=str(lane["run_id"]),
                        sandbox_id=args.sandbox_id,
                        status=str(result.get("status") or "completed"),
                        result=result,
                    )
                    payload = {"status": result.get("status") or "completed", "resource_lane": lane, "execution_result": result, "resource_finish": finish}
                except Exception:
                    finish_external_research_context(
                        project_root=gov_root,
                        run_id=str(lane["run_id"]),
                        sandbox_id=args.sandbox_id,
                        status="failed",
                        result={"job_type": "sandbox_full_pipeline"},
                    )
                    raise
        elif action == "set-active":
            payload = set_active_sandbox_payload(args.sandbox_id, root=root, operation_context=ctx)
        elif action == "clear-active":
            payload = set_active_sandbox_payload(None, root=root, operation_context=ctx)
        elif action == "delete":
            policy = {"allow_purge": bool(args.confirm_purge)}
            payload = delete_sandbox_payload(
                args.sandbox_id,
                mode=args.mode,
                reason=args.reason,
                confirm=args.confirm_purge,
                root=root,
                operation_context=ctx | {"operation_policy": policy},
            )
        elif action == "job":
            options = json.loads(args.options_json) if args.options_json else {}
            if args.strategy_line:
                options["strategy_line"] = args.strategy_line
            options.update(ctx)
            gov_root = project_root or previous_cwd
            lane = start_external_research_context(
                project_root=gov_root,
                sandbox_id=args.sandbox_id,
                caller_surface=str(ctx.get("source_surface") or "cli"),
                caller_type=str(ctx.get("caller_type") or "cli_user"),
                requires_live_rest=bool(options.get("requires_live_rest", False)),
                cache_hit=bool(options.get("cache_hit", True)),
                options={"job_type": args.job_type, **options},
            )
            if not lane.get("accepted"):
                payload = {"status": "blocked", "resource_lane": lane}
            else:
                try:
                    job = job_payload(args.sandbox_id, args.job_type, options, root=root, operation_context=ctx)
                    finish = finish_external_research_context(
                        project_root=gov_root,
                        run_id=str(lane["run_id"]),
                        sandbox_id=args.sandbox_id,
                        status="completed",
                        result={"job_type": args.job_type, "job_status": job.get("status")},
                    )
                    payload = {"status": "completed", "resource_lane": lane, "job": job, "resource_finish": finish}
                except Exception:
                    finish_external_research_context(
                        project_root=gov_root,
                        run_id=str(lane["run_id"]),
                        sandbox_id=args.sandbox_id,
                        status="failed",
                        result={"job_type": args.job_type},
                    )
                    raise
        else:
            print(f"[ERROR] unknown sandbox action: {action}", file=sys.stderr)
            return EXIT_CONFIG
        _print_cli_payload(payload, stdout_json=args.stdout_json)
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "data": None, "error": str(exc)}, ensure_ascii=False, sort_keys=True), file=sys.stdout if args.stdout_json else sys.stderr)
        return EXIT_CONFIG
    finally:
        if project_root:
            os.chdir(previous_cwd)


def _register_pipeline_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: cwd).",
    )
    p.add_argument(
        "--force-universe",
        action="store_true",
        default=False,
        help="Step 1: pass --force to build-universe.",
    )
    p.add_argument(
        "--skip-universe",
        action="store_true",
        default=False,
        help="Skip Step 1 (reuse existing CANDIDATE_UNIVERSE.json).",
    )
    p.add_argument(
        "--skip-fetch-light-snapshot",
        action="store_true",
        default=False,
        help="Skip Step 1.5 (reuse existing futures_light_snapshot.json).",
    )
    p.add_argument(
        "--light-limit",
        type=int,
        default=0,
        help="Step 1.5: max symbols after filter (0 = no limit).",
    )
    p.add_argument(
        "--light-symbols",
        default=None,
        help="Step 1.5: comma-separated futures symbols; intersect with eligible list.",
    )
    p.add_argument(
        "--fetch-mode",
        default=LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT,
        choices=["legacy", "async", "distributed"],
        help="Step 1.5 fetch mode.",
    )
    p.add_argument(
        "--max-concurrency",
        type=int,
        default=None,
        help="Step 1.5: override max concurrency.",
    )
    p.add_argument(
        "--scan-allow-stale-input",
        action="store_true",
        default=False,
        help="Step 2.0: allow stale light snapshot.",
    )
    p.add_argument(
        "--stdout-json",
        action="store_true",
        default=False,
        help="Emit JSON summary lines on stdout for scan, route, 3B, 4.",
    )
    p.add_argument(
        "--allow-watch-now",
        action="store_true",
        default=False,
        help="Step 4: allow watch_candidate to output NOW.",
    )
    p.add_argument(
        "--disable-context-guards-for-now",
        action="store_true",
        default=False,
        help="Step 4: smoke only; do not require OI/Funding/Basis for NOW.",
    )
    p.add_argument(
        "--skip-market-context",
        action="store_true",
        default=False,
        help="Step 3B: do not fetch Binance OI/Funding/Basis (placeholders; skip STEP4.1 REST).",
    )
    p.add_argument(
        "--skip-sl-tp",
        action="store_true",
        default=False,
        help=(
        "Skip Step 5.0 (latest_decisions.json). "
        "Also skipped when default.yaml pipeline.skip_final_decisions is true (either condition skips)."
        ),
    )
    p.add_argument(
        "--skip-factor-snapshot-without-ofi-cvd",
        action="store_true",
        default=False,
        help=(
            "Skip Step 3B.1 pre-micro snapshot (latest_factor_snapshot_withoutoficvd.json). "
            "Also skipped when default.yaml pipeline.skip_factor_snapshot_without_ofi_cvd is true."
        ),
    )


def _register_persistent_micro_wait_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--wait-micro-ready",
        action="store_true",
        default=False,
        help="Step 3 persistent mode: wait for an already-running micro collector daemon; do not spawn one.",
    )
    p.add_argument(
        "--micro-max-wait-sec",
        type=float,
        default=None,
        metavar="SEC",
        help="Persistent Step 3 wait override (default from default.yaml wait_until_ready, currently 1200 sec).",
    )
    p.add_argument(
        "--micro-min-ready-count",
        type=int,
        default=None,
        metavar="N",
        help="Persistent Step 3 wait min ready count override.",
    )


def _register_micro_daemon_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    p.add_argument("--targets", default=None, help="Override DATA/micro/micro_targets.json path.")
    p.add_argument("--latest-out", default=None, help="Override DATA/micro/latest_micro_features.json path.")
    p.add_argument("--latest-state-out", default=None, help="Override DATA/micro/latest_micro_state.json path.")
    p.add_argument("--heartbeat-out", default=None, help="Override DATA/micro/micro_collector_heartbeat.json path.")
    p.add_argument("--transport", choices=["fake", "real"], default="real")
    p.add_argument("--target-stale-sec", type=int, default=1500)
    p.add_argument("--output-interval-sec", type=int, default=2)
    p.add_argument("--event-drain-interval-sec", type=float, default=1.0)
    p.add_argument("--ring-buffer-sec", type=int, default=1800)
    p.add_argument("--once", action="store_true", default=False)
    p.add_argument("--short-run-sec", type=float, default=None)
    p.add_argument("--fixture-events", default=None)
    p.add_argument("--proxy-url", default=None)
    p.add_argument(
        "--permissive-quality-smoke",
        action="store_true",
        default=False,
        help="Relax quality gates for real-WS smoke runs only.",
    )


def _register_micro_timed_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--skip-scan-and-route",
        action="store_true",
        default=False,
        help="After Step 1/1.5, skip scan and micro router; use existing micro_targets.json (lab/recovery).",
    )
    p.add_argument(
        "--micro-run-sec",
        type=float,
        default=900.0,
        help="Step 3: micro collector run duration in seconds (default 900 = 15 minutes).",
    )
    p.add_argument(
        "--micro-transport",
        choices=["real", "fake"],
        default="real",
        help="Step 3: WS transport (default real Binance futures).",
    )
    p.add_argument(
        "--micro-permissive-quality-smoke",
        action="store_true",
        default=False,
        help="Step 3: relax quality gates (smoke / lab only).",
    )
    p.add_argument(
        "--micro-wait-until-ready",
        action="store_true",
        default=False,
        help=(
            "Step 3: use wait-until-ready (STEP3.8D) subprocess + poll until ready or timeout, "
            "then 3B+4. Does not use --micro-run-sec to stop the daemon; see laoma_signal_engine/config/default.yaml wait_until_ready."
        ),
    )
    p.add_argument(
        "--micro-max-wait-sec",
        type=float,
        default=None,
        metavar="SEC",
        help="Step 3 (wait-until-ready): override max wait seconds (default from default.yaml wait_until_ready).",
    )
    p.add_argument(
        "--micro-min-ready-count",
        type=int,
        default=None,
        metavar="N",
        help="Step 3 (wait-until-ready): override min ready count (default from default.yaml wait_until_ready).",
    )


def _register_strategy_pipeline_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    p.add_argument("--line", choices=["without_micro", "micro_fast", "micro_full", "all"], default=None)
    p.add_argument(
        "--lines",
        default=None,
        help="Comma-separated selected strategy lines. Overrides --line. Example: without_micro,micro_fast",
    )
    p.add_argument("--mode", choices=["once", "interval"], default=None)
    p.add_argument("--interval-sec", type=int, default=None)
    p.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="For tests/manual bounded interval runs. Omit for continuous interval mode.",
    )
    p.add_argument("--force-universe", action="store_true", default=False)
    p.add_argument("--light-limit", type=int, default=None)
    p.add_argument("--fetch-mode", choices=["legacy", "async", "distributed"], default=None)
    p.add_argument("--max-concurrency", type=int, default=None)
    p.add_argument("--scan-allow-stale-input", action="store_true", default=False)
    p.add_argument("--skip-market-context", action="store_true", default=False)
    p.add_argument("--skip-micro-wait", action="store_true", default=False)
    p.add_argument("--skip-abc-audit", action="store_true", default=False)
    p.add_argument("--skip-json-stage-audit", action="store_true", default=False)
    p.add_argument("--skip-aggregate-final-decisions", action="store_true", default=False)
    p.add_argument("--stdout-json", action="store_true", default=False)


def _register_micro_daemon_control_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument("action", choices=["start", "status", "stop"])
    p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    p.add_argument("--transport", choices=["fake", "real"], default=None)
    p.add_argument("--stdout-json", action="store_true", default=False)


def _register_paper_daemon_control_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument("action", choices=["start", "status", "stop", "restart", "run-once"])
    p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    p.add_argument("--stdout-json", action="store_true", default=False)


def _register_paper_daemon_run_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")


def _register_feishu_notify_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    p.add_argument("--mock-signals", action="store_true", default=False, help="Use three synthetic executable strategy signals.")
    p.add_argument("--mock-send", action="store_true", default=False, help="Build cards and delivery rows without calling Feishu.")
    p.add_argument("--force-enabled", action="store_true", default=False, help="Send even when FEISHU_BOT_ENABLED/default.yaml enabled is false.")
    p.add_argument("--stdout-json", action="store_true", default=False)


def _run_pipeline_from_args(args: argparse.Namespace) -> int:
    root = Path(args.project_root).resolve() if args.project_root else None
    syms = None
    if args.light_symbols:
        syms = [x.strip() for x in args.light_symbols.split(",") if x.strip()]
    return run_decision_pipeline_safe(
        project_root=root,
        force_universe=args.force_universe,
        skip_universe=args.skip_universe,
        skip_fetch_light_snapshot=args.skip_fetch_light_snapshot,
        light_limit=args.light_limit,
        light_symbols=syms,
        fetch_mode=args.fetch_mode,
        max_concurrency=args.max_concurrency,
        scan_allow_stale_input=args.scan_allow_stale_input,
        stdout_json=args.stdout_json,
        allow_watch_now=args.allow_watch_now,
        disable_context_guards_for_now=args.disable_context_guards_for_now,
        skip_market_context=args.skip_market_context,
        skip_final_decisions=args.skip_sl_tp,
        skip_factor_snapshot_without_ofi_cvd=args.skip_factor_snapshot_without_ofi_cvd,
        wait_micro_ready=args.wait_micro_ready,
        micro_max_wait_sec=args.micro_max_wait_sec,
        micro_min_ready_count=args.micro_min_ready_count,
    )


def _run_micro_daemon_from_args(args: argparse.Namespace) -> int:
    from laoma_signal_engine.micro.daemon.cli import main as daemon_main

    root = Path(args.project_root).resolve() if args.project_root else Path.cwd().resolve()
    targets = Path(args.targets).resolve() if args.targets else (root / "DATA" / "micro" / "micro_targets.json")
    latest = Path(args.latest_out).resolve() if args.latest_out else (root / "DATA" / "micro" / "latest_micro_features.json")
    latest_state = (
        Path(args.latest_state_out).resolve()
        if args.latest_state_out
        else (root / "DATA" / "micro" / "latest_micro_state.json")
    )
    heartbeat = (
        Path(args.heartbeat_out).resolve()
        if args.heartbeat_out
        else (root / "DATA" / "micro" / "micro_collector_heartbeat.json")
    )
    argv = [
        "--targets",
        str(targets),
        "--latest-out",
        str(latest),
        "--latest-state-out",
        str(latest_state),
        "--heartbeat-out",
        str(heartbeat),
        "--transport",
        args.transport,
        "--target-stale-sec",
        str(args.target_stale_sec),
        "--output-interval-sec",
        str(args.output_interval_sec),
        "--event-drain-interval-sec",
        str(args.event_drain_interval_sec),
        "--ring-buffer-sec",
        str(args.ring_buffer_sec),
    ]
    if args.once:
        argv.append("--once")
    if args.short_run_sec is not None:
        argv.extend(["--short-run-sec", str(args.short_run_sec)])
    if args.fixture_events:
        argv.extend(["--fixture-events", str(Path(args.fixture_events).resolve())])
    if args.proxy_url:
        argv.extend(["--proxy-url", str(args.proxy_url)])
    if args.permissive_quality_smoke:
        argv.append("--permissive-quality-smoke")
    return daemon_main(argv)


def _paper_config_from_yaml(root: Path):
    import yaml

    from laoma_signal_engine.core.config_loader import package_root
    from laoma_signal_engine.paper.models import PaperConfig

    cfg_path = package_root() / "config" / "default.yaml"
    doc = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    paper = doc.get("paper") or {}
    daemon = paper.get("daemon") or {}
    archive = paper.get("archive") or {}
    cooldown_after = paper.get("reentry_cooldown_after") or {}
    return PaperConfig(
        db_path=str(paper.get("db_path", "DATA/paper/paper_trading.db")),
        summary_path=str(paper.get("summary_path", "DATA/paper/latest_paper_state.json")),
        default_account_equity_usdt=float(paper.get("default_account_equity_usdt", 1000)),
        default_margin_usdt=float(paper.get("default_margin_usdt", 100)),
        default_leverage=float(paper.get("default_leverage", 20)),
        taker_fee_bps=float(paper.get("taker_fee_bps", 5)),
        maker_fee_bps=float(paper.get("maker_fee_bps", 2)),
        default_slippage_bps=float(paper.get("default_slippage_bps", 5)),
        prevent_same_line_symbol_reentry=bool(paper.get("prevent_same_line_symbol_reentry", True)),
        active_slot_scope=str(paper.get("active_slot_scope", "strategy_line_symbol")),
        allow_cross_line_same_symbol=bool(paper.get("allow_cross_line_same_symbol", True)),
        daemon_tick_interval_sec=int(daemon.get("tick_interval_sec", 60)),
        daemon_lock_path=str(daemon.get("singleton_lock_path", "DATA/runtime/paper_daemon.lock")),
        daemon_pid_path=str(daemon.get("pid_path", "DATA/runtime/paper_daemon.pid")),
        daemon_log_path=str(daemon.get("log_path", "DATA/logs/paper_daemon.log")),
        daemon_heartbeat_path=str(daemon.get("heartbeat_path", "DATA/runtime/paper_daemon_heartbeat.json")),
        daemon_status_path=str(daemon.get("status_path", "DATA/runtime/paper_daemon_status.json")),
        max_trade_plan_age_sec=int(paper.get("max_trade_plan_age_sec", 0) or 0),
        reentry_cooldown_sec=int(paper.get("reentry_cooldown_sec", 0) or 0),
        reentry_cooldown_scope=str(paper.get("reentry_cooldown_scope", "strategy_line_symbol_side")),
        reentry_cooldown_after_sl=bool(cooldown_after.get("sl", True)),
        reentry_cooldown_after_tp=bool(cooldown_after.get("tp", False)),
        reentry_cooldown_after_forced_close=bool(cooldown_after.get("archive_reset_forced_close", True)),
        archive_enabled=bool(archive.get("enabled", True)),
        archive_dir=str(archive.get("archive_dir", "DATA/paper/archives")),
        archive_metadata_path=str(archive.get("metadata_path", "DATA/paper/paper_experiments.json")),
        archive_force_close_exit_reason=str(archive.get("forced_close_exit_reason", "archive_reset_forced_close")),
    )


def _read_paper_daemon_pid(pid_path: Path) -> int | None:
    try:
        raw = json.loads(pid_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    try:
        return int(raw.get("pid") if isinstance(raw, dict) else None)
    except (TypeError, ValueError):
        return None


def _run_paper_daemon_control_from_args(args: argparse.Namespace) -> int:
    from laoma_signal_engine.core.exit_codes import EXIT_SUCCESS
    from laoma_signal_engine.core.json_io import write_json_atomic
    from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
    from laoma_signal_engine.paper.daemon import daemon_paths, mark_stopped, read_status, run_once

    root = Path(args.project_root).resolve() if args.project_root else Path.cwd().resolve()
    cfg = _paper_config_from_yaml(root)
    paths = daemon_paths(root, cfg)
    if args.action == "status":
        payload = read_status(root, cfg)
        print(json.dumps(payload, ensure_ascii=False))
        return EXIT_SUCCESS
    if args.action == "run-once":
        payload = run_once(root, config=cfg)
        print(json.dumps(payload, ensure_ascii=False) if args.stdout_json else "paper daemon debug tick complete")
        return EXIT_SUCCESS
    if args.action == "stop":
        pid = _read_paper_daemon_pid(paths["pid"])
        if pid and _pid_running(pid):
            os.kill(pid, signal.SIGTERM)
        payload = mark_stopped(root, config=cfg)
        print(json.dumps(payload, ensure_ascii=False) if args.stdout_json else "paper daemon stopped")
        return EXIT_SUCCESS
    if args.action == "restart":
        stop_args = argparse.Namespace(action="stop", project_root=str(root), stdout_json=True)
        _run_paper_daemon_control_from_args(stop_args)
    pid = _read_paper_daemon_pid(paths["pid"])
    if pid and _pid_running(pid):
        payload = {"source": "paper_daemon", "action": "start", "pid": pid, "status": "already_running"}
        print(json.dumps(payload, ensure_ascii=False) if args.stdout_json else f"paper daemon already running pid={pid}")
        return EXIT_SUCCESS
    paths["log"].parent.mkdir(parents=True, exist_ok=True)
    paths["pid"].parent.mkdir(parents=True, exist_ok=True)
    argv = [
        sys.executable,
        "-m",
        "laoma_signal_engine.cli",
        "paper-daemon-run",
        "--project-root",
        str(root),
    ]
    with open(paths["log"], "ab") as log_file:
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        proc = subprocess.Popen(
            argv,
            cwd=str(root),
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    write_json_atomic(
        paths["pid"],
        {"schema_version": "1.0", "source": "paper_daemon", "pid": proc.pid, "started_at": to_iso_z(utc_now()), "log_path": str(paths["log"])},
    )
    payload = {"source": "paper_daemon", "action": "start", "pid": proc.pid, "status": "started"}
    print(json.dumps(payload, ensure_ascii=False) if args.stdout_json else f"paper daemon started pid={proc.pid}")
    return EXIT_SUCCESS


def _run_paper_daemon_forever_from_args(args: argparse.Namespace) -> int:
    from laoma_signal_engine.paper.daemon import run_forever

    root = Path(args.project_root).resolve() if args.project_root else Path.cwd().resolve()
    return run_forever(root, config=_paper_config_from_yaml(root))


def _run_feishu_notify_from_args(args: argparse.Namespace) -> int:
    from laoma_signal_engine.core.exit_codes import EXIT_SUCCESS
    from laoma_signal_engine.notifications.service import send_trade_plan_notifications

    root = Path(args.project_root).resolve() if args.project_root else Path.cwd().resolve()
    payload = send_trade_plan_notifications(
        root,
        mock_signals=args.mock_signals,
        mock_send=args.mock_send,
        force_enabled=args.force_enabled,
    )
    if args.stdout_json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(f"feishu deliveries={len(payload.get('deliveries') or [])}")
    return EXIT_SUCCESS


def _run_full_micro_pipeline_from_args(args: argparse.Namespace) -> int:
    root = Path(args.project_root).resolve() if args.project_root else None
    syms = None
    if args.light_symbols:
        syms = [x.strip() for x in args.light_symbols.split(",") if x.strip()]
    return run_full_pipeline_with_micro_timed_safe(
        project_root=root,
        force_universe=args.force_universe,
        skip_universe=args.skip_universe,
        skip_fetch_light_snapshot=args.skip_fetch_light_snapshot,
        light_limit=args.light_limit,
        light_symbols=syms,
        fetch_mode=args.fetch_mode,
        max_concurrency=args.max_concurrency,
        scan_allow_stale_input=args.scan_allow_stale_input,
        stdout_json=args.stdout_json,
        allow_watch_now=args.allow_watch_now,
        disable_context_guards_for_now=args.disable_context_guards_for_now,
        micro_run_sec=float(args.micro_run_sec),
        micro_transport=args.micro_transport,
        micro_permissive_quality_smoke=args.micro_permissive_quality_smoke,
        skip_scan_and_route=args.skip_scan_and_route,
        micro_wait_until_ready=args.micro_wait_until_ready,
        micro_max_wait_sec=args.micro_max_wait_sec,
        micro_min_ready_count=args.micro_min_ready_count,
        skip_market_context=args.skip_market_context,
        skip_final_decisions=args.skip_sl_tp,
        skip_factor_snapshot_without_ofi_cvd=args.skip_factor_snapshot_without_ofi_cvd,
    )


def _run_strategy_pipeline_from_args(args: argparse.Namespace) -> int:
    from laoma_signal_engine.strategy_pipeline import run_strategy_pipeline_safe

    root = Path(args.project_root).resolve() if args.project_root else None
    return run_strategy_pipeline_safe(
        project_root=root,
        line=args.line,
        lines=[x.strip() for x in str(args.lines or "").split(",") if x.strip()] if args.lines else None,
        mode=args.mode,
        interval_sec=args.interval_sec,
        max_cycles=args.max_cycles,
        stdout_json=args.stdout_json,
        force_universe=True if args.force_universe else None,
        light_limit=args.light_limit,
        fetch_mode=args.fetch_mode,
        max_concurrency=args.max_concurrency,
        scan_allow_stale_input=True if args.scan_allow_stale_input else None,
        skip_market_context=args.skip_market_context,
        skip_micro_wait=args.skip_micro_wait,
        run_abc_audit=False if args.skip_abc_audit else None,
        run_json_stage_audit=False if args.skip_json_stage_audit else None,
        aggregate_final_decisions=False if args.skip_aggregate_final_decisions else None,
    )


def _pid_running(pid: int) -> bool:
    return runtime_pid_running(pid)


def _backup_stale_micro_pid_file(pid_path: Path) -> str | None:
    if not pid_path.exists():
        return None
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    backup_path = pid_path.with_name(f"{pid_path.name}.stale_{stamp}")
    try:
        pid_path.replace(backup_path)
    except OSError:
        return None
    return str(backup_path)


def _read_micro_daemon_pid(pid_path: Path) -> int | None:
    try:
        raw = json.loads(pid_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    pid = raw.get("pid") if isinstance(raw, dict) else None
    try:
        return int(pid)
    except (TypeError, ValueError):
        return None


def _micro_daemon_status_payload(root: Path) -> dict[str, object]:
    from laoma_signal_engine.core.config_loader import EngineConfig
    from laoma_signal_engine.core.json_io import read_json_object
    from laoma_signal_engine.runtime_health import micro_daemon_health

    cfg = EngineConfig.load(root)
    pid = _read_micro_daemon_pid(cfg.micro_daemon_cli_pid_path)
    heartbeat = None
    state = None
    if cfg.micro_daemon_cli_heartbeat_path.is_file():
        try:
            heartbeat = read_json_object(cfg.micro_daemon_cli_heartbeat_path)
        except (OSError, ValueError):
            heartbeat = None
    if cfg.micro_daemon_cli_state_path.is_file():
        try:
            state = read_json_object(cfg.micro_daemon_cli_state_path)
        except (OSError, ValueError):
            state = None
    health = micro_daemon_health(
        pid_path=cfg.micro_daemon_cli_pid_path,
        heartbeat_path=cfg.micro_daemon_cli_heartbeat_path,
        state_path=cfg.micro_daemon_cli_state_path,
        features_path=cfg.micro_daemon_cli_features_path,
        heartbeat_stale_sec=cfg.strategy_pipeline_micro_preflight_heartbeat_stale_sec,
    )
    return {
        "source": "micro_daemon_cli",
        "root": str(root),
        "pid": pid,
        "pid_running": bool(health.get("pid_running")),
        "status": health.get("status"),
        "health_state": health.get("health_state"),
        "process_registry_status": health.get("process_registry_status"),
        "data_plane_status": health.get("data_plane_status"),
        "ws_connected": health.get("ws_connected"),
        "pid_path": str(cfg.micro_daemon_cli_pid_path),
        "heartbeat_path": str(cfg.micro_daemon_cli_heartbeat_path),
        "state_path": str(cfg.micro_daemon_cli_state_path),
        "features_path": str(cfg.micro_daemon_cli_features_path),
        "heartbeat_exists": cfg.micro_daemon_cli_heartbeat_path.is_file(),
        "state_exists": cfg.micro_daemon_cli_state_path.is_file(),
        "features_exists": cfg.micro_daemon_cli_features_path.is_file(),
        "heartbeat_generated_at": heartbeat.get("generated_at") if isinstance(heartbeat, dict) else None,
        "state_generated_at": state.get("generated_at") if isinstance(state, dict) else None,
    }


def _run_micro_daemon_control_from_args(args: argparse.Namespace) -> int:
    from laoma_signal_engine.core.config_loader import EngineConfig
    from laoma_signal_engine.core.exit_codes import EXIT_SUCCESS
    from laoma_signal_engine.core.json_io import write_json_atomic
    from laoma_signal_engine.core.time_utils import to_iso_z, utc_now

    root = Path(args.project_root).resolve() if args.project_root else Path.cwd().resolve()
    cfg = EngineConfig.load(root)
    if args.action == "status":
        payload = _micro_daemon_status_payload(root)
        print(json.dumps(payload, ensure_ascii=False))
        return EXIT_SUCCESS

    pid = _read_micro_daemon_pid(cfg.micro_daemon_cli_pid_path)
    if args.action == "stop":
        running = bool(pid and _pid_running(pid))
        stop_error = None
        if running:
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError as exc:
                stop_error = str(exc)
        if not running or not stop_error:
            cfg.micro_daemon_cli_pid_path.unlink(missing_ok=True)
        payload = {
            "source": "micro_daemon_cli",
            "action": "stop",
            "pid": pid,
            "pid_running": running,
            "status": "stop_failed" if stop_error else "stopped",
        }
        if stop_error:
            payload["error"] = stop_error
        if args.stdout_json:
            print(json.dumps(payload, ensure_ascii=False))
        else:
            print("micro daemon stop failed" if stop_error else "micro daemon stopped")
        return EXIT_SUCCESS

    if pid and _pid_running(pid):
        payload = {"source": "micro_daemon_cli", "action": "start", "pid": pid, "status": "already_running"}
        print(json.dumps(payload, ensure_ascii=False) if args.stdout_json else f"micro daemon already running pid={pid}")
        return EXIT_SUCCESS
    stale_pid_backup = _backup_stale_micro_pid_file(cfg.micro_daemon_cli_pid_path) if pid else None

    cfg.micro_daemon_cli_log_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.micro_daemon_cli_pid_path.parent.mkdir(parents=True, exist_ok=True)
    rotated_log = False
    if cfg.micro_daemon_cli_log_rotation_enabled:
        rotated_log = rotate_file_if_needed(
            cfg.micro_daemon_cli_log_path,
            max_bytes=cfg.micro_daemon_cli_log_rotation_max_bytes,
            backup_count=cfg.micro_daemon_cli_log_rotation_backup_count,
        )
    transport = args.transport or cfg.micro_daemon_cli_transport
    argv = [
        sys.executable,
        "-m",
        "laoma_signal_engine.cli",
        "micro-collector-daemon",
        "--project-root",
        str(root),
        "--transport",
        str(transport),
    ]
    with open(cfg.micro_daemon_cli_log_path, "ab") as log_file:
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        env = dict(os.environ)
        env["MICRO_DAEMON_LOG_PATH"] = str(cfg.micro_daemon_cli_log_path)
        env["MICRO_DAEMON_LOG_ROTATION_ENABLED"] = "1" if cfg.micro_daemon_cli_log_rotation_enabled else "0"
        proc = subprocess.Popen(
            argv,
            cwd=str(root),
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
            env=env,
        )
    write_json_atomic(
        cfg.micro_daemon_cli_pid_path,
        {
            "schema_version": "1.0",
            "source": "micro_daemon_cli",
            "pid": proc.pid,
            "started_at": to_iso_z(utc_now()),
            "transport": transport,
            "log_path": str(cfg.micro_daemon_cli_log_path),
            "log_rotation_enabled": cfg.micro_daemon_cli_log_rotation_enabled,
            "log_rotated_before_start": rotated_log,
        },
    )
    payload = {"source": "micro_daemon_cli", "action": "start", "pid": proc.pid, "status": "started"}
    if stale_pid_backup:
        payload["stale_pid_backup"] = stale_pid_backup
    print(json.dumps(payload, ensure_ascii=False) if args.stdout_json else f"micro daemon started pid={proc.pid}")
    return EXIT_SUCCESS


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="laoma_signal_engine", description="Strategy Lab CLI")
    sub = p.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser(
        "run",
        help="run --mode once == run-pipeline. Loop mode not implemented.",
    )
    run_p.add_argument("--mode", default="once", choices=["once", "loop"])
    _register_pipeline_arguments(run_p)
    _register_persistent_micro_wait_arguments(run_p)

    pipeline_p = sub.add_parser(
        "run-pipeline",
        help="Steps 1-2.5, 3B.1 pre-micro snapshot, 3B+STEP4.1, 4, 5.0. Step 3 micro: run daemon separately.",
    )
    _register_pipeline_arguments(pipeline_p)
    _register_persistent_micro_wait_arguments(pipeline_p)

    micro_pipeline_p = sub.add_parser(
        "run-pipeline-with-micro",
        help="Steps 1-2.5, 3B.1 pre-micro snapshot, micro Step3, then 3B+STEP4.1, 4, 5.0.",
    )
    _register_pipeline_arguments(micro_pipeline_p)
    _register_micro_timed_arguments(micro_pipeline_p)

    strategy_p = sub.add_parser(
        "run-strategy-pipeline",
        help="STEP11.0: CLI/API-ready orchestrator for without_micro, micro_fast, micro_full, or all.",
    )
    _register_strategy_pipeline_arguments(strategy_p)

    micro_control_p = sub.add_parser(
        "micro-daemon",
        help="STEP11.0: start/status/stop wrapper for the persistent micro collector daemon.",
    )
    _register_micro_daemon_control_arguments(micro_control_p)

    paper_control_p = sub.add_parser(
        "paper-daemon",
        help="STEP14.6A: start/status/stop/restart wrapper for the persistent paper daemon.",
    )
    _register_paper_daemon_control_arguments(paper_control_p)

    paper_run_p = sub.add_parser(
        "paper-daemon-run",
        help="Internal STEP14.6A long-running paper daemon loop.",
    )
    _register_paper_daemon_run_arguments(paper_run_p)

    strategy4_p = sub.add_parser(
        "strategy4-observe",
        help="STEP17: sync/run/status the persistent Strategy4 observe pool.",
    )
    strategy4_p.add_argument("action", choices=["sync", "run-once", "status", "daemon"])
    strategy4_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    strategy4_p.add_argument("--run-id", default=None)
    strategy4_p.add_argument("--cycle-id", default=None)
    strategy4_p.add_argument("--max-symbols", type=int, default=None)
    strategy4_p.add_argument("--stdout-json", action="store_true", default=False)

    feishu_p = sub.add_parser(
        "feishu-send-trade-plans",
        help="STEP15: send executable trade plan signals as Feishu interactive cards.",
    )
    _register_feishu_notify_arguments(feishu_p)

    sandbox_p = sub.add_parser(
        "sandbox",
        help="STEP30: managed Strategy Sandbox create/list/status/active/delete/job gateway.",
    )
    sandbox_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    sandbox_p.add_argument("--sandbox-root", default=None, help="Override sandbox root directory.")
    sandbox_p.add_argument("--stdout-json", action="store_true", default=True, help="Emit stable JSON output.")
    sandbox_p.add_argument("--external", action="store_true", default=False, help="Mark caller as external_ai_trader via external_connector.")
    sandbox_p.add_argument("--caller-id", default=None)
    sandbox_p.add_argument("--caller-type", default=None)
    sandbox_p.add_argument("--source-surface", default=None)
    sandbox_p.add_argument("--audit-trace-id", default=None)
    sandbox_p.add_argument(
        "--allow-active-context-write",
        action="store_true",
        default=False,
        help="Explicit governance override for external set-active / create --set-active.",
    )
    sandbox_sub = sandbox_p.add_subparsers(dest="sandbox_action", required=True)

    sandbox_list_p = sandbox_sub.add_parser("list")
    sandbox_list_p.add_argument("--strategy-line", default=None)
    sandbox_list_p.add_argument("--status", default=None)
    sandbox_list_p.add_argument("--tag", default=None)
    sandbox_list_p.add_argument("--limit", type=int, default=100)
    sandbox_list_p.add_argument("--include-deleted", action="store_true", default=False)

    sandbox_create_p = sandbox_sub.add_parser("create")
    sandbox_create_p.add_argument("--strategy-line", default="experiment")
    sandbox_create_p.add_argument("--strategy-lines", default="")
    sandbox_create_p.add_argument("--strategy-version", default="review")
    sandbox_create_p.add_argument("--data-scope-json", default="{}")
    sandbox_create_p.add_argument("--config-scope-json", default="{}")
    sandbox_create_p.add_argument("--tags", default="sandbox")
    sandbox_create_p.add_argument("--set-active", action="store_true", default=False)

    sandbox_status_p = sandbox_sub.add_parser("status")
    sandbox_status_p.add_argument("--sandbox-id", required=True)

    sandbox_sub.add_parser("active")

    sandbox_sub.add_parser("resource-status")

    sandbox_resource_runs_p = sandbox_sub.add_parser("resource-runs")
    sandbox_resource_runs_p.add_argument("--resource-lane", default=None)
    sandbox_resource_runs_p.add_argument("--sandbox-id", default=None)
    sandbox_resource_runs_p.add_argument("--limit", type=int, default=100)

    sandbox_resource_run_p = sandbox_sub.add_parser("resource-run")
    sandbox_resource_run_p.add_argument("--run-id", required=True)

    sandbox_daemon_writer_p = sandbox_sub.add_parser("daemon-writer-status")
    sandbox_daemon_writer_p.add_argument("--run-id", default=None)

    sandbox_full_pipeline_p = sandbox_sub.add_parser("full-pipeline-run")
    sandbox_full_pipeline_p.add_argument("--sandbox-id", required=True)
    sandbox_full_pipeline_p.add_argument("--run-id", default=None)
    sandbox_full_pipeline_p.add_argument("--strategy-line", default=None)
    sandbox_full_pipeline_p.add_argument("--symbol", default=None)
    sandbox_full_pipeline_p.add_argument("--options-json", default="{}")

    sandbox_set_active_p = sandbox_sub.add_parser("set-active")
    sandbox_set_active_p.add_argument("--sandbox-id", required=True)

    sandbox_sub.add_parser("clear-active")

    sandbox_delete_p = sandbox_sub.add_parser("delete")
    sandbox_delete_p.add_argument("--sandbox-id", required=True)
    sandbox_delete_p.add_argument("--mode", default="soft_delete", choices=["soft_delete", "purge"])
    sandbox_delete_p.add_argument("--reason", default="cli")
    sandbox_delete_p.add_argument("--confirm-purge", action="store_true", default=False)

    sandbox_job_p = sandbox_sub.add_parser("job")
    sandbox_job_p.add_argument("--sandbox-id", required=True)
    sandbox_job_p.add_argument("--job-type", required=True)
    sandbox_job_p.add_argument("--strategy-line", default=None)
    sandbox_job_p.add_argument("--options-json", default="{}")

    bu_p = sub.add_parser("build-universe", help="Step 1: build CANDIDATE_UNIVERSE.json.")
    bu_p.add_argument("--force", action="store_true", default=False)
    bu_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: current working directory).",
    )

    fs_p = sub.add_parser(
        "fetch-futures-light-snapshot",
        help="Step 1.5: build futures_light_snapshot.json from Universe + Binance klines.",
    )
    fs_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: cwd).",
    )
    fs_p.add_argument("--limit", type=int, default=0, help="Max symbols after filter (0 = no limit).")
    fs_p.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated futures symbols; intersect with eligible list.",
    )
    fs_p.add_argument(
        "--max-concurrency",
        type=int,
        default=None,
        help="Override config max_concurrency.",
    )
    fs_p.add_argument(
        "--fetch-mode",
        default=LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT,
        choices=["legacy", "async", "distributed"],
        help="legacy=thread pool; async|distributed=Step1.51 asyncio + shared client + IP weight limiter (default async).",
    )
    fs_p.add_argument(
        "--dry-run-plan",
        action="store_true",
        help="Print planned request counts and rough weight estimate; no HTTP.",
    )
    fs_p.add_argument(
        "--stdout-json",
        action="store_true",
        help="With --dry-run-plan: print plan JSON only to stdout. No perf on stdout.",
    )
    fs_p.add_argument("--output", default=None, help="Override output JSON path.")
    snap_daemon_p = sub.add_parser(
        "snapshot-daemon",
        help="STEP1.69: run/status for persistent Step1.5 snapshot shard daemon.",
    )
    snap_daemon_p.add_argument("action", choices=["run", "tick", "status"], help="Daemon action.")
    snap_daemon_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    snap_daemon_p.add_argument("--max-ticks", type=int, default=None, help="For action=run, stop after N ticks.")
    snap_daemon_p.add_argument("--stdout-json", action="store_true", help="Emit status JSON for status/tick.")
    val_p = sub.add_parser(
        "validate-light-snapshot",
        help="Regression checks on futures_light_snapshot.json (Step 1.5).",
    )
    val_p.add_argument(
        "--input",
        required=True,
        help="Path to futures_light_snapshot.json",
    )
    val_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root (for optional universe order check).",
    )
    val_p.add_argument(
        "--universe",
        default=None,
        help="Optional CANDIDATE_UNIVERSE.json path to verify item order vs eligible list.",
    )
    val_p.add_argument(
        "--stdout-json",
        action="store_true",
        help="Emit validation result as one JSON line on stdout.",
    )
    liq_p = sub.add_parser(
        "fetch-market-entry-liquidity",
        help="STEP1.6: build latest_market_entry_liquidity.json from current light snapshot + Binance depth.",
    )
    liq_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    liq_p.add_argument("--light", default=None, help="Override futures_light_snapshot.json path.")
    liq_p.add_argument("--output", default=None, help="Override latest_market_entry_liquidity.json path.")
    liq_p.add_argument("--symbols", default=None, help="Comma-separated symbols; default all light snapshot items.")
    liq_p.add_argument("--stdout-json", action="store_true", default=False)

    refresh_p = sub.add_parser(
        "refresh-decision-candidates",
        help="STEP4.3A: refresh current factor symbols before market-entry decision.",
    )
    refresh_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    refresh_p.add_argument("--factor", default=None, help="Override latest_factor_snapshot.json path.")
    refresh_p.add_argument("--light", default=None, help="Override futures_light_snapshot.json path.")
    refresh_p.add_argument("--liquidity", default=None, help="Override latest_market_entry_liquidity.json path.")
    refresh_p.add_argument("--output", default=None, help="Override latest_decision_refresh_snapshot.json path.")
    refresh_p.add_argument("--fetch-mode", default=LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT, choices=["legacy", "async", "distributed"])
    refresh_p.add_argument("--max-concurrency", type=int, default=None)
    refresh_p.add_argument("--max-refresh-age-sec", type=int, default=180)
    refresh_p.add_argument("--max-liquidity-age-sec", type=int, default=180)
    refresh_p.add_argument(
        "--skip-fetch-latest",
        action="store_true",
        default=False,
        help="Do not call Binance; build refresh contract from the provided/current light snapshot.",
    )
    refresh_p.add_argument(
        "--skip-fetch-liquidity",
        action="store_true",
        default=False,
        help="Do not refresh liquidity for current candidates; reuse provided/current liquidity snapshot.",
    )
    refresh_p.add_argument("--stdout-json", action="store_true", default=False)

    scan_p = sub.add_parser("scan", help="Step 2.0: abnormal scanner (raw/watch/strong).")
    scan_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: cwd).",
    )
    scan_p.add_argument(
        "--input",
        default=None,
        help="Override futures_light_snapshot.json path.",
    )
    scan_p.add_argument(
        "--universe",
        default=None,
        help="Override CANDIDATE_UNIVERSE.json path.",
    )
    scan_p.add_argument(
        "--stdout-json",
        action="store_true",
        help="Emit one JSON summary line on stdout; logs stay on stderr.",
    )
    scan_p.add_argument(
        "--allow-stale-input",
        action="store_true",
        default=False,
        help="Allow stale snapshot (Step 2.1 dev); output status ok_dev_stale_allowed.",
    )
    scan_p.add_argument(
        "--strict-freshness",
        action="store_true",
        default=False,
        help="Force strict freshness (degraded also rejected) for this run.",
    )
    scan_p.add_argument(
        "--max-snapshot-age-sec",
        type=int,
        default=None,
        help="Override only hard stale threshold (sec); does not change fresh/degraded boundary.",
    )
    route_p = sub.add_parser("route-micro-targets", help="Step 2.5: micro target router.")
    route_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: cwd).",
    )
    route_p.add_argument("--raw", default=None, help="Override latest_raw_candidates.json path.")
    route_p.add_argument("--watch", default=None, help="Override latest_watch_signals.json path.")
    route_p.add_argument("--strong", default=None, help="Override latest_strong_candidates.json path.")
    route_p.add_argument("--output", default=None, help="Override micro_targets.json path.")
    route_p.add_argument(
        "--stdout-json",
        action="store_true",
        help="Emit one JSON summary line on stdout; logs stay on stderr.",
    )

    decide_p = sub.add_parser("decide", help="Decision for specific symbols (stub).")
    decide_p.add_argument("--symbols", default="", help="Comma-separated list, e.g. BTCUSDT,ETHUSDT")
    decide_p.add_argument("--stdout", action="store_true", default=False)

    micro_daemon_p = sub.add_parser(
        "micro-collector-daemon",
        help="Persistent Step 3 micro collector daemon; watches DATA/micro/micro_targets.json.",
    )
    _register_micro_daemon_arguments(micro_daemon_p)
    micro_alias_p = sub.add_parser("micro-collector", help="Alias of micro-collector-daemon.")
    _register_micro_daemon_arguments(micro_alias_p)

    fsnap_p = sub.add_parser(
        "assemble-factor-snapshot",
        help="Step 3B: merge watch+strong + light snapshot + micro -> latest_factor_snapshot.json.",
    )
    fsnap_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: cwd).",
    )
    fsnap_p.add_argument("--watch", default=None, help="Override latest_watch_signals.json path.")
    fsnap_p.add_argument("--strong", default=None, help="Override latest_strong_candidates.json path.")
    fsnap_p.add_argument("--light", default=None, help="Override futures_light_snapshot.json path.")
    fsnap_p.add_argument("--micro", default=None, help="Override latest_micro_features.json path.")
    fsnap_p.add_argument("--output", default=None, help="Override latest_factor_snapshot.json path.")
    fsnap_p.add_argument(
        "--skip-market-context",
        action="store_true",
        default=False,
        help="Do not call Binance for OI/Funding/Basis (placeholders / tests).",
    )
    fsnap_p.add_argument(
        "--stdout-json",
        action="store_true",
        help="Emit one JSON summary line on stdout; logs stay on stderr.",
    )

    fsnap_nm_p = sub.add_parser(
        "assemble-factor-snapshot-without-ofi-cvd",
        help="Step 3B.1: watch+strong + light + OI/Funding/Basis REST; no micro file. See docs/STEP3B.1.",
    )
    fsnap_nm_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: cwd).",
    )
    fsnap_nm_p.add_argument("--watch", default=None, help="Override latest_watch_signals.json path.")
    fsnap_nm_p.add_argument("--strong", default=None, help="Override latest_strong_candidates.json path.")
    fsnap_nm_p.add_argument("--light", default=None, help="Override futures_light_snapshot.json path.")
    fsnap_nm_p.add_argument(
        "--output",
        default=None,
        help="Override latest_factor_snapshot_withoutoficvd.json path.",
    )
    fsnap_nm_p.add_argument(
        "--skip-market-context",
        action="store_true",
        default=False,
        help="Do not call Binance for OI/Funding/Basis (placeholders / tests).",
    )
    fsnap_nm_p.add_argument(
        "--stdout-json",
        action="store_true",
        help="Emit one JSON summary line on stdout; logs stay on stderr.",
    )

    dg_p = sub.add_parser(
        "apply-direction-gate",
        help="Step 4: latest_factor_snapshot.json -> latest_direction_decisions.json.",
    )
    dg_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: cwd).",
    )
    dg_p.add_argument("--factor", default=None, help="Override latest_factor_snapshot.json path.")
    dg_p.add_argument("--output", default=None, help="Override latest_direction_decisions.json path.")
    dg_p.add_argument(
        "--allow-watch-now",
        action="store_true",
        default=False,
        help="Allow LONG_NOW/SHORT_NOW for watch_candidate (default off).",
    )
    dg_p.add_argument(
        "--disable-context-guards-for-now",
        action="store_true",
        default=False,
        help="Do not require OI/Funding/Basis ready for NOW (smoke only).",
    )
    dg_p.add_argument(
        "--stdout-json",
        action="store_true",
        help="Emit one JSON summary line on stdout; logs stay on stderr.",
    )

    medg_p = sub.add_parser(
        "apply-market-entry-direction-gate",
        help="STEP4.3: factor + decision refresh + fast micro -> market-entry direction decisions.",
    )
    medg_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    medg_p.add_argument("--factor", default=None, help="Override latest_factor_snapshot.json path.")
    medg_p.add_argument("--refresh", default=None, help="Override latest_decision_refresh_snapshot.json path.")
    medg_p.add_argument("--micro", default=None, help="Override latest_micro_features.json path.")
    medg_p.add_argument("--output", default=None, help="Override latest_market_entry_direction_decisions.json path.")
    medg_p.add_argument("--allow-watch-market-entry", action="store_true", default=False)
    medg_p.add_argument("--stdout-json", action="store_true", default=False)

    mesltp_p = sub.add_parser(
        "apply-market-entry-sl-tp",
        help="STEP5.1: market-entry direction decisions + refresh -> market entry SL/TP plans.",
    )
    mesltp_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    mesltp_p.add_argument("--direction", default=None, help="Override latest_market_entry_direction_decisions.json path.")
    mesltp_p.add_argument("--refresh", default=None, help="Override latest_decision_refresh_snapshot.json path.")
    mesltp_p.add_argument("--output", default=None, help="Override latest_market_entry_decisions.json path.")
    mesltp_p.add_argument("--stdout-json", action="store_true", default=False)

    tpl_p = sub.add_parser(
        "apply-trade-plan-line",
        help="P10: build one independent trade plan line final JSON.",
    )
    tpl_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    tpl_p.add_argument("--line", required=True, choices=["without_micro", "micro_fast", "micro_full", "strategy4"])
    tpl_p.add_argument("--factor", default=None, help="Override factor snapshot path.")
    tpl_p.add_argument("--refresh", default=None, help="Override latest_decision_refresh_snapshot.json path.")
    tpl_p.add_argument("--liquidity", default=None, help="Override latest_market_entry_liquidity.json path.")
    tpl_p.add_argument("--micro", default=None, help="Override latest_micro_features.json path.")
    tpl_p.add_argument("--micro-state", default=None, help="Override latest_micro_state.json path.")
    tpl_p.add_argument("--output", default=None, help="Override line output JSON path.")
    tpl_p.add_argument("--run-id", default=None, help="Scheduler run_id for this line output.")
    tpl_p.add_argument("--cycle-id", default=None, help="Scheduler cycle_id for this line output.")
    tpl_p.add_argument("--stdout-json", action="store_true", default=False)

    tpla_p = sub.add_parser(
        "audit-trade-plan-lines",
        help="P10.5: read-only ABC audit for independent trade plan line JSON files.",
    )
    tpla_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    tpla_p.add_argument("--output", default=None, help="Override latest_trade_plan_lines_compare.json path.")
    tpla_p.add_argument("--stdout-json", action="store_true", default=False)

    run_audit_p = sub.add_parser(
        "build-run-audit",
        help="STEP7.15: build run-level chain audit JSON from current run artifacts.",
    )
    run_audit_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    run_audit_p.add_argument("--output", default=None, help="Override DATA/reports/latest_run_audit.json path.")
    run_audit_p.add_argument("--run-id", default=None, help="Optional expected run_id.")
    run_audit_p.add_argument("--cycle-id", default=None, help="Optional expected cycle_id.")
    run_audit_p.add_argument("--stdout-json", action="store_true", default=False)

    run_audit_ingest_p = sub.add_parser(
        "ingest-run-audit",
        help="STEP7.16: ingest latest run audit JSON and current evidence into audit SQLite.",
    )
    run_audit_ingest_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    run_audit_ingest_p.add_argument("--audit", default=None, help="Override run audit JSON path.")
    run_audit_ingest_p.add_argument("--db", default=None, help="Override DATA/audit/run_audit.db path.")
    run_audit_ingest_p.add_argument("--stdout-json", action="store_true", default=False)

    micro_quality_audit_p = sub.add_parser(
        "audit-micro-quality",
        help="STEP10.43: build micro CVD/OFI/full-z data quality attribution report.",
    )
    micro_quality_audit_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    micro_quality_audit_p.add_argument("--output-json", default=None, help="Override STEP10.43 findings JSON path.")
    micro_quality_audit_p.add_argument("--output-md", default=None, help="Override STEP10.43 markdown report path.")
    micro_quality_audit_p.add_argument("--db", default=None, help="Override DATA/audit/run_audit.db path.")
    micro_quality_audit_p.add_argument("--expected-run-id", default=None, help="Expected pipeline run_id for stale guard.")
    micro_quality_audit_p.add_argument("--expected-cycle-id", default=None, help="Expected pipeline cycle_id for stale guard.")
    micro_quality_audit_p.add_argument(
        "--selected-lines",
        default=None,
        help="Comma-separated selected micro lines for STEP10.43 lineage scoping.",
    )
    micro_quality_audit_p.add_argument("--non-blocking", action="store_true", default=False)
    micro_quality_audit_p.add_argument("--stdout-json", action="store_true", default=False)

    micro_quality_soak_p = sub.add_parser(
        "audit-micro-quality-soak",
        help="STEP10.50: aggregate recent run micro data quality attribution and downstream consumption.",
    )
    micro_quality_soak_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    micro_quality_soak_p.add_argument("--output-json", default=None, help="Override STEP10.50 findings JSON path.")
    micro_quality_soak_p.add_argument("--output-md", default=None, help="Override STEP10.50 markdown report path.")
    micro_quality_soak_p.add_argument("--db", default=None, help="Override DATA/audit/run_audit.db path.")
    micro_quality_soak_p.add_argument("--lookback-runs", type=int, default=20)
    micro_quality_soak_p.add_argument("--min-runs", type=int, default=10)
    micro_quality_soak_p.add_argument("--stdout-json", action="store_true", default=False)

    sched_p = sub.add_parser(
        "run-trade-plan-cycle",
        help="STEP10.6: guarded 5m scheduler cycle for P10 trade plan lines.",
    )
    sched_p.add_argument("--project-root", default=None, help="Repo root for DATA/ paths (default: cwd).")
    sched_p.add_argument("--lock", default=None, help="Override scheduler lock path.")
    sched_p.add_argument("--report", default=None, help="Override scheduler report path.")
    sched_p.add_argument("--overlap-policy", choices=["skip", "merge"], default="skip")
    sched_p.add_argument("--lock-ttl-sec", type=int, default=540)
    sched_p.add_argument("--wait-target-ack", action="store_true", default=False)
    sched_p.add_argument("--target-ack-timeout-sec", type=float, default=10.0)
    sched_p.add_argument("--stdout-json", action="store_true", default=False)

    fd_p = sub.add_parser(
        "apply-final-decisions",
        help="Step 5.0: latest_direction_decisions + factor (+ light) -> latest_decisions.json.",
    )
    fd_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: cwd).",
    )
    fd_p.add_argument("--direction", default=None, help="Override latest_direction_decisions.json path.")
    fd_p.add_argument("--factor", default=None, help="Override latest_factor_snapshot.json path.")
    fd_p.add_argument("--light", default=None, help="Override futures_light_snapshot.json path.")
    fd_p.add_argument("--output", default=None, help="Override latest_decisions.json path.")
    fd_p.add_argument(
        "--stdout-json",
        action="store_true",
        help="Emit one JSON summary line on stdout; logs stay on stderr.",
    )

    fda_p = sub.add_parser(
        "aggregate-final-decisions-from-trade-plans",
        help="Step 5.2: P10 trade plan lines -> latest_decisions.json compatibility output.",
    )
    fda_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: cwd).",
    )
    fda_p.add_argument("--output", default=None, help="Override latest_decisions.json path.")
    fda_p.add_argument(
        "--stdout-json",
        action="store_true",
        help="Emit one JSON summary line on stdout; logs stay on stderr.",
    )

    llm_p = sub.add_parser(
        "run-llm-factor-assist",
        help=(
            "STEP6.0: call DeepSeek with two factor JSON + matching prompt .txt; "
            "write DATA/llm/out/llm_out_*.json (needs DEEPSEEK_API_KEY)."
        ),
    )
    llm_p.add_argument(
        "--project-root",
        default=None,
        help="Repo root for DATA/ paths (default: cwd).",
    )
    llm_p.add_argument(
        "--stdout-json",
        action="store_true",
        default=False,
        help="Emit batch summary JSON one line on stdout.",
    )
    llm_p.add_argument("--factor-a", default=None, help="Override DATA/factors/latest_factor_snapshot.json")
    llm_p.add_argument(
        "--prompt-a",
        default=None,
        help="Override DATA/llm/prompts/latest_factor_snapshot.txt",
    )
    llm_p.add_argument(
        "--out-a",
        default=None,
        help="Override DATA/llm/out/llm_out_latest_factor_snapshot.json",
    )
    llm_p.add_argument(
        "--factor-b",
        default=None,
        help="Override DATA/factors/latest_factor_snapshot_withoutoficvd.json",
    )
    llm_p.add_argument(
        "--prompt-b",
        default=None,
        help="Override DATA/llm/prompts/latest_factor_snapshot_withoutoficvd.txt",
    )
    llm_p.add_argument(
        "--out-b",
        default=None,
        help="Override DATA/llm/out/llm_out_latest_factor_snapshot_withoutoficvd.json",
    )
    llm_p.add_argument(
        "--max-factor-items",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Send only the first N symbols from each factor file to the LLM (full file still validated). "
            "0 = send all items. Use small N for cheaper tests (e.g. 2)."
        ),
    )

    return p


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    setup_stderr_logging()
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    cmd = args.command

    if cmd == "run":
        if args.mode == "once":
            return _run_pipeline_from_args(args)
        return _cmd_not_implemented("run loop")
    if cmd == "run-pipeline":
        return _run_pipeline_from_args(args)
    if cmd == "run-pipeline-with-micro":
        return _run_full_micro_pipeline_from_args(args)
    if cmd == "run-strategy-pipeline":
        return _run_strategy_pipeline_from_args(args)
    if cmd == "micro-daemon":
        return _run_micro_daemon_control_from_args(args)
    if cmd == "paper-daemon":
        return _run_paper_daemon_control_from_args(args)
    if cmd == "paper-daemon-run":
        return _run_paper_daemon_forever_from_args(args)
    if cmd == "strategy4-observe":
        from laoma_signal_engine.strategy4.observe import main as strategy4_main

        argv = [args.action]
        if args.project_root:
            argv.extend(["--project-root", str(args.project_root)])
        if args.run_id:
            argv.extend(["--run-id", str(args.run_id)])
        if args.cycle_id:
            argv.extend(["--cycle-id", str(args.cycle_id)])
        if args.max_symbols is not None:
            argv.extend(["--max-symbols", str(args.max_symbols)])
        if args.stdout_json:
            argv.append("--stdout-json")
        return strategy4_main(argv)
    if cmd == "feishu-send-trade-plans":
        return _run_feishu_notify_from_args(args)
    if cmd == "sandbox":
        return _run_sandbox_from_args(args)
    if cmd == "build-universe":
        from laoma_signal_engine.universe.candidate_universe import run_build_universe_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        return run_build_universe_safe(force=args.force, project_root=root)
    if cmd == "fetch-futures-light-snapshot":
        from laoma_signal_engine.market.futures_light_snapshot import run_fetch_light_snapshot_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        lim = args.limit if args.limit and args.limit > 0 else None
        syms = None
        if args.symbols:
            syms = [x.strip() for x in args.symbols.split(",") if x.strip()]
        out = Path(args.output).resolve() if args.output else None
        return run_fetch_light_snapshot_safe(
            project_root=root,
            limit=lim,
            symbols_filter=syms,
            max_concurrency=args.max_concurrency,
            output_path=out,
            fetch_mode=args.fetch_mode,
            dry_run_plan=args.dry_run_plan,
            stdout_json=args.stdout_json,
        )
    if cmd == "snapshot-daemon":
        from laoma_signal_engine.market.snapshot_daemon import (
            run_snapshot_daemon_forever_safe,
            run_snapshot_daemon_tick_safe,
            snapshot_daemon_status,
        )

        root = Path(args.project_root).resolve() if args.project_root else None
        if args.action == "status":
            payload = snapshot_daemon_status(project_root=root)
            if args.stdout_json:
                print(json.dumps(payload, ensure_ascii=False))
            else:
                print(f"snapshot_daemon_status={payload.get('daemon_status')} health={payload.get('status')}")
            return 0
        if args.action == "tick":
            rc = run_snapshot_daemon_tick_safe(project_root=root)
            if args.stdout_json:
                print(json.dumps(snapshot_daemon_status(project_root=root), ensure_ascii=False))
            return rc
        return run_snapshot_daemon_forever_safe(project_root=root, max_ticks=args.max_ticks)
    if cmd == "validate-light-snapshot":
        from laoma_signal_engine.market.validate_light_snapshot import run_validate_light_snapshot

        root = Path(args.project_root).resolve() if args.project_root else None
        inp = Path(args.input).resolve()
        uni = Path(args.universe).resolve() if args.universe else None
        return run_validate_light_snapshot(
            input_path=inp,
            stdout_json=args.stdout_json,
            universe_order_path=uni,
        )
    if cmd == "fetch-market-entry-liquidity":
        from laoma_signal_engine.market.market_entry_liquidity import run_fetch_market_entry_liquidity_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        lp = Path(args.light).resolve() if args.light else None
        out = Path(args.output).resolve() if args.output else None
        syms = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
        return run_fetch_market_entry_liquidity_safe(
            project_root=root,
            light_path=lp,
            output_path=out,
            symbols=syms,
            stdout_json=args.stdout_json,
        )
    if cmd == "refresh-decision-candidates":
        from laoma_signal_engine.market.decision_refresh import run_pre_decision_candidate_refresh_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        fp = Path(args.factor).resolve() if args.factor else None
        lp = Path(args.light).resolve() if args.light else None
        liq = Path(args.liquidity).resolve() if args.liquidity else None
        out = Path(args.output).resolve() if args.output else None
        return run_pre_decision_candidate_refresh_safe(
            project_root=root,
            factor_path=fp,
            light_path=lp,
            liquidity_path=liq,
            output_path=out,
            fetch_latest=not args.skip_fetch_latest,
            fetch_mode=args.fetch_mode,
            max_concurrency=args.max_concurrency,
            max_refresh_age_sec=args.max_refresh_age_sec,
            max_liquidity_age_sec=args.max_liquidity_age_sec,
            refresh_liquidity=not args.skip_fetch_liquidity,
            stdout_json=args.stdout_json,
        )
    if cmd == "scan":
        from laoma_signal_engine.scanner.abnormal_scanner import run_abnormal_scan_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        inp = Path(args.input).resolve() if args.input else None
        uni = Path(args.universe).resolve() if args.universe else None
        return run_abnormal_scan_safe(
            project_root=root,
            snapshot_path=inp,
            universe_path=uni,
            stdout_json=args.stdout_json,
            allow_stale_input=args.allow_stale_input,
            strict_freshness_cli=args.strict_freshness,
            max_snapshot_age_sec=args.max_snapshot_age_sec,
        )
    if cmd == "route-micro-targets":
        from laoma_signal_engine.micro.micro_target_router import run_micro_target_router_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        rp = Path(args.raw).resolve() if args.raw else None
        wp = Path(args.watch).resolve() if args.watch else None
        sp = Path(args.strong).resolve() if args.strong else None
        out = Path(args.output).resolve() if args.output else None
        return run_micro_target_router_safe(
            project_root=root,
            raw_path=rp,
            watch_path=wp,
            strong_path=sp,
            output_path=out,
            stdout_json=args.stdout_json,
        )
    if cmd == "decide":
        return _cmd_not_implemented("decide")
    if cmd in ("micro-collector", "micro-collector-daemon"):
        return _run_micro_daemon_from_args(args)
    if cmd == "assemble-factor-snapshot":
        from laoma_signal_engine.factors.factor_snapshot import run_assemble_factor_snapshot_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        wp = Path(args.watch).resolve() if args.watch else None
        sp = Path(args.strong).resolve() if args.strong else None
        lp = Path(args.light).resolve() if args.light else None
        mp = Path(args.micro).resolve() if args.micro else None
        out = Path(args.output).resolve() if args.output else None
        return run_assemble_factor_snapshot_safe(
            project_root=root,
            watch_path=wp,
            strong_path=sp,
            light_path=lp,
            micro_path=mp,
            output_path=out,
            stdout_json=args.stdout_json,
            skip_market_context=args.skip_market_context,
        )
    if cmd == "assemble-factor-snapshot-without-ofi-cvd":
        from laoma_signal_engine.factors.factor_snapshot import run_assemble_factor_snapshot_without_ofi_cvd_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        wp = Path(args.watch).resolve() if args.watch else None
        sp = Path(args.strong).resolve() if args.strong else None
        lp = Path(args.light).resolve() if args.light else None
        out = Path(args.output).resolve() if args.output else None
        return run_assemble_factor_snapshot_without_ofi_cvd_safe(
            project_root=root,
            watch_path=wp,
            strong_path=sp,
            light_path=lp,
            output_path=out,
            stdout_json=args.stdout_json,
            skip_market_context=args.skip_market_context,
        )
    if cmd == "apply-direction-gate":
        from laoma_signal_engine.decision.direction_gate import run_apply_direction_gate_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        fp = Path(args.factor).resolve() if args.factor else None
        out = Path(args.output).resolve() if args.output else None
        return run_apply_direction_gate_safe(
            project_root=root,
            factor_path=fp,
            output_path=out,
            stdout_json=args.stdout_json,
            allow_watch_now=args.allow_watch_now,
            require_context_guards_for_now=not args.disable_context_guards_for_now,
        )
    if cmd == "apply-market-entry-direction-gate":
        from laoma_signal_engine.decision.market_entry_direction_gate import run_apply_market_entry_direction_gate_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        fp = Path(args.factor).resolve() if args.factor else None
        rp = Path(args.refresh).resolve() if args.refresh else None
        mp = Path(args.micro).resolve() if args.micro else None
        out = Path(args.output).resolve() if args.output else None
        return run_apply_market_entry_direction_gate_safe(
            project_root=root,
            factor_path=fp,
            refresh_path=rp,
            micro_path=mp,
            output_path=out,
            stdout_json=args.stdout_json,
            allow_watch_market_entry=True if args.allow_watch_market_entry else None,
        )
    if cmd == "apply-market-entry-sl-tp":
        from laoma_signal_engine.decision.market_entry_sl_tp_planner import run_apply_market_entry_sl_tp_planner_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        dp = Path(args.direction).resolve() if args.direction else None
        rp = Path(args.refresh).resolve() if args.refresh else None
        out = Path(args.output).resolve() if args.output else None
        return run_apply_market_entry_sl_tp_planner_safe(
            project_root=root,
            direction_path=dp,
            refresh_path=rp,
            output_path=out,
            stdout_json=args.stdout_json,
        )
    if cmd == "apply-trade-plan-line":
        from laoma_signal_engine.decision.trade_plan_lines import run_apply_trade_plan_line_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        fp = Path(args.factor).resolve() if args.factor else None
        rp = Path(args.refresh).resolve() if args.refresh else None
        lp = Path(args.liquidity).resolve() if args.liquidity else None
        mp = Path(args.micro).resolve() if args.micro else None
        msp = Path(args.micro_state).resolve() if args.micro_state else None
        out = Path(args.output).resolve() if args.output else None
        return run_apply_trade_plan_line_safe(
            line=args.line,
            project_root=root,
            factor_path=fp,
            refresh_path=rp,
            liquidity_path=lp,
            micro_path=mp,
            micro_state_path=msp,
            output_path=out,
            run_id=args.run_id,
            cycle_id=args.cycle_id,
            stdout_json=args.stdout_json,
        )
    if cmd == "audit-trade-plan-lines":
        from laoma_signal_engine.decision.trade_plan_lines_audit import run_audit_trade_plan_lines_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        out = Path(args.output).resolve() if args.output else None
        return run_audit_trade_plan_lines_safe(
            project_root=root,
            output_path=out,
            stdout_json=args.stdout_json,
        )
    if cmd == "build-run-audit":
        from laoma_signal_engine.audit.run_audit import run_build_run_audit_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        out = Path(args.output).resolve() if args.output else None
        return run_build_run_audit_safe(
            project_root=root,
            output_path=out,
            run_id=args.run_id,
            cycle_id=args.cycle_id,
            stdout_json=args.stdout_json,
        )
    if cmd == "ingest-run-audit":
        from laoma_signal_engine.audit.run_audit import run_ingest_run_audit_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        audit = Path(args.audit).resolve() if args.audit else None
        db = Path(args.db).resolve() if args.db else None
        return run_ingest_run_audit_safe(project_root=root, audit_path=audit, db_path=db, stdout_json=args.stdout_json)
    if cmd == "audit-micro-quality":
        from laoma_signal_engine.micro.data_quality_attribution import run_write_micro_quality_attribution_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        output_json = Path(args.output_json).resolve() if args.output_json else None
        output_md = Path(args.output_md).resolve() if args.output_md else None
        db = Path(args.db).resolve() if args.db else None
        return run_write_micro_quality_attribution_safe(
            project_root=root,
            output_json=output_json,
            output_md=output_md,
            db_path=db,
            expected_run_id=args.expected_run_id,
            expected_cycle_id=args.expected_cycle_id,
            selected_lines=[x.strip() for x in str(args.selected_lines or "").split(",") if x.strip()],
            non_blocking=args.non_blocking,
            stdout_json=args.stdout_json,
        )
    if cmd == "audit-micro-quality-soak":
        from laoma_signal_engine.micro.data_quality_soak_audit import run_write_micro_data_quality_soak_audit_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        output_json = Path(args.output_json).resolve() if args.output_json else None
        output_md = Path(args.output_md).resolve() if args.output_md else None
        db = Path(args.db).resolve() if args.db else None
        return run_write_micro_data_quality_soak_audit_safe(
            project_root=root,
            output_json=output_json,
            output_md=output_md,
            db_path=db,
            lookback_runs=args.lookback_runs,
            min_runs=args.min_runs,
            stdout_json=args.stdout_json,
        )
    if cmd == "run-trade-plan-cycle":
        from laoma_signal_engine.scheduler_5m import run_trade_plan_cycle_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        lock = Path(args.lock).resolve() if args.lock else None
        report = Path(args.report).resolve() if args.report else None
        return run_trade_plan_cycle_safe(
            project_root=root,
            lock_path=lock,
            report_path=report,
            overlap_policy=args.overlap_policy,
            lock_ttl_sec=args.lock_ttl_sec,
            wait_target_ack=args.wait_target_ack,
            target_ack_timeout_sec=args.target_ack_timeout_sec,
            stdout_json=args.stdout_json,
        )
    if cmd == "apply-final-decisions":
        from laoma_signal_engine.decision.final_decisions import run_apply_final_decisions_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        dp = Path(args.direction).resolve() if args.direction else None
        fp = Path(args.factor).resolve() if args.factor else None
        lp = Path(args.light).resolve() if args.light else None
        out = Path(args.output).resolve() if args.output else None
        return run_apply_final_decisions_safe(
            project_root=root,
            direction_path=dp,
            factor_path=fp,
            light_path=lp,
            output_path=out,
            stdout_json=args.stdout_json,
        )
    if cmd == "aggregate-final-decisions-from-trade-plans":
        from laoma_signal_engine.decision.final_decisions import run_apply_final_decisions_from_trade_plans_safe

        root = Path(args.project_root).resolve() if args.project_root else None
        out = Path(args.output).resolve() if args.output else None
        return run_apply_final_decisions_from_trade_plans_safe(
            project_root=root,
            output_path=out,
            stdout_json=args.stdout_json,
        )

    if cmd == "run-llm-factor-assist":
        from laoma_signal_engine.llm.run_factor_assist import (
            default_factor_assist_pairs,
            run_llm_factor_assist_twice_safe,
        )

        root = Path(args.project_root).resolve() if args.project_root else Path.cwd().resolve()
        defaults = default_factor_assist_pairs(root)
        ov = (args.factor_a, args.prompt_a, args.out_a, args.factor_b, args.prompt_b, args.out_b)
        pairs = None
        if any(ov):
            pairs = [
                (
                    Path(args.factor_a).resolve() if args.factor_a else defaults[0][0],
                    Path(args.prompt_a).resolve() if args.prompt_a else defaults[0][1],
                    Path(args.out_a).resolve() if args.out_a else defaults[0][2],
                ),
                (
                    Path(args.factor_b).resolve() if args.factor_b else defaults[1][0],
                    Path(args.prompt_b).resolve() if args.prompt_b else defaults[1][1],
                    Path(args.out_b).resolve() if args.out_b else defaults[1][2],
                ),
            ]
        return run_llm_factor_assist_twice_safe(
            project_root=root,
            stdout_json=args.stdout_json,
            pairs=pairs,
            max_factor_items=args.max_factor_items if args.max_factor_items > 0 else None,
        )

    print(f"[ERROR] unknown command: {cmd}", file=sys.stderr)
    return EXIT_CONFIG


if __name__ == "__main__":
    raise SystemExit(main())
