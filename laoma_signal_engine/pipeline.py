"""Orchestrates Step 1 through Step 5: universe, light snapshot, scan, route, micro (separate), 3B, 4, 5.0.

Step 5.0 writes DATA/decisions/latest_decisions.json (planner + risk gate). See docs/STEP5.0.
"""

from __future__ import annotations

import asyncio
import logging
import yaml
from dataclasses import replace
from pathlib import Path
from typing import Any, Literal

from laoma_signal_engine.micro.wait_until_ready.config import load_wait_until_ready_config
from laoma_signal_engine.micro.wait_until_ready.runner import run_wait_until_ready_orchestration

from laoma_signal_engine.core.config_loader import EngineConfig, package_root
from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.models import CandidateUniverseDocument
from laoma_signal_engine.core.time_utils import utc_now
from laoma_signal_engine.universe.cache import universe_cache_is_fresh

# Step 1.5 default transport: Step 1.51 asyncio + shared client + IP weight limiter (orchestration + CLI).
LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT = "async"

TransportKind = Literal["fake", "real"]
log = logging.getLogger(__name__)


def _load_pipeline_yaml_flags() -> tuple[bool, bool]:
    """(skip_final_decisions, skip_factor_snapshot_without_ofi_cvd)."""
    cfg_path = package_root() / "config" / "default.yaml"
    try:
        raw_text = cfg_path.read_text(encoding="utf-8")
    except OSError:
        return False, False
    doc: dict[str, Any] = yaml.safe_load(raw_text) or {}
    pipe = doc.get("pipeline") or {}
    return (
        bool(pipe.get("skip_final_decisions", False)),
        bool(pipe.get("skip_factor_snapshot_without_ofi_cvd", False)),
    )


def _pipeline_skip_final_decisions_from_config() -> bool:
    return _load_pipeline_yaml_flags()[0]


def run_pipeline_once(use_cached_universe: bool = True) -> None:
    raise NotImplementedError("pipeline.run_pipeline_once")


def load_candidate_universe_if_fresh(
    project_root: Path | None = None,
) -> CandidateUniverseDocument | None:
    """Return validated document when cache is fresh; otherwise None (caller may rebuild)."""
    cfg = EngineConfig.load(project_root)
    if not universe_cache_is_fresh(cfg.candidate_universe_path, cfg.schema_version, utc_now()):
        return None
    data = read_json_object(cfg.candidate_universe_path)
    return CandidateUniverseDocument.model_validate(data)


def load_candidate_universe_required(project_root: Path | None = None) -> CandidateUniverseDocument:
    """Load fresh universe or raise with a clear message."""
    doc = load_candidate_universe_if_fresh(project_root)
    if doc is None:
        raise FileNotFoundError(
            "CANDIDATE_UNIVERSE.json missing or stale; run build-universe (use --force after schema bump).",
        )
    return doc


def _permissive_micro_quality_config():
    """Relaxed quality gates for real-WS smoke only (matches micro.daemon.cli)."""
    from laoma_signal_engine.micro.quality.models import MicroQualityConfig

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


def _run_step_3b1_without_ofi_cvd(
    pr: Path,
    *,
    stdout_json: bool,
    skip_market_context: bool,
    skip_factor_snapshot_without_ofi_cvd_cli: bool,
) -> int:
    """Write DATA/factors/latest_factor_snapshot_withoutoficvd.json. docs/STEP3B.1."""
    from laoma_signal_engine.core.exit_codes import EXIT_SUCCESS
    from laoma_signal_engine.factors.factor_snapshot import run_assemble_factor_snapshot_without_ofi_cvd_safe

    skip_cfg = _load_pipeline_yaml_flags()[1]
    skip_effective = skip_factor_snapshot_without_ofi_cvd_cli or skip_cfg
    if skip_effective:
        log.info(
            "pipeline: skipping Step 3B.1 without OFI/CVD snapshot (cli=%s config=%s)",
            skip_factor_snapshot_without_ofi_cvd_cli,
            skip_cfg,
        )
        return EXIT_SUCCESS
    log.info("pipeline: Step 3B.1 factor snapshot without OFI/CVD (pre-micro)")
    rc = run_assemble_factor_snapshot_without_ofi_cvd_safe(
        project_root=pr,
        stdout_json=stdout_json,
        skip_market_context=skip_market_context,
    )
    if rc != EXIT_SUCCESS:
        log.error("pipeline: Step 3B.1 failed exit=%s", rc)
    return rc


def run_pipeline_pre_micro_safe(
    *,
    project_root: Path | None = None,
    force_universe: bool = False,
    skip_universe: bool = False,
    skip_fetch_light_snapshot: bool = False,
    skip_scan_and_route: bool = False,
    light_limit: int = 0,
    light_symbols: list[str] | None = None,
    fetch_mode: str | None = None,
    max_concurrency: int | None = None,
    scan_allow_stale_input: bool = False,
    stdout_json: bool = False,
    skip_market_context: bool = False,
    skip_factor_snapshot_without_ofi_cvd: bool = False,
) -> int:
    """Steps 1, 1.5, 2.0, 2.5, and 3B.1 (no-micro snapshot); before micro collector / full 3B / 4."""
    from laoma_signal_engine.core.exit_codes import EXIT_SUCCESS
    from laoma_signal_engine.market.futures_light_snapshot import run_fetch_light_snapshot_safe
    from laoma_signal_engine.micro.micro_target_router import run_micro_target_router_safe
    from laoma_signal_engine.scanner.abnormal_scanner import run_abnormal_scan_safe
    from laoma_signal_engine.universe.candidate_universe import run_build_universe_safe

    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    mode = fetch_mode if fetch_mode is not None else LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT
    lim = light_limit if light_limit > 0 else None

    if not skip_universe:
        log.info("pipeline: Step 1 build-universe")
        rc = run_build_universe_safe(force=force_universe, project_root=pr)
        if rc != EXIT_SUCCESS:
            log.error("pipeline: Step 1 failed exit=%s", rc)
            return rc

    if not skip_fetch_light_snapshot:
        log.info("pipeline: Step 1.5 fetch-futures-light-snapshot")
        rc = run_fetch_light_snapshot_safe(
            project_root=pr,
            limit=lim,
            symbols_filter=light_symbols,
            max_concurrency=max_concurrency,
            output_path=None,
            fetch_mode=mode,
            dry_run_plan=False,
            stdout_json=False,
        )
        if rc != EXIT_SUCCESS:
            log.error("pipeline: Step 1.5 failed exit=%s", rc)
            return rc

    if skip_scan_and_route:
        log.info("pipeline: skipping Step 2.0 scan and 2.5 route (using existing watch/strong/router JSON)")
        rc = _run_step_3b1_without_ofi_cvd(
            pr,
            stdout_json=stdout_json,
            skip_market_context=skip_market_context,
            skip_factor_snapshot_without_ofi_cvd_cli=skip_factor_snapshot_without_ofi_cvd,
        )
        if rc != EXIT_SUCCESS:
            return rc
        log.info("pipeline: pre-micro stages finished root=%s", pr)
        return EXIT_SUCCESS

    log.info("pipeline: Step 2.0 scan")
    rc = run_abnormal_scan_safe(
        project_root=pr,
        snapshot_path=None,
        universe_path=None,
        stdout_json=stdout_json,
        allow_stale_input=scan_allow_stale_input,
        strict_freshness_cli=False,
        max_snapshot_age_sec=None,
    )
    if rc != EXIT_SUCCESS:
        log.error("pipeline: Step 2.0 failed exit=%s", rc)
        return rc

    log.info("pipeline: Step 2.5 route-micro-targets")
    rc = run_micro_target_router_safe(
        project_root=pr,
        raw_path=None,
        watch_path=None,
        strong_path=None,
        output_path=None,
        stdout_json=stdout_json,
    )
    if rc != EXIT_SUCCESS:
        log.error("pipeline: Step 2.5 failed exit=%s", rc)
        return rc

    rc = _run_step_3b1_without_ofi_cvd(
        pr,
        stdout_json=stdout_json,
        skip_market_context=skip_market_context,
        skip_factor_snapshot_without_ofi_cvd_cli=skip_factor_snapshot_without_ofi_cvd,
    )
    if rc != EXIT_SUCCESS:
        return rc

    log.info("pipeline: pre-micro stages finished root=%s", pr)
    return EXIT_SUCCESS


def run_pipeline_post_micro_compact_safe(
    *,
    project_root: Path | None = None,
    stdout_json: bool = False,
    allow_watch_now: bool = False,
    disable_context_guards_for_now: bool = False,
    skip_market_context: bool = False,
    skip_final_decisions: bool = False,
) -> int:
    """Steps 3B (Factor Snapshot + STEP4.1 market context) + Step 4 + Step 5 after micro has written latest_micro_features.json."""
    from laoma_signal_engine.core.exit_codes import EXIT_SUCCESS
    from laoma_signal_engine.decision.direction_gate import run_apply_direction_gate_safe
    from laoma_signal_engine.decision.final_decisions import run_apply_final_decisions_safe
    from laoma_signal_engine.factors.factor_snapshot import run_assemble_factor_snapshot_safe

    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    micro_path = pr / "DATA" / "micro" / "latest_micro_features.json"
    if not micro_path.is_file():
        log.warning(
            "pipeline: Step 3 micro file missing at %s; 3B/4 continue with micro_missing if any.",
            micro_path,
        )

    if skip_market_context:
        log.info("pipeline: Step 3B assemble-factor-snapshot (skip_market_context=true, OI/Funding/Basis placeholders)")
    else:
        log.info(
            "pipeline: Step 3B assemble-factor-snapshot + STEP4.1 Binance REST (OI/Funding/Basis per symbol)",
        )
    rc = run_assemble_factor_snapshot_safe(
        project_root=pr,
        stdout_json=stdout_json,
        skip_market_context=skip_market_context,
    )
    if rc != EXIT_SUCCESS:
        log.error("pipeline: Step 3B failed exit=%s", rc)
        return rc

    if not skip_market_context:
        _log_factor_context_summary(pr)

    log.info("pipeline: Step 4 apply-direction-gate")
    rc = run_apply_direction_gate_safe(
        project_root=pr,
        stdout_json=stdout_json,
        allow_watch_now=allow_watch_now,
        require_context_guards_for_now=not disable_context_guards_for_now,
    )
    if rc != EXIT_SUCCESS:
        log.error("pipeline: Step 4 failed exit=%s", rc)
        return rc

    skip_cfg = _pipeline_skip_final_decisions_from_config()
    skip_final_effective = bool(skip_final_decisions) or skip_cfg
    if skip_final_effective:
        log.info(
            "pipeline: skipping Step 5.0 final decisions (cli=%s config_pipeline=%s)",
            bool(skip_final_decisions),
            skip_cfg,
        )
    else:
        log.info("pipeline: Step 5.0 apply-final-decisions")
        rc = run_apply_final_decisions_safe(project_root=pr, stdout_json=stdout_json)
        if rc != EXIT_SUCCESS:
            log.error("pipeline: Step 5.0 failed exit=%s", rc)
            return rc

    log.info("pipeline: post-micro (3B+4.1+4+5) finished ok root=%s", pr)
    return EXIT_SUCCESS


def _log_factor_context_summary(pr: Path) -> None:
    """Best-effort counts from latest_factor_snapshot.json after STEP4.1 assembly."""
    snap_path = (pr / "DATA" / "factors" / "latest_factor_snapshot.json").resolve()
    try:
        data = read_json_object(snap_path)
    except (OSError, ValueError) as exc:
        log.warning("pipeline: factor snapshot context summary skipped: %s", exc)
        return
    items = data.get("items")
    if not isinstance(items, list):
        return
    triple = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        oi = it.get("oi_15m")
        fc = it.get("funding_context")
        bc = it.get("basis_15m")
        if (
            isinstance(oi, dict)
            and isinstance(fc, dict)
            and isinstance(bc, dict)
            and oi.get("ready") is True
            and fc.get("ready") is True
            and bc.get("ready") is True
        ):
            triple += 1
    log.info(
        "pipeline: Step 4.1 context ready counts symbols=%s oi_funding_basis_all_ready=%s",
        len(items),
        triple,
    )


def run_decision_pipeline_safe(
    *,
    project_root: Path | None = None,
    force_universe: bool = False,
    skip_universe: bool = False,
    skip_fetch_light_snapshot: bool = False,
    light_limit: int = 0,
    light_symbols: list[str] | None = None,
    fetch_mode: str | None = None,
    max_concurrency: int | None = None,
    scan_allow_stale_input: bool = False,
    stdout_json: bool = False,
    allow_watch_now: bool = False,
    disable_context_guards_for_now: bool = False,
    skip_market_context: bool = False,
    skip_final_decisions: bool = False,
    skip_factor_snapshot_without_ofi_cvd: bool = False,
    wait_micro_ready: bool = False,
    micro_max_wait_sec: float | None = None,
    micro_min_ready_count: int | None = None,
) -> int:
    """Chain Step 1, 1.5, 2.0, 2.5, 3B.1, optional persistent micro wait, 3B+4.1, 4, Step 5."""
    from laoma_signal_engine.core.exit_codes import EXIT_SUCCESS, EXIT_WAIT_UNTIL_READY_TIMEOUT

    rc = run_pipeline_pre_micro_safe(
        project_root=project_root,
        force_universe=force_universe,
        skip_universe=skip_universe,
        skip_fetch_light_snapshot=skip_fetch_light_snapshot,
        skip_scan_and_route=False,
        light_limit=light_limit,
        light_symbols=light_symbols,
        fetch_mode=fetch_mode,
        max_concurrency=max_concurrency,
        scan_allow_stale_input=scan_allow_stale_input,
        stdout_json=stdout_json,
        skip_market_context=skip_market_context,
        skip_factor_snapshot_without_ofi_cvd=skip_factor_snapshot_without_ofi_cvd,
    )
    if rc != EXIT_SUCCESS:
        return rc

    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    if wait_micro_ready:
        wait_cfg = load_wait_until_ready_config(pr)
        if micro_max_wait_sec is not None:
            wait_cfg = replace(wait_cfg, max_wait_sec=float(micro_max_wait_sec))
        if micro_min_ready_count is not None:
            wait_cfg = replace(wait_cfg, min_ready_count=int(micro_min_ready_count))
        cfg = EngineConfig.load(pr)
        latest_path = (pr / "DATA" / "micro" / "latest_micro_features.json").resolve()
        hb_path = (pr / "DATA" / "micro" / "micro_collector_heartbeat.json").resolve()
        log.info(
            "pipeline: Step 3 persistent micro wait max_wait_sec=%.0f min_ready_count=%s targets=%s",
            wait_cfg.max_wait_sec,
            wait_cfg.min_ready_count,
            cfg.micro_targets_path,
        )
        try:
            wait_rc = run_wait_until_ready_orchestration(
                project_root=pr,
                cfg=wait_cfg,
                latest_path=latest_path,
                heartbeat_path=hb_path,
                targets_path=cfg.micro_targets_path,
                transport="real",
                start_subprocess=False,
            )
        except Exception:
            log.exception("pipeline: Step 3 persistent micro wait failed; continuing to 3B with current micro state")
        else:
            if wait_rc == EXIT_SUCCESS:
                log.info("pipeline: Step 3 persistent micro ready")
            elif wait_rc == EXIT_WAIT_UNTIL_READY_TIMEOUT:
                log.warning(
                    "pipeline: Step 3 persistent micro wait timed out after %.0fs; continuing with freshness gates",
                    wait_cfg.max_wait_sec,
                )
            else:
                log.warning(
                    "pipeline: Step 3 persistent micro wait exit=%s; continuing with freshness gates",
                    wait_rc,
                )

    return run_pipeline_post_micro_compact_safe(
        project_root=pr,
        stdout_json=stdout_json,
        allow_watch_now=allow_watch_now,
        disable_context_guards_for_now=disable_context_guards_for_now,
        skip_market_context=skip_market_context,
        skip_final_decisions=skip_final_decisions,
    )


def run_full_pipeline_with_micro_timed_safe(
    *,
    project_root: Path | None = None,
    force_universe: bool = False,
    skip_universe: bool = False,
    skip_fetch_light_snapshot: bool = False,
    light_limit: int = 0,
    light_symbols: list[str] | None = None,
    fetch_mode: str | None = None,
    max_concurrency: int | None = None,
    scan_allow_stale_input: bool = False,
    stdout_json: bool = False,
    allow_watch_now: bool = False,
    disable_context_guards_for_now: bool = False,
    micro_run_sec: float = 900.0,
    micro_transport: TransportKind = "real",
    micro_permissive_quality_smoke: bool = False,
    skip_scan_and_route: bool = False,
    micro_wait_until_ready: bool = False,
    micro_max_wait_sec: float | None = None,
    micro_min_ready_count: int | None = None,
    skip_market_context: bool = False,
    skip_final_decisions: bool = False,
    skip_factor_snapshot_without_ofi_cvd: bool = False,
) -> int:
    """Steps 1-2.5 (+3B.1), then micro collector (or wait-until-ready), then 3B+4.1+4+5.

    If micro_wait_until_ready, Step 3 runs subprocess daemon + STEP3.8D poll until ready or timeout;
    micro_run_sec is not used to stop the daemon (target_stale_sec follows wait_until_ready.max_wait_sec).

    STEP4.1: unless skip_market_context, assemble-factor-snapshot pulls Binance OI/Funding/Basis per symbol.
    STEP5.0: unless skip_final_decisions or default.yaml pipeline.skip_final_decisions, writes latest_decisions.json.
    """
    from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
    from laoma_signal_engine.micro.daemon.app import run_daemon
    from laoma_signal_engine.micro.daemon.config import DaemonConfig
    from laoma_signal_engine.micro.micro_target_models import MicroTargetsDocument

    rc = run_pipeline_pre_micro_safe(
        project_root=project_root,
        force_universe=force_universe,
        skip_universe=skip_universe,
        skip_fetch_light_snapshot=skip_fetch_light_snapshot,
        skip_scan_and_route=skip_scan_and_route,
        light_limit=light_limit,
        light_symbols=light_symbols,
        fetch_mode=fetch_mode,
        max_concurrency=max_concurrency,
        scan_allow_stale_input=scan_allow_stale_input,
        stdout_json=stdout_json,
        skip_market_context=skip_market_context,
        skip_factor_snapshot_without_ofi_cvd=skip_factor_snapshot_without_ofi_cvd,
    )
    if rc != EXIT_SUCCESS:
        return rc

    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    cfg = EngineConfig.load(pr)
    mt_path = cfg.micro_targets_path.resolve()
    mt_doc = MicroTargetsDocument.model_validate(read_json_object(mt_path))
    n_targets = len(mt_doc.tier1_warm_watch) + len(mt_doc.tier2_active_strong)
    if n_targets == 0:
        log.error(
            "pipeline: micro_targets.json has 0 symbols at %s; cannot run Step 3 collector. "
            "Ensure watch/strong outputs (avoid crippling --light-limit) or seed targets.",
            mt_path,
        )
        return EXIT_CONFIG

    latest_path = (pr / "DATA" / "micro" / "latest_micro_features.json").resolve()
    latest_state_path = (pr / "DATA" / "micro" / "latest_micro_state.json").resolve()
    hb_path = (pr / "DATA" / "micro" / "micro_collector_heartbeat.json").resolve()
    quality_config = _permissive_micro_quality_config() if micro_permissive_quality_smoke else None
    run_sec_f = max(1.0, float(micro_run_sec))
    target_stale_sec = max(420, int(run_sec_f) + 300)
    daemon_cfg = DaemonConfig(
        targets_path=mt_path,
        latest_features_path=latest_path,
        heartbeat_path=hb_path,
        latest_state_path=latest_state_path,
        transport=micro_transport,
        target_stale_sec=target_stale_sec,
    )

    if micro_wait_until_ready:
        wait_cfg = load_wait_until_ready_config(pr)
        if micro_max_wait_sec is not None:
            wait_cfg = replace(wait_cfg, max_wait_sec=float(micro_max_wait_sec))
        if micro_min_ready_count is not None:
            wait_cfg = replace(wait_cfg, min_ready_count=int(micro_min_ready_count))
        log.info(
            "pipeline: Step 3 micro wait-until-ready mode=%s max_wait_sec=%.0f min_ready_count=%s "
            "targets=%s transport=%s (then 3B+4.1+4 if ready_met)",
            wait_cfg.mode,
            wait_cfg.max_wait_sec,
            wait_cfg.min_ready_count,
            n_targets,
            micro_transport,
        )
        try:
            orc_rc = run_wait_until_ready_orchestration(
                project_root=pr,
                cfg=wait_cfg,
                latest_path=latest_path,
                heartbeat_path=hb_path,
                targets_path=mt_path,
                transport=micro_transport,
                start_subprocess=True,
                target_stale_sec_override=None,
                output_interval_sec=int(daemon_cfg.output_interval_sec),
                event_drain_interval_sec=float(daemon_cfg.event_drain_interval_sec),
                ring_buffer_sec=int(daemon_cfg.ring_buffer_seconds),
                permissive_quality_smoke=micro_permissive_quality_smoke,
            )
        except Exception:
            log.exception("pipeline: Step 3 micro wait-until-ready orchestration failed")
            return EXIT_INTERNAL
        if orc_rc != EXIT_SUCCESS:
            log.error("pipeline: Step 3 wait-until-ready exit=%s (see DATA/micro/run_reports/)", orc_rc)
            return int(orc_rc)
    else:
        log.info(
            "pipeline: daemon target_stale_sec=%s (must exceed micro_run_sec=%s to avoid empty latest_micro_features)",
            target_stale_sec,
            micro_run_sec,
        )
        log.info(
            "pipeline: Step 3 micro collector %.0fs targets=%s transport=%s (then 3B+4.1+4+5)",
            micro_run_sec,
            n_targets,
            micro_transport,
        )

        async def _micro() -> None:
            await run_daemon(
                daemon_cfg,
                now_fn=utc_now,
                fixture_events_path=None,
                quality_config=quality_config,
                once=False,
                short_run_sec=micro_run_sec,
            )

        try:
            asyncio.run(_micro())
        except Exception:
            log.exception("pipeline: Step 3 micro collector failed")
            return EXIT_INTERNAL

    return run_pipeline_post_micro_compact_safe(
        project_root=pr,
        stdout_json=stdout_json,
        allow_watch_now=allow_watch_now,
        disable_context_guards_for_now=disable_context_guards_for_now,
        skip_market_context=skip_market_context,
        skip_final_decisions=skip_final_decisions,
    )
