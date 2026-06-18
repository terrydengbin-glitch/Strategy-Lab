"""Load YAML config relative to the package or project root."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


def package_root() -> Path:
    """Directory containing the `laoma_signal_engine` package."""
    return Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class EngineConfig:
    data_root: Path
    schema_version: str
    source: str
    candidate_universe_path: Path
    manual_watchlist_path: Path
    futures_light_snapshot_path: Path
    micro_targets_path: Path
    latest_raw_candidates_path: Path
    latest_watch_signals_path: Path
    latest_strong_candidates_path: Path
    universe_ttl_seconds: int
    universe_top_tag_rank: int
    project_root: Path
    strict_freshness: bool
    universe_max_age_sec: int
    light_snapshot_max_age_sec: int
    light_snapshot_hard_stale_sec: int
    step2_signal_max_age_sec: int
    micro_features_max_age_sec: int
    micro_target_max_age_sec: int
    factor_snapshot_max_age_sec: int
    direction_decision_max_age_sec: int
    final_light_snapshot_max_age_sec: int
    mr_warm_watch_limit: int
    mr_active_strong_limit: int
    mr_max_active_micro_symbols: int
    mr_include_raw_in_warm_pool: bool
    mr_manual_watchlist_priority_bonus: int
    mr_min_collect_seconds: int
    mr_ttl_seconds_tier1: int
    mr_ttl_seconds_tier2: int
    mr_target_ready_tf: str
    mr_tier1_subscribe: tuple[str, ...]
    mr_tier2_subscribe: tuple[str, ...]
    mr_priority_mode: str
    mr_allow_trade_rank_priority: bool
    mr_exclude_market_entry_avoid_from_micro: bool
    step2_market_entry_preferred_min_score: int
    step2_market_entry_allowed_min_score: int
    step2_trade_candidate_observe_min_score: int
    step2_promote_raw_market_entry_allowed: bool
    step2_raw_promote_min_market_entry_score: int
    step2_raw_promote_min_scan_score: int
    step2_raw_promote_limit: int
    strategy_pipeline_mode: str
    strategy_pipeline_interval_sec: int
    strategy_pipeline_overlap_policy: str
    strategy_pipeline_lock_path: Path
    strategy_pipeline_report_path: Path
    strategy_pipeline_run_lines: tuple[str, ...]
    strategy_pipeline_fetch_mode: str
    strategy_pipeline_max_concurrency: int
    strategy_pipeline_force_universe: bool
    strategy_pipeline_light_limit: int
    strategy_pipeline_scan_allow_stale_input: bool
    strategy_pipeline_auto_refresh_before_trade_plan: bool
    strategy_pipeline_require_refresh_anchor_current_factor: bool
    strategy_pipeline_aggregate_final_decisions: bool
    strategy_pipeline_run_abc_audit: bool
    strategy_pipeline_run_json_stage_audit: bool
    strategy_pipeline_allow_llm_stale: bool
    strategy_pipeline_require_daemon: bool
    strategy_pipeline_wait_fast_sec: int
    strategy_pipeline_wait_full_sec: int
    strategy_pipeline_max_wait_sec: int
    strategy_pipeline_min_full_ready_count: int
    strategy_pipeline_full_wait_policy: str
    strategy_pipeline_max_active_symbols: int
    strategy_pipeline_timeout_policy: str
    strategy_pipeline_require_fresh_step2_before_micro: bool
    strategy_pipeline_auto_refresh_step2_before_micro: bool
    strategy_pipeline_step2_max_age_sec_for_micro: int
    strategy_pipeline_micro_empty_target_daemon_state: str
    strategy_pipeline_micro_health_preflight_enabled: bool
    strategy_pipeline_micro_unhealthy_policy: str
    strategy_pipeline_micro_preflight_heartbeat_stale_sec: int
    strategy_pipeline_micro_health_grace_recheck_enabled: bool
    strategy_pipeline_micro_health_grace_wait_sec: int
    strategy_pipeline_micro_health_grace_max_attempts: int
    strategy_pipeline_micro_health_grace_accept_fresh_heartbeat_sec: int
    strategy_pipeline_micro_inline_recovery_enabled: bool
    strategy_pipeline_micro_inline_recovery_max_attempts: int
    strategy_pipeline_micro_inline_recovery_startup_wait_sec: int
    strategy_pipeline_micro_inline_recovery_heartbeat_fresh_sec: int
    strategy_pipeline_micro_inline_recovery_state_fresh_sec: int
    strategy_pipeline_micro_inline_recovery_features_fresh_sec: int
    strategy_pipeline_micro_sticky_pool_enabled: bool
    strategy_pipeline_micro_sticky_ttl_sec: int
    strategy_pipeline_micro_sticky_min_cycles: int
    strategy_pipeline_micro_sticky_max_cycles: int
    strategy_pipeline_micro_sticky_include_ready_symbols: bool
    micro_daemon_cli_transport: str
    micro_daemon_cli_start_mode: str
    micro_daemon_cli_pid_path: Path
    micro_daemon_cli_log_path: Path
    micro_daemon_cli_log_rotation_enabled: bool
    micro_daemon_cli_log_rotation_max_bytes: int
    micro_daemon_cli_log_rotation_backup_count: int
    micro_daemon_cli_heartbeat_path: Path
    micro_daemon_cli_state_path: Path
    micro_daemon_cli_features_path: Path

    @staticmethod
    def load(project_root: Path | None = None) -> EngineConfig:
        root = project_root.resolve() if project_root else Path.cwd().resolve()
        cfg_path = package_root() / "config" / "default.yaml"
        raw_text = cfg_path.read_text(encoding="utf-8")
        doc: dict[str, Any] = yaml.safe_load(raw_text) or {}

        data_root_name = str(doc.get("data_root", "DATA"))
        schema_version = str(doc.get("schema_version", "1.6"))
        source = str(doc.get("source", "binance"))
        paths = doc.get("paths") or {}
        cu = str(paths.get("candidate_universe", "DATA/universe/CANDIDATE_UNIVERSE.json"))
        mw = str(paths.get("manual_watchlist", "DATA/universe/manual_watchlist.json"))
        fl = str(
            paths.get("futures_light_snapshot", "DATA/market/futures_light_snapshot.json")
        )
        mt = str(paths.get("micro_targets", "DATA/micro/micro_targets.json"))
        lr = str(paths.get("latest_raw_candidates", "DATA/raw_signals/latest_raw_candidates.json"))
        lw = str(paths.get("latest_watch_signals", "DATA/raw_signals/latest_watch_signals.json"))
        ls = str(paths.get("latest_strong_candidates", "DATA/raw_signals/latest_strong_candidates.json"))

        univ = doc.get("universe") or {}
        ttl = int(univ.get("ttl_seconds", 86400))
        top_rank = int(univ.get("top_tag_rank", 10))

        fr = doc.get("freshness") or {}
        strict_freshness = bool(fr.get("strict_freshness", True))
        universe_max_age_sec = int(fr.get("universe_max_age_sec", 86400))
        light_snapshot_max_age_sec = int(fr.get("light_snapshot_max_age_sec", 180))
        light_snapshot_hard_stale_sec = int(fr.get("light_snapshot_hard_stale_sec", 300))
        step2_signal_max_age_sec = int(fr.get("step2_signal_max_age_sec", 300))
        micro_features_max_age_sec = int(fr.get("micro_features_max_age_sec", 30))
        micro_target_max_age_sec = int(fr.get("micro_target_max_age_sec", 300))
        factor_snapshot_max_age_sec = int(fr.get("factor_snapshot_max_age_sec", 180))
        direction_decision_max_age_sec = int(fr.get("direction_decision_max_age_sec", 180))
        final_light_snapshot_max_age_sec = int(fr.get("final_light_snapshot_max_age_sec", 300))

        mr = doc.get("micro_router") or {}
        mr_warm = int(mr.get("warm_watch_limit", 30))
        mr_strong_lim = int(mr.get("active_strong_limit", 10))
        mr_max_active = int(mr.get("max_active_micro_symbols", mr_warm + mr_strong_lim))
        mr_raw_pool = bool(mr.get("include_raw_in_warm_pool", False))
        mr_bonus = int(mr.get("manual_watchlist_priority_bonus", 50))
        mr_min_col = int(mr.get("min_collect_seconds", 900))
        mr_ttl1 = int(mr.get("ttl_seconds_tier1", 1800))
        mr_ttl2 = int(mr.get("ttl_seconds_tier2", 1800))
        mr_tf = str(mr.get("target_ready_tf", "15m"))
        t1 = mr.get("tier1_subscribe") or ["aggTrade", "bookTicker"]
        t2 = mr.get("tier2_subscribe") or ["aggTrade", "bookTicker", "partialDepth5"]
        mr_t1_sub = tuple(str(x) for x in t1)
        mr_t2_sub = tuple(str(x) for x in t2)
        mr_priority_mode = str(mr.get("priority_mode", "scan_score"))
        mr_allow_trade_rank_priority = bool(mr.get("allow_trade_rank_priority", False))
        mr_exclude_market_entry_avoid_from_micro = bool(mr.get("exclude_market_entry_avoid_from_micro", False))

        s2 = doc.get("step2") or {}
        step2_market_entry_preferred_min_score = int(s2.get("market_entry_preferred_min_score", 75))
        step2_market_entry_allowed_min_score = int(s2.get("market_entry_allowed_min_score", 55))
        step2_trade_candidate_observe_min_score = int(s2.get("trade_candidate_observe_min_score", 60))
        step2_promote_raw_market_entry_allowed = bool(s2.get("promote_raw_market_entry_allowed", False))
        step2_raw_promote_min_market_entry_score = int(s2.get("raw_promote_min_market_entry_score", 75))
        step2_raw_promote_min_scan_score = int(s2.get("raw_promote_min_scan_score", 35))
        step2_raw_promote_limit = int(s2.get("raw_promote_limit", 3))

        sp = doc.get("strategy_pipeline") or {}
        sp_micro = sp.get("micro") or {}
        strategy_pipeline_mode = str(sp.get("mode", "once"))
        strategy_pipeline_interval_sec = int(sp.get("interval_sec", 300))
        strategy_pipeline_overlap_policy = str(sp.get("overlap_policy", "skip"))
        strategy_pipeline_lock = str(sp.get("lock_path", "DATA/runtime/strategy_pipeline.lock"))
        strategy_pipeline_report = str(
            sp.get("report_path", "DATA/reports/latest_strategy_pipeline_report.json")
        )
        sp_lines = sp.get("run_lines") or ["without_micro", "micro_fast", "micro_full"]
        strategy_pipeline_run_lines = tuple(str(x) for x in sp_lines)
        strategy_pipeline_fetch_mode = str(sp.get("fetch_mode", "async"))
        strategy_pipeline_max_concurrency = int(sp.get("max_concurrency", 6))
        strategy_pipeline_force_universe = bool(sp.get("force_universe", False))
        strategy_pipeline_light_limit = int(sp.get("light_limit", 0))
        strategy_pipeline_scan_allow_stale_input = bool(sp.get("scan_allow_stale_input", False))
        strategy_pipeline_auto_refresh_before_trade_plan = bool(
            sp.get("auto_refresh_before_trade_plan", True)
        )
        strategy_pipeline_require_refresh_anchor_current_factor = bool(
            sp.get("require_refresh_anchor_current_factor", True)
        )
        strategy_pipeline_aggregate_final_decisions = bool(
            sp.get("aggregate_final_decisions", True)
        )
        strategy_pipeline_run_abc_audit = bool(sp.get("run_abc_audit", True))
        strategy_pipeline_run_json_stage_audit = bool(sp.get("run_json_stage_audit", True))
        strategy_pipeline_allow_llm_stale = bool(sp.get("allow_llm_stale", True))
        strategy_pipeline_require_daemon = bool(sp_micro.get("require_daemon", True))
        strategy_pipeline_wait_fast_sec = int(sp_micro.get("wait_fast_sec", 180))
        strategy_pipeline_wait_full_sec = int(sp_micro.get("wait_full_sec", 1200))
        strategy_pipeline_max_wait_sec = int(sp_micro.get("max_wait_sec", 1200))
        strategy_pipeline_min_full_ready_count = int(sp_micro.get("min_full_ready_count", 1))
        strategy_pipeline_full_wait_policy = str(sp_micro.get("full_wait_policy", "strict_until_ready"))
        strategy_pipeline_max_active_symbols = int(sp_micro.get("max_active_symbols", 10))
        strategy_pipeline_timeout_policy = str(sp_micro.get("timeout_policy", "return_reason"))
        strategy_pipeline_require_fresh_step2_before_micro = bool(
            sp_micro.get("require_fresh_step2_before_micro", True)
        )
        strategy_pipeline_auto_refresh_step2_before_micro = bool(
            sp_micro.get("auto_refresh_step2_before_micro", True)
        )
        strategy_pipeline_step2_max_age_sec_for_micro = int(
            sp_micro.get("step2_max_age_sec_for_micro", step2_signal_max_age_sec)
        )
        strategy_pipeline_micro_empty_target_daemon_state = str(
            sp_micro.get("empty_target_daemon_state", "healthy_idle")
        )
        strategy_pipeline_micro_health_preflight_enabled = bool(
            sp_micro.get("health_preflight_enabled", True)
        )
        strategy_pipeline_micro_unhealthy_policy = str(sp_micro.get("unhealthy_policy", "block_line"))
        strategy_pipeline_micro_preflight_heartbeat_stale_sec = int(
            sp_micro.get("preflight_heartbeat_stale_sec", 180)
        )
        strategy_pipeline_micro_health_grace_recheck_enabled = bool(
            sp_micro.get("health_grace_recheck_enabled", True)
        )
        strategy_pipeline_micro_health_grace_wait_sec = int(sp_micro.get("health_grace_wait_sec", 15))
        strategy_pipeline_micro_health_grace_max_attempts = int(sp_micro.get("health_grace_max_attempts", 2))
        strategy_pipeline_micro_health_grace_accept_fresh_heartbeat_sec = int(
            sp_micro.get("health_grace_accept_fresh_heartbeat_sec", 60)
        )
        inline_recovery = sp_micro.get("inline_recovery") or {}
        strategy_pipeline_micro_inline_recovery_enabled = bool(
            inline_recovery.get("enabled", True)
        )
        strategy_pipeline_micro_inline_recovery_max_attempts = int(
            inline_recovery.get("max_attempts", 1)
        )
        strategy_pipeline_micro_inline_recovery_startup_wait_sec = int(
            inline_recovery.get("startup_wait_sec", 45)
        )
        strategy_pipeline_micro_inline_recovery_heartbeat_fresh_sec = int(
            inline_recovery.get("heartbeat_fresh_sec", 20)
        )
        strategy_pipeline_micro_inline_recovery_state_fresh_sec = int(
            inline_recovery.get("state_fresh_sec", 30)
        )
        strategy_pipeline_micro_inline_recovery_features_fresh_sec = int(
            inline_recovery.get("features_fresh_sec", 30)
        )
        strategy_pipeline_micro_sticky_pool_enabled = bool(sp_micro.get("sticky_pool_enabled", True))
        strategy_pipeline_micro_sticky_ttl_sec = int(sp_micro.get("sticky_ttl_sec", 900))
        strategy_pipeline_micro_sticky_min_cycles = int(sp_micro.get("sticky_min_cycles", 2))
        strategy_pipeline_micro_sticky_max_cycles = int(sp_micro.get("sticky_max_cycles", 4))
        strategy_pipeline_micro_sticky_include_ready_symbols = bool(
            sp_micro.get("sticky_include_ready_symbols", True)
        )

        mcli = doc.get("micro_daemon_cli") or {}
        micro_daemon_cli_transport = str(mcli.get("transport", "real"))
        micro_daemon_cli_start_mode = str(mcli.get("start_mode", "background"))
        micro_daemon_cli_pid = str(mcli.get("pid_path", "DATA/runtime/micro_daemon.pid"))
        micro_daemon_cli_log = str(mcli.get("log_path", "DATA/logs/micro_daemon.log"))
        mcli_rotation = mcli.get("log_rotation") or {}
        micro_daemon_cli_log_rotation_enabled = bool(mcli_rotation.get("enabled", True))
        micro_daemon_cli_log_rotation_max_bytes = int(mcli_rotation.get("max_bytes", 104857600))
        micro_daemon_cli_log_rotation_backup_count = int(mcli_rotation.get("backup_count", 5))
        micro_daemon_cli_heartbeat = str(
            mcli.get("heartbeat_path", "DATA/micro/micro_collector_heartbeat.json")
        )
        micro_daemon_cli_state = str(mcli.get("state_path", "DATA/micro/latest_micro_state.json"))
        micro_daemon_cli_features = str(
            mcli.get("features_path", "DATA/micro/latest_micro_features.json")
        )

        data_root = (root / data_root_name).resolve()

        def _rp(p: str) -> Path:
            return (root / p).resolve() if not Path(p).is_absolute() else Path(p).resolve()

        candidate = _rp(cu)
        manual = _rp(mw)
        fut_light = _rp(fl)
        micro_targets = _rp(mt)
        raw_candidates = _rp(lr)
        watch_sig = _rp(lw)
        strong_sig = _rp(ls)

        return EngineConfig(
            data_root=data_root,
            schema_version=schema_version,
            source=source,
            candidate_universe_path=candidate,
            manual_watchlist_path=manual,
            futures_light_snapshot_path=fut_light,
            micro_targets_path=micro_targets,
            latest_raw_candidates_path=raw_candidates,
            latest_watch_signals_path=watch_sig,
            latest_strong_candidates_path=strong_sig,
            universe_ttl_seconds=ttl,
            universe_top_tag_rank=top_rank,
            project_root=root,
            strict_freshness=strict_freshness,
            universe_max_age_sec=universe_max_age_sec,
            light_snapshot_max_age_sec=light_snapshot_max_age_sec,
            light_snapshot_hard_stale_sec=light_snapshot_hard_stale_sec,
            step2_signal_max_age_sec=step2_signal_max_age_sec,
            micro_features_max_age_sec=micro_features_max_age_sec,
            micro_target_max_age_sec=micro_target_max_age_sec,
            factor_snapshot_max_age_sec=factor_snapshot_max_age_sec,
            direction_decision_max_age_sec=direction_decision_max_age_sec,
            final_light_snapshot_max_age_sec=final_light_snapshot_max_age_sec,
            mr_warm_watch_limit=mr_warm,
            mr_active_strong_limit=mr_strong_lim,
            mr_max_active_micro_symbols=mr_max_active,
            mr_include_raw_in_warm_pool=mr_raw_pool,
            mr_manual_watchlist_priority_bonus=mr_bonus,
            mr_min_collect_seconds=mr_min_col,
            mr_ttl_seconds_tier1=mr_ttl1,
            mr_ttl_seconds_tier2=mr_ttl2,
            mr_target_ready_tf=mr_tf,
            mr_tier1_subscribe=mr_t1_sub,
            mr_tier2_subscribe=mr_t2_sub,
            mr_priority_mode=mr_priority_mode,
            mr_allow_trade_rank_priority=mr_allow_trade_rank_priority,
            mr_exclude_market_entry_avoid_from_micro=mr_exclude_market_entry_avoid_from_micro,
            step2_market_entry_preferred_min_score=step2_market_entry_preferred_min_score,
            step2_market_entry_allowed_min_score=step2_market_entry_allowed_min_score,
            step2_trade_candidate_observe_min_score=step2_trade_candidate_observe_min_score,
            step2_promote_raw_market_entry_allowed=step2_promote_raw_market_entry_allowed,
            step2_raw_promote_min_market_entry_score=step2_raw_promote_min_market_entry_score,
            step2_raw_promote_min_scan_score=step2_raw_promote_min_scan_score,
            step2_raw_promote_limit=step2_raw_promote_limit,
            strategy_pipeline_mode=strategy_pipeline_mode,
            strategy_pipeline_interval_sec=strategy_pipeline_interval_sec,
            strategy_pipeline_overlap_policy=strategy_pipeline_overlap_policy,
            strategy_pipeline_lock_path=_rp(strategy_pipeline_lock),
            strategy_pipeline_report_path=_rp(strategy_pipeline_report),
            strategy_pipeline_run_lines=strategy_pipeline_run_lines,
            strategy_pipeline_fetch_mode=strategy_pipeline_fetch_mode,
            strategy_pipeline_max_concurrency=strategy_pipeline_max_concurrency,
            strategy_pipeline_force_universe=strategy_pipeline_force_universe,
            strategy_pipeline_light_limit=strategy_pipeline_light_limit,
            strategy_pipeline_scan_allow_stale_input=strategy_pipeline_scan_allow_stale_input,
            strategy_pipeline_auto_refresh_before_trade_plan=(
                strategy_pipeline_auto_refresh_before_trade_plan
            ),
            strategy_pipeline_require_refresh_anchor_current_factor=(
                strategy_pipeline_require_refresh_anchor_current_factor
            ),
            strategy_pipeline_aggregate_final_decisions=strategy_pipeline_aggregate_final_decisions,
            strategy_pipeline_run_abc_audit=strategy_pipeline_run_abc_audit,
            strategy_pipeline_run_json_stage_audit=strategy_pipeline_run_json_stage_audit,
            strategy_pipeline_allow_llm_stale=strategy_pipeline_allow_llm_stale,
            strategy_pipeline_require_daemon=strategy_pipeline_require_daemon,
            strategy_pipeline_wait_fast_sec=strategy_pipeline_wait_fast_sec,
            strategy_pipeline_wait_full_sec=strategy_pipeline_wait_full_sec,
            strategy_pipeline_max_wait_sec=strategy_pipeline_max_wait_sec,
            strategy_pipeline_min_full_ready_count=strategy_pipeline_min_full_ready_count,
            strategy_pipeline_full_wait_policy=strategy_pipeline_full_wait_policy,
            strategy_pipeline_max_active_symbols=strategy_pipeline_max_active_symbols,
            strategy_pipeline_timeout_policy=strategy_pipeline_timeout_policy,
            strategy_pipeline_require_fresh_step2_before_micro=(
                strategy_pipeline_require_fresh_step2_before_micro
            ),
            strategy_pipeline_auto_refresh_step2_before_micro=(
                strategy_pipeline_auto_refresh_step2_before_micro
            ),
            strategy_pipeline_step2_max_age_sec_for_micro=strategy_pipeline_step2_max_age_sec_for_micro,
            strategy_pipeline_micro_empty_target_daemon_state=(
                strategy_pipeline_micro_empty_target_daemon_state
            ),
            strategy_pipeline_micro_health_preflight_enabled=(
                strategy_pipeline_micro_health_preflight_enabled
            ),
            strategy_pipeline_micro_unhealthy_policy=strategy_pipeline_micro_unhealthy_policy,
            strategy_pipeline_micro_preflight_heartbeat_stale_sec=(
                strategy_pipeline_micro_preflight_heartbeat_stale_sec
            ),
            strategy_pipeline_micro_health_grace_recheck_enabled=(
                strategy_pipeline_micro_health_grace_recheck_enabled
            ),
            strategy_pipeline_micro_health_grace_wait_sec=(
                strategy_pipeline_micro_health_grace_wait_sec
            ),
            strategy_pipeline_micro_health_grace_max_attempts=(
                strategy_pipeline_micro_health_grace_max_attempts
            ),
            strategy_pipeline_micro_health_grace_accept_fresh_heartbeat_sec=(
                strategy_pipeline_micro_health_grace_accept_fresh_heartbeat_sec
            ),
            strategy_pipeline_micro_inline_recovery_enabled=(
                strategy_pipeline_micro_inline_recovery_enabled
            ),
            strategy_pipeline_micro_inline_recovery_max_attempts=(
                strategy_pipeline_micro_inline_recovery_max_attempts
            ),
            strategy_pipeline_micro_inline_recovery_startup_wait_sec=(
                strategy_pipeline_micro_inline_recovery_startup_wait_sec
            ),
            strategy_pipeline_micro_inline_recovery_heartbeat_fresh_sec=(
                strategy_pipeline_micro_inline_recovery_heartbeat_fresh_sec
            ),
            strategy_pipeline_micro_inline_recovery_state_fresh_sec=(
                strategy_pipeline_micro_inline_recovery_state_fresh_sec
            ),
            strategy_pipeline_micro_inline_recovery_features_fresh_sec=(
                strategy_pipeline_micro_inline_recovery_features_fresh_sec
            ),
            strategy_pipeline_micro_sticky_pool_enabled=(
                strategy_pipeline_micro_sticky_pool_enabled
            ),
            strategy_pipeline_micro_sticky_ttl_sec=strategy_pipeline_micro_sticky_ttl_sec,
            strategy_pipeline_micro_sticky_min_cycles=strategy_pipeline_micro_sticky_min_cycles,
            strategy_pipeline_micro_sticky_max_cycles=strategy_pipeline_micro_sticky_max_cycles,
            strategy_pipeline_micro_sticky_include_ready_symbols=(
                strategy_pipeline_micro_sticky_include_ready_symbols
            ),
            micro_daemon_cli_transport=micro_daemon_cli_transport,
            micro_daemon_cli_start_mode=micro_daemon_cli_start_mode,
            micro_daemon_cli_pid_path=_rp(micro_daemon_cli_pid),
            micro_daemon_cli_log_path=_rp(micro_daemon_cli_log),
            micro_daemon_cli_log_rotation_enabled=micro_daemon_cli_log_rotation_enabled,
            micro_daemon_cli_log_rotation_max_bytes=micro_daemon_cli_log_rotation_max_bytes,
            micro_daemon_cli_log_rotation_backup_count=micro_daemon_cli_log_rotation_backup_count,
            micro_daemon_cli_heartbeat_path=_rp(micro_daemon_cli_heartbeat),
            micro_daemon_cli_state_path=_rp(micro_daemon_cli_state),
            micro_daemon_cli_features_path=_rp(micro_daemon_cli_features),
        )
