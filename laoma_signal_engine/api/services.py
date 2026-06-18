from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

from laoma_signal_engine.api.contracts import PipelineRunRequest
from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.core.config_loader import package_root
from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import parse_iso_z, to_iso_z, utc_now
from laoma_signal_engine.paper.candles import fetch_binance_1m_candles
from laoma_signal_engine.paper.archive import archive_reset_strategy, get_experiment, list_experiments
from laoma_signal_engine.paper.daemon import inspect_tick_lock
from laoma_signal_engine.paper.daemon import read_status as paper_read_status
from laoma_signal_engine.paper.daemon import run_once as paper_run_once
from laoma_signal_engine.paper.models import STRATEGY_LINES, PaperConfig
from laoma_signal_engine.paper.utils import read_json
from laoma_signal_engine.pipeline_funnel import (
    cross_strategy_funnel_history_payload as _cross_strategy_funnel_history_payload,
    latest_cross_strategy_funnel_payload as _latest_cross_strategy_funnel_payload,
)
import laoma_signal_engine.runtime_health as runtime_health
from laoma_signal_engine.trade_quality.archive_backfill import (
    archive_backfill_payload,
    archive_ingest_summary,
    enrich_sample_sources,
    ingest_ledger_rows,
)
from laoma_signal_engine.trade_quality.engine import ensure_trade_quality_tables
from laoma_signal_engine.trade_quality.recommendation_rules import (
    recommendation_rules_payload,
    rebuild_recommendation_rules,
)
from laoma_signal_engine.trade_quality.recommendation_validation import recommendation_validation_payload
from laoma_signal_engine.trade_quality.promotion_policy import (
    apply_promotion,
    disable_promotion,
    promotion_dry_run,
    promotions_payload,
)
from laoma_signal_engine.trade_quality.promotion_candidates import (
    promotion_candidates_payload,
    rebuild_promotion_candidates,
)
from laoma_signal_engine.trade_quality.replay_backfill import (
    replay_backfill_ledger_rows,
    replay_backfill_payload,
    replay_backfill_summary,
)
from laoma_signal_engine.trade_quality.diagnostics import (
    diagnostic_aggregates_payload,
    diagnostic_archive_packages_payload,
    diagnostic_backfill_payload,
    diagnostic_entry_context_v3_payload,
    diagnostic_entry_feature_payload,
    diagnostic_entry_market_context_payload,
    diagnostic_entry_microstructure_payload,
    diagnostic_replay_ledger_payload,
    diagnostic_replay_payload,
    diagnostic_sample_detail_payload,
    diagnostic_samples_payload,
    diagnostic_summary_payload,
    diagnostic_sync_status_payload,
)
from laoma_signal_engine.backtest.p21 import (
    baseline_payload as p21_baseline_payload,
    experiment_detail_payload as p21_experiment_detail_payload,
    experiments_payload as p21_experiments_payload,
    export_config_candidate_payload as p21_export_config_candidate_payload,
    packages_payload as p21_packages_payload,
    recommendations_payload as p21_recommendations_payload,
    run_matrix_payload as p21_run_matrix_payload,
)
from laoma_signal_engine.backtest.p21_v2 import (
    config_matrix_contract_payload as p21_v2_config_matrix_contract_payload,
    download_kline_cache_payload as p21_v2_download_kline_cache_payload,
    experiment_daily_payload as p21_v2_experiment_daily_payload,
    experiment_detail_payload as p21_v2_experiment_detail_payload,
    experiment_orders_payload as p21_v2_experiment_orders_payload,
    experiment_symbols_payload as p21_v2_experiment_symbols_payload,
    experiments_payload as p21_v2_experiments_payload,
    export_config_candidate_payload as p21_v2_export_config_candidate_payload,
    jobs_payload as p21_v2_jobs_payload,
    job_status_payload as p21_v2_job_status_payload,
    kline_cache_status_payload as p21_v2_kline_cache_status_payload,
    leaderboard_payload as p21_v2_leaderboard_payload,
    run_config_matrix_payload as p21_v2_run_config_matrix_payload,
    start_job_payload as p21_v2_start_job_payload,
    stop_job_payload as p21_v2_stop_job_payload,
)
from laoma_signal_engine.backtest.p21_trade_quality import (
    aggregates_payload as p21_v2_quality_aggregates_payload,
    materialize_payload as p21_v2_quality_materialize_payload,
    packages_payload as p21_v2_quality_packages_payload,
    samples_payload as p21_v2_quality_samples_payload,
    summary_payload as p21_v2_quality_summary_payload,
)
from laoma_signal_engine.backtest.p21_gate_scoring import (
    batch_materialize_payload as p21_v2_gate_batch_materialize_payload,
    buckets_payload as p21_v2_gate_buckets_payload,
    candidates_payload as p21_v2_gate_candidates_payload,
    features_payload as p21_v2_gate_features_payload,
    generate_candidates_payload as p21_v2_gate_generate_candidates_payload,
    materialize_features_payload as p21_v2_gate_materialize_features_payload,
    rebuild_buckets_payload as p21_v2_gate_rebuild_buckets_payload,
    rebuild_scores_payload as p21_v2_gate_rebuild_scores_payload,
    recommendations_payload as p21_v2_gate_recommendations_payload,
    scores_payload as p21_v2_gate_scores_payload,
)
from laoma_signal_engine.backtest.p21_trade_quality_v4 import (
    deep_root_payload as tq_v4_deep_root_payload,
    evidence_payload as tq_v4_evidence_payload,
    gate_candidates_payload as tq_v4_gate_candidates_payload,
    generate_gate_candidates_v4_payload,
    materialize_v4_payload,
    summary_payload as tq_v4_summary_payload,
)
from laoma_signal_engine.backtest.p21_trade_quality_v5 import (
    causal_factors_payload as tq_v5_causal_factors_payload,
    gate_candidates_payload as tq_v5_gate_candidates_payload,
    generate_gate_candidates_v5_payload,
    materialize_v5_payload,
    summary_payload as tq_v5_summary_payload,
    writer_coverage_payload as tq_v5_writer_coverage_payload,
)
from laoma_signal_engine.research_db import (
    dataset_cards_payload as research_db_dataset_cards_payload,
    entry_features_payload as research_db_entry_features_payload,
    field_coverage_payload as research_db_field_coverage_payload,
    lineage_audit_payload as research_db_lineage_audit_payload,
    materialize_payload as research_db_materialize_payload,
    summary_payload as research_db_summary_payload,
    tq_samples_payload as research_db_tq_samples_payload,
    trade_facts_payload as research_db_trade_facts_payload,
    writer_status_payload as research_db_writer_status_payload,
)
from laoma_signal_engine.backtest.p21_strategy4_replay import (
    run_strategy4_replay_payload as p21_v2_strategy4_replay_run_payload,
    strategy4_replay_attempts_payload as p21_v2_strategy4_replay_attempts_payload,
    strategy4_replay_pool_payload as p21_v2_strategy4_replay_pool_payload,
    strategy4_replay_summary_payload as p21_v2_strategy4_replay_summary_payload,
)
from laoma_signal_engine.backtest.p21_ops import (
    enhanced_validation_payload as p21_v2_ops_enhanced_validation_payload,
    enqueue_tq_materialization_job as p21_v2_ops_enqueue_tq_materialization_job,
    export_candidate_audit_package as p21_v2_ops_export_candidate_audit_package,
    footprint_payload as p21_v2_ops_footprint_payload,
    process_next_tq_materialization_job as p21_v2_ops_process_next_tq_materialization_job,
    rebuild_serving_read_model_payload as p21_v2_ops_rebuild_serving_read_model_payload,
    retention_manifest_payload as p21_v2_ops_retention_manifest_payload,
    serving_summary_payload as p21_v2_ops_serving_summary_payload,
    tq_materialization_jobs_payload as p21_v2_ops_tq_materialization_jobs_payload,
)
from laoma_signal_engine.strategy_sandbox.service import (
    active_sandbox_payload as sandbox_active_payload,
    branches_payload as sandbox_branches_payload,
    add_code_patch_payload as sandbox_add_code_patch_payload,
    build_runtime_payload as sandbox_build_runtime_payload,
    cancel_full_backtest_run_payload as sandbox_cancel_full_backtest_run_payload,
    code_overlay_payload as sandbox_code_overlay_payload,
    create_full_backtest_run_payload as sandbox_create_full_backtest_run_payload,
    create_sandbox_payload as sandbox_create_payload,
    create_code_overlay_payload as sandbox_create_code_overlay_payload,
    db_health_payload as sandbox_db_health_payload,
    delete_sandbox_payload as sandbox_delete_payload,
    external_integration_audit_events_payload as sandbox_external_integration_audit_events_payload,
    external_integration_health_payload as sandbox_external_integration_health_payload,
    external_integration_run_payload as sandbox_external_integration_run_payload,
    full_backtest_run_payload as sandbox_full_backtest_run_payload,
    gated_orders_payload as sandbox_gated_orders_payload,
    gated_paper_shadow_payload as sandbox_gated_paper_shadow_payload,
    gated_performance_payload as sandbox_gated_performance_payload,
    gated_replay_payload as sandbox_gated_replay_payload,
    gated_trade_quality_samples_payload as sandbox_gated_trade_quality_samples_payload,
    gate_compare_payload as sandbox_gate_compare_payload,
    get_sandbox_payload as sandbox_get_payload,
    ingest_gate_action_payload as sandbox_ingest_gate_action_payload,
    job_payload as sandbox_job_payload,
    leaderboard_payload as sandbox_leaderboard_payload,
    list_sandboxes_payload as sandbox_list_payload,
    resume_full_backtest_run_payload as sandbox_resume_full_backtest_run_payload,
    set_active_sandbox_payload as sandbox_set_active_payload,
    summary_payload as sandbox_summary_payload,
    runtime_smoke_payload as sandbox_runtime_smoke_payload,
    trade_quality_compare_payload as sandbox_trade_quality_compare_payload,
    trade_candidates_payload as sandbox_trade_candidates_payload,
    universe_payload as sandbox_universe_payload,
)
from laoma_signal_engine.strategy_sandbox.resource_governor import (
    finish_ui_sandbox_pipeline_context,
    governor_status as sandbox_resource_governor_status,
    resource_run_payload as sandbox_resource_governor_run_payload,
    resource_runs_payload as sandbox_resource_governor_runs_payload,
    rest_budget_snapshot as sandbox_resource_rest_budget_snapshot,
    start_ui_sandbox_pipeline_context,
    stop_ui_sandbox_pipeline_context,
)
from laoma_signal_engine.strategy_sandbox.paper_pipeline import run_sandbox_paper_pipeline
from laoma_signal_engine.strategy_sandbox.daemon_writer import daemon_writer_status_payload
from laoma_signal_engine.strategy_sandbox.full_pipeline import run_sandbox_full_pipeline
from laoma_signal_engine.notifications.config import load_feishu_config
from laoma_signal_engine.notifications.service import delivery_history, send_trade_plan_notifications
from laoma_signal_engine.audit.run_audit import get_run_audit as _get_run_audit
from laoma_signal_engine.audit.run_audit import list_run_audits as _list_run_audits
from laoma_signal_engine.micro.data_quality_attribution import (
    get_micro_evidence_runtime_v2,
    get_micro_quality_attribution,
    ingest_micro_evidence_runtime_v2_to_sqlite,
)
from laoma_signal_engine.micro.training_ledger import (
    coverage_payload as micro_training_coverage_payload,
    latest_training_payload,
    run_list as micro_training_run_list,
    run_payload as micro_training_run_payload,
    symbol_payload as micro_training_symbol_payload,
)
from laoma_signal_engine.micro.target_source_ledger import latest_target_source_ledger
from laoma_signal_engine.market.light_snapshot_settings import load_light_snapshot_settings
from laoma_signal_engine.market.light_snapshot_async import _load_exchange_info_cache
from laoma_signal_engine.market.rest_circuit import close_rest_circuit, read_rest_circuit
from laoma_signal_engine.market.snapshot_daemon import snapshot_daemon_status
from laoma_signal_engine.scanner.current_freshness import build_step2_current_freshness
from laoma_signal_engine.scheduler_5m import inspect_scheduler_lock

PROJECT_ROOT = Path.cwd().resolve()
CONFIG_PATH = package_root() / "config" / "default.yaml"
CONFIG_FIELD_IMPACT_MAP_PATH = PROJECT_ROOT / "DATA" / "runtime" / "config_field_used_by_map.json"
CONFIG_FIELD_IMPACT_SUMMARY_PATH = PROJECT_ROOT / "DATA" / "runtime" / "config_field_used_by_map_summary.json"
MINIMAL_PROFILE_CLEANUP_PLAN_PATH = PROJECT_ROOT / "DATA" / "runtime" / "step26_5_minimal_profile_config_cleanup_plan.json"
API_PIPELINE_PID_PATH = PROJECT_ROOT / "DATA" / "runtime" / "api_pipeline_interval.pid"
PIPELINE_PROGRESS_PATH = PROJECT_ROOT / "DATA" / "runtime" / "strategy_pipeline_progress.json"

CURRENT_JSON_PATHS = {
    "trade_plan_without_micro": PROJECT_ROOT / "DATA/decisions/latest_trade_plan_without_micro.json",
    "trade_plan_micro_fast": PROJECT_ROOT / "DATA/decisions/latest_trade_plan_micro_fast.json",
    "trade_plan_micro_full": PROJECT_ROOT / "DATA/decisions/latest_trade_plan_micro_full.json",
    "trade_plan_strategy4": PROJECT_ROOT / "DATA/decisions/latest_trade_plan_strategy4.json",
    "trade_plan_strategy5": PROJECT_ROOT / "DATA/decisions/latest_trade_plan_strategy5.json",
    "trade_plan_strategy6": PROJECT_ROOT / "DATA/decisions/latest_trade_plan_strategy6.json",
    "strategy5_evidence": PROJECT_ROOT / "DATA/strategy5/latest_direction_evidence.json",
    "strategy6_evidence": PROJECT_ROOT / "DATA/strategy6/latest_evidence.json",
    "strategy6_decisions": PROJECT_ROOT / "DATA/strategy6/latest_decisions.json",
    "strategy6_wait_pool": PROJECT_ROOT / "DATA/strategy6/latest_wait_pool.json",
    "candidate_universe": PROJECT_ROOT / "DATA/universe/CANDIDATE_UNIVERSE.json",
    "futures_light_snapshot": PROJECT_ROOT / "DATA/market/futures_light_snapshot.json",
    "latest_decisions": PROJECT_ROOT / "DATA/decisions/latest_decisions.json",
    "latest_strategy": PROJECT_ROOT / "DATA/reports/latest_strategy_pipeline_report.json",
    "latest_audit": PROJECT_ROOT / "DATA/reports/latest_current_json_chain_audit_summary.json",
    "abc": PROJECT_ROOT / "DATA/reports/latest_trade_plan_lines_compare.json",
}

CONFIG_SECTION_ALIASES = {
    "strategy-pipeline": "strategy_pipeline",
    "strategy_pipeline": "strategy_pipeline",
    "micro-daemon": "micro_daemon_cli",
    "trade-plan-lines": "trade_plan_lines",
    "trade_plan_lines": "trade_plan_lines",
    "market-entry-liquidity": "market_entry_liquidity",
    "liquidity": "market_entry_liquidity",
    "decision-refresh": "decision_refresh",
    "decision_refresh": "decision_refresh",
    "project-runtime": "project_runtime",
    "paper": "paper",
    "position-sizing": "position_sizing",
    "position_sizing": "position_sizing",
    "feishu": "feishu",
}

STRATEGY_LINE_KEYS = {"without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"}
STRATEGY_LINES_ORDERED = ("without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6")
PIPELINE_STRATEGY_LINES_ORDERED = ("without_micro", "micro_fast", "micro_full", "strategy5", "strategy6")
TRADE_PLAN_LINE_ALLOWED_KEYS = {
    "allow_market_entry",
    "allow_wait_plan",
    "allow_market_now",
    "allow_limit_pullback",
    "allow_breakout_trigger",
    "min_score",
    "require_refresh_fresh",
    "require_direction_still_valid",
    "require_range_room_ok",
    "require_liquidity_ok",
    "require_micro_ready",
    "require_micro_alignment",
    "require_micro_symbol_lifecycle_confirmed",
    "micro_consumption_policy",
    "allow_weak_micro_consumption",
    "weak_micro_min_state",
    "weak_micro_require_signal_usable",
    "weak_micro_require_direction_not_conflict",
    "weak_micro_block_reasons",
    "max_refresh_age_sec",
    "max_liquidity_age_sec",
    "max_micro_age_sec",
    "target_rr",
    "min_rr",
    "min_net_rr",
    "min_stop_bps",
    "preferred_stop_bps",
    "max_stop_bps",
    "min_tp_after_cost_bps",
    "min_effective_rr",
    "min_reachable_reward_bps",
    "allow_fallback_target_for_executable",
    "taker_fee_bps",
    "maker_fee_bps",
    "atr_1m_mult",
    "atr_5m_mult",
    "max_pullback_bps",
    "conditional_plan_expire_sec",
    "stop_atr_mult",
    "max_stop_atr_mult",
    "profile_gate_enabled",
    "min_profile_market_entry_score",
    "min_profile_hf_stop_score",
    "max_profile_slippage_risk_score",
    "position_sizing",
    "trade_plan_risk",
    "short_now_calibration",
    "market_now_calibration",
    "tp_target_policy",
    "trade_quality_gate",
    "sl_tp_quality",
}
TP_TARGET_POLICY_ALLOWED_KEYS = {
    "mode",
    "target_rr",
    "target_rr_cap",
    "target_rr_basis",
    "target_net_rr",
    "min_target_net_rr",
    "max_target_net_rr",
    "min_reward_bps",
    "require_market_room",
    "market_room_buffer_bps",
    "allow_structure_runner",
    "reward_to_spread_min",
    "include_entry_fee",
    "include_exit_fee",
    "include_slippage_reserve",
    "slippage_reserve_bps",
    "max_loss_net_r",
    "sizing_basis",
}
TP_TARGET_POLICY_MODES = {"structure", "fast_capped_rr", "structure_or_capped_rr"}
TRADE_QUALITY_GATE_ALLOWED_KEYS = {
    "enabled",
    "mode",
    "min_samples_per_symbol",
    "min_samples_per_root_cause",
    "max_negative_expectancy_R",
    "signal_no_edge_wait_enabled",
    "side_specific_enabled",
}
SL_TP_QUALITY_ALLOWED_KEYS = {
    "enabled",
    "mode",
    "single_tp_only",
    "min_samples_per_cluster",
    "stop_too_tight_widen_factor",
    "tp_too_far_reduce_factor",
    "entered_too_early_wait_enabled",
}
MARKET_NOW_SIDE_ALLOWED_KEYS = {
    "min_range_pos",
    "max_range_pos",
    "min_available_room_bps",
    "max_stop_bps",
    "max_stop_atr_mult",
    "min_net_rr",
    "allow_if_liquidity_missing",
    "max_spread_bps",
    "max_slippage_bps",
    "require_recent_up_impulse",
    "reject_if_pullback_required",
    "require_recent_down_impulse",
    "reject_if_rebound_required",
}
MARKET_NOW_ALLOWED_KEYS = {
    "enabled",
    "legacy_short_now_fallback",
    "long",
    "short",
}
POSITION_SIZING_ALLOWED_KEYS = {
    "enabled",
    "method",
    "account_equity_usdt",
    "default_leverage",
    "risk_budget_usdt",
    "risk_pct_equity",
    "min_risk_budget_usdt",
    "max_risk_budget_usdt",
    "max_margin_usdt",
    "min_notional_usdt",
    "max_notional_usdt",
    "include_fee_in_risk_budget",
    "reject_if_capped_below_min_risk",
}
TRADE_PLAN_RISK_ALLOWED_KEYS = {
    "planned_loss_guard_enabled",
    "sizing_policy",
    "base_notional_usdt",
    "target_planned_loss_usdt",
    "max_planned_loss_usdt",
    "min_planned_notional_usdt",
    "allow_notional_resize",
    "reject_if_resized_below_min_notional",
    "include_fee_in_planned_loss",
    "paper_fallback_notional_allowed",
}
STRATEGY_PIPELINE_MICRO_ALLOWED_KEYS = {
    "require_daemon",
    "health_preflight_enabled",
    "unhealthy_policy",
    "preflight_heartbeat_stale_sec",
    "health_grace_recheck_enabled",
    "health_grace_wait_sec",
    "health_grace_max_attempts",
    "health_grace_accept_fresh_heartbeat_sec",
    "inline_recovery",
    "wait_fast_sec",
    "wait_full_sec",
    "max_wait_sec",
    "min_full_ready_count",
    "full_wait_policy",
    "max_active_symbols",
    "timeout_policy",
    "require_fresh_step2_before_micro",
    "auto_refresh_step2_before_micro",
    "step2_max_age_sec_for_micro",
    "empty_target_daemon_state",
    "sticky_pool_enabled",
    "sticky_ttl_sec",
    "sticky_min_cycles",
    "sticky_max_cycles",
    "sticky_include_ready_symbols",
}
PAPER_DAEMON_ALLOWED_KEYS = {
    "enabled",
    "auto_start_with_project",
    "tick_interval_sec",
    "candle_interval",
    "singleton_lock_path",
    "pid_path",
    "log_path",
    "heartbeat_path",
    "status_path",
    "stale_after_sec",
    "catchup_on_start",
    "max_catchup_candles",
}
PAPER_ARCHIVE_ALLOWED_KEYS = {
    "enabled",
    "archive_dir",
    "metadata_path",
    "open_position_policy",
    "forced_close_exit_reason",
}
PAPER_FILL_MODEL_ALLOWED_KEYS = {
    "mode",
    "use_trade_plan_slippage",
    "use_liquidity_profile",
    "entry_delay_sec",
    "max_entry_drift_bps",
    "default_market_slippage_bps",
    "fallback_market_slippage_bps",
    "volatility_slippage_mult",
    "thin_book_slippage_mult",
    "max_allowed_paper_slippage_bps",
    "slippage_too_high_policy",
    "same_candle_sl_tp_policy",
}
RUN_CYCLE_WATCHDOG_ALLOWED_KEYS = {
    "enabled",
    "heartbeat_interval_sec",
    "stale_after_sec",
    "long_stage_grace_sec",
    "interval_wait_grace_sec",
    "fail_on_missing_pid",
    "warn_on_inactive_active_job_residue",
}

ALLOWED_CONFIG_KEYS = {
    "strategy_pipeline": {
        "mode",
        "interval_sec",
        "overlap_policy",
        "run_lines",
        "fetch_mode",
        "max_concurrency",
        "force_universe",
        "light_limit",
        "scan_allow_stale_input",
        "auto_refresh_before_trade_plan",
        "require_refresh_anchor_current_factor",
        "aggregate_final_decisions",
        "run_abc_audit",
        "run_json_stage_audit",
        "allow_llm_stale",
        "micro",
    },
    "project_runtime": {
        "autostart_enabled",
        "autostart_micro_daemon",
        "autostart_paper_daemon",
        "autostart_snapshot_daemon",
        "startup_grace_sec",
        "heartbeat_stale_sec",
        "restart_on_stale",
        "daemon_health_check_interval_sec",
        "restart_cooldown_sec",
        "max_restart_attempts_per_hour",
        "restart_micro_daemon_on_stale",
        "restart_paper_daemon_on_stale",
        "stop_children_on_api_shutdown",
        "run_cycle_watchdog",
    },
    "micro_daemon_cli": {
        "transport",
        "start_mode",
        "pid_path",
        "log_path",
        "heartbeat_path",
        "state_path",
        "features_path",
    },
    "micro_daemon_state": {
        "enabled",
        "output",
        "target_ack_timeout_sec",
        "fast_min_collect_sec",
        "full_min_collect_sec",
        "consumer_state_max_age_sec",
        "require_target_ack_for_fast_full",
    },
    "trade_plan_lines": {"without_micro", "micro_fast", "micro_full"},
    "decision_refresh": {
        "max_refresh_age_sec",
        "max_liquidity_age_sec",
        "max_age_sec",
        "range_room",
    },
    "market_entry_liquidity": {
        "margin_usdt",
        "leverage",
        "notional_usdt",
        "max_spread_bps",
        "max_estimated_slippage_bps",
        "min_top_depth_usdt",
        "min_quote_volume_24h",
        "depth_limit",
    },
    "market_entry_direction": {
        "allow_watch_market_entry",
        "min_suitability_score",
        "watch_min_suitability_score",
    },
    "step2": {
        "market_entry_preferred_min_score",
        "market_entry_allowed_min_score",
        "trade_candidate_observe_min_score",
        "promote_raw_market_entry_allowed",
        "raw_promote_min_market_entry_score",
        "raw_promote_min_scan_score",
        "raw_promote_limit",
    },
    "micro_router": {
        "max_active_micro_symbols",
        "warm_watch_limit",
        "active_strong_limit",
        "include_raw_in_warm_pool",
        "priority_mode",
        "allow_trade_rank_priority",
        "exclude_market_entry_avoid_from_micro",
        "manual_watchlist_priority_bonus",
        "target_hysteresis_enabled",
        "min_target_hold_cycles",
        "min_target_hold_sec",
        "max_target_churn_ratio_per_cycle",
        "keep_previous_ready_targets",
        "min_collect_seconds",
        "ttl_seconds_tier1",
        "ttl_seconds_tier2",
        "target_ready_tf",
        "tier1_subscribe",
        "tier2_subscribe",
    },
    "paper": {
        "enabled",
        "db_path",
        "summary_path",
        "lines",
        "default_account_equity_usdt",
        "default_margin_usdt",
        "default_leverage",
        "paper_fallback_notional_allowed",
        "taker_fee_bps",
        "maker_fee_bps",
        "default_slippage_bps",
        "prevent_same_line_symbol_reentry",
        "active_slot_scope",
        "allow_cross_line_same_symbol",
        "max_trade_plan_age_sec",
        "reentry_cooldown_sec",
        "reentry_cooldown_scope",
        "reentry_cooldown_after",
        "fill_model",
        "archive",
        "daemon",
    },
    "position_sizing": POSITION_SIZING_ALLOWED_KEYS,
    "trade_plan_risk": TRADE_PLAN_RISK_ALLOWED_KEYS,
    "feishu": {
        "enabled",
        "webhook_url",
        "webhook_secret",
        "keyword",
        "message_mode",
        "notify_trade_plan",
        "notify_paper_order",
        "notify_pipeline_summary",
        "notify_audit_failure",
        "notify_lines",
        "strategy_display_names",
        "executable_only",
        "min_opportunity_level",
        "include_non_executable_opportunities",
        "block_pipeline_on_partial_failure",
        "block_pipeline_on_total_failure",
        "timeout_sec",
    },
}

SECRET_KEYS = {"webhook_url", "webhook_secret"}

NESTED_CONFIG_KEYS: dict[str, dict[str, set[str]]] = {
    "trade_plan_lines": {line: TRADE_PLAN_LINE_ALLOWED_KEYS for line in STRATEGY_LINE_KEYS},
    "strategy_pipeline": {"micro": STRATEGY_PIPELINE_MICRO_ALLOWED_KEYS},
    "project_runtime": {"run_cycle_watchdog": RUN_CYCLE_WATCHDOG_ALLOWED_KEYS},
    "paper": {"daemon": PAPER_DAEMON_ALLOWED_KEYS, "archive": PAPER_ARCHIVE_ALLOWED_KEYS, "fill_model": PAPER_FILL_MODEL_ALLOWED_KEYS},
    "decision_refresh": {"range_room": {"long_max_range_pos", "short_min_range_pos"}},
    "feishu": {"strategy_display_names": STRATEGY_LINE_KEYS},
}
NUMERIC_CONFIG_PATHS = {
    ("strategy_pipeline", "interval_sec"),
    ("strategy_pipeline", "max_concurrency"),
    ("strategy_pipeline", "light_limit"),
    ("strategy_pipeline", "micro", "preflight_heartbeat_stale_sec"),
    ("strategy_pipeline", "micro", "wait_fast_sec"),
    ("strategy_pipeline", "micro", "wait_full_sec"),
    ("strategy_pipeline", "micro", "max_wait_sec"),
    ("strategy_pipeline", "micro", "min_full_ready_count"),
    ("strategy_pipeline", "micro", "max_active_symbols"),
    ("strategy_pipeline", "micro", "step2_max_age_sec_for_micro"),
    ("strategy_pipeline", "micro", "sticky_ttl_sec"),
    ("strategy_pipeline", "micro", "sticky_min_cycles"),
    ("strategy_pipeline", "micro", "sticky_max_cycles"),
    ("project_runtime", "run_cycle_watchdog", "heartbeat_interval_sec"),
    ("project_runtime", "run_cycle_watchdog", "stale_after_sec"),
    ("project_runtime", "run_cycle_watchdog", "long_stage_grace_sec"),
    ("project_runtime", "run_cycle_watchdog", "interval_wait_grace_sec"),
    ("market_entry_liquidity", "margin_usdt"),
    ("market_entry_liquidity", "leverage"),
    ("market_entry_liquidity", "notional_usdt"),
    ("market_entry_liquidity", "max_spread_bps"),
    ("market_entry_liquidity", "max_estimated_slippage_bps"),
    ("market_entry_liquidity", "min_top_depth_usdt"),
    ("market_entry_liquidity", "min_quote_volume_24h"),
    ("market_entry_liquidity", "depth_limit"),
    ("market_entry_direction", "min_suitability_score"),
    ("market_entry_direction", "watch_min_suitability_score"),
    ("step2", "market_entry_preferred_min_score"),
    ("step2", "market_entry_allowed_min_score"),
    ("step2", "trade_candidate_observe_min_score"),
    ("step2", "raw_promote_min_market_entry_score"),
    ("step2", "raw_promote_min_scan_score"),
    ("step2", "raw_promote_limit"),
    ("micro_router", "max_active_micro_symbols"),
    ("micro_router", "warm_watch_limit"),
    ("micro_router", "active_strong_limit"),
    ("micro_router", "manual_watchlist_priority_bonus"),
    ("micro_router", "min_target_hold_cycles"),
    ("micro_router", "min_target_hold_sec"),
    ("micro_router", "max_target_churn_ratio_per_cycle"),
    ("micro_router", "min_collect_seconds"),
    ("micro_router", "ttl_seconds_tier1"),
    ("micro_router", "ttl_seconds_tier2"),
    ("micro_daemon_state", "target_ack_timeout_sec"),
    ("micro_daemon_state", "fast_min_collect_sec"),
    ("micro_daemon_state", "full_min_collect_sec"),
    ("micro_daemon_state", "consumer_state_max_age_sec"),
    ("decision_refresh", "max_refresh_age_sec"),
    ("decision_refresh", "max_liquidity_age_sec"),
    ("decision_refresh", "max_age_sec"),
    ("decision_refresh", "range_room", "long_max_range_pos"),
    ("decision_refresh", "range_room", "short_min_range_pos"),
    ("paper", "default_account_equity_usdt"),
    ("paper", "default_margin_usdt"),
    ("paper", "default_leverage"),
    ("paper", "taker_fee_bps"),
    ("paper", "maker_fee_bps"),
    ("paper", "default_slippage_bps"),
    ("paper", "fill_model", "entry_delay_sec"),
    ("paper", "fill_model", "max_entry_drift_bps"),
    ("paper", "fill_model", "default_market_slippage_bps"),
    ("paper", "fill_model", "fallback_market_slippage_bps"),
    ("paper", "fill_model", "volatility_slippage_mult"),
    ("paper", "fill_model", "thin_book_slippage_mult"),
    ("paper", "fill_model", "max_allowed_paper_slippage_bps"),
    ("position_sizing", "account_equity_usdt"),
    ("position_sizing", "default_leverage"),
    ("position_sizing", "risk_budget_usdt"),
    ("position_sizing", "risk_pct_equity"),
    ("position_sizing", "min_risk_budget_usdt"),
    ("position_sizing", "max_risk_budget_usdt"),
    ("position_sizing", "max_margin_usdt"),
    ("position_sizing", "min_notional_usdt"),
    ("position_sizing", "max_notional_usdt"),
    ("trade_plan_risk", "base_notional_usdt"),
    ("trade_plan_risk", "target_planned_loss_usdt"),
    ("trade_plan_risk", "max_planned_loss_usdt"),
    ("trade_plan_risk", "min_planned_notional_usdt"),
    ("paper", "daemon", "tick_interval_sec"),
    ("paper", "daemon", "stale_after_sec"),
    ("paper", "daemon", "max_catchup_candles"),
    ("feishu", "timeout_sec"),
    ("strategy_pipeline", "micro", "health_grace_wait_sec"),
    ("strategy_pipeline", "micro", "health_grace_max_attempts"),
    ("strategy_pipeline", "micro", "health_grace_accept_fresh_heartbeat_sec"),
}

for _line in STRATEGY_LINE_KEYS:
    for _key in {
        "min_score",
        "max_refresh_age_sec",
        "max_liquidity_age_sec",
        "max_micro_age_sec",
        "target_rr",
        "min_rr",
        "min_net_rr",
        "min_effective_rr",
        "min_reachable_reward_bps",
        "min_stop_bps",
        "preferred_stop_bps",
        "max_stop_bps",
        "min_tp_after_cost_bps",
        "taker_fee_bps",
        "maker_fee_bps",
        "atr_1m_mult",
        "atr_5m_mult",
        "max_pullback_bps",
        "conditional_plan_expire_sec",
        "stop_atr_mult",
        "max_stop_atr_mult",
        "min_profile_market_entry_score",
        "min_profile_hf_stop_score",
        "max_profile_slippage_risk_score",
    }:
        NUMERIC_CONFIG_PATHS.add(("trade_plan_lines", _line, _key))
    for _key in {
        "base_notional_usdt",
        "target_planned_loss_usdt",
        "max_planned_loss_usdt",
        "min_planned_notional_usdt",
    }:
        NUMERIC_CONFIG_PATHS.add(("trade_plan_lines", _line, "trade_plan_risk", _key))
    for _key in {
        "target_rr",
        "target_rr_cap",
        "target_net_rr",
        "min_target_net_rr",
        "max_target_net_rr",
        "min_reward_bps",
        "market_room_buffer_bps",
        "reward_to_spread_min",
        "slippage_reserve_bps",
        "max_loss_net_r",
    }:
        NUMERIC_CONFIG_PATHS.add(("trade_plan_lines", _line, "tp_target_policy", _key))
    for _side in {"long", "short"}:
        for _key in {
            "min_range_pos",
            "max_range_pos",
            "min_available_room_bps",
            "max_stop_bps",
            "max_stop_atr_mult",
            "min_net_rr",
            "max_spread_bps",
            "max_slippage_bps",
        }:
            NUMERIC_CONFIG_PATHS.add(("trade_plan_lines", _line, "market_now_calibration", _side, _key))
    for _key in {
        "min_samples_per_symbol",
        "min_samples_per_root_cause",
        "max_negative_expectancy_R",
    }:
        NUMERIC_CONFIG_PATHS.add(("trade_plan_lines", _line, "trade_quality_gate", _key))
    for _key in {
        "min_samples_per_cluster",
        "stop_too_tight_widen_factor",
        "tp_too_far_reduce_factor",
    }:
        NUMERIC_CONFIG_PATHS.add(("trade_plan_lines", _line, "sl_tp_quality", _key))


def _market_now_profile(
    *,
    long_min_range: float,
    long_max_range: float,
    short_min_range: float,
    short_max_range: float,
    min_room_bps: float,
    max_stop_bps: float,
    max_stop_atr_mult: float,
    min_net_rr: float,
    max_spread_bps: float,
    max_slippage_bps: float,
) -> dict[str, Any]:
    common = {
        "min_available_room_bps": min_room_bps,
        "max_stop_bps": max_stop_bps,
        "max_stop_atr_mult": max_stop_atr_mult,
        "min_net_rr": min_net_rr,
        "allow_if_liquidity_missing": False,
        "max_spread_bps": max_spread_bps,
        "max_slippage_bps": max_slippage_bps,
    }
    return {
        "enabled": True,
        "legacy_short_now_fallback": True,
        "long": {
            **common,
            "min_range_pos": long_min_range,
            "max_range_pos": long_max_range,
            "require_recent_up_impulse": True,
            "reject_if_pullback_required": True,
        },
        "short": {
            **common,
            "min_range_pos": short_min_range,
            "max_range_pos": short_max_range,
            "require_recent_down_impulse": True,
            "reject_if_rebound_required": True,
        },
    }


RELAXED_PROFIT_MARKET_NOW = _market_now_profile(
    long_min_range=0.04,
    long_max_range=0.96,
    short_min_range=0.04,
    short_max_range=0.96,
    min_room_bps=10,
    max_stop_bps=900,
    max_stop_atr_mult=6.0,
    min_net_rr=0.1,
    max_spread_bps=160,
    max_slippage_bps=300,
)
RELAXED_PROFIT_MARKET_NOW["long"].update(
    {
        "allow_if_liquidity_missing": True,
        "require_recent_up_impulse": False,
        "reject_if_pullback_required": False,
    },
)
RELAXED_PROFIT_MARKET_NOW["short"].update(
    {
        "allow_if_liquidity_missing": True,
        "require_recent_down_impulse": False,
        "reject_if_rebound_required": False,
    },
)
BALANCED_MARKET_NOW = _market_now_profile(
    long_min_range=0.16,
    long_max_range=0.84,
    short_min_range=0.16,
    short_max_range=0.84,
    min_room_bps=35,
    max_stop_bps=460,
    max_stop_atr_mult=4.0,
    min_net_rr=0.65,
    max_spread_bps=80,
    max_slippage_bps=150,
)
PRODUCTION_MARKET_NOW = _market_now_profile(
    long_min_range=0.18,
    long_max_range=0.82,
    short_min_range=0.18,
    short_max_range=0.82,
    min_room_bps=45,
    max_stop_bps=420,
    max_stop_atr_mult=3.5,
    min_net_rr=0.9,
    max_spread_bps=50,
    max_slippage_bps=100,
)

RELAXED_TEST_TRADE_PLAN_RISK = {
    "planned_loss_guard_enabled": True,
    "sizing_policy": "notional_by_loss_cap",
    "base_notional_usdt": 2000,
    "target_planned_loss_usdt": 50,
    "max_planned_loss_usdt": 80,
    "min_planned_notional_usdt": 20,
    "allow_notional_resize": True,
    "reject_if_resized_below_min_notional": True,
    "include_fee_in_planned_loss": True,
    "paper_fallback_notional_allowed": False,
}
RELAXED_PROFIT_TRADE_PLAN_RISK = {
    **RELAXED_TEST_TRADE_PLAN_RISK,
    "target_planned_loss_usdt": 60,
    "max_planned_loss_usdt": 100,
}
BALANCED_TRADE_PLAN_RISK = {
    **RELAXED_TEST_TRADE_PLAN_RISK,
    "target_planned_loss_usdt": 40,
    "max_planned_loss_usdt": 60,
}
PRODUCTION_TRADE_PLAN_RISK = {
    **RELAXED_TEST_TRADE_PLAN_RISK,
    "target_planned_loss_usdt": 25,
    "max_planned_loss_usdt": 40,
}

TRADE_QUALITY_GATE_WARN = {
    "enabled": False,
    "mode": "off",
    "min_samples_per_symbol": 3,
    "min_samples_per_root_cause": 5,
    "max_negative_expectancy_R": -0.6,
    "signal_no_edge_wait_enabled": True,
    "side_specific_enabled": True,
}
TRADE_QUALITY_GATE_SHADOW = {**TRADE_QUALITY_GATE_WARN, "mode": "off"}
SL_TP_QUALITY_WARN = {
    "enabled": False,
    "mode": "off",
    "single_tp_only": True,
    "min_samples_per_cluster": 5,
    "stop_too_tight_widen_factor": 1.15,
    "tp_too_far_reduce_factor": 0.9,
    "entered_too_early_wait_enabled": True,
}
SL_TP_QUALITY_SHADOW = {**SL_TP_QUALITY_WARN, "mode": "off"}

TP_TARGET_POLICY_STRUCTURE = {
    "mode": "structure",
    "target_rr_basis": "gross",
    "target_net_rr": None,
    "min_target_net_rr": 0.25,
    "max_target_net_rr": 3.0,
    "min_reward_bps": 8,
    "require_market_room": True,
    "market_room_buffer_bps": 2,
    "allow_structure_runner": False,
    "reward_to_spread_min": 2.5,
    "include_entry_fee": True,
    "include_exit_fee": True,
    "include_slippage_reserve": False,
    "slippage_reserve_bps": 0,
    "max_loss_net_r": 1.10,
    "sizing_basis": "gross_stop",
}
TP_TARGET_POLICY_BALANCED_WITHOUT = {
    **TP_TARGET_POLICY_STRUCTURE,
    "mode": "structure_or_capped_rr",
    "target_rr": 0.65,
    "target_rr_cap": 0.75,
}
TP_TARGET_POLICY_BALANCED_MICRO_FAST = {
    **TP_TARGET_POLICY_STRUCTURE,
    "mode": "fast_capped_rr",
    "target_rr": 0.70,
    "target_rr_cap": 0.80,
}
TP_TARGET_POLICY_PRODUCTION_WITHOUT = {
    **TP_TARGET_POLICY_STRUCTURE,
    "mode": "structure_or_capped_rr",
    "target_rr": 0.75,
    "target_rr_cap": 0.90,
    "min_reward_bps": 12,
}
TP_TARGET_POLICY_PRODUCTION_MICRO_FAST = {
    **TP_TARGET_POLICY_STRUCTURE,
    "mode": "fast_capped_rr",
    "target_rr": 0.80,
    "target_rr_cap": 0.95,
    "min_reward_bps": 12,
}


CONFIG_PROFILES: dict[str, dict[str, Any]] = {
    "relaxed_test": {
        "active_profile": "relaxed_test",
        "trade_plan_risk": RELAXED_TEST_TRADE_PLAN_RISK,
        "paper": {"paper_fallback_notional_allowed": False},
        "step2": {
            "market_entry_preferred_min_score": 35,
            "market_entry_allowed_min_score": 25,
            "trade_candidate_observe_min_score": 25,
            "promote_raw_market_entry_allowed": True,
            "raw_promote_min_market_entry_score": 35,
            "raw_promote_min_scan_score": 20,
            "raw_promote_limit": 10,
        },
        "micro_router": {
            "max_active_micro_symbols": 20,
            "warm_watch_limit": 20,
            "active_strong_limit": 10,
            "include_raw_in_warm_pool": True,
            "min_collect_seconds": 300,
        },
        "micro_daemon_state": {
            "fast_min_collect_sec": 300,
            "full_min_collect_sec": 900,
        },
        "market_entry_liquidity": {
            "max_spread_bps": 80,
            "max_estimated_slippage_bps": 150,
            "min_top_depth_usdt": 500,
            "min_quote_volume_24h": 50000,
        },
        "market_entry_direction": {
            "min_suitability_score": 25,
            "watch_min_suitability_score": 35,
        },
        "decision_refresh": {"range_room": {"long_max_range_pos": 0.98, "short_min_range_pos": 0.02}},
        "trade_plan_lines": {
            "without_micro": {
                "min_score": 20,
                "require_range_room_ok": False,
                "require_liquidity_ok": False,
                "target_rr": 1.1,
                "min_rr": 0.1,
                "min_net_rr": 0.1,
                "min_effective_rr": 0.85,
                "min_reachable_reward_bps": 10,
                "allow_fallback_target_for_executable": False,
                "min_stop_bps": 5,
                "preferred_stop_bps": 25,
                "max_stop_bps": 800,
                "max_stop_atr_mult": 4.0,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 20,
                "min_profile_hf_stop_score": 0,
                "max_profile_slippage_risk_score": 90,
                "trade_quality_gate": TRADE_QUALITY_GATE_WARN,
                "sl_tp_quality": SL_TP_QUALITY_WARN,
            },
            "micro_fast": {
                "min_score": 20,
                "require_range_room_ok": False,
                "require_liquidity_ok": False,
                "require_micro_ready": True,
                "require_micro_alignment": True,
                "micro_consumption_policy": "ready_signal_usable",
                "allow_weak_micro_consumption": True,
                "weak_micro_min_state": "ready",
                "target_rr": 1.1,
                "min_rr": 0.1,
                "min_net_rr": 0.1,
                "min_effective_rr": 0.85,
                "min_reachable_reward_bps": 10,
                "allow_fallback_target_for_executable": False,
                "min_stop_bps": 5,
                "preferred_stop_bps": 25,
                "max_stop_bps": 800,
                "max_stop_atr_mult": 4.0,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 20,
                "min_profile_hf_stop_score": 0,
                "max_profile_slippage_risk_score": 90,
                "trade_quality_gate": TRADE_QUALITY_GATE_WARN,
                "sl_tp_quality": SL_TP_QUALITY_WARN,
            },
            "micro_full": {
                "min_score": 20,
                "require_range_room_ok": False,
                "require_liquidity_ok": False,
                "require_micro_ready": True,
                "require_micro_alignment": True,
                "micro_consumption_policy": "ready_signal_usable",
                "allow_weak_micro_consumption": True,
                "target_rr": 1.1,
                "min_rr": 0.1,
                "min_net_rr": 0.1,
                "min_effective_rr": 0.85,
                "min_reachable_reward_bps": 10,
                "allow_fallback_target_for_executable": False,
                "min_stop_bps": 5,
                "preferred_stop_bps": 30,
                "max_stop_bps": 900,
                "max_stop_atr_mult": 4.5,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 20,
                "min_profile_hf_stop_score": 0,
                "max_profile_slippage_risk_score": 90,
                "trade_quality_gate": TRADE_QUALITY_GATE_WARN,
                "sl_tp_quality": SL_TP_QUALITY_WARN,
            },
        },
        "strategy_pipeline": {"micro": {"max_active_symbols": 20, "wait_fast_sec": 300}},
    },
    "relaxed_profit": {
        "active_profile": "relaxed_profit",
        "trade_plan_risk": RELAXED_PROFIT_TRADE_PLAN_RISK,
        "paper": {"paper_fallback_notional_allowed": False},
        "step2": {
            "market_entry_preferred_min_score": 20,
            "market_entry_allowed_min_score": 15,
            "trade_candidate_observe_min_score": 15,
            "promote_raw_market_entry_allowed": True,
            "raw_promote_min_market_entry_score": 20,
            "raw_promote_min_scan_score": 15,
            "raw_promote_limit": 15,
        },
        "micro_router": {
            "max_active_micro_symbols": 20,
            "warm_watch_limit": 20,
            "active_strong_limit": 10,
            "include_raw_in_warm_pool": True,
            "min_collect_seconds": 300,
        },
        "micro_daemon_state": {
            "fast_min_collect_sec": 300,
            "full_min_collect_sec": 900,
        },
        "market_entry_liquidity": {
            "max_spread_bps": 160,
            "max_estimated_slippage_bps": 300,
            "min_top_depth_usdt": 100,
            "min_quote_volume_24h": 10000,
        },
        "market_entry_direction": {
            "min_suitability_score": 15,
            "watch_min_suitability_score": 20,
        },
        "decision_refresh": {"range_room": {"long_max_range_pos": 0.99, "short_min_range_pos": 0.01}},
        "trade_plan_lines": {
            "without_micro": {
                "min_score": 10,
                "require_range_room_ok": False,
                "require_liquidity_ok": False,
                "target_rr": 1.0,
                "min_rr": 0.05,
                "min_net_rr": 0.02,
                "min_effective_rr": 0.70,
                "min_reachable_reward_bps": 8,
                "allow_fallback_target_for_executable": False,
                "min_stop_bps": 3,
                "preferred_stop_bps": 20,
                "max_stop_bps": 1200,
                "max_stop_atr_mult": 6.0,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 10,
                "min_profile_hf_stop_score": 0,
                "max_profile_slippage_risk_score": 98,
                "trade_quality_gate": TRADE_QUALITY_GATE_WARN,
                "sl_tp_quality": SL_TP_QUALITY_WARN,
                "market_now_calibration": RELAXED_PROFIT_MARKET_NOW,
                "short_now_calibration": {
                    "enabled": True,
                    "min_range_pos": 0.04,
                    "max_range_pos": 0.96,
                    "min_available_room_bps": 10,
                    "max_stop_bps": 900,
                    "max_stop_atr_mult": 6.0,
                    "min_net_rr": 0.1,
                    "allow_if_liquidity_missing": True,
                    "max_spread_bps": 160,
                    "max_slippage_bps": 300,
                    "require_recent_down_impulse": False,
                    "reject_if_rebound_required": False,
                },
            },
            "micro_fast": {
                "min_score": 10,
                "require_range_room_ok": False,
                "require_liquidity_ok": False,
                "require_micro_ready": True,
                "require_micro_alignment": True,
                "micro_consumption_policy": "ready_signal_usable",
                "allow_weak_micro_consumption": True,
                "weak_micro_min_state": "ready",
                "target_rr": 1.0,
                "min_rr": 0.05,
                "min_net_rr": 0.02,
                "min_effective_rr": 0.70,
                "min_reachable_reward_bps": 8,
                "allow_fallback_target_for_executable": False,
                "min_stop_bps": 3,
                "preferred_stop_bps": 20,
                "max_stop_bps": 1200,
                "max_stop_atr_mult": 6.0,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 10,
                "min_profile_hf_stop_score": 0,
                "max_profile_slippage_risk_score": 98,
                "trade_quality_gate": TRADE_QUALITY_GATE_WARN,
                "sl_tp_quality": SL_TP_QUALITY_WARN,
                "market_now_calibration": RELAXED_PROFIT_MARKET_NOW,
                "short_now_calibration": {
                    "enabled": True,
                    "min_range_pos": 0.04,
                    "max_range_pos": 0.96,
                    "min_available_room_bps": 10,
                    "max_stop_bps": 900,
                    "max_stop_atr_mult": 6.0,
                    "min_net_rr": 0.1,
                    "allow_if_liquidity_missing": True,
                    "max_spread_bps": 160,
                    "max_slippage_bps": 300,
                    "require_recent_down_impulse": False,
                    "reject_if_rebound_required": False,
                },
            },
            "micro_full": {
                "min_score": 10,
                "require_range_room_ok": False,
                "require_liquidity_ok": False,
                "require_micro_ready": True,
                "require_micro_alignment": True,
                "micro_consumption_policy": "ready_signal_usable",
                "allow_weak_micro_consumption": True,
                "target_rr": 1.0,
                "min_rr": 0.05,
                "min_net_rr": 0.02,
                "min_effective_rr": 0.70,
                "min_reachable_reward_bps": 8,
                "allow_fallback_target_for_executable": False,
                "min_stop_bps": 3,
                "preferred_stop_bps": 25,
                "max_stop_bps": 1300,
                "max_stop_atr_mult": 6.2,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 10,
                "min_profile_hf_stop_score": 0,
                "max_profile_slippage_risk_score": 98,
                "trade_quality_gate": TRADE_QUALITY_GATE_WARN,
                "sl_tp_quality": SL_TP_QUALITY_WARN,
                "market_now_calibration": {
                    **RELAXED_PROFIT_MARKET_NOW,
                    "long": {**RELAXED_PROFIT_MARKET_NOW["long"], "max_stop_bps": 1000, "max_stop_atr_mult": 6.2},
                    "short": {**RELAXED_PROFIT_MARKET_NOW["short"], "max_stop_bps": 1000, "max_stop_atr_mult": 6.2},
                },
                "short_now_calibration": {
                    "enabled": True,
                    "min_range_pos": 0.04,
                    "max_range_pos": 0.96,
                    "min_available_room_bps": 10,
                    "max_stop_bps": 1000,
                    "max_stop_atr_mult": 6.2,
                    "min_net_rr": 0.1,
                    "allow_if_liquidity_missing": True,
                    "max_spread_bps": 160,
                    "max_slippage_bps": 300,
                    "require_recent_down_impulse": False,
                    "reject_if_rebound_required": False,
                },
            },
        },
        "strategy_pipeline": {"micro": {"max_active_symbols": 20, "wait_fast_sec": 300}},
    },
    "balanced_test": {
        "active_profile": "balanced_test",
        "trade_plan_risk": BALANCED_TRADE_PLAN_RISK,
        "paper": {"paper_fallback_notional_allowed": False},
        "market_entry_liquidity": {
            "max_spread_bps": 35,
            "max_estimated_slippage_bps": 80,
            "min_top_depth_usdt": 2000,
            "min_quote_volume_24h": 200000,
        },
        "decision_refresh": {"range_room": {"long_max_range_pos": 0.9, "short_min_range_pos": 0.1}},
        "trade_plan_lines": {
            "without_micro": {
                "min_score": 58,
                "target_rr": 0.65,
                "min_net_rr": 0.7,
                "min_effective_rr": 0.65,
                "min_reachable_reward_bps": 8,
                "allow_fallback_target_for_executable": False,
                "preferred_stop_bps": 18,
                "max_stop_bps": 220,
                "max_stop_atr_mult": 2.8,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 35,
                "min_profile_hf_stop_score": 25,
                "max_profile_slippage_risk_score": 75,
                "trade_quality_gate": TRADE_QUALITY_GATE_SHADOW,
                "sl_tp_quality": SL_TP_QUALITY_SHADOW,
                "tp_target_policy": TP_TARGET_POLICY_BALANCED_WITHOUT,
                "market_now_calibration": BALANCED_MARKET_NOW,
            },
            "micro_fast": {
                "min_score": 53,
                "target_rr": 0.7,
                "min_net_rr": 0.75,
                "min_effective_rr": 0.7,
                "min_reachable_reward_bps": 8,
                "allow_fallback_target_for_executable": False,
                "preferred_stop_bps": 18,
                "max_stop_bps": 220,
                "max_stop_atr_mult": 2.8,
                "micro_consumption_policy": "ready_signal_usable",
                "allow_weak_micro_consumption": True,
                "weak_micro_min_state": "ready",
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 35,
                "min_profile_hf_stop_score": 25,
                "max_profile_slippage_risk_score": 75,
                "trade_quality_gate": TRADE_QUALITY_GATE_SHADOW,
                "sl_tp_quality": SL_TP_QUALITY_SHADOW,
                "tp_target_policy": TP_TARGET_POLICY_BALANCED_MICRO_FAST,
                "market_now_calibration": BALANCED_MARKET_NOW,
            },
            "micro_full": {
                "min_score": 55,
                "target_rr": 0.8,
                "min_net_rr": 0.8,
                "min_effective_rr": 0.8,
                "min_reachable_reward_bps": 12,
                "allow_fallback_target_for_executable": False,
                "max_stop_bps": 320,
                "max_stop_atr_mult": 3.2,
                "micro_consumption_policy": "confirmed_only",
                "allow_weak_micro_consumption": False,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 40,
                "min_profile_hf_stop_score": 30,
                "max_profile_slippage_risk_score": 70,
                "trade_quality_gate": TRADE_QUALITY_GATE_SHADOW,
                "sl_tp_quality": SL_TP_QUALITY_SHADOW,
                "tp_target_policy": TP_TARGET_POLICY_STRUCTURE,
                "market_now_calibration": {
                    **BALANCED_MARKET_NOW,
                    "long": {**BALANCED_MARKET_NOW["long"], "max_stop_bps": 520, "max_stop_atr_mult": 4.2},
                    "short": {**BALANCED_MARKET_NOW["short"], "max_stop_bps": 520, "max_stop_atr_mult": 4.2},
                },
            },
        },
    },
    "production_strict": {
        "active_profile": "production_strict",
        "trade_plan_risk": PRODUCTION_TRADE_PLAN_RISK,
        "paper": {"paper_fallback_notional_allowed": False},
        "market_entry_liquidity": {
            "max_spread_bps": 15,
            "max_estimated_slippage_bps": 30,
            "min_top_depth_usdt": 6000,
            "min_quote_volume_24h": 500000,
        },
        "decision_refresh": {"range_room": {"long_max_range_pos": 0.82, "short_min_range_pos": 0.18}},
        "trade_plan_lines": {
            "without_micro": {
                "min_score": 73,
                "require_range_room_ok": True,
                "require_liquidity_ok": True,
                "target_rr": 0.75,
                "min_net_rr": 0.9,
                "min_effective_rr": 0.85,
                "min_reachable_reward_bps": 18,
                "allow_fallback_target_for_executable": False,
                "max_stop_bps": 180,
                "max_stop_atr_mult": 2.2,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 60,
                "min_profile_hf_stop_score": 40,
                "max_profile_slippage_risk_score": 50,
                "trade_quality_gate": TRADE_QUALITY_GATE_SHADOW,
                "sl_tp_quality": SL_TP_QUALITY_SHADOW,
                "tp_target_policy": TP_TARGET_POLICY_PRODUCTION_WITHOUT,
                "market_now_calibration": PRODUCTION_MARKET_NOW,
            },
            "micro_fast": {
                "min_score": 68,
                "require_range_room_ok": True,
                "require_liquidity_ok": True,
                "require_micro_ready": True,
                "require_micro_alignment": True,
                "micro_consumption_policy": "confirmed_only",
                "allow_weak_micro_consumption": False,
                "target_rr": 0.8,
                "min_net_rr": 0.95,
                "min_effective_rr": 0.9,
                "min_reachable_reward_bps": 18,
                "allow_fallback_target_for_executable": False,
                "max_stop_bps": 160,
                "max_stop_atr_mult": 2.2,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 60,
                "min_profile_hf_stop_score": 40,
                "max_profile_slippage_risk_score": 50,
                "trade_quality_gate": TRADE_QUALITY_GATE_SHADOW,
                "sl_tp_quality": SL_TP_QUALITY_SHADOW,
                "tp_target_policy": TP_TARGET_POLICY_PRODUCTION_MICRO_FAST,
                "market_now_calibration": PRODUCTION_MARKET_NOW,
            },
            "micro_full": {
                "min_score": 70,
                "require_range_room_ok": True,
                "require_liquidity_ok": True,
                "require_micro_ready": True,
                "require_micro_alignment": True,
                "micro_consumption_policy": "confirmed_only",
                "allow_weak_micro_consumption": False,
                "min_net_rr": 1.3,
                "min_effective_rr": 1.20,
                "min_reachable_reward_bps": 18,
                "allow_fallback_target_for_executable": False,
                "max_stop_bps": 240,
                "max_stop_atr_mult": 2.5,
                "profile_gate_enabled": True,
                "min_profile_market_entry_score": 60,
                "min_profile_hf_stop_score": 45,
                "max_profile_slippage_risk_score": 50,
                "trade_quality_gate": TRADE_QUALITY_GATE_SHADOW,
                "sl_tp_quality": SL_TP_QUALITY_SHADOW,
                "tp_target_policy": TP_TARGET_POLICY_STRUCTURE,
                "market_now_calibration": {
                    **PRODUCTION_MARKET_NOW,
                    "long": {**PRODUCTION_MARKET_NOW["long"], "max_stop_bps": 520, "max_stop_atr_mult": 3.8},
                    "short": {**PRODUCTION_MARKET_NOW["short"], "max_stop_bps": 520, "max_stop_atr_mult": 3.8},
                },
            },
        },
    },
}


class ApiServiceError(Exception):
    def __init__(self, code: str, message: str, detail: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}


def load_yaml_config() -> dict[str, Any]:
    try:
        return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ApiServiceError("config_invalid", "default.yaml is not valid YAML", {"error": str(exc)}) from exc


def _atomic_write_yaml(path: Path, data: dict[str, Any]) -> None:
    payload = yaml.safe_dump(data, allow_unicode=True, sort_keys=False).encode("utf-8")
    write_file_atomic(path, payload)


def _mask_value(value: Any) -> Any:
    if not value:
        return ""
    text = str(value)
    if len(text) <= 8:
        return "****"
    return f"{text[:6]}***{text[-4:]}"


def mask_config(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: (_mask_value(v) if k in SECRET_KEYS else mask_config(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [mask_config(x) for x in obj]
    return obj


def _config_field_impact_map() -> dict[str, Any]:
    wrapped = read_json_file(CONFIG_FIELD_IMPACT_MAP_PATH)
    data = wrapped.get("data") if isinstance(wrapped, dict) else wrapped
    if not data:
        raise ApiServiceError(
            "config_field_impact_not_ready",
            "Config field impact map is missing; run STEP7.140 audit first.",
            {"path": str(CONFIG_FIELD_IMPACT_MAP_PATH)},
        )
    fields = data.get("fields")
    if not isinstance(fields, list):
        raise ApiServiceError(
            "config_field_impact_invalid",
            "Config field impact map has no fields list.",
            {"path": str(CONFIG_FIELD_IMPACT_MAP_PATH)},
        )
    return data


def _config_field_impact_summary() -> dict[str, Any]:
    wrapped = read_json_file(CONFIG_FIELD_IMPACT_SUMMARY_PATH) if CONFIG_FIELD_IMPACT_SUMMARY_PATH.exists() else {}
    summary = wrapped.get("data") if isinstance(wrapped, dict) else wrapped
    if summary:
        return summary
    data = _config_field_impact_map()
    fields = data.get("fields") or []
    status_counts = Counter(str(row.get("status") or "unknown") for row in fields if isinstance(row, dict))
    ui_counts = Counter(str(row.get("ui_recommendation") or "unknown") for row in fields if isinstance(row, dict))
    return {
        "schema_version": "7.140-config-used-by-summary-v1",
        "generated_at": data.get("generated_at"),
        "field_count": len(fields),
        "status_counts": dict(status_counts),
        "ui_recommendation_counts": dict(ui_counts),
        "source": "derived_from_map",
    }


def config_field_impact_map_payload() -> dict[str, Any]:
    data = _config_field_impact_map()
    return {
        "schema_version": data.get("schema_version", "7.140-config-used-by-map-v1"),
        "generated_at": data.get("generated_at"),
        "source_path": str(CONFIG_FIELD_IMPACT_MAP_PATH),
        "summary": _config_field_impact_summary(),
        "fields": data.get("fields") or [],
    }


def config_field_impact_summary_payload() -> dict[str, Any]:
    summary = _config_field_impact_summary()
    return {
        **summary,
        "source_path": str(CONFIG_FIELD_IMPACT_SUMMARY_PATH if CONFIG_FIELD_IMPACT_SUMMARY_PATH.exists() else CONFIG_FIELD_IMPACT_MAP_PATH),
    }


def _field_rows() -> list[dict[str, Any]]:
    return [row for row in (_config_field_impact_map().get("fields") or []) if isinstance(row, dict)]


def _minimal_profile_cleanup_items() -> dict[str, dict[str, Any]]:
    wrapped = read_optional_json_file(MINIMAL_PROFILE_CLEANUP_PLAN_PATH)
    data = wrapped.get("data") if isinstance(wrapped, dict) else {}
    if not isinstance(data, dict) or data.get("status") != "ok":
        return {}
    items = data.get("cleanup_items") or []
    return {
        str(item.get("field_path")): item
        for item in items
        if isinstance(item, dict) and item.get("field_path")
    }


def _minimal_profile_cleanup_summary(cleanup_items: dict[str, dict[str, Any]]) -> dict[str, Any]:
    by_action = Counter(str(item.get("cleanup_action") or "unknown") for item in cleanup_items.values())
    by_group = Counter(str(item.get("cleanup_group") or "unknown") for item in cleanup_items.values())
    return {
        "schema_version": "step26.6-minimal-profile-hidden-fields-v1",
        "source_path": str(MINIMAL_PROFILE_CLEANUP_PLAN_PATH),
        "field_count": len(cleanup_items),
        "by_action": dict(by_action),
        "by_group": dict(by_group),
        "default_yaml_changed": False,
        "raw_effective_config_preserved": True,
    }


def _attach_minimal_profile_cleanup(row: dict[str, Any], cleanup_items: dict[str, dict[str, Any]]) -> dict[str, Any]:
    item = cleanup_items.get(str(row.get("field_path") or ""))
    if not item:
        return row
    enriched = dict(row)
    cleanup_action = str(item.get("cleanup_action") or "")
    enriched["minimal_profile_cleanup_action"] = cleanup_action
    enriched["minimal_profile_cleanup_group"] = item.get("cleanup_group")
    enriched["minimal_profile_hidden"] = cleanup_action in {
        "profile_exclude_only",
        "ui_hide_only",
        "default_cleanup_candidate",
    }
    enriched["minimal_profile_hidden_reason"] = item.get("application_note") or item.get("required_validation") or ""
    enriched["default_yaml_action"] = item.get("default_yaml_action")
    return enriched


def _field_rows_with_minimal_profile_cleanup() -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    cleanup_items = _minimal_profile_cleanup_items()
    rows = [_attach_minimal_profile_cleanup(row, cleanup_items) for row in _field_rows()]
    return rows, cleanup_items


def _compact_config_impact_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "field_path": row.get("field_path"),
        "section": row.get("section"),
        "status": row.get("status"),
        "used_by_strategies": row.get("used_by_strategies") or [],
        "effective_for_strategies": row.get("effective_for_strategies") or [],
        "inherits_from": row.get("inherits_from"),
        "business_stages": row.get("business_stages") or [],
        "direct_executable_impact": bool(row.get("direct_executable_impact")),
        "paper_impact": bool(row.get("paper_impact")),
        "backtest_impact": bool(row.get("backtest_impact")),
        "sandbox_impact": bool(row.get("sandbox_impact")),
        "ui_recommendation": row.get("ui_recommendation"),
        "minimal_profile_hidden": bool(row.get("minimal_profile_hidden")),
        "minimal_profile_cleanup_action": row.get("minimal_profile_cleanup_action"),
        "minimal_profile_cleanup_group": row.get("minimal_profile_cleanup_group"),
        "minimal_profile_hidden_reason": row.get("minimal_profile_hidden_reason") or "",
        "default_yaml_action": row.get("default_yaml_action"),
        "notes": row.get("notes") or "",
    }


def config_legacy_fields_payload() -> dict[str, Any]:
    rows, _cleanup_items = _field_rows_with_minimal_profile_cleanup()
    rows = [
        row
        for row in rows
        if row.get("status") in {"legacy", "disabled"} or row.get("ui_recommendation") == "hide_legacy"
    ]
    return {
        "count": len(rows),
        "fields": [_compact_config_impact_row(row) for row in rows],
    }


def config_ui_schema_payload() -> dict[str, Any]:
    rows, cleanup_items = _field_rows_with_minimal_profile_cleanup()
    groups: dict[str, list[dict[str, Any]]] = {
        "primary": [],
        "compact": [],
        "advanced": [],
        "hide_legacy": [],
        "minimal_hidden": [],
    }
    for row in rows:
        key = str(row.get("ui_recommendation") or "advanced")
        if row.get("minimal_profile_hidden"):
            groups["minimal_hidden"].append(row)
            if key == "advanced":
                continue
        if key not in groups:
            key = "advanced"
        groups[key].append(row)
    tabs = [
        {
            "key": "strategy-runtime",
            "label": "Strategy Runtime",
            "field_prefixes": ["strategy_pipeline.", "project_runtime.", "paper.daemon."],
        },
        {
            "key": "entry-executable",
            "label": "Entry & Executable",
            "field_prefixes": ["market_entry_", "decision_refresh.", "trade_plan_lines."],
        },
        {
            "key": "exit-rr",
            "label": "Exit / RR",
            "field_contains": ["tp_target_policy", "min_effective_rr", "min_net_rr", "target_rr"],
        },
        {
            "key": "trade-gate",
            "label": "Trade Gate",
            "field_contains": ["trade_quality_gate", "sl_tp_quality", "v5", "gate"],
        },
        {
            "key": "advanced-legacy",
            "label": "Advanced / Legacy",
            "ui_recommendations": ["advanced", "hide_legacy"],
        },
    ]
    return {
        "schema_version": "26.6-config-ui-schema-minimal-hidden-v1",
        "generated_at": _config_field_impact_summary().get("generated_at"),
        "minimal_profile_cleanup": _minimal_profile_cleanup_summary(cleanup_items),
        "groups": {
            key: {"count": len(value), "fields": [_compact_config_impact_row(row) for row in value[:200]]}
            for key, value in groups.items()
        },
        "tabs": tabs,
    }


def config_effective_payload(strategy_line: str) -> dict[str, Any]:
    if strategy_line not in STRATEGY_LINES_ORDERED:
        raise ApiServiceError(
            "invalid_strategy_line",
            "strategy_line must be one of the known strategy lines",
            {"strategy_line": strategy_line, "allowed": list(STRATEGY_LINES_ORDERED)},
        )
    field_rows, cleanup_items = _field_rows_with_minimal_profile_cleanup()
    rows = [
        row
        for row in field_rows
        if strategy_line in (row.get("effective_for_strategies") or [])
        or strategy_line in (row.get("used_by_strategies") or [])
    ]
    direct_rows = [row for row in rows if row.get("direct_executable_impact")]
    paper_rows = [row for row in rows if row.get("paper_impact") and not row.get("direct_executable_impact")]
    backtest_rows = [row for row in rows if row.get("backtest_impact")]
    legacy_rows = [
        row
        for row in rows
        if row.get("status") in {"legacy", "disabled"} or row.get("ui_recommendation") == "hide_legacy"
    ]
    minimal_hidden_rows = [row for row in rows if row.get("minimal_profile_hidden")]
    row_limit = 160

    def cap(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [_compact_config_impact_row(row) for row in items[:row_limit]]

    inherits_base = strategy_line in {"strategy4", "strategy5", "strategy6"}
    live_notes: list[str] = []
    if inherits_base:
        live_notes.append(
            f"{strategy_line} live executable lineage can inherit the without_micro base trade plan; verify direct adapter use before treating line-only fields as live-effective."
        )
    if strategy_line in {"strategy5", "strategy6"}:
        live_notes.append("V5 trade gate is a paper pre-order gate, not a trade-plan executable generator.")
    return {
        "strategy_line": strategy_line,
        "generated_at": _config_field_impact_summary().get("generated_at"),
        "inherits_from": "without_micro" if inherits_base else None,
        "live_executable_source": {
            "base_plan": "without_micro" if inherits_base else strategy_line,
            "evidence_overlay": strategy_line if strategy_line in {"strategy5", "strategy6"} else None,
            "paper_gate": "V5 gate" if strategy_line in {"strategy5", "strategy6"} else "paper config",
        },
        "counts": {
            "fields": len(rows),
            "direct_executable": len(direct_rows),
            "paper_only": len(paper_rows),
            "backtest": len(backtest_rows),
            "legacy_or_disabled": len(legacy_rows),
            "minimal_profile_hidden": len(minimal_hidden_rows),
        },
        "field_limit": row_limit,
        "minimal_profile_cleanup": _minimal_profile_cleanup_summary(cleanup_items),
        "notes": live_notes,
        "fields": cap(rows),
        "direct_executable_fields": cap(direct_rows),
        "paper_only_fields": cap(paper_rows),
        "backtest_fields": cap(backtest_rows),
        "legacy_fields": cap(legacy_rows),
        "minimal_profile_hidden_fields": cap(minimal_hidden_rows),
    }


def _is_bool_key(key: str) -> bool:
    prefixes = ("allow_", "require_", "auto_", "restart_", "notify_", "include_", "scan_", "block_")
    suffixes = ("_enabled", "_ok")
    return key == "enabled" or key.startswith(prefixes) or key.endswith(suffixes)


def _validate_scalar(path: tuple[str, ...], value: Any) -> None:
    key = path[-1]
    if (
        len(path) >= 2
        and path[-2] == "tp_target_policy"
        and key in {"target_rr", "target_rr_cap", "target_net_rr"}
        and value is None
    ):
        return
    if path in NUMERIC_CONFIG_PATHS and not isinstance(value, (int, float)):
        raise ApiServiceError("config_invalid", "Config value must be numeric", {"path": ".".join(path)})
    if _is_bool_key(key) and not isinstance(value, bool):
        raise ApiServiceError("config_invalid", "Config value must be boolean", {"path": ".".join(path)})
    if key in {"run_lines", "notify_lines", "lines"} and not isinstance(value, list):
        raise ApiServiceError("config_invalid", "Config value must be a list", {"path": ".".join(path)})


def _validate_trade_plan_line(line: str, merged: dict[str, Any]) -> None:
    score = merged.get("min_score")
    if score is not None and not 0 <= float(score) <= 100:
        raise ApiServiceError("config_invalid", "trade_plan_lines min_score must be 0-100", {"line": line})
    stop_min = merged.get("min_stop_bps")
    stop_pref = merged.get("preferred_stop_bps")
    stop_max = merged.get("max_stop_bps")
    if stop_min is not None and stop_max is not None and float(stop_min) > float(stop_max):
        raise ApiServiceError("config_invalid", "min_stop_bps cannot exceed max_stop_bps", {"line": line})
    if stop_pref is not None and stop_max is not None and float(stop_pref) > float(stop_max):
        raise ApiServiceError("config_invalid", "preferred_stop_bps cannot exceed max_stop_bps", {"line": line})
    policy = merged.get("micro_consumption_policy")
    if policy is not None and policy not in {"confirmed_only", "ready_signal_usable", "weak_ready_test", "audit_only"}:
        raise ApiServiceError("config_invalid", "Unsupported micro_consumption_policy", {"line": line, "policy": policy})
    min_state = merged.get("weak_micro_min_state")
    if min_state is not None and min_state not in {"ready", "signal_usable"}:
        raise ApiServiceError("config_invalid", "Unsupported weak_micro_min_state", {"line": line, "state": min_state})
    for profile_key in {
        "min_profile_market_entry_score",
        "min_profile_hf_stop_score",
        "max_profile_slippage_risk_score",
    }:
        value = merged.get(profile_key)
        if value is not None and not 0 <= float(value) <= 100:
            raise ApiServiceError(
                "config_invalid",
                "Profile gate score must be 0-100",
                {"line": line, "key": profile_key},
            )
    block_reasons = merged.get("weak_micro_block_reasons")
    if block_reasons is not None:
        if not isinstance(block_reasons, list) or any(not isinstance(v, str) for v in block_reasons):
            raise ApiServiceError(
                "config_invalid",
                "weak_micro_block_reasons must be a list of strings",
                {"line": line},
            )
    tp_policy = merged.get("tp_target_policy")
    if tp_policy is not None:
        if not isinstance(tp_policy, dict):
            raise ApiServiceError("config_invalid", "tp_target_policy must be an object", {"line": line})
        unknown = sorted(set(tp_policy) - TP_TARGET_POLICY_ALLOWED_KEYS)
        if unknown:
            raise ApiServiceError(
                "config_invalid",
                "tp_target_policy contains non-whitelisted keys",
                {"line": line, "keys": unknown},
            )
        mode = tp_policy.get("mode", "structure")
        if mode not in TP_TARGET_POLICY_MODES:
            raise ApiServiceError("config_invalid", "Unsupported tp_target_policy mode", {"line": line, "mode": mode})
        target_rr = tp_policy.get("target_rr")
        target_rr_cap = tp_policy.get("target_rr_cap")
        target_rr_basis = str(tp_policy.get("target_rr_basis", "gross"))
        target_net_rr = tp_policy.get("target_net_rr")
        if mode != "structure":
            if target_rr_basis not in {"gross", "net"}:
                raise ApiServiceError(
                    "config_invalid",
                    "tp_target_policy target_rr_basis must be gross or net",
                    {"line": line, "target_rr_basis": target_rr_basis},
                )
            if target_rr is None or target_rr_cap is None:
                raise ApiServiceError(
                    "config_invalid",
                    "fast TP policy requires target_rr and target_rr_cap",
                    {"line": line},
                )
            if float(target_rr) <= 0 or float(target_rr_cap) <= 0:
                raise ApiServiceError(
                    "config_invalid",
                    "fast TP target_rr values must be positive",
                    {"line": line},
                )
            if float(target_rr_cap) < float(target_rr):
                raise ApiServiceError(
                    "config_invalid",
                    "tp_target_policy target_rr_cap cannot be below target_rr",
                    {"line": line},
                )
            if target_rr_basis == "net":
                effective_target_net_rr = float(target_net_rr if target_net_rr is not None else target_rr)
                if effective_target_net_rr <= 0:
                    raise ApiServiceError(
                        "config_invalid",
                        "tp_target_policy target_net_rr must be positive",
                        {"line": line},
                    )
                if float(target_rr_cap) < effective_target_net_rr:
                    raise ApiServiceError(
                        "config_invalid",
                        "tp_target_policy target_rr_cap cannot be below target_net_rr when basis is net",
                        {"line": line},
                    )
        for num_key in {
            "min_reward_bps",
            "market_room_buffer_bps",
            "reward_to_spread_min",
            "target_net_rr",
            "min_target_net_rr",
            "max_target_net_rr",
            "slippage_reserve_bps",
            "max_loss_net_r",
        }:
            if num_key in tp_policy and tp_policy[num_key] is not None and float(tp_policy[num_key]) < 0:
                raise ApiServiceError(
                    "config_invalid",
                    "tp_target_policy numeric thresholds must be non-negative",
                    {"line": line, "key": num_key},
                )
        if (
            "min_target_net_rr" in tp_policy
            and "max_target_net_rr" in tp_policy
            and float(tp_policy["max_target_net_rr"]) < float(tp_policy["min_target_net_rr"])
        ):
            raise ApiServiceError(
                "config_invalid",
                "tp_target_policy max_target_net_rr cannot be below min_target_net_rr",
                {"line": line},
            )
        sizing_basis = tp_policy.get("sizing_basis")
        if sizing_basis is not None and sizing_basis not in {"gross_stop", "net_planned_loss"}:
            raise ApiServiceError(
                "config_invalid",
                "tp_target_policy sizing_basis must be gross_stop or net_planned_loss",
                {"line": line, "sizing_basis": sizing_basis},
            )
        for bool_key in {
            "require_market_room",
            "allow_structure_runner",
            "include_entry_fee",
            "include_exit_fee",
            "include_slippage_reserve",
        }:
            if bool_key in tp_policy and not isinstance(tp_policy[bool_key], bool):
                raise ApiServiceError(
                    "config_invalid",
                    "tp_target_policy boolean field must be boolean",
                    {"line": line, "key": bool_key},
                )
    tq_gate = merged.get("trade_quality_gate")
    if tq_gate is not None:
        if not isinstance(tq_gate, dict):
            raise ApiServiceError("config_invalid", "trade_quality_gate must be an object", {"line": line})
        unknown = sorted(set(tq_gate) - TRADE_QUALITY_GATE_ALLOWED_KEYS)
        if unknown:
            raise ApiServiceError(
                "config_invalid",
                "trade_quality_gate contains non-whitelisted keys",
                {"line": line, "keys": unknown},
            )
        mode = tq_gate.get("mode")
        if mode is not None and mode not in {"off", "shadow", "warn", "wait_only", "block_executable"}:
            raise ApiServiceError("config_invalid", "Unsupported trade_quality_gate mode", {"line": line, "mode": mode})
        for num_key in {"min_samples_per_symbol", "min_samples_per_root_cause"}:
            if num_key in tq_gate and int(tq_gate[num_key]) < 0:
                raise ApiServiceError(
                    "config_invalid",
                    "trade_quality_gate sample thresholds must be non-negative",
                    {"line": line, "key": num_key},
                )
        for bool_key in {"enabled", "signal_no_edge_wait_enabled", "side_specific_enabled"}:
            if bool_key in tq_gate and not isinstance(tq_gate[bool_key], bool):
                raise ApiServiceError(
                    "config_invalid",
                    "trade_quality_gate boolean field must be boolean",
                    {"line": line, "key": bool_key},
                )
    sltp_quality = merged.get("sl_tp_quality")
    if sltp_quality is not None:
        if not isinstance(sltp_quality, dict):
            raise ApiServiceError("config_invalid", "sl_tp_quality must be an object", {"line": line})
        unknown = sorted(set(sltp_quality) - SL_TP_QUALITY_ALLOWED_KEYS)
        if unknown:
            raise ApiServiceError(
                "config_invalid",
                "sl_tp_quality contains non-whitelisted keys",
                {"line": line, "keys": unknown},
            )
        mode = sltp_quality.get("mode")
        if mode is not None and mode not in {"off", "shadow", "warn", "apply"}:
            raise ApiServiceError("config_invalid", "Unsupported sl_tp_quality mode", {"line": line, "mode": mode})
        if "min_samples_per_cluster" in sltp_quality and int(sltp_quality["min_samples_per_cluster"]) < 0:
            raise ApiServiceError(
                "config_invalid",
                "sl_tp_quality min_samples_per_cluster must be non-negative",
                {"line": line},
            )
        if "stop_too_tight_widen_factor" in sltp_quality and float(sltp_quality["stop_too_tight_widen_factor"]) < 1.0:
            raise ApiServiceError(
                "config_invalid",
                "sl_tp_quality stop_too_tight_widen_factor must be >= 1",
                {"line": line},
            )
        if "tp_too_far_reduce_factor" in sltp_quality and not 0 < float(sltp_quality["tp_too_far_reduce_factor"]) <= 1.0:
            raise ApiServiceError(
                "config_invalid",
                "sl_tp_quality tp_too_far_reduce_factor must be in (0, 1]",
                {"line": line},
            )
        for bool_key in {"enabled", "single_tp_only", "entered_too_early_wait_enabled"}:
            if bool_key in sltp_quality and not isinstance(sltp_quality[bool_key], bool):
                raise ApiServiceError(
                    "config_invalid",
                    "sl_tp_quality boolean field must be boolean",
                    {"line": line, "key": bool_key},
                )
    short_now = merged.get("short_now_calibration")
    if short_now is not None:
        if not isinstance(short_now, dict):
            raise ApiServiceError("config_invalid", "short_now_calibration must be an object", {"line": line})
        allowed = {
            "enabled",
            "min_range_pos",
            "max_range_pos",
            "min_available_room_bps",
            "max_stop_bps",
            "max_stop_atr_mult",
            "min_net_rr",
            "allow_if_liquidity_missing",
            "max_spread_bps",
            "max_slippage_bps",
            "require_recent_down_impulse",
            "reject_if_rebound_required",
        }
        unknown = sorted(set(short_now) - allowed)
        if unknown:
            raise ApiServiceError(
                "config_invalid",
                "short_now_calibration contains non-whitelisted keys",
                {"line": line, "keys": unknown},
            )
        min_range = short_now.get("min_range_pos")
        max_range = short_now.get("max_range_pos")
        if min_range is not None and max_range is not None and float(min_range) > float(max_range):
            raise ApiServiceError(
                "config_invalid",
                "short_now_calibration min_range_pos cannot exceed max_range_pos",
                {"line": line},
            )
    market_now = merged.get("market_now_calibration")
    if market_now is not None:
        if not isinstance(market_now, dict):
            raise ApiServiceError("config_invalid", "market_now_calibration must be an object", {"line": line})
        unknown = sorted(set(market_now) - MARKET_NOW_ALLOWED_KEYS)
        if unknown:
            raise ApiServiceError(
                "config_invalid",
                "market_now_calibration contains non-whitelisted keys",
                {"line": line, "keys": unknown},
            )
        for bool_key in ("enabled", "legacy_short_now_fallback"):
            if bool_key in market_now and not isinstance(market_now[bool_key], bool):
                raise ApiServiceError(
                    "config_invalid",
                    "market_now_calibration boolean field must be boolean",
                    {"line": line, "key": bool_key},
                )
        for side in ("long", "short"):
            side_cfg = market_now.get(side)
            if side_cfg is None:
                continue
            if not isinstance(side_cfg, dict):
                raise ApiServiceError(
                    "config_invalid",
                    "market_now_calibration side config must be an object",
                    {"line": line, "side": side},
                )
            side_unknown = sorted(set(side_cfg) - MARKET_NOW_SIDE_ALLOWED_KEYS)
            if side_unknown:
                raise ApiServiceError(
                    "config_invalid",
                    "market_now_calibration side contains non-whitelisted keys",
                    {"line": line, "side": side, "keys": side_unknown},
                )
            min_range = side_cfg.get("min_range_pos")
            max_range = side_cfg.get("max_range_pos")
            if min_range is not None and max_range is not None and float(min_range) > float(max_range):
                raise ApiServiceError(
                    "config_invalid",
                    "market_now_calibration min_range_pos cannot exceed max_range_pos",
                    {"line": line, "side": side},
                )
            for bool_key in {
                "allow_if_liquidity_missing",
                "require_recent_up_impulse",
                "reject_if_pullback_required",
                "require_recent_down_impulse",
                "reject_if_rebound_required",
            }:
                if bool_key in side_cfg and not isinstance(side_cfg[bool_key], bool):
                    raise ApiServiceError(
                        "config_invalid",
                        "market_now_calibration side boolean field must be boolean",
                        {"line": line, "side": side, "key": bool_key},
                    )


def _validate_section_values(section: str, values: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    allowed = ALLOWED_CONFIG_KEYS[section]
    unknown = sorted(set(values) - allowed)
    if unknown:
        raise ApiServiceError("config_invalid", "Config update contains non-whitelisted keys", {"keys": unknown})

    merged = dict(current)
    nested_allowed = NESTED_CONFIG_KEYS.get(section, {})
    for key, value in values.items():
        path = (section, key)
        if isinstance(value, dict):
            if key not in nested_allowed:
                raise ApiServiceError("config_invalid", "Nested config update is not allowed", {"path": ".".join(path)})
            child_allowed = nested_allowed[key]
            child_unknown = sorted(set(value) - child_allowed)
            if child_unknown:
                raise ApiServiceError(
                    "config_invalid",
                    "Nested config update contains non-whitelisted keys",
                    {"path": ".".join(path), "keys": child_unknown},
                )
            child_current = merged.get(key) if isinstance(merged.get(key), dict) else {}
            child_merged = {**child_current, **value}
            for child_key, child_value in value.items():
                _validate_scalar((*path, child_key), child_value)
            if section == "trade_plan_lines" and key in STRATEGY_LINE_KEYS:
                _validate_trade_plan_line(key, child_merged)
            merged[key] = child_merged
        else:
            _validate_scalar(path, value)
            merged[key] = value

    if section == "paper":
        db_path = str(merged.get("db_path", ""))
        if Path(db_path).is_absolute() or ".." in Path(db_path).parts:
            raise ApiServiceError("config_invalid", "paper.db_path must be project-relative", {"db_path": db_path})
    if section == "market_entry_liquidity":
        if float(merged.get("margin_usdt", 1)) <= 0 or float(merged.get("leverage", 1)) <= 0:
            raise ApiServiceError("config_invalid", "liquidity margin/leverage must be positive", {})
    return merged


def validate_config_section(section_alias: str, values: dict[str, Any]) -> dict[str, Any]:
    section = CONFIG_SECTION_ALIASES.get(section_alias)
    if not section:
        raise ApiServiceError("config_invalid", "Unsupported config section", {"section": section_alias})
    cfg = load_yaml_config()
    current = cfg.get(section) or {}
    if not isinstance(current, dict):
        raise ApiServiceError("config_invalid", "Config section is not an object", {"section": section})
    merged = _validate_section_values(section, values, current)
    return {"section": section, "valid": True, "merged": mask_config(merged)}


def update_config_section(section_alias: str, values: dict[str, Any]) -> dict[str, Any]:
    section = CONFIG_SECTION_ALIASES.get(section_alias)
    if not section:
        raise ApiServiceError("config_invalid", "Unsupported config section", {"section": section_alias})
    cfg = load_yaml_config()
    current = cfg.get(section) or {}
    if not isinstance(current, dict):
        raise ApiServiceError("config_invalid", "Config section is not an object", {"section": section})
    merged = _validate_section_values(section, values, current)
    cfg[section] = merged
    cfg["active_profile"] = "custom"
    _atomic_write_yaml(CONFIG_PATH, cfg)
    return mask_config(merged)


def config_profiles() -> dict[str, Any]:
    cfg = load_yaml_config()
    return {
        "active_profile": cfg.get("active_profile", "custom"),
        "profiles": [
            {"name": name, "sections": sorted(set(profile) - {"active_profile"})}
            for name, profile in CONFIG_PROFILES.items()
        ],
    }


def apply_config_profile(profile_name: str) -> dict[str, Any]:
    profile = CONFIG_PROFILES.get(profile_name)
    if not profile:
        raise ApiServiceError("config_invalid", "Unsupported config profile", {"profile": profile_name})
    cfg = load_yaml_config()
    for section, values in profile.items():
        if section == "active_profile":
            continue
        current = cfg.get(section) or {}
        if not isinstance(current, dict):
            raise ApiServiceError("config_invalid", "Config section is not an object", {"section": section})
        cfg[section] = _validate_section_values(section, values, current)
    cfg["active_profile"] = profile_name
    _atomic_write_yaml(CONFIG_PATH, cfg)
    return {"active_profile": profile_name, "config": mask_config(cfg)}


def reload_config() -> dict[str, Any]:
    return {"status": "loaded", "config": mask_config(load_yaml_config())}


def read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ApiServiceError("file_missing", "JSON file does not exist", {"path": str(path)})
    try:
        data = read_json_object(path)
    except json.JSONDecodeError as exc:
        raise ApiServiceError("json_invalid", "JSON file is invalid", {"path": str(path)}) from exc
    generated_at = data.get("generated_at") if isinstance(data, dict) else None
    return {"data": data, "source_path": str(path), "generated_at": generated_at}


def read_optional_json_file(path: Path) -> dict[str, Any] | None:
    try:
        return read_json_file(path)
    except ApiServiceError as exc:
        if exc.code == "file_missing":
            return None
        raise


def trade_plans_payload() -> dict[str, Any]:
    items: dict[str, Any] = {}
    latest_report = _read_optional_raw_json(CURRENT_JSON_PATHS["latest_strategy"])
    display_run_id = latest_report.get("run_id") if isinstance(latest_report, dict) else None
    display_cycle_id = latest_report.get("cycle_id") if isinstance(latest_report, dict) else None
    for line, key in (
        ("without_micro", "trade_plan_without_micro"),
        ("micro_fast", "trade_plan_micro_fast"),
        ("micro_full", "trade_plan_micro_full"),
        ("strategy4", "trade_plan_strategy4"),
        ("strategy5", "trade_plan_strategy5"),
        ("strategy6", "trade_plan_strategy6"),
    ):
        path = CURRENT_JSON_PATHS.get(key)
        got = read_optional_json_file(path) if path is not None else None
        doc = got["data"] if got else None
        if isinstance(doc, dict):
            output_run_id = doc.get("run_id")
            output_cycle_id = doc.get("cycle_id")
            output_fresh = bool(
                display_run_id
                and display_cycle_id
                and output_run_id == display_run_id
                and output_cycle_id == display_cycle_id
            )
            doc = dict(doc)
            doc["display_run_id"] = display_run_id
            doc["display_cycle_id"] = display_cycle_id
            doc["output_run_id"] = output_run_id
            doc["output_cycle_id"] = output_cycle_id
            doc["output_fresh"] = output_fresh
            doc["stale_output_reason"] = "" if output_fresh else "output_run_id_mismatch"
            doc["effective_executable_count"] = int(doc.get("executable_count") or 0) if output_fresh else 0
            doc["plans_for_current_run"] = doc.get("plans") if output_fresh and isinstance(doc.get("plans"), list) else []
        items[line] = doc
    return {"lines": items, "display_run_id": display_run_id, "display_cycle_id": display_cycle_id}


def _summary_json_lite(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, str) or not raw:
        return {}
    if len(raw) > 100_000:
        return {"omitted": True, "reason": "summary_json_too_large", "bytes": len(raw)}
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {"omitted": True, "reason": "summary_json_invalid"}
    if not isinstance(data, dict):
        return {}
    counts = data.get("counts") if isinstance(data.get("counts"), dict) else {}
    strategy_line_status = data.get("strategy_line_status") if isinstance(data.get("strategy_line_status"), dict) else {}
    if not strategy_line_status:
        lines = data.get("strategy_lines")
        if isinstance(lines, dict):
            strategy_line_status = {
                str(line): (payload.get("status") if isinstance(payload, dict) else payload)
                for line, payload in lines.items()
            }
        elif isinstance(lines, list):
            strategy_line_status = {
                str(item.get("line") or item.get("strategy_line") or idx): item.get("status")
                for idx, item in enumerate(lines)
                if isinstance(item, dict)
            }
    return {
        "counts": counts,
        "strategy_line_status": strategy_line_status,
        "warning_count": data.get("warning_count"),
        "failure_count": data.get("failure_count"),
    }


def run_audit_list_lite(*, limit: int = 20, status: str | None = None) -> dict[str, Any]:
    limit = max(1, min(int(limit or 20), 100))
    if status and status not in {"ok", "warning", "failed"}:
        raise ApiServiceError("status_invalid", "unsupported audit status filter", {"status": status})
    db = PROJECT_ROOT / "DATA/audit/run_audit.db"
    if not db.exists():
        doc = _read_optional_raw_json(PROJECT_ROOT / "DATA/reports/latest_run_audit.json") or {}
        row = {
            "run_id": doc.get("run_id"),
            "cycle_id": doc.get("cycle_id"),
            "status": doc.get("status"),
            "generated_at": doc.get("generated_at"),
            "failure_count": doc.get("failure_count"),
            "warning_count": doc.get("warning_count"),
            "summary_counts": {},
            "strategy_line_status": {},
            "detail_endpoint": f"/api/audit/runs/{doc.get('run_id')}" if doc.get("run_id") else "/api/audit/runs/latest",
        }
        return {"source": "json_fallback_lite", "runs": [row] if row["run_id"] else []}
    query = (
        "select run_id, cycle_id, status, generated_at, failure_count, warning_count, summary_json "
        "from audit_runs "
    )
    params: list[Any] = []
    if status:
        query += "where status = ? "
        params.append(status)
    query += "order by generated_at desc limit ?"
    params.append(limit)
    rows: list[dict[str, Any]] = []
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(query, params).fetchall():
            item = dict(row)
            summary = _summary_json_lite(item.pop("summary_json", None))
            item["summary_counts"] = summary.get("counts") or {}
            item["strategy_line_status"] = summary.get("strategy_line_status") or {}
            item["summary_omitted"] = bool(summary.get("omitted"))
            item["detail_endpoint"] = f"/api/audit/runs/{item.get('run_id')}"
            rows.append(item)
    return {"source": "sqlite_lite", "db_path": str(db), "runs": rows, "count": len(rows)}


def run_audit_latest_lite() -> dict[str, Any]:
    rows = run_audit_list_lite(limit=1).get("runs") or []
    if not rows:
        raise ApiServiceError("file_missing", "run audit does not exist", {"path": "DATA/reports/latest_run_audit.json"})
    latest = dict(rows[0])
    latest["detail_endpoint"] = f"/api/audit/runs/{latest.get('run_id')}" if latest.get("run_id") else "/api/audit/runs/latest"
    return latest


def strategy4_observe_pool_payload() -> dict[str, Any]:
    from laoma_signal_engine.strategy4.observe import pool_path

    got = read_optional_json_file(pool_path(PROJECT_ROOT))
    return got["data"] if got else {"schema_version": "17.1", "source": "strategy4_observe_pool", "count": 0, "items": []}


def strategy4_runtime_payload() -> dict[str, Any]:
    from laoma_signal_engine.strategy4.observe import runtime_status

    return runtime_status(PROJECT_ROOT)


def strategy4_attempts_payload(limit: int = 200) -> dict[str, Any]:
    import json
    import sqlite3

    from laoma_signal_engine.strategy4.observe import db_path

    db = db_path(PROJECT_ROOT)
    if not db.is_file():
        return {"schema_version": "17.6", "source": "strategy4_attempts", "count": 0, "items": []}
    rows: list[dict[str, Any]] = []
    with sqlite3.connect(db) as con:
        con.row_factory = sqlite3.Row
        for row in con.execute(
            "SELECT * FROM strategy4_attempts ORDER BY attempted_at DESC LIMIT ?",
            (max(1, min(int(limit), 1000)),),
        ).fetchall():
            got = dict(row)
            for key in ("reason_codes_json", "plan_json", "lineage_json"):
                try:
                    got[key[:-5] if key.endswith("_json") else key] = json.loads(got.get(key) or "[]")
                except (TypeError, ValueError):
                    got[key[:-5] if key.endswith("_json") else key] = [] if "reason" in key else {}
            rows.append(got)
    return {"schema_version": "17.6", "source": "strategy4_attempts", "count": len(rows), "items": rows}


def strategy5_evidence_payload(limit: int = 200) -> dict[str, Any]:
    got = read_optional_json_file(CURRENT_JSON_PATHS["strategy5_evidence"])
    payload = got["data"] if got else {"schema_version": "20.1", "source": "strategy5_direction_evidence", "count": 0, "items": []}
    rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    payload = dict(payload)
    payload["items"] = rows[: max(1, min(int(limit or 200), 1000))]
    payload["count"] = len(rows)
    payload["display_count"] = len(payload["items"])
    return payload


def strategy5_runtime_payload(limit: int = 200) -> dict[str, Any]:
    from laoma_signal_engine.strategy5.evidence import paths

    p = paths(PROJECT_ROOT)
    latest_plan = _read_optional_raw_json(p.latest_trade_plan) or {}
    latest_evidence = _read_optional_raw_json(p.latest_evidence) or {}
    rows: list[dict[str, Any]] = []
    if p.db.is_file():
        try:
            with sqlite3.connect(p.db) as con:
                con.row_factory = sqlite3.Row
                for row in con.execute(
                    "select evidence_id, run_id, cycle_id, generated_at, symbol, trigger_side, legacy_side, "
                    "label, recommendation, continuation_score, exhaustion_score, executable "
                    "from strategy5_evidence order by generated_at desc limit ?",
                    (max(1, min(int(limit or 200), 1000)),),
                ).fetchall():
                    rows.append(dict(row))
        except sqlite3.Error:
            rows = []
    return {
        "schema_version": "20.4",
        "source": "strategy5_runtime",
        "db_path": str(p.db),
        "latest_trade_plan": {
            "run_id": latest_plan.get("run_id") if isinstance(latest_plan, dict) else None,
            "cycle_id": latest_plan.get("cycle_id") if isinstance(latest_plan, dict) else None,
            "generated_at": latest_plan.get("generated_at") if isinstance(latest_plan, dict) else None,
            "count": latest_plan.get("count") if isinstance(latest_plan, dict) else 0,
            "executable_count": latest_plan.get("executable_count") if isinstance(latest_plan, dict) else 0,
            "status": latest_plan.get("status") if isinstance(latest_plan, dict) else "missing",
        },
        "latest_evidence": {
            "generated_at": latest_evidence.get("generated_at") if isinstance(latest_evidence, dict) else None,
            "count": latest_evidence.get("count") if isinstance(latest_evidence, dict) else 0,
        },
        "ledger_count": len(rows),
        "items": rows,
    }


def strategy6_evidence_payload(limit: int = 200) -> dict[str, Any]:
    got = read_optional_json_file(CURRENT_JSON_PATHS["strategy6_evidence"])
    payload = got["data"] if got else {"schema_version": "22.1", "source": "strategy6_market_accepted_entry", "count": 0, "items": []}
    rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    payload = dict(payload)
    payload["items"] = rows[: max(1, min(int(limit or 200), 1000))]
    payload["count"] = len(rows)
    payload["display_count"] = len(payload["items"])
    return payload


def strategy6_decisions_payload(limit: int = 200) -> dict[str, Any]:
    got = read_optional_json_file(CURRENT_JSON_PATHS["strategy6_decisions"])
    payload = got["data"] if got else {"schema_version": "22.1", "source": "strategy6_decisions", "count": 0, "items": []}
    rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    payload = dict(payload)
    payload["items"] = rows[: max(1, min(int(limit or 200), 1000))]
    payload["count"] = len(rows)
    payload["display_count"] = len(payload["items"])
    return payload


def strategy6_wait_pool_payload(limit: int = 200) -> dict[str, Any]:
    got = read_optional_json_file(CURRENT_JSON_PATHS["strategy6_wait_pool"])
    payload = got["data"] if got else {"schema_version": "22.1", "source": "strategy6_wait_pool", "count": 0, "items": []}
    rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    payload = dict(payload)
    payload["items"] = rows[: max(1, min(int(limit or 200), 1000))]
    payload["count"] = len(rows)
    payload["display_count"] = len(payload["items"])
    return payload


def strategy6_observe_pool_payload(limit: int = 200) -> dict[str, Any]:
    got = read_optional_json_file(CURRENT_JSON_PATHS["strategy6_wait_pool"])
    payload = got["data"] if got else {"schema_version": "22.12", "source": "strategy6_wait_pool", "count": 0, "items": []}
    rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    payload = dict(payload)
    payload["items"] = rows[: max(1, min(int(limit or 200), 1000))]
    payload["count"] = len(rows)
    payload["display_count"] = len(payload["items"])
    return payload


def strategy6_attempts_payload(limit: int = 200) -> dict[str, Any]:
    from laoma_signal_engine.strategy6.evidence import paths

    p = paths(PROJECT_ROOT)
    got = read_optional_json_file(p.latest_observe_attempts)
    payload = got["data"] if got else {"schema_version": "22.16", "source": "strategy6_observe_attempts", "count": 0, "items": []}
    rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    payload = dict(payload)
    payload["items"] = rows[: max(1, min(int(limit or 200), 1000))]
    payload["count"] = len(rows)
    payload["display_count"] = len(payload["items"])
    return payload


def strategy6_heartbeat_payload() -> dict[str, Any]:
    from laoma_signal_engine.strategy6.evidence import strategy6_daemon_status

    return strategy6_daemon_status(PROJECT_ROOT)


def strategy6_watchdog_payload(recover: bool = False) -> dict[str, Any]:
    from laoma_signal_engine.strategy6.evidence import strategy6_watchdog

    return strategy6_watchdog(PROJECT_ROOT, recover=bool(recover))


def strategy6_runtime_payload(limit: int = 200) -> dict[str, Any]:
    from laoma_signal_engine.strategy6.evidence import paths, strategy6_daemon_status

    p = paths(PROJECT_ROOT)
    latest_plan = _read_optional_raw_json(p.latest_trade_plan) or {}
    latest_evidence = _read_optional_raw_json(p.latest_evidence) or {}
    latest_decisions = _read_optional_raw_json(p.latest_decisions) or {}
    latest_wait = _read_optional_raw_json(p.latest_wait_pool) or {}
    rows: list[dict[str, Any]] = []
    if p.db.is_file():
        try:
            with sqlite3.connect(p.db) as con:
                con.row_factory = sqlite3.Row
                for row in con.execute(
                    "select evidence_id, run_id, cycle_id, generated_at, symbol, legacy_side, strategy6_side, "
                    "direction_acceptance_score, entry_price_quality_score, market_acceptance_score, "
                    "decision_state, wait_state, executable "
                    "from strategy6_evidence order by generated_at desc limit ?",
                    (max(1, min(int(limit or 200), 1000)),),
                ).fetchall():
                    rows.append(dict(row))
        except sqlite3.Error:
            rows = []
    return {
        "schema_version": "22.9",
        "source": "strategy6_runtime",
        "db_path": str(p.db),
        "daemon": strategy6_daemon_status(PROJECT_ROOT),
        "latest_trade_plan": {
            "run_id": latest_plan.get("run_id") if isinstance(latest_plan, dict) else None,
            "cycle_id": latest_plan.get("cycle_id") if isinstance(latest_plan, dict) else None,
            "generated_at": latest_plan.get("generated_at") if isinstance(latest_plan, dict) else None,
            "count": latest_plan.get("count") if isinstance(latest_plan, dict) else 0,
            "executable_count": latest_plan.get("executable_count") if isinstance(latest_plan, dict) else 0,
            "status": latest_plan.get("status") if isinstance(latest_plan, dict) else "missing",
        },
        "latest_evidence": {
            "generated_at": latest_evidence.get("generated_at") if isinstance(latest_evidence, dict) else None,
            "count": latest_evidence.get("count") if isinstance(latest_evidence, dict) else 0,
        },
        "latest_decisions": {
            "generated_at": latest_decisions.get("generated_at") if isinstance(latest_decisions, dict) else None,
            "count": latest_decisions.get("count") if isinstance(latest_decisions, dict) else 0,
        },
        "latest_wait_pool": {
            "generated_at": latest_wait.get("generated_at") if isinstance(latest_wait, dict) else None,
            "count": latest_wait.get("count") if isinstance(latest_wait, dict) else 0,
        },
        "ledger_count": len(rows),
        "items": rows,
    }


def strategy6_run_once_payload() -> dict[str, Any]:
    from laoma_signal_engine.strategy6.evidence import write_strategy6_outputs

    run_id = f"strategy6_run_once_{int(time.time())}"
    cycle_id = f"cycle_{run_id}"
    return write_strategy6_outputs(PROJECT_ROOT, run_id=run_id, cycle_id=cycle_id)


def strategy6_recheck_now_payload() -> dict[str, Any]:
    from laoma_signal_engine.strategy6.evidence import run_strategy6_observe_once

    run_id = f"strategy6_recheck_now_{int(time.time())}"
    return run_strategy6_observe_once(PROJECT_ROOT, run_id=run_id, cycle_id=f"cycle_{run_id}")


def strategy6_daemon_action_payload(action: str) -> dict[str, Any]:
    from laoma_signal_engine.strategy6.daemon import start_daemon, stop_daemon
    from laoma_signal_engine.strategy6.evidence import strategy6_daemon_status

    action_l = str(action or "").lower()
    if action_l == "start":
        return start_daemon(PROJECT_ROOT)
    if action_l == "stop":
        return stop_daemon(PROJECT_ROOT)
    if action_l == "status":
        return strategy6_daemon_status(PROJECT_ROOT)
    raise ApiServiceError("strategy6_daemon_action_invalid", "unsupported strategy6 daemon action", {"action": action})


TRADE_PLAN_FUNNEL_LINE_NAMES = {
    "without_micro": "策略1 without micro",
    "micro_fast": "策略2 micro fast",
    "micro_full": "策略3 micro full",
    "strategy4": "策略4 wait observe",
    "strategy4_wait_observe": "策略4 wait observe",
    "strategy5": "策略5 direction evidence",
    "strategy6": "策略6 market accepted entry",
}

TRADE_PLAN_REASON_CATEGORIES = {
    "micro": ("micro_", "cvd_", "ofi_", "full_z", "fast_z", "coverage_", "warmup_"),
    "market_now": ("long_now_", "short_now_", "market_now_", "better_entry_required", "market_only_"),
    "liquidity": ("liquidity", "slippage", "spread", "depth_"),
    "risk_sizing": ("position_sizing", "planned_loss", "risk_budget", "notional", "loss_cap", "paper_fallback"),
    "rr_sl_tp": ("rr", "effective_rr", "stop_", "tp_", "take_profit", "valid_risk", "reachable_reward"),
    "profile": ("symbol_execution_tier", "profile_", "business_pool", "watch_only", "tradability"),
    "direction": ("direction", "range_", "residual", "score_too_low"),
    "paper": ("paper_", "duplicate", "consumed", "source_epoch"),
    "freshness": ("fresh", "stale", "age_", "run_id_mismatch", "output_"),
    "contract": ("contract", "schema", "missing_", "invalid_", "json_"),
}


def _reason_category(reason_code: str) -> str:
    code = str(reason_code or "").lower()
    for category, prefixes in TRADE_PLAN_REASON_CATEGORIES.items():
        if any(code.startswith(prefix) or prefix in code for prefix in prefixes):
            return category
    return "other"


def _trade_plan_archive_path(run_id: str, line: str) -> Path:
    return PROJECT_ROOT / "DATA" / "decisions" / "trade_plan_runs" / run_id / f"latest_trade_plan_{line}.json"


def _strategy_pipeline_report_path(run_id: str) -> Path:
    return PROJECT_ROOT / "DATA" / "reports" / "pipeline_runs" / run_id / "strategy_pipeline_report.json"


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _paper_rows_by_run(run_id: str) -> dict[str, list[dict[str, Any]]]:
    db_path = _paper_db_path()
    tables = ("paper_intent_inbox", "paper_trade_plans", "paper_orders", "paper_skip_ledger", "paper_positions")
    rows: dict[str, list[dict[str, Any]]] = {table: [] for table in tables}
    if not db_path.exists():
        return rows
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            for table in tables:
                rows[table] = [
                    dict(row)
                    for row in conn.execute(
                        f"select * from {table} where source_run_id = ? order by rowid desc",
                        (run_id,),
                    ).fetchall()
                ]
    except sqlite3.Error:
        return rows
    return rows


def _paper_index(rows: dict[str, list[dict[str, Any]]], table: str) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    indexed: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows.get(table) or []:
        key = (
            str(row.get("strategy_line") or ""),
            str(row.get("symbol") or "").upper(),
            str(row.get("source_plan_hash") or ""),
        )
        indexed.setdefault(key, []).append(row)
    return indexed


def _line_funnel_doc(
    *,
    line: str,
    run_id: str,
    cycle_id: str | None,
    selected: bool,
    skipped: bool,
    paper_rows: dict[str, list[dict[str, Any]]],
    symbol_limit: int,
) -> dict[str, Any]:
    archive_path = _trade_plan_archive_path(run_id, line)
    doc = _read_optional_raw_json(archive_path)
    output_run_id = doc.get("run_id") if isinstance(doc, dict) else None
    output_cycle_id = doc.get("cycle_id") if isinstance(doc, dict) else None
    output_fresh = bool(output_run_id == run_id and (not cycle_id or output_cycle_id == cycle_id))
    plans = doc.get("plans") if isinstance(doc, dict) and isinstance(doc.get("plans"), list) else []

    orders_by_key = _paper_index(paper_rows, "paper_orders")
    skips_by_key = _paper_index(paper_rows, "paper_skip_ledger")
    intents_by_key = _paper_index(paper_rows, "paper_intent_inbox")

    action_counts = Counter(str(plan.get("action") or "unknown") for plan in plans if isinstance(plan, dict))
    reason_counter: Counter[str] = Counter()
    reason_symbols: dict[str, set[str]] = {}
    symbols: list[dict[str, Any]] = []
    market_now_passed = 0
    market_now_blocked = 0
    risk_ok = 0
    paper_order_count = 0
    paper_skip_count = 0
    paper_missing = 0

    for plan in plans:
        if not isinstance(plan, dict):
            continue
        symbol = str(plan.get("symbol") or "").upper()
        reason_codes = [str(code) for code in (plan.get("reason_codes") or [])]
        for code in reason_codes:
            reason_counter[code] += 1
            reason_symbols.setdefault(code, set()).add(symbol)

        guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
        sizing = plan.get("position_sizing") if isinstance(plan.get("position_sizing"), dict) else {}
        input_refs = plan.get("input_refs") if isinstance(plan.get("input_refs"), dict) else {}
        source_hash = str(input_refs.get("source_plan_hash") or "")
        key = (line, symbol, source_hash)
        orders = orders_by_key.get(key) or []
        skips = skips_by_key.get(key) or []
        intents = intents_by_key.get(key) or []
        if orders:
            paper_order_count += 1
            paper_status = "consumed"
        elif skips:
            paper_skip_count += 1
            paper_status = "skipped"
        elif plan.get("executable"):
            paper_missing += 1
            paper_status = "missing"
        else:
            paper_status = "not_applicable"

        market_cal = guards.get("market_now_calibration") if isinstance(guards.get("market_now_calibration"), dict) else {}
        if market_cal.get("ok") is True:
            market_now_passed += 1
        elif market_cal.get("ok") is False:
            market_now_blocked += 1
        if plan.get("executable") and sizing:
            risk_ok += 1

        planned_notional = _safe_float(sizing.get("planned_notional_usdt") or sizing.get("notional_usdt") or guards.get("planned_notional_usdt"))
        order = orders[0] if orders else {}
        order_notional = _safe_float(order.get("planned_notional_usdt") or order.get("notional_usdt"))
        symbols.append(
            {
                "symbol": symbol,
                "line": line,
                "side": plan.get("side") or plan.get("decision"),
                "decision": plan.get("decision"),
                "action": plan.get("action"),
                "entry_mode": plan.get("entry_mode"),
                "executable": bool(plan.get("executable")),
                "paper_eligible": plan.get("paper_eligible"),
                "notify_eligible": plan.get("notify_eligible"),
                "reason_codes": reason_codes,
                "risk": {
                    "entry": plan.get("estimated_entry_price"),
                    "stop_loss": plan.get("stop_loss"),
                    "take_profit": plan.get("take_profit"),
                    "rr": plan.get("rr"),
                    "effective_rr": guards.get("effective_rr"),
                    "valid_risk_bps": guards.get("valid_risk_bps"),
                    "planned_notional_usdt": planned_notional,
                    "planned_quantity": sizing.get("planned_quantity") or sizing.get("quantity") or guards.get("planned_quantity"),
                    "planned_loss_usdt": sizing.get("planned_loss_usdt") or guards.get("planned_loss_usdt"),
                    "planned_profit_usdt": sizing.get("planned_profit_usdt") or guards.get("planned_profit_usdt"),
                    "max_planned_loss_usdt": sizing.get("max_planned_loss_usdt") or guards.get("max_planned_loss_usdt"),
                    "loss_cap_applied": bool(
                        sizing.get("loss_cap_applied")
                        or guards.get("loss_cap_applied")
                        or "max_planned_loss_cap" in (sizing.get("sizing_caps_applied") or [])
                    ),
                },
                "quality": {
                    "market_now_status": guards.get("market_now_calibration_status"),
                    "trade_worthiness": guards.get("trade_worthiness"),
                    "liquidity_ok": guards.get("liquidity_ok"),
                    "range_room_ok": guards.get("range_room_ok"),
                    "profile_gate_enabled": guards.get("profile_gate_enabled"),
                    "symbol_execution_tier": guards.get("symbol_execution_tier"),
                    "business_pool": guards.get("business_pool"),
                    "micro_confirmation_level": guards.get("micro_confirmation_level"),
                    "micro_exec_allowed_reason": guards.get("micro_exec_allowed_reason"),
                },
                "paper": {
                    "status": paper_status,
                    "paper_status": paper_status,
                    "intent_id": (intents[0] if intents else {}).get("intent_id"),
                    "order_id": order.get("id"),
                    "paper_order_id": order.get("id"),
                    "skip_reason": (skips[0] if skips else {}).get("skip_reason"),
                    "planned_notional_usdt": order_notional,
                    "notional_match": (
                        None
                        if planned_notional is None or order_notional is None
                        else abs(planned_notional - order_notional) <= 0.01
                    ),
                },
                "lineage": {
                    "source_plan_hash": source_hash,
                    "source_path": str(archive_path),
                    "source_generated_at": doc.get("generated_at") if isinstance(doc, dict) else None,
                    "factor_generated_at": input_refs.get("factor_generated_at"),
                    "refresh_generated_at": input_refs.get("refresh_generated_at"),
                    "micro_generated_at": input_refs.get("micro_generated_at"),
                },
            }
        )

    executable_count = sum(1 for plan in plans if isinstance(plan, dict) and plan.get("executable"))
    wait_count = sum(1 for plan in plans if isinstance(plan, dict) and str(plan.get("action") or "").upper() == "WAIT")
    enter_market_count = sum(1 for plan in plans if isinstance(plan, dict) and str(plan.get("action") or "").upper() == "ENTER_MARKET")
    reason_groups = [
        {
            "reason": code,
            "reason_code": code,
            "category": _reason_category(code),
            "count": count,
            "symbols": sorted(reason_symbols.get(code) or [])[:20],
        }
        for code, count in reason_counter.most_common(80)
    ]
    counts = {
        "input_symbols": len(plans),
        "plans": len(plans),
        "total_plans": len(plans),
        "wait": wait_count,
        "enter_market": enter_market_count,
        "market": enter_market_count,
        "executable": executable_count,
        "blocked": max(0, len(plans) - executable_count),
        "market_now_passed": market_now_passed,
        "market_now_blocked": market_now_blocked,
        "risk_ok": risk_ok,
        "paper_orders": paper_order_count,
        "paper_skipped": paper_skip_count,
        "paper_skips": paper_skip_count,
        "paper_missing": paper_missing,
    }
    return {
        "line": line,
        "display_name": TRADE_PLAN_FUNNEL_LINE_NAMES.get(line, line),
        "selected": selected,
        "skipped": skipped,
        "exists": bool(doc),
        "status": doc.get("status") if isinstance(doc, dict) else "missing",
        "generated_at": doc.get("generated_at") if isinstance(doc, dict) else None,
        "stale": not output_fresh,
        "output_run_id": output_run_id,
        "output_cycle_id": output_cycle_id,
        "stale_reason": "" if output_fresh else "output_run_id_mismatch_or_missing",
        "freshness": {
            "output_fresh": output_fresh,
            "output_run_id": output_run_id,
            "output_cycle_id": output_cycle_id,
            "stale_reason": "" if output_fresh else "output_run_id_mismatch_or_missing",
        },
        "counts": counts,
        "funnel": [
            {"key": "candidate", "stage": "candidate", "label": "Candidate", "count": len(plans)},
            {"key": "trade_plan", "stage": "trade_plan", "label": "Trade Plan", "count": len(plans)},
            {"key": "market_now_passed", "stage": "market_now_passed", "label": "Market NOW", "count": market_now_passed},
            {"key": "risk_ok", "stage": "risk_ok", "label": "Risk OK", "count": risk_ok},
            {"key": "executable", "stage": "executable", "label": "Executable", "count": executable_count},
            {"key": "paper_ordered", "stage": "paper_ordered", "label": "Paper Ordered", "count": paper_order_count},
        ],
        "action_counts": dict(action_counts),
        "reason_groups": reason_groups,
        "symbols": symbols[: max(1, symbol_limit)],
        "symbol_total": len(symbols),
        "source_path": str(archive_path),
    }


def trade_plan_funnel_payload(*, run_id: str | None = None, symbol_limit: int = 300) -> dict[str, Any]:
    latest_report = _read_optional_raw_json(CURRENT_JSON_PATHS["latest_strategy"])
    resolved_run_id = run_id if run_id and run_id != "latest" else (
        latest_report.get("run_id") if isinstance(latest_report, dict) else None
    )
    if not resolved_run_id:
        raise ApiServiceError("run_id_missing", "run_id is required and latest pipeline report is missing")
    pipeline_report = _read_optional_raw_json(_strategy_pipeline_report_path(resolved_run_id))
    if not pipeline_report and isinstance(latest_report, dict) and latest_report.get("run_id") == resolved_run_id:
        pipeline_report = latest_report
    cycle_id = pipeline_report.get("cycle_id") if isinstance(pipeline_report, dict) else None
    selected_lines = pipeline_report.get("selected_lines") if isinstance(pipeline_report, dict) else None
    skipped_lines = pipeline_report.get("skipped_lines") if isinstance(pipeline_report, dict) else None
    selected_set = set(selected_lines or [])
    skipped_set = set(skipped_lines or [])
    line_order = list(dict.fromkeys([*STRATEGY_LINES_ORDERED, *selected_set, *skipped_set]))
    paper_rows = _paper_rows_by_run(resolved_run_id)
    strategy_lines = [
        _line_funnel_doc(
            line=line,
            run_id=resolved_run_id,
            cycle_id=cycle_id,
            selected=True if line == "strategy4" else ((line in selected_set) if selected_lines is not None else True),
            skipped=False if line == "strategy4" else line in skipped_set,
            paper_rows=paper_rows,
            symbol_limit=max(1, min(int(symbol_limit or 300), 1000)),
        )
        for line in line_order
    ]
    totals = Counter()
    for line in strategy_lines:
        totals.update(line.get("counts") or {})
    return {
        "schema_version": "1.0",
        "source": "trade_plan_funnel",
        "run_id": resolved_run_id,
        "cycle_id": cycle_id,
        "generated_at": to_iso_z(utc_now()),
        "pipeline_status": pipeline_report.get("status") if isinstance(pipeline_report, dict) else None,
        "selected_lines": selected_lines or [],
        "skipped_lines": skipped_lines or [],
        "counts": dict(totals),
        "strategy_lines": strategy_lines,
    }


def candidate_pool_governance_payload(limit: int = 120) -> dict[str, Any]:
    universe_got = read_optional_json_file(CURRENT_JSON_PATHS["candidate_universe"])
    light_got = read_optional_json_file(CURRENT_JSON_PATHS["futures_light_snapshot"])
    universe_doc = universe_got["data"] if universe_got else {}
    light_doc = light_got["data"] if light_got else {}

    universe_rows = universe_doc.get("pairs") if isinstance(universe_doc, dict) else []
    light_items = light_doc.get("items") if isinstance(light_doc, dict) else []
    by_symbol: dict[str, dict[str, Any]] = {}
    if isinstance(universe_rows, list):
        for row in universe_rows:
            if not isinstance(row, dict):
                continue
            sym = str(row.get("futures_symbol") or row.get("symbol_safe_id") or "").upper().strip()
            if not sym:
                continue
            by_symbol[sym] = {
                "symbol": sym,
                "base_asset": row.get("base_asset"),
                "universe_profile": row.get("universe_profile") or {},
                "risk_profile": row.get("risk_profile") or {},
                "profile_hydration": {
                    "source": "candidate_universe",
                    "status": "ok"
                    if str((row.get("universe_profile") or {}).get("business_pool") or "unknown") != "unknown"
                    else "incomplete",
                    "reason_codes": []
                    if str((row.get("universe_profile") or {}).get("business_pool") or "unknown") != "unknown"
                    else ["business_pool_missing"],
                },
                "source": "candidate_universe",
            }
    if isinstance(light_items, list):
        for item in light_items:
            if not isinstance(item, dict):
                continue
            sym = str(item.get("symbol") or "").upper().strip()
            if not sym:
                continue
            row = by_symbol.setdefault(sym, {"symbol": sym, "source": "futures_light_snapshot"})
            row.update(
                {
                    "base_asset": item.get("base_asset") or row.get("base_asset"),
                    "universe_profile": item.get("universe_profile") or row.get("universe_profile") or {},
                    "risk_profile": item.get("risk_profile") or row.get("risk_profile") or {},
                    "tradability_profile": item.get("tradability_profile") or {},
                    "primary_pool": item.get("primary_pool") or "unknown",
                    "pool_tags": list(item.get("pool_tags") or []),
                    "source": "merged",
                }
            )

    rows = list(by_symbol.values())
    rows.sort(
        key=lambda r: (
            -int((r.get("tradability_profile") or {}).get("scan_priority") or (r.get("universe_profile") or {}).get("universe_priority_score") or 0),
            str(r.get("symbol") or ""),
        )
    )
    rows = rows[: max(1, min(int(limit or 120), 500))]
    business_counts = Counter(str((r.get("universe_profile") or {}).get("business_pool") or "unknown") for r in rows)
    trade_quality_counts = Counter(str((r.get("tradability_profile") or {}).get("trade_quality_tier") or "unknown") for r in rows)
    execution_counts = Counter(str((r.get("risk_profile") or {}).get("execution_tier") or "unknown") for r in rows)
    hydration_counts = Counter(str((r.get("profile_hydration") or {}).get("status") or "unknown") for r in rows)
    return {
        "schema_version": "1.0",
        "source": "candidate_pool_governance",
        "generated_at": to_iso_z(utc_now()),
        "universe_generated_at": universe_doc.get("generated_at") if isinstance(universe_doc, dict) else None,
        "light_snapshot_generated_at": light_doc.get("generated_at") if isinstance(light_doc, dict) else None,
        "count": len(rows),
        "counts": {
            "business_pool": dict(sorted(business_counts.items())),
            "trade_quality_tier": dict(sorted(trade_quality_counts.items())),
            "execution_tier": dict(sorted(execution_counts.items())),
            "profile_hydration_status": dict(sorted(hydration_counts.items())),
        },
        "profile_hydration": {
            "schema_version": universe_doc.get("profile_schema_version") if isinstance(universe_doc, dict) else None,
            "status": universe_doc.get("profile_hydration_status") if isinstance(universe_doc, dict) else None,
            "reason_codes": universe_doc.get("profile_hydration_reason_codes") if isinstance(universe_doc, dict) else [],
            "counts": universe_doc.get("profile_hydration_counts") if isinstance(universe_doc, dict) else {},
        },
        "items": rows,
    }


def rest_health_payload() -> dict[str, Any]:
    settings = load_light_snapshot_settings()
    circuit = read_rest_circuit(PROJECT_ROOT)
    cache = _load_exchange_info_cache(PROJECT_ROOT, settings)
    latest = step15_snapshot_quality_latest()
    daemon = step15_daemon_health_payload()
    return {
        "schema_version": "STEP12.38_rest_health_v1",
        "generated_at": to_iso_z(utc_now()),
        "rest_circuit_state": circuit.get("rest_circuit_state"),
        "rest_circuit_until": circuit.get("rest_circuit_until"),
        "rest_circuit_remaining_sec": circuit.get("rest_circuit_remaining_sec"),
        "rest_circuit_reason": circuit.get("rest_circuit_reason"),
        "rest_circuit_source_stage": circuit.get("rest_circuit_source_stage"),
        "rest_circuit_source_endpoint": circuit.get("rest_circuit_source_endpoint"),
        "http_429_count": circuit.get("http_429_count"),
        "http_418_count": circuit.get("http_418_count"),
        "retry_after_sec": circuit.get("retry_after_sec"),
        "live_rest_allowed": circuit.get("live_rest_allowed"),
        "degraded_mode_allowed": circuit.get("degraded_mode_allowed"),
        "exchange_info": {
            "policy": "cache_first" if settings.exchange_info_cache_first_enabled else "live_first",
            "source": latest.get("exchange_info_source"),
            "cache_path": cache.get("path"),
            "cache_age_sec": cache.get("age_sec"),
            "cache_ttl_sec": cache.get("ttl_sec"),
            "cache_fresh": cache.get("fresh"),
            "cache_reason": cache.get("reason"),
            "live_refresh_policy": settings.exchange_info_live_refresh_policy,
            "live_refresh_allowed": bool(circuit.get("live_rest_allowed")),
        },
        "market_snapshot": {
            "cache_first_enabled": settings.market_snapshot_cache_first_enabled,
            "cache_ttl_sec": settings.market_snapshot_cache_ttl_sec,
            "cache_min_coverage_ratio": settings.market_snapshot_cache_min_coverage_ratio,
            "source": latest.get("market_snapshot_source"),
            "cache_age_sec": latest.get("market_snapshot_cache_age_sec"),
            "freshness_tier": latest.get("market_snapshot_freshness_tier"),
            "live_attempted": latest.get("market_snapshot_live_attempted"),
            "coverage_ratio": latest.get("market_snapshot_coverage_ratio"),
            "missing_symbol_count": latest.get("market_snapshot_missing_symbol_count"),
        },
        "rest_budget": {
            "preflight_enabled": settings.rest_budget_preflight_enabled,
            "state": latest.get("rest_budget_state"),
            "required_estimate": latest.get("rest_budget_required_estimate"),
            "remaining_estimate": latest.get("rest_budget_remaining_estimate"),
            "min_remaining_weight": settings.rest_budget_min_remaining_weight,
        },
        "snapshot_daemon": daemon,
        "latest_snapshot": latest,
        "source": "runtime_file+current_json",
    }


def step15_snapshot_quality_latest() -> dict[str, Any]:
    got = read_optional_json_file(CURRENT_JSON_PATHS["futures_light_snapshot"])
    doc = got.get("data") if isinstance(got, dict) else None
    if not isinstance(doc, dict):
        return {
            "schema_version": "STEP12.38_snapshot_quality_v1",
            "source": "missing",
            "snapshot_status": "missing",
            "reason_codes": ["futures_light_snapshot_missing"],
        }
    quality = doc.get("snapshot_quality") if isinstance(doc.get("snapshot_quality"), dict) else {}
    items = doc.get("items") if isinstance(doc.get("items"), list) else []
    freshness_counts: Counter[str] = Counter()
    source_mix: Counter[str] = Counter()
    stale_blocked_symbols: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        freshness = str(item.get("item_freshness_status") or "unknown")
        source = str(item.get("item_snapshot_source") or "unknown")
        freshness_counts[freshness] += 1
        source_mix[source] += 1
        if freshness == "stale_blocked":
            stale_blocked_symbols.append(str(item.get("symbol") or "").upper())
    return {
        "schema_version": "STEP12.38_snapshot_quality_v1",
        "source": "futures_light_snapshot",
        "source_path": got.get("source_path"),
        "generated_at": doc.get("generated_at"),
        "run_id": doc.get("run_id"),
        "cycle_id": doc.get("cycle_id"),
        "snapshot_status": quality.get("snapshot_status") or "unknown",
        "success_count": quality.get("snapshot_success_count") or doc.get("success_count"),
        "failed_count": quality.get("snapshot_failed_count") or doc.get("failed_count"),
        "failed_symbols": quality.get("snapshot_failed_symbols") or [],
        "failed_symbol_count": quality.get("snapshot_failed_symbol_count"),
        "candidate_allowed_count": quality.get("downstream_candidate_count"),
        "candidate_dropped_count": quality.get("snapshot_failed_count") or 0,
        "exchange_info_source": quality.get("exchange_info_source"),
        "exchange_info_live_error": quality.get("exchange_info_live_error"),
        "market_snapshot_source": quality.get("market_snapshot_source"),
        "market_snapshot_cache_age_sec": quality.get("market_snapshot_cache_age_sec"),
        "market_snapshot_freshness_tier": quality.get("market_snapshot_freshness_tier"),
        "market_snapshot_live_attempted": quality.get("market_snapshot_live_attempted"),
        "market_snapshot_coverage_ratio": quality.get("market_snapshot_coverage_ratio"),
        "market_snapshot_missing_symbol_count": quality.get("market_snapshot_missing_symbol_count"),
        "rest_budget_state": quality.get("rest_budget_state"),
        "rest_budget_required_estimate": quality.get("rest_budget_required_estimate"),
        "rest_budget_remaining_estimate": quality.get("rest_budget_remaining_estimate"),
        "rest_recovery_stage": quality.get("rest_recovery_stage"),
        "rest_consecutive_successful_shards": quality.get("rest_consecutive_successful_shards"),
        "rest_closed_successful_shards": quality.get("rest_closed_successful_shards"),
        "rest_success_required_for_close": quality.get("rest_success_required_for_close"),
        "current_shard_size": quality.get("current_shard_size"),
        "next_shard_size": quality.get("next_shard_size"),
        "rest_request_count": quality.get("rest_request_count"),
        "rest_weight_used": quality.get("rest_weight_used"),
        "rest_endpoint_counts": quality.get("rest_endpoint_counts") or {},
        "rest_status_code_counts": quality.get("rest_status_code_counts") or {},
        "status_418_count": quality.get("status_418_count"),
        "status_429_count": quality.get("status_429_count"),
        "snapshot_runtime_mode": quality.get("snapshot_runtime_mode"),
        "daemon_status": quality.get("daemon_status"),
        "daemon_heartbeat_at": quality.get("daemon_heartbeat_at"),
        "daemon_heartbeat_age_sec": quality.get("daemon_heartbeat_age_sec"),
        "current_shard_id": quality.get("current_shard_id"),
        "next_shard_cursor": quality.get("next_shard_cursor"),
        "planned_symbols": quality.get("planned_symbols") or [],
        "live_refreshed_symbols": quality.get("live_refreshed_symbols") or [],
        "symbol_source_mix": quality.get("symbol_source_mix") or dict(source_mix),
        "freshness_counts": quality.get("freshness_counts") or dict(freshness_counts),
        "stale_blocked_symbols": stale_blocked_symbols[:200],
        "stale_blocked_symbol_count": len(stale_blocked_symbols),
        "degraded_symbol_count": quality.get("degraded_symbol_count"),
        "skipped_symbol_count": quality.get("skipped_symbol_count"),
        "skipped_symbols": quality.get("skipped_symbols") or [],
        "websocket_snapshot_available": quality.get("websocket_snapshot_available"),
        "websocket_snapshot_age_sec": quality.get("websocket_snapshot_age_sec"),
        "rest_circuit_state": quality.get("rest_circuit_state"),
        "reason_codes": quality.get("reason_codes") or [],
        "raw": quality,
        "items": items[:500],
    }


def step15_daemon_payload() -> dict[str, Any]:
    status = snapshot_daemon_status(PROJECT_ROOT)
    latest = step15_snapshot_quality_latest()
    got = read_optional_json_file(CURRENT_JSON_PATHS["futures_light_snapshot"])
    doc = got.get("data") if isinstance(got, dict) else None
    raw_items = doc.get("items") if isinstance(doc, dict) and isinstance(doc.get("items"), list) else []
    items = []
    for item in raw_items[:500]:
        if not isinstance(item, dict):
            continue
        items.append(
            {
                "symbol": item.get("symbol"),
                "primary_pool": item.get("primary_pool"),
                "item_snapshot_source": item.get("item_snapshot_source"),
                "item_snapshot_age_sec": item.get("item_snapshot_age_sec"),
                "item_freshness_sla_sec": item.get("item_freshness_sla_sec"),
                "item_freshness_status": item.get("item_freshness_status"),
                "item_downstream_allowed": item.get("item_downstream_allowed"),
                "item_downstream_scope": item.get("item_downstream_scope"),
                "last_live_refresh_at": item.get("last_live_refresh_at"),
                "websocket_cache_generated_at": item.get("websocket_cache_generated_at"),
                "rest_cache_generated_at": item.get("rest_cache_generated_at"),
                "snapshot_source_priority": item.get("snapshot_source_priority"),
                "shard_id": item.get("shard_id"),
                "reason_codes": item.get("reason_codes") or [],
                "tradability_profile": item.get("tradability_profile") or {},
                "risk_profile": item.get("risk_profile") or {},
                "universe_profile": item.get("universe_profile") or {},
            }
        )
    return {
        **status,
        "latest_snapshot": latest,
        "items": items,
        "item_count": len(raw_items),
        "items_returned": len(items),
    }


def step15_daemon_health_payload() -> dict[str, Any]:
    status = snapshot_daemon_status(PROJECT_ROOT)
    return {
        "schema_version": status.get("schema_version"),
        "source": status.get("source"),
        "generated_at": status.get("generated_at"),
        "status": status.get("status"),
        "daemon_status": status.get("daemon_status"),
        "heartbeat_at": status.get("heartbeat_at"),
        "heartbeat_age_sec": status.get("heartbeat_age_sec"),
        "stale_after_sec": status.get("stale_after_sec"),
        "pid": status.get("pid"),
        "pid_alive": status.get("pid_alive"),
        "watchdog_status": status.get("watchdog_status"),
        "watchdog_action": status.get("watchdog_action"),
        "restart_count": status.get("restart_count"),
        "last_tick_at": status.get("last_tick_at"),
        "last_successful_shard_at": status.get("last_successful_shard_at"),
        "current_shard_id": status.get("current_shard_id"),
        "next_shard_at": status.get("next_shard_at"),
        "queue_depth": status.get("queue_depth"),
        "source_mix": status.get("source_mix") or {},
        "freshness_counts": status.get("freshness_counts") or {},
        "rest_circuit_state": status.get("rest_circuit_state"),
        "rest_recovery_stage": status.get("rest_recovery_stage"),
        "rest_consecutive_successful_shards": status.get("rest_consecutive_successful_shards"),
        "rest_closed_successful_shards": status.get("rest_closed_successful_shards"),
        "rest_success_required_for_close": status.get("rest_success_required_for_close"),
        "half_open_success_required": status.get("half_open_success_required"),
        "current_shard_size": status.get("current_shard_size"),
        "next_shard_size": status.get("next_shard_size"),
        "rest_cooldown_until": status.get("rest_cooldown_until"),
        "rest_circuit_remaining_sec": status.get("rest_circuit_remaining_sec"),
        "rest_request_count": status.get("rest_request_count"),
        "rest_weight_used": status.get("rest_weight_used"),
        "rest_endpoint_counts": status.get("rest_endpoint_counts") or {},
        "rest_status_code_counts": status.get("rest_status_code_counts") or {},
        "status_418_count": status.get("status_418_count"),
        "status_429_count": status.get("status_429_count"),
        "retry_after_sec": status.get("retry_after_sec"),
        "cooldown_until": status.get("cooldown_until"),
        "reason_codes": status.get("reason_codes") or [],
    }


def rest_budget_runtime_payload() -> dict[str, Any]:
    daemon = step15_daemon_health_payload()
    latest = step15_snapshot_quality_latest()
    return {
        "schema_version": "STEP12.42_rest_budget_runtime_v1",
        "source": "snapshot_daemon+latest_snapshot",
        "generated_at": to_iso_z(utc_now()),
        "rest_circuit_state": daemon.get("rest_circuit_state") or latest.get("rest_circuit_state"),
        "rest_recovery_stage": daemon.get("rest_recovery_stage") or latest.get("rest_recovery_stage"),
        "rest_consecutive_successful_shards": daemon.get("rest_consecutive_successful_shards"),
        "rest_closed_successful_shards": daemon.get("rest_closed_successful_shards"),
        "rest_success_required_for_close": daemon.get("rest_success_required_for_close"),
        "half_open_success_required": daemon.get("half_open_success_required"),
        "current_shard_size": daemon.get("current_shard_size"),
        "next_shard_size": daemon.get("next_shard_size"),
        "rest_request_count": daemon.get("rest_request_count") or latest.get("rest_request_count") or 0,
        "rest_weight_used": daemon.get("rest_weight_used") or latest.get("rest_weight_used"),
        "rest_endpoint_counts": daemon.get("rest_endpoint_counts") or latest.get("rest_endpoint_counts") or {},
        "rest_status_code_counts": daemon.get("rest_status_code_counts") or latest.get("rest_status_code_counts") or {},
        "status_418_count": daemon.get("status_418_count") or latest.get("status_418_count") or 0,
        "status_429_count": daemon.get("status_429_count") or latest.get("status_429_count") or 0,
        "retry_after_sec": daemon.get("retry_after_sec"),
        "cooldown_until": daemon.get("cooldown_until") or daemon.get("rest_cooldown_until"),
        "freshness_counts": daemon.get("freshness_counts") or latest.get("freshness_counts") or {},
        "source_mix": daemon.get("source_mix") or latest.get("symbol_source_mix") or {},
        "reason_codes": sorted(set([*(daemon.get("reason_codes") or []), *(latest.get("reason_codes") or [])])),
    }


def snapshot_warmup_payload(*, min_usable_symbol_count: int = 3) -> dict[str, Any]:
    daemon = step15_daemon_health_payload()
    latest = step15_snapshot_quality_latest()
    freshness = latest.get("freshness_counts") if isinstance(latest.get("freshness_counts"), dict) else {}
    fresh_count = _safe_int(freshness.get("fresh"), 0)
    stale_usable_count = _safe_int(freshness.get("stale_usable"), 0)
    stale_blocked_count = _safe_int(freshness.get("stale_blocked"), 0)
    usable_count = fresh_count + stale_usable_count
    daemon_state = str(daemon.get("daemon_status") or daemon.get("status") or "").lower()
    watchdog_state = str(daemon.get("watchdog_status") or "").lower()
    snapshot_status = str(latest.get("snapshot_status") or "").lower()
    snapshot_exists = latest.get("source") != "missing" and snapshot_status != "missing"
    daemon_ok = daemon_state in {"running", "degraded_cache", "paused"} and watchdog_state not in {"stale", "missing"}
    min_count = max(1, int(min_usable_symbol_count))
    enough_symbols = usable_count >= min_count
    reason_codes: list[str] = []
    if not snapshot_exists:
        reason_codes.append("snapshot_missing")
    if not daemon_ok:
        reason_codes.append("snapshot_daemon_not_ready")
    if not enough_symbols:
        reason_codes.append("snapshot_usable_symbols_below_min")
    rest_state = str(latest.get("rest_circuit_state") or daemon.get("rest_circuit_state") or "").lower()
    ready = bool(snapshot_exists and daemon_ok and enough_symbols)
    if ready and rest_state == "half_open":
        status = "ready_degraded"
        ready_status_detail = "ready_degraded_rest_half_open"
        reason_codes.append("ready_degraded_rest_half_open")
        reason_codes.append(f"rest_circuit_{rest_state}")
    elif ready and rest_state == "open":
        status = "ready_degraded"
        ready_status_detail = "ready_degraded_rest_open"
        reason_codes.append("ready_degraded_rest_open")
        reason_codes.append(f"rest_circuit_{rest_state}")
    elif ready and stale_blocked_count > 0:
        status = "ready_degraded"
        ready_status_detail = "ready_degraded_partial_freshness"
        reason_codes.append("ready_degraded_partial_freshness")
    elif ready:
        status = "ready"
        ready_status_detail = "ready_full"
    elif snapshot_exists and daemon_state in {"running", "degraded_cache", "paused"}:
        status = "warming"
        ready_status_detail = "warming_snapshot"
    else:
        status = "blocked"
        ready_status_detail = "blocked_warmup"
    return {
        "schema_version": "STEP16.8_snapshot_warmup_v1",
        "source": "snapshot_warmup_gate",
        "generated_at": to_iso_z(utc_now()),
        "status": status,
        "ready": ready,
        "allow_run_once": ready,
        "allow_run_cycle": ready,
        "usable_symbol_count": usable_count,
        "fresh_count": fresh_count,
        "stale_usable_count": stale_usable_count,
        "stale_blocked_count": stale_blocked_count,
        "min_usable_symbol_count": min_count,
        "snapshot_exists": snapshot_exists,
        "snapshot_generated_at": latest.get("generated_at"),
        "snapshot_runtime_mode": latest.get("snapshot_runtime_mode"),
        "snapshot_status": latest.get("snapshot_status"),
        "daemon_status": daemon.get("daemon_status") or daemon.get("status"),
        "watchdog_status": daemon.get("watchdog_status"),
        "heartbeat_age_sec": daemon.get("heartbeat_age_sec"),
        "rest_circuit_state": latest.get("rest_circuit_state") or daemon.get("rest_circuit_state"),
        "ready_status_detail": ready_status_detail,
        "freshness_degradation_reason": "rest_half_open" if rest_state == "half_open" else ("rest_open" if rest_state == "open" else ("partial_freshness" if stale_blocked_count > 0 else None)),
        "rest_recovery_stage": daemon.get("rest_recovery_stage") or latest.get("rest_recovery_stage"),
        "current_shard_size": daemon.get("current_shard_size") or latest.get("current_shard_size"),
        "next_shard_size": daemon.get("next_shard_size") or latest.get("next_shard_size"),
        "rest_consecutive_successful_shards": daemon.get("rest_consecutive_successful_shards") or latest.get("rest_consecutive_successful_shards"),
        "rest_success_required_for_close": daemon.get("rest_success_required_for_close") or latest.get("rest_success_required_for_close"),
        "disabled_reason": None if ready else "snapshot_warmup_not_ready",
        "reason_codes": sorted(set(reason_codes)),
    }


def step15_snapshot_quality_by_run(run_id: str) -> dict[str, Any]:
    if not run_id or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789TtZz_-:" for ch in run_id):
        raise ApiServiceError("run_id_invalid", "run_id contains unsupported characters", {"run_id": run_id})
    latest = step15_snapshot_quality_latest()
    if latest.get("run_id") == run_id:
        return latest
    report = _read_optional_raw_json(PROJECT_ROOT / "DATA" / "reports" / "pipeline_runs" / run_id / "strategy_pipeline_report.json")
    if isinstance(report, dict):
        for row in report.get("stages") or []:
            if isinstance(row, dict) and "fetch" in str(row.get("name") or ""):
                return {
                    "schema_version": "STEP12.38_snapshot_quality_v1",
                    "source": "pipeline_report_archive",
                    "run_id": run_id,
                    "snapshot_status": row.get("status") or ("ok" if row.get("ok") else "failed"),
                    "reason_codes": row.get("reason_codes") or [],
                    "raw": row,
                }
    raise ApiServiceError("file_missing", "Step1.5 snapshot quality for run does not exist", {"run_id": run_id})


def rest_safety_config_payload() -> dict[str, Any]:
    settings = load_light_snapshot_settings()
    return {
        "exchange_info_cache_first_enabled": settings.exchange_info_cache_first_enabled,
        "exchange_info_cache_path": settings.exchange_info_cache_path,
        "exchange_info_cache_ttl_sec": settings.exchange_info_cache_ttl_sec,
        "exchange_info_live_refresh_policy": settings.exchange_info_live_refresh_policy,
        "exchange_info_allow_cache_on_429_418": settings.exchange_info_allow_cache_on_429_418,
        "exchange_info_fail_if_cache_missing": settings.exchange_info_fail_if_cache_missing,
        "rest_circuit_default_418_cooldown_sec": settings.rest_circuit_default_418_cooldown_sec,
        "rest_circuit_default_429_cooldown_sec": settings.rest_circuit_default_429_cooldown_sec,
        "market_snapshot_cache_first_enabled": settings.market_snapshot_cache_first_enabled,
        "market_snapshot_cache_ttl_sec": settings.market_snapshot_cache_ttl_sec,
        "market_snapshot_cache_min_coverage_ratio": settings.market_snapshot_cache_min_coverage_ratio,
        "market_snapshot_fail_closed_on_circuit_open": settings.market_snapshot_fail_closed_on_circuit_open,
        "rest_budget_preflight_enabled": settings.rest_budget_preflight_enabled,
        "rest_budget_min_remaining_weight": settings.rest_budget_min_remaining_weight,
    }


def update_rest_safety_config_payload(values: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "exchange_info_cache_first_enabled",
        "exchange_info_cache_ttl_sec",
        "exchange_info_live_refresh_policy",
        "exchange_info_allow_cache_on_429_418",
        "exchange_info_fail_if_cache_missing",
        "rest_circuit_default_418_cooldown_sec",
        "rest_circuit_default_429_cooldown_sec",
        "market_snapshot_cache_first_enabled",
        "market_snapshot_cache_ttl_sec",
        "market_snapshot_cache_min_coverage_ratio",
        "market_snapshot_fail_closed_on_circuit_open",
        "rest_budget_preflight_enabled",
        "rest_budget_min_remaining_weight",
    }
    clean: dict[str, Any] = {}
    for key, value in (values or {}).items():
        if key not in allowed:
            raise ApiServiceError("rest_safety_key_invalid", "unsupported rest safety config key", {"key": key})
        if key.endswith("_sec"):
            intval = int(value)
            if intval <= 0:
                raise ApiServiceError("rest_safety_value_invalid", "seconds must be positive", {"key": key, "value": value})
            clean[key] = intval
        elif key in {
            "exchange_info_cache_first_enabled",
            "exchange_info_allow_cache_on_429_418",
            "exchange_info_fail_if_cache_missing",
            "market_snapshot_cache_first_enabled",
            "market_snapshot_fail_closed_on_circuit_open",
            "rest_budget_preflight_enabled",
        }:
            clean[key] = bool(value)
        elif key.endswith("_ratio"):
            fval = float(value)
            if not 0.0 <= fval <= 1.0:
                raise ApiServiceError("rest_safety_value_invalid", "ratio must be between 0 and 1", {"key": key, "value": value})
            clean[key] = fval
        elif key.endswith("_weight"):
            intval = int(value)
            if intval < 0:
                raise ApiServiceError("rest_safety_value_invalid", "weight must be non-negative", {"key": key, "value": value})
            clean[key] = intval
        else:
            text = str(value or "").strip()
            if not text:
                raise ApiServiceError("rest_safety_value_invalid", "string value cannot be empty", {"key": key})
            clean[key] = text
    cfg_path = package_root() / "config" / "light_snapshot_fetch.yaml"
    try:
        doc = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except OSError as exc:
        raise ApiServiceError("config_read_failed", "cannot read light snapshot config", {"path": str(cfg_path)}) from exc
    af = doc.setdefault("async_fetch", {})
    if not isinstance(af, dict):
        raise ApiServiceError("config_shape_invalid", "async_fetch config must be an object", {"path": str(cfg_path)})
    af.update(clean)
    try:
        data = yaml.safe_dump(doc, allow_unicode=True, sort_keys=False).encode("utf-8")
        write_file_atomic(cfg_path, data)
    except OSError as exc:
        raise ApiServiceError("config_write_failed", "cannot write light snapshot config", {"path": str(cfg_path)}) from exc
    return rest_safety_config_payload()


def exchange_info_cache_refresh_payload() -> dict[str, Any]:
    circuit = read_rest_circuit(PROJECT_ROOT)
    if circuit.get("rest_circuit_state") == "open":
        raise ApiServiceError("rest_circuit_open", "manual refresh blocked while REST circuit is open", circuit)
    return {
        "status": "skipped",
        "reason": "manual_refresh_not_implemented_in_api",
        "rest_circuit_state": circuit.get("rest_circuit_state"),
    }


def run_cli(
    args: list[str],
    *,
    background: bool = False,
    register_background: bool = True,
    background_log_name: str = "api_strategy_pipeline.log",
) -> dict[str, Any]:
    command = [sys.executable, "-m", "laoma_signal_engine.cli", *args]
    if background:
        log_path = PROJECT_ROOT / "DATA" / "logs" / background_log_name
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "ab") as log_file:
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
            proc = subprocess.Popen(
                command,
                cwd=str(PROJECT_ROOT),
                stdout=log_file,
                stderr=log_file,
                stdin=subprocess.DEVNULL,
                creationflags=creationflags,
            )
        if register_background:
            API_PIPELINE_PID_PATH.parent.mkdir(parents=True, exist_ok=True)
            write_json_atomic(
                API_PIPELINE_PID_PATH,
                {
                    "source": "api_pipeline_runner",
                    "pid": proc.pid,
                    "started_at": to_iso_z(utc_now()),
                    "command": command,
                    "log_path": str(log_path),
                    "mode": _arg_value(args, "--mode"),
                    "line": _arg_value(args, "--line"),
                    "lines": _arg_value(args, "--lines"),
                    "interval_sec": _arg_value(args, "--interval-sec"),
                    "status": "started",
                },
            )
        return {"status": "started", "pid": proc.pid, "log_path": str(log_path), "command": command}
    completed = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )
    parsed_stdout = None
    if completed.stdout.strip():
        try:
            parsed_stdout = json.loads(completed.stdout.strip())
        except json.JSONDecodeError:
            parsed_stdout = None
    return {
        "status": "completed" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "payload": parsed_stdout,
        "stderr": completed.stderr.strip(),
        "command": command,
    }


def _arg_value(args: list[str], flag: str) -> str | None:
    try:
        idx = args.index(flag)
    except ValueError:
        return None
    if idx + 1 >= len(args):
        return None
    return str(args[idx + 1])


def _is_strategy_pipeline_registry(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    command = payload.get("command")
    if isinstance(command, list):
        return any(str(part) == "run-strategy-pipeline" for part in command)
    if isinstance(command, str):
        return "run-strategy-pipeline" in command
    mode = payload.get("mode")
    return str(mode or "").strip() in {"once", "interval"}


def _pid_running(pid: int | None) -> bool:
    return runtime_health.pid_running(pid)


def _resolve_project_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _json_pid(path: Path) -> int | None:
    data = _read_optional_raw_json(path)
    if not isinstance(data, dict):
        return None
    try:
        return int(data.get("pid"))
    except (TypeError, ValueError):
        return None


def _iso_from_payload(payload: dict[str, Any] | None, *keys: str) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _age_sec(iso_text: str | None) -> int | None:
    if not iso_text:
        return None
    try:
        return max(0, int((utc_now() - parse_iso_z(iso_text)).total_seconds()))
    except Exception:
        return None


def _runtime_config() -> dict[str, Any]:
    cfg = load_yaml_config()
    raw = cfg.get("project_runtime") if isinstance(cfg.get("project_runtime"), dict) else {}
    watchdog = raw.get("run_cycle_watchdog") if isinstance(raw.get("run_cycle_watchdog"), dict) else {}
    return {
        "autostart_enabled": bool(raw.get("autostart_enabled", True)),
        "autostart_micro_daemon": bool(raw.get("autostart_micro_daemon", True)),
        "autostart_paper_daemon": bool(raw.get("autostart_paper_daemon", True)),
        "autostart_snapshot_daemon": bool(raw.get("autostart_snapshot_daemon", True)),
        "startup_grace_sec": int(raw.get("startup_grace_sec", 10)),
        "heartbeat_stale_sec": int(raw.get("heartbeat_stale_sec", 180)),
        "restart_on_stale": bool(raw.get("restart_on_stale", False)),
        "daemon_health_check_interval_sec": int(raw.get("daemon_health_check_interval_sec", 30)),
        "restart_cooldown_sec": int(raw.get("restart_cooldown_sec", 120)),
        "max_restart_attempts_per_hour": int(raw.get("max_restart_attempts_per_hour", 5)),
        "restart_micro_daemon_on_stale": bool(raw.get("restart_micro_daemon_on_stale", True)),
        "restart_paper_daemon_on_stale": bool(raw.get("restart_paper_daemon_on_stale", True)),
        "stop_children_on_api_shutdown": bool(raw.get("stop_children_on_api_shutdown", False)),
        "run_cycle_watchdog": {
            "enabled": bool(watchdog.get("enabled", True)),
            "heartbeat_interval_sec": int(watchdog.get("heartbeat_interval_sec", 15)),
            "stale_after_sec": int(watchdog.get("stale_after_sec", 90)),
            "long_stage_grace_sec": int(watchdog.get("long_stage_grace_sec", 180)),
            "interval_wait_grace_sec": int(watchdog.get("interval_wait_grace_sec", 30)),
            "fail_on_missing_pid": bool(watchdog.get("fail_on_missing_pid", True)),
            "warn_on_inactive_active_job_residue": bool(watchdog.get("warn_on_inactive_active_job_residue", True)),
        },
    }


def _read_optional_raw_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = read_json_object(path)
    except (OSError, ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _strategy_lock_payload() -> dict[str, Any] | None:
    cfg = load_yaml_config()
    lock_path = PROJECT_ROOT / str((cfg.get("strategy_pipeline") or {}).get("lock_path", "DATA/runtime/strategy_pipeline.lock"))
    payload = _read_optional_raw_json(lock_path)
    if payload is not None:
        payload["path"] = str(lock_path)
    return payload


def _lock_is_stale(lock: dict[str, Any] | None) -> bool:
    if not lock:
        return False
    pid_int = _lock_owner_pid(lock)
    if pid_int and not _pid_running(pid_int):
        return True
    expires_at = lock.get("expires_at")
    if isinstance(expires_at, str):
        try:
            return expires_at <= to_iso_z(utc_now())
        except Exception:
            return False
    return False


def _lock_owner_pid(lock: dict[str, Any] | None) -> int | None:
    if not lock:
        return None
    pid = lock.get("lock_owner_pid")
    try:
        return int(pid)
    except (TypeError, ValueError):
        return None


def _lock_owner_pid_running(lock: dict[str, Any] | None) -> bool:
    pid = _lock_owner_pid(lock)
    return bool(pid and _pid_running(pid))


def _cleanup_stale_strategy_lock(*, mode: str = "run_start") -> dict[str, Any]:
    lock = _strategy_lock_payload()
    result: dict[str, Any] = {
        "strategy_pipeline_lock": "missing",
        "mode": mode,
        "reason_codes": [],
        "reconcile_action": "none",
    }
    if not lock:
        return result
    result["lock"] = lock
    state = inspect_scheduler_lock(Path(str(lock.get("path"))))
    result["lock_state"] = state
    if _lock_owner_pid_running(lock):
        result.update(
            {
                "strategy_pipeline_lock": "kept_alive",
                "reconcile_action": "blocked_alive_pid",
                "reason_codes": ["pipeline_lock_alive_busy"],
            },
        )
        return result
    if not _lock_is_stale(lock):
        result.update({"strategy_pipeline_lock": "kept_fresh", "reconcile_action": "none"})
        return result
    if _lock_owner_pid_running(lock):
        result.update(
            {
                "strategy_pipeline_lock": "kept_alive",
                "reconcile_action": "blocked_alive_pid",
                "reason_codes": ["pipeline_lock_alive_busy"],
            },
        )
        return result
    path = Path(str(lock.get("path")))
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        result.update(
            {
                "strategy_pipeline_lock": "failed",
                "reconcile_action": "cleanup_failed",
                "reason_codes": ["pipeline_lock_recovery_failed"],
                "error": str(exc),
            },
        )
        return result
    reason_codes = list(state.get("reason_codes") or [])
    if "pipeline_lock_stale_dead_pid" not in reason_codes:
        reason_codes.append("pipeline_lock_stale_dead_pid")
    reason_codes.append("pipeline_lock_stale_stop_cleanup" if mode == "stop" else "pipeline_lock_stale_auto_recovered")
    result.update(
        {
            "strategy_pipeline_lock": "removed_stale",
            "reconcile_action": "stop_cleanup" if mode == "stop" else "auto_recovered",
            "reason_codes": reason_codes,
            "removed_lock": lock,
        },
    )
    return result


def micro_daemon_action(action: str) -> dict[str, Any]:
    return run_cli(["micro-daemon", action, "--project-root", str(PROJECT_ROOT), "--stdout-json"])


def snapshot_daemon_action(action: str) -> dict[str, Any]:
    if action == "status":
        return step15_daemon_health_payload()
    if action == "start":
        health = step15_daemon_health_payload()
        if health.get("daemon_status") == "running" and health.get("watchdog_status") == "healthy":
            return {"source": "snapshot_daemon", "action": "start", "status": "already_running", "health": health}
        result = run_cli(
            ["snapshot-daemon", "run", "--project-root", str(PROJECT_ROOT), "--stdout-json"],
            background=True,
            register_background=False,
            background_log_name="step15_snapshot_daemon.log",
        )
        return {"source": "snapshot_daemon", "action": "start", **result}
    if action == "stop":
        health = step15_daemon_health_payload()
        pid = health.get("pid")
        try:
            pid_int = int(pid)
        except (TypeError, ValueError):
            pid_int = -1
        if pid_int > 0:
            try:
                os.kill(pid_int, 15)
            except OSError:
                pass
        return {"source": "snapshot_daemon", "action": "stop", "status": "stopped" if pid_int > 0 else "not_running", "pid": pid_int if pid_int > 0 else None}
    raise ApiServiceError("snapshot_daemon_action_invalid", "unsupported snapshot daemon action", {"action": action})


def _selected_pipeline_lines(req: PipelineRunRequest) -> list[str]:
    raw = req.lines if req.lines is not None else [req.line]
    if any(str(item) == "all" for item in raw):
        return list(STRATEGY_LINES_ORDERED)
    selected: list[str] = []
    for item in raw:
        value = str(item).strip()
        if value in STRATEGY_LINE_KEYS and value not in selected:
            selected.append(value)
    if not selected:
        raise ApiServiceError(
            "pipeline_no_strategy_selected",
            "at least one strategy line must be selected",
            {"lines": raw},
        )
    return [line for line in STRATEGY_LINES_ORDERED if line in selected]


def _pipeline_line_budget_sec(pipeline_cfg: dict[str, Any], line: str) -> int:
    base = max(60, _safe_int(pipeline_cfg.get("interval_sec"), 300))
    buffer_sec = max(0, _safe_int(pipeline_cfg.get("post_stage_buffer_sec"), 60))
    micro_cfg = pipeline_cfg.get("micro") if isinstance(pipeline_cfg.get("micro"), dict) else {}
    if line == "micro_fast":
        return max(base, _safe_int(micro_cfg.get("wait_fast_sec"), 300) + buffer_sec)
    if line == "micro_full":
        return max(base, _safe_int(micro_cfg.get("wait_full_sec"), 1200) + buffer_sec)
    return base


def _effective_pipeline_interval_sec(selected_lines: list[str], requested_interval_sec: int) -> tuple[int, dict[str, int]]:
    pipeline_cfg = load_yaml_config().get("strategy_pipeline") or {}
    budgets = {line: _pipeline_line_budget_sec(pipeline_cfg, line) for line in selected_lines}
    return int(requested_interval_sec), budgets


def run_pipeline(req: PipelineRunRequest) -> dict[str, Any]:
    lock_cleanup = _cleanup_stale_strategy_lock(mode="run_start")
    if lock_cleanup.get("strategy_pipeline_lock") == "failed":
        raise ApiServiceError(
            "pipeline_lock_recovery_failed",
            "failed to recover stale pipeline lock",
            {"cleanup": lock_cleanup},
        )
    active = pipeline_status()
    controls = active.get("run_controls") if isinstance(active.get("run_controls"), dict) else {}
    if active.get("job_running") or (active.get("progress") or {}).get("status") == "running":
        raise ApiServiceError(
            "pipeline_already_running",
            "pipeline is already running",
            {
                "active_job": active.get("active_job"),
                "current_stage": (active.get("progress") or {}).get("current_stage"),
                "run_controls": controls,
                "cleanup": lock_cleanup,
            },
        )
    control_key = "can_run_cycle" if req.mode == "interval" else "can_run_once"
    if controls.get(control_key) is False:
        disabled_reason = str(controls.get("disabled_reason") or "pipeline_run_disabled")
        error_code = "pipeline_interval_active" if disabled_reason == "interval_cycle_waiting" else disabled_reason
        raise ApiServiceError(
            error_code,
            "pipeline run is disabled by current run_controls contract",
            {
                "requested_mode": req.mode,
                "control_key": control_key,
                "disabled_reason": disabled_reason,
                "active_job": active.get("active_job"),
                "active_interval": active.get("active_interval"),
                "display_state": active.get("display_state"),
                "display_run_id": active.get("display_run_id"),
                "next_cycle_eta_sec": active.get("next_cycle_eta_sec"),
                "run_controls": controls,
            },
        )
    warmup = controls.get("snapshot_warmup") if isinstance(controls.get("snapshot_warmup"), dict) else snapshot_warmup_payload()
    if not warmup.get("ready"):
        raise ApiServiceError(
            "snapshot_warmup_not_ready",
            "snapshot warmup is not ready for run once / run cycle",
            {"snapshot_warmup": warmup, "run_controls": controls},
        )
    step2_readiness = controls.get("step2_readiness") if isinstance(controls.get("step2_readiness"), dict) else step2_readiness_payload()
    selected_lines = _selected_pipeline_lines(req)
    requested_interval_sec = req.interval_sec
    if requested_interval_sec is None:
        requested_interval_sec = max(1, _safe_int((load_yaml_config().get("strategy_pipeline") or {}).get("interval_sec"), 300))
    effective_interval_sec, line_budgets = _effective_pipeline_interval_sec(
        selected_lines,
        requested_interval_sec,
    )
    args = [
        "run-strategy-pipeline",
        "--project-root",
        str(PROJECT_ROOT),
        "--lines",
        ",".join(selected_lines),
        "--mode",
        req.mode,
        "--stdout-json",
    ]
    if requested_interval_sec is not None:
        args.extend(["--interval-sec", str(requested_interval_sec)])
    if req.max_cycles is not None:
        args.extend(["--max-cycles", str(req.max_cycles)])
    if req.force_universe:
        args.append("--force-universe")
    if req.skip_micro_wait:
        args.append("--skip-micro-wait")
    if req.skip_market_context:
        args.append("--skip-market-context")
    if req.skip_abc_audit:
        args.append("--skip-abc-audit")
    if req.skip_json_stage_audit:
        args.append("--skip-json-stage-audit")
    if req.skip_aggregate_final_decisions:
        args.append("--skip-aggregate-final-decisions")
    result = run_cli(args, background=True)
    result.update(
        {
            "mode": req.mode,
            "line": req.line,
            "selected_lines": selected_lines,
            "requested_interval_sec": requested_interval_sec,
            "effective_interval_sec": effective_interval_sec,
            "post_run_cooldown_sec": effective_interval_sec,
            "interval_semantics": "post_run_cooldown",
            "line_runtime_budgets": line_budgets,
            "interval_sec": effective_interval_sec,
            "stale_lock_removed": lock_cleanup.get("removed_lock"),
            "cleanup": lock_cleanup,
            "step2_readiness": step2_readiness,
        },
    )
    return result


def stop_pipeline() -> dict[str, Any]:
    status = pipeline_status()
    payload = read_json_object(API_PIPELINE_PID_PATH) if API_PIPELINE_PID_PATH.exists() else {}
    active_job = status.get("active_job") if isinstance(status.get("active_job"), dict) else {}
    pid = int((active_job or payload).get("pid") or -1)
    if pid <= 0:
        cleanup = _cleanup_stale_strategy_lock(mode="stop")
        return {
            "status": "stopped" if cleanup.get("strategy_pipeline_lock") == "removed_stale" else "not_running",
            "cleanup": cleanup,
        }
    if pid > 0:
        try:
            os.kill(pid, 15)
        except OSError:
            pass
    stopped_payload = dict(active_job or payload or {})
    stopped_payload.update({"status": "stopped", "stopped_at": to_iso_z(utc_now())})
    write_json_atomic(API_PIPELINE_PID_PATH, stopped_payload)
    cleanup = _cleanup_stale_strategy_lock(mode="stop")
    return {"status": "stopped", "pid": pid, "source": status.get("job_status_source"), "cleanup": cleanup}


def step2_readiness_payload() -> dict[str, Any]:
    try:
        current = build_step2_current_freshness(project_root=PROJECT_ROOT)
    except Exception as exc:
        return {
            "schema_version": "STEP1.77_step2_readiness_v1",
            "source": "step2_current_freshness",
            "status": "unknown",
            "ready": False,
            "reason_codes": ["step2_readiness_exception"],
            "error": str(exc),
        }
    freshness = str(current.get("current_freshness") or "unknown")
    ages = [
        int(v)
        for v in (
            current.get("watch_output_age_sec"),
            current.get("strong_output_age_sec"),
            current.get("current_input_snapshot_age_sec"),
        )
        if isinstance(v, (int, float))
    ]
    max_age = int(current.get("max_age_sec") or 300)
    age = max(ages) if ages else None
    remaining = None if age is None else max_age - age
    return {
        "schema_version": "STEP1.77_step2_readiness_v1",
        "source": "step2_current_freshness",
        "status": "ready" if freshness == "fresh" else "stale",
        "ready": freshness == "fresh",
        "current_freshness": freshness,
        "step2_age_sec": age,
        "step2_max_age_sec": max_age,
        "step2_remaining_sec": remaining,
        "reason_codes": [str(x) for x in current.get("reason_codes") or []],
        "freshness": current,
        "run_control_policy": "observe_only_pipeline_can_renew",
    }


def pipeline_status() -> dict[str, Any]:
    report = read_optional_json_file(PROJECT_ROOT / "DATA" / "reports" / "latest_strategy_pipeline_report.json")
    progress_doc = read_optional_json_file(PROJECT_ROOT / "DATA" / "runtime" / "strategy_pipeline_progress.json")
    api_pid_path = PROJECT_ROOT / "DATA" / "runtime" / "api_pipeline_interval.pid"
    pipeline_cfg = load_yaml_config().get("strategy_pipeline") or {}
    interval_sec = max(1, _safe_int(pipeline_cfg.get("interval_sec"), 300))
    interval_proc = read_optional_json_file(api_pid_path)
    active_job = interval_proc["data"] if interval_proc else None
    if isinstance(active_job, dict) and not _is_strategy_pipeline_registry(active_job):
        active_job = None
        try:
            api_pid_path.unlink(missing_ok=True)
        except OSError:
            pass
        interval_proc = None
    registry_pid = active_job.get("pid") if isinstance(active_job, dict) else None
    try:
        pid_int = int(registry_pid) if registry_pid is not None else None
    except (TypeError, ValueError):
        pid_int = None
    lock = _strategy_lock_payload()
    lock_stale = _lock_is_stale(lock)
    lock_pid = _lock_owner_pid(lock)
    lock_pid_running = bool(lock_pid and _pid_running(lock_pid))
    lock_reason_codes: list[str] = []
    if isinstance(lock, dict):
        if lock_stale and lock_pid and not lock_pid_running:
            lock_reason_codes.append("pipeline_lock_stale_dead_pid")
        if lock_stale and lock_pid_running:
            lock_reason_codes.append("pipeline_lock_expired")
        if lock_pid_running and not lock_stale:
            lock_reason_codes.append("pipeline_lock_alive_busy")
    registry_pid_running = _pid_running(pid_int)
    recovered_from: str | None = None
    recovered = False
    progress_data = progress_doc["data"] if progress_doc else None
    progress_running = isinstance(progress_data, dict) and progress_data.get("status") == "running"
    if lock and lock_pid_running and not lock_stale and (progress_running or not registry_pid_running):
        active_job = {
            "source": "api_pipeline_runner_recovered" if not isinstance(active_job, dict) else "api_pipeline_runner_lock_authority",
            "pid": lock_pid,
            "started_at": lock.get("started_at"),
            "run_id": lock.get("run_id"),
            "cycle_id": lock.get("cycle_id"),
            "mode": (progress_data or {}).get("mode") or "unknown",
            "line": lock.get("line") or (progress_data or {}).get("line") or "all",
            "status": "running_recovered_from_lock",
        }
        pid_int = lock_pid
        recovered_from = "lock" if not interval_proc else "lock_owner"
        recovered = True
    elif not isinstance(active_job, dict) and isinstance(progress_data, dict):
        progress_pid = _lock_owner_pid(lock) if lock else None
        if progress_pid and _pid_running(progress_pid):
            active_job = {
                "source": "api_pipeline_runner_recovered",
                "pid": progress_pid,
                "started_at": progress_data.get("started_at"),
                "run_id": progress_data.get("run_id"),
                "cycle_id": progress_data.get("cycle_id"),
                "mode": progress_data.get("mode") or "unknown",
                "line": progress_data.get("line") or "all",
                "status": "running_recovered",
            }
            pid_int = progress_pid
            recovered_from = "progress"
            recovered = True
    job_running = _pid_running(pid_int)
    lock_stale_but_pid_running = bool(lock_stale and lock_pid_running)
    if job_running and not lock and _latest_report_finished_after_active_job(report["data"] if report else None, active_job):
        job_running = False
    finalize_action = _finalize_inactive_pipeline_job(
        api_pid_path,
        active_job,
        latest_report=report["data"] if report else None,
        lock=lock,
        job_running=job_running,
    )
    if finalize_action:
        interval_proc = read_optional_json_file(api_pid_path)
        active_job = interval_proc["data"] if interval_proc else active_job
    latest_report = report["data"] if report else None
    selected_for_interval = (
        latest_report.get("selected_lines")
        if isinstance(latest_report, dict) and isinstance(latest_report.get("selected_lines"), list)
        else None
    )
    if not selected_for_interval and isinstance(active_job, dict):
        raw_lines = active_job.get("lines")
        if isinstance(raw_lines, str) and raw_lines:
            selected_for_interval = [x.strip() for x in raw_lines.split(",") if x.strip()]
    if selected_for_interval:
        interval_sec = max(
            interval_sec,
            _safe_int((latest_report or {}).get("post_run_cooldown_sec"), 0),
            _safe_int((latest_report or {}).get("effective_interval_sec"), 0),
            _safe_int((active_job or {}).get("post_run_cooldown_sec"), 0),
            _safe_int((active_job or {}).get("effective_interval_sec"), 0),
        )
    interval_waiting = _is_interval_waiting_state(
        active_job=active_job,
        latest_report=latest_report,
        lock=lock,
        job_running=job_running,
        registry_pid_running=registry_pid_running,
        interval_sec=interval_sec,
    )
    interval_waiting_job: dict[str, Any] | None = None
    if interval_waiting:
        interval_waiting_job = _sync_interval_waiting_registry(
            api_pid_path,
            active_job,
            latest_report=latest_report,
            interval_sec=interval_sec,
        )
        interval_proc = read_optional_json_file(api_pid_path)
        active_job = interval_proc["data"] if interval_proc else interval_waiting_job
    progress_payload = pipeline_progress_payload(
        active_job=active_job,
        job_running=job_running,
        lock=lock,
        latest_report=latest_report,
        progress_doc=progress_data,
    )
    display_state = "running" if job_running else ("interval_waiting" if interval_waiting else ("completed_or_idle" if latest_report else "idle"))
    display_run_id = (
        (active_job or {}).get("run_id")
        if job_running
        else ((latest_report or {}).get("run_id") or progress_payload.get("run_id"))
    )
    snapshot_warmup = snapshot_warmup_payload()
    warmup_ready = bool(snapshot_warmup.get("ready"))
    step2_readiness = step2_readiness_payload()
    disabled_reason = (
        "pipeline_already_running"
        if job_running
        else ("interval_cycle_waiting" if interval_waiting else (None if warmup_ready else "snapshot_warmup_not_ready"))
    )
    run_controls = {
        "can_run_once": not job_running and not interval_waiting and warmup_ready,
        "can_run_cycle": not job_running and not interval_waiting and warmup_ready,
        "can_stop": job_running or interval_waiting,
        "disabled_reason": disabled_reason,
        "active_mode": (active_job or {}).get("mode") if (job_running or interval_waiting) else None,
        "snapshot_warmup": snapshot_warmup,
        "step2_readiness": step2_readiness,
    }
    return {
        "active_interval": active_job if (job_running or interval_waiting) else None,
        "active_job": active_job if job_running else None,
        "job_running": job_running,
        "cycle_enabled": interval_waiting or str(pipeline_cfg.get("mode") or "once") == "interval",
        "display_state": display_state,
        "display_run_id": display_run_id,
        "next_cycle_eta_sec": _next_cycle_eta_sec(latest_report, interval_sec) if interval_waiting else None,
            "selected_lines": progress_payload.get("selected_lines") or (latest_report or {}).get("selected_lines") or (active_job or {}).get("selected_lines"),
            "skipped_lines": progress_payload.get("skipped_lines") or (latest_report or {}).get("skipped_lines"),
            "requested_interval_sec": progress_payload.get("requested_interval_sec") or (latest_report or {}).get("requested_interval_sec") or (active_job or {}).get("requested_interval_sec"),
            "effective_interval_sec": progress_payload.get("effective_interval_sec") or (latest_report or {}).get("effective_interval_sec") or (active_job or {}).get("effective_interval_sec") or interval_sec,
            "post_run_cooldown_sec": progress_payload.get("post_run_cooldown_sec") or (latest_report or {}).get("post_run_cooldown_sec") or (active_job or {}).get("post_run_cooldown_sec") or interval_sec,
            "interval_semantics": progress_payload.get("interval_semantics") or (latest_report or {}).get("interval_semantics") or "post_run_cooldown",
            "line_runtime_budgets": progress_payload.get("line_runtime_budgets") or (latest_report or {}).get("line_runtime_budgets") or {},
        "last_completed_job": latest_report if interval_waiting else None,
        "job_status_source": "lock_authority" if recovered_from == "lock_owner" else ("recovered" if recovered else ("registry" if interval_proc else "none")),
        "active_job_recovered": recovered,
        "registry_health": {
            "registry_exists": bool(interval_proc),
            "registry_pid_running": registry_pid_running,
            "lock_exists": bool(lock),
            "lock_pid_running": lock_pid_running,
            "lock_stale": lock_stale,
            "pid_running": job_running,
            "progress_exists": bool(progress_doc),
            "reconcile_action": finalize_action
            or ("interval_waiting_registry_synced" if interval_waiting else None)
            or ("promoted_lock_owner" if recovered_from == "lock_owner" else (f"recovered_from_{recovered_from}" if recovered_from else "none")),
            "reconcile_reason_codes": lock_reason_codes,
        },
        "lock": lock,
        "lock_stale": lock_stale,
        "lock_stale_but_pid_running": lock_stale_but_pid_running,
        "latest_report": latest_report,
        "step2_readiness": step2_readiness,
        "progress": progress_payload,
        "run_controls": run_controls,
    }


def _active_job_lite_from_registry() -> dict[str, Any] | None:
    api_pid_path = PROJECT_ROOT / "DATA" / "runtime" / "api_pipeline_interval.pid"
    registry = read_optional_json_file(api_pid_path)
    active_job = registry["data"] if registry else None
    if not isinstance(active_job, dict) or not _is_strategy_pipeline_registry(active_job):
        return None
    pid = active_job.get("pid")
    try:
        pid_int = int(pid) if pid is not None else None
    except (TypeError, ValueError):
        pid_int = None
    return {
        "source": active_job.get("source"),
        "pid": pid_int,
        "pid_running": _pid_running(pid_int),
        "started_at": active_job.get("started_at"),
        "run_id": active_job.get("run_id"),
        "cycle_id": active_job.get("cycle_id"),
        "mode": active_job.get("mode"),
        "line": active_job.get("line"),
        "selected_lines": active_job.get("selected_lines") or active_job.get("lines"),
        "status": active_job.get("status"),
    }


PIPELINE_LINE_PROGRESS_LITE_KEYS = (
    "percent",
    "stage",
    "status",
    "done",
    "selected",
    "skipped",
    "run_id",
    "cycle_id",
    "output_fresh",
    "line_exec_status",
    "line_lifecycle_status",
    "trade_plan_allowed",
    "output_run_id",
    "output_cycle_id",
    "output_generated_at",
    "stale_output_reason",
    "effective_executable_count",
    "terminal_state",
    "terminal_reason",
    "stage_status_class",
    "business_terminal_reason",
    "technical_failure_reason",
    "technical_blocked",
    "technical_block_reason",
    "recovery",
    "wait_result",
    "terminalized_symbol_count",
    "unfinished_symbol_count",
    "consumable_symbol_count",
    "rejected_count",
    "not_ready_count",
    "timeout_count",
    "observing_count",
    "symbol_counts",
)


def _pipeline_line_progress_lite(line_doc: Any) -> dict[str, Any]:
    if not isinstance(line_doc, dict):
        return {}
    return {
        key: line_doc.get(key)
        for key in PIPELINE_LINE_PROGRESS_LITE_KEYS
        if key in line_doc and line_doc.get(key) is not None
    }


def pipeline_status_lite() -> dict[str, Any]:
    latest_ref = read_optional_json_file(PROJECT_ROOT / "DATA" / "reports" / "latest_strategy_pipeline_report.json")
    progress_ref = read_optional_json_file(PROJECT_ROOT / "DATA" / "runtime" / "strategy_pipeline_progress.json")
    latest_report = latest_ref["data"] if latest_ref else {}
    progress_data = progress_ref["data"] if progress_ref else {}
    if not isinstance(latest_report, dict):
        latest_report = {}
    if not isinstance(progress_data, dict):
        progress_data = {}
    active_job = _active_job_lite_from_registry()
    lock = _strategy_lock_payload()
    lock_pid = _lock_owner_pid(lock)
    lock_pid_running = bool(lock_pid and _pid_running(lock_pid))
    registry_running = bool(active_job and active_job.get("pid_running"))
    progress_running = progress_data.get("status") == "running"
    job_running = bool(registry_running or (lock_pid_running and progress_running))
    pipeline_cfg = load_yaml_config().get("strategy_pipeline") or {}
    interval_sec = max(1, _safe_int(pipeline_cfg.get("interval_sec"), 300))
    interval_waiting = _is_interval_waiting_state(
        active_job=active_job,
        latest_report=latest_report,
        lock=lock,
        job_running=job_running,
        registry_pid_running=registry_running,
        interval_sec=interval_sec,
    )
    display_state = "running" if job_running else ("interval_waiting" if interval_waiting else ("completed_or_idle" if latest_report else "idle"))
    display_run_id = (
        (active_job or {}).get("run_id")
        if job_running
        else (latest_report.get("run_id") or progress_data.get("run_id"))
    )
    snapshot_warmup = snapshot_warmup_payload()
    warmup_ready = bool(snapshot_warmup.get("ready"))
    disabled_reason = (
        "pipeline_already_running"
        if job_running
        else ("interval_cycle_waiting" if interval_waiting else (None if warmup_ready else "snapshot_warmup_not_ready"))
    )
    selected_lines = (
        progress_data.get("selected_lines")
        or latest_report.get("selected_lines")
        or (active_job or {}).get("selected_lines")
    )
    raw_progress_status = progress_data.get("status")
    stale_progress_running = bool(raw_progress_status == "running" and not job_running and not interval_waiting)
    progress_lite = {
        "status": "stopped" if stale_progress_running else raw_progress_status,
        "raw_status": raw_progress_status if stale_progress_running else None,
        "overall_percent": max(0, min(100, int(progress_data.get("overall_percent") or 0))),
        "current_stage": progress_data.get("current_stage"),
        "current_line": progress_data.get("current_line"),
        "run_id": progress_data.get("run_id") or display_run_id,
        "cycle_id": progress_data.get("cycle_id") or latest_report.get("cycle_id"),
        "mode": progress_data.get("mode") or (active_job or {}).get("mode"),
        "line": progress_data.get("line") or (active_job or {}).get("line"),
        "selected_lines": selected_lines,
        "stage": progress_data.get("stage"),
        "lines": {
            line: _pipeline_line_progress_lite(doc)
            for line, doc in ((progress_data.get("lines") or {}).items() if isinstance(progress_data.get("lines"), dict) else [])
        },
        "updated_at": progress_data.get("updated_at") or progress_data.get("generated_at"),
        "reason_codes": ["stale_progress_running_normalized"] if stale_progress_running else [],
    }
    run_controls = {
        "can_run_once": not job_running and not interval_waiting and warmup_ready,
        "can_run_cycle": not job_running and not interval_waiting and warmup_ready,
        "can_stop": job_running or interval_waiting,
        "disabled_reason": disabled_reason,
        "active_mode": (active_job or {}).get("mode") if (job_running or interval_waiting) else None,
        "snapshot_warmup": {
            "ready": warmup_ready,
            "status": snapshot_warmup.get("status"),
            "reason_codes": snapshot_warmup.get("reason_codes") or [],
        },
    }
    return {
        "schema_version": "12.59",
        "payload_scope": "lite",
        "active_interval": active_job if (job_running or interval_waiting) else None,
        "active_job": active_job if job_running else None,
        "job_running": job_running,
        "cycle_enabled": interval_waiting or str(pipeline_cfg.get("mode") or "once") == "interval",
        "display_state": display_state,
        "display_run_id": display_run_id,
        "next_cycle_eta_sec": _next_cycle_eta_sec(latest_report, interval_sec) if interval_waiting else None,
        "errors": [],
        "progress": progress_lite,
        "run_controls": run_controls,
        "registry_health": {
            "registry_pid_running": registry_running,
            "lock_exists": bool(lock),
            "lock_pid_running": lock_pid_running,
            "lock_stale": _lock_is_stale(lock),
        },
        "latest_report_summary": {
            "run_id": latest_report.get("run_id"),
            "cycle_id": latest_report.get("cycle_id"),
            "status": latest_report.get("status"),
            "generated_at": latest_report.get("generated_at"),
            "finished_at": latest_report.get("finished_at"),
            "selected_lines": latest_report.get("selected_lines"),
            "skipped_lines": latest_report.get("skipped_lines"),
        },
        "generated_at": to_iso_z(utc_now()),
    }


def _run_cycle_watchdog_latest_path() -> Path:
    return PROJECT_ROOT / "DATA" / "runtime" / "run_cycle_watchdog.json"


def _run_cycle_watchdog_heartbeat_path() -> Path:
    return PROJECT_ROOT / "DATA" / "runtime" / "run_cycle_heartbeat.json"


def _run_cycle_watchdog_events_path() -> Path:
    return PROJECT_ROOT / "DATA" / "logs" / f"run_cycle_watchdog_{utc_now():%Y%m%d}.jsonl"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _report_finished_at(report: dict[str, Any] | None) -> str | None:
    if not isinstance(report, dict):
        return None
    value = report.get("finished_at") or report.get("generated_at")
    return str(value) if value else None


def _next_cycle_eta_sec(report: dict[str, Any] | None, interval_sec: int) -> int | None:
    if not isinstance(report, dict):
        return None
    next_run_at = report.get("next_run_at")
    if isinstance(next_run_at, str) and next_run_at:
        age = _age_sec(next_run_at)
        if age is not None:
            try:
                return max(0, int((parse_iso_z(next_run_at) - utc_now()).total_seconds()))
            except (TypeError, ValueError):
                return None
    finished_at = _report_finished_at(report)
    if not finished_at:
        return None
    try:
        return max(0, int(interval_sec - (utc_now() - parse_iso_z(finished_at)).total_seconds()))
    except (TypeError, ValueError):
        return None


def _is_interval_waiting_state(
    *,
    active_job: dict[str, Any] | None,
    latest_report: dict[str, Any] | None,
    lock: dict[str, Any] | None,
    job_running: bool,
    registry_pid_running: bool,
    interval_sec: int,
) -> bool:
    if job_running or lock or not isinstance(active_job, dict) or not isinstance(latest_report, dict):
        return False
    if str(active_job.get("mode") or latest_report.get("mode") or "") != "interval":
        return False
    if not registry_pid_running:
        return False
    if not latest_report.get("finished_at") and not latest_report.get("generated_at"):
        return False
    return _next_cycle_eta_sec(latest_report, interval_sec) is not None


def _sync_interval_waiting_registry(
    path: Path,
    active_job: dict[str, Any] | None,
    *,
    latest_report: dict[str, Any] | None,
    interval_sec: int,
) -> dict[str, Any] | None:
    if not isinstance(active_job, dict) or not isinstance(latest_report, dict):
        return None
    payload = dict(active_job)
    payload.update(
        {
            "status": "interval_waiting",
            "pid_running": True,
            "last_run_id": latest_report.get("run_id"),
            "last_cycle_id": latest_report.get("cycle_id"),
            "last_finished_at": _report_finished_at(latest_report),
            "next_run_at": latest_report.get("next_run_at"),
            "interval_sec": int(active_job.get("interval_sec") or interval_sec),
            "selected_lines": latest_report.get("selected_lines") or active_job.get("selected_lines"),
            "effective_interval_sec": int(latest_report.get("effective_interval_sec") or active_job.get("effective_interval_sec") or interval_sec),
            "post_run_cooldown_sec": int(latest_report.get("post_run_cooldown_sec") or active_job.get("post_run_cooldown_sec") or latest_report.get("effective_interval_sec") or active_job.get("effective_interval_sec") or interval_sec),
            "interval_semantics": latest_report.get("interval_semantics") or active_job.get("interval_semantics") or "post_run_cooldown",
            "requested_interval_sec": latest_report.get("requested_interval_sec") or active_job.get("requested_interval_sec"),
            "updated_at": to_iso_z(utc_now()),
        },
    )
    write_json_atomic(path, payload)
    return payload


def pipeline_funnel_latest_payload(refresh: bool = True) -> dict[str, Any]:
    return _latest_cross_strategy_funnel_payload(PROJECT_ROOT, refresh=refresh)


def pipeline_funnel_history_payload(limit: int = 50) -> dict[str, Any]:
    return _cross_strategy_funnel_history_payload(PROJECT_ROOT, limit=limit)


def run_cycle_watchdog_health() -> dict[str, Any]:
    runtime_cfg = _runtime_config()
    watchdog_cfg = runtime_cfg.get("run_cycle_watchdog") if isinstance(runtime_cfg.get("run_cycle_watchdog"), dict) else {}
    pipeline_cfg = load_yaml_config().get("strategy_pipeline") or {}
    interval_sec = max(1, _safe_int(pipeline_cfg.get("interval_sec"), 300))
    status = pipeline_status()
    latest_report = status.get("latest_report") if isinstance(status.get("latest_report"), dict) else None
    progress = status.get("progress") if isinstance(status.get("progress"), dict) else {}
    active_job = status.get("active_job") if isinstance(status.get("active_job"), dict) else None
    active_interval = status.get("active_interval") if isinstance(status.get("active_interval"), dict) else None
    interval_owner = active_job or active_interval
    lock = status.get("lock") if isinstance(status.get("lock"), dict) else None
    micro = _micro_daemon_health()
    paper = _paper_daemon_health()

    reason_codes: list[str] = []
    job_running = bool(status.get("job_running"))
    cycle_enabled = str(pipeline_cfg.get("mode") or "once") == "interval" or bool(interval_owner and interval_owner.get("mode") == "interval")
    latest_report_run_id = latest_report.get("run_id") if latest_report else None
    progress_run_id = progress.get("run_id") if isinstance(progress, dict) else None

    lock_age = _age_sec(str(lock.get("heartbeat_at") or lock.get("updated_at") or "")) if lock else None
    progress_age = _age_sec(str(progress.get("updated_at") or "")) if progress else None
    stale_after = _safe_int(watchdog_cfg.get("stale_after_sec"), 90)
    lock_pid_running = bool((status.get("registry_health") or {}).get("lock_pid_running"))
    registry_pid_running = bool((status.get("registry_health") or {}).get("registry_pid_running"))
    pid_running = bool((status.get("registry_health") or {}).get("pid_running"))
    registry_reason_codes = (status.get("registry_health") or {}).get("reconcile_reason_codes")
    if isinstance(registry_reason_codes, list):
        for code in registry_reason_codes:
            code = str(code)
            if code == "pipeline_lock_alive_busy":
                continue
            if code not in reason_codes:
                reason_codes.append(code)

    if job_running:
        scheduler_status = "running"
        job_status = "running"
        display_state = "running"
        display_run_id = progress_run_id or (active_job or {}).get("run_id") or (lock or {}).get("run_id")
        if not pid_running:
            reason_codes.append("job_running_without_pid")
        if lock_age is not None and lock_age > stale_after:
            reason_codes.append("lock_heartbeat_stale")
        if progress_age is not None and progress_age > stale_after:
            reason_codes.append("progress_heartbeat_stale")
    else:
        next_eta = _next_cycle_eta_sec(latest_report, interval_sec)
        if cycle_enabled and next_eta is not None:
            scheduler_status = "interval_waiting"
            display_state = "interval_waiting"
        else:
            scheduler_status = "stopped"
            display_state = "completed_or_idle" if latest_report else "idle"
        job_status = "idle"
        display_run_id = latest_report_run_id or progress_run_id
        if active_job and active_job.get("run_id") and active_job.get("run_id") != latest_report_run_id:
            reason_codes.append("inactive_active_job_residue")

    if micro.get("stale"):
        reason_codes.append("micro_daemon_stale")
    for code in micro.get("reason_codes") or []:
        code = str(code)
        if code and code not in reason_codes:
            reason_codes.append(code)
    if paper.get("stale"):
        reason_codes.append("paper_daemon_stale")
    if micro.get("status") not in {"running", "idle"}:
        reason_codes.append("micro_daemon_not_healthy")
    if paper.get("status") != "running":
        reason_codes.append("paper_daemon_not_healthy")

    fail_reasons = {
        "job_running_without_pid",
        "lock_heartbeat_stale",
        "micro_daemon_not_healthy",
        "paper_daemon_not_healthy",
    }
    if any(code in fail_reasons for code in reason_codes):
        health = "fail"
    elif reason_codes:
        health = "warn"
    else:
        health = "ok"

    payload = {
        "schema_version": "1.0",
        "generated_at": to_iso_z(utc_now()),
        "source": "run_cycle_watchdog",
        "enabled": bool(watchdog_cfg.get("enabled", True)),
        "health": health,
        "scheduler_status": scheduler_status,
        "job_status": job_status,
        "display_state": display_state,
        "display_run_id": display_run_id,
        "cycle_enabled": cycle_enabled,
        "job_running": job_running,
        "run_id": progress_run_id or (active_job or {}).get("run_id") or latest_report_run_id,
        "cycle_id": progress.get("cycle_id") or (active_job or {}).get("cycle_id") or (latest_report or {}).get("cycle_id"),
        "mode": (interval_owner or {}).get("mode") or progress.get("mode") or (latest_report or {}).get("mode"),
        "stage": progress.get("current_stage"),
        "line": progress.get("current_line") or (active_job or {}).get("line"),
        "pid": (interval_owner or {}).get("pid"),
        "pid_running": pid_running,
        "lock_age_sec": lock_age,
        "progress_age_sec": progress_age,
        "latest_report": {
            "run_id": latest_report_run_id,
            "cycle_id": (latest_report or {}).get("cycle_id"),
            "status": (latest_report or {}).get("status"),
            "duration_sec": (latest_report or {}).get("duration_sec"),
            "finished_at": _report_finished_at(latest_report),
        },
        "active_job": active_job if job_running else None,
        "active_interval": active_interval if not job_running else None,
        "next_cycle_eta_sec": _next_cycle_eta_sec(latest_report, interval_sec),
        "micro_daemon": {
            "status": micro.get("status"),
            "heartbeat_age_sec": micro.get("heartbeat_age_sec"),
            "active_targets": micro.get("active_targets"),
            "stale": micro.get("stale"),
        },
        "paper_daemon": {
            "status": paper.get("status"),
            "heartbeat_age_sec": paper.get("heartbeat_age_sec"),
            "active_symbols": paper.get("active_symbols"),
            "stale": paper.get("stale"),
        },
        "watchdog": {
            "lock_age_sec": lock_age,
            "progress_age_sec": progress_age,
            "lock_pid_running": lock_pid_running,
            "registry_pid_running": registry_pid_running,
            "pid_running": pid_running,
            "stale_after_sec": stale_after,
        },
        "reason_codes": reason_codes,
    }
    _run_cycle_watchdog_latest_path().parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(_run_cycle_watchdog_latest_path(), payload)
    write_json_atomic(_run_cycle_watchdog_heartbeat_path(), {
        "schema_version": "1.0",
        "generated_at": payload["generated_at"],
        "health": payload["health"],
        "scheduler_status": payload["scheduler_status"],
        "display_state": payload["display_state"],
        "display_run_id": payload["display_run_id"],
        "reason_codes": payload["reason_codes"],
    })
    _run_cycle_watchdog_events_path().parent.mkdir(parents=True, exist_ok=True)
    with _run_cycle_watchdog_events_path().open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return payload


def _latest_report_finished_after_active_job(report: dict[str, Any] | None, active_job: dict[str, Any] | None) -> bool:
    if not isinstance(report, dict) or not isinstance(active_job, dict):
        return False
    if report.get("status") not in {"ok", "failed", "skipped"}:
        return False
    try:
        finished_at = parse_iso_z(str(report.get("finished_at") or report.get("generated_at") or ""))
        started_at = parse_iso_z(str(active_job.get("started_at") or ""))
    except (TypeError, ValueError):
        return False
    return finished_at >= started_at


def _finalize_inactive_pipeline_job(
    path: Path,
    active_job: dict[str, Any] | None,
    *,
    latest_report: dict[str, Any] | None,
    lock: dict[str, Any] | None,
    job_running: bool,
) -> str | None:
    if job_running or lock or not isinstance(active_job, dict):
        return None
    status = str(active_job.get("status") or "")
    if status in {"completed", "failed", "stopped"}:
        return None
    if status == "started":
        final = dict(active_job)
        if isinstance(latest_report, dict) and latest_report.get("finished_at"):
            final.update(
                {
                    "status": "completed" if latest_report.get("status") in {"ok", "partial", "issues_found"} else "failed",
                    "finished_at": latest_report.get("finished_at"),
                    "exit_status": latest_report.get("status"),
                    "run_id": latest_report.get("run_id") or active_job.get("run_id"),
                    "cycle_id": latest_report.get("cycle_id") or active_job.get("cycle_id"),
                },
            )
            action = "finalized_completed" if final["status"] == "completed" else "finalized_failed"
        else:
            final.update({"status": "failed", "finished_at": to_iso_z(utc_now()), "exit_status": "missing_latest_report"})
            action = "finalized_failed"
        write_json_atomic(path, final)
        return action
    return None


def pipeline_progress_payload(
    *,
    active_job: dict[str, Any] | None,
    job_running: bool,
    lock: dict[str, Any] | None,
    latest_report: dict[str, Any] | None,
    progress_doc: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stage_names = [
        "common_upstream_step1_to_step2_5",
        "assemble_factor_without_micro",
        "wait_micro_ready_micro_fast",
        "assemble_factor_with_micro",
        "apply_trade_plan_micro_fast",
        "audit_trade_plan_lines",
    ]
    current_run_id = (lock or {}).get("run_id") or (progress_doc or {}).get("run_id") or (active_job or {}).get("run_id")
    current_cycle_id = (lock or {}).get("cycle_id") or (progress_doc or {}).get("cycle_id") or (active_job or {}).get("cycle_id")
    if isinstance(progress_doc, dict) and (
        (not current_run_id or progress_doc.get("run_id") == current_run_id)
        and (not current_cycle_id or progress_doc.get("cycle_id") == current_cycle_id)
    ):
        return _normalize_pipeline_progress(progress_doc, job_running=job_running, latest_report=latest_report, lock=lock, active_job=active_job)
    report_stages = latest_report.get("stages") if isinstance(latest_report, dict) else []
    report_matches_current = bool(
        isinstance(latest_report, dict)
        and (not current_run_id or latest_report.get("run_id") == current_run_id)
        and (not current_cycle_id or latest_report.get("cycle_id") == current_cycle_id)
    )
    if job_running and not report_matches_current:
        return _running_unknown_progress(lock=lock, active_job=active_job)
    done_names = {str(row.get("name")) for row in report_stages or [] if row.get("ok")}
    status = "running" if job_running else str((latest_report or {}).get("status") or "idle")
    if job_running:
        overall = 12
        if lock:
            overall = 18
        overall += min(70, len(done_names) * 12)
    elif latest_report and latest_report.get("status") == "ok":
        overall = 100
    elif latest_report and latest_report.get("status") == "failed":
        overall = min(95, len(done_names) * 12)
    else:
        overall = 0
    lines: dict[str, dict[str, Any]] = {}
    selected_lines = latest_report.get("selected_lines") if isinstance(latest_report, dict) and isinstance(latest_report.get("selected_lines"), list) else list(PIPELINE_STRATEGY_LINES_ORDERED)
    selected_set = {str(x) for x in selected_lines}
    for line in PIPELINE_STRATEGY_LINES_ORDERED:
        selected = line in selected_set
        line_done = any(name.endswith(line) for name in done_names)
        if not selected:
            percent = 100
            stage = "skipped_not_selected"
            line_done = True
        elif line_done:
            percent = 100
            stage = "completed"
        elif job_running:
            percent = overall if line == "without_micro" else max(5, overall - (10 if line in {"micro_fast", "strategy5", "strategy6"} else 20))
            stage = str(lock.get("stage") if lock else "running")
        else:
            percent = 0
            stage = "waiting"
        lines[line] = {
            "percent": max(0, min(100, int(percent))),
            "stage": stage,
            "done": line_done,
            "selected": selected,
            "skipped": not selected,
        }
    return _with_output_freshness({
        "status": status,
        "overall_percent": max(0, min(100, int(overall))),
        "lines": lines,
        "stage_names": stage_names,
        "run_id": (lock or latest_report or active_job or {}).get("run_id"),
        "cycle_id": (lock or latest_report or active_job or {}).get("cycle_id"),
        "selected_lines": selected_lines,
        "skipped_lines": [line for line in PIPELINE_STRATEGY_LINES_ORDERED if line not in selected_set],
        "requested_interval_sec": (latest_report or {}).get("requested_interval_sec"),
        "effective_interval_sec": (latest_report or {}).get("effective_interval_sec"),
        "post_run_cooldown_sec": (latest_report or {}).get("post_run_cooldown_sec") or (latest_report or {}).get("effective_interval_sec"),
        "interval_semantics": (latest_report or {}).get("interval_semantics") or "post_run_cooldown",
        "line_runtime_budgets": (latest_report or {}).get("line_runtime_budgets") or {},
    })


def _running_unknown_progress(*, lock: dict[str, Any] | None, active_job: dict[str, Any] | None) -> dict[str, Any]:
    payload = {
        "status": "running",
        "overall_percent": 5 if lock else 0,
        "current_stage": (lock or {}).get("stage") or "running",
        "current_line": None,
        "lines": {
            line: {"percent": 0, "stage": "waiting", "done": False, "run_id": None, "cycle_id": None, "output_fresh": False}
            for line in PIPELINE_STRATEGY_LINES_ORDERED
        },
        "stage_names": [],
        "run_id": (lock or active_job or {}).get("run_id"),
        "cycle_id": (lock or active_job or {}).get("cycle_id"),
    }
    return _with_output_freshness(payload)


def _normalize_pipeline_progress(
    progress: dict[str, Any],
    *,
    job_running: bool,
    latest_report: dict[str, Any] | None,
    lock: dict[str, Any] | None,
    active_job: dict[str, Any] | None,
) -> dict[str, Any]:
    raw_lines = progress.get("lines") if isinstance(progress.get("lines"), dict) else {}
    raw_selected_lines = progress.get("selected_lines")
    selected_set = {str(line) for line in raw_selected_lines} if isinstance(raw_selected_lines, list) else None
    lines: dict[str, dict[str, Any]] = {}
    for line in PIPELINE_STRATEGY_LINES_ORDERED:
        raw = raw_lines.get(line) if isinstance(raw_lines.get(line), dict) else {}
        raw_selected = raw.get("selected")
        selected = bool(raw_selected) if raw_selected is not None else (line in selected_set if selected_set is not None else True)
        skipped = bool(raw.get("skipped", False)) or not selected
        lines[line] = {
            "percent": max(0, min(100, int(raw.get("percent") or (100 if skipped else 0)))),
            "stage": str(raw.get("stage") or ("skipped_not_selected" if skipped else "waiting")),
            "done": bool(raw.get("done")) or skipped,
            "selected": selected,
            "skipped": skipped,
            "run_id": raw.get("run_id"),
            "cycle_id": raw.get("cycle_id"),
            "output_fresh": bool(raw.get("output_fresh")),
        }
        for key in (
            "line_exec_status",
            "line_lifecycle_status",
            "wait_result",
            "line_lifecycle_complete",
            "trade_plan_allowed",
            "terminalized_symbol_count",
            "unfinished_symbol_count",
            "consumable_symbol_count",
            "rejected_count",
            "not_ready_count",
            "timeout_count",
            "observing_count",
            "ready_source_counts",
            "symbol_counts",
            "wait_evidence_path",
            "wait_predicate",
            "wait_pass_micro_generated_at",
            "wait_pass_micro_state_generated_at",
            "wait_pass_ready_symbols",
            "wait_pass_fast_ready_symbols",
            "wait_pass_full_ready_symbols",
        ):
            if key in raw:
                lines[line][key] = raw.get(key)
        _reconcile_progress_line_with_lifecycle(
            line=line,
            row=lines[line],
            run_id=progress.get("run_id"),
            cycle_id=progress.get("cycle_id"),
        )
    if job_running:
        status = "running"
    elif progress.get("status") == "running":
        if isinstance(latest_report, dict) and latest_report.get("run_id") == (progress.get("run_id") or ""):
            status = str(latest_report.get("status") or "stopped")
        else:
            status = "stopped"
    elif isinstance(latest_report, dict) and latest_report.get("run_id") == (progress.get("run_id") or ""):
        status = str(latest_report.get("status") or progress.get("status") or "idle")
    else:
        status = str(progress.get("status") or (latest_report or {}).get("status") or "idle")
    payload = {
        "status": status,
        "overall_percent": max(0, min(100, int(progress.get("overall_percent") or 0))),
        "current_stage": progress.get("current_stage"),
        "current_line": progress.get("current_line"),
        "lines": lines,
        "stage_names": [str(row.get("name")) for row in progress.get("stages") or [] if isinstance(row, dict) and row.get("name")],
        "run_id": progress.get("run_id") or (lock or latest_report or active_job or {}).get("run_id"),
        "cycle_id": progress.get("cycle_id") or (lock or latest_report or active_job or {}).get("cycle_id"),
        "updated_at": progress.get("updated_at"),
        "selected_lines": progress.get("selected_lines"),
        "skipped_lines": progress.get("skipped_lines"),
        "requested_interval_sec": progress.get("requested_interval_sec"),
        "effective_interval_sec": progress.get("effective_interval_sec"),
        "post_run_cooldown_sec": progress.get("post_run_cooldown_sec") or progress.get("effective_interval_sec"),
        "interval_semantics": progress.get("interval_semantics") or "post_run_cooldown",
        "line_runtime_budgets": progress.get("line_runtime_budgets") or {},
    }
    return _with_output_freshness(payload)


def _reconcile_progress_line_with_lifecycle(
    *,
    line: str,
    row: dict[str, Any],
    run_id: Any,
    cycle_id: Any,
) -> None:
    if line not in {"micro_fast", "micro_full"} or not row.get("done"):
        return
    path = PROJECT_ROOT / "DATA" / "micro" / f"latest_micro_lifecycle_{line}.json"
    doc = _read_optional_raw_json(path)
    if not isinstance(doc, dict):
        return
    if doc.get("run_id") != run_id or doc.get("cycle_id") != cycle_id:
        return
    items = doc.get("items")
    if not isinstance(items, list):
        return

    state_counts: dict[str, int] = {}
    ready_count = 0
    confirmed_count = 0
    emitted_count = 0
    consumable_count = 0
    terminalized_count = 0
    unfinished_count = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        state = str(item.get("state") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1
        if item.get("ready") is True:
            ready_count += 1
        if item.get("confirmed") is True or state == "confirmed":
            confirmed_count += 1
        if item.get("trade_plan_emitted") is True or state == "emitted":
            emitted_count += 1
        if item.get("trade_plan_consumable") is True:
            consumable_count += 1
        terminal = bool(item.get("terminal", state not in {"observing", "queued", "pending"}))
        if terminal and state not in {"observing", "queued", "pending"}:
            terminalized_count += 1
        else:
            unfinished_count += 1

    target_count = int(doc.get("count") or len(items))
    rejected_count = int(state_counts.get("rejected", 0))
    not_ready_count = int(state_counts.get("not_ready", 0))
    timeout_count = int(state_counts.get("timeout", 0))
    observing_count = int(state_counts.get("observing", 0)) + int(state_counts.get("queued", 0)) + int(state_counts.get("pending", 0))
    unfinished_count = max(unfinished_count, observing_count)

    if unfinished_count > 0:
        row["stage"] = "completed_with_unfinished_symbols"
        row["line_exec_status"] = "usable_partial" if consumable_count > 0 else "no_ready"
        row["line_lifecycle_status"] = "observing"
    elif consumable_count > 0:
        row["stage"] = "completed_with_consumable_symbols"
        row["line_exec_status"] = "usable_all_ready"
        row["line_lifecycle_status"] = "terminalized_with_consumable"
    elif target_count > 0:
        row["stage"] = "completed_terminalized"
        row["line_exec_status"] = "no_confirmed" if ready_count > 0 else "no_ready"
        row["line_lifecycle_status"] = "terminalized_no_consumable"
    else:
        row["stage"] = "completed_terminalized"
        row["line_exec_status"] = "blocked"
        row["line_lifecycle_status"] = "no_targets"

    if unfinished_count > 0 and consumable_count > 0:
        row["stage_status_class"] = "business_partial_consumable"
        row["business_terminal_reason"] = "partial_consumable_symbols"
        row["technical_failure_reason"] = ""
    elif unfinished_count > 0:
        row["stage_status_class"] = "technical_failed"
        row["business_terminal_reason"] = ""
        row["technical_failure_reason"] = "unfinished_micro_symbols"
    elif consumable_count > 0:
        row["stage_status_class"] = "completed_with_consumable"
        row["business_terminal_reason"] = "consumable_symbols_ready"
        row["technical_failure_reason"] = ""
    elif target_count > 0:
        row["stage_status_class"] = "business_no_signal"
        row["business_terminal_reason"] = row["line_exec_status"]
        row["technical_failure_reason"] = ""
    else:
        row["stage_status_class"] = "business_no_signal"
        row["business_terminal_reason"] = "no_targets"
        row["technical_failure_reason"] = ""

    row["line_lifecycle_complete"] = unfinished_count == 0 and target_count > 0
    row["trade_plan_allowed"] = consumable_count > 0
    row["terminalized_symbol_count"] = terminalized_count
    row["unfinished_symbol_count"] = unfinished_count
    row["consumable_symbol_count"] = consumable_count
    row["rejected_count"] = rejected_count
    row["not_ready_count"] = not_ready_count
    row["timeout_count"] = timeout_count
    row["observing_count"] = observing_count
    row["symbol_counts"] = {
        "target": target_count,
        "ready": ready_count,
        "confirmed": confirmed_count,
        "emitted": emitted_count,
        "consumable": consumable_count,
        "rejected": rejected_count,
        "not_ready": not_ready_count,
        "timeout": timeout_count,
        "observing": observing_count,
        "unfinished": unfinished_count,
        "states": state_counts,
    }


def _with_output_freshness(progress: dict[str, Any]) -> dict[str, Any]:
    run_id = progress.get("run_id")
    cycle_id = progress.get("cycle_id")
    paths = {
        "without_micro": CURRENT_JSON_PATHS["trade_plan_without_micro"],
        "micro_fast": CURRENT_JSON_PATHS["trade_plan_micro_fast"],
        "micro_full": CURRENT_JSON_PATHS["trade_plan_micro_full"],
        "strategy5": CURRENT_JSON_PATHS["trade_plan_strategy5"],
        "strategy6": CURRENT_JSON_PATHS["trade_plan_strategy6"],
    }
    lines = progress.setdefault("lines", {})
    for line, path in paths.items():
        row = lines.setdefault(line, {"percent": 0, "stage": "waiting", "done": False})
        doc = _read_optional_raw_json(path)
        if not isinstance(doc, dict):
            row["output_fresh"] = False
            continue
        row["output_run_id"] = doc.get("run_id")
        row["output_cycle_id"] = doc.get("cycle_id")
        row["output_generated_at"] = doc.get("generated_at")
        fresh = bool(run_id and cycle_id and doc.get("run_id") == run_id and doc.get("cycle_id") == cycle_id)
        row["output_fresh"] = fresh
        row["stale_output_reason"] = "" if fresh else "output_run_id_mismatch"
        row["effective_executable_count"] = int(doc.get("executable_count") or 0) if fresh else 0
        if fresh:
            doc_status = str(doc.get("status") or "").lower()
            input_refs = doc.get("input_refs") if isinstance(doc.get("input_refs"), dict) else {}
            if doc_status in {"blocked", "skipped", "error"}:
                blocked_reason = str(input_refs.get("blocked_reason") or input_refs.get("error_reason") or doc_status)
                row["done"] = True
                row["percent"] = 100
                row["run_id"] = doc.get("run_id")
                row["cycle_id"] = doc.get("cycle_id")
                row["trade_plan_allowed"] = False
                row["effective_executable_count"] = 0
                row["terminal_reason"] = blocked_reason
                if doc_status == "skipped":
                    row["stage"] = "skipped_not_selected"
                    row["terminal_state"] = "skipped"
                    row["skipped"] = True
                    row["selected"] = False
                    row["line_exec_status"] = row.get("line_exec_status") or "skipped_not_selected"
                    row["line_lifecycle_status"] = row.get("line_lifecycle_status") or "skipped_not_selected"
                elif doc_status == "error":
                    row["stage"] = str(input_refs.get("failed_stage") or "blocked_output_error")
                    row["terminal_state"] = "failed"
                    row["line_exec_status"] = row.get("line_exec_status") or "error"
                    row["line_lifecycle_status"] = row.get("line_lifecycle_status") or "error"
                else:
                    row["stage"] = _blocked_stage_for_line(line=line, blocked_reason=blocked_reason)
                    row["terminal_state"] = "blocked"
                    row["line_exec_status"] = row.get("line_exec_status") or "blocked"
                    row["line_lifecycle_status"] = row.get("line_lifecycle_status") or "blocked"
        if fresh and row.get("done"):
            row["run_id"] = doc.get("run_id")
            row["cycle_id"] = doc.get("cycle_id")
        elif not fresh and progress.get("status") == "running":
            row["done"] = False
            row["percent"] = min(int(row.get("percent") or 0), 95)
    _recompute_pipeline_overall(progress)
    return progress


def _blocked_stage_for_line(*, line: str, blocked_reason: str) -> str:
    reason = blocked_reason.lower()
    if "no_consumable" in reason or "no_confirmed" in reason or "quality_ready" in reason:
        return f"blocked_{line}_no_consumable_symbol"
    if "unhealthy" in reason:
        return f"blocked_micro_unhealthy_{line}"
    return f"blocked_{line}"


def _recompute_pipeline_overall(progress: dict[str, Any]) -> None:
    lines = progress.get("lines") if isinstance(progress.get("lines"), dict) else {}
    rows = [
        row
        for row in lines.values()
        if isinstance(row, dict) and row.get("selected", True) is not False
    ]
    if not rows:
        rows = [row for row in lines.values() if isinstance(row, dict)]
    if not rows:
        return
    progress["overall_percent"] = max(
        0,
        min(100, int(round(sum(int(row.get("percent") or 0) for row in rows) / len(rows)))),
    )


def _micro_daemon_health() -> dict[str, Any]:
    cfg = load_yaml_config()
    runtime = _runtime_config()
    micro_cfg = cfg.get("micro_daemon_cli") if isinstance(cfg.get("micro_daemon_cli"), dict) else {}
    pid_path = _resolve_project_path(micro_cfg.get("pid_path", "DATA/runtime/micro_daemon.pid"))
    heartbeat_path = _resolve_project_path(
        micro_cfg.get("heartbeat_path", "DATA/micro/micro_collector_heartbeat.json"),
    )
    state_path = _resolve_project_path(micro_cfg.get("state_path", "DATA/micro/latest_micro_state.json"))
    features_path = _resolve_project_path(micro_cfg.get("features_path", "DATA/micro/latest_micro_features.json"))
    runtime_health.utc_now = utc_now
    return runtime_health.micro_daemon_health(
        pid_path=pid_path,
        heartbeat_path=heartbeat_path,
        state_path=state_path,
        features_path=features_path,
        heartbeat_stale_sec=runtime["heartbeat_stale_sec"],
    )


def _paper_daemon_health() -> dict[str, Any]:
    cfg = _paper_config()
    paths = {
        "pid": _resolve_project_path(cfg.daemon_pid_path),
        "heartbeat": _resolve_project_path(cfg.daemon_heartbeat_path),
        "status": _resolve_project_path(cfg.daemon_status_path),
        "summary": _resolve_project_path(cfg.summary_path),
    }
    runtime = _runtime_config()
    pid = _json_pid(paths["pid"])
    heartbeat = _read_optional_raw_json(paths["heartbeat"])
    status_doc = _read_optional_raw_json(paths["status"])
    summary = _read_optional_raw_json(paths["summary"])
    heartbeat_at = _iso_from_payload(heartbeat, "heartbeat_at", "generated_at")
    heartbeat_age = _age_sec(heartbeat_at)
    stale_limit = int((load_yaml_config().get("paper") or {}).get("daemon", {}).get("stale_after_sec", runtime["heartbeat_stale_sec"]))
    stale = heartbeat_age is None or heartbeat_age > stale_limit
    running = _pid_running(pid)
    if running and not stale:
        status = "running"
    elif running and stale:
        status = "stale"
    elif paths["pid"].exists() and not running:
        status = "stopped"
    else:
        status = "stopped"
    return {
        "name": "paper_daemon",
        "status": status,
        "pid": pid,
        "pid_path": str(paths["pid"]),
        "pid_exists": paths["pid"].exists(),
        "pid_running": running,
        "heartbeat_path": str(paths["heartbeat"]),
        "heartbeat_exists": paths["heartbeat"].exists(),
        "heartbeat_at": heartbeat_at,
        "heartbeat_age_sec": heartbeat_age,
        "stale": stale,
        "status_path": str(paths["status"]),
        "last_tick_at": _iso_from_payload(status_doc, "last_tick_at", "updated_at"),
        "summary_path": str(paths["summary"]),
        "summary_generated_at": _iso_from_payload(summary, "generated_at"),
        "active_symbols": status_doc.get("active_symbols") if isinstance(status_doc, dict) else None,
    }


def runtime_status() -> dict[str, Any]:
    micro = _micro_daemon_health()
    paper = _paper_daemon_health()
    snapshot = step15_daemon_health_payload()
    warmup = snapshot_warmup_payload()
    recovery = _runtime_auto_recover_if_needed(micro=micro, paper=paper)
    if recovery.get("attempted"):
        micro = _micro_daemon_health()
        paper = _paper_daemon_health()
    errors: list[str] = []
    if micro["status"] not in {"running", "idle"}:
        errors.append("micro_daemon_not_healthy")
    for code in micro.get("reason_codes") or []:
        code = str(code)
        if code and code not in errors:
            errors.append(code)
    if paper["status"] != "running":
        errors.append("paper_daemon_not_healthy")
    if not errors:
        status = "ok"
    elif micro["status"] == "stopped" and paper["status"] == "stopped":
        status = "stopped"
    else:
        status = "partial"
    return {
        "status": status,
        "config": _runtime_config(),
        "micro_daemon": micro,
        "paper_daemon": paper,
        "snapshot_daemon": snapshot,
        "snapshot_warmup": warmup,
        "recovery": recovery,
        "errors": errors,
        "generated_at": to_iso_z(utc_now()),
    }


def runtime_status_lite() -> dict[str, Any]:
    micro = _micro_daemon_health()
    paper = _paper_daemon_health()
    snapshot = step15_daemon_health_payload()
    errors: list[str] = []
    if micro.get("status") not in {"running", "idle"}:
        errors.append("micro_daemon_not_healthy")
    for code in micro.get("reason_codes") or []:
        code = str(code)
        if code and code not in errors:
            errors.append(code)
    if paper.get("status") != "running":
        errors.append("paper_daemon_not_healthy")
    if not errors:
        status = "ok"
    elif micro.get("status") == "stopped" and paper.get("status") == "stopped":
        status = "stopped"
    else:
        status = "partial"
    return {
        "schema_version": "12.59",
        "payload_scope": "lite",
        "status": status,
        "micro_daemon": {
            "status": micro.get("status"),
            "heartbeat_age_sec": micro.get("heartbeat_age_sec"),
            "stale": micro.get("stale"),
            "reason_codes": micro.get("reason_codes") or [],
            "pid": micro.get("pid"),
            "pid_running": micro.get("pid_running"),
        },
        "paper_daemon": {
            "status": paper.get("status"),
            "heartbeat_age_sec": paper.get("heartbeat_age_sec"),
            "stale": paper.get("stale"),
            "reason_codes": paper.get("reason_codes") or [],
            "pid": paper.get("pid"),
            "pid_running": paper.get("pid_running"),
        },
        "snapshot_daemon": {
            "status": snapshot.get("status"),
            "heartbeat_age_sec": snapshot.get("heartbeat_age_sec"),
            "stale": snapshot.get("stale"),
            "reason_codes": snapshot.get("reason_codes") or [],
            "fresh_count": snapshot.get("fresh_count"),
            "usable_count": snapshot.get("usable_count"),
            "blocked_count": snapshot.get("blocked_count"),
        },
        "errors": errors,
        "generated_at": to_iso_z(utc_now()),
    }


def _recovery_latest_path() -> Path:
    return PROJECT_ROOT / "DATA" / "runtime" / "latest_runtime_recovery.json"


def _recovery_events_path() -> Path:
    return PROJECT_ROOT / "DATA" / "runtime" / "runtime_recovery_events.jsonl"


def _runtime_auto_recover_if_needed(*, micro: dict[str, Any], paper: dict[str, Any]) -> dict[str, Any]:
    cfg = _runtime_config()
    payload: dict[str, Any] = {
        "enabled": cfg["restart_on_stale"],
        "attempted": False,
        "actions": [],
    }
    if os.getenv("PYTEST_CURRENT_TEST") or not cfg["restart_on_stale"]:
        return payload
    latest = _read_optional_raw_json(_recovery_latest_path()) or {}
    last_at = _iso_from_payload(latest, "generated_at")
    last_age = _age_sec(last_at)
    if last_age is not None and last_age < cfg["restart_cooldown_sec"]:
        payload["cooldown"] = True
        payload["last_recovery_at"] = last_at
        return payload
    if cfg["restart_micro_daemon_on_stale"] and micro.get("status") not in {"running", "idle"}:
        payload["actions"].append(_recover_daemon("micro_daemon", micro))
        payload["attempted"] = True
    if cfg["restart_paper_daemon_on_stale"] and paper.get("status") != "running":
        payload["actions"].append(_recover_daemon("paper_daemon", paper))
        payload["attempted"] = True
    if payload["attempted"]:
        event = {
            "generated_at": to_iso_z(utc_now()),
            "source": "runtime_auto_recovery",
            "actions": payload["actions"],
            "status": "recovered"
            if all(a.get("status") in {"recovered", "started", "completed"} for a in payload["actions"])
            else "partial",
        }
        _recovery_latest_path().parent.mkdir(parents=True, exist_ok=True)
        write_json_atomic(_recovery_latest_path(), event)
        with _recovery_events_path().open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False) + "\n")
        payload.update(event)
    return payload


def _backup_runtime_pid_file(pid_path: str | Path | None) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "pid_file_backed_up": False,
        "force_cleanup_applied": False,
    }
    if not pid_path:
        return evidence
    path = _resolve_project_path(pid_path)
    if not path.exists():
        return evidence
    stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    backup_path = path.with_name(f"{path.name}.stale_{stamp}")
    try:
        path.replace(backup_path)
    except OSError as exc:
        evidence.update({"force_cleanup_error": str(exc)})
        return evidence
    evidence.update(
        {
            "pid_file_backed_up": True,
            "force_cleanup_applied": True,
            "backup_path": str(backup_path),
        }
    )
    return evidence


def _wait_micro_daemon_recovered(timeout_sec: float = 8.0) -> dict[str, Any]:
    deadline = time.time() + max(0.0, timeout_sec)
    last = _micro_daemon_health()
    while time.time() < deadline:
        if (
            last.get("status") in {"running", "idle"}
            and not last.get("stale")
            and last.get("data_plane_fresh")
        ):
            return last
        time.sleep(1.0)
        last = _micro_daemon_health()
    return last


def _recover_daemon(name: str, health: dict[str, Any]) -> dict[str, Any]:
    action: dict[str, Any] = {
        "daemon": name,
        "reason": "heartbeat_stale" if health.get("stale") else str(health.get("status") or "unhealthy"),
        "old_pid": health.get("pid"),
        "os_pid_exists": bool(health.get("pid_running")),
        "pid_probe_source": "runtime_health.pid_running",
    }
    try:
        if name == "micro_daemon":
            stop = micro_daemon_action("stop")
            start = micro_daemon_action("start")
            cleanup: dict[str, Any] = {}
            if stop.get("status") == "stop_failed" or start.get("status") == "already_running":
                cleanup = _backup_runtime_pid_file(health.get("pid_path"))
                if cleanup.get("force_cleanup_applied"):
                    start = micro_daemon_action("start")
            final_health = _wait_micro_daemon_recovered()
            recovered = (
                final_health.get("status") in {"running", "idle"}
                and not final_health.get("stale")
                and final_health.get("data_plane_fresh")
            )
            action.update(
                {
                    "status": "recovered" if recovered else "failed",
                    "stop": stop,
                    "start": start,
                    "final_health": final_health,
                    "new_pid": final_health.get("pid"),
                    **cleanup,
                }
            )
        else:
            stop = paper_daemon_payload("stop")
            start = paper_daemon_payload("start")
            action.update({"status": str(start.get("status") or "started"), "stop": stop, "start": start})
    except Exception as exc:
        action.update({"status": "failed", "error": str(exc)})
    return action


def runtime_start() -> dict[str, Any]:
    cfg = _runtime_config()
    started: dict[str, bool] = {"micro_daemon": False, "paper_daemon": False, "snapshot_daemon": False}
    results: dict[str, Any] = {}
    errors: list[str] = []
    micro = _micro_daemon_health()
    paper = _paper_daemon_health()
    snapshot = step15_daemon_health_payload()
    if cfg["autostart_snapshot_daemon"] and snapshot.get("watchdog_status") != "healthy":
        try:
            results["snapshot_daemon"] = snapshot_daemon_action("start")
            started["snapshot_daemon"] = True
        except Exception as exc:
            errors.append(f"snapshot_daemon_start_failed:{type(exc).__name__}")
            results["snapshot_daemon_error"] = str(exc)
    if cfg["autostart_micro_daemon"] and (
        micro["status"] != "running" or micro.get("process_registry_status") != "running"
    ):
        try:
            results["micro_daemon"] = micro_daemon_action("start")
            started["micro_daemon"] = True
        except Exception as exc:
            errors.append(f"micro_daemon_start_failed:{type(exc).__name__}")
            results["micro_daemon_error"] = str(exc)
    if cfg["autostart_paper_daemon"] and paper["status"] != "running":
        try:
            results["paper_daemon"] = paper_daemon_payload("start")
            started["paper_daemon"] = True
        except Exception as exc:
            errors.append(f"paper_daemon_start_failed:{type(exc).__name__}")
            results["paper_daemon_error"] = str(exc)
    status_payload = runtime_status()
    return {
        **status_payload,
        "started": started,
        "start_results": results,
        "errors": [*status_payload.get("errors", []), *errors],
    }


def runtime_stop() -> dict[str, Any]:
    results: dict[str, Any] = {}
    errors: list[str] = []
    try:
        results["snapshot_daemon"] = snapshot_daemon_action("stop")
    except Exception as exc:
        errors.append(f"snapshot_daemon_stop_failed:{type(exc).__name__}")
        results["snapshot_daemon_error"] = str(exc)
    try:
        results["micro_daemon"] = micro_daemon_action("stop")
    except Exception as exc:
        errors.append(f"micro_daemon_stop_failed:{type(exc).__name__}")
        results["micro_daemon_error"] = str(exc)
    try:
        results["paper_daemon"] = paper_daemon_payload("stop")
    except Exception as exc:
        errors.append(f"paper_daemon_stop_failed:{type(exc).__name__}")
        results["paper_daemon_error"] = str(exc)
    return {**runtime_status(), "stop_results": results, "errors": errors}


def runtime_restart() -> dict[str, Any]:
    stop_result = runtime_stop()
    start_result = runtime_start()
    return {**start_result, "restart": {"stop": stop_result, "start": start_result}}


def runtime_autostart_if_configured() -> dict[str, Any]:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return {"status": "skipped", "reason": "pytest"}
    cfg = _runtime_config()
    if not cfg["autostart_enabled"]:
        return {"status": "skipped", "reason": "autostart_disabled"}
    return runtime_start()


def _paper_config() -> PaperConfig:
    cfg = load_yaml_config()
    paper = cfg.get("paper") or {}
    daemon = paper.get("daemon") or {}
    archive = paper.get("archive") or {}
    fill_model = paper.get("fill_model") if isinstance(paper.get("fill_model"), dict) else {}
    cooldown_after = paper.get("reentry_cooldown_after") or {}
    return PaperConfig(
        db_path=str(paper.get("db_path", "DATA/paper/paper_trading.db")),
        summary_path=str(paper.get("summary_path", "DATA/paper/latest_paper_state.json")),
        default_account_equity_usdt=float(paper.get("default_account_equity_usdt", 1000)),
        default_margin_usdt=float(paper.get("default_margin_usdt", 100)),
        default_leverage=float(paper.get("default_leverage", 20)),
        paper_fallback_notional_allowed=bool(paper.get("paper_fallback_notional_allowed", True)),
        taker_fee_bps=float(paper.get("taker_fee_bps", 5)),
        maker_fee_bps=float(paper.get("maker_fee_bps", 2)),
        default_slippage_bps=float(paper.get("default_slippage_bps", 5)),
        fill_model_mode=str(fill_model.get("mode", "fixed_1m")),
        use_trade_plan_slippage=bool(fill_model.get("use_trade_plan_slippage", False)),
        use_liquidity_profile=bool(fill_model.get("use_liquidity_profile", False)),
        entry_delay_sec=int(fill_model.get("entry_delay_sec", 0) or 0),
        max_entry_drift_bps=float(fill_model.get("max_entry_drift_bps", 80)),
        default_market_slippage_bps=(
            float(fill_model["default_market_slippage_bps"])
            if fill_model.get("default_market_slippage_bps") is not None
            else None
        ),
        fallback_market_slippage_bps=float(fill_model.get("fallback_market_slippage_bps", 15)),
        volatility_slippage_mult=float(fill_model.get("volatility_slippage_mult", 0.15)),
        thin_book_slippage_mult=float(fill_model.get("thin_book_slippage_mult", 1.5)),
        max_allowed_paper_slippage_bps=float(fill_model.get("max_allowed_paper_slippage_bps", 120)),
        slippage_too_high_policy=str(fill_model.get("slippage_too_high_policy", "cap")),
        same_candle_sl_tp_policy=str(fill_model.get("same_candle_sl_tp_policy", "sl_first")),
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


def _paper_db_path() -> Path:
    return PROJECT_ROOT / _paper_config().db_path


def _paper_summary_path() -> Path:
    return PROJECT_ROOT / _paper_config().summary_path


def _validate_paper_line(line: str | None) -> None:
    if line is not None and line not in STRATEGY_LINES:
        raise ApiServiceError("invalid_strategy_line", "invalid paper strategy line", {"line": line})


def _sqlite_rows(db_path: Path, table: str, limit: int = 200, *, line: str | None = None) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    allowed = {
        "paper_accounts",
        "paper_orders",
        "paper_positions",
        "paper_fills",
        "paper_performance_snapshots",
        "paper_reset_epochs",
        "paper_intent_inbox",
        "paper_trade_plans",
        "paper_skip_ledger",
    }
    if table not in allowed:
        raise ApiServiceError("paper_table_invalid", "unsupported paper table", {"table": table})
    order_by = "rowid desc"
    if table == "paper_orders":
        order_by = "coalesce(closed_at, opened_at, updated_at, created_at) desc, rowid desc"
    elif table == "paper_positions":
        order_by = "coalesce(closed_at, opened_at, updated_at) desc, rowid desc"
    elif table == "paper_fills":
        order_by = "coalesce(filled_at, '') desc, rowid desc"
    elif table == "paper_intent_inbox":
        order_by = "coalesce(updated_at, created_at) desc, rowid desc"
    elif table == "paper_reset_epochs":
        order_by = "coalesce(reset_at, '') desc, rowid desc"
    elif table == "paper_trade_plans":
        order_by = "coalesce(consumed_at, created_at) desc, rowid desc"
    elif table == "paper_skip_ledger":
        order_by = "coalesce(created_at, '') desc, rowid desc"
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            if line:
                rows = conn.execute(f"select * from {table} where strategy_line = ? order by {order_by} limit ?", (line, limit)).fetchall()
            else:
                rows = conn.execute(f"select * from {table} order by {order_by} limit ?", (limit,)).fetchall()
    except sqlite3.Error:
        return []
    return [dict(row) for row in rows]


def _sqlite_table_exists(db_path: Path, table: str) -> bool:
    if not db_path.exists():
        return False
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    except sqlite3.Error:
        return False
    return row is not None


def _trade_quality_rows(
    table: str,
    *,
    strategy_line: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    sample_source: str | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    root_cause: str | None = None,
    order_id: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    db_path = _paper_db_path()
    allowed = {
        "trade_quality_samples",
        "trade_quality_aggregates",
        "trade_quality_recommendations",
    }
    if table not in allowed:
        raise ApiServiceError("trade_quality_table_invalid", "unsupported trade quality table", {"table": table})
    if not _sqlite_table_exists(db_path, table):
        return []
    safe_limit = max(1, min(int(limit or 200), 1000))
    clauses: list[str] = []
    params: list[Any] = []
    if strategy_line:
        _validate_paper_line(strategy_line)
        if table == "trade_quality_aggregates":
            clauses.extend(["dimension = ?", "key = ?"])
            params.extend(["strategy_line", strategy_line])
        elif table != "trade_quality_recommendations":
            clauses.append("strategy_line = ?")
            params.append(strategy_line)
    if symbol and table == "trade_quality_samples":
        clauses.append("upper(symbol) = ?")
        params.append(symbol.upper())
    if side and table == "trade_quality_samples":
        clauses.append("upper(side) = ?")
        params.append(side.upper())
    if sample_source and table == "trade_quality_samples":
        source = str(sample_source).lower()
        if source == "archive":
            clauses.append(
                "sample_id IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
            )
        elif source == "live":
            clauses.append(
                "sample_id NOT IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
            )
    if run_id and table == "trade_quality_samples":
        clauses.append("source_run_id = ?")
        params.append(run_id)
    if cycle_id and table == "trade_quality_samples":
        clauses.append("source_cycle_id = ?")
        params.append(cycle_id)
    if root_cause:
        if table == "trade_quality_samples":
            clauses.append("root_cause_label = ?")
            params.append(root_cause)
        elif table == "trade_quality_aggregates":
            clauses.extend(["dimension = ?", "key = ?"])
            params.extend(["root_cause", root_cause])
    if order_id and table == "trade_quality_samples":
        clauses.append("order_id = ?")
        params.append(order_id)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    if table == "trade_quality_samples":
        order_by = "coalesce(closed_at, generated_at) desc, rowid desc"
    elif table == "trade_quality_aggregates":
        order_by = "dimension asc, total_R asc, sample_count desc"
    else:
        order_by = "case priority when 'P0' then 0 when 'P1' then 1 else 2 end, rowid asc"
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(f"SELECT * FROM {table}{where} ORDER BY {order_by} LIMIT ?", [*params, safe_limit]).fetchall()
    except sqlite3.Error:
        return []
    decoded: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in ("root_cause_evidence_json", "secondary_labels_json", "evidence_json"):
            if key in item:
                out_key = key.removesuffix("_json")
                item[out_key] = json.loads(item.pop(key) or "{}") if key != "secondary_labels_json" else json.loads(item.pop(key) or "[]")
        decoded.append(item)
    if table == "trade_quality_samples":
        decoded = enrich_sample_sources(db_path, decoded)
    return decoded


def trade_quality_payload(
    kind: str,
    *,
    strategy_line: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    sample_source: str | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    root_cause: str | None = None,
    order_id: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    db_path = _paper_db_path()
    if strategy_line:
        _validate_paper_line(strategy_line)
    if db_path.exists():
        ensure_trade_quality_tables(db_path)
        archive_summary = archive_ingest_summary(db_path)
        replay_summary = replay_backfill_summary(db_path)
    else:
        archive_summary = {"ledger_exists": False}
        replay_summary = {"ledger_exists": False}
    safe_limit = max(1, min(int(limit or 200), 1000))
    samples = _trade_quality_rows(
        "trade_quality_samples",
        strategy_line=strategy_line,
        symbol=symbol,
        side=side,
        sample_source=sample_source,
        run_id=run_id,
        cycle_id=cycle_id,
        root_cause=root_cause,
        order_id=order_id,
        limit=safe_limit,
    )
    aggregates = _trade_quality_rows("trade_quality_aggregates", strategy_line=strategy_line, root_cause=root_cause, limit=1000)
    recommendations = _trade_quality_rows("trade_quality_recommendations", limit=200)
    base = {
        "schema_version": "12.50",
        "source": f"trade_quality_{kind}",
        "db_path": str(db_path),
        "db_exists": db_path.exists(),
        "filters": {
            "strategy_line": strategy_line,
            "symbol": symbol,
            "side": side,
            "sample_source": sample_source,
            "run_id": run_id,
            "cycle_id": cycle_id,
            "root_cause": root_cause,
            "order_id": order_id,
            "limit": safe_limit,
        },
    }
    if kind == "samples":
        return {**base, "count": len(samples), "samples": samples}
    if kind == "root-causes":
        return {
            **base,
            "root_causes": [row for row in aggregates if row.get("dimension") == "root_cause"],
            "samples": samples[:safe_limit],
        }
    if kind == "clusters":
        loss_clusters = [
            row
            for row in aggregates
            if row.get("dimension") in {"root_cause", "symbol", "side", "strategy_line"} and float(row.get("total_R") or 0) < 0
        ]
        return {**base, "clusters": loss_clusters[:safe_limit]}
    if kind == "recommendations":
        return {**base, "count": len(recommendations), "recommendations": recommendations}
    if kind == "order":
        if not order_id:
            raise ApiServiceError("trade_quality_order_required", "order_id is required", {})
        return {**base, "sample": samples[0] if samples else None}
    if kind == "summary":
        r_values = [float(row.get("net_R") or 0) for row in samples if row.get("net_R") is not None]
        win_count = len([row for row in samples if float(row.get("net_R") or 0) > 0])
        mfe_values = [float(row.get("MFE_R") or 0) for row in samples if row.get("MFE_R") is not None]
        mae_values = [float(row.get("MAE_R") or 0) for row in samples if row.get("MAE_R") is not None]
        root_counts = Counter(str(row.get("root_cause_label") or "unknown") for row in samples)
        model_counts = Counter(str(row.get("excursion_model") or "unknown") for row in samples)
        return {
            **base,
            "summary": {
                "sample_count": len(samples),
                "total_R": round(sum(r_values), 8),
                "expectancy_R": round(sum(r_values) / len(r_values), 8) if r_values else 0.0,
                "win_rate": round(win_count / len(samples), 6) if samples else 0.0,
                "avg_MFE_R": round(sum(mfe_values) / len(mfe_values), 8) if mfe_values else 0.0,
                "avg_MAE_R": round(sum(mae_values) / len(mae_values), 8) if mae_values else 0.0,
                "root_cause_counts": dict(root_counts),
                "excursion_model_counts": dict(model_counts),
                "replay_sample_count": int(model_counts.get("candle_1m_replay", 0)),
                "proxy_sample_count": int(model_counts.get("outcome_proxy_no_candle_replay", 0)),
            },
            "archive_ingest": archive_summary,
            "replay_backfill": replay_summary,
            "aggregates": aggregates,
            "recommendations": recommendations,
            "samples": samples[:safe_limit],
        }
    raise ApiServiceError("trade_quality_kind_invalid", "unsupported trade quality payload", {"kind": kind})


def trade_quality_archive_backfill_service(*, dry_run: bool = True, limit: int | None = None) -> dict[str, Any]:
    return archive_backfill_payload(PROJECT_ROOT, write=not dry_run, limit=limit, config=_paper_config())


def trade_quality_ingest_ledger_payload(*, limit: int = 200) -> dict[str, Any]:
    db_path = _paper_db_path()
    rows = ingest_ledger_rows(db_path, limit=limit)
    return {
        "schema_version": "18.9",
        "source": "trade_quality_archive_ingest_ledger",
        "db_path": str(db_path),
        "count": len(rows),
        "ledger": rows,
        "summary": archive_ingest_summary(db_path),
    }


def trade_quality_replay_backfill_service(
    *,
    dry_run: bool = True,
    limit: int | None = None,
    sample_source: str = "all",
) -> dict[str, Any]:
    return replay_backfill_payload(
        PROJECT_ROOT,
        write=not dry_run,
        limit=limit,
        sample_source=sample_source,
        config=_paper_config(),
    )


def trade_quality_replay_ledger_payload(*, limit: int = 200) -> dict[str, Any]:
    db_path = _paper_db_path()
    rows = replay_backfill_ledger_rows(db_path, limit=limit)
    return {
        "schema_version": "18.16",
        "source": "trade_quality_replay_backfill_ledger",
        "db_path": str(db_path),
        "count": len(rows),
        "ledger": rows,
        "summary": replay_backfill_summary(db_path),
    }


def _diagnostic_filters(
    *,
    source: str | None = None,
    archive_id: str | None = None,
    strategy_line: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    exit_reason: str | None = None,
    root_cause: str | None = None,
    quality_tag: str | None = None,
    replay_status: str | None = None,
    entry_quality_label: str | None = None,
    entry_quality_v2_label: str | None = None,
    microstructure_coverage: str | None = None,
    market_context_label: str | None = None,
    market_context_status: str | None = None,
    entry_context_v3_label: str | None = None,
    funding_regime: str | None = None,
    oi_direction: str | None = None,
    btc_alignment: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any]:
    if strategy_line and strategy_line != "all":
        _validate_paper_line(strategy_line)
    return {
        "source": None if source == "all" else source,
        "archive_id": archive_id,
        "strategy_line": strategy_line,
        "symbol": symbol,
        "side": side,
        "exit_reason": exit_reason,
        "root_cause": root_cause,
        "quality_tag": quality_tag,
        "replay_status": replay_status,
        "entry_quality_label": entry_quality_label,
        "entry_quality_v2_label": entry_quality_v2_label,
        "microstructure_coverage": microstructure_coverage,
        "market_context_label": market_context_label,
        "market_context_status": market_context_status,
        "entry_context_v3_label": entry_context_v3_label,
        "funding_regime": funding_regime,
        "oi_direction": oi_direction,
        "btc_alignment": btc_alignment,
        "date_from": date_from,
        "date_to": date_to,
    }


def _ensure_trade_quality_diagnostics_synced(*, limit: int | None = None, source: str = "all") -> None:
    diagnostic_backfill_payload(PROJECT_ROOT, write=True, limit=limit, source=source, config=_paper_config())


def trade_quality_diagnostics_sync_service(*, dry_run: bool = True, limit: int | None = None, source: str = "all") -> dict[str, Any]:
    return diagnostic_backfill_payload(PROJECT_ROOT, write=not dry_run, limit=limit, source=source, config=_paper_config())


def trade_quality_diagnostics_summary_service(**filters: Any) -> dict[str, Any]:
    return diagnostic_summary_payload(_paper_db_path(), **_diagnostic_filters(**filters))


def trade_quality_diagnostics_samples_service(*, limit: int = 200, offset: int = 0, **filters: Any) -> dict[str, Any]:
    return diagnostic_samples_payload(_paper_db_path(), limit=limit, offset=offset, **_diagnostic_filters(**filters))


def trade_quality_diagnostics_sample_detail_service(trade_id: str) -> dict[str, Any]:
    return diagnostic_sample_detail_payload(_paper_db_path(), trade_id)


def trade_quality_diagnostics_aggregates_service(**filters: Any) -> dict[str, Any]:
    return diagnostic_aggregates_payload(_paper_db_path(), **_diagnostic_filters(**filters))


def trade_quality_diagnostics_archive_packages_service() -> dict[str, Any]:
    return diagnostic_archive_packages_payload(PROJECT_ROOT, config=_paper_config())


def backtest_p21_packages_service() -> dict[str, Any]:
    return p21_packages_payload(PROJECT_ROOT, config=_paper_config())


def backtest_p21_problem_baseline_service(
    *,
    source: str = "all",
    archive_id: str | None = None,
    strategy_line: str = "all",
    limit: int = 5000,
    write: bool = True,
) -> dict[str, Any]:
    return p21_baseline_payload(
        PROJECT_ROOT,
        source=source,
        archive_id=archive_id,
        strategy_line=strategy_line,
        limit=limit,
        write=write,
        config=_paper_config(),
    )


def backtest_p21_run_matrix_service(
    *,
    source: str = "all",
    archive_id: str | None = None,
    strategy_line: str = "all",
    limit: int = 5000,
    max_sets: int = 120,
    parameter_grid: list[dict[str, Any]] | None = None,
    write: bool = True,
) -> dict[str, Any]:
    return p21_run_matrix_payload(
        PROJECT_ROOT,
        source=source,
        archive_id=archive_id,
        strategy_line=strategy_line,
        limit=limit,
        max_sets=max_sets,
        parameter_grid=parameter_grid,
        write=write,
        config=_paper_config(),
    )


def backtest_p21_experiments_service(*, limit: int = 50) -> dict[str, Any]:
    return p21_experiments_payload(PROJECT_ROOT, limit=limit)


def backtest_p21_experiment_detail_service(experiment_id: str) -> dict[str, Any]:
    return p21_experiment_detail_payload(PROJECT_ROOT, experiment_id)


def backtest_p21_recommendations_service(*, limit: int = 50) -> dict[str, Any]:
    return p21_recommendations_payload(PROJECT_ROOT, limit=limit)


def backtest_p21_export_config_candidate_service(
    *,
    experiment_id: str,
    parameter_set_id: str | None = None,
) -> dict[str, Any]:
    return p21_export_config_candidate_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        parameter_set_id=parameter_set_id,
    )


def backtest_p21_v2_kline_cache_status_service(
    *,
    symbols: list[str] | None = None,
    days: int = 30,
    max_symbols: int = 50,
) -> dict[str, Any]:
    return p21_v2_kline_cache_status_payload(PROJECT_ROOT, symbols=symbols, days=days, max_symbols=max_symbols)


def backtest_p21_v2_kline_cache_download_service(
    *,
    symbols: list[str] | None = None,
    days: int = 30,
    max_symbols: int = 10,
    dry_run: bool = False,
    sleep_sec: float = 0.05,
) -> dict[str, Any]:
    return p21_v2_download_kline_cache_payload(
        PROJECT_ROOT,
        symbols=symbols,
        days=days,
        max_symbols=max_symbols,
        dry_run=dry_run,
        sleep_sec=sleep_sec,
    )


def backtest_p21_v2_matrix_contracts_service(
    *,
    strategy_line: str = "all",
    max_sets: int = 240,
) -> dict[str, Any]:
    return p21_v2_config_matrix_contract_payload(PROJECT_ROOT, strategy_line=strategy_line, max_sets=max_sets)


def backtest_p21_v2_matrix_run_service(
    *,
    symbols: list[str] | None = None,
    strategy_line: str = "all",
    days: int = 30,
    max_symbols: int = 20,
    max_sets: int = 120,
    parameter_grid: list[dict[str, Any]] | None = None,
    write: bool = True,
) -> dict[str, Any]:
    return p21_v2_run_config_matrix_payload(
        PROJECT_ROOT,
        symbols=symbols,
        strategy_line=strategy_line,
        days=days,
        max_symbols=max_symbols,
        max_sets=max_sets,
        parameter_grid=parameter_grid,
        write=write,
    )


def backtest_p21_v2_job_start_service(
    *,
    job_type: str = "matrix_backtest",
    symbols: list[str] | None = None,
    strategy_line: str = "all",
    days: int = 30,
    max_symbols: int = 20,
    max_sets: int = 120,
    symbol_shard_size: int = 25,
    max_workers: int = 1,
    scheduler_mode: str = "parameter_batch",
    resume_experiment_id: str | None = None,
    sleep_sec: float = 0.6,
) -> dict[str, Any]:
    return p21_v2_start_job_payload(
        PROJECT_ROOT,
        job_type=job_type,
        symbols=symbols,
        strategy_line=strategy_line,
        days=days,
        max_symbols=max_symbols,
        max_sets=max_sets,
        symbol_shard_size=symbol_shard_size,
        max_workers=max_workers,
        scheduler_mode=scheduler_mode,
        resume_experiment_id=resume_experiment_id,
        sleep_sec=sleep_sec,
    )


def backtest_p21_v2_job_status_service(job_id: str) -> dict[str, Any]:
    return p21_v2_job_status_payload(PROJECT_ROOT, job_id)


def backtest_p21_v2_job_stop_service(job_id: str) -> dict[str, Any]:
    return p21_v2_stop_job_payload(PROJECT_ROOT, job_id)


def backtest_p21_v2_jobs_service(*, limit: int = 20) -> dict[str, Any]:
    return p21_v2_jobs_payload(PROJECT_ROOT, limit=limit)


def backtest_p21_v2_experiments_service(*, limit: int = 50) -> dict[str, Any]:
    return p21_v2_experiments_payload(PROJECT_ROOT, limit=limit)


def backtest_p21_v2_experiment_detail_service(experiment_id: str) -> dict[str, Any]:
    return p21_v2_experiment_detail_payload(PROJECT_ROOT, experiment_id)


def backtest_p21_v2_experiment_orders_service(
    experiment_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
    strategy_line: str | None = None,
    symbol: str | None = None,
    parameter_set_id: str | None = None,
) -> dict[str, Any]:
    return p21_v2_experiment_orders_payload(
        PROJECT_ROOT,
        experiment_id,
        limit=limit,
        offset=offset,
        strategy_line=strategy_line,
        symbol=symbol,
        parameter_set_id=parameter_set_id,
    )


def backtest_p21_v2_experiment_daily_service(experiment_id: str, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    return p21_v2_experiment_daily_payload(PROJECT_ROOT, experiment_id, limit=limit, offset=offset)


def backtest_p21_v2_experiment_symbols_service(experiment_id: str, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    return p21_v2_experiment_symbols_payload(PROJECT_ROOT, experiment_id, limit=limit, offset=offset)


def backtest_p21_v2_leaderboard_service(*, limit: int = 50, exclude_legacy: bool = True) -> dict[str, Any]:
    return p21_v2_leaderboard_payload(PROJECT_ROOT, limit=limit, exclude_legacy=exclude_legacy)


def backtest_p21_v2_strategy4_replay_run_service(
    *,
    symbols: list[str] | None = None,
    days: int = 3,
    max_symbols: int = 5,
    max_sets: int = 1,
    max_admissions_per_symbol: int = 20,
    max_attempts: int = 12,
    observe_interval_min: int = 5,
    write: bool = True,
) -> dict[str, Any]:
    return p21_v2_strategy4_replay_run_payload(
        PROJECT_ROOT,
        symbols=symbols,
        days=days,
        max_symbols=max_symbols,
        max_sets=max_sets,
        max_admissions_per_symbol=max_admissions_per_symbol,
        max_attempts=max_attempts,
        observe_interval_min=observe_interval_min,
        write=write,
    )


def backtest_p21_v2_strategy4_replay_summary_service(
    *,
    experiment_id: str | None = None,
    parameter_set_id: str | None = None,
) -> dict[str, Any]:
    return p21_v2_strategy4_replay_summary_payload(PROJECT_ROOT, experiment_id=experiment_id, parameter_set_id=parameter_set_id)


def backtest_p21_v2_strategy4_replay_pool_service(
    *,
    experiment_id: str | None = None,
    parameter_set_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    return p21_v2_strategy4_replay_pool_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        parameter_set_id=parameter_set_id,
        status=status,
        limit=limit,
        offset=offset,
    )


def backtest_p21_v2_strategy4_replay_attempts_service(
    *,
    experiment_id: str | None = None,
    parameter_set_id: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    return p21_v2_strategy4_replay_attempts_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        parameter_set_id=parameter_set_id,
        symbol=symbol,
        limit=limit,
        offset=offset,
    )


def backtest_p21_v2_export_config_candidate_service(
    *,
    experiment_id: str,
    parameter_set_id: str | None = None,
) -> dict[str, Any]:
    return p21_v2_export_config_candidate_payload(PROJECT_ROOT, experiment_id=experiment_id, parameter_set_id=parameter_set_id)


def backtest_p21_v2_quality_packages_service(
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    mode: str = "materialized",
    limit: int = 50,
) -> dict[str, Any]:
    return p21_v2_quality_packages_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        strategy_line=strategy_line,
        mode=mode,
        limit=limit,
    )


def backtest_p21_v2_quality_summary_service(**filters: Any) -> dict[str, Any]:
    return p21_v2_quality_summary_payload(PROJECT_ROOT, **filters)


def backtest_p21_v2_quality_aggregates_service(**filters: Any) -> dict[str, Any]:
    return p21_v2_quality_aggregates_payload(PROJECT_ROOT, **filters)


def backtest_p21_v2_quality_samples_service(*, limit: int = 200, offset: int = 0, **filters: Any) -> dict[str, Any]:
    return p21_v2_quality_samples_payload(PROJECT_ROOT, limit=limit, offset=offset, **filters)


def backtest_p21_v2_quality_materialize_service(
    *,
    experiment_id: str,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    top_n: int = 1,
    limit: int = 5000,
    dry_run: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    result = p21_v2_quality_materialize_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        strategy_line=strategy_line,
        parameter_set_id=parameter_set_id,
        top_n=top_n,
        limit=limit,
        dry_run=dry_run,
        force=force,
    )
    result["async_followup"] = {
        "recommended": not dry_run,
        "queue": "tq_materialization_jobs",
        "reason": "V4/V5/gate materialization runs through STEP24.18 async job chain",
    }
    return result


def backtest_p21_v2_gate_tq_batch_materialize_service(
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = "all",
    top_n: int = 5,
    limit: int = 500,
    dry_run: bool = True,
) -> dict[str, Any]:
    return p21_v2_gate_batch_materialize_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        strategy_line=strategy_line,
        top_n=top_n,
        limit=limit,
        dry_run=dry_run,
    )


def backtest_p21_v2_gate_features_materialize_service(
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    limit: int = 5000,
    dry_run: bool = True,
) -> dict[str, Any]:
    return p21_v2_gate_materialize_features_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        strategy_line=strategy_line,
        parameter_set_id=parameter_set_id,
        limit=limit,
        dry_run=dry_run,
    )


def backtest_p21_v2_gate_features_service(*, limit: int = 200, **filters: Any) -> dict[str, Any]:
    return p21_v2_gate_features_payload(PROJECT_ROOT, limit=limit, **filters)


def backtest_p21_v2_gate_buckets_rebuild_service(
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    min_samples: int = 5,
    dry_run: bool = True,
) -> dict[str, Any]:
    return p21_v2_gate_rebuild_buckets_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        strategy_line=strategy_line,
        parameter_set_id=parameter_set_id,
        min_samples=min_samples,
        dry_run=dry_run,
    )


def backtest_p21_v2_gate_buckets_service(*, limit: int = 200, **filters: Any) -> dict[str, Any]:
    return p21_v2_gate_buckets_payload(PROJECT_ROOT, limit=limit, **filters)


def backtest_p21_v2_gate_scores_rebuild_service(
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    return p21_v2_gate_rebuild_scores_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        strategy_line=strategy_line,
        parameter_set_id=parameter_set_id,
        dry_run=dry_run,
    )


def backtest_p21_v2_gate_scores_service(*, limit: int = 200, **filters: Any) -> dict[str, Any]:
    return p21_v2_gate_scores_payload(PROJECT_ROOT, limit=limit, **filters)


def backtest_p21_v2_gate_candidates_generate_service(
    *,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    min_test_pf: float = 1.0,
    min_coverage: float = 0.05,
    dry_run: bool = True,
) -> dict[str, Any]:
    return p21_v2_gate_generate_candidates_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        strategy_line=strategy_line,
        parameter_set_id=parameter_set_id,
        min_test_pf=min_test_pf,
        min_coverage=min_coverage,
        dry_run=dry_run,
    )


def backtest_p21_v2_gate_candidates_service(*, limit: int = 200, **filters: Any) -> dict[str, Any]:
    return p21_v2_gate_candidates_payload(PROJECT_ROOT, limit=limit, **filters)


def backtest_p21_v2_gate_recommendations_service(*, limit: int = 100, **filters: Any) -> dict[str, Any]:
    return p21_v2_gate_recommendations_payload(PROJECT_ROOT, limit=limit, **filters)


def trade_quality_v4_materialize_service(*, strategies: list[str] | None = None, limit: int | None = None) -> dict[str, Any]:
    return materialize_v4_payload(PROJECT_ROOT, strategies=strategies, limit=limit)


def trade_quality_v4_gate_candidates_generate_service(
    *,
    strategy_line: str | None = None,
    min_samples: int = 50,
    limit: int = 80,
) -> dict[str, Any]:
    return generate_gate_candidates_v4_payload(PROJECT_ROOT, strategy_line=strategy_line, min_samples=min_samples, limit=limit)


def trade_quality_v4_summary_service() -> dict[str, Any]:
    return tq_v4_summary_payload(PROJECT_ROOT)


def trade_quality_v4_evidence_service(*, limit: int = 200, **filters: Any) -> dict[str, Any]:
    return tq_v4_evidence_payload(PROJECT_ROOT, limit=limit, **filters)


def trade_quality_v4_deep_root_service(*, limit: int = 200, **filters: Any) -> dict[str, Any]:
    return tq_v4_deep_root_payload(PROJECT_ROOT, limit=limit, **filters)


def trade_quality_v4_gate_candidates_service(*, limit: int = 200, **filters: Any) -> dict[str, Any]:
    return tq_v4_gate_candidates_payload(PROJECT_ROOT, limit=limit, **filters)


def trade_quality_v5_materialize_service(*, strategies: list[str] | None = None, limit: int | None = None) -> dict[str, Any]:
    return materialize_v5_payload(PROJECT_ROOT, strategies=strategies, limit=limit)


def trade_quality_v5_gate_candidates_generate_service(
    *,
    strategy_line: str | None = None,
    min_samples: int = 50,
    limit: int = 80,
) -> dict[str, Any]:
    return generate_gate_candidates_v5_payload(PROJECT_ROOT, strategy_line=strategy_line, min_samples=min_samples, limit=limit)


def trade_quality_v5_summary_service() -> dict[str, Any]:
    return tq_v5_summary_payload(PROJECT_ROOT)


def trade_quality_v5_causal_factors_service(*, limit: int = 200, offset: int = 0, **filters: Any) -> dict[str, Any]:
    return tq_v5_causal_factors_payload(PROJECT_ROOT, limit=limit, offset=offset, **filters)


def trade_quality_v5_gate_candidates_service(*, limit: int = 200, offset: int = 0, **filters: Any) -> dict[str, Any]:
    return tq_v5_gate_candidates_payload(PROJECT_ROOT, limit=limit, offset=offset, **filters)


def trade_quality_v5_writer_coverage_service() -> dict[str, Any]:
    return tq_v5_writer_coverage_payload(PROJECT_ROOT)


def research_db_materialize_service(*, limit: int | None = None, dry_run: bool = False) -> dict[str, Any]:
    return research_db_materialize_payload(PROJECT_ROOT, limit=limit, dry_run=dry_run)


def research_db_summary_service() -> dict[str, Any]:
    return research_db_summary_payload(PROJECT_ROOT)


def research_db_trade_facts_service(
    *,
    strategy_line: str | None = None,
    source_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    return research_db_trade_facts_payload(
        PROJECT_ROOT,
        strategy_line=strategy_line,
        source_type=source_type,
        limit=limit,
        offset=offset,
    )


def research_db_entry_features_service(
    *,
    strategy_line: str | None = None,
    source_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    return research_db_entry_features_payload(
        PROJECT_ROOT,
        strategy_line=strategy_line,
        source_type=source_type,
        limit=limit,
        offset=offset,
    )


def research_db_tq_samples_service(
    *,
    strategy_line: str | None = None,
    source_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    return research_db_tq_samples_payload(
        PROJECT_ROOT,
        strategy_line=strategy_line,
        source_type=source_type,
        limit=limit,
        offset=offset,
    )


def research_db_dataset_cards_service(*, limit: int = 20) -> dict[str, Any]:
    return research_db_dataset_cards_payload(PROJECT_ROOT, limit=limit)


def research_db_writer_status_service() -> dict[str, Any]:
    return research_db_writer_status_payload(PROJECT_ROOT)


def research_db_field_coverage_service(
    *,
    strategy_line: str | None = None,
    source_type: str | None = None,
) -> dict[str, Any]:
    return research_db_field_coverage_payload(PROJECT_ROOT, strategy_line=strategy_line, source_type=source_type)


def research_db_lineage_audit_service() -> dict[str, Any]:
    return research_db_lineage_audit_payload(PROJECT_ROOT)


def backtest_p21_v2_ops_footprint_service(
    *,
    row_count_budget: int = 0,
    include_dbstat: bool = False,
) -> dict[str, Any]:
    return p21_v2_ops_footprint_payload(
        PROJECT_ROOT,
        row_count_budget=row_count_budget,
        include_dbstat=include_dbstat,
    )


def backtest_p21_v2_ops_retention_manifest_service(
    *,
    min_trade_count: int = 30,
    write: bool = True,
    shadow_count_budget: int = 75000,
) -> dict[str, Any]:
    return p21_v2_ops_retention_manifest_payload(
        PROJECT_ROOT,
        min_trade_count=min_trade_count,
        write=write,
        shadow_count_budget=shadow_count_budget,
    )


def backtest_p21_v2_ops_serving_rebuild_service(*, limit: int = 200) -> dict[str, Any]:
    return p21_v2_ops_rebuild_serving_read_model_payload(PROJECT_ROOT, limit=limit)


def backtest_p21_v2_ops_serving_summary_service(*, limit: int = 50) -> dict[str, Any]:
    return p21_v2_ops_serving_summary_payload(PROJECT_ROOT, limit=limit)


def backtest_p21_v2_ops_tq_jobs_service(*, limit: int = 50) -> dict[str, Any]:
    return p21_v2_ops_tq_materialization_jobs_payload(PROJECT_ROOT, limit=limit)


def backtest_p21_v2_ops_tq_job_enqueue_service(request: dict[str, Any]) -> dict[str, Any]:
    return p21_v2_ops_enqueue_tq_materialization_job(PROJECT_ROOT, request)


def backtest_p21_v2_ops_tq_job_process_next_service() -> dict[str, Any]:
    return p21_v2_ops_process_next_tq_materialization_job(PROJECT_ROOT)


def backtest_p21_v2_ops_enhanced_validation_service(
    *,
    experiment_id: str | None = None,
    parameter_set_id: str | None = None,
    strategy_line: str | None = None,
    min_test_pf: float = 1.05,
    min_test_trade_count: int = 100,
    min_coverage: float = 0.10,
) -> dict[str, Any]:
    return p21_v2_ops_enhanced_validation_payload(
        PROJECT_ROOT,
        experiment_id=experiment_id,
        parameter_set_id=parameter_set_id,
        strategy_line=strategy_line,
        min_test_pf=min_test_pf,
        min_test_trade_count=min_test_trade_count,
        min_coverage=min_coverage,
    )


def backtest_p21_v2_ops_candidate_export_service(
    *,
    candidate_id: str,
    target_profile: str = "review_only",
) -> dict[str, Any]:
    return p21_v2_ops_export_candidate_audit_package(
        PROJECT_ROOT,
        candidate_id=candidate_id,
        target_profile=target_profile,
    )


def strategy_sandbox_list_service(
    *,
    strategy_line: str | None = None,
    status: str | None = None,
    tag: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    return sandbox_list_payload(strategy_line=strategy_line, status=status, tag=tag, limit=limit)


def strategy_sandbox_create_service(payload: dict[str, Any]) -> dict[str, Any]:
    return sandbox_create_payload(
        strategy_line=payload.get("strategy_line") or "experiment",
        strategy_lines=payload.get("strategy_lines"),
        strategy_version=payload.get("strategy_version") or "review",
        data_scope=payload.get("data_scope") or {},
        config_scope=payload.get("config_scope") or {},
        source_refs=payload.get("source_refs") or [],
        storage_policy=payload.get("storage_policy") or {},
        llm_training_policy=payload.get("llm_training_policy") or {},
        tags=payload.get("tags") or [],
        operation_context=payload,
    )


def strategy_sandbox_universe_service(strategy_line: str = "all", sandbox_id: str | None = None) -> dict[str, Any]:
    return sandbox_universe_payload(strategy_line=strategy_line, sandbox_id=sandbox_id)


def strategy_sandbox_external_integration_health_service() -> dict[str, Any]:
    return sandbox_external_integration_health_payload()


def strategy_sandbox_external_integration_run_service(run_id: str) -> dict[str, Any]:
    return sandbox_external_integration_run_payload(run_id)


def strategy_sandbox_external_integration_audit_events_service(
    *,
    run_id: str | None = None,
    sandbox_id: str | None = None,
    candidate_id: str | None = None,
    gated_run_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    return sandbox_external_integration_audit_events_payload(
        run_id=run_id,
        sandbox_id=sandbox_id,
        candidate_id=candidate_id,
        gated_run_id=gated_run_id,
        limit=limit,
    )


def strategy_sandbox_active_service() -> dict[str, Any]:
    return sandbox_active_payload()


def strategy_sandbox_resource_governor_status_service() -> dict[str, Any]:
    return sandbox_resource_governor_status(PROJECT_ROOT)


def strategy_sandbox_resource_governor_runs_service(
    *,
    resource_lane: str | None = None,
    sandbox_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    return sandbox_resource_governor_runs_payload(
        PROJECT_ROOT,
        resource_lane=resource_lane,
        sandbox_id=sandbox_id,
        limit=limit,
    )


def strategy_sandbox_resource_governor_run_service(run_id: str) -> dict[str, Any]:
    return sandbox_resource_governor_run_payload(PROJECT_ROOT, run_id=run_id)


def strategy_sandbox_resource_governor_rest_budget_service(
    *,
    requires_live_rest: bool = False,
    cache_hit: bool = False,
) -> dict[str, Any]:
    return sandbox_resource_rest_budget_snapshot(
        PROJECT_ROOT,
        requires_live_rest=requires_live_rest,
        cache_hit=cache_hit,
    )


def strategy_sandbox_daemon_writer_status_service(
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    writer_context = None
    if run_id:
        run = sandbox_resource_governor_run_payload(PROJECT_ROOT, run_id=run_id)
        latest = run.get("latest") if isinstance(run.get("latest"), dict) else {}
        got_context = latest.get("writer_context") if isinstance(latest.get("writer_context"), dict) else None
        if got_context:
            writer_context = got_context
    return daemon_writer_status_payload(PROJECT_ROOT, writer_context=writer_context)


def strategy_sandbox_pipeline_run_service(payload: dict[str, Any]) -> dict[str, Any]:
    active = sandbox_active_payload()
    options = payload.get("options") or {}
    context = start_ui_sandbox_pipeline_context(
        project_root=PROJECT_ROOT,
        sandbox_id=payload.get("sandbox_id"),
        active_sandbox_id=active.get("active_sandbox_id"),
        caller_surface=payload.get("source_surface") or "fastapi",
        caller_type=payload.get("caller_type") or "local_ui",
        dry_run=bool(payload.get("dry_run", True)),
        requires_live_rest=bool(payload.get("requires_live_rest", False)),
        cache_hit=bool(payload.get("cache_hit", False)),
        options=options,
    )
    if not context.get("accepted") or bool(payload.get("dry_run", True)):
        return context
    try:
        result = run_sandbox_paper_pipeline(
            PROJECT_ROOT,
            sandbox_id=str(context["sandbox_id"]),
            run_id=str(context["run_id"]),
            cycle_id=str(context["cycle_id"]),
            writer_context=context.get("writer_context") or {},
            docs=options.get("docs") if isinstance(options.get("docs"), dict) else None,
            candles_by_symbol=options.get("candles_by_symbol") if isinstance(options.get("candles_by_symbol"), dict) else None,
            max_ticks=options.get("max_ticks"),
            options=options,
        )
        finish = finish_ui_sandbox_pipeline_context(
            project_root=PROJECT_ROOT,
            run_id=str(context["run_id"]),
            sandbox_id=str(context["sandbox_id"]),
            status=str(result.get("status") or "completed"),
            result=result,
        )
        return {**context, "status": result.get("status") or context.get("status"), "execution_result": result, "finish": finish}
    except Exception as exc:
        finish = finish_ui_sandbox_pipeline_context(
            project_root=PROJECT_ROOT,
            run_id=str(context["run_id"]),
            sandbox_id=str(context["sandbox_id"]),
            status="failed",
            result={"error_type": type(exc).__name__, "error": str(exc)},
        )
        return {**context, "status": "failed", "reason_code": "sandbox_paper_pipeline_failed", "error": str(exc), "finish": finish}


def strategy_sandbox_full_pipeline_run_service(payload: dict[str, Any]) -> dict[str, Any]:
    active = sandbox_active_payload()
    options = payload.get("options") or {}
    options = {"pipeline_mode": "sandbox_full_pipeline", **options}
    context = start_ui_sandbox_pipeline_context(
        project_root=PROJECT_ROOT,
        sandbox_id=payload.get("sandbox_id"),
        active_sandbox_id=active.get("active_sandbox_id"),
        caller_surface=payload.get("source_surface") or "fastapi",
        caller_type=payload.get("caller_type") or "local_ui",
        dry_run=bool(payload.get("dry_run", False)),
        requires_live_rest=bool(payload.get("requires_live_rest", False)),
        cache_hit=bool(payload.get("cache_hit", False)),
        options=options,
    )
    if not context.get("accepted") or bool(payload.get("dry_run", False)):
        return context
    try:
        result = run_sandbox_full_pipeline(
            PROJECT_ROOT,
            sandbox_id=str(context["sandbox_id"]),
            run_id=str(context["run_id"]),
            cycle_id=str(context["cycle_id"]),
            writer_context=context.get("writer_context") or {},
            options=options,
        )
        finish = finish_ui_sandbox_pipeline_context(
            project_root=PROJECT_ROOT,
            run_id=str(context["run_id"]),
            sandbox_id=str(context["sandbox_id"]),
            status=str(result.get("status") or "completed"),
            result=result,
        )
        return {**context, "status": result.get("status") or context.get("status"), "execution_result": result, "finish": finish}
    except Exception as exc:
        finish = finish_ui_sandbox_pipeline_context(
            project_root=PROJECT_ROOT,
            run_id=str(context["run_id"]),
            sandbox_id=str(context["sandbox_id"]),
            status="failed",
            result={"error_type": type(exc).__name__, "error": str(exc)},
        )
        return {**context, "status": "failed", "reason_code": "sandbox_full_pipeline_failed", "error": str(exc), "finish": finish}


def strategy_sandbox_pipeline_stop_service(payload: dict[str, Any]) -> dict[str, Any]:
    return stop_ui_sandbox_pipeline_context(
        project_root=PROJECT_ROOT,
        run_id=payload.get("run_id"),
        cancel_reason=payload.get("cancel_reason") or "manual_stop",
    )


def strategy_sandbox_set_active_service(sandbox_id: str | None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    before = sandbox_active_payload().get("active_sandbox_id")
    if before and sandbox_id and str(before) != str(sandbox_id):
        stop_ui_sandbox_pipeline_context(
            project_root=PROJECT_ROOT,
            cancel_reason="active_sandbox_switch",
        )
    return sandbox_set_active_payload(sandbox_id, operation_context=payload or {})


def strategy_sandbox_get_service(sandbox_id: str) -> dict[str, Any]:
    return sandbox_get_payload(sandbox_id)


def strategy_sandbox_summary_service(sandbox_id: str) -> dict[str, Any]:
    return sandbox_summary_payload(sandbox_id)


def strategy_sandbox_branches_service(sandbox_id: str) -> dict[str, Any]:
    return sandbox_branches_payload(sandbox_id)


def strategy_sandbox_leaderboard_service(sandbox_id: str) -> dict[str, Any]:
    return sandbox_leaderboard_payload(sandbox_id)


def strategy_sandbox_trade_quality_compare_service(sandbox_id: str) -> dict[str, Any]:
    return sandbox_trade_quality_compare_payload(sandbox_id)


def strategy_sandbox_gate_compare_service(sandbox_id: str) -> dict[str, Any]:
    return sandbox_gate_compare_payload(sandbox_id)


def strategy_sandbox_db_health_service(sandbox_id: str) -> dict[str, Any]:
    return sandbox_db_health_payload(sandbox_id)


def strategy_sandbox_delete_service(sandbox_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return sandbox_delete_payload(
        sandbox_id,
        mode=payload.get("mode") or "soft_delete",
        reason=payload.get("reason") or "",
        confirm=bool(payload.get("confirm")),
        operation_context=payload,
    )


def strategy_sandbox_job_service(sandbox_id: str, job_type: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
    return sandbox_job_payload(sandbox_id, job_type, options or {}, operation_context=options or {})


def strategy_sandbox_full_backtest_run_create_service(sandbox_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return sandbox_create_full_backtest_run_payload(sandbox_id, payload, operation_context=payload)


def strategy_sandbox_full_backtest_run_service(sandbox_id: str, run_id: str) -> dict[str, Any]:
    return sandbox_full_backtest_run_payload(sandbox_id, run_id)


def strategy_sandbox_full_backtest_run_cancel_service(
    sandbox_id: str,
    run_id: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return sandbox_cancel_full_backtest_run_payload(sandbox_id, run_id, operation_context=payload or {})


def strategy_sandbox_full_backtest_run_resume_service(
    sandbox_id: str,
    run_id: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return sandbox_resume_full_backtest_run_payload(sandbox_id, run_id, operation_context=payload or {})


def strategy_sandbox_trade_candidates_service(
    sandbox_id: str,
    strategy_line: str,
    *,
    run_id: str | None = None,
    source_mode: str = "backtest",
    symbol: str | None = None,
    cursor: str | None = None,
    limit: int = 100,
    since: str | None = None,
    include_features: bool = True,
) -> dict[str, Any]:
    return sandbox_trade_candidates_payload(
        sandbox_id,
        strategy_line,
        run_id=run_id,
        source_mode=source_mode,
        symbol=symbol,
        cursor=cursor,
        limit=limit,
        since=since,
        include_features=include_features,
    )


def strategy_sandbox_gate_action_ingest_service(sandbox_id: str, strategy_line: str, payload: dict[str, Any]) -> dict[str, Any]:
    return sandbox_ingest_gate_action_payload(sandbox_id, strategy_line, payload, operation_context=payload)


def strategy_sandbox_gated_replay_service(sandbox_id: str, strategy_line: str, payload: dict[str, Any]) -> dict[str, Any]:
    return sandbox_gated_replay_payload(sandbox_id, strategy_line, payload, operation_context=payload)


def strategy_sandbox_gated_paper_shadow_service(sandbox_id: str, strategy_line: str, payload: dict[str, Any]) -> dict[str, Any]:
    return sandbox_gated_paper_shadow_payload(sandbox_id, strategy_line, payload, operation_context=payload)


def strategy_sandbox_gated_orders_service(
    sandbox_id: str,
    strategy_line: str,
    *,
    run_id: str | None = None,
    gated_run_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    return sandbox_gated_orders_payload(sandbox_id, strategy_line, run_id=run_id, gated_run_id=gated_run_id, limit=limit)


def strategy_sandbox_gated_trade_quality_samples_service(
    sandbox_id: str,
    strategy_line: str,
    *,
    gated_run_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    return sandbox_gated_trade_quality_samples_payload(sandbox_id, strategy_line, gated_run_id=gated_run_id, limit=limit)


def strategy_sandbox_gated_performance_service(
    sandbox_id: str,
    strategy_line: str,
    *,
    gated_run_id: str | None = None,
) -> dict[str, Any]:
    return sandbox_gated_performance_payload(sandbox_id, strategy_line, gated_run_id=gated_run_id)


def strategy_sandbox_code_overlay_service(sandbox_id: str, strategy_line: str) -> dict[str, Any]:
    return sandbox_code_overlay_payload(sandbox_id, strategy_line)


def strategy_sandbox_create_code_overlay_service(
    sandbox_id: str,
    strategy_line: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return sandbox_create_code_overlay_payload(sandbox_id, strategy_line, operation_context=payload or {})


def strategy_sandbox_add_code_patch_service(sandbox_id: str, strategy_line: str, payload: dict[str, Any]) -> dict[str, Any]:
    return sandbox_add_code_patch_payload(sandbox_id, strategy_line, payload, operation_context=payload)


def strategy_sandbox_runtime_build_service(sandbox_id: str, strategy_line: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return sandbox_build_runtime_payload(sandbox_id, strategy_line, payload or {}, operation_context=payload or {})


def strategy_sandbox_runtime_smoke_service(sandbox_id: str, strategy_line: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return sandbox_runtime_smoke_payload(sandbox_id, strategy_line, payload or {}, operation_context=payload or {})


def trade_quality_diagnostics_sync_status_service() -> dict[str, Any]:
    return diagnostic_sync_status_payload(PROJECT_ROOT, config=_paper_config())


def trade_quality_diagnostics_replay_ledger_service(*, limit: int = 200) -> dict[str, Any]:
    return diagnostic_replay_ledger_payload(_paper_db_path(), limit=limit)


def trade_quality_diagnostics_replay_service(
    *,
    dry_run: bool = True,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
) -> dict[str, Any]:
    return diagnostic_replay_payload(
        PROJECT_ROOT,
        write=not dry_run,
        limit=limit,
        source=source,
        archive_id=archive_id,
        config=_paper_config(),
    )


def trade_quality_entry_features_backfill_service(
    *,
    dry_run: bool = True,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return diagnostic_entry_feature_payload(
        PROJECT_ROOT,
        write=not dry_run,
        limit=limit,
        source=source,
        archive_id=archive_id,
        force=force,
        config=_paper_config(),
    )


def trade_quality_entry_microstructure_backfill_service(
    *,
    dry_run: bool = True,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
    evidence_window_sec: int = 180,
) -> dict[str, Any]:
    return diagnostic_entry_microstructure_payload(
        PROJECT_ROOT,
        write=not dry_run,
        limit=limit,
        source=source,
        archive_id=archive_id,
        force=force,
        evidence_window_sec=evidence_window_sec,
        config=_paper_config(),
    )


def trade_quality_entry_market_context_backfill_service(
    *,
    dry_run: bool = True,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return diagnostic_entry_market_context_payload(
        PROJECT_ROOT,
        write=not dry_run,
        limit=limit,
        source=source,
        archive_id=archive_id,
        force=force,
        config=_paper_config(),
    )


def trade_quality_entry_context_v3_backfill_service(
    *,
    dry_run: bool = True,
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return diagnostic_entry_context_v3_payload(
        PROJECT_ROOT,
        write=not dry_run,
        limit=limit,
        source=source,
        archive_id=archive_id,
        force=force,
        config=_paper_config(),
    )


def trade_quality_diagnostics_refresh_enrich_service(
    *,
    dry_run: bool = False,
    limit: int | None = 100,
    source: str = "current_paper",
    archive_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    stages: list[dict[str, Any]] = []

    def run_stage(name: str, fn: Any) -> None:
        try:
            result = fn()
            stages.append({"stage": name, "status": "ok", "result": result})
        except Exception as exc:
            stages.append({"stage": name, "status": "failed", "error": str(exc)})
            raise

    try:
        run_stage("sync", lambda: trade_quality_diagnostics_sync_service(dry_run=dry_run, limit=limit, source=source))
        run_stage("replay", lambda: trade_quality_diagnostics_replay_service(dry_run=dry_run, limit=limit, source=source, archive_id=archive_id))
        run_stage("entry_feature", lambda: trade_quality_entry_features_backfill_service(dry_run=dry_run, limit=limit, source=source, archive_id=archive_id, force=force))
        run_stage("market_context", lambda: trade_quality_entry_market_context_backfill_service(dry_run=dry_run, limit=limit, source=source, archive_id=archive_id, force=force))
        run_stage("micro_context", lambda: trade_quality_entry_microstructure_backfill_service(dry_run=dry_run, limit=limit, source=source, archive_id=archive_id, force=force))
        run_stage("entry_context_v3", lambda: trade_quality_entry_context_v3_backfill_service(dry_run=dry_run, limit=limit, source=source, archive_id=archive_id, force=force))
        if not dry_run:
            run_stage("v5_causal_factors", lambda: materialize_v5_payload(PROJECT_ROOT, limit=limit))
        summary = trade_quality_diagnostics_summary_service(source=source, archive_id=archive_id)
        stages.append({"stage": "reload_summary", "status": "ok", "result": {"sample_count": summary.get("summary", {}).get("sample_count")}})
        return {
            "schema_version": "12.57",
            "mode": "dry_run" if dry_run else "run",
            "source": source,
            "archive_id": archive_id,
            "limit": limit,
            "force": bool(force),
            "status": "ok",
            "stages": stages,
            "summary": summary.get("summary", {}),
        }
    except Exception:
        return {
            "schema_version": "12.57",
            "mode": "dry_run" if dry_run else "run",
            "source": source,
            "archive_id": archive_id,
            "limit": limit,
            "force": bool(force),
            "status": "failed",
            "stages": stages,
        }


def trade_quality_recommendation_rules_service(
    *,
    rule_type: str | None = None,
    strategy_line: str | None = None,
    side: str | None = None,
    symbol: str | None = None,
    sample_source: str | None = None,
    config_profile: str | None = None,
    severity: str | None = None,
    mode: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    if strategy_line:
        _validate_paper_line(strategy_line)
    return recommendation_rules_payload(
        _paper_db_path(),
        rule_type=rule_type,
        strategy_line=strategy_line,
        side=side,
        symbol=symbol,
        sample_source=sample_source,
        config_profile=config_profile,
        severity=severity,
        mode=mode,
        limit=limit,
    )


def trade_quality_recommendation_rules_rebuild_service() -> dict[str, Any]:
    return rebuild_recommendation_rules(_paper_db_path())


def trade_quality_recommendation_validation_service(
    *,
    sample_source: str | None = "live",
    rule_type: str | None = None,
    strategy_line: str | None = None,
    side: str | None = None,
    symbol: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    if strategy_line:
        _validate_paper_line(strategy_line)
    return recommendation_validation_payload(
        _paper_db_path(),
        sample_source=sample_source,
        rule_type=rule_type,
        strategy_line=strategy_line,
        side=side,
        symbol=symbol,
        limit=limit,
    )


def trade_quality_recommendation_promotions_service(
    *,
    profile: str | None = None,
    strategy_line: str | None = None,
    enabled: bool | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    if strategy_line:
        _validate_paper_line(strategy_line)
    return promotions_payload(_paper_db_path(), profile=profile, strategy_line=strategy_line, enabled=enabled, limit=limit)


def trade_quality_recommendation_promotion_dry_run_service(
    *,
    rule_id: str,
    profile: str,
    strategy_line: str | None,
    mode: str,
) -> dict[str, Any]:
    if strategy_line:
        _validate_paper_line(strategy_line)
    try:
        return promotion_dry_run(_paper_db_path(), rule_id=rule_id, profile=profile, strategy_line=strategy_line, mode=mode)
    except ValueError as exc:
        raise ApiServiceError("trade_quality_promotion_invalid", str(exc), {"rule_id": rule_id, "profile": profile, "mode": mode}) from exc


def trade_quality_recommendation_promotion_apply_service(
    *,
    rule_id: str,
    profile: str,
    strategy_line: str | None,
    mode: str,
    reason: str,
) -> dict[str, Any]:
    if strategy_line:
        _validate_paper_line(strategy_line)
    try:
        return apply_promotion(
            _paper_db_path(),
            rule_id=rule_id,
            profile=profile,
            strategy_line=strategy_line,
            mode=mode,
            reason=reason,
        )
    except ValueError as exc:
        raise ApiServiceError("trade_quality_promotion_invalid", str(exc), {"rule_id": rule_id, "profile": profile, "mode": mode}) from exc


def trade_quality_recommendation_promotion_disable_service(*, promotion_id: str, reason: str) -> dict[str, Any]:
    try:
        return disable_promotion(_paper_db_path(), promotion_id=promotion_id, reason=reason)
    except ValueError as exc:
        raise ApiServiceError("trade_quality_promotion_invalid", str(exc), {"promotion_id": promotion_id}) from exc


def trade_quality_promotion_candidates_service(*, limit: int = 200) -> dict[str, Any]:
    return promotion_candidates_payload(_paper_db_path(), limit=limit)


def trade_quality_promotion_candidates_rebuild_service(*, limit: int = 200, write: bool = True) -> dict[str, Any]:
    return rebuild_promotion_candidates(_paper_db_path(), limit=limit, write=write)


def _filter_rows(
    rows: list[dict[str, Any]],
    *,
    symbol: str | None = None,
    order_id: str | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    got = rows
    if symbol:
        sym = symbol.upper()
        got = [row for row in got if str(row.get("symbol") or "").upper() == sym]
    if order_id:
        got = [row for row in got if str(row.get("order_id") or row.get("id") or "") == order_id]
    if run_id:
        got = [row for row in got if str(row.get("source_run_id") or "") == run_id]
    return got


def _realism_metrics_from_fills(fills: list[dict[str, Any]]) -> dict[str, Any]:
    entry_fills = [row for row in fills if row.get("action") == "entry"]
    drift = [float(row.get("entry_drift_bps") or 0) for row in entry_fills if row.get("entry_drift_bps") is not None]
    slippage_bps = [float(row.get("slippage_bps") or 0) for row in fills if row.get("slippage_bps") is not None]
    delay = [float(row.get("fill_delay_sec") or 0) for row in entry_fills if row.get("fill_delay_sec") is not None]
    fees = sum(float(row.get("fee_usdt") or 0) for row in fills)
    slippage_usdt = sum(float(row.get("slippage_usdt") or 0) for row in fills)
    same_candle = len([row for row in fills if row.get("same_candle_policy")])
    return {
        "fill_count": len(fills),
        "entry_fill_count": len(entry_fills),
        "avg_entry_drift_bps": round(sum(drift) / len(drift), 4) if drift else 0.0,
        "avg_slippage_bps": round(sum(slippage_bps) / len(slippage_bps), 4) if slippage_bps else 0.0,
        "avg_fill_delay_sec": round(sum(delay) / len(delay), 4) if delay else 0.0,
        "fee_usdt": round(fees, 8),
        "slippage_usdt": round(slippage_usdt, 8),
        "same_candle_policy_count": same_candle,
    }


def paper_realism_payload(
    kind: str,
    *,
    line: str | None = None,
    symbol: str | None = None,
    run_id: str | None = None,
    order_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    _validate_paper_line(line)
    db_path = _paper_db_path()
    safe_limit = max(1, min(int(limit or 100), 500))
    fills = _filter_rows(_sqlite_rows(db_path, "paper_fills", safe_limit, line=line), symbol=symbol, order_id=order_id, run_id=run_id)
    orders = _filter_rows(_sqlite_rows(db_path, "paper_orders", safe_limit, line=line), symbol=symbol, order_id=order_id, run_id=run_id)
    positions = _filter_rows(_sqlite_rows(db_path, "paper_positions", safe_limit, line=line), symbol=symbol, order_id=order_id, run_id=run_id)
    intents = _filter_rows(_sqlite_rows(db_path, "paper_intent_inbox", safe_limit, line=line), symbol=symbol, run_id=run_id)
    plans = _filter_rows(_sqlite_rows(db_path, "paper_trade_plans", safe_limit, line=line), symbol=symbol, run_id=run_id)
    skips = _filter_rows(_sqlite_rows(db_path, "paper_skip_ledger", safe_limit, line=line), symbol=symbol, run_id=run_id)
    metrics = _realism_metrics_from_fills(fills)
    base = {
        "schema_version": "12.49",
        "source": f"paper_{kind}",
        "line": line,
        "symbol": symbol,
        "run_id": run_id,
        "order_id": order_id,
        "db_path": str(db_path),
        "db_exists": db_path.exists(),
        "metrics": metrics,
        "warnings": [],
    }
    if kind == "fills":
        return {**base, "fills": fills}
    if kind == "realism-metrics":
        return {**base, "fills_sample": fills[:20]}
    if kind == "order-trace":
        return {**base, "orders": orders, "positions": positions, "fills": fills, "intents": intents, "plans": plans, "skips": skips}
    if kind == "reconciliation":
        return {
            **base,
            "counts": {
                "intents": len(intents),
                "plans": len(plans),
                "orders": len(orders),
                "positions": len(positions),
                "fills": len(fills),
                "skips": len(skips),
            },
            "intents": intents[:50],
            "plans": plans[:50],
            "orders": orders[:50],
            "positions": positions[:50],
            "fills": fills[:50],
            "skips": skips[:50],
        }
    raise ApiServiceError("paper_realism_kind_invalid", "unsupported paper realism payload", {"kind": kind})


def paper_payload(kind: str, *, line: str | None = None, symbol: str | None = None) -> dict[str, Any]:
    _validate_paper_line(line)
    db_path = _paper_db_path()
    if kind == "summary":
        summary_path = _paper_summary_path()
        if summary_path.exists():
            data = read_json(summary_path)
            if line and isinstance(data, dict):
                return {
                    "line": line,
                    "db_path": str(db_path),
                    "summary_path": str(summary_path),
                    "account": (data.get("accounts") or {}).get(line, {}),
                    "stats": ((data.get("stats") or {}).get("by_line") or {}).get(line, {}),
                    "orders": ((data.get("orders") or {}).get(line) or []),
                    "positions": ((data.get("positions") or {}).get(line) or []),
                    "open_positions": ((data.get("open_positions") or {}).get(line) or []),
                    "settled_positions": ((data.get("settled_positions") or {}).get(line) or []),
                    "closed_orders": ((data.get("closed_orders") or {}).get(line) or []),
                    "skipped_signals": [
                        row for row in (data.get("skipped_signals") or []) if row.get("strategy_line") == line or row.get("line") == line
                    ],
                    "recent_fills": ((data.get("recent_fills") or {}).get(line) or []),
                    "worker": data.get("worker") or {},
                }
            return data
        return {
            "db_path": str(db_path),
            "exists": db_path.exists(),
            "accounts": _sqlite_rows(db_path, "paper_accounts", 20, line=line),
            "open_positions": _sqlite_rows(db_path, "paper_positions", 200, line=line),
            "pending_orders": _sqlite_rows(db_path, "paper_orders", 200, line=line),
        }
    if kind == "stats":
        summary_path = _paper_summary_path()
        if summary_path.exists():
            data = read_json(summary_path)
            stats = data.get("stats") if isinstance(data, dict) else {}
            return ((stats.get("by_line") or {}).get(line) if line else stats) or {}
        return {}
    if kind == "detail":
        fills = _sqlite_rows(db_path, "paper_fills", 500, line=line)
        orders = _sqlite_rows(db_path, "paper_orders", 500, line=line)
        positions = _sqlite_rows(db_path, "paper_positions", 500, line=line)
        if symbol:
            sym = symbol.upper()
            fills = [row for row in fills if str(row.get("symbol") or "").upper() == sym]
            orders = [row for row in orders if str(row.get("symbol") or "").upper() == sym]
            positions = [row for row in positions if str(row.get("symbol") or "").upper() == sym]
        markers = [
            {
                "type": row.get("action"),
                "time": row.get("candle_open_time_ms"),
                "price": row.get("fill_price"),
                "label": "开仓" if row.get("action") == "entry" else "平仓",
                "order_id": row.get("order_id"),
                "fill_id": row.get("id"),
            }
            for row in fills
        ]
        levels: list[dict[str, Any]] = []
        primary = positions[0] if positions else (orders[0] if orders else {})
        if primary:
            levels = [
                {"type": "ENTRY", "label": "开仓价", "price": primary.get("entry_price") or primary.get("filled_entry_price")},
                {"type": "SL", "label": "止损", "price": primary.get("stop_loss")},
                {"type": "TP", "label": "止盈", "price": primary.get("take_profit")},
            ]
            levels = [row for row in levels if row.get("price") not in (None, "")]
        candles = []
        if symbol:
            try:
                candles = [c.__dict__ for c in fetch_binance_1m_candles(symbol, limit=120)]
            except Exception:
                candles = []
        return {
            "line": line,
            "symbol": symbol,
            "candles": candles,
            "markers": markers,
            "levels": levels,
            "fills": fills,
            "orders": orders,
            "positions": positions,
        }
    if kind == "intents":
        return {"db_path": str(db_path), "exists": db_path.exists(), "line": line, "rows": _sqlite_rows(db_path, "paper_intent_inbox", line=line)}
    if kind == "epochs":
        return {"db_path": str(db_path), "exists": db_path.exists(), "line": line, "rows": _sqlite_rows(db_path, "paper_reset_epochs", line=line)}
    if kind == "trace":
        rows = {
            "orders": _sqlite_rows(db_path, "paper_orders", 500, line=line),
            "positions": _sqlite_rows(db_path, "paper_positions", 500, line=line),
            "fills": _sqlite_rows(db_path, "paper_fills", 500, line=line),
            "intents": _sqlite_rows(db_path, "paper_intent_inbox", 500, line=line),
            "epochs": _sqlite_rows(db_path, "paper_reset_epochs", 50, line=line),
        }
        if symbol:
            sym = symbol.upper()
            for key in ("orders", "positions", "fills", "intents"):
                rows[key] = [row for row in rows[key] if str(row.get("symbol") or "").upper() == sym]
        return {"db_path": str(db_path), "exists": db_path.exists(), "line": line, "symbol": symbol, **rows}
    table_map = {
        "accounts": "paper_accounts",
        "orders": "paper_orders",
        "positions": "paper_positions",
        "fills": "paper_fills",
        "performance": "paper_performance_snapshots",
    }
    return {"db_path": str(db_path), "exists": db_path.exists(), "line": line, "rows": _sqlite_rows(db_path, table_map[kind], line=line)}


PAPER_LITE_ROW_KEYS = {
    "id",
    "order_id",
    "position_id",
    "intent_id",
    "plan_id",
    "symbol",
    "side",
    "strategy_line",
    "line",
    "status",
    "order_type",
    "source_action",
    "source_entry_mode",
    "source_executable",
    "source_plan_hash",
    "entry_price",
    "filled_entry_price",
    "planned_entry_price",
    "exit_price",
    "stop_loss",
    "take_profit",
    "tp1",
    "quantity",
    "remaining_quantity",
    "planned_quantity",
    "notional_usdt",
    "margin_usdt",
    "leverage",
    "realized_pnl_usdt",
    "unrealized_pnl_usdt",
    "fee_usdt",
    "slippage_usdt",
    "slippage_bps",
    "net_pnl",
    "net_R",
    "exit_reason",
    "reason",
    "reason_code",
    "skip_reason",
    "reset_epoch_id",
    "created_at",
    "updated_at",
    "opened_at",
    "closed_at",
    "filled_at",
    "source_run_id",
    "source_cycle_id",
}


def _slim_paper_row(row: Any) -> Any:
    if not isinstance(row, dict):
        return row
    return {key: row.get(key) for key in PAPER_LITE_ROW_KEYS if key in row and row.get(key) not in (None, "")}


def _top_n_rows(rows: Any, limit: int = 20) -> list[Any]:
    return [_slim_paper_row(row) for row in list(rows or [])[: max(0, min(int(limit), 50))]]


def _worker_lite(worker: Any) -> dict[str, Any]:
    if not isinstance(worker, dict):
        return {}
    keep = (
        "status",
        "generated_at",
        "last_tick_at",
        "heartbeat_at",
        "heartbeat_age_sec",
        "active_symbols",
        "reason_codes",
        "errors",
    )
    return {key: worker.get(key) for key in keep if key in worker}


def paper_summary_lite(*, line: str | None = None, limit: int = 20) -> dict[str, Any]:
    _validate_paper_line(line)
    db_path = _paper_db_path()
    summary_path = _paper_summary_path()
    safe_limit = max(1, min(int(limit or 20), 50))
    if summary_path.exists():
        data = read_json(summary_path)
        if not isinstance(data, dict):
            data = {}
        if line:
            orders = ((data.get("orders") or {}).get(line) or [])
            positions = ((data.get("positions") or {}).get(line) or [])
            open_positions = ((data.get("open_positions") or {}).get(line) or [])
            settled_positions = ((data.get("settled_positions") or {}).get(line) or [])
            closed_orders = ((data.get("closed_orders") or {}).get(line) or [])
            skipped_signals = [
                row for row in (data.get("skipped_signals") or []) if row.get("strategy_line") == line or row.get("line") == line
            ]
            recent_fills = ((data.get("recent_fills") or {}).get(line) or [])
            return {
                "schema_version": "12.59",
                "payload_scope": "lite",
                "line": line,
                "db_path": str(db_path),
                "summary_path": str(summary_path),
                "summary_generated_at": data.get("generated_at"),
                "account": (data.get("accounts") or {}).get(line, {}),
                "stats": ((data.get("stats") or {}).get("by_line") or {}).get(line, {}),
                "counts": {
                    "orders": len(orders),
                    "positions": len(positions),
                    "open_positions": len(open_positions),
                    "settled_positions": len(settled_positions),
                    "closed_orders": len(closed_orders),
                    "skipped_signals": len(skipped_signals),
                    "recent_fills": len(recent_fills),
                },
                "orders": _top_n_rows(orders, safe_limit),
                "positions": _top_n_rows(positions, safe_limit),
                "open_positions": _top_n_rows(open_positions, safe_limit),
                "settled_positions": _top_n_rows(settled_positions, safe_limit),
                "closed_orders": _top_n_rows(closed_orders, safe_limit),
                "skipped_signals": _top_n_rows(skipped_signals, safe_limit),
                "recent_fills": _top_n_rows(recent_fills, safe_limit),
                "worker": _worker_lite(data.get("worker")),
            }
        counts: dict[str, Any] = {}
        for key in ("orders", "positions", "open_positions", "settled_positions", "closed_orders", "recent_fills"):
            got = data.get(key) or {}
            counts[key] = {k: len(v or []) for k, v in got.items()} if isinstance(got, dict) else len(got or [])
        skipped = data.get("skipped_signals") or []
        return {
            "schema_version": "12.59",
            "payload_scope": "lite",
            "db_path": str(db_path),
            "summary_path": str(summary_path),
            "summary_generated_at": data.get("generated_at"),
            "accounts": data.get("accounts") or {},
            "stats": data.get("stats") or {},
            "counts": {**counts, "skipped_signals": len(skipped)},
            "open_positions": {
                k: _top_n_rows(v, safe_limit)
                for k, v in ((data.get("open_positions") or {}).items() if isinstance(data.get("open_positions"), dict) else [])
            },
            "closed_orders": {
                k: _top_n_rows(v, safe_limit)
                for k, v in ((data.get("closed_orders") or {}).items() if isinstance(data.get("closed_orders"), dict) else [])
            },
            "skipped_signals": _top_n_rows(skipped, safe_limit),
            "worker": _worker_lite(data.get("worker")),
        }
    return {
        "schema_version": "12.59",
        "payload_scope": "lite",
        "db_path": str(db_path),
        "exists": db_path.exists(),
        "accounts": _sqlite_rows(db_path, "paper_accounts", safe_limit, line=line),
        "open_positions": _sqlite_rows(db_path, "paper_positions", safe_limit, line=line),
        "pending_orders": _sqlite_rows(db_path, "paper_orders", safe_limit, line=line),
        "counts": {},
    }


def paper_archive_reset(payload: dict[str, Any]) -> dict[str, Any]:
    line = str(payload.get("strategy_line") or payload.get("line") or "")
    _validate_paper_line(line)
    if not line:
        raise ApiServiceError("invalid_strategy_line", "strategy_line is required", {})
    try:
        return archive_reset_strategy(
            PROJECT_ROOT,
            strategy_line=line,
            profile_name=str(payload.get("profile_name") or ""),
            notes=str(payload.get("notes") or ""),
            config=_paper_config(),
        )
    except ValueError as exc:
        code = str(exc)
        raise ApiServiceError(code, code, {"strategy_line": line}) from exc


def paper_experiments(*, line: str | None = None, limit: int = 50) -> dict[str, Any]:
    _validate_paper_line(line)
    try:
        return list_experiments(PROJECT_ROOT, config=_paper_config(), line=line, limit=limit)
    except ValueError as exc:
        code = str(exc)
        raise ApiServiceError(code, code, {"line": line}) from exc


def paper_experiment_detail(experiment_id: str) -> dict[str, Any]:
    if not experiment_id or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789TtZz_-:" for ch in experiment_id):
        raise ApiServiceError("experiment_id_invalid", "experiment_id contains unsupported characters", {"experiment_id": experiment_id})
    try:
        return get_experiment(PROJECT_ROOT, experiment_id, config=_paper_config())
    except FileNotFoundError as exc:
        raise ApiServiceError("file_missing", "paper experiment archive does not exist", {"experiment_id": experiment_id}) from exc


def paper_daemon_payload(action: str) -> dict[str, Any]:
    cfg = _paper_config()
    if action == "status":
        return paper_read_status(PROJECT_ROOT, cfg)
    if action == "run-once":
        return paper_run_once(PROJECT_ROOT, config=cfg)
    if action == "stop":
        return run_cli(["paper-daemon", "stop", "--project-root", str(PROJECT_ROOT), "--stdout-json"])
    if action in {"start", "restart"}:
        return run_cli(["paper-daemon", action, "--project-root", str(PROJECT_ROOT), "--stdout-json"])
    raise ApiServiceError("paper_daemon_action_invalid", "unsupported paper daemon action", {"action": action})


def _pipeline_report_for_run(run_id: str | None = None) -> tuple[dict[str, Any], Path | None]:
    if run_id:
        archive = PROJECT_ROOT / "DATA" / "reports" / "pipeline_runs" / run_id / "strategy_pipeline_report.json"
        if archive.exists():
            data = read_json(archive)
            return (data if isinstance(data, dict) else {}), archive
        latest = PROJECT_ROOT / "DATA" / "reports" / "latest_strategy_pipeline_report.json"
        if latest.exists():
            data = read_json(latest)
            if isinstance(data, dict) and data.get("run_id") == run_id:
                return data, latest
        return {}, None
    latest = PROJECT_ROOT / "DATA" / "reports" / "latest_strategy_pipeline_report.json"
    if latest.exists():
        data = read_json(latest)
        return (data if isinstance(data, dict) else {}), latest
    return {}, None


def _paper_table_count_for_run(db_path: Path, table: str, run_id: str | None) -> int:
    if not run_id or not db_path.exists():
        return 0
    try:
        with sqlite3.connect(db_path) as conn:
            columns = [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            if "source_run_id" not in columns:
                return 0
            row = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE source_run_id = ?", (run_id,)).fetchone()
            return int(row[0] or 0) if row else 0
    except sqlite3.Error:
        return 0


def _paper_rows_for_run(db_path: Path, table: str, run_id: str | None, *, limit: int = 50) -> list[dict[str, Any]]:
    if not run_id or not db_path.exists():
        return []
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            columns = [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            if "source_run_id" not in columns:
                return []
            order_column = "rowid"
            for candidate in ("created_at", "consumed_at", "opened_at", "filled_at", "updated_at"):
                if candidate in columns:
                    order_column = candidate
                    break
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE source_run_id = ? ORDER BY {order_column} DESC, rowid DESC LIMIT ?",
                (run_id, max(1, min(int(limit or 50), 200))),
            ).fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error:
        return []


def paper_consumption_status(*, run_id: str | None = None, limit: int = 50) -> dict[str, Any]:
    if run_id and any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789TtZz_-:" for ch in run_id):
        raise ApiServiceError("run_id_invalid", "run_id contains unsupported characters", {"run_id": run_id})
    report, report_path = _pipeline_report_for_run(run_id)
    resolved_run_id = str(run_id or report.get("run_id") or "")
    db_path = _paper_db_path()
    cfg = _paper_config()
    barrier = report.get("paper_settlement_barrier") if isinstance(report.get("paper_settlement_barrier"), dict) else {}
    paper_run_once = barrier.get("paper_run_once") if isinstance(barrier.get("paper_run_once"), dict) else {}
    tables = [
        "paper_intent_inbox",
        "paper_trade_plans",
        "paper_orders",
        "paper_skip_ledger",
        "paper_fills",
        "paper_positions",
    ]
    db_rows_by_table = {table: _paper_table_count_for_run(db_path, table, resolved_run_id) for table in tables}
    executable_count = int(barrier.get("executable_count") or 0)
    order_count = int(barrier.get("order_count") or db_rows_by_table.get("paper_orders") or 0)
    skip_count = int(barrier.get("skip_count") or db_rows_by_table.get("paper_skip_ledger") or 0)
    missing_count = int(barrier.get("missing_count") or max(0, executable_count - order_count - skip_count))
    status = str(barrier.get("status") or ("not_found" if not report else ("missing_after_settlement" if missing_count else "settled")))
    return {
        "schema_version": "12.45",
        "source": "paper_consumption_status",
        "status": status,
        "run_id": resolved_run_id or None,
        "cycle_id": report.get("cycle_id"),
        "selected_lines": report.get("selected_lines") or [],
        "report_found": bool(report),
        "source_report_path": str(report_path) if report_path else None,
        "paper_db_path": str(db_path),
        "paper_db_exists": db_path.exists(),
        "executable_count": executable_count,
        "executables_by_line": barrier.get("executables_by_line") or {},
        "order_count": order_count,
        "skip_count": skip_count,
        "missing_count": missing_count,
        "missing_by_line": barrier.get("missing_by_line") or {},
        "missing_reason_codes": barrier.get("missing_reason_codes") or [],
        "missing_detail_by_line": barrier.get("missing_detail_by_line") or {},
        "paper_run_once_status": paper_run_once.get("status"),
        "paper_run_once_reason": paper_run_once.get("reason"),
        "paper_run_once_reason_codes": paper_run_once.get("reason_codes") or [],
        "paper_run_once_consume": paper_run_once.get("consume") or {},
        "paper_run_once_inline_retry": paper_run_once.get("inline_retry"),
        "paper_tick_lock": paper_run_once.get("tick_lock") or inspect_tick_lock(PROJECT_ROOT, cfg),
        "paper_daemon": paper_read_status(PROJECT_ROOT, cfg),
        "db_rows_by_table": db_rows_by_table,
        "rows": {
            "orders": _paper_rows_for_run(db_path, "paper_orders", resolved_run_id, limit=limit),
            "skips": _paper_rows_for_run(db_path, "paper_skip_ledger", resolved_run_id, limit=limit),
            "intents": _paper_rows_for_run(db_path, "paper_intent_inbox", resolved_run_id, limit=limit),
        },
    }


def feishu_config() -> dict[str, Any]:
    return load_feishu_config(PROJECT_ROOT, CONFIG_PATH).public_dict()


def feishu_send_trade_plans(*, mock_signals: bool = False, mock_send: bool = False) -> dict[str, Any]:
    return send_trade_plan_notifications(PROJECT_ROOT, mock_signals=mock_signals, mock_send=mock_send)


def notification_deliveries(
    *,
    event_type: str | None = None,
    status: str | None = None,
    line: str | None = None,
) -> list[dict[str, Any]]:
    rows = delivery_history(PROJECT_ROOT)
    if event_type:
        rows = [row for row in rows if row.get("event_type") == event_type]
    if status:
        rows = [row for row in rows if row.get("status") == status]
    if line:
        rows = [row for row in rows if row.get("strategy_line") == line]
    return rows


def run_audit_latest() -> dict[str, Any]:
    try:
        return _get_run_audit(PROJECT_ROOT)
    except FileNotFoundError as exc:
        raise ApiServiceError("file_missing", "run audit does not exist", {"path": "DATA/reports/latest_run_audit.json"}) from exc


def run_audit_by_id(run_id: str) -> dict[str, Any]:
    if not run_id or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789TtZz_-:" for ch in run_id):
        raise ApiServiceError("run_id_invalid", "run_id contains unsupported characters", {"run_id": run_id})
    try:
        return _get_run_audit(PROJECT_ROOT, run_id=run_id)
    except FileNotFoundError as exc:
        raise ApiServiceError("file_missing", "run audit does not exist", {"run_id": run_id}) from exc


def run_audit_list(*, limit: int = 20, status: str | None = None) -> dict[str, Any]:
    limit = max(1, min(int(limit or 20), 100))
    if status and status not in {"ok", "warning", "failed"}:
        raise ApiServiceError("status_invalid", "unsupported audit status filter", {"status": status})
    return _list_run_audits(PROJECT_ROOT, limit=limit, status=status)


def micro_quality_audit_latest() -> dict[str, Any]:
    latest_report_ref = read_optional_json_file(PROJECT_ROOT / "DATA" / "reports" / "latest_strategy_pipeline_report.json")
    latest_report = latest_report_ref.get("data") if isinstance(latest_report_ref, dict) else None
    run_id = latest_report.get("run_id") if isinstance(latest_report, dict) else None
    if run_id:
        payload = get_micro_quality_attribution(PROJECT_ROOT, run_id=str(run_id))
        if payload.get("source") == "missing_current_run":
            payload["cycle_id"] = latest_report.get("cycle_id")
            payload["pipeline_generated_at"] = latest_report.get("generated_at")
            payload["pipeline_finished_at"] = latest_report.get("finished_at")
        return payload
    return get_micro_quality_attribution(PROJECT_ROOT)


def micro_quality_audit_by_id(run_id: str) -> dict[str, Any]:
    if not run_id or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789TtZz_-:" for ch in run_id):
        raise ApiServiceError("run_id_invalid", "run_id contains unsupported characters", {"run_id": run_id})
    return get_micro_quality_attribution(PROJECT_ROOT, run_id=run_id)


def micro_evidence_runtime_latest() -> dict[str, Any]:
    payload = get_micro_evidence_runtime_v2(PROJECT_ROOT)
    if not payload.get("symbols"):
        try:
            ingest_micro_evidence_runtime_v2_to_sqlite(PROJECT_ROOT)
            payload = get_micro_evidence_runtime_v2(PROJECT_ROOT)
        except (OSError, ValueError, sqlite3.Error):
            pass
    return payload


def micro_evidence_runtime_by_id(run_id: str) -> dict[str, Any]:
    if not run_id or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789TtZz_-:" for ch in run_id):
        raise ApiServiceError("run_id_invalid", "run_id contains unsupported characters", {"run_id": run_id})
    return get_micro_evidence_runtime_v2(PROJECT_ROOT, run_id=run_id)


def micro_training_latest(*, symbol_limit: int = 100) -> dict[str, Any]:
    return latest_training_payload(PROJECT_ROOT, symbol_limit=max(1, min(int(symbol_limit or 100), 1000)))


def micro_training_runs(*, limit: int = 50) -> dict[str, Any]:
    return micro_training_run_list(PROJECT_ROOT, limit=max(1, min(int(limit or 50), 500)))


def micro_training_by_id(run_id: str, *, symbol_limit: int = 200) -> dict[str, Any]:
    if not run_id or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789TtZz_-:" for ch in run_id):
        raise ApiServiceError("run_id_invalid", "run_id contains unsupported characters", {"run_id": run_id})
    return micro_training_run_payload(run_id, root=PROJECT_ROOT, symbol_limit=max(1, min(int(symbol_limit or 200), 1000)))


def micro_training_symbol(symbol: str, *, limit: int = 100) -> dict[str, Any]:
    clean_symbol = str(symbol or "").upper().strip()
    if not clean_symbol or any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in clean_symbol):
        raise ApiServiceError("symbol_invalid", "symbol contains unsupported characters", {"symbol": symbol})
    return micro_training_symbol_payload(clean_symbol, root=PROJECT_ROOT, limit=max(1, min(int(limit or 100), 1000)))


def micro_training_coverage() -> dict[str, Any]:
    return micro_training_coverage_payload(PROJECT_ROOT)


def micro_evidence_runtime_findings(
    *,
    run_id: str | None = None,
    line: str | None = None,
    symbol: str | None = None,
    severity: str | None = None,
    reason: str | None = None,
    attributed_reason: str | None = None,
    commit_barrier_status: str | None = None,
    bucket_gap_class: str | None = None,
    ofi_gap_class: str | None = None,
    history_gap_class: str | None = None,
    queue_backpressure_state: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    if run_id and any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789TtZz_-:" for ch in run_id):
        raise ApiServiceError("run_id_invalid", "run_id contains unsupported characters", {"run_id": run_id})
    if symbol and any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-" for ch in symbol):
        raise ApiServiceError("symbol_invalid", "symbol contains unsupported characters", {"symbol": symbol})
    payload = get_micro_evidence_runtime_v2(PROJECT_ROOT, run_id=run_id)
    rows = payload.get("symbols") if isinstance(payload.get("symbols"), list) else []
    clean_line = str(line or "").strip()
    clean_symbol = str(symbol or "").upper().strip()
    clean_severity = str(severity or "").strip()
    clean_reason = str(reason or "").strip()
    clean_attr = str(attributed_reason or "").strip()
    clean_barrier = str(commit_barrier_status or "").strip()
    clean_bucket_gap = str(bucket_gap_class or "").strip()
    clean_ofi_gap = str(ofi_gap_class or "").strip()
    clean_history_gap = str(history_gap_class or "").strip()
    clean_backpressure = str(queue_backpressure_state or "").strip()
    filtered: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if clean_line and clean_line not in {str(row.get("line") or ""), str(row.get("strategy_line") or "")}:
            continue
        if clean_symbol and str(row.get("symbol") or "").upper() != clean_symbol:
            continue
        if clean_severity and str(row.get("severity") or "") != clean_severity:
            continue
        raw_reasons = [str(x) for x in row.get("raw_reasons") or []]
        attributed = [str(x) for x in row.get("attributed_reasons") or []]
        if clean_reason and clean_reason not in raw_reasons:
            continue
        if clean_attr and clean_attr not in attributed:
            continue
        runtime = row.get("runtime_evidence") if isinstance(row.get("runtime_evidence"), dict) else {}
        alignment = runtime.get("bucket_alignment") if isinstance(runtime.get("bucket_alignment"), dict) else {}
        if clean_barrier and str(alignment.get("commit_barrier_status") or alignment.get("alignment_status") or "") != clean_barrier:
            continue
        aggtrade_runtime = runtime.get("aggtrade_runtime") if isinstance(runtime.get("aggtrade_runtime"), dict) else {}
        book_depth_runtime = runtime.get("book_depth_runtime") if isinstance(runtime.get("book_depth_runtime"), dict) else {}
        z_history_runtime = runtime.get("z_history_runtime") if isinstance(runtime.get("z_history_runtime"), dict) else {}
        if clean_bucket_gap and str(aggtrade_runtime.get("bucket_gap_class") or "") != clean_bucket_gap:
            continue
        if clean_ofi_gap and str(book_depth_runtime.get("ofi_gap_class") or "") != clean_ofi_gap:
            continue
        if clean_history_gap and str(z_history_runtime.get("history_gap_class") or "") != clean_history_gap:
            continue
        if clean_backpressure and str(book_depth_runtime.get("queue_backpressure_state") or "") != clean_backpressure:
            continue
        filtered.append(row)
    limited = filtered[: max(1, min(int(limit or 500), 1000))]
    return {
        "source": payload.get("source"),
        "run_id": payload.get("run_id"),
        "cycle_id": payload.get("cycle_id"),
        "generated_at": payload.get("generated_at"),
        "summary": payload.get("summary") or {},
        "filters": {
            "line": clean_line or None,
            "symbol": clean_symbol or None,
            "severity": clean_severity or None,
            "reason": clean_reason or None,
            "attributed_reason": clean_attr or None,
            "commit_barrier_status": clean_barrier or None,
            "bucket_gap_class": clean_bucket_gap or None,
            "ofi_gap_class": clean_ofi_gap or None,
            "history_gap_class": clean_history_gap or None,
            "queue_backpressure_state": clean_backpressure or None,
            "limit": max(1, min(int(limit or 500), 1000)),
        },
        "count": len(limited),
        "total_matched": len(filtered),
        "symbols": limited,
    }


def micro_evidence_runtime_reason(reason: str, *, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    clean = str(reason or "").strip()
    if not clean or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in clean):
        raise ApiServiceError("reason_invalid", "reason contains unsupported characters", {"reason": reason})
    return micro_evidence_runtime_findings(run_id=run_id, reason=clean, limit=limit)


def micro_evidence_target_source() -> dict[str, Any]:
    targets = _read_optional_raw_json(PROJECT_ROOT / "DATA" / "micro" / "micro_targets.json") or {}
    runtime = micro_evidence_runtime_latest()
    target_rows: list[dict[str, Any]] = []
    for tier_name in ("tier1_warm_watch", "tier2_active_strong"):
        for item in targets.get(tier_name) or []:
            if not isinstance(item, dict):
                continue
            target_rows.append(
                {
                    "tier": tier_name,
                    "symbol": str(item.get("symbol") or "").upper(),
                    "source_state": item.get("source_state"),
                    "retained_reason": item.get("retained_reason"),
                    "sticky_source": item.get("sticky_source"),
                    "sticky_age_sec": item.get("sticky_age_sec"),
                    "sticky_cycle_count": item.get("sticky_cycle_count"),
                    "sticky_plan_candidate": item.get("sticky_plan_candidate"),
                }
            )
    runtime_rows = runtime.get("symbols") if isinstance(runtime.get("symbols"), list) else []
    raw_fill = targets.get("raw_fill") if isinstance(targets.get("raw_fill"), dict) else {}
    sticky_pool = targets.get("sticky_pool") if isinstance(targets.get("sticky_pool"), dict) else {}
    distribution = targets.get("target_source_distribution")
    if not isinstance(distribution, dict):
        distribution = {}
        for row in target_rows:
            key = str(row.get("retained_reason") or row.get("source_state") or "unknown")
            if row.get("source_state") == "raw_candidate":
                key = "raw_fill"
            distribution[key] = int(distribution.get(key, 0)) + 1
    return {
        "source": "micro_target_source_contract",
        "generated_at": targets.get("generated_at"),
        "target_set_id": targets.get("target_set_id"),
        "candidate_hash": targets.get("candidate_hash"),
        "status": targets.get("status"),
        "target_count": targets.get("target_count"),
        "plan_candidate_count": targets.get("plan_candidate_count"),
        "target_source_distribution": distribution,
        "raw_fill": raw_fill,
        "sticky_pool": sticky_pool,
        "targets": target_rows,
        "runtime": {
            "run_id": runtime.get("run_id"),
            "cycle_id": runtime.get("cycle_id"),
            "summary": runtime.get("summary") or {},
            "symbol_count": len(runtime_rows),
        },
    }


def _micro_full_z_payload(runtime: dict[str, Any]) -> dict[str, Any]:
    symbols = runtime.get("symbols") if isinstance(runtime.get("symbols"), list) else []
    ledger = latest_target_source_ledger(PROJECT_ROOT / "DATA" / "audit" / "run_audit.db", limit=1000)
    ledger_by_symbol: dict[str, dict[str, Any]] = {}
    for row in ledger.get("targets") or []:
        if isinstance(row, dict) and row.get("symbol") and row.get("symbol") not in ledger_by_symbol:
            ledger_by_symbol[str(row.get("symbol")).upper()] = row
    rows: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    for row in symbols:
        if not isinstance(row, dict) or row.get("strategy_line") != "micro_full":
            continue
        evidence = row.get("runtime_evidence") if isinstance(row.get("runtime_evidence"), dict) else {}
        z_runtime = evidence.get("z_history_runtime") if isinstance(evidence.get("z_history_runtime"), dict) else {}
        store = z_runtime.get("store_window") if isinstance(z_runtime.get("store_window"), dict) else {}
        status = str(store.get("full_z_status") or "missing")
        reason = str(store.get("full_z_missing_reason") or "none")
        status_counts[status] = int(status_counts.get(status, 0)) + 1
        reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1
        symbol = str(row.get("symbol") or "").upper()
        rows.append(
            {
                "run_id": row.get("run_id"),
                "cycle_id": row.get("cycle_id"),
                "strategy_line": row.get("strategy_line"),
                "symbol": symbol,
                "state": row.get("state"),
                "primary_reason": row.get("primary_reason"),
                "attributed_reason": row.get("attributed_reason"),
                "full_z_status": status,
                "full_z_missing_reason": None if reason == "none" else reason,
                "history_gap_class": z_runtime.get("history_gap_class"),
                "store_window": store,
                "target_source": ledger_by_symbol.get(symbol) or {},
            }
        )
    return {
        "source": "micro_full_z_contract",
        "run_id": runtime.get("run_id"),
        "cycle_id": runtime.get("cycle_id"),
        "summary": {
            "micro_full_rows": len(rows),
            "status_counts": status_counts,
            "missing_reason_counts": reason_counts,
        },
        "symbols": rows,
    }


def micro_full_z_latest() -> dict[str, Any]:
    return _micro_full_z_payload(micro_evidence_runtime_latest())


def micro_full_z_by_id(run_id: str) -> dict[str, Any]:
    return _micro_full_z_payload(micro_evidence_runtime_by_id(run_id))


def _micro_fast_runtime_stability_payload(runtime: dict[str, Any]) -> dict[str, Any]:
    symbols = runtime.get("symbols") if isinstance(runtime.get("symbols"), list) else []
    rows: list[dict[str, Any]] = []
    barrier_counts: Counter[str] = Counter()
    aligned_gate_counts: Counter[str] = Counter()
    z_continuity_counts: Counter[str] = Counter()
    depth5_role_counts: Counter[str] = Counter()
    cvd_trace_counts: Counter[str] = Counter()
    fast_z_nan_counts: Counter[str] = Counter()
    candidate_dwell_counts: Counter[str] = Counter()
    coverage_counts: dict[str, Counter[str]] = {
        "aggTrade": Counter(),
        "bookTicker": Counter(),
        "partialDepth5": Counter(),
    }
    reason_counts: Counter[str] = Counter()
    for row in symbols:
        if not isinstance(row, dict) or row.get("strategy_line") != "micro_fast":
            continue
        evidence = row.get("runtime_evidence") if isinstance(row.get("runtime_evidence"), dict) else {}
        barrier = evidence.get("bucket_commit_barrier") if isinstance(evidence.get("bucket_commit_barrier"), dict) else {}
        coverage = evidence.get("coverage_root_cause_v2") if isinstance(evidence.get("coverage_root_cause_v2"), dict) else {}
        continuity = evidence.get("fast_z_continuity") if isinstance(evidence.get("fast_z_continuity"), dict) else {}
        gate = evidence.get("aligned_frame_gate") if isinstance(evidence.get("aligned_frame_gate"), dict) else {}
        dwell = evidence.get("candidate_dwell") if isinstance(evidence.get("candidate_dwell"), dict) else {}
        cvd_trace = evidence.get("cvd_commit_missing_trace") if isinstance(evidence.get("cvd_commit_missing_trace"), dict) else {}
        z_nan_trace = evidence.get("fast_z_nan_trace") if isinstance(evidence.get("fast_z_nan_trace"), dict) else {}
        reader_short_trace = (
            evidence.get("fast_z_reader_window_short_trace") if isinstance(evidence.get("fast_z_reader_window_short_trace"), dict) else {}
        )
        invalid_value_trace = evidence.get("fast_z_invalid_value_trace") if isinstance(evidence.get("fast_z_invalid_value_trace"), dict) else {}
        cvd_tail_trace = evidence.get("cvd_commit_tail_trace") if isinstance(evidence.get("cvd_commit_tail_trace"), dict) else {}
        judgeable_throughput = (
            evidence.get("judgeable_throughput_trace") if isinstance(evidence.get("judgeable_throughput_trace"), dict) else {}
        )
        target_cadence = evidence.get("target_cadence_trace") if isinstance(evidence.get("target_cadence_trace"), dict) else {}
        observe_pool = evidence.get("observe_pool_trace") if isinstance(evidence.get("observe_pool_trace"), dict) else {}
        coverage_split = (
            evidence.get("coverage_market_technical_split")
            if isinstance(evidence.get("coverage_market_technical_split"), dict)
            else {}
        )
        valid_bucket_trace = (
            evidence.get("valid_bucket_ratio_low_trace")
            if isinstance(evidence.get("valid_bucket_ratio_low_trace"), dict)
            else {}
        )
        barrier_status = str(barrier.get("barrier_status") or "unknown")
        gate_status = "pass" if gate.get("aligned_frame_pass") is True else str(gate.get("block_reason") or "blocked")
        z_status = str(continuity.get("continuity_status") or "unknown")
        barrier_counts[barrier_status] += 1
        aligned_gate_counts[gate_status] += 1
        z_continuity_counts[z_status] += 1
        candidate_dwell_counts[str(dwell.get("dwell_state") or "unknown")] += 1
        cvd_trace_counts[str(cvd_trace.get("root_cause") or "none")] += 1
        fast_z_nan_counts[str(z_nan_trace.get("reason") or "unknown")] += 1
        for stream in coverage_counts:
            entry = coverage.get(stream) if isinstance(coverage.get(stream), dict) else {}
            coverage_counts[stream][str(entry.get("coverage_class") or "unknown")] += 1
            if stream == "partialDepth5":
                depth5_role_counts[str(entry.get("role") or "unknown")] += 1
        for reason in row.get("raw_reasons") or []:
            reason_counts[str(reason)] += 1
        rows.append(
            {
                "run_id": row.get("run_id"),
                "cycle_id": row.get("cycle_id"),
                "strategy_line": "micro_fast",
                "symbol": row.get("symbol"),
                "state": row.get("state"),
                "status": row.get("status"),
                "severity": row.get("severity"),
                "raw_reasons": row.get("raw_reasons") or [],
                "attributed_reasons": row.get("attributed_reasons") or [],
                "bucket_commit_barrier": barrier,
                "coverage_root_cause_v2": coverage,
                "fast_z_continuity": continuity,
                "aligned_frame_gate": gate,
                "candidate_dwell": dwell,
                "cvd_commit_missing_trace": cvd_trace,
                "fast_z_nan_trace": z_nan_trace,
                "judgeable_scope": evidence.get("judgeable_scope") if isinstance(evidence.get("judgeable_scope"), dict) else {},
                "judgeable_throughput_trace": judgeable_throughput,
                "target_cadence_trace": target_cadence,
                "observe_pool_trace": observe_pool,
                "coverage_market_technical_split": coverage_split,
                "valid_bucket_ratio_low_trace": valid_bucket_trace,
                "fast_z_append_read_trace": evidence.get("fast_z_append_read_trace") if isinstance(evidence.get("fast_z_append_read_trace"), dict) else {},
                "fast_z_reader_window_short_trace": reader_short_trace,
                "fast_z_invalid_value_trace": invalid_value_trace,
                "cvd_ofi_bucket_freshness_trace": evidence.get("cvd_ofi_bucket_freshness_trace") if isinstance(evidence.get("cvd_ofi_bucket_freshness_trace"), dict) else {},
                "cvd_commit_tail_trace": cvd_tail_trace,
            }
        )
    return {
        "source": "micro_fast_runtime_stability_contract",
        "run_id": runtime.get("run_id"),
        "cycle_id": runtime.get("cycle_id"),
        "generated_at": runtime.get("generated_at"),
        "summary": {
            "micro_fast_rows": len(rows),
            "bucket_commit_barrier_counts": dict(barrier_counts),
            "aligned_frame_gate_counts": dict(aligned_gate_counts),
            "fast_z_continuity_counts": dict(z_continuity_counts),
            "depth5_role_counts": dict(depth5_role_counts),
            "candidate_dwell_counts": dict(candidate_dwell_counts),
            "cvd_commit_trace_counts": dict(cvd_trace_counts),
            "fast_z_nan_trace_counts": dict(fast_z_nan_counts),
            "coverage_root_cause_v2_counts": {key: dict(value) for key, value in coverage_counts.items()},
            "raw_reason_counts": dict(reason_counts),
        },
        "symbols": rows,
    }


def micro_fast_runtime_stability_latest() -> dict[str, Any]:
    return _micro_fast_runtime_stability_payload(micro_evidence_runtime_latest())


def micro_fast_runtime_stability_by_id(run_id: str) -> dict[str, Any]:
    return _micro_fast_runtime_stability_payload(micro_evidence_runtime_by_id(run_id))


def micro_fast_runtime_stability_reason(reason: str, *, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    clean = str(reason or "").strip()
    if not clean or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in clean):
        raise ApiServiceError("reason_invalid", "reason contains unsupported characters", {"reason": reason})
    payload = micro_fast_runtime_stability_by_id(run_id) if run_id else micro_fast_runtime_stability_latest()
    rows = [row for row in payload.get("symbols") or [] if clean in {str(x) for x in row.get("raw_reasons") or []}]
    limited = rows[: max(1, min(int(limit or 500), 1000))]
    return {
        **{key: payload.get(key) for key in ("source", "run_id", "cycle_id", "generated_at")},
        "reason": clean,
        "count": len(limited),
        "total_matched": len(rows),
        "symbols": limited,
    }


def micro_fast_tail_cleanup_latest() -> dict[str, Any]:
    payload = micro_fast_runtime_stability_latest()
    payload["source"] = "micro_fast_tail_cleanup_contract"
    return payload


def micro_fast_tail_cleanup_by_id(run_id: str) -> dict[str, Any]:
    payload = micro_fast_runtime_stability_by_id(run_id)
    payload["source"] = "micro_fast_tail_cleanup_contract"
    return payload


def micro_fast_tail_cleanup_reason(reason: str, *, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    clean = str(reason or "").strip()
    if not clean or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in clean):
        raise ApiServiceError("reason_invalid", "reason contains unsupported characters", {"reason": reason})
    payload = micro_fast_tail_cleanup_by_id(run_id) if run_id else micro_fast_tail_cleanup_latest()
    rows = []
    for row in payload.get("symbols") or []:
        tokens = {str(x) for x in row.get("raw_reasons") or []}
        tokens.add(str(row.get("candidate_dwell", {}).get("dwell_state") or ""))
        tokens.add(str(row.get("candidate_dwell", {}).get("block_reason") or ""))
        tokens.add(str(row.get("cvd_commit_missing_trace", {}).get("root_cause") or ""))
        tokens.add(str(row.get("fast_z_nan_trace", {}).get("reason") or ""))
        depth = row.get("coverage_root_cause_v2", {}).get("partialDepth5", {})
        if isinstance(depth, dict):
            tokens.add(str(depth.get("role") or ""))
            tokens.add(str(depth.get("coverage_class") or ""))
        if clean in tokens:
            rows.append(row)
    limited = rows[: max(1, min(int(limit or 500), 1000))]
    return {
        **{key: payload.get(key) for key in ("source", "run_id", "cycle_id", "generated_at")},
        "reason": clean,
        "count": len(limited),
        "total_matched": len(rows),
        "symbols": limited,
    }


def _micro_fast_judgeable_payload(runtime: dict[str, Any]) -> dict[str, Any]:
    base = _micro_fast_runtime_stability_payload(runtime)
    rows: list[dict[str, Any]] = []
    scope_counts: Counter[str] = Counter()
    append_read_counts: Counter[str] = Counter()
    nan_reason_counts: Counter[str] = Counter()
    freshness_counts: Counter[str] = Counter()
    stale_root_counts: Counter[str] = Counter()
    for row in base.get("symbols") or []:
        evidence = {}
        # Rows produced by _micro_fast_runtime_stability_payload already flatten the
        # primary runtime evidence blocks. Keep this adapter intentionally shallow
        # so older payloads remain readable.
        judgeable = row.get("judgeable_scope") if isinstance(row.get("judgeable_scope"), dict) else {}
        append_read = row.get("fast_z_append_read_trace") if isinstance(row.get("fast_z_append_read_trace"), dict) else {}
        freshness = row.get("cvd_ofi_bucket_freshness_trace") if isinstance(row.get("cvd_ofi_bucket_freshness_trace"), dict) else {}
        if not judgeable or not append_read or not freshness:
            # Fallback for rows that still carry the blocks only in nested runtime evidence.
            evidence = row.get("runtime_evidence") if isinstance(row.get("runtime_evidence"), dict) else {}
            judgeable = judgeable or (evidence.get("judgeable_scope") if isinstance(evidence.get("judgeable_scope"), dict) else {})
            append_read = append_read or (evidence.get("fast_z_append_read_trace") if isinstance(evidence.get("fast_z_append_read_trace"), dict) else {})
            freshness = freshness or (evidence.get("cvd_ofi_bucket_freshness_trace") if isinstance(evidence.get("cvd_ofi_bucket_freshness_trace"), dict) else {})
        nan_trace = row.get("fast_z_nan_trace") if isinstance(row.get("fast_z_nan_trace"), dict) else {}
        scope_counts[str(judgeable.get("scope") or "unknown")] += 1
        append_read_counts[str(append_read.get("trace_status") or "unknown")] += 1
        nan_reason_counts[str(nan_trace.get("reason") or "unknown")] += 1
        freshness_counts[str(freshness.get("freshness_status") or "unknown")] += 1
        stale_root_counts[str(freshness.get("stale_root_cause") or "none")] += 1
        rows.append(
            {
                **row,
                "judgeable_scope": judgeable,
                "fast_z_append_read_trace": append_read,
                "cvd_ofi_bucket_freshness_trace": freshness,
            }
        )
    return {
        "source": "micro_fast_judgeable_runtime_contract",
        "run_id": base.get("run_id"),
        "cycle_id": base.get("cycle_id"),
        "generated_at": base.get("generated_at"),
        "summary": {
            **(base.get("summary") or {}),
            "scope_counts": dict(scope_counts),
            "fast_z_trace_counts": dict(append_read_counts),
            "nan_reason_counts": dict(nan_reason_counts),
            "freshness_status_counts": dict(freshness_counts),
            "stale_root_cause_counts": dict(stale_root_counts),
        },
        "symbols": rows,
    }


def _micro_fast_judgeable_only_payload(runtime: dict[str, Any]) -> dict[str, Any]:
    base = _micro_fast_judgeable_payload(runtime)
    rows = [row for row in base.get("symbols") or [] if isinstance(row, dict)]
    not_judgeable_rows = []
    judgeable_rows = []
    technical_countable_rows = []
    reader_short_counts: Counter[str] = Counter()
    invalid_value_counts: Counter[str] = Counter()
    cvd_tail_counts: Counter[str] = Counter()
    stale_root_counts: Counter[str] = Counter()

    for row in rows:
        judgeable = row.get("judgeable_scope") if isinstance(row.get("judgeable_scope"), dict) else {}
        scope = str(judgeable.get("scope") or "unknown")
        if scope == "not_judgeable_yet":
            not_judgeable_rows.append(row)
        else:
            judgeable_rows.append(row)
        if judgeable.get("technical_failure_countable") is True or scope in {"judgeable_but_z_missing", "judgeable_and_technical_failed"}:
            technical_countable_rows.append(row)

        reader = row.get("fast_z_reader_window_short_trace") if isinstance(row.get("fast_z_reader_window_short_trace"), dict) else {}
        invalid = row.get("fast_z_invalid_value_trace") if isinstance(row.get("fast_z_invalid_value_trace"), dict) else {}
        cvd_tail = row.get("cvd_commit_tail_trace") if isinstance(row.get("cvd_commit_tail_trace"), dict) else {}
        fresh = row.get("cvd_ofi_bucket_freshness_trace") if isinstance(row.get("cvd_ofi_bucket_freshness_trace"), dict) else {}
        if reader.get("root_cause"):
            reader_short_counts[str(reader.get("root_cause"))] += 1
        if invalid.get("root_cause"):
            invalid_value_counts[str(invalid.get("root_cause"))] += 1
        if cvd_tail.get("root_cause"):
            cvd_tail_counts[str(cvd_tail.get("root_cause"))] += 1
        stale_root_counts[str(fresh.get("stale_root_cause") or "none")] += 1

    return {
        "source": "micro_fast_judgeable_only_metrics",
        "run_id": base.get("run_id"),
        "cycle_id": base.get("cycle_id"),
        "generated_at": base.get("generated_at"),
        "summary": {
            **(base.get("summary") or {}),
            "all_rows": len(rows),
            "not_judgeable_rows": len(not_judgeable_rows),
            "judgeable_rows": len(judgeable_rows),
            "technical_countable_rows": len(technical_countable_rows),
            "reader_window_short_root_cause_counts": dict(reader_short_counts),
            "invalid_value_root_cause_counts": dict(invalid_value_counts),
            "cvd_commit_tail_root_cause_counts": dict(cvd_tail_counts),
            "stale_root_cause_counts": dict(stale_root_counts),
        },
        "symbols": rows,
        "judgeable_symbols": judgeable_rows,
        "technical_countable_symbols": technical_countable_rows,
    }


def micro_fast_judgeable_latest() -> dict[str, Any]:
    return _micro_fast_judgeable_payload(micro_evidence_runtime_latest())


def micro_fast_judgeable_by_id(run_id: str) -> dict[str, Any]:
    return _micro_fast_judgeable_payload(micro_evidence_runtime_by_id(run_id))


def micro_fast_judgeable_reason(reason: str, *, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    clean = str(reason or "").strip()
    if not clean or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in clean):
        raise ApiServiceError("reason_invalid", "reason contains unsupported characters", {"reason": reason})
    payload = micro_fast_judgeable_by_id(run_id) if run_id else micro_fast_judgeable_latest()
    rows = []
    for row in payload.get("symbols") or []:
        tokens = {str(x) for x in row.get("raw_reasons") or []}
        tokens.add(str(row.get("judgeable_scope", {}).get("scope") or ""))
        tokens.add(str(row.get("judgeable_scope", {}).get("reason") or ""))
        tokens.add(str(row.get("fast_z_append_read_trace", {}).get("trace_status") or ""))
        tokens.add(str(row.get("fast_z_append_read_trace", {}).get("append_skip_reason") or ""))
        tokens.add(str(row.get("fast_z_nan_trace", {}).get("reason") or ""))
        tokens.add(str(row.get("cvd_ofi_bucket_freshness_trace", {}).get("freshness_status") or ""))
        tokens.add(str(row.get("cvd_ofi_bucket_freshness_trace", {}).get("stale_root_cause") or ""))
        if clean in tokens:
            rows.append(row)
    limited = rows[: max(1, min(int(limit or 500), 1000))]
    return {
        **{key: payload.get(key) for key in ("source", "run_id", "cycle_id", "generated_at")},
        "reason": clean,
        "count": len(limited),
        "total_matched": len(rows),
        "symbols": limited,
    }


def micro_fast_judgeable_symbol(symbol: str, *, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    clean_symbol = str(symbol or "").upper()
    if not clean_symbol or any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in clean_symbol):
        raise ApiServiceError("symbol_invalid", "symbol contains unsupported characters", {"symbol": symbol})
    payload = micro_fast_judgeable_by_id(run_id) if run_id else micro_fast_judgeable_latest()
    rows = [row for row in payload.get("symbols") or [] if str(row.get("symbol") or "").upper() == clean_symbol]
    limited = rows[: max(1, min(int(limit or 500), 1000))]
    return {
        **{key: payload.get(key) for key in ("source", "run_id", "cycle_id", "generated_at")},
        "symbol": clean_symbol,
        "count": len(limited),
        "total_matched": len(rows),
        "symbols": limited,
    }


def micro_fast_judgeable_only_latest() -> dict[str, Any]:
    return _micro_fast_judgeable_only_payload(micro_evidence_runtime_latest())


def micro_fast_judgeable_only_by_id(run_id: str) -> dict[str, Any]:
    return _micro_fast_judgeable_only_payload(micro_evidence_runtime_by_id(run_id))


def micro_fast_judgeable_only_reason(reason: str, *, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    clean = str(reason or "").strip()
    if not clean or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in clean):
        raise ApiServiceError("reason_invalid", "reason contains unsupported characters", {"reason": reason})
    payload = micro_fast_judgeable_only_by_id(run_id) if run_id else micro_fast_judgeable_only_latest()
    rows = []
    for row in payload.get("symbols") or []:
        tokens = {str(x) for x in row.get("raw_reasons") or []}
        for block_name in (
            "judgeable_scope",
            "fast_z_append_read_trace",
            "fast_z_reader_window_short_trace",
            "fast_z_invalid_value_trace",
            "cvd_ofi_bucket_freshness_trace",
            "cvd_commit_tail_trace",
        ):
            block = row.get(block_name) if isinstance(row.get(block_name), dict) else {}
            for value in block.values():
                if isinstance(value, (str, int, float)) and value is not None:
                    tokens.add(str(value))
        if clean in tokens:
            rows.append(row)
    limited = rows[: max(1, min(int(limit or 500), 1000))]
    return {
        **{key: payload.get(key) for key in ("source", "run_id", "cycle_id", "generated_at")},
        "reason": clean,
        "count": len(limited),
        "total_matched": len(rows),
        "symbols": limited,
    }


def micro_fast_judgeable_only_symbol(symbol: str, *, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    clean_symbol = str(symbol or "").upper()
    if not clean_symbol or any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in clean_symbol):
        raise ApiServiceError("symbol_invalid", "symbol contains unsupported characters", {"symbol": symbol})
    payload = micro_fast_judgeable_only_by_id(run_id) if run_id else micro_fast_judgeable_only_latest()
    rows = [row for row in payload.get("symbols") or [] if str(row.get("symbol") or "").upper() == clean_symbol]
    limited = rows[: max(1, min(int(limit or 500), 1000))]
    return {
        **{key: payload.get(key) for key in ("source", "run_id", "cycle_id", "generated_at")},
        "symbol": clean_symbol,
        "count": len(limited),
        "total_matched": len(rows),
        "symbols": limited,
    }


def _micro_fast_judgeable_throughput_payload(runtime: dict[str, Any]) -> dict[str, Any]:
    base = _micro_fast_runtime_stability_payload(runtime)
    rows = [row for row in base.get("symbols") or [] if isinstance(row, dict)]
    reason_counts: Counter[str] = Counter()
    target_source_counts: Counter[str] = Counter()
    dwell_counts: Counter[str] = Counter()
    pool_counts: Counter[str] = Counter()
    ages: list[float] = []
    dwell_values: list[float] = []
    for row in rows:
        trace = row.get("judgeable_throughput_trace") if isinstance(row.get("judgeable_throughput_trace"), dict) else {}
        cadence = row.get("target_cadence_trace") if isinstance(row.get("target_cadence_trace"), dict) else {}
        pool = row.get("observe_pool_trace") if isinstance(row.get("observe_pool_trace"), dict) else {}
        reason_counts[str(trace.get("not_judgeable_reason") or "unknown")] += 1
        target_source_counts[str(trace.get("target_source") or "unknown")] += 1
        dwell_counts[str(cadence.get("judgeable_transition") is True and "judgeable_transition" or trace.get("not_judgeable_reason") or "unknown")] += 1
        pool_counts[str(pool.get("pool_state") or "unknown")] += 1
        try:
            ages.append(float(trace.get("target_age_sec")))
        except (TypeError, ValueError):
            pass
        try:
            dwell_values.append(float(trace.get("dwell_sec")))
        except (TypeError, ValueError):
            pass

    judgeable_count = sum(count for reason, count in reason_counts.items() if reason == "judgeable")
    all_rows = len(rows)
    return {
        "source": "micro_fast_judgeable_throughput_contract",
        "run_id": base.get("run_id"),
        "cycle_id": base.get("cycle_id"),
        "generated_at": base.get("generated_at"),
        "summary": {
            **(base.get("summary") or {}),
            "runtime_rows": all_rows,
            "judgeable_count": judgeable_count,
            "not_judgeable_count": max(0, all_rows - judgeable_count),
            "judgeable_yield": (judgeable_count / all_rows) if all_rows else 0.0,
            "not_judgeable_reason_counts": dict(reason_counts),
            "target_source_counts": dict(target_source_counts),
            "candidate_dwell_counts": dict(dwell_counts),
            "observe_pool_counts": dict(pool_counts),
            "avg_target_age_sec": (sum(ages) / len(ages)) if ages else None,
            "avg_dwell_sec": (sum(dwell_values) / len(dwell_values)) if dwell_values else None,
        },
        "symbols": rows,
    }


def micro_fast_judgeable_throughput_latest() -> dict[str, Any]:
    return _micro_fast_judgeable_throughput_payload(micro_evidence_runtime_latest())


def micro_fast_judgeable_throughput_by_id(run_id: str) -> dict[str, Any]:
    return _micro_fast_judgeable_throughput_payload(micro_evidence_runtime_by_id(run_id))


def _micro_fast_coverage_split_payload(runtime: dict[str, Any]) -> dict[str, Any]:
    base = _micro_fast_runtime_stability_payload(runtime)
    rows = [row for row in base.get("symbols") or [] if isinstance(row, dict)]
    group_counts: dict[str, Counter[str]] = {
        "aggTrade": Counter(),
        "bookTicker": Counter(),
        "partialDepth5": Counter(),
    }
    root_counts: dict[str, Counter[str]] = {
        "aggTrade": Counter(),
        "bookTicker": Counter(),
        "partialDepth5": Counter(),
    }
    for row in rows:
        split = row.get("coverage_market_technical_split") if isinstance(row.get("coverage_market_technical_split"), dict) else {}
        for stream in group_counts:
            entry = split.get(stream) if isinstance(split.get(stream), dict) else {}
            group_counts[stream][str(entry.get("group") or "unknown")] += 1
            root_counts[stream][str(entry.get("root_cause") or "unknown")] += 1
    return {
        "source": "micro_fast_coverage_split_contract",
        "run_id": base.get("run_id"),
        "cycle_id": base.get("cycle_id"),
        "generated_at": base.get("generated_at"),
        "summary": {
            "runtime_rows": len(rows),
            "coverage_market_counts": {key: {"market": value.get("market", 0)} for key, value in group_counts.items()},
            "coverage_technical_counts": {key: {"technical": value.get("technical", 0)} for key, value in group_counts.items()},
            "coverage_group_counts": {key: dict(value) for key, value in group_counts.items()},
            "coverage_root_counts": {key: dict(value) for key, value in root_counts.items()},
        },
        "symbols": rows,
    }


def micro_fast_coverage_split_latest() -> dict[str, Any]:
    return _micro_fast_coverage_split_payload(micro_evidence_runtime_latest())


def micro_fast_coverage_split_by_id(run_id: str) -> dict[str, Any]:
    return _micro_fast_coverage_split_payload(micro_evidence_runtime_by_id(run_id))


def _micro_fast_valid_bucket_payload(runtime: dict[str, Any]) -> dict[str, Any]:
    base = _micro_fast_runtime_stability_payload(runtime)
    rows = [row for row in base.get("symbols") or [] if isinstance(row, dict)]
    root_counts: Counter[str] = Counter()
    ratio_values: list[float] = []
    for row in rows:
        trace = row.get("valid_bucket_ratio_low_trace") if isinstance(row.get("valid_bucket_ratio_low_trace"), dict) else {}
        root_counts[str(trace.get("root_cause") or "unknown")] += 1
        try:
            ratio_values.append(float(trace.get("valid_bucket_ratio")))
        except (TypeError, ValueError):
            pass
    return {
        "source": "micro_fast_valid_bucket_contract",
        "run_id": base.get("run_id"),
        "cycle_id": base.get("cycle_id"),
        "generated_at": base.get("generated_at"),
        "summary": {
            "runtime_rows": len(rows),
            "valid_bucket_root_counts": dict(root_counts),
            "avg_valid_bucket_ratio": (sum(ratio_values) / len(ratio_values)) if ratio_values else None,
            "low_valid_bucket_rows": sum(1 for row in rows if (row.get("valid_bucket_ratio_low_trace") or {}).get("root_cause") not in {None, "", "ok"}),
        },
        "symbols": rows,
    }


def micro_fast_valid_bucket_latest() -> dict[str, Any]:
    return _micro_fast_valid_bucket_payload(micro_evidence_runtime_latest())


def micro_fast_valid_bucket_by_id(run_id: str) -> dict[str, Any]:
    return _micro_fast_valid_bucket_payload(micro_evidence_runtime_by_id(run_id))


def micro_evidence_runtime_symbol(symbol: str, *, limit: int = 100) -> dict[str, Any]:
    clean_symbol = str(symbol or "").upper()
    if not clean_symbol or any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in clean_symbol):
        raise ApiServiceError("symbol_invalid", "symbol contains unsupported characters", {"symbol": symbol})
    return get_micro_evidence_runtime_v2(PROJECT_ROOT, symbol=clean_symbol, limit=max(1, min(int(limit or 100), 500)))
