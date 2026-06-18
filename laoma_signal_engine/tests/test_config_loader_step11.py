from pathlib import Path

from laoma_signal_engine.core.config_loader import EngineConfig


def test_step11_strategy_pipeline_defaults_are_loaded(tmp_path: Path) -> None:
    cfg = EngineConfig.load(tmp_path)

    assert cfg.strategy_pipeline_mode == "once"
    assert cfg.strategy_pipeline_interval_sec == 60
    assert cfg.strategy_pipeline_overlap_policy == "skip"
    assert cfg.strategy_pipeline_run_lines == (
        "without_micro",
        "micro_fast",
    )
    assert cfg.strategy_pipeline_fetch_mode == "async"
    assert cfg.strategy_pipeline_max_concurrency == 6
    assert cfg.strategy_pipeline_auto_refresh_before_trade_plan is True
    assert cfg.strategy_pipeline_require_refresh_anchor_current_factor is True
    assert cfg.strategy_pipeline_aggregate_final_decisions is True
    assert cfg.strategy_pipeline_run_abc_audit is True
    assert cfg.strategy_pipeline_run_json_stage_audit is True
    assert cfg.strategy_pipeline_allow_llm_stale is True
    assert cfg.strategy_pipeline_lock_path == tmp_path / "DATA/runtime/strategy_pipeline.lock"
    assert cfg.strategy_pipeline_report_path == (
        tmp_path / "DATA/reports/latest_strategy_pipeline_report.json"
    )


def test_step11_micro_daemon_cli_defaults_are_loaded(tmp_path: Path) -> None:
    cfg = EngineConfig.load(tmp_path)

    assert cfg.strategy_pipeline_require_daemon is True
    assert cfg.strategy_pipeline_wait_fast_sec == 300
    assert cfg.strategy_pipeline_wait_full_sec == 1200
    assert cfg.strategy_pipeline_max_wait_sec == 1200
    assert cfg.strategy_pipeline_min_full_ready_count == 1
    assert cfg.strategy_pipeline_full_wait_policy == "strict_until_ready"
    assert cfg.strategy_pipeline_max_active_symbols == 20
    assert cfg.strategy_pipeline_timeout_policy == "return_reason"
    assert cfg.strategy_pipeline_micro_health_grace_recheck_enabled is True
    assert cfg.strategy_pipeline_micro_health_grace_wait_sec == 15
    assert cfg.strategy_pipeline_micro_health_grace_max_attempts == 2
    assert cfg.strategy_pipeline_micro_health_grace_accept_fresh_heartbeat_sec == 60
    assert cfg.strategy_pipeline_micro_inline_recovery_enabled is True
    assert cfg.strategy_pipeline_micro_inline_recovery_max_attempts == 1
    assert cfg.strategy_pipeline_micro_inline_recovery_startup_wait_sec == 45
    assert cfg.strategy_pipeline_micro_inline_recovery_heartbeat_fresh_sec == 20
    assert cfg.strategy_pipeline_micro_inline_recovery_state_fresh_sec == 30
    assert cfg.strategy_pipeline_micro_inline_recovery_features_fresh_sec == 30
    assert cfg.micro_daemon_cli_transport == "real"
    assert cfg.micro_daemon_cli_start_mode == "background"
    assert cfg.micro_daemon_cli_pid_path == tmp_path / "DATA/runtime/micro_daemon.pid"
    assert cfg.micro_daemon_cli_log_path == tmp_path / "DATA/logs/micro_daemon.log"
    assert cfg.micro_daemon_cli_heartbeat_path == (
        tmp_path / "DATA/micro/micro_collector_heartbeat.json"
    )
    assert cfg.micro_daemon_cli_state_path == tmp_path / "DATA/micro/latest_micro_state.json"
    assert cfg.micro_daemon_cli_features_path == (
        tmp_path / "DATA/micro/latest_micro_features.json"
    )
