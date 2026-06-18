"""Config loader for paper trading components."""

from __future__ import annotations

from pathlib import Path

import yaml

from laoma_signal_engine.core.config_loader import package_root
from laoma_signal_engine.paper.models import PaperConfig


def load_paper_config(project_root: Path | None = None) -> PaperConfig:
    """Load the paper config from the package default YAML.

    The project_root argument is accepted for symmetry with other loaders; paper
    paths remain project-relative strings inside PaperConfig.
    """

    cfg_path = package_root() / "config" / "default.yaml"
    doc = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    paper = doc.get("paper") or {}
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
        require_position_sizing=bool(paper.get("require_position_sizing", False)),
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
        trigger_sl_first=bool(paper.get("trigger_sl_first", True)),
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
