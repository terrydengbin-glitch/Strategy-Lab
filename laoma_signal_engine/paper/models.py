"""Models and constants for the P14 paper trading layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


STRATEGY_LINES = ("without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6")
ACTIVE_ORDER_STATUSES = ("pending_entry", "open", "tp1_hit")
OPEN_POSITION_STATUSES = ("open",)

StrategyLine = Literal["without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"]
Side = Literal["LONG", "SHORT"]
OrderStatus = Literal["pending_entry", "open", "tp1_hit", "closed", "cancelled", "rejected", "expired"]
OrderType = Literal["market", "limit", "trigger"]


@dataclass(frozen=True)
class PaperConfig:
    db_path: str = "DATA/paper/paper_trading.db"
    summary_path: str = "DATA/paper/latest_paper_state.json"
    default_account_equity_usdt: float = 1000.0
    default_margin_usdt: float = 100.0
    default_leverage: float = 20.0
    require_position_sizing: bool = False
    paper_fallback_notional_allowed: bool = True
    taker_fee_bps: float = 5.0
    maker_fee_bps: float = 2.0
    default_slippage_bps: float = 5.0
    fill_model_mode: str = "fixed_1m"
    use_trade_plan_slippage: bool = False
    use_liquidity_profile: bool = False
    entry_delay_sec: int = 0
    max_entry_drift_bps: float = 80.0
    default_market_slippage_bps: float | None = None
    fallback_market_slippage_bps: float = 15.0
    volatility_slippage_mult: float = 0.15
    thin_book_slippage_mult: float = 1.5
    max_allowed_paper_slippage_bps: float = 120.0
    slippage_too_high_policy: str = "cap"
    same_candle_sl_tp_policy: str = "sl_first"
    prevent_same_line_symbol_reentry: bool = True
    active_slot_scope: str = "strategy_line_symbol"
    allow_cross_line_same_symbol: bool = True
    trigger_sl_first: bool = True
    daemon_tick_interval_sec: int = 60
    daemon_lock_path: str = "DATA/runtime/paper_daemon.lock"
    daemon_pid_path: str = "DATA/runtime/paper_daemon.pid"
    daemon_log_path: str = "DATA/logs/paper_daemon.log"
    daemon_heartbeat_path: str = "DATA/runtime/paper_daemon_heartbeat.json"
    daemon_status_path: str = "DATA/runtime/paper_daemon_status.json"
    max_trade_plan_age_sec: int = 0
    reentry_cooldown_sec: int = 0
    reentry_cooldown_scope: str = "strategy_line_symbol_side"
    reentry_cooldown_after_sl: bool = True
    reentry_cooldown_after_tp: bool = False
    reentry_cooldown_after_forced_close: bool = True
    archive_enabled: bool = True
    archive_dir: str = "DATA/paper/archives"
    archive_metadata_path: str = "DATA/paper/paper_experiments.json"
    archive_force_close_exit_reason: str = "archive_reset_forced_close"


@dataclass(frozen=True)
class PaperIntent:
    strategy_line: str
    source: str
    source_path: str
    source_run_id: str | None
    source_cycle_id: str | None
    source_generated_at: str | None
    source_plan_hash: str
    signal_class: str
    paper_eligible: bool
    notify_eligible: bool
    source_executable: bool
    source_action: str
    source_entry_mode: str
    symbol: str
    side: str
    order_type: str
    intent_type: str
    opportunity_type: str
    reference_price: float
    entry_price: float
    stop_loss: float
    take_profit: float
    tp1: float | None = None
    leverage: float = 20.0
    margin_usdt: float = 100.0
    sizing_method: str | None = None
    risk_budget_usdt: float | None = None
    planned_quantity: float | None = None
    planned_notional_usdt: float | None = None
    estimated_max_loss_usdt: float | None = None
    reason_codes: list[str] = field(default_factory=list)
    guards: dict[str, Any] = field(default_factory=dict)
    source_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Candle:
    symbol: str
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass(frozen=True)
class FillCost:
    reference_price: float
    fill_price: float
    fee_bps: float
    fee_usdt: float
    slippage_bps: float
    slippage_usdt: float
    notional_usdt: float
    cost_source: str
    planned_entry_price: float | None = None
    entry_drift_bps: float = 0.0
    fill_delay_sec: float | None = None
    fill_model: str = "fixed_1m"
    slippage_source: str = "default"
    liquidity_penalty_bps: float = 0.0
    volatility_penalty_bps: float = 0.0
    same_candle_policy: str = "sl_first"
    source_generated_at: str | None = None
    consumed_at: str | None = None
