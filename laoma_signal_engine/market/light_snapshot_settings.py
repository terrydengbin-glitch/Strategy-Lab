"""Load Step 1.5 fetch + algorithm knobs from config/light_snapshot_fetch.yaml."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from laoma_signal_engine.core.config_loader import package_root


@dataclass(frozen=True)
class LightSnapshotSettings:
    max_concurrency: int
    request_timeout_sec: float
    retry_times: int
    retry_backoff_sec: float
    batch_sleep_ms: int
    max_symbols_per_run: int | None
    atr_period_15m: int
    entry_1m_bars: int
    top_gainer_rank_threshold: int
    top_loser_rank_threshold: int
    async_soft_limit_ratio: float
    async_hard_limit_ratio: float
    async_backoff_base_sec: float
    async_backoff_max_sec: float
    async_circuit_break_on_418: bool
    async_perf_log_path: str
    exchange_info_cache_path: str
    exchange_info_cache_ttl_sec: int
    exchange_info_cache_first_enabled: bool
    exchange_info_live_refresh_policy: str
    exchange_info_allow_cache_on_429_418: bool
    exchange_info_fail_if_cache_missing: bool
    rest_circuit_default_418_cooldown_sec: int
    rest_circuit_default_429_cooldown_sec: int
    market_snapshot_cache_first_enabled: bool
    market_snapshot_cache_ttl_sec: int
    market_snapshot_cache_min_coverage_ratio: float
    market_snapshot_fail_closed_on_circuit_open: bool
    rest_budget_preflight_enabled: bool
    rest_budget_min_remaining_weight: int
    step15_daemon_enabled: bool
    step15_daemon_tick_sec: int
    step15_daemon_stale_after_sec: int
    step15_daemon_shard_size_normal: int
    step15_daemon_shard_size_tight_budget: int
    step15_daemon_shard_size_half_open: int
    step15_daemon_half_open_success_required: int
    step15_daemon_close_after_successful_shards: int
    step15_daemon_half_open_expand_steps: list[int]
    step15_daemon_pid_path: str
    step15_daemon_heartbeat_path: str
    step15_daemon_status_path: str
    freshness_tradeable_hot_fresh_sec: int
    freshness_tradeable_hot_stale_usable_sec: int
    freshness_active_watch_fresh_sec: int
    freshness_active_watch_stale_usable_sec: int
    freshness_watch_only_fresh_sec: int
    freshness_watch_only_stale_usable_sec: int
    freshness_low_quality_fresh_sec: int
    freshness_low_quality_stale_usable_sec: int


def load_light_snapshot_settings() -> LightSnapshotSettings:
    path = package_root() / "config" / "light_snapshot_fetch.yaml"
    raw = path.read_text(encoding="utf-8")
    doc: dict[str, Any] = yaml.safe_load(raw) or {}
    f = doc.get("fetch") or {}
    ls = doc.get("light_snapshot") or {}
    af = doc.get("async_fetch") or {}
    daemon = doc.get("step15_daemon") or {}
    freshness = doc.get("freshness_sla") or {}
    hot = freshness.get("tradeable_hot") or {}
    active = freshness.get("active_watch") or {}
    watch = freshness.get("watch_only") or {}
    low = freshness.get("low_quality") or {}
    max_run = f.get("max_symbols_per_run")
    return LightSnapshotSettings(
        max_concurrency=int(f.get("max_concurrency", 8)),
        request_timeout_sec=float(f.get("request_timeout_sec", 10)),
        retry_times=int(f.get("retry_times", 2)),
        retry_backoff_sec=float(f.get("retry_backoff_sec", 1.5)),
        batch_sleep_ms=int(f.get("batch_sleep_ms", 200)),
        max_symbols_per_run=int(max_run) if max_run is not None else None,
        atr_period_15m=int(ls.get("atr_period_15m", 14)),
        entry_1m_bars=int(ls.get("entry_1m_bars", 3)),
        top_gainer_rank_threshold=int(ls.get("top_gainer_rank_threshold", 10)),
        top_loser_rank_threshold=int(ls.get("top_loser_rank_threshold", 10)),
        async_soft_limit_ratio=float(af.get("soft_limit_ratio", 0.80)),
        async_hard_limit_ratio=float(af.get("hard_limit_ratio", 0.95)),
        async_backoff_base_sec=float(af.get("backoff_base_sec", 1.0)),
        async_backoff_max_sec=float(af.get("backoff_max_sec", 30.0)),
        async_circuit_break_on_418=bool(af.get("circuit_break_on_418", True)),
        async_perf_log_path=str(af.get("perf_log_path", "DATA/logs/light_snapshot_perf.jsonl")),
        exchange_info_cache_path=str(af.get("exchange_info_cache_path", "DATA/market/exchange_info_futures_cache.json")),
        exchange_info_cache_ttl_sec=int(af.get("exchange_info_cache_ttl_sec", 86400)),
        exchange_info_cache_first_enabled=bool(af.get("exchange_info_cache_first_enabled", True)),
        exchange_info_live_refresh_policy=str(af.get("exchange_info_live_refresh_policy", "cache_missing_or_expired")),
        exchange_info_allow_cache_on_429_418=bool(af.get("exchange_info_allow_cache_on_429_418", True)),
        exchange_info_fail_if_cache_missing=bool(af.get("exchange_info_fail_if_cache_missing", True)),
        rest_circuit_default_418_cooldown_sec=int(af.get("rest_circuit_default_418_cooldown_sec", 3600)),
        rest_circuit_default_429_cooldown_sec=int(af.get("rest_circuit_default_429_cooldown_sec", 180)),
        market_snapshot_cache_first_enabled=bool(af.get("market_snapshot_cache_first_enabled", True)),
        market_snapshot_cache_ttl_sec=int(af.get("market_snapshot_cache_ttl_sec", 120)),
        market_snapshot_cache_min_coverage_ratio=float(af.get("market_snapshot_cache_min_coverage_ratio", 0.80)),
        market_snapshot_fail_closed_on_circuit_open=bool(af.get("market_snapshot_fail_closed_on_circuit_open", True)),
        rest_budget_preflight_enabled=bool(af.get("rest_budget_preflight_enabled", True)),
        rest_budget_min_remaining_weight=int(af.get("rest_budget_min_remaining_weight", 200)),
        step15_daemon_enabled=bool(daemon.get("enabled", True)),
        step15_daemon_tick_sec=int(daemon.get("tick_sec", 5)),
        step15_daemon_stale_after_sec=int(daemon.get("stale_after_sec", 30)),
        step15_daemon_shard_size_normal=int(daemon.get("shard_size_normal", 30)),
        step15_daemon_shard_size_tight_budget=int(daemon.get("shard_size_tight_budget", 10)),
        step15_daemon_shard_size_half_open=int(daemon.get("shard_size_half_open", 3)),
        step15_daemon_half_open_success_required=int(daemon.get("half_open_success_required", 5)),
        step15_daemon_close_after_successful_shards=int(daemon.get("close_after_successful_shards", 20)),
        step15_daemon_half_open_expand_steps=[int(x) for x in (daemon.get("half_open_expand_steps") or [3, 6, 12, 24])],
        step15_daemon_pid_path=str(daemon.get("pid_path", "DATA/runtime/step15_snapshot_daemon.pid")),
        step15_daemon_heartbeat_path=str(daemon.get("heartbeat_path", "DATA/runtime/step15_snapshot_daemon_heartbeat.json")),
        step15_daemon_status_path=str(daemon.get("status_path", "DATA/runtime/step15_snapshot_daemon_status.json")),
        freshness_tradeable_hot_fresh_sec=int(hot.get("fresh_sec", 60)),
        freshness_tradeable_hot_stale_usable_sec=int(hot.get("stale_usable_sec", 120)),
        freshness_active_watch_fresh_sec=int(active.get("fresh_sec", 120)),
        freshness_active_watch_stale_usable_sec=int(active.get("stale_usable_sec", 300)),
        freshness_watch_only_fresh_sec=int(watch.get("fresh_sec", 300)),
        freshness_watch_only_stale_usable_sec=int(watch.get("stale_usable_sec", 600)),
        freshness_low_quality_fresh_sec=int(low.get("fresh_sec", 600)),
        freshness_low_quality_stale_usable_sec=int(low.get("stale_usable_sec", 900)),
    )
