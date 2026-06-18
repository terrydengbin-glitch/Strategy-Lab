"""Daemon configuration. docs/STEP3.8_任务卡.md."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

TransportKind = Literal["fake", "real"]


@dataclass
class DaemonConfig:
    """Runtime paths and cadence. YAML wiring can map into this in a later PR."""

    targets_path: Path
    latest_features_path: Path
    heartbeat_path: Path
    latest_state_path: Path | None = None
    target_reload_interval_sec: int = 5
    output_interval_sec: int = 2
    event_drain_interval_sec: float = 1.0
    target_stale_sec: int = 420
    unsubscribe_grace_sec: int = 600
    max_managed_symbols: int = 100
    transport: TransportKind = "fake"
    ring_buffer_seconds: int = 1800
    ws_per_connection_stream_limit: int = 80
    proxy_url: str | None = None
    ack_timeout_sec: float = 10.0
    real_base_url: str = "wss://fstream.binance.com"
    real_public_path: str = "/public"
    real_market_path: str = "/market"
    control_msg_rate_limit_per_sec: float = 5.0
    subscribe_batch_size: int = 50
    unsubscribe_batch_size: int = 50
    real_connect_timeout_sec: float = 10.0
