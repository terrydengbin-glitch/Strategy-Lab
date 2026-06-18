"""STEP3.8 Micro Collector daemon. docs/STEP3.8_任务卡.md."""

from __future__ import annotations

from laoma_signal_engine.micro.daemon.app import run_daemon
from laoma_signal_engine.micro.daemon.config import DaemonConfig
from laoma_signal_engine.micro.daemon.loop import (
    CollectStartedAckBridge,
    DaemonRunContext,
    build_run_context,
    run_once,
)

__all__ = [
    "CollectStartedAckBridge",
    "DaemonConfig",
    "DaemonRunContext",
    "build_run_context",
    "run_daemon",
    "run_once",
]
