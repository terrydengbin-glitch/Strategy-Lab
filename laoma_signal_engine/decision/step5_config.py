"""Load STEP5.0 planner + risk_gate knobs from laoma_signal_engine/config/default.yaml."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from laoma_signal_engine.core.config_loader import package_root


@dataclass(frozen=True)
class SlTpPlannerConfig:
    rr_to_tp1: float = 1.5
    rr_to_tp2: float = 2.5
    swing_sl_atr_buffer: float = 0.25
    entry_zone_atr_fraction: float = 0.1
    time_stop_minutes: int = 15


@dataclass(frozen=True)
class RiskGateConfig:
    min_rr_to_tp1: float = 1.0
    max_sl_atr_multiple: float = 4.0


@dataclass(frozen=True)
class Step5Bundle:
    planner: SlTpPlannerConfig
    risk: RiskGateConfig
    planner_config_version: str = "5.0-mvp"


def load_step5_config(project_root: Path | None = None) -> Step5Bundle:
    root = project_root.resolve() if project_root else Path.cwd().resolve()
    _ = root  # reserved for future project-local overrides
    cfg_path = package_root() / "config" / "default.yaml"
    raw_text = cfg_path.read_text(encoding="utf-8")
    doc: dict[str, Any] = yaml.safe_load(raw_text) or {}

    p = doc.get("sl_tp_planner") or {}
    r = doc.get("risk_gate") or {}

    planner = SlTpPlannerConfig(
        rr_to_tp1=float(p.get("rr_to_tp1", 1.5)),
        rr_to_tp2=float(p.get("rr_to_tp2", 2.5)),
        swing_sl_atr_buffer=float(p.get("swing_sl_atr_buffer", 0.25)),
        entry_zone_atr_fraction=float(p.get("entry_zone_atr_fraction", 0.1)),
        time_stop_minutes=int(p.get("time_stop_minutes", 15)),
    )
    risk = RiskGateConfig(
        min_rr_to_tp1=float(r.get("min_rr_to_tp1", 1.0)),
        max_sl_atr_multiple=float(r.get("max_sl_atr_multiple", 4.0)),
    )
    ver = str(p.get("planner_config_version", "5.0-mvp"))
    return Step5Bundle(planner=planner, risk=risk, planner_config_version=ver)
