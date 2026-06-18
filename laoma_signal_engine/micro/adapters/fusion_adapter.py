"""FusionAdapter: fuse z-scores -> engine dict. docs/STEP3.1_任务卡.md section 6."""

from __future__ import annotations

from typing import Any

from laoma_signal_engine.micro.calculators.fusion import FusionEngine, FusionParams


class FusionAdapter:
    def __init__(self, params: FusionParams | None = None) -> None:
        self._engine = FusionEngine(params)

    def fuse(
        self,
        ts_ms: int,
        z_ofi: float,
        z_cvd: float,
        lag_sec: float = 0.0,
    ) -> dict[str, Any]:
        return self._engine.fuse(ts_ms=ts_ms, z_ofi=z_ofi, z_cvd=z_cvd, lag_sec=lag_sec)
