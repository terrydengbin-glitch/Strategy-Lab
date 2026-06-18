"""CVDAdapter: NormalizedTrade -> engine dict. docs/STEP3.1_任务卡.md section 6."""

from __future__ import annotations

from typing import Any

from laoma_signal_engine.micro.adapters.binance_common import normalize_binance_symbol
from laoma_signal_engine.micro.calculators.cvd import CVDEngine, CVDParams
from laoma_signal_engine.micro.normalized_models import NormalizedTrade


class CVDAdapter:
    def __init__(self, symbol: str, params: CVDParams | None = None) -> None:
        self._symbol = normalize_binance_symbol(symbol)
        self._engine = CVDEngine(self._symbol, params)

    def update_trade(self, norm: NormalizedTrade) -> dict[str, Any]:
        if norm.symbol != self._symbol:
            msg = "NormalizedTrade symbol does not match adapter symbol"
            raise ValueError(msg)
        return self._engine.update_with_trade(
            ts_ms=norm.ts_ms,
            price=norm.price,
            qty=norm.qty,
            side=norm.side,
        )
