"""OFIAdapter: NormalizedBook -> engine dict. docs/STEP3.1_任务卡.md section 6."""

from __future__ import annotations

from typing import Any

from laoma_signal_engine.micro.adapters.binance_common import normalize_binance_symbol
from laoma_signal_engine.micro.calculators.ofi import OFIEngine, OFIParams
from laoma_signal_engine.micro.normalized_models import NormalizedBook


class OFIAdapter:
    def __init__(self, symbol: str, params: OFIParams | None = None) -> None:
        self._symbol = normalize_binance_symbol(symbol)
        self._params = params if params is not None else OFIParams()
        self._engine = OFIEngine(self._symbol, self._params)

    def update_book(self, norm: NormalizedBook) -> dict[str, Any]:
        if norm.symbol != self._symbol:
            msg = "NormalizedBook symbol does not match adapter symbol"
            raise ValueError(msg)
        if norm.levels != self._params.levels:
            msg = "NormalizedBook levels does not match OFIParams.levels"
            raise ValueError(msg)
        return self._engine.update_with_snapshot(
            ts_ms=norm.ts_ms,
            bids=list(norm.bids),
            asks=list(norm.asks),
        )
