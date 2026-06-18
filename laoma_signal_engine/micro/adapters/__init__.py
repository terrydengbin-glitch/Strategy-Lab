"""Micro Binance normalization and CVD/OFI/Fusion adapters. docs/STEP3.1_任务卡.md."""

from laoma_signal_engine.micro.adapters.binance_agg_trade import normalize_agg_trade
from laoma_signal_engine.micro.adapters.binance_book import (
    normalize_book_ticker,
    normalize_partial_depth5,
)
from laoma_signal_engine.micro.adapters.binance_common import normalize_binance_symbol
from laoma_signal_engine.micro.adapters.cvd_adapter import CVDAdapter
from laoma_signal_engine.micro.adapters.fusion_adapter import FusionAdapter
from laoma_signal_engine.micro.adapters.ofi_adapter import OFIAdapter

__all__ = [
    "CVDAdapter",
    "FusionAdapter",
    "OFIAdapter",
    "normalize_agg_trade",
    "normalize_binance_symbol",
    "normalize_book_ticker",
    "normalize_partial_depth5",
]
