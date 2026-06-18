"""Normalized Binance micro inputs. See docs/STEP3.1_任务卡.md section 5."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class NormalizedTrade:
    """docs/STEP3.1_任务卡.md section 5.1"""

    symbol: str
    ts_ms: int
    price: float
    qty: float
    side: Literal["buy", "sell"]


@dataclass(frozen=True)
class NormalizedBook:
    """docs/STEP3.1_任务卡.md section 5.2"""

    symbol: str
    ts_ms: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    levels: int
