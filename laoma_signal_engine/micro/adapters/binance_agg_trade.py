"""Binance aggTrade -> NormalizedTrade. docs/STEP3.1_任务卡.md section 5.4."""

from __future__ import annotations

from typing import Any, Literal

from laoma_signal_engine.micro.adapters.binance_common import normalize_binance_symbol
from laoma_signal_engine.micro.normalized_models import NormalizedTrade


def normalize_agg_trade(event: dict[str, Any]) -> NormalizedTrade:
    if not isinstance(event, dict):
        msg = "aggTrade event must be a dict"
        raise TypeError(msg)

    sym_raw = event.get("s")
    if sym_raw is None:
        msg = "aggTrade missing s"
        raise ValueError(msg)
    symbol = normalize_binance_symbol(sym_raw)

    ts_ms: int | None = None
    if "T" in event and event["T"] is not None:
        try:
            ts_ms = int(event["T"])
        except (TypeError, ValueError) as e:
            msg = "aggTrade T must be int-convertible"
            raise ValueError(msg) from e
    elif "E" in event and event["E"] is not None:
        try:
            ts_ms = int(event["E"])
        except (TypeError, ValueError) as e:
            msg = "aggTrade E must be int-convertible"
            raise ValueError(msg) from e
    if ts_ms is None:
        msg = "aggTrade missing both T and E"
        raise ValueError(msg)

    try:
        price = float(event["p"])
        qty = float(event["q"])
    except KeyError as e:
        missing = e.args[0]
        msg = f"aggTrade missing {missing}"
        raise ValueError(msg) from e
    except (TypeError, ValueError) as e:
        msg = "aggTrade p/q must be float-convertible"
        raise ValueError(msg) from e

    if "m" not in event:
        msg = "aggTrade missing m"
        raise ValueError(msg)
    m = event["m"]
    if not isinstance(m, bool):
        msg = "aggTrade m must be bool"
        raise ValueError(msg)
    side: Literal["buy", "sell"] = "sell" if m is True else "buy"

    return NormalizedTrade(symbol=symbol, ts_ms=ts_ms, price=price, qty=qty, side=side)
