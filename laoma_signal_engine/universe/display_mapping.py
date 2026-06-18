"""Map internal base_asset (e.g. 1000PEPE) to social/display names (e.g. PEPE)."""

from __future__ import annotations

import re

# Binance USDT-M often encodes notional multipliers in baseAsset (1000X, 10000X, ...).
_KNOWN_PREFIXES: tuple[str, ...] = (
    "1000000",
    "100000",
    "10000",
    "1000",
)

_DISPLAY_BASE = re.compile(
    r"^(?P<prefix>" + "|".join(_KNOWN_PREFIXES) + r")(?P<rest>[A-Z0-9]+)$"
)


def display_base_asset_from_internal(base_asset: str) -> str:
    """Strip a leading contract multiplier prefix when present (ASCII uppercase in, uppercase out)."""
    u = base_asset.strip().upper()
    m = _DISPLAY_BASE.match(u)
    if m:
        return m.group("rest")
    return u


def contract_multiplier_from_internal(base_asset: str) -> int:
    """Return the explicit Binance contract multiplier prefix when present."""
    u = base_asset.strip().upper()
    m = _DISPLAY_BASE.match(u)
    if not m:
        return 1
    try:
        return int(m.group("prefix"))
    except (TypeError, ValueError):
        return 1


def cashtag_from_display(display_base: str) -> str:
    return f"${display_base}"


def spot_cashtag_symbol_from_display(display_base: str) -> str:
    """Normalized spot-style label for posts (not always equal to exchange spot symbol)."""
    return f"{display_base}USDT"
