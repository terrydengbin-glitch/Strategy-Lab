"""Thresholds for STEP4.1 context guards (ASCII-only)."""

from __future__ import annotations

# Funding: absolute rate thresholds (fraction, e.g. 0.0001 = 0.01%)
FUNDING_ABS_NEUTRAL_MAX: float = 0.0001
FUNDING_ABS_WARM_MAX: float = 0.0005

# Basis: mark-index basis bps beyond which basis_extreme is true
BASIS_EXTREME_ABS_BPS: float = 50.0

# OI: minimum relative change to call "oi up" / "oi down" vs flat
OI_PCT_EPS: float = 0.001

# Primary price_ret neutral band (align with direction_gate NEUTRAL_EPS)
PRIMARY_PRICE_NEUTRAL_EPS: float = 0.05
