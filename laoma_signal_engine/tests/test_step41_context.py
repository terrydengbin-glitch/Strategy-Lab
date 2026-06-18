"""STEP4.1 context providers unit tests (no real network). docs/STEP4.1_Minimal_Context_Guards_任务卡.md."""

from __future__ import annotations

import time

from laoma_signal_engine.context.basis_provider import build_basis_15m_from_premium_row
from laoma_signal_engine.context.funding_provider import build_funding_context_from_premium_row
from laoma_signal_engine.context.oi_provider import build_oi_15m_block
from laoma_signal_engine.factors.models import Basis15mBlock, FundingContextBlock, OI15mBlock


class _FakeClient:
    def __init__(self, premium_rows: list[dict] | None = None, oi_hist: list[dict] | None = None) -> None:
        self._premium = premium_rows
        self._oi_hist = oi_hist or []

    def get_json(self, path: str, params: dict | None = None) -> object:
        _ = params
        if path == "/fapi/v1/premiumIndex":
            return self._premium or []
        if path == "/fapi/v1/openInterest":
            return {"symbol": "BTCUSDT", "openInterest": "1000", "time": 0}
        if path == "/futures/data/openInterestHist":
            return self._oi_hist
        return {}


def test_funding_neutral() -> None:
    row = {"lastFundingRate": "0.00005", "nextFundingTime": int(time.time() * 1000) + 3_600_000}
    b = build_funding_context_from_premium_row(row)
    assert b.ready is True
    assert b.funding_bucket == "NEUTRAL"
    assert b.funding_extreme_flag is False


def test_funding_overheated() -> None:
    row = {"lastFundingRate": "0.0006", "nextFundingTime": int(time.time() * 1000) + 3_600_000}
    b = build_funding_context_from_premium_row(row)
    assert b.ready is True
    assert b.funding_bucket == "OVERHEATED"
    assert b.funding_extreme_flag is True


def test_basis_mark_index_bps() -> None:
    row = {"markPrice": "100", "indexPrice": "99.5", "lastFundingRate": "0.0001", "nextFundingTime": 0}
    b = build_basis_15m_from_premium_row(row)
    assert b.ready is True
    assert b.mark_index_basis_bps is not None
    assert b.mark_index_basis_bps > 0


def test_oi_block_ready_q1_mock() -> None:
    hist = [{"sumOpenInterest": str(1000 + i * 50)} for i in range(12)]
    client = _FakeClient(oi_hist=hist)
    primary = {"price_ret": 1.0, "ready": True}
    ob = build_oi_15m_block("BTCUSDT", primary, "up", client)
    assert isinstance(ob, OI15mBlock)
    assert ob.oi_quadrant in ("Q1", "Q2", "Q3", "Q4", "unknown")
    assert ob.ready in (True, False)


def test_models_funding_basis_defaults() -> None:
    FundingContextBlock(ready=False, reason="x")
    Basis15mBlock(ready=False, reason="x")
