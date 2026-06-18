"""Tests for exchangeInfo weight parsing (Step 1.51)."""

from __future__ import annotations

from laoma_signal_engine.market.kline_fetcher import request_weight_limit_1m_from_exchange_info


def test_request_weight_limit_1m_from_exchange_info() -> None:
    sample = {
        "rateLimits": [
            {
                "rateLimitType": "REQUEST_WEIGHT",
                "interval": "MINUTE",
                "intervalNum": 1,
                "limit": 2400,
            }
        ]
    }
    assert request_weight_limit_1m_from_exchange_info(sample) == 2400


def test_request_weight_limit_missing_returns_none() -> None:
    assert request_weight_limit_1m_from_exchange_info({}) is None
    assert request_weight_limit_1m_from_exchange_info({"rateLimits": []}) is None
