from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import orjson
import pytest

from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.market import light_snapshot_async as async_mod
from laoma_signal_engine.market.light_snapshot_settings import load_light_snapshot_settings
from laoma_signal_engine.market.rest_circuit import read_rest_circuit
from laoma_signal_engine.market.rest_circuit import write_rest_circuit_open


def _settings(tmp_path: Path) -> Any:
    return replace(
        load_light_snapshot_settings(),
        exchange_info_cache_path=str(tmp_path / "DATA" / "market" / "exchange_info_futures_cache.json"),
        exchange_info_cache_ttl_sec=86400,
        exchange_info_allow_cache_on_429_418=True,
        exchange_info_fail_if_cache_missing=True,
    )


def _http_status_error(status_code: int, *, retry_after: str | None = None) -> httpx.HTTPStatusError:
    req = httpx.Request("GET", "https://fapi.binance.com/fapi/v1/exchangeInfo")
    headers = {"Retry-After": retry_after} if retry_after is not None else None
    resp = httpx.Response(status_code, request=req, headers=headers)
    return httpx.HTTPStatusError(f"HTTP {status_code}", request=req, response=resp)


def test_exchange_info_live_writes_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def fake_fetch(client: httpx.AsyncClient) -> int:
        return 1234

    monkeypatch.setattr(async_mod, "fetch_request_weight_limit_1m_async", fake_fetch)
    settings = _settings(tmp_path)

    async def run() -> tuple[int | None, dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            return await async_mod._resolve_request_weight_limit_1m(client, tmp_path, settings)

    limit, meta = asyncio.run(run())

    assert limit == 1234
    assert meta["exchange_info_source"] == "live"
    assert meta["exchange_info_fallback_used"] is False
    cache_path = Path(meta["exchange_info_cache_path"])
    assert cache_path.exists()
    raw = orjson.loads(cache_path.read_bytes())
    assert raw["schema_version"] == async_mod.EXCHANGE_INFO_CACHE_SCHEMA
    assert raw["exchangeInfo"]["rateLimits"][0]["limit"] == 1234


def test_exchange_info_418_uses_fresh_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    cache_path = Path(settings.exchange_info_cache_path)
    async_mod._write_exchange_info_cache(tmp_path, settings, limit_1m=2400, source="unit_test")

    async def fake_fetch(client: httpx.AsyncClient) -> int:
        raise _http_status_error(418)

    monkeypatch.setattr(async_mod, "fetch_request_weight_limit_1m_async", fake_fetch)

    async def run() -> tuple[int | None, dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            return await async_mod._resolve_request_weight_limit_1m(client, tmp_path, settings)

    limit, meta = asyncio.run(run())

    assert cache_path.exists()
    assert limit == 2400
    assert meta["exchange_info_source"] == "cache"
    assert meta["exchange_info_live_attempted"] is False
    assert meta["exchange_info_fallback_used"] is False
    assert meta["light_snapshot_status"] == "degraded_cache"
    assert "exchange_info_cache_used" in meta["reason_codes"]


def test_exchange_info_live_418_uses_fresh_cache_when_cache_first_disabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = replace(_settings(tmp_path), exchange_info_cache_first_enabled=False)
    async_mod._write_exchange_info_cache(tmp_path, settings, limit_1m=2400, source="unit_test")

    async def fake_fetch(client: httpx.AsyncClient) -> int:
        raise _http_status_error(418)

    monkeypatch.setattr(async_mod, "fetch_request_weight_limit_1m_async", fake_fetch)

    async def run() -> tuple[int | None, dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            return await async_mod._resolve_request_weight_limit_1m(client, tmp_path, settings)

    limit, meta = asyncio.run(run())

    assert limit == 2400
    assert meta["exchange_info_source"] == "cache"
    assert meta["exchange_info_live_attempted"] is True
    assert meta["exchange_info_fallback_used"] is True
    assert meta["light_snapshot_status"] == "degraded_cache"
    assert "exchange_info_live_418" in meta["reason_codes"]
    assert "exchange_info_cache_used" in meta["reason_codes"]


def test_exchange_info_418_without_cache_records_fail_closed_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)

    async def fake_fetch(client: httpx.AsyncClient) -> int:
        raise _http_status_error(418, retry_after="7200")

    monkeypatch.setattr(async_mod, "fetch_request_weight_limit_1m_async", fake_fetch)

    async def run() -> None:
        async with httpx.AsyncClient() as client:
            await async_mod._resolve_request_weight_limit_1m(client, tmp_path, settings)

    with pytest.raises(async_mod.ExchangeInfoFailClosed) as err:
        asyncio.run(run())

    meta = err.value.meta
    assert meta["exchange_info_source"] == "live_failed_no_cache"
    assert meta["exchange_info_live_error"] == "http_418"
    assert meta["exchange_info_live_status_code"] == 418
    assert meta["exchange_info_retry_after_sec"] == 7200
    assert meta["exchange_info_cache_reason"] == "exchange_info_cache_missing"
    assert meta["exchange_info_fallback_used"] is False
    assert meta["light_snapshot_status"] == "failed"
    assert "exchange_info_live_418" in meta["reason_codes"]
    assert "exchange_info_cache_missing" in meta["reason_codes"]
    assert "exchange_info_fail_closed" in meta["reason_codes"]
    circuit = read_rest_circuit(tmp_path)
    assert circuit["rest_circuit_state"] == "open"
    assert circuit["rest_circuit_reason"] == "http_418"


def test_exchange_info_429_without_cache_records_retry_after(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)

    async def fake_fetch(client: httpx.AsyncClient) -> int:
        raise _http_status_error(429, retry_after="180")

    monkeypatch.setattr(async_mod, "fetch_request_weight_limit_1m_async", fake_fetch)

    async def run() -> None:
        async with httpx.AsyncClient() as client:
            await async_mod._resolve_request_weight_limit_1m(client, tmp_path, settings)

    with pytest.raises(async_mod.ExchangeInfoFailClosed) as err:
        asyncio.run(run())

    meta = err.value.meta
    assert meta["exchange_info_live_error"] == "http_429"
    assert meta["exchange_info_live_status_code"] == 429
    assert meta["exchange_info_retry_after_sec"] == 180
    assert "exchange_info_live_429" in meta["reason_codes"]
    assert "exchange_info_fail_closed" in meta["reason_codes"]
    circuit = read_rest_circuit(tmp_path)
    assert circuit["rest_circuit_state"] == "open"
    assert circuit["retry_after_sec"] == 180


def test_exchange_info_open_circuit_with_fresh_cache_uses_cache_without_live(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async_mod._write_exchange_info_cache(tmp_path, settings, limit_1m=2400, source="unit_test")
    write_rest_circuit_open(
        tmp_path,
        status_code=418,
        endpoint="https://fapi.binance.com/fapi/v1/ticker/24hr",
        source_stage="unit_test",
        retry_after_sec=3600,
    )

    async def fake_fetch(client: httpx.AsyncClient) -> int:
        raise AssertionError("live exchangeInfo should not be attempted while circuit is open")

    monkeypatch.setattr(async_mod, "fetch_request_weight_limit_1m_async", fake_fetch)

    async def run() -> tuple[int | None, dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            return await async_mod._resolve_request_weight_limit_1m(client, tmp_path, settings)

    limit, meta = asyncio.run(run())

    assert limit == 2400
    assert meta["exchange_info_source"] == "cache"
    assert meta["exchange_info_live_attempted"] is False
    assert "rest_circuit_open" in meta["reason_codes"]


def test_exchange_info_expired_cache_records_cache_expired_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    cache_path = Path(settings.exchange_info_cache_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(
        orjson.dumps(
            {
                "schema_version": async_mod.EXCHANGE_INFO_CACHE_SCHEMA,
                "fetched_at": "2020-01-01T00:00:00Z",
                "source": "unit_test",
                "exchangeInfo": {
                    "rateLimits": [
                        {
                            "rateLimitType": "REQUEST_WEIGHT",
                            "interval": "MINUTE",
                            "intervalNum": 1,
                            "limit": 2400,
                        }
                    ]
                },
            }
        )
    )

    async def fake_fetch(client: httpx.AsyncClient) -> int:
        raise _http_status_error(418)

    monkeypatch.setattr(async_mod, "fetch_request_weight_limit_1m_async", fake_fetch)

    async def run() -> None:
        async with httpx.AsyncClient() as client:
            await async_mod._resolve_request_weight_limit_1m(client, tmp_path, settings)

    with pytest.raises(async_mod.ExchangeInfoFailClosed) as err:
        asyncio.run(run())

    meta = err.value.meta
    assert meta["exchange_info_source"] == "stale_cache_blocked"
    assert meta["exchange_info_cache_reason"] == "exchange_info_cache_expired"
    assert "exchange_info_cache_expired" in meta["reason_codes"]
    assert "exchange_info_fail_closed" in meta["reason_codes"]


def _minimal_snapshot(symbols: list[str]) -> dict[str, Any]:
    return {
        "schema_version": "test",
        "generated_at": to_iso_z(utc_now()),
        "source": "unit_test",
        "universe_generated_at": to_iso_z(utc_now()),
        "universe_age_sec": 1,
        "universe_count": len(symbols),
        "eligible_futures_count": len(symbols),
        "snapshot_count": len(symbols),
        "success_count": len(symbols),
        "failed_count": 0,
        "skipped_count": 0,
        "timeframe_contract": {
            "primary_tf": "15m",
            "trigger_tf": "5m",
            "entry_tf": "1m",
            "background_tfs": ["1h"],
            "decision_basis": "unit_test",
        },
        "items": [
            {
                "symbol": sym,
                "base_asset": sym.replace("USDT", ""),
                "last_price": 1.0,
                "primary_15m": {"ready": True},
                "trigger_5m": {},
                "entry_1m": {},
                "background": {},
                "reason_codes": [],
                "data_quality": {
                    "kline_1m_ready": True,
                    "kline_5m_ready": True,
                    "kline_15m_ready": True,
                    "kline_1h_ready": True,
                    "ticker_24h_ready": True,
                },
            }
            for sym in symbols
        ],
        "errors": [],
        "pools": {"core": symbols},
        "snapshot_quality": {},
    }


def test_market_snapshot_cache_contract_allows_partial_safe_coverage(tmp_path: Path) -> None:
    settings = replace(
        _settings(tmp_path),
        market_snapshot_cache_ttl_sec=120,
        market_snapshot_cache_min_coverage_ratio=0.5,
    )
    path = tmp_path / "DATA" / "market" / "futures_light_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orjson.dumps(_minimal_snapshot(["BTCUSDT", "ETHUSDT"])))

    cache = async_mod._load_market_snapshot_cache(
        path,
        requested_symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
        settings=settings,
    )

    assert cache["fresh"] is True
    assert cache["coverage_ratio"] == pytest.approx(2 / 3, rel=0.01)
    assert cache["covered_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert cache["missing_symbols"] == ["BNBUSDT"]

    quality = async_mod._cached_snapshot_quality(
        cache=cache,
        requested_count=3,
        eligible_count=3,
        skipped_base=0,
        circuit={"rest_circuit_state": "closed"},
        exchange_info_meta={"exchange_info_source": "cache", "reason_codes": []},
    )

    assert quality["market_snapshot_source"] == "cache"
    assert quality["market_snapshot_live_attempted"] is False
    assert quality["skipped_symbol_count"] == 1
    assert "market_snapshot_cache_partial_coverage" in quality["reason_codes"]
