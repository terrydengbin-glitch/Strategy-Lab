"""Fixture parity: legacy vs async snapshot must match on identical mock HTTP payloads (Step 1.51)."""

from __future__ import annotations

import asyncio
import json
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import orjson
import pytest

from laoma_signal_engine.market import futures_light_snapshot as fs_mod
from laoma_signal_engine.market import light_snapshot_async as async_mod
from laoma_signal_engine.market.futures_light_snapshot import run_fetch_light_snapshot
from laoma_signal_engine.market.light_snapshot_async import (
    _latency_summary,
    dry_run_plan_dict,
    run_fetch_light_snapshot_async,
)
from laoma_signal_engine.market.kline_fetcher import parse_klines_response
from laoma_signal_engine.market.light_snapshot_models import FuturesLightSnapshotDocument


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "light_snapshot"


def _load_fixture_list(name: str) -> Any:
    p = FIXTURE_DIR / name
    with open(p, encoding="utf-8") as fp:
        return json.load(fp)


def _fake_fetch_klines(
    client: httpx.Client,
    symbol: str,
    interval: str,
    limit: int,
) -> list[Any]:
    raw = _load_fixture_list(f"klines_{symbol}_{interval}.json")
    if not isinstance(raw, list):
        raise TypeError("bad fixture")
    chunk = raw[-limit:] if len(raw) >= limit else raw
    return parse_klines_response(chunk)


def _fake_fetch_ticker_24h_all(client: httpx.Client) -> list[dict[str, Any]]:
    raw = _load_fixture_list("ticker_24hr.json")
    if not isinstance(raw, list):
        raise TypeError("bad ticker fixture")
    return [x for x in raw if isinstance(x, dict)]


async def _fake_exchange_weight_limit(client: httpx.AsyncClient) -> int:
    return 2400


async def _fake_limiter_get(
    self: Any,
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
) -> httpx.Response:
    mp = dict(params or {})
    if "ticker" in url and "24hr" in url:
        data = _load_fixture_list("ticker_24hr.json")
    elif "/klines" in url:
        sym = str(mp.get("symbol", ""))
        interval = str(mp.get("interval", ""))
        data = _load_fixture_list(f"klines_{sym}_{interval}.json")
    else:
        raise AssertionError(f"unexpected url={url} params={mp}")
    req = httpx.Request("GET", url, params=mp)
    return httpx.Response(200, json=data, request=req, headers={"X-MBX-USED-WEIGHT-1M": "12"})


def _snapshot_from_output(path: Path) -> FuturesLightSnapshotDocument:
    with open(path, encoding="utf-8") as fp:
        raw = json.load(fp)
    return FuturesLightSnapshotDocument.model_validate(raw)


def _normalize_for_compare(d: dict[str, Any]) -> dict[str, Any]:
    errs = d.get("errors") or []
    if isinstance(errs, list):
        d = dict(d)
        d["errors"] = sorted(
            errs,
            key=lambda x: (
                str((x or {}).get("symbol", "")),
                str((x or {}).get("error_code", "")),
                str((x or {}).get("stage", "")),
            ),
        )
    return d


def test_legacy_vs_async_fixture_snapshot_parity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    univ_dst = tmp_path / "DATA" / "universe"
    univ_dst.mkdir(parents=True)
    shutil.copy(FIXTURE_DIR / "universe_min.json", univ_dst / "CANDIDATE_UNIVERSE.json")

    fixed = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(fs_mod, "utc_now", lambda: fixed)
    monkeypatch.setattr(async_mod, "utc_now", lambda: fixed)
    monkeypatch.setattr(time, "time", lambda: 1700000900.0)
    monkeypatch.setattr(time, "sleep", lambda *a, **kw: None)

    monkeypatch.setattr(fs_mod, "fetch_klines", _fake_fetch_klines)
    monkeypatch.setattr(fs_mod, "fetch_ticker_24h_all", _fake_fetch_ticker_24h_all)

    monkeypatch.setattr(async_mod, "fetch_request_weight_limit_1m_async", _fake_exchange_weight_limit)
    monkeypatch.setattr(async_mod.AsyncIpWeightLimiter, "get", _fake_limiter_get)

    out_leg = tmp_path / "snapshot_legacy.json"
    out_async = tmp_path / "snapshot_async.json"

    code_leg = run_fetch_light_snapshot(
        project_root=tmp_path,
        limit=1,
        symbols_filter=["BTCUSDT"],
        max_concurrency=2,
        output_path=out_leg,
        fetch_mode="legacy",
    )
    assert code_leg == 0

    code_async = asyncio.run(
        run_fetch_light_snapshot_async(
            project_root=tmp_path,
            limit=1,
            symbols_filter=["BTCUSDT"],
            max_concurrency=2,
            output_path=out_async,
        )
    )
    assert code_async == 0

    doc_leg = _snapshot_from_output(out_leg)
    doc_async = _snapshot_from_output(out_async)

    d_leg = _normalize_for_compare(doc_leg.model_dump(mode="json"))
    d_async = _normalize_for_compare(doc_async.model_dump(mode="json"))
    assert d_leg == d_async

    raw_leg = orjson.dumps(d_leg, option=orjson.OPT_SORT_KEYS)
    raw_async = orjson.dumps(d_async, option=orjson.OPT_SORT_KEYS)
    assert raw_leg == raw_async


def test_dry_run_plan_json_has_rough_weight(tmp_path: Path) -> None:
    univ_dst = tmp_path / "DATA" / "universe"
    univ_dst.mkdir(parents=True)
    shutil.copy(FIXTURE_DIR / "universe_min.json", univ_dst / "CANDIDATE_UNIVERSE.json")

    rec = dry_run_plan_dict(
        project_root=tmp_path,
        limit=1,
        symbols_filter=["BTCUSDT"],
        max_concurrency=2,
        fetch_mode="async",
    )

    assert rec["dry_run"] is True
    assert rec["requested_count"] == 1
    assert rec["planned_kline_requests"] == 3
    assert rec["planned_exchangeInfo_requests"] == 1
    assert rec["estimated_kline_weight_per_symbol"] == 4
    assert rec["estimated_ticker_24h_weight"] == 40
    assert rec["estimated_weight_rough"] == 45
    assert rec["estimated_weight"] == rec["estimated_weight_rough"]
    assert rec["rate_limit_source"] == "dry_run_rough_estimate"


def test_latency_summary_reports_avg_p95_and_count() -> None:
    avg, p95, count = _latency_summary([10.0, 30.0, 20.0])
    assert avg == 20.0
    assert p95 == 30.0
    assert count == 3

    avg_empty, p95_empty, count_empty = _latency_summary([])
    assert avg_empty is None
    assert p95_empty is None
    assert count_empty == 0
