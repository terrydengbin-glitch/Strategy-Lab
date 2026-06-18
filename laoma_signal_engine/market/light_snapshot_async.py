"""Step 1.51: asyncio fetch for futures_light_snapshot (shared client + IP weight limiter)."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import orjson

from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.core.config_loader import EngineConfig
from laoma_signal_engine.core.exit_codes import EXIT_BINANCE, EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.models import CandidateUniverseDocument, UniversePairRow
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.market.futures_light_snapshot import (
    KLINES_1M_LIMIT,
    _build_item_for_symbol,
    _failed_item,
    _load_timeframe_contract,
    _pair_index,
    _parse_univ_time,
)
from laoma_signal_engine.market.kline_fetcher import (
    FUTURES_REST,
    KlineBar,
    fetch_request_weight_limit_1m_async,
    parse_klines_response,
    request_weight_limit_1m_from_exchange_info,
    ticker_by_symbol_map,
)
from laoma_signal_engine.market.light_snapshot_models import (
    FuturesLightSnapshotDocument,
    LightSnapshotItem,
    SnapshotErrorEntry,
)
from laoma_signal_engine.market.light_snapshot_settings import LightSnapshotSettings, load_light_snapshot_settings
from laoma_signal_engine.market.rest_circuit import read_rest_circuit, write_rest_circuit_open
from laoma_signal_engine.universe.step15_symbols import futures_symbols_for_step_1_5

log = logging.getLogger(__name__)

KLINES_15M_LIMIT = 45
KLINES_1H_LIMIT = 8
ESTIMATED_KLINE_WEIGHT_PER_SYMBOL = 4
ESTIMATED_TICKER_24H_WEIGHT = 40
DEFAULT_WEIGHT_LIMIT_1M = 2400
MAX_429_RETRIES = 8
EXCHANGE_INFO_CACHE_SCHEMA = "STEP1.59_exchange_info_cache_v1"


class BinanceCircuit418(Exception):
    """IP ban or 418 response; stop all outbound requests."""


class ExchangeInfoFailClosed(Exception):
    """exchangeInfo failed and no safe fallback is available; carries audit meta."""

    def __init__(self, message: str, *, meta: dict[str, Any], cause: BaseException) -> None:
        super().__init__(message)
        self.meta = meta
        self.__cause__ = cause


def _used_weight_from_response(response: httpx.Response) -> int | None:
    for key, val in response.headers.items():
        if key.upper() == "X-MBX-USED-WEIGHT-1M":
            try:
                return int(str(val).strip())
            except ValueError:
                return None
    return None


class AsyncIpWeightLimiter:
    """Global limiter: response header backpressure + 429 backoff + 418 circuit break."""

    def __init__(
        self,
        *,
        weight_limit_1m: int,
        soft_limit_ratio: float,
        hard_limit_ratio: float,
        backoff_base_sec: float,
        backoff_max_sec: float,
        circuit_break_on_418: bool,
        project_root: Path | None = None,
        source_stage: str = "step1_5_light_snapshot",
    ) -> None:
        self._weight_limit = max(1, int(weight_limit_1m))
        self._soft = float(soft_limit_ratio)
        self._hard = float(hard_limit_ratio)
        self._backoff_base = float(backoff_base_sec)
        self._backoff_max = float(backoff_max_sec)
        self._circuit_on_418 = bool(circuit_break_on_418)
        self._project_root = project_root.resolve() if project_root else None
        self._source_stage = source_stage
        self._lock = asyncio.Lock()
        self._last_used: int | None = None
        self.circuit_418 = False
        self.count_429 = 0
        self.count_418 = 0
        self.count_hard_throttle = 0
        self.retry_count = 0
        self.request_latencies_ms: list[float] = []
        self.endpoint_counts: dict[str, int] = {}
        self.status_code_counts: dict[str, int] = {}

    async def handle_response(self, response: httpx.Response) -> None:
        used = _used_weight_from_response(response)
        async with self._lock:
            if used is not None:
                self._last_used = used

        if response.status_code == 418 and self._circuit_on_418:
            self.count_418 += 1
            self.circuit_418 = True
            if self._project_root is not None:
                write_rest_circuit_open(
                    self._project_root,
                    status_code=418,
                    endpoint=str(response.request.url),
                    source_stage=self._source_stage,
                    retry_after_sec=_retry_after_from_response(response),
                    reason="http_418",
                )
            raise BinanceCircuit418("HTTP 418 from Binance")
        if response.status_code == 429 and self._project_root is not None:
            write_rest_circuit_open(
                self._project_root,
                status_code=429,
                endpoint=str(response.request.url),
                source_stage=self._source_stage,
                retry_after_sec=_retry_after_from_response(response),
                reason="http_429",
            )

        if used is None:
            return
        ratio = used / float(self._weight_limit)
        if ratio >= self._hard:
            pause = min(8.0, 2.0 + random.random() * 2.0)
            self.count_hard_throttle += 1
            log.warning("weight hard throttle used=%s limit=%s sleep=%.2fs", used, self._weight_limit, pause)
            await asyncio.sleep(pause)
        elif ratio >= self._soft:
            pause = 0.05 + random.random() * 0.08
            await asyncio.sleep(pause)

    def ensure_not_circuited(self) -> None:
        if self._project_root is not None:
            circuit = read_rest_circuit(self._project_root)
            if circuit.get("rest_circuit_state") == "open":
                raise BinanceCircuit418("global REST circuit open")
        if self.circuit_418:
            raise BinanceCircuit418("circuit open after 418")

    async def get(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        self.ensure_not_circuited()
        attempt = 0
        while True:
            self.ensure_not_circuited()
            t_req = time.perf_counter()
            response = await client.get(url, params=params)
            self.request_latencies_ms.append((time.perf_counter() - t_req) * 1000.0)
            endpoint_key = str(response.request.url).split("?", 1)[0]
            self.endpoint_counts[endpoint_key] = self.endpoint_counts.get(endpoint_key, 0) + 1
            status_key = str(int(response.status_code))
            self.status_code_counts[status_key] = self.status_code_counts.get(status_key, 0) + 1
            await self.handle_response(response)
            if response.status_code == 429:
                self.count_429 += 1
                self.retry_count += 1
                delay = min(
                    self._backoff_max,
                    self._backoff_base * (2**attempt) + random.random() * 0.5,
                )
                log.warning("HTTP 429 retry in %.2fs (attempt %s)", delay, attempt)
                await asyncio.sleep(delay)
                attempt += 1
                if attempt > MAX_429_RETRIES:
                    response.raise_for_status()
                continue
            response.raise_for_status()
            return response


def _latency_summary(values_ms: list[float]) -> tuple[float | None, float | None, int]:
    if not values_ms:
        return None, None, 0
    ordered = sorted(values_ms)
    avg = sum(ordered) / len(ordered)
    p95_idx = min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))
    return round(avg, 3), round(ordered[p95_idx], 3), len(ordered)


async def _fetch_symbol_klines(
    client: httpx.AsyncClient,
    limiter: AsyncIpWeightLimiter,
    symbol: str,
) -> tuple[list[KlineBar], list[KlineBar], list[KlineBar]]:
    url = f"{FUTURES_REST}/fapi/v1/klines"
    r1 = await limiter.get(
        client,
        url,
        params={"symbol": symbol, "interval": "1m", "limit": KLINES_1M_LIMIT},
    )
    k1 = parse_klines_response(r1.json())
    r15 = await limiter.get(
        client,
        url,
        params={"symbol": symbol, "interval": "15m", "limit": KLINES_15M_LIMIT},
    )
    k15 = parse_klines_response(r15.json())
    r1h = await limiter.get(
        client,
        url,
        params={"symbol": symbol, "interval": "1h", "limit": KLINES_1H_LIMIT},
    )
    k1h = parse_klines_response(r1h.json())
    return k1, k15, k1h


async def _one_symbol_task(
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
    limiter: AsyncIpWeightLimiter,
    symbol: str,
    pair: UniversePairRow,
    settings: LightSnapshotSettings,
) -> tuple[
    str,
    tuple[list[KlineBar], list[KlineBar], list[KlineBar]] | None,
    tuple[LightSnapshotItem, SnapshotErrorEntry] | None,
]:
    await asyncio.sleep(0)
    async with sem:
        try:
            k1, k15, k1h = await _fetch_symbol_klines(client, limiter, symbol)
            return symbol, (k1, k15, k1h), None
        except (httpx.HTTPError, OSError, TypeError, ValueError) as exc:
            log.warning("async kline fetch failed %s: %s", symbol, exc)
            item, err = _failed_item(
                symbol,
                pair,
                code="KLINE_FETCH_FAILED",
                message=str(exc),
                stage="fetch_klines_async",
                reasons=["kline_fetch_failed"],
            )
            return symbol, None, (item, err)


def append_perf_log(project_root: Path, rel_path: str, record: dict[str, Any]) -> None:
    log_path = (project_root / rel_path).resolve()
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE).decode("utf-8")
        with open(log_path, "a", encoding="utf-8", newline="") as fp:
            fp.write(line)
    except OSError as exc:
        log.warning("perf log append failed: %s", exc)


def _exchange_info_cache_path(project_root: Path, settings: LightSnapshotSettings) -> Path:
    p = Path(settings.exchange_info_cache_path)
    if not p.is_absolute():
        p = project_root / p
    return p.resolve()


def _status_code_from_exc(exc: BaseException) -> int | None:
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            return int(exc.response.status_code)
        except Exception:
            return None
    return None


def _retry_after_from_exc(exc: BaseException) -> int | None:
    if not isinstance(exc, httpx.HTTPStatusError):
        return None
    value = exc.response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except ValueError:
        return None


def _retry_after_from_response(response: httpx.Response) -> int | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except ValueError:
        return None


def _fail_closed_meta(meta: dict[str, Any], cache_reason: Any) -> dict[str, Any]:
    out = dict(meta)
    reason_codes = list(out.get("reason_codes") or [])
    if cache_reason:
        cache_code = str(cache_reason).split(":", 1)[0]
        if cache_code not in reason_codes:
            reason_codes.append(cache_code)
    if "exchange_info_fail_closed" not in reason_codes:
        reason_codes.append("exchange_info_fail_closed")
    source = "stale_cache_blocked" if cache_reason and "expired" in str(cache_reason) else "live_failed_no_cache"
    out.update(
        {
            "exchange_info_source": source,
            "exchange_info_fallback_used": False,
            "light_snapshot_status": "failed",
            "reason_codes": reason_codes,
        }
    )
    return out


def _load_exchange_info_cache(
    project_root: Path,
    settings: LightSnapshotSettings,
) -> dict[str, Any]:
    path = _exchange_info_cache_path(project_root, settings)
    now = utc_now()
    base: dict[str, Any] = {
        "path": str(path),
        "data": None,
        "limit": None,
        "age_sec": None,
        "ttl_sec": int(settings.exchange_info_cache_ttl_sec),
        "fresh": False,
        "reason": None,
    }
    try:
        raw = read_json_object(path)
    except OSError:
        base["reason"] = "exchange_info_cache_missing"
        return base
    except (TypeError, ValueError) as exc:
        base["reason"] = f"exchange_info_cache_invalid:{str(exc)[:120]}"
        return base
    if not isinstance(raw, dict):
        base["reason"] = "exchange_info_cache_invalid"
        return base
    fetched_at = raw.get("fetched_at")
    age_sec: int | None = None
    if isinstance(fetched_at, str):
        try:
            from datetime import datetime

            dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
            age_sec = max(0, int((now - dt).total_seconds()))
        except ValueError:
            age_sec = None
    data = raw.get("exchangeInfo")
    if not isinstance(data, dict):
        # Keep compatibility with a raw exchangeInfo-shaped cache.
        data = raw
    limit = request_weight_limit_1m_from_exchange_info(data)
    base.update(
        {
            "data": data,
            "limit": limit,
            "age_sec": age_sec,
            "fresh": bool(
                limit is not None
                and age_sec is not None
                and age_sec <= int(settings.exchange_info_cache_ttl_sec)
            ),
            "reason": None,
        }
    )
    if limit is None:
        base["reason"] = "exchange_info_cache_no_request_weight"
    elif age_sec is None:
        base["reason"] = "exchange_info_cache_missing_fetched_at"
    elif not base["fresh"]:
        base["reason"] = "exchange_info_cache_expired"
    return base


def _write_exchange_info_cache(
    project_root: Path,
    settings: LightSnapshotSettings,
    *,
    limit_1m: int,
    source: str,
) -> str | None:
    path = _exchange_info_cache_path(project_root, settings)
    payload = {
        "schema_version": EXCHANGE_INFO_CACHE_SCHEMA,
        "fetched_at": to_iso_z(utc_now()),
        "source": source,
        "source_url": f"{FUTURES_REST}/fapi/v1/exchangeInfo",
        "exchangeInfo": {
            "rateLimits": [
                {
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": int(limit_1m),
                }
            ]
        },
    }
    try:
        data = orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
        write_file_atomic(path, data)
        return str(path)
    except OSError as exc:
        log.warning("exchangeInfo cache write failed: %s", exc)
        return None


def _parse_iso_age_sec(value: Any) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0, int((utc_now() - dt).total_seconds()))


def _load_market_snapshot_cache(
    path: Path,
    *,
    requested_symbols: list[str],
    settings: LightSnapshotSettings,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "path": str(path),
        "doc": None,
        "age_sec": None,
        "ttl_sec": int(settings.market_snapshot_cache_ttl_sec),
        "fresh": False,
        "coverage_ratio": 0.0,
        "covered_symbols": [],
        "missing_symbols": list(requested_symbols),
        "reason": None,
    }
    try:
        raw = read_json_object(path)
    except OSError:
        base["reason"] = "market_snapshot_cache_missing"
        return base
    except (TypeError, ValueError) as exc:
        base["reason"] = f"market_snapshot_cache_invalid:{str(exc)[:120]}"
        return base
    if not isinstance(raw, dict):
        base["reason"] = "market_snapshot_cache_invalid"
        return base
    try:
        doc = FuturesLightSnapshotDocument.model_validate(raw)
    except Exception as exc:
        base["reason"] = f"market_snapshot_cache_schema_invalid:{str(exc)[:120]}"
        return base
    age_sec = _parse_iso_age_sec(doc.generated_at)
    requested = [s.upper() for s in requested_symbols]
    item_symbols = {it.symbol.upper() for it in doc.items}
    covered = [s for s in requested if s in item_symbols]
    missing = [s for s in requested if s not in item_symbols]
    coverage = (len(covered) / float(len(requested))) if requested else 1.0
    fresh = bool(
        age_sec is not None
        and age_sec <= int(settings.market_snapshot_cache_ttl_sec)
        and coverage >= float(settings.market_snapshot_cache_min_coverage_ratio)
    )
    base.update(
        {
            "doc": doc,
            "age_sec": age_sec,
            "fresh": fresh,
            "coverage_ratio": round(coverage, 4),
            "covered_symbols": covered,
            "missing_symbols": missing,
            "reason": None,
        }
    )
    if age_sec is None:
        base["reason"] = "market_snapshot_cache_missing_generated_at"
    elif age_sec > int(settings.market_snapshot_cache_ttl_sec):
        base["reason"] = "market_snapshot_cache_expired"
    elif coverage < float(settings.market_snapshot_cache_min_coverage_ratio):
        base["reason"] = "market_snapshot_cache_coverage_too_low"
    return base


def _cached_snapshot_quality(
    *,
    cache: dict[str, Any],
    requested_count: int,
    eligible_count: int,
    skipped_base: int,
    circuit: dict[str, Any],
    exchange_info_meta: dict[str, Any],
) -> dict[str, Any]:
    covered = list(cache.get("covered_symbols") or [])
    missing = list(cache.get("missing_symbols") or [])
    reason_codes = set(str(x) for x in (exchange_info_meta.get("reason_codes") or []) if x)
    reason_codes.add("market_snapshot_cache_used")
    reason_codes.add("websocket_snapshot_missing")
    if missing:
        reason_codes.add("market_snapshot_cache_partial_coverage")
    if circuit.get("rest_circuit_state") == "open":
        reason_codes.add("rest_circuit_open")
    success = len(covered)
    skipped_count = max(0, skipped_base) + len(missing)
    return {
        "snapshot_status": "degraded_cache",
        "snapshot_success_count": success,
        "snapshot_failed_count": 0,
        "snapshot_failed_symbols": [],
        "snapshot_failed_symbol_count": 0,
        "requested_count": requested_count,
        "eligible_futures_count": eligible_count,
        "skipped_count": skipped_count,
        "downstream_candidate_count": success,
        "weight_throttle_count": 0,
        "http_429_count": 0,
        "http_418_count": 0,
        "cache_fallback_count": 1,
        "exchange_info_source": exchange_info_meta.get("exchange_info_source"),
        "exchange_info_live_error": exchange_info_meta.get("exchange_info_live_error"),
        "market_snapshot_source": "cache",
        "market_snapshot_cache_path": cache.get("path"),
        "market_snapshot_cache_age_sec": cache.get("age_sec"),
        "market_snapshot_cache_ttl_sec": cache.get("ttl_sec"),
        "market_snapshot_freshness_tier": "fresh" if cache.get("fresh") else "stale_usable",
        "market_snapshot_live_attempted": False,
        "market_snapshot_coverage_ratio": cache.get("coverage_ratio"),
        "market_snapshot_missing_symbols": missing[:200],
        "market_snapshot_missing_symbol_count": len(missing),
        "rest_budget_state": "circuit_open" if circuit.get("rest_circuit_state") == "open" else "cache_first",
        "rest_budget_required_estimate": 0,
        "rest_budget_remaining_estimate": None,
        "degraded_symbol_count": len(missing),
        "skipped_symbol_count": skipped_count,
        "skipped_symbols": missing[:200],
        "websocket_snapshot_available": False,
        "websocket_snapshot_age_sec": None,
        "rest_circuit_state": circuit.get("rest_circuit_state"),
        "rest_circuit_until": circuit.get("rest_circuit_until"),
        "rest_circuit_remaining_sec": circuit.get("rest_circuit_remaining_sec"),
        "rest_circuit_reason": circuit.get("rest_circuit_reason"),
        "reason_codes": sorted(reason_codes),
    }


def _write_cached_market_snapshot(
    *,
    out: Path,
    cfg: EngineConfig,
    source_doc: FuturesLightSnapshotDocument,
    cache: dict[str, Any],
    requested_symbols: list[str],
    full_eligible_count: int,
    skipped_base: int,
    uni_age: int,
    snapshot_quality: dict[str, Any],
) -> tuple[int, int, int]:
    requested = {s.upper() for s in requested_symbols}
    items = [it for it in source_doc.items if it.symbol.upper() in requested]
    for idx, item in enumerate(items):
        dq = item.data_quality.model_copy(update={"snapshot_age_sec": cache.get("age_sec")})
        reasons = sorted(set([*item.reason_codes, "market_snapshot_cache_used"]))
        items[idx] = item.model_copy(update={"data_quality": dq, "reason_codes": reasons})
    success = sum(1 for it in items if it.primary_15m.ready)
    failed = len(items) - success
    pools: dict[str, list[str]] = {}
    for it in items:
        pools.setdefault(it.primary_pool or "unknown", []).append(it.symbol.upper())
    pools = {k: sorted(set(v)) for k, v in sorted(pools.items())}
    snapshot = FuturesLightSnapshotDocument(
        schema_version=cfg.schema_version,
        generated_at=to_iso_z(utc_now()),
        source="binance_um_futures_cache",
        universe_generated_at=source_doc.universe_generated_at,
        universe_age_sec=uni_age,
        universe_count=source_doc.universe_count,
        eligible_futures_count=full_eligible_count,
        snapshot_count=len(items),
        success_count=success,
        failed_count=failed,
        skipped_count=int(snapshot_quality.get("skipped_count") or skipped_base),
        timeframe_contract=source_doc.timeframe_contract,
        items=items,
        errors=[],
        pools=pools,
        snapshot_quality=snapshot_quality,
    )
    data = orjson.dumps(snapshot.model_dump(mode="json"), option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
    write_file_atomic(out, data)
    return len(items), success, failed


async def _resolve_request_weight_limit_1m(
    client: httpx.AsyncClient,
    project_root: Path,
    settings: LightSnapshotSettings,
) -> tuple[int | None, dict[str, Any]]:
    cache = _load_exchange_info_cache(project_root, settings)
    meta: dict[str, Any] = {
        "exchange_info_policy": "cache_first" if settings.exchange_info_cache_first_enabled else "live_first",
        "exchange_info_source": None,
        "exchange_info_cache_path": cache["path"],
        "exchange_info_cache_age_sec": cache["age_sec"],
        "exchange_info_cache_ttl_sec": cache["ttl_sec"],
        "exchange_info_cache_fresh": cache["fresh"],
        "exchange_info_live_error": None,
        "exchange_info_live_status_code": None,
        "exchange_info_retry_after_sec": None,
        "exchange_info_live_attempted": False,
        "exchange_info_fallback_used": False,
        "exchange_info_cache_reason": cache["reason"],
        "light_snapshot_status": "ok",
        "reason_codes": [],
    }
    circuit = read_rest_circuit(project_root)
    meta.update(
        {
            "rest_circuit_state": circuit.get("rest_circuit_state"),
            "rest_circuit_until": circuit.get("rest_circuit_until"),
            "rest_circuit_remaining_sec": circuit.get("rest_circuit_remaining_sec"),
            "rest_circuit_reason": circuit.get("rest_circuit_reason"),
        }
    )
    if settings.exchange_info_cache_first_enabled and cache["fresh"] and cache["limit"] is not None:
        meta.update(
            {
                "exchange_info_source": "cache",
                "exchange_info_fallback_used": False,
                "light_snapshot_status": "degraded_cache",
            }
        )
        meta["reason_codes"].append("exchange_info_cache_used")
        if circuit.get("rest_circuit_state") == "open":
            meta["reason_codes"].append("rest_circuit_open")
        return int(cache["limit"]), meta
    if circuit.get("rest_circuit_state") == "open":
        if cache["fresh"] and cache["limit"] is not None:
            meta.update(
                {
                    "exchange_info_source": "cache",
                    "exchange_info_fallback_used": True,
                    "light_snapshot_status": "degraded_cache",
                }
            )
            meta["reason_codes"].append("exchange_info_cache_used")
            meta["reason_codes"].append("rest_circuit_open")
            return int(cache["limit"]), meta
        if cache["reason"]:
            meta["reason_codes"].append(str(cache["reason"]).split(":", 1)[0])
        meta["reason_codes"].append("rest_circuit_open")
        fail_meta = _fail_closed_meta(meta, cache["reason"])
        raise ExchangeInfoFailClosed("REST circuit open and no fresh exchangeInfo cache", meta=fail_meta, cause=BinanceCircuit418("global REST circuit open"))
    try:
        meta["exchange_info_live_attempted"] = True
        limit_1m = await fetch_request_weight_limit_1m_async(client)
    except httpx.HTTPStatusError as exc:
        status = _status_code_from_exc(exc)
        retry_after = _retry_after_from_exc(exc)
        if status in {429, 418}:
            write_rest_circuit_open(
                project_root,
                status_code=int(status),
                endpoint=f"{FUTURES_REST}/fapi/v1/exchangeInfo",
                source_stage="step1_5_exchangeInfo",
                retry_after_sec=retry_after,
                reason=f"http_{status}",
            )
        meta["exchange_info_live_error"] = f"http_{status}" if status else str(exc)[:240]
        meta["exchange_info_live_status_code"] = status
        meta["exchange_info_retry_after_sec"] = retry_after
        if status == 429:
            meta["reason_codes"].append("exchange_info_live_429")
        elif status == 418:
            meta["reason_codes"].append("exchange_info_live_418")
        if (
            status in {429, 418}
            and settings.exchange_info_allow_cache_on_429_418
            and cache["fresh"]
            and cache["limit"] is not None
        ):
            meta.update(
                {
                    "exchange_info_source": "cache",
                    "exchange_info_fallback_used": True,
                    "light_snapshot_status": "degraded_cache",
                }
            )
            meta["reason_codes"].append("exchange_info_cache_used")
            return int(cache["limit"]), meta
        if (
            status in {429, 418}
            and settings.exchange_info_allow_cache_on_429_418
            and not settings.exchange_info_fail_if_cache_missing
        ):
            if cache["reason"]:
                meta["reason_codes"].append(str(cache["reason"]).split(":", 1)[0])
            meta.update(
                {
                    "exchange_info_source": "fallback_default",
                    "exchange_info_fallback_used": True,
                    "light_snapshot_status": "degraded_cache",
                }
            )
            return DEFAULT_WEIGHT_LIMIT_1M, meta
        if cache["reason"]:
            meta["reason_codes"].append(str(cache["reason"]).split(":", 1)[0])
        fail_meta = _fail_closed_meta(meta, cache["reason"])
        raise ExchangeInfoFailClosed(str(exc), meta=fail_meta, cause=exc) from exc
    except httpx.HTTPError as exc:
        meta["exchange_info_live_error"] = str(exc)[:240]
        meta["exchange_info_live_status_code"] = _status_code_from_exc(exc)
        meta["exchange_info_retry_after_sec"] = _retry_after_from_exc(exc)
        if cache["fresh"] and cache["limit"] is not None:
            meta.update(
                {
                    "exchange_info_source": "cache",
                    "exchange_info_fallback_used": True,
                    "light_snapshot_status": "degraded_cache",
                }
            )
            meta["reason_codes"].append("exchange_info_cache_used")
            return int(cache["limit"]), meta
        if not settings.exchange_info_fail_if_cache_missing:
            if cache["reason"]:
                meta["reason_codes"].append(str(cache["reason"]).split(":", 1)[0])
            meta.update(
                {
                    "exchange_info_source": "fallback_default",
                    "exchange_info_fallback_used": True,
                    "light_snapshot_status": "degraded_cache",
                }
            )
            return DEFAULT_WEIGHT_LIMIT_1M, meta
        if cache["reason"]:
            meta["reason_codes"].append(str(cache["reason"]).split(":", 1)[0])
        fail_meta = _fail_closed_meta(meta, cache["reason"])
        raise ExchangeInfoFailClosed(str(exc), meta=fail_meta, cause=exc) from exc
    if limit_1m is not None and limit_1m > 0:
        cache_path = _write_exchange_info_cache(
            project_root,
            settings,
            limit_1m=int(limit_1m),
            source="live",
        )
        meta.update(
            {
                "exchange_info_source": "live",
                "exchange_info_cache_path": cache_path or cache["path"],
                "exchange_info_cache_age_sec": 0,
                "exchange_info_cache_reason": None,
            }
        )
        return int(limit_1m), meta
    meta["exchange_info_source"] = "fallback_default"
    meta["exchange_info_live_error"] = "request_weight_missing"
    return limit_1m, meta


async def run_fetch_light_snapshot_async(
    *,
    project_root: Path | None = None,
    limit: int | None = None,
    symbols_filter: list[str] | None = None,
    max_concurrency: int | None = None,
    output_path: Path | None = None,
    settings: LightSnapshotSettings | None = None,
    perf_fetch_mode: str = "async",
) -> int:
    cfg = EngineConfig.load(project_root)
    ls = settings or load_light_snapshot_settings()
    out = output_path or cfg.futures_light_snapshot_path
    root = cfg.project_root

    t0 = time.perf_counter()
    try:
        univ_raw = read_json_object(cfg.candidate_universe_path)
        doc = CandidateUniverseDocument.model_validate(univ_raw)
    except (OSError, TypeError, ValueError) as exc:
        log.error("cannot load universe: %s", exc)
        return EXIT_CONFIG

    full_eligible = futures_symbols_for_step_1_5(doc)
    if doc.counts.futures_count != len(full_eligible):
        log.warning(
            "Universe counts.futures_count=%s but step15 symbol list length=%s",
            doc.counts.futures_count,
            len(full_eligible),
        )
    pairs = _pair_index(doc)

    symbols: list[str] = list(full_eligible)
    if symbols_filter:
        allow = {s.strip().upper() for s in symbols_filter if s.strip()}
        symbols = [s for s in symbols if s in allow]
    if limit is not None and limit > 0:
        symbols = symbols[:limit]
    symbols = [s for s in symbols if s in pairs]

    skipped = len(full_eligible) - len(symbols)
    workers = max_concurrency if max_concurrency is not None else ls.max_concurrency
    workers = max(1, min(workers, 64))

    gen_dt = _parse_univ_time(doc.generated_at)
    uni_age = int((utc_now() - gen_dt).total_seconds())
    tc = _load_timeframe_contract()

    limit_conn = workers
    timeout = httpx.Timeout(ls.request_timeout_sec)
    limits = httpx.Limits(max_connections=limit_conn, max_keepalive_connections=limit_conn)

    errors_list: list[SnapshotErrorEntry] = []
    items: list[LightSnapshotItem] = []
    sym_klines: dict[str, tuple[list[KlineBar], list[KlineBar], list[KlineBar]]] = {}
    sym_fail: dict[str, tuple[LightSnapshotItem, SnapshotErrorEntry]] = {}
    count_429_final = 0
    retry_total_final = 0
    http_418_final = 0
    hard_throttle_final = 0
    avg_request_ms: float | None = None
    p95_request_ms: float | None = None
    request_count = 0
    rest_endpoint_counts: dict[str, int] = {}
    rest_status_code_counts: dict[str, int] = {}
    rate_limit_source = "exchangeInfo"
    exchange_info_meta: dict[str, Any] = {
        "exchange_info_policy": "cache_first" if ls.exchange_info_cache_first_enabled else "live_first",
        "exchange_info_source": None,
        "exchange_info_cache_path": str(_exchange_info_cache_path(root, ls)),
        "exchange_info_cache_age_sec": None,
        "exchange_info_cache_ttl_sec": int(ls.exchange_info_cache_ttl_sec),
        "exchange_info_cache_fresh": False,
        "exchange_info_live_error": None,
        "exchange_info_live_status_code": None,
        "exchange_info_retry_after_sec": None,
        "exchange_info_live_attempted": False,
        "exchange_info_fallback_used": False,
        "exchange_info_cache_reason": None,
        "light_snapshot_status": "ok",
        "rest_circuit_state": read_rest_circuit(root).get("rest_circuit_state"),
        "rest_circuit_until": read_rest_circuit(root).get("rest_circuit_until"),
        "rest_circuit_remaining_sec": read_rest_circuit(root).get("rest_circuit_remaining_sec"),
        "rest_circuit_reason": read_rest_circuit(root).get("rest_circuit_reason"),
        "reason_codes": [],
    }
    circuit = read_rest_circuit(root)
    planned_live_weight = ESTIMATED_TICKER_24H_WEIGHT + len(symbols) * ESTIMATED_KLINE_WEIGHT_PER_SYMBOL
    cache_candidates = [out]
    if cfg.futures_light_snapshot_path not in cache_candidates:
        cache_candidates.append(cfg.futures_light_snapshot_path)
    market_cache: dict[str, Any] | None = None
    for cache_path in cache_candidates:
        candidate = _load_market_snapshot_cache(cache_path, requested_symbols=symbols, settings=ls)
        if candidate.get("fresh"):
            market_cache = candidate
            break
        if market_cache is None:
            market_cache = candidate
    if ls.market_snapshot_cache_first_enabled and market_cache and market_cache.get("fresh"):
        quality = _cached_snapshot_quality(
            cache=market_cache,
            requested_count=len(symbols),
            eligible_count=len(full_eligible),
            skipped_base=max(0, skipped),
            circuit=circuit,
            exchange_info_meta={
                **exchange_info_meta,
                "exchange_info_source": "cache" if exchange_info_meta.get("exchange_info_cache_fresh") else exchange_info_meta.get("exchange_info_source"),
                "reason_codes": [*list(exchange_info_meta.get("reason_codes") or []), "market_snapshot_cache_first"],
            },
        )
        try:
            snapshot_count, success_count, failed_count = _write_cached_market_snapshot(
                out=out,
                cfg=cfg,
                source_doc=market_cache["doc"],
                cache=market_cache,
                requested_symbols=symbols,
                full_eligible_count=len(full_eligible),
                skipped_base=max(0, skipped),
                uni_age=uni_age,
                snapshot_quality=quality,
            )
        except OSError as exc:
            log.error("write cached snapshot failed: %s", exc)
            return EXIT_CONFIG
        append_perf_log(
            root,
            ls.async_perf_log_path,
            {
                "ts": to_iso_z(utc_now()),
                "command": "fetch-futures-light-snapshot",
                "fetch_mode": perf_fetch_mode,
                "duration_ms": int(round((time.perf_counter() - t0) * 1000)),
                "worker_count": workers,
                "max_concurrency": workers,
                "eligible_futures_count": len(full_eligible),
                "requested_count": len(symbols),
                "snapshot_count": snapshot_count,
                "success_count": success_count,
                "failed_count": failed_count,
                "skipped_count": int(quality.get("skipped_count") or max(0, skipped)),
                "retry_count": 0,
                "rate_limited_count": 0,
                "http_429_count": 0,
                "weight_hard_throttle_count": 0,
                "http_418_count": 0,
                "snapshot_status": "degraded_cache",
                "avg_request_ms": None,
                "p95_request_ms": None,
                "request_count": 0,
                "rate_limit_source": "market_snapshot_cache",
                **exchange_info_meta,
                **quality,
                "ok": True,
            },
        )
        return EXIT_SUCCESS
    if (
        ls.rest_budget_preflight_enabled
        and circuit.get("rest_circuit_state") == "open"
        and ls.market_snapshot_fail_closed_on_circuit_open
    ):
        reason_codes = set(str(x) for x in (exchange_info_meta.get("reason_codes") or []) if x)
        reason_codes.add("rest_circuit_open")
        if market_cache and market_cache.get("reason"):
            reason_codes.add(str(market_cache.get("reason")).split(":", 1)[0])
        append_perf_log(
            root,
            ls.async_perf_log_path,
            {
                "ts": to_iso_z(utc_now()),
                "command": "fetch-futures-light-snapshot",
                "fetch_mode": perf_fetch_mode,
                "duration_ms": int(round((time.perf_counter() - t0) * 1000)),
                "worker_count": workers,
                "max_concurrency": workers,
                "eligible_futures_count": len(full_eligible),
                "requested_count": len(symbols),
                "snapshot_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "skipped_count": max(0, skipped),
                "retry_count": 0,
                "rate_limited_count": 0,
                "http_429_count": 0,
                "weight_hard_throttle_count": 0,
                "http_418_count": 0,
                "snapshot_status": "fail_closed",
                "avg_request_ms": None,
                "p95_request_ms": None,
                "request_count": 0,
                "rate_limit_source": "rest_budget_preflight",
                **exchange_info_meta,
                "market_snapshot_source": "fail_closed",
                "market_snapshot_cache_path": market_cache.get("path") if market_cache else str(out),
                "market_snapshot_cache_age_sec": market_cache.get("age_sec") if market_cache else None,
                "market_snapshot_cache_ttl_sec": int(ls.market_snapshot_cache_ttl_sec),
                "market_snapshot_freshness_tier": "stale_blocked",
                "market_snapshot_live_attempted": False,
                "market_snapshot_coverage_ratio": market_cache.get("coverage_ratio") if market_cache else 0.0,
                "rest_budget_state": "circuit_open",
                "rest_budget_required_estimate": planned_live_weight,
                "rest_budget_remaining_estimate": 0,
                "degraded_symbol_count": len(symbols),
                "skipped_symbol_count": len(symbols) + max(0, skipped),
                "skipped_symbols": symbols[:200],
                "websocket_snapshot_available": False,
                "rest_circuit_state": circuit.get("rest_circuit_state"),
                "rest_circuit_until": circuit.get("rest_circuit_until"),
                "rest_circuit_remaining_sec": circuit.get("rest_circuit_remaining_sec"),
                "rest_circuit_reason": circuit.get("rest_circuit_reason"),
                "reason_codes": sorted(reason_codes),
                "ok": False,
                "error": "REST circuit open and no fresh market snapshot cache",
            },
        )
        return EXIT_BINANCE

    try:
        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            wlim, exchange_info_meta = await _resolve_request_weight_limit_1m(client, root, ls)
            if wlim is None or wlim <= 0:
                wlim = DEFAULT_WEIGHT_LIMIT_1M
                rate_limit_source = "fallback_default"
                log.warning("exchangeInfo REQUEST_WEIGHT 1m missing; fallback %s", wlim)
            else:
                rate_limit_source = (
                    "exchangeInfo_cache"
                    if exchange_info_meta.get("exchange_info_source") == "cache"
                    else "exchangeInfo_live"
                )

            limiter = AsyncIpWeightLimiter(
                weight_limit_1m=wlim,
                soft_limit_ratio=ls.async_soft_limit_ratio,
                hard_limit_ratio=ls.async_hard_limit_ratio,
                backoff_base_sec=ls.async_backoff_base_sec,
                backoff_max_sec=ls.async_backoff_max_sec,
                circuit_break_on_418=ls.async_circuit_break_on_418,
                project_root=root,
                source_stage="step1_5_light_snapshot",
            )

            ticker_url = f"{FUTURES_REST}/fapi/v1/ticker/24hr"
            tr = await limiter.get(client, ticker_url)
            data = tr.json()
            if not isinstance(data, list):
                raise TypeError("ticker/24hr response must be a list")
            ticker_rows = [x for x in data if isinstance(x, dict)]
            ticker_map = ticker_by_symbol_map(ticker_rows)

            if not symbols:
                log.warning("no symbols to scan")
            else:
                sem = asyncio.Semaphore(workers)
                tasks = [
                    asyncio.create_task(
                        _one_symbol_task(
                            sem,
                            client,
                            limiter,
                            sym,
                            pairs[sym],
                            ls,
                        )
                    )
                    for sym in symbols
                    if sym in pairs
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, BinanceCircuit418):
                        raise res
                    if isinstance(res, Exception):
                        log.error("async task exception: %s", res)
                        continue
                    if not (isinstance(res, tuple) and len(res) == 3):
                        log.error("async task bad result: %s", res)
                        continue
                    sym, bundle, fail = res
                    if fail is not None:
                        sym_fail[sym] = fail
                    elif bundle is not None:
                        sym_klines[sym] = bundle

            count_429_final = limiter.count_429
            retry_total_final = limiter.retry_count
            http_418_final = limiter.count_418
            hard_throttle_final = limiter.count_hard_throttle
            avg_request_ms, p95_request_ms, request_count = _latency_summary(limiter.request_latencies_ms)
            rest_endpoint_counts = dict(limiter.endpoint_counts)
            rest_status_code_counts = dict(limiter.status_code_counts)

            snapshot_ref_ms = int(time.time() * 1000)
            for sym in symbols:
                if sym not in pairs:
                    continue
                if sym in sym_fail:
                    fit, fer = sym_fail[sym]
                    items.append(fit)
                    errors_list.append(fer)
                elif sym in sym_klines:
                    k1, k15, k1h = sym_klines[sym]
                    t_row = ticker_map.get(sym.upper())
                    b_item, err2 = await asyncio.to_thread(
                        _build_item_for_symbol,
                        sym,
                        pairs[sym],
                        k1,
                        k15,
                        k1h,
                        t_row,
                        ls,
                        snapshot_ref_ms,
                    )
                    items.append(b_item)
                    if err2:
                        errors_list.append(err2)
    except BinanceCircuit418 as exc:
        log.error("Binance circuit 418: %s", exc)
        if "limiter" in locals():
            count_429_final = limiter.count_429
            retry_total_final = limiter.retry_count
            http_418_final = limiter.count_418
            hard_throttle_final = limiter.count_hard_throttle
            avg_request_ms, p95_request_ms, request_count = _latency_summary(limiter.request_latencies_ms)
            rest_endpoint_counts = dict(limiter.endpoint_counts)
            rest_status_code_counts = dict(limiter.status_code_counts)
        circuit = read_rest_circuit(root)
        circuit_reason = circuit.get("rest_circuit_reason") or "rest_circuit_open"
        reason_set = set(str(x) for x in (exchange_info_meta.get("reason_codes") or []) if x)
        reason_set.add(str(circuit_reason))
        if http_418_final > 0:
            reason_set.add("light_snapshot_http_418")
        append_perf_log(
            root,
            ls.async_perf_log_path,
            {
                "ts": to_iso_z(utc_now()),
                "command": "fetch-futures-light-snapshot",
                "fetch_mode": perf_fetch_mode,
                "duration_ms": int(round((time.perf_counter() - t0) * 1000)),
                "worker_count": workers,
                "max_concurrency": workers,
                "eligible_futures_count": len(full_eligible),
                "requested_count": len(symbols),
                "snapshot_count": len(items),
                "success_count": sum(1 for it in items if it.primary_15m.ready),
                "failed_count": len(items) - sum(1 for it in items if it.primary_15m.ready),
                "skipped_count": max(0, skipped),
                "retry_count": retry_total_final,
                "rate_limited_count": count_429_final,
                "http_429_count": count_429_final,
                "weight_hard_throttle_count": hard_throttle_final,
                "http_418_count": http_418_final,
                "snapshot_status": "fail_closed",
                "avg_request_ms": avg_request_ms,
                "p95_request_ms": p95_request_ms,
                "request_count": request_count,
                "rest_request_count": request_count,
                "rest_endpoint_counts": rest_endpoint_counts,
                "rest_status_code_counts": rest_status_code_counts,
                "rate_limit_source": rate_limit_source,
                **exchange_info_meta,
                "rest_circuit_state": circuit.get("rest_circuit_state"),
                "rest_circuit_until": circuit.get("rest_circuit_until"),
                "rest_circuit_remaining_sec": circuit.get("rest_circuit_remaining_sec"),
                "rest_circuit_reason": circuit_reason,
                "reason_codes": sorted(reason_set),
                "ok": False,
                "error": str(exc)[:500],
            },
        )
        return EXIT_BINANCE
    except ExchangeInfoFailClosed as exc:
        log.error("exchangeInfo fail-closed: %s", exc)
        exchange_info_meta = dict(exc.meta)
        append_perf_log(
            root,
            ls.async_perf_log_path,
            {
                "ts": to_iso_z(utc_now()),
                "command": "fetch-futures-light-snapshot",
                "fetch_mode": perf_fetch_mode,
                "duration_ms": int(round((time.perf_counter() - t0) * 1000)),
                "worker_count": workers,
                "max_concurrency": workers,
                "eligible_futures_count": len(full_eligible),
                "requested_count": len(symbols),
                "snapshot_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "skipped_count": max(0, skipped),
                "retry_count": retry_total_final,
                "rate_limited_count": count_429_final,
                "http_429_count": count_429_final,
                "weight_hard_throttle_count": hard_throttle_final,
                "http_418_count": http_418_final,
                "snapshot_status": "fail_closed",
                "avg_request_ms": avg_request_ms,
                "p95_request_ms": p95_request_ms,
                "request_count": request_count,
                "rest_request_count": request_count,
                "rest_endpoint_counts": rest_endpoint_counts,
                "rest_status_code_counts": rest_status_code_counts,
                "rate_limit_source": rate_limit_source,
                **exchange_info_meta,
                "ok": False,
                "error": str(exc)[:500],
            },
        )
        return EXIT_BINANCE
    except (httpx.HTTPError, OSError, TypeError, ValueError) as exc:
        log.error("async fetch failed: %s", exc)
        append_perf_log(
            root,
            ls.async_perf_log_path,
            {
                "ts": to_iso_z(utc_now()),
                "command": "fetch-futures-light-snapshot",
                "fetch_mode": perf_fetch_mode,
                "duration_ms": int(round((time.perf_counter() - t0) * 1000)),
                "worker_count": workers,
                "max_concurrency": workers,
                "eligible_futures_count": len(full_eligible),
                "requested_count": len(symbols),
                "snapshot_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "skipped_count": max(0, skipped),
                "retry_count": retry_total_final,
                "rate_limited_count": count_429_final,
                "http_429_count": count_429_final,
                "weight_hard_throttle_count": hard_throttle_final,
                "http_418_count": http_418_final,
                "avg_request_ms": avg_request_ms,
                "p95_request_ms": p95_request_ms,
                "request_count": request_count,
                "rest_request_count": request_count,
                "rest_endpoint_counts": rest_endpoint_counts,
                "rest_status_code_counts": rest_status_code_counts,
                "rate_limit_source": rate_limit_source,
                **exchange_info_meta,
                "ok": False,
                "error": str(exc)[:500],
            },
        )
        return EXIT_BINANCE

    success = sum(1 for it in items if it.primary_15m.ready)
    failed = len(items) - success
    failed_symbols = sorted(
        {
            str(err.symbol).upper()
            for err in errors_list
            if getattr(err, "symbol", None)
        }
    )
    quality_reason_codes = list(exchange_info_meta.get("reason_codes") or [])
    if failed > 0:
        quality_reason_codes.append("light_snapshot_partial_failed_symbols")
    if count_429_final > 0:
        quality_reason_codes.append("light_snapshot_http_429")
    if hard_throttle_final > 0:
        quality_reason_codes.append("light_snapshot_weight_hard_throttle")
    if http_418_final > 0:
        quality_reason_codes.append("light_snapshot_http_418")
    quality_reason_codes = sorted(set(str(x) for x in quality_reason_codes if x))
    if exchange_info_meta.get("light_snapshot_status") == "degraded_cache":
        snapshot_status = "degraded_cache"
    elif failed > 0:
        snapshot_status = "partial"
    else:
        snapshot_status = "ok"
    snapshot_quality = {
        "snapshot_status": snapshot_status,
        "snapshot_success_count": success,
        "snapshot_failed_count": failed,
        "snapshot_failed_symbols": failed_symbols[:200],
        "snapshot_failed_symbol_count": len(failed_symbols),
        "requested_count": len(symbols),
        "eligible_futures_count": len(full_eligible),
        "skipped_count": max(0, skipped),
        "downstream_candidate_count": success,
        "weight_throttle_count": hard_throttle_final,
        "http_429_count": count_429_final,
        "http_418_count": http_418_final,
        "cache_fallback_count": 1 if exchange_info_meta.get("exchange_info_fallback_used") else 0,
        "exchange_info_source": exchange_info_meta.get("exchange_info_source"),
        "exchange_info_live_error": exchange_info_meta.get("exchange_info_live_error"),
        "market_snapshot_source": "live",
        "market_snapshot_cache_path": market_cache.get("path") if market_cache else str(out),
        "market_snapshot_cache_age_sec": market_cache.get("age_sec") if market_cache else None,
        "market_snapshot_cache_ttl_sec": int(ls.market_snapshot_cache_ttl_sec),
        "market_snapshot_freshness_tier": "fresh",
        "market_snapshot_live_attempted": True,
        "market_snapshot_coverage_ratio": 1.0,
        "market_snapshot_missing_symbols": [],
        "market_snapshot_missing_symbol_count": 0,
        "rest_budget_state": "ok",
        "rest_budget_required_estimate": planned_live_weight,
        "rest_budget_remaining_estimate": None,
        "degraded_symbol_count": 0,
        "skipped_symbol_count": max(0, skipped),
        "skipped_symbols": [],
        "websocket_snapshot_available": False,
        "websocket_snapshot_age_sec": None,
        "rest_circuit_state": read_rest_circuit(root).get("rest_circuit_state"),
        "rest_circuit_until": read_rest_circuit(root).get("rest_circuit_until"),
        "rest_circuit_remaining_sec": read_rest_circuit(root).get("rest_circuit_remaining_sec"),
        "rest_circuit_reason": read_rest_circuit(root).get("rest_circuit_reason"),
        "reason_codes": quality_reason_codes,
    }
    if not snapshot_quality["websocket_snapshot_available"]:
        quality_reason_codes = sorted(set([*quality_reason_codes, "websocket_snapshot_missing"]))
        snapshot_quality["reason_codes"] = quality_reason_codes
    pools: dict[str, list[str]] = {}
    for it in items:
        pools.setdefault(it.primary_pool or "unknown", []).append(it.symbol.upper())
    pools = {k: sorted(set(v)) for k, v in sorted(pools.items())}

    snapshot = FuturesLightSnapshotDocument(
        schema_version=cfg.schema_version,
        generated_at=to_iso_z(utc_now()),
        source="binance_um_futures",
        universe_generated_at=doc.generated_at,
        universe_age_sec=uni_age,
        universe_count=doc.count,
        eligible_futures_count=len(full_eligible),
        snapshot_count=len(items),
        success_count=success,
        failed_count=failed,
        skipped_count=max(0, skipped),
        timeframe_contract=tc,
        items=items,
        errors=errors_list,
        pools=pools,
        snapshot_quality=snapshot_quality,
    )

    payload = snapshot.model_dump(mode="json")
    try:
        data = orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
        write_file_atomic(out, data)
    except OSError as exc:
        log.error("write failed: %s", exc)
        return EXIT_CONFIG

    elapsed = round(time.perf_counter() - t0, 3)
    log.info(
        "async wrote %s snapshot_count=%s success=%s failed=%s skipped=%s elapsed=%ss",
        out,
        len(items),
        success,
        failed,
        skipped,
        elapsed,
    )
    append_perf_log(
        root,
        ls.async_perf_log_path,
        {
            "ts": to_iso_z(utc_now()),
            "command": "fetch-futures-light-snapshot",
            "fetch_mode": perf_fetch_mode,
            "duration_ms": int(round(elapsed * 1000)),
            "worker_count": workers,
            "max_concurrency": workers,
            "eligible_futures_count": len(full_eligible),
            "requested_count": len(symbols),
            "snapshot_count": len(items),
            "success_count": success,
            "failed_count": failed,
            "skipped_count": max(0, skipped),
            "retry_count": retry_total_final,
            "rate_limited_count": count_429_final,
            "http_429_count": count_429_final,
            "weight_hard_throttle_count": hard_throttle_final,
            "http_418_count": http_418_final,
            "snapshot_status": snapshot_status,
            "snapshot_failed_symbols": failed_symbols[:200],
            "snapshot_failed_symbol_count": len(failed_symbols),
            "downstream_candidate_count": success,
            "avg_request_ms": avg_request_ms,
            "p95_request_ms": p95_request_ms,
            "request_count": request_count,
            "rest_request_count": request_count,
            "rest_endpoint_counts": rest_endpoint_counts,
            "rest_status_code_counts": rest_status_code_counts,
            "rate_limit_source": rate_limit_source,
            **exchange_info_meta,
            "market_snapshot_source": snapshot_quality.get("market_snapshot_source"),
            "market_snapshot_cache_path": snapshot_quality.get("market_snapshot_cache_path"),
            "market_snapshot_cache_age_sec": snapshot_quality.get("market_snapshot_cache_age_sec"),
            "market_snapshot_cache_ttl_sec": snapshot_quality.get("market_snapshot_cache_ttl_sec"),
            "market_snapshot_freshness_tier": snapshot_quality.get("market_snapshot_freshness_tier"),
            "market_snapshot_live_attempted": snapshot_quality.get("market_snapshot_live_attempted"),
            "market_snapshot_coverage_ratio": snapshot_quality.get("market_snapshot_coverage_ratio"),
            "rest_budget_state": snapshot_quality.get("rest_budget_state"),
            "rest_budget_required_estimate": snapshot_quality.get("rest_budget_required_estimate"),
            "rest_budget_remaining_estimate": snapshot_quality.get("rest_budget_remaining_estimate"),
            "degraded_symbol_count": snapshot_quality.get("degraded_symbol_count"),
            "skipped_symbol_count": snapshot_quality.get("skipped_symbol_count"),
            "skipped_symbols": snapshot_quality.get("skipped_symbols"),
            "websocket_snapshot_available": snapshot_quality.get("websocket_snapshot_available"),
            "websocket_snapshot_age_sec": snapshot_quality.get("websocket_snapshot_age_sec"),
            "ok": True,
        },
    )
    return EXIT_SUCCESS


def run_fetch_light_snapshot_async_safe(**kwargs: Any) -> int:
    try:
        return asyncio.run(run_fetch_light_snapshot_async(**kwargs))
    except Exception as exc:
        log.exception("async light snapshot failed: %s", exc)
        return EXIT_INTERNAL


def dry_run_plan_dict(
    *,
    project_root: Path | None,
    limit: int | None,
    symbols_filter: list[str] | None,
    max_concurrency: int | None,
    fetch_mode: str,
) -> dict[str, Any]:
    cfg = EngineConfig.load(project_root)
    ls = load_light_snapshot_settings()
    try:
        univ_raw = read_json_object(cfg.candidate_universe_path)
        doc = CandidateUniverseDocument.model_validate(univ_raw)
    except (OSError, TypeError, ValueError) as exc:
        return {"dry_run": True, "error": f"cannot_load_universe: {exc}"}

    full_eligible = futures_symbols_for_step_1_5(doc)
    pairs = _pair_index(doc)
    symbols: list[str] = list(full_eligible)
    if symbols_filter:
        allow = {s.strip().upper() for s in symbols_filter if s.strip()}
        symbols = [s for s in symbols if s in allow]
    if limit is not None and limit > 0:
        symbols = symbols[:limit]
    symbols = [s for s in symbols if s in pairs]

    n = len(symbols)
    mc = max_concurrency if max_concurrency is not None else ls.max_concurrency
    mc = max(1, min(mc, 64))
    planned_exchange_info_requests = 1
    estimated_weight_rough = (
        ESTIMATED_TICKER_24H_WEIGHT
        + planned_exchange_info_requests
        + n * ESTIMATED_KLINE_WEIGHT_PER_SYMBOL
    )
    return {
        "schema_version": cfg.schema_version,
        "command": "fetch-futures-light-snapshot",
        "dry_run": True,
        "fetch_mode": fetch_mode,
        "universe_count": doc.count,
        "eligible_futures_count": len(full_eligible),
        "requested_count": n,
        "worker_count": mc,
        "planned_kline_requests": n * 3,
        "planned_ticker_requests": 1,
        "planned_exchangeInfo_requests": planned_exchange_info_requests,
        "estimated_kline_weight_per_symbol": ESTIMATED_KLINE_WEIGHT_PER_SYMBOL,
        "estimated_ticker_24h_weight": ESTIMATED_TICKER_24H_WEIGHT,
        "estimated_weight": estimated_weight_rough,
        "estimated_weight_rough": estimated_weight_rough,
        "rate_limit_source": "dry_run_rough_estimate",
        "will_write_snapshot": False,
    }


def dry_run_plan_text(
    *,
    project_root: Path | None,
    limit: int | None,
    symbols_filter: list[str] | None,
    max_concurrency: int | None,
    fetch_mode: str,
) -> str:
    d = dry_run_plan_dict(
        project_root=project_root,
        limit=limit,
        symbols_filter=symbols_filter,
        max_concurrency=max_concurrency,
        fetch_mode=fetch_mode,
    )
    err = d.get("error")
    if err:
        return f"dry_run_plan_error={err}\n"
    n = int(d["requested_count"])
    mc = int(d["worker_count"])
    full_eligible = int(d["eligible_futures_count"])
    k_req = n * 3
    ticker_req = 1
    exch_req = int(d.get("planned_exchangeInfo_requests", 1))
    est_w = int(d.get("estimated_weight_rough", 0))
    lines = [
        f"eligible_symbols={n}",
        f"full_eligible_universe={full_eligible}",
        f"planned_kline_requests={k_req}",
        f"planned_ticker_requests={ticker_req}",
        f"planned_exchangeInfo_requests={exch_req}",
        f"estimated_weight_rough={est_w} (ticker~{ESTIMATED_TICKER_24H_WEIGHT} + klines~{n}*{ESTIMATED_KLINE_WEIGHT_PER_SYMBOL})",
        f"max_concurrency={mc}",
        f"fetch_mode={fetch_mode}",
        "ticker_request_once=true",
        f"output_schema=STEP1.5_futures_light_snapshot",
    ]
    return "\n".join(lines) + "\n"
