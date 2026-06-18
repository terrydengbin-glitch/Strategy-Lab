"""HTTP client for Binance USDS-M Futures public REST (context providers)."""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://fapi.binance.com"
MAX_RETRIES = 4
RETRY_BACKOFF_SEC = 0.35


class BinanceFuturesContextClient:
    """Thin synchronous GET with simple 429 / 5xx retry."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout_sec: float = 15.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout_sec)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> BinanceFuturesContextClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self._base}{path}"
        params = params or {}
        last_exc: BaseException | None = None
        for attempt in range(MAX_RETRIES):
            try:
                r = self._client.get(url, params=params)
                if r.status_code == 429 or r.status_code >= 500:
                    sleep = RETRY_BACKOFF_SEC * (attempt + 1)
                    log.warning("binance_context retry status=%s sleep=%.2fs", r.status_code, sleep)
                    time.sleep(sleep)
                    continue
                r.raise_for_status()
                return r.json()
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                last_exc = exc
                sleep = RETRY_BACKOFF_SEC * (attempt + 1)
                log.warning("binance_context http error %s sleep=%.2fs", exc, sleep)
                time.sleep(sleep)
        raise RuntimeError(f"binance_context failed after retries: {last_exc}") from last_exc


def fetch_premium_index_all(client: BinanceFuturesContextClient) -> list[dict[str, Any]]:
    data = client.get_json("/fapi/v1/premiumIndex")
    if not isinstance(data, list):
        raise RuntimeError("premiumIndex expected list")
    return [x for x in data if isinstance(x, dict)]


def premium_index_by_symbol(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        sym = row.get("symbol")
        if isinstance(sym, str) and sym:
            out[sym.upper().strip()] = row
    return out
