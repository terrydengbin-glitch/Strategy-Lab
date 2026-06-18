"""
Fetch Binance Spot USDT pairs that are TRADING, then rank by 24h quote volume
and save top 50 to DATA/universe/TOP50VOL.json.

Uses public REST only: GET /api/v3/exchangeInfo and GET /api/v3/ticker/24hr

Timeouts from config.yaml fetch_top50 (app_config); env FETCH_TOP50_* still overrides.

HTTPS_PROXY / HTTP_PROXY respected by httpx trust_env when set.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

_REPO = Path(__file__).resolve().parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
from backend.app.data_layout import ensure_data_directories
from backend.app.project_root import get_app_root
from backend.app.services.app_config import (
    get_fetch_top50_connect_timeout_sec,
    get_fetch_top50_read_timeout_sec,
)

BASE_URL = "https://api.binance.com"
EXCHANGE_INFO = f"{BASE_URL}/api/v3/exchangeInfo"
TICKER_24HR = f"{BASE_URL}/api/v3/ticker/24hr"

ROOT = get_app_root()
DATA_DIR = ROOT / "DATA" / "universe"
OUT_FILE = DATA_DIR / "TOP50VOL.json"
LOG_DIR = ROOT / "DATA" / "logs"
TOP50_LOG = LOG_DIR / "fetch_binance_top50.log"

USER_AGENT = "abnormal-fluctuation-top50vol/1.0"


def _timeouts() -> httpx.Timeout:
    connect_s = get_fetch_top50_connect_timeout_sec()
    read_s = get_fetch_top50_read_timeout_sec()
    return httpx.Timeout(read_s, connect=connect_s)


def _log(msg: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"{ts} {msg}\n"
    with open(TOP50_LOG, "a", encoding="utf-8", newline="") as f:
        f.write(line)
    print(msg, flush=True)


def _http_get_json(url: str) -> Any:
    with httpx.Client(
        timeout=_timeouts(),
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    ) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.json()


def load_spot_usdt_trading_symbols() -> set[str]:
    data = _http_get_json(EXCHANGE_INFO)
    symbols = data.get("symbols") or []
    out: set[str] = set()
    for s in symbols:
        if not isinstance(s, dict):
            continue
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        perms = s.get("permissions") or []
        spot_ok = "SPOT" in perms
        if not spot_ok and s.get("isSpotTradingAllowed") is True:
            spot_ok = True
        if not spot_ok:
            continue
        sym = s.get("symbol")
        if isinstance(sym, str) and sym:
            out.add(sym)
    return out


def load_all_24hr_tickers() -> list[dict[str, Any]]:
    data = _http_get_json(TICKER_24HR)
    if not isinstance(data, list):
        raise RuntimeError("unexpected ticker/24hr response shape")
    return [x for x in data if isinstance(x, dict)]


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    ensure_data_directories()

    _log("[top50] GET exchangeInfo ...")
    allowed = load_spot_usdt_trading_symbols()
    _log(f"[top50] exchangeInfo ok, {len(allowed)} USDT spot symbols")
    _log("[top50] GET ticker/24hr ...")
    tickers = load_all_24hr_tickers()
    _log(f"[top50] ticker/24hr ok, {len(tickers)} rows")

    rows: list[tuple[float, dict[str, Any]]] = []
    for t in tickers:
        sym = t.get("symbol")
        if not isinstance(sym, str) or sym not in allowed:
            continue
        qv_raw = t.get("quoteVolume")
        try:
            qv = float(qv_raw) if qv_raw is not None else 0.0
        except (TypeError, ValueError):
            qv = 0.0
        rows.append((qv, t))

    rows.sort(key=lambda x: x[0], reverse=True)
    top = rows[:50]

    payload: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "binance_spot",
        "description": "Top 50 USDT spot symbols by 24h rolling quote volume",
        "count": len(top),
        "pairs": [],
    }

    for i, (qv, t) in enumerate(top, start=1):
        sym = t.get("symbol", "")
        item: dict[str, Any] = {
            "rank": i,
            "symbol": sym,
            "quoteVolume": str(t.get("quoteVolume", "")),
            "lastPrice": str(t.get("lastPrice", "")),
            "volume": str(t.get("volume", "")),
            "priceChangePercent": str(t.get("priceChangePercent", "")),
        }
        payload["pairs"].append(item)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8", newline="") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")

    _log(f"[OK] wrote {len(top)} pairs to {OUT_FILE}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except httpx.HTTPStatusError as e:
        body = (e.response.text or "")[:500]
        err = f"[ERROR] HTTP {e.response.status_code} for {e.request.url!r}\n{body}"
        _log(err.replace("\n", " ")[:2000])
        print(err, file=sys.stderr)
        raise SystemExit(1)
    except httpx.TimeoutException as e:
        err = f"[ERROR] timeout (connect or read). Check network, proxy, or raise FETCH_TOP50_READ_S. detail={e!r}"
        _log(err)
        print(err, file=sys.stderr)
        raise SystemExit(1)
    except httpx.RequestError as e:
        err = f"[ERROR] request failed: {e!r}"
        _log(err)
        print(err, file=sys.stderr)
        raise SystemExit(1)
    except Exception as e:
        err = f"[ERROR] {type(e).__name__}: {e}"
        _log(err)
        print(err, file=sys.stderr)
        raise SystemExit(1)
