"""
Step 2 entry: scan 15m / 1h volatility from DATA/universe/TOP50VOL.json.

Usage (from project root):
  python run_step2_volatility_scan.py
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
from backend.app.data_layout import ensure_data_directories
from backend.app.project_root import get_app_root

ROOT = get_app_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.volatility_scanner import run_volatility_scan


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    ensure_data_directories()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    n = len(run_volatility_scan())
    watch_path = ROOT / "DATA" / "raw_signals" / "latest_watch_signals.json"
    nw = 0
    try:
        with open(watch_path, encoding="utf-8", newline="") as f:
            wj = json.load(f)
        sigs = wj.get("signals")
        if isinstance(sigs, list):
            nw = len(sigs)
    except OSError:
        pass
    print(f"[OK] Step 2 done, strong(raw)={n}, watch={nw}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
