from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize baseline Trade Quality V5 causal factors.")
    parser.add_argument("--project-root", default=".", help="Project root")
    parser.add_argument("--strategy", action="append", dest="strategies", help="Strategy line to include")
    parser.add_argument("--limit", type=int, default=None, help="Optional sample limit")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    sys.path.insert(0, str(root))
    from laoma_signal_engine.backtest.p21_trade_quality_v5 import materialize_v5_payload, summary_payload

    payload = materialize_v5_payload(root, strategies=args.strategies, limit=args.limit)
    print(json.dumps({"materialized": payload, "summary": summary_payload(root)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
