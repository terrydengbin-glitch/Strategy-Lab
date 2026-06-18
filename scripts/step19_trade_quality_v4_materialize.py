from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize Trade Quality V4 evidence and shadow gate candidates.")
    parser.add_argument("--project-root", default=".", help="Project root")
    parser.add_argument("--strategy-line", default="all", help="strategy5, strategy6, or all")
    parser.add_argument("--limit", type=int, default=0, help="Optional sample limit for smoke")
    parser.add_argument("--min-samples", type=int, default=50, help="Min samples per shadow gate bucket")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    sys.path.insert(0, str(root))
    from laoma_signal_engine.backtest.p21_trade_quality_v4 import (
        generate_gate_candidates_v4_payload,
        materialize_v4_payload,
        summary_payload,
    )

    strategies = None if args.strategy_line in ("", "all") else [args.strategy_line]
    materialized = materialize_v4_payload(root, strategies=strategies, limit=args.limit or None)
    gates = generate_gate_candidates_v4_payload(
        root,
        strategy_line=None if args.strategy_line == "all" else args.strategy_line,
        min_samples=args.min_samples,
    )
    summary = summary_payload(root)
    print(json.dumps({"materialized": materialized, "gates": gates, "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
