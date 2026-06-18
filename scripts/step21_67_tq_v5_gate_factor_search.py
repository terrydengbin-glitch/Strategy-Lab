from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Trade Quality V5 shadow gate factor search.")
    parser.add_argument("--project-root", default=".", help="Project root")
    parser.add_argument("--strategy-line", default=None, help="Optional strategy line")
    parser.add_argument("--min-samples", type=int, default=50, help="Minimum samples per candidate bucket")
    parser.add_argument("--limit", type=int, default=80, help="Candidate limit")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    sys.path.insert(0, str(root))
    from laoma_signal_engine.backtest.p21_trade_quality_v5 import generate_gate_candidates_v5_payload

    payload = generate_gate_candidates_v5_payload(
        root,
        strategy_line=args.strategy_line,
        min_samples=args.min_samples,
        limit=args.limit,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
