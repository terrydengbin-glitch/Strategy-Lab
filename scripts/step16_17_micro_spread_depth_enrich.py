from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.micro.training_ledger import (
    enrich_from_audit_factor_frames,
    enrich_spread_depth_missing_reasons,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP16.17 enrich micro training ledger with spread/depth evidence.")
    parser.add_argument("--limit-runs", type=int, default=500)
    parser.add_argument("--max-lag-sec", type=int, default=900)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    factor = enrich_from_audit_factor_frames(
        PROJECT_ROOT,
        limit_runs=args.limit_runs,
        max_lag_sec=args.max_lag_sec,
        dry_run=args.dry_run,
    )
    missing = enrich_spread_depth_missing_reasons(
        PROJECT_ROOT,
        limit_runs=args.limit_runs,
        dry_run=args.dry_run,
    )
    result = {
        "status": "ok",
        "limit_runs": args.limit_runs,
        "dry_run": args.dry_run,
        "factor_frame_enrichment": factor,
        "spread_depth_missing_reason_enrichment": missing,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
