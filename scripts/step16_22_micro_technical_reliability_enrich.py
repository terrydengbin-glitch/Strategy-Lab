from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.micro.training_ledger import (
    backfill_from_audit,
    classify_run_sample_gaps,
    coverage_payload,
    enrich_downstream_labels,
    enrich_from_audit_factor_frames,
    enrich_spread_depth_missing_reasons,
    refresh_technical_reliability_from_samples,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP16.22 micro technical reliability enrichment")
    parser.add_argument("--root", default=".", help="project root")
    parser.add_argument("--limit-runs", type=int, default=500)
    parser.add_argument("--max-lag-sec", type=int, default=900)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    backfill = backfill_from_audit(root, limit_runs=args.limit_runs, dry_run=args.dry_run)
    factor = enrich_from_audit_factor_frames(
        root,
        limit_runs=args.limit_runs,
        max_lag_sec=args.max_lag_sec,
        dry_run=args.dry_run,
    )
    depth = enrich_spread_depth_missing_reasons(root, limit_runs=args.limit_runs, dry_run=args.dry_run)
    technical = refresh_technical_reliability_from_samples(root, limit_runs=args.limit_runs, dry_run=args.dry_run)
    gaps = classify_run_sample_gaps(root, limit_runs=args.limit_runs, dry_run=args.dry_run)
    labels = enrich_downstream_labels(root, limit_runs=args.limit_runs, dry_run=args.dry_run)
    coverage = coverage_payload(root)

    print(
        json.dumps(
            {
                "status": "ok",
                "dry_run": args.dry_run,
                "backfill": backfill,
                "factor_frame_enrichment": factor,
                "spread_depth_enrichment": depth,
                "technical_reliability_refresh": technical,
                "run_gap_classification": gaps,
                "downstream_label_enrichment": labels,
                "coverage": coverage,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
