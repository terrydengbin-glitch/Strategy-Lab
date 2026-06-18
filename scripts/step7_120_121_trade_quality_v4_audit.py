from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run STEP7.120/7.121 Trade Quality V4 audit.")
    parser.add_argument("--project-root", default=".", help="Project root")
    parser.add_argument("--report", default="docs/reports/STEP7.121_strategy5_6_deep_tq_reanalysis_after_v4_upgrade_20260615.md")
    parser.add_argument("--limit", type=int, default=0, help="Optional smoke sample limit")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    sys.path.insert(0, str(root))
    from laoma_signal_engine.backtest.p21_trade_quality_v4 import (
        generate_gate_candidates_v4_payload,
        materialize_v4_payload,
        reanalysis_markdown,
        summary_payload,
    )

    materialize_v4_payload(root, limit=args.limit or None)
    generate_gate_candidates_v4_payload(root, min_samples=50)
    summary = summary_payload(root)
    report_path = root / args.report
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(reanalysis_markdown(root), encoding="utf-8")
    print(f"wrote={report_path}")
    print(f"features={summary.get('feature_count')} deep_roots={summary.get('deep_root_count')} gates={summary.get('gate_candidate_count')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
