from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit Strategy5/6 Trade Quality V5 gate candidates.")
    parser.add_argument("--project-root", default=".", help="Project root")
    parser.add_argument("--min-samples", type=int, default=50, help="Minimum samples per gate bucket")
    parser.add_argument("--limit", type=int, default=120, help="Candidate limit")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    sys.path.insert(0, str(root))
    from laoma_signal_engine.backtest.p21_trade_quality_v5 import (
        audit_markdown,
        generate_gate_candidates_v5_payload,
        materialize_v5_payload,
    )

    materialize_v5_payload(root, strategies=["strategy5", "strategy6"])
    generate_gate_candidates_v5_payload(root, min_samples=args.min_samples, limit=args.limit)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = root / "docs" / "reports" / f"STEP7.129_strategy5_6_p24_tq_v5_gate_pf_holdout_audit_{ts}.md"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(audit_markdown(root), encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
