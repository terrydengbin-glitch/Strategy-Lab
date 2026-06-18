from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.micro.training_ledger import backfill_from_audit, init_micro_training_db


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP16.14 micro training ledger backfill")
    parser.add_argument("--root", default=".", help="Project root")
    parser.add_argument("--limit-runs", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--init-only", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    if args.init_only:
        db = init_micro_training_db(root=root)
        print(json.dumps({"status": "ok", "db_path": str(db)}, ensure_ascii=False))
        return 0
    result = backfill_from_audit(root, limit_runs=args.limit_runs, dry_run=args.dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
