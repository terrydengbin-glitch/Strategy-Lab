from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.research_db import materialize_payload, write_audit_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize P24 unified strategy research DB tables.")
    parser.add_argument("--project-root", default=".", help="Project root path.")
    parser.add_argument("--limit", type=int, default=None, help="Optional per-source row limit for smoke runs.")
    parser.add_argument("--dry-run", action="store_true", help="Count source rows without writing research rows.")
    parser.add_argument("--write-report", action="store_true", help="Write STEP7.124 audit report after materialization.")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    payload = materialize_payload(project_root, limit=args.limit, dry_run=args.dry_run)
    if args.write_report and payload.get("ok") and not args.dry_run:
        payload["report_path"] = str(write_audit_report(project_root, payload))
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
