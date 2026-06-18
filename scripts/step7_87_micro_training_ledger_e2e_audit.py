from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.micro.training_ledger import coverage_payload, run_list


def main() -> int:
    generated_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    coverage = coverage_payload(ROOT)
    runs = run_list(ROOT, limit=20)
    verdict = "PASS"
    findings: list[str] = []
    if not coverage.get("run_count"):
        verdict = "FAIL"
        findings.append("micro_training_ledger_empty")
    if coverage.get("audit_run_count") and (coverage.get("run_coverage_ratio") or 0) < 0.8:
        verdict = "PASS_WITH_FINDINGS" if verdict == "PASS" else verdict
        findings.append("historical_run_coverage_below_80_percent")
    report = {
        "generated_at": generated_at,
        "verdict": verdict,
        "coverage": coverage,
        "recent_runs": runs.get("runs") or [],
        "findings": findings,
    }
    out_json = ROOT / "docs" / "reports" / f"STEP7.87_micro_training_ledger_e2e_audit_{generated_at}.json"
    out_md = ROOT / "docs" / "reports" / f"STEP7.87_micro_training_ledger_e2e_audit_{generated_at}.md"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# STEP7.87 Micro Training Ledger E2E Audit",
        "",
        f"- generated_at: `{generated_at}`",
        f"- verdict: `{verdict}`",
        "",
        "## Coverage",
        "",
        f"- training_db: `{coverage.get('db_path')}`",
        f"- run_count: `{coverage.get('run_count')}`",
        f"- audit_run_count: `{coverage.get('audit_run_count')}`",
        f"- run_coverage_ratio: `{coverage.get('run_coverage_ratio')}`",
        f"- symbol_sample_count: `{coverage.get('symbol_sample_count')}`",
        f"- label_count: `{coverage.get('label_count')}`",
        "",
        "## Findings",
        "",
    ]
    if findings:
        lines.extend([f"- {item}" for item in findings])
    else:
        lines.append("- none")
    lines.extend(["", "## Recent Runs", ""])
    for row in runs.get("runs") or []:
        lines.append(f"- `{row.get('run_id')}` symbols/lines={row.get('line_count')} generated_at={row.get('generated_at')}")
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "ok", "report": str(out_md), "verdict": verdict}, ensure_ascii=False))
    return 0 if verdict != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
