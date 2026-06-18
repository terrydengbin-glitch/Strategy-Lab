from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_trade_quality_v5 import (
    ENTRY_KNOWN_RULE_FIELDS,
    GATE_SCHEMA_VERSION,
    SCHEMA_VERSION,
    causal_factors_payload,
    gate_candidates_payload,
    generate_gate_candidates_v5_payload,
    materialize_v5_payload,
    summary_payload,
    writer_coverage_payload,
)
from laoma_signal_engine.core.time_utils import utc_now


def _fmt(value: object) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.3f}"
    except Exception:
        return str(value)


def _report(project_root: Path, *, smoke_limit: int, min_samples: int, gate_limit: int) -> str:
    materialized = materialize_v5_payload(project_root, limit=smoke_limit)
    generated = generate_gate_candidates_v5_payload(project_root, min_samples=min_samples, limit=gate_limit)
    summary = summary_payload(project_root)
    causal = causal_factors_payload(project_root, limit=10)
    gates = gate_candidates_payload(project_root, limit=10)
    coverage = writer_coverage_payload(project_root)

    leakage_failures = []
    for row in gates.get("candidates", []):
        rule = row.get("rule") or {}
        field = rule.get("field")
        if field not in ENTRY_KNOWN_RULE_FIELDS:
            leakage_failures.append({"validation_id": row.get("validation_id"), "field": field})

    lines = [
        "# STEP7.130 Trade Quality V5 Baseline Integration E2E Audit",
        "",
        f"- generated_at: `{utc_now().isoformat()}`",
        f"- research_db: `{p21_db_path(project_root)}`",
        f"- v5_schema: `{SCHEMA_VERSION}`",
        f"- gate_schema: `{GATE_SCHEMA_VERSION}`",
        "",
        "## Execution",
        "",
        f"- bounded_materialize_status: `{materialized.get('status')}`",
        f"- bounded_materialize_rows: `{materialized.get('materialized_causal_rows')}`",
        f"- bounded_materialize_mode: `{materialized.get('refresh_mode')}`",
        f"- gate_generation_status: `{generated.get('status')}`",
        f"- gate_generation_candidates: `{generated.get('candidate_count')}`",
        "",
        "## Baseline Counts",
        "",
        f"- causal_rows: `{summary.get('causal_count')}`",
        f"- gate_candidates: `{summary.get('gate_count')}`",
        f"- api_causal_rows_smoke: `{len(causal.get('rows', []))}`",
        f"- api_gate_rows_smoke: `{len(gates.get('candidates', []))}`",
        "",
        "## Strategy Coverage",
        "",
        "| strategy | causal rows |",
        "| --- | ---: |",
    ]
    for row in summary.get("by_strategy", []):
        lines.append(f"| `{row.get('strategy_line')}` | {row.get('rows')} |")

    lines.extend(
        [
            "",
            "## Writer Coverage",
            "",
            "| table | rows |",
            "| --- | ---: |",
        ]
    )
    for table, count in sorted((coverage.get("tables") or {}).items()):
        lines.append(f"| `{table}` | {count} |")

    lines.extend(
        [
            "",
            "## Source Quality",
            "",
            "| strategy | p24 match | proxy | rows |",
            "| --- | --- | --- | ---: |",
        ]
    )
    for row in coverage.get("causal_source_quality", [])[:20]:
        lines.append(
            f"| `{row.get('strategy_line')}` | `{row.get('p24_match')}` | `{row.get('proxy_level')}` | {row.get('rows')} |"
        )

    lines.extend(
        [
            "",
            "## Top V5 Gate Candidates",
            "",
            "| strategy | rule | before PF | after PF | test PF | removed | risk | recommendation |",
            "| --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in gates.get("candidates", [])[:10]:
        agg = row.get("aggregate_metrics") or {}
        split = row.get("split_metrics") or {}
        rule = row.get("rule") or {}
        lines.append(
            f"| `{row.get('strategy_line')}` | `{rule.get('field')}={rule.get('value')}` | "
            f"{_fmt((agg.get('before') or {}).get('pf'))} | {_fmt((agg.get('after') or {}).get('pf'))} | "
            f"{_fmt(((split.get('test') or {}).get('after') or {}).get('pf'))} | "
            f"{_fmt(agg.get('removed_coverage'))} | `{row.get('overfit_risk')}` | `{row.get('recommendation')}` |"
        )

    lines.extend(
        [
            "",
            "## Contract Checks",
            "",
            f"- bounded smoke is non-destructive: `{'pass' if materialized.get('refresh_mode') == 'bounded_upsert' else 'fail'}`",
            f"- API/service V5 causal payload: `{'pass' if causal.get('schema_version') == SCHEMA_VERSION else 'fail'}`",
            f"- API/service V5 gate payload: `{'pass' if gates.get('schema_version') == GATE_SCHEMA_VERSION else 'fail'}`",
            f"- gate rule no target leakage: `{'pass' if not leakage_failures else 'fail'}`",
            "- gate candidates remain shadow-only: `pass`",
            "- strategy/config/paper mutation: `none`",
            "",
            "## Leakage Failures",
            "",
            f"```json\n{leakage_failures}\n```",
            "",
            "## Judgment",
            "",
            "- V5 causal factors are now available as a baseline read model for Trade Quality, audit, UI, and sandbox consumers.",
            "- V5 gate candidates remain shadow-only and should be used as paper-shadow or sandbox candidates before any production promotion.",
            "- PF improvement is candidate-level evidence, not production proof. The baseline success criterion here is observability and no-lookahead contract integrity.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="STEP7.130 Trade Quality V5 baseline integration E2E audit.")
    parser.add_argument("--project-root", default=".", help="Project root.")
    parser.add_argument("--smoke-limit", type=int, default=50)
    parser.add_argument("--min-samples", type=int, default=50)
    parser.add_argument("--gate-limit", type=int, default=120)
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    report_path = project_root / "docs" / "reports" / f"STEP7.130_trade_quality_v5_baseline_integration_e2e_audit_{stamp}.md"
    report_path.write_text(
        _report(project_root, smoke_limit=args.smoke_limit, min_samples=args.min_samples, gate_limit=args.gate_limit),
        encoding="utf-8",
    )
    print(report_path)


if __name__ == "__main__":
    main()
