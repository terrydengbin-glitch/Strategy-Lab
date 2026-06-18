from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.api.services import _paper_config
from laoma_signal_engine.backtest.p21 import baseline_payload, run_matrix_payload


def _now_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_md(path: Path, payload: dict) -> None:
    baseline = payload["baseline"]
    matrix = payload["matrix"]
    best = matrix.get("best") or {}
    best_metrics = best.get("metrics") or {}
    lines = [
        "# STEP7.93 Parameter Optimization Backtest E2E Audit",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- source: `{payload['source']}`",
        f"- strategy scope: `without_micro / strategy4 / strategy5`",
        f"- sample_count: `{baseline.get('sample_count')}`",
        f"- baseline_profit_factor: `{baseline.get('metrics', {}).get('profit_factor')}`",
        f"- matrix_parameter_sets: `{matrix.get('parameter_set_count')}`",
        f"- best_parameter_set_id: `{best.get('parameter_set_id')}`",
        f"- best_profit_factor: `{best_metrics.get('profit_factor')}`",
        f"- best_expectancy_R: `{best_metrics.get('expectancy_R')}`",
        f"- target_pf_gt_1: `{payload['target_pf_gt_1']}`",
        "",
        "## Boundary",
        "",
        "- P21 only reads P19 diagnostic samples and writes `DATA/backtest/p21_parameter_optimization.db`.",
        "- P21 does not write trade plans, paper orders, Feishu messages, or runtime config.",
        "- Micro strategy parameters are outside this first matrix.",
        "",
        "## Top Candidates",
        "",
        "| rank | parameter_set | line | PF | expectancy_R | accepted | blocked | rules |",
        "|---:|---|---|---:|---:|---:|---:|---|",
    ]
    for idx, row in enumerate((matrix.get("leaderboard") or [])[:10], start=1):
        metrics = row.get("metrics") or {}
        params = row.get("parameters") or {}
        rules = f"MFE>={params.get('min_MFE_R')} MAE<={params.get('max_MAE_R')} block={','.join(params.get('blocked_root_causes') or []) or '-'}"
        lines.append(
            f"| {idx} | `{row.get('parameter_set_id')}` | `{params.get('target_strategy_line')}` | "
            f"{metrics.get('profit_factor')} | {metrics.get('expectancy_R')} | "
            f"{metrics.get('accepted_count')} | {metrics.get('blocked_count')} | {rules} |"
        )
    lines.extend(
        [
            "",
            "## Result",
            "",
            "PASS: backtest chain produced reproducible baseline and matrix ledger."
            if payload["status"] == "PASS"
            else "WARN: backtest ran, but no PF > 1 candidate was found in this sample.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    root = Path.cwd().resolve()
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cfg = _paper_config()
    baseline = baseline_payload(root, source="all", strategy_line="all", limit=5000, write=True, config=cfg)
    matrix = run_matrix_payload(root, source="all", strategy_line="all", limit=5000, max_sets=120, write=True, config=cfg)
    best_pf = ((matrix.get("best") or {}).get("metrics") or {}).get("profit_factor")
    target = bool(best_pf and best_pf > 1)
    payload = {
        "schema_version": "7.93",
        "status": "PASS" if matrix.get("leaderboard") else "FAIL",
        "source": "all",
        "generated_at": generated_at,
        "baseline": baseline,
        "matrix": matrix,
        "target_pf_gt_1": target,
    }
    stamp = _now_slug()
    report_dir = root / "docs" / "reports"
    json_path = report_dir / f"STEP7.93_parameter_optimization_backtest_e2e_audit_{stamp}.json"
    md_path = report_dir / f"STEP7.93_parameter_optimization_backtest_e2e_audit_{stamp}.md"
    _write_json(json_path, payload)
    _write_md(md_path, payload)
    print(json.dumps({"json": str(json_path), "md": str(md_path), "target_pf_gt_1": target, "best_pf": best_pf}, ensure_ascii=False))


if __name__ == "__main__":
    main()
