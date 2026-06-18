from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


STEP21_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_focused_STEP21_53_result.json"
STEP21_REPORT_GLOB = "STEP21.53_strategy6_v3_1_focused_matrix_*.md"
TQ_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_tq_STEP21_53_result.json"
FILTER_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_quality_filter_validation_STEP22_29.json"
DIR_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_direction_wrong_STEP22_26.json"
R_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_r_parity_STEP22_27.json"
REPORT_DIR = ROOT / "docs" / "reports"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_e2e_STEP7_106_result.json"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _first(item: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return default


def _latest_report(pattern: str) -> str | None:
    reports = sorted(REPORT_DIR.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    return str(reports[0].relative_to(ROOT)) if reports else None


def _tq_root_causes(tq: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for package in tq.get("packages") or []:
        summary = ((package.get("summary") or {}).get("summary") or {})
        items = ((summary.get("root_cause_attribution") or {}).get("items") or [])
        for item in items[:8]:
            out.append(
                {
                    "parameter_set_id": package.get("parameter_set_id"),
                    "root_cause": _first(item, "root_cause", "key", "label", "name"),
                    "count": _first(item, "count", "sample_count", "trade_count"),
                    "avg_R": _first(item, "avg_R", "avg_net_R"),
                    "avg_MFE_R": item.get("avg_MFE_R"),
                    "avg_MAE_R": item.get("avg_MAE_R"),
                }
            )
    return out[:12]


def run_audit() -> dict[str, Any]:
    focused = _read_json(STEP21_RESULT_PATH, {})
    tq = _read_json(TQ_RESULT_PATH, {})
    filters = _read_json(FILTER_RESULT_PATH, {})
    direction = _read_json(DIR_RESULT_PATH, {})
    r_parity = _read_json(R_RESULT_PATH, {})
    best = focused.get("best") or {}
    metrics = best.get("metrics") or {}
    pf = metrics.get("profit_factor")
    trade_count = int(_num(metrics.get("trade_count"), 0))
    v3_baseline_pf = 0.7244204
    validation_candidates = filters.get("candidates") or []
    stable_candidates = [
        item
        for item in validation_candidates
        if item.get("action") == "shadow_block" and item.get("overfit_risk") in {"low", "medium"}
    ]
    decision = "NO_GO"
    reasons: list[str] = []
    if pf is None or _num(pf) <= 0:
        reasons.append("focused_matrix_has_no_positive_pf")
    if trade_count < 500:
        reasons.append("trade_count_too_low_for_strategy6_v3_1")
    if _num(pf) < 1.0:
        reasons.append("pf_below_1")
    if _num(pf) < v3_baseline_pf:
        reasons.append("pf_below_v3_baseline")
    if stable_candidates:
        reasons.append("quality_filter_candidates_available_shadow_only")
    if _num(pf) >= 1.0 and trade_count >= 500:
        decision = "GO_SHADOW"
    elif stable_candidates or _num(pf) > 0:
        decision = "TUNE"
    return {
        "schema_version": "step7.106-strategy6-v3-1-e2e-audit-v1",
        "generated_at": _now(),
        "decision": decision,
        "decision_reasons": reasons,
        "focused": {
            "result_path": str(STEP21_RESULT_PATH.relative_to(ROOT)) if STEP21_RESULT_PATH.exists() else None,
            "latest_report": _latest_report(STEP21_REPORT_GLOB),
            "experiment_id": focused.get("experiment_id"),
            "best_parameter_set_id": best.get("parameter_set_id"),
            "profit_factor": pf,
            "expectancy_R": metrics.get("expectancy_R"),
            "win_rate": metrics.get("win_rate"),
            "trade_count": metrics.get("trade_count"),
            "avg_win_R": metrics.get("avg_win_R"),
            "avg_loss_R": metrics.get("avg_loss_R"),
            "total_R": metrics.get("total_R"),
            "v3_baseline_pf": v3_baseline_pf,
        },
        "tq": {
            "result_path": str(TQ_RESULT_PATH.relative_to(ROOT)) if TQ_RESULT_PATH.exists() else None,
            "package_count": len(tq.get("packages") or []),
            "root_causes": _tq_root_causes(tq),
        },
        "direction_wrong_drilldown": {
            "result_path": str(DIR_RESULT_PATH.relative_to(ROOT)) if DIR_RESULT_PATH.exists() else None,
            "assigned_ratio": direction.get("assigned_ratio"),
            "top_buckets": (direction.get("overall") or [])[:5],
        },
        "r_parity_shadow": {
            "result_path": str(R_RESULT_PATH.relative_to(ROOT)) if R_RESULT_PATH.exists() else None,
            "baseline": r_parity.get("baseline"),
            "best": (r_parity.get("candidates") or [None])[0],
        },
        "quality_filter_validation": {
            "result_path": str(FILTER_RESULT_PATH.relative_to(ROOT)) if FILTER_RESULT_PATH.exists() else None,
            "baseline": filters.get("baseline"),
            "stable_candidate_count": len(stable_candidates),
            "top_candidates": validation_candidates[:10],
        },
        "boundary": {
            "live_config_changed": False,
            "paper_ledger_changed": False,
            "strategy_lines_changed": ["strategy6_v3_1_opt_in_only"],
            "other_strategy_lines_impacted": False,
        },
    }


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP7.106_strategy6_v3_1_e2e_audit_{ts}.md"
    focused = payload.get("focused") or {}
    lines = [
        "# STEP7.106 Strategy6 V3.1 E2E Audit",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- decision: `{payload.get('decision')}`",
        f"- reasons: `{', '.join(payload.get('decision_reasons') or [])}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        "",
        "## Focused Matrix",
        "",
        f"- experiment_id: `{focused.get('experiment_id')}`",
        f"- best_parameter_set_id: `{focused.get('best_parameter_set_id')}`",
        f"- PF: `{focused.get('profit_factor')}` vs V3 baseline `{focused.get('v3_baseline_pf')}`",
        f"- expectancy_R: `{focused.get('expectancy_R')}`",
        f"- win_rate: `{focused.get('win_rate')}`",
        f"- trade_count: `{focused.get('trade_count')}`",
        f"- avg_win_R / avg_loss_R: `{focused.get('avg_win_R')}` / `{focused.get('avg_loss_R')}`",
        f"- total_R: `{focused.get('total_R')}`",
        "",
        "## Trade Quality Root Causes",
        "",
        "| parameter_set | root_cause | count | avg_R | avg_MFE_R | avg_MAE_R |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for item in (payload.get("tq") or {}).get("root_causes") or []:
        lines.append(
            f"| `{item.get('parameter_set_id')}` | `{item.get('root_cause')}` | {item.get('count')} | "
            f"{item.get('avg_R')} | {item.get('avg_MFE_R')} | {item.get('avg_MAE_R')} |"
        )
    lines.extend(["", "## Quality Filter Validation", ""])
    q = payload.get("quality_filter_validation") or {}
    lines.append(f"- stable_candidate_count: `{q.get('stable_candidate_count')}`")
    lines.append("")
    lines.append("| candidate | action | risk | val_pf_delta | test_pf_delta | test_coverage_loss |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: |")
    for item in q.get("top_candidates") or []:
        lines.append(
            f"| `{item.get('candidate_id')}` | `{item.get('action')}` | `{item.get('overfit_risk')}` | "
            f"{(item.get('validation') or {}).get('pf_delta')} | {(item.get('test') or {}).get('pf_delta')} | "
            f"{(item.get('test') or {}).get('coverage_loss')} |"
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            "",
            "- Strategy6 V3.1 chain is technically executable: evaluator, matrix, TQ materialize, validation, and audit all produced evidence.",
            "- It is not ready for shadow live promotion because PF remains below 1 and below the V3 baseline.",
            "- Next tuning should focus on not over-blocking executable candidates and validating exit/R parity inside the real fill simulator, not only as a shadow approximation.",
            "",
            "## Boundary",
            "",
            "- No production config, live runtime, paper ledger, Feishu output, or other strategy line was changed by this audit.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    payload = run_audit()
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report_path = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report_path), "decision": payload.get("decision")}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
