from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


V3_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_focused_STEP21_52_result.json"
V31_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_focused_STEP21_53_result.json"
V32_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_focused_STEP21_54_result.json"
V32_TQ_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_tq_STEP19_32_result.json"
R_AUDIT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_r_parity_fill_audit_STEP22_33.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_e2e_STEP7_107_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _best(path: Path) -> dict[str, Any]:
    return _load(path).get("best") or {}


def _metrics(best: dict[str, Any]) -> dict[str, Any]:
    return best.get("metrics") or {}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _first_package_stats(tq: dict[str, Any]) -> dict[str, Any]:
    packages = tq.get("packages") or []
    if not packages:
        return {}
    return (((packages[0].get("summary") or {}).get("summary") or {}).get("performance_stats") or {})


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP7.107_strategy6_v3_2_e2e_audit_{ts}.md"
    lines = [
        "# STEP7.107 Strategy6 V3.2 E2E Audit",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- verdict: `{payload.get('verdict')}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        "",
        "## Baseline Comparison",
        "",
        "| version | PF | expectancy_R | win_rate | trades | avg_win_R | avg_loss_R | total_R |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload.get("comparisons") or []:
        m = row.get("metrics") or {}
        lines.append(
            f"| `{row.get('version')}` | {m.get('profit_factor')} | {m.get('expectancy_R')} | "
            f"{m.get('win_rate')} | {m.get('trade_count')} | {m.get('avg_win_R')} | {m.get('avg_loss_R')} | {m.get('total_R')} |"
        )
    lines.extend(["", "## Checks", ""])
    for key, value in (payload.get("checks") or {}).items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Recommendation", "", payload.get("recommendation") or ""])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    v3 = _best(V3_RESULT_PATH)
    v31 = _best(V31_RESULT_PATH)
    v32 = _best(V32_RESULT_PATH)
    v3m = _metrics(v3)
    v31m = _metrics(v31)
    v32m = _metrics(v32)
    tq = _load(V32_TQ_PATH)
    r_audit = _load(R_AUDIT_PATH)
    r_items = r_audit.get("items") or []
    v32_experiment_id = str(v32.get("experiment_id") or "")
    r_violation_count = sum(
        int(item.get("loss_cap_violation_count") or 0)
        for item in r_items
        if str(item.get("experiment_id") or "") == v32_experiment_id
    )
    tq_stats = _first_package_stats(tq)

    v3_pf = _num(v3m.get("profit_factor"))
    v31_pf = _num(v31m.get("profit_factor"))
    v32_pf = _num(v32m.get("profit_factor"))
    trade_count = int(_num(v32m.get("trade_count")))
    verdict = "NO_GO"
    reasons: list[str] = []
    if not V32_RESULT_PATH.exists():
        reasons.append("missing_v3_2_matrix_result")
    elif trade_count < 300:
        reasons.append("trade_count_too_low")
    elif v32_pf > max(v3_pf, v31_pf):
        verdict = "TUNE"
        reasons.append("v3_2_improves_baselines_but_pf_below_1")
    elif v32_pf >= 1.0:
        verdict = "SHADOW_CANDIDATE"
        reasons.append("pf_above_1")
    else:
        reasons.append("v3_2_did_not_improve_baseline")
    if r_violation_count:
        reasons.append("r_parity_contract_has_violations")

    payload = {
        "schema_version": "step7.107-strategy6-v3-2-e2e-audit-v1",
        "generated_at": _now(),
        "verdict": verdict,
        "reasons": reasons,
        "comparisons": [
            {"version": "v3", "parameter_set_id": v3.get("parameter_set_id"), "metrics": v3m},
            {"version": "v3_1", "parameter_set_id": v31.get("parameter_set_id"), "metrics": v31m},
            {"version": "v3_2", "parameter_set_id": v32.get("parameter_set_id"), "metrics": v32m},
        ],
        "checks": {
            "v3_2_result_exists": V32_RESULT_PATH.exists(),
            "v3_2_tq_exists": V32_TQ_PATH.exists(),
            "r_parity_audit_exists": R_AUDIT_PATH.exists(),
            "r_parity_loss_cap_violation_count": r_violation_count,
            "tq_profit_factor": tq_stats.get("profit_factor"),
            "tq_avg_loss_R": tq_stats.get("avg_loss_R"),
        },
        "recommendation": (
            "V3.2 can move to larger focused matrix only if PF improves over V3 and R-parity violations are zero. "
            "If not, prioritize fill/R-parity contract repair over adding more gates."
        ),
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report), "verdict": verdict, "reasons": reasons}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
