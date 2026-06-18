from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


V32_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_focused_STEP21_54_result.json"
V33_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_3_walk_forward_STEP21_55_result.json"
V33_TQ_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_3_tq_STEP19_33_result.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_3_e2e_STEP7_108_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _best_metrics(result: dict[str, Any]) -> dict[str, Any]:
    return ((result.get("best") or {}).get("metrics") or {})


def _selected(result: dict[str, Any]) -> dict[str, Any]:
    return ((result.get("walk_forward") or {}).get("selected") or {})


def _first_package_stats(tq: dict[str, Any]) -> dict[str, Any]:
    packages = tq.get("packages") or []
    if not packages:
        return {}
    return (((packages[0].get("summary") or {}).get("summary") or {}).get("performance_stats") or {})


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP7.108_strategy6_v3_3_no_lookahead_e2e_audit_{ts}.md"
    lines = [
        "# STEP7.108 Strategy6 V3.3 No-Lookahead Anti-Overfit E2E Audit",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- verdict: `{payload.get('verdict')}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        "",
        "## Checks",
        "",
    ]
    for key, value in (payload.get("checks") or {}).items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Metrics",
            "",
            "| source | PF | expectancy_R | win_rate | trades | avg_win_R | avg_loss_R | total_R |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload.get("comparisons") or []:
        m = row.get("metrics") or {}
        lines.append(
            f"| `{row.get('source')}` | {m.get('profit_factor')} | {m.get('expectancy_R')} | "
            f"{m.get('win_rate')} | {m.get('trade_count')} | {m.get('avg_win_R')} | {m.get('avg_loss_R')} | {m.get('total_R')} |"
        )
    lines.extend(["", "## Reasons", ""])
    for reason in payload.get("reasons") or []:
        lines.append(f"- `{reason}`")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- V3.3 is opt-in only via `strategy6_version=v3_3`.",
            "- Test split was not used for candidate selection.",
            "- No production config or live/paper runtime behavior was changed.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    v32 = _load(V32_RESULT_PATH)
    v33 = _load(V33_RESULT_PATH)
    tq = _load(V33_TQ_PATH)
    selected = _selected(v33)
    v32m = _best_metrics(v32)
    val = selected.get("validation") or {}
    test = selected.get("test") or {}
    tq_stats = _first_package_stats(tq)
    checks = {
        "v33_result_exists": bool(v33),
        "v33_tq_exists": bool(tq),
        "entry_feature_contract_field": ((v33.get("no_lookahead_contract") or {}).get("entry_feature_contract_field")),
        "test_split_used_for_selection": ((v33.get("no_lookahead_contract") or {}).get("test_split_used_for_selection")),
        "selected_parameter_set_id": selected.get("parameter_set_id"),
        "validation_trade_count": val.get("trade_count"),
        "test_trade_count": test.get("trade_count"),
        "tq_profit_factor": tq_stats.get("profit_factor"),
    }
    reasons: list[str] = []
    verdict = "NO_GO"
    if not v33:
        reasons.append("missing_v3_3_walk_forward_result")
    if checks.get("test_split_used_for_selection") is not False:
        reasons.append("test_split_selection_boundary_failed")
    if _num(test.get("trade_count")) < 50:
        reasons.append("test_trade_count_too_low_for_promotion")
    if _num(test.get("profit_factor")) <= 0:
        reasons.append("test_profit_factor_missing_or_zero")
    if not reasons:
        if _num(test.get("profit_factor")) >= 1.0:
            verdict = "SHADOW_CANDIDATE"
            reasons.append("holdout_pf_above_1")
        elif _num(test.get("profit_factor")) > _num(v32m.get("profit_factor")):
            verdict = "TUNE"
            reasons.append("holdout_improves_v3_2_but_pf_below_1")
        else:
            verdict = "TUNE"
            reasons.append("valid_chain_but_holdout_not_better_than_v3_2")
    payload = {
        "schema_version": "step7.108-strategy6-v3-3-no-lookahead-e2e-v1",
        "generated_at": _now(),
        "verdict": verdict,
        "reasons": reasons,
        "checks": checks,
        "comparisons": [
            {"source": "v3_2_best_overall", "metrics": v32m},
            {"source": "v3_3_validation_selected_validation", "metrics": val},
            {"source": "v3_3_validation_selected_test", "metrics": test},
            {"source": "v3_3_tq_materialized", "metrics": tq_stats},
        ],
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report), "verdict": verdict, "reasons": reasons}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
