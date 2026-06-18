from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


V33_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_3_walk_forward_STEP21_55_result.json"
V34_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_4_walk_forward_STEP21_56_result.json"
V34_TQ_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_4_tq_STEP19_35_result.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_4_e2e_STEP7_109_result.json"
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


def _selected(result: dict[str, Any]) -> dict[str, Any]:
    return ((result.get("walk_forward") or {}).get("selected") or {})


def _first_package_stats(tq: dict[str, Any]) -> dict[str, Any]:
    packages = tq.get("packages") or []
    if not packages:
        return {}
    return (((packages[0].get("summary") or {}).get("summary") or {}).get("performance_stats") or {})


def _root_causes(tq: dict[str, Any]) -> list[dict[str, Any]]:
    packages = tq.get("packages") or []
    if not packages:
        return []
    summary = packages[0].get("summary") or {}
    return list((((summary.get("summary") or {}).get("root_cause_attribution") or {}).get("items") or []))


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP7.109_strategy6_v3_4_anti_overfit_e2e_audit_{ts}.md"
    lines = [
        "# STEP7.109 Strategy6 V3.4 Anti-Overfit E2E Audit",
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
    lines.extend(["", "## Root Cause Snapshot", "", "| root_cause | count | avg_R | avg_MFE_R | avg_MAE_R |", "| --- | ---: | ---: | ---: | ---: |"])
    for item in payload.get("root_causes") or []:
        label = item.get("root_cause") or item.get("key") or item.get("label") or item.get("name")
        lines.append(f"| `{label}` | {item.get('count') or item.get('trade_count')} | {item.get('avg_R') or item.get('avg_net_R')} | {item.get('avg_MFE_R')} | {item.get('avg_MAE_R')} |")
    lines.extend(["", "## Reasons", ""])
    for reason in payload.get("reasons") or []:
        lines.append(f"- `{reason}`")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- V3.4 is opt-in only via `strategy6_version=v3_4`.",
            "- Test split was not used for parameter selection.",
            "- V3.4 only uses entry-known causal features; no future bars are allowed in gate/scoring.",
            "- No production config or live/paper runtime behavior was changed.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    v33 = _load(V33_RESULT_PATH)
    v34 = _load(V34_RESULT_PATH)
    tq = _load(V34_TQ_PATH)
    v33_selected = _selected(v33)
    v34_selected = _selected(v34)
    v33_test = v33_selected.get("test") or {}
    v34_val = v34_selected.get("validation") or {}
    v34_test = v34_selected.get("test") or {}
    tq_stats = _first_package_stats(tq)
    root_causes = _root_causes(tq)[:10]
    checks = {
        "v34_result_exists": bool(v34),
        "v34_tq_exists": bool(tq),
        "entry_feature_contract_field": ((v34.get("no_lookahead_contract") or {}).get("entry_feature_contract_field")),
        "test_split_used_for_selection": ((v34.get("no_lookahead_contract") or {}).get("test_split_used_for_selection")),
        "selected_parameter_set_id": v34_selected.get("parameter_set_id"),
        "validation_trade_count": v34_val.get("trade_count"),
        "test_trade_count": v34_test.get("trade_count"),
        "tq_profit_factor": tq_stats.get("profit_factor"),
        "v33_test_profit_factor": v33_test.get("profit_factor"),
    }
    reasons: list[str] = []
    verdict = "NO_GO"
    if not v34:
        reasons.append("missing_v3_4_walk_forward_result")
    if checks.get("test_split_used_for_selection") is not False:
        reasons.append("test_split_selection_boundary_failed")
    if checks.get("entry_feature_contract_field") != "strategy6_v3_4_known_at_contract":
        reasons.append("v3_4_no_lookahead_contract_missing")
    if _num(v34_test.get("trade_count")) < 50:
        reasons.append("test_trade_count_too_low_for_promotion")
    if _num(v34_test.get("profit_factor")) <= 0:
        reasons.append("test_profit_factor_missing_or_zero")
    if _num(v34_val.get("profit_factor")) > 0 and _num(v34_test.get("profit_factor")) < _num(v34_val.get("profit_factor")) * 0.45:
        reasons.append("validation_test_decay_too_large")
    if not reasons:
        if _num(v34_test.get("profit_factor")) >= 1.0 and _num(tq_stats.get("profit_factor")) >= 1.0:
            verdict = "SHADOW_CANDIDATE"
            reasons.append("holdout_and_tq_pf_above_1")
        elif _num(v34_test.get("profit_factor")) > _num(v33_test.get("profit_factor")):
            verdict = "TUNE"
            reasons.append("v3_4_improves_v3_3_holdout_but_pf_below_1_or_tq_below_1")
        else:
            verdict = "TUNE"
            reasons.append("valid_chain_but_no_holdout_improvement_over_v3_3")
    payload = {
        "schema_version": "step7.109-strategy6-v3-4-anti-overfit-e2e-v1",
        "generated_at": _now(),
        "verdict": verdict,
        "reasons": reasons,
        "checks": checks,
        "comparisons": [
            {"source": "v3_3_selected_test", "metrics": v33_test},
            {"source": "v3_4_selected_validation", "metrics": v34_val},
            {"source": "v3_4_selected_test", "metrics": v34_test},
            {"source": "v3_4_tq_materialized", "metrics": tq_stats},
        ],
        "root_causes": root_causes,
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report), "verdict": verdict, "reasons": reasons}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
