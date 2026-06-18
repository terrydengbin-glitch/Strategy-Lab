from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path


REPORT_DIR = ROOT / "docs" / "reports"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_r_parity_fill_audit_STEP22_33.json"
SOURCE_RESULTS = [
    ROOT / "DATA" / "backtest" / "strategy6_v3_focused_STEP21_52_result.json",
    ROOT / "DATA" / "backtest" / "strategy6_v3_1_focused_STEP21_53_result.json",
    ROOT / "DATA" / "backtest" / "strategy6_v3_2_focused_STEP21_54_result.json",
]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _loads(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _top_sets(path: Path, limit: int = 5) -> tuple[str, list[str]]:
    if not path.exists():
        return "", []
    payload = json.loads(path.read_text(encoding="utf-8"))
    experiment_id = str(payload.get("experiment_id") or ((payload.get("best") or {}).get("experiment_id")) or "")
    out: list[str] = []
    best = (payload.get("best") or {}).get("parameter_set_id")
    if best:
        out.append(str(best))
    for row in payload.get("leaderboard") or []:
        ps = row.get("parameter_set_id")
        if ps and str(ps) not in out:
            out.append(str(ps))
        if len(out) >= limit:
            break
    return experiment_id, out[:limit]


def _configured_loss_cap(config: dict[str, Any], features: dict[str, Any]) -> float:
    block = config.get("strategy6") if isinstance(config.get("strategy6"), dict) else {}
    tier = str(features.get("strategy6_adaptive_exit_tier") or "medium_quality")
    direct = _num(block.get("max_loss_R_cap") or config.get("max_loss_R_cap"), 0.0)
    if direct > 0:
        return direct
    if tier == "high_quality":
        return _num(block.get("high_quality_loss_cap_R"), 0.0)
    if tier == "low_quality":
        return _num(block.get("low_quality_loss_cap_R"), 0.0)
    return _num(block.get("medium_quality_loss_cap_R"), 0.0)


def _configured_first_tp(config: dict[str, Any], features: dict[str, Any]) -> float:
    block = config.get("strategy6") if isinstance(config.get("strategy6"), dict) else {}
    tier = str(features.get("strategy6_adaptive_exit_tier") or "medium_quality")
    direct = _num(block.get("first_tp_R") or config.get("first_tp_R"), 0.0)
    if direct > 0:
        return direct
    if tier == "high_quality":
        return _num(block.get("high_quality_first_tp_R"), 0.0)
    if tier == "low_quality":
        return _num(block.get("low_quality_first_tp_R"), 0.0)
    return _num(block.get("medium_quality_first_tp_R"), 0.0)


def audit_parameter_set(conn: sqlite3.Connection, experiment_id: str, parameter_set_id: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT net_R, exit_reason, planned_rr, features_json, config_patch_json, fast_exit_policy_json
        FROM p21_v2_shadow_orders
        WHERE experiment_id = ? AND parameter_set_id = ? AND strategy_line = 'strategy6'
        """,
        (experiment_id, parameter_set_id),
    ).fetchall()
    total = len(rows)
    loss_count = 0
    loss_cap_count = 0
    loss_cap_violations = 0
    first_tp_count = 0
    first_tp_hits = 0
    first_tp_misses = 0
    worst_loss = 0.0
    sample_violations: list[dict[str, Any]] = []
    for net_r, exit_reason, planned_rr, features_raw, config_raw, policy_raw in rows:
        features = _loads(features_raw, {})
        config = _loads(config_raw, {})
        policy = _loads(policy_raw, {})
        net = _num(net_r, 0.0)
        worst_loss = min(worst_loss, net)
        if net < 0:
            loss_count += 1
        loss_cap = _configured_loss_cap(config, features)
        if loss_cap > 0:
            loss_cap_count += 1
            if net < -(loss_cap + 0.08):
                loss_cap_violations += 1
                if len(sample_violations) < 8:
                    sample_violations.append(
                        {
                            "net_R": round(net, 6),
                            "loss_cap_R": loss_cap,
                            "exit_reason": exit_reason,
                            "planned_rr": planned_rr,
                            "tier": features.get("strategy6_adaptive_exit_tier"),
                            "policy": policy.get("strategy6_exit_protection") if isinstance(policy, dict) else None,
                        }
                    )
        first_tp = _configured_first_tp(config, features)
        if first_tp > 0:
            first_tp_count += 1
            if exit_reason == "strategy6_first_tp":
                first_tp_hits += 1
            elif net > first_tp + 0.15:
                first_tp_misses += 1
    return {
        "experiment_id": experiment_id,
        "parameter_set_id": parameter_set_id,
        "total_orders": total,
        "loss_count": loss_count,
        "loss_cap_count": loss_cap_count,
        "loss_cap_violation_count": loss_cap_violations,
        "loss_cap_violation_ratio": round(loss_cap_violations / loss_cap_count, 8) if loss_cap_count else 0.0,
        "first_tp_count": first_tp_count,
        "first_tp_hit_count": first_tp_hits,
        "first_tp_possible_miss_count": first_tp_misses,
        "worst_net_R": round(worst_loss, 8),
        "sample_violations": sample_violations,
    }


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP22.33_strategy6_v3_2_r_parity_fill_contract_audit_{ts}.md"
    lines = [
        "# STEP22.33 Strategy6 V3.2 R-Parity Fill Contract Audit",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        "",
        "| experiment | parameter_set | orders | loss cap violations | violation ratio | first TP hits | first TP miss hints | worst net_R |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload.get("items") or []:
        lines.append(
            f"| `{row.get('experiment_id')}` | `{row.get('parameter_set_id')}` | {row.get('total_orders')} | "
            f"{row.get('loss_cap_violation_count')} | {row.get('loss_cap_violation_ratio')} | "
            f"{row.get('first_tp_hit_count')} | {row.get('first_tp_possible_miss_count')} | {row.get('worst_net_R')} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- `loss_cap_violation_count > 0` means configured Strategy6 loss cap did not match realized `net_R` for some fills.",
            "- `first_tp_possible_miss_count > 0` means orders exceeded configured first TP without `strategy6_first_tp`, which needs fill-order review.",
            "- This report is audit-only and does not change strategy or fill logic.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    db = p21_db_path(ROOT)
    if not db.exists():
        raise SystemExit(f"missing p21 db: {db}")
    items: list[dict[str, Any]] = []
    with sqlite3.connect(db) as conn:
        for result_path in SOURCE_RESULTS:
            exp_id, param_sets = _top_sets(result_path)
            for param_set in param_sets:
                items.append(audit_parameter_set(conn, exp_id, param_set))
    payload = {
        "schema_version": "step22.33-strategy6-r-parity-fill-contract-audit-v1",
        "generated_at": _now(),
        "source_results": [str(p.relative_to(ROOT)) for p in SOURCE_RESULTS if p.exists()],
        "items": items,
    }
    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report), "items": len(items)}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
