from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.paper_equivalent import EXECUTION_CONTRACT
from scripts.step28_2_bounded_paper_equivalent_parameter_search import _connect, _run_candidate


TASK_ID = "STEP28.4"
SCHEMA_VERSION = "step28.4.parameter-gate-joint-paper-equivalent-validation.v1"
STEP28_2_JSON = ROOT / "DATA" / "backtest" / "step28" / "step28_2_bounded_paper_equivalent_parameter_search.json"
STEP28_3_JSON = ROOT / "DATA" / "backtest" / "step28" / "step28_3_trade_quality_fast_gate_candidate_search.json"
OUTPUT_JSON = ROOT / "DATA" / "backtest" / "step28" / "step28_4_parameter_gate_joint_paper_equivalent_validation.json"
REPORT_DIR = ROOT / "docs" / "reports"
GATE_CONFIG = ROOT / "DATA" / "paper" / "v5_trade_gate_experiment.json"
TARGET_LINES = ("without_micro", "strategy4", "strategy5", "strategy6")
EXPECTED_PAPER_QUALITY_FIELDS = [
    "paper_equivalent_profit_factor",
    "trade_coverage",
    "blocked_count",
    "pass_count",
    "skip_rows",
    "max_drawdown_R",
    "gate_decision",
    "gate_rule_json",
    "gate_features_json",
    "training_dataset_status",
    "training_leakage_violations",
]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_tag(value: Any) -> str:
    text = "".join(ch if ch.isalnum() else "_" for ch in str(value or "gate"))
    return text.strip("_")[:32] or "gate"


def _loads(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _backup_gate_config() -> Path | None:
    if not GATE_CONFIG.exists():
        return None
    backup = ROOT / "DATA" / "paper" / "gate_config_snapshots" / f"v5_trade_gate_experiment_before_STEP28.4_{_stamp()}.json"
    backup.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(GATE_CONFIG, backup)
    return backup


def _restore_gate_config(backup: Path | None) -> None:
    if backup and backup.exists():
        shutil.copy2(backup, GATE_CONFIG)
    elif GATE_CONFIG.exists():
        GATE_CONFIG.unlink()


def _write_gate_config(candidate: dict[str, Any], experiment_id: str) -> None:
    line = str(candidate["strategy_line"])
    cfg = {
        "enabled": True,
        "experiment_id": experiment_id,
        "paper_epoch_id": f"{experiment_id}_epoch",
        "line_epochs": {line: f"{experiment_id}_{line}"},
        "mode": "step28_4_paper_equivalent_validation",
        "feature_missing_policy": "block",
        "rules": {
            line: {
                "parameter_set_id": candidate.get("parameter_set_id"),
                "gate_candidate_id": candidate.get("gate_candidate_id"),
                "action": "block",
                "rule_json": candidate.get("rule_json") or {},
                "evidence": {
                    "source_step28_3_pf_before": candidate.get("pf_before"),
                    "source_step28_3_pf_after_test": candidate.get("pf_after_test"),
                    "source_step28_3_overfit_risk": candidate.get("overfit_risk"),
                },
            }
        },
    }
    _write_json(GATE_CONFIG, cfg)


def _metric_num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _candidate_rows(conn: sqlite3.Connection, step28_3: dict[str, Any], *, max_candidates_per_line: int) -> list[dict[str, Any]]:
    by_line: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_candidates: list[dict[str, Any]] = []
    for line_result in step28_3.get("line_results") or []:
        for candidate in line_result.get("candidates") or []:
            source_candidates.append(dict(candidate))
    if not source_candidates:
        source_candidates = [dict(row) for row in step28_3.get("leaderboard") or []]
    for row in source_candidates:
        line = str(row.get("strategy_line") or "")
        if line in TARGET_LINES and row.get("known_at_entry") is True and not row.get("microstructure_fields_used"):
            by_line[line].append(row)
    selected: list[dict[str, Any]] = []
    for line in TARGET_LINES:
        rows = sorted(
            by_line.get(line, []),
            key=lambda item: (
                1 if item.get("overfit_risk") != "high" else 0,
                _metric_num(item.get("pf_after_test"), -1),
                _metric_num(item.get("trade_coverage_test"), 0),
            ),
            reverse=True,
        )
        selected.extend(rows[: max(1, int(max_candidates_per_line))])
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in selected:
        key = (str(row.get("strategy_line")), str(row.get("parameter_set_id")), json.dumps(row.get("rule_json"), sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        p21 = _p21_candidate(conn, str(row.get("strategy_line")), str(row.get("parameter_set_id")))
        if not p21:
            row["selection_block_reason"] = "p21_candidate_not_found"
            out.append(row)
            continue
        out.append({**row, "p21_candidate": p21})
    return out


def _p21_candidate(conn: sqlite3.Connection, strategy_line: str, parameter_set_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM p21_v2_30d_metrics
        WHERE strategy_line = ? AND parameter_set_id = ?
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        (strategy_line, parameter_set_id),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["metrics"] = json.loads(item.pop("metrics_json") or "{}")
    item["parameters"] = json.loads(item.pop("parameters_json") or "{}")
    return item


def _validate_candidate(conn: sqlite3.Connection, candidate: dict[str, Any], *, max_orders: int) -> dict[str, Any]:
    p21 = candidate.get("p21_candidate")
    if not isinstance(p21, dict):
        return {
            "strategy_line": candidate.get("strategy_line"),
            "parameter_set_id": candidate.get("parameter_set_id"),
            "gate_candidate_id": candidate.get("gate_candidate_id"),
            "status": "blocked",
            "promotion_allowed": False,
            "promotion_block_reason": candidate.get("selection_block_reason") or "missing_p21_candidate",
        }
    experiment_id = f"step28_4_{_safe_tag(candidate.get('strategy_line'))}_{_safe_tag(candidate.get('gate_candidate_id'))}_{_stamp()}"
    _write_gate_config(candidate, experiment_id)
    result = _run_candidate(
        conn,
        p21,
        max_orders=max_orders,
        run_id_tag=f"s28_4_{_safe_tag(candidate.get('gate_candidate_id'))}",
    )
    metrics = result.get("metrics") or {}
    training = result.get("training_dataset") or {}
    gate_decisions = result.get("gate_decisions") or {}
    blocked = int(gate_decisions.get("blocked") or 0)
    feature_missing = int(gate_decisions.get("feature_missing") or 0)
    pass_count = int(gate_decisions.get("pass") or result.get("created_orders") or 0)
    replayed = int(result.get("shadow_orders_replayed") or 0)
    coverage = round(pass_count / replayed, 8) if replayed else 0.0
    after_pf = metrics.get("profit_factor")
    before_pf = candidate.get("pf_before")
    leakage_violations = int(training.get("leakage_violations") or 0) if isinstance(training, dict) else 0
    promotion_block = _promotion_block_reason(
        before_pf=before_pf,
        after_pf=after_pf,
        coverage=coverage,
        trade_count=int(metrics.get("trade_count") or metrics.get("closed_orders") or 0),
        max_drawdown=_metric_num(metrics.get("max_drawdown_R"), 999999),
        overfit_risk=str(candidate.get("overfit_risk") or "unknown"),
        feature_missing=feature_missing,
        leakage_violations=leakage_violations,
    )
    return {
        "status": "ok",
        "strategy_line": candidate.get("strategy_line"),
        "parameter_set_id": candidate.get("parameter_set_id"),
        "gate_candidate_id": candidate.get("gate_candidate_id"),
        "gate_rule_json": candidate.get("rule_json"),
        "execution_contract": EXECUTION_CONTRACT,
        "source_step28_3": {
            "pf_before": before_pf,
            "pf_after_test": candidate.get("pf_after_test"),
            "trade_coverage_test": candidate.get("trade_coverage_test"),
            "overfit_risk": candidate.get("overfit_risk"),
        },
        "paper_equivalent_run_id": result.get("paper_equivalent_run_id"),
        "db_path": result.get("db_path"),
        "summary_path": result.get("summary_path"),
        "profit_factor_before_gate": before_pf,
        "profit_factor_after_gate": after_pf,
        "paper_equivalent_profit_factor": after_pf,
        "trade_coverage": coverage,
        "blocked_count": blocked,
        "feature_missing_count": feature_missing,
        "pass_count": pass_count,
        "skip_rows": result.get("skip_rows"),
        "created_orders": result.get("created_orders"),
        "closed_orders": metrics.get("closed_orders") or metrics.get("trade_count"),
        "max_drawdown_R": metrics.get("max_drawdown_R"),
        "overfit_risk": candidate.get("overfit_risk"),
        "promotion_allowed": promotion_block is None,
        "promotion_block_reason": promotion_block or "",
        "paper_reusable": feature_missing == 0,
        "expected_paper_quality_fields": EXPECTED_PAPER_QUALITY_FIELDS,
        "training_dataset_status": training.get("training_dataset_status") if isinstance(training, dict) else None,
        "training_dataset_manifest_path": training.get("manifest_path") if isinstance(training, dict) else None,
        "training_leakage_violations": leakage_violations,
        "gate_decisions": gate_decisions,
        "metrics": metrics,
    }


def _promotion_block_reason(
    *,
    before_pf: Any,
    after_pf: Any,
    coverage: float,
    trade_count: int,
    max_drawdown: float,
    overfit_risk: str,
    feature_missing: int,
    leakage_violations: int,
) -> str | None:
    after = _metric_num(after_pf, -1)
    before = _metric_num(before_pf, -1)
    if leakage_violations:
        return "training_leakage_violations"
    if feature_missing:
        return "gate_feature_missing"
    if overfit_risk == "high":
        return "high_overfit_risk"
    if coverage < 0.25:
        return "coverage_below_minimum"
    if trade_count < 20:
        return "trade_count_below_minimum"
    if after <= before:
        return "pf_not_improved_vs_before_gate"
    if after < 1.0:
        return "pf_below_1"
    if max_drawdown > 500:
        return "max_drawdown_R_too_high"
    return None


def _rank(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            bool(row.get("promotion_allowed")),
            _metric_num(row.get("paper_equivalent_profit_factor"), -1),
            _metric_num(row.get("trade_coverage"), 0),
            int(row.get("closed_orders") or 0),
        ),
        reverse=True,
    )


def _per_strategy_best(leaderboard: list[dict[str, Any]]) -> dict[str, Any]:
    best: dict[str, Any] = {}
    for line in TARGET_LINES:
        row = next((item for item in leaderboard if item.get("strategy_line") == line), None)
        if row:
            best[line] = row
    return best


def _write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"STEP28.4_parameter_gate_joint_paper_equivalent_validation_{_stamp()}.md"
    lines = [
        "# STEP28.4 Parameter + Gate Joint Paper-Equivalent Validation",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- output_json: `{OUTPUT_JSON.relative_to(ROOT)}`",
        f"- validated_count: `{len(payload.get('results') or [])}`",
        f"- promotion_count: `{payload.get('promotion_count')}`",
        "",
        "## Leaderboard",
        "",
        "| rank | strategy_line | parameter_set_id | gate_candidate_id | before PF | after PF | coverage | closed | blocked | allowed | block_reason |",
        "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for idx, row in enumerate(payload.get("leaderboard") or [], start=1):
        lines.append(
            "| {} | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                idx,
                row.get("strategy_line"),
                row.get("parameter_set_id"),
                row.get("gate_candidate_id"),
                row.get("profit_factor_before_gate"),
                row.get("profit_factor_after_gate"),
                row.get("trade_coverage"),
                row.get("closed_orders"),
                row.get("blocked_count"),
                row.get("promotion_allowed"),
                row.get("promotion_block_reason"),
            )
        )
    overall = payload.get("overall_best") or {}
    lines.extend(
        [
            "",
            "## Overall Best",
            "",
            f"- strategy_line: `{overall.get('strategy_line')}`",
            f"- parameter_set_id: `{overall.get('parameter_set_id')}`",
            f"- gate_candidate_id: `{overall.get('gate_candidate_id')}`",
            f"- paper_equivalent_profit_factor: `{overall.get('paper_equivalent_profit_factor')}`",
            f"- promotion_allowed: `{overall.get('promotion_allowed')}`",
            f"- promotion_block_reason: `{overall.get('promotion_block_reason')}`",
            "",
            "## Boundary",
            "",
            "- Each validation temporarily writes an isolated V5 gate experiment config and restores the previous config after the run.",
            "- Validation uses PaperEngine / paper.adapter / isolated paper-equivalent ledger.",
            "- No runtime paper config promotion is performed by this task.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-candidates-per-line", type=int, default=3)
    parser.add_argument("--max-orders-per-candidate", type=int, default=100)
    args = parser.parse_args()
    step28_3 = _loads(STEP28_3_JSON)
    backup = _backup_gate_config()
    try:
        with _connect() as conn:
            candidates = _candidate_rows(conn, step28_3, max_candidates_per_line=args.max_candidates_per_line)
            results = [
                _validate_candidate(conn, candidate, max_orders=args.max_orders_per_candidate)
                for candidate in candidates
            ]
    finally:
        _restore_gate_config(backup)
    leaderboard = _rank([row for row in results if row.get("status") == "ok"])
    payload = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "generated_at": _now(),
        "status": "ok" if results else "blocked",
        "source_step28_2": str(STEP28_2_JSON.relative_to(ROOT)),
        "source_step28_3": str(STEP28_3_JSON.relative_to(ROOT)),
        "request": {
            "max_candidates_per_line": int(args.max_candidates_per_line),
            "max_orders_per_candidate": int(args.max_orders_per_candidate),
        },
        "execution_contract": EXECUTION_CONTRACT,
        "results": results,
        "leaderboard": leaderboard,
        "per_strategy_best": _per_strategy_best(leaderboard),
        "overall_best": leaderboard[0] if leaderboard else None,
        "promotion_count": sum(1 for row in leaderboard if row.get("promotion_allowed")),
        "expected_paper_quality_fields": EXPECTED_PAPER_QUALITY_FIELDS,
    }
    _write_json(OUTPUT_JSON, payload)
    report = _write_report(payload)
    print(json.dumps({"status": payload["status"], "output": str(OUTPUT_JSON), "report": str(report), "promotion_count": payload["promotion_count"]}, ensure_ascii=False))
    return 0 if results else 2


if __name__ == "__main__":
    raise SystemExit(main())
