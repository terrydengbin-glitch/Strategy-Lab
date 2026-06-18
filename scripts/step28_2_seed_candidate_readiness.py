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

from laoma_signal_engine.training_snapshot_sync import sync_paper_sqlite_source


TASK_ID = "STEP28.2"
OUT_DIR = ROOT / "DATA" / "backtest" / "step28"
REPORT_DIR = ROOT / "docs" / "reports"
OUTPUT_JSON = OUT_DIR / "step28_2_seed_candidate_readiness.json"

STEP7_150_JSON = ROOT / "DATA" / "backtest" / "step7_150_strategy1_2_3_4_minimal_paper_equivalent_smoke.json"
STEP7_146_JSON = ROOT / "DATA" / "backtest" / "step7_146_strategy5_6_v5_gate_paper_equivalent_backtest.json"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _sidecar_counts() -> dict[str, Any]:
    db_path = ROOT / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db"
    out = {"db_path": str(db_path.relative_to(ROOT)), "exists": db_path.exists(), "samples": 0, "events": 0, "source_modes": {}}
    if not db_path.exists():
        return out
    with sqlite3.connect(db_path) as conn:
        for table, key in (("trade_training_samples", "samples"), ("trade_snapshot_events", "events")):
            try:
                out[key] = int(conn.execute(f"select count(*) from {table}").fetchone()[0])
            except sqlite3.Error:
                out[key] = 0
        try:
            rows = conn.execute(
                "select source_mode, count(*) from trade_training_samples group by source_mode order by source_mode"
            ).fetchall()
            out["source_modes"] = {str(mode): int(count) for mode, count in rows}
        except sqlite3.Error:
            pass
    return out


def _sync_if_db(row: dict[str, Any], *, run_prefix: str) -> dict[str, Any]:
    db_path = Path(str(row.get("db_path") or ""))
    if not db_path.is_absolute():
        db_path = ROOT / db_path
    if not db_path.exists():
        return {
            "training_dataset_status": "missing_source_db",
            "source_db_path": str(db_path),
        }
    run_id = f"{run_prefix}_{row.get('paper_equivalent_run_id') or row.get('strategy_line') or 'unknown'}"
    return sync_paper_sqlite_source(
        ROOT,
        source_db_path=db_path,
        run_id=run_id,
        source_mode="paper_equivalent_backtest",
    )


def _seed_from_step7_150(data: dict[str, Any]) -> list[dict[str, Any]]:
    seeds: list[dict[str, Any]] = []
    for row in data.get("results") or []:
        line = str(row.get("strategy_line") or "")
        if line not in {"without_micro", "strategy4"}:
            continue
        training = _sync_if_db(row, run_prefix="step28_2_seed")
        seeds.append(
            {
                "strategy_line": line,
                "strategy_name": "strategy1" if line == "without_micro" else "strategy4",
                "seed_source": "STEP7.150_minimal_paper_equivalent_smoke",
                "seed_role": "runner_contract_seed_not_profit_candidate",
                "parameter_set_id": "current_config_seed",
                "gate_candidate_id": None,
                "paper_equivalent_run_id": row.get("paper_equivalent_run_id"),
                "db_path": row.get("db_path"),
                "counts": row.get("counts") or {},
                "metrics": row.get("metrics") or {},
                "promotion_allowed": False,
                "promotion_block_reason": "minimal_smoke_not_parameter_search",
                "training_dataset": training,
            }
        )
    return seeds


def _seed_from_step7_146(data: dict[str, Any]) -> list[dict[str, Any]]:
    seeds: list[dict[str, Any]] = []
    for row in data.get("results") or []:
        line = str(row.get("strategy_line") or "")
        if line not in {"strategy5", "strategy6"}:
            continue
        branch = str(row.get("branch") or "unknown")
        training = _sync_if_db(row, run_prefix="step28_2_seed")
        seeds.append(
            {
                "strategy_line": line,
                "strategy_name": line,
                "seed_source": "STEP7.146_strategy5_6_v5_gate_paper_equivalent_backtest",
                "seed_role": "paper_equivalent_gate_seed",
                "parameter_set_id": row.get("parameter_set_id") or row.get("experiment_id") or f"{line}_{branch}_seed",
                "gate_candidate_id": f"{line}_v5_gate_seed" if branch == "gate_on" else None,
                "branch": branch,
                "paper_equivalent_run_id": row.get("paper_equivalent_run_id"),
                "db_path": row.get("db_path"),
                "created_orders": row.get("created_orders"),
                "gate_decisions": row.get("gate_decisions") or {},
                "metrics": row.get("metrics") or {},
                "promotion_allowed": False,
                "promotion_block_reason": "seed_requires_step28_2_bounded_search_and_step28_4_validation",
                "training_dataset": training,
            }
        )
    return seeds


def _best_seed_by_line(seeds: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for line in ("without_micro", "strategy4", "strategy5", "strategy6"):
        rows = [row for row in seeds if row.get("strategy_line") == line]
        if not rows:
            out[line] = {"status": "missing_seed"}
            continue
        ranked = sorted(rows, key=lambda row: float(((row.get("metrics") or {}).get("profit_factor") or -1)), reverse=True)
        out[line] = ranked[0]
    return out


def build_payload() -> dict[str, Any]:
    before = _sidecar_counts()
    seeds = _seed_from_step7_150(_load(STEP7_150_JSON)) + _seed_from_step7_146(_load(STEP7_146_JSON))
    after = _sidecar_counts()
    return {
        "schema_version": "step28.2.seed_candidate_readiness.v1",
        "task_id": TASK_ID,
        "generated_at": _now(),
        "status": "in_progress",
        "objective": "seed_readiness_for_formal_bounded_paper_equivalent_parameter_search",
        "target_strategy_lines": ["without_micro", "strategy4", "strategy5", "strategy6"],
        "hard_constraints": {
            "execution_contract_required": "paper_equivalent",
            "feature_scope": "kline_only",
            "per_strategy_config_policy": "independent",
            "training_sidecar_required": True,
            "promotion_from_seed_allowed": False,
        },
        "source_refs": {
            "step28_1_manifest": "DATA/backtest/step28/step28_1_optimization_universe_metric_contract.json",
            "step7_150": str(STEP7_150_JSON.relative_to(ROOT)),
            "step7_146": str(STEP7_146_JSON.relative_to(ROOT)),
        },
        "sidecar_before": before,
        "sidecar_after": after,
        "seed_candidates": seeds,
        "best_seed_by_line": _best_seed_by_line(seeds),
        "next_actions": [
            "run bounded paper-equivalent parameter search for without_micro and strategy4 beyond smoke seeds",
            "run or reuse S5/S6 candidate matrix then rerun top-N through paper_equivalent",
            "feed top-N trades to STEP28.3 K-line-only Trade Quality gate search",
        ],
    }


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"STEP28.2_seed_candidate_readiness_{_stamp()}.md"
    lines = [
        "# STEP28.2 Seed Candidate Readiness",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- status: `{payload['status']}`",
        f"- output_json: `{OUTPUT_JSON.relative_to(ROOT)}`",
        f"- sidecar_before: samples=`{payload['sidecar_before']['samples']}` events=`{payload['sidecar_before']['events']}`",
        f"- sidecar_after: samples=`{payload['sidecar_after']['samples']}` events=`{payload['sidecar_after']['events']}`",
        "",
        "## Seeds",
        "",
        "| strategy_line | source | branch | PF | trades | training_status | promotion |",
        "| --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for row in payload["seed_candidates"]:
        metrics = row.get("metrics") or {}
        training = row.get("training_dataset") or {}
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                row.get("strategy_line"),
                row.get("seed_source"),
                row.get("branch") or "-",
                metrics.get("profit_factor", "-"),
                metrics.get("trade_count") or metrics.get("closed_orders") or "-",
                training.get("training_dataset_status") or training.get("status") or training.get("training_dataset_status", "-"),
                "allowed" if row.get("promotion_allowed") else row.get("promotion_block_reason"),
            )
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            "",
            "- Four target lines have seed evidence or runner contract evidence.",
            "- Seeds are not promotion candidates by themselves.",
            "- P29 sidecar sync is mandatory and was invoked for seed isolated ledgers.",
            "- Continue with bounded paper-equivalent parameter search.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    payload = build_payload()
    _write_json(OUTPUT_JSON, payload)
    report = write_report(payload)
    print(json.dumps({"status": payload["status"], "output": str(OUTPUT_JSON), "report": str(report)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
