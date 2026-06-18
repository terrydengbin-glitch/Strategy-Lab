from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.strategy_sandbox.full_pipeline import run_sandbox_full_pipeline
from laoma_signal_engine.strategy_sandbox.resource_governor import (
    finish_ui_sandbox_pipeline_context,
    start_ui_sandbox_pipeline_context,
)
from laoma_signal_engine.strategy_sandbox.service import active_sandbox_payload
from scripts.step28_2_bounded_paper_equivalent_parameter_search import (
    GATE_FEATURE_REPAIR_LINES,
    _candles_for_orders,
    _materialize_gate_search_features,
    _repaired_gate_features,
    _shadow_orders,
)
from scripts.step28_4_parameter_gate_joint_paper_equivalent_validation import (
    _backup_gate_config,
    _connect,
    _p21_candidate,
    _restore_gate_config,
)
from scripts.step7_146_strategy5_6_v5_gate_paper_equivalent_backtest import _trade_plan_doc


TASK_ID = "STEP34.10"
SCHEMA_VERSION = "step34.10.baseline-s5-s6-ui-sandbox-replay.v1"
STEP28_4_JSON = ROOT / "DATA" / "backtest" / "step28" / "step28_4_parameter_gate_joint_paper_equivalent_validation.json"
STEP28_3_JSON = ROOT / "DATA" / "backtest" / "step28" / "step28_3_trade_quality_fast_gate_candidate_search.json"
OUT_DIR = ROOT / "DATA" / "sandboxes" / "experiments" / "step34_10"
REPORT_DIR = ROOT / "docs" / "reports"
GATE_CONFIG = ROOT / "DATA" / "paper" / "v5_trade_gate_experiment.json"
TARGET_LINES = ("strategy5", "strategy6")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _loads(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _rel(path: str | Path | None) -> str | None:
    if not path:
        return None
    got = Path(path)
    if not got.is_absolute():
        got = ROOT / got
    try:
        return got.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return got.as_posix()


def _candle_to_dict(candle: Any) -> dict[str, Any]:
    if isinstance(candle, dict):
        return dict(candle)
    return {
        "symbol": str(getattr(candle, "symbol")),
        "open_time_ms": int(getattr(candle, "open_time_ms")),
        "open": float(getattr(candle, "open")),
        "high": float(getattr(candle, "high")),
        "low": float(getattr(candle, "low")),
        "close": float(getattr(candle, "close")),
        "volume": float(getattr(candle, "volume", 0.0)),
    }


def _gate_candidate_lookup() -> dict[tuple[str, str, str], dict[str, Any]]:
    step28_3 = _loads(STEP28_3_JSON)
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in step28_3.get("leaderboard") or []:
        key = (str(row.get("strategy_line")), str(row.get("parameter_set_id")), str(row.get("gate_candidate_id")))
        out[key] = row
    for line_result in step28_3.get("line_results") or []:
        for row in line_result.get("candidates") or []:
            key = (str(row.get("strategy_line")), str(row.get("parameter_set_id")), str(row.get("gate_candidate_id")))
            out.setdefault(key, row)
    return out


def _selected_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    step28_4 = _loads(STEP28_4_JSON)
    best = step28_4.get("per_strategy_best") or {}
    gate_lookup = _gate_candidate_lookup()
    selected: list[dict[str, Any]] = []
    for line in TARGET_LINES:
        row = dict(best.get(line) or {})
        if not row:
            continue
        key = (line, str(row.get("parameter_set_id")), str(row.get("gate_candidate_id")))
        gate_row = dict(gate_lookup.get(key) or {})
        p21 = _p21_candidate(conn, line, str(row.get("parameter_set_id")))
        row["gate_rule_json"] = row.get("gate_rule_json") or gate_row.get("rule_json") or {}
        row["source_step28_3_candidate"] = gate_row
        row["p21_candidate"] = p21
        selected.append(row)
    return selected


def _write_combined_gate_config(candidates: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
    rules: dict[str, Any] = {}
    for row in candidates:
        line = str(row["strategy_line"])
        rules[line] = {
            "parameter_set_id": row.get("parameter_set_id"),
            "gate_candidate_id": row.get("gate_candidate_id"),
            "action": "block",
            "rule_json": row.get("gate_rule_json") or {},
            "evidence": {
                "source_step": "STEP28.4",
                "baseline_paper_equivalent_run_id": row.get("paper_equivalent_run_id"),
                "baseline_paper_equivalent_profit_factor": row.get("paper_equivalent_profit_factor"),
                "promotion_block_reason": row.get("promotion_block_reason"),
            },
        }
    cfg = {
        "enabled": True,
        "experiment_id": f"{TASK_ID}_{run_id}",
        "paper_epoch_id": f"{TASK_ID}_{run_id}_epoch",
        "line_epochs": {line: f"{TASK_ID}_{run_id}_{line}" for line in rules},
        "mode": "step34_10_ui_sandbox_replay",
        "feature_missing_policy": "block",
        "rules": rules,
    }
    _write_json(GATE_CONFIG, cfg)
    return cfg


def _candidate_doc(
    conn: sqlite3.Connection,
    *,
    candidate: dict[str, Any],
    run_id: str,
    cycle_id: str,
    generated_at: str,
) -> tuple[dict[str, Any] | None, dict[str, list[Any]], dict[str, Any]]:
    line = str(candidate["strategy_line"])
    p21 = candidate.get("p21_candidate")
    if not isinstance(p21, dict):
        return None, {}, {"status": "blocked", "reason": "missing_p21_candidate"}
    orders = _shadow_orders(
        conn,
        experiment_id=str(p21["experiment_id"]),
        parameter_set_id=str(candidate["parameter_set_id"]),
        strategy_line=line,
        max_orders=20,
    )
    candles = _candles_for_orders(conn, orders)
    usable = [order for order in orders if candles.get(str(order.get("symbol") or "").upper())]
    if not usable:
        return None, {}, {
            "status": "blocked",
            "reason": "no_usable_shadow_order_with_candles",
            "shadow_orders": len(orders),
            "symbols_with_candles": sorted(candles),
        }
    plans: list[dict[str, Any]] = []
    source_order_ids: list[str] = []
    feature_audits: list[dict[str, Any]] = []
    candles_out: dict[str, list[dict[str, Any]]] = {}
    base_doc: dict[str, Any] | None = None
    for order in usable[:20]:
        features = order.get("features") if isinstance(order.get("features"), dict) else {}
        feature_audit: dict[str, Any] = {"enabled": False}
        if line in GATE_FEATURE_REPAIR_LINES:
            features, feature_audit = _repaired_gate_features(
                conn,
                candidate=candidate,
                order=order,
                generated_at=generated_at,
                fallback=features,
            )
        features = _materialize_gate_search_features(features, order)
        one_doc = _trade_plan_doc(
            root=ROOT,
            line=line,
            order=order,
            features=features,
            run_id=f"{run_id}_{line}_{order.get('order_id')}",
            cycle_id=f"{cycle_id}_{line}_{order.get('order_id')}",
            generated_at=generated_at,
        )
        base_doc = base_doc or one_doc
        plans.extend(one_doc.get("plans") or [])
        source_order_ids.append(str(order.get("order_id")))
        feature_audits.append({"source_order_id": order.get("order_id"), "feature_audit": feature_audit})
        symbol = str(order["symbol"]).upper()
        candles_out[symbol] = [_candle_to_dict(candle) for candle in candles[symbol]]
    if not base_doc or not plans:
        return None, {}, {"status": "blocked", "reason": "no_trade_plans_materialized", "usable_orders": len(usable)}
    doc = {
        **base_doc,
        "run_id": f"{run_id}_{line}",
        "cycle_id": f"{cycle_id}_{line}",
        "count": len(plans),
        "executable_count": len(plans),
        "plans": plans,
        "input_refs": {
            **(base_doc.get("input_refs") if isinstance(base_doc.get("input_refs"), dict) else {}),
            "source_order_ids": source_order_ids,
            "source_order_count": len(source_order_ids),
            "baseline_leaderboard_replay_task": TASK_ID,
        },
    }
    return doc, candles_out, {
        "status": "ok",
        "source_order_count": len(source_order_ids),
        "source_order_ids": source_order_ids,
        "symbols": sorted(candles_out),
        "feature_audits": feature_audits,
    }


def _sidecar_counts(run_id: str) -> dict[str, Any]:
    db = ROOT / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db"
    if not db.exists():
        return {"exists": False}
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        samples = conn.execute(
            "SELECT source_mode, COUNT(*) AS c FROM trade_training_samples WHERE sample_id LIKE ? GROUP BY source_mode",
            (f"{run_id}:%",),
        ).fetchall()
        manifests = conn.execute(
            "SELECT run_id, source_mode, coverage_json FROM trade_snapshot_manifests WHERE run_id=? ORDER BY created_at DESC",
            (run_id,),
        ).fetchall()
    return {
        "exists": True,
        "db_path": _rel(db),
        "sample_source_modes": {str(row["source_mode"]): int(row["c"]) for row in samples},
        "manifest_count": len(manifests),
        "manifests": [dict(row) for row in manifests],
    }


def _paper_counts(path: str | None) -> dict[str, int]:
    if not path:
        return {}
    db = ROOT / path
    if not db.exists():
        return {"missing": 1}
    out: dict[str, int] = {}
    with sqlite3.connect(db) as conn:
        for table in ("paper_orders", "paper_fills", "trade_quality_samples", "paper_skip_ledger"):
            out[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    return out


def _write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"STEP34.10_baseline_s5_s6_ui_sandbox_replay_{_stamp()}.md"
    rows = payload.get("selected_candidates") or []
    result = payload.get("execution_result") or {}
    training = result.get("training_dataset") if isinstance(result.get("training_dataset"), dict) else {}
    tq = result.get("trade_quality_completion") if isinstance(result.get("trade_quality_completion"), dict) else {}
    lines = [
        "# STEP34.10 Baseline S5/S6 Leaderboard Config UI Sandbox Replay",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- status: `{payload.get('status')}`",
        f"- sandbox_id: `{payload.get('sandbox_id')}`",
        f"- run_id: `{payload.get('run_id')}`",
        f"- output_json: `{_rel(payload.get('output_json'))}`",
        "",
        "## Selected Candidates",
        "",
        "| strategy | parameter_set_id | gate_candidate_id | baseline PF | baseline run | block reason |",
        "| --- | --- | --- | ---: | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                row.get("strategy_line"),
                row.get("parameter_set_id"),
                row.get("gate_candidate_id"),
                row.get("paper_equivalent_profit_factor"),
                row.get("paper_equivalent_run_id"),
                row.get("promotion_block_reason"),
            )
        )
    lines.extend(
        [
            "",
            "## Replay Result",
            "",
            f"- artifact_manifest_path: `{result.get('artifact_manifest_path')}`",
            f"- paper_db_path: `{result.get('paper_db_path')}`",
            f"- paper_counts: `{payload.get('paper_counts')}`",
            f"- trade_quality_completion_status: `{tq.get('trade_quality_completion_status')}`",
            f"- training_dataset_status: `{training.get('training_dataset_status')}`",
            f"- training_dataset_source_mode: `{training.get('source_mode') or training.get('training_dataset_source_mode')}`",
            f"- training_dataset_manifest_path: `{training.get('training_dataset_manifest_path')}`",
            f"- training_export_dir: `{training.get('training_export_dir')}`",
            f"- sidecar_counts: `{payload.get('sidecar_counts')}`",
            f"- baseline_gate_config_restored: `{payload.get('baseline_gate_config_restored')}`",
            "",
            "## Boundary",
            "",
            "- 本实验只复制 baseline leaderboard 配置到 UI sandbox run；不 promotion。",
            "- V5 gate experiment config 已在 finally 中恢复。",
            "- 结果用于链路验证，不作为长期收益结论。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    # The service `root` parameter is the sandbox registry root, not the
    # project root. Keep default semantics aligned with FastAPI/CLI.
    active = active_sandbox_payload()
    sandbox_id = str(active.get("active_sandbox_id") or "")
    if not sandbox_id:
        print(json.dumps({"status": "blocked", "reason": "active_sandbox_missing"}, ensure_ascii=False))
        return 2
    run_id = f"step34_10_s5_s6_{_stamp()}"
    cycle_id = f"cycle_{run_id}"
    generated_at = _now()
    backup = _backup_gate_config()
    context: dict[str, Any] | None = None
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "generated_at": generated_at,
        "sandbox_id": sandbox_id,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "source_step28_4": _rel(STEP28_4_JSON),
    }
    try:
        with _connect() as conn:
            candidates = _selected_candidates(conn)
            if len(candidates) != 2:
                payload.update({"status": "blocked", "reason": "missing_s5_or_s6_candidate", "selected_candidates": candidates})
                return_code = 2
            else:
                gate_config = _write_combined_gate_config(candidates, run_id)
                docs: dict[str, dict[str, Any]] = {}
                candles_by_symbol: dict[str, list[Any]] = {}
                materialization: dict[str, Any] = {}
                for candidate in candidates:
                    doc, candles, audit = _candidate_doc(conn, candidate=candidate, run_id=run_id, cycle_id=cycle_id, generated_at=generated_at)
                    materialization[str(candidate["strategy_line"])] = audit
                    if doc:
                        docs[str(candidate["strategy_line"])] = doc
                    candles_by_symbol.update(candles)
                candidate_snapshot = {
                    "schema_version": "step34.10.selected-baseline-candidates.v1",
                    "sandbox_id": sandbox_id,
                    "run_id": run_id,
                    "generated_at": generated_at,
                    "selected_candidates": candidates,
                    "gate_config": gate_config,
                    "materialization": materialization,
                }
                snapshot_path = OUT_DIR / f"{run_id}_baseline_s5_s6_candidates.json"
                _write_json(snapshot_path, candidate_snapshot)
                if not docs:
                    payload.update(
                        {
                            "status": "blocked",
                            "reason": "no_docs_materialized",
                            "selected_candidates": candidates,
                            "materialization": materialization,
                            "candidate_snapshot_path": _rel(snapshot_path),
                        }
                    )
                    return_code = 2
                else:
                    context = start_ui_sandbox_pipeline_context(
                        project_root=ROOT,
                        sandbox_id=sandbox_id,
                        active_sandbox_id=sandbox_id,
                        caller_surface="codex",
                        caller_type="local_ui",
                        dry_run=False,
                        requires_live_rest=False,
                        cache_hit=True,
                        options={
                            "run_id": run_id,
                            "cycle_id": cycle_id,
                            "strategy_lines": ",".join(sorted(docs)),
                            "pipeline_mode": "sandbox_full_pipeline",
                            "training_dataset_id": f"{TASK_ID}_{run_id}",
                        },
                    )
                    if not context.get("accepted"):
                        payload.update({"status": "blocked", "reason": "ui_lane_not_accepted", "resource_context": context})
                        return_code = 2
                    else:
                        result = run_sandbox_full_pipeline(
                            ROOT,
                            sandbox_id=sandbox_id,
                            run_id=run_id,
                            cycle_id=cycle_id,
                            writer_context=context.get("writer_context") or {},
                            options={
                                "pipeline_mode": "sandbox_full_pipeline",
                                "docs": docs,
                                "candles_by_symbol": candles_by_symbol,
                                "max_ticks": None,
                                "baseline_leaderboard_config_snapshot_path": _rel(snapshot_path),
                            },
                        )
                        payload.update(
                            {
                                "status": result.get("status") or "completed",
                                "selected_candidates": candidates,
                                "candidate_snapshot_path": _rel(snapshot_path),
                                "materialization": materialization,
                                "resource_context": context,
                                "execution_result": result,
                                "paper_counts": _paper_counts(result.get("paper_db_path")),
                                "sidecar_counts": _sidecar_counts(run_id),
                            }
                        )
                        return_code = 0
        return return_code
    finally:
        _restore_gate_config(backup)
        payload["baseline_gate_config_restored"] = True
        if context and context.get("accepted"):
            finish_ui_sandbox_pipeline_context(
                project_root=ROOT,
                run_id=run_id,
                sandbox_id=sandbox_id,
                status=str(payload.get("status") or "completed"),
                result=payload.get("execution_result") if isinstance(payload.get("execution_result"), dict) else payload,
            )
        payload["sidecar_counts_after_restore"] = _sidecar_counts(run_id)
        output_json = OUT_DIR / f"{run_id}_result.json"
        payload["output_json"] = str(output_json)
        _write_json(output_json, payload)
        report = _write_report(payload)
        payload["report"] = _rel(report)
        _write_json(output_json, payload)
        print(json.dumps({"status": payload.get("status"), "output_json": _rel(output_json), "report": _rel(report)}, ensure_ascii=False))


if __name__ == "__main__":
    raise SystemExit(main())
