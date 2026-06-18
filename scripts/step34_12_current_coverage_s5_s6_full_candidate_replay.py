from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21_v2 import (
    download_kline_cache_payload,
    kline_cache_status_payload,
    run_config_matrix_streaming_payload,
    universe_symbols,
)
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
)
from scripts.step28_4_parameter_gate_joint_paper_equivalent_validation import (
    _backup_gate_config,
    _connect,
    _p21_candidate,
    _restore_gate_config,
)
from scripts.step34_10_baseline_s5_s6_leaderboard_config_ui_sandbox_replay import (
    STEP28_4_JSON,
    _gate_candidate_lookup,
    _rel,
)
from scripts.step7_146_strategy5_6_v5_gate_paper_equivalent_backtest import _trade_plan_doc


TASK_ID = "STEP34.12"
SCHEMA_VERSION = "step34.12.current-coverage-s5-s6-full-candidate-replay.v1"
TARGET_LINES = ("strategy5", "strategy6")
OUT_DIR = ROOT / "DATA" / "sandboxes" / "experiments" / "step34_12"
REPORT_DIR = ROOT / "docs" / "reports"
GATE_CONFIG = ROOT / "DATA" / "paper" / "v5_trade_gate_experiment.json"
PROGRESS_PATH = ROOT / "DATA" / "sandboxes" / "experiments" / "step34_12" / "step34_12_progress.json"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _loads(raw: Any, default: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    if raw in (None, ""):
        return default
    try:
        return json.loads(str(raw))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _update_progress(**updates: Any) -> None:
    current: dict[str, Any] = {}
    if PROGRESS_PATH.exists():
        try:
            current = json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
        except Exception:
            current = {}
    current.update(updates)
    current["updated_at"] = _now()
    _write_json(PROGRESS_PATH, current)


def _selected_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    step28_4 = _read_json(STEP28_4_JSON)
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
                "source_step": TASK_ID,
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
        "mode": "step34_12_current_coverage_full_candidate_replay",
        "feature_missing_policy": "block",
        "rules": rules,
    }
    _write_json(GATE_CONFIG, cfg)
    return cfg


def _coverage_audit(*, days: int, max_symbols: int | None = None) -> dict[str, Any]:
    all_symbols = universe_symbols(ROOT, limit=max_symbols)
    status = kline_cache_status_payload(ROOT, symbols=all_symbols, days=days, max_symbols=len(all_symbols))
    rows = status.get("symbols") or []
    ready = [str(row["symbol"]) for row in rows if row.get("status") == "ready"]
    missing = [str(row["symbol"]) for row in rows if row.get("status") != "ready"]
    cached = [str(row["symbol"]) for row in rows if int(row.get("row_count") or 0) > 0]
    coverage_values = [float(row.get("coverage") or 0.0) for row in rows]
    return {
        "schema_version": "step34.12.kline-coverage-audit.v1",
        "days": days,
        "universe_target_count": len(all_symbols),
        "universe_cached_count": len(cached),
        "universe_ready_count": len(ready),
        "universe_missing_count": len(missing),
        "kline_coverage_rate": round(len(ready) / len(all_symbols), 8) if all_symbols else 0.0,
        "avg_symbol_coverage": round(sum(coverage_values) / len(coverage_values), 8) if coverage_values else 0.0,
        "full_universe_500_missing": len(all_symbols) < 500,
        "ready_symbols": ready,
        "missing_symbols": missing,
        "status": status,
        "generated_at": _now(),
    }


def _download_missing(symbols: list[str], *, days: int, batch_size: int, sleep_sec: float) -> dict[str, Any]:
    batches = [symbols[i : i + max(1, batch_size)] for i in range(0, len(symbols), max(1, batch_size))]
    ledgers: list[dict[str, Any]] = []
    for index, batch in enumerate(batches, start=1):
        _update_progress(phase="download", download_batch_index=index, download_batch_total=len(batches), current_symbols=batch)
        payload = download_kline_cache_payload(
            ROOT,
            symbols=batch,
            days=days,
            max_symbols=len(batch),
            dry_run=False,
            sleep_sec=sleep_sec,
        )
        ledgers.extend(payload.get("ledger") or [])
        print(json.dumps({"phase": "download", "batch": index, "total": len(batches), "symbols": batch, "ledger": payload.get("ledger")}, ensure_ascii=False), flush=True)
        time.sleep(max(0.0, sleep_sec))
    return {
        "downloaded_symbol_count": len({str(row.get("symbol")) for row in ledgers if row.get("status") == "ok"}),
        "download_error_count": sum(1 for row in ledgers if row.get("status") not in {"ok", "dry_run"}),
        "ledger": ledgers,
    }


def _run_shadow_matrix(
    *,
    candidate: dict[str, Any],
    ready_symbols: list[str],
    days: int,
    symbol_shard_size: int,
    max_workers: int,
    resume_experiment_id: str | None = None,
) -> dict[str, Any]:
    line = str(candidate["strategy_line"])
    p21 = candidate.get("p21_candidate") if isinstance(candidate.get("p21_candidate"), dict) else {}
    params = dict(p21.get("parameters") or {})
    if not params:
        return {"status": "blocked", "reason": "missing_parameter_grid", "strategy_line": line}
    params["strategy_line"] = line
    parameter_set_id = str(candidate["parameter_set_id"])
    grid = [{"parameter_set_id": parameter_set_id, "parameters": params}]
    last_print = {"done": -1, "at": 0.0}

    def cb(progress: dict[str, Any]) -> None:
        payload = {
            "phase": "shadow_matrix",
            "strategy_line": line,
            **dict(progress),
        }
        _update_progress(**payload)
        done = int(payload.get("done_count") or 0)
        total = int(payload.get("total_count") or 0)
        now_ts = time.time()
        if done == total or done != last_print["done"] and now_ts - last_print["at"] >= 20:
            last_print["done"] = done
            last_print["at"] = now_ts
            print(json.dumps({"phase": "shadow_matrix", "strategy_line": line, "done_count": done, "total_count": total}, ensure_ascii=False), flush=True)

    payload = run_config_matrix_streaming_payload(
        ROOT,
        symbols=ready_symbols,
        strategy_line=line,
        days=days,
        max_symbols=len(ready_symbols),
        max_sets=1,
        parameter_grid=grid,
        write=True,
        symbol_shard_size=symbol_shard_size,
        max_workers=max_workers,
        scheduler_mode="global_queue",
        resume_experiment_id=resume_experiment_id,
        progress_callback=cb,
    )
    return payload


def _shadow_orders(
    conn: sqlite3.Connection,
    *,
    experiment_id: str,
    parameter_set_id: str,
    strategy_line: str,
    symbols: list[str] | None = None,
) -> list[dict[str, Any]]:
    args: list[Any] = [experiment_id, parameter_set_id, strategy_line]
    where = "experiment_id = ? AND parameter_set_id = ? AND strategy_line = ?"
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        where += f" AND symbol IN ({placeholders})"
        args.extend([s.upper() for s in symbols])
    rows = conn.execute(
        f"""
        SELECT *
        FROM p21_v2_shadow_orders
        WHERE {where}
        ORDER BY signal_time_ms ASC, order_id ASC
        """,
        args,
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["reasons"] = _loads(item.pop("reasons_json", None), [])
        item["features"] = _loads(item.pop("features_json", None), {})
        item["config_patch"] = _loads(item.pop("config_patch_json", None), {})
        item["trade_plan_payload"] = _loads(item.pop("trade_plan_payload_json", None), {})
        item["fill_result"] = _loads(item.pop("fill_result_json", None), {})
        item["fast_exit_policy"] = _loads(item.pop("fast_exit_policy_json", None), {})
        out.append(item)
    return out


def _symbol_shards(symbols: list[str], size: int) -> list[list[str]]:
    size = max(1, int(size or 25))
    return [symbols[i : i + size] for i in range(0, len(symbols), size)]


def _candidate_doc(
    conn: sqlite3.Connection,
    *,
    candidate: dict[str, Any],
    experiment_id: str,
    symbols: list[str],
    run_id: str,
    cycle_id: str,
    generated_at: str,
) -> tuple[dict[str, Any] | None, dict[str, list[Any]], dict[str, Any]]:
    line = str(candidate["strategy_line"])
    parameter_set_id = str(candidate["parameter_set_id"])
    orders = _shadow_orders(
        conn,
        experiment_id=experiment_id,
        parameter_set_id=parameter_set_id,
        strategy_line=line,
        symbols=symbols,
    )
    candles = _candles_for_orders(conn, orders)
    usable = [order for order in orders if candles.get(str(order.get("symbol") or "").upper())]
    missing = sorted({str(order.get("symbol") or "").upper() for order in orders if not candles.get(str(order.get("symbol") or "").upper())})
    if not usable:
        return None, {}, {
            "status": "blocked",
            "reason": "no_usable_shadow_order_with_candles",
            "shadow_orders": len(orders),
            "missing_candle_symbols": missing[:50],
        }
    plans: list[dict[str, Any]] = []
    source_order_ids: list[str] = []
    feature_repair_counter: Counter[str] = Counter()
    feature_unavailable_counter: Counter[str] = Counter()
    candles_out: dict[str, list[dict[str, Any]]] = {}
    base_doc: dict[str, Any] | None = None
    for order in usable:
        features = order.get("features") if isinstance(order.get("features"), dict) else {}
        if line in GATE_FEATURE_REPAIR_LINES:
            features, feature_audit = _repaired_gate_features(
                conn,
                candidate={**candidate, "experiment_id": experiment_id},
                order=order,
                generated_at=generated_at,
                fallback=features,
            )
            feature_repair_counter.update(feature_audit.get("status_counts") or {})
            feature_unavailable_counter.update(feature_audit.get("unavailable_fields") or [])
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
        symbol = str(order["symbol"]).upper()
        if symbol not in candles_out:
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
            "full_replay_experiment_id": experiment_id,
        },
    }
    return doc, candles_out, {
        "status": "ok",
        "shadow_order_count": len(orders),
        "source_order_count": len(source_order_ids),
        "missing_candle_symbols": missing[:50],
        "symbols": sorted(candles_out),
        "feature_repair_counts": dict(feature_repair_counter),
        "feature_unavailable_counts": dict(feature_unavailable_counter),
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


def _pipeline_run_dir(*, sandbox_id: str, run_id: str) -> Path:
    return ROOT / "DATA" / "sandboxes" / str(sandbox_id) / "runtime" / "pipeline_runs" / str(run_id)


def _paper_stage(manifest: dict[str, Any]) -> dict[str, Any]:
    for stage in manifest.get("stages") or []:
        if isinstance(stage, dict) and stage.get("stage_name") == "paper":
            return stage
    return {}


def _load_completed_replay_shard(
    *,
    sandbox_id: str,
    parent_run_id: str,
    shard_index: int,
    shard_symbols: list[str],
) -> dict[str, Any] | None:
    run_id = f"{parent_run_id}_shard{shard_index:03d}"
    manifest_path = _pipeline_run_dir(sandbox_id=sandbox_id, run_id=run_id) / "artifact_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = _read_json(manifest_path)
    except Exception:
        return None
    paper_stage = _paper_stage(manifest)
    result = paper_stage.get("result") if isinstance(paper_stage.get("result"), dict) else {}
    candidate = result.get("candidate_ledger") if isinstance(result.get("candidate_ledger"), dict) else {}
    training = result.get("training_dataset") if isinstance(result.get("training_dataset"), dict) else {}
    if (
        manifest.get("status") != "completed"
        or paper_stage.get("stage_status") != "completed"
        or candidate.get("candidate_ledger_status") != "completed"
        or not training.get("training_export_dir")
    ):
        return None
    return {
        "status": "completed_existing",
        "run_id": run_id,
        "cycle_id": manifest.get("cycle_id"),
        "symbols": shard_symbols,
        "candidate_count": int(candidate.get("candidate_count") or 0),
        "paper_counts": _paper_counts(result.get("paper_db_path")),
        "execution_result": result,
        "artifact_manifest_path": _rel(manifest_path),
        "resume_reason": "existing_completed_shard_reused",
    }


def _replay_resume_plan(*, sandbox_id: str, parent_run_id: str, shards: list[list[str]]) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    for index, symbols in enumerate(shards, start=1):
        existing = _load_completed_replay_shard(
            sandbox_id=sandbox_id,
            parent_run_id=parent_run_id,
            shard_index=index,
            shard_symbols=symbols,
        )
        plan.append(
            {
                "shard_index": index,
                "run_id": f"{parent_run_id}_shard{index:03d}",
                "status": "completed_existing" if existing else "pending",
                "candidate_count": (existing or {}).get("candidate_count"),
                "symbols": symbols,
            }
        )
    return plan


def _run_replay_shard(
    *,
    sandbox_id: str,
    parent_run_id: str,
    shard_index: int,
    shard_total: int,
    docs: dict[str, dict[str, Any]],
    candles_by_symbol: dict[str, list[Any]],
    candidates: list[dict[str, Any]],
    gate_config_snapshot_path: str,
) -> dict[str, Any]:
    run_id = f"{parent_run_id}_shard{shard_index:03d}"
    cycle_id = f"cycle_{run_id}"
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
            "parent_run_id": parent_run_id,
            "shard_index": shard_index,
            "shard_total": shard_total,
        },
    )
    if not context.get("accepted"):
        return {"status": "blocked", "reason": "ui_lane_not_accepted", "run_id": run_id, "resource_context": context}
    try:
        result = run_sandbox_full_pipeline(
            ROOT,
            sandbox_id=sandbox_id,
            run_id=run_id,
            cycle_id=cycle_id,
            writer_context=context.get("writer_context") or {},
            options={
                "pipeline_mode": "sandbox_full_pipeline",
                "training_source_mode": "ui_sandbox_full_pipeline",
                "docs": docs,
                "candles_by_symbol": candles_by_symbol,
                "max_ticks": None,
                "baseline_leaderboard_config_snapshot_path": gate_config_snapshot_path,
                "parent_run_id": parent_run_id,
                "shard_index": shard_index,
                "shard_total": shard_total,
                "selected_candidates": candidates,
            },
        )
        return {
            "status": result.get("status") or "completed",
            "run_id": run_id,
            "cycle_id": cycle_id,
            "resource_context": context,
            "execution_result": result,
            "paper_counts": _paper_counts(result.get("paper_db_path")),
        }
    finally:
        finish_ui_sandbox_pipeline_context(
            project_root=ROOT,
            run_id=run_id,
            sandbox_id=sandbox_id,
            status="completed",
            result={"parent_run_id": parent_run_id, "shard_index": shard_index},
        )


def _write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"STEP34.12_current_coverage_s5_s6_full_candidate_replay_{_stamp()}.md"
    lines = [
        "# STEP34.12 Current-Coverage S5/S6 Full Candidate Replay",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- status: `{payload.get('status')}`",
        f"- sandbox_id: `{payload.get('sandbox_id')}`",
        f"- parent_run_id: `{payload.get('parent_run_id')}`",
        f"- output_json: `{_rel(payload.get('output_json'))}`",
        f"- progress_path: `{_rel(PROGRESS_PATH)}`",
        "",
        "## Coverage",
        "",
    ]
    coverage = payload.get("coverage_after_download") or payload.get("coverage_before_download") or {}
    lines.extend(
        [
            f"- universe_target_count: `{coverage.get('universe_target_count')}`",
            f"- universe_ready_count: `{coverage.get('universe_ready_count')}`",
            f"- universe_missing_count: `{coverage.get('universe_missing_count')}`",
            f"- kline_coverage_rate: `{coverage.get('kline_coverage_rate')}`",
            f"- avg_symbol_coverage: `{coverage.get('avg_symbol_coverage')}`",
            f"- full_universe_500_missing: `{coverage.get('full_universe_500_missing')}`",
            "",
            "## Shadow Matrix",
            "",
            "| strategy | experiment_id | parameter_set_id | trade_count | status |",
            "| --- | --- | --- | ---: | --- |",
        ]
    )
    for row in payload.get("shadow_matrix_results") or []:
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                row.get("strategy_line"),
                row.get("experiment_id"),
                row.get("parameter_set_id"),
                row.get("trade_count"),
                row.get("status"),
            )
        )
    lines.extend(
        [
            "",
            "## Replay Totals",
            "",
            f"- shard_count: `{len(payload.get('shards') or [])}`",
            f"- candidate_count: `{payload.get('totals', {}).get('candidate_count')}`",
            f"- paper_orders: `{payload.get('totals', {}).get('paper_orders')}`",
            f"- paper_fills: `{payload.get('totals', {}).get('paper_fills')}`",
            f"- trade_quality_samples: `{payload.get('totals', {}).get('trade_quality_samples')}`",
            f"- paper_skip_ledger: `{payload.get('totals', {}).get('paper_skip_ledger')}`",
            "",
            "## AI Training Handoff",
            "",
            "- 每个 shard 的 `execution_result.training_dataset.training_export_dir` 是训练数据 export 目录。",
            "- 每个 shard 的 `execution_result.candidate_ledger.candidate_ledger_dir` 是 candidate/gate/result sidecar 目录。",
            "- `training_ready` 仍由 P29 判定；本任务不伪造 readiness。",
            "",
            "## Boundary",
            "",
            "- 本任务不 promotion，不修改 baseline production config。",
            "- 若下载失败或 coverage 不满，报告保留缺口，不把 current coverage 冒充 full universe。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--max-symbols", type=int, default=0, help="0 = full universe")
    parser.add_argument("--skip-download", action="store_true", default=False)
    parser.add_argument("--download-batch-size", type=int, default=5)
    parser.add_argument("--download-sleep-sec", type=float, default=0.05)
    parser.add_argument("--matrix-shard-size", type=int, default=25)
    parser.add_argument("--matrix-workers", type=int, default=1)
    parser.add_argument("--replay-shard-size", type=int, default=25)
    parser.add_argument("--dry-run-coverage", action="store_true", default=False)
    parser.add_argument("--resume-strategy5-experiment-id", default="")
    parser.add_argument("--resume-strategy6-experiment-id", default="")
    parser.add_argument("--parent-run-id", default="", help="Reuse an existing STEP34.12 parent run id for replay resume.")
    parser.add_argument("--resume-replay", action="store_true", default=False, help="Skip replay shards that already have completed sandbox artifacts.")
    parser.add_argument("--resume-replay-from-shard", type=int, default=0, help="Optional guard: require shards before this index to exist before continuing.")
    parser.add_argument("--dry-run-replay-resume-plan", action="store_true", default=False)
    args = parser.parse_args()

    active = active_sandbox_payload()
    sandbox_id = str(active.get("active_sandbox_id") or "")
    if not sandbox_id:
        print(json.dumps({"status": "blocked", "reason": "active_sandbox_missing"}, ensure_ascii=False))
        return 2

    parent_run_id = str(args.parent_run_id or "").strip() or f"step34_12_s5_s6_full_{_stamp()}"
    generated_at = _now()
    max_symbols = args.max_symbols if args.max_symbols and args.max_symbols > 0 else None
    backup = _backup_gate_config()
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "generated_at": generated_at,
        "sandbox_id": sandbox_id,
        "parent_run_id": parent_run_id,
        "request": vars(args),
        "source_step28_4": _rel(STEP28_4_JSON),
    }
    return_code = 0
    try:
        coverage_before = _coverage_audit(days=args.days, max_symbols=max_symbols)
        payload["coverage_before_download"] = coverage_before
        _update_progress(status="running", phase="coverage_before_download", parent_run_id=parent_run_id, coverage=coverage_before)
        print(json.dumps({"phase": "coverage_before_download", "ready": coverage_before["universe_ready_count"], "target": coverage_before["universe_target_count"], "missing": coverage_before["universe_missing_count"]}, ensure_ascii=False), flush=True)
        if args.dry_run_coverage:
            payload["status"] = "coverage_only"
            return 0
        download_result: dict[str, Any] = {"skipped": bool(args.skip_download)}
        if not args.skip_download and coverage_before["missing_symbols"]:
            download_result = _download_missing(
                coverage_before["missing_symbols"],
                days=args.days,
                batch_size=args.download_batch_size,
                sleep_sec=args.download_sleep_sec,
            )
        payload["download_result"] = download_result
        coverage_after = _coverage_audit(days=args.days, max_symbols=max_symbols)
        payload["coverage_after_download"] = coverage_after
        ready_symbols = list(coverage_after.get("ready_symbols") or [])
        if not ready_symbols:
            payload.update({"status": "blocked", "reason": "no_ready_symbols_after_download"})
            return 2

        with _connect() as conn:
            selected = _selected_candidates(conn)
        if len(selected) != 2:
            payload.update({"status": "blocked", "reason": "missing_s5_or_s6_candidate", "selected_candidates": selected})
            return 2

        gate_config = _write_combined_gate_config(selected, parent_run_id)
        snapshot_path = OUT_DIR / f"{parent_run_id}_selected_candidates.json"
        _write_json(snapshot_path, {"selected_candidates": selected, "gate_config": gate_config, "coverage": coverage_after, "generated_at": _now()})
        payload["candidate_snapshot_path"] = _rel(snapshot_path)
        payload["selected_candidates"] = selected

        matrix_results: list[dict[str, Any]] = []
        resume_map = {
            "strategy5": str(args.resume_strategy5_experiment_id or "").strip() or None,
            "strategy6": str(args.resume_strategy6_experiment_id or "").strip() or None,
        }
        for candidate in selected:
            matrix = _run_shadow_matrix(
                candidate=candidate,
                ready_symbols=ready_symbols,
                days=args.days,
                symbol_shard_size=args.matrix_shard_size,
                max_workers=args.matrix_workers,
                resume_experiment_id=resume_map.get(str(candidate.get("strategy_line"))),
            )
            matrix_results.append(
                {
                    "strategy_line": candidate.get("strategy_line"),
                    "parameter_set_id": candidate.get("parameter_set_id"),
                    "experiment_id": matrix.get("experiment_id"),
                    "trade_count": matrix.get("trade_count"),
                    "status": matrix.get("status") or "completed",
                    "best": matrix.get("best"),
                    "raw": matrix,
                }
            )
        payload["shadow_matrix_results"] = matrix_results

        shards = _symbol_shards(ready_symbols, args.replay_shard_size)
        resume_plan = _replay_resume_plan(sandbox_id=sandbox_id, parent_run_id=parent_run_id, shards=shards)
        payload["replay_resume_plan"] = resume_plan
        if args.dry_run_replay_resume_plan:
            payload["status"] = "resume_plan_only"
            payload["shards"] = [row for row in resume_plan if row.get("status") == "completed_existing"]
            payload["totals"] = {
                "completed_existing_shards": sum(1 for row in resume_plan if row.get("status") == "completed_existing"),
                "pending_shards": sum(1 for row in resume_plan if row.get("status") == "pending"),
            }
            return 0
        shard_results: list[dict[str, Any]] = []
        totals = Counter()
        with _connect() as conn:
            for shard_index, shard_symbols in enumerate(shards, start=1):
                existing = _load_completed_replay_shard(
                    sandbox_id=sandbox_id,
                    parent_run_id=parent_run_id,
                    shard_index=shard_index,
                    shard_symbols=shard_symbols,
                )
                if args.resume_replay and existing:
                    print(json.dumps({"phase": "replay_shard", "shard": shard_index, "total": len(shards), "status": "completed_existing"}, ensure_ascii=False), flush=True)
                    shard_results.append(existing)
                    totals["candidate_count"] += int(existing.get("candidate_count") or 0)
                    for key, value in (existing.get("paper_counts") or {}).items():
                        if isinstance(value, int):
                            totals[key] += value
                    payload["shards"] = shard_results
                    payload["totals"] = dict(totals)
                    continue
                if args.resume_replay_from_shard and shard_index < int(args.resume_replay_from_shard):
                    payload.update(
                        {
                            "status": "blocked",
                            "reason": "resume_from_shard_gap_detected",
                            "blocked_shard_index": shard_index,
                            "resume_replay_from_shard": int(args.resume_replay_from_shard),
                        }
                    )
                    return 2
                docs: dict[str, dict[str, Any]] = {}
                candles_by_symbol: dict[str, list[Any]] = {}
                materialization: dict[str, Any] = {}
                for candidate in selected:
                    matrix = next((row for row in matrix_results if row.get("strategy_line") == candidate.get("strategy_line")), {})
                    experiment_id = str(matrix.get("experiment_id") or "")
                    if not experiment_id:
                        continue
                    doc, candles, audit = _candidate_doc(
                        conn,
                        candidate=candidate,
                        experiment_id=experiment_id,
                        symbols=shard_symbols,
                        run_id=f"{parent_run_id}_shard{shard_index:03d}",
                        cycle_id=f"cycle_{parent_run_id}_shard{shard_index:03d}",
                        generated_at=generated_at,
                    )
                    materialization[str(candidate["strategy_line"])] = audit
                    if doc:
                        docs[str(candidate["strategy_line"])] = doc
                    candles_by_symbol.update(candles)
                candidate_count = sum(int((doc or {}).get("count") or 0) for doc in docs.values())
                if not docs:
                    shard_results.append(
                        {
                            "status": "skipped",
                            "reason": "no_docs_materialized_for_shard",
                            "shard_index": shard_index,
                            "symbols": shard_symbols,
                            "materialization": materialization,
                        }
                    )
                    continue
                if args.resume_replay_from_shard and shard_index < int(args.resume_replay_from_shard):
                    payload.update(
                        {
                            "status": "blocked",
                            "reason": "resume_from_shard_gap_no_docs",
                            "blocked_shard_index": shard_index,
                            "resume_replay_from_shard": int(args.resume_replay_from_shard),
                        }
                    )
                    return 2
                _update_progress(
                    phase="replay_shard",
                    parent_run_id=parent_run_id,
                    replay_shard_index=shard_index,
                    replay_shard_total=len(shards),
                    replay_shard_symbols=shard_symbols,
                    replay_shard_candidate_count=candidate_count,
                )
                print(json.dumps({"phase": "replay_shard", "shard": shard_index, "total": len(shards), "candidate_count": candidate_count}, ensure_ascii=False), flush=True)
                shard = _run_replay_shard(
                    sandbox_id=sandbox_id,
                    parent_run_id=parent_run_id,
                    shard_index=shard_index,
                    shard_total=len(shards),
                    docs=docs,
                    candles_by_symbol=candles_by_symbol,
                    candidates=selected,
                    gate_config_snapshot_path=_rel(snapshot_path) or "",
                )
                shard["symbols"] = shard_symbols
                shard["candidate_count"] = candidate_count
                shard["materialization"] = materialization
                shard_results.append(shard)
                totals["candidate_count"] += candidate_count
                for key, value in (shard.get("paper_counts") or {}).items():
                    if isinstance(value, int):
                        totals[key] += value
                payload["shards"] = shard_results
                payload["totals"] = dict(totals)
        payload["shards"] = shard_results
        payload["totals"] = dict(totals)
        payload["status"] = "completed" if shard_results else "blocked"
        if not shard_results:
            payload["reason"] = "no_replay_shards"
            return_code = 2
        return return_code
    finally:
        _restore_gate_config(backup)
        payload["baseline_gate_config_restored"] = True
        payload["completed_at"] = _now()
        if payload.get("status") is None:
            payload["status"] = "partial_interrupted" if payload.get("shards") else "failed_before_replay"
            payload["reason"] = payload.get("reason") or "runner_exited_before_completion"
        output_json = OUT_DIR / f"{parent_run_id}_result.json"
        payload["output_json"] = str(output_json)
        _write_json(output_json, payload)
        report = _write_report(payload)
        payload["report"] = _rel(report)
        _write_json(output_json, payload)
        _update_progress(status=payload.get("status"), phase="done", parent_run_id=parent_run_id, output_json=_rel(output_json), report=_rel(report))
        print(json.dumps({"status": payload.get("status"), "output_json": _rel(output_json), "report": _rel(report)}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
