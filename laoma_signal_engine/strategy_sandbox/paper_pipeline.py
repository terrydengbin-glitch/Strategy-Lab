"""STEP32 sandbox-scoped PaperEngine runner bridge."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping

from laoma_signal_engine.backtest.paper_equivalent import HistoricalCandleProvider
from laoma_signal_engine.candidate_ledger import (
    sandbox_candidate_ledger_mirror_dir,
    sync_candidate_ledger_from_paper_sqlite,
)
from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.paper.adapter import load_trade_plan_documents
from laoma_signal_engine.paper.engine import PaperEngine
from laoma_signal_engine.paper.models import Candle, PaperConfig
from laoma_signal_engine.paper.storage import PaperStore
from laoma_signal_engine.paper.utils import utc_now_iso
from laoma_signal_engine.training_snapshot_sync import (
    sandbox_training_mirror_dir,
    source_mode_for_sandbox_paper,
    sync_paper_sqlite_source,
)
from laoma_signal_engine.trade_quality.auto_completion import complete_paper_trade_quality

EXECUTION_CONTRACT = "paper_engine_sandbox_scoped"
SCHEMA_VERSION = "STEP32.1_sandbox_paper_pipeline_v1"


class SandboxPaperStore(PaperStore):
    """PaperStore variant that never syncs closed orders into main research DB."""

    def close_position(self, position: dict[str, Any], cost: Any, *, gross_pnl: float, exit_reason: str, at: str, candle_ms: int | None) -> bool:
        net_pnl = gross_pnl - cost.fee_usdt
        total_fee = float(position.get("fee_usdt") or 0) + cost.fee_usdt
        total_slippage = float(position.get("slippage_usdt") or 0) + cost.slippage_usdt
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE paper_positions
                SET status='closed', remaining_quantity=0, realized_pnl_usdt=?,
                    unrealized_pnl_usdt=0, fee_usdt=?, slippage_usdt=?,
                    closed_at=?, updated_at=?
                WHERE id=? AND status='open'
                """,
                (net_pnl, total_fee, total_slippage, at, now, position["id"]),
            )
            changed = conn.execute("SELECT changes() AS c").fetchone()["c"]
            if int(changed or 0) != 1:
                return False
            conn.execute(
                """
                UPDATE paper_orders
                SET status='closed', remaining_quantity=0, realized_pnl_usdt=?,
                    unrealized_pnl_usdt=0, exit_price=?, exit_reason=?, fee_usdt=?,
                    slippage_usdt=?, fill_model=?, cost_source=?, slippage_source=?,
                    liquidity_penalty_bps=?, volatility_penalty_bps=?, same_candle_policy=?,
                    closed_at=?, updated_at=?
                WHERE id=?
                """,
                (
                    net_pnl,
                    cost.fill_price,
                    exit_reason,
                    total_fee,
                    total_slippage,
                    cost.fill_model,
                    cost.cost_source,
                    cost.slippage_source,
                    cost.liquidity_penalty_bps,
                    cost.volatility_penalty_bps,
                    cost.same_candle_policy,
                    at,
                    now,
                    position["order_id"],
                ),
            )
            conn.execute(
                """
                UPDATE paper_accounts
                SET realized_pnl_usdt = realized_pnl_usdt + ?,
                    fee_usdt = fee_usdt + ?,
                    slippage_usdt = slippage_usdt + ?,
                    equity_usdt = equity_usdt + ?,
                    updated_at = ?
                WHERE strategy_line = ?
                """,
                (net_pnl, cost.fee_usdt, cost.slippage_usdt, net_pnl, now, position["strategy_line"]),
            )
            self._insert_fill(
                conn,
                position,
                position["id"],
                exit_reason,
                cost,
                float(position["remaining_quantity"]),
                gross_pnl,
                net_pnl,
                candle_ms,
                at,
            )
        return True


class SandboxPaperEngine(PaperEngine):
    def __init__(self, project_root: Path, *, config: PaperConfig, candle_provider: Any | None = None) -> None:
        super().__init__(project_root, config=config, candle_provider=candle_provider)
        self.store = SandboxPaperStore(self.project_root, config)


def _project_rel(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"


def sandbox_paper_config(
    project_root: Path,
    *,
    sandbox_id: str,
    run_id: str,
    run_root_rel: str | None = None,
    base: PaperConfig | None = None,
) -> PaperConfig:
    cfg = base or PaperConfig()
    run_root = Path(run_root_rel) / "paper" if run_root_rel else Path("DATA") / "sandboxes" / str(sandbox_id) / "runtime" / "pipeline_runs" / str(run_id) / "paper"
    return replace(
        cfg,
        db_path=(run_root / "paper_trading.db").as_posix(),
        summary_path=(run_root / "latest_paper_state.json").as_posix(),
        max_trade_plan_age_sec=0,
        daemon_lock_path=(run_root / "paper_daemon.lock").as_posix(),
        daemon_pid_path=(run_root / "paper_daemon.pid").as_posix(),
        daemon_log_path=(run_root / "paper_daemon.log").as_posix(),
        daemon_heartbeat_path=(run_root / "paper_daemon_heartbeat.json").as_posix(),
        daemon_status_path=(run_root / "paper_daemon_status.json").as_posix(),
        archive_dir=(run_root / "archives").as_posix(),
        archive_metadata_path=(run_root / "paper_experiments.json").as_posix(),
    )


def _input_snapshot(
    project_root: Path,
    *,
    sandbox_id: str,
    run_id: str,
    docs: dict[str, dict[str, Any]],
    source: str,
    run_root_rel: str | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    base = root / run_root_rel if run_root_rel else root / "DATA" / "sandboxes" / str(sandbox_id) / "runtime" / "pipeline_runs" / str(run_id)
    path = base / "input_trade_plans.json"
    payload = {
        "schema_version": "STEP32.2_readonly_trade_plan_snapshot_v1",
        "source": source,
        "sandbox_id": sandbox_id,
        "run_id": run_id,
        "generated_at": utc_now_iso(),
        "line_count": len(docs),
        "lines": sorted(docs),
        "docs": docs,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    write_file_atomic(path, _json_bytes(payload))
    return {"input_snapshot_path": _project_rel(root, path), "input_source": source, "line_count": len(docs), "lines": sorted(docs)}


def _ledger_counts(engine: PaperEngine) -> dict[str, int]:
    out: dict[str, int] = {}
    with engine.store.connect() as conn:
        for table in ("paper_intent_inbox", "paper_skip_ledger", "paper_trade_plans", "paper_orders", "paper_positions", "paper_fills", "trade_quality_samples"):
            try:
                out[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            except Exception:
                out[table] = 0
    return out


def run_sandbox_paper_pipeline(
    project_root: Path,
    *,
    sandbox_id: str,
    run_id: str,
    cycle_id: str,
    writer_context: dict[str, Any],
    docs: dict[str, dict[str, Any]] | None = None,
    candles_by_symbol: Mapping[str, list[Candle | Mapping[str, Any]]] | None = None,
    max_ticks: int | None = None,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    context = writer_context.get("context") if isinstance(writer_context.get("context"), dict) else {}
    if not context or context.get("sandbox_id") != sandbox_id or context.get("run_id") != run_id:
        return {
            "schema_version": SCHEMA_VERSION,
            "execution_contract": EXECUTION_CONTRACT,
            "status": "blocked",
            "reason_code": "sandbox_writer_context_required",
            "sandbox_id": sandbox_id,
            "run_id": run_id,
        }
    if context.get("main_chain_mutation_allowed"):
        return {
            "schema_version": SCHEMA_VERSION,
            "execution_contract": EXECUTION_CONTRACT,
            "status": "blocked",
            "reason_code": "main_chain_mutation_not_allowed_for_sandbox_pipeline",
            "sandbox_id": sandbox_id,
            "run_id": run_id,
        }

    targets = writer_context.get("writer_targets") if isinstance(writer_context.get("writer_targets"), dict) else {}
    run_root_rel = targets.get("sandbox_runtime_dir")
    got_docs = docs if docs is not None else load_trade_plan_documents(root)
    snapshot = _input_snapshot(
        root,
        sandbox_id=sandbox_id,
        run_id=run_id,
        docs=got_docs,
        source="injected_docs" if docs is not None else "readonly_current_trade_plan_json",
        run_root_rel=run_root_rel,
    )
    paper_config = sandbox_paper_config(root, sandbox_id=sandbox_id, run_id=run_id, run_root_rel=run_root_rel)
    provider = HistoricalCandleProvider(candles_by_symbol or {}) if candles_by_symbol is not None else None
    engine = SandboxPaperEngine(root, config=paper_config, candle_provider=provider)
    engine.initialize()
    consume = engine.consume_trade_plans(got_docs)
    ticks: list[dict[str, Any]] = []
    if provider is not None:
        for idx, open_time_ms in enumerate(provider.timeline):
            if max_ticks is not None and idx >= int(max_ticks):
                break
            provider.advance_to(open_time_ms)
            ticks.append(
                {
                    "open_time_ms": open_time_ms,
                    "entries": engine.process_pending_entries(),
                    "closes": engine.process_open_positions(),
                }
            )
    summary = engine.store.write_summary()
    trade_quality_completion = complete_paper_trade_quality(root, config=paper_config, candle_provider=provider)
    opts = options or {}
    pipeline_mode = str(opts.get("pipeline_mode") or "")
    resource_lane = str(context.get("resource_lane") or "")
    source_mode = str(opts.get("training_source_mode") or source_mode_for_sandbox_paper(resource_lane, pipeline_mode=pipeline_mode))
    mirror_dir = sandbox_training_mirror_dir(root, sandbox_id=sandbox_id, run_id=run_id, run_root_rel=run_root_rel)
    training_dataset = sync_paper_sqlite_source(
        root,
        source_db_path=engine.store.db_path,
        run_id=run_id,
        source_mode=source_mode,
        sandbox_id=sandbox_id,
        cycle_id=cycle_id,
        resource_lane=resource_lane,
        source_chain=str(context.get("source_chain") or ""),
        writer_context_id=str(writer_context.get("context_id") or ""),
        mirror_dir=mirror_dir,
    )
    candidate_ledger = sync_candidate_ledger_from_paper_sqlite(
        root,
        source_db_path=engine.store.db_path,
        run_id=run_id,
        source_mode=source_mode,
        sandbox_id=sandbox_id,
        cycle_id=cycle_id,
        docs=got_docs,
        counterfactual_candles_by_symbol=dict(candles_by_symbol or {}),
        mirror_dir=sandbox_candidate_ledger_mirror_dir(
            root,
            sandbox_id=sandbox_id,
            run_id=run_id,
            run_root_rel=run_root_rel,
        ),
        execution_source=source_mode,
    )
    counts = _ledger_counts(engine)
    status = "completed" if counts.get("paper_orders", 0) or counts.get("paper_skip_ledger", 0) else "completed_no_orders"
    reason_codes: list[str] = []
    if not got_docs:
        reason_codes.append("no_trade_plan_documents")
    if not counts.get("paper_orders", 0):
        reason_codes.append("no_paper_orders_created")
    if not counts.get("trade_quality_samples", 0):
        reason_codes.append("trade_quality_samples_missing_or_no_closed_trades")
    if not training_dataset.get("training_ready"):
        reason_codes.append(str(training_dataset.get("reason") or "training_dataset_incomplete"))
    result = {
        "schema_version": SCHEMA_VERSION,
        "execution_contract": EXECUTION_CONTRACT,
        "status": status,
        "sandbox_id": sandbox_id,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "paper_db_path": _project_rel(root, engine.store.db_path),
        "paper_summary_path": _project_rel(root, engine.store.summary_path),
        "consume": consume,
        "ticks": ticks,
        "counts": counts,
        "summary": {"stats": summary.get("stats"), "summary": summary.get("summary")},
        "input_snapshot": snapshot,
        "writer_context_id": writer_context.get("context_id"),
        "writer_target": writer_context.get("writer_targets"),
        "trade_quality_completion": trade_quality_completion,
        "training_dataset": training_dataset,
        "candidate_ledger": candidate_ledger,
        "main_chain_mutation_allowed": False,
        "reason_codes": sorted(set(reason_codes)),
        "options": opts,
    }
    result_base = root / run_root_rel if run_root_rel else root / "DATA" / "sandboxes" / str(sandbox_id) / "runtime" / "pipeline_runs" / str(run_id)
    result_path = result_base / "sandbox_paper_pipeline_result.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    write_file_atomic(result_path, _json_bytes(result))
    result["result_path"] = _project_rel(root, result_path)
    return result
