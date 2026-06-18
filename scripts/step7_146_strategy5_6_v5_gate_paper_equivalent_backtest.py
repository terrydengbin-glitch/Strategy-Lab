from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_real_evaluator import ENGINE_MODE, evaluate_signal_offline
from laoma_signal_engine.backtest.p21_v2 import _connect, build_historical_inputs, load_runtime_line_config
from laoma_signal_engine.backtest.paper_equivalent import (
    EXECUTION_CONTRACT,
    EXECUTION_CONTRACT_VERSION,
    PAPER_ADAPTER_VERSION,
    PAPER_GATE_VERSION,
    PaperEquivalentBacktestSession,
    default_paper_equivalent_config,
)
from laoma_signal_engine.decision.trade_plan_lines import _build_position_sizing, load_trade_plan_line_config
from laoma_signal_engine.paper.config import load_paper_config
from laoma_signal_engine.paper.models import Candle
from laoma_signal_engine.paper.utils import utc_now_iso
from scripts.step7_143_strategy5_6_best_params_v5_trade_gate_e2e_backtest import (
    STRATEGY6_REFERENCE_EXPERIMENT,
    TARGETS,
    _entry_features_from_order,
    _load_strategy5_params,
    _load_strategy6_reference,
    _metrics,
    _ms_from_iso,
    _rows_for_symbol,
    _stable_id,
)


TASK_ID = "STEP7.146"
SCHEMA_VERSION = "step7.146-paper-equivalent-v5-gate-rerun-v1"
OUTPUT_JSON = Path("DATA/backtest/step7_146_strategy5_6_v5_gate_paper_equivalent_backtest.json")
GATE_CONFIG = Path("DATA/paper/v5_trade_gate_experiment.json")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _write_gate_config(root: Path, *, enabled: bool, experiment_id: str) -> None:
    path = root / GATE_CONFIG
    cfg = {
        "enabled": bool(enabled),
        "experiment_id": experiment_id,
        "paper_epoch_id": f"{experiment_id}_epoch",
        "line_epochs": {
            "strategy5": f"{experiment_id}_strategy5",
            "strategy6": f"{experiment_id}_strategy6",
        },
        "mode": "paper_equivalent_backtest",
        "feature_missing_policy": "block",
        "rules": {
            "strategy5": {
                "parameter_set_id": TARGETS["strategy5"]["parameter_set_id"],
                "gate_candidate_id": "strategy5_v5_opposite_flow_combo_gate",
                "action": "block",
                "rule_json": TARGETS["strategy5"]["gate"],
            },
            "strategy6": {
                "parameter_set_id": TARGETS["strategy6"]["parameter_set_id"],
                "gate_candidate_id": "strategy6_v5_negative_funding_short_crowded_gate",
                "action": "block",
                "rule_json": TARGETS["strategy6"]["gate"],
            },
        },
    }
    _write_json(path, cfg)


def _restore_gate_config(root: Path, backup_path: Path | None) -> None:
    path = root / GATE_CONFIG
    if backup_path and backup_path.exists():
        shutil.copy2(backup_path, path)
    elif path.exists():
        path.unlink()


def _backup_gate_config(root: Path, stamp: str) -> Path | None:
    path = root / GATE_CONFIG
    if not path.exists():
        return None
    backup = root / "DATA" / "paper" / "gate_config_snapshots" / f"v5_trade_gate_experiment_before_STEP7.146_{stamp}.json"
    backup.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, backup)
    return backup


def _candle_rows(rows: list[dict[str, Any]]) -> list[Candle]:
    return [
        Candle(
            symbol=str(row["symbol"]).upper(),
            open_time_ms=int(row["open_time_ms"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row.get("volume") or 0.0),
        )
        for row in rows
    ]


def _trade_plan_doc(
    *,
    root: Path,
    line: str,
    order: dict[str, Any],
    features: dict[str, Any],
    run_id: str,
    cycle_id: str,
    generated_at: str,
) -> dict[str, Any]:
    payload = dict(order.get("trade_plan_payload") or {})
    symbol = str(order.get("symbol") or payload.get("symbol") or "").upper()
    side = str(order.get("side") or payload.get("decision") or "").upper()
    entry = float(order.get("entry_price") or payload.get("estimated_entry_price"))
    stop = float(order.get("stop_loss") or payload.get("stop_loss"))
    take = float(order.get("take_profit") or payload.get("take_profit"))
    risk_per_unit = abs(entry - stop)
    reward_per_unit = abs(take - entry)
    position_sizing, sizing_reject = _historical_position_sizing(
        root=root,
        line=line,
        entry=entry,
        stop=stop,
        take=take,
        risk_per_unit=risk_per_unit,
        reward_per_unit=reward_per_unit,
        existing=payload.get("position_sizing"),
    )
    guards = payload.get("guards") if isinstance(payload.get("guards"), dict) else {}
    guards = {
        **guards,
        **features,
        "line": line,
        "margin_usdt": guards.get("margin_usdt", 100),
        "leverage": guards.get("leverage", 20),
        "paper_equivalent_execution_contract": EXECUTION_CONTRACT,
        "paper_equivalent_source_order_id": order.get("order_id"),
    }
    if position_sizing:
        guards.update(
            {
                "planned_loss_usdt": position_sizing.get("planned_loss_usdt") or position_sizing.get("gross_risk_usdt"),
                "planned_profit_usdt": position_sizing.get("planned_profit_usdt") or position_sizing.get("gross_reward_usdt"),
                "estimated_max_loss_usdt": position_sizing.get("estimated_max_loss_usdt"),
                "planned_notional_usdt": position_sizing.get("planned_notional_usdt") or position_sizing.get("notional_usdt"),
                "planned_quantity": position_sizing.get("planned_quantity") or position_sizing.get("quantity"),
                "target_planned_loss_usdt": position_sizing.get("target_planned_loss_usdt"),
                "max_planned_loss_usdt": position_sizing.get("max_planned_loss_usdt"),
                "loss_cap_applied": position_sizing.get("loss_cap_applied"),
                "sizing_policy": position_sizing.get("method"),
                "paper_fallback_notional_allowed": position_sizing.get("paper_fallback_notional_allowed"),
                "historical_position_sizing_source": position_sizing.get("historical_position_sizing_source"),
            }
        )
    if sizing_reject:
        guards["historical_position_sizing_reject_reason"] = sizing_reject
    plan = {
        **payload,
        "symbol": symbol,
        "decision_tf": payload.get("decision_tf") or "15m",
        "decision": side,
        "action": "ENTER_MARKET",
        "entry_mode": "MARKET",
        "estimated_entry_price": entry,
        "stop_loss": stop,
        "take_profit": take,
        "risk_per_unit": risk_per_unit,
        "reward_per_unit": reward_per_unit,
        "rr": reward_per_unit / risk_per_unit if risk_per_unit > 0 else None,
        "executable": True,
        "confidence": payload.get("confidence", order.get("score", 0)),
        "reason_codes": list(order.get("reasons") or payload.get("reason_codes") or []),
        "position_sizing": position_sizing,
        "guards": guards,
        "input_refs": {
            **(payload.get("input_refs") if isinstance(payload.get("input_refs"), dict) else {}),
            "paper_equivalent_source_order_id": order.get("order_id"),
            "paper_equivalent_entry_time_ms": order.get("entry_time_ms"),
        },
    }
    return {
        "schema_version": "1.0",
        "generated_at": generated_at,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "source": f"trade_plan_{line}",
        "micro_mode": line,
        "status": "ok",
        "count": 1,
        "executable_count": 1,
        "input_refs": {
            "execution_contract": EXECUTION_CONTRACT,
            "source_order_id": order.get("order_id"),
        },
        "plans": [plan],
    }


def _historical_position_sizing(
    *,
    root: Path,
    line: str,
    entry: float,
    stop: float,
    take: float,
    risk_per_unit: float,
    reward_per_unit: float,
    existing: Any,
) -> tuple[dict[str, Any] | None, str | None]:
    if isinstance(existing, dict) and not existing.get("sizing_reject_reason"):
        out = dict(existing)
        out.setdefault("historical_position_sizing_source", "evaluator_payload")
        return out, None
    cfg = load_trade_plan_line_config(root, line)  # type: ignore[arg-type]
    sizing, reject = _build_position_sizing(
        cfg=cfg,
        entry=entry,
        stop=stop,
        take=take,
        risk_per_unit=risk_per_unit,
        reward_per_unit=reward_per_unit,
    )
    if sizing:
        sizing = dict(sizing)
        sizing["historical_position_sizing_source"] = "trade_plan_line_config"
    return sizing, reject


def _paper_orders(root: Path, db_rel: str, *, line: str) -> list[dict[str, Any]]:
    db_path = root / db_rel
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM paper_orders
            WHERE strategy_line = ?
            ORDER BY COALESCE(opened_at, created_at), id
            """,
            (line,),
        ).fetchall()
    return [dict(row) for row in rows]


def _paper_skips(root: Path, db_rel: str, *, line: str) -> list[dict[str, Any]]:
    db_path = root / db_rel
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM paper_skip_ledger
            WHERE strategy_line = ?
            ORDER BY created_at, id
            """,
            (line,),
        ).fetchall()
    return [dict(row) for row in rows]


def _metrics_from_paper_orders(orders: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [row for row in orders if str(row.get("status") or "") == "closed"]
    synthetic = [{"net_R": row.get("realized_pnl_usdt"), "entry_time_ms": 0} for row in closed]
    metrics = _metrics(synthetic)
    metrics["closed_orders"] = len(closed)
    metrics["created_orders"] = len(orders)
    metrics["open_or_pending_orders"] = len(orders) - len(closed)
    return metrics


def _run_branch(
    *,
    root: Path,
    source_conn: sqlite3.Connection,
    line: str,
    params: dict[str, Any],
    symbols: list[str],
    start_ms: int,
    end_ms: int,
    base_min_score: float,
    generated_at: str,
    branch: str,
) -> dict[str, Any]:
    parameter_set_id = str(TARGETS[line]["parameter_set_id"])
    experiment_id = f"step7_146_{branch}_{line}_{parameter_set_id}"
    run_id = f"step7_146_{branch}_{line}"
    paper_config = default_paper_equivalent_config(run_id=run_id, base=load_paper_config(root))
    db_path = root / paper_config.db_path
    if db_path.exists():
        db_path.unlink()
    summary_path = root / paper_config.summary_path
    if summary_path.exists():
        summary_path.unlink()
    reason_counter: Counter[str] = Counter()
    evaluated_signals = 0
    executable_plans = 0
    consumed_plans = 0
    created_total = 0
    skipped_total = 0
    symbols_with_rows = 0
    symbols_with_signals = 0

    for symbol in symbols:
        rows = _rows_for_symbol(source_conn, symbol, start_ms, end_ms)
        if len(rows) < 40:
            reason_counter["missing_or_short_kline_rows"] += 1
            continue
        symbols_with_rows += 1
        signals = build_historical_inputs(rows, symbol=symbol, strategy_line=line, base_min_score=base_min_score)
        if signals:
            symbols_with_signals += 1
        session = PaperEquivalentBacktestSession(
            root,
            run_id=run_id,
            config=paper_config,
            candles_by_symbol={symbol: _candle_rows(rows)},
        )
        for signal in signals:
            evaluated_signals += 1
            evaluated = evaluate_signal_offline(signal, rows, params)
            if not evaluated.get("executable"):
                reason_counter.update(evaluated.get("reason_codes") or ["not_executable"])
                continue
            order = dict(evaluated.get("order") or {})
            if not order:
                reason_counter.update(evaluated.get("reason_codes") or ["missing_order"])
                continue
            executable_plans += 1
            order["order_id"] = _stable_id("s7146ord", {"line": line, "p": parameter_set_id, "s": signal.signal_id}, 24)
            order["parameter_set_id"] = parameter_set_id
            order["reasons"] = list(evaluated.get("reason_codes") or [])
            order["lineage_mode"] = evaluated.get("lineage_mode") or ENGINE_MODE
            order["source_contract_version"] = evaluated.get("source_contract_version")
            order["config_patch"] = evaluated.get("config_patch") or params
            order["trade_plan_payload"] = evaluated.get("trade_plan_payload") or order.get("trade_plan_payload") or {}
            features = _entry_features_from_order(source_conn, experiment_id, parameter_set_id, line, params, order, generated_at)
            doc = _trade_plan_doc(
                root=root,
                line=line,
                order=order,
                features=features,
                run_id=f"{run_id}_{symbol}",
                cycle_id=f"cycle_{run_id}_{symbol}",
                generated_at=generated_at,
            )
            result = session.consume_trade_plan({line: doc}, at_ms=int(order.get("signal_time_ms") or order.get("entry_time_ms") or start_ms))
            consumed_plans += 1
            created_total += int(result.get("created") or 0)
            skipped_total += len(result.get("skipped") or [])
        session.finish()

    db_rel = paper_config.db_path
    orders = _paper_orders(root, db_rel, line=line)
    skips = _paper_skips(root, db_rel, line=line)
    gate_decisions = Counter(str(row.get("gate_decision") or "none") for row in [*orders, *skips])
    return {
        "strategy_line": line,
        "branch": branch,
        "parameter_set_id": parameter_set_id,
        "validation_id": TARGETS[line]["validation_id"],
        "experiment_id": experiment_id,
        "paper_equivalent_run_id": run_id,
        "db_path": str(root / db_rel),
        "symbols": len(symbols),
        "symbols_with_rows": symbols_with_rows,
        "symbols_with_signals": symbols_with_signals,
        "evaluated_signals": evaluated_signals,
        "executable_plans": executable_plans,
        "consumed_plans": consumed_plans,
        "created_orders": len(orders),
        "skip_rows": len(skips),
        "created_total_reported": created_total,
        "skipped_total_reported": skipped_total,
        "gate_decisions": dict(gate_decisions),
        "non_executable_reasons": dict(reason_counter.most_common(20)),
        "metrics": _metrics_from_paper_orders(orders),
    }


def _report(root: Path, payload: dict[str, Any]) -> Path:
    path = root / "docs" / "reports" / f"STEP7.146_strategy5_6_v5_gate_paper_equivalent_backtest_{_stamp()}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# STEP7.146 Strategy5/6 V5 Gate Paper-Equivalent Backtest",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- status: `{payload['status']}`",
        f"- execution_contract: `{payload['execution_contract']}`",
        f"- execution_contract_version: `{payload['execution_contract_version']}`",
        f"- output_json: `{payload['output_json']}`",
        f"- symbol_count: `{payload['window']['symbol_count']}`",
        f"- legacy_comparison_ref: `{payload['legacy_comparison_ref']}`",
        "",
        "## Results",
        "",
        "| strategy | branch | executable | orders | skips | closed | PF | gate decisions |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for item in payload["results"]:
        metrics = item.get("metrics") or {}
        lines.append(
            f"| `{item['strategy_line']}` | `{item['branch']}` | {item.get('executable_plans')} | "
            f"{item.get('created_orders')} | {item.get('skip_rows')} | {metrics.get('closed_orders')} | "
            f"{metrics.get('profit_factor')} | `{item.get('gate_decisions')}` |"
        )
    lines.extend(
        [
            "",
            "## Judgment",
            "",
            "- This run uses PaperEngine / paper.adapter / paper V5 gate through the STEP7.145 paper-equivalent adapter.",
            "- STEP7.143 remains a legacy comparison reference and should not be used as promotion evidence by itself.",
            "- STEP7.135 remains blocked until paper-equivalent evidence and live paper lineage are both acceptable.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _selected_symbols(all_symbols: list[str], *, max_symbols: int | None, symbols: list[str] | None) -> tuple[list[str], dict[str, Any]]:
    if symbols:
        wanted = [str(item).strip().upper() for item in symbols if str(item).strip()]
        available = set(all_symbols)
        selected = [item for item in wanted if item in available]
        missing = [item for item in wanted if item not in available]
        return selected, {"mode": "explicit", "requested": wanted, "missing": missing}
    selected = list(all_symbols)
    if max_symbols is not None:
        selected = selected[: max(1, int(max_symbols))]
        return selected, {"mode": "first_n", "max_symbols": max_symbols}
    return selected, {"mode": "all"}


def run(project_root: Path, *, max_symbols: int | None = None, symbols: list[str] | None = None) -> dict[str, Any]:
    root = Path(project_root).resolve()
    stamp = _stamp()
    generated_at = _now()
    backup = _backup_gate_config(root, stamp)
    db_path = p21_db_path(root)
    strategy5_params = _load_strategy5_params(root)
    strategy6_params, experiment = _load_strategy6_reference(root)
    all_symbols = [str(s).upper() for s in experiment["symbols"]]
    symbols, symbol_selection = _selected_symbols(all_symbols, max_symbols=max_symbols, symbols=symbols)
    if not symbols:
        raise RuntimeError("no_symbols_selected")
    start_ms = _ms_from_iso(experiment["start_time"])
    end_ms = _ms_from_iso(experiment["end_time"])
    base_min_score = float(load_runtime_line_config(root, "without_micro").get("min_score") or 68.0)
    results: list[dict[str, Any]] = []
    try:
        with _connect(db_path) as source_conn:
            _write_gate_config(root, enabled=False, experiment_id=f"step7_146_baseline_{stamp}")
            for line, params in (("strategy5", strategy5_params), ("strategy6", strategy6_params)):
                results.append(
                    _run_branch(
                        root=root,
                        source_conn=source_conn,
                        line=line,
                        params=params,
                        symbols=symbols,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        base_min_score=base_min_score,
                        generated_at=generated_at,
                        branch="baseline",
                    )
                )
            _write_gate_config(root, enabled=True, experiment_id=f"step7_146_gate_on_{stamp}")
            for line, params in (("strategy5", strategy5_params), ("strategy6", strategy6_params)):
                results.append(
                    _run_branch(
                        root=root,
                        source_conn=source_conn,
                        line=line,
                        params=params,
                        symbols=symbols,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        base_min_score=base_min_score,
                        generated_at=generated_at,
                        branch="gate_on",
                    )
                )
    finally:
        _restore_gate_config(root, backup)
    out_json = root / OUTPUT_JSON
    payload = {
        "task_id": TASK_ID,
        "schema_version": SCHEMA_VERSION,
        "status": _status_from_results(results),
        "generated_at": generated_at,
        "execution_contract": EXECUTION_CONTRACT,
        "execution_contract_version": EXECUTION_CONTRACT_VERSION,
        "legacy_comparison_ref": "STEP7.143",
        "paper_adapter_version": PAPER_ADAPTER_VERSION,
        "paper_gate_version": PAPER_GATE_VERSION,
        "paper_fill_model": load_paper_config(root).fill_model_mode,
        "source_db": str(db_path),
        "output_json": str(out_json),
        "gate_config_backup": str(backup) if backup else None,
        "available_symbol_count": len(all_symbols),
        "symbol_selection": symbol_selection,
        "symbol_count": len(symbols),
        "symbols": symbols,
        "window": {
            "source_experiment_id": STRATEGY6_REFERENCE_EXPERIMENT,
            "start_time": experiment["start_time"],
            "end_time": experiment["end_time"],
            "available_symbol_count": len(all_symbols),
            "symbol_selection": symbol_selection,
            "symbol_count": len(symbols),
            "symbols": symbols,
        },
        "strategy6_backtest_only_config": {
            "strategy6_backtest_max_effective_planned_rr": (strategy6_params.get("strategy6") or {}).get(
                "strategy6_backtest_max_effective_planned_rr"
            )
            if isinstance(strategy6_params.get("strategy6"), dict)
            else strategy6_params.get("strategy6_backtest_max_effective_planned_rr"),
        },
        "results": results,
    }
    report = _report(root, payload)
    payload["report"] = str(report)
    _write_json(out_json, payload)
    return payload


def _status_from_results(results: list[dict[str, Any]]) -> str:
    executable = sum(int(row.get("executable_plans") or 0) for row in results)
    orders = sum(int(row.get("created_orders") or 0) for row in results)
    gate_rows = sum(sum(int(v or 0) for k, v in (row.get("gate_decisions") or {}).items() if str(k) not in {"", "none"}) for row in results)
    if executable > 0 and orders == 0:
        return "blocked_paper_adapter_contract"
    if executable > 0 and gate_rows == 0:
        return "blocked_no_gate_lineage"
    return "ok"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--symbols", default=None, help="Comma-separated explicit symbols from the source experiment.")
    args = parser.parse_args()
    symbols = [item.strip() for item in str(args.symbols).split(",")] if args.symbols else None
    payload = run(Path(args.project_root), max_symbols=args.max_symbols, symbols=symbols)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
