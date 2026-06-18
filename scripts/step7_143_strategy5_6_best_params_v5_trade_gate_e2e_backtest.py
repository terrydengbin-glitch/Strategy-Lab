from __future__ import annotations

import argparse
import json
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
from laoma_signal_engine.backtest.p21_v2 import (
    _connect,
    _metrics,
    _rows_for_symbol,
    _stable_id,
    build_historical_inputs,
    load_runtime_line_config,
    simulate_1m_fill,
)
from laoma_signal_engine.research_db import build_backtest_entry_feature, build_backtest_trade_fact


TASK_ID = "STEP7.143"
SCHEMA_VERSION = "step7.143-real-evaluator-v5-gate-e2e-v1"
OUTPUT_JSON = Path("DATA/backtest/step7_143_strategy5_6_best_params_v5_trade_gate_e2e_backtest.json")
OUTPUT_SQLITE = Path("DATA/backtest/step7_143_strategy5_6_best_params_v5_trade_gate_e2e_trades.sqlite")
STRATEGY5_EVIDENCE_DB = Path("DATA/backtest/evidence/strategy5/strategy5_evidence_pack_20260612T083752Z.sqlite")
STRATEGY6_REFERENCE_EXPERIMENT = "p21v2exp_e28845ec2c05c86a3af1"

TARGETS = {
    "strategy5": {
        "parameter_set_id": "p21v2_72340cb432fa7977",
        "validation_id": "tqv5combo_99ef989cfd6a75fd26c46a",
        "gate": {
            "operator": "AND",
            "rules": [
                {"field": "side_flow_alignment", "op": "eq", "value": "opposite"},
                {"field": "price_flow_alignment", "op": "eq", "value": "opposite"},
            ],
        },
    },
    "strategy6": {
        "parameter_set_id": "s6v32_edcd6b1030331422",
        "validation_id": "tqv5combo_b62820ba88465531d7e991",
        "gate": {
            "operator": "AND",
            "rules": [
                {"field": "funding_bucket", "op": "eq", "value": "NEGATIVE_EXTREME"},
                {"field": "funding_crowded_side", "op": "eq", "value": "short"},
            ],
        },
    },
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _loads(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        out = float(value)
        if out != out or out in (float("inf"), float("-inf")):
            return None
        return out
    except Exception:
        return None


def _fmt(value: Any, digits: int = 3) -> str:
    got = _float(value)
    return "-" if got is None else f"{got:.{digits}f}"


def _pct(value: Any) -> str:
    got = _float(value)
    return "-" if got is None else f"{got * 100:.1f}%"


def _ms_from_iso(value: str) -> int:
    clean = value.replace("Z", "+00:00")
    return int(datetime.fromisoformat(clean).timestamp() * 1000)


def _load_strategy5_params(project_root: Path) -> dict[str, Any]:
    path = project_root / STRATEGY5_EVIDENCE_DB
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT parameters_json FROM parameter_sets WHERE parameter_set_id = ?",
            (TARGETS["strategy5"]["parameter_set_id"],),
        ).fetchone()
    if row is None:
        raise RuntimeError("missing_strategy5_parameter_set")
    params = _loads(row["parameters_json"], {})
    params["strategy_line"] = "strategy5"
    return params


def _load_strategy6_reference(project_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    db_path = p21_db_path(project_root)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        param = conn.execute(
            """
            SELECT parameters_json
            FROM p21_v2_parameter_sets
            WHERE parameter_set_id = ?
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (TARGETS["strategy6"]["parameter_set_id"],),
        ).fetchone()
        exp = conn.execute(
            "SELECT * FROM p21_v2_experiments WHERE experiment_id = ?",
            (STRATEGY6_REFERENCE_EXPERIMENT,),
        ).fetchone()
    if param is None:
        raise RuntimeError("missing_strategy6_parameter_set")
    if exp is None:
        raise RuntimeError("missing_strategy6_reference_experiment")
    params = _loads(param["parameters_json"], {})
    params["strategy_line"] = "strategy6"
    experiment = dict(exp)
    experiment["symbols"] = _loads(experiment.pop("symbols_json", "[]"), [])
    return params, experiment


def _rule_fields(rule: dict[str, Any]) -> list[str]:
    fields: list[str] = []
    if "field" in rule:
        fields.append(str(rule["field"]))
    for item in rule.get("rules") or []:
        if isinstance(item, dict):
            fields.extend(_rule_fields(item))
    return sorted(set(fields))


def _norm(value: Any) -> str:
    return str(value).strip().lower()


def _rule_matches(rule: dict[str, Any], features: dict[str, Any]) -> bool:
    if "field" in rule:
        field = str(rule.get("field"))
        op = str(rule.get("op") or "eq").lower()
        expected = rule.get("value")
        actual = features.get(field)
        if op in {"eq", "=="}:
            return _norm(actual) == _norm(expected)
        if op in {"neq", "!="}:
            return _norm(actual) != _norm(expected)
        return False
    children = [item for item in rule.get("rules") or [] if isinstance(item, dict)]
    if not children:
        return False
    operator = str(rule.get("operator") or "AND").upper()
    if operator == "OR":
        return any(_rule_matches(item, features) for item in children)
    return all(_rule_matches(item, features) for item in children)


def _gate_decision(rule: dict[str, Any], features: dict[str, Any]) -> dict[str, Any]:
    fields = _rule_fields(rule)
    missing = [field for field in fields if features.get(field) in (None, "", "missing")]
    if missing:
        return {"decision": "gate_feature_missing", "action": "block", "missing_fields": missing}
    matched = _rule_matches(rule, features)
    return {
        "decision": "gate_shadow_block" if matched else "gate_pass",
        "action": "block" if matched else "pass",
        "missing_fields": [],
    }


def _entry_features_from_order(conn: sqlite3.Connection, experiment_id: str, parameter_set_id: str, strategy_line: str, params: dict[str, Any], order: dict[str, Any], generated_at: str) -> dict[str, Any]:
    fact = build_backtest_trade_fact(
        experiment_id=experiment_id,
        parameter_set_id=parameter_set_id,
        strategy_line=strategy_line,
        parameters=params,
        order=order,
        generated_at=generated_at,
    )
    item = build_backtest_entry_feature(fact, order=order, conn=conn, generated_at=generated_at)
    return _loads(item.get("features_json"), {})


def _init_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE e2e_trades(
          row_id TEXT PRIMARY KEY,
          experiment_id TEXT NOT NULL,
          branch TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          parameter_set_id TEXT NOT NULL,
          validation_id TEXT NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          signal_time_ms INTEGER NOT NULL,
          entry_time_ms INTEGER NOT NULL,
          exit_time_ms INTEGER,
          trade_plan_id TEXT NOT NULL,
          plan_executable INTEGER NOT NULL,
          gate_decision TEXT NOT NULL,
          gate_action TEXT NOT NULL,
          gate_rule_json TEXT NOT NULL,
          gate_features_json TEXT NOT NULL,
          missing_fields_json TEXT NOT NULL,
          net_R REAL,
          exit_reason TEXT,
          trade_plan_payload_json TEXT NOT NULL,
          fill_result_json TEXT NOT NULL,
          generated_at TEXT NOT NULL
        )
        """
    )
    return conn


def _insert_trade(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = [
        "row_id",
        "experiment_id",
        "branch",
        "strategy_line",
        "parameter_set_id",
        "validation_id",
        "symbol",
        "side",
        "signal_time_ms",
        "entry_time_ms",
        "exit_time_ms",
        "trade_plan_id",
        "plan_executable",
        "gate_decision",
        "gate_action",
        "gate_rule_json",
        "gate_features_json",
        "missing_fields_json",
        "net_R",
        "exit_reason",
        "trade_plan_payload_json",
        "fill_result_json",
        "generated_at",
    ]
    conn.execute(
        f"INSERT OR REPLACE INTO e2e_trades({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
        tuple(row.get(col) for col in columns),
    )


def _split_orders(orders: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    ordered = sorted(orders, key=lambda row: int(row.get("entry_time_ms") or 0))
    n = len(ordered)
    train_end = int(n * 0.6)
    validation_end = int(n * 0.8)
    return {
        "train": ordered[:train_end],
        "validation": ordered[train_end:validation_end],
        "test": ordered[validation_end:],
    }


def _metrics_with_splits(orders: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "all": _metrics(orders),
        "split": {name: _metrics(items) for name, items in _split_orders(orders).items()},
    }


def _run_line(
    *,
    project_root: Path,
    source_conn: sqlite3.Connection,
    ledger_conn: sqlite3.Connection,
    line: str,
    params: dict[str, Any],
    symbols: list[str],
    start_ms: int,
    end_ms: int,
    base_min_score: float,
    generated_at: str,
) -> dict[str, Any]:
    target = TARGETS[line]
    parameter_set_id = str(target["parameter_set_id"])
    validation_id = str(target["validation_id"])
    rule = dict(target["gate"])
    experiment_id = f"step7_143_{line}_{parameter_set_id}"
    baseline_orders: list[dict[str, Any]] = []
    gate_on_orders: list[dict[str, Any]] = []
    reason_counter: Counter[str] = Counter()
    gate_counter: Counter[str] = Counter()
    evaluated_signals = 0
    executable_plans = 0
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
            order["order_id"] = _stable_id("s7143ord", {"line": line, "p": parameter_set_id, "s": signal.signal_id}, 24)
            order["parameter_set_id"] = parameter_set_id
            order["reasons"] = list(evaluated.get("reason_codes") or [])
            order["lineage_mode"] = evaluated.get("lineage_mode") or ENGINE_MODE
            order["source_contract_version"] = evaluated.get("source_contract_version")
            order["config_patch"] = evaluated.get("config_patch") or params
            order["trade_plan_payload"] = evaluated.get("trade_plan_payload") or order.get("trade_plan_payload") or {}
            trade_plan_id = _stable_id("s7143plan", order.get("trade_plan_payload") or order, 20)
            features = _entry_features_from_order(source_conn, experiment_id, parameter_set_id, line, params, order, generated_at)
            decision = _gate_decision(rule, features)
            gate_counter[decision["decision"]] += 1

            filled_base = simulate_1m_fill(order, rows, params)
            filled_base["fill_result"] = {
                "exit_time_ms": filled_base.get("exit_time_ms"),
                "exit_price": filled_base.get("exit_price"),
                "exit_reason": filled_base.get("exit_reason"),
                "net_R": filled_base.get("net_R"),
            }
            baseline_orders.append(filled_base)
            common = {
                "experiment_id": experiment_id,
                "strategy_line": line,
                "parameter_set_id": parameter_set_id,
                "validation_id": validation_id,
                "symbol": order.get("symbol"),
                "side": order.get("side"),
                "signal_time_ms": order.get("signal_time_ms"),
                "entry_time_ms": order.get("entry_time_ms"),
                "trade_plan_id": trade_plan_id,
                "plan_executable": 1,
                "gate_rule_json": _json(rule),
                "gate_features_json": _json(features),
                "missing_fields_json": _json(decision["missing_fields"]),
                "trade_plan_payload_json": _json(order.get("trade_plan_payload") or {}),
                "generated_at": generated_at,
            }
            _insert_trade(
                ledger_conn,
                {
                    **common,
                    "row_id": _stable_id("s7143row", {"branch": "baseline", "order": order["order_id"]}, 24),
                    "branch": "baseline",
                    "exit_time_ms": filled_base.get("exit_time_ms"),
                    "gate_decision": "baseline_no_gate",
                    "gate_action": "pass",
                    "net_R": filled_base.get("net_R"),
                    "exit_reason": filled_base.get("exit_reason"),
                    "fill_result_json": _json(filled_base.get("fill_result") or {}),
                },
            )
            if decision["action"] == "pass":
                filled_gate = simulate_1m_fill(order, rows, params)
                filled_gate["fill_result"] = {
                    "exit_time_ms": filled_gate.get("exit_time_ms"),
                    "exit_price": filled_gate.get("exit_price"),
                    "exit_reason": filled_gate.get("exit_reason"),
                    "net_R": filled_gate.get("net_R"),
                }
                gate_on_orders.append(filled_gate)
                _insert_trade(
                    ledger_conn,
                    {
                        **common,
                        "row_id": _stable_id("s7143row", {"branch": "gate_on", "order": order["order_id"]}, 24),
                        "branch": "gate_on",
                        "exit_time_ms": filled_gate.get("exit_time_ms"),
                        "gate_decision": decision["decision"],
                        "gate_action": decision["action"],
                        "net_R": filled_gate.get("net_R"),
                        "exit_reason": filled_gate.get("exit_reason"),
                        "fill_result_json": _json(filled_gate.get("fill_result") or {}),
                    },
                )
            else:
                _insert_trade(
                    ledger_conn,
                    {
                        **common,
                        "row_id": _stable_id("s7143row", {"branch": "gate_on_blocked", "order": order["order_id"]}, 24),
                        "branch": "gate_on_blocked",
                        "exit_time_ms": None,
                        "gate_decision": decision["decision"],
                        "gate_action": decision["action"],
                        "net_R": None,
                        "exit_reason": "blocked_before_fill",
                        "fill_result_json": "{}",
                    },
                )

    return {
        "strategy_line": line,
        "parameter_set_id": parameter_set_id,
        "validation_id": validation_id,
        "gate_rule_json": rule,
        "experiment_id": experiment_id,
        "symbols": len(symbols),
        "symbols_with_rows": symbols_with_rows,
        "symbols_with_signals": symbols_with_signals,
        "evaluated_signals": evaluated_signals,
        "executable_plans": executable_plans,
        "non_executable_reasons": dict(reason_counter.most_common(20)),
        "gate_decisions": dict(gate_counter),
        "baseline": _metrics_with_splits(baseline_orders),
        "gate_on": _metrics_with_splits(gate_on_orders),
    }


def _report(project_root: Path, payload: dict[str, Any]) -> Path:
    path = project_root / "docs" / "reports" / f"STEP7.143_strategy5_6_best_params_v5_trade_gate_e2e_backtest_{_stamp()}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# STEP7.143 Strategy5/6 Best Params V5 Trade Gate E2E Backtest",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- schema_version: `{payload['schema_version']}`",
        f"- source_db: `{payload['source_db']}`",
        f"- output_json: `{payload['output_json']}`",
        f"- output_sqlite: `{payload['output_sqlite']}`",
        f"- start_time: `{payload['window']['start_time']}`",
        f"- end_time: `{payload['window']['end_time']}`",
        f"- symbol_count: `{payload['window']['symbol_count']}`",
        "",
        "## Contract",
        "",
        "- This run executed `evaluate_signal_offline()` for strategy5 and strategy6 before fill.",
        "- V5 trade gate was evaluated after trade plan/order creation and before `simulate_1m_fill()` for the gate-on branch.",
        "- Baseline branch fills all executable trade plans. Gate-on branch fills only gate-pass plans.",
        "- Production config, live paper ledger, daemon, and Feishu notification chain were not modified.",
        "",
        "## Results",
        "",
        "| strategy | parameter_set | validation | executable plans | gate pass | gate block | feature missing | baseline PF | gate PF | baseline trades | gate trades | baseline DD | gate DD |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    pf_gt_one = 0
    for row in payload["results"]:
        gate_decisions = row.get("gate_decisions") or {}
        base = (row.get("baseline") or {}).get("all") or {}
        gate = (row.get("gate_on") or {}).get("all") or {}
        if (_float(gate.get("profit_factor")) or 0.0) >= 1.0:
            pf_gt_one += 1
        lines.append(
            f"| `{row['strategy_line']}` | `{row['parameter_set_id']}` | `{row['validation_id']}` | "
            f"{row.get('executable_plans')} | {gate_decisions.get('gate_pass', 0)} | "
            f"{gate_decisions.get('gate_shadow_block', 0)} | {gate_decisions.get('gate_feature_missing', 0)} | "
            f"{_fmt(base.get('profit_factor'))} | {_fmt(gate.get('profit_factor'))} | "
            f"{base.get('trade_count')} | {gate.get('trade_count')} | "
            f"{_fmt(base.get('max_drawdown_R'))} | {_fmt(gate.get('max_drawdown_R'))} |"
        )
    lines.extend(["", "## Split Detail", ""])
    for row in payload["results"]:
        lines.extend(
            [
                f"### {row['strategy_line']}",
                "",
                "| split | baseline PF | gate PF | baseline trades | gate trades | baseline expectancy | gate expectancy |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        base_split = (row.get("baseline") or {}).get("split") or {}
        gate_split = (row.get("gate_on") or {}).get("split") or {}
        for split in ("train", "validation", "test"):
            b = base_split.get(split) or {}
            g = gate_split.get(split) or {}
            lines.append(
                f"| `{split}` | {_fmt(b.get('profit_factor'))} | {_fmt(g.get('profit_factor'))} | "
                f"{b.get('trade_count')} | {g.get('trade_count')} | "
                f"{_fmt(b.get('expectancy_R'))} | {_fmt(g.get('expectancy_R'))} |"
            )
        lines.append("")
    lines.extend(["## Judgment", ""])
    lines.append(f"- PF >= 1 gate-on strategy count: `{pf_gt_one}`.")
    if pf_gt_one:
        lines.append("- At least one strategy reached PF >= 1 in real evaluator + gate-before-fill E2E. Paper shadow is still required before promotion.")
    else:
        lines.append("- No strategy reached PF >= 1 in real evaluator + gate-before-fill E2E. Treat as paper-shadow research evidence only.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def run(project_root: Path, *, max_symbols: int | None = None) -> dict[str, Any]:
    generated_at = _now()
    db_path = p21_db_path(project_root)
    strategy5_params = _load_strategy5_params(project_root)
    strategy6_params, experiment = _load_strategy6_reference(project_root)
    symbols = [str(s).upper() for s in experiment["symbols"]]
    if max_symbols is not None:
        symbols = symbols[: max(1, int(max_symbols))]
    start_ms = _ms_from_iso(experiment["start_time"])
    end_ms = _ms_from_iso(experiment["end_time"])
    out_sqlite = project_root / OUTPUT_SQLITE
    out_json = project_root / OUTPUT_JSON
    out_json.parent.mkdir(parents=True, exist_ok=True)
    ledger_conn = _init_ledger(out_sqlite)
    base_min_score = float(load_runtime_line_config(project_root, "without_micro").get("min_score") or 68.0)
    with _connect(db_path) as source_conn:
        results = [
            _run_line(
                project_root=project_root,
                source_conn=source_conn,
                ledger_conn=ledger_conn,
                line="strategy5",
                params=strategy5_params,
                symbols=symbols,
                start_ms=start_ms,
                end_ms=end_ms,
                base_min_score=base_min_score,
                generated_at=generated_at,
            ),
            _run_line(
                project_root=project_root,
                source_conn=source_conn,
                ledger_conn=ledger_conn,
                line="strategy6",
                params=strategy6_params,
                symbols=symbols,
                start_ms=start_ms,
                end_ms=end_ms,
                base_min_score=base_min_score,
                generated_at=generated_at,
            ),
        ]
    ledger_conn.commit()
    ledger_conn.close()
    payload = {
        "task_id": TASK_ID,
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "generated_at": generated_at,
        "engine_mode": ENGINE_MODE,
        "source_db": str(db_path),
        "output_json": str(out_json),
        "output_sqlite": str(out_sqlite),
        "window": {
            "source_experiment_id": STRATEGY6_REFERENCE_EXPERIMENT,
            "start_time": experiment["start_time"],
            "end_time": experiment["end_time"],
            "symbol_count": len(symbols),
            "symbols": symbols,
        },
        "results": results,
    }
    report = _report(project_root, payload)
    payload["report"] = str(report)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--max-symbols", type=int, default=None)
    args = parser.parse_args()
    payload = run(Path(args.project_root).resolve(), max_symbols=args.max_symbols)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
