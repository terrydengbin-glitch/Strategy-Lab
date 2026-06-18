from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.backtest.paper_equivalent import (
    EXECUTION_CONTRACT,
    EXECUTION_CONTRACT_VERSION,
    PAPER_ADAPTER_VERSION,
    PAPER_GATE_VERSION,
    default_paper_equivalent_config,
    run_paper_equivalent_backtest,
)
from laoma_signal_engine.paper.models import Candle, PaperConfig


TASK_ID = "STEP7.150"
SCHEMA_VERSION = "step7.150-strategy1-2-3-4-minimal-paper-equivalent-smoke-v1"
OUTPUT_JSON = Path("DATA/backtest/step7_150_strategy1_2_3_4_minimal_paper_equivalent_smoke.json")
INVENTORY_JSON = Path("DATA/runtime/step26_1_business_chain_equivalence_inventory.json")

LINE_SPECS = (
    {"strategy": "strategy1", "line": "without_micro", "symbol": "BTCUSDT"},
    {"strategy": "strategy2", "line": "micro_fast", "symbol": "ETHUSDT"},
    {"strategy": "strategy3", "line": "micro_full", "symbol": "SOLUSDT"},
    {"strategy": "strategy4", "line": "strategy4", "symbol": "BNBUSDT"},
)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _source_for_line(line: str) -> str:
    return {
        "without_micro": "trade_plan_without_micro",
        "micro_fast": "trade_plan_micro_fast",
        "micro_full": "trade_plan_micro_full",
        "strategy4": "trade_plan_strategy4",
    }[line]


def _trade_plan_doc(line: str, *, strategy: str, symbol: str, generated_at: str) -> dict[str, Any]:
    entry = 100.0
    stop = 95.0
    take = 105.0
    planned_quantity = 1.0
    planned_notional = entry * planned_quantity
    margin = 5.0
    leverage = 20.0
    risk_budget = abs(entry - stop) * planned_quantity
    lineage = {
        "origin_run_id": f"{TASK_ID.lower()}_{line}_origin_run",
        "origin_cycle_id": f"{TASK_ID.lower()}_{line}_origin_cycle",
        "observe_pool_id": f"{TASK_ID.lower()}_{line}_observe_pool",
        "observe_attempt_id": f"{TASK_ID.lower()}_{line}_observe_attempt",
        "recheck_id": f"{TASK_ID.lower()}_{line}_recheck",
        "source": "minimal_paper_equivalent_contract_fixture",
    }
    guards: dict[str, Any] = {
        "line": line,
        "margin_usdt": margin,
        "leverage": leverage,
        "opportunity_type": "MARKET",
        "side_flow_alignment": "same",
        "price_flow_alignment": "same",
        "paper_equivalent_execution_contract": EXECUTION_CONTRACT,
        "paper_equivalent_smoke_task": TASK_ID,
        "smoke_scope": "execution_chain_contract_fixture",
    }
    input_refs: dict[str, Any] = {
        "smoke_fixture": "minimal_paper_equivalent_contract_fixture",
        "historical_candle_fixture": f"DATA/backtest/paper_equivalent/{TASK_ID.lower()}_{line}/synthetic_1m_candles",
    }
    if line in {"micro_fast", "micro_full"}:
        guards.update(
            {
                "micro_symbol_confirmed": True,
                "micro_direction_confirmed": True,
                "micro_exec_allowed": True,
                "micro_exec_allowed_reason": "allowed",
                "micro_consumption_policy": "confirmed_only",
                "trade_plan_consumable": True,
                "micro_lifecycle_state": "confirmed",
            }
        )
        input_refs["micro_readiness_fixture"] = f"{line}_confirmed_contract_fixture"
    if line == "strategy4":
        guards["strategy4_lineage"] = lineage
        input_refs["strategy4_lineage"] = lineage
    plan = {
        "symbol": symbol,
        "decision_tf": "15m",
        "decision": "LONG",
        "action": "ENTER_MARKET",
        "entry_mode": "MARKET",
        "estimated_entry_price": entry,
        "stop_loss": stop,
        "take_profit": take,
        "risk_per_unit": abs(entry - stop),
        "reward_per_unit": abs(take - entry),
        "rr": 1.0,
        "executable": True,
        "confidence": 80,
        "reason_codes": [f"{TASK_ID.lower()}_minimal_smoke"],
        "position_sizing": {
            "method": "step7_150_minimal_contract_fixture",
            "margin_usdt": margin,
            "leverage": leverage,
            "planned_quantity": planned_quantity,
            "planned_notional_usdt": planned_notional,
            "estimated_max_loss_usdt": risk_budget,
            "risk_budget_usdt": risk_budget,
        },
        "guards": guards,
        "input_refs": input_refs,
    }
    return {
        "schema_version": "1.0",
        "generated_at": generated_at,
        "run_id": f"{TASK_ID.lower()}_{line}_source_run",
        "cycle_id": f"{TASK_ID.lower()}_{line}_source_cycle",
        "source": _source_for_line(line),
        "micro_mode": line,
        "status": "ok",
        "count": 1,
        "executable_count": 1,
        "input_refs": input_refs,
        "plans": [plan],
    }


def _candles(symbol: str) -> list[Candle]:
    return [
        Candle(symbol=symbol, open_time_ms=1, open=100.0, high=101.0, low=99.0, close=100.0, volume=1000.0),
        Candle(symbol=symbol, open_time_ms=2, open=100.0, high=106.0, low=99.0, close=105.0, volume=1000.0),
    ]


def _sample_rows(db_path: Path, table: str, columns: list[str], *, limit: int = 3) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        selected = [column for column in columns if column in existing]
        if not selected:
            return []
        order_column = "rowid"
        rows = conn.execute(
            f"SELECT {', '.join(selected)} FROM {table} ORDER BY {order_column} DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    return [dict(row) for row in rows]


def _ledger_samples(db_path: Path) -> dict[str, list[dict[str, Any]]]:
    return {
        "paper_intent_inbox": _sample_rows(
            db_path,
            "paper_intent_inbox",
            [
                "intent_id",
                "strategy_line",
                "symbol",
                "side",
                "source_run_id",
                "source_cycle_id",
                "source_plan_hash",
                "status",
                "skip_reason",
                "gate_decision",
            ],
        ),
        "paper_skip_ledger": _sample_rows(
            db_path,
            "paper_skip_ledger",
            [
                "strategy_line",
                "symbol",
                "side",
                "source_run_id",
                "source_cycle_id",
                "source_plan_hash",
                "skip_reason",
                "gate_decision",
            ],
        ),
        "paper_orders": _sample_rows(
            db_path,
            "paper_orders",
            [
                "id",
                "strategy_line",
                "symbol",
                "side",
                "source_run_id",
                "source_cycle_id",
                "source_plan_hash",
                "status",
                "gate_decision",
                "fill_model",
                "realized_pnl_usdt",
            ],
        ),
        "paper_positions": _sample_rows(
            db_path,
            "paper_positions",
            [
                "id",
                "order_id",
                "strategy_line",
                "symbol",
                "side",
                "source_run_id",
                "source_cycle_id",
                "source_plan_hash",
                "status",
                "entry_price",
                "realized_pnl_usdt",
                "closed_at",
            ],
        ),
        "paper_fills": _sample_rows(
            db_path,
            "paper_fills",
            [
                "id",
                "order_id",
                "position_id",
                "strategy_line",
                "symbol",
                "side",
                "action",
                "fill_price",
                "net_pnl_usdt",
                "candle_open_time_ms",
            ],
        ),
    }


def _equivalence_status(result: dict[str, Any]) -> str:
    counts = result.get("counts") or {}
    intents = int(counts.get("paper_intent_inbox") or 0)
    skips = int(counts.get("paper_skip_ledger") or 0)
    orders = int(counts.get("paper_orders") or 0)
    positions = int(counts.get("paper_positions") or 0)
    fills = int(counts.get("paper_fills") or 0)
    if intents >= 1 and orders >= 1 and positions >= 1 and fills >= 2:
        return "field_mapped_equivalent"
    if skips >= 1:
        return "adapter_only_blocked"
    if intents >= 1:
        return "comparison_only"
    return "not_comparable"


def _line_summary(strategy: str, line: str, result: dict[str, Any]) -> dict[str, Any]:
    db_path = Path(str(result.get("db_path") or ""))
    samples = _ledger_samples(db_path)
    consume = result.get("consume") or {}
    plan_hash = None
    intent_rows = samples.get("paper_intent_inbox") or []
    if intent_rows:
        plan_hash = intent_rows[0].get("source_plan_hash")
    elif consume.get("skipped"):
        plan_hash = consume["skipped"][0].get("source_plan_hash")
    status = _equivalence_status(result)
    return {
        "strategy": strategy,
        "strategy_line": line,
        "equivalence_status": status,
        "execution_contract": result.get("execution_contract"),
        "execution_contract_version": result.get("execution_contract_version"),
        "paper_adapter_version": result.get("paper_adapter_version"),
        "paper_gate_version": result.get("paper_gate_version"),
        "paper_fill_model": result.get("paper_fill_model"),
        "paper_equivalent_run_id": result.get("paper_equivalent_run_id"),
        "db_path": result.get("db_path"),
        "source_plan_hash": plan_hash,
        "consume": {
            "intents": consume.get("intents"),
            "created": consume.get("created"),
            "skipped_count": len(consume.get("skipped") or []),
            "skip_reasons": sorted({str(row.get("skip_reason") or row.get("reason") or "") for row in consume.get("skipped") or [] if row}),
        },
        "counts": result.get("counts"),
        "ledger_samples": samples,
    }


def _update_step26_inventory(root: Path, output_rel: Path, results: list[dict[str, Any]], generated_at: str) -> dict[str, Any] | None:
    path = root / INVENTORY_JSON
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    by_line = {row["strategy_line"]: row for row in results}
    evidence = {
        line: {
            "status": row["equivalence_status"],
            "db_path": row["db_path"],
            "created_orders": row["consume"]["created"],
            "skip_rows": (row.get("counts") or {}).get("paper_skip_ledger"),
            "source_plan_hash": row.get("source_plan_hash"),
        }
        for line, row in by_line.items()
    }
    data["step7_150"] = {
        "status": "ok",
        "source": str(output_rel),
        "generated_at": generated_at,
        "execution_contract": EXECUTION_CONTRACT,
        "execution_contract_version": EXECUTION_CONTRACT_VERSION,
        "paper_adapter_version": PAPER_ADAPTER_VERSION,
        "paper_gate_version": PAPER_GATE_VERSION,
        "by_line": evidence,
    }
    for item in data.get("strategy_inventory") or []:
        line = item.get("strategy_line")
        row = by_line.get(line)
        if not row:
            continue
        item["backtest_paper_equivalence"] = row["equivalence_status"]
        item["backtest_reason"] = (
            "STEP7.150 minimal smoke uses paper.adapter + isolated PaperEngine + paper ledger/fill state machine; "
            "signal generation is a contract fixture, not a profit/promotion run."
        )
        item["step7_150_evidence"] = evidence[line]
    old_gap = "S1/S2/S3/S4 do not yet have current paper-equivalent run evidence."
    gaps = [gap for gap in data.get("blocking_gaps") or [] if gap != old_gap]
    if len(gaps) != len(data.get("blocking_gaps") or []):
        data["resolved_gaps"] = sorted(set((data.get("resolved_gaps") or []) + [old_gap]))
    data["blocking_gaps"] = gaps
    _write_json(path, data)
    return data


def _write_report(root: Path, payload: dict[str, Any], report_path: Path) -> None:
    rows = payload["results"]
    lines = [
        f"# {TASK_ID} Strategy1/2/3/4 Minimal Paper-Equivalent Smoke",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- output_json: `{payload['output_json']}`",
        f"- execution_contract: `{payload['execution_contract']}`",
        f"- execution_contract_version: `{payload['execution_contract_version']}`",
        f"- evidence_scope: `{payload['evidence_scope']}`",
        "",
        "## Conclusion",
        "",
        "本报告只断言 S1/S2/S3/S4 的最小执行链条可进入 paper-equivalent 合约：TradePlanLineDocument -> paper.adapter -> PaperIntent -> isolated PaperEngine -> paper ledger -> historical candle fill。它不是收益优化或 promotion 证据。",
        "",
        "| Strategy | Line | Status | Created | Skips | Orders | Positions | Fills | DB |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        counts = row.get("counts") or {}
        consume = row.get("consume") or {}
        lines.append(
            "| {strategy} | {line} | {status} | {created} | {skips} | {orders} | {positions} | {fills} | `{db}` |".format(
                strategy=row["strategy"],
                line=row["strategy_line"],
                status=row["equivalence_status"],
                created=consume.get("created"),
                skips=counts.get("paper_skip_ledger"),
                orders=counts.get("paper_orders"),
                positions=counts.get("paper_positions"),
                fills=counts.get("paper_fills"),
                db=row["db_path"],
            )
        )
    lines.extend(
        [
            "",
            "## Chain Evidence",
            "",
        ]
    )
    for row in rows:
        counts = row.get("counts") or {}
        lines.extend(
            [
                f"### {row['strategy']} / {row['strategy_line']}",
                "",
                f"- run_id: `{row['paper_equivalent_run_id']}`",
                f"- source_plan_hash: `{row.get('source_plan_hash')}`",
                f"- counts: `intent={counts.get('paper_intent_inbox')}`, `skip={counts.get('paper_skip_ledger')}`, `order={counts.get('paper_orders')}`, `position={counts.get('paper_positions')}`, `fill={counts.get('paper_fills')}`",
                f"- consume: `created={row['consume'].get('created')}`, `skipped={row['consume'].get('skipped_count')}`, `skip_reasons={row['consume'].get('skip_reasons')}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Boundary",
            "",
            "- 未修改策略参数或 `config/default.yaml`。",
            "- 未写入生产 `DATA/paper/paper_trading.db`。",
            "- 使用隔离 SQLite ledger：`DATA/backtest/paper_equivalent/<run_id>/paper_equivalent.db`。",
            "- S2/S3 的 micro readiness 使用 contract fixture，只证明 paper adapter/engine 字段映射，不证明真实 micro 历史信号质量。",
            "- S4 保留 observe/recheck lineage fixture，只证明 strategy4 lineage 能进入 PaperIntent 与 ledger。",
            "",
        ]
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")


def run_smoke(root: Path = PROJECT_ROOT, *, stamp: str | None = None) -> dict[str, Any]:
    project_root = Path(root).resolve()
    got_stamp = stamp or _stamp()
    generated_at = _now()
    results: list[dict[str, Any]] = []
    for spec in LINE_SPECS:
        line = spec["line"]
        symbol = spec["symbol"]
        run_id = f"step7_150_{line}_{got_stamp}"
        doc = _trade_plan_doc(line, strategy=spec["strategy"], symbol=symbol, generated_at=generated_at)
        result = run_paper_equivalent_backtest(
            project_root,
            docs={line: doc},
            candles_by_symbol={symbol: _candles(symbol)},
            run_id=run_id,
            config=default_paper_equivalent_config(
                run_id=run_id,
                base=PaperConfig(default_slippage_bps=0, taker_fee_bps=0, maker_fee_bps=0),
            ),
        )
        results.append(_line_summary(spec["strategy"], line, result))

    output_rel = OUTPUT_JSON
    report_rel = Path("docs/reports") / f"STEP7.150_strategy1_2_3_4_minimal_paper_equivalent_smoke_{got_stamp}.md"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "status": "ok",
        "generated_at": generated_at,
        "output_json": str(output_rel),
        "report": str(report_rel),
        "evidence_scope": "minimal_execution_chain_contract_fixture_not_profit_or_promotion_evidence",
        "execution_contract": EXECUTION_CONTRACT,
        "execution_contract_version": EXECUTION_CONTRACT_VERSION,
        "paper_adapter_version": PAPER_ADAPTER_VERSION,
        "paper_gate_version": PAPER_GATE_VERSION,
        "strategy_mapping": {spec["strategy"]: spec["line"] for spec in LINE_SPECS},
        "results": results,
    }
    _write_json(project_root / output_rel, payload)
    _write_report(project_root, payload, project_root / report_rel)
    _update_step26_inventory(project_root, output_rel, results, generated_at)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--stamp", default=None)
    args = parser.parse_args()
    payload = run_smoke(args.root, stamp=args.stamp)
    print(json.dumps({"status": payload["status"], "output_json": payload["output_json"], "report": payload["report"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
