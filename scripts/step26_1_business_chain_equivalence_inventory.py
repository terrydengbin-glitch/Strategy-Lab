from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_JSON = ROOT / "DATA" / "runtime" / "step26_1_business_chain_equivalence_inventory.json"
REPORT_DIR = ROOT / "docs" / "reports"

STEP7_146_JSON = ROOT / "DATA" / "backtest" / "step7_146_strategy5_6_v5_gate_paper_equivalent_backtest.json"
STEP7_143_JSON = ROOT / "DATA" / "backtest" / "step7_143_strategy5_6_best_params_v5_trade_gate_e2e_backtest.json"
STEP7_134_JSON = ROOT / "DATA" / "paper" / "step7_134_strategy5_6_v5_gate_paper_simulation.json"


STRATEGY_LINES = ("without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _exists(path: str) -> bool:
    return (ROOT / path).exists()


def _sqlite_tables(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        with sqlite3.connect(path) as conn:
            rows = conn.execute("select name from sqlite_master where type='table' order by name").fetchall()
        return [str(row[0]) for row in rows]
    except sqlite3.Error:
        return []


def _step7_146_evidence() -> dict[str, Any]:
    payload = _read_json(STEP7_146_JSON)
    by_line: dict[str, dict[str, Any]] = {}
    for row in payload.get("results") or []:
        if not isinstance(row, dict):
            continue
        line = str(row.get("strategy_line") or "")
        branch = str(row.get("branch") or "")
        if not line or not branch:
            continue
        got = by_line.setdefault(line, {"branches": {}})
        got["branches"][branch] = {
            "executable_plans": row.get("executable_plans"),
            "consumed_plans": row.get("consumed_plans"),
            "created_orders": row.get("created_orders"),
            "skip_rows": row.get("skip_rows"),
            "gate_decisions": row.get("gate_decisions") or {},
            "db_path": row.get("db_path"),
            "profit_factor": (row.get("metrics") or {}).get("profit_factor"),
        }
    return {
        "status": payload.get("status"),
        "execution_contract": payload.get("execution_contract"),
        "execution_contract_version": payload.get("execution_contract_version"),
        "paper_adapter_version": payload.get("paper_adapter_version"),
        "paper_gate_version": payload.get("paper_gate_version"),
        "paper_fill_model": payload.get("paper_fill_model"),
        "symbol_selection": ((payload.get("window") or {}).get("symbol_selection") or {}),
        "symbol_count": (payload.get("window") or {}).get("symbol_count"),
        "source": _rel(STEP7_146_JSON),
        "by_line": by_line,
    }


def _step7_143_evidence() -> dict[str, Any]:
    payload = _read_json(STEP7_143_JSON)
    by_line: dict[str, dict[str, Any]] = {}
    for line in ("strategy5", "strategy6"):
        got = payload.get(line) if isinstance(payload.get(line), dict) else None
        if got:
            by_line[line] = got
    # Older payloads store summaries under results; keep this lightweight and
    # rely on the task card/report for exact metrics.
    return {
        "status": payload.get("status"),
        "engine_mode": payload.get("engine_mode") or "offline_real_evaluator",
        "source": _rel(STEP7_143_JSON),
        "evidence_present": STEP7_143_JSON.exists(),
        "equivalence_level": "comparison_only",
        "reason": "real evaluator was used, but fill/order execution bypassed paper.adapter and PaperEngine ledgers.",
    }


def _step7_134_evidence() -> dict[str, Any]:
    payload = _read_json(STEP7_134_JSON)
    smoke = payload.get("smoke") if isinstance(payload.get("smoke"), dict) else {}
    by_line = {}
    for line in ("strategy5", "strategy6"):
        row = smoke.get(line) if isinstance(smoke.get(line), dict) else {}
        latest = row.get("latest_plan") if isinstance(row.get("latest_plan"), dict) else {}
        by_line[line] = {
            "latest_plan_exists": bool(latest.get("exists")),
            "latest_plan_status": latest.get("status"),
            "executable_count": latest.get("executable_count"),
            "paper_eligible_count": latest.get("paper_eligible_count"),
            "pipeline_status": (row.get("latest_strategy_pipeline_report") or {}).get("status")
            if isinstance(row.get("latest_strategy_pipeline_report"), dict)
            else None,
        }
    return {
        "status": payload.get("status"),
        "experiment_id": payload.get("experiment_id"),
        "paper_epoch_id": payload.get("paper_epoch_id"),
        "source": _rel(STEP7_134_JSON),
        "paper_db": ((payload.get("paper_before") or {}).get("db_path")),
        "by_line": by_line,
    }


def _chain_components() -> dict[str, Any]:
    paper_db = ROOT / "DATA" / "paper" / "paper_trading.db"
    return {
        "paper_runtime": {
            "signal_documents": {
                "without_micro": "DATA/decisions/latest_trade_plan_without_micro.json",
                "micro_fast": "DATA/decisions/latest_trade_plan_micro_fast.json",
                "micro_full": "DATA/decisions/latest_trade_plan_micro_full.json",
                "strategy4": "DATA/decisions/latest_trade_plan_strategy4.json",
                "strategy5": "DATA/decisions/latest_trade_plan_strategy5.json",
                "strategy6": "DATA/decisions/latest_trade_plan_strategy6.json",
            },
            "order_intent": "laoma_signal_engine/paper/adapter.py::intent_from_plan_document",
            "execution_engine": "laoma_signal_engine/paper/engine.py::PaperEngine.consume_trade_plans",
            "gate": "laoma_signal_engine/paper/v5_gate.py::evaluate_paper_v5_trade_gate",
            "fill_state_machine": [
                "PaperEngine.process_pending_entries",
                "PaperEngine.process_open_positions",
                "PaperStore.create_plan_and_order",
                "PaperStore.close_position",
            ],
            "ledger": {
                "path": _rel(paper_db),
                "exists": paper_db.exists(),
                "tables": [table for table in _sqlite_tables(paper_db) if table.startswith("paper_") or table.startswith("trade_quality")],
            },
            "api_ui_consumers": [
                "laoma_signal_engine/api/services.py paper payloads",
                "web/src/App.vue paper page / config effective preview",
            ],
        },
        "paper_equivalent_backtest": {
            "adapter": "laoma_signal_engine/backtest/paper_equivalent.py",
            "contract": "paper_equivalent",
            "contract_version": "step7.145.v1",
            "same_components_as_paper": [
                "paper.adapter.adapt_documents",
                "PaperEngine.consume_trade_plans",
                "paper V5 gate",
                "paper skip/order/fill/position ledger schema",
            ],
            "intentional_differences": [
                "HistoricalCandleProvider replaces live BinanceCandleProvider",
                "isolated DB under DATA/backtest/paper_equivalent/<run_id>/paper_equivalent.db",
                "historical event clock advances by candle open_time_ms",
                "source signal generation comes from offline evaluator and historical cached inputs",
            ],
        },
        "legacy_direct_fill_backtest": {
            "script": "scripts/step7_143_strategy5_6_best_params_v5_trade_gate_e2e_backtest.py",
            "level": "comparison_only",
            "reason": "does not write through PaperIntent/PaperEngine order state machine.",
        },
        "replay": {
            "current_level": "simple comparison replay",
            "known_legacy_ref": "STEP7.143 prior gate-filtered replay",
            "promotion_allowed": False,
            "reason": "reads historical samples / filter results; does not regenerate full paper order state.",
        },
        "live": {
            "implemented": False,
            "current_equivalent_environment": "paper only",
            "reason": "no real exchange order adapter / live account sync chain is present in this P26 inventory.",
        },
    }


def _strategy_inventory(step7146: dict[str, Any], step7134: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for line in STRATEGY_LINES:
        signal_entry = {
            "without_micro": "decision.trade_plan_lines::run_apply_trade_plan_line_safe",
            "micro_fast": "decision.trade_plan_lines::run_apply_trade_plan_line_safe + micro confirmation inputs",
            "micro_full": "decision.trade_plan_lines::run_apply_trade_plan_line_safe + full micro readiness",
            "strategy4": "strategy4 observe trade plan document if latest JSON exists",
            "strategy5": "strategy5.evidence::run_strategy5_pipeline_safe -> build_strategy5_document",
            "strategy6": "strategy6.evidence::run_strategy6_pipeline_safe / run_strategy6_observe_once -> build_strategy6_document",
        }[line]
        latest_path = {
            "without_micro": "DATA/decisions/latest_trade_plan_without_micro.json",
            "micro_fast": "DATA/decisions/latest_trade_plan_micro_fast.json",
            "micro_full": "DATA/decisions/latest_trade_plan_micro_full.json",
            "strategy4": "DATA/decisions/latest_trade_plan_strategy4.json",
            "strategy5": "DATA/decisions/latest_trade_plan_strategy5.json",
            "strategy6": "DATA/decisions/latest_trade_plan_strategy6.json",
        }[line]
        has_paper_equiv_evidence = line in (step7146.get("by_line") or {})
        if line in {"strategy5", "strategy6"} and has_paper_equiv_evidence:
            backtest_equivalence = "field_mapped_equivalent"
            backtest_reason = "STEP7.146 targeted run uses paper.adapter + isolated PaperEngine + V5 gate; historical inputs and clock differ from live paper."
        elif line in {"strategy5", "strategy6"}:
            backtest_equivalence = "comparison_only"
            backtest_reason = "Legacy STEP7.143 exists, but paper-equivalent evidence is missing or not current."
        else:
            backtest_equivalence = "contract_available_no_current_evidence"
            backtest_reason = "STEP7.145 adapter supports this line, but no STEP26 paper-equivalent evidence run was found for this strategy."
        rows.append(
            {
                "strategy_line": line,
                "paper_signal_generation": signal_entry,
                "latest_trade_plan_path": latest_path,
                "latest_trade_plan_exists": _exists(latest_path),
                "paper_order_intent": "PaperIntent via paper.adapter.intent_from_plan_document",
                "paper_execution": "PaperEngine.consume_trade_plans -> gate/skip/order/fill/position",
                "backtest_paper_equivalence": backtest_equivalence,
                "backtest_reason": backtest_reason,
                "step7_146_evidence": (step7146.get("by_line") or {}).get(line),
                "step7_134_paper_evidence": (step7134.get("by_line") or {}).get(line),
                "replay_equivalence": "comparison_only",
                "live_equivalence": "not_implemented",
            }
        )
    return rows


def _write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"STEP26.1_business_chain_equivalence_inventory_{_stamp()}.md"
    lines = [
        "# STEP26.1 Backtest / Replay / Paper / Live Business Chain Equivalence Inventory",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- status: `{payload['status']}`",
        f"- output_json: `{payload['output_json']}`",
        f"- focus: `{payload['focus']}`",
        "",
        "## Executive Judgment",
        "",
        "- `strategy5` / `strategy6` 的 STEP7.146 targeted 回测当前可定级为 `field_mapped_equivalent`：订单意图、paper adapter、V5 gate、skip/order/fill/position ledger 与 paper 链条一致，但历史输入、事件时钟和 candle provider 与实时 paper 不同。",
        "- STEP7.143 direct-fill E2E 只能作为 `comparison_only`，不能作为 STEP7.134 / STEP7.135 的 promotion evidence。",
        "- `without_micro`、`micro_fast`、`micro_full`、`strategy4` 目前有 STEP7.145 paper-equivalent adapter 能力，但本轮未发现同等级 paper-equivalent 跑数证据。",
        "- live 实盘链条暂未实现；当前可比较目标应限定为 backtest/paper/paper-equivalent。",
        "",
        "## Strategy Mainline Matrix",
        "",
        "| strategy | paper signal generation | latest JSON | backtest vs paper level | replay | live |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in payload["strategy_inventory"]:
        lines.append(
            f"| `{row['strategy_line']}` | {row['paper_signal_generation']} | "
            f"`{row['latest_trade_plan_path']}` ({'exists' if row['latest_trade_plan_exists'] else 'missing'}) | "
            f"`{row['backtest_paper_equivalence']}` | `{row['replay_equivalence']}` | `{row['live_equivalence']}` |"
        )
    lines.extend(
        [
            "",
            "## Backtest / Paper Chain Nodes",
            "",
            "| node | paper runtime | paper-equivalent backtest | equivalence note |",
            "| --- | --- | --- | --- |",
            "| signal generation | live/current JSON by strategy pipeline | historical evaluator/wrapper docs | field-mapped, not same-chain |",
            "| data availability time | current factor/refresh/liquidity/micro JSON freshness | P21/P24 historical entry-known inputs | requires entry-known audit; proxy fields must be reported |",
            "| event clock | daemon cycle / paper tick / current time | historical candle `open_time_ms` cursor | different clock provider |",
            "| risk/config checks | runtime `default.yaml`, trade plan config, paper config | same loader where wrapped; explicit experiment config for gate | field-mapped; config deltas must be reported |",
            "| order intent | `PaperIntent` via `paper.adapter` | same `paper.adapter` in STEP7.145/146 | same contract |",
            "| trade gate | `paper.v5_gate` inside `PaperEngine.consume_trade_plans` | same paper V5 gate in isolated engine | same contract |",
            "| execution model | `PaperEngine` fill/cost/slippage/SL/TP state machine | same engine with `HistoricalCandleProvider` | same engine, different candle provider |",
            "| ledger/account | `DATA/paper/paper_trading.db` | `DATA/backtest/paper_equivalent/<run_id>/paper_equivalent.db` | same schema, isolated DB |",
            "| audit/UI | paper summary/API/UI and reports | JSON/report + isolated SQLite | needs equivalence report link for promotion |",
            "",
            "## STEP7 Evidence Levels",
            "",
            "| evidence | level | can promote? | reason |",
            "| --- | --- | --- | --- |",
            "| STEP7.143 E2E direct-fill | `comparison_only` | no | real evaluator, but bypasses PaperIntent/PaperEngine ledger |",
            "| STEP7.146 targeted paper-equivalent | `field_mapped_equivalent` | partial/no full promotion | uses paper adapter/engine/gate, but only targeted symbols |",
            "| STEP7.149 full 100-symbol paper-equivalent | `required_next_evidence` | pending | needed to replace STEP7.143 as full-window evidence |",
            "| STEP7.134 paper simulation | `paper_runtime_evidence` | blocked/limited | current smoke produced no strategy5 entries; strategy6 mostly wait/non-executable in captured run |",
            "| live | `not_implemented` | no | no real exchange order/account sync chain |",
            "",
            "## Current STEP7.146 Snapshot",
            "",
            f"- execution_contract: `{payload['step7_146']['execution_contract']}`",
            f"- execution_contract_version: `{payload['step7_146']['execution_contract_version']}`",
            f"- paper_adapter_version: `{payload['step7_146']['paper_adapter_version']}`",
            f"- paper_gate_version: `{payload['step7_146']['paper_gate_version']}`",
            f"- paper_fill_model: `{payload['step7_146']['paper_fill_model']}`",
            f"- symbol_selection: `{payload['step7_146']['symbol_selection']}`",
            "",
            "| strategy | branch | executable | consumed | orders | skips | gate decisions | PF |",
            "| --- | --- | ---: | ---: | ---: | ---: | --- | ---: |",
        ]
    )
    for line, detail in (payload["step7_146"].get("by_line") or {}).items():
        for branch, branch_row in (detail.get("branches") or {}).items():
            lines.append(
                f"| `{line}` | `{branch}` | {branch_row.get('executable_plans')} | "
                f"{branch_row.get('consumed_plans')} | {branch_row.get('created_orders')} | "
                f"{branch_row.get('skip_rows')} | `{branch_row.get('gate_decisions')}` | {branch_row.get('profit_factor')} |"
            )
    lines.extend(
        [
            "",
            "## Blocking Gaps",
            "",
            "- `STEP7.149` 仍是 S5/S6 全窗口 paper-equivalent promotion evidence 的必要下一步。",
            "- S1/S2/S3/S4 需要各自最小 paper-equivalent smoke，证明 STEP7.145 adapter 在这些策略上的 skip/order/gate/ledger parity。",
            "- STEP7.134/STEP7.135 需要重新以 STEP7.146/STEP7.149 的字段为对照，检查真实 paper run 是否在同一 `PaperIntent` / gate / ledger 字段上可比较。",
            "- live 实盘若未来接入，必须新增 live intent、exchange order state、account/position sync 与 paper-equivalent mapping，不得直接复用 paper 结论。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    step7146 = _step7_146_evidence()
    step7134 = _step7_134_evidence()
    payload = {
        "schema_version": "step26.1-business-chain-equivalence-inventory-v1",
        "task_id": "STEP26.1",
        "generated_at": _utc_now(),
        "status": "ok",
        "focus": "per-strategy backtest/replay/paper/live chain inventory, with backtest-paper equivalence as primary target",
        "output_json": _rel(OUTPUT_JSON),
        "chain_components": _chain_components(),
        "step7_143": _step7_143_evidence(),
        "step7_146": step7146,
        "step7_134": step7134,
        "strategy_inventory": _strategy_inventory(step7146, step7134),
        "blocking_gaps": [
            "STEP7.149 full 100-symbol paper-equivalent replay is still pending.",
            "S1/S2/S3/S4 do not yet have current paper-equivalent run evidence.",
            "STEP7.134/STEP7.135 paper evidence must be compared against PaperIntent/gate/ledger fields, not strategy name alone.",
            "live exchange/account chain is not implemented.",
        ],
    }
    report = _write_report(payload)
    payload["report"] = _rel(report)
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"status": payload["status"], "output_json": payload["output_json"], "report": payload["report"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
