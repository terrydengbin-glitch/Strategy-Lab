from __future__ import annotations

import json
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.paper.engine import PaperEngine
from laoma_signal_engine.paper.models import PaperConfig


SMOKE_ID = "step7_136_v5_gate_before_paper_bounded_smoke"
EXPERIMENT_ID = "paper_exp_step7_136_v5_gate_bounded_smoke"
SMOKE_ROOT = ROOT / "DATA" / "paper" / "step7_136_smoke"
DB_PATH = SMOKE_ROOT / "paper_trading.db"
RESULT_PATH = SMOKE_ROOT / "result.json"
REPORT_PATH = ROOT / "docs" / "reports" / "STEP7.136_strategy5_6_v5_gate_before_paper_bounded_smoke.md"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_gate_config() -> None:
    gate_dir = SMOKE_ROOT / "DATA" / "paper"
    gate_dir.mkdir(parents=True, exist_ok=True)
    config = {
        "enabled": True,
        "experiment_id": EXPERIMENT_ID,
        "paper_epoch_id": "paper_epoch_step7_136_bounded_smoke",
        "mode": "paper_experiment_bounded_smoke",
        "feature_missing_policy": "block",
        "rules": {
            "strategy5": {
                "parameter_set_id": "p21v2_72340cb432fa7977",
                "gate_candidate_id": "strategy5_v5_opposite_flow_combo_gate",
                "action": "block",
                "rule_json": {
                    "operator": "AND",
                    "rules": [
                        {"field": "side_flow_alignment", "op": "eq", "value": "opposite"},
                        {"field": "price_flow_alignment", "op": "eq", "value": "opposite"},
                    ],
                },
            },
            "strategy6": {
                "parameter_set_id": "s6v32_edcd6b1030331422",
                "gate_candidate_id": "strategy6_v5_negative_funding_short_crowded_gate",
                "action": "block",
                "rule_json": {
                    "operator": "AND",
                    "rules": [
                        {"field": "funding_bucket", "op": "eq", "value": "NEGATIVE_EXTREME"},
                        {"field": "funding_crowded_side", "op": "eq", "value": "short"},
                    ],
                },
            },
        },
    }
    (gate_dir / "v5_trade_gate_experiment.json").write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def plan(
    *,
    symbol: str,
    side: str,
    entry: float,
    sl: float,
    tp: float,
    line: str,
    guards: dict[str, Any],
) -> dict[str, Any]:
    decision = "LONG" if side.upper() == "LONG" else "SHORT"
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    return {
        "symbol": symbol,
        "decision_tf": "15m",
        "decision": decision,
        "action": "ENTER_MARKET",
        "entry_mode": "MARKET",
        "estimated_entry_price": entry,
        "stop_loss": sl,
        "take_profit": tp,
        "risk_per_unit": risk,
        "reward_per_unit": reward,
        "rr": round(reward / risk, 6) if risk else 0,
        "executable": True,
        "confidence": 80,
        "reason_codes": [SMOKE_ID],
        "guards": {
            "line": line,
            "margin_usdt": 100,
            "leverage": 20,
            **guards,
        },
        "input_refs": {"bounded_smoke": SMOKE_ID},
    }


def doc(line: str, plans: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "generated_at": utc_now(),
        "run_id": f"run_{SMOKE_ID}_{line}",
        "cycle_id": f"cycle_{SMOKE_ID}",
        "source": f"trade_plan_{line}",
        "micro_mode": "none",
        "status": "ok",
        "count": len(plans),
        "executable_count": len(plans),
        "input_refs": {"bounded_smoke": SMOKE_ID},
        "plans": plans,
    }


def smoke_docs() -> dict[str, dict[str, Any]]:
    return {
        "strategy5": doc(
            "strategy5",
            [
                plan(
                    symbol="S5BLOCKUSDT",
                    side="LONG",
                    entry=100.0,
                    sl=99.0,
                    tp=101.0,
                    line="strategy5",
                    guards={
                        "side_flow_alignment": "opposite",
                        "price_flow_alignment": "opposite",
                        "smoke_case": "strategy5_block",
                    },
                ),
                plan(
                    symbol="S5PASSUSDT",
                    side="LONG",
                    entry=100.0,
                    sl=99.0,
                    tp=101.0,
                    line="strategy5",
                    guards={
                        "side_flow_alignment": "same",
                        "price_flow_alignment": "same",
                        "smoke_case": "strategy5_pass",
                    },
                ),
            ],
        ),
        "strategy6": doc(
            "strategy6",
            [
                plan(
                    symbol="S6BLOCKUSDT",
                    side="SHORT",
                    entry=100.0,
                    sl=101.0,
                    tp=99.0,
                    line="strategy6",
                    guards={
                        "funding_bucket": "NEGATIVE_EXTREME",
                        "funding_crowded_side": "short",
                        "smoke_case": "strategy6_block",
                    },
                ),
                plan(
                    symbol="S6PASSUSDT",
                    side="SHORT",
                    entry=100.0,
                    sl=101.0,
                    tp=99.0,
                    line="strategy6",
                    guards={
                        "funding_bucket": "NEUTRAL",
                        "funding_crowded_side": "neutral",
                        "smoke_case": "strategy6_pass",
                    },
                ),
            ],
        ),
    }


def rows(table: str) -> list[dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        got = conn.execute(f"SELECT * FROM {table} ORDER BY rowid ASC").fetchall()
        return [dict(row) for row in got]
    finally:
        conn.close()


def assert_lineage(result: dict[str, Any]) -> None:
    orders = rows("paper_orders")
    skips = rows("paper_skip_ledger")
    inbox = rows("paper_intent_inbox")
    plans = rows("paper_trade_plans")

    pass_orders = [row for row in orders if row.get("gate_decision") == "pass"]
    blocked_skips = [row for row in skips if row.get("gate_decision") == "blocked"]
    gated_inbox = [row for row in inbox if row.get("gate_decision") in {"pass", "blocked"}]

    if result.get("created") != 2:
        raise AssertionError(f"expected 2 pass orders, got {result.get('created')}")
    if len(pass_orders) != 2:
        raise AssertionError(f"expected 2 gate pass orders, got {len(pass_orders)}")
    if len(blocked_skips) != 2:
        raise AssertionError(f"expected 2 gate blocked skips, got {len(blocked_skips)}")
    if len(gated_inbox) != 4:
        raise AssertionError(f"expected 4 gated intent inbox rows, got {len(gated_inbox)}")

    expected_order_symbols = {"S5PASSUSDT", "S6PASSUSDT"}
    expected_skip_symbols = {"S5BLOCKUSDT", "S6BLOCKUSDT"}
    if {row.get("symbol") for row in pass_orders} != expected_order_symbols:
        raise AssertionError(f"unexpected pass order symbols: {pass_orders}")
    if {row.get("symbol") for row in blocked_skips} != expected_skip_symbols:
        raise AssertionError(f"unexpected blocked skip symbols: {blocked_skips}")

    for row in [*pass_orders, *blocked_skips, *gated_inbox]:
        if row.get("experiment_id") != EXPERIMENT_ID:
            raise AssertionError(f"missing experiment lineage: {row}")
        if not row.get("gate_candidate_id"):
            raise AssertionError(f"missing gate candidate lineage: {row}")
        if not row.get("gate_rule_json"):
            raise AssertionError(f"missing gate rule lineage: {row}")
        features = json.loads(row.get("gate_features_json") or "{}")
        if not features:
            raise AssertionError(f"missing gate feature lineage: {row}")

    return {
        "orders": pass_orders,
        "skips": blocked_skips,
        "inbox": gated_inbox,
        "inbox_count": len(inbox),
        "trade_plan_count": len(plans),
    }


def write_report(payload: dict[str, Any]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# STEP7.136 Strategy5/6 V5 Gate-Before-Paper Bounded Smoke",
        "",
        f"> generated_at: {payload['generated_at']}",
        f"> status: `{payload['status']}`",
        f"> experiment_id: `{EXPERIMENT_ID}`",
        "",
        "## Summary",
        "",
        "Bounded smoke passed using an isolated paper SQLite DB. Controlled strategy5/strategy6 trade plans were evaluated by V5 trade gate before paper order creation.",
        "",
        "## Evidence",
        "",
        f"- smoke root: `{SMOKE_ROOT.relative_to(ROOT)}`",
        f"- smoke db: `{DB_PATH.relative_to(ROOT)}`",
        f"- created pass orders: `{payload['consume_result']['created']}`",
        f"- blocked skips: `{len(payload['lineage']['skips'])}`",
        f"- inbox rows: `{payload['lineage']['inbox_count']}`",
        f"- trade plan rows: `{payload['lineage']['trade_plan_count']}`",
        "",
        "## Gate Lineage",
        "",
        "```json",
        json.dumps(
            {
                "orders": payload["lineage"]["orders"],
                "skips": payload["lineage"]["skips"],
                "inbox": payload["lineage"]["inbox"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        "```",
        "",
        "## Decision",
        "",
        "V5 gate-before-paper lineage is complete in the bounded smoke. STEP7.135 can wait for real executable strategy5/6 market signals before restarting the 10h paper audit.",
        "",
    ]
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    if SMOKE_ROOT.exists():
        shutil.rmtree(SMOKE_ROOT)
    SMOKE_ROOT.mkdir(parents=True, exist_ok=True)
    write_gate_config()

    config = PaperConfig(
        db_path="paper_trading.db",
        summary_path="latest_paper_state.json",
        default_slippage_bps=0,
        taker_fee_bps=5,
    )
    engine = PaperEngine(SMOKE_ROOT, config=config)
    consume_result = engine.consume_trade_plans(smoke_docs())
    lineage = assert_lineage(consume_result)

    payload = {
        "generated_at": utc_now(),
        "status": "PASS",
        "experiment_id": EXPERIMENT_ID,
        "smoke_root": str(SMOKE_ROOT),
        "db_path": str(DB_PATH),
        "consume_result": consume_result,
        "lineage": lineage,
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(payload)
    print(json.dumps({"status": "PASS", "db_path": str(DB_PATH), "report": str(REPORT_PATH)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
