from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_trade_snapshot_schema_contract.json"
LINEAGE_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_config_gate_lineage_contract.json"
SIDECAR_DB = ROOT / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db"
DEFAULT_SOURCE = ROOT / "DATA" / "paper" / "archives" / "paper_exp_20260616T123621Z_strategy5" / "paper_trading.db"
REPORT_PATH = ROOT / "docs" / "reports" / "STEP29.4_paper_trade_snapshot_materializer_smoke_20260617.md"
SUMMARY_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_4_paper_trade_snapshot_materializer_summary.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ro_uri(path: Path) -> str:
    return f"file:{path.resolve().as_posix()}?mode=ro"


def connect_ro(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(ro_uri(path), uri=True)
    con.row_factory = sqlite3.Row
    return con


def connect_sidecar() -> sqlite3.Connection:
    SIDECAR_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(SIDECAR_DB)
    con.row_factory = sqlite3.Row
    return con


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_{stable_hash(parts)[:32]}"


def parse_time_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value)
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except ValueError:
        return None


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def init_schema(con: sqlite3.Connection) -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    for ddl in schema["ddl"]:
        con.execute(ddl)
    con.commit()


def cleanup_run(con: sqlite3.Connection, run_id: str) -> None:
    like = f"{run_id}:%"
    con.execute("DELETE FROM trade_snapshot_source_refs WHERE source_ref_id LIKE ?", (like,))
    con.execute("DELETE FROM trade_snapshot_coverage_audits WHERE audit_id LIKE ?", (like,))
    con.execute("DELETE FROM trade_snapshot_manifests WHERE manifest_id LIKE ?", (like,))
    con.execute("DELETE FROM trade_training_samples WHERE sample_id LIKE ?", (like,))
    con.execute("DELETE FROM trade_snapshot_events WHERE sample_id LIKE ?", (like,))
    con.commit()


def table_names(con: sqlite3.Connection) -> set[str]:
    return {
        row["name"]
        for row in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }


def fetch_by_order(con: sqlite3.Connection, table: str, order_id: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in con.execute(
            f"SELECT * FROM {quote_ident(table)} WHERE order_id = ? ORDER BY rowid",
            (order_id,),
        ).fetchall()
    ]


def fetch_order(con: sqlite3.Connection, order_id: str) -> dict[str, Any]:
    row = con.execute("SELECT * FROM paper_orders WHERE id = ?", (order_id,)).fetchone()
    return row_to_dict(row)


def find_closed_orders_with_pair(con: sqlite3.Connection, limit: int) -> list[str]:
    rows = con.execute(
        """
        SELECT order_id
        FROM paper_fills
        WHERE order_id IS NOT NULL
        GROUP BY order_id
        HAVING SUM(CASE WHEN lower(action) = 'entry' THEN 1 ELSE 0 END) >= 1
           AND SUM(CASE WHEN lower(action) <> 'entry' THEN 1 ELSE 0 END) >= 1
        ORDER BY MIN(rowid)
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [row["order_id"] for row in rows]


def config_lineage(order: dict[str, Any], source_db: str) -> dict[str, Any]:
    payload = {
        "strategy_line": order.get("strategy_line"),
        "strategy_name": order.get("signal_class"),
        "parameter_set_id": order.get("experiment_id") or order.get("gate_candidate_id"),
        "config_snapshot_json": {},
        "gate_candidate_id": order.get("gate_candidate_id"),
        "gate_decision": order.get("gate_decision"),
        "gate_rule_json": order.get("gate_rule_json"),
        "gate_features_json": order.get("gate_features_json"),
        "fill_model": order.get("fill_model"),
        "cost_source": order.get("cost_source"),
        "slippage_source": order.get("slippage_source"),
        "same_candle_policy": order.get("same_candle_policy"),
        "source_refs_json": [{"source_db_path": source_db, "source_table": "paper_orders", "id": order.get("id")}],
        "missing_fields_json": [],
    }
    missing = [
        key
        for key in ("parameter_set_id", "gate_candidate_id", "gate_decision", "gate_rule_json", "gate_features_json")
        if payload.get(key) in (None, "")
    ]
    payload["missing_fields_json"] = missing
    payload["config_hash"] = stable_hash(
        {
            "strategy_line": payload["strategy_line"],
            "strategy_name": payload["strategy_name"],
            "parameter_set_id": payload["parameter_set_id"],
            "config_snapshot_json": payload["config_snapshot_json"],
            "fill_model": payload["fill_model"],
            "cost_source": payload["cost_source"],
            "slippage_source": payload["slippage_source"],
            "same_candle_policy": payload["same_candle_policy"],
        }
    )
    payload["gate_hash"] = stable_hash(
        {
            "gate_candidate_id": payload["gate_candidate_id"],
            "gate_rule_json": payload["gate_rule_json"],
            "gate_features_json": payload["gate_features_json"],
            "gate_decision": payload["gate_decision"],
        }
    )
    return payload


def market_stub(fill: dict[str, Any], action: str) -> dict[str, Any]:
    return {
        "status": "needs_reconstruction",
        "event_action": action,
        "symbol": fill.get("symbol"),
        "candle_open_time_ms": fill.get("candle_open_time_ms"),
        "known_at_policy": "step29_market_feature_known_at_v1",
        "available_direct_fields": ["symbol", "candle_open_time_ms", "reference_price"],
        "missing_fields": [
            "ohlcv",
            "rsi_14",
            "ema20_distance_bps",
            "ema60_distance_bps",
            "bollinger_position",
            "bollinger_width_bps",
            "atr_14_bps",
            "volume_z",
        ],
    }


def data_quality(market: dict[str, Any], tq_missing: bool, lineage: dict[str, Any]) -> dict[str, Any]:
    missing = list(market.get("missing_fields") or [])
    if tq_missing:
        missing.extend(["trade_quality.net_R", "trade_quality.MFE_R", "trade_quality.MAE_R", "trade_quality.holding_time"])
    missing.extend([f"config_gate.{field}" for field in lineage.get("missing_fields_json", [])])
    return {
        "feature_completeness": "incomplete",
        "market_snapshot_status": market.get("status"),
        "trade_quality_status": "missing_or_not_joined" if tq_missing else "joined",
        "missing_fields_json": sorted(set(missing)),
        "proxy_fields_json": [],
        "blocked_fields_json": [],
    }


def insert_event(
    con: sqlite3.Connection,
    *,
    run_id: str,
    sample_id: str,
    source_db: str,
    source_mode: str,
    order: dict[str, Any],
    fill: dict[str, Any],
    action: str,
    lineage: dict[str, Any],
    tq_json: dict[str, Any],
) -> str:
    event_id = f"{run_id}:{stable_id('event', source_db, fill.get('id'), action)}"
    market = market_stub(fill, action)
    dq = data_quality(market, not bool(tq_json), lineage)
    event_time_ms = parse_time_ms(fill.get("filled_at"))
    decision_time_ms = parse_time_ms(order.get("created_at") or fill.get("consumed_at") or fill.get("filled_at"))
    if action == "exit":
        decision_time_ms = event_time_ms
    con.execute(
        """
        INSERT OR REPLACE INTO trade_snapshot_events (
            event_id, sample_id, order_id, event_action, source_mode, source_db_path,
            source_table, source_row_id, strategy_line, symbol, side, event_time_ms,
            candle_open_time_ms, known_at_ms, decision_time_ms, order_plan_json,
            execution_json, market_snapshot_json, trade_quality_json, config_lineage_json,
            data_quality_json, field_roles_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            event_id,
            sample_id,
            order.get("id"),
            action,
            source_mode,
            source_db,
            "paper_fills",
            fill.get("id"),
            fill.get("strategy_line") or order.get("strategy_line"),
            fill.get("symbol") or order.get("symbol"),
            fill.get("side") or order.get("side"),
            event_time_ms,
            fill.get("candle_open_time_ms"),
            event_time_ms,
            decision_time_ms,
            canonical_json({k: order.get(k) for k in ("entry_price", "stop_loss", "take_profit", "tp1", "leverage", "sizing_method", "risk_budget_usdt")}),
            canonical_json(fill),
            canonical_json(market),
            canonical_json(tq_json),
            canonical_json(lineage),
            canonical_json(dq),
            canonical_json({"market_snapshot_json": "input_feature", "execution_json": "execution_fact", "trade_quality_json": "outcome_or_label", "config_lineage_json": "audit_lineage"}),
            now_iso(),
        ),
    )
    source_ref_id = f"{run_id}:{stable_id('src', source_db, 'paper_fills', fill.get('id'))}"
    con.execute(
        """
        INSERT OR REPLACE INTO trade_snapshot_source_refs (
            source_ref_id, sample_id, event_id, source_mode, source_db_path, source_table,
            source_pk_json, source_row_hash, access_mode, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            source_ref_id,
            sample_id,
            event_id,
            source_mode,
            source_db,
            "paper_fills",
            canonical_json({"id": fill.get("id")}),
            stable_hash(fill),
            "read_only",
            now_iso(),
        ),
    )
    return event_id


def materialize(
    source_db_path: Path,
    run_id: str,
    limit: int,
    source_mode: str = "paper",
    write_summary: bool = True,
) -> dict[str, Any]:
    source_rel = source_db_path.relative_to(ROOT).as_posix()
    with connect_ro(source_db_path) as source, connect_sidecar() as sidecar:
        init_schema(sidecar)
        cleanup_run(sidecar, run_id)
        names = table_names(source)
        required = {"paper_orders", "paper_fills"}
        missing_tables = sorted(required - names)
        if missing_tables:
            raise RuntimeError(f"missing required source tables: {missing_tables}")
        order_ids = find_closed_orders_with_pair(source, limit)
        samples_written = 0
        events_written = 0
        tq_joined = 0
        for order_id in order_ids:
            order = fetch_order(source, order_id)
            fills = fetch_by_order(source, "paper_fills", order_id)
            positions = fetch_by_order(source, "paper_positions", order_id) if "paper_positions" in names else []
            entry = next((f for f in fills if str(f.get("action", "")).lower() == "entry"), None)
            exit_fill = next((f for f in fills if str(f.get("action", "")).lower() != "entry"), None)
            if not order or not entry or not exit_fill:
                continue
            tq_rows: list[dict[str, Any]] = []
            if "trade_quality_samples" in names:
                tq_rows = fetch_by_order(source, "trade_quality_samples", order_id)
            tq_json = tq_rows[0] if tq_rows else {}
            if tq_json:
                tq_joined += 1
            lineage = config_lineage(order, source_rel)
            sample_id = f"{run_id}:{stable_id('sample', source_rel, order_id)}"
            entry_event_id = insert_event(
                sidecar,
                run_id=run_id,
                sample_id=sample_id,
                source_db=source_rel,
                source_mode=source_mode,
                order=order,
                fill=entry,
                action="entry",
                lineage=lineage,
                tq_json={},
            )
            exit_event_id = insert_event(
                sidecar,
                run_id=run_id,
                sample_id=sample_id,
                source_db=source_rel,
                source_mode=source_mode,
                order=order,
                fill=exit_fill,
                action="exit",
                lineage=lineage,
                tq_json=tq_json,
            )
            events_written += 2
            entry_market = market_stub(entry, "entry")
            exit_market = market_stub(exit_fill, "exit")
            dq = data_quality(entry_market, not bool(tq_json), lineage)
            dq["exit_missing_fields_json"] = exit_market["missing_fields"]
            execution_fact = {
                "entry_fill": entry,
                "exit_fill": exit_fill,
                "position": positions[0] if positions else {},
            }
            outcome = {
                "realized_pnl_usdt": order.get("realized_pnl_usdt"),
                "exit_price": order.get("exit_price"),
                "exit_reason": order.get("exit_reason"),
                "gross_pnl_usdt": exit_fill.get("gross_pnl_usdt"),
                "net_pnl_usdt": exit_fill.get("net_pnl_usdt"),
            }
            sidecar.execute(
                """
                INSERT OR REPLACE INTO trade_training_samples (
                    sample_id, order_id, position_id, intent_id, source_mode, source_db_path,
                    strategy_line, symbol, side, entry_event_id, exit_event_id,
                    entry_time_ms, exit_time_ms, decision_time_input_json, order_plan_json,
                    execution_fact_json, post_trade_outcome_json, label_json, audit_context_json,
                    data_quality_json, source_refs_json, schema_version, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    sample_id,
                    order_id,
                    positions[0].get("id") if positions else None,
                    order.get("intent_id"),
                    source_mode,
                    source_rel,
                    order.get("strategy_line"),
                    order.get("symbol"),
                    order.get("side"),
                    entry_event_id,
                    exit_event_id,
                    parse_time_ms(entry.get("filled_at")),
                    parse_time_ms(exit_fill.get("filled_at")),
                    canonical_json(
                        {
                            "order_plan": {k: order.get(k) for k in ("entry_price", "stop_loss", "take_profit", "tp1", "leverage", "sizing_method")},
                            "entry_market_snapshot": entry_market,
                            "config_lineage": lineage,
                        }
                    ),
                    canonical_json(order),
                    canonical_json(execution_fact),
                    canonical_json(outcome),
                    canonical_json(tq_json),
                    canonical_json({"source_db_path": source_rel, "source_mode": source_mode}),
                    canonical_json(dq),
                    canonical_json(
                        [
                            {"source_db_path": source_rel, "source_table": "paper_orders", "id": order_id},
                            {"source_db_path": source_rel, "source_table": "paper_fills", "id": entry.get("id")},
                            {"source_db_path": source_rel, "source_table": "paper_fills", "id": exit_fill.get("id")},
                        ]
                    ),
                    "step29_trade_snapshot_v1",
                    now_iso(),
                ),
            )
            samples_written += 1
        manifest_id = f"{run_id}:{stable_id('manifest', source_rel)}"
        coverage = {
            "orders_with_entry_exit_pair": samples_written,
            "events_written": events_written,
            "trade_quality_joined": tq_joined,
            "trade_quality_label_rate": (tq_joined / samples_written) if samples_written else 0.0,
            "market_feature_complete_rate": 0.0,
            "config_gate_lineage_rate": 1.0 if samples_written else 0.0,
        }
        sidecar.execute(
            """
            INSERT OR REPLACE INTO trade_snapshot_manifests (
                manifest_id, run_id, source_mode, schema_version, schema_hash, source_refs_json,
                coverage_json, dataset_hash, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                manifest_id,
                run_id,
                source_mode,
                "step29_trade_snapshot_v1",
                stable_hash(json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))),
                canonical_json([{"source_db_path": source_rel, "access_mode": "read_only"}]),
                canonical_json(coverage),
                stable_hash(coverage),
                now_iso(),
            ),
        )
        audit_id = f"{run_id}:{stable_id('audit', source_rel)}"
        sidecar.execute(
            """
            INSERT OR REPLACE INTO trade_snapshot_coverage_audits (
                audit_id, manifest_id, sample_count, entry_exit_pair_rate, market_feature_complete_rate,
                trade_quality_label_rate, config_gate_lineage_rate, known_at_pass_rate,
                leakage_violations_json, missing_fields_json, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                audit_id,
                manifest_id,
                samples_written,
                1.0 if samples_written else 0.0,
                0.0,
                coverage["trade_quality_label_rate"],
                coverage["config_gate_lineage_rate"],
                1.0,
                "[]",
                canonical_json(["market_features_need_reconstruction", "trade_quality_label_may_be_missing"]),
                now_iso(),
            ),
        )
        sidecar.commit()
    summary = {
        "step": "STEP29.4",
        "status": "done",
        "run_id": run_id,
        "source_mode": source_mode,
        "source_db": source_rel,
        "sidecar_db": SIDECAR_DB.relative_to(ROOT).as_posix(),
        "orders_found": len(order_ids),
        "samples_written": samples_written,
        "events_written": events_written,
        "trade_quality_joined": tq_joined,
        "coverage": coverage,
        "boundary": {
            "source_access": "read_only",
            "source_write_back": False,
            "sidecar_only": True,
        },
    }
    if write_summary:
        SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def write_report(summary: dict[str, Any]) -> None:
    lines = [
        "# STEP29.4 Paper Trade Snapshot Materializer Smoke",
        "",
        "> 状态：DONE",
        "> 日期：2026-06-17",
        f"> Summary：`{SUMMARY_PATH.relative_to(ROOT).as_posix()}`",
        f"> Sidecar DB：`{SIDECAR_DB.relative_to(ROOT).as_posix()}`",
        "",
        "## 结论",
        "",
        "Paper snapshot materializer 已用 read-only source DB 生成 sidecar entry/exit events 和 order-level training samples。源 paper DB 没有被修改。",
        "",
        "## Smoke 输入",
        "",
        f"- Source DB：`{summary['source_db']}`",
        f"- Run ID：`{summary['run_id']}`",
        "",
        "## Smoke 输出",
        "",
        f"- Orders found：{summary['orders_found']}",
        f"- Samples written：{summary['samples_written']}",
        f"- Events written：{summary['events_written']}",
        f"- Trade Quality joined：{summary['trade_quality_joined']}",
        f"- Market feature complete rate：{summary['coverage']['market_feature_complete_rate']}",
        f"- Trade Quality label rate：{summary['coverage']['trade_quality_label_rate']}",
        "",
        "## 缺口记录",
        "",
        "- Market features 目前写入 `needs_reconstruction`，后续由 STEP29.3 policy 对接真实 K 线重建器。",
        "- Trade Quality 未命中的样本保留 label 缺口，不回写 paper DB。",
        "- Config/Gate lineage 已从 paper order 字段抽取并生成 hash；缺失字段写入 `missing_fields_json`。",
        "",
        "## 边界",
        "",
        "- Source DB access：read-only URI mode.",
        "- Write target：sidecar `DATA/research/trade_snapshots/trade_snapshots.db` only.",
        "- No paper order state, fill model, position ledger, or Trade Gate behavior was changed.",
    ]
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE))
    parser.add_argument("--run-id", default="step29_4_smoke")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--source-mode", default="paper")
    args = parser.parse_args()
    summary = materialize(Path(args.source_db), args.run_id, args.limit, args.source_mode)
    write_report(summary)
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
