from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.training_snapshot_sync import canonical_json, sidecar_db_path, stable_hash


TASK_ID = "STEP29.12"
POLICY_VERSION = "step29_trade_quality_module_backfill_v1"
SIDECAR_DB = sidecar_db_path(ROOT)
P21_DB = ROOT / "DATA" / "backtest" / "p21_parameter_optimization.db"
OUTPUT_JSON = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_12_trade_quality_label_backfill_summary.json"
CON_CACHE: dict[str, sqlite3.Connection | None] = {}
TABLE_CACHE: dict[str, set[str]] = {}
COLUMN_CACHE: dict[tuple[str, str], set[str]] = {}

TQ_REQUIRED_FIELDS = ("net_R", "MFE_R", "MAE_R", "holding_time_sec", "exit_reason")
POST_TRADE_KEYS = {
    "net_R",
    "MFE_R",
    "MAE_R",
    "holding_time",
    "holding_time_sec",
    "exit_reason",
    "root_cause_label",
    "gross_pnl_usdt",
    "net_pnl_usdt",
    "exit_price",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_json(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def open_ro(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    key = path.resolve().as_posix()
    if key not in CON_CACHE:
        con = sqlite3.connect(f"file:{key}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        CON_CACHE[key] = con
    return CON_CACHE[key]


def table_names(con: sqlite3.Connection) -> set[str]:
    key = str(id(con))
    if key not in TABLE_CACHE:
        TABLE_CACHE[key] = {str(row["name"]) for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    return TABLE_CACHE[key]


def columns(con: sqlite3.Connection, table: str) -> set[str]:
    key = (str(id(con)), table)
    if key not in COLUMN_CACHE:
        COLUMN_CACHE[key] = {str(row["name"]) for row in con.execute(f"PRAGMA table_info({table})").fetchall()}
    return COLUMN_CACHE[key]


def fetch_one_by_order(con: sqlite3.Connection, table: str, order_id: str) -> dict[str, Any] | None:
    cols = columns(con, table)
    for key in ("order_id", "trade_id"):
        if key in cols:
            row = con.execute(f"SELECT * FROM {table} WHERE {key}=? ORDER BY rowid LIMIT 1", (order_id,)).fetchone()
            if row:
                return dict(row)
    return None


def fetch_source_order(source_db_path: str, order_id: str) -> dict[str, Any]:
    path = ROOT / source_db_path
    con = open_ro(path)
    if con is None:
        return {}
    names = table_names(con)
    if "paper_orders" in names:
        row = con.execute("SELECT * FROM paper_orders WHERE id=? LIMIT 1", (order_id,)).fetchone()
        if row:
            return dict(row)
    if "sandbox_orders" in names:
        row = con.execute("SELECT * FROM sandbox_orders WHERE order_id=? LIMIT 1", (order_id,)).fetchone()
        if row:
            return dict(row)
    return {}


def p21_order_id_from_source_order(order: dict[str, Any]) -> str | None:
    text = " ".join(str(order.get(key) or "") for key in ("source_run_id", "source_cycle_id", "source_plan_hash", "intent_id"))
    match = re.search(r"(p21v2ord_[A-Za-z0-9]+)", text)
    return match.group(1) if match else None


def find_tq_from_source_db(source_db_path: str, order_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    path = ROOT / source_db_path
    con = open_ro(path)
    if con is None:
        return None, None
    names = table_names(con)
    if "trade_quality_samples" in names:
        row = fetch_one_by_order(con, "trade_quality_samples", order_id)
        if row:
            return row, {
                "trade_quality_provider": "trade_quality_module",
                "trade_quality_module": "laoma_signal_engine.trade_quality.engine",
                "source_db_path": source_db_path,
                "source_table": "trade_quality_samples",
                "source_row_id": row.get("sample_id") or row.get("diagnostic_id") or order_id,
            }
    if "backtest_trade_quality_samples" in names:
        row = fetch_one_by_order(con, "backtest_trade_quality_samples", order_id)
        if row:
            return row, {
                "trade_quality_provider": "backtest_trade_quality_module",
                "trade_quality_module": "laoma_signal_engine.backtest.p21_trade_quality",
                "source_db_path": source_db_path,
                "source_table": "backtest_trade_quality_samples",
                "source_row_id": row.get("diagnostic_id") or row.get("sample_id") or order_id,
            }
    return None, None


def find_tq_from_p21(order: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    p21_order_id = p21_order_id_from_source_order(order)
    if not p21_order_id:
        return None, None
    con = open_ro(P21_DB)
    if con is None:
        return None, None
    names = table_names(con)
    if "backtest_trade_quality_samples" not in names:
        return None, None
    row = con.execute(
        "SELECT * FROM backtest_trade_quality_samples WHERE order_id=? OR trade_id=? ORDER BY updated_at DESC LIMIT 1",
        (p21_order_id, p21_order_id),
    ).fetchone()
    if not row:
        return None, None
    data = dict(row)
    return data, {
        "trade_quality_provider": "backtest_trade_quality_module",
        "trade_quality_module": "laoma_signal_engine.backtest.p21_trade_quality",
        "source_db_path": P21_DB.relative_to(ROOT).as_posix(),
        "source_table": "backtest_trade_quality_samples",
        "source_row_id": data.get("diagnostic_id") or p21_order_id,
        "source_order_id": p21_order_id,
    }


def as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def normalize_tq(row: dict[str, Any], lineage: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    holding_time_sec = as_float(row.get("holding_time_sec"))
    if holding_time_sec is None:
        holding_time_sec = as_float(row.get("holding_sec"))
    if holding_time_sec is None and row.get("holding_minutes") is not None:
        minutes = as_float(row.get("holding_minutes"))
        holding_time_sec = minutes * 60.0 if minutes is not None else None
    net_r = as_float(row.get("net_R"))
    mfe_r = as_float(row.get("MFE_R"))
    mae_r = as_float(row.get("MAE_R"))
    exit_reason = row.get("exit_reason")
    root_cause = row.get("root_cause_label") or row.get("root_cause")
    confidence = as_float(row.get("root_cause_confidence"))
    manual = bool(row.get("needs_manual_review") in (1, "1", True, "true", "True"))
    missing = [
        field
        for field, value in (
            ("net_R", net_r),
            ("MFE_R", mfe_r),
            ("MAE_R", mae_r),
            ("holding_time_sec", holding_time_sec),
            ("exit_reason", exit_reason),
        )
        if value in (None, "")
    ]
    quality_label = "unknown"
    if net_r is not None:
        quality_label = "winner" if net_r > 0 else "loser"
    post_trade = {
        "net_R": net_r,
        "MFE_R": mfe_r,
        "MAE_R": mae_r,
        "holding_time_sec": holding_time_sec,
        "exit_reason": exit_reason,
        "gross_pnl_usdt": as_float(row.get("gross_pnl_usdt") if "gross_pnl_usdt" in row else row.get("gross_pnl")),
        "net_pnl_usdt": as_float(row.get("net_pnl_usdt") if "net_pnl_usdt" in row else row.get("net_pnl")),
        "exit_price": as_float(row.get("exit_price")),
        "trade_quality_provider": lineage.get("trade_quality_provider"),
        "trade_quality_module": lineage.get("trade_quality_module"),
        "trade_quality_module_version": row.get("label_schema_version") or row.get("schema_version") or row.get("sample_schema_version"),
        "trade_quality_source_ref": lineage,
        "missing_fields_json": missing,
    }
    label = {
        "root_cause_label": root_cause or "unknown",
        "root_cause_confidence": confidence,
        "bad_trade_flag": bool(net_r is not None and net_r < 0),
        "quality_label": quality_label,
        "entry_quality_label": row.get("entry_quality_label"),
        "market_context_label": row.get("entry_context_v3_label") or row.get("market_context_label"),
        "review_status": "needs_human_review" if manual or missing else "ready",
        "training_label_ready": not missing and not manual,
        "trade_quality_provider": lineage.get("trade_quality_provider"),
        "trade_quality_module": lineage.get("trade_quality_module"),
        "trade_quality_module_version": post_trade["trade_quality_module_version"],
        "trade_quality_source_ref": lineage,
        "missing_fields_json": missing,
    }
    return post_trade, label


def missing_label(reason: str, existing_outcome: dict[str, Any] | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    outcome = dict(existing_outcome or {})
    outcome.update(
        {
            "trade_quality_provider": None,
            "trade_quality_module": None,
            "trade_quality_status": reason,
            "missing_fields_json": list(TQ_REQUIRED_FIELDS),
        }
    )
    label = {
        "review_status": "needs_human_review",
        "training_label_ready": False,
        "trade_quality_status": reason,
        "trade_quality_provider": None,
        "trade_quality_module": None,
        "missing_fields_json": list(TQ_REQUIRED_FIELDS),
        "root_cause_label": "unknown",
        "quality_label": "needs_human_review",
        "bad_trade_flag": None,
    }
    return outcome, label


def decision_has_post_trade(decision: Any) -> list[str]:
    hits: list[str] = []
    def rec(value: Any, path: str = "") -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                p = f"{path}.{key}" if path else str(key)
                if key in POST_TRADE_KEYS:
                    hits.append(p)
                rec(item, p)
        elif isinstance(value, list):
            for idx, item in enumerate(value):
                rec(item, f"{path}[{idx}]")
    rec(decision)
    return hits


def process_sample(row: sqlite3.Row) -> dict[str, Any]:
    sample = dict(row)
    sample_id = sample["sample_id"]
    label_json = parse_json(sample.get("label_json"), {})
    existing_outcome = parse_json(sample.get("post_trade_outcome_json"), {})
    dq = parse_json(sample.get("data_quality_json"), {})
    source_db_path = str(sample.get("source_db_path") or "")
    order_id = str(sample.get("order_id") or "")

    tq_row = None
    lineage = None
    tq_row, lineage = find_tq_from_source_db(source_db_path, order_id)
    source_order = fetch_source_order(source_db_path, order_id)
    if tq_row is None:
        tq_row, lineage = find_tq_from_p21(source_order)
    if tq_row is None and isinstance(label_json, dict) and label_json and label_json.get("trade_quality_provider"):
        tq_row = {**existing_outcome, **label_json}
        lineage = label_json.get("trade_quality_source_ref") or {
            "trade_quality_provider": label_json.get("trade_quality_provider"),
            "trade_quality_module": label_json.get("trade_quality_module"),
            "source_db_path": source_db_path,
            "source_table": "trade_quality_samples",
            "source_row_id": label_json.get("sample_id") or order_id,
        }

    if tq_row and lineage:
        post_trade, label = normalize_tq(tq_row, lineage)
        status = "complete" if label.get("training_label_ready") else "joined_needs_review"
        reason = "tq_module_joined"
    else:
        post_trade, label = missing_label("tq_module_missing", existing_outcome)
        status = "tq_module_missing"
        reason = "tq_module_missing"

    dq_missing = set(dq.get("missing_fields_json") or [])
    for field in list(dq_missing):
        if str(field).startswith("trade_quality."):
            dq_missing.remove(field)
    if status != "complete":
        for field in label.get("missing_fields_json") or []:
            dq_missing.add(f"trade_quality.{field}")
    dq.update(
        {
            "trade_quality_policy_version": POLICY_VERSION,
            "trade_quality_status": status,
            "trade_quality_join_reason": reason,
            "trade_quality_provider": label.get("trade_quality_provider"),
            "trade_quality_module": label.get("trade_quality_module"),
            "trade_quality_training_label_ready": bool(label.get("training_label_ready")),
            "review_status": label.get("review_status"),
            "missing_fields_json": sorted(dq_missing),
        }
    )
    decision = parse_json(sample.get("decision_time_input_json"), {})
    leakage_hits = decision_has_post_trade(decision)
    return {
        "sample_id": sample_id,
        "exit_event_id": sample.get("exit_event_id"),
        "status": status,
        "reason": reason,
        "post_trade_outcome_json": post_trade,
        "label_json": label,
        "data_quality_json": dq,
        "leakage_hits": leakage_hits,
    }


def refresh_audit_row(con: sqlite3.Connection, summary: dict[str, Any]) -> None:
    now = now_iso()
    manifest_id = f"{TASK_ID.lower()}:{stable_hash(summary)[:24]}"
    audit_id = f"{TASK_ID.lower()}:audit:{stable_hash({'at': now, 'summary': summary})[:24]}"
    coverage = {
        "sample_count": summary["sample_count"],
        "trade_quality_label_rate": summary["trade_quality_label_rate"],
        "trade_quality_module_complete_rate": summary["trade_quality_module_complete_rate"],
        "trade_quality_review_required_rate": summary["trade_quality_review_required_rate"],
        "status_counts": summary["status_counts"],
    }
    con.execute(
        """
        INSERT OR REPLACE INTO trade_snapshot_manifests (
            manifest_id, run_id, source_mode, schema_version, schema_hash, source_refs_json,
            coverage_json, dataset_hash, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            manifest_id,
            TASK_ID,
            "sidecar_trade_quality_backfill",
            POLICY_VERSION,
            stable_hash({"policy": POLICY_VERSION}),
            canonical_json([{"sidecar_db": SIDECAR_DB.relative_to(ROOT).as_posix()}]),
            canonical_json(coverage),
            stable_hash(summary),
            now,
        ),
    )
    con.execute(
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
            summary["sample_count"],
            summary["entry_exit_pair_rate"],
            summary["market_feature_complete_rate"],
            summary["trade_quality_label_rate"],
            summary["config_gate_lineage_rate"],
            summary["known_at_pass_rate"],
            canonical_json(summary["leakage_violations"][:200]),
            canonical_json(summary["missing_label_examples"][:200]),
            now,
        ),
    )


def write_report(summary: dict[str, Any]) -> Path:
    path = ROOT / "docs" / "reports" / f"STEP29.12_trade_quality_label_backfill_{stamp()}.md"
    lines = [
        "# STEP29.12 Trade Quality Label Backfill",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- sidecar_db: `{SIDECAR_DB.relative_to(ROOT).as_posix()}`",
        f"- sample_count: `{summary['sample_count']}`",
        f"- trade_quality_label_rate: `{summary['trade_quality_label_rate']}`",
        f"- trade_quality_module_complete_rate: `{summary['trade_quality_module_complete_rate']}`",
        f"- trade_quality_review_required_rate: `{summary['trade_quality_review_required_rate']}`",
        f"- leakage_violations: `{len(summary['leakage_violations'])}`",
        "",
        "## Status Counts",
        "",
    ]
    for key, value in sorted(summary["status_counts"].items()):
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Missing / Review Examples", ""])
    if summary["missing_label_examples"]:
        lines.extend(["| sample_id | status | reason |", "| --- | --- | --- |"])
        for item in summary["missing_label_examples"][:20]:
            lines.append(f"| `{item['sample_id']}` | `{item['status']}` | `{item['reason']}` |")
    else:
        lines.append("- No missing labels.")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- TQ complete labels are accepted only from Trade Quality module tables.",
            "- Paper / paper-equivalent / sandbox source facts were not used as substitute labels.",
            "- Post-trade fields were not written into `decision_time_input_json`.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    con = sqlite3.connect(SIDECAR_DB)
    con.row_factory = sqlite3.Row
    try:
        query = "SELECT * FROM trade_training_samples ORDER BY sample_id"
        params: tuple[Any, ...] = ()
        if args.limit:
            query += " LIMIT ?"
            params = (args.limit,)
        rows = con.execute(query, params).fetchall()
        results = [process_sample(row) for row in rows]
        if not args.dry_run:
            for result in results:
                con.execute(
                    """
                    UPDATE trade_training_samples
                    SET post_trade_outcome_json=?, label_json=?, data_quality_json=?
                    WHERE sample_id=?
                    """,
                    (
                        canonical_json(result["post_trade_outcome_json"]),
                        canonical_json(result["label_json"]),
                        canonical_json(result["data_quality_json"]),
                        result["sample_id"],
                    ),
                )
                if result.get("exit_event_id"):
                    con.execute(
                        "UPDATE trade_snapshot_events SET trade_quality_json=? WHERE event_id=?",
                        (canonical_json(result["label_json"]), result["exit_event_id"]),
                    )
            con.commit()

        all_dq = [parse_json(row["data_quality_json"], {}) for row in con.execute("SELECT data_quality_json FROM trade_training_samples").fetchall()]
        sample_count = len(results)
        status_counts = dict(Counter(str(item["status"]) for item in results))
        label_count = sum(1 for item in results if item["label_json"])
        complete_count = sum(1 for item in results if item["status"] == "complete")
        review_count = sum(1 for item in results if item["label_json"].get("review_status") == "needs_human_review")
        leakage = [
            {"sample_id": item["sample_id"], "field_path": field, "reason": "post_trade_field_in_decision_input"}
            for item in results
            for field in item["leakage_hits"]
        ]
        paired = con.execute("SELECT COUNT(*) FROM trade_training_samples WHERE entry_event_id IS NOT NULL AND exit_event_id IS NOT NULL").fetchone()[0]
        market_complete = sum(1 for dq in all_dq if dq.get("market_feature_completeness") == "complete" or dq.get("feature_completeness") == "complete")
        known_at = sum(1 for dq in all_dq if dq.get("known_at_pass") is True or dq.get("market_known_at_pass") is True)
        config_lineage = 0
        for row in con.execute("SELECT decision_time_input_json FROM trade_training_samples").fetchall():
            decision = parse_json(row["decision_time_input_json"], {})
            if isinstance(decision, dict) and decision.get("config_lineage"):
                config_lineage += 1
        missing = [
            {"sample_id": item["sample_id"], "status": item["status"], "reason": item["reason"]}
            for item in results
            if item["status"] != "complete"
        ]
        summary = {
            "task_id": TASK_ID,
            "policy_version": POLICY_VERSION,
            "generated_at": now_iso(),
            "dry_run": args.dry_run,
            "sample_count": sample_count,
            "status_counts": status_counts,
            "trade_quality_label_rate": round(label_count / sample_count, 8) if sample_count else 0.0,
            "trade_quality_module_complete_rate": round(complete_count / sample_count, 8) if sample_count else 0.0,
            "trade_quality_review_required_rate": round(review_count / sample_count, 8) if sample_count else 0.0,
            "entry_exit_pair_rate": round(paired / len(all_dq), 8) if all_dq else 0.0,
            "market_feature_complete_rate": round(market_complete / len(all_dq), 8) if all_dq else 0.0,
            "known_at_pass_rate": round(known_at / len(all_dq), 8) if all_dq else 0.0,
            "config_gate_lineage_rate": round(config_lineage / len(all_dq), 8) if all_dq else 0.0,
            "leakage_violations": leakage[:200],
            "missing_label_examples": missing[:200],
        }
        if not args.dry_run:
            refresh_audit_row(con, summary)
            con.commit()
        OUTPUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        report = write_report(summary)
        print(json.dumps({"status": "ok", "summary": str(OUTPUT_JSON), "report": str(report), **summary}, ensure_ascii=False))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
