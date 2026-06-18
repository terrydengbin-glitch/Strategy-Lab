from __future__ import annotations

import argparse
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

from laoma_signal_engine.training_readiness.tq_completion_v2 import TQ_COMPLETION_POLICY_VERSION, classify_tq_completion_v2
from laoma_signal_engine.training_snapshot_sync import canonical_json, sidecar_db_path, stable_hash


TASK_ID = "STEP29.18"
SIDECAR_DB = sidecar_db_path(ROOT)
OUT_DIR = ROOT / "DATA" / "research" / "trade_snapshots"
SUMMARY_JSON = OUT_DIR / "step29_18_tq_module_completion_backfill_v2_summary.json"


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


def process_row(row: sqlite3.Row) -> dict[str, Any]:
    label = parse_json(row["label_json"], {})
    outcome = parse_json(row["post_trade_outcome_json"], {})
    if row["exit_time_ms"] is not None and "exit_time_ms" not in outcome:
        outcome["exit_time_ms"] = row["exit_time_ms"]
    dq = parse_json(row["data_quality_json"], {})
    decision = parse_json(row["decision_time_input_json"], {})
    result = classify_tq_completion_v2(label, outcome, dq, decision_time_input_json=decision)
    return {
        "sample_id": row["sample_id"],
        "exit_event_id": row["exit_event_id"],
        **result,
    }


def refresh_audit_row(con: sqlite3.Connection, summary: dict[str, Any]) -> None:
    now = now_iso()
    manifest_id = f"step29_18:{stable_hash(summary)[:24]}"
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
            "sidecar_trade_quality_completion_v2",
            TQ_COMPLETION_POLICY_VERSION,
            stable_hash({"policy": TQ_COMPLETION_POLICY_VERSION}),
            canonical_json([{"sidecar_db": SIDECAR_DB.relative_to(ROOT).as_posix()}]),
            canonical_json(summary),
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
            f"step29_18:audit:{stable_hash(summary)[:24]}",
            manifest_id,
            summary["sample_count"],
            summary["entry_exit_pair_rate"],
            summary["market_feature_complete_rate"],
            summary["trade_quality_label_rate"],
            summary["config_gate_lineage_rate"],
            summary["known_at_pass_rate"],
            "[]",
            canonical_json(summary["missing_examples"][:200]),
            now,
        ),
    )


def compute_rates(con: sqlite3.Connection, results: list[dict[str, Any]]) -> dict[str, Any]:
    sample_count = len(results)
    rows = con.execute("SELECT data_quality_json, decision_time_input_json, label_json FROM trade_training_samples").fetchall()
    paired = con.execute(
        "SELECT COUNT(*) FROM trade_training_samples WHERE entry_event_id IS NOT NULL AND exit_event_id IS NOT NULL"
    ).fetchone()[0]
    market_complete = 0
    known_at = 0
    config_lineage = 0
    label_any = 0
    for row in rows:
        dq = parse_json(row["data_quality_json"], {})
        decision = parse_json(row["decision_time_input_json"], {})
        if dq.get("market_feature_completeness") == "complete" or dq.get("feature_completeness") == "complete":
            market_complete += 1
        if dq.get("known_at_pass") is True or dq.get("market_known_at_pass") is True:
            known_at += 1
        if isinstance(decision, dict) and decision.get("config_lineage"):
            config_lineage += 1
        if parse_json(row["label_json"], {}):
            label_any += 1
    total = len(rows)
    return {
        "entry_exit_pair_rate": round(paired / total, 8) if total else 0.0,
        "market_feature_complete_rate": round(market_complete / total, 8) if total else 0.0,
        "known_at_pass_rate": round(known_at / total, 8) if total else 0.0,
        "config_gate_lineage_rate": round(config_lineage / total, 8) if total else 0.0,
        "trade_quality_label_rate": round(label_any / total, 8) if total else 0.0,
        "trade_quality_module_complete_rate": round(
            sum(1 for item in results if item["trade_quality_module_complete"]) / sample_count, 8
        )
        if sample_count
        else 0.0,
        "label_policy_v2_contract_valid_rate": round(
            sum(1 for item in results if item["label_policy_v2_contract_valid"]) / sample_count, 8
        )
        if sample_count
        else 0.0,
        "label_policy_v2_pass_rate": round(sum(1 for item in results if item["label_policy_v2_pass"]) / sample_count, 8)
        if sample_count
        else 0.0,
    }


def write_report(summary: dict[str, Any]) -> Path:
    path = ROOT / "docs" / "reports" / f"STEP29.18_tq_module_completion_backfill_v2_{stamp()}.md"
    lines = [
        "# STEP29.18 Trade Quality Module Completion Backfill v2",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- sidecar_db: `{SIDECAR_DB.relative_to(ROOT).as_posix()}`",
        f"- sample_count: `{summary['sample_count']}`",
        f"- trade_quality_module_complete_rate: `{summary['trade_quality_module_complete_rate']}`",
        f"- label_policy_v2_contract_valid_rate: `{summary['label_policy_v2_contract_valid_rate']}`",
        f"- label_policy_v2_pass_rate: `{summary['label_policy_v2_pass_rate']}`",
        "",
        "## Coverage Status Counts",
        "",
    ]
    for key, value in sorted(summary["label_coverage_status_counts"].items()):
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Reason Counts", ""])
    for key, value in sorted(summary["reason_counts"].items()):
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Missing Examples", ""])
    if summary["missing_examples"]:
        lines.extend(["| sample_id | label_coverage_status | reasons |", "| --- | --- | --- |"])
        for item in summary["missing_examples"][:20]:
            lines.append(f"| `{item['sample_id']}` | `{item['label_coverage_status']}` | `{', '.join(item['reason_codes'][:6])}` |")
    else:
        lines.append("- No missing examples.")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This step writes only P29 training sidecar fields and report artifacts.",
            "- TQ facts are accepted only when they already carry official Trade Quality provider lineage.",
            "- Samples without official TQ lineage are marked review/excluded, not promoted to training-ready.",
            "- Cost-aware labels remain blocked until expected/realized cost fields are covered by later v2 gates.",
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
        results = [process_row(row) for row in rows]
        if not args.dry_run:
            for item in results:
                con.execute(
                    """
                    UPDATE trade_training_samples
                    SET post_trade_outcome_json=?, label_json=?, data_quality_json=?
                    WHERE sample_id=?
                    """,
                    (
                        canonical_json(item["post_trade_outcome_json"]),
                        canonical_json(item["label_json"]),
                        canonical_json(item["data_quality_json"]),
                        item["sample_id"],
                    ),
                )
                if item.get("exit_event_id"):
                    con.execute(
                        "UPDATE trade_snapshot_events SET trade_quality_json=?, data_quality_json=? WHERE event_id=?",
                        (
                            canonical_json(item["label_json"]),
                            canonical_json(item["data_quality_json"]),
                            item["exit_event_id"],
                        ),
                    )

        reason_counts: Counter[str] = Counter()
        for item in results:
            reason_counts.update(item["reason_codes"])
        missing_examples = [
            {
                "sample_id": item["sample_id"],
                "label_coverage_status": item["label_coverage_status"],
                "reason_codes": item["reason_codes"],
            }
            for item in results
            if not item["label_policy_v2_pass"]
        ][:200]
        summary = {
            "task_id": TASK_ID,
            "policy_version": TQ_COMPLETION_POLICY_VERSION,
            "generated_at": now_iso(),
            "dry_run": args.dry_run,
            "sample_count": len(results),
            "label_coverage_status_counts": dict(Counter(item["label_coverage_status"] for item in results)),
            "reason_counts": dict(reason_counts),
            "missing_examples": missing_examples,
            **compute_rates(con, results),
        }
        if not args.dry_run:
            refresh_audit_row(con, summary)
            con.commit()
        SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        report = write_report(summary)
        print(json.dumps({"status": "ok", "summary": str(SUMMARY_JSON), "report": str(report), **summary}, ensure_ascii=False))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
