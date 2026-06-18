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

from laoma_signal_engine.training_readiness.feature_schema_v2 import FEATURE_SCHEMA_VERSION, validate_decision_time_feature_schema_v2
from laoma_signal_engine.training_snapshot_sync import canonical_json, sidecar_db_path, stable_hash


TASK_ID = "STEP29.19"
SIDECAR_DB = sidecar_db_path(ROOT)
OUT_DIR = ROOT / "DATA" / "research" / "trade_snapshots"
SUMMARY_JSON = OUT_DIR / "step29_19_decision_time_feature_schema_v2_summary.json"


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
    decision = parse_json(row["decision_time_input_json"], {})
    dq = parse_json(row["data_quality_json"], {})
    result = validate_decision_time_feature_schema_v2(decision, decision_time_ms=dq.get("decision_time_ms") or row["entry_time_ms"])
    dq_missing = set(dq.get("missing_fields_json") or [])
    for field in result["missing_fields"]:
        dq_missing.add(f"decision_time_feature_schema_v2.{field}")
    for field in result["missing_lineage_fields"]:
        dq_missing.add(f"decision_time_feature_schema_v2.lineage.{field}")
    for item in result["known_at_violations"]:
        dq_missing.add(f"decision_time_feature_schema_v2.known_at.{item['field']}.{item['reason']}")
    for item in result["forbidden_decision_time_fields"]:
        dq_missing.add(f"decision_time_feature_schema_v2.forbidden.{item['field_path']}")
    dq.update(
        {
            "decision_time_feature_schema_version": FEATURE_SCHEMA_VERSION,
            "decision_time_feature_schema_v2_pass": result["decision_time_feature_schema_v2_pass"],
            "decision_time_feature_schema_v2_missing_fields_json": result["missing_fields"],
            "decision_time_feature_schema_v2_missing_lineage_json": result["missing_lineage_fields"],
            "decision_time_feature_schema_v2_known_at_violations_json": result["known_at_violations"],
            "decision_time_feature_schema_v2_forbidden_fields_json": result["forbidden_decision_time_fields"],
            "missing_fields_json": sorted(dq_missing),
        }
    )
    return {"sample_id": row["sample_id"], "data_quality_json": dq, **result}


def refresh_audit_row(con: sqlite3.Connection, summary: dict[str, Any]) -> None:
    now = now_iso()
    manifest_id = f"step29_19:{stable_hash(summary)[:24]}"
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
            "sidecar_decision_time_feature_schema_v2",
            FEATURE_SCHEMA_VERSION,
            stable_hash({"feature_schema_version": FEATURE_SCHEMA_VERSION}),
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
            f"step29_19:audit:{stable_hash(summary)[:24]}",
            manifest_id,
            summary["sample_count"],
            summary["entry_exit_pair_rate"],
            summary["market_feature_complete_rate"],
            summary["trade_quality_label_rate"],
            summary["config_gate_lineage_rate"],
            summary["known_at_pass_rate"],
            canonical_json(summary["forbidden_examples"][:200]),
            canonical_json(summary["missing_examples"][:200]),
            now,
        ),
    )


def compute_rates(con: sqlite3.Connection, results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(results)
    rows = con.execute("SELECT data_quality_json, decision_time_input_json, label_json FROM trade_training_samples").fetchall()
    paired = con.execute(
        "SELECT COUNT(*) FROM trade_training_samples WHERE entry_event_id IS NOT NULL AND exit_event_id IS NOT NULL"
    ).fetchone()[0]
    market_complete = 0
    known_at = 0
    config_lineage = 0
    label_any = 0
    tq_complete = 0
    for row in rows:
        dq = parse_json(row["data_quality_json"], {})
        decision = parse_json(row["decision_time_input_json"], {})
        label = parse_json(row["label_json"], {})
        if dq.get("market_feature_completeness") == "complete" or dq.get("feature_completeness") == "complete":
            market_complete += 1
        if dq.get("known_at_pass") is True or dq.get("market_known_at_pass") is True:
            known_at += 1
        if isinstance(decision, dict) and decision.get("config_lineage"):
            config_lineage += 1
        if label:
            label_any += 1
        if dq.get("trade_quality_module_complete") is True:
            tq_complete += 1
    return {
        "entry_exit_pair_rate": round(paired / len(rows), 8) if rows else 0.0,
        "market_feature_complete_rate": round(market_complete / len(rows), 8) if rows else 0.0,
        "known_at_pass_rate": round(known_at / len(rows), 8) if rows else 0.0,
        "config_gate_lineage_rate": round(config_lineage / len(rows), 8) if rows else 0.0,
        "trade_quality_label_rate": round(label_any / len(rows), 8) if rows else 0.0,
        "trade_quality_module_complete_rate": round(tq_complete / len(rows), 8) if rows else 0.0,
        "decision_time_feature_schema_v2_pass_rate": round(
            sum(1 for item in results if item["decision_time_feature_schema_v2_pass"]) / total, 8
        )
        if total
        else 0.0,
    }


def write_report(summary: dict[str, Any]) -> Path:
    path = ROOT / "docs" / "reports" / f"STEP29.19_decision_time_feature_schema_v2_{stamp()}.md"
    lines = [
        "# STEP29.19 Decision-Time Feature Schema v2 Completion",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- sidecar_db: `{SIDECAR_DB.relative_to(ROOT).as_posix()}`",
        f"- sample_count: `{summary['sample_count']}`",
        f"- decision_time_feature_schema_v2_pass_rate: `{summary['decision_time_feature_schema_v2_pass_rate']}`",
        f"- forbidden_decision_time_field_count: `{summary['forbidden_decision_time_field_count']}`",
        "",
        "## Missing Field Counts",
        "",
    ]
    for key, value in sorted(summary["missing_field_counts"].items()):
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Missing Lineage Counts", ""])
    for key, value in sorted(summary["missing_lineage_counts"].items()):
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Boundary", ""])
    lines.extend(
        [
            "- This step does not invent expected cost/liquidity/regime fields.",
            "- `realized_slippage_bps` and other post-trade labels are forbidden in decision-time input.",
            "- Samples that fail this v2 schema remain blocked for STEP29.20 training readiness.",
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
                    "UPDATE trade_training_samples SET data_quality_json=? WHERE sample_id=?",
                    (canonical_json(item["data_quality_json"]), item["sample_id"]),
                )

        missing_field_counts: Counter[str] = Counter()
        missing_lineage_counts: Counter[str] = Counter()
        for item in results:
            missing_field_counts.update(item["missing_fields"])
            missing_lineage_counts.update(item["missing_lineage_fields"])
        forbidden = [
            {"sample_id": item["sample_id"], **field}
            for item in results
            for field in item["forbidden_decision_time_fields"]
        ]
        missing_examples = [
            {
                "sample_id": item["sample_id"],
                "missing_fields": item["missing_fields"][:20],
                "missing_lineage_fields": item["missing_lineage_fields"][:20],
                "known_at_violations": item["known_at_violations"][:20],
            }
            for item in results
            if not item["decision_time_feature_schema_v2_pass"]
        ][:200]
        summary = {
            "task_id": TASK_ID,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "generated_at": now_iso(),
            "dry_run": args.dry_run,
            "sample_count": len(results),
            "missing_field_counts": dict(missing_field_counts),
            "missing_lineage_counts": dict(missing_lineage_counts),
            "known_at_violation_count": sum(len(item["known_at_violations"]) for item in results),
            "forbidden_decision_time_field_count": len(forbidden),
            "forbidden_examples": forbidden[:200],
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
