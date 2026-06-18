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
from laoma_signal_engine.training_readiness.label_policy_v2 import LABEL_POLICY_VERSION, validate_cost_aware_label_v2
from laoma_signal_engine.training_readiness.manifest_v2 import MANIFEST_SCHEMA_VERSION, gate_training_readiness_v2
from laoma_signal_engine.training_snapshot_sync import canonical_json, sidecar_db_path, stable_hash


TASK_ID = "STEP29.20"
SIDECAR_DB = sidecar_db_path(ROOT)
OUT_DIR = ROOT / "DATA" / "research" / "trade_snapshots"
STATUS_JSON = OUT_DIR / "step29_20_training_readiness_manifest_v2_status.json"
MANIFEST_JSON = OUT_DIR / "step29_20_training_readiness_manifest_v2.json"


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


def table_count(con: sqlite3.Connection, table: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def compute_status() -> tuple[dict[str, Any], dict[str, Any]]:
    con = sqlite3.connect(SIDECAR_DB)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute("SELECT * FROM trade_training_samples ORDER BY sample_id").fetchall()
        events = con.execute("SELECT event_id FROM trade_snapshot_events").fetchall()
        source_refs = con.execute("SELECT DISTINCT sample_id FROM trade_snapshot_source_refs WHERE sample_id IS NOT NULL").fetchall()
        sample_count = len(rows)
        sample_ids = [str(row["sample_id"]) for row in rows]
        event_ids = [str(row["event_id"]) for row in events]
        source_ref_sample_ids = {str(row["sample_id"]) for row in source_refs}

        counters: Counter[str] = Counter()
        source_modes: Counter[str] = Counter()
        label_statuses: Counter[str] = Counter()
        paired = market_complete = known_at = config_lineage = tq_complete = 0
        cost_complete = feature_pass = label_pass = 0
        leakage: list[dict[str, Any]] = []
        feature_blockers: Counter[str] = Counter()
        label_blockers: Counter[str] = Counter()

        for row in rows:
            sample_id = str(row["sample_id"])
            source_modes[str(row["source_mode"])] += 1
            dq = parse_json(row["data_quality_json"], {})
            decision = parse_json(row["decision_time_input_json"], {})
            label = parse_json(row["label_json"], {})
            outcome = parse_json(row["post_trade_outcome_json"], {})

            if row["entry_event_id"] and row["exit_event_id"]:
                paired += 1
            if dq.get("market_feature_completeness") == "complete" or dq.get("feature_completeness") == "complete":
                market_complete += 1
            if dq.get("known_at_pass") is True or dq.get("market_known_at_pass") is True:
                known_at += 1
            if isinstance(decision, dict) and decision.get("config_lineage"):
                config_lineage += 1
            if dq.get("trade_quality_module_complete") is True:
                tq_complete += 1

            feature = validate_decision_time_feature_schema_v2(decision, decision_time_ms=dq.get("decision_time_ms") or row["entry_time_ms"])
            if feature["decision_time_feature_schema_v2_pass"]:
                feature_pass += 1
            feature_blockers.update(feature["missing_fields"])
            feature_blockers.update(feature["missing_lineage_fields"])
            feature_blockers.update(item["reason"] for item in feature["known_at_violations"])

            label_result = validate_cost_aware_label_v2(label, post_trade_outcome_json=outcome, decision_time_input_json=decision)
            label_statuses[str(label_result.get("label_coverage_status") or "unknown")] += 1
            if label_result["cost_fields_complete"]:
                cost_complete += 1
            if label_result["label_policy_v2_pass"]:
                label_pass += 1
            label_blockers.update(label_result["reason_codes"])
            leakage.extend({"sample_id": sample_id, **item} for item in label_result["forbidden_decision_time_fields"])

        duplicate_samples = sample_count - len(set(sample_ids))
        duplicate_events = len(event_ids) - len(set(event_ids))
        samples_without_source_ref = len([sid for sid in sample_ids if sid not in source_ref_sample_ids])
        rates = {
            "entry_exit_pair_rate": paired / sample_count if sample_count else 0.0,
            "market_feature_complete_rate": market_complete / sample_count if sample_count else 0.0,
            "known_at_pass_rate": known_at / sample_count if sample_count else 0.0,
            "config_gate_lineage_rate": config_lineage / sample_count if sample_count else 0.0,
            "trade_quality_module_complete_rate": tq_complete / sample_count if sample_count else 0.0,
            "cost_fields_coverage": cost_complete / sample_count if sample_count else 0.0,
            "decision_time_feature_schema_v2_pass_rate": feature_pass / sample_count if sample_count else 0.0,
            "label_policy_v2_pass_rate": label_pass / sample_count if sample_count else 0.0,
        }
        status = {
            "task_id": TASK_ID,
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "label_policy_version": LABEL_POLICY_VERSION,
            "generated_at": now_iso(),
            "sidecar_db": SIDECAR_DB.relative_to(ROOT).as_posix(),
            "sample_count": sample_count,
            "event_count": table_count(con, "trade_snapshot_events"),
            "source_ref_count": table_count(con, "trade_snapshot_source_refs"),
            "source_mode_counts": dict(source_modes),
            "label_coverage_status_counts": dict(label_statuses),
            **{key: round(value, 8) for key, value in rates.items()},
            "post_trade_leakage_count": len(leakage),
            "duplicate_sample_ids": duplicate_samples,
            "duplicate_event_ids": duplicate_events,
            "samples_without_source_ref": samples_without_source_ref,
            "oos_used_for_training_or_hpo": False,
            "paper_shadow_used_for_training_or_hpo": False,
            "feature_blocker_counts": dict(feature_blockers.most_common(50)),
            "label_blocker_counts": dict(label_blockers.most_common(50)),
            "leakage_violations": leakage[:200],
            "split_manifest_hash": stable_hash(
                {
                    "oos_used_for_training_or_hpo": False,
                    "paper_shadow_used_for_training_or_hpo": False,
                    "source_modes": dict(source_modes),
                }
            ),
        }
        status.update(gate_training_readiness_v2(status))
        dataset_rows = [dict(row) for row in rows]
        manifest = {
            "manifest_id": f"step29_20:{stable_hash(status)[:24]}",
            "dataset_version": "step29_training_readiness_v2",
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "label_policy_version": LABEL_POLICY_VERSION,
            "schema_hash": stable_hash(
                {
                    "schema_version": MANIFEST_SCHEMA_VERSION,
                    "feature_schema_version": FEATURE_SCHEMA_VERSION,
                    "label_policy_version": LABEL_POLICY_VERSION,
                    "status_keys": sorted(status),
                }
            ),
            "dataset_hash": stable_hash(dataset_rows),
            "split_manifest_hash": status["split_manifest_hash"],
            "sidecar_db": status["sidecar_db"],
            "status_path": STATUS_JSON.relative_to(ROOT).as_posix(),
            "coverage_json": status,
            "allowed_for_training": status["allowed_for_training"],
            "generated_at": status["generated_at"],
        }
        status["dataset_hash"] = manifest["dataset_hash"]
        return status, manifest
    finally:
        con.close()


def persist_status(status: dict[str, Any], manifest: dict[str, Any]) -> None:
    STATUS_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    MANIFEST_JSON.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    con = sqlite3.connect(SIDECAR_DB)
    try:
        now = now_iso()
        con.execute(
            """
            INSERT OR REPLACE INTO trade_snapshot_manifests (
                manifest_id, run_id, source_mode, schema_version, schema_hash, source_refs_json,
                coverage_json, dataset_hash, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                manifest["manifest_id"],
                TASK_ID,
                "sidecar_training_readiness_v2",
                MANIFEST_SCHEMA_VERSION,
                manifest["schema_hash"],
                canonical_json([{"sidecar_db": status["sidecar_db"]}]),
                canonical_json(status),
                manifest["dataset_hash"],
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
                f"step29_20:audit:{stable_hash(status)[:24]}",
                manifest["manifest_id"],
                status["sample_count"],
                status["entry_exit_pair_rate"],
                status["market_feature_complete_rate"],
                status["trade_quality_module_complete_rate"],
                status["config_gate_lineage_rate"],
                status["known_at_pass_rate"],
                canonical_json(status["leakage_violations"]),
                canonical_json(status["blocking_reasons"]),
                now,
            ),
        )
        con.commit()
    finally:
        con.close()


def write_report(status: dict[str, Any], manifest: dict[str, Any]) -> Path:
    path = ROOT / "docs" / "reports" / f"STEP29.20_training_readiness_manifest_v2_{stamp()}.md"
    lines = [
        "# STEP29.20 Training Readiness Manifest v2 Gate",
        "",
        f"- generated_at: `{status['generated_at']}`",
        f"- status_json: `{STATUS_JSON.relative_to(ROOT).as_posix()}`",
        f"- manifest_json: `{MANIFEST_JSON.relative_to(ROOT).as_posix()}`",
        f"- dataset_status: `{status['dataset_status']}`",
        f"- allowed_for_training: `{status['allowed_for_training']}`",
        f"- sample_count: `{status['sample_count']}`",
        "",
        "## V2 Rates",
        "",
        f"- entry_exit_pair_rate: `{status['entry_exit_pair_rate']}`",
        f"- market_feature_complete_rate: `{status['market_feature_complete_rate']}`",
        f"- known_at_pass_rate: `{status['known_at_pass_rate']}`",
        f"- trade_quality_module_complete_rate: `{status['trade_quality_module_complete_rate']}`",
        f"- cost_fields_coverage: `{status['cost_fields_coverage']}`",
        f"- decision_time_feature_schema_v2_pass_rate: `{status['decision_time_feature_schema_v2_pass_rate']}`",
        f"- label_policy_v2_pass_rate: `{status['label_policy_v2_pass_rate']}`",
        f"- post_trade_leakage_count: `{status['post_trade_leakage_count']}`",
        "",
        "## Blocking Reasons",
        "",
    ]
    if status["blocking_reasons"]:
        lines.extend(f"- `{item}`" for item in status["blocking_reasons"])
    else:
        lines.append("- None.")
    lines.extend(["", "## Top Feature Blockers", ""])
    for key, value in list(status["feature_blocker_counts"].items())[:20]:
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Top Label Blockers", ""])
    for key, value in list(status["label_blocker_counts"].items())[:20]:
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- `allowed_for_training=true` is emitted only when all v2 gates are 1.0 and leakage is zero.",
            "- v1 manifest paths remain untouched for backward-compatible readers.",
            f"- dataset_hash: `{manifest['dataset_hash']}`",
            f"- split_manifest_hash: `{manifest['split_manifest_hash']}`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    status, manifest = compute_status()
    report = write_report(status, manifest)
    if not args.dry_run:
        persist_status(status, manifest)
    print(json.dumps({"status": "ok", "status_path": str(STATUS_JSON), "manifest_path": str(MANIFEST_JSON), "report": str(report), **status}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
