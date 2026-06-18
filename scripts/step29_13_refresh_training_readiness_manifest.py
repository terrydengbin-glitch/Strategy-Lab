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

from laoma_signal_engine.training_snapshot_sync import canonical_json, sidecar_db_path, stable_hash


TASK_ID = "STEP29.13"
SCHEMA_VERSION = "step29_training_readiness_manifest_v1"
SIDECAR_DB = sidecar_db_path(ROOT)
OUT_DIR = ROOT / "DATA" / "research" / "trade_snapshots"
STATUS_JSON = OUT_DIR / "step29_13_training_readiness_status.json"
MANIFEST_JSON = OUT_DIR / "step29_13_training_readiness_manifest.json"

POST_TRADE_KEYS = {
    "MFE_R",
    "MAE_R",
    "net_R",
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


def walk(value: Any, prefix: str = "") -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            out.append((path, item))
            out.extend(walk(item, path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            path = f"{prefix}[{idx}]"
            out.append((path, item))
            out.extend(walk(item, path))
    return out


def decision_leaks(decision: Any, sample_id: str) -> list[dict[str, str]]:
    leaks = []
    for path, _ in walk(decision):
        leaf = path.split(".")[-1].split("[")[0]
        if leaf in POST_TRADE_KEYS:
            leaks.append({"sample_id": sample_id, "field_path": path, "reason": "post_trade_field_in_decision_input"})
    return leaks


def table_count(con: sqlite3.Connection, table: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def compute_status() -> tuple[dict[str, Any], dict[str, Any]]:
    con = sqlite3.connect(SIDECAR_DB)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute("SELECT * FROM trade_training_samples ORDER BY sample_id").fetchall()
        events = table_count(con, "trade_snapshot_events")
        source_refs = table_count(con, "trade_snapshot_source_refs")
        sample_count = len(rows)
        sample_ids = [str(row["sample_id"]) for row in rows]
        event_ids = [str(row["event_id"]) for row in con.execute("SELECT event_id FROM trade_snapshot_events").fetchall()]
        source_ref_sample_ids = {
            str(row["sample_id"])
            for row in con.execute("SELECT DISTINCT sample_id FROM trade_snapshot_source_refs WHERE sample_id IS NOT NULL").fetchall()
        }
        paired = 0
        market_complete = 0
        known_at = 0
        label_any = 0
        tq_complete = 0
        config_lineage = 0
        leakage: list[dict[str, str]] = []
        status_counts: Counter[str] = Counter()
        source_modes: Counter[str] = Counter()
        for row in rows:
            dq = parse_json(row["data_quality_json"], {})
            decision = parse_json(row["decision_time_input_json"], {})
            label = parse_json(row["label_json"], {})
            source_modes[str(row["source_mode"])] += 1
            status_counts[str(dq.get("trade_quality_status") or "unknown")] += 1
            if row["entry_event_id"] and row["exit_event_id"]:
                paired += 1
            if dq.get("market_feature_completeness") == "complete" or dq.get("feature_completeness") == "complete":
                market_complete += 1
            if dq.get("known_at_pass") is True or dq.get("market_known_at_pass") is True:
                known_at += 1
            if label:
                label_any += 1
            if dq.get("trade_quality_training_label_ready") is True and label.get("training_label_ready") is True:
                tq_complete += 1
            if isinstance(decision, dict) and decision.get("config_lineage"):
                config_lineage += 1
            leakage.extend(decision_leaks(decision, str(row["sample_id"])))

        duplicate_samples = sample_count - len(set(sample_ids))
        duplicate_events = events - len(set(event_ids))
        samples_without_source_ref = len([sid for sid in sample_ids if sid not in source_ref_sample_ids])
        rates = {
            "entry_exit_pair_rate": paired / sample_count if sample_count else 0.0,
            "market_feature_complete_rate": market_complete / sample_count if sample_count else 0.0,
            "trade_quality_label_rate": label_any / sample_count if sample_count else 0.0,
            "trade_quality_module_complete_rate": tq_complete / sample_count if sample_count else 0.0,
            "known_at_pass_rate": known_at / sample_count if sample_count else 0.0,
            "config_gate_lineage_rate": config_lineage / sample_count if sample_count else 0.0,
        }
        checks = {
            "entry_exit_pair_rate": rates["entry_exit_pair_rate"] >= 1.0,
            "market_feature_complete_rate": rates["market_feature_complete_rate"] >= 1.0,
            "trade_quality_label_rate": rates["trade_quality_label_rate"] >= 1.0,
            "trade_quality_module_complete_rate": rates["trade_quality_module_complete_rate"] >= 1.0,
            "known_at_pass_rate": rates["known_at_pass_rate"] >= 1.0,
            "leakage_violation_count": len(leakage) == 0,
            "duplicate_sample_ids": duplicate_samples == 0,
            "duplicate_event_ids": duplicate_events == 0,
            "samples_without_source_ref": samples_without_source_ref == 0,
        }
        blocking = [key for key, ok in checks.items() if not ok]
        allowed = sample_count > 0 and not blocking
        status = {
            "task_id": TASK_ID,
            "schema_version": SCHEMA_VERSION,
            "generated_at": now_iso(),
            "sidecar_db": SIDECAR_DB.relative_to(ROOT).as_posix(),
            "sample_count": sample_count,
            "event_count": events,
            "source_ref_count": source_refs,
            "source_mode_counts": dict(source_modes),
            "trade_quality_status_counts": dict(status_counts),
            **{key: round(value, 8) for key, value in rates.items()},
            "leakage_violation_count": len(leakage),
            "duplicate_sample_ids": duplicate_samples,
            "duplicate_event_ids": duplicate_events,
            "samples_without_source_ref": samples_without_source_ref,
            "checks": checks,
            "blocking_reasons": blocking,
            "dataset_status": "training_ready" if allowed else "needs_review",
            "allowed_for_training": allowed,
            "allowed_for_llm_training": allowed,
            "leakage_violations": leakage[:200],
            "notes": [
                "trade_quality_label_rate counts explicit labels/review statuses.",
                "trade_quality_module_complete_rate counts only labels produced by the Trade Quality module and ready for training.",
            ],
        }
        manifest = {
            "manifest_id": f"step29_13:{stable_hash(status)[:24]}",
            "dataset_version": "step29_training_readiness_v1",
            "schema_version": SCHEMA_VERSION,
            "schema_hash": stable_hash({"schema_version": SCHEMA_VERSION, "status_keys": sorted(status)}),
            "dataset_hash": stable_hash([dict(row) for row in rows]),
            "sidecar_db": SIDECAR_DB.relative_to(ROOT).as_posix(),
            "status_path": STATUS_JSON.relative_to(ROOT).as_posix(),
            "coverage_json": status,
            "allowed_for_training": allowed,
            "generated_at": status["generated_at"],
        }
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
                "sidecar_training_readiness",
                SCHEMA_VERSION,
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
                f"step29_13:audit:{stable_hash(status)[:24]}",
                manifest["manifest_id"],
                status["sample_count"],
                status["entry_exit_pair_rate"],
                status["market_feature_complete_rate"],
                status["trade_quality_label_rate"],
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
    path = ROOT / "docs" / "reports" / f"STEP29.13_training_readiness_status_{stamp()}.md"
    lines = [
        "# STEP29.13 Training Readiness Status Gate",
        "",
        f"- generated_at: `{status['generated_at']}`",
        f"- status_json: `{STATUS_JSON.relative_to(ROOT).as_posix()}`",
        f"- manifest_json: `{MANIFEST_JSON.relative_to(ROOT).as_posix()}`",
        f"- sample_count: `{status['sample_count']}`",
        f"- dataset_status: `{status['dataset_status']}`",
        f"- allowed_for_training: `{status['allowed_for_training']}`",
        "",
        "## Rates",
        "",
        f"- entry_exit_pair_rate: `{status['entry_exit_pair_rate']}`",
        f"- market_feature_complete_rate: `{status['market_feature_complete_rate']}`",
        f"- known_at_pass_rate: `{status['known_at_pass_rate']}`",
        f"- trade_quality_label_rate: `{status['trade_quality_label_rate']}`",
        f"- trade_quality_module_complete_rate: `{status['trade_quality_module_complete_rate']}`",
        "",
        "## Blocking Reasons",
        "",
    ]
    if status["blocking_reasons"]:
        lines.extend(f"- `{item}`" for item in status["blocking_reasons"])
    else:
        lines.append("- None.")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This gate is read-only against source DBs.",
            "- `allowed_for_training=true` requires Trade Quality module-complete labels, not just human-review placeholders.",
            f"- dataset_hash: `{manifest['dataset_hash']}`",
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
