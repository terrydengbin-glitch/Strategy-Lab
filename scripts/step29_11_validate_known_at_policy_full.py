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

SIDECAR_DB = sidecar_db_path(ROOT)
SCHEMA_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_trade_snapshot_schema_contract.json"
OUTPUT_JSON = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_11_known_at_full_audit_summary.json"
POLICY_VERSION = "step29_known_at_full_audit_v1"
TASK_ID = "STEP29.11"

METADATA_KEYS = {
    "blocked_fields",
    "decision_time_ms",
    "event_action",
    "field_lineage_json",
    "field_role",
    "known_at_policy",
    "known_at_pass",
    "max_feature_known_at_ms",
    "missing_fields",
    "observed_fields",
    "proxy_fields",
    "schema_version",
    "symbol",
    "source_priority",
    "source_db_path",
    "source_table",
    "source_row_id",
    "status",
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


def leaf_key(path: str) -> str:
    last = path.split(".")[-1]
    if "[" in last:
        last = last.split("[", 1)[0]
    return last


def path_has_forbidden(path: str, post_trade_fields: set[str]) -> bool:
    parts = []
    for chunk in path.replace("[", ".").replace("]", "").split("."):
        if chunk:
            parts.append(chunk)
    return any(part in post_trade_fields for part in parts)


def string_json_hits(value: str, post_trade_fields: set[str]) -> list[str]:
    text = value.strip()
    if not text or text[0] not in "{[":
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        return []
    hits = []
    for path, _ in walk(parsed):
        if path_has_forbidden(path, post_trade_fields):
            hits.append(path)
    return hits


def lineage_known_at(snapshot: dict[str, Any], field: str, fallback: int | None) -> int | None:
    lineage = snapshot.get("field_lineage_json")
    if isinstance(lineage, dict):
        payload = lineage.get(field)
        if isinstance(payload, dict):
            for key in ("known_at_ms", "source_available_time_ms", "feature_timestamp_ms"):
                value = payload.get(key)
                if isinstance(value, (int, float)):
                    return int(value)
    value = snapshot.get("known_at_ms")
    if isinstance(value, (int, float)):
        return int(value)
    return fallback


def audit_market_snapshot(
    sample_id: str,
    location: str,
    snapshot: dict[str, Any],
    decision_time_ms: int | None,
) -> tuple[dict[str, int | None], list[dict[str, Any]], int | None]:
    timestamp_map: dict[str, int | None] = {}
    failures: list[dict[str, Any]] = []
    max_known_at: int | None = None
    for key, value in snapshot.items():
        if key in METADATA_KEYS or isinstance(value, (dict, list)):
            continue
        known_at = lineage_known_at(snapshot, key, snapshot.get("known_at_ms"))
        path = f"{location}.{key}"
        timestamp_map[path] = known_at
        if isinstance(known_at, int):
            max_known_at = known_at if max_known_at is None else max(max_known_at, known_at)
        if not isinstance(decision_time_ms, int):
            failures.append(
                {
                    "sample_id": sample_id,
                    "field_path": path,
                    "reason": "missing_decision_time_ms",
                    "known_at_ms": known_at,
                    "decision_time_ms": decision_time_ms,
                }
            )
        elif not isinstance(known_at, int):
            failures.append(
                {
                    "sample_id": sample_id,
                    "field_path": path,
                    "reason": "missing_known_at_ms",
                    "known_at_ms": known_at,
                    "decision_time_ms": decision_time_ms,
                }
            )
        elif known_at > decision_time_ms:
            failures.append(
                {
                    "sample_id": sample_id,
                    "field_path": path,
                    "reason": "known_at_after_decision_time",
                    "known_at_ms": known_at,
                    "decision_time_ms": decision_time_ms,
                }
            )
    return timestamp_map, failures, max_known_at


def audit_sample(row: sqlite3.Row, post_trade_fields: set[str]) -> dict[str, Any]:
    sample = dict(row)
    sample_id = str(sample["sample_id"])
    decision = parse_json(sample.get("decision_time_input_json"), {})
    data_quality = parse_json(sample.get("data_quality_json"), {})
    entry_decision_time_ms = sample.get("entry_time_ms")
    if not isinstance(entry_decision_time_ms, int):
        snap = decision.get("entry_market_snapshot") if isinstance(decision, dict) else {}
        if isinstance(snap, dict) and isinstance(snap.get("decision_time_ms"), int):
            entry_decision_time_ms = int(snap["decision_time_ms"])

    forbidden: list[dict[str, Any]] = []
    timestamp_map: dict[str, int | None] = {}
    failures: list[dict[str, Any]] = []
    max_known_at: int | None = None

    for path, value in walk(decision):
        key = leaf_key(path)
        if key in post_trade_fields or path_has_forbidden(path, post_trade_fields):
            forbidden.append(
                {
                    "sample_id": sample_id,
                    "field_path": path,
                    "reason": "post_trade_field_in_decision_input",
                }
            )
        if isinstance(value, str):
            for hit in string_json_hits(value, post_trade_fields):
                forbidden.append(
                    {
                        "sample_id": sample_id,
                        "field_path": f"{path}.{hit}",
                        "reason": "post_trade_field_in_string_json_decision_input",
                    }
                )

    entry_snapshot = decision.get("entry_market_snapshot") if isinstance(decision, dict) else {}
    if isinstance(entry_snapshot, dict):
        ts_map, snap_failures, snap_max = audit_market_snapshot(
            sample_id,
            "decision_time_input_json.entry_market_snapshot",
            entry_snapshot,
            entry_decision_time_ms,
        )
        timestamp_map.update(ts_map)
        failures.extend(snap_failures)
        if snap_max is not None:
            max_known_at = snap_max if max_known_at is None else max(max_known_at, snap_max)

    extended = decision.get("extended_market_context") if isinstance(decision, dict) else {}
    if isinstance(extended, dict):
        ts_map, ext_failures, ext_max = audit_market_snapshot(
            sample_id,
            "decision_time_input_json.extended_market_context",
            extended,
            entry_decision_time_ms,
        )
        timestamp_map.update(ts_map)
        failures.extend(ext_failures)
        if ext_max is not None:
            max_known_at = ext_max if max_known_at is None else max(max_known_at, ext_max)

    all_failures = failures + forbidden
    known_at_pass = not all_failures
    data_quality.update(
        {
            "known_at_policy_version": POLICY_VERSION,
            "known_at_pass": known_at_pass,
            "known_at_fail_fields_json": all_failures,
            "future_leakage_fields_json": all_failures,
            "feature_timestamp_map_json": timestamp_map,
            "decision_time_ms": entry_decision_time_ms,
            "max_feature_known_at_ms": max_known_at,
        }
    )
    return {
        "sample_id": sample_id,
        "known_at_pass": known_at_pass,
        "data_quality_json": data_quality,
        "failures": all_failures,
        "max_feature_known_at_ms": max_known_at,
        "decision_time_ms": entry_decision_time_ms,
    }


def refresh_audit_row(con: sqlite3.Connection, summary: dict[str, Any]) -> None:
    now = now_iso()
    manifest_id = f"{TASK_ID.lower()}:{stable_hash(summary)[:24]}"
    audit_id = f"{TASK_ID.lower()}:audit:{stable_hash({'at': now, 'summary': summary})[:24]}"
    coverage = {
        "sample_count": summary["sample_count"],
        "known_at_pass_rate": summary["known_at_pass_rate"],
        "leakage_violations": summary["leakage_violations"],
        "future_leakage_count": summary["future_leakage_count"],
        "policy_version": POLICY_VERSION,
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
            "sidecar_known_at_audit",
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
            canonical_json(summary["known_at_fail_examples"][:200]),
            now,
        ),
    )


def write_report(summary: dict[str, Any]) -> Path:
    path = ROOT / "docs" / "reports" / f"STEP29.11_known_at_full_audit_{stamp()}.md"
    lines = [
        "# STEP29.11 Known-At Full Audit",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- sidecar_db: `{SIDECAR_DB.relative_to(ROOT).as_posix()}`",
        f"- sample_count: `{summary['sample_count']}`",
        f"- known_at_pass_rate: `{summary['known_at_pass_rate']}`",
        f"- future_leakage_count: `{summary['future_leakage_count']}`",
        f"- leakage_violations: `{len(summary['leakage_violations'])}`",
        "",
        "## Coverage",
        "",
        f"- entry_exit_pair_rate: `{summary['entry_exit_pair_rate']}`",
        f"- market_feature_complete_rate: `{summary['market_feature_complete_rate']}`",
        f"- trade_quality_label_rate: `{summary['trade_quality_label_rate']}`",
        f"- config_gate_lineage_rate: `{summary['config_gate_lineage_rate']}`",
        "",
        "## Failure Examples",
        "",
    ]
    if summary["known_at_fail_examples"]:
        lines.extend(["| sample_id | field_path | reason | known_at_ms | decision_time_ms |", "| --- | --- | --- | ---: | ---: |"])
        for item in summary["known_at_fail_examples"][:20]:
            lines.append(
                f"| `{item.get('sample_id')}` | `{item.get('field_path')}` | `{item.get('reason')}` | `{item.get('known_at_ms')}` | `{item.get('decision_time_ms')}` |"
            )
    else:
        lines.append("- No known-at failures.")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Audited `decision_time_input_json` only.",
            "- Post-trade outcome / label fields remain forbidden in decision input.",
            "- Source paper/backtest/sandbox DBs were not modified.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    schema = parse_json(SCHEMA_PATH.read_text(encoding="utf-8"), {})
    post_trade_fields = set(schema.get("post_trade_only_fields") or [])

    con = sqlite3.connect(SIDECAR_DB)
    con.row_factory = sqlite3.Row
    try:
        query = "SELECT * FROM trade_training_samples ORDER BY sample_id"
        params: tuple[Any, ...] = ()
        if args.limit:
            query += " LIMIT ?"
            params = (args.limit,)
        rows = con.execute(query, params).fetchall()
        results = [audit_sample(row, post_trade_fields) for row in rows]
        if not args.dry_run:
            for result in results:
                con.execute(
                    "UPDATE trade_training_samples SET data_quality_json=? WHERE sample_id=?",
                    (canonical_json(result["data_quality_json"]), result["sample_id"]),
                )
            con.commit()

        sample_count = len(results)
        pass_count = sum(1 for item in results if item["known_at_pass"])
        failures = [failure for item in results for failure in item["failures"]]
        leakage = [item for item in failures if "post_trade" in str(item.get("reason", ""))]

        dq_rows = [parse_json(row["data_quality_json"], {}) for row in con.execute("SELECT data_quality_json FROM trade_training_samples").fetchall()]
        paired = con.execute(
            "SELECT COUNT(*) FROM trade_training_samples WHERE entry_event_id IS NOT NULL AND exit_event_id IS NOT NULL"
        ).fetchone()[0]
        market_complete = sum(1 for dq in dq_rows if dq.get("market_feature_completeness") == "complete" or dq.get("feature_completeness") == "complete")
        tq_labeled = sum(1 for row in con.execute("SELECT label_json FROM trade_training_samples").fetchall() if parse_json(row["label_json"], {}))
        config_lineage = 0
        for row in con.execute("SELECT decision_time_input_json FROM trade_training_samples").fetchall():
            decision = parse_json(row["decision_time_input_json"], {})
            if isinstance(decision, dict) and decision.get("config_lineage"):
                config_lineage += 1

        summary = {
            "task_id": TASK_ID,
            "policy_version": POLICY_VERSION,
            "generated_at": now_iso(),
            "dry_run": args.dry_run,
            "sample_count": sample_count,
            "known_at_pass_count": pass_count,
            "known_at_pass_rate": round(pass_count / sample_count, 8) if sample_count else 0.0,
            "future_leakage_count": len(failures),
            "leakage_violations": leakage[:200],
            "known_at_fail_examples": failures[:200],
            "entry_exit_pair_rate": round(paired / len(dq_rows), 8) if dq_rows else 0.0,
            "market_feature_complete_rate": round(market_complete / len(dq_rows), 8) if dq_rows else 0.0,
            "trade_quality_label_rate": round(tq_labeled / len(dq_rows), 8) if dq_rows else 0.0,
            "config_gate_lineage_rate": round(config_lineage / len(dq_rows), 8) if dq_rows else 0.0,
            "failure_reason_counts": dict(Counter(str(item.get("reason")) for item in failures)),
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
