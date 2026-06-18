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

from laoma_signal_engine.training_snapshot_sync import (
    canonical_json,
    complete_scoped_known_at_reconstruction,
    sidecar_db_path,
    stable_hash,
)


TASK_ID = "STEP29.14"
SCHEMA_VERSION = "step29_extended_market_context_oi_funding_known_at_v1"
SIDECAR_DB = sidecar_db_path(ROOT)
SOURCE_DB = ROOT / "DATA" / "backtest" / "p21_parameter_optimization.db"
OUTPUT_JSON = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_14_extended_oi_funding_context_summary.json"
OI_DELAY_MS = 15 * 60 * 1000

EXTENDED_FIELDS = ("oi_change", "oi_state", "oi_z", "funding_rate", "funding_bucket", "funding_crowded_side")


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


def ensure_sidecar_columns(con: sqlite3.Connection) -> None:
    event_cols = {row["name"] for row in con.execute("PRAGMA table_info(trade_snapshot_events)").fetchall()}
    if "extended_market_context_json" not in event_cols:
        con.execute("ALTER TABLE trade_snapshot_events ADD COLUMN extended_market_context_json TEXT NOT NULL DEFAULT '{}'")


def lineage(field: str, source_table: str, source_row_id: str, feature_ts: int, known_at: int) -> dict[str, Any]:
    payload = {
        "field": field,
        "source_priority": "observed",
        "source_db_path": SOURCE_DB.relative_to(ROOT).as_posix(),
        "source_table": source_table,
        "source_row_id": source_row_id,
        "feature_timestamp_ms": feature_ts,
        "known_at_ms": known_at,
        "source_available_time_ms": known_at,
        "schema_version": SCHEMA_VERSION,
    }
    payload["lineage_id"] = stable_hash(payload)[:24]
    return payload


def event_decision_time_ms(event: dict[str, Any]) -> int | None:
    decision = event.get("decision_time_ms") or event.get("event_time_ms")
    event_time = event.get("event_time_ms")
    try:
        decision_int = int(decision) if decision is not None else None
        event_int = int(event_time) if event_time is not None else None
    except Exception:
        return None
    if str(event.get("event_action") or "").lower() == "entry" and decision_int and event_int and decision_int > event_int:
        return event_int
    return decision_int


def fetch_oi(con: sqlite3.Connection, symbol: str, decision_time_ms: int) -> dict[str, Any] | None:
    row = con.execute(
        """
        SELECT *
        FROM market_oi_15m
        WHERE symbol=? AND period='15m' AND source_time_ms + ? <= ?
        ORDER BY source_time_ms DESC
        LIMIT 1
        """,
        (symbol, OI_DELAY_MS, decision_time_ms),
    ).fetchone()
    return dict(row) if row else None


def fetch_funding(con: sqlite3.Connection, symbol: str, decision_time_ms: int) -> dict[str, Any] | None:
    row = con.execute(
        """
        SELECT *
        FROM market_funding_8h
        WHERE symbol=? AND funding_time_ms <= ?
        ORDER BY funding_time_ms DESC
        LIMIT 1
        """,
        (symbol, decision_time_ms),
    ).fetchone()
    return dict(row) if row else None


def build_context(source: sqlite3.Connection, event: dict[str, Any]) -> dict[str, Any]:
    symbol = event.get("symbol")
    decision_time = event_decision_time_ms(event)
    context: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": "missing",
        "event_action": event.get("event_action"),
        "symbol": symbol,
        "decision_time_ms": decision_time,
        "field_role": "decision_time_extended_context" if event.get("event_action") == "entry" else "audit_extended_context",
        "field_lineage_json": {},
        "missing_fields": [],
        "blocked_fields": [],
        "known_at_pass": True,
        "source_db_path": SOURCE_DB.relative_to(ROOT).as_posix(),
    }
    if not symbol or not isinstance(decision_time, int):
        context["missing_fields"] = list(EXTENDED_FIELDS)
        context["status"] = "missing_clock_or_symbol"
        context["known_at_pass"] = False
        return context

    oi = fetch_oi(source, str(symbol), int(decision_time))
    if oi:
        oi_ts = int(oi["source_time_ms"])
        oi_known_at = oi_ts + OI_DELAY_MS
        row_id = f"{symbol}:{oi_ts}:15m"
        for field, value in (
            ("oi_change", oi.get("oi_change")),
            ("oi_state", oi.get("oi_state")),
            ("oi_z", oi.get("oi_z")),
        ):
            if value is None:
                context["missing_fields"].append(field)
                continue
            if oi_known_at > decision_time:
                context["known_at_pass"] = False
                context["blocked_fields"].append(field)
                context["missing_fields"].append(field)
                continue
            context[field] = value
            context["field_lineage_json"][field] = lineage(field, "market_oi_15m", row_id, oi_ts, oi_known_at)
    else:
        context["missing_fields"].extend(["oi_change", "oi_state", "oi_z"])

    funding = fetch_funding(source, str(symbol), int(decision_time))
    if funding:
        ft = int(funding["funding_time_ms"])
        row_id = f"{symbol}:{ft}"
        for field, value in (
            ("funding_rate", funding.get("funding_rate")),
            ("funding_bucket", funding.get("funding_bucket")),
            ("funding_crowded_side", funding.get("funding_crowded_side")),
        ):
            if value is None:
                context["missing_fields"].append(field)
                continue
            if ft > decision_time:
                context["known_at_pass"] = False
                context["blocked_fields"].append(field)
                context["missing_fields"].append(field)
                continue
            context[field] = value
            context["field_lineage_json"][field] = lineage(field, "market_funding_8h", row_id, ft, ft)
    else:
        context["missing_fields"].extend(["funding_rate", "funding_bucket", "funding_crowded_side"])

    observed = [field for field in EXTENDED_FIELDS if field in context]
    context["missing_fields"] = sorted(set(context["missing_fields"]))
    context["blocked_fields"] = sorted(set(context["blocked_fields"]))
    context["observed_fields"] = sorted(observed)
    context["status"] = "complete" if len(observed) == len(EXTENDED_FIELDS) and context["known_at_pass"] else "partial" if observed else "missing"
    max_known_at = [
        item.get("known_at_ms")
        for item in context["field_lineage_json"].values()
        if isinstance(item, dict) and isinstance(item.get("known_at_ms"), int)
    ]
    context["max_feature_known_at_ms"] = max(max_known_at) if max_known_at else None
    return context


def update_sample_payload(sample: dict[str, Any], entry_context: dict[str, Any] | None, exit_context: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    decision = parse_json(sample.get("decision_time_input_json"), {})
    audit = parse_json(sample.get("audit_context_json"), {})
    dq = parse_json(sample.get("data_quality_json"), {})
    missing: set[str] = set(dq.get("extended_context_missing_fields_json") or [])
    blocked: set[str] = set(dq.get("extended_context_blocked_fields_json") or [])
    if entry_context:
        decision["extended_market_context"] = entry_context
        missing.update(f"extended_market_context.{field}" for field in entry_context.get("missing_fields") or [])
        blocked.update(f"extended_market_context.{field}" for field in entry_context.get("blocked_fields") or [])
    if exit_context:
        audit["extended_market_context"] = exit_context
    dq.update(
        {
            "extended_context_policy_version": SCHEMA_VERSION,
            "extended_context_status": (entry_context or {}).get("status", "missing"),
            "extended_context_known_at_pass": bool((entry_context or {}).get("known_at_pass") is True),
            "extended_context_missing_fields_json": sorted(missing),
            "extended_context_blocked_fields_json": sorted(blocked),
        }
    )
    return decision, audit, dq


def refresh_audit_row(con: sqlite3.Connection, summary: dict[str, Any]) -> None:
    now = now_iso()
    manifest_id = f"{TASK_ID.lower()}:{stable_hash(summary)[:24]}"
    audit_id = f"{TASK_ID.lower()}:audit:{stable_hash({'summary': summary, 'at': now})[:24]}"
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
            "sidecar_extended_market_context",
            SCHEMA_VERSION,
            stable_hash({"schema_version": SCHEMA_VERSION}),
            canonical_json([{"source_db_path": SOURCE_DB.relative_to(ROOT).as_posix(), "access_mode": "read_only"}]),
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
            audit_id,
            manifest_id,
            summary["sample_count"],
            summary["entry_exit_pair_rate"],
            summary["market_feature_complete_rate"],
            summary["trade_quality_label_rate"],
            summary["config_gate_lineage_rate"],
            summary["known_at_pass_rate"],
            canonical_json(summary["known_at_violations"][:200]),
            canonical_json(summary["missing_examples"][:200]),
            now,
        ),
    )


def write_report(summary: dict[str, Any]) -> Path:
    path = ROOT / "docs" / "reports" / f"STEP29.14_extended_oi_funding_context_{stamp()}.md"
    lines = [
        "# STEP29.14 Extended OI Funding Context Reconstruction",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- source_db: `{SOURCE_DB.relative_to(ROOT).as_posix()}`",
        f"- sidecar_db: `{SIDECAR_DB.relative_to(ROOT).as_posix()}`",
        f"- sample_count: `{summary['sample_count']}`",
        f"- event_count: `{summary['event_count']}`",
        f"- entry_extended_context_complete_rate: `{summary['entry_extended_context_complete_rate']}`",
        f"- entry_extended_context_observed_any_rate: `{summary['entry_extended_context_observed_any_rate']}`",
        f"- extended_context_known_at_pass_rate: `{summary['extended_context_known_at_pass_rate']}`",
        "",
        "## Field Coverage",
        "",
    ]
    for field, count in sorted(summary["field_observed_counts"].items()):
        lines.append(f"- `{field}`: {count}")
    lines.extend(["", "## Missing Examples", ""])
    if summary["missing_examples"]:
        lines.extend(["| sample_id | missing_fields |", "| --- | --- |"])
        for item in summary["missing_examples"][:20]:
            lines.append(f"| `{item['sample_id']}` | `{','.join(item['missing_fields'])}` |")
    else:
        lines.append("- No missing examples.")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- OI/funding are extended context and do not change STEP29.10 pure K-line completeness.",
            "- No gate/config funding bucket was used as a substitute for market source rows.",
            "- Source DB was read-only.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--sandbox-id", default=None)
    parser.add_argument("--source-mode", default=None)
    parser.add_argument("--max-source-lag-ms", type=int, default=60_000)
    args = parser.parse_args()

    if args.run_id or args.sandbox_id or args.source_mode:
        scoped = complete_scoped_known_at_reconstruction(
            ROOT,
            run_id=args.run_id,
            sandbox_id=args.sandbox_id,
            source_mode=args.source_mode,
            limit=args.limit,
            dry_run=bool(args.dry_run),
            max_source_lag_ms=int(args.max_source_lag_ms),
            include_market=False,
            include_extended=True,
        )
        summary = {
            **scoped,
            "generated_at": now_iso(),
            "sample_count": scoped.get("samples_processed", 0),
            "event_count": scoped.get("events_processed", 0),
            "entry_extended_context_complete_rate": 0.0,
            "entry_extended_context_observed_any_rate": scoped.get("extended_context_observed_any_rate", 0.0),
            "extended_context_known_at_pass_rate": scoped.get("extended_context_known_at_pass_rate", 0.0),
            "field_observed_counts": {},
            "known_at_violations": [],
            "missing_examples": [],
        }
        OUTPUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        report = write_report(summary)
        print(json.dumps({"status": "ok", "summary": str(OUTPUT_JSON), "report": str(report), **summary}, ensure_ascii=False))
        return 0

    sidecar = sqlite3.connect(SIDECAR_DB)
    sidecar.row_factory = sqlite3.Row
    source = sqlite3.connect(f"file:{SOURCE_DB.resolve().as_posix()}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    try:
        ensure_sidecar_columns(sidecar)
        query = "SELECT * FROM trade_snapshot_events ORDER BY sample_id, event_action"
        params: tuple[Any, ...] = ()
        if args.limit:
            query += " LIMIT ?"
            params = (args.limit,)
        events = [dict(row) for row in sidecar.execute(query, params).fetchall()]
        contexts: dict[str, dict[str, Any]] = {}
        for event in events:
            contexts[event["event_id"]] = build_context(source, event)
        samples = [dict(row) for row in sidecar.execute("SELECT * FROM trade_training_samples ORDER BY sample_id").fetchall()]
        if args.limit:
            event_sample_ids = {event["sample_id"] for event in events}
            samples = [sample for sample in samples if sample["sample_id"] in event_sample_ids]

        field_observed: Counter[str] = Counter()
        missing_examples: list[dict[str, Any]] = []
        known_at_violations: list[dict[str, Any]] = []
        entry_complete = 0
        entry_any = 0
        entry_known_at = 0
        for sample in samples:
            entry_context = contexts.get(sample.get("entry_event_id"))
            exit_context = contexts.get(sample.get("exit_event_id"))
            if entry_context:
                for field in entry_context.get("observed_fields") or []:
                    field_observed[field] += 1
                if entry_context.get("status") == "complete":
                    entry_complete += 1
                if entry_context.get("observed_fields"):
                    entry_any += 1
                if entry_context.get("known_at_pass") is True:
                    entry_known_at += 1
                if entry_context.get("missing_fields"):
                    missing_examples.append({"sample_id": sample["sample_id"], "missing_fields": entry_context["missing_fields"]})
                for field in entry_context.get("blocked_fields") or []:
                    known_at_violations.append({"sample_id": sample["sample_id"], "field": field, "reason": "known_at_failed"})
            if not args.dry_run:
                decision, audit, dq = update_sample_payload(sample, entry_context, exit_context)
                sidecar.execute(
                    "UPDATE trade_training_samples SET decision_time_input_json=?, audit_context_json=?, data_quality_json=? WHERE sample_id=?",
                    (canonical_json(decision), canonical_json(audit), canonical_json(dq), sample["sample_id"]),
                )
        if not args.dry_run:
            for event_id, context in contexts.items():
                sidecar.execute(
                    "UPDATE trade_snapshot_events SET extended_market_context_json=? WHERE event_id=?",
                    (canonical_json(context), event_id),
                )

        total_samples = len(samples)
        all_dq = [parse_json(row["data_quality_json"], {}) for row in sidecar.execute("SELECT data_quality_json FROM trade_training_samples").fetchall()]
        paired = sidecar.execute("SELECT COUNT(*) FROM trade_training_samples WHERE entry_event_id IS NOT NULL AND exit_event_id IS NOT NULL").fetchone()[0]
        market_complete = sum(1 for dq in all_dq if dq.get("market_feature_completeness") == "complete" or dq.get("feature_completeness") == "complete")
        known_at = sum(1 for dq in all_dq if dq.get("known_at_pass") is True or dq.get("market_known_at_pass") is True)
        label = sum(1 for row in sidecar.execute("SELECT label_json FROM trade_training_samples").fetchall() if parse_json(row["label_json"], {}))
        config_lineage = 0
        for row in sidecar.execute("SELECT decision_time_input_json FROM trade_training_samples").fetchall():
            decision = parse_json(row["decision_time_input_json"], {})
            if isinstance(decision, dict) and decision.get("config_lineage"):
                config_lineage += 1
        summary = {
            "task_id": TASK_ID,
            "schema_version": SCHEMA_VERSION,
            "generated_at": now_iso(),
            "dry_run": args.dry_run,
            "sample_count": total_samples,
            "event_count": len(events),
            "entry_extended_context_complete_rate": round(entry_complete / total_samples, 8) if total_samples else 0.0,
            "entry_extended_context_observed_any_rate": round(entry_any / total_samples, 8) if total_samples else 0.0,
            "extended_context_known_at_pass_rate": round(entry_known_at / total_samples, 8) if total_samples else 0.0,
            "field_observed_counts": dict(field_observed),
            "known_at_violations": known_at_violations[:200],
            "missing_examples": missing_examples[:200],
            "entry_exit_pair_rate": round(paired / len(all_dq), 8) if all_dq else 0.0,
            "market_feature_complete_rate": round(market_complete / len(all_dq), 8) if all_dq else 0.0,
            "trade_quality_label_rate": round(label / len(all_dq), 8) if all_dq else 0.0,
            "config_gate_lineage_rate": round(config_lineage / len(all_dq), 8) if all_dq else 0.0,
            "known_at_pass_rate": round(known_at / len(all_dq), 8) if all_dq else 0.0,
        }
        if not args.dry_run:
            refresh_audit_row(sidecar, summary)
            sidecar.commit()
        OUTPUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        report = write_report(summary)
        print(json.dumps({"status": "ok", "summary": str(OUTPUT_JSON), "report": str(report), **summary}, ensure_ascii=False))
    finally:
        source.close()
        sidecar.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
