"""Per-run archive helpers for P10 trade plan line documents."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now

LINE_TO_FILENAME = {
    "without_micro": "latest_trade_plan_without_micro.json",
    "micro_fast": "latest_trade_plan_micro_fast.json",
    "micro_full": "latest_trade_plan_micro_full.json",
    "strategy4": "latest_trade_plan_strategy4.json",
}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def trade_plan_source_plan_hash(line: str, doc: dict[str, Any], plan: dict[str, Any]) -> str:
    refs = plan.get("input_refs") if isinstance(plan.get("input_refs"), dict) else {}
    existing = refs.get("source_plan_hash")
    if existing:
        return str(existing)
    guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
    payload = {
        "line": line,
        "run_id": doc.get("run_id"),
        "cycle_id": doc.get("cycle_id"),
        "generated_at": doc.get("generated_at"),
        "symbol": plan.get("symbol"),
        "decision": plan.get("decision"),
        "action": plan.get("action"),
        "entry_mode": plan.get("entry_mode"),
        "entry": plan.get("estimated_entry_price") or guards.get("better_entry_price") or guards.get("trigger_price"),
        "sl": plan.get("stop_loss") or guards.get("structure_stop"),
        "tp": plan.get("take_profit") or guards.get("tp2") or guards.get("tp1"),
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def trade_plan_run_archive_dir(root: Path, run_id: str | None) -> Path:
    safe_run_id = str(run_id or "unknown_run").strip() or "unknown_run"
    return root / "DATA" / "decisions" / "trade_plan_runs" / safe_run_id


def archive_trade_plan_line_payload(
    *,
    root: Path,
    line: str,
    payload: dict[str, Any],
    latest_path: Path,
) -> dict[str, Any]:
    """Write a per-run immutable copy and return payload annotated with archive refs."""
    if line not in LINE_TO_FILENAME:
        raise ValueError(f"unsupported trade plan line: {line}")
    run_id = str(payload.get("run_id") or "unknown_run")
    archive_dir = trade_plan_run_archive_dir(root, run_id)
    archive_path = archive_dir / LINE_TO_FILENAME[line]
    manifest_path = archive_dir / "manifest.json"

    annotated = dict(payload)
    refs = dict(annotated.get("input_refs") if isinstance(annotated.get("input_refs"), dict) else {})
    source_hashes: list[str] = []
    plans: list[dict[str, Any]] = []
    for raw_plan in annotated.get("plans") or []:
        if not isinstance(raw_plan, dict):
            continue
        plan = dict(raw_plan)
        plan_refs = dict(plan.get("input_refs") if isinstance(plan.get("input_refs"), dict) else {})
        source_hash = trade_plan_source_plan_hash(line, annotated, plan)
        source_hashes.append(source_hash)
        plan_refs.update(
            {
                "source_plan_hash": source_hash,
                "trade_plan_archive_path": str(archive_path),
                "trade_plan_archive_manifest_path": str(manifest_path),
                "trade_plan_latest_path": str(latest_path),
            },
        )
        plan["input_refs"] = plan_refs
        plans.append(plan)
    annotated["plans"] = plans
    refs.update(
        {
            "trade_plan_archive_path": str(archive_path),
            "trade_plan_archive_manifest_path": str(manifest_path),
            "trade_plan_latest_path": str(latest_path),
            "trade_plan_source_plan_hashes": source_hashes,
        },
    )
    annotated["input_refs"] = refs

    archive_dir.mkdir(parents=True, exist_ok=True)
    write_json_atomic(archive_path, annotated)

    try:
        existing = read_json_object(manifest_path)
    except (OSError, ValueError, TypeError):
        existing = {}
    manifest = existing if isinstance(existing, dict) else {}
    lines = manifest.get("lines") if isinstance(manifest.get("lines"), dict) else {}
    lines[line] = {
        "strategy_line": line,
        "source": annotated.get("source"),
        "micro_mode": annotated.get("micro_mode"),
        "status": annotated.get("status"),
        "count": annotated.get("count"),
        "executable_count": annotated.get("executable_count"),
        "generated_at": annotated.get("generated_at"),
        "archive_path": str(archive_path),
        "latest_path": str(latest_path),
        "source_plan_hashes": source_hashes,
    }
    manifest.update(
        {
            "schema_version": "10.55",
            "source": "per_run_trade_plan_line_archive",
            "run_id": annotated.get("run_id"),
            "cycle_id": annotated.get("cycle_id"),
            "updated_at": to_iso_z(utc_now()),
            "archive_dir": str(archive_dir),
            "lines": lines,
        },
    )
    write_json_atomic(manifest_path, manifest)
    return annotated
