"""STEP31.9 sandbox-scoped writer target contract.

This module does not run trading daemons. It provides the guard layer that
daemon/pipeline writers must pass before mutating sandbox-scoped outputs.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.atomic_writer import write_file_atomic

SCHEMA_VERSION = "STEP31.9_sandbox_writer_context_v1"
SANDBOX_ROOT_ENV = "LAOMA_SANDBOX_ROOT"
UI_ACTIVE_SANDBOX_LANE = "ui_active_sandbox_real_pipeline"
EXTERNAL_CLI_RESEARCH_LANE = "external_cli_research_lane"

ALLOWED_RESOURCE_LANES = {UI_ACTIVE_SANDBOX_LANE, EXTERNAL_CLI_RESEARCH_LANE}
SANDBOX_SCOPED_TARGETS = {
    "sandbox_runtime",
    "sandbox_db",
    "p29_training_sidecar",
    "observer_mirror",
}
MAIN_CHAIN_TARGETS = {
    "main_business_db",
    "main_paper_ledger",
    "main_runtime_current_json",
    "main_config",
    "production_strategy_source",
}


class SandboxWriterContextError(ValueError):
    """Raised when a writer context would violate sandbox isolation."""


@dataclass(frozen=True)
class SandboxWriterContext:
    sandbox_id: str
    resource_lane: str
    run_id: str
    cycle_id: str
    source_chain: str
    writer_target: str = "sandbox_db"
    strategy_line: str | None = None
    strategy_id: str | None = None
    symbol: str | None = None
    event_time_ms: int | None = None
    known_at_ms: int | None = None
    training_dataset_id: str | None = None
    trade_quality_status: str = "pending_post_close_trade_quality_module"
    equivalence_status: str = "sandbox_scoped_writer_guard_ready"
    main_chain_mutation_allowed: bool = False

    def validate(self) -> None:
        missing = [
            name
            for name, value in (
                ("sandbox_id", self.sandbox_id),
                ("resource_lane", self.resource_lane),
                ("run_id", self.run_id),
                ("cycle_id", self.cycle_id),
                ("source_chain", self.source_chain),
                ("writer_target", self.writer_target),
            )
            if value in (None, "")
        ]
        if missing:
            raise SandboxWriterContextError(f"missing_required_writer_context_fields:{','.join(missing)}")
        if self.resource_lane not in ALLOWED_RESOURCE_LANES:
            raise SandboxWriterContextError(f"unsupported_resource_lane:{self.resource_lane}")
        if self.writer_target in MAIN_CHAIN_TARGETS and not self.main_chain_mutation_allowed:
            raise SandboxWriterContextError(f"main_chain_writer_target_denied:{self.writer_target}")
        if self.writer_target not in SANDBOX_SCOPED_TARGETS and self.writer_target not in MAIN_CHAIN_TARGETS:
            raise SandboxWriterContextError(f"unsupported_writer_target:{self.writer_target}")

    def to_payload(self) -> dict[str, Any]:
        self.validate()
        payload = asdict(self)
        payload["schema_version"] = SCHEMA_VERSION
        payload["main_chain_mutation_allowed"] = bool(self.main_chain_mutation_allowed)
        payload["reason_codes"] = writer_reason_codes(payload)
        return payload


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def safe_part(value: str) -> str:
    got = re.sub(r"[^A-Za-z0-9_.:-]+", "_", str(value or "")).strip("_")
    return got or "unknown"


def project_rel(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def sandbox_root(project_root: Path) -> Path:
    configured = os.environ.get(SANDBOX_ROOT_ENV)
    if configured:
        return Path(configured)
    return project_root / "DATA" / "sandboxes"


def registry_db_path(project_root: Path) -> Path:
    return sandbox_root(project_root) / "sandbox_registry.db"


def fallback_sandbox_db_path(project_root: Path, sandbox_id: str) -> Path:
    return sandbox_root(project_root) / safe_part(sandbox_id) / "sandbox.db"


def resolve_sandbox_db_path(project_root: Path, sandbox_id: str) -> tuple[Path, str]:
    registry = registry_db_path(project_root)
    if registry.exists():
        try:
            with sqlite3.connect(registry) as conn:
                row = conn.execute("SELECT db_path FROM sandbox_registry WHERE sandbox_id=?", (sandbox_id,)).fetchone()
            if row and row[0]:
                return Path(row[0]), "sandbox_registry"
        except sqlite3.Error:
            pass
    return fallback_sandbox_db_path(project_root, sandbox_id), "fallback_sandbox_root"


def sandbox_run_dir(project_root: Path, sandbox_id: str, run_id: str) -> Path:
    return sandbox_root(project_root) / safe_part(sandbox_id) / "runtime" / "pipeline_runs" / safe_part(run_id)


def p29_sidecar_path(project_root: Path) -> Path:
    return project_root / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db"


def writer_reason_codes(payload: dict[str, Any]) -> list[str]:
    reasons = ["sandbox_scoped_writer_context_required", "main_chain_mutation_denied"]
    if payload.get("trade_quality_status") != "complete":
        reasons.append("trade_quality_completion_required")
    if not payload.get("training_dataset_id"):
        reasons.append("p29_sidecar_materialization_required")
    return sorted(set(reasons))


def ensure_writer_tables(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sandbox_pipeline_writer_contexts(
              context_id TEXT PRIMARY KEY,
              sandbox_id TEXT NOT NULL,
              resource_lane TEXT NOT NULL,
              run_id TEXT NOT NULL,
              cycle_id TEXT NOT NULL,
              source_chain TEXT NOT NULL,
              writer_target TEXT NOT NULL,
              target_paths_json TEXT NOT NULL,
              context_json TEXT NOT NULL,
              main_chain_mutation_allowed INTEGER NOT NULL DEFAULT 0,
              trade_quality_status TEXT,
              training_dataset_id TEXT,
              equivalence_status TEXT,
              reason_codes_json TEXT NOT NULL DEFAULT '[]',
              created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sandbox_pipeline_writer_contexts_run ON sandbox_pipeline_writer_contexts(sandbox_id, run_id, resource_lane)"
        )
        conn.commit()


def build_writer_targets(
    project_root: Path,
    *,
    sandbox_id: str,
    run_id: str,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    sandbox_db, db_source = resolve_sandbox_db_path(root, sandbox_id)
    run_dir = sandbox_run_dir(root, sandbox_id, run_id)
    return {
        "sandbox_db_path": project_rel(root, sandbox_db),
        "sandbox_db_path_source": db_source,
        "sandbox_runtime_dir": project_rel(root, run_dir),
        "sandbox_run_context_path": project_rel(root, run_dir / "writer_context.json"),
        "p29_training_sidecar_db_path": project_rel(root, p29_sidecar_path(root)),
        "observer_mirror": "DATA/runtime/sandbox_resource_governor/runs.jsonl",
        "main_chain_mutation_allowed": False,
        "denied_main_targets": sorted(MAIN_CHAIN_TARGETS),
    }


def create_writer_context(
    project_root: Path | None = None,
    *,
    sandbox_id: str,
    resource_lane: str,
    run_id: str,
    cycle_id: str,
    source_chain: str = "active_sandbox_real_pipeline",
    writer_target: str = "sandbox_db",
    strategy_line: str | None = None,
    strategy_id: str | None = None,
    symbol: str | None = None,
    event_time_ms: int | None = None,
    known_at_ms: int | None = None,
    training_dataset_id: str | None = None,
    trade_quality_status: str = "pending_post_close_trade_quality_module",
    equivalence_status: str = "sandbox_scoped_writer_guard_ready",
) -> dict[str, Any]:
    root = Path(project_root or Path.cwd()).resolve()
    ctx = SandboxWriterContext(
        sandbox_id=str(sandbox_id),
        resource_lane=str(resource_lane),
        run_id=str(run_id),
        cycle_id=str(cycle_id),
        source_chain=str(source_chain),
        writer_target=str(writer_target),
        strategy_line=strategy_line,
        strategy_id=strategy_id,
        symbol=symbol,
        event_time_ms=event_time_ms,
        known_at_ms=known_at_ms,
        training_dataset_id=training_dataset_id,
        trade_quality_status=trade_quality_status,
        equivalence_status=equivalence_status,
        main_chain_mutation_allowed=False,
    )
    context_payload = ctx.to_payload()
    targets = build_writer_targets(root, sandbox_id=sandbox_id, run_id=run_id)
    sandbox_db = root / targets["sandbox_db_path"]
    run_context_path = root / targets["sandbox_run_context_path"]
    run_context_path.parent.mkdir(parents=True, exist_ok=True)
    context_id = f"sbwctx_{safe_part(sandbox_id)}_{safe_part(run_id)}_{safe_part(resource_lane)}"
    record = {
        "schema_version": SCHEMA_VERSION,
        "context_id": context_id,
        "context": context_payload,
        "writer_targets": targets,
        "created_at": utc_now_iso(),
    }
    write_file_atomic(
        run_context_path,
        json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8") + b"\n",
    )
    ensure_writer_tables(sandbox_db)
    with sqlite3.connect(sandbox_db, timeout=30) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO sandbox_pipeline_writer_contexts(
              context_id, sandbox_id, resource_lane, run_id, cycle_id, source_chain,
              writer_target, target_paths_json, context_json, main_chain_mutation_allowed,
              trade_quality_status, training_dataset_id, equivalence_status,
              reason_codes_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                context_id,
                sandbox_id,
                resource_lane,
                run_id,
                cycle_id,
                source_chain,
                writer_target,
                json.dumps(targets, ensure_ascii=False, sort_keys=True),
                json.dumps(context_payload, ensure_ascii=False, sort_keys=True),
                0,
                context_payload.get("trade_quality_status"),
                context_payload.get("training_dataset_id"),
                context_payload.get("equivalence_status"),
                json.dumps(context_payload.get("reason_codes") or [], ensure_ascii=False, sort_keys=True),
                record["created_at"],
            ),
        )
        conn.commit()
    return record


def validate_writer_context_payload(payload: dict[str, Any]) -> dict[str, Any]:
    ctx = SandboxWriterContext(
        sandbox_id=str(payload.get("sandbox_id") or ""),
        resource_lane=str(payload.get("resource_lane") or ""),
        run_id=str(payload.get("run_id") or ""),
        cycle_id=str(payload.get("cycle_id") or ""),
        source_chain=str(payload.get("source_chain") or ""),
        writer_target=str(payload.get("writer_target") or "sandbox_db"),
        strategy_line=payload.get("strategy_line"),
        strategy_id=payload.get("strategy_id"),
        symbol=payload.get("symbol"),
        event_time_ms=payload.get("event_time_ms"),
        known_at_ms=payload.get("known_at_ms"),
        training_dataset_id=payload.get("training_dataset_id"),
        trade_quality_status=str(payload.get("trade_quality_status") or "pending_post_close_trade_quality_module"),
        equivalence_status=str(payload.get("equivalence_status") or "sandbox_scoped_writer_guard_ready"),
        main_chain_mutation_allowed=bool(payload.get("main_chain_mutation_allowed")),
    )
    return ctx.to_payload()
