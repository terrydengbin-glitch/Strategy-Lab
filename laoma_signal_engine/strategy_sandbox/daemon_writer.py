"""STEP33 daemon writer inventory and sandbox target adapters."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.paper.models import PaperConfig
from laoma_signal_engine.strategy_sandbox.paper_pipeline import sandbox_paper_config
from laoma_signal_engine.strategy_sandbox.writer_context import (
    SandboxWriterContextError,
    project_rel,
    safe_part,
    sandbox_run_dir,
    validate_writer_context_payload,
)

SCHEMA_VERSION = "STEP33_daemon_writer_migration_v1"
INVENTORY_PATH = Path("DATA") / "runtime" / "step33_daemon_writer_inventory.json"
SHARED_GOVERNANCE_COMPONENTS = {"rest_circuit", "rest_budget"}


@dataclass(frozen=True)
class DaemonWriterTarget:
    component: str
    logical_name: str
    mode: str
    path: Path
    writer_context_id: str | None
    sandbox_id: str | None
    run_id: str | None
    cycle_id: str | None
    resource_lane: str | None
    main_chain_mutation_allowed: bool
    reason_codes: tuple[str, ...]

    def to_payload(self, project_root: Path) -> dict[str, Any]:
        root = Path(project_root).resolve()
        return {
            "schema_version": SCHEMA_VERSION,
            "component": self.component,
            "logical_name": self.logical_name,
            "mode": self.mode,
            "path": project_rel(root, self.path),
            "writer_context_id": self.writer_context_id,
            "sandbox_id": self.sandbox_id,
            "run_id": self.run_id,
            "cycle_id": self.cycle_id,
            "resource_lane": self.resource_lane,
            "main_chain_mutation_allowed": self.main_chain_mutation_allowed,
            "reason_codes": list(self.reason_codes),
        }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"


def daemon_writer_inventory() -> list[dict[str, Any]]:
    rows = [
        {
            "component": "snapshot",
            "function": "run_snapshot_daemon_tick/_write_status_and_heartbeat",
            "path_or_db": "DATA/market/futures_light_snapshot.json; DATA/runtime/snapshot_daemon_status.json",
            "main_chain_mutation": True,
            "sandbox_migration_status": "adapter_ready",
            "risk_level": "high",
            "migration_task": "STEP33.3",
            "notes": "latest snapshot/status must route to sandbox daemon_outputs in sandbox lane",
        },
        {
            "component": "rest_circuit",
            "function": "market.rest_circuit/shared REST budget files",
            "path_or_db": "DATA/runtime/rest_circuit*.json",
            "main_chain_mutation": True,
            "sandbox_migration_status": "shared_governance_not_sandbox_scoped",
            "risk_level": "medium",
            "migration_task": "STEP33.3",
            "notes": "REST budget remains shared governance; sandbox lane may observe but must not fork the budget",
        },
        {
            "component": "micro",
            "function": "micro.daemon.state_writer.atomic_write_micro_state",
            "path_or_db": "DATA/micro/latest_micro_state.json; DATA/micro/latest_micro_features.json; micro SQLite ledgers",
            "main_chain_mutation": True,
            "sandbox_migration_status": "adapter_ready",
            "risk_level": "high",
            "migration_task": "STEP33.4",
            "notes": "sandbox lane must write latest/features/evidence under daemon_outputs/micro",
        },
        {
            "component": "strategy4",
            "function": "strategy4.observe._write_strategy4_doc/write_status/_settle_pool_after_attempt",
            "path_or_db": "DATA/decisions/latest_trade_plan_strategy4.json; strategy4 status/archive/attempt DB",
            "main_chain_mutation": True,
            "sandbox_migration_status": "adapter_ready",
            "risk_level": "high",
            "migration_task": "STEP33.5",
            "notes": "strategy semantics unchanged; only output namespace changes under sandbox context",
        },
        {
            "component": "strategy5",
            "function": "strategy5.evidence writer",
            "path_or_db": "DATA/decisions/latest_trade_plan_strategy5.json; strategy5 evidence/archive/SQLite",
            "main_chain_mutation": True,
            "sandbox_migration_status": "adapter_ready",
            "risk_level": "high",
            "migration_task": "STEP33.5",
            "notes": "latest trade plan/evidence must not overwrite production current JSON in sandbox lane",
        },
        {
            "component": "strategy6",
            "function": "strategy6.evidence/daemon writers",
            "path_or_db": "DATA/decisions/latest_trade_plan_strategy6.json; wait pool; observe attempts; evidence DB",
            "main_chain_mutation": True,
            "sandbox_migration_status": "adapter_ready",
            "risk_level": "high",
            "migration_task": "STEP33.5",
            "notes": "wait pool/attempt/evidence outputs route to sandbox daemon_outputs/strategy6",
        },
        {
            "component": "paper",
            "function": "paper.daemon.run_once/PaperEngine.tick/PaperStore.close_position",
            "path_or_db": "DATA/paper/paper_trading.db; DATA/paper/latest_paper_state.json; P24/P29/TQ side effects",
            "main_chain_mutation": True,
            "sandbox_migration_status": "adapter_ready_via_step32_bridge",
            "risk_level": "high",
            "migration_task": "STEP33.6",
            "notes": "sandbox PaperEngine uses sandbox paper DB and still runs TQ/P29 completion",
        },
        {
            "component": "training_sidecar",
            "function": "training_snapshot_sync.sync_paper_sqlite_source",
            "path_or_db": "DATA/research/trade_snapshots/trade_snapshots.db",
            "main_chain_mutation": False,
            "sandbox_migration_status": "sidecar_append_only_training_dataset",
            "risk_level": "medium",
            "migration_task": "STEP33.6",
            "notes": "training DB is isolated from business chain DB and tagged source_mode=sandbox_paper",
        },
    ]
    required = {
        "component",
        "function",
        "path_or_db",
        "main_chain_mutation",
        "sandbox_migration_status",
        "risk_level",
        "migration_task",
    }
    for row in rows:
        missing = sorted(required - set(row))
        if missing:
            raise ValueError(f"inventory_row_missing_fields:{row.get('component')}:{','.join(missing)}")
    return rows


def write_daemon_writer_inventory(project_root: Path) -> dict[str, Any]:
    root = Path(project_root).resolve()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "inventory_path": INVENTORY_PATH.as_posix(),
        "count": len(daemon_writer_inventory()),
        "writers": daemon_writer_inventory(),
    }
    out = root / INVENTORY_PATH
    out.parent.mkdir(parents=True, exist_ok=True)
    write_file_atomic(out, _json_bytes(payload))
    return payload


def write_daemon_writer_inventory_report(project_root: Path, *, suffix: str | None = None) -> dict[str, Any]:
    root = Path(project_root).resolve()
    inventory = write_daemon_writer_inventory(root)
    got_suffix = suffix or datetime.now(timezone.utc).strftime("%Y%m%d")
    report_path = root / "docs" / "reports" / f"STEP33.1_daemon_writer_inventory_{got_suffix}.md"
    lines = [
        "# STEP33.1 Daemon Writer Inventory",
        "",
        f"- generated_at: `{inventory['generated_at']}`",
        f"- schema_version: `{SCHEMA_VERSION}`",
        f"- inventory_path: `{inventory['inventory_path']}`",
        f"- writer_count: `{inventory['count']}`",
        "",
        "| component | migration_task | risk | migration_status | main_chain_mutation | path_or_db |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in inventory["writers"]:
        lines.append(
            "| {component} | {migration_task} | {risk_level} | {sandbox_migration_status} | {main_chain_mutation} | {path_or_db} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Result",
            "",
            "Inventory complete. P33 migration must keep strategy, paper, and backtest semantics unchanged; only daemon writer targets are sandbox-scoped when a valid SandboxWriterContext is present.",
        ]
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    write_file_atomic(report_path, ("\n".join(lines) + "\n").encode("utf-8"))
    return {"report_path": project_rel(root, report_path), "inventory": inventory}


def _context_payload(writer_context: dict[str, Any] | None) -> tuple[str | None, dict[str, Any] | None]:
    if not writer_context:
        return None, None
    context_id = writer_context.get("context_id") if isinstance(writer_context, dict) else None
    ctx = writer_context.get("context") if isinstance(writer_context.get("context"), dict) else writer_context
    if not isinstance(ctx, dict):
        return str(context_id) if context_id else None, None
    return str(context_id) if context_id else None, validate_writer_context_payload(ctx)


def _sandbox_base(project_root: Path, writer_context: dict[str, Any], ctx: dict[str, Any]) -> Path:
    targets = writer_context.get("writer_targets") if isinstance(writer_context.get("writer_targets"), dict) else {}
    runtime_rel = targets.get("sandbox_runtime_dir")
    if runtime_rel:
        return Path(project_root).resolve() / str(runtime_rel)
    return sandbox_run_dir(Path(project_root).resolve(), str(ctx["sandbox_id"]), str(ctx["run_id"]))


def _filename(component: str, logical_name: str, production_path: Path | str | None) -> str:
    if production_path:
        return Path(production_path).name
    defaults = {
        ("snapshot", "latest_snapshot"): "futures_light_snapshot.json",
        ("snapshot", "status"): "snapshot_daemon_status.json",
        ("snapshot", "heartbeat"): "snapshot_daemon_heartbeat.json",
        ("micro", "latest_state"): "latest_micro_state.json",
        ("micro", "latest_features"): "latest_micro_features.json",
        ("strategy4", "latest_trade_plan"): "latest_trade_plan_strategy4.json",
        ("strategy4", "status"): "strategy4_status.json",
        ("strategy5", "latest_trade_plan"): "latest_trade_plan_strategy5.json",
        ("strategy5", "evidence"): "latest_strategy5_evidence.json",
        ("strategy6", "latest_trade_plan"): "latest_trade_plan_strategy6.json",
        ("strategy6", "wait_pool"): "latest_strategy6_wait_pool.json",
        ("paper", "status"): "paper_daemon_status.json",
        ("paper", "summary"): "latest_paper_state.json",
    }
    return defaults.get((component, logical_name), f"{safe_part(logical_name)}.json")


def resolve_daemon_writer_target(
    project_root: Path,
    *,
    component: str,
    logical_name: str,
    production_path: Path | str | None = None,
    writer_context: dict[str, Any] | None = None,
    sandbox_required: bool = False,
) -> DaemonWriterTarget:
    root = Path(project_root).resolve()
    comp = safe_part(component)
    logical = safe_part(logical_name)
    if comp in SHARED_GOVERNANCE_COMPONENTS:
        if production_path is None:
            raise SandboxWriterContextError(f"production_path_required_for_shared_governance:{comp}:{logical}")
        return DaemonWriterTarget(
            component=comp,
            logical_name=logical,
            mode="shared_governance_observed",
            path=(root / production_path).resolve() if not Path(production_path).is_absolute() else Path(production_path),
            writer_context_id=None,
            sandbox_id=None,
            run_id=None,
            cycle_id=None,
            resource_lane=None,
            main_chain_mutation_allowed=False,
            reason_codes=("shared_rest_budget_not_sandbox_scoped",),
        )

    context_id, ctx = _context_payload(writer_context)
    if ctx is None:
        if sandbox_required:
            raise SandboxWriterContextError(f"sandbox_writer_context_required:{comp}:{logical}")
        if production_path is None:
            raise SandboxWriterContextError(f"production_path_required_for_production_lane:{comp}:{logical}")
        return DaemonWriterTarget(
            component=comp,
            logical_name=logical,
            mode="production_default_lane",
            path=(root / production_path).resolve() if not Path(production_path).is_absolute() else Path(production_path),
            writer_context_id=None,
            sandbox_id=None,
            run_id=None,
            cycle_id=None,
            resource_lane=None,
            main_chain_mutation_allowed=True,
            reason_codes=("production_default_path_unchanged",),
        )

    base = _sandbox_base(root, writer_context or {}, ctx)
    path = base / "daemon_outputs" / comp / _filename(comp, logical, production_path)
    return DaemonWriterTarget(
        component=comp,
        logical_name=logical,
        mode="sandbox_scoped_daemon_writer",
        path=path,
        writer_context_id=context_id,
        sandbox_id=str(ctx["sandbox_id"]),
        run_id=str(ctx["run_id"]),
        cycle_id=str(ctx["cycle_id"]),
        resource_lane=str(ctx["resource_lane"]),
        main_chain_mutation_allowed=False,
        reason_codes=("sandbox_scoped_writer_context_validated", "main_chain_mutation_denied"),
    )


def snapshot_writer_targets(project_root: Path, *, writer_context: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "latest_snapshot": resolve_daemon_writer_target(
            project_root,
            component="snapshot",
            logical_name="latest_snapshot",
            production_path=Path("DATA") / "market" / "futures_light_snapshot.json",
            writer_context=writer_context,
            sandbox_required=writer_context is not None,
        ).to_payload(Path(project_root)),
        "status": resolve_daemon_writer_target(
            project_root,
            component="snapshot",
            logical_name="status",
            production_path=Path("DATA") / "runtime" / "snapshot_daemon_status.json",
            writer_context=writer_context,
            sandbox_required=writer_context is not None,
        ).to_payload(Path(project_root)),
        "rest_circuit": resolve_daemon_writer_target(
            project_root,
            component="rest_circuit",
            logical_name="budget",
            production_path=Path("DATA") / "runtime" / "rest_circuit.json",
        ).to_payload(Path(project_root)),
    }


def micro_writer_targets(project_root: Path, *, writer_context: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "latest_state": resolve_daemon_writer_target(
            project_root,
            component="micro",
            logical_name="latest_state",
            production_path=Path("DATA") / "micro" / "latest_micro_state.json",
            writer_context=writer_context,
            sandbox_required=writer_context is not None,
        ).to_payload(Path(project_root)),
        "latest_features": resolve_daemon_writer_target(
            project_root,
            component="micro",
            logical_name="latest_features",
            production_path=Path("DATA") / "micro" / "latest_micro_features.json",
            writer_context=writer_context,
            sandbox_required=writer_context is not None,
        ).to_payload(Path(project_root)),
    }


def strategy_writer_targets(
    project_root: Path,
    *,
    strategy_id: str,
    writer_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    strategy = safe_part(strategy_id)
    return {
        "latest_trade_plan": resolve_daemon_writer_target(
            project_root,
            component=strategy,
            logical_name="latest_trade_plan",
            production_path=Path("DATA") / "decisions" / f"latest_trade_plan_{strategy}.json",
            writer_context=writer_context,
            sandbox_required=writer_context is not None,
        ).to_payload(Path(project_root)),
        "evidence": resolve_daemon_writer_target(
            project_root,
            component=strategy,
            logical_name="evidence",
            production_path=Path("DATA") / "runtime" / f"{strategy}_evidence.json",
            writer_context=writer_context,
            sandbox_required=writer_context is not None,
        ).to_payload(Path(project_root)),
    }


def paper_daemon_config(
    project_root: Path,
    *,
    writer_context: dict[str, Any] | None = None,
    base: PaperConfig | None = None,
) -> PaperConfig:
    if not writer_context:
        return base or PaperConfig()
    _, ctx = _context_payload(writer_context)
    if ctx is None:
        raise SandboxWriterContextError("sandbox_writer_context_required:paper:daemon_config")
    targets = writer_context.get("writer_targets") if isinstance(writer_context.get("writer_targets"), dict) else {}
    return sandbox_paper_config(
        Path(project_root).resolve(),
        sandbox_id=str(ctx["sandbox_id"]),
        run_id=str(ctx["run_id"]),
        run_root_rel=targets.get("sandbox_runtime_dir"),
        base=base,
    )


def daemon_writer_status_payload(project_root: Path, *, writer_context: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(project_root).resolve()
    inventory = daemon_writer_inventory()
    adapters = {
        "snapshot": snapshot_writer_targets(root, writer_context=writer_context),
        "micro": micro_writer_targets(root, writer_context=writer_context),
        "strategy4": strategy_writer_targets(root, strategy_id="strategy4", writer_context=writer_context),
        "strategy5": strategy_writer_targets(root, strategy_id="strategy5", writer_context=writer_context),
        "strategy6": strategy_writer_targets(root, strategy_id="strategy6", writer_context=writer_context),
    }
    paper_cfg = paper_daemon_config(root, writer_context=writer_context)
    adapters["paper"] = {
        "db_path": paper_cfg.db_path,
        "summary_path": paper_cfg.summary_path,
        "status_path": paper_cfg.daemon_status_path,
        "mode": "sandbox_scoped_daemon_writer" if writer_context else "production_default_lane",
        "main_chain_mutation_allowed": False if writer_context else True,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "inventory_count": len(inventory),
        "writers": inventory,
        "adapters": adapters,
        "observer_modes": ["production_default_lane", "ui_active_sandbox_real_pipeline", "external_cli_research_lane"],
    }
