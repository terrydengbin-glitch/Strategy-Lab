from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.backtest.p21_real_evaluator import ENGINE_MODE, evaluate_signal_offline
from laoma_signal_engine.backtest.p21_v2 import _metrics as p21_metrics
from laoma_signal_engine.backtest.p21_v2 import simulate_1m_fill
from laoma_signal_engine.trade_quality.engine import label_root_cause
from laoma_signal_engine.training_snapshot_sync import sync_sandbox_job_result

SCHEMA_VERSION = "23.1-strategy-sandbox"
SUPPORTED_STRATEGY_LINES = {"without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"}
STRATEGY_ALIAS_TO_LINE = {
    "strategy1": "without_micro",
    "strategy2": "micro_fast",
    "strategy3": "micro_full",
    "without_micro": "without_micro",
    "micro_fast": "micro_fast",
    "micro_full": "micro_full",
    "strategy4": "strategy4",
    "strategy5": "strategy5",
    "strategy6": "strategy6",
}
EXPERIMENT_SANDBOX_LINE = "experiment"
SANDBOX_ROOT_ENV = "LAOMA_SANDBOX_ROOT"
MAIN_BASELINE_FILES = (
    "laoma_signal_engine/config/default.yaml",
    "task_index.md",
    "DATA/decisions/latest_trade_plan_strategy4.json",
    "DATA/decisions/latest_trade_plan_strategy5.json",
    "DATA/decisions/latest_trade_plan_strategy6.json",
)
EXTERNAL_SURFACES = {"external_connector", "external_ai_trader"}
EXTERNAL_CALLER_TYPES = {"external", "external_ai", "external_ai_trader"}


@dataclass(frozen=True)
class SandboxSignal:
    signal_id: str
    strategy_line: str
    symbol: str
    side: str
    index: int
    signal_time_ms: int
    score: float
    features: dict[str, Any]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _loads(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _stable_id(prefix: str, payload: Any, size: int = 16) -> str:
    digest = hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()[:size]
    return f"{prefix}_{digest}"


def _hash_payload(payload: Any, size: int = 24) -> str:
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()[:size]


def _operation_context(
    operation_kind: str,
    raw: dict[str, Any] | None = None,
    *,
    source_surface: str = "fastapi",
    caller_type: str = "local_ui",
    caller_id: str = "local",
    operation_policy: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    audit_trace_id: str | None = None,
) -> dict[str, Any]:
    raw = raw or {}
    policy = raw.get("operation_policy") if isinstance(raw.get("operation_policy"), dict) else operation_policy or {}
    surface = str(raw.get("source_surface") or source_surface or "fastapi")
    ctype = str(raw.get("caller_type") or caller_type or "local_ui")
    cid = str(raw.get("caller_id") or caller_id or surface)
    identity_source = str(raw.get("caller_identity_source") or ("body" if raw else "default"))
    idem = str(raw.get("idempotency_key") or idempotency_key or "").strip() or None
    trace = str(raw.get("audit_trace_id") or audit_trace_id or "").strip()
    if not trace:
        trace = _stable_id("sbaudit", {"operation": operation_kind, "surface": surface, "caller": cid, "nonce": uuid.uuid4().hex}, 22)
    operation_id = _stable_id(
        "sbop",
        {
            "operation": operation_kind,
            "source_surface": surface,
            "caller_type": ctype,
            "caller_id": cid,
            "idempotency_key": idem,
            "audit_trace_id": trace,
        },
        22,
    )
    return {
        "operation_id": operation_id,
        "operation_kind": operation_kind,
        "source_surface": surface,
        "caller_type": ctype,
        "caller_id": cid,
        "caller_identity_source": identity_source,
        "operation_policy": policy,
        "authenticated": bool(policy.get("authenticated")),
        "idempotency_key": idem,
        "audit_trace_id": trace,
        "write_scope": "sandbox_only",
    }


def _is_external_context(ctx: dict[str, Any]) -> bool:
    return str(ctx.get("source_surface") or "") in EXTERNAL_SURFACES or str(ctx.get("caller_type") or "") in EXTERNAL_CALLER_TYPES


def _assert_operation_allowed(ctx: dict[str, Any], *, mode: str | None = None) -> None:
    kind = str(ctx.get("operation_kind") or "")
    policy = ctx.get("operation_policy") if isinstance(ctx.get("operation_policy"), dict) else {}
    external = _is_external_context(ctx)
    if policy.get("auth_failed") or (policy.get("auth_required") and not policy.get("authenticated")):
        raise ValueError("sandbox_policy_denied: api_key_required")
    if kind in {"set_active", "clear_active"} and external and not policy.get("allow_active_context_write"):
        raise ValueError("sandbox_policy_denied: external_active_context_write_denied")
    if kind == "delete" and external and str(mode or "") == "purge":
        raise ValueError("sandbox_policy_denied: external_purge_denied")
    if kind == "delete" and str(mode or "") == "purge" and not policy.get("allow_purge", True):
        raise ValueError("sandbox_policy_denied: purge_policy_denied")
    if kind == "code_overlay" and external and not policy.get("allow_code_overlay"):
        raise ValueError("sandbox_policy_denied: external_code_overlay_denied")


def _with_operation_payload(payload: dict[str, Any], ctx: dict[str, Any], **extra: Any) -> dict[str, Any]:
    out = dict(payload)
    out.update(
        {
            "operation_id": ctx.get("operation_id"),
            "source_surface": ctx.get("source_surface"),
            "caller_type": ctx.get("caller_type"),
            "caller_id": ctx.get("caller_id"),
            "caller_identity_source": ctx.get("caller_identity_source"),
            "authenticated": bool(ctx.get("authenticated")),
            "write_scope": ctx.get("write_scope", "sandbox_only"),
            "audit_trace_id": ctx.get("audit_trace_id"),
        }
    )
    out.update(extra)
    return out


def _parameter_set_id(strategy_line: str, options: dict[str, Any]) -> str | None:
    explicit = str(options.get("parameter_set_id") or "").strip()
    if explicit:
        return explicit
    if options.get("_matrix_parameter_set") or options.get("matrix_profile"):
        strategy_params = options.get("strategy_params") if isinstance(options.get("strategy_params"), dict) else {}
        payload = {
            "strategy_line": strategy_line,
            "matrix_profile": options.get("matrix_profile"),
            "strategy_params": strategy_params,
            "target_rr": options.get("target_rr"),
            "min_score": options.get("min_score"),
            "max_stop_bps": options.get("max_stop_bps"),
        }
        return _stable_id("ps", payload, 18)
    return None


def _strategy_line_alias(strategy_line: str) -> str:
    value = str(strategy_line or "").strip()
    return STRATEGY_ALIAS_TO_LINE.get(value, value)


def sandbox_root(root: Path | None = None) -> Path:
    if root is not None:
        return Path(root)
    configured = os.environ.get(SANDBOX_ROOT_ENV)
    if configured:
        return Path(configured)
    return Path.cwd() / "DATA" / "sandboxes"


def _p29_project_root_from_sandbox_root(root: Path | None = None) -> Path:
    """Return the project root for P29 sidecar writes from a sandbox-root input."""
    got = sandbox_root(root).resolve()
    if got.name == "sandboxes" and got.parent.name == "DATA":
        return got.parent.parent
    return Path.cwd().resolve()


def registry_db_path(root: Path | None = None) -> Path:
    return sandbox_root(root) / "sandbox_registry.db"


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.OperationalError:
        pass
    return conn


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _file_hash(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _main_baseline_context() -> dict[str, Any]:
    root = Path.cwd()
    refs: dict[str, Any] = {}
    for rel in MAIN_BASELINE_FILES:
        path = root / rel
        refs[rel] = {
            "exists": path.exists(),
            "sha256": _file_hash(path),
            "size": path.stat().st_size if path.exists() and path.is_file() else None,
            "mtime": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            if path.exists()
            else None,
        }
    digest = hashlib.sha256(_json(refs).encode("utf-8")).hexdigest()[:24]
    return {
        "baseline_context_id": f"main_{digest}",
        "baseline_parent_type": "main_system",
        "baseline_parent_id": digest,
        "derived_from_sandbox_id": None,
        "write_scope": "sandbox_only",
        "write_guard_status": "enforced",
        "source_hash": refs,
        "captured_at": _now(),
    }


def _registry_conn(root: Path | None = None) -> sqlite3.Connection:
    path = registry_db_path(root)
    conn = _connect(path)
    ensure_registry_tables(conn)
    return conn


def ensure_registry_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sandbox_registry(
          sandbox_id TEXT PRIMARY KEY,
          strategy_line TEXT NOT NULL,
          strategy_version TEXT NOT NULL,
          status TEXT NOT NULL,
          root_path TEXT NOT NULL,
          db_path TEXT NOT NULL,
          data_scope_json TEXT NOT NULL,
          config_scope_json TEXT NOT NULL,
          source_refs_json TEXT NOT NULL,
          storage_policy_json TEXT NOT NULL,
          llm_training_policy_json TEXT NOT NULL,
          tags_json TEXT NOT NULL,
          best_pf REAL,
          best_candidate_id TEXT,
          last_job_status TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          schema_version TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sandbox_registry_strategy_status
          ON sandbox_registry(strategy_line, status, updated_at);

        CREATE TABLE IF NOT EXISTS active_sandbox_context(
          context_key TEXT PRIMARY KEY,
          sandbox_id TEXT,
          updated_at TEXT NOT NULL
        );
        """
    )
    _ensure_columns(
        conn,
        "sandbox_registry",
        {
            "baseline_context_id": "TEXT",
            "baseline_parent_type": "TEXT",
            "baseline_parent_id": "TEXT",
            "derived_from_sandbox_id": "TEXT",
            "homepage_context_json": "TEXT",
            "strategy_adjustment_scope_json": "TEXT",
            "source_hash_json": "TEXT",
            "write_scope": "TEXT",
            "write_guard_status": "TEXT",
            "deleted_at": "TEXT",
            "delete_mode": "TEXT",
            "delete_reason": "TEXT",
        },
    )


def ensure_sandbox_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sandbox_manifest(
          sandbox_id TEXT PRIMARY KEY,
          strategy_line TEXT NOT NULL,
          strategy_version TEXT NOT NULL,
          status TEXT NOT NULL,
          data_scope_json TEXT NOT NULL,
          config_scope_json TEXT NOT NULL,
          source_refs_json TEXT NOT NULL,
          storage_policy_json TEXT NOT NULL,
          llm_training_policy_json TEXT NOT NULL,
          tags_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          schema_version TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sandbox_baseline_context(
          baseline_context_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          baseline_parent_type TEXT NOT NULL,
          baseline_parent_id TEXT NOT NULL,
          derived_from_sandbox_id TEXT,
          homepage_context_json TEXT NOT NULL,
          strategy_adjustment_scope_json TEXT NOT NULL,
          source_hash_json TEXT NOT NULL,
          write_scope TEXT NOT NULL,
          write_guard_status TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sandbox_strategy_branches(
          branch_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          strategy_version TEXT NOT NULL,
          branch_status TEXT NOT NULL,
          branch_config_scope_json TEXT NOT NULL,
          branch_data_scope_json TEXT NOT NULL,
          branch_metrics_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sandbox_strategy_branches
          ON sandbox_strategy_branches(sandbox_id, strategy_line, branch_status);
        CREATE TABLE IF NOT EXISTS sandbox_branch_leaderboard(
          leaderboard_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          best_pf REAL,
          trade_count INTEGER NOT NULL DEFAULT 0,
          tq_sample_count INTEGER NOT NULL DEFAULT 0,
          gate_candidate_count INTEGER NOT NULL DEFAULT 0,
          best_candidate_id TEXT,
          metrics_json TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sandbox_branch_leaderboard
          ON sandbox_branch_leaderboard(sandbox_id, strategy_line, best_pf);
        CREATE TABLE IF NOT EXISTS sandbox_parameter_sets(
          parameter_set_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          matrix_profile TEXT NOT NULL,
          params_json TEXT NOT NULL,
          status TEXT NOT NULL,
          metrics_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sandbox_parameter_sets
          ON sandbox_parameter_sets(sandbox_id, strategy_line, matrix_profile, status);
        CREATE TABLE IF NOT EXISTS strategy_specs(
          spec_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          strategy_version TEXT NOT NULL,
          config_candidate_json TEXT NOT NULL,
          evaluator_contract_json TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS historical_input_snapshots(
          snapshot_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          source_refs_json TEXT NOT NULL,
          data_scope_json TEXT NOT NULL,
          quality_json TEXT NOT NULL,
          frozen_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS evaluator_runs(
          run_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          adapter_name TEXT NOT NULL,
          status TEXT NOT NULL,
          metrics_json TEXT NOT NULL,
          evidence_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sandbox_orders(
          order_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          evaluator_run_id TEXT,
          fill_run_id TEXT,
          strategy_line TEXT NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          signal_time_ms INTEGER NOT NULL,
          entry_time_ms INTEGER NOT NULL,
          exit_time_ms INTEGER,
          entry_price REAL NOT NULL,
          exit_price REAL,
          stop_loss REAL NOT NULL,
          take_profit REAL NOT NULL,
          planned_rr REAL NOT NULL,
          net_R REAL,
          MFE_R REAL,
          MAE_R REAL,
          exit_reason TEXT,
          score REAL NOT NULL,
          reasons_json TEXT NOT NULL,
          features_json TEXT NOT NULL,
          trade_plan_payload_json TEXT NOT NULL,
          fill_result_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sandbox_orders
          ON sandbox_orders(sandbox_id, strategy_line, symbol, entry_time_ms);
        CREATE TABLE IF NOT EXISTS fill_model_runs(
          run_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          assumption_id TEXT NOT NULL,
          same_candle_policy TEXT NOT NULL,
          status TEXT NOT NULL,
          metrics_json TEXT NOT NULL,
          evidence_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trade_quality_samples(
          sample_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          trade_id TEXT,
          symbol TEXT,
          side TEXT,
          entry_time TEXT,
          exit_time TEXT,
          net_R REAL,
          MFE_R REAL,
          MAE_R REAL,
          root_cause TEXT,
          features_known_at_entry_json TEXT NOT NULL,
          future_outcome_labels_json TEXT NOT NULL,
          source_ref TEXT NOT NULL,
          review_status TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sandbox_tq_samples
          ON trade_quality_samples(sandbox_id, root_cause, symbol, side);
        CREATE TABLE IF NOT EXISTS gate_candidates(
          candidate_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          rule_json TEXT NOT NULL,
          train_metrics_json TEXT NOT NULL,
          validation_metrics_json TEXT NOT NULL,
          test_metrics_json TEXT NOT NULL,
          overfit_risk TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS holdout_validations(
          validation_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          candidate_id TEXT,
          split_json TEXT NOT NULL,
          leakage_report_json TEXT NOT NULL,
          decision TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS config_candidates(
          config_candidate_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          candidate_id TEXT,
          target_profile TEXT NOT NULL,
          patch_json TEXT NOT NULL,
          promotion_state TEXT NOT NULL,
          review_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trade_plan_candidates(
          trade_plan_candidate_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          config_candidate_id TEXT,
          candidate_reason TEXT NOT NULL,
          trade_plan_json TEXT NOT NULL,
          metrics_json TEXT NOT NULL,
          promotion_state TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS paper_shadow_results(
          shadow_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          config_candidate_id TEXT,
          status TEXT NOT NULL,
          metrics_json TEXT NOT NULL,
          evidence_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS llm_dataset_exports(
          export_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          dataset_card_json TEXT NOT NULL,
          sample_count INTEGER NOT NULL,
          leakage_status TEXT NOT NULL,
          export_path TEXT,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sandbox_code_overlays(
          code_overlay_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          base_source_hash_json TEXT NOT NULL,
          overlay_path TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sandbox_code_overlays
          ON sandbox_code_overlays(sandbox_id, strategy_line, status, updated_at);
        CREATE TABLE IF NOT EXISTS sandbox_code_patches(
          code_patch_id TEXT PRIMARY KEY,
          code_overlay_id TEXT NOT NULL,
          sandbox_id TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          patch_type TEXT NOT NULL,
          target_relpath TEXT NOT NULL,
          patch_json TEXT NOT NULL,
          diff_text TEXT NOT NULL,
          author TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sandbox_code_patches
          ON sandbox_code_patches(sandbox_id, strategy_line, status, created_at);
        CREATE TABLE IF NOT EXISTS sandbox_evaluator_runtime(
          runtime_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          code_overlay_id TEXT,
          code_patch_id TEXT,
          runtime_manifest_json TEXT NOT NULL,
          import_map_json TEXT NOT NULL,
          code_digest_json TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sandbox_evaluator_runtime
          ON sandbox_evaluator_runtime(sandbox_id, strategy_line, status, created_at);
        CREATE TABLE IF NOT EXISTS sandbox_jobs(
          job_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          job_type TEXT NOT NULL,
          status TEXT NOT NULL,
          progress_json TEXT NOT NULL,
          result_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sandbox_metric_summary(
          sandbox_id TEXT PRIMARY KEY,
          best_pf REAL,
          best_candidate_id TEXT,
          trade_count INTEGER NOT NULL DEFAULT 0,
          tq_sample_count INTEGER NOT NULL DEFAULT 0,
          gate_candidate_count INTEGER NOT NULL DEFAULT 0,
          llm_export_count INTEGER NOT NULL DEFAULT 0,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS external_full_backtest_runs(
          run_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          idempotency_key TEXT,
          scope_hash TEXT NOT NULL,
          universe_hash TEXT NOT NULL,
          status TEXT NOT NULL,
          time_start TEXT,
          time_end TEXT,
          timeframe TEXT NOT NULL,
          bar_source TEXT NOT NULL,
          symbols_json TEXT NOT NULL,
          data_quality_summary_json TEXT NOT NULL,
          resource_budget_json TEXT NOT NULL,
          progress_json TEXT NOT NULL,
          completed_batches INTEGER NOT NULL DEFAULT 0,
          failed_batches INTEGER NOT NULL DEFAULT 0,
          expected_batches INTEGER NOT NULL DEFAULT 0,
          error_code TEXT,
          error_message TEXT,
          retryable INTEGER NOT NULL DEFAULT 1,
          resume_token TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          finished_at TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_external_full_backtest_runs_idempotency
          ON external_full_backtest_runs(sandbox_id, idempotency_key)
          WHERE idempotency_key IS NOT NULL AND idempotency_key != '';
        CREATE INDEX IF NOT EXISTS idx_external_full_backtest_runs_status
          ON external_full_backtest_runs(sandbox_id, strategy_line, status, updated_at);
        CREATE TABLE IF NOT EXISTS external_full_backtest_batches(
          batch_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          sandbox_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          batch_index INTEGER NOT NULL,
          status TEXT NOT NULL,
          symbols_json TEXT NOT NULL,
          symbol_count INTEGER NOT NULL,
          completed_symbol_count INTEGER NOT NULL DEFAULT 0,
          failed_symbol_count INTEGER NOT NULL DEFAULT 0,
          progress_json TEXT NOT NULL,
          error_code TEXT,
          error_message TEXT,
          retryable INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          finished_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_external_full_backtest_batches_run
          ON external_full_backtest_batches(run_id, batch_index);
        CREATE TABLE IF NOT EXISTS external_full_backtest_events(
          event_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          sandbox_id TEXT NOT NULL,
          event_type TEXT NOT NULL,
          status TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_external_full_backtest_events_run
          ON external_full_backtest_events(run_id, created_at);
        CREATE TABLE IF NOT EXISTS external_trade_candidates(
          candidate_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          sandbox_id TEXT NOT NULL,
          branch_id TEXT,
          strategy_line TEXT NOT NULL,
          strategy_version TEXT,
          source_mode TEXT NOT NULL,
          source_order_id TEXT,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          decision_time TEXT NOT NULL,
          intended_size REAL NOT NULL,
          order_type TEXT,
          entry_price_hint REAL,
          limit_price REAL,
          stop_loss REAL,
          take_profit REAL,
          planned_rr REAL,
          price_context_json TEXT NOT NULL,
          risk_context_json TEXT NOT NULL,
          decision_time_features_json TEXT NOT NULL,
          context_refs_json TEXT NOT NULL,
          feature_schema_version TEXT NOT NULL,
          leakage_status TEXT NOT NULL,
          leakage_report_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_external_trade_candidates_run
          ON external_trade_candidates(sandbox_id, strategy_line, run_id, created_at);
        CREATE TABLE IF NOT EXISTS external_gate_actions(
          gate_action_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          candidate_id TEXT NOT NULL,
          sandbox_id TEXT NOT NULL,
          branch_id TEXT,
          strategy_line TEXT NOT NULL,
          unit_id TEXT NOT NULL,
          unit_version TEXT NOT NULL,
          selection_id TEXT,
          scorer_output_ref TEXT,
          final_gate_decision_ref TEXT,
          gate_decision TEXT NOT NULL,
          gate_action_payload_json TEXT NOT NULL,
          reason_codes_json TEXT NOT NULL,
          audit_trace_id TEXT,
          idempotency_key TEXT,
          status TEXT NOT NULL,
          applied_policy_json TEXT NOT NULL,
          error_code TEXT,
          error_message TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_external_gate_actions_idempotency
          ON external_gate_actions(sandbox_id, idempotency_key)
          WHERE idempotency_key IS NOT NULL AND idempotency_key != '';
        CREATE UNIQUE INDEX IF NOT EXISTS idx_external_gate_actions_active_candidate
          ON external_gate_actions(candidate_id)
          WHERE status = 'accepted';
        CREATE INDEX IF NOT EXISTS idx_external_gate_actions_run
          ON external_gate_actions(sandbox_id, strategy_line, run_id, created_at);
        CREATE TABLE IF NOT EXISTS external_gate_action_events(
          event_id TEXT PRIMARY KEY,
          gate_action_id TEXT,
          run_id TEXT,
          candidate_id TEXT,
          sandbox_id TEXT NOT NULL,
          event_type TEXT NOT NULL,
          status TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_external_gate_action_events_candidate
          ON external_gate_action_events(candidate_id, created_at);
        CREATE TABLE IF NOT EXISTS external_gated_runs(
          gated_run_id TEXT PRIMARY KEY,
          sandbox_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          run_id TEXT NOT NULL,
          baseline_run_id TEXT,
          gate_action_batch_id TEXT,
          execution_mode TEXT NOT NULL,
          execution_policy_json TEXT NOT NULL,
          status TEXT NOT NULL,
          candidate_count INTEGER NOT NULL DEFAULT 0,
          allowed_count INTEGER NOT NULL DEFAULT 0,
          blocked_count INTEGER NOT NULL DEFAULT 0,
          reduced_count INTEGER NOT NULL DEFAULT 0,
          review_count INTEGER NOT NULL DEFAULT 0,
          order_count INTEGER NOT NULL DEFAULT 0,
          result_ref TEXT,
          metrics_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_external_gated_runs_line
          ON external_gated_runs(sandbox_id, strategy_line, run_id, created_at);
        CREATE TABLE IF NOT EXISTS external_gated_orders(
          order_id TEXT PRIMARY KEY,
          gated_run_id TEXT NOT NULL,
          baseline_run_id TEXT,
          candidate_id TEXT NOT NULL,
          gate_action_id TEXT,
          sandbox_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          original_size REAL NOT NULL,
          executed_size REAL NOT NULL,
          gate_decision TEXT NOT NULL,
          applied_action TEXT NOT NULL,
          order_status TEXT NOT NULL,
          fill_status TEXT NOT NULL,
          context_refs_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_external_gated_orders_run
          ON external_gated_orders(gated_run_id, candidate_id);
        CREATE TABLE IF NOT EXISTS external_gated_results(
          result_id TEXT PRIMARY KEY,
          gated_run_id TEXT NOT NULL,
          order_id TEXT NOT NULL,
          candidate_id TEXT NOT NULL,
          sandbox_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          net_R REAL,
          MFE_R REAL,
          MAE_R REAL,
          exit_reason TEXT,
          quality_label TEXT,
          result_ref TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_external_gated_results_run
          ON external_gated_results(gated_run_id, candidate_id);
        CREATE TABLE IF NOT EXISTS external_gated_performance(
          performance_id TEXT PRIMARY KEY,
          gated_run_id TEXT NOT NULL,
          baseline_run_id TEXT,
          sandbox_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          baseline_metrics_json TEXT NOT NULL,
          gated_metrics_json TEXT NOT NULL,
          delta_metrics_json TEXT NOT NULL,
          coverage_json TEXT NOT NULL,
          result_refs_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_external_gated_performance_run
          ON external_gated_performance(gated_run_id);
        CREATE TABLE IF NOT EXISTS external_integration_audit_events(
          event_id TEXT PRIMARY KEY,
          event_type TEXT NOT NULL,
          sandbox_id TEXT,
          strategy_line TEXT,
          run_id TEXT,
          candidate_id TEXT,
          gate_action_id TEXT,
          gated_run_id TEXT,
          idempotency_key TEXT,
          request_hash TEXT,
          response_hash TEXT,
          status TEXT NOT NULL,
          error_code TEXT,
          retryable INTEGER NOT NULL DEFAULT 1,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_external_integration_audit_events_run
          ON external_integration_audit_events(run_id, gated_run_id, candidate_id, created_at);
        CREATE TABLE IF NOT EXISTS external_connector_smoke_runs(
          smoke_run_id TEXT PRIMARY KEY,
          sandbox_id TEXT,
          strategy_line TEXT,
          status TEXT NOT NULL,
          checks_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS external_contract_versions(
          contract_key TEXT PRIMARY KEY,
          contract_version TEXT NOT NULL,
          status TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    branch_column_tables = (
        "strategy_specs",
        "historical_input_snapshots",
        "evaluator_runs",
        "sandbox_orders",
        "fill_model_runs",
        "trade_quality_samples",
        "gate_candidates",
        "holdout_validations",
        "config_candidates",
        "trade_plan_candidates",
        "paper_shadow_results",
        "llm_dataset_exports",
        "sandbox_code_overlays",
        "sandbox_code_patches",
        "sandbox_evaluator_runtime",
        "sandbox_jobs",
    )
    for table in branch_column_tables:
        _ensure_columns(conn, table, {"branch_id": "TEXT"})
    strategy_column_tables = (
        "evaluator_runs",
        "fill_model_runs",
        "trade_quality_samples",
        "gate_candidates",
        "holdout_validations",
        "config_candidates",
        "paper_shadow_results",
        "llm_dataset_exports",
        "sandbox_jobs",
        "sandbox_metric_summary",
    )
    for table in strategy_column_tables:
        _ensure_columns(conn, table, {"strategy_line": "TEXT"})
    code_lineage_tables = (
        "evaluator_runs",
        "sandbox_orders",
        "fill_model_runs",
        "trade_quality_samples",
        "gate_candidates",
        "config_candidates",
        "paper_shadow_results",
        "llm_dataset_exports",
        "sandbox_jobs",
    )
    for table in code_lineage_tables:
        _ensure_columns(
            conn,
            table,
            {
                "code_overlay_id": "TEXT",
                "code_patch_id": "TEXT",
                "runtime_id": "TEXT",
            },
        )
    _ensure_columns(conn, "trade_plan_candidates", {"code_overlay_id": "TEXT", "code_patch_id": "TEXT", "runtime_id": "TEXT"})
    parameter_set_tables = (
        "evaluator_runs",
        "sandbox_orders",
        "fill_model_runs",
        "trade_quality_samples",
        "gate_candidates",
        "config_candidates",
        "trade_plan_candidates",
        "paper_shadow_results",
        "sandbox_jobs",
    )
    for table in parameter_set_tables:
        _ensure_columns(conn, table, {"parameter_set_id": "TEXT"})


def _validate_strategy_line(strategy_line: str) -> str:
    value = _strategy_line_alias(strategy_line)
    if value not in SUPPORTED_STRATEGY_LINES:
        raise ValueError(f"unsupported strategy_line: {strategy_line}")
    return value


def _validate_sandbox_line(strategy_line: str) -> str:
    value = str(strategy_line or "").strip()
    if value == EXPERIMENT_SANDBOX_LINE:
        return value
    return _validate_strategy_line(value)


def _validate_strategy_lines(strategy_lines: list[str] | None, fallback: str) -> list[str]:
    raw = strategy_lines or [fallback]
    seen: list[str] = []
    for item in raw:
        value = _validate_strategy_line(str(item))
        if value not in seen:
            seen.append(value)
    if not seen:
        raise ValueError("at least one strategy branch is required")
    return seen


def _branch_id(sandbox_id: str, strategy_line: str) -> str:
    return _stable_id("br", {"sandbox_id": sandbox_id, "strategy_line": strategy_line}, 18)


def _sandbox_id(strategy_line: str, strategy_version: str, data_scope: dict[str, Any], config_scope: dict[str, Any]) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem = _stable_id(
        "sb",
        {
            "strategy_line": strategy_line,
            "strategy_version": strategy_version,
            "data_scope": data_scope,
            "config_scope": config_scope,
            "nonce": uuid.uuid4().hex,
        },
        10,
    )
    return f"{strategy_line}_{stamp}_{stem}"


def _sandbox_paths(sandbox_id: str, strategy_line: str, root: Path | None = None) -> tuple[Path, Path]:
    base_line = "experiments" if strategy_line == EXPERIMENT_SANDBOX_LINE else strategy_line
    base = sandbox_root(root) / base_line / sandbox_id
    return base, base / "sandbox.db"


def _branch_rows(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    try:
        with _connect(db_path) as conn:
            ensure_sandbox_tables(conn)
            rows = conn.execute(
                """
                SELECT branch_id, sandbox_id, strategy_line, strategy_version, branch_status,
                       branch_config_scope_json, branch_data_scope_json, branch_metrics_json,
                       created_at, updated_at
                FROM sandbox_strategy_branches
                ORDER BY strategy_line ASC
                """
            ).fetchall()
            return [
                {
                    "branch_id": row["branch_id"],
                    "sandbox_id": row["sandbox_id"],
                    "strategy_line": row["strategy_line"],
                    "strategy_version": row["strategy_version"],
                    "branch_status": row["branch_status"],
                    "branch_config_scope": _loads(row["branch_config_scope_json"], {}),
                    "branch_data_scope": _loads(row["branch_data_scope_json"], {}),
                    "branch_metrics": _loads(row["branch_metrics_json"], {}),
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]
    except sqlite3.Error:
        return []


def _manifest_row(row: sqlite3.Row) -> dict[str, Any]:
    db_path = Path(row["db_path"])
    branches = _branch_rows(db_path)
    homepage_context = _loads(row["homepage_context_json"], {}) if "homepage_context_json" in row.keys() else {}
    return {
        "sandbox_id": row["sandbox_id"],
        "strategy_line": row["strategy_line"],
        "strategy_version": row["strategy_version"],
        "status": row["status"],
        "root_path": row["root_path"],
        "db_path": row["db_path"],
        "data_scope": _loads(row["data_scope_json"], {}),
        "config_scope": _loads(row["config_scope_json"], {}),
        "source_refs": _loads(row["source_refs_json"], []),
        "storage_policy": _loads(row["storage_policy_json"], {}),
        "llm_training_policy": _loads(row["llm_training_policy_json"], {}),
        "tags": _loads(row["tags_json"], []),
        "best_pf": row["best_pf"],
        "best_candidate_id": row["best_candidate_id"],
        "last_job_status": row["last_job_status"],
        "baseline_context_id": row["baseline_context_id"] if "baseline_context_id" in row.keys() else None,
        "baseline_parent_type": row["baseline_parent_type"] if "baseline_parent_type" in row.keys() else None,
        "baseline_parent_id": row["baseline_parent_id"] if "baseline_parent_id" in row.keys() else None,
        "derived_from_sandbox_id": row["derived_from_sandbox_id"] if "derived_from_sandbox_id" in row.keys() else None,
        "homepage_context": homepage_context,
        "reset_mode": homepage_context.get("reset_mode"),
        "strategy_adjustment_scope": _loads(row["strategy_adjustment_scope_json"], {}) if "strategy_adjustment_scope_json" in row.keys() else {},
        "write_scope": row["write_scope"] if "write_scope" in row.keys() else None,
        "write_guard_status": row["write_guard_status"] if "write_guard_status" in row.keys() else None,
        "deleted_at": row["deleted_at"] if "deleted_at" in row.keys() else None,
        "delete_mode": row["delete_mode"] if "delete_mode" in row.keys() else None,
        "delete_reason": row["delete_reason"] if "delete_reason" in row.keys() else None,
        "branches_summary": {
            "count": len(branches),
            "strategy_lines": [item["strategy_line"] for item in branches],
            "branches": branches,
        },
        "legacy_strategy_line": None if row["strategy_line"] == EXPERIMENT_SANDBOX_LINE else row["strategy_line"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "schema_version": row["schema_version"],
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(_json(payload), encoding="utf-8")
    tmp.replace(path)


def _directory_skeleton(base: Path) -> None:
    for child in ("config", "reports", "exports", "tq", "gates", "paper_shadow", "branches"):
        (base / child).mkdir(parents=True, exist_ok=True)


def _branch_directory_skeleton(base: Path, strategy_line: str) -> Path:
    branch_base = base / "branches" / strategy_line
    for child in (
        "artifacts",
        "reports",
        "exports",
        "code_overlay",
        "code_overlay/patches",
        "code_overlay/files",
        "code_overlay/metadata",
        "runtime",
    ):
        (branch_base / child).mkdir(parents=True, exist_ok=True)
    readme = branch_base / "code_overlay" / "README.md"
    if not readme.exists():
        readme.write_text(
            "Sandbox code overlay only. Files here are isolated from baseline and require explicit promotion.\n",
            encoding="utf-8",
        )
    return branch_base


def _strategy_adjustment_scope(strategy_line: str) -> dict[str, Any]:
    scopes = {
        "micro_fast": ["micro_readiness_params", "micro_fast_scoring", "trade_quality_gate"],
        "micro_full": ["micro_full_scoring", "depth_cvd_ofi_params", "trade_quality_gate"],
        "strategy4": ["observe_recheck", "gate_candidates", "scheduler_params"],
        "strategy5": ["direction_scoring", "tp_policy", "trade_quality_gate"],
        "strategy6": ["acceptance_gate", "rebound_wait", "price_quality_gate", "adaptive_exit"],
        "without_micro": ["direction_scoring", "tp_policy", "trade_quality_gate"],
    }
    return {
        "strategy_line": strategy_line,
        "allowed_adjustments": scopes.get(strategy_line, []),
        "write_scope": "sandbox_only",
        "production_mutation_allowed": False,
    }


def create_sandbox_payload(
    *,
    strategy_line: str = EXPERIMENT_SANDBOX_LINE,
    strategy_lines: list[str] | None = None,
    strategy_version: str = "review",
    data_scope: dict[str, Any] | None = None,
    config_scope: dict[str, Any] | None = None,
    source_refs: list[str] | None = None,
    storage_policy: dict[str, Any] | None = None,
    llm_training_policy: dict[str, Any] | None = None,
    tags: list[str] | None = None,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("create", operation_context)
    _assert_operation_allowed(op)
    requested_line = str(strategy_line or EXPERIMENT_SANDBOX_LINE).strip()
    branch_lines = _validate_strategy_lines(strategy_lines, requested_line if requested_line in SUPPORTED_STRATEGY_LINES else "strategy6")
    line = EXPERIMENT_SANDBOX_LINE if strategy_lines else _validate_sandbox_line(requested_line)
    if line == EXPERIMENT_SANDBOX_LINE and not strategy_lines:
        branch_lines = ["without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"]
    data_scope = data_scope or {"mode": "unspecified"}
    config_scope = config_scope or {"mode": "shadow_only"}
    source_refs = source_refs or []
    storage_policy = storage_policy or {"retention": "manual_review", "raw_large_table_policy": "external_ref_only"}
    llm_training_policy = llm_training_policy or {
        "allowed": False,
        "requires_sanitization": True,
        "requires_review": True,
    }
    tags = tags or []
    sandbox_id = _sandbox_id(line, strategy_version, data_scope, config_scope)
    base, db_path = _sandbox_paths(sandbox_id, line, root)
    _directory_skeleton(base)
    now = _now()
    baseline = _main_baseline_context()
    strategy_scope = {
        "sandbox_topology": "multi_strategy_single_sandbox" if line == EXPERIMENT_SANDBOX_LINE else "legacy_single_strategy_sandbox",
        "branches": [_strategy_adjustment_scope(item) for item in branch_lines],
        "write_scope": "sandbox_only",
        "production_mutation_allowed": False,
    }
    homepage_context = {
        "active_sandbox_semantics": "analysis_context_only",
        "baseline_source": "main_system",
        "reset_mode": "reset_from_main_baseline",
        "sandbox_topology": "multi_strategy_single_sandbox" if line == EXPERIMENT_SANDBOX_LINE else "legacy_single_strategy_sandbox",
    }
    manifest = {
        "sandbox_id": sandbox_id,
        "strategy_line": line,
        "strategy_version": strategy_version,
        "status": "created",
        "root_path": str(base),
        "db_path": str(db_path),
        "data_scope": data_scope,
        "config_scope": config_scope,
        "source_refs": source_refs,
        "storage_policy": storage_policy,
        "llm_training_policy": llm_training_policy,
        "tags": tags,
        "baseline_context_id": baseline["baseline_context_id"],
        "baseline_parent_type": baseline["baseline_parent_type"],
        "baseline_parent_id": baseline["baseline_parent_id"],
        "derived_from_sandbox_id": None,
        "homepage_context": homepage_context,
        "strategy_adjustment_scope": strategy_scope,
        "source_hash": baseline["source_hash"],
        "write_scope": "sandbox_only",
        "write_guard_status": "enforced",
        "strategy_lines": branch_lines,
        "created_at": now,
        "updated_at": now,
        "schema_version": SCHEMA_VERSION,
    }
    _write_json(base / "manifest.json", manifest)
    for branch_line in branch_lines:
        branch_base = _branch_directory_skeleton(base, branch_line)
        _write_json(
            branch_base / "branch_manifest.json",
            {
                "sandbox_id": sandbox_id,
                "branch_id": _branch_id(sandbox_id, branch_line),
                "strategy_line": branch_line,
                "strategy_version": strategy_version,
                "branch_status": "created",
                "config_scope": config_scope,
                "data_scope": data_scope,
                "evaluator_contract": {
                    "mode": "real_evaluator_adapter_required",
                    "no_shadow_logic": True,
                    "no_future_function": True,
                    "strategy_logic_mutation_allowed": True,
                    "mutation_scope": "sandbox_code_overlay_only",
                    "baseline_mutation_allowed": False,
                },
                "created_at": now,
            },
        )
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        conn.execute(
            """
            INSERT INTO sandbox_manifest(
              sandbox_id, strategy_line, strategy_version, status,
              data_scope_json, config_scope_json, source_refs_json,
              storage_policy_json, llm_training_policy_json, tags_json,
              created_at, updated_at, schema_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sandbox_id,
                line,
                strategy_version,
                "created",
                _json(data_scope),
                _json(config_scope),
                _json(source_refs),
                _json(storage_policy),
                _json(llm_training_policy),
                _json(tags),
                now,
                now,
                SCHEMA_VERSION,
            ),
        )
        for branch_line in branch_lines:
            _seed_branch_rows(conn, sandbox_id, branch_line, strategy_version, data_scope, config_scope, now)
            _seed_contract_rows(conn, sandbox_id, branch_line, strategy_version, data_scope, config_scope, source_refs, now)
        conn.execute(
            """
            INSERT OR REPLACE INTO sandbox_baseline_context(
              baseline_context_id, sandbox_id, baseline_parent_type, baseline_parent_id,
              derived_from_sandbox_id, homepage_context_json, strategy_adjustment_scope_json,
              source_hash_json, write_scope, write_guard_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                baseline["baseline_context_id"],
                sandbox_id,
                baseline["baseline_parent_type"],
                baseline["baseline_parent_id"],
                None,
                _json(homepage_context),
                _json(strategy_scope),
                _json(baseline["source_hash"]),
                "sandbox_only",
                "enforced",
                now,
            ),
        )
        _audit_event(
            conn,
            event_type="sandbox_management_create",
            status="accepted",
            sandbox_id=sandbox_id,
            strategy_line=line,
            idempotency_key=op.get("idempotency_key"),
            request={
                "strategy_line": requested_line,
                "strategy_lines": strategy_lines,
                "strategy_version": strategy_version,
                "data_scope": data_scope,
                "config_scope": config_scope,
            },
            response={"sandbox_id": sandbox_id, "active_changed": False},
            payload={"operation": op, "baseline_context_id": baseline["baseline_context_id"]},
            retryable=False,
            now=now,
        )
        conn.commit()
    with _registry_conn(root) as conn:
        conn.execute(
            """
            INSERT INTO sandbox_registry(
              sandbox_id, strategy_line, strategy_version, status,
              root_path, db_path, data_scope_json, config_scope_json,
              source_refs_json, storage_policy_json, llm_training_policy_json,
              tags_json, best_pf, best_candidate_id, last_job_status,
              created_at, updated_at, schema_version,
              baseline_context_id, baseline_parent_type, baseline_parent_id, derived_from_sandbox_id,
              homepage_context_json, strategy_adjustment_scope_json, source_hash_json,
              write_scope, write_guard_status, deleted_at, delete_mode, delete_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sandbox_id,
                line,
                strategy_version,
                "created",
                str(base),
                str(db_path),
                _json(data_scope),
                _json(config_scope),
                _json(source_refs),
                _json(storage_policy),
                _json(llm_training_policy),
                _json(tags),
                None,
                None,
                "none",
                now,
                now,
                SCHEMA_VERSION,
                baseline["baseline_context_id"],
                baseline["baseline_parent_type"],
                baseline["baseline_parent_id"],
                None,
                _json(homepage_context),
                _json(strategy_scope),
                _json(baseline["source_hash"]),
                "sandbox_only",
                "enforced",
                None,
                None,
                None,
            ),
        )
        conn.commit()
    sandbox = _registry_row(sandbox_id, root)
    return _with_operation_payload(
        {"sandbox": sandbox, "branches": sandbox["branches_summary"]["branches"], "created": True},
        op,
        sandbox_id=sandbox_id,
        active_changed=False,
    )


def _seed_branch_rows(
    conn: sqlite3.Connection,
    sandbox_id: str,
    strategy_line: str,
    strategy_version: str,
    data_scope: dict[str, Any],
    config_scope: dict[str, Any],
    now: str,
) -> None:
    branch_id = _branch_id(sandbox_id, strategy_line)
    conn.execute(
        """
        INSERT OR REPLACE INTO sandbox_strategy_branches(
          branch_id, sandbox_id, strategy_line, strategy_version, branch_status,
          branch_config_scope_json, branch_data_scope_json, branch_metrics_json,
          created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            branch_id,
            sandbox_id,
            strategy_line,
            strategy_version,
            "created",
            _json(config_scope),
            _json(data_scope),
            _json({"job_count": 0, "best_pf": None, "trade_count": 0}),
            now,
            now,
        ),
    )


def _seed_contract_rows(
    conn: sqlite3.Connection,
    sandbox_id: str,
    strategy_line: str,
    strategy_version: str,
    data_scope: dict[str, Any],
    config_scope: dict[str, Any],
    source_refs: list[str],
    now: str,
) -> None:
    branch_id = _branch_id(sandbox_id, strategy_line)
    spec_id = _stable_id("spec", {"sandbox_id": sandbox_id, "strategy_line": strategy_line, "strategy_version": strategy_version})
    conn.execute(
        """
        INSERT OR REPLACE INTO strategy_specs(
          spec_id, sandbox_id, strategy_line, strategy_version,
          config_candidate_json, evaluator_contract_json, status, created_at
          , branch_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            spec_id,
            sandbox_id,
            strategy_line,
            strategy_version,
            _json(config_scope),
            _json(
                {
                    "real_evaluator_adapter": "required",
                    "strategy_logic_mutation_allowed": True,
                    "mutation_scope": "sandbox_code_overlay_only",
                    "baseline_mutation_allowed": False,
                    "execution_environment": "sandbox",
                }
            ),
            "draft",
            now,
            branch_id,
        ),
    )
    snapshot_id = _stable_id("snap", {"sandbox_id": sandbox_id, "strategy_line": strategy_line, "data_scope": data_scope})
    conn.execute(
        """
        INSERT OR REPLACE INTO historical_input_snapshots(
          snapshot_id, sandbox_id, source_refs_json, data_scope_json, quality_json, frozen_at, branch_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            sandbox_id,
            _json(source_refs),
            _json(data_scope),
            _json({"status": "contract_only", "source_time_lte_entry_time_required": True}),
            now,
            branch_id,
        ),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO sandbox_metric_summary(
          sandbox_id, best_pf, best_candidate_id, trade_count, tq_sample_count,
          gate_candidate_count, llm_export_count, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (sandbox_id, None, None, 0, 0, 0, 0, now),
    )


def list_sandboxes_payload(
    *,
    strategy_line: str | None = None,
    status: str | None = None,
    tag: str | None = None,
    limit: int = 100,
    root: Path | None = None,
    include_deleted: bool = False,
) -> dict[str, Any]:
    limit = max(1, min(int(limit or 100), 500))
    clauses: list[str] = []
    params: list[Any] = []
    if status and status != "all":
        clauses.append("status = ?")
        params.append(status)
    if not include_deleted:
        clauses.append("COALESCE(status, '') != 'deleted'")
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _registry_conn(root) as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM sandbox_registry
            {where}
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()
        sandboxes = [_manifest_row(row) for row in rows]
    if strategy_line and strategy_line != "all":
        expected = str(strategy_line)
        sandboxes = [
            item
            for item in sandboxes
            if item.get("strategy_line") == expected
            or expected in ((item.get("branches_summary") or {}).get("strategy_lines") or [])
        ]
    if tag and tag != "all":
        sandboxes = [item for item in sandboxes if tag in (item.get("tags") or [])]
    return {"sandboxes": sandboxes, "count": len(sandboxes), "root": str(sandbox_root(root))}


def _registry_row(sandbox_id: str, root: Path | None = None) -> dict[str, Any]:
    with _registry_conn(root) as conn:
        row = conn.execute("SELECT * FROM sandbox_registry WHERE sandbox_id = ?", (sandbox_id,)).fetchone()
    if not row:
        raise FileNotFoundError(f"sandbox not found: {sandbox_id}")
    return _manifest_row(row)


def _assert_active_sandbox_writable(sandbox_id: str, root: Path | None = None) -> dict[str, Any]:
    row = _registry_row(sandbox_id, root)
    if row.get("status") == "deleted" or row.get("deleted_at"):
        raise ValueError(f"sandbox is deleted: {sandbox_id}")
    if row.get("write_scope") not in {None, "sandbox_only"}:
        raise ValueError(f"unsupported sandbox write_scope: {row.get('write_scope')}")
    return row


def _sandbox_db_from_registry(sandbox_id: str, root: Path | None = None) -> Path:
    row = _registry_row(sandbox_id, root)
    return Path(row["db_path"])


def get_sandbox_payload(sandbox_id: str, *, root: Path | None = None) -> dict[str, Any]:
    row = _registry_row(sandbox_id, root)
    return {"sandbox": row, "summary": summary_payload(sandbox_id, root=root)["summary"]}


def set_active_sandbox_payload(
    sandbox_id: str | None,
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("set_active" if sandbox_id else "clear_active", operation_context)
    _assert_operation_allowed(op)
    now = _now()
    before = active_sandbox_payload(root=root).get("active_sandbox_id")
    if sandbox_id:
        _assert_active_sandbox_writable(sandbox_id, root)
    with _registry_conn(root) as conn:
        conn.execute(
            """
            INSERT INTO active_sandbox_context(context_key, sandbox_id, updated_at)
            VALUES('main', ?, ?)
            ON CONFLICT(context_key) DO UPDATE SET
              sandbox_id = excluded.sandbox_id,
              updated_at = excluded.updated_at
            """,
            (sandbox_id, now),
        )
        conn.commit()
    out = active_sandbox_payload(root=root)
    changed = before != out.get("active_sandbox_id")
    if sandbox_id:
        try:
            with _connect(_sandbox_db_from_registry(sandbox_id, root)) as sandbox_conn:
                ensure_sandbox_tables(sandbox_conn)
                _audit_event(
                    sandbox_conn,
                    event_type="sandbox_management_set_active",
                    status="accepted",
                    sandbox_id=sandbox_id,
                    request={"sandbox_id": sandbox_id},
                    response={"active_sandbox_id": out.get("active_sandbox_id"), "active_changed": changed},
                    payload={"operation": op},
                    retryable=False,
                    now=now,
                )
                sandbox_conn.commit()
        except Exception:
            pass
    return _with_operation_payload(out, op, sandbox_id=sandbox_id, active_changed=changed)


def active_sandbox_payload(*, root: Path | None = None) -> dict[str, Any]:
    with _registry_conn(root) as conn:
        row = conn.execute("SELECT sandbox_id, updated_at FROM active_sandbox_context WHERE context_key = 'main'").fetchone()
    sandbox_id = row["sandbox_id"] if row else None
    active = None
    if sandbox_id:
        try:
            active = _assert_active_sandbox_writable(sandbox_id, root)
        except Exception:
            set_active_sandbox_payload(None, root=root)
            sandbox_id = None
    return {
        "active_sandbox_id": sandbox_id,
        "active": active,
        "updated_at": row["updated_at"] if row else None,
    }


def delete_sandbox_payload(
    sandbox_id: str,
    *,
    mode: str = "soft_delete",
    reason: str = "",
    confirm: bool = False,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("delete", operation_context)
    mode = str(mode or "soft_delete").strip()
    _assert_operation_allowed(op, mode=mode)
    if mode not in {"soft_delete", "purge"}:
        raise ValueError(f"unsupported delete mode: {mode}")
    if mode == "purge" and not confirm:
        raise ValueError("purge_requires_confirm_true")
    row = _registry_row(sandbox_id, root)
    db_path = Path(row["db_path"])
    base = Path(row["root_path"]).resolve()
    allowed_root = sandbox_root(root).resolve()
    path_guard_status = "not_required"
    if mode == "purge":
        if allowed_root != base and allowed_root not in base.parents:
            raise ValueError(f"purge path outside sandbox root: {base}")
        if base.name != sandbox_id:
            raise ValueError(f"purge path sandbox_id mismatch: {base}")
        path_guard_status = "inside_sandbox_root"
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        running = int(conn.execute("SELECT COUNT(*) FROM sandbox_jobs WHERE status IN ('running','queued','stopping')").fetchone()[0])
    if running:
        raise ValueError("sandbox_has_running_job")
    now = _now()
    active_changed = False
    with _connect(db_path) as sandbox_conn:
        ensure_sandbox_tables(sandbox_conn)
        _audit_event(
            sandbox_conn,
            event_type="sandbox_management_delete",
            status="accepted",
            sandbox_id=sandbox_id,
            request={"sandbox_id": sandbox_id, "mode": mode, "reason": reason, "confirm": confirm},
            response={"status": "deleted", "path_guard_status": path_guard_status},
            payload={"operation": op},
            retryable=False,
            now=now,
        )
        sandbox_conn.commit()
    with _registry_conn(root) as conn:
        conn.execute(
            """
            UPDATE sandbox_registry
            SET status='deleted', deleted_at=?, delete_mode=?, delete_reason=?, updated_at=?
            WHERE sandbox_id=?
            """,
            (now, mode, reason, now, sandbox_id),
        )
        active = conn.execute("SELECT sandbox_id FROM active_sandbox_context WHERE context_key='main'").fetchone()
        if active and active["sandbox_id"] == sandbox_id:
            active_changed = True
            conn.execute(
                """
                INSERT INTO active_sandbox_context(context_key, sandbox_id, updated_at)
                VALUES('main', NULL, ?)
                ON CONFLICT(context_key) DO UPDATE SET sandbox_id=NULL, updated_at=excluded.updated_at
                """,
                (now,),
            )
        conn.commit()
    purged = False
    if mode == "purge":
        shutil.rmtree(base, ignore_errors=True)
        purged = True
    return _with_operation_payload(
        {
            "sandbox_id": sandbox_id,
            "status": "deleted",
            "mode": mode,
            "purged": purged,
            "deleted_at": now,
            "active": active_sandbox_payload(root=root),
            "path_guard_status": path_guard_status,
        },
        op,
        active_changed=active_changed,
    )


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    except sqlite3.Error:
        return 0


def _best_gate(conn: sqlite3.Connection) -> tuple[float | None, str | None]:
    rows = conn.execute("SELECT candidate_id, test_metrics_json FROM gate_candidates").fetchall()
    best_pf: float | None = None
    best_id: str | None = None
    for row in rows:
        metrics = _loads(row["test_metrics_json"], {})
        try:
            pf = float(metrics.get("profit_factor"))
        except Exception:
            continue
        if best_pf is None or pf > best_pf:
            best_pf = pf
            best_id = row["candidate_id"]
    return best_pf, best_id


def branches_payload(sandbox_id: str, *, root: Path | None = None) -> dict[str, Any]:
    row = _registry_row(sandbox_id, root)
    branches = _branch_rows(Path(row["db_path"]))
    return {"sandbox_id": sandbox_id, "branches": branches, "count": len(branches)}


def _branch_row(conn: sqlite3.Connection, sandbox_id: str, strategy_line: str) -> sqlite3.Row:
    line = _validate_strategy_line(strategy_line)
    row = conn.execute(
        """
        SELECT * FROM sandbox_strategy_branches
        WHERE sandbox_id=? AND strategy_line=? AND COALESCE(branch_status, '') != 'deleted'
        """,
        (sandbox_id, line),
    ).fetchone()
    if not row:
        raise ValueError(f"strategy branch not in sandbox: {line}")
    return row


def _branch_base_path(sandbox: dict[str, Any], strategy_line: str) -> Path:
    return Path(sandbox["root_path"]) / "branches" / _validate_strategy_line(strategy_line)


def _safe_overlay_relpath(target_relpath: str) -> Path:
    raw = str(target_relpath or "").strip().replace("\\", "/")
    if not raw:
        raise ValueError("target_relpath_required")
    rel = Path(raw)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError("target_relpath_must_stay_inside_overlay")
    forbidden_roots = {"laoma_signal_engine", "config", "DATA", ".git", "web"}
    if rel.parts and rel.parts[0] in forbidden_roots:
        raise ValueError(f"baseline_path_forbidden: {raw}")
    return rel


def _active_overlay_row(
    conn: sqlite3.Connection,
    sandbox_id: str,
    strategy_line: str,
    *,
    code_overlay_id: str | None = None,
) -> sqlite3.Row | None:
    line = _validate_strategy_line(strategy_line)
    if code_overlay_id:
        return conn.execute(
            """
            SELECT * FROM sandbox_code_overlays
            WHERE sandbox_id=? AND strategy_line=? AND code_overlay_id=?
            """,
            (sandbox_id, line, code_overlay_id),
        ).fetchone()
    return conn.execute(
        """
        SELECT * FROM sandbox_code_overlays
        WHERE sandbox_id=? AND strategy_line=? AND status IN ('active','created')
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
        """,
        (sandbox_id, line),
    ).fetchone()


def _latest_runtime_row(
    conn: sqlite3.Connection,
    sandbox_id: str,
    strategy_line: str,
    *,
    runtime_id: str | None = None,
) -> sqlite3.Row | None:
    line = _validate_strategy_line(strategy_line)
    if runtime_id:
        return conn.execute(
            """
            SELECT * FROM sandbox_evaluator_runtime
            WHERE sandbox_id=? AND strategy_line=? AND runtime_id=?
            """,
            (sandbox_id, line, runtime_id),
        ).fetchone()
    return conn.execute(
        """
        SELECT * FROM sandbox_evaluator_runtime
        WHERE sandbox_id=? AND strategy_line=? AND status IN ('built','active','manifest_only_ready','smoke_passed')
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (sandbox_id, line),
    ).fetchone()


def _runtime_lineage(conn: sqlite3.Connection, sandbox_id: str, strategy_line: str, options: dict[str, Any]) -> dict[str, Any]:
    runtime = _latest_runtime_row(conn, sandbox_id, strategy_line, runtime_id=options.get("runtime_id"))
    if runtime:
        return {
            "code_overlay_id": runtime["code_overlay_id"],
            "code_patch_id": runtime["code_patch_id"],
            "runtime_id": runtime["runtime_id"],
            "runtime_status": runtime["status"],
            "runtime_manifest": _loads(runtime["runtime_manifest_json"], {}),
            "code_digest": _loads(runtime["code_digest_json"], {}),
        }
    overlay = _active_overlay_row(conn, sandbox_id, strategy_line, code_overlay_id=options.get("code_overlay_id"))
    if overlay:
        return {
            "code_overlay_id": overlay["code_overlay_id"],
            "code_patch_id": options.get("code_patch_id"),
            "runtime_id": None,
            "runtime_status": "runtime_required",
            "runtime_manifest": {},
            "code_digest": {},
        }
    return {
        "code_overlay_id": None,
        "code_patch_id": None,
        "runtime_id": None,
        "runtime_status": "baseline_runtime",
        "runtime_manifest": {},
        "code_digest": {},
    }


def code_overlay_payload(sandbox_id: str, strategy_line: str, *, root: Path | None = None) -> dict[str, Any]:
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    line = _validate_strategy_line(strategy_line)
    db_path = Path(sandbox["db_path"])
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        _branch_row(conn, sandbox_id, line)
        overlays = conn.execute(
            """
            SELECT * FROM sandbox_code_overlays
            WHERE sandbox_id=? AND strategy_line=?
            ORDER BY updated_at DESC, created_at DESC
            """,
            (sandbox_id, line),
        ).fetchall()
        patches = conn.execute(
            """
            SELECT * FROM sandbox_code_patches
            WHERE sandbox_id=? AND strategy_line=?
            ORDER BY created_at DESC
            """,
            (sandbox_id, line),
        ).fetchall()
        runtimes = conn.execute(
            """
            SELECT * FROM sandbox_evaluator_runtime
            WHERE sandbox_id=? AND strategy_line=?
            ORDER BY created_at DESC
            """,
            (sandbox_id, line),
        ).fetchall()
    return {
        "sandbox_id": sandbox_id,
        "strategy_line": line,
        "overlay_count": len(overlays),
        "patch_count": len(patches),
        "runtime_count": len(runtimes),
        "active_overlay": dict(overlays[0]) | {"base_source_hash": _loads(overlays[0]["base_source_hash_json"], {})} if overlays else None,
        "patches": [
            {
                "code_patch_id": row["code_patch_id"],
                "code_overlay_id": row["code_overlay_id"],
                "patch_type": row["patch_type"],
                "target_relpath": row["target_relpath"],
                "patch": _loads(row["patch_json"], {}),
                "diff_text": row["diff_text"],
                "author": row["author"],
                "status": row["status"],
                "created_at": row["created_at"],
            }
            for row in patches
        ],
        "runtimes": [
            {
                "runtime_id": row["runtime_id"],
                "code_overlay_id": row["code_overlay_id"],
                "code_patch_id": row["code_patch_id"],
                "runtime_manifest": _loads(row["runtime_manifest_json"], {}),
                "import_map": _loads(row["import_map_json"], {}),
                "code_digest": _loads(row["code_digest_json"], {}),
                "status": row["status"],
                "created_at": row["created_at"],
            }
            for row in runtimes
        ],
        "boundary": {
            "write_scope": "sandbox_only",
            "baseline_mutation_allowed": False,
            "runtime_mode": "manifest_overlay_adapter",
        },
    }


def create_code_overlay_payload(
    sandbox_id: str,
    strategy_line: str,
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("code_overlay", operation_context)
    _assert_operation_allowed(op)
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    line = _validate_strategy_line(strategy_line)
    db_path = Path(sandbox["db_path"])
    now = _now()
    branch_base = _branch_directory_skeleton(Path(sandbox["root_path"]), line)
    overlay_path = branch_base / "code_overlay"
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        branch = _branch_row(conn, sandbox_id, line)
        existing = _active_overlay_row(conn, sandbox_id, line)
        if existing:
            return _with_operation_payload(code_overlay_payload(sandbox_id, line, root=root) | {"created": False}, op, sandbox_id=sandbox_id)
        code_overlay_id = _stable_id("ovl", {"sandbox_id": sandbox_id, "branch_id": branch["branch_id"], "strategy_line": line}, 24)
        conn.execute(
            """
            INSERT INTO sandbox_code_overlays(
              code_overlay_id, sandbox_id, branch_id, strategy_line, base_source_hash_json,
              overlay_path, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                code_overlay_id,
                sandbox_id,
                branch["branch_id"],
                line,
                _json(sandbox.get("source_hash") or {}),
                str(overlay_path),
                "active",
                now,
                now,
            ),
        )
        conn.execute(
            """
            UPDATE sandbox_strategy_branches
            SET branch_metrics_json=json_set(COALESCE(branch_metrics_json, '{}'), '$.code_overlay_status', 'active'),
                updated_at=?
            WHERE branch_id=?
            """,
            (now, branch["branch_id"]),
        )
        conn.commit()
    _write_json(
        overlay_path / "metadata" / "overlay_manifest.json",
        {
            "code_overlay_id": code_overlay_id,
            "sandbox_id": sandbox_id,
            "strategy_line": line,
            "branch_id": branch["branch_id"],
            "write_scope": "sandbox_only",
            "baseline_mutation_allowed": False,
            "created_at": now,
        },
    )
    return _with_operation_payload(code_overlay_payload(sandbox_id, line, root=root) | {"created": True}, op, sandbox_id=sandbox_id)


def add_code_patch_payload(
    sandbox_id: str,
    strategy_line: str,
    payload: dict[str, Any],
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("code_overlay", operation_context)
    _assert_operation_allowed(op)
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    line = _validate_strategy_line(strategy_line)
    target_relpath = _safe_overlay_relpath(str(payload.get("target_relpath") or "notes/patch.md"))
    patch_type = str(payload.get("patch_type") or "manifest_note")
    author = str(payload.get("author") or "codex")
    diff_text = str(payload.get("diff_text") or "")
    patch_json = payload.get("patch_json") or {"note": payload.get("note") or "sandbox code overlay patch"}
    content = payload.get("content")
    now = _now()
    create_code_overlay_payload(sandbox_id, line, root=root, operation_context=op)
    db_path = Path(sandbox["db_path"])
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        branch = _branch_row(conn, sandbox_id, line)
        overlay = _active_overlay_row(conn, sandbox_id, line)
        if not overlay:
            raise ValueError("code_overlay_missing")
        code_patch_id = _stable_id(
            "patch",
            {
                "sandbox_id": sandbox_id,
                "strategy_line": line,
                "target_relpath": str(target_relpath),
                "patch_json": patch_json,
                "diff_text": diff_text,
                "now": now,
            },
            24,
        )
        overlay_path = Path(overlay["overlay_path"]).resolve()
        patch_path = overlay_path / "patches" / f"{code_patch_id}.json"
        file_path = (overlay_path / "files" / target_relpath).resolve()
        if overlay_path not in file_path.parents:
            raise ValueError("resolved_target_outside_overlay")
        if content is not None:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(str(content), encoding="utf-8")
        _write_json(
            patch_path,
            {
                "code_patch_id": code_patch_id,
                "code_overlay_id": overlay["code_overlay_id"],
                "sandbox_id": sandbox_id,
                "branch_id": branch["branch_id"],
                "strategy_line": line,
                "patch_type": patch_type,
                "target_relpath": str(target_relpath),
                "patch_json": patch_json,
                "diff_text": diff_text,
                "author": author,
                "status": "active",
                "created_at": now,
            },
        )
        conn.execute(
            """
            INSERT INTO sandbox_code_patches(
              code_patch_id, code_overlay_id, sandbox_id, branch_id, strategy_line,
              patch_type, target_relpath, patch_json, diff_text, author, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                code_patch_id,
                overlay["code_overlay_id"],
                sandbox_id,
                branch["branch_id"],
                line,
                patch_type,
                str(target_relpath),
                _json(patch_json),
                diff_text,
                author,
                "active",
                now,
            ),
        )
        conn.execute("UPDATE sandbox_code_overlays SET updated_at=?, status='active' WHERE code_overlay_id=?", (now, overlay["code_overlay_id"]))
        conn.commit()
    return _with_operation_payload(
        code_overlay_payload(sandbox_id, line, root=root) | {"code_patch_id": code_patch_id, "patch_path": str(patch_path)},
        op,
        sandbox_id=sandbox_id,
    )


def build_runtime_payload(
    sandbox_id: str,
    strategy_line: str,
    payload: dict[str, Any] | None = None,
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("code_overlay", operation_context)
    _assert_operation_allowed(op)
    payload = payload or {}
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    line = _validate_strategy_line(strategy_line)
    create_code_overlay_payload(sandbox_id, line, root=root, operation_context=op)
    db_path = Path(sandbox["db_path"])
    now = _now()
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        branch = _branch_row(conn, sandbox_id, line)
        overlay = _active_overlay_row(conn, sandbox_id, line, code_overlay_id=payload.get("code_overlay_id"))
        if not overlay:
            raise ValueError("code_overlay_missing")
        patch = conn.execute(
            """
            SELECT * FROM sandbox_code_patches
            WHERE sandbox_id=? AND strategy_line=? AND code_overlay_id=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (sandbox_id, line, overlay["code_overlay_id"]),
        ).fetchone()
        overlay_path = Path(overlay["overlay_path"])
        digest_refs: dict[str, Any] = {}
        if overlay_path.exists():
            for path in sorted(item for item in overlay_path.rglob("*") if item.is_file()):
                rel = path.relative_to(overlay_path).as_posix()
                digest_refs[rel] = {"sha256": _file_hash(path), "size": path.stat().st_size}
        code_digest = {
            "baseline_context_id": sandbox.get("baseline_context_id"),
            "base_source_hash": _loads(overlay["base_source_hash_json"], {}),
            "overlay_files": digest_refs,
        }
        runtime_id = _stable_id(
            "rt",
            {
                "sandbox_id": sandbox_id,
                "branch_id": branch["branch_id"],
                "strategy_line": line,
                "overlay": overlay["code_overlay_id"],
                "patch": patch["code_patch_id"] if patch else None,
                "digest": code_digest,
            },
            24,
        )
        runtime_manifest = {
            "runtime_id": runtime_id,
            "sandbox_id": sandbox_id,
            "branch_id": branch["branch_id"],
            "strategy_line": line,
            "runtime_mode": "manifest_overlay_adapter",
            "isolation_level": "sandbox_overlay_no_baseline_write",
            "dynamic_import_enabled": False,
            "baseline_mutation_allowed": False,
            "created_at": now,
        }
        import_map = {
            "baseline_evaluator": "laoma_signal_engine.backtest.p21_real_evaluator.evaluate_signal_offline",
            "overlay_path": overlay["overlay_path"],
            "patch_manifest_only": True,
        }
        runtime_dir = _branch_base_path(sandbox, line) / "runtime"
        _write_json(runtime_dir / "evaluator_manifest.json", runtime_manifest)
        _write_json(runtime_dir / "import_map.json", import_map)
        _write_json(runtime_dir / "code_digest.json", code_digest)
        conn.execute(
            """
            INSERT OR REPLACE INTO sandbox_evaluator_runtime(
              runtime_id, sandbox_id, branch_id, strategy_line, code_overlay_id, code_patch_id,
              runtime_manifest_json, import_map_json, code_digest_json, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                runtime_id,
                sandbox_id,
                branch["branch_id"],
                line,
                overlay["code_overlay_id"],
                patch["code_patch_id"] if patch else None,
                _json(runtime_manifest),
                _json(import_map),
                _json(code_digest),
                "manifest_only_ready",
                now,
            ),
        )
        conn.commit()
    return _with_operation_payload(
        code_overlay_payload(sandbox_id, line, root=root) | {"runtime_id": runtime_id, "runtime_manifest": runtime_manifest},
        op,
        sandbox_id=sandbox_id,
    )


def runtime_smoke_payload(
    sandbox_id: str,
    strategy_line: str,
    payload: dict[str, Any] | None = None,
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("code_overlay", operation_context)
    _assert_operation_allowed(op)
    payload = payload or {}
    built = build_runtime_payload(sandbox_id, strategy_line, payload, root=root, operation_context=op)
    runtime_id = built["runtime_id"]
    result = job_payload(
        sandbox_id,
        "backtest",
        {"strategy_line": _validate_strategy_line(strategy_line), "runtime_id": runtime_id, "symbols": payload.get("symbols") or ["BTCUSDT"], "source": "runtime_smoke"},
        root=root,
    )
    with _connect(_sandbox_db_from_registry(sandbox_id, root)) as conn:
        ensure_sandbox_tables(conn)
        conn.execute("UPDATE sandbox_evaluator_runtime SET status='smoke_passed' WHERE runtime_id=?", (runtime_id,))
        conn.commit()
    return _with_operation_payload({"runtime_id": runtime_id, "smoke": result, "status": "smoke_passed"}, op, sandbox_id=sandbox_id)


def leaderboard_payload(sandbox_id: str, *, root: Path | None = None) -> dict[str, Any]:
    row = _registry_row(sandbox_id, root)
    db_path = Path(row["db_path"])
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        now = _now()
        for line in _sandbox_branch_lines(conn, sandbox_id):
            _update_branch_metrics(conn, sandbox_id, line, now)
        rows = conn.execute(
            """
            SELECT * FROM sandbox_branch_leaderboard
            WHERE sandbox_id=?
            ORDER BY COALESCE(best_pf, -1) DESC, trade_count DESC, strategy_line ASC
            """,
            (sandbox_id,),
        ).fetchall()
        leaderboard = [
            {
                "branch_id": item["branch_id"],
                "strategy_line": item["strategy_line"],
                "best_pf": item["best_pf"],
                "trade_count": item["trade_count"],
                "tq_sample_count": item["tq_sample_count"],
                "gate_candidate_count": item["gate_candidate_count"],
                "best_candidate_id": item["best_candidate_id"],
                "metrics": _loads(item["metrics_json"], {}),
                "updated_at": item["updated_at"],
            }
            for item in rows
        ]
    return {"sandbox_id": sandbox_id, "leaderboard": leaderboard, "count": len(leaderboard)}


def trade_quality_compare_payload(sandbox_id: str, *, root: Path | None = None) -> dict[str, Any]:
    row = _registry_row(sandbox_id, root)
    db_path = Path(row["db_path"])
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        rows = conn.execute(
            """
            SELECT strategy_line, root_cause, COUNT(*) AS sample_count,
                   AVG(net_R) AS avg_R, AVG(MFE_R) AS avg_mfe_R, AVG(MAE_R) AS avg_mae_R,
                   SUM(CASE WHEN net_R > 0 THEN 1 ELSE 0 END) AS wins
            FROM trade_quality_samples
            WHERE sandbox_id=?
            GROUP BY strategy_line, root_cause
            ORDER BY strategy_line ASC, sample_count DESC
            """,
            (sandbox_id,),
        ).fetchall()
    items = [
        {
            "strategy_line": item["strategy_line"],
            "root_cause": item["root_cause"],
            "sample_count": item["sample_count"],
            "win_rate": round(float(item["wins"] or 0) / max(1, int(item["sample_count"] or 0)), 8),
            "avg_R": item["avg_R"],
            "avg_MFE_R": item["avg_mfe_R"],
            "avg_MAE_R": item["avg_mae_R"],
        }
        for item in rows
    ]
    return {"sandbox_id": sandbox_id, "items": items, "count": len(items)}


def gate_compare_payload(sandbox_id: str, *, root: Path | None = None) -> dict[str, Any]:
    row = _registry_row(sandbox_id, root)
    db_path = Path(row["db_path"])
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        rows = conn.execute(
            """
            SELECT candidate_id, strategy_line, rule_json, test_metrics_json, overfit_risk, status, created_at
            FROM gate_candidates
            WHERE sandbox_id=?
            ORDER BY strategy_line ASC, created_at DESC
            """,
            (sandbox_id,),
        ).fetchall()
    items = [
        {
            "candidate_id": item["candidate_id"],
            "strategy_line": item["strategy_line"],
            "rule": _loads(item["rule_json"], {}),
            "test_metrics": _loads(item["test_metrics_json"], {}),
            "overfit_risk": item["overfit_risk"],
            "status": item["status"],
            "created_at": item["created_at"],
        }
        for item in rows
    ]
    return {"sandbox_id": sandbox_id, "items": items, "count": len(items)}


def summary_payload(sandbox_id: str, *, root: Path | None = None) -> dict[str, Any]:
    row = _registry_row(sandbox_id, root)
    db_path = Path(row["db_path"])
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        best_pf, best_candidate_id = _best_gate(conn)
        counts = {
            "sandbox_baseline_context": _table_count(conn, "sandbox_baseline_context"),
            "sandbox_strategy_branches": _table_count(conn, "sandbox_strategy_branches"),
            "sandbox_parameter_sets": _table_count(conn, "sandbox_parameter_sets"),
            "strategy_specs": _table_count(conn, "strategy_specs"),
            "historical_input_snapshots": _table_count(conn, "historical_input_snapshots"),
            "evaluator_runs": _table_count(conn, "evaluator_runs"),
            "sandbox_orders": _table_count(conn, "sandbox_orders"),
            "fill_model_runs": _table_count(conn, "fill_model_runs"),
            "trade_quality_samples": _table_count(conn, "trade_quality_samples"),
            "gate_candidates": _table_count(conn, "gate_candidates"),
            "holdout_validations": _table_count(conn, "holdout_validations"),
            "config_candidates": _table_count(conn, "config_candidates"),
            "trade_plan_candidates": _table_count(conn, "trade_plan_candidates"),
            "paper_shadow_results": _table_count(conn, "paper_shadow_results"),
            "llm_dataset_exports": _table_count(conn, "llm_dataset_exports"),
            "sandbox_code_overlays": _table_count(conn, "sandbox_code_overlays"),
            "sandbox_code_patches": _table_count(conn, "sandbox_code_patches"),
            "sandbox_evaluator_runtime": _table_count(conn, "sandbox_evaluator_runtime"),
            "sandbox_jobs": _table_count(conn, "sandbox_jobs"),
            "external_full_backtest_runs": _table_count(conn, "external_full_backtest_runs"),
            "external_full_backtest_batches": _table_count(conn, "external_full_backtest_batches"),
            "external_full_backtest_events": _table_count(conn, "external_full_backtest_events"),
            "external_trade_candidates": _table_count(conn, "external_trade_candidates"),
            "external_gate_actions": _table_count(conn, "external_gate_actions"),
            "external_gate_action_events": _table_count(conn, "external_gate_action_events"),
            "external_gated_runs": _table_count(conn, "external_gated_runs"),
            "external_gated_orders": _table_count(conn, "external_gated_orders"),
            "external_gated_results": _table_count(conn, "external_gated_results"),
            "external_gated_performance": _table_count(conn, "external_gated_performance"),
            "external_integration_audit_events": _table_count(conn, "external_integration_audit_events"),
            "external_connector_smoke_runs": _table_count(conn, "external_connector_smoke_runs"),
            "external_contract_versions": _table_count(conn, "external_contract_versions"),
        }
        branch_lines = _sandbox_branch_lines(conn, sandbox_id)
        now = _now()
        branch_metrics = [_update_branch_metrics(conn, sandbox_id, line, now) | {"strategy_line": line, "branch_id": _branch_id(sandbox_id, line)} for line in branch_lines]
        job = conn.execute(
            "SELECT job_id, job_type, status, progress_json, updated_at FROM sandbox_jobs ORDER BY updated_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
        conn.execute(
            """
            INSERT INTO sandbox_metric_summary(
              sandbox_id, best_pf, best_candidate_id, trade_count, tq_sample_count,
              gate_candidate_count, llm_export_count, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sandbox_id) DO UPDATE SET
              best_pf = excluded.best_pf,
              best_candidate_id = excluded.best_candidate_id,
              trade_count = excluded.trade_count,
              tq_sample_count = excluded.tq_sample_count,
              gate_candidate_count = excluded.gate_candidate_count,
              llm_export_count = excluded.llm_export_count,
              updated_at = excluded.updated_at
            """,
            (
                sandbox_id,
                best_pf,
                best_candidate_id,
                counts["trade_quality_samples"],
                counts["trade_quality_samples"],
                counts["gate_candidates"],
                counts["llm_dataset_exports"],
                now,
            ),
        )
        conn.commit()
    storage_bytes = _directory_size(Path(row["root_path"]))
    with _registry_conn(root) as reg:
        reg.execute(
            """
            UPDATE sandbox_registry
            SET best_pf = ?, best_candidate_id = ?, last_job_status = ?, updated_at = ?
            WHERE sandbox_id = ?
            """,
            (best_pf, best_candidate_id, job["status"] if job else "none", now, sandbox_id),
        )
        reg.commit()
    return {
        "summary": {
            "sandbox_id": sandbox_id,
            "strategy_line": row["strategy_line"],
            "status": row["status"],
            "best_pf": best_pf,
            "best_candidate_id": best_candidate_id,
            "branches": branch_metrics,
            "last_job": dict(job) | {"progress": _loads(job["progress_json"], {})} if job else None,
            "counts": counts,
            "storage_bytes": storage_bytes,
            "updated_at": now,
        }
    }


def _directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for file in path.rglob("*"):
        if file.is_file():
            try:
                total += file.stat().st_size
            except OSError:
                pass
    return total


def db_health_payload(sandbox_id: str, *, root: Path | None = None) -> dict[str, Any]:
    row = _registry_row(sandbox_id, root)
    db_path = Path(row["db_path"])
    checks: dict[str, Any] = {"exists": db_path.exists(), "db_path": str(db_path), "schema_version": SCHEMA_VERSION}
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        checks["integrity_check"] = integrity
        checks["tables"] = [
            item[0]
            for item in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
        ]
    return {"sandbox_id": sandbox_id, "health": checks}


def _normalize_symbols(symbols: Any) -> list[str]:
    if not isinstance(symbols, list):
        return []
    seen: list[str] = []
    for item in symbols:
        value = str(item or "").strip().upper()
        if value and value not in seen:
            seen.append(value)
    return seen


def _candidate_universe_symbols() -> tuple[list[str], dict[str, Any]]:
    path = Path.cwd() / "DATA" / "universe" / "CANDIDATE_UNIVERSE.json"
    if not path.exists():
        return [], {"source": "candidate_universe_file", "path": str(path), "exists": False}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [], {"source": "candidate_universe_file", "path": str(path), "exists": True, "error": str(exc)}
    pairs = raw.get("pairs") if isinstance(raw, dict) else []
    symbols: list[str] = []
    if isinstance(pairs, list):
        for row in pairs:
            if not isinstance(row, dict):
                continue
            symbol = row.get("futures_symbol") or row.get("symbol_safe_id") or row.get("spot_symbol")
            if symbol:
                symbols.append(str(symbol))
    return _normalize_symbols(symbols), {
        "source": "candidate_universe_file",
        "path": str(path),
        "exists": True,
        "generated_at": raw.get("generated_at") if isinstance(raw, dict) else None,
        "expires_at": raw.get("expires_at") if isinstance(raw, dict) else None,
        "status": raw.get("status") if isinstance(raw, dict) else None,
        "count": raw.get("count") if isinstance(raw, dict) else None,
    }


def _sandbox_universe_symbols(strategy_line: str, sandbox_id: str | None, root: Path | None) -> tuple[list[str], dict[str, Any]]:
    row: dict[str, Any] | None = None
    if sandbox_id:
        row = _registry_row(sandbox_id, root)
    else:
        active = active_sandbox_payload(root=root).get("active")
        if isinstance(active, dict):
            row = active
    if not row:
        return [], {"source": "active_sandbox", "exists": False}
    data_scope = row.get("data_scope") or {}
    sandbox_symbols = _normalize_symbols(data_scope.get("symbols"))
    branches = ((row.get("branches_summary") or {}).get("strategy_lines") or [])
    requested = _strategy_line_alias(strategy_line) if strategy_line and strategy_line != "all" else "all"
    line_match = requested == "all" or requested == row.get("strategy_line") or requested in branches
    if sandbox_symbols and line_match:
        return sandbox_symbols, {
            "source": "sandbox_data_scope",
            "sandbox_id": row.get("sandbox_id"),
            "strategy_line": row.get("strategy_line"),
            "branch_strategy_lines": branches,
            "exists": True,
        }
    return [], {
        "source": "sandbox_data_scope",
        "sandbox_id": row.get("sandbox_id"),
        "exists": True,
        "empty_or_line_mismatch": True,
    }

def universe_payload(
    strategy_line: str = "all",
    *,
    sandbox_id: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    requested_line = "all" if strategy_line in {None, "", "all"} else _validate_strategy_line(str(strategy_line))
    symbols, source = _sandbox_universe_symbols(requested_line, sandbox_id, root)
    if not symbols:
        symbols, source = _candidate_universe_symbols()
    universe_hash = _hash_payload({"strategy_line": requested_line, "symbols": symbols, "source": source})
    return {
        "strategy_line": requested_line,
        "sandbox_id": sandbox_id,
        "symbols": symbols,
        "symbol_count": len(symbols),
        "time_start": None,
        "time_end": None,
        "timeframe": "1m",
        "bar_source": "historical_kline_cache_or_binance",
        "data_quality_summary": {
            "status": "ok" if symbols else "empty",
            "source": source,
            "requested_symbol_count": len(symbols),
            "available_symbol_count": len(symbols),
            "missing_symbol_count": 0,
            "fallback_universe": False,
        },
        "universe_hash": universe_hash,
    }


def _batch_symbols(symbols: list[str], batch_size: int) -> list[list[str]]:
    size = max(1, int(batch_size or 25))
    return [symbols[idx : idx + size] for idx in range(0, len(symbols), size)]


def _external_event(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    sandbox_id: str,
    event_type: str,
    status: str,
    payload: dict[str, Any],
    now: str,
) -> None:
    conn.execute(
        """
        INSERT INTO external_full_backtest_events(
          event_id, run_id, sandbox_id, event_type, status, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _stable_id("efbevt", {"run_id": run_id, "event_type": event_type, "now": now, "payload": payload}, 22),
            run_id,
            sandbox_id,
            event_type,
            status,
            _json(payload),
            now,
        ),
    )


def _audit_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    status: str,
    sandbox_id: str | None = None,
    strategy_line: str | None = None,
    run_id: str | None = None,
    candidate_id: str | None = None,
    gate_action_id: str | None = None,
    gated_run_id: str | None = None,
    idempotency_key: str | None = None,
    request: dict[str, Any] | None = None,
    response: dict[str, Any] | None = None,
    error_code: str | None = None,
    retryable: bool = True,
    payload: dict[str, Any] | None = None,
    now: str | None = None,
) -> None:
    stamp = now or _now()
    request_hash = _hash_payload(request, 24) if request is not None else None
    response_hash = _hash_payload(response, 24) if response is not None else None
    conn.execute(
        """
        INSERT INTO external_integration_audit_events(
          event_id, event_type, sandbox_id, strategy_line, run_id, candidate_id,
          gate_action_id, gated_run_id, idempotency_key, request_hash, response_hash,
          status, error_code, retryable, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _stable_id(
                "eiaevt",
                {
                    "event_type": event_type,
                    "sandbox_id": sandbox_id,
                    "run_id": run_id,
                    "candidate_id": candidate_id,
                    "gate_action_id": gate_action_id,
                    "gated_run_id": gated_run_id,
                    "stamp": stamp,
                    "payload": payload or {},
                },
                24,
            ),
            event_type,
            sandbox_id,
            strategy_line,
            run_id,
            candidate_id,
            gate_action_id,
            gated_run_id,
            idempotency_key,
            request_hash,
            response_hash,
            status,
            error_code,
            1 if retryable else 0,
            _json(payload or {}),
            stamp,
        ),
    )


def _external_run_row(conn: sqlite3.Connection, run_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM external_full_backtest_runs WHERE run_id = ?", (run_id,)).fetchone()
    if not row:
        raise FileNotFoundError(f"external full backtest run not found: {run_id}")
    return row


def _external_run_payload(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    run_id = row["run_id"]
    batches = [
        {
            "batch_id": item["batch_id"],
            "batch_index": item["batch_index"],
            "status": item["status"],
            "symbols": _loads(item["symbols_json"], []),
            "symbol_count": item["symbol_count"],
            "completed_symbol_count": item["completed_symbol_count"],
            "failed_symbol_count": item["failed_symbol_count"],
            "progress": _loads(item["progress_json"], {}),
            "error_code": item["error_code"],
            "error_message": item["error_message"],
            "retryable": bool(item["retryable"]),
            "created_at": item["created_at"],
            "updated_at": item["updated_at"],
            "finished_at": item["finished_at"],
        }
        for item in conn.execute(
            """
            SELECT * FROM external_full_backtest_batches
            WHERE run_id = ?
            ORDER BY batch_index ASC
            """,
            (run_id,),
        ).fetchall()
    ]
    symbols = _loads(row["symbols_json"], [])
    completed_symbol_count = sum(int(item["completed_symbol_count"] or 0) for item in batches)
    failed_symbol_count = sum(int(item["failed_symbol_count"] or 0) for item in batches)
    progress = _loads(row["progress_json"], {})
    coverage = {
        "requested_symbols": len(symbols),
        "completed_symbols": completed_symbol_count,
        "failed_symbols": failed_symbol_count,
        "pending_symbols": max(0, len(symbols) - completed_symbol_count - failed_symbol_count),
        "time_start": row["time_start"],
        "time_end": row["time_end"],
    }
    return {
        "sandbox_id": row["sandbox_id"],
        "strategy_line": row["strategy_line"],
        "external_full_backtest_run_id": run_id,
        "run_id": run_id,
        "scope_hash": row["scope_hash"],
        "universe_hash": row["universe_hash"],
        "status": row["status"],
        "symbols": symbols,
        "symbol_count": len(symbols),
        "time_start": row["time_start"],
        "time_end": row["time_end"],
        "timeframe": row["timeframe"],
        "bar_source": row["bar_source"],
        "data_quality_summary": _loads(row["data_quality_summary_json"], {}),
        "resource_budget": _loads(row["resource_budget_json"], {}),
        "progress": progress,
        "coverage": coverage,
        "expected_batches": row["expected_batches"],
        "completed_batches": row["completed_batches"],
        "failed_batches": row["failed_batches"],
        "batches": batches,
        "error_code": row["error_code"],
        "error_message": row["error_message"],
        "retryable": bool(row["retryable"]),
        "resume_token": row["resume_token"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "finished_at": row["finished_at"],
    }


def create_full_backtest_run_payload(
    sandbox_id: str,
    payload: dict[str, Any] | None = None,
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    body = payload or {}
    op = _operation_context("full_backtest_run", operation_context or body)
    _assert_operation_allowed(op)
    strategy_line = body.get("strategy_line") or sandbox.get("strategy_line") or "all"
    strategy_line = "all" if strategy_line in {"all", EXPERIMENT_SANDBOX_LINE} else _validate_strategy_line(str(strategy_line))
    universe = universe_payload(strategy_line, sandbox_id=sandbox_id, root=root)
    requested_symbols = _normalize_symbols(body.get("symbols"))
    symbols = [item for item in requested_symbols if item in set(universe["symbols"])] if requested_symbols else universe["symbols"]
    time_start = body.get("time_start")
    time_end = body.get("time_end")
    timeframe = str(body.get("timeframe") or universe.get("timeframe") or "1m")
    bar_source = str(body.get("bar_source") or universe.get("bar_source") or "historical_kline_cache_or_binance")
    resource_budget = body.get("resource_budget") if isinstance(body.get("resource_budget"), dict) else {}
    batch_size = int(body.get("batch_size") or resource_budget.get("symbol_batch_size") or 25)
    batches = _batch_symbols(symbols, batch_size)
    scope = {
        "sandbox_id": sandbox_id,
        "strategy_line": strategy_line,
        "symbols": symbols,
        "time_start": time_start,
        "time_end": time_end,
        "timeframe": timeframe,
        "bar_source": bar_source,
        "resource_budget": resource_budget,
    }
    scope_hash = _hash_payload(scope)
    universe_hash = _hash_payload({"strategy_line": strategy_line, "symbols": symbols, "source_hash": universe["universe_hash"]})
    idempotency_key = str(body.get("idempotency_key") or "").strip() or None
    run_id = _stable_id("efbrun", {"sandbox_id": sandbox_id, "idempotency_key": idempotency_key}, 22) if idempotency_key else _stable_id("efbrun", {**scope, "nonce": uuid.uuid4().hex}, 22)
    resume_token = _stable_id("efbresume", {"run_id": run_id, "scope_hash": scope_hash}, 22)
    now = _now()
    db_path = Path(sandbox["db_path"])
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        if idempotency_key:
            existing = conn.execute(
                """
                SELECT * FROM external_full_backtest_runs
                WHERE sandbox_id = ? AND idempotency_key = ?
                """,
                (sandbox_id, idempotency_key),
            ).fetchone()
            if existing:
                out = _external_run_payload(conn, existing)
                out["idempotent_replay"] = True
                out["training_dataset"] = sync_sandbox_job_result(
                    _p29_project_root_from_sandbox_root(root),
                    sandbox_db_path=db_path,
                    sandbox_id=sandbox_id,
                    job_id=str(existing["run_id"]),
                    job_type="external_full_backtest",
                )
                return _with_operation_payload(out, op, sandbox_id=sandbox_id)
        progress = {
            "state": "manifest_ready",
            "percent": 0.0,
            "runner_attached": False,
            "message": "Manifest created; worker attachment is outside STEP27.1.",
        }
        data_quality = dict(universe.get("data_quality_summary") or {})
        data_quality.update(
            {
                "requested_symbol_count": len(symbols),
                "expected_batches": len(batches),
                "scope_hash": scope_hash,
            }
        )
        conn.execute(
            """
            INSERT INTO external_full_backtest_runs(
              run_id, sandbox_id, strategy_line, idempotency_key, scope_hash, universe_hash,
              status, time_start, time_end, timeframe, bar_source, symbols_json,
              data_quality_summary_json, resource_budget_json, progress_json,
              completed_batches, failed_batches, expected_batches, error_code, error_message,
              retryable, resume_token, created_at, updated_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, NULL, NULL, 1, ?, ?, ?, NULL)
            """,
            (
                run_id,
                sandbox_id,
                strategy_line,
                idempotency_key,
                scope_hash,
                universe_hash,
                "manifest_ready",
                time_start,
                time_end,
                timeframe,
                bar_source,
                _json(symbols),
                _json(data_quality),
                _json(resource_budget | {"symbol_batch_size": batch_size}),
                _json(progress),
                len(batches),
                resume_token,
                now,
                now,
            ),
        )
        for idx, batch_symbols in enumerate(batches):
            batch_id = _stable_id("efbbat", {"run_id": run_id, "batch_index": idx, "symbols": batch_symbols}, 22)
            conn.execute(
                """
                INSERT INTO external_full_backtest_batches(
                  batch_id, run_id, sandbox_id, strategy_line, batch_index, status,
                  symbols_json, symbol_count, completed_symbol_count, failed_symbol_count,
                  progress_json, error_code, error_message, retryable, created_at, updated_at, finished_at
                ) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, 0, 0, ?, NULL, NULL, 1, ?, ?, NULL)
                """,
                (
                    batch_id,
                    run_id,
                    sandbox_id,
                    strategy_line,
                    idx,
                    _json(batch_symbols),
                    len(batch_symbols),
                    _json({"percent": 0.0, "runner_attached": False}),
                    now,
                    now,
                ),
            )
        _external_event(
            conn,
            run_id=run_id,
            sandbox_id=sandbox_id,
            event_type="manifest_created",
            status="manifest_ready",
            payload={"scope_hash": scope_hash, "universe_hash": universe_hash, "expected_batches": len(batches)},
            now=now,
        )
        _audit_event(
            conn,
            event_type="full_backtest_manifest_created",
            status="manifest_ready",
            sandbox_id=sandbox_id,
            strategy_line=strategy_line,
            run_id=run_id,
            idempotency_key=idempotency_key,
            request=body,
            response={"run_id": run_id, "expected_batches": len(batches), "scope_hash": scope_hash},
            payload={"scope_hash": scope_hash, "universe_hash": universe_hash, "operation": op},
            now=now,
        )
        conn.commit()
        out = _external_run_payload(conn, _external_run_row(conn, run_id))
    out["idempotent_replay"] = False
    out["training_dataset"] = sync_sandbox_job_result(
        _p29_project_root_from_sandbox_root(root),
        sandbox_db_path=db_path,
        sandbox_id=sandbox_id,
        job_id=run_id,
        job_type="external_full_backtest",
    )
    return _with_operation_payload(out, op, sandbox_id=sandbox_id)


def full_backtest_run_payload(sandbox_id: str, run_id: str, *, root: Path | None = None) -> dict[str, Any]:
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    with _connect(Path(sandbox["db_path"])) as conn:
        ensure_sandbox_tables(conn)
        return _external_run_payload(conn, _external_run_row(conn, run_id))


def cancel_full_backtest_run_payload(
    sandbox_id: str,
    run_id: str,
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("full_backtest_cancel", operation_context)
    _assert_operation_allowed(op)
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    now = _now()
    with _connect(Path(sandbox["db_path"])) as conn:
        ensure_sandbox_tables(conn)
        row = _external_run_row(conn, run_id)
        if row["status"] not in {"completed", "canceled"}:
            progress = _loads(row["progress_json"], {})
            progress.update({"state": "canceled", "percent": progress.get("percent", 0.0)})
            conn.execute(
                """
                UPDATE external_full_backtest_runs
                SET status='canceled', progress_json=?, updated_at=?, finished_at=COALESCE(finished_at, ?)
                WHERE run_id=?
                """,
                (_json(progress), now, now, run_id),
            )
            conn.execute(
                """
                UPDATE external_full_backtest_batches
                SET status='canceled', updated_at=?, finished_at=COALESCE(finished_at, ?)
                WHERE run_id=? AND status IN ('queued','running','manifest_ready')
                """,
                (now, now, run_id),
            )
            _external_event(conn, run_id=run_id, sandbox_id=sandbox_id, event_type="cancel_requested", status="canceled", payload={}, now=now)
            _audit_event(
                conn,
                event_type="full_backtest_cancel_requested",
                status="canceled",
                sandbox_id=sandbox_id,
                strategy_line=row["strategy_line"],
                run_id=run_id,
                payload={"operation": op},
                now=now,
            )
            conn.commit()
        return _with_operation_payload(_external_run_payload(conn, _external_run_row(conn, run_id)), op, sandbox_id=sandbox_id)


def resume_full_backtest_run_payload(
    sandbox_id: str,
    run_id: str,
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("full_backtest_resume", operation_context)
    _assert_operation_allowed(op)
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    now = _now()
    with _connect(Path(sandbox["db_path"])) as conn:
        ensure_sandbox_tables(conn)
        row = _external_run_row(conn, run_id)
        if row["status"] in {"canceled", "failed"}:
            progress = _loads(row["progress_json"], {})
            progress.update(
                {
                    "state": "manifest_ready",
                    "runner_attached": False,
                    "message": "Resume accepted; worker attachment is outside STEP27.1.",
                }
            )
            conn.execute(
                """
                UPDATE external_full_backtest_runs
                SET status='manifest_ready', progress_json=?, updated_at=?, finished_at=NULL,
                    error_code=NULL, error_message=NULL, retryable=1
                WHERE run_id=?
                """,
                (_json(progress), now, run_id),
            )
            conn.execute(
                """
                UPDATE external_full_backtest_batches
                SET status='queued', updated_at=?, finished_at=NULL, error_code=NULL, error_message=NULL, retryable=1
                WHERE run_id=? AND status IN ('canceled','failed')
                """,
                (now, run_id),
            )
            _external_event(conn, run_id=run_id, sandbox_id=sandbox_id, event_type="resume_requested", status="manifest_ready", payload={}, now=now)
            _audit_event(
                conn,
                event_type="full_backtest_resume_requested",
                status="manifest_ready",
                sandbox_id=sandbox_id,
                strategy_line=row["strategy_line"],
                run_id=run_id,
                payload={"operation": op},
                now=now,
            )
            conn.commit()
        return _with_operation_payload(_external_run_payload(conn, _external_run_row(conn, run_id)), op, sandbox_id=sandbox_id)


LEAKAGE_DENYLIST = {
    "pnl",
    "return",
    "net_r",
    "mfe_r",
    "mae_r",
    "exit_price",
    "exit_time",
    "exit_reason",
    "win_loss",
    "future_outcome_labels",
    "post_trade_metric",
    "realized_slippage",
    "realized_fee",
    "future_bar_high",
    "future_bar_low",
}
GATE_DECISIONS = {"allow", "block", "reduce_size", "review"}


def _iso_from_ms(value: Any) -> str:
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return _now()
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _strip_internal_feature_payload(features: dict[str, Any]) -> dict[str, Any]:
    clean = dict(features)
    clean.pop("_sandbox_rows", None)
    return clean


def _leakage_paths(value: Any, *, prefix: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            normalized = str(key).strip().lower()
            if normalized in LEAKAGE_DENYLIST:
                found.append(path)
            found.extend(_leakage_paths(child, prefix=path))
    elif isinstance(value, list):
        for idx, child in enumerate(value):
            found.extend(_leakage_paths(child, prefix=f"{prefix}[{idx}]"))
    return found


def _assert_no_feature_leakage(features: dict[str, Any]) -> dict[str, Any]:
    paths = _leakage_paths(features)
    if paths:
        raise ValueError(f"feature_leakage_detected: {','.join(paths[:20])}")
    return {"status": "pass", "denylist": sorted(LEAKAGE_DENYLIST), "matched_paths": []}


def _candidate_from_order(row: sqlite3.Row, sandbox: dict[str, Any], source_mode: str, now: str) -> dict[str, Any]:
    features = _strip_internal_feature_payload(_loads(row["features_json"], {}))
    leakage = _assert_no_feature_leakage(features)
    plan = _loads(row["trade_plan_payload_json"], {})
    run_id = str(row["evaluator_run_id"] or row["fill_run_id"] or "sandbox_candidate_run")
    candidate_id = _stable_id(
        "tcand",
        {
            "sandbox_id": row["sandbox_id"],
            "strategy_line": row["strategy_line"],
            "run_id": run_id,
            "source_order_id": row["order_id"],
        },
        24,
    )
    intended_size = float(plan.get("quantity") or plan.get("intended_size") or plan.get("notional_usdt") or 1.0)
    price_context = {
        "signal_time_ms": row["signal_time_ms"],
        "entry_time_ms": row["entry_time_ms"],
        "entry_price_hint": row["entry_price"],
        "limit_price": row["entry_price"],
        "stop_loss": row["stop_loss"],
        "take_profit": row["take_profit"],
        "score": row["score"],
    }
    risk_context = {
        "planned_rr": row["planned_rr"],
        "intended_size": intended_size,
        "risk_basis": "sandbox_trade_plan_candidate",
        "result_fields_excluded_from_candidate": True,
    }
    context_refs = {
        "source_order_id": row["order_id"],
        "evaluator_run_id": row["evaluator_run_id"],
        "fill_run_id": row["fill_run_id"],
        "source_mode": source_mode,
        "sandbox_db_owner": "abnormal_enchanced",
        "external_direct_sqlite_write_allowed": False,
    }
    return {
        "candidate_id": candidate_id,
        "run_id": run_id,
        "sandbox_id": row["sandbox_id"],
        "branch_id": row["branch_id"],
        "strategy_line": row["strategy_line"],
        "strategy_version": sandbox.get("strategy_version"),
        "source_mode": source_mode,
        "source_order_id": row["order_id"],
        "symbol": row["symbol"],
        "side": row["side"],
        "decision_time": _iso_from_ms(row["signal_time_ms"]),
        "intended_size": intended_size,
        "order_type": str(plan.get("order_type") or "market_or_limit_hint"),
        "entry_price_hint": row["entry_price"],
        "limit_price": row["entry_price"],
        "stop_loss": row["stop_loss"],
        "take_profit": row["take_profit"],
        "planned_rr": row["planned_rr"],
        "price_context": price_context,
        "risk_context": risk_context,
        "decision_time_features": features,
        "context_refs": context_refs,
        "feature_schema_version": "step27.2-decision-time-v1",
        "leakage_status": "pass",
        "leakage_report": leakage,
        "created_at": now,
        "updated_at": now,
    }


def _upsert_trade_candidate(conn: sqlite3.Connection, candidate: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO external_trade_candidates(
          candidate_id, run_id, sandbox_id, branch_id, strategy_line, strategy_version,
          source_mode, source_order_id, symbol, side, decision_time, intended_size,
          order_type, entry_price_hint, limit_price, stop_loss, take_profit, planned_rr,
          price_context_json, risk_context_json, decision_time_features_json, context_refs_json,
          feature_schema_version, leakage_status, leakage_report_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id) DO UPDATE SET
          updated_at=excluded.updated_at,
          decision_time_features_json=excluded.decision_time_features_json,
          leakage_status=excluded.leakage_status,
          leakage_report_json=excluded.leakage_report_json
        """,
        (
            candidate["candidate_id"],
            candidate["run_id"],
            candidate["sandbox_id"],
            candidate["branch_id"],
            candidate["strategy_line"],
            candidate["strategy_version"],
            candidate["source_mode"],
            candidate["source_order_id"],
            candidate["symbol"],
            candidate["side"],
            candidate["decision_time"],
            candidate["intended_size"],
            candidate["order_type"],
            candidate["entry_price_hint"],
            candidate["limit_price"],
            candidate["stop_loss"],
            candidate["take_profit"],
            candidate["planned_rr"],
            _json(candidate["price_context"]),
            _json(candidate["risk_context"]),
            _json(candidate["decision_time_features"]),
            _json(candidate["context_refs"]),
            candidate["feature_schema_version"],
            candidate["leakage_status"],
            _json(candidate["leakage_report"]),
            candidate["created_at"],
            candidate["updated_at"],
        ),
    )


def _candidate_row_payload(row: sqlite3.Row, include_features: bool = True) -> dict[str, Any]:
    payload = {
        "candidate_id": row["candidate_id"],
        "run_id": row["run_id"],
        "sandbox_id": row["sandbox_id"],
        "strategy_line": row["strategy_line"],
        "strategy_version": row["strategy_version"],
        "source_mode": row["source_mode"],
        "symbol": row["symbol"],
        "side": row["side"],
        "decision_time": row["decision_time"],
        "intended_size": row["intended_size"],
        "order_type": row["order_type"],
        "entry_price_hint": row["entry_price_hint"],
        "limit_price": row["limit_price"],
        "stop_loss": row["stop_loss"],
        "take_profit": row["take_profit"],
        "planned_rr": row["planned_rr"],
        "price_context": _loads(row["price_context_json"], {}),
        "risk_context": _loads(row["risk_context_json"], {}),
        "context_refs": _loads(row["context_refs_json"], {}),
        "feature_schema_version": row["feature_schema_version"],
        "leakage_status": row["leakage_status"],
        "leakage_report": _loads(row["leakage_report_json"], {}),
        "created_at": row["created_at"],
    }
    if include_features:
        payload["decision_time_features"] = _loads(row["decision_time_features_json"], {})
    return payload


def trade_candidates_payload(
    sandbox_id: str,
    strategy_line: str,
    *,
    run_id: str | None = None,
    source_mode: str = "backtest",
    symbol: str | None = None,
    cursor: str | None = None,
    limit: int = 100,
    since: str | None = None,
    include_features: bool = True,
    root: Path | None = None,
) -> dict[str, Any]:
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    line = _validate_strategy_line(strategy_line)
    limit = max(1, min(int(limit or 100), 500))
    now = _now()
    db_path = Path(sandbox["db_path"])
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        clauses = ["sandbox_id=?", "strategy_line=?"]
        params: list[Any] = [sandbox_id, line]
        if run_id:
            clauses.append("evaluator_run_id=?")
            params.append(run_id)
        if symbol:
            clauses.append("symbol=?")
            params.append(str(symbol).upper())
        order_rows = conn.execute(
            f"""
            SELECT * FROM sandbox_orders
            WHERE {' AND '.join(clauses)}
            ORDER BY signal_time_ms ASC, order_id ASC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()
        for order in order_rows:
            candidate = _candidate_from_order(order, sandbox, source_mode, now)
            _upsert_trade_candidate(conn, candidate)
            _audit_event(
                conn,
                event_type="trade_candidate_exported",
                status="exported",
                sandbox_id=sandbox_id,
                strategy_line=line,
                run_id=candidate["run_id"],
                candidate_id=candidate["candidate_id"],
                response={"candidate_id": candidate["candidate_id"], "leakage_status": candidate["leakage_status"]},
                payload={"source_order_id": candidate["source_order_id"], "source_mode": source_mode},
                now=now,
            )
        conn.commit()

        clauses = ["sandbox_id=?", "strategy_line=?", "source_mode=?"]
        params = [sandbox_id, line, source_mode]
        if run_id:
            clauses.append("run_id=?")
            params.append(run_id)
        if symbol:
            clauses.append("symbol=?")
            params.append(str(symbol).upper())
        if since:
            clauses.append("created_at>=?")
            params.append(since)
        if cursor:
            clauses.append("candidate_id>?")
            params.append(cursor)
        rows = conn.execute(
            f"""
            SELECT * FROM external_trade_candidates
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at ASC, candidate_id ASC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()
    candidates = [_candidate_row_payload(row, include_features=include_features) for row in rows]
    return {
        "sandbox_id": sandbox_id,
        "strategy_line": line,
        "run_id": run_id,
        "source_mode": source_mode,
        "count": len(candidates),
        "candidates": candidates,
        "next_cursor": candidates[-1]["candidate_id"] if len(candidates) == limit else None,
        "leakage_guard": {"status": "pass", "denylist": sorted(LEAKAGE_DENYLIST)},
    }


def _gate_action_event(
    conn: sqlite3.Connection,
    *,
    gate_action_id: str | None,
    run_id: str | None,
    candidate_id: str | None,
    sandbox_id: str,
    event_type: str,
    status: str,
    payload: dict[str, Any],
    now: str,
) -> None:
    conn.execute(
        """
        INSERT INTO external_gate_action_events(
          event_id, gate_action_id, run_id, candidate_id, sandbox_id, event_type, status, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _stable_id("egaevt", {"gate_action_id": gate_action_id, "event_type": event_type, "now": now, "payload": payload}, 22),
            gate_action_id,
            run_id,
            candidate_id,
            sandbox_id,
            event_type,
            status,
            _json(payload),
            now,
        ),
    )


def _gate_action_row_payload(row: sqlite3.Row, *, accepted: bool | None = None, status: str | None = None) -> dict[str, Any]:
    out = {
        "accepted": bool(accepted) if accepted is not None else row["status"] in {"accepted", "duplicate"},
        "candidate_id": row["candidate_id"],
        "gate_action_id": row["gate_action_id"],
        "run_id": row["run_id"],
        "sandbox_id": row["sandbox_id"],
        "strategy_line": row["strategy_line"],
        "gate_decision": row["gate_decision"],
        "applied_policy": _loads(row["applied_policy_json"], {}),
        "status": status or row["status"],
        "error_code": row["error_code"],
        "error_message": row["error_message"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    return out


def ingest_gate_action_payload(
    sandbox_id: str,
    strategy_line: str,
    payload: dict[str, Any],
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("gate_action", operation_context or payload)
    _assert_operation_allowed(op)
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    line = _validate_strategy_line(strategy_line)
    now = _now()
    candidate_id = str(payload.get("candidate_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    gate_decision = str(payload.get("gate_decision") or "").strip()
    if gate_decision not in GATE_DECISIONS:
        return _with_operation_payload(
            {
                "accepted": False,
                "candidate_id": candidate_id,
                "gate_action_id": None,
                "applied_policy": {},
                "status": "invalid",
                "error_code": "invalid_gate_decision",
                "error_message": f"invalid gate_decision: {gate_decision}",
            },
            op,
            sandbox_id=sandbox_id,
        )
    db_path = Path(sandbox["db_path"])
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        candidate = conn.execute(
            """
            SELECT * FROM external_trade_candidates
            WHERE sandbox_id=? AND strategy_line=? AND candidate_id=?
            """,
            (sandbox_id, line, candidate_id),
        ).fetchone()
        if not candidate:
            _gate_action_event(
                conn,
                gate_action_id=None,
                run_id=str(payload.get("run_id") or ""),
                candidate_id=candidate_id,
                sandbox_id=sandbox_id,
                event_type="gate_action_rejected",
                status="candidate_missing",
                payload={"error_code": "candidate_missing"},
                now=now,
            )
            _audit_event(
                conn,
                event_type="gate_action_rejected",
                status="candidate_missing",
                sandbox_id=sandbox_id,
                strategy_line=line,
                run_id=str(payload.get("run_id") or ""),
                candidate_id=candidate_id,
                idempotency_key=idempotency_key,
                request=payload,
                response={"accepted": False, "error_code": "candidate_missing"},
                error_code="candidate_missing",
                now=now,
            )
            conn.commit()
            return _with_operation_payload(
                {
                    "accepted": False,
                    "candidate_id": candidate_id,
                    "gate_action_id": None,
                    "applied_policy": {},
                    "status": "rejected",
                    "error_code": "candidate_missing",
                    "error_message": f"candidate not found: {candidate_id}",
                },
                op,
                sandbox_id=sandbox_id,
            )
        if idempotency_key:
            existing = conn.execute(
                """
                SELECT * FROM external_gate_actions
                WHERE sandbox_id=? AND idempotency_key=?
                """,
                (sandbox_id, idempotency_key),
            ).fetchone()
            if existing:
                if existing["candidate_id"] == candidate_id and existing["gate_decision"] == gate_decision:
                    out = _gate_action_row_payload(existing, accepted=True, status="duplicate")
                    _gate_action_event(
                        conn,
                        gate_action_id=existing["gate_action_id"],
                        run_id=existing["run_id"],
                        candidate_id=candidate_id,
                        sandbox_id=sandbox_id,
                        event_type="gate_action_duplicate",
                        status="duplicate",
                        payload={"idempotency_key": idempotency_key},
                        now=now,
                    )
                    _audit_event(
                        conn,
                        event_type="gate_action_duplicate",
                        status="duplicate",
                        sandbox_id=sandbox_id,
                        strategy_line=line,
                        run_id=existing["run_id"],
                        candidate_id=candidate_id,
                        gate_action_id=existing["gate_action_id"],
                        idempotency_key=idempotency_key,
                        request=payload,
                        response=out,
                        payload={"idempotency_key": idempotency_key},
                        now=now,
                    )
                    conn.commit()
                    return _with_operation_payload(out, op, sandbox_id=sandbox_id)
                return _with_operation_payload(
                    {
                        "accepted": False,
                        "candidate_id": candidate_id,
                        "gate_action_id": existing["gate_action_id"],
                        "applied_policy": _loads(existing["applied_policy_json"], {}),
                        "status": "rejected",
                        "error_code": "idempotency_conflict",
                        "error_message": "idempotency_key already used for a different gate action",
                    },
                    op,
                    sandbox_id=sandbox_id,
                )
        active = conn.execute(
            """
            SELECT * FROM external_gate_actions
            WHERE candidate_id=? AND status='accepted'
            """,
            (candidate_id,),
        ).fetchone()
        if active:
            return _with_operation_payload(
                {
                    "accepted": False,
                    "candidate_id": candidate_id,
                    "gate_action_id": active["gate_action_id"],
                    "applied_policy": _loads(active["applied_policy_json"], {}),
                    "status": "rejected",
                    "error_code": "active_action_conflict",
                    "error_message": "candidate already has an accepted gate action",
                },
                op,
                sandbox_id=sandbox_id,
            )
        action_payload = payload.get("gate_action_payload") if isinstance(payload.get("gate_action_payload"), dict) else {}
        applied_policy = {
            "gate_decision": gate_decision,
            "action": action_payload.get("action") or gate_decision,
            "deterministic": bool(action_payload.get("deterministic", True)),
            "final_gate_decision_by_llm": bool(action_payload.get("final_gate_decision_by_llm", False)),
            "execution_default_when_scorer_unavailable": "review_or_block",
        }
        gate_action_id = _stable_id(
            "egact",
            {
                "sandbox_id": sandbox_id,
                "candidate_id": candidate_id,
                "idempotency_key": idempotency_key or uuid.uuid4().hex,
            },
            24,
        )
        run_id = str(payload.get("run_id") or candidate["run_id"])
        conn.execute(
            """
            INSERT INTO external_gate_actions(
              gate_action_id, run_id, candidate_id, sandbox_id, branch_id, strategy_line,
              unit_id, unit_version, selection_id, scorer_output_ref, final_gate_decision_ref,
              gate_decision, gate_action_payload_json, reason_codes_json, audit_trace_id,
              idempotency_key, status, applied_policy_json, error_code, error_message,
              created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'accepted', ?, NULL, NULL, ?, ?)
            """,
            (
                gate_action_id,
                run_id,
                candidate_id,
                sandbox_id,
                candidate["branch_id"],
                line,
                str(payload.get("unit_id") or ""),
                str(payload.get("unit_version") or ""),
                payload.get("selection_id"),
                payload.get("scorer_output_ref"),
                payload.get("final_gate_decision_ref"),
                gate_decision,
                _json(action_payload),
                _json(payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else []),
                payload.get("audit_trace_id"),
                idempotency_key,
                _json(applied_policy),
                str(payload.get("created_at") or now),
                now,
            ),
        )
        row = conn.execute("SELECT * FROM external_gate_actions WHERE gate_action_id=?", (gate_action_id,)).fetchone()
        _gate_action_event(
            conn,
            gate_action_id=gate_action_id,
            run_id=run_id,
            candidate_id=candidate_id,
            sandbox_id=sandbox_id,
            event_type="gate_action_accepted",
            status="accepted",
            payload={"gate_decision": gate_decision, "idempotency_key": idempotency_key},
            now=now,
        )
        _audit_event(
            conn,
            event_type="gate_action_accepted",
            status="accepted",
            sandbox_id=sandbox_id,
            strategy_line=line,
            run_id=run_id,
            candidate_id=candidate_id,
            gate_action_id=gate_action_id,
            idempotency_key=idempotency_key,
            request=payload,
            response={"accepted": True, "gate_action_id": gate_action_id, "gate_decision": gate_decision},
            payload={"gate_decision": gate_decision, "operation": op},
            now=now,
        )
        conn.commit()
        return _with_operation_payload(_gate_action_row_payload(row, accepted=True), op, sandbox_id=sandbox_id)


def _metric_summary(values: list[float]) -> dict[str, Any]:
    wins = [v for v in values if v > 0]
    losses = [v for v in values if v < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    return {
        "trade_count": len(values),
        "profit_factor": round(gp / gl, 8) if gl else (999.0 if gp else None),
        "gross_profit_R": round(gp, 8),
        "gross_loss_R": round(gl, 8),
        "net_R_sum": round(sum(values), 8),
        "win_count": len(wins),
        "loss_count": len(losses),
    }


def _latest_action_for_candidate(conn: sqlite3.Connection, candidate_id: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM external_gate_actions
        WHERE candidate_id=? AND status='accepted'
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
        """,
        (candidate_id,),
    ).fetchone()


def _baseline_order_for_candidate(conn: sqlite3.Connection, candidate: sqlite3.Row) -> sqlite3.Row | None:
    refs = _loads(candidate["context_refs_json"], {})
    source_order_id = refs.get("source_order_id") or candidate["source_order_id"]
    if not source_order_id:
        return None
    return conn.execute("SELECT * FROM sandbox_orders WHERE order_id=?", (source_order_id,)).fetchone()


def _write_gated_run(
    sandbox_id: str,
    strategy_line: str,
    payload: dict[str, Any] | None,
    *,
    execution_mode: str,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    line = _validate_strategy_line(strategy_line)
    body = payload or {}
    op = _operation_context(execution_mode, operation_context or body)
    _assert_operation_allowed(op)
    now = _now()
    run_id = str(body.get("run_id") or "")
    baseline_run_id = str(body.get("baseline_run_id") or run_id or "")
    gate_action_batch_id = str(body.get("gate_action_batch_id") or _stable_id("egab", {"sandbox_id": sandbox_id, "strategy_line": line, "run_id": run_id}, 18))
    execution_policy = body.get("execution_policy") if isinstance(body.get("execution_policy"), dict) else {}
    execution_policy = {
        "mode": execution_mode,
        "missing_gate_action_policy": execution_policy.get("missing_gate_action_policy", "review"),
        "review_policy": execution_policy.get("review_policy", "shadow_review"),
        "scorer_unavailable_policy": execution_policy.get("scorer_unavailable_policy", "review_or_block"),
        "llm_unavailable_policy": execution_policy.get("llm_unavailable_policy", "semantic_audit_unavailable_only"),
        **execution_policy,
    }
    gated_run_id = _stable_id(
        "egrun",
        {
            "sandbox_id": sandbox_id,
            "strategy_line": line,
            "run_id": run_id,
            "baseline_run_id": baseline_run_id,
            "execution_mode": execution_mode,
            "nonce": uuid.uuid4().hex,
        },
        24,
    )
    db_path = Path(sandbox["db_path"])
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        clauses = ["sandbox_id=?", "strategy_line=?"]
        params: list[Any] = [sandbox_id, line]
        if run_id:
            clauses.append("run_id=?")
            params.append(run_id)
        candidates = conn.execute(
            f"""
            SELECT * FROM external_trade_candidates
            WHERE {' AND '.join(clauses)}
            ORDER BY decision_time ASC, candidate_id ASC
            """,
            params,
        ).fetchall()
        counts = {"allow": 0, "block": 0, "reduce_size": 0, "review": 0}
        gated_values: list[float] = []
        baseline_values: list[float] = []
        result_refs: list[dict[str, Any]] = []
        order_count = 0
        for candidate in candidates:
            action = _latest_action_for_candidate(conn, candidate["candidate_id"])
            baseline = _baseline_order_for_candidate(conn, candidate)
            baseline_net_r = float(baseline["net_R"]) if baseline and baseline["net_R"] is not None else None
            if baseline_net_r is not None:
                baseline_values.append(baseline_net_r)
            action_payload = _loads(action["gate_action_payload_json"], {}) if action else {}
            applied_policy = _loads(action["applied_policy_json"], {}) if action else {}
            decision = str(action["gate_decision"] if action else execution_policy["missing_gate_action_policy"])
            if decision not in GATE_DECISIONS:
                decision = "review"
            counts[decision] = counts.get(decision, 0) + 1
            original_size = float(candidate["intended_size"] or 0.0)
            multiplier = 1.0
            if decision == "reduce_size":
                if action_payload.get("adjusted_size") is not None and original_size:
                    multiplier = max(0.0, float(action_payload.get("adjusted_size")) / original_size)
                else:
                    multiplier = max(0.0, float(action_payload.get("size_multiplier") or 0.5))
            elif decision in {"block", "review"}:
                multiplier = 0.0
            executed_size = round(original_size * multiplier, 8)
            order_status = "executed" if decision in {"allow", "reduce_size"} and executed_size > 0 else ("blocked" if decision == "block" else "shadow_review")
            fill_status = "counterfactual_reused" if order_status == "executed" and baseline_net_r is not None else "not_filled"
            adjusted_net_r = round((baseline_net_r or 0.0) * multiplier, 8) if order_status == "executed" else 0.0
            if order_status == "executed":
                order_count += 1
                gated_values.append(adjusted_net_r)
            result_ref = f"sandbox:{sandbox_id}:gated_run:{gated_run_id}:candidate:{candidate['candidate_id']}"
            order_id = _stable_id("egord", {"gated_run_id": gated_run_id, "candidate_id": candidate["candidate_id"]}, 24)
            result_id = _stable_id("egres", {"gated_run_id": gated_run_id, "candidate_id": candidate["candidate_id"]}, 24)
            context_refs = _loads(candidate["context_refs_json"], {})
            context_refs.update(
                {
                    "baseline_order_id": baseline["order_id"] if baseline else None,
                    "gate_action_id": action["gate_action_id"] if action else None,
                    "audit_trace_id": action["audit_trace_id"] if action else None,
                    "execution_mode": execution_mode,
                    "outcome_source": "sandbox_baseline_counterfactual",
                }
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO external_gated_orders(
                  order_id, gated_run_id, baseline_run_id, candidate_id, gate_action_id,
                  sandbox_id, strategy_line, symbol, side, original_size, executed_size,
                  gate_decision, applied_action, order_status, fill_status, context_refs_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    gated_run_id,
                    baseline_run_id,
                    candidate["candidate_id"],
                    action["gate_action_id"] if action else None,
                    sandbox_id,
                    line,
                    candidate["symbol"],
                    candidate["side"],
                    original_size,
                    executed_size,
                    decision,
                    applied_policy.get("action") or decision,
                    order_status,
                    fill_status,
                    _json(context_refs),
                    now,
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO external_gated_results(
                  result_id, gated_run_id, order_id, candidate_id, sandbox_id, strategy_line,
                  net_R, MFE_R, MAE_R, exit_reason, quality_label, result_ref, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_id,
                    gated_run_id,
                    order_id,
                    candidate["candidate_id"],
                    sandbox_id,
                    line,
                    adjusted_net_r,
                    (float(baseline["MFE_R"]) * multiplier) if baseline and baseline["MFE_R"] is not None else None,
                    (float(baseline["MAE_R"]) * multiplier) if baseline and baseline["MAE_R"] is not None else None,
                    baseline["exit_reason"] if baseline and order_status == "executed" else order_status,
                    "gated_executed" if order_status == "executed" else order_status,
                    result_ref,
                    now,
                ),
            )
            result_refs.append({"candidate_id": candidate["candidate_id"], "order_id": order_id, "result_ref": result_ref})
        baseline_metrics = _metric_summary(baseline_values)
        gated_metrics = _metric_summary(gated_values)
        delta_metrics = {
            "trade_count_delta": gated_metrics["trade_count"] - baseline_metrics["trade_count"],
            "net_R_sum_delta": round(gated_metrics["net_R_sum"] - baseline_metrics["net_R_sum"], 8),
            "profit_factor_delta": None
            if gated_metrics["profit_factor"] is None or baseline_metrics["profit_factor"] is None
            else round(float(gated_metrics["profit_factor"]) - float(baseline_metrics["profit_factor"]), 8),
        }
        coverage = {
            "candidate_count": len(candidates),
            "action_count": sum(counts.values()),
            "missing_action_policy": execution_policy["missing_gate_action_policy"],
            "baseline_result_count": len(baseline_values),
            "gated_result_count": len(gated_values),
        }
        metrics = {
            "baseline_metrics": baseline_metrics,
            "gated_metrics": gated_metrics,
            "delta_metrics": delta_metrics,
            "coverage": coverage,
        }
        conn.execute(
            """
            INSERT OR REPLACE INTO external_gated_runs(
              gated_run_id, sandbox_id, strategy_line, run_id, baseline_run_id, gate_action_batch_id,
              execution_mode, execution_policy_json, status, candidate_count, allowed_count,
              blocked_count, reduced_count, review_count, order_count, result_ref, metrics_json,
              created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gated_run_id,
                sandbox_id,
                line,
                run_id,
                baseline_run_id,
                gate_action_batch_id,
                execution_mode,
                _json(execution_policy),
                len(candidates),
                counts.get("allow", 0),
                counts.get("block", 0),
                counts.get("reduce_size", 0),
                counts.get("review", 0),
                order_count,
                f"sandbox:{sandbox_id}:gated_run:{gated_run_id}",
                _json(metrics),
                now,
                now,
            ),
        )
        performance_id = _stable_id("egperf", {"gated_run_id": gated_run_id}, 24)
        conn.execute(
            """
            INSERT OR REPLACE INTO external_gated_performance(
              performance_id, gated_run_id, baseline_run_id, sandbox_id, strategy_line,
              baseline_metrics_json, gated_metrics_json, delta_metrics_json, coverage_json,
              result_refs_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                performance_id,
                gated_run_id,
                baseline_run_id,
                sandbox_id,
                line,
                _json(baseline_metrics),
                _json(gated_metrics),
                _json(delta_metrics),
                _json(coverage),
                _json(result_refs),
                now,
            ),
        )
        response = {
            "gated_run_id": gated_run_id,
            "baseline_run_id": baseline_run_id,
            "gate_action_batch_id": gate_action_batch_id,
            "candidate_count": len(candidates),
            "allowed_count": counts.get("allow", 0),
            "blocked_count": counts.get("block", 0),
            "reduced_count": counts.get("reduce_size", 0),
            "review_count": counts.get("review", 0),
            "order_count": order_count,
            "status": "completed",
            "result_ref": f"sandbox:{sandbox_id}:gated_run:{gated_run_id}",
            "metrics": metrics,
        }
        _audit_event(
            conn,
            event_type=f"{execution_mode}_completed",
            status="completed",
            sandbox_id=sandbox_id,
            strategy_line=line,
            run_id=run_id,
            gated_run_id=gated_run_id,
            request=body,
            response=response,
            payload={"execution_mode": execution_mode, "gate_action_batch_id": gate_action_batch_id, "operation": op},
            now=now,
        )
        conn.commit()
        return _with_operation_payload(response, op, sandbox_id=sandbox_id)


def gated_replay_payload(
    sandbox_id: str,
    strategy_line: str,
    payload: dict[str, Any] | None = None,
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _write_gated_run(sandbox_id, strategy_line, payload, execution_mode="gated_replay", root=root, operation_context=operation_context)


def gated_paper_shadow_payload(
    sandbox_id: str,
    strategy_line: str,
    payload: dict[str, Any] | None = None,
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = dict(payload or {})
    policy = body.get("execution_policy") if isinstance(body.get("execution_policy"), dict) else {}
    body["execution_policy"] = {
        "paper_shadow_safe_mode": True,
        "missing_gate_action_policy": "review",
        "paper_api_unavailable_policy": "record_failed_evidence_only",
        **policy,
    }
    return _write_gated_run(sandbox_id, strategy_line, body, execution_mode="gated_paper_shadow", root=root, operation_context=operation_context or body)


def gated_orders_payload(
    sandbox_id: str,
    strategy_line: str,
    *,
    run_id: str | None = None,
    gated_run_id: str | None = None,
    limit: int = 100,
    root: Path | None = None,
) -> dict[str, Any]:
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    line = _validate_strategy_line(strategy_line)
    limit = max(1, min(int(limit or 100), 500))
    clauses = ["o.sandbox_id=?", "o.strategy_line=?"]
    params: list[Any] = [sandbox_id, line]
    if gated_run_id:
        clauses.append("o.gated_run_id=?")
        params.append(gated_run_id)
    if run_id:
        clauses.append("r.run_id=?")
        params.append(run_id)
    with _connect(Path(sandbox["db_path"])) as conn:
        ensure_sandbox_tables(conn)
        rows = conn.execute(
            f"""
            SELECT o.* FROM external_gated_orders o
            JOIN external_gated_runs r ON r.gated_run_id=o.gated_run_id
            WHERE {' AND '.join(clauses)}
            ORDER BY o.created_at ASC, o.order_id ASC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()
    orders = [
        {
            "order_id": row["order_id"],
            "candidate_id": row["candidate_id"],
            "run_id": run_id,
            "gated_run_id": row["gated_run_id"],
            "strategy_line": row["strategy_line"],
            "symbol": row["symbol"],
            "side": row["side"],
            "original_size": row["original_size"],
            "executed_size": row["executed_size"],
            "gate_decision": row["gate_decision"],
            "applied_action": row["applied_action"],
            "order_status": row["order_status"],
            "fill_status": row["fill_status"],
            "created_at": row["created_at"],
            "context_refs": _loads(row["context_refs_json"], {}),
        }
        for row in rows
    ]
    return {"sandbox_id": sandbox_id, "strategy_line": line, "count": len(orders), "orders": orders}


def gated_trade_quality_samples_payload(
    sandbox_id: str,
    strategy_line: str,
    *,
    gated_run_id: str | None = None,
    limit: int = 100,
    root: Path | None = None,
) -> dict[str, Any]:
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    line = _validate_strategy_line(strategy_line)
    clauses = ["sandbox_id=?", "strategy_line=?"]
    params: list[Any] = [sandbox_id, line]
    if gated_run_id:
        clauses.append("gated_run_id=?")
        params.append(gated_run_id)
    with _connect(Path(sandbox["db_path"])) as conn:
        ensure_sandbox_tables(conn)
        rows = conn.execute(
            f"""
            SELECT * FROM external_gated_results
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at ASC, result_id ASC
            LIMIT ?
            """,
            [*params, max(1, min(int(limit or 100), 500))],
        ).fetchall()
    samples = [
        {
            "trade_id": row["result_id"],
            "order_id": row["order_id"],
            "candidate_id": row["candidate_id"],
            "net_R": row["net_R"],
            "MFE_R": row["MFE_R"],
            "MAE_R": row["MAE_R"],
            "exit_reason": row["exit_reason"],
            "quality_label": row["quality_label"],
            "result_ref": row["result_ref"],
            "created_at": row["created_at"],
        }
        for row in rows
    ]
    return {"sandbox_id": sandbox_id, "strategy_line": line, "count": len(samples), "samples": samples}


def gated_performance_payload(
    sandbox_id: str,
    strategy_line: str,
    *,
    gated_run_id: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    sandbox = _assert_active_sandbox_writable(sandbox_id, root)
    line = _validate_strategy_line(strategy_line)
    clauses = ["sandbox_id=?", "strategy_line=?"]
    params: list[Any] = [sandbox_id, line]
    if gated_run_id:
        clauses.append("gated_run_id=?")
        params.append(gated_run_id)
    with _connect(Path(sandbox["db_path"])) as conn:
        ensure_sandbox_tables(conn)
        row = conn.execute(
            f"""
            SELECT * FROM external_gated_performance
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
    if not row:
        return {"sandbox_id": sandbox_id, "strategy_line": line, "status": "missing", "error_code": "gated_performance_missing"}
    return {
        "sandbox_id": sandbox_id,
        "strategy_line": line,
        "baseline_run_id": row["baseline_run_id"],
        "gated_run_id": row["gated_run_id"],
        "baseline_metrics": _loads(row["baseline_metrics_json"], {}),
        "gated_metrics": _loads(row["gated_metrics_json"], {}),
        "delta_metrics": _loads(row["delta_metrics_json"], {}),
        "coverage": _loads(row["coverage_json"], {}),
        "result_refs": _loads(row["result_refs_json"], []),
        "created_at": row["created_at"],
    }


P27_CONTRACTS = {
    "external_full_backtest_manifest": "STEP27.1-v1",
    "external_trade_candidate_gate_action": "STEP27.2-v1",
    "external_gated_replay_paper_shadow": "STEP27.3-v1",
    "external_integration_audit": "STEP27.4-v1",
}


def _ensure_external_contract_versions(conn: sqlite3.Connection, now: str) -> None:
    for key, version in P27_CONTRACTS.items():
        conn.execute(
            """
            INSERT INTO external_contract_versions(contract_key, contract_version, status, payload_json, updated_at)
            VALUES (?, ?, 'active', ?, ?)
            ON CONFLICT(contract_key) DO UPDATE SET
              contract_version=excluded.contract_version,
              status=excluded.status,
              payload_json=excluded.payload_json,
              updated_at=excluded.updated_at
            """,
            (key, version, _json({"contract_key": key, "contract_version": version, "owner": "abnormal_enchanced"}), now),
        )


def external_integration_health_payload(*, root: Path | None = None) -> dict[str, Any]:
    sandboxes = list_sandboxes_payload(root=root, limit=500).get("sandboxes", [])
    now = _now()
    rows: list[dict[str, Any]] = []
    aggregate = {
        "sandboxes": len(sandboxes),
        "external_full_backtest_runs": 0,
        "external_trade_candidates": 0,
        "external_gate_actions": 0,
        "external_gated_runs": 0,
        "external_integration_audit_events": 0,
    }
    for sandbox in sandboxes:
        db_path = Path(sandbox["db_path"])
        with _connect(db_path) as conn:
            ensure_sandbox_tables(conn)
            _ensure_external_contract_versions(conn, now)
            conn.commit()
            counts = {
                "external_full_backtest_runs": _table_count(conn, "external_full_backtest_runs"),
                "external_trade_candidates": _table_count(conn, "external_trade_candidates"),
                "external_gate_actions": _table_count(conn, "external_gate_actions"),
                "external_gated_runs": _table_count(conn, "external_gated_runs"),
                "external_integration_audit_events": _table_count(conn, "external_integration_audit_events"),
                "external_contract_versions": _table_count(conn, "external_contract_versions"),
            }
        for key in aggregate:
            if key != "sandboxes":
                aggregate[key] += counts.get(key, 0)
        rows.append(
            {
                "sandbox_id": sandbox["sandbox_id"],
                "strategy_line": sandbox["strategy_line"],
                "db_path": str(db_path),
                "write_scope": sandbox.get("write_scope"),
                "external_direct_sqlite_write_allowed": False,
                "counts": counts,
            }
        )
    status = "ok" if sandboxes else "no_sandboxes"
    return {
        "status": status,
        "generated_at": now,
        "contract_versions": P27_CONTRACTS,
        "aggregate": aggregate,
        "sandboxes": rows,
        "http_api_only_for_external": True,
        "external_sqlite_write_allowed": False,
    }


def external_integration_run_payload(run_id: str, *, root: Path | None = None) -> dict[str, Any]:
    sandboxes = list_sandboxes_payload(root=root, limit=500).get("sandboxes", [])
    matches: list[dict[str, Any]] = []
    for sandbox in sandboxes:
        with _connect(Path(sandbox["db_path"])) as conn:
            ensure_sandbox_tables(conn)
            full = conn.execute("SELECT * FROM external_full_backtest_runs WHERE run_id=?", (run_id,)).fetchone()
            if full:
                matches.append({"type": "full_backtest_run", "sandbox_id": sandbox["sandbox_id"], "payload": _external_run_payload(conn, full)})
            gated_rows = conn.execute("SELECT * FROM external_gated_runs WHERE gated_run_id=? OR run_id=?", (run_id, run_id)).fetchall()
            for row in gated_rows:
                matches.append(
                    {
                        "type": "gated_run",
                        "sandbox_id": sandbox["sandbox_id"],
                        "payload": {
                            "gated_run_id": row["gated_run_id"],
                            "run_id": row["run_id"],
                            "baseline_run_id": row["baseline_run_id"],
                            "strategy_line": row["strategy_line"],
                            "execution_mode": row["execution_mode"],
                            "status": row["status"],
                            "candidate_count": row["candidate_count"],
                            "order_count": row["order_count"],
                            "metrics": _loads(row["metrics_json"], {}),
                            "result_ref": row["result_ref"],
                            "created_at": row["created_at"],
                        },
                    }
                )
    return {"run_id": run_id, "count": len(matches), "runs": matches, "status": "ok" if matches else "missing"}


def external_integration_audit_events_payload(
    *,
    run_id: str | None = None,
    sandbox_id: str | None = None,
    candidate_id: str | None = None,
    gated_run_id: str | None = None,
    limit: int = 100,
    root: Path | None = None,
) -> dict[str, Any]:
    sandboxes = list_sandboxes_payload(root=root, limit=500).get("sandboxes", [])
    limit = max(1, min(int(limit or 100), 500))
    events: list[dict[str, Any]] = []
    for sandbox in sandboxes:
        if sandbox_id and sandbox["sandbox_id"] != sandbox_id:
            continue
        clauses: list[str] = []
        params: list[Any] = []
        if run_id:
            clauses.append("run_id=?")
            params.append(run_id)
        if candidate_id:
            clauses.append("candidate_id=?")
            params.append(candidate_id)
        if gated_run_id:
            clauses.append("gated_run_id=?")
            params.append(gated_run_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with _connect(Path(sandbox["db_path"])) as conn:
            ensure_sandbox_tables(conn)
            rows = conn.execute(
                f"""
                SELECT * FROM external_integration_audit_events
                {where}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [*params, limit],
            ).fetchall()
        for row in rows:
            events.append(
                {
                    "event_id": row["event_id"],
                    "event_type": row["event_type"],
                    "sandbox_id": row["sandbox_id"],
                    "strategy_line": row["strategy_line"],
                    "run_id": row["run_id"],
                    "candidate_id": row["candidate_id"],
                    "gate_action_id": row["gate_action_id"],
                    "gated_run_id": row["gated_run_id"],
                    "idempotency_key": row["idempotency_key"],
                    "request_hash": row["request_hash"],
                    "response_hash": row["response_hash"],
                    "status": row["status"],
                    "error_code": row["error_code"],
                    "retryable": bool(row["retryable"]),
                    "payload": _loads(row["payload_json"], {}),
                    "created_at": row["created_at"],
                }
            )
    events.sort(key=lambda item: item["created_at"], reverse=True)
    return {"count": len(events[:limit]), "events": events[:limit]}


def _sandbox_params(strategy_line: str, options: dict[str, Any]) -> dict[str, Any]:
    params = {
        "strategy_line": strategy_line,
        "min_score": float(options.get("min_score", 20)),
        "target_rr": float(options.get("target_rr", 0.8)),
        "min_rr": float(options.get("min_rr", 0.2)),
        "min_net_rr": float(options.get("min_net_rr", 0.2)),
        "min_effective_rr": float(options.get("min_effective_rr", 0.2)),
        "stop_atr_mult": float(options.get("stop_atr_mult", 1.0)),
        "max_stop_bps": float(options.get("max_stop_bps", 240)),
        "min_stop_bps": float(options.get("min_stop_bps", 3)),
        "min_reachable_reward_bps": float(options.get("min_reachable_reward_bps", 5)),
        "min_tp_after_cost_bps": float(options.get("min_tp_after_cost_bps", 0)),
        "tp_target_policy": options.get(
            "tp_target_policy",
            {
                "mode": "fast_capped_rr",
                "target_net_rr": 1.0,
                "target_rr_cap": 1.0,
                "min_reward_bps": 1,
                "require_market_room": False,
                "allow_structure_runner": True,
            },
        ),
        "range_room": options.get("range_room", {"long_max_range_pos": 1.0, "short_min_range_pos": 0.0}),
        "taker_fee_bps": float(options.get("taker_fee_bps", 5)),
        "slippage_bps": float(options.get("slippage_bps", 1)),
        "max_hold_minutes": int(options.get("max_hold_minutes", 60)),
    }
    if strategy_line == "strategy6":
        # Sandbox smoke jobs must prove the real evaluator/replay/TQ chain can
        # close without weakening the production Strategy6 config. These values
        # live only in sandbox job params unless explicitly overridden.
        params.update(
            {
                "strategy6_wait_rebound_enabled": True,
                "strategy6_wait_allow_base_wait": True,
                "wait_check_interval_min": 1,
                "max_wait_minutes": 5,
                "min_rebound_score": 0,
                "pullback_min_bps": 0,
                "pullback_max_bps": 999,
                "max_chase_after_wait_bps": 999,
                "continuation_confirm_bars": 0,
                "min_direction_acceptance_score": 20,
                "v2_min_direction_acceptance_score": 20,
                "v3_min_direction_context_score": 20,
                "v3_1_min_direction_context_score": 20,
                "v3_2_long_min_direction_context_score": 20,
                "v3_2_short_min_direction_context_score": 20,
            }
        )
    for key, value in (options.get("strategy_params") or {}).items():
        params[key] = value
    return params


def _default_coarse_parameter_sets(strategy_line: str) -> list[dict[str, Any]]:
    if strategy_line == "strategy4":
        return [
            {"observe_ttl_minutes": 15, "check_interval_minutes": 3, "max_attempts": 3, "recheck_min_score": 55, "target_rr": 0.6},
            {"observe_ttl_minutes": 30, "check_interval_minutes": 5, "max_attempts": 6, "recheck_min_score": 60, "target_rr": 0.8},
        ]
    if strategy_line == "strategy5":
        return [
            {"min_score": 20, "target_rr": 0.6, "max_stop_bps": 180, "entry_chase_limit": "loose"},
            {"min_score": 35, "target_rr": 0.8, "max_stop_bps": 240, "entry_chase_limit": "normal"},
        ]
    if strategy_line == "strategy6":
        return [
            {"min_direction_acceptance_score": 20, "wait_max_minutes": 5, "pullback_min_bps": 0, "target_rr": 0.6},
            {"min_direction_acceptance_score": 35, "wait_max_minutes": 10, "pullback_min_bps": 5, "target_rr": 0.8},
        ]
    return [{"min_score": 20, "target_rr": 0.8}]


def _coarse_parameter_sets(strategy_line: str, options: dict[str, Any]) -> list[dict[str, Any]]:
    raw = options.get("parameter_sets")
    if isinstance(raw, list) and raw:
        return [item for item in raw if isinstance(item, dict)]
    max_sets = max(1, int(options.get("max_sets") or 2))
    return _default_coarse_parameter_sets(strategy_line)[:max_sets]


def _register_parameter_set(
    conn: sqlite3.Connection,
    sandbox_id: str,
    strategy_line: str,
    options: dict[str, Any],
    now: str,
    *,
    status: str = "running",
    metrics: dict[str, Any] | None = None,
) -> str | None:
    parameter_set_id = _parameter_set_id(strategy_line, options)
    if not parameter_set_id:
        return None
    conn.execute(
        """
        INSERT INTO sandbox_parameter_sets(
          parameter_set_id, sandbox_id, branch_id, strategy_line, matrix_profile,
          params_json, status, metrics_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(parameter_set_id) DO UPDATE SET
          status=excluded.status,
          metrics_json=excluded.metrics_json,
          updated_at=excluded.updated_at
        """,
        (
            parameter_set_id,
            sandbox_id,
            _branch_id(sandbox_id, strategy_line),
            strategy_line,
            str(options.get("matrix_profile") or "manual"),
            _json(options.get("strategy_params") or {}),
            status,
            _json(metrics or {}),
            now,
            now,
        ),
    )
    return parameter_set_id


def _query_param_clause(parameter_set_id: str | None, column: str = "parameter_set_id") -> tuple[str, tuple[Any, ...]]:
    if parameter_set_id:
        return f" AND {column}=?", (parameter_set_id,)
    return f" AND ({column} IS NULL OR {column}='')", ()


def _synthetic_rows(symbol: str, *, minutes: int = 90, scenario: str = "mixed") -> list[dict[str, Any]]:
    start = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=minutes + 5)
    price = 100.0 + (sum(ord(c) for c in symbol) % 20)
    rows: list[dict[str, Any]] = []
    for idx in range(minutes):
        drift = 1.0004 if idx % 5 else 0.9995
        if scenario == "winner" and idx > 42:
            drift = 1.0025
        elif scenario == "loser" and idx > 42:
            drift = 0.9975
        price *= drift
        open_time_ms = int((start + timedelta(minutes=idx)).timestamp() * 1000)
        rows.append(
            {
                "open_time_ms": open_time_ms,
                "open": price * 0.999,
                "high": price * 1.006,
                "low": price * 0.994,
                "close": price,
                "volume": 1000.0 + idx * 3,
            }
        )
    return rows


def _signal_from_rows(strategy_line: str, symbol: str, rows: list[dict[str, Any]], *, side: str = "LONG", index: int = 40) -> SandboxSignal:
    def _pct(new: float, old: float) -> float:
        return (new / old - 1.0) * 10000.0 if old else 0.0

    close = float(rows[index]["close"])
    vol_window = [float(row["volume"]) for row in rows[max(0, index - 30) : index]]
    avg_vol = sum(vol_window) / max(1, len(vol_window))
    high_30 = max(float(row["high"]) for row in rows[max(0, index - 30) : index + 1])
    low_30 = min(float(row["low"]) for row in rows[max(0, index - 30) : index + 1])
    range_pos = (close - low_30) / (high_30 - low_30) if high_30 > low_30 else 0.5
    features = {
        "pct_1m_bps": _pct(close, float(rows[index - 1]["close"])),
        "pct_3m_bps": _pct(close, float(rows[index - 3]["close"])),
        "pct_5m_bps": _pct(close, float(rows[index - 5]["close"])),
        "pct_15m_bps": _pct(close, float(rows[index - 15]["close"])),
        "volume_z": float(rows[index]["volume"]) / avg_vol if avg_vol else 1.0,
        "range_pos_30m": range_pos,
        "atr_1m_bps": 18.0,
        "close": close,
    }
    score = min(95.0, 65.0 + abs(features["pct_3m_bps"]) * 0.2 + max(0.0, features["volume_z"] - 1.0) * 8.0)
    return SandboxSignal(
        signal_id=f"{strategy_line}_{symbol}_{index}",
        strategy_line=strategy_line,
        symbol=symbol,
        side=side,
        index=index,
        signal_time_ms=int(rows[index]["open_time_ms"]),
        score=score,
        features=features,
    )


def _sandbox_symbols(options: dict[str, Any]) -> list[str]:
    raw = options.get("symbols") or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    limit = max(1, min(500, int(options.get("symbol_limit") or options.get("max_symbols") or 20)))
    return [str(item).upper() for item in raw if str(item).strip()][:limit]


def _sandbox_branch_lines(conn: sqlite3.Connection, sandbox_id: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT strategy_line FROM sandbox_strategy_branches
        WHERE sandbox_id=? AND COALESCE(branch_status, '') != 'deleted'
        ORDER BY strategy_line ASC
        """,
        (sandbox_id,),
    ).fetchall()
    lines = [str(row["strategy_line"]) for row in rows]
    if lines:
        return lines
    manifest = conn.execute("SELECT strategy_line FROM sandbox_manifest WHERE sandbox_id=?", (sandbox_id,)).fetchone()
    if manifest and str(manifest["strategy_line"]) in SUPPORTED_STRATEGY_LINES:
        return [str(manifest["strategy_line"])]
    return ["strategy6"]


def _selected_branch_lines(conn: sqlite3.Connection, sandbox_id: str, options: dict[str, Any]) -> list[str]:
    available = _sandbox_branch_lines(conn, sandbox_id)
    requested = str(options.get("strategy_line") or options.get("branch") or "all").strip()
    if requested in {"", "all", "*"}:
        return available
    line = _validate_strategy_line(requested)
    if line not in available:
        raise ValueError(f"strategy branch not in sandbox: {line}")
    return [line]


def _update_branch_metrics(conn: sqlite3.Connection, sandbox_id: str, strategy_line: str, now: str) -> dict[str, Any]:
    branch_id = _branch_id(sandbox_id, strategy_line)
    order_rows = conn.execute(
        "SELECT net_R FROM sandbox_orders WHERE sandbox_id=? AND strategy_line=? AND net_R IS NOT NULL",
        (sandbox_id, strategy_line),
    ).fetchall()
    values = [float(row["net_R"] or 0.0) for row in order_rows]
    gross_profit = sum(v for v in values if v > 0)
    gross_loss = abs(sum(v for v in values if v < 0))
    pf = round(gross_profit / gross_loss, 8) if gross_loss else (None if not gross_profit else 999.0)
    tq_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM trade_quality_samples WHERE sandbox_id=? AND strategy_line=?",
            (sandbox_id, strategy_line),
        ).fetchone()[0]
    )
    gate_rows = conn.execute(
        "SELECT candidate_id, test_metrics_json FROM gate_candidates WHERE sandbox_id=? AND strategy_line=?",
        (sandbox_id, strategy_line),
    ).fetchall()
    best_pf = pf
    best_candidate_id = None
    for row in gate_rows:
        metrics = _loads(row["test_metrics_json"], {})
        try:
            candidate_pf = float(metrics.get("profit_factor"))
        except Exception:
            continue
        if best_pf is None or candidate_pf > best_pf:
            best_pf = candidate_pf
            best_candidate_id = row["candidate_id"]
    metrics = {
        "profit_factor": best_pf,
        "raw_profit_factor": pf,
        "trade_count": len(values),
        "tq_sample_count": tq_count,
        "gate_candidate_count": len(gate_rows),
        "gross_profit_R": round(gross_profit, 8),
        "gross_loss_R": round(gross_loss, 8),
    }
    conn.execute(
        """
        INSERT OR REPLACE INTO sandbox_branch_leaderboard(
          leaderboard_id, sandbox_id, branch_id, strategy_line, best_pf, trade_count,
          tq_sample_count, gate_candidate_count, best_candidate_id, metrics_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _stable_id("sbl", {"sandbox_id": sandbox_id, "strategy_line": strategy_line}, 24),
            sandbox_id,
            branch_id,
            strategy_line,
            best_pf,
            len(values),
            tq_count,
            len(gate_rows),
            best_candidate_id,
            _json(metrics),
            now,
        ),
    )
    conn.execute(
        """
        UPDATE sandbox_strategy_branches
        SET branch_status='active', branch_metrics_json=?, updated_at=?
        WHERE branch_id=?
        """,
        (_json(metrics), now, branch_id),
    )
    return metrics


def _excursion(order: dict[str, Any], rows: list[dict[str, Any]], fill: dict[str, Any]) -> tuple[float, float]:
    entry_idx = int(order.get("entry_idx") or 0)
    exit_ms = int(fill.get("exit_time_ms") or rows[min(len(rows) - 1, entry_idx)]["open_time_ms"])
    entry = float(order.get("entry_price") or 0)
    stop = float(order.get("stop_loss") or entry)
    risk = abs(entry - stop)
    if risk <= 0:
        return 0.0, 0.0
    max_fav = 0.0
    max_adv = 0.0
    for row in rows[entry_idx:]:
        if int(row["open_time_ms"]) > exit_ms:
            break
        high = float(row["high"])
        low = float(row["low"])
        if str(order.get("side")).upper() == "LONG":
            max_fav = max(max_fav, high - entry)
            max_adv = max(max_adv, entry - low)
        else:
            max_fav = max(max_fav, entry - low)
            max_adv = max(max_adv, high - entry)
    return round(max_fav / risk, 8), round(max_adv / risk, 8)


def _ensure_real_evaluator_orders(conn: sqlite3.Connection, sandbox_id: str, strategy_line: str, options: dict[str, Any], now: str) -> dict[str, Any]:
    params = _sandbox_params(strategy_line, options)
    branch_id = _branch_id(sandbox_id, strategy_line)
    lineage = _runtime_lineage(conn, sandbox_id, strategy_line, options)
    parameter_set_id = _register_parameter_set(conn, sandbox_id, strategy_line, options, now)
    if lineage["runtime_status"] == "runtime_required" and not options.get("allow_runtime_missing"):
        raise ValueError(f"runtime_required_for_branch_overlay: {strategy_line}")
    run_id = _stable_id("eval", {"sandbox_id": sandbox_id, "branch_id": branch_id, "strategy_line": strategy_line, "options": options})
    existing = int(conn.execute("SELECT COUNT(*) FROM sandbox_orders WHERE sandbox_id=? AND evaluator_run_id=?", (sandbox_id, run_id)).fetchone()[0])
    if existing:
        return {"run_id": run_id, "order_count": existing, "status": "reused"}
    orders: list[dict[str, Any]] = []
    decisions = 0
    for idx, symbol in enumerate(_sandbox_symbols(options)):
        scenario = "winner" if idx % 3 == 0 else "loser" if idx % 3 == 1 else "mixed"
        side = "LONG" if idx % 2 == 0 else "SHORT"
        rows = _synthetic_rows(symbol, scenario=scenario)
        if side == "SHORT":
            # Mirror the post-signal path so the smoke covers both sides.
            for row in rows[42:]:
                row["high"], row["low"], row["close"] = row["high"] * 0.997, row["low"] * 0.997, row["close"] * 0.997
        signal_index = max(20, min(int(options.get("signal_index", 40)), len(rows) - 3))
        signal = _signal_from_rows(strategy_line, symbol, rows, side=side, index=signal_index)
        result = evaluate_signal_offline(signal, rows, params)
        decisions += 1
        order = result.get("order")
        if not order:
            continue
        order_id = _stable_id("sbo", {"sandbox_id": sandbox_id, "run_id": run_id, "symbol": symbol, "signal": signal.signal_time_ms}, 24)
        conn.execute(
            """
            INSERT OR REPLACE INTO sandbox_orders(
              order_id, sandbox_id, evaluator_run_id, fill_run_id, strategy_line, symbol, side,
              signal_time_ms, entry_time_ms, exit_time_ms, entry_price, exit_price, stop_loss,
              take_profit, planned_rr, net_R, MFE_R, MAE_R, exit_reason, score,
              reasons_json, features_json, trade_plan_payload_json, fill_result_json, created_at,
              branch_id, code_overlay_id, code_patch_id, runtime_id, parameter_set_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                sandbox_id,
                run_id,
                None,
                strategy_line,
                symbol,
                str(order.get("side") or side).upper(),
                int(order.get("signal_time_ms") or signal.signal_time_ms),
                int(order.get("entry_time_ms") or rows[signal.index + 1]["open_time_ms"]),
                None,
                float(order.get("entry_price") or 0.0),
                None,
                float(order.get("stop_loss") or 0.0),
                float(order.get("take_profit") or 0.0),
                float(order.get("planned_rr") or 0.0),
                None,
                None,
                None,
                None,
                float(order.get("score") or signal.score),
                _json(result.get("reason_codes") or []),
                _json({**(order.get("features") or {}), "_sandbox_rows": rows}),
                _json(result.get("trade_plan_payload") or {}),
                _json({}),
                now,
                branch_id,
                lineage["code_overlay_id"],
                lineage["code_patch_id"],
                lineage["runtime_id"],
                parameter_set_id,
            ),
        )
        orders.append(order)
    metrics = {"decision_count": decisions, "order_count": len(orders), "profit_factor": None, "trade_count": 0}
    conn.execute(
        """
        INSERT OR REPLACE INTO evaluator_runs(
          run_id, sandbox_id, adapter_name, status, metrics_json, evidence_json, created_at,
          branch_id, strategy_line, code_overlay_id, code_patch_id, runtime_id, parameter_set_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            sandbox_id,
            f"{ENGINE_MODE}:sandbox",
            "real_evaluator_smoke_complete",
            _json(metrics),
            _json({"no_shadow_logic": True, "runtime_mutation": False, "write_scope": "sandbox_only", "code_lineage": lineage}),
            now,
            branch_id,
            strategy_line,
            lineage["code_overlay_id"],
            lineage["code_patch_id"],
            lineage["runtime_id"],
            parameter_set_id,
        ),
    )
    _update_branch_metrics(conn, sandbox_id, strategy_line, now)
    return {"run_id": run_id, "order_count": len(orders), "decision_count": decisions, "status": "created"}


def _ensure_replayed_fills(conn: sqlite3.Connection, sandbox_id: str, strategy_line: str, options: dict[str, Any], now: str) -> dict[str, Any]:
    eval_result = _ensure_real_evaluator_orders(conn, sandbox_id, strategy_line, options, now)
    branch_id = _branch_id(sandbox_id, strategy_line)
    lineage = _runtime_lineage(conn, sandbox_id, strategy_line, options)
    parameter_set_id = _register_parameter_set(conn, sandbox_id, strategy_line, options, now)
    run_id = _stable_id("fill", {"sandbox_id": sandbox_id, "branch_id": branch_id, "strategy_line": strategy_line, "options": options})
    rows = conn.execute(
        "SELECT * FROM sandbox_orders WHERE sandbox_id=? AND evaluator_run_id=? ORDER BY entry_time_ms ASC",
        (sandbox_id, eval_result["run_id"]),
    ).fetchall()
    filled: list[dict[str, Any]] = []
    params = _sandbox_params(strategy_line, options)
    for row in rows:
        features = _loads(row["features_json"], {})
        krows = features.pop("_sandbox_rows", None) or _synthetic_rows(row["symbol"])
        order = {
            "symbol": row["symbol"],
            "strategy_line": row["strategy_line"],
            "side": row["side"],
            "entry_time_ms": row["entry_time_ms"],
            "entry_idx": next((i for i, item in enumerate(krows) if int(item["open_time_ms"]) == int(row["entry_time_ms"])), 41),
            "entry_price": row["entry_price"],
            "stop_loss": row["stop_loss"],
            "take_profit": row["take_profit"],
            "planned_rr": row["planned_rr"],
            "cost_bps": params.get("taker_fee_bps", 5) * 2 + params.get("slippage_bps", 1),
            "features": features,
            "fast_exit_policy": {},
        }
        fill = simulate_1m_fill(order, krows, params)
        mfe_r, mae_r = _excursion(order, krows, fill)
        fill["MFE_R"] = mfe_r
        fill["MAE_R"] = mae_r
        conn.execute(
            """
            UPDATE sandbox_orders
            SET fill_run_id=?, exit_time_ms=?, exit_price=?, net_R=?, MFE_R=?, MAE_R=?,
                exit_reason=?, features_json=?, fill_result_json=?
            WHERE order_id=?
            """,
            (
                run_id,
                fill.get("exit_time_ms"),
                fill.get("exit_price"),
                fill.get("net_R"),
                mfe_r,
                mae_r,
                fill.get("exit_reason"),
                _json(features),
                _json(fill),
                row["order_id"],
            ),
        )
        filled.append(fill)
    metrics = p21_metrics(filled)
    conn.execute(
        """
        INSERT OR REPLACE INTO fill_model_runs(
          run_id, sandbox_id, assumption_id, same_candle_policy, status, metrics_json, evidence_json,
          created_at, branch_id, strategy_line, code_overlay_id, code_patch_id, runtime_id, parameter_set_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            sandbox_id,
            options.get("assumption_id", "paper_style_1m_no_intrabar_v1"),
            options.get("same_candle_policy", "pessimistic"),
            "real_replay_smoke_complete",
            _json(metrics),
            _json({"fill_model_not_strategy_logic": True, "source_evaluator_run_id": eval_result["run_id"], "code_lineage": lineage}),
            now,
            branch_id,
            strategy_line,
            lineage["code_overlay_id"],
            lineage["code_patch_id"],
            lineage["runtime_id"],
            parameter_set_id,
        ),
    )
    _update_branch_metrics(conn, sandbox_id, strategy_line, now)
    return {"run_id": run_id, "filled": len(filled), "metrics": metrics, "source_evaluator_run_id": eval_result["run_id"]}


def job_payload(
    sandbox_id: str,
    job_type: str,
    options: dict[str, Any] | None = None,
    *,
    root: Path | None = None,
    operation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    op = _operation_context("job", operation_context)
    _assert_operation_allowed(op)
    options = options or {}
    job_type = str(job_type or "").replace("-", "_")
    row = _assert_active_sandbox_writable(sandbox_id, root)
    db_path = Path(row["db_path"])
    now = _now()
    job_id = _stable_id("job", {"sandbox_id": sandbox_id, "job_type": job_type, "options": options, "now": now})
    result = _apply_job_side_effect(db_path, sandbox_id, job_type, options, now)
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        strategy_line = str(options.get("strategy_line") or "all")
        branch_id = None if strategy_line in {"", "all", "*"} else _branch_id(sandbox_id, strategy_line)
        lineage = (
            {"code_overlay_id": None, "code_patch_id": None, "runtime_id": None}
            if strategy_line in {"", "all", "*"}
            else _runtime_lineage(conn, sandbox_id, strategy_line, options)
        )
        parameter_set_id = None if strategy_line in {"", "all", "*"} else _parameter_set_id(strategy_line, options)
        conn.execute(
            """
            INSERT INTO sandbox_jobs(
              job_id, sandbox_id, job_type, status, progress_json, result_json, created_at, updated_at,
              branch_id, strategy_line, code_overlay_id, code_patch_id, runtime_id, parameter_set_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                sandbox_id,
                job_type,
                "completed",
                _json({"total_count": 1, "done_count": 1, "message": "contract scaffold completed"}),
                _json(result),
                now,
                now,
                branch_id,
                strategy_line,
                lineage["code_overlay_id"],
                lineage["code_patch_id"],
                lineage["runtime_id"],
                parameter_set_id,
            ),
        )
        conn.commit()
    with _registry_conn(root) as reg:
        reg.execute("UPDATE sandbox_registry SET last_job_status = ?, updated_at = ? WHERE sandbox_id = ?", ("completed", now, sandbox_id))
        reg.commit()
    training_dataset = sync_sandbox_job_result(
        _p29_project_root_from_sandbox_root(root),
        sandbox_db_path=db_path,
        sandbox_id=sandbox_id,
        job_id=job_id,
        job_type=job_type,
    )
    return _with_operation_payload(
        {
            "job_id": job_id,
            "sandbox_id": sandbox_id,
            "job_type": job_type,
            "status": "completed",
            "result": result,
            "training_dataset": training_dataset,
        },
        op,
    )


def _run_coarse_matrix(conn: sqlite3.Connection, sandbox_id: str, selected_lines: list[str], options: dict[str, Any], now: str) -> dict[str, Any]:
    matrix_profile = str(options.get("matrix_profile") or "coarse_pf_discovery")
    branch_results: list[dict[str, Any]] = []
    total_sets = 0
    for strategy_line in selected_lines:
        set_results: list[dict[str, Any]] = []
        for idx, params in enumerate(_coarse_parameter_sets(strategy_line, options), start=1):
            parameter_options = {
                **options,
                "strategy_line": strategy_line,
                "strategy_params": params,
                "matrix_profile": matrix_profile,
                "_matrix_parameter_set": True,
            }
            parameter_options["parameter_set_id"] = _parameter_set_id(strategy_line, parameter_options)
            result = _apply_branch_job_side_effect(
                conn,
                sandbox_id,
                strategy_line,
                "paper_shadow",
                parameter_options,
                now,
            )
            metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
            _register_parameter_set(
                conn,
                sandbox_id,
                strategy_line,
                parameter_options,
                now,
                status="completed",
                metrics=metrics,
            )
            set_results.append(
                {
                    "parameter_set_id": parameter_options["parameter_set_id"],
                    "strategy_line": strategy_line,
                    "params": params,
                    "metrics": metrics,
                    "config_candidate_id": result.get("config_candidate_id"),
                }
            )
            total_sets += 1
        branch_results.append(
            {
                "strategy_line": strategy_line,
                "branch_id": _branch_id(sandbox_id, strategy_line),
                "parameter_set_count": len(set_results),
                "parameter_sets": set_results,
            }
        )
    return {
        "note": "coarse matrix completed with parameter_set_id lineage",
        "sandbox_topology": "multi_strategy_single_sandbox",
        "matrix_profile": matrix_profile,
        "branch_count": len(branch_results),
        "parameter_set_count": total_sets,
        "branches": branch_results,
    }


def _apply_job_side_effect(db_path: Path, sandbox_id: str, job_type: str, options: dict[str, Any], now: str) -> dict[str, Any]:
    with _connect(db_path) as conn:
        ensure_sandbox_tables(conn)
        selected_lines = _selected_branch_lines(conn, sandbox_id, options)
        if job_type == "coarse_matrix":
            result = _run_coarse_matrix(conn, sandbox_id, selected_lines, options, now)
            conn.commit()
            return result
        if len(selected_lines) > 1:
            branch_results: list[dict[str, Any]] = []
            for line in selected_lines:
                branch_options = {**options, "strategy_line": line}
                branch_results.append(
                    {
                        "strategy_line": line,
                        "branch_id": _branch_id(sandbox_id, line),
                        "result": _apply_branch_job_side_effect(conn, sandbox_id, line, job_type, branch_options, now),
                    }
                )
            conn.commit()
            return {
                "note": "multi-branch sandbox job completed",
                "sandbox_topology": "multi_strategy_single_sandbox",
                "branch_count": len(branch_results),
                "branches": branch_results,
            }
        strategy_line = selected_lines[0]
        result = _apply_branch_job_side_effect(conn, sandbox_id, strategy_line, job_type, {**options, "strategy_line": strategy_line}, now)
        conn.commit()
        return result


def _apply_branch_job_side_effect(
    conn: sqlite3.Connection,
    sandbox_id: str,
    strategy_line: str,
    job_type: str,
    options: dict[str, Any],
    now: str,
) -> dict[str, Any]:
        branch_id = _branch_id(sandbox_id, strategy_line)
        lineage = _runtime_lineage(conn, sandbox_id, strategy_line, options)
        parameter_set_id = _register_parameter_set(conn, sandbox_id, strategy_line, options, now)
        if job_type == "backtest":
            result = _ensure_real_evaluator_orders(conn, sandbox_id, strategy_line, options, now)
            return {"note": "real evaluator adapter executed in sandbox", **result}
        if job_type == "replay":
            result = _ensure_replayed_fills(conn, sandbox_id, strategy_line, options, now)
            return {"note": "1m replay fill materialized in sandbox", **result}
        if job_type == "trade_quality":
            replay_result = _ensure_replayed_fills(conn, sandbox_id, strategy_line, options, now)
            rows = conn.execute(
                "SELECT * FROM sandbox_orders WHERE sandbox_id=? AND strategy_line=? AND fill_run_id=? AND net_R IS NOT NULL",
                (sandbox_id, strategy_line, replay_result["run_id"]),
            ).fetchall()
            inserted = 0
            root_counts: dict[str, int] = {}
            for row in rows:
                holding_sec = max(0, (int(row["exit_time_ms"] or row["entry_time_ms"]) - int(row["entry_time_ms"])) // 1000)
                metrics = {
                    "exit_reason": row["exit_reason"],
                    "net_R": row["net_R"],
                    "MFE_R": row["MFE_R"],
                    "MAE_R": row["MAE_R"],
                    "planned_RR": row["planned_rr"],
                    "cost_ratio_R": None,
                    "holding_sec": holding_sec,
                    "excursion_model": "sandbox_1m_replay",
                }
                root, confidence, evidence, secondary, manual = label_root_cause(metrics)
                root_counts[root] = root_counts.get(root, 0) + 1
                sample_id = _stable_id("tq", {"sandbox_id": sandbox_id, "order_id": row["order_id"]}, 24)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO trade_quality_samples(
                      sample_id, sandbox_id, trade_id, symbol, side, entry_time, exit_time,
                      net_R, MFE_R, MAE_R, root_cause, features_known_at_entry_json,
                      future_outcome_labels_json, source_ref, review_status, created_at,
                      branch_id, strategy_line, code_overlay_id, code_patch_id, runtime_id, parameter_set_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sample_id,
                        sandbox_id,
                        row["order_id"],
                        row["symbol"],
                        row["side"],
                        row["entry_time_ms"],
                        row["exit_time_ms"],
                        row["net_R"],
                        row["MFE_R"],
                        row["MAE_R"],
                        root,
                        row["features_json"],
                        _json({"root_cause_confidence": confidence, "root_cause_evidence": evidence, "secondary_labels": secondary, "manual_review": manual}),
                        f"sandbox:{sandbox_id}:order:{row['order_id']}",
                        "diagnostic_ready",
                        now,
                        branch_id,
                        strategy_line,
                        lineage["code_overlay_id"],
                        lineage["code_patch_id"],
                        lineage["runtime_id"],
                        parameter_set_id,
                    ),
                )
                inserted += 1
            _update_branch_metrics(conn, sandbox_id, strategy_line, now)
            return {"note": "real sandbox fills materialized to TQ", "samples": inserted, "root_cause_counts": root_counts, "replay": replay_result}
        if job_type == "gate_search":
            _apply_branch_job_side_effect(conn, sandbox_id, strategy_line, "trade_quality", options, now)
            sample_clause, sample_args = _query_param_clause(parameter_set_id)
            samples = conn.execute(
                f"SELECT * FROM trade_quality_samples WHERE sandbox_id=? AND strategy_line=?{sample_clause}",
                (sandbox_id, strategy_line, *sample_args),
            ).fetchall()
            root_counts = {str(row["root_cause"]): 0 for row in samples}
            for row in samples:
                root_counts[str(row["root_cause"])] = root_counts.get(str(row["root_cause"]), 0) + 1
            total = len(samples)
            losses = [row for row in samples if (row["net_R"] or 0) < 0]
            rule = {
                "mode": "shadow_only",
                "entry_known_features_only": True,
                "blocked_roots_observed": [key for key, count in root_counts.items() if count and key in {"signal_no_edge", "direction_wrong", "entered_too_early"}],
                "min_samples": total,
                "coverage": round(len(losses) / total, 8) if total else 0,
            }
            candidate_id = _stable_id(
                "gate",
                {
                    "sandbox_id": sandbox_id,
                    "branch_id": branch_id,
                    "strategy_line": strategy_line,
                    "parameter_set_id": parameter_set_id,
                    "rule": rule,
                },
            )
            pf = None
            rows = conn.execute(
                f"SELECT net_R FROM trade_quality_samples WHERE sandbox_id=? AND strategy_line=?{sample_clause}",
                (sandbox_id, strategy_line, *sample_args),
            ).fetchall()
            values = [float(row["net_R"] or 0.0) for row in rows]
            gp = sum(v for v in values if v > 0)
            gl = abs(sum(v for v in values if v < 0))
            if gl:
                pf = round(gp / gl, 8)
            conn.execute(
                """
                INSERT OR REPLACE INTO gate_candidates(
                  candidate_id, sandbox_id, rule_json, train_metrics_json, validation_metrics_json,
                  test_metrics_json, overfit_risk, status, created_at, branch_id, strategy_line,
                  code_overlay_id, code_patch_id, runtime_id, parameter_set_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate_id,
                    sandbox_id,
                    _json(rule),
                    _json({"profit_factor": pf, "sample_count": total}),
                    _json({"profit_factor": pf, "sample_count": total, "method": "smoke_same_sample"}),
                    _json({"profit_factor": pf, "sample_count": total, "method": "smoke_same_sample"}),
                    "medium_smoke_only",
                    "shadow_candidate",
                    now,
                    branch_id,
                    strategy_line,
                    lineage["code_overlay_id"],
                    lineage["code_patch_id"],
                    lineage["runtime_id"],
                    parameter_set_id,
                ),
            )
            _update_branch_metrics(conn, sandbox_id, strategy_line, now)
            return {"candidate_id": candidate_id, "status": "shadow_candidate", "profit_factor": pf, "sample_count": total}
        if job_type == "holdout":
            validation_id = _stable_id("val", {"sandbox_id": sandbox_id, "options": options})
            conn.execute(
                """
                INSERT OR REPLACE INTO holdout_validations(validation_id, sandbox_id, candidate_id, split_json, leakage_report_json, decision, created_at, branch_id, strategy_line)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    validation_id,
                    sandbox_id,
                    options.get("candidate_id"),
                    _json({"walk_forward": True, "train_validation_test": True}),
                    _json({"future_function_violations": 0, "status": "contract_only"}),
                    "needs_real_evidence",
                    now,
                    branch_id,
                    strategy_line,
                ),
            )
            return {"validation_id": validation_id}
        if job_type == "config_export":
            gate = _apply_branch_job_side_effect(conn, sandbox_id, strategy_line, "gate_search", options, now)
            config_candidate_id = _stable_id("cfg", {"sandbox_id": sandbox_id, "options": options})
            patch = options.get("patch") or {
                "sandbox_id": sandbox_id,
                "strategy_line": strategy_line,
                "trade_quality_gate": {
                    "enabled": True,
                    "mode": "shadow",
                    "candidate_id": gate.get("candidate_id"),
                },
            }
            conn.execute(
                """
                INSERT OR REPLACE INTO config_candidates(
                  config_candidate_id, sandbox_id, candidate_id, target_profile, patch_json, promotion_state,
                  review_json, created_at, branch_id, strategy_line, code_overlay_id, code_patch_id, runtime_id, parameter_set_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    config_candidate_id,
                    sandbox_id,
                    gate.get("candidate_id") or options.get("candidate_id"),
                    options.get("target_profile", "review_only"),
                    _json(patch),
                    "shadow_review",
                    _json({"approved": False, "production_mutation": False}),
                    now,
                    branch_id,
                    strategy_line,
                    lineage["code_overlay_id"],
                    lineage["code_patch_id"],
                    lineage["runtime_id"],
                    parameter_set_id,
                ),
            )
            tp_id = _stable_id("tpc", {"sandbox_id": sandbox_id, "cfg": config_candidate_id}, 24)
            order_clause, order_args = _query_param_clause(parameter_set_id)
            plans = [
                _loads(row["trade_plan_payload_json"], {})
                for row in conn.execute(
                    f"SELECT trade_plan_payload_json FROM sandbox_orders WHERE sandbox_id=? AND strategy_line=?{order_clause} LIMIT 20",
                    (sandbox_id, strategy_line, *order_args),
                ).fetchall()
            ]
            metrics = p21_metrics([
                _loads(row["fill_result_json"], {})
                for row in conn.execute(
                    f"SELECT fill_result_json FROM sandbox_orders WHERE sandbox_id=? AND strategy_line=? AND net_R IS NOT NULL{order_clause}",
                    (sandbox_id, strategy_line, *order_args),
                ).fetchall()
            ])
            conn.execute(
                """
                INSERT OR REPLACE INTO trade_plan_candidates(
                  trade_plan_candidate_id, sandbox_id, strategy_line, config_candidate_id,
                  candidate_reason, trade_plan_json, metrics_json, promotion_state, created_at,
                  code_overlay_id, code_patch_id, runtime_id, parameter_set_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tp_id,
                    sandbox_id,
                    strategy_line,
                    config_candidate_id,
                    "sandbox_real_chain_candidate",
                    _json({"plans": plans}),
                    _json(metrics),
                    "shadow_review",
                    now,
                    lineage["code_overlay_id"],
                    lineage["code_patch_id"],
                    lineage["runtime_id"],
                    parameter_set_id,
                ),
            )
            _update_branch_metrics(conn, sandbox_id, strategy_line, now)
            return {"config_candidate_id": config_candidate_id, "trade_plan_candidate_id": tp_id, "promotion_state": "shadow_review"}
        if job_type == "paper_shadow":
            cfg = _apply_branch_job_side_effect(conn, sandbox_id, strategy_line, "config_export", options, now)
            order_clause, order_args = _query_param_clause(parameter_set_id)
            fills = [
                _loads(row["fill_result_json"], {})
                for row in conn.execute(
                    f"SELECT fill_result_json FROM sandbox_orders WHERE sandbox_id=? AND strategy_line=? AND net_R IS NOT NULL{order_clause}",
                    (sandbox_id, strategy_line, *order_args),
                ).fetchall()
            ]
            metrics = p21_metrics(fills)
            shadow_id = _stable_id("shadow", {"sandbox_id": sandbox_id, "options": options})
            conn.execute(
                """
                INSERT OR REPLACE INTO paper_shadow_results(
                  shadow_id, sandbox_id, config_candidate_id, status, metrics_json, evidence_json,
                  created_at, branch_id, strategy_line, code_overlay_id, code_patch_id, runtime_id, parameter_set_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    shadow_id,
                    sandbox_id,
                    cfg.get("config_candidate_id") or options.get("config_candidate_id"),
                    "paper_shadow_smoke_complete",
                    _json({"profit_factor": metrics.get("profit_factor"), "closed_trades": metrics.get("trade_count"), **metrics}),
                    _json({"isolated_from_paper_main_ledger": True}),
                    now,
                    branch_id,
                    strategy_line,
                    lineage["code_overlay_id"],
                    lineage["code_patch_id"],
                    lineage["runtime_id"],
                    parameter_set_id,
                ),
            )
            _update_branch_metrics(conn, sandbox_id, strategy_line, now)
            return {"shadow_id": shadow_id, "metrics": metrics, "config_candidate_id": cfg.get("config_candidate_id")}
        if job_type == "llm_export":
            export_id = _stable_id("llm", {"sandbox_id": sandbox_id, "options": options})
            dataset_card = {
                "sandbox_id": sandbox_id,
                "allowed_for_training": False,
                "requires_sanitization": True,
                "requires_review": True,
                "split_leakage_check": "required",
            }
            conn.execute(
                """
                INSERT OR REPLACE INTO llm_dataset_exports(
                  export_id, sandbox_id, dataset_card_json, sample_count, leakage_status, export_path,
                  created_at, branch_id, strategy_line, code_overlay_id, code_patch_id, runtime_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    export_id,
                    sandbox_id,
                    _json(dataset_card | {"code_lineage": lineage}),
                    0,
                    "not_ready",
                    None,
                    now,
                    branch_id,
                    strategy_line,
                    lineage["code_overlay_id"],
                    lineage["code_patch_id"],
                    lineage["runtime_id"],
                ),
            )
            _update_branch_metrics(conn, sandbox_id, strategy_line, now)
            return {"export_id": export_id, "leakage_status": "not_ready"}
        return {"note": f"job_type {job_type} recorded without side effect"}
