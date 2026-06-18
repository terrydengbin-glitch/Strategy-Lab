"""STEP11.0 CLI/API-ready strategy pipeline orchestrator."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

from laoma_signal_engine.core.config_loader import EngineConfig
from laoma_signal_engine.core.exit_codes import (
    EXIT_CONFIG,
    EXIT_INTERNAL,
    EXIT_SUCCESS,
    EXIT_WAIT_UNTIL_READY_TIMEOUT,
)
from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import parse_iso_z, to_iso_z, utc_now
from laoma_signal_engine.decision.final_decisions import run_apply_final_decisions_from_trade_plans_safe
from laoma_signal_engine.decision.trade_plan_lines import load_trade_plan_line_config, run_apply_trade_plan_line_safe
from laoma_signal_engine.decision.trade_plan_lines import write_blocked_trade_plan_line
from laoma_signal_engine.decision.trade_plan_lines import write_blocked_micro_lifecycle_document
from laoma_signal_engine.decision.trade_plan_lines import write_failed_trade_plan_line
from laoma_signal_engine.decision.trade_plan_lines import write_micro_timeout_lifecycle_document
from laoma_signal_engine.micro.wait_until_ready.evaluate import normalize_symbol, scope_micro_to_expected_symbols
from laoma_signal_engine.decision.trade_plan_lines_audit import run_audit_trade_plan_lines_safe
from laoma_signal_engine.factors.factor_snapshot import (
    run_assemble_factor_snapshot_safe,
    run_assemble_factor_snapshot_without_ofi_cvd_safe,
)
from laoma_signal_engine.market.decision_refresh import run_pre_decision_candidate_refresh_safe
from laoma_signal_engine.micro.wait_until_ready.config import load_wait_until_ready_config
from laoma_signal_engine.micro.wait_until_ready.runner import run_wait_until_ready_orchestration
from laoma_signal_engine.micro.data_quality_attribution import write_micro_quality_attribution
from laoma_signal_engine.notifications.config import load_feishu_config
from laoma_signal_engine.notifications.service import send_trade_plan_notifications
from laoma_signal_engine.paper.config import load_paper_config
from laoma_signal_engine.paper.daemon import inspect_tick_lock, read_status as read_paper_status
from laoma_signal_engine.paper.daemon import run_once as run_paper_once
from laoma_signal_engine.pipeline import LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT, run_pipeline_pre_micro_safe
from laoma_signal_engine.audit.run_audit import (
    ingest_failed_pipeline_run_to_sqlite,
    ingest_run_audit_to_sqlite,
    write_run_level_audit,
)
from laoma_signal_engine.runtime_health import micro_daemon_health
from laoma_signal_engine.scheduler_5m import acquire_scheduler_lock, release_scheduler_lock
from laoma_signal_engine.scanner.current_freshness import build_step2_current_freshness
from laoma_signal_engine.strategy5.evidence import run_strategy5_pipeline_safe
from laoma_signal_engine.strategy6.evidence import run_strategy6_pipeline_safe

ConcreteStrategyLine = Literal["without_micro", "micro_fast", "micro_full", "strategy5", "strategy6"]
StrategyLine = Literal["without_micro", "micro_fast", "micro_full", "strategy5", "strategy6", "all"]
StrategyMode = Literal["once", "interval"]
STRATEGY_LINES: tuple[ConcreteStrategyLine, ...] = ("without_micro", "micro_fast", "micro_full", "strategy5", "strategy6")
LINE_TRADE_PLAN_PATHS: dict[str, str] = {
    "without_micro": "DATA/decisions/latest_trade_plan_without_micro.json",
    "micro_fast": "DATA/decisions/latest_trade_plan_micro_fast.json",
    "micro_full": "DATA/decisions/latest_trade_plan_micro_full.json",
    "strategy5": "DATA/decisions/latest_trade_plan_strategy5.json",
    "strategy6": "DATA/decisions/latest_trade_plan_strategy6.json",
}


@dataclass(frozen=True)
class StrategyPipelineOptions:
    project_root: Path
    line: StrategyLine = "all"
    selected_lines: tuple[ConcreteStrategyLine, ...] = STRATEGY_LINES
    mode: StrategyMode = "once"
    interval_sec: int = 300
    requested_interval_sec: int = 300
    line_runtime_budgets: dict[str, int] | None = None
    max_cycles: int | None = None
    overlap_policy: str = "skip"
    stdout_json: bool = False
    force_universe: bool = False
    light_limit: int = 0
    fetch_mode: str = LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT
    max_concurrency: int | None = None
    scan_allow_stale_input: bool = False
    skip_market_context: bool = False
    skip_micro_wait: bool = False
    run_abc_audit: bool = True
    run_json_stage_audit: bool = True
    aggregate_final_decisions: bool = True


def default_report_path(root: Path) -> Path:
    return root / "DATA" / "reports" / "latest_strategy_pipeline_report.json"


def _line_executable_symbols_for_run(root: Path, run_id: str, selected_lines: Sequence[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for line in selected_lines:
        line_key = str(line)
        rel = LINE_TRADE_PLAN_PATHS.get(line_key)
        if not rel:
            continue
        path = root / rel
        try:
            doc = read_json_object(path) if path.exists() else {}
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            doc = {}
        if not isinstance(doc, dict) or str(doc.get("run_id") or "") != run_id:
            out[line_key] = []
            continue
        symbols = [
            str(plan["symbol"]).upper()
            for plan in doc.get("plans") or []
            if isinstance(plan, dict) and plan.get("executable") is True and plan.get("symbol")
        ]
        out[line_key] = sorted(set(symbols))
    return out


def _paper_settlement_rows(root: Path, run_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    cfg = load_paper_config(root)
    db_path = root / cfg.db_path
    if not db_path.exists():
        return [], [], str(db_path)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            orders = [
                dict(row)
                for row in conn.execute(
                    "select * from paper_orders where source_run_id = ? limit 500",
                    (run_id,),
                ).fetchall()
            ]
            skips = [
                dict(row)
                for row in conn.execute(
                    "select * from paper_skip_ledger where source_run_id = ? and executable = 1 limit 500",
                    (run_id,),
                ).fetchall()
            ]
        return orders, skips, str(db_path)
    except sqlite3.Error:
        return [], [], str(db_path)


def _paper_db_row_counts_for_run(root: Path, run_id: str) -> dict[str, int]:
    cfg = load_paper_config(root)
    db_path = root / cfg.db_path
    tables = [
        "paper_intent_inbox",
        "paper_trade_plans",
        "paper_orders",
        "paper_skip_ledger",
        "paper_fills",
        "paper_positions",
    ]
    counts = {table: 0 for table in tables}
    if not db_path.exists():
        return counts
    try:
        with sqlite3.connect(db_path) as conn:
            for table in tables:
                try:
                    row = conn.execute(f"select count(*) from {table} where source_run_id = ?", (run_id,)).fetchone()
                    counts[table] = int(row[0]) if row else 0
                except sqlite3.Error:
                    counts[table] = 0
    except sqlite3.Error:
        return counts
    return counts


def _paper_runtime_snapshot(root: Path) -> dict[str, Any]:
    cfg = load_paper_config(root)
    try:
        status = read_paper_status(root, cfg)
    except Exception as exc:
        status = {"status": "error", "last_error": str(exc)}
    try:
        tick_lock = inspect_tick_lock(root, cfg)
    except Exception as exc:
        tick_lock = {"status": "error", "error": str(exc)}
    return {"daemon_status": status, "tick_lock": tick_lock}


def _paper_once_needs_alive_lock_retry(result: dict[str, Any]) -> bool:
    reason = str(result.get("reason") or "")
    tick_lock = result.get("tick_lock") if isinstance(result.get("tick_lock"), dict) else {}
    codes = result.get("reason_codes") if isinstance(result.get("reason_codes"), list) else []
    tick_codes = tick_lock.get("reason_codes") if isinstance(tick_lock.get("reason_codes"), list) else []
    return (
        str(result.get("status") or "") == "skipped"
        and (
            reason == "paper_tick_lock_alive_busy"
            or "paper_tick_lock_alive_busy" in codes
            or "paper_tick_lock_alive_busy" in tick_codes
        )
    )


def _run_paper_once_with_tick_retry(root: Path, *, max_wait_sec: float = 75.0) -> dict[str, Any]:
    cfg = load_paper_config(root)
    first = run_paper_once(root, config=cfg)
    if not isinstance(first, dict) or not _paper_once_needs_alive_lock_retry(first):
        return first
    attempts = 0
    started = time.monotonic()
    last_lock: dict[str, Any] | None = first.get("tick_lock") if isinstance(first.get("tick_lock"), dict) else None
    while time.monotonic() - started < max_wait_sec:
        attempts += 1
        time.sleep(min(2.0, 0.35 * attempts))
        last_lock = inspect_tick_lock(root, cfg)
        if last_lock.get("exists") and last_lock.get("pid_alive"):
            continue
        retried = run_paper_once(root, config=cfg)
        if isinstance(retried, dict):
            reason_codes = retried.get("reason_codes") if isinstance(retried.get("reason_codes"), list) else []
            retried["reason_codes"] = sorted(set([*reason_codes, "paper_inline_wakeup_retried"]))
            retried["inline_retry"] = {
                "enabled": True,
                "attempts": attempts,
                "first_reason": first.get("reason"),
                "first_tick_lock": first.get("tick_lock"),
                "last_tick_lock_before_retry": last_lock,
                "waited_sec": round(time.monotonic() - started, 3),
            }
        return retried
    reason_codes = first.get("reason_codes") if isinstance(first.get("reason_codes"), list) else []
    first["reason_codes"] = sorted(set([*reason_codes, "paper_inline_wakeup_missing_after_retry"]))
    first["inline_retry"] = {
        "enabled": True,
        "attempts": attempts,
        "first_reason": first.get("reason"),
        "first_tick_lock": first.get("tick_lock"),
        "last_tick_lock_before_retry": last_lock,
        "waited_sec": round(time.monotonic() - started, 3),
        "status": "exhausted",
    }
    return first


def _run_paper_settlement_barrier(root: Path, report: dict[str, Any]) -> dict[str, Any]:
    run_id = str(report.get("run_id") or "")
    cycle_id = str(report.get("cycle_id") or "")
    selected_lines = [str(line) for line in (report.get("selected_lines") or STRATEGY_LINES)]
    payload: dict[str, Any] = {
        "schema_version": "7.47",
        "source": "paper_settlement_barrier",
        "run_id": run_id,
        "cycle_id": cycle_id,
        "selected_lines": selected_lines,
        "started_at": to_iso_z(utc_now()),
    }
    executables_by_line = _line_executable_symbols_for_run(root, run_id, selected_lines)
    executable_pairs = {
        (line, symbol)
        for line, symbols in executables_by_line.items()
        for symbol in symbols
    }
    payload["executables_by_line"] = executables_by_line
    payload["executable_count"] = len(executable_pairs)
    payload["paper_runtime_before"] = _paper_runtime_snapshot(root)
    if not executable_pairs:
        payload.update(
            {
                "status": "no_executable",
                "settled_at": to_iso_z(utc_now()),
                "missing_by_line": {line: [] for line in selected_lines},
                "order_count": 0,
                "skip_count": 0,
            },
        )
        return payload

    try:
        paper_result = _run_paper_once_with_tick_retry(root)
        consume = paper_result.get("consume") if isinstance(paper_result, dict) else {}
        summary = paper_result.get("summary") if isinstance(paper_result, dict) else {}
        reason_codes = paper_result.get("reason_codes") if isinstance(paper_result, dict) else []
        payload["paper_run_once"] = {
            "status": paper_result.get("status") if isinstance(paper_result, dict) else None,
            "reason": paper_result.get("reason") if isinstance(paper_result, dict) else None,
            "reason_codes": reason_codes if isinstance(reason_codes, list) else [],
            "inline_retry": paper_result.get("inline_retry") if isinstance(paper_result, dict) else None,
            "tick_lock": paper_result.get("tick_lock") if isinstance(paper_result, dict) else None,
            "summary_generated_at": summary.get("generated_at") if isinstance(summary, dict) else None,
            "consume": consume if isinstance(consume, dict) else {},
        }
    except Exception as exc:  # pragma: no cover - defensive around runtime IO.
        payload.update(
            {
                "status": "error",
                "error": str(exc),
                "reason_codes": ["paper_settlement_barrier_run_once_failed"],
                "finished_at": to_iso_z(utc_now()),
            },
        )
        return payload

    orders, skips, db_path = _paper_settlement_rows(root, run_id)
    payload["paper_db_path"] = db_path
    payload["paper_runtime_after"] = _paper_runtime_snapshot(root)
    payload["db_rows_by_table"] = _paper_db_row_counts_for_run(root, run_id)
    payload["order_count"] = len(orders)
    payload["skip_count"] = len(skips)
    settled_pairs = {
        (str(row.get("strategy_line") or "").lower(), str(row.get("symbol") or "").upper())
        for row in [*orders, *skips]
        if row.get("strategy_line") and row.get("symbol")
    }
    missing_by_line: dict[str, list[str]] = {}
    for line, symbols in executables_by_line.items():
        missing_by_line[line] = [symbol for symbol in symbols if (line, symbol) not in settled_pairs]
    payload["missing_by_line"] = missing_by_line
    payload["missing_count"] = sum(len(symbols) for symbols in missing_by_line.values())
    payload["status"] = "complete" if payload["missing_count"] == 0 else "missing_after_settlement"
    missing_reason_codes: list[str] = []
    paper_once = payload.get("paper_run_once") if isinstance(payload.get("paper_run_once"), dict) else {}
    if payload["missing_count"] > 0:
        once_status = str(paper_once.get("status") or "")
        once_reason = str(paper_once.get("reason") or "")
        once_codes = paper_once.get("reason_codes") if isinstance(paper_once.get("reason_codes"), list) else []
        if once_status == "skipped":
            missing_reason_codes.append("paper_run_once_skipped")
        if once_reason:
            missing_reason_codes.append(once_reason)
        missing_reason_codes.extend(str(code) for code in once_codes if code)
        if once_reason.startswith("paper_tick_lock") or any(str(code).startswith("paper_tick_lock") for code in once_codes):
            missing_reason_codes.append("paper_settlement_missing_after_tick_lock_failure")
        if not missing_reason_codes:
            missing_reason_codes.append("paper_settlement_unknown_missing")
    payload["missing_reason_codes"] = sorted(set(missing_reason_codes))
    payload["missing_detail_by_line"] = {
        line: [
            {
                "symbol": symbol,
                "reason_codes": payload["missing_reason_codes"],
                "paper_run_once_status": paper_once.get("status"),
                "paper_run_once_reason": paper_once.get("reason"),
            }
            for symbol in symbols
        ]
        for line, symbols in missing_by_line.items()
    }
    payload["settled_at"] = to_iso_z(utc_now())
    return payload


def _run_paper_wakeup_after_trade_plan(root: Path, line: str, *, run_id: str) -> dict[str, Any]:
    executables = _line_executable_symbols_for_run(root, run_id, [line]).get(line, [])
    try:
        result = _run_paper_once_with_tick_retry(root)
    except Exception as exc:
        return {
            "rc": EXIT_INTERNAL,
            "paper_wakeup_effective": "error",
            "error": str(exc),
            "executable_symbols": executables,
            "executable_count": len(executables),
            "reason_codes": ["paper_run_once_exception"],
        }
    if not isinstance(result, dict):
        return {
            "rc": EXIT_INTERNAL,
            "paper_wakeup_effective": "error",
            "executable_symbols": executables,
            "executable_count": len(executables),
            "reason_codes": ["paper_run_once_invalid_result"],
        }
    status = str(result.get("status") or "ok")
    consume = result.get("consume") if isinstance(result.get("consume"), dict) else {}
    created = int(consume.get("created") or 0) if consume else 0
    skipped = len(consume.get("skipped") or []) if consume else 0
    reason_codes = result.get("reason_codes") if isinstance(result.get("reason_codes"), list) else []
    if status == "skipped":
        effective = "skipped"
        rc = EXIT_INTERNAL if executables else EXIT_SUCCESS
    elif status == "error":
        effective = "error"
        rc = EXIT_INTERNAL
    elif created > 0 or skipped > 0:
        effective = "consumed"
        rc = EXIT_SUCCESS
    else:
        effective = "no_executable" if not executables else "no_current_rows"
        rc = EXIT_SUCCESS
    return {
        "rc": rc,
        "paper_run_once_status": status,
        "paper_run_once_reason": result.get("reason"),
        "paper_run_once_consume": consume,
        "paper_wakeup_effective": effective,
        "executable_symbols": executables,
        "executable_count": len(executables),
        "tick_lock": result.get("tick_lock"),
        "inline_retry": result.get("inline_retry"),
        "reason_codes": reason_codes,
    }


def _pipeline_run_report_dir(root: Path, run_id: str) -> Path:
    return root / "DATA" / "reports" / "pipeline_runs" / run_id


def _first_failed_stage(stages: Sequence[dict[str, Any]]) -> dict[str, Any] | None:
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        status = str(stage.get("status") or "").lower()
        if stage.get("ok") is False or status in {"failed", "blocked", "error", "exception"} or stage.get("error"):
            return stage
    return None


def _enrich_pipeline_report_failure_metadata(root: Path, report: dict[str, Any], *, latest_report_path: Path) -> None:
    run_id = str(report.get("run_id") or "")
    if not run_id:
        return
    run_dir = _pipeline_run_report_dir(root, run_id)
    archive_path = run_dir / "strategy_pipeline_report.json"
    failed_stage_path = run_dir / "strategy_pipeline_failed_stage.json"
    outputs = report.setdefault("outputs", {})
    if isinstance(outputs, dict):
        outputs["strategy_report"] = str(latest_report_path)
        outputs["pipeline_report_archive"] = str(archive_path)
        if str(report.get("status") or "") == "failed":
            outputs["pipeline_failed_stage_archive"] = str(failed_stage_path)
    failed_stage = _first_failed_stage(report.get("stages") if isinstance(report.get("stages"), list) else [])
    if failed_stage:
        report["first_failed_stage"] = failed_stage.get("name")
        report["first_failed_stage_status"] = str(failed_stage.get("status") or "failed")
        report["first_failed_stage_rc"] = failed_stage.get("rc")
        report["first_failed_stage_reason_codes"] = (failed_stage.get("detail") or {}).get("reason_codes") if isinstance(failed_stage.get("detail"), dict) else []
        report["failure_reason"] = failed_stage.get("error") or failed_stage.get("detail") or f"stage_failed:{failed_stage.get('name')}"
        report["failure_domain"] = "pipeline_stage"


def _write_pipeline_report_archive(root: Path, report: dict[str, Any]) -> None:
    run_id = str(report.get("run_id") or "")
    if not run_id:
        return
    run_dir = _pipeline_run_report_dir(root, run_id)
    write_json_atomic(run_dir / "strategy_pipeline_report.json", report)
    if str(report.get("status") or "") == "failed":
        failed_stage = _first_failed_stage(report.get("stages") if isinstance(report.get("stages"), list) else []) or {}
        write_json_atomic(
            run_dir / "strategy_pipeline_failed_stage.json",
            {
                "schema_version": "1.0",
                "source": "strategy_pipeline_failed_stage",
                "run_id": report.get("run_id"),
                "cycle_id": report.get("cycle_id"),
                "status": "failed",
                "first_failed_stage": report.get("first_failed_stage"),
                "first_failed_stage_status": report.get("first_failed_stage_status"),
                "first_failed_stage_rc": report.get("first_failed_stage_rc"),
                "failure_domain": report.get("failure_domain"),
                "failure_reason": report.get("failure_reason"),
                "stage": failed_stage,
                "generated_at": to_iso_z(utc_now()),
            },
        )


def _make_run_id() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def normalize_strategy_lines(
    *,
    line: StrategyLine | str | None = None,
    lines: list[str] | tuple[str, ...] | None = None,
) -> tuple[ConcreteStrategyLine, ...]:
    requested = [str(item).strip() for item in (lines or []) if str(item).strip()]
    if not requested:
        requested = [str(line or "all")]
    if any(item == "all" for item in requested):
        return STRATEGY_LINES
    allowed = set(STRATEGY_LINES)
    normalized: list[ConcreteStrategyLine] = []
    for item in requested:
        if item not in allowed:
            raise ValueError(f"invalid strategy line: {item}")
        if item not in normalized:
            normalized.append(item)  # type: ignore[arg-type]
    if not normalized:
        raise ValueError("at least one strategy line must be selected")
    return tuple(line for line in STRATEGY_LINES if line in normalized)


def _display_line_for_selection(selected_lines: tuple[ConcreteStrategyLine, ...]) -> StrategyLine:
    if selected_lines == STRATEGY_LINES:
        return "all"
    if len(selected_lines) == 1:
        return selected_lines[0]
    return "all"


def _line_runtime_budget_sec(cfg: EngineConfig, line: str) -> int:
    buffer_sec = 60
    base = max(60, int(cfg.strategy_pipeline_interval_sec))
    if line == "micro_fast":
        return max(base, int(cfg.strategy_pipeline_wait_fast_sec) + buffer_sec)
    if line == "micro_full":
        return max(base, int(cfg.strategy_pipeline_wait_full_sec) + buffer_sec)
    return base


def _duration_aware_interval(
    *,
    cfg: EngineConfig,
    selected_lines: tuple[ConcreteStrategyLine, ...],
    requested_interval_sec: int,
) -> tuple[int, dict[str, int]]:
    budgets = {line: _line_runtime_budget_sec(cfg, line) for line in selected_lines}
    return int(requested_interval_sec), budgets


def _factor_path(root: Path, line: StrategyLine) -> Path:
    if line == "without_micro":
        return root / "DATA" / "factors" / "latest_factor_snapshot_withoutoficvd.json"
    if line in {"strategy5", "strategy6"}:
        return root / "DATA" / "factors" / "latest_factor_snapshot_withoutoficvd.json"
    return root / "DATA" / "factors" / "latest_factor_snapshot.json"


def _json_audit_path(root: Path) -> Path:
    return root / "DATA" / "reports" / "latest_current_json_chain_audit_summary.json"


def _run_json_stage_audit(root: Path) -> int:
    script = root / "scripts" / "audit_current_json_chain.py"
    if not script.is_file():
        return EXIT_CONFIG
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--project-root",
            str(root),
            "--audit-mode",
            "full_chain",
            "--output",
            str(_json_audit_path(root)),
            "--stdout-json",
        ],
        cwd=str(root),
        check=False,
    )
    return int(completed.returncode)


def _append_micro_quality_attribution_stage(
    stages: list[dict[str, Any]],
    *,
    root: Path,
    run_id: str,
    cycle_id: str,
    selected_lines: Sequence[str] | None = None,
    on_update: Callable[[str, str], None] | None = None,
) -> dict[str, Any]:
    name = "micro_quality_attribution"
    if on_update:
        on_update(name, "running")
    t0 = time.monotonic()
    try:
        payload = write_micro_quality_attribution(
            root,
            expected_run_id=run_id,
            expected_cycle_id=cycle_id,
            selected_lines=tuple(selected_lines or ()),
        )
        detail = {
            "status": payload.get("status") or "ok",
            "run_id": payload.get("run_id"),
            "cycle_id": payload.get("cycle_id"),
            "reason_codes": payload.get("reason_codes") or [],
            "symbol_count": len(payload.get("symbols") or []),
            "report_path": payload.get("report_path"),
            "findings_path": payload.get("findings_path"),
        }
        stages.append(
            {
                "name": name,
                "ok": True,
                "rc": EXIT_SUCCESS,
                "duration_sec": round(time.monotonic() - t0, 3),
                "detail": detail,
            }
        )
        if on_update:
            on_update(name, "done")
        return detail
    except Exception as exc:
        detail = {
            "status": "failed_non_blocking",
            "run_id": run_id,
            "cycle_id": cycle_id,
            "reason_codes": ["micro_quality_attribution_failed"],
            "error": str(exc),
        }
        stages.append(
            {
                "name": name,
                "ok": True,
                "rc": EXIT_SUCCESS,
                "duration_sec": round(time.monotonic() - t0, 3),
                "detail": detail,
            }
        )
        if on_update:
            on_update(name, "done")
        return detail


def _run_trade_plan_feishu_delivery(root: Path, *, line: str | None = None) -> int:
    cfg = load_feishu_config(root)
    result = send_trade_plan_notifications(root, config=cfg, line=line)
    status = str(result.get("status") or "")
    if status == "failed" and cfg.block_pipeline_on_total_failure:
        return EXIT_CONFIG
    if status == "partial" and cfg.block_pipeline_on_partial_failure:
        return EXIT_CONFIG
    return EXIT_SUCCESS


def _progress_path(root: Path) -> Path:
    return root / "DATA" / "runtime" / "strategy_pipeline_progress.json"


def _line_stage_percent(line: str, stage_name: str, status: str) -> int:
    if status == "done":
        done_values = {
            "without_micro": {
                "assemble_factor_without_micro": 35,
                "refresh_without_micro": 75,
                "apply_trade_plan_without_micro": 100,
                "notify_feishu_without_micro": 100,
            },
            "micro_fast": {
                "micro_inline_recovery_micro_fast": 20,
                "micro_recovery_success_continue_micro_fast": 25,
                "wait_micro_ready_micro_fast": 35,
                "assemble_factor_with_micro": 55,
                "refresh_micro_fast": 85,
                "apply_trade_plan_micro_fast": 100,
                "notify_feishu_micro_fast": 100,
            },
            "micro_full": {
                "micro_inline_recovery_micro_full": 20,
                "micro_recovery_success_continue_micro_full": 25,
                "wait_micro_ready_micro_full": 35,
                "assemble_factor_with_micro": 55,
                "refresh_micro_full": 85,
                "apply_trade_plan_micro_full": 100,
                "notify_feishu_micro_full": 100,
            },
            "strategy5": {
                "apply_trade_plan_strategy5": 100,
                "paper_wakeup_strategy5": 100,
            },
            "strategy6": {
                "apply_trade_plan_strategy6": 100,
                "paper_wakeup_strategy6": 100,
            },
        }
        return done_values.get(line, {}).get(stage_name, 0)
    running_values = {
        "without_micro": {
            "assemble_factor_without_micro": 18,
            "refresh_without_micro": 55,
            "apply_trade_plan_without_micro": 90,
            "notify_feishu_without_micro": 98,
        },
        "micro_fast": {
            "micro_health_recovering_micro_fast": 18,
            "micro_inline_recovery_micro_fast": 20,
            "micro_recovery_success_continue_micro_fast": 25,
            "wait_micro_ready_micro_fast": 20,
            "assemble_factor_with_micro": 45,
            "refresh_micro_fast": 70,
            "apply_trade_plan_micro_fast": 92,
            "notify_feishu_micro_fast": 98,
        },
        "micro_full": {
            "micro_health_recovering_micro_full": 18,
            "micro_inline_recovery_micro_full": 20,
            "micro_recovery_success_continue_micro_full": 25,
            "wait_micro_ready_micro_full": 20,
            "assemble_factor_with_micro": 45,
            "refresh_micro_full": 70,
            "apply_trade_plan_micro_full": 92,
            "notify_feishu_micro_full": 98,
        },
        "strategy5": {
            "apply_trade_plan_strategy5": 70,
            "paper_wakeup_strategy5": 95,
        },
        "strategy6": {
            "apply_trade_plan_strategy6": 70,
            "paper_wakeup_strategy6": 95,
        },
    }
    return running_values.get(line, {}).get(stage_name, 0)


def _write_pipeline_progress(
    *,
    root: Path,
    run_id: str,
    cycle_id: str,
    line: StrategyLine,
    mode: StrategyMode,
    started_at: str,
    status: str,
    current_stage: str | None,
    current_line: str | None,
    lines: dict[str, dict[str, Any]],
    stages: list[dict[str, Any]],
    selected_lines: tuple[ConcreteStrategyLine, ...] = STRATEGY_LINES,
    requested_interval_sec: int | None = None,
    effective_interval_sec: int | None = None,
    line_runtime_budgets: dict[str, int] | None = None,
) -> None:
    known_lines = STRATEGY_LINES
    selected_set = set(selected_lines)
    normalized: dict[str, dict[str, Any]] = {}
    for item in known_lines:
        raw = lines.get(item, {})
        selected = bool(raw.get("selected", item in selected_set))
        skipped = bool(raw.get("skipped", not selected))
        normalized[item] = {
            "percent": max(0, min(100, int(raw.get("percent") or 0))),
            "stage": str(raw.get("stage") or "waiting"),
            "done": bool(raw.get("done")),
            "selected": selected,
            "skipped": skipped,
            "run_id": run_id if raw.get("done") else raw.get("run_id"),
            "cycle_id": cycle_id if raw.get("done") else raw.get("cycle_id"),
            "output_fresh": bool(raw.get("output_fresh")),
        }
        for key in (
            "line_exec_status",
            "line_lifecycle_status",
            "wait_result",
            "stage_status_class",
            "business_terminal_reason",
            "technical_failure_reason",
            "technical_blocked",
            "technical_block_reason",
            "recovery",
            "line_lifecycle_complete",
            "trade_plan_allowed",
            "terminalized_symbol_count",
            "unfinished_symbol_count",
            "consumable_symbol_count",
            "rejected_count",
            "not_ready_count",
            "timeout_count",
            "observing_count",
            "ready_source_counts",
            "symbol_counts",
        ):
            if key in raw:
                normalized[item][key] = raw.get(key)
    progress_rows = [row for row in normalized.values() if row.get("selected")]
    if not progress_rows:
        progress_rows = list(normalized.values())
    overall = int(round(sum(row["percent"] for row in progress_rows) / len(progress_rows)))
    payload = {
        "schema_version": "1.0",
        "source": "strategy_pipeline_progress",
        "run_id": run_id,
        "cycle_id": cycle_id,
        "line": line,
        "selected_lines": list(selected_lines),
        "skipped_lines": [item for item in known_lines if item not in selected_set],
        "requested_interval_sec": requested_interval_sec,
        "effective_interval_sec": effective_interval_sec,
        "post_run_cooldown_sec": effective_interval_sec,
        "interval_semantics": "post_run_cooldown",
        "line_runtime_budgets": line_runtime_budgets or {},
        "mode": mode,
        "status": status,
        "started_at": started_at,
        "updated_at": to_iso_z(utc_now()),
        "current_stage": current_stage,
        "current_line": current_line,
        "overall_percent": max(0, min(100, overall)),
        "lines": normalized,
        "stages": stages,
    }
    write_json_atomic(_progress_path(root), payload)


def _micro_ready_source_counts(
    *,
    latest: dict[str, Any],
    targets: dict[str, Any],
    line: StrategyLine,
) -> dict[str, int]:
    target_meta: dict[str, dict[str, Any]] = {}
    for key in ("tier1_warm_watch", "tier2_active_strong"):
        rows = targets.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            sym = normalize_symbol(str(row.get("symbol") or ""))
            if sym:
                target_meta[sym] = row

    counts = {"current_cycle": 0, "sticky_ready_cache": 0, "unknown": 0}
    items = latest.get("items")
    if not isinstance(items, list):
        return counts
    for item in items:
        if not isinstance(item, dict):
            continue
        quality_key = "micro_full_quality" if line == "micro_full" else "micro_quality"
        quality = item.get(quality_key)
        if not isinstance(quality, dict) or quality.get("ready") is not True:
            if line == "micro_fast":
                fast_quality = item.get("micro_fast_quality")
                if not isinstance(fast_quality, dict) or fast_quality.get("ready") is not True:
                    continue
            else:
                continue
        sym = normalize_symbol(str(item.get("symbol") or ""))
        meta = target_meta.get(sym, {})
        sticky_source = str(meta.get("sticky_source") or "")
        retained_reason = str(meta.get("retained_reason") or "")
        if sticky_source == "daemon_state" or retained_reason == "ready_cache":
            counts["sticky_ready_cache"] += 1
        elif meta:
            counts["current_cycle"] += 1
        else:
            counts["unknown"] += 1
    return counts


def _micro_line_wait_contract(detail: dict[str, Any]) -> dict[str, Any]:
    line = str(detail.get("line") or "")
    target_count = max(0, int(detail.get("target_count") or detail.get("expected_symbol_count") or 0))
    ready_count = max(0, int(detail.get("ready_count") or 0))
    fast_ready_count = max(0, int(detail.get("fast_ready_count") or 0))
    full_ready_count = max(0, int(detail.get("full_ready_count") or 0))
    if line == "micro_full":
        usable_ready_count = full_ready_count
    elif line == "micro_fast":
        usable_ready_count = fast_ready_count
    else:
        usable_ready_count = ready_count
    confirmed_count = max(0, int(detail.get("confirmed_ready_count") or 0))
    consumable_count = max(0, int(detail.get("consumable_ready_count") or 0))
    if line == "micro_fast":
        usable_ready_count = consumable_count
    missing_count = len(detail.get("missing_target_symbols") or []) if isinstance(detail.get("missing_target_symbols"), list) else 0
    unfinished_symbol_count = max(0, target_count - usable_ready_count)
    symbol_counts = {
        "target": target_count,
        "ready": ready_count,
        "fast_ready": fast_ready_count,
        "full_ready": full_ready_count,
        "usable_ready": usable_ready_count,
        "quality_ready": fast_ready_count if line == "micro_fast" else ready_count,
        "confirmed": confirmed_count,
        "consumable": consumable_count,
        "unfinished": unfinished_symbol_count,
        "missing": missing_count,
    }
    if usable_ready_count > 0 and unfinished_symbol_count > 0:
        exec_status = "usable_partial"
        lifecycle_status = "partial_ready"
        wait_result = "partial_ready"
        stage_status_class = "business_partial_consumable"
        business_terminal_reason = "partial_consumable_symbols"
        technical_failure_reason = ""
    elif usable_ready_count > 0:
        exec_status = "usable_all_ready"
        lifecycle_status = "completed_all_symbols"
        wait_result = "ready"
        stage_status_class = "completed_with_consumable"
        business_terminal_reason = "consumable_symbols_ready"
        technical_failure_reason = ""
    elif target_count > 0:
        exec_status = "no_confirmed" if line == "micro_fast" and fast_ready_count > 0 else "no_ready"
        lifecycle_status = "quality_ready_no_consumable" if line == "micro_fast" and fast_ready_count > 0 else "observing"
        wait_result = "quality_ready_but_no_confirmed_symbol" if line == "micro_fast" and fast_ready_count > 0 else "observing"
        stage_status_class = "business_no_signal"
        business_terminal_reason = exec_status
        technical_failure_reason = ""
    else:
        exec_status = "blocked"
        lifecycle_status = "no_targets"
        wait_result = "no_targets"
        stage_status_class = "business_no_signal"
        business_terminal_reason = "no_targets"
        technical_failure_reason = ""
    return {
        "stage_status_class": stage_status_class,
        "business_terminal_reason": business_terminal_reason,
        "technical_failure_reason": technical_failure_reason,
        "line_exec_status": exec_status,
        "line_lifecycle_status": lifecycle_status,
        "wait_result": wait_result,
        "line_lifecycle_complete": unfinished_symbol_count == 0 and target_count > 0,
        "trade_plan_allowed": usable_ready_count > 0,
        "quality_ready_count": fast_ready_count if line == "micro_fast" else ready_count,
        "confirmed_ready_count": confirmed_count,
        "consumable_ready_count": consumable_count,
        "unfinished_symbol_count": unfinished_symbol_count,
        "symbol_counts": symbol_counts,
    }


def _micro_signal_consumable_counts(items: Any, *, line: StrategyLine) -> dict[str, Any]:
    if not isinstance(items, list) or line not in {"micro_fast", "micro_full"}:
        return {
            "quality_ready_count": 0,
            "confirmed_ready_count": 0,
            "consumable_ready_count": 0,
            "quality_ready_symbols": [],
            "confirmed_symbols": [],
            "consumable_symbols": [],
        }
    quality_key = "micro_fast_quality" if line == "micro_fast" else "micro_full_quality"
    signal_key = "micro_fast_signal" if line == "micro_fast" else "micro_full_signal"
    quality_symbols: list[str] = []
    confirmed_symbols: list[str] = []
    consumable_symbols: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        sym = normalize_symbol(str(item.get("symbol") or ""))
        if not sym:
            continue
        quality = item.get(quality_key)
        signal = item.get(signal_key)
        if isinstance(quality, dict) and quality.get("ready") is True:
            quality_symbols.append(sym)
        if isinstance(signal, dict) and signal.get("micro_direction_confirmed") is True:
            confirmed_symbols.append(sym)
        if isinstance(signal, dict) and signal.get("micro_exec_allowed") is True and _micro_signal_aligned_frame_ok(item, line=line):
            consumable_symbols.append(sym)
    return {
        "quality_ready_count": len(quality_symbols),
        "confirmed_ready_count": len(confirmed_symbols),
        "consumable_ready_count": len(consumable_symbols),
        "quality_ready_symbols": sorted(quality_symbols),
        "confirmed_symbols": sorted(confirmed_symbols),
        "consumable_symbols": sorted(consumable_symbols),
    }


def _micro_signal_aligned_frame_ok(item: dict[str, Any], *, line: StrategyLine) -> bool:
    if line != "micro_fast":
        return True
    quality = item.get("micro_fast_quality") if isinstance(item, dict) else None
    if not isinstance(quality, dict):
        return True
    reasons = {str(x) for x in quality.get("reason_codes") or []}
    if reasons.intersection({"cvd_stale", "ofi_stale", "ofi_cvd_lag_high", "cvd_never_updated", "ofi_never_updated"}):
        return False
    cvd_ts = quality.get("last_cvd_update_bucket_ts_sec")
    ofi_ts = quality.get("last_ofi_update_bucket_ts_sec")
    lag = quality.get("ofi_cvd_lag_bucket_sec")
    has_frame_evidence = any(
        key in quality
        for key in (
            "last_cvd_update_bucket_ts_sec",
            "last_ofi_update_bucket_ts_sec",
            "last_processed_bucket_ts_sec",
            "ofi_cvd_lag_bucket_sec",
        )
    )
    if not has_frame_evidence:
        return True
    if cvd_ts is None or ofi_ts is None:
        return False
    try:
        lag_num = abs(float(lag if lag is not None else float(cvd_ts) - float(ofi_ts)))
    except (TypeError, ValueError):
        return False
    return lag_num <= 30


def _micro_lifecycle_progress_reconcile(
    *,
    root: Path,
    line: str,
    run_id: str,
    cycle_id: str,
) -> dict[str, Any] | None:
    if line not in {"micro_fast", "micro_full"}:
        return None
    path = root / "DATA" / "micro" / f"latest_micro_lifecycle_{line}.json"
    try:
        doc = read_json_object(path)
    except (OSError, ValueError, TypeError):
        return None
    if doc.get("run_id") != run_id or doc.get("cycle_id") != cycle_id:
        return None
    items = doc.get("items")
    if not isinstance(items, list):
        return None

    state_counts: dict[str, int] = {}
    ready_count = 0
    confirmed_count = 0
    emitted_count = 0
    consumable_count = 0
    terminalized_count = 0
    unfinished_count = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        state = str(item.get("state") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1
        if item.get("ready") is True:
            ready_count += 1
        if item.get("confirmed") is True or state == "confirmed":
            confirmed_count += 1
        if item.get("trade_plan_emitted") is True or state == "emitted":
            emitted_count += 1
        if item.get("trade_plan_consumable") is True:
            consumable_count += 1
        terminal = bool(item.get("terminal", state not in {"observing", "queued", "pending"}))
        if terminal and state not in {"observing", "queued", "pending"}:
            terminalized_count += 1
        else:
            unfinished_count += 1

    target_count = int(doc.get("count") or len(items))
    rejected_count = int(state_counts.get("rejected", 0))
    not_ready_count = int(state_counts.get("not_ready", 0))
    timeout_count = int(state_counts.get("timeout", 0))
    observing_count = int(state_counts.get("observing", 0)) + int(state_counts.get("queued", 0)) + int(state_counts.get("pending", 0))
    unfinished_count = max(unfinished_count, observing_count)

    if doc.get("status") == "blocked":
        blocked_reason = str(doc.get("blocked_reason") or "")
        technical_blocked = bool(doc.get("technical_blocked")) or blocked_reason in {
            "micro_daemon_stale_during_wait",
            "technical_blocked_micro_daemon_stale",
        }
        line_exec_status = str(
            doc.get("line_exec_status")
            or ("technical_blocked" if technical_blocked else ("no_confirmed" if ready_count > 0 else "blocked")),
        )
        line_lifecycle_status = str(
            doc.get("line_lifecycle_status") or ("technical_blocked" if technical_blocked else blocked_reason or "blocked"),
        )
        if technical_blocked:
            stage = f"blocked_micro_wait_technical_{line}"
        elif blocked_reason == "micro_fast_quality_ready_but_no_confirmed_symbol":
            stage = "blocked_micro_fast_no_consumable_symbol"
        else:
            stage = f"blocked_{line}" if blocked_reason else "completed_terminalized"
    elif unfinished_count > 0:
        line_exec_status = "usable_partial" if consumable_count > 0 else "no_ready"
        line_lifecycle_status = "observing"
        stage = "completed_with_unfinished_symbols"
    elif consumable_count > 0:
        line_exec_status = "usable_all_ready"
        line_lifecycle_status = "terminalized_with_consumable"
        stage = "completed_with_consumable_symbols"
    elif target_count > 0:
        line_exec_status = "no_confirmed" if ready_count > 0 else "no_ready"
        line_lifecycle_status = "terminalized_no_consumable"
        stage = "completed_terminalized"
    else:
        line_exec_status = "blocked"
        line_lifecycle_status = "no_targets"
        stage = "completed_terminalized"

    technical_blocked = line_exec_status == "technical_blocked" or line_lifecycle_status == "technical_blocked"
    if technical_blocked:
        stage_status_class = "technical_failed"
        business_terminal_reason = ""
        technical_failure_reason = str(doc.get("technical_block_reason") or doc.get("blocked_reason") or "micro_fast_technical_blocked")
    elif unfinished_count > 0 and consumable_count > 0:
        stage_status_class = "business_partial_consumable"
        business_terminal_reason = "partial_consumable_symbols"
        technical_failure_reason = ""
    elif unfinished_count > 0:
        stage_status_class = "technical_failed"
        business_terminal_reason = ""
        technical_failure_reason = "unfinished_micro_symbols"
    elif consumable_count > 0:
        stage_status_class = "completed_with_consumable"
        business_terminal_reason = "consumable_symbols_ready"
        technical_failure_reason = ""
    elif target_count > 0:
        stage_status_class = "business_no_signal"
        business_terminal_reason = line_exec_status
        technical_failure_reason = ""
    else:
        stage_status_class = "business_no_signal"
        business_terminal_reason = "no_targets"
        technical_failure_reason = ""

    return {
        "stage": stage,
        "stage_status_class": stage_status_class,
        "business_terminal_reason": business_terminal_reason,
        "technical_failure_reason": technical_failure_reason,
        "line_exec_status": line_exec_status,
        "line_lifecycle_status": line_lifecycle_status,
        "line_lifecycle_complete": unfinished_count == 0 and target_count > 0,
        "trade_plan_allowed": consumable_count > 0,
        "technical_blocked": technical_blocked,
        "technical_block_reason": doc.get("technical_block_reason") or (doc.get("blocked_reason") if technical_blocked else None),
        "recovery": doc.get("recovery") if isinstance(doc.get("recovery"), dict) else {},
        "terminalized_symbol_count": terminalized_count,
        "unfinished_symbol_count": unfinished_count,
        "consumable_symbol_count": consumable_count,
        "rejected_count": rejected_count,
        "not_ready_count": not_ready_count,
        "timeout_count": timeout_count,
        "observing_count": observing_count,
        "symbol_counts": {
            "target": target_count,
            "ready": ready_count,
            "confirmed": confirmed_count,
            "emitted": emitted_count,
            "consumable": consumable_count,
            "rejected": rejected_count,
            "not_ready": not_ready_count,
            "timeout": timeout_count,
            "observing": observing_count,
            "unfinished": unfinished_count,
            "states": state_counts,
        },
    }


def _renew_strategy_lock(
    lock_path: Path,
    *,
    run_id: str,
    cycle_id: str,
    ttl_sec: int,
    stage: str | None,
    line: str | None,
) -> None:
    try:
        payload = read_json_object(lock_path) if lock_path.is_file() else {}
    except (OSError, ValueError, TypeError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    now = utc_now()
    payload.update(
        {
            "schema_version": "1.0",
            "lock_owner_pid": os.getpid(),
            "run_id": run_id,
            "cycle_id": cycle_id,
            "updated_at": to_iso_z(now),
            "heartbeat_at": to_iso_z(now),
            "expires_at": to_iso_z(now + timedelta(seconds=ttl_sec)),
            "stage": stage or payload.get("stage") or "running",
            "line": line,
            "heartbeat_seq": int(payload.get("heartbeat_seq") or 0) + 1,
        },
    )
    payload.setdefault("started_at", to_iso_z(now))
    write_json_atomic(lock_path, payload)


def _run_stage(
    stages: list[dict[str, Any]],
    name: str,
    fn: Callable[[], int],
    on_update: Callable[[str, str], None] | None = None,
) -> int:
    if on_update:
        on_update(name, "running")
    t0 = time.monotonic()
    try:
        rc = int(fn())
    except Exception as exc:
        stages.append(
            {
                "name": name,
                "ok": False,
                "rc": EXIT_INTERNAL,
                "duration_sec": round(time.monotonic() - t0, 3),
                "error": str(exc),
            },
        )
        if on_update:
            on_update(name, "failed")
        return EXIT_INTERNAL
    stages.append(
        {
            "name": name,
            "ok": rc == EXIT_SUCCESS,
            "rc": rc,
            "duration_sec": round(time.monotonic() - t0, 3),
        },
    )
    if on_update:
        on_update(name, "done" if rc == EXIT_SUCCESS else "failed")
    return rc


def _run_stage_detail(
    stages: list[dict[str, Any]],
    name: str,
    fn: Callable[[], dict[str, Any]],
    on_update: Callable[[str, str], None] | None = None,
) -> int:
    if on_update:
        on_update(name, "running")
    t0 = time.monotonic()
    try:
        detail = fn()
        rc = int(detail.get("rc", EXIT_SUCCESS))
    except Exception as exc:
        stages.append(
            {
                "name": name,
                "ok": False,
                "rc": EXIT_INTERNAL,
                "duration_sec": round(time.monotonic() - t0, 3),
                "error": str(exc),
            },
        )
        if on_update:
            on_update(name, "failed")
        return EXIT_INTERNAL
    stage = {
        "name": name,
        "ok": rc == EXIT_SUCCESS,
        "rc": rc,
        "duration_sec": round(time.monotonic() - t0, 3),
        "detail": {k: v for k, v in detail.items() if k != "rc"},
    }
    stages.append(stage)
    if on_update:
        on_update(name, "done" if rc == EXIT_SUCCESS else "failed")
    return rc


def _micro_wait_detail(*, cfg: EngineConfig, line: StrategyLine) -> dict[str, Any]:
    latest: dict[str, Any] = {}
    try:
        latest = read_json_object(cfg.micro_daemon_cli_features_path)
    except (OSError, ValueError, TypeError):
        latest = {}
    target_set_id = ""
    expected_symbols: set[str] = set()
    target_generated_at = ""
    try:
        targets = read_json_object(cfg.micro_targets_path)
        target_set_id = str(targets.get("target_set_id") or "")
        target_generated_at = str(targets.get("generated_at") or "")
        raw_symbols = targets.get("target_symbols")
        if isinstance(raw_symbols, list):
            expected_symbols = {
                normalize_symbol(str(sym or ""))
                for sym in raw_symbols
                if normalize_symbol(str(sym or ""))
            }
        for key in ("tier1_warm_watch", "tier2_active_strong"):
            rows = targets.get(key)
            if not isinstance(rows, list):
                continue
            for row in rows:
                if isinstance(row, dict):
                    sym = normalize_symbol(str(row.get("symbol") or ""))
                    if sym:
                        expected_symbols.add(sym)
    except (OSError, ValueError, TypeError):
        targets = {}
    if isinstance(latest, dict) and expected_symbols:
        latest = scope_micro_to_expected_symbols(
            latest,
            expected_symbols,
            target_set_id=target_set_id,
            expected_target_generated_at=target_generated_at,
        )
    items = latest.get("items") if isinstance(latest, dict) else None
    full_ready_count = latest.get("full_ready_count") if isinstance(latest, dict) else None
    fast_ready_count = latest.get("fast_ready_count") if isinstance(latest, dict) else None
    ready_count = latest.get("ready_count") if isinstance(latest, dict) else None
    max_eta = 0
    reason_counts: dict[str, int] = {}
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            refs = item.get("input_refs") if isinstance(item.get("input_refs"), dict) else {}
            eta = refs.get("full_ready_eta_sec") if isinstance(refs, dict) else None
            if isinstance(eta, (int, float)):
                max_eta = max(max_eta, int(eta))
            for block_key in ("micro_full_quality", "micro_quality"):
                block = item.get(block_key)
                reasons = block.get("reason_codes") if isinstance(block, dict) else None
                if isinstance(reasons, list):
                    for reason in reasons:
                        key = str(reason)
                        reason_counts[key] = reason_counts.get(key, 0) + 1
    detail = {
        "line": line,
        "wait_policy": (
            cfg.strategy_pipeline_full_wait_policy if line == "micro_full" else "fast_ready"
        ),
        "wait_sec": (
            cfg.strategy_pipeline_wait_full_sec
            if line == "micro_full"
            else cfg.strategy_pipeline_wait_fast_sec
        ),
        "min_full_ready_count": (
            cfg.strategy_pipeline_min_full_ready_count if line == "micro_full" else None
        ),
        "ready_count": ready_count if isinstance(ready_count, int) else 0,
        "fast_ready_count": fast_ready_count if isinstance(fast_ready_count, int) else 0,
        "full_ready_count": full_ready_count if isinstance(full_ready_count, int) else 0,
        "symbol_count": len(items) if isinstance(items, list) else 0,
        "ready_scope": str(latest.get("ready_scope") or latest.get("scope") or "") if isinstance(latest, dict) else "",
        "target_set_id": str(latest.get("target_set_id") or target_set_id) if isinstance(latest, dict) else target_set_id,
        "target_count": len(expected_symbols),
        "expected_symbol_count": len(expected_symbols),
        "global_symbol_count": int(latest.get("global_symbol_count") or 0) if isinstance(latest, dict) else 0,
        "global_ready_count": latest.get("global_ready_count") if isinstance(latest, dict) else None,
        "global_full_ready_count": latest.get("global_full_ready_count") if isinstance(latest, dict) else None,
        "missing_target_symbols": latest.get("missing_target_symbols") if isinstance(latest, dict) else [],
        "max_full_ready_eta_sec": max_eta,
        "top_reason_codes": sorted(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:8],
        "micro_generated_at": str(latest.get("generated_at") or "") if isinstance(latest, dict) else "",
        "target_generated_at": str(latest.get("target_generated_at") or "") if isinstance(latest, dict) else "",
        "target_status": str(latest.get("target_status") or "") if isinstance(latest, dict) else "",
    }
    detail.update(_micro_signal_consumable_counts(items, line=line))
    evidence_path = cfg.project_root / "DATA" / "micro" / "evidence" / f"latest_wait_pass_{line}.json"
    try:
        evidence = read_json_object(evidence_path)
    except (OSError, ValueError, TypeError):
        evidence = {}
    if isinstance(evidence, dict) and str(evidence.get("strategy_line") or "") == str(line):
        if not target_set_id or str(evidence.get("target_set_id") or "") == target_set_id:
            detail.update(
                {
                    "wait_evidence_path": str(evidence_path.resolve()),
                    "wait_predicate": str(evidence.get("wait_predicate") or ""),
                    "wait_pass_micro_generated_at": str(evidence.get("micro_generated_at") or ""),
                    "wait_pass_micro_state_generated_at": str(evidence.get("micro_state_generated_at") or ""),
                    "wait_pass_ready_symbols": evidence.get("ready_symbols") if isinstance(evidence.get("ready_symbols"), list) else [],
                    "wait_pass_fast_ready_symbols": evidence.get("fast_ready_symbols")
                    if isinstance(evidence.get("fast_ready_symbols"), list)
                    else [],
                    "wait_pass_full_ready_symbols": evidence.get("full_ready_symbols")
                    if isinstance(evidence.get("full_ready_symbols"), list)
                    else [],
                    "quality_ready_count": int(evidence.get("quality_ready_count") or detail.get("quality_ready_count") or 0),
                    "confirmed_ready_count": int(evidence.get("confirmed_ready_count") or detail.get("confirmed_ready_count") or 0),
                    "consumable_ready_count": int(evidence.get("consumable_ready_count") or detail.get("consumable_ready_count") or 0),
                    "quality_ready_symbols": evidence.get("quality_ready_symbols")
                    if isinstance(evidence.get("quality_ready_symbols"), list)
                    else detail.get("quality_ready_symbols", []),
                    "confirmed_symbols": evidence.get("confirmed_symbols")
                    if isinstance(evidence.get("confirmed_symbols"), list)
                    else detail.get("confirmed_symbols", []),
                    "consumable_symbols": evidence.get("consumable_symbols")
                    if isinstance(evidence.get("consumable_symbols"), list)
                    else detail.get("consumable_symbols", []),
                },
            )
    if isinstance(latest, dict) and isinstance(targets, dict):
        detail["ready_source_counts"] = _micro_ready_source_counts(
            latest=latest,
            targets=targets,
            line=line,
        )
    detail.update(_micro_line_wait_contract(detail))
    return detail


def _step2_freshness_gate_stages(stages: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for stage in stages:
        if not str(stage.get("name") or "").startswith("step2_freshness_"):
            continue
        detail = stage.get("detail") if isinstance(stage.get("detail"), dict) else {}
        rows.append(
            {
                "name": stage.get("name"),
                "ok": stage.get("ok"),
                "rc": stage.get("rc"),
                "duration_sec": stage.get("duration_sec"),
                "line": detail.get("line"),
                "checkpoint": detail.get("checkpoint"),
                "status": detail.get("status"),
                "step2_age_sec": detail.get("step2_age_sec"),
                "step2_max_age_sec": detail.get("step2_max_age_sec"),
                "step2_remaining_sec": detail.get("step2_remaining_sec"),
                "step2_renew_action": detail.get("step2_renew_action"),
                "step2_renew_result": detail.get("step2_renew_result"),
                "reason_codes": detail.get("reason_codes") or [],
            }
        )
    return rows


def _run_pipeline_pre_micro_for_options(
    *,
    root: Path,
    opts: StrategyPipelineOptions,
    scan_allow_stale_input: bool | None = None,
) -> int:
    return run_pipeline_pre_micro_safe(
        project_root=root,
        force_universe=opts.force_universe,
        light_limit=opts.light_limit,
        fetch_mode=opts.fetch_mode,
        max_concurrency=opts.max_concurrency,
        scan_allow_stale_input=opts.scan_allow_stale_input if scan_allow_stale_input is None else bool(scan_allow_stale_input),
        skip_market_context=opts.skip_market_context,
        stdout_json=False,
    )


def _step2_age_stale_reason_present(reason_codes: Sequence[str]) -> bool:
    age_stale_reasons = {
        "watch_output_stale",
        "strong_output_stale",
        "step2_current_stale",
    }
    return any(str(code) in age_stale_reasons for code in reason_codes)


def _common_upstream_step1_to_step2_5_detail(*, root: Path, opts: StrategyPipelineOptions) -> dict[str, Any]:
    rc = _run_pipeline_pre_micro_for_options(root=root, opts=opts)
    current = build_step2_current_freshness(project_root=root)
    reason_codes = [str(x) for x in current.get("reason_codes") or []]
    detail: dict[str, Any] = {
        "rc": int(rc),
        "schema_version": "STEP1.77_common_upstream_freshness_renewal_v1",
        "status": "ok" if rc == EXIT_SUCCESS else "failed",
        "initial_rc": int(rc),
        "step2_freshness_after_initial": current,
        "step2_renew_action": "none",
        "step2_renew_result": "not_needed" if rc == EXIT_SUCCESS else "not_attempted",
        "reason_codes": reason_codes,
    }
    if rc == EXIT_SUCCESS:
        return detail
    if not _step2_age_stale_reason_present(reason_codes):
        detail["status"] = "failed_without_stale_retry"
        detail["reason_codes"] = list(dict.fromkeys(["common_upstream_failed", *reason_codes]))
        return detail

    retry_rc = _run_pipeline_pre_micro_for_options(root=root, opts=opts, scan_allow_stale_input=True)
    after_retry = build_step2_current_freshness(project_root=root)
    after_reason_codes = [str(x) for x in after_retry.get("reason_codes") or []]
    detail.update(
        {
            "retry_rc": int(retry_rc),
            "step2_freshness_after_retry": after_retry,
            "step2_renew_action": "retry_common_upstream_with_scan_allow_stale_input",
            "step2_renew_result": "fresh" if retry_rc == EXIT_SUCCESS and after_retry.get("current_freshness") == "fresh" else "failed",
        }
    )
    if retry_rc == EXIT_SUCCESS and after_retry.get("current_freshness") == "fresh":
        detail["rc"] = EXIT_SUCCESS
        detail["status"] = "renewed_after_common_upstream_retry"
        detail["reason_codes"] = ["step2_freshness_renewed_before_long_stage"]
        return detail
    detail["rc"] = int(retry_rc) if retry_rc != EXIT_SUCCESS else EXIT_CONFIG
    detail["status"] = "failed_after_stale_retry"
    detail["reason_codes"] = list(
        dict.fromkeys(
            [
                "step2_freshness_common_upstream_retry_failed",
                *reason_codes,
                *after_reason_codes,
            ]
        )
    )
    return detail


def _wait_micro_ready(
    *,
    root: Path,
    line: StrategyLine,
    cfg: EngineConfig,
    stages: list[dict[str, Any]],
    skip_micro_wait: bool,
    run_id: str | None = None,
    cycle_id: str | None = None,
    on_update: Callable[[str, str], None] | None = None,
    on_poll: Callable[[float], None] | None = None,
) -> int:
    if line not in ("micro_fast", "micro_full") or skip_micro_wait:
        return EXIT_SUCCESS
    if cfg.strategy_pipeline_micro_health_preflight_enabled:
        health = _pipeline_micro_health(cfg)
        health, grace_stage = _micro_health_grace_recheck(cfg=cfg, line=line, initial_health=health)
        if grace_stage:
            stages.append(grace_stage)
            if on_update:
                on_update(str(grace_stage.get("name")), "done" if grace_stage.get("ok") else "failed")
        if health.get("status") != "running":
            health, recovery_stage = _micro_health_inline_recovery(
                cfg=cfg,
                line=line,
                initial_health=health,
                on_update=on_update,
            )
            if recovery_stage:
                stages.append(recovery_stage)
                if on_update:
                    on_update(
                        str(recovery_stage.get("name")),
                        "done" if recovery_stage.get("ok") else "failed",
                    )
        if health.get("status") != "running":
            rc = _run_stage(
                stages,
                f"blocked_micro_unhealthy_{line}",
                lambda: write_blocked_trade_plan_line(
                    line=line,  # type: ignore[arg-type]
                    project_root=root,
                    run_id=run_id,
                    cycle_id=cycle_id,
                    blocked_reason="micro_daemon_unhealthy",
                    reason_codes=_micro_unhealthy_reason_codes(health),
                    runtime_health=health,
                ),
                on_update=on_update,
            )
            return EXIT_SUCCESS if cfg.strategy_pipeline_micro_unhealthy_policy == "block_line" else rc
    wait_sec = (
        cfg.strategy_pipeline_wait_fast_sec
        if line == "micro_fast"
        else cfg.strategy_pipeline_wait_full_sec
    )

    def _do_wait_for(max_wait_sec: int | float) -> int:
        wait_cfg = load_wait_until_ready_config(root)
        mode = "min_fast_ready_count" if line == "micro_fast" else "min_ready_count"
        min_ready_count = 1
        if line == "micro_full" and cfg.strategy_pipeline_full_wait_policy == "strict_until_ready":
            mode = "min_full_ready_count"
            min_ready_count = max(1, int(cfg.strategy_pipeline_min_full_ready_count))
        wait_cfg = wait_cfg.__class__(
            **{
                **wait_cfg.__dict__,
                "max_wait_sec": float(max_wait_sec),
                "mode": mode,
                "min_ready_count": min_ready_count,
            },
        )
        return run_wait_until_ready_orchestration(
            project_root=root,
            cfg=wait_cfg,
            latest_path=cfg.micro_daemon_cli_features_path,
            heartbeat_path=cfg.micro_daemon_cli_heartbeat_path,
            targets_path=cfg.micro_targets_path,
            transport=cfg.micro_daemon_cli_transport,
            start_subprocess=False,
            evidence_path=root / "DATA" / "micro" / "evidence" / f"latest_wait_pass_{line}.json",
            strategy_line=line,
            run_id=run_id,
            cycle_id=cycle_id,
            on_poll=on_poll,
        )

    def _do_wait() -> int:
        return _do_wait_for(wait_sec)

    rc = _run_stage(stages, f"wait_micro_ready_{line}", _do_wait, on_update=on_update)
    if stages and str(stages[-1].get("name")) == f"wait_micro_ready_{line}":
        stages[-1]["detail"] = _micro_wait_detail(cfg=cfg, line=line)
        if on_update:
            on_update(f"wait_micro_ready_{line}", "done" if rc == EXIT_SUCCESS else "failed")
    if line == "micro_fast" and not skip_micro_wait:
        post_wait_health = _pipeline_micro_health(cfg)
        if _micro_health_stale_during_wait(post_wait_health):
            recovery_health, recovery_stage = _micro_health_inline_recovery(
                cfg=cfg,
                line=line,
                initial_health=post_wait_health,
                on_update=on_update,
            )
            if recovery_stage:
                recovery_stage["reason_codes"] = list(
                    dict.fromkeys(
                        [
                            "micro_wait_recovery_attempted",
                            *[str(x) for x in recovery_stage.get("reason_codes") or []],
                        ],
                    ),
                )
                if recovery_stage.get("ok"):
                    recovery_stage["reason_codes"].append("micro_wait_recovery_success")
                stages.append(recovery_stage)
                if on_update:
                    on_update(
                        str(recovery_stage.get("name")),
                        "done" if recovery_stage.get("ok") else "failed",
                    )
            if recovery_health.get("status") == "running" and not _micro_health_stale_during_wait(recovery_health):
                extension_wait_sec = max(10, min(90, int(wait_sec)))
                extension_stage_name = f"wait_micro_ready_{line}_recovered_extension"
                ext_rc = _run_stage(
                    stages,
                    extension_stage_name,
                    lambda: _do_wait_for(extension_wait_sec),
                    on_update=on_update,
                )
                if stages and str(stages[-1].get("name")) == extension_stage_name:
                    detail = _micro_wait_detail(cfg=cfg, line=line)
                    detail.update(
                        {
                            "wait_recovery": {
                                "attempted": bool(recovery_stage),
                                "success": True,
                                "extended_wait_sec": extension_wait_sec,
                                "recovery_stage": recovery_stage or {},
                            },
                            "reason_codes": list(
                                dict.fromkeys(
                                    [
                                        *[str(x) for x in detail.get("reason_codes") or []],
                                        "micro_wait_recovery_success",
                                        "micro_wait_extended_after_recovery",
                                    ],
                                ),
                            ),
                        },
                    )
                    stages[-1]["detail"] = detail
                    if on_update:
                        on_update(extension_stage_name, "done" if ext_rc == EXIT_SUCCESS else "failed")
                rc = ext_rc
            else:
                return _write_micro_fast_technical_blocked(
                    root=root,
                    line=line,
                    run_id=run_id,
                    cycle_id=cycle_id,
                    health=recovery_health,
                    recovery_stage=recovery_stage,
                    stages=stages,
                    on_update=on_update,
                )
    if rc != EXIT_SUCCESS and cfg.strategy_pipeline_timeout_policy == "return_reason":
        if line == "micro_fast":
            line_cfg = load_trade_plan_line_config(root, "micro_fast")
            if not line_cfg.require_micro_ready:
                detail = _micro_wait_detail(cfg=cfg, line=line)
                detail["wait_rc"] = rc
                detail["reason_codes"] = [
                    "micro_fast_wait_timeout",
                    "fast_ready_not_required",
                    "continue_degraded",
                ]
                if stages and str(stages[-1].get("name")) == f"wait_micro_ready_{line}":
                    stages[-1].update(
                        {
                            "ok": True,
                            "rc": EXIT_SUCCESS,
                            "original_rc": rc,
                            "status": "degraded",
                            "detail": detail,
                        },
                    )
                    if on_update:
                        on_update(f"wait_micro_ready_{line}", "done")
                return EXIT_SUCCESS
        if line == "micro_full" and cfg.strategy_pipeline_full_wait_policy == "strict_until_ready":
            reason = (
                "micro_full_wait_timeout"
                if rc == EXIT_WAIT_UNTIL_READY_TIMEOUT
                else "micro_full_wait_failed"
            )
            detail = _micro_wait_detail(cfg=cfg, line=line)
            detail["wait_rc"] = rc
            detail["reason_codes"] = [reason, "full_warmup_incomplete"]
            _run_stage(
                stages,
                f"blocked_{reason}_{line}",
                lambda: (
                    write_micro_timeout_lifecycle_document(
                        line="micro_full",
                        project_root=root,
                        run_id=run_id,
                        cycle_id=cycle_id,
                        reason_codes=[reason, "full_warmup_incomplete"],
                        runtime_health=detail,
                    ),
                    write_blocked_trade_plan_line(
                        line="micro_full",
                        project_root=root,
                        run_id=run_id,
                        cycle_id=cycle_id,
                        blocked_reason=reason,
                        reason_codes=[reason, "full_warmup_incomplete"],
                        runtime_health=detail,
                    ),
                )[1],
                on_update=on_update,
            )
        return EXIT_SUCCESS
    return rc


def _pipeline_micro_health(cfg: EngineConfig) -> dict[str, Any]:
    return micro_daemon_health(
        pid_path=cfg.micro_daemon_cli_pid_path,
        heartbeat_path=cfg.micro_daemon_cli_heartbeat_path,
        state_path=cfg.micro_daemon_cli_state_path,
        features_path=cfg.micro_daemon_cli_features_path,
        heartbeat_stale_sec=cfg.strategy_pipeline_micro_preflight_heartbeat_stale_sec,
    )


def _age_from_iso(iso_text: str) -> int | None:
    if not iso_text:
        return None
    try:
        return max(0, int((utc_now() - parse_iso_z(iso_text)).total_seconds()))
    except Exception:
        return None


def _micro_health_grace_recheck(
    *,
    cfg: EngineConfig,
    line: StrategyLine,
    initial_health: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not cfg.strategy_pipeline_micro_health_grace_recheck_enabled:
        return initial_health, None
    if initial_health.get("status") == "running":
        return initial_health, None
    if bool(initial_health.get("pid_running")) and str(initial_health.get("data_plane_status") or "") == "stale":
        return initial_health, None
    if not _micro_health_recoverable(initial_health, cfg):
        return initial_health, None

    attempts: list[dict[str, Any]] = []
    wait_sec = max(0, min(30, int(cfg.strategy_pipeline_micro_health_grace_wait_sec)))
    max_attempts = max(1, min(3, int(cfg.strategy_pipeline_micro_health_grace_max_attempts)))
    health = initial_health
    old_pid = health.get("pid")
    for attempt in range(1, max_attempts + 1):
        if wait_sec > 0:
            time.sleep(wait_sec)
        health = _pipeline_micro_health(cfg)
        attempts.append(
            {
                "attempt": attempt,
                "status": health.get("status"),
                "pid": health.get("pid"),
                "pid_running": health.get("pid_running"),
                "heartbeat_age_sec": health.get("heartbeat_age_sec"),
            },
        )
        if health.get("status") == "running":
            return health, {
                "name": f"micro_health_grace_recheck_{line}",
                "ok": True,
                "rc": EXIT_SUCCESS,
                "duration_sec": round(wait_sec * attempt, 3),
                "status": "recovered",
                "attempts": attempts,
                "old_pid": old_pid,
                "new_pid": health.get("pid"),
            }

    return health, {
        "name": f"micro_health_grace_recheck_{line}",
        "ok": False,
        "rc": EXIT_CONFIG,
        "duration_sec": round(wait_sec * max_attempts, 3),
        "status": "blocked",
        "attempts": attempts,
        "old_pid": old_pid,
        "new_pid": health.get("pid"),
        "reason_codes": ["micro_daemon_unhealthy_after_grace"],
    }


def _micro_daemon_control_action(cfg: EngineConfig, action: str) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "-m",
        "laoma_signal_engine.cli",
        "micro-daemon",
        action,
        "--project-root",
        str(cfg.project_root),
        "--stdout-json",
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(cfg.project_root),
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    payload: dict[str, Any] | None = None
    if proc.stdout.strip():
        try:
            parsed = json.loads(proc.stdout)
            if isinstance(parsed, dict):
                payload = parsed
        except json.JSONDecodeError:
            payload = None
    return {
        "status": "completed" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "payload": payload,
        "command": cmd,
    }


def _micro_health_is_fresh_after_recovery(health: dict[str, Any], cfg: EngineConfig) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if health.get("status") != "running":
        reasons.append("micro_daemon_not_running_after_recovery")
    heartbeat_age = health.get("heartbeat_age_sec")
    heartbeat_limit = max(1, int(cfg.strategy_pipeline_micro_inline_recovery_heartbeat_fresh_sec))
    if not isinstance(heartbeat_age, (int, float)) or float(heartbeat_age) > heartbeat_limit:
        reasons.append("micro_heartbeat_not_fresh")
    state_age = _age_from_iso(str(health.get("state_generated_at") or ""))
    state_limit = max(1, int(cfg.strategy_pipeline_micro_inline_recovery_state_fresh_sec))
    if state_age is None or state_age > state_limit:
        reasons.append("micro_state_not_fresh")
    features_age = _age_from_iso(str(health.get("features_generated_at") or ""))
    features_limit = max(1, int(cfg.strategy_pipeline_micro_inline_recovery_features_fresh_sec))
    if features_age is None or features_age > features_limit:
        reasons.append("micro_features_not_fresh")
    return not reasons, reasons


def _micro_health_inline_recovery(
    *,
    cfg: EngineConfig,
    line: StrategyLine,
    initial_health: dict[str, Any],
    on_update: Callable[[str, str], None] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not cfg.strategy_pipeline_micro_inline_recovery_enabled:
        return initial_health, None
    attempts: list[dict[str, Any]] = []
    old_pid = initial_health.get("pid")
    health = initial_health
    max_attempts = max(1, min(3, int(cfg.strategy_pipeline_micro_inline_recovery_max_attempts)))
    wait_sec = max(0, min(90, int(cfg.strategy_pipeline_micro_inline_recovery_startup_wait_sec)))
    started_at = utc_now()
    for attempt in range(1, max_attempts + 1):
        if on_update:
            on_update(f"micro_health_recovering_{line}", "running")
        stop = _micro_daemon_control_action(cfg, "stop")
        start = _micro_daemon_control_action(cfg, "start")
        if wait_sec > 0:
            time.sleep(wait_sec)
        health = _pipeline_micro_health(cfg)
        fresh, reasons = _micro_health_is_fresh_after_recovery(health, cfg)
        attempts.append(
            {
                "attempt": attempt,
                "old_pid": old_pid,
                "new_pid": health.get("pid"),
                "stop": stop,
                "start": start,
                "status": health.get("status"),
                "pid_running": health.get("pid_running"),
                "heartbeat_age_sec": health.get("heartbeat_age_sec"),
                "state_generated_at": health.get("state_generated_at"),
                "features_generated_at": health.get("features_generated_at"),
                "fresh": fresh,
                "reason_codes": reasons,
            },
        )
        if fresh:
            if on_update:
                on_update(f"micro_recovery_success_continue_{line}", "done")
            return health, {
                "name": f"micro_inline_recovery_{line}",
                "ok": True,
                "rc": EXIT_SUCCESS,
                "duration_sec": round((utc_now() - started_at).total_seconds(), 3),
                "status": "recovered",
                "attempts": attempts,
                "old_pid": old_pid,
                "new_pid": health.get("pid"),
                "heartbeat_age_sec": health.get("heartbeat_age_sec"),
                "state_generated_at": health.get("state_generated_at"),
                "features_generated_at": health.get("features_generated_at"),
                "reason_codes": [],
            }
    if on_update:
        on_update(f"micro_recovery_failed_blocked_{line}", "failed")
    reason_codes = ["micro_inline_recovery_failed"]
    if attempts:
        reason_codes.extend(str(x) for x in attempts[-1].get("reason_codes") or [])
    return health, {
        "name": f"micro_inline_recovery_{line}",
        "ok": False,
        "rc": EXIT_CONFIG,
        "duration_sec": round((utc_now() - started_at).total_seconds(), 3),
        "status": "failed",
        "attempts": attempts,
        "old_pid": old_pid,
        "new_pid": health.get("pid"),
        "heartbeat_age_sec": health.get("heartbeat_age_sec"),
        "state_generated_at": health.get("state_generated_at"),
        "features_generated_at": health.get("features_generated_at"),
        "reason_codes": reason_codes,
    }


def _micro_health_recoverable(health: dict[str, Any], cfg: EngineConfig) -> bool:
    if bool(health.get("pid_running")):
        return str(health.get("health_state") or "") == "degraded_transport_ok_data_stale" or str(
            health.get("data_plane_status") or "",
        ) == "stale"
    if not bool(health.get("heartbeat_exists")) and not health.get("features_generated_at"):
        return False
    fresh_limit = max(1, int(cfg.strategy_pipeline_micro_health_grace_accept_fresh_heartbeat_sec))
    heartbeat_age = health.get("heartbeat_age_sec")
    if isinstance(heartbeat_age, (int, float)) and float(heartbeat_age) <= fresh_limit:
        return True
    if health.get("stale") is False and bool(health.get("heartbeat_exists")):
        return True
    return False


def _micro_unhealthy_reason_codes(health: dict[str, Any]) -> list[str]:
    reasons = ["micro_daemon_unhealthy"]
    for code in health.get("reason_codes") or []:
        code = str(code)
        if code and code not in reasons:
            reasons.append(code)
    if not health.get("pid_running"):
        reasons.append("micro_daemon_stopped")
    if health.get("stale"):
        reasons.append("micro_heartbeat_stale")
    if not health.get("heartbeat_exists"):
        reasons.append("micro_heartbeat_missing")
    return reasons


def _micro_wait_stale_reason_codes(health: dict[str, Any]) -> list[str]:
    reasons = ["micro_daemon_stale_during_wait"]
    for code in _micro_unhealthy_reason_codes(health):
        if code not in reasons:
            reasons.append(code)
    health_state = str(health.get("health_state") or "")
    data_plane_status = str(health.get("data_plane_status") or "")
    if health_state and health_state not in reasons:
        reasons.append(f"micro_health_state_{health_state}")
    if data_plane_status and data_plane_status not in {"fresh", "unknown"}:
        reasons.append(f"micro_data_plane_{data_plane_status}")
    return reasons


def _micro_health_stale_during_wait(health: dict[str, Any]) -> bool:
    if health.get("status") != "running":
        return True
    if health.get("stale") is True:
        return True
    if str(health.get("health_state") or "") in {
        "degraded_transport_ok_data_stale",
        "stale",
        "down",
    }:
        return True
    if str(health.get("data_plane_status") or "") == "stale":
        return True
    reason_codes = {str(x) for x in health.get("reason_codes") or []}
    return bool(reason_codes.intersection({"micro_heartbeat_stale", "micro_state_write_stale", "micro_features_write_stale"}))


def _write_micro_fast_technical_blocked(
    *,
    root: Path,
    line: StrategyLine,
    run_id: str | None,
    cycle_id: str | None,
    health: dict[str, Any],
    recovery_stage: dict[str, Any] | None,
    stages: list[dict[str, Any]],
    on_update: Callable[[str, str], None] | None = None,
) -> int:
    reason_codes = list(
        dict.fromkeys(
            [
                "micro_fast_technical_blocked",
                "technical_blocked_micro_daemon_stale",
                *_micro_wait_stale_reason_codes(health),
                *(str(x) for x in (recovery_stage or {}).get("reason_codes") or []),
            ],
        ),
    )
    runtime_health = {
        **health,
        "line_exec_status": "technical_blocked",
        "line_lifecycle_status": "technical_blocked",
        "technical_blocked": True,
        "technical_block_reason": "micro_daemon_stale_during_wait",
        "recovery": {
            "attempted": bool(recovery_stage),
            "success": bool((recovery_stage or {}).get("ok")),
            "stage": recovery_stage or {},
        },
        "reason_codes": reason_codes,
    }
    rc = _run_stage(
        stages,
        f"blocked_micro_wait_technical_{line}",
        lambda: (
            write_blocked_micro_lifecycle_document(
                line=line,  # type: ignore[arg-type]
                project_root=root,
                run_id=run_id,
                cycle_id=cycle_id,
                blocked_reason="micro_daemon_stale_during_wait",
                reason_codes=reason_codes,
                runtime_health=runtime_health,
            ),
            write_blocked_trade_plan_line(
                line=line,  # type: ignore[arg-type]
                project_root=root,
                run_id=run_id,
                cycle_id=cycle_id,
                blocked_reason="micro_daemon_stale_during_wait",
                reason_codes=reason_codes,
                runtime_health=runtime_health,
            ),
        )[1],
        on_update=on_update,
    )
    return EXIT_SUCCESS if rc == EXIT_SUCCESS else rc


def _run_refresh_for_line(
    *,
    root: Path,
    line: StrategyLine,
    cfg: EngineConfig,
    stages: list[dict[str, Any]],
    opts: StrategyPipelineOptions,
    run_id: str,
    cycle_id: str,
    on_update: Callable[[str, str], None] | None = None,
) -> int:
    factor = _factor_path(root, line)
    return _run_stage(
        stages,
        f"refresh_{line}",
        lambda: run_pre_decision_candidate_refresh_safe(
            project_root=root,
            factor_path=factor,
            fetch_latest=cfg.strategy_pipeline_auto_refresh_before_trade_plan,
            fetch_mode=opts.fetch_mode,
            max_concurrency=opts.max_concurrency,
            line=line,
            run_id=run_id,
            cycle_id=cycle_id,
            stdout_json=False,
        ),
        on_update=on_update,
    )


def _step2_freshness_gate_detail(
    *,
    root: Path,
    line: str,
    checkpoint: str,
    opts: StrategyPipelineOptions,
    allow_renew: bool,
) -> dict[str, Any]:
    before = build_step2_current_freshness(project_root=root)
    before_reason_codes = [str(x) for x in before.get("reason_codes") or []]
    has_age_stale_reason = _step2_age_stale_reason_present(before_reason_codes)
    max_age = int(before.get("max_age_sec") or 300)
    ages = [
        int(v)
        for v in (
            before.get("watch_output_age_sec"),
            before.get("strong_output_age_sec"),
            before.get("current_input_snapshot_age_sec"),
        )
        if isinstance(v, (int, float))
    ]
    age = max(ages) if ages else None
    line_budget = int((opts.line_runtime_budgets or {}).get(line) or 0)
    renew_margin = max(90, min(240, line_budget or 180))
    remaining = None if age is None else max_age - age
    near_expiry = bool(
        before.get("current_freshness") == "fresh"
        and remaining is not None
        and remaining <= renew_margin
    )
    stale = before.get("current_freshness") != "fresh"
    detail: dict[str, Any] = {
        "rc": EXIT_SUCCESS,
        "schema_version": "STEP1.77_step2_freshness_gate_v1",
        "line": line,
        "checkpoint": checkpoint,
        "step2_freshness_before": before,
        "step2_age_sec": age,
        "step2_max_age_sec": max_age,
        "step2_remaining_sec": remaining,
        "step2_renew_margin_sec": renew_margin,
        "step2_renew_action": "none",
        "step2_renew_result": "not_needed",
        "reason_codes": [],
    }
    if not stale and not near_expiry:
        detail["status"] = "fresh"
        return detail
    if stale and not has_age_stale_reason:
        detail["status"] = "missing_or_invalid_evidence_skipped"
        detail["step2_renew_result"] = "skipped_missing_or_invalid_evidence"
        detail["reason_codes"] = list(
            dict.fromkeys(
                [
                    "step2_freshness_missing_or_invalid_evidence_not_enforced",
                    *before_reason_codes,
                ]
            )
        )
        return detail
    if allow_renew:
        detail["step2_renew_action"] = "run_common_upstream_step1_to_step2_5"
        renew_rc = _run_pipeline_pre_micro_for_options(root=root, opts=opts)
        after = build_step2_current_freshness(project_root=root)
        detail["step2_renew_rc"] = int(renew_rc)
        detail["step2_freshness_after"] = after
        if renew_rc == EXIT_SUCCESS and after.get("current_freshness") == "fresh":
            detail["status"] = "renewed"
            detail["step2_renew_result"] = "fresh"
            detail["reason_codes"] = ["step2_freshness_renewed_before_long_stage"]
            return detail
        detail["status"] = "renew_failed"
        detail["step2_renew_result"] = "failed"
        detail["rc"] = EXIT_CONFIG
        detail["reason_codes"] = list(
            dict.fromkeys(
                [
                    "step2_freshness_renew_failed",
                    *before_reason_codes,
                    *[str(x) for x in after.get("reason_codes") or []],
                ]
            )
        )
        return detail
    detail["status"] = "stale_blocked" if stale else "near_expiry_blocked"
    detail["rc"] = EXIT_CONFIG
    detail["reason_codes"] = list(
        dict.fromkeys(
            [
                "step2_freshness_stale_before_refresh" if stale else "step2_freshness_near_expiry_before_refresh",
                *before_reason_codes,
            ]
        )
    )
    return detail


def _run_step2_freshness_gate(
    *,
    root: Path,
    line: Literal["without_micro", "micro_fast", "micro_full"],
    stages: list[dict[str, Any]],
    opts: StrategyPipelineOptions,
    checkpoint: str,
    allow_renew: bool,
    run_id: str,
    cycle_id: str,
    on_update: Callable[[str, str], None] | None = None,
) -> int:
    stage_name = f"step2_freshness_{checkpoint}_{line}"
    rc = _run_stage_detail(
        stages,
        stage_name,
        lambda: _step2_freshness_gate_detail(
            root=root,
            line=line,
            checkpoint=checkpoint,
            opts=opts,
            allow_renew=allow_renew,
        ),
        on_update=on_update,
    )
    if rc != EXIT_SUCCESS:
        detail = stages[-1].get("detail") if stages else {}
        reason_codes = detail.get("reason_codes") if isinstance(detail, dict) else []
        write_failed_trade_plan_line(
            line=line,
            project_root=root,
            run_id=run_id,
            cycle_id=cycle_id,
            failed_stage=stage_name,
            failed_rc=rc,
            reason_codes=[str(x) for x in reason_codes or ["step2_freshness_gate_failed"]],
            runtime_health=detail if isinstance(detail, dict) else {},
        )
    return rc


def _run_line(
    *,
    root: Path,
    line: Literal["without_micro", "micro_fast", "micro_full", "strategy5", "strategy6"],
    cfg: EngineConfig,
    stages: list[dict[str, Any]],
    opts: StrategyPipelineOptions,
    run_id: str,
    cycle_id: str,
    on_update: Callable[[str, str], None] | None = None,
) -> int:
    rc = _run_step2_freshness_gate(
        root=root,
        line=line,
        stages=stages,
        opts=opts,
        checkpoint="before_line",
        allow_renew=True,
        run_id=run_id,
        cycle_id=cycle_id,
        on_update=on_update,
    )
    if rc != EXIT_SUCCESS:
        return rc
    if line == "strategy5":
        rc = _run_stage(
            stages,
            "apply_trade_plan_strategy5",
            lambda: run_strategy5_pipeline_safe(
                project_root=root,
                run_id=run_id,
                cycle_id=cycle_id,
                stdout_json=False,
            ),
            on_update=on_update,
        )
        if rc != EXIT_SUCCESS:
            write_failed_trade_plan_line(
                line=line,
                project_root=root,
                run_id=run_id,
                cycle_id=cycle_id,
                failed_stage="apply_trade_plan_strategy5",
                failed_rc=rc,
                reason_codes=["apply_trade_plan_strategy5_failed", f"apply_trade_plan_rc_{rc}"],
                runtime_health={},
            )
            return rc
        _run_stage_detail(
            stages,
            "paper_wakeup_strategy5",
            lambda: _run_paper_wakeup_after_trade_plan(root, line, run_id=run_id),
            on_update=on_update,
        )
        return EXIT_SUCCESS
    if line == "strategy6":
        rc = _run_stage(
            stages,
            "apply_trade_plan_strategy6",
            lambda: run_strategy6_pipeline_safe(
                project_root=root,
                run_id=run_id,
                cycle_id=cycle_id,
                stdout_json=False,
            ),
            on_update=on_update,
        )
        if rc != EXIT_SUCCESS:
            write_failed_trade_plan_line(
                line=line,
                project_root=root,
                run_id=run_id,
                cycle_id=cycle_id,
                failed_stage="apply_trade_plan_strategy6",
                failed_rc=rc,
                reason_codes=["apply_trade_plan_strategy6_failed", f"apply_trade_plan_rc_{rc}"],
                runtime_health={},
            )
            return rc
        _run_stage_detail(
            stages,
            "paper_wakeup_strategy6",
            lambda: _run_paper_wakeup_after_trade_plan(root, line, run_id=run_id),
            on_update=on_update,
        )
        return EXIT_SUCCESS
    if line == "without_micro":
        rc = _run_stage(
            stages,
            "assemble_factor_without_micro",
            lambda: run_assemble_factor_snapshot_without_ofi_cvd_safe(
                project_root=root,
                stdout_json=False,
                skip_market_context=opts.skip_market_context,
            ),
            on_update=on_update,
        )
        if rc != EXIT_SUCCESS:
            return rc
    else:
        rc = _wait_micro_ready(
            root=root,
            line=line,
            cfg=cfg,
            stages=stages,
            skip_micro_wait=opts.skip_micro_wait,
            run_id=run_id,
            cycle_id=cycle_id,
            on_update=on_update,
            on_poll=lambda _: on_update(f"wait_micro_ready_{line}", "running") if on_update else None,
        )
        if rc != EXIT_SUCCESS:
            return rc
        if stages and str(stages[-1].get("name")).startswith("blocked_"):
            line_progress_name = f"blocked_micro_unhealthy_{line}"
            if str(stages[-1].get("name")).startswith("blocked_micro_full_wait_"):
                line_progress_name = str(stages[-1].get("name"))
            if on_update:
                on_update(line_progress_name, "done")
            return EXIT_SUCCESS
        detail = _micro_wait_detail(cfg=cfg, line=line)
        if (
            not opts.skip_micro_wait
            and line == "micro_fast"
            and detail.get("wait_result") == "quality_ready_but_no_confirmed_symbol"
            and detail.get("trade_plan_allowed") is False
        ):
            rc_block = _run_stage(
                stages,
                "blocked_micro_fast_no_consumable_symbol",
                lambda: (
                    write_blocked_micro_lifecycle_document(
                        line="micro_fast",
                        project_root=root,
                        run_id=run_id,
                        cycle_id=cycle_id,
                        blocked_reason="micro_fast_quality_ready_but_no_confirmed_symbol",
                        reason_codes=[
                            "micro_fast_quality_ready_but_no_confirmed_symbol",
                            "micro_fast_no_consumable_symbol",
                        ],
                        runtime_health=detail,
                    ),
                    write_blocked_trade_plan_line(
                        line="micro_fast",
                        project_root=root,
                        run_id=run_id,
                        cycle_id=cycle_id,
                        blocked_reason="micro_fast_quality_ready_but_no_confirmed_symbol",
                        reason_codes=[
                            "micro_fast_quality_ready_but_no_confirmed_symbol",
                            "micro_fast_no_consumable_symbol",
                        ],
                        runtime_health=detail,
                    ),
                )[1],
                on_update=on_update,
            )
            return EXIT_SUCCESS if rc_block == EXIT_SUCCESS else rc_block
        rc = _run_stage(
            stages,
            "assemble_factor_with_micro",
            lambda: run_assemble_factor_snapshot_safe(
                project_root=root,
                stdout_json=False,
                skip_market_context=opts.skip_market_context,
            ),
            on_update=on_update,
        )
        if rc != EXIT_SUCCESS:
            return rc

    rc = _run_step2_freshness_gate(
        root=root,
        line=line,
        stages=stages,
        opts=opts,
        checkpoint="before_refresh",
        allow_renew=True,
        run_id=run_id,
        cycle_id=cycle_id,
        on_update=on_update,
    )
    if rc != EXIT_SUCCESS:
        return rc

    rc = _run_refresh_for_line(
        root=root,
        line=line,
        cfg=cfg,
        stages=stages,
        opts=opts,
        run_id=run_id,
        cycle_id=cycle_id,
        on_update=on_update,
    )
    if rc != EXIT_SUCCESS:
        write_failed_trade_plan_line(
            line=line,
            project_root=root,
            run_id=run_id,
            cycle_id=cycle_id,
            failed_stage=f"refresh_{line}",
            failed_rc=rc,
            reason_codes=[f"refresh_{line}_failed", f"refresh_rc_{rc}"],
            runtime_health=_micro_wait_detail(cfg=cfg, line=line) if line != "without_micro" else {},
        )
        return rc
    rc = _run_stage(
        stages,
        f"apply_trade_plan_{line}",
        lambda: run_apply_trade_plan_line_safe(
            line=line,
            project_root=root,
            factor_path=_factor_path(root, line),
            run_id=run_id,
            cycle_id=cycle_id,
            stdout_json=False,
        ),
        on_update=on_update,
    )
    if rc != EXIT_SUCCESS:
        write_failed_trade_plan_line(
            line=line,
            project_root=root,
            run_id=run_id,
            cycle_id=cycle_id,
            failed_stage=f"apply_trade_plan_{line}",
            failed_rc=rc,
            reason_codes=[f"apply_trade_plan_{line}_failed", f"apply_trade_plan_rc_{rc}"],
            runtime_health=_micro_wait_detail(cfg=cfg, line=line) if line != "without_micro" else {},
        )
        return rc
    _run_stage_detail(
        stages,
        f"paper_wakeup_{line}",
        lambda: _run_paper_wakeup_after_trade_plan(root, line, run_id=run_id),
        on_update=on_update,
    )
    return _run_stage(
        stages,
        f"notify_feishu_{line}",
        lambda: _run_trade_plan_feishu_delivery(root, line=line),
        on_update=on_update,
    )


def _run_once(opts: StrategyPipelineOptions, cfg: EngineConfig) -> dict[str, Any]:
    root = opts.project_root
    run_id = _make_run_id()
    cycle_id = f"cycle_{run_id}"
    started_at = to_iso_z(utc_now())
    lock = acquire_scheduler_lock(
        lock_path=cfg.strategy_pipeline_lock_path,
        run_id=run_id,
        cycle_id=cycle_id,
        ttl_sec=max(540, opts.interval_sec * 2),
        overlap_policy=opts.overlap_policy,  # type: ignore[arg-type]
    )
    lock_ttl_sec = max(540, opts.interval_sec * 2)
    stages: list[dict[str, Any]] = []
    upstream_refresh: dict[str, Any] = {}
    selected_lines = opts.selected_lines
    selected_set = set(selected_lines)
    skipped_lines = tuple(line for line in STRATEGY_LINES if line not in selected_set)
    line_progress: dict[str, dict[str, Any]] = {}
    for line_name in STRATEGY_LINES:
        selected = line_name in selected_set
        line_progress[line_name] = {
            "percent": 0 if selected else 100,
            "stage": "waiting" if selected else "skipped_not_selected",
            "done": not selected,
            "selected": selected,
            "skipped": not selected,
            "output_fresh": False,
            "line_exec_status": "skipped_not_selected" if not selected else None,
            "line_lifecycle_status": "skipped_not_selected" if not selected else None,
            "trade_plan_allowed": False if not selected else None,
        }

    def _line_for_stage(stage_name: str) -> str | None:
        if stage_name.endswith("without_micro") or stage_name == "assemble_factor_without_micro":
            return "without_micro"
        if (
            stage_name.endswith("micro_fast")
            or stage_name == "wait_micro_ready_micro_fast"
            or "micro_fast" in stage_name
        ):
            return "micro_fast"
        if (
            stage_name.endswith("micro_full")
            or stage_name == "wait_micro_ready_micro_full"
            or "micro_full" in stage_name
        ):
            return "micro_full"
        if stage_name in {"apply_trade_plan_strategy5", "paper_wakeup_strategy5"}:
            return "strategy5"
        if stage_name in {"apply_trade_plan_strategy6", "paper_wakeup_strategy6"}:
            return "strategy6"
        return None

    def _latest_micro_wait_detail(line: str) -> dict[str, Any]:
        wait_name = f"wait_micro_ready_{line}"
        for stage in reversed(stages):
            if str(stage.get("name")) != wait_name:
                continue
            detail = stage.get("detail")
            return detail if isinstance(detail, dict) else {}
        return {}

    def _progress_update(stage_name: str, stage_status: str) -> None:
        current_line = _line_for_stage(stage_name)
        if lock.acquired:
            _renew_strategy_lock(
                lock.lock_path,
                run_id=run_id,
                cycle_id=cycle_id,
                ttl_sec=lock_ttl_sec,
                stage=stage_name,
                line=current_line,
            )
        if stage_name == "common_upstream_step1_to_step2_5":
            base = 15 if stage_status == "done" else 5
            for row in line_progress.values():
                if not row.get("done"):
                    row["percent"] = max(int(row.get("percent") or 0), base)
                    row["stage"] = stage_name
        elif current_line:
            blocked = stage_name.startswith("blocked_")
            percent = 100 if blocked and stage_status == "done" else _line_stage_percent(current_line, stage_name, stage_status)
            row = line_progress[current_line]
            row["percent"] = max(int(row.get("percent") or 0), percent)
            row["stage"] = stage_name if blocked else ("completed" if percent >= 100 and stage_status == "done" else stage_name)
            row["done"] = percent >= 100 and stage_status == "done"
            row["output_fresh"] = bool(row["done"])
            if current_line in {"micro_fast", "micro_full"}:
                detail = _latest_micro_wait_detail(current_line)
                for key in (
                    "line_exec_status",
                    "line_lifecycle_status",
                    "wait_result",
                    "line_lifecycle_complete",
                    "trade_plan_allowed",
                    "stage_status_class",
                    "business_terminal_reason",
                    "technical_failure_reason",
                    "unfinished_symbol_count",
                    "ready_source_counts",
                    "symbol_counts",
                    "wait_evidence_path",
                    "wait_predicate",
                    "wait_pass_micro_generated_at",
                    "wait_pass_micro_state_generated_at",
                    "wait_pass_ready_symbols",
                    "wait_pass_fast_ready_symbols",
                    "wait_pass_full_ready_symbols",
                ):
                    if key in detail:
                        row[key] = detail.get(key)
                if (
                    row.get("done")
                    and row.get("line_lifecycle_status") == "partial_ready"
                    and int(row.get("unfinished_symbol_count") or 0) > 0
                ):
                    row["stage"] = "completed_with_unfinished_symbols"
                if row.get("done"):
                    final_reconcile = _micro_lifecycle_progress_reconcile(
                        root=root,
                        line=current_line,
                        run_id=run_id,
                        cycle_id=cycle_id,
                    )
                    if final_reconcile:
                        wait_result = row.get("wait_result")
                        ready_source_counts = row.get("ready_source_counts")
                        row.update(final_reconcile)
                        if wait_result is not None:
                            row["wait_result"] = wait_result
                        if ready_source_counts is not None:
                            row["ready_source_counts"] = ready_source_counts
            if row["done"]:
                row["run_id"] = run_id
                row["cycle_id"] = cycle_id
        elif stage_name in {"aggregate_final_decisions", "json_stage_audit"} and stage_status == "done":
            for row in line_progress.values():
                if row.get("done"):
                    row["percent"] = 100
        progress_status = "running" if stage_status != "failed" else "failed"
        _write_pipeline_progress(
            root=root,
            run_id=run_id,
            cycle_id=cycle_id,
            line=opts.line,
            mode=opts.mode,
            started_at=started_at,
            status=progress_status,
            current_stage=stage_name,
            current_line=current_line,
            lines=line_progress,
            stages=stages,
            selected_lines=selected_lines,
            requested_interval_sec=opts.requested_interval_sec,
            effective_interval_sec=opts.interval_sec,
            line_runtime_budgets=opts.line_runtime_budgets,
        )
    outputs = {
        "without_micro": str(root / "DATA" / "decisions" / "latest_trade_plan_without_micro.json"),
        "micro_fast": str(root / "DATA" / "decisions" / "latest_trade_plan_micro_fast.json"),
        "micro_full": str(root / "DATA" / "decisions" / "latest_trade_plan_micro_full.json"),
        "strategy_report": str(cfg.strategy_pipeline_report_path),
    }
    if not lock.acquired:
        _write_pipeline_progress(
            root=root,
            run_id=run_id,
            cycle_id=cycle_id,
            line=opts.line,
            mode=opts.mode,
            started_at=started_at,
            status="skipped",
            current_stage="lock_skipped",
            current_line=None,
            lines=line_progress,
            stages=stages,
            selected_lines=selected_lines,
            requested_interval_sec=opts.requested_interval_sec,
            effective_interval_sec=opts.interval_sec,
            line_runtime_budgets=opts.line_runtime_budgets,
        )
        return {
            "schema_version": "1.0",
            "generated_at": to_iso_z(utc_now()),
            "source": "strategy_pipeline",
            "run_id": run_id,
            "cycle_id": cycle_id,
            "line": opts.line,
            "selected_lines": list(selected_lines),
            "skipped_lines": list(skipped_lines),
            "requested_interval_sec": opts.requested_interval_sec,
            "effective_interval_sec": opts.interval_sec,
            "post_run_cooldown_sec": opts.interval_sec,
            "interval_semantics": "post_run_cooldown",
            "line_runtime_budgets": opts.line_runtime_budgets or {},
            "mode": opts.mode,
            "status": "skipped",
            "skip_reason": lock.reason,
            "started_at": started_at,
            "finished_at": to_iso_z(utc_now()),
            "duration_sec": 0,
            "stages": stages,
            "outputs": outputs,
        }

    try:
        for skipped_line in skipped_lines:
            _run_stage(
                stages,
                f"skip_not_selected_{skipped_line}",
                lambda skipped_line=skipped_line: write_blocked_trade_plan_line(
                    line=skipped_line,
                    project_root=root,
                    run_id=run_id,
                    cycle_id=cycle_id,
                    blocked_reason="strategy_line_not_selected",
                    reason_codes=["strategy_line_not_selected"],
                    runtime_health={
                        "selected_lines": list(selected_lines),
                        "skipped_lines": list(skipped_lines),
                    },
                ),
                on_update=None,
            )

        rc = _run_stage_detail(
            stages,
            "common_upstream_step1_to_step2_5",
            lambda: _common_upstream_step1_to_step2_5_detail(root=root, opts=opts),
            on_update=_progress_update,
        )
        upstream_refresh = build_step2_current_freshness(project_root=root)
        if rc == EXIT_SUCCESS:
            for line in selected_lines:
                rc = _run_line(
                    root=root,
                    line=line,
                    cfg=cfg,
                    stages=stages,
                    opts=opts,
                    run_id=run_id,
                    cycle_id=cycle_id,
                    on_update=_progress_update,
                )
                if rc != EXIT_SUCCESS:
                    break

        all_lines_selected = selected_lines == STRATEGY_LINES
        if rc == EXIT_SUCCESS and all_lines_selected and opts.run_abc_audit:
            rc = _run_stage(
                stages,
                "audit_trade_plan_lines",
                lambda: run_audit_trade_plan_lines_safe(project_root=root, stdout_json=False),
                on_update=_progress_update,
            )
            outputs["abc_audit"] = str(root / "DATA" / "reports" / "latest_trade_plan_lines_compare.json")

        if rc == EXIT_SUCCESS and all_lines_selected and opts.aggregate_final_decisions:
            rc = _run_stage(
                stages,
                "aggregate_final_decisions",
                lambda: run_apply_final_decisions_from_trade_plans_safe(project_root=root, stdout_json=False),
                on_update=_progress_update,
            )
            outputs["latest_decisions"] = str(root / "DATA" / "decisions" / "latest_decisions.json")

        if rc == EXIT_SUCCESS and all_lines_selected and opts.run_json_stage_audit:
            rc = _run_stage(stages, "json_stage_audit", lambda: _run_json_stage_audit(root), on_update=_progress_update)
            outputs["json_stage_audit"] = str(_json_audit_path(root))

        micro_quality_attribution = None
        micro_lines_selected = any(line_name in selected_lines for line_name in ("micro_fast", "micro_full"))
        if rc == EXIT_SUCCESS and micro_lines_selected:
            micro_quality_attribution = _append_micro_quality_attribution_stage(
                stages,
                root=root,
                run_id=run_id,
                cycle_id=cycle_id,
                selected_lines=selected_lines,
                on_update=_progress_update,
            )
            outputs["micro_quality_attribution"] = str(root / "DATA" / "reports" / "latest_micro_quality_attribution.json")

        finished_at = to_iso_z(utc_now())
        status = "ok" if rc == EXIT_SUCCESS else "failed"
        for line_name, row in line_progress.items():
            if line_name not in {"micro_fast", "micro_full"} or not row.get("done"):
                continue
            final_reconcile = _micro_lifecycle_progress_reconcile(
                root=root,
                line=line_name,
                run_id=run_id,
                cycle_id=cycle_id,
            )
            if final_reconcile:
                wait_result = row.get("wait_result")
                ready_source_counts = row.get("ready_source_counts")
                row.update(final_reconcile)
                if wait_result is not None:
                    row["wait_result"] = wait_result
                if ready_source_counts is not None:
                    row["ready_source_counts"] = ready_source_counts
        _write_pipeline_progress(
            root=root,
            run_id=run_id,
            cycle_id=cycle_id,
            line=opts.line,
            mode=opts.mode,
            started_at=started_at,
            status=status,
            current_stage=None,
            current_line=None,
            lines=line_progress,
            stages=stages,
            selected_lines=selected_lines,
            requested_interval_sec=opts.requested_interval_sec,
            effective_interval_sec=opts.interval_sec,
            line_runtime_budgets=opts.line_runtime_budgets,
        )
        return {
            "schema_version": "1.0",
            "generated_at": finished_at,
            "source": "strategy_pipeline",
            "run_id": run_id,
            "cycle_id": cycle_id,
            "line": opts.line,
            "selected_lines": list(selected_lines),
            "skipped_lines": list(skipped_lines),
            "requested_interval_sec": opts.requested_interval_sec,
            "effective_interval_sec": opts.interval_sec,
            "post_run_cooldown_sec": opts.interval_sec,
            "interval_semantics": "post_run_cooldown",
            "line_runtime_budgets": opts.line_runtime_budgets or {},
            "mode": opts.mode,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_sec": max(0, int((parse_iso_z(finished_at) - parse_iso_z(started_at)).total_seconds())),
            "stages": stages,
            "upstream_refresh": upstream_refresh,
            "step2_freshness_gates": _step2_freshness_gate_stages(stages),
            "micro_quality_attribution": micro_quality_attribution,
            "outputs": outputs,
        }
    except Exception as exc:
        finished_at = to_iso_z(utc_now())
        if not _first_failed_stage(stages):
            stages.append(
                {
                    "name": "pipeline_unhandled_exception" if stages else "pipeline_bootstrap",
                    "ok": False,
                    "rc": EXIT_INTERNAL,
                    "duration_sec": 0,
                    "error": str(exc),
                    "exception_type": type(exc).__name__,
                }
            )
        try:
            _write_pipeline_progress(
                root=root,
                run_id=run_id,
                cycle_id=cycle_id,
                line=opts.line,
                mode=opts.mode,
                started_at=started_at,
                status="failed",
                current_stage=str((_first_failed_stage(stages) or {}).get("name") or "pipeline_unhandled_exception"),
                current_line=None,
                lines=line_progress,
                stages=stages,
                selected_lines=selected_lines,
                requested_interval_sec=opts.requested_interval_sec,
                effective_interval_sec=opts.interval_sec,
                line_runtime_budgets=opts.line_runtime_budgets,
            )
        except Exception:
            pass
        return {
            "schema_version": "1.0",
            "generated_at": finished_at,
            "source": "strategy_pipeline",
            "run_id": run_id,
            "cycle_id": cycle_id,
            "line": opts.line,
            "selected_lines": list(selected_lines),
            "skipped_lines": list(skipped_lines),
            "requested_interval_sec": opts.requested_interval_sec,
            "effective_interval_sec": opts.interval_sec,
            "post_run_cooldown_sec": opts.interval_sec,
            "interval_semantics": "post_run_cooldown",
            "line_runtime_budgets": opts.line_runtime_budgets or {},
            "mode": opts.mode,
            "status": "failed",
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_sec": max(0, int((parse_iso_z(finished_at) - parse_iso_z(started_at)).total_seconds())),
            "exception_type": type(exc).__name__,
            "exception_summary": str(exc),
            "stages": stages,
            "upstream_refresh": upstream_refresh,
            "step2_freshness_gates": _step2_freshness_gate_stages(stages),
            "outputs": outputs,
        }
    finally:
        release_scheduler_lock(lock)


def _options_from_config(
    *,
    project_root: Path,
    line: StrategyLine | None,
    lines: list[str] | tuple[str, ...] | None,
    mode: StrategyMode | None,
    interval_sec: int | None,
    max_cycles: int | None,
    stdout_json: bool,
    force_universe: bool | None,
    light_limit: int | None,
    fetch_mode: str | None,
    max_concurrency: int | None,
    scan_allow_stale_input: bool | None,
    skip_market_context: bool,
    skip_micro_wait: bool,
    run_abc_audit: bool | None,
    run_json_stage_audit: bool | None,
    aggregate_final_decisions: bool | None,
) -> tuple[StrategyPipelineOptions, EngineConfig]:
    cfg = EngineConfig.load(project_root)
    selected_lines = normalize_strategy_lines(line=line, lines=lines)
    requested_interval = int(interval_sec or cfg.strategy_pipeline_interval_sec)
    effective_interval, line_runtime_budgets = _duration_aware_interval(
        cfg=cfg,
        selected_lines=selected_lines,
        requested_interval_sec=requested_interval,
    )
    opts = StrategyPipelineOptions(
        project_root=project_root,
        line=_display_line_for_selection(selected_lines),
        selected_lines=selected_lines,
        mode=mode or cfg.strategy_pipeline_mode,  # type: ignore[arg-type]
        interval_sec=effective_interval,
        requested_interval_sec=requested_interval,
        line_runtime_budgets=line_runtime_budgets,
        max_cycles=max_cycles,
        overlap_policy=cfg.strategy_pipeline_overlap_policy,
        stdout_json=stdout_json,
        force_universe=cfg.strategy_pipeline_force_universe if force_universe is None else force_universe,
        light_limit=cfg.strategy_pipeline_light_limit if light_limit is None else light_limit,
        fetch_mode=fetch_mode or cfg.strategy_pipeline_fetch_mode,
        max_concurrency=max_concurrency or cfg.strategy_pipeline_max_concurrency,
        scan_allow_stale_input=(
            cfg.strategy_pipeline_scan_allow_stale_input
            if scan_allow_stale_input is None
            else scan_allow_stale_input
        ),
        skip_market_context=skip_market_context,
        skip_micro_wait=skip_micro_wait,
        run_abc_audit=cfg.strategy_pipeline_run_abc_audit if run_abc_audit is None else run_abc_audit,
        run_json_stage_audit=(
            cfg.strategy_pipeline_run_json_stage_audit
            if run_json_stage_audit is None
            else run_json_stage_audit
        ),
        aggregate_final_decisions=(
            cfg.strategy_pipeline_aggregate_final_decisions
            if aggregate_final_decisions is None
            else aggregate_final_decisions
        ),
    )
    return opts, cfg


def run_strategy_pipeline_safe(
    *,
    project_root: Path | None = None,
    line: StrategyLine | None = None,
    lines: list[str] | tuple[str, ...] | None = None,
    mode: StrategyMode | None = None,
    interval_sec: int | None = None,
    max_cycles: int | None = None,
    stdout_json: bool = False,
    force_universe: bool | None = None,
    light_limit: int | None = None,
    fetch_mode: str | None = None,
    max_concurrency: int | None = None,
    scan_allow_stale_input: bool | None = None,
    skip_market_context: bool = False,
    skip_micro_wait: bool = False,
    run_abc_audit: bool | None = None,
    run_json_stage_audit: bool | None = None,
    aggregate_final_decisions: bool | None = None,
) -> int:
    root = (project_root or Path.cwd()).resolve()
    opts, cfg = _options_from_config(
        project_root=root,
        line=line,
        lines=lines,
        mode=mode,
        interval_sec=interval_sec,
        max_cycles=max_cycles,
        stdout_json=stdout_json,
        force_universe=force_universe,
        light_limit=light_limit,
        fetch_mode=fetch_mode,
        max_concurrency=max_concurrency,
        scan_allow_stale_input=scan_allow_stale_input,
        skip_market_context=skip_market_context,
        skip_micro_wait=skip_micro_wait,
        run_abc_audit=run_abc_audit,
        run_json_stage_audit=run_json_stage_audit,
        aggregate_final_decisions=aggregate_final_decisions,
    )
    cycles = 0
    last_status = "unknown"
    while True:
        report = _run_once(opts, cfg)
        last_status = str(report.get("status", "unknown"))
        if opts.mode == "interval":
            report["next_run_at"] = to_iso_z(utc_now() + timedelta(seconds=opts.interval_sec))
        _enrich_pipeline_report_failure_metadata(root, report, latest_report_path=cfg.strategy_pipeline_report_path)
        if last_status == "failed":
            try:
                ledger_result = ingest_failed_pipeline_run_to_sqlite(root, report=report)
                report["failed_run_ledger"] = ledger_result
            except Exception as exc:
                report["failed_run_ledger"] = {
                    "status": "failed",
                    "error": str(exc),
                    "reason_codes": ["failed_run_sqlite_minimal_ledger_failed"],
                }
        _write_pipeline_report_archive(root, report)
        write_json_atomic(cfg.strategy_pipeline_report_path, report)
        if last_status != "failed":
            try:
                settlement = _run_paper_settlement_barrier(root, report)
                report["paper_settlement_barrier"] = settlement
                _write_pipeline_report_archive(root, report)
                write_json_atomic(cfg.strategy_pipeline_report_path, report)
                audit_payload = write_run_level_audit(
                    root,
                    run_id=str(report.get("run_id") or ""),
                    cycle_id=str(report.get("cycle_id") or ""),
                )
                ledger_result = ingest_run_audit_to_sqlite(root, audit_path=root / "DATA/reports/latest_run_audit.json")
                report["run_audit_ledger"] = {
                    "status": "ok",
                    "run_id": audit_payload.get("run_id"),
                    "audit_status": audit_payload.get("status"),
                    **ledger_result,
                }
            except Exception as exc:
                report["run_audit_ledger"] = {
                    "status": "failed",
                    "error": str(exc),
                    "reason_codes": ["run_audit_sqlite_auto_ingest_failed"],
                }
            _write_pipeline_report_archive(root, report)
            write_json_atomic(cfg.strategy_pipeline_report_path, report)
        if stdout_json:
            print(
                json.dumps(
                    {
                        "step": "STEP11.0",
                        "status": report.get("status"),
                        "line": opts.line,
                        "selected_lines": list(opts.selected_lines),
                        "mode": opts.mode,
                        "requested_interval_sec": opts.requested_interval_sec,
                        "effective_interval_sec": opts.interval_sec,
                        "post_run_cooldown_sec": opts.interval_sec,
                        "interval_semantics": "post_run_cooldown",
                        "run_id": report.get("run_id"),
                        "output": str(cfg.strategy_pipeline_report_path),
                        "archive_report_path": ((report.get("outputs") or {}).get("pipeline_report_archive")),
                        "first_failed_stage": report.get("first_failed_stage"),
                        "exception_summary": report.get("exception_summary"),
                    },
                    ensure_ascii=False,
                ),
            )
        cycles += 1
        if opts.mode == "once" or (opts.max_cycles is not None and cycles >= opts.max_cycles):
            break
        time.sleep(max(1, opts.interval_sec))
    return EXIT_SUCCESS if last_status in ("ok", "skipped") else EXIT_CONFIG
