"""P10.5 read-only ABC audit for independent trade plan lines."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanLineDocument
from laoma_signal_engine.decision.trade_plan_lines import default_output_path


def _fail(name: str, detail: Any) -> dict[str, Any]:
    return {"name": name, "ok": False, "detail": detail}


def _pass(name: str, detail: Any = None) -> dict[str, Any]:
    return {"name": name, "ok": True, "detail": detail}


def _warning(name: str, detail: Any) -> dict[str, Any]:
    return {"name": name, "ok": True, "warning": True, "detail": detail}


def _lineage_status(
    *,
    expected_lines: tuple[str, ...],
    run_ids: dict[str, str | None],
    cycle_ids: dict[str, str | None],
) -> str:
    if not any(run_ids.get(line) or cycle_ids.get(line) for line in expected_lines):
        return "no_lineage"
    missing = [line for line in expected_lines if not run_ids.get(line) or not cycle_ids.get(line)]
    if missing:
        return "partial_missing"
    if len({run_ids[line] for line in expected_lines}) > 1:
        return "mixed_run_id"
    if len({cycle_ids[line] for line in expected_lines}) > 1:
        return "mixed_cycle_id"
    return "aligned"


def _is_relaxed_micro_gate(plan_guards: dict[str, Any]) -> bool:
    snap = plan_guards.get("gate_config_snapshot") or {}
    policy = str(plan_guards.get("micro_consumption_policy") or snap.get("micro_consumption_policy") or "confirmed_only")
    return (
        bool(plan_guards.get("micro_policy_relaxed"))
        or policy != "confirmed_only"
        or (
            snap.get("require_micro_ready") is False
            and snap.get("require_micro_alignment") is False
        )
    )


def _load_doc(path: Path) -> tuple[TradePlanLineDocument | None, list[dict[str, Any]]]:
    checks: list[dict[str, Any]] = []
    if not path.is_file():
        return None, [_fail("file.exists", str(path))]
    try:
        doc = TradePlanLineDocument.model_validate(read_json_object(path))
    except (OSError, ValueError, ValidationError) as exc:
        return None, [_fail("file.contract", str(exc))]
    checks.append(_pass("file.contract", str(path)))
    return doc, checks


def build_trade_plan_lines_audit(*, project_root: Path, generated_at: str) -> dict[str, Any]:
    paths = {
        "without_micro": default_output_path(project_root, "without_micro"),
        "micro_fast": default_output_path(project_root, "micro_fast"),
        "micro_full": default_output_path(project_root, "micro_full"),
    }
    docs: dict[str, TradePlanLineDocument] = {}
    checks: list[dict[str, Any]] = []
    for line, path in paths.items():
        doc, line_checks = _load_doc(path)
        checks.extend([{**c, "line": line} for c in line_checks])
        if doc is not None:
            docs[line] = doc

    expected = {
        "without_micro": ("trade_plan_without_micro", "none"),
        "micro_fast": ("trade_plan_micro_fast", "fast"),
        "micro_full": ("trade_plan_micro_full", "full"),
    }
    for line, doc in docs.items():
        source, mode = expected[line]
        checks.append(
            _pass("line.source_mode", {"source": doc.source, "micro_mode": doc.micro_mode})
            if doc.source == source and doc.micro_mode == mode
            else _fail("line.source_mode", {"source": doc.source, "micro_mode": doc.micro_mode})
        )
        if line == "without_micro":
            leaks = [
                p.symbol
                for p in doc.plans
                if "micro_generated_at" in p.input_refs or p.guards.get("micro_ready") is not None
            ]
            checks.append(_pass("without_micro.no_micro_refs") if not leaks else _fail("without_micro.no_micro_refs", leaks))
        if line in ("micro_fast", "micro_full"):
            state_refs = [p.input_refs.get("micro_state_generated_at") for p in doc.plans]
            checks.append(
                _pass("micro_line.state_refs_present", line)
                if all(v is not None for v in state_refs)
                else _pass("micro_line.state_missing_fallback", line)
            )
        for plan in doc.plans:
            opportunity_type = plan.guards.get("opportunity_type")
            opportunity_level = plan.guards.get("opportunity_level")
            if opportunity_type is None or opportunity_level is None:
                checks.append(_fail("plan.opportunity_contract_present", plan.symbol))
            else:
                checks.append(
                    _pass(
                        "plan.opportunity_contract_present",
                        {
                            "symbol": plan.symbol,
                            "opportunity_type": opportunity_type,
                            "opportunity_level": opportunity_level,
                        },
                    ),
                )
            if plan.executable:
                complete = all(
                    v is not None
                    for v in [
                        plan.estimated_entry_price,
                        plan.stop_loss,
                        plan.take_profit,
                        plan.risk_per_unit,
                        plan.reward_per_unit,
                        plan.rr,
                    ]
                )
                checks.append(
                    _pass("plan.executable_has_prices", plan.symbol)
                    if complete
                    else _fail("plan.executable_has_prices", plan.symbol)
                )
                net_rr = plan.guards.get("net_rr")
                min_net_rr = plan.guards.get("min_net_rr")
                gross_risk_bps = plan.guards.get("gross_risk_bps")
                noise_floor_bps = plan.guards.get("noise_floor_bps")
                available_room_bps = plan.guards.get("available_room_bps")
                required_reward_bps = plan.guards.get("required_reward_bps")
                sl_tp_model_version = plan.guards.get("sl_tp_model_version")
                effective_rr = plan.guards.get("effective_rr")
                min_effective_rr = plan.guards.get("min_effective_rr")
                single_tp_reachable = plan.guards.get("single_tp_reachable")
                quality_ok = (
                    sl_tp_model_version in ("10.9", "10.63")
                    and net_rr is not None
                    and min_net_rr is not None
                    and float(net_rr) >= float(min_net_rr)
                    and gross_risk_bps is not None
                    and noise_floor_bps is not None
                    and float(gross_risk_bps) >= float(noise_floor_bps)
                    and available_room_bps is not None
                    and required_reward_bps is not None
                    and float(available_room_bps) >= float(required_reward_bps)
                    and (
                        sl_tp_model_version != "10.63"
                        or (
                            effective_rr is not None
                            and min_effective_rr is not None
                            and float(effective_rr) >= float(min_effective_rr)
                            and single_tp_reachable is True
                            and plan.guards.get("tp_model") == "single_reachable_tp"
                            and plan.guards.get("tp2") is None
                        )
                    )
                )
                checks.append(
                    _pass("plan.executable_step109_quality", plan.symbol)
                    if quality_ok
                    else _fail("plan.executable_step109_quality", {"symbol": plan.symbol, "guards": plan.guards})
                )
                if line in ("micro_fast", "micro_full"):
                    micro_exec_ok = (
                        plan.guards.get("micro_signal_missing") is False
                        and plan.guards.get("micro_signal_usable") is True
                        and plan.guards.get("micro_direction_confirmed") is True
                        and plan.guards.get("micro_exec_allowed") is True
                    )
                    micro_lifecycle_ok = (
                        plan.guards.get("micro_lifecycle_scope") == "symbol"
                        and plan.guards.get("micro_lifecycle_state") in ("confirmed", "emitted")
                        and plan.guards.get("micro_symbol_ready") is True
                        and (
                            plan.guards.get("micro_symbol_confirmed") is True
                            or (
                                _is_relaxed_micro_gate(plan.guards)
                                and plan.guards.get("trade_plan_consumable") is True
                            )
                        )
                    )
                    if micro_exec_ok:
                        checks.append(_pass("plan.executable_micro_signal_contract", plan.symbol))
                    elif _is_relaxed_micro_gate(plan.guards):
                        checks.append(
                            _warning(
                                "relaxed_micro_confirmation_bypass",
                                {
                                    "line": line,
                                    "symbol": plan.symbol,
                                    "run_id": doc.run_id,
                                    "cycle_id": doc.cycle_id,
                                    "micro_ready": plan.guards.get("micro_ready"),
                                    "micro_signal_usable": plan.guards.get("micro_signal_usable"),
                                    "micro_direction_confirmed": plan.guards.get("micro_direction_confirmed"),
                                    "micro_exec_allowed": plan.guards.get("micro_exec_allowed"),
                                    "gate_config_snapshot": plan.guards.get("gate_config_snapshot"),
                                },
                            )
                        )
                    else:
                        checks.append(
                            _fail(
                                "plan.executable_micro_signal_contract",
                                {"symbol": plan.symbol, "guards": plan.guards},
                            )
                        )
                    checks.append(
                        _pass("plan.executable_micro_symbol_lifecycle_contract", plan.symbol)
                        if micro_lifecycle_ok
                        else _fail(
                            "plan.executable_micro_symbol_lifecycle_contract",
                            {"symbol": plan.symbol, "guards": plan.guards},
                        )
                    )
            if line in ("micro_fast", "micro_full"):
                has_signal_contract = all(
                    key in plan.guards
                    for key in (
                        "micro_signal_missing",
                        "micro_signal_usable",
                        "micro_direction_confirmed",
                        "micro_exec_allowed",
                        "micro_alignment_state",
                        "micro_strength",
                    )
                )
                checks.append(
                    _pass("plan.micro_signal_contract_present", plan.symbol)
                    if has_signal_contract
                    else _fail("plan.micro_signal_contract_present", {"symbol": plan.symbol, "guards": plan.guards})
                )
                has_lifecycle_contract = all(
                    key in plan.guards
                    for key in (
                        "micro_lifecycle_scope",
                        "micro_lifecycle_state",
                        "micro_symbol_ready",
                        "micro_symbol_confirmed",
                        "micro_symbol_trade_plan_emitted",
                        "micro_symbol_reason_codes",
                    )
                )
                checks.append(
                    _pass("plan.micro_symbol_lifecycle_contract_present", plan.symbol)
                    if has_lifecycle_contract
                    else _fail(
                        "plan.micro_symbol_lifecycle_contract_present",
                        {"symbol": plan.symbol, "guards": plan.guards},
                    )
                )
            if plan.guards.get("opportunity_type") in {"LIMIT_PULLBACK", "LIMIT_REBOUND"}:
                checks.append(
                    _pass("plan.limit_opportunity_has_better_entry", plan.symbol)
                    if plan.guards.get("better_entry_price") is not None
                    else _fail("plan.limit_opportunity_has_better_entry", {"symbol": plan.symbol, "guards": plan.guards})
                )
            if plan.guards.get("opportunity_type") in {"BREAKOUT_TRIGGER", "BREAKDOWN_TRIGGER"}:
                checks.append(
                    _pass("plan.trigger_opportunity_has_trigger", plan.symbol)
                    if plan.guards.get("trigger_price") is not None
                    else _fail("plan.trigger_opportunity_has_trigger", {"symbol": plan.symbol, "guards": plan.guards})
                )
            if plan.action == "NO_TRADE":
                checks.append(
                    _pass("plan.no_trade_has_reasons", plan.symbol)
                    if plan.reason_codes
                    else _fail("plan.no_trade_has_reasons", plan.symbol)
                )

    micro_targets_path = project_root / "DATA" / "micro" / "micro_targets.json"
    try:
        micro_targets = read_json_object(micro_targets_path) if micro_targets_path.is_file() else {}
    except (OSError, ValueError, TypeError):
        micro_targets = {}
    if isinstance(micro_targets, dict) and micro_targets.get("status") == "stale_input":
        for line in ("micro_fast", "micro_full"):
            doc = docs.get(line)
            ok = (
                doc is not None
                and doc.status == "blocked"
                and doc.input_refs.get("blocked_reason") == "upstream_step2_stale"
            )
            checks.append(
                _pass("micro_targets.stale_input_blocks_line", line)
                if ok
                else _fail(
                    "micro_targets.stale_input_blocks_line",
                    {
                        "line": line,
                        "expected": "blocked/upstream_step2_stale",
                        "actual_status": doc.status if doc is not None else None,
                        "actual_blocked_reason": (
                            doc.input_refs.get("blocked_reason") if doc is not None else None
                        ),
                    },
                )
            )

    micro_full_doc = docs.get("micro_full")
    if (
        micro_full_doc is not None
        and micro_full_doc.status == "blocked"
        and micro_full_doc.input_refs.get("blocked_reason") == "micro_full_wait_timeout"
    ):
        lifecycle_path = project_root / "DATA" / "micro" / "latest_micro_lifecycle_micro_full.json"
        try:
            lifecycle = read_json_object(lifecycle_path) if lifecycle_path.is_file() else {}
        except (OSError, ValueError, TypeError):
            lifecycle = {}
        timeout_count = 0
        if isinstance(lifecycle, dict):
            state_counts = lifecycle.get("state_counts")
            if isinstance(state_counts, dict):
                timeout_count = int(state_counts.get("timeout") or 0)
        target_set_id = micro_full_doc.input_refs.get("micro_target_set_id")
        ledger_ok = (
            isinstance(lifecycle, dict)
            and lifecycle.get("strategy_line") == "micro_full"
            and lifecycle.get("run_id") == micro_full_doc.run_id
            and lifecycle.get("cycle_id") == micro_full_doc.cycle_id
            and lifecycle.get("target_set_id") == target_set_id
            and timeout_count >= 1
        )
        checks.append(
            _pass(
                "micro_full.timeout_lifecycle_ledger",
                {
                    "path": str(lifecycle_path),
                    "timeout_count": timeout_count,
                    "target_set_id": target_set_id,
                },
            )
            if ledger_ok
            else _fail(
                "micro_full.timeout_lifecycle_ledger",
                {
                    "path": str(lifecycle_path),
                    "exists": lifecycle_path.is_file(),
                    "timeout_count": timeout_count,
                    "expected_run_id": micro_full_doc.run_id,
                    "actual_run_id": lifecycle.get("run_id") if isinstance(lifecycle, dict) else None,
                    "expected_cycle_id": micro_full_doc.cycle_id,
                    "actual_cycle_id": lifecycle.get("cycle_id") if isinstance(lifecycle, dict) else None,
                    "expected_target_set_id": target_set_id,
                    "actual_target_set_id": lifecycle.get("target_set_id") if isinstance(lifecycle, dict) else None,
                },
            )
        )

    expected_lines = ("without_micro", "micro_fast", "micro_full")
    run_ids_all = {line: (docs[line].run_id if line in docs else None) for line in expected_lines}
    cycle_ids_all = {line: (docs[line].cycle_id if line in docs else None) for line in expected_lines}
    run_ids = {line: doc.run_id for line, doc in docs.items() if doc.run_id is not None}
    cycle_ids = {line: doc.cycle_id for line, doc in docs.items() if doc.cycle_id is not None}
    lineage_status = _lineage_status(
        expected_lines=tuple(line for line in expected_lines if line in docs),
        run_ids=run_ids_all,
        cycle_ids=cycle_ids_all,
    )
    if run_ids:
        checks.append(
            _pass("line.run_id_consistent", run_ids)
            if len(set(run_ids.values())) == 1
            else _fail("line.run_id_consistent", run_ids)
        )
    if cycle_ids:
        checks.append(
            _pass("line.cycle_id_consistent", cycle_ids)
            if len(set(cycle_ids.values())) == 1
            else _fail("line.cycle_id_consistent", cycle_ids)
        )
    if lineage_status == "aligned" or lineage_status == "no_lineage":
        checks.append(_pass("lineage.status", lineage_status))
    else:
        checks.append(
            _warning(
                "lineage.status",
                {"status": lineage_status, "run_ids": run_ids_all, "cycle_ids": cycle_ids_all},
            )
        )
    state_refs_by_line = {
        line: doc.input_refs.get("micro_state_generated_at")
        for line, doc in docs.items()
        if line in ("micro_fast", "micro_full")
    }
    present_state_refs = {line: ref for line, ref in state_refs_by_line.items() if ref is not None}
    if len(present_state_refs) == 2:
        checks.append(
            _pass(
                "micro_lines.independent_state_refs",
                {
                    "refs": present_state_refs,
                    "note": "micro_fast and micro_full are independent pipeline lines; matching timestamps are not required",
                },
            )
        )

    all_symbols = sorted({p.symbol for doc in docs.values() for p in doc.plans})
    comparisons: list[dict[str, Any]] = []
    for sym in all_symbols:
        row: dict[str, Any] = {"symbol": sym}
        for line, doc in docs.items():
            plan = next((p for p in doc.plans if p.symbol == sym), None)
            if plan is not None:
                row[line] = {
                    "decision": plan.decision,
                    "action": plan.action,
                    "entry_mode": plan.entry_mode,
                    "executable": plan.executable,
                    "opportunity_type": plan.guards.get("opportunity_type"),
                    "opportunity_level": plan.guards.get("opportunity_level"),
                    "net_rr": plan.guards.get("net_rr"),
                    "reason_codes": plan.reason_codes,
                }
        comparisons.append(row)

    opportunity_distribution = {
        line: dict(Counter(str(plan.guards.get("opportunity_type", "missing")) for plan in doc.plans))
        for line, doc in docs.items()
    }
    failure_count = sum(1 for c in checks if not c["ok"])
    warnings = [c for c in checks if c.get("warning") is True]
    warning_count = len(warnings)
    audit_profile = "relaxed" if any(c["name"] == "relaxed_micro_confirmation_bypass" for c in warnings) else "strict"
    top_run_id = next(iter(set(run_ids.values())), None) if lineage_status == "aligned" and run_ids else None
    top_cycle_id = next(iter(set(cycle_ids.values())), None) if lineage_status == "aligned" and cycle_ids else None
    return {
        "schema_version": "1.0",
        "generated_at": generated_at,
        "source": "trade_plan_lines_abc_audit",
        "run_id": top_run_id,
        "cycle_id": top_cycle_id,
        "line_run_ids": run_ids_all,
        "line_cycle_ids": cycle_ids_all,
        "lineage_status": lineage_status,
        "status": "failed" if failure_count else ("warning" if warning_count else "ok"),
        "audit_profile": audit_profile,
        "failure_count": failure_count,
        "warning_count": warning_count,
        "warnings": warnings,
        "line_counts": {
            line: {
                "status": doc.status,
                "count": doc.count,
                "executable_count": doc.executable_count,
                "run_id": doc.run_id,
                "cycle_id": doc.cycle_id,
                "opportunity_distribution": opportunity_distribution.get(line, {}),
            }
            for line, doc in docs.items()
        },
        "opportunity_distribution": opportunity_distribution,
        "checks": checks,
        "comparisons": comparisons,
    }


def run_audit_trade_plan_lines_safe(
    *,
    project_root: Path | None = None,
    output_path: Path | None = None,
    stdout_json: bool = False,
) -> int:
    root = (project_root or Path.cwd()).resolve()
    out_p = output_path or (root / "DATA" / "reports" / "latest_trade_plan_lines_compare.json")
    try:
        report = build_trade_plan_lines_audit(project_root=root, generated_at=to_iso_z(utc_now()))
        out_p.parent.mkdir(parents=True, exist_ok=True)
        write_json_atomic(out_p, report)
        if stdout_json:
            print(
                json.dumps(
                    {
                        "step": "STEP10.5",
                        "status": report["status"],
                        "failure_count": report["failure_count"],
                        "output": str(out_p),
                    },
                    ensure_ascii=False,
                ),
            )
        return EXIT_SUCCESS if report["failure_count"] == 0 else EXIT_CONFIG
    except (OSError, ValueError, ValidationError) as e:
        print(f"[ERROR] trade plan lines audit failed: {e}", file=sys.stderr)
        return EXIT_CONFIG if isinstance(e, ValidationError) else EXIT_INTERNAL
