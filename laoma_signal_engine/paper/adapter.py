"""Adapt P10 trade plan line JSON into P14 paper intents."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from laoma_signal_engine.core.symbol_contract import validate_exchange_symbol
from laoma_signal_engine.decision.trade_plan_archive import trade_plan_source_plan_hash
from laoma_signal_engine.paper.models import STRATEGY_LINES, PaperConfig, PaperIntent
from laoma_signal_engine.paper.utils import read_json


LINE_TO_SOURCE = {
    "without_micro": "trade_plan_without_micro",
    "micro_fast": "trade_plan_micro_fast",
    "micro_full": "trade_plan_micro_full",
    "strategy4": "trade_plan_strategy4",
    "strategy5": "trade_plan_strategy5",
    "strategy6": "trade_plan_strategy6",
}
SOURCE_TO_LINE = {v: k for k, v in LINE_TO_SOURCE.items()}
LINE_TO_PATH = {
    "without_micro": "DATA/decisions/latest_trade_plan_without_micro.json",
    "micro_fast": "DATA/decisions/latest_trade_plan_micro_fast.json",
    "micro_full": "DATA/decisions/latest_trade_plan_micro_full.json",
    "strategy4": "DATA/decisions/latest_trade_plan_strategy4.json",
    "strategy5": "DATA/decisions/latest_trade_plan_strategy5.json",
    "strategy6": "DATA/decisions/latest_trade_plan_strategy6.json",
}


def load_trade_plan_documents(project_root: Path) -> dict[str, dict[str, Any]]:
    docs: dict[str, dict[str, Any]] = {}
    for line, rel in LINE_TO_PATH.items():
        path = project_root / rel
        if path.exists():
            data = read_json(path)
            if isinstance(data, dict):
                docs[line] = data | {"__source_path": str(path), "__project_root": str(project_root)}
    return docs


def intents_from_documents(
    docs: dict[str, dict[str, Any]],
    *,
    config: PaperConfig | None = None,
) -> list[PaperIntent]:
    return adapt_documents(docs, config=config)["intents"]


def adapt_documents(
    docs: dict[str, dict[str, Any]],
    *,
    config: PaperConfig | None = None,
) -> dict[str, Any]:
    cfg = config or PaperConfig()
    intents: list[PaperIntent] = []
    skipped: list[dict[str, Any]] = []
    for line in STRATEGY_LINES:
        doc = docs.get(line)
        if not doc:
            continue
        for plan in doc.get("plans") or []:
            intent, reason = intent_from_plan_document(line, doc, plan, config=cfg, with_reason=True)
            if intent:
                intents.append(intent)
            else:
                skipped.append(_skip_row(line, doc, plan, reason))
    return {"intents": intents, "skipped": skipped}


def intent_from_plan_document(
    line: str,
    doc: dict[str, Any],
    plan: dict[str, Any],
    *,
    config: PaperConfig,
    with_reason: bool = False,
) -> PaperIntent | tuple[PaperIntent | None, str] | None:
    intent, reason = _intent_from_plan_document(line, doc, plan, config=config)
    return (intent, reason) if with_reason else intent


def _intent_from_plan_document(
    line: str,
    doc: dict[str, Any],
    plan: dict[str, Any],
    *,
    config: PaperConfig,
) -> tuple[PaperIntent | None, str]:
    if line not in STRATEGY_LINES:
        return None, "invalid_strategy_line"
    decision = str(plan.get("decision") or "").upper()
    if decision not in {"LONG", "SHORT"}:
        return None, "non_entry_decision"

    guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
    symbol_contract = _symbol_contract_for_doc(doc, plan)
    if not symbol_contract.ok:
        return None, "paper_reject_invalid_exchange_symbol"
    guards = {**guards, **symbol_contract.guards()}
    action = str(plan.get("action") or "").upper()
    entry_mode = str(plan.get("entry_mode") or "").upper()
    executable = bool(plan.get("executable"))
    entry_price = _float(plan.get("estimated_entry_price"))
    stop_loss = _float(plan.get("stop_loss"))
    take_profit = _float(plan.get("take_profit"))
    intent_type = "MARKET_EXECUTABLE"
    signal_class = _signal_class_for_plan(line, guards)
    notify_eligible = executable

    if not executable:
        return None, "non_executable"
    micro_skip_reason = _micro_line_skip_reason(line, guards)
    if micro_skip_reason:
        return None, micro_skip_reason
    if action != "ENTER_MARKET" or entry_mode != "MARKET":
        return None, "pending_not_allowed"
    order_type = "market"
    paper_eligible = True
    if entry_price is None or stop_loss is None or take_profit is None:
        return None, "missing_price_contract"
    if entry_price <= 0 or stop_loss <= 0 or take_profit <= 0:
        return None, "missing_price_contract"
    position_sizing = plan.get("position_sizing") if isinstance(plan.get("position_sizing"), dict) else {}
    if config.require_position_sizing and not position_sizing:
        return None, "position_sizing_missing"
    if not position_sizing and not config.paper_fallback_notional_allowed:
        return None, "paper_fallback_notional_disallowed"
    sizing_reject = position_sizing.get("sizing_reject_reason") if position_sizing else None
    if sizing_reject:
        return None, str(sizing_reject)
    sizing_leverage = _float(position_sizing.get("leverage")) if position_sizing else None
    sizing_margin = _float(position_sizing.get("margin_usdt")) if position_sizing else None
    planned_quantity = None
    planned_notional = None
    if position_sizing:
        planned_quantity = _float(position_sizing.get("planned_quantity")) or _float(position_sizing.get("quantity"))
        planned_notional = _float(position_sizing.get("planned_notional_usdt")) or _float(
            position_sizing.get("notional_usdt"),
        )
    estimated_max_loss = _float(position_sizing.get("estimated_max_loss_usdt")) if position_sizing else None
    risk_budget = _float(position_sizing.get("risk_budget_usdt")) if position_sizing else None
    if position_sizing and (
        planned_quantity is None
        or planned_quantity <= 0
        or planned_notional is None
        or planned_notional <= 0
        or sizing_margin is None
        or sizing_margin <= 0
    ):
        return None, "position_sizing_invalid"

    source = str(doc.get("source") or LINE_TO_SOURCE[line])
    source_path = _source_path_for_doc(doc, plan, line)
    source_plan_hash = _source_plan_hash(line, doc, plan)
    lineage = _strategy4_lineage(line, doc, plan, guards)
    source_run_id = _paper_source_run_id(line, doc, lineage)
    source_cycle_id = _paper_source_cycle_id(line, doc, lineage)
    source_json = {
        **plan,
        "paper_eligible": paper_eligible,
        "notify_eligible": notify_eligible,
        "signal_class": signal_class,
        "source_executable": executable,
        "source_action": action,
        "source_entry_mode": entry_mode,
        "source_path": source_path,
        "source_archive_path": source_path,
        "symbol_contract": symbol_contract.guards(),
    }
    if lineage:
        source_json["strategy4_lineage"] = lineage
        guards = {**guards, "strategy4_lineage": lineage}
    return PaperIntent(
        strategy_line=line,
        source=source,
        source_path=source_path,
        source_run_id=source_run_id,
        source_cycle_id=source_cycle_id,
        source_generated_at=doc.get("generated_at"),
        source_plan_hash=source_plan_hash,
        signal_class=signal_class,
        paper_eligible=paper_eligible,
        notify_eligible=notify_eligible,
        source_executable=executable,
        source_action=action,
        source_entry_mode=entry_mode,
        symbol=symbol_contract.normalized_symbol,
        side=decision,
        order_type=order_type,
        intent_type=intent_type,
        opportunity_type=str(guards.get("opportunity_type") or entry_mode or intent_type),
        reference_price=entry_price,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        tp1=_float(guards.get("tp1")),
        leverage=float(sizing_leverage or guards.get("leverage") or config.default_leverage),
        margin_usdt=float(sizing_margin or guards.get("margin_usdt") or config.default_margin_usdt),
        sizing_method=str(position_sizing.get("method") or "") if position_sizing else None,
        risk_budget_usdt=risk_budget,
        planned_quantity=planned_quantity,
        planned_notional_usdt=planned_notional,
        estimated_max_loss_usdt=estimated_max_loss,
        reason_codes=list(plan.get("reason_codes") or []),
        guards=guards,
        source_json=source_json,
    ), ""


def _skip_row(line: str, doc: dict[str, Any], plan: dict[str, Any], reason: str) -> dict[str, Any]:
    guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
    symbol_contract = _symbol_contract_for_doc(doc, plan)
    lineage = _strategy4_lineage(line, doc, plan, guards)
    source_json = {
        **plan,
        "source_path": _source_path_for_doc(doc, plan, line),
        "source_archive_path": _source_path_for_doc(doc, plan, line),
        "symbol_contract": symbol_contract.guards(),
    }
    if lineage:
        source_json["strategy4_lineage"] = lineage
    return {
        "strategy_line": line,
        "symbol": symbol_contract.normalized_symbol or str(plan.get("symbol") or "").upper(),
        "side": str(plan.get("decision") or "").upper(),
        "source_run_id": _paper_source_run_id(line, doc, lineage),
        "source_cycle_id": _paper_source_cycle_id(line, doc, lineage),
        "source_generated_at": doc.get("generated_at"),
        "source_path": _source_path_for_doc(doc, plan, line),
        "source_archive_path": _source_path_for_doc(doc, plan, line),
        "source_plan_hash": _source_plan_hash(line, doc, plan),
        "source_executable": bool(plan.get("executable")),
        "source_action": str(plan.get("action") or "").upper(),
        "source_entry_mode": str(plan.get("entry_mode") or "").upper(),
        "opportunity_type": str(guards.get("opportunity_type") or ""),
        "micro_symbol_confirmed": guards.get("micro_symbol_confirmed"),
        "micro_direction_confirmed": guards.get("micro_direction_confirmed"),
        "micro_exec_allowed": guards.get("micro_exec_allowed"),
        "micro_exec_allowed_reason": guards.get("micro_exec_allowed_reason"),
        "micro_consumption_policy": guards.get("micro_consumption_policy"),
        "allow_weak_micro_consumption": guards.get("allow_weak_micro_consumption"),
        "micro_policy_relaxed": guards.get("micro_policy_relaxed"),
        "micro_confirmation_strength": guards.get("micro_confirmation_strength"),
        "trade_plan_consumable": guards.get("trade_plan_consumable"),
        "consumption_block_reason": guards.get("consumption_block_reason"),
        "symbol_contract_ok": symbol_contract.ok,
        "symbol_contract_reason": symbol_contract.reason,
        "symbol_contract_source": symbol_contract.source,
        "symbol_raw": symbol_contract.raw_symbol,
        "symbol_normalized": symbol_contract.normalized_symbol,
        "skip_reason": reason,
        "reason_codes": list(plan.get("reason_codes") or []),
        "source_json": source_json,
    }


def _symbol_contract_for_doc(doc: dict[str, Any], plan: dict[str, Any]):
    root_raw = doc.get("__project_root")
    root = Path(str(root_raw)).resolve() if root_raw else None
    return validate_exchange_symbol(
        plan.get("symbol"),
        project_root=root,
        fail_closed_on_missing_whitelist=root is not None,
    )


def _source_path_for_doc(doc: dict[str, Any], plan: dict[str, Any], line: str) -> str:
    plan_refs = plan.get("input_refs") if isinstance(plan.get("input_refs"), dict) else {}
    doc_refs = doc.get("input_refs") if isinstance(doc.get("input_refs"), dict) else {}
    if line in {"strategy5", "strategy6"}:
        key = f"{line}_trade_plan_latest_path"
        return str(doc.get("__source_path") or doc_refs.get(key) or LINE_TO_PATH[line])
    return str(
        plan_refs.get("trade_plan_archive_path")
        or doc_refs.get("trade_plan_archive_path")
        or doc.get("__source_path")
        or LINE_TO_PATH[line]
    )


def _micro_line_skip_reason(line: str, guards: dict[str, Any]) -> str:
    if line in {"without_micro", "strategy4", "strategy5", "strategy6"}:
        return ""
    if guards.get("trade_plan_consumable") is not True:
        return "paper_reject_micro_consumable_false"
    policy = _micro_consumption_policy(guards)
    if policy == "audit_only":
        return "paper_reject_micro_policy_audit_only"
    if policy == "confirmed_only":
        if guards.get("micro_symbol_confirmed") is not True:
            return "paper_reject_confirmed_only_non_confirmed_micro_symbol"
        if guards.get("micro_direction_confirmed") is not True:
            return "paper_reject_micro_direction_not_confirmed"
        if guards.get("micro_exec_allowed") is not True:
            return "paper_reject_micro_exec_not_allowed"
        return ""
    if policy in {"ready_signal_usable", "weak_ready_test"}:
        if guards.get("micro_symbol_confirmed") is True:
            return ""
        if guards.get("allow_weak_micro_consumption") is not True:
            return "paper_reject_relaxed_micro_policy_not_enabled"
        if not (guards.get("micro_policy_relaxed") is True or guards.get("micro_confirmation_strength") == "weak"):
            return "paper_reject_relaxed_micro_missing_evidence"
        if str(guards.get("consumption_block_reason") or ""):
            return "paper_reject_relaxed_micro_has_block_reason"
        if str(guards.get("micro_lifecycle_state") or "") in {"observing", "timeout", "rejected", "not_ready"}:
            return "paper_reject_relaxed_micro_non_terminal_consumable"
        return ""
    return "paper_reject_unknown_micro_consumption_policy"


def _micro_consumption_policy(guards: dict[str, Any]) -> str:
    snap = guards.get("gate_config_snapshot") if isinstance(guards.get("gate_config_snapshot"), dict) else {}
    policy = str(guards.get("micro_consumption_policy") or snap.get("micro_consumption_policy") or "confirmed_only")
    if policy not in {"confirmed_only", "ready_signal_usable", "weak_ready_test", "audit_only"}:
        return "confirmed_only"
    return policy


def _signal_class_for_plan(line: str, guards: dict[str, Any]) -> str:
    if line == "strategy4":
        return "strategy4_observe"
    if line == "strategy5":
        return "strategy5_direction_evidence"
    if line == "strategy6":
        return "strategy6_market_accepted_entry"
    if line in {"micro_fast", "micro_full"} and guards.get("micro_policy_relaxed") is True:
        return "relaxed_micro_test"
    return "executable"


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _source_plan_hash(line: str, doc: dict[str, Any], plan: dict[str, Any]) -> str:
    return trade_plan_source_plan_hash(line, doc, plan)


def _strategy4_lineage(line: str, doc: dict[str, Any], plan: dict[str, Any], guards: dict[str, Any]) -> dict[str, Any]:
    if line != "strategy4":
        return {}
    candidates = [
        guards.get("strategy4_lineage") if isinstance(guards.get("strategy4_lineage"), dict) else None,
        (plan.get("input_refs") or {}).get("strategy4_lineage") if isinstance(plan.get("input_refs"), dict) else None,
        (doc.get("input_refs") or {}).get("strategy4_lineage") if isinstance(doc.get("input_refs"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate:
            return dict(candidate)
    return {}


def _paper_source_run_id(line: str, doc: dict[str, Any], lineage: dict[str, Any]) -> str | None:
    if line == "strategy4":
        return (
            lineage.get("origin_run_id")
            or lineage.get("source_run_id")
            or lineage.get("source_plan_run_id")
            or doc.get("run_id")
        )
    return doc.get("run_id")


def _paper_source_cycle_id(line: str, doc: dict[str, Any], lineage: dict[str, Any]) -> str | None:
    if line == "strategy4":
        return (
            lineage.get("origin_cycle_id")
            or lineage.get("source_cycle_id")
            or lineage.get("source_plan_cycle_id")
            or doc.get("cycle_id")
        )
    return doc.get("cycle_id")
