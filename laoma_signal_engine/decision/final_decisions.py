"""STEP5.0: direction + factor + planner + risk -> latest_decisions.json."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import orjson
from pydantic import ValidationError

from laoma_signal_engine.core.config_loader import EngineConfig
from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.time_utils import age_sec_from_iso_z, to_iso_z, utc_now
from laoma_signal_engine.decision.final_models import (
    FinalDecisionItem,
    FinalDecisionsDocument,
    FinalDecisionsMeta,
    RiskPlanBlock,
)
from laoma_signal_engine.decision.final_writer import atomic_write_latest_decisions
from laoma_signal_engine.decision.models import DirectionGateDocument
from laoma_signal_engine.decision.risk_gate import apply_risk_gate
from laoma_signal_engine.decision.sl_tp_planner import build_risk_plan, infer_base_asset
from laoma_signal_engine.decision.step5_config import Step5Bundle, load_step5_config
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanItem, TradePlanLineDocument
from laoma_signal_engine.decision.trade_plan_lines import TradePlanLineName, default_output_path
from laoma_signal_engine.factors.models import FactorSnapshotDocument, FactorSnapshotItem
from laoma_signal_engine.market.light_snapshot_models import FuturesLightSnapshotDocument

log = logging.getLogger(__name__)

P10_AGGREGATE_ORDER: tuple[TradePlanLineName, ...] = ("micro_fast", "without_micro", "micro_full")


def _factor_index(factor: FactorSnapshotDocument) -> dict[str, FactorSnapshotItem]:
    return {it.symbol: it for it in factor.items}


def _light_last_prices(path: Path) -> dict[str, float]:
    if not path.is_file():
        return {}
    try:
        doc = FuturesLightSnapshotDocument.model_validate(read_json_object(path))
    except (OSError, ValueError, ValidationError) as exc:
        log.warning("final_decisions: skip light snapshot %s (%s)", path, exc)
        return {}
    out: dict[str, float] = {}
    for it in doc.items:
        if it.last_price is not None:
            out[it.symbol] = float(it.last_price)
    return out


def build_final_decisions_document(
    direction: DirectionGateDocument,
    factor: FactorSnapshotDocument,
    *,
    last_prices: dict[str, float],
    generated_at: str,
    bundle: Step5Bundle,
) -> FinalDecisionsDocument:
    fac_ix = _factor_index(factor)
    decisions_out: list[FinalDecisionItem] = []
    rejected_out = []

    for d in direction.decisions:
        fac = fac_ix.get(d.symbol)
        lastp = last_prices.get(d.symbol)
        base = fac.base_asset if fac is not None else infer_base_asset(d.symbol)
        cashtag = f"${base}"
        rp = build_risk_plan(
            d,
            fac,
            lastp,
            bundle.planner,
            bundle.risk.max_sl_atr_multiple,
        )
        item = FinalDecisionItem(
            symbol=d.symbol,
            base_asset=base,
            cashtag=cashtag,
            decision_tf=d.decision_tf,
            decision=d.decision,
            direction=d.direction,
            action=d.action,
            entry_mode=d.entry_mode,
            confidence=d.confidence,
            risk_plan=rp,
            reason_codes=list(d.reason_codes),
            guards=dict(d.guards),
            input_refs=dict(d.input_refs),
            summary_for_orchestrator=d.summary_for_orchestrator,
            llm_hint=None,
        )
        rej = apply_risk_gate(item, fac, bundle.risk)
        if rej is not None:
            rejected_out.append(rej)
        else:
            decisions_out.append(item)

    meta = FinalDecisionsMeta(
        direction_generated_at=direction.generated_at,
        factor_snapshot_generated_at=factor.generated_at,
        planner_config_version=bundle.planner_config_version,
    )
    return FinalDecisionsDocument(
        schema_version="1.6",
        generated_at=generated_at,
        source="final_decision_planner",
        status=direction.status,
        count=len(decisions_out),
        decisions=decisions_out,
        rejected=rejected_out,
        meta=meta,
    )


def run_apply_final_decisions(
    *,
    direction_path: Path,
    factor_path: Path,
    light_path: Path | None,
    output_path: Path,
    generated_at: str,
    project_root: Path | None = None,
) -> FinalDecisionsDocument:
    direction = DirectionGateDocument.model_validate(read_json_object(direction_path))
    factor = FactorSnapshotDocument.model_validate(read_json_object(factor_path))
    lp: dict[str, float] = {}
    if light_path is not None:
        lp = _light_last_prices(light_path)
    bundle = load_step5_config(project_root)
    doc = build_final_decisions_document(
        direction,
        factor,
        last_prices=lp,
        generated_at=generated_at,
        bundle=bundle,
    )
    atomic_write_latest_decisions(output_path, doc)
    log.info(
        "final_decisions status=%s decisions=%s rejected=%s out=%s",
        doc.status,
        doc.count,
        len(doc.rejected),
        output_path,
    )
    return doc


def _trade_plan_to_final_item(plan: TradePlanItem, *, line: TradePlanLineName) -> FinalDecisionItem:
    base = infer_base_asset(plan.symbol)
    if plan.decision == "LONG":
        direction = "LONG"
        decision = "LONG_NOW" if plan.executable else "LONG_WAIT_PULLBACK"
        action = "ENTER" if plan.executable else "WAIT"
        entry_mode = "NOW" if plan.executable else "WAIT_PULLBACK"
    elif plan.decision == "SHORT":
        direction = "SHORT"
        decision = "SHORT_NOW" if plan.executable else "SHORT_WAIT_REBOUND"
        action = "ENTER" if plan.executable else "WAIT"
        entry_mode = "NOW" if plan.executable else "WAIT_REBOUND"
    else:
        direction = "HOLD"
        decision = "HOLD_NO_TRADE"
        action = "HOLD"
        entry_mode = "NONE"

    if plan.executable:
        risk_plan = RiskPlanBlock(
            plan_status="executable",
            entry_price_basis="last_price",
            entry_zone_low=plan.estimated_entry_price,
            entry_zone_high=plan.estimated_entry_price,
            stop_loss=plan.stop_loss,
            tp1=plan.take_profit,
            tp2=plan.take_profit,
            rr_to_tp1=plan.rr,
            rr_to_tp2=plan.rr,
            time_stop_minutes=None,
            invalid_condition="source_trade_plan_invalidated",
            estimated_entry_zone_low=plan.estimated_entry_price,
            estimated_entry_zone_high=plan.estimated_entry_price,
            trigger_condition="source_trade_plan_executable",
        )
    elif plan.action in ("ENTER_LIMIT", "WAIT"):
        risk_plan = RiskPlanBlock(
            plan_status="pending_trigger" if plan.decision != "NO_TRADE" else "no_trade",
            entry_price_basis="pullback_level" if plan.decision == "LONG" else "rebound_level",
            time_stop_minutes=None,
            invalid_condition="source_trade_plan_expired",
            trigger_condition=str(plan.guards.get("trigger_condition") or plan.entry_mode),
        )
    else:
        risk_plan = RiskPlanBlock(plan_status="no_trade")

    guards = dict(plan.guards)
    guards.update(
        {
            "source_trade_plan_line": line,
            "source_micro_mode": {"without_micro": "none", "micro_fast": "fast", "micro_full": "full"}[line],
            "source_opportunity_type": plan.guards.get("opportunity_type"),
            "source_net_rr": plan.guards.get("net_rr"),
            "source_micro_direction_confirmed": plan.guards.get("micro_direction_confirmed"),
        },
    )
    refs = dict(plan.input_refs)
    refs["source_trade_plan_line"] = line
    return FinalDecisionItem(
        symbol=plan.symbol,
        base_asset=base,
        cashtag=f"${base}",
        decision_tf=plan.decision_tf,
        decision=decision,  # type: ignore[arg-type]
        direction=direction,  # type: ignore[arg-type]
        action=action,  # type: ignore[arg-type]
        entry_mode=entry_mode,  # type: ignore[arg-type]
        confidence=plan.confidence,
        risk_plan=risk_plan,
        reason_codes=list(plan.reason_codes),
        guards=guards,
        input_refs=refs,
        summary_for_orchestrator="P10 aggregate trade plan.",
        llm_hint=None,
    )


def build_final_decisions_from_trade_plans(
    *,
    docs: dict[TradePlanLineName, TradePlanLineDocument],
    generated_at: str,
    sources: dict[TradePlanLineName, Path],
) -> FinalDecisionsDocument:
    if any(doc.status == "stale_input" for doc in docs.values()):
        return FinalDecisionsDocument(
            generated_at=generated_at,
            status="stale_input",
            count=0,
            decisions=[],
            rejected=[],
            meta=FinalDecisionsMeta(
                planner_config_version="5.2-p10-aggregate",
                trade_plan_sources={line: str(path) for line, path in sources.items()},
            ),
        )

    by_symbol: dict[str, dict[TradePlanLineName, TradePlanItem]] = {}
    for line, doc in docs.items():
        for plan in doc.plans:
            by_symbol.setdefault(plan.symbol, {})[line] = plan

    selected: list[FinalDecisionItem] = []
    for sym in sorted(by_symbol):
        plans = by_symbol[sym]
        chosen_line: TradePlanLineName | None = None
        chosen_plan: TradePlanItem | None = None
        for line in P10_AGGREGATE_ORDER:
            plan = plans.get(line)
            if plan is None:
                continue
            if plan.executable:
                chosen_line = line
                chosen_plan = plan
                break
        if chosen_plan is None:
            for line in P10_AGGREGATE_ORDER:
                plan = plans.get(line)
                if plan is not None and plan.decision != "NO_TRADE":
                    chosen_line = line
                    chosen_plan = plan
                    break
        if chosen_plan is None:
            chosen_line = next(iter(plans))
            chosen_plan = plans[chosen_line]
        selected.append(_trade_plan_to_final_item(chosen_plan, line=chosen_line))

    status = "no_candidates" if not selected else ("ok" if any(d.action == "ENTER" for d in selected) else "partial")
    return FinalDecisionsDocument(
        generated_at=generated_at,
        status=status,  # type: ignore[arg-type]
        count=len(selected),
        decisions=selected,
        rejected=[],
        meta=FinalDecisionsMeta(
            planner_config_version="5.2-p10-aggregate",
            trade_plan_sources={line: str(path) for line, path in sources.items()},
        ),
    )


def run_apply_final_decisions_from_trade_plans_safe(
    *,
    project_root: Path | None = None,
    output_path: Path | None = None,
    generated_at: str | None = None,
    stdout_json: bool = False,
) -> int:
    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    out = output_path.resolve() if output_path else (pr / "DATA/decisions/latest_decisions.json").resolve()
    gen_at = generated_at or to_iso_z(utc_now())
    try:
        paths = {line: default_output_path(pr, line) for line in P10_AGGREGATE_ORDER}
        docs = {
            line: TradePlanLineDocument.model_validate(read_json_object(path))
            for line, path in paths.items()
        }
        doc = build_final_decisions_from_trade_plans(docs=docs, generated_at=gen_at, sources=paths)
        atomic_write_latest_decisions(out, doc)
        if stdout_json:
            summary = {
                "schema_version": doc.schema_version,
                "source": doc.source,
                "status": doc.status,
                "count": doc.count,
                "rejected_count": len(doc.rejected),
                "planner_config_version": doc.meta.planner_config_version,
            }
            sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))
            sys.stdout.buffer.flush()
        return EXIT_SUCCESS
    except FileNotFoundError as exc:
        log.error("final_decisions p10 aggregate input missing: %s", exc)
        return EXIT_CONFIG
    except json.JSONDecodeError as exc:
        log.error("final_decisions p10 aggregate json: %s", exc)
        return EXIT_INTERNAL
    except ValidationError as exc:
        log.exception("final_decisions p10 aggregate validation: %s", exc)
        return EXIT_INTERNAL
    except OSError as exc:
        log.error("final_decisions p10 aggregate io error: %s", exc)
        return EXIT_CONFIG
    except Exception as exc:
        log.exception("final_decisions p10 aggregate failed: %s", exc)
        return EXIT_INTERNAL


def _stale_final_doc(
    *,
    generated_at: str,
    reason: str,
    direction_generated_at: str = "",
    factor_generated_at: str = "",
    planner_config_version: str = "5.0-mvp",
) -> FinalDecisionsDocument:
    _ = reason
    return FinalDecisionsDocument(
        schema_version="1.6",
        generated_at=generated_at,
        source="final_decision_planner",
        status="stale_input",
        count=0,
        decisions=[],
        rejected=[],
        meta=FinalDecisionsMeta(
            direction_generated_at=direction_generated_at,
            factor_snapshot_generated_at=factor_generated_at,
            planner_config_version=planner_config_version,
        ),
    )


def _age_violation(generated_at: str, *, max_age_sec: int) -> tuple[bool, int]:
    age = age_sec_from_iso_z(generated_at)
    return age > max_age_sec, age


def run_apply_final_decisions_safe(
    *,
    project_root: Path | None = None,
    direction_path: Path | None = None,
    factor_path: Path | None = None,
    light_path: Path | None = None,
    output_path: Path | None = None,
    generated_at: str | None = None,
    stdout_json: bool = False,
) -> int:
    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    dp = direction_path.resolve() if direction_path else (pr / "DATA/decisions/latest_direction_decisions.json").resolve()
    fp = factor_path.resolve() if factor_path else (pr / "DATA/factors/latest_factor_snapshot.json").resolve()
    lp_default = (pr / "DATA/market/futures_light_snapshot.json").resolve()
    lpath = light_path.resolve() if light_path else lp_default
    out = output_path.resolve() if output_path else (pr / "DATA/decisions/latest_decisions.json").resolve()
    gen_at = generated_at or to_iso_z(utc_now())
    try:
        engine_cfg = EngineConfig.load(pr)
        direction_probe = DirectionGateDocument.model_validate(read_json_object(dp))
        factor_probe = FactorSnapshotDocument.model_validate(read_json_object(fp))
        bundle_probe = load_step5_config(pr)

        checks: list[tuple[str, str, int]] = [
            (
                "direction_decision_stale",
                direction_probe.generated_at,
                engine_cfg.direction_decision_max_age_sec,
            ),
            (
                "factor_snapshot_stale",
                factor_probe.generated_at,
                engine_cfg.factor_snapshot_max_age_sec,
            ),
        ]
        if lpath.is_file():
            light_raw = read_json_object(lpath)
            if isinstance(light_raw, dict) and "generated_at" in light_raw:
                checks.append(
                    (
                        "light_snapshot_stale",
                        str(light_raw.get("generated_at", "")),
                        engine_cfg.final_light_snapshot_max_age_sec,
                    )
                )
        for reason, ts, max_age in checks:
            stale, age = _age_violation(ts, max_age_sec=max_age)
            if stale:
                doc = _stale_final_doc(
                    generated_at=gen_at,
                    reason=reason,
                    direction_generated_at=direction_probe.generated_at,
                    factor_generated_at=factor_probe.generated_at,
                    planner_config_version=bundle_probe.planner_config_version,
                )
                atomic_write_latest_decisions(out, doc)
                log.error(
                    "final_decisions stale input reason=%s age_sec=%s max_age_sec=%s out=%s",
                    reason,
                    age,
                    max_age,
                    out,
                )
                if stdout_json:
                    summary = {
                        "schema_version": doc.schema_version,
                        "source": doc.source,
                        "status": doc.status,
                        "count": doc.count,
                        "rejected_count": len(doc.rejected),
                        "reason": reason,
                        "input_age_sec": age,
                        "max_age_sec": max_age,
                    }
                    sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))
                    sys.stdout.buffer.flush()
                return EXIT_CONFIG
        doc = run_apply_final_decisions(
            direction_path=dp,
            factor_path=fp,
            light_path=lpath,
            output_path=out,
            generated_at=gen_at,
            project_root=pr,
        )
    except FileNotFoundError as exc:
        log.error("final_decisions input missing: %s", exc)
        return EXIT_CONFIG
    except json.JSONDecodeError as exc:
        log.error("final_decisions json: %s", exc)
        return EXIT_INTERNAL
    except ValidationError as exc:
        log.exception("final_decisions validation: %s", exc)
        return EXIT_INTERNAL
    except OSError as exc:
        log.error("final_decisions io error: %s", exc)
        return EXIT_CONFIG
    except Exception as exc:
        log.exception("final_decisions failed: %s", exc)
        return EXIT_INTERNAL

    if stdout_json:
        summary = {
            "schema_version": doc.schema_version,
            "source": doc.source,
            "status": doc.status,
            "count": doc.count,
            "rejected_count": len(doc.rejected),
        }
        sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))
        sys.stdout.buffer.flush()
    return EXIT_SUCCESS
