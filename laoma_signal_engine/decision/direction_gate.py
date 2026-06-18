"""Direction Gate: consume latest_factor_snapshot.json. docs/STEP4.0_Decision_Layer_Phase1_任务卡.md."""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import orjson
from pydantic import ValidationError

from laoma_signal_engine.core.config_loader import EngineConfig
from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.time_utils import age_sec_from_iso_z, to_iso_z, utc_now

from laoma_signal_engine.decision.models import (
    ActionKind,
    DecisionKind,
    DirectionDecisionItem,
    DirectionGateDocument,
    DirectionGateStatus,
    DirectionKind,
    EntryModeKind,
)
from laoma_signal_engine.decision.writer import atomic_write_direction_decisions
from laoma_signal_engine.factors.models import FactorSnapshotDocument, FactorSnapshotItem
from laoma_signal_engine.factors.reason_order import sort_reason_codes
from laoma_signal_engine.micro.assembly.models import Micro15mBlock, MicroSignalBlock
from laoma_signal_engine.micro.data_quality_contract import build_micro_data_quality_contract

log = logging.getLogger(__name__)

BULL_STRUCT = frozenset({"up_impulse", "breakout"})
BEAR_STRUCT = frozenset({"down_impulse", "breakdown"})
BLOCK_NOW_REASONS = frozenset(
    {
        "micro_missing",
        "micro_input_not_fresh",
        "micro_input_invalid",
        "micro_features_stale",
        "micro_target_anchor_stale",
    }
)
NEUTRAL_EPS = 0.05


@dataclass(frozen=True)
class DirectionGateConfig:
    allow_watch_now: bool = False
    require_context_guards_for_now: bool = True


def _price_ret(primary: dict[str, Any]) -> float | None:
    pr = primary.get("price_ret")
    if isinstance(pr, (int, float)):
        return float(pr)
    return None


def primary_bullish(primary: dict[str, Any]) -> bool:
    pr = _price_ret(primary)
    if pr is not None and pr > NEUTRAL_EPS:
        return True
    st = str(primary.get("structure_state", ""))
    return st in BULL_STRUCT


def primary_bearish(primary: dict[str, Any]) -> bool:
    pr = _price_ret(primary)
    if pr is not None and pr < -NEUTRAL_EPS:
        return True
    st = str(primary.get("structure_state", ""))
    return st in BEAR_STRUCT


def primary_ready(primary: dict[str, Any]) -> bool:
    return primary.get("ready") is True


def context_guard_codes(item: FactorSnapshotItem) -> list[str]:
    codes: list[str] = []
    if not item.oi_15m.ready:
        codes.append("oi_not_ready")
    if not item.funding_context.ready:
        codes.append("funding_not_ready")
    if not item.basis_15m.ready:
        codes.append("basis_not_ready")
    return codes


def context_all_ready(item: FactorSnapshotItem) -> bool:
    return item.oi_15m.ready and item.funding_context.ready and item.basis_15m.ready


def context_risk_reasons_for_long(item: FactorSnapshotItem) -> list[str]:
    codes: list[str] = []
    fc = item.funding_context
    if fc.ready and fc.funding_bucket == "OVERHEATED":
        codes.append("funding_overheated")
    bc = item.basis_15m
    if (
        bc.ready
        and bc.basis_extreme
        and bc.mark_index_basis_bps is not None
        and bc.mark_index_basis_bps > 0
    ):
        codes.append("basis_overheated")
    if item.oi_15m.ready and item.oi_15m.oi_conflict:
        codes.append("oi_conflict")
    return codes


def context_risk_reasons_for_short(item: FactorSnapshotItem) -> list[str]:
    codes: list[str] = []
    fc = item.funding_context
    if fc.ready and fc.funding_bucket == "NEGATIVE_EXTREME":
        codes.append("funding_extreme_negative")
    bc = item.basis_15m
    if (
        bc.ready
        and bc.basis_extreme
        and bc.mark_index_basis_bps is not None
        and bc.mark_index_basis_bps < 0
    ):
        codes.append("basis_overheated_short")
    if item.oi_15m.ready and item.oi_15m.oi_conflict:
        codes.append("oi_conflict")
    return codes


def micro_numeric_long_ok(m: Micro15mBlock) -> tuple[bool, bool]:
    """Returns (ok, conflict); conflict True means unusable micro numerics for NOW."""
    usable_cvd = m.z_cvd is not None
    usable_ofi = m.z_ofi is not None
    if not usable_cvd or not usable_ofi:
        return False, True
    cvd_ok = m.z_cvd >= 0
    ofi_ok = m.z_ofi >= 0
    return cvd_ok and ofi_ok, False


def micro_numeric_short_ok(m: Micro15mBlock) -> tuple[bool, bool]:
    usable_cvd = m.z_cvd is not None
    usable_ofi = m.z_ofi is not None
    if not usable_cvd or not usable_ofi:
        return False, True
    cvd_ok = m.z_cvd <= 0
    ofi_ok = m.z_ofi <= 0
    return cvd_ok and ofi_ok, False


def _signal_contract_for_item(item: FactorSnapshotItem) -> MicroSignalBlock | None:
    if item.micro_fast_signal is not None and item.micro_fast_signal.micro_signal_usable:
        return item.micro_fast_signal
    if item.micro_full_signal is not None and item.micro_full_signal.micro_signal_usable:
        return item.micro_full_signal
    return item.micro_fast_signal or item.micro_full_signal


def _micro_signal_now_ok(item: FactorSnapshotItem) -> tuple[bool, bool, dict[str, Any]]:
    signal = _signal_contract_for_item(item)
    guards = {
        "micro_signal_missing": signal is None,
        "micro_signal_usable": signal.micro_signal_usable if signal is not None else False,
        "micro_direction_confirmed": signal.micro_direction_confirmed if signal is not None else False,
        "micro_exec_allowed": signal.micro_exec_allowed if signal is not None else False,
        "micro_alignment_state": signal.micro_alignment_state if signal is not None else "insufficient",
        "micro_strength": signal.micro_strength if signal is not None else "none",
    }
    if signal is None:
        return False, True, guards
    conflict = signal.micro_alignment_state in {
        "mixed",
        "conflict",
        "bullish_divergence",
        "bearish_divergence",
        "buy_absorption",
        "sell_absorption",
        "exhaustion",
        "data_quality_blocked",
        "insufficient",
    }
    return signal.micro_direction_confirmed and signal.micro_exec_allowed, conflict, guards


def tier_allows_now(source_state: str, cfg: DirectionGateConfig) -> bool:
    if source_state == "strong_candidate":
        return True
    return source_state == "watch_candidate" and cfg.allow_watch_now


def factor_blocks_now(item: FactorSnapshotItem) -> bool:
    return bool(set(item.factor_quality.reason_codes) & BLOCK_NOW_REASONS)


def _micro_data_quality_guards(item: FactorSnapshotItem) -> dict[str, Any]:
    contract = build_micro_data_quality_contract(
        line="direction_gate",
        quality=item.micro_quality,
        micro_15m=item.micro_15m,
        signal=_signal_contract_for_item(item),
    )
    return {
        key: contract[key]
        for key in (
            "micro_data_quality_state",
            "micro_data_quality_class",
            "micro_data_quality_reasons",
            "micro_data_quality_attributions",
            "micro_data_quality_evidence",
        )
    }


def _micro_data_quality_reason_codes(guards: dict[str, Any]) -> list[str]:
    state = str(guards.get("micro_data_quality_state") or "ok")
    if state == "technical_blocked":
        return ["data_quality_blocked", "technical_not_ready"]
    if state == "config_warmup_incomplete":
        return ["micro_warmup_incomplete"]
    if state == "unknown":
        return ["micro_data_quality_unknown"]
    return []


def _map_decision_to_action_entry(d: DecisionKind) -> tuple[ActionKind, EntryModeKind, DirectionKind]:
    if d == "LONG_NOW":
        return "ENTER", "NOW", "LONG"
    if d == "LONG_WAIT_PULLBACK":
        return "WAIT", "WAIT_PULLBACK", "LONG"
    if d == "SHORT_NOW":
        return "ENTER", "NOW", "SHORT"
    if d == "SHORT_WAIT_REBOUND":
        return "WAIT", "WAIT_REBOUND", "SHORT"
    if d == "HOLD_WATCH":
        return "HOLD", "WATCH", "HOLD"
    if d == "HOLD_NO_TRADE":
        return "HOLD", "NONE", "HOLD"
    return "REJECT", "NONE", "NONE"


def decide_item(
    item: FactorSnapshotItem,
    *,
    factor_doc: FactorSnapshotDocument,
    cfg: DirectionGateConfig,
) -> DirectionDecisionItem:
    primary = item.primary_15m
    bull = primary_bullish(primary)
    bear = primary_bearish(primary)
    pr_ok = primary_ready(primary)
    mq_ready = item.micro_quality.ready
    ms = item.move_side.strip().lower()
    src = item.source_state.strip().lower()
    dq_guards = _micro_data_quality_guards(item)
    dq_reason_codes = _micro_data_quality_reason_codes(dq_guards)

    reasons: list[str] = []
    reasons.extend(item.factor_quality.reason_codes)
    reasons.extend(dq_reason_codes)

    if not pr_ok:
        reasons.append("primary_15m_not_ready")

    ctx_codes = context_guard_codes(item)
    ctx_ready = context_all_ready(item)
    reasons.extend(ctx_codes)

    kline_st = str(primary.get("kline_cvd_state", ""))
    if not mq_ready and kline_st not in ("", "neutral", "unavailable"):
        reasons.append("kline_cvd_proxy_only")

    if ms == "neutral" or (ms not in ("up", "down")):
        reasons.append("reject_no_direction")
        reasons = sort_reason_codes(reasons)
        action, entry, direction = _map_decision_to_action_entry("REJECT")
        return DirectionDecisionItem(
            symbol=item.symbol,
            decision_tf=item.decision_tf,
            decision="REJECT",
            direction=direction,
            action=action,
            entry_mode=entry,
            confidence=min(100, max(0, item.scan_score)),
            reason_codes=reasons,
            guards={
                "micro_quality_ready": mq_ready,
                **dq_guards,
                "primary_ready": pr_ok,
                "primary_bullish": bull,
                "primary_bearish": bear,
                "context_all_ready": ctx_ready,
                "require_context_guards_for_now": cfg.require_context_guards_for_now,
                "allow_watch_now": cfg.allow_watch_now,
            },
            input_refs={
                "factor_snapshot_generated_at": factor_doc.generated_at,
                "factor_snapshot_status": factor_doc.status,
            },
            summary_for_orchestrator="Neutral or unknown move_side; no trade direction.",
        )

    if bull and ms == "down":
        reasons.append("reject_no_direction")
        reasons = sort_reason_codes(reasons)
        action, entry, direction = _map_decision_to_action_entry("REJECT")
        return DirectionDecisionItem(
            symbol=item.symbol,
            decision_tf=item.decision_tf,
            decision="REJECT",
            direction=direction,
            action=action,
            entry_mode=entry,
            confidence=min(100, max(0, item.scan_score)),
            reason_codes=reasons,
            guards={
                "micro_quality_ready": mq_ready,
                **dq_guards,
                "primary_ready": pr_ok,
                "primary_bullish": bull,
                "primary_bearish": bear,
                "context_all_ready": ctx_ready,
                "require_context_guards_for_now": cfg.require_context_guards_for_now,
                "allow_watch_now": cfg.allow_watch_now,
            },
            input_refs={
                "factor_snapshot_generated_at": factor_doc.generated_at,
                "factor_snapshot_status": factor_doc.status,
            },
            summary_for_orchestrator="Primary bias conflicts with scanner move_side.",
        )

    if bear and ms == "up":
        reasons.append("reject_no_direction")
        reasons = sort_reason_codes(reasons)
        action, entry, direction = _map_decision_to_action_entry("REJECT")
        return DirectionDecisionItem(
            symbol=item.symbol,
            decision_tf=item.decision_tf,
            decision="REJECT",
            direction=direction,
            action=action,
            entry_mode=entry,
            confidence=min(100, max(0, item.scan_score)),
            reason_codes=reasons,
            guards={
                "micro_quality_ready": mq_ready,
                **dq_guards,
                "primary_ready": pr_ok,
                "primary_bullish": bull,
                "primary_bearish": bear,
                "context_all_ready": ctx_ready,
                "require_context_guards_for_now": cfg.require_context_guards_for_now,
                "allow_watch_now": cfg.allow_watch_now,
            },
            input_refs={
                "factor_snapshot_generated_at": factor_doc.generated_at,
                "factor_snapshot_status": factor_doc.status,
            },
            summary_for_orchestrator="Primary bias conflicts with scanner move_side.",
        )

    guards_dict: dict[str, Any] = {
        "micro_quality_ready": mq_ready,
        **dq_guards,
        "primary_ready": pr_ok,
        "primary_bullish": bull,
        "primary_bearish": bear,
        "context_all_ready": ctx_ready,
        "require_context_guards_for_now": cfg.require_context_guards_for_now,
        "allow_watch_now": cfg.allow_watch_now,
    }
    refs_dict: dict[str, Any] = {
        "factor_snapshot_generated_at": factor_doc.generated_at,
        "factor_snapshot_status": factor_doc.status,
    }

    decision_kind: DecisionKind = "HOLD_NO_TRADE"
    extra_reasons: list[str] = []

    if ms == "up":
        long_num_ok, long_cnf, micro_signal_guards = _micro_signal_now_ok(item)
        guards_dict.update(micro_signal_guards)
        if not bull:
            extra_reasons.append("primary_15m_not_ready")
        rp = primary.get("range_pos")
        range_high = isinstance(rp, (int, float)) and float(rp) >= 0.72

        risk_side = context_risk_reasons_for_long(item)
        reasons.extend(risk_side)

        now_blocked = (
            factor_blocks_now(item)
            or not mq_ready
            or bool(dq_reason_codes)
            or not tier_allows_now(src, cfg)
            or (cfg.require_context_guards_for_now and not ctx_ready)
            or long_cnf
            or not long_num_ok
            or bool(risk_side)
        )
        can_now = not now_blocked and bull and pr_ok

        if can_now and not range_high:
            decision_kind = "LONG_NOW"
            extra_reasons.append("long_now_confirmed")
        elif can_now and range_high:
            decision_kind = "LONG_WAIT_PULLBACK"
            extra_reasons.append("range_too_high_wait_pullback")
            extra_reasons.append("wait_pullback")
        elif src == "watch_candidate" and not bull and not bear:
            decision_kind = "HOLD_WATCH"
            extra_reasons.append("hold_watch")
        else:
            decision_kind = "LONG_WAIT_PULLBACK"
            extra_reasons.append("wait_pullback")
            if not mq_ready:
                extra_reasons.append("micro_15m_not_ready")
            elif long_cnf or not long_num_ok:
                extra_reasons.append("micro_conflict")
            if range_high:
                extra_reasons.append("range_too_high_wait_pullback")

    elif ms == "down":
        short_num_ok, short_cnf, micro_signal_guards = _micro_signal_now_ok(item)
        guards_dict.update(micro_signal_guards)
        if not bear:
            extra_reasons.append("primary_15m_not_ready")
        rp = primary.get("range_pos")
        range_low = isinstance(rp, (int, float)) and float(rp) <= 0.28

        risk_side = context_risk_reasons_for_short(item)
        reasons.extend(risk_side)

        now_blocked = (
            factor_blocks_now(item)
            or not mq_ready
            or bool(dq_reason_codes)
            or not tier_allows_now(src, cfg)
            or (cfg.require_context_guards_for_now and not ctx_ready)
            or short_cnf
            or not short_num_ok
            or bool(risk_side)
        )
        can_now = not now_blocked and bear and pr_ok

        if can_now and not range_low:
            decision_kind = "SHORT_NOW"
            extra_reasons.append("short_now_confirmed")
        elif can_now and range_low:
            decision_kind = "SHORT_WAIT_REBOUND"
            extra_reasons.append("range_too_low_wait_rebound")
            extra_reasons.append("wait_rebound")
        elif src == "watch_candidate" and not bull and not bear:
            decision_kind = "HOLD_WATCH"
            extra_reasons.append("hold_watch")
        else:
            decision_kind = "SHORT_WAIT_REBOUND"
            extra_reasons.append("wait_rebound")
            if not mq_ready:
                extra_reasons.append("micro_15m_not_ready")
            elif short_cnf or not short_num_ok:
                extra_reasons.append("micro_conflict")
            if range_low:
                extra_reasons.append("range_too_low_wait_rebound")

    reasons.extend(extra_reasons)
    reasons = sort_reason_codes(reasons)
    action, entry, direction = _map_decision_to_action_entry(decision_kind)

    summaries: dict[DecisionKind, str] = {
        "LONG_NOW": "Long NOW: primary and micro aligned; context allows.",
        "LONG_WAIT_PULLBACK": "Long bias but NOW blocked; wait pullback.",
        "SHORT_NOW": "Short NOW: primary and micro aligned; context allows.",
        "SHORT_WAIT_REBOUND": "Short bias but NOW blocked; wait rebound.",
        "HOLD_WATCH": "Watch-tier; observe or staged wait.",
        "HOLD_NO_TRADE": "No trade; hold flat.",
        "REJECT": "Reject; no workable direction.",
    }
    summary = summaries.get(decision_kind, "Direction gate outcome.")

    return DirectionDecisionItem(
        symbol=item.symbol,
        decision_tf=item.decision_tf,
        decision=decision_kind,
        direction=direction,
        action=action,
        entry_mode=entry,
        confidence=min(100, max(0, item.scan_score)),
        reason_codes=reasons,
        guards=guards_dict,
        input_refs=refs_dict,
        summary_for_orchestrator=summary,
    )


def _gate_doc_status(factor: FactorSnapshotDocument) -> DirectionGateStatus:
    if factor.status == "error":
        return "error"
    if factor.count == 0:
        return "no_candidates"
    if factor.status == "partial":
        return "partial"
    return "ok"


def build_direction_gate_document(
    factor: FactorSnapshotDocument,
    *,
    generated_at: str,
    cfg: DirectionGateConfig | None = None,
) -> DirectionGateDocument:
    c = cfg or DirectionGateConfig()
    st = _gate_doc_status(factor)
    if st in ("error", "no_candidates"):
        return DirectionGateDocument(
            schema_version="1.6",
            generated_at=generated_at,
            source="direction_gate",
            status=st,
            count=0,
            decisions=[],
        )
    decisions = [decide_item(it, factor_doc=factor, cfg=c) for it in factor.items]
    return DirectionGateDocument(
        schema_version="1.6",
        generated_at=generated_at,
        source="direction_gate",
        status=st,
        count=len(decisions),
        decisions=decisions,
    )


def run_apply_direction_gate(
    *,
    factor_path: Path,
    output_path: Path,
    generated_at: str,
    cfg: DirectionGateConfig | None = None,
) -> DirectionGateDocument:
    factor = FactorSnapshotDocument.model_validate(read_json_object(factor_path))
    doc = build_direction_gate_document(factor, generated_at=generated_at, cfg=cfg)
    atomic_write_direction_decisions(output_path, doc)
    log.info("direction_gate status=%s count=%s out=%s", doc.status, doc.count, output_path)
    return doc


def _stale_direction_doc(
    *,
    generated_at: str,
    reason: str,
    input_age_sec: int,
    max_age_sec: int,
) -> DirectionGateDocument:
    _ = (reason, input_age_sec, max_age_sec)
    return DirectionGateDocument(
        schema_version="1.6",
        generated_at=generated_at,
        source="direction_gate",
        status="stale_input",
        count=0,
        decisions=[],
    )


def _factor_is_stale(factor: FactorSnapshotDocument, *, max_age_sec: int) -> tuple[bool, int]:
    age = age_sec_from_iso_z(factor.generated_at)
    return age > max_age_sec, age


def run_apply_direction_gate_safe(
    *,
    project_root: Path | None = None,
    factor_path: Path | None = None,
    output_path: Path | None = None,
    generated_at: str | None = None,
    stdout_json: bool = False,
    allow_watch_now: bool = False,
    require_context_guards_for_now: bool = True,
) -> int:
    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    fp = factor_path.resolve() if factor_path else (pr / "DATA/factors/latest_factor_snapshot.json").resolve()
    out = output_path.resolve() if output_path else (pr / "DATA/decisions/latest_direction_decisions.json").resolve()
    gen_at = generated_at or to_iso_z(utc_now())
    engine_cfg = EngineConfig.load(pr)
    cfg = DirectionGateConfig(
        allow_watch_now=allow_watch_now,
        require_context_guards_for_now=require_context_guards_for_now,
    )
    try:
        factor_obj = FactorSnapshotDocument.model_validate(read_json_object(fp))
        stale, age = _factor_is_stale(factor_obj, max_age_sec=engine_cfg.factor_snapshot_max_age_sec)
        if stale:
            doc = _stale_direction_doc(
                generated_at=gen_at,
                reason="factor_snapshot_stale",
                input_age_sec=age,
                max_age_sec=engine_cfg.factor_snapshot_max_age_sec,
            )
            atomic_write_direction_decisions(out, doc)
            log.error(
                "direction_gate stale input factor_age_sec=%s max_age_sec=%s factor=%s",
                age,
                engine_cfg.factor_snapshot_max_age_sec,
                fp,
            )
            if stdout_json:
                summary = {
                    "schema_version": doc.schema_version,
                    "source": doc.source,
                    "status": doc.status,
                    "count": doc.count,
                    "reason": "factor_snapshot_stale",
                    "input_age_sec": age,
                    "max_age_sec": engine_cfg.factor_snapshot_max_age_sec,
                }
                sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))
                sys.stdout.buffer.flush()
            return EXIT_CONFIG
        doc = run_apply_direction_gate(factor_path=fp, output_path=out, generated_at=gen_at, cfg=cfg)
    except FileNotFoundError as exc:
        log.error("direction_gate input missing: %s", exc)
        return EXIT_CONFIG
    except json.JSONDecodeError as exc:
        log.error("direction_gate json: %s", exc)
        return EXIT_INTERNAL
    except ValidationError as exc:
        log.exception("direction_gate validation: %s", exc)
        return EXIT_INTERNAL
    except OSError as exc:
        log.error("direction_gate io error: %s", exc)
        return EXIT_CONFIG
    except Exception as exc:
        log.exception("direction_gate failed: %s", exc)
        return EXIT_INTERNAL

    if stdout_json:
        summary = {
            "schema_version": doc.schema_version,
            "source": doc.source,
            "status": doc.status,
            "count": doc.count,
        }
        sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))
        sys.stdout.buffer.flush()
    return EXIT_SUCCESS
