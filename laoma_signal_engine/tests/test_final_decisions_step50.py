"""STEP5.0 final decisions: models, planner mapping, risk gate, roundtrip."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from laoma_signal_engine.decision.final_decisions import build_final_decisions_document
from laoma_signal_engine.decision.final_decisions import build_final_decisions_from_trade_plans
from laoma_signal_engine.decision.final_decisions import run_apply_final_decisions_safe
from laoma_signal_engine.decision.final_decisions import run_apply_final_decisions_from_trade_plans_safe
from laoma_signal_engine.decision.final_models import FinalDecisionItem, RiskPlanBlock
from laoma_signal_engine.decision.final_models import FinalDecisionsDocument
from laoma_signal_engine.decision.final_writer import atomic_write_latest_decisions
from laoma_signal_engine.decision.models import DirectionDecisionItem, DirectionGateDocument
from laoma_signal_engine.decision.risk_gate import apply_risk_gate
from laoma_signal_engine.decision.sl_tp_planner import build_risk_plan, map_decision_to_plan_status
from laoma_signal_engine.decision.step5_config import RiskGateConfig, SlTpPlannerConfig, Step5Bundle
from laoma_signal_engine.core.exit_codes import EXIT_CONFIG
from laoma_signal_engine.factors.models import (
    FactorQualityBlock,
    FactorSnapshotDocument,
    FactorSnapshotItem,
)
from laoma_signal_engine.micro.assembly.models import Micro15mBlock, MicroQualityBlock
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanItem, TradePlanLineDocument


def _micro_blocks() -> tuple[Micro15mBlock, MicroQualityBlock]:
    m15 = Micro15mBlock(
        ready=False,
        cvd=None,
        z_cvd=None,
        cvd_state="unknown",
        ofi=None,
        z_ofi=None,
        ofi_state="unknown",
        ofi_pressure="unknown",
        fusion_score=None,
        fusion_consistency=None,
        fusion_signal=None,
        fusion_ready=False,
        micro_state="not_ready",
    )
    mq = MicroQualityBlock(
        ready=False,
        reason_codes=[],
        reference_ts_sec=0,
        collect_started_ts_sec=0,
        warmup_age_sec=0,
        cvd_update_age_sec=None,
        ofi_update_age_sec=None,
        last_update_age_sec=None,
        max_lag_sec=None,
        coverage={},
        driver_metrics_summary={},
    )
    return m15, mq


def _factor_item(
    symbol: str,
    *,
    atr: float,
    swing_low: float,
    swing_high: float,
    breakout: float,
    breakdown: float,
) -> FactorSnapshotItem:
    m15, mq = _micro_blocks()
    return FactorSnapshotItem(
        symbol=symbol,
        base_asset="TST",
        source_state="strong_candidate",
        move_side="up",
        scan_score=50,
        trigger_type="test",
        primary_15m={
            "atr": atr,
            "recent_swing_low": swing_low,
            "recent_swing_high": swing_high,
            "breakout_level": breakout,
            "breakdown_level": breakdown,
            "ready": True,
        },
        trigger_5m={},
        entry_1m={
            "atr": atr * 0.5,
            "last_pullback_low": swing_low + 0.01,
            "last_breakout_high": breakout,
            "last_rebound_high": None,
            "last_breakdown_low": None,
        },
        background={},
        micro_15m=m15,
        micro_quality=mq,
        factor_quality=FactorQualityBlock(ready=True, reason_codes=[], input_warnings=[]),
    )


def _dir_item(symbol: str, decision: str) -> DirectionDecisionItem:
    if decision == "LONG_NOW":
        d, di, a, e = "LONG_NOW", "LONG", "ENTER", "NOW"
    elif decision == "SHORT_NOW":
        d, di, a, e = "SHORT_NOW", "SHORT", "ENTER", "NOW"
    elif decision == "LONG_WAIT_PULLBACK":
        d, di, a, e = "LONG_WAIT_PULLBACK", "LONG", "WAIT", "WAIT_PULLBACK"
    elif decision == "HOLD_WATCH":
        d, di, a, e = "HOLD_WATCH", "HOLD", "HOLD", "WATCH"
    else:
        d, di, a, e = "HOLD_NO_TRADE", "HOLD", "HOLD", "NONE"
    return DirectionDecisionItem(
        symbol=symbol,
        decision_tf="15m",
        decision=d,
        direction=di,
        action=a,
        entry_mode=e,
        confidence=50,
        reason_codes=[],
        guards={},
        input_refs={},
        summary_for_orchestrator="test",
    )


def test_map_decision_to_plan_status() -> None:
    assert map_decision_to_plan_status("LONG_NOW") == "executable"
    assert map_decision_to_plan_status("LONG_WAIT_PULLBACK") == "pending_trigger"
    assert map_decision_to_plan_status("HOLD_WATCH") == "observe_only"
    assert map_decision_to_plan_status("HOLD_NO_TRADE") == "no_trade"


def test_wait_decision_rejects_enter_executable() -> None:
    d = _dir_item("AAAUSDT", "LONG_WAIT_PULLBACK")
    fac = _factor_item("AAAUSDT", atr=1.0, swing_low=90.0, swing_high=110.0, breakout=110.0, breakdown=90.0)
    rp = build_risk_plan(d, fac, 100.0, SlTpPlannerConfig(), max_sl_atr_multiple=4.0)
    assert rp.plan_status == "pending_trigger"
    item = FinalDecisionItem(
        symbol=d.symbol,
        base_asset="AAA",
        cashtag="$AAA",
        decision_tf=d.decision_tf,
        decision=d.decision,
        direction=d.direction,
        action=d.action,
        entry_mode=d.entry_mode,
        confidence=d.confidence,
        risk_plan=rp,
        reason_codes=[],
        guards={},
        input_refs={},
        summary_for_orchestrator="x",
    )
    bad = item.model_copy(
        update={
            "action": "ENTER",
            "risk_plan": RiskPlanBlock(plan_status="executable", entry_price_basis="last_price"),
        },
    )
    with pytest.raises(ValidationError):
        _ = FinalDecisionItem.model_validate(bad.model_dump(mode="json"))


def test_risk_gate_accepts_long_now() -> None:
    d = _dir_item("AAAUSDT", "LONG_NOW")
    fac = _factor_item("AAAUSDT", atr=1.0, swing_low=98.0, swing_high=102.0, breakout=102.0, breakdown=98.0)
    rp = build_risk_plan(d, fac, 100.0, SlTpPlannerConfig(), max_sl_atr_multiple=4.0)
    item = FinalDecisionItem(
        symbol=d.symbol,
        base_asset="AAA",
        cashtag="$AAA",
        decision_tf=d.decision_tf,
        decision=d.decision,
        direction=d.direction,
        action=d.action,
        entry_mode=d.entry_mode,
        confidence=d.confidence,
        risk_plan=rp,
        reason_codes=[],
        guards={},
        input_refs={},
        summary_for_orchestrator="x",
    )
    assert apply_risk_gate(item, fac, RiskGateConfig()) is None


def test_risk_gate_rejects_bad_rr() -> None:
    d = _dir_item("AAAUSDT", "LONG_NOW")
    fac = _factor_item("AAAUSDT", atr=1.0, swing_low=98.0, swing_high=102.0, breakout=102.0, breakdown=98.0)
    rp = build_risk_plan(d, fac, 100.0, SlTpPlannerConfig(), max_sl_atr_multiple=4.0)
    rp2 = rp.model_copy(update={"rr_to_tp1": 0.5})
    item = FinalDecisionItem(
        symbol=d.symbol,
        base_asset="AAA",
        cashtag="$AAA",
        decision_tf=d.decision_tf,
        decision=d.decision,
        direction=d.direction,
        action=d.action,
        entry_mode=d.entry_mode,
        confidence=d.confidence,
        risk_plan=rp2,
        reason_codes=[],
        guards={},
        input_refs={},
        summary_for_orchestrator="x",
    )
    rej = apply_risk_gate(item, fac, RiskGateConfig(min_rr_to_tp1=1.0))
    assert rej is not None
    assert "risk_rr_below_min" in rej.reject_reason_codes


def test_build_document_count_excludes_rejected(tmp_path: Path) -> None:
    fac = _factor_item("AAAUSDT", atr=1.0, swing_low=98.0, swing_high=102.0, breakout=102.0, breakdown=98.0)
    factor_doc = FactorSnapshotDocument(
        generated_at="t1",
        status="ok",
        count=1,
        items=[fac],
    )
    direction = DirectionGateDocument(
        generated_at="t0",
        status="ok",
        count=2,
        decisions=[
            _dir_item("AAAUSDT", "LONG_NOW"),
            _dir_item("AAAUSDT", "LONG_NOW"),
        ],
    )
    bundle = Step5Bundle(
        planner=SlTpPlannerConfig(),
        risk=RiskGateConfig(min_rr_to_tp1=99.0),
        planner_config_version="test",
    )
    doc = build_final_decisions_document(
        direction,
        factor_doc,
        last_prices={"AAAUSDT": 100.0},
        generated_at="t2",
        bundle=bundle,
    )
    assert doc.count == len(doc.decisions)
    assert doc.count == 0
    assert len(doc.rejected) == 2


def test_atomic_write_roundtrip(tmp_path: Path) -> None:
    from laoma_signal_engine.decision.final_models import FinalDecisionsDocument, FinalDecisionsMeta

    d = _dir_item("AAAUSDT", "HOLD_WATCH")
    fac = _factor_item("AAAUSDT", atr=1.0, swing_low=98.0, swing_high=102.0, breakout=102.0, breakdown=98.0)
    rp = build_risk_plan(d, fac, 100.0, SlTpPlannerConfig(), max_sl_atr_multiple=4.0)
    item = FinalDecisionItem(
        symbol=d.symbol,
        base_asset="AAA",
        cashtag="$AAA",
        decision_tf=d.decision_tf,
        decision=d.decision,
        direction=d.direction,
        action=d.action,
        entry_mode=d.entry_mode,
        confidence=d.confidence,
        risk_plan=rp,
        reason_codes=[],
        guards={},
        input_refs={},
        summary_for_orchestrator="x",
    )
    doc = FinalDecisionsDocument(
        generated_at="t",
        status="ok",
        count=1,
        decisions=[item],
        rejected=[],
        meta=FinalDecisionsMeta(),
    )
    p = tmp_path / "latest_decisions.json"
    atomic_write_latest_decisions(p, doc)
    raw = json.loads(p.read_text(encoding="utf-8"))
    doc2 = FinalDecisionsDocument.model_validate(raw)
    assert doc2.count == 1
    assert doc2.decisions[0].risk_plan.plan_status == "observe_only"


def test_apply_final_decisions_safe_rejects_stale_direction(tmp_path: Path) -> None:
    fac = _factor_item("AAAUSDT", atr=1.0, swing_low=98.0, swing_high=102.0, breakout=102.0, breakdown=98.0)
    factor_doc = FactorSnapshotDocument(generated_at="2026-01-01T00:00:00Z", status="ok", count=1, items=[fac])
    direction_doc = DirectionGateDocument(
        generated_at="2026-01-01T00:00:00Z",
        status="ok",
        count=1,
        decisions=[_dir_item("AAAUSDT", "LONG_NOW")],
    )
    direction_path = tmp_path / "DATA" / "decisions" / "latest_direction_decisions.json"
    factor_path = tmp_path / "DATA" / "factors" / "latest_factor_snapshot.json"
    out = tmp_path / "DATA" / "decisions" / "latest_decisions.json"
    direction_path.parent.mkdir(parents=True)
    factor_path.parent.mkdir(parents=True)
    direction_path.write_text(json.dumps(direction_doc.model_dump(mode="json")), encoding="utf-8")
    factor_path.write_text(json.dumps(factor_doc.model_dump(mode="json")), encoding="utf-8")

    rc = run_apply_final_decisions_safe(
        project_root=tmp_path,
        direction_path=direction_path,
        factor_path=factor_path,
        light_path=tmp_path / "missing_light.json",
        output_path=out,
        stdout_json=False,
    )
    assert rc == EXIT_CONFIG
    parsed = FinalDecisionsDocument.model_validate(json.loads(out.read_text(encoding="utf-8")))
    assert parsed.status == "stale_input"
    assert parsed.decisions == []


def _trade_doc(line: str, plan: TradePlanItem, *, status: str = "ok") -> TradePlanLineDocument:
    source = {
        "without_micro": "trade_plan_without_micro",
        "micro_fast": "trade_plan_micro_fast",
        "micro_full": "trade_plan_micro_full",
    }[line]
    mode = {"without_micro": "none", "micro_fast": "fast", "micro_full": "full"}[line]
    return TradePlanLineDocument.model_validate(
        {
            "schema_version": "1.0",
            "generated_at": "2099-01-01T00:00:00Z",
            "source": source,
            "micro_mode": mode,
            "status": status,
            "count": 1,
            "executable_count": 1 if plan.executable else 0,
            "input_refs": {},
            "plans": [plan.model_dump(mode="json")],
        },
    )


def _trade_plan(symbol: str, *, executable: bool, line: str) -> TradePlanItem:
    return TradePlanItem.model_validate(
        {
            "symbol": symbol,
            "decision_tf": "15m",
            "decision": "LONG",
            "action": "ENTER_MARKET" if executable else "WAIT",
            "entry_mode": "MARKET" if executable else "WAIT_PULLBACK",
            "estimated_entry_price": 100.0 if executable else None,
            "stop_loss": 99.0 if executable else None,
            "take_profit": 102.0 if executable else None,
            "risk_per_unit": 1.0 if executable else None,
            "reward_per_unit": 2.0 if executable else None,
            "rr": 2.0 if executable else None,
            "executable": executable,
            "confidence": 80,
            "reason_codes": [],
            "guards": {
                "opportunity_type": "MARKET_EXECUTABLE" if executable else "WAIT_FOR_RETEST",
                "net_rr": 1.5 if executable else None,
                "micro_direction_confirmed": line != "without_micro" if executable else False,
            },
            "input_refs": {},
        },
    )


def test_step52_p10_aggregate_prefers_micro_fast_executable() -> None:
    docs = {
        "without_micro": _trade_doc("without_micro", _trade_plan("AAAUSDT", executable=True, line="without_micro")),
        "micro_fast": _trade_doc("micro_fast", _trade_plan("AAAUSDT", executable=True, line="micro_fast")),
        "micro_full": _trade_doc("micro_full", _trade_plan("AAAUSDT", executable=False, line="micro_full")),
    }
    out = build_final_decisions_from_trade_plans(
        docs=docs,
        generated_at="2099-01-01T00:00:00Z",
        sources={line: Path(f"{line}.json") for line in docs},
    )
    assert out.meta.planner_config_version == "5.2-p10-aggregate"
    assert out.count == 1
    item = out.decisions[0]
    assert item.action == "ENTER"
    assert item.guards["source_trade_plan_line"] == "micro_fast"
    assert item.risk_plan.plan_status == "executable"


def test_step52_p10_aggregate_cli_writes_latest_decisions(tmp_path: Path) -> None:
    out_dir = tmp_path / "DATA" / "decisions"
    out_dir.mkdir(parents=True)
    docs = {
        "without_micro": _trade_doc("without_micro", _trade_plan("AAAUSDT", executable=False, line="without_micro")),
        "micro_fast": _trade_doc("micro_fast", _trade_plan("AAAUSDT", executable=True, line="micro_fast")),
        "micro_full": _trade_doc("micro_full", _trade_plan("AAAUSDT", executable=False, line="micro_full")),
    }
    names = {
        "without_micro": "latest_trade_plan_without_micro.json",
        "micro_fast": "latest_trade_plan_micro_fast.json",
        "micro_full": "latest_trade_plan_micro_full.json",
    }
    for line, doc in docs.items():
        (out_dir / names[line]).write_text(json.dumps(doc.model_dump(mode="json")), encoding="utf-8")
    out = out_dir / "latest_decisions.json"
    rc = run_apply_final_decisions_from_trade_plans_safe(project_root=tmp_path, output_path=out)
    assert rc == 0
    parsed = FinalDecisionsDocument.model_validate(json.loads(out.read_text(encoding="utf-8")))
    assert parsed.meta.planner_config_version == "5.2-p10-aggregate"
    assert parsed.decisions[0].guards["source_trade_plan_line"] == "micro_fast"
