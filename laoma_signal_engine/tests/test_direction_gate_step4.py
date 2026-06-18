"""Direction Gate tests (STEP4). docs/STEP4.0_Decision_Layer_Phase1_任务卡.md."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from laoma_signal_engine.decision.direction_gate import (
    DirectionGateConfig,
    build_direction_gate_document,
    decide_item,
    run_apply_direction_gate_safe,
)
from laoma_signal_engine.decision.models import DirectionDecisionItem, DirectionGateDocument
from laoma_signal_engine.decision.writer import atomic_write_direction_decisions
from laoma_signal_engine.core.exit_codes import EXIT_CONFIG
from laoma_signal_engine.factors.assembler import build_factor_snapshot_document
from laoma_signal_engine.factors.models import (
    Basis15mBlock,
    FactorSnapshotDocument,
    FundingContextBlock,
    OI15mBlock,
)
from laoma_signal_engine.tests.test_factor_snapshot_step3b import (
    GEN,
    _light_with_entry,
    _micro_doc,
    _signal,
    _tier,
)


def _long_strong_factor(*, sym: str = "HUSDT") -> FactorSnapshotDocument:
    s = _signal(symbol=sym, state="strong_candidate", move_side="up")
    s["primary_15m"] = {
        "price_ret": 1.8,
        "volume_ratio": 1.5,
        "ready": True,
        "structure_state": "up_impulse",
        "range_pos": 0.5,
    }
    w = _tier()
    st = _tier(s)
    light = _light_with_entry(sym)
    micro = _micro_doc(sym, ready=True, move_side="up")
    return build_factor_snapshot_document(watch=w, strong=st, light=light, micro=micro, generated_at=GEN)


def _with_ctx_ready(f: FactorSnapshotDocument) -> FactorSnapshotDocument:
    it = f.items[0]
    ok_oi = OI15mBlock(ready=True, reason="ok")
    ok_f = FundingContextBlock(ready=True, reason="ok")
    ok_b = Basis15mBlock(ready=True, reason="ok")
    updates = {"oi_15m": ok_oi, "funding_context": ok_f, "basis_15m": ok_b}
    if it.micro_full_signal is not None and it.micro_full_signal.micro_signal_usable:
        updates["micro_full_signal"] = it.micro_full_signal.model_copy(
            update={
                "micro_direction_confirmed": True,
                "micro_exec_allowed": True,
                "persistence_ok": True,
                "reason_codes": [],
            },
        )
    new_it = it.model_copy(update=updates)
    dumped = f.model_dump(mode="json")
    dumped["items"] = [new_it.model_dump(mode="json")]
    dumped["count"] = 1
    return FactorSnapshotDocument.model_validate(dumped)


def _watch_factor(sym: str = "HUSDT") -> FactorSnapshotDocument:
    s = _signal(symbol=sym, state="watch_candidate", move_side="up")
    s["primary_15m"] = {
        "price_ret": 2.0,
        "volume_ratio": 1.5,
        "ready": True,
        "structure_state": "up_impulse",
        "range_pos": 0.5,
    }
    w = _tier(s)
    st = _tier()
    light = _light_with_entry(sym)
    micro = _micro_doc(sym, ready=True, move_side="up")
    return build_factor_snapshot_document(watch=w, strong=st, light=light, micro=micro, generated_at=GEN)


def test_gate_no_candidates_mirrors_factor() -> None:
    w = _tier()
    s = _tier()
    light = _light_with_entry("X")
    micro = _micro_doc("X", ready=True)
    fac = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    g = build_direction_gate_document(fac, generated_at=GEN)
    assert g.status == "no_candidates"
    assert g.count == 0


def test_gate_error_factor_empty_decisions() -> None:
    fac = FactorSnapshotDocument.model_validate(
        {
            "schema_version": "1.6",
            "generated_at": GEN,
            "source": "factor_snapshot",
            "status": "error",
            "count": 0,
            "items": [],
        }
    )
    g = build_direction_gate_document(fac, generated_at=GEN)
    assert g.status == "error"
    assert g.decisions == []


def test_strong_long_now_when_context_bypass() -> None:
    fac = _with_ctx_ready(_long_strong_factor())
    g = build_direction_gate_document(
        fac,
        generated_at=GEN,
        cfg=DirectionGateConfig(require_context_guards_for_now=False),
    )
    assert g.decisions[0].decision == "LONG_NOW"


def test_watch_no_now_without_allow_watch_even_if_else_ok() -> None:
    fac = _with_ctx_ready(_watch_factor())
    g = build_direction_gate_document(
        fac,
        generated_at=GEN,
        cfg=DirectionGateConfig(
            allow_watch_now=False,
            require_context_guards_for_now=False,
        ),
    )
    assert g.decisions[0].decision != "LONG_NOW"
    assert g.decisions[0].decision == "LONG_WAIT_PULLBACK"


def test_watch_long_now_when_allow_watch() -> None:
    fac = _with_ctx_ready(_watch_factor())
    g = build_direction_gate_document(
        fac,
        generated_at=GEN,
        cfg=DirectionGateConfig(
            allow_watch_now=True,
            require_context_guards_for_now=False,
        ),
    )
    assert g.decisions[0].decision == "LONG_NOW"


def test_micro_not_ready_blocks_now() -> None:
    sym = "HUSDT"
    s = _signal(symbol=sym, state="strong_candidate", move_side="up")
    s["primary_15m"] = {
        "price_ret": 2.0,
        "ready": True,
        "structure_state": "up_impulse",
        "range_pos": 0.5,
    }
    w = _tier()
    st = _tier(s)
    light = _light_with_entry(sym)
    micro = _micro_doc(sym, ready=False)
    fac0 = build_factor_snapshot_document(watch=w, strong=st, light=light, micro=micro, generated_at=GEN)
    fac = _with_ctx_ready(fac0)
    g = build_direction_gate_document(
        fac,
        generated_at=GEN,
        cfg=DirectionGateConfig(require_context_guards_for_now=False),
    )
    assert g.decisions[0].decision != "LONG_NOW"
    codes = g.decisions[0].reason_codes
    assert "micro_15m_not_ready" in codes
    assert "micro_conflict" not in codes
    guards = g.decisions[0].guards
    assert guards["micro_data_quality_state"] in {"ok", "unknown"}


def test_step1049_direction_gate_exposes_micro_data_quality_contract() -> None:
    fac0 = _with_ctx_ready(_long_strong_factor())
    it = fac0.items[0]
    quality = it.micro_quality.model_copy(
        update={
            "ready": False,
            "reason_codes": ["cvd_stale"],
            "cvd_update_age_sec": 120.0,
            "driver_metrics_summary": {
                **it.micro_quality.driver_metrics_summary,
                "processed_trade_bucket_count": 3,
                "cvd_update_count": 0,
            },
        },
    )
    new_it = it.model_copy(update={"micro_quality": quality})
    fac = FactorSnapshotDocument.model_validate(
        {**fac0.model_dump(mode="json"), "items": [new_it.model_dump(mode="json")], "count": 1},
    )

    g = build_direction_gate_document(
        fac,
        generated_at=GEN,
        cfg=DirectionGateConfig(require_context_guards_for_now=False),
    )

    decision = g.decisions[0]
    assert decision.decision != "LONG_NOW"
    assert "data_quality_blocked" in decision.reason_codes
    assert "technical_not_ready" in decision.reason_codes
    assert decision.guards["micro_data_quality_state"] == "technical_blocked"
    assert decision.guards["micro_data_quality_class"] == "technical_fix"
    assert decision.guards["micro_data_quality_attributions"][0]["attributed_reason"] == "technical_bug_cvd_adapter_not_updated"


def test_micro_conflict_when_ready_but_numeric_opposes_long() -> None:
    fac0 = _with_ctx_ready(_long_strong_factor())
    it = fac0.items[0]
    sig = it.micro_full_signal.model_copy(
        update={
            "micro_direction_confirmed": False,
            "micro_exec_allowed": False,
            "micro_alignment_state": "conflict",
            "reason_codes": ["micro_direction_conflict"],
        },
    )
    new_it = it.model_copy(update={"micro_full_signal": sig})
    fac = FactorSnapshotDocument.model_validate(
        {**fac0.model_dump(mode="json"), "items": [new_it.model_dump(mode="json")], "count": 1}
    )
    g = build_direction_gate_document(
        fac,
        generated_at=GEN,
        cfg=DirectionGateConfig(require_context_guards_for_now=False),
    )
    assert g.decisions[0].decision == "LONG_WAIT_PULLBACK"
    assert "micro_conflict" in g.decisions[0].reason_codes
    assert "micro_15m_not_ready" not in g.decisions[0].reason_codes


def test_funding_overheated_blocks_long_now() -> None:
    fac0 = _with_ctx_ready(_long_strong_factor())
    it = fac0.items[0]
    fc = FundingContextBlock(
        ready=True,
        reason="ok",
        funding_rate_raw=0.0006,
        funding_bucket="OVERHEATED",
        funding_extreme_flag=True,
        hours_to_settlement=1.0,
    )
    new_it = it.model_copy(update={"funding_context": fc})
    fac = FactorSnapshotDocument.model_validate(
        {**fac0.model_dump(mode="json"), "items": [new_it.model_dump(mode="json")], "count": 1},
    )
    g = build_direction_gate_document(
        fac,
        generated_at=GEN,
        cfg=DirectionGateConfig(require_context_guards_for_now=False),
    )
    assert g.decisions[0].decision != "LONG_NOW"
    assert "funding_overheated" in g.decisions[0].reason_codes


def test_default_context_placeholders_block_now() -> None:
    fac = _long_strong_factor()
    g = build_direction_gate_document(fac, generated_at=GEN, cfg=DirectionGateConfig())
    assert g.decisions[0].decision == "LONG_WAIT_PULLBACK"
    codes = g.decisions[0].reason_codes
    assert "oi_not_ready" in codes
    assert "funding_not_ready" in codes
    assert "basis_not_ready" in codes


def test_summary_ascii_only() -> None:
    with pytest.raises(ValidationError):
        DirectionDecisionItem(
            symbol="X",
            decision="REJECT",
            direction="NONE",
            action="REJECT",
            entry_mode="NONE",
            confidence=0,
            reason_codes=[],
            summary_for_orchestrator="bad \u03b1",
        )


def test_atomic_write_roundtrip(tmp_path: Path) -> None:
    fac = _with_ctx_ready(_long_strong_factor())
    g = build_direction_gate_document(
        fac,
        generated_at=GEN,
        cfg=DirectionGateConfig(require_context_guards_for_now=False),
    )
    out = tmp_path / "latest_direction_decisions.json"
    atomic_write_direction_decisions(out, g)
    loaded = json.loads(out.read_text(encoding="utf-8"))
    parsed = DirectionGateDocument.model_validate(loaded)
    assert parsed.count == len(parsed.decisions)


def test_decide_item_rejects_neutral_move() -> None:
    fac = _long_strong_factor()
    it = fac.items[0]
    it2 = it.model_copy(update={"move_side": "neutral"})
    fac2 = FactorSnapshotDocument.model_validate(
        {**fac.model_dump(mode="json"), "items": [it2.model_dump(mode="json")], "count": 1}
    )
    d = decide_item(fac2.items[0], factor_doc=fac2, cfg=DirectionGateConfig())
    assert d.decision == "REJECT"


def test_partial_status_from_factor() -> None:
    fac = _long_strong_factor()
    raw = fac.model_dump(mode="json")
    raw["status"] = "partial"
    fac_p = FactorSnapshotDocument.model_validate(raw)
    g = build_direction_gate_document(fac_p, generated_at=GEN)
    assert g.status == "partial"


def test_apply_direction_gate_safe_rejects_stale_factor(tmp_path: Path) -> None:
    fac = _with_ctx_ready(_long_strong_factor())
    factor_path = tmp_path / "DATA" / "factors" / "latest_factor_snapshot.json"
    out = tmp_path / "DATA" / "decisions" / "latest_direction_decisions.json"
    factor_path.parent.mkdir(parents=True)
    factor_path.write_text(json.dumps(fac.model_dump(mode="json")), encoding="utf-8")

    rc = run_apply_direction_gate_safe(
        project_root=tmp_path,
        factor_path=factor_path,
        output_path=out,
        stdout_json=False,
    )
    assert rc == EXIT_CONFIG
    parsed = DirectionGateDocument.model_validate(json.loads(out.read_text(encoding="utf-8")))
    assert parsed.status == "stale_input"
    assert parsed.decisions == []
