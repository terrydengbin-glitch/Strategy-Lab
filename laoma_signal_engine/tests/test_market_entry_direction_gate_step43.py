"""STEP4.3 market-entry direction gate tests."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from laoma_signal_engine.decision.market_entry_direction_gate import (
    build_market_entry_direction_document,
    run_apply_market_entry_direction_gate_safe,
)
from laoma_signal_engine.factors.models import FactorSnapshotDocument
from laoma_signal_engine.market.decision_refresh import build_decision_refresh_document
from laoma_signal_engine.market.decision_refresh_models import DecisionRefreshDocument
from laoma_signal_engine.micro.assembly.assembler import AssemblyTargetRow, build_document
from laoma_signal_engine.micro.assembly.models import LatestMicroFeaturesDocument, MicroSignalBlock
from laoma_signal_engine.tests.test_decision_refresh_step43a import GEN, SYM, _Drv, _factor_doc, _light_doc, _quality


def _factor_with_market_entry(score: int = 80, move_side: str = "up") -> FactorSnapshotDocument:
    raw = _factor_doc(move_side).model_dump(mode="json")
    raw["items"][0]["market_entry_suitability_score"] = score
    raw["items"][0]["market_entry_suitability"] = "preferred" if score >= 75 else "allowed"
    raw["items"][0]["market_entry_reason_codes"] = []
    raw["items"][0]["micro_fast_signal"] = {
        "micro_data_ready": True,
        "micro_stat_ready": True,
        "micro_signal_usable": True,
        "micro_direction_confirmed": True,
        "micro_exec_allowed": True,
        "micro_alignment_state": "aligned_strong",
        "micro_strength": "strong",
        "price_response_ok": True,
        "persistence_ok": None,
        "reason_codes": [],
    }
    return FactorSnapshotDocument.model_validate(raw)


def _micro_doc(*, fast_ready: bool = True, full_ready: bool = False, z: float = 1.0) -> LatestMicroFeaturesDocument:
    fast_signal = MicroSignalBlock(
        micro_data_ready=fast_ready,
        micro_stat_ready=True,
        micro_signal_usable=fast_ready,
        micro_direction_confirmed=fast_ready and z > 0,
        micro_exec_allowed=fast_ready and z > 0,
        micro_alignment_state="aligned_strong" if fast_ready and z > 0 else "conflict",
        micro_strength="strong" if fast_ready and z > 0 else "none",
        price_response_ok=True if fast_ready and z > 0 else False,
        persistence_ok=None,
        reason_codes=[] if fast_ready and z > 0 else ["micro_direction_conflict"],
    )
    full_signal = MicroSignalBlock(
        micro_data_ready=full_ready,
        micro_stat_ready=True,
        micro_signal_usable=full_ready,
        micro_direction_confirmed=full_ready and z > 0,
        micro_exec_allowed=full_ready and z > 0,
        micro_alignment_state="aligned_strong" if full_ready and z > 0 else "insufficient",
        micro_strength="strong" if full_ready and z > 0 else "none",
        price_response_ok=None,
        persistence_ok=True if full_ready and z > 0 else False,
        reason_codes=[] if full_ready and z > 0 else ["full_not_confirmed"],
    )
    return build_document(
        targets=[AssemblyTargetRow(symbol=SYM, ofi_levels=1)],
        quality_by_symbol={SYM: replace(_quality(SYM), ready=full_ready)},
        fast_quality_by_symbol={SYM: replace(_quality(SYM), ready=fast_ready)},
        driver=_Drv(),
        generated_at=GEN,
        status="ok",
        target_generated_at=GEN,
        target_age_sec=1,
        target_status="fresh",
        dropped_events_trade=0,
        dropped_events_book=0,
        dropped_events_depth=0,
    ).model_copy(
        update={
            "items": [
                build_document(
                    targets=[AssemblyTargetRow(symbol=SYM, ofi_levels=1)],
                    quality_by_symbol={SYM: replace(_quality(SYM), ready=full_ready)},
                    fast_quality_by_symbol={SYM: replace(_quality(SYM), ready=fast_ready)},
                    driver=_Drv(),
                    generated_at=GEN,
                    status="ok",
                    target_generated_at=GEN,
                    target_age_sec=1,
                    target_status="fresh",
                    dropped_events_trade=0,
                    dropped_events_book=0,
                    dropped_events_depth=0,
                ).items[0].model_copy(
                    update={
                        "micro_fast_15m": build_document(
                            targets=[AssemblyTargetRow(symbol=SYM, ofi_levels=1)],
                            quality_by_symbol={SYM: replace(_quality(SYM), ready=full_ready)},
                            fast_quality_by_symbol={SYM: replace(_quality(SYM), ready=fast_ready)},
                            driver=_Drv(),
                            generated_at=GEN,
                            status="ok",
                            target_generated_at=GEN,
                            target_age_sec=1,
                            target_status="fresh",
                            dropped_events_trade=0,
                            dropped_events_book=0,
                            dropped_events_depth=0,
                        ).items[0].micro_fast_15m.model_copy(update={"z_cvd": z, "z_ofi": z}),
                        "micro_fast_signal": fast_signal,
                        "micro_full_signal": full_signal,
                    },
                )
            ],
        },
    )


def _refresh(factor: FactorSnapshotDocument) -> DecisionRefreshDocument:
    return build_decision_refresh_document(
        factor=factor,
        light=_light_doc(),
        liquidity_by_symbol={SYM: {"symbol": SYM, "liquidity_ok_for_market_entry": True}},
        liquidity_generated_at=GEN,
        max_refresh_age_sec=999999,
        max_liquidity_age_sec=999999,
    )


def test_long_market_when_fast_micro_and_refresh_align() -> None:
    factor = _factor_with_market_entry()
    doc = build_market_entry_direction_document(
        factor=factor,
        refresh=_refresh(factor),
        micro=_micro_doc(fast_ready=True, full_ready=False, z=1.0),
        generated_at=GEN,
    )
    assert doc.decisions[0].decision == "LONG_MARKET"
    assert doc.decisions[0].guards["micro_fast_ready"] is True
    assert doc.decisions[0].guards["micro_full_ready"] is False


def test_no_market_entry_when_fast_micro_not_ready() -> None:
    factor = _factor_with_market_entry()
    doc = build_market_entry_direction_document(
        factor=factor,
        refresh=_refresh(factor),
        micro=_micro_doc(fast_ready=False, full_ready=False, z=1.0),
        generated_at=GEN,
    )
    d = doc.decisions[0]
    assert d.decision == "NO_MARKET_ENTRY"
    assert "micro_fast_not_ready" in d.reason_codes


def test_no_market_entry_when_refresh_invalid() -> None:
    factor = _factor_with_market_entry()
    refresh = build_decision_refresh_document(
        factor=factor,
        light=_light_doc(price_ret=-1.0, range_pos=0.95),
        max_refresh_age_sec=999999,
    )
    doc = build_market_entry_direction_document(
        factor=factor,
        refresh=refresh,
        micro=_micro_doc(fast_ready=True, full_ready=True, z=1.0),
        generated_at=GEN,
    )
    d = doc.decisions[0]
    assert d.decision == "NO_MARKET_ENTRY"
    assert "direction_invalid_after_refresh" in d.reason_codes


def test_market_entry_direction_cli_writes_json(tmp_path: Path) -> None:
    factor = _factor_with_market_entry()
    refresh = _refresh(factor)
    micro = _micro_doc()
    factor_p = tmp_path / "factor.json"
    refresh_p = tmp_path / "refresh.json"
    micro_p = tmp_path / "micro.json"
    out_p = tmp_path / "market_entry_direction.json"
    factor_p.write_text(json.dumps(factor.model_dump(mode="json")), encoding="utf-8")
    refresh_p.write_text(json.dumps(refresh.model_dump(mode="json")), encoding="utf-8")
    micro_p.write_text(json.dumps(micro.model_dump(mode="json")), encoding="utf-8")
    code = run_apply_market_entry_direction_gate_safe(
        project_root=tmp_path,
        factor_path=factor_p,
        refresh_path=refresh_p,
        micro_path=micro_p,
        output_path=out_p,
    )
    assert code == 0
    assert out_p.is_file()
