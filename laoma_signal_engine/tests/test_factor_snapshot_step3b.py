"""STEP3B factor snapshot tests. docs/STEP4.0_Decision_Layer_Phase1_任务卡.md."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from laoma_signal_engine.core.time_utils import parse_iso_z as parse_dt
from laoma_signal_engine.factors.assembler import build_factor_snapshot_document
from laoma_signal_engine.factors.factor_snapshot import run_assemble_factor_snapshot
from laoma_signal_engine.factors.models import FactorSnapshotDocument
from laoma_signal_engine.factors.writer import atomic_write_factor_snapshot
from laoma_signal_engine.market.light_snapshot_models import FuturesLightSnapshotDocument
from laoma_signal_engine.micro.assembly.assembler import AssemblyTargetRow, build_document
from laoma_signal_engine.micro.assembly.models import LatestMicroFeaturesDocument
from laoma_signal_engine.micro.micro_target_models import MicroTargetEntry
from laoma_signal_engine.micro.quality.models import CoverageSummary, MicroQualitySnapshot
from laoma_signal_engine.scanner.signal_models import AbnormalTierDocument

GEN = "2026-05-12T12:00:00Z"


def _cov() -> CoverageSummary:
    return CoverageSummary(
        stream_type="aggTrade",
        window_sec=900,
        expected_seconds=900,
        covered_seconds=900,
    )


def _quality(symbol: str, *, ready: bool) -> MicroQualitySnapshot:
    return MicroQualitySnapshot(
        symbol=symbol,
        ready=ready,
        reason_codes=(),
        reference_ts_sec=1,
        collect_started_ts_sec=1,
        warmup_age_sec=1,
        cvd_update_age_sec=1.0,
        ofi_update_age_sec=1.0,
        last_update_age_sec=1.0,
        max_lag_sec=0.0,
        coverage={"aggTrade": _cov()},
        driver_metrics_summary={"cvd_update_count": 1},
    )


class _Drv:
    def get_latest_cvd(self, symbol: str) -> dict[str, Any]:
        _ = symbol
        return {"cvd": 1.0, "z_cvd": 0.5, "cvd_state": "positive"}

    def get_latest_ofi(self, symbol: str) -> dict[str, Any]:
        _ = symbol
        return {"ofi": 1.0, "z_ofi": 0.5, "ofi_state": "positive"}


def _micro_doc(symbol: str, *, ready: bool, move_side: str = "down") -> LatestMicroFeaturesDocument:
    row = AssemblyTargetRow(symbol=symbol, ofi_levels=1, move_side=move_side)
    return build_document(
        targets=[row],
        quality_by_symbol={symbol: _quality(symbol, ready=ready)},
        driver=_Drv(),
        generated_at=GEN,
        status="ok",
        target_generated_at=GEN,
        target_age_sec=1,
        target_status="fresh",
        dropped_events_trade=0,
        dropped_events_book=0,
        dropped_events_depth=0,
    )


def _score_breakdown() -> dict[str, Any]:
    return {
        "price_score": 10,
        "volume_score": 10,
        "kline_cvd_score": 5,
        "trigger_5m_score": 5,
        "liquidity_score": 5,
        "background_penalty": 0,
    }


def _signal(
    *,
    symbol: str,
    state: str,
    scan_score: int = 60,
    move_side: str = "down",
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "base_asset": symbol.replace("USDT", ""),
        "futures_symbol": symbol,
        "has_um_futures": True,
        "decision_tf": "15m",
        "source_tags": [],
        "state": state,
        "move_side": move_side,
        "scan_score": scan_score,
        "score_breakdown": _score_breakdown(),
        "input_snapshot_generated_at": GEN,
        "trigger_type": "futures_15m_price_spike",
        "primary_15m": {
            "price_ret": -1.0,
            "volume_ratio": 1.5,
            "ready": True,
        },
        "trigger_5m": {"acceleration_state": "neutral"},
        "background": {"price_ret_24h": 0.0},
        "reason_codes": [],
        "next_stage": "warm_pool",
    }


def _tier(*signals: dict[str, Any]) -> AbnormalTierDocument:
    return AbnormalTierDocument.model_validate(
        {
            "schema_version": "1.6",
            "generated_at": GEN,
            "tier": "watch_candidate",
            "status": "ok",
            "input_snapshot_generated_at": GEN,
            "input_snapshot_age_sec": 0,
            "input_freshness": "fresh",
            "count": len(signals),
            "signals": list(signals),
        }
    )


def _light_with_entry(symbol: str, entry_val: float = 1.23) -> FuturesLightSnapshotDocument:
    return FuturesLightSnapshotDocument.model_validate(
        {
            "schema_version": "1.6",
            "generated_at": GEN,
            "source": "binance_um_futures",
            "universe_generated_at": GEN,
            "universe_age_sec": 0,
            "universe_count": 1,
            "eligible_futures_count": 1,
            "snapshot_count": 1,
            "success_count": 1,
            "failed_count": 0,
            "skipped_count": 0,
            "timeframe_contract": {
                "primary_tf": "15m",
                "trigger_tf": "5m",
                "entry_tf": "1m",
                "background_tfs": ["1h", "24h"],
                "decision_basis": "rolling_15m",
            },
            "items": [
                {
                    "symbol": symbol,
                    "base_asset": "H",
                    "last_price": 0.1,
                    "decision_tf": "15m",
                    "primary_15m": {"ready": True},
                    "trigger_5m": {},
                    "entry_1m": {"atr": entry_val, "last_pullback_low": 0.2},
                    "background": {},
                    "reason_codes": [],
                    "data_quality": {},
                }
            ],
            "errors": [],
        }
    )


def test_no_candidates_empty_watch_and_strong() -> None:
    w = _tier()
    s = _tier()
    light = _light_with_entry("BTCUSDT")
    micro = _micro_doc("BTCUSDT", ready=True)
    doc = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    assert doc.status == "no_candidates"
    assert doc.count == 0
    assert doc.items == []


def test_merges_entry_1m_from_light_snapshot() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym, entry_val=99.0)
    micro = _micro_doc(sym, ready=True)
    doc = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    assert doc.status == "ok"
    assert doc.count == 1
    assert doc.items[0].entry_1m.get("atr") == 99.0
    assert doc.items[0].source_state == "watch_candidate"
    assert doc.items[0].micro_fast_signal is not None
    assert doc.items[0].micro_fast_signal.micro_signal_usable is False
    assert doc.items[0].micro_full_signal is not None


def test_step3b4_factor_snapshot_carries_micro_signal_contract() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate", move_side="up"))
    s = _tier()
    light = _light_with_entry(sym)
    micro = _micro_doc(sym, ready=True, move_side="up")
    doc = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    item = doc.items[0]
    assert item.micro_full_signal is not None
    assert item.micro_full_signal.micro_signal_usable is True
    assert item.micro_full_signal.micro_alignment_state in {"aligned_weak", "aligned_strong"}


def test_step3b4_stale_micro_downgrades_signal_contract() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    micro = _micro_doc(sym, ready=True)
    doc = build_factor_snapshot_document(
        watch=w,
        strong=s,
        light=light,
        micro=micro,
        generated_at=GEN,
        now=parse_dt("2026-05-12T12:10:00Z"),
        micro_features_max_age_sec=30,
        micro_target_max_age_sec=9999,
    )
    item = doc.items[0]
    assert item.micro_fast_signal is not None
    assert item.micro_fast_signal.micro_signal_usable is False
    assert item.micro_fast_signal.micro_direction_confirmed is False
    assert "micro_features_stale" in item.micro_fast_signal.reason_codes


def test_strong_overwrites_watch_same_symbol() -> None:
    sym = "BTCUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate", scan_score=50))
    s = _tier(_signal(symbol=sym, state="strong_candidate", scan_score=99))
    light = _light_with_entry("HUSDT")
    micro = _micro_doc(sym, ready=True)
    doc = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    assert doc.items[0].source_state == "strong_candidate"
    assert doc.items[0].scan_score == 99


def test_micro_missing_sets_factor_reason_and_partial_status() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    micro = _micro_doc("OTHERUSDT", ready=True)
    doc = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    assert doc.status == "partial"
    assert "micro_missing" in doc.items[0].factor_quality.reason_codes
    assert doc.items[0].micro_15m.ready is False
    assert doc.items[0].micro_quality.ready is False
    assert doc.items[0].factor_quality.ready is False


def test_with_micro_filters_candidates_to_micro_targets() -> None:
    allowed = "HUSDT"
    excluded = "OTHERUSDT"
    w = _tier(
        _signal(symbol=allowed, state="watch_candidate"),
        _signal(symbol=excluded, state="watch_candidate"),
    )
    s = _tier()
    light = _light_with_entry(allowed)
    micro = _micro_doc(allowed, ready=True)
    doc = build_factor_snapshot_document(
        watch=w,
        strong=s,
        light=light,
        micro=micro,
        generated_at=GEN,
        micro_plan_candidate_symbols={allowed},
        micro_target_generated_at=GEN,
        micro_target_version="test-target-version",
    )

    assert doc.status == "ok"
    assert [item.symbol for item in doc.items] == [allowed]
    assert doc.candidate_alignment["mode"] == "micro_targets_authoritative"
    assert doc.candidate_alignment["input_symbol_count"] == 2
    assert doc.candidate_alignment["allowed_symbol_count"] == 1
    assert doc.candidate_alignment["output_symbol_count"] == 1
    assert doc.candidate_alignment["excluded_symbols"] == [excluded]
    assert doc.input_refs["micro_plan_candidate_count"] == 1


def test_with_micro_includes_sticky_micro_target_not_in_current_step2() -> None:
    current = "HUSDT"
    sticky = "OLDUSDT"
    w = _tier(_signal(symbol=current, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(current)
    micro = _micro_doc(current, ready=True)
    sticky_entry = MicroTargetEntry(
        symbol=sticky,
        base_asset="OLD",
        source_state="watch_candidate",
        priority=70,
        scan_score=70,
        market_entry_suitability_score=65,
        market_entry_suitability="allowed",
        trade_candidate_rank_score=60,
        trade_candidate_bucket="allowed",
        move_side="up",
        trigger_type="sticky_previous_target",
        subscribe=["aggTrade", "bookTicker"],
        target_ready_tf="15m",
        min_collect_seconds=900,
        ttl_seconds=1800,
        sticky_source="previous_target",
        retained_reason="sticky_warmup",
    )
    doc = build_factor_snapshot_document(
        watch=w,
        strong=s,
        light=light,
        micro=micro,
        generated_at=GEN,
        micro_plan_candidate_symbols={current, sticky},
        micro_target_entries={sticky: sticky_entry},
        micro_target_generated_at=GEN,
        micro_target_version="sticky-target-version",
    )

    assert doc.count == 2
    assert [item.symbol for item in doc.items] == [current, sticky]
    old = next(item for item in doc.items if item.symbol == sticky)
    assert old.source_state == "watch_candidate"
    assert old.scan_score == 70
    assert "micro_missing" in old.factor_quality.reason_codes
    assert doc.candidate_alignment["synthetic_sticky_symbol_count"] == 1
    assert doc.candidate_alignment["synthetic_sticky_symbols"] == [sticky]


def test_factor_snapshot_blocks_when_micro_targets_stale(tmp_path: Path) -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    watch_p = tmp_path / "watch.json"
    strong_p = tmp_path / "strong.json"
    light_p = tmp_path / "light.json"
    targets_p = tmp_path / "micro_targets.json"
    out_p = tmp_path / "factor.json"
    watch_p.write_text(json.dumps(w.model_dump(mode="json")), encoding="utf-8")
    strong_p.write_text(json.dumps(s.model_dump(mode="json")), encoding="utf-8")
    light_p.write_text(json.dumps(light.model_dump(mode="json")), encoding="utf-8")
    targets_p.write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": GEN,
                "source": "micro_target_router",
                "status": "stale_input",
                "warm_watch_limit": 30,
                "active_strong_limit": 10,
                "input_watch_status": "stale_input",
                "input_strong_status": "stale_input",
                "input_snapshot_generated_at": GEN,
                "input_snapshot_age_sec": 600,
                "router_freshness_ok": False,
                "input_counts": {"raw": 0, "watch": 1, "strong": 0},
                "routed_counts": {"tier1": 0, "tier2": 0},
                "truncated": {"tier1": False, "tier2": False},
                "skip_reasons": ["watch_input_stale", "strong_input_stale"],
                "block_downstream": True,
                "block_reason": "step2_stale",
                "tier1_warm_watch": [],
                "tier2_active_strong": [],
            },
        ),
        encoding="utf-8",
    )

    assert (
        run_assemble_factor_snapshot(
            project_root=tmp_path,
            watch_path=watch_p,
            strong_path=strong_p,
            light_path=light_p,
            micro_targets_path=targets_p,
            output_path=out_p,
            skip_market_context=True,
        )
        == 0
    )
    doc = FactorSnapshotDocument.model_validate(json.loads(out_p.read_text(encoding="utf-8")))
    assert doc.status == "blocked"
    assert doc.count == 0
    assert doc.input_refs["blocked_reason"] == "micro_targets_stale_input"
    assert doc.candidate_alignment["blocked_by"] == "step2_stale"


def test_micro_doc_not_ok_partial_all_items() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    base = _micro_doc(sym, ready=True)
    raw = base.model_dump(mode="json")
    raw["status"] = "error"
    micro = LatestMicroFeaturesDocument.model_validate(raw)
    doc = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    assert doc.status == "partial"
    assert "micro_input_invalid" in doc.items[0].factor_quality.reason_codes


def test_micro_target_stale_reason() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    base = _micro_doc(sym, ready=True)
    raw = base.model_dump(mode="json")
    raw["target_status"] = "stale"
    micro = LatestMicroFeaturesDocument.model_validate(raw)
    doc = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    assert doc.status == "partial"
    assert "micro_input_not_fresh" in doc.items[0].factor_quality.reason_codes


def test_micro_features_generated_at_stale_forces_not_ready() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    micro = _micro_doc(sym, ready=True)
    doc = build_factor_snapshot_document(
        watch=w,
        strong=s,
        light=light,
        micro=micro,
        generated_at=GEN,
        now=parse_dt("2026-05-12T12:10:00Z"),
        micro_features_max_age_sec=30,
        micro_target_max_age_sec=9999,
    )
    assert doc.status == "partial"
    assert "micro_features_stale" in doc.items[0].factor_quality.reason_codes
    assert doc.items[0].micro_15m.ready is False
    assert doc.items[0].micro_quality.ready is False


def test_micro_target_anchor_stale_forces_not_ready() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    micro = _micro_doc(sym, ready=True)
    doc = build_factor_snapshot_document(
        watch=w,
        strong=s,
        light=light,
        micro=micro,
        generated_at=GEN,
        now=parse_dt("2026-05-12T12:10:00Z"),
        micro_features_max_age_sec=9999,
        micro_target_max_age_sec=30,
    )
    assert doc.status == "partial"
    assert "micro_target_anchor_stale" in doc.items[0].factor_quality.reason_codes
    assert doc.items[0].micro_15m.ready is False


def test_context_guard_placeholders_not_shared() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    micro = _micro_doc(sym, ready=True)
    doc = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    it = doc.items[0]
    assert it.oi_15m is not it.funding_context
    assert it.oi_15m is not it.basis_15m


def test_extra_forbid_item() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    micro = _micro_doc(sym, ready=True)
    doc = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    payload = doc.model_dump(mode="json")
    payload["items"][0]["bogus"] = True
    with pytest.raises(ValidationError):
        FactorSnapshotDocument.model_validate(payload)


def test_atomic_write_roundtrip(tmp_path: Path) -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    micro = _micro_doc(sym, ready=True)
    doc = build_factor_snapshot_document(watch=w, strong=s, light=light, micro=micro, generated_at=GEN)
    out = tmp_path / "latest_factor_snapshot.json"
    atomic_write_factor_snapshot(out, doc)
    loaded = json.loads(out.read_text(encoding="utf-8"))
    parsed = FactorSnapshotDocument.model_validate(loaded)
    assert parsed.count == len(parsed.items)
    assert parsed.items[0].symbol == sym


def test_build_without_micro_offline() -> None:
    sym = "HUSDT"
    w = _tier(_signal(symbol=sym, state="watch_candidate"))
    s = _tier()
    light = _light_with_entry(sym)
    doc = build_factor_snapshot_document(
        watch=w,
        strong=s,
        light=light,
        micro=None,
        generated_at=GEN,
        fetch_market_context=False,
    )
    assert doc.source == "factor_snapshot_without_ofi_cvd"
    assert doc.status == "partial"
    assert doc.items[0].micro_quality.reason_codes == ["micro_pipeline_skipped"]
    assert "micro_pipeline_skipped" in doc.items[0].factor_quality.reason_codes
    assert doc.items[0].micro_fast_signal is None
    assert doc.items[0].micro_full_signal is None
