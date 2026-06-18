"""STEP3.7 Feature assembly tests A1-A12. docs/STEP3.7_任务卡.md."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from laoma_signal_engine.micro.assembly import (
    AssemblyTargetRow,
    LatestMicroFeaturesDocument,
    atomic_write_json,
    build_document,
    build_micro_15m_block,
    snapshot_to_micro_quality,
)
from laoma_signal_engine.micro.assembly.models import (
    DroppedEventsBlock,
    Micro15mBlock,
    MicroFeatureItem,
)
from laoma_signal_engine.micro.quality.models import CoverageSummary, MicroQualitySnapshot


SYM = "BTCUSDT"
GEN_AT = "2026-05-11T00:00:00Z"


def _cov_agg() -> CoverageSummary:
    return CoverageSummary(
        stream_type="aggTrade",
        window_sec=900,
        expected_seconds=900,
        covered_seconds=800,
    )


def _minimal_snapshot(
    *,
    symbol: str = SYM,
    ready: bool = True,
    reason_codes: tuple[str, ...] = (),
) -> MicroQualitySnapshot:
    return MicroQualitySnapshot(
        symbol=symbol,
        ready=ready,
        reason_codes=reason_codes,
        reference_ts_sec=1000,
        collect_started_ts_sec=500,
        warmup_age_sec=600,
        cvd_update_age_sec=1.0,
        ofi_update_age_sec=1.0,
        last_update_age_sec=1.0,
        max_lag_sec=0.0,
        coverage={"aggTrade": _cov_agg()},
        driver_metrics_summary={"cvd_update_count": 1},
    )


class DummyDriver:
    def __init__(
        self,
        cvd: dict[str, Any] | None = None,
        ofi: dict[str, Any] | None = None,
    ) -> None:
        self._cvd = cvd
        self._ofi = ofi

    def get_latest_cvd(self, symbol: str) -> dict[str, Any] | None:
        _ = symbol
        return self._cvd

    def get_latest_ofi(self, symbol: str) -> dict[str, Any] | None:
        _ = symbol
        return self._ofi


def _base_meta() -> dict[str, Any]:
    return {
        "generated_at": GEN_AT,
        "status": "ok",
        "target_generated_at": GEN_AT,
        "target_age_sec": 12,
        "target_status": "fresh",
        "dropped_events_trade": 0,
        "dropped_events_book": 0,
        "dropped_events_depth": 0,
    }


def test_a1_minimal_one_item() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=5, tier="tier2_active_strong")
    snap = _minimal_snapshot()
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(),
        **_base_meta(),
    )
    assert doc.symbol_count == 1
    assert len(doc.items) == 1
    assert doc.items[0].symbol == SYM
    dumped = doc.model_dump(mode="json")
    for key in (
        "schema_version",
        "generated_at",
        "source",
        "status",
        "target_generated_at",
        "target_age_sec",
        "target_status",
        "symbol_count",
        "ready_count",
        "not_ready_count",
        "fast_ready_count",
        "full_ready_count",
        "ws_status",
        "last_ws_message_age_sec",
        "dropped_events",
        "items",
    ):
        assert key in dumped


def test_a2_ready_count_matches_quality() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=1)
    snap = _minimal_snapshot(ready=True)
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(),
        **_base_meta(),
    )
    assert doc.ready_count == 1
    assert doc.not_ready_count == 0
    assert doc.full_ready_count == 1


def test_fast_full_ready_counts_are_separate() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=1)
    full_snap = _minimal_snapshot(ready=False)
    fast_snap = _minimal_snapshot(ready=True)
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: full_snap},
        fast_quality_by_symbol={SYM: fast_snap},
        driver=DummyDriver(),
        **_base_meta(),
    )
    assert doc.ready_count == 0
    assert doc.full_ready_count == 0
    assert doc.fast_ready_count == 1
    assert doc.items[0].micro_quality.ready is False
    assert doc.items[0].micro_full_quality is not None
    assert doc.items[0].micro_full_quality.ready is False
    assert doc.items[0].micro_fast_quality is not None
    assert doc.items[0].micro_fast_quality.ready is True


def test_a3_fusion_placeholders_null_or_false() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=5)
    snap = _minimal_snapshot()
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(),
        **_base_meta(),
    )
    m = doc.items[0].micro_15m
    assert m.fusion_score is None
    assert m.fusion_consistency is None
    assert m.fusion_signal is None
    assert m.fusion_ready is False


def test_a4_round_trip_dump_validate() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=5)
    snap = _minimal_snapshot()
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(),
        **_base_meta(),
    )
    raw = doc.model_dump(mode="json")
    doc2 = LatestMicroFeaturesDocument.model_validate(raw)
    assert doc2.model_dump(mode="json") == raw


def test_a5_driver_latest_none_still_full_micro_15m() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=5)
    snap = _minimal_snapshot(ready=False)
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(cvd=None, ofi=None),
        **_base_meta(),
    )
    m = doc.items[0].micro_15m
    assert m.cvd is None
    assert m.z_cvd is None
    assert m.cvd_state == "unknown"
    assert m.ofi is None
    assert m.z_ofi is None
    assert m.ofi_state == "unknown"
    assert m.ofi_pressure == "unknown"
    assert m.ready is False


def test_a6_mismatched_ready_raises() -> None:
    snap = _minimal_snapshot(ready=True)
    mq = snapshot_to_micro_quality(snap)
    m15_bad = Micro15mBlock(
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
    item = MicroFeatureItem(
        symbol=SYM,
        ofi_levels=5,
        micro_15m=m15_bad,
        micro_quality=mq,
    )
    with pytest.raises(ValidationError):
        LatestMicroFeaturesDocument(
            generated_at=GEN_AT,
            status="ok",
            target_generated_at=GEN_AT,
            target_age_sec=0,
            target_status="fresh",
            symbol_count=1,
            ready_count=1,
            not_ready_count=0,
            ws_status="unknown",
            last_ws_message_age_sec=None,
            dropped_events=DroppedEventsBlock(trade=0, book=0, depth=0),
            items=[item],
        )


def test_a7_schema_version_and_source() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=5)
    snap = _minimal_snapshot()
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(),
        **_base_meta(),
    )
    assert doc.schema_version == "1.6"
    assert doc.source == "micro_feature_assembler"


def test_a8_no_websocket_httpx_imports_in_assembly_sources() -> None:
    root = Path(__file__).resolve().parents[1] / "micro" / "assembly"
    for name in ("__init__.py", "assembler.py", "models.py", "writer.py"):
        text = (root / name).read_text(encoding="utf-8")
        for bad in (
            "import websocket",
            "from websocket",
            "import httpx",
            "from httpx",
        ):
            assert bad not in text, f"{name} must not {bad}"


def test_a9_missing_quality_snapshot_raises() -> None:
    row = AssemblyTargetRow(symbol="ETHUSDT", ofi_levels=5)
    snap = _minimal_snapshot(symbol=SYM)
    with pytest.raises(ValueError, match="ETHUSDT"):
        build_document(
            targets=[row],
            quality_by_symbol={SYM: snap},
            driver=DummyDriver(),
            **_base_meta(),
        )


def test_a10_extra_forbid_top_level() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=5)
    snap = _minimal_snapshot()
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(),
        **_base_meta(),
    )
    raw = doc.model_dump(mode="json")
    raw["unexpected_field"] = 1
    with pytest.raises(ValidationError):
        LatestMicroFeaturesDocument.model_validate(raw)


def test_a11_atomic_write_then_parse(tmp_path: Path) -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=5)
    snap = _minimal_snapshot()
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(),
        **_base_meta(),
    )
    path = tmp_path / "latest_micro_features.json"
    atomic_write_json(path, doc)
    text = path.read_text(encoding="utf-8")
    loaded = json.loads(text)
    doc2 = LatestMicroFeaturesDocument.model_validate(loaded)
    assert doc2.schema_version == doc.schema_version
    assert doc2.symbol_count == doc.symbol_count


def test_a12_count_mismatch_raises() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=5)
    snap = _minimal_snapshot()
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(),
        **_base_meta(),
    )
    raw = doc.model_dump(mode="json")
    raw["symbol_count"] = 99
    with pytest.raises(ValidationError):
        LatestMicroFeaturesDocument.model_validate(raw)
    raw = doc.model_dump(mode="json")
    raw["ready_count"] = 0
    with pytest.raises(ValidationError):
        LatestMicroFeaturesDocument.model_validate(raw)


def test_mvp_micro_state_fixed_not_ready() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=5)
    snap = _minimal_snapshot()
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(
            cvd={"cvd": 1.0, "z_cvd": 0.1, "cvd_state": "bull"},
            ofi={"ofi": 2.0, "z_ofi": -0.2, "ofi_state": "x", "ofi_pressure": "y"},
        ),
        **_base_meta(),
    )
    assert doc.items[0].micro_15m.micro_state == "not_ready"


def test_empty_target_strings_become_null() -> None:
    row = AssemblyTargetRow(
        symbol=SYM,
        ofi_levels=5,
        symbol_safe_id="",
        tier="",
        source_state="",
        move_side="",
        trigger_type="",
    )
    snap = _minimal_snapshot()
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(),
        **_base_meta(),
    )
    it = doc.items[0]
    assert it.symbol_safe_id is None
    assert it.tier is None
    assert it.source_state is None
    assert it.move_side is None
    assert it.trigger_type is None


def test_build_micro_15m_maps_engine_dict() -> None:
    drv = DummyDriver(
        cvd={"cvd": 3.0, "z_cvd": 0.5, "cvd_state": "steady"},
        ofi={"ofi": -1.0, "z_ofi": None, "ofi_state": "bid", "ofi_pressure": "light"},
    )
    b = build_micro_15m_block(drv, SYM, quality_ready=True)
    assert b.ready is True
    assert b.cvd == 3.0
    assert b.z_cvd == 0.5
    assert b.cvd_state == "steady"
    assert b.ofi == -1.0
    assert b.z_ofi is None
    assert b.ofi_state == "bid"
    assert b.ofi_pressure == "light"


def test_step310_raw_cvd_ofi_sign_does_not_confirm_without_z() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=1, move_side="up")
    snap = _minimal_snapshot(ready=True)
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        fast_quality_by_symbol={SYM: snap},
        driver=DummyDriver(
            cvd={"cvd": 100.0, "z_cvd": None, "cvd_state": "bull"},
            ofi={"ofi": 100.0, "z_ofi": None, "ofi_state": "bid", "ofi_pressure": "strong"},
        ),
        **_base_meta(),
    )
    sig = doc.items[0].micro_fast_signal
    assert sig is not None
    assert sig.micro_data_ready is True
    assert sig.micro_stat_ready is False
    assert sig.micro_direction_confirmed is False
    assert sig.micro_exec_allowed is False
    assert "fast_z_missing" in sig.reason_codes


def test_step310_fast_one_z_is_usable_but_weak_only() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=1, move_side="up")
    snap = _minimal_snapshot(ready=True)
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        fast_quality_by_symbol={SYM: snap},
        driver=DummyDriver(
            cvd={"cvd": 100.0, "z_cvd": 1.5, "cvd_state": "bull"},
            ofi={"ofi": 100.0, "z_ofi": None, "ofi_state": "bid", "ofi_pressure": "strong"},
        ),
        **_base_meta(),
    )
    sig = doc.items[0].micro_fast_signal
    assert sig is not None
    assert sig.micro_stat_ready is True
    assert sig.micro_signal_usable is True
    assert sig.micro_alignment_state == "aligned_weak"
    assert sig.micro_direction_confirmed is False
    assert sig.micro_confirmation_level == "hint"
    assert "fast_one_z_available_weak_only" in sig.reason_codes


def test_step1015_fast_two_strong_z_aligned_confirms_with_proxy() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=1, move_side="up")
    snap = _minimal_snapshot(ready=True)
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        fast_quality_by_symbol={SYM: snap},
        driver=DummyDriver(
            cvd={"cvd": 100.0, "z_cvd": 1.5, "cvd_state": "bull"},
            ofi={"ofi": 100.0, "z_ofi": 1.2, "ofi_state": "bid", "ofi_pressure": "strong"},
        ),
        **_base_meta(),
    )
    sig = doc.items[0].micro_fast_signal
    assert sig is not None
    assert sig.micro_signal_usable is True
    assert sig.micro_alignment_state == "aligned_strong"
    assert sig.micro_strength == "strong"
    assert sig.micro_confirmation_level == "strong"
    assert sig.price_response_ok is True
    assert sig.micro_direction_confirmed is True
    assert sig.micro_exec_allowed is True
    assert "fast_price_response_proxy" in sig.reason_codes


def test_step1015_fast_two_weak_z_aligned_does_not_confirm() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=1, move_side="up")
    snap = _minimal_snapshot(ready=True)
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        fast_quality_by_symbol={SYM: snap},
        driver=DummyDriver(
            cvd={"cvd": 100.0, "z_cvd": 0.5, "cvd_state": "bull"},
            ofi={"ofi": 100.0, "z_ofi": 0.7, "ofi_state": "bid", "ofi_pressure": "strong"},
        ),
        **_base_meta(),
    )
    sig = doc.items[0].micro_fast_signal
    assert sig is not None
    assert sig.micro_alignment_state == "aligned_weak"
    assert sig.micro_confirmation_level == "weak"
    assert sig.price_response_ok is None
    assert sig.micro_direction_confirmed is True
    assert sig.micro_exec_allowed is True
    assert "fast_weak_alignment_confirmed" in sig.reason_codes


def test_step1015_full_two_z_aligned_confirms_with_persistence_proxy() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=5, move_side="down")
    snap = _minimal_snapshot(ready=True)
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        driver=DummyDriver(
            cvd={"cvd": -100.0, "z_cvd": -0.6, "cvd_state": "bear"},
            ofi={"ofi": -100.0, "z_ofi": -0.8, "ofi_state": "ask", "ofi_pressure": "strong"},
        ),
        **_base_meta(),
    )
    sig = doc.items[0].micro_full_signal
    assert sig is not None
    assert sig.micro_signal_usable is True
    assert sig.micro_alignment_state == "aligned_weak"
    assert sig.micro_confirmation_level == "weak"
    assert sig.persistence_ok is True
    assert sig.micro_direction_confirmed is True
    assert sig.micro_exec_allowed is True
    assert "full_persistence_proxy" in sig.reason_codes


def test_step1015_conflict_still_does_not_confirm() -> None:
    row = AssemblyTargetRow(symbol=SYM, ofi_levels=1, move_side="up")
    snap = _minimal_snapshot(ready=True)
    doc = build_document(
        targets=[row],
        quality_by_symbol={SYM: snap},
        fast_quality_by_symbol={SYM: snap},
        driver=DummyDriver(
            cvd={"cvd": 100.0, "z_cvd": 1.5, "cvd_state": "bull"},
            ofi={"ofi": -100.0, "z_ofi": -1.2, "ofi_state": "ask", "ofi_pressure": "strong"},
        ),
        **_base_meta(),
    )
    sig = doc.items[0].micro_fast_signal
    assert sig is not None
    assert sig.micro_alignment_state == "mixed"
    assert sig.micro_confirmation_level == "hint"
    assert sig.micro_direction_confirmed is False
    assert sig.micro_exec_allowed is False


def test_micro_quality_projection_matches_snapshot() -> None:
    snap = _minimal_snapshot(ready=False, reason_codes=("warmup_not_met", "cvd_stale"))
    blk = snapshot_to_micro_quality(snap)
    assert blk.ready is snap.ready
    assert tuple(blk.reason_codes) == snap.reason_codes
    assert blk.reference_ts_sec == snap.reference_ts_sec
    assert blk.collect_started_ts_sec == snap.collect_started_ts_sec
    assert blk.warmup_age_sec == snap.warmup_age_sec
    assert blk.cvd_update_age_sec == snap.cvd_update_age_sec
    assert blk.ofi_update_age_sec == snap.ofi_update_age_sec
    assert blk.last_update_age_sec == snap.last_update_age_sec
    assert blk.max_lag_sec == snap.max_lag_sec
    assert blk.coverage.keys() == snap.coverage.keys()
    for k, cs in snap.coverage.items():
        b = blk.coverage[k]
        assert b.stream_type == cs.stream_type
        assert b.window_sec == cs.window_sec
        assert b.expected_seconds == cs.expected_seconds
        assert b.covered_seconds == cs.covered_seconds
    assert blk.driver_metrics_summary == snap.driver_metrics_summary
    assert blk.data_quality_root_cause_class == snap.data_quality_root_cause_class
    assert blk.reason_root_causes == (snap.reason_root_causes or {})
