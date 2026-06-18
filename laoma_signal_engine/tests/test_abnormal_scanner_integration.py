"""Integration test: Step 2 scanner writes three tier JSON files."""

from __future__ import annotations

import json
from pathlib import Path

from laoma_signal_engine.core.models import CandidateUniverseDocument, UniverseCounts, UniversePairRow
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.market.light_snapshot_models import (
    BackgroundBlock,
    DataQualityBlock,
    Entry1mBlock,
    FuturesLightSnapshotDocument,
    LightSnapshotItem,
    Primary15mBlock,
    TimeframeContract,
    Trigger5mBlock,
)
from laoma_signal_engine.scanner.abnormal_scanner import raw_signal_paths, run_abnormal_scan
from laoma_signal_engine.scanner.signal_models import AbnormalTierDocument


def _minimal_snapshot() -> FuturesLightSnapshotDocument:
    tc = TimeframeContract(
        primary_tf="15m",
        trigger_tf="5m",
        entry_tf="1m",
        background_tfs=["1h", "24h"],
        decision_basis="rolling_15m",
    )
    item = LightSnapshotItem(
        symbol="BTCUSDT",
        base_asset="BTC",
        primary_15m=Primary15mBlock(
            ready=True,
            price_ret=2.0,
            volume_ratio=2.5,
            kline_cvd_state="buy_dominant",
            structure_state="up_impulse",
            range_pos=0.5,
        ),
        trigger_5m=Trigger5mBlock(acceleration_state="accelerating_up"),
        entry_1m=Entry1mBlock(),
        background=BackgroundBlock(background_overheat=False),
        data_quality=DataQualityBlock(kline_1m_ready=True, kline_15m_ready=True),
        reason_codes=["futures_15m_price_up"],
    )
    return FuturesLightSnapshotDocument(
        schema_version="1.6",
        generated_at=to_iso_z(utc_now()),
        source="binance_um_futures",
        universe_generated_at="2026-01-01T00:00:00Z",
        universe_age_sec=0,
        universe_count=1,
        eligible_futures_count=1,
        snapshot_count=1,
        success_count=1,
        failed_count=0,
        skipped_count=0,
        timeframe_contract=tc,
        items=[item],
        errors=[],
    )


def _minimal_universe() -> CandidateUniverseDocument:
    counts = UniverseCounts(
        total_pairs=1,
        futures_count=1,
        spot_count=1,
        both_spot_and_futures=1,
        futures_only=0,
        spot_only=0,
        neither_spot_nor_futures=0,
    )
    pair = UniversePairRow(
        base_asset="BTC",
        display_base_asset="BTC",
        cashtag="$BTC",
        spot_cashtag_symbol="BTCUSDT",
        futures_symbol="BTCUSDT",
        has_spot=True,
        has_um_futures=True,
        eligible_for_signal_engine=True,
        eligible_for_post=True,
        eligible_for_trade_analysis=True,
        rank_futures_volume=10,
        source_tags=["futures_universe"],
    )
    return CandidateUniverseDocument(
        schema_version="1.6",
        generated_at="2026-01-01T10:00:00Z",
        expires_at="2026-01-02T10:00:00Z",
        count=1,
        counts=counts,
        pairs=[pair],
    )


def test_run_abnormal_scan_writes_three_files(tmp_path: Path) -> None:
    snap_p = tmp_path / "DATA" / "market" / "futures_light_snapshot.json"
    snap_p.parent.mkdir(parents=True)
    univ_p = tmp_path / "DATA" / "universe" / "CANDIDATE_UNIVERSE.json"
    univ_p.parent.mkdir(parents=True)

    snap = _minimal_snapshot()
    with open(snap_p, "w", encoding="utf-8", newline="") as fp:
        json.dump(snap.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)

    univ = _minimal_universe()
    with open(univ_p, "w", encoding="utf-8", newline="") as fp:
        json.dump(univ.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)

    code = run_abnormal_scan(
        project_root=tmp_path,
        snapshot_path=snap_p,
        universe_path=univ_p,
        stdout_json=False,
    )
    assert code == 0

    raw_path, watch_path, strong_path = raw_signal_paths(tmp_path)
    assert strong_path.exists()
    with open(strong_path, encoding="utf-8") as fp:
        doc = AbnormalTierDocument.model_validate(json.load(fp))
    assert doc.tier == "strong_candidate"
    assert doc.status == "ok"
    assert doc.input_freshness == "fresh"
    assert doc.stale_warning is False
    assert doc.reason_codes == []
    assert doc.count == 1
    assert doc.signals[0].symbol == "BTCUSDT"
    assert doc.signals[0].state == "strong_candidate"
    assert doc.signals[0].next_stage == "micro_confirm"
    assert doc.signals[0].market_entry_suitability_score > 0
    assert doc.signals[0].market_entry_suitability in ("allowed", "preferred")
    assert doc.signals[0].trade_candidate_rank_score > 0
    assert doc.signals[0].trade_candidate_bucket in ("allowed", "preferred", "observe")
    assert doc.market_entry_counts.allowed + doc.market_entry_counts.preferred == 1
    assert doc.trade_candidate_counts.allowed + doc.trade_candidate_counts.preferred + doc.trade_candidate_counts.observe == 1

    with open(raw_path, encoding="utf-8") as fp:
        raw_doc = AbnormalTierDocument.model_validate(json.load(fp))
    assert raw_doc.count == 0
