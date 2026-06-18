"""STEP4.3A pre-decision refresh tests."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from laoma_signal_engine.factors.models import FactorSnapshotDocument
from laoma_signal_engine.market.decision_refresh import (
    build_decision_refresh_document,
    run_pre_decision_candidate_refresh_safe,
)
from laoma_signal_engine.market.light_snapshot_models import FuturesLightSnapshotDocument
from laoma_signal_engine.micro.assembly.assembler import AssemblyTargetRow, build_document
from laoma_signal_engine.micro.quality.models import CoverageSummary, MicroQualitySnapshot


GEN = "2026-05-24T12:00:00Z"
SYM = "BTCUSDT"


def _quality(symbol: str) -> MicroQualitySnapshot:
    return MicroQualitySnapshot(
        symbol=symbol,
        ready=True,
        reason_codes=(),
        reference_ts_sec=1,
        collect_started_ts_sec=1,
        warmup_age_sec=1,
        cvd_update_age_sec=1.0,
        ofi_update_age_sec=1.0,
        last_update_age_sec=1.0,
        max_lag_sec=0.0,
        coverage={
            "aggTrade": CoverageSummary(
                stream_type="aggTrade",
                window_sec=900,
                expected_seconds=900,
                covered_seconds=900,
            ),
        },
        driver_metrics_summary={"cvd_update_count": 1},
    )


class _Drv:
    def get_latest_cvd(self, symbol: str) -> dict[str, Any]:
        _ = symbol
        return {"cvd": 1.0, "z_cvd": 0.5, "cvd_state": "positive"}

    def get_latest_ofi(self, symbol: str) -> dict[str, Any]:
        _ = symbol
        return {"ofi": 1.0, "z_ofi": 0.5, "ofi_state": "positive"}


def _factor_doc(move_side: str = "up") -> FactorSnapshotDocument:
    micro = build_document(
        targets=[AssemblyTargetRow(symbol=SYM, ofi_levels=1)],
        quality_by_symbol={SYM: _quality(SYM)},
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
    return FactorSnapshotDocument.model_validate(
        {
            "schema_version": "1.6",
            "generated_at": GEN,
            "source": "factor_snapshot",
            "status": "ok",
            "count": 1,
            "items": [
                {
                    "symbol": SYM,
                    "base_asset": "BTC",
                    "source_state": "strong_candidate",
                    "move_side": move_side,
                    "scan_score": 80,
                    "trigger_type": "futures_15m_price_spike",
                    "micro_15m": micro.items[0].micro_15m.model_dump(mode="json"),
                    "micro_quality": micro.items[0].micro_quality.model_dump(mode="json"),
                    "factor_quality": {"ready": True, "reason_codes": [], "input_warnings": []},
                },
            ],
        },
    )


def _light_doc(generated_at: str = GEN, price_ret: float = 1.0, range_pos: float = 0.5) -> FuturesLightSnapshotDocument:
    return FuturesLightSnapshotDocument.model_validate(
        {
            "schema_version": "1.6",
            "generated_at": generated_at,
            "source": "binance_um_futures",
            "universe_generated_at": generated_at,
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
                    "symbol": SYM,
                    "base_asset": "BTC",
                    "last_price": 100.0,
                    "decision_tf": "15m",
                    "primary_15m": {"price_ret": price_ret, "range_pos": range_pos, "ready": True},
                    "trigger_5m": {"price_ret": price_ret / 2, "acceleration_state": "neutral"},
                    "entry_1m": {"atr": 0.8, "last_pullback_low": 99.2},
                    "background": {"quote_volume_24h": 1_000_000.0},
                    "reason_codes": [],
                    "data_quality": {
                        "kline_1m_ready": True,
                        "kline_5m_ready": True,
                        "kline_15m_ready": True,
                    },
                },
            ],
            "errors": [],
        },
    )


def test_refresh_document_marks_direction_and_range_ok(monkeypatch) -> None:
    monkeypatch.setattr(
        "laoma_signal_engine.market.decision_refresh.utc_now",
        lambda: datetime(2026, 5, 24, 12, 1, 0, tzinfo=UTC),
    )
    doc = build_decision_refresh_document(
        factor=_factor_doc("up"),
        light=_light_doc(),
        liquidity_by_symbol={SYM: {"symbol": SYM, "liquidity_ok_for_market_entry": True}},
        liquidity_generated_at=GEN,
        max_refresh_age_sec=180,
    )
    assert doc.status == "ok"
    assert doc.items[0].direction_still_valid is True
    assert doc.items[0].range_room_ok is True
    assert doc.items[0].liquidity_ok is True
    assert doc.items[0].liquidity_age_sec == 60
    assert doc.items[0].reason_codes == []


def test_refresh_document_uses_entry_side_liquidity(monkeypatch) -> None:
    monkeypatch.setattr(
        "laoma_signal_engine.market.decision_refresh.utc_now",
        lambda: datetime(2026, 5, 24, 12, 1, 0, tzinfo=UTC),
    )
    doc = build_decision_refresh_document(
        factor=_factor_doc("up"),
        light=_light_doc(),
        liquidity_by_symbol={
            SYM: {
                "symbol": SYM,
                "liquidity_ok_for_market_entry": False,
                "buy_liquidity_ok_for_market_entry": True,
                "sell_liquidity_ok_for_market_entry": False,
                "buy_reason_codes": [],
                "sell_reason_codes": ["bid_top_depth_too_thin"],
                "reason_codes": ["bid_top_depth_too_thin"],
            },
        },
        liquidity_generated_at=GEN,
        max_refresh_age_sec=180,
    )
    item = doc.items[0]
    assert item.liquidity_ok is True
    assert "liquidity_not_ok" not in item.reason_codes
    assert "bid_top_depth_too_thin" not in item.reason_codes


def test_refresh_document_uses_configurable_range_room(monkeypatch) -> None:
    monkeypatch.setattr(
        "laoma_signal_engine.market.decision_refresh.utc_now",
        lambda: datetime(2026, 5, 24, 12, 1, 0, tzinfo=UTC),
    )
    strict = build_decision_refresh_document(
        factor=_factor_doc("up"),
        light=_light_doc(range_pos=0.9),
        max_refresh_age_sec=180,
        long_max_range_pos=0.82,
    )
    loose = build_decision_refresh_document(
        factor=_factor_doc("up"),
        light=_light_doc(range_pos=0.9),
        max_refresh_age_sec=180,
        long_max_range_pos=0.95,
    )
    assert strict.items[0].range_room_ok is False
    assert "range_room_insufficient_after_refresh" in strict.items[0].reason_codes
    assert loose.items[0].range_room_ok is True
    assert "range_room_insufficient_after_refresh" not in loose.items[0].reason_codes
    assert loose.items[0].range_gate["long_max_range_pos"] == 0.95


def test_refresh_document_marks_stale_and_invalid(monkeypatch) -> None:
    monkeypatch.setattr(
        "laoma_signal_engine.market.decision_refresh.utc_now",
        lambda: datetime(2026, 5, 24, 12, 10, 0, tzinfo=UTC),
    )
    doc = build_decision_refresh_document(
        factor=_factor_doc("up"),
        light=_light_doc(price_ret=-1.0, range_pos=0.95),
        max_refresh_age_sec=180,
    )
    reasons = doc.items[0].reason_codes
    assert doc.status == "stale_input"
    assert "refresh_stale" in reasons
    assert "direction_invalid_after_refresh" in reasons
    assert "range_room_insufficient_after_refresh" in reasons


def test_refresh_cli_contract_without_fetch(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "laoma_signal_engine.market.decision_refresh.utc_now",
        lambda: datetime(2026, 5, 24, 12, 1, 0, tzinfo=UTC),
    )
    factor_p = tmp_path / "factor.json"
    light_p = tmp_path / "light.json"
    out_p = tmp_path / "refresh.json"
    factor_p.write_text(_factor_doc().model_dump_json(), encoding="utf-8")
    light_p.write_text(_light_doc().model_dump_json(), encoding="utf-8")
    code = run_pre_decision_candidate_refresh_safe(
        project_root=tmp_path,
        factor_path=factor_p,
        light_path=light_p,
        output_path=out_p,
        fetch_latest=False,
    )
    assert code == 0
    assert out_p.is_file()
