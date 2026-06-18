from __future__ import annotations

from pathlib import Path

from laoma_signal_engine.micro.assembly import AssemblyTargetRow, build_document
from laoma_signal_engine.micro.factor_frame_store import (
    full_z_window_from_store,
    ingest_micro_factor_frames,
    recent_factor_frames,
    rolling_z_from_store,
)
from laoma_signal_engine.micro.quality.models import CoverageSummary, MicroQualitySnapshot


class _Driver:
    def __init__(self, cvd: float, ofi: float) -> None:
        self._cvd = cvd
        self._ofi = ofi

    def get_latest_cvd(self, symbol: str) -> dict[str, float]:
        return {"cvd": self._cvd, "z_cvd": self._cvd / 10.0, "cvd_state": "ok"}

    def get_latest_ofi(self, symbol: str) -> dict[str, float | str]:
        return {"ofi": self._ofi, "z_ofi": self._ofi / 10.0, "ofi_state": "ok", "ofi_pressure": "ok"}


def _snapshot(bucket: int) -> MicroQualitySnapshot:
    cov = CoverageSummary(
        stream_type="aggTrade",
        window_sec=900,
        expected_seconds=900,
        covered_seconds=900,
    )
    return MicroQualitySnapshot(
        symbol="BTCUSDT",
        ready=True,
        reason_codes=(),
        reference_ts_sec=bucket,
        collect_started_ts_sec=bucket - 900,
        warmup_age_sec=900,
        cvd_update_age_sec=1.0,
        ofi_update_age_sec=1.0,
        last_update_age_sec=1.0,
        max_lag_sec=0.0,
        coverage={"aggTrade": cov, "bookTicker": cov, "partialDepth5": cov},
        driver_metrics_summary={"cvd_update_count": 1, "ofi_update_count": 1},
        last_processed_bucket_ts_sec=bucket,
        last_cvd_update_bucket_ts_sec=bucket,
        last_ofi_update_bucket_ts_sec=bucket,
    )


def _doc(bucket: int, cvd: float, ofi: float):
    return build_document(
        targets=[AssemblyTargetRow(symbol="BTCUSDT", ofi_levels=5, tier="tier2_active_strong")],
        quality_by_symbol={"BTCUSDT": _snapshot(bucket)},
        fast_quality_by_symbol={"BTCUSDT": _snapshot(bucket)},
        driver=_Driver(cvd, ofi),
        generated_at="2026-05-31T00:00:00Z",
        status="ok",
        target_generated_at="2026-05-31T00:00:00Z",
        target_age_sec=1,
        target_status="fresh",
        dropped_events_trade=0,
        dropped_events_book=0,
        dropped_events_depth=0,
    )


def test_step339_persists_micro_factor_frames(tmp_path: Path) -> None:
    db = tmp_path / "audit.db"
    result = ingest_micro_factor_frames(_doc(100, 1.0, 2.0), db_path=db)
    assert result["inserted_or_replaced"] == 2

    rows = recent_factor_frames(
        db_path=db,
        strategy_line="micro_full",
        symbol="BTCUSDT",
        limit=10,
    )
    assert len(rows) == 1
    assert rows[0]["bucket_ts_sec"] == 100
    assert rows[0]["cvd_available"] == 1
    assert rows[0]["ofi_available"] == 1


def test_step340_rolling_z_from_persistent_store(tmp_path: Path) -> None:
    db = tmp_path / "audit.db"
    for idx, cvd in enumerate([1.0, 2.0, 3.0], start=100):
        ingest_micro_factor_frames(_doc(idx, cvd, cvd * 2), db_path=db)

    z = rolling_z_from_store(
        db_path=db,
        strategy_line="micro_full",
        symbol="BTCUSDT",
        field="cvd",
        window=3,
    )
    assert z["available"] is True
    assert z["series_length"] == 3
    assert z["z"] is not None

    missing = rolling_z_from_store(
        db_path=db,
        strategy_line="micro_full",
        symbol="ETHUSDT",
        field="cvd",
        window=3,
    )
    assert missing["available"] is False
    assert missing["missing_reason"] == "insufficient_history"


def test_step343_full_z_window_reads_persisted_buckets(tmp_path: Path) -> None:
    db = tmp_path / "audit.db"
    for idx, cvd in enumerate([1.0, 2.0, 4.0], start=100):
        ingest_micro_factor_frames(_doc(idx, cvd, cvd * 2), db_path=db)

    window = full_z_window_from_store(
        db_path=db,
        strategy_line="micro_full",
        symbol="BTCUSDT",
        now_bucket_ts_sec=102,
        window_sec=3,
        min_valid_bucket_ratio=1.0,
        max_gap_sec=1,
    )

    assert window["full_z_status"] == "available"
    assert window["expected_bucket_count"] == 3
    assert window["actual_bucket_count"] == 3
    assert window["valid_bucket_ratio"] == 1.0
    assert window["z_cvd_available"] is True
    assert window["z_ofi_available"] is True


def test_step343_full_z_window_reports_gap_reason(tmp_path: Path) -> None:
    db = tmp_path / "audit.db"
    ingest_micro_factor_frames(_doc(100, 1.0, 2.0), db_path=db)
    ingest_micro_factor_frames(_doc(103, 4.0, 8.0), db_path=db)

    window = full_z_window_from_store(
        db_path=db,
        strategy_line="micro_full",
        symbol="BTCUSDT",
        now_bucket_ts_sec=103,
        window_sec=4,
        min_valid_bucket_ratio=0.5,
        max_gap_sec=1,
    )

    assert window["full_z_status"] == "missing"
    assert window["full_z_missing_reason"] == "bucket_gap"
    assert window["max_gap_sec"] == 3
