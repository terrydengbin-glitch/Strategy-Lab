"""STEP3.6 MicroQualityGate tests Q1-Q15. docs/STEP3.6_任务卡.md."""

from __future__ import annotations

from pathlib import Path

from laoma_signal_engine.micro.bucket.bucket_aggregator import (
    CoverageSnapshot,
    OneSecondBucket,
    TradeBucketStats,
)
from laoma_signal_engine.micro.normalized_models import NormalizedBook
from laoma_signal_engine.micro.quality.models import (
    MicroQualityConfig,
    SymbolQualityInput,
    WSQualitySignal,
)
from laoma_signal_engine.micro.quality.quality_gate import MicroQualityGate
from laoma_signal_engine.micro.realtime.cvd_ofi_driver import RealtimeCvdOfiDriver, RealtimeCvdOfiMetrics


SYM = "BTCUSDT"
T_REF = 10_000


class FakeCoverage:
    def __init__(self, ratios: dict[str, float]) -> None:
        self.ratios = ratios

    def get_coverage(
        self,
        symbol: str,
        stream_type: object,
        end_ts_sec: int,
        window_sec: int,
    ) -> CoverageSnapshot:
        key = str(stream_type)
        r = self.ratios.get(key, 1.0)
        cov = int(float(window_sec) * r)
        if cov > window_sec:
            cov = window_sec
        return CoverageSnapshot(
            expected_seconds=window_sec,
            covered_seconds=cov,
            gap_count=0,
            max_gap_sec=0,
        )


class MockDriver:
    def __init__(self) -> None:
        self.cvd_ts: int | None = 9_999
        self.ofi_ts: int | None = 9_999
        self.proc_ts: int | None = 9_999
        self.metrics = RealtimeCvdOfiMetrics()

    def get_metrics(self, symbol: str) -> RealtimeCvdOfiMetrics:
        return self.metrics

    def get_last_cvd_update_bucket_ts_sec(self, symbol: str) -> int | None:
        return self.cvd_ts

    def get_last_ofi_update_bucket_ts_sec(self, symbol: str) -> int | None:
        return self.ofi_ts

    def get_last_processed_bucket_ts_sec(self, symbol: str) -> int | None:
        return self.proc_ts


def _cfg(**kwargs: object) -> MicroQualityConfig:
    base: dict[str, object] = {
        "window_sec": 900,
        "min_ready_seconds": 900,
        "aggtrade_coverage_min": 0.60,
        "bookticker_coverage_min": 0.70,
        "depth5_coverage_min": 0.70,
        "max_stale_sec": 10,
        "max_lag_sec": 3,
        "event_queue_overflow_hard_fail": True,
        "adapter_error_hard_fail": True,
    }
    base.update(kwargs)
    return MicroQualityConfig(**base)  # type: ignore[arg-type]


def test_q1_ideal_ready() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp)
    assert snap.ready is True
    assert snap.reason_codes == ()


def test_q2_event_queue_overflow_recent() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp, WSQualitySignal(event_queue_overflow_recent=True))
    assert snap.ready is False
    assert "event_queue_overflow" in snap.reason_codes


def test_step338_ofi_backpressure_critical_blocks_ready() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(event_queue_overflow_hard_fail=False), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp, WSQualitySignal(ofi_backpressure_state="critical"))
    assert snap.ready is False
    assert "ofi_backpressure_critical" in snap.reason_codes
    assert snap.driver_metrics_summary["ofi_backpressure_state_code"] == 2


def test_step338_ofi_backpressure_degraded_is_context_by_default() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp, WSQualitySignal(ofi_backpressure_state="degraded"))
    assert snap.ready is True
    assert "ofi_backpressure_degraded" not in snap.reason_codes
    assert snap.driver_metrics_summary["ofi_backpressure_state_code"] == 1


def test_q3_aggtrade_weak() -> None:
    cov = FakeCoverage({"aggTrade": 0.50, "bookTicker": 0.71})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp)
    assert snap.ready is False
    assert "coverage_aggtrade_weak" in snap.reason_codes
    assert snap.reason_root_causes["coverage_aggtrade_weak"] == "stream_coverage_gap"
    assert snap.data_quality_root_cause_class == "stream_coverage_gap"


def test_q4_depth5_weak_tier2() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "partialDepth5": 0.50, "bookTicker": 0.99})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=5, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp)
    assert "coverage_depth5_weak" in snap.reason_codes


def test_q5_cvd_never_updated() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    drv.cvd_ts = None
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp)
    assert "cvd_never_updated" in snap.reason_codes
    assert snap.max_lag_sec is None
    assert snap.reason_root_causes["cvd_never_updated"] == "cvd_commit_gap"


def test_q6_cvd_stale() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    drv.cvd_ts = T_REF - 20
    drv.ofi_ts = T_REF - 1
    drv.proc_ts = T_REF - 1
    g = MicroQualityGate(_cfg(max_stale_sec=10), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp)
    assert "cvd_stale" in snap.reason_codes


def test_q7_ofi_cvd_lag_high() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    drv.cvd_ts = T_REF - 13
    drv.ofi_ts = T_REF - 1
    drv.proc_ts = T_REF - 1
    g = MicroQualityGate(_cfg(max_stale_sec=100, max_lag_sec=3), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp)
    assert "ofi_cvd_lag_high" in snap.reason_codes
    assert snap.last_cvd_update_bucket_ts_sec == T_REF - 13
    assert snap.last_ofi_update_bucket_ts_sec == T_REF - 1
    assert snap.last_processed_bucket_ts_sec == T_REF - 1
    assert snap.ofi_cvd_lag_side == "cvd_old"
    assert snap.reference_bucket_ts_sec == T_REF - 1
    assert snap.cvd_age_bucket_sec == 12
    assert snap.ofi_age_bucket_sec == 0
    assert snap.ofi_cvd_lag_bucket_sec == 12
    assert snap.driver_metrics_summary["cvd_update_count"] == 0
    assert snap.driver_metrics_summary["ofi_update_count"] == 0
    assert snap.reason_root_causes["ofi_cvd_lag_high"] == "bucket_lag_cvd_old"
    assert snap.data_quality_root_cause_class == "bucket_alignment_gap"


def test_q8_warmup_not_met() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(
        symbol=SYM,
        ofi_levels=1,
        collect_started_ts_sec=T_REF - 100,
    )
    snap = g.evaluate(T_REF, inp)
    assert "warmup_not_met" in snap.reason_codes


def test_q9_multi_reasons_sorted() -> None:
    cov = FakeCoverage({"aggTrade": 0.50, "bookTicker": 0.71})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(
        symbol=SYM,
        ofi_levels=1,
        collect_started_ts_sec=T_REF - 100,
    )
    snap = g.evaluate(T_REF, inp)
    assert snap.reason_codes.index("coverage_aggtrade_weak") < snap.reason_codes.index("warmup_not_met")


def test_q10_reference_ts_deterministic() -> None:
    test_q1_ideal_ready()


def test_q11_late_duplicate_in_summary_only() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    drv.metrics.late_bucket_skipped = 42
    drv.metrics.duplicate_bucket_skipped = 7
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp)
    assert snap.ready is True
    assert snap.driver_metrics_summary["late_bucket_skipped"] == 42
    assert snap.driver_metrics_summary["duplicate_bucket_skipped"] == 7


def test_q12_forbidden_substrings_in_quality_sources() -> None:
    root = Path(__file__).resolve().parents[1]
    for rel in (
        "micro/quality/models.py",
        "micro/quality/quality_gate.py",
        "micro/quality/__init__.py",
    ):
        src = (root / rel).read_text(encoding="utf-8")
        for bad in ("FusionEngine", "websocket", "httpx", "latest_micro_features"):
            assert bad not in src, rel


def test_q13_tier2_bookticker_weak_no_reason() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "partialDepth5": 0.71, "bookTicker": 0.10})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=5, collect_started_ts_sec=T_REF - 1_000)
    snap = g.evaluate(T_REF, inp)
    assert snap.ready is True
    assert "coverage_bookticker_weak" not in snap.reason_codes
    assert "bookTicker" in snap.coverage


def test_q14_overflow_only_when_recent() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    snap_a = g.evaluate(T_REF, inp, WSQualitySignal(event_queue_overflow_recent=False))
    assert "event_queue_overflow" not in snap_a.reason_codes
    snap_b = g.evaluate(T_REF, inp, WSQualitySignal(event_queue_overflow_recent=True))
    assert "event_queue_overflow" in snap_b.reason_codes


def test_q15_strict_reason_order() -> None:
    cov = FakeCoverage({"aggTrade": 0.50, "bookTicker": 0.71})
    drv = MockDriver()
    drv.cvd_ts = T_REF - 12
    drv.ofi_ts = T_REF - 9
    drv.proc_ts = T_REF - 1
    g = MicroQualityGate(_cfg(max_stale_sec=10), cov, drv)
    inp = SymbolQualityInput(
        symbol=SYM,
        ofi_levels=1,
        collect_started_ts_sec=T_REF - 100,
    )
    snap = g.evaluate(T_REF, inp)
    expected = (
        "coverage_aggtrade_weak",
        "cvd_stale",
        "warmup_not_met",
    )
    assert snap.reason_codes == expected


def test_adapter_error_delta_triggers_once_per_increment() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    g = MicroQualityGate(_cfg(adapter_error_hard_fail=True), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)
    s0 = g.evaluate(T_REF, inp)
    assert "adapter_error_seen" not in s0.reason_codes
    drv.metrics.adapter_error_count += 1
    s1 = g.evaluate(T_REF, inp)
    assert "adapter_error_seen" in s1.reason_codes
    s2 = g.evaluate(T_REF, inp)
    assert "adapter_error_seen" not in s2.reason_codes


def test_realtime_driver_getters_match_latest_after_apply() -> None:
    d = RealtimeCvdOfiDriver()
    d.register_symbol(SYM, 1)
    tr = TradeBucketStats(1.0, 0.0, 100.0, 0.0, 1, 100.0)
    b1 = NormalizedBook(symbol=SYM, ts_ms=1_000, bids=[(100.0, 1.0)], asks=[(100.5, 1.0)], levels=1)
    bucket = OneSecondBucket(
        symbol=SYM,
        bucket_ts_sec=1,
        trade=tr,
        last_book_tier1=b1,
        last_book_tier2=None,
    )
    d.apply_buckets(SYM, [bucket])
    assert d.get_latest_cvd(SYM) is not None
    assert d.get_last_cvd_update_bucket_ts_sec(SYM) == 1
    assert d.get_last_ofi_update_bucket_ts_sec(SYM) == 1
    assert d.get_last_processed_bucket_ts_sec(SYM) == 1


def test_step311_quality_snapshot_exposes_lag_old_side_for_ofi_old() -> None:
    cov = FakeCoverage({"aggTrade": 0.65, "bookTicker": 0.71})
    drv = MockDriver()
    drv.cvd_ts = T_REF - 1
    drv.ofi_ts = T_REF - 12
    drv.proc_ts = T_REF - 1
    drv.metrics.cvd_update_count = 5
    drv.metrics.ofi_update_count = 2
    g = MicroQualityGate(_cfg(max_stale_sec=100, max_lag_sec=3), cov, drv)
    inp = SymbolQualityInput(symbol=SYM, ofi_levels=1, collect_started_ts_sec=T_REF - 1_000)

    snap = g.evaluate(T_REF, inp)

    assert "ofi_cvd_lag_high" in snap.reason_codes
    assert snap.ofi_cvd_lag_side == "ofi_old"
    assert snap.reference_bucket_ts_sec == T_REF - 1
    assert snap.cvd_age_bucket_sec == 0
    assert snap.ofi_age_bucket_sec == 11
    assert snap.driver_metrics_summary["cvd_update_count"] == 5
    assert snap.driver_metrics_summary["ofi_update_count"] == 2
