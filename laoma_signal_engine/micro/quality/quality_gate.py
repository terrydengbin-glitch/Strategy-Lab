"""STEP3.6 MicroQualityGate. docs/STEP3.6_任务卡.md."""

from __future__ import annotations

from typing import Protocol

from laoma_signal_engine.micro.adapters.binance_common import normalize_binance_symbol
from laoma_signal_engine.micro.bucket.bucket_aggregator import CoverageSnapshot, CoverageStreamType
from laoma_signal_engine.micro.quality.models import (
    CoverageSummary,
    MicroQualityConfig,
    MicroQualitySnapshot,
    SymbolQualityInput,
    WSQualitySignal,
    sort_reason_codes,
)
from laoma_signal_engine.micro.realtime.cvd_ofi_driver import RealtimeCvdOfiMetrics


class CoverageProvider(Protocol):
    def get_coverage(
        self,
        symbol: str,
        stream_type: CoverageStreamType,
        end_ts_sec: int,
        window_sec: int,
    ) -> CoverageSnapshot: ...


class RealtimeDriverLike(Protocol):
    def get_metrics(self, symbol: str) -> RealtimeCvdOfiMetrics:
        ...

    def get_last_cvd_update_bucket_ts_sec(self, symbol: str) -> int | None:
        ...

    def get_last_ofi_update_bucket_ts_sec(self, symbol: str) -> int | None:
        ...

    def get_last_processed_bucket_ts_sec(self, symbol: str) -> int | None:
        ...


class MicroQualityGate:
    """Per-symbol micro_quality.ready evaluation (STEP3.6)."""

    def __init__(
        self,
        config: MicroQualityConfig,
        coverage_provider: CoverageProvider,
        driver: RealtimeDriverLike,
    ) -> None:
        self._cfg = config
        self._coverage = coverage_provider
        self._driver = driver
        self._prev_adapter_error_count: dict[str, int | None] = {}

    def evaluate(
        self,
        reference_ts_sec: int,
        symbol_input: SymbolQualityInput,
        ws_signal: WSQualitySignal | None = None,
    ) -> MicroQualitySnapshot:
        sym = normalize_binance_symbol(symbol_input.symbol)
        ws = ws_signal if ws_signal is not None else WSQualitySignal()
        cfg = self._cfg
        w = cfg.window_sec
        t_ref = reference_ts_sec
        min_ready = (
            symbol_input.min_ready_seconds
            if symbol_input.min_ready_seconds is not None
            else cfg.min_ready_seconds
        )
        warmup_age_sec = t_ref - symbol_input.collect_started_ts_sec

        reasons: set[str] = set()

        if cfg.event_queue_overflow_hard_fail and ws.event_queue_overflow_recent:
            reasons.add("event_queue_overflow")
        if ws.ofi_backpressure_state == "critical":
            reasons.add("ofi_backpressure_critical")
        elif cfg.ofi_backpressure_degraded_hard_fail and ws.ofi_backpressure_state == "degraded":
            reasons.add("ofi_backpressure_degraded")

        m = self._driver.get_metrics(sym)
        prev_err = self._prev_adapter_error_count.get(sym)
        if prev_err is None:
            self._prev_adapter_error_count[sym] = m.adapter_error_count
            adapter_delta = 0
        else:
            adapter_delta = max(0, m.adapter_error_count - prev_err)
            self._prev_adapter_error_count[sym] = m.adapter_error_count

        if cfg.adapter_error_hard_fail and adapter_delta > 0:
            reasons.add("adapter_error_seen")

        coverage_map: dict[str, CoverageSummary] = {}

        def add_cov(stream: CoverageStreamType, key: str) -> CoverageSummary:
            snap = self._coverage.get_coverage(sym, stream, t_ref, w)
            summary = CoverageSummary(
                stream_type=key,
                window_sec=w,
                expected_seconds=snap.expected_seconds,
                covered_seconds=snap.covered_seconds,
            )
            coverage_map[key] = summary
            return summary

        agg = add_cov("aggTrade", "aggTrade")
        if agg.coverage_ratio < cfg.aggtrade_coverage_min:
            reasons.add("coverage_aggtrade_weak")

        ofi_lv = symbol_input.ofi_levels
        if ofi_lv == 1:
            bt = add_cov("bookTicker", "bookTicker")
            if bt.coverage_ratio < cfg.bookticker_coverage_min:
                reasons.add("coverage_bookticker_weak")
        else:
            d5 = add_cov("partialDepth5", "partialDepth5")
            if d5.coverage_ratio < cfg.depth5_coverage_min:
                reasons.add("coverage_depth5_weak")
            bt2 = add_cov("bookTicker", "bookTicker")
            if bt2.coverage_ratio < cfg.bookticker_coverage_min:
                pass

        cvd_ts = self._driver.get_last_cvd_update_bucket_ts_sec(sym)
        ofi_ts = self._driver.get_last_ofi_update_bucket_ts_sec(sym)
        processed_ts = self._driver.get_last_processed_bucket_ts_sec(sym)

        cvd_age: float | None = None
        ofi_age: float | None = None
        if cvd_ts is None:
            reasons.add("cvd_never_updated")
        else:
            cvd_age = float(t_ref - cvd_ts)
            if cvd_age > float(cfg.max_stale_sec):
                reasons.add("cvd_stale")

        if ofi_ts is None:
            reasons.add("ofi_never_updated")
        else:
            ofi_age = float(t_ref - ofi_ts)
            if ofi_age > float(cfg.max_stale_sec):
                reasons.add("ofi_stale")

        max_lag: float | None = None
        lag_side: str | None = None
        reference_bucket_ts = processed_ts if processed_ts is not None else t_ref
        cvd_age_bucket: int | None = None
        ofi_age_bucket: int | None = None
        lag_bucket: int | None = None
        if cvd_ts is not None:
            cvd_age_bucket = max(0, int(reference_bucket_ts - cvd_ts))
        if ofi_ts is not None:
            ofi_age_bucket = max(0, int(reference_bucket_ts - ofi_ts))
        if cvd_ts is not None and ofi_ts is not None:
            max_lag = float(abs(cvd_ts - ofi_ts))
            lag_bucket = int(abs(cvd_ts - ofi_ts))
            if cvd_ts < ofi_ts:
                lag_side = "cvd_old"
            elif ofi_ts < cvd_ts:
                lag_side = "ofi_old"
            else:
                lag_side = "aligned"
            if max_lag > float(cfg.max_lag_sec):
                reasons.add("ofi_cvd_lag_high")

        last_age: float | None = None
        ages_defined = [a for a in (cvd_age, ofi_age) if a is not None]
        if ages_defined:
            last_age = max(ages_defined)

        if warmup_age_sec < min_ready:
            reasons.add("warmup_not_met")

        ready = len(reasons) == 0
        ordered = sort_reason_codes(reasons)

        drv_summary: dict[str, int] = {
            "cvd_update_count": m.cvd_update_count,
            "ofi_update_count": m.ofi_update_count,
            "cvd_skipped_no_trade": m.cvd_skipped_no_trade,
            "cvd_skipped_missing_last_price": m.cvd_skipped_missing_last_price,
            "ofi_skipped_no_book": m.ofi_skipped_no_book,
            "ofi_skipped_level_mismatch": m.ofi_skipped_level_mismatch,
            "processed_bucket_count": m.processed_bucket_count,
            "processed_trade_bucket_count": m.processed_trade_bucket_count,
            "processed_book_bucket_count": m.processed_book_bucket_count,
            "late_bucket_skipped": m.late_bucket_skipped,
            "duplicate_bucket_skipped": m.duplicate_bucket_skipped,
            "adapter_error_count": m.adapter_error_count,
            "dropped_trade_delta": ws.dropped_trade_delta,
            "dropped_book_delta": ws.dropped_book_delta,
            "dropped_depth_delta": ws.dropped_depth_delta,
            "ofi_backpressure_state_code": {"ok": 0, "degraded": 1, "critical": 2}.get(ws.ofi_backpressure_state, 0),
        }

        if not ready and len(ordered) == 0:
            msg = "micro_quality: ready=false but empty reason_codes (logic error)"
            raise RuntimeError(msg)

        def root_cause_for(reason: str) -> str:
            if reason == "warmup_not_met":
                return "expected_warmup"
            if reason == "coverage_aggtrade_weak":
                return "market_low_activity" if agg.coverage_ratio < 0.15 else "stream_coverage_gap"
            if reason == "coverage_bookticker_weak":
                bt_cov = coverage_map.get("bookTicker")
                ratio = bt_cov.coverage_ratio if bt_cov else 0.0
                return "market_low_activity" if ratio < 0.15 else "stream_coverage_gap"
            if reason == "coverage_depth5_weak":
                d5_cov = coverage_map.get("partialDepth5")
                ratio = d5_cov.coverage_ratio if d5_cov else 0.0
                return "market_low_activity" if ratio < 0.15 else "stream_coverage_gap"
            if reason == "cvd_never_updated":
                if agg.coverage_ratio < 0.15 or m.cvd_skipped_no_trade > 0:
                    return "market_low_activity"
                return "cvd_commit_gap"
            if reason == "ofi_never_updated":
                if m.ofi_skipped_no_book > 0:
                    return "book_commit_gap"
                return "ofi_commit_gap"
            if reason == "cvd_stale":
                return "stream_drop" if ws.dropped_trade_delta > 0 else "cvd_commit_lag"
            if reason == "ofi_stale":
                return "stream_drop" if (ws.dropped_book_delta > 0 or ws.dropped_depth_delta > 0) else "ofi_commit_lag"
            if reason == "ofi_cvd_lag_high":
                return f"bucket_lag_{lag_side or 'unknown'}"
            if reason in {"event_queue_overflow", "adapter_error_seen", "ofi_backpressure_critical", "ofi_backpressure_degraded"}:
                return "runtime_backpressure"
            return "technical_gap"

        reason_root_causes = {reason: root_cause_for(reason) for reason in ordered}
        if not reason_root_causes:
            root_cause_class = "ok"
        elif all(value in {"expected_warmup", "market_low_activity"} for value in reason_root_causes.values()):
            root_cause_class = "market_or_warmup"
        elif any(value in {"stream_coverage_gap", "stream_drop", "runtime_backpressure"} for value in reason_root_causes.values()):
            root_cause_class = "stream_coverage_gap"
        elif any(value.startswith("bucket_lag_") for value in reason_root_causes.values()):
            root_cause_class = "bucket_alignment_gap"
        else:
            root_cause_class = "technical_gap"

        return MicroQualitySnapshot(
            symbol=sym,
            ready=ready,
            reason_codes=ordered,
            reference_ts_sec=t_ref,
            collect_started_ts_sec=symbol_input.collect_started_ts_sec,
            warmup_age_sec=warmup_age_sec,
            cvd_update_age_sec=cvd_age,
            ofi_update_age_sec=ofi_age,
            last_update_age_sec=last_age,
            max_lag_sec=max_lag,
            last_cvd_update_bucket_ts_sec=cvd_ts,
            last_ofi_update_bucket_ts_sec=ofi_ts,
            last_processed_bucket_ts_sec=processed_ts,
            ofi_cvd_lag_side=lag_side,  # type: ignore[arg-type]
            reference_bucket_ts_sec=reference_bucket_ts,
            cvd_age_bucket_sec=cvd_age_bucket,
            ofi_age_bucket_sec=ofi_age_bucket,
            ofi_cvd_lag_bucket_sec=lag_bucket,
            data_quality_root_cause_class=root_cause_class,
            reason_root_causes=reason_root_causes,
            coverage=coverage_map,
            driver_metrics_summary=drv_summary,
        )

