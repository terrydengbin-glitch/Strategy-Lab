"""STEP3.5 realtime CVD/OFI driver. docs/STEP3.5_任务卡.md."""

from __future__ import annotations

import copy
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Literal

from laoma_signal_engine.micro.adapters.binance_common import normalize_binance_symbol
from laoma_signal_engine.micro.adapters.cvd_adapter import CVDAdapter
from laoma_signal_engine.micro.adapters.ofi_adapter import OFIAdapter
from laoma_signal_engine.micro.bucket.bucket_aggregator import OneSecondBucket
from laoma_signal_engine.micro.calculators.cvd import CVDParams
from laoma_signal_engine.micro.calculators.ofi import OFIParams
from laoma_signal_engine.micro.normalized_models import NormalizedBook, NormalizedTrade

OFILevels = Literal[1, 5]


@dataclass
class RealtimeCvdOfiMetrics:
    cvd_update_count: int = 0
    ofi_update_count: int = 0
    cvd_skipped_no_trade: int = 0
    cvd_skipped_missing_last_price: int = 0
    ofi_skipped_no_book: int = 0
    ofi_skipped_level_mismatch: int = 0
    processed_bucket_count: int = 0
    processed_trade_bucket_count: int = 0
    processed_book_bucket_count: int = 0
    duplicate_bucket_skipped: int = 0
    late_bucket_skipped: int = 0
    adapter_error_count: int = 0


@dataclass
class _SymbolRuntime:
    symbol: str
    ofi_levels: OFILevels
    cvd: CVDAdapter
    ofi: OFIAdapter
    last_processed_bucket_ts_sec: int | None = None
    last_cvd_update_bucket_ts_sec: int | None = None
    last_ofi_update_bucket_ts_sec: int | None = None
    latest_cvd: dict[str, Any] | None = None
    latest_ofi: dict[str, Any] | None = None
    metrics: RealtimeCvdOfiMetrics = field(default_factory=RealtimeCvdOfiMetrics)


class RealtimeCvdOfiDriver:
    """Apply OneSecondBucket streams to CVD/OFI adapters with monotonic cursor per symbol."""

    def __init__(self) -> None:
        self._runtimes: dict[str, _SymbolRuntime] = {}

    def register_symbol(
        self,
        symbol: str,
        ofi_levels: OFILevels,
        *,
        cvd_params: CVDParams | None = None,
        ofi_params: OFIParams | None = None,
    ) -> None:
        sym = normalize_binance_symbol(symbol)
        if ofi_levels not in (1, 5):
            msg = "ofi_levels must be 1 or 5"
            raise ValueError(msg)
        op = ofi_params if ofi_params is not None else OFIParams(levels=ofi_levels)
        if op.levels != ofi_levels:
            msg = "OFIParams.levels must match ofi_levels"
            raise ValueError(msg)
        self._runtimes[sym] = _SymbolRuntime(
            symbol=sym,
            ofi_levels=ofi_levels,
            cvd=CVDAdapter(sym, cvd_params),
            ofi=OFIAdapter(sym, op),
        )

    def apply_buckets(self, symbol: str, buckets: Iterable[OneSecondBucket]) -> None:
        sym = normalize_binance_symbol(symbol)
        rt = self._runtimes.get(sym)
        if rt is None:
            msg = f"symbol not registered: {sym!r}"
            raise ValueError(msg)
        ordered = sorted(buckets, key=lambda b: b.bucket_ts_sec)
        for bucket in ordered:
            bsym = normalize_binance_symbol(bucket.symbol)
            if bsym != sym:
                msg = f"bucket symbol {bsym!r} does not match apply_buckets symbol {sym!r}"
                raise ValueError(msg)
            self._apply_one_bucket(rt, bucket)

    def _apply_one_bucket(self, rt: _SymbolRuntime, bucket: OneSecondBucket) -> None:
        m = rt.metrics
        lp = rt.last_processed_bucket_ts_sec
        ts = bucket.bucket_ts_sec
        if lp is not None:
            if ts == lp:
                m.duplicate_bucket_skipped += 1
                return
            if ts < lp:
                m.late_bucket_skipped += 1
                return

        self._process_bucket_body(rt, bucket)

        rt.last_processed_bucket_ts_sec = ts
        m.processed_bucket_count += 1

    def _safe_cvd_trade(
        self,
        rt: _SymbolRuntime,
        bucket_ts_sec: int,
        nt: NormalizedTrade,
    ) -> None:
        try:
            out = rt.cvd.update_trade(nt)
            rt.latest_cvd = out
            rt.last_cvd_update_bucket_ts_sec = bucket_ts_sec
            rt.metrics.cvd_update_count += 1
        except (TypeError, ValueError):
            rt.metrics.adapter_error_count += 1

    def _safe_ofi_book(
        self,
        rt: _SymbolRuntime,
        bucket_ts_sec: int,
        book: NormalizedBook,
    ) -> None:
        try:
            out = rt.ofi.update_book(book)
            rt.latest_ofi = out
            rt.last_ofi_update_bucket_ts_sec = bucket_ts_sec
            rt.metrics.ofi_update_count += 1
        except (TypeError, ValueError):
            rt.metrics.adapter_error_count += 1

    def _process_bucket_body(self, rt: _SymbolRuntime, bucket: OneSecondBucket) -> None:
        sym = rt.symbol
        trade = bucket.trade
        ts_ms = bucket.bucket_ts_sec * 1000 + 999

        if trade.trade_count > 0:
            rt.metrics.processed_trade_bucket_count += 1
            if trade.last_price is None:
                rt.metrics.cvd_skipped_missing_last_price += 1
            else:
                price = float(trade.last_price)
                if trade.buy_qty > 0.0:
                    nt = NormalizedTrade(
                        symbol=sym,
                        ts_ms=ts_ms,
                        price=price,
                        qty=float(trade.buy_qty),
                        side="buy",
                    )
                    self._safe_cvd_trade(rt, bucket.bucket_ts_sec, nt)
                if trade.sell_qty > 0.0:
                    nt = NormalizedTrade(
                        symbol=sym,
                        ts_ms=ts_ms,
                        price=price,
                        qty=float(trade.sell_qty),
                        side="sell",
                    )
                    self._safe_cvd_trade(rt, bucket.bucket_ts_sec, nt)
        else:
            rt.metrics.cvd_skipped_no_trade += 1

        if rt.ofi_levels == 1:
            book = bucket.last_book_tier1
            if book is None:
                rt.metrics.ofi_skipped_no_book += 1
            elif book.levels != 1:
                rt.metrics.ofi_skipped_level_mismatch += 1
            else:
                rt.metrics.processed_book_bucket_count += 1
                self._safe_ofi_book(rt, bucket.bucket_ts_sec, book)
        else:
            book = bucket.last_book_tier2
            if book is None:
                rt.metrics.ofi_skipped_no_book += 1
            elif book.levels != 5:
                rt.metrics.ofi_skipped_level_mismatch += 1
            else:
                rt.metrics.processed_book_bucket_count += 1
                self._safe_ofi_book(rt, bucket.bucket_ts_sec, book)

    def get_last_cvd_update_bucket_ts_sec(self, symbol: str) -> int | None:
        sym = normalize_binance_symbol(symbol)
        rt = self._runtimes.get(sym)
        return None if rt is None else rt.last_cvd_update_bucket_ts_sec

    def get_last_ofi_update_bucket_ts_sec(self, symbol: str) -> int | None:
        sym = normalize_binance_symbol(symbol)
        rt = self._runtimes.get(sym)
        return None if rt is None else rt.last_ofi_update_bucket_ts_sec

    def get_last_processed_bucket_ts_sec(self, symbol: str) -> int | None:
        sym = normalize_binance_symbol(symbol)
        rt = self._runtimes.get(sym)
        return None if rt is None else rt.last_processed_bucket_ts_sec

    def get_latest_cvd(self, symbol: str) -> dict[str, Any] | None:
        sym = normalize_binance_symbol(symbol)
        rt = self._runtimes.get(sym)
        if rt is None or rt.latest_cvd is None:
            return None
        return dict(rt.latest_cvd)

    def get_latest_ofi(self, symbol: str) -> dict[str, Any] | None:
        sym = normalize_binance_symbol(symbol)
        rt = self._runtimes.get(sym)
        if rt is None or rt.latest_ofi is None:
            return None
        return dict(rt.latest_ofi)

    def get_metrics(self, symbol: str) -> RealtimeCvdOfiMetrics:
        sym = normalize_binance_symbol(symbol)
        rt = self._runtimes.get(sym)
        if rt is None:
            return RealtimeCvdOfiMetrics()
        return copy.copy(rt.metrics)

    def get_all_metrics(self) -> dict[str, RealtimeCvdOfiMetrics]:
        return {s: copy.copy(rt.metrics) for s, rt in self._runtimes.items()}

    def get_global_metrics(self) -> RealtimeCvdOfiMetrics:
        acc = RealtimeCvdOfiMetrics()
        for rt in self._runtimes.values():
            m = rt.metrics
            acc.cvd_update_count += m.cvd_update_count
            acc.ofi_update_count += m.ofi_update_count
            acc.cvd_skipped_no_trade += m.cvd_skipped_no_trade
            acc.cvd_skipped_missing_last_price += m.cvd_skipped_missing_last_price
            acc.ofi_skipped_no_book += m.ofi_skipped_no_book
            acc.ofi_skipped_level_mismatch += m.ofi_skipped_level_mismatch
            acc.processed_bucket_count += m.processed_bucket_count
            acc.processed_trade_bucket_count += m.processed_trade_bucket_count
            acc.processed_book_bucket_count += m.processed_book_bucket_count
            acc.duplicate_bucket_skipped += m.duplicate_bucket_skipped
            acc.late_bucket_skipped += m.late_bucket_skipped
            acc.adapter_error_count += m.adapter_error_count
        return acc
