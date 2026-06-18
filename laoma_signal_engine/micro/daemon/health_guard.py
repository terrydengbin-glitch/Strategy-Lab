"""STEP3.16 micro runtime health guard.

The guard is diagnostic and recovery-intent oriented. It does not change
micro ready/confirmed semantics; it classifies runtime data-chain symptoms so
the daemon/UI can distinguish transient warning, resubscribe intent, runtime
rebuild intent, and terminal technical block.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from laoma_signal_engine.micro.realtime.cvd_ofi_driver import RealtimeCvdOfiMetrics


@dataclass(frozen=True)
class MicroHealthGuardResult:
    state: str
    anomaly_count: int
    action: str
    reason_codes: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "anomaly_count": self.anomaly_count,
            "action": self.action,
            "reason_codes": list(self.reason_codes),
        }


@dataclass
class _GuardCursor:
    anomaly_count: int = 0
    last_processed_bucket_count: int | None = None
    last_cvd_update_count: int | None = None
    last_ofi_update_count: int | None = None


@dataclass
class MicroRuntimeHealthGuard:
    resubscribe_after: int = 2
    rebuild_after: int = 3
    technical_block_after: int = 4
    _cursors: dict[str, _GuardCursor] = field(default_factory=dict)

    def evaluate_symbol(
        self,
        symbol: str,
        *,
        subscription_state: dict[str, dict[str, Any]],
        metrics: RealtimeCvdOfiMetrics,
    ) -> MicroHealthGuardResult:
        sym = symbol.strip().upper()
        cursor = self._cursors.setdefault(sym, _GuardCursor())
        reasons: list[str] = []

        for logical, state in sorted(subscription_state.items()):
            if state.get("required") is True and state.get("active") is False:
                reasons.append(f"health_subscription_missing_{logical}")

        if cursor.last_processed_bucket_count is not None and metrics.processed_bucket_count <= cursor.last_processed_bucket_count:
            reasons.append("health_processed_bucket_not_advancing")
        if cursor.last_cvd_update_count is not None and metrics.cvd_update_count <= cursor.last_cvd_update_count:
            if metrics.cvd_skipped_no_trade <= 0:
                reasons.append("health_cvd_not_advancing")
        if cursor.last_ofi_update_count is not None and metrics.ofi_update_count <= cursor.last_ofi_update_count:
            if metrics.ofi_skipped_no_book <= 0:
                reasons.append("health_ofi_not_advancing")

        cursor.last_processed_bucket_count = metrics.processed_bucket_count
        cursor.last_cvd_update_count = metrics.cvd_update_count
        cursor.last_ofi_update_count = metrics.ofi_update_count

        if reasons:
            cursor.anomaly_count += 1
        else:
            cursor.anomaly_count = 0

        if cursor.anomaly_count >= self.technical_block_after:
            return MicroHealthGuardResult(
                state="technical_blocked",
                anomaly_count=cursor.anomaly_count,
                action="stop_consumption_until_recovered",
                reason_codes=tuple(dict.fromkeys([*reasons, "data_quality_blocked", "technical_not_ready"])),
            )
        if cursor.anomaly_count >= self.rebuild_after:
            return MicroHealthGuardResult(
                state="runtime_rebuild_intent",
                anomaly_count=cursor.anomaly_count,
                action="rebuild_symbol_runtime",
                reason_codes=tuple(dict.fromkeys(reasons)),
            )
        if cursor.anomaly_count >= self.resubscribe_after:
            return MicroHealthGuardResult(
                state="resubscribe_intent",
                anomaly_count=cursor.anomaly_count,
                action="resubscribe_symbol_streams",
                reason_codes=tuple(dict.fromkeys(reasons)),
            )
        if cursor.anomaly_count > 0:
            return MicroHealthGuardResult(
                state="warning",
                anomaly_count=cursor.anomaly_count,
                action="record_warning",
                reason_codes=tuple(dict.fromkeys(reasons)),
            )
        return MicroHealthGuardResult(
            state="healthy",
            anomaly_count=0,
            action="none",
            reason_codes=(),
        )

