"""STEP10.7 persistent micro daemon state tests."""

from __future__ import annotations

from datetime import UTC, datetime

from laoma_signal_engine.micro.daemon.state_writer import build_micro_daemon_state_document
from laoma_signal_engine.micro.quality.models import CoverageSummary, MicroQualitySnapshot
from laoma_signal_engine.micro.target_intent_models import SubscribeIntent

GEN = "2026-05-25T00:00:00Z"


def _quality(symbol: str, *, ready: bool, warmup_age_sec: int) -> MicroQualitySnapshot:
    return MicroQualitySnapshot(
        symbol=symbol,
        ready=ready,
        reason_codes=() if ready else ("warmup_not_met",),
        reference_ts_sec=1,
        collect_started_ts_sec=1,
        warmup_age_sec=warmup_age_sec,
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
        driver_metrics_summary={},
    )


def test_micro_state_records_full_eta_and_consumer_safety() -> None:
    intent = SubscribeIntent(
        symbol="BTCUSDT",
        symbol_safe_id="BTCUSDT",
        tier_key="tier1_warm_watch",
        source_state="strong_candidate",
        streams=("aggTrade", "bookTicker"),
        priority=100,
        scan_score=80,
        move_side="up",
        trigger_type="test",
        min_collect_seconds=900,
        ttl_seconds=1800,
        lifecycle="warming",
        first_seen_at="2026-05-24T23:58:00Z",
        last_target_seen_at=GEN,
    )
    doc = build_micro_daemon_state_document(
        generated_at=GEN,
        now_dt=datetime(2026, 5, 25, 0, 0, 0, tzinfo=UTC),
        target_generated_at=GEN,
        target_age_sec=0,
        intents=[intent],
        full_quality_by_symbol={"BTCUSDT": _quality("BTCUSDT", ready=False, warmup_age_sec=120)},
        fast_quality_by_symbol={"BTCUSDT": _quality("BTCUSDT", ready=True, warmup_age_sec=120)},
    )
    s = doc.symbols[0]
    assert doc.state_ready_for_consumers is True
    assert s.fast_ready is True
    assert s.full_ready is False
    assert s.full_ready_eta_sec == 780


def test_step312_micro_state_records_subscription_state() -> None:
    intent = SubscribeIntent(
        symbol="ETHUSDT",
        symbol_safe_id="ETHUSDT",
        tier_key="tier1_warm_watch",
        source_state="strong_candidate",
        streams=("aggTrade", "bookTicker"),
        priority=100,
        scan_score=80,
        move_side="up",
        trigger_type="test",
        min_collect_seconds=300,
        ttl_seconds=1800,
        lifecycle="warming",
        first_seen_at="2026-05-24T23:58:00Z",
        last_target_seen_at=GEN,
    )
    doc = build_micro_daemon_state_document(
        generated_at=GEN,
        now_dt=datetime(2026, 5, 25, 0, 0, 0, tzinfo=UTC),
        target_generated_at=GEN,
        target_age_sec=0,
        intents=[intent],
        full_quality_by_symbol={"ETHUSDT": _quality("ETHUSDT", ready=False, warmup_age_sec=120)},
        fast_quality_by_symbol={"ETHUSDT": _quality("ETHUSDT", ready=True, warmup_age_sec=120)},
        subscription_state_by_symbol={
            "ETHUSDT": {
                "aggTrade": {
                    "required": True,
                    "desired": True,
                    "active": False,
                    "last_event_ts_sec": None,
                    "last_ack_ts_sec": None,
                    "missing_reason": "subscription_missing_aggTrade",
                },
                "partialDepth5": {
                    "required": False,
                    "desired": False,
                    "active": False,
                    "last_event_ts_sec": None,
                    "last_ack_ts_sec": None,
                    "missing_reason": "not_required_for_tier",
                },
            },
        },
        health_guard_state_by_symbol={
            "ETHUSDT": {
                "state": "resubscribe_intent",
                "anomaly_count": 2,
                "action": "resubscribe_symbol_streams",
                "reason_codes": ["health_subscription_missing_aggTrade"],
            }
        },
    )
    state = doc.symbols[0].subscription_state
    assert state["aggTrade"].required is True
    assert state["aggTrade"].active is False
    assert state["aggTrade"].missing_reason == "subscription_missing_aggTrade"
    assert state["partialDepth5"].required is False
    assert state["partialDepth5"].missing_reason == "not_required_for_tier"
    assert doc.symbols[0].health_guard_state["state"] == "resubscribe_intent"
