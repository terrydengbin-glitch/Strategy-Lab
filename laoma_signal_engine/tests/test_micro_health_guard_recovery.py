from __future__ import annotations

from laoma_signal_engine.micro.daemon.health_guard import MicroRuntimeHealthGuard
from laoma_signal_engine.micro.realtime.cvd_ofi_driver import RealtimeCvdOfiMetrics


def test_step316_health_guard_escalates_subscription_missing() -> None:
    guard = MicroRuntimeHealthGuard(resubscribe_after=2, rebuild_after=3, technical_block_after=4)
    sub = {
        "aggTrade": {
            "required": True,
            "active": False,
        }
    }
    metrics = RealtimeCvdOfiMetrics()

    r1 = guard.evaluate_symbol("BTCUSDT", subscription_state=sub, metrics=metrics)
    r2 = guard.evaluate_symbol("BTCUSDT", subscription_state=sub, metrics=metrics)
    r3 = guard.evaluate_symbol("BTCUSDT", subscription_state=sub, metrics=metrics)
    r4 = guard.evaluate_symbol("BTCUSDT", subscription_state=sub, metrics=metrics)

    assert r1.state == "warning"
    assert r2.state == "resubscribe_intent"
    assert r3.state == "runtime_rebuild_intent"
    assert r4.state == "technical_blocked"
    assert "data_quality_blocked" in r4.reason_codes


def test_step316_health_guard_recovers_after_clean_tick() -> None:
    guard = MicroRuntimeHealthGuard()
    bad = {"bookTicker": {"required": True, "active": False}}
    good = {"bookTicker": {"required": True, "active": True}}

    assert guard.evaluate_symbol("ETHUSDT", subscription_state=bad, metrics=RealtimeCvdOfiMetrics()).state == "warning"
    clean_metrics = RealtimeCvdOfiMetrics(processed_bucket_count=1, cvd_update_count=1, ofi_update_count=1)
    assert guard.evaluate_symbol("ETHUSDT", subscription_state=good, metrics=clean_metrics).state == "healthy"

