"""STEP3.2 Target Manager tests TM1-TM14. docs/STEP3.2_任务卡.md."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest

from laoma_signal_engine.core.time_utils import parse_iso_z
from laoma_signal_engine.micro.target_intent_models import TargetManagerSettings, build_symbol_safe_id
from laoma_signal_engine.micro.target_manager import MicroTargetManager


def _entry(symbol: str, **kwargs: object) -> dict[str, object]:
    base: dict[str, object] = {
        "symbol": symbol,
        "base_asset": "X",
        "source_state": "watch_candidate",
        "priority": 50,
        "scan_score": 50,
        "move_side": "up",
        "trigger_type": "t",
        "subscribe": ["aggTrade", "bookTicker"],
        "target_ready_tf": "15m",
        "min_collect_seconds": 900,
        "ttl_seconds": 1800,
    }
    base.update(kwargs)
    return base


def _doc(
    *,
    generated_at: str = "2026-01-10T12:00:00Z",
    status: str = "ok",
    tier1: list | None = None,
    tier2: list | None = None,
) -> dict[str, object]:
    t1 = tier1 or []
    t2 = tier2 or []
    return {
        "schema_version": "1.6",
        "generated_at": generated_at,
        "source": "micro_target_router",
        "status": status,
        "warm_watch_limit": 30,
        "active_strong_limit": 10,
        "input_watch_status": "ok",
        "input_strong_status": "ok",
        "input_snapshot_generated_at": generated_at,
        "input_snapshot_age_sec": 0,
        "step2_reported_input_snapshot_age_sec": 0,
        "router_computed_input_snapshot_age_sec": 0,
        "router_freshness_ok": True,
        "input_counts": {"raw": 0, "watch": len(t1), "strong": len(t2)},
        "routed_counts": {"tier1": len(t1), "tier2": len(t2)},
        "truncated": {"tier1": False, "tier2": False},
        "skip_reasons": [],
        "tier1_warm_watch": t1,
        "tier2_active_strong": t2,
    }


def _write(p: Path, obj: dict[str, object]) -> None:
    p.write_text(json.dumps(obj), encoding="utf-8", newline="")


@pytest.fixture
def t0() -> object:
    return parse_iso_z("2026-01-10T12:00:00Z")


def test_tm1_empty_tiers_ok_file(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    _write(p, _doc(tier1=[], tier2=[]))
    clock = {"now": t0}
    m = MicroTargetManager(
        p,
        TargetManagerSettings(target_stale_sec=999999),
        now_fn=lambda: clock["now"],
    )
    r = m.reload()
    assert r.loaded
    assert r.status == "ok"
    assert m.get_subscribe_intents() == []


def test_tm2_new_tier1_subscribe_intent(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    _write(p, _doc(tier1=[_entry("BTCUSDT", priority=71)]))
    m = MicroTargetManager(p, TargetManagerSettings(target_stale_sec=999999), now_fn=lambda: t0)
    m.reload()
    subs = m.get_subscribe_intents()
    assert len(subs) == 1
    s = subs[0]
    assert s.symbol == "BTCUSDT"
    assert s.streams == ("aggTrade", "bookTicker")
    assert s.priority == 71
    assert s.tier_key == "tier1_warm_watch"


def test_tm3_disappear_retiring_then_retire_intent(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    t_start = t0
    clock = {"now": t_start}
    settings = TargetManagerSettings(target_stale_sec=999999, unsubscribe_grace_sec=60, max_managed_symbols=10)
    m = MicroTargetManager(p, settings, now_fn=lambda: clock["now"])
    _write(p, _doc(tier1=[_entry("ETHUSDT")]))
    m.reload()
    _write(p, _doc(tier1=[]))
    m.reload()
    assert not m.get_retire_intents()
    clock["now"] = t_start + timedelta(seconds=61)
    ri = m.get_retire_intents()
    assert any(x.symbol == "ETHUSDT" and x.reason == "missing_from_file" for x in ri)


def test_tm4_stale_no_new_subscribe(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    gen = "2026-01-10T10:00:00Z"
    clock = {"now": parse_iso_z("2026-01-10T12:30:00Z")}
    settings = TargetManagerSettings(target_stale_sec=300, max_managed_symbols=10)
    m = MicroTargetManager(p, settings, now_fn=lambda: clock["now"])
    _write(
        p,
        _doc(
            generated_at=gen,
            tier1=[_entry("AAAUSDT", priority=99)],
        ),
    )
    r = m.reload()
    assert r.status == "stale"
    assert r.target_status == "stale"
    assert "AAAUSDT" in r.blocked_new_due_stale
    assert m.get_subscribe_intents() == []


def test_tm5_invalid_status_no_subscribe(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    d = _doc(tier1=[_entry("XRPUSDT")])
    d["status"] = "stale_input"
    _write(p, d)
    m = MicroTargetManager(p, TargetManagerSettings(target_stale_sec=999999), now_fn=lambda: t0)
    r = m.reload()
    assert r.status == "stale"
    assert r.target_status == "stale"
    assert m.get_subscribe_intents() == []


def test_tm6_missing_file(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "nope.json"
    m = MicroTargetManager(p, TargetManagerSettings(), now_fn=lambda: t0)
    r = m.reload()
    assert r.status == "missing"
    assert not r.loaded


def test_tm7_cap_evict_lowest_retiring_priority(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    clock = {"now": t0}
    settings = TargetManagerSettings(
        target_stale_sec=999999,
        unsubscribe_grace_sec=10,
        max_managed_symbols=3,
    )
    m = MicroTargetManager(p, settings, now_fn=lambda: clock["now"])
    _write(
        p,
        _doc(
            tier1=[
                _entry("A", priority=30),
                _entry("B", priority=20),
                _entry("C", priority=10),
            ]
        ),
    )
    m.reload()
    _write(p, _doc(tier1=[]))
    m.reload()
    _write(p, _doc(tier1=[_entry("NEW1", priority=25)]))
    r = m.reload()
    assert "C" in r.evicted
    subs = {x.symbol for x in m.get_subscribe_intents()}
    assert "NEW1" in subs
    assert "C" not in subs


def test_tm8_tier1_to_tier2_upgrade_streams(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    clock = {"now": t0}
    m = MicroTargetManager(p, TargetManagerSettings(target_stale_sec=999999), now_fn=lambda: clock["now"])
    _write(p, _doc(tier1=[_entry("ZZUSDT")]))
    m.reload()
    assert m.get_subscribe_intents()[0].tier_key == "tier1_warm_watch"
    _write(
        p,
        _doc(
            tier1=[],
            tier2=[
                _entry(
                    "ZZUSDT",
                    source_state="strong_candidate",
                    subscribe=["aggTrade", "bookTicker", "partialDepth5"],
                )
            ],
        ),
    )
    m.reload()
    s = m.get_subscribe_intents()[0]
    assert s.tier_key == "tier2_active_strong"
    assert "partialDepth5" in s.streams


def test_tm9_stale_reload_does_not_refresh_last_seen(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    fresh_gen = "2026-01-10T12:00:00Z"
    clock = {"now": parse_iso_z("2026-01-10T12:01:00Z")}
    settings = TargetManagerSettings(target_stale_sec=60, max_managed_symbols=10)
    m = MicroTargetManager(p, settings, now_fn=lambda: clock["now"])
    _write(
        p,
        _doc(
            generated_at=fresh_gen,
            tier1=[_entry("KEEPUSDT", ttl_seconds=999999)],
        ),
    )
    m.reload()
    before = m.get_subscribe_intents()[0].last_target_seen_at
    stale_gen = "2026-01-10T10:00:00Z"
    clock["now"] = parse_iso_z("2026-01-10T13:00:00Z")
    _write(
        p,
        _doc(
            generated_at=stale_gen,
            tier1=[_entry("KEEPUSDT", priority=80, ttl_seconds=999999)],
        ),
    )
    r = m.reload()
    assert r.status == "stale"
    after = m.get_subscribe_intents()[0].last_target_seen_at
    assert after == before


def test_tm10_duplicate_tier2_wins(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    _write(
        p,
        _doc(
            tier1=[_entry("DUPEUSDT", priority=40, source_state="watch_candidate")],
            tier2=[
                _entry(
                    "DUPEUSDT",
                    priority=35,
                    source_state="strong_candidate",
                    subscribe=["aggTrade", "bookTicker", "partialDepth5"],
                )
            ],
        ),
    )
    m = MicroTargetManager(p, TargetManagerSettings(target_stale_sec=999999), now_fn=lambda: t0)
    r = m.reload()
    assert "DUPEUSDT" in r.duplicate_symbol_tier2_wins
    s = m.get_subscribe_intents()[0]
    assert s.source_state == "strong_candidate"
    assert s.priority == 40
    assert "partialDepth5" in s.streams


def test_tm11_ttl_deadline_refreshes_when_reappears(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    t_start = t0
    clock = {"now": t_start}
    settings = TargetManagerSettings(target_stale_sec=999999, max_managed_symbols=10)
    m = MicroTargetManager(p, settings, now_fn=lambda: clock["now"])
    _write(p, _doc(tier1=[_entry("TTLUSDT", ttl_seconds=1000)]))
    m.reload()
    m1 = m.get_subscribe_intents()[0]
    ttl1 = m._managed["TTLUSDT"].ttl_deadline
    clock["now"] = t_start + timedelta(seconds=50)
    m.reload()
    m2 = m.get_subscribe_intents()[0]
    ttl2 = m._managed["TTLUSDT"].ttl_deadline
    d1 = parse_iso_z(m1.last_target_seen_at)
    d2 = parse_iso_z(m2.last_target_seen_at)
    assert d2 > d1
    assert ttl2 > ttl1
    assert ttl2 - d2 == ttl1 - d1


def test_tm12_no_retiring_cap_blocks_new(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    clock = {"now": t0}
    settings = TargetManagerSettings(target_stale_sec=999999, max_managed_symbols=2)
    m = MicroTargetManager(p, settings, now_fn=lambda: clock["now"])
    _write(p, _doc(tier1=[_entry("P1", priority=50), _entry("P2", priority=51)]))
    m.reload()
    m.mark_subscribed("P1")
    m.mark_subscribed("P2")
    _write(
        p,
        _doc(
            tier1=[
                _entry("P1", priority=50),
                _entry("P2", priority=51),
                _entry("P3", priority=99),
            ]
        ),
    )
    r = m.reload()
    assert "P3" in r.blocked_new_due_cap
    assert all(x.symbol != "P3" for x in m.get_subscribe_intents())


def test_tm13_symbol_safe_id_stable(tmp_path: Path, t0: object) -> None:
    weird = "BTC/USDT"
    a = build_symbol_safe_id(weird)
    b = build_symbol_safe_id(weird)
    assert a == b
    assert a.startswith("BTC_USDT_")
    assert len(a.split("_")[-1]) == 8


def test_tm14_invalid_json_parse_error(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    p.write_text("{ not json", encoding="utf-8", newline="")
    m = MicroTargetManager(p, TargetManagerSettings(), now_fn=lambda: t0)
    r = m.reload()
    assert r.status == "parse_error"
    assert m.get_subscribe_intents() == []


def test_tm15_additive_target_metadata_is_compatible(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    doc = _doc(tier1=[_entry("METAUSDT", extra_entry_metric=12)])
    doc["step2_current_freshness"] = {"current_freshness": "fresh", "extra": "ok"}
    doc["plan_candidate_symbols"] = ["METAUSDT"]
    doc["candidate_alignment"] = {
        "schema_version": "1.0",
        "mode": "micro_targets_authoritative",
        "include_tier1": True,
        "include_tier2": True,
        "include_ready_cache": False,
        "ready_cache_max_age_sec": 0,
        "generated_at": doc["generated_at"],
        "future_field": "must_not_break_daemon",
    }
    doc["future_top_level_field"] = {"ignored": True}
    _write(p, doc)

    m = MicroTargetManager(p, TargetManagerSettings(target_stale_sec=999999), now_fn=lambda: t0)
    r = m.reload()

    assert r.status == "ok"
    assert r.target_status == "fresh"
    subs = m.get_subscribe_intents()
    assert len(subs) == 1
    assert subs[0].symbol == "METAUSDT"


def test_mark_unsubscribed_clears_retire_intent(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    clock = {"now": t0}
    settings = TargetManagerSettings(target_stale_sec=999999, unsubscribe_grace_sec=1, max_managed_symbols=10)
    m = MicroTargetManager(p, settings, now_fn=lambda: clock["now"])
    _write(p, _doc(tier1=[_entry("ACKUSDT")]))
    m.reload()
    _write(p, _doc(tier1=[]))
    m.reload()
    clock["now"] = t0 + timedelta(seconds=5)
    assert m.get_retire_intents()
    m.mark_unsubscribed("ACKUSDT")
    assert not m.get_retire_intents()


def test_update_quality_state_stub(tmp_path: Path, t0: object) -> None:
    p = tmp_path / "micro_targets.json"
    _write(p, _doc(tier1=[_entry("QUSDT")]))
    m = MicroTargetManager(p, TargetManagerSettings(target_stale_sec=999999), now_fn=lambda: t0)
    m.reload()
    m.mark_subscribed("QUSDT")
    m.update_quality_state("QUSDT", True)
    assert m.get_subscribe_intents()[0].lifecycle == "active"
