"""STEP3.8 Micro Collector daemon tests D1-D12. docs/STEP3.8_任务卡.md."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from laoma_signal_engine.core.time_utils import parse_iso_z, to_iso_z, utc_now
from laoma_signal_engine.micro.assembly.models import LatestMicroFeaturesDocument
from laoma_signal_engine.micro.daemon.app import run_daemon
from laoma_signal_engine.micro.daemon.config import DaemonConfig
from laoma_signal_engine.micro.daemon.fixture_envelopes import load_fixture_envelopes_from_text
from laoma_signal_engine.micro.daemon.heartbeat import MicroCollectorHeartbeat
from laoma_signal_engine.micro.daemon.loop import (
    CollectStartedAckBridge,
    assembly_rows_from_intents,
    build_run_context,
    coerce_target_status,
    default_fast_micro_quality_config,
    document_status_for,
    run_once,
)
from laoma_signal_engine.micro.daemon.state_models import MicroDaemonStateDocument
from laoma_signal_engine.micro.quality.models import MicroQualityConfig
from laoma_signal_engine.micro.target_intent_models import TargetManagerSettings
from laoma_signal_engine.micro.target_manager import MicroTargetManager
from laoma_signal_engine.micro.ws.real_transport import RealBinanceFuturesWebSocketTransport
from laoma_signal_engine.micro.ws.subscription_manager import BinanceFuturesWSManager, FakeWebSocketTransport, WSConfig
from laoma_signal_engine.tests.test_target_manager_step32 import _doc as tm_doc
from laoma_signal_engine.tests.test_target_manager_step32 import _entry as tm_entry


SYM = "BTCUSDT"


def _micro_targets_doc(*, generated_at: str, status: str = "ok") -> dict[str, object]:
    t1 = [
        tm_entry(
            SYM,
            min_collect_seconds=0,
            subscribe=["aggTrade", "bookTicker"],
        ),
    ]
    return tm_doc(generated_at=generated_at, status=status, tier1=t1, tier2=[])


def _write_json(p: Path, obj: dict[str, object]) -> None:
    p.write_text(json.dumps(obj), encoding="utf-8", newline="")


def _permissive_quality() -> MicroQualityConfig:
    return MicroQualityConfig(
        window_sec=30,
        min_ready_seconds=0,
        aggtrade_coverage_min=0.01,
        bookticker_coverage_min=0.01,
        depth5_coverage_min=0.01,
        max_stale_sec=999_999,
        max_lag_sec=999_999,
        event_queue_overflow_hard_fail=False,
        adapter_error_hard_fail=False,
    )


def _fixture_json(ts_ms: int, recv_ms: int) -> str:
    return json.dumps(
        [
            {
                "symbol": SYM,
                "stream_type": "aggTrade",
                "event_ts_ms": ts_ms,
                "recv_ts_ms": recv_ms,
                "normalized": {
                    "type": "trade",
                    "symbol": SYM,
                    "ts_ms": ts_ms,
                    "price": 100.0,
                    "qty": 1.0,
                    "side": "buy",
                },
            },
            {
                "symbol": SYM,
                "stream_type": "bookTicker",
                "event_ts_ms": ts_ms,
                "recv_ts_ms": recv_ms,
                "normalized": {
                    "type": "book",
                    "symbol": SYM,
                    "ts_ms": ts_ms,
                    "bids": [[100.0, 1.0]],
                    "asks": [[100.5, 1.0]],
                    "levels": 1,
                },
            },
        ],
    )


@pytest.fixture
def t0() -> object:
    return parse_iso_z("2026-01-10T12:00:00Z")


def test_d1_once_writes_parseable_latest(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    fixture = tmp_path / "ev.json"
    ts_ms = int(t0.timestamp() * 1000) + 1000
    fixture.write_text(_fixture_json(ts_ms, ts_ms), encoding="utf-8", newline="")
    _write_json(targets, _micro_targets_doc(generated_at=to_iso_z(t0)))

    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=999_999,
    )
    r = asyncio.run(
        run_daemon(
            cfg,
            now_fn=lambda: t0,
            fixture_events_path=fixture,
            quality_config=_permissive_quality(),
            once=True,
        ),
    )
    assert r is not None
    assert r.exit_code == 0
    doc = LatestMicroFeaturesDocument.model_validate_json(latest.read_text(encoding="utf-8"))
    assert len(doc.items) >= 1
    assert doc.full_ready_count == doc.ready_count
    assert doc.items[0].micro_full_quality is not None
    assert doc.items[0].micro_fast_quality is not None


def test_d2_stale_target_subscribe_empty_items_empty(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    old_gen = to_iso_z(t0 - timedelta(seconds=9_999))
    _write_json(targets, _micro_targets_doc(generated_at=old_gen))
    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=60,
    )
    transport_calls: list[list[str]] = []

    class RecordingFake:
        def __init__(self) -> None:
            self._active: set[str] = set()

        @property
        def active_streams(self) -> set[str]:
            return set(self._active)

        async def subscribe_streams(self, streams: list[str]) -> None:
            transport_calls.append(list(streams))
            for s in streams:
                self._active.add(s)

        async def unsubscribe_streams(self, streams: list[str]) -> None:
            for s in streams:
                self._active.discard(s)

    settings = TargetManagerSettings(target_stale_sec=60)
    tm = MicroTargetManager(targets, settings, now_fn=lambda: t0)
    bridge = CollectStartedAckBridge(tm, lambda: t0)
    rec = RecordingFake()
    ws = BinanceFuturesWSManager(WSConfig(), rec, target_manager=bridge)
    ctx = build_run_context(cfg, lambda: t0, quality_config=_permissive_quality())
    ctx.ws_manager = ws
    r = asyncio.run(run_once(ctx))
    assert r.exit_code == 0
    assert transport_calls == [] or all(len(c) == 0 for c in transport_calls)
    doc = LatestMicroFeaturesDocument.model_validate_json(latest.read_text(encoding="utf-8"))
    assert doc.items == []
    assert doc.symbol_count == 0
    assert doc.status == "stale_targets"
    assert doc.target_status == "stale"
    state = MicroDaemonStateDocument.model_validate_json(
        latest.with_name("latest_micro_state.json").read_text(encoding="utf-8"),
    )
    assert state.daemon_status == "running"
    assert state.health_state == "healthy_idle"
    assert state.state_ready_for_consumers is False
    assert "idle_no_valid_targets" in state.reason_codes


def test_d3_reload_second_iteration(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    _write_json(targets, _micro_targets_doc(generated_at=to_iso_z(t0)))
    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=999_999,
    )
    asyncio.run(run_daemon(cfg, now_fn=lambda: t0, quality_config=_permissive_quality(), once=True))
    asyncio.run(run_daemon(cfg, now_fn=lambda: t0, quality_config=_permissive_quality(), once=True))


def test_d456_heartbeat_and_dropped(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    fixture = tmp_path / "ev.json"
    ts_ms = int(t0.timestamp() * 1000) + 2000
    fixture.write_text(_fixture_json(ts_ms, ts_ms), encoding="utf-8", newline="")
    _write_json(targets, _micro_targets_doc(generated_at=to_iso_z(t0)))
    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=999_999,
    )
    r = asyncio.run(
        run_daemon(
            cfg,
            now_fn=lambda: t0,
            fixture_events_path=fixture,
            quality_config=_permissive_quality(),
            once=True,
        ),
    )
    assert r is not None and r.exit_code == 0
    ldoc = LatestMicroFeaturesDocument.model_validate_json(latest.read_text(encoding="utf-8"))
    hdoc = MicroCollectorHeartbeat.model_validate_json(hb.read_text(encoding="utf-8"))
    assert hdoc.target_status == ldoc.target_status
    assert ldoc.model_dump(mode="json")["dropped_events"] == {"trade": 0, "book": 0, "depth": 0}


def test_d6_reconnect_reset(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    fixture = tmp_path / "ev.json"
    ts_ms = int(t0.timestamp() * 1000) + 500
    fixture.write_text(_fixture_json(ts_ms, ts_ms), encoding="utf-8", newline="")
    _write_json(targets, _micro_targets_doc(generated_at=to_iso_z(t0)))
    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=999_999,
    )
    ctx = build_run_context(
        cfg,
        lambda: t0,
        fixture_events_path=fixture,
        quality_config=_permissive_quality(),
    )
    ctx.ws_manager.apply_reconnect_reset()
    r = asyncio.run(run_once(ctx))
    assert r.exit_code == 0


def test_d7_status_mapping_table() -> None:
    assert document_status_for(coerce_target_status("fresh"), 2) == "ok"
    assert document_status_for(coerce_target_status("fresh"), 0) == "no_targets"
    assert document_status_for(coerce_target_status("stale_observing"), 2) == "observing_stale_targets"
    assert document_status_for(coerce_target_status("stale_observing"), 0) == "stale_targets"
    assert document_status_for(coerce_target_status("stale"), 0) == "stale_targets"
    assert document_status_for(coerce_target_status("invalid_targets"), 0) == "error"
    assert document_status_for(coerce_target_status("unknown"), 0) == "error"


def test_fast_micro_default_is_shorter_than_full() -> None:
    cfg = default_fast_micro_quality_config()
    assert cfg.window_sec == 180
    assert cfg.min_ready_seconds == 90
    assert cfg.min_ready_seconds < MicroQualityConfig().min_ready_seconds


def test_d8_invalid_targets_items_empty(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    _write_json(targets, _micro_targets_doc(generated_at=to_iso_z(t0), status="error"))
    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=999_999,
    )
    r = asyncio.run(run_daemon(cfg, now_fn=lambda: t0, quality_config=_permissive_quality(), once=True))
    assert r is not None and r.exit_code == 0
    doc = LatestMicroFeaturesDocument.model_validate_json(latest.read_text(encoding="utf-8"))
    assert doc.items == []
    assert doc.target_status == "invalid_targets"
    assert doc.status == "error"


def test_d9_collect_started_resets_on_unsub() -> None:
    t = parse_iso_z("2026-01-10T12:00:00Z")

    class TM:
        def mark_subscribed(self, s: str) -> None:
            _ = s

        def mark_unsubscribed(self, s: str) -> None:
            _ = s

    tm = TM()
    br = CollectStartedAckBridge(tm, lambda: t)  # type: ignore[arg-type]
    br.mark_subscribed("BTCUSDT")
    a = br.collect_started_ts_sec("BTCUSDT")
    assert a is not None
    br.mark_unsubscribed("BTCUSDT")
    assert br.collect_started_ts_sec("BTCUSDT") is None
    br.mark_subscribed("BTCUSDT")
    assert br.collect_started_ts_sec("BTCUSDT") is not None


def test_d10_build_failure_preserves_latest(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    fixture = tmp_path / "ev.json"
    ts_ms = int(t0.timestamp() * 1000) + 700
    fixture.write_text(_fixture_json(ts_ms, ts_ms), encoding="utf-8", newline="")
    _write_json(targets, _micro_targets_doc(generated_at=to_iso_z(t0)))
    latest.write_text("SENTINEL_LATEST", encoding="utf-8", newline="")
    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=999_999,
    )
    ctx = build_run_context(
        cfg,
        lambda: t0,
        fixture_events_path=fixture,
        quality_config=_permissive_quality(),
    )
    with patch(
        "laoma_signal_engine.micro.daemon.loop.build_document",
        side_effect=ValueError("injected_fail"),
    ):
        r = asyncio.run(run_once(ctx))
    assert r.exit_code == 1
    assert latest.read_text(encoding="utf-8") == "SENTINEL_LATEST"
    hbdoc = MicroCollectorHeartbeat.model_validate_json(hb.read_text(encoding="utf-8"))
    assert hbdoc.last_error is not None and "injected_fail" in hbdoc.last_error


def test_d11_heartbeat_extra_forbidden() -> None:
    with pytest.raises(ValidationError):
        MicroCollectorHeartbeat.model_validate(
            {
                "schema_version": "1.0",
                "generated_at": "2026-01-01T00:00:00Z",
                "source": "micro_collector_daemon",
                "process_uptime_sec": 0,
                "last_loop_ok": True,
                "last_error": None,
                "target_reload_last_at": "2026-01-01T00:00:00Z",
                "target_manager_status": "ok",
                "target_status": "fresh",
                "target_age_sec": 0.0,
                "managed_symbol_count": 0,
                "ws_connected": True,
                "ws_last_message_age_sec": None,
                "dropped_events": {"trade": 0, "book": 0, "depth": 0},
                "latest_features_written_at": None,
                "evil": 1,
            },
        )


def test_d12_fixture_load_roundtrip() -> None:
    evs = load_fixture_envelopes_from_text(_fixture_json(5000, 5000))
    assert len(evs) == 2
    assert evs[0].stream_type == "aggTrade"


def test_assembly_rows_from_subscribe_only(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    _write_json(targets, _micro_targets_doc(generated_at=to_iso_z(t0)))
    tm = MicroTargetManager(
        targets,
        TargetManagerSettings(target_stale_sec=999_999),
        now_fn=lambda: t0,
    )
    tm.reload()
    rows = assembly_rows_from_intents(tm.get_subscribe_intents())
    assert len(rows) == 1 and rows[0].symbol == SYM


def test_stale_once_exits_zero(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    old_gen = to_iso_z(t0 - timedelta(seconds=9_999))
    _write_json(targets, _micro_targets_doc(generated_at=old_gen))
    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=60,
    )
    r = asyncio.run(run_daemon(cfg, now_fn=lambda: t0, once=True))
    assert r is not None and r.exit_code == 0


def test_stale_target_observes_existing_managed_symbols(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    fixture = tmp_path / "ev.json"
    ts_ms = int(t0.timestamp() * 1000) + 1000
    fixture.write_text(_fixture_json(ts_ms, ts_ms), encoding="utf-8", newline="")
    _write_json(targets, _micro_targets_doc(generated_at=to_iso_z(t0)))

    clock = {"now": t0}
    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=60,
    )
    ctx = build_run_context(
        cfg,
        lambda: clock["now"],
        fixture_events_path=fixture,
        quality_config=_permissive_quality(),
    )

    r1 = asyncio.run(run_once(ctx))
    assert r1.exit_code == 0
    clock["now"] = t0 + timedelta(seconds=120)
    r2 = asyncio.run(run_once(ctx))
    assert r2.exit_code == 0

    doc = LatestMicroFeaturesDocument.model_validate_json(latest.read_text(encoding="utf-8"))
    assert doc.status == "observing_stale_targets"
    assert doc.target_status == "stale_observing"
    assert doc.symbol_count == 1
    assert doc.items[0].symbol == SYM
    assert "target_stale_observing_existing_symbols" in doc.reason_codes
    state = MicroDaemonStateDocument.model_validate_json(
        latest.with_name("latest_micro_state.json").read_text(encoding="utf-8"),
    )
    assert state.health_state == "stale_observing"
    assert state.active_symbol_count == 1
    assert state.state_ready_for_consumers is True
    assert "target_stale_observing_existing_symbols" in state.reason_codes


def test_fresh_no_target_ack_exit_one(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    fixture = tmp_path / "ev.json"
    ts_ms = int(t0.timestamp() * 1000) + 800
    fixture.write_text(_fixture_json(ts_ms, ts_ms), encoding="utf-8", newline="")
    _write_json(targets, _micro_targets_doc(generated_at=to_iso_z(t0)))
    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=999_999,
    )
    ctx = build_run_context(
        cfg,
        lambda: t0,
        fixture_events_path=fixture,
        quality_config=_permissive_quality(),
    )
    ctx.ws_manager = BinanceFuturesWSManager(WSConfig(), FakeWebSocketTransport(), target_manager=None)
    r = asyncio.run(run_once(ctx))
    assert r.exit_code == 1


def test_no_websocket_httpx_in_daemon() -> None:
    root = Path(__file__).resolve().parents[1] / "micro" / "daemon"
    for name in (
        "__init__.py",
        "app.py",
        "cli.py",
        "config.py",
        "fixture_envelopes.py",
        "heartbeat.py",
        "loop.py",
    ):
        text = (root / name).read_text(encoding="utf-8")
        for bad in (
            "import websocket",
            "from websocket",
            "import websockets",
            "from websockets",
            "import httpx",
            "from httpx",
        ):
            assert bad not in text, name


def test_transport_real_proxy_url_raises_value_error() -> None:
    cfg = DaemonConfig(
        targets_path=Path("x"),
        latest_features_path=Path("y"),
        heartbeat_path=Path("z"),
        transport="real",
        proxy_url="http://127.0.0.1:9",
    )
    with pytest.raises(ValueError, match="proxy"):
        build_run_context(cfg, utc_now, quality_config=_permissive_quality())


def test_transport_real_builds_without_network(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    _write_json(targets, _micro_targets_doc(generated_at=to_iso_z(t0)))

    async def _close() -> None:
        cfg = DaemonConfig(
            targets_path=targets,
            latest_features_path=tmp_path / "latest_micro_features.json",
            heartbeat_path=tmp_path / "micro_collector_heartbeat.json",
            transport="real",
            proxy_url=None,
        )
        ctx = build_run_context(cfg, lambda: t0, quality_config=_permissive_quality())
        assert isinstance(ctx.ws_manager._transport, RealBinanceFuturesWebSocketTransport)
        await ctx.aclose_optional_real()

    asyncio.run(_close())


def test_short_run_executes_loop(tmp_path: Path, t0: object) -> None:
    targets = tmp_path / "micro_targets.json"
    latest = tmp_path / "latest_micro_features.json"
    hb = tmp_path / "heartbeat.json"
    old_gen = to_iso_z(t0 - timedelta(seconds=9_999))
    _write_json(targets, _micro_targets_doc(generated_at=old_gen))
    cfg = DaemonConfig(
        targets_path=targets,
        latest_features_path=latest,
        heartbeat_path=hb,
        target_stale_sec=60,
        output_interval_sec=0,
    )
    asyncio.run(run_daemon(cfg, now_fn=lambda: t0, short_run_sec=0.12))
    assert latest.is_file()
