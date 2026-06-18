"""Tests for Step 2.5 micro target router."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from laoma_signal_engine.core.config_loader import EngineConfig
from laoma_signal_engine.market.light_snapshot_models import (
    BackgroundBlock,
    Primary15mBlock,
    Trigger5mBlock,
)
from laoma_signal_engine.micro.micro_target_models import MicroTargetsDocument
from laoma_signal_engine.micro.micro_target_router import run_micro_target_router
from laoma_signal_engine.scanner.signal_models import (
    AbnormalSignalEntry,
    AbnormalTierDocument,
    ScoreBreakdownBlock,
)

FIXED_ROUTER_NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
SNAP_FRESH = "2026-06-01T11:55:30Z"


@pytest.fixture(autouse=True)
def _patch_micro_router_utc_now(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "laoma_signal_engine.micro.micro_target_router.utc_now",
        lambda: FIXED_ROUTER_NOW,
    )


def _bd() -> ScoreBreakdownBlock:
    return ScoreBreakdownBlock(
        price_score=10,
        volume_score=10,
        kline_cvd_score=10,
        trigger_5m_score=10,
        liquidity_score=5,
        background_penalty=0,
    )


def _sig(
    *,
    symbol: str,
    base: str,
    state: str,
    scan_score: int = 55,
    market_entry_suitability_score: int = 0,
    market_entry_suitability: str = "unknown",
    trade_candidate_rank_score: int = 0,
    trade_candidate_bucket: str = "unknown",
    vr: float | None = 2.0,
    qv: float | None = 1000000.0,
) -> AbnormalSignalEntry:
    return AbnormalSignalEntry(
        symbol=symbol,
        base_asset=base,
        futures_symbol=symbol,
        has_um_futures=True,
        decision_tf="15m",
        source_tags=[],
        state=state,
        move_side="up",
        scan_score=scan_score,
        market_entry_suitability_score=market_entry_suitability_score,
        market_entry_suitability=market_entry_suitability,
        trade_candidate_rank_score=trade_candidate_rank_score,
        trade_candidate_bucket=trade_candidate_bucket,
        score_breakdown=_bd(),
        input_snapshot_generated_at=SNAP_FRESH,
        trigger_type="futures_15m_momentum",
        primary_15m=Primary15mBlock(ready=True, volume_ratio=vr),
        trigger_5m=Trigger5mBlock(),
        background=BackgroundBlock(quote_volume_24h=qv),
        reason_codes=[],
        next_stage="warm_pool" if state == "watch_candidate" else ("micro_confirm" if state == "strong_candidate" else "none"),
    )


def _tier(
    tier: str,
    *,
    status: str,
    signals: list[AbnormalSignalEntry],
    snap: str = SNAP_FRESH,
    age: int = 10,
) -> AbnormalTierDocument:
    return AbnormalTierDocument(
        schema_version="1.6",
        generated_at="2026-06-01T12:05:00Z",
        tier=tier,
        status=status,
        input_snapshot_generated_at=snap,
        input_snapshot_age_sec=age,
        input_freshness="fresh",
        stale_warning=False,
        reason_codes=[],
        count=len(signals),
        signals=signals,
    )


def _write_three(tmp_path: Path, raw: AbnormalTierDocument, watch: AbnormalTierDocument, strong: AbnormalTierDocument) -> tuple[Path, Path, Path]:
    dr = tmp_path / "DATA" / "raw_signals"
    dm = tmp_path / "DATA" / "micro"
    dr.mkdir(parents=True, exist_ok=True)
    dm.mkdir(parents=True, exist_ok=True)
    rp = dr / "latest_raw_candidates.json"
    wp = dr / "latest_watch_signals.json"
    sp = dr / "latest_strong_candidates.json"
    for p, doc in ((rp, raw), (wp, watch), (sp, strong)):
        with open(p, "w", encoding="utf-8", newline="") as fp:
            json.dump(doc.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)
    return rp, wp, sp


def test_micro_router_ok(tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[_sig(symbol="AUSDT", base="A", state="raw_candidate")])
    watch = _tier("watch_candidate", status="ok", signals=[_sig(symbol="BUSDT", base="B", state="watch_candidate")])
    strong = _tier(
        "strong_candidate",
        status="ok",
        signals=[_sig(symbol="CUSDT", base="C", state="strong_candidate", scan_score=90)],
    )
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"
    assert (
        run_micro_target_router(
            project_root=tmp_path,
            raw_path=tmp_path / "DATA" / "raw_signals" / "latest_raw_candidates.json",
            watch_path=tmp_path / "DATA" / "raw_signals" / "latest_watch_signals.json",
            strong_path=tmp_path / "DATA" / "raw_signals" / "latest_strong_candidates.json",
            output_path=out,
        )
        == 0
    )
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.status == "ok"
    assert doc.router_freshness_ok is True
    assert doc.input_snapshot_age_sec == 270
    assert doc.router_computed_input_snapshot_age_sec == 270
    assert doc.step2_reported_input_snapshot_age_sec == 10
    assert doc.routed_counts.tier1 == 1
    assert doc.routed_counts.tier2 == 1
    assert doc.plan_candidate_symbols == ["BUSDT", "CUSDT"]
    assert doc.plan_candidate_count == 2
    assert doc.candidate_alignment is not None
    assert doc.candidate_alignment.mode == "micro_targets_authoritative"
    assert doc.tier2_active_strong[0].symbol == "CUSDT"
    assert doc.tier2_active_strong[0].subscribe == ["aggTrade", "bookTicker", "partialDepth5"]


def test_micro_router_both_stale(tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch = _tier("watch_candidate", status="stale_input", signals=[], age=99999)
    strong = _tier("strong_candidate", status="stale_input", signals=[], age=99999)
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.status == "stale_input"
    assert doc.router_freshness_ok is False
    assert doc.block_downstream is True
    assert doc.block_reason == "step2_stale"
    assert doc.step2_current_freshness["current_freshness"] == "stale"
    assert "watch_status_stale_input" in doc.step2_current_freshness["reason_codes"]
    assert "strong_status_stale_input" in doc.step2_current_freshness["reason_codes"]
    assert doc.routed_counts.tier1 == 0
    assert doc.routed_counts.tier2 == 0


def test_micro_router_partial_watch_stale(tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch = _tier("watch_candidate", status="stale_input", signals=[_sig(symbol="XUSDT", base="X", state="watch_candidate")])
    strong = _tier(
        "strong_candidate",
        status="ok",
        signals=[_sig(symbol="YUSDT", base="Y", state="strong_candidate")],
    )
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.status == "partial_input_stale"
    assert doc.routed_counts.tier2 == 1
    assert doc.routed_counts.tier1 == 0


def test_micro_router_ok_dev_stale_blocks_watch(tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch = _tier(
        "watch_candidate",
        status="ok_dev_stale_allowed",
        signals=[_sig(symbol="BUSDT", base="B", state="watch_candidate")],
    )
    strong = _tier("strong_candidate", status="ok", signals=[])
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.status == "partial_input_stale"
    assert doc.routed_counts.tier1 == 0


def test_micro_router_truncated_tier2(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch = _tier("watch_candidate", status="ok", signals=[])
    strong_sigs = [
        _sig(symbol=f"S{i}USDT", base=f"S{i}", state="strong_candidate", scan_score=80 - i) for i in range(5)
    ]
    strong = _tier("strong_candidate", status="ok", signals=strong_sigs)
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"

    base_cfg = EngineConfig.load(tmp_path)
    monkeypatch.setattr(
        "laoma_signal_engine.micro.micro_target_router.EngineConfig.load",
        lambda _pr=None: replace(base_cfg, mr_active_strong_limit=2),
    )
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.truncated.tier2 is True
    assert "tier2_truncated" in doc.skip_reasons
    assert doc.routed_counts.tier2 == 2


def test_micro_router_respects_total_active_cap(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch_sigs = [
        _sig(symbol=f"W{i}USDT", base=f"W{i}", state="watch_candidate", scan_score=70 - i) for i in range(5)
    ]
    strong_sigs = [
        _sig(symbol=f"S{i}USDT", base=f"S{i}", state="strong_candidate", scan_score=90 - i) for i in range(5)
    ]
    watch = _tier("watch_candidate", status="ok", signals=watch_sigs)
    strong = _tier("strong_candidate", status="ok", signals=strong_sigs)
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"

    base_cfg = EngineConfig.load(tmp_path)
    monkeypatch.setattr(
        "laoma_signal_engine.micro.micro_target_router.EngineConfig.load",
        lambda _pr=None: replace(
            base_cfg,
            mr_active_strong_limit=3,
            mr_warm_watch_limit=5,
            mr_max_active_micro_symbols=4,
        ),
    )
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.max_active_micro_symbols == 4
    assert doc.routed_counts.tier2 == 3
    assert doc.routed_counts.tier1 == 1
    assert doc.routed_counts.tier1 + doc.routed_counts.tier2 == 4


def test_micro_router_raw_fills_remaining(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    raws = [
        _sig(symbol=f"R{i}USDT", base=f"R{i}", state="raw_candidate", scan_score=60 + i) for i in range(3)
    ]
    raw = _tier("raw_candidate", status="ok", signals=raws)
    watch_sigs = [
        _sig(symbol=f"W{i}USDT", base=f"W{i}", state="watch_candidate", scan_score=70 + i) for i in range(2)
    ]
    watch = _tier("watch_candidate", status="ok", signals=watch_sigs)
    strong = _tier("strong_candidate", status="ok", signals=[])
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"

    base_cfg = EngineConfig.load(tmp_path)
    monkeypatch.setattr(
        "laoma_signal_engine.micro.micro_target_router.EngineConfig.load",
        lambda _pr=None: replace(base_cfg, mr_warm_watch_limit=3, mr_include_raw_in_warm_pool=True),
    )
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.routed_counts.tier1 == 3
    assert doc.tier1_warm_watch[0].source_state == "watch_candidate"
    assert doc.tier1_warm_watch[1].source_state == "watch_candidate"
    assert doc.tier1_warm_watch[2].source_state == "raw_candidate"


def test_micro_router_trade_rank_priority_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch = _tier(
        "watch_candidate",
        status="ok",
        signals=[
            _sig(symbol="SCANUSDT", base="SCAN", state="watch_candidate", scan_score=90, trade_candidate_rank_score=30),
            _sig(symbol="TRADEUSDT", base="TRADE", state="watch_candidate", scan_score=60, trade_candidate_rank_score=95),
        ],
    )
    strong = _tier("strong_candidate", status="ok", signals=[])
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"

    base_cfg = EngineConfig.load(tmp_path)
    monkeypatch.setattr(
        "laoma_signal_engine.micro.micro_target_router.EngineConfig.load",
        lambda _pr=None: replace(
            base_cfg,
            mr_warm_watch_limit=1,
            mr_priority_mode="trade_candidate_rank",
            mr_allow_trade_rank_priority=True,
        ),
    )
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.priority_mode == "trade_candidate_rank"
    assert doc.tier1_warm_watch[0].symbol == "TRADEUSDT"


def test_micro_router_can_exclude_market_entry_avoid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch = _tier(
        "watch_candidate",
        status="ok",
        signals=[
            _sig(symbol="BADUSDT", base="BAD", state="watch_candidate", market_entry_suitability="avoid", trade_candidate_bucket="avoid"),
            _sig(
                symbol="GOODUSDT",
                base="GOOD",
                state="watch_candidate",
                market_entry_suitability="allowed",
                market_entry_suitability_score=80,
                trade_candidate_bucket="allowed",
                trade_candidate_rank_score=80,
            ),
        ],
    )
    strong = _tier("strong_candidate", status="ok", signals=[])
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"

    base_cfg = EngineConfig.load(tmp_path)
    monkeypatch.setattr(
        "laoma_signal_engine.micro.micro_target_router.EngineConfig.load",
        lambda _pr=None: replace(base_cfg, mr_exclude_market_entry_avoid_from_micro=True),
    )
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.excluded_trade_avoid_count == 1
    assert doc.tier1_warm_watch[0].symbol == "GOODUSDT"
    assert "market_entry_avoid_excluded" in doc.skip_reasons


def test_micro_router_sticky_retains_previous_candidate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch = _tier("watch_candidate", status="ok", signals=[_sig(symbol="OLDUSDT", base="OLD", state="watch_candidate", scan_score=70)])
    strong = _tier("strong_candidate", status="ok", signals=[])
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"

    base_cfg = EngineConfig.load(tmp_path)
    monkeypatch.setattr(
        "laoma_signal_engine.micro.micro_target_router.EngineConfig.load",
        lambda _pr=None: replace(base_cfg, mr_warm_watch_limit=10, mr_max_active_micro_symbols=10),
    )
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0

    watch2 = _tier("watch_candidate", status="ok", signals=[_sig(symbol="NEWUSDT", base="NEW", state="watch_candidate", scan_score=75)])
    _write_three(tmp_path, raw, watch2, strong)
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0

    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    symbols = doc.plan_candidate_symbols
    assert symbols == ["NEWUSDT", "OLDUSDT"]
    old = next(e for e in doc.tier1_warm_watch if e.symbol == "OLDUSDT")
    assert old.sticky_source == "previous_target"
    assert old.retained_reason == "sticky_warmup"
    assert doc.sticky_pool["retained_count"] == 1
    assert doc.candidate_alignment is not None
    assert doc.candidate_alignment.include_ready_cache is True


def test_micro_router_sticky_expired_candidate_evicted(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch = _tier("watch_candidate", status="ok", signals=[_sig(symbol="OLDUSDT", base="OLD", state="watch_candidate", scan_score=70)])
    strong = _tier("strong_candidate", status="ok", signals=[])
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"

    base_cfg = EngineConfig.load(tmp_path)
    monkeypatch.setattr(
        "laoma_signal_engine.micro.micro_target_router.EngineConfig.load",
        lambda _pr=None: replace(base_cfg, strategy_pipeline_micro_sticky_ttl_sec=60),
    )
    # Reuse the router once to obtain a valid MicroTargetEntry, then age the document.
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        valid_previous = MicroTargetsDocument.model_validate(json.load(fp))
    expired = valid_previous.model_copy(update={"generated_at": "2026-06-01T11:58:00Z"})
    with open(out, "w", encoding="utf-8", newline="") as fp:
        json.dump(expired.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)

    watch2 = _tier("watch_candidate", status="ok", signals=[_sig(symbol="NEWUSDT", base="NEW", state="watch_candidate", scan_score=75)])
    _write_three(tmp_path, raw, watch2, strong)
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.plan_candidate_symbols == ["NEWUSDT"]
    assert "sticky_expired" in doc.sticky_pool["reason_codes"]
    assert "OLDUSDT" not in {e.symbol for e in doc.tier1_warm_watch}


def test_micro_router_current_strong_evicts_sticky_when_cap_full(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch = _tier("watch_candidate", status="ok", signals=[_sig(symbol="OLDUSDT", base="OLD", state="watch_candidate", scan_score=99)])
    strong = _tier("strong_candidate", status="ok", signals=[])
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"

    base_cfg = EngineConfig.load(tmp_path)
    monkeypatch.setattr(
        "laoma_signal_engine.micro.micro_target_router.EngineConfig.load",
        lambda _pr=None: replace(
            base_cfg,
            mr_warm_watch_limit=10,
            mr_active_strong_limit=5,
            mr_max_active_micro_symbols=1,
        ),
    )
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0

    watch2 = _tier("watch_candidate", status="ok", signals=[])
    strong2 = _tier("strong_candidate", status="ok", signals=[_sig(symbol="NEWUSDT", base="NEW", state="strong_candidate", scan_score=80)])
    _write_three(tmp_path, raw, watch2, strong2)
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0

    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.plan_candidate_symbols == ["NEWUSDT"]
    assert doc.routed_counts.tier1 + doc.routed_counts.tier2 == 1
    assert doc.sticky_pool["evicted_count"] == 1
    assert "sticky_pool_truncated" in doc.skip_reasons


def test_micro_router_can_retain_daemon_state_symbol(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    raw = _tier("raw_candidate", status="ok", signals=[])
    watch = _tier("watch_candidate", status="ok", signals=[_sig(symbol="NEWUSDT", base="NEW", state="watch_candidate", scan_score=75)])
    strong = _tier("strong_candidate", status="ok", signals=[])
    _write_three(tmp_path, raw, watch, strong)
    micro_dir = tmp_path / "DATA" / "micro"
    state_path = micro_dir / "latest_micro_state.json"
    state_path.write_text(
        json.dumps(
            {
                "symbols": [
                    {
                        "symbol": "READYUSDT",
                        "source_state": "watch_candidate",
                        "move_side": "up",
                        "priority": 80,
                        "continuous_collect_sec": 120,
                        "seen_cycle_count": 2,
                        "fast_ready": True,
                        "full_ready": False,
                        "consumer_safe": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    out = micro_dir / "micro_targets.json"

    base_cfg = EngineConfig.load(tmp_path)
    monkeypatch.setattr(
        "laoma_signal_engine.micro.micro_target_router.EngineConfig.load",
        lambda _pr=None: replace(base_cfg, micro_daemon_cli_state_path=state_path),
    )
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.plan_candidate_symbols == ["NEWUSDT", "READYUSDT"]
    ready = next(e for e in doc.tier1_warm_watch if e.symbol == "READYUSDT")
    assert ready.sticky_source == "daemon_state"
    assert ready.retained_reason == "ready_cache"


def test_micro_router_missing_input_returns_error_json(tmp_path: Path) -> None:
    tmp_path.joinpath("DATA/micro").mkdir(parents=True)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"
    code = run_micro_target_router(project_root=tmp_path, output_path=out)
    assert code != 0
    assert out.is_file()
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.status == "error"
    assert doc.router_computed_input_snapshot_age_sec == -1


def test_router_rejects_when_step2_reports_young_age_but_snapshot_is_old(tmp_path: Path) -> None:
    stale_snap = "2026-06-01T11:54:00Z"
    raw = _tier("raw_candidate", status="ok", signals=[], snap=stale_snap)
    watch = _tier(
        "watch_candidate",
        status="ok",
        signals=[_sig(symbol="BUSDT", base="B", state="watch_candidate")],
        snap=stale_snap,
        age=3,
    )
    strong = _tier(
        "strong_candidate",
        status="ok",
        signals=[_sig(symbol="CUSDT", base="C", state="strong_candidate", scan_score=90)],
        snap=stale_snap,
        age=3,
    )
    _write_three(tmp_path, raw, watch, strong)
    out = tmp_path / "DATA" / "micro" / "micro_targets.json"
    assert run_micro_target_router(project_root=tmp_path, output_path=out) == 0
    with open(out, encoding="utf-8") as fp:
        doc = MicroTargetsDocument.model_validate(json.load(fp))
    assert doc.status == "stale_input"
    assert doc.router_freshness_ok is False
    assert doc.input_snapshot_age_sec == 360
    assert doc.router_computed_input_snapshot_age_sec == 360
    assert doc.step2_reported_input_snapshot_age_sec == 3
    assert doc.routed_counts.tier1 == 0
    assert doc.routed_counts.tier2 == 0
    assert doc.tier1_warm_watch == []
    assert doc.tier2_active_strong == []
    assert "watch_input_stale" in doc.skip_reasons
    assert "strong_input_stale" in doc.skip_reasons
