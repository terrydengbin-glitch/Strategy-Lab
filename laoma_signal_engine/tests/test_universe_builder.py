"""Universe merge and ranking (mocked inputs)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from laoma_signal_engine.universe.cache import universe_cache_is_fresh
from laoma_signal_engine.universe.candidate_universe import build_candidate_document_from_maps
from laoma_signal_engine.universe.step15_symbols import futures_symbols_for_step_1_5


def test_merge_ranks_and_tags() -> None:
    spot = {"BTC": "BTCUSDT", "AAA": "AAAUSDT"}
    fut = {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}
    tick = {"BTCUSDT": (100.0, 5.0), "ETHUSDT": (200.0, -3.0)}
    manual = {"ZZZ"}
    gen = datetime(2026, 5, 10, 0, 5, 0, tzinfo=UTC)

    doc = build_candidate_document_from_maps(
        spot_by_base=spot,
        futures_by_base=fut,
        fut_ticker_by_symbol=tick,
        manual_bases=manual,
        exclude_spot=set(),
        exclude_futures=set(),
        schema_version="1.6",
        source="binance",
        ttl_seconds=86400,
        top_tag_rank=10,
        generated_at=gen,
    )

    assert doc.counts.total_pairs == len(doc.pairs)
    assert doc.counts.both_spot_and_futures + doc.counts.futures_only == doc.counts.futures_count
    assert doc.counts.both_spot_and_futures + doc.counts.spot_only == doc.counts.spot_count
    assert (
        doc.counts.both_spot_and_futures
        + doc.counts.futures_only
        + doc.counts.spot_only
        + doc.counts.neither_spot_nor_futures
        == doc.counts.total_pairs
    )

    by_base = {p.base_asset: p for p in doc.pairs}

    assert by_base["BTC"].display_base_asset == "BTC"
    assert by_base["BTC"].cashtag == "$BTC"
    assert by_base["BTC"].spot_cashtag_symbol == "BTCUSDT"
    assert by_base["BTC"].symbol_safe_id == "BTCUSDT"
    assert by_base["BTC"].eligible_for_signal_engine is True
    assert by_base["BTC"].eligible_for_post is True
    assert by_base["BTC"].universe_profile.business_pool == "watch_only"
    assert by_base["BTC"].universe_profile.scan_eligibility == "observe_only"
    assert by_base["BTC"].risk_profile.sl_template == "wide"
    assert by_base["BTC"].risk_profile.sizing_template == "micro"

    assert by_base["AAA"].eligible_for_post is False
    assert by_base["AAA"].eligible_for_signal_engine is False

    assert by_base["ETH"].rank_futures_volume == 1
    assert by_base["ETH"].eligible_for_post is False

    assert by_base["BTC"].rank_futures_volume == 2
    assert by_base["BTC"].rank_futures_gainer == 1
    assert by_base["ETH"].rank_futures_gainer is None
    assert by_base["ETH"].rank_futures_loser == 1
    assert "manual_watchlist" in by_base["ZZZ"].source_tags
    assert by_base["ZZZ"].has_um_futures is False
    assert by_base["ZZZ"].eligible_for_signal_engine is False
    assert doc.counts.neither_spot_nor_futures == 1


def test_exclude_drops_leg() -> None:
    spot = {"BTC": "BTCUSDT"}
    fut = {"BTC": "BTCUSDT"}
    tick = {"BTCUSDT": (50.0, 1.0)}
    doc = build_candidate_document_from_maps(
        spot_by_base=spot,
        futures_by_base=fut,
        fut_ticker_by_symbol=tick,
        manual_bases=set(),
        exclude_spot={"BTCUSDT"},
        exclude_futures=set(),
        schema_version="1.6",
        source="binance",
        ttl_seconds=86400,
        top_tag_rank=10,
        generated_at=datetime(2026, 5, 10, 0, 5, 0, tzinfo=UTC),
    )
    row = next(p for p in doc.pairs if p.base_asset == "BTC")
    assert row.has_spot is False
    assert row.has_um_futures is True
    assert row.eligible_for_post is False
    assert row.symbol_safe_id == "BTCUSDT"


def test_multiplier_row_mapping() -> None:
    spot = {"1000PEPE": "1000PEPEUSDT"}
    fut = {"1000PEPE": "1000PEPEUSDT"}
    tick = {"1000PEPEUSDT": (1e6, 2.5)}
    doc = build_candidate_document_from_maps(
        spot_by_base=spot,
        futures_by_base=fut,
        fut_ticker_by_symbol=tick,
        manual_bases=set(),
        exclude_spot=set(),
        exclude_futures=set(),
        schema_version="1.6",
        source="binance",
        ttl_seconds=86400,
        top_tag_rank=10,
        generated_at=datetime(2026, 5, 10, 0, 5, 0, tzinfo=UTC),
    )
    row = next(p for p in doc.pairs if p.base_asset == "1000PEPE")
    assert row.display_base_asset == "PEPE"
    assert row.cashtag == "$PEPE"
    assert row.spot_cashtag_symbol == "PEPEUSDT"
    assert row.symbol_safe_id == "1000PEPEUSDT"
    assert row.universe_profile.contract_multiplier == 1000
    assert row.universe_profile.is_multiplier_contract is True
    assert "multiplier_contract" in row.universe_profile.symbol_risk_tags
    assert row.risk_profile.rr_policy == "conservative"
    assert row.universe_profile.business_pool == "watch_only"
    assert row.risk_profile.sizing_template == "micro"


def test_manual_entry_no_trade_blocks_static_profile() -> None:
    from laoma_signal_engine.core.models import ManualWatchlistEntry

    fut = {"ABC": "ABCUSDT", "XYZ": "XYZUSDT"}
    tick = {"ABCUSDT": (100_000_000.0, 2.0), "XYZUSDT": (100_000_000.0, 2.0)}
    doc = build_candidate_document_from_maps(
        spot_by_base={},
        futures_by_base=fut,
        fut_ticker_by_symbol=tick,
        manual_bases={"ABC"},
        manual_entries={"ABC": ManualWatchlistEntry(base="ABC", mode="no_trade", priority=90, reason="manual block")},
        exclude_spot=set(),
        exclude_futures=set(),
        schema_version="1.6",
        source="binance",
        ttl_seconds=86400,
        top_tag_rank=10,
        generated_at=datetime(2026, 5, 10, 0, 5, 0, tzinfo=UTC),
    )
    by_base = {p.base_asset: p for p in doc.pairs}
    assert by_base["ABC"].eligible_for_signal_engine is False
    assert by_base["ABC"].universe_profile.manual_mode == "no_trade"
    assert by_base["ABC"].universe_profile.business_pool == "no_trade"
    assert by_base["ABC"].universe_profile.scan_eligibility == "block"
    assert by_base["ABC"].risk_profile.execution_tier == "no_trade"
    assert by_base["ABC"].risk_profile.sizing_template == "disabled"
    assert by_base["XYZ"].eligible_for_signal_engine is True


def test_step_1_5_symbol_list() -> None:
    spot = {"BTC": "BTCUSDT", "AAA": "AAAUSDT"}
    fut = {"BTC": "BTCUSDT"}
    tick = {"BTCUSDT": (10.0, 1.0)}
    doc = build_candidate_document_from_maps(
        spot_by_base=spot,
        futures_by_base=fut,
        fut_ticker_by_symbol=tick,
        manual_bases=set(),
        exclude_spot=set(),
        exclude_futures=set(),
        schema_version="1.6",
        source="binance",
        ttl_seconds=86400,
        top_tag_rank=10,
        generated_at=datetime(2026, 5, 10, 0, 5, 0, tzinfo=UTC),
    )
    syms = futures_symbols_for_step_1_5(doc)
    assert syms == ["BTCUSDT"]
    assert doc.counts.total_pairs > len(syms)


def test_universe_cache_fresh(tmp_path: Path) -> None:
    p = tmp_path / "CANDIDATE_UNIVERSE.json"
    now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
    exp = now + timedelta(hours=1)
    doc = {
        "schema_version": "1.6",
        "generated_at": "2026-05-11T12:00:00Z",
        "expires_at": exp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": 0,
        "pairs": [],
    }
    p.write_text(json.dumps(doc), encoding="utf-8")
    assert universe_cache_is_fresh(p, "1.6", now) is True
    assert universe_cache_is_fresh(p, "1.5", now) is False


def test_universe_cache_rejects_fresh_file_missing_business_profile(tmp_path: Path) -> None:
    p = tmp_path / "CANDIDATE_UNIVERSE.json"
    now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
    exp = now + timedelta(hours=1)
    doc = {
        "schema_version": "1.6",
        "generated_at": "2026-05-11T12:00:00Z",
        "expires_at": exp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": 1,
        "pairs": [{"base_asset": "BTC", "futures_symbol": "BTCUSDT", "has_um_futures": True}],
    }
    p.write_text(json.dumps(doc), encoding="utf-8")
    assert universe_cache_is_fresh(p, "1.6", now) is False
