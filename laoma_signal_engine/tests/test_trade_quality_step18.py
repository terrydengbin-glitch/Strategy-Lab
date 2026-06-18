from __future__ import annotations

import sqlite3
from pathlib import Path

from laoma_signal_engine.paper.candles import StaticCandleProvider
from laoma_signal_engine.paper.engine import PaperEngine
from laoma_signal_engine.paper.models import Candle, PaperConfig
from laoma_signal_engine.trade_quality import analyze_paper_trades
from laoma_signal_engine.trade_quality.archive_backfill import (
    archive_backfill_payload,
    load_samples_from_db,
)
from laoma_signal_engine.trade_quality.recommendation_rules import (
    recommendation_rules_payload,
    rebuild_recommendation_rules,
)
from laoma_signal_engine.trade_quality.recommendation_validation import recommendation_validation_payload
from laoma_signal_engine.trade_quality.promotion_policy import (
    apply_promotion,
    disable_promotion,
    promotion_dry_run,
    promotions_payload,
)
from laoma_signal_engine.trade_quality.promotion_candidates import (
    promotion_candidates_payload,
    rebuild_promotion_candidates,
)
from laoma_signal_engine.trade_quality.replay_backfill import (
    replay_backfill_ledger_rows,
    replay_backfill_payload,
)


def _config() -> PaperConfig:
    return PaperConfig(
        db_path="DATA/paper/test_trade_quality.db",
        summary_path="DATA/paper/latest_paper_state.json",
        default_slippage_bps=0,
        taker_fee_bps=0,
    )


def _doc(
    line: str,
    *,
    symbol: str,
    side: str,
    entry: float,
    sl: float,
    tp: float,
    run_id: str = "run_step18",
) -> dict:
    source = {
        "without_micro": "trade_plan_without_micro",
        "micro_fast": "trade_plan_micro_fast",
        "micro_full": "trade_plan_micro_full",
    }[line]
    mode = {"without_micro": "none", "micro_fast": "fast", "micro_full": "full"}[line]
    return {
        "schema_version": "1.0",
        "generated_at": "2026-06-03T00:00:00Z",
        "run_id": run_id,
        "cycle_id": f"cycle_{run_id}",
        "source": source,
        "micro_mode": mode,
        "status": "ok",
        "count": 1,
        "executable_count": 1,
        "input_refs": {},
        "plans": [
            {
                "symbol": symbol,
                "decision_tf": "15m",
                "decision": side,
                "action": "ENTER_MARKET",
                "entry_mode": "MARKET",
                "estimated_entry_price": entry,
                "stop_loss": sl,
                "take_profit": tp,
                "risk_per_unit": abs(entry - sl),
                "reward_per_unit": abs(tp - entry),
                "rr": abs(tp - entry) / abs(entry - sl),
                "executable": True,
                "confidence": 80,
                "reason_codes": [],
                "guards": {
                    "line": line,
                    "margin_usdt": 100,
                    "leverage": 20,
                    **(
                        {}
                        if line == "without_micro"
                        else {
                            "micro_symbol_confirmed": True,
                            "micro_direction_confirmed": True,
                            "micro_exec_allowed": True,
                            "micro_exec_allowed_reason": "allowed",
                            "trade_plan_consumable": True,
                        }
                    ),
                },
                "input_refs": {"source_plan_hash": f"{run_id}_{line}_{symbol}_{side}"},
            }
        ],
    }


def _close_trade(tmp_path: Path, doc: dict, entry_candle: Candle, exit_candle: Candle, replay: list[Candle]) -> dict:
    symbol = doc["plans"][0]["symbol"]
    engine = PaperEngine(tmp_path, config=_config(), candle_provider=StaticCandleProvider({symbol: [entry_candle]}))
    first = engine.tick({doc["source"].replace("trade_plan_", ""): doc})
    assert first["entries"]["entered"] == 1
    engine.candle_provider = StaticCandleProvider({symbol: [exit_candle]})
    second = engine.tick({})
    assert second["closes"]["closed"] == 1
    return analyze_paper_trades(tmp_path, config=_config(), candle_provider=StaticCandleProvider({symbol: replay}))


def test_step18_ledger_computes_long_r_mfe_mae_and_persists(tmp_path: Path) -> None:
    doc = _doc("without_micro", symbol="LONGUSDT", side="LONG", entry=100, sl=95, tp=110)
    result = _close_trade(
        tmp_path,
        doc,
        Candle("LONGUSDT", 1, 100, 101, 99, 100),
        Candle("LONGUSDT", 2, 100, 112, 97, 110),
        [Candle("LONGUSDT", 1, 100, 101, 99, 100), Candle("LONGUSDT", 2, 100, 112, 97, 110)],
    )

    sample = result["samples"][0]
    assert sample["side"] == "LONG"
    assert sample["exit_reason"] == "TP"
    assert sample["initial_risk_usdt"] > 0
    assert round(sample["planned_RR"], 4) == 2.0
    assert sample["net_R"] > 1.9
    assert round(sample["MFE_R"], 4) == 2.4
    assert round(sample["MAE_R"], 4) == 0.6
    assert sample["root_cause_label"] == "tp_hit_good_trade"

    db_path = tmp_path / _config().db_path
    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT count(*) FROM trade_quality_samples").fetchone()[0]
        agg_count = conn.execute("SELECT count(*) FROM trade_quality_aggregates").fetchone()[0]
    assert count == 1
    assert agg_count >= 4


def test_step18_ledger_computes_short_r_mfe_mae_and_labels_stop(tmp_path: Path) -> None:
    doc = _doc("micro_fast", symbol="SHORTUSDT", side="SHORT", entry=100, sl=105, tp=90)
    result = _close_trade(
        tmp_path,
        doc,
        Candle("SHORTUSDT", 1, 100, 101, 99, 100),
        Candle("SHORTUSDT", 2, 100, 106, 96, 105),
        [Candle("SHORTUSDT", 1, 100, 101, 99, 100), Candle("SHORTUSDT", 2, 100, 106, 96, 105)],
    )

    sample = result["samples"][0]
    assert sample["side"] == "SHORT"
    assert sample["exit_reason"] == "SL"
    assert sample["net_R"] < -0.9
    assert round(sample["MFE_R"], 4) == 0.8
    assert round(sample["MAE_R"], 4) == 1.2
    assert sample["root_cause_label"] in {"stop_too_tight", "direction_wrong"}
    assert any(item["action_id"].startswith("trade_quality_") for item in result["recommendations"])


def test_step18_aggregation_keeps_strategy_line_and_root_cause_dimensions(tmp_path: Path) -> None:
    long_doc = _doc("without_micro", symbol="LONG2USDT", side="LONG", entry=10, sl=9, tp=12, run_id="run_a")
    _close_trade(
        tmp_path,
        long_doc,
        Candle("LONG2USDT", 1, 10, 10.2, 9.8, 10),
        Candle("LONG2USDT", 2, 10, 12.2, 9.8, 12),
        [Candle("LONG2USDT", 1, 10, 10.2, 9.8, 10), Candle("LONG2USDT", 2, 10, 12.2, 9.8, 12)],
    )
    short_doc = _doc("micro_fast", symbol="SHORT2USDT", side="SHORT", entry=10, sl=10.5, tp=9, run_id="run_b")
    result = _close_trade(
        tmp_path,
        short_doc,
        Candle("SHORT2USDT", 1, 10, 10.1, 9.9, 10),
        Candle("SHORT2USDT", 2, 10, 10.6, 9.6, 10.5),
        [Candle("SHORT2USDT", 1, 10, 10.1, 9.9, 10), Candle("SHORT2USDT", 2, 10, 10.6, 9.6, 10.5)],
    )

    keys = {(row["dimension"], row["key"]) for row in result["aggregates"]}
    assert ("strategy_line", "without_micro") in keys
    assert ("strategy_line", "micro_fast") in keys
    assert ("side", "LONG") in keys
    assert ("side", "SHORT") in keys
    assert any(dim == "root_cause" for dim, _ in keys)


def _archive_order(order_id: str, *, status: str = "closed", symbol: str = "ARCHUSDT") -> dict:
    return {
        "id": order_id,
        "strategy_line": "without_micro",
        "symbol": symbol,
        "side": "LONG",
        "status": status,
        "entry_price": 100,
        "filled_entry_price": 100,
        "exit_price": 110,
        "stop_loss": 95,
        "take_profit": 110,
        "quantity": 2,
        "realized_pnl_usdt": 20,
        "fee_usdt": 0,
        "slippage_usdt": 0,
        "source_run_id": "run_archive",
        "source_cycle_id": "cycle_archive",
        "source_plan_hash": f"plan_{order_id}",
        "opened_at": "2026-06-03T00:00:00Z",
        "closed_at": "2026-06-03T00:10:00Z",
        "exit_reason": "TP",
    }


def _write_archive(tmp_path: Path, orders: list[dict]) -> Path:
    archive_dir = tmp_path / "archives" / "paper_exp_20260603T000000Z_without_micro"
    archive_dir.mkdir(parents=True)
    (archive_dir / "metadata.json").write_text(
        '{"schema_version":"14.31","profile_name":"relaxed_profit"}',
        encoding="utf-8",
    )
    (archive_dir / "orders.json").write_text(__import__("json").dumps(orders), encoding="utf-8")
    fills = [
        {"order_id": row["id"], "action": "entry", "fill_price": 100, "filled_at": row.get("opened_at")}
        for row in orders
        if row.get("status") == "closed"
    ]
    fills.extend(
        [
            {
                "order_id": row["id"],
                "action": "take_profit",
                "fill_price": 110,
                "gross_pnl_usdt": 20,
                "net_pnl_usdt": 20,
                "filled_at": row.get("closed_at"),
            }
            for row in orders
            if row.get("status") == "closed"
        ]
    )
    (archive_dir / "fills.json").write_text(__import__("json").dumps(fills), encoding="utf-8")
    return tmp_path / "archives"


def _replay_order(
    order_id: str,
    *,
    symbol: str,
    side: str,
    entry: float,
    sl: float,
    tp: float,
    exit_price: float,
    exit_reason: str = "SL",
) -> dict:
    row = _archive_order(order_id, symbol=symbol)
    row.update(
        {
            "side": side,
            "entry_price": entry,
            "filled_entry_price": entry,
            "stop_loss": sl,
            "take_profit": tp,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "quantity": 2,
            "realized_pnl_usdt": (exit_price - entry) * 2 if side == "LONG" else (entry - exit_price) * 2,
            "opened_at": "1970-01-01T00:00:00Z",
            "closed_at": "1970-01-01T00:02:00Z",
        }
    )
    return row


def test_step189_archive_backfill_dedup_idempotent(tmp_path: Path) -> None:
    archive_root = _write_archive(tmp_path, [_archive_order("arch_1"), _archive_order("arch_1")])
    dry = archive_backfill_payload(tmp_path, write=False, archive_root=archive_root, config=_config())
    assert dry["closed_orders_seen"] == 1
    assert dry["samples_inserted"] == 0
    assert dry["duplicates_skipped"] == 1

    first = archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    second = archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    db_path = tmp_path / _config().db_path
    with sqlite3.connect(db_path) as conn:
        sample_count = conn.execute("SELECT count(*) FROM trade_quality_samples").fetchone()[0]
        ledger_count = conn.execute("SELECT count(*) FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted'").fetchone()[0]
    assert first["samples_inserted"] == 1
    assert second["samples_inserted"] == 0
    assert sample_count == 1
    assert ledger_count == 1


def test_step189_archive_backfill_skips_partial_records(tmp_path: Path) -> None:
    partial = _archive_order("arch_partial", status="open")
    archive_root = _write_archive(tmp_path, [partial])
    result = archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    assert result["samples_inserted"] == 0
    assert result["partial_records"] == 1
    assert result["reason_counts"]["order_not_closed"] == 1


def test_step189_archive_backfill_preserves_version_tags_and_source(tmp_path: Path) -> None:
    archive_root = _write_archive(tmp_path, [_archive_order("arch_2", symbol="VERSUSDT")])
    archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    db_path = tmp_path / _config().db_path
    samples = load_samples_from_db(db_path, sample_source="archive")
    assert len(samples) == 1
    evidence = samples[0].root_cause_evidence
    assert evidence["sample_source"] == "archive"
    assert evidence["archive_schema_version"] == "14.31"
    assert evidence["config_profile"] == "relaxed_profit"
    assert load_samples_from_db(db_path, sample_source="live") == []


def test_step1810_recommendation_rules_rebuild_idempotent(tmp_path: Path) -> None:
    archive_root = _write_archive(
        tmp_path,
        [
            _archive_order(f"arch_win_{idx}", symbol="GOODUSDT")
            for idx in range(6)
        ]
        + [
            {**_archive_order(f"arch_loss_{idx}", symbol="BADUSDT"), "exit_price": 95, "exit_reason": "SL", "realized_pnl_usdt": -10}
            for idx in range(12)
        ],
    )
    archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    db_path = tmp_path / _config().db_path

    first = rebuild_recommendation_rules(db_path)
    second = rebuild_recommendation_rules(db_path)
    got = recommendation_rules_payload(db_path, limit=500)

    assert first["rule_count"] == second["rule_count"]
    assert got["summary"]["mode_counts"]["shadow"] >= 1
    assert set(got["summary"]["mode_counts"]) <= {"shadow", "warn"}


def test_step1810_direction_cost_symbol_rules(tmp_path: Path) -> None:
    bad_orders = []
    for idx in range(12):
        row = _archive_order(f"bad_{idx}", symbol="BADDIRUSDT")
        row.update({"exit_price": 95, "exit_reason": "SL", "realized_pnl_usdt": -10})
        bad_orders.append(row)
    cost_orders = []
    for idx in range(5):
        row = _archive_order(f"cost_{idx}", symbol="COSTUSDT")
        row.update({"exit_price": 95, "exit_reason": "SL", "realized_pnl_usdt": -10, "fee_usdt": 5, "slippage_usdt": 2})
        cost_orders.append(row)
    archive_root = _write_archive(tmp_path, bad_orders + cost_orders)
    archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    db_path = tmp_path / _config().db_path
    rebuild_recommendation_rules(db_path)

    direction = recommendation_rules_payload(db_path, rule_type="direction_gate", sample_source="archive", limit=50)
    cost = recommendation_rules_payload(db_path, rule_type="cost_liquidity", symbol="COSTUSDT", limit=50)
    symbol = recommendation_rules_payload(db_path, rule_type="symbol_quality_tier", symbol="BADDIRUSDT", limit=50)

    assert any(row["recommendation"] in {"direction_warn_review", "direction_shadow_block"} for row in direction["rules"])
    assert any(row["recommendation"] == "cost_shadow_blacklist" for row in cost["rules"])
    assert any(row["recommendation"] == "quality_shadow_blacklist" for row in symbol["rules"])


def test_step1811_recommendation_validation_matches_fresh_live_samples(tmp_path: Path) -> None:
    archive_root = _write_archive(
        tmp_path,
        [
            {**_archive_order(f"arch_bad_{idx}", symbol="VALUSDT"), "exit_price": 95, "exit_reason": "SL", "realized_pnl_usdt": -10}
            for idx in range(12)
        ],
    )
    archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    live_doc = _doc("without_micro", symbol="VALUSDT", side="LONG", entry=100, sl=95, tp=110, run_id="run_live_val")
    _close_trade(
        tmp_path,
        live_doc,
        Candle("VALUSDT", 1, 100, 101, 99, 100),
        Candle("VALUSDT", 2, 100, 106, 94, 95),
        [Candle("VALUSDT", 1, 100, 101, 99, 100), Candle("VALUSDT", 2, 100, 106, 94, 95)],
    )
    db_path = tmp_path / _config().db_path
    rebuild_recommendation_rules(db_path)

    result = recommendation_validation_payload(db_path, sample_source="live", symbol="VALUSDT")

    assert result["sample_count"] >= 1
    assert result["matched_count"] >= 1
    assert result["summary"]["rule_hit_count"] >= 1
    assert all(row["rule_mode"] in {"shadow", "warn"} for row in result["matches"])


def test_step1812_promotion_policy_dry_run_apply_disable(tmp_path: Path) -> None:
    archive_root = _write_archive(
        tmp_path,
        [
            {
                **_archive_order(f"arch_cost_{idx}", symbol="PROMOUSDT"),
                "exit_price": 95,
                "exit_reason": "SL",
                "realized_pnl_usdt": -10,
                "fee_usdt": 5,
                "slippage_usdt": 2,
            }
            for idx in range(5)
        ],
    )
    archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    db_path = tmp_path / _config().db_path
    rebuild_recommendation_rules(db_path)
    rules = recommendation_rules_payload(db_path, rule_type="cost_liquidity", symbol="PROMOUSDT", limit=10)["rules"]
    rule_id = rules[0]["rule_id"]

    dry = promotion_dry_run(db_path, rule_id=rule_id, profile="relaxed_profit", strategy_line="without_micro", mode="wait_only")
    applied = apply_promotion(
        db_path,
        rule_id=rule_id,
        profile="relaxed_profit",
        strategy_line="without_micro",
        mode="wait_only",
        reason="test_apply",
    )
    promotions = promotions_payload(db_path)
    disabled = disable_promotion(db_path, promotion_id=applied["promotion_id"], reason="test_disable")
    after = promotions_payload(db_path)

    assert dry["would_write"] is False
    assert applied["status"] == "applied_shadow_contract"
    assert promotions["summary"]["enabled"] == 1
    assert disabled["status"] == "disabled"
    assert after["summary"]["enabled"] == 0


def test_step1814_promotion_candidates_are_wait_only_dry_run_only(tmp_path: Path) -> None:
    archive_root = _write_archive(
        tmp_path,
        [
            {
                **_archive_order(f"arch_cost_loss_{idx}", symbol="CANDUSDT"),
                "exit_price": 95,
                "exit_reason": "SL",
                "realized_pnl_usdt": -10,
                "fee_usdt": 5,
                "slippage_usdt": 2,
            }
            for idx in range(6)
        ]
        + [
            {
                **_archive_order(f"arch_cost_win_{idx}", symbol="CANDUSDT"),
                "exit_price": 110,
                "exit_reason": "TP",
                "realized_pnl_usdt": 20,
                "fee_usdt": 1,
                "slippage_usdt": 0,
            }
            for idx in range(2)
        ],
    )
    archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    db_path = tmp_path / _config().db_path
    rebuild_recommendation_rules(db_path)

    result = rebuild_promotion_candidates(db_path, write=True, limit=20)
    payload = promotion_candidates_payload(db_path, limit=20)
    promotions = promotions_payload(db_path)

    assert result["candidate_count"] >= 1
    assert payload["count"] == result["candidate_count"]
    assert all(row["mode"] == "wait_only" for row in payload["candidates"])
    assert {row["rule_type"] for row in payload["candidates"]} <= {"cost_liquidity", "symbol_quality_tier"}
    assert all(row["net_saved_R"] > 0 for row in payload["candidates"])
    assert promotions["summary"]["enabled"] == 0


def test_step1816_replay_long_sl_high_mfe_becomes_stop_too_tight(tmp_path: Path) -> None:
    order = _replay_order(
        "replay_long_stop",
        symbol="RPLAYLUSDT",
        side="LONG",
        entry=100,
        sl=95,
        tp=110,
        exit_price=95,
    )
    archive_root = _write_archive(tmp_path, [order])
    archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    db_path = tmp_path / _config().db_path
    before = load_samples_from_db(db_path, sample_source="archive")[0]
    assert before.excursion_model == "outcome_proxy_no_candle_replay"
    assert before.root_cause_label == "direction_wrong"

    candles = [
        Candle("RPLAYLUSDT", 0, 100, 101, 99, 100),
        Candle("RPLAYLUSDT", 60_000, 100, 105.5, 98, 104),
        Candle("RPLAYLUSDT", 120_000, 100, 101, 94, 95),
    ]
    result = replay_backfill_payload(
        tmp_path,
        write=True,
        config=_config(),
        candle_provider=StaticCandleProvider({"RPLAYLUSDT": candles}),
    )
    after = load_samples_from_db(db_path, sample_source="archive")[0]

    assert result["updated_samples"] == 1
    assert after.excursion_model == "candle_1m_replay"
    assert round(after.MFE_R or 0, 2) >= 1.0
    assert after.root_cause_label == "stop_too_tight"


def test_step1816_replay_short_mfe_mae_side_aware(tmp_path: Path) -> None:
    order = _replay_order(
        "replay_short_stop",
        symbol="RPLAYSUSDT",
        side="SHORT",
        entry=100,
        sl=105,
        tp=90,
        exit_price=105,
    )
    archive_root = _write_archive(tmp_path, [order])
    archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())

    candles = [
        Candle("RPLAYSUSDT", 0, 100, 101, 99, 100),
        Candle("RPLAYSUSDT", 60_000, 100, 101, 96, 98),
        Candle("RPLAYSUSDT", 120_000, 100, 106, 99, 105),
    ]
    result = replay_backfill_payload(
        tmp_path,
        write=True,
        config=_config(),
        candle_provider=StaticCandleProvider({"RPLAYSUSDT": candles}),
    )
    sample = load_samples_from_db(tmp_path / _config().db_path, sample_source="archive")[0]

    assert result["updated_samples"] == 1
    assert round(sample.MFE_R or 0, 4) == 0.8
    assert round(sample.MAE_R or 0, 4) == 1.2
    assert sample.root_cause_label == "stop_too_tight"


def test_step1816_replay_low_mfe_becomes_signal_no_edge_and_idempotent(tmp_path: Path) -> None:
    order = _replay_order(
        "replay_no_edge",
        symbol="RPLAYNUSDT",
        side="LONG",
        entry=100,
        sl=95,
        tp=110,
        exit_price=95,
    )
    archive_root = _write_archive(tmp_path, [order])
    archive_backfill_payload(tmp_path, write=True, archive_root=archive_root, config=_config())
    candles = [
        Candle("RPLAYNUSDT", 0, 100, 100.5, 99, 100),
        Candle("RPLAYNUSDT", 60_000, 100, 100.2, 97, 98),
        Candle("RPLAYNUSDT", 120_000, 100, 100.1, 94, 95),
    ]
    first = replay_backfill_payload(
        tmp_path,
        write=True,
        config=_config(),
        candle_provider=StaticCandleProvider({"RPLAYNUSDT": candles}),
    )
    second = replay_backfill_payload(
        tmp_path,
        write=True,
        config=_config(),
        candle_provider=StaticCandleProvider({"RPLAYNUSDT": candles}),
    )
    db_path = tmp_path / _config().db_path
    sample = load_samples_from_db(db_path, sample_source="archive")[0]
    ledger = replay_backfill_ledger_rows(db_path, limit=10)

    assert first["updated_samples"] == 1
    assert second["updated_samples"] == 0
    assert sample.root_cause_label == "signal_no_edge"
    assert len(ledger) == 1
