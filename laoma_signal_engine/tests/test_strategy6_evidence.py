from __future__ import annotations

import sqlite3
from pathlib import Path

from laoma_signal_engine.core.json_io import write_json_atomic
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanItem, TradePlanLineDocument
from laoma_signal_engine.paper.adapter import adapt_documents, load_trade_plan_documents
from laoma_signal_engine.strategy6.evidence import (
    paths,
    run_strategy6_observe_once,
    score_strategy6,
    strategy6_daemon_status,
    strategy6_watchdog,
    write_daemon_heartbeat,
    write_strategy6_outputs,
)


def _write_fixture_docs(root: Path) -> None:
    factor = {
        "schema_version": "1.6",
        "generated_at": "2026-06-11T00:00:00Z",
        "source": "factor_snapshot_without_ofi_cvd",
        "items": [
            {
                "symbol": "BTCUSDT",
                "move_side": "up",
                "primary_15m": {
                    "price_ret": 1.0,
                    "volume_ratio": 2.0,
                    "taker_buy_ratio": 0.62,
                    "kline_cvd_state": "buy_dominant",
                    "range_pos": 0.55,
                },
                "trigger_5m": {"price_ret": 0.35, "breakout_state": "local_breakout"},
                "entry_1m": {"price_ret": 0.08, "atr": 10.0, "distance_to_vwap_bps": 2.0},
            }
        ],
    }
    light = {
        "schema_version": "test",
        "generated_at": "2026-06-11T00:00:00Z",
        "source": "fixture_futures_light_snapshot",
        "items": [{"symbol": "BTCUSDT"}],
    }
    plan = TradePlanLineDocument(
        generated_at="2026-06-11T00:00:05Z",
        run_id="BASE_RUN",
        cycle_id="cycle_BASE_RUN",
        source="trade_plan_without_micro",
        micro_mode="none",
        status="ok",
        count=1,
        executable_count=1,
        plans=[
            TradePlanItem(
                symbol="BTCUSDT",
                decision="LONG",
                action="ENTER_MARKET",
                entry_mode="MARKET",
                estimated_entry_price=100.0,
                stop_loss=99.0,
                take_profit=101.0,
                risk_per_unit=1.0,
                reward_per_unit=1.0,
                rr=1.0,
                executable=True,
                confidence=80,
                reason_codes=["fixture"],
                guards={"line": "without_micro"},
                position_sizing={"quantity": 1.0, "notional_usdt": 100.0, "margin_usdt": 10.0},
            )
        ],
    ).model_dump(mode="json")
    write_json_atomic(root / "DATA" / "factors" / "latest_factor_snapshot_withoutoficvd.json", factor)
    write_json_atomic(root / "DATA" / "market" / "futures_light_snapshot.json", light)
    write_json_atomic(root / "DATA" / "decisions" / "latest_trade_plan_without_micro.json", plan)


def _write_wait_fixture_docs(root: Path) -> None:
    _write_fixture_docs(root)
    factor = {
        "schema_version": "1.6",
        "generated_at": "2026-06-11T00:00:00Z",
        "source": "factor_snapshot_without_ofi_cvd",
        "items": [
            {
                "symbol": "BTCUSDT",
                "move_side": "up",
                "primary_15m": {
                    "price_ret": 1.0,
                    "volume_ratio": 2.0,
                    "taker_buy_ratio": 0.62,
                    "kline_cvd_state": "buy_dominant",
                    "range_pos": 0.96,
                },
                "trigger_5m": {"price_ret": 0.35, "breakout_state": "local_breakout"},
                "entry_1m": {"price_ret": 0.08, "atr": 10.0, "distance_to_vwap_bps": 160.0},
            }
        ],
    }
    write_json_atomic(root / "DATA" / "factors" / "latest_factor_snapshot_withoutoficvd.json", factor)


def test_strategy6_generates_trade_plan_ledger_and_paper_intent(tmp_path: Path) -> None:
    _write_fixture_docs(tmp_path)

    result = write_strategy6_outputs(tmp_path, run_id="RUN6", cycle_id="cycle_RUN6")

    assert result["status"] == "ok"
    assert result["count"] == 1
    assert Path(result["output_path"]).is_file()
    assert Path(result["archive_path"]).is_file()
    doc = paths(tmp_path).latest_trade_plan.read_text(encoding="utf-8")
    assert "trade_plan_strategy6" in doc
    assert "strategy6_evidence_id" in doc
    assert "strategy6_market_accepted" in doc

    with sqlite3.connect(paths(tmp_path).db) as con:
        rows = con.execute(
            "select symbol, legacy_side, decision_state, executable from strategy6_evidence",
        ).fetchall()
    assert rows == [("BTCUSDT", "LONG", "EXECUTABLE", 1)]

    docs = load_trade_plan_documents(tmp_path)
    adapted = adapt_documents({"strategy6": docs["strategy6"]})
    assert len(adapted["intents"]) == 1
    intent = adapted["intents"][0]
    assert intent.strategy_line == "strategy6"
    assert intent.signal_class == "strategy6_market_accepted_entry"
    assert intent.source_run_id == "RUN6"


def test_strategy6_observe_pool_rechecks_wait_using_original_base_plan(tmp_path: Path) -> None:
    _write_wait_fixture_docs(tmp_path)

    result = write_strategy6_outputs(tmp_path, run_id="RUN6_WAIT", cycle_id="cycle_RUN6_WAIT")

    assert result["executable_count"] == 0
    assert result["wait_count"] == 1
    with sqlite3.connect(paths(tmp_path).db) as con:
        row = con.execute(
            "select status, attempts, last_plan_json from strategy6_observe_pool where symbol='BTCUSDT'",
        ).fetchone()
        assert row is not None
        assert row[0] == "WAIT_REBOUND"
        assert int(row[1]) == 1
        assert '"executable": true' in row[2]
        con.execute(
            "update strategy6_observe_pool set next_check_at='2026-06-10T00:00:00Z' where symbol='BTCUSDT'",
        )

    _write_fixture_docs(tmp_path)
    recheck = run_strategy6_observe_once(tmp_path, run_id="RUN6_RECHECK", cycle_id="cycle_RUN6_RECHECK")

    assert recheck["executable_count"] == 1
    with sqlite3.connect(paths(tmp_path).db) as con:
        status, attempts = con.execute(
            "select status, attempts from strategy6_observe_pool where symbol='BTCUSDT'",
        ).fetchone()
        attempt_count = con.execute(
            "select count(*) from strategy6_observe_attempts where symbol='BTCUSDT'",
        ).fetchone()[0]
    assert status == "EXECUTABLE"
    assert int(attempts) == 2
    assert int(attempt_count) == 2


def test_strategy6_daemon_health_and_watchdog_contract(tmp_path: Path) -> None:
    hb = write_daemon_heartbeat(tmp_path, status="idle", pid=0, last_check_at="2026-06-11T00:00:00Z")
    assert hb["source"] == "strategy6_daemon_heartbeat"

    status = strategy6_daemon_status(tmp_path)
    assert status["source"] == "strategy6_daemon_heartbeat"
    assert status["watchdog_enabled"] is True
    assert "health_status" in status
    assert "heartbeat_age_sec" in status
    assert "stale_after_sec" in status

    stale = dict(status)
    stale["heartbeat_at"] = "2026-06-10T00:00:00Z"
    stale["pid"] = 99999999
    write_json_atomic(paths(tmp_path).daemon_heartbeat, stale)

    stale_status = strategy6_daemon_status(tmp_path)
    assert stale_status["health_status"] in {"dead_pid", "stale"}
    assert stale_status["watchdog_recommended_action"] in {"start", "restart_recommended"}
    assert stale_status["reason_codes"]

    dry_run = strategy6_watchdog(tmp_path, recover=False)
    assert dry_run["source"] == "strategy6_daemon_watchdog"
    assert dry_run["recover"] is False
    assert dry_run["action_taken"] == "none"
    with sqlite3.connect(paths(tmp_path).db) as con:
        event_count = con.execute("select count(*) from strategy6_runtime_events").fetchone()[0]
    assert event_count >= 1


def test_strategy6_v2_direction_triage_denies_immediate_reversal() -> None:
    features = {
        "legacy_side": "LONG",
        "pct_1m_bps": -30.0,
        "pct_3m_bps": -20.0,
        "pct_5m_bps": 25.0,
        "volume_z": 2.0,
        "taker_buy_ratio": 0.45,
        "range_pos": 0.5,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 5.0,
        "distance_to_ema_bps": 5.0,
        "feature_quality": {"micro_optional_missing": True},
    }
    cfg = {
        "strategy6_version": "v2",
        "min_direction_acceptance_score": 58,
        "min_entry_price_quality_score": 56,
        "min_market_acceptance_score": 58,
        "hard_deny_direction_score": 38,
        "long_max_range_pos": 0.78,
        "short_min_range_pos": 0.22,
        "max_spread_bps": 45,
        "max_abs_1m_chase_bps": 80,
        "v2_adverse_1m_deny_bps": 24,
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v2"
    assert score["direction_state"] == "denied_direction"
    assert score["decision_state"] == "DENY_DIRECTION_CONFLICT"
    assert "strategy6_v2_direction_denied" in score["reason_codes"]


def test_strategy6_v2_entry_timing_waits_for_chased_price() -> None:
    features = {
        "legacy_side": "LONG",
        "pct_1m_bps": 85.0,
        "pct_3m_bps": 90.0,
        "pct_5m_bps": 120.0,
        "volume_z": 2.5,
        "taker_buy_ratio": 0.62,
        "range_pos": 0.93,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 120.0,
        "distance_to_ema_bps": 80.0,
        "feature_quality": {"micro_optional_missing": True},
    }
    cfg = {
        "strategy6_version": "v2",
        "min_direction_acceptance_score": 58,
        "min_entry_price_quality_score": 56,
        "min_market_acceptance_score": 58,
        "hard_deny_direction_score": 38,
        "long_max_range_pos": 0.78,
        "short_min_range_pos": 0.22,
        "max_spread_bps": 45,
        "max_abs_1m_chase_bps": 80,
        "v2_max_chase_bps": 55,
        "v2_distance_from_mean_max_bps": 85,
    }

    score = score_strategy6(features, cfg)

    assert score["direction_state"] == "accepted_direction"
    assert score["entry_quality_state"] == "entry_price_needs_rebound"
    assert score["decision_state"] == "WAIT_REBOUND"
    assert score["wait_state"] == "WAIT_REBOUND"


def _strategy6_v3_cfg() -> dict[str, object]:
    return {
        "strategy6_version": "v3",
        "min_direction_acceptance_score": 58,
        "min_entry_price_quality_score": 56,
        "min_market_acceptance_score": 58,
        "hard_deny_direction_score": 38,
        "long_max_range_pos": 0.78,
        "short_min_range_pos": 0.22,
        "max_spread_bps": 45,
        "max_abs_1m_chase_bps": 80,
        "v2_min_direction_acceptance_score": 62,
        "v2_uncertain_direction_score": 48,
        "v2_hard_deny_direction_score": 38,
        "v2_max_chase_bps": 55,
        "v2_adverse_1m_deny_bps": 24,
        "v2_reversal_1m_wait_bps": 10,
        "v2_distance_from_mean_max_bps": 85,
        "v2_high_quality_score": 74,
        "v2_medium_quality_score": 62,
        "v3_min_direction_context_score": 62,
        "v3_uncertain_direction_context_score": 52,
        "v3_hard_deny_context_score": 38,
        "v3_reverse_1m_deny_bps": 12,
        "v3_reverse_3m_deny_bps": 24,
        "v3_fake_breakout_range_pos": 0.88,
        "v3_second_acceptance_min_bps": 4,
        "v3_max_entry_slippage_bps": 45,
        "v3_quality_filter_mode": "shadow",
        "v3_bad_symbols": [],
        "v3_bad_sides": [],
        "market_score_direction_weight": 0.58,
        "market_score_entry_weight": 0.42,
    }


def test_strategy6_v3_direction_context_denies_reverse_momentum() -> None:
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": -18.0,
        "pct_3m_bps": -30.0,
        "pct_5m_bps": 20.0,
        "volume_z": 2.0,
        "taker_buy_ratio": 0.52,
        "range_pos": 0.5,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 8.0,
        "distance_to_ema_bps": 8.0,
        "btc_alignment": "opposite",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, _strategy6_v3_cfg())

    assert score["strategy6_version"] == "v3"
    assert score["direction_state"] == "denied_direction"
    assert score["decision_state"] == "DENY_DIRECTION_CONFLICT"
    assert score["direction_gate_state"] == "denied"
    assert "strategy6_v3_direction_context_denied" in score["reason_codes"]


def test_strategy6_v3_entry_confirmation_waits_for_chase_tail() -> None:
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": 62.0,
        "pct_3m_bps": 88.0,
        "pct_5m_bps": 120.0,
        "volume_z": 2.8,
        "taker_buy_ratio": 0.64,
        "range_pos": 0.92,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 110.0,
        "distance_to_ema_bps": 88.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, _strategy6_v3_cfg())

    assert score["strategy6_version"] == "v3"
    assert score["decision_state"] in {"WAIT_REBOUND", "WAIT_CONFIRM"}
    assert score["entry_confirmation_state"] in {"waiting", "wait_rebound", "wait_second_acceptance"}
    assert any(code in score["reason_codes"] for code in ("strategy6_v3_entry_price_too_far", "strategy6_v3_wait_second_acceptance"))


def test_strategy6_v3_accepts_clean_aligned_context() -> None:
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": 8.0,
        "pct_3m_bps": 28.0,
        "pct_5m_bps": 42.0,
        "volume_z": 2.4,
        "taker_buy_ratio": 0.63,
        "range_pos": 0.58,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 12.0,
        "distance_to_ema_bps": 10.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, _strategy6_v3_cfg())

    assert score["strategy6_version"] == "v3"
    assert score["decision_state"] == "EXECUTABLE"
    assert score["direction_gate_state"] == "accepted"
    assert score["entry_confirmation_state"] == "confirmed"
    assert "strategy6_v3_direction_context_accepted" in score["reason_codes"]


def test_strategy6_v3_1_denies_side_adjusted_reverse_1m() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update({"strategy6_version": "v3_1", "v3_1_reverse_1m_deny_bps": 10})
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": -14.0,
        "pct_3m_bps": 30.0,
        "pct_5m_bps": 50.0,
        "volume_z": 2.0,
        "taker_buy_ratio": 0.6,
        "range_pos": 0.5,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 8.0,
        "distance_to_ema_bps": 8.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v3_1"
    assert score["decision_state"] == "DENY_DIRECTION_CONFLICT"
    assert score["direction_gate_state"] == "denied"
    assert score["v3_1_adverse_1m_bps"] == 14.0
    assert "strategy6_v3_1_reverse_1m_denied" in score["reason_codes"]


def test_strategy6_v3_1_waits_on_low_followthrough() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update(
        {
            "strategy6_version": "v3_1",
            "v3_1_low_followthrough_min_volume_z": 0.9,
            "v3_1_low_followthrough_min_5m_bps": 10,
        }
    )
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": 8.0,
        "pct_3m_bps": 58.0,
        "pct_5m_bps": 38.0,
        "volume_z": 0.7,
        "taker_buy_ratio": 0.63,
        "range_pos": 0.58,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 12.0,
        "distance_to_ema_bps": 10.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v3_1"
    assert score["decision_state"] == "WAIT_CONFIRM"
    assert score["wait_state"] == "WAIT_CONFIRM"
    assert score["v3_1_low_followthrough"] is True
    assert "strategy6_v3_1_low_followthrough_wait_confirm" in score["reason_codes"]


def test_strategy6_v3_2_long_btc_against_waits_without_denying() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update(
        {
            "strategy6_version": "v3_2",
            "v3_2_long_min_direction_context_score": 58,
            "v3_2_long_btc_against_action": "wait",
        }
    )
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": 8.0,
        "pct_3m_bps": 28.0,
        "pct_5m_bps": 42.0,
        "volume_z": 2.4,
        "taker_buy_ratio": 0.63,
        "range_pos": 0.58,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 12.0,
        "distance_to_ema_bps": 10.0,
        "btc_alignment": "opposite",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v3_2"
    assert score["decision_state"] == "WAIT_CONFIRM"
    assert score["v3_2_side_profile"] == "long_strict"
    assert score["v3_2_btc_against"] is True
    assert "strategy6_v3_2_btc_against" in score["reason_codes"]


def test_strategy6_v3_2_short_uses_baseline_and_shadow_quality_filter() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update(
        {
            "strategy6_version": "v3_2",
            "v3_2_short_min_direction_context_score": 52,
            "v3_2_bad_sides": ["SHORT"],
            "v3_2_quality_filter_mode": "shadow",
        }
    )
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "SHORT",
        "pct_1m_bps": -8.0,
        "pct_3m_bps": -28.0,
        "pct_5m_bps": -42.0,
        "volume_z": 2.4,
        "taker_buy_ratio": 0.37,
        "range_pos": 0.42,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": -12.0,
        "distance_to_ema_bps": -10.0,
        "btc_alignment": "unknown",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v3_2"
    assert score["v3_2_side_profile"] == "short_baseline"
    assert score["v3_2_quality_filter_state"] == "shadow_block"
    assert "strategy6_v3_2_quality_filter_side" in score["reason_codes"]


def test_strategy6_v3_3_waits_on_causal_adverse_1m() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update(
        {
            "strategy6_version": "v3_3",
            "v3_3_long_min_direction_context_score": 58,
            "v3_3_adverse_1m_wait_bps": 6,
            "v3_3_adverse_3m_deny_bps": 24,
        }
    )
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": -8.0,
        "pct_3m_bps": 16.0,
        "pct_5m_bps": 42.0,
        "volume_z": 2.4,
        "taker_buy_ratio": 0.63,
        "range_pos": 0.58,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 12.0,
        "distance_to_ema_bps": 10.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v3_3"
    assert score["decision_state"] == "WAIT_CONFIRM"
    assert score["v3_3_no_lookahead"] is True
    assert score["v3_3_adverse_1m_bps"] == 8.0
    assert "strategy6_v3_3_causal_adverse_1m_wait" in score["reason_codes"]


def test_strategy6_v3_3_denies_causal_adverse_3m() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update(
        {
            "strategy6_version": "v3_3",
            "v3_3_short_min_direction_context_score": 52,
            "v3_3_adverse_3m_deny_bps": 18,
        }
    )
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "SHORT",
        "pct_1m_bps": 4.0,
        "pct_3m_bps": 22.0,
        "pct_5m_bps": -36.0,
        "volume_z": 2.4,
        "taker_buy_ratio": 0.37,
        "range_pos": 0.42,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": -12.0,
        "distance_to_ema_bps": -10.0,
        "btc_alignment": "unknown",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v3_3"
    assert score["decision_state"] == "DENY_DIRECTION_CONFLICT"
    assert score["v3_3_adverse_3m_bps"] == 22.0
    assert "strategy6_v3_3_causal_adverse_3m_denied" in score["reason_codes"]


def test_strategy6_v3_3_known_at_contract_marks_entry_time_fields() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update({"strategy6_version": "v3_3"})
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": 8.0,
        "pct_3m_bps": 28.0,
        "pct_5m_bps": 42.0,
        "volume_z": 2.4,
        "taker_buy_ratio": 0.63,
        "range_pos": 0.58,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 12.0,
        "distance_to_ema_bps": 10.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["v3_3_no_lookahead"] is True
    assert score["v3_3_known_at_contract"]["pct_1m_bps"] == "entry_time"
    assert "mfe" not in " ".join(score["v3_3_known_at_contract"].keys()).lower()
    assert "pnl" not in " ".join(score["v3_3_known_at_contract"].keys()).lower()


def test_strategy6_v3_4_waits_on_no_edge_without_future_data() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update(
        {
            "strategy6_version": "v3_4",
            "v3_3_long_min_direction_context_score": 58,
            "v3_4_min_followthrough_5m_bps": 80,
            "v3_4_min_volume_z": 0.8,
        }
    )
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": 8.0,
        "pct_3m_bps": 28.0,
        "pct_5m_bps": 42.0,
        "volume_z": 2.4,
        "taker_buy_ratio": 0.63,
        "range_pos": 0.58,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 12.0,
        "distance_to_ema_bps": 10.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v3_4"
    assert score["decision_state"] == "WAIT_CONFIRM"
    assert score["v3_4_no_lookahead"] is True
    assert score["v3_4_no_edge"] is True
    assert "no_edge" in score["v3_4_gate_hits"]
    assert "strategy6_v3_4_no_edge_wait" in score["reason_codes"]


def test_strategy6_v3_4_waits_rebound_on_range_noise() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update(
        {
            "strategy6_version": "v3_4",
            "v3_3_long_min_direction_context_score": 58,
            "v3_fake_breakout_range_pos": 0.98,
            "v3_4_long_max_range_pos": 0.86,
            "v3_4_range_noise_action": "wait_rebound",
        }
    )
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": 8.0,
        "pct_3m_bps": 28.0,
        "pct_5m_bps": 42.0,
        "volume_z": 2.4,
        "taker_buy_ratio": 0.63,
        "range_pos": 0.9,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 12.0,
        "distance_to_ema_bps": 10.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v3_4"
    assert score["decision_state"] == "WAIT_REBOUND"
    assert score["wait_state"] == "WAIT_REBOUND"
    assert score["v3_4_range_extreme"] is True
    assert "range_noise" in score["v3_4_gate_hits"]


def test_strategy6_v3_5_routes_no_edge_with_entry_known_contract() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update(
        {
            "strategy6_version": "v3_5",
            "v3_3_long_min_direction_context_score": 52,
            "v3_4_min_followthrough_5m_bps": -100,
            "v3_5_no_edge_aligned_5m_bps": 12,
            "v3_5_no_edge_volume_z": 0.8,
        }
    )
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": 5.0,
        "pct_3m_bps": 9.0,
        "pct_5m_bps": 8.0,
        "volume_z": 0.72,
        "taker_buy_ratio": 0.58,
        "range_pos": 0.52,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 6.0,
        "distance_to_ema_bps": 5.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v3_5"
    assert score["v3_5_no_lookahead"] is True
    assert score["v3_5_known_at_contract"]["v3_5_no_edge_aligned_5m_bps"] == "config_time"
    assert score["v3_5_loss_mode"] == "no_edge"
    assert "strategy6_v3_5_no_edge_route" in score["reason_codes"]


def test_strategy6_v3_5_route_stable_when_future_payload_changes() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update({"strategy6_version": "v3_5", "v3_3_long_min_direction_context_score": 52})
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": -10.0,
        "pct_3m_bps": 18.0,
        "pct_5m_bps": 36.0,
        "volume_z": 2.0,
        "taker_buy_ratio": 0.62,
        "range_pos": 0.46,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 8.0,
        "distance_to_ema_bps": 8.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }
    changed = dict(features)
    changed.update({"MFE_R": 1.8, "MAE_R": 0.2, "net_R": 0.7, "exit_reason": "TP"})

    base_score = score_strategy6(features, cfg)
    changed_score = score_strategy6(changed, cfg)

    assert base_score["v3_5_loss_mode"] == changed_score["v3_5_loss_mode"]
    assert base_score["v3_5_route_reason_codes"] == changed_score["v3_5_route_reason_codes"]
    assert "MFE_R" not in base_score["v3_5_known_at_contract"]


def test_strategy6_v3_6_hard_wrong_denies_before_entry() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update(
        {
            "strategy6_version": "v3_6",
            "v3_3_long_min_direction_context_score": 52,
            "v3_6_hard_wrong_1m_bps": 10.0,
            "v3_6_hard_wrong_3m_bps": 22.0,
        }
    )
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": -11.0,
        "pct_3m_bps": 18.0,
        "pct_5m_bps": 36.0,
        "volume_z": 2.0,
        "taker_buy_ratio": 0.62,
        "range_pos": 0.46,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 8.0,
        "distance_to_ema_bps": 8.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    score = score_strategy6(features, cfg)

    assert score["strategy6_version"] == "v3_6"
    assert score["decision_state"] == "DENY_DIRECTION_CONFLICT"
    assert score["adaptive_exit_tier"] == "reject"
    assert score["v3_6_hard_wrong"] is True
    assert score["v3_6_known_at_contract"]["pct_1m_bps"] == "entry_snapshot"
    assert "strategy6_v3_6_hard_wrong_deny" in score["reason_codes"]


def test_strategy6_v3_6_no_edge_wait_or_deny_is_configurable() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update(
        {
            "strategy6_version": "v3_6",
            "v3_3_long_min_direction_context_score": 52,
            "v3_6_min_followthrough_5m_bps": 6.0,
            "v3_6_min_volume_z": 0.9,
            "v3_6_no_edge_action": "wait",
        }
    )
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": 5.0,
        "pct_3m_bps": 11.0,
        "pct_5m_bps": 3.0,
        "volume_z": 0.75,
        "taker_buy_ratio": 0.59,
        "range_pos": 0.48,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 6.0,
        "distance_to_ema_bps": 6.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }

    wait_score = score_strategy6(features, cfg)
    deny_score = score_strategy6(features, {**cfg, "v3_6_no_edge_action": "deny"})

    assert wait_score["v3_6_no_edge"] is True
    assert wait_score["decision_state"] == "WAIT_CONFIRM"
    assert "strategy6_v3_6_no_edge_wait" in wait_score["reason_codes"]
    assert deny_score["decision_state"] == "DENY_DIRECTION_CONFLICT"
    assert deny_score["adaptive_exit_tier"] == "reject"
    assert "strategy6_v3_6_no_edge_deny" in deny_score["reason_codes"]


def test_strategy6_v3_6_entry_gate_ignores_future_trade_payload() -> None:
    cfg = _strategy6_v3_cfg()
    cfg.update({"strategy6_version": "v3_6", "v3_3_long_min_direction_context_score": 52})
    features = {
        "symbol": "BTCUSDT",
        "legacy_side": "LONG",
        "pct_1m_bps": -12.0,
        "pct_3m_bps": 20.0,
        "pct_5m_bps": 34.0,
        "volume_z": 2.0,
        "taker_buy_ratio": 0.62,
        "range_pos": 0.46,
        "spread_bps": 3.0,
        "distance_to_vwap_bps": 8.0,
        "distance_to_ema_bps": 8.0,
        "btc_alignment": "same",
        "feature_quality": {"micro_optional_missing": True},
    }
    changed = dict(features)
    changed.update({"MFE_R": 2.0, "MAE_R": 0.1, "net_R": 0.8, "root_cause": "profitable_trade"})

    base_score = score_strategy6(features, cfg)
    changed_score = score_strategy6(changed, cfg)

    assert base_score["v3_6_direction_gate"] == changed_score["v3_6_direction_gate"]
    assert base_score["v3_6_signal_edge_gate"] == changed_score["v3_6_signal_edge_gate"]
    assert base_score["decision_state"] == changed_score["decision_state"]
    assert "MFE_R" not in base_score["v3_6_known_at_contract"]
