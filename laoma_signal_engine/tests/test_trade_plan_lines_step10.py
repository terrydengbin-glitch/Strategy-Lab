"""P10 independent trade plan line tests."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

from laoma_signal_engine.decision.trade_plan_line_models import TradePlanLineDocument
from laoma_signal_engine.decision.trade_plan_archive import archive_trade_plan_line_payload
from laoma_signal_engine.decision.trade_plan_lines import (
    DEFAULT_CONFIGS,
    TradePlanLineConfig,
    _build_position_sizing,
    build_trade_plan_line_document,
    run_apply_trade_plan_line_safe,
)
from laoma_signal_engine.decision.trade_plan_lines_audit import build_trade_plan_lines_audit
from laoma_signal_engine.factors.models import FactorSnapshotDocument
from laoma_signal_engine.micro.daemon.state_models import MicroDaemonStateDocument
from laoma_signal_engine.trade_quality.engine import ensure_trade_quality_tables
from laoma_signal_engine.trade_quality.promotion_policy import ensure_promotion_tables
from laoma_signal_engine.tests.test_market_entry_direction_gate_step43 import (
    _factor_with_market_entry,
    _micro_doc,
    _refresh,
)

GEN = "2099-01-01T00:00:00Z"


def _write_trade_quality_sample(
    root: Path,
    *,
    line: str = "without_micro",
    symbol: str = "BTCUSDT",
    side: str = "LONG",
    root_cause: str = "signal_no_edge",
    net_r: float = -1.2,
) -> None:
    db_path = root / "DATA" / "paper" / "paper_trading.db"
    ensure_trade_quality_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO trade_quality_samples(
              sample_id, order_id, strategy_line, symbol, side, source_run_id, source_cycle_id, source_plan_hash,
              opened_at, closed_at, exit_reason, entry_price, exit_price, stop_loss, take_profit, quantity,
              initial_risk_usdt, gross_pnl_usdt, net_pnl_usdt, fee_usdt, slippage_usdt, cost_ratio_R,
              planned_RR, net_R, MFE_R, MAE_R, holding_sec, holding_bucket, excursion_model,
              root_cause_label, root_cause_confidence, root_cause_evidence_json, secondary_labels_json,
              needs_manual_review, sample_schema_version, label_schema_version, generated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                f"{line}:{symbol}:{root_cause}",
                f"order_{symbol}_{root_cause}",
                line,
                symbol,
                side,
                "run_tq",
                "cycle_tq",
                "hash_tq",
                GEN,
                GEN,
                "SL",
                100.0,
                99.0,
                99.0,
                102.0,
                1.0,
                1.0,
                net_r,
                net_r,
                0.0,
                0.0,
                0.0,
                1.5,
                net_r,
                0.2,
                -1.0,
                600.0,
                "10m",
                "unit_test",
                root_cause,
                0.9,
                "{}",
                "[]",
                0,
                "18.1",
                "18.2",
                GEN,
            ),
        )


def _write_active_profile(root: Path, profile: str) -> None:
    config_dir = root / "laoma_signal_engine" / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "default.yaml").write_text(f"active_profile: {profile}\n", encoding="utf-8")


def _write_trade_quality_promotion(
    root: Path,
    *,
    rule_type: str = "cost_liquidity",
    symbol: str = "BTCUSDT",
    side: str = "LONG",
    line: str = "without_micro",
    profile: str = "relaxed_profit",
    mode: str = "wait_only",
) -> str:
    db_path = root / "DATA" / "paper" / "paper_trading.db"
    ensure_promotion_tables(db_path)
    rule_id = f"rule_{rule_type}_{symbol}_{side}"
    promotion_id = f"promo_{rule_type}_{symbol}_{side}_{profile}_{mode}"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO trade_quality_recommendation_rules(
              rule_id, rule_type, scope_type, scope_key, strategy_line, side, symbol,
              sample_source, config_profile, sample_count, total_R, avg_R, win_rate,
              root_cause_counts_json, evidence_json, recommendation, severity, mode,
              confidence, schema_version, generated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                rule_id,
                rule_type,
                "symbol_strategy_side",
                f"{symbol}|{line}|{side}|archive",
                line,
                side,
                symbol,
                "archive",
                profile,
                7,
                -8.0,
                -1.14,
                0.0,
                '{"cost_too_high":5}',
                '{"sample_ids":["s1","s2"]}',
                "cost_shadow_blacklist" if rule_type == "cost_liquidity" else "quality_shadow_blacklist",
                "P0",
                "shadow",
                0.6,
                "18.10",
                GEN,
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO trade_quality_recommendation_promotions(
              promotion_id, rule_id, profile, strategy_line, mode, enabled, reason,
              created_at, updated_at, schema_version, evidence_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                promotion_id,
                rule_id,
                profile,
                line,
                mode,
                1,
                "unit_test",
                GEN,
                GEN,
                "18.12",
                "{}",
            ),
        )
    return promotion_id


def test_step1055_archives_trade_plan_line_per_run_and_keeps_contract(tmp_path: Path) -> None:
    latest_path = tmp_path / "DATA/decisions/latest_trade_plan_micro_fast.json"
    payload = {
        "schema_version": "1.0",
        "generated_at": GEN,
        "run_id": "run_archive_1",
        "cycle_id": "cycle_archive_1",
        "source": "trade_plan_micro_fast",
        "micro_mode": "fast",
        "status": "ok",
        "count": 1,
        "executable_count": 0,
        "input_refs": {"micro_wait_evidence_used": True},
        "plans": [
            {
                "symbol": "BTCUSDT",
                "decision_tf": "15m",
                "decision": "LONG",
                "action": "WAIT",
                "entry_mode": "WAIT_CONFIRMATION",
                "estimated_entry_price": None,
                "stop_loss": None,
                "take_profit": None,
                "risk_per_unit": None,
                "reward_per_unit": None,
                "rr": None,
                "executable": False,
                "confidence": 50,
                "reason_codes": ["not_executable_after_micro"],
                "guards": {
                    "line": "micro_fast",
                    "trade_plan_consumable": True,
                    "micro_exec_allowed": True,
                    "micro_consumption_policy": "confirmed_only",
                },
                "input_refs": {},
            },
        ],
    }

    annotated = archive_trade_plan_line_payload(
        root=tmp_path,
        line="micro_fast",
        payload=payload,
        latest_path=latest_path,
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(annotated), encoding="utf-8")

    refs = annotated["input_refs"]
    archive_path = Path(refs["trade_plan_archive_path"])
    manifest_path = Path(refs["trade_plan_archive_manifest_path"])
    assert archive_path.is_file()
    assert manifest_path.is_file()
    assert refs["trade_plan_source_plan_hashes"]
    assert annotated["plans"][0]["input_refs"]["source_plan_hash"] == refs["trade_plan_source_plan_hashes"][0]
    TradePlanLineDocument.model_validate(json.loads(archive_path.read_text(encoding="utf-8")))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["lines"]["micro_fast"]["archive_path"] == str(archive_path)


def _write_symbol_whitelist(root: Path, *symbols: str) -> None:
    market_dir = root / "DATA" / "market"
    market_dir.mkdir(parents=True, exist_ok=True)
    got = list(symbols or ("BTCUSDT",))
    (market_dir / "futures_light_snapshot.json").write_text(
        json.dumps({"schema_version": "1.6", "items": [{"symbol": symbol} for symbol in got]}),
        encoding="utf-8",
    )


def _write_light_profile(
    root: Path,
    *,
    symbol: str = "BTCUSDT",
    market_entry_score: int = 80,
    slippage_risk_score: int = 10,
    trade_quality_tier: str = "market_entry_fit",
) -> None:
    market_dir = root / "DATA" / "market"
    market_dir.mkdir(parents=True, exist_ok=True)
    (market_dir / "futures_light_snapshot.json").write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": GEN,
                "items": [
                    {
                        "symbol": symbol,
                        "tradability_profile": {
                            "market_entry_score": market_entry_score,
                            "slippage_risk_score": slippage_risk_score,
                            "trade_quality_tier": trade_quality_tier,
                            "tradability_score": market_entry_score,
                        },
                        "primary_pool": "test_pool",
                        "pool_tags": ["test_pool", trade_quality_tier],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _write_universe_profile(
    root: Path,
    *,
    symbol: str = "BTCUSDT",
    business_pool: str = "active_alt",
    execution_tier: str = "market_ok",
) -> None:
    universe_dir = root / "DATA" / "universe"
    universe_dir.mkdir(parents=True, exist_ok=True)
    (universe_dir / "CANDIDATE_UNIVERSE.json").write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": GEN,
                "expires_at": "2099-01-02T00:00:00Z",
                "count": 1,
                "counts": {
                    "total_pairs": 1,
                    "futures_count": 1,
                    "spot_count": 0,
                    "both_spot_and_futures": 0,
                    "futures_only": 1,
                    "spot_only": 0,
                    "neither_spot_nor_futures": 0,
                },
                "pairs": [
                    {
                        "base_asset": symbol.removesuffix("USDT"),
                        "display_base_asset": symbol.removesuffix("USDT"),
                        "cashtag": f"${symbol.removesuffix('USDT')}",
                        "spot_cashtag_symbol": symbol,
                        "symbol_safe_id": symbol,
                        "futures_symbol": symbol,
                        "has_um_futures": True,
                        "eligible_for_signal_engine": True,
                        "eligible_for_post": False,
                        "eligible_for_trade_analysis": True,
                        "source_tags": ["futures_universe"],
                        "universe_profile": {
                            "universe_tier": "tier_B_active_alt",
                            "universe_priority_score": 80,
                            "scan_tier": "active_mover",
                            "business_pool": business_pool,
                            "scan_eligibility": "scan",
                            "trade_symbol": symbol,
                        },
                        "risk_profile": {
                            "liquidity_tier": "B",
                            "volatility_tier": "normal",
                            "execution_tier": execution_tier,
                            "rr_policy": "normal",
                            "sl_template": "normal",
                            "rr_template": "standard",
                            "sizing_template": "normal",
                            "feishu_policy": "send",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _fresh_factor(*, without_micro: bool = False) -> FactorSnapshotDocument:
    raw = _factor_with_market_entry(score=80, move_side="up").model_dump(mode="json")
    raw["generated_at"] = GEN
    if without_micro:
        raw["source"] = "factor_snapshot_without_ofi_cvd"
        raw["items"][0]["micro_quality"]["ready"] = False
        raw["items"][0]["micro_quality"]["reason_codes"] = ["without_micro"]
        raw["items"][0]["micro_15m"]["ready"] = False
    return FactorSnapshotDocument.model_validate(raw)


def _fresh_short_factor(*, without_micro: bool = False) -> FactorSnapshotDocument:
    raw = _factor_with_market_entry(score=80, move_side="down").model_dump(mode="json")
    raw["generated_at"] = GEN
    if without_micro:
        raw["source"] = "factor_snapshot_without_ofi_cvd"
        raw["items"][0]["micro_quality"]["ready"] = False
        raw["items"][0]["micro_quality"]["reason_codes"] = ["without_micro"]
        raw["items"][0]["micro_15m"]["ready"] = False
    return FactorSnapshotDocument.model_validate(raw)


def _fresh_refresh(factor: FactorSnapshotDocument):
    raw = _refresh(factor).model_dump(mode="json")
    raw["generated_at"] = GEN
    for item in raw["items"]:
        item["refresh_age_sec"] = 0
        item["liquidity_age_sec"] = 0
        item["reason_codes"] = [c for c in item.get("reason_codes", []) if c not in ("refresh_stale", "liquidity_stale")]
    raw["stale_count"] = 0
    return type(_refresh(factor)).model_validate(raw)


def _refresh_with(factor: FactorSnapshotDocument, **updates):
    raw = _fresh_refresh(factor).model_dump(mode="json")
    raw["items"][0].update(updates)
    return type(_refresh(factor)).model_validate(raw)


def _liquidity(ok: bool = True):
    from laoma_signal_engine.market.market_entry_liquidity_models import MarketEntryLiquidityDocument

    return MarketEntryLiquidityDocument.model_validate(
        {
            "schema_version": "1.6",
            "generated_at": GEN,
            "source": "market_entry_liquidity_snapshot",
            "status": "ok",
            "count": 1,
            "max_spread_bps": 8,
            "max_estimated_slippage_bps": 15,
            "min_top_depth_usdt": 20000,
            "min_quote_volume_24h": 3000000,
            "items": [
                {
                    "symbol": "BTCUSDT",
                    "last_price": 100.0,
                    "bid_price": 99.99,
                    "ask_price": 100.01,
                    "spread_bps": 2.0,
                    "top_bid_depth_usdt": 50000.0,
                    "top_ask_depth_usdt": 50000.0,
                    "estimated_market_buy_slippage_bps": 5.0,
                    "estimated_market_sell_slippage_bps": 5.0,
                    "liquidity_ok_for_market_entry": ok,
                    "reason_codes": [] if ok else ["spread_too_wide"],
                },
            ],
        },
    )


def _sell_liquidity(ok: bool = True):
    from laoma_signal_engine.market.market_entry_liquidity_models import MarketEntryLiquidityDocument

    return MarketEntryLiquidityDocument.model_validate(
        {
            "schema_version": "1.6",
            "generated_at": GEN,
            "source": "market_entry_liquidity_snapshot",
            "status": "ok",
            "count": 1,
            "max_spread_bps": 8,
            "max_estimated_slippage_bps": 15,
            "min_top_depth_usdt": 20000,
            "min_quote_volume_24h": 3000000,
            "items": [
                {
                    "symbol": "BTCUSDT",
                    "last_price": 100.0,
                    "bid_price": 99.99,
                    "ask_price": 100.01,
                    "spread_bps": 2.0,
                    "top_bid_depth_usdt": 50000.0,
                    "top_ask_depth_usdt": 50000.0,
                    "estimated_market_buy_slippage_bps": 5.0,
                    "estimated_market_sell_slippage_bps": 5.0,
                    "liquidity_ok_for_market_entry": ok,
                    "buy_liquidity_ok_for_market_entry": ok,
                    "sell_liquidity_ok_for_market_entry": ok,
                    "buy_reason_codes": [] if ok else ["spread_too_wide"],
                    "sell_reason_codes": [] if ok else ["spread_too_wide"],
                    "reason_codes": [] if ok else ["spread_too_wide"],
                },
            ],
        },
    )


def _micro_state(*, fast_ready: bool = True, full_ready: bool = True) -> MicroDaemonStateDocument:
    return MicroDaemonStateDocument.model_validate(
        {
            "schema_version": "1.0",
            "generated_at": GEN,
            "source": "persistent_micro_daemon_state",
            "daemon_status": "running",
            "target_generated_at": GEN,
            "target_version": "20990101T000000Z",
            "target_age_sec": 0,
            "active_symbol_count": 1,
            "state_ready_for_consumers": True,
            "reason_codes": [],
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "target_status": "active",
                    "source_state": "strong_candidate",
                    "move_side": "up",
                    "priority": 100,
                    "first_seen_at": GEN,
                    "last_seen_at": GEN,
                    "continuous_collect_sec": 900 if full_ready else 120,
                    "seen_cycle_count": 3,
                    "fast_ready": fast_ready,
                    "full_ready": full_ready,
                    "fast_reason_codes": [] if fast_ready else ["warmup_not_met"],
                    "full_reason_codes": [] if full_ready else ["warmup_not_met"],
                    "full_ready_eta_sec": 0 if full_ready else 780,
                    "last_micro_generated_at": GEN,
                    "target_churn_state": "kept",
                    "consumer_safe": True,
                    "consumer_reason_codes": [],
                },
            ],
        },
    )


def test_step101_common_contract_rejects_count_mismatch() -> None:
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=_fresh_factor(without_micro=True),
        refresh_doc=_fresh_refresh(_fresh_factor(without_micro=True)),
        liquidity_doc=None,
        micro_doc=None,
        generated_at=GEN,
    )
    raw = doc.model_dump(mode="json")
    raw["count"] = raw["count"] + 1
    try:
        TradePlanLineDocument.model_validate(raw)
    except ValueError as exc:
        assert "count must equal len(plans)" in str(exc)
    else:
        raise AssertionError("count mismatch should fail")


def test_step102_without_micro_outputs_wait_plan_without_micro_refs() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _fresh_refresh(factor)
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=None,
        generated_at=GEN,
        cfg=TradePlanLineConfig(
            allow_market_entry=False,
            allow_wait_plan=True,
            min_score=65,
            require_refresh_fresh=True,
            require_direction_still_valid=True,
            require_range_room_ok=False,
            require_liquidity_ok=False,
            require_micro_ready=False,
            require_micro_alignment=False,
            max_refresh_age_sec=300,
            max_liquidity_age_sec=300,
            max_micro_age_sec=0,
            target_rr=1.25,
            min_rr=1.0,
            stop_atr_mult=1.4,
            max_stop_atr_mult=2.2,
        ),
    )
    plan = doc.plans[0]
    assert doc.source == "trade_plan_without_micro"
    assert doc.micro_mode == "none"
    assert plan.action == "WAIT"
    assert plan.entry_mode in {"WAIT_PULLBACK", "BREAKOUT_TRIGGER"}
    assert "micro_generated_at" not in plan.input_refs
    assert "micro_ready" not in plan.guards


def test_step108_without_micro_can_generate_executable_without_micro_refs() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
    )
    p = doc.plans[0]
    assert p.executable is True
    assert p.action == "ENTER_MARKET"
    assert p.entry_mode == "MARKET"
    assert p.guards["opportunity_type"] == "MARKET_EXECUTABLE"
    assert p.guards["micro_confirmation"] is False
    assert p.guards["without_micro_executable_enabled"] is True
    assert "micro_generated_at" not in p.input_refs
    assert "micro_state_generated_at" not in p.input_refs
    assert p.position_sizing is not None
    assert p.position_sizing["method"] == "fixed_risk"
    assert p.position_sizing["quantity"] > 0
    assert p.position_sizing["notional_usdt"] > 0
    assert p.position_sizing["margin_usdt"] > 0
    assert p.position_sizing["stop_loss"] == p.stop_loss


def test_step187_trade_quality_gate_shadow_keeps_executable(tmp_path: Path) -> None:
    _write_light_profile(tmp_path)
    _write_trade_quality_sample(tmp_path, root_cause="signal_no_edge", net_r=-1.4)
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={**_fresh_refresh(factor).items[0].primary_15m, "recent_swing_high": 103.0},
    )
    cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        trade_quality_gate_enabled=True,
        trade_quality_gate_mode="shadow",
        trade_quality_gate_min_samples_per_symbol=1,
        trade_quality_gate_min_samples_per_root_cause=1,
        trade_quality_gate_max_negative_expectancy_R=-0.1,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
        project_root=tmp_path,
    )
    plan = doc.plans[0]
    assert plan.executable is True
    assert plan.guards["trade_quality_gate"]["ok"] is False
    assert plan.guards["trade_quality_gate"]["trade_quality_gate_pass"] is True


def test_step187_trade_quality_gate_wait_only_downgrades_executable(tmp_path: Path) -> None:
    _write_light_profile(tmp_path)
    _write_trade_quality_sample(tmp_path, root_cause="signal_no_edge", net_r=-1.4)
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={**_fresh_refresh(factor).items[0].primary_15m, "recent_swing_high": 103.0},
    )
    cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        trade_quality_gate_enabled=True,
        trade_quality_gate_mode="wait_only",
        trade_quality_gate_min_samples_per_symbol=1,
        trade_quality_gate_min_samples_per_root_cause=1,
        trade_quality_gate_max_negative_expectancy_R=-0.1,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
        project_root=tmp_path,
    )
    plan = doc.plans[0]
    assert plan.executable is False
    assert plan.guards["trade_quality_gate"]["trade_quality_gate_pass"] is False
    assert "trade_quality_gate_wait_signal_no_edge" in plan.reason_codes


def test_step1813_promotion_wait_only_downgrades_when_profile_matches(tmp_path: Path) -> None:
    _write_light_profile(tmp_path)
    _write_active_profile(tmp_path, "relaxed_profit")
    _write_trade_quality_promotion(tmp_path, rule_type="cost_liquidity", profile="relaxed_profit")
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={**_fresh_refresh(factor).items[0].primary_15m, "recent_swing_high": 103.0},
    )
    cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        trade_quality_gate_enabled=True,
        trade_quality_gate_mode="wait_only",
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
        project_root=tmp_path,
    )
    plan = doc.plans[0]
    gate = plan.guards["trade_quality_gate"]
    assert plan.executable is False
    assert gate["promotion_policy_active"] is True
    assert gate["trade_quality_gate_pass"] is False
    assert "trade_quality_promotion_wait_only" in plan.reason_codes
    assert "trade_quality_promotion_cost_liquidity" in gate["reason_codes"]


def test_step1813_promotion_wait_only_ignores_profile_mismatch(tmp_path: Path) -> None:
    _write_light_profile(tmp_path)
    _write_active_profile(tmp_path, "custom")
    _write_trade_quality_promotion(tmp_path, rule_type="cost_liquidity", profile="relaxed_profit")
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={**_fresh_refresh(factor).items[0].primary_15m, "recent_swing_high": 103.0},
    )
    cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        trade_quality_gate_enabled=True,
        trade_quality_gate_mode="wait_only",
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
        project_root=tmp_path,
    )
    plan = doc.plans[0]
    gate = plan.guards["trade_quality_gate"]
    assert plan.executable is True
    assert gate["promotion_policy_active"] is False


def test_step1813_promotion_handoff_ignores_direction_and_block_modes(tmp_path: Path) -> None:
    _write_light_profile(tmp_path)
    _write_active_profile(tmp_path, "relaxed_profit")
    _write_trade_quality_promotion(tmp_path, rule_type="direction_gate", profile="relaxed_profit", mode="wait_only")
    _write_trade_quality_promotion(tmp_path, rule_type="cost_liquidity", profile="relaxed_profit", mode="block_executable")
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={**_fresh_refresh(factor).items[0].primary_15m, "recent_swing_high": 103.0},
    )
    cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        trade_quality_gate_enabled=True,
        trade_quality_gate_mode="wait_only",
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
        project_root=tmp_path,
    )
    plan = doc.plans[0]
    gate = plan.guards["trade_quality_gate"]
    assert plan.executable is True
    assert gate["promotion_policy_active"] is False


def test_step188_sl_tp_quality_apply_keeps_single_tp_and_records_adjustment(tmp_path: Path) -> None:
    _write_light_profile(tmp_path)
    _write_trade_quality_sample(tmp_path, root_cause="stop_too_tight", net_r=-1.0)
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={**_fresh_refresh(factor).items[0].primary_15m, "recent_swing_high": 104.0},
    )
    cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        min_rr=0.01,
        min_net_rr=0.01,
        min_effective_rr=0.01,
        sl_tp_quality_enabled=True,
        sl_tp_quality_mode="apply",
        sl_tp_quality_min_samples_per_cluster=1,
        sl_tp_quality_stop_too_tight_widen_factor=1.5,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
        project_root=tmp_path,
    )
    plan = doc.plans[0]
    quality = plan.guards["sl_tp_quality"]
    assert quality["adjustment_applied"] is True
    assert quality["original_stop_loss"] != quality["adjusted_stop_loss"]
    assert plan.guards.get("tp2") is None


def test_step108_without_micro_blocks_when_liquidity_not_ok() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _fresh_refresh(factor)
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=False),
        micro_doc=None,
        generated_at=GEN,
    )
    p = doc.plans[0]
    assert p.executable is False
    assert "liquidity_not_ok" in p.reason_codes


def test_step1026_long_uses_buy_side_liquidity_gate() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
        liquidity={
            "symbol": "BTCUSDT",
            "liquidity_ok_for_market_entry": False,
            "buy_liquidity_ok_for_market_entry": True,
            "sell_liquidity_ok_for_market_entry": False,
            "notional_usdt": 2000,
            "spread_bps": 2.0,
            "top_ask_depth_usdt": 3000.0,
            "top_bid_depth_usdt": 500.0,
            "estimated_market_buy_slippage_bps": 5.0,
            "estimated_market_sell_slippage_bps": None,
            "buy_reason_codes": [],
            "sell_reason_codes": ["depth_not_enough_for_notional"],
            "reason_codes": ["bid_depth_too_thin", "slippage_missing"],
        },
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=None,
        generated_at=GEN,
    )
    p = doc.plans[0]
    assert "liquidity_not_ok" not in p.reason_codes
    assert p.guards["liquidity_ok"] is True
    assert p.guards["liquidity_gate"]["side"] == "buy"
    assert p.guards["liquidity_gate"]["reason_codes"] == []


def test_step108_without_micro_blocks_when_range_room_not_ok() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(factor, range_room_ok=False)
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
    )
    p = doc.plans[0]
    assert p.executable is False
    assert "range_room_insufficient_after_refresh" in p.reason_codes


def test_step108_without_micro_blocks_when_rr_too_low() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _fresh_refresh(factor)
    cfg = TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=75,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=True,
        require_liquidity_ok=True,
        require_micro_ready=False,
        require_micro_alignment=False,
        max_refresh_age_sec=180,
        max_liquidity_age_sec=180,
        max_micro_age_sec=0,
        target_rr=1.25,
        min_rr=2.0,
        stop_atr_mult=1.2,
        max_stop_atr_mult=2.2,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
    )
    p = doc.plans[0]
    assert p.executable is False
    assert "rr_too_low" in p.reason_codes


def test_step1042_short_now_calibration_allows_quality_market_short() -> None:
    factor = _fresh_short_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        move_side="down",
        direction_still_valid=True,
        range_room_ok=True,
        range_gate={
            "ok": True,
            "move_side": "down",
            "range_pos": 0.5,
            "long_max_range_pos": 0.82,
            "short_min_range_pos": 0.18,
        },
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "price_ret": -1.0,
            "range_pos": 0.5,
            "recent_swing_low": 97.0,
            "breakdown_level": 97.0,
        },
        entry_1m={
            **_fresh_refresh(factor).items[0].entry_1m,
            "atr": 0.8,
            "last_rebound_high": 100.8,
        },
    )
    cfg = TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=0,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=False,
        require_liquidity_ok=False,
        require_micro_ready=False,
        require_micro_alignment=False,
        max_refresh_age_sec=300,
        max_liquidity_age_sec=300,
        max_micro_age_sec=0,
        target_rr=1.1,
        min_rr=0.1,
        stop_atr_mult=1.2,
        max_stop_atr_mult=4.0,
        min_net_rr=0.1,
        min_tp_after_cost_bps=0,
        max_stop_bps=800,
        allow_limit_pullback=False,
        allow_breakout_trigger=False,
        short_now_calibration_enabled=True,
        short_now_min_range_pos=0.18,
        short_now_max_range_pos=0.82,
        short_now_min_available_room_bps=40,
        short_now_max_stop_bps=200,
        short_now_max_stop_atr_mult=4.0,
        short_now_min_net_rr=0.2,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_sell_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
    )
    p = doc.plans[0]
    assert p.decision == "SHORT"
    assert p.executable is True
    assert p.entry_mode == "MARKET"
    assert p.stop_loss is not None and p.stop_loss > p.estimated_entry_price
    assert p.take_profit is not None and p.take_profit < p.estimated_entry_price
    assert p.guards["short_now_calibration"]["ok"] is True


def test_step1042_short_now_calibration_keeps_low_range_as_wait_rebound() -> None:
    factor = _fresh_short_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        move_side="down",
        direction_still_valid=True,
        range_room_ok=True,
        range_gate={
            "ok": False,
            "move_side": "down",
            "range_pos": 0.05,
            "long_max_range_pos": 0.82,
            "short_min_range_pos": 0.18,
        },
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "price_ret": -1.0,
            "range_pos": 0.05,
            "recent_swing_low": 97.0,
            "breakdown_level": 97.0,
        },
        entry_1m={
            **_fresh_refresh(factor).items[0].entry_1m,
            "atr": 0.8,
            "last_rebound_high": 100.8,
        },
    )
    cfg = TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=0,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=False,
        require_liquidity_ok=False,
        require_micro_ready=False,
        require_micro_alignment=False,
        max_refresh_age_sec=300,
        max_liquidity_age_sec=300,
        max_micro_age_sec=0,
        target_rr=1.1,
        min_rr=0.1,
        stop_atr_mult=1.2,
        max_stop_atr_mult=4.0,
        min_net_rr=0.1,
        min_tp_after_cost_bps=0,
        max_stop_bps=800,
        allow_limit_pullback=False,
        allow_breakout_trigger=False,
        short_now_calibration_enabled=True,
        short_now_min_range_pos=0.18,
        short_now_max_range_pos=0.82,
        short_now_min_available_room_bps=40,
        short_now_max_stop_bps=200,
        short_now_max_stop_atr_mult=4.0,
        short_now_min_net_rr=0.2,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_sell_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
    )
    p = doc.plans[0]
    assert p.decision == "SHORT"
    assert p.executable is False
    assert p.entry_mode == "WAIT_REBOUND"
    assert "short_now_range_too_low" in p.reason_codes
    assert "short_now_rebound_required" in p.reason_codes
    assert "short_now_market_entry_bad_price_wait_rebound" in p.reason_codes
    assert "market_entry_bad_price_wait_pullback" not in p.reason_codes


def test_step1061_market_now_calibration_blocks_long_high_range_as_wait_pullback() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        move_side="up",
        direction_still_valid=True,
        range_room_ok=True,
        range_gate={
            "ok": False,
            "move_side": "up",
            "range_pos": 0.96,
            "long_max_range_pos": 0.82,
            "short_min_range_pos": 0.18,
        },
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "price_ret": 1.0,
            "range_pos": 0.96,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    cfg = TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=0,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=False,
        require_liquidity_ok=False,
        require_micro_ready=False,
        require_micro_alignment=False,
        max_refresh_age_sec=300,
        max_liquidity_age_sec=300,
        max_micro_age_sec=0,
        target_rr=1.1,
        min_rr=0.1,
        stop_atr_mult=1.2,
        max_stop_atr_mult=4.0,
        min_net_rr=0.1,
        min_tp_after_cost_bps=0,
        max_stop_bps=800,
        allow_limit_pullback=False,
        allow_breakout_trigger=False,
        market_now_calibration_enabled=True,
        long_now_min_range_pos=0.18,
        long_now_max_range_pos=0.82,
        long_now_min_available_room_bps=40,
        long_now_max_stop_bps=300,
        long_now_max_stop_atr_mult=4.0,
        long_now_min_net_rr=0.2,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
    )
    p = doc.plans[0]
    assert p.decision == "LONG"
    assert p.executable is False
    assert p.entry_mode == "WAIT_PULLBACK"
    assert p.guards["market_now_calibration"]["side"] == "LONG"
    assert p.guards["market_now_calibration"]["ok"] is False
    assert "long_now_range_too_high" in p.reason_codes
    assert "long_now_pullback_required" in p.reason_codes
    assert "long_now_market_entry_bad_price_wait_pullback" in p.reason_codes


def test_step1062_market_now_not_reached_when_direction_invalid_after_refresh() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(factor, move_side="up", direction_still_valid=False, range_room_ok=True)
    cfg = TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=0,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=False,
        require_liquidity_ok=False,
        require_micro_ready=False,
        require_micro_alignment=False,
        max_refresh_age_sec=300,
        max_liquidity_age_sec=300,
        max_micro_age_sec=0,
        target_rr=1.1,
        min_rr=0.1,
        stop_atr_mult=1.2,
        max_stop_atr_mult=4.0,
        market_now_calibration_enabled=True,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
    )
    p = doc.plans[0]
    assert p.executable is False
    assert "direction_invalid_after_refresh" in p.reason_codes
    assert p.guards["market_now_calibration_status"] == "not_reached"
    assert p.guards["market_now_pre_calibration_blocker"] == "direction_invalid_after_refresh"
    assert "market_now_calibration" not in p.guards


def test_step1062_market_now_not_reached_when_score_too_low() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _fresh_refresh(factor)
    cfg = TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=999,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=False,
        require_liquidity_ok=False,
        require_micro_ready=False,
        require_micro_alignment=False,
        max_refresh_age_sec=300,
        max_liquidity_age_sec=300,
        max_micro_age_sec=0,
        target_rr=1.1,
        min_rr=0.1,
        stop_atr_mult=1.2,
        max_stop_atr_mult=4.0,
        market_now_calibration_enabled=True,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
    )
    p = doc.plans[0]
    assert p.executable is False
    assert "score_too_low" in p.reason_codes
    assert p.guards["market_now_calibration_status"] == "not_reached"
    assert p.guards["market_now_pre_calibration_blocker"] == "score_too_low"
    assert "market_now_calibration" not in p.guards


def test_step103_micro_fast_can_generate_market_entry_when_net_rr_room_is_enough() -> None:
    factor = _fresh_factor()
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    micro = _micro_doc(fast_ready=True, full_ready=False, z=1.0).model_copy(update={"generated_at": GEN})
    doc = build_trade_plan_line_document(
        line="micro_fast",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=micro,
        micro_state_doc=_micro_state(fast_ready=True, full_ready=False),
        generated_at=GEN,
    )
    plan = doc.plans[0]
    assert doc.source == "trade_plan_micro_fast"
    assert doc.micro_mode == "fast"
    assert doc.candidate_alignment["mode"] == "micro_targets_authoritative"
    assert doc.candidate_alignment["factor_symbol_count"] == 1
    assert doc.candidate_alignment["missing_micro_feature_count"] == 0
    assert doc.candidate_alignment["missing_micro_state_count"] == 0
    assert plan.executable is True
    assert plan.entry_mode == "MARKET"
    assert plan.guards["opportunity_type"] == "MARKET_EXECUTABLE"
    assert plan.guards["micro_lifecycle_scope"] == "symbol"
    assert plan.guards["micro_lifecycle_state"] == "emitted"
    assert plan.guards["micro_symbol_ready"] is True
    assert plan.guards["micro_symbol_confirmed"] is True
    assert plan.guards["micro_symbol_trade_plan_emitted"] is True
    assert plan.input_refs["micro_lifecycle_scope"] == "symbol"
    assert plan.input_refs["micro_lifecycle_state"] == "emitted"
    assert doc.input_refs["line_exec_status"] == "usable_all_ready"
    assert doc.input_refs["line_lifecycle_status"] == "completed_all_symbols"
    assert doc.input_refs["line_lifecycle_complete"] is True
    assert doc.input_refs["symbol_counts"]["ready"] == 1
    assert plan.guards["net_rr"] >= plan.guards["min_net_rr"]
    assert plan.guards["sl_tp_model_version"] == "10.63"
    assert plan.guards["tp_model"] == "single_reachable_tp"
    assert plan.guards["effective_rr"] >= plan.guards["min_effective_rr"]
    assert plan.guards["single_tp_reachable"] is True
    assert plan.guards["target_source"] != "fallback_target_bps"
    assert plan.guards["fallback_target_only"] is False
    assert plan.guards["trade_worthiness"] == "enter_now"
    assert plan.guards["tp2"] is None
    assert plan.stop_loss is not None
    assert plan.take_profit is not None


def test_step1063_effective_rr_gate_blocks_weak_single_tp_plan() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 101.0,
            "breakout_level": 101.0,
        },
    )
    cfg = TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=20,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=False,
        require_liquidity_ok=False,
        require_micro_ready=False,
        require_micro_alignment=False,
        max_refresh_age_sec=300,
        max_liquidity_age_sec=300,
        max_micro_age_sec=0,
        target_rr=1.0,
        min_rr=0.05,
        min_net_rr=0.02,
        min_effective_rr=5.0,
        min_reachable_reward_bps=1.0,
        min_stop_bps=3,
        preferred_stop_bps=20,
        max_stop_bps=1200,
        stop_atr_mult=1.2,
        max_stop_atr_mult=6.0,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
    )
    plan = doc.plans[0]
    assert plan.executable is False
    assert "effective_rr_below_min" in plan.reason_codes
    assert plan.guards["effective_rr"] < plan.guards["min_effective_rr"]
    assert plan.guards["trade_worthiness"] == "wait_rr"
    assert plan.guards["tp2"] is None


def test_step1066_fast_capped_rr_caps_take_profit_without_changing_stop() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    base_cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        min_score=20,
        min_rr=0.01,
        require_range_room_ok=False,
        min_net_rr=-10.0,
        min_effective_rr=-10.0,
        min_reachable_reward_bps=1.0,
        min_tp_after_cost_bps=-100.0,
        max_stop_bps=1200.0,
        max_stop_atr_mult=6.0,
    )
    structure_doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=base_cfg,
    )
    fast_doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=replace(
            base_cfg,
            tp_target_policy_mode="fast_capped_rr",
            tp_target_policy_target_rr=0.65,
            tp_target_policy_target_rr_cap=0.75,
            tp_target_policy_min_reward_bps=1.0,
            tp_target_policy_market_room_buffer_bps=0.0,
        ),
    )
    structure_plan = structure_doc.plans[0]
    fast_plan = fast_doc.plans[0]
    assert fast_plan.stop_loss == structure_plan.stop_loss
    assert fast_plan.take_profit != structure_plan.take_profit
    assert fast_plan.guards["tp_target_policy_mode"] == "fast_capped_rr"
    assert fast_plan.guards["tp_was_capped"] is True
    assert fast_plan.guards["final_gross_rr"] <= 0.750001
    assert fast_plan.guards["target_source"] == "fast_capped_rr"


def test_step1068_fast_exit_net_rr_basis_targets_one_r_without_moving_stop() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    base_cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        min_score=20,
        min_rr=0.01,
        require_range_room_ok=False,
        min_net_rr=-10.0,
        min_effective_rr=-10.0,
        min_reachable_reward_bps=1.0,
        min_tp_after_cost_bps=-100.0,
        max_stop_bps=1200.0,
        max_stop_atr_mult=6.0,
    )
    structure_doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=base_cfg,
    )
    net_doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=replace(
            base_cfg,
            min_net_rr=0.01,
            min_effective_rr=0.01,
            tp_target_policy_mode="fast_capped_rr",
            tp_target_policy_target_rr=0.65,
            tp_target_policy_target_rr_cap=1.05,
            tp_target_policy_target_rr_basis="net",
            tp_target_policy_target_net_rr=1.0,
            tp_target_policy_min_reward_bps=1.0,
            tp_target_policy_market_room_buffer_bps=0.0,
            tp_target_policy_sizing_basis="net_planned_loss",
        ),
    )
    structure_plan = structure_doc.plans[0]
    net_plan = net_doc.plans[0]
    assert net_plan.stop_loss == structure_plan.stop_loss
    assert net_plan.guards["tp_target_policy_basis"] == "net"
    assert net_plan.guards["r_parity_overlay_enabled"] is True
    assert net_plan.guards["target_source"] == "fast_capped_rr"
    assert abs(net_plan.guards["final_net_rr"] - 1.0) < 0.02
    assert net_plan.position_sizing["r_sizing_basis"] == "net_planned_loss"


def test_step1066_fast_exit_market_room_insufficient_blocks_executable() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 100.05,
            "breakout_level": 100.05,
        },
    )
    cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        min_score=20,
        min_rr=0.01,
        require_range_room_ok=False,
        min_net_rr=-10.0,
        min_effective_rr=-10.0,
        min_reachable_reward_bps=1.0,
        min_tp_after_cost_bps=-100.0,
        max_stop_bps=1200.0,
        max_stop_atr_mult=6.0,
        tp_target_policy_mode="fast_capped_rr",
        tp_target_policy_target_rr=0.65,
        tp_target_policy_target_rr_cap=0.75,
        tp_target_policy_min_reward_bps=1.0,
        tp_target_policy_market_room_buffer_bps=2.0,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
    )
    plan = doc.plans[0]
    assert plan.executable is False
    assert "market_room_insufficient_for_fast_exit" in plan.reason_codes
    assert plan.guards["tp_reject_reason"] == "market_room_insufficient_for_fast_exit"


def test_step1066_reward_floor_exceeds_rr_cap_blocks_executable() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        min_score=20,
        min_rr=0.01,
        require_range_room_ok=False,
        min_net_rr=-10.0,
        min_effective_rr=-10.0,
        min_reachable_reward_bps=1.0,
        min_tp_after_cost_bps=-100.0,
        max_stop_bps=1200.0,
        max_stop_atr_mult=6.0,
        tp_target_policy_mode="fast_capped_rr",
        tp_target_policy_target_rr=0.65,
        tp_target_policy_target_rr_cap=0.75,
        tp_target_policy_min_reward_bps=500.0,
        tp_target_policy_market_room_buffer_bps=0.0,
    )
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
        cfg=cfg,
    )
    plan = doc.plans[0]
    assert plan.executable is False
    assert "reward_floor_exceeds_rr_cap" in plan.reason_codes
    assert plan.guards["tp_reject_reason"] == "reward_floor_exceeds_rr_cap"


def test_step1064_notional_by_loss_cap_resizes_wide_stop_plan() -> None:
    cfg = replace(
        DEFAULT_CONFIGS["without_micro"],
        position_sizing_enabled=False,
        planned_loss_guard_enabled=True,
        planned_loss_sizing_policy="notional_by_loss_cap",
        base_notional_usdt=2000,
        target_planned_loss_usdt=40,
        max_planned_loss_usdt=60,
        min_notional_usdt=20,
        max_notional_usdt=2000,
        max_margin_usdt=100,
        default_leverage=20,
        allow_notional_resize=True,
    )

    sizing, reject = _build_position_sizing(
        cfg=cfg,
        entry=0.16126,
        stop=0.17178,
        take=0.13756,
        risk_per_unit=0.01052,
        reward_per_unit=0.02370,
    )

    assert reject is None
    assert sizing is not None
    assert sizing["method"] == "notional_by_loss_cap"
    assert sizing["loss_cap_applied"] is True
    assert sizing["notional_usdt"] < 1000
    assert sizing["planned_loss_usdt"] <= 60.01
    assert sizing["planned_profit_usdt"] > sizing["planned_loss_usdt"]


def test_step1039_micro_fast_not_ready_symbol_stays_lifecycle_only_not_consumed() -> None:
    factor = _fresh_factor()
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    micro = _micro_doc(fast_ready=False, full_ready=False, z=1.0).model_copy(update={"generated_at": GEN})
    doc = build_trade_plan_line_document(
        line="micro_fast",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=micro,
        micro_state_doc=_micro_state(fast_ready=False, full_ready=False),
        generated_at=GEN,
    )
    assert doc.count == 0
    assert doc.plans == []
    assert doc.input_refs["micro_consumption_policy"] == "confirmed_only"
    assert doc.input_refs["micro_lifecycle_consumed_symbols"] == []
    assert doc.input_refs["micro_lifecycle_excluded_symbols"] == ["BTCUSDT"]
    assert doc.input_refs["micro_lifecycle_excluded_counts"]["not_ready"] == 1
    excluded = doc.input_refs["micro_lifecycle_excluded_items"][0]
    assert excluded["state"] == "not_ready"
    assert excluded["terminal"] is True
    assert excluded["trade_plan_consumable"] is False
    assert excluded["consumption_block_reason"] == "micro_symbol_not_ready"
    assert doc.input_refs["line_exec_status"] == "no_ready"
    assert doc.input_refs["line_lifecycle_status"] == "completed_without_ready"
    assert doc.input_refs["line_lifecycle_complete"] is True
    assert doc.input_refs["unfinished_symbol_count"] == 0


def test_step1049_trade_plan_excluded_item_carries_data_quality_attribution() -> None:
    factor = _fresh_factor()
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    raw_micro = _micro_doc(fast_ready=False, full_ready=False, z=1.0).model_dump(mode="json")
    raw_micro["generated_at"] = GEN
    raw_micro["ws_status"] = "connected"
    raw_micro["last_ws_message_age_sec"] = 0
    q = raw_micro["items"][0]["micro_fast_quality"]
    q["reason_codes"] = ["cvd_stale"]
    q["cvd_update_age_sec"] = 120.0
    q["driver_metrics_summary"]["processed_trade_bucket_count"] = 3
    q["driver_metrics_summary"]["cvd_update_count"] = 0
    micro = type(_micro_doc(fast_ready=False, full_ready=False, z=1.0)).model_validate(raw_micro)

    doc = build_trade_plan_line_document(
        line="micro_fast",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=micro,
        micro_state_doc=_micro_state(fast_ready=False, full_ready=False),
        generated_at=GEN,
    )

    assert doc.plans == []
    excluded = doc.input_refs["micro_lifecycle_excluded_items"][0]
    assert excluded["state"] == "not_ready"
    assert excluded["trade_plan_consumable"] is False
    assert excluded["micro_data_quality_state"] == "technical_blocked"
    assert excluded["micro_data_quality_class"] == "technical_fix"
    assert excluded["raw_reason"] == "cvd_stale"
    assert excluded["attributed_reason"] == "technical_bug_cvd_adapter_not_updated"
    assert excluded["category"] == "technical_fix"
    assert excluded["recommended_action"]
    assert excluded["evidence"]["driver_metrics_summary"]["processed_trade_bucket_count"] == 3
    assert "data_quality_blocked" in excluded["reason_codes"]
    assert "technical_not_ready" in excluded["plan_reason_codes"]


def test_step54_trade_plan_rejects_invalid_exchange_symbol() -> None:
    raw = _fresh_factor(without_micro=True).model_dump(mode="json")
    raw["items"][0]["symbol"] = "我踏马来了USDT"
    factor = FactorSnapshotDocument.model_validate(raw)
    refresh = _fresh_refresh(factor)

    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=None,
        generated_at=GEN,
    )

    assert doc.count == 0
    assert doc.executable_count == 0
    assert doc.input_refs["invalid_symbol_count"] == 1
    invalid = doc.input_refs["invalid_symbol_items"][0]
    assert invalid["symbol"] == "我踏马来了USDT"
    assert invalid["symbol_contract_ok"] is False
    assert invalid["symbol_contract_reason"] == "invalid_symbol_format"
    assert "invalid_exchange_symbol" in invalid["reason_codes"]


def test_step1053_ready_signal_usable_policy_consumes_weak_micro_symbol_when_config_allows() -> None:
    factor = _fresh_factor()
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    micro = _micro_doc(fast_ready=True, full_ready=False, z=-1.0).model_copy(update={"generated_at": GEN})
    cfg = TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=0,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=False,
        require_liquidity_ok=False,
        require_micro_ready=True,
        require_micro_alignment=True,
        max_refresh_age_sec=300,
        max_liquidity_age_sec=300,
        max_micro_age_sec=300,
        target_rr=1.25,
        min_rr=0.2,
        stop_atr_mult=1.2,
        max_stop_atr_mult=4.0,
        min_net_rr=0.2,
        min_tp_after_cost_bps=0,
        max_stop_bps=500,
        micro_consumption_policy="ready_signal_usable",
        allow_weak_micro_consumption=True,
        weak_micro_require_direction_not_conflict=False,
    )
    doc = build_trade_plan_line_document(
        line="micro_fast",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=micro,
        micro_state_doc=_micro_state(fast_ready=True, full_ready=False),
        generated_at=GEN,
        cfg=cfg,
    )
    assert doc.input_refs["micro_consumption_policy"] == "ready_signal_usable"
    assert doc.input_refs["allow_weak_micro_consumption"] is True
    assert doc.count == 1
    assert doc.input_refs["micro_lifecycle_consumed_symbols"] == ["BTCUSDT"]
    assert doc.input_refs["micro_lifecycle_excluded_symbols"] == []
    plan = doc.plans[0]
    guards = plan.guards
    assert guards["trade_plan_consumable"] is True
    assert guards["micro_symbol_confirmed"] is False
    assert guards["micro_confirmation_strength"] == "weak"
    assert guards["micro_policy_relaxed"] is True
    assert guards["allow_weak_micro_consumption"] is True
    assert guards["consumption_block_reason"] == ""


def test_step1035_run_apply_writes_micro_lifecycle_current_json(tmp_path: Path) -> None:
    _write_symbol_whitelist(tmp_path, "BTCUSDT")
    factor = _fresh_factor()
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    micro = _micro_doc(fast_ready=True, full_ready=False, z=1.0).model_copy(update={"generated_at": GEN})
    state = _micro_state(fast_ready=True, full_ready=False)
    factor_p = tmp_path / "factor.json"
    refresh_p = tmp_path / "refresh.json"
    micro_p = tmp_path / "micro.json"
    state_p = tmp_path / "state.json"
    out_p = tmp_path / "trade_plan.json"
    factor_p.write_text(json.dumps(factor.model_dump(mode="json")), encoding="utf-8")
    refresh_p.write_text(json.dumps(refresh.model_dump(mode="json")), encoding="utf-8")
    micro_p.write_text(json.dumps(micro.model_dump(mode="json")), encoding="utf-8")
    state_p.write_text(json.dumps(state.model_dump(mode="json")), encoding="utf-8")

    code = run_apply_trade_plan_line_safe(
        line="micro_fast",
        project_root=tmp_path,
        factor_path=factor_p,
        refresh_path=refresh_p,
        micro_path=micro_p,
        micro_state_path=state_p,
        output_path=out_p,
        run_id="run_x",
        cycle_id="cycle_x",
    )

    assert code == 0
    lifecycle_p = tmp_path / "DATA" / "micro" / "latest_micro_lifecycle_micro_fast.json"
    assert lifecycle_p.is_file()
    lifecycle = json.loads(lifecycle_p.read_text(encoding="utf-8"))
    assert lifecycle["schema_version"] == "10.35"
    assert lifecycle["strategy_line"] == "micro_fast"
    assert lifecycle["run_id"] == "run_x"
    assert lifecycle["cycle_id"] == "cycle_x"
    assert lifecycle["state_counts"]["emitted"] == 1
    assert lifecycle["line_exec_status"] == "usable_all_ready"
    assert lifecycle["line_lifecycle_status"] == "completed_all_symbols"
    assert lifecycle["line_lifecycle_complete"] is True
    assert lifecycle["items"][0]["trade_plan_emitted"] is True


def test_step1038_run_apply_uses_wait_pass_evidence_over_current_micro(tmp_path: Path) -> None:
    _write_symbol_whitelist(tmp_path, "BTCUSDT")
    factor = _fresh_factor()
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    ready_micro = _micro_doc(fast_ready=True, full_ready=False, z=1.0).model_copy(update={"generated_at": GEN})
    stale_micro = _micro_doc(fast_ready=False, full_ready=False, z=1.0).model_copy(update={"generated_at": "2099-01-01T00:00:30Z"})
    ready_state = _micro_state(fast_ready=True, full_ready=False)
    stale_state = _micro_state(fast_ready=False, full_ready=False)
    (tmp_path / "DATA/factors").mkdir(parents=True, exist_ok=True)
    (tmp_path / "DATA/market").mkdir(parents=True, exist_ok=True)
    (tmp_path / "DATA/micro/evidence").mkdir(parents=True, exist_ok=True)
    (tmp_path / "DATA/factors/latest_factor_snapshot.json").write_text(
        json.dumps(factor.model_dump(mode="json")),
        encoding="utf-8",
    )
    (tmp_path / "DATA/market/latest_decision_refresh_micro_fast_snapshot.json").write_text(
        json.dumps(refresh.model_dump(mode="json")),
        encoding="utf-8",
    )
    (tmp_path / "DATA/micro/latest_micro_features.json").write_text(
        json.dumps(stale_micro.model_dump(mode="json")),
        encoding="utf-8",
    )
    (tmp_path / "DATA/micro/latest_micro_state.json").write_text(
        json.dumps(stale_state.model_dump(mode="json")),
        encoding="utf-8",
    )
    (tmp_path / "DATA/micro/micro_targets.json").write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": GEN,
                "target_set_id": "target_x",
                "target_symbols": ["BTCUSDT"],
            },
        ),
        encoding="utf-8",
    )
    (tmp_path / "DATA/micro/evidence/latest_wait_pass_micro_fast.json").write_text(
        json.dumps(
            {
                "schema_version": "10.38",
                "source": "micro_wait_pass_evidence",
                "strategy_line": "micro_fast",
                "run_id": "run_x",
                "cycle_id": "cycle_x",
                "target_set_id": "target_x",
                "generated_at": GEN,
                "micro_generated_at": GEN,
                "micro_state_generated_at": GEN,
                "wait_predicate": "min_fast_ready_count",
                "ready_symbols": ["BTCUSDT"],
                "fast_ready_symbols": ["BTCUSDT"],
                "full_ready_symbols": [],
                "micro_features": ready_micro.model_dump(mode="json"),
                "micro_state": ready_state.model_dump(mode="json"),
            },
        ),
        encoding="utf-8",
    )
    out_p = tmp_path / "DATA/decisions/latest_trade_plan_micro_fast.json"

    code = run_apply_trade_plan_line_safe(
        line="micro_fast",
        project_root=tmp_path,
        output_path=out_p,
        run_id="run_x",
        cycle_id="cycle_x",
    )

    assert code == 0
    doc = TradePlanLineDocument.model_validate(json.loads(out_p.read_text(encoding="utf-8")))
    assert doc.input_refs["micro_wait_evidence_used"] is True
    assert doc.input_refs["micro_wait_predicate"] == "min_fast_ready_count"
    assert doc.input_refs["micro_wait_pass_fast_ready_symbols"] == ["BTCUSDT"]
    assert doc.input_refs["micro_generated_at"] == GEN
    assert doc.input_refs["symbol_counts"]["ready"] == 1
    assert doc.plans[0].guards["micro_symbol_ready"] is True


def test_step109_micro_fast_rejects_bad_market_without_pending() -> None:
    factor = _fresh_factor()
    refresh = _fresh_refresh(factor)
    micro = _micro_doc(fast_ready=True, full_ready=False, z=1.0).model_copy(update={"generated_at": GEN})
    doc = build_trade_plan_line_document(
        line="micro_fast",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=micro,
        micro_state_doc=_micro_state(fast_ready=True, full_ready=False),
        generated_at=GEN,
    )
    plan = doc.plans[0]
    assert plan.executable is False
    assert plan.action == "WAIT"
    assert plan.entry_mode == "WAIT_CONFIRMATION"
    assert "range_room_not_enough" in plan.reason_codes
    assert "market_only_no_pending" in plan.reason_codes
    assert "limit_entry_available" not in plan.reason_codes


def test_step104_micro_full_outputs_wait_with_legacy_full_fallback() -> None:
    factor = _fresh_factor()
    refresh = _fresh_refresh(factor)
    micro = _micro_doc(fast_ready=True, full_ready=True, z=1.0).model_copy(update={"generated_at": GEN})
    doc = build_trade_plan_line_document(
        line="micro_full",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=micro,
        micro_state_doc=_micro_state(fast_ready=True, full_ready=True),
        generated_at=GEN,
    )
    plan = doc.plans[0]
    assert doc.source == "trade_plan_micro_full"
    assert doc.micro_mode == "full"
    assert plan.executable is False
    assert plan.action == "WAIT"
    assert plan.guards["micro_ready"] is True
    assert "market_only_no_pending" in plan.reason_codes


def test_step105_abc_audit_accepts_three_independent_outputs(tmp_path: Path) -> None:
    factor = _fresh_factor()
    factor_without = _fresh_factor(without_micro=True)
    refresh = _fresh_refresh(factor)
    micro = _micro_doc(fast_ready=True, full_ready=True, z=1.0).model_copy(update={"generated_at": GEN})
    docs = {
        "without_micro": build_trade_plan_line_document(
            line="without_micro",
            factor_doc=factor_without,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=None,
            generated_at=GEN,
        ),
        "micro_fast": build_trade_plan_line_document(
            line="micro_fast",
            factor_doc=factor,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=micro,
            micro_state_doc=_micro_state(fast_ready=True, full_ready=True),
            generated_at=GEN,
        ),
        "micro_full": build_trade_plan_line_document(
            line="micro_full",
            factor_doc=factor,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=micro,
            generated_at=GEN,
        ),
    }
    out_dir = tmp_path / "DATA" / "decisions"
    out_dir.mkdir(parents=True)
    names = {
        "without_micro": "latest_trade_plan_without_micro.json",
        "micro_fast": "latest_trade_plan_micro_fast.json",
        "micro_full": "latest_trade_plan_micro_full.json",
    }
    for line, doc in docs.items():
        (out_dir / names[line]).write_text(json.dumps(doc.model_dump(mode="json")), encoding="utf-8")
    report = build_trade_plan_lines_audit(project_root=tmp_path, generated_at=GEN)
    assert report["failure_count"] == 0
    assert report["status"] == "ok"


def test_step1011_abc_audit_accepts_independent_micro_state_refs(tmp_path: Path) -> None:
    factor = _fresh_factor()
    factor_without = _fresh_factor(without_micro=True)
    refresh = _fresh_refresh(factor)
    micro = _micro_doc(fast_ready=True, full_ready=True, z=1.0).model_copy(update={"generated_at": GEN})
    docs = {
        "without_micro": build_trade_plan_line_document(
            line="without_micro",
            factor_doc=factor_without,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=None,
            generated_at=GEN,
        ),
        "micro_fast": build_trade_plan_line_document(
            line="micro_fast",
            factor_doc=factor,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=micro,
            micro_state_doc=_micro_state(fast_ready=True, full_ready=True),
            generated_at=GEN,
        ),
        "micro_full": build_trade_plan_line_document(
            line="micro_full",
            factor_doc=factor,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=micro,
            micro_state_doc=_micro_state(fast_ready=True, full_ready=True).model_copy(
                update={"generated_at": "2099-01-01T00:05:00Z"},
            ),
            generated_at=GEN,
        ),
    }
    out_dir = tmp_path / "DATA" / "decisions"
    out_dir.mkdir(parents=True)
    names = {
        "without_micro": "latest_trade_plan_without_micro.json",
        "micro_fast": "latest_trade_plan_micro_fast.json",
        "micro_full": "latest_trade_plan_micro_full.json",
    }
    for line, doc in docs.items():
        (out_dir / names[line]).write_text(json.dumps(doc.model_dump(mode="json")), encoding="utf-8")

    report = build_trade_plan_lines_audit(project_root=tmp_path, generated_at=GEN)
    check_names = {check["name"]: check for check in report["checks"]}

    assert report["status"] == "ok"
    assert "micro_lines.same_state_ref" not in check_names
    assert check_names["micro_lines.independent_state_refs"]["ok"] is True


def test_step107_micro_full_uses_state_warmup_reason() -> None:
    factor = _fresh_factor()
    refresh = _fresh_refresh(factor)
    micro = _micro_doc(fast_ready=True, full_ready=False, z=1.0).model_copy(update={"generated_at": GEN})
    doc = build_trade_plan_line_document(
        line="micro_full",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=micro,
        micro_state_doc=_micro_state(fast_ready=True, full_ready=False),
        generated_at=GEN,
    )
    assert doc.plans == []
    excluded = doc.input_refs["micro_lifecycle_excluded_items"][0]
    assert excluded["state"] == "not_ready"
    assert excluded["trade_plan_consumable"] is False
    assert "full_warmup_incomplete" in excluded["plan_reason_codes"]


def test_step1027_micro_full_warmup_gate_can_be_disabled_by_config() -> None:
    factor = _fresh_factor()
    refresh = _fresh_refresh(factor)
    micro = _micro_doc(fast_ready=True, full_ready=False, z=1.0).model_copy(update={"generated_at": GEN})
    cfg = TradePlanLineConfig(
        allow_market_entry=True,
        allow_wait_plan=True,
        min_score=0,
        require_refresh_fresh=True,
        require_direction_still_valid=True,
        require_range_room_ok=False,
        require_liquidity_ok=False,
        require_micro_ready=False,
        require_micro_alignment=False,
        max_refresh_age_sec=300,
        max_liquidity_age_sec=300,
        max_micro_age_sec=1500,
        target_rr=1.5,
        min_rr=1.0,
        stop_atr_mult=1.5,
        max_stop_atr_mult=2.5,
        min_net_rr=0.2,
        min_tp_after_cost_bps=0,
        max_stop_bps=600,
    )
    doc = build_trade_plan_line_document(
        line="micro_full",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=None,
        micro_doc=micro,
        micro_state_doc=_micro_state(fast_ready=True, full_ready=False),
        generated_at=GEN,
        cfg=cfg,
    )
    assert doc.plans == []
    excluded = doc.input_refs["micro_lifecycle_excluded_items"][0]
    assert excluded["state"] == "not_ready"
    assert excluded["trade_plan_consumable"] is False
    assert doc.input_refs["micro_consumption_policy"] == "confirmed_only"


def test_step1027_guards_include_gate_config_snapshot() -> None:
    factor = _fresh_factor(without_micro=True)
    refresh = _fresh_refresh(factor)
    doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=factor,
        refresh_doc=refresh,
        liquidity_doc=_liquidity(ok=True),
        micro_doc=None,
        generated_at=GEN,
    )
    snap = doc.plans[0].guards["gate_config_snapshot"]
    assert snap["line"] == "without_micro"
    assert snap["min_score"] == doc.plans[0].guards["min_score"]
    assert "min_net_rr" in snap
    assert "liquidity_notional_usdt" in snap


def _write_trade_plan_docs_for_audit(tmp_path: Path, docs: dict[str, TradePlanLineDocument]) -> None:
    out_dir = tmp_path / "DATA" / "decisions"
    out_dir.mkdir(parents=True)
    names = {
        "without_micro": "latest_trade_plan_without_micro.json",
        "micro_fast": "latest_trade_plan_micro_fast.json",
        "micro_full": "latest_trade_plan_micro_full.json",
    }
    for line, doc in docs.items():
        payload = doc if isinstance(doc, dict) else doc.model_dump(mode="json")
        (out_dir / names[line]).write_text(json.dumps(payload), encoding="utf-8")


def test_step1029_relaxed_micro_executable_bypass_is_warning(tmp_path: Path) -> None:
    factor = _fresh_factor()
    factor_without = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    micro = _micro_doc(fast_ready=True, full_ready=True, z=1.0).model_copy(update={"generated_at": GEN})
    docs = {
        "without_micro": build_trade_plan_line_document(
            line="without_micro",
            factor_doc=factor_without,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=None,
            generated_at=GEN,
        ),
        "micro_fast": build_trade_plan_line_document(
            line="micro_fast",
            factor_doc=factor,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=micro,
            micro_state_doc=_micro_state(fast_ready=True, full_ready=True),
            generated_at=GEN,
        ),
        "micro_full": build_trade_plan_line_document(
            line="micro_full",
            factor_doc=factor,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=micro,
            micro_state_doc=_micro_state(fast_ready=True, full_ready=True),
            generated_at=GEN,
        ),
    }
    raw_docs = {line: doc.model_dump(mode="json") for line, doc in docs.items()}
    fast_plan = raw_docs["micro_fast"]["plans"][0]
    assert fast_plan["executable"] is True
    fast_plan["guards"]["micro_signal_usable"] = False
    fast_plan["guards"]["micro_direction_confirmed"] = False
    fast_plan["guards"]["micro_exec_allowed"] = False
    fast_plan["guards"]["gate_config_snapshot"]["require_micro_ready"] = False
    fast_plan["guards"]["gate_config_snapshot"]["require_micro_alignment"] = False

    _write_trade_plan_docs_for_audit(tmp_path, raw_docs)
    report = build_trade_plan_lines_audit(project_root=tmp_path, generated_at=GEN)

    assert report["failure_count"] == 0
    assert report["warning_count"] == 1
    assert report["status"] == "warning"
    assert report["audit_profile"] == "relaxed"
    assert report["warnings"][0]["name"] == "relaxed_micro_confirmation_bypass"


def test_step1029_strict_micro_executable_without_confirmation_fails(tmp_path: Path) -> None:
    factor = _fresh_factor()
    factor_without = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    micro = _micro_doc(fast_ready=True, full_ready=True, z=1.0).model_copy(update={"generated_at": GEN})
    docs = {
        "without_micro": build_trade_plan_line_document(
            line="without_micro",
            factor_doc=factor_without,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=None,
            generated_at=GEN,
        ),
        "micro_fast": build_trade_plan_line_document(
            line="micro_fast",
            factor_doc=factor,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=micro,
            micro_state_doc=_micro_state(fast_ready=True, full_ready=True),
            generated_at=GEN,
        ),
        "micro_full": build_trade_plan_line_document(
            line="micro_full",
            factor_doc=factor,
            refresh_doc=refresh,
            liquidity_doc=None,
            micro_doc=micro,
            micro_state_doc=_micro_state(fast_ready=True, full_ready=True),
            generated_at=GEN,
        ),
    }
    raw_docs = {line: doc.model_dump(mode="json") for line, doc in docs.items()}
    fast_plan = raw_docs["micro_fast"]["plans"][0]
    assert fast_plan["executable"] is True
    fast_plan["guards"]["micro_signal_usable"] = False
    fast_plan["guards"]["micro_direction_confirmed"] = False
    fast_plan["guards"]["micro_exec_allowed"] = False
    fast_plan["guards"]["gate_config_snapshot"]["require_micro_ready"] = True
    fast_plan["guards"]["gate_config_snapshot"]["require_micro_alignment"] = True

    _write_trade_plan_docs_for_audit(tmp_path, raw_docs)
    report = build_trade_plan_lines_audit(project_root=tmp_path, generated_at=GEN)

    assert report["failure_count"] == 1
    assert report["warning_count"] == 0
    assert report["status"] == "failed"
    assert any(c["name"] == "plan.executable_micro_signal_contract" and not c["ok"] for c in report["checks"])


def test_step1014_micro_line_blocks_when_factor_blocked(tmp_path: Path) -> None:
    raw = _fresh_factor().model_dump(mode="json")
    raw.update(
        {
            "status": "blocked",
            "count": 0,
            "items": [],
            "input_refs": {
                "blocked_reason": "micro_targets_stale_input",
                "reason_codes": ["micro_targets_stale_input", "step2_watch_strong_stale"],
            },
            "candidate_alignment": {"blocked_by": "step2_stale"},
        },
    )
    factor = FactorSnapshotDocument.model_validate(raw)
    factor_p = tmp_path / "factor.json"
    out_p = tmp_path / "fast.json"
    factor_p.write_text(json.dumps(factor.model_dump(mode="json")), encoding="utf-8")
    code = run_apply_trade_plan_line_safe(
        line="micro_fast",
        project_root=tmp_path,
        factor_path=factor_p,
        output_path=out_p,
    )
    assert code == 0
    doc = TradePlanLineDocument.model_validate(json.loads(out_p.read_text(encoding="utf-8")))
    assert doc.status == "blocked"
    assert doc.count == 0
    assert doc.input_refs["blocked_reason"] == "upstream_step2_stale"
    assert "micro_targets_stale_input" in doc.input_refs["reason_codes"]


def test_step1014_audit_requires_blocked_micro_lines_when_targets_stale(tmp_path: Path) -> None:
    out_dir = tmp_path / "DATA" / "decisions"
    out_dir.mkdir(parents=True)
    micro_dir = tmp_path / "DATA" / "micro"
    micro_dir.mkdir(parents=True)
    (micro_dir / "micro_targets.json").write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": GEN,
                "source": "micro_target_router",
                "status": "stale_input",
                "warm_watch_limit": 30,
                "active_strong_limit": 10,
                "input_watch_status": "stale_input",
                "input_strong_status": "stale_input",
                "input_snapshot_generated_at": GEN,
                "input_snapshot_age_sec": 600,
                "router_freshness_ok": False,
                "input_counts": {"raw": 0, "watch": 0, "strong": 0},
                "routed_counts": {"tier1": 0, "tier2": 0},
                "truncated": {"tier1": False, "tier2": False},
                "skip_reasons": ["watch_input_stale", "strong_input_stale"],
                "block_downstream": True,
                "block_reason": "step2_stale",
                "tier1_warm_watch": [],
                "tier2_active_strong": [],
            },
        ),
        encoding="utf-8",
    )
    without_doc = build_trade_plan_line_document(
        line="without_micro",
        factor_doc=_fresh_factor(without_micro=True),
        refresh_doc=_fresh_refresh(_fresh_factor(without_micro=True)),
        liquidity_doc=None,
        micro_doc=None,
        generated_at=GEN,
    )
    (out_dir / "latest_trade_plan_without_micro.json").write_text(
        json.dumps(without_doc.model_dump(mode="json")),
        encoding="utf-8",
    )
    for line, name in (
        ("micro_fast", "latest_trade_plan_micro_fast.json"),
        ("micro_full", "latest_trade_plan_micro_full.json"),
    ):
        doc = TradePlanLineDocument(
            generated_at=GEN,
            source="trade_plan_micro_fast" if line == "micro_fast" else "trade_plan_micro_full",
            micro_mode="fast" if line == "micro_fast" else "full",
            status="blocked",
            count=0,
            executable_count=0,
            input_refs={"blocked_reason": "upstream_step2_stale"},
            plans=[],
        )
        (out_dir / name).write_text(json.dumps(doc.model_dump(mode="json")), encoding="utf-8")

    report = build_trade_plan_lines_audit(project_root=tmp_path, generated_at=GEN)
    checks = [
        c
        for c in report["checks"]
        if c["name"] == "micro_targets.stale_input_blocks_line"
    ]
    assert len(checks) == 2
    assert all(c["ok"] for c in checks)


def test_trade_plan_line_cli_writes_without_micro_json(tmp_path: Path) -> None:
    _write_symbol_whitelist(tmp_path, "BTCUSDT")
    factor = _fresh_factor(without_micro=True)
    refresh = _fresh_refresh(factor)
    factor_p = tmp_path / "factor_without.json"
    refresh_p = tmp_path / "refresh.json"
    out_p = tmp_path / "without.json"
    factor_p.write_text(json.dumps(factor.model_dump(mode="json")), encoding="utf-8")
    refresh_p.write_text(json.dumps(refresh.model_dump(mode="json")), encoding="utf-8")
    code = run_apply_trade_plan_line_safe(
        line="without_micro",
        project_root=tmp_path,
        factor_path=factor_p,
        refresh_path=refresh_p,
        output_path=out_p,
    )
    assert code == 0
    assert out_p.is_file()


def test_step1056_live_profile_can_block_low_market_entry_trade_plan(tmp_path: Path) -> None:
    _write_light_profile(
        tmp_path,
        symbol="BTCUSDT",
        market_entry_score=20,
        slippage_risk_score=30,
        trade_quality_tier="observe_only",
    )
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "range_pos": 0.45,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    liquidity = _sell_liquidity(ok=True)
    factor_p = tmp_path / "factor_without.json"
    refresh_p = tmp_path / "refresh.json"
    liquidity_p = tmp_path / "liquidity.json"
    out_p = tmp_path / "without.json"
    factor_p.write_text(json.dumps(factor.model_dump(mode="json")), encoding="utf-8")
    refresh_p.write_text(json.dumps(refresh.model_dump(mode="json")), encoding="utf-8")
    liquidity_p.write_text(json.dumps(liquidity.model_dump(mode="json")), encoding="utf-8")

    code = run_apply_trade_plan_line_safe(
        line="without_micro",
        project_root=tmp_path,
        factor_path=factor_p,
        refresh_path=refresh_p,
        liquidity_path=liquidity_p,
        output_path=out_p,
    )

    assert code == 0
    doc = TradePlanLineDocument.model_validate(json.loads(out_p.read_text(encoding="utf-8")))
    assert doc.plans
    plan = doc.plans[0]
    assert plan.executable is False
    assert "profile_market_entry_score_too_low" in plan.reason_codes
    assert plan.input_refs["tradability_profile"]["market_entry_score"] == 20
    assert plan.guards["trade_quality_tier"] == "observe_only"


def test_step1056_profile_gate_threshold_is_configurable(tmp_path: Path) -> None:
    _write_light_profile(
        tmp_path,
        symbol="BTCUSDT",
        market_entry_score=25,
        slippage_risk_score=30,
        trade_quality_tier="observe_only",
    )
    cfg_path = tmp_path / "laoma_signal_engine/config/default.yaml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        """
trade_plan_lines:
  without_micro:
    min_profile_market_entry_score: 20
    min_profile_hf_stop_score: 0
    max_profile_slippage_risk_score: 90
""".strip(),
        encoding="utf-8",
    )
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "range_pos": 0.45,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    liquidity = _sell_liquidity(ok=True)
    factor_p = tmp_path / "factor_without.json"
    refresh_p = tmp_path / "refresh.json"
    liquidity_p = tmp_path / "liquidity.json"
    out_p = tmp_path / "without.json"
    factor_p.write_text(json.dumps(factor.model_dump(mode="json")), encoding="utf-8")
    refresh_p.write_text(json.dumps(refresh.model_dump(mode="json")), encoding="utf-8")
    liquidity_p.write_text(json.dumps(liquidity.model_dump(mode="json")), encoding="utf-8")

    code = run_apply_trade_plan_line_safe(
        line="without_micro",
        project_root=tmp_path,
        factor_path=factor_p,
        refresh_path=refresh_p,
        liquidity_path=liquidity_p,
        output_path=out_p,
    )

    assert code == 0
    doc = TradePlanLineDocument.model_validate(json.loads(out_p.read_text(encoding="utf-8")))
    plan = doc.plans[0]
    assert "profile_market_entry_score_too_low" not in plan.reason_codes
    assert plan.guards["min_profile_market_entry_score"] == 20
    assert plan.guards["max_profile_slippage_risk_score"] == 90


def test_step161_trade_plan_preserves_business_pool_hydration_contract(tmp_path: Path) -> None:
    _write_universe_profile(tmp_path, symbol="BTCUSDT", business_pool="active_alt")
    _write_light_profile(tmp_path, symbol="BTCUSDT")
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "range_pos": 0.45,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    liquidity = _sell_liquidity(ok=True)
    factor_p = tmp_path / "factor_without.json"
    refresh_p = tmp_path / "refresh.json"
    liquidity_p = tmp_path / "liquidity.json"
    out_p = tmp_path / "without.json"
    factor_p.write_text(json.dumps(factor.model_dump(mode="json")), encoding="utf-8")
    refresh_p.write_text(json.dumps(refresh.model_dump(mode="json")), encoding="utf-8")
    liquidity_p.write_text(json.dumps(liquidity.model_dump(mode="json")), encoding="utf-8")

    code = run_apply_trade_plan_line_safe(
        line="without_micro",
        project_root=tmp_path,
        factor_path=factor_p,
        refresh_path=refresh_p,
        liquidity_path=liquidity_p,
        output_path=out_p,
    )

    assert code == 0
    doc = TradePlanLineDocument.model_validate(json.loads(out_p.read_text(encoding="utf-8")))
    plan = doc.plans[0]
    assert plan.guards["business_pool"] == "active_alt"
    assert plan.guards["profile_hydration_status"] == "ok"
    assert plan.guards["profile_hydration_reason_codes"] == []
    assert plan.input_refs["profile_hydration"]["source"] == "candidate_universe"


def test_step1058_without_micro_executable_keeps_profile_guard_contract_when_profile_missing(tmp_path: Path) -> None:
    _write_symbol_whitelist(tmp_path, "BTCUSDT")
    factor = _fresh_factor(without_micro=True)
    refresh = _refresh_with(
        factor,
        primary_15m={
            **_fresh_refresh(factor).items[0].primary_15m,
            "range_pos": 0.45,
            "recent_swing_high": 103.0,
            "breakout_level": 103.0,
        },
    )
    liquidity = _sell_liquidity(ok=True)
    factor_p = tmp_path / "factor_without.json"
    refresh_p = tmp_path / "refresh.json"
    liquidity_p = tmp_path / "liquidity.json"
    out_p = tmp_path / "without.json"
    factor_p.write_text(json.dumps(factor.model_dump(mode="json")), encoding="utf-8")
    refresh_p.write_text(json.dumps(refresh.model_dump(mode="json")), encoding="utf-8")
    liquidity_p.write_text(json.dumps(liquidity.model_dump(mode="json")), encoding="utf-8")

    code = run_apply_trade_plan_line_safe(
        line="without_micro",
        project_root=tmp_path,
        factor_path=factor_p,
        refresh_path=refresh_p,
        liquidity_path=liquidity_p,
        output_path=out_p,
    )

    assert code == 0
    doc = TradePlanLineDocument.model_validate(json.loads(out_p.read_text(encoding="utf-8")))
    plan = doc.plans[0]
    assert plan.executable is True
    required = {
        "business_pool",
        "scan_eligibility",
        "symbol_execution_tier",
        "symbol_liquidity_tier",
        "symbol_rr_policy",
        "sl_template",
        "rr_template",
        "sizing_template",
        "feishu_policy",
        "profile_hydration_status",
        "profile_hydration_reason_codes",
        "primary_pool",
        "pool_tags",
        "trade_quality_tier",
        "market_entry_score",
        "hf_stop_score",
        "slippage_risk_score",
        "depth_stability_score",
        "profile_gate_enabled",
        "min_profile_market_entry_score",
        "min_profile_hf_stop_score",
        "max_profile_slippage_risk_score",
        "symbol_contract_ok",
        "symbol_contract_reason",
        "symbol_contract_source",
    }
    assert required <= set(plan.guards)
    assert plan.guards["profile_hydration_status"] == "missing"
    assert "tradability_profile_missing" in plan.guards["profile_hydration_reason_codes"]
    assert plan.input_refs["profile_hydration"]["status"] == "missing"
