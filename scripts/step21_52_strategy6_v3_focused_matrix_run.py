from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21_v2 import (
    download_kline_cache_payload,
    kline_cache_status_payload,
    run_config_matrix_streaming_payload,
    universe_symbols,
)


GRID_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_focused_40params_STEP21_52.json"
PROGRESS_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_focused_STEP21_52_progress.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_focused_STEP21_52_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stable_id(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return "s6v3_" + hashlib.sha1(raw).hexdigest()[:16]


def build_grid() -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    def add_profile(
        *,
        direction_context: int,
        reverse_1m: float,
        reverse_3m: float,
        max_slippage: float,
        second_acceptance: float,
        rr_cap: float,
        max_wait: int,
    ) -> None:
        strategy6 = {
            "strategy6_version": "v3",
            "v2_min_direction_acceptance_score": max(40, direction_context - 8),
            "v2_uncertain_direction_score": max(34, direction_context - 14),
            "v2_hard_deny_direction_score": max(25, direction_context - 24),
            "v2_max_chase_bps": max_slippage,
            "v2_adverse_1m_deny_bps": reverse_1m,
            "v2_reversal_1m_wait_bps": max(5.0, reverse_1m * 0.5),
            "v2_distance_from_mean_max_bps": max(85.0, max_slippage * 1.6),
            "v2_high_quality_score": 74,
            "v2_medium_quality_score": max(46, direction_context - 2),
            "v3_min_direction_context_score": direction_context,
            "v3_uncertain_direction_context_score": max(34, direction_context - 10),
            "v3_hard_deny_context_score": max(25, direction_context - 22),
            "v3_reverse_1m_deny_bps": reverse_1m,
            "v3_reverse_3m_deny_bps": reverse_3m,
            "v3_fake_breakout_range_pos": 0.95,
            "v3_second_acceptance_min_bps": second_acceptance,
            "v3_max_entry_slippage_bps": max_slippage,
            "v3_quality_filter_mode": "shadow",
            "strategy6_wait_rebound_enabled": True,
            "strategy6_wait_max_minutes": max_wait,
            "strategy6_wait_pullback_min_bps": 6.0,
            "strategy6_wait_pullback_max_bps": 60.0,
            "strategy6_wait_confirm_bars": 1,
            "strategy6_backtest_max_effective_planned_rr": rr_cap,
            "strategy6_adaptive_exit_enabled": True,
            "medium_quality_loss_cap_R": 0.95,
            "medium_quality_first_tp_R": 0.60,
            "medium_quality_protect_after_mfe_R": 0.50,
            "medium_quality_trail_after_mfe_R": 0.40,
            "low_quality_loss_cap_R": 0.75,
            "low_quality_first_tp_R": 0.42,
            "low_quality_protect_after_mfe_R": 0.35,
            "low_quality_trail_after_mfe_R": 0.25,
            "high_quality_loss_cap_R": 1.00,
            "high_quality_protect_after_mfe_R": 0.80,
            "high_quality_trail_after_mfe_R": 0.60,
        }
        params = {
            "strategy_line": "strategy6",
            "min_score": 45.0,
            "target_rr": rr_cap,
            "min_rr": min(0.5, rr_cap),
            "min_net_rr": min(0.55, rr_cap),
            "min_effective_rr": max(0.30, min(0.50, rr_cap - 0.05)),
            "stop_atr_mult": 1.2,
            "max_stop_bps": 180.0,
            "min_stop_bps": 3.0,
            "min_reachable_reward_bps": 6.0,
            "tp_target_policy": {"mode": "fast_capped_rr", "target_net_rr": rr_cap, "target_rr_cap": rr_cap},
            "range_room": {"long_max_range_pos": 0.96, "short_min_range_pos": 0.04},
            "taker_fee_bps": 5.0,
            "slippage_bps": 2.0,
            "max_hold_minutes": 120,
            "strategy6": strategy6,
        }
        candidates.append({"parameter_set_id": _stable_id(params), "parameters": params})

    add_profile(direction_context=42, reverse_1m=48.0, reverse_3m=80.0, max_slippage=140.0, second_acceptance=-20.0, rr_cap=0.60, max_wait=10)
    add_profile(direction_context=48, reverse_1m=32.0, reverse_3m=55.0, max_slippage=100.0, second_acceptance=0.0, rr_cap=0.60, max_wait=10)
    add_profile(direction_context=52, reverse_1m=24.0, reverse_3m=42.0, max_slippage=80.0, second_acceptance=2.0, rr_cap=0.75, max_wait=14)
    direction_contexts = [54, 58, 62, 66]
    reverse_1m_caps = [8.0, 12.0, 18.0]
    reverse_3m_caps = [18.0, 24.0, 34.0]
    max_slippage_caps = [30.0, 45.0, 65.0]
    second_acceptances = [2.0, 4.0, 7.0]
    rr_caps = [0.45, 0.6, 0.75]
    max_waits = [6, 10, 14]
    exit_profiles = [
        {
            "medium_quality_loss_cap_R": 0.85,
            "medium_quality_first_tp_R": 0.50,
            "medium_quality_protect_after_mfe_R": 0.40,
            "medium_quality_trail_after_mfe_R": 0.30,
        },
        {
            "medium_quality_loss_cap_R": 0.95,
            "medium_quality_first_tp_R": 0.60,
            "medium_quality_protect_after_mfe_R": 0.50,
            "medium_quality_trail_after_mfe_R": 0.40,
        },
    ]
    for direction_context in direction_contexts:
        for reverse_1m in reverse_1m_caps:
            for reverse_3m in reverse_3m_caps:
                for max_slippage in max_slippage_caps:
                    for second_acceptance in second_acceptances:
                        for rr_cap in rr_caps:
                            for max_wait in max_waits:
                                for exit_profile in exit_profiles:
                                    strategy6 = {
                                        "strategy6_version": "v3",
                                        "v2_min_direction_acceptance_score": max(50, direction_context - 4),
                                        "v2_uncertain_direction_score": max(42, direction_context - 14),
                                        "v2_hard_deny_direction_score": max(30, direction_context - 24),
                                        "v2_max_chase_bps": max_slippage + 10.0,
                                        "v2_adverse_1m_deny_bps": max(14.0, reverse_1m + 4.0),
                                        "v2_reversal_1m_wait_bps": max(5.0, reverse_1m - 4.0),
                                        "v2_distance_from_mean_max_bps": max(60.0, max_slippage * 1.8),
                                        "v2_high_quality_score": 74,
                                        "v2_medium_quality_score": max(56, direction_context - 2),
                                        "v3_min_direction_context_score": direction_context,
                                        "v3_uncertain_direction_context_score": max(42, direction_context - 10),
                                        "v3_hard_deny_context_score": max(30, direction_context - 22),
                                        "v3_reverse_1m_deny_bps": reverse_1m,
                                        "v3_reverse_3m_deny_bps": reverse_3m,
                                        "v3_fake_breakout_range_pos": 0.88,
                                        "v3_second_acceptance_min_bps": second_acceptance,
                                        "v3_max_entry_slippage_bps": max_slippage,
                                        "v3_quality_filter_mode": "shadow",
                                        "strategy6_wait_rebound_enabled": True,
                                        "strategy6_wait_max_minutes": max_wait,
                                        "strategy6_wait_pullback_min_bps": 6.0,
                                        "strategy6_wait_pullback_max_bps": 48.0,
                                        "strategy6_wait_confirm_bars": 2,
                                        "strategy6_backtest_max_effective_planned_rr": rr_cap,
                                        "strategy6_adaptive_exit_enabled": True,
                                        **exit_profile,
                                        "low_quality_loss_cap_R": 0.75,
                                        "low_quality_first_tp_R": 0.42,
                                        "low_quality_protect_after_mfe_R": 0.35,
                                        "low_quality_trail_after_mfe_R": 0.25,
                                        "high_quality_loss_cap_R": 1.00,
                                        "high_quality_protect_after_mfe_R": 0.80,
                                        "high_quality_trail_after_mfe_R": 0.60,
                                    }
                                    params = {
                                        "strategy_line": "strategy6",
                                        "min_score": max(48.0, float(direction_context) - 8.0),
                                        "target_rr": rr_cap,
                                        "min_rr": min(0.5, rr_cap),
                                        "min_net_rr": min(0.55, rr_cap),
                                        "min_effective_rr": max(0.30, min(0.50, rr_cap - 0.05)),
                                        "stop_atr_mult": 1.2,
                                        "max_stop_bps": 180.0,
                                        "min_stop_bps": 3.0,
                                        "min_reachable_reward_bps": 6.0,
                                        "tp_target_policy": {
                                            "mode": "fast_capped_rr",
                                            "target_net_rr": rr_cap,
                                            "target_rr_cap": rr_cap,
                                        },
                                        "range_room": {"long_max_range_pos": 0.92, "short_min_range_pos": 0.08},
                                        "taker_fee_bps": 5.0,
                                        "slippage_bps": 2.0,
                                        "max_hold_minutes": 120,
                                        "strategy6": strategy6,
                                    }
                                    candidates.append({"parameter_set_id": _stable_id(params), "parameters": params})
    picks: list[dict[str, Any]] = candidates[:3]
    for idx in [0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377]:
        item = candidates[idx % len(candidates)]
        if item["parameter_set_id"] not in {p["parameter_set_id"] for p in picks}:
            picks.append(item)
    stride = max(1, len(candidates) // 80)
    for item in candidates[0::stride]:
        if item["parameter_set_id"] not in {p["parameter_set_id"] for p in picks}:
            picks.append(item)
        if len(picks) >= 40:
            break
    return picks


def ensure_grid(regenerate: bool = False) -> list[dict[str, Any]]:
    GRID_PATH.parent.mkdir(parents=True, exist_ok=True)
    if regenerate and GRID_PATH.exists():
        GRID_PATH.unlink()
    if GRID_PATH.exists():
        return json.loads(GRID_PATH.read_text(encoding="utf-8"))["parameter_sets"]
    grid = build_grid()
    GRID_PATH.write_text(
        json.dumps(
            {
                "schema_version": "step21.52-strategy6-v3-focused-grid-v1",
                "purpose": "focused Strategy6 V3 matrix; V1/V2/default strategy behavior remains unchanged",
                "parameter_sets": grid,
                "generated_at": _now(),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return grid


def write_report(result: dict[str, Any], *, smoke: bool) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP21.52_strategy6_v3_focused_matrix_{ts}.md"
    best = result.get("best") or {}
    metrics = best.get("metrics") or {}
    leaderboard = result.get("leaderboard") or []
    lines = [
        "# STEP21.52 Strategy6 V3 Focused Matrix Backtest Report",
        "",
        f"- generated_at: `{_now()}`",
        f"- mode: `{'smoke' if smoke else 'focused'}`",
        f"- experiment_id: `{result.get('experiment_id')}`",
        f"- parameter_set_count: `{result.get('parameter_set_count')}`",
        f"- trade_count: `{result.get('trade_count')}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        f"- progress_json: `{PROGRESS_PATH.relative_to(ROOT)}`",
        "",
        "## Best",
        "",
        f"- parameter_set_id: `{best.get('parameter_set_id')}`",
        f"- profit_factor: `{metrics.get('profit_factor')}`",
        f"- expectancy_R: `{metrics.get('expectancy_R')}`",
        f"- win_rate: `{metrics.get('win_rate')}`",
        f"- trade_count: `{metrics.get('trade_count')}`",
        f"- total_R: `{metrics.get('total_R')}`",
        f"- max_drawdown_R: `{metrics.get('max_drawdown_R')}`",
        "",
        "## Top Leaderboard",
        "",
        "| rank | parameter_set_id | PF | expectancy_R | win_rate | trades | total_R |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in leaderboard[:10]:
        m = item.get("metrics") or {}
        lines.append(
            f"| {item.get('rank')} | `{item.get('parameter_set_id')}` | "
            f"{m.get('profit_factor')} | {m.get('expectancy_R')} | {m.get('win_rate')} | "
            f"{m.get('trade_count')} | {m.get('total_R')} |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Only Strategy6 V3 offline backtest was executed.",
            "- No live config, paper ledger, Feishu, or other strategy line was changed.",
            "- This report is evidence for STEP7.105; it is not a production recommendation.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true", help="Run a tiny 5 symbols * 3 params validation")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--max-sets", type=int, default=None)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--symbol-shard-size", type=int, default=5)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--download-sleep-sec", type=float, default=0.02)
    parser.add_argument("--regenerate-grid", action="store_true")
    parser.add_argument("--resume-experiment-id", default=None)
    args = parser.parse_args()

    grid = ensure_grid(args.regenerate_grid)
    max_symbols = args.max_symbols if args.max_symbols is not None else (5 if args.smoke else 100)
    max_sets = args.max_sets if args.max_sets is not None else (3 if args.smoke else 40)
    selected_symbols = [s.upper() for s in universe_symbols(ROOT, limit=max_symbols)[:max_symbols]]
    if not args.skip_download:
        status = kline_cache_status_payload(ROOT, symbols=selected_symbols, days=30, max_symbols=len(selected_symbols))
        missing = [row["symbol"] for row in status.get("symbols", []) if row.get("status") != "ready"]
        if missing:
            print(json.dumps({"phase": "download_missing_klines", "missing_count": len(missing), "symbols": missing[:10]}, ensure_ascii=False), flush=True)
            download_kline_cache_payload(ROOT, symbols=missing, days=30, max_symbols=len(missing), sleep_sec=args.download_sleep_sec)

    progress_last = {"done": 0, "t": 0.0}

    def cb(progress: dict[str, Any]) -> None:
        payload = dict(progress)
        payload["updated_at"] = _now()
        PROGRESS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        done = int(payload.get("done_count") or 0)
        total = int(payload.get("total_count") or 0)
        now = time.time()
        if done == 1 or done == total or done - int(progress_last["done"]) >= 10 or now - float(progress_last["t"]) >= 60:
            progress_last["done"] = done
            progress_last["t"] = now
            print(
                json.dumps(
                    {
                        "done_count": done,
                        "total_count": total,
                        "current_parameter_set_id": payload.get("current_parameter_set_id"),
                        "current_symbol_shard": payload.get("current_symbol_shard"),
                        "persisted_order_count": payload.get("persisted_order_count"),
                        "max_workers": payload.get("max_workers"),
                        "active_workers": payload.get("active_workers"),
                        "updated_at": payload.get("updated_at"),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    payload = run_config_matrix_streaming_payload(
        ROOT,
        symbols=selected_symbols,
        strategy_line="strategy6",
        days=30,
        max_symbols=max_symbols,
        max_sets=max_sets,
        parameter_grid=grid[:max_sets],
        write=True,
        symbol_shard_size=args.symbol_shard_size,
        max_workers=args.max_workers,
        scheduler_mode="global_queue",
        resume_experiment_id=args.resume_experiment_id,
        progress_callback=cb,
    )
    result = {
        "schema_version": "step21.52-result-v1",
        "mode": "smoke" if args.smoke else "focused",
        "experiment_id": payload.get("experiment_id"),
        "strategy_line": payload.get("strategy_line"),
        "parameter_set_count": payload.get("parameter_set_count"),
        "trade_count": payload.get("trade_count"),
        "best": payload.get("best"),
        "leaderboard": payload.get("leaderboard", [])[:20],
        "symbols": payload.get("symbols", []),
        "generated_at": payload.get("generated_at"),
    }
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report_path = write_report(result, smoke=args.smoke)
    print(
        json.dumps(
            {
                "result_path": str(RESULT_PATH),
                "report_path": str(report_path),
                "experiment_id": result["experiment_id"],
                "best_parameter_set_id": ((result.get("best") or {}).get("parameter_set_id")),
                "best_metrics": ((result.get("best") or {}).get("metrics")),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
