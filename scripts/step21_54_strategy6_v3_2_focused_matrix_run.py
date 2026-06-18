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


GRID_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_focused_60params_STEP21_54.json"
PROGRESS_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_focused_STEP21_54_progress.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_2_focused_STEP21_54_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stable_id(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return "s6v32_" + hashlib.sha1(raw).hexdigest()[:16]


def _params(
    *,
    long_context: int,
    short_context: int,
    long_reverse_1m: float,
    short_reverse_1m: float,
    first_tp: float,
    loss_cap: float,
    long_btc_action: str,
    quality_mode: str = "shadow",
    bad_sides: list[str] | None = None,
    bad_symbols: list[str] | None = None,
    bad_hours: list[int] | None = None,
) -> dict[str, Any]:
    strategy6 = {
        "strategy6_version": "v3_2",
        "v2_min_direction_acceptance_score": min(long_context, short_context) - 4,
        "v2_uncertain_direction_score": 44,
        "v2_hard_deny_direction_score": 34,
        "v2_max_chase_bps": 55.0,
        "v2_adverse_1m_deny_bps": 14.0,
        "v2_reversal_1m_wait_bps": 5.0,
        "v2_distance_from_mean_max_bps": 81.0,
        "v2_high_quality_score": 74,
        "v2_medium_quality_score": 56,
        "v3_min_direction_context_score": min(short_context, long_context),
        "v3_uncertain_direction_context_score": 48,
        "v3_hard_deny_context_score": 36,
        "v3_reverse_1m_deny_bps": 8.0,
        "v3_reverse_3m_deny_bps": 24.0,
        "v3_fake_breakout_range_pos": 0.88,
        "v3_second_acceptance_min_bps": 7.0,
        "v3_max_entry_slippage_bps": 45.0,
        "v3_quality_filter_mode": "shadow",
        "v3_2_long_min_direction_context_score": long_context,
        "v3_2_short_min_direction_context_score": short_context,
        "v3_2_long_reverse_1m_deny_bps": long_reverse_1m,
        "v3_2_short_reverse_1m_deny_bps": short_reverse_1m,
        "v3_2_long_reverse_3m_deny_bps": max(20.0, long_reverse_1m * 2.5),
        "v3_2_short_reverse_3m_deny_bps": max(24.0, short_reverse_1m * 2.0),
        "v3_2_long_btc_against_action": long_btc_action,
        "v3_2_short_btc_against_action": "shadow",
        "v3_2_quality_filter_mode": quality_mode,
        "v3_2_bad_sides": bad_sides or [],
        "v3_2_bad_symbols": bad_symbols or [],
        "v3_2_bad_hours": bad_hours or [],
        "strategy6_wait_rebound_enabled": True,
        "strategy6_wait_max_minutes": 10,
        "strategy6_wait_pullback_min_bps": 6.0,
        "strategy6_wait_pullback_max_bps": 48.0,
        "strategy6_wait_confirm_bars": 2,
        "strategy6_adaptive_exit_enabled": True,
        "strategy6_backtest_max_effective_planned_rr": first_tp,
        "max_loss_R_cap": loss_cap,
        "first_tp_R": first_tp,
        "medium_quality_loss_cap_R": loss_cap,
        "medium_quality_first_tp_R": first_tp,
        "medium_quality_protect_after_mfe_R": max(0.45, first_tp - 0.10),
        "medium_quality_trail_after_mfe_R": 0.35,
        "low_quality_loss_cap_R": min(loss_cap, 0.75),
        "low_quality_first_tp_R": min(first_tp, 0.50),
        "low_quality_protect_after_mfe_R": 0.35,
        "low_quality_trail_after_mfe_R": 0.25,
        "high_quality_loss_cap_R": min(0.95, loss_cap + 0.10),
        "high_quality_first_tp_R": first_tp,
        "high_quality_protect_after_mfe_R": max(0.55, first_tp - 0.05),
        "high_quality_trail_after_mfe_R": 0.40,
    }
    return {
        "strategy_line": "strategy6",
        "min_score": 50.0,
        "target_rr": first_tp,
        "min_rr": min(0.5, first_tp),
        "min_net_rr": min(0.55, first_tp),
        "min_effective_rr": max(0.30, min(0.50, first_tp - 0.05)),
        "stop_atr_mult": 1.2,
        "max_stop_bps": 180.0,
        "min_stop_bps": 3.0,
        "min_reachable_reward_bps": 6.0,
        "tp_target_policy": {"mode": "fast_capped_rr", "target_net_rr": first_tp, "target_rr_cap": first_tp},
        "range_room": {"long_max_range_pos": 0.92, "short_min_range_pos": 0.08},
        "taker_fee_bps": 5.0,
        "slippage_bps": 2.0,
        "max_hold_minutes": 120,
        "strategy6": strategy6,
    }


def build_grid() -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for long_context in (58, 62, 66, 70):
        for short_context in (54, 58, 62):
            for long_rev in (8.0, 10.0, 12.0):
                for short_rev in (12.0, 16.0, 20.0):
                    for first_tp, loss_cap in ((0.60, 0.75), (0.75, 0.85), (0.60, 0.65)):
                        profiles.append(
                            _params(
                                long_context=long_context,
                                short_context=short_context,
                                long_reverse_1m=long_rev,
                                short_reverse_1m=short_rev,
                                first_tp=first_tp,
                                loss_cap=loss_cap,
                                long_btc_action="wait",
                            )
                        )
    # Explicit quality-filter probes from STEP22.29, kept sparse to avoid overfitting.
    profiles.append(
        _params(
            long_context=66,
            short_context=58,
            long_reverse_1m=10.0,
            short_reverse_1m=16.0,
            first_tp=0.60,
            loss_cap=0.75,
            long_btc_action="wait",
            quality_mode="block",
            bad_sides=["LONG"],
        )
    )
    profiles.append(
        _params(
            long_context=62,
            short_context=58,
            long_reverse_1m=10.0,
            short_reverse_1m=16.0,
            first_tp=0.60,
            loss_cap=0.75,
            long_btc_action="wait",
            quality_mode="block",
            bad_symbols=["ALLOUSDT", "LITUSDT", "PLAYUSDT"],
            bad_hours=[22, 19, 10],
        )
    )
    picks: list[dict[str, Any]] = []
    seen: set[str] = set()
    stride = max(1, len(profiles) // 58)
    for params in profiles[0::stride]:
        ps = _stable_id(params)
        if ps not in seen:
            picks.append({"parameter_set_id": ps, "parameters": params})
            seen.add(ps)
        if len(picks) >= 60:
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
                "schema_version": "step21.54-strategy6-v3-2-focused-grid-v1",
                "purpose": "V3 baseline plus side/BTC/light quality overlays; opt-in only",
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
    path = REPORT_DIR / f"STEP21.54_strategy6_v3_2_focused_matrix_{ts}.md"
    best = result.get("best") or {}
    metrics = best.get("metrics") or {}
    lines = [
        "# STEP21.54 Strategy6 V3.2 Focused Matrix Backtest Report",
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
        f"- avg_win_R: `{metrics.get('avg_win_R')}`",
        f"- avg_loss_R: `{metrics.get('avg_loss_R')}`",
        f"- total_R: `{metrics.get('total_R')}`",
        "",
        "## Top Leaderboard",
        "",
        "| rank | parameter_set_id | PF | expectancy_R | win_rate | trades | avg_win_R | avg_loss_R | total_R |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in (result.get("leaderboard") or [])[:10]:
        m = item.get("metrics") or {}
        lines.append(
            f"| {item.get('rank')} | `{item.get('parameter_set_id')}` | {m.get('profit_factor')} | "
            f"{m.get('expectancy_R')} | {m.get('win_rate')} | {m.get('trade_count')} | "
            f"{m.get('avg_win_R')} | {m.get('avg_loss_R')} | {m.get('total_R')} |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Only Strategy6 V3.2 offline backtest was executed.",
            "- No live config, paper ledger, Feishu, or other strategy line was changed.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
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
    max_symbols = args.max_symbols if args.max_symbols is not None else (20 if args.smoke else 100)
    max_sets = args.max_sets if args.max_sets is not None else (8 if args.smoke else 40)
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
            print(json.dumps({"done_count": done, "total_count": total, "current_parameter_set_id": payload.get("current_parameter_set_id")}, ensure_ascii=False), flush=True)

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
        "schema_version": "step21.54-strategy6-v3-2-focused-result-v1",
        "mode": "smoke" if args.smoke else "focused",
        "grid_path": str(GRID_PATH),
        "progress_path": str(PROGRESS_PATH),
        **payload,
    }
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report = write_report(result, smoke=args.smoke)
    print(
        json.dumps(
            {
                "result_path": str(RESULT_PATH),
                "report_path": str(report),
                "experiment_id": result.get("experiment_id"),
                "best_parameter_set_id": ((result.get("best") or {}).get("parameter_set_id")),
                "best_profit_factor": (((result.get("best") or {}).get("metrics") or {}).get("profit_factor")),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
