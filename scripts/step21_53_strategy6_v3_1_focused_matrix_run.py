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


GRID_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_focused_80params_STEP21_53.json"
PROGRESS_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_focused_STEP21_53_progress.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_focused_STEP21_53_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stable_id(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return "s6v31_" + hashlib.sha1(raw).hexdigest()[:16]


def _params(
    *,
    min_context: int,
    reverse_1m: float,
    reverse_3m: float,
    low_volume: float,
    low_5m: float,
    range_extreme: float,
    loss_cap: float,
    first_tp: float,
    wait_minutes: int,
) -> dict[str, Any]:
    strategy6 = {
        "strategy6_version": "v3_1",
        "v2_min_direction_acceptance_score": max(50, min_context - 4),
        "v2_uncertain_direction_score": max(42, min_context - 14),
        "v2_hard_deny_direction_score": max(30, min_context - 24),
        "v2_max_chase_bps": 55.0,
        "v2_adverse_1m_deny_bps": max(14.0, reverse_1m + 4.0),
        "v2_reversal_1m_wait_bps": max(5.0, reverse_1m - 4.0),
        "v2_distance_from_mean_max_bps": 85.0,
        "v2_high_quality_score": 74,
        "v2_medium_quality_score": max(54, min_context - 2),
        "v3_min_direction_context_score": min_context,
        "v3_uncertain_direction_context_score": max(42, min_context - 10),
        "v3_hard_deny_context_score": max(30, min_context - 22),
        "v3_reverse_1m_deny_bps": reverse_1m,
        "v3_reverse_3m_deny_bps": reverse_3m,
        "v3_fake_breakout_range_pos": 0.88,
        "v3_second_acceptance_min_bps": 4.0,
        "v3_max_entry_slippage_bps": 45.0,
        "v3_quality_filter_mode": "shadow",
        "v3_1_min_direction_context_score": min_context,
        "v3_1_uncertain_direction_context_score": max(42, min_context - 10),
        "v3_1_hard_deny_context_score": max(30, min_context - 22),
        "v3_1_reverse_1m_deny_bps": reverse_1m,
        "v3_1_reverse_3m_deny_bps": reverse_3m,
        "v3_1_low_followthrough_min_volume_z": low_volume,
        "v3_1_low_followthrough_min_5m_bps": low_5m,
        "v3_1_range_extreme_pos": range_extreme,
        "v3_1_btc_against_action": "wait",
        "strategy6_wait_rebound_enabled": True,
        "strategy6_wait_max_minutes": wait_minutes,
        "strategy6_wait_pullback_min_bps": 6.0,
        "strategy6_wait_pullback_max_bps": 48.0,
        "strategy6_wait_confirm_bars": 2,
        "strategy6_backtest_max_effective_planned_rr": first_tp,
        "strategy6_adaptive_exit_enabled": True,
        "max_loss_R_cap": loss_cap,
        "first_tp_R": first_tp,
        "medium_quality_loss_cap_R": loss_cap,
        "medium_quality_first_tp_R": first_tp,
        "medium_quality_protect_after_mfe_R": max(0.35, first_tp - 0.10),
        "medium_quality_trail_after_mfe_R": 0.25,
        "low_quality_loss_cap_R": min(loss_cap, 0.65),
        "low_quality_first_tp_R": min(first_tp, 0.45),
        "low_quality_protect_after_mfe_R": 0.30,
        "low_quality_trail_after_mfe_R": 0.20,
        "high_quality_loss_cap_R": min(0.75, loss_cap + 0.10),
        "high_quality_first_tp_R": first_tp,
        "high_quality_protect_after_mfe_R": max(0.45, first_tp - 0.05),
        "high_quality_trail_after_mfe_R": 0.30,
    }
    return {
        "strategy_line": "strategy6",
        "min_score": max(48.0, float(min_context) - 8.0),
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
    candidates: list[dict[str, Any]] = []
    def add_profile(
        *,
        min_context: int,
        reverse_1m: float,
        reverse_3m: float,
        low_volume: float,
        low_5m: float,
        range_extreme: float,
        loss_cap: float,
        first_tp: float,
        max_slippage: float,
        second_acceptance: float,
        wait_minutes: int,
    ) -> None:
        params = _params(
            min_context=min_context,
            reverse_1m=reverse_1m,
            reverse_3m=reverse_3m,
            low_volume=low_volume,
            low_5m=low_5m,
            range_extreme=range_extreme,
            loss_cap=loss_cap,
            first_tp=first_tp,
            wait_minutes=wait_minutes,
        )
        strategy6 = params["strategy6"]
        strategy6["v2_max_chase_bps"] = max_slippage + 10.0
        strategy6["v2_distance_from_mean_max_bps"] = max(85.0, max_slippage * 1.8)
        strategy6["v3_max_entry_slippage_bps"] = max_slippage
        strategy6["v3_second_acceptance_min_bps"] = second_acceptance
        params["range_room"] = {"long_max_range_pos": 0.96, "short_min_range_pos": 0.04}
        candidates.append({"parameter_set_id": _stable_id(params), "parameters": params})

    # Keep V3-compatible wide-entry baselines first.  They make V3.1 comparable
    # with STEP21.52 instead of measuring an accidental all-WAIT grid.
    add_profile(
        min_context=42,
        reverse_1m=48.0,
        reverse_3m=80.0,
        low_volume=0.0,
        low_5m=-20.0,
        range_extreme=0.95,
        loss_cap=0.75,
        first_tp=0.60,
        max_slippage=140.0,
        second_acceptance=-20.0,
        wait_minutes=10,
    )
    add_profile(
        min_context=48,
        reverse_1m=32.0,
        reverse_3m=55.0,
        low_volume=0.25,
        low_5m=-10.0,
        range_extreme=0.92,
        loss_cap=0.75,
        first_tp=0.60,
        max_slippage=100.0,
        second_acceptance=0.0,
        wait_minutes=10,
    )
    add_profile(
        min_context=52,
        reverse_1m=24.0,
        reverse_3m=42.0,
        low_volume=0.50,
        low_5m=0.0,
        range_extreme=0.88,
        loss_cap=0.85,
        first_tp=0.75,
        max_slippage=80.0,
        second_acceptance=2.0,
        wait_minutes=14,
    )
    for min_context in (58, 62, 66):
        for reverse_1m in (8.0, 12.0, 16.0):
            for reverse_3m in (18.0, 24.0, 30.0):
                for low_volume in (0.75, 0.85, 1.00):
                    for low_5m in (4.0, 8.0, 12.0):
                        for range_extreme in (0.68, 0.72, 0.78):
                            for loss_cap, first_tp in ((0.55, 0.65), (0.55, 0.45), (0.65, 0.65), (0.75, 0.55)):
                                params = _params(
                                    min_context=min_context,
                                    reverse_1m=reverse_1m,
                                    reverse_3m=reverse_3m,
                                    low_volume=low_volume,
                                    low_5m=low_5m,
                                    range_extreme=range_extreme,
                                    loss_cap=loss_cap,
                                    first_tp=first_tp,
                                    wait_minutes=10,
                                )
                                candidates.append({"parameter_set_id": _stable_id(params), "parameters": params})
    picks: list[dict[str, Any]] = []
    picks.extend(candidates[:3])
    # Cover the strongest STEP22.27 R-parity profiles after the baseline profiles.
    seeds = [
        (58, 8.0, 18.0, 0.75, 4.0, 0.72, 0.55, 0.65),
        (62, 8.0, 24.0, 0.85, 8.0, 0.72, 0.55, 0.65),
        (62, 12.0, 24.0, 1.00, 8.0, 0.68, 0.55, 0.45),
        (66, 12.0, 30.0, 0.85, 12.0, 0.72, 0.65, 0.65),
        (58, 16.0, 30.0, 0.75, 4.0, 0.78, 0.75, 0.55),
    ]
    for seed in seeds:
        params = _params(
            min_context=seed[0],
            reverse_1m=seed[1],
            reverse_3m=seed[2],
            low_volume=seed[3],
            low_5m=seed[4],
            range_extreme=seed[5],
            loss_cap=seed[6],
            first_tp=seed[7],
            wait_minutes=10,
        )
        item = {"parameter_set_id": _stable_id(params), "parameters": params}
        if item["parameter_set_id"] not in {p["parameter_set_id"] for p in picks}:
            picks.append(item)
    stride = max(1, len(candidates) // 100)
    seen = {item["parameter_set_id"] for item in picks}
    for item in candidates[0::stride]:
        if item["parameter_set_id"] not in seen:
            picks.append(item)
            seen.add(item["parameter_set_id"])
        if len(picks) >= 80:
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
                "schema_version": "step21.53-strategy6-v3-1-focused-grid-v1",
                "purpose": "focused Strategy6 V3.1 matrix; V1/V2/V3/default strategy behavior remains unchanged",
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
    path = REPORT_DIR / f"STEP21.53_strategy6_v3_1_focused_matrix_{ts}.md"
    best = result.get("best") or {}
    metrics = best.get("metrics") or {}
    lines = [
        "# STEP21.53 Strategy6 V3.1 Focused Matrix Backtest Report",
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
            "- Only Strategy6 V3.1 offline backtest was executed.",
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
    max_symbols = args.max_symbols if args.max_symbols is not None else (10 if args.smoke else 100)
    max_sets = args.max_sets if args.max_sets is not None else (5 if args.smoke else 40)
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
        "schema_version": "step21.53-strategy6-v3-1-focused-result-v1",
        "mode": "smoke" if args.smoke else "focused",
        "grid_path": str(GRID_PATH),
        "progress_path": str(PROGRESS_PATH),
        **payload,
    }
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report_path = write_report(result, smoke=args.smoke)
    print(
        json.dumps(
            {
                "result_path": str(RESULT_PATH),
                "report_path": str(report_path),
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
