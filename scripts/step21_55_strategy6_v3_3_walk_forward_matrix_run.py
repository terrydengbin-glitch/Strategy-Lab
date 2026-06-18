from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_v2 import (
    download_kline_cache_payload,
    kline_cache_status_payload,
    run_config_matrix_streaming_payload,
    universe_symbols,
)


GRID_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_3_walk_forward_STEP21_55_grid.json"
PROGRESS_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_3_walk_forward_STEP21_55_progress.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_3_walk_forward_STEP21_55_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stable_id(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return "s6v33_" + hashlib.sha1(raw).hexdigest()[:16]


def _params(*, long_context: int, short_context: int, adverse_1m: float, adverse_3m: float, first_tp: float, loss_cap: float, abort_mfe: float, abort_mae: float) -> dict[str, Any]:
    strategy6 = {
        "strategy6_version": "v3_3",
        "v2_uncertain_direction_score": 44,
        "v2_hard_deny_direction_score": 34,
        "v2_max_chase_bps": 55.0,
        "v2_adverse_1m_deny_bps": 14.0,
        "v2_reversal_1m_wait_bps": 5.0,
        "v2_distance_from_mean_max_bps": 81.0,
        "v2_high_quality_score": 74,
        "v2_medium_quality_score": 56,
        "v3_min_direction_context_score": min(long_context, short_context),
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
        "v3_2_long_reverse_1m_deny_bps": 10.0,
        "v3_2_short_reverse_1m_deny_bps": 16.0,
        "v3_2_long_reverse_3m_deny_bps": 24.0,
        "v3_2_short_reverse_3m_deny_bps": 30.0,
        "v3_2_long_btc_against_action": "wait",
        "v3_2_short_btc_against_action": "shadow",
        "v3_2_quality_filter_mode": "shadow",
        "v3_3_long_min_direction_context_score": long_context,
        "v3_3_short_min_direction_context_score": short_context,
        "v3_3_adverse_1m_wait_bps": adverse_1m,
        "v3_3_adverse_3m_deny_bps": adverse_3m,
        "v3_3_weak_followthrough_wait_bps": 4.0,
        "v3_3_min_volume_z": 0.6,
        "v3_3_early_abort_enabled": True,
        "v3_3_abort_if_mfe_lt_R": abort_mfe,
        "v3_3_abort_if_mae_gt_R": abort_mae,
        "v3_3_abort_window_min": 3,
        "v3_3_max_initial_adverse_R": loss_cap,
        "strategy6_wait_rebound_enabled": True,
        "strategy6_wait_max_minutes": 10,
        "strategy6_wait_pullback_min_bps": 6.0,
        "strategy6_wait_pullback_max_bps": 48.0,
        "strategy6_wait_confirm_bars": 2,
        "strategy6_adaptive_exit_enabled": True,
        "strategy6_backtest_max_effective_planned_rr": first_tp,
        "max_loss_R_cap": loss_cap,
        "first_tp_R": first_tp,
        "abort_if_mfe_lt_R": abort_mfe,
        "abort_if_mae_gt_R": abort_mae,
        "abort_window_min": 3,
        "max_initial_adverse_R": loss_cap,
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
        "strategy6_exit_protection_enabled": True,
        "strategy6_adaptive_exit_enabled": True,
        "strategy6": strategy6,
    }


def build_grid() -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for long_context in (62, 66, 70):
        for short_context in (54, 58, 62):
            for adverse_1m in (4.0, 6.0, 8.0):
                for first_tp, loss_cap in ((0.60, 0.75), (0.65, 0.75), (0.70, 0.85)):
                    profiles.append(
                        _params(
                            long_context=long_context,
                            short_context=short_context,
                            adverse_1m=adverse_1m,
                            adverse_3m=18.0,
                            first_tp=first_tp,
                            loss_cap=loss_cap,
                            abort_mfe=0.10,
                            abort_mae=0.45,
                        )
                    )
    picks: list[dict[str, Any]] = []
    seen: set[str] = set()
    stride = max(1, len(profiles) // 40)
    for params in profiles[0::stride]:
        ps = _stable_id(params)
        if ps not in seen:
            picks.append({"parameter_set_id": ps, "parameters": params})
            seen.add(ps)
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
                "schema_version": "step21.55-strategy6-v3-3-walk-forward-grid-v1",
                "boundary": "train days 1-18, validation days 19-24, test days 25-30; test never selects params",
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


def _avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 8) if values else 0.0


def _median(values: list[float]) -> float:
    return round(float(median(values)), 8) if values else 0.0


def _metrics(values: list[float]) -> dict[str, Any]:
    wins = [v for v in values if v > 0]
    losses = [v for v in values if v < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    pf = round(gross_win / gross_loss, 8) if gross_loss else (999.0 if gross_win else 0.0)
    return {
        "trade_count": len(values),
        "profit_factor": pf,
        "expectancy_R": _avg(values),
        "win_rate": round(len(wins) / len(values), 8) if values else 0.0,
        "avg_win_R": _avg(wins),
        "avg_loss_R": round(abs(_avg(losses)), 8),
        "median_R": _median(values),
        "total_R": round(sum(values), 8),
    }


def _split_name(day_index: int) -> str:
    if day_index <= 18:
        return "train"
    if day_index <= 24:
        return "validation"
    return "test"


def split_metrics(experiment_id: str) -> dict[str, Any]:
    db = p21_db_path(ROOT)
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            """
            SELECT parameter_set_id, entry_time_ms, net_R
            FROM p21_v2_shadow_orders
            WHERE experiment_id = ? AND strategy_line = 'strategy6' AND net_R IS NOT NULL
            ORDER BY entry_time_ms ASC
            """,
            (experiment_id,),
        ).fetchall()
    if not rows:
        return {"leaderboard": [], "by_parameter_set": {}}
    min_ts = min(int(r[1]) for r in rows)
    by_param: dict[str, dict[str, list[float]]] = {}
    for param, entry_ms, net_r in rows:
        day_index = int((int(entry_ms) - min_ts) // 86_400_000) + 1
        split = _split_name(day_index)
        by_param.setdefault(str(param), {"train": [], "validation": [], "test": [], "all": []})
        by_param[str(param)][split].append(float(net_r))
        by_param[str(param)]["all"].append(float(net_r))
    metrics_by_param = {
        param: {split: _metrics(values) for split, values in splits.items()}
        for param, splits in by_param.items()
    }
    leaderboard = sorted(
        (
            {
                "parameter_set_id": param,
                "selection_metric": "validation_profit_factor",
                "train": metrics["train"],
                "validation": metrics["validation"],
                "test": metrics["test"],
                "all": metrics["all"],
            }
            for param, metrics in metrics_by_param.items()
        ),
        key=lambda x: (
            float((x["validation"] or {}).get("profit_factor") or 0),
            float((x["validation"] or {}).get("expectancy_R") or -999),
            int((x["validation"] or {}).get("trade_count") or 0),
        ),
        reverse=True,
    )
    for idx, item in enumerate(leaderboard, start=1):
        item["rank"] = idx
    return {"leaderboard": leaderboard, "by_parameter_set": metrics_by_param}


def write_report(result: dict[str, Any], *, smoke: bool) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP21.55_strategy6_v3_3_walk_forward_matrix_{ts}.md"
    selected = (result.get("walk_forward") or {}).get("selected") or {}
    lines = [
        "# STEP21.55 Strategy6 V3.3 Walk-Forward Matrix Report",
        "",
        f"- generated_at: `{_now()}`",
        f"- mode: `{'smoke' if smoke else 'focused'}`",
        f"- experiment_id: `{result.get('experiment_id')}`",
        f"- parameter_set_count: `{result.get('parameter_set_count')}`",
        f"- trade_count: `{result.get('trade_count')}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        f"- progress_json: `{PROGRESS_PATH.relative_to(ROOT)}`",
        "",
        "## Selected By Validation Only",
        "",
        f"- parameter_set_id: `{selected.get('parameter_set_id')}`",
        f"- validation_pf: `{((selected.get('validation') or {}).get('profit_factor'))}`",
        f"- test_pf: `{((selected.get('test') or {}).get('profit_factor'))}`",
        f"- test_expectancy_R: `{((selected.get('test') or {}).get('expectancy_R'))}`",
        "",
        "## Walk-Forward Leaderboard",
        "",
        "| rank | parameter_set_id | train PF | val PF | test PF | val trades | test trades | test expectancy_R |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ((result.get("walk_forward") or {}).get("leaderboard") or [])[:10]:
        lines.append(
            f"| {item.get('rank')} | `{item.get('parameter_set_id')}` | "
            f"{(item.get('train') or {}).get('profit_factor')} | {(item.get('validation') or {}).get('profit_factor')} | "
            f"{(item.get('test') or {}).get('profit_factor')} | {(item.get('validation') or {}).get('trade_count')} | "
            f"{(item.get('test') or {}).get('trade_count')} | {(item.get('test') or {}).get('expectancy_R')} |"
        )
    lines.extend(
        [
            "",
            "## No-Lookahead / Anti-Overfit Boundary",
            "",
            "- Entry selection only uses signal/entry-time features declared by `strategy6_v3_3_known_at_contract`.",
            "- Train/validation can rank candidates; test split is holdout evidence only.",
            "- This report does not change runtime config, paper, Feishu, or live strategy behavior.",
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
    max_sets = args.max_sets if args.max_sets is not None else (8 if args.smoke else 30)
    selected_symbols = [s.upper() for s in universe_symbols(ROOT, limit=max_symbols)[:max_symbols]]
    if not args.skip_download:
        status = kline_cache_status_payload(ROOT, symbols=selected_symbols, days=30, max_symbols=len(selected_symbols))
        missing = [row["symbol"] for row in status.get("symbols", []) if row.get("status") != "ready"]
        if missing:
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
    wf = split_metrics(str(payload.get("experiment_id") or ""))
    selected = (wf.get("leaderboard") or [{}])[0]
    wf["selected"] = selected
    result = {
        "schema_version": "step21.55-strategy6-v3-3-walk-forward-result-v1",
        "mode": "smoke" if args.smoke else "focused",
        "no_lookahead_contract": {
            "entry_feature_contract_field": "strategy6_v3_3_known_at_contract",
            "test_split_used_for_selection": False,
            "train_days": "1-18",
            "validation_days": "19-24",
            "test_days": "25-30",
        },
        "grid_path": str(GRID_PATH),
        "progress_path": str(PROGRESS_PATH),
        "walk_forward": wf,
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
                "selected_parameter_set_id": selected.get("parameter_set_id"),
                "validation_pf": ((selected.get("validation") or {}).get("profit_factor")),
                "test_pf": ((selected.get("test") or {}).get("profit_factor")),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
