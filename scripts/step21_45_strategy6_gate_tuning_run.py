from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21_v2 import run_config_matrix_streaming_payload


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    root = ROOT
    grid_path = root / "DATA" / "backtest" / "strategy6_gate_tuning_30params_STEP21_45.json"
    grid = json.loads(grid_path.read_text(encoding="utf-8"))["parameter_sets"]
    progress_path = root / "DATA" / "backtest" / "strategy6_gate_tuning_STEP21_45_progress.json"
    result_path = root / "DATA" / "backtest" / "strategy6_gate_tuning_STEP21_45_result.json"
    last_print = {"n": 0, "t": 0.0}

    def cb(progress: dict) -> None:
        progress = dict(progress)
        progress["updated_at"] = _now()
        progress_path.write_text(json.dumps(progress, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        done = int(progress.get("done_count") or 0)
        total = int(progress.get("total_count") or 0)
        now = time.time()
        if done == 1 or done == total or done - int(last_print["n"]) >= 20 or now - float(last_print["t"]) >= 60:
            last_print["n"] = done
            last_print["t"] = now
            print(
                json.dumps(
                    {
                        key: progress.get(key)
                        for key in [
                            "done_count",
                            "total_count",
                            "current_parameter_set_id",
                            "current_symbol_shard",
                            "persisted_order_count",
                            "avg_shard_sec",
                            "p95_shard_sec",
                            "sqlite_write_sec",
                            "max_workers",
                            "active_workers",
                            "updated_at",
                        ]
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    payload = run_config_matrix_streaming_payload(
        root,
        strategy_line="strategy6",
        days=30,
        max_symbols=100,
        max_sets=30,
        parameter_grid=grid,
        write=True,
        symbol_shard_size=10,
        max_workers=6,
        scheduler_mode="global_queue",
        progress_callback=cb,
    )
    out = {
        "experiment_id": payload.get("experiment_id"),
        "strategy_line": payload.get("strategy_line"),
        "parameter_set_count": payload.get("parameter_set_count"),
        "trade_count": payload.get("trade_count"),
        "best": payload.get("best"),
        "leaderboard": payload.get("leaderboard", [])[:10],
        "generated_at": payload.get("generated_at"),
    }
    result_path.write_text(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(
        "RESULT "
        + json.dumps(
            {
                "result_path": str(result_path),
                "experiment_id": out["experiment_id"],
                "best_parameter_set_id": (out.get("best") or {}).get("parameter_set_id"),
                "best_metrics": (out.get("best") or {}).get("metrics"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
