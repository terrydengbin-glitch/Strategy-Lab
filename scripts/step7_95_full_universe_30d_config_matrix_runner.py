from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.backtest.p21_v2 import (
    SCHEMA_VERSION,
    download_kline_cache_payload,
    export_config_candidate_payload,
    kline_cache_status_payload,
    leaderboard_payload,
    run_config_matrix_streaming_payload,
    universe_symbols,
)

TARGET_LINES = ("without_micro", "strategy4", "strategy5", "strategy6")
PROGRESS_PATH = PROJECT_ROOT / "DATA" / "backtest" / "step7_95_full_universe_progress.json"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_epoch(value: Any) -> float | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return None


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_progress() -> dict[str, Any]:
    if not PROGRESS_PATH.exists():
        return {}
    try:
        return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _update_progress(**updates: Any) -> dict[str, Any]:
    progress = _read_progress()
    now = _now()
    if "done_count" in updates:
        try:
            previous_done = int(progress.get("done_count") or 0)
            next_done = int(updates.get("done_count") or 0)
        except Exception:
            previous_done = 0
            next_done = 0
        if next_done > previous_done:
            last_progress_at = progress.get("last_progress_at") or progress.get("updated_at")
            last_epoch = _parse_iso_epoch(last_progress_at)
            now_epoch = _parse_iso_epoch(now)
            updates.setdefault("last_done_count", previous_done)
            updates.setdefault("last_progress_at", now)
            if last_epoch and now_epoch and now_epoch > last_epoch:
                updates.setdefault("shards_per_min", round(((next_done - previous_done) * 60.0) / (now_epoch - last_epoch), 4))
    progress.update(updates)
    progress.setdefault("progress_source", "step7_95_script_progress")
    progress.setdefault("pid", os.getpid())
    progress["updated_at"] = now
    _write_json(PROGRESS_PATH, progress)
    return progress


def _symbol_ready(symbol: str, *, days: int) -> bool:
    status = kline_cache_status_payload(PROJECT_ROOT, symbols=[symbol], days=days, max_symbols=1)
    rows = status.get("symbols") or []
    return bool(rows and rows[0].get("status") == "ready")


def _download_all_symbols(
    *,
    symbols: list[str],
    days: int,
    sleep_sec: float,
    force: bool,
    target_lines: tuple[str, ...],
) -> dict[str, Any]:
    completed: list[str] = list((_read_progress().get("download_completed_symbols") or []))
    completed_set = set(completed)
    failures: list[dict[str, Any]] = list((_read_progress().get("download_failures") or []))
    started = _now()
    for idx, symbol in enumerate(symbols, start=1):
        if not force and (symbol in completed_set or _symbol_ready(symbol, days=days)):
            if symbol not in completed_set:
                completed.append(symbol)
                completed_set.add(symbol)
            _update_progress(
                status="running",
                phase="download",
                target_strategy_lines=list(target_lines),
                download_index=idx,
                download_total=len(symbols),
                download_completed_symbols=completed,
                download_failures=failures,
                current_symbol=symbol,
            )
            continue
        try:
            payload = download_kline_cache_payload(
                PROJECT_ROOT,
                symbols=[symbol],
                days=days,
                max_symbols=1,
                dry_run=False,
                sleep_sec=sleep_sec,
            )
            ledger = payload.get("ledger") or []
            ok = bool(ledger and ledger[0].get("status") in {"ok", "dry_run"})
            if ok:
                completed.append(symbol)
                completed_set.add(symbol)
            else:
                failures.append({"symbol": symbol, "payload": ledger, "at": _now()})
        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc), "traceback": traceback.format_exc(limit=4), "at": _now()})
        _update_progress(
            status="running",
            phase="download",
            target_strategy_lines=list(target_lines),
            started_at=started,
            download_index=idx,
            download_total=len(symbols),
            download_completed_symbols=completed,
            download_failures=failures[-50:],
            current_symbol=symbol,
        )
        time.sleep(max(0.0, sleep_sec))
    return {
        "completed_count": len(completed_set),
        "failure_count": len(failures),
        "failures": failures,
    }


def _run_matrices(
    *,
    symbols: list[str],
    days: int,
    max_sets_per_line: int,
    symbol_shard_size: int,
    max_workers: int,
    scheduler_mode: str,
    resume_experiment_id: str | None,
    target_lines: tuple[str, ...],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    execution_mode = "sharded_global_queue" if scheduler_mode == "global_queue" else "sharded_streaming"
    memory_guard_status = "global_queue_single_writer" if scheduler_mode == "global_queue" else ("streaming_parallel" if max_workers > 1 else "streaming")
    for idx, line in enumerate(target_lines, start=1):
        _update_progress(
            phase="matrix",
            matrix_index=idx,
            matrix_total=len(target_lines),
            current_strategy_line=line,
            execution_mode=execution_mode,
            memory_guard_status=memory_guard_status,
            max_workers=max_workers,
            scheduler_mode=scheduler_mode,
            resume_experiment_id=resume_experiment_id,
        )
        def _progress_callback(update: dict[str, Any]) -> None:
            clean_update = dict(update)
            clean_update.pop("phase", None)
            clean_update.pop("current_strategy_line", None)
            clean_update.pop("memory_guard_status", None)
            clean_update.pop("max_workers", None)
            clean_update.pop("execution_mode", None)
            _update_progress(
                **clean_update,
                phase="matrix",
                matrix_index=idx,
                matrix_total=len(target_lines),
                strategy_line_index=idx,
                strategy_line_total=len(target_lines),
                current_strategy_line=line,
                execution_mode=execution_mode,
                memory_guard_status=memory_guard_status,
                max_workers=max_workers,
                scheduler_mode=scheduler_mode,
            )

        payload = run_config_matrix_streaming_payload(
            PROJECT_ROOT,
            symbols=symbols,
            strategy_line=line,
            days=days,
            max_symbols=len(symbols),
            max_sets=max_sets_per_line,
            write=True,
            symbol_shard_size=symbol_shard_size,
            max_workers=max_workers,
            scheduler_mode=scheduler_mode,
            resume_experiment_id=resume_experiment_id if idx == 1 else None,
            job_id=str(_read_progress().get("job_id") or ""),
            progress_callback=_progress_callback,
        )
        best = payload.get("best") or {}
        if best.get("parameter_set_id"):
            export_config_candidate_payload(
                PROJECT_ROOT,
                experiment_id=payload["experiment_id"],
                parameter_set_id=best["parameter_set_id"],
            )
        results.append(
            {
                "strategy_line": line,
                "experiment_id": payload.get("experiment_id"),
                "best": best,
                "parameter_set_count": payload.get("parameter_set_count"),
                "trade_count": payload.get("trade_count"),
                "execution_mode": payload.get("execution_mode"),
                "memory_guard_status": payload.get("memory_guard_status"),
                "shard_count": payload.get("shard_count"),
                "max_workers": payload.get("max_workers"),
                "scheduler_mode": scheduler_mode,
                "resume_experiment_id": payload.get("resume_experiment_id"),
            }
        )
        _update_progress(phase="matrix", matrix_results=results)
    return results


def _write_report(payload: dict[str, Any]) -> tuple[Path, Path]:
    stamp = _stamp()
    report_dir = PROJECT_ROOT / "docs" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / f"STEP7.95_full_universe_30d_config_matrix_optimization_{stamp}.json"
    md_path = report_dir / f"STEP7.95_full_universe_30d_config_matrix_optimization_{stamp}.md"
    _write_json(json_path, payload)
    lines = [
        "# STEP7.95 Full-Universe 30d Config Matrix Optimization Audit",
        "",
        f"- Status: {payload.get('status')}",
        f"- Schema: {SCHEMA_VERSION}",
        f"- Symbols: {payload.get('symbol_count')}",
        f"- Days: {payload.get('days')}",
        f"- Kline ready: {payload.get('kline_ready_count')}/{payload.get('kline_status_count')}",
        "",
        "## Best By Strategy",
        "",
    ]
    for item in payload.get("matrix_results") or []:
        best = item.get("best") or {}
        metrics = best.get("metrics") or {}
        lines.append(
            f"- {item.get('strategy_line')}: PF={metrics.get('profit_factor')} expectancy={metrics.get('expectancy_R')} "
            f"trades={metrics.get('trade_count')} parameter={best.get('parameter_set_id')}"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- No runtime config was changed.",
            "- No paper / Feishu / runtime current trade plan was written.",
            "- Results are shadow backtest candidates only.",
            "",
            f"JSON: `{json_path.as_posix()}`",
        ]
    )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--max-symbols", type=int, default=0, help="0 means full universe")
    parser.add_argument("--max-sets-per-line", type=int, default=240)
    parser.add_argument("--symbol-shard-size", type=int, default=25)
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--scheduler-mode", choices=["parameter_batch", "global_queue"], default="parameter_batch")
    parser.add_argument("--resume-experiment-id", default="")
    parser.add_argument("--sleep-sec", type=float, default=0.08)
    parser.add_argument("--download-only", action="store_true")
    parser.add_argument("--matrix-only", action="store_true")
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument(
        "--strategy-lines",
        default=",".join(TARGET_LINES),
        help="Comma-separated strategy lines to run. Valid: without_micro,strategy4,strategy5,strategy6",
    )
    args = parser.parse_args()

    requested_lines = tuple(line.strip() for line in str(args.strategy_lines or "").split(",") if line.strip())
    invalid_lines = [line for line in requested_lines if line not in TARGET_LINES]
    if not requested_lines or invalid_lines:
        print(f"invalid --strategy-lines: {args.strategy_lines}; valid={','.join(TARGET_LINES)}", file=sys.stderr)
        return 2

    all_symbols = universe_symbols(PROJECT_ROOT, limit=None)
    symbols = all_symbols[: args.max_symbols] if args.max_symbols and args.max_symbols > 0 else all_symbols
    previous_progress = _read_progress()
    started_at = previous_progress.get("started_at") or _now()
    job_id = previous_progress.get("job_id") or f"step7_95_{_stamp()}_{os.getpid()}"
    _update_progress(
        status="running",
        reason=None,
        last_error=None,
        schema_version=SCHEMA_VERSION,
        job_id=job_id,
        pid=os.getpid(),
        progress_source="step7_95_script_progress",
        started_at=started_at,
        days=args.days,
        symbol_count=len(symbols),
        max_sets_per_line=args.max_sets_per_line,
        scheduler_mode=args.scheduler_mode,
        target_strategy_lines=list(requested_lines),
    )
    if not args.matrix_only:
        _download_all_symbols(
            symbols=symbols,
            days=args.days,
            sleep_sec=args.sleep_sec,
            force=args.force_download,
            target_lines=requested_lines,
        )
    if args.download_only:
        _update_progress(status="download_complete", phase="download")
        return 0

    status = kline_cache_status_payload(PROJECT_ROOT, symbols=symbols, days=args.days, max_symbols=len(symbols))
    ready_symbols = [row["symbol"] for row in status.get("symbols") or [] if row.get("status") == "ready"]
    _update_progress(phase="pre_matrix_status", kline_ready_count=len(ready_symbols), kline_status_count=status.get("count"))
    if not ready_symbols:
        _update_progress(status="blocked", reason="no_ready_kline_symbols")
        return 2

    matrix_results = _run_matrices(
        symbols=ready_symbols,
        days=args.days,
        max_sets_per_line=args.max_sets_per_line,
        symbol_shard_size=args.symbol_shard_size,
        max_workers=max(1, int(args.max_workers or 1)),
        scheduler_mode=args.scheduler_mode,
        resume_experiment_id=args.resume_experiment_id or None,
        target_lines=requested_lines,
    )
    global_leaderboard = leaderboard_payload(PROJECT_ROOT, limit=50)
    final_payload = {
        "schema_version": SCHEMA_VERSION,
        "step": "STEP7.95",
        "status": "PASS",
        "generated_at": _now(),
        "days": args.days,
        "symbol_count": len(symbols),
        "kline_ready_count": len(ready_symbols),
        "kline_status_count": status.get("count"),
        "matrix_results": matrix_results,
        "execution_mode": "sharded_global_queue" if args.scheduler_mode == "global_queue" else "sharded_streaming",
        "scheduler_mode": args.scheduler_mode,
        "symbol_shard_size": args.symbol_shard_size,
        "max_workers": max(1, int(args.max_workers or 1)),
        "resume_experiment_id": args.resume_experiment_id or None,
        "target_strategy_lines": list(requested_lines),
        "global_leaderboard": global_leaderboard,
        "progress_path": str(PROGRESS_PATH),
    }
    json_path, md_path = _write_report(final_payload)
    _update_progress(status="complete", phase="done", report_json=str(json_path), report_md=str(md_path))
    print(md_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
