"""Poll latest_micro_features.json while daemon runs; write run report."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS, EXIT_WAIT_UNTIL_READY_TIMEOUT
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.micro.wait_until_ready.config import WaitUntilReadyConfig, recommended_target_stale_sec
from laoma_signal_engine.micro.wait_until_ready.evaluate import (
    micro_current_run_skip_reason,
    micro_satisfies_current_run_wait,
    normalize_symbol,
    scope_micro_to_expected_symbols,
)
from laoma_signal_engine.micro.wait_until_ready.summary import (
    build_not_ready_summary,
    ready_symbol_lists,
)

_ReadJsonFn = Callable[[Path], tuple[dict[str, Any] | None, str | None]]


def default_read_micro_json(latest_path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not latest_path.is_file():
        return None, None
    try:
        text = latest_path.read_text(encoding="utf-8")
        doc = json.loads(text)
        if not isinstance(doc, dict):
            return None, "json root is not an object"
        return doc, None
    except OSError as exc:
        return None, str(exc)[:500]
    except json.JSONDecodeError as exc:
        return None, f"json decode: {exc}"[:500]


def _report_stamp_utc(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def _empty_frame() -> dict[str, Any]:
    return {
        "target_generated_at": "",
        "target_age_sec": -1,
        "final_micro_generated_at": "",
        "final_status": "",
        "final_target_status": "",
        "ws_status": "",
        "last_ws_message_age_sec": None,
        "ready_count": 0,
        "fast_ready_count": 0,
        "full_ready_count": 0,
        "symbol_count": 0,
        "ready_scope": "",
        "target_set_id": "",
        "global_symbol_count": 0,
        "global_ready_count": 0,
        "global_fast_ready_count": 0,
        "global_full_ready_count": 0,
    }


def _frame_from_micro(m: dict[str, Any] | None) -> dict[str, Any]:
    if not m:
        return _empty_frame()
    items = m.get("items")
    sc = len(items) if isinstance(items, list) else 0
    rc = m.get("ready_count")
    rc_i = rc if isinstance(rc, int) else 0
    ta = m.get("target_age_sec")
    ta_i = int(ta) if isinstance(ta, int) else -1
    age_sec = m.get("last_ws_message_age_sec")
    return {
        "target_generated_at": str(m.get("target_generated_at") or ""),
        "target_age_sec": ta_i,
        "final_micro_generated_at": str(m.get("generated_at") or ""),
        "final_status": str(m.get("status") or ""),
        "final_target_status": str(m.get("target_status") or ""),
        "ws_status": str(m.get("ws_status") or ""),
        "last_ws_message_age_sec": age_sec,
        "ready_count": rc_i,
        "fast_ready_count": int(m.get("fast_ready_count") or 0),
        "full_ready_count": int(m.get("full_ready_count") or 0),
        "symbol_count": sc,
        "ready_scope": str(m.get("ready_scope") or m.get("scope") or ""),
        "target_set_id": str(m.get("target_set_id") or ""),
        "global_symbol_count": int(m.get("global_symbol_count") or 0),
        "global_ready_count": int(m.get("global_ready_count") or 0),
        "global_fast_ready_count": int(m.get("global_fast_ready_count") or 0),
        "global_full_ready_count": int(m.get("global_full_ready_count") or 0),
    }


def _ready_symbols_for(items: list[dict[str, Any]], quality_key: str) -> list[str]:
    out: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        sym = normalize_symbol(str(item.get("symbol") or ""))
        quality = item.get(quality_key)
        if sym and isinstance(quality, dict) and quality.get("ready") is True:
            out.append(sym)
    return sorted(out)


def _signal_symbol_lists(items: list[dict[str, Any]], strategy_line: str) -> dict[str, list[str]]:
    quality_key = "micro_fast_quality" if strategy_line == "micro_fast" else "micro_full_quality"
    signal_key = "micro_fast_signal" if strategy_line == "micro_fast" else "micro_full_signal"
    quality_ready: list[str] = []
    confirmed: list[str] = []
    consumable: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        sym = normalize_symbol(str(item.get("symbol") or ""))
        if not sym:
            continue
        quality = item.get(quality_key)
        signal = item.get(signal_key)
        if isinstance(quality, dict) and quality.get("ready") is True:
            quality_ready.append(sym)
        if isinstance(signal, dict) and signal.get("micro_direction_confirmed") is True:
            confirmed.append(sym)
        if isinstance(signal, dict) and signal.get("micro_exec_allowed") is True and _aligned_frame_ok_for_wait(item, strategy_line):
            consumable.append(sym)
    return {
        "quality_ready_symbols": sorted(quality_ready),
        "confirmed_symbols": sorted(confirmed),
        "consumable_symbols": sorted(consumable),
    }


def _aligned_frame_ok_for_wait(item: dict[str, Any], strategy_line: str) -> bool:
    if strategy_line != "micro_fast":
        return True
    quality = item.get("micro_fast_quality") if isinstance(item, dict) else None
    if not isinstance(quality, dict):
        return True
    reasons = {str(x) for x in quality.get("reason_codes") or []}
    if reasons.intersection({"cvd_stale", "ofi_stale", "ofi_cvd_lag_high", "cvd_never_updated", "ofi_never_updated"}):
        return False
    has_frame_evidence = any(
        key in quality
        for key in (
            "last_cvd_update_bucket_ts_sec",
            "last_ofi_update_bucket_ts_sec",
            "last_processed_bucket_ts_sec",
            "ofi_cvd_lag_bucket_sec",
        )
    )
    if not has_frame_evidence:
        return True
    cvd_ts = quality.get("last_cvd_update_bucket_ts_sec")
    ofi_ts = quality.get("last_ofi_update_bucket_ts_sec")
    if cvd_ts is None or ofi_ts is None:
        return False
    try:
        lag = quality.get("ofi_cvd_lag_bucket_sec")
        lag_num = abs(float(lag if lag is not None else float(cvd_ts) - float(ofi_ts)))
    except (TypeError, ValueError):
        return False
    return lag_num <= 30


def _canonical_micro_feature_doc(micro: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "schema_version",
        "generated_at",
        "source",
        "status",
        "target_generated_at",
        "target_age_sec",
        "target_status",
        "symbol_count",
        "ready_count",
        "not_ready_count",
        "fast_ready_count",
        "full_ready_count",
        "ws_status",
        "last_ws_message_age_sec",
        "dropped_events",
        "reason_codes",
        "items",
    }
    doc = {k: v for k, v in micro.items() if k in allowed}
    items = [x for x in doc.get("items", []) if isinstance(x, dict)] if isinstance(doc.get("items"), list) else []
    doc["items"] = items
    doc["symbol_count"] = len(items)
    doc["ready_count"] = sum(
        1 for item in items if isinstance(item.get("micro_quality"), dict) and item["micro_quality"].get("ready") is True
    )
    doc["not_ready_count"] = max(0, len(items) - int(doc["ready_count"]))
    doc["fast_ready_count"] = sum(
        1
        for item in items
        if isinstance(item.get("micro_fast_quality"), dict) and item["micro_fast_quality"].get("ready") is True
    )
    doc["full_ready_count"] = sum(
        1
        for item in items
        if isinstance(item.get("micro_full_quality"), dict) and item["micro_full_quality"].get("ready") is True
    )
    return doc


def _canonical_micro_state_doc(state: dict[str, Any], expected_symbols: set[str]) -> dict[str, Any]:
    allowed = {
        "schema_version",
        "generated_at",
        "source",
        "daemon_status",
        "health_state",
        "target_generated_at",
        "target_version",
        "target_age_sec",
        "active_symbol_count",
        "state_ready_for_consumers",
        "reason_codes",
        "symbols",
    }
    doc = {k: v for k, v in state.items() if k in allowed}
    raw_symbols = doc.get("symbols")
    rows = [x for x in raw_symbols if isinstance(x, dict)] if isinstance(raw_symbols, list) else []
    if expected_symbols:
        rows = [x for x in rows if normalize_symbol(str(x.get("symbol") or "")) in expected_symbols]
    doc["symbols"] = rows
    doc["active_symbol_count"] = len(rows)
    return doc


def _write_wait_pass_evidence(
    *,
    evidence_path: Path,
    strategy_line: str,
    cfg: WaitUntilReadyConfig,
    latest_path: Path,
    latest_micro: dict[str, Any],
    expected_symbols: set[str],
    expected_target_set_id: str,
    expected_target_generated_at: str,
    run_id: str | None,
    cycle_id: str | None,
) -> dict[str, Any]:
    items = [x for x in latest_micro.get("items", []) if isinstance(x, dict)] if isinstance(latest_micro.get("items"), list) else []
    state_raw: dict[str, Any] = {}
    try:
        raw = json.loads(latest_path.with_name("latest_micro_state.json").read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            state_raw = raw
    except (OSError, json.JSONDecodeError):
        state_raw = {}
    micro_features = _canonical_micro_feature_doc(latest_micro)
    micro_state = _canonical_micro_state_doc(state_raw, expected_symbols) if state_raw else None
    signal_lists = _signal_symbol_lists(items, strategy_line)
    payload: dict[str, Any] = {
        "schema_version": "10.38",
        "source": "micro_wait_pass_evidence",
        "strategy_line": strategy_line,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "target_set_id": str(latest_micro.get("target_set_id") or expected_target_set_id),
        "target_generated_at": str(latest_micro.get("target_generated_at") or expected_target_generated_at),
        "generated_at": to_iso_z(utc_now()),
        "micro_generated_at": str(latest_micro.get("generated_at") or ""),
        "micro_state_generated_at": str(state_raw.get("generated_at") or ""),
        "wait_predicate": cfg.mode,
        "min_ready_count": int(cfg.min_ready_count),
        "ready_symbols": _ready_symbols_for(items, "micro_quality"),
        "fast_ready_symbols": _ready_symbols_for(items, "micro_fast_quality"),
        "full_ready_symbols": _ready_symbols_for(items, "micro_full_quality"),
        "quality_ready_symbols": signal_lists["quality_ready_symbols"],
        "confirmed_symbols": signal_lists["confirmed_symbols"],
        "consumable_symbols": signal_lists["consumable_symbols"],
        "quality_ready_count": len(signal_lists["quality_ready_symbols"]),
        "confirmed_ready_count": len(signal_lists["confirmed_symbols"]),
        "consumable_ready_count": len(signal_lists["consumable_symbols"]),
        "micro_features": micro_features,
        "micro_state": micro_state,
    }
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    write_file_atomic(evidence_path, (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))
    return payload


def _read_target_anchor(targets_path: Path) -> tuple[str, set[str], str]:
    try:
        raw = json.loads(targets_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "", set(), ""
    if not isinstance(raw, dict):
        return "", set(), ""
    gen = str(raw.get("generated_at") or "")
    target_set_id = str(raw.get("target_set_id") or "")
    symbols: set[str] = set()
    direct_symbols = raw.get("target_symbols")
    if isinstance(direct_symbols, list):
        for sym_raw in direct_symbols:
            sym = normalize_symbol(str(sym_raw or ""))
            if sym:
                symbols.add(sym)
    for key in ("tier1_warm_watch", "tier2_active_strong"):
        rows = raw.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            sym = normalize_symbol(str(row.get("symbol") or ""))
            if sym:
                symbols.add(sym)
    return gen, symbols, target_set_id


def build_run_report_payload(
    *,
    cfg: WaitUntilReadyConfig,
    latest_path: Path,
    heartbeat_path: Path,
    targets_path: Path | None,
    started_at: str,
    ended_at: str,
    elapsed_sec: int,
    report_status: str,
    last_micro: dict[str, Any] | None,
    read_error_count: int,
    last_read_error: str | None,
    error_message: str | None,
    daemon_pid: int | None,
    expected_target_generated_at: str = "",
    expected_target_set_id: str = "",
    expected_symbols: set[str] | None = None,
    current_run_skip_reason: str = "",
) -> dict[str, Any]:
    fr = _frame_from_micro(last_micro)
    items: list[dict[str, Any]] = []
    if last_micro and isinstance(last_micro.get("items"), list):
        items = [x for x in last_micro["items"] if isinstance(x, dict)]
    all_r, strong_r, watch_r = ready_symbol_lists(items)
    not_ready_summary = build_not_ready_summary(items)
    top_global = not_ready_summary.get("top_reason_codes")
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "status": report_status,
        "started_at": started_at,
        "ended_at": ended_at,
        "elapsed_sec": elapsed_sec,
        "mode": cfg.mode,
        "latest_path": str(latest_path.resolve()),
        "heartbeat_path": str(heartbeat_path.resolve()),
        "target_generated_at": fr["target_generated_at"],
        "target_age_sec": fr["target_age_sec"],
        "final_micro_generated_at": fr["final_micro_generated_at"],
        "final_status": fr["final_status"],
        "final_target_status": fr["final_target_status"],
        "ws_status": fr["ws_status"],
        "last_ws_message_age_sec": fr["last_ws_message_age_sec"],
        "ready_count": fr["ready_count"],
        "fast_ready_count": fr["fast_ready_count"],
        "full_ready_count": fr["full_ready_count"],
        "symbol_count": fr["symbol_count"],
        "ready_scope": fr["ready_scope"],
        "target_set_id": fr["target_set_id"] or expected_target_set_id,
        "expected_target_set_id": expected_target_set_id,
        "global_symbol_count": fr["global_symbol_count"],
        "global_ready_count": fr["global_ready_count"],
        "global_fast_ready_count": fr["global_fast_ready_count"],
        "global_full_ready_count": fr["global_full_ready_count"],
        "ready_symbols": all_r,
        "ready_strong_symbols": strong_r,
        "ready_watch_symbols": watch_r,
        "read_error_count": read_error_count,
        "last_read_error": last_read_error,
        "not_ready_summary": not_ready_summary,
        "top_reason_codes": top_global if isinstance(top_global, list) else [],
        "expected_target_generated_at": expected_target_generated_at,
        "expected_symbol_count": len(expected_symbols or set()),
        "actual_symbol_count": fr["symbol_count"],
        "current_run_freshness_ok": current_run_skip_reason == "",
        "current_run_skip_reason": current_run_skip_reason,
    }
    if targets_path is not None:
        payload["targets_path"] = str(targets_path.resolve())
    if error_message:
        payload["error_message"] = error_message
    if daemon_pid is not None:
        payload["daemon_pid"] = daemon_pid
    return payload


def run_wait_until_ready_orchestration(
    *,
    project_root: Path,
    cfg: WaitUntilReadyConfig,
    latest_path: Path,
    heartbeat_path: Path,
    targets_path: Path,
    transport: str,
    start_subprocess: bool = True,
    target_stale_sec_override: int | None = None,
    output_interval_sec: int = 2,
    event_drain_interval_sec: float = 1.0,
    ring_buffer_sec: int = 1800,
    permissive_quality_smoke: bool = False,
    read_micro_json: _ReadJsonFn | None = None,
    report_dir: Path | None = None,
    evidence_path: Path | None = None,
    strategy_line: str | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    on_poll: Any | None = None,
) -> int:
    read_fn = read_micro_json or default_read_micro_json
    pr = project_root.resolve()
    latest_path = latest_path.resolve()
    heartbeat_path = heartbeat_path.resolve()
    targets_path = targets_path.resolve()
    expected_target_generated_at, expected_symbols, expected_target_set_id = _read_target_anchor(targets_path)

    target_stale = (
        int(target_stale_sec_override)
        if target_stale_sec_override is not None
        else recommended_target_stale_sec(
            max_wait_sec=cfg.max_wait_sec,
            buffer_sec=cfg.target_stale_buffer_sec,
        )
    )

    proc: subprocess.Popen | None = None
    daemon_pid: int | None = None
    if start_subprocess:
        cmd = [
            sys.executable,
            "-m",
            "laoma_signal_engine.micro.daemon.cli",
            "--targets",
            str(targets_path),
            "--latest-out",
            str(latest_path),
            "--latest-state-out",
            str(latest_path.with_name("latest_micro_state.json")),
            "--heartbeat-out",
            str(heartbeat_path),
            "--transport",
            transport,
            "--target-stale-sec",
            str(target_stale),
            "--output-interval-sec",
            str(output_interval_sec),
            "--event-drain-interval-sec",
            str(event_drain_interval_sec),
            "--ring-buffer-sec",
            str(ring_buffer_sec),
        ]
        if permissive_quality_smoke:
            cmd.append("--permissive-quality-smoke")
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(pr),
                stdin=subprocess.DEVNULL,
            )
            daemon_pid = proc.pid
        except OSError as exc:
            started = to_iso_z(utc_now())
            rep_dir = (report_dir or (pr / "DATA" / "micro" / "run_reports")).resolve()
            rep_dir.mkdir(parents=True, exist_ok=True)
            rep_path = rep_dir / f"micro_until_ready_{_report_stamp_utc(utc_now())}.json"
            payload = build_run_report_payload(
                cfg=cfg,
                latest_path=latest_path,
                heartbeat_path=heartbeat_path,
                targets_path=targets_path,
                started_at=started,
                ended_at=started,
                elapsed_sec=0,
                report_status="error",
                last_micro=None,
                read_error_count=0,
                last_read_error=None,
                error_message=f"daemon spawn failed: {exc}",
                daemon_pid=None,
            )
            data = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
            write_file_atomic(rep_path, data.encode("utf-8"))
            return EXIT_INTERNAL

    started_mono = time.monotonic()
    started_dt = utc_now()
    started_wall = to_iso_z(started_dt)
    consecutive_parse_fail = 0
    read_error_count = 0
    last_read_error: str | None = None
    last_good_micro: dict[str, Any] | None = None
    poll = max(0.05, float(cfg.poll_interval_sec))
    exit_code = EXIT_INTERNAL
    loop_error_message: str | None = None
    current_run_skip_reason = ""
    wait_evidence_payload: dict[str, Any] | None = None

    try:
        while True:
            elapsed = time.monotonic() - started_mono
            if on_poll is not None:
                try:
                    on_poll(elapsed)
                except Exception:
                    pass
            if elapsed >= float(cfg.max_wait_sec):
                exit_code = EXIT_WAIT_UNTIL_READY_TIMEOUT
                break

            if proc is not None:
                daemon_rc = proc.poll()
                if daemon_rc is not None:
                    exit_code = EXIT_INTERNAL
                    loop_error_message = f"daemon subprocess exited early (code {daemon_rc})"
                    break

            doc, err = read_fn(latest_path)
            if doc is not None:
                consecutive_parse_fail = 0
                scoped_doc = scope_micro_to_expected_symbols(
                    doc,
                    expected_symbols,
                    target_set_id=expected_target_set_id,
                    expected_target_generated_at=expected_target_generated_at,
                )
                last_good_micro = scoped_doc
                skip = micro_current_run_skip_reason(
                    doc,
                    started_at=started_dt,
                    expected_target_generated_at=expected_target_generated_at,
                    expected_symbols=expected_symbols,
                )
                if skip:
                    current_run_skip_reason = skip
                if micro_satisfies_current_run_wait(
                    doc,
                    cfg,
                    started_at=started_dt,
                    expected_target_generated_at=expected_target_generated_at,
                    expected_symbols=expected_symbols,
                ):
                    current_run_skip_reason = ""
                    exit_code = EXIT_SUCCESS
                    if evidence_path is not None and strategy_line:
                        wait_evidence_payload = _write_wait_pass_evidence(
                            evidence_path=evidence_path.resolve(),
                            strategy_line=strategy_line,
                            cfg=cfg,
                            latest_path=latest_path,
                            latest_micro=scoped_doc,
                            expected_symbols=expected_symbols,
                            expected_target_set_id=expected_target_set_id,
                            expected_target_generated_at=expected_target_generated_at,
                            run_id=run_id,
                            cycle_id=cycle_id,
                        )
                    break
            else:
                if err is not None:
                    consecutive_parse_fail += 1
                    read_error_count += 1
                    last_read_error = err
                    if consecutive_parse_fail >= int(cfg.max_consecutive_read_failures):
                        exit_code = EXIT_INTERNAL
                        loop_error_message = "max_consecutive_read_failures exceeded"
                        break
                else:
                    consecutive_parse_fail = 0

            time.sleep(poll)
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)

    ended_wall = to_iso_z(utc_now())
    elapsed_sec = int(max(0.0, time.monotonic() - started_mono))
    rep_dir = (report_dir or (pr / "DATA" / "micro" / "run_reports")).resolve()
    rep_dir.mkdir(parents=True, exist_ok=True)
    rep_path = rep_dir / f"micro_until_ready_{_report_stamp_utc(utc_now())}.json"

    if exit_code == EXIT_SUCCESS:
        status = "ready_met"
        err_msg = None
    elif exit_code == EXIT_WAIT_UNTIL_READY_TIMEOUT:
        status = "timeout"
        err_msg = None
    else:
        status = "error"
        err_msg = loop_error_message or "wait loop ended with error"

    payload = build_run_report_payload(
        cfg=cfg,
        latest_path=latest_path,
        heartbeat_path=heartbeat_path,
        targets_path=targets_path,
        started_at=started_wall,
        ended_at=ended_wall,
        elapsed_sec=elapsed_sec,
        report_status=status,
        last_micro=last_good_micro,
        read_error_count=read_error_count,
        last_read_error=last_read_error,
        error_message=err_msg,
        daemon_pid=daemon_pid,
        expected_target_generated_at=expected_target_generated_at,
        expected_target_set_id=expected_target_set_id,
        expected_symbols=expected_symbols,
        current_run_skip_reason=current_run_skip_reason,
    )
    if wait_evidence_payload is not None and evidence_path is not None:
        payload.update(
            {
                "wait_evidence_path": str(evidence_path.resolve()),
                "wait_predicate": cfg.mode,
                "wait_pass_micro_generated_at": wait_evidence_payload.get("micro_generated_at"),
                "wait_pass_micro_state_generated_at": wait_evidence_payload.get("micro_state_generated_at"),
                "wait_pass_ready_symbols": wait_evidence_payload.get("ready_symbols"),
                "wait_pass_fast_ready_symbols": wait_evidence_payload.get("fast_ready_symbols"),
                "wait_pass_full_ready_symbols": wait_evidence_payload.get("full_ready_symbols"),
            },
        )
    data = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    write_file_atomic(rep_path, data.encode("utf-8"))

    if exit_code == EXIT_INTERNAL and consecutive_parse_fail >= int(cfg.max_consecutive_read_failures):
        return EXIT_CONFIG
    return exit_code
