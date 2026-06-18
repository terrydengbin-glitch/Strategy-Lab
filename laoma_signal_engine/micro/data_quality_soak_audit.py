"""STEP10.50 real-run soak audit for micro data-quality attribution."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.json_io import write_json_atomic
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.micro.data_quality_attribution import RAW_REASONS, init_micro_quality_db


def _root(project_root: Path | None = None) -> Path:
    return Path(project_root).resolve() if project_root else Path.cwd().resolve()


def _db_path(root: Path, db_path: Path | None = None) -> Path:
    return db_path or root / "DATA/audit/run_audit.db"


def _connect(db: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "select 1 from sqlite_master where type='table' and name=?",
        (table,),
    ).fetchone()
    return row is not None


def _loads(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _safe_rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _recent_runs(conn: sqlite3.Connection, *, limit: int) -> list[dict[str, Any]]:
    runs: dict[str, dict[str, Any]] = {}
    if _table_exists(conn, "audit_runs"):
        for row in _safe_rows(
            conn,
            "select run_id, cycle_id, status, generated_at from audit_runs order by generated_at desc limit ?",
            (int(limit),),
        ):
            runs[str(row.get("run_id"))] = row
    if _table_exists(conn, "micro_quality_attributions"):
        for row in _safe_rows(
            conn,
            """
            select run_id, max(cycle_id) as cycle_id, max(generated_at) as generated_at
            from micro_quality_attributions
            group by run_id
            order by generated_at desc
            limit ?
            """,
            (int(limit),),
        ):
            run_id = str(row.get("run_id"))
            runs.setdefault(run_id, {"run_id": run_id, "cycle_id": row.get("cycle_id"), "status": "micro_quality_only", "generated_at": row.get("generated_at")})
    return sorted(runs.values(), key=lambda r: str(r.get("generated_at") or ""), reverse=True)[: int(limit)]


def _audit_symbols(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    if not _table_exists(conn, "audit_symbols"):
        return []
    rows = _safe_rows(
        conn,
        "select * from audit_symbols where run_id = ?",
        (run_id,),
    )
    for row in rows:
        row["reason_codes"] = _loads(row.get("reason_codes_json")) or []
        row["payload"] = _loads(row.get("payload_json")) or {}
    return rows


def _quality_rows(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    if not _table_exists(conn, "micro_quality_attributions"):
        return []
    rows = _safe_rows(
        conn,
        "select * from micro_quality_attributions where run_id = ? order by strategy_line, symbol, raw_reason",
        (run_id,),
    )
    for row in rows:
        row["missing_evidence_fields"] = _loads(row.get("missing_evidence_fields_json")) or []
        row["evidence"] = _loads(row.get("evidence_json")) or {}
    return rows


def _runtime_v2_rows(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    if not _table_exists(conn, "micro_evidence_runtime_v2_symbols"):
        return []
    rows = _safe_rows(
        conn,
        "select * from micro_evidence_runtime_v2_symbols where run_id = ? order by strategy_line, symbol",
        (run_id,),
    )
    for row in rows:
        row["raw_reasons"] = _loads(row.get("raw_reasons_json")) or []
        row["categories"] = _loads(row.get("categories_json")) or []
        row["factor_frame"] = _loads(row.get("factor_frame_json")) or {}
        row["stream_heartbeat"] = _loads(row.get("stream_heartbeat_json")) or {}
        row["z_window"] = _loads(row.get("z_window_json")) or {}
    return rows


def _latest_target_source(root: Path) -> dict[str, Any]:
    path = root / "DATA" / "micro" / "micro_targets.json"
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {
        "generated_at": raw.get("generated_at"),
        "target_set_id": raw.get("target_set_id"),
        "status": raw.get("status"),
        "target_count": raw.get("target_count"),
        "raw_fill": raw.get("raw_fill") if isinstance(raw.get("raw_fill"), dict) else {},
        "sticky_pool": raw.get("sticky_pool") if isinstance(raw.get("sticky_pool"), dict) else {},
        "target_source_distribution": raw.get("target_source_distribution")
        if isinstance(raw.get("target_source_distribution"), dict)
        else {},
    }


def _downstream_rows(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    if not _table_exists(conn, "audit_downstream_events"):
        return []
    rows = _safe_rows(
        conn,
        "select * from audit_downstream_events where run_id = ?",
        (run_id,),
    )
    for row in rows:
        row["payload"] = _loads(row.get("payload_json")) or {}
    return rows


def _run_summary(conn: sqlite3.Connection, run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run.get("run_id") or "")
    quality = _quality_rows(conn, run_id)
    runtime_v2 = _runtime_v2_rows(conn, run_id)
    symbols = _audit_symbols(conn, run_id)
    downstream = _downstream_rows(conn, run_id)
    raw_counts = Counter(str(row.get("raw_reason") or "") for row in quality if row.get("raw_reason"))
    attr_counts = Counter(str(row.get("attributed_reason") or "") for row in quality if row.get("attributed_reason"))
    category_counts = Counter(str(row.get("category") or "") for row in quality if row.get("category"))
    by_line: dict[str, Counter[str]] = defaultdict(Counter)
    for row in quality:
        by_line[str(row.get("strategy_line") or "unknown")][str(row.get("raw_reason") or "")] += 1

    executable_symbols = {
        (str(row.get("strategy_line") or ""), str(row.get("symbol") or "").upper())
        for row in symbols
        if int(row.get("executable") or 0) == 1
    }
    consumed_symbols = {
        (str(row.get("strategy_line") or ""), str(row.get("symbol") or "").upper())
        for row in symbols
    }
    technical_blocked_symbols = {
        (str(row.get("strategy_line") or ""), str(row.get("symbol") or "").upper())
        for row in quality
        if row.get("category") == "technical_fix"
    }

    violations: list[dict[str, Any]] = []
    for line, symbol in sorted(technical_blocked_symbols & consumed_symbols):
        violations.append(
            {
                "type": "technical_not_ready_consumed_by_trade_plan",
                "strategy_line": line,
                "symbol": symbol,
            },
        )
    for row in downstream:
        event_type = str(row.get("event_type") or "")
        line = str(row.get("strategy_line") or "")
        symbol = str(row.get("symbol") or "").upper()
        if event_type == "paper_order" and line and symbol and (line, symbol) not in executable_symbols:
            violations.append({"type": "paper_non_executable_consumed", "strategy_line": line, "symbol": symbol})
        if "feishu" in event_type and line and symbol and (line, symbol) not in executable_symbols:
            violations.append({"type": "feishu_non_executable_sent", "strategy_line": line, "symbol": symbol})

    missing_evidence = Counter()
    for row in quality:
        missing_evidence.update(row.get("missing_evidence_fields") or [])
    runtime_severity = Counter(str(row.get("severity") or "") for row in runtime_v2 if row.get("severity"))
    runtime_status = Counter(str(row.get("status") or "") for row in runtime_v2 if row.get("status"))
    alignment_status = Counter(
        str((row.get("factor_frame") or {}).get("alignment_status") or "")
        for row in runtime_v2
        if isinstance(row.get("factor_frame"), dict)
    )
    true_alignment_reason = Counter(
        str((row.get("factor_frame") or {}).get("true_alignment_reason") or "")
        for row in runtime_v2
        if isinstance(row.get("factor_frame"), dict)
    )
    store_status = Counter()
    store_missing_reason = Counter()
    coverage_root_cause = Counter()
    for row in runtime_v2:
        z_window = row.get("z_window") if isinstance(row.get("z_window"), dict) else {}
        store_window = z_window.get("store_window") if isinstance(z_window.get("store_window"), dict) else {}
        if store_window:
            store_status[str(store_window.get("full_z_status") or "")] += 1
            if store_window.get("full_z_missing_reason"):
                store_missing_reason[str(store_window.get("full_z_missing_reason"))] += 1
        heartbeat = row.get("stream_heartbeat") if isinstance(row.get("stream_heartbeat"), dict) else {}
        streams = heartbeat.get("streams") if isinstance(heartbeat.get("streams"), dict) else {}
        for entry in streams.values():
            if isinstance(entry, dict) and entry.get("root_cause"):
                coverage_root_cause[str(entry.get("root_cause"))] += 1

    confirmed_count = 0
    for row in symbols:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        guards = payload.get("guards") if isinstance(payload.get("guards"), dict) else {}
        if guards.get("trade_plan_consumable") is True:
            confirmed_count += 1

    return {
        "run_id": run_id,
        "cycle_id": run.get("cycle_id"),
        "status": run.get("status"),
        "generated_at": run.get("generated_at"),
        "selected_strategy_lines": sorted({str(row.get("strategy_line") or "") for row in symbols if row.get("strategy_line")}),
        "metrics": {
            "cvd_never_updated_count": raw_counts.get("cvd_never_updated", 0),
            "ofi_never_updated_count": raw_counts.get("ofi_never_updated", 0),
            "cvd_stale_count": raw_counts.get("cvd_stale", 0),
            "ofi_stale_count": raw_counts.get("ofi_stale", 0),
            "ofi_cvd_lag_high_count": raw_counts.get("ofi_cvd_lag_high", 0),
            "fast_z_missing_count": raw_counts.get("fast_z_missing", 0),
            "full_z_missing_count": raw_counts.get("full_z_missing", 0),
            "technical_fix_count": category_counts.get("technical_fix", 0),
            "config_fix_count": category_counts.get("config_fix", 0),
            "market_accept_count": category_counts.get("market_accept", 0),
            "expected_warmup_count": category_counts.get("expected_warmup", 0),
            "unknown_count": category_counts.get("unknown_blocker", 0),
            "confirmed_symbol_count": confirmed_count,
            "trade_plan_consumed_symbol_count": len(consumed_symbols),
            "paper_order_count": sum(1 for row in downstream if row.get("event_type") == "paper_order"),
            "feishu_delivery_count": sum(1 for row in downstream if "feishu" in str(row.get("event_type") or "")),
            "runtime_v2_symbol_count": len(runtime_v2),
            "runtime_v2_p0_count": runtime_severity.get("P0", 0),
            "runtime_v2_p1_count": runtime_severity.get("P1", 0),
            "runtime_v2_aligned_count": alignment_status.get("aligned", 0),
            "runtime_v2_lagging_count": alignment_status.get("lagging", 0),
            "runtime_v2_broken_count": alignment_status.get("broken", 0),
            "full_z_store_available_count": store_status.get("available", 0),
            "full_z_store_missing_count": store_status.get("missing", 0),
        },
        "raw_reason_counts": dict(raw_counts),
        "attribution_counts": dict(attr_counts),
        "category_counts": dict(category_counts),
        "line_raw_reason_counts": {line: dict(counter) for line, counter in by_line.items()},
        "missing_evidence_fields": dict(missing_evidence),
        "runtime_v2": {
            "severity_counts": dict(runtime_severity),
            "status_counts": dict(runtime_status),
            "alignment_counts": dict(alignment_status),
            "true_alignment_reason_counts": dict(true_alignment_reason),
            "store_status_counts": dict(store_status),
            "store_missing_reason_counts": dict(store_missing_reason),
            "coverage_root_cause_counts": dict(coverage_root_cause),
            "symbols": runtime_v2[:200],
        },
        "violations": violations,
        "symbol_blockers": quality[:200],
    }


def build_micro_data_quality_soak_audit(
    project_root: Path | None = None,
    *,
    lookback_runs: int = 20,
    min_runs: int = 10,
    db_path: Path | None = None,
) -> dict[str, Any]:
    root = _root(project_root)
    db = _db_path(root, db_path)
    generated_at = to_iso_z(utc_now())
    if not db.exists():
        return {
            "schema_version": "10.50",
            "source": "micro_data_quality_real_soak_audit",
            "generated_at": generated_at,
            "status": "no_data",
            "db_path": str(db),
            "reason_codes": ["audit_db_missing"],
            "runs": [],
            "summary": {},
        }
    init_micro_quality_db(db)
    with _connect(db) as conn:
        runs = _recent_runs(conn, limit=lookback_runs)
        run_payloads = [_run_summary(conn, run) for run in runs]
    global_raw = Counter()
    global_attr = Counter()
    global_category = Counter()
    global_missing = Counter()
    global_runtime_severity = Counter()
    global_runtime_alignment = Counter()
    global_true_alignment = Counter()
    global_store_status = Counter()
    global_store_missing = Counter()
    global_coverage_root = Counter()
    violations: list[dict[str, Any]] = []
    for run in run_payloads:
        global_raw.update(run.get("raw_reason_counts") or {})
        global_attr.update(run.get("attribution_counts") or {})
        global_category.update(run.get("category_counts") or {})
        global_missing.update(run.get("missing_evidence_fields") or {})
        runtime_v2 = run.get("runtime_v2") or {}
        global_runtime_severity.update(runtime_v2.get("severity_counts") or {})
        global_runtime_alignment.update(runtime_v2.get("alignment_counts") or {})
        global_true_alignment.update(runtime_v2.get("true_alignment_reason_counts") or {})
        global_store_status.update(runtime_v2.get("store_status_counts") or {})
        global_store_missing.update(runtime_v2.get("store_missing_reason_counts") or {})
        global_coverage_root.update(runtime_v2.get("coverage_root_cause_counts") or {})
        for violation in run.get("violations") or []:
            violations.append({"run_id": run.get("run_id"), **violation})
    reason_codes: list[str] = []
    if len(run_payloads) < int(min_runs):
        reason_codes.append("soak_run_count_below_min")
    if violations:
        reason_codes.append("downstream_consumption_violation")
    if global_category.get("unknown_blocker", 0):
        reason_codes.append("unknown_blocker_present")
    status = "failed" if violations else ("warning" if reason_codes else "ok")
    return {
        "schema_version": "10.50",
        "source": "micro_data_quality_real_soak_audit",
        "generated_at": generated_at,
        "status": status,
        "db_path": str(db),
        "lookback_runs": int(lookback_runs),
        "min_runs": int(min_runs),
        "reason_codes": reason_codes,
        "summary": {
            "run_count": len(run_payloads),
            "raw_reason_counts": dict(global_raw),
            "attribution_counts": dict(global_attr),
            "category_counts": dict(global_category),
            "technical_fix_count": global_category.get("technical_fix", 0),
            "config_fix_count": global_category.get("config_fix", 0),
            "market_accept_count": global_category.get("market_accept", 0),
            "expected_warmup_count": global_category.get("expected_warmup", 0),
            "unknown_count": global_category.get("unknown_blocker", 0),
            "missing_evidence_fields": dict(global_missing),
            "runtime_v2_severity_counts": dict(global_runtime_severity),
            "runtime_v2_alignment_counts": dict(global_runtime_alignment),
            "runtime_v2_true_alignment_reason_counts": dict(global_true_alignment),
            "full_z_store_status_counts": dict(global_store_status),
            "full_z_store_missing_reason_counts": dict(global_store_missing),
            "coverage_root_cause_counts": dict(global_coverage_root),
            "violation_count": len(violations),
        },
        "runs": run_payloads,
        "violations": violations,
        "latest_target_source": _latest_target_source(root),
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# STEP10.50 Micro Data Quality Real Soak Audit",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- status: `{payload.get('status')}`",
        f"- run_count: `{(payload.get('summary') or {}).get('run_count', 0)}`",
        f"- reason_codes: `{', '.join(payload.get('reason_codes') or []) or 'none'}`",
        "",
        "## Top Raw Reasons",
        "",
    ]
    for key, value in (payload.get("summary") or {}).get("raw_reason_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Top Attributed Reasons", ""])
    for key, value in (payload.get("summary") or {}).get("attribution_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Category Counts", ""])
    for key, value in (payload.get("summary") or {}).get("category_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Missing Evidence Fields", ""])
    missing = (payload.get("summary") or {}).get("missing_evidence_fields") or {}
    if missing:
        for key, value in missing.items():
            lines.append(f"- `{key}`: {value}")
    else:
        lines.append("- none")
    lines.extend(["", "## Runtime V2 Severity", ""])
    for key, value in (payload.get("summary") or {}).get("runtime_v2_severity_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Runtime V2 Bucket Alignment", ""])
    for key, value in (payload.get("summary") or {}).get("runtime_v2_alignment_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Runtime V2 True Alignment Reason", ""])
    for key, value in (payload.get("summary") or {}).get("runtime_v2_true_alignment_reason_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Full Z Store Window", ""])
    for key, value in (payload.get("summary") or {}).get("full_z_store_status_counts", {}).items():
        lines.append(f"- status `{key}`: {value}")
    for key, value in (payload.get("summary") or {}).get("full_z_store_missing_reason_counts", {}).items():
        lines.append(f"- missing `{key}`: {value}")
    lines.extend(["", "## Coverage Root Cause", ""])
    for key, value in (payload.get("summary") or {}).get("coverage_root_cause_counts", {}).items():
        lines.append(f"- `{key}`: {value}")
    latest_target = payload.get("latest_target_source") if isinstance(payload.get("latest_target_source"), dict) else {}
    lines.extend(["", "## Latest Target Source / Raw Fill", ""])
    if latest_target:
        lines.append(f"- target_set_id: `{latest_target.get('target_set_id')}`")
        lines.append(f"- raw_fill: `{json.dumps(latest_target.get('raw_fill') or {}, ensure_ascii=False)}`")
        lines.append(f"- target_source_distribution: `{json.dumps(latest_target.get('target_source_distribution') or {}, ensure_ascii=False)}`")
    else:
        lines.append("- no latest target source snapshot")
    lines.extend(["", "## Downstream Consumption Violations", ""])
    if payload.get("violations"):
        for row in payload.get("violations") or []:
            lines.append(
                f"- `{row.get('run_id')}` `{row.get('strategy_line')}` `{row.get('symbol')}`: {row.get('type')}",
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Per-Run Summary", ""])
    lines.append("| run_id | status | raw reasons | technical | market | unknown | consumed | paper | violations |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for run in payload.get("runs") or []:
        metrics = run.get("metrics") or {}
        raw_total = sum((run.get("raw_reason_counts") or {}).values())
        lines.append(
            "| {run_id} | {status} | {raw_total} | {technical} | {market} | {unknown} | {consumed} | {paper} | {violations} |".format(
                run_id=run.get("run_id") or "",
                status=run.get("status") or "",
                raw_total=raw_total,
                technical=metrics.get("technical_fix_count", 0),
                market=metrics.get("market_accept_count", 0),
                unknown=metrics.get("unknown_count", 0),
                consumed=metrics.get("trade_plan_consumed_symbol_count", 0),
                paper=metrics.get("paper_order_count", 0),
                violations=len(run.get("violations") or []),
            ),
        )
    lines.append("")
    return "\n".join(lines)


def write_micro_data_quality_soak_audit(
    project_root: Path | None = None,
    *,
    output_json: Path | None = None,
    output_md: Path | None = None,
    db_path: Path | None = None,
    lookback_runs: int = 20,
    min_runs: int = 10,
) -> dict[str, Any]:
    root = _root(project_root)
    payload = build_micro_data_quality_soak_audit(
        root,
        lookback_runs=lookback_runs,
        min_runs=min_runs,
        db_path=db_path,
    )
    ts = to_iso_z(utc_now()).replace("-", "").replace(":", "").replace("Z", "Z")
    reports = root / "docs/reports"
    json_path = output_json or reports / f"STEP10.50_micro_data_quality_soak_{ts}.json"
    md_path = output_md or reports / f"STEP10.50_micro_data_quality_soak_{ts}.md"
    payload["findings_path"] = str(json_path)
    payload["report_path"] = str(md_path)
    write_json_atomic(json_path, payload)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(_markdown(payload), encoding="utf-8")
    latest = root / "DATA/reports/latest_micro_data_quality_soak_audit.json"
    write_json_atomic(latest, payload)
    return payload


def run_write_micro_data_quality_soak_audit_safe(
    *,
    project_root: Path | None = None,
    output_json: Path | None = None,
    output_md: Path | None = None,
    db_path: Path | None = None,
    lookback_runs: int = 20,
    min_runs: int = 10,
    stdout_json: bool = False,
) -> int:
    try:
        payload = write_micro_data_quality_soak_audit(
            project_root,
            output_json=output_json,
            output_md=output_md,
            db_path=db_path,
            lookback_runs=lookback_runs,
            min_runs=min_runs,
        )
        summary = {
            "status": payload.get("status"),
            "reason_codes": payload.get("reason_codes") or [],
            "run_count": (payload.get("summary") or {}).get("run_count", 0),
            "findings_path": payload.get("findings_path"),
            "report_path": payload.get("report_path"),
        }
        print(json.dumps(summary, ensure_ascii=False) if stdout_json else f"STEP10.50 soak audit written status={summary['status']}")
        return 0
    except Exception as exc:
        result = {"status": "failed", "error": str(exc)}
        print(json.dumps(result, ensure_ascii=False) if stdout_json else f"STEP10.50 soak audit failed: {exc}")
        return 1
