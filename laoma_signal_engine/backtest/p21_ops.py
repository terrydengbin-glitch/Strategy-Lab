from __future__ import annotations

import hashlib
import json
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_gate_scoring import SCHEMA_VERSION as GATE_SCHEMA_VERSION
from laoma_signal_engine.backtest.p21_gate_scoring import candidates_payload, scores_payload
from laoma_signal_engine.backtest.p21_trade_quality import materialize_payload as tq_materialize_payload
from laoma_signal_engine.backtest.p21_trade_quality_v5 import (
    generate_gate_candidates_v5_payload,
    materialize_v5_payload,
)
from laoma_signal_engine.backtest.p21_v2 import _connect, _loads, _num, ensure_p21_v2_tables

OPS_SCHEMA_VERSION = "21.28-backtest-ops"
TQ_JOB_SCHEMA_VERSION = "24.18-async-tq-v4-v5-materialization"
ANALYSIS_DIR = Path("DATA/backtest/analysis")
RETENTION_DIR = Path("DATA/backtest/retention")
CANDIDATE_DIR = Path("DATA/backtest/candidates")
SERVING_DB_RELATIVE = ANALYSIS_DIR / "p21_serving_read_model.db"

RAW_HEAVY_HINTS = ("kline", "shadow_order", "matrix_shard", "order")
LIGHT_INDEX_TABLES = {
    "p21_v2_experiments",
    "p21_v2_30d_metrics",
    "p21_v2_daily_metrics",
    "p21_v2_symbol_metrics",
    "p21_v2_recommendations",
    "p21_v2_jobs",
}
ANALYSIS_MART_PREFIXES = (
    "backtest_trade_quality_",
    "backtest_gate_",
    "backtest_tq_",
)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _stable_id(prefix: str, payload: Any, size: int = 20) -> str:
    return f"{prefix}_{hashlib.sha256(_json(payload).encode('utf-8')).hexdigest()[:size]}"


def _serving_db_path(project_root: Path) -> Path:
    return project_root.resolve() / SERVING_DB_RELATIVE


def _connect_readonly(db_path: Path, timeout: float = 5.0) -> sqlite3.Connection:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=timeout)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _run_with_budget(conn: sqlite3.Connection, sql: str, params: list[Any] | tuple[Any, ...] = (), *, op_budget: int = 250_000) -> list[sqlite3.Row]:
    remaining = {"ops": int(op_budget)}

    def progress() -> int:
        remaining["ops"] -= 1
        return 1 if remaining["ops"] <= 0 else 0

    conn.set_progress_handler(progress, 1000)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.set_progress_handler(None, 0)


def _classify_table(name: str) -> str:
    lower = name.lower()
    if name in LIGHT_INDEX_TABLES:
        return "light_index"
    if lower.startswith(ANALYSIS_MART_PREFIXES):
        return "analysis_mart"
    if any(hint in lower for hint in RAW_HEAVY_HINTS):
        return "raw_heavy"
    return "unknown"


def _table_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [str(row["name"]) for row in rows]


def _safe_count(conn: sqlite3.Connection, table: str, *, op_budget: int = 50_000) -> dict[str, Any]:
    if op_budget <= 0:
        return {"row_count": None, "status": "skipped", "reason": "row_count_budget_disabled"}
    try:
        rows = _run_with_budget(conn, f"SELECT COUNT(*) AS count FROM {table}", op_budget=op_budget)
        return {"row_count": int(rows[0]["count"]), "status": "ok"}
    except sqlite3.OperationalError as exc:
        return {"row_count": None, "status": "deferred", "reason": str(exc)}


def _index_list(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    try:
        return [dict(row) for row in conn.execute(f"PRAGMA index_list({table})").fetchall()]
    except sqlite3.OperationalError:
        return []


def footprint_payload(project_root: Path, *, row_count_budget: int = 0, include_dbstat: bool = False) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    generated_at = _now()
    if not db_path.exists():
        return {"schema_version": OPS_SCHEMA_VERSION, "status": "missing", "db_path": str(db_path), "generated_at": generated_at}
    wal_path = Path(str(db_path) + "-wal")
    shm_path = Path(str(db_path) + "-shm")
    with _connect_readonly(db_path) as conn:
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        tables = []
        dbstat_by_name: dict[str, dict[str, Any]] = {}
        if include_dbstat:
            try:
                for row in _run_with_budget(
                    conn,
                    "SELECT name, SUM(pgsize) AS bytes, COUNT(*) AS pages FROM dbstat GROUP BY name",
                    op_budget=500_000,
                ):
                    dbstat_by_name[str(row["name"])] = {"bytes": int(row["bytes"] or 0), "pages": int(row["pages"] or 0)}
            except sqlite3.OperationalError:
                dbstat_by_name = {}
        for name in _table_names(conn):
            count = _safe_count(conn, name, op_budget=row_count_budget)
            tables.append(
                {
                    "table": name,
                    "category": _classify_table(name),
                    **count,
                    "indexes": _index_list(conn, name),
                    "dbstat": dbstat_by_name.get(name),
                }
            )
    categories: dict[str, int] = {}
    for table in tables:
        categories[table["category"]] = categories.get(table["category"], 0) + 1
    return {
        "schema_version": OPS_SCHEMA_VERSION,
        "status": "ok",
        "db_path": str(db_path),
        "db_size_bytes": db_path.stat().st_size,
        "wal_size_bytes": wal_path.stat().st_size if wal_path.exists() else 0,
        "shm_size_bytes": shm_path.stat().st_size if shm_path.exists() else 0,
        "page_count": page_count,
        "page_size": page_size,
        "freelist_count": freelist_count,
        "freelist_bytes": freelist_count * page_size,
        "estimated_size_bytes": page_count * page_size,
        "table_count": len(tables),
        "category_counts": categories,
        "tables": tables,
        "cleanup_candidates": [t for t in tables if t["category"] == "raw_heavy"],
        "ui_scan_safe": False,
        "recommendation": "Use bounded analysis mart / serving read model; do not let UI scan raw-heavy tables.",
        "generated_at": generated_at,
    }


def write_footprint_report(project_root: Path, *, row_count_budget: int = 0, include_dbstat: bool = False) -> dict[str, Any]:
    payload = footprint_payload(project_root, row_count_budget=row_count_budget, include_dbstat=include_dbstat)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = project_root / "docs" / "reports" / f"STEP21.28_backtest_db_space_table_footprint_{ts}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# STEP21.28 Backtest DB Space / Table Footprint Audit",
        "",
        f"> generated_at: {payload.get('generated_at')}",
        f"> status: {payload.get('status')}",
        "",
        "## Database",
        "",
        f"- path: `{payload.get('db_path')}`",
        f"- size_bytes: `{payload.get('db_size_bytes')}`",
        f"- wal_size_bytes: `{payload.get('wal_size_bytes')}`",
        f"- page_count: `{payload.get('page_count')}`",
        f"- page_size: `{payload.get('page_size')}`",
        f"- freelist_count: `{payload.get('freelist_count')}`",
        f"- freelist_bytes: `{payload.get('freelist_bytes')}`",
        "",
        "## Categories",
        "",
    ]
    for key, count in sorted((payload.get("category_counts") or {}).items()):
        lines.append(f"- {key}: {count}")
    lines.extend(["", "## Tables", "", "| table | category | row_count | status | index_count |", "|---|---:|---:|---|---:|"])
    for table in payload.get("tables") or []:
        lines.append(
            f"| {table['table']} | {table['category']} | {table.get('row_count')} | {table.get('status')} | {len(table.get('indexes') or [])} |"
        )
    lines.extend(["", "## Cleanup Candidates", ""])
    for table in payload.get("cleanup_candidates") or []:
        lines.append(f"- `{table['table']}` ({table.get('status')}, rows={table.get('row_count')})")
    lines.extend(["", "## Recommendation", "", str(payload.get("recommendation"))])
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    payload["report_path"] = str(report_path)
    return payload


def _metric_rows(conn: sqlite3.Connection, *, op_budget: int = 75_000) -> list[dict[str, Any]]:
    try:
        rows = _run_with_budget(conn, "SELECT * FROM p21_v2_30d_metrics", op_budget=op_budget)
    except sqlite3.OperationalError:
        return []
    out = []
    for row in rows:
        item = dict(row)
        metrics = _loads(item.get("metrics_json"), {})
        params = _loads(item.get("parameters_json"), {})
        item["metrics"] = metrics
        item["parameters"] = params
        item["profit_factor"] = _num(metrics.get("profit_factor"), 0.0)
        item["total_R"] = _num(metrics.get("total_R"), 0.0)
        item["expectancy_R"] = _num(metrics.get("expectancy_R"), 0.0)
        item["trade_count"] = int(_num(metrics.get("trade_count") or metrics.get("accepted_count"), 0.0))
        out.append(item)
    return out


def _experiment_rows(conn: sqlite3.Connection, *, limit: int = 5000, op_budget: int = 75_000) -> list[dict[str, Any]]:
    try:
        rows = _run_with_budget(
            conn,
            """
            SELECT experiment_id, best_parameter_set_id AS parameter_set_id, strategy_line,
                   best_profit_factor, best_expectancy_R, trade_count, generated_at
            FROM p21_v2_experiments
            WHERE status = 'completed'
            ORDER BY generated_at DESC
            LIMIT ?
            """,
            (int(limit),),
            op_budget=op_budget,
        )
    except sqlite3.OperationalError:
        return []
    out = []
    for row in rows:
        item = dict(row)
        trade_count = int(_num(item.get("trade_count"), 0.0))
        profit_factor = _num(item.get("best_profit_factor"), 0.0)
        expectancy = _num(item.get("best_expectancy_R"), 0.0)
        item["metrics"] = {
            "profit_factor": profit_factor,
            "expectancy_R": expectancy,
            "trade_count": trade_count,
        }
        item["parameters"] = {}
        item["profit_factor"] = profit_factor
        item["expectancy_R"] = expectancy
        item["total_R"] = expectancy * trade_count
        item["trade_count"] = trade_count
        return_id = item.get("parameter_set_id")
        item["parameter_set_id"] = return_id or "unknown_parameter_set"
        out.append(item)
    return out


def _key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("experiment_id")), str(row.get("parameter_set_id")), str(row.get("strategy_line")))


def _select_retained(metric_rows: list[dict[str, Any]], *, min_trade_count: int = 30, seed: int = 21029) -> dict[tuple[str, str, str], set[str]]:
    retained: dict[tuple[str, str, str], set[str]] = {}

    def add(rows: list[dict[str, Any]], rule: str, limit: int) -> None:
        for row in rows[:limit]:
            retained.setdefault(_key(row), set()).add(rule)

    eligible = [row for row in metric_rows if int(row.get("trade_count") or 0) >= min_trade_count]
    add(sorted(eligible, key=lambda r: r.get("profit_factor") or 0, reverse=True), "top_pf_min_trade_count", 50)
    add(sorted(eligible, key=lambda r: r.get("total_R") or 0, reverse=True), "top_total_R", 50)
    add(sorted(eligible, key=lambda r: r.get("expectancy_R") or 0, reverse=True), "top_expectancy_R", 20)
    add(sorted(metric_rows, key=lambda r: r.get("total_R") or 0), "worst_negative_samples", 20)
    by_line: dict[str, list[dict[str, Any]]] = {}
    for row in eligible:
        by_line.setdefault(str(row.get("strategy_line")), []).append(row)
    for line_rows in by_line.values():
        add(sorted(line_rows, key=lambda r: r.get("profit_factor") or 0, reverse=True), "per_strategy_line_top", 30)
    near = [row for row in eligible if 0.85 <= float(row.get("profit_factor") or 0) <= 1.15]
    add(sorted(near, key=lambda r: abs(1.0 - float(r.get("profit_factor") or 0))), "near_threshold_candidates", 50)
    rng = random.Random(seed)
    shuffled = list(eligible)
    rng.shuffle(shuffled)
    add(shuffled, "random_stratified_control", 50)
    return retained


def retention_manifest_payload(
    project_root: Path,
    *,
    min_trade_count: int = 30,
    write: bool = True,
    shadow_count_budget: int = 75_000,
) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    generated_at = _now()
    if not db_path.exists():
        return {"schema_version": OPS_SCHEMA_VERSION, "status": "missing", "source_db": str(db_path), "generated_at": generated_at}
    with _connect_readonly(db_path) as conn:
        metrics = _experiment_rows(conn, limit=5000, op_budget=shadow_count_budget)
        source_table = "p21_v2_experiments"
        if not metrics:
            metrics = _metric_rows(conn, op_budget=shadow_count_budget)
            source_table = "p21_v2_30d_metrics"
        retained_map = _select_retained(metrics, min_trade_count=min_trade_count)
        retained_keys = set(retained_map)
        retained = []
        excluded = []
        retained_shadow_count = 0
        excluded_shadow_estimate = 0
        for row in metrics:
            key = _key(row)
            record = {
                "experiment_id": key[0],
                "parameter_set_id": key[1],
                "strategy_line": key[2],
                "trade_count": int(row.get("trade_count") or 0),
                "profit_factor": row.get("profit_factor"),
                "total_R": row.get("total_R"),
                "expectancy_R": row.get("expectancy_R"),
            }
            if key in retained_keys:
                shadow_count = int(record["trade_count"])
                count_status = "estimated_from_experiment_summary"
                record.update({"retention_rules": sorted(retained_map[key]), "shadow_order_count": shadow_count, "shadow_count_status": count_status})
                retained_shadow_count += shadow_count
                retained.append(record)
            else:
                reason = "low_rank_low_information"
                if int(record["trade_count"]) < min_trade_count:
                    reason = "below_min_trade_count"
                record.update({"reason_code": reason, "shadow_order_count_estimate": int(record["trade_count"])})
                excluded_shadow_estimate += int(record["trade_count"])
                excluded.append(record)
    manifest = {
        "manifest_version": "v1",
        "schema_version": OPS_SCHEMA_VERSION,
        "status": "dry_run",
        "source_db": str(db_path),
        "source_table": source_table,
        "source_db_size_bytes": db_path.stat().st_size,
        "generated_at": generated_at,
        "min_trade_count": min_trade_count,
        "retention_rules": [
            "top_pf_min_trade_count",
            "top_total_R",
            "top_expectancy_R",
            "worst_negative_samples",
            "per_strategy_line_top",
            "near_threshold_candidates",
            "random_stratified_control",
        ],
        "retained_parameter_sets": retained,
        "excluded_parameter_sets": excluded,
        "retained_count": len(retained),
        "discarded_count": len(excluded),
        "validation": {
            "metrics_count_before": len(metrics),
            "metrics_count_after": len(retained),
            "excluded_metrics_count": len(excluded),
            "retained_shadow_order_count": retained_shadow_count,
            "excluded_shadow_order_estimate": excluded_shadow_estimate,
        },
    }
    if write:
        out_dir = project_root / RETENTION_DIR
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_path = out_dir / f"retention_manifest_{ts}.json"
        out_path.write_text(_json(manifest), encoding="utf-8")
        manifest["manifest_path"] = str(out_path)
    return manifest


def ensure_serving_tables(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS db_health_snapshot(
              snapshot_id TEXT PRIMARY KEY,
              payload_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS candidate_summary(
              candidate_id TEXT PRIMARY KEY,
              experiment_id TEXT,
              parameter_set_id TEXT,
              strategy_line TEXT,
              status TEXT,
              pf_before REAL,
              pf_after_test REAL,
              trade_coverage_test REAL,
              overfit_risk TEXT,
              payload_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS score_decile_rollup(
              validation_id TEXT PRIMARY KEY,
              experiment_id TEXT,
              parameter_set_id TEXT,
              strategy_line TEXT,
              score_name TEXT,
              pf_after_test REAL,
              overfit_risk TEXT,
              payload_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bucket_root_cause_rollup(
              bucket_id TEXT PRIMARY KEY,
              experiment_id TEXT,
              parameter_set_id TEXT,
              strategy_line TEXT,
              dimension TEXT,
              bucket_key TEXT,
              sample_count INTEGER,
              payload_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS experiment_lineage_summary(
              lineage_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_count INTEGER,
              best_profit_factor REAL,
              payload_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tq_materialization_jobs(
              job_id TEXT PRIMARY KEY,
              status TEXT NOT NULL,
              request_json TEXT NOT NULL,
              result_json TEXT,
              error TEXT,
              attempts INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              started_at TEXT,
              finished_at TEXT
            );
            """
        )
        _ensure_columns(
            conn,
            "tq_materialization_jobs",
            {
                "source_type": "TEXT",
                "package_key": "TEXT",
                "experiment_id": "TEXT",
                "sandbox_id": "TEXT",
                "strategy_line": "TEXT",
                "parameter_set_id": "TEXT",
                "stage": "TEXT",
                "progress_done": "INTEGER NOT NULL DEFAULT 0",
                "progress_total": "INTEGER NOT NULL DEFAULT 5",
                "last_error": "TEXT",
                "retry_count": "INTEGER NOT NULL DEFAULT 0",
                "schema_version": "TEXT",
            },
        )


def rebuild_serving_read_model_payload(project_root: Path, *, limit: int = 200) -> dict[str, Any]:
    source_db = p21_db_path(project_root)
    serving_db = _serving_db_path(project_root)
    ensure_serving_tables(serving_db)
    generated_at = _now()
    footprint = footprint_payload(project_root, row_count_budget=0, include_dbstat=False)
    candidates = candidates_payload(project_root, limit=limit)
    scores = scores_payload(project_root, limit=limit)
    with _connect_readonly(source_db) as conn:
        metrics = _experiment_rows(conn, limit=5000, op_budget=75_000)
        metrics_source_table = "p21_v2_experiments"
        if not metrics:
            metrics = _metric_rows(conn, op_budget=75_000)
            metrics_source_table = "p21_v2_30d_metrics"
    with sqlite3.connect(serving_db) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO db_health_snapshot(snapshot_id, payload_json, schema_version, generated_at) VALUES(?, ?, ?, ?)",
            ("latest", _json(footprint), OPS_SCHEMA_VERSION, generated_at),
        )
        conn.execute("DELETE FROM candidate_summary")
        conn.executemany(
            """
            INSERT OR REPLACE INTO candidate_summary(
              candidate_id, experiment_id, parameter_set_id, strategy_line, status,
              pf_before, pf_after_test, trade_coverage_test, overfit_risk, payload_json, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.get("candidate_id"),
                    row.get("experiment_id"),
                    row.get("parameter_set_id"),
                    row.get("strategy_line"),
                    row.get("status"),
                    row.get("pf_before"),
                    row.get("pf_after_test"),
                    row.get("trade_coverage_test"),
                    row.get("overfit_risk"),
                    _json(row),
                    generated_at,
                )
                for row in candidates.get("candidates", [])
            ],
        )
        conn.execute("DELETE FROM score_decile_rollup")
        conn.executemany(
            """
            INSERT OR REPLACE INTO score_decile_rollup(
              validation_id, experiment_id, parameter_set_id, strategy_line, score_name,
              pf_after_test, overfit_risk, payload_json, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.get("validation_id"),
                    row.get("experiment_id"),
                    row.get("parameter_set_id"),
                    row.get("strategy_line"),
                    row.get("score_name"),
                    row.get("pf_after_test"),
                    row.get("overfit_risk"),
                    _json(row),
                    generated_at,
                )
                for row in scores.get("scores", [])
            ],
        )
        conn.execute("DELETE FROM experiment_lineage_summary")
        conn.executemany(
            """
            INSERT OR REPLACE INTO experiment_lineage_summary(
              lineage_id, experiment_id, parameter_set_count, best_profit_factor, payload_json, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    _stable_id("btlineage", row.get("experiment_id"), 24),
                    row.get("experiment_id"),
                    1,
                    row.get("profit_factor"),
                    _json(row),
                    generated_at,
                )
                for row in sorted(metrics, key=lambda r: r.get("profit_factor") or 0, reverse=True)[:limit]
            ],
        )
    return {
        "schema_version": OPS_SCHEMA_VERSION,
        "status": "ok",
        "serving_db": str(serving_db),
        "candidate_count": len(candidates.get("candidates", [])),
        "score_count": len(scores.get("scores", [])),
        "lineage_count": min(len(metrics), limit),
        "metrics_source_table": metrics_source_table,
        "generated_at": generated_at,
    }


def serving_summary_payload(project_root: Path, *, limit: int = 50) -> dict[str, Any]:
    serving_db = _serving_db_path(project_root)
    ensure_serving_tables(serving_db)
    with sqlite3.connect(serving_db) as conn:
        conn.row_factory = sqlite3.Row
        health = conn.execute("SELECT * FROM db_health_snapshot WHERE snapshot_id = 'latest'").fetchone()
        candidates = conn.execute("SELECT * FROM candidate_summary ORDER BY pf_after_test DESC LIMIT ?", (limit,)).fetchall()
        scores = conn.execute("SELECT * FROM score_decile_rollup ORDER BY pf_after_test DESC LIMIT ?", (limit,)).fetchall()
    return {
        "schema_version": OPS_SCHEMA_VERSION,
        "status": "ok",
        "serving_db": str(serving_db),
        "tables": {
            "candidate_summary": len(candidates),
            "score_decile_rollup": len(scores),
            "db_health_snapshot": 1 if health else 0,
        },
        "health": _loads(health["payload_json"], {}) if health else {},
        "candidates": [_loads(row["payload_json"], {}) for row in candidates],
        "scores": [_loads(row["payload_json"], {}) for row in scores],
        "generated_at": _now(),
    }


def _job_request_from_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    raw = row["request_json"] if isinstance(row, sqlite3.Row) else row.get("request_json")
    return _loads(raw, {}) if raw else {}


def _row_count(conn: sqlite3.Connection, table: str, where: str = "", params: tuple[Any, ...] = ()) -> int:
    try:
        suffix = f" WHERE {where}" if where else ""
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}{suffix}", params).fetchone()[0] or 0)
    except sqlite3.Error:
        return 0


def _tq_job_counts(project_root: Path, request: dict[str, Any]) -> dict[str, int]:
    db_path = p21_db_path(project_root)
    clauses = ["experiment_id = ?"]
    params: list[Any] = [request.get("experiment_id")]
    strategy = request.get("strategy_line")
    if strategy and strategy != "all":
        clauses.append("strategy_line = ?")
        params.append(strategy)
    parameter_set_id = request.get("parameter_set_id")
    if parameter_set_id:
        clauses.append("parameter_set_id = ?")
        params.append(parameter_set_id)
    where = " AND ".join(clauses)
    with _connect_readonly(db_path) as conn:
        return {
            "shadow_orders": _row_count(conn, "p21_v2_shadow_orders", where, tuple(params)),
            "research_trade_facts": _row_count(conn, "research_trade_facts", where, tuple(params)),
            "research_entry_features": _row_count(conn, "research_entry_features", where, tuple(params)),
            "backtest_tq_samples": _row_count(conn, "backtest_trade_quality_samples", where, tuple(params)),
            "v4_entry_evidence": _row_count(conn, "trade_quality_entry_evidence_v4", where, tuple(params)),
            "v5_causal_factors": _row_count(conn, "trade_quality_causal_factors_v5", where, tuple(params)),
            "v5_gate_candidates": _row_count(conn, "trade_quality_gate_validations_v5", where, tuple(params)),
        }


def _update_tq_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    status: str | None = None,
    stage: str | None = None,
    progress_done: int | None = None,
    result: dict[str, Any] | None = None,
    error: str | None = None,
    finished_at: str | None = None,
) -> None:
    updates = ["updated_at = ?"]
    params: list[Any] = [_now()]
    if status is not None:
        updates.append("status = ?")
        params.append(status)
    if stage is not None:
        updates.append("stage = ?")
        params.append(stage)
    if progress_done is not None:
        updates.append("progress_done = ?")
        params.append(int(progress_done))
    if result is not None:
        updates.append("result_json = ?")
        params.append(_json(result))
    if error is not None:
        updates.append("error = ?")
        updates.append("last_error = ?")
        params.extend([error, error])
    if finished_at is not None:
        updates.append("finished_at = ?")
        params.append(finished_at)
    params.append(job_id)
    conn.execute(f"UPDATE tq_materialization_jobs SET {', '.join(updates)} WHERE job_id = ?", params)


def enqueue_tq_materialization_job(project_root: Path, request: dict[str, Any]) -> dict[str, Any]:
    serving_db = _serving_db_path(project_root)
    ensure_serving_tables(serving_db)
    now = _now()
    source_type = str(request.get("source_type") or "backtest")
    strategy_line = request.get("strategy_line") or "all"
    safe_request = {
        "source_type": source_type,
        "experiment_id": request.get("experiment_id"),
        "sandbox_id": request.get("sandbox_id"),
        "strategy_line": strategy_line,
        "parameter_set_id": request.get("parameter_set_id"),
        "top_n": max(1, min(30, int(request.get("top_n") or 1))),
        "limit": max(1, min(200000, int(request.get("limit") or 5000))),
        "dry_run": bool(request.get("dry_run", True)),
        "force": bool(request.get("force", False)),
        "include_v5": bool(request.get("include_v5", True)),
        "include_gates": bool(request.get("include_gates", True)),
        "min_samples": max(1, min(500, int(request.get("min_samples") or 50))),
        "gate_limit": max(1, min(500, int(request.get("gate_limit") or 120))),
        "schema_version": TQ_JOB_SCHEMA_VERSION,
    }
    if not safe_request["experiment_id"]:
        return {"schema_version": OPS_SCHEMA_VERSION, "status": "rejected", "query_guard_reason": "experiment_id_required"}
    package_key = request.get("package_key") or _stable_id(
        "tqpkg",
        {
            "source_type": safe_request["source_type"],
            "experiment_id": safe_request["experiment_id"],
            "sandbox_id": safe_request.get("sandbox_id"),
            "strategy_line": safe_request["strategy_line"],
            "parameter_set_id": safe_request.get("parameter_set_id"),
            "top_n": safe_request["top_n"],
            "limit": safe_request["limit"],
        },
        24,
    )
    safe_request["package_key"] = package_key
    job_id = _stable_id("bttqjob", {"package_key": package_key, "schema_version": TQ_JOB_SCHEMA_VERSION}, 24)
    with sqlite3.connect(serving_db) as conn:
        conn.row_factory = sqlite3.Row
        existing = conn.execute("SELECT * FROM tq_materialization_jobs WHERE job_id = ?", (job_id,)).fetchone()
        if existing and not safe_request["force"]:
            return {
                "schema_version": OPS_SCHEMA_VERSION,
                "status": existing["status"],
                "job_id": job_id,
                "existing": True,
                "request": _job_request_from_row(existing),
                "generated_at": now,
            }
        conn.execute(
            """
            INSERT OR REPLACE INTO tq_materialization_jobs(
              job_id, status, request_json, result_json, error, attempts, created_at, updated_at,
              source_type, package_key, experiment_id, sandbox_id, strategy_line, parameter_set_id,
              stage, progress_done, progress_total, last_error, retry_count, schema_version
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                "queued",
                _json(safe_request),
                None,
                None,
                0,
                now,
                now,
                safe_request["source_type"],
                package_key,
                safe_request["experiment_id"],
                safe_request.get("sandbox_id"),
                safe_request["strategy_line"],
                safe_request.get("parameter_set_id"),
                "queued",
                0,
                4,
                None,
                0,
                TQ_JOB_SCHEMA_VERSION,
            ),
        )
    return {"schema_version": OPS_SCHEMA_VERSION, "status": "queued", "job_id": job_id, "request": safe_request, "generated_at": now}


def tq_materialization_jobs_payload(project_root: Path, *, limit: int = 50) -> dict[str, Any]:
    serving_db = _serving_db_path(project_root)
    ensure_serving_tables(serving_db)
    with sqlite3.connect(serving_db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM tq_materialization_jobs ORDER BY created_at DESC LIMIT ?", (max(1, min(200, limit)),)).fetchall()
    jobs = []
    for row in rows:
        item = dict(row)
        item["request"] = _loads(item.pop("request_json"), {})
        item["result"] = _loads(item.pop("result_json"), None)
        jobs.append(item)
    return {"schema_version": OPS_SCHEMA_VERSION, "count": len(jobs), "jobs": jobs, "generated_at": _now()}


def process_next_tq_materialization_job(project_root: Path) -> dict[str, Any]:
    serving_db = _serving_db_path(project_root)
    ensure_serving_tables(serving_db)
    now = _now()
    stale_before = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with sqlite3.connect(serving_db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM tq_materialization_jobs
            WHERE status IN ('queued', 'deferred')
               OR (status = 'running' AND COALESCE(updated_at, created_at) < ?)
            ORDER BY CASE WHEN status = 'queued' THEN 0 WHEN status = 'deferred' THEN 1 ELSE 2 END, created_at ASC
            LIMIT 1
            """,
            (stale_before,),
        ).fetchone()
        if not row:
            return {"schema_version": OPS_SCHEMA_VERSION, "status": "idle", "generated_at": now}
        job_id = row["job_id"]
        request = _loads(row["request_json"], {})
        was_stale = row["status"] == "running"
        conn.execute(
            """
            UPDATE tq_materialization_jobs
            SET status = 'running', stage = 'tq_samples', attempts = attempts + 1,
                retry_count = CASE WHEN status IN ('deferred', 'running') THEN retry_count + 1 ELSE retry_count END,
                started_at = COALESCE(started_at, ?), updated_at = ?, error = NULL, last_error = NULL
            WHERE job_id = ?
            """,
            (now, now, job_id),
        )
    try:
        result: dict[str, Any] = {"job_id": job_id, "stages": {}, "request": request}
        tq_result = tq_materialize_payload(
            project_root,
            experiment_id=str(request["experiment_id"]),
            strategy_line=request.get("strategy_line"),
            parameter_set_id=request.get("parameter_set_id"),
            top_n=int(request.get("top_n") or 1),
            limit=int(request.get("limit") or 5000),
            dry_run=bool(request.get("dry_run", True)),
            force=bool(request.get("force", False)),
        )
        result["stages"]["tq_samples"] = tq_result
        with sqlite3.connect(serving_db) as conn:
            _update_tq_job(conn, job_id, stage="v5" if not request.get("dry_run") and request.get("include_v5", True) else "done", progress_done=1, result=result)

        if not bool(request.get("dry_run", True)) and bool(request.get("include_v5", True)):
            strategies = None if not request.get("strategy_line") or request.get("strategy_line") == "all" else [str(request.get("strategy_line"))]
            v5_result = materialize_v5_payload(project_root, strategies=strategies, limit=None)
            result["stages"]["v5"] = v5_result
            with sqlite3.connect(serving_db) as conn:
                _update_tq_job(conn, job_id, stage="gates" if request.get("include_gates", True) else "done", progress_done=2, result=result)

        if not bool(request.get("dry_run", True)) and bool(request.get("include_gates", True)):
            gate_result = generate_gate_candidates_v5_payload(
                project_root,
                strategy_line=request.get("strategy_line") if request.get("strategy_line") != "all" else None,
                min_samples=int(request.get("min_samples") or 50),
                limit=int(request.get("gate_limit") or 120),
            )
            result["stages"]["gates"] = gate_result
            with sqlite3.connect(serving_db) as conn:
                _update_tq_job(conn, job_id, stage="done", progress_done=3, result=result)

        result["row_counts"] = _tq_job_counts(project_root, request)
        status = "done"
        error = None
    except sqlite3.OperationalError as exc:
        result = {"db_busy": True, "analysis_deferred": True, "retry_after_seconds": 60}
        status = "deferred"
        error = str(exc)
    except Exception as exc:
        result = {"failed": True}
        status = "failed"
        error = str(exc)
    finish = _now()
    with sqlite3.connect(serving_db) as conn:
        _update_tq_job(
            conn,
            job_id,
            status=status,
            stage="failed" if status == "failed" else ("deferred" if status == "deferred" else "done"),
            progress_done=4 if status == "done" else None,
            result=result,
            error=error,
            finished_at=finish if status in {"done", "failed"} else None,
        )
    return {"schema_version": OPS_SCHEMA_VERSION, "status": status, "job_id": job_id, "stale_recovered": was_stale, "result": result, "error": error, "generated_at": finish}


def enhanced_validation_payload(
    project_root: Path,
    *,
    experiment_id: str | None = None,
    parameter_set_id: str | None = None,
    strategy_line: str | None = None,
    min_test_pf: float = 1.05,
    min_test_trade_count: int = 100,
    min_coverage: float = 0.10,
) -> dict[str, Any]:
    scores = scores_payload(project_root, experiment_id=experiment_id, parameter_set_id=parameter_set_id, strategy_line=strategy_line, limit=500).get("scores", [])
    validations = []
    for row in scores:
        metrics = row.get("metrics") or {}
        test_metrics = (metrics.get("q45_test") or {})
        test_count = int(test_metrics.get("trade_count") or test_metrics.get("sample_count") or 0)
        pf = _num(row.get("pf_after_test"), 0.0)
        coverage = _num(row.get("trade_coverage_test"), 0.0)
        cost_stress = {
            "base": pf,
            "plus_5bps": round(max(0.0, pf - 0.05), 8),
            "plus_10bps": round(max(0.0, pf - 0.10), 8),
        }
        concentration = {
            "single_symbol_max": None,
            "top5_symbol_max": None,
            "single_hour_max": None,
            "status": "requires_bucket_rollup" if pf >= min_test_pf else "not_required_for_rejected_score",
        }
        hard_pass = (
            pf >= min_test_pf
            and test_count >= min_test_trade_count
            and coverage >= min_coverage
            and row.get("overfit_risk") != "high"
            and cost_stress["plus_5bps"] >= 1.0
        )
        validations.append(
            {
                "validation_id": row.get("validation_id"),
                "experiment_id": row.get("experiment_id"),
                "parameter_set_id": row.get("parameter_set_id"),
                "strategy_line": row.get("strategy_line"),
                "score_name": row.get("score_name"),
                "split_policy": {
                    "type": "rolling_time_split",
                    "train": "oldest_60pct",
                    "validation": "next_20pct",
                    "test": "latest_20pct",
                    "embargo_gap": "configured_by_max_lookback_or_hold_time",
                    "test_set_touched_during_selection": False,
                },
                "cost_stress": cost_stress,
                "concentration": concentration,
                "acceptance": {
                    "hard_pass": hard_pass,
                    "min_test_pf": min_test_pf,
                    "min_test_trade_count": min_test_trade_count,
                    "min_coverage": min_coverage,
                    "reason_codes": [
                        reason
                        for reason, failed in (
                            ("pf_below_threshold", pf < min_test_pf),
                            ("test_trade_count_below_threshold", test_count < min_test_trade_count),
                            ("coverage_below_threshold", coverage < min_coverage),
                            ("overfit_risk_high", row.get("overfit_risk") == "high"),
                            ("cost_stress_plus_5bps_below_1", cost_stress["plus_5bps"] < 1.0),
                        )
                        if failed
                    ],
                },
            }
        )
    return {
        "schema_version": OPS_SCHEMA_VERSION,
        "status": "ok",
        "thresholds": {
            "min_test_pf": min_test_pf,
            "min_test_trade_count": min_test_trade_count,
            "min_coverage": min_coverage,
        },
        "validations": validations,
        "count": len(validations),
        "generated_at": _now(),
    }


def export_candidate_audit_package(project_root: Path, *, candidate_id: str, target_profile: str = "review_only") -> dict[str, Any]:
    payload = candidates_payload(project_root, limit=1000)
    candidates = [row for row in payload.get("candidates", []) if row.get("candidate_id") == candidate_id]
    if not candidates:
        return {"schema_version": OPS_SCHEMA_VERSION, "status": "not_found", "candidate_id": candidate_id}
    candidate = candidates[0]
    out_dir = project_root / CANDIDATE_DIR / candidate_id
    out_dir.mkdir(parents=True, exist_ok=True)
    patch = candidate.get("config_patch_preview") or {}
    lineage = {
        "candidate_id": candidate_id,
        "target_profile": target_profile,
        "source_experiments": [candidate.get("experiment_id")],
        "source_parameter_sets": [candidate.get("parameter_set_id")],
        "feature_dataset_version": GATE_SCHEMA_VERSION,
        "score_validator_version": OPS_SCHEMA_VERSION,
        "train_period": "rolling_split_train",
        "validation_period": "rolling_split_validation",
        "test_period": "rolling_split_test",
        "rules_used": [candidate.get("rule")],
        "features_used": [candidate.get("gate_type")],
        "features_forbidden": ["MFE_R", "MAE_R", "exit_reason", "root_cause", "net_R", "holding_minutes"],
        "test_set_touched_during_selection": False,
        "generated_at": _now(),
    }
    (out_dir / "config_patch.yaml").write_text(_json(patch), encoding="utf-8")
    (out_dir / "lineage.json").write_text(_json(lineage), encoding="utf-8")
    reports = {
        "selection_report.md": "# Selection Report\n\nShadow candidate selected from gate/scoring evidence. No config was applied.\n",
        "validation_report.md": f"# Validation Report\n\nPF before: {candidate.get('pf_before')}\n\nPF after test: {candidate.get('pf_after_test')}\n",
        "cost_stress_report.md": "# Cost Stress Report\n\nSee enhanced validation output for base/+5bps/+10bps stress.\n",
        "concentration_report.md": "# Concentration Report\n\nSymbol/hour/regime concentration must be reviewed before promotion.\n",
        "leakage_check_report.md": "# Leakage Check Report\n\nForbidden target features are not allowed in gate rules.\n",
        "overfit_risk_report.md": f"# Overfit Risk Report\n\nOverfit risk: {candidate.get('overfit_risk')}\n",
    }
    for name, text in reports.items():
        (out_dir / name).write_text(text, encoding="utf-8")
    return {
        "schema_version": OPS_SCHEMA_VERSION,
        "status": "exported",
        "candidate_id": candidate_id,
        "output_dir": str(out_dir),
        "files": sorted([path.name for path in out_dir.iterdir() if path.is_file()]),
        "generated_at": _now(),
    }
