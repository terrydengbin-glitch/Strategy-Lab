from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path
from laoma_signal_engine.backtest.p21_trade_quality import (
    materialize_payload,
    package_key,
    summary_payload,
)
from laoma_signal_engine.backtest.p21_v2 import _connect, _loads


DEFAULT_MANIFEST = PROJECT_ROOT / "DATA/backtest/evidence/strategy1_5_retention_manifest_20260611T110224Z.json"
OUT_DIR = PROJECT_ROOT / "DATA/backtest/evidence/without_micro"
REPORT_DIR = PROJECT_ROOT / "docs/reports"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _connect_dest(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS evidence_parameter_sets(
          parameter_set_id TEXT NOT NULL,
          experiment_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          retention_reason TEXT,
          profit_factor REAL,
          expectancy_R REAL,
          trade_count INTEGER,
          win_rate REAL,
          avg_win_R REAL,
          avg_loss_R REAL,
          total_R REAL,
          max_drawdown_R REAL,
          selected_order_count INTEGER,
          materialized_count INTEGER,
          package_key TEXT,
          manifest_json TEXT NOT NULL,
          metrics_json TEXT NOT NULL,
          parameters_json TEXT NOT NULL,
          materialize_json TEXT NOT NULL,
          summary_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          PRIMARY KEY(experiment_id, parameter_set_id)
        );
        CREATE TABLE IF NOT EXISTS daily_metrics(
          metric_id TEXT PRIMARY KEY,
          experiment_id TEXT NOT NULL,
          parameter_set_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          day TEXT NOT NULL,
          payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS symbol_metrics(
          metric_id TEXT PRIMARY KEY,
          experiment_id TEXT NOT NULL,
          parameter_set_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          symbol TEXT NOT NULL,
          payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tq_samples(
          diagnostic_id TEXT PRIMARY KEY,
          package_key TEXT NOT NULL,
          experiment_id TEXT NOT NULL,
          parameter_set_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          order_id TEXT NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          root_cause TEXT,
          exit_reason TEXT,
          net_R REAL,
          MFE_R REAL,
          MAE_R REAL,
          holding_minutes REAL,
          entry_time TEXT,
          exit_time TEXT,
          payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_step21_48_samples_pkg
          ON tq_samples(package_key, root_cause, symbol, side);
        CREATE TABLE IF NOT EXISTS tq_rollups(
          rollup_id TEXT PRIMARY KEY,
          package_key TEXT NOT NULL,
          experiment_id TEXT NOT NULL,
          parameter_set_id TEXT,
          strategy_line TEXT,
          dimension TEXT NOT NULL,
          key TEXT NOT NULL,
          sample_count INTEGER NOT NULL,
          metrics_json TEXT NOT NULL,
          evidence_json TEXT NOT NULL,
          payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS evidence_examples(
          example_id TEXT PRIMARY KEY,
          example_type TEXT NOT NULL,
          package_key TEXT NOT NULL,
          experiment_id TEXT NOT NULL,
          parameter_set_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          order_id TEXT,
          symbol TEXT,
          side TEXT,
          root_cause TEXT,
          net_R REAL,
          MFE_R REAL,
          MAE_R REAL,
          payload_json TEXT NOT NULL
        );
        """
    )
    return conn


def _selected_without_micro(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [row for row in manifest.get("selected_evidence", []) if row.get("strategy_line") == "without_micro"]
    rows.sort(
        key=lambda row: (
            str(row.get("retention_reason") or ""),
            -_num(row.get("profit_factor")),
            str(row.get("parameter_set_id") or ""),
        )
    )
    return rows


def _metric_row(conn: sqlite3.Connection, experiment_id: str, parameter_set_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT *
        FROM p21_v2_30d_metrics
        WHERE experiment_id = ? AND parameter_set_id = ? AND strategy_line = 'without_micro'
        LIMIT 1
        """,
        (experiment_id, parameter_set_id),
    ).fetchone()
    return _row_dict(row)


def _copy_metric_rows(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    table: str,
    experiment_id: str,
    parameter_set_id: str,
) -> int:
    if table == "p21_v2_daily_metrics":
        rows = src.execute(
            """
            SELECT *
            FROM p21_v2_daily_metrics
            WHERE experiment_id = ? AND parameter_set_id = ? AND strategy_line = 'without_micro'
            ORDER BY day
            """,
            (experiment_id, parameter_set_id),
        ).fetchall()
        dst.executemany(
            """
            INSERT OR REPLACE INTO daily_metrics(
              metric_id, experiment_id, parameter_set_id, strategy_line, day, payload_json
            ) VALUES(?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["metric_id"],
                    row["experiment_id"],
                    row["parameter_set_id"],
                    row["strategy_line"],
                    row["day"],
                    _json(dict(row)),
                )
                for row in rows
            ],
        )
        return len(rows)
    rows = src.execute(
        """
        SELECT *
        FROM p21_v2_symbol_metrics
        WHERE experiment_id = ? AND parameter_set_id = ? AND strategy_line = 'without_micro'
        ORDER BY symbol
        """,
        (experiment_id, parameter_set_id),
    ).fetchall()
    dst.executemany(
        """
        INSERT OR REPLACE INTO symbol_metrics(
          metric_id, experiment_id, parameter_set_id, strategy_line, symbol, payload_json
        ) VALUES(?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["metric_id"],
                row["experiment_id"],
                row["parameter_set_id"],
                row["strategy_line"],
                row["symbol"],
                _json(dict(row)),
            )
            for row in rows
        ],
    )
    return len(rows)


def _copy_tq_samples(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    experiment_id: str,
    parameter_set_id: str,
) -> list[dict[str, Any]]:
    rows = src.execute(
        """
        SELECT *
        FROM backtest_trade_quality_samples
        WHERE experiment_id = ? AND parameter_set_id = ? AND strategy_line = 'without_micro'
        ORDER BY rowid
        """,
        (experiment_id, parameter_set_id),
    ).fetchall()
    payloads = [dict(row) for row in rows]
    dst.executemany(
        """
        INSERT OR REPLACE INTO tq_samples(
          diagnostic_id, package_key, experiment_id, parameter_set_id, strategy_line,
          order_id, symbol, side, root_cause, exit_reason, net_R, MFE_R, MAE_R,
          holding_minutes, entry_time, exit_time, payload_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["diagnostic_id"],
                row["package_key"],
                row["experiment_id"],
                row["parameter_set_id"],
                row["strategy_line"],
                row["order_id"],
                row["symbol"],
                row["side"],
                row["root_cause"],
                row["exit_reason"],
                row["net_R"],
                row["MFE_R"],
                row["MAE_R"],
                row["holding_minutes"],
                row["entry_time"],
                row["exit_time"],
                _json(dict(row)),
            )
            for row in rows
        ],
    )
    return payloads


def _copy_tq_rollups(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    experiment_id: str,
    parameter_set_id: str,
) -> int:
    rows = src.execute(
        """
        SELECT *
        FROM backtest_trade_quality_rollups
        WHERE experiment_id = ? AND parameter_set_id = ? AND strategy_line = 'without_micro'
        ORDER BY dimension, sample_count DESC, key
        """,
        (experiment_id, parameter_set_id),
    ).fetchall()
    dst.executemany(
        """
        INSERT OR REPLACE INTO tq_rollups(
          rollup_id, package_key, experiment_id, parameter_set_id, strategy_line,
          dimension, key, sample_count, metrics_json, evidence_json, payload_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["rollup_id"],
                row["package_key"],
                row["experiment_id"],
                row["parameter_set_id"],
                row["strategy_line"],
                row["dimension"],
                row["key"],
                row["sample_count"],
                row["metrics_json"],
                row["evidence_json"],
                _json(dict(row)),
            )
            for row in rows
        ],
    )
    return len(rows)


def _example_rows(samples: list[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    rows: list[tuple[str, dict[str, Any]]] = []
    ordered = sorted(samples, key=lambda row: _num(row.get("net_R")), reverse=True)
    rows.extend(("top_winner", row) for row in ordered[:10])
    rows.extend(("top_loser", row) for row in sorted(samples, key=lambda row: _num(row.get("net_R")))[:10])
    high_mfe_lost = [
        row
        for row in samples
        if _num(row.get("net_R")) <= 0 and _num(row.get("MFE_R")) >= 0.8
    ]
    rows.extend(("high_mfe_lost", row) for row in sorted(high_mfe_lost, key=lambda row: _num(row.get("MFE_R")), reverse=True)[:10])
    immediate_adverse = [
        row
        for row in samples
        if _num(row.get("net_R")) <= 0 and _num(row.get("MAE_R")) >= 0.8 and _num(row.get("MFE_R")) < 0.3
    ]
    rows.extend(("immediate_adverse_loss", row) for row in sorted(immediate_adverse, key=lambda row: _num(row.get("MAE_R")), reverse=True)[:10])
    return rows


def _insert_examples(dst: sqlite3.Connection, package: dict[str, Any], samples: list[dict[str, Any]]) -> int:
    rows = _example_rows(samples)
    dst.executemany(
        """
        INSERT OR REPLACE INTO evidence_examples(
          example_id, example_type, package_key, experiment_id, parameter_set_id,
          strategy_line, order_id, symbol, side, root_cause, net_R, MFE_R, MAE_R, payload_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                hashlib.sha256(
                    f"{kind}:{row.get('diagnostic_id') or row.get('order_id')}".encode("utf-8")
                ).hexdigest()[:24],
                kind,
                str(package["package_key"]),
                str(package["experiment_id"]),
                str(package["parameter_set_id"]),
                "without_micro",
                row.get("order_id"),
                row.get("symbol"),
                row.get("side"),
                row.get("root_cause"),
                row.get("net_R"),
                row.get("MFE_R"),
                row.get("MAE_R"),
                _json(row),
            )
            for kind, row in rows
        ],
    )
    return len(rows)


def _root_cause_compact(summary: dict[str, Any]) -> list[dict[str, Any]]:
    items = (
        summary.get("summary", {})
        .get("root_cause_attribution", {})
        .get("items", [])
    )
    compact = []
    for item in items[:8]:
        key = item.get("key") or item.get("root_cause") or item.get("label") or item.get("name")
        compact.append(
            {
                "root_cause": key,
                "count": item.get("count"),
                "ratio": item.get("ratio"),
                "avg_R": item.get("avg_R") if item.get("avg_R") is not None else item.get("avg_net_R"),
                "loss": item.get("loss") if item.get("loss") is not None else item.get("loss_count"),
            }
        )
    return compact


def run(manifest_path: Path, *, limit_per_package: int, force: bool = False) -> dict[str, Any]:
    manifest = _read_json(manifest_path)
    selected = _selected_without_micro(manifest)
    if not selected:
        raise RuntimeError(f"No without_micro selected_evidence rows in {manifest_path}")

    tag = _now_tag()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    sqlite_path = OUT_DIR / f"without_micro_evidence_pack_{tag}.sqlite"
    json_path = OUT_DIR / f"without_micro_evidence_pack_{tag}.json"
    report_path = REPORT_DIR / f"STEP21.48_without_micro_evidence_pack_{tag}.md"

    p21_db = p21_db_path(PROJECT_ROOT)
    started_at = _now_iso()
    packages: list[dict[str, Any]] = []
    totals = {
        "selected_parameter_sets": len(selected),
        "selected_order_count": 0,
        "materialized_count": 0,
        "exported_samples": 0,
        "exported_rollups": 0,
        "exported_daily_metrics": 0,
        "exported_symbol_metrics": 0,
        "exported_examples": 0,
    }

    with _connect(p21_db) as src, _connect_dest(sqlite_path) as dst:
        src.row_factory = sqlite3.Row
        for item in selected:
            experiment_id = str(item["experiment_id"])
            parameter_set_id = str(item["parameter_set_id"])
            pkg = package_key(experiment_id, "without_micro", parameter_set_id)
            existing_count = int(
                src.execute(
                    """
                    SELECT COUNT(*)
                    FROM backtest_trade_quality_samples
                    WHERE experiment_id = ? AND parameter_set_id = ? AND strategy_line = 'without_micro'
                    """,
                    (experiment_id, parameter_set_id),
                ).fetchone()[0]
            )
            if existing_count >= limit_per_package and not force:
                materialized = {
                    "dry_run": False,
                    "skipped_materialize": True,
                    "reason": "existing_count_ge_limit",
                    "experiment_id": experiment_id,
                    "selected_parameter_sets": [parameter_set_id],
                    "selected_order_count": int(item.get("estimated_shadow_order_count") or item.get("trade_count") or 0),
                    "materialized_count": existing_count,
                    "rollup_count": 0,
                    "package_keys": [pkg],
                    "limit": limit_per_package,
                }
            else:
                materialized = materialize_payload(
                    PROJECT_ROOT,
                    experiment_id=experiment_id,
                    strategy_line="without_micro",
                    parameter_set_id=parameter_set_id,
                    limit=limit_per_package,
                    dry_run=False,
                    force=force,
                )
            metric_row = _metric_row(src, experiment_id, parameter_set_id)
            metrics = _loads(metric_row.get("metrics_json"), {}) if metric_row else {}
            parameters = _loads(metric_row.get("parameters_json"), {}) if metric_row else {}
            summary = summary_payload(
                PROJECT_ROOT,
                experiment_id=experiment_id,
                strategy_line="without_micro",
                parameter_set_id=parameter_set_id,
                limit=limit_per_package,
            )
            samples = _copy_tq_samples(src, dst, experiment_id, parameter_set_id)
            rollup_count = _copy_tq_rollups(src, dst, experiment_id, parameter_set_id)
            daily_count = _copy_metric_rows(src, dst, "p21_v2_daily_metrics", experiment_id, parameter_set_id)
            symbol_count = _copy_metric_rows(src, dst, "p21_v2_symbol_metrics", experiment_id, parameter_set_id)
            package_record = {
                "experiment_id": experiment_id,
                "parameter_set_id": parameter_set_id,
                "strategy_line": "without_micro",
                "retention_reason": item.get("retention_reason"),
                "package_key": pkg,
                "profit_factor": item.get("profit_factor"),
                "expectancy_R": item.get("expectancy_R"),
                "trade_count": item.get("trade_count"),
                "win_rate": item.get("win_rate"),
                "avg_win_R": item.get("avg_win_R"),
                "avg_loss_R": item.get("avg_loss_R"),
                "total_R": item.get("total_R"),
                "max_drawdown_R": item.get("max_drawdown_R"),
                "selected_order_count": materialized.get("selected_order_count"),
                "materialized_count": len(samples),
                "rollup_count": rollup_count,
                "daily_metric_count": daily_count,
                "symbol_metric_count": symbol_count,
                "root_cause_top": _root_cause_compact(summary),
            }
            example_count = _insert_examples(dst, package_record, samples)
            dst.execute(
                """
                INSERT OR REPLACE INTO evidence_parameter_sets(
                  parameter_set_id, experiment_id, strategy_line, retention_reason,
                  profit_factor, expectancy_R, trade_count, win_rate, avg_win_R, avg_loss_R,
                  total_R, max_drawdown_R, selected_order_count, materialized_count, package_key,
                  manifest_json, metrics_json, parameters_json, materialize_json, summary_json, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    parameter_set_id,
                    experiment_id,
                    "without_micro",
                    item.get("retention_reason"),
                    item.get("profit_factor"),
                    item.get("expectancy_R"),
                    item.get("trade_count"),
                    item.get("win_rate"),
                    item.get("avg_win_R"),
                    item.get("avg_loss_R"),
                    item.get("total_R"),
                    item.get("max_drawdown_R"),
                    materialized.get("selected_order_count"),
                    len(samples),
                    pkg,
                    _json(item),
                    _json(metrics),
                    _json(parameters),
                    _json(materialized),
                    _json(summary),
                    started_at,
                ),
            )
            dst.commit()
            packages.append(package_record)
            totals["selected_order_count"] += int(materialized.get("selected_order_count") or 0)
            totals["materialized_count"] += int(materialized.get("materialized_count") or 0)
            totals["exported_samples"] += len(samples)
            totals["exported_rollups"] += rollup_count
            totals["exported_daily_metrics"] += daily_count
            totals["exported_symbol_metrics"] += symbol_count
            totals["exported_examples"] += example_count

    result = {
        "schema_version": "21.48-without-micro-evidence-pack-v1",
        "generated_at": _now_iso(),
        "status": "done",
        "source_manifest": str(manifest_path.relative_to(PROJECT_ROOT)),
        "source_db": str(p21_db.relative_to(PROJECT_ROOT)),
        "boundary": {
            "no_delete_executed": True,
            "no_vacuum_executed": True,
            "strategy_code_changed": False,
            "bounded_limit_per_package": limit_per_package,
        },
        "outputs": {
            "sqlite": str(sqlite_path.relative_to(PROJECT_ROOT)),
            "json": str(json_path.relative_to(PROJECT_ROOT)),
            "report": str(report_path.relative_to(PROJECT_ROOT)),
        },
        "totals": totals,
        "packages": packages,
    }
    _write_json(json_path, result)
    sqlite_hash = _sha256_file(sqlite_path)
    json_hash = _sha256_file(json_path)
    result["outputs"]["sqlite_sha256"] = sqlite_hash
    result["outputs"]["json_sha256"] = json_hash
    _write_json(json_path, result)

    top_lines = sorted(packages, key=lambda row: _num(row.get("profit_factor")), reverse=True)[:8]
    worst_lines = sorted(packages, key=lambda row: _num(row.get("profit_factor")))[:5]
    report = [
        "# STEP21.48 Without-Micro Evidence Pack & Bounded TQ Materialize",
        "",
        f"- generated_at: `{result['generated_at']}`",
        f"- source_manifest: `{result['source_manifest']}`",
        f"- source_db: `{result['source_db']}`",
        f"- evidence_sqlite: `{result['outputs']['sqlite']}`",
        f"- evidence_json: `{result['outputs']['json']}`",
        f"- sqlite_sha256: `{sqlite_hash}`",
        f"- json_sha256: `{json_hash}`",
        "",
        "## Boundary",
        "",
        "- no DELETE executed",
        "- no VACUUM executed",
        "- no strategy / evaluator / runner changes",
        f"- bounded_limit_per_package: `{limit_per_package}`",
        "",
        "## Totals",
        "",
        f"- selected_parameter_sets: `{totals['selected_parameter_sets']}`",
        f"- selected_order_count: `{totals['selected_order_count']}`",
        f"- exported_samples: `{totals['exported_samples']}`",
        f"- exported_rollups: `{totals['exported_rollups']}`",
        f"- exported_daily_metrics: `{totals['exported_daily_metrics']}`",
        f"- exported_symbol_metrics: `{totals['exported_symbol_metrics']}`",
        f"- exported_examples: `{totals['exported_examples']}`",
        "",
        "## Top PF Retained Packages",
        "",
        "| rank | parameter_set_id | reason | PF | expectancy_R | trades | exported_samples |",
        "|---:|---|---|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(top_lines, 1):
        report.append(
            f"| {idx} | `{row['parameter_set_id']}` | {row.get('retention_reason')} | "
            f"{_num(row.get('profit_factor')):.6f} | {_num(row.get('expectancy_R')):.6f} | "
            f"{int(row.get('trade_count') or 0)} | {int(row.get('materialized_count') or 0)} |"
        )
    report.extend(
        [
            "",
            "## Worst PF Retained Packages",
            "",
            "| rank | parameter_set_id | reason | PF | expectancy_R | trades | exported_samples |",
            "|---:|---|---|---:|---:|---:|---:|",
        ]
    )
    for idx, row in enumerate(worst_lines, 1):
        report.append(
            f"| {idx} | `{row['parameter_set_id']}` | {row.get('retention_reason')} | "
            f"{_num(row.get('profit_factor')):.6f} | {_num(row.get('expectancy_R')):.6f} | "
            f"{int(row.get('trade_count') or 0)} | {int(row.get('materialized_count') or 0)} |"
        )
    report.extend(
        [
            "",
            "## Root Cause Preview",
            "",
        ]
    )
    for row in top_lines[:5]:
        compact = ", ".join(
            f"{item.get('root_cause')}:{item.get('count')}"
            for item in row.get("root_cause_top", [])[:5]
        )
        report.append(f"- `{row['parameter_set_id']}`: {compact or 'no TQ samples'}")
    report.extend(
        [
            "",
            "## Cleanup Readiness",
            "",
            "This evidence pack is enough for package-level traceability and bounded Trade Quality analysis.",
            "It is not a full raw shadow-order backup. Any physical prune of the original P21 DB must be handled by a separate delete-approved task.",
        ]
    )
    report_path.write_text("\n".join(report) + "\n", encoding="utf-8")
    result["outputs"]["report_sha256"] = _sha256_file(report_path)
    _write_json(json_path, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="STEP21.48 without_micro evidence pack exporter")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--limit-per-package", type=int, default=5000)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    result = run(args.manifest, limit_per_package=args.limit_per_package, force=args.force)
    print(json.dumps(result["outputs"], ensure_ascii=False, indent=2, sort_keys=True))
    print(json.dumps(result["totals"], ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
