from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path


DEFAULT_MANIFEST = PROJECT_ROOT / "DATA/backtest/evidence/strategy1_5_retention_manifest_20260611T110224Z.json"
EVIDENCE_SQLITE = PROJECT_ROOT / "DATA/backtest/evidence/without_micro/without_micro_evidence_pack_20260612T061438Z.sqlite"
OUT_DIR = PROJECT_ROOT / "DATA/backtest/evidence/without_micro"
REPORT_DIR = PROJECT_ROOT / "docs/reports"

SMALL_TABLES = (
    "backtest_trade_quality_rollups",
    "backtest_trade_quality_samples",
    "p21_v2_daily_metrics",
    "p21_v2_symbol_metrics",
    "p21_v2_30d_metrics",
    "p21_v2_matrix_shards",
)
LARGE_TABLE = "p21_v2_shadow_orders"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _pair_key(row: dict[str, Any] | sqlite3.Row) -> tuple[str, str]:
    return str(row["experiment_id"]), str(row["parameter_set_id"])


def _retained_pairs(manifest: dict[str, Any]) -> set[tuple[str, str]]:
    return {
        (str(row["experiment_id"]), str(row["parameter_set_id"]))
        for row in manifest.get("selected_evidence", [])
        if row.get("strategy_line") == "without_micro"
    }


def _all_without_metric_pairs(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT DISTINCT experiment_id, parameter_set_id
        FROM p21_v2_30d_metrics
        WHERE strategy_line = 'without_micro'
        ORDER BY experiment_id, parameter_set_id
        """
    ).fetchall()
    return [_pair_key(row) for row in rows]


def _count_pair(conn: sqlite3.Connection, table: str, pair: tuple[str, str]) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE strategy_line = 'without_micro'
          AND experiment_id = ?
          AND parameter_set_id = ?
        """,
        pair,
    ).fetchone()
    return int(row[0])


def _sum_counts(conn: sqlite3.Connection, table: str, pairs: list[tuple[str, str]]) -> int:
    return sum(_count_pair(conn, table, pair) for pair in pairs)


def _table_totals_by_pairs(conn: sqlite3.Connection, pairs: list[tuple[str, str]]) -> dict[str, int]:
    totals = {table: _sum_counts(conn, table, pairs) for table in [LARGE_TABLE, *SMALL_TABLES]}
    return totals


def _strategy_small_counts(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for table in ("p21_v2_30d_metrics", "p21_v2_daily_metrics", "p21_v2_symbol_metrics", "p21_v2_matrix_shards"):
        rows = conn.execute(
            f"SELECT strategy_line, COUNT(*) AS rows FROM {table} GROUP BY strategy_line ORDER BY strategy_line"
        ).fetchall()
        result[table] = {str(row["strategy_line"]): int(row["rows"]) for row in rows}
    return result


def _checkpoint(conn: sqlite3.Connection, mode: str = "PASSIVE") -> list[Any]:
    try:
        safe_mode = mode if mode in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"} else "PASSIVE"
        return list(conn.execute(f"PRAGMA wal_checkpoint({safe_mode})").fetchone() or [])
    except sqlite3.DatabaseError as exc:
        return ["checkpoint_failed", str(exc)]


def _free_bytes() -> int:
    return int(shutil.disk_usage(PROJECT_ROOT.anchor or str(PROJECT_ROOT)).free)


def _assert_free_space(min_free_gb: float) -> None:
    min_bytes = int(float(min_free_gb) * 1024 * 1024 * 1024)
    free = _free_bytes()
    if free < min_bytes:
        raise RuntimeError(f"Free disk below safety floor: free={free} min_required={min_bytes}")


def _delete_shadow_pair(
    conn: sqlite3.Connection,
    pair: tuple[str, str],
    batch_size: int,
    *,
    checkpoint_mode: str,
    min_free_gb: float,
) -> int:
    total = 0
    while True:
        _assert_free_space(min_free_gb)
        cur = conn.execute(
            f"""
            DELETE FROM {LARGE_TABLE}
            WHERE rowid IN (
              SELECT rowid
              FROM {LARGE_TABLE}
              WHERE strategy_line = 'without_micro'
                AND experiment_id = ?
                AND parameter_set_id = ?
              LIMIT ?
            )
            """,
            (*pair, batch_size),
        )
        deleted = int(cur.rowcount if cur.rowcount is not None else 0)
        conn.commit()
        _checkpoint(conn, checkpoint_mode)
        total += max(deleted, 0)
        if deleted <= 0:
            break
    return total


def _delete_small_pair(conn: sqlite3.Connection, table: str, pair: tuple[str, str]) -> int:
    cur = conn.execute(
        f"""
        DELETE FROM {table}
        WHERE strategy_line = 'without_micro'
          AND experiment_id = ?
          AND parameter_set_id = ?
        """,
        pair,
    )
    conn.commit()
    return int(cur.rowcount if cur.rowcount is not None else 0)


def run(
    *,
    manifest_path: Path,
    execute: bool,
    batch_size: int,
    progress_every: int,
    checkpoint_mode: str,
    min_free_gb: float,
) -> dict[str, Any]:
    manifest = _read_json(manifest_path)
    retained = _retained_pairs(manifest)
    if len(retained) != 22:
        raise RuntimeError(f"Expected 22 retained without_micro pairs, got {len(retained)}")
    if not EVIDENCE_SQLITE.exists():
        raise RuntimeError(f"STEP21.48 evidence SQLite missing: {EVIDENCE_SQLITE}")

    db_path = p21_db_path(PROJECT_ROOT)
    tag = _now_tag()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUT_DIR / f"without_micro_prune_delete_candidates_{tag}.json"
    report_path = REPORT_DIR / f"STEP21.49_without_micro_prune_delete_candidates_{tag}.md"

    with _connect(db_path) as conn:
        all_pairs = _all_without_metric_pairs(conn)
        delete_pairs = [pair for pair in all_pairs if pair not in retained]
        retained_pairs = [pair for pair in all_pairs if pair in retained]
        before_delete_counts = _table_totals_by_pairs(conn, delete_pairs)
        before_retained_counts = _table_totals_by_pairs(conn, retained_pairs)
        before_small_strategy_counts = _strategy_small_counts(conn)
        before_files = {
            "db_bytes": db_path.stat().st_size if db_path.exists() else 0,
            "wal_bytes": db_path.with_suffix(db_path.suffix + "-wal").stat().st_size
            if db_path.with_suffix(db_path.suffix + "-wal").exists()
            else 0,
            "shm_bytes": db_path.with_suffix(db_path.suffix + "-shm").stat().st_size
            if db_path.with_suffix(db_path.suffix + "-shm").exists()
            else 0,
        }
        deleted_by_table: dict[str, int] = defaultdict(int)
        pair_results: list[dict[str, Any]] = []
        checkpoint_result: list[Any] | None = None
        if execute:
            for index, pair in enumerate(delete_pairs, 1):
                pair_deleted: dict[str, int] = {}
                print(
                    f"[STEP21.49] deleting pair {index}/{len(delete_pairs)} "
                    f"experiment={pair[0]} parameter={pair[1]}",
                    flush=True,
                )
                pair_deleted[LARGE_TABLE] = _delete_shadow_pair(
                    conn,
                    pair,
                    batch_size,
                    checkpoint_mode=checkpoint_mode,
                    min_free_gb=min_free_gb,
                )
                deleted_by_table[LARGE_TABLE] += pair_deleted[LARGE_TABLE]
                for table in SMALL_TABLES:
                    count = _delete_small_pair(conn, table, pair)
                    pair_deleted[table] = count
                    deleted_by_table[table] += count
                if index % max(progress_every, 1) == 0:
                    checkpoint_result = _checkpoint(conn, checkpoint_mode)
                    print(
                        f"[STEP21.49] checkpoint index={index} result={checkpoint_result} "
                        f"shadow_deleted={deleted_by_table[LARGE_TABLE]} free_bytes={_free_bytes()}",
                        flush=True,
                    )
                pair_results.append(
                    {
                        "index": index,
                        "experiment_id": pair[0],
                        "parameter_set_id": pair[1],
                        "deleted": pair_deleted,
                    }
                )
            checkpoint_result = _checkpoint(conn, checkpoint_mode)
        after_delete_counts = _table_totals_by_pairs(conn, delete_pairs)
        after_retained_counts = _table_totals_by_pairs(conn, retained_pairs)
        after_small_strategy_counts = _strategy_small_counts(conn)
        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0]) if execute else "not_run_in_dry_run"

    after_files = {
        "db_bytes": db_path.stat().st_size if db_path.exists() else 0,
        "wal_bytes": db_path.with_suffix(db_path.suffix + "-wal").stat().st_size
        if db_path.with_suffix(db_path.suffix + "-wal").exists()
        else 0,
        "shm_bytes": db_path.with_suffix(db_path.suffix + "-shm").stat().st_size
        if db_path.with_suffix(db_path.suffix + "-shm").exists()
        else 0,
    }
    payload = {
        "schema_version": "21.49-without-micro-prune-v1",
        "generated_at": _now_iso(),
        "mode": "execute" if execute else "dry_run",
        "status": "done",
        "source_db": str(db_path.relative_to(PROJECT_ROOT)),
        "source_manifest": str(manifest_path.relative_to(PROJECT_ROOT)),
        "evidence_sqlite": str(EVIDENCE_SQLITE.relative_to(PROJECT_ROOT)),
        "boundary": {
            "delete_scope": "without_micro_non_retained_pairs_only",
            "vacuum_executed": False,
            "parameter_sets_table_deleted": False,
            "other_strategy_delete_allowed": False,
            "checkpoint_mode": checkpoint_mode,
            "min_free_gb": min_free_gb,
        },
        "pair_counts": {
            "all_without_micro_pairs": len(all_pairs),
            "retained_pairs": len(retained_pairs),
            "delete_candidate_pairs": len(delete_pairs),
        },
        "before_delete_counts": before_delete_counts,
        "after_delete_counts": after_delete_counts,
        "before_retained_counts": before_retained_counts,
        "after_retained_counts": after_retained_counts,
        "deleted_by_table": dict(deleted_by_table),
        "before_small_strategy_counts": before_small_strategy_counts,
        "after_small_strategy_counts": after_small_strategy_counts,
        "file_sizes": {
            "before": before_files,
            "after": after_files,
        },
        "sqlite_quick_check": quick_check,
        "checkpoint_result": checkpoint_result,
        "pair_results": pair_results if execute else [],
    }
    _write_json(json_path, payload)
    lines = [
        "# STEP21.49 Without-Micro Backtest Delete-Candidate Prune",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- mode: `{payload['mode']}`",
        f"- source_db: `{payload['source_db']}`",
        f"- source_manifest: `{payload['source_manifest']}`",
        f"- evidence_sqlite: `{payload['evidence_sqlite']}`",
        f"- manifest_json: `{json_path.relative_to(PROJECT_ROOT)}`",
        "",
        "## Boundary",
        "",
        "- deleted scope: `without_micro` non-retained package pairs only",
        "- retained 22 package pairs preserved",
        "- no strategy5 / strategy6 / strategy4 delete allowed",
        "- no VACUUM executed",
        "- `p21_v2_parameter_sets` not deleted",
        "",
        "## Pair Counts",
        "",
        f"- all_without_micro_pairs: `{len(all_pairs)}`",
        f"- retained_pairs: `{len(retained_pairs)}`",
        f"- delete_candidate_pairs: `{len(delete_pairs)}`",
        "",
        "## Delete Counts",
        "",
        "| table | before candidate rows | deleted rows | after candidate rows | retained rows after |",
        "|---|---:|---:|---:|---:|",
    ]
    for table in [LARGE_TABLE, *SMALL_TABLES]:
        lines.append(
            f"| `{table}` | {before_delete_counts.get(table, 0)} | "
            f"{dict(deleted_by_table).get(table, 0)} | {after_delete_counts.get(table, 0)} | "
            f"{after_retained_counts.get(table, 0)} |"
        )
    lines.extend(
        [
            "",
            "## File Size Note",
            "",
            f"- db_bytes_before: `{before_files['db_bytes']}`",
            f"- db_bytes_after: `{after_files['db_bytes']}`",
            f"- wal_bytes_before: `{before_files['wal_bytes']}`",
            f"- wal_bytes_after: `{after_files['wal_bytes']}`",
            "",
            "SQLite will not physically shrink the main DB without a separate VACUUM / rebuild task.",
            "",
            "## Verification",
            "",
            f"- sqlite_quick_check: `{quick_check}`",
            f"- checkpoint_result: `{checkpoint_result}`",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    payload["report"] = str(report_path.relative_to(PROJECT_ROOT))
    _write_json(json_path, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="STEP21.49 without_micro delete-candidate prune")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--progress-every", type=int, default=10)
    parser.add_argument("--checkpoint-mode", choices=["PASSIVE", "FULL", "RESTART", "TRUNCATE"], default="TRUNCATE")
    parser.add_argument("--min-free-gb", type=float, default=50.0)
    args = parser.parse_args()
    payload = run(
        manifest_path=args.manifest,
        execute=args.execute,
        batch_size=args.batch_size,
        progress_every=args.progress_every,
        checkpoint_mode=args.checkpoint_mode,
        min_free_gb=args.min_free_gb,
    )
    print(json.dumps({k: payload[k] for k in ("mode", "status", "pair_counts", "before_delete_counts", "after_delete_counts", "deleted_by_table", "report") if k in payload}, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
