"""Persistent MicroFactorFrame store for runtime audit and full z continuity."""

from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

from laoma_signal_engine.micro.assembly.models import LatestMicroFeaturesDocument


def default_micro_factor_db(project_root: Path | None = None) -> Path:
    root = Path(project_root).resolve() if project_root else Path.cwd().resolve()
    return root / "DATA" / "audit" / "run_audit.db"


def infer_project_root_from_output(path: Path) -> Path:
    resolved = Path(path).resolve()
    parts = list(resolved.parts)
    for idx, part in enumerate(parts):
        if part.upper() == "DATA" and idx > 0:
            return Path(*parts[:idx])
    return resolved.parent


def init_micro_factor_frame_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            create table if not exists micro_factor_frames (
              strategy_line text not null,
              symbol text not null,
              bucket_ts_sec integer not null,
              generated_at text,
              cvd real,
              ofi real,
              z_cvd real,
              z_ofi real,
              cvd_available integer not null default 0,
              ofi_available integer not null default 0,
              z_cvd_available integer not null default 0,
              z_ofi_available integer not null default 0,
              payload_json text not null,
              primary key(strategy_line, symbol, bucket_ts_sec)
            )
            """
        )
        conn.execute(
            "create index if not exists idx_micro_factor_frames_line_symbol_ts "
            "on micro_factor_frames(strategy_line, symbol, bucket_ts_sec desc)"
        )


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        got = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(got) or math.isinf(got):
        return None
    return got


def _bucket_ts(item: Any) -> int | None:
    q = item.micro_quality
    for raw in (
        q.last_processed_bucket_ts_sec,
        q.reference_bucket_ts_sec,
        q.last_cvd_update_bucket_ts_sec,
        q.last_ofi_update_bucket_ts_sec,
    ):
        if raw is not None:
            return int(raw)
    return None


def ingest_micro_factor_frames(
    doc: LatestMicroFeaturesDocument,
    *,
    db_path: Path,
) -> dict[str, Any]:
    init_micro_factor_frame_db(db_path)
    rows: list[tuple[Any, ...]] = []
    for item in doc.items:
        bucket = _bucket_ts(item)
        if bucket is None:
            continue
        candidates = (
            ("micro_fast", item.micro_fast_15m),
            ("micro_full", item.micro_full_15m or item.micro_15m),
        )
        for line, block in candidates:
            if block is None:
                continue
            cvd = _float_or_none(block.cvd)
            ofi = _float_or_none(block.ofi)
            z_cvd = _float_or_none(block.z_cvd)
            z_ofi = _float_or_none(block.z_ofi)
            payload = {
                "generated_at": doc.generated_at,
                "line": line,
                "symbol": item.symbol,
                "bucket_ts_sec": bucket,
                "cvd": cvd,
                "ofi": ofi,
                "z_cvd": z_cvd,
                "z_ofi": z_ofi,
                "quality_ready": bool(
                    item.micro_fast_quality.ready if line == "micro_fast" and item.micro_fast_quality else item.micro_quality.ready
                ),
            }
            rows.append(
                (
                    line,
                    item.symbol,
                    bucket,
                    doc.generated_at,
                    cvd,
                    ofi,
                    z_cvd,
                    z_ofi,
                    int(cvd is not None),
                    int(ofi is not None),
                    int(z_cvd is not None),
                    int(z_ofi is not None),
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                )
            )
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            insert or replace into micro_factor_frames(
              strategy_line, symbol, bucket_ts_sec, generated_at,
              cvd, ofi, z_cvd, z_ofi,
              cvd_available, ofi_available, z_cvd_available, z_ofi_available,
              payload_json
            ) values(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
    return {"db_path": str(db_path), "inserted_or_replaced": len(rows)}


def recent_factor_frames(
    *,
    db_path: Path,
    strategy_line: str,
    symbol: str,
    limit: int = 200,
) -> list[dict[str, Any]]:
    if not db_path.is_file():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            select * from micro_factor_frames
            where strategy_line=? and symbol=?
            order by bucket_ts_sec desc
            limit ?
            """,
            (strategy_line, symbol.strip().upper(), int(limit)),
        ).fetchall()
    return [dict(row) for row in rows][::-1]


def rolling_z_from_store(
    *,
    db_path: Path,
    strategy_line: str,
    symbol: str,
    field: str,
    window: int,
) -> dict[str, Any]:
    if field not in {"cvd", "ofi"}:
        raise ValueError("field must be 'cvd' or 'ofi'")
    rows = recent_factor_frames(
        db_path=db_path,
        strategy_line=strategy_line,
        symbol=symbol,
        limit=max(1, int(window)),
    )
    values = [_float_or_none(row.get(field)) for row in rows]
    clean = [v for v in values if v is not None]
    required = max(2, int(window))
    if len(clean) < required:
        return {
            "available": False,
            "field": field,
            "series_length": len(clean),
            "required_length": required,
            "missing_reason": "insufficient_history",
            "z": None,
        }
    current = clean[-1]
    mean = sum(clean) / len(clean)
    variance = sum((v - mean) ** 2 for v in clean) / max(1, len(clean) - 1)
    std = math.sqrt(variance)
    z = 0.0 if std <= 1e-12 else (current - mean) / std
    return {
        "available": True,
        "field": field,
        "series_length": len(clean),
        "required_length": required,
        "missing_reason": None,
        "z": z,
    }


def full_z_window_from_store(
    *,
    db_path: Path,
    strategy_line: str,
    symbol: str,
    now_bucket_ts_sec: int | None = None,
    window_sec: int = 900,
    min_valid_bucket_ratio: float = 0.7,
    max_gap_sec: int = 15,
) -> dict[str, Any]:
    """Explain whether persisted CVD/OFI buckets can support a full z-window."""

    expected = max(2, int(window_sec or 0))
    if not db_path.is_file():
        return _missing_full_z_window("store_missing", expected)

    store_rows_total = 0
    try:
        with sqlite3.connect(db_path) as conn:
            total_row = conn.execute(
                """
                select count(*) from micro_factor_frames
                where strategy_line=? and symbol=?
                """,
                (strategy_line, symbol.strip().upper()),
            ).fetchone()
            store_rows_total = int(total_row[0] or 0) if total_row else 0

            if now_bucket_ts_sec is None:
                row = conn.execute(
                    """
                    select max(bucket_ts_sec) from micro_factor_frames
                    where strategy_line=? and symbol=?
                    """,
                    (strategy_line, symbol.strip().upper()),
                ).fetchone()
                now_bucket_ts_sec = int(row[0]) if row and row[0] is not None else None

            if now_bucket_ts_sec is None:
                out = _missing_full_z_window("series_not_persisted", expected)
                out["store_rows_total"] = store_rows_total
                out["store_rows_in_window"] = 0
                out["reader_rows_loaded"] = 0
                out["eligible_rows"] = 0
                out["rejected_rows"] = 0
                out["reject_reason_counts"] = {}
                return out

            start_ts = int(now_bucket_ts_sec) - expected + 1
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                select bucket_ts_sec, cvd, ofi, z_cvd, z_ofi,
                       cvd_available, ofi_available,
                       z_cvd_available, z_ofi_available
                from micro_factor_frames
                where strategy_line=?
                  and symbol=?
                  and bucket_ts_sec between ? and ?
                order by bucket_ts_sec asc
                """,
                (strategy_line, symbol.strip().upper(), start_ts, int(now_bucket_ts_sec)),
            ).fetchall()
    except sqlite3.Error as exc:
        out = _missing_full_z_window("store_read_failed", expected)
        out["store_error"] = str(exc)
        out["store_rows_total"] = store_rows_total
        return out

    actual = len(rows)
    reject_reason_counts: Counter[str] = Counter()
    for row in rows:
        cvd_ok = row["cvd"] is not None and int(row["cvd_available"] or 0) == 1
        ofi_ok = row["ofi"] is not None and int(row["ofi_available"] or 0) == 1
        if not cvd_ok:
            reject_reason_counts["cvd_missing_or_unavailable"] += 1
        if not ofi_ok:
            reject_reason_counts["ofi_missing_or_unavailable"] += 1
        if row["z_cvd"] is None or int(row["z_cvd_available"] or 0) != 1:
            reject_reason_counts["z_cvd_missing_or_unavailable"] += 1
        if row["z_ofi"] is None or int(row["z_ofi_available"] or 0) != 1:
            reject_reason_counts["z_ofi_missing_or_unavailable"] += 1

    if actual < 2:
        out = _missing_full_z_window("insufficient_history", expected)
        out.update(
            {
                "now_bucket_ts_sec": now_bucket_ts_sec,
                "store_rows_total": store_rows_total,
                "store_rows_in_window": actual,
                "reader_rows_loaded": actual,
                "actual_bucket_count": actual,
                "valid_bucket_count": actual,
                "valid_bucket_ratio": float(actual / expected),
                "eligible_rows": actual,
                "rejected_rows": 0,
                "reject_reason_counts": dict(reject_reason_counts),
            }
        )
        return out

    buckets = [int(row["bucket_ts_sec"]) for row in rows]
    gaps = [buckets[idx] - buckets[idx - 1] for idx in range(1, len(buckets))]
    observed_max_gap = max(gaps) if gaps else 0
    cvd_values = [
        float(row["cvd"])
        for row in rows
        if row["cvd"] is not None and int(row["cvd_available"] or 0) == 1
    ]
    ofi_values = [
        float(row["ofi"])
        for row in rows
        if row["ofi"] is not None and int(row["ofi_available"] or 0) == 1
    ]
    z_cvd_values = [
        float(row["z_cvd"])
        for row in rows
        if row["z_cvd"] is not None and int(row["z_cvd_available"] or 0) == 1
    ]
    z_ofi_values = [
        float(row["z_ofi"])
        for row in rows
        if row["z_ofi"] is not None and int(row["z_ofi_available"] or 0) == 1
    ]

    valid_bucket_count = min(len(cvd_values), len(ofi_values))
    valid_ratio = float(valid_bucket_count / expected)
    cvd_ratio = float(len(cvd_values) / expected)
    ofi_ratio = float(len(ofi_values) / expected)
    z_cvd = _series_z(cvd_values)
    z_ofi = _series_z(ofi_values)

    missing_reason = None
    if valid_ratio < float(min_valid_bucket_ratio):
        missing_reason = "valid_bucket_ratio_low"
    elif observed_max_gap > int(max_gap_sec):
        missing_reason = "bucket_gap"
    elif len(cvd_values) < 2:
        missing_reason = "cvd_valid_ratio_low"
    elif len(ofi_values) < 2:
        missing_reason = "ofi_valid_ratio_low"
    elif z_cvd is None or z_ofi is None:
        missing_reason = "zero_variance"

    rejected_rows = max(0, actual - valid_bucket_count)
    return {
        "store_read_status": "ok",
        "full_z_status": "available" if missing_reason is None else "missing",
        "full_z_missing_reason": missing_reason,
        "trace_status": "pass" if missing_reason is None else "blocked",
        "strategy_line": strategy_line,
        "symbol": symbol.strip().upper(),
        "now_bucket_ts_sec": int(now_bucket_ts_sec),
        "start_bucket_ts_sec": int(now_bucket_ts_sec) - expected + 1,
        "window_sec": expected,
        "store_rows_total": store_rows_total,
        "store_rows_in_window": actual,
        "reader_rows_loaded": actual,
        "eligible_rows": valid_bucket_count,
        "rejected_rows": rejected_rows,
        "reject_reason_counts": dict(reject_reason_counts),
        "expected_bucket_count": expected,
        "actual_bucket_count": actual,
        "valid_bucket_count": valid_bucket_count,
        "valid_bucket_ratio": valid_ratio,
        "cvd_valid_count": len(cvd_values),
        "ofi_valid_count": len(ofi_values),
        "cvd_valid_ratio": cvd_ratio,
        "ofi_valid_ratio": ofi_ratio,
        "max_gap_sec": observed_max_gap,
        "gap_limit_sec": int(max_gap_sec),
        "z_cvd_available": z_cvd is not None,
        "z_ofi_available": z_ofi is not None,
        "persisted_z_cvd_available": bool(z_cvd_values),
        "persisted_z_ofi_available": bool(z_ofi_values),
        "z_cvd": z_cvd,
        "z_ofi": z_ofi,
        "latest_cvd": cvd_values[-1] if cvd_values else None,
        "latest_ofi": ofi_values[-1] if ofi_values else None,
    }


def _missing_full_z_window(reason: str, expected_bucket_count: int) -> dict[str, Any]:
    return {
        "store_read_status": "missing" if reason == "store_missing" else "ok",
        "full_z_status": "missing",
        "full_z_missing_reason": reason,
        "trace_status": "blocked",
        "store_rows_total": 0,
        "store_rows_in_window": 0,
        "reader_rows_loaded": 0,
        "eligible_rows": 0,
        "rejected_rows": 0,
        "reject_reason_counts": {},
        "expected_bucket_count": int(expected_bucket_count),
        "actual_bucket_count": 0,
        "valid_bucket_count": 0,
        "valid_bucket_ratio": 0.0,
        "cvd_valid_count": 0,
        "ofi_valid_count": 0,
        "cvd_valid_ratio": 0.0,
        "ofi_valid_ratio": 0.0,
        "max_gap_sec": None,
        "z_cvd_available": False,
        "z_ofi_available": False,
        "persisted_z_cvd_available": False,
        "persisted_z_ofi_available": False,
        "z_cvd": None,
        "z_ofi": None,
    }


def _series_z(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    avg = sum(values) / len(values)
    variance = sum((value - avg) ** 2 for value in values) / max(1, len(values) - 1)
    std = math.sqrt(variance)
    if std <= 1e-12:
        return None
    return float((values[-1] - avg) / std)
