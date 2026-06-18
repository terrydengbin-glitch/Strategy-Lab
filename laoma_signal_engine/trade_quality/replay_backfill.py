"""STEP18.16 historical 1m candle replay backfill for trade quality samples."""

from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

import httpx

from laoma_signal_engine.paper.candles import FUTURES_REST, candle_from_binance_row
from laoma_signal_engine.paper.models import Candle, PaperConfig
from laoma_signal_engine.paper.utils import utc_now_iso
from laoma_signal_engine.trade_quality.archive_backfill import (
    rebuild_quality_rollups,
)
from laoma_signal_engine.trade_quality.engine import (
    LABEL_SCHEMA_VERSION,
    _json,
    _loads,
    _num,
    _parse_iso,
    ensure_trade_quality_tables,
    label_root_cause,
)
from laoma_signal_engine.trade_quality.recommendation_rules import rebuild_recommendation_rules


REPLAY_BACKFILL_SCHEMA_VERSION = "18.16"


class BinanceHistoricalCandleProvider:
    """Small range-aware Binance 1m provider for controlled historical replay."""

    def __init__(self, *, max_candles: int = 1000, timeout: float = 10.0) -> None:
        self.max_candles = max(1, min(int(max_candles or 1000), 1500))
        self.timeout = float(timeout)

    def get_range_1m(self, symbol: str, opened_at: str | None, closed_at: str | None) -> list[Candle]:
        opened = _parse_iso(opened_at)
        closed = _parse_iso(closed_at)
        if opened is None or closed is None or closed < opened:
            return []
        start_ms = int(opened.timestamp() * 1000)
        end_ms = int(closed.timestamp() * 1000)
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.get(
                f"{FUTURES_REST}/fapi/v1/klines",
                params={
                    "symbol": str(symbol).upper(),
                    "interval": "1m",
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": self.max_candles,
                },
            )
            resp.raise_for_status()
            data = resp.json()
        if not isinstance(data, list):
            return []
        return [candle_from_binance_row(symbol, row) for row in data]


def ensure_replay_backfill_tables(db_path: Path) -> None:
    ensure_trade_quality_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_replay_backfill_ledger (
              sample_id TEXT PRIMARY KEY,
              order_id TEXT,
              symbol TEXT,
              strategy_line TEXT,
              side TEXT,
              opened_at TEXT,
              closed_at TEXT,
              replay_status TEXT NOT NULL,
              replay_reason TEXT,
              candle_count INTEGER NOT NULL DEFAULT 0,
              candle_source TEXT,
              old_excursion_model TEXT,
              new_excursion_model TEXT,
              old_root_cause_label TEXT,
              new_root_cause_label TEXT,
              old_MFE_R REAL,
              new_MFE_R REAL,
              old_MAE_R REAL,
              new_MAE_R REAL,
              schema_version TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_quality_replay_status
              ON trade_quality_replay_backfill_ledger(replay_status, updated_at)
            """
        )


def replay_backfill_payload(
    project_root: Path,
    *,
    write: bool = False,
    limit: int | None = None,
    sample_source: str = "all",
    archive_id: str | None = None,
    config: PaperConfig | None = None,
    candle_provider: Any | None = None,
    rebuild_rollups: bool = True,
) -> dict[str, Any]:
    root = project_root.resolve()
    db_path = root / (config.db_path if config else PaperConfig().db_path)
    ensure_replay_backfill_tables(db_path)
    rows = _candidate_rows(db_path, limit=limit, sample_source=sample_source, archive_id=archive_id)
    provider = candle_provider or BinanceHistoricalCandleProvider()
    now = utc_now_iso()
    updates: list[dict[str, Any]] = []
    ledger_rows: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    before_root: Counter[str] = Counter()
    after_root: Counter[str] = Counter()
    before_model: Counter[str] = Counter()
    after_model: Counter[str] = Counter()
    sl_mfe_buckets: Counter[str] = Counter()

    for row in rows:
        before_root[str(row.get("root_cause_label") or "unknown")] += 1
        before_model[str(row.get("excursion_model") or "unknown")] += 1
        result = _replay_row(row, provider)
        status_counts[result["replay_status"]] += 1
        if result["replay_status"] == "updated":
            updates.append(result["sample_update"])
            after_root[str(result["sample_update"].get("root_cause_label") or "unknown")] += 1
            after_model[str(result["sample_update"].get("excursion_model") or "unknown")] += 1
            if str(row.get("exit_reason") or "").upper() == "SL":
                sl_mfe_buckets[_mfe_bucket(result["sample_update"].get("MFE_R"))] += 1
        else:
            after_root[str(row.get("root_cause_label") or "unknown")] += 1
            after_model[str(row.get("excursion_model") or "unknown")] += 1
        ledger_rows.append(
            _ledger_row_from_result(row, result, now)
        )

    rollup: dict[str, Any] | None = None
    rules: dict[str, Any] | None = None
    if write:
        with sqlite3.connect(db_path) as conn:
            for update in updates:
                _update_sample(conn, update)
            for ledger in ledger_rows:
                _upsert_replay_ledger(conn, ledger)
        if rebuild_rollups:
            rollup = rebuild_quality_rollups(db_path)
            rules = rebuild_recommendation_rules(db_path)

    return {
        "schema_version": REPLAY_BACKFILL_SCHEMA_VERSION,
        "mode": "run" if write else "dry_run",
        "db_path": str(db_path),
        "sample_source": _normalize_source(sample_source),
        "archive_id": archive_id,
        "limit": limit,
        "generated_at": now,
        "eligible_samples": len(rows),
        "updated_samples": len(updates) if write else 0,
        "would_update_samples": len(updates),
        "status_counts": dict(status_counts),
        "before_root_cause_counts": dict(before_root),
        "after_root_cause_counts": dict(after_root),
        "before_excursion_model_counts": dict(before_model),
        "after_excursion_model_counts": dict(after_model),
        "sl_replayed_mfe_buckets": dict(sl_mfe_buckets),
        "ledger_rows": ledger_rows[:200],
        "rollup": rollup,
        "recommendation_rules": rules,
    }


def replay_backfill_ledger_rows(db_path: Path, *, limit: int = 200) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    ensure_replay_backfill_tables(db_path)
    safe_limit = max(1, min(int(limit or 200), 1000))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM trade_quality_replay_backfill_ledger
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def replay_backfill_summary(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"ledger_exists": False}
    ensure_replay_backfill_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        status = {
            str(row["replay_status"]): int(row["count"])
            for row in conn.execute(
                "SELECT replay_status, count(*) AS count FROM trade_quality_replay_backfill_ledger GROUP BY replay_status"
            ).fetchall()
        }
        models = {
            str(row["excursion_model"]): int(row["count"])
            for row in conn.execute(
                "SELECT excursion_model, count(*) AS count FROM trade_quality_samples GROUP BY excursion_model"
            ).fetchall()
        }
    return {"ledger_exists": True, "status_counts": status, "excursion_model_counts": models}


def _candidate_rows(
    db_path: Path,
    *,
    limit: int | None,
    sample_source: str,
    archive_id: str | None = None,
) -> list[dict[str, Any]]:
    source = _normalize_source(sample_source)
    clauses = ["COALESCE(excursion_model, '') != 'candle_1m_replay'"]
    params: list[Any] = []
    clauses.append(
        """
        sample_id NOT IN (
          SELECT sample_id
          FROM trade_quality_replay_backfill_ledger
          WHERE replay_status IN (
            'skipped_no_candles',
            'skipped_missing_time',
            'skipped_invalid_time',
            'skipped_missing_risk'
          )
        )
        """
    )
    if source == "archive":
        clauses.append(
            "sample_id IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
        )
        if archive_id:
            clauses.append(
                "sample_id IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE archive_path LIKE ?)"
            )
            params.append(f"%{archive_id}%")
    elif source == "live":
        clauses.append(
            "sample_id NOT IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
        )
    sql = "SELECT * FROM trade_quality_samples WHERE " + " AND ".join(clauses)
    sql += " ORDER BY COALESCE(closed_at, opened_at, generated_at) DESC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(max(0, int(limit)))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _replay_row(row: dict[str, Any], provider: Any) -> dict[str, Any]:
    reason = _eligibility_skip_reason(row)
    if reason:
        return {"replay_status": f"skipped_{reason}", "replay_reason": reason, "candles": []}
    symbol = str(row.get("symbol") or "").upper()
    try:
        candles = _provider_range(provider, symbol, row.get("opened_at"), row.get("closed_at"))
    except Exception as exc:
        return {"replay_status": "skipped_provider_error", "replay_reason": type(exc).__name__, "candles": []}
    if not candles:
        return {"replay_status": "skipped_no_candles", "replay_reason": "no_candles", "candles": []}
    mfe_r, mae_r = _mfe_mae_from_candles(row, candles)
    root, confidence, evidence, secondary, manual = label_root_cause(
        {
            "exit_reason": str(row.get("exit_reason") or "").upper(),
            "net_R": row.get("net_R"),
            "MFE_R": mfe_r,
            "MAE_R": mae_r,
            "planned_RR": row.get("planned_RR"),
            "cost_ratio_R": row.get("cost_ratio_R"),
            "holding_sec": row.get("holding_sec"),
            "excursion_model": "candle_1m_replay",
        }
    )
    old_evidence = _loads(row.get("root_cause_evidence_json"), {})
    evidence = {
        **old_evidence,
        **evidence,
        "replay_schema_version": REPLAY_BACKFILL_SCHEMA_VERSION,
        "replay_candle_count": len(candles),
        "replay_candle_source": _provider_name(provider),
        "old_excursion_model": row.get("excursion_model"),
        "old_root_cause_label": row.get("root_cause_label"),
        "old_MFE_R": row.get("MFE_R"),
        "old_MAE_R": row.get("MAE_R"),
    }
    return {
        "replay_status": "updated",
        "replay_reason": "candle_1m_replay",
        "candles": candles,
        "sample_update": {
            "sample_id": row["sample_id"],
            "MFE_R": mfe_r,
            "MAE_R": mae_r,
            "excursion_model": "candle_1m_replay",
            "root_cause_label": root,
            "root_cause_confidence": confidence,
            "root_cause_evidence_json": _json(evidence),
            "secondary_labels_json": _json(secondary),
            "needs_manual_review": 1 if manual else 0,
            "label_schema_version": LABEL_SCHEMA_VERSION,
            "generated_at": utc_now_iso(),
        },
    }


def _provider_range(provider: Any, symbol: str, opened_at: str | None, closed_at: str | None) -> list[Candle]:
    if hasattr(provider, "get_range_1m"):
        candles = list(provider.get_range_1m(symbol, opened_at, closed_at) or [])
    else:
        candles = list(provider.get_1m(symbol, limit=1000) or [])
    opened = _parse_iso(opened_at)
    closed = _parse_iso(closed_at)
    if opened is None or closed is None:
        return candles
    start_ms = int(opened.timestamp() * 1000)
    end_ms = int(closed.timestamp() * 1000)
    filtered = [c for c in candles if start_ms <= int(c.open_time_ms) <= end_ms]
    return filtered


def _mfe_mae_from_candles(row: dict[str, Any], candles: list[Candle]) -> tuple[float, float]:
    entry = _num(row.get("entry_price"))
    qty = _num(row.get("quantity"))
    initial_risk = _num(row.get("initial_risk_usdt"))
    side = str(row.get("side") or "").upper()
    if initial_risk <= 0 or entry <= 0 or qty <= 0:
        return 0.0, 0.0
    if side == "SHORT":
        favorable = max(max(0.0, entry - _num(c.low)) * qty for c in candles)
        adverse = max(max(0.0, _num(c.high) - entry) * qty for c in candles)
    else:
        favorable = max(max(0.0, _num(c.high) - entry) * qty for c in candles)
        adverse = max(max(0.0, entry - _num(c.low)) * qty for c in candles)
    return favorable / initial_risk, adverse / initial_risk


def _eligibility_skip_reason(row: dict[str, Any]) -> str | None:
    if not row.get("opened_at") or not row.get("closed_at"):
        return "missing_time"
    if _parse_iso(row.get("opened_at")) is None or _parse_iso(row.get("closed_at")) is None:
        return "invalid_time"
    for key in ("symbol", "side", "entry_price", "stop_loss", "take_profit", "quantity", "initial_risk_usdt"):
        if not row.get(key):
            return f"missing_{key}"
    if _num(row.get("initial_risk_usdt")) <= 0:
        return "missing_risk"
    return None


def _ledger_row_from_result(row: dict[str, Any], result: dict[str, Any], now: str) -> dict[str, Any]:
    update = result.get("sample_update") or {}
    return {
        "sample_id": row.get("sample_id"),
        "order_id": row.get("order_id"),
        "symbol": row.get("symbol"),
        "strategy_line": row.get("strategy_line"),
        "side": row.get("side"),
        "opened_at": row.get("opened_at"),
        "closed_at": row.get("closed_at"),
        "replay_status": result.get("replay_status"),
        "replay_reason": result.get("replay_reason"),
        "candle_count": len(result.get("candles") or []),
        "candle_source": "none" if not result.get("candles") else "provider",
        "old_excursion_model": row.get("excursion_model"),
        "new_excursion_model": update.get("excursion_model") or row.get("excursion_model"),
        "old_root_cause_label": row.get("root_cause_label"),
        "new_root_cause_label": update.get("root_cause_label") or row.get("root_cause_label"),
        "old_MFE_R": row.get("MFE_R"),
        "new_MFE_R": update.get("MFE_R", row.get("MFE_R")),
        "old_MAE_R": row.get("MAE_R"),
        "new_MAE_R": update.get("MAE_R", row.get("MAE_R")),
        "schema_version": REPLAY_BACKFILL_SCHEMA_VERSION,
        "updated_at": now,
    }


def _update_sample(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        UPDATE trade_quality_samples
        SET MFE_R=?,
            MAE_R=?,
            excursion_model=?,
            root_cause_label=?,
            root_cause_confidence=?,
            root_cause_evidence_json=?,
            secondary_labels_json=?,
            needs_manual_review=?,
            label_schema_version=?,
            generated_at=?
        WHERE sample_id=?
        """,
        (
            row["MFE_R"],
            row["MAE_R"],
            row["excursion_model"],
            row["root_cause_label"],
            row["root_cause_confidence"],
            row["root_cause_evidence_json"],
            row["secondary_labels_json"],
            row["needs_manual_review"],
            row["label_schema_version"],
            row["generated_at"],
            row["sample_id"],
        ),
    )


def _upsert_replay_ledger(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO trade_quality_replay_backfill_ledger(
          sample_id, order_id, symbol, strategy_line, side, opened_at, closed_at,
          replay_status, replay_reason, candle_count, candle_source,
          old_excursion_model, new_excursion_model, old_root_cause_label, new_root_cause_label,
          old_MFE_R, new_MFE_R, old_MAE_R, new_MAE_R, schema_version, updated_at
        ) VALUES(
          :sample_id, :order_id, :symbol, :strategy_line, :side, :opened_at, :closed_at,
          :replay_status, :replay_reason, :candle_count, :candle_source,
          :old_excursion_model, :new_excursion_model, :old_root_cause_label, :new_root_cause_label,
          :old_MFE_R, :new_MFE_R, :old_MAE_R, :new_MAE_R, :schema_version, :updated_at
        )
        ON CONFLICT(sample_id) DO UPDATE SET
          replay_status=excluded.replay_status,
          replay_reason=excluded.replay_reason,
          candle_count=excluded.candle_count,
          candle_source=excluded.candle_source,
          new_excursion_model=excluded.new_excursion_model,
          new_root_cause_label=excluded.new_root_cause_label,
          new_MFE_R=excluded.new_MFE_R,
          new_MAE_R=excluded.new_MAE_R,
          schema_version=excluded.schema_version,
          updated_at=excluded.updated_at
        """,
        row,
    )


def _mfe_bucket(value: Any) -> str:
    if value is None:
        return "mfe_null"
    got = _num(value)
    if got < 0.3:
        return "mfe_lt_0.3"
    if got < 0.8:
        return "mfe_0.3_0.8"
    return "mfe_gte_0.8"


def _provider_name(provider: Any) -> str:
    return provider.__class__.__name__


def _normalize_source(value: str | None) -> str:
    got = str(value or "all").lower()
    return got if got in {"all", "archive", "live"} else "all"
