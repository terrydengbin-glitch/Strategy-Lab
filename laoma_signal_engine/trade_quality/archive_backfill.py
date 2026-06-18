from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import replace
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.models import PaperConfig
from laoma_signal_engine.paper.utils import utc_now_iso
from laoma_signal_engine.trade_quality.engine import (
    AGG_SCHEMA_VERSION,
    LABEL_SCHEMA_VERSION,
    SAMPLE_SCHEMA_VERSION,
    TradeQualityAnalyzer,
    TradeQualitySample,
    _json,
    _loads,
    build_aggregates,
    build_recommendations,
    ensure_trade_quality_tables,
)


ARCHIVE_LEDGER_SCHEMA_VERSION = "18.9"


class _NoopCandleProvider:
    def get_range_1m(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []


def ensure_archive_ingest_tables(db_path: Path) -> None:
    ensure_trade_quality_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_archive_ingest_ledger (
              dedup_key TEXT PRIMARY KEY,
              sample_id TEXT,
              order_id TEXT,
              source_plan_hash TEXT,
              source_run_id TEXT,
              source_cycle_id TEXT,
              strategy_line TEXT,
              symbol TEXT,
              side TEXT,
              archive_path TEXT NOT NULL,
              archive_mtime REAL,
              archive_hash TEXT,
              ingest_status TEXT NOT NULL,
              skip_reason TEXT,
              schema_version TEXT NOT NULL,
              ingested_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_quality_archive_ingest_sample
              ON trade_quality_archive_ingest_ledger(sample_id, ingest_status)
            """
        )


def archive_dedup_key(order: dict[str, Any], archive_path: Path | str) -> str:
    source_plan_hash = str(order.get("source_plan_hash") or "").strip()
    order_id = str(order.get("id") or order.get("order_id") or "").strip()
    symbol = str(order.get("symbol") or "").strip().upper()
    side = str(order.get("side") or "").strip().upper()
    run_id = str(order.get("source_run_id") or "").strip()
    if source_plan_hash and order_id:
        raw = f"plan_order|{source_plan_hash}|{order_id}"
    elif source_plan_hash and symbol and side and run_id:
        raw = f"plan_symbol_run|{source_plan_hash}|{symbol}|{side}|{run_id}"
    elif order_id:
        raw = f"order|{order_id}"
    else:
        raw = "|".join(
            [
                "fallback",
                str(order.get("strategy_line") or ""),
                symbol,
                side,
                str(order.get("opened_at") or ""),
                str(order.get("closed_at") or ""),
                str(order.get("entry_price") or order.get("filled_entry_price") or ""),
                str(order.get("stop_loss") or ""),
                str(order.get("take_profit") or ""),
                str(archive_path),
            ]
        )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def archive_sample_id(dedup_key: str) -> str:
    return hashlib.sha256(f"archive|{dedup_key}|{SAMPLE_SCHEMA_VERSION}".encode("utf-8")).hexdigest()[:24]


def archive_backfill_payload(
    project_root: Path,
    *,
    write: bool = False,
    limit: int | None = None,
    archive_root: Path | None = None,
    config: PaperConfig | None = None,
) -> dict[str, Any]:
    root = project_root.resolve()
    db_path = root / (config.db_path if config else PaperConfig().db_path)
    source_root = archive_root or (root / "DATA" / "paper" / "archives")
    ensure_archive_ingest_tables(db_path)
    result = _scan_archives(root, source_root, limit=limit)
    result.update(
        {
            "schema_version": ARCHIVE_LEDGER_SCHEMA_VERSION,
            "mode": "run" if write else "dry_run",
            "db_path": str(db_path),
            "archive_root": str(source_root),
            "generated_at": utc_now_iso(),
        }
    )
    if not write:
        result["samples_inserted"] = 0
        return result
    _persist_archive_scan(db_path, result)
    return result


def ingest_ledger_rows(db_path: Path, *, limit: int = 200) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    ensure_archive_ingest_tables(db_path)
    safe_limit = max(1, min(int(limit or 200), 1000))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM trade_quality_archive_ingest_ledger
            ORDER BY ingested_at DESC, archive_path DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def enrich_sample_sources(db_path: Path, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows or not db_path.exists():
        return rows
    ensure_archive_ingest_tables(db_path)
    sample_ids = [str(row.get("sample_id") or "") for row in rows if row.get("sample_id")]
    if not sample_ids:
        return rows
    placeholders = ",".join("?" for _ in sample_ids)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        ledger_rows = conn.execute(
            f"""
            SELECT sample_id, dedup_key, archive_path, schema_version
            FROM trade_quality_archive_ingest_ledger
            WHERE ingest_status='inserted' AND sample_id IN ({placeholders})
            """,
            sample_ids,
        ).fetchall()
    by_sample = {str(row["sample_id"]): dict(row) for row in ledger_rows}
    enriched: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        ledger = by_sample.get(str(item.get("sample_id") or ""))
        item["sample_source"] = "archive" if ledger else "live"
        if ledger:
            item["archive_path"] = ledger.get("archive_path")
            item["archive_dedup_key"] = ledger.get("dedup_key")
            item["archive_schema_version"] = ledger.get("schema_version")
        enriched.append(item)
    return enriched


def archive_ingest_summary(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"ledger_exists": False}
    ensure_archive_ingest_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        status = {
            str(row["ingest_status"]): int(row["count"])
            for row in conn.execute(
                "SELECT ingest_status, count(*) AS count FROM trade_quality_archive_ingest_ledger GROUP BY ingest_status"
            ).fetchall()
        }
        sample_sources = {
            "archive": int(
                conn.execute(
                    "SELECT count(DISTINCT sample_id) FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted'"
                ).fetchone()[0]
                or 0
            ),
            "total": int(conn.execute("SELECT count(*) FROM trade_quality_samples").fetchone()[0] or 0),
        }
        sample_sources["live"] = max(0, sample_sources["total"] - sample_sources["archive"])
    return {"ledger_exists": True, "status_counts": status, "sample_sources": sample_sources}


def load_samples_from_db(db_path: Path, *, sample_source: str = "all") -> list[TradeQualitySample]:
    if not db_path.exists():
        return []
    ensure_archive_ingest_tables(db_path)
    source = _normalize_sample_source(sample_source)
    where = ""
    if source == "archive":
        where = "WHERE sample_id IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
    elif source == "live":
        where = "WHERE sample_id NOT IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(f"SELECT * FROM trade_quality_samples {where}").fetchall()
    return [_sample_from_db_row(dict(row)) for row in rows]


def rebuild_quality_rollups(db_path: Path) -> dict[str, Any]:
    samples = load_samples_from_db(db_path, sample_source="all")
    aggregates = build_aggregates(samples)
    recommendations = build_recommendations(samples, aggregates)
    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM trade_quality_aggregates")
        for item in aggregates:
            conn.execute(
                """
                INSERT INTO trade_quality_aggregates(
                  aggregate_id, dimension, key, sample_count, total_R, avg_R, win_rate,
                  total_net_pnl_usdt, evidence_json, schema_version, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["aggregate_id"],
                    item["dimension"],
                    item["key"],
                    item["sample_count"],
                    item["total_R"],
                    item.get("avg_R"),
                    item.get("win_rate"),
                    item["total_net_pnl_usdt"],
                    _json(item.get("evidence") or {}),
                    AGG_SCHEMA_VERSION,
                    now,
                ),
            )
        conn.execute("DELETE FROM trade_quality_recommendations")
        for item in recommendations:
            conn.execute(
                """
                INSERT INTO trade_quality_recommendations(
                  action_id, priority, problem, evidence_json, affected_scope,
                  suggested_change, expected_effect, requires_followup_task,
                  schema_version, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["action_id"],
                    item["priority"],
                    item["problem"],
                    _json(item.get("evidence") or {}),
                    item["affected_scope"],
                    item["suggested_change"],
                    item["expected_effect"],
                    1 if item.get("requires_followup_task") else 0,
                    AGG_SCHEMA_VERSION,
                    now,
                ),
            )
    return {"sample_count": len(samples), "aggregate_count": len(aggregates), "recommendation_count": len(recommendations)}


def _scan_archives(project_root: Path, archive_root: Path, *, limit: int | None = None) -> dict[str, Any]:
    analyzer = TradeQualityAnalyzer(project_root, candle_provider=_NoopCandleProvider())
    archive_dirs = sorted(path for path in archive_root.glob("paper_exp_*") if path.is_dir()) if archive_root.exists() else []
    if limit is not None:
        archive_dirs = archive_dirs[: max(0, int(limit))]
    counters: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    version_counts: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    ledger_rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    duplicate_keys: set[str] = set()
    for archive_dir in archive_dirs:
        counters["archive_dirs_scanned"] += 1
        metadata = _read_json_obj(archive_dir / "metadata.json", {})
        orders = _read_json_obj(archive_dir / "orders.json", [])
        fills = _read_json_obj(archive_dir / "fills.json", [])
        if isinstance(orders, dict):
            orders = orders.get("orders") or orders.get("rows") or []
        if isinstance(fills, dict):
            fills = fills.get("fills") or fills.get("rows") or []
        if not isinstance(orders, list):
            orders = []
            reason_counts["orders_not_list"] += 1
        if not isinstance(fills, list):
            fills = []
            reason_counts["fills_not_list"] += 1
        counters["archive_files_scanned"] += int((archive_dir / "orders.json").exists()) + int((archive_dir / "fills.json").exists())
        counters["orders_seen"] += len(orders)
        counters["fills_seen"] += len(fills)
        fills_by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for fill in fills:
            if isinstance(fill, dict):
                fills_by_order[str(fill.get("order_id") or "")].append(fill)
        archive_hash = _archive_hash(archive_dir)
        archive_version = _archive_version(metadata)
        version_counts[archive_version] += len(orders)
        for raw_order in orders:
            if not isinstance(raw_order, dict):
                counters["samples_skipped"] += 1
                reason_counts["order_not_object"] += 1
                continue
            order = dict(raw_order)
            dedup_key = archive_dedup_key(order, archive_dir)
            if dedup_key in seen_keys:
                counters["duplicates_skipped"] += 1
                duplicate_keys.add(dedup_key)
                continue
            seen_keys.add(dedup_key)
            skip_reason = _skip_reason(order)
            ledger = _ledger_row(
                dedup_key,
                archive_dir,
                archive_hash,
                order,
                "skipped" if skip_reason else "candidate",
                skip_reason,
            )
            if skip_reason:
                counters["samples_skipped"] += 1
                counters["partial_records"] += 1
                reason_counts[skip_reason] += 1
                ledger_rows.append(ledger)
                continue
            counters["closed_orders_seen"] += 1
            try:
                sample = analyzer._sample_from_order(order, fills_by_order.get(str(order.get("id") or ""), []))
            except Exception:
                counters["samples_skipped"] += 1
                reason_counts["sample_build_error"] += 1
                ledger["ingest_status"] = "skipped"
                ledger["skip_reason"] = "sample_build_error"
                ledger_rows.append(ledger)
                continue
            sample_id = archive_sample_id(dedup_key)
            evidence = {
                **(sample.root_cause_evidence or {}),
                "sample_source": "archive",
                "archive_path": str(archive_dir),
                "archive_schema_version": str(metadata.get("schema_version") or "unknown"),
                "config_profile": str(metadata.get("profile_name") or "unknown"),
                "paper_fill_model_version": _fill_model_version(order, fills_by_order.get(str(order.get("id") or ""), [])),
                "trade_plan_model_version": _trade_plan_model_version(archive_dir, order),
                "dedup_key": dedup_key,
            }
            sample = replace(sample, sample_id=sample_id, root_cause_evidence=evidence)
            row = sample.as_row()
            row["sample_schema_version"] = SAMPLE_SCHEMA_VERSION
            row["label_schema_version"] = LABEL_SCHEMA_VERSION
            row["generated_at"] = utc_now_iso()
            samples.append(row)
            ledger["sample_id"] = sample_id
            ledger["ingest_status"] = "inserted"
            ledger_rows.append(ledger)
            counters["samples_inserted"] += 1
            reason_counts[f"root_cause:{sample.root_cause_label}"] += 1
    return {
        **dict(counters),
        "duplicates_in_scan": len(duplicate_keys),
        "reason_counts": dict(sorted(reason_counts.items())),
        "version_counts": dict(sorted(version_counts.items())),
        "samples": samples,
        "ledger_rows": ledger_rows,
    }


def _persist_archive_scan(db_path: Path, result: dict[str, Any]) -> None:
    ensure_archive_ingest_tables(db_path)
    samples = list(result.get("samples") or [])
    ledger_rows = list(result.get("ledger_rows") or [])
    inserted = 0
    duplicates = 0
    with sqlite3.connect(db_path) as conn:
        existing = {
            str(row[0])
            for row in conn.execute(
                "SELECT dedup_key FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted'"
            ).fetchall()
        }
        for ledger in ledger_rows:
            dedup_key = str(ledger.get("dedup_key") or "")
            if ledger.get("ingest_status") == "inserted" and dedup_key in existing:
                duplicates += 1
                continue
            if ledger.get("ingest_status") == "inserted":
                sample = next((row for row in samples if row.get("sample_id") == ledger.get("sample_id")), None)
                if sample:
                    _insert_sample(conn, sample)
                    inserted += 1
                    existing.add(dedup_key)
            _upsert_ledger(conn, ledger)
    rollup = rebuild_quality_rollups(db_path)
    result["samples_inserted"] = inserted
    result["duplicates_skipped"] = int(result.get("duplicates_skipped") or 0) + duplicates
    result["rollup"] = rollup
    result.pop("samples", None)
    result.pop("ledger_rows", None)


def _insert_sample(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO trade_quality_samples(
          sample_id, order_id, strategy_line, symbol, side, source_run_id, source_cycle_id,
          source_plan_hash, opened_at, closed_at, exit_reason, entry_price, exit_price,
          stop_loss, take_profit, quantity, initial_risk_usdt, gross_pnl_usdt, net_pnl_usdt,
          fee_usdt, slippage_usdt, cost_ratio_R, planned_RR, net_R, MFE_R, MAE_R,
          holding_sec, holding_bucket, excursion_model, root_cause_label,
          root_cause_confidence, root_cause_evidence_json, secondary_labels_json,
          needs_manual_review, sample_schema_version, label_schema_version, generated_at
        ) VALUES(
          :sample_id, :order_id, :strategy_line, :symbol, :side, :source_run_id, :source_cycle_id,
          :source_plan_hash, :opened_at, :closed_at, :exit_reason, :entry_price, :exit_price,
          :stop_loss, :take_profit, :quantity, :initial_risk_usdt, :gross_pnl_usdt, :net_pnl_usdt,
          :fee_usdt, :slippage_usdt, :cost_ratio_R, :planned_RR, :net_R, :MFE_R, :MAE_R,
          :holding_sec, :holding_bucket, :excursion_model, :root_cause_label,
          :root_cause_confidence, :root_cause_evidence_json, :secondary_labels_json,
          :needs_manual_review, :sample_schema_version, :label_schema_version, :generated_at
        )
        ON CONFLICT(sample_id) DO UPDATE SET
          net_R=excluded.net_R,
          MFE_R=excluded.MFE_R,
          MAE_R=excluded.MAE_R,
          root_cause_label=excluded.root_cause_label,
          root_cause_confidence=excluded.root_cause_confidence,
          root_cause_evidence_json=excluded.root_cause_evidence_json,
          secondary_labels_json=excluded.secondary_labels_json,
          generated_at=excluded.generated_at
        """,
        row,
    )


def _upsert_ledger(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO trade_quality_archive_ingest_ledger(
          dedup_key, sample_id, order_id, source_plan_hash, source_run_id, source_cycle_id,
          strategy_line, symbol, side, archive_path, archive_mtime, archive_hash,
          ingest_status, skip_reason, schema_version, ingested_at
        ) VALUES(
          :dedup_key, :sample_id, :order_id, :source_plan_hash, :source_run_id, :source_cycle_id,
          :strategy_line, :symbol, :side, :archive_path, :archive_mtime, :archive_hash,
          :ingest_status, :skip_reason, :schema_version, :ingested_at
        )
        ON CONFLICT(dedup_key) DO UPDATE SET
          sample_id=excluded.sample_id,
          ingest_status=excluded.ingest_status,
          skip_reason=excluded.skip_reason,
          ingested_at=excluded.ingested_at
        """,
        row,
    )


def _ledger_row(
    dedup_key: str,
    archive_dir: Path,
    archive_hash: str,
    order: dict[str, Any],
    status: str,
    skip_reason: str | None,
) -> dict[str, Any]:
    return {
        "dedup_key": dedup_key,
        "sample_id": None,
        "order_id": str(order.get("id") or order.get("order_id") or ""),
        "source_plan_hash": order.get("source_plan_hash"),
        "source_run_id": order.get("source_run_id"),
        "source_cycle_id": order.get("source_cycle_id"),
        "strategy_line": order.get("strategy_line"),
        "symbol": order.get("symbol"),
        "side": order.get("side"),
        "archive_path": str(archive_dir),
        "archive_mtime": archive_dir.stat().st_mtime if archive_dir.exists() else None,
        "archive_hash": archive_hash,
        "ingest_status": status,
        "skip_reason": skip_reason,
        "schema_version": ARCHIVE_LEDGER_SCHEMA_VERSION,
        "ingested_at": utc_now_iso(),
    }


def _sample_from_db_row(row: dict[str, Any]) -> TradeQualitySample:
    return TradeQualitySample(
        sample_id=row["sample_id"],
        order_id=row["order_id"],
        strategy_line=row["strategy_line"],
        symbol=row["symbol"],
        side=row["side"],
        source_run_id=row.get("source_run_id"),
        source_cycle_id=row.get("source_cycle_id"),
        source_plan_hash=row.get("source_plan_hash"),
        opened_at=row.get("opened_at"),
        closed_at=row.get("closed_at"),
        exit_reason=row["exit_reason"],
        entry_price=float(row["entry_price"]),
        exit_price=float(row["exit_price"]),
        stop_loss=float(row["stop_loss"]),
        take_profit=float(row["take_profit"]),
        quantity=float(row["quantity"]),
        initial_risk_usdt=float(row["initial_risk_usdt"]),
        gross_pnl_usdt=float(row["gross_pnl_usdt"]),
        net_pnl_usdt=float(row["net_pnl_usdt"]),
        fee_usdt=float(row["fee_usdt"]),
        slippage_usdt=float(row["slippage_usdt"]),
        cost_ratio_R=row.get("cost_ratio_R"),
        planned_RR=row.get("planned_RR"),
        net_R=row.get("net_R"),
        MFE_R=row.get("MFE_R"),
        MAE_R=row.get("MAE_R"),
        holding_sec=row.get("holding_sec"),
        holding_bucket=row["holding_bucket"],
        excursion_model=row["excursion_model"],
        root_cause_label=row["root_cause_label"],
        root_cause_confidence=float(row["root_cause_confidence"]),
        root_cause_evidence=_loads(row.get("root_cause_evidence_json"), {}),
        secondary_labels=_loads(row.get("secondary_labels_json"), []),
        needs_manual_review=bool(row.get("needs_manual_review")),
    )


def _normalize_sample_source(value: str | None) -> str:
    got = str(value or "all").lower()
    return got if got in {"all", "archive", "live"} else "all"


def _skip_reason(order: dict[str, Any]) -> str | None:
    if str(order.get("status") or "").lower() != "closed":
        return "order_not_closed"
    required = ["closed_at", "opened_at", "symbol", "side", "stop_loss", "take_profit"]
    for key in required:
        if not order.get(key):
            return f"missing_{key}"
    if not (order.get("filled_entry_price") or order.get("entry_price")):
        return "missing_entry_price"
    if not (order.get("exit_price") or str(order.get("exit_reason") or "").upper() in {"TP", "SL"}):
        return "missing_exit_price"
    if float(order.get("quantity") or order.get("planned_quantity") or 0) <= 0:
        return "missing_quantity"
    return None


def _read_json_obj(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _archive_hash(path: Path) -> str:
    parts: list[str] = []
    for name in ("metadata.json", "orders.json", "fills.json"):
        file = path / name
        if file.exists():
            parts.append(f"{name}:{file.stat().st_mtime_ns}:{file.stat().st_size}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:24]


def _archive_version(metadata: dict[str, Any]) -> str:
    schema = str(metadata.get("schema_version") or "unknown")
    profile = str(metadata.get("profile_name") or "unknown")
    return f"archive_schema_{schema}|profile_{profile}"


def _fill_model_version(order: dict[str, Any], fills: list[dict[str, Any]]) -> str:
    if any(fill.get("same_candle_policy") is not None or fill.get("entry_drift_bps") is not None for fill in fills):
        return "realistic_1m"
    if order.get("slippage_usdt") is not None or any(fill.get("slippage_bps") is not None for fill in fills):
        return "legacy_slippage_fields"
    return "unknown"


def _trade_plan_model_version(archive_dir: Path, order: dict[str, Any]) -> str:
    plan_hash = str(order.get("source_plan_hash") or "")
    plans = _read_json_obj(archive_dir / "trade_plans.json", [])
    if isinstance(plans, dict):
        plans = plans.get("plans") or []
    if isinstance(plans, list):
        for plan in plans:
            if not isinstance(plan, dict):
                continue
            refs = plan.get("input_refs") if isinstance(plan.get("input_refs"), dict) else {}
            if plan_hash and refs.get("source_plan_hash") == plan_hash:
                guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
                return str(guards.get("sl_tp_model_version") or guards.get("trade_plan_model_version") or "matched_unknown")
    return "unknown"
