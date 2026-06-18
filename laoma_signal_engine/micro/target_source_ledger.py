"""SQLite ledger for per-target source lineage.

This ledger is intentionally append/upsert oriented: the router owns target
selection, while downstream audit/API layers use this table to explain whether a
symbol entered only the observe pool or became consumable later.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


def default_target_source_db(project_root: Path | None = None) -> Path:
    root = Path(project_root).resolve() if project_root else Path.cwd().resolve()
    return root / "DATA" / "audit" / "run_audit.db"


def init_target_source_ledger_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            create table if not exists micro_target_source_ledger (
              target_set_id text not null,
              generated_at text,
              run_id text,
              cycle_id text,
              strategy_line text not null default 'micro',
              symbol text not null,
              tier text,
              source_type text,
              selected_for_fast integer not null default 0,
              selected_for_full integer not null default 0,
              entered_observe integer not null default 1,
              entered_trade_candidate integer not null default 0,
              first_seen_ts text,
              last_seen_ts text,
              retention_until_ts text,
              target_age_sec integer,
              consumed_by_trade_plan integer not null default 0,
              payload_json text not null,
              primary key(target_set_id, strategy_line, symbol)
            )
            """
        )
        conn.execute(
            "create index if not exists idx_micro_target_source_ledger_symbol "
            "on micro_target_source_ledger(symbol, generated_at desc)"
        )
        conn.execute(
            "create index if not exists idx_micro_target_source_ledger_run "
            "on micro_target_source_ledger(run_id, generated_at desc)"
        )


def _source_type(item: dict[str, Any]) -> str:
    source_state = str(item.get("source_state") or "unknown")
    retained_reason = str(item.get("retained_reason") or "")
    sticky_source = str(item.get("sticky_source") or "")
    if source_state == "raw_candidate":
        return "raw_fill"
    if sticky_source and sticky_source != "current":
        return sticky_source
    return retained_reason or source_state


def ingest_target_source_ledger(
    target_doc: dict[str, Any],
    *,
    db_path: Path,
    run_id: str | None = None,
    cycle_id: str | None = None,
) -> int:
    init_target_source_ledger_db(db_path)
    target_set_id = str(target_doc.get("target_set_id") or "")
    if not target_set_id:
        return 0
    generated_at = target_doc.get("generated_at")
    rows: list[tuple[Any, ...]] = []
    for tier in ("tier1_warm_watch", "tier2_active_strong"):
        for raw in target_doc.get(tier) or []:
            item = raw if isinstance(raw, dict) else getattr(raw, "model_dump", lambda: {})()
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            payload = json.dumps(item, ensure_ascii=False, sort_keys=True)
            rows.append(
                (
                    target_set_id,
                    generated_at,
                    run_id,
                    cycle_id,
                    "micro",
                    symbol,
                    tier,
                    _source_type(item),
                    1,
                    1,
                    1,
                    0,
                    item.get("first_seen_at") or generated_at,
                    item.get("last_seen_at") or generated_at,
                    item.get("retention_until_ts"),
                    int(item.get("sticky_age_sec") or 0),
                    0,
                    payload,
                )
            )
    if not rows:
        return 0
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            insert into micro_target_source_ledger (
              target_set_id, generated_at, run_id, cycle_id, strategy_line, symbol,
              tier, source_type, selected_for_fast, selected_for_full,
              entered_observe, entered_trade_candidate, first_seen_ts, last_seen_ts,
              retention_until_ts, target_age_sec, consumed_by_trade_plan, payload_json
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(target_set_id, strategy_line, symbol) do update set
              generated_at=excluded.generated_at,
              run_id=coalesce(excluded.run_id, micro_target_source_ledger.run_id),
              cycle_id=coalesce(excluded.cycle_id, micro_target_source_ledger.cycle_id),
              tier=excluded.tier,
              source_type=excluded.source_type,
              selected_for_fast=excluded.selected_for_fast,
              selected_for_full=excluded.selected_for_full,
              entered_observe=excluded.entered_observe,
              first_seen_ts=coalesce(micro_target_source_ledger.first_seen_ts, excluded.first_seen_ts),
              last_seen_ts=excluded.last_seen_ts,
              retention_until_ts=excluded.retention_until_ts,
              target_age_sec=excluded.target_age_sec,
              payload_json=excluded.payload_json
            """,
            rows,
        )
    return len(rows)


def latest_target_source_ledger(db_path: Path, *, limit: int = 500) -> dict[str, Any]:
    if not db_path.is_file():
        return {"source": "micro_target_source_ledger", "targets": [], "count": 0}
    init_target_source_ledger_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            select * from micro_target_source_ledger
            order by generated_at desc
            limit ?
            """,
            (max(1, min(int(limit or 500), 2000)),),
        ).fetchall()
    out = [dict(row) for row in rows]
    return {"source": "micro_target_source_ledger", "targets": out, "count": len(out)}

