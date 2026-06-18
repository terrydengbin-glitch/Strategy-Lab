from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from laoma_signal_engine.core.json_io import write_json_atomic
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanLineDocument
from laoma_signal_engine.strategy4.observe import (
    _due_items,
    _settle_pool_after_attempt,
    db_path,
    load_pool,
    pool_path,
    sync_observe_pool_from_without_micro,
)


def _write_pool(root: Path, items: list[dict]) -> None:
    write_json_atomic(
        pool_path(root),
        {
            "schema_version": "17.1",
            "source": "strategy4_observe_pool",
            "generated_at": "2026-06-06T00:00:00Z",
            "status": "ok",
            "count": len(items),
            "status_counts": {},
            "items": items,
            "rejected_items": [],
            "input_refs": {},
        },
    )


def test_step179_retryable_wait_is_retained_after_ttl(tmp_path: Path) -> None:
    _write_pool(
        tmp_path,
        [
            {
                "symbol": "AAAUSDT",
                "status": "still_wait",
                "first_seen_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
                "next_check_at": "2000-01-01T00:00:00Z",
                "attempt_count": 3,
                "source_reason_codes": ["WAIT_REBOUND"],
                "last_reason_codes": ["WAIT_REBOUND"],
            }
        ],
    )

    due = _due_items(tmp_path, load_pool(tmp_path))
    pool = load_pool(tmp_path)
    item = pool["items"][0]

    assert [row["symbol"] for row in due] == ["AAAUSDT"]
    assert item["status"] == "still_wait"
    assert item["ttl_expired"] is True
    assert item["retention_policy"] == "retryable_wait_retained_after_ttl"
    assert item.get("evict_reason", "") == ""


def test_step1710_plan_missing_attempt_has_reason_code(tmp_path: Path) -> None:
    due = [
        {
            "symbol": "BBBUsdt".upper(),
            "status": "observing",
            "first_seen_at": "2026-06-06T00:00:00Z",
            "updated_at": "2026-06-06T00:00:00Z",
            "next_check_at": "2026-06-06T00:00:00Z",
            "attempt_count": 0,
            "source_reason_codes": ["WAIT_PULLBACK"],
            "lineage": {"admission_source": "strategy1_without_micro"},
        }
    ]
    _write_pool(tmp_path, due)
    doc = TradePlanLineDocument(
        generated_at="2026-06-06T00:05:00Z",
        run_id="run_step1710",
        cycle_id="cycle_step1710",
        source="trade_plan_strategy4",
        micro_mode="none",
        status="no_entries",
        count=0,
        executable_count=0,
        input_refs={},
        plans=[],
    )

    _settle_pool_after_attempt(tmp_path, doc, due, "2026-06-06T00:05:00Z")

    with sqlite3.connect(db_path(tmp_path)) as con:
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM strategy4_attempts WHERE symbol='BBBUSDT'").fetchone()
    assert row is not None
    reasons = json.loads(row["reason_codes_json"])
    assert reasons == ["strategy4_plan_missing_for_due_symbol"]
    assert row["action"] == "WAIT"
    assert row["entry_mode"] == "WAIT_EVIDENCE_MISSING"


def test_step179_evicted_symbol_reentry_restarts_observe_epoch(tmp_path: Path) -> None:
    _write_pool(
        tmp_path,
        [
            {
                "symbol": "CCCUSDT",
                "status": "evicted",
                "first_seen_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
                "next_check_at": "",
                "attempt_count": 9,
                "evict_reason": "observe_ttl_expired",
                "source_reason_codes": ["WAIT_REBOUND"],
                "last_reason_codes": ["WAIT_REBOUND"],
            }
        ],
    )
    doc = {
        "source": "trade_plan_without_micro",
        "generated_at": "2026-06-06T00:00:00Z",
        "run_id": "run_reentry",
        "cycle_id": "cycle_reentry",
        "plans": [
            {
                "symbol": "CCCUSDT",
                "decision": "SHORT",
                "action": "WAIT",
                "entry_mode": "WAIT_REBOUND",
                "executable": False,
                "reason_codes": ["WAIT_REBOUND"],
                "guards": {},
                "input_refs": {"source_plan_hash": "hash_reentry"},
            }
        ],
    }

    pool = sync_observe_pool_from_without_micro(project_root=tmp_path, trade_plan_doc=doc)
    item = next(row for row in pool["items"] if row["symbol"] == "CCCUSDT")

    assert item["status"] == "observing"
    assert item["attempt_count"] == 0
    assert item["first_seen_at"] == pool["generated_at"]
    assert item["next_check_at"] == pool["generated_at"]
    assert item["evict_reason"] == ""
    assert item["ttl_expired"] is False
    assert item["retention_policy"] == "new_epoch_after_reentry"
