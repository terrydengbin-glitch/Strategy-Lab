from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.utils import utc_now_iso
from laoma_signal_engine.trade_quality.recommendation_rules import ensure_recommendation_rule_tables


CANDIDATE_SCHEMA_VERSION = "18.14"
ALLOWED_CANDIDATE_RULE_TYPES = {"cost_liquidity", "symbol_quality_tier"}


def ensure_promotion_candidate_tables(db_path: Path) -> None:
    ensure_recommendation_rule_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_promotion_candidates (
              candidate_id TEXT PRIMARY KEY,
              rule_id TEXT NOT NULL,
              rule_type TEXT NOT NULL,
              profile TEXT,
              strategy_line TEXT,
              side TEXT,
              symbol TEXT,
              sample_source TEXT,
              mode TEXT NOT NULL,
              matched_trade_count INTEGER NOT NULL,
              blocked_loss_count INTEGER NOT NULL,
              blocked_win_count INTEGER NOT NULL,
              saved_loss_R REAL NOT NULL,
              missed_profit_R REAL NOT NULL,
              net_saved_R REAL NOT NULL,
              false_block_rate REAL NOT NULL,
              confidence REAL NOT NULL,
              recommendation_priority REAL NOT NULL,
              evidence_json TEXT NOT NULL,
              source_version TEXT NOT NULL,
              created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_quality_promotion_candidates_priority
              ON trade_quality_promotion_candidates(rule_type, mode, net_saved_R DESC, false_block_rate ASC)
            """
        )


def rebuild_promotion_candidates(
    db_path: Path,
    *,
    limit: int = 200,
    write: bool = True,
    min_matched: int = 5,
    max_false_block_rate: float = 0.55,
) -> dict[str, Any]:
    ensure_promotion_candidate_tables(db_path)
    rules = _load_rules(db_path)
    candidates = []
    for rule in rules:
        samples = _matched_samples(db_path, rule)
        item = _candidate_from_rule(rule, samples)
        if item["matched_trade_count"] < min_matched:
            continue
        if item["net_saved_R"] <= 0:
            continue
        if item["false_block_rate"] > max_false_block_rate:
            continue
        candidates.append(item)
    candidates.sort(key=lambda row: (row["recommendation_priority"], row["net_saved_R"], row["matched_trade_count"]), reverse=True)
    safe_limit = max(1, min(int(limit or 200), 1000))
    candidates = candidates[:safe_limit]
    now = utc_now_iso()
    if write:
        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM trade_quality_promotion_candidates")
            for item in candidates:
                row = dict(item)
                row["created_at"] = now
                row["source_version"] = CANDIDATE_SCHEMA_VERSION
                row["evidence_json"] = _json(row.pop("evidence"))
                conn.execute(
                    """
                    INSERT OR REPLACE INTO trade_quality_promotion_candidates(
                      candidate_id, rule_id, rule_type, profile, strategy_line, side, symbol,
                      sample_source, mode, matched_trade_count, blocked_loss_count, blocked_win_count,
                      saved_loss_R, missed_profit_R, net_saved_R, false_block_rate, confidence,
                      recommendation_priority, evidence_json, source_version, created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row["candidate_id"],
                        row["rule_id"],
                        row["rule_type"],
                        row["profile"],
                        row["strategy_line"],
                        row["side"],
                        row["symbol"],
                        row["sample_source"],
                        row["mode"],
                        row["matched_trade_count"],
                        row["blocked_loss_count"],
                        row["blocked_win_count"],
                        row["saved_loss_R"],
                        row["missed_profit_R"],
                        row["net_saved_R"],
                        row["false_block_rate"],
                        row["confidence"],
                        row["recommendation_priority"],
                        row["evidence_json"],
                        row["source_version"],
                        row["created_at"],
                    ),
                )
    return {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "source": "trade_quality_promotion_candidate_dry_run",
        "generated_at": now,
        "db_path": str(db_path),
        "would_write": bool(write),
        "rule_count_considered": len(rules),
        "candidate_count": len(candidates),
        "summary": _summary(candidates),
        "candidates": candidates,
    }


def promotion_candidates_payload(db_path: Path, *, limit: int = 200) -> dict[str, Any]:
    ensure_promotion_candidate_tables(db_path)
    safe_limit = max(1, min(int(limit or 200), 1000))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM trade_quality_promotion_candidates
            ORDER BY recommendation_priority DESC, net_saved_R DESC, false_block_rate ASC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
    candidates = [_decode_candidate(dict(row)) for row in rows]
    return {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "source": "trade_quality_promotion_candidates",
        "generated_at": utc_now_iso(),
        "db_path": str(db_path),
        "count": len(candidates),
        "summary": _summary(candidates),
        "candidates": candidates,
    }


def _load_rules(db_path: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM trade_quality_recommendation_rules
            WHERE rule_type IN ('cost_liquidity', 'symbol_quality_tier')
              AND sample_count >= 5
              AND confidence >= 0.45
            ORDER BY
              CASE severity WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 ELSE 2 END,
              total_R ASC,
              sample_count DESC
            """
        ).fetchall()
    return [_decode_rule(dict(row)) for row in rows]


def _matched_samples(db_path: Path, rule: dict[str, Any]) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if rule.get("strategy_line"):
        clauses.append("strategy_line=?")
        params.append(rule["strategy_line"])
    if rule.get("symbol"):
        clauses.append("upper(symbol)=?")
        params.append(str(rule["symbol"]).upper())
    if rule.get("side"):
        clauses.append("upper(side)=?")
        params.append(str(rule["side"]).upper())
    if rule.get("sample_source") and str(rule.get("sample_source")) != "all":
        if str(rule.get("sample_source")) == "archive":
            clauses.append(
                "sample_id IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
            )
        elif str(rule.get("sample_source")) == "live":
            clauses.append(
                "sample_id NOT IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
            )
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT sample_id, order_id, symbol, strategy_line, side, root_cause_label,
              net_R, closed_at
            FROM trade_quality_samples
            {where}
            ORDER BY closed_at DESC, sample_id DESC
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def _candidate_from_rule(rule: dict[str, Any], samples: list[dict[str, Any]]) -> dict[str, Any]:
    losses = [_num(row.get("net_R")) for row in samples if _num(row.get("net_R")) <= 0]
    wins = [_num(row.get("net_R")) for row in samples if _num(row.get("net_R")) > 0]
    saved_loss = round(sum(abs(value) for value in losses), 8)
    missed_profit = round(sum(wins), 8)
    net_saved = round(saved_loss - missed_profit, 8)
    matched = len(samples)
    false_block_rate = round(len(wins) / matched, 8) if matched else 0.0
    confidence = float(rule.get("confidence") or 0.0)
    priority = round(net_saved * confidence * (1.0 - min(false_block_rate, 0.95)), 8)
    mode = "wait_only"
    candidate_id = hashlib.sha256(
        "|".join(
            [
                str(rule.get("rule_id") or ""),
                str(rule.get("rule_type") or ""),
                str(rule.get("strategy_line") or "all"),
                str(rule.get("symbol") or "all"),
                str(rule.get("side") or "all"),
                mode,
                CANDIDATE_SCHEMA_VERSION,
            ]
        ).encode("utf-8")
    ).hexdigest()[:24]
    return {
        "candidate_id": candidate_id,
        "rule_id": str(rule.get("rule_id") or ""),
        "rule_type": str(rule.get("rule_type") or ""),
        "profile": rule.get("config_profile"),
        "strategy_line": rule.get("strategy_line"),
        "side": rule.get("side"),
        "symbol": rule.get("symbol"),
        "sample_source": rule.get("sample_source"),
        "mode": mode,
        "matched_trade_count": matched,
        "blocked_loss_count": len(losses),
        "blocked_win_count": len(wins),
        "saved_loss_R": saved_loss,
        "missed_profit_R": missed_profit,
        "net_saved_R": net_saved,
        "false_block_rate": false_block_rate,
        "confidence": confidence,
        "recommendation_priority": priority,
        "evidence": {
            "rule": rule,
            "sample_ids": [str(row.get("sample_id") or "") for row in samples[:50]],
            "loss_sample_ids": [str(row.get("sample_id") or "") for row in samples if _num(row.get("net_R")) <= 0][:50],
            "win_sample_ids": [str(row.get("sample_id") or "") for row in samples if _num(row.get("net_R")) > 0][:50],
        },
    }


def _summary(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, int] = {}
    for row in candidates:
        key = str(row.get("rule_type") or "unknown")
        by_type[key] = by_type.get(key, 0) + 1
    return {
        "rule_type_counts": by_type,
        "total_net_saved_R": round(sum(float(row.get("net_saved_R") or 0.0) for row in candidates), 8),
        "total_saved_loss_R": round(sum(float(row.get("saved_loss_R") or 0.0) for row in candidates), 8),
        "total_missed_profit_R": round(sum(float(row.get("missed_profit_R") or 0.0) for row in candidates), 8),
        "avg_false_block_rate": round(
            sum(float(row.get("false_block_rate") or 0.0) for row in candidates) / len(candidates), 8
        ) if candidates else 0.0,
    }


def _decode_rule(row: dict[str, Any]) -> dict[str, Any]:
    for key in ("root_cause_counts_json", "evidence_json"):
        if key in row:
            row[key.replace("_json", "")] = _loads(row.pop(key), {})
    return row


def _decode_candidate(row: dict[str, Any]) -> dict[str, Any]:
    if "evidence_json" in row:
        row["evidence"] = _loads(row.pop("evidence_json"), {})
    return row


def _num(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default
