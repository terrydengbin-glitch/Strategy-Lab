from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.utils import utc_now_iso
from laoma_signal_engine.trade_quality.recommendation_rules import (
    ensure_recommendation_rule_tables,
    recommendation_rules_payload,
)


VALIDATION_SCHEMA_VERSION = "18.11"


def recommendation_validation_payload(
    db_path: Path,
    *,
    sample_source: str | None = "live",
    rule_type: str | None = None,
    strategy_line: str | None = None,
    side: str | None = None,
    symbol: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    ensure_recommendation_rule_tables(db_path)
    rules = recommendation_rules_payload(db_path, rule_type=rule_type, limit=1000).get("rules", [])
    samples = _load_samples(
        db_path,
        sample_source=sample_source,
        strategy_line=strategy_line,
        side=side,
        symbol=symbol,
        limit=limit,
    )
    matches = _match_samples(samples, rules)
    summary = _summary(matches)
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "source": "trade_quality_recommendation_validation",
        "generated_at": utc_now_iso(),
        "db_path": str(db_path),
        "filters": {
            "sample_source": _normalize_source(sample_source),
            "rule_type": rule_type,
            "strategy_line": strategy_line,
            "side": side,
            "symbol": symbol,
            "limit": limit,
        },
        "sample_count": len(samples),
        "matched_count": len(matches),
        "summary": summary,
        "matches": matches,
    }


def _load_samples(
    db_path: Path,
    *,
    sample_source: str | None,
    strategy_line: str | None,
    side: str | None,
    symbol: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 500), 2000))
    clauses: list[str] = []
    params: list[Any] = []
    source = _normalize_source(sample_source)
    if source == "archive":
        clauses.append(
            "s.sample_id IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
        )
    elif source == "live":
        clauses.append(
            "s.sample_id NOT IN (SELECT sample_id FROM trade_quality_archive_ingest_ledger WHERE ingest_status='inserted')"
        )
    if strategy_line:
        clauses.append("s.strategy_line = ?")
        params.append(strategy_line)
    if side:
        clauses.append("upper(s.side) = ?")
        params.append(side.upper())
    if symbol:
        clauses.append("upper(s.symbol) = ?")
        params.append(symbol.upper())
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT s.*,
              CASE WHEN l.sample_id IS NULL THEN 'live' ELSE 'archive' END AS sample_source,
              json_extract(s.root_cause_evidence_json, '$.config_profile') AS config_profile
            FROM trade_quality_samples s
            LEFT JOIN trade_quality_archive_ingest_ledger l
              ON s.sample_id = l.sample_id AND l.ingest_status='inserted'
            {where}
            ORDER BY s.closed_at DESC, s.sample_id DESC
            LIMIT ?
            """,
            [*params, safe_limit],
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["config_profile"] = item.get("config_profile") or ("live" if item.get("sample_source") == "live" else "unknown")
        out.append(item)
    return out


def _match_samples(samples: list[dict[str, Any]], rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for sample in samples:
        sample_matches = [_match_payload(sample, rule) for rule in rules if _rule_matches_sample(rule, sample)]
        for payload in sample_matches:
            if payload:
                matches.append(payload)
    return matches


def _rule_matches_sample(rule: dict[str, Any], sample: dict[str, Any]) -> bool:
    rule_source = str(rule.get("sample_source") or "all")
    if rule_source not in {"all", str(sample.get("sample_source") or "")}:
        return False
    if rule.get("strategy_line") and rule.get("strategy_line") != sample.get("strategy_line"):
        return False
    if rule.get("side") and str(rule.get("side")).upper() != str(sample.get("side") or "").upper():
        return False
    if rule.get("symbol") and str(rule.get("symbol")).upper() != str(sample.get("symbol") or "").upper():
        return False
    profile = rule.get("config_profile")
    if profile and profile not in {"unknown", str(sample.get("config_profile") or "unknown")}:
        return False
    rule_type = str(rule.get("rule_type") or "")
    if rule_type == "direction_gate":
        return str(sample.get("root_cause_label") or "") in {"direction_wrong", "tp_hit_good_trade", "stop_too_tight"}
    if rule_type == "cost_liquidity":
        return str(sample.get("root_cause_label") or "") == "cost_too_high"
    if rule_type == "symbol_quality_tier":
        return True
    if rule_type == "strategy_side_profile":
        return True
    return False


def _match_payload(sample: dict[str, Any], rule: dict[str, Any]) -> dict[str, Any]:
    net_r = _num(sample.get("net_R"))
    return {
        "sample_id": sample.get("sample_id"),
        "order_id": sample.get("order_id"),
        "symbol": sample.get("symbol"),
        "strategy_line": sample.get("strategy_line"),
        "side": sample.get("side"),
        "sample_source": sample.get("sample_source"),
        "root_cause_label": sample.get("root_cause_label"),
        "net_R": net_r,
        "rule_id": rule.get("rule_id"),
        "rule_type": rule.get("rule_type"),
        "rule_scope": rule.get("scope_key"),
        "recommendation": rule.get("recommendation"),
        "rule_mode": rule.get("mode"),
        "rule_severity": rule.get("severity"),
        "validation_outcome": "loss_confirmed" if net_r < 0 else "profit_contradicts" if net_r > 0 else "flat",
    }


def _summary(matches: list[dict[str, Any]]) -> dict[str, Any]:
    by_rule: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in matches:
        by_rule[str(row.get("rule_id") or "")].append(row)
    rule_rows = []
    for rule_id, rows in by_rule.items():
        values = [_num(row.get("net_R")) for row in rows]
        rule_rows.append(
            {
                "rule_id": rule_id,
                "rule_type": rows[0].get("rule_type"),
                "recommendation": rows[0].get("recommendation"),
                "severity": rows[0].get("rule_severity"),
                "mode": rows[0].get("rule_mode"),
                "hit_count": len(rows),
                "total_R": round(sum(values), 8),
                "avg_R": round(sum(values) / len(values), 8) if values else 0.0,
                "win_rate": round(len([value for value in values if value > 0]) / len(values), 6) if values else 0.0,
            }
        )
    rule_rows.sort(key=lambda row: (str(row.get("severity") or "P9"), float(row.get("total_R") or 0)))
    return {
        "rule_hit_count": len(by_rule),
        "recommendation_counts": dict(Counter(str(row.get("recommendation") or "unknown") for row in matches)),
        "outcome_counts": dict(Counter(str(row.get("validation_outcome") or "unknown") for row in matches)),
        "rule_rows": rule_rows[:100],
    }


def _normalize_source(value: str | None) -> str:
    got = str(value or "all").lower()
    return got if got in {"all", "archive", "live"} else "all"


def _num(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
