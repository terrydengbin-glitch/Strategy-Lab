from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.utils import utc_now_iso
from laoma_signal_engine.trade_quality.archive_backfill import ensure_archive_ingest_tables


RULE_SCHEMA_VERSION = "18.10"


def ensure_recommendation_rule_tables(db_path: Path) -> None:
    ensure_archive_ingest_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_recommendation_rules (
              rule_id TEXT PRIMARY KEY,
              rule_type TEXT NOT NULL,
              scope_type TEXT NOT NULL,
              scope_key TEXT NOT NULL,
              strategy_line TEXT,
              side TEXT,
              symbol TEXT,
              sample_source TEXT NOT NULL,
              config_profile TEXT,
              sample_count INTEGER NOT NULL,
              total_R REAL NOT NULL,
              avg_R REAL,
              win_rate REAL,
              root_cause_counts_json TEXT NOT NULL,
              evidence_json TEXT NOT NULL,
              recommendation TEXT NOT NULL,
              severity TEXT NOT NULL,
              mode TEXT NOT NULL DEFAULT 'shadow',
              confidence REAL NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_quality_rules_type_scope
              ON trade_quality_recommendation_rules(rule_type, scope_type, scope_key, severity)
            """
        )


def rebuild_recommendation_rules(db_path: Path) -> dict[str, Any]:
    ensure_recommendation_rule_tables(db_path)
    samples = _load_sample_rows(db_path)
    rules = build_recommendation_rules(samples)
    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM trade_quality_recommendation_rules")
        for rule in rules:
            item = dict(rule)
            item["generated_at"] = now
            item["schema_version"] = RULE_SCHEMA_VERSION
            item["root_cause_counts_json"] = _json(item.pop("root_cause_counts"))
            item["evidence_json"] = _json(item.pop("evidence"))
            conn.execute(
                """
                INSERT INTO trade_quality_recommendation_rules(
                  rule_id, rule_type, scope_type, scope_key, strategy_line, side, symbol,
                  sample_source, config_profile, sample_count, total_R, avg_R, win_rate,
                  root_cause_counts_json, evidence_json, recommendation, severity, mode,
                  confidence, schema_version, generated_at
                ) VALUES(
                  :rule_id, :rule_type, :scope_type, :scope_key, :strategy_line, :side, :symbol,
                  :sample_source, :config_profile, :sample_count, :total_R, :avg_R, :win_rate,
                  :root_cause_counts_json, :evidence_json, :recommendation, :severity, :mode,
                  :confidence, :schema_version, :generated_at
                )
                """,
                item,
            )
    return {
        "schema_version": RULE_SCHEMA_VERSION,
        "generated_at": now,
        "db_path": str(db_path),
        "sample_count": len(samples),
        "rule_count": len(rules),
        "rule_type_counts": dict(Counter(rule["rule_type"] for rule in rules)),
        "severity_counts": dict(Counter(rule["severity"] for rule in rules)),
        "mode_counts": dict(Counter(rule["mode"] for rule in rules)),
    }


def recommendation_rules_payload(
    db_path: Path,
    *,
    rule_type: str | None = None,
    strategy_line: str | None = None,
    side: str | None = None,
    symbol: str | None = None,
    sample_source: str | None = None,
    config_profile: str | None = None,
    severity: str | None = None,
    mode: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    ensure_recommendation_rule_tables(db_path)
    safe_limit = max(1, min(int(limit or 200), 1000))
    clauses: list[str] = []
    params: list[Any] = []
    filters = {
        "rule_type": rule_type,
        "strategy_line": strategy_line,
        "side": side,
        "symbol": symbol,
        "sample_source": sample_source,
        "config_profile": config_profile,
        "severity": severity,
        "mode": mode,
        "limit": safe_limit,
    }
    if rule_type:
        clauses.append("rule_type = ?")
        params.append(rule_type)
    if strategy_line:
        clauses.append("strategy_line = ?")
        params.append(strategy_line)
    if side:
        clauses.append("upper(side) = ?")
        params.append(side.upper())
    if symbol:
        clauses.append("upper(symbol) = ?")
        params.append(symbol.upper())
    source = _normalize_source(sample_source)
    if source != "all":
        clauses.append("sample_source = ?")
        params.append(source)
    if config_profile:
        clauses.append("config_profile = ?")
        params.append(config_profile)
    if severity:
        clauses.append("severity = ?")
        params.append(severity)
    if mode:
        clauses.append("mode = ?")
        params.append(mode)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT * FROM trade_quality_recommendation_rules
            {where}
            ORDER BY
              CASE severity WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 ELSE 2 END,
              total_R ASC,
              sample_count DESC
            LIMIT ?
            """,
            [*params, safe_limit],
        ).fetchall()
        all_rows = conn.execute("SELECT rule_type, severity, mode, sample_source, count(*) AS count FROM trade_quality_recommendation_rules GROUP BY rule_type, severity, mode, sample_source").fetchall()
    rules = [_decode_rule(dict(row)) for row in rows]
    return {
        "schema_version": RULE_SCHEMA_VERSION,
        "source": "trade_quality_recommendation_rules",
        "db_path": str(db_path),
        "generated_at": utc_now_iso(),
        "filters": filters,
        "count": len(rules),
        "summary": _summary_from_group_rows([dict(row) for row in all_rows]),
        "rules": rules,
    }


def build_recommendation_rules(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    rules.extend(_direction_rules(samples))
    rules.extend(_cost_rules(samples))
    rules.extend(_symbol_tier_rules(samples))
    rules.extend(_strategy_side_rules(samples))
    seen: dict[str, dict[str, Any]] = {}
    for rule in rules:
        seen[rule["rule_id"]] = rule
    return list(seen.values())


def _direction_rules(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = _group(samples, ["strategy_line", "side", "sample_source", "config_profile"])
    rules: list[dict[str, Any]] = []
    for key, rows in groups.items():
        stats = _stats(rows)
        wrong_count = stats["root_cause_counts"].get("direction_wrong", 0)
        tp_count = stats["root_cause_counts"].get("tp_hit_good_trade", 0)
        wrong_rate = wrong_count / stats["sample_count"] if stats["sample_count"] else 0.0
        if stats["sample_count"] < 20 and wrong_count < 8:
            continue
        if wrong_count >= 20 and wrong_rate >= 0.45 and stats["total_R"] < 0:
            rec, sev, mode = "direction_shadow_block", "P0", "shadow"
        elif wrong_count >= 8 and wrong_rate >= 0.35:
            rec, sev, mode = "direction_warn_review", "P1", "warn"
        else:
            rec, sev, mode = "direction_ok", "P2", "shadow"
        strategy_line, side, sample_source, profile = key
        rules.append(
            _rule(
                "direction_gate",
                "strategy_side_profile",
                "|".join(key),
                strategy_line,
                side,
                None,
                sample_source,
                profile,
                stats,
                rec,
                sev,
                mode,
                {
                    "direction_wrong_count": wrong_count,
                    "direction_wrong_rate": round(wrong_rate, 6),
                    "tp_hit_count": tp_count,
                    "evidence_sample_ids": _sample_ids(rows, "direction_wrong"),
                },
            )
        )
    return rules


def _cost_rules(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = _group(samples, ["symbol", "strategy_line", "side", "sample_source"])
    rules: list[dict[str, Any]] = []
    for key, rows in groups.items():
        stats = _stats(rows)
        cost_count = stats["root_cause_counts"].get("cost_too_high", 0)
        cost_rate = cost_count / stats["sample_count"] if stats["sample_count"] else 0.0
        if cost_count <= 0:
            continue
        if stats["sample_count"] >= 5 and (cost_count >= 3 or cost_rate >= 0.3):
            rec, sev, mode = "cost_shadow_blacklist", "P0", "shadow"
        else:
            rec, sev, mode = "cost_warn_reduce_size", "P1", "warn"
        symbol, strategy_line, side, sample_source = key
        rules.append(
            _rule(
                "cost_liquidity",
                "symbol_strategy_side",
                "|".join(key),
                strategy_line,
                side,
                symbol,
                sample_source,
                None,
                stats,
                rec,
                sev,
                mode,
                {
                    "cost_too_high_count": cost_count,
                    "cost_too_high_rate": round(cost_rate, 6),
                    "avg_cost_ratio_R": _avg(row.get("cost_ratio_R") for row in rows),
                    "evidence_sample_ids": _sample_ids(rows, "cost_too_high"),
                },
            )
        )
    return rules


def _symbol_tier_rules(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = _group(samples, ["symbol", "sample_source"])
    rules: list[dict[str, Any]] = []
    for key, rows in groups.items():
        stats = _stats(rows)
        symbol, sample_source = key
        if stats["sample_count"] < 5:
            rec, sev, mode, confidence = "insufficient_samples", "P2", "shadow", 0.25
        elif stats["sample_count"] >= 10 and stats["total_R"] <= -6 and stats["win_rate"] <= 0.35:
            rec, sev, mode, confidence = "quality_shadow_blacklist", "P0", "shadow", _confidence(stats["sample_count"])
        elif stats["sample_count"] >= 5 and stats["total_R"] >= 3 and stats["win_rate"] >= 0.55:
            rec, sev, mode, confidence = "quality_boost", "P1", "warn", _confidence(stats["sample_count"])
        elif stats["sample_count"] >= 5 and stats["total_R"] < 0:
            rec, sev, mode, confidence = "quality_watch", "P1", "warn", _confidence(stats["sample_count"])
        else:
            rec, sev, mode, confidence = "quality_normal", "P2", "shadow", _confidence(stats["sample_count"])
        rules.append(
            _rule(
                "symbol_quality_tier",
                "symbol",
                "|".join(key),
                None,
                None,
                symbol,
                sample_source,
                None,
                stats,
                rec,
                sev,
                mode,
                {
                    "tier": rec,
                    "min_samples_per_symbol": 5,
                    "min_confidence_samples": 10,
                    "top_root_causes": stats["root_cause_counts"],
                },
                confidence_override=confidence,
            )
        )
    return rules


def _strategy_side_rules(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = _group(samples, ["strategy_line", "side", "sample_source"])
    rules: list[dict[str, Any]] = []
    for key, rows in groups.items():
        stats = _stats(rows)
        strategy_line, side, sample_source = key
        wrong_rate = stats["root_cause_counts"].get("direction_wrong", 0) / stats["sample_count"] if stats["sample_count"] else 0.0
        cost_rate = stats["root_cause_counts"].get("cost_too_high", 0) / stats["sample_count"] if stats["sample_count"] else 0.0
        tp_rate = stats["root_cause_counts"].get("tp_hit_good_trade", 0) / stats["sample_count"] if stats["sample_count"] else 0.0
        if stats["sample_count"] < 20:
            rec, sev, mode = "profile_insufficient_samples", "P2", "shadow"
        elif stats["avg_R"] < -0.2 or wrong_rate >= 0.45:
            rec, sev, mode = "profile_warn_direction_quality", "P0", "warn"
        elif stats["avg_R"] < 0:
            rec, sev, mode = "profile_watch", "P1", "warn"
        else:
            rec, sev, mode = "profile_ok", "P2", "shadow"
        rules.append(
            _rule(
                "strategy_side_profile",
                "strategy_side",
                "|".join(key),
                strategy_line,
                side,
                None,
                sample_source,
                None,
                stats,
                rec,
                sev,
                mode,
                {
                    "direction_wrong_rate": round(wrong_rate, 6),
                    "cost_too_high_rate": round(cost_rate, 6),
                    "tp_hit_rate": round(tp_rate, 6),
                },
            )
        )
    return rules


def _load_sample_rows(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    ensure_recommendation_rule_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT s.*,
              CASE WHEN l.sample_id IS NULL THEN 'live' ELSE 'archive' END AS sample_source,
              json_extract(s.root_cause_evidence_json, '$.config_profile') AS config_profile
            FROM trade_quality_samples s
            LEFT JOIN trade_quality_archive_ingest_ledger l
              ON s.sample_id = l.sample_id AND l.ingest_status='inserted'
            """
        ).fetchall()
    decoded = []
    for row in rows:
        item = dict(row)
        item["root_cause_evidence"] = _loads(item.pop("root_cause_evidence_json", None), {})
        item["secondary_labels"] = _loads(item.pop("secondary_labels_json", None), [])
        item["config_profile"] = item.get("config_profile") or ("live" if item.get("sample_source") == "live" else "unknown")
        decoded.append(item)
    return decoded


def _group(samples: list[dict[str, Any]], keys: list[str]) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        groups[tuple(str(sample.get(key) or "unknown") for key in keys)].append(sample)
    return groups


def _stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    r_values = [_num(row.get("net_R")) for row in rows]
    total_r = sum(r_values)
    return {
        "sample_count": len(rows),
        "total_R": round(total_r, 8),
        "avg_R": round(total_r / len(rows), 8) if rows else 0.0,
        "win_rate": round(len([value for value in r_values if value > 0]) / len(rows), 6) if rows else 0.0,
        "root_cause_counts": dict(Counter(str(row.get("root_cause_label") or "unknown") for row in rows)),
        "sample_ids": [str(row.get("sample_id") or "") for row in rows[:25]],
    }


def _rule(
    rule_type: str,
    scope_type: str,
    scope_key: str,
    strategy_line: str | None,
    side: str | None,
    symbol: str | None,
    sample_source: str | None,
    config_profile: str | None,
    stats: dict[str, Any],
    recommendation: str,
    severity: str,
    mode: str,
    evidence: dict[str, Any],
    *,
    confidence_override: float | None = None,
) -> dict[str, Any]:
    raw_id = "|".join([rule_type, scope_type, scope_key, sample_source or "all", config_profile or "all", RULE_SCHEMA_VERSION])
    if mode not in {"shadow", "warn"}:
        mode = "shadow"
    return {
        "rule_id": hashlib.sha256(raw_id.encode("utf-8")).hexdigest()[:24],
        "rule_type": rule_type,
        "scope_type": scope_type,
        "scope_key": scope_key,
        "strategy_line": None if strategy_line == "unknown" else strategy_line,
        "side": None if side == "unknown" else side,
        "symbol": None if symbol == "unknown" else symbol,
        "sample_source": sample_source or "all",
        "config_profile": config_profile,
        "sample_count": int(stats["sample_count"]),
        "total_R": float(stats["total_R"]),
        "avg_R": float(stats["avg_R"]),
        "win_rate": float(stats["win_rate"]),
        "root_cause_counts": stats["root_cause_counts"],
        "evidence": {**evidence, "sample_ids": stats["sample_ids"]},
        "recommendation": recommendation,
        "severity": severity,
        "mode": mode,
        "confidence": confidence_override if confidence_override is not None else _confidence(stats["sample_count"]),
    }


def _decode_rule(row: dict[str, Any]) -> dict[str, Any]:
    row["root_cause_counts"] = _loads(row.pop("root_cause_counts_json", None), {})
    row["evidence"] = _loads(row.pop("evidence_json", None), {})
    return row


def _summary_from_group_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, dict[str, int]] = {
        "rule_type_counts": {},
        "severity_counts": {},
        "mode_counts": {},
        "sample_source_counts": {},
    }
    for row in rows:
        count = int(row.get("count") or 0)
        summary["rule_type_counts"][str(row.get("rule_type"))] = summary["rule_type_counts"].get(str(row.get("rule_type")), 0) + count
        summary["severity_counts"][str(row.get("severity"))] = summary["severity_counts"].get(str(row.get("severity")), 0) + count
        summary["mode_counts"][str(row.get("mode"))] = summary["mode_counts"].get(str(row.get("mode")), 0) + count
        summary["sample_source_counts"][str(row.get("sample_source"))] = summary["sample_source_counts"].get(str(row.get("sample_source")), 0) + count
    return summary


def _sample_ids(rows: list[dict[str, Any]], root_cause: str) -> list[str]:
    return [str(row.get("sample_id") or "") for row in rows if row.get("root_cause_label") == root_cause][:25]


def _confidence(sample_count: int) -> float:
    if sample_count >= 50:
        return 0.9
    if sample_count >= 20:
        return 0.75
    if sample_count >= 10:
        return 0.6
    if sample_count >= 5:
        return 0.45
    return 0.25


def _avg(values: Any) -> float | None:
    nums = [_num(value) for value in values if value is not None]
    return round(sum(nums) / len(nums), 8) if nums else None


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


def _normalize_source(value: str | None) -> str:
    got = str(value or "all").lower()
    return got if got in {"all", "archive", "live"} else "all"
