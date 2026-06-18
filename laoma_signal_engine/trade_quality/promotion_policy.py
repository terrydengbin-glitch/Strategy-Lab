from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.utils import utc_now_iso
from laoma_signal_engine.trade_quality.recommendation_rules import ensure_recommendation_rule_tables


PROMOTION_SCHEMA_VERSION = "18.12"
ALLOWED_PROMOTION_MODES = {"wait_only", "block_executable"}
DEFAULT_FIRST_STAGE_RULE_TYPES = {"cost_liquidity", "symbol_quality_tier"}


def ensure_promotion_tables(db_path: Path) -> None:
    ensure_recommendation_rule_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_recommendation_promotions (
              promotion_id TEXT PRIMARY KEY,
              rule_id TEXT NOT NULL,
              profile TEXT NOT NULL,
              strategy_line TEXT,
              mode TEXT NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1,
              reason TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              evidence_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_quality_promotions_rule
              ON trade_quality_recommendation_promotions(rule_id, profile, strategy_line, enabled)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_recommendation_promotion_ledger (
              event_id TEXT PRIMARY KEY,
              promotion_id TEXT NOT NULL,
              rule_id TEXT NOT NULL,
              action TEXT NOT NULL,
              profile TEXT NOT NULL,
              strategy_line TEXT,
              mode TEXT,
              reason TEXT NOT NULL,
              created_at TEXT NOT NULL,
              evidence_json TEXT NOT NULL
            )
            """
        )


def promotions_payload(
    db_path: Path,
    *,
    profile: str | None = None,
    strategy_line: str | None = None,
    enabled: bool | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    ensure_promotion_tables(db_path)
    clauses: list[str] = []
    params: list[Any] = []
    if profile:
        clauses.append("p.profile = ?")
        params.append(profile)
    if strategy_line:
        clauses.append("(p.strategy_line = ? OR p.strategy_line IS NULL)")
        params.append(strategy_line)
    if enabled is not None:
        clauses.append("p.enabled = ?")
        params.append(1 if enabled else 0)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    safe_limit = max(1, min(int(limit or 200), 1000))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT p.*, r.rule_type, r.scope_type, r.scope_key, r.symbol, r.side,
              r.sample_source, r.sample_count, r.total_R, r.avg_R, r.win_rate,
              r.recommendation, r.severity, r.confidence
            FROM trade_quality_recommendation_promotions p
            LEFT JOIN trade_quality_recommendation_rules r ON p.rule_id = r.rule_id
            {where}
            ORDER BY p.enabled DESC, p.updated_at DESC
            LIMIT ?
            """,
            [*params, safe_limit],
        ).fetchall()
    promotions = [_decode(dict(row)) for row in rows]
    return {
        "schema_version": PROMOTION_SCHEMA_VERSION,
        "source": "trade_quality_recommendation_promotions",
        "generated_at": utc_now_iso(),
        "db_path": str(db_path),
        "count": len(promotions),
        "summary": {
            "enabled": len([row for row in promotions if row.get("enabled")]),
            "disabled": len([row for row in promotions if not row.get("enabled")]),
        },
        "promotions": promotions,
    }


def promotion_dry_run(
    db_path: Path,
    *,
    rule_id: str,
    profile: str,
    strategy_line: str | None,
    mode: str,
) -> dict[str, Any]:
    rule = _load_rule(db_path, rule_id)
    _validate_promotion(rule, profile=profile, mode=mode)
    affected = _affected_samples(db_path, rule, strategy_line=strategy_line)
    return {
        "schema_version": PROMOTION_SCHEMA_VERSION,
        "source": "trade_quality_recommendation_promotion_dry_run",
        "generated_at": utc_now_iso(),
        "rule": rule,
        "requested": {"profile": profile, "strategy_line": strategy_line, "mode": mode},
        "allowed": True,
        "would_write": False,
        "affected_recent_samples": affected,
        "affected_count": len(affected),
        "reason_codes": _promotion_reason_codes(rule, mode),
    }


def apply_promotion(
    db_path: Path,
    *,
    rule_id: str,
    profile: str,
    strategy_line: str | None,
    mode: str,
    reason: str = "manual_enable",
) -> dict[str, Any]:
    dry = promotion_dry_run(db_path, rule_id=rule_id, profile=profile, strategy_line=strategy_line, mode=mode)
    now = utc_now_iso()
    promotion_id = _promotion_id(rule_id, profile, strategy_line, mode)
    evidence = {"dry_run": dry, "reason_codes": dry["reason_codes"]}
    ensure_promotion_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO trade_quality_recommendation_promotions(
              promotion_id, rule_id, profile, strategy_line, mode, enabled, reason,
              created_at, updated_at, schema_version, evidence_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(promotion_id) DO UPDATE SET
              enabled=1, reason=excluded.reason, updated_at=excluded.updated_at, evidence_json=excluded.evidence_json
            """,
            (
                promotion_id,
                rule_id,
                profile,
                strategy_line,
                mode,
                1,
                reason,
                now,
                now,
                PROMOTION_SCHEMA_VERSION,
                _json(evidence),
            ),
        )
        _write_ledger(conn, promotion_id, rule_id, "apply", profile, strategy_line, mode, reason, evidence)
    return {**dry, "would_write": True, "promotion_id": promotion_id, "status": "applied_shadow_contract"}


def disable_promotion(
    db_path: Path,
    *,
    promotion_id: str,
    reason: str = "manual_disable",
) -> dict[str, Any]:
    ensure_promotion_tables(db_path)
    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM trade_quality_recommendation_promotions WHERE promotion_id=?", (promotion_id,)).fetchone()
        if row is None:
            raise ValueError(f"promotion not found: {promotion_id}")
        conn.execute(
            "UPDATE trade_quality_recommendation_promotions SET enabled=0, reason=?, updated_at=? WHERE promotion_id=?",
            (reason, now, promotion_id),
        )
        _write_ledger(
            conn,
            promotion_id,
            str(row["rule_id"]),
            "disable",
            str(row["profile"]),
            row["strategy_line"],
            row["mode"],
            reason,
            {"previous": dict(row)},
        )
    return {"schema_version": PROMOTION_SCHEMA_VERSION, "generated_at": now, "promotion_id": promotion_id, "status": "disabled"}


def _load_rule(db_path: Path, rule_id: str) -> dict[str, Any]:
    ensure_promotion_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM trade_quality_recommendation_rules WHERE rule_id=?", (rule_id,)).fetchone()
    if row is None:
        raise ValueError(f"rule not found: {rule_id}")
    return _decode(dict(row))


def _validate_promotion(rule: dict[str, Any], *, profile: str, mode: str) -> None:
    if not profile:
        raise ValueError("profile is required")
    if mode not in ALLOWED_PROMOTION_MODES:
        raise ValueError(f"unsupported promotion mode: {mode}")
    if mode == "block_executable" and rule.get("rule_type") not in DEFAULT_FIRST_STAGE_RULE_TYPES:
        raise ValueError("block_executable is not allowed for direction/profile rules in first-stage promotion")
    if float(rule.get("confidence") or 0) < 0.45:
        raise ValueError("rule confidence too low for promotion")
    if int(rule.get("sample_count") or 0) < 5:
        raise ValueError("rule sample_count too low for promotion")


def _affected_samples(db_path: Path, rule: dict[str, Any], *, strategy_line: str | None) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    line = strategy_line or rule.get("strategy_line")
    if line:
        clauses.append("strategy_line=?")
        params.append(line)
    if rule.get("symbol"):
        clauses.append("upper(symbol)=?")
        params.append(str(rule["symbol"]).upper())
    if rule.get("side"):
        clauses.append("upper(side)=?")
        params.append(str(rule["side"]).upper())
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT sample_id, order_id, symbol, strategy_line, side, root_cause_label, net_R, closed_at
            FROM trade_quality_samples
            {where}
            ORDER BY closed_at DESC, sample_id DESC
            LIMIT 50
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def _promotion_reason_codes(rule: dict[str, Any], mode: str) -> list[str]:
    return [
        "trade_quality_recommendation_promotion",
        f"trade_quality_rule_{rule.get('rule_type')}",
        f"trade_quality_promotion_{mode}",
        f"trade_quality_rule_{rule.get('recommendation')}",
    ]


def _write_ledger(
    conn: sqlite3.Connection,
    promotion_id: str,
    rule_id: str,
    action: str,
    profile: str,
    strategy_line: str | None,
    mode: str | None,
    reason: str,
    evidence: dict[str, Any],
) -> None:
    now = utc_now_iso()
    event_id = hashlib.sha256(f"{promotion_id}|{action}|{now}".encode("utf-8")).hexdigest()[:24]
    conn.execute(
        """
        INSERT INTO trade_quality_recommendation_promotion_ledger(
          event_id, promotion_id, rule_id, action, profile, strategy_line, mode,
          reason, created_at, evidence_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (event_id, promotion_id, rule_id, action, profile, strategy_line, mode, reason, now, _json(evidence)),
    )


def _promotion_id(rule_id: str, profile: str, strategy_line: str | None, mode: str) -> str:
    return hashlib.sha256(f"{rule_id}|{profile}|{strategy_line or 'all'}|{mode}".encode("utf-8")).hexdigest()[:24]


def _decode(row: dict[str, Any]) -> dict[str, Any]:
    for key in ("root_cause_counts_json", "evidence_json"):
        if key in row:
            row[key.replace("_json", "")] = _loads(row.pop(key), {})
    if "enabled" in row:
        row["enabled"] = bool(row["enabled"])
    return row


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default
