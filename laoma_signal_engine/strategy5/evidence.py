"""STEP20 Strategy5 direction evidence collector and trade-plan adapter.

Strategy5 is a normal pipeline line, but v1 does not reserve micro slots or alter
existing strategy semantics. It consumes the current factor/trade-plan evidence and
emits an independent Strategy5 trade-plan document for paper/audit comparison.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanLineDocument


SCHEMA_VERSION = "20.1"
SOURCE = "strategy5_direction_evidence"
FACTOR_FRESHNESS_POLICY = "latest_generated_at_with_required_strategy5_evidence"
FACTOR_CANDIDATES = (
    "DATA/factors/latest_factor_snapshot_withoutoficvd.json",
    "DATA/factors/latest_factor_snapshot.json",
)


@dataclass(frozen=True)
class Strategy5Paths:
    db: Path
    latest_trade_plan: Path
    latest_evidence: Path


def paths(project_root: Path) -> Strategy5Paths:
    root = Path(project_root)
    return Strategy5Paths(
        db=root / "DATA" / "strategy5" / "strategy5.db",
        latest_trade_plan=root / "DATA" / "decisions" / "latest_trade_plan_strategy5.json",
        latest_evidence=root / "DATA" / "strategy5" / "latest_direction_evidence.json",
    )


def _trade_plan_archive_path(project_root: Path, run_id: str | None) -> Path | None:
    if not run_id:
        return None
    return Path(project_root) / "DATA" / "decisions" / "trade_plan_runs" / str(run_id) / "latest_trade_plan_strategy5.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = read_json_object(path)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _items(doc: dict[str, Any]) -> list[dict[str, Any]]:
    rows = doc.get("items")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    rows = doc.get("plans")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _parse_generated_at(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _has_strategy5_required_evidence(row: dict[str, Any]) -> bool:
    return all(isinstance(row.get(key), dict) and bool(row.get(key)) for key in ("primary_15m", "trigger_5m", "entry_1m"))


def _factor_required_evidence_count(doc: dict[str, Any]) -> int:
    return sum(1 for row in _items(doc) if _has_strategy5_required_evidence(row))


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _side_from_move(move_side: str, price_ret: float) -> str:
    side = str(move_side or "").lower()
    if side in {"up", "long", "bullish"}:
        return "LONG"
    if side in {"down", "short", "bearish"}:
        return "SHORT"
    if price_ret > 0:
        return "LONG"
    if price_ret < 0:
        return "SHORT"
    return "NO_TRADE"


def _signed_for_side(side: str, value: float) -> float:
    if side == "SHORT":
        return -value
    return value


def build_evidence_vector(item: dict[str, Any]) -> dict[str, Any]:
    primary = item.get("primary_15m") if isinstance(item.get("primary_15m"), dict) else {}
    trigger = item.get("trigger_5m") if isinstance(item.get("trigger_5m"), dict) else {}
    entry = item.get("entry_1m") if isinstance(item.get("entry_1m"), dict) else {}
    micro = item.get("micro_15m") if isinstance(item.get("micro_15m"), dict) else {}
    oi = item.get("oi_15m") if isinstance(item.get("oi_15m"), dict) else {}
    funding = item.get("funding_context") if isinstance(item.get("funding_context"), dict) else {}
    price_15m = _num(primary.get("price_ret"))
    side = _side_from_move(str(item.get("move_side") or ""), price_15m)
    price_5m = _num(trigger.get("price_ret"))
    volume_z = _num(primary.get("volume_ratio"), 1.0)
    taker_buy_ratio = _num(primary.get("taker_buy_ratio"), 0.5)
    cvd_state = str(micro.get("cvd_state") or primary.get("kline_cvd_state") or "unknown")
    ofi_state = str(micro.get("ofi_state") or micro.get("ofi_pressure") or "unknown")
    spread_bps = _num(micro.get("spread_bps") or item.get("spread_bps"))
    oi_change = _num(oi.get("oi_change_pct") or oi.get("open_interest_change_pct"))
    funding_rate = _num(funding.get("funding_rate") or funding.get("last_funding_rate"))
    evidence_present = {
        "primary_15m": bool(primary),
        "trigger_5m": bool(trigger),
        "entry_1m": bool(entry),
        "micro": bool(micro and (micro.get("ready") is True or cvd_state != "unknown" or ofi_state != "unknown")),
        "oi": bool(oi),
        "funding": bool(funding),
    }
    required = ["primary_15m", "trigger_5m", "entry_1m"]
    missing_required = [key for key in required if not evidence_present.get(key)]
    usable_required = sum(1 for key in required if evidence_present.get(key))
    usable_all = sum(1 for ok in evidence_present.values() if ok)
    return {
        "schema_version": SCHEMA_VERSION,
        "symbol": str(item.get("symbol") or "").upper(),
        "trigger_side": side,
        "legacy_move_side": item.get("move_side"),
        "price_ret_15m": price_15m,
        "price_ret_5m": price_5m,
        "volume_z": volume_z,
        "taker_buy_ratio": taker_buy_ratio,
        "cvd_state": cvd_state,
        "ofi_state": ofi_state,
        "spread_bps": spread_bps,
        "oi_change_pct": oi_change,
        "funding_rate": funding_rate,
        "range_pos": _num(primary.get("range_pos"), 0.5),
        "evidence_present": evidence_present,
        "evidence_quality": {
            "required_usable": usable_required,
            "required_total": len(required),
            "total_usable": usable_all,
            "total_known": len(evidence_present),
            "usable": usable_required == len(required),
            "missing_required_keys": missing_required,
            "micro_optional_missing": not evidence_present["micro"],
            "oi_optional_missing": not evidence_present["oi"],
            "funding_optional_missing": not evidence_present["funding"],
        },
    }


def score_hypothesis(ev: dict[str, Any]) -> dict[str, Any]:
    side = str(ev.get("trigger_side") or "NO_TRADE")
    if side not in {"LONG", "SHORT"}:
        return {
            "shadow_hypothesis_side": "NO_TRADE",
            "shadow_label": "no_trigger_side",
            "shadow_recommendation": "no_trade",
            "continuation_score": 0,
            "exhaustion_score": 0,
            "reason_codes": ["strategy5_no_trigger_side"],
        }
    price_score = min(35.0, abs(_num(ev.get("price_ret_15m"))) * 12.0 + abs(_num(ev.get("price_ret_5m"))) * 8.0)
    volume_score = min(20.0, max(0.0, (_num(ev.get("volume_z"), 1.0) - 1.0) * 18.0))
    taker_bias = _num(ev.get("taker_buy_ratio"), 0.5) - 0.5
    taker_score = min(15.0, max(-15.0, _signed_for_side(side, taker_bias) * 60.0))
    cvd = str(ev.get("cvd_state") or "unknown").lower()
    ofi = str(ev.get("ofi_state") or "unknown").lower()
    cvd_score = 0.0
    if side == "LONG" and any(x in cvd for x in ("buy", "bull", "positive")):
        cvd_score = 10.0
    elif side == "SHORT" and any(x in cvd for x in ("sell", "bear", "negative")):
        cvd_score = 10.0
    elif cvd != "unknown":
        cvd_score = -8.0
    ofi_score = 0.0
    if side == "LONG" and any(x in ofi for x in ("buy", "bid", "positive", "bull")):
        ofi_score = 10.0
    elif side == "SHORT" and any(x in ofi for x in ("sell", "ask", "negative", "bear")):
        ofi_score = 10.0
    elif ofi != "unknown":
        ofi_score = -8.0
    spread_penalty = min(12.0, max(0.0, (_num(ev.get("spread_bps")) - 12.0) * 0.35))
    continuation = max(0, min(100, round(40 + price_score + volume_score + taker_score + cvd_score + ofi_score - spread_penalty)))
    exhaustion = max(0, min(100, round(abs(_num(ev.get("price_ret_15m"))) * 8.0 + spread_penalty * 2.0 - volume_score * 0.4)))
    reasons: list[str] = []
    if not ev.get("evidence_quality", {}).get("usable"):
        reasons.append("strategy5_required_evidence_missing")
        for key in ev.get("evidence_quality", {}).get("missing_required_keys") or []:
            reasons.append(f"strategy5_missing_{key}")
    if ev.get("evidence_quality", {}).get("micro_optional_missing"):
        reasons.append("strategy5_micro_optional_missing")
    if ev.get("evidence_quality", {}).get("oi_optional_missing"):
        reasons.append("strategy5_oi_optional_missing")
    if ev.get("evidence_quality", {}).get("funding_optional_missing"):
        reasons.append("strategy5_funding_optional_missing")
    if continuation < 60:
        reasons.append("strategy5_continuation_score_low")
    if exhaustion >= 55:
        reasons.append("strategy5_exhaustion_risk_high")
    label = "continuation" if continuation >= 65 and exhaustion < 55 else "mixed_or_no_edge"
    recommendation = "allow_if_legacy_agrees" if label == "continuation" else "shadow_only"
    return {
        "shadow_hypothesis_side": side,
        "shadow_label": label,
        "shadow_recommendation": recommendation,
        "continuation_score": continuation,
        "exhaustion_score": exhaustion,
        "reason_codes": reasons or ["strategy5_evidence_ok"],
    }


def _factor_doc(project_root: Path) -> dict[str, Any]:
    root = Path(project_root)
    candidates: list[tuple[int, datetime, str, dict[str, Any], int]] = []
    for rel in FACTOR_CANDIDATES:
        path = root / rel
        doc = _read_json(path)
        if not doc:
            continue
        item_count = len(_items(doc))
        if item_count <= 0:
            continue
        required_count = _factor_required_evidence_count(doc)
        candidates.append((1 if required_count > 0 else 0, _parse_generated_at(doc.get("generated_at")), rel, doc, required_count))
    if candidates:
        _required_ok, _generated_at, rel, doc, required_count = sorted(candidates, key=lambda row: (row[0], row[1]), reverse=True)[0]
        selected = dict(doc)
        selected["_selected_path"] = str(root / rel)
        selected["_selected_relpath"] = rel
        selected["_selected_reason"] = "latest_with_required_evidence" if required_count > 0 else "latest_available_without_required_evidence"
        selected["_freshness_policy"] = FACTOR_FRESHNESS_POLICY
        selected["_required_evidence_count"] = required_count
        return selected
    return {}


def _base_trade_plan_doc(project_root: Path) -> dict[str, Any]:
    return _read_json(Path(project_root) / "DATA" / "decisions" / "latest_trade_plan_without_micro.json")


def _init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            create table if not exists strategy5_evidence (
                evidence_id text primary key,
                run_id text,
                cycle_id text,
                generated_at text,
                symbol text,
                trigger_side text,
                legacy_side text,
                label text,
                recommendation text,
                continuation_score integer,
                exhaustion_score integer,
                executable integer,
                evidence_json text not null,
                plan_json text
            )
            """,
        )
        con.execute(
            """
            create table if not exists strategy5_runs (
                run_id text primary key,
                cycle_id text,
                generated_at text,
                evidence_count integer,
                plan_count integer,
                executable_count integer,
                status text,
                output_path text
            )
            """,
        )


def _evidence_id(run_id: str | None, symbol: str) -> str:
    return f"{run_id or 'no_run'}:{symbol}:strategy5"


def build_strategy5_document(project_root: Path, *, run_id: str | None, cycle_id: str | None) -> dict[str, Any]:
    root = Path(project_root)
    now = to_iso_z(utc_now())
    factor = _factor_doc(root)
    base_doc = _base_trade_plan_doc(root)
    archive_path = _trade_plan_archive_path(root, run_id)
    factor_by_symbol = {str(row.get("symbol") or "").upper(): row for row in _items(factor) if row.get("symbol")}
    base_plans = [row for row in _items(base_doc) if row.get("symbol")]
    plans: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    for base in base_plans:
        symbol = str(base.get("symbol") or "").upper()
        ev = build_evidence_vector(factor_by_symbol.get(symbol, {"symbol": symbol, "move_side": base.get("decision")}))
        hyp = score_hypothesis(ev)
        legacy_side = str(base.get("decision") or "NO_TRADE").upper()
        agrees = legacy_side in {"LONG", "SHORT"} and hyp["shadow_hypothesis_side"] == legacy_side
        allow = (
            bool(base.get("executable"))
            and agrees
            and hyp["shadow_recommendation"] == "allow_if_legacy_agrees"
            and ev.get("evidence_quality", {}).get("usable") is True
        )
        evidence_id = _evidence_id(run_id, symbol)
        reason_codes = list(dict.fromkeys([*list(base.get("reason_codes") or []), *hyp["reason_codes"]]))
        if not bool(base.get("executable")) and ev.get("evidence_quality", {}).get("usable") is True:
            reason_codes.append("strategy5_base_trade_plan_not_executable")
        if not allow and bool(base.get("executable")):
            reason_codes.append("strategy5_shadow_blocked_not_promoted")
        plan = dict(base)
        plan["decision"] = legacy_side if agrees else str(hyp["shadow_hypothesis_side"])
        if not allow:
            plan["executable"] = False
            plan["action"] = "WAIT" if plan["decision"] in {"LONG", "SHORT"} else "NO_TRADE"
            plan["entry_mode"] = "WAIT_CONFIRMATION" if plan["decision"] in {"LONG", "SHORT"} else "NONE"
            if plan["action"] == "NO_TRADE":
                plan["estimated_entry_price"] = None
                plan["stop_loss"] = None
                plan["take_profit"] = None
                plan["risk_per_unit"] = None
                plan["reward_per_unit"] = None
                plan["rr"] = None
                plan["position_sizing"] = None
        plan["reason_codes"] = reason_codes
        guards = plan.get("guards") if isinstance(plan.get("guards"), dict) else {}
        plan["guards"] = {
            **guards,
            "line": "strategy5",
            "strategy5_evidence_id": evidence_id,
            "strategy5_shadow_hypothesis_side": hyp["shadow_hypothesis_side"],
            "strategy5_shadow_label": hyp["shadow_label"],
            "strategy5_shadow_recommendation": hyp["shadow_recommendation"],
            "strategy5_continuation_score": hyp["continuation_score"],
            "strategy5_exhaustion_score": hyp["exhaustion_score"],
            "strategy5_legacy_side": legacy_side,
            "strategy5_legacy_agrees": agrees,
            "strategy5_promotion_state": "display_only",
            "strategy5_allow_reason": "legacy_agrees_with_evidence" if allow else "shadow_or_disagree",
        }
        refs = plan.get("input_refs") if isinstance(plan.get("input_refs"), dict) else {}
        plan["input_refs"] = {
            **refs,
            "strategy5_evidence_id": evidence_id,
            "strategy5_source": SOURCE,
            "strategy5_trade_plan_latest_path": str(paths(root).latest_trade_plan),
            "strategy5_trade_plan_archive_path": str(archive_path) if archive_path is not None else None,
            "trade_plan_archive_path": str(archive_path) if archive_path is not None else None,
            "base_trade_plan_source": base_doc.get("source"),
            "base_trade_plan_run_id": base_doc.get("run_id"),
            "base_trade_plan_archive_path": refs.get("trade_plan_archive_path"),
            "factor_source": factor.get("source"),
            "factor_generated_at": factor.get("generated_at"),
            "factor_path": factor.get("_selected_path"),
            "factor_freshness_policy": factor.get("_freshness_policy"),
            "factor_snapshot_selected_reason": factor.get("_selected_reason"),
        }
        plans.append(plan)
        evidence_rows.append(
            {
                "evidence_id": evidence_id,
                "run_id": run_id,
                "cycle_id": cycle_id,
                "generated_at": now,
                "symbol": symbol,
                "trigger_side": hyp["shadow_hypothesis_side"],
                "legacy_side": legacy_side,
                "label": hyp["shadow_label"],
                "recommendation": hyp["shadow_recommendation"],
                "continuation_score": hyp["continuation_score"],
                "exhaustion_score": hyp["exhaustion_score"],
                "executable": bool(plan.get("executable")),
                "evidence": {**ev, **hyp},
                "plan": plan,
            },
        )
    doc = TradePlanLineDocument(
        generated_at=now,
        run_id=run_id,
        cycle_id=cycle_id,
        source="trade_plan_strategy5",
        micro_mode="strategy5_evidence",
        status="ok" if plans else "no_entries",
        count=len(plans),
        executable_count=sum(1 for row in plans if row.get("executable") is True),
        input_refs={
            "source": SOURCE,
            "factor_source": factor.get("source"),
            "factor_generated_at": factor.get("generated_at"),
            "factor_path": factor.get("_selected_path"),
            "factor_freshness_policy": factor.get("_freshness_policy"),
            "factor_snapshot_selected_reason": factor.get("_selected_reason"),
            "base_trade_plan_source": base_doc.get("source"),
            "base_trade_plan_run_id": base_doc.get("run_id"),
            "base_trade_plan_cycle_id": base_doc.get("cycle_id"),
            "strategy5_promotion_state": "display_only",
            "strategy5_contract": "normal_pipeline_line_no_micro_slot",
            "strategy5_trade_plan_latest_path": str(paths(root).latest_trade_plan),
            "strategy5_trade_plan_archive_path": str(archive_path) if archive_path is not None else None,
            "trade_plan_archive_path": str(archive_path) if archive_path is not None else None,
        },
        candidate_alignment={
            "base_count": len(base_plans),
            "factor_count": len(factor_by_symbol),
            "strategy5_shadow_only": True,
        },
        plans=plans,
    ).model_dump(mode="json")
    return {"trade_plan": doc, "evidence_rows": evidence_rows}


def write_strategy5_outputs(project_root: Path, *, run_id: str | None, cycle_id: str | None) -> dict[str, Any]:
    p = paths(Path(project_root))
    payload = build_strategy5_document(Path(project_root), run_id=run_id, cycle_id=cycle_id)
    doc = payload["trade_plan"]
    evidence_rows = payload["evidence_rows"]
    write_json_atomic(p.latest_trade_plan, doc)
    archive_path = _trade_plan_archive_path(Path(project_root), run_id)
    if archive_path is not None:
        write_json_atomic(archive_path, doc)
    write_json_atomic(
        p.latest_evidence,
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at": doc.get("generated_at"),
            "source": SOURCE,
            "run_id": run_id,
            "cycle_id": cycle_id,
            "count": len(evidence_rows),
            "items": [row["evidence"] | {"symbol": row["symbol"], "evidence_id": row["evidence_id"]} for row in evidence_rows],
        },
    )
    _init_db(p.db)
    with sqlite3.connect(p.db) as con:
        for row in evidence_rows:
            con.execute(
                """
                insert or replace into strategy5_evidence (
                    evidence_id, run_id, cycle_id, generated_at, symbol, trigger_side, legacy_side,
                    label, recommendation, continuation_score, exhaustion_score, executable,
                    evidence_json, plan_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["evidence_id"],
                    row["run_id"],
                    row["cycle_id"],
                    row["generated_at"],
                    row["symbol"],
                    row["trigger_side"],
                    row["legacy_side"],
                    row["label"],
                    row["recommendation"],
                    int(row["continuation_score"]),
                    int(row["exhaustion_score"]),
                    1 if row["executable"] else 0,
                    json.dumps(row["evidence"], ensure_ascii=False),
                    json.dumps(row["plan"], ensure_ascii=False),
                ),
            )
        con.execute(
            """
            insert or replace into strategy5_runs (
                run_id, cycle_id, generated_at, evidence_count, plan_count, executable_count, status, output_path
            ) values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                cycle_id,
                doc.get("generated_at"),
                len(evidence_rows),
                int(doc.get("count") or 0),
                int(doc.get("executable_count") or 0),
                doc.get("status"),
                str(archive_path or p.latest_trade_plan),
            ),
        )
    return {
        "status": doc.get("status"),
        "run_id": run_id,
        "cycle_id": cycle_id,
        "count": doc.get("count"),
        "executable_count": doc.get("executable_count"),
        "output_path": str(p.latest_trade_plan),
        "archive_path": str(archive_path) if archive_path is not None else None,
        "evidence_path": str(p.latest_evidence),
        "db_path": str(p.db),
    }


def run_strategy5_pipeline_safe(
    *,
    project_root: Path,
    run_id: str | None = None,
    cycle_id: str | None = None,
    stdout_json: bool = False,
) -> int:
    try:
        result = write_strategy5_outputs(Path(project_root), run_id=run_id, cycle_id=cycle_id)
        if stdout_json:
            print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception as exc:  # pragma: no cover - defensive command boundary
        if stdout_json:
            print(json.dumps({"status": "error", "reason": str(exc)}, ensure_ascii=False))
        return 1
