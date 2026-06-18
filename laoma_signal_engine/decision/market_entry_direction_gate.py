"""STEP4.3 market-entry direction gate."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object, write_json_bytes
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.decision.market_entry_models import (
    MarketEntryDecisionKind,
    MarketEntryDirectionDocument,
    MarketEntryDirectionItem,
)
from laoma_signal_engine.factors.models import FactorSnapshotDocument, FactorSnapshotItem
from laoma_signal_engine.market.decision_refresh_models import DecisionRefreshDocument, DecisionRefreshItem
from laoma_signal_engine.micro.assembly.models import LatestMicroFeaturesDocument, MicroFeatureItem


@dataclass(frozen=True)
class MarketEntryDirectionGateConfig:
    min_suitability_score: int = 55
    watch_min_suitability_score: int = 75
    require_fast_micro_ready: bool = True
    allow_watch_market_entry: bool = False
    require_refresh_fresh: bool = True


def _default_factor_path(root: Path) -> Path:
    return root / "DATA" / "factors" / "latest_factor_snapshot.json"


def _default_refresh_path(root: Path) -> Path:
    return root / "DATA" / "market" / "latest_decision_refresh_snapshot.json"


def _default_micro_path(root: Path) -> Path:
    return root / "DATA" / "micro" / "latest_micro_features.json"


def _default_output_path(root: Path) -> Path:
    return root / "DATA" / "decisions" / "latest_market_entry_direction_decisions.json"


def load_market_entry_direction_gate_config(
    project_root: Path,
    *,
    allow_watch_market_entry_override: bool | None = None,
) -> MarketEntryDirectionGateConfig:
    cfg_path = project_root / "laoma_signal_engine" / "config" / "default.yaml"
    try:
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except OSError:
        raw = {}
    frag = raw.get("market_entry_direction") or {}
    allow_watch = bool(frag.get("allow_watch_market_entry", False))
    if allow_watch_market_entry_override is not None:
        allow_watch = bool(allow_watch_market_entry_override)
    return MarketEntryDirectionGateConfig(
        min_suitability_score=int(frag.get("min_suitability_score", 55)),
        watch_min_suitability_score=int(frag.get("watch_min_suitability_score", 75)),
        allow_watch_market_entry=allow_watch,
    )


def _key(symbol: str) -> str:
    return symbol.strip().upper()


def _micro_fast_ready(item: MicroFeatureItem | None) -> bool:
    if item is None:
        return False
    if item.micro_fast_quality is not None:
        return item.micro_fast_quality.ready
    return item.micro_quality.ready


def _micro_full_ready(item: MicroFeatureItem | None) -> bool:
    if item is None:
        return False
    if item.micro_full_quality is not None:
        return item.micro_full_quality.ready
    return item.micro_quality.ready


def _micro_alignment_ok(move_side: str, micro: MicroFeatureItem | None) -> bool:
    _ = move_side
    if micro is None:
        return False
    if micro.micro_fast_signal is None:
        return False
    return micro.micro_fast_signal.micro_direction_confirmed


def _factor_micro_alignment_ok(item: FactorSnapshotItem) -> bool:
    signal = item.micro_fast_signal
    if signal is None:
        return False
    return signal.micro_direction_confirmed and signal.micro_exec_allowed


def _factor_micro_signal_guards(item: FactorSnapshotItem) -> dict[str, Any]:
    signal = item.micro_fast_signal
    if signal is None:
        return {
            "micro_signal_missing": True,
            "micro_signal_usable": False,
            "micro_direction_confirmed": False,
            "micro_exec_allowed": False,
            "micro_alignment_state": "insufficient",
            "micro_strength": "none",
            "micro_confirmation_level": "none",
            "micro_exec_allowed_reason": "",
            "micro_confidence_score": 0,
            "micro_confirmation_penalty_bps": 0.0,
        }
    return {
        "micro_signal_missing": False,
        "micro_signal_usable": signal.micro_signal_usable,
        "micro_direction_confirmed": signal.micro_direction_confirmed,
        "micro_exec_allowed": signal.micro_exec_allowed,
        "micro_alignment_state": signal.micro_alignment_state,
        "micro_strength": signal.micro_strength,
        "micro_confirmation_level": signal.micro_confirmation_level,
        "micro_exec_allowed_reason": signal.micro_exec_allowed_reason,
        "micro_confidence_score": signal.micro_confidence_score,
        "micro_confirmation_penalty_bps": signal.micro_confirmation_penalty_bps,
    }


def _direction_decision(move_side: str) -> tuple[MarketEntryDecisionKind, str, str]:
    side = move_side.lower()
    if side == "up":
        return "LONG_MARKET", "LONG", "ENTER_MARKET"
    if side == "down":
        return "SHORT_MARKET", "SHORT", "ENTER_MARKET"
    return "NO_MARKET_ENTRY", "NONE", "NO_TRADE"


def decide_market_entry_direction_item(
    item: FactorSnapshotItem,
    *,
    refresh: DecisionRefreshItem | None,
    micro: MicroFeatureItem | None,
    factor_doc: FactorSnapshotDocument,
    refresh_doc: DecisionRefreshDocument,
    micro_doc: LatestMicroFeaturesDocument,
    cfg: MarketEntryDirectionGateConfig | None = None,
) -> MarketEntryDirectionItem:
    c = cfg or MarketEntryDirectionGateConfig()
    reasons: list[str] = []
    guards: dict[str, Any] = {}

    side = item.move_side.strip().lower()
    if side not in ("up", "down"):
        reasons.append("no_direction")

    score = int(item.market_entry_suitability_score or 0)
    if score < c.min_suitability_score:
        reasons.append("market_entry_score_too_low")
    if item.market_entry_suitability == "avoid":
        reasons.append("market_entry_suitability_avoid")
    reasons.extend(item.market_entry_reason_codes)

    if item.source_state == "watch_candidate":
        if not c.allow_watch_market_entry:
            reasons.append("watch_market_entry_not_allowed")
        elif score < c.watch_min_suitability_score:
            reasons.append("watch_market_entry_score_too_low")

    if refresh is None:
        reasons.append("refresh_missing")
        refresh_fresh = False
        direction_valid = False
        range_room_ok = False
        liquidity_ok = False
    else:
        refresh_fresh = "refresh_stale" not in refresh.reason_codes
        direction_valid = refresh.direction_still_valid
        range_room_ok = refresh.range_room_ok
        liquidity_ok = refresh.liquidity_ok is not False
        if c.require_refresh_fresh and not refresh_fresh:
            reasons.append("refresh_stale")
        if not direction_valid:
            reasons.append("direction_invalid_after_refresh")
        if not range_room_ok:
            reasons.append("range_room_insufficient_after_refresh")
        if not liquidity_ok:
            reasons.append("liquidity_not_ok")
        if "liquidity_stale" in refresh.reason_codes:
            reasons.append("liquidity_stale")
        if "liquidity_missing" in refresh.reason_codes:
            reasons.append("liquidity_missing")

    fast_ready = _micro_fast_ready(micro)
    full_ready = _micro_full_ready(micro)
    micro_align = _factor_micro_alignment_ok(item)
    micro_signal_guards = _factor_micro_signal_guards(item)
    micro_signal = item.micro_fast_signal
    if micro is None:
        reasons.append("micro_missing")
    if c.require_fast_micro_ready and not fast_ready:
        reasons.append("micro_fast_not_ready")
    if fast_ready and not micro_align:
        reasons.append("micro_fast_not_confirmed")

    guards.update(
        {
            "market_entry_suitability_score": score,
            "market_entry_suitability": item.market_entry_suitability,
            "refresh_fresh": refresh_fresh,
            "direction_still_valid": direction_valid,
            "range_room_ok": range_room_ok,
            "liquidity_ok": liquidity_ok,
            "micro_fast_ready": fast_ready,
            "micro_full_ready": full_ready,
            "micro_alignment_ok": micro_align,
            **micro_signal_guards,
            "allow_watch_market_entry": c.allow_watch_market_entry,
            "watch_min_suitability_score": c.watch_min_suitability_score,
        },
    )

    can_enter = not reasons
    if can_enter:
        decision, direction, action = _direction_decision(side)
    else:
        decision, direction, action = "NO_MARKET_ENTRY", "NONE", "NO_TRADE"
    confidence = max(0, min(100, int((score + item.scan_score) / 2)))
    return MarketEntryDirectionItem(
        symbol=item.symbol,
        decision_tf=item.decision_tf,
        decision=decision,
        direction=direction,  # type: ignore[arg-type]
        action=action,  # type: ignore[arg-type]
        confidence=confidence,
        reason_codes=sorted(set(reasons)),
        guards=guards,
        input_refs={
            "factor_generated_at": factor_doc.generated_at,
            "refresh_generated_at": refresh_doc.generated_at,
            "micro_generated_at": micro_doc.generated_at,
        },
    )


def build_market_entry_direction_document(
    *,
    factor: FactorSnapshotDocument,
    refresh: DecisionRefreshDocument,
    micro: LatestMicroFeaturesDocument,
    generated_at: str,
    cfg: MarketEntryDirectionGateConfig | None = None,
) -> MarketEntryDirectionDocument:
    if factor.count == 0:
        status = "no_candidates"
        decisions: list[MarketEntryDirectionItem] = []
    elif refresh.status in ("stale_input", "error"):
        status = "stale_input" if refresh.status == "stale_input" else "error"
        refresh_by_symbol = {_key(it.symbol): it for it in refresh.items}
        micro_by_symbol = {_key(it.symbol): it for it in micro.items}
        decisions = [
            decide_market_entry_direction_item(
                it,
                refresh=refresh_by_symbol.get(_key(it.symbol)),
                micro=micro_by_symbol.get(_key(it.symbol)),
                factor_doc=factor,
                refresh_doc=refresh,
                micro_doc=micro,
                cfg=cfg,
            )
            for it in factor.items
        ]
    else:
        refresh_by_symbol = {_key(it.symbol): it for it in refresh.items}
        micro_by_symbol = {_key(it.symbol): it for it in micro.items}
        decisions = [
            decide_market_entry_direction_item(
                it,
                refresh=refresh_by_symbol.get(_key(it.symbol)),
                micro=micro_by_symbol.get(_key(it.symbol)),
                factor_doc=factor,
                refresh_doc=refresh,
                micro_doc=micro,
                cfg=cfg,
            )
            for it in factor.items
        ]
        status = "partial" if any(d.decision == "NO_MARKET_ENTRY" for d in decisions) else "ok"
    return MarketEntryDirectionDocument(
        generated_at=generated_at,
        status=status,  # type: ignore[arg-type]
        count=len(decisions),
        decisions=decisions,
    )


def run_apply_market_entry_direction_gate_safe(
    *,
    project_root: Path | None = None,
    factor_path: Path | None = None,
    refresh_path: Path | None = None,
    micro_path: Path | None = None,
    output_path: Path | None = None,
    stdout_json: bool = False,
    allow_watch_market_entry: bool | None = None,
) -> int:
    root = (project_root or Path.cwd()).resolve()
    factor_p = factor_path or _default_factor_path(root)
    refresh_p = refresh_path or _default_refresh_path(root)
    micro_p = micro_path or _default_micro_path(root)
    out_p = output_path or _default_output_path(root)
    try:
        factor = FactorSnapshotDocument.model_validate(read_json_object(factor_p))
        refresh = DecisionRefreshDocument.model_validate(read_json_object(refresh_p))
        micro = LatestMicroFeaturesDocument.model_validate(read_json_object(micro_p))
        doc = build_market_entry_direction_document(
            factor=factor,
            refresh=refresh,
            micro=micro,
            generated_at=to_iso_z(utc_now()),
            cfg=load_market_entry_direction_gate_config(
                root,
                allow_watch_market_entry_override=allow_watch_market_entry,
            ),
        )
        out_p.parent.mkdir(parents=True, exist_ok=True)
        write_json_bytes(out_p, doc.model_dump(mode="json"))
        if stdout_json:
            print(
                json.dumps(
                    {
                        "step": "STEP4.3",
                        "status": doc.status,
                        "count": doc.count,
                        "enter_count": sum(1 for d in doc.decisions if d.action == "ENTER_MARKET"),
                        "output": str(out_p),
                    },
                    ensure_ascii=False,
                ),
            )
        return EXIT_SUCCESS
    except (OSError, ValueError, ValidationError) as e:
        print(f"[ERROR] market entry direction gate failed: {e}", file=sys.stderr)
        return EXIT_CONFIG if isinstance(e, ValidationError) else EXIT_INTERNAL
