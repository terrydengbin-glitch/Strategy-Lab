"""STEP4.3A pre-decision candidate refresh."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import yaml
from pydantic import ValidationError

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object, write_json_bytes
from laoma_signal_engine.core.time_utils import age_sec_from_iso_z, to_iso_z, utc_now
from laoma_signal_engine.factors.models import FactorSnapshotDocument
from laoma_signal_engine.market.decision_refresh_models import (
    DecisionRefreshDocument,
    DecisionRefreshItem,
    DecisionRefreshStatus,
)
from laoma_signal_engine.market.futures_light_snapshot import run_fetch_light_snapshot_safe
from laoma_signal_engine.market.light_snapshot_models import FuturesLightSnapshotDocument, LightSnapshotItem
from laoma_signal_engine.market.market_entry_liquidity import run_fetch_market_entry_liquidity_safe
from laoma_signal_engine.market.market_entry_liquidity_models import MarketEntryLiquidityDocument


DEFAULT_MAX_REFRESH_AGE_SEC = 180
DEFAULT_MAX_LIQUIDITY_AGE_SEC = 180
DEFAULT_LONG_MAX_RANGE_POS = 0.82
DEFAULT_SHORT_MIN_RANGE_POS = 0.18


@dataclass(frozen=True)
class DecisionRefreshConfig:
    max_refresh_age_sec: int = DEFAULT_MAX_REFRESH_AGE_SEC
    max_liquidity_age_sec: int = DEFAULT_MAX_LIQUIDITY_AGE_SEC
    long_max_range_pos: float = DEFAULT_LONG_MAX_RANGE_POS
    short_min_range_pos: float = DEFAULT_SHORT_MIN_RANGE_POS


def load_decision_refresh_config(project_root: Path) -> DecisionRefreshConfig:
    cfg_path = project_root / "laoma_signal_engine" / "config" / "default.yaml"
    try:
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except OSError:
        raw = {}
    frag = raw.get("decision_refresh") or {}
    range_frag = frag.get("range_room") or {}
    return DecisionRefreshConfig(
        max_refresh_age_sec=int(frag.get("max_refresh_age_sec", DEFAULT_MAX_REFRESH_AGE_SEC)),
        max_liquidity_age_sec=int(frag.get("max_liquidity_age_sec", DEFAULT_MAX_LIQUIDITY_AGE_SEC)),
        long_max_range_pos=float(range_frag.get("long_max_range_pos", DEFAULT_LONG_MAX_RANGE_POS)),
        short_min_range_pos=float(range_frag.get("short_min_range_pos", DEFAULT_SHORT_MIN_RANGE_POS)),
    )


def _default_factor_path(root: Path) -> Path:
    return root / "DATA" / "factors" / "latest_factor_snapshot.json"


def _default_light_path(root: Path) -> Path:
    return root / "DATA" / "market" / "futures_light_snapshot.json"


def _default_refreshed_light_path(root: Path) -> Path:
    return root / "DATA" / "market" / "decision_refresh_light_snapshot.json"


def _line_refreshed_light_path(root: Path, line: str) -> Path:
    return root / "DATA" / "market" / f"decision_refresh_{line}_light_snapshot.json"


def _default_liquidity_path(root: Path) -> Path:
    return root / "DATA" / "market" / "latest_market_entry_liquidity.json"


def _default_refreshed_liquidity_path(root: Path) -> Path:
    return root / "DATA" / "market" / "decision_refresh_liquidity_snapshot.json"


def _line_refreshed_liquidity_path(root: Path, line: str) -> Path:
    return root / "DATA" / "market" / f"decision_refresh_{line}_liquidity_snapshot.json"


def _default_output_path(root: Path) -> Path:
    return root / "DATA" / "market" / "latest_decision_refresh_snapshot.json"


def line_output_path(root: Path, line: str) -> Path:
    return root / "DATA" / "market" / f"latest_decision_refresh_{line}_snapshot.json"


def _symbol_key(symbol: str) -> str:
    return symbol.strip().upper()


def _candidate_meta(factor: FactorSnapshotDocument) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in factor.items:
        out[_symbol_key(item.symbol)] = {
            "move_side": item.move_side,
            "source_state": item.source_state,
        }
    return out


def _liquidity_by_symbol(path: Path) -> tuple[dict[str, dict[str, Any]], str | None]:
    if not path.is_file():
        return {}, None
    doc = MarketEntryLiquidityDocument.model_validate(read_json_object(path))
    out: dict[str, dict[str, Any]] = {}
    for it in doc.items:
        row = it.model_dump(mode="json")
        row["generated_at"] = doc.generated_at
        out[_symbol_key(it.symbol)] = row
    return out, doc.generated_at


def _dict(model: Any) -> dict[str, Any]:
    return model.model_dump(mode="json") if hasattr(model, "model_dump") else dict(model)


def _direction_valid(move_side: str | None, light: LightSnapshotItem) -> bool:
    side = (move_side or "").lower()
    p_ret = light.primary_15m.price_ret
    t_ret = light.trigger_5m.price_ret
    acc = light.trigger_5m.acceleration_state
    if side == "up":
        return (p_ret is not None and p_ret > 0) or acc == "up" or (t_ret is not None and t_ret > 0)
    if side == "down":
        return (p_ret is not None and p_ret < 0) or acc == "down" or (t_ret is not None and t_ret < 0)
    return False


def _range_room_ok(
    move_side: str | None,
    light: LightSnapshotItem,
    *,
    long_max_range_pos: float,
    short_min_range_pos: float,
) -> bool:
    rp = light.primary_15m.range_pos
    if rp is None:
        return False
    side = (move_side or "").lower()
    if side == "up":
        return rp <= long_max_range_pos
    if side == "down":
        return rp >= short_min_range_pos
    return False


def _side_liquidity_ok(move_side: str | None, liquidity: dict[str, Any] | None) -> bool | None:
    if liquidity is None:
        return None
    side = (move_side or "").lower()
    if side == "up":
        if "buy_liquidity_ok_for_market_entry" in liquidity:
            return bool(liquidity.get("buy_liquidity_ok_for_market_entry"))
    elif side == "down":
        if "sell_liquidity_ok_for_market_entry" in liquidity:
            return bool(liquidity.get("sell_liquidity_ok_for_market_entry"))
    return bool(liquidity.get("liquidity_ok_for_market_entry"))


def _side_liquidity_reasons(move_side: str | None, liquidity: dict[str, Any] | None) -> list[str]:
    if liquidity is None:
        return []
    side = (move_side or "").lower()
    if side == "up":
        return list(liquidity.get("buy_reason_codes") or liquidity.get("reason_codes") or [])
    if side == "down":
        return list(liquidity.get("sell_reason_codes") or liquidity.get("reason_codes") or [])
    return list(liquidity.get("reason_codes") or [])


def _item_reason_codes(
    *,
    light: LightSnapshotItem,
    refresh_age_sec: int,
    max_refresh_age_sec: int,
    direction_still_valid: bool,
    range_room_ok: bool,
    liquidity: dict[str, Any] | None,
    move_side: str | None,
    liquidity_age_sec: int | None,
    max_liquidity_age_sec: int,
) -> list[str]:
    reasons: list[str] = []
    if refresh_age_sec > max_refresh_age_sec:
        reasons.append("refresh_stale")
    dq = light.data_quality
    if not (dq.kline_1m_ready and dq.kline_5m_ready and dq.kline_15m_ready):
        reasons.append("kline_not_ready")
    if not direction_still_valid:
        reasons.append("direction_invalid_after_refresh")
    if not range_room_ok:
        reasons.append("range_room_insufficient_after_refresh")
    if _side_liquidity_ok(move_side, liquidity) is False:
        reasons.append("liquidity_not_ok")
        reasons.extend(_side_liquidity_reasons(move_side, liquidity))
    if liquidity_age_sec is None:
        reasons.append("liquidity_missing")
    elif liquidity_age_sec > max_liquidity_age_sec:
        reasons.append("liquidity_stale")
    return reasons


def build_decision_refresh_document(
    *,
    factor: FactorSnapshotDocument,
    light: FuturesLightSnapshotDocument,
    liquidity_by_symbol: dict[str, dict[str, Any]] | None = None,
    liquidity_generated_at: str | None = None,
    max_refresh_age_sec: int = DEFAULT_MAX_REFRESH_AGE_SEC,
    max_liquidity_age_sec: int = DEFAULT_MAX_LIQUIDITY_AGE_SEC,
    long_max_range_pos: float = DEFAULT_LONG_MAX_RANGE_POS,
    short_min_range_pos: float = DEFAULT_SHORT_MIN_RANGE_POS,
    line: str | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    input_refs: dict[str, Any] | None = None,
    compat_view: bool = False,
    canonical_per_line_path: str | None = None,
) -> DecisionRefreshDocument:
    now = utc_now()
    now_iso = to_iso_z(now)
    meta_by_symbol = _candidate_meta(factor)
    light_by_symbol = {_symbol_key(it.symbol): it for it in light.items}
    liq_by_symbol = liquidity_by_symbol or {}
    items: list[DecisionRefreshItem] = []
    for sym, meta in meta_by_symbol.items():
        light_item = light_by_symbol.get(sym)
        if light_item is None:
            continue
        refresh_age_sec = age_sec_from_iso_z(light.generated_at, now=now)
        direction_ok = _direction_valid(meta.get("move_side"), light_item)
        room_ok = _range_room_ok(
            meta.get("move_side"),
            light_item,
            long_max_range_pos=long_max_range_pos,
            short_min_range_pos=short_min_range_pos,
        )
        liquidity = liq_by_symbol.get(sym)
        liquidity_ok = _side_liquidity_ok(meta.get("move_side"), liquidity)
        liquidity_age_sec = None
        if liquidity_generated_at:
            liquidity_age_sec = age_sec_from_iso_z(liquidity_generated_at, now=now)
        reasons = _item_reason_codes(
            light=light_item,
            refresh_age_sec=refresh_age_sec,
            max_refresh_age_sec=max_refresh_age_sec,
            direction_still_valid=direction_ok,
            range_room_ok=room_ok,
            liquidity=liquidity,
            move_side=meta.get("move_side"),
            liquidity_age_sec=liquidity_age_sec,
            max_liquidity_age_sec=max_liquidity_age_sec,
        )
        items.append(
            DecisionRefreshItem(
                symbol=sym,
                base_asset=light_item.base_asset,
                move_side=meta.get("move_side"),
                source_state=meta.get("source_state"),
                last_price=light_item.last_price,
                refresh_age_sec=refresh_age_sec,
                direction_still_valid=direction_ok,
                range_room_ok=room_ok,
                range_gate={
                    "ok": room_ok,
                    "move_side": meta.get("move_side"),
                    "range_pos": light_item.primary_15m.range_pos,
                    "long_max_range_pos": long_max_range_pos,
                    "short_min_range_pos": short_min_range_pos,
                },
                liquidity_ok=liquidity_ok,
                liquidity_age_sec=liquidity_age_sec,
                reason_codes=reasons,
                primary_15m=_dict(light_item.primary_15m),
                trigger_5m=_dict(light_item.trigger_5m),
                entry_1m=_dict(light_item.entry_1m),
                background=_dict(light_item.background),
                data_quality=_dict(light_item.data_quality),
                liquidity=liquidity,
            ),
        )
    status: DecisionRefreshStatus
    if not meta_by_symbol:
        status = "no_candidates"
    elif not items:
        status = "error"
    elif any("refresh_stale" in it.reason_codes for it in items):
        status = "stale_input"
    elif any("liquidity_stale" in it.reason_codes or "liquidity_missing" in it.reason_codes for it in items):
        status = "partial"
    elif len(items) < len(meta_by_symbol):
        status = "partial"
    else:
        status = "ok"
    return DecisionRefreshDocument(
        schema_version="1.1" if line or run_id or cycle_id or input_refs or compat_view else "1.0",
        generated_at=now_iso,
        line=line,
        run_id=run_id,
        cycle_id=cycle_id,
        input_refs=input_refs or {},
        compat_view=compat_view,
        canonical_per_line_path=canonical_per_line_path,
        status=status,
        input_light_generated_at=light.generated_at,
        input_factor_generated_at=factor.generated_at,
        input_liquidity_generated_at=liquidity_generated_at,
        max_refresh_age_sec=max_refresh_age_sec,
        max_liquidity_age_sec=max_liquidity_age_sec,
        long_max_range_pos=long_max_range_pos,
        short_min_range_pos=short_min_range_pos,
        candidate_count=len(meta_by_symbol),
        refreshed_count=len(items),
        stale_count=sum(1 for it in items if "refresh_stale" in it.reason_codes),
        items=items,
    )


def run_pre_decision_candidate_refresh_safe(
    *,
    project_root: Path | None = None,
    factor_path: Path | None = None,
    light_path: Path | None = None,
    liquidity_path: Path | None = None,
    output_path: Path | None = None,
    fetch_latest: bool = True,
    fetch_mode: str = "async",
    max_concurrency: int | None = None,
    max_refresh_age_sec: int = DEFAULT_MAX_REFRESH_AGE_SEC,
    max_liquidity_age_sec: int = DEFAULT_MAX_LIQUIDITY_AGE_SEC,
    refresh_liquidity: bool = True,
    line: str | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    stdout_json: bool = False,
) -> int:
    root = (project_root or Path.cwd()).resolve()
    factor_p = factor_path or _default_factor_path(root)
    light_p = light_path or _default_light_path(root)
    liquidity_p = liquidity_path or _default_liquidity_path(root)
    out_p = output_path or (line_output_path(root, line) if line else _default_output_path(root))
    try:
        cfg = load_decision_refresh_config(root)
        if max_refresh_age_sec == DEFAULT_MAX_REFRESH_AGE_SEC:
            max_refresh_age_sec = cfg.max_refresh_age_sec
        if max_liquidity_age_sec == DEFAULT_MAX_LIQUIDITY_AGE_SEC:
            max_liquidity_age_sec = cfg.max_liquidity_age_sec
        factor = FactorSnapshotDocument.model_validate(read_json_object(factor_p))
        symbols = sorted(_candidate_meta(factor))
        if fetch_latest and symbols:
            refreshed_light = _line_refreshed_light_path(root, line) if line else _default_refreshed_light_path(root)
            code = run_fetch_light_snapshot_safe(
                project_root=root,
                symbols_filter=symbols,
                output_path=refreshed_light,
                fetch_mode=fetch_mode,
                max_concurrency=max_concurrency,
            )
            if code != EXIT_SUCCESS:
                return code
            light_p = refreshed_light
            if refresh_liquidity:
                refreshed_liquidity = (
                    _line_refreshed_liquidity_path(root, line)
                    if line
                    else _default_refreshed_liquidity_path(root)
                )
                code = run_fetch_market_entry_liquidity_safe(
                    project_root=root,
                    light_path=refreshed_light,
                    output_path=refreshed_liquidity,
                    symbols=symbols,
                )
                if code != EXIT_SUCCESS:
                    return code
                liquidity_p = refreshed_liquidity
        light = FuturesLightSnapshotDocument.model_validate(read_json_object(light_p))
        liquidity, liquidity_generated_at = _liquidity_by_symbol(liquidity_p)
        doc = build_decision_refresh_document(
            factor=factor,
            light=light,
            liquidity_by_symbol=liquidity,
            liquidity_generated_at=liquidity_generated_at,
            max_refresh_age_sec=max_refresh_age_sec,
            max_liquidity_age_sec=max_liquidity_age_sec,
            long_max_range_pos=cfg.long_max_range_pos,
            short_min_range_pos=cfg.short_min_range_pos,
            line=line,
            run_id=run_id,
            cycle_id=cycle_id,
            input_refs={
                "factor_generated_at": factor.generated_at,
                "light_generated_at": light.generated_at,
                "liquidity_generated_at": liquidity_generated_at,
                "factor_path": str(factor_p),
                "light_path": str(light_p),
                "liquidity_path": str(liquidity_p) if liquidity_p else None,
            },
        )
        doc = doc.model_copy(
            update={
                "input_refs": {
                    **doc.input_refs,
                    "refresh_snapshot_path": str(out_p),
                },
            },
        )
        out_p.parent.mkdir(parents=True, exist_ok=True)
        write_json_bytes(out_p, doc.model_dump(mode="json"))
        if line:
            compat_doc = doc.model_copy(
                update={
                    "compat_view": True,
                    "canonical_per_line_path": str(out_p),
                },
            )
            write_json_bytes(_default_output_path(root), compat_doc.model_dump(mode="json"))
            if light_p != _default_refreshed_light_path(root) and light_p.is_file():
                write_json_bytes(_default_refreshed_light_path(root), read_json_object(light_p))
            if liquidity_p != _default_refreshed_liquidity_path(root) and liquidity_p.is_file():
                write_json_bytes(_default_refreshed_liquidity_path(root), read_json_object(liquidity_p))
        if stdout_json:
            print(
                json.dumps(
                    {
                        "step": "STEP4.3A",
                        "status": doc.status,
                        "candidate_count": doc.candidate_count,
                        "refreshed_count": doc.refreshed_count,
                        "stale_count": doc.stale_count,
                        "output": str(out_p),
                        "line": line,
                        "run_id": run_id,
                        "cycle_id": cycle_id,
                    },
                    ensure_ascii=False,
                ),
            )
        return EXIT_SUCCESS
    except (OSError, ValueError, ValidationError) as e:
        print(f"[ERROR] pre decision candidate refresh failed: {e}", file=sys.stderr)
        return EXIT_CONFIG if isinstance(e, ValidationError) else EXIT_INTERNAL
