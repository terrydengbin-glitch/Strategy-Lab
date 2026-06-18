"""Pure predicates: parsed micro JSON dict vs WaitUntilReadyConfig."""

from __future__ import annotations

from typing import Any, Mapping
from datetime import datetime

from laoma_signal_engine.core.time_utils import parse_iso_z
from laoma_signal_engine.micro.wait_until_ready.config import WaitUntilReadyConfig


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def global_preconditions(micro: Mapping[str, Any], cfg: WaitUntilReadyConfig) -> bool:
    if micro.get("status") not in ("ok", "observing_stale_targets"):
        return False
    if cfg.require_target_fresh and micro.get("target_status") not in ("fresh", "stale_observing"):
        return False
    if cfg.require_ws_connected and micro.get("ws_status") != "connected":
        return False
    age = micro.get("last_ws_message_age_sec")
    if age is None or not isinstance(age, (int, float)):
        return False
    if float(age) > float(cfg.ws_message_max_age_sec):
        return False
    return True


def mode_satisfied(micro: Mapping[str, Any], cfg: WaitUntilReadyConfig) -> bool:
    items = micro.get("items")
    if not isinstance(items, list):
        return False
    mode = cfg.mode
    if mode == "min_ready_count":
        rc = micro.get("ready_count")
        return isinstance(rc, int) and rc >= int(cfg.min_ready_count)
    if mode == "min_fast_ready_count":
        rc = micro.get("fast_ready_count")
        if isinstance(rc, int):
            return rc >= int(cfg.min_ready_count)
        n = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            mq = it.get("micro_fast_quality")
            if isinstance(mq, dict) and mq.get("ready") is True:
                n += 1
        return n >= int(cfg.min_ready_count)
    if mode == "min_full_ready_count":
        rc = micro.get("full_ready_count")
        if isinstance(rc, int):
            return rc >= int(cfg.min_ready_count)
        n = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            mq = it.get("micro_full_quality")
            if isinstance(mq, dict) and mq.get("ready") is True:
                n += 1
        return n >= int(cfg.min_ready_count)
    if mode == "min_ready_strong":
        need = int(cfg.min_ready_strong_count)
        n = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            if str(it.get("source_state") or "") != "strong_candidate":
                continue
            mq = it.get("micro_quality")
            if isinstance(mq, dict) and mq.get("ready") is True:
                n += 1
        return n >= need
    if mode == "symbols":
        syms = [normalize_symbol(s) for s in cfg.symbols]
        if not syms:
            return False
        by_sym: dict[str, dict[str, Any]] = {}
        for it in items:
            if not isinstance(it, dict):
                continue
            sym = normalize_symbol(str(it.get("symbol") or ""))
            if sym:
                by_sym[sym] = it
        found = [by_sym.get(s) for s in syms]
        if cfg.require_symbols == "all":
            if any(x is None for x in found):
                return False
            return all(
                isinstance(x, dict) and isinstance(x.get("micro_quality"), dict) and x["micro_quality"].get("ready") is True
                for x in found
            )
        for x in found:
            if x is None:
                continue
            mq = x.get("micro_quality")
            if isinstance(mq, dict) and mq.get("ready") is True:
                return True
        return False
    if mode == "strict_coverage":
        if not cfg.strict_coverage_min_by_stream:
            return bool(items)
        for it in items:
            if not isinstance(it, dict):
                return False
            mq = it.get("micro_quality")
            cov = mq.get("coverage") if isinstance(mq, dict) else None
            if not isinstance(cov, dict):
                return False
            for stream, need in cfg.strict_coverage_min_by_stream:
                block = cov.get(stream)
                if not isinstance(block, dict):
                    return False
                got = block.get("covered_seconds")
                if not isinstance(got, (int, float)) or int(got) < int(need):
                    return False
        return len(items) > 0
    return False


def micro_satisfies_wait(micro: Mapping[str, Any], cfg: WaitUntilReadyConfig) -> bool:
    if not global_preconditions(micro, cfg):
        return False
    return mode_satisfied(micro, cfg)


def _ready(block: Any) -> bool:
    return isinstance(block, Mapping) and block.get("ready") is True


def scope_micro_to_expected_symbols(
    micro: Mapping[str, Any],
    expected_symbols: set[str],
    *,
    target_set_id: str = "",
    expected_target_generated_at: str = "",
) -> dict[str, Any]:
    """Return a current-target-set view of a daemon/global micro document."""
    scoped = dict(micro)
    raw_items = micro.get("items")
    items = [it for it in raw_items if isinstance(it, dict)] if isinstance(raw_items, list) else []
    expected = {normalize_symbol(s) for s in expected_symbols if normalize_symbol(s)}
    if not expected:
        scoped["scope"] = str(micro.get("scope") or "global_daemon_pool")
        scoped["ready_scope"] = str(micro.get("ready_scope") or "global_daemon_pool")
        return scoped

    by_symbol = {
        normalize_symbol(str(it.get("symbol") or "")): it
        for it in items
        if normalize_symbol(str(it.get("symbol") or ""))
    }
    scoped_items = [by_symbol[sym] for sym in sorted(expected) if sym in by_symbol]
    ready_count = sum(1 for it in scoped_items if _ready(it.get("micro_quality")))
    fast_ready_count = sum(1 for it in scoped_items if _ready(it.get("micro_fast_quality")))
    full_ready_count = sum(1 for it in scoped_items if _ready(it.get("micro_full_quality")))
    got = set(by_symbol)

    scoped.update(
        {
            "scope": "target_set",
            "ready_scope": "target_set",
            "target_set_id": target_set_id,
            "expected_target_generated_at": expected_target_generated_at,
            "expected_symbol_count": len(expected),
            "target_count": len(expected),
            "global_symbol_count": len(items),
            "global_ready_count": micro.get("ready_count"),
            "global_fast_ready_count": micro.get("fast_ready_count"),
            "global_full_ready_count": micro.get("full_ready_count"),
            "missing_target_symbols": sorted(expected - got),
            "extra_global_symbol_count": max(0, len(got - expected)),
            "items": scoped_items,
            "symbol_count": len(scoped_items),
            "ready_count": ready_count,
            "fast_ready_count": fast_ready_count,
            "full_ready_count": full_ready_count,
            "not_ready_count": max(0, len(scoped_items) - ready_count),
        },
    )
    return scoped


def micro_current_run_skip_reason(
    micro: Mapping[str, Any],
    *,
    started_at: datetime,
    expected_target_generated_at: str,
    expected_symbols: set[str],
) -> str:
    """Return empty string when this micro doc belongs to the current wait run."""
    target_gen = str(micro.get("target_generated_at") or "").strip()
    if expected_target_generated_at and target_gen != expected_target_generated_at:
        return "target_generated_at_mismatch"

    micro_gen = str(micro.get("generated_at") or "").strip()
    try:
        micro_dt = parse_iso_z(micro_gen)
    except (TypeError, ValueError):
        return "micro_generated_at_invalid"
    min_dt = started_at.astimezone(micro_dt.tzinfo)
    if expected_target_generated_at:
        try:
            target_dt = parse_iso_z(expected_target_generated_at)
            min_dt = min(min_dt, target_dt.astimezone(micro_dt.tzinfo))
        except (TypeError, ValueError):
            pass
    if micro_dt < min_dt:
        return "micro_generated_before_wait_start"

    if expected_symbols:
        items = micro.get("items")
        if not isinstance(items, list):
            return "items_missing"
        got = {
            normalize_symbol(str(it.get("symbol") or ""))
            for it in items
            if isinstance(it, dict) and normalize_symbol(str(it.get("symbol") or ""))
        }
        if not expected_symbols.issubset(got):
            return "target_symbols_mismatch"
    return ""


def micro_satisfies_current_run_wait(
    micro: Mapping[str, Any],
    cfg: WaitUntilReadyConfig,
    *,
    started_at: datetime,
    expected_target_generated_at: str,
    expected_symbols: set[str],
) -> bool:
    if micro_current_run_skip_reason(
        micro,
        started_at=started_at,
        expected_target_generated_at=expected_target_generated_at,
        expected_symbols=expected_symbols,
    ):
        return False
    scoped = scope_micro_to_expected_symbols(
        micro,
        expected_symbols,
        expected_target_generated_at=expected_target_generated_at,
    )
    return micro_satisfies_wait(scoped, cfg)
