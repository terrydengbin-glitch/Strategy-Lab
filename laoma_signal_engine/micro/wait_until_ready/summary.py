"""Aggregates for STEP3.8D run reports."""

from __future__ import annotations

from typing import Any


def _item_ready(item: dict[str, Any]) -> bool:
    mq = item.get("micro_quality")
    return isinstance(mq, dict) and mq.get("ready") is True


def ready_symbol_lists(items: list[dict[str, Any]]) -> tuple[list[str], list[str], list[str]]:
    """Returns (all_ready, strong_ready, watchish_ready)."""
    all_r: list[str] = []
    strong_r: list[str] = []
    watch_r: list[str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or "").strip().upper()
        if not sym or not _item_ready(it):
            continue
        all_r.append(sym)
        ss = str(it.get("source_state") or "")
        if ss == "strong_candidate":
            strong_r.append(sym)
        else:
            watch_r.append(sym)
    return all_r, strong_r, watch_r


def build_not_ready_summary(
    items: list[dict[str, Any]],
    *,
    coverage_top_n: int = 5,
    reason_top_n: int = 10,
) -> dict[str, Any]:
    by_ss: dict[str, dict[str, int]] = {}
    by_tier: dict[str, dict[str, int]] = {}
    reason_counts: dict[str, int] = {}
    not_ready_syms: list[str] = []
    cov_rows: list[dict[str, Any]] = []

    for it in items:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or "").strip().upper()
        ss = str(it.get("source_state") or "unknown")
        tier = str(it.get("tier") or "unknown")
        if ss not in by_ss:
            by_ss[ss] = {"ready": 0, "not_ready": 0}
        if tier not in by_tier:
            by_tier[tier] = {"ready": 0, "not_ready": 0}
        if _item_ready(it):
            by_ss[ss]["ready"] += 1
            by_tier[tier]["ready"] += 1
        else:
            by_ss[ss]["not_ready"] += 1
            by_tier[tier]["not_ready"] += 1
            if sym:
                not_ready_syms.append(sym)
            mq = it.get("micro_quality") if isinstance(it.get("micro_quality"), dict) else {}
            for code in mq.get("reason_codes") or []:
                c = str(code)
                reason_counts[c] = reason_counts.get(c, 0) + 1
            cov = mq.get("coverage") if isinstance(mq.get("coverage"), dict) else {}
            agg = cov.get("aggTrade") if isinstance(cov.get("aggTrade"), dict) else {}
            book = cov.get("bookTicker") if isinstance(cov.get("bookTicker"), dict) else {}
            cov_rows.append(
                {
                    "symbol": sym,
                    "aggTrade_covered_seconds": agg.get("covered_seconds"),
                    "bookTicker_covered_seconds": book.get("covered_seconds"),
                }
            )

    cov_rows.sort(
        key=lambda r: (
            float(r["aggTrade_covered_seconds"]) if isinstance(r["aggTrade_covered_seconds"], (int, float)) else -1.0,
        )
    )
    coverage_digest = cov_rows[: max(0, coverage_top_n)]

    top_pairs = sorted(reason_counts.items(), key=lambda x: (-x[1], x[0]))[: max(0, reason_top_n)]
    top_reason_codes = [{"code": k, "count": v} for k, v in top_pairs]

    return {
        "by_source_state": by_ss,
        "by_tier": by_tier,
        "top_reason_codes": top_reason_codes,
        "coverage_digest": coverage_digest,
        "not_ready_symbols": not_ready_syms,
    }


def top_reason_codes_from_summary(summary: dict[str, Any]) -> list[dict[str, Any]]:
    raw = summary.get("top_reason_codes")
    if isinstance(raw, list):
        return raw
    return []
