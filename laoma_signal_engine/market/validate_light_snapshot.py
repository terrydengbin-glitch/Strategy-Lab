"""Validate futures_light_snapshot.json (Step 1.5 / 1.51 regression checks)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_DATA_INSUFFICIENT, EXIT_SUCCESS
from laoma_signal_engine.core.models import CandidateUniverseDocument
from laoma_signal_engine.universe.step15_symbols import futures_symbols_for_step_1_5

REQUIRED_TOP_FIELDS = (
    "schema_version",
    "source",
    "eligible_futures_count",
    "snapshot_count",
    "success_count",
    "failed_count",
    "skipped_count",
    "items",
)

REQUIRED_ITEM_FIELDS = (
    "symbol",
    "base_asset",
    "last_price",
    "decision_tf",
    "primary_15m",
    "trigger_5m",
    "entry_1m",
    "background",
    "reason_codes",
    "data_quality",
)


def _is_bad(x: Any) -> bool:
    return x is None or (isinstance(x, float) and x != x)


def validate_light_snapshot_doc(raw: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    for k in REQUIRED_TOP_FIELDS:
        if k not in raw:
            errs.append(f"missing_top_field:{k}")

    if raw.get("schema_version") != "1.6":
        errs.append("schema_version_must_be_1.6")
    if raw.get("source") != "binance_um_futures":
        errs.append("source_must_be_binance_um_futures")

    elig = raw.get("eligible_futures_count")
    snap_n = raw.get("snapshot_count")
    items = raw.get("items")
    if not isinstance(items, list):
        errs.append("items_must_be_list")
        return errs

    if isinstance(elig, int) and isinstance(snap_n, int) and elig < snap_n:
        errs.append("eligible_futures_count_lt_snapshot_count")

    if snap_n != len(items):
        errs.append("snapshot_count_ne_len_items")

    sc = raw.get("success_count")
    fc = raw.get("failed_count")
    sk = raw.get("skipped_count")
    if isinstance(sc, int) and isinstance(fc, int) and isinstance(snap_n, int):
        if sc + fc != snap_n:
            errs.append("success_plus_failed_ne_snapshot_count")

    for forbidden in ("perf", "worker", "latency", "strong_candidate"):
        if forbidden in raw:
            errs.append(f"forbidden_top_field:{forbidden}")

    vol_ratios: list[Any] = []
    atr_1m: list[Any] = []
    reason_lens: list[int] = []

    for i, it in enumerate(items):
        if not isinstance(it, dict):
            errs.append(f"item_{i}_not_object")
            continue
        if "strong_candidate" in it:
            errs.append(f"item_{i}_has_strong_candidate")
        for fk in REQUIRED_ITEM_FIELDS:
            if fk not in it:
                errs.append(f"item_{i}_missing:{fk}")
        sym = str(it.get("symbol", f"idx_{i}"))
        p15 = it.get("primary_15m") or {}
        t5 = it.get("trigger_5m") or {}
        e1 = it.get("entry_1m") or {}
        if not isinstance(p15, dict) or not isinstance(t5, dict) or not isinstance(e1, dict):
            continue
        if p15.get("ready") is True:
            for k in (
                "price_ret",
                "volume_ratio",
                "quote_volume",
                "atr",
                "range_pos",
                "range_pos_raw",
                "range_pos_clamped",
                "range_break_state",
                "structure_state",
                "taker_buy_ratio",
                "kline_cvd_delta",
                "kline_cvd_state",
            ):
                if k not in p15:
                    errs.append(f"{sym}.primary_15m.missing:{k}")
        vol_ratios.append(t5.get("volume_ratio"))
        atr_1m.append(e1.get("atr"))
        rc = it.get("reason_codes")
        reason_lens.append(len(rc) if isinstance(rc, list) else 0)

    if items and all(_is_bad(v) for v in vol_ratios):
        errs.append("all_trigger_5m_volume_ratio_null")
    if items and all(_is_bad(v) for v in atr_1m):
        errs.append("all_entry_1m_atr_null")
    if items and all(l == 0 for l in reason_lens):
        errs.append("all_reason_codes_empty")

    return errs


def run_validate_light_snapshot(
    *,
    input_path: Path,
    stdout_json: bool = False,
    universe_order_path: Path | None = None,
) -> int:
    try:
        with open(input_path, encoding="utf-8") as fp:
            raw = json.load(fp)
    except (OSError, TypeError, ValueError) as exc:
        msg = f"cannot_read_input: {exc}"
        if stdout_json:
            sys.stdout.buffer.write(
                json.dumps({"ok": False, "errors": [msg]}, ensure_ascii=False).encode("utf-8")
                + b"\n"
            )
        else:
            print(msg, file=sys.stderr)
        return EXIT_CONFIG
    if not isinstance(raw, dict):
        errs = ["root_must_be_object"]
    else:
        errs = validate_light_snapshot_doc(raw)
        if universe_order_path is not None:
            try:
                with open(universe_order_path, encoding="utf-8") as fp:
                    udoc = json.load(fp)
                doc = CandidateUniverseDocument.model_validate(udoc)
                eligible = futures_symbols_for_step_1_5(doc)
                syms = [str(x.get("symbol", "")) for x in raw.get("items") or [] if isinstance(x, dict)]
                if syms != list(eligible)[: len(syms)]:
                    errs.append("items_symbol_order_mismatch_universe")
            except (OSError, TypeError, ValueError) as exc:
                errs.append(f"universe_order_check_failed:{exc}")

    ok = len(errs) == 0
    if stdout_json:
        out_obj = {"ok": ok, "errors": errs, "input": str(input_path)}
        sys.stdout.buffer.write(
            json.dumps(out_obj, ensure_ascii=False).encode("utf-8") + b"\n"
        )
    else:
        if ok:
            print("validate_light_snapshot: OK")
        else:
            for e in errs:
                print(e, file=sys.stderr)
    return EXIT_SUCCESS if ok else EXIT_DATA_INSUFFICIENT
