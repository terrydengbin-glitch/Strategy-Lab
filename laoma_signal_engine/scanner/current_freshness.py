"""Current freshness audit for Step2 raw/watch/strong outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from laoma_signal_engine.core.config_loader import EngineConfig
from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.time_utils import age_sec_from_iso_z
from laoma_signal_engine.scanner.signal_models import AbnormalTierDocument


def _load_tier(path: Path) -> AbnormalTierDocument | None:
    try:
        return AbnormalTierDocument.model_validate(read_json_object(path))
    except (OSError, TypeError, ValueError):
        return None


def build_step2_current_freshness(
    *,
    project_root: Path | None = None,
    max_age_sec: int | None = None,
) -> dict[str, Any]:
    cfg = EngineConfig.load(project_root)
    max_age = int(max_age_sec if max_age_sec is not None else cfg.step2_signal_max_age_sec)
    watch = _load_tier(cfg.latest_watch_signals_path)
    strong = _load_tier(cfg.latest_strong_candidates_path)
    reason_codes: list[str] = []

    if watch is None:
        reason_codes.append("watch_missing_or_invalid")
    if strong is None:
        reason_codes.append("strong_missing_or_invalid")

    watch_output_age = age_sec_from_iso_z(watch.generated_at) if watch is not None else None
    strong_output_age = age_sec_from_iso_z(strong.generated_at) if strong is not None else None
    input_snapshot_generated_at = ""
    if watch is not None and watch.input_snapshot_generated_at:
        input_snapshot_generated_at = watch.input_snapshot_generated_at
    elif strong is not None:
        input_snapshot_generated_at = strong.input_snapshot_generated_at
    input_snapshot_age = age_sec_from_iso_z(input_snapshot_generated_at) if input_snapshot_generated_at else None

    for name, doc, output_age in (
        ("watch", watch, watch_output_age),
        ("strong", strong, strong_output_age),
    ):
        if doc is None:
            continue
        if doc.status not in ("ok", "ok_degraded"):
            reason_codes.append(f"{name}_status_{doc.status}")
        if output_age is not None and output_age > max_age:
            reason_codes.append(f"{name}_output_stale")
    if input_snapshot_age is not None and input_snapshot_age > max_age:
        reason_codes.append("step2_current_stale")

    freshness = "fresh" if not reason_codes else "stale"
    return {
        "schema_version": "1.0",
        "source": "step2_current_freshness",
        "current_freshness": freshness,
        "max_age_sec": max_age,
        "watch_status": watch.status if watch is not None else None,
        "strong_status": strong.status if strong is not None else None,
        "watch_generated_at": watch.generated_at if watch is not None else None,
        "strong_generated_at": strong.generated_at if strong is not None else None,
        "watch_output_age_sec": watch_output_age,
        "strong_output_age_sec": strong_output_age,
        "input_snapshot_generated_at": input_snapshot_generated_at,
        "current_input_snapshot_age_sec": input_snapshot_age,
        "watch_count": watch.count if watch is not None else 0,
        "strong_count": strong.count if strong is not None else 0,
        "reason_codes": list(dict.fromkeys(reason_codes)),
    }
