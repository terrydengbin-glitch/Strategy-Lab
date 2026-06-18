"""STEP3B CLI entry: assemble latest_factor_snapshot.json. docs/STEP4.0_Decision_Layer_Phase1_任务卡.md."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any

import orjson
from pydantic import ValidationError

from laoma_signal_engine.core.config_loader import EngineConfig
from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.factors.assembler import (
    attach_error_status,
    build_factor_snapshot_document,
)
from laoma_signal_engine.factors.models import FactorSnapshotDocument
from laoma_signal_engine.factors.writer import atomic_write_factor_snapshot
from laoma_signal_engine.market.light_snapshot_models import FuturesLightSnapshotDocument
from laoma_signal_engine.micro.micro_target_models import MicroTargetsDocument
from laoma_signal_engine.micro.assembly.models import LatestMicroFeaturesDocument
from laoma_signal_engine.scanner.signal_models import AbnormalTierDocument

log = logging.getLogger(__name__)


def _rel_project_path(project_root: Path, path: Path) -> str:
    return path.resolve().relative_to(project_root.resolve()).as_posix()


def run_assemble_factor_snapshot(
    *,
    project_root: Path | None = None,
    watch_path: Path | None = None,
    strong_path: Path | None = None,
    light_path: Path | None = None,
    micro_path: Path | None = None,
    micro_targets_path: Path | None = None,
    output_path: Path | None = None,
    stdout_json: bool = False,
    skip_market_context: bool = False,
) -> int:
    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    cfg = EngineConfig.load(pr)
    gen_at = to_iso_z(utc_now())

    wp = watch_path.resolve() if watch_path else cfg.latest_watch_signals_path
    sp = strong_path.resolve() if strong_path else cfg.latest_strong_candidates_path
    lp = light_path.resolve() if light_path else cfg.futures_light_snapshot_path
    mp = micro_path.resolve() if micro_path else (pr / "DATA/micro/latest_micro_features.json").resolve()
    mtp = micro_targets_path.resolve() if micro_targets_path else cfg.micro_targets_path.resolve()
    out_p = output_path.resolve() if output_path else (pr / "DATA/factors/latest_factor_snapshot.json").resolve()

    def _fail(msg: str, *, exc: BaseException | None = None) -> int:
        if exc is not None:
            log.exception("%s", msg)
        else:
            log.error("%s", msg)
        err_doc = attach_error_status(generated_at=gen_at, message=msg)
        try:
            atomic_write_factor_snapshot(out_p, err_doc)
        except OSError as wexc:
            log.error("factor snapshot error write failed: %s", wexc)
            return EXIT_CONFIG
        return EXIT_INTERNAL

    try:
        watch_doc = AbnormalTierDocument.model_validate(read_json_object(wp))
        strong_doc = AbnormalTierDocument.model_validate(read_json_object(sp))
        light_doc = FuturesLightSnapshotDocument.model_validate(read_json_object(lp))
        micro_targets_doc = MicroTargetsDocument.model_validate(read_json_object(mtp))
    except FileNotFoundError as exc:
        log.error("factor snapshot input missing: %s", exc)
        err_doc = attach_error_status(generated_at=gen_at, message=str(exc))
        try:
            atomic_write_factor_snapshot(out_p, err_doc)
        except OSError as wexc:
            log.error("factor snapshot error write failed: %s", wexc)
            return EXIT_CONFIG
        return EXIT_CONFIG
    except json.JSONDecodeError as exc:
        return _fail(f"factor snapshot json: {exc}", exc=exc)
    except ValidationError as exc:
        return _fail(f"factor snapshot validation: {exc}", exc=exc)
    except OSError as exc:
        log.error("factor snapshot read failed: %s", exc)
        return EXIT_CONFIG

    if micro_targets_doc.block_downstream or micro_targets_doc.status == "stale_input":
        doc = FactorSnapshotDocument(
            generated_at=gen_at,
            source="factor_snapshot",
            status="blocked",
            count=0,
            input_refs={
                "watch_generated_at": watch_doc.generated_at,
                "strong_generated_at": strong_doc.generated_at,
                "light_generated_at": light_doc.generated_at,
                "micro_target_generated_at": micro_targets_doc.generated_at,
                "micro_target_version": micro_targets_doc.generated_at,
                "micro_target_status": micro_targets_doc.status,
                "blocked_reason": "micro_targets_stale_input",
                "reason_codes": ["micro_targets_stale_input", "step2_watch_strong_stale"],
            },
            candidate_alignment={
                "mode": "micro_targets_authoritative",
                "source": "micro_targets.plan_candidate_symbols",
                "input_symbol_count": len(watch_doc.signals) + len(strong_doc.signals),
                "allowed_symbol_count": 0,
                "output_symbol_count": 0,
                "excluded_not_in_micro_target": len(watch_doc.signals) + len(strong_doc.signals),
                "excluded_symbols": sorted(
                    {
                        str(s.futures_symbol or s.symbol).upper().strip()
                        for s in [*watch_doc.signals, *strong_doc.signals]
                    },
                )[:50],
                "micro_target_generated_at": micro_targets_doc.generated_at,
                "micro_target_version": micro_targets_doc.generated_at,
                "blocked_by": "step2_stale",
            },
        )
        try:
            atomic_write_factor_snapshot(out_p, doc)
        except OSError as exc:
            log.error("factor snapshot blocked write failed: %s", exc)
            return EXIT_CONFIG
        if stdout_json:
            summary = {
                "schema_version": doc.schema_version,
                "source": doc.source,
                "status": doc.status,
                "count": doc.count,
                "blocked_reason": "micro_targets_stale_input",
                "output_file": _rel_project_path(pr, out_p),
            }
            sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))
            sys.stdout.buffer.flush()
        return EXIT_SUCCESS

    try:
        micro_doc = LatestMicroFeaturesDocument.model_validate(read_json_object(mp))
        micro_plan_candidate_symbols = list(micro_targets_doc.plan_candidate_symbols)
        if not micro_plan_candidate_symbols:
            micro_plan_candidate_symbols = [
                e.symbol
                for e in [*micro_targets_doc.tier1_warm_watch, *micro_targets_doc.tier2_active_strong]
            ]
        micro_target_entries = {
            e.symbol.upper().strip(): e
            for e in [*micro_targets_doc.tier1_warm_watch, *micro_targets_doc.tier2_active_strong]
        }

        doc = build_factor_snapshot_document(
            watch=watch_doc,
            strong=strong_doc,
            light=light_doc,
            micro=micro_doc,
            generated_at=gen_at,
            fetch_market_context=not skip_market_context,
            micro_features_max_age_sec=cfg.micro_features_max_age_sec,
            micro_target_max_age_sec=cfg.micro_target_max_age_sec,
            micro_plan_candidate_symbols=set(micro_plan_candidate_symbols),
            micro_target_entries=micro_target_entries,
            micro_target_generated_at=micro_targets_doc.generated_at,
            micro_target_version=micro_targets_doc.generated_at,
        )
        atomic_write_factor_snapshot(out_p, doc)
    except OSError as exc:
        log.error("factor snapshot write failed: %s", exc)
        return EXIT_CONFIG

    ctx_tag = "off" if skip_market_context else "binance_rest"
    log.info(
        "factor_snapshot status=%s count=%s market_context=%s out=%s",
        doc.status,
        doc.count,
        ctx_tag,
        out_p,
    )

    if stdout_json:
        summary = {
            "schema_version": doc.schema_version,
            "source": doc.source,
            "status": doc.status,
            "count": doc.count,
            "output_file": _rel_project_path(pr, out_p),
        }
        sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))
        sys.stdout.buffer.flush()
    return EXIT_SUCCESS


def run_assemble_factor_snapshot_without_ofi_cvd(
    *,
    project_root: Path | None = None,
    watch_path: Path | None = None,
    strong_path: Path | None = None,
    light_path: Path | None = None,
    output_path: Path | None = None,
    stdout_json: bool = False,
    skip_market_context: bool = False,
) -> int:
    """STEP3B.1: watch+strong+light + OI/Funding/Basis; no latest_micro_features.json (docs/STEP3B.1)."""
    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    cfg = EngineConfig.load(pr)
    gen_at = to_iso_z(utc_now())

    wp = watch_path.resolve() if watch_path else cfg.latest_watch_signals_path
    sp = strong_path.resolve() if strong_path else cfg.latest_strong_candidates_path
    lp = light_path.resolve() if light_path else cfg.futures_light_snapshot_path
    out_p = (
        output_path.resolve()
        if output_path
        else (pr / "DATA/factors/latest_factor_snapshot_withoutoficvd.json").resolve()
    )

    def _fail(msg: str, *, exc: BaseException | None = None) -> int:
        if exc is not None:
            log.exception("%s", msg)
        else:
            log.error("%s", msg)
        err_doc = attach_error_status(generated_at=gen_at, message=msg)
        try:
            atomic_write_factor_snapshot(out_p, err_doc)
        except OSError as wexc:
            log.error("factor snapshot (no micro) error write failed: %s", wexc)
            return EXIT_CONFIG
        return EXIT_INTERNAL

    try:
        watch_doc = AbnormalTierDocument.model_validate(read_json_object(wp))
        strong_doc = AbnormalTierDocument.model_validate(read_json_object(sp))
        light_doc = FuturesLightSnapshotDocument.model_validate(read_json_object(lp))
    except FileNotFoundError as exc:
        log.error("factor snapshot (no micro) input missing: %s", exc)
        err_doc = attach_error_status(generated_at=gen_at, message=str(exc))
        try:
            atomic_write_factor_snapshot(out_p, err_doc)
        except OSError as wexc:
            log.error("factor snapshot (no micro) error write failed: %s", wexc)
            return EXIT_CONFIG
        return EXIT_CONFIG
    except json.JSONDecodeError as exc:
        return _fail(f"factor snapshot (no micro) json: {exc}", exc=exc)
    except ValidationError as exc:
        return _fail(f"factor snapshot (no micro) validation: {exc}", exc=exc)
    except OSError as exc:
        log.error("factor snapshot (no micro) read failed: %s", exc)
        return EXIT_CONFIG

    try:
        doc = build_factor_snapshot_document(
            watch=watch_doc,
            strong=strong_doc,
            light=light_doc,
            micro=None,
            generated_at=gen_at,
            fetch_market_context=not skip_market_context,
        )
        atomic_write_factor_snapshot(out_p, doc)
    except OSError as exc:
        log.error("factor snapshot (no micro) write failed: %s", exc)
        return EXIT_CONFIG

    ctx_tag = "off" if skip_market_context else "binance_rest"
    log.info(
        "factor_snapshot_without_ofi_cvd status=%s count=%s market_context=%s out=%s",
        doc.status,
        doc.count,
        ctx_tag,
        out_p,
    )

    if stdout_json:
        summary = {
            "schema_version": doc.schema_version,
            "source": doc.source,
            "status": doc.status,
            "count": doc.count,
            "output_file": _rel_project_path(pr, out_p),
        }
        sys.stdout.buffer.write(orjson.dumps(summary, option=orjson.OPT_APPEND_NEWLINE))
        sys.stdout.buffer.flush()
    return EXIT_SUCCESS


def run_assemble_factor_snapshot_safe(**kwargs: Any) -> int:
    try:
        return run_assemble_factor_snapshot(**kwargs)
    except Exception as exc:
        log.exception("assemble factor snapshot failed: %s", exc)
        return EXIT_INTERNAL


def run_assemble_factor_snapshot_without_ofi_cvd_safe(**kwargs: Any) -> int:
    try:
        return run_assemble_factor_snapshot_without_ofi_cvd(**kwargs)
    except Exception as exc:
        log.exception("assemble factor snapshot (no micro) failed: %s", exc)
        return EXIT_INTERNAL
