"""STEP34 sandbox full pipeline bounded runner.

The runner is explicit: it never changes the baseline pipeline entrypoint. A
caller must provide a valid SandboxWriterContext created by the P31 resource
governor.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from laoma_signal_engine.core.atomic_writer import write_file_atomic
from laoma_signal_engine.paper.models import Candle, STRATEGY_LINES
from laoma_signal_engine.strategy_sandbox.daemon_writer import (
    micro_writer_targets,
    snapshot_writer_targets,
    strategy_writer_targets,
)
from laoma_signal_engine.strategy_sandbox.paper_pipeline import run_sandbox_paper_pipeline
from laoma_signal_engine.strategy_sandbox.writer_context import (
    SandboxWriterContextError,
    project_rel,
    safe_part,
    sandbox_run_dir,
    validate_writer_context_payload,
)

SCHEMA_VERSION = "STEP34_sandbox_full_pipeline_v1"
EXECUTION_CONTRACT = "sandbox_full_pipeline_explicit_context"
PIPELINE_MODE = "sandbox_full_pipeline"
DEFAULT_SYMBOL = "OPGUSDT"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_file_atomic(path, _json_bytes(payload))


def _root_and_rel(project_root: Path, rel_or_abs: str) -> Path:
    path = Path(rel_or_abs)
    return path if path.is_absolute() else Path(project_root).resolve() / path


def _context(writer_context: dict[str, Any], *, sandbox_id: str, run_id: str, cycle_id: str) -> dict[str, Any]:
    ctx = writer_context.get("context") if isinstance(writer_context.get("context"), dict) else {}
    if not ctx:
        raise SandboxWriterContextError("sandbox_writer_context_required:sandbox_full_pipeline")
    validated = validate_writer_context_payload(ctx)
    mismatches = [
        name
        for name, expected, got in (
            ("sandbox_id", sandbox_id, validated.get("sandbox_id")),
            ("run_id", run_id, validated.get("run_id")),
            ("cycle_id", cycle_id, validated.get("cycle_id")),
        )
        if str(expected) != str(got)
    ]
    if mismatches:
        raise SandboxWriterContextError(f"sandbox_writer_context_mismatch:{','.join(mismatches)}")
    if validated.get("main_chain_mutation_allowed"):
        raise SandboxWriterContextError("main_chain_mutation_not_allowed_for_sandbox_full_pipeline")
    return validated


def _run_root(project_root: Path, *, sandbox_id: str, run_id: str, writer_context: dict[str, Any]) -> Path:
    targets = writer_context.get("writer_targets") if isinstance(writer_context.get("writer_targets"), dict) else {}
    runtime_rel = targets.get("sandbox_runtime_dir")
    if runtime_rel:
        return _root_and_rel(project_root, str(runtime_rel))
    return sandbox_run_dir(Path(project_root).resolve(), sandbox_id, run_id)


def _stage(
    *,
    name: str,
    status: str,
    artifacts: dict[str, Any] | None = None,
    reason_codes: list[str] | None = None,
    result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "stage_name": name,
        "stage_status": status,
        "artifacts": artifacts or {},
        "reason_codes": sorted(set(reason_codes or [])),
        "result": result or {},
        "completed_at": utc_now_iso(),
    }


def _default_snapshot(symbol: str) -> dict[str, Any]:
    return {
        "schema_version": "STEP34_fixture_snapshot_v1",
        "source": "sandbox_full_pipeline_fixture_or_cache",
        "generated_at": utc_now_iso(),
        "items": [
            {
                "symbol": symbol,
                "last_price": 1.0,
                "quote_volume": 1_000_000,
                "price_change_percent": 1.5,
                "known_at_ms": 1_000,
            }
        ],
    }


def _default_micro(symbol: str) -> tuple[dict[str, Any], dict[str, Any]]:
    state = {
        "schema_version": "STEP34_fixture_micro_state_v1",
        "status": "ready",
        "generated_at": utc_now_iso(),
        "ready_symbols": [symbol],
        "source": "sandbox_full_pipeline_fixture_or_bounded_micro",
    }
    features = {
        "schema_version": "STEP34_fixture_micro_features_v1",
        "generated_at": utc_now_iso(),
        "features": {
            symbol: {
                "ofi": 0.0,
                "cvd": 0.0,
                "spread_bps": 2.0,
                "micro_ready": True,
            }
        },
    }
    return state, features


def _default_candles(symbol: str) -> dict[str, list[dict[str, Any]]]:
    return {
        symbol: [
            {"symbol": symbol, "open_time_ms": 1_000, "open": 1.0, "high": 1.02, "low": 0.99, "close": 1.01, "volume": 1000},
            {"symbol": symbol, "open_time_ms": 61_000, "open": 1.01, "high": 1.12, "low": 1.0, "close": 1.1, "volume": 1000},
        ]
    }


def _trade_plan_doc(line: str, *, sandbox_id: str, run_id: str, cycle_id: str, symbol: str) -> dict[str, Any]:
    source = "trade_plan_without_micro" if line == "without_micro" else f"trade_plan_{line}"
    return {
        "schema_version": "STEP34_sandbox_strategy_trade_plan_v1",
        "generated_at": utc_now_iso(),
        "run_id": run_id,
        "cycle_id": cycle_id,
        "source": source,
        "sandbox_id": sandbox_id,
        "status": "ok",
        "count": 1,
        "executable_count": 1,
        "input_refs": {},
        "plans": [
            {
                "symbol": symbol,
                "decision_tf": "15m",
                "decision": "LONG",
                "action": "ENTER_MARKET",
                "entry_mode": "MARKET",
                "estimated_entry_price": 1.0,
                "stop_loss": 0.9,
                "take_profit": 1.1,
                "risk_per_unit": 0.1,
                "reward_per_unit": 0.1,
                "rr": 1.0,
                "executable": True,
                "confidence": 80,
                "reason_codes": ["sandbox_full_pipeline_fixture_strategy_doc"],
                "guards": {"line": line, "margin_usdt": 100, "leverage": 20},
                "input_refs": {},
            }
        ],
    }


def _strategy_lines(options: dict[str, Any]) -> list[str]:
    got = options.get("strategy_lines")
    if isinstance(got, str):
        lines = [x.strip() for x in got.split(",") if x.strip()]
    elif isinstance(got, list):
        lines = [str(x).strip() for x in got if str(x).strip()]
    else:
        line = str(options.get("strategy_line") or "without_micro")
        lines = [line]
    out: list[str] = []
    for line in lines:
        if line == "strategy1":
            line = "without_micro"
        if line not in STRATEGY_LINES:
            raise ValueError(f"unsupported_sandbox_full_pipeline_strategy_line:{line}")
        if line not in out:
            out.append(line)
    return out or ["without_micro"]


def _write_manifest(manifest_path: Path, manifest: dict[str, Any]) -> None:
    _write_json(manifest_path, manifest)


def run_sandbox_full_pipeline(
    project_root: Path,
    *,
    sandbox_id: str,
    run_id: str,
    cycle_id: str,
    writer_context: dict[str, Any],
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    opts = options or {}
    ctx = _context(writer_context, sandbox_id=sandbox_id, run_id=run_id, cycle_id=cycle_id)
    run_root = _run_root(root, sandbox_id=sandbox_id, run_id=run_id, writer_context=writer_context)
    manifest_path = run_root / "artifact_manifest.json"
    symbol = str(opts.get("symbol") or DEFAULT_SYMBOL).upper()
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "execution_contract": EXECUTION_CONTRACT,
        "pipeline_mode": PIPELINE_MODE,
        "status": "running",
        "sandbox_id": sandbox_id,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "resource_lane": ctx.get("resource_lane"),
        "writer_context_id": writer_context.get("context_id"),
        "main_chain_mutation_allowed": False,
        "started_at": utc_now_iso(),
        "stages": [],
        "reason_codes": [],
    }
    _write_manifest(manifest_path, manifest)

    snapshot_targets = snapshot_writer_targets(root, writer_context=writer_context)
    snapshot_payload = opts.get("snapshot") if isinstance(opts.get("snapshot"), dict) else _default_snapshot(symbol)
    snapshot_path = _root_and_rel(root, snapshot_targets["latest_snapshot"]["path"])
    _write_json(snapshot_path, snapshot_payload | {"sandbox_id": sandbox_id, "run_id": run_id, "cycle_id": cycle_id})
    manifest["stages"].append(
        _stage(
            name="snapshot",
            status="completed",
            artifacts={"snapshot_output_path": project_rel(root, snapshot_path), "targets": snapshot_targets},
            reason_codes=["rest_budget_shared_governance_observed"],
        )
    )
    _write_manifest(manifest_path, manifest)

    micro_targets = micro_writer_targets(root, writer_context=writer_context)
    state, features = _default_micro(symbol)
    if isinstance(opts.get("micro_state"), dict):
        state = opts["micro_state"]
    if isinstance(opts.get("micro_features"), dict):
        features = opts["micro_features"]
    micro_state_path = _root_and_rel(root, micro_targets["latest_state"]["path"])
    micro_features_path = _root_and_rel(root, micro_targets["latest_features"]["path"])
    _write_json(micro_state_path, state | {"sandbox_id": sandbox_id, "run_id": run_id, "cycle_id": cycle_id})
    _write_json(micro_features_path, features | {"sandbox_id": sandbox_id, "run_id": run_id, "cycle_id": cycle_id})
    manifest["stages"].append(
        _stage(
            name="micro",
            status="completed",
            artifacts={
                "micro_state_path": project_rel(root, micro_state_path),
                "micro_features_path": project_rel(root, micro_features_path),
                "targets": micro_targets,
            },
        )
    )
    _write_manifest(manifest_path, manifest)

    docs: dict[str, dict[str, Any]]
    if isinstance(opts.get("docs"), dict):
        docs = opts["docs"]
        lines = sorted(docs)
    else:
        lines = _strategy_lines(opts)
        docs = {line: _trade_plan_doc(line, sandbox_id=sandbox_id, run_id=run_id, cycle_id=cycle_id, symbol=symbol) for line in lines}
    strategy_artifacts: dict[str, Any] = {}
    for line, doc in docs.items():
        targets = strategy_writer_targets(root, strategy_id=line, writer_context=writer_context)
        trade_plan_path = _root_and_rel(root, targets["latest_trade_plan"]["path"])
        evidence_path = _root_and_rel(root, targets["evidence"]["path"])
        _write_json(trade_plan_path, doc)
        _write_json(
            evidence_path,
            {
                "schema_version": "STEP34_strategy_evidence_v1",
                "sandbox_id": sandbox_id,
                "run_id": run_id,
                "cycle_id": cycle_id,
                "strategy_line": line,
                "trade_plan_path": project_rel(root, trade_plan_path),
                "source": "sandbox_full_pipeline_strategy_stage",
                "generated_at": utc_now_iso(),
            },
        )
        strategy_artifacts[line] = {
            "trade_plan_path": project_rel(root, trade_plan_path),
            "evidence_path": project_rel(root, evidence_path),
            "targets": targets,
        }
    manifest["stages"].append(
        _stage(
            name="strategy",
            status="completed",
            artifacts={"strategy_output_paths": strategy_artifacts, "lines": lines},
        )
    )
    _write_manifest(manifest_path, manifest)

    candles = opts.get("candles_by_symbol") if isinstance(opts.get("candles_by_symbol"), Mapping) else _default_candles(symbol)
    paper = run_sandbox_paper_pipeline(
        root,
        sandbox_id=sandbox_id,
        run_id=run_id,
        cycle_id=cycle_id,
        writer_context=writer_context,
        docs=docs,
        candles_by_symbol=cast_candles(candles),
        max_ticks=opts.get("max_ticks"),
        options={"pipeline_mode": PIPELINE_MODE, **opts},
    )
    manifest["stages"].append(
        _stage(
            name="paper",
            status=str(paper.get("status") or "completed"),
            artifacts={
                "paper_db_path": paper.get("paper_db_path"),
                "paper_summary_path": paper.get("paper_summary_path"),
                "paper_result_path": paper.get("result_path"),
            },
            reason_codes=paper.get("reason_codes") if isinstance(paper.get("reason_codes"), list) else [],
            result={
                "counts": paper.get("counts"),
                "trade_quality_completion": paper.get("trade_quality_completion"),
                "training_dataset": paper.get("training_dataset"),
                "candidate_ledger": paper.get("candidate_ledger"),
            },
        )
    )
    status = "completed" if str(paper.get("status") or "").startswith("completed") else str(paper.get("status") or "completed")
    manifest["status"] = status
    manifest["completed_at"] = utc_now_iso()
    manifest["artifact_manifest_path"] = project_rel(root, manifest_path)
    manifest["paper_db_path"] = paper.get("paper_db_path")
    manifest["paper_summary_path"] = paper.get("paper_summary_path")
    manifest["trade_quality_completion"] = paper.get("trade_quality_completion")
    manifest["training_dataset"] = paper.get("training_dataset")
    manifest["candidate_ledger"] = paper.get("candidate_ledger")
    manifest["reason_codes"] = sorted(set(paper.get("reason_codes") or []))
    _write_manifest(manifest_path, manifest)
    return manifest


def cast_candles(candles: Mapping[str, Any]) -> Mapping[str, list[Candle | Mapping[str, Any]]]:
    return candles  # type: ignore[return-value]
