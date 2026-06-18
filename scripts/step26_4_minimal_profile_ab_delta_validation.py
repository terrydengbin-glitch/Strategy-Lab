from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DEFAULT_CONFIG = ROOT / "laoma_signal_engine" / "config" / "default.yaml"
CANDIDATE_PROFILE = ROOT / "DATA" / "runtime" / "minimal_chain_config_profile_candidate.json"
MATRIX_PATH = ROOT / "DATA" / "runtime" / "config_essentiality_matrix.json"
STEP7_146_JSON = ROOT / "DATA" / "backtest" / "step7_146_strategy5_6_v5_gate_paper_equivalent_backtest.json"
V5_GATE_CONFIG = ROOT / "DATA" / "paper" / "v5_trade_gate_experiment.json"
OUTPUT_JSON = ROOT / "DATA" / "runtime" / "step26_4_minimal_profile_ab_delta_validation.json"
REPORT_DIR = ROOT / "docs" / "reports"

LINES = ["without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"]
PROTECTED_PREFIXES = (
    "paper.",
    "trade_plan_risk.",
    "position_sizing.",
    "freshness.",
    "strategy_pipeline.",
    "project_runtime.",
    "micro_daemon_state.",
    "micro_daemon_cli.",
    "wait_until_ready.",
)


def iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def read_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} must contain a mapping")
    return data


def flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            out.update(flatten(value, child))
    elif isinstance(obj, list):
        out[prefix] = obj
    else:
        out[prefix] = obj
    return out


def stable_hash(data: Any) -> str:
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def dataclass_to_dict(obj: Any) -> dict[str, Any]:
    if is_dataclass(obj):
        return asdict(obj)
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    raise TypeError(f"unsupported config object: {type(obj)!r}")


def write_candidate_project_root(candidate_config: dict[str, Any]) -> tuple[Path, tempfile.TemporaryDirectory[str]]:
    tmp = tempfile.TemporaryDirectory(prefix="step26_4_candidate_")
    root = Path(tmp.name)
    cfg_dir = root / "laoma_signal_engine" / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "default.yaml").write_text(yaml.safe_dump(candidate_config, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return root, tmp


def load_line_configs(candidate_config: dict[str, Any]) -> dict[str, Any]:
    from laoma_signal_engine.decision.trade_plan_lines import load_trade_plan_line_config

    candidate_root, tmp = write_candidate_project_root(candidate_config)
    try:
        baseline: dict[str, dict[str, Any]] = {}
        candidate: dict[str, dict[str, Any]] = {}
        deltas: dict[str, dict[str, Any]] = {}
        for line in LINES:
            base_cfg = dataclass_to_dict(load_trade_plan_line_config(ROOT, line))  # type: ignore[arg-type]
            cand_cfg = dataclass_to_dict(load_trade_plan_line_config(candidate_root, line))  # type: ignore[arg-type]
            baseline[line] = base_cfg
            candidate[line] = cand_cfg
            changed = {
                key: {"baseline": base_cfg.get(key), "candidate": cand_cfg.get(key)}
                for key in sorted(set(base_cfg) | set(cand_cfg))
                if base_cfg.get(key) != cand_cfg.get(key)
            }
            if changed:
                deltas[line] = changed
    finally:
        tmp.cleanup()
    return {
        "baseline_hash": stable_hash(baseline),
        "candidate_hash": stable_hash(candidate),
        "line_deltas": deltas,
        "line_delta_count": sum(len(delta) for delta in deltas.values()),
        "checked_lines": LINES,
    }


def protected_field_delta(default_cfg: dict[str, Any], candidate_cfg: dict[str, Any]) -> dict[str, Any]:
    base = flatten(default_cfg)
    cand = flatten(candidate_cfg)
    deltas: dict[str, dict[str, Any]] = {}
    for path, value in base.items():
        if path.startswith(PROTECTED_PREFIXES):
            if cand.get(path) != value:
                deltas[path] = {"baseline": value, "candidate": cand.get(path)}
    return {
        "protected_prefixes": list(PROTECTED_PREFIXES),
        "delta_count": len(deltas),
        "deltas": deltas,
    }


def removed_field_validation(profile: dict[str, Any], matrix: dict[str, Any]) -> dict[str, Any]:
    removed = profile.get("removed_fields") or []
    matrix_rows = {str(row.get("field_path")): row for row in matrix.get("fields") or [] if isinstance(row, dict)}
    errors: list[str] = []
    by_batch: dict[str, list[str]] = {}
    for batch in profile.get("disable_batches") or []:
        by_batch[str(batch.get("batch_id"))] = list(batch.get("fields") or [])
    for row in removed:
        field_path = str(row.get("field_path"))
        matrix_row = matrix_rows.get(field_path)
        if not matrix_row:
            errors.append(f"missing_matrix_row:{field_path}")
            continue
        if matrix_row.get("necessity_class") != "disable_candidate":
            errors.append(f"removed_non_disable_candidate:{field_path}:{matrix_row.get('necessity_class')}")
        if any(field_path.startswith(prefix) for prefix in PROTECTED_PREFIXES) or field_path.startswith("DATA.paper."):
            errors.append(f"removed_protected_field:{field_path}")
    return {
        "removed_field_count": len(removed),
        "removed_batches": {key: len(value) for key, value in by_batch.items()},
        "validation_errors": errors,
    }


def latest_step7146_evidence() -> dict[str, Any]:
    data = read_json(STEP7_146_JSON, {})
    if not isinstance(data, dict) or not data:
        return {"exists": False}
    rows = []
    for row in data.get("results") or []:
        if isinstance(row, dict):
            rows.append(
                {
                    "strategy_line": row.get("strategy_line"),
                    "branch": row.get("branch"),
                    "executable_plans": row.get("executable_plans"),
                    "consumed_plans": row.get("consumed_plans"),
                    "created_orders": row.get("created_orders"),
                    "skip_rows": row.get("skip_rows"),
                    "gate_decisions": row.get("gate_decisions"),
                }
            )
    return {
        "exists": True,
        "path": str(STEP7_146_JSON.relative_to(ROOT)),
        "generated_at": data.get("generated_at"),
        "execution_contract": data.get("execution_contract"),
        "window": data.get("window"),
        "results": rows,
    }


def gate_evidence() -> dict[str, Any]:
    gate = read_json(V5_GATE_CONFIG, {})
    if not isinstance(gate, dict):
        return {"exists": False}
    return {
        "exists": V5_GATE_CONFIG.exists(),
        "path": str(V5_GATE_CONFIG.relative_to(ROOT)),
        "enabled": gate.get("enabled"),
        "experiment_id": gate.get("experiment_id"),
        "paper_epoch_id": gate.get("paper_epoch_id"),
        "mode": gate.get("mode"),
        "lines": sorted((gate.get("rules") or {}).keys()) if isinstance(gate.get("rules"), dict) else [],
        "hash": stable_hash(gate),
    }


def write_report(payload: dict[str, Any]) -> Path:
    stamp = utc_stamp()
    path = REPORT_DIR / f"STEP26.4_minimal_profile_ab_delta_validation_{stamp}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    line_delta_count = payload["trade_plan_line_config_comparison"]["line_delta_count"]
    protected_delta_count = payload["protected_field_comparison"]["delta_count"]
    removed_errors = payload["removed_field_validation"]["validation_errors"]
    lines = [
        "# STEP26.4 Minimal Profile A/B Delta Validation",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- status: `{payload['status']}`",
        f"- output_json: `{payload['output_json']}`",
        f"- candidate_profile: `{payload['candidate_profile']}`",
        f"- default_yaml_changed: `{payload['default_yaml_changed']}`",
        "",
        "## Verdict",
        "",
        f"- trade_plan_line_config_delta_count: `{line_delta_count}`",
        f"- protected_field_delta_count: `{protected_delta_count}`",
        f"- removed_field_validation_errors: `{len(removed_errors)}`",
        f"- expected_paper_equivalent_delta: `{payload['expected_paper_equivalent_delta']}`",
        "",
        "## Latest STEP7.146 Evidence",
        "",
    ]
    evidence = payload["latest_step7_146_evidence"]
    if evidence.get("exists"):
        window = evidence.get("window") or {}
        lines.extend(
            [
                f"- execution_contract: `{evidence.get('execution_contract')}`",
                f"- generated_at: `{evidence.get('generated_at')}`",
                f"- symbols: `{window.get('symbols')}`",
                "",
                "| strategy | branch | executable | consumed | orders | skips | gate |",
                "| --- | --- | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in evidence.get("results") or []:
            lines.append(
                f"| `{row.get('strategy_line')}` | `{row.get('branch')}` | {row.get('executable_plans')} | "
                f"{row.get('consumed_plans')} | {row.get('created_orders')} | {row.get('skip_rows')} | `{row.get('gate_decisions')}` |"
            )
    else:
        lines.append("- latest STEP7.146 result not found")
    lines.extend(
        [
            "",
            "## Removed Field Batches",
            "",
            "| batch | count |",
            "| --- | ---: |",
        ]
    )
    for batch, count in payload["removed_field_validation"]["removed_batches"].items():
        lines.append(f"| `{batch}` | {count} |")
    lines.extend(
        [
            "",
            "## Gate Evidence",
            "",
            f"- gate_enabled: `{payload['gate_evidence'].get('enabled')}`",
            f"- experiment_id: `{payload['gate_evidence'].get('experiment_id')}`",
            f"- mode: `{payload['gate_evidence'].get('mode')}`",
            f"- lines: `{payload['gate_evidence'].get('lines')}`",
            "",
            "## Notes",
            "",
            "- This validation does not apply the candidate profile to production.",
            "- Candidate removed fields are limited to non-trading metadata, disabled Feishu notification config, and disabled legacy micro_fast/micro_full quality gates.",
            "- Actual production cleanup still requires a follow-up task.",
        ]
    )
    if removed_errors:
        lines.extend(["", "## Validation Errors", ""])
        lines.extend(f"- `{err}`" for err in removed_errors)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def build_payload() -> dict[str, Any]:
    default_cfg = read_yaml(DEFAULT_CONFIG)
    profile = read_json(CANDIDATE_PROFILE, {})
    matrix = read_json(MATRIX_PATH, {})
    candidate_cfg = profile.get("candidate_config")
    if not isinstance(candidate_cfg, dict):
        raise RuntimeError("minimal_chain_config_profile_candidate.candidate_config must be a dict")

    line_config_cmp = load_line_configs(candidate_cfg)
    protected_cmp = protected_field_delta(default_cfg, candidate_cfg)
    removed_validation = removed_field_validation(profile, matrix)
    errors: list[str] = []
    if line_config_cmp["line_delta_count"]:
        errors.append("trade_plan_line_config_delta_nonzero")
    if protected_cmp["delta_count"]:
        errors.append("protected_field_delta_nonzero")
    errors.extend(removed_validation["validation_errors"])

    payload = {
        "schema_version": "step26.4-minimal-profile-ab-delta-validation-v1",
        "generated_at": iso_now(),
        "task_id": "STEP26.4",
        "status": "ok" if not errors else "blocked",
        "candidate_profile": str(CANDIDATE_PROFILE.relative_to(ROOT)),
        "source_default_config": str(DEFAULT_CONFIG.relative_to(ROOT)),
        "source_matrix": str(MATRIX_PATH.relative_to(ROOT)),
        "output_json": str(OUTPUT_JSON.relative_to(ROOT)),
        "default_yaml_changed": False,
        "trade_plan_line_config_comparison": line_config_cmp,
        "protected_field_comparison": protected_cmp,
        "removed_field_validation": removed_validation,
        "gate_evidence": gate_evidence(),
        "latest_step7_146_evidence": latest_step7146_evidence(),
        "expected_paper_equivalent_delta": "zero" if not errors else "nonzero_or_blocked",
        "validation_errors": errors,
    }
    report = write_report(payload)
    payload["report"] = str(report.relative_to(ROOT))
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def main() -> None:
    payload = build_payload()
    print(
        json.dumps(
            {
                "status": payload["status"],
                "output_json": payload["output_json"],
                "report": payload["report"],
                "line_delta_count": payload["trade_plan_line_config_comparison"]["line_delta_count"],
                "protected_delta_count": payload["protected_field_comparison"]["delta_count"],
                "validation_errors": payload["validation_errors"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if payload["validation_errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    sys.exit(main())
