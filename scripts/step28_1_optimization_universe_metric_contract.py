from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
TASK_ID = "STEP28.1"
OUT_DIR = ROOT / "DATA" / "backtest" / "step28"
REPORT_DIR = ROOT / "docs" / "reports"
MANIFEST_PATH = OUT_DIR / "step28_1_optimization_universe_metric_contract.json"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _config() -> dict[str, Any]:
    path = ROOT / "laoma_signal_engine" / "config" / "default.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _sidecar_status() -> dict[str, Any]:
    db_path = ROOT / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db"
    out: dict[str, Any] = {
        "required": True,
        "db_path": str(db_path.relative_to(ROOT)),
        "exists": db_path.exists(),
        "source_mode_counts": {},
        "sample_count": 0,
        "event_count": 0,
    }
    if not db_path.exists():
        return out
    with sqlite3.connect(db_path) as conn:
        for table, key in (
            ("trade_training_samples", "sample_count"),
            ("trade_snapshot_events", "event_count"),
        ):
            try:
                out[key] = int(conn.execute(f"select count(*) from {table}").fetchone()[0])
            except sqlite3.Error:
                out[key] = 0
        try:
            rows = conn.execute(
                "select source_mode, count(*) from trade_training_samples group by source_mode order by source_mode"
            ).fetchall()
            out["source_mode_counts"] = {str(mode): int(count) for mode, count in rows}
        except sqlite3.Error:
            out["source_mode_counts"] = {}
    return out


def _line_config_refs(cfg: dict[str, Any]) -> dict[str, Any]:
    trade_plan_lines = cfg.get("trade_plan_lines") if isinstance(cfg.get("trade_plan_lines"), dict) else {}
    return {
        "without_micro": {
            "strategy_name": "strategy1",
            "config_ref": "trade_plan_lines.without_micro",
            "inherits_from": None,
            "current_keys": sorted((trade_plan_lines.get("without_micro") or {}).keys()),
        },
        "strategy4": {
            "strategy_name": "strategy4",
            "config_ref": "trade_plan_lines.strategy4 + strategy4",
            "inherits_from": (trade_plan_lines.get("strategy4") or {}).get("inherit_from"),
            "current_keys": sorted(set((trade_plan_lines.get("strategy4") or {}).keys()) | set((cfg.get("strategy4") or {}).keys())),
        },
        "strategy5": {
            "strategy_name": "strategy5",
            "config_ref": "trade_plan_lines.strategy5 + strategy5 evidence",
            "inherits_from": (trade_plan_lines.get("strategy5") or {}).get("inherit_from"),
            "current_keys": sorted((trade_plan_lines.get("strategy5") or {}).keys()),
        },
        "strategy6": {
            "strategy_name": "strategy6",
            "config_ref": "trade_plan_lines.strategy6 + strategy6",
            "inherits_from": (trade_plan_lines.get("strategy6") or {}).get("inherit_from"),
            "current_keys": sorted(set((trade_plan_lines.get("strategy6") or {}).keys()) | set((cfg.get("strategy6") or {}).keys())),
        },
    }


def build_manifest() -> dict[str, Any]:
    cfg = _config()
    return {
        "schema_version": "step28.1.optimization_universe_metric_contract.v1",
        "task_id": TASK_ID,
        "generated_at": _now(),
        "objective": {
            "primary": "quickly_find_max_paper_pf_strategy_parameter_gate_candidate",
            "target_strategy_lines": ["without_micro", "strategy4", "strategy5", "strategy6"],
            "strategy_mapping": {
                "strategy1": "without_micro",
                "strategy4": "strategy4",
                "strategy5": "strategy5",
                "strategy6": "strategy6",
            },
            "paper_reuse_required": True,
            "training_sidecar_required": True,
        },
        "hard_constraints": {
            "feature_scope": "kline_only",
            "microstructure_excluded": True,
            "per_strategy_config_policy": "independent",
            "search_mode": "fast_coarse_to_topN",
            "deep_long_grid_search": False,
            "legacy_direct_fill_promotion_allowed": False,
            "execution_contract_required": "paper_equivalent",
        },
        "universe": {
            "base_source": "STEP7.149 full 100-symbol paper-equivalent replay universe",
            "formal_fast_start": {
                "mode": "bounded_coarse",
                "symbol_limit": 100,
                "topN_per_strategy_after_coarse": 5,
                "topN_global_after_gate": 10,
            },
            "fallback_smoke": {
                "mode": "contract_smoke",
                "min_parameter_sets_per_line": 1,
                "purpose": "runner_contract_and_training_sidecar_validation",
            },
        },
        "split_policy": {
            "mode": "fast_holdout",
            "train": "earliest_60_percent",
            "validation": "next_20_percent",
            "test": "latest_20_percent",
            "walk_forward_required_for_promotion": True,
        },
        "metrics": {
            "primary_ranking_metric": "paper_equivalent_test_profit_factor",
            "secondary_metrics": [
                "expectancy_R",
                "max_drawdown_R",
                "win_rate",
                "trade_count",
                "trade_coverage",
                "gate_pass_count",
                "gate_block_count",
                "skip_count",
            ],
            "minimum_trade_count_fast_candidate": 20,
            "minimum_trade_coverage_test": 0.25,
            "promotion_requires_no_leakage": True,
        },
        "feature_policy": {
            "allowlist": [
                "symbol",
                "side",
                "decision_tf",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "ema",
                "rsi",
                "bollinger_band",
                "atr",
                "return_pct",
                "range_pct",
                "volatility",
                "hour_of_day",
                "day_of_week",
                "funding_rate_if_known_at_entry",
                "open_interest_if_known_at_entry",
            ],
            "denylist": [
                "order_book",
                "depth",
                "spread",
                "cvd",
                "ofi",
                "micro_lifecycle_state",
                "mfe_after_entry",
                "mae_after_entry",
                "exit_reason_as_entry_feature",
                "future_pnl",
            ],
        },
        "per_strategy_contract": _line_config_refs(cfg),
        "parameter_space_policy": {
            "without_micro": {
                "coarse_fields": [
                    "entry confidence threshold",
                    "rr / stop / take-profit profile",
                    "fast-exit profile",
                    "liquidity/slippage threshold already available from K-line plan context",
                ],
            },
            "strategy4": {
                "coarse_fields": [
                    "observe/recheck TTL",
                    "recheck timing",
                    "side-change/rejudge policy",
                    "rr / market-room thresholds",
                ],
            },
            "strategy5": {
                "coarse_fields": [
                    "direction evidence threshold",
                    "continuation / reversal labels",
                    "V5 gate compatible kline factors",
                    "rr / exit profile",
                ],
            },
            "strategy6": {
                "coarse_fields": [
                    "wait_confirm_bars",
                    "wait_pullback_min/max_bps",
                    "entry slippage threshold",
                    "adaptive exit / r-parity profile",
                    "V5 gate compatible kline factors",
                ],
            },
        },
        "training_sidecar_contract": {
            "root": "DATA/research/trade_snapshots",
            "db_path": "DATA/research/trade_snapshots/trade_snapshots.db",
            "required_source_modes": [
                "paper_equivalent_backtest",
                "sandbox_backtest",
                "sandbox_paper_shadow",
                "paper",
            ],
            "required_artifacts": [
                "training_dataset.jsonl",
                "dataset_manifest.json",
                "coverage_audit.json",
                "leakage_audit.json",
            ],
            "source_db_writeback_allowed": False,
        },
        "current_training_sidecar_status": _sidecar_status(),
        "outputs": {
            "manifest_path": str(MANIFEST_PATH.relative_to(ROOT)),
            "next_task": "STEP28.2",
        },
    }


def write_report(manifest: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"STEP28.1_optimization_universe_metric_contract_{_stamp()}.md"
    sidecar = manifest["current_training_sidecar_status"]
    lines = [
        "# STEP28.1 Optimization Universe Metric Contract",
        "",
        f"- generated_at: `{manifest['generated_at']}`",
        f"- manifest: `{manifest['outputs']['manifest_path']}`",
        f"- strategy_lines: `{', '.join(manifest['objective']['target_strategy_lines'])}`",
        f"- execution_contract_required: `{manifest['hard_constraints']['execution_contract_required']}`",
        f"- feature_scope: `{manifest['hard_constraints']['feature_scope']}`",
        f"- per_strategy_config_policy: `{manifest['hard_constraints']['per_strategy_config_policy']}`",
        f"- search_mode: `{manifest['hard_constraints']['search_mode']}`",
        f"- training_sidecar_db: `{sidecar['db_path']}` exists=`{sidecar['exists']}` samples=`{sidecar['sample_count']}` events=`{sidecar['event_count']}`",
        "",
        "## Formal Expectations",
        "",
        "1. 快速找出策略1/4/5/6 各自独立的最佳参数，并通过 Trade Quality 找到可复用到 paper 的 Trade Gate。",
        "2. 所有 paper-equivalent / sandbox / paper 输出必须自动落盘 P29 训练 sidecar DB，不修改主业务 DB。",
        "",
        "## Ranking",
        "",
        f"- primary: `{manifest['metrics']['primary_ranking_metric']}`",
        f"- secondary: `{', '.join(manifest['metrics']['secondary_metrics'])}`",
        f"- minimum_trade_count_fast_candidate: `{manifest['metrics']['minimum_trade_count_fast_candidate']}`",
        f"- minimum_trade_coverage_test: `{manifest['metrics']['minimum_trade_coverage_test']}`",
        "",
        "## Feature Boundary",
        "",
        f"- allowlist: `{', '.join(manifest['feature_policy']['allowlist'])}`",
        f"- denylist: `{', '.join(manifest['feature_policy']['denylist'])}`",
        "",
        "## Next",
        "",
        "- Start STEP28.2 with bounded paper-equivalent parameter search.",
        "- Each run must return `training_dataset` metadata from P29 automatic sync.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    manifest = build_manifest()
    _write_json(MANIFEST_PATH, manifest)
    report = write_report(manifest)
    print(json.dumps({"status": "ok", "manifest": str(MANIFEST_PATH), "report": str(report)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
