from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from statistics import mean
from typing import Any


SCHEMA_VERSION = "7.122-tq-v4-combo-gate-holdout"
ALLOWED_RULE_FIELDS = {
    "strategy_line",
    "symbol",
    "side",
    "entry_hour_utc",
    "entry_session",
    "rsi_bucket",
    "bollinger_bucket",
    "spread_bucket",
    "volume_z_bucket",
    "side_flow_alignment",
    "price_flow_alignment",
    "cvd_proxy_state",
    "ofi_proxy_state",
    "entry_price_context",
}
FORBIDDEN_RULE_FIELDS = {
    "net_R",
    "mfe_R",
    "mae_R",
    "MFE_R",
    "MAE_R",
    "exit_reason",
    "root_cause",
    "deep_subcause",
    "is_loss",
    "is_win",
    "final_pnl",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _stable_id(prefix: str, payload: Any, size: int = 22) -> str:
    return f"{prefix}_{hashlib.sha256(_json(payload).encode('utf-8')).hexdigest()[:size]}"


def _loads(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value in (None, ""):
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _pf(values: list[float]) -> float | None:
    gross_profit = sum(v for v in values if v > 0)
    gross_loss = abs(sum(v for v in values if v < 0))
    if gross_loss <= 0:
        return None if gross_profit <= 0 else 999.0
    return gross_profit / gross_loss


def _max_drawdown(values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def _expectancy(values: list[float]) -> float | None:
    return mean(values) if values else None


def _pct(value: float | None) -> str:
    return "-" if value is None else f"{value * 100:.1f}%"


def _fmt(value: float | None, digits: int = 3) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def _bucket_session(hour: int | None) -> str | None:
    if hour is None:
        return None
    if 0 <= hour < 8:
        return "asia_early"
    if 8 <= hour < 16:
        return "eu_us_overlap"
    return "us_late"


def _bucket_volume_z(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 6:
        return "volume_extreme"
    if value >= 3:
        return "volume_high"
    if value >= 1:
        return "volume_normal"
    return "volume_low"


def _bucket_spread(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 8:
        return "spread_very_high"
    if value >= 5:
        return "spread_high"
    if value >= 2:
        return "spread_mid"
    return "spread_low"


def _bucket_rsi(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 70:
        return "rsi_high"
    if value <= 30:
        return "rsi_low"
    return "rsi_mid"


def _bucket_bollinger(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 0.85:
        return "boll_high"
    if value <= 0.15:
        return "boll_low"
    return "boll_mid"


def _bucket_entry_price(features: dict[str, Any]) -> str | None:
    vwap = _safe_float(features.get("vwap_distance_bps"))
    ema20 = _safe_float(features.get("ema20_distance_bps"))
    value = vwap if vwap is not None else ema20
    if value is None:
        return None
    av = abs(value)
    if av >= 120:
        return "far_from_mean"
    if av >= 50:
        return "extended_from_mean"
    return "near_mean"


def _flow_state(value: Any) -> str | None:
    if value in (None, "", "unknown"):
        return None
    return str(value)


@dataclass(frozen=True)
class Sample:
    feature_id: str
    strategy_line: str
    package_key: str
    parameter_set_id: str
    symbol: str
    side: str
    entry_time_ms: int
    net_r: float
    buckets: dict[str, str]


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_quality_combo_gate_validations_v4(
          validation_id TEXT PRIMARY KEY,
          package_key TEXT NOT NULL,
          parameter_set_id TEXT NOT NULL,
          strategy_line TEXT NOT NULL,
          status TEXT NOT NULL,
          rule_json TEXT NOT NULL,
          feature_scope_json TEXT NOT NULL,
          split_metrics_json TEXT NOT NULL,
          aggregate_metrics_json TEXT NOT NULL,
          config_patch_preview_json TEXT NOT NULL,
          leakage_check_status TEXT NOT NULL,
          overfit_risk TEXT NOT NULL,
          recommendation TEXT NOT NULL,
          schema_version TEXT NOT NULL,
          generated_at TEXT NOT NULL,
          UNIQUE(package_key, parameter_set_id, strategy_line, rule_json, schema_version)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tq_combo_gate_v4_rank
        ON trade_quality_combo_gate_validations_v4(strategy_line, status, overfit_risk)
        """
    )


def _sample_from_row(row: sqlite3.Row) -> Sample | None:
    features = _loads(row["features_json"], {})
    targets = _loads(row["targets_json"], {})
    net_r = _safe_float(targets.get("net_R"))
    if net_r is None:
        return None
    entry_ms = row["entry_time_ms"]
    if entry_ms is None:
        return None
    hour_raw = features.get("entry_hour_utc")
    try:
        hour = int(hour_raw) if hour_raw is not None else None
    except Exception:
        hour = None
    buckets: dict[str, str] = {}
    base = {
        "strategy_line": row["strategy_line"],
        "symbol": row["symbol"],
        "side": row["side"],
        "entry_hour_utc": str(hour) if hour is not None else None,
        "entry_session": _bucket_session(hour),
        "rsi_bucket": _bucket_rsi(_safe_float(features.get("rsi_14"))),
        "bollinger_bucket": _bucket_bollinger(_safe_float(features.get("bollinger_position"))),
        "spread_bucket": _bucket_spread(_safe_float(features.get("spread_bps"))),
        "volume_z_bucket": _bucket_volume_z(_safe_float(features.get("volume_z"))),
        "side_flow_alignment": _flow_state(features.get("side_flow_alignment")),
        "price_flow_alignment": _flow_state(features.get("price_flow_alignment")),
        "cvd_proxy_state": "cvd_positive" if (_safe_float(features.get("cvd_proxy_5m")) or 0) > 0 else "cvd_negative"
        if features.get("cvd_proxy_5m") is not None
        else None,
        "ofi_proxy_state": _flow_state(features.get("ofi_proxy_state")),
        "entry_price_context": _bucket_entry_price(features),
    }
    for key, value in base.items():
        if key in ALLOWED_RULE_FIELDS and value not in (None, "", "unknown"):
            buckets[key] = str(value)
    return Sample(
        feature_id=row["feature_id"],
        strategy_line=row["strategy_line"],
        package_key=row["package_key"],
        parameter_set_id=row["parameter_set_id"],
        symbol=row["symbol"],
        side=row["side"],
        entry_time_ms=int(entry_ms),
        net_r=net_r,
        buckets=buckets,
    )


def _load_samples(conn: sqlite3.Connection) -> list[Sample]:
    rows = conn.execute(
        """
        SELECT *
        FROM trade_quality_entry_evidence_v4
        WHERE strategy_line IN ('strategy5', 'strategy6')
        ORDER BY strategy_line, entry_time_ms, feature_id
        """
    ).fetchall()
    out: list[Sample] = []
    for row in rows:
        sample = _sample_from_row(row)
        if sample:
            out.append(sample)
    return out


def _split_samples(samples: list[Sample]) -> dict[str, list[Sample]]:
    ordered = sorted(samples, key=lambda s: (s.entry_time_ms, s.feature_id))
    n = len(ordered)
    if n < 10:
        return {"train": ordered, "validation": [], "test": []}
    train_end = max(1, int(n * 0.60))
    validation_end = max(train_end + 1, int(n * 0.80))
    return {
        "train": ordered[:train_end],
        "validation": ordered[train_end:validation_end],
        "test": ordered[validation_end:],
    }


def _metrics(samples: list[Sample], rule: dict[str, str] | None = None) -> dict[str, Any]:
    if rule:
        removed = [s for s in samples if all(s.buckets.get(k) == v for k, v in rule.items())]
        kept = [s for s in samples if not all(s.buckets.get(k) == v for k, v in rule.items())]
    else:
        removed = []
        kept = samples
    values = [s.net_r for s in kept]
    before_values = [s.net_r for s in samples]
    removed_values = [s.net_r for s in removed]
    return {
        "sample_count": len(samples),
        "retained_trades": len(kept),
        "removed_trades": len(removed),
        "coverage": (len(removed) / len(samples)) if samples else 0.0,
        "pf_before": _pf(before_values),
        "pf_after": _pf(values),
        "expectancy_before": _expectancy(before_values),
        "expectancy_after": _expectancy(values),
        "removed_avg_R": _expectancy(removed_values),
        "removed_total_R": sum(removed_values),
        "total_R_before": sum(before_values),
        "total_R_after": sum(values),
        "max_dd_before": _max_drawdown(before_values),
        "max_dd_after": _max_drawdown(values),
    }


def _candidate_rules(train: list[Sample], *, min_samples: int) -> list[dict[str, Any]]:
    dim_sets = [
        ("side", "entry_session"),
        ("side", "entry_hour_utc"),
        ("side", "spread_bucket"),
        ("side", "volume_z_bucket"),
        ("side", "rsi_bucket"),
        ("side", "bollinger_bucket"),
        ("side", "side_flow_alignment"),
        ("side", "price_flow_alignment"),
        ("side", "entry_price_context"),
        ("side", "entry_session", "spread_bucket"),
        ("side", "entry_session", "volume_z_bucket"),
        ("side", "bollinger_bucket", "side_flow_alignment"),
        ("side", "entry_price_context", "price_flow_alignment"),
        ("symbol",),
        ("symbol", "side"),
        ("symbol", "entry_session"),
    ]
    # Keep a few single global dimensions, but avoid broad all-line removal later by coverage limits.
    dim_sets += [("entry_session",), ("spread_bucket",), ("volume_z_bucket",), ("bollinger_bucket",), ("rsi_bucket",)]
    grouped: dict[tuple[tuple[str, str], ...], list[Sample]] = defaultdict(list)
    for sample in train:
        for dims in dim_sets:
            if not all(dim in sample.buckets for dim in dims):
                continue
            rule = tuple(sorted((dim, sample.buckets[dim]) for dim in dims))
            grouped[rule].append(sample)
    out = []
    for rule_items, group in grouped.items():
        if len(group) < min_samples:
            continue
        rule = dict(rule_items)
        coverage = len(group) / len(train) if train else 0
        if coverage < 0.01 or coverage > 0.50:
            continue
        values = [s.net_r for s in group]
        avg_r = mean(values)
        total_r = sum(values)
        if avg_r >= -0.03 or total_r >= 0:
            continue
        baseline = _metrics(train)
        after = _metrics(train, rule)
        pf_before = baseline["pf_before"] or 0.0
        pf_after = after["pf_after"] or 0.0
        if pf_after <= pf_before:
            continue
        out.append(
            {
                "rule": rule,
                "train_removed": len(group),
                "train_removed_avg_R": avg_r,
                "train_removed_total_R": total_r,
                "train_pf_delta": pf_after - pf_before,
            }
        )
    out.sort(key=lambda r: (r["train_pf_delta"], -r["train_removed_total_R"], r["train_removed"]), reverse=True)
    return out[:250]


def _overfit_risk(split_metrics: dict[str, dict[str, Any]], rule: dict[str, str]) -> tuple[str, str, str]:
    rule_fields = set(rule)
    if rule_fields & FORBIDDEN_RULE_FIELDS:
        return "reject", "failed_target_field_detected", "reject_for_leakage"
    if not rule_fields <= ALLOWED_RULE_FIELDS:
        return "reject", "failed_unknown_rule_field", "reject_for_contract"
    test = split_metrics.get("test") or {}
    validation = split_metrics.get("validation") or {}
    train = split_metrics.get("train") or {}
    test_before = test.get("pf_before") or 0.0
    test_after = test.get("pf_after") or 0.0
    validation_before = validation.get("pf_before") or 0.0
    validation_after = validation.get("pf_after") or 0.0
    train_cov = train.get("coverage") or 0.0
    validation_cov = validation.get("coverage") or 0.0
    test_cov = test.get("coverage") or 0.0
    if train_cov > 0.50 or validation_cov > 0.50 or test_cov > 0.50:
        return "shadow_reject", "pass_entry_known_rule_only", "high_overbroad_gate"
    if test.get("removed_trades", 0) < 10 or validation.get("removed_trades", 0) < 10:
        return "shadow_reject", "pass_entry_known_rule_only", "high_low_holdout_coverage"
    if test_after <= test_before:
        return "shadow_reject", "pass_entry_known_rule_only", "high_train_only_or_test_failed"
    if validation_after <= validation_before:
        return "shadow_watch", "pass_entry_known_rule_only", "medium_validation_failed"
    if abs(train_cov - test_cov) > 0.20:
        return "shadow_watch", "pass_entry_known_rule_only", "medium_coverage_shift"
    return "shadow_candidate", "pass_entry_known_rule_only", "low_holdout_confirmed"


def _evaluate_strategy(samples: list[Sample], strategy_line: str, *, min_samples: int) -> list[dict[str, Any]]:
    line_samples = [s for s in samples if s.strategy_line == strategy_line]
    splits = _split_samples(line_samples)
    candidates = _candidate_rules(splits["train"], min_samples=min_samples)
    rows = []
    for item in candidates:
        rule = item["rule"]
        split_metrics = {name: _metrics(split, rule) for name, split in splits.items()}
        baseline_metrics = {name: _metrics(split) for name, split in splits.items()}
        status, leakage, risk = _overfit_risk(split_metrics, rule)
        package_key = line_samples[0].package_key if line_samples else ""
        parameter_set_id = line_samples[0].parameter_set_id if line_samples else ""
        validation_id = _stable_id("tqv4combo", [strategy_line, package_key, parameter_set_id, rule])
        test = split_metrics["test"]
        test_before = test.get("pf_before") or 0.0
        test_after = test.get("pf_after") or 0.0
        rows.append(
            {
                "validation_id": validation_id,
                "package_key": package_key,
                "parameter_set_id": parameter_set_id,
                "strategy_line": strategy_line,
                "status": status,
                "rule": rule,
                "feature_scope": {
                    "entry_known_only": True,
                    "uses_targets": False,
                    "allowed_rule_fields": sorted(rule),
                    "forbidden_target_fields_checked": sorted(FORBIDDEN_RULE_FIELDS),
                },
                "split_metrics": split_metrics,
                "baseline_metrics": baseline_metrics,
                "aggregate_metrics": {
                    "test_pf_delta": test_after - test_before,
                    "validation_pf_delta": (split_metrics["validation"].get("pf_after") or 0.0)
                    - (split_metrics["validation"].get("pf_before") or 0.0),
                    "train_pf_delta": (split_metrics["train"].get("pf_after") or 0.0)
                    - (split_metrics["train"].get("pf_before") or 0.0),
                    "test_expectancy_delta_R": (test.get("expectancy_after") or 0.0)
                    - (test.get("expectancy_before") or 0.0),
                },
                "config_patch_preview": {"trade_quality_gate": {"mode": "shadow", "rules": [{"action": "shadow_block_or_downweight", **rule}]}},
                "leakage_check_status": leakage,
                "overfit_risk": risk,
                "recommendation": "eligible_for_paper_shadow_review" if status == "shadow_candidate" else "do_not_promote",
                "generated_at": _now(),
            }
        )
    rows.sort(
        key=lambda r: (
            1 if r["status"] == "shadow_candidate" else 0,
            r["aggregate_metrics"].get("test_pf_delta") or -999,
            r["aggregate_metrics"].get("validation_pf_delta") or -999,
        ),
        reverse=True,
    )
    return rows


def _persist(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    _ensure_table(conn)
    conn.execute("DELETE FROM trade_quality_combo_gate_validations_v4 WHERE schema_version = ?", (SCHEMA_VERSION,))
    for row in rows:
        conn.execute(
            """
            INSERT OR REPLACE INTO trade_quality_combo_gate_validations_v4
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                row["validation_id"],
                row["package_key"],
                row["parameter_set_id"],
                row["strategy_line"],
                row["status"],
                _json(row["rule"]),
                _json(row["feature_scope"]),
                _json(row["split_metrics"]),
                _json(row["aggregate_metrics"]),
                _json(row["config_patch_preview"]),
                row["leakage_check_status"],
                row["overfit_risk"],
                row["recommendation"],
                SCHEMA_VERSION,
                row["generated_at"],
            ),
        )


def _report(rows: list[dict[str, Any]], samples: list[Sample]) -> str:
    lines = [
        "# STEP7.122 Strategy5/6 V4 Combo Gate Holdout Validation",
        f"> generated_at: {_now()}",
        "",
        "## Summary",
        "",
        f"- V4 samples evaluated: {len(samples)}",
        f"- Candidate validations: {len(rows)}",
        f"- Shadow candidates passing holdout: {sum(1 for r in rows if r['status'] == 'shadow_candidate')}",
        "- Boundary: shadow-only; no live config, strategy, paper ledger, or sandbox mutation.",
        "- Leakage guard: candidate rules are built only from entry-known V4 feature buckets.",
        "",
        "## Strategy Coverage",
    ]
    for line in ("strategy5", "strategy6"):
        line_samples = [s for s in samples if s.strategy_line == line]
        splits = _split_samples(line_samples)
        baseline = {name: _metrics(split) for name, split in splits.items()}
        lines.append(
            f"- {line}: samples={len(line_samples)} "
            f"train={len(splits['train'])} validation={len(splits['validation'])} test={len(splits['test'])} "
            f"test_pf={_fmt(baseline['test'].get('pf_before'))} test_expectancy={_fmt(baseline['test'].get('expectancy_before'))}R"
        )
    lines.extend(["", "## Holdout-Passed Shadow Candidates"])
    passed = [r for r in rows if r["status"] == "shadow_candidate"]
    if not passed:
        lines.append("- No combo gate candidate passed both validation and test holdout.")
    for strategy_line in ("strategy5", "strategy6"):
        strategy_passed = [r for r in passed if r["strategy_line"] == strategy_line]
        lines.append(f"### {strategy_line}")
        if not strategy_passed:
            lines.append("- No holdout-passed candidate for this strategy.")
            continue
        for row in strategy_passed[:10]:
            test = row["split_metrics"]["test"]
            val = row["split_metrics"]["validation"]
            lines.append(
                f"- rule={row['rule']} "
                f"test_pf {_fmt(test.get('pf_before'))} -> {_fmt(test.get('pf_after'))} "
                f"test_coverage={_pct(test.get('coverage'))} removed={test.get('removed_trades')} "
                f"validation_pf {_fmt(val.get('pf_before'))} -> {_fmt(val.get('pf_after'))} "
                f"risk={row['overfit_risk']}"
            )
    lines.extend(["", "## Top Rejected / Watch Candidates"])
    for row in [r for r in rows if r["status"] != "shadow_candidate"][:20]:
        test = row["split_metrics"]["test"]
        val = row["split_metrics"]["validation"]
        lines.append(
            f"- {row['strategy_line']} {row['status']} rule={row['rule']} "
            f"test_pf {_fmt(test.get('pf_before'))} -> {_fmt(test.get('pf_after'))} "
            f"validation_pf {_fmt(val.get('pf_before'))} -> {_fmt(val.get('pf_after'))} "
            f"risk={row['overfit_risk']}"
        )
    lines.extend(
        [
            "",
            "## Contract Check",
            "",
            "- Allowed rule fields are limited to entry-known V4 buckets.",
            "- Target fields such as `net_R`, `MFE_R`, `MAE_R`, `root_cause`, `deep_subcause`, and `exit_reason` are used only for scoring.",
            "- All candidates remain `shadow` and require later paper-shadow or sandbox promotion review.",
            "",
            "## Next Step",
            "",
            "- Use only holdout-passed candidates as inputs for a later sandbox paper-shadow task.",
            "- Re-run with larger Strategy6 reference samples before promoting any Strategy6-specific symbol/time gate.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP7.122 TQ V4 combo gate holdout validation.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--report", default="")
    parser.add_argument("--strategy5-min-samples", type=int, default=150)
    parser.add_argument("--strategy6-min-samples", type=int, default=35)
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    sys.path.insert(0, str(root))

    from laoma_signal_engine.backtest.p21 import p21_db_path
    from laoma_signal_engine.backtest.p21_trade_quality_v4 import ensure_trade_quality_v4_tables

    db_path = p21_db_path(root)
    ensure_trade_quality_v4_tables(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_table(conn)
        samples = _load_samples(conn)
        rows: list[dict[str, Any]] = []
        rows.extend(_evaluate_strategy(samples, "strategy5", min_samples=args.strategy5_min_samples))
        rows.extend(_evaluate_strategy(samples, "strategy6", min_samples=args.strategy6_min_samples))
        _persist(conn, rows)
        conn.commit()
    finally:
        conn.close()

    report_name = args.report or f"docs/reports/STEP7.122_strategy5_6_v4_combo_gate_holdout_validation_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.md"
    report_path = root / report_name
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(_report(rows, samples), encoding="utf-8")
    print(f"db={db_path}")
    print(f"report={report_path}")
    print(f"samples={len(samples)} candidates={len(rows)} passed={sum(1 for r in rows if r['status'] == 'shadow_candidate')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
