from __future__ import annotations

import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path


TQ_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_tq_STEP21_53_result.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_quality_filter_validation_STEP22_29.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _loads(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value) if value else default
    except Exception:
        return default


def _pf(rows: list[dict[str, Any]]) -> float | None:
    gp = sum(max(0.0, _num(row.get("net_R"))) for row in rows)
    gl = abs(sum(min(0.0, _num(row.get("net_R"))) for row in rows))
    if gl <= 0:
        return None if gp <= 0 else 999.0
    return round(gp / gl, 8)


def _stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"sample_count": 0, "pf": None, "expectancy_R": 0.0, "win_rate": 0.0, "total_R": 0.0}
    total = sum(_num(row.get("net_R")) for row in rows)
    wins = sum(1 for row in rows if _num(row.get("net_R")) > 0)
    return {
        "sample_count": len(rows),
        "pf": _pf(rows),
        "expectancy_R": round(total / len(rows), 8),
        "win_rate": round(wins / len(rows), 8),
        "total_R": round(total, 8),
    }


def _split(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    ordered = sorted(rows, key=lambda row: str(row.get("entry_time") or row.get("exit_time") or ""))
    n = len(ordered)
    a = max(1, int(n * 0.50))
    b = max(a + 1, int(n * 0.75)) if n > 2 else n
    return {"train": ordered[:a], "validation": ordered[a:b], "test": ordered[b:]}


def _dim_value(row: dict[str, Any], dim: str) -> str:
    if dim == "hour":
        ts = str(row.get("entry_time") or row.get("exit_time") or "")
        return ts[11:13] if len(ts) >= 13 else "unknown"
    return str(row.get(dim) or "unknown")


def _delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_pf = before.get("pf")
    after_pf = after.get("pf")
    pf_delta = None if before_pf is None or after_pf is None else round(float(after_pf) - float(before_pf), 8)
    return {
        "before": before,
        "after": after,
        "pf_delta": pf_delta,
        "expectancy_delta_R": round(_num(after.get("expectancy_R")) - _num(before.get("expectancy_R")), 8),
        "coverage_loss": round(1.0 - ((_num(after.get("sample_count")) / _num(before.get("sample_count"), 1.0)) if before.get("sample_count") else 0.0), 8),
    }


def _risk(train: dict[str, Any], validation: dict[str, Any], test: dict[str, Any], candidate_count: int) -> str:
    if candidate_count < 30:
        return "high_small_sample"
    deltas = [train.get("pf_delta"), validation.get("pf_delta"), test.get("pf_delta")]
    positive = [value for value in deltas if value is not None and value > 0]
    if len(positive) == 3:
        return "low"
    if len(positive) >= 2 and validation.get("pf_delta") is not None and validation["pf_delta"] > 0:
        return "medium"
    return "high"


def _load_rows() -> tuple[str, list[dict[str, Any]]]:
    payload = json.loads(TQ_RESULT_PATH.read_text(encoding="utf-8"))
    packages = payload.get("packages") or []
    package_keys = [pkg.get("package_key") for pkg in packages if pkg.get("package_key")]
    experiment_id = str(payload.get("experiment_id") or "")
    if not package_keys:
        return experiment_id, []
    db_path = p21_db_path(ROOT)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" for _ in package_keys)
        rows = conn.execute(
            f"""
            SELECT *
            FROM backtest_trade_quality_samples
            WHERE package_key IN ({placeholders})
            ORDER BY entry_time, symbol
            """,
            package_keys,
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["evidence"] = _loads(item.pop("evidence_json", None), {})
            item["source_payload"] = _loads(item.pop("source_payload_json", None), {})
            out.append(item)
        return experiment_id, out
    finally:
        conn.close()


def run() -> dict[str, Any]:
    experiment_id, rows = _load_rows()
    splits = _split(rows)
    split_baseline = {name: _stats(part) for name, part in splits.items()}
    baseline = _stats(rows)
    candidates: list[dict[str, Any]] = []
    for dim in ("symbol", "hour", "side"):
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[_dim_value(row, dim)].append(row)
        for key, items in grouped.items():
            item_stats = _stats(items)
            if item_stats["sample_count"] < 10 or _num(item_stats["expectancy_R"]) >= 0:
                continue
            split_results: dict[str, Any] = {}
            for split_name, split_rows in splits.items():
                before = _stats(split_rows)
                after = _stats([row for row in split_rows if _dim_value(row, dim) != key])
                split_results[split_name] = _delta(before, after)
            candidate_count = int(item_stats["sample_count"])
            risk = _risk(
                split_results["train"],
                split_results["validation"],
                split_results["test"],
                candidate_count,
            )
            candidates.append(
                {
                    "candidate_id": f"strategy6_v3_1_{dim}_{key}",
                    "dimension": dim,
                    "key": key,
                    "action": "shadow_block" if risk in {"low", "medium"} else "watch",
                    "sample_stats": item_stats,
                    "train": split_results["train"],
                    "validation": split_results["validation"],
                    "test": split_results["test"],
                    "overfit_risk": risk,
                }
            )
    candidates.sort(
        key=lambda item: (
            {"low": 0, "medium": 1, "high": 2, "high_small_sample": 3}.get(str(item["overfit_risk"]), 9),
            -_num((item["validation"] or {}).get("pf_delta"), -999.0),
            -_num((item["sample_stats"] or {}).get("sample_count")),
        )
    )
    return {
        "schema_version": "step22.29-strategy6-v3-1-quality-filter-validation-v1",
        "experiment_id": experiment_id,
        "sample_count": len(rows),
        "baseline": baseline,
        "split_baseline": split_baseline,
        "candidate_count": len(candidates),
        "candidates": candidates[:100],
        "generated_at": _now(),
    }


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP22.29_strategy6_v3_1_quality_filter_validation_{ts}.md"
    lines = [
        "# STEP22.29 Strategy6 V3.1 Quality Filter Validation Split",
        "",
        f"- generated_at: `{_now()}`",
        f"- experiment_id: `{payload.get('experiment_id')}`",
        f"- sample_count: `{payload.get('sample_count')}`",
        f"- baseline_pf: `{(payload.get('baseline') or {}).get('pf')}`",
        f"- baseline_expectancy_R: `{(payload.get('baseline') or {}).get('expectancy_R')}`",
        f"- candidate_count: `{payload.get('candidate_count')}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        "",
        "## Top Candidates",
        "",
        "| candidate | action | risk | samples | item_exp_R | train_pf_delta | val_pf_delta | test_pf_delta | coverage_loss_test |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in payload.get("candidates") or []:
        lines.append(
            f"| `{item.get('candidate_id')}` | `{item.get('action')}` | `{item.get('overfit_risk')}` | "
            f"{(item.get('sample_stats') or {}).get('sample_count')} | {(item.get('sample_stats') or {}).get('expectancy_R')} | "
            f"{(item.get('train') or {}).get('pf_delta')} | {(item.get('validation') or {}).get('pf_delta')} | "
            f"{(item.get('test') or {}).get('pf_delta')} | {(item.get('test') or {}).get('coverage_loss')} |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Shadow-only validation; no production config or Strategy6 runtime behavior was changed.",
            "- Candidates with high or small-sample risk must remain watch-only.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    payload = run()
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report_path = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report_path)}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
