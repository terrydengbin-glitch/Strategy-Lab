from __future__ import annotations

import argparse
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


STEP19_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_tq_STEP19_30_result.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_direction_wrong_STEP22_26.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"missing json: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        out = float(value)
        if out != out or out in (float("inf"), float("-inf")):
            return default
        return out
    except Exception:
        return default


def _avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 8) if values else 0.0


def _ratio(part: int | float, whole: int | float) -> float:
    return round(float(part) / float(whole), 8) if whole else 0.0


def _side_sign(side: str) -> int:
    return -1 if str(side).upper() == "SHORT" else 1


def _adverse_bps(features: dict[str, Any], side: str, key: str) -> float:
    move = _num(features.get(key))
    # Positive means the pre-entry move was against the trade side.
    return max(0.0, -_side_sign(side) * move)


def _range_extreme(features: dict[str, Any], side: str) -> bool:
    pos = _num(features.get("range_pos_30m"), 0.5)
    side_u = str(side).upper()
    return (side_u == "LONG" and pos >= 0.72) or (side_u == "SHORT" and pos <= 0.28)


def _btc_against(features: dict[str, Any], side: str) -> bool:
    state = str(features.get("strategy6_btc_alignment") or "").lower()
    if state in {"", "unknown", "missing", "neutral"}:
        return False
    if "against" in state or "opposite" in state:
        return True
    side_u = str(side).upper()
    if side_u == "LONG":
        return any(token in state for token in ("bear", "down", "short"))
    if side_u == "SHORT":
        return any(token in state for token in ("bull", "up", "long"))
    return False


def classify_subcause(row: sqlite3.Row) -> tuple[str, dict[str, Any]]:
    source = json.loads(row["source_payload_json"] or "{}")
    features = source.get("features") if isinstance(source.get("features"), dict) else {}
    side = str(row["side"] or "").upper()
    adverse_1m = _adverse_bps(features, side, "pct_1m_bps")
    adverse_3m = _adverse_bps(features, side, "pct_3m_bps")
    adverse_5m = _adverse_bps(features, side, "pct_5m_bps")
    volume_z = _num(features.get("volume_z"), 0.0)
    direction_score = _num(features.get("strategy6_direction_context_score"), 0.0)
    market_score = _num(features.get("strategy6_market_acceptance_score"), 0.0)
    fake_breakout = bool(features.get("strategy6_fake_breakout_flag"))
    evidence = {
        "adverse_1m_bps": round(adverse_1m, 6),
        "adverse_3m_bps": round(adverse_3m, 6),
        "adverse_5m_bps": round(adverse_5m, 6),
        "volume_z": volume_z,
        "range_pos_30m": _num(features.get("range_pos_30m"), 0.5),
        "direction_context_score": direction_score,
        "market_acceptance_score": market_score,
        "btc_alignment": features.get("strategy6_btc_alignment"),
        "fake_breakout_flag": fake_breakout,
    }
    if adverse_1m >= 12.0:
        return "direction_wrong_reverse_1m", evidence
    if adverse_3m >= 24.0:
        return "direction_wrong_reverse_3m", evidence
    if _btc_against(features, side):
        return "direction_wrong_btc_against", evidence
    if fake_breakout:
        return "direction_wrong_fake_breakout", evidence
    if _range_extreme(features, side):
        return "direction_wrong_range_extreme", evidence
    if adverse_5m >= 35.0 or volume_z < 0.75 or direction_score < 70.0 or market_score < 65.0:
        return "direction_wrong_low_followthrough", evidence
    return "direction_wrong_unknown", evidence


def _stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    r_values = [_num(row.get("net_R")) for row in rows if row.get("net_R") is not None]
    mfe_values = [_num(row.get("MFE_R")) for row in rows if row.get("MFE_R") is not None]
    mae_values = [_num(row.get("MAE_R")) for row in rows if row.get("MAE_R") is not None]
    return {
        "count": len(rows),
        "loss_count": sum(1 for value in r_values if value < 0),
        "avg_R": _avg(r_values),
        "avg_MFE_R": _avg(mfe_values),
        "avg_MAE_R": _avg(mae_values),
    }


def _package_keys(step19: dict[str, Any], top_n: int) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for package in step19.get("packages") or []:
        key = package.get("package_key")
        param = package.get("parameter_set_id")
        if key and param:
            out.append((str(param), str(key)))
        if len(out) >= top_n:
            break
    return out


def run(*, top_n: int) -> dict[str, Any]:
    step19 = _load_json(STEP19_RESULT_PATH)
    packages = _package_keys(step19, top_n)
    db_path = p21_db_path(ROOT)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    package_results: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    for parameter_set_id, package_key in packages:
        rows = conn.execute(
            """
            SELECT *
            FROM backtest_trade_quality_samples
            WHERE package_key = ? AND root_cause = 'direction_wrong'
            ORDER BY exit_time_ms, order_id
            """,
            (package_key,),
        ).fetchall()
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            subcause, evidence = classify_subcause(row)
            item = {
                "parameter_set_id": parameter_set_id,
                "package_key": package_key,
                "order_id": row["order_id"],
                "symbol": row["symbol"],
                "side": row["side"],
                "entry_time": row["entry_time"],
                "net_R": row["net_R"],
                "MFE_R": row["MFE_R"],
                "MAE_R": row["MAE_R"],
                "subcause": subcause,
                "evidence": evidence,
            }
            grouped[subcause].append(item)
            all_rows.append(item)
        subtotals = []
        for subcause, items in grouped.items():
            subtotals.append({"subcause": subcause, **_stats(items), "ratio": _ratio(len(items), len(rows))})
        subtotals.sort(key=lambda item: (-int(item["count"]), str(item["subcause"])))
        package_results.append(
            {
                "parameter_set_id": parameter_set_id,
                "package_key": package_key,
                "direction_wrong_count": len(rows),
                "assigned_ratio": _ratio(sum(1 for row in all_rows if row["subcause"] != "direction_wrong_unknown"), len(all_rows)),
                "subcauses": subtotals,
            }
        )
    all_grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in all_rows:
        all_grouped[item["subcause"]].append(item)
    overall = []
    for subcause, items in all_grouped.items():
        overall.append({"subcause": subcause, **_stats(items), "ratio": _ratio(len(items), len(all_rows))})
    overall.sort(key=lambda item: (-int(item["count"]), str(item["subcause"])))
    payload = {
        "schema_version": "step22.26-strategy6-v3-direction-wrong-drilldown-v1",
        "generated_at": _now(),
        "source_step19_result": str(STEP19_RESULT_PATH.relative_to(ROOT)),
        "db_path": str(db_path.relative_to(ROOT)),
        "package_count": len(packages),
        "sample_count": len(all_rows),
        "assigned_ratio": _ratio(sum(1 for row in all_rows if row["subcause"] != "direction_wrong_unknown"), len(all_rows)),
        "overall": overall,
        "packages": package_results,
        "gate_candidate_features": [
            "side_adjusted_pct_1m_bps",
            "side_adjusted_pct_3m_bps",
            "side_adjusted_pct_5m_bps",
            "volume_z",
            "range_pos_30m",
            "strategy6_direction_context_score",
            "strategy6_market_acceptance_score",
            "strategy6_btc_alignment",
            "strategy6_fake_breakout_flag",
        ],
        "notes": [
            "MFE_R and MAE_R are target diagnostics only; they are not used as gate inputs.",
            "Subcause classification uses source_payload_json.features captured at entry/backtest signal time.",
        ],
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP22.26_strategy6_v3_direction_wrong_drilldown_{ts}.md"
    lines = [
        "# STEP22.26 Strategy6 V3 Direction Wrong Drilldown",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- sample_count: `{payload.get('sample_count')}`",
        f"- assigned_ratio: `{payload.get('assigned_ratio')}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        "",
        "## Overall Subcauses",
        "",
        "| subcause | count | ratio | avg_R | avg_MFE_R | avg_MAE_R |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in payload.get("overall") or []:
        lines.append(
            f"| `{item.get('subcause')}` | {item.get('count')} | {item.get('ratio')} | "
            f"{item.get('avg_R')} | {item.get('avg_MFE_R')} | {item.get('avg_MAE_R')} |"
        )
    lines.extend(
        [
            "",
            "## Gate Candidate Features",
            "",
        ]
    )
    for feature in payload.get("gate_candidate_features") or []:
        lines.append(f"- `{feature}`")
    lines.extend(
        [
            "",
            "## Package Detail",
            "",
        ]
    )
    for package in payload.get("packages") or []:
        lines.append(f"### `{package.get('parameter_set_id')}`")
        lines.append(f"- direction_wrong_count: `{package.get('direction_wrong_count')}`")
        lines.append("| subcause | count | ratio | avg_R | avg_MFE_R | avg_MAE_R |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
        for item in package.get("subcauses") or []:
            lines.append(
                f"| `{item.get('subcause')}` | {item.get('count')} | {item.get('ratio')} | "
                f"{item.get('avg_R')} | {item.get('avg_MFE_R')} | {item.get('avg_MAE_R')} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Boundary",
            "",
            "- This is diagnostic-only analysis for Strategy6 V3.1 tuning.",
            "- It does not write live config, paper orders, or runtime strategy state.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=5)
    args = parser.parse_args()
    payload = run(top_n=args.top_n)
    report_path = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report_path)}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
