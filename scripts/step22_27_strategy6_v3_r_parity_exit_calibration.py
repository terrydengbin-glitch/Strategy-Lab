from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path


STEP19_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_tq_STEP19_30_result.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_r_parity_STEP22_27.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def _metrics(values: list[float]) -> dict[str, Any]:
    wins = [v for v in values if v > 0]
    losses = [abs(v) for v in values if v < 0]
    gross_profit = round(sum(wins), 8)
    gross_loss = round(sum(losses), 8)
    return {
        "trade_count": len(values),
        "win_rate": round(len(wins) / len(values), 8) if values else 0.0,
        "profit_factor": round(gross_profit / gross_loss, 8) if gross_loss else (999.0 if gross_profit else None),
        "expectancy_R": _avg(values),
        "avg_win_R": _avg(wins),
        "avg_loss_R": _avg(losses),
        "total_R": round(sum(values), 8),
    }


def _load_top_package() -> tuple[str, str]:
    payload = json.loads(STEP19_RESULT_PATH.read_text(encoding="utf-8"))
    package = (payload.get("packages") or [])[0]
    return str(package["parameter_set_id"]), str(package["package_key"])


def _rows(package_key: str) -> list[dict[str, Any]]:
    conn = sqlite3.connect(p21_db_path(ROOT))
    conn.row_factory = sqlite3.Row
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT order_id, symbol, side, net_R, MFE_R, MAE_R, holding_minutes, root_cause
            FROM backtest_trade_quality_samples
            WHERE package_key = ? AND net_R IS NOT NULL
            ORDER BY exit_time_ms, order_id
            """,
            (package_key,),
        ).fetchall()
    ]


def _simulate_profile(rows: list[dict[str, Any]], profile: dict[str, Any]) -> dict[str, Any]:
    first_tp = _num(profile.get("first_tp_R"), 0.0)
    loss_cap = _num(profile.get("loss_cap_R"), 0.0)
    protect = _num(profile.get("protect_after_mfe_R"), 0.0)
    trail = _num(profile.get("trail_after_mfe_R"), 0.0)
    max_hold = _num(profile.get("max_hold_minutes"), 0.0)
    values: list[float] = []
    changed = 0
    for row in rows:
        net = _num(row.get("net_R"))
        mfe = _num(row.get("MFE_R"))
        mae = _num(row.get("MAE_R"))
        holding = _num(row.get("holding_minutes"))
        adjusted = net
        if first_tp > 0 and mfe >= first_tp:
            adjusted = first_tp
        elif max_hold > 0 and holding > max_hold:
            adjusted = max(net, -min(loss_cap or abs(net), abs(net)))
        elif loss_cap > 0 and net < 0:
            adjusted = max(net, -loss_cap)
        if protect > 0 and trail > 0 and mfe >= protect and net < protect - trail:
            adjusted = max(adjusted, max(0.0, protect - trail))
        if abs(adjusted - net) > 1e-9:
            changed += 1
        values.append(round(adjusted, 8))
    return {
        "profile_id": profile["profile_id"],
        "profile": profile,
        "changed_count": changed,
        "changed_ratio": round(changed / len(rows), 8) if rows else 0.0,
        "metrics": _metrics(values),
    }


def _profiles() -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for loss_cap in (0.55, 0.65, 0.75, 0.85, 0.95):
        for first_tp in (0.35, 0.45, 0.55, 0.65):
            profiles.append(
                {
                    "profile_id": f"rp_loss{loss_cap}_tp{first_tp}",
                    "loss_cap_R": loss_cap,
                    "first_tp_R": first_tp,
                    "protect_after_mfe_R": 0.0,
                    "trail_after_mfe_R": 0.0,
                    "max_hold_minutes": 120,
                }
            )
    profiles.extend(
        [
            {
                "profile_id": "rp_protect_050_020_loss075_tp045",
                "loss_cap_R": 0.75,
                "first_tp_R": 0.45,
                "protect_after_mfe_R": 0.50,
                "trail_after_mfe_R": 0.20,
                "max_hold_minutes": 90,
            },
            {
                "profile_id": "rp_protect_060_025_loss085_tp055",
                "loss_cap_R": 0.85,
                "first_tp_R": 0.55,
                "protect_after_mfe_R": 0.60,
                "trail_after_mfe_R": 0.25,
                "max_hold_minutes": 90,
            },
        ]
    )
    return profiles


def run() -> dict[str, Any]:
    parameter_set_id, package_key = _load_top_package()
    rows = _rows(package_key)
    baseline = _metrics([_num(row.get("net_R")) for row in rows])
    candidates = [_simulate_profile(rows, profile) for profile in _profiles()]
    candidates.sort(
        key=lambda item: (
            -float((item.get("metrics") or {}).get("profit_factor") or 0.0),
            -float((item.get("metrics") or {}).get("expectancy_R") or 0.0),
            str(item.get("profile_id")),
        )
    )
    payload = {
        "schema_version": "step22.27-strategy6-v3-r-parity-v1",
        "generated_at": _now(),
        "source_package_key": package_key,
        "source_parameter_set_id": parameter_set_id,
        "sample_count": len(rows),
        "baseline": baseline,
        "candidates": candidates,
        "best": candidates[0] if candidates else None,
        "notes": [
            "This is a shadow approximation from materialized MFE/MAE/net_R samples.",
            "It does not replace the paper-style 1m fill simulator used by STEP21.53.",
        ],
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP22.27_strategy6_v3_r_parity_exit_calibration_{ts}.md"
    baseline = payload.get("baseline") or {}
    best = payload.get("best") or {}
    lines = [
        "# STEP22.27 Strategy6 V3 R-Parity Exit Calibration",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- sample_count: `{payload.get('sample_count')}`",
        f"- source_parameter_set_id: `{payload.get('source_parameter_set_id')}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        "",
        "## Baseline",
        "",
        f"- PF: `{baseline.get('profit_factor')}`",
        f"- expectancy_R: `{baseline.get('expectancy_R')}`",
        f"- avg_win_R: `{baseline.get('avg_win_R')}`",
        f"- avg_loss_R: `{baseline.get('avg_loss_R')}`",
        "",
        "## Best Shadow Profile",
        "",
        f"- profile_id: `{best.get('profile_id')}`",
        f"- metrics: `{(best.get('metrics') or {})}`",
        "",
        "## Candidate Leaderboard",
        "",
        "| rank | profile | PF | expectancy_R | win_rate | avg_win_R | avg_loss_R | changed |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for rank, item in enumerate(payload.get("candidates") or [], start=1):
        metrics = item.get("metrics") or {}
        lines.append(
            f"| {rank} | `{item.get('profile_id')}` | {metrics.get('profit_factor')} | {metrics.get('expectancy_R')} | "
            f"{metrics.get('win_rate')} | {metrics.get('avg_win_R')} | {metrics.get('avg_loss_R')} | {item.get('changed_ratio')} |"
        )
        if rank >= 12:
            break
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Shadow-only R parity calibration; no live config or paper state is changed.",
            "- STEP21.53 must validate candidates with the real offline evaluator and 1m fill simulator.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    payload = run()
    report_path = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report_path)}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
