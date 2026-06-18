from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21 import p21_db_path


V33_TQ_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_3_tq_STEP19_33_result.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_loss_unclassified_STEP19_34_result.json"
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


def _loads(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _load_packages() -> list[str]:
    data = _loads(V33_TQ_PATH.read_text(encoding="utf-8") if V33_TQ_PATH.exists() else "", {})
    out: list[str] = []
    for pkg in data.get("packages") or []:
        key = pkg.get("package_key")
        if key:
            out.append(str(key))
    return out


def _aligned(features: dict[str, Any], side: str, key: str) -> float:
    sign = -1.0 if side.upper() == "SHORT" else 1.0
    return sign * _num(features.get(key), 0.0)


def _label(row: sqlite3.Row) -> tuple[str, str]:
    mfe = _num(row["MFE_R"])
    mae = _num(row["MAE_R"])
    net_r = _num(row["net_R"])
    holding = _num(row["holding_minutes"])
    side = str(row["side"] or "").upper()
    payload = _loads(row["source_payload_json"], {})
    features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    a1 = _aligned(features, side, "pct_1m_bps")
    a3 = _aligned(features, side, "pct_3m_bps")
    a5 = _aligned(features, side, "pct_5m_bps")
    volume_z = _num(features.get("volume_z"), 1.0)

    if net_r >= 0:
        return "not_loss", "sample ended positive"
    if (a1 < -4.0 or a3 < -8.0) and mfe < 0.65:
        return "late_entry_reversal", "entry-time aligned momentum was already adverse"
    if mae >= 0.75 and mfe >= 0.45:
        return "early_adverse_then_recover", "large early adverse excursion before useful favorable move"
    if mfe >= 0.45 and mae < 0.8:
        return "mfe_half_r_then_revert", "trade reached near half-R but failed to lock profit"
    if holding <= 3 and 0.30 <= mfe < 0.60 and mae >= 0.55:
        return "range_noise_stop", "short hold with both modest MFE and noisy adverse path"
    if holding > 6 and mfe < 0.55 and mae < 0.85:
        return "slow_bleed_no_edge", "slow negative drift without enough favorable edge"
    if volume_z < 0.7 and a5 < 8.0:
        return "weak_volume_no_edge", "low volume and weak 5m follow-through at entry"
    return "unclear_path", "path does not match first-pass decomposition rules"


def _stats(rows: list[sqlite3.Row]) -> dict[str, Any]:
    if not rows:
        return {"count": 0}
    vals = [_num(r["net_R"]) for r in rows]
    mfes = [_num(r["MFE_R"]) for r in rows]
    maes = [_num(r["MAE_R"]) for r in rows]
    holds = [_num(r["holding_minutes"]) for r in rows]
    return {
        "count": len(rows),
        "avg_R": round(sum(vals) / len(vals), 8),
        "avg_MFE_R": round(sum(mfes) / len(mfes), 8),
        "avg_MAE_R": round(sum(maes) / len(maes), 8),
        "avg_holding_minutes": round(sum(holds) / len(holds), 8),
        "symbol_top": Counter(str(r["symbol"]) for r in rows).most_common(8),
        "side_counts": dict(Counter(str(r["side"]) for r in rows)),
        "hour_top": Counter(str(r["entry_time"] or "")[11:13] for r in rows if r["entry_time"]).most_common(8),
    }


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP19.34_strategy6_loss_unclassified_decomposition_{ts}.md"
    lines = [
        "# STEP19.34 Strategy6 Loss Unclassified Decomposition",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- source_packages: `{len(payload.get('package_keys') or [])}`",
        f"- total_loss_unclassified: `{payload.get('total_loss_unclassified')}`",
        f"- explained_ratio: `{payload.get('explained_ratio')}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        "",
        "| sub_label | count | avg_R | avg_MFE_R | avg_MAE_R | avg_hold_min | interpretation |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for label, block in (payload.get("sub_labels") or {}).items():
        stats = block.get("stats") or {}
        lines.append(
            f"| `{label}` | {stats.get('count')} | {stats.get('avg_R')} | {stats.get('avg_MFE_R')} | "
            f"{stats.get('avg_MAE_R')} | {stats.get('avg_holding_minutes')} | {block.get('meaning')} |"
        )
    lines.extend(
        [
            "",
            "## V3.4 Candidate Direction",
            "",
            "- `late_entry_reversal` and `weak_volume_no_edge` can become causal entry gates.",
            "- `mfe_half_r_then_revert` should be handled by sequential exit protection, not entry filtering.",
            "- `early_adverse_then_recover` suggests entry timing / rebound wait tuning, not hard direction deny.",
            "- `unclear_path` must remain diagnostic-only until more evidence exists.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    packages = _load_packages()
    if not packages:
        raise SystemExit("missing V3.3 TQ packages; run STEP19.33 first")
    placeholders = ",".join("?" for _ in packages)
    db = p21_db_path(ROOT)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT *
            FROM backtest_trade_quality_samples
            WHERE package_key IN ({placeholders}) AND root_cause = 'loss_unclassified'
            """,
            packages,
        ).fetchall()
    by_label: dict[str, list[sqlite3.Row]] = defaultdict(list)
    meanings: dict[str, str] = {}
    for row in rows:
        label, meaning = _label(row)
        by_label[label].append(row)
        meanings[label] = meaning
    clear_count = sum(len(v) for k, v in by_label.items() if k != "unclear_path")
    total = len(rows)
    payload = {
        "schema_version": "step19.34-strategy6-loss-unclassified-decomposition-v1",
        "generated_at": _now(),
        "package_keys": packages,
        "total_loss_unclassified": total,
        "explained_count": clear_count,
        "explained_ratio": round(clear_count / total, 8) if total else 0.0,
        "sub_labels": {
            label: {"meaning": meanings.get(label), "stats": _stats(label_rows)}
            for label, label_rows in sorted(by_label.items(), key=lambda item: len(item[1]), reverse=True)
        },
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report), "total": total, "explained_ratio": payload["explained_ratio"]}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
