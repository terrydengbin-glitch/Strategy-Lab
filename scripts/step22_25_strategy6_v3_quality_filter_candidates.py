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


TQ_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_tq_STEP19_30_result.json"
OUTPUT_JSON = ROOT / "DATA" / "backtest" / "strategy6_v3_quality_filter_candidates_STEP22_25.json"
OUTPUT_DB = ROOT / "DATA" / "backtest" / "strategy6_v3_quality_filter_candidates_STEP22_25.sqlite"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"missing input: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _candidate(row: dict[str, Any], *, dimension: str, min_samples: int, max_avg_r: float, max_win_rate: float) -> dict[str, Any]:
    trade_count = int(row.get("trade_count") or row.get("sample_count") or 0)
    avg_r = float(row.get("avg_R") or row.get("avg_net_R") or 0.0)
    win_rate = float(row.get("win_rate") or 0.0)
    low_confidence = trade_count < min_samples
    should_shadow = avg_r <= max_avg_r or win_rate <= max_win_rate
    if low_confidence:
        tier = "watch_low_confidence"
    elif should_shadow:
        tier = "shadow_bad"
    else:
        tier = "pass"
    return {
        "dimension": dimension,
        "key": str(row.get("key") or ""),
        "tier": tier,
        "sample_count": trade_count,
        "avg_R": avg_r,
        "win_rate": win_rate,
        "loss_count": int(row.get("loss_count") or 0),
        "top_root_cause": row.get("top_root_cause"),
        "confidence": "low" if low_confidence else "normal",
        "reason_codes": [
            code
            for code, enabled in (
                ("strategy6_v3_quality_low_sample", low_confidence),
                ("strategy6_v3_quality_negative_avg_r", avg_r <= max_avg_r),
                ("strategy6_v3_quality_low_win_rate", win_rate <= max_win_rate),
            )
            if enabled
        ],
    }


def build_candidates(payload: dict[str, Any], *, min_samples: int, max_avg_r: float, max_win_rate: float) -> dict[str, Any]:
    packages: list[dict[str, Any]] = []
    for package in payload.get("packages") or []:
        summary = ((package.get("summary") or {}).get("summary") or {})
        dims = summary.get("dimension_attribution") or {}
        items: list[dict[str, Any]] = []
        for dimension in ("symbol", "hour_bucket", "side"):
            for row in (dims.get(dimension) or []):
                items.append(_candidate(row, dimension=dimension, min_samples=min_samples, max_avg_r=max_avg_r, max_win_rate=max_win_rate))
        packages.append(
            {
                "parameter_set_id": package.get("parameter_set_id"),
                "package_key": package.get("package_key"),
                "candidates": items,
                "shadow_bad_count": sum(1 for item in items if item["tier"] == "shadow_bad"),
                "watch_low_confidence_count": sum(1 for item in items if item["tier"] == "watch_low_confidence"),
            }
        )
    return {
        "schema_version": "step22.25-strategy6-v3-quality-filter-candidates-v1",
        "generated_at": _now(),
        "source_tq_result": str(TQ_RESULT_PATH),
        "mode": "shadow_only",
        "criteria": {
            "min_samples": min_samples,
            "max_avg_R": max_avg_r,
            "max_win_rate": max_win_rate,
        },
        "packages": packages,
    }


def write_sqlite(payload: dict[str, Any]) -> None:
    OUTPUT_DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(OUTPUT_DB) as con:
        con.execute(
            """
            create table if not exists strategy6_v3_quality_candidates (
                parameter_set_id text,
                package_key text,
                dimension text,
                key text,
                tier text,
                sample_count integer,
                avg_R real,
                win_rate real,
                loss_count integer,
                top_root_cause text,
                confidence text,
                reason_codes_json text,
                generated_at text,
                primary key (parameter_set_id, dimension, key)
            )
            """
        )
        for package in payload.get("packages") or []:
            for item in package.get("candidates") or []:
                con.execute(
                    """
                    insert or replace into strategy6_v3_quality_candidates (
                        parameter_set_id, package_key, dimension, key, tier, sample_count,
                        avg_R, win_rate, loss_count, top_root_cause, confidence,
                        reason_codes_json, generated_at
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        package.get("parameter_set_id"),
                        package.get("package_key"),
                        item.get("dimension"),
                        item.get("key"),
                        item.get("tier"),
                        item.get("sample_count"),
                        item.get("avg_R"),
                        item.get("win_rate"),
                        item.get("loss_count"),
                        item.get("top_root_cause"),
                        item.get("confidence"),
                        json.dumps(item.get("reason_codes") or [], ensure_ascii=False),
                        payload.get("generated_at"),
                    ),
                )


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP22.25_strategy6_v3_quality_filter_candidates_{ts}.md"
    lines = [
        "# STEP22.25 Strategy6 V3 Quality Filter Candidate Report",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- mode: `{payload.get('mode')}`",
        f"- json: `{OUTPUT_JSON.relative_to(ROOT)}`",
        f"- sqlite: `{OUTPUT_DB.relative_to(ROOT)}`",
        "",
        "## Packages",
        "",
    ]
    for package in payload.get("packages") or []:
        lines.append(f"### `{package.get('parameter_set_id')}`")
        lines.append(f"- shadow_bad_count: `{package.get('shadow_bad_count')}`")
        lines.append(f"- watch_low_confidence_count: `{package.get('watch_low_confidence_count')}`")
        lines.append("")
        lines.append("| dimension | key | tier | samples | avg_R | win_rate | top_root_cause |")
        lines.append("| --- | --- | --- | ---: | ---: | ---: | --- |")
        for item in (package.get("candidates") or [])[:20]:
            lines.append(
                f"| `{item.get('dimension')}` | `{item.get('key')}` | `{item.get('tier')}` | "
                f"{item.get('sample_count')} | {item.get('avg_R')} | {item.get('win_rate')} | `{item.get('top_root_cause')}` |"
            )
        lines.append("")
    lines.extend(
        [
            "## Boundary",
            "",
            "- Candidates are shadow-only and are not written to live config.",
            "- Low-confidence candidates must not be promoted without larger focused matrix evidence.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=TQ_RESULT_PATH)
    parser.add_argument("--min-samples", type=int, default=30)
    parser.add_argument("--max-avg-r", type=float, default=-0.1)
    parser.add_argument("--max-win-rate", type=float, default=0.35)
    args = parser.parse_args()
    raw = _load_json(args.input)
    payload = build_candidates(raw, min_samples=args.min_samples, max_avg_r=args.max_avg_r, max_win_rate=args.max_win_rate)
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    write_sqlite(payload)
    report = write_report(payload)
    print(json.dumps({"json": str(OUTPUT_JSON), "sqlite": str(OUTPUT_DB), "report": str(report)}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
