from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from laoma_signal_engine.backtest.p21_trade_quality import materialize_payload, summary_payload


STEP21_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v2_focused_STEP21_51_result.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v2_tq_STEP19_29_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_step21_result(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"missing STEP21.51 result: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _top_parameter_sets(result: dict[str, Any], top_n: int) -> list[str]:
    out: list[str] = []
    for item in result.get("leaderboard") or []:
        param = item.get("parameter_set_id")
        if param and param not in out:
            out.append(str(param))
        if len(out) >= top_n:
            break
    best = ((result.get("best") or {}).get("parameter_set_id"))
    if best and str(best) not in out:
        out.insert(0, str(best))
    return out[:top_n]


def _items(summary: dict[str, Any], key: str) -> list[dict[str, Any]]:
    block = ((summary.get("summary") or {}).get(key) or {})
    return list(block.get("items") or [])


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP19.29_strategy6_v2_tq_root_cause_{ts}.md"
    lines = [
        "# STEP19.29 Strategy6 V2 Trade Quality Root Cause Review",
        "",
        f"- generated_at: `{_now()}`",
        f"- experiment_id: `{payload.get('experiment_id')}`",
        f"- selected_parameter_sets: `{len(payload.get('packages') or [])}`",
        f"- result_json: `{RESULT_PATH.relative_to(ROOT)}`",
        "",
    ]
    for package in payload.get("packages") or []:
        summary = package.get("summary") or {}
        stats = ((summary.get("summary") or {}).get("performance_stats") or {})
        materialized = package.get("materialize") or {}
        lines.extend(
            [
                f"## `{package.get('parameter_set_id')}`",
                "",
                f"- package_key: `{package.get('package_key')}`",
                f"- materialized_count: `{materialized.get('materialized_count')}` / selected `{materialized.get('selected_order_count')}`",
                f"- total samples: `{summary.get('total')}`",
                f"- win_rate: `{stats.get('win_rate')}`",
                f"- profit_factor: `{stats.get('profit_factor')}`",
                f"- expectancy_R: `{stats.get('expectancy_R')}`",
                f"- avg_win_R: `{stats.get('avg_win_R')}`",
                f"- avg_loss_R: `{stats.get('avg_loss_R')}`",
                "",
                "### Root Cause",
                "",
                "| root_cause | count | loss | ratio | avg_R | avg_MFE_R | avg_MAE_R |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for item in _items(summary, "root_cause_attribution")[:10]:
            lines.append(
                f"| `{item.get('key')}` | {item.get('count')} | {item.get('loss_count')} | "
                f"{item.get('ratio')} | {item.get('avg_R')} | {item.get('avg_MFE_R')} | {item.get('avg_MAE_R')} |"
            )
        lines.extend(["", "### Dimension Samples", ""])
        dimension = ((summary.get("summary") or {}).get("dimension_attribution") or {})
        for dim_name, dim_block in (dimension.items() if isinstance(dimension, dict) else []):
            items = (dim_block or {}).get("items") if isinstance(dim_block, dict) else None
            if not items:
                continue
            lines.append(f"- {dim_name}: " + ", ".join(f"`{i.get('key')}` count={i.get('count')} avg_R={i.get('avg_R')}" for i in items[:5]))
        lines.append("")
    lines.extend(
        [
            "## Boundary",
            "",
            "- This task only materialized bounded backtest samples into the Backtest Trade Quality tables.",
            "- No live config, paper order, or runtime strategy behavior was changed.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", type=Path, default=STEP21_RESULT_PATH)
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--limit", type=int, default=1500)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    step21 = _load_step21_result(args.result)
    experiment_id = str(step21.get("experiment_id") or "")
    if not experiment_id:
        raise SystemExit("STEP21.51 result has no experiment_id")
    params = _top_parameter_sets(step21, args.top_n)
    packages: list[dict[str, Any]] = []
    for param in params:
        mat = materialize_payload(
            ROOT,
            experiment_id=experiment_id,
            strategy_line="strategy6",
            parameter_set_id=param,
            top_n=1,
            limit=args.limit,
            dry_run=False,
            force=args.force,
        )
        package_keys = mat.get("package_keys") or []
        pkg_key = package_keys[0] if package_keys else None
        summary = summary_payload(
            ROOT,
            experiment_id=experiment_id,
            strategy_line="strategy6",
            parameter_set_id=param,
            package_key=pkg_key,
            limit=args.limit,
        )
        packages.append(
            {
                "parameter_set_id": param,
                "package_key": pkg_key,
                "materialize": mat,
                "summary": summary,
            }
        )
    payload = {
        "schema_version": "step19.29-strategy6-v2-tq-review-v1",
        "experiment_id": experiment_id,
        "source_result": str(args.result),
        "packages": packages,
        "generated_at": _now(),
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report_path = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report_path)}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
