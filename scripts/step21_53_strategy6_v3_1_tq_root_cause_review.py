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


STEP21_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_focused_STEP21_53_result.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_1_tq_STEP21_53_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_result(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"missing STEP21.53 result: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _top_parameter_sets(result: dict[str, Any], top_n: int) -> list[str]:
    out: list[str] = []
    best = ((result.get("best") or {}).get("parameter_set_id"))
    if best:
        out.append(str(best))
    for item in result.get("leaderboard") or []:
        param = item.get("parameter_set_id")
        metrics = item.get("metrics") or {}
        if not param:
            continue
        if int(float(metrics.get("trade_count") or 0)) <= 0:
            continue
        if str(param) not in out:
            out.append(str(param))
        if len(out) >= top_n:
            break
    return out[:top_n]


def _items(summary: dict[str, Any], key: str) -> list[dict[str, Any]]:
    block = ((summary.get("summary") or {}).get(key) or {})
    return list(block.get("items") or [])


def _first(item: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return default


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP21.53_strategy6_v3_1_tq_root_cause_{ts}.md"
    lines = [
        "# STEP21.53 Strategy6 V3.1 Trade Quality Root Cause Review",
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
        for item in _items(summary, "root_cause_attribution")[:12]:
            lines.append(
                f"| `{_first(item, 'root_cause', 'key', 'label', 'name')}` | "
                f"{_first(item, 'count', 'sample_count', 'trade_count')} | {item.get('loss_count')} | "
                f"{item.get('ratio')} | {_first(item, 'avg_R', 'avg_net_R')} | {item.get('avg_MFE_R')} | {item.get('avg_MAE_R')} |"
            )
        lines.extend(["", "### Dimensions", ""])
        for block_name in ("entry_quality_attribution", "entry_context_v3_attribution", "dimension_attribution"):
            items = _items(summary, block_name)
            if items:
                lines.append(
                    f"- {block_name}: "
                    + ", ".join(
                        f"`{_first(i, 'key', 'entry_quality_label', 'entry_context_v3_label', 'label', 'name')}` "
                        f"count={_first(i, 'count', 'sample_count', 'trade_count')} "
                        f"avg_R={_first(i, 'avg_R', 'avg_net_R')}"
                        for i in items[:6]
                    )
                )
        lines.append("")
    lines.extend(
        [
            "## Boundary",
            "",
            "- This review only materializes bounded Strategy6 V3.1 backtest samples into Backtest Trade Quality tables.",
            "- It does not write live config, paper orders, Feishu payloads, or runtime strategy outputs.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", type=Path, default=STEP21_RESULT_PATH)
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    step21 = _load_result(args.result)
    experiment_id = str(step21.get("experiment_id") or "")
    if not experiment_id:
        raise SystemExit("STEP21.53 result has no experiment_id")
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
        packages.append({"parameter_set_id": param, "package_key": pkg_key, "materialize": mat, "summary": summary})
    payload = {
        "schema_version": "step21.53-strategy6-v3-1-tq-review-v1",
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
