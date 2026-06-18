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


STEP21_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_3_walk_forward_STEP21_55_result.json"
RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v3_3_tq_STEP19_33_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"missing result: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _selected_parameter_sets(result: dict[str, Any], top_n: int) -> list[str]:
    out: list[str] = []
    selected = (((result.get("walk_forward") or {}).get("selected") or {}).get("parameter_set_id"))
    if selected:
        out.append(str(selected))
    for item in ((result.get("walk_forward") or {}).get("leaderboard") or []):
        param = item.get("parameter_set_id")
        if param and str(param) not in out:
            out.append(str(param))
        if len(out) >= top_n:
            break
    return out[:top_n]


def _items(summary: dict[str, Any], key: str) -> list[dict[str, Any]]:
    return list((((summary.get("summary") or {}).get(key) or {}).get("items") or []))


def _first(item: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return default


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP19.33_strategy6_v3_3_tq_generalization_{ts}.md"
    lines = [
        "# STEP19.33 Strategy6 V3.3 Trade Quality Generalization Review",
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
        wf = package.get("walk_forward") or {}
        lines.extend(
            [
                f"## `{package.get('parameter_set_id')}`",
                "",
                f"- package_key: `{package.get('package_key')}`",
                f"- validation_pf: `{((wf.get('validation') or {}).get('profit_factor'))}`",
                f"- test_pf: `{((wf.get('test') or {}).get('profit_factor'))}`",
                f"- tq_profit_factor: `{stats.get('profit_factor')}`",
                f"- tq_expectancy_R: `{stats.get('expectancy_R')}`",
                f"- tq_avg_loss_R: `{stats.get('avg_loss_R')}`",
                "",
                "| root_cause | count | loss | ratio | avg_R | avg_MFE_R | avg_MAE_R |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for item in _items(summary, "root_cause_attribution")[:10]:
            lines.append(
                f"| `{_first(item, 'root_cause', 'key', 'label', 'name')}` | "
                f"{_first(item, 'count', 'sample_count', 'trade_count')} | {item.get('loss_count')} | "
                f"{item.get('ratio')} | {_first(item, 'avg_R', 'avg_net_R')} | {item.get('avg_MFE_R')} | {item.get('avg_MAE_R')} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Boundary",
            "",
            "- Bounded Backtest Trade Quality materialization only.",
            "- No runtime strategy, paper, Feishu, or config mutation.",
            "- Test split remains audit evidence only; it is not used to choose candidates.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", type=Path, default=STEP21_RESULT_PATH)
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--limit", type=int, default=3000)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    step21 = _load(args.result)
    experiment_id = str(step21.get("experiment_id") or "")
    if not experiment_id:
        raise SystemExit("STEP21.55 result has no experiment_id")
    wf_by_param = {item.get("parameter_set_id"): item for item in ((step21.get("walk_forward") or {}).get("leaderboard") or [])}
    packages: list[dict[str, Any]] = []
    for param in _selected_parameter_sets(step21, args.top_n):
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
        pkg_key = (mat.get("package_keys") or [None])[0]
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
                "walk_forward": wf_by_param.get(param, {}),
            }
        )
    payload = {
        "schema_version": "step19.33-strategy6-v3-3-tq-generalization-v1",
        "experiment_id": experiment_id,
        "source_result": str(args.result),
        "packages": packages,
        "generated_at": _now(),
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report = write_report(payload)
    print(json.dumps({"result_path": str(RESULT_PATH), "report_path": str(report)}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
