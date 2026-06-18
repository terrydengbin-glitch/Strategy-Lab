from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from step29_4_paper_trade_snapshot_materializer import ROOT, SIDECAR_DB, materialize


SUMMARY_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_5_paper_equivalent_backtest_snapshot_alignment_summary.json"
REPORT_PATH = ROOT / "docs" / "reports" / "STEP29.5_paper_equivalent_backtest_snapshot_alignment_20260617.md"

DEFAULT_SOURCES = [
    ("step29_5_s5_gate", ROOT / "DATA" / "backtest" / "paper_equivalent" / "step7_146_gate_on_strategy5" / "paper_equivalent.db", 5),
    ("step29_5_s6_gate", ROOT / "DATA" / "backtest" / "paper_equivalent" / "step7_146_gate_on_strategy6" / "paper_equivalent.db", 5),
    ("step29_5_s4_smoke", ROOT / "DATA" / "backtest" / "paper_equivalent" / "step7_150_strategy4_20260617T063457Z" / "paper_equivalent.db", 1),
    ("step29_5_s1_smoke", ROOT / "DATA" / "backtest" / "paper_equivalent" / "step7_150_without_micro_20260617T063457Z" / "paper_equivalent.db", 1),
]


def sidecar_counts(run_ids: list[str]) -> dict[str, Any]:
    con = sqlite3.connect(SIDECAR_DB)
    try:
        result: dict[str, Any] = {}
        for run_id in run_ids:
            result[run_id] = {
                "events": con.execute(
                    "SELECT COUNT(*) FROM trade_snapshot_events WHERE sample_id LIKE ?",
                    (f"{run_id}:%",),
                ).fetchone()[0],
                "samples": con.execute(
                    "SELECT COUNT(*) FROM trade_training_samples WHERE sample_id LIKE ?",
                    (f"{run_id}:%",),
                ).fetchone()[0],
            }
        return result
    finally:
        con.close()


def write_report(summary: dict[str, Any]) -> None:
    lines = [
        "# STEP29.5 Paper-Equivalent Backtest Snapshot Writer Alignment",
        "",
        "> 状态：DONE",
        "> 日期：2026-06-17",
        f"> Summary：`{SUMMARY_PATH.relative_to(ROOT).as_posix()}`",
        f"> Sidecar DB：`{SIDECAR_DB.relative_to(ROOT).as_posix()}`",
        "",
        "## 结论",
        "",
        "Paper-equivalent backtest 已按 STEP29.2 同一 sidecar schema 输出 snapshot events 和 training samples。`source_mode` 标记为 `paper_equivalent_backtest`，不与真实 paper 混淆。",
        "",
        "## 输出",
        "",
    ]
    for item in summary["runs"]:
        lines.extend(
            [
                f"- `{item['run_id']}`：source `{item['source_db']}`，samples {item['samples_written']}，events {item['events_written']}，TQ joined {item['trade_quality_joined']}",
            ]
        )
    lines.extend(
        [
            "",
            "## 可比字段",
            "",
            "- `order_id`, `strategy_line`, `symbol`, `side`",
            "- `entry_time_ms`, `exit_time_ms`",
            "- `fill_price`, `fee_usdt`, `slippage_bps`, `fill_delay_sec`, `fill_model`",
            "- `exit_price`, `exit_reason`, `gross_pnl_usdt`, `net_pnl_usdt`",
            "- `gate_decision`, `gate_rule_json`, `gate_features_json`, `config_hash`, `gate_hash`",
            "",
            "## 当前不可比/缺口",
            "",
            "- Market features 仍是 `needs_reconstruction`，等待 29.3 policy 接入真实 K 线重建。",
            "- Trade Quality rows 未在本轮 wrapper 强制 join；缺失保留在 sidecar data_quality。",
            "- 真 paper latency 与 replay/backtest latency 语义不同，必须在后续 equivalence diff 中显式标注。",
            "",
            "## 边界",
            "",
            "- 只读读取 paper-equivalent DB。",
            "- 只写 `DATA/research/trade_snapshots/trade_snapshots.db`。",
            "- 不恢复 legacy direct-fill backtest 为 promotion evidence。",
        ]
    )
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    runs: list[dict[str, Any]] = []
    for run_id, source, limit in DEFAULT_SOURCES:
        summary = materialize(
            source,
            run_id=run_id,
            limit=limit,
            source_mode="paper_equivalent_backtest",
            write_summary=False,
        )
        runs.append(summary)
    result = {
        "step": "STEP29.5",
        "status": "done",
        "sidecar_db": SIDECAR_DB.relative_to(ROOT).as_posix(),
        "runs": runs,
        "sidecar_counts": sidecar_counts([item["run_id"] for item in runs]),
        "schema_version": "step29_trade_snapshot_v1",
        "source_mode": "paper_equivalent_backtest",
        "boundary": {
            "source_access": "read_only",
            "source_write_back": False,
            "sidecar_only": True,
            "legacy_direct_fill_promotion_evidence": False,
        },
    }
    SUMMARY_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    write_report(result)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
