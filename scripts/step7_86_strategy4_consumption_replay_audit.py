from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
S4_DB = ROOT / "DATA" / "strategy4" / "strategy4_observe.db"
PAPER_DB = ROOT / "DATA" / "paper" / "paper_trading.db"
LATEST_S4 = ROOT / "DATA" / "decisions" / "latest_trade_plan_strategy4.json"
REPORT_MD = ROOT / "docs" / "reports" / f"STEP7.86_strategy4_live_executable_consumption_replay_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.md"
REPORT_JSON = REPORT_MD.with_suffix(".json")


def _rows(db: Path, query: str, params: tuple = ()) -> list[dict]:
    if not db.is_file():
        return []
    with sqlite3.connect(db) as con:
        con.row_factory = sqlite3.Row
        return [dict(r) for r in con.execute(query, params).fetchall()]


def _read_json(path: Path) -> dict:
    try:
        got = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return got if isinstance(got, dict) else {}


def main() -> int:
    executable_attempts = _rows(
        S4_DB,
        "select attempt_id, symbol, run_id, cycle_id, attempted_at, status, decision, action, entry_mode, executable, reason_codes_json, lineage_json "
        "from strategy4_attempts where executable=1 order by attempted_at desc",
    )
    paper_orders = _rows(
        PAPER_DB,
        "select id, symbol, strategy_line, source_run_id, source_cycle_id, created_at from paper_orders where strategy_line='strategy4' order by created_at desc",
    )
    paper_skips = _rows(
        PAPER_DB,
        "select id, symbol, strategy_line, source_run_id, source_cycle_id, skip_reason, created_at, source_json from paper_skip_ledger "
        "where strategy_line='strategy4' order by created_at desc",
    )
    paper_intents = _rows(
        PAPER_DB,
        "select intent_id, symbol, strategy_line, source_run_id, source_cycle_id, created_at from paper_intent_inbox where strategy_line='strategy4' order by created_at desc",
    )

    skip_reasons = Counter(str(row.get("skip_reason") or "unknown") for row in paper_skips)
    lineage_present = 0
    lineage_missing = 0
    for row in paper_skips:
        try:
            src = json.loads(row.get("source_json") or "{}")
        except (TypeError, ValueError):
            src = {}
        if isinstance(src, dict) and (src.get("strategy4_lineage") or (src.get("guards") or {}).get("strategy4_lineage")):
            lineage_present += 1
        else:
            lineage_missing += 1

    latest = _read_json(LATEST_S4)
    payload = {
        "schema_version": "7.86",
        "source": "strategy4_live_executable_consumption_replay",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "strategy4_executable_attempts": len(executable_attempts),
        "paper_orders": len(paper_orders),
        "paper_intents": len(paper_intents),
        "paper_skips": len(paper_skips),
        "paper_skip_reasons": dict(skip_reasons),
        "paper_skip_lineage_present": lineage_present,
        "paper_skip_lineage_missing": lineage_missing,
        "latest_strategy4": {
            "generated_at": latest.get("generated_at"),
            "run_id": latest.get("run_id"),
            "cycle_id": latest.get("cycle_id"),
            "count": latest.get("count"),
            "executable_count": latest.get("executable_count"),
            "strategy4_due_count": (latest.get("input_refs") or {}).get("strategy4_due_count"),
        },
        "sample_executable_attempts": executable_attempts[:10],
        "sample_strategy4_skips": [
            {k: v for k, v in row.items() if k != "source_json"} for row in paper_skips[:10]
        ],
    }
    verdict = "PASS_CONSUMPTION_BOUNDARY_VISIBLE" if paper_orders or paper_skips else "BLOCKED_NO_PAPER_CONSUMPTION_EVIDENCE"
    if executable_attempts and not paper_orders and paper_skips:
        verdict = "PASS_WITH_SKIP_ONLY_CONSUMPTION"
    payload["verdict"] = verdict
    REPORT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# STEP7.86 Strategy4 Live Executable Consumption Replay",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- verdict: `{verdict}`",
        f"- executable attempts all-time: `{len(executable_attempts)}`",
        f"- paper orders: `{len(paper_orders)}`",
        f"- paper intents: `{len(paper_intents)}`",
        f"- paper skips: `{len(paper_skips)}`",
        "",
        "## Paper Skip Reasons",
        "",
    ]
    for reason, count in skip_reasons.most_common():
        lines.append(f"- `{reason}`: {count}")
    lines += [
        "",
        "## Latest Strategy4 Sidecar",
        "",
        f"- generated_at: `{payload['latest_strategy4']['generated_at']}`",
        f"- count / executable: `{payload['latest_strategy4']['count']} / {payload['latest_strategy4']['executable_count']}`",
        f"- due_count: `{payload['latest_strategy4']['strategy4_due_count']}`",
        "",
        "## Interpretation",
        "",
        "- Strategy4 executable attempts exist historically.",
        "- Current paper ledger has Strategy4 skip evidence but no Strategy4 order evidence.",
        "- This validates that the downstream boundary is observable, but current live order creation was not proven in this sample.",
        "- Follow-up should focus on skip reasons and producing a fresh executable after pool retention repair.",
    ]
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(str(REPORT_MD))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
