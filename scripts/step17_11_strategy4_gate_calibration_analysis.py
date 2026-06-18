from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "DATA" / "strategy4" / "strategy4_observe.db"
REPORT_MD = ROOT / "docs" / "reports" / f"STEP17.11_strategy4_gate_calibration_analysis_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.md"
REPORT_JSON = REPORT_MD.with_suffix(".json")


GATE_GROUPS = {
    "rr_market_room": ("rr", "room", "target_space", "effective_rr", "range_room"),
    "entry_wait_price": ("better_entry", "WAIT_REBOUND", "WAIT_PULLBACK", "bad_price", "pullback", "rebound"),
    "liquidity_depth_slippage": ("liquidity", "depth", "slippage", "spread"),
    "refresh_freshness": ("refresh", "stale"),
    "hard_direction_score": ("score_too_low", "direction_invalid", "NO_TRADE"),
}


def _load_reasons(raw: str | None) -> list[str]:
    try:
        got = json.loads(raw or "[]")
    except (TypeError, ValueError):
        return []
    return [str(x) for x in got if str(x)]


def _group_reason(reason: str) -> str:
    lower = reason.lower()
    for group, needles in GATE_GROUPS.items():
        if any(needle.lower() in lower for needle in needles):
            return group
    return "other"


def main() -> int:
    rows: list[dict] = []
    if DB.is_file():
        with sqlite3.connect(DB) as con:
            con.row_factory = sqlite3.Row
            rows = [dict(r) for r in con.execute("select * from strategy4_attempts").fetchall()]

    reason_counts: Counter[str] = Counter()
    group_counts: Counter[str] = Counter()
    empty_reason_attempts = 0
    executable_count = 0
    status_counts: Counter[str] = Counter()

    for row in rows:
        status_counts[str(row.get("status") or "unknown")] += 1
        executable_count += 1 if row.get("executable") else 0
        reasons = _load_reasons(row.get("reason_codes_json"))
        if not reasons:
            empty_reason_attempts += 1
        for reason in reasons:
            reason_counts[reason] += 1
            group_counts[_group_reason(reason)] += 1

    candidate_deltas = [
        {
            "tier": "safe_observation_only",
            "change": "Do not pass refresh/liquidity missing; only make them visible and retry later.",
            "target_groups": ["refresh_freshness", "liquidity_depth_slippage"],
            "risk": "Low. Improves diagnosis without releasing weak trades.",
        },
        {
            "tier": "relaxed_test",
            "change": "Strategy4-only fast-exit RR shadow: lower TP target / market room requirement while keeping SL unchanged.",
            "target_groups": ["rr_market_room", "entry_wait_price"],
            "risk": "Medium. May increase executable count but can admit lower-quality continuation.",
        },
        {
            "tier": "not_recommended",
            "change": "Blindly ignore liquidity/depth/slippage blockers.",
            "target_groups": ["liquidity_depth_slippage"],
            "risk": "High. Paper/live slippage can dominate fast-exit reward.",
        },
    ]

    payload = {
        "schema_version": "17.11",
        "source": "strategy4_gate_calibration_analysis",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "db_path": str(DB),
        "attempt_count": len(rows),
        "executable_count": executable_count,
        "status_counts": dict(status_counts),
        "empty_reason_attempts": empty_reason_attempts,
        "top_reasons": reason_counts.most_common(30),
        "gate_group_counts": dict(group_counts),
        "candidate_deltas": candidate_deltas,
        "config_mutated": False,
    }
    REPORT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# STEP17.11 Strategy4 Gate Calibration Analysis",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- verdict: `SHADOW_RECOMMENDATIONS_ONLY`",
        f"- attempts: `{len(rows)}`",
        f"- executable attempts: `{executable_count}`",
        f"- empty reason attempts: `{empty_reason_attempts}`",
        "",
        "## Gate Group Counts",
        "",
    ]
    for group, count in group_counts.most_common():
        lines.append(f"- `{group}`: {count}")
    lines += ["", "## Top Reasons", ""]
    for reason, count in reason_counts.most_common(20):
        lines.append(f"- `{reason}`: {count}")
    lines += ["", "## Recommendations", ""]
    for item in candidate_deltas:
        lines.append(f"- `{item['tier']}`: {item['change']} Risk: {item['risk']}")
    lines += [
        "",
        "## Boundary",
        "",
        "- No config or strategy logic was changed by this analysis.",
        "- Strategy4-only gate changes should be promoted only through a separate config task.",
    ]
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(str(REPORT_MD))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
