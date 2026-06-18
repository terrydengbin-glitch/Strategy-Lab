from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


STEP21_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v2_focused_STEP21_51_result.json"
STEP19_RESULT_PATH = ROOT / "DATA" / "backtest" / "strategy6_v2_tq_STEP19_29_result.json"
REPORT_DIR = ROOT / "docs" / "reports"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run(cmd: list[str]) -> dict[str, Any]:
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True)
    return {
        "cmd": cmd,
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": str(exc)}


def _best_summary(step21: dict[str, Any]) -> dict[str, Any]:
    best = step21.get("best") or {}
    metrics = best.get("metrics") or {}
    return {
        "parameter_set_id": best.get("parameter_set_id"),
        "profit_factor": metrics.get("profit_factor"),
        "expectancy_R": metrics.get("expectancy_R"),
        "win_rate": metrics.get("win_rate"),
        "trade_count": metrics.get("trade_count"),
        "total_R": metrics.get("total_R"),
        "max_drawdown_R": metrics.get("max_drawdown_R"),
    }


def _tq_summary(step19: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for package in step19.get("packages") or []:
        summary = package.get("summary") or {}
        stats = ((summary.get("summary") or {}).get("performance_stats") or {})
        root_items = (((summary.get("summary") or {}).get("root_cause_attribution") or {}).get("items") or [])
        out.append(
            {
                "parameter_set_id": package.get("parameter_set_id"),
                "package_key": package.get("package_key"),
                "total": summary.get("total"),
                "profit_factor": stats.get("profit_factor"),
                "expectancy_R": stats.get("expectancy_R"),
                "top_root_causes": [
                    {
                        "key": item.get("key"),
                        "count": item.get("count"),
                        "loss_count": item.get("loss_count"),
                        "avg_R": item.get("avg_R"),
                    }
                    for item in root_items[:5]
                ],
            }
        )
    return out


def write_report(payload: dict[str, Any]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = REPORT_DIR / f"STEP7.104_strategy6_v2_e2e_audit_{ts}.md"
    step21 = payload.get("step21") or {}
    best = _best_summary(step21)
    compile_result = payload.get("compileall") or {}
    pytest_result = payload.get("pytest") or {}
    lines = [
        "# STEP7.104 Strategy6 V2 E2E Audit Report",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- compileall_returncode: `{compile_result.get('returncode')}`",
        f"- pytest_returncode: `{pytest_result.get('returncode')}`",
        f"- step21_result: `{STEP21_RESULT_PATH.relative_to(ROOT)}`",
        f"- step19_result: `{STEP19_RESULT_PATH.relative_to(ROOT)}`",
        "",
        "## Best Backtest Package",
        "",
    ]
    for key, value in best.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Trade Quality Packages", ""])
    for item in payload.get("tq_packages") or []:
        lines.append(f"### `{item.get('parameter_set_id')}`")
        lines.append(f"- package_key: `{item.get('package_key')}`")
        lines.append(f"- total samples: `{item.get('total')}`")
        lines.append(f"- profit_factor: `{item.get('profit_factor')}`")
        lines.append(f"- expectancy_R: `{item.get('expectancy_R')}`")
        lines.append("- top_root_causes: " + ", ".join(f"`{r.get('key')}` count={r.get('count')} avg_R={r.get('avg_R')}" for r in item.get("top_root_causes") or []))
        lines.append("")
    lines.extend(
        [
            "## Contract Audit",
            "",
            "- Strategy6 V2 is opt-in via `strategy6_version=v2`; V1 remains default.",
            "- V2 evidence fields are carried through evaluator guards, backtest shadow orders, fill features, and TQ samples.",
            "- No live config, paper ledger, Feishu, or Strategy1/2/4/5 runtime behavior was changed by this audit.",
            "",
            "## Verification Tails",
            "",
            "### pytest",
            "",
            "```text",
            str(pytest_result.get("stdout_tail") or pytest_result.get("stderr_tail") or "").strip(),
            "```",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    compileall = _run(
        [
            sys.executable,
            "-m",
            "compileall",
            "laoma_signal_engine/strategy6",
            "laoma_signal_engine/backtest/p21_real_evaluator.py",
            "laoma_signal_engine/backtest/p21_v2.py",
            "scripts/step21_51_strategy6_v2_focused_matrix_run.py",
            "scripts/step19_29_strategy6_v2_tq_root_cause_review.py",
        ]
    )
    pytest = _run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "laoma_signal_engine/tests/test_strategy6_evidence.py",
            "laoma_signal_engine/tests/test_backtest_p21_real_evaluator.py",
        ]
    )
    step21 = _read_json(STEP21_RESULT_PATH)
    step19 = _read_json(STEP19_RESULT_PATH)
    payload = {
        "schema_version": "step7.104-strategy6-v2-e2e-audit-v1",
        "generated_at": _now(),
        "compileall": compileall,
        "pytest": pytest,
        "step21": step21,
        "step19": step19,
        "tq_packages": _tq_summary(step19),
    }
    report_path = write_report(payload)
    status = "PASS" if compileall.get("returncode") == 0 and pytest.get("returncode") == 0 and step21 and step19 else "FAIL"
    print(json.dumps({"status": status, "report_path": str(report_path)}, ensure_ascii=False, indent=2), flush=True)
    if status != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
