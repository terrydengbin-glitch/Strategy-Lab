from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.api.app import app
from laoma_signal_engine.backtest.p21_v2 import SCHEMA_VERSION, config_matrix_contract_payload, kline_cache_status_payload


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _run(cmd: list[str], cwd: Path | None = None) -> dict[str, object]:
    if sys.platform.startswith("win") and cmd and cmd[0] == "npm":
        cmd = ["npm.cmd", *cmd[1:]]
    proc = subprocess.run(cmd, cwd=str(cwd or PROJECT_ROOT), text=True, capture_output=True, encoding="utf-8")
    return {
        "cmd": " ".join(cmd),
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-2000:],
        "stderr_tail": proc.stderr[-2000:],
    }


def main() -> int:
    stamp = _now_stamp()
    test_result = _run([sys.executable, "-m", "pytest", "laoma_signal_engine/tests/test_backtest_p21_v2.py", "-q"])
    build_result = _run(["npm", "run", "build"], PROJECT_ROOT / "web")
    client = TestClient(app)
    endpoints = {}
    for url in [
        "/api/backtest/p21/v2/kline-cache/status?days=1&max_symbols=1",
        "/api/backtest/p21/v2/matrix/contracts?strategy_line=without_micro&max_sets=2",
        "/api/backtest/p21/v2/matrix/experiments?limit=1",
        "/api/backtest/p21/v2/matrix/leaderboard?limit=1",
    ]:
        response = client.get(url)
        endpoints[url] = {"status_code": response.status_code, "ok": response.json().get("ok")}

    kline_status = kline_cache_status_payload(PROJECT_ROOT, days=30, max_symbols=5)
    matrix_contract = config_matrix_contract_payload(PROJECT_ROOT, strategy_line="all", max_sets=20)
    passed = (
        test_result["returncode"] == 0
        and build_result["returncode"] == 0
        and all(item["status_code"] == 200 and item["ok"] for item in endpoints.values())
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "step": "STEP7.94",
        "status": "PASS" if passed else "FAIL",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "test_result": test_result,
        "build_result": build_result,
        "api_smoke": endpoints,
        "kline_status_sample": kline_status,
        "matrix_contract_sample": {
            "parameter_set_count": matrix_contract.get("parameter_set_count"),
            "target_strategy_lines": matrix_contract.get("target_strategy_lines"),
        },
        "scope_note": "This audit validates the V2 contract, API, Vue build, fixture E2E engine, and SQLite schema. Full all-universe 30d Binance download is available through the new downloader endpoint and should be run as a long network job under rate-limit controls.",
    }
    report_dir = PROJECT_ROOT / "docs" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / f"STEP7.94_30d_config_matrix_backtest_e2e_audit_{stamp}.json"
    md_path = report_dir / f"STEP7.94_30d_config_matrix_backtest_e2e_audit_{stamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md = [
        "# STEP7.94 30d Config Matrix Backtest E2E Audit",
        "",
        f"- Status: {'PASS' if passed else 'FAIL'}",
        f"- Schema: {SCHEMA_VERSION}",
        f"- Unit test: returncode {test_result['returncode']}",
        f"- Vue build: returncode {build_result['returncode']}",
        f"- API smoke: {sum(1 for item in endpoints.values() if item['ok'])}/{len(endpoints)} passed",
        f"- Kline sample ready: {kline_status.get('ready_count')}/{kline_status.get('count')}",
        f"- Matrix contract sample sets: {matrix_contract.get('parameter_set_count')}",
        "",
        "## Boundary",
        "",
        "- V2 uses 1m Kline cache and config-writable parameter sets.",
        "- V1 trade-quality diagnostic backtest remains legacy and is not used as config optimization evidence.",
        "- No runtime strategy config, paper ledger, Feishu, snapshot, or micro daemon state is modified.",
        "- Full all-universe 30d Binance download is intentionally not forced by this smoke audit.",
        "",
        "## Evidence",
        "",
        f"- JSON: `{json_path.as_posix()}`",
    ]
    md_path.write_text("\n".join(md) + "\n", encoding="utf-8")
    print(md_path)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
