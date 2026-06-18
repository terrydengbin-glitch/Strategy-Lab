from __future__ import annotations

import json
from pathlib import Path

from laoma_signal_engine.cli import _build_parser, main
from laoma_signal_engine.runtime_health import pid_running


def test_step11_cli_parser_has_strategy_pipeline_and_micro_daemon() -> None:
    parser = _build_parser()

    got = parser.parse_args(
        [
            "run-strategy-pipeline",
            "--line",
            "all",
            "--mode",
            "interval",
            "--interval-sec",
            "300",
            "--max-cycles",
            "1",
        ],
    )
    assert got.command == "run-strategy-pipeline"
    assert got.line == "all"
    assert got.mode == "interval"
    assert got.interval_sec == 300

    got = parser.parse_args(["micro-daemon", "status", "--stdout-json"])
    assert got.command == "micro-daemon"
    assert got.action == "status"
    assert got.stdout_json is True


def test_micro_daemon_status_cli_reads_yaml_paths(tmp_path: Path, capsys) -> None:
    code = main(["micro-daemon", "status", "--project-root", str(tmp_path), "--stdout-json"])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source"] == "micro_daemon_cli"
    assert payload["root"] == str(tmp_path)
    assert payload["pid_running"] is False
    assert payload["features_path"] == str(tmp_path / "DATA/micro/latest_micro_features.json")


def test_step16_11_shared_pid_probe_rejects_missing_pid() -> None:
    assert pid_running(999999) is False
