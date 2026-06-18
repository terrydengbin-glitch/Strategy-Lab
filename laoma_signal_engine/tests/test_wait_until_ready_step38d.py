"""STEP3.8D wait-until-ready: evaluate, runner, summary. docs/STEP3.8D_*.md."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from laoma_signal_engine.core.exit_codes import (
    EXIT_CONFIG,
    EXIT_INTERNAL,
    EXIT_SUCCESS,
    EXIT_WAIT_UNTIL_READY_TIMEOUT,
)
from laoma_signal_engine.micro.wait_until_ready.config import WaitUntilReadyConfig, recommended_target_stale_sec
from laoma_signal_engine.micro.wait_until_ready.evaluate import (
    micro_satisfies_current_run_wait,
    micro_satisfies_wait,
    normalize_symbol,
    scope_micro_to_expected_symbols,
)
from laoma_signal_engine.micro.wait_until_ready.runner import build_run_report_payload, run_wait_until_ready_orchestration
from laoma_signal_engine.micro.wait_until_ready.summary import build_not_ready_summary


def _base_micro(
    *,
    ready_count: int = 1,
    status: str = "ok",
    target_status: str = "fresh",
    ws_status: str = "connected",
    msg_age: float = 0.0,
    items: list[dict] | None = None,
    generated_at: str = "2999-01-01T00:00:00Z",
    target_generated_at: str = "2026-01-01T00:00:00Z",
) -> dict:
    if items is None:
        items = [
            {
                "symbol": "BTCUSDT",
                "tier": "tier1_warm_watch",
                "source_state": "watch_candidate",
                "micro_quality": {"ready": ready_count > 0, "reason_codes": [], "coverage": {}},
            }
        ]
    return {
        "schema_version": "1.6",
        "generated_at": generated_at,
        "status": status,
        "target_generated_at": target_generated_at,
        "target_age_sec": 10,
        "target_status": target_status,
        "symbol_count": len(items),
        "ready_count": ready_count,
        "not_ready_count": max(0, len(items) - ready_count),
        "ws_status": ws_status,
        "last_ws_message_age_sec": msg_age,
        "items": items,
    }


def test_normalize_symbol() -> None:
    assert normalize_symbol(" btcusdt ") == "BTCUSDT"


def test_recommended_target_stale_sec() -> None:
    assert recommended_target_stale_sec(max_wait_sec=900, buffer_sec=300) == 1200
    assert recommended_target_stale_sec(max_wait_sec=100, buffer_sec=300) == 420


def test_min_ready_count_pass() -> None:
    cfg = WaitUntilReadyConfig(mode="min_ready_count", min_ready_count=1)
    assert micro_satisfies_wait(_base_micro(ready_count=1), cfg) is True


def test_min_ready_count_fail() -> None:
    cfg = WaitUntilReadyConfig(mode="min_ready_count", min_ready_count=2)
    m = _base_micro(ready_count=1)
    m["items"][0]["micro_quality"]["ready"] = False
    m["ready_count"] = 0
    assert micro_satisfies_wait(m, cfg) is False


def test_min_full_ready_count_uses_full_quality_not_fast_ready() -> None:
    items = [
        {
            "symbol": "BTCUSDT",
            "source_state": "watch_candidate",
            "micro_quality": {"ready": True, "reason_codes": [], "coverage": {}},
            "micro_full_quality": {"ready": False, "reason_codes": ["warmup_not_met"], "coverage": {}},
        }
    ]
    m = _base_micro(ready_count=1, items=items)
    m["full_ready_count"] = 0
    cfg = WaitUntilReadyConfig(mode="min_full_ready_count", min_ready_count=1)

    assert micro_satisfies_wait(m, cfg) is False

    m["items"][0]["micro_full_quality"]["ready"] = True
    m["full_ready_count"] = 1
    assert micro_satisfies_wait(m, cfg) is True


def test_min_fast_ready_count_uses_fast_quality_not_generic_ready() -> None:
    items = [
        {
            "symbol": "BTCUSDT",
            "source_state": "watch_candidate",
            "micro_quality": {"ready": True, "reason_codes": [], "coverage": {}},
            "micro_fast_quality": {"ready": False, "reason_codes": ["fast_warmup_not_met"], "coverage": {}},
        }
    ]
    m = _base_micro(ready_count=1, items=items)
    m["fast_ready_count"] = 0
    cfg = WaitUntilReadyConfig(mode="min_fast_ready_count", min_ready_count=1)

    assert micro_satisfies_wait(m, cfg) is False

    m["items"][0]["micro_fast_quality"]["ready"] = True
    m["fast_ready_count"] = 1
    assert micro_satisfies_wait(m, cfg) is True


def test_target_set_scope_ignores_global_full_ready_from_other_symbols() -> None:
    items = [
        {
            "symbol": "AUSDT",
            "source_state": "watch_candidate",
            "micro_quality": {"ready": False, "reason_codes": ["warmup"], "coverage": {}},
            "micro_full_quality": {"ready": False, "reason_codes": ["warmup"], "coverage": {}},
        },
        {
            "symbol": "BUSDT",
            "source_state": "watch_candidate",
            "micro_quality": {"ready": False, "reason_codes": ["warmup"], "coverage": {}},
            "micro_full_quality": {"ready": False, "reason_codes": ["warmup"], "coverage": {}},
        },
        {
            "symbol": "OLDUSDT",
            "source_state": "watch_candidate",
            "micro_quality": {"ready": True, "reason_codes": [], "coverage": {}},
            "micro_full_quality": {"ready": True, "reason_codes": [], "coverage": {}},
        },
    ]
    m = _base_micro(ready_count=1, items=items, generated_at="2999-01-01T00:00:00Z")
    m["full_ready_count"] = 1
    scoped = scope_micro_to_expected_symbols(m, {"AUSDT", "BUSDT"}, target_set_id="ts1")

    assert scoped["ready_scope"] == "target_set"
    assert scoped["target_set_id"] == "ts1"
    assert scoped["symbol_count"] == 2
    assert scoped["global_symbol_count"] == 3
    assert scoped["full_ready_count"] == 0
    assert scoped["global_full_ready_count"] == 1

    cfg = WaitUntilReadyConfig(mode="min_full_ready_count", min_ready_count=1)
    assert (
        micro_satisfies_current_run_wait(
            m,
            cfg,
            started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            expected_target_generated_at="2026-01-01T00:00:00Z",
            expected_symbols={"AUSDT", "BUSDT"},
        )
        is False
    )


def test_min_ready_strong_pass() -> None:
    items = [
        {
            "symbol": "AUSDT",
            "source_state": "strong_candidate",
            "micro_quality": {"ready": True, "reason_codes": [], "coverage": {}},
        },
        {
            "symbol": "BUSDT",
            "source_state": "watch_candidate",
            "micro_quality": {"ready": False, "reason_codes": ["x"], "coverage": {}},
        },
    ]
    m = _base_micro(ready_count=1, items=items)
    cfg = WaitUntilReadyConfig(mode="min_ready_strong", min_ready_strong_count=1)
    assert micro_satisfies_wait(m, cfg) is True


def test_symbols_any_all() -> None:
    items = [
        {
            "symbol": "XUSDT",
            "source_state": "watch_candidate",
            "micro_quality": {"ready": False, "reason_codes": [], "coverage": {}},
        },
        {
            "symbol": "YUSDT",
            "source_state": "watch_candidate",
            "micro_quality": {"ready": True, "reason_codes": [], "coverage": {}},
        },
    ]
    m = _base_micro(ready_count=1, items=items)
    cfg_any = WaitUntilReadyConfig(mode="symbols", symbols=("XUSDT", "YUSDT"), require_symbols="any")
    assert micro_satisfies_wait(m, cfg_any) is True
    cfg_all = WaitUntilReadyConfig(mode="symbols", symbols=("XUSDT", "YUSDT"), require_symbols="all")
    assert micro_satisfies_wait(m, cfg_all) is False
    items2 = [
        dict(items[0], micro_quality={"ready": True, "reason_codes": [], "coverage": {}}),
        dict(items[1], micro_quality={"ready": True, "reason_codes": [], "coverage": {}}),
    ]
    m2 = _base_micro(ready_count=2, items=items2)
    assert micro_satisfies_wait(m2, cfg_all) is True


def test_target_not_fresh_blocks() -> None:
    cfg = WaitUntilReadyConfig(require_target_fresh=True)
    m = _base_micro(ready_count=1, target_status="stale")
    assert micro_satisfies_wait(m, cfg) is False


def test_stale_observing_can_satisfy_wait() -> None:
    cfg = WaitUntilReadyConfig(require_target_fresh=True)
    m = _base_micro(
        ready_count=1,
        status="observing_stale_targets",
        target_status="stale_observing",
    )
    assert micro_satisfies_wait(m, cfg) is True


def test_require_target_fresh_false_allows() -> None:
    cfg = WaitUntilReadyConfig(require_target_fresh=False)
    m = _base_micro(ready_count=1, target_status="stale")
    assert micro_satisfies_wait(m, cfg) is True


def test_ws_disconnected_blocks() -> None:
    cfg = WaitUntilReadyConfig(require_ws_connected=True)
    m = _base_micro(ready_count=1, ws_status="disconnected")
    assert micro_satisfies_wait(m, cfg) is False


def test_ws_message_age_null_blocks() -> None:
    m = _base_micro(ready_count=1)
    m["last_ws_message_age_sec"] = None
    cfg = WaitUntilReadyConfig(ws_message_max_age_sec=5)
    assert micro_satisfies_wait(m, cfg) is False


def test_strict_coverage_pass() -> None:
    items = [
        {
            "symbol": "ZUSDT",
            "source_state": "watch_candidate",
            "micro_quality": {
                "ready": True,
                "reason_codes": [],
                "coverage": {
                    "aggTrade": {"covered_seconds": 500, "stream_type": "aggTrade"},
                },
            },
        }
    ]
    m = _base_micro(ready_count=1, items=items)
    cfg = WaitUntilReadyConfig(
        mode="strict_coverage",
        strict_coverage_min_by_stream=(("aggTrade", 100),),
    )
    assert micro_satisfies_wait(m, cfg) is True


def test_strict_coverage_fail() -> None:
    items = [
        {
            "symbol": "ZUSDT",
            "source_state": "watch_candidate",
            "micro_quality": {
                "ready": True,
                "reason_codes": [],
                "coverage": {
                    "aggTrade": {"covered_seconds": 50, "stream_type": "aggTrade"},
                },
            },
        }
    ]
    m = _base_micro(ready_count=1, items=items)
    cfg = WaitUntilReadyConfig(
        mode="strict_coverage",
        strict_coverage_min_by_stream=(("aggTrade", 200),),
    )
    assert micro_satisfies_wait(m, cfg) is False


def test_build_not_ready_summary_shape() -> None:
    items = [
        {
            "symbol": "A",
            "tier": "t1",
            "source_state": "watch_candidate",
            "micro_quality": {
                "ready": False,
                "reason_codes": ["warmup_not_met"],
                "coverage": {"aggTrade": {"covered_seconds": 10}},
            },
        }
    ]
    s = build_not_ready_summary(items)
    assert "by_source_state" in s
    assert "by_tier" in s
    assert s["top_reason_codes"]


def test_runner_timeout(tmp_path: Path) -> None:
    pr = tmp_path
    latest = pr / "latest.json"
    hb = pr / "hb.json"
    tgt = pr / "targets.json"
    tgt.write_text('{"schema_version": "1.6", "generated_at": "2026-01-01T00:00:00Z"}', encoding="utf-8")
    cfg = WaitUntilReadyConfig(
        max_wait_sec=0.35,
        poll_interval_sec=0.05,
        max_consecutive_read_failures=50,
    )

    def _read(_p: Path) -> tuple[dict | None, str | None]:
        doc = _base_micro(ready_count=0)
        doc["items"][0]["micro_quality"]["ready"] = False
        doc["ready_count"] = 0
        return doc, None

    rc = run_wait_until_ready_orchestration(
        project_root=pr,
        cfg=cfg,
        latest_path=latest,
        heartbeat_path=hb,
        targets_path=tgt,
        transport="fake",
        start_subprocess=False,
        read_micro_json=_read,
        report_dir=pr / "reports",
    )
    assert rc == EXIT_WAIT_UNTIL_READY_TIMEOUT
    reps = list((pr / "reports").glob("micro_until_ready_*.json"))
    assert len(reps) == 1
    data = json.loads(reps[0].read_text(encoding="utf-8"))
    assert data["status"] == "timeout"
    assert "not_ready_summary" in data


def test_runner_ready_met(tmp_path: Path) -> None:
    pr_tmp = tmp_path
    latest = pr_tmp / "latest.json"
    hb = pr_tmp / "hb.json"
    tgt = pr_tmp / "targets.json"
    tgt.write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": "2026-01-01T00:00:00Z",
                "tier1_warm_watch": [{"symbol": "BTCUSDT"}],
                "tier2_active_strong": [],
            },
        ),
        encoding="utf-8",
    )
    cfg = WaitUntilReadyConfig(
        max_wait_sec=5.0,
        poll_interval_sec=0.03,
        mode="min_ready_count",
        min_ready_count=1,
    )
    n = {"c": 0}

    def _read(_p: Path) -> tuple[dict | None, str | None]:
        n["c"] += 1
        if n["c"] < 2:
            m = _base_micro(ready_count=0)
            m["items"][0]["micro_quality"]["ready"] = False
            m["ready_count"] = 0
            return m, None
        return _base_micro(ready_count=1), None

    rc = run_wait_until_ready_orchestration(
        project_root=pr_tmp,
        cfg=cfg,
        latest_path=latest,
        heartbeat_path=hb,
        targets_path=tgt,
        transport="fake",
        start_subprocess=False,
        read_micro_json=_read,
        report_dir=pr_tmp / "r2",
    )
    assert rc == EXIT_SUCCESS
    reps = list((pr_tmp / "r2").glob("micro_until_ready_*.json"))
    assert len(reps) == 1
    body = json.loads(reps[0].read_text(encoding="utf-8"))
    assert body["status"] == "ready_met"
    assert body["current_run_freshness_ok"] is True


def test_step1047_wait_pass_evidence_records_quality_confirmed_consumable_symbols(tmp_path: Path) -> None:
    pr_tmp = tmp_path
    latest = pr_tmp / "latest.json"
    hb = pr_tmp / "hb.json"
    tgt = pr_tmp / "targets.json"
    evidence = pr_tmp / "evidence" / "latest_wait_pass_micro_fast.json"
    tgt.write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": "2026-01-01T00:00:00Z",
                "target_set_id": "target_evidence",
                "tier1_warm_watch": [{"symbol": "AAAUSDT"}, {"symbol": "BBBUSDT"}],
                "tier2_active_strong": [],
            },
        ),
        encoding="utf-8",
    )
    cfg = WaitUntilReadyConfig(
        max_wait_sec=5.0,
        poll_interval_sec=0.03,
        mode="min_fast_ready_count",
        min_ready_count=1,
    )

    def _read(_p: Path) -> tuple[dict | None, str | None]:
        return (
            _base_micro(
                ready_count=2,
                items=[
                    {
                        "symbol": "AAAUSDT",
                        "micro_quality": {"ready": True, "reason_codes": []},
                        "micro_fast_quality": {"ready": True, "reason_codes": []},
                        "micro_fast_signal": {
                            "micro_direction_confirmed": True,
                            "micro_exec_allowed": True,
                        },
                    },
                    {
                        "symbol": "BBBUSDT",
                        "micro_quality": {"ready": True, "reason_codes": []},
                        "micro_fast_quality": {"ready": True, "reason_codes": []},
                        "micro_fast_signal": {
                            "micro_direction_confirmed": False,
                            "micro_exec_allowed": False,
                        },
                    },
                ],
            )
            | {"fast_ready_count": 2},
            None,
        )

    rc = run_wait_until_ready_orchestration(
        project_root=pr_tmp,
        cfg=cfg,
        latest_path=latest,
        heartbeat_path=hb,
        targets_path=tgt,
        transport="fake",
        start_subprocess=False,
        read_micro_json=_read,
        report_dir=pr_tmp / "r_step1047",
        evidence_path=evidence,
        strategy_line="micro_fast",
        run_id="run1047",
        cycle_id="cycle1047",
    )

    assert rc == EXIT_SUCCESS
    body = json.loads(evidence.read_text(encoding="utf-8"))
    assert body["strategy_line"] == "micro_fast"
    assert body["quality_ready_count"] == 2
    assert body["confirmed_ready_count"] == 1
    assert body["consumable_ready_count"] == 1
    assert body["quality_ready_symbols"] == ["AAAUSDT", "BBBUSDT"]
    assert body["confirmed_symbols"] == ["AAAUSDT"]
    assert body["consumable_symbols"] == ["AAAUSDT"]


def test_runner_daemon_early_exit_subprocess(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Parent loop must not spin until max_wait_sec if the daemon child already died."""

    class FakeProc:
        pid = 4242

        def poll(self) -> int:
            return 7

        def terminate(self) -> None:
            return None

        def wait(self, timeout: float | None = None) -> None:
            return None

        def kill(self) -> None:
            return None

    def _fake_popen(*_a: object, **_kw: object) -> FakeProc:
        return FakeProc()

    monkeypatch.setattr(
        "laoma_signal_engine.micro.wait_until_ready.runner.subprocess.Popen",
        _fake_popen,
    )

    pr_tmp = tmp_path
    latest = pr_tmp / "latest.json"
    hb = pr_tmp / "hb.json"
    tgt = pr_tmp / "targets.json"
    tgt.write_text("{}", encoding="utf-8")
    cfg = WaitUntilReadyConfig(
        max_wait_sec=60.0,
        poll_interval_sec=0.02,
        max_consecutive_read_failures=50,
    )
    rc = run_wait_until_ready_orchestration(
        project_root=pr_tmp,
        cfg=cfg,
        latest_path=latest,
        heartbeat_path=hb,
        targets_path=tgt,
        transport="fake",
        start_subprocess=True,
        read_micro_json=lambda _p: (None, None),
        report_dir=pr_tmp / "r_daemon_die",
    )
    assert rc == EXIT_INTERNAL
    reps = list((pr_tmp / "r_daemon_die").glob("micro_until_ready_*.json"))
    assert len(reps) == 1
    body = json.loads(reps[0].read_text(encoding="utf-8"))
    assert body["status"] == "error"
    assert "daemon subprocess exited early" in (body.get("error_message") or "")
    assert body.get("daemon_pid") == 4242


def test_runner_ignores_old_micro_until_current_run_doc(tmp_path: Path) -> None:
    pr_tmp = tmp_path
    latest = pr_tmp / "latest.json"
    hb = pr_tmp / "hb.json"
    tgt = pr_tmp / "targets.json"
    tgt.write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": "2026-02-01T00:00:00Z",
                "tier1_warm_watch": [{"symbol": "BTCUSDT"}],
                "tier2_active_strong": [],
            },
        ),
        encoding="utf-8",
    )
    cfg = WaitUntilReadyConfig(
        max_wait_sec=5.0,
        poll_interval_sec=0.03,
        mode="min_ready_count",
        min_ready_count=1,
    )
    n = {"c": 0}

    def _read(_p: Path) -> tuple[dict | None, str | None]:
        n["c"] += 1
        if n["c"] < 2:
            return _base_micro(
                ready_count=1,
                generated_at="2026-01-01T00:00:00Z",
                target_generated_at="2026-01-01T00:00:00Z",
            ), None
        return _base_micro(
            ready_count=1,
            generated_at="2999-01-01T00:00:00Z",
            target_generated_at="2026-02-01T00:00:00Z",
        ), None

    rc = run_wait_until_ready_orchestration(
        project_root=pr_tmp,
        cfg=cfg,
        latest_path=latest,
        heartbeat_path=hb,
        targets_path=tgt,
        transport="fake",
        start_subprocess=False,
        read_micro_json=_read,
        report_dir=pr_tmp / "r_current",
    )
    assert rc == EXIT_SUCCESS
    assert n["c"] >= 2
    rep = next((pr_tmp / "r_current").glob("micro_until_ready_*.json"))
    body = json.loads(rep.read_text(encoding="utf-8"))
    assert body["status"] == "ready_met"
    assert body["expected_target_generated_at"] == "2026-02-01T00:00:00Z"
    assert body["current_run_freshness_ok"] is True


def test_runner_timeout_reports_current_run_skip_reason(tmp_path: Path) -> None:
    pr_tmp = tmp_path
    latest = pr_tmp / "latest.json"
    hb = pr_tmp / "hb.json"
    tgt = pr_tmp / "targets.json"
    tgt.write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": "2026-02-01T00:00:00Z",
                "tier1_warm_watch": [{"symbol": "BTCUSDT"}],
                "tier2_active_strong": [],
            },
        ),
        encoding="utf-8",
    )
    cfg = WaitUntilReadyConfig(
        max_wait_sec=0.25,
        poll_interval_sec=0.05,
        max_consecutive_read_failures=50,
    )

    def _read(_p: Path) -> tuple[dict | None, str | None]:
        return _base_micro(
            ready_count=1,
            generated_at="2026-01-01T00:00:00Z",
            target_generated_at="2026-01-01T00:00:00Z",
        ), None

    rc = run_wait_until_ready_orchestration(
        project_root=pr_tmp,
        cfg=cfg,
        latest_path=latest,
        heartbeat_path=hb,
        targets_path=tgt,
        transport="fake",
        start_subprocess=False,
        read_micro_json=_read,
        report_dir=pr_tmp / "r_current_timeout",
    )
    assert rc == EXIT_WAIT_UNTIL_READY_TIMEOUT
    rep = next((pr_tmp / "r_current_timeout").glob("micro_until_ready_*.json"))
    body = json.loads(rep.read_text(encoding="utf-8"))
    assert body["status"] == "timeout"
    assert body["current_run_freshness_ok"] is False
    assert body["current_run_skip_reason"] == "target_generated_at_mismatch"


def test_runner_parse_failures_exit(tmp_path: Path) -> None:
    pr_tmp = tmp_path
    latest = pr_tmp / "latest.json"
    latest.write_text("{ not json", encoding="utf-8")
    hb = pr_tmp / "hb.json"
    tgt = pr_tmp / "targets.json"
    tgt.write_text("{}", encoding="utf-8")
    cfg = WaitUntilReadyConfig(
        max_wait_sec=10.0,
        poll_interval_sec=0.02,
        max_consecutive_read_failures=4,
    )
    rc = run_wait_until_ready_orchestration(
        project_root=pr_tmp,
        cfg=cfg,
        latest_path=latest,
        heartbeat_path=hb,
        targets_path=tgt,
        transport="fake",
        start_subprocess=False,
        read_micro_json=None,
        report_dir=pr_tmp / "r3",
    )
    assert rc == EXIT_CONFIG
    rep = next((pr_tmp / "r3").glob("micro_until_ready_*.json"))
    body = json.loads(rep.read_text(encoding="utf-8"))
    assert body["status"] == "error"
    assert body["read_error_count"] >= 4


def test_build_report_payload_top_reason_duplicate() -> None:
    cfg = WaitUntilReadyConfig()
    m = _base_micro(ready_count=0)
    m["items"][0]["micro_quality"]["ready"] = False
    m["items"][0]["micro_quality"]["reason_codes"] = ["a", "b"]
    p = build_run_report_payload(
        cfg=cfg,
        latest_path=Path("/tmp/x"),
        heartbeat_path=Path("/tmp/h"),
        targets_path=Path("/tmp/t"),
        started_at="s",
        ended_at="e",
        elapsed_sec=1,
        report_status="timeout",
        last_micro=m,
        read_error_count=0,
        last_read_error=None,
        error_message=None,
        daemon_pid=None,
    )
    assert isinstance(p["top_reason_codes"], list)


def test_missing_file_poll_timeout_no_read_errors(tmp_path: Path) -> None:
    pr_tmp = tmp_path
    latest = pr_tmp / "nope.json"
    hb = pr_tmp / "hb.json"
    tgt = pr_tmp / "targets.json"
    tgt.write_text("{}", encoding="utf-8")
    cfg = WaitUntilReadyConfig(
        max_wait_sec=0.25,
        poll_interval_sec=0.05,
        max_consecutive_read_failures=5,
    )
    rc = run_wait_until_ready_orchestration(
        project_root=pr_tmp,
        cfg=cfg,
        latest_path=latest,
        heartbeat_path=hb,
        targets_path=tgt,
        transport="fake",
        start_subprocess=False,
        read_micro_json=None,
        report_dir=pr_tmp / "r4",
    )
    assert rc == EXIT_WAIT_UNTIL_READY_TIMEOUT
    rep = next((pr_tmp / "r4").glob("micro_until_ready_*.json"))
    body = json.loads(rep.read_text(encoding="utf-8"))
    assert body["read_error_count"] == 0
    assert body["last_read_error"] is None


def test_run_pipeline_with_micro_wait_cli_flags_parse() -> None:
    from laoma_signal_engine.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(
        [
            "run-pipeline-with-micro",
            "--micro-wait-until-ready",
            "--micro-max-wait-sec",
            "123",
            "--micro-min-ready-count",
            "2",
        ]
    )
    assert args.command == "run-pipeline-with-micro"
    assert args.micro_wait_until_ready is True
    assert args.micro_max_wait_sec == 123.0
    assert args.micro_min_ready_count == 2


def test_run_pipeline_persistent_micro_wait_cli_flags_parse() -> None:
    from laoma_signal_engine.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(
        [
            "run-pipeline",
            "--wait-micro-ready",
            "--micro-max-wait-sec",
            "1200",
            "--micro-min-ready-count",
            "1",
        ]
    )
    assert args.command == "run-pipeline"
    assert args.wait_micro_ready is True
    assert args.micro_max_wait_sec == 1200.0
    assert args.micro_min_ready_count == 1


def test_micro_collector_daemon_cli_parse() -> None:
    from laoma_signal_engine.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["micro-collector-daemon", "--transport", "fake", "--once"])
    assert args.command == "micro-collector-daemon"
    assert args.transport == "fake"
    assert args.once is True
