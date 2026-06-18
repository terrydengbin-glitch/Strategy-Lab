"""Step 2.1 freshness gate unit and scanner integration tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from laoma_signal_engine.core.time_utils import to_iso_z
from laoma_signal_engine.scanner.abnormal_scanner import raw_signal_paths, run_abnormal_scan
from laoma_signal_engine.scanner.current_freshness import build_step2_current_freshness
from laoma_signal_engine.scanner.freshness_gate import (
    REASON_STALE_SNAPSHOT,
    classify_input_freshness,
    decide_freshness_gate,
    effective_hard_stale_sec,
    snapshot_age_sec,
)
from laoma_signal_engine.tests.test_abnormal_scanner_integration import (
    _minimal_snapshot,
    _minimal_universe,
)


def test_effective_hard_clamp_cli_below_max() -> None:
    assert effective_hard_stale_sec(config_hard=300, max_age_sec=180, cli_override=50) == 180


def test_effective_hard_uses_config_when_no_cli() -> None:
    assert effective_hard_stale_sec(config_hard=300, max_age_sec=180, cli_override=None) == 300


def test_classify_input_freshness_bands() -> None:
    assert classify_input_freshness(age_sec=100, max_age_sec=180, effective_hard_sec=300) == "fresh"
    assert classify_input_freshness(age_sec=200, max_age_sec=180, effective_hard_sec=300) == "degraded"
    assert classify_input_freshness(age_sec=400, max_age_sec=180, effective_hard_sec=300) == "stale"


def test_decide_freshness_gate_matrix() -> None:
    assert decide_freshness_gate(
        input_freshness="fresh", strict_freshness=True, allow_stale_input=False
    ).status == "ok"
    r = decide_freshness_gate(input_freshness="degraded", strict_freshness=True, allow_stale_input=False)
    assert not r.scan_allowed and r.status == "stale_input" and r.top_reason_codes == [REASON_STALE_SNAPSHOT]
    r2 = decide_freshness_gate(input_freshness="degraded", strict_freshness=False, allow_stale_input=False)
    assert r2.scan_allowed and r2.status == "ok_degraded" and r2.stale_warning
    r3 = decide_freshness_gate(input_freshness="stale", strict_freshness=True, allow_stale_input=False)
    assert not r3.scan_allowed and r3.top_reason_codes == [REASON_STALE_SNAPSHOT]
    r4 = decide_freshness_gate(input_freshness="stale", strict_freshness=True, allow_stale_input=True)
    assert r4.scan_allowed and r4.status == "ok_dev_stale_allowed" and r4.stale_warning


def test_snapshot_age_sec() -> None:
    now = datetime(2026, 1, 10, 12, 0, 0, tzinfo=UTC)
    assert snapshot_age_sec("2026-01-10T11:58:00Z", now=now) == 120


def test_stale_input_writes_empty_tier_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fixed = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("laoma_signal_engine.scanner.freshness_gate.utc_now", lambda: fixed)
    monkeypatch.setattr("laoma_signal_engine.scanner.abnormal_scanner.utc_now", lambda: fixed)

    snap_p = tmp_path / "DATA" / "market" / "futures_light_snapshot.json"
    snap_p.parent.mkdir(parents=True)
    univ_p = tmp_path / "DATA" / "universe" / "CANDIDATE_UNIVERSE.json"
    univ_p.parent.mkdir(parents=True)

    snap = _minimal_snapshot()
    snap = snap.model_copy(update={"generated_at": "2010-01-01T00:00:00Z"})
    with open(snap_p, "w", encoding="utf-8", newline="") as fp:
        json.dump(snap.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)

    univ = _minimal_universe()
    with open(univ_p, "w", encoding="utf-8", newline="") as fp:
        json.dump(univ.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)

    assert run_abnormal_scan(project_root=tmp_path, snapshot_path=snap_p, universe_path=univ_p) == 0

    for p in raw_signal_paths(tmp_path):
        with open(p, encoding="utf-8") as fp:
            doc = json.load(fp)
        assert doc["status"] == "stale_input"
        assert doc["reason_codes"] == ["stale_snapshot"]
        assert doc["count"] == 0
        assert doc["signals"] == []
        assert doc["input_freshness"] == "stale"


def test_allow_stale_scans_old_snapshot(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fixed = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("laoma_signal_engine.scanner.freshness_gate.utc_now", lambda: fixed)
    monkeypatch.setattr("laoma_signal_engine.scanner.abnormal_scanner.utc_now", lambda: fixed)

    snap_p = tmp_path / "DATA" / "market" / "futures_light_snapshot.json"
    snap_p.parent.mkdir(parents=True)
    univ_p = tmp_path / "DATA" / "universe" / "CANDIDATE_UNIVERSE.json"
    univ_p.parent.mkdir(parents=True)

    snap = _minimal_snapshot()
    snap = snap.model_copy(update={"generated_at": "2010-01-01T00:00:00Z"})
    with open(snap_p, "w", encoding="utf-8", newline="") as fp:
        json.dump(snap.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)

    univ = _minimal_universe()
    with open(univ_p, "w", encoding="utf-8", newline="") as fp:
        json.dump(univ.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)

    assert (
        run_abnormal_scan(
            project_root=tmp_path,
            snapshot_path=snap_p,
            universe_path=univ_p,
            allow_stale_input=True,
        )
        == 0
    )

    _, _, strong_path = raw_signal_paths(tmp_path)
    with open(strong_path, encoding="utf-8") as fp:
        doc = json.load(fp)
    assert doc["status"] == "ok_dev_stale_allowed"
    assert doc["stale_warning"] is True
    assert doc["input_freshness"] == "stale"
    assert doc["count"] == 1


def test_ok_degraded_when_strict_off(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fixed = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("laoma_signal_engine.scanner.freshness_gate.utc_now", lambda: fixed)
    monkeypatch.setattr("laoma_signal_engine.scanner.abnormal_scanner.utc_now", lambda: fixed)

    snap_p = tmp_path / "DATA" / "market" / "futures_light_snapshot.json"
    snap_p.parent.mkdir(parents=True)
    univ_p = tmp_path / "DATA" / "universe" / "CANDIDATE_UNIVERSE.json"
    univ_p.parent.mkdir(parents=True)

    snap = _minimal_snapshot()
    snap = snap.model_copy(update={"generated_at": to_iso_z(fixed - timedelta(seconds=200))})
    with open(snap_p, "w", encoding="utf-8", newline="") as fp:
        json.dump(snap.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)

    univ = _minimal_universe()
    with open(univ_p, "w", encoding="utf-8", newline="") as fp:
        json.dump(univ.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)

    assert (
        run_abnormal_scan(
            project_root=tmp_path,
            snapshot_path=snap_p,
            universe_path=univ_p,
            strict_freshness_override=False,
        )
        == 0
    )

    _, _, strong_path = raw_signal_paths(tmp_path)
    with open(strong_path, encoding="utf-8") as fp:
        doc = json.load(fp)
    assert doc["status"] == "ok_degraded"
    assert doc["stale_warning"] is True
    assert doc["input_freshness"] == "degraded"
    assert doc["count"] == 1


def test_step1016_step2_current_freshness_detects_old_ok_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    written_at = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    now = written_at + timedelta(seconds=600)
    monkeypatch.setattr("laoma_signal_engine.core.time_utils.utc_now", lambda: now)
    monkeypatch.setattr("laoma_signal_engine.scanner.freshness_gate.utc_now", lambda: written_at)
    monkeypatch.setattr("laoma_signal_engine.scanner.abnormal_scanner.utc_now", lambda: written_at)

    snap_p = tmp_path / "DATA" / "market" / "futures_light_snapshot.json"
    snap_p.parent.mkdir(parents=True)
    univ_p = tmp_path / "DATA" / "universe" / "CANDIDATE_UNIVERSE.json"
    univ_p.parent.mkdir(parents=True)

    snap = _minimal_snapshot().model_copy(update={"generated_at": to_iso_z(written_at)})
    with open(snap_p, "w", encoding="utf-8", newline="") as fp:
        json.dump(snap.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)
    univ = _minimal_universe()
    with open(univ_p, "w", encoding="utf-8", newline="") as fp:
        json.dump(univ.model_dump(mode="json"), fp, ensure_ascii=False, indent=2)

    assert run_abnormal_scan(project_root=tmp_path, snapshot_path=snap_p, universe_path=univ_p) == 0
    current = build_step2_current_freshness(project_root=tmp_path, max_age_sec=300)
    assert current["watch_status"] == "ok"
    assert current["strong_status"] == "ok"
    assert current["current_freshness"] == "stale"
    assert current["current_input_snapshot_age_sec"] == 600
    assert "step2_current_stale" in current["reason_codes"]
