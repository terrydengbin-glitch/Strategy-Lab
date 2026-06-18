from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from laoma_signal_engine.audit.run_audit import (
    build_run_level_audit,
    ingest_failed_pipeline_run_to_sqlite,
    ingest_run_audit_to_sqlite,
    write_run_level_audit,
)
from laoma_signal_engine.paper.models import PaperConfig
from laoma_signal_engine.paper.storage import PaperStore


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_run(tmp_path: Path) -> None:
    run_id = "run_1"
    cycle_id = "cycle_1"
    _write(
        tmp_path / "DATA/reports/latest_strategy_pipeline_report.json",
        {
            "generated_at": "2026-05-28T00:00:10Z",
            "run_id": run_id,
            "cycle_id": cycle_id,
            "status": "ok",
            "stages": [
                {"name": "common_upstream_step1_to_step2_5", "ok": True},
                {"name": "refresh_without_micro", "ok": True},
                {"name": "apply_trade_plan_without_micro", "ok": True},
                {"name": "wait_micro_ready_micro_fast", "ok": True, "detail": {"ready_count": 1}},
                {"name": "refresh_micro_fast", "ok": True},
                {"name": "apply_trade_plan_micro_fast", "ok": True},
                {"name": "wait_micro_ready_micro_full", "ok": True, "detail": {"full_ready_count": 1}},
                {"name": "refresh_micro_full", "ok": True},
                {"name": "apply_trade_plan_micro_full", "ok": True},
            ],
        },
    )
    for rel in [
        "DATA/universe/CANDIDATE_UNIVERSE.json",
        "DATA/market/futures_light_snapshot.json",
        "DATA/raw_signals/latest_raw_candidates.json",
        "DATA/raw_signals/latest_watch_signals.json",
        "DATA/raw_signals/latest_strong_candidates.json",
        "DATA/micro/micro_targets.json",
    ]:
        _write(tmp_path / rel, {"generated_at": "2026-05-28T00:00:01Z", "run_id": run_id, "cycle_id": cycle_id, "items": [{"symbol": "ABCUSDT"}]})
    for line in ("without_micro", "micro_fast", "micro_full"):
        _write(
            tmp_path / f"DATA/market/latest_decision_refresh_{line}_snapshot.json",
            {
                "generated_at": "2026-05-28T00:00:02Z",
                "run_id": run_id,
                "cycle_id": cycle_id,
                "items": [{"symbol": "ABCUSDT", "direction_still_valid": True, "refresh_age_sec": 2, "last_price": 1.0}],
            },
        )
        _write(
            tmp_path / f"DATA/decisions/latest_trade_plan_{line}.json",
            {
                "generated_at": "2026-05-28T00:00:03Z",
                "run_id": run_id,
                "cycle_id": cycle_id,
                "status": "partial",
                "count": 1,
                "executable_count": 1,
                "plans": [
                    {
                        "symbol": "ABCUSDT",
                        "decision": "LONG",
                        "action": "MARKET",
                        "entry_mode": "MARKET_NOW",
                        "estimated_entry_price": 1,
                        "stop_loss": 0.9,
                        "take_profit": 1.2,
                        "executable": True,
                        "reason_codes": [],
                        "guards": {"refresh_fresh": True, "direction_still_valid": True},
                    },
                ],
            },
        )
    for line in ("micro_fast", "micro_full"):
        _write(
            tmp_path / f"DATA/micro/latest_micro_lifecycle_{line}.json",
            {
                "generated_at": "2026-05-28T00:00:03Z",
                "run_id": run_id,
                "cycle_id": cycle_id,
                "items": [{"symbol": "ABCUSDT", "state": "confirmed", "trade_plan_consumable": True}],
            },
        )


def test_step7_15_build_run_level_audit_contract(tmp_path: Path) -> None:
    _seed_run(tmp_path)

    payload = build_run_level_audit(tmp_path)

    assert payload["run_id"] == "run_1"
    assert payload["cycle_id"] == "cycle_1"
    assert set(payload["strategy_lines"]) == {"without_micro", "micro_fast", "micro_full", "strategy5", "strategy6"}
    assert payload["summary"]["executable_count"]["micro_fast"] == 1
    assert payload["summary"]["funnel"]["micro_fast"]["lifecycle_consumable_count"] == 1
    assert payload["consumable_to_executable_funnel"]["micro_fast"]["executable_count"] == 1
    assert payload["symbols"][0]["refresh"]["present"] is True


def test_step778_strategy4_is_sidecar_audit_evidence(tmp_path: Path) -> None:
    _seed_run(tmp_path)
    _write(
        tmp_path / "DATA/decisions/strategy4_observe_pool.json",
        {
            "schema_version": "17.1",
            "count": 1,
            "status_counts": {"still_wait": 1},
            "items": [{"symbol": "XYZUSDT", "status": "still_wait"}],
        },
    )
    _write(
        tmp_path / "DATA/decisions/latest_trade_plan_strategy4.json",
        {
            "generated_at": "2026-05-28T00:05:00Z",
            "run_id": "strategy4_sidecar_run",
            "cycle_id": "strategy4_sidecar_cycle",
            "count": 1,
            "executable_count": 0,
            "plans": [{"symbol": "XYZUSDT", "decision": "WAIT", "action": "WAIT", "executable": False}],
        },
    )
    _write(tmp_path / "DATA/runtime/strategy4_daemon_status.json", {"state": "ok"})
    _write(tmp_path / "DATA/runtime/strategy4_heartbeat.json", {"updated_at": "2026-05-28T00:05:00Z"})
    db = tmp_path / "DATA/strategy4/strategy4_observe.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE strategy4_attempts (
              attempt_id TEXT,
              symbol TEXT,
              run_id TEXT,
              cycle_id TEXT,
              attempted_at TEXT,
              status TEXT,
              decision TEXT,
              action TEXT,
              entry_mode TEXT,
              executable INTEGER,
              reason_codes_json TEXT,
              plan_json TEXT,
              lineage_json TEXT,
              original_side TEXT,
              current_side TEXT,
              side_changed INTEGER
            )
            """,
        )
        conn.execute(
            "INSERT INTO strategy4_attempts(attempt_id, symbol, status, action, executable) VALUES(?,?,?,?,?)",
            ("attempt_1", "XYZUSDT", "still_wait", "WAIT", 0),
        )

    payload = build_run_level_audit(tmp_path)

    assert "strategy4" not in payload["strategy_lines"]
    sidecar = payload["sidecar_lines"]["strategy4"]
    assert sidecar["pipeline_selected"] is False
    assert sidecar["pool_count"] == 1
    assert sidecar["status_counts"]["still_wait"] == 1
    assert sidecar["attempt_count"] == 1
    assert sidecar["latest_trade_plan"]["count"] == 1
    assert sidecar["latest_trade_plan"]["output_fresh"] is False
    assert payload["summary"]["sidecar_line_status"]["strategy4"]["pool_count"] == 1


def test_step783_strategy5_reuses_without_micro_refresh_evidence(tmp_path: Path) -> None:
    _seed_run(tmp_path)
    _write(
        tmp_path / "DATA/reports/latest_strategy_pipeline_report.json",
        {
            "generated_at": "2026-06-05T00:00:10Z",
            "run_id": "run_1",
            "cycle_id": "cycle_1",
            "status": "ok",
            "selected_lines": ["strategy5"],
            "skipped_lines": ["without_micro", "micro_fast", "micro_full"],
            "stages": [
                {"name": "common_upstream_step1_to_step2_5", "ok": True},
                {"name": "apply_trade_plan_strategy5", "ok": True},
                {"name": "paper_wakeup_strategy5", "ok": True},
            ],
        },
    )
    _write(
        tmp_path / "DATA/decisions/latest_trade_plan_strategy5.json",
        {
            "generated_at": "2026-06-05T00:00:03Z",
            "run_id": "run_1",
            "cycle_id": "cycle_1",
            "source": "trade_plan_strategy5",
            "status": "partial",
            "count": 1,
            "executable_count": 0,
            "plans": [
                {
                    "symbol": "ABCUSDT",
                    "decision": "LONG",
                    "action": "WAIT",
                    "entry_mode": "WAIT_PULLBACK",
                    "executable": False,
                    "reason_codes": ["strategy5_shadow_blocked_not_promoted"],
                    "guards": {"refresh_fresh": True, "direction_still_valid": True},
                },
            ],
        },
    )

    payload = build_run_level_audit(tmp_path)

    strategy5 = payload["strategy_lines"]["strategy5"]
    assert strategy5["selected"] is True
    assert strategy5["artifact_refs"]["refresh"]["key"] == "refresh_strategy5"
    assert strategy5["artifact_refs"]["refresh"]["exists"] is True
    assert payload["symbols"][0]["strategy_line"] == "strategy5"
    assert payload["symbols"][0]["refresh"]["present"] is True
    assert not [
        row
        for row in payload["failures"]
        if row.get("scope") == "strategy5" and row.get("name") == "line.pre_trade_refresh.present"
    ]


def test_step1418_run_audit_treats_paper_skip_ledger_as_explained_outcome(tmp_path: Path) -> None:
    _seed_run(tmp_path)
    store = PaperStore(
        tmp_path,
        PaperConfig(db_path="DATA/paper/paper_trading.db", summary_path="DATA/paper/latest_paper_state.json"),
    )
    store.initialize()
    store.record_skip(
        {
            "strategy_line": "micro_fast",
            "symbol": "ABCUSDT",
            "source_run_id": "run_1",
            "source_cycle_id": "cycle_1",
            "source_generated_at": "2026-05-28T00:00:03Z",
            "source_plan_hash": "hash_abc",
            "source_executable": True,
            "paper_eligible": True,
            "source_json": {"symbol": "ABCUSDT", "executable": True},
        },
        reason="active_slot_occupied",
    )

    payload = build_run_level_audit(tmp_path)

    funnel = payload["consumable_to_executable_funnel"]["micro_fast"]
    assert funnel["paper_order_count"] == 0
    assert funnel["paper_skip_count"] == 1
    assert funnel["paper_skipped_reason_counts"]["skipped_same_symbol_open"] == 1
    assert funnel["executable_missing_paper_symbols"] == []
    warning_scopes = {
        row["scope"]
        for row in payload["warnings"]
        if row["name"] == "funnel.executable_missing_paper"
    }
    assert "micro_fast" not in warning_scopes


def test_step747_pending_paper_settlement_downgrades_missing_paper_to_warning(tmp_path: Path) -> None:
    _seed_run(tmp_path)
    report_path = tmp_path / "DATA/reports/latest_strategy_pipeline_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["paper_settlement_barrier"] = {
        "schema_version": "7.47",
        "status": "pending",
        "run_id": "run_1",
        "missing_by_line": {"micro_fast": ["ABCUSDT"]},
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")

    payload = build_run_level_audit(tmp_path)

    assert payload["summary"]["paper_settlement_status"] == "pending"
    assert not any(row["name"] == "funnel.executable_missing_paper" for row in payload["failures"])
    warning = next(row for row in payload["warnings"] if row["name"] == "funnel.executable_missing_paper_pending_settlement")
    assert warning["warning_class"] == "paper_settlement"
    assert warning["detail"]["paper_settlement_status"] == "pending"


def test_step747_complete_paper_settlement_keeps_missing_paper_as_p0(tmp_path: Path) -> None:
    _seed_run(tmp_path)
    report_path = tmp_path / "DATA/reports/latest_strategy_pipeline_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["paper_settlement_barrier"] = {
        "schema_version": "7.47",
        "status": "missing_after_settlement",
        "run_id": "run_1",
        "missing_by_line": {"micro_fast": ["ABCUSDT"]},
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")

    payload = build_run_level_audit(tmp_path)

    failure = next(row for row in payload["failures"] if row["name"] == "funnel.executable_missing_paper")
    assert failure["detail"]["paper_settlement_status"] == "missing_after_settlement"
    assert payload["summary"]["paper_settlement_status"] == "missing_after_settlement"


def test_step746_micro_wait_count_drift_is_business_warning_not_failure(tmp_path: Path) -> None:
    _seed_run(tmp_path)
    report_path = tmp_path / "DATA/reports/latest_strategy_pipeline_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    for stage in report["stages"]:
        if stage["name"] == "wait_micro_ready_micro_fast":
            stage["detail"] = {
                "symbol_counts": {"usable_ready": 1},
                "wait_evidence_path": str(tmp_path / "DATA/micro/evidence/latest_wait_pass_micro_fast.json"),
            }
    report_path.write_text(json.dumps(report), encoding="utf-8")
    plan_path = tmp_path / "DATA/decisions/latest_trade_plan_micro_fast.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["input_refs"] = {"micro_wait_evidence_used": True, "symbol_counts": {"ready": 3, "consumable": 1}}
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    payload = build_run_level_audit(tmp_path)

    failures = {(row["scope"], row["name"]) for row in payload["failures"]}
    warning = next(
        row
        for row in payload["warnings"]
        if row["scope"] == "micro_fast" and row["name"] == "line.micro_wait_trade_plan_ready_count.aligned"
    )
    assert ("micro_fast", "line.micro_wait_trade_plan_ready_count.aligned") not in failures
    assert warning["warning_class"] == "business"
    assert payload["technical_failure_count"] >= 0


def test_step746_skipped_not_selected_line_ignores_stale_current_json(tmp_path: Path) -> None:
    _seed_run(tmp_path)
    report_path = tmp_path / "DATA/reports/latest_strategy_pipeline_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["selected_lines"] = ["without_micro", "micro_fast"]
    report["skipped_lines"] = ["micro_full"]
    report_path.write_text(json.dumps(report), encoding="utf-8")
    plan_path = tmp_path / "DATA/decisions/latest_trade_plan_micro_full.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["run_id"] = "old_run"
    plan["cycle_id"] = "old_cycle"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    payload = build_run_level_audit(tmp_path)

    assert payload["strategy_lines"]["micro_full"]["status"] == "skipped_not_selected"
    assert payload["strategy_lines"]["micro_full"]["selected"] is False
    assert all(row["scope"] != "micro_full" for row in payload["failures"])
    assert not any(row["scope"] == "micro_full" and row["name"] == "line.no_executable" for row in payload["warnings"])


def test_step746_relaxed_micro_policy_is_not_confirmed_only_p0(tmp_path: Path) -> None:
    _seed_run(tmp_path)
    plan_path = tmp_path / "DATA/decisions/latest_trade_plan_micro_fast.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    guards = plan["plans"][0]["guards"]
    guards.update(
        {
            "micro_consumption_policy": "ready_signal_usable",
            "allow_weak_micro_consumption": True,
            "micro_symbol_confirmed": False,
            "micro_direction_confirmed": False,
            "micro_exec_allowed": False,
            "micro_lifecycle_state": "confirmed",
            "micro_policy_relaxed": True,
            "micro_confirmation_strength": "weak",
            "trade_plan_consumable": True,
            "consumption_block_reason": "",
        },
    )
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    payload = build_run_level_audit(tmp_path)

    assert not any(
        row.get("scope") == "micro_fast"
        and row.get("symbol") == "ABCUSDT"
        and "micro_confirmed_only" in ",".join(row.get("reason_codes") or [])
        for row in payload["failures"]
    )


def test_step7_16_ingests_run_audit_sqlite(tmp_path: Path) -> None:
    _seed_run(tmp_path)
    payload = write_run_level_audit(tmp_path)

    result = ingest_run_audit_to_sqlite(tmp_path)

    assert result["run_id"] == payload["run_id"]
    db = tmp_path / "DATA/audit/run_audit.db"
    with sqlite3.connect(db) as conn:
        run_count = conn.execute("select count(*) from audit_runs where run_id = ?", ("run_1",)).fetchone()[0]
        symbol_count = conn.execute("select count(*) from audit_symbols where run_id = ?", ("run_1",)).fetchone()[0]
        artifact_count = conn.execute("select count(*) from audit_artifacts where run_id = ?", ("run_1",)).fetchone()[0]
    assert run_count == 1
    assert symbol_count == 3
    assert artifact_count > 5


def test_step7_38_ingests_failed_pipeline_minimal_ledger(tmp_path: Path) -> None:
    report = {
        "schema_version": "1.0",
        "source": "strategy_pipeline",
        "run_id": "run_failed",
        "cycle_id": "cycle_failed",
        "line": "all",
        "selected_lines": ["without_micro", "micro_fast"],
        "skipped_lines": ["micro_full"],
        "status": "failed",
        "generated_at": "2026-06-01T00:00:00Z",
        "first_failed_stage": "wait_micro_ready_micro_fast",
        "first_failed_stage_status": "failed",
        "first_failed_stage_rc": 4,
        "failure_reason": "wait timeout",
        "outputs": {
            "strategy_report": str(tmp_path / "DATA/reports/latest_strategy_pipeline_report.json"),
            "pipeline_report_archive": str(tmp_path / "DATA/reports/pipeline_runs/run_failed/strategy_pipeline_report.json"),
        },
    }

    result = ingest_failed_pipeline_run_to_sqlite(tmp_path, report=report)

    assert result["run_id"] == "run_failed"
    with sqlite3.connect(tmp_path / "DATA/audit/run_audit.db") as conn:
        row = conn.execute("select status, summary_json, payload_json from audit_runs where run_id = ?", ("run_failed",)).fetchone()
        event = conn.execute(
            "select status, payload_json from audit_downstream_events where run_id = ? and event_type = ?",
            ("run_failed", "pipeline_failed"),
        ).fetchone()
        symbol_count = conn.execute("select count(*) from audit_symbols where run_id = ?", ("run_failed",)).fetchone()[0]
    assert row is not None
    assert row[0] == "failed"
    assert json.loads(row[1])["first_failed_stage"] == "wait_micro_ready_micro_fast"
    assert json.loads(row[2])["status"] == "failed"
    assert event is not None
    assert event[0] == "failed"
    assert symbol_count == 0
