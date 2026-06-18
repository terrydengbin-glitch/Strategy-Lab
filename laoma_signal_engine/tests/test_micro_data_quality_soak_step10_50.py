from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from laoma_signal_engine.audit.run_audit import init_run_audit_db
from laoma_signal_engine.micro.data_quality_attribution import init_micro_quality_db
from laoma_signal_engine.micro.data_quality_soak_audit import (
    build_micro_data_quality_soak_audit,
    write_micro_data_quality_soak_audit,
)


def _seed_db(root: Path) -> Path:
    db = root / "DATA/audit/run_audit.db"
    init_run_audit_db(db)
    init_micro_quality_db(db)
    with sqlite3.connect(db) as conn:
        for idx in range(1, 4):
            run_id = f"run_{idx}"
            conn.execute(
                """
                insert into audit_runs(run_id, cycle_id, status, generated_at, failure_count, warning_count, summary_json, payload_json)
                values(?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    f"cycle_{idx}",
                    "ok",
                    f"2099-01-01T00:0{idx}:00Z",
                    0,
                    0,
                    "{}",
                    "{}",
                ),
            )
        conn.execute(
            """
            insert into micro_quality_attributions(
              run_id, cycle_id, target_set_id, strategy_line, symbol, state,
              raw_reason, attributed_reason, category, recommended_action,
              evidence_completeness_ratio, missing_evidence_fields_json,
              evidence_json, generated_at, source_report_path
            ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "run_1",
                "cycle_1",
                "target_1",
                "micro_fast",
                "AAAUSDT",
                "not_ready",
                "cvd_stale",
                "technical_bug_cvd_adapter_not_updated",
                "technical_fix",
                "inspect CVD adapter",
                1.0,
                "[]",
                json.dumps({"driver_metrics_summary": {"processed_trade_bucket_count": 3}}),
                "2099-01-01T00:01:10Z",
                "report.md",
            ),
        )
        conn.execute(
            """
            insert into micro_quality_attributions(
              run_id, cycle_id, target_set_id, strategy_line, symbol, state,
              raw_reason, attributed_reason, category, recommended_action,
              evidence_completeness_ratio, missing_evidence_fields_json,
              evidence_json, generated_at, source_report_path
            ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "run_2",
                "cycle_2",
                "target_2",
                "micro_full",
                "BBBUSDT",
                "not_ready",
                "full_z_missing",
                "full_z_missing_warmup_incomplete",
                "expected_warmup",
                "wait until required observation window completes",
                1.0,
                "[]",
                "{}",
                "2099-01-01T00:02:10Z",
                "report.md",
            ),
        )
        conn.execute(
            """
            insert into audit_symbols(run_id, cycle_id, strategy_line, symbol, decision, action, entry_mode, executable, status, reason_codes_json, payload_json)
            values(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "run_3",
                "cycle_3",
                "without_micro",
                "CCCUSDT",
                "LONG",
                "ENTER_MARKET",
                "MARKET",
                1,
                "ok",
                "[]",
                json.dumps({"guards": {"trade_plan_consumable": True}}),
            ),
        )
        conn.execute(
            "insert into audit_downstream_events(run_id, event_type, strategy_line, symbol, status, payload_json) values(?,?,?,?,?,?)",
            ("run_3", "paper_order", "without_micro", "CCCUSDT", "OPEN", "{}"),
        )
    return db


def test_step1050_soak_audit_aggregates_recent_runs(tmp_path: Path) -> None:
    _seed_db(tmp_path)
    payload = build_micro_data_quality_soak_audit(tmp_path, lookback_runs=20, min_runs=3)

    assert payload["status"] == "ok"
    assert payload["summary"]["run_count"] == 3
    assert payload["summary"]["raw_reason_counts"]["cvd_stale"] == 1
    assert payload["summary"]["raw_reason_counts"]["full_z_missing"] == 1
    assert payload["summary"]["technical_fix_count"] == 1
    assert payload["summary"]["expected_warmup_count"] == 1
    assert payload["summary"]["violation_count"] == 0


def test_step1050_soak_audit_detects_downstream_violations(tmp_path: Path) -> None:
    db = _seed_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            insert into audit_symbols(run_id, cycle_id, strategy_line, symbol, decision, action, entry_mode, executable, status, reason_codes_json, payload_json)
            values(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "run_1",
                "cycle_1",
                "micro_fast",
                "AAAUSDT",
                "LONG",
                "ENTER_MARKET",
                "MARKET",
                1,
                "ok",
                "[]",
                json.dumps({"guards": {"trade_plan_consumable": True}}),
            ),
        )

    payload = build_micro_data_quality_soak_audit(tmp_path, lookback_runs=20, min_runs=3)

    assert payload["status"] == "failed"
    assert "downstream_consumption_violation" in payload["reason_codes"]
    assert payload["violations"][0]["type"] == "technical_not_ready_consumed_by_trade_plan"


def test_step1050_soak_audit_writes_report_files(tmp_path: Path) -> None:
    _seed_db(tmp_path)
    payload = write_micro_data_quality_soak_audit(tmp_path, lookback_runs=20, min_runs=3)

    assert Path(payload["findings_path"]).is_file()
    assert Path(payload["report_path"]).is_file()
    assert (tmp_path / "DATA/reports/latest_micro_data_quality_soak_audit.json").is_file()
