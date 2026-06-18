from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

import laoma_signal_engine.api.services as api_services
from laoma_signal_engine.api.app import app
from laoma_signal_engine.micro.data_quality_attribution import (
    attribute_micro_not_ready_reason,
    build_micro_evidence_runtime_v2,
    get_micro_evidence_runtime_v2,
    get_micro_quality_attribution,
    ingest_micro_evidence_runtime_v2_to_sqlite,
    write_micro_quality_attribution,
)
from laoma_signal_engine.strategy_pipeline import _append_micro_quality_attribution_stage


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _seed_micro_docs(root: Path) -> None:
    _write_json(
        root / "DATA/micro/latest_micro_features.json",
        {
            "run_id": "run_step10_43",
            "cycle_id": "cycle_001",
            "ws_status": "connected",
            "last_ws_message_age_sec": 3,
            "dropped_events": {},
            "items": [
                {
                    "symbol": "AAAUSDT",
                    "micro_fast_quality": {
                        "reason_codes": ["cvd_stale", "ofi_cvd_lag_high"],
                        "cvd_update_age_sec": 88,
                        "ofi_update_age_sec": 7,
                        "max_lag_sec": 81,
                        "reference_bucket_ts_sec": 1000,
                        "last_processed_bucket_ts_sec": 1000,
                        "last_ofi_update_bucket_ts_sec": 1000,
                        "cvd_age_bucket_sec": 88,
                        "ofi_age_bucket_sec": 7,
                        "ofi_cvd_lag_bucket_sec": 81,
                        "ofi_cvd_lag_side": "cvd_old",
                        "warmup_age_sec": 310,
                        "min_ready_seconds": 300,
                        "coverage": {
                            "aggTrade": {"expected_seconds": 300, "covered_seconds": 300},
                            "bookTicker": {"expected_seconds": 300, "covered_seconds": 290},
                            "partialDepth5": {"expected_seconds": 300, "covered_seconds": 280},
                        },
                        "driver_metrics_summary": {
                            "processed_bucket_count": 10,
                            "processed_trade_bucket_count": 3,
                            "processed_book_bucket_count": 9,
                            "cvd_update_count": 0,
                            "ofi_update_count": 9,
                        },
                    },
                },
                {
                    "symbol": "BBBUSDT",
                    "micro_full_quality": {
                        "reason_codes": ["full_z_missing"],
                        "warmup_age_sec": 120,
                        "min_ready_seconds": 900,
                        "coverage": {
                            "aggTrade": {"expected_seconds": 900, "covered_seconds": 850},
                            "bookTicker": {"expected_seconds": 900, "covered_seconds": 840},
                            "partialDepth5": {"expected_seconds": 900, "covered_seconds": 830},
                        },
                    },
                },
            ],
        },
    )
    _write_json(root / "DATA/micro/latest_micro_state.json", {"daemon_status": "running", "health_state": "healthy"})
    _write_json(
        root / "DATA/micro/latest_micro_lifecycle_micro_fast.json",
        {
            "run_id": "run_step10_43",
            "cycle_id": "cycle_001",
            "generated_at": "2026-05-30T01:00:00Z",
            "items": [
                {
                    "symbol": "AAAUSDT",
                    "state": "not_ready",
                    "terminal": True,
                    "trade_plan_consumable": False,
                    "reason_codes": ["cvd_stale", "ofi_cvd_lag_high"],
                    "observed_sec": 310,
                    "required_observed_sec": 300,
                }
            ],
        },
    )
    _write_json(
        root / "DATA/micro/latest_micro_lifecycle_micro_full.json",
        {
            "run_id": "run_step10_43",
            "cycle_id": "cycle_001",
            "generated_at": "2026-05-30T01:15:00Z",
            "items": [
                {
                    "symbol": "BBBUSDT",
                    "state": "timeout",
                    "terminal": True,
                    "trade_plan_consumable": False,
                    "reason_codes": ["full_z_missing"],
                    "observed_sec": 120,
                    "required_observed_sec": 900,
                }
            ],
        },
    )


def test_step10_43_reason_attribution_core_cases() -> None:
    assert (
        attribute_micro_not_ready_reason(
            "cvd_stale",
            {"ws_status": "connected", "aggtrade_coverage_ratio": 0.0, "cvd_update_age_sec": 20},
        )["attributed_reason"]
        == "cvd_stale_no_trade"
    )
    assert (
        attribute_micro_not_ready_reason("cvd_stale", {"ws_status": "connected"})["attributed_reason"]
        == "cvd_stale_unknown_missing_evidence"
    )
    assert (
        attribute_micro_not_ready_reason(
            "ofi_cvd_lag_high",
            {
                "ofi_cvd_lag_bucket_sec": 80,
                "cvd_age_bucket_sec": 90,
                "ofi_age_bucket_sec": 5,
            },
        )["attributed_reason"]
        == "ofi_new_cvd_old"
    )
    assert (
        attribute_micro_not_ready_reason(
            "full_z_missing",
            {"warmup_age_sec": 120, "warmup_required_sec": 900},
        )["category"]
        == "expected_warmup"
    )
    assert (
        attribute_micro_not_ready_reason(
            "cvd_never_updated",
            {
                "ws_status": "connected",
                "aggtrade_coverage_ratio": 0.7,
                "driver_metrics_summary": {"processed_bucket_count": 10, "cvd_update_count": 0},
            },
        )["category"]
        == "technical_fix"
    )
    assert (
        attribute_micro_not_ready_reason(
            "cvd_never_updated",
            {
                "ws_status": "connected",
                "aggtrade_coverage_ratio": 0.7,
                "cvd_runtime": {"never_updated_class": "adapter_commit_failed"},
                "driver_metrics_summary": {
                    "processed_bucket_count": 10,
                    "processed_trade_bucket_count": 3,
                    "cvd_update_count": 0,
                },
            },
        )["attributed_reason"]
        == "cvd_never_updated_adapter_commit_failed"
    )
    assert (
        attribute_micro_not_ready_reason(
            "cvd_never_updated",
            {
                "cvd_runtime": {
                    "never_updated_class": "low_activity_or_churn",
                },
            },
        )["category"]
        == "market_accept"
    )
    assert (
        attribute_micro_not_ready_reason(
            "ofi_stale",
            {
                "ws_status": "connected",
                "bookticker_coverage_ratio": 0.8,
                "ofi_update_age_sec": 40,
                "driver_metrics_summary": {"ofi_skipped_no_book": 3, "ofi_update_count": 0},
            },
        )["attributed_reason"]
        == "ofi_stale_no_book_in_bucket"
    )
    assert (
        attribute_micro_not_ready_reason(
            "ofi_stale",
            {
                "ws_status": "connected",
                "bookticker_coverage_ratio": 0.8,
                "ofi_update_age_sec": 40,
                "driver_metrics_summary": {
                    "processed_book_bucket_count": 2,
                    "ofi_update_count": 0,
                },
            },
        )["attributed_reason"]
        == "technical_bug_ofi_adapter_not_updated"
    )
    assert (
        attribute_micro_not_ready_reason(
            "coverage_aggtrade_weak",
            {"ws_status": "connected", "aggtrade_coverage_ratio": 0.0},
        )["attributed_reason"]
        == "coverage_aggtrade_weak_no_bucket"
    )
    assert (
        attribute_micro_not_ready_reason(
            "cvd_stale",
            {
                "ws_status": "connected",
                "subscription_state": {
                    "aggTrade": {
                        "required": True,
                        "active": False,
                        "missing_reason": "subscription_missing_aggTrade",
                    }
                },
            },
        )["attributed_reason"]
        == "cvd_stale_symbol_not_subscribed"
    )
    assert (
        attribute_micro_not_ready_reason(
            "ofi_stale",
            {
                "ws_status": "connected",
                "subscription_state": {
                    "bookTicker": {
                        "required": True,
                        "active": False,
                        "missing_reason": "subscription_missing_bookTicker",
                    }
                },
            },
        )["attributed_reason"]
        == "ofi_stale_symbol_not_subscribed"
    )


def test_step10_43_builds_report_and_sqlite(tmp_path: Path) -> None:
    _seed_micro_docs(tmp_path)

    payload = write_micro_quality_attribution(tmp_path)

    assert payload["run_id"] == "run_step10_43"
    assert payload["summary"]["not_ready_symbols"] == 2
    assert (tmp_path / "DATA/reports/latest_micro_quality_attribution.json").exists()
    assert (tmp_path / "DATA/reports/latest_micro_quality_attribution_report.txt").exists()

    db_path = tmp_path / "DATA/audit/run_audit.db"
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "select run_id, strategy_line, symbol, raw_reason, attributed_reason, category "
            "from micro_quality_attributions order by symbol"
        ).fetchall()
    assert rows[0][0] == "run_step10_43"
    assert {row[2] for row in rows} == {"AAAUSDT", "BBBUSDT"}
    assert any(row[4] == "technical_bug_cvd_adapter_not_updated" for row in rows)
    assert any(row[5] == "expected_warmup" for row in rows)
    bbb = next(row for row in payload["symbols"] if row["symbol"] == "BBBUSDT")
    assert bbb["evidence"]["z_window"]["mode"] == "full"
    assert bbb["evidence"]["z_window"]["z_window_required_count"] == 2
    assert bbb["attributions"][0]["attributed_reason"] == "full_z_missing_warmup_incomplete"


def test_step10_43_api_reads_micro_quality(tmp_path: Path, monkeypatch) -> None:
    _seed_micro_docs(tmp_path)
    write_micro_quality_attribution(tmp_path)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)

    client = TestClient(app)
    latest = client.get("/api/audit/micro-quality/latest")
    assert latest.status_code == 200
    assert latest.json()["data"]["run_id"] == "run_step10_43"

    by_id = client.get("/api/audit/micro-quality/run_step10_43")
    assert by_id.status_code == 200
    assert by_id.json()["data"]["summary"]["not_ready_symbols"] == 2


def test_step3_17_23_builds_micro_evidence_runtime_v2(tmp_path: Path) -> None:
    _seed_micro_docs(tmp_path)
    quality = write_micro_quality_attribution(tmp_path)

    payload = build_micro_evidence_runtime_v2(tmp_path, quality_payload=quality)
    assert payload["schema_version"] == "3.17-3.23-v2"
    assert payload["run_id"] == "run_step10_43"
    assert payload["summary"]["symbol_count"] == 2
    assert payload["summary"]["severity_counts"]["P0"] == 1
    aaa = next(row for row in payload["symbols"] if row["symbol"] == "AAAUSDT")
    assert aaa["factor_frame"]["cvd_status"] in {"missing", "stale"}
    assert aaa["factor_frame"]["alignment_status"] in {"missing", "lagging", "broken"}
    assert aaa["stream_heartbeat"]["streams"]["aggTrade"]["coverage_ratio"] == 1.0
    assert aaa["stream_heartbeat"]["streams"]["aggTrade"]["expected_bucket_count"] == 300
    assert aaa["factor_frame"]["adapter_commit_state"]["cvd_update_count"] == 0
    assert aaa["runtime_evidence"]["bucket_alignment"]["alignment_status"] in {"missing", "lagging", "broken"}
    assert aaa["runtime_evidence"]["bucket_alignment"]["commit_barrier_status"] in {"missing", "lagging", "broken"}
    assert aaa["runtime_evidence"]["cvd_runtime"]["never_updated_class"] == "adapter_commit_failed"
    assert aaa["runtime_evidence"]["aggtrade_runtime"]["bucket_gap_class"] == "adapter_gap"
    assert aaa["runtime_evidence"]["book_depth_runtime"]["ofi_gap_class"] == "ok"
    assert aaa["runtime_evidence"]["book_depth_runtime"]["queue_backpressure_state"] == "ok"
    assert aaa["runtime_evidence"]["coverage"]["aggTrade"]["gap_class"] == "ok"
    assert aaa["runtime_evidence"]["coverage_root_cause_v2"]["partialDepth5"]["role"] == "optional_evidence"
    assert aaa["runtime_evidence"]["coverage_root_cause_v2"]["partialDepth5"]["required_for_gate"] is False
    assert aaa["runtime_evidence"]["candidate_dwell"]["dwell_state"] == "judgeable_no_z"
    assert aaa["runtime_evidence"]["cvd_commit_missing_trace"]["root_cause"] == "adapter_commit_failed"
    assert aaa["runtime_evidence"]["fast_z_nan_trace"]["reason"] in {"insufficient_history", "ok"}
    assert aaa["runtime_evidence"]["judgeable_scope"]["scope"] == "judgeable_but_z_missing"
    assert aaa["runtime_evidence"]["fast_z_append_read_trace"]["trace_status"] in {
        "append_skipped_no_commit",
        "reader_window_short",
        "append_success_reader_empty",
    }
    assert aaa["runtime_evidence"]["cvd_ofi_bucket_freshness_trace"]["stale_root_cause"] == "cvd_missing_commit"
    assert aaa["z_window"]["z_window_required_count"] == 1
    bbb = next(row for row in payload["symbols"] if row["symbol"] == "BBBUSDT")
    assert bbb["z_window"]["missing_reason"] == "warmup_incomplete"
    assert bbb["runtime_evidence"]["z_history_runtime"]["history_gap_class"] == "warmup_incomplete"
    assert "target_retention_sec" in bbb["z_window"]

    ingest = ingest_micro_evidence_runtime_v2_to_sqlite(tmp_path, payload=payload)
    assert ingest["row_count"] == 2
    from_db = get_micro_evidence_runtime_v2(tmp_path, run_id="run_step10_43")
    assert from_db["source"] == "sqlite"
    assert from_db["summary"]["severity_counts"]["P0"] == 1
    assert {row["symbol"] for row in from_db["symbols"]} == {"AAAUSDT", "BBBUSDT"}


def test_step12_25_api_reads_micro_evidence_runtime_v2(tmp_path: Path, monkeypatch) -> None:
    _seed_micro_docs(tmp_path)
    write_micro_quality_attribution(tmp_path)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)

    client = TestClient(app)
    latest = client.get("/api/audit/micro-evidence/latest")
    assert latest.status_code == 200
    assert latest.json()["data"]["run_id"] == "run_step10_43"

    by_id = client.get("/api/audit/micro-evidence/run_step10_43")
    assert by_id.status_code == 200
    assert by_id.json()["data"]["summary"]["symbol_count"] == 2

    by_symbol = client.get("/api/audit/micro-evidence/symbols/AAAUSDT")
    assert by_symbol.status_code == 200
    assert by_symbol.json()["data"]["symbols"][0]["symbol"] == "AAAUSDT"

    findings = client.get("/api/audit/micro-evidence/findings?severity=P0&reason=ofi_cvd_lag_high")
    assert findings.status_code == 200
    assert findings.json()["data"]["count"] == 1
    assert findings.json()["data"]["symbols"][0]["symbol"] == "AAAUSDT"

    bucket_findings = client.get("/api/audit/micro-evidence/findings?bucket_gap_class=adapter_gap")
    assert bucket_findings.status_code == 200
    assert bucket_findings.json()["data"]["count"] == 1
    assert bucket_findings.json()["data"]["symbols"][0]["symbol"] == "AAAUSDT"

    history_findings = client.get("/api/audit/micro-evidence/findings?history_gap_class=warmup_incomplete")
    assert history_findings.status_code == 200
    assert history_findings.json()["data"]["count"] == 1
    assert history_findings.json()["data"]["symbols"][0]["symbol"] == "BBBUSDT"

    by_reason = client.get("/api/audit/micro-evidence/reasons/ofi_cvd_lag_high")
    assert by_reason.status_code == 200
    assert by_reason.json()["data"]["total_matched"] == 1

    tail_latest = client.get("/api/audit/micro-fast-runtime/tail-cleanup/latest")
    assert tail_latest.status_code == 200
    tail_payload = tail_latest.json()["data"]
    assert tail_payload["source"] == "micro_fast_tail_cleanup_contract"
    assert tail_payload["summary"]["depth5_role_counts"]["optional_evidence"] == 1
    assert tail_payload["symbols"][0]["cvd_commit_missing_trace"]["root_cause"] == "adapter_commit_failed"

    tail_reason = client.get("/api/audit/micro-fast-runtime/tail-cleanup/reasons/adapter_commit_failed")
    assert tail_reason.status_code == 200
    assert tail_reason.json()["data"]["total_matched"] == 1

    judgeable = client.get("/api/audit/micro-fast-runtime/judgeable/latest")
    assert judgeable.status_code == 200
    judge_payload = judgeable.json()["data"]
    assert judge_payload["source"] == "micro_fast_judgeable_runtime_contract"
    assert judge_payload["summary"]["scope_counts"]["judgeable_but_z_missing"] == 1
    assert judge_payload["symbols"][0]["cvd_ofi_bucket_freshness_trace"]["stale_root_cause"] == "cvd_missing_commit"

    judge_reason = client.get("/api/audit/micro-fast-runtime/judgeable/reasons/judgeable_but_z_missing")
    assert judge_reason.status_code == 200
    assert judge_reason.json()["data"]["total_matched"] == 1

    judge_symbol = client.get("/api/audit/micro-fast-runtime/judgeable/symbols/AAAUSDT")
    assert judge_symbol.status_code == 200
    assert judge_symbol.json()["data"]["total_matched"] == 1

    judgeable_only = client.get("/api/audit/micro-fast-runtime/judgeable-only/latest")
    assert judgeable_only.status_code == 200
    judgeable_only_payload = judgeable_only.json()["data"]
    assert judgeable_only_payload["source"] == "micro_fast_judgeable_only_metrics"
    assert judgeable_only_payload["summary"]["all_rows"] == 1
    assert "reader_window_short_root_cause_counts" in judgeable_only_payload["summary"]
    assert judgeable_only_payload["symbols"][0]["fast_z_reader_window_short_trace"]["root_cause"]

    judgeable_only_reason = client.get("/api/audit/micro-fast-runtime/judgeable-only/reasons/judgeable_but_z_missing")
    assert judgeable_only_reason.status_code == 200
    assert judgeable_only_reason.json()["data"]["total_matched"] == 1

    judgeable_only_symbol = client.get("/api/audit/micro-fast-runtime/judgeable-only/symbols/AAAUSDT")
    assert judgeable_only_symbol.status_code == 200
    assert judgeable_only_symbol.json()["data"]["total_matched"] == 1

    throughput = client.get("/api/audit/micro-fast-runtime/judgeable-throughput/latest")
    assert throughput.status_code == 200
    throughput_payload = throughput.json()["data"]
    assert throughput_payload["source"] == "micro_fast_judgeable_throughput_contract"
    assert throughput_payload["summary"]["runtime_rows"] == 1
    assert "not_judgeable_reason_counts" in throughput_payload["summary"]
    assert "judgeable_throughput_trace" in throughput_payload["symbols"][0]
    assert "target_cadence_trace" in throughput_payload["symbols"][0]
    assert "observe_pool_trace" in throughput_payload["symbols"][0]

    coverage = client.get("/api/audit/micro-fast-runtime/coverage-split/latest")
    assert coverage.status_code == 200
    coverage_payload = coverage.json()["data"]
    assert coverage_payload["source"] == "micro_fast_coverage_split_contract"
    assert "coverage_group_counts" in coverage_payload["summary"]
    assert "coverage_market_technical_split" in coverage_payload["symbols"][0]

    valid_bucket = client.get("/api/audit/micro-fast-runtime/valid-bucket/latest")
    assert valid_bucket.status_code == 200
    valid_bucket_payload = valid_bucket.json()["data"]
    assert valid_bucket_payload["source"] == "micro_fast_valid_bucket_contract"
    assert "valid_bucket_root_counts" in valid_bucket_payload["summary"]
    assert "valid_bucket_ratio_low_trace" in valid_bucket_payload["symbols"][0]


def test_step10_44_pipeline_tail_writes_current_micro_quality(tmp_path: Path) -> None:
    _seed_micro_docs(tmp_path)
    stages: list[dict] = []

    detail = _append_micro_quality_attribution_stage(
        stages,
        root=tmp_path,
        run_id="run_step10_43",
        cycle_id="cycle_001",
    )

    latest = json.loads((tmp_path / "DATA/reports/latest_micro_quality_attribution.json").read_text(encoding="utf-8"))
    assert detail["status"] == "ok"
    assert latest["run_id"] == "run_step10_43"
    assert latest["cycle_id"] == "cycle_001"
    assert latest["source_pipeline_run_id"] == "run_step10_43"
    assert stages[-1]["name"] == "micro_quality_attribution"
    assert stages[-1]["ok"] is True


def test_step10_44_expected_run_mismatch_returns_warning(tmp_path: Path) -> None:
    _seed_micro_docs(tmp_path)

    payload = write_micro_quality_attribution(
        tmp_path,
        expected_run_id="run_newer",
        expected_cycle_id="cycle_newer",
    )

    assert payload["status"] == "warning"
    assert "micro_quality_run_id_mismatch" in payload["reason_codes"]
    assert "micro_quality_cycle_id_mismatch" in payload["reason_codes"]
    assert payload["run_id"] == "run_newer"
    assert payload["cycle_id"] == "cycle_newer"


def test_step3_73_micro_full_standalone_scopes_and_rekeys_runtime(tmp_path: Path) -> None:
    _seed_micro_docs(tmp_path)
    _write_json(
        tmp_path / "DATA/micro/latest_micro_lifecycle_micro_fast.json",
        {
            "run_id": "old_fast_run",
            "cycle_id": "old_fast_cycle",
            "generated_at": "2026-05-30T01:00:00Z",
            "items": [
                {
                    "symbol": "AAAUSDT",
                    "state": "not_ready",
                    "terminal": True,
                    "trade_plan_consumable": False,
                    "reason_codes": ["cvd_stale"],
                }
            ],
        },
    )
    _write_json(
        tmp_path / "DATA/micro/latest_micro_lifecycle_micro_full.json",
        {
            "run_id": "full_run_new",
            "cycle_id": "full_cycle_new",
            "generated_at": "2026-05-30T02:00:00Z",
            "items": [
                {
                    "symbol": "BBBUSDT",
                    "state": "timeout",
                    "terminal": True,
                    "trade_plan_consumable": False,
                    "reason_codes": ["full_z_missing"],
                    "observed_sec": 120,
                    "required_observed_sec": 900,
                }
            ],
        },
    )

    payload = write_micro_quality_attribution(
        tmp_path,
        expected_run_id="full_run_new",
        expected_cycle_id="full_cycle_new",
        selected_lines=("micro_full",),
    )

    assert payload["status"] == "ok"
    assert payload["run_id"] == "full_run_new"
    assert payload["cycle_id"] == "full_cycle_new"
    assert payload["selected_lines"] == ["micro_full"]
    assert [row["line"] for row in payload["symbols"]] == ["micro_full"]
    assert [row["symbol"] for row in payload["symbols"]] == ["BBBUSDT"]
    assert payload["symbols"][0]["run_id"] == "full_run_new"

    with sqlite3.connect(tmp_path / "DATA/audit/run_audit.db") as conn:
        quality_rows = conn.execute(
            "select run_id, cycle_id, strategy_line, symbol from micro_quality_attributions where run_id = ?",
            ("full_run_new",),
        ).fetchall()
        runtime_rows = conn.execute(
            "select run_id, cycle_id, strategy_line, symbol from micro_evidence_runtime_v2_symbols where run_id = ?",
            ("full_run_new",),
        ).fetchall()

    assert quality_rows
    assert runtime_rows
    assert {row[2] for row in quality_rows} == {"micro_full"}
    assert {row[2] for row in runtime_rows} == {"micro_full"}
    assert {row[3] for row in runtime_rows} == {"BBBUSDT"}


def test_step10_44_api_latest_does_not_return_stale_micro_quality(tmp_path: Path, monkeypatch) -> None:
    _seed_micro_docs(tmp_path)
    write_micro_quality_attribution(tmp_path)
    _write_json(
        tmp_path / "DATA/reports/latest_strategy_pipeline_report.json",
        {
            "run_id": "run_newer",
            "cycle_id": "cycle_newer",
            "generated_at": "2026-05-30T02:00:00Z",
            "finished_at": "2026-05-30T02:10:00Z",
        },
    )
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)

    client = TestClient(app)
    latest = client.get("/api/audit/micro-quality/latest")

    assert latest.status_code == 200
    payload = latest.json()["data"]
    assert payload["source"] == "missing_current_run"
    assert payload["run_id"] == "run_newer"
    assert payload["stale_latest_run_id"] == "run_step10_43"


def test_step10_44_empty_current_run_json_fallback(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "DATA/reports/latest_micro_quality_attribution.json",
        {
            "schema_version": "10.43",
            "source": "micro_data_quality_attribution",
            "run_id": "run_empty",
            "cycle_id": "cycle_empty",
            "status": "ok_empty",
            "summary": {"not_ready_symbols": 0},
            "symbols": [],
        },
    )

    payload = get_micro_quality_attribution(tmp_path, run_id="run_empty")

    assert payload["source"] == "json_fallback"
    assert payload["run_id"] == "run_empty"
    assert payload["status"] == "ok_empty"
