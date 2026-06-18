from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import yaml
from fastapi.testclient import TestClient

import laoma_signal_engine.api.services as api_services
from laoma_signal_engine.api.app import app
from laoma_signal_engine.core.config_loader import package_root
from laoma_signal_engine.paper.candles import StaticCandleProvider
from laoma_signal_engine.paper.engine import PaperEngine
from laoma_signal_engine.paper.models import Candle, PaperConfig
from laoma_signal_engine.trade_quality import analyze_paper_trades


def _use_temp_config(tmp_path: Path, monkeypatch) -> Path:
    src = package_root() / "config" / "default.yaml"
    dst = tmp_path / "default.yaml"
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setattr(api_services, "CONFIG_PATH", dst)
    return dst


def _mock_snapshot_warmup_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        api_services,
        "snapshot_warmup_payload",
        lambda *args, **kwargs: {
            "schema_version": "STEP16.8_snapshot_warmup_v1",
            "status": "ready",
            "ready": True,
            "allow_run_once": True,
            "allow_run_cycle": True,
            "usable_symbol_count": 3,
            "fresh_count": 3,
            "stale_usable_count": 0,
            "stale_blocked_count": 0,
            "min_usable_symbol_count": 3,
            "disabled_reason": None,
            "reason_codes": [],
        },
    )


def _paper_test_config() -> PaperConfig:
    return PaperConfig(
        db_path="DATA/paper/test_api_trade_quality.db",
        summary_path="DATA/paper/latest_paper_state.json",
        default_slippage_bps=0,
        taker_fee_bps=0,
    )


def _trade_quality_doc() -> dict:
    return {
        "schema_version": "1.0",
        "generated_at": "2026-06-03T00:00:00Z",
        "run_id": "run_api_tq",
        "cycle_id": "cycle_api_tq",
        "source": "trade_plan_without_micro",
        "micro_mode": "none",
        "status": "ok",
        "count": 1,
        "executable_count": 1,
        "input_refs": {},
        "plans": [
            {
                "symbol": "TQUSDT",
                "decision_tf": "15m",
                "decision": "LONG",
                "action": "ENTER_MARKET",
                "entry_mode": "MARKET",
                "estimated_entry_price": 100,
                "stop_loss": 95,
                "take_profit": 110,
                "risk_per_unit": 5,
                "reward_per_unit": 10,
                "rr": 2.0,
                "executable": True,
                "confidence": 80,
                "reason_codes": [],
                "guards": {"line": "without_micro", "margin_usdt": 100, "leverage": 20},
                "input_refs": {"source_plan_hash": "run_api_tq_without_micro_TQUSDT_LONG"},
            }
        ],
    }


def _prepare_trade_quality_db(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    config_path = _use_temp_config(tmp_path, monkeypatch)
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw.setdefault("paper", {})
    raw["paper"]["db_path"] = _paper_test_config().db_path
    raw["paper"]["summary_path"] = _paper_test_config().summary_path
    config_path.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8")
    doc = _trade_quality_doc()
    engine = PaperEngine(
        tmp_path,
        config=_paper_test_config(),
        candle_provider=StaticCandleProvider({"TQUSDT": [Candle("TQUSDT", 1, 100, 101, 99, 100)]}),
    )
    engine.tick({"without_micro": doc})
    engine.candle_provider = StaticCandleProvider({"TQUSDT": [Candle("TQUSDT", 2, 100, 112, 97, 110)]})
    engine.tick({})
    analyze_paper_trades(
        tmp_path,
        config=_paper_test_config(),
        candle_provider=StaticCandleProvider(
            {"TQUSDT": [Candle("TQUSDT", 1, 100, 101, 99, 100), Candle("TQUSDT", 2, 100, 112, 97, 110)]}
        ),
        persist=True,
    )


def test_step12_health_contract() -> None:
    client = TestClient(app)

    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["service"] == "abnormal-signal-engine-api"
    assert payload["error"] is None


def test_step1274_strategy_sandbox_api_contract(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LAOMA_SANDBOX_ROOT", str(tmp_path / "sandboxes"))
    client = TestClient(app)

    create = client.post(
        "/api/strategy-sandbox/sandboxes",
        json={
            "strategy_line": "strategy6",
            "strategy_version": "api-test",
            "data_scope": {"days": 3, "symbols": ["BTCUSDT"]},
            "config_scope": {"mode": "shadow_only"},
            "tags": ["api-test"],
        },
    )
    assert create.status_code == 200
    payload = create.json()
    assert payload["ok"] is True
    sandbox_id = payload["data"]["sandbox"]["sandbox_id"]

    external_switch = client.put(
        "/api/strategy-sandbox/active",
        json={
            "sandbox_id": sandbox_id,
            "caller_type": "external_ai_trader",
            "caller_id": "external-smoke",
            "source_surface": "external_connector",
        },
    )
    assert external_switch.status_code == 200
    assert external_switch.json()["ok"] is False
    assert "external_active_context_write_denied" in external_switch.json()["error"]["message"]

    switched = client.put("/api/strategy-sandbox/active", json={"sandbox_id": sandbox_id})
    assert switched.status_code == 200
    assert switched.json()["data"]["active_sandbox_id"] == sandbox_id

    listed = client.get("/api/strategy-sandbox/sandboxes?limit=10")
    assert listed.status_code == 200
    assert listed.json()["data"]["count"] == 1

    job = client.post(f"/api/strategy-sandbox/sandboxes/{sandbox_id}/trade-quality", json={"options": {"smoke": True}})
    assert job.status_code == 200
    assert job.json()["data"]["status"] == "completed"

    summary = client.get(f"/api/strategy-sandbox/sandboxes/{sandbox_id}/summary")
    assert summary.status_code == 200
    assert summary.json()["data"]["summary"]["counts"]["trade_quality_samples"] >= 1

    health = client.get(f"/api/strategy-sandbox/sandboxes/{sandbox_id}/db-health")
    assert health.status_code == 200
    assert health.json()["data"]["health"]["integrity_check"] == "ok"

    external_purge = client.request(
        "DELETE",
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}",
        json={
            "mode": "purge",
            "reason": "external-smoke",
            "confirm": True,
            "caller_type": "external_ai_trader",
            "caller_id": "external-smoke",
            "source_surface": "external_connector",
        },
    )
    assert external_purge.status_code == 200
    assert external_purge.json()["ok"] is False
    assert "external_purge_denied" in external_purge.json()["error"]["message"]

    deleted = client.request(
        "DELETE",
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}",
        json={"mode": "soft_delete", "reason": "api-test", "confirm": False},
    )
    assert deleted.status_code == 200
    assert deleted.json()["data"]["status"] == "deleted"

    active_after_delete = client.get("/api/strategy-sandbox/active")
    assert active_after_delete.status_code == 200
    assert active_after_delete.json()["data"]["active"] is None


def test_step302_strategy_sandbox_header_api_key_boundary(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LAOMA_SANDBOX_ROOT", str(tmp_path / "sandboxes"))
    monkeypatch.setenv("LAOMA_SANDBOX_REQUIRE_API_KEY", "1")
    monkeypatch.setenv("LAOMA_SANDBOX_API_KEY", "secret-test-key")
    client = TestClient(app)

    denied = client.post(
        "/api/strategy-sandbox/sandboxes",
        json={
            "strategy_line": "strategy6",
            "strategy_version": "missing-key",
            "data_scope": {"days": 1, "symbols": ["BTCUSDT"]},
            "config_scope": {"mode": "shadow_only"},
        },
    )
    assert denied.status_code == 200
    assert denied.json()["ok"] is False
    assert "api_key_required" in denied.json()["error"]["message"]

    allowed = client.post(
        "/api/strategy-sandbox/sandboxes",
        headers={
            "X-Sandbox-Api-Key": "secret-test-key",
            "X-Sandbox-Caller-Type": "external_ai_trader",
            "X-Sandbox-Caller-Id": "header-caller",
            "X-Sandbox-Source-Surface": "external_connector",
        },
        json={
            "strategy_line": "strategy6",
            "strategy_version": "header-key",
            "data_scope": {"days": 1, "symbols": ["BTCUSDT"]},
            "config_scope": {"mode": "shadow_only"},
        },
    )
    assert allowed.status_code == 200
    payload = allowed.json()
    assert payload["ok"] is True
    assert payload["data"]["caller_id"] == "header-caller"
    assert payload["data"]["caller_identity_source"] == "header_api_key"
    assert payload["data"]["authenticated"] is True


def test_step271_strategy_sandbox_external_full_backtest_api_contract(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LAOMA_SANDBOX_ROOT", str(tmp_path / "sandboxes"))
    client = TestClient(app)

    create = client.post(
        "/api/strategy-sandbox/sandboxes",
        json={
            "strategy_line": "strategy6",
            "strategy_version": "step27-api-test",
            "data_scope": {"days": 3, "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]},
            "config_scope": {"mode": "shadow_only"},
            "tags": ["api-test", "step27.1"],
        },
    )
    assert create.status_code == 200
    assert create.json()["ok"] is True
    sandbox_id = create.json()["data"]["sandbox"]["sandbox_id"]

    universe = client.get(f"/api/strategy-sandbox/universe?strategy_line=strategy6&sandbox_id={sandbox_id}")
    assert universe.status_code == 200
    assert universe.json()["ok"] is True
    assert universe.json()["data"]["symbols"] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    manifest = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/full-backtest-runs",
        json={
            "strategy_line": "strategy6",
            "symbols": ["BTCUSDT", "SOLUSDT"],
            "time_start": "2026-06-01T00:00:00Z",
            "time_end": "2026-06-02T00:00:00Z",
            "batch_size": 1,
            "idempotency_key": "api-step27-1",
            "resource_budget": {"max_workers": 1},
        },
    )
    assert manifest.status_code == 200
    assert manifest.json()["ok"] is True
    data = manifest.json()["data"]
    run_id = data["run_id"]
    assert data["status"] == "manifest_ready"
    assert data["external_full_backtest_run_id"] == run_id
    assert data["expected_batches"] == 2
    assert data["resource_budget"]["symbol_batch_size"] == 1
    assert data["operation_id"].startswith("sbop_")

    replay = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/full-backtest-runs",
        json={"strategy_line": "strategy6", "idempotency_key": "api-step27-1"},
    )
    assert replay.status_code == 200
    assert replay.json()["data"]["run_id"] == run_id
    assert replay.json()["data"]["idempotent_replay"] is True

    fetched = client.get(f"/api/strategy-sandbox/sandboxes/{sandbox_id}/full-backtest-runs/{run_id}")
    assert fetched.status_code == 200
    assert fetched.json()["data"]["run_id"] == run_id

    canceled = client.post(f"/api/strategy-sandbox/sandboxes/{sandbox_id}/full-backtest-runs/{run_id}/cancel")
    assert canceled.status_code == 200
    assert canceled.json()["data"]["status"] == "canceled"

    resumed = client.post(f"/api/strategy-sandbox/sandboxes/{sandbox_id}/full-backtest-runs/{run_id}/resume")
    assert resumed.status_code == 200
    assert resumed.json()["data"]["status"] == "manifest_ready"


def test_step272_strategy_sandbox_external_gate_candidate_api_contract(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LAOMA_SANDBOX_ROOT", str(tmp_path / "sandboxes"))
    client = TestClient(app)

    create = client.post(
        "/api/strategy-sandbox/sandboxes",
        json={
            "strategy_line": "strategy6",
            "strategy_version": "step27-gate-api-test",
            "data_scope": {"days": 3, "symbols": ["BTCUSDT", "ETHUSDT"]},
            "config_scope": {"mode": "shadow_only"},
            "tags": ["api-test", "step27.2"],
        },
    )
    assert create.status_code == 200
    assert create.json()["ok"] is True
    sandbox_id = create.json()["data"]["sandbox"]["sandbox_id"]

    job = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/backtest",
        json={"options": {"symbols": ["BTCUSDT", "ETHUSDT"], "pytest": True}},
    )
    assert job.status_code == 200
    assert job.json()["ok"] is True

    candidates = client.get(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/trade-candidates?limit=10"
    )
    assert candidates.status_code == 200
    assert candidates.json()["ok"] is True
    data = candidates.json()["data"]
    assert data["count"] >= 1
    candidate = data["candidates"][0]
    assert candidate["candidate_id"]
    assert candidate["leakage_status"] == "pass"
    assert "_sandbox_rows" not in candidate["decision_time_features"]

    action = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/gate-actions",
        json={
            "run_id": candidate["run_id"],
            "candidate_id": candidate["candidate_id"],
            "unit_id": "unit_api",
            "unit_version": "v1",
            "selection_id": "sel_api",
            "scorer_output_ref": "ai_trader://scorer/api",
            "final_gate_decision_ref": "ai_trader://final-gate/api",
            "gate_decision": "reduce_size",
            "gate_action_payload": {
                "action": "reduce_size",
                "original_size": candidate["intended_size"],
                "size_multiplier": 0.5,
                "threshold_policy_version": "api-test",
                "calibration_status": "ok",
                "bad_trade_risk": 0.73,
                "deterministic": True,
                "final_gate_decision_by_llm": False,
            },
            "reason_codes": ["api_reduce_size"],
            "audit_trace_id": "trace_api",
            "idempotency_key": "api-step27-2-action",
        },
    )
    assert action.status_code == 200
    assert action.json()["ok"] is True
    assert action.json()["data"]["accepted"] is True
    assert action.json()["data"]["gate_decision"] == "reduce_size"
    assert action.json()["data"]["operation_id"].startswith("sbop_")

    duplicate = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/gate-actions",
        json={
            "run_id": candidate["run_id"],
            "candidate_id": candidate["candidate_id"],
            "unit_id": "unit_api",
            "unit_version": "v1",
            "gate_decision": "reduce_size",
            "idempotency_key": "api-step27-2-action",
        },
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["data"]["status"] == "duplicate"

    missing = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/gate-actions",
        json={
            "run_id": candidate["run_id"],
            "candidate_id": "missing_candidate",
            "unit_id": "unit_api",
            "unit_version": "v1",
            "gate_decision": "review",
            "idempotency_key": "api-step27-2-missing",
        },
    )
    assert missing.status_code == 200
    assert missing.json()["data"]["accepted"] is False
    assert missing.json()["data"]["error_code"] == "candidate_missing"


def test_step273_274_strategy_sandbox_external_gated_execution_and_audit_api_contract(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LAOMA_SANDBOX_ROOT", str(tmp_path / "sandboxes"))
    client = TestClient(app)

    create = client.post(
        "/api/strategy-sandbox/sandboxes",
        json={
            "strategy_line": "strategy6",
            "strategy_version": "step27-gated-api-test",
            "data_scope": {"days": 3, "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]},
            "config_scope": {"mode": "shadow_only"},
            "tags": ["api-test", "step27.3", "step27.4"],
        },
    )
    assert create.status_code == 200
    sandbox_id = create.json()["data"]["sandbox"]["sandbox_id"]

    job = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/trade-quality",
        json={"options": {"symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"], "pytest": True}},
    )
    assert job.status_code == 200
    assert job.json()["ok"] is True

    candidates = client.get(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/trade-candidates?limit=10"
    )
    assert candidates.status_code == 200
    candidate_rows = candidates.json()["data"]["candidates"][:2]
    assert len(candidate_rows) == 2

    for idx, candidate in enumerate(candidate_rows):
        decision = "block" if idx == 0 else "reduce_size"
        action = client.post(
            f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/gate-actions",
            json={
                "run_id": candidate["run_id"],
                "candidate_id": candidate["candidate_id"],
                "unit_id": "unit_api",
                "unit_version": "v1",
                "selection_id": "sel_api",
                "scorer_output_ref": f"ai_trader://scorer/api/{idx}",
                "final_gate_decision_ref": f"ai_trader://final-gate/api/{idx}",
                "gate_decision": decision,
                "gate_action_payload": {
                    "action": decision,
                    "original_size": candidate["intended_size"],
                    "size_multiplier": 0.5,
                    "deterministic": True,
                    "final_gate_decision_by_llm": False,
                },
                "reason_codes": [f"api_{decision}"],
                "audit_trace_id": f"trace_api_{idx}",
                "idempotency_key": f"api-step27-3-action-{idx}",
            },
        )
        assert action.status_code == 200
        assert action.json()["data"]["accepted"] is True

    run_id = candidate_rows[0]["run_id"]
    replay = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/gated-replay",
        json={"run_id": run_id, "baseline_run_id": run_id, "execution_policy": {"missing_gate_action_policy": "review"}},
    )
    assert replay.status_code == 200
    assert replay.json()["ok"] is True
    replay_data = replay.json()["data"]
    gated_run_id = replay_data["gated_run_id"]
    assert replay_data["blocked_count"] == 1
    assert replay_data["reduced_count"] == 1
    assert replay_data["operation_id"].startswith("sbop_")

    shadow = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/gated-paper-shadow",
        json={"run_id": run_id, "baseline_run_id": run_id},
    )
    assert shadow.status_code == 200
    assert shadow.json()["data"]["status"] == "completed"

    orders = client.get(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/orders?gated_run_id={gated_run_id}"
    )
    assert orders.status_code == 200
    assert orders.json()["data"]["count"] == replay_data["candidate_count"]

    samples = client.get(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/trade-quality-samples?gated_run_id={gated_run_id}"
    )
    assert samples.status_code == 200
    assert samples.json()["data"]["count"] == replay_data["candidate_count"]

    performance = client.get(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy6/gated-performance?gated_run_id={gated_run_id}"
    )
    assert performance.status_code == 200
    assert performance.json()["data"]["gated_run_id"] == gated_run_id

    health = client.get("/api/strategy-sandbox/external-integration/health")
    assert health.status_code == 200
    assert health.json()["data"]["status"] == "ok"
    assert health.json()["data"]["external_sqlite_write_allowed"] is False

    run_lookup = client.get(f"/api/strategy-sandbox/external-integration/runs/{gated_run_id}")
    assert run_lookup.status_code == 200
    assert run_lookup.json()["data"]["count"] >= 1

    audit = client.get(
        f"/api/strategy-sandbox/external-integration/audit-events?gated_run_id={gated_run_id}&limit=20"
    )
    assert audit.status_code == 200
    assert any(row["event_type"] == "gated_replay_completed" for row in audit.json()["data"]["events"])


def test_step2321_strategy_sandbox_code_overlay_api_contract(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LAOMA_SANDBOX_ROOT", str(tmp_path / "sandboxes"))
    client = TestClient(app)

    create = client.post(
        "/api/strategy-sandbox/sandboxes",
        json={
            "strategy_line": "experiment",
            "strategy_lines": ["strategy5"],
            "strategy_version": "api-code-overlay-test",
            "data_scope": {"days": 1, "symbols": ["BTCUSDT"]},
            "config_scope": {"mode": "shadow_only"},
            "tags": ["api-code-overlay"],
        },
    )
    assert create.status_code == 200
    assert create.json()["ok"] is True
    sandbox_id = create.json()["data"]["sandbox"]["sandbox_id"]

    external_overlay = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy5/code-overlay",
        json={
            "caller_type": "external_ai_trader",
            "caller_id": "external-smoke",
            "source_surface": "external_connector",
        },
    )
    assert external_overlay.status_code == 200
    assert external_overlay.json()["ok"] is False
    assert "external_code_overlay_denied" in external_overlay.json()["error"]["message"]

    overlay = client.post(f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy5/code-overlay")
    assert overlay.status_code == 200
    assert overlay.json()["ok"] is True
    assert overlay.json()["data"]["overlay_count"] == 1

    patch = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy5/code-patches",
        json={
            "target_relpath": "notes/api_patch.md",
            "patch_type": "manifest_note",
            "note": "api sandbox-only patch",
            "diff_text": "+ api sandbox-only patch",
        },
    )
    assert patch.status_code == 200
    assert patch.json()["ok"] is True
    assert patch.json()["data"]["patch_count"] == 1

    rejected = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy5/code-patches",
        json={"target_relpath": "config/default.yaml", "note": "bad"},
    )
    assert rejected.status_code == 200
    assert rejected.json()["ok"] is False
    assert "baseline_path_forbidden" in rejected.json()["error"]["message"]

    external_build = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy5/runtime/build",
        json={
            "caller_type": "external_ai_trader",
            "caller_id": "external-smoke",
            "source_surface": "external_connector",
        },
    )
    assert external_build.status_code == 200
    assert external_build.json()["ok"] is False
    assert "external_code_overlay_denied" in external_build.json()["error"]["message"]

    built = client.post(f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy5/runtime/build", json={})
    assert built.status_code == 200
    assert built.json()["ok"] is True
    runtime_id = built.json()["data"]["runtime_id"]

    smoke = client.post(
        f"/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/strategy5/runtime/smoke",
        json={"symbols": ["BTCUSDT"]},
    )
    assert smoke.status_code == 200
    assert smoke.json()["ok"] is True
    assert smoke.json()["data"]["status"] == "smoke_passed"

    summary = client.get(f"/api/strategy-sandbox/sandboxes/{sandbox_id}/summary")
    assert summary.status_code == 200
    counts = summary.json()["data"]["summary"]["counts"]
    assert counts["sandbox_code_overlays"] == 1
    assert counts["sandbox_code_patches"] == 1
    assert counts["sandbox_evaluator_runtime"] == 1
    assert runtime_id


def test_step1259_lite_payload_contract() -> None:
    client = TestClient(app)

    checks = {
        "/api/pipeline/status-lite": 100_000,
        "/api/runtime/status-lite": 100_000,
        "/api/audit/runs-lite?limit=5": 200_000,
        "/api/audit/runs/latest-lite": 300_000,
        "/api/paper/summary-lite?limit=5": 300_000,
    }

    for path, max_bytes in checks.items():
        response = client.get(path)
        assert response.status_code == 200
        assert len(response.content) < max_bytes
        payload = response.json()
        assert payload["ok"] is True
        assert payload["data"]
        assert "X-Response-Time-ms".lower() in {key.lower() for key in response.headers.keys()}

    pipeline = client.get("/api/pipeline/status-lite").json()["data"]
    assert pipeline["payload_scope"] == "lite"
    assert "latest_report" not in pipeline
    assert "latest_report_summary" in pipeline
    assert "overall_percent" in pipeline["progress"]
    assert "lines" in pipeline["progress"]
    assert set(pipeline["progress"]["lines"]).issubset(
        {"without_micro", "micro_fast", "micro_full", "strategy5", "strategy6"},
    )


def test_step1250_trade_quality_api_contract(tmp_path: Path, monkeypatch) -> None:
    _prepare_trade_quality_db(tmp_path, monkeypatch)
    client = TestClient(app)

    summary = client.get("/api/trade-quality/summary?strategy_line=without_micro")
    samples = client.get("/api/trade-quality/samples?symbol=TQUSDT")
    recs = client.get("/api/trade-quality/recommendations")

    assert summary.status_code == 200
    summary_data = summary.json()["data"]
    assert summary_data["summary"]["sample_count"] == 1
    assert summary_data["summary"]["total_R"] > 1.9
    assert samples.status_code == 200
    sample = samples.json()["data"]["samples"][0]
    assert sample["symbol"] == "TQUSDT"
    assert sample["source_run_id"] == "run_api_tq"
    assert sample["root_cause_label"] == "tp_hit_good_trade"
    order = client.get(f"/api/trade-quality/order/{sample['order_id']}")
    assert order.status_code == 200
    assert order.json()["data"]["sample"]["order_id"] == sample["order_id"]
    assert recs.status_code == 200
    assert "recommendations" in recs.json()["data"]


def test_step189_archive_backfill_api_contract(tmp_path: Path, monkeypatch) -> None:
    _prepare_trade_quality_db(tmp_path, monkeypatch)
    archive_dir = tmp_path / "DATA" / "paper" / "archives" / "paper_exp_20260603T010000Z_without_micro"
    archive_dir.mkdir(parents=True)
    (archive_dir / "metadata.json").write_text(
        json.dumps({"schema_version": "14.31", "profile_name": "relaxed_profit"}),
        encoding="utf-8",
    )
    order = {
        "id": "api_arch_1",
        "strategy_line": "without_micro",
        "symbol": "APIAUSDT",
        "side": "LONG",
        "status": "closed",
        "entry_price": 100,
        "filled_entry_price": 100,
        "exit_price": 110,
        "stop_loss": 95,
        "take_profit": 110,
        "quantity": 1,
        "realized_pnl_usdt": 10,
        "fee_usdt": 0,
        "slippage_usdt": 0,
        "source_run_id": "run_api_archive",
        "source_cycle_id": "cycle_api_archive",
        "source_plan_hash": "plan_api_arch_1",
        "opened_at": "2026-06-03T00:00:00Z",
        "closed_at": "2026-06-03T00:05:00Z",
        "exit_reason": "TP",
    }
    (archive_dir / "orders.json").write_text(json.dumps([order]), encoding="utf-8")
    (archive_dir / "fills.json").write_text(
        json.dumps(
            [
                {"order_id": "api_arch_1", "action": "entry", "fill_price": 100},
                {"order_id": "api_arch_1", "action": "take_profit", "fill_price": 110, "net_pnl_usdt": 10},
            ]
        ),
        encoding="utf-8",
    )
    client = TestClient(app)

    dry = client.post("/api/trade-quality/archive-backfill/dry-run")
    run = client.post("/api/trade-quality/archive-backfill/run")
    archive_summary = client.get("/api/trade-quality/summary?sample_source=archive")
    live_summary = client.get("/api/trade-quality/summary?sample_source=live")
    ledger = client.get("/api/trade-quality/ingest-ledger")

    assert dry.status_code == 200
    assert dry.json()["data"]["closed_orders_seen"] == 1
    assert dry.json()["data"]["samples_inserted"] == 0
    assert run.status_code == 200
    assert run.json()["data"]["samples_inserted"] == 1
    assert archive_summary.status_code == 200
    assert archive_summary.json()["data"]["summary"]["sample_count"] == 1
    assert archive_summary.json()["data"]["samples"][0]["sample_source"] == "archive"
    assert live_summary.json()["data"]["summary"]["sample_count"] == 1
    assert ledger.json()["data"]["summary"]["sample_sources"]["archive"] == 1


def test_step1810_trade_quality_recommendation_rules_api_contract(tmp_path: Path, monkeypatch) -> None:
    _prepare_trade_quality_db(tmp_path, monkeypatch)
    archive_dir = tmp_path / "DATA" / "paper" / "archives" / "paper_exp_20260603T020000Z_without_micro"
    archive_dir.mkdir(parents=True)
    (archive_dir / "metadata.json").write_text(
        json.dumps({"schema_version": "14.31", "profile_name": "relaxed_profit"}),
        encoding="utf-8",
    )
    orders = []
    for idx in range(12):
        orders.append(
            {
                "id": f"api_rule_{idx}",
                "strategy_line": "without_micro",
                "symbol": "RULEUSDT",
                "side": "LONG",
                "status": "closed",
                "entry_price": 100,
                "filled_entry_price": 100,
                "exit_price": 95,
                "stop_loss": 95,
                "take_profit": 110,
                "quantity": 1,
                "realized_pnl_usdt": -5,
                "fee_usdt": 0,
                "slippage_usdt": 0,
                "source_run_id": f"run_api_rule_{idx}",
                "source_cycle_id": f"cycle_api_rule_{idx}",
                "source_plan_hash": f"plan_api_rule_{idx}",
                "opened_at": "2026-06-03T00:00:00Z",
                "closed_at": "2026-06-03T00:05:00Z",
                "exit_reason": "SL",
            }
        )
    (archive_dir / "orders.json").write_text(json.dumps(orders), encoding="utf-8")
    (archive_dir / "fills.json").write_text(json.dumps([]), encoding="utf-8")
    client = TestClient(app)

    ingest = client.post("/api/trade-quality/archive-backfill/run")
    rebuild = client.post("/api/trade-quality/recommendation-rules/rebuild")
    rules = client.get("/api/trade-quality/recommendation-rules?rule_type=direction_gate&sample_source=archive")

    assert ingest.status_code == 200
    assert rebuild.status_code == 200
    assert rebuild.json()["data"]["rule_count"] >= 1
    assert rules.status_code == 200
    got = rules.json()["data"]
    assert got["count"] >= 1
    assert any(row["mode"] in {"shadow", "warn"} for row in got["rules"])
    assert not any(row["mode"] in {"block_executable", "wait_only"} for row in got["rules"])


def test_step1811_1812_trade_quality_validation_and_promotion_api_contract(tmp_path: Path, monkeypatch) -> None:
    _prepare_trade_quality_db(tmp_path, monkeypatch)
    archive_dir = tmp_path / "DATA" / "paper" / "archives" / "paper_exp_20260603T030000Z_without_micro"
    archive_dir.mkdir(parents=True)
    (archive_dir / "metadata.json").write_text(
        json.dumps({"schema_version": "14.31", "profile_name": "relaxed_profit"}),
        encoding="utf-8",
    )
    orders = []
    for idx in range(5):
        orders.append(
            {
                "id": f"api_promo_{idx}",
                "strategy_line": "without_micro",
                "symbol": "PROMOUSDT",
                "side": "LONG",
                "status": "closed",
                "entry_price": 100,
                "filled_entry_price": 100,
                "exit_price": 95,
                "stop_loss": 95,
                "take_profit": 110,
                "quantity": 1,
                "realized_pnl_usdt": -5,
                "fee_usdt": 5,
                "slippage_usdt": 2,
                "source_run_id": f"run_api_promo_{idx}",
                "source_cycle_id": f"cycle_api_promo_{idx}",
                "source_plan_hash": f"plan_api_promo_{idx}",
                "opened_at": "2026-06-03T00:00:00Z",
                "closed_at": "2026-06-03T00:05:00Z",
                "exit_reason": "SL",
            }
        )
    (archive_dir / "orders.json").write_text(json.dumps(orders), encoding="utf-8")
    (archive_dir / "fills.json").write_text(json.dumps([]), encoding="utf-8")
    client = TestClient(app)

    client.post("/api/trade-quality/archive-backfill/run")
    client.post("/api/trade-quality/recommendation-rules/rebuild")
    rule_payload = client.get("/api/trade-quality/recommendation-rules?rule_type=cost_liquidity&symbol=PROMOUSDT&limit=10")
    rule_id = rule_payload.json()["data"]["rules"][0]["rule_id"]
    validation = client.get("/api/trade-quality/recommendation-validation?sample_source=all&symbol=PROMOUSDT&limit=20")
    dry = client.post(
        "/api/trade-quality/recommendation-promotions/dry-run",
        json={"rule_id": rule_id, "profile": "relaxed_profit", "strategy_line": "without_micro", "mode": "wait_only"},
    )
    applied = client.post(
        "/api/trade-quality/recommendation-promotions/apply",
        json={
            "rule_id": rule_id,
            "profile": "relaxed_profit",
            "strategy_line": "without_micro",
            "mode": "wait_only",
            "reason": "api_test",
        },
    )
    promotion_id = applied.json()["data"]["promotion_id"]
    promotions = client.get("/api/trade-quality/recommendation-promotions")
    disabled = client.post(
        "/api/trade-quality/recommendation-promotions/disable",
        json={"promotion_id": promotion_id, "reason": "api_test_disable"},
    )

    assert validation.status_code == 200
    assert "summary" in validation.json()["data"]
    assert dry.status_code == 200
    assert dry.json()["data"]["would_write"] is False
    assert applied.status_code == 200
    assert promotions.status_code == 200
    assert promotions.json()["data"]["summary"]["enabled"] >= 1
    assert disabled.status_code == 200


def test_step1236_candidate_pool_governance_api_contract(tmp_path: Path, monkeypatch) -> None:
    universe_p = tmp_path / "CANDIDATE_UNIVERSE.json"
    light_p = tmp_path / "futures_light_snapshot.json"
    universe_p.write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": "2026-06-01T00:00:00Z",
                "expires_at": "2026-06-02T00:00:00Z",
                "count": 1,
                "counts": {
                    "total_pairs": 1,
                    "futures_count": 1,
                    "spot_count": 1,
                    "both_spot_and_futures": 1,
                    "futures_only": 0,
                    "spot_only": 0,
                    "neither_spot_nor_futures": 0,
                },
                "pairs": [
                    {
                        "base_asset": "BTC",
                        "display_base_asset": "BTC",
                        "cashtag": "$BTC",
                        "spot_cashtag_symbol": "BTCUSDT",
                        "futures_symbol": "BTCUSDT",
                        "has_spot": True,
                        "has_um_futures": True,
                        "eligible_for_signal_engine": True,
                        "eligible_for_post": True,
                        "eligible_for_trade_analysis": True,
                        "universe_profile": {"business_pool": "liquid_major", "scan_eligibility": "scan"},
                        "risk_profile": {"execution_tier": "market_ok", "sizing_template": "normal"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    light_p.write_text(
        json.dumps(
            {
                "schema_version": "1.6",
                "generated_at": "2026-06-01T00:01:00Z",
                "items": [
                    {
                        "symbol": "BTCUSDT",
                        "base_asset": "BTC",
                        "tradability_profile": {
                            "trade_quality_tier": "market_entry_fit",
                            "market_entry_score": 80,
                            "scan_priority": 90,
                        },
                        "primary_pool": "core_liquid_pool",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setitem(api_services.CURRENT_JSON_PATHS, "candidate_universe", universe_p)
    monkeypatch.setitem(api_services.CURRENT_JSON_PATHS, "futures_light_snapshot", light_p)
    client = TestClient(app)

    response = client.get("/api/governance/candidate-pool?limit=10")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    data = payload["data"]
    assert data["count"] == 1
    assert data["counts"]["business_pool"]["liquid_major"] == 1
    assert data["counts"]["profile_hydration_status"]["ok"] == 1
    assert data["items"][0]["symbol"] == "BTCUSDT"
    assert data["items"][0]["profile_hydration"]["status"] == "ok"
    assert data["items"][0]["tradability_profile"]["market_entry_score"] == 80


def test_step12_config_masks_feishu_secret(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.put(
        "/api/notifications/feishu/config",
        json={"values": {"webhook_url": "https://example.com/abcdef", "webhook_secret": "super-secret-value"}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["webhook_url"] != "https://example.com/abcdef"
    assert payload["data"]["webhook_secret"] != "super-secret-value"


def test_step12_rejects_non_whitelisted_config_key(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.put("/api/config/paper", json={"values": {"evil": True}})

    assert response.status_code == 400
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "config_invalid"


def test_step12_17_trade_plan_nested_config_update_preserves_line(tmp_path: Path, monkeypatch) -> None:
    config_path = _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.put(
        "/api/config/trade-plan-lines",
        json={"values": {"micro_fast": {"min_score": 31, "require_micro_ready": False}}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    got = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert got["trade_plan_lines"]["micro_fast"]["min_score"] == 31
    assert got["trade_plan_lines"]["micro_fast"]["target_rr"] > 0
    assert got["active_profile"] == "custom"


def test_step12_58_trade_plan_tp_target_policy_update(tmp_path: Path, monkeypatch) -> None:
    config_path = _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.put(
        "/api/config/trade-plan-lines",
        json={
            "values": {
                "micro_fast": {
                    "tp_target_policy": {
                        "mode": "fast_capped_rr",
                        "target_rr": 0.7,
                        "target_rr_cap": 0.8,
                        "min_reward_bps": 8,
                        "require_market_room": True,
                        "market_room_buffer_bps": 2,
                        "allow_structure_runner": False,
                        "reward_to_spread_min": 2.5,
                    },
                },
            },
        },
    )

    assert response.status_code == 200
    got = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    policy = got["trade_plan_lines"]["micro_fast"]["tp_target_policy"]
    assert policy["mode"] == "fast_capped_rr"
    assert policy["target_rr"] == 0.7
    assert got["active_profile"] == "custom"


def test_step12_64_trade_plan_tp_target_policy_net_r_fields_update(tmp_path: Path, monkeypatch) -> None:
    config_path = _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.put(
        "/api/config/trade-plan-lines",
        json={
            "values": {
                "micro_fast": {
                    "tp_target_policy": {
                        "mode": "fast_capped_rr",
                        "target_rr": 0.7,
                        "target_rr_cap": 1.05,
                        "target_rr_basis": "net",
                        "target_net_rr": 1.0,
                        "min_target_net_rr": 0.5,
                        "max_target_net_rr": 1.5,
                        "min_reward_bps": 8,
                        "require_market_room": True,
                        "market_room_buffer_bps": 2,
                        "allow_structure_runner": False,
                        "reward_to_spread_min": 2.5,
                        "include_entry_fee": True,
                        "include_exit_fee": True,
                        "include_slippage_reserve": True,
                        "slippage_reserve_bps": 2,
                        "max_loss_net_r": 1.1,
                        "sizing_basis": "net_planned_loss",
                    },
                },
            },
        },
    )

    assert response.status_code == 200
    got = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    policy = got["trade_plan_lines"]["micro_fast"]["tp_target_policy"]
    assert policy["target_rr_basis"] == "net"
    assert policy["target_net_rr"] == 1.0
    assert policy["sizing_basis"] == "net_planned_loss"
    assert got["active_profile"] == "custom"


def test_step12_58_rejects_tp_target_policy_cap_below_target(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.put(
        "/api/config/trade-plan-lines",
        json={
            "values": {
                "micro_fast": {
                    "tp_target_policy": {
                        "mode": "fast_capped_rr",
                        "target_rr": 0.8,
                        "target_rr_cap": 0.7,
                    },
                },
            },
        },
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "config_invalid"


def test_step12_17_trade_plan_short_now_calibration_update(tmp_path: Path, monkeypatch) -> None:
    config_path = _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.put(
        "/api/config/trade-plan-lines",
        json={
            "values": {
                "without_micro": {
                    "short_now_calibration": {
                        "enabled": True,
                        "min_range_pos": 0.2,
                        "max_range_pos": 0.8,
                        "min_available_room_bps": 50,
                        "max_stop_bps": 420,
                        "max_stop_atr_mult": 4.0,
                        "min_net_rr": 0.75,
                        "allow_if_liquidity_missing": False,
                        "max_spread_bps": 80,
                        "max_slippage_bps": 150,
                        "require_recent_down_impulse": True,
                        "reject_if_rebound_required": True,
                    },
                },
            },
        },
    )

    assert response.status_code == 200
    got = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    short_now = got["trade_plan_lines"]["without_micro"]["short_now_calibration"]
    assert short_now["enabled"] is True
    assert short_now["min_range_pos"] == 0.2
    assert got["active_profile"] == "custom"


def test_step12_37_trade_plan_market_now_calibration_update(tmp_path: Path, monkeypatch) -> None:
    config_path = _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.put(
        "/api/config/trade-plan-lines",
        json={
            "values": {
                "without_micro": {
                    "market_now_calibration": {
                        "enabled": True,
                        "legacy_short_now_fallback": True,
                        "long": {
                            "min_range_pos": 0.18,
                            "max_range_pos": 0.82,
                            "min_available_room_bps": 45,
                            "max_stop_bps": 420,
                            "max_stop_atr_mult": 4.0,
                            "min_net_rr": 0.75,
                            "allow_if_liquidity_missing": False,
                            "max_spread_bps": 80,
                            "max_slippage_bps": 150,
                            "require_recent_up_impulse": True,
                            "reject_if_pullback_required": True,
                        },
                        "short": {
                            "min_range_pos": 0.18,
                            "max_range_pos": 0.82,
                            "min_available_room_bps": 45,
                            "max_stop_bps": 420,
                            "max_stop_atr_mult": 4.0,
                            "min_net_rr": 0.75,
                            "allow_if_liquidity_missing": False,
                            "max_spread_bps": 80,
                            "max_slippage_bps": 150,
                            "require_recent_down_impulse": True,
                            "reject_if_rebound_required": True,
                        },
                    },
                },
            },
        },
    )

    assert response.status_code == 200
    got = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    market_now = got["trade_plan_lines"]["without_micro"]["market_now_calibration"]
    assert market_now["enabled"] is True
    assert market_now["long"]["max_range_pos"] == 0.82
    assert market_now["short"]["min_range_pos"] == 0.18
    assert got["active_profile"] == "custom"


def test_step1251_trade_quality_gate_sltp_config_contract(tmp_path: Path, monkeypatch) -> None:
    config_path = _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.put(
        "/api/config/trade-plan-lines",
        json={
            "values": {
                "without_micro": {
                    "trade_quality_gate": {
                        "enabled": True,
                        "mode": "wait_only",
                        "min_samples_per_symbol": 2,
                        "min_samples_per_root_cause": 4,
                        "max_negative_expectancy_R": -0.5,
                        "signal_no_edge_wait_enabled": True,
                        "side_specific_enabled": True,
                    },
                    "sl_tp_quality": {
                        "enabled": True,
                        "mode": "apply",
                        "single_tp_only": True,
                        "min_samples_per_cluster": 3,
                        "stop_too_tight_widen_factor": 1.2,
                        "tp_too_far_reduce_factor": 0.85,
                        "entered_too_early_wait_enabled": True,
                    },
                },
            },
        },
    )

    assert response.status_code == 200
    got = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    line = got["trade_plan_lines"]["without_micro"]
    assert line["trade_quality_gate"]["mode"] == "wait_only"
    assert line["sl_tp_quality"]["mode"] == "apply"
    invalid = client.post(
        "/api/config/trade-plan-lines/validate",
        json={"values": {"without_micro": {"trade_quality_gate": {"enabled": True, "mode": "YOLO"}}}},
    )
    assert invalid.status_code == 400


def test_step12_17_rejects_unknown_nested_config_key(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.put(
        "/api/config/trade-plan-lines",
        json={"values": {"micro_fast": {"unknown_gate": True}}},
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "config_invalid"


def test_step12_17_validate_and_apply_profile_contract(tmp_path: Path, monkeypatch) -> None:
    config_path = _use_temp_config(tmp_path, monkeypatch)
    client = TestClient(app)

    valid = client.post(
        "/api/config/trade-plan-lines/validate",
        json={
            "values": {
                "micro_fast": {
                    "min_score": 33,
                    "micro_consumption_policy": "ready_signal_usable",
                    "allow_weak_micro_consumption": True,
                    "weak_micro_min_state": "ready",
                    "weak_micro_block_reasons": ["micro_direction_conflict"],
                },
            },
        },
    )
    invalid_policy = client.post(
        "/api/config/trade-plan-lines/validate",
        json={"values": {"micro_fast": {"micro_consumption_policy": "anything_goes"}}},
    )
    invalid = client.post(
        "/api/config/trade-plan-lines/validate",
        json={"values": {"micro_fast": {"min_score": 120}}},
    )
    profiles = client.get("/api/config/profiles")
    applied = client.post("/api/config/profiles/relaxed_test/apply")

    assert valid.status_code == 200
    assert valid.json()["data"]["valid"] is True
    assert invalid.status_code == 400
    assert invalid_policy.status_code == 400
    assert profiles.status_code == 200
    assert "relaxed_test" in {row["name"] for row in profiles.json()["data"]["profiles"]}
    assert applied.status_code == 200
    got = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert got["active_profile"] == "relaxed_test"
    assert got["trade_plan_lines"]["micro_fast"]["require_micro_ready"] is True
    assert got["trade_plan_lines"]["micro_fast"]["require_micro_alignment"] is True
    assert got["trade_plan_lines"]["micro_fast"]["micro_consumption_policy"] == "ready_signal_usable"
    assert got["trade_plan_lines"]["micro_fast"]["allow_weak_micro_consumption"] is True
    assert got["trade_plan_lines"]["micro_fast"]["profile_gate_enabled"] is True
    assert got["trade_plan_lines"]["micro_fast"]["min_profile_market_entry_score"] == 20
    assert got["trade_plan_lines"]["micro_fast"]["max_profile_slippage_risk_score"] == 90
    assert got["trade_plan_risk"]["planned_loss_guard_enabled"] is True
    assert got["trade_plan_risk"]["sizing_policy"] == "notional_by_loss_cap"
    assert got["trade_plan_risk"]["target_planned_loss_usdt"] == 50
    assert got["trade_plan_risk"]["max_planned_loss_usdt"] == 80
    assert got["paper"]["paper_fallback_notional_allowed"] is False


def test_step12_trade_plan_contract_reads_strategy_lines() -> None:
    client = TestClient(app)

    response = client.get("/api/decisions/trade-plans")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert set(payload["data"]["lines"]) == {
        "without_micro",
        "micro_fast",
        "micro_full",
        "strategy4",
        "strategy5",
        "strategy6",
    }


def test_step1248_trade_plan_funnel_api_contract(tmp_path: Path, monkeypatch) -> None:
    run_id = "run_funnel"
    cycle_id = "cycle_funnel"
    report_path = tmp_path / "DATA" / "reports" / "pipeline_runs" / run_id / "strategy_pipeline_report.json"
    plan_path = tmp_path / "DATA" / "decisions" / "trade_plan_runs" / run_id / "latest_trade_plan_without_micro.json"
    latest_strategy = tmp_path / "DATA" / "reports" / "latest_strategy_pipeline_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    latest_strategy.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "run_id": run_id,
        "cycle_id": cycle_id,
        "status": "completed",
        "selected_lines": ["without_micro"],
        "skipped_lines": ["micro_fast", "micro_full"],
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")
    latest_strategy.write_text(json.dumps(report), encoding="utf-8")
    plan_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "cycle_id": cycle_id,
                "line": "without_micro",
                "generated_at": "2026-06-03T00:00:00Z",
                "status": "ok",
                "plans": [
                    {
                        "symbol": "AAAUSDT",
                        "decision": "LONG",
                        "action": "ENTER_MARKET",
                        "entry_mode": "MARKET",
                        "estimated_entry_price": 10.0,
                        "stop_loss": 9.8,
                        "take_profit": 10.4,
                        "rr": 2.0,
                        "executable": True,
                        "reason_codes": [],
                        "input_refs": {"source_plan_hash": "hash_aaa"},
                        "guards": {"market_now_calibration": {"ok": True}, "effective_rr": 2.0},
                        "position_sizing": {"planned_notional_usdt": 100.0, "planned_loss_usdt": 2.0},
                    },
                    {
                        "symbol": "BBBUSDT",
                        "decision": "SHORT",
                        "action": "WAIT",
                        "entry_mode": "WAIT_REBOUND",
                        "executable": False,
                        "reason_codes": ["short_now_stop_too_wide"],
                        "input_refs": {"source_plan_hash": "hash_bbb"},
                        "guards": {"market_now_calibration": {"ok": False}},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    paths = dict(api_services.CURRENT_JSON_PATHS)
    paths["latest_strategy"] = latest_strategy
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(api_services, "CURRENT_JSON_PATHS", paths)
    monkeypatch.setattr(
        api_services,
        "_paper_rows_by_run",
        lambda _run_id: {
            "paper_intent_inbox": [],
            "paper_trade_plans": [],
            "paper_orders": [
                {
                    "strategy_line": "without_micro",
                    "symbol": "AAAUSDT",
                    "source_plan_hash": "hash_aaa",
                    "id": "paper_order_1",
                    "planned_notional_usdt": 100.0,
                }
            ],
            "paper_skip_ledger": [],
            "paper_positions": [],
        },
    )
    client = TestClient(app)

    response = client.get(f"/api/decisions/trade-plans/funnel?run_id={run_id}&symbol_limit=10")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    data = payload["data"]
    assert data["run_id"] == run_id
    assert data["counts"]["total_plans"] == 2
    assert data["counts"]["executable"] == 1
    line = data["strategy_lines"][0]
    assert line["line"] == "without_micro"
    assert line["counts"]["paper_orders"] == 1
    assert line["reason_groups"][0]["reason"] == "short_now_stop_too_wide"
    assert line["symbols"][0]["paper"]["paper_status"] == "consumed"


def test_step1047_trade_plan_payload_hides_stale_executable_output(tmp_path: Path, monkeypatch) -> None:
    paths = {
        "trade_plan_without_micro": tmp_path / "DATA/decisions/latest_trade_plan_without_micro.json",
        "trade_plan_micro_fast": tmp_path / "DATA/decisions/latest_trade_plan_micro_fast.json",
        "trade_plan_micro_full": tmp_path / "DATA/decisions/latest_trade_plan_micro_full.json",
        "latest_decisions": tmp_path / "DATA/decisions/latest_decisions.json",
        "latest_strategy": tmp_path / "DATA/reports/latest_strategy_pipeline_report.json",
        "latest_audit": tmp_path / "DATA/reports/latest_current_json_chain_audit_summary.json",
        "abc": tmp_path / "DATA/reports/latest_trade_plan_lines_compare.json",
    }
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
    paths["latest_strategy"].write_text(
        json.dumps({"run_id": "run_current", "cycle_id": "cycle_current"}),
        encoding="utf-8",
    )
    paths["trade_plan_micro_fast"].write_text(
        json.dumps(
            {
                "schema_version": "10.47",
                "run_id": "run_old",
                "cycle_id": "cycle_old",
                "line": "micro_fast",
                "status": "ok",
                "count": 2,
                "executable_count": 2,
                "plans": [
                    {"symbol": "OLD1USDT", "executable": True},
                    {"symbol": "OLD2USDT", "executable": True},
                ],
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "CURRENT_JSON_PATHS", paths)

    payload = api_services.trade_plans_payload()
    row = payload["lines"]["micro_fast"]

    assert payload["display_run_id"] == "run_current"
    assert row["output_fresh"] is False
    assert row["stale_output_reason"] == "output_run_id_mismatch"
    assert row["effective_executable_count"] == 0
    assert row["plans_for_current_run"] == []


def test_step12_paper_and_feishu_mock_contracts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    client = TestClient(app)

    paper = client.get("/api/paper/summary").json()
    assert paper["ok"] is True
    assert "exists" in paper["data"]

    feishu = client.post("/api/notifications/feishu/test", json={"message": "hello", "mock": True}).json()
    assert feishu["ok"] is True
    assert feishu["data"]["status"] == "mock_sent"


def test_step12_9_feishu_delivery_filters_and_latest(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    path = tmp_path / "DATA" / "notifications" / "delivery_history.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "15.6",
                "generated_at": "2026-05-26T00:00:00Z",
                "deliveries": [
                    {
                        "event_type": "trade_plan_executable",
                        "strategy_line": "without_micro",
                        "status": "success",
                        "dedup_key": "a",
                    },
                    {
                        "event_type": "trade_plan_executable",
                        "strategy_line": "micro_fast",
                        "status": "failed",
                        "dedup_key": "b",
                    },
                    {
                        "event_type": "paper_order",
                        "strategy_line": "micro_fast",
                        "status": "success",
                        "dedup_key": "c",
                    },
                ],
            },
        ),
        encoding="utf-8",
    )
    client = TestClient(app)

    filtered = client.get(
        "/api/notifications/deliveries",
        params={"type": "trade_plan_executable", "status": "failed", "line": "micro_fast"},
    ).json()
    latest = client.get("/api/notifications/deliveries/latest").json()

    assert filtered["ok"] is True
    assert [row["dedup_key"] for row in filtered["data"]["deliveries"]] == ["b"]
    assert latest["ok"] is True
    assert latest["data"]["delivery"]["dedup_key"] == "c"


def test_step12_9_feishu_send_trade_plans_mock_api(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    client = TestClient(app)

    response = client.post(
        "/api/notifications/feishu/send-trade-plans",
        json={"mock_signals": True, "mock_send": True},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["mock_signals"] is True
    assert payload["data"]["mock_send"] is True
    assert payload["data"]["selected"]["without_micro"] == 1
    assert payload["data"]["selected"]["micro_fast"] == 1
    assert payload["data"]["selected"]["micro_full"] == 1
    assert payload["data"]["selected"].get("strategy4", 0) == 0
    assert {row["status"] for row in payload["data"]["deliveries"]} <= {"mock_sent", "duplicate"}


def test_step12_20_run_level_audit_api_sqlite_fallback(tmp_path: Path, monkeypatch) -> None:
    from laoma_signal_engine.audit.run_audit import init_run_audit_db

    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    db = tmp_path / "DATA/audit/run_audit.db"
    init_run_audit_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            insert into audit_runs(run_id, cycle_id, status, generated_at, failure_count, warning_count, summary_json, payload_json)
            values(?,?,?,?,?,?,?,?)
            """,
            (
                "run_api_1",
                "cycle_api_1",
                "ok",
                "2026-05-28T00:00:00Z",
                0,
                0,
                json.dumps({"executable_count": {"micro_fast": 1}}),
                json.dumps({"schema_version": "7.14", "run_id": "run_api_1", "cycle_id": "cycle_api_1", "status": "ok"}),
            ),
        )
    client = TestClient(app)

    latest = client.get("/api/audit/runs/latest")
    by_id = client.get("/api/audit/runs/run_api_1")
    listed = client.get("/api/audit/runs")

    assert latest.status_code == 200
    assert latest.json()["data"]["run_id"] == "run_api_1"
    assert by_id.status_code == 200
    assert by_id.json()["data"]["cycle_id"] == "cycle_api_1"
    assert listed.status_code == 200
    assert listed.json()["data"]["runs"][0]["run_id"] == "run_api_1"


def test_step12_10_pipeline_run_starts_background_job(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    _mock_snapshot_warmup_ready(monkeypatch)
    calls: list[dict[str, object]] = []

    def fake_run_cli(args: list[str], *, background: bool = False) -> dict[str, object]:
        calls.append({"args": args, "background": background})
        return {"status": "started", "pid": 1234, "log_path": str(tmp_path / "pipeline.log"), "command": args}

    monkeypatch.setattr(api_services, "run_cli", fake_run_cli)
    client = TestClient(app)

    response = client.post("/api/pipeline/run", json={"line": "all", "mode": "once"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["status"] == "started"
    assert calls[0]["background"] is True
    assert "--mode" in calls[0]["args"]
    assert "--lines" in calls[0]["args"]


def test_step12_45_pipeline_run_accepts_selected_lines_and_fixed_cooldown(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    _mock_snapshot_warmup_ready(monkeypatch)
    calls: list[dict[str, object]] = []

    def fake_run_cli(args: list[str], *, background: bool = False) -> dict[str, object]:
        calls.append({"args": args, "background": background})
        return {"status": "started", "pid": 2345, "log_path": str(tmp_path / "pipeline.log"), "command": args}

    monkeypatch.setattr(api_services, "run_cli", fake_run_cli)
    client = TestClient(app)

    response = client.post(
        "/api/pipeline/run",
        json={"lines": ["micro_full", "without_micro", "micro_full"], "mode": "interval", "interval_sec": 300},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["selected_lines"] == ["without_micro", "micro_full"]
    assert data["requested_interval_sec"] == 300
    assert data["effective_interval_sec"] == 300
    assert data["post_run_cooldown_sec"] == 300
    assert data["interval_semantics"] == "post_run_cooldown"
    assert data["line_runtime_budgets"]["micro_full"] >= 1200
    args = calls[0]["args"]
    assert args[args.index("--lines") + 1] == "without_micro,micro_full"
    assert args[args.index("--interval-sec") + 1] == "300"


def test_step16_8_pipeline_run_blocks_when_snapshot_warmup_not_ready(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        api_services,
        "snapshot_warmup_payload",
        lambda *args, **kwargs: {
            "schema_version": "STEP16.8_snapshot_warmup_v1",
            "status": "warming",
            "ready": False,
            "allow_run_once": False,
            "allow_run_cycle": False,
            "usable_symbol_count": 0,
            "fresh_count": 0,
            "stale_usable_count": 0,
            "stale_blocked_count": 0,
            "min_usable_symbol_count": 3,
            "disabled_reason": "snapshot_warmup_not_ready",
            "reason_codes": ["snapshot_usable_symbols_below_min"],
        },
    )
    client = TestClient(app)

    status_response = client.get("/api/pipeline/status/latest")
    run_response = client.post("/api/pipeline/run", json={"line": "all", "mode": "once"})

    assert status_response.status_code == 200
    controls = status_response.json()["data"]["run_controls"]
    assert controls["can_run_once"] is False
    assert controls["disabled_reason"] == "snapshot_warmup_not_ready"
    assert run_response.status_code == 400
    assert run_response.json()["error"]["code"] == "snapshot_warmup_not_ready"


def test_step12_10_pipeline_status_marks_stale_lock(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    lock = tmp_path / "DATA" / "runtime" / "strategy_pipeline.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "lock_owner_pid": 999999,
                "run_id": "run_x",
                "cycle_id": "cycle_x",
                "started_at": "2026-05-26T00:00:00Z",
                "expires_at": "2099-01-01T00:00:00Z",
                "stage": "acquire",
            },
        ),
        encoding="utf-8",
    )
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["lock"]["run_id"] == "run_x"
    assert payload["data"]["lock_stale"] is True
    assert payload["data"]["progress"]["overall_percent"] >= 0


def test_step12_16_pipeline_status_alias_matches_latest(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "strategy_pipeline_progress.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "run_id": "run_alias",
                "cycle_id": "cycle_alias",
                "mode": "once",
                "status": "completed",
                "overall_percent": 100,
                "lines": {},
                "updated_at": "2026-05-27T00:00:00Z",
            },
        ),
        encoding="utf-8",
    )
    client = TestClient(app)

    latest = client.get("/api/pipeline/status/latest")
    compat = client.get("/api/pipeline/status")

    assert latest.status_code == 200
    assert compat.status_code == 200
    assert latest.json()["data"]["progress"]["run_id"] == "run_alias"
    assert compat.json()["data"]["progress"]["run_id"] == "run_alias"


def test_step12_21_pipeline_status_reconciles_terminal_lifecycle(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    micro = tmp_path / "DATA" / "micro"
    runtime.mkdir(parents=True, exist_ok=True)
    micro.mkdir(parents=True, exist_ok=True)
    (runtime / "strategy_pipeline_progress.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "run_id": "run_reconcile",
                "cycle_id": "cycle_reconcile",
                "mode": "once",
                "status": "ok",
                "overall_percent": 100,
                "lines": {
                    "micro_fast": {
                        "percent": 100,
                        "stage": "completed_with_unfinished_symbols",
                        "done": True,
                        "run_id": "run_reconcile",
                        "cycle_id": "cycle_reconcile",
                        "line_lifecycle_status": "partial_ready",
                        "unfinished_symbol_count": 2,
                    },
                },
                "updated_at": "2026-05-28T00:00:00Z",
            },
        ),
        encoding="utf-8",
    )
    (micro / "latest_micro_lifecycle_micro_fast.json").write_text(
        json.dumps(
            {
                "schema_version": "10.35",
                "run_id": "run_reconcile",
                "cycle_id": "cycle_reconcile",
                "strategy_line": "micro_fast",
                "count": 2,
                "items": [
                    {
                        "symbol": "AAAUSDT",
                        "state": "rejected",
                        "terminal": True,
                        "ready": True,
                        "confirmed": False,
                        "trade_plan_consumable": False,
                    },
                    {
                        "symbol": "BBBUSDT",
                        "state": "not_ready",
                        "terminal": True,
                        "ready": False,
                        "confirmed": False,
                        "trade_plan_consumable": False,
                    },
                ],
            },
        ),
        encoding="utf-8",
    )
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    row = response.json()["data"]["progress"]["lines"]["micro_fast"]
    assert row["stage"] == "completed_terminalized"
    assert row["line_lifecycle_status"] == "terminalized_no_consumable"
    assert row["stage_status_class"] == "business_no_signal"
    assert row["business_terminal_reason"] == "no_confirmed"
    assert row["unfinished_symbol_count"] == 0
    assert row["terminalized_symbol_count"] == 2
    assert row["rejected_count"] == 1
    assert row["not_ready_count"] == 1


def test_step16_5_pipeline_status_reconciles_fresh_blocked_trade_plan_terminal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    reports = tmp_path / "DATA" / "reports"
    decisions = tmp_path / "DATA" / "decisions"
    runtime.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    decisions.mkdir(parents=True, exist_ok=True)
    monkeypatch.setitem(
        api_services.CURRENT_JSON_PATHS,
        "latest_strategy",
        reports / "latest_strategy_pipeline_report.json",
    )
    monkeypatch.setitem(
        api_services.CURRENT_JSON_PATHS,
        "trade_plan_micro_fast",
        decisions / "latest_trade_plan_micro_fast.json",
    )
    report = {
        "schema_version": "1.0",
        "run_id": "run_blocked",
        "cycle_id": "cycle_blocked",
        "mode": "once",
        "status": "ok",
        "stages": [
            {"name": "wait_micro_ready_micro_fast", "ok": True, "rc": 0},
            {"name": "blocked_micro_fast_no_consumable_symbol", "ok": True, "rc": 0},
        ],
    }
    (reports / "latest_strategy_pipeline_report.json").write_text(json.dumps(report), encoding="utf-8")
    (runtime / "strategy_pipeline_progress.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "run_id": "run_blocked",
                "cycle_id": "cycle_blocked",
                "mode": "once",
                "status": "ok",
                "overall_percent": 68,
                "lines": {
                    "without_micro": {
                        "percent": 100,
                        "stage": "completed",
                        "done": True,
                        "selected": True,
                    },
                    "micro_fast": {
                        "percent": 35,
                        "stage": "wait_micro_ready_micro_fast",
                        "done": False,
                        "selected": True,
                        "line_exec_status": "no_confirmed",
                        "line_lifecycle_status": "quality_ready_no_consumable",
                        "trade_plan_allowed": False,
                    },
                    "micro_full": {
                        "percent": 100,
                        "stage": "skipped_not_selected",
                        "done": True,
                        "selected": False,
                        "skipped": True,
                    },
                },
                "selected_lines": ["without_micro", "micro_fast"],
            },
        ),
        encoding="utf-8",
    )
    (decisions / "latest_trade_plan_micro_fast.json").write_text(
        json.dumps(
            {
                "schema_version": "10.47",
                "run_id": "run_blocked",
                "cycle_id": "cycle_blocked",
                "source": "trade_plan_micro_fast",
                "status": "blocked",
                "count": 0,
                "executable_count": 0,
                "plans": [],
                "input_refs": {
                    "blocked_reason": "micro_fast_quality_ready_but_no_confirmed_symbol",
                    "reason_codes": [
                        "micro_fast_quality_ready_but_no_confirmed_symbol",
                        "micro_fast_no_consumable_symbol",
                    ],
                },
            },
        ),
        encoding="utf-8",
    )
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    progress = response.json()["data"]["progress"]
    row = progress["lines"]["micro_fast"]
    assert row["done"] is True
    assert row["percent"] == 100
    assert row["stage"] == "blocked_micro_fast_no_consumable_symbol"
    assert row["terminal_state"] == "blocked"
    assert row["terminal_reason"] == "micro_fast_quality_ready_but_no_confirmed_symbol"
    assert row["trade_plan_allowed"] is False
    assert row["effective_executable_count"] == 0
    assert progress["overall_percent"] == 100


def test_step12_38_rest_health_and_step15_snapshot_quality_api(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    market = tmp_path / "DATA" / "market"
    market.mkdir(parents=True, exist_ok=True)
    light_p = market / "futures_light_snapshot.json"
    light_p.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "generated_at": "2026-06-02T00:00:00Z",
                "success_count": 2,
                "failed_count": 1,
                "snapshot_quality": {
                    "snapshot_status": "degraded_cache",
                    "snapshot_success_count": 2,
                    "snapshot_failed_count": 1,
                    "snapshot_failed_symbols": ["BADUSDT"],
                    "downstream_candidate_count": 2,
                    "exchange_info_source": "cache",
                    "exchange_info_live_error": "http_418",
                    "rest_circuit_state": "open",
                    "reason_codes": ["exchange_info_cache_used", "exchange_info_live_418"],
                },
                "items": [],
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setitem(api_services.CURRENT_JSON_PATHS, "futures_light_snapshot", light_p)
    client = TestClient(app)

    quality = client.get("/api/audit/step15/snapshot-quality/latest")
    assert quality.status_code == 200
    qdata = quality.json()["data"]
    assert qdata["snapshot_status"] == "degraded_cache"
    assert qdata["exchange_info_source"] == "cache"
    assert qdata["candidate_allowed_count"] == 2

    health = client.get("/api/runtime/rest-health")
    assert health.status_code == 200
    hdata = health.json()["data"]
    assert hdata["exchange_info"]["policy"] == "cache_first"
    assert hdata["latest_snapshot"]["snapshot_status"] == "degraded_cache"


def test_step12_12_running_progress_ignores_stale_latest_report(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    reports = tmp_path / "DATA" / "reports"
    runtime.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    (runtime / "api_pipeline_interval.pid").write_text(
        json.dumps({"pid": 123456, "run_id": "run_current", "cycle_id": "cycle_current", "mode": "once"}),
        encoding="utf-8",
    )
    (runtime / "strategy_pipeline.lock").write_text(
        json.dumps({"lock_owner_pid": 123456, "run_id": "run_current", "cycle_id": "cycle_current", "stage": "acquire"}),
        encoding="utf-8",
    )
    (reports / "latest_strategy_pipeline_report.json").write_text(
        json.dumps(
            {
                "run_id": "run_old",
                "cycle_id": "cycle_old",
                "status": "ok",
                "stages": [
                    {"name": "apply_trade_plan_without_micro", "ok": True},
                    {"name": "apply_trade_plan_micro_fast", "ok": True},
                    {"name": "apply_trade_plan_micro_full", "ok": True},
                ],
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: int(pid or 0) == 123456)
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    progress = response.json()["data"]["progress"]
    assert progress["status"] == "running"
    assert progress["run_id"] == "run_current"
    assert progress["lines"]["without_micro"]["done"] is False
    assert progress["lines"]["micro_fast"]["percent"] == 0
    assert progress["lines"]["micro_full"]["percent"] == 0


def test_step12_12_pid_reuse_does_not_keep_finished_pipeline_running(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    reports = tmp_path / "DATA" / "reports"
    runtime.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    (runtime / "api_pipeline_interval.pid").write_text(
        json.dumps({"pid": 123456, "started_at": "2026-05-26T00:00:00Z", "mode": "once"}),
        encoding="utf-8",
    )
    (runtime / "strategy_pipeline_progress.json").write_text(
        json.dumps({"run_id": "run_done", "cycle_id": "cycle_done", "status": "running", "overall_percent": 82, "lines": {}}),
        encoding="utf-8",
    )
    (reports / "latest_strategy_pipeline_report.json").write_text(
        json.dumps({"run_id": "run_done", "cycle_id": "cycle_done", "status": "failed", "finished_at": "2026-05-26T00:10:00Z"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: int(pid or 0) == 4242)
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["job_running"] is False
    assert payload["progress"]["status"] == "failed"


def test_step12_13_pipeline_status_reports_expired_lock_with_live_pid(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "api_pipeline_interval.pid").write_text(
        json.dumps({"pid": 123456, "run_id": "run_live", "cycle_id": "cycle_live", "mode": "once"}),
        encoding="utf-8",
    )
    (runtime / "strategy_pipeline.lock").write_text(
        json.dumps(
            {
                "lock_owner_pid": 123456,
                "run_id": "run_live",
                "cycle_id": "cycle_live",
                "stage": "wait_micro_ready_micro_full",
                "expires_at": "2020-01-01T00:00:00Z",
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: int(pid or 0) == 123456)
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["job_running"] is True
    assert payload["lock_stale"] is True
    assert payload["lock_stale_but_pid_running"] is True
    assert payload["lock"]["run_id"] == "run_live"


def test_step12_14_pipeline_status_recovers_active_job_from_lock(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "strategy_pipeline.lock").write_text(
        json.dumps(
            {
                "lock_owner_pid": 123456,
                "run_id": "run_recovered",
                "cycle_id": "cycle_recovered",
                "started_at": "2026-05-26T00:00:00Z",
                "stage": "wait_micro_ready_micro_full",
                "expires_at": "2099-01-01T00:00:00Z",
            },
        ),
        encoding="utf-8",
    )
    (runtime / "strategy_pipeline_progress.json").write_text(
        json.dumps(
            {
                "run_id": "run_recovered",
                "cycle_id": "cycle_recovered",
                "mode": "interval",
                "line": "all",
                "status": "running",
                "overall_percent": 73,
                "lines": {},
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: pid == 123456)
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["job_running"] is True
    assert payload["active_job_recovered"] is True
    assert payload["job_status_source"] == "recovered"
    assert payload["registry_health"]["reconcile_action"] == "recovered_from_lock"
    assert payload["active_job"]["run_id"] == "run_recovered"


def test_step12_15_pipeline_status_promotes_live_lock_over_dead_registry(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "api_pipeline_interval.pid").write_text(
        json.dumps(
            {
                "source": "api_pipeline_runner",
                "pid": 654321,
                "started_at": "2026-05-26T00:10:00Z",
                "mode": "once",
                "line": "all",
                "status": "started",
            },
        ),
        encoding="utf-8",
    )
    (runtime / "strategy_pipeline.lock").write_text(
        json.dumps(
            {
                "lock_owner_pid": 123456,
                "run_id": "run_cycle",
                "cycle_id": "cycle_cycle",
                "started_at": "2026-05-26T00:00:00Z",
                "stage": "wait_micro_ready_micro_full",
                "expires_at": "2099-01-01T00:00:00Z",
                "line": "micro_full",
            },
        ),
        encoding="utf-8",
    )
    (runtime / "strategy_pipeline_progress.json").write_text(
        json.dumps(
            {
                "run_id": "run_cycle",
                "cycle_id": "cycle_cycle",
                "mode": "interval",
                "line": "all",
                "status": "running",
                "overall_percent": 73,
                "current_stage": "wait_micro_ready_micro_full",
                "lines": {},
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: pid == 123456)
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["job_running"] is True
    assert payload["job_status_source"] == "lock_authority"
    assert payload["active_job"]["pid"] == 123456
    assert payload["active_job"]["mode"] == "interval"
    assert payload["registry_health"]["registry_pid_running"] is False
    assert payload["registry_health"]["lock_pid_running"] is True
    assert payload["registry_health"]["reconcile_action"] == "promoted_lock_owner"
    assert payload["run_controls"]["can_run_once"] is False
    assert payload["run_controls"]["can_run_cycle"] is False


def test_step12_15_pipeline_run_rejects_when_live_lock_active(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "strategy_pipeline.lock").write_text(
        json.dumps(
            {
                "lock_owner_pid": 123456,
                "run_id": "run_active",
                "cycle_id": "cycle_active",
                "started_at": "2026-05-26T00:00:00Z",
                "stage": "wait_micro_ready_micro_full",
                "expires_at": "2099-01-01T00:00:00Z",
            },
        ),
        encoding="utf-8",
    )
    (runtime / "strategy_pipeline_progress.json").write_text(
        json.dumps(
            {
                "run_id": "run_active",
                "cycle_id": "cycle_active",
                "mode": "interval",
                "line": "all",
                "status": "running",
                "overall_percent": 73,
                "lines": {},
            },
        ),
        encoding="utf-8",
    )
    calls: list[list[str]] = []
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: pid == 123456)
    monkeypatch.setattr(api_services, "run_cli", lambda args, background=False: calls.append(args) or {"status": "started"})
    client = TestClient(app)

    response = client.post("/api/pipeline/run", json={"line": "all", "mode": "once"})

    assert response.status_code == 400
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "pipeline_already_running"
    assert payload["error"]["detail"]["active_job"]["pid"] == 123456
    assert calls == []


def test_step12_15_dead_lock_progress_is_not_running(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    _mock_snapshot_warmup_ready(monkeypatch)
    runtime = tmp_path / "DATA" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "strategy_pipeline.lock").write_text(
        json.dumps(
            {
                "lock_owner_pid": 123456,
                "run_id": "run_stopped",
                "cycle_id": "cycle_stopped",
                "stage": "assemble_factor_with_micro",
                "expires_at": "2099-01-01T00:00:00Z",
            },
        ),
        encoding="utf-8",
    )
    (runtime / "strategy_pipeline_progress.json").write_text(
        json.dumps(
            {
                "run_id": "run_stopped",
                "cycle_id": "cycle_stopped",
                "mode": "interval",
                "line": "all",
                "status": "running",
                "overall_percent": 50,
                "lines": {},
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: False)
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["job_running"] is False
    assert payload["progress"]["status"] == "stopped"
    assert payload["run_controls"]["can_run_cycle"] is True
    assert payload["run_controls"]["can_stop"] is False


def test_step16_9_stop_cleans_dead_pid_strategy_pipeline_lock(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    lock_path = runtime / "strategy_pipeline.lock"
    lock_path.write_text(
        json.dumps(
            {
                "lock_owner_pid": 999999,
                "run_id": "run_dead",
                "cycle_id": "cycle_dead",
                "stage": "common_upstream_step1_to_step2_5",
                "expires_at": "2099-01-01T00:00:00Z",
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: False)
    client = TestClient(app)

    response = client.post("/api/pipeline/stop")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["status"] == "stopped"
    assert payload["cleanup"]["strategy_pipeline_lock"] == "removed_stale"
    assert "pipeline_lock_stale_stop_cleanup" in payload["cleanup"]["reason_codes"]
    assert not lock_path.exists()


def test_step16_9_run_start_cleans_dead_pid_strategy_pipeline_lock(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    _mock_snapshot_warmup_ready(monkeypatch)
    runtime = tmp_path / "DATA" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    lock_path = runtime / "strategy_pipeline.lock"
    lock_path.write_text(
        json.dumps(
            {
                "lock_owner_pid": 999999,
                "run_id": "run_dead",
                "cycle_id": "cycle_dead",
                "stage": "common_upstream_step1_to_step2_5",
                "expires_at": "2099-01-01T00:00:00Z",
            },
        ),
        encoding="utf-8",
    )
    calls: list[list[str]] = []
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: False)
    monkeypatch.setattr(api_services, "run_cli", lambda args, background=False: calls.append(args) or {"status": "started"})
    client = TestClient(app)

    response = client.post("/api/pipeline/run", json={"line": "all", "mode": "once"})

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["cleanup"]["strategy_pipeline_lock"] == "removed_stale"
    assert "pipeline_lock_stale_auto_recovered" in payload["cleanup"]["reason_codes"]
    assert not lock_path.exists()
    assert calls


def test_step12_19_inactive_started_registry_finalizes_from_latest_report(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    reports = tmp_path / "DATA" / "reports"
    runtime.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    registry = runtime / "api_pipeline_interval.pid"
    registry.write_text(
        json.dumps({"pid": 123456, "started_at": "2026-05-26T00:00:00Z", "status": "started", "mode": "once"}),
        encoding="utf-8",
    )
    (reports / "latest_strategy_pipeline_report.json").write_text(
        json.dumps(
            {
                "run_id": "run_done",
                "cycle_id": "cycle_done",
                "status": "ok",
                "started_at": "2026-05-26T00:00:00Z",
                "finished_at": "2026-05-26T00:10:00Z",
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: False)
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["job_running"] is False
    assert payload["active_job"] is None
    assert payload["display_state"] == "completed_or_idle"
    assert payload["display_run_id"] == "run_done"
    assert payload["latest_report"]["status"] == "ok"
    assert payload["registry_health"]["reconcile_action"] == "finalized_completed"


def test_step16_8_snapshot_daemon_start_does_not_claim_pipeline_registry(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        api_services,
        "step15_daemon_health_payload",
        lambda: {"daemon_status": "stale", "watchdog_status": "stale"},
    )

    class FakeProcess:
        pid = 4242

    monkeypatch.setattr(api_services.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())

    result = api_services.snapshot_daemon_action("start")

    assert result["status"] == "started"
    assert result["pid"] == 4242
    assert not (tmp_path / "DATA" / "runtime" / "api_pipeline_interval.pid").exists()
    assert result["log_path"].endswith("step15_snapshot_daemon.log")


def test_step16_8_pipeline_status_ignores_snapshot_daemon_registry(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    _mock_snapshot_warmup_ready(monkeypatch)
    runtime = tmp_path / "DATA" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    registry = runtime / "api_pipeline_interval.pid"
    registry.write_text(
        json.dumps(
            {
                "source": "api_pipeline_runner",
                "pid": 4242,
                "command": ["python", "-m", "laoma_signal_engine.cli", "snapshot-daemon", "run"],
                "status": "started",
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: int(pid or 0) == 4242)
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["active_job"] is None
    assert payload["job_running"] is False
    assert payload["run_controls"]["can_run_once"] is True
    assert not registry.exists()


def test_step16_2_pipeline_status_marks_live_interval_between_cycles(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(api_services, "utc_now", lambda: api_services.parse_iso_z("2026-05-26T00:12:00Z"))
    runtime = tmp_path / "DATA" / "runtime"
    reports = tmp_path / "DATA" / "reports"
    runtime.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    (runtime / "api_pipeline_interval.pid").write_text(
        json.dumps(
            {
                "pid": 123456,
                "started_at": "2026-05-26T00:00:00Z",
                "status": "completed",
                "run_id": "old_run",
                "cycle_id": "old_cycle",
                "mode": "interval",
                "interval_sec": 300,
            },
        ),
        encoding="utf-8",
    )
    (reports / "latest_strategy_pipeline_report.json").write_text(
        json.dumps(
            {
                "run_id": "run_latest",
                "cycle_id": "cycle_latest",
                "mode": "interval",
                "status": "ok",
                "started_at": "2026-05-26T00:10:00Z",
                "finished_at": "2026-05-26T00:10:30Z",
                "next_run_at": "2026-05-26T00:15:00Z",
            },
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: int(pid or 0) == 123456)
    client = TestClient(app)

    response = client.get("/api/pipeline/status/latest")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["job_running"] is False
    assert payload["display_state"] == "interval_waiting"
    assert payload["display_run_id"] == "run_latest"
    assert payload["active_job"] is None
    assert payload["active_interval"]["status"] == "interval_waiting"
    assert payload["active_interval"]["pid"] == 123456
    assert payload["run_controls"]["can_run_once"] is False
    assert payload["run_controls"]["can_run_cycle"] is False
    assert payload["run_controls"]["can_stop"] is True
    assert payload["run_controls"]["disabled_reason"] == "interval_cycle_waiting"


def test_step12_44_pipeline_run_rejects_once_during_interval_waiting(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(api_services, "utc_now", lambda: api_services.parse_iso_z("2026-05-26T00:12:00Z"))
    _mock_snapshot_warmup_ready(monkeypatch)
    runtime = tmp_path / "DATA" / "runtime"
    reports = tmp_path / "DATA" / "reports"
    runtime.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    (runtime / "api_pipeline_interval.pid").write_text(
        json.dumps(
            {
                "pid": 123456,
                "started_at": "2026-05-26T00:00:00Z",
                "status": "completed",
                "run_id": "old_run",
                "cycle_id": "old_cycle",
                "mode": "interval",
                "interval_sec": 300,
            },
        ),
        encoding="utf-8",
    )
    (reports / "latest_strategy_pipeline_report.json").write_text(
        json.dumps(
            {
                "run_id": "run_latest",
                "cycle_id": "cycle_latest",
                "mode": "interval",
                "status": "ok",
                "started_at": "2026-05-26T00:10:00Z",
                "finished_at": "2026-05-26T00:10:30Z",
                "next_run_at": "2026-05-26T00:15:00Z",
            },
        ),
        encoding="utf-8",
    )
    calls: list[list[str]] = []
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: int(pid or 0) == 123456)
    monkeypatch.setattr(api_services, "run_cli", lambda args, background=False: calls.append(args) or {"status": "started"})
    client = TestClient(app)

    response = client.post("/api/pipeline/run", json={"line": "all", "mode": "once"})

    assert response.status_code == 400
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "pipeline_interval_active"
    assert payload["error"]["detail"]["disabled_reason"] == "interval_cycle_waiting"
    assert payload["error"]["detail"]["active_interval"]["pid"] == 123456
    assert calls == []


def test_step12_24_run_cycle_watchdog_interval_waiting_uses_latest_report(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(api_services, "utc_now", lambda: api_services.parse_iso_z("2026-05-26T00:12:00Z"))
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: int(pid or 0) == 123456)
    monkeypatch.setattr(
        api_services,
        "_micro_daemon_health",
        lambda: {"status": "running", "heartbeat_age_sec": 1, "active_targets": 3, "stale": False},
    )
    monkeypatch.setattr(
        api_services,
        "_paper_daemon_health",
        lambda: {"status": "running", "heartbeat_age_sec": 2, "active_symbols": 1, "stale": False},
    )
    runtime = tmp_path / "DATA" / "runtime"
    reports = tmp_path / "DATA" / "reports"
    runtime.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)
    (runtime / "api_pipeline_interval.pid").write_text(
        json.dumps({"pid": 123456, "started_at": "2026-05-26T00:00:00Z", "status": "completed", "run_id": "old_run", "mode": "interval"}),
        encoding="utf-8",
    )
    (reports / "latest_strategy_pipeline_report.json").write_text(
        json.dumps(
            {
                "run_id": "run_latest",
                "cycle_id": "cycle_latest",
                "mode": "interval",
                "status": "ok",
                "started_at": "2026-05-26T00:00:00Z",
                "finished_at": "2026-05-26T00:10:00Z",
                "duration_sec": 600,
                "next_run_at": "2026-05-26T00:15:00Z",
            },
        ),
        encoding="utf-8",
    )
    client = TestClient(app)

    response = client.get("/api/pipeline/watchdog")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["health"] == "ok"
    assert payload["display_state"] == "interval_waiting"
    assert payload["display_run_id"] == "run_latest"
    assert payload["next_cycle_eta_sec"] == 180
    assert "inactive_active_job_residue" not in payload["reason_codes"]
    assert payload["active_interval"]["status"] == "interval_waiting"
    registry = json.loads((runtime / "api_pipeline_interval.pid").read_text(encoding="utf-8"))
    assert registry["status"] == "interval_waiting"
    assert registry["last_run_id"] == "run_latest"
    assert registry["pid_running"] is True
    assert (tmp_path / "DATA" / "runtime" / "run_cycle_watchdog.json").exists()


def test_step12_24_run_cycle_watchdog_running_detects_current_job(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(api_services, "utc_now", lambda: api_services.parse_iso_z("2026-05-26T00:00:10Z"))
    monkeypatch.setattr(api_services, "_pid_running", lambda pid: int(pid or 0) == 123456)
    monkeypatch.setattr(
        api_services,
        "_micro_daemon_health",
        lambda: {"status": "running", "heartbeat_age_sec": 1, "active_targets": 3, "stale": False},
    )
    monkeypatch.setattr(
        api_services,
        "_paper_daemon_health",
        lambda: {"status": "running", "heartbeat_age_sec": 2, "active_symbols": 1, "stale": False},
    )
    runtime = tmp_path / "DATA" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "strategy_pipeline.lock").write_text(
        json.dumps(
            {
                "lock_owner_pid": 123456,
                "run_id": "run_current",
                "cycle_id": "cycle_current",
                "started_at": "2026-05-26T00:00:00Z",
                "expires_at": "2099-01-01T00:00:00Z",
                "stage": "wait_micro_ready_micro_full",
                "line": "micro_full",
                "heartbeat_at": "2026-05-26T00:00:08Z",
            },
        ),
        encoding="utf-8",
    )
    (runtime / "strategy_pipeline_progress.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "run_id": "run_current",
                "cycle_id": "cycle_current",
                "mode": "interval",
                "status": "running",
                "current_stage": "wait_micro_ready_micro_full",
                "current_line": "micro_full",
                "overall_percent": 73,
                "updated_at": "2026-05-26T00:00:09Z",
                "lines": {},
            },
        ),
        encoding="utf-8",
    )
    client = TestClient(app)

    response = client.get("/api/runtime/run-cycle/health")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["health"] == "ok"
    assert payload["display_state"] == "running"
    assert payload["display_run_id"] == "run_current"
    assert payload["pid_running"] is True


def test_step12_11_runtime_status_detects_healthy_and_stale_daemons(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    micro = tmp_path / "DATA" / "micro"
    runtime.mkdir(parents=True, exist_ok=True)
    micro.mkdir(parents=True, exist_ok=True)
    (runtime / "micro_daemon.pid").write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    (micro / "micro_collector_heartbeat.json").write_text(
        json.dumps({"generated_at": "2026-05-26T00:00:00Z", "active_symbol_count": 3}),
        encoding="utf-8",
    )
    (runtime / "paper_daemon.pid").write_text(json.dumps({"pid": 999999}), encoding="utf-8")
    (runtime / "paper_daemon_heartbeat.json").write_text(
        json.dumps({"heartbeat_at": "2020-01-01T00:00:00Z"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "utc_now", lambda: api_services.parse_iso_z("2026-05-26T00:00:20Z"))
    client = TestClient(app)

    response = client.get("/api/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["micro_daemon"]["status"] == "running"
    assert payload["data"]["micro_daemon"]["heartbeat_age_sec"] == 20
    assert payload["data"]["paper_daemon"]["status"] == "stopped"
    assert "paper_daemon_not_healthy" in payload["data"]["errors"]


def test_step16_6_runtime_status_treats_fresh_micro_heartbeat_with_stale_pid_as_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    micro = tmp_path / "DATA" / "micro"
    runtime.mkdir(parents=True, exist_ok=True)
    micro.mkdir(parents=True, exist_ok=True)
    (runtime / "micro_daemon.pid").write_text(json.dumps({"pid": 999999}), encoding="utf-8")
    (micro / "micro_collector_heartbeat.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-26T00:00:10Z",
                "active_symbol_count": 3,
                "ws_connected": True,
            },
        ),
        encoding="utf-8",
    )
    (runtime / "paper_daemon.pid").write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    (runtime / "paper_daemon_heartbeat.json").write_text(
        json.dumps({"heartbeat_at": "2026-05-26T00:00:10Z"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "utc_now", lambda: api_services.parse_iso_z("2026-05-26T00:00:20Z"))
    client = TestClient(app)

    response = client.get("/api/runtime/status")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["micro_daemon"]["status"] == "running"
    assert payload["micro_daemon"]["health_state"] == "data_plane_healthy_pid_stale"
    assert payload["micro_daemon"]["process_registry_status"] == "pid_stale"
    assert payload["micro_daemon"]["data_plane_status"] == "fresh"
    assert "micro_daemon_not_healthy" not in payload["errors"]


def test_step16_11_runtime_start_rehydrates_micro_when_data_plane_fresh_but_pid_stale(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    micro = tmp_path / "DATA" / "micro"
    runtime.mkdir(parents=True, exist_ok=True)
    micro.mkdir(parents=True, exist_ok=True)
    (micro / "micro_collector_heartbeat.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-26T00:00:10Z",
                "active_symbol_count": 3,
                "ws_connected": True,
            },
        ),
        encoding="utf-8",
    )
    (runtime / "paper_daemon.pid").write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    (runtime / "paper_daemon_heartbeat.json").write_text(
        json.dumps({"heartbeat_at": "2026-05-26T00:00:10Z"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "utc_now", lambda: api_services.parse_iso_z("2026-05-26T00:00:20Z"))
    calls: list[str] = []

    def fake_micro(action: str) -> dict[str, object]:
        calls.append(action)
        return {"status": "started", "pid": 123}

    monkeypatch.setattr(api_services, "micro_daemon_action", fake_micro)
    client = TestClient(app)

    response = client.post("/api/runtime/start")

    assert response.status_code == 200
    assert response.json()["data"]["started"]["micro_daemon"] is True
    assert calls == ["start"]


def test_step16_10_runtime_status_exposes_micro_data_plane_stale_reason_codes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    runtime = tmp_path / "DATA" / "runtime"
    micro = tmp_path / "DATA" / "micro"
    runtime.mkdir(parents=True, exist_ok=True)
    micro.mkdir(parents=True, exist_ok=True)
    (runtime / "micro_daemon.pid").write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    (micro / "micro_collector_heartbeat.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-26T00:00:00Z",
                "active_symbol_count": 3,
                "ws_connected": True,
                "heartbeat_seq": 42,
                "last_ws_event_at": "2026-05-26T00:00:00Z",
            },
        ),
        encoding="utf-8",
    )
    (micro / "latest_micro_state.json").write_text(json.dumps({"generated_at": "2026-05-26T00:00:00Z"}), encoding="utf-8")
    (micro / "latest_micro_features.json").write_text(json.dumps({"generated_at": "2026-05-26T00:00:00Z"}), encoding="utf-8")
    (runtime / "paper_daemon.pid").write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    (runtime / "paper_daemon_heartbeat.json").write_text(
        json.dumps({"heartbeat_at": "2026-05-26T00:20:00Z"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "utc_now", lambda: api_services.parse_iso_z("2026-05-26T00:20:00Z"))
    client = TestClient(app)

    response = client.get("/api/runtime/status")

    assert response.status_code == 200
    payload = response.json()["data"]
    micro_payload = payload["micro_daemon"]
    assert micro_payload["status"] == "stale"
    assert micro_payload["health_state"] == "degraded_transport_ok_data_stale"
    assert micro_payload["data_plane_status"] == "stale"
    assert "micro_alive_but_not_emitting" in micro_payload["reason_codes"]
    assert "micro_ws_connected_but_no_emit" in micro_payload["reason_codes"]
    assert "micro_alive_but_not_emitting" in payload["errors"]


def test_step12_11_runtime_start_uses_existing_daemon_commands(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    calls: list[tuple[str, str]] = []

    def fake_micro(action: str) -> dict[str, object]:
        calls.append(("micro", action))
        return {"status": "started", "pid": 11}

    def fake_paper(action: str) -> dict[str, object]:
        calls.append(("paper", action))
        return {"status": "started", "pid": 22}

    monkeypatch.setattr(api_services, "micro_daemon_action", fake_micro)
    monkeypatch.setattr(api_services, "paper_daemon_payload", fake_paper)
    client = TestClient(app)

    response = client.post("/api/runtime/start")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert ("micro", "start") in calls
    assert ("paper", "start") in calls


def test_step11_8_runtime_status_auto_recovers_stale_daemon(tmp_path: Path, monkeypatch) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    runtime = tmp_path / "DATA" / "runtime"
    micro = tmp_path / "DATA" / "micro"
    runtime.mkdir(parents=True, exist_ok=True)
    micro.mkdir(parents=True, exist_ok=True)
    (runtime / "micro_daemon.pid").write_text(json.dumps({"pid": 999999}), encoding="utf-8")
    (micro / "micro_collector_heartbeat.json").write_text(
        json.dumps({"heartbeat_at": "2020-01-01T00:00:00Z"}),
        encoding="utf-8",
    )
    (runtime / "paper_daemon.pid").write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    (runtime / "paper_daemon_heartbeat.json").write_text(
        json.dumps({"heartbeat_at": "2026-05-26T00:00:10Z"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "utc_now", lambda: api_services.parse_iso_z("2026-05-26T00:00:20Z"))
    actions: list[tuple[str, str]] = []

    def fake_micro(action: str) -> dict[str, object]:
        actions.append(("micro", action))
        return {"status": "completed" if action == "stop" else "started", "pid": 123}

    monkeypatch.setattr(api_services, "micro_daemon_action", fake_micro)
    monkeypatch.setattr(
        api_services,
        "_wait_micro_daemon_recovered",
        lambda timeout_sec=8.0: {
            "status": "running",
            "stale": False,
            "data_plane_fresh": True,
            "pid": 123,
        },
    )
    client = TestClient(app)

    response = client.get("/api/runtime/status")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["recovery"]["attempted"] is True
    assert ("micro", "stop") in actions
    assert ("micro", "start") in actions
    assert (tmp_path / "DATA/runtime/latest_runtime_recovery.json").is_file()


def test_step16_11_runtime_recovery_force_cleans_stale_micro_pid_after_already_running(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _use_temp_config(tmp_path, monkeypatch)
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    runtime = tmp_path / "DATA" / "runtime"
    micro = tmp_path / "DATA" / "micro"
    runtime.mkdir(parents=True, exist_ok=True)
    micro.mkdir(parents=True, exist_ok=True)
    pid_path = runtime / "micro_daemon.pid"
    pid_path.write_text(json.dumps({"pid": 999999}), encoding="utf-8")
    (micro / "micro_collector_heartbeat.json").write_text(
        json.dumps({"heartbeat_at": "2020-01-01T00:00:00Z"}),
        encoding="utf-8",
    )
    (runtime / "paper_daemon.pid").write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    (runtime / "paper_daemon_heartbeat.json").write_text(
        json.dumps({"heartbeat_at": "2026-05-26T00:00:10Z"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(api_services, "utc_now", lambda: api_services.parse_iso_z("2026-05-26T00:00:20Z"))
    calls: list[str] = []

    def fake_micro(action: str) -> dict[str, object]:
        calls.append(action)
        if action == "stop":
            return {"status": "stop_failed", "pid": 999999}
        if calls.count("start") == 1:
            return {"status": "already_running", "pid": 999999}
        return {"status": "started", "pid": 123}

    monkeypatch.setattr(api_services, "micro_daemon_action", fake_micro)
    monkeypatch.setattr(
        api_services,
        "_wait_micro_daemon_recovered",
        lambda timeout_sec=8.0: {
            "status": "running",
            "stale": False,
            "data_plane_fresh": True,
            "pid": 123,
        },
    )
    client = TestClient(app)

    response = client.get("/api/runtime/status")

    assert response.status_code == 200
    payload = response.json()["data"]
    action = payload["recovery"]["actions"][0]
    assert payload["recovery"]["status"] == "recovered"
    assert action["status"] == "recovered"
    assert action["force_cleanup_applied"] is True
    assert action["pid_file_backed_up"] is True
    assert action["new_pid"] == 123
    assert calls == ["stop", "start", "start"]
    assert not pid_path.exists()
    assert list(runtime.glob("micro_daemon.pid.stale_*"))


def test_step1242_rest_budget_and_warmup_degradation_api_contract(monkeypatch) -> None:
    import laoma_signal_engine.api.app as api_app

    monkeypatch.setattr(
        api_app,
        "rest_budget_runtime_payload",
        lambda: {
            "schema_version": "STEP12.42_rest_budget_runtime_v1",
            "source": "snapshot_daemon+latest_snapshot",
            "generated_at": "2026-06-02T00:00:00Z",
            "rest_circuit_state": "half_open",
            "rest_recovery_stage": "half_open_probe",
            "current_shard_size": 3,
            "next_shard_size": 6,
            "rest_request_count": 4,
            "rest_endpoint_counts": {"https://fapi.binance.com/fapi/v1/ticker/24hr": 4},
            "rest_status_code_counts": {"200": 4},
            "status_418_count": 0,
            "status_429_count": 0,
            "freshness_counts": {"fresh": 5, "stale_usable": 1, "stale_blocked": 0},
            "source_mix": {"live_shard_count": 3, "cache_merged_count": 3},
            "reason_codes": ["rest_circuit_half_open"],
        },
    )
    monkeypatch.setattr(
        api_app,
        "snapshot_warmup_payload",
        lambda *args, **kwargs: {
            "schema_version": "STEP16.8_snapshot_warmup_v1",
            "status": "ready_degraded",
            "ready_status_detail": "ready_degraded_rest_half_open",
            "freshness_degradation_reason": "rest_half_open",
            "ready": True,
            "allow_run_once": True,
            "allow_run_cycle": True,
            "reason_codes": ["ready_degraded_rest_half_open", "rest_circuit_half_open"],
        },
    )
    client = TestClient(app)

    rest_budget = client.get("/api/runtime/rest-budget").json()["data"]
    warmup = client.get("/api/runtime/warmup").json()["data"]

    assert rest_budget["schema_version"] == "STEP12.42_rest_budget_runtime_v1"
    assert rest_budget["rest_recovery_stage"] == "half_open_probe"
    assert rest_budget["rest_status_code_counts"] == {"200": 4}
    assert warmup["status"] == "ready_degraded"
    assert warmup["ready_status_detail"] == "ready_degraded_rest_half_open"
    assert warmup["allow_run_once"] is True
