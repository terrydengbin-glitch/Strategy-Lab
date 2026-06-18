from __future__ import annotations

from contextlib import asynccontextmanager
import json
import os
import sys
import time
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from laoma_signal_engine.api.contracts import (
    BacktestP21ExportRequest,
    BacktestP21MatrixRequest,
    BacktestP21V2JobStartRequest,
    BacktestP21V2KlineDownloadRequest,
    BacktestP21V2GateBatchMaterializeRequest,
    BacktestP21V2GateBuildRequest,
    BacktestP21V2MatrixRequest,
    BacktestP21V2OpsCandidateExportRequest,
    BacktestP21V2OpsEnhancedValidationRequest,
    BacktestP21V2OpsRetentionRequest,
    BacktestP21V2OpsServingRebuildRequest,
    BacktestP21V2OpsTqMaterializeJobRequest,
    BacktestP21V2QualityMaterializeRequest,
    BacktestP21V2Strategy4ReplayRunRequest,
    ConfigUpdateRequest,
    FeishuSendTradePlansRequest,
    FeishuTestRequest,
    PipelineRunRequest,
    StrategySandboxActiveRequest,
    StrategySandboxCodePatchRequest,
    StrategySandboxCreateRequest,
    StrategySandboxDeleteRequest,
    StrategySandboxFullBacktestRunRequest,
    StrategySandboxGateActionRequest,
    StrategySandboxGatedRunRequest,
    StrategySandboxJobRequest,
    StrategySandboxPipelineRunRequest,
    StrategySandboxPipelineStopRequest,
    StrategySandboxRuntimeBuildRequest,
    TradeQualityPromotionDisableRequest,
    TradeQualityPromotionRequest,
    fail,
    ok,
)
from laoma_signal_engine.api.services import (
    ApiServiceError,
    CURRENT_JSON_PATHS,
    apply_config_profile,
    backtest_p21_experiment_detail_service,
    backtest_p21_experiments_service,
    backtest_p21_export_config_candidate_service,
    backtest_p21_packages_service,
    backtest_p21_problem_baseline_service,
    backtest_p21_recommendations_service,
    backtest_p21_run_matrix_service,
    backtest_p21_v2_experiment_detail_service,
    backtest_p21_v2_experiment_daily_service,
    backtest_p21_v2_experiment_orders_service,
    backtest_p21_v2_experiment_symbols_service,
    backtest_p21_v2_experiments_service,
    backtest_p21_v2_export_config_candidate_service,
    backtest_p21_v2_gate_buckets_rebuild_service,
    backtest_p21_v2_gate_buckets_service,
    backtest_p21_v2_gate_candidates_generate_service,
    backtest_p21_v2_gate_candidates_service,
    backtest_p21_v2_gate_features_materialize_service,
    backtest_p21_v2_gate_features_service,
    backtest_p21_v2_gate_recommendations_service,
    backtest_p21_v2_gate_scores_rebuild_service,
    backtest_p21_v2_gate_scores_service,
    backtest_p21_v2_gate_tq_batch_materialize_service,
    backtest_p21_v2_job_start_service,
    backtest_p21_v2_job_status_service,
    backtest_p21_v2_job_stop_service,
    backtest_p21_v2_jobs_service,
    backtest_p21_v2_kline_cache_download_service,
    backtest_p21_v2_kline_cache_status_service,
    backtest_p21_v2_leaderboard_service,
    backtest_p21_v2_matrix_contracts_service,
    backtest_p21_v2_matrix_run_service,
    backtest_p21_v2_ops_candidate_export_service,
    backtest_p21_v2_ops_enhanced_validation_service,
    backtest_p21_v2_ops_footprint_service,
    backtest_p21_v2_ops_retention_manifest_service,
    backtest_p21_v2_ops_serving_rebuild_service,
    backtest_p21_v2_ops_serving_summary_service,
    backtest_p21_v2_ops_tq_job_enqueue_service,
    backtest_p21_v2_ops_tq_job_process_next_service,
    backtest_p21_v2_ops_tq_jobs_service,
    backtest_p21_v2_quality_aggregates_service,
    backtest_p21_v2_quality_materialize_service,
    backtest_p21_v2_quality_packages_service,
    backtest_p21_v2_quality_samples_service,
    backtest_p21_v2_quality_summary_service,
    backtest_p21_v2_strategy4_replay_attempts_service,
    backtest_p21_v2_strategy4_replay_pool_service,
    backtest_p21_v2_strategy4_replay_run_service,
    backtest_p21_v2_strategy4_replay_summary_service,
    candidate_pool_governance_payload,
    config_profiles,
    config_effective_payload,
    config_field_impact_map_payload,
    config_field_impact_summary_payload,
    config_legacy_fields_payload,
    config_ui_schema_payload,
    exchange_info_cache_refresh_payload,
    feishu_send_trade_plans,
    feishu_config,
    load_yaml_config,
    mask_config,
    micro_daemon_action,
    notification_deliveries,
    paper_archive_reset,
    paper_consumption_status,
    paper_daemon_payload,
    paper_experiment_detail,
    paper_experiments,
    paper_payload,
    paper_summary_lite,
    paper_realism_payload,
    pipeline_funnel_history_payload,
    pipeline_funnel_latest_payload,
    pipeline_status,
    pipeline_status_lite,
    read_json_file,
    run_pipeline,
    run_cycle_watchdog_health,
    runtime_autostart_if_configured,
    runtime_restart,
    runtime_start,
    runtime_status,
    runtime_status_lite,
    runtime_stop,
    research_db_dataset_cards_service,
    research_db_entry_features_service,
    research_db_field_coverage_service,
    research_db_lineage_audit_service,
    research_db_materialize_service,
    research_db_summary_service,
    research_db_tq_samples_service,
    research_db_trade_facts_service,
    research_db_writer_status_service,
    stop_pipeline,
    reload_config,
    rest_health_payload,
    rest_budget_runtime_payload,
    rest_safety_config_payload,
    run_audit_by_id,
    run_audit_latest,
    run_audit_latest_lite,
    run_audit_list,
    run_audit_list_lite,
    snapshot_warmup_payload,
    step15_daemon_health_payload,
    step15_daemon_payload,
    step15_snapshot_quality_by_run,
    step15_snapshot_quality_latest,
    strategy4_attempts_payload,
    strategy4_observe_pool_payload,
    strategy4_runtime_payload,
    strategy5_evidence_payload,
    strategy5_runtime_payload,
    strategy6_decisions_payload,
    strategy6_attempts_payload,
    strategy6_daemon_action_payload,
    strategy6_evidence_payload,
    strategy6_heartbeat_payload,
    strategy6_observe_pool_payload,
    strategy6_recheck_now_payload,
    strategy6_run_once_payload,
    strategy6_runtime_payload,
    strategy6_wait_pool_payload,
    strategy6_watchdog_payload,
    strategy_sandbox_active_service,
    strategy_sandbox_add_code_patch_service,
    strategy_sandbox_branches_service,
    strategy_sandbox_code_overlay_service,
    strategy_sandbox_create_code_overlay_service,
    strategy_sandbox_create_service,
    strategy_sandbox_db_health_service,
    strategy_sandbox_delete_service,
    strategy_sandbox_external_integration_audit_events_service,
    strategy_sandbox_external_integration_health_service,
    strategy_sandbox_external_integration_run_service,
    strategy_sandbox_full_backtest_run_cancel_service,
    strategy_sandbox_full_backtest_run_create_service,
    strategy_sandbox_full_backtest_run_resume_service,
    strategy_sandbox_full_backtest_run_service,
    strategy_sandbox_gate_action_ingest_service,
    strategy_sandbox_gated_orders_service,
    strategy_sandbox_gated_paper_shadow_service,
    strategy_sandbox_gated_performance_service,
    strategy_sandbox_gated_replay_service,
    strategy_sandbox_gated_trade_quality_samples_service,
    strategy_sandbox_gate_compare_service,
    strategy_sandbox_get_service,
    strategy_sandbox_job_service,
    strategy_sandbox_leaderboard_service,
    strategy_sandbox_list_service,
    strategy_sandbox_runtime_build_service,
    strategy_sandbox_runtime_smoke_service,
    strategy_sandbox_resource_governor_status_service,
    strategy_sandbox_resource_governor_runs_service,
    strategy_sandbox_resource_governor_run_service,
    strategy_sandbox_resource_governor_rest_budget_service,
    strategy_sandbox_daemon_writer_status_service,
    strategy_sandbox_full_pipeline_run_service,
    strategy_sandbox_pipeline_run_service,
    strategy_sandbox_pipeline_stop_service,
    strategy_sandbox_set_active_service,
    strategy_sandbox_summary_service,
    strategy_sandbox_trade_quality_compare_service,
    strategy_sandbox_trade_candidates_service,
    strategy_sandbox_universe_service,
    micro_quality_audit_by_id,
    micro_quality_audit_latest,
    micro_evidence_runtime_by_id,
    micro_evidence_runtime_findings,
    micro_evidence_runtime_latest,
    micro_evidence_runtime_reason,
    micro_evidence_runtime_symbol,
    micro_evidence_target_source,
    micro_training_by_id,
    micro_training_coverage,
    micro_training_latest,
    micro_training_runs,
    micro_training_symbol,
    micro_fast_runtime_stability_by_id,
    micro_fast_runtime_stability_latest,
    micro_fast_runtime_stability_reason,
    micro_fast_tail_cleanup_by_id,
    micro_fast_tail_cleanup_latest,
    micro_fast_tail_cleanup_reason,
    micro_fast_judgeable_by_id,
    micro_fast_judgeable_latest,
    micro_fast_judgeable_only_by_id,
    micro_fast_judgeable_only_latest,
    micro_fast_judgeable_only_reason,
    micro_fast_judgeable_only_symbol,
    micro_fast_judgeable_reason,
    micro_fast_judgeable_symbol,
    micro_fast_judgeable_throughput_by_id,
    micro_fast_judgeable_throughput_latest,
    micro_fast_coverage_split_by_id,
    micro_fast_coverage_split_latest,
    micro_fast_valid_bucket_by_id,
    micro_fast_valid_bucket_latest,
    micro_full_z_by_id,
    micro_full_z_latest,
    trade_plan_funnel_payload,
    trade_plans_payload,
    trade_quality_archive_backfill_service,
    trade_quality_ingest_ledger_payload,
    trade_quality_payload,
    trade_quality_replay_backfill_service,
    trade_quality_replay_ledger_payload,
    trade_quality_diagnostics_archive_packages_service,
    trade_quality_diagnostics_aggregates_service,
    trade_quality_diagnostics_replay_ledger_service,
    trade_quality_diagnostics_replay_service,
    trade_quality_diagnostics_sample_detail_service,
    trade_quality_diagnostics_samples_service,
    trade_quality_diagnostics_summary_service,
    trade_quality_diagnostics_sync_service,
    trade_quality_diagnostics_sync_status_service,
    trade_quality_diagnostics_refresh_enrich_service,
    trade_quality_entry_context_v3_backfill_service,
    trade_quality_entry_features_backfill_service,
    trade_quality_entry_market_context_backfill_service,
    trade_quality_entry_microstructure_backfill_service,
    trade_quality_recommendation_promotion_apply_service,
    trade_quality_recommendation_promotion_disable_service,
    trade_quality_recommendation_promotion_dry_run_service,
    trade_quality_recommendation_promotions_service,
    trade_quality_recommendation_rules_rebuild_service,
    trade_quality_recommendation_rules_service,
    trade_quality_recommendation_validation_service,
    trade_quality_v4_deep_root_service,
    trade_quality_v4_evidence_service,
    trade_quality_v4_gate_candidates_generate_service,
    trade_quality_v4_gate_candidates_service,
    trade_quality_v4_materialize_service,
    trade_quality_v4_summary_service,
    trade_quality_v5_causal_factors_service,
    trade_quality_v5_gate_candidates_generate_service,
    trade_quality_v5_gate_candidates_service,
    trade_quality_v5_materialize_service,
    trade_quality_v5_summary_service,
    trade_quality_v5_writer_coverage_service,
    trade_quality_promotion_candidates_rebuild_service,
    trade_quality_promotion_candidates_service,
    update_rest_safety_config_payload,
    update_config_section,
    validate_config_section,
)
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now


@asynccontextmanager
async def lifespan(_: FastAPI):
    runtime_autostart_if_configured()
    yield


app = FastAPI(title="abnormal-signal-engine-api", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "http://127.0.0.1:4173",
        "http://localhost:4173",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def performance_headers(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    duration_ms = round((time.perf_counter() - started) * 1000, 1)
    response.headers["X-Response-Time-ms"] = str(duration_ms)
    size_header = response.headers.get("content-length")
    try:
        response_bytes = int(size_header) if size_header else 0
    except ValueError:
        response_bytes = 0
    if duration_ms > 1000 or response_bytes > 1_000_000:
        print(
            f"[api-perf] route={request.url.path} duration_ms={duration_ms} response_bytes={response_bytes}",
            file=sys.stderr,
        )
    return response


def _truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _sandbox_mutation_payload(request: Request, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    out = dict(payload or {})
    headers = request.headers
    header_map = {
        "x-sandbox-caller-type": "caller_type",
        "x-sandbox-caller-id": "caller_id",
        "x-sandbox-source-surface": "source_surface",
        "x-sandbox-audit-trace-id": "audit_trace_id",
        "x-idempotency-key": "idempotency_key",
    }
    for header_name, payload_key in header_map.items():
        value = headers.get(header_name)
        if value:
            out[payload_key] = value

    policy = out.get("operation_policy") if isinstance(out.get("operation_policy"), dict) else {}
    policy = dict(policy)
    raw_policy = headers.get("x-sandbox-operation-policy")
    if raw_policy:
        try:
            parsed = json.loads(raw_policy)
            if isinstance(parsed, dict):
                policy.update(parsed)
        except json.JSONDecodeError:
            policy["auth_failed"] = True

    expected_key = os.environ.get("LAOMA_SANDBOX_API_KEY")
    require_key = _truthy_env(os.environ.get("LAOMA_SANDBOX_REQUIRE_API_KEY"))
    provided_key = headers.get("x-sandbox-api-key")
    if expected_key and provided_key and provided_key == expected_key:
        policy["authenticated"] = True
        out["caller_identity_source"] = "header_api_key"
    elif require_key:
        policy["auth_required"] = True
        policy["auth_failed"] = True
        out["caller_identity_source"] = "missing_or_invalid_api_key"
    elif any(headers.get(name) for name in header_map):
        out["caller_identity_source"] = "header"

    if require_key:
        policy["auth_required"] = True
    out["operation_policy"] = policy
    return out


@app.exception_handler(ApiServiceError)
async def api_service_error_handler(_: Any, exc: ApiServiceError) -> JSONResponse:
    status = 404 if exc.code == "file_missing" else 400
    return JSONResponse(status_code=status, content=fail(exc.code, exc.message, exc.detail))


@app.get("/api/health")
def health() -> dict[str, Any]:
    return ok(
        {
            "service": "abnormal-signal-engine-api",
            "status": "ok",
            "generated_at": to_iso_z(utc_now()),
        },
    )


@app.get("/api/config")
def get_config() -> dict[str, Any]:
    return ok(mask_config(load_yaml_config()))


@app.put("/api/config/{section}")
def put_config(section: str, req: ConfigUpdateRequest) -> dict[str, Any]:
    return ok(update_config_section(section, req.values))


@app.post("/api/config/{section}/validate")
def post_config_validate(section: str, req: ConfigUpdateRequest) -> dict[str, Any]:
    return ok(validate_config_section(section, req.values))


@app.get("/api/config/profiles")
def get_config_profiles() -> dict[str, Any]:
    return ok(config_profiles())


@app.get("/api/config/field-impact-map")
def get_config_field_impact_map() -> dict[str, Any]:
    return ok(config_field_impact_map_payload())


@app.get("/api/config/field-impact-summary")
def get_config_field_impact_summary() -> dict[str, Any]:
    return ok(config_field_impact_summary_payload())


@app.get("/api/config/effective")
def get_config_effective(strategy_line: str) -> dict[str, Any]:
    return ok(config_effective_payload(strategy_line))


@app.get("/api/config/ui-schema")
def get_config_ui_schema() -> dict[str, Any]:
    return ok(config_ui_schema_payload())


@app.get("/api/config/legacy-fields")
def get_config_legacy_fields() -> dict[str, Any]:
    return ok(config_legacy_fields_payload())


@app.post("/api/config/profiles/{profile_name}/apply")
def post_config_profile_apply(profile_name: str) -> dict[str, Any]:
    return ok(apply_config_profile(profile_name))


@app.post("/api/config/reload")
def post_config_reload() -> dict[str, Any]:
    return ok(reload_config())


@app.post("/api/micro-daemon/start")
def start_micro_daemon() -> dict[str, Any]:
    return ok(micro_daemon_action("start"))


@app.post("/api/micro-daemon/stop")
def stop_micro_daemon() -> dict[str, Any]:
    return ok(micro_daemon_action("stop"))


@app.get("/api/micro-daemon/status")
def micro_daemon_status() -> dict[str, Any]:
    return ok(micro_daemon_action("status"))


@app.post("/api/pipeline/run")
def post_pipeline_run(req: PipelineRunRequest) -> dict[str, Any]:
    return ok(run_pipeline(req))


@app.post("/api/pipeline/stop")
def post_pipeline_stop() -> dict[str, Any]:
    return ok(stop_pipeline())


@app.get("/api/pipeline/status/latest")
def get_pipeline_status() -> dict[str, Any]:
    return ok(pipeline_status())


@app.get("/api/pipeline/status-lite")
def get_pipeline_status_lite() -> dict[str, Any]:
    return ok(pipeline_status_lite())


@app.get("/api/pipeline/status")
def get_pipeline_status_compat() -> dict[str, Any]:
    return ok(pipeline_status())


@app.get("/api/pipeline/watchdog")
def get_pipeline_watchdog() -> dict[str, Any]:
    return ok(run_cycle_watchdog_health())


@app.get("/api/pipeline/funnel/latest")
def get_pipeline_funnel_latest(refresh: bool = True) -> dict[str, Any]:
    return ok(pipeline_funnel_latest_payload(refresh=refresh))


@app.get("/api/pipeline/funnel/history")
def get_pipeline_funnel_history(limit: int = 50) -> dict[str, Any]:
    return ok(pipeline_funnel_history_payload(limit=limit))


@app.get("/api/runtime/run-cycle/health")
def get_runtime_run_cycle_health() -> dict[str, Any]:
    return ok(run_cycle_watchdog_health())


@app.get("/api/runtime/status")
def get_runtime_status() -> dict[str, Any]:
    return ok(runtime_status())


@app.get("/api/runtime/status-lite")
def get_runtime_status_lite() -> dict[str, Any]:
    return ok(runtime_status_lite())


@app.get("/api/runtime/rest-health")
def get_runtime_rest_health() -> dict[str, Any]:
    return ok(rest_health_payload())


@app.get("/api/runtime/rest-budget")
def get_runtime_rest_budget() -> dict[str, Any]:
    return ok(rest_budget_runtime_payload())


@app.get("/api/runtime/step15-daemon")
def get_runtime_step15_daemon() -> dict[str, Any]:
    return ok(step15_daemon_payload())


@app.get("/api/runtime/step15-daemon/health")
def get_runtime_step15_daemon_health() -> dict[str, Any]:
    return ok(step15_daemon_health_payload())


@app.get("/api/runtime/step15-daemon/watchdog")
def get_runtime_step15_daemon_watchdog() -> dict[str, Any]:
    return ok(step15_daemon_health_payload())


@app.get("/api/runtime/warmup")
def get_runtime_warmup() -> dict[str, Any]:
    return ok(snapshot_warmup_payload())


@app.get("/api/audit/step15/snapshot-quality/latest")
def get_step15_snapshot_quality_latest() -> dict[str, Any]:
    return ok(step15_snapshot_quality_latest())


@app.get("/api/audit/step15/snapshot-quality/runs/{run_id}")
def get_step15_snapshot_quality_run(run_id: str) -> dict[str, Any]:
    return ok(step15_snapshot_quality_by_run(run_id))


@app.get("/api/config/rest-safety")
def get_rest_safety_config() -> dict[str, Any]:
    return ok(rest_safety_config_payload())


@app.patch("/api/config/rest-safety")
def patch_rest_safety_config(req: ConfigUpdateRequest) -> dict[str, Any]:
    return ok(update_rest_safety_config_payload(req.values))


@app.post("/api/runtime/exchange-info-cache/refresh")
def post_exchange_info_cache_refresh() -> dict[str, Any]:
    return ok(exchange_info_cache_refresh_payload())


@app.post("/api/runtime/start")
def post_runtime_start() -> dict[str, Any]:
    return ok(runtime_start())


@app.post("/api/runtime/stop")
def post_runtime_stop() -> dict[str, Any]:
    return ok(runtime_stop())


@app.post("/api/runtime/restart")
def post_runtime_restart() -> dict[str, Any]:
    return ok(runtime_restart())


@app.get("/api/decisions/trade-plans")
def get_trade_plans() -> dict[str, Any]:
    return ok(trade_plans_payload())


@app.get("/api/decisions/trade-plans/funnel")
def get_trade_plan_funnel(run_id: str | None = None, symbol_limit: int = 300) -> dict[str, Any]:
    return ok(trade_plan_funnel_payload(run_id=run_id, symbol_limit=symbol_limit))


@app.get("/api/strategy4/observe-pool")
def get_strategy4_observe_pool() -> dict[str, Any]:
    return ok(strategy4_observe_pool_payload())


@app.get("/api/strategy4/runtime")
def get_strategy4_runtime() -> dict[str, Any]:
    return ok(strategy4_runtime_payload())


@app.get("/api/strategy4/attempts")
def get_strategy4_attempts(limit: int = 200) -> dict[str, Any]:
    return ok(strategy4_attempts_payload(limit=limit))


@app.get("/api/strategy5/runtime")
def get_strategy5_runtime(limit: int = 200) -> dict[str, Any]:
    return ok(strategy5_runtime_payload(limit=limit))


@app.get("/api/strategy5/evidence")
def get_strategy5_evidence(limit: int = 200) -> dict[str, Any]:
    return ok(strategy5_evidence_payload(limit=limit))


@app.get("/api/strategy6/runtime")
def get_strategy6_runtime(limit: int = 200) -> dict[str, Any]:
    return ok(strategy6_runtime_payload(limit=limit))


@app.get("/api/strategy6/evidence")
def get_strategy6_evidence(limit: int = 200) -> dict[str, Any]:
    return ok(strategy6_evidence_payload(limit=limit))


@app.get("/api/strategy6/decisions")
def get_strategy6_decisions(limit: int = 200) -> dict[str, Any]:
    return ok(strategy6_decisions_payload(limit=limit))


@app.get("/api/strategy6/wait-pool")
def get_strategy6_wait_pool(limit: int = 200) -> dict[str, Any]:
    return ok(strategy6_wait_pool_payload(limit=limit))


@app.get("/api/strategy6/observe-pool")
def get_strategy6_observe_pool(limit: int = 200) -> dict[str, Any]:
    return ok(strategy6_observe_pool_payload(limit=limit))


@app.get("/api/strategy6/attempts")
def get_strategy6_attempts(limit: int = 200) -> dict[str, Any]:
    return ok(strategy6_attempts_payload(limit=limit))


@app.get("/api/strategy6/heartbeat")
def get_strategy6_heartbeat() -> dict[str, Any]:
    return ok(strategy6_heartbeat_payload())


@app.get("/api/strategy6/daemon/watchdog")
def get_strategy6_daemon_watchdog() -> dict[str, Any]:
    return ok(strategy6_watchdog_payload(recover=False))


@app.post("/api/strategy6/run-once")
def post_strategy6_run_once() -> dict[str, Any]:
    return ok(strategy6_run_once_payload())


@app.post("/api/strategy6/daemon/start")
def post_strategy6_daemon_start() -> dict[str, Any]:
    return ok(strategy6_daemon_action_payload("start"))


@app.post("/api/strategy6/daemon/stop")
def post_strategy6_daemon_stop() -> dict[str, Any]:
    return ok(strategy6_daemon_action_payload("stop"))


@app.post("/api/strategy6/daemon/recheck-now")
def post_strategy6_recheck_now() -> dict[str, Any]:
    return ok(strategy6_recheck_now_payload())


@app.post("/api/strategy6/daemon/watchdog/recover")
def post_strategy6_daemon_watchdog_recover() -> dict[str, Any]:
    return ok(strategy6_watchdog_payload(recover=True))


@app.get("/api/governance/candidate-pool")
def get_candidate_pool_governance(limit: int = 120) -> dict[str, Any]:
    return ok(candidate_pool_governance_payload(limit=limit))


@app.get("/api/decisions/latest")
def get_latest_decisions() -> dict[str, Any]:
    got = read_json_file(CURRENT_JSON_PATHS["latest_decisions"])
    return ok(got["data"], source_path=got["source_path"], generated_at=got["generated_at"])


@app.get("/api/reports/latest-strategy")
def get_latest_strategy_report() -> dict[str, Any]:
    got = read_json_file(CURRENT_JSON_PATHS["latest_strategy"])
    return ok(got["data"], source_path=got["source_path"], generated_at=got["generated_at"])


@app.get("/api/reports/latest-audit")
def get_latest_audit_report() -> dict[str, Any]:
    got = read_json_file(CURRENT_JSON_PATHS["latest_audit"])
    return ok(got["data"], source_path=got["source_path"], generated_at=got["generated_at"])


@app.get("/api/reports/abc")
def get_abc_report() -> dict[str, Any]:
    got = read_json_file(CURRENT_JSON_PATHS["abc"])
    return ok(got["data"], source_path=got["source_path"], generated_at=got["generated_at"])


@app.get("/api/audit/runs")
def get_run_audits(limit: int = 20, status: str | None = None) -> dict[str, Any]:
    return ok(run_audit_list(limit=limit, status=status))


@app.get("/api/audit/runs-lite")
def get_run_audits_lite(limit: int = 20, status: str | None = None) -> dict[str, Any]:
    return ok(run_audit_list_lite(limit=limit, status=status))


@app.get("/api/audit/runs/latest")
def get_latest_run_audit() -> dict[str, Any]:
    return ok(run_audit_latest())


@app.get("/api/audit/runs/latest-lite")
def get_latest_run_audit_lite() -> dict[str, Any]:
    return ok(run_audit_latest_lite())


@app.get("/api/audit/runs/{run_id}")
def get_run_audit(run_id: str) -> dict[str, Any]:
    return ok(run_audit_by_id(run_id))


@app.get("/api/audit/micro-quality/latest")
def get_micro_quality_audit_latest() -> dict[str, Any]:
    return ok(micro_quality_audit_latest())


@app.get("/api/audit/micro-quality/{run_id}")
def get_micro_quality_audit(run_id: str) -> dict[str, Any]:
    return ok(micro_quality_audit_by_id(run_id))


@app.get("/api/audit/micro-quality/{run_id}/symbols")
def get_micro_quality_audit_symbols(run_id: str) -> dict[str, Any]:
    payload = micro_quality_audit_by_id(run_id)
    return ok({"run_id": payload.get("run_id"), "symbols": payload.get("symbols") or []})


@app.get("/api/audit/micro-quality/{run_id}/summary")
def get_micro_quality_audit_summary(run_id: str) -> dict[str, Any]:
    payload = micro_quality_audit_by_id(run_id)
    return ok({"run_id": payload.get("run_id"), "summary": payload.get("summary") or {}})


@app.get("/api/audit/micro-evidence/latest")
def get_micro_evidence_runtime_latest() -> dict[str, Any]:
    return ok(micro_evidence_runtime_latest())


@app.get("/api/audit/micro-evidence/symbols/{symbol}")
def get_micro_evidence_runtime_symbol(symbol: str, limit: int = 100) -> dict[str, Any]:
    return ok(micro_evidence_runtime_symbol(symbol, limit=limit))


@app.get("/api/audit/micro-evidence/findings")
def get_micro_evidence_runtime_findings(
    run_id: str | None = None,
    line: str | None = None,
    symbol: str | None = None,
    severity: str | None = None,
    reason: str | None = None,
    attributed_reason: str | None = None,
    commit_barrier_status: str | None = None,
    bucket_gap_class: str | None = None,
    ofi_gap_class: str | None = None,
    history_gap_class: str | None = None,
    queue_backpressure_state: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    return ok(
        micro_evidence_runtime_findings(
            run_id=run_id,
            line=line,
            symbol=symbol,
            severity=severity,
            reason=reason,
            attributed_reason=attributed_reason,
            commit_barrier_status=commit_barrier_status,
            bucket_gap_class=bucket_gap_class,
            ofi_gap_class=ofi_gap_class,
            history_gap_class=history_gap_class,
            queue_backpressure_state=queue_backpressure_state,
            limit=limit,
        )
    )


@app.get("/api/audit/micro-evidence/reasons/{reason}")
def get_micro_evidence_runtime_reason(reason: str, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    return ok(micro_evidence_runtime_reason(reason, run_id=run_id, limit=limit))


@app.get("/api/audit/micro-evidence/target-source")
def get_micro_evidence_target_source() -> dict[str, Any]:
    return ok(micro_evidence_target_source())


@app.get("/api/micro-training/latest")
def get_micro_training_latest(symbol_limit: int = 100) -> dict[str, Any]:
    return ok(micro_training_latest(symbol_limit=symbol_limit))


@app.get("/api/micro-training/runs")
def get_micro_training_runs(limit: int = 50) -> dict[str, Any]:
    return ok(micro_training_runs(limit=limit))


@app.get("/api/micro-training/runs/{run_id}")
def get_micro_training_run(run_id: str, symbol_limit: int = 200) -> dict[str, Any]:
    return ok(micro_training_by_id(run_id, symbol_limit=symbol_limit))


@app.get("/api/micro-training/symbols/{symbol}")
def get_micro_training_symbol(symbol: str, limit: int = 100) -> dict[str, Any]:
    return ok(micro_training_symbol(symbol, limit=limit))


@app.get("/api/micro-training/coverage")
def get_micro_training_coverage() -> dict[str, Any]:
    return ok(micro_training_coverage())


@app.get("/api/audit/micro-full-z/latest")
def get_micro_full_z_latest() -> dict[str, Any]:
    return ok(micro_full_z_latest())


@app.get("/api/audit/micro-full-z/{run_id}")
def get_micro_full_z(run_id: str) -> dict[str, Any]:
    return ok(micro_full_z_by_id(run_id))


@app.get("/api/audit/micro-fast-runtime/latest")
def get_micro_fast_runtime_latest() -> dict[str, Any]:
    return ok(micro_fast_runtime_stability_latest())


@app.get("/api/audit/micro-fast-runtime/tail-cleanup/latest")
def get_micro_fast_tail_cleanup_latest() -> dict[str, Any]:
    return ok(micro_fast_tail_cleanup_latest())


@app.get("/api/audit/micro-fast-runtime/tail-cleanup/reasons/{reason}")
def get_micro_fast_tail_cleanup_reason(reason: str, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    return ok(micro_fast_tail_cleanup_reason(reason, run_id=run_id, limit=limit))


@app.get("/api/audit/micro-fast-runtime/tail-cleanup/{run_id}")
def get_micro_fast_tail_cleanup(run_id: str) -> dict[str, Any]:
    return ok(micro_fast_tail_cleanup_by_id(run_id))


@app.get("/api/audit/micro-fast-runtime/judgeable/latest")
def get_micro_fast_judgeable_latest() -> dict[str, Any]:
    return ok(micro_fast_judgeable_latest())


@app.get("/api/audit/micro-fast-runtime/judgeable/reasons/{reason}")
def get_micro_fast_judgeable_reason(reason: str, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    return ok(micro_fast_judgeable_reason(reason, run_id=run_id, limit=limit))


@app.get("/api/audit/micro-fast-runtime/judgeable/symbols/{symbol}")
def get_micro_fast_judgeable_symbol(symbol: str, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    return ok(micro_fast_judgeable_symbol(symbol, run_id=run_id, limit=limit))


@app.get("/api/audit/micro-fast-runtime/judgeable/{run_id}")
def get_micro_fast_judgeable(run_id: str) -> dict[str, Any]:
    return ok(micro_fast_judgeable_by_id(run_id))


@app.get("/api/audit/micro-fast-runtime/judgeable-only/latest")
def get_micro_fast_judgeable_only_latest() -> dict[str, Any]:
    return ok(micro_fast_judgeable_only_latest())


@app.get("/api/audit/micro-fast-runtime/judgeable-only/reasons/{reason}")
def get_micro_fast_judgeable_only_reason(reason: str, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    return ok(micro_fast_judgeable_only_reason(reason, run_id=run_id, limit=limit))


@app.get("/api/audit/micro-fast-runtime/judgeable-only/symbols/{symbol}")
def get_micro_fast_judgeable_only_symbol(symbol: str, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    return ok(micro_fast_judgeable_only_symbol(symbol, run_id=run_id, limit=limit))


@app.get("/api/audit/micro-fast-runtime/judgeable-only/{run_id}")
def get_micro_fast_judgeable_only(run_id: str) -> dict[str, Any]:
    return ok(micro_fast_judgeable_only_by_id(run_id))


@app.get("/api/audit/micro-fast-runtime/judgeable-throughput/latest")
def api_get_micro_fast_judgeable_throughput_latest() -> dict[str, Any]:
    return ok(micro_fast_judgeable_throughput_latest())


@app.get("/api/audit/micro-fast-runtime/judgeable-throughput/{run_id}")
def get_micro_fast_judgeable_throughput(run_id: str) -> dict[str, Any]:
    return ok(micro_fast_judgeable_throughput_by_id(run_id))


@app.get("/api/audit/micro-fast-runtime/coverage-split/latest")
def api_get_micro_fast_coverage_split_latest() -> dict[str, Any]:
    return ok(micro_fast_coverage_split_latest())


@app.get("/api/audit/micro-fast-runtime/coverage-split/{run_id}")
def get_micro_fast_coverage_split(run_id: str) -> dict[str, Any]:
    return ok(micro_fast_coverage_split_by_id(run_id))


@app.get("/api/audit/micro-fast-runtime/valid-bucket/latest")
def api_get_micro_fast_valid_bucket_latest() -> dict[str, Any]:
    return ok(micro_fast_valid_bucket_latest())


@app.get("/api/audit/micro-fast-runtime/valid-bucket/{run_id}")
def get_micro_fast_valid_bucket(run_id: str) -> dict[str, Any]:
    return ok(micro_fast_valid_bucket_by_id(run_id))


@app.get("/api/audit/micro-fast-runtime/reasons/{reason}")
def get_micro_fast_runtime_reason(reason: str, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    return ok(micro_fast_runtime_stability_reason(reason, run_id=run_id, limit=limit))


@app.get("/api/audit/micro-fast-runtime/{run_id}")
def get_micro_fast_runtime(run_id: str) -> dict[str, Any]:
    return ok(micro_fast_runtime_stability_by_id(run_id))


@app.get("/api/audit/micro-evidence/{run_id}")
def get_micro_evidence_runtime(run_id: str) -> dict[str, Any]:
    return ok(micro_evidence_runtime_by_id(run_id))


@app.get("/api/paper/summary")
def get_paper_summary(line: str | None = None) -> dict[str, Any]:
    return ok(paper_payload("summary", line=line))


@app.get("/api/paper/summary-lite")
def get_paper_summary_lite(line: str | None = None, limit: int = 20) -> dict[str, Any]:
    return ok(paper_summary_lite(line=line, limit=limit))


@app.get("/api/paper/accounts")
def get_paper_accounts(line: str | None = None) -> dict[str, Any]:
    return ok(paper_payload("accounts", line=line))


@app.get("/api/paper/orders")
def get_paper_orders(line: str | None = None) -> dict[str, Any]:
    return ok(paper_payload("orders", line=line))


@app.get("/api/paper/positions")
def get_paper_positions(line: str | None = None) -> dict[str, Any]:
    return ok(paper_payload("positions", line=line))


@app.get("/api/paper/fills")
def get_paper_fills(line: str | None = None, symbol: str | None = None, run_id: str | None = None, limit: int = 100) -> dict[str, Any]:
    if symbol or run_id:
        return ok(paper_realism_payload("fills", line=line, symbol=symbol, run_id=run_id, limit=limit))
    return ok(paper_payload("fills", line=line))


@app.get("/api/paper/reconciliation")
def get_paper_reconciliation(line: str | None = None, symbol: str | None = None, run_id: str | None = None, limit: int = 100) -> dict[str, Any]:
    return ok(paper_realism_payload("reconciliation", line=line, symbol=symbol, run_id=run_id, limit=limit))


@app.get("/api/paper/order-trace")
def get_paper_order_trace(order_id: str | None = None, line: str | None = None, symbol: str | None = None, run_id: str | None = None, limit: int = 100) -> dict[str, Any]:
    return ok(paper_realism_payload("order-trace", line=line, symbol=symbol, run_id=run_id, order_id=order_id, limit=limit))


@app.get("/api/paper/realism-metrics")
def get_paper_realism_metrics(line: str | None = None, symbol: str | None = None, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    return ok(paper_realism_payload("realism-metrics", line=line, symbol=symbol, run_id=run_id, limit=limit))


@app.get("/api/paper/performance")
def get_paper_performance(line: str | None = None) -> dict[str, Any]:
    return ok(paper_payload("performance", line=line))


@app.get("/api/paper/stats")
def get_paper_stats(line: str | None = None) -> dict[str, Any]:
    return ok(paper_payload("stats", line=line))


@app.get("/api/paper/intents")
def get_paper_intents(line: str | None = None) -> dict[str, Any]:
    return ok(paper_payload("intents", line=line))


@app.get("/api/paper/epochs")
def get_paper_epochs(line: str | None = None) -> dict[str, Any]:
    return ok(paper_payload("epochs", line=line))


@app.get("/api/paper/trace")
def get_paper_trace(line: str | None = None, symbol: str | None = None) -> dict[str, Any]:
    return ok(paper_payload("trace", line=line, symbol=symbol))


@app.get("/api/paper/consumption-status")
def get_paper_consumption_status(run_id: str | None = None, limit: int = 50) -> dict[str, Any]:
    return ok(paper_consumption_status(run_id=run_id, limit=limit))


@app.get("/api/paper/detail")
def get_paper_detail(line: str | None = None, symbol: str | None = None) -> dict[str, Any]:
    return ok(paper_payload("detail", line=line, symbol=symbol))


@app.post("/api/paper/archive-reset")
def post_paper_archive_reset(payload: dict[str, Any]) -> dict[str, Any]:
    return ok(paper_archive_reset(payload))


@app.get("/api/paper/experiments")
def get_paper_experiments(line: str | None = None, limit: int = 50) -> dict[str, Any]:
    return ok(paper_experiments(line=line, limit=limit))


@app.get("/api/paper/experiments/{experiment_id}")
def get_paper_experiment(experiment_id: str) -> dict[str, Any]:
    return ok(paper_experiment_detail(experiment_id))


@app.get("/api/paper/daemon/status")
def get_paper_daemon_status() -> dict[str, Any]:
    return ok(paper_daemon_payload("status"))


@app.post("/api/paper/daemon/start")
def post_paper_daemon_start() -> dict[str, Any]:
    return ok(paper_daemon_payload("start"))


@app.post("/api/paper/daemon/stop")
def post_paper_daemon_stop() -> dict[str, Any]:
    return ok(paper_daemon_payload("stop"))


@app.post("/api/paper/daemon/restart")
def post_paper_daemon_restart() -> dict[str, Any]:
    return ok(paper_daemon_payload("restart"))


@app.get("/api/paper/worker/status")
def get_paper_worker_status() -> dict[str, Any]:
    return ok(paper_daemon_payload("status"))


@app.post("/api/paper/worker/run-once")
def post_paper_worker_run_once() -> dict[str, Any]:
    return ok(paper_daemon_payload("run-once"))


@app.post("/api/paper/worker/start")
def post_paper_worker_start() -> dict[str, Any]:
    return ok(paper_daemon_payload("start"))


@app.post("/api/paper/worker/stop")
def post_paper_worker_stop() -> dict[str, Any]:
    return ok(paper_daemon_payload("stop"))


@app.get("/api/backtest/p21/packages")
def get_backtest_p21_packages() -> dict[str, Any]:
    return ok(backtest_p21_packages_service())


@app.post("/api/backtest/p21/problem-baseline")
def post_backtest_p21_problem_baseline(
    source: str = "all",
    archive_id: str | None = None,
    strategy_line: str = "all",
    limit: int = 5000,
    write: bool = True,
) -> dict[str, Any]:
    return ok(
        backtest_p21_problem_baseline_service(
            source=source,
            archive_id=archive_id,
            strategy_line=strategy_line,
            limit=limit,
            write=write,
        )
    )


@app.post("/api/backtest/p21/run-matrix")
def post_backtest_p21_run_matrix(body: BacktestP21MatrixRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_run_matrix_service(
            source=body.source,
            archive_id=body.archive_id,
            strategy_line=body.strategy_line,
            limit=body.limit,
            max_sets=body.max_sets,
            parameter_grid=body.parameter_grid,
            write=body.write,
        )
    )


@app.get("/api/backtest/p21/experiments")
def get_backtest_p21_experiments(limit: int = 50) -> dict[str, Any]:
    return ok(backtest_p21_experiments_service(limit=limit))


@app.get("/api/backtest/p21/experiments/{experiment_id}")
def get_backtest_p21_experiment_detail(experiment_id: str) -> dict[str, Any]:
    return ok(backtest_p21_experiment_detail_service(experiment_id))


@app.get("/api/backtest/p21/recommendations")
def get_backtest_p21_recommendations(limit: int = 50) -> dict[str, Any]:
    return ok(backtest_p21_recommendations_service(limit=limit))


@app.post("/api/backtest/p21/export-config-candidate")
def post_backtest_p21_export_config_candidate(body: BacktestP21ExportRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_export_config_candidate_service(
            experiment_id=body.experiment_id,
            parameter_set_id=body.parameter_set_id,
        )
    )


@app.get("/api/backtest/p21/v2/kline-cache/status")
def get_backtest_p21_v2_kline_cache_status(
    days: int = 30,
    max_symbols: int = 50,
    symbols: str | None = None,
) -> dict[str, Any]:
    symbol_list = [item.strip().upper() for item in symbols.split(",") if item.strip()] if symbols else None
    return ok(backtest_p21_v2_kline_cache_status_service(symbols=symbol_list, days=days, max_symbols=max_symbols))


@app.post("/api/backtest/p21/v2/kline-cache/download")
def post_backtest_p21_v2_kline_cache_download(body: BacktestP21V2KlineDownloadRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_kline_cache_download_service(
            symbols=body.symbols,
            days=body.days,
            max_symbols=body.max_symbols,
            dry_run=body.dry_run,
            sleep_sec=body.sleep_sec,
        )
    )


@app.get("/api/backtest/p21/v2/matrix/contracts")
def get_backtest_p21_v2_matrix_contracts(strategy_line: str = "all", max_sets: int = 240) -> dict[str, Any]:
    return ok(backtest_p21_v2_matrix_contracts_service(strategy_line=strategy_line, max_sets=max_sets))


@app.post("/api/backtest/p21/v2/matrix/run")
def post_backtest_p21_v2_matrix_run(body: BacktestP21V2MatrixRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_matrix_run_service(
            symbols=body.symbols,
            strategy_line=body.strategy_line,
            days=body.days,
            max_symbols=body.max_symbols,
            max_sets=body.max_sets,
            parameter_grid=body.parameter_grid,
            write=body.write,
        )
    )


@app.post("/api/backtest/p21/v2/jobs/start")
def post_backtest_p21_v2_job_start(body: BacktestP21V2JobStartRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_job_start_service(
            job_type=body.job_type,
            symbols=body.symbols,
            strategy_line=body.strategy_line,
            days=body.days,
            max_symbols=body.max_symbols,
            max_sets=body.max_sets,
            symbol_shard_size=body.symbol_shard_size,
            max_workers=body.max_workers,
            scheduler_mode=body.scheduler_mode,
            resume_experiment_id=body.resume_experiment_id,
            sleep_sec=body.sleep_sec,
        )
    )


@app.get("/api/backtest/p21/v2/jobs")
def get_backtest_p21_v2_jobs(limit: int = 20) -> dict[str, Any]:
    return ok(backtest_p21_v2_jobs_service(limit=limit))


@app.get("/api/backtest/p21/v2/jobs/{job_id}/status")
def get_backtest_p21_v2_job_status(job_id: str) -> dict[str, Any]:
    return ok(backtest_p21_v2_job_status_service(job_id))


@app.post("/api/backtest/p21/v2/jobs/{job_id}/stop")
def post_backtest_p21_v2_job_stop(job_id: str) -> dict[str, Any]:
    return ok(backtest_p21_v2_job_stop_service(job_id))


@app.get("/api/backtest/p21/v2/matrix/experiments")
def get_backtest_p21_v2_experiments(limit: int = 50) -> dict[str, Any]:
    return ok(backtest_p21_v2_experiments_service(limit=limit))


@app.get("/api/backtest/p21/v2/matrix/experiments/{experiment_id}")
def get_backtest_p21_v2_experiment_detail(experiment_id: str) -> dict[str, Any]:
    return ok(backtest_p21_v2_experiment_detail_service(experiment_id))


@app.get("/api/backtest/p21/v2/matrix/experiments/{experiment_id}/orders")
def get_backtest_p21_v2_experiment_orders(
    experiment_id: str,
    limit: int = 100,
    offset: int = 0,
    strategy_line: str | None = None,
    symbol: str | None = None,
    parameter_set_id: str | None = None,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_experiment_orders_service(
            experiment_id,
            limit=limit,
            offset=offset,
            strategy_line=strategy_line,
            symbol=symbol,
            parameter_set_id=parameter_set_id,
        )
    )


@app.get("/api/backtest/p21/v2/matrix/experiments/{experiment_id}/daily")
def get_backtest_p21_v2_experiment_daily(experiment_id: str, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    return ok(backtest_p21_v2_experiment_daily_service(experiment_id, limit=limit, offset=offset))


@app.get("/api/backtest/p21/v2/matrix/experiments/{experiment_id}/symbols")
def get_backtest_p21_v2_experiment_symbols(experiment_id: str, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    return ok(backtest_p21_v2_experiment_symbols_service(experiment_id, limit=limit, offset=offset))


@app.get("/api/backtest/p21/v2/matrix/leaderboard")
def get_backtest_p21_v2_leaderboard(limit: int = 50, exclude_legacy: bool = True) -> dict[str, Any]:
    return ok(backtest_p21_v2_leaderboard_service(limit=limit, exclude_legacy=exclude_legacy))


@app.post("/api/backtest/p21/v2/strategy4/replay/run")
def post_backtest_p21_v2_strategy4_replay_run(body: BacktestP21V2Strategy4ReplayRunRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_strategy4_replay_run_service(
            symbols=body.symbols,
            days=body.days,
            max_symbols=body.max_symbols,
            max_sets=body.max_sets,
            max_admissions_per_symbol=body.max_admissions_per_symbol,
            max_attempts=body.max_attempts,
            observe_interval_min=body.observe_interval_min,
            write=body.write,
        )
    )


@app.get("/api/backtest/p21/v2/strategy4/replay/summary")
def get_backtest_p21_v2_strategy4_replay_summary(
    experiment_id: str | None = None,
    parameter_set_id: str | None = None,
) -> dict[str, Any]:
    return ok(backtest_p21_v2_strategy4_replay_summary_service(experiment_id=experiment_id, parameter_set_id=parameter_set_id))


@app.get("/api/backtest/p21/v2/strategy4/replay/pool")
def get_backtest_p21_v2_strategy4_replay_pool(
    experiment_id: str | None = None,
    parameter_set_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_strategy4_replay_pool_service(
            experiment_id=experiment_id,
            parameter_set_id=parameter_set_id,
            status=status,
            limit=limit,
            offset=offset,
        )
    )


@app.get("/api/backtest/p21/v2/strategy4/replay/attempts")
def get_backtest_p21_v2_strategy4_replay_attempts(
    experiment_id: str | None = None,
    parameter_set_id: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_strategy4_replay_attempts_service(
            experiment_id=experiment_id,
            parameter_set_id=parameter_set_id,
            symbol=symbol,
            limit=limit,
            offset=offset,
        )
    )


@app.post("/api/backtest/p21/v2/matrix/export-config-candidate")
def post_backtest_p21_v2_export_config_candidate(body: BacktestP21ExportRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_export_config_candidate_service(
            experiment_id=body.experiment_id,
            parameter_set_id=body.parameter_set_id,
        )
    )


@app.get("/api/backtest/p21/v2/quality/packages")
def get_backtest_p21_v2_quality_packages(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    mode: str = "materialized",
    limit: int = 50,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_quality_packages_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            mode=mode,
            limit=limit,
        )
    )


@app.get("/api/backtest/p21/v2/quality/summary")
def get_backtest_p21_v2_quality_summary(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    exit_reason: str | None = None,
    root_cause: str | None = None,
    entry_quality_label: str | None = None,
    entry_context_v3_label: str | None = None,
    limit: int = 5000,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_quality_summary_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            symbol=symbol,
            side=side,
            exit_reason=exit_reason,
            root_cause=root_cause,
            entry_quality_label=entry_quality_label,
            entry_context_v3_label=entry_context_v3_label,
            limit=limit,
        )
    )


@app.get("/api/backtest/p21/v2/quality/aggregates")
def get_backtest_p21_v2_quality_aggregates(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    package_key: str | None = None,
    dimension: str | None = None,
    key: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_quality_aggregates_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            package_key=package_key,
            dimension=dimension,
            key=key,
            limit=limit,
        )
    )


@app.get("/api/backtest/p21/v2/quality/samples")
def get_backtest_p21_v2_quality_samples(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    exit_reason: str | None = None,
    root_cause: str | None = None,
    entry_quality_label: str | None = None,
    entry_context_v3_label: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_quality_samples_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            symbol=symbol,
            side=side,
            exit_reason=exit_reason,
            root_cause=root_cause,
            entry_quality_label=entry_quality_label,
            entry_context_v3_label=entry_context_v3_label,
            limit=limit,
            offset=offset,
        )
    )


@app.post("/api/backtest/p21/v2/quality/materialize")
def post_backtest_p21_v2_quality_materialize(body: BacktestP21V2QualityMaterializeRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_quality_materialize_service(
            experiment_id=body.experiment_id,
            strategy_line=body.strategy_line,
            parameter_set_id=body.parameter_set_id,
            top_n=body.top_n,
            limit=body.limit,
            dry_run=body.dry_run,
            force=body.force,
        )
    )


@app.post("/api/backtest/p21/v2/gate/tq-batch-materialize")
def post_backtest_p21_v2_gate_tq_batch_materialize(body: BacktestP21V2GateBatchMaterializeRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_tq_batch_materialize_service(
            experiment_id=body.experiment_id,
            strategy_line=body.strategy_line,
            top_n=body.top_n,
            limit=body.limit,
            dry_run=body.dry_run,
        )
    )


@app.post("/api/backtest/p21/v2/gate/features/materialize")
def post_backtest_p21_v2_gate_features_materialize(body: BacktestP21V2GateBuildRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_features_materialize_service(
            experiment_id=body.experiment_id,
            strategy_line=body.strategy_line,
            parameter_set_id=body.parameter_set_id,
            limit=body.limit,
            dry_run=body.dry_run,
        )
    )


@app.get("/api/backtest/p21/v2/gate/features")
def get_backtest_p21_v2_gate_features(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    train_split: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_features_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            symbol=symbol,
            side=side,
            train_split=train_split,
            limit=limit,
        )
    )


@app.post("/api/backtest/p21/v2/gate/buckets/rebuild")
def post_backtest_p21_v2_gate_buckets_rebuild(body: BacktestP21V2GateBuildRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_buckets_rebuild_service(
            experiment_id=body.experiment_id,
            strategy_line=body.strategy_line,
            parameter_set_id=body.parameter_set_id,
            min_samples=body.min_samples,
            dry_run=body.dry_run,
        )
    )


@app.get("/api/backtest/p21/v2/gate/buckets")
def get_backtest_p21_v2_gate_buckets(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    dimension: str | None = None,
    sample_period: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_buckets_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            dimension=dimension,
            sample_period=sample_period,
            limit=limit,
        )
    )


@app.post("/api/backtest/p21/v2/gate/scores/rebuild")
def post_backtest_p21_v2_gate_scores_rebuild(body: BacktestP21V2GateBuildRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_scores_rebuild_service(
            experiment_id=body.experiment_id,
            strategy_line=body.strategy_line,
            parameter_set_id=body.parameter_set_id,
            dry_run=body.dry_run,
        )
    )


@app.get("/api/backtest/p21/v2/gate/scores")
def get_backtest_p21_v2_gate_scores(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    score_name: str | None = None,
    overfit_risk: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_scores_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            score_name=score_name,
            overfit_risk=overfit_risk,
            limit=limit,
        )
    )


@app.post("/api/backtest/p21/v2/gate/candidates/generate")
def post_backtest_p21_v2_gate_candidates_generate(body: BacktestP21V2GateBuildRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_candidates_generate_service(
            experiment_id=body.experiment_id,
            strategy_line=body.strategy_line,
            parameter_set_id=body.parameter_set_id,
            min_test_pf=body.min_test_pf,
            min_coverage=body.min_coverage,
            dry_run=body.dry_run,
        )
    )


@app.get("/api/backtest/p21/v2/gate/candidates")
def get_backtest_p21_v2_gate_candidates(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    gate_type: str | None = None,
    status: str | None = None,
    overfit_risk: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_candidates_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            gate_type=gate_type,
            status=status,
            overfit_risk=overfit_risk,
            limit=limit,
        )
    )


@app.get("/api/backtest/p21/v2/gate/recommendations")
def get_backtest_p21_v2_gate_recommendations(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    status: str | None = "shadow",
    target_profile: str | None = "review_only",
    limit: int = 100,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_recommendations_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            status=status,
            target_profile=target_profile,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/gates/features")
def get_trade_quality_gates_features(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    train_split: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_features_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            symbol=symbol,
            side=side,
            train_split=train_split,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/gates/buckets")
def get_trade_quality_gates_buckets(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    dimension: str | None = None,
    sample_period: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_buckets_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            dimension=dimension,
            sample_period=sample_period,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/gates/validations")
def get_trade_quality_gates_validations(
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    score_name: str | None = None,
    overfit_risk: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_scores_service(
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            score_name=score_name,
            overfit_risk=overfit_risk,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/gates/candidates")
def get_trade_quality_gates_candidates(
    candidate_id: str | None = None,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    gate_type: str | None = None,
    status: str | None = None,
    overfit_risk: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_candidates_service(
            candidate_id=candidate_id,
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            gate_type=gate_type,
            status=status,
            overfit_risk=overfit_risk,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/gates/config-preview")
def get_trade_quality_gates_config_preview(
    candidate_id: str | None = None,
    experiment_id: str | None = None,
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    status: str | None = "shadow",
    target_profile: str | None = "review_only",
    limit: int = 100,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_gate_recommendations_service(
            candidate_id=candidate_id,
            experiment_id=experiment_id,
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            status=status,
            target_profile=target_profile,
            limit=limit,
        )
    )


@app.post("/api/trade-quality/v4/materialize")
def post_trade_quality_v4_materialize(
    strategy_line: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    strategies = None if not strategy_line or strategy_line == "all" else [strategy_line]
    return ok(trade_quality_v4_materialize_service(strategies=strategies, limit=limit))


@app.post("/api/trade-quality/v4/gate-candidates/generate")
def post_trade_quality_v4_gate_candidates_generate(
    strategy_line: str | None = None,
    min_samples: int = 50,
    limit: int = 80,
) -> dict[str, Any]:
    return ok(
        trade_quality_v4_gate_candidates_generate_service(
            strategy_line=strategy_line,
            min_samples=min_samples,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/v4/summary")
def get_trade_quality_v4_summary() -> dict[str, Any]:
    return ok(trade_quality_v4_summary_service())


@app.get("/api/trade-quality/v4/evidence")
def get_trade_quality_v4_evidence(
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        trade_quality_v4_evidence_service(
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/v4/deep-root-causes")
def get_trade_quality_v4_deep_root_causes(
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        trade_quality_v4_deep_root_service(
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/v4/gate-candidates")
def get_trade_quality_v4_gate_candidates(
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        trade_quality_v4_gate_candidates_service(
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            limit=limit,
        )
    )


@app.post("/api/trade-quality/v5/materialize")
def post_trade_quality_v5_materialize(
    strategy_line: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    strategies = None if not strategy_line or strategy_line == "all" else [strategy_line]
    return ok(trade_quality_v5_materialize_service(strategies=strategies, limit=limit))


@app.post("/api/trade-quality/v5/gate-candidates/generate")
def post_trade_quality_v5_gate_candidates_generate(
    strategy_line: str | None = None,
    min_samples: int = 50,
    limit: int = 80,
) -> dict[str, Any]:
    return ok(
        trade_quality_v5_gate_candidates_generate_service(
            strategy_line=strategy_line,
            min_samples=min_samples,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/v5/summary")
def get_trade_quality_v5_summary() -> dict[str, Any]:
    return ok(trade_quality_v5_summary_service())


@app.get("/api/trade-quality/v5/causal-factors")
def get_trade_quality_v5_causal_factors(
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    root_cause: str | None = None,
    direction_factor_v5: str | None = None,
    entry_timing_factor_v5: str | None = None,
    tp_realism_factor_v5: str | None = None,
    profit_factor_v5: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    return ok(
        trade_quality_v5_causal_factors_service(
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            root_cause=root_cause,
            direction_factor_v5=direction_factor_v5,
            entry_timing_factor_v5=entry_timing_factor_v5,
            tp_realism_factor_v5=tp_realism_factor_v5,
            profit_factor_v5=profit_factor_v5,
            limit=limit,
            offset=offset,
        )
    )


@app.get("/api/trade-quality/v5/gate-candidates")
def get_trade_quality_v5_gate_candidates(
    strategy_line: str | None = None,
    parameter_set_id: str | None = None,
    recommendation: str | None = None,
    overfit_risk: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    return ok(
        trade_quality_v5_gate_candidates_service(
            strategy_line=strategy_line,
            parameter_set_id=parameter_set_id,
            recommendation=recommendation,
            overfit_risk=overfit_risk,
            limit=limit,
            offset=offset,
        )
    )


@app.get("/api/trade-quality/v5/writer-coverage")
def get_trade_quality_v5_writer_coverage() -> dict[str, Any]:
    return ok(trade_quality_v5_writer_coverage_service())


@app.post("/api/research-db/materialize")
def post_research_db_materialize(
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    return ok(research_db_materialize_service(limit=limit, dry_run=dry_run))


@app.get("/api/research-db/summary")
def get_research_db_summary() -> dict[str, Any]:
    return ok(research_db_summary_service())


@app.get("/api/research-db/trade-facts")
def get_research_db_trade_facts(
    strategy_line: str | None = None,
    source_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    return ok(
        research_db_trade_facts_service(
            strategy_line=strategy_line,
            source_type=source_type,
            limit=limit,
            offset=offset,
        )
    )


@app.get("/api/research-db/entry-features")
def get_research_db_entry_features(
    strategy_line: str | None = None,
    source_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    return ok(
        research_db_entry_features_service(
            strategy_line=strategy_line,
            source_type=source_type,
            limit=limit,
            offset=offset,
        )
    )


@app.get("/api/research-db/tq-samples")
def get_research_db_tq_samples(
    strategy_line: str | None = None,
    source_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    return ok(
        research_db_tq_samples_service(
            strategy_line=strategy_line,
            source_type=source_type,
            limit=limit,
            offset=offset,
        )
    )


@app.get("/api/research-db/dataset-cards")
def get_research_db_dataset_cards(limit: int = 20) -> dict[str, Any]:
    return ok(research_db_dataset_cards_service(limit=limit))


@app.get("/api/research-db/writer-status")
def get_research_db_writer_status() -> dict[str, Any]:
    return ok(research_db_writer_status_service())


@app.get("/api/research-db/field-coverage")
def get_research_db_field_coverage(
    strategy_line: str | None = None,
    source_type: str | None = None,
) -> dict[str, Any]:
    return ok(research_db_field_coverage_service(strategy_line=strategy_line, source_type=source_type))


@app.get("/api/research-db/lineage-audit")
def get_research_db_lineage_audit() -> dict[str, Any]:
    return ok(research_db_lineage_audit_service())


@app.get("/api/backtest/p21/v2/ops/footprint")
def get_backtest_p21_v2_ops_footprint(
    row_count_budget: int = 0,
    include_dbstat: bool = False,
) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_ops_footprint_service(
            row_count_budget=row_count_budget,
            include_dbstat=include_dbstat,
        )
    )


@app.post("/api/backtest/p21/v2/ops/retention-manifest")
def post_backtest_p21_v2_ops_retention_manifest(body: BacktestP21V2OpsRetentionRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_ops_retention_manifest_service(
            min_trade_count=body.min_trade_count,
            write=body.write,
            shadow_count_budget=body.shadow_count_budget,
        )
    )


@app.post("/api/backtest/p21/v2/ops/serving/rebuild")
def post_backtest_p21_v2_ops_serving_rebuild(body: BacktestP21V2OpsServingRebuildRequest) -> dict[str, Any]:
    return ok(backtest_p21_v2_ops_serving_rebuild_service(limit=body.limit))


@app.get("/api/backtest/p21/v2/ops/serving/summary")
def get_backtest_p21_v2_ops_serving_summary(limit: int = 50) -> dict[str, Any]:
    return ok(backtest_p21_v2_ops_serving_summary_service(limit=limit))


@app.get("/api/backtest/p21/v2/ops/tq-jobs")
def get_backtest_p21_v2_ops_tq_jobs(limit: int = 50) -> dict[str, Any]:
    return ok(backtest_p21_v2_ops_tq_jobs_service(limit=limit))


@app.post("/api/backtest/p21/v2/ops/tq-jobs/enqueue")
def post_backtest_p21_v2_ops_tq_job_enqueue(body: BacktestP21V2OpsTqMaterializeJobRequest) -> dict[str, Any]:
    return ok(backtest_p21_v2_ops_tq_job_enqueue_service(body.model_dump()))


@app.post("/api/backtest/p21/v2/ops/tq-jobs/process-next")
def post_backtest_p21_v2_ops_tq_job_process_next() -> dict[str, Any]:
    return ok(backtest_p21_v2_ops_tq_job_process_next_service())


@app.post("/api/backtest/p21/v2/ops/enhanced-validation")
def post_backtest_p21_v2_ops_enhanced_validation(body: BacktestP21V2OpsEnhancedValidationRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_ops_enhanced_validation_service(
            experiment_id=body.experiment_id,
            parameter_set_id=body.parameter_set_id,
            strategy_line=body.strategy_line,
            min_test_pf=body.min_test_pf,
            min_test_trade_count=body.min_test_trade_count,
            min_coverage=body.min_coverage,
        )
    )


@app.post("/api/backtest/p21/v2/ops/candidate-export")
def post_backtest_p21_v2_ops_candidate_export(body: BacktestP21V2OpsCandidateExportRequest) -> dict[str, Any]:
    return ok(
        backtest_p21_v2_ops_candidate_export_service(
            candidate_id=body.candidate_id,
            target_profile=body.target_profile,
        )
    )


@app.get("/api/strategy-sandbox/sandboxes")
def get_strategy_sandboxes(
    strategy_line: str | None = None,
    status: str | None = None,
    tag: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_list_service(strategy_line=strategy_line, status=status, tag=tag, limit=limit))
    except Exception as exc:
        return fail("strategy_sandbox_list_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes")
def post_strategy_sandbox(request: Request, body: StrategySandboxCreateRequest) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_create_service(_sandbox_mutation_payload(request, body.model_dump())))
    except Exception as exc:
        return fail("strategy_sandbox_create_failed", str(exc))


@app.get("/api/strategy-sandbox/universe")
def get_strategy_sandbox_universe(strategy_line: str = "all", sandbox_id: str | None = None) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_universe_service(strategy_line=strategy_line, sandbox_id=sandbox_id))
    except Exception as exc:
        return fail("strategy_sandbox_universe_failed", str(exc))


@app.get("/api/strategy-sandbox/external-integration/health")
def get_strategy_sandbox_external_integration_health() -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_external_integration_health_service())
    except Exception as exc:
        return fail("strategy_sandbox_external_integration_health_failed", str(exc))


@app.get("/api/strategy-sandbox/external-integration/runs/{run_id}")
def get_strategy_sandbox_external_integration_run(run_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_external_integration_run_service(run_id))
    except Exception as exc:
        return fail("strategy_sandbox_external_integration_run_failed", str(exc))


@app.get("/api/strategy-sandbox/external-integration/audit-events")
def get_strategy_sandbox_external_integration_audit_events(
    run_id: str | None = None,
    sandbox_id: str | None = None,
    candidate_id: str | None = None,
    gated_run_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    try:
        return ok(
            strategy_sandbox_external_integration_audit_events_service(
                run_id=run_id,
                sandbox_id=sandbox_id,
                candidate_id=candidate_id,
                gated_run_id=gated_run_id,
                limit=limit,
            )
        )
    except Exception as exc:
        return fail("strategy_sandbox_external_integration_audit_events_failed", str(exc))


@app.get("/api/strategy-sandbox/active")
def get_strategy_sandbox_active() -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_active_service())
    except Exception as exc:
        return fail("strategy_sandbox_active_failed", str(exc))


@app.get("/api/strategy-sandbox/resource-governor/status")
def get_strategy_sandbox_resource_governor_status() -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_resource_governor_status_service())
    except Exception as exc:
        return fail("strategy_sandbox_resource_governor_status_failed", str(exc))


@app.get("/api/strategy-sandbox/resource-governor/runs")
def get_strategy_sandbox_resource_governor_runs(
    resource_lane: str | None = None,
    sandbox_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_resource_governor_runs_service(resource_lane=resource_lane, sandbox_id=sandbox_id, limit=limit))
    except Exception as exc:
        return fail("strategy_sandbox_resource_governor_runs_failed", str(exc))


@app.get("/api/strategy-sandbox/resource-governor/runs/{run_id}")
def get_strategy_sandbox_resource_governor_run(run_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_resource_governor_run_service(run_id))
    except Exception as exc:
        return fail("strategy_sandbox_resource_governor_run_failed", str(exc))


@app.get("/api/strategy-sandbox/resource-governor/rest-budget")
def get_strategy_sandbox_resource_governor_rest_budget(
    requires_live_rest: bool = False,
    cache_hit: bool = False,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_resource_governor_rest_budget_service(requires_live_rest=requires_live_rest, cache_hit=cache_hit))
    except Exception as exc:
        return fail("strategy_sandbox_resource_governor_rest_budget_failed", str(exc))


@app.get("/api/strategy-sandbox/daemon-writers/status")
def get_strategy_sandbox_daemon_writers_status(run_id: str | None = None) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_daemon_writer_status_service(run_id=run_id))
    except Exception as exc:
        return fail("strategy_sandbox_daemon_writers_status_failed", str(exc))


@app.post("/api/strategy-sandbox/pipeline/run")
def post_strategy_sandbox_pipeline_run(body: StrategySandboxPipelineRunRequest) -> dict[str, Any]:
    try:
        payload = body.model_dump()
        payload.update({"source_surface": "fastapi", "caller_type": "local_ui"})
        return ok(strategy_sandbox_pipeline_run_service(payload))
    except Exception as exc:
        return fail("strategy_sandbox_pipeline_run_failed", str(exc))


@app.post("/api/strategy-sandbox/pipeline/full-run")
def post_strategy_sandbox_full_pipeline_run(body: StrategySandboxPipelineRunRequest) -> dict[str, Any]:
    try:
        payload = body.model_dump()
        payload.update({"source_surface": "fastapi", "caller_type": "local_ui"})
        return ok(strategy_sandbox_full_pipeline_run_service(payload))
    except Exception as exc:
        return fail("strategy_sandbox_full_pipeline_run_failed", str(exc))


@app.post("/api/strategy-sandbox/pipeline/stop")
def post_strategy_sandbox_pipeline_stop(body: StrategySandboxPipelineStopRequest) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_pipeline_stop_service(body.model_dump()))
    except Exception as exc:
        return fail("strategy_sandbox_pipeline_stop_failed", str(exc))


@app.put("/api/strategy-sandbox/active")
def put_strategy_sandbox_active(request: Request, body: StrategySandboxActiveRequest) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_set_active_service(body.sandbox_id, _sandbox_mutation_payload(request, body.model_dump())))
    except Exception as exc:
        return fail("strategy_sandbox_active_set_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}")
def get_strategy_sandbox(sandbox_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_get_service(sandbox_id))
    except Exception as exc:
        return fail("strategy_sandbox_get_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/summary")
def get_strategy_sandbox_summary(sandbox_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_summary_service(sandbox_id))
    except Exception as exc:
        return fail("strategy_sandbox_summary_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/full-backtest-runs")
def post_strategy_sandbox_full_backtest_run(
    request: Request,
    sandbox_id: str,
    body: StrategySandboxFullBacktestRunRequest,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_full_backtest_run_create_service(sandbox_id, _sandbox_mutation_payload(request, body.model_dump())))
    except Exception as exc:
        return fail("strategy_sandbox_full_backtest_run_create_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/full-backtest-runs/{run_id}")
def get_strategy_sandbox_full_backtest_run(sandbox_id: str, run_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_full_backtest_run_service(sandbox_id, run_id))
    except Exception as exc:
        return fail("strategy_sandbox_full_backtest_run_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/full-backtest-runs/{run_id}/cancel")
def post_strategy_sandbox_full_backtest_run_cancel(request: Request, sandbox_id: str, run_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_full_backtest_run_cancel_service(sandbox_id, run_id, _sandbox_mutation_payload(request, {})))
    except Exception as exc:
        return fail("strategy_sandbox_full_backtest_run_cancel_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/full-backtest-runs/{run_id}/resume")
def post_strategy_sandbox_full_backtest_run_resume(request: Request, sandbox_id: str, run_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_full_backtest_run_resume_service(sandbox_id, run_id, _sandbox_mutation_payload(request, {})))
    except Exception as exc:
        return fail("strategy_sandbox_full_backtest_run_resume_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches")
def get_strategy_sandbox_branches(sandbox_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_branches_service(sandbox_id))
    except Exception as exc:
        return fail("strategy_sandbox_branches_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/code-overlay")
def get_strategy_sandbox_code_overlay(sandbox_id: str, strategy_line: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_code_overlay_service(sandbox_id, strategy_line))
    except Exception as exc:
        return fail("strategy_sandbox_code_overlay_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/trade-candidates")
def get_strategy_sandbox_trade_candidates(
    sandbox_id: str,
    strategy_line: str,
    run_id: str | None = None,
    source_mode: str = "backtest",
    symbol: str | None = None,
    cursor: str | None = None,
    limit: int = 100,
    since: str | None = None,
    include_features: bool = True,
) -> dict[str, Any]:
    try:
        return ok(
            strategy_sandbox_trade_candidates_service(
                sandbox_id,
                strategy_line,
                run_id=run_id,
                source_mode=source_mode,
                symbol=symbol,
                cursor=cursor,
                limit=limit,
                since=since,
                include_features=include_features,
            )
        )
    except Exception as exc:
        message = str(exc)
        code = "feature_leakage_detected" if message.startswith("feature_leakage_detected") else "strategy_sandbox_trade_candidates_failed"
        return fail(code, message)


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/gate-actions")
def post_strategy_sandbox_gate_action(
    request: Request,
    sandbox_id: str,
    strategy_line: str,
    body: StrategySandboxGateActionRequest,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_gate_action_ingest_service(sandbox_id, strategy_line, _sandbox_mutation_payload(request, body.model_dump())))
    except Exception as exc:
        return fail("strategy_sandbox_gate_action_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/gated-replay")
def post_strategy_sandbox_gated_replay(
    request: Request,
    sandbox_id: str,
    strategy_line: str,
    body: StrategySandboxGatedRunRequest,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_gated_replay_service(sandbox_id, strategy_line, _sandbox_mutation_payload(request, body.model_dump())))
    except Exception as exc:
        return fail("strategy_sandbox_gated_replay_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/gated-paper-shadow")
def post_strategy_sandbox_gated_paper_shadow(
    request: Request,
    sandbox_id: str,
    strategy_line: str,
    body: StrategySandboxGatedRunRequest,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_gated_paper_shadow_service(sandbox_id, strategy_line, _sandbox_mutation_payload(request, body.model_dump())))
    except Exception as exc:
        return fail("strategy_sandbox_gated_paper_shadow_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/orders")
def get_strategy_sandbox_gated_orders(
    sandbox_id: str,
    strategy_line: str,
    run_id: str | None = None,
    gated_run_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_gated_orders_service(sandbox_id, strategy_line, run_id=run_id, gated_run_id=gated_run_id, limit=limit))
    except Exception as exc:
        return fail("strategy_sandbox_gated_orders_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/trade-quality-samples")
def get_strategy_sandbox_gated_trade_quality_samples(
    sandbox_id: str,
    strategy_line: str,
    gated_run_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_gated_trade_quality_samples_service(sandbox_id, strategy_line, gated_run_id=gated_run_id, limit=limit))
    except Exception as exc:
        return fail("strategy_sandbox_gated_trade_quality_samples_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/gated-performance")
def get_strategy_sandbox_gated_performance(
    sandbox_id: str,
    strategy_line: str,
    gated_run_id: str | None = None,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_gated_performance_service(sandbox_id, strategy_line, gated_run_id=gated_run_id))
    except Exception as exc:
        return fail("strategy_sandbox_gated_performance_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/code-overlay")
def post_strategy_sandbox_code_overlay(
    request: Request,
    sandbox_id: str,
    strategy_line: str,
    body: StrategySandboxRuntimeBuildRequest | None = None,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_create_code_overlay_service(sandbox_id, strategy_line, _sandbox_mutation_payload(request, body.model_dump() if body else {})))
    except Exception as exc:
        return fail("strategy_sandbox_code_overlay_create_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/code-patches")
def get_strategy_sandbox_code_patches(sandbox_id: str, strategy_line: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_code_overlay_service(sandbox_id, strategy_line))
    except Exception as exc:
        return fail("strategy_sandbox_code_patches_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/code-patches")
def post_strategy_sandbox_code_patch(
    request: Request,
    sandbox_id: str,
    strategy_line: str,
    body: StrategySandboxCodePatchRequest,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_add_code_patch_service(sandbox_id, strategy_line, _sandbox_mutation_payload(request, body.model_dump())))
    except Exception as exc:
        return fail("strategy_sandbox_code_patch_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/runtime")
def get_strategy_sandbox_runtime(sandbox_id: str, strategy_line: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_code_overlay_service(sandbox_id, strategy_line))
    except Exception as exc:
        return fail("strategy_sandbox_runtime_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/runtime/build")
def post_strategy_sandbox_runtime_build(
    request: Request,
    sandbox_id: str,
    strategy_line: str,
    body: StrategySandboxRuntimeBuildRequest | None = None,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_runtime_build_service(sandbox_id, strategy_line, _sandbox_mutation_payload(request, body.model_dump() if body else {})))
    except Exception as exc:
        return fail("strategy_sandbox_runtime_build_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/runtime/smoke")
def post_strategy_sandbox_runtime_smoke(
    request: Request,
    sandbox_id: str,
    strategy_line: str,
    body: StrategySandboxRuntimeBuildRequest | None = None,
) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_runtime_smoke_service(sandbox_id, strategy_line, _sandbox_mutation_payload(request, body.model_dump() if body else {})))
    except Exception as exc:
        return fail("strategy_sandbox_runtime_smoke_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/leaderboard")
def get_strategy_sandbox_leaderboard(sandbox_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_leaderboard_service(sandbox_id))
    except Exception as exc:
        return fail("strategy_sandbox_leaderboard_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/trade-quality/compare")
def get_strategy_sandbox_trade_quality_compare(sandbox_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_trade_quality_compare_service(sandbox_id))
    except Exception as exc:
        return fail("strategy_sandbox_tq_compare_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/gate/compare")
def get_strategy_sandbox_gate_compare(sandbox_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_gate_compare_service(sandbox_id))
    except Exception as exc:
        return fail("strategy_sandbox_gate_compare_failed", str(exc))


@app.get("/api/strategy-sandbox/sandboxes/{sandbox_id}/db-health")
def get_strategy_sandbox_db_health(sandbox_id: str) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_db_health_service(sandbox_id))
    except Exception as exc:
        return fail("strategy_sandbox_db_health_failed", str(exc))


@app.delete("/api/strategy-sandbox/sandboxes/{sandbox_id}")
def delete_strategy_sandbox(request: Request, sandbox_id: str, body: StrategySandboxDeleteRequest | None = None) -> dict[str, Any]:
    try:
        return ok(strategy_sandbox_delete_service(sandbox_id, _sandbox_mutation_payload(request, body.model_dump() if body else {})))
    except Exception as exc:
        return fail("strategy_sandbox_delete_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/branches/{strategy_line}/{job_type}")
def post_strategy_sandbox_branch_job(
    request: Request,
    sandbox_id: str,
    strategy_line: str,
    job_type: str,
    body: StrategySandboxJobRequest | None = None,
) -> dict[str, Any]:
    allowed = {
        "backtest",
        "replay",
        "trade-quality",
        "gate-search",
        "holdout",
        "config-export",
        "paper-shadow",
        "llm-export",
    }
    if job_type not in allowed:
        return fail("strategy_sandbox_invalid_job", f"invalid sandbox job_type: {job_type}")
    try:
        options = dict(body.options if body else {})
        if body:
            dumped = _sandbox_mutation_payload(request, body.model_dump())
            for key in ("caller_type", "caller_id", "source_surface", "caller_identity_source", "operation_policy", "audit_trace_id", "idempotency_key"):
                if key in dumped:
                    options[key] = dumped[key]
        else:
            options.update(_sandbox_mutation_payload(request, {}))
        options["strategy_line"] = strategy_line
        return ok(strategy_sandbox_job_service(sandbox_id, job_type, options))
    except Exception as exc:
        return fail("strategy_sandbox_branch_job_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/jobs/{job_type}")
def post_strategy_sandbox_multi_branch_job(
    request: Request,
    sandbox_id: str,
    job_type: str,
    body: StrategySandboxJobRequest | None = None,
) -> dict[str, Any]:
    allowed = {
        "backtest",
        "replay",
        "trade-quality",
        "gate-search",
        "holdout",
        "config-export",
        "paper-shadow",
        "llm-export",
    }
    if job_type not in allowed:
        return fail("strategy_sandbox_invalid_job", f"invalid sandbox job_type: {job_type}")
    try:
        options = dict(body.options if body else {})
        if body:
            dumped = _sandbox_mutation_payload(request, body.model_dump())
            for key in ("caller_type", "caller_id", "source_surface", "caller_identity_source", "operation_policy", "audit_trace_id", "idempotency_key"):
                if key in dumped:
                    options[key] = dumped[key]
        else:
            options.update(_sandbox_mutation_payload(request, {}))
        options.setdefault("strategy_line", "all")
        return ok(strategy_sandbox_job_service(sandbox_id, job_type, options))
    except Exception as exc:
        return fail("strategy_sandbox_multi_branch_job_failed", str(exc))


@app.post("/api/strategy-sandbox/sandboxes/{sandbox_id}/{job_type}")
def post_strategy_sandbox_job(request: Request, sandbox_id: str, job_type: str, body: StrategySandboxJobRequest | None = None) -> dict[str, Any]:
    allowed = {
        "backtest",
        "replay",
        "trade-quality",
        "gate-search",
        "holdout",
        "config-export",
        "paper-shadow",
        "llm-export",
    }
    if job_type not in allowed:
        return fail("strategy_sandbox_invalid_job", f"invalid sandbox job_type: {job_type}")
    try:
        options = dict(body.options if body else {})
        if body:
            dumped = _sandbox_mutation_payload(request, body.model_dump())
            for key in ("caller_type", "caller_id", "source_surface", "caller_identity_source", "operation_policy", "audit_trace_id", "idempotency_key"):
                if key in dumped:
                    options[key] = dumped[key]
        else:
            options.update(_sandbox_mutation_payload(request, {}))
        return ok(strategy_sandbox_job_service(sandbox_id, job_type, options))
    except Exception as exc:
        return fail("strategy_sandbox_job_failed", str(exc))


@app.get("/api/trade-quality/diagnostics/summary")
def get_trade_quality_diagnostics_summary(
    source: str | None = None,
    archive_id: str | None = None,
    strategy_line: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    exit_reason: str | None = None,
    root_cause: str | None = None,
    quality_tag: str | None = None,
    replay_status: str | None = None,
    entry_quality_label: str | None = None,
    entry_quality_v2_label: str | None = None,
    microstructure_coverage: str | None = None,
    market_context_label: str | None = None,
    market_context_status: str | None = None,
    entry_context_v3_label: str | None = None,
    funding_regime: str | None = None,
    oi_direction: str | None = None,
    btc_alignment: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any]:
    return ok(
        trade_quality_diagnostics_summary_service(
            source=source,
            archive_id=archive_id,
            strategy_line=strategy_line,
            symbol=symbol,
            side=side,
            exit_reason=exit_reason,
            root_cause=root_cause,
            quality_tag=quality_tag,
            replay_status=replay_status,
            entry_quality_label=entry_quality_label,
            entry_quality_v2_label=entry_quality_v2_label,
            microstructure_coverage=microstructure_coverage,
            market_context_label=market_context_label,
            market_context_status=market_context_status,
            entry_context_v3_label=entry_context_v3_label,
            funding_regime=funding_regime,
            oi_direction=oi_direction,
            btc_alignment=btc_alignment,
            date_from=date_from,
            date_to=date_to,
        )
    )


@app.get("/api/trade-quality/diagnostics/archive-packages")
def get_trade_quality_diagnostics_archive_packages() -> dict[str, Any]:
    return ok(trade_quality_diagnostics_archive_packages_service())


@app.get("/api/trade-quality/diagnostics/sync-status")
def get_trade_quality_diagnostics_sync_status() -> dict[str, Any]:
    return ok(trade_quality_diagnostics_sync_status_service())


@app.get("/api/trade-quality/diagnostics/samples")
def get_trade_quality_diagnostics_samples(
    source: str | None = None,
    archive_id: str | None = None,
    strategy_line: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    exit_reason: str | None = None,
    root_cause: str | None = None,
    quality_tag: str | None = None,
    replay_status: str | None = None,
    entry_quality_label: str | None = None,
    entry_quality_v2_label: str | None = None,
    microstructure_coverage: str | None = None,
    market_context_label: str | None = None,
    market_context_status: str | None = None,
    entry_context_v3_label: str | None = None,
    funding_regime: str | None = None,
    oi_direction: str | None = None,
    btc_alignment: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    return ok(
        trade_quality_diagnostics_samples_service(
            source=source,
            archive_id=archive_id,
            strategy_line=strategy_line,
            symbol=symbol,
            side=side,
            exit_reason=exit_reason,
            root_cause=root_cause,
            quality_tag=quality_tag,
            replay_status=replay_status,
            entry_quality_label=entry_quality_label,
            entry_quality_v2_label=entry_quality_v2_label,
            microstructure_coverage=microstructure_coverage,
            market_context_label=market_context_label,
            market_context_status=market_context_status,
            entry_context_v3_label=entry_context_v3_label,
            funding_regime=funding_regime,
            oi_direction=oi_direction,
            btc_alignment=btc_alignment,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )
    )


@app.get("/api/trade-quality/diagnostics/samples/{trade_id}")
def get_trade_quality_diagnostics_sample_detail(trade_id: str) -> dict[str, Any]:
    return ok(trade_quality_diagnostics_sample_detail_service(trade_id))


@app.get("/api/trade-quality/diagnostics/aggregates")
def get_trade_quality_diagnostics_aggregates(
    source: str | None = None,
    archive_id: str | None = None,
    strategy_line: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    exit_reason: str | None = None,
    root_cause: str | None = None,
    quality_tag: str | None = None,
    replay_status: str | None = None,
    entry_quality_label: str | None = None,
    entry_quality_v2_label: str | None = None,
    microstructure_coverage: str | None = None,
    market_context_label: str | None = None,
    market_context_status: str | None = None,
    entry_context_v3_label: str | None = None,
    funding_regime: str | None = None,
    oi_direction: str | None = None,
    btc_alignment: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any]:
    return ok(
        trade_quality_diagnostics_aggregates_service(
            source=source,
            archive_id=archive_id,
            strategy_line=strategy_line,
            symbol=symbol,
            side=side,
            exit_reason=exit_reason,
            root_cause=root_cause,
            quality_tag=quality_tag,
            replay_status=replay_status,
            entry_quality_label=entry_quality_label,
            entry_quality_v2_label=entry_quality_v2_label,
            microstructure_coverage=microstructure_coverage,
            market_context_label=market_context_label,
            market_context_status=market_context_status,
            entry_context_v3_label=entry_context_v3_label,
            funding_regime=funding_regime,
            oi_direction=oi_direction,
            btc_alignment=btc_alignment,
            date_from=date_from,
            date_to=date_to,
        )
    )


@app.get("/api/trade-quality/diagnostics/replay-ledger")
def get_trade_quality_diagnostics_replay_ledger(limit: int = 200) -> dict[str, Any]:
    return ok(trade_quality_diagnostics_replay_ledger_service(limit=limit))


@app.post("/api/trade-quality/diagnostics/sync/dry-run")
def post_trade_quality_diagnostics_sync_dry_run(limit: int | None = None, source: str = "all") -> dict[str, Any]:
    return ok(trade_quality_diagnostics_sync_service(dry_run=True, limit=limit, source=source))


@app.post("/api/trade-quality/diagnostics/sync/run")
def post_trade_quality_diagnostics_sync_run(limit: int | None = None, source: str = "all") -> dict[str, Any]:
    return ok(trade_quality_diagnostics_sync_service(dry_run=False, limit=limit, source=source))


@app.post("/api/trade-quality/diagnostics/replay/dry-run")
def post_trade_quality_diagnostics_replay_dry_run(
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
) -> dict[str, Any]:
    return ok(trade_quality_diagnostics_replay_service(dry_run=True, limit=limit, source=source, archive_id=archive_id))


@app.post("/api/trade-quality/diagnostics/replay/run")
def post_trade_quality_diagnostics_replay_run(
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
) -> dict[str, Any]:
    return ok(trade_quality_diagnostics_replay_service(dry_run=False, limit=limit, source=source, archive_id=archive_id))


@app.post("/api/trade-quality/entry-features/backfill/dry-run")
def post_trade_quality_entry_features_backfill_dry_run(
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return ok(
        trade_quality_entry_features_backfill_service(
            dry_run=True,
            limit=limit,
            source=source,
            archive_id=archive_id,
            force=force,
        )
    )


@app.post("/api/trade-quality/entry-features/backfill/run")
def post_trade_quality_entry_features_backfill_run(
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return ok(
        trade_quality_entry_features_backfill_service(
            dry_run=False,
            limit=limit,
            source=source,
            archive_id=archive_id,
            force=force,
        )
    )


@app.post("/api/trade-quality/entry-microstructure/backfill/dry-run")
def post_trade_quality_entry_microstructure_backfill_dry_run(
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
    evidence_window_sec: int = 180,
) -> dict[str, Any]:
    return ok(
        trade_quality_entry_microstructure_backfill_service(
            dry_run=True,
            limit=limit,
            source=source,
            archive_id=archive_id,
            force=force,
            evidence_window_sec=evidence_window_sec,
        )
    )


@app.post("/api/trade-quality/entry-microstructure/backfill/run")
def post_trade_quality_entry_microstructure_backfill_run(
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
    evidence_window_sec: int = 180,
) -> dict[str, Any]:
    return ok(
        trade_quality_entry_microstructure_backfill_service(
            dry_run=False,
            limit=limit,
            source=source,
            archive_id=archive_id,
            force=force,
            evidence_window_sec=evidence_window_sec,
        )
    )


@app.post("/api/trade-quality/entry-market-context/backfill/dry-run")
def post_trade_quality_entry_market_context_backfill_dry_run(
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return ok(
        trade_quality_entry_market_context_backfill_service(
            dry_run=True,
            limit=limit,
            source=source,
            archive_id=archive_id,
            force=force,
        )
    )


@app.post("/api/trade-quality/entry-market-context/backfill/run")
def post_trade_quality_entry_market_context_backfill_run(
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return ok(
        trade_quality_entry_market_context_backfill_service(
            dry_run=False,
            limit=limit,
            source=source,
            archive_id=archive_id,
            force=force,
        )
    )


@app.post("/api/trade-quality/entry-context-v3/backfill/dry-run")
def post_trade_quality_entry_context_v3_backfill_dry_run(
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return ok(
        trade_quality_entry_context_v3_backfill_service(
            dry_run=True,
            limit=limit,
            source=source,
            archive_id=archive_id,
            force=force,
        )
    )


@app.post("/api/trade-quality/entry-context-v3/backfill/run")
def post_trade_quality_entry_context_v3_backfill_run(
    limit: int | None = None,
    source: str = "all",
    archive_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return ok(
        trade_quality_entry_context_v3_backfill_service(
            dry_run=False,
            limit=limit,
            source=source,
            archive_id=archive_id,
            force=force,
        )
    )


@app.post("/api/trade-quality/diagnostics/refresh-enrich")
def post_trade_quality_diagnostics_refresh_enrich(
    limit: int | None = 100,
    source: str = "current_paper",
    archive_id: str | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    return ok(
        trade_quality_diagnostics_refresh_enrich_service(
            dry_run=dry_run,
            limit=limit,
            source=source,
            archive_id=archive_id,
            force=force,
        )
    )


@app.get("/api/trade-quality/summary")
def get_trade_quality_summary(
    strategy_line: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    sample_source: str | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    root_cause: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        trade_quality_payload(
            "summary",
            strategy_line=strategy_line,
            symbol=symbol,
            side=side,
            sample_source=sample_source,
            run_id=run_id,
            cycle_id=cycle_id,
            root_cause=root_cause,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/samples")
def get_trade_quality_samples(
    strategy_line: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    sample_source: str | None = None,
    run_id: str | None = None,
    cycle_id: str | None = None,
    root_cause: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        trade_quality_payload(
            "samples",
            strategy_line=strategy_line,
            symbol=symbol,
            side=side,
            sample_source=sample_source,
            run_id=run_id,
            cycle_id=cycle_id,
            root_cause=root_cause,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/root-causes")
def get_trade_quality_root_causes(root_cause: str | None = None, limit: int = 200) -> dict[str, Any]:
    return ok(trade_quality_payload("root-causes", root_cause=root_cause, limit=limit))


@app.get("/api/trade-quality/clusters")
def get_trade_quality_clusters(strategy_line: str | None = None, root_cause: str | None = None, limit: int = 200) -> dict[str, Any]:
    return ok(trade_quality_payload("clusters", strategy_line=strategy_line, root_cause=root_cause, limit=limit))


@app.get("/api/trade-quality/recommendations")
def get_trade_quality_recommendations(limit: int = 200) -> dict[str, Any]:
    return ok(trade_quality_payload("recommendations", limit=limit))


@app.get("/api/trade-quality/order/{order_id}")
def get_trade_quality_order(order_id: str) -> dict[str, Any]:
    return ok(trade_quality_payload("order", order_id=order_id, limit=1))


@app.get("/api/trade-quality/recommendation-rules")
def get_trade_quality_recommendation_rules(
    rule_type: str | None = None,
    strategy_line: str | None = None,
    side: str | None = None,
    symbol: str | None = None,
    sample_source: str | None = None,
    config_profile: str | None = None,
    severity: str | None = None,
    mode: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        trade_quality_recommendation_rules_service(
            rule_type=rule_type,
            strategy_line=strategy_line,
            side=side,
            symbol=symbol,
            sample_source=sample_source,
            config_profile=config_profile,
            severity=severity,
            mode=mode,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/recommendation-rules/summary")
def get_trade_quality_recommendation_rules_summary() -> dict[str, Any]:
    return ok(trade_quality_recommendation_rules_service(limit=1))


@app.post("/api/trade-quality/recommendation-rules/rebuild")
def post_trade_quality_recommendation_rules_rebuild() -> dict[str, Any]:
    return ok(trade_quality_recommendation_rules_rebuild_service())


@app.get("/api/trade-quality/recommendation-validation")
def get_trade_quality_recommendation_validation(
    sample_source: str | None = "live",
    rule_type: str | None = None,
    strategy_line: str | None = None,
    side: str | None = None,
    symbol: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    return ok(
        trade_quality_recommendation_validation_service(
            sample_source=sample_source,
            rule_type=rule_type,
            strategy_line=strategy_line,
            side=side,
            symbol=symbol,
            limit=limit,
        )
    )


@app.get("/api/trade-quality/recommendation-promotions")
def get_trade_quality_recommendation_promotions(
    profile: str | None = None,
    strategy_line: str | None = None,
    enabled: bool | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return ok(
        trade_quality_recommendation_promotions_service(
            profile=profile,
            strategy_line=strategy_line,
            enabled=enabled,
            limit=limit,
        )
    )


@app.post("/api/trade-quality/recommendation-promotions/dry-run")
def post_trade_quality_recommendation_promotion_dry_run(req: TradeQualityPromotionRequest) -> dict[str, Any]:
    return ok(
        trade_quality_recommendation_promotion_dry_run_service(
            rule_id=req.rule_id,
            profile=req.profile,
            strategy_line=req.strategy_line,
            mode=req.mode,
        )
    )


@app.post("/api/trade-quality/recommendation-promotions/apply")
def post_trade_quality_recommendation_promotion_apply(req: TradeQualityPromotionRequest) -> dict[str, Any]:
    return ok(
        trade_quality_recommendation_promotion_apply_service(
            rule_id=req.rule_id,
            profile=req.profile,
            strategy_line=req.strategy_line,
            mode=req.mode,
            reason=req.reason,
        )
    )


@app.post("/api/trade-quality/recommendation-promotions/disable")
def post_trade_quality_recommendation_promotion_disable(req: TradeQualityPromotionDisableRequest) -> dict[str, Any]:
    return ok(trade_quality_recommendation_promotion_disable_service(promotion_id=req.promotion_id, reason=req.reason))


@app.get("/api/trade-quality/promotion-candidates")
def get_trade_quality_promotion_candidates(limit: int = 200) -> dict[str, Any]:
    return ok(trade_quality_promotion_candidates_service(limit=limit))


@app.post("/api/trade-quality/promotion-candidates/rebuild")
def post_trade_quality_promotion_candidates_rebuild(limit: int = 200, write: bool = True) -> dict[str, Any]:
    return ok(trade_quality_promotion_candidates_rebuild_service(limit=limit, write=write))


@app.get("/api/trade-quality/ingest-ledger")
def get_trade_quality_ingest_ledger(limit: int = 200) -> dict[str, Any]:
    return ok(trade_quality_ingest_ledger_payload(limit=limit))


@app.post("/api/trade-quality/archive-backfill/dry-run")
def post_trade_quality_archive_backfill_dry_run(limit: int | None = None) -> dict[str, Any]:
    return ok(trade_quality_archive_backfill_service(dry_run=True, limit=limit))


@app.post("/api/trade-quality/archive-backfill/run")
def post_trade_quality_archive_backfill_run(limit: int | None = None) -> dict[str, Any]:
    return ok(trade_quality_archive_backfill_service(dry_run=False, limit=limit))


@app.get("/api/trade-quality/replay-backfill/ledger")
def get_trade_quality_replay_backfill_ledger(limit: int = 200) -> dict[str, Any]:
    return ok(trade_quality_replay_ledger_payload(limit=limit))


@app.post("/api/trade-quality/replay-backfill/dry-run")
def post_trade_quality_replay_backfill_dry_run(limit: int | None = None, sample_source: str = "all") -> dict[str, Any]:
    return ok(trade_quality_replay_backfill_service(dry_run=True, limit=limit, sample_source=sample_source))


@app.post("/api/trade-quality/replay-backfill/run")
def post_trade_quality_replay_backfill_run(limit: int | None = None, sample_source: str = "all") -> dict[str, Any]:
    return ok(trade_quality_replay_backfill_service(dry_run=False, limit=limit, sample_source=sample_source))


@app.get("/api/notifications/feishu/config")
def get_feishu_config() -> dict[str, Any]:
    return ok(feishu_config())


@app.put("/api/notifications/feishu/config")
def put_feishu_config(req: ConfigUpdateRequest) -> dict[str, Any]:
    return ok(update_config_section("feishu", req.values))


@app.post("/api/notifications/feishu/test")
def post_feishu_test(req: FeishuTestRequest) -> dict[str, Any]:
    if req.mock:
        return ok(
            {
                "status": "mock_sent",
                "message": req.message,
                "attempted_at": to_iso_z(utc_now()),
            },
        )
    return ok({"status": "not_sent", "reason": "real Feishu sending is owned by P15"})


@app.post("/api/notifications/feishu/send-trade-plans")
def post_feishu_send_trade_plans(req: FeishuSendTradePlansRequest) -> dict[str, Any]:
    return ok(feishu_send_trade_plans(mock_signals=req.mock_signals, mock_send=req.mock_send))


@app.get("/api/notifications/deliveries")
def get_notification_deliveries(
    type: str | None = None,
    status: str | None = None,
    line: str | None = None,
) -> dict[str, Any]:
    return ok({"deliveries": notification_deliveries(event_type=type, status=status, line=line)})


@app.get("/api/notifications/deliveries/latest")
def get_latest_notification_delivery() -> dict[str, Any]:
    rows = notification_deliveries()
    return ok({"delivery": rows[-1] if rows else None})
