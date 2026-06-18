from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


STRATEGY_LINES = ("without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6")


class ApiError(BaseModel):
    code: str
    message: str
    detail: dict[str, Any] = Field(default_factory=dict)


class ApiResponse(BaseModel):
    ok: bool
    data: Any = None
    error: ApiError | None = None
    source_path: str | None = None
    generated_at: str | None = None


def ok(
    data: Any,
    *,
    source_path: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    return ApiResponse(
        ok=True,
        data=data,
        error=None,
        source_path=source_path,
        generated_at=generated_at,
    ).model_dump()


def fail(code: str, message: str, detail: dict[str, Any] | None = None) -> dict[str, Any]:
    return ApiResponse(
        ok=False,
        data=None,
        error=ApiError(code=code, message=message, detail=detail or {}),
    ).model_dump()


class PipelineRunRequest(BaseModel):
    line: str = Field(default="all", pattern="^(without_micro|micro_fast|micro_full|strategy4|strategy5|strategy6|all)$")
    lines: list[str] | None = None
    mode: str = Field(default="once", pattern="^(once|interval)$")
    interval_sec: int | None = Field(default=None, ge=30, le=86400)
    max_cycles: int | None = Field(default=None, ge=1, le=1000)
    force_universe: bool = False
    skip_micro_wait: bool = False
    skip_market_context: bool = False
    skip_abc_audit: bool = False
    skip_json_stage_audit: bool = False
    skip_aggregate_final_decisions: bool = False

    @model_validator(mode="after")
    def _validate_lines(self) -> "PipelineRunRequest":
        if self.lines is None:
            return self
        seen: list[str] = []
        for item in self.lines:
            value = str(item).strip()
            if value == "all":
                seen = list(STRATEGY_LINES)
                break
            if value not in STRATEGY_LINES:
                raise ValueError(f"invalid strategy line: {value}")
            if value not in seen:
                seen.append(value)
        if not seen:
            raise ValueError("at least one strategy line must be selected")
        self.lines = [line for line in STRATEGY_LINES if line in seen]
        return self


class ConfigUpdateRequest(BaseModel):
    values: dict[str, Any]


class TradeQualityPromotionRequest(BaseModel):
    rule_id: str
    profile: str = "relaxed_profit"
    strategy_line: str | None = None
    mode: str = Field(default="wait_only", pattern="^(wait_only|block_executable)$")
    reason: str = "manual_review"


class TradeQualityPromotionDisableRequest(BaseModel):
    promotion_id: str
    reason: str = "manual_disable"


class BacktestP21MatrixRequest(BaseModel):
    source: str = Field(default="all", pattern="^(all|current_paper|archive)$")
    archive_id: str | None = None
    strategy_line: str = Field(default="all", pattern="^(without_micro|strategy4|strategy5|strategy6|all)$")
    limit: int = Field(default=5000, ge=1, le=20000)
    max_sets: int = Field(default=120, ge=1, le=500)
    write: bool = True
    parameter_grid: list[dict[str, Any]] | None = None


class BacktestP21ExportRequest(BaseModel):
    experiment_id: str
    parameter_set_id: str | None = None


class BacktestP21V2KlineDownloadRequest(BaseModel):
    symbols: list[str] | None = None
    days: int = Field(default=30, ge=1, le=90)
    max_symbols: int = Field(default=10, ge=1, le=600)
    dry_run: bool = False
    sleep_sec: float = Field(default=0.05, ge=0, le=5)


class BacktestP21V2MatrixRequest(BaseModel):
    symbols: list[str] | None = None
    strategy_line: str = Field(default="all", pattern="^(without_micro|strategy4|strategy5|strategy6|all)$")
    days: int = Field(default=30, ge=1, le=90)
    max_symbols: int = Field(default=20, ge=1, le=600)
    max_sets: int = Field(default=120, ge=1, le=1000)
    write: bool = True
    parameter_grid: list[dict[str, Any]] | None = None


class BacktestP21V2Strategy4ReplayRunRequest(BaseModel):
    symbols: list[str] | None = None
    days: int = Field(default=3, ge=1, le=30)
    max_symbols: int = Field(default=5, ge=1, le=50)
    max_sets: int = Field(default=1, ge=1, le=20)
    max_admissions_per_symbol: int = Field(default=20, ge=1, le=500)
    max_attempts: int = Field(default=12, ge=1, le=288)
    observe_interval_min: int = Field(default=5, ge=1, le=60)
    write: bool = True


class BacktestP21V2JobStartRequest(BaseModel):
    job_type: str = Field(default="matrix_backtest", pattern="^(kline_download|matrix_backtest)$")
    symbols: list[str] | None = None
    strategy_line: str = Field(default="all", pattern="^(without_micro|strategy4|strategy5|strategy6|all)$")
    days: int = Field(default=30, ge=1, le=90)
    max_symbols: int = Field(default=20, ge=1, le=600)
    max_sets: int = Field(default=120, ge=1, le=5000)
    symbol_shard_size: int = Field(default=25, ge=1, le=200)
    max_workers: int = Field(default=1, ge=1, le=32)
    scheduler_mode: str = Field(default="parameter_batch", pattern="^(parameter_batch|global_queue)$")
    resume_experiment_id: str | None = None
    sleep_sec: float = Field(default=0.6, ge=0, le=10)


class BacktestP21V2QualityMaterializeRequest(BaseModel):
    experiment_id: str
    strategy_line: str | None = None
    parameter_set_id: str | None = None
    top_n: int = Field(default=1, ge=1, le=20)
    limit: int = Field(default=5000, ge=1, le=200000)
    dry_run: bool = True
    force: bool = False


class BacktestP21V2GateBatchMaterializeRequest(BaseModel):
    experiment_id: str | None = None
    strategy_line: str = Field(default="all", pattern="^(without_micro|strategy4|strategy5|strategy6|all)$")
    top_n: int = Field(default=5, ge=1, le=30)
    limit: int = Field(default=500, ge=1, le=5000)
    dry_run: bool = True


class BacktestP21V2GateBuildRequest(BaseModel):
    experiment_id: str | None = None
    strategy_line: str = Field(default="all", pattern="^(without_micro|strategy4|strategy5|strategy6|all)$")
    parameter_set_id: str | None = None
    limit: int = Field(default=5000, ge=1, le=100000)
    min_samples: int = Field(default=5, ge=1, le=500)
    min_test_pf: float = Field(default=1.0, ge=0, le=1000)
    min_coverage: float = Field(default=0.05, ge=0, le=1)
    dry_run: bool = True


class BacktestP21V2OpsRetentionRequest(BaseModel):
    min_trade_count: int = Field(default=30, ge=1, le=100000)
    write: bool = True
    shadow_count_budget: int = Field(default=75000, ge=1000, le=1000000)


class BacktestP21V2OpsServingRebuildRequest(BaseModel):
    limit: int = Field(default=200, ge=1, le=1000)


class BacktestP21V2OpsTqMaterializeJobRequest(BaseModel):
    source_type: str = "backtest"
    experiment_id: str
    sandbox_id: str | None = None
    strategy_line: str | None = None
    parameter_set_id: str | None = None
    top_n: int = Field(default=1, ge=1, le=30)
    limit: int = Field(default=5000, ge=1, le=200000)
    min_samples: int = Field(default=50, ge=1, le=500)
    gate_limit: int = Field(default=120, ge=1, le=500)
    include_v5: bool = True
    include_gates: bool = True
    dry_run: bool = True
    force: bool = False


class BacktestP21V2OpsEnhancedValidationRequest(BaseModel):
    experiment_id: str | None = None
    parameter_set_id: str | None = None
    strategy_line: str | None = None
    min_test_pf: float = Field(default=1.05, ge=0, le=1000)
    min_test_trade_count: int = Field(default=100, ge=1, le=100000)
    min_coverage: float = Field(default=0.10, ge=0, le=1)


class BacktestP21V2OpsCandidateExportRequest(BaseModel):
    candidate_id: str
    target_profile: str = "review_only"


class StrategySandboxCreateRequest(BaseModel):
    strategy_line: str = Field(default="experiment", pattern="^(strategy1|strategy2|strategy3|without_micro|micro_fast|micro_full|strategy4|strategy5|strategy6|experiment)$")
    strategy_lines: list[str] | None = None
    strategy_version: str = "review"
    data_scope: dict[str, Any] = Field(default_factory=dict)
    config_scope: dict[str, Any] = Field(default_factory=dict)
    source_refs: list[str] = Field(default_factory=list)
    storage_policy: dict[str, Any] = Field(default_factory=dict)
    llm_training_policy: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    caller_type: str = "local_ui"
    caller_id: str = "fastapi"
    source_surface: str = "fastapi"
    operation_policy: dict[str, Any] = Field(default_factory=dict)
    audit_trace_id: str | None = None

    @model_validator(mode="after")
    def _validate_strategy_lines(self) -> "StrategySandboxCreateRequest":
        if self.strategy_lines is None:
            return self
        seen: list[str] = []
        for item in self.strategy_lines:
            value = str(item).strip()
            aliases = {"strategy1": "without_micro", "strategy2": "micro_fast", "strategy3": "micro_full"}
            value = aliases.get(value, value)
            if value not in {"without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"}:
                raise ValueError(f"invalid sandbox strategy branch: {value}")
            if value not in seen:
                seen.append(value)
        if not seen:
            raise ValueError("strategy_lines cannot be empty")
        self.strategy_lines = seen
        return self


class StrategySandboxActiveRequest(BaseModel):
    sandbox_id: str | None = None
    caller_type: str = "local_ui"
    caller_id: str = "fastapi"
    source_surface: str = "fastapi"
    operation_policy: dict[str, Any] = Field(default_factory=dict)
    audit_trace_id: str | None = None


class StrategySandboxJobRequest(BaseModel):
    options: dict[str, Any] = Field(default_factory=dict)


class StrategySandboxPipelineRunRequest(BaseModel):
    sandbox_id: str | None = None
    dry_run: bool = True
    requires_live_rest: bool = False
    cache_hit: bool = False
    options: dict[str, Any] = Field(default_factory=dict)


class StrategySandboxPipelineStopRequest(BaseModel):
    run_id: str | None = None
    cancel_reason: str = "manual_stop"
    caller_type: str = "local_ui"
    caller_id: str = "fastapi"
    source_surface: str = "fastapi"
    operation_policy: dict[str, Any] = Field(default_factory=dict)
    audit_trace_id: str | None = None


class StrategySandboxFullBacktestRunRequest(BaseModel):
    strategy_line: str = Field(default="all", pattern="^(strategy1|strategy2|strategy3|without_micro|micro_fast|micro_full|strategy4|strategy5|strategy6|all)$")
    symbols: list[str] | None = None
    time_start: str | None = None
    time_end: str | None = None
    timeframe: str = Field(default="1m", pattern="^(1m|3m|5m|15m|1h|4h|1d)$")
    bar_source: str = "historical_kline_cache_or_binance"
    batch_size: int = Field(default=25, ge=1, le=200)
    resource_budget: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None
    caller_type: str = "local_ui"
    caller_id: str = "fastapi"
    source_surface: str = "fastapi"
    operation_policy: dict[str, Any] = Field(default_factory=dict)
    audit_trace_id: str | None = None

    @model_validator(mode="after")
    def _normalize_strategy_alias(self) -> "StrategySandboxFullBacktestRunRequest":
        aliases = {"strategy1": "without_micro", "strategy2": "micro_fast", "strategy3": "micro_full"}
        self.strategy_line = aliases.get(self.strategy_line, self.strategy_line)
        return self


class StrategySandboxGateActionRequest(BaseModel):
    run_id: str
    candidate_id: str
    unit_id: str
    unit_version: str
    selection_id: str | None = None
    scorer_output_ref: str | None = None
    final_gate_decision_ref: str | None = None
    gate_decision: str = Field(pattern="^(allow|block|reduce_size|review)$")
    gate_action_payload: dict[str, Any] = Field(default_factory=dict)
    reason_codes: list[str] = Field(default_factory=list)
    audit_trace_id: str | None = None
    idempotency_key: str
    created_at: str | None = None
    caller_type: str = "external_ai_trader"
    caller_id: str = "external_ai_trader"
    source_surface: str = "external_connector"
    operation_policy: dict[str, Any] = Field(default_factory=dict)


class StrategySandboxGatedRunRequest(BaseModel):
    run_id: str | None = None
    baseline_run_id: str | None = None
    gate_action_batch_id: str | None = None
    execution_policy: dict[str, Any] = Field(default_factory=dict)
    requested_by: str = "external_ai_trader"
    reason: str = "external_gated_smoke"
    caller_type: str = "external_ai_trader"
    caller_id: str = "external_ai_trader"
    source_surface: str = "external_connector"
    operation_policy: dict[str, Any] = Field(default_factory=dict)
    audit_trace_id: str | None = None


class StrategySandboxCodePatchRequest(BaseModel):
    patch_type: str = "manifest_note"
    target_relpath: str = "notes/patch.md"
    patch_json: dict[str, Any] = Field(default_factory=dict)
    diff_text: str = ""
    author: str = "codex"
    content: str | None = None
    note: str | None = None
    caller_type: str = "local_ui"
    caller_id: str = "fastapi"
    source_surface: str = "fastapi"
    operation_policy: dict[str, Any] = Field(default_factory=dict)
    audit_trace_id: str | None = None


class StrategySandboxRuntimeBuildRequest(BaseModel):
    code_overlay_id: str | None = None
    code_patch_id: str | None = None
    symbols: list[str] = Field(default_factory=list)
    caller_type: str = "local_ui"
    caller_id: str = "fastapi"
    source_surface: str = "fastapi"
    operation_policy: dict[str, Any] = Field(default_factory=dict)
    audit_trace_id: str | None = None


class StrategySandboxDeleteRequest(BaseModel):
    mode: str = Field(default="soft_delete", pattern="^(soft_delete|purge)$")
    reason: str = ""
    confirm: bool = False
    caller_type: str = "local_ui"
    caller_id: str = "fastapi"
    source_surface: str = "fastapi"
    operation_policy: dict[str, Any] = Field(default_factory=dict)
    audit_trace_id: str | None = None


class FeishuTestRequest(BaseModel):
    message: str = "P15 Feishu test message"
    mock: bool = True


class FeishuSendTradePlansRequest(BaseModel):
    mock_signals: bool = False
    mock_send: bool = False
