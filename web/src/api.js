const API_BASE = import.meta.env.VITE_API_BASE || "";

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {})
    },
    ...options
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload.ok === false) {
    const message = payload?.error?.message || `API request failed: ${response.status}`;
    throw new Error(message);
  }
  return payload.data;
}

export const api = {
  health: () => request("/api/health"),
  config: () => request("/api/config"),
  configProfiles: () => request("/api/config/profiles"),
  configFieldImpactMap: () => request("/api/config/field-impact-map"),
  configFieldImpactSummary: () => request("/api/config/field-impact-summary"),
  configEffective: (strategyLine) => request(`/api/config/effective?strategy_line=${encodeURIComponent(strategyLine)}`),
  configUiSchema: () => request("/api/config/ui-schema"),
  configLegacyFields: () => request("/api/config/legacy-fields"),
  validateConfig: (section, values) =>
    request(`/api/config/${section}/validate`, {
      method: "POST",
      body: JSON.stringify({ values })
    }),
  updateConfig: (section, values) =>
    request(`/api/config/${section}`, {
      method: "PUT",
      body: JSON.stringify({ values })
    }),
  applyConfigProfile: (name) => request(`/api/config/profiles/${name}/apply`, { method: "POST" }),
  reloadConfig: () => request("/api/config/reload", { method: "POST" }),
  microStatus: () => request("/api/micro-daemon/status"),
  microStart: () => request("/api/micro-daemon/start", { method: "POST" }),
  microStop: () => request("/api/micro-daemon/stop", { method: "POST" }),
  pipelineStatus: () => request("/api/pipeline/status/latest"),
  pipelineStatusLite: () => request("/api/pipeline/status-lite"),
  pipelineWatchdog: () => request("/api/pipeline/watchdog"),
  pipelineFunnelLatest: (refresh = true) => request(`/api/pipeline/funnel/latest?refresh=${refresh ? "true" : "false"}`),
  pipelineFunnelHistory: (limit = 50) => request(`/api/pipeline/funnel/history?limit=${limit}`),
  pipelineRun: (body) =>
    request("/api/pipeline/run", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  pipelineStop: () => request("/api/pipeline/stop", { method: "POST" }),
  runtimeStatus: () => request("/api/runtime/status"),
  runtimeStatusLite: () => request("/api/runtime/status-lite"),
  restHealth: () => request("/api/runtime/rest-health"),
  runtimeRestBudget: () => request("/api/runtime/rest-budget"),
  step15Daemon: () => request("/api/runtime/step15-daemon"),
  step15DaemonHealth: () => request("/api/runtime/step15-daemon/health"),
  runtimeWarmup: () => request("/api/runtime/warmup"),
  step15SnapshotQualityLatest: () => request("/api/audit/step15/snapshot-quality/latest"),
  step15SnapshotQualityByRun: (runId) => request(`/api/audit/step15/snapshot-quality/runs/${encodeURIComponent(runId)}`),
  restSafetyConfig: () => request("/api/config/rest-safety"),
  updateRestSafetyConfig: (values) =>
    request("/api/config/rest-safety", {
      method: "PATCH",
      body: JSON.stringify({ values })
    }),
  runtimeStart: () => request("/api/runtime/start", { method: "POST" }),
  runtimeStop: () => request("/api/runtime/stop", { method: "POST" }),
  runtimeRestart: () => request("/api/runtime/restart", { method: "POST" }),
  tradePlans: () => request("/api/decisions/trade-plans"),
  tradePlanFunnel: (runId = "latest", symbolLimit = 300) =>
    request(`/api/decisions/trade-plans/funnel?run_id=${encodeURIComponent(runId || "latest")}&symbol_limit=${symbolLimit}`),
  strategy4ObservePool: () => request("/api/strategy4/observe-pool"),
  strategy4Runtime: () => request("/api/strategy4/runtime"),
  strategy4Attempts: (limit = 200) => request(`/api/strategy4/attempts?limit=${limit}`),
  strategy5Runtime: (limit = 200) => request(`/api/strategy5/runtime?limit=${limit}`),
  strategy5Evidence: (limit = 200) => request(`/api/strategy5/evidence?limit=${limit}`),
  strategy6Runtime: (limit = 200) => request(`/api/strategy6/runtime?limit=${limit}`),
  strategy6Evidence: (limit = 200) => request(`/api/strategy6/evidence?limit=${limit}`),
  strategy6Decisions: (limit = 200) => request(`/api/strategy6/decisions?limit=${limit}`),
  strategy6WaitPool: (limit = 200) => request(`/api/strategy6/wait-pool?limit=${limit}`),
  strategy6ObservePool: (limit = 200) => request(`/api/strategy6/observe-pool?limit=${limit}`),
  strategy6Attempts: (limit = 200) => request(`/api/strategy6/attempts?limit=${limit}`),
  strategy6Heartbeat: () => request("/api/strategy6/heartbeat"),
  strategy6Watchdog: () => request("/api/strategy6/daemon/watchdog"),
  strategy6RunOnce: () => request("/api/strategy6/run-once", { method: "POST" }),
  strategy6DaemonStart: () => request("/api/strategy6/daemon/start", { method: "POST" }),
  strategy6DaemonStop: () => request("/api/strategy6/daemon/stop", { method: "POST" }),
  strategy6RecheckNow: () => request("/api/strategy6/daemon/recheck-now", { method: "POST" }),
  strategy6WatchdogRecover: () => request("/api/strategy6/daemon/watchdog/recover", { method: "POST" }),
  candidatePoolGovernance: (limit = 120) => request(`/api/governance/candidate-pool?limit=${limit}`),
  latestAudit: () => request("/api/reports/latest-audit"),
  runAuditLatest: () => request("/api/audit/runs/latest"),
  runAuditLatestLite: () => request("/api/audit/runs/latest-lite"),
  runAudits: (limit = 20) => request(`/api/audit/runs?limit=${limit}`),
  runAuditsLite: (limit = 20) => request(`/api/audit/runs-lite?limit=${limit}`),
  runAuditById: (runId) => request(`/api/audit/runs/${encodeURIComponent(runId)}`),
  microQualityLatest: () => request("/api/audit/micro-quality/latest"),
  microQualityById: (runId) => request(`/api/audit/micro-quality/${encodeURIComponent(runId)}`),
  microEvidenceLatest: () => request("/api/audit/micro-evidence/latest"),
  microEvidenceTargetSource: () => request("/api/audit/micro-evidence/target-source"),
  microTrainingLatest: (symbolLimit = 100) => request(`/api/micro-training/latest?symbol_limit=${symbolLimit}`),
  microTrainingRuns: (limit = 50) => request(`/api/micro-training/runs?limit=${limit}`),
  microTrainingById: (runId, symbolLimit = 200) =>
    request(`/api/micro-training/runs/${encodeURIComponent(runId)}?symbol_limit=${symbolLimit}`),
  microTrainingSymbol: (symbol, limit = 100) =>
    request(`/api/micro-training/symbols/${encodeURIComponent(symbol)}?limit=${limit}`),
  microTrainingCoverage: () => request("/api/micro-training/coverage"),
  microEvidenceById: (runId) => request(`/api/audit/micro-evidence/${encodeURIComponent(runId)}`),
  microFullZLatest: () => request("/api/audit/micro-full-z/latest"),
  microFullZById: (runId) => request(`/api/audit/micro-full-z/${encodeURIComponent(runId)}`),
  microFastRuntimeLatest: () => request("/api/audit/micro-fast-runtime/latest"),
  microFastRuntimeById: (runId) => request(`/api/audit/micro-fast-runtime/${encodeURIComponent(runId)}`),
  microFastTailCleanupLatest: () => request("/api/audit/micro-fast-runtime/tail-cleanup/latest"),
  microFastTailCleanupById: (runId) => request(`/api/audit/micro-fast-runtime/tail-cleanup/${encodeURIComponent(runId)}`),
  microFastJudgeableLatest: () => request("/api/audit/micro-fast-runtime/judgeable/latest"),
  microFastJudgeableById: (runId) => request(`/api/audit/micro-fast-runtime/judgeable/${encodeURIComponent(runId)}`),
  microFastJudgeableOnlyLatest: () => request("/api/audit/micro-fast-runtime/judgeable-only/latest"),
  microFastJudgeableOnlyById: (runId) => request(`/api/audit/micro-fast-runtime/judgeable-only/${encodeURIComponent(runId)}`),
  microFastJudgeableThroughputLatest: () => request("/api/audit/micro-fast-runtime/judgeable-throughput/latest"),
  microFastJudgeableThroughputById: (runId) => request(`/api/audit/micro-fast-runtime/judgeable-throughput/${encodeURIComponent(runId)}`),
  microFastCoverageSplitLatest: () => request("/api/audit/micro-fast-runtime/coverage-split/latest"),
  microFastCoverageSplitById: (runId) => request(`/api/audit/micro-fast-runtime/coverage-split/${encodeURIComponent(runId)}`),
  microFastValidBucketLatest: () => request("/api/audit/micro-fast-runtime/valid-bucket/latest"),
  microFastValidBucketById: (runId) => request(`/api/audit/micro-fast-runtime/valid-bucket/${encodeURIComponent(runId)}`),
  microFastRuntimeReason: (reason, params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/audit/micro-fast-runtime/reasons/${encodeURIComponent(reason)}${suffix}`);
  },
  microEvidenceSymbol: (symbol, limit = 100) =>
    request(`/api/audit/micro-evidence/symbols/${encodeURIComponent(symbol)}?limit=${limit}`),
  microEvidenceFindings: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/audit/micro-evidence/findings${suffix}`);
  },
  microEvidenceReason: (reason, params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/audit/micro-evidence/reasons/${encodeURIComponent(reason)}${suffix}`);
  },
  paperSummary: (line) => request(`/api/paper/summary${line ? `?line=${line}` : ""}`),
  paperSummaryLite: (line, limit = 20) => request(`/api/paper/summary-lite?limit=${limit}${line ? `&line=${line}` : ""}`),
  paperStats: (line) => request(`/api/paper/stats${line ? `?line=${line}` : ""}`),
  paperIntents: (line) => request(`/api/paper/intents${line ? `?line=${line}` : ""}`),
  paperEpochs: (line) => request(`/api/paper/epochs${line ? `?line=${line}` : ""}`),
  paperTrace: (line, symbol) =>
    request(`/api/paper/trace?line=${line || ""}&symbol=${encodeURIComponent(symbol || "")}`),
  paperRealismMetrics: (line) => request(`/api/paper/realism-metrics${line && line !== "overview" ? `?line=${line}` : ""}`),
  paperReconciliation: (line, limit = 100) =>
    request(`/api/paper/reconciliation?limit=${limit}${line && line !== "overview" ? `&line=${line}` : ""}`),
  paperOrderTrace: (orderId, line, symbol) => {
    const query = new URLSearchParams();
    if (orderId) query.set("order_id", orderId);
    if (line && line !== "overview") query.set("line", line);
    if (symbol) query.set("symbol", symbol);
    return request(`/api/paper/order-trace?${query.toString()}`);
  },
  paperConsumptionStatus: (runId, limit = 50) =>
    request(`/api/paper/consumption-status?limit=${limit}${runId ? `&run_id=${encodeURIComponent(runId)}` : ""}`),
  paperDetail: (line, symbol) =>
    request(`/api/paper/detail?line=${line || ""}&symbol=${encodeURIComponent(symbol || "")}`),
  paperExperiments: (line, limit = 50) =>
    request(`/api/paper/experiments?limit=${limit}${line ? `&line=${line}` : ""}`),
  paperExperiment: (experimentId) => request(`/api/paper/experiments/${encodeURIComponent(experimentId)}`),
  paperArchiveReset: (body) =>
    request("/api/paper/archive-reset", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  paperDaemonStatus: () => request("/api/paper/daemon/status"),
  paperDaemonStart: () => request("/api/paper/daemon/start", { method: "POST" }),
  paperDaemonStop: () => request("/api/paper/daemon/stop", { method: "POST" }),
  paperRunOnce: () => request("/api/paper/worker/run-once", { method: "POST" }),
  tradeQualitySummary: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/summary${suffix}`);
  },
  tradeQualitySamples: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/samples${suffix}`);
  },
  tradeQualityRecommendations: () => request("/api/trade-quality/recommendations"),
  tradeQualityIngestLedger: (limit = 100) => request(`/api/trade-quality/ingest-ledger?limit=${limit}`),
  tradeQualityArchiveBackfillDryRun: (limit = null) =>
    request(`/api/trade-quality/archive-backfill/dry-run${limit ? `?limit=${limit}` : ""}`, { method: "POST" }),
  tradeQualityArchiveBackfillRun: (limit = null) =>
    request(`/api/trade-quality/archive-backfill/run${limit ? `?limit=${limit}` : ""}`, { method: "POST" }),
  tradeQualityReplayLedger: (limit = 100) => request(`/api/trade-quality/replay-backfill/ledger?limit=${limit}`),
  tradeQualityReplayBackfillDryRun: (limit = null, sampleSource = "all") => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (sampleSource && sampleSource !== "all") query.set("sample_source", sampleSource);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/replay-backfill/dry-run${suffix}`, { method: "POST" });
  },
  tradeQualityReplayBackfillRun: (limit = null, sampleSource = "all") => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (sampleSource && sampleSource !== "all") query.set("sample_source", sampleSource);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/replay-backfill/run${suffix}`, { method: "POST" });
  },
  tradeQualityDiagnosticsSummary: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/diagnostics/summary${suffix}`);
  },
  tradeQualityDiagnosticsSamples: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/diagnostics/samples${suffix}`);
  },
  tradeQualityDiagnosticsAggregates: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/diagnostics/aggregates${suffix}`);
  },
  tradeQualityDiagnosticsArchivePackages: () => request("/api/trade-quality/diagnostics/archive-packages"),
  tradeQualityDiagnosticsSyncStatus: () => request("/api/trade-quality/diagnostics/sync-status"),
  tradeQualityDiagnosticsSyncDryRun: (limit = null, source = "all") => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/diagnostics/sync/dry-run${suffix}`, { method: "POST" });
  },
  tradeQualityDiagnosticsSyncRun: (limit = null, source = "all") => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/diagnostics/sync/run${suffix}`, { method: "POST" });
  },
  tradeQualityDiagnosticsReplayLedger: (limit = 100) => request(`/api/trade-quality/diagnostics/replay-ledger?limit=${limit}`),
  tradeQualityDiagnosticsReplayDryRun: (limit = null, source = "all", archiveId = "") => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    if (archiveId) query.set("archive_id", archiveId);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/diagnostics/replay/dry-run${suffix}`, { method: "POST" });
  },
  tradeQualityDiagnosticsReplayRun: (limit = null, source = "all", archiveId = "") => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    if (archiveId) query.set("archive_id", archiveId);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/diagnostics/replay/run${suffix}`, { method: "POST" });
  },
  tradeQualityEntryFeaturesBackfillDryRun: (limit = null, source = "all", archiveId = "", force = false) => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    if (archiveId) query.set("archive_id", archiveId);
    if (force) query.set("force", "true");
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/entry-features/backfill/dry-run${suffix}`, { method: "POST" });
  },
  tradeQualityEntryFeaturesBackfillRun: (limit = null, source = "all", archiveId = "", force = false) => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    if (archiveId) query.set("archive_id", archiveId);
    if (force) query.set("force", "true");
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/entry-features/backfill/run${suffix}`, { method: "POST" });
  },
  tradeQualityEntryMicrostructureBackfillDryRun: (limit = null, source = "all", archiveId = "", force = false, evidenceWindowSec = 180) => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    if (archiveId) query.set("archive_id", archiveId);
    if (force) query.set("force", "true");
    if (evidenceWindowSec) query.set("evidence_window_sec", evidenceWindowSec);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/entry-microstructure/backfill/dry-run${suffix}`, { method: "POST" });
  },
  tradeQualityEntryMicrostructureBackfillRun: (limit = null, source = "all", archiveId = "", force = false, evidenceWindowSec = 180) => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    if (archiveId) query.set("archive_id", archiveId);
    if (force) query.set("force", "true");
    if (evidenceWindowSec) query.set("evidence_window_sec", evidenceWindowSec);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/entry-microstructure/backfill/run${suffix}`, { method: "POST" });
  },
  tradeQualityEntryMarketContextBackfillRun: (limit = null, source = "all", archiveId = "", force = false) => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    if (archiveId) query.set("archive_id", archiveId);
    if (force) query.set("force", "true");
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/entry-market-context/backfill/run${suffix}`, { method: "POST" });
  },
  tradeQualityEntryContextV3BackfillRun: (limit = null, source = "all", archiveId = "", force = false) => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    if (archiveId) query.set("archive_id", archiveId);
    if (force) query.set("force", "true");
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/entry-context-v3/backfill/run${suffix}`, { method: "POST" });
  },
  tradeQualityDiagnosticsRefreshEnrich: (limit = 100, source = "current_paper", archiveId = "", force = false, dryRun = false) => {
    const query = new URLSearchParams();
    if (limit) query.set("limit", limit);
    if (source && source !== "all") query.set("source", source);
    if (archiveId) query.set("archive_id", archiveId);
    if (force) query.set("force", "true");
    if (dryRun) query.set("dry_run", "true");
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/diagnostics/refresh-enrich${suffix}`, { method: "POST" });
  },
  tradeQualityRecommendationRules: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/recommendation-rules${suffix}`);
  },
  tradeQualityRecommendationRulesRebuild: () =>
    request("/api/trade-quality/recommendation-rules/rebuild", { method: "POST" }),
  tradeQualityRecommendationValidation: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/recommendation-validation${suffix}`);
  },
  tradeQualityRecommendationPromotions: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/recommendation-promotions${suffix}`);
  },
  tradeQualityRecommendationPromotionDryRun: (body) =>
    request("/api/trade-quality/recommendation-promotions/dry-run", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  tradeQualityRecommendationPromotionApply: (body) =>
    request("/api/trade-quality/recommendation-promotions/apply", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  tradeQualityRecommendationPromotionDisable: (body) =>
    request("/api/trade-quality/recommendation-promotions/disable", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  tradeQualityV4Materialize: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/v4/materialize${suffix}`, { method: "POST" });
  },
  tradeQualityV4GateCandidatesGenerate: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/v4/gate-candidates/generate${suffix}`, { method: "POST" });
  },
  tradeQualityV4Summary: () => request("/api/trade-quality/v4/summary"),
  tradeQualityV4Evidence: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/v4/evidence${suffix}`);
  },
  tradeQualityV4DeepRootCauses: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/v4/deep-root-causes${suffix}`);
  },
  tradeQualityV4GateCandidates: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/v4/gate-candidates${suffix}`);
  },
  tradeQualityV5Materialize: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/v5/materialize${suffix}`, { method: "POST" });
  },
  tradeQualityV5GateCandidatesGenerate: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/v5/gate-candidates/generate${suffix}`, { method: "POST" });
  },
  tradeQualityV5Summary: () => request("/api/trade-quality/v5/summary"),
  tradeQualityV5CausalFactors: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/v5/causal-factors${suffix}`);
  },
  tradeQualityV5GateCandidates: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/trade-quality/v5/gate-candidates${suffix}`);
  },
  tradeQualityV5WriterCoverage: () => request("/api/trade-quality/v5/writer-coverage"),
  researchDbMaterialize: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/research-db/materialize${suffix}`, { method: "POST" });
  },
  researchDbSummary: () => request("/api/research-db/summary"),
  researchDbTradeFacts: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/research-db/trade-facts${suffix}`);
  },
  researchDbEntryFeatures: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/research-db/entry-features${suffix}`);
  },
  researchDbTqSamples: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/research-db/tq-samples${suffix}`);
  },
  researchDbDatasetCards: (limit = 20) => request(`/api/research-db/dataset-cards?limit=${limit}`),
  researchDbWriterStatus: () => request("/api/research-db/writer-status"),
  researchDbFieldCoverage: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/research-db/field-coverage${suffix}`);
  },
  researchDbLineageAudit: () => request("/api/research-db/lineage-audit"),
  backtestP21Packages: () => request("/api/backtest/p21/packages"),
  backtestP21ProblemBaseline: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/problem-baseline${suffix}`, { method: "POST" });
  },
  backtestP21RunMatrix: (body) =>
    request("/api/backtest/p21/run-matrix", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21Experiments: (limit = 50) => request(`/api/backtest/p21/experiments?limit=${limit}`),
  backtestP21Experiment: (experimentId) => request(`/api/backtest/p21/experiments/${encodeURIComponent(experimentId)}`),
  backtestP21Recommendations: (limit = 50) => request(`/api/backtest/p21/recommendations?limit=${limit}`),
  backtestP21ExportConfigCandidate: (body) =>
    request("/api/backtest/p21/export-config-candidate", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2KlineStatus: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/kline-cache/status${suffix}`);
  },
  backtestP21V2DownloadKlines: (body) =>
    request("/api/backtest/p21/v2/kline-cache/download", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2MatrixContracts: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/matrix/contracts${suffix}`);
  },
  backtestP21V2RunMatrix: (body) =>
    request("/api/backtest/p21/v2/matrix/run", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2StartJob: (body) =>
    request("/api/backtest/p21/v2/jobs/start", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2Jobs: (limit = 20) => request(`/api/backtest/p21/v2/jobs?limit=${limit}`),
  backtestP21V2JobStatus: (jobId) => request(`/api/backtest/p21/v2/jobs/${encodeURIComponent(jobId)}/status`),
  backtestP21V2StopJob: (jobId) =>
    request(`/api/backtest/p21/v2/jobs/${encodeURIComponent(jobId)}/stop`, {
      method: "POST"
    }),
  backtestP21V2Experiments: (limit = 50) => request(`/api/backtest/p21/v2/matrix/experiments?limit=${limit}`),
  backtestP21V2Experiment: (experimentId) => request(`/api/backtest/p21/v2/matrix/experiments/${encodeURIComponent(experimentId)}`),
  backtestP21V2ExperimentOrders: (experimentId, params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/matrix/experiments/${encodeURIComponent(experimentId)}/orders${suffix}`);
  },
  backtestP21V2ExperimentDaily: (experimentId, params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/matrix/experiments/${encodeURIComponent(experimentId)}/daily${suffix}`);
  },
  backtestP21V2ExperimentSymbols: (experimentId, params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/matrix/experiments/${encodeURIComponent(experimentId)}/symbols${suffix}`);
  },
  backtestP21V2Leaderboard: (limit = 50, params = {}) => {
    const query = new URLSearchParams({ limit: String(limit) });
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") query.set(key, value);
    });
    return request(`/api/backtest/p21/v2/matrix/leaderboard?${query.toString()}`);
  },
  backtestP21V2ExportConfigCandidate: (body) =>
    request("/api/backtest/p21/v2/matrix/export-config-candidate", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2Strategy4ReplayRun: (body) =>
    request("/api/backtest/p21/v2/strategy4/replay/run", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2Strategy4ReplaySummary: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/strategy4/replay/summary${suffix}`);
  },
  backtestP21V2Strategy4ReplayPool: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/strategy4/replay/pool${suffix}`);
  },
  backtestP21V2Strategy4ReplayAttempts: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/strategy4/replay/attempts${suffix}`);
  },
  backtestP21V2QualityPackages: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/quality/packages${suffix}`);
  },
  backtestP21V2QualitySummary: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/quality/summary${suffix}`);
  },
  backtestP21V2QualityAggregates: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/quality/aggregates${suffix}`);
  },
  backtestP21V2QualitySamples: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/quality/samples${suffix}`);
  },
  backtestP21V2QualityMaterialize: (body) =>
    request("/api/backtest/p21/v2/quality/materialize", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2GateTqBatchMaterialize: (body) =>
    request("/api/backtest/p21/v2/gate/tq-batch-materialize", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2GateFeaturesMaterialize: (body) =>
    request("/api/backtest/p21/v2/gate/features/materialize", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2GateFeatures: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/gate/features${suffix}`);
  },
  backtestP21V2GateBucketsRebuild: (body) =>
    request("/api/backtest/p21/v2/gate/buckets/rebuild", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2GateBuckets: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/gate/buckets${suffix}`);
  },
  backtestP21V2GateScoresRebuild: (body) =>
    request("/api/backtest/p21/v2/gate/scores/rebuild", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2GateScores: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/gate/scores${suffix}`);
  },
  backtestP21V2GateCandidatesGenerate: (body) =>
    request("/api/backtest/p21/v2/gate/candidates/generate", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2GateCandidates: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/gate/candidates${suffix}`);
  },
  backtestP21V2GateRecommendations: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/gate/recommendations${suffix}`);
  },
  backtestP21V2OpsFootprint: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/ops/footprint${suffix}`);
  },
  backtestP21V2OpsRetentionManifest: (body) =>
    request("/api/backtest/p21/v2/ops/retention-manifest", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2OpsServingRebuild: (body) =>
    request("/api/backtest/p21/v2/ops/serving/rebuild", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2OpsServingSummary: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/ops/serving/summary${suffix}`);
  },
  backtestP21V2OpsTqJobs: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/backtest/p21/v2/ops/tq-jobs${suffix}`);
  },
  backtestP21V2OpsTqJobEnqueue: (body) =>
    request("/api/backtest/p21/v2/ops/tq-jobs/enqueue", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2OpsTqJobProcessNext: () =>
    request("/api/backtest/p21/v2/ops/tq-jobs/process-next", {
      method: "POST",
      body: JSON.stringify({})
    }),
  backtestP21V2OpsEnhancedValidation: (body) =>
    request("/api/backtest/p21/v2/ops/enhanced-validation", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  backtestP21V2OpsCandidateExport: (body) =>
    request("/api/backtest/p21/v2/ops/candidate-export", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  strategySandboxList: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "" && value !== "all") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/strategy-sandbox/sandboxes${suffix}`);
  },
  strategySandboxCreate: (body) =>
    request("/api/strategy-sandbox/sandboxes", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  strategySandboxActive: () => request("/api/strategy-sandbox/active"),
  strategySandboxResourceGovernorStatus: () => request("/api/strategy-sandbox/resource-governor/status"),
  strategySandboxResourceGovernorRuns: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/strategy-sandbox/resource-governor/runs${suffix}`);
  },
  strategySandboxResourceGovernorRun: (runId) =>
    request(`/api/strategy-sandbox/resource-governor/runs/${encodeURIComponent(runId)}`),
  strategySandboxResourceGovernorRestBudget: (params = {}) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") query.set(key, value);
    });
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request(`/api/strategy-sandbox/resource-governor/rest-budget${suffix}`);
  },
  strategySandboxPipelineRun: (body = {}) =>
    request("/api/strategy-sandbox/pipeline/run", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  strategySandboxPipelineStop: (body = {}) =>
    request("/api/strategy-sandbox/pipeline/stop", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  strategySandboxSetActive: (sandboxId) =>
    request("/api/strategy-sandbox/active", {
      method: "PUT",
      body: JSON.stringify({ sandbox_id: sandboxId || null })
    }),
  strategySandboxDetail: (sandboxId) => request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}`),
  strategySandboxSummary: (sandboxId) => request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/summary`),
  strategySandboxBranches: (sandboxId) => request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/branches`),
  strategySandboxCodeOverlay: (sandboxId, strategyLine) =>
    request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/branches/${encodeURIComponent(strategyLine)}/code-overlay`),
  strategySandboxCreateCodeOverlay: (sandboxId, strategyLine) =>
    request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/branches/${encodeURIComponent(strategyLine)}/code-overlay`, {
      method: "POST",
      body: JSON.stringify({})
    }),
  strategySandboxAddCodePatch: (sandboxId, strategyLine, body = {}) =>
    request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/branches/${encodeURIComponent(strategyLine)}/code-patches`, {
      method: "POST",
      body: JSON.stringify(body)
    }),
  strategySandboxBuildRuntime: (sandboxId, strategyLine, body = {}) =>
    request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/branches/${encodeURIComponent(strategyLine)}/runtime/build`, {
      method: "POST",
      body: JSON.stringify(body)
    }),
  strategySandboxRuntimeSmoke: (sandboxId, strategyLine, body = {}) =>
    request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/branches/${encodeURIComponent(strategyLine)}/runtime/smoke`, {
      method: "POST",
      body: JSON.stringify(body)
    }),
  strategySandboxLeaderboard: (sandboxId) => request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/leaderboard`),
  strategySandboxTradeQualityCompare: (sandboxId) =>
    request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/trade-quality/compare`),
  strategySandboxGateCompare: (sandboxId) => request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/gate/compare`),
  strategySandboxDbHealth: (sandboxId) => request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/db-health`),
  strategySandboxDelete: (sandboxId, options = {}) =>
    request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}`, {
      method: "DELETE",
      body: JSON.stringify(options)
    }),
  strategySandboxJob: (sandboxId, jobType, options = {}) =>
    request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/${encodeURIComponent(jobType)}`, {
      method: "POST",
      body: JSON.stringify({ options })
    }),
  strategySandboxBranchJob: (sandboxId, strategyLine, jobType, options = {}) =>
    request(
      `/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/branches/${encodeURIComponent(strategyLine)}/${encodeURIComponent(jobType)}`,
      {
        method: "POST",
        body: JSON.stringify({ options })
      }
    ),
  strategySandboxMultiBranchJob: (sandboxId, jobType, options = {}) =>
    request(`/api/strategy-sandbox/sandboxes/${encodeURIComponent(sandboxId)}/jobs/${encodeURIComponent(jobType)}`, {
      method: "POST",
      body: JSON.stringify({ options })
    }),
  feishuConfig: () => request("/api/notifications/feishu/config"),
  updateFeishuConfig: (values) =>
    request("/api/notifications/feishu/config", {
      method: "PUT",
      body: JSON.stringify({ values })
    }),
  feishuTest: (message = "P13 Feishu test message") =>
    request("/api/notifications/feishu/test", {
      method: "POST",
      body: JSON.stringify({ message, mock: true })
    }),
  feishuSendTradePlans: (mockSignals = false, mockSend = true) =>
    request("/api/notifications/feishu/send-trade-plans", {
      method: "POST",
      body: JSON.stringify({ mock_signals: mockSignals, mock_send: mockSend })
    }),
  feishuDeliveries: () => request("/api/notifications/deliveries"),
  latestDelivery: () => request("/api/notifications/deliveries/latest")
};
